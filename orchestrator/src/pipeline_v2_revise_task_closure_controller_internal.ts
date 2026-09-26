import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2StageGenerationRecord,
  type PipelineV2StageIterationRecord,
  type PipelineV2TaskRevisionState,
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import type {
  PreparedPipelineV2RunWaitIntent,
  PipelineV2ReviseTaskIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral durable closure controller for an already accepted
 * `revise_task_intent` (unwired).
 *
 * The controller applies the durable closure step of the revise flow: the
 * exact boundary `accepted revise_task intent + accepted task revision →
 * stage_iteration_closed {by: "replanned"}`. It performs no filesystem
 * work at all — the accepted task revision is read exclusively from the
 * durable ledger, no manifest is published, no loader is called and no
 * response is recorded; it dispatches through the structural sink
 * (satisfied by the production `PipelineV2RunStateSink` without an
 * adapter) and owns no successor rules beyond its own single command.
 *
 * Capture order (fail-closed): the options shape; the fields `sink` →
 * `intent` read exactly once; the sink's `poisoned`, `dispatch` and
 * initial `snapshot` members captured exactly once as opaque references
 * with `dispatch` bound to the sink before the first await (no re-read of
 * `sink.dispatch`); the poison latch; the intent provenance gate (the
 * shared manifest registry — hand-built, cast, spread, `structuredClone`
 * and Proxy look-alikes are rejected before any field of the intent or of
 * the durable snapshot is read, Proxy traps never invoked); strictly the
 * `revise_task_intent` kind; only then the single
 * `validatePipelineV2RunState` (a missing or invalid snapshot is the
 * controller's own typed `invalid_state` with a fixed content-free
 * diagnostic and `state: null`; unexpected causes propagate unchanged)
 * and the durable bindings. A hostile extra options field is ignored.
 * Unexpected getters and errors propagate by identity and are never
 * classified from message text.
 *
 * Durable bindings (the controller accepts only an already durably
 * accepted revise intent):
 * - the waiting form: `status`/`phase` both `waiting`; the target wait the
 *   last and only record of its index; no response; the intent's run id
 *   and wait index matching the durable wait; the wait carrying the exact
 *   accepted `intent_sha256` (a different accepted intent is typed
 *   `invalid_state`); the declared `revise_task` action present; and the
 *   cursor exactly on the wait boundary (`current_state` = the wait's
 *   state id, `transition_count` = the wait's `transition_count`, and the
 *   transition and execution journals exactly at that count);
 * - the last durable plan revision exists; the last generation exists,
 *   remains the last one and is open (`lifecycle_conflict` otherwise);
 *   `generation.plan_sha256` equals the last plan record's digest
 *   (`lifecycle_conflict` otherwise); the target iteration is the
 *   generation's last iteration; the generation identity bindings
 *   (`index`, `stage_id`, `stage_position`, `template_id`, `plan_sha256`,
 *   `initial_budget`, `opened_transition_count`, `iteration_count`) are
 *   fixed as the comparison basis of the post-dispatch verification;
 * - the accepted task revision: the ledger is the only source (no
 *   filesystem read). The wait-bound records of the target wait — of ANY
 *   task — must be exactly one, and that single record must be bound
 *   exactly to the intent's task (`task_id`), digests
 *   (`new_task_revision_sha256`, `expected_previous_task_sha256`), wait
 *   index and intent digest, with a positive safe revision above 1 — and
 *   it must be the last durable revision of that task (no later revision
 *   of the same task). No accepted task record is `invalid_state`; a
 *   record contradicting the task, the digest, the predecessor or the
 *   intent binding, a ledger that already moved further, and several
 *   wait-bound records (of the same or of another task) are all
 *   `revision_conflict` (fail closed, never success).
 *
 * Reconciliation is ONE internal classification:
 * - C0 — the closure is absent: the target iteration is open. The single
 *   command `stage_iteration_closed {generationIndex, iterationIndex,
 *   by: "replanned", waitIndex}` is pre-checked through the single
 *   reducer on a local snapshot BEFORE the dispatch (a rejection is typed
 *   `invalid_state` with zero dispatch — unreachable through
 *   binding-valid states, kept as defense-in-depth), then dispatched
 *   exactly once;
 * - C1 — the exact closure is already durable (`by: "replanned"`, the
 *   wait index, `closed_transition_count` equal to the wait's
 *   `transition_count`) with the generation still last and open, the
 *   target iteration still the last one, no `open_iteration`, and the
 *   task revision and wait bindings exact — zero dispatch, the
 *   authoritative state returned;
 * - conflicts — an iteration closed with another reason, against another
 *   wait or another anchor, a closed/replaced/no-longer-last generation,
 *   another or later iteration, an `open_iteration` contradicting C0/C1,
 *   or a lifecycle that moved past the admissible boundary are typed
 *   `lifecycle_conflict`. A partial match is never an idempotent success.
 *
 * The active/answered retry is recognized ONLY as the immediate
 * post-response retry of the future completion flow: `status`/`phase`
 * active/running, the target wait still the last and only record of its
 * index keeping the exact accepted intent, the response carrying exactly
 * the `revise_task` action id, the cursor exactly at the declared
 * `revise_task` action's target with the transition and execution
 * journals exactly at the wait's `transition_count`, the accepted task
 * record exact, the exact `replanned` closure present, the generation
 * still the last one and open, the target iteration the last one and
 * closed with the exact closure, and no `open_iteration`. This is a
 * zero-dispatch success. Any later execution or transition, a new wait,
 * a new generation or iteration, another response action or a shifted
 * cursor is typed `lifecycle_conflict`; no publication is ever restored
 * (the controller performs no filesystem work).
 *
 * Post-dispatch verification is the same full targeted check on the
 * normal resolve path and the racing `PipelineV2StateError` path: the
 * only allowed changes are the target iteration's exact `replanned`
 * closure, the disappearing `open_iteration` projection and the expected
 * state revision increment (`after.revision === before.revision + 1`,
 * with the run identity pinned: `after.run_id === before.run_id`).
 * Verified: the
 * wait journal by length, position and every binding (ordered
 * `{id,to}` actions, exact intent, response absent); the task ledger
 * fully unchanged by length, positions and every contract field
 * (`index`/`task_id`/`revision`/`sha256`/`previous_sha256`/`wait_index`/
 * `intent_sha256`); the plan ledger boundary unchanged; the target
 * generation's exact index and identity bindings, its whole historical
 * iteration prefix unchanged by position (index, opening anchor and the
 * exact closed projection) and the exact closure anchor on the last
 * target iteration; the cursor, transition
 * and execution journals still at the wait boundary. Every array and
 * nested entry is checked defensively (`Array.isArray`/record guards)
 * before any field read, so a hostile `null`, primitive or malformed
 * nested snapshot yields a typed error and never a random `TypeError`;
 * there is no general deep comparator. A racing `PipelineV2StateError`
 * admits idempotent success only on the full exact C1 verification; a
 * dispatch that resolves without the required durable change is typed
 * `invalid_state`. The result is built from the authoritative snapshot
 * that passed the verification; the snapshot is never re-read after a
 * successful classification.
 *
 * Durability: a sink `not_committed` keeps the previous open state
 * authoritative (a fresh retry dispatches the closure again); a sink
 * `durability_unknown` adopts the visible candidate (the closure is
 * durable), poisons the sink and dispatches nothing further (a fresh
 * reopened sink recognizes the durable prefix with zero dispatch).
 * Nothing is ever rolled back.
 *
 * Runtime export surface (public module) is exactly
 * `PipelineV2ReviseTaskClosureControllerError` and
 * `applyPipelineV2ReviseTaskClosure`; the internal core module exports
 * exactly `PipelineV2ReviseTaskClosureControllerError` and
 * `applyPipelineV2ReviseTaskClosureInternal`. The closed reason set is
 * `invalid_intent | invalid_state | revision_conflict |
 * lifecycle_conflict | state_persist_failed` with the last authoritative
 * state (`null` when none exists). Diagnostics are content-free
 * (validated safe ids and indexes only — no digest values, task bodies,
 * canonical JSON, paths, env values or credentials); errors are never
 * classified from message text; unexpected causes propagate unchanged.
 *
 * Not implemented (stays unwired): the wait response publication and
 * `wait_response_recorded`, new task/plan revisions, the generation
 * closure, opening the next generation or iteration, the
 * architect/replanning execution, resume, the revise/continue action
 * policy and intent selection, coordinator/runner/CLI wiring, schema
 * changes, migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ReviseTaskClosureControllerFailureReason =
  | "invalid_intent"
  | "invalid_state"
  | "revision_conflict"
  | "lifecycle_conflict"
  | "state_persist_failed";

export class PipelineV2ReviseTaskClosureControllerError extends Error {
  readonly reason: PipelineV2ReviseTaskClosureControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReviseTaskClosureControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReviseTaskClosureControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam; the production `PipelineV2RunStateSink`
 * satisfies it without an adapter. The initial `snapshot` may be `null`
 * (no durable run); after the dispatch the sink's authoritative snapshot
 * is re-read through the captured sink object — never memoized.
 */
export interface PipelineV2ReviseTaskClosureControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface ApplyPipelineV2ReviseTaskClosureOptions {
  readonly sink: PipelineV2ReviseTaskClosureControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

export interface AppliedPipelineV2ReviseTaskClosure {
  readonly wait_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_sha256: string;
  readonly intent_sha256: string;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2ReviseTaskClosureControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskClosureControllerError {
  return new PipelineV2ReviseTaskClosureControllerError(reason, message, state);
}

function invalidIntent(message: string): PipelineV2ReviseTaskClosureControllerError {
  return controllerError("invalid_intent", message, null);
}

const REVISE_TASK_ACTION_ID = "revise_task";

interface ReviseClosureBindings {
  readonly manifest: PipelineV2ReviseTaskIntentManifest;
  readonly wait: PipelineV2WaitRecord;
  readonly generation: PipelineV2StageGenerationRecord;
  readonly iteration: PipelineV2StageIterationRecord;
  readonly lastPlanRecord: PipelineV2RunState["plan_revisions"][number];
}

/**
 * The target wait record must exist exactly once in the journal; a
 * duplicated wait index is never a valid verification target. Defensive
 * against structurally hostile snapshots: a non-array journal or a
 * non-record entry yields no match.
 */
function findWaitRecord(state: PipelineV2RunState, waitIndex: number): PipelineV2WaitRecord | undefined {
  if (!Array.isArray(state.waits)) {
    return undefined;
  }
  let found: PipelineV2WaitRecord | undefined;
  let count = 0;
  for (const record of state.waits) {
    if (isRecord(record) && record["index"] === waitIndex) {
      found = record as PipelineV2WaitRecord;
      count += 1;
    }
  }
  return count === 1 ? found : undefined;
}

/**
 * The durable bindings of the waiting form: the waiting run, the open
 * wait (the last and only record of its index) with the exact accepted
 * intent and the declared `revise_task` action, the cursor exactly on the
 * wait boundary, the last durable plan revision, and the last open
 * generation bound to it with its target (last) iteration.
 */
function requireReviseClosureBindings(
  state: PipelineV2RunState,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): ReviseClosureBindings {
  const manifest = preparedIntent.manifest as PipelineV2ReviseTaskIntentManifest;
  if (state.status !== "waiting" || state.phase !== "waiting") {
    throw controllerError(
      "invalid_state",
      "the run is not waiting; a revise task closure is applied only inside the open wait",
      state,
    );
  }
  const wait = findWaitRecord(state, manifest.wait_index);
  if (wait === undefined) {
    throw controllerError(
      "invalid_state",
      `the run records no wait ${manifest.wait_index} for the revise task closure`,
      state,
    );
  }
  if (manifest.run_id !== state.run_id) {
    throw controllerError(
      "invalid_state",
      "the wait intent names another run than the durable run state",
      state,
    );
  }
  const lastWait = state.waits[state.waits.length - 1];
  if (lastWait === undefined || lastWait.index !== wait.index) {
    if (wait.response !== undefined) {
      throw controllerError(
        "lifecycle_conflict",
        `wait ${wait.index} is no longer the last wait; the run moved past the answered boundary`,
        state,
      );
    }
    throw controllerError(
      "invalid_state",
      `the wait intent names wait index ${manifest.wait_index}, but the open wait record is ${lastWait === undefined ? "none" : lastWait.index}`,
      state,
    );
  }
  if (wait.response !== undefined) {
    throw controllerError(
      "invalid_state",
      `wait record ${wait.index} is already answered; the revise task closure applies inside the open wait`,
      state,
    );
  }
  if (wait.intent === undefined) {
    throw controllerError(
      "invalid_state",
      `the open wait ${wait.index} has not accepted an intent; record it before applying the closure`,
      state,
    );
  }
  if (wait.intent.intent_sha256 !== preparedIntent.sha256) {
    throw controllerError(
      "invalid_state",
      `the open wait ${wait.index} accepted a different intent; one intent belongs to one wait`,
      state,
    );
  }
  if (!wait.actions.some((action) => action.id === REVISE_TASK_ACTION_ID)) {
    throw controllerError(
      "invalid_state",
      "the open wait does not declare the revise_task action",
      state,
    );
  }
  if (state.cursor.current_state !== wait.state_id) {
    throw controllerError(
      "invalid_state",
      "the cursor is not at the open wait's state",
      state,
    );
  }
  if (state.cursor.transition_count !== wait.transition_count) {
    throw controllerError(
      "invalid_state",
      "the transition journal moved past the open wait boundary",
      state,
    );
  }
  if (state.transitions.length !== wait.transition_count) {
    throw controllerError(
      "invalid_state",
      "the transition journal does not match the open wait boundary",
      state,
    );
  }
  if (state.executions.length !== wait.transition_count) {
    throw controllerError(
      "invalid_state",
      "an execution was started inside the open wait boundary",
      state,
    );
  }
  const lastPlanRecord = state.plan_revisions[state.plan_revisions.length - 1];
  if (lastPlanRecord === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no durable plan revision for the revise task closure",
      state,
    );
  }
  const generation = state.generations[state.generations.length - 1];
  if (generation === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no stage generation for the revise task closure",
      state,
    );
  }
  if (generation.closed !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the last stage generation ${generation.index} is closed; a replanned closure applies only inside an open generation`,
      state,
    );
  }
  if (generation.plan_sha256 !== lastPlanRecord.sha256) {
    throw controllerError(
      "lifecycle_conflict",
      `the open stage generation ${generation.index} does not belong to the last durable plan revision ${lastPlanRecord.revision}`,
      state,
    );
  }
  const iteration = generation.iterations[generation.iterations.length - 1];
  if (iteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the stage generation ${generation.index} records no iterations`,
      state,
    );
  }
  return { manifest, wait, generation, iteration, lastPlanRecord };
}

/**
 * The accepted task revision of the target wait, read exclusively from
 * the durable ledger (no filesystem): the wait-bound records of the
 * target wait — of ANY task — must be exactly one, and that single record
 * must be bound exactly to the intent's task, digests, wait and intent,
 * with a positive safe revision above 1, and no later revision of the
 * same task. A second task revision accepted for the same wait is never
 * passed by an exact task-id filter first. No accepted record is
 * `invalid_state`; a contradicting record, a ledger that moved further
 * and several wait-bound records are `revision_conflict` (fail closed,
 * never success).
 */
function requireAcceptedTaskRecord(
  state: PipelineV2RunState,
  taskId: string,
  waitIndex: number,
  expectedNewSha256: string,
  expectedPreviousSha256: string,
  intentSha256: string,
): PipelineV2TaskRevisionState {
  const waitBound = state.task_revisions.filter((record) => record.wait_index === waitIndex);
  if (waitBound.length === 0) {
    throw controllerError(
      "invalid_state",
      `wait ${waitIndex} carries no accepted task revision for the revise closure`,
      state,
    );
  }
  if (waitBound.length > 1) {
    throw controllerError(
      "revision_conflict",
      `wait ${waitIndex} carries several accepted task revisions`,
      state,
    );
  }
  const record = waitBound[0]!;
  if (
    record.task_id !== taskId ||
    record.sha256 !== expectedNewSha256 ||
    record.previous_sha256 !== expectedPreviousSha256 ||
    record.intent_sha256 !== intentSha256 ||
    !(typeof record.revision === "number" && Number.isSafeInteger(record.revision) && record.revision > 1)
  ) {
    throw controllerError(
      "revision_conflict",
      `the accepted task revision of wait ${waitIndex} for task ${JSON.stringify(taskId)} contradicts the revise intent`,
      state,
    );
  }
  const movedFurther = state.task_revisions.some(
    (other) => other.task_id === taskId && other.revision > record.revision,
  );
  if (movedFurther) {
    throw controllerError(
      "revision_conflict",
      `the task ledger already moved past revision ${record.revision} of task ${JSON.stringify(taskId)}`,
      state,
    );
  }
  return record;
}

/**
 * The single internal reconciliation: C0 (the target iteration is open —
 * the closure command is dispatched once after the pre-check), C1 (the
 * exact replanned closure of this wait is durable — zero dispatch) and
 * every typed conflict between them. A partial match is never an
 * idempotent success.
 */
function classifyReviseClosure(
  state: PipelineV2RunState,
  bindings: ReviseClosureBindings,
): { readonly kind: "c0" } | { readonly kind: "c1" } {
  const closed = bindings.iteration.closed;
  if (closed === undefined) {
    return { kind: "c0" };
  }
  if (closed.by !== "replanned") {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${bindings.iteration.index} of generation ${bindings.generation.index} was closed with ${JSON.stringify(closed.by)}, not by the replanned closure`,
      state,
    );
  }
  if (closed.wait_index !== bindings.wait.index) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${bindings.iteration.index} of generation ${bindings.generation.index} was closed against another wait`,
      state,
    );
  }
  if (closed.closed_transition_count !== bindings.wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${bindings.iteration.index} of generation ${bindings.generation.index} was closed against another boundary`,
      state,
    );
  }
  if (bindings.generation.open_iteration !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the generation ${bindings.generation.index} still projects an open iteration`,
      state,
    );
  }
  return { kind: "c1" };
}

/**
 * The reducer pre-check of the closure command on a local snapshot,
 * before the dispatch; a reducer rejection is a typed `invalid_state`
 * with zero dispatch and any other cause propagates unchanged. Internal
 * to this module: not a runtime export and not a test seam (the
 * pre-check-before-dispatch ordering is proven by the source-order test
 * of the flow).
 */
function precheckClosure(
  state: PipelineV2RunState,
  command: PipelineV2RunCommand,
  snapshot: PipelineV2RunState,
): void {
  try {
    reducePipelineV2RunCommand(state, command, new Date());
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "the current run state does not accept the revise task closure",
        snapshot,
      );
    }
    throw cause;
  }
}

/**
 * The per-position binding equality of one wait record: identity fields,
 * ordered `{id,to}` actions, the accepted intent and the response. Every
 * viewed entry is shape-checked (`null`, primitives and non-record
 * entries fail closed) before any field access.
 */
function waitRecordBindingsMatch(before: PipelineV2WaitRecord, after: unknown): boolean {
  if (!isRecord(after)) {
    return false;
  }
  if (
    before.index !== after["index"] ||
    before.transition_count !== after["transition_count"] ||
    before.state_id !== after["state_id"] ||
    before.reason !== after["reason"] ||
    before.request_sha256 !== after["request_sha256"]
  ) {
    return false;
  }
  const afterActions = after["actions"];
  if (!Array.isArray(afterActions) || before.actions.length !== afterActions.length) {
    return false;
  }
  const actionsEqual = before.actions.every((action, position) => {
    const other = afterActions[position];
    return (
      other !== undefined &&
      isRecord(other) &&
      other["id"] === action.id &&
      other["to"] === action.to
    );
  });
  if (!actionsEqual) {
    return false;
  }
  const beforeIntent = before.intent;
  const afterIntent = after["intent"];
  const intentEqual =
    beforeIntent === undefined
      ? afterIntent === undefined
      : isRecord(beforeIntent) && isRecord(afterIntent) && beforeIntent["intent_sha256"] === (afterIntent as Record<string, unknown>)["intent_sha256"];
  if (!intentEqual) {
    return false;
  }
  const beforeResponse = before.response;
  const afterResponse = after["response"];
  const responseEqual =
    beforeResponse === undefined
      ? afterResponse === undefined
      : isRecord(beforeResponse) &&
        isRecord(afterResponse) &&
        beforeResponse["action_id"] === (afterResponse as Record<string, unknown>)["action_id"] &&
        beforeResponse["response_sha256"] === (afterResponse as Record<string, unknown>)["response_sha256"];
  return responseEqual;
}

/**
 * The wait journal is unchanged: the same length and every record's
 * bindings at its position (the closure changes no wait record).
 */
function waitJournalUnchanged(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.waits) || after.waits.length !== before.waits.length) {
    return false;
  }
  return before.waits.every((beforeEntry, position) => waitRecordBindingsMatch(beforeEntry, after.waits[position]));
}

/**
 * The task ledger is fully unchanged: the same length and every record's
 * contract fields (index, task id, revision, digest, chain predecessor,
 * wait and intent links) at its position.
 */
function taskLedgerUnchanged(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.task_revisions) || after.task_revisions.length !== before.task_revisions.length) {
    return false;
  }
  return before.task_revisions.every((beforeEntry, position) => {
    const afterEntry = after.task_revisions[position];
    return (
      afterEntry !== undefined &&
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["task_id"] === beforeEntry.task_id &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["wait_index"] === beforeEntry.wait_index &&
      afterEntry["intent_sha256"] === beforeEntry.intent_sha256
    );
  });
}

/**
 * The plan ledger boundary is unchanged: the same length and every
 * record's chain fields (index, revision, digest, predecessor, origin
 * execution) at its position.
 */
function planLedgerUnchanged(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.plan_revisions) || after.plan_revisions.length !== before.plan_revisions.length) {
    return false;
  }
  return before.plan_revisions.every((beforeEntry, position) => {
    const afterEntry = after.plan_revisions[position];
    return (
      afterEntry !== undefined &&
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["origin_execution"] === beforeEntry.origin_execution
    );
  });
}

/**
 * The exact post-closure generation: still the last generation at its
 * position with its exact index, still open, with unchanged identity
 * bindings; the whole historical iteration prefix (every iteration before
 * the last) is unchanged by position — index, opening anchor and the
 * exact closed projection (absence/presence, `by`, wait index and
 * boundary); only the last target iteration may have changed, gaining the
 * exact `replanned` closure of the wait; no `open_iteration` projection.
 */
function generationClosedExactly(after: PipelineV2RunState, bindings: ReviseClosureBindings): boolean {
  if (!Array.isArray(after.generations) || after.generations.length !== bindings.generation.index) {
    return false;
  }
  const generation = after.generations[bindings.generation.index - 1];
  if (generation === undefined || !isRecord(generation)) {
    return false;
  }
  if (
    generation["index"] !== bindings.generation.index ||
    generation["closed"] !== undefined ||
    generation["stage_id"] !== bindings.generation.stage_id ||
    generation["stage_position"] !== bindings.generation.stage_position ||
    generation["template_id"] !== bindings.generation.template_id ||
    generation["plan_sha256"] !== bindings.generation.plan_sha256 ||
    generation["initial_budget"] !== bindings.generation.initial_budget ||
    generation["opened_transition_count"] !== bindings.generation.opened_transition_count ||
    generation["iteration_count"] !== bindings.generation.iteration_count
  ) {
    return false;
  }
  const iterations = generation["iterations"];
  if (!Array.isArray(iterations) || iterations.length !== bindings.generation.iterations.length) {
    return false;
  }
  const lastPosition = iterations.length - 1;
  for (let position = 0; position < lastPosition; position += 1) {
    const beforeIteration = bindings.generation.iterations[position];
    if (beforeIteration === undefined) {
      return false;
    }
    const entry = iterations[position];
    if (entry === undefined || !isRecord(entry)) {
      return false;
    }
    const closed = entry["closed"];
    const closedEqual =
      beforeIteration.closed === undefined
        ? closed === undefined
        : isRecord(closed) &&
          closed["by"] === beforeIteration.closed.by &&
          closed["wait_index"] === beforeIteration.closed.wait_index &&
          closed["closed_transition_count"] === beforeIteration.closed.closed_transition_count;
    if (
      entry["index"] !== beforeIteration.index ||
      entry["opened_transition_count"] !== beforeIteration.opened_transition_count ||
      !closedEqual
    ) {
      return false;
    }
  }
  const last = iterations[lastPosition];
  if (last === undefined || !isRecord(last)) {
    return false;
  }
  if (
    last["index"] !== bindings.iteration.index ||
    last["opened_transition_count"] !== bindings.iteration.opened_transition_count
  ) {
    return false;
  }
  const closed = last["closed"];
  if (
    !isRecord(closed) ||
    closed["by"] !== "replanned" ||
    closed["wait_index"] !== bindings.wait.index ||
    closed["closed_transition_count"] !== bindings.wait.transition_count
  ) {
    return false;
  }
  return generation["open_iteration"] === undefined;
}

/**
 * The cursor, transition and execution journals are still exactly on the
 * wait boundary.
 */
function cursorAtWaitBoundary(after: PipelineV2RunState, wait: PipelineV2WaitRecord): boolean {
  if (!isRecord(after.cursor)) {
    return false;
  }
  return (
    after.cursor["current_state"] === wait.state_id &&
    after.cursor["transition_count"] === wait.transition_count &&
    Array.isArray(after.transitions) &&
    after.transitions.length === wait.transition_count &&
    Array.isArray(after.executions) &&
    after.executions.length === wait.transition_count
  );
}

/**
 * The full post-closure verification: the only allowed changes are the
 * target iteration's exact `replanned` closure, the disappearing
 * `open_iteration` projection and the expected state revision increment
 * (`after.revision === before.revision + 1`, with the run identity
 * pinned). Every array and nested entry is shape-checked before any field
 * read, so a hostile malformed snapshot yields `false` and a typed error,
 * never a `TypeError`.
 */
function closureAppliedExactly(
  after: PipelineV2RunState,
  before: PipelineV2RunState,
  bindings: ReviseClosureBindings,
): boolean {
  if (!isRecord(after)) {
    return false;
  }
  if (after.status !== "waiting" || after.phase !== "waiting") {
    return false;
  }
  if (after.run_id !== before.run_id) {
    return false;
  }
  if (after.revision !== before.revision + 1) {
    return false;
  }
  if (!waitJournalUnchanged(before, after)) {
    return false;
  }
  if (!taskLedgerUnchanged(before, after)) {
    return false;
  }
  if (!planLedgerUnchanged(before, after)) {
    return false;
  }
  if (!generationClosedExactly(after, bindings)) {
    return false;
  }
  return cursorAtWaitBoundary(after, bindings.wait);
}

/**
 * The typed classification of a failed authoritative verification,
 * shared by the normal resolve path and the racing dispatch path: a
 * removed or replaced accepted intent is `invalid_state`; an iteration
 * already closed differently is `lifecycle_conflict`; everything else is
 * the failed verification of the applied closure.
 */
function raceOrMismatch(
  after: PipelineV2RunState | null,
  bindings: ReviseClosureBindings,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): never {
  if (after !== null && isRecord(after)) {
    const afterWait = findWaitRecord(after, bindings.wait.index);
    if (afterWait === undefined || afterWait.intent?.intent_sha256 !== preparedIntent.sha256) {
      throw controllerError(
        "invalid_state",
        `the accepted intent of open wait ${bindings.wait.index} is missing or replaced in the authoritative state`,
        after,
      );
    }
    if (Array.isArray(after.generations) && after.generations.length >= bindings.generation.index) {
      const afterGeneration = after.generations[bindings.generation.index - 1];
      if (isRecord(afterGeneration) && Array.isArray(afterGeneration["iterations"])) {
        const closedIteration = (afterGeneration["iterations"] as unknown[]).find(
          (entry) => isRecord(entry) && entry["index"] === bindings.iteration.index,
        );
        const closed = closedIteration === undefined ? undefined : (closedIteration as Record<string, unknown>)["closed"];
        if (
          closed !== undefined &&
          isRecord(closed) &&
          (closed["by"] !== "replanned" ||
            closed["wait_index"] !== bindings.wait.index ||
            closed["closed_transition_count"] !== bindings.wait.transition_count)
        ) {
          throw controllerError(
            "lifecycle_conflict",
            `the iteration ${bindings.iteration.index} of generation ${bindings.generation.index} is already closed differently`,
            after,
          );
        }
      }
    }
  }
  throw controllerError(
    "invalid_state",
    `the run state does not carry the applied closure of wait ${bindings.wait.index}`,
    after,
  );
}

/**
 * The immediate post-response retry of the future completion flow: the
 * exact completed boundary on the active/answered run. The recognition
 * requires the target wait to be the last and only record of its index
 * keeping the exact accepted intent, the response to carry exactly the
 * `revise_task` action id, the cursor exactly at the declared action's
 * target with the transition and execution journals exactly at the wait's
 * `transition_count`, the exact accepted task revision, the exact
 * `replanned` closure on the last open generation's last iteration, and
 * no `open_iteration`. Zero dispatch, no state restoration. Later
 * lifecycle progress is a typed `lifecycle_conflict`, never a retry of
 * this boundary.
 */
function applyCompletedClosureRetry(
  state: PipelineV2RunState,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): AppliedPipelineV2ReviseTaskClosure {
  const manifest = preparedIntent.manifest as PipelineV2ReviseTaskIntentManifest;
  if (manifest.run_id !== state.run_id) {
    throw controllerError(
      "invalid_state",
      "the wait intent names another run than the durable run state",
      state,
    );
  }
  const wait = findWaitRecord(state, manifest.wait_index);
  if (wait === undefined) {
    throw controllerError(
      "invalid_state",
      `the run records no wait ${manifest.wait_index} to recognize the completed revise-task boundary`,
      state,
    );
  }
  const lastWait = state.waits[state.waits.length - 1];
  if (lastWait === undefined || lastWait.index !== wait.index) {
    throw controllerError(
      "lifecycle_conflict",
      `wait ${wait.index} is no longer the last wait; a later intervention moved the run past this boundary`,
      state,
    );
  }
  if (!wait.actions.some((action) => action.id === REVISE_TASK_ACTION_ID)) {
    throw controllerError(
      "invalid_state",
      "the last wait record does not declare the revise_task action",
      state,
    );
  }
  if (wait.intent?.intent_sha256 !== preparedIntent.sha256) {
    throw controllerError(
      "invalid_state",
      `the last wait ${wait.index} accepted a different intent; one intent belongs to one wait`,
      state,
    );
  }
  const response = wait.response;
  if (response === undefined) {
    throw controllerError(
      "invalid_state",
      `the last wait ${wait.index} is not answered; the completed boundary requires the recorded response`,
      state,
    );
  }
  if (response.action_id !== REVISE_TASK_ACTION_ID) {
    throw controllerError(
      "lifecycle_conflict",
      `the last wait ${wait.index} was answered with another action; this is not the revise-task completion boundary`,
      state,
    );
  }
  const declared = wait.actions.find((action) => action.id === REVISE_TASK_ACTION_ID);
  if (declared === undefined) {
    throw controllerError(
      "invalid_state",
      "the last wait record does not declare the revise_task action",
      state,
    );
  }
  if (state.cursor.current_state !== declared.to) {
    throw controllerError(
      "lifecycle_conflict",
      "the cursor is not at the revise_task action target of the completed boundary",
      state,
    );
  }
  if (state.cursor.transition_count !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      "the transition journal moved past the settled wait boundary",
      state,
    );
  }
  if (state.transitions.length !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      "the transition journal does not match the settled wait boundary",
      state,
    );
  }
  if (state.executions.length !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      "an execution was started after the settled wait boundary",
      state,
    );
  }
  const taskRecord = requireAcceptedTaskRecord(
    state,
    manifest.task_id,
    wait.index,
    manifest.new_task_revision_sha256,
    manifest.expected_previous_task_sha256,
    preparedIntent.sha256,
  );
  const lastPlanRecord = state.plan_revisions[state.plan_revisions.length - 1];
  if (lastPlanRecord === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no durable plan revision for the revise task closure",
      state,
    );
  }
  const generation = state.generations[state.generations.length - 1];
  if (generation === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no stage generation for the revise task closure",
      state,
    );
  }
  if (state.generations.length !== generation.index) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${generation.index} is no longer the last generation`,
      state,
    );
  }
  if (generation.closed !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${generation.index} is closed`,
      state,
    );
  }
  if (generation.plan_sha256 !== lastPlanRecord.sha256) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${generation.index} does not belong to the last durable plan revision ${lastPlanRecord.revision}`,
      state,
    );
  }
  const iteration = generation.iterations[generation.iterations.length - 1];
  if (iteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the stage generation ${generation.index} records no iterations`,
      state,
    );
  }
  const closed = iteration.closed;
  if (closed === undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} is open; the completed boundary is not a recognizable retry`,
      state,
    );
  }
  if (
    closed.by !== "replanned" ||
    closed.wait_index !== wait.index ||
    closed.closed_transition_count !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} does not carry the exact replanned closure of wait ${wait.index}`,
      state,
    );
  }
  if (generation.open_iteration !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the generation ${generation.index} still projects an open iteration`,
      state,
    );
  }
  return deepFreezeValue({
    wait_index: wait.index,
    generation_index: generation.index,
    iteration_index: iteration.index,
    task_id: taskRecord.task_id,
    task_revision: taskRecord.revision,
    task_sha256: taskRecord.sha256,
    intent_sha256: preparedIntent.sha256,
    state,
  });
}

function finishResult(
  state: PipelineV2RunState,
  bindings: ReviseClosureBindings,
  taskRecord: PipelineV2TaskRevisionState,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): AppliedPipelineV2ReviseTaskClosure {
  return deepFreezeValue({
    wait_index: bindings.wait.index,
    generation_index: bindings.generation.index,
    iteration_index: bindings.iteration.index,
    task_id: taskRecord.task_id,
    task_revision: taskRecord.revision,
    task_sha256: taskRecord.sha256,
    intent_sha256: preparedIntent.sha256,
    state,
  });
}

/**
 * Validate, bind and apply the durable revise-task closure through the
 * existing reducer (see the module docstring for the full order and
 * durability semantics).
 */
export async function applyPipelineV2ReviseTaskClosureInternal(
  options: unknown,
): Promise<AppliedPipelineV2ReviseTaskClosure> {
  // Capture boundary: every options field is read exactly once (`sink` →
  // `intent`), and the sink's `poisoned`, `dispatch` and initial
  // `snapshot` members are read exactly once as opaque references. No
  // field of the intent or of the durable snapshot is read here.
  if (!isRecord(options)) {
    throw invalidIntent("applyPipelineV2ReviseTaskClosure requires an options object");
  }
  const sink = options["sink"];
  const intent = options["intent"];
  if (!isRecord(sink)) {
    throw invalidIntent("applyPipelineV2ReviseTaskClosure requires a sink object");
  }
  if (!isRecord(intent)) {
    throw invalidIntent("applyPipelineV2ReviseTaskClosure requires a prepared wait intent object");
  }
  const poisoned = sink["poisoned"];
  const dispatch = sink["dispatch"];
  const initialSnapshot = sink["snapshot"];
  if (typeof poisoned !== "boolean") {
    throw invalidIntent("the run state sink requires a boolean poisoned flag");
  }
  if (typeof dispatch !== "function") {
    throw invalidIntent("the run state sink requires a dispatch function");
  }
  const sinkRef = sink as unknown as PipelineV2ReviseTaskClosureControllerSink;
  // The dispatch is bound to the sink immediately at capture: a later
  // reassignment of the sink's member cannot change the dispatch target,
  // and the sink member is never read again.
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no closure.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no revise task closure can be applied",
      null,
    );
  }
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the revise kind. Hand-built,
  // cast, spread, cloned and Proxy look-alikes are rejected here, before
  // any field of the intent or of the durable snapshot is read.
  if (!hasPreparedRunPlanProvenance(intent, "revise_task_intent")) {
    throw invalidIntent("the intent is not a provenance-registered revise_task_intent");
  }
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  // The single state validation of the durable snapshot.
  let state: PipelineV2RunState;
  try {
    state = validatePipelineV2RunState(initialSnapshot as unknown as PipelineV2RunState | null);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "the durable run state is missing or not a valid pipeline v2 run state",
        null,
      );
    }
    throw cause;
  }
  // The run must be either waiting (the open-wait closure boundary) or
  // active with the answered target wait recognized as the exact
  // completed retry (zero dispatch). Any other status is a typed failure;
  // later lifecycle progress is never treated as this boundary's retry.
  if (state.status === "active" && state.phase === "running") {
    return applyCompletedClosureRetry(state, preparedIntent);
  }
  const bindings = requireReviseClosureBindings(state, preparedIntent);
  const taskRecord = requireAcceptedTaskRecord(
    state,
    bindings.manifest.task_id,
    bindings.wait.index,
    bindings.manifest.new_task_revision_sha256,
    bindings.manifest.expected_previous_task_sha256,
    preparedIntent.sha256,
  );
  const plan = classifyReviseClosure(state, bindings);
  if (plan.kind === "c1") {
    // C1: the exact durable closure; the authoritative state is the
    // verified result — zero dispatch.
    return finishResult(state, bindings, taskRecord, preparedIntent);
  }
  // C0: the reducer pre-check of the single closure command precedes the
  // dispatch; a rejection is typed `invalid_state` with zero dispatch.
  const closureCommand: PipelineV2RunCommand = {
    kind: "stage_iteration_closed",
    generationIndex: bindings.generation.index,
    iterationIndex: bindings.iteration.index,
    by: "replanned",
    waitIndex: bindings.wait.index,
  };
  precheckClosure(state, closureCommand, state);
  try {
    await dispatchCommand(closureCommand);
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "the stage iteration closure could not be confirmed durable",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "the stage iteration closure could not be committed",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2StateError) {
      // A racing identical dispatch is idempotent success only on the
      // full exact C1 verification of the authoritative snapshot.
      const after = sinkRef.snapshot;
      if (after !== null && closureAppliedExactly(after, state, bindings)) {
        return finishResult(after, bindings, taskRecord, preparedIntent);
      }
      raceOrMismatch(after, bindings, preparedIntent);
    }
    throw cause;
  }
  const after = sinkRef.snapshot;
  if (after === null || !closureAppliedExactly(after, state, bindings)) {
    raceOrMismatch(after, bindings, preparedIntent);
  }
  return finishResult(after, bindings, taskRecord, preparedIntent);
}
