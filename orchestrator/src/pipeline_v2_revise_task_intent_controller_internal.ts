import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2TaskRevisionState,
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  loadPipelineV2PlanRevision,
  loadPipelineV2TaskRevision,
  publishPipelineV2TaskRevision,
  publishPipelineV2WaitIntent,
  type PublishedPipelineV2RunPlanRevision,
  type PublishedPipelineV2RunTaskRevision,
  type PublishedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_store.ts";
import {
  validateReviseIntentBinding,
  validateTaskRevisionChain,
} from "./pipeline_v2_run_plan_bindings.ts";
import { PipelineV2RunStateDurabilityError, PipelineV2RunStateStoreError } from "./pipeline_v2_state_store.ts";
import type {
  PreparedPipelineV2RunTaskRevision,
  PreparedPipelineV2RunWaitIntent,
  PipelineV2ReviseTaskIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral acceptance controller for `revise_task_intent` wait
 * intents (unwired).
 *
 * The controller accepts one provenance-registered prepared wait intent
 * (the exact deep-frozen object of the run-plan manifest substrate, and
 * strictly the `revise_task_intent` kind) together with one
 * provenance-registered prepared candidate task revision (the only place
 * the task `body` exists), binds both against the current durable run,
 * publishes the immutable task-revision and wait-intent manifests through
 * the existing run-plan filesystem store in that exact order, and durably
 * records the missing suffix of the exact sequence
 * `plan_intent_accepted` → `task_revision_accepted` through the
 * structural sink. The increment ends at the accepted task revision: the
 * controller never closes the iteration, never records a wait response,
 * never creates a plan revision and never resumes the run.
 *
 * CURRENT task revision is derived ONLY from the last accepted plan
 * revision's task pointer of the current stage, never from the end of the
 * task ledger: the last open stage generation and its open iteration must
 * exist, the last durable plan record must exist and its digest must
 * equal the generation's `plan_sha256`, the plan manifest is loaded
 * read-only by the durable revision and must match the durable record
 * exactly (run id, revision, digest, previous digest, origin execution),
 * the plan must carry the open generation's stage, the stage must carry
 * exactly the intent's task as its task pointer, that pointer must have
 * an exact durable task record (task id, revision, digest), and the
 * pointer's immutable task artifact must be loadable and must match both
 * the pointer and the durable record. A historical task, a task of
 * another stage, a task of an older plan or a task that survives only in
 * the ledger is rejected as `invalid_state` before any publication or
 * dispatch.
 *
 * The candidate binding is fully checked in every reconciliation state,
 * including the exact durable candidate: the existing
 * `validateTaskRevisionChain` (the candidate is the exact successor of
 * the plan-bound current revision) and `validateReviseIntentBinding` (the
 * intent's two digests name the current/candidate digests exactly, the
 * candidate origin `user_response`) are the only binding authorities —
 * their errors keep their original classes — and the candidate must
 * actually change the task: an unchanged body is the controller's own
 * `invalid_intent` with zero publication and zero dispatch. The body is
 * never part of any diagnostic.
 *
 * Reconciliation over the durable records only, with the reducer
 * pre-check preceding every filesystem effect:
 * - R0: no durable intent, no durable candidate — the whole missing
 *   sequence (`plan_intent_accepted` → `task_revision_accepted`) is
 *   pre-checked through the single reducer on a local snapshot BEFORE any
 *   publication (a rejection is `invalid_state` with zero publishers
 *   called and zero dispatch);
 * - R1: the exact durable intent, no durable candidate — only the task
 *   revision is pre-checked, again before any publication;
 * - R2: the exact durable intent and the exact durable candidate — no
 *   pre-check, zero dispatch, but both artifacts are still re-published
 *   and re-verified (idempotent adoption restores removed files without
 *   touching existing ones);
 * - a durable candidate without the exact accepted wait intent is an
 *   explicit fail-closed `invalid_state` branch (unreachable through
 *   loader-valid states, enforced before any publication); another digest
 *   at the candidate revision or a ledger entry beyond the candidate
 *   revision is `candidate_conflict`; nothing is ever returned as success
 *   before the full binding checks and the filesystem reconciliation.
 * The pre-check helper is internal to this module: not a runtime export
 * and not a test seam; the pre-check-before-publication ordering is
 * proven by the source-order test of the acceptance flow.
 *
 * Publication order (always `task` → `intent`, and always after the
 * pre-check): the task revision manifest is published and structurally
 * verified first, then the wait-intent manifest. A task publication
 * failure or conflict never calls the intent publisher and never
 * dispatches; an intent publication failure leaves the task artifact as
 * an orphan and dispatches nothing; conflicting files are never
 * overwritten and exact retries adopt the existing bytes without changing
 * inode, mode, mtime or content.
 *
 * Dispatch capture: `sink.dispatch` is read exactly once before the
 * first await and is used through one bound local in every branch and
 * helper; any re-read of the sink member is impossible. The
 * authoritative `snapshot` is re-read after every dispatch through one
 * captured accessor local. Unexpected throwing getters propagate by
 * identity and are never sanitized by message text.
 *
 * Post-intent reconciliation is ONE unified targeted classification of
 * the authoritative snapshot against the verified `before` state, used
 * identically on the normal resolve path and the racing-error path: P1
 * (the exact intent accepted, the plan/lifecycle boundary unchanged, the
 * task ledger fully unchanged) — dispatch the task suffix; P2 (the exact
 * intent accepted, the boundary unchanged, the ledger differing only by
 * the one exact candidate record appended last) — idempotent success with
 * ZERO further dispatch, and the result carries exactly the snapshot that
 * passed the classification (no re-read of the sink between the
 * classification and the result construction); mismatch — typed
 * `invalid_state` with zero task dispatch. The task suffix is never
 * entered when the generation/iteration/plan/wait boundary changed.
 *
 * Post-dispatch verification is a full targeted boundary check (no second
 * state validator, no general deep comparator, and structurally safe:
 * every nested access is guarded by array/record shape checks, so a
 * hostile snapshot carrying `null`, a primitive or a non-record entry at
 * any viewed position yields `false` and a typed `invalid_state`, never a
 * `TypeError`; string and other primitive values are rejected the same
 * way because property reads on them never throw): the run stays waiting
 * with the wait journal pinned by length (no record appears, disappears
 * or moves), the target wait the last and only record of its index with
 * unchanged binding fields, exact ordered `{id,to}` actions, no response
 * and the exact accepted intent; the last durable plan record (position
 * and fields), the last open generation (index and identity bindings),
 * the open target iteration (the last, with the exact `open_iteration`
 * projection), the cursor shape and fields and the transition/execution
 * journal lengths stay unchanged; the task-ledger delta is checked
 * exactly (after the intent command the ledger is unchanged or differs
 * only by the exact racing candidate appended last; after the task
 * command exactly one exact candidate record is appended at its append
 * position with the exact predecessor and no duplicate or later record).
 * A dispatch that reports failure without the required durable change is
 * `invalid_state`; a racing identical dispatch is idempotent success only
 * on the exact R1/R2 progression. Unexpected throwing getters propagate
 * by identity and are never sanitized or classified by message text.
 *
 * Durability: sink `not_committed` keeps the published manifests as
 * orphans with the previous snapshot authoritative (a fresh retry adopts
 * the files and dispatches the remaining suffix); sink `durability_unknown`
 * adopts the visible candidate, poisons the sink and dispatches nothing
 * further (a fresh retry recognizes the durable prefix). Nothing is ever
 * rolled back.
 *
 * Runtime export surface (public module) is exactly
 * `PipelineV2ReviseTaskIntentControllerError` and
 * `acceptPipelineV2ReviseTaskIntent`. The internal core module exports
 * exactly `PipelineV2ReviseTaskIntentControllerError`,
 * `acceptPipelineV2ReviseTaskIntentWithIo` and `productionReviseTaskIntentOps`
 * — the reducer pre-check helper is internal and never exported. The closed
 * reason set is
 * `invalid_intent | invalid_state | intent_conflict | candidate_conflict |
 * state_persist_failed` with the last authoritative state (`null` when
 * none exists). Diagnostics are content-free (validated safe ids and
 * indexes only — no digest values, canonical JSON, paths, task bodies,
 * env values or credentials); errors are never classified from message
 * text; unexpected causes propagate unchanged.
 *
 * Not implemented (stays unwired): the revise/continue action policy,
 * the iteration closure, the wait response recording, task/plan
 * replanning, the next plan revision, opening the next iteration,
 * automatic resume, coordinator/runner/CLI wiring, schema changes,
 * migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ReviseTaskIntentControllerFailureReason =
  | "invalid_intent"
  | "invalid_state"
  | "intent_conflict"
  | "candidate_conflict"
  | "state_persist_failed";

export class PipelineV2ReviseTaskIntentControllerError extends Error {
  readonly reason: PipelineV2ReviseTaskIntentControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReviseTaskIntentControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReviseTaskIntentControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam; the production `PipelineV2RunStateSink`
 * satisfies it without an adapter. The initial `snapshot` may be `null`
 * (no durable run); after every dispatch the sink's authoritative
 * snapshot is re-read — never memoized.
 */
export interface PipelineV2ReviseTaskIntentControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface AcceptPipelineV2ReviseTaskIntentOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2ReviseTaskIntentControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly candidateTaskRevision: PreparedPipelineV2RunTaskRevision;
}

export interface AcceptedPipelineV2ReviseTaskIntent {
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_sha256: string;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call structural ops of the internal core: the plan revision
 * loader, the task revision loader and the task/intent publishers of the
 * existing run-plan store. One frozen production object binds them to the
 * public store functions; tests inject their own per-call object. There
 * is no mutable module-global seam and no installer.
 */
export interface PipelineV2ReviseTaskIntentControllerOps {
  readonly loadPlanRevision: (
    runRoot: string,
    revision: number,
  ) => Promise<PublishedPipelineV2RunPlanRevision | null>;
  readonly loadTaskRevision: (
    runRoot: string,
    taskId: string,
    revision: number,
  ) => Promise<PublishedPipelineV2RunTaskRevision | null>;
  readonly publishTaskRevision: (
    runRoot: string,
    manifest: unknown,
  ) => Promise<PublishedPipelineV2RunTaskRevision>;
  readonly publishWaitIntent: (
    runRoot: string,
    manifest: unknown,
  ) => Promise<PublishedPipelineV2RunWaitIntent>;
}

export const productionReviseTaskIntentOps: PipelineV2ReviseTaskIntentControllerOps = Object.freeze({
  loadPlanRevision: (runRoot: string, revision: number) => loadPipelineV2PlanRevision(runRoot, revision),
  loadTaskRevision: (runRoot: string, taskId: string, revision: number) =>
    loadPipelineV2TaskRevision(runRoot, taskId, revision),
  publishTaskRevision: (runRoot: string, manifest: unknown) => publishPipelineV2TaskRevision(runRoot, manifest),
  publishWaitIntent: (runRoot: string, manifest: unknown) => publishPipelineV2WaitIntent(runRoot, manifest),
});

const REVISE_TASK_ACTION_ID = "revise_task";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2ReviseTaskIntentControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskIntentControllerError {
  return new PipelineV2ReviseTaskIntentControllerError(reason, message, state);
}

function invalidIntent(message: string): PipelineV2ReviseTaskIntentControllerError {
  return controllerError("invalid_intent", message, null);
}

/**
 * The durable task record exactly matching one plan task pointer:
 * task id, revision and digest.
 */
function findDurableTaskRecord(
  state: PipelineV2RunState,
  taskId: string,
  revision: number,
  sha256: string,
): PipelineV2TaskRevisionState | undefined {
  for (const record of state.task_revisions) {
    if (record.task_id === taskId && record.revision === revision && record.sha256 === sha256) {
      return record;
    }
  }
  return undefined;
}

/**
 * The task ledger entries of one task id that sit at or beyond a given
 * revision.
 */
function taskRecordsAtOrBeyond(
  state: PipelineV2RunState,
  taskId: string,
  revision: number,
): PipelineV2TaskRevisionState[] {
  const found: PipelineV2TaskRevisionState[] = [];
  for (const record of state.task_revisions) {
    if (record.task_id === taskId && record.revision >= revision) {
      found.push(record);
    }
  }
  return found;
}

/**
 * The exact durable candidate record: task id, revision, digest, chain
 * predecessor and the wait/intent links of the open wait. The record is
 * accepted as a structurally unknown value and fails closed on a
 * non-record; every caller may hand over an untrusted entry.
 */
function taskRevisionRecordMatches(
  record: unknown,
  candidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
): boolean {
  if (!isRecord(record)) {
    return false;
  }
  const manifest = candidate.manifest;
  return (
    record["task_id"] === manifest.task_id &&
    record["revision"] === manifest.revision &&
    record["sha256"] === candidate.sha256 &&
    record["previous_sha256"] === manifest.previous_sha256 &&
    record["wait_index"] === waitIndex &&
    record["intent_sha256"] === intentSha256
  );
}

/**
 * The task ledger prefix is exact: the first `length` positions carry the
 * same record identities and contract fields (index, task id, revision,
 * digest, chain predecessor, wait and intent links). Structural
 * malformation (a non-array ledger or a non-record entry) yields `false`.
 */
function taskLedgerPrefixExact(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
  length: number,
): boolean {
  if (!Array.isArray(after.task_revisions)) {
    return false;
  }
  for (let index = 0; index < length; index += 1) {
    const left = before.task_revisions[index];
    const right = after.task_revisions[index];
    if (
      left === undefined ||
      !isRecord(right) ||
      left.index !== right.index ||
      left.task_id !== right.task_id ||
      left.revision !== right.revision ||
      left.sha256 !== right.sha256 ||
      left.previous_sha256 !== right.previous_sha256 ||
      left.wait_index !== right.wait_index ||
      left.intent_sha256 !== right.intent_sha256
    ) {
      return false;
    }
  }
  return true;
}

/**
 * The task ledger is unchanged: the same length and the exact contract
 * fields at every position.
 */
function taskLedgerUnchanged(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
): boolean {
  return (
    Array.isArray(after.task_revisions) &&
    after.task_revisions.length === before.task_revisions.length &&
    taskLedgerPrefixExact(before, after, before.task_revisions.length)
  );
}

/**
 * The exact task-ledger delta of the task command: the ledger grows by
 * exactly one record, the whole prefix is exact by positions and contract
 * fields, the appended record is the exact candidate at its append
 * position, and nothing follows or duplicates it.
 */
function taskLedgerExactAppend(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
  candidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
): boolean {
  if (
    !Array.isArray(after.task_revisions) ||
    after.task_revisions.length !== before.task_revisions.length + 1 ||
    !taskLedgerPrefixExact(before, after, before.task_revisions.length)
  ) {
    return false;
  }
  const appended = after.task_revisions[after.task_revisions.length - 1];
  if (
    appended === undefined ||
    !isRecord(appended) ||
    appended.index !== after.task_revisions.length ||
    !taskRevisionRecordMatches(appended, candidate, waitIndex, intentSha256)
  ) {
    return false;
  }
  return true;
}

/**
 * The unified reconciliation of the authoritative post-intent snapshot
 * against the verified `before` state, shared by the normal resolve path
 * and the racing-error path: P1 (the exact intent accepted, the
 * plan/lifecycle boundary unchanged, the task ledger fully unchanged) —
 * dispatch the task suffix; P2 (the exact intent accepted, the boundary
 * unchanged, the ledger differing only by the one exact candidate record
 * appended last) — idempotent success with zero further dispatch, and the
 * classification result carries exactly the snapshot that passed the
 * checks (the caller never re-reads the sink for the result); mismatch —
 * typed `invalid_state` with zero task dispatch.
 */
function classifyPostIntentState(
  before: PipelineV2RunState,
  beforeWait: PipelineV2WaitRecord,
  after: PipelineV2RunState | null,
  waitIndex: number,
  intentSha256: string,
  candidate: PreparedPipelineV2RunTaskRevision,
): { readonly kind: "p1" } | { readonly kind: "p2"; readonly state: PipelineV2RunState } | { readonly kind: "mismatch" } {
  if (after === null || !isRecord(after)) {
    return { kind: "mismatch" };
  }
  if (
    !waitBoundaryUnchangedForRevise(after, waitIndex, intentSha256, beforeWait, before.waits.length) ||
    !planBoundaryUnchanged(before, after)
  ) {
    return { kind: "mismatch" };
  }
  if (taskLedgerUnchanged(before, after)) {
    return { kind: "p1" };
  }
  if (taskLedgerExactAppend(before, after, candidate, waitIndex, intentSha256)) {
    return { kind: "p2", state: after };
  }
  return { kind: "mismatch" };
}

/**
 * The exact post-wait wait record: the wait journal pinned by length, the
 * target found exactly once by index and still the last element, binding
 * fields unchanged, no response, and the exact accepted intent digest.
 * Every viewed entry is shape-checked (`null`, primitives and non-record
 * entries fail closed) before any field access.
 */
function waitBoundaryUnchangedForRevise(
  state: PipelineV2RunState,
  waitIndex: number,
  intentSha256: string,
  before: PipelineV2WaitRecord,
  beforeWaitsLength: number,
): boolean {
  if (!isRecord(state)) {
    return false;
  }
  if (state.status !== "waiting" || state.phase !== "waiting") {
    return false;
  }
  if (!Array.isArray(state.waits) || state.waits.length !== beforeWaitsLength) {
    return false;
  }
  let matches = 0;
  let after: PipelineV2WaitRecord | undefined;
  for (const entry of state.waits) {
    if (!isRecord(entry)) {
      return false;
    }
    if (entry.index === waitIndex) {
      matches += 1;
      after = entry;
    }
  }
  if (matches !== 1 || after === undefined) {
    return false;
  }
  // The target wait stays the last element of an unchanged journal.
  const last = state.waits[state.waits.length - 1];
  if (last === undefined || last.index !== waitIndex) {
    return false;
  }
  const intentLink = after.intent as { readonly intent_sha256?: unknown } | undefined;
  if (
    before.index !== after.index ||
    before.transition_count !== after.transition_count ||
    before.state_id !== after.state_id ||
    before.reason !== after.reason ||
    before.request_sha256 !== after.request_sha256 ||
    after.response !== undefined ||
    !Array.isArray(after.actions) ||
    before.actions.length !== after.actions.length ||
    !isRecord(intentLink) ||
    intentLink.intent_sha256 !== intentSha256
  ) {
    return false;
  }
  return before.actions.every((action, position) => {
    const other = after.actions[position];
    return (
      other !== undefined &&
      isRecord(other) &&
      other.id === action.id &&
      other.to === action.to
    );
  });
}

/**
 * The plan and lifecycle boundary is unchanged: the last durable plan
 * record, the last open generation with its identity bindings, the open
 * target iteration with its exact `open_iteration` projection and the
 * cursor, transition and execution journals. Every viewed entry is
 * shape-checked (`null`, primitives and non-record entries fail closed)
 * before any field access.
 */
function planBoundaryUnchanged(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
): boolean {
  if (
    !isRecord(after) ||
    !Array.isArray(after.plan_revisions) ||
    !Array.isArray(after.generations) ||
    !isRecord(after.cursor)
  ) {
    return false;
  }
  const afterPlan = after.plan_revisions[after.plan_revisions.length - 1];
  if (
    afterPlan === undefined ||
    !isRecord(afterPlan) ||
    after.plan_revisions.length !== before.plan_revisions.length
  ) {
    return false;
  }
  const beforePlan = before.plan_revisions[before.plan_revisions.length - 1];
  if (
    beforePlan === undefined ||
    beforePlan.index !== afterPlan.index ||
    beforePlan.revision !== afterPlan.revision ||
    beforePlan.sha256 !== afterPlan.sha256 ||
    beforePlan.previous_sha256 !== afterPlan.previous_sha256 ||
    beforePlan.origin_execution !== afterPlan.origin_execution
  ) {
    return false;
  }
  const afterGeneration = after.generations[after.generations.length - 1];
  if (
    afterGeneration === undefined ||
    !isRecord(afterGeneration) ||
    afterGeneration.closed !== undefined ||
    after.generations.length !== before.generations.length
  ) {
    return false;
  }
  const beforeGeneration = before.generations[before.generations.length - 1];
  if (
    beforeGeneration === undefined ||
    afterGeneration.index !== beforeGeneration.index ||
    beforeGeneration.stage_id !== afterGeneration.stage_id ||
    beforeGeneration.stage_position !== afterGeneration.stage_position ||
    beforeGeneration.template_id !== afterGeneration.template_id ||
    beforeGeneration.plan_sha256 !== afterGeneration.plan_sha256 ||
    beforeGeneration.initial_budget !== afterGeneration.initial_budget ||
    beforeGeneration.opened_transition_count !== afterGeneration.opened_transition_count ||
    beforeGeneration.iteration_count !== afterGeneration.iteration_count ||
    !Array.isArray(afterGeneration.iterations) ||
    afterGeneration.iterations.length !== beforeGeneration.iterations.length
  ) {
    return false;
  }
  const afterIteration = afterGeneration.iterations[afterGeneration.iterations.length - 1];
  if (afterIteration === undefined || !isRecord(afterIteration)) {
    return false;
  }
  const beforeIteration = beforeGeneration.iterations[beforeGeneration.iterations.length - 1];
  const openIteration = afterGeneration.open_iteration;
  if (
    beforeIteration === undefined ||
    beforeIteration.index !== afterIteration.index ||
    beforeIteration.opened_transition_count !== afterIteration.opened_transition_count ||
    afterIteration.closed !== undefined ||
    !isRecord(openIteration) ||
    openIteration.index !== afterIteration.index ||
    openIteration.opened_transition_count !== afterIteration.opened_transition_count
  ) {
    return false;
  }
  return (
    after.cursor.current_state === before.cursor.current_state &&
    after.cursor.transition_count === before.cursor.transition_count &&
    Array.isArray(after.transitions) &&
    after.transitions.length === before.transitions.length &&
    Array.isArray(after.executions) &&
    after.executions.length === before.executions.length
  );
}

/**
 * The published wait intent must match the accepted provenance intent
 * exactly (kind, run id, wait index, canonical JSON and digest); a
 * hostile publisher result fails closed before any dispatch.
 */
function requirePublishedIntent(
  published: PublishedPipelineV2RunWaitIntent,
  intent: PreparedPipelineV2RunWaitIntent,
  state: PipelineV2RunState,
): void {
  if (
    published.intent.manifest.kind !== "revise_task_intent" ||
    published.intent.manifest.run_id !== intent.manifest.run_id ||
    published.intent.manifest.wait_index !== intent.manifest.wait_index ||
    published.intent.canonical_json !== intent.canonical_json ||
    published.intent.sha256 !== intent.sha256
  ) {
    throw controllerError(
      "invalid_state",
      "the published wait intent manifest does not match the accepted revise_task intent",
      state,
    );
  }
}

/**
 * The published task revision must match the accepted provenance
 * candidate exactly (run id, task id, revision, canonical JSON and
 * digest); a hostile publisher result fails closed before any dispatch.
 */
function requirePublishedTask(
  published: PublishedPipelineV2RunTaskRevision,
  candidate: PreparedPipelineV2RunTaskRevision,
  state: PipelineV2RunState,
): void {
  if (
    published.task.manifest.run_id !== candidate.manifest.run_id ||
    published.task.manifest.task_id !== candidate.manifest.task_id ||
    published.task.manifest.revision !== candidate.manifest.revision ||
    published.task.canonical_json !== candidate.canonical_json ||
    published.task.sha256 !== candidate.sha256
  ) {
    throw controllerError(
      "invalid_state",
      "the published task revision manifest does not match the accepted candidate",
      state,
    );
  }
}

/**
 * The reducer pre-check of the missing durable sequence on a local
 * snapshot, before any filesystem side effect; every reducer precondition
 * is already covered by the reconciliation, so this is defense-in-depth.
 * Internal to this module: not a runtime export and not a test seam (the
 * ordering is proven by the source-order test of the acceptance flow).
 */
function precheckReviseSequence(
  start: PipelineV2RunState,
  commands: readonly PipelineV2RunCommand[],
  snapshot: PipelineV2RunState,
): void {
  let local = start;
  try {
    for (const command of commands) {
      local = reducePipelineV2RunCommand(local, command, new Date());
    }
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "the current run state does not accept the revise task revision sequence",
        snapshot,
      );
    }
    throw cause;
  }
}

/**
 * Validate, bind and accept one `revise_task_intent` with its candidate
 * task revision through the existing reducer, store and binding
 * validators (see the module docstring for the full order and durability
 * semantics).
 */
export async function acceptPipelineV2ReviseTaskIntentWithIo(
  ops: PipelineV2ReviseTaskIntentControllerOps,
  options: unknown,
): Promise<AcceptedPipelineV2ReviseTaskIntent> {
  // Capture boundary: every options field is read exactly once
  // (`runRoot` → `sink` → `intent` → `candidateTaskRevision`), and the
  // sink's `poisoned`, `dispatch` and initial `snapshot` members are read
  // exactly once as opaque references. No field of the intent, of the
  // candidate or of the durable snapshot is read here.
  if (!isRecord(options)) {
    throw invalidIntent("acceptPipelineV2ReviseTaskIntent requires an options object");
  }
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const intent = options["intent"];
  const candidateTaskRevision = options["candidateTaskRevision"];
  if (typeof runRoot !== "string") {
    throw invalidIntent("acceptPipelineV2ReviseTaskIntent requires a runRoot string");
  }
  if (!isRecord(sink)) {
    throw invalidIntent("acceptPipelineV2ReviseTaskIntent requires a sink object");
  }
  if (!isRecord(intent)) {
    throw invalidIntent("acceptPipelineV2ReviseTaskIntent requires a prepared wait intent object");
  }
  if (!isRecord(candidateTaskRevision)) {
    throw invalidIntent("acceptPipelineV2ReviseTaskIntent requires a prepared candidate task revision object");
  }
  const poisoned = sink["poisoned"];
  const dispatch = sink["dispatch"];
  const initialSnapshot = sink["snapshot"];
  const sinkRef = sink as unknown as PipelineV2ReviseTaskIntentControllerSink;
  if (typeof poisoned !== "boolean") {
    throw invalidIntent("the run state sink requires a boolean poisoned flag");
  }
  if (typeof dispatch !== "function") {
    throw invalidIntent("the run state sink requires a dispatch function");
  }
  const loadPlanRevision = ops.loadPlanRevision;
  const loadTaskRevision = ops.loadTaskRevision;
  const publishTaskRevision = ops.publishTaskRevision;
  const publishWaitIntent = ops.publishWaitIntent;
  if (
    typeof loadPlanRevision !== "function" ||
    typeof loadTaskRevision !== "function" ||
    typeof publishTaskRevision !== "function" ||
    typeof publishWaitIntent !== "function"
  ) {
    throw invalidIntent("the revise task intent controller requires its loaders and publishers");
  }
  // The dispatch is captured and bound exactly once here; every branch
  // and helper uses only this bound local.
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no acceptance.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no revise task intent can be accepted",
      null,
    );
  }
  // The provenance gates: the exact registered prepared objects of the
  // manifest substrate, strictly the revise kind for the intent and the
  // task-revision kind for the candidate. Hand-built, cast, spread,
  // cloned and Proxy look-alikes are rejected here, before any field of
  // the intent, of the candidate or of the durable snapshot is read.
  if (!hasPreparedRunPlanProvenance(intent, "revise_task_intent")) {
    throw invalidIntent("the intent is not a provenance-registered revise_task_intent");
  }
  if (!hasPreparedRunPlanProvenance(candidateTaskRevision, "task_revision")) {
    throw invalidIntent("the candidate task revision is not a provenance-registered task revision");
  }
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  const preparedCandidate = candidateTaskRevision as unknown as PreparedPipelineV2RunTaskRevision;
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
  // The provenance gate above guarantees the exact revise kind; the
  // manifest is the frozen revise form from here on.
  const manifest = preparedIntent.manifest as PipelineV2ReviseTaskIntentManifest;
  // The durable boundary: the waiting run, its open wait record and its
  // declared revise_task action.
  if (state.status !== "waiting" || state.phase !== "waiting") {
    throw controllerError(
      "invalid_state",
      "the run is not waiting; a revise_task intent is accepted only inside the open wait",
      state,
    );
  }
  const wait = state.waits[state.waits.length - 1];
  if (wait === undefined || wait.response !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no open wait record; a revise_task intent is accepted only inside the open wait",
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
  if (manifest.wait_index !== wait.index) {
    throw controllerError(
      "invalid_state",
      `the wait intent names wait index ${manifest.wait_index}, but the open wait record is ${wait.index}`,
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
  // A different durable intent is a conflict before any filesystem read.
  if (wait.intent !== undefined && wait.intent.intent_sha256 !== preparedIntent.sha256) {
    throw controllerError(
      "intent_conflict",
      `the open wait ${wait.index} already accepted a different revise_task intent; one intent belongs to one wait`,
      state,
    );
  }
  // The open stage generation and its open iteration.
  const generation = state.generations[state.generations.length - 1];
  if (generation === undefined || generation.closed !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no open stage generation for the revise intent",
      state,
    );
  }
  if (generation.open_iteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the stage generation ${generation.index} carries no open iteration for the revise intent`,
      state,
    );
  }
  // The last durable plan record and the generation's plan binding.
  const lastPlanRecord = state.plan_revisions[state.plan_revisions.length - 1];
  if (lastPlanRecord === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no durable plan revision for the revise intent",
      state,
    );
  }
  if (generation.plan_sha256 !== lastPlanRecord.sha256) {
    throw controllerError(
      "invalid_state",
      `the open stage generation ${generation.index} does not belong to the last durable plan revision ${lastPlanRecord.revision}`,
      state,
    );
  }
  // The authoritative plan revision: loaded only from the durable ledger
  // through the existing run-plan store, then bound to the durable record
  // exactly.
  const loadedPlan = await loadPlanRevision(runRoot, lastPlanRecord.revision);
  if (loadedPlan === null) {
    throw controllerError(
      "invalid_state",
      `the durable plan revision ${lastPlanRecord.revision} is not published on the run's data plane`,
      state,
    );
  }
  const plan = loadedPlan.plan;
  if (
    plan.manifest.run_id !== state.run_id ||
    plan.manifest.revision !== lastPlanRecord.revision ||
    plan.sha256 !== lastPlanRecord.sha256 ||
    plan.manifest.previous_sha256 !== lastPlanRecord.previous_sha256 ||
    plan.manifest.origin_execution !== lastPlanRecord.origin_execution
  ) {
    throw controllerError(
      "invalid_state",
      `the published plan revision ${lastPlanRecord.revision} does not match the durable plan record`,
      state,
    );
  }
  // The plan-bound CURRENT task pointer: the intent's task must be a task
  // of the open generation's stage in the last accepted plan revision.
  const stage = plan.manifest.stages.find((entry) => entry.id === generation.stage_id);
  if (stage === undefined) {
    throw controllerError(
      "invalid_state",
      `the published plan revision ${lastPlanRecord.revision} does not carry the stage of the open generation`,
      state,
    );
  }
  const pointer = stage.tasks.find((entry) => entry.id === manifest.task_id);
  if (pointer === undefined) {
    throw controllerError(
      "invalid_state",
      `the stage ${JSON.stringify(generation.stage_id)} of the last accepted plan revision does not carry the task of the revise intent`,
      state,
    );
  }
  // The plan pointer must have an exact durable task record.
  const currentRecord = findDurableTaskRecord(state, pointer.id, pointer.revision, pointer.sha256);
  if (currentRecord === undefined) {
    throw controllerError(
      "invalid_state",
      `the plan's task pointer for revision ${pointer.revision} of the revise intent's task has no exact durable task record`,
      state,
    );
  }
  // The authoritative current task artifact: loaded read-only from the
  // run-plan store, then bound to the pointer and the durable record
  // exactly.
  const loadedCurrent = await loadTaskRevision(runRoot, pointer.id, pointer.revision);
  if (loadedCurrent === null) {
    throw controllerError(
      "invalid_state",
      `the durable task revision ${currentRecord.revision} of the revise intent's task is not published on the run's data plane`,
      state,
    );
  }
  const currentPrepared = loadedCurrent.task;
  if (
    currentPrepared.manifest.run_id !== state.run_id ||
    currentPrepared.manifest.task_id !== pointer.id ||
    currentPrepared.manifest.revision !== pointer.revision ||
    currentPrepared.sha256 !== pointer.sha256 ||
    currentPrepared.sha256 !== currentRecord.sha256 ||
    currentPrepared.manifest.previous_sha256 !== currentRecord.previous_sha256
  ) {
    throw controllerError(
      "invalid_state",
      `the published task revision ${currentRecord.revision} does not match the durable task record`,
      state,
    );
  }
  // The existing binding validators are the only binding authorities;
  // their errors keep their original classes.
  validateTaskRevisionChain({ previous: currentPrepared, current: preparedCandidate });
  validateReviseIntentBinding({
    intent: preparedIntent,
    candidateTaskRevision: preparedCandidate,
    currentTaskRevision: currentPrepared,
  });
  // The candidate must actually change the task body; the body itself is
  // never part of any diagnostic.
  if (preparedCandidate.manifest.body === currentPrepared.manifest.body) {
    throw controllerError(
      "invalid_intent",
      "the candidate task revision does not change the current task body",
      state,
    );
  }
  // The task ledger reconciliation of a durable candidate revision.
  const candidateRecords = taskRecordsAtOrBeyond(state, manifest.task_id, preparedCandidate.manifest.revision);
  for (const record of candidateRecords) {
    if (record.revision > preparedCandidate.manifest.revision) {
      throw controllerError(
        "candidate_conflict",
        `the task ledger already moved past revision ${preparedCandidate.manifest.revision} of the candidate task`,
        state,
      );
    }
    if (!taskRevisionRecordMatches(record, preparedCandidate, manifest.wait_index, preparedIntent.sha256)) {
      throw controllerError(
        "candidate_conflict",
        `the durable task revision ${record.revision} of the candidate task already carries different content`,
        state,
      );
    }
  }
  const candidateDurable = candidateRecords.length > 0;
  // The reconciliation classification over the durable records only.
  const intentDurable = wait.intent?.intent_sha256 === preparedIntent.sha256;
  if (candidateDurable && !intentDurable) {
    throw controllerError(
      "invalid_state",
      `the durable candidate task revision is not linked to the exact accepted intent of open wait ${manifest.wait_index}`,
      state,
    );
  }
  const intentCommand: PipelineV2RunCommand = {
    kind: "plan_intent_accepted",
    waitIndex: manifest.wait_index,
    intentSha256: preparedIntent.sha256,
  };
  const taskCommand: PipelineV2RunCommand = {
    kind: "task_revision_accepted",
    taskId: preparedCandidate.manifest.task_id,
    revision: preparedCandidate.manifest.revision,
    taskSha256: preparedCandidate.sha256,
    waitIndex: manifest.wait_index,
    intentSha256: preparedIntent.sha256,
  };
  // The reducer pre-check of the missing durable sequence precedes every
  // filesystem effect: R0 pre-checks the whole sequence, R1 only the task
  // revision, R2 needs no pre-check. A pre-check rejection is
  // `invalid_state` with zero publications and zero dispatch.
  if (!intentDurable && !candidateDurable) {
    precheckReviseSequence(state, [intentCommand, taskCommand], state);
  } else if (intentDurable && !candidateDurable) {
    precheckReviseSequence(state, [taskCommand], state);
  }
  // Publication order (always task → intent): the task revision manifest
  // first, then the wait intent manifest, each verified structurally
  // against the prepared object before any dispatch. A task publication
  // failure never calls the intent publisher and never dispatches.
  const publishedTask = await publishTaskRevision(runRoot, preparedCandidate.manifest);
  requirePublishedTask(publishedTask, preparedCandidate, state);
  const publishedIntent = await publishWaitIntent(runRoot, manifest);
  requirePublishedIntent(publishedIntent, preparedIntent, state);
  if (!intentDurable) {
    // R0: dispatch the intent command, then reconcile the authoritative
    // snapshot through the unified P1/P2/mismatch classification (shared
    // with the racing-error path).
    try {
      await dispatchCommand(intentCommand);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError(
          "state_persist_failed",
          "the revise task intent acceptance could not be confirmed durable",
          sinkRefSnapshot(sinkRef),
        );
      }
      if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          "the revise task intent acceptance could not be committed",
          sinkRefSnapshot(sinkRef),
        );
      }
      if (cause instanceof PipelineV2StateError) {
        // A racing identical dispatch is idempotent success only on the
        // exact R1/R2 progression of the same call.
        const after = sinkRefSnapshot(sinkRef);
        const classified = classifyPostIntentState(state, wait, after, manifest.wait_index, preparedIntent.sha256, preparedCandidate);
        if (classified.kind === "p1") {
          return await dispatchTaskRevisionSuffix(
            dispatchCommand,
            sinkRefSnapshot,
            sinkRef,
            preparedCandidate,
            manifest.wait_index,
            preparedIntent.sha256,
            state,
            wait,
          );
        }
        if (classified.kind === "p2") {
          // The exact snapshot that passed the classification is the
          // result; the sink is never read again between the
          // classification and the result construction.
          return deepFreezeValue({
            wait_index: manifest.wait_index,
            intent_sha256: preparedIntent.sha256,
            task_id: preparedCandidate.manifest.task_id,
            task_revision: preparedCandidate.manifest.revision,
            task_sha256: preparedCandidate.sha256,
            state: classified.state,
          });
        }
        throw controllerError(
          "invalid_state",
          `the run state rejected the revise task intent acceptance and does not carry it in the open wait ${manifest.wait_index}`,
          sinkRefSnapshot(sinkRef),
        );
      }
      throw cause;
    }
    const afterIntent = sinkRefSnapshot(sinkRef);
    const classified = classifyPostIntentState(state, wait, afterIntent, manifest.wait_index, preparedIntent.sha256, preparedCandidate);
    if (classified.kind === "p1") {
      return await dispatchTaskRevisionSuffix(
        dispatchCommand,
        sinkRefSnapshot,
        sinkRef,
        preparedCandidate,
        manifest.wait_index,
        preparedIntent.sha256,
        state,
        wait,
      );
    }
    if (classified.kind === "p2") {
      // The exact snapshot that passed the classification is the result.
      return deepFreezeValue({
        wait_index: manifest.wait_index,
        intent_sha256: preparedIntent.sha256,
        task_id: preparedCandidate.manifest.task_id,
        task_revision: preparedCandidate.manifest.revision,
        task_sha256: preparedCandidate.sha256,
        state: classified.state,
      });
    }
    throw controllerError(
      "invalid_state",
      `the committed run state does not carry the accepted revise intent in the open wait ${manifest.wait_index}`,
      afterIntent,
    );
  }
  if (!candidateDurable) {
    // R1: dispatch only the task revision.
    return await dispatchTaskRevisionSuffix(
      dispatchCommand,
      sinkRefSnapshot,
      sinkRef,
      preparedCandidate,
      manifest.wait_index,
      preparedIntent.sha256,
      state,
      wait,
    );
  }
  // R2: the exact durable intent and candidate; zero dispatch, both
  // artifacts re-published and re-verified above.
  return deepFreezeValue({
    wait_index: manifest.wait_index,
    intent_sha256: preparedIntent.sha256,
    task_id: preparedCandidate.manifest.task_id,
    task_revision: preparedCandidate.manifest.revision,
    task_sha256: preparedCandidate.sha256,
    state,
  });
}

/**
 * Reads the sink's authoritative snapshot through one captured accessor
 * local; the snapshot is never memoized.
 */
function sinkRefSnapshot(sink: PipelineV2ReviseTaskIntentControllerSink): PipelineV2RunState | null {
  return sink.snapshot;
}

/**
 * The shared task-revision suffix: dispatch the task revision command
 * through the captured dispatch local, then verify the full boundary —
 * the wait, the plan/lifecycle state and the task ledger's exact single
 * append. A racing identical dispatch is idempotent success only on the
 * exact durable record.
 */
async function dispatchTaskRevisionSuffix(
  dispatchCommand: (command: PipelineV2RunCommand) => Promise<unknown>,
  readSnapshot: (sink: PipelineV2ReviseTaskIntentControllerSink) => PipelineV2RunState | null,
  sink: PipelineV2ReviseTaskIntentControllerSink,
  preparedCandidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
  before: PipelineV2RunState,
  beforeWait: PipelineV2WaitRecord,
): Promise<AcceptedPipelineV2ReviseTaskIntent> {
  const taskCommand: PipelineV2RunCommand = {
    kind: "task_revision_accepted",
    taskId: preparedCandidate.manifest.task_id,
    revision: preparedCandidate.manifest.revision,
    taskSha256: preparedCandidate.sha256,
    waitIndex,
    intentSha256,
  };
  try {
    await dispatchCommand(taskCommand);
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "the task revision acceptance could not be confirmed durable",
        readSnapshot(sink),
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "the task revision acceptance could not be committed",
        readSnapshot(sink),
      );
    }
    if (cause instanceof PipelineV2StateError) {
      // A racing identical dispatch is idempotent success only on the
      // exact durable record with the full boundary unchanged.
      const after = readSnapshot(sink);
      if (
        after !== null &&
        waitBoundaryUnchangedForRevise(after, waitIndex, intentSha256, beforeWait, before.waits.length) &&
        planBoundaryUnchanged(before, after) &&
        taskLedgerExactAppend(before, after, preparedCandidate, waitIndex, intentSha256)
      ) {
        return deepFreezeValue({
          wait_index: waitIndex,
          intent_sha256: intentSha256,
          task_id: preparedCandidate.manifest.task_id,
          task_revision: preparedCandidate.manifest.revision,
          task_sha256: preparedCandidate.sha256,
          state: after,
        });
      }
      throw controllerError(
        "invalid_state",
        `the run state rejected the task revision acceptance and does not carry it for task revision ${preparedCandidate.manifest.revision}`,
        readSnapshot(sink),
      );
    }
    throw cause;
  }
  const after = readSnapshot(sink);
  if (
    after === null ||
    !waitBoundaryUnchangedForRevise(after, waitIndex, intentSha256, beforeWait, before.waits.length) ||
    !planBoundaryUnchanged(before, after) ||
    !taskLedgerExactAppend(before, after, preparedCandidate, waitIndex, intentSha256)
  ) {
    throw controllerError(
      "invalid_state",
      `the committed run state does not carry the accepted task revision ${preparedCandidate.manifest.revision} of the candidate task`,
      after,
    );
  }
  return deepFreezeValue({
    wait_index: waitIndex,
    intent_sha256: intentSha256,
    task_id: preparedCandidate.manifest.task_id,
    task_revision: preparedCandidate.manifest.revision,
    task_sha256: preparedCandidate.sha256,
    state: after,
  });
}
