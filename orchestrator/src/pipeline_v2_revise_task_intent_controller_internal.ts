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
  loadPipelineV2TaskRevision,
  publishPipelineV2TaskRevision,
  publishPipelineV2WaitIntent,
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
 * publishes the immutable wait-intent and task-revision manifests through
 * the existing run-plan filesystem store, and durably records the exact
 * sequence `plan_intent_accepted` → `task_revision_accepted` through the
 * structural sink. The increment ends at the accepted task revision: the
 * controller never closes the iteration, never records a wait response,
 * never creates a plan revision and never resumes the run.
 *
 * Binding chain (fail-closed, before any filesystem effect): the waiting
 * run, its open wait record and its declared `revise_task` action; the
 * intent's run id and wait index against the durable record; a different
 * already-accepted intent digest is an `intent_conflict` with zero writes
 * and zero dispatch. The CURRENT task revision is taken only from the
 * durable ledger (the latest record for the intent's task id — a missing
 * current revision is `invalid_state`), its immutable manifest is loaded
 * read-only through the existing run-plan store and must match the
 * durable record exactly (run, task, revision, digest), and the chain and
 * revise binding validators are the only binding authorities: the
 * candidate must be the exact successor of the current revision
 * (`validateTaskRevisionChain`) and the intent's two digests must name
 * the current and candidate digests exactly with the candidate origin
 * `user_response` (`validateReviseIntentBinding`). Binding, manifest and
 * store errors keep their original classes and identity.
 *
 * Validation order: the options shape; every options field read exactly
 * once (`runRoot` → `sink` → `intent` → `candidateTaskRevision`); the
 * sink's `poisoned`, `dispatch` and initial `snapshot` members captured
 * exactly once as opaque references with `dispatch` bound to the sink
 * before the first await; the per-call ops getters read exactly once; the
 * poisoned-sink latch; the intent provenance gate and then the candidate
 * provenance gate (registry lookups — hand-built, cast, spread,
 * `structuredClone` and Proxy look-alikes are rejected before any field
 * of the intent, the candidate or the durable snapshot is read, Proxy
 * traps never invoked); only then the single `validatePipelineV2RunState`
 * of the durable snapshot and the durable bindings. A hostile extra
 * options field is ignored.
 *
 * Reconciliation: a wait without a durable intent is pre-checked (the
 * whole missing reducer sequence on a local snapshot — a rejection is
 * `invalid_state` with zero filesystem effects), then the wait-intent
 * manifest and the task-revision manifest are published in that order
 * (each published result is verified structurally against the prepared
 * object before any dispatch; store errors keep their class), then the
 * two commands are dispatched strictly in order with the authoritative
 * sink snapshot re-read and structurally verified after each dispatch.
 * An exact durable intent is an idempotent retry: the intent publication
 * is re-verified or restored (no second `plan_intent_accepted`), and the
 * task revision branch decides — a missing revision is published
 * (idempotent adoption) and dispatched once, the exact durable record is
 * a zero-dispatch success, a different digest at the same revision or a
 * durable ledger that moved past the candidate is a `candidate_conflict`.
 * A racing identical dispatch is idempotent success only on the exact
 * durable record; a dispatch that reports failure without any durable
 * effect is `invalid_state`. Sink `not_committed` keeps the published
 * manifests as orphans with the previous snapshot authoritative; sink
 * `durability_unknown` adopts the visible candidate, poisons the sink and
 * dispatches nothing further. Nothing is ever rolled back.
 *
 * Runtime export surface (public module) is exactly
 * `PipelineV2ReviseTaskIntentControllerError` and
 * `acceptPipelineV2ReviseTaskIntent`. The closed reason set is
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
 * The per-call structural ops of the internal core: the task revision
 * loader and the task/intent publishers of the existing run-plan store.
 * One frozen production object binds them to the public store functions;
 * tests inject their own per-call object. There is no mutable
 * module-global seam and no installer.
 */
export interface PipelineV2ReviseTaskIntentControllerOps {
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
 * The latest durable task revision record for one task id, read from the
 * append-only ledger.
 */
function latestTaskRevisionRecord(
  state: PipelineV2RunState,
  taskId: string,
): PipelineV2TaskRevisionState | undefined {
  for (let index = state.task_revisions.length - 1; index >= 0; index -= 1) {
    const record = state.task_revisions[index];
    if (record !== undefined && record.task_id === taskId) {
      return record;
    }
  }
  return undefined;
}

/**
 * The exact durable task revision record of the accepted candidate:
 * identity, digest, chain predecessor and the wait/intent links of the
 * open wait.
 */
function taskRevisionRecordMatches(
  record: PipelineV2TaskRevisionState,
  candidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
): boolean {
  return (
    record.task_id === candidate.manifest.task_id &&
    record.revision === candidate.manifest.revision &&
    record.sha256 === candidate.sha256 &&
    record.previous_sha256 === candidate.manifest.previous_sha256 &&
    record.wait_index === waitIndex &&
    record.intent_sha256 === intentSha256
  );
}

/**
 * The durable authoritative state after the task revision dispatch must
 * carry exactly the accepted record at the end of the ledger.
 */
function stateCarriesTaskRevision(
  state: PipelineV2RunState | null,
  candidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
): boolean {
  if (state === null) {
    return false;
  }
  const record = latestTaskRevisionRecord(state, candidate.manifest.task_id);
  return record !== undefined && taskRevisionRecordMatches(record, candidate, waitIndex, intentSha256);
}

/**
 * The open wait record after the intent dispatch must carry exactly the
 * accepted intent digest with its binding fields unchanged.
 */
function waitCarriesIntent(
  state: PipelineV2RunState | null,
  waitIndex: number,
  intentSha256: string,
  before: PipelineV2WaitRecord,
): boolean {
  if (state === null) {
    return false;
  }
  let after: PipelineV2WaitRecord | undefined;
  for (const record of state.waits) {
    if (record.index === waitIndex) {
      after = record;
    }
  }
  if (after === undefined) {
    return false;
  }
  if (after.intent?.intent_sha256 !== intentSha256) {
    return false;
  }
  if (
    before.index !== after.index ||
    before.transition_count !== after.transition_count ||
    before.state_id !== after.state_id ||
    before.reason !== after.reason ||
    before.request_sha256 !== after.request_sha256 ||
    after.response !== undefined ||
    before.actions.length !== after.actions.length
  ) {
    return false;
  }
  return before.actions.every((action, position) => {
    const other = after.actions[position];
    return other !== undefined && other.id === action.id && other.to === action.to;
  });
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
 * The reducer pre-check of the whole missing durable sequence on a local
 * snapshot, before any filesystem side effect; every reducer precondition
 * is already covered by the reconciliation, so this is defense-in-depth.
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
  if (typeof poisoned !== "boolean") {
    throw invalidIntent("the run state sink requires a boolean poisoned flag");
  }
  if (typeof dispatch !== "function") {
    throw invalidIntent("the run state sink requires a dispatch function");
  }
  const loadTaskRevision = ops.loadTaskRevision;
  const publishTaskRevision = ops.publishTaskRevision;
  const publishWaitIntent = ops.publishWaitIntent;
  if (typeof loadTaskRevision !== "function" || typeof publishTaskRevision !== "function" || typeof publishWaitIntent !== "function") {
    throw invalidIntent("the revise task intent controller requires its task loader and publishers");
  }
  const sinkRef = sink as unknown as PipelineV2ReviseTaskIntentControllerSink;
  // The dispatch is bound to the sink immediately at capture: a later
  // reassignment of the sink's member cannot change the dispatch target.
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
  // The current task revision comes only from the durable ledger.
  const currentRecord = latestTaskRevisionRecord(state, manifest.task_id);
  if (currentRecord === undefined) {
    throw controllerError(
      "invalid_state",
      `the run has no durable task revision for the revise intent's task`,
      state,
    );
  }
  // The task-revision reconciliation of an already durable candidate
  // revision runs before the current artifact load: a ledger that already
  // carries the candidate revision (or moved past it) is decided by the
  // durable records alone.
  const existingRecord = currentRecord;
  if (existingRecord.revision === preparedCandidate.manifest.revision) {
    if (taskRevisionRecordMatches(existingRecord, preparedCandidate, manifest.wait_index, preparedIntent.sha256)) {
      return deepFreezeValue({
        wait_index: manifest.wait_index,
        intent_sha256: preparedIntent.sha256,
        task_id: preparedCandidate.manifest.task_id,
        task_revision: preparedCandidate.manifest.revision,
        task_sha256: preparedCandidate.sha256,
        state,
      });
    }
    throw controllerError(
      "candidate_conflict",
      `the durable task revision ${existingRecord.revision} of the candidate task already carries different content`,
      state,
    );
  }
  if (existingRecord.revision > preparedCandidate.manifest.revision) {
    throw controllerError(
      "candidate_conflict",
      `the task ledger already moved past revision ${preparedCandidate.manifest.revision} of the candidate task`,
      state,
    );
  }
  // The authoritative current task revision: loaded read-only from the
  // run-plan store, then bound to the durable record exactly.
  const loadedCurrent = await loadTaskRevision(runRoot, manifest.task_id, currentRecord.revision);
  if (loadedCurrent === null) {
    throw controllerError(
      "invalid_state",
      `the durable task revision ${currentRecord.revision} is not published on the run's data plane`,
      state,
    );
  }
  const currentPrepared = loadedCurrent.task;
  if (
    currentPrepared.manifest.run_id !== state.run_id ||
    currentPrepared.manifest.task_id !== manifest.task_id ||
    currentPrepared.manifest.revision !== currentRecord.revision ||
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
  validateReviseIntentBinding({ intent: preparedIntent, candidateTaskRevision: preparedCandidate, currentTaskRevision: currentPrepared });
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
  if (wait.intent === undefined) {
    // The reducer pre-check of the whole missing sequence on the local
    // snapshot, before any filesystem side effect.
    precheckReviseSequence(state, [intentCommand, taskCommand], state);
    // Publish the immutable manifests in the durable order (the intent
    // manifest, then the task revision manifest); store errors keep
    // their original class and identity (a conflict leaves nothing
    // behind).
    const publishedIntent = await publishWaitIntent(runRoot, manifest);
    requirePublishedIntent(publishedIntent, preparedIntent, state);
    const publishedTask = await publishTaskRevision(runRoot, preparedCandidate.manifest);
    requirePublishedTask(publishedTask, preparedCandidate, state);
    // The durable dispatches, strictly in order, each followed by the
    // authoritative verification.
    try {
      await dispatchCommand(intentCommand);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError(
          "state_persist_failed",
          "the revise task intent acceptance could not be confirmed durable",
          sinkRef.snapshot,
        );
      }
      if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          "the revise task intent acceptance could not be committed",
          sinkRef.snapshot,
        );
      }
      if (cause instanceof PipelineV2StateError) {
        // A racing identical dispatch is idempotent success only on the
        // exact durable record.
        const after = sinkRef.snapshot;
        if (after !== null && waitCarriesIntent(after, manifest.wait_index, preparedIntent.sha256, wait)) {
          return await acceptAfterIntentDurable(
            publishTaskRevision,
            runRoot,
            sinkRef,
            preparedIntent,
            preparedCandidate,
            manifest.wait_index,
            preparedIntent.sha256,
            after,
          );
        }
        throw controllerError(
          "invalid_state",
          `the run state rejected the revise task intent acceptance and does not carry it in the open wait ${manifest.wait_index}`,
          after,
        );
      }
      throw cause;
    }
    const afterIntent = sinkRef.snapshot;
    if (afterIntent === null || !waitCarriesIntent(afterIntent, manifest.wait_index, preparedIntent.sha256, wait)) {
      throw controllerError(
        "invalid_state",
        `the committed run state does not carry the accepted revise intent in the open wait ${manifest.wait_index}`,
        afterIntent,
      );
    }
    return await acceptAfterIntentDurable(
      publishTaskRevision,
      runRoot,
      sinkRef,
      preparedIntent,
      preparedCandidate,
      manifest.wait_index,
      preparedIntent.sha256,
      afterIntent,
    );
  }
  // The exact durable intent: idempotent retry. The intent publication is
  // re-verified or restored; no second `plan_intent_accepted` happens.
  const publishedIntent = await publishWaitIntent(runRoot, manifest);
  requirePublishedIntent(publishedIntent, preparedIntent, state);
  return await acceptAfterIntentDurable(
    publishTaskRevision,
    runRoot,
    sinkRef,
    preparedIntent,
    preparedCandidate,
    manifest.wait_index,
    preparedIntent.sha256,
    state,
  );
}

/**
 * The shared task-revision suffix of the acceptance: the durable state
 * after the intent acceptance (or on the exact durable intent retry) is
 * reconciled against the candidate task revision — missing revisions are
 * published (idempotent adoption) and dispatched once, the exact durable
 * record is a zero-dispatch success, and anything else is a typed
 * conflict.
 */
async function acceptAfterIntentDurable(
  publishTaskRevision: PipelineV2ReviseTaskIntentControllerOps["publishTaskRevision"],
  runRoot: string,
  sinkRef: PipelineV2ReviseTaskIntentControllerSink,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
  preparedCandidate: PreparedPipelineV2RunTaskRevision,
  waitIndex: number,
  intentSha256: string,
  state: PipelineV2RunState,
): Promise<AcceptedPipelineV2ReviseTaskIntent> {
  const taskCommand: PipelineV2RunCommand = {
    kind: "task_revision_accepted",
    taskId: preparedCandidate.manifest.task_id,
    revision: preparedCandidate.manifest.revision,
    taskSha256: preparedCandidate.sha256,
    waitIndex,
    intentSha256,
  };
  const record = latestTaskRevisionRecord(state, preparedCandidate.manifest.task_id);
  if (record !== undefined) {
    if (record.revision > preparedCandidate.manifest.revision) {
      throw controllerError(
        "candidate_conflict",
        `the task ledger already moved past revision ${preparedCandidate.manifest.revision} of the candidate task`,
        state,
      );
    }
    if (record.revision === preparedCandidate.manifest.revision) {
      if (taskRevisionRecordMatches(record, preparedCandidate, waitIndex, intentSha256)) {
        return deepFreezeValue({
          wait_index: waitIndex,
          intent_sha256: intentSha256,
          task_id: preparedCandidate.manifest.task_id,
          task_revision: preparedCandidate.manifest.revision,
          task_sha256: preparedCandidate.sha256,
          state,
        });
      }
      throw controllerError(
        "candidate_conflict",
        `the durable task revision ${record.revision} of the candidate task already carries different content`,
        state,
      );
    }
  }
  // The exact retry adoption of the task manifest, then the single
  // durable dispatch.
  const publishedTask = await publishTaskRevision(runRoot, preparedCandidate.manifest);
  requirePublishedTask(publishedTask, preparedCandidate, state);
  try {
    await Promise.resolve(
      (sinkRef.dispatch as (...args: unknown[]) => unknown).call(sinkRef, taskCommand),
    );
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "the task revision acceptance could not be confirmed durable",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "the task revision acceptance could not be committed",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2StateError) {
      const after = sinkRef.snapshot;
      if (after !== null && stateCarriesTaskRevision(after, preparedCandidate, waitIndex, intentSha256)) {
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
        after,
      );
    }
    throw cause;
  }
  const after = sinkRef.snapshot;
  if (after === null || !stateCarriesTaskRevision(after, preparedCandidate, waitIndex, intentSha256)) {
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
