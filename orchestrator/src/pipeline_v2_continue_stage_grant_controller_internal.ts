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
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import type { PreparedPipelineV2RunWaitIntent } from "./pipeline_v2_run_plan_manifests.ts";
import type { PipelineV2ContinueStageIntentManifest } from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral application controller for the continue-stage
 * iteration grant (unwired).
 *
 * The controller is the durable step that follows
 * `acceptPipelineV2ContinueStageIntent`: for an already durably accepted
 * `continue_stage_intent` it records the iteration grant and closes the
 * current stage iteration with the wait-bound grant closure. It performs
 * no filesystem work and never republishes the intent or calls the
 * acceptance controller — the plan binding was already proven before the
 * durable `plan_intent_accepted`, so this controller re-checks only the
 * durable bindings (the waiting run, the open wait with the accepted
 * exact intent digest and the declared `continue_stage` action, the open
 * generation bound to the intent's stage, the generation's target
 * iteration, and the positive `additional_iterations` of the provenance
 * manifest).
 *
 * Capture order (fail-closed): the options shape; the fields `sink` →
 * `intent` read exactly once; the sink's `poisoned`, `dispatch` and
 * initial `snapshot` members captured exactly once as opaque references
 * with `dispatch` bound to the sink before the first await; the poison
 * latch; the intent provenance gate (the shared manifest registry —
 * hand-built, cast, spread, `structuredClone` and Proxy look-alikes are
 * rejected before any field of the intent or of the durable snapshot is
 * read, Proxy traps never invoked); strictly the `continue_stage_intent`
 * kind; only then the single `validatePipelineV2RunState` (a missing or
 * invalid snapshot is the controller's own typed `invalid_state` with a
 * fixed content-free diagnostic and `state: null`; unexpected causes
 * propagate unchanged) and the durable bindings. A hostile extra options
 * field is ignored.
 *
 * Reconciliation over the durable records only:
 * - S0: no grant for `(generation, wait)` and the target iteration open —
 *   the whole suffix (`iteration_grant_recorded` →
 *   `stage_iteration_closed {by: "grant", waitIndex}`) is pre-checked
 *   through the single reducer on a local sequence before the first
 *   dispatch, then dispatched strictly grant → closure with the
 *   authoritative snapshot re-read and structurally verified after each
 *   dispatch;
 * - S1: the exact durable grant (generation index, wait index, intent
 *   digest, additional iterations) with the target iteration still open —
 *   only the closure is pre-checked and dispatched;
 * - S2: the exact durable grant plus the exact grant closure
 *   (`by: "grant"`, the wait index, `closed_transition_count` equal to the
 *   wait's `transition_count`, no `open_iteration`) — zero dispatch, the
 *   authoritative state returned;
 * - S2 on the active/answered run: after `wait_response_recorded` the run
 *   is active again, so an already answered target wait is recognized
 *   ONLY as the exact completed S2 retry — zero dispatch, no state
 *   restoration: the target wait must be the last wait and must keep the
 *   exact accepted intent digest, its response must carry exactly the
 *   `continue_stage` action id, exactly one exact grant record must exist
 *   for the wait, the grant's generation must remain the last open
 *   generation bound to the intent's stage, and the generation's last
 *   iteration must carry the exact grant closure with the wait's anchor
 *   and no `open_iteration`. Later lifecycle progress (an open next
 *   iteration, a closed or foreign generation, another wait) is a typed
 *   failure, never a retry of this boundary;
 * - conflicts: an existing grant with a different digest or amount is
 *   `grant_conflict`; a differently closed target iteration (another
 *   close reason, another wait index or another anchor) is
 *   `lifecycle_conflict`; a generation/iteration that does not match the
 *   intent/wait is `lifecycle_conflict`; a closure without the exact
 *   durable grant and several matching grants or otherwise impossible
 *   ledger forms are `invalid_state`. Nothing is ever rewritten or
 *   repaired.
 *
 * The authoritative verification after every dispatch is complete, and no
 * partial match is ever a success: the target wait record exists exactly
 * once and keeps every binding field (index, transition count, state id,
 * reason, request digest, ordered actions) with no response and exactly
 * the accepted intent digest; the target generation exists at its exact
 * position, remains the last one, stays open with unchanged identity
 * bindings (stage, position, template, plan digest, budget, opening
 * anchor); the target iteration remains the generation's last iteration
 * — open after the grant, with the `open_iteration` projection
 * referencing it exactly with its opening anchor, or carrying exactly the
 * grant closure after the closure dispatch; and the exact grant record is
 * present exactly once. A removed or replaced wait intent is
 * `invalid_state` identically on the resolve path and on the racing
 * dispatch path; a closed or substituted generation is classified
 * uniformly through the same race-or-mismatch mapping.
 *
 * Durability: a sink `not_committed` keeps the previous snapshot
 * authoritative (a fresh retry dispatches the failed command again — the
 * exact durable prefix is recognized); a sink `durability_unknown` adopts
 * the visible candidate, poisons the sink and dispatches nothing further
 * (a fresh sink recognizes the durable prefix and dispatches only the
 * remaining suffix or nothing at all). Nothing is ever rolled back.
 *
 * Runtime export surface (public module) is exactly
 * `PipelineV2ContinueStageGrantControllerError` and
 * `applyPipelineV2ContinueStageGrant`. Diagnostics are content-free
 * (validated safe ids and indexes only); errors are never classified from
 * message text; unexpected causes propagate unchanged.
 *
 * Not implemented (stays unwired): the response manifest and
 * `wait_response_recorded`, opening the next iteration, automatic resume,
 * `revise_task_intent`, the task/plan revision replanning chain, the
 * continue/revise action policy, coordinator/runner/CLI wiring, schema
 * changes, migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ContinueStageGrantControllerFailureReason =
  | "invalid_intent"
  | "invalid_state"
  | "grant_conflict"
  | "lifecycle_conflict"
  | "state_persist_failed";

export class PipelineV2ContinueStageGrantControllerError extends Error {
  readonly reason: PipelineV2ContinueStageGrantControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinueStageGrantControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinueStageGrantControllerError";
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
export interface PipelineV2ContinueStageGrantControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface ApplyPipelineV2ContinueStageGrantOptions {
  readonly sink: PipelineV2ContinueStageGrantControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

export interface AppliedPipelineV2ContinueStageGrant {
  readonly wait_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly additional_iterations: number;
  readonly intent_sha256: string;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2ContinueStageGrantControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageGrantControllerError {
  return new PipelineV2ContinueStageGrantControllerError(reason, message, state);
}

function invalidIntent(message: string): PipelineV2ContinueStageGrantControllerError {
  return controllerError("invalid_intent", message, null);
}

const CONTINUE_STAGE_ACTION_ID = "continue_stage";

interface GrantBindings {
  readonly manifest: PipelineV2ContinueStageIntentManifest;
  readonly wait: PipelineV2WaitRecord;
  readonly generation: PipelineV2StageGenerationRecord;
  readonly iteration: PipelineV2StageIterationRecord;
}

/**
 * The durable bindings shared by the flow and its error-contract tests:
 * the waiting run, the open wait with the accepted exact intent, the
 * declared `continue_stage` action, the open generation bound to the
 * intent's stage, and the generation's target (last) iteration.
 */
export function requireContinueStageGrantBindings(
  state: PipelineV2RunState,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): GrantBindings {
  const manifest = preparedIntent.manifest as PipelineV2ContinueStageIntentManifest;
  if (state.status !== "waiting" || state.phase !== "waiting") {
    throw controllerError(
      "invalid_state",
      "the run is not waiting; an iteration grant is applied only inside the open wait",
      state,
    );
  }
  const wait = state.waits[state.waits.length - 1];
  if (wait === undefined || wait.response !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no open wait record; an iteration grant is applied only inside the open wait",
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
  if (!wait.actions.some((action) => action.id === CONTINUE_STAGE_ACTION_ID)) {
    throw controllerError(
      "invalid_state",
      "the open wait does not declare the continue_stage action",
      state,
    );
  }
  if (wait.intent === undefined) {
    throw controllerError(
      "invalid_state",
      `the open wait ${wait.index} has not accepted an intent; record it before applying the grant`,
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
  const generation = state.generations[state.generations.length - 1];
  if (generation === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no stage generation for the grant",
      state,
    );
  }
  if (generation.closed !== undefined) {
    throw controllerError(
      "invalid_state",
      `the last stage generation ${generation.index} is closed; a grant is applied only inside an open generation`,
      state,
    );
  }
  if (generation.stage_id !== manifest.stage_id) {
    throw controllerError(
      "lifecycle_conflict",
      `the open stage generation ${generation.index} belongs to stage ${JSON.stringify(generation.stage_id)}, but the wait intent names stage ${JSON.stringify(manifest.stage_id)}`,
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
  return { manifest, wait, generation, iteration };
}

/**
 * The grant ledger records of one `(generation, wait)` pair.
 */
function grantRecordsFor(
  state: PipelineV2RunState,
  generationIndex: number,
  waitIndex: number,
): readonly (typeof state.grants)[number][] {
  return state.grants.filter(
    (grant) => grant.generation_index === generationIndex && grant.wait_index === waitIndex,
  );
}

function exactGrantPresent(
  state: PipelineV2RunState,
  generationIndex: number,
  waitIndex: number,
  intentSha256: string,
  additionalIterations: number,
): boolean {
  const matching = grantRecordsFor(state, generationIndex, waitIndex);
  if (matching.length !== 1) {
    return false;
  }
  const grant = matching[0]!;
  return grant.intent_sha256 === intentSha256 && grant.additional_iterations === additionalIterations;
}

/**
 * The contract-owned binding fields of the target generation after a
 * dispatch: the generation exists at its exact position, remains the last
 * one, stays open, and carries unchanged identity bindings.
 */
function targetGenerationBindingMatches(
  after: PipelineV2RunState,
  before: PipelineV2StageGenerationRecord,
): boolean {
  const generation = after.generations[before.index - 1];
  if (generation === undefined || generation.index !== before.index) {
    return false;
  }
  if (after.generations.length !== before.index) {
    return false;
  }
  if (generation.closed !== undefined) {
    return false;
  }
  return (
    generation.stage_id === before.stage_id &&
    generation.stage_position === before.stage_position &&
    generation.template_id === before.template_id &&
    generation.plan_sha256 === before.plan_sha256 &&
    generation.initial_budget === before.initial_budget &&
    generation.opened_transition_count === before.opened_transition_count
  );
}

/**
 * The target iteration after the grant dispatch: still the last
 * iteration of the generation, still open, with its opening anchor
 * unchanged and the `open_iteration` projection referencing it exactly.
 */
function targetIterationOpenMatches(
  after: PipelineV2RunState,
  before: PipelineV2StageGenerationRecord,
  iteration: PipelineV2StageIterationRecord,
): boolean {
  const generation = after.generations[before.index - 1];
  if (generation === undefined) {
    return false;
  }
  const last = generation.iterations[generation.iterations.length - 1];
  if (last === undefined || last.index !== iteration.index || last.closed !== undefined) {
    return false;
  }
  if (last.opened_transition_count !== iteration.opened_transition_count) {
    return false;
  }
  const open = generation.open_iteration;
  return open !== undefined && open.index === iteration.index && open.opened_transition_count === iteration.opened_transition_count;
}

function exactGrantClosurePresent(
  state: PipelineV2RunState,
  generationIndex: number,
  iterationIndex: number,
  iterationOpenedTransitionCount: number,
  waitIndex: number,
  waitTransitionCount: number,
): boolean {
  const generation = state.generations[generationIndex - 1];
  if (generation === undefined || generation.closed !== undefined || generation.open_iteration !== undefined) {
    return false;
  }
  const last = generation.iterations[generation.iterations.length - 1];
  if (last === undefined || last.index !== iterationIndex || last.opened_transition_count !== iterationOpenedTransitionCount) {
    return false;
  }
  const closed = last.closed;
  if (closed === undefined) {
    return false;
  }
  return closed.by === "grant" && closed.wait_index === waitIndex && closed.closed_transition_count === waitTransitionCount;
}

/**
 * The binding fields of one wait record that a grant application must not
 * change: everything except the exact accepted intent, which must remain
 * present with exactly the accepted digest. The grant changes only the
 * ledgers, never the wait record's bindings.
 */
function waitBindingMatches(before: PipelineV2WaitRecord, after: PipelineV2WaitRecord, intentSha256: string): boolean {
  if (
    before.index !== after.index ||
    before.transition_count !== after.transition_count ||
    before.state_id !== after.state_id ||
    before.reason !== after.reason ||
    before.request_sha256 !== after.request_sha256 ||
    after.response !== undefined ||
    after.intent?.intent_sha256 !== intentSha256 ||
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
 * The target wait record must exist exactly once in the journal; a
 * duplicated wait index is never a valid verification target.
 */
function findWaitRecord(state: PipelineV2RunState, waitIndex: number): PipelineV2WaitRecord | undefined {
  let found: PipelineV2WaitRecord | undefined;
  let count = 0;
  for (const record of state.waits) {
    if (record.index === waitIndex) {
      found = record;
      count += 1;
    }
  }
  return count === 1 ? found : undefined;
}

/**
 * The reducer pre-check of a command suffix on a local sequence, before
 * any dispatch: a reducer rejection is a typed `invalid_state` with zero
 * dispatch; any other cause propagates unchanged. Through loader-valid
 * states the reconciliation already proves every reducer precondition of
 * the covered suffixes, so this is defense-in-depth; the helper is the
 * single pre-check path of both the flow and its error-contract test.
 */
export function precheckGrantSequence(
  state: PipelineV2RunState,
  commands: readonly PipelineV2RunCommand[],
  snapshot: PipelineV2RunState,
): void {
  let checked = state;
  for (const command of commands) {
    try {
      checked = reducePipelineV2RunCommand(checked, command, new Date());
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        throw controllerError(
          "invalid_state",
          "the current run state does not accept the grant sequence",
          snapshot,
        );
      }
      throw cause;
    }
  }
}

/**
 * The exact completed S2 retry on the active/answered run: the grant and
 * the grant-bound closure are already durable, the target wait is
 * answered with the `continue_stage` action, and nothing is dispatched or
 * restored. The recognition requires the target wait to be the last wait,
 * the response to carry the `continue_stage` action id (the response
 * digest's structural validity is guaranteed by the state validation
 * above), the wait to keep the exact accepted intent digest, exactly one
 * exact grant record for the wait, the grant's generation to be the last
 * open generation bound to the intent's stage, the generation's last
 * iteration to carry the exact grant closure with the wait's anchor, and
 * no `open_iteration`. Later lifecycle progress (an open next iteration,
 * a closed or foreign generation, or another wait) is a typed failure,
 * never a retry of this boundary.
 */
function applyCompletedGrantRetry(
  state: PipelineV2RunState,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): AppliedPipelineV2ContinueStageGrant {
  const manifest = preparedIntent.manifest as PipelineV2ContinueStageIntentManifest;
  const wait = state.waits[state.waits.length - 1];
  if (wait === undefined) {
    throw controllerError(
      "invalid_state",
      "the run records no wait to recognize the completed continue-stage boundary",
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
      `the wait intent names wait index ${manifest.wait_index}, but the last wait record is ${wait.index}`,
      state,
    );
  }
  if (!wait.actions.some((action) => action.id === CONTINUE_STAGE_ACTION_ID)) {
    throw controllerError(
      "invalid_state",
      "the last wait record does not declare the continue_stage action",
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
  if (response.action_id !== CONTINUE_STAGE_ACTION_ID) {
    throw controllerError(
      "lifecycle_conflict",
      `the last wait ${wait.index} was answered with another action; this is not the continue-stage completion boundary`,
      state,
    );
  }
  // The exact durable grant of this wait, exactly once.
  const grantsOfWait = state.grants.filter((grant) => grant.wait_index === wait.index);
  if (grantsOfWait.length > 1) {
    throw controllerError(
      "invalid_state",
      `wait ${wait.index} carries several grant records; the completed boundary is not a recognizable retry`,
      state,
    );
  }
  const grant = grantsOfWait[0];
  if (grant === undefined) {
    throw controllerError(
      "invalid_state",
      `wait ${wait.index} carries no durable grant; the completed boundary is not a recognizable retry`,
      state,
    );
  }
  if (grant.intent_sha256 !== preparedIntent.sha256 || grant.additional_iterations !== manifest.additional_iterations) {
    throw controllerError(
      "grant_conflict",
      `wait ${wait.index} already carries a different grant for generation ${grant.generation_index}`,
      state,
    );
  }
  // The grant's generation: the last open generation bound to the
  // intent's stage.
  const generation = state.generations[grant.generation_index - 1];
  if (generation === undefined || generation.index !== grant.generation_index) {
    throw controllerError(
      "invalid_state",
      `the durable grant names generation ${grant.generation_index}, which the run does not record`,
      state,
    );
  }
  if (state.generations.length !== grant.generation_index) {
    throw controllerError(
      "lifecycle_conflict",
      `the generation ${grant.generation_index} of the completed boundary is no longer the last generation`,
      state,
    );
  }
  if (generation.closed !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the generation ${grant.generation_index} of the completed boundary is closed`,
      state,
    );
  }
  if (generation.stage_id !== manifest.stage_id) {
    throw controllerError(
      "lifecycle_conflict",
      `the generation ${generation.index} belongs to stage ${JSON.stringify(generation.stage_id)}, but the wait intent names stage ${JSON.stringify(manifest.stage_id)}`,
      state,
    );
  }
  // The generation's last iteration carries the exact grant closure.
  const iteration = generation.iterations[generation.iterations.length - 1];
  if (iteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the generation ${generation.index} records no iterations`,
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
  if (closed.by !== "grant") {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} was closed with ${JSON.stringify(closed.by)}, not by the grant`,
      state,
    );
  }
  if (closed.wait_index !== wait.index || closed.closed_transition_count !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} was closed against another wait or another boundary`,
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
    generation_index: grant.generation_index,
    iteration_index: iteration.index,
    additional_iterations: manifest.additional_iterations,
    intent_sha256: preparedIntent.sha256,
    state,
  });
}

/**
 * Validate, bind and apply the continue-stage grant through the existing
 * reducer (see the module docstring for the full order and durability
 * semantics).
 */
export async function applyPipelineV2ContinueStageGrantInternal(
  options: unknown,
): Promise<AppliedPipelineV2ContinueStageGrant> {
  // Capture boundary: every options field is read exactly once (`sink` →
  // `intent`), and the sink's `poisoned`, `dispatch` and initial
  // `snapshot` members are read exactly once as opaque references. No
  // field of the intent or of the durable snapshot is read here.
  if (!isRecord(options)) {
    throw invalidIntent("applyPipelineV2ContinueStageGrant requires an options object");
  }
  const sink = options["sink"];
  const intent = options["intent"];
  if (!isRecord(sink)) {
    throw invalidIntent("applyPipelineV2ContinueStageGrant requires a sink object");
  }
  if (!isRecord(intent)) {
    throw invalidIntent("applyPipelineV2ContinueStageGrant requires a prepared wait intent object");
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
  const sinkRef = sink as unknown as PipelineV2ContinueStageGrantControllerSink;
  // The dispatch is bound to the sink immediately at capture: a later
  // reassignment of the sink's member cannot change the dispatch target.
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no application.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no iteration grant can be applied",
      null,
    );
  }
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the continue-stage kind. Hand
  // -built, cast, spread, cloned and Proxy look-alikes are rejected here,
  // before any field of the intent or of the durable snapshot is read.
  if (!hasPreparedRunPlanProvenance(intent, "continue_stage_intent")) {
    throw invalidIntent("the intent is not a provenance-registered continue_stage_intent");
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
  // The run must be either waiting (the open-wait grant boundary) or
  // active with the answered target wait recognized as the exact
  // completed S2 retry (zero dispatch). Any other status is a typed
  // failure; later lifecycle progress is never treated as this
  // boundary's retry.
  if (state.status === "active" && state.phase === "running") {
    return applyCompletedGrantRetry(state, preparedIntent);
  }
  const bindings = requireContinueStageGrantBindings(state, preparedIntent);
  const manifest = bindings.manifest;
  const wait = bindings.wait;
  const generation = bindings.generation;
  const iteration = bindings.iteration;
  // The authoritative verification after every dispatch: the exact
  // durable record plus the unchanged wait bindings including the exact
  // accepted intent, the unchanged generation bindings and the exact
  // iteration state. No partial match is ever a success.
  const grantVerification = (after: PipelineV2RunState): boolean => {
    if (!exactGrantPresent(after, generation.index, wait.index, preparedIntent.sha256, manifest.additional_iterations)) {
      return false;
    }
    if (!targetGenerationBindingMatches(after, generation)) {
      return false;
    }
    if (!targetIterationOpenMatches(after, generation, iteration)) {
      return false;
    }
    const afterWait = findWaitRecord(after, wait.index);
    return afterWait !== undefined && waitBindingMatches(wait, afterWait, preparedIntent.sha256);
  };
  const closureVerification = (after: PipelineV2RunState): boolean => {
    if (!targetGenerationBindingMatches(after, generation)) {
      return false;
    }
    if (!exactGrantClosurePresent(after, generation.index, iteration.index, iteration.opened_transition_count, wait.index, wait.transition_count)) {
      return false;
    }
    if (!exactGrantPresent(after, generation.index, wait.index, preparedIntent.sha256, manifest.additional_iterations)) {
      return false;
    }
    const afterWait = findWaitRecord(after, wait.index);
    return afterWait !== undefined && waitBindingMatches(wait, afterWait, preparedIntent.sha256);
  };
  const finish = (after: PipelineV2RunState): AppliedPipelineV2ContinueStageGrant =>
    deepFreezeValue({
      wait_index: wait.index,
      generation_index: generation.index,
      iteration_index: iteration.index,
      additional_iterations: manifest.additional_iterations,
      intent_sha256: preparedIntent.sha256,
      state: after,
    });
  function raceOrMismatch(after: PipelineV2RunState | null, step: "grant" | "closure"): never {
    if (step === "grant" && after !== null) {
      const matching = grantRecordsFor(after, generation.index, wait.index);
      if (matching.length > 0 && !exactGrantPresent(after, generation.index, wait.index, preparedIntent.sha256, manifest.additional_iterations)) {
        throw controllerError(
          "grant_conflict",
          `wait ${wait.index} already carries a different grant for generation ${generation.index}`,
          after,
        );
      }
    }
    // The accepted intent of the open wait is contract-owned: a removed
    // or replaced intent is never a conflict-class mismatch, it is a
    // failed authoritative verification — identically on the resolve path
    // and on the racing dispatch path.
    if (after !== null) {
      const afterWait = findWaitRecord(after, wait.index);
      if (afterWait === undefined || afterWait.intent?.intent_sha256 !== preparedIntent.sha256) {
        throw controllerError(
          "invalid_state",
          `the accepted intent of open wait ${wait.index} is missing or replaced in the authoritative state`,
          after,
        );
      }
    }
    if (step === "closure" && after !== null) {
      const afterGeneration = after.generations[generation.index - 1];
      const closedIteration = afterGeneration?.iterations.find((record) => record.index === iteration.index);
      if (afterGeneration !== undefined && closedIteration?.closed !== undefined) {
        throw controllerError(
          "lifecycle_conflict",
          `the iteration ${iteration.index} of generation ${generation.index} is already closed differently`,
          after,
        );
      }
    }
    throw controllerError(
      "invalid_state",
      `the run state does not carry the applied ${step === "grant" ? "grant" : "closure"} of wait ${wait.index}`,
      after,
    );
  }
  async function dispatchStep(
    command: PipelineV2RunCommand,
    step: "grant" | "closure",
    verify: (after: PipelineV2RunState) => boolean,
  ): Promise<PipelineV2RunState> {
    const failMessage = step === "grant"
      ? "the iteration grant could not be committed"
      : "the stage iteration closure could not be committed";
    const durableMessage = step === "grant"
      ? "the iteration grant could not be confirmed durable"
      : "the stage iteration closure could not be confirmed durable";
    try {
      await dispatchCommand(command);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError("state_persist_failed", durableMessage, sinkRef.snapshot);
      }
      if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError("state_persist_failed", failMessage, sinkRef.snapshot);
      }
      if (cause instanceof PipelineV2StateError) {
        const after = sinkRef.snapshot;
        if (after !== null && verify(after)) {
          return after;
        }
        raceOrMismatch(after, step);
      }
      throw cause;
    }
    const after = sinkRef.snapshot;
    if (after === null || !verify(after)) {
      raceOrMismatch(after, step);
    }
    return after;
  }
  const plan = planContinueStageGrant(state, bindings, preparedIntent);
  if (plan.kind === "s2") {
    // S2: the exact durable grant plus the exact grant closure; the full
    // binding verification applies to the authoritative state as well.
    if (!targetGenerationBindingMatches(plan.state, generation)) {
      throw controllerError(
        "lifecycle_conflict",
        `the stage generation ${generation.index} does not match the grant's durable bindings`,
        plan.state,
      );
    }
    if (!exactGrantClosurePresent(plan.state, generation.index, iteration.index, iteration.opened_transition_count, wait.index, wait.transition_count)) {
      throw controllerError(
        "lifecycle_conflict",
        `the iteration ${iteration.index} of generation ${generation.index} does not carry the exact grant closure`,
        plan.state,
      );
    }
    const afterWait = findWaitRecord(plan.state, bindings.wait.index);
    if (afterWait === undefined || !waitBindingMatches(bindings.wait, afterWait, preparedIntent.sha256)) {
      throw controllerError("invalid_state", "the open wait record does not match the accepted intent", plan.state);
    }
    return finish(plan.state);
  }
  // S0/S1: the whole remaining suffix is proven before the first dispatch,
  // then the commands are dispatched strictly in order with the
  // authoritative verification after each one.
  precheckGrantSequence(state, plan.commands, state);
  let latest = state;
  for (const command of plan.commands) {
    const step = command.kind === "iteration_grant_recorded" ? "grant" as const : "closure" as const;
    const verify = step === "grant" ? grantVerification : closureVerification;
    latest = await dispatchStep(command, step, verify);
  }
  return finish(latest);
}

/**
 * The pure reconciliation of the continue-stage grant over the durable
 * records: the S0/S1/S2 classification with the exact remaining command
 * suffix and every typed conflict. This is the single classification
 * authority of both the flow and its error-contract tests; the branches
 * that are unreachable through loader-valid states (the reducer gates
 * them upstream) are exercised through this helper with states derived
 * from real reducer prefixes.
 */
export type ContinueStageGrantPlan =
  | { readonly kind: "s0"; readonly commands: readonly PipelineV2RunCommand[] }
  | { readonly kind: "s1"; readonly commands: readonly PipelineV2RunCommand[] }
  | { readonly kind: "s2"; readonly state: PipelineV2RunState };

export function planContinueStageGrant(
  state: PipelineV2RunState,
  bindings: GrantBindings,
  preparedIntent: PreparedPipelineV2RunWaitIntent,
): ContinueStageGrantPlan {
  const manifest = bindings.manifest;
  const wait = bindings.wait;
  const generation = bindings.generation;
  const iteration = bindings.iteration;
  const grantCommand: PipelineV2RunCommand = {
    kind: "iteration_grant_recorded",
    generationIndex: generation.index,
    waitIndex: wait.index,
    intentSha256: preparedIntent.sha256,
    additionalIterations: manifest.additional_iterations,
  };
  const closureCommand: PipelineV2RunCommand = {
    kind: "stage_iteration_closed",
    generationIndex: generation.index,
    iterationIndex: iteration.index,
    by: "grant",
    waitIndex: wait.index,
  };
  if (iteration.closed === undefined) {
    const matching = grantRecordsFor(state, generation.index, wait.index);
    if (matching.length > 1) {
      throw controllerError(
        "invalid_state",
        `wait ${wait.index} carries several grant records for generation ${generation.index}`,
        state,
      );
    }
    const existing = matching[0];
    if (existing === undefined) {
      // S0: the whole suffix is proven before the first dispatch.
      return { kind: "s0", commands: [grantCommand, closureCommand] };
    }
    if (existing.intent_sha256 !== preparedIntent.sha256 || existing.additional_iterations !== manifest.additional_iterations) {
      throw controllerError(
        "grant_conflict",
        `wait ${wait.index} already carries a different grant for generation ${generation.index}`,
        state,
      );
    }
    // S1: the exact durable grant; only the closure remains.
    return { kind: "s1", commands: [closureCommand] };
  }
  // The target iteration is already closed.
  const closed = iteration.closed;
  if (closed.by !== "grant") {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} was closed with ${JSON.stringify(closed.by)}, not by the grant`,
      state,
    );
  }
  if (closed.wait_index !== wait.index || closed.closed_transition_count !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      `the iteration ${iteration.index} of generation ${generation.index} was closed against another wait or another boundary`,
      state,
    );
  }
  if (!exactGrantPresent(state, generation.index, wait.index, preparedIntent.sha256, manifest.additional_iterations)) {
    throw controllerError(
      "invalid_state",
      `the iteration ${iteration.index} of generation ${generation.index} carries a grant closure without the exact durable grant`,
      state,
    );
  }
  // S2: the exact durable grant plus the exact grant closure.
  return { kind: "s2", state };
}
