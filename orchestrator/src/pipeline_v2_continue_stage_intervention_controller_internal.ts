/**
 * Production-neutral restart-aware continue-stage intervention controller
 * (unwired).
 *
 * This controller is the single layer that connects the three existing
 * authoritative layers into one full `continue_stage` intervention that
 * survives a process restart:
 *
 * 1. `acceptPipelineV2ContinueStageIntent` — the durable intent
 *    acceptance;
 * 2. `restorePipelineV2AcceptedRunPlan` — the read-only reconstruction of
 *    the provenance-backed compiled run plan from the durable ledger and
 *    the immutable manifests;
 * 3. `openPipelineV2ContinuedStage` — the grant, the grant-bound
 *    iteration closure, the `continue_stage` response and the successor
 *    iteration opening.
 *
 * The controller exists because after a restart no in-memory compiled
 * plan survives: the continued-stage composition refuses anything but a
 * real provenance-backed `CompiledPipelineV2RunPlan` of the existing
 * chain. The intervention runs the intent acceptance first, then hands
 * the acceptance result's authoritative durable state to the restore —
 * the restore stays the single owner of the compiled plan reconstruction
 * — and only then calls the composition with the exact restored compiled
 * plan, the original intent, the caller budget and the same sink. The
 * controller dispatches nothing itself, performs no filesystem work of
 * its own, never calls the reducer, never serializes or digests anything,
 * and never starts a worker: the three composed layers remain the only
 * owners of durable side effects.
 *
 * Capture and caller policy (fail-closed, before the first side effect):
 * the options shape; then `pipeline` → `runRoot` → `sink` → `intent` →
 * `initialBudget` each read exactly once; then the ops record shape and
 * its three members (`acceptIntent`, `restoreAcceptedPlan`,
 * `openContinuedStage`) each read exactly once with function checks; the
 * run root must be a non-empty string and `initialBudget` a positive safe
 * integer; the pipeline is gated through the existing provenance gate
 * (`requireResolvedPipelineV2Provenance`) and the intent through the
 * single existing manifest provenance registry (strictly the
 * `continue_stage_intent` kind) plus its continue-stage contract fields —
 * all before any field of a durable state is read. A hostile extra
 * options field is ignored. `compiledPlan` is not a caller field: it is
 * only ever obtained through the restore. The policy is fixed as captured
 * scalars (the intent's `run_id`/`wait_index`/digest/`stage_id`/
 * `expected_plan_sha256`/`additional_iterations` and the caller budget),
 * so no later mutation of the caller's options or objects can reinterpret
 * the checks; the caller's objects are never frozen or modified.
 *
 * Fixed sequence (the normal C0 window): accept the intent, fully verify
 * the successful acceptance result, restore the compiled plan from the
 * acceptance result's authoritative state, fully verify the restore
 * result, call the composition, fully verify the open result, and return
 * one unified flat result. No second compiler, parser, store traversal or
 * registry exists here.
 *
 * Acceptance-result verification (the waiting form): the result is a
 * record whose `wait_index`/`intent_sha256` equal the accepted intent's,
 * and whose durable state is the waiting run bound to the intent's run
 * id; the target wait is the last and the only record of its index,
 * carries the exact accepted intent digest and no response, and declares
 * exactly one `continue_stage` action whose target is the routing target
 * of the whole intervention.
 *
 * Progressed retry (one narrow reconciliation, never message parsing):
 * after a durable progression the intent controller may legitimately
 * refuse the original waiting/open-iteration boundary with its own
 * `invalid_state` — the grant closure already closed the iteration, the
 * response is already recorded, or the successor iteration is already
 * open. When — and only when — the cause is exactly a
 * `PipelineV2ContinueStageIntentControllerError` with the reason
 * `invalid_state`, the authoritative `error.state` shows the target wait
 * as the last and only record of its index carrying the exact accepted
 * intent digest, the run bound to the intent, and the durable lifecycle
 * provably progressed beyond the intent-acceptance boundary (the exact
 * `by: "grant"` closure of the target wait recorded on the last open
 * generation's last iteration, or the recorded `continue_stage`
 * response), with the transition and execution journals sitting exactly
 * at the wait boundary — then the acceptance is already complete: the
 * authoritative `error.state` continues as the acceptance result's state.
 * A state after an already started successor execution (an execution or a
 * transition beyond the wait boundary) is never an intervention retry,
 * and neither is any state whose intent is absent, different, historical
 * or answered with another action. Every unrecognized acceptance error is
 * re-thrown unchanged by object identity.
 *
 * Restore-result verification: the result is a record carrying a compiled
 * plan and a durable state; the state must be the unchanged authoritative
 * state the acceptance returned (the identity scalars, the cursor, the
 * durable pipeline identity through the single shared comparator, the
 * journal lengths and the live contract records — the last plan revision,
 * the target wait and the last generation — pinned field by field); the
 * compiled plan must be the real provenance-backed object of the same
 * pipeline identity (probed through the single public stage resolver and
 * the compiled layer's originating-identity comparison, both wrapped into
 * this layer's own `invalid_result`) whose revision, digest and origin
 * execution equal the authoritative last plan ledger record and the
 * intent's expected plan digest, and whose stage of the intent's
 * `stage_id` resolves through the single trusted resolver.
 *
 * Open-result verification (targeted binding, never a duplicate of the
 * composition's own full verification): the flat fields equal the
 * accepted intent's bindings and the durable wait declaration (the
 * request digest and the declared `continue_stage` action target); the
 * iteration index is the exact successor of the closed one; the
 * `compiled_stage` is the exact restored compiled stage object by
 * identity; and the result's durable state is the active running
 * boundary with the answered target wait, the cursor at the declared
 * target on the wait boundary, the journals at the wait boundary, exactly
 * one grant of the granted (generation, wait) pair carrying the exact
 * intent digest and additional iteration count, and the granted last open
 * generation bound to the restored plan, stage template, caller budget
 * and wait anchor with the exact grant closure and the exact open
 * successor iteration. A hostile or malformed successful result is this
 * layer's own `invalid_result` before the next layer is called — never a
 * leaked `TypeError`, never healed downstream; diagnostics are
 * content-free.
 *
 * Durability: the three composed layers' typed failures pass through
 * unchanged by identity (their own state/validation/durability windows
 * stay theirs); the suffix after a failed step is never executed, a fresh
 * retry executes only the missing durable suffix, and nothing is ever
 * rolled back or dispatched twice. Two identical concurrent calls
 * converge to one durable state through the composed layers' own
 * reconciliations.
 *
 * The unified result is flat, deep-frozen and not wider than the
 * composition's result: `{wait_index, intent_sha256, request_sha256,
 * response_sha256, additional_iterations, action_id: "continue_stage",
 * action_to, closed_iteration_index, iteration_index, generation_index,
 * compiled_stage, state}` — no restored plan, no manifests, no canonical
 * JSON, no paths and no caller-owned objects beyond the exact frozen
 * compiled stage enter the result.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ContinueStageInterventionControllerError`,
 * `applyPipelineV2ContinueStageInterventionWithIo` and the frozen
 * `productionContinueStageInterventionOps`; the public module exports
 * exactly the error and `applyPipelineV2ContinueStageIntervention` (types
 * are not runtime keys).
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, the revise-task branch (`revise_task_intent`), the
 * graph transition on the opened iteration and the next stage execution,
 * automatic resume, coordinator/runner/CLI/default-pipeline wiring,
 * schema/reducer changes, migrations/API/T3 and multi-process locking.
 */
import {
  acceptPipelineV2ContinueStageIntent,
  PipelineV2ContinueStageIntentControllerError,
  type AcceptedPipelineV2ContinueStageIntent,
} from "./pipeline_v2_continue_stage_intent_controller.ts";
import {
  openPipelineV2ContinuedStage,
  type OpenedPipelineV2ContinuedStage,
} from "./pipeline_v2_continued_stage_controller.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "./pipeline_v2_run_plan_compiled_internal.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import { restorePipelineV2AcceptedRunPlan } from "./pipeline_v2_run_plan_restore.ts";
import { isNonNegativeSafeInteger, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type {
  PipelineV2ContinueStageIntentManifest,
  PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";
import type {
  PipelineV2PlanRevisionState,
  PipelineV2RunCommand,
  PipelineV2RunPipelineIdentity,
  PipelineV2RunState,
  PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

export type PipelineV2ContinueStageInterventionControllerFailureReason = "invalid_options" | "invalid_result";

/**
 * A failure of the intervention layer itself with its stable
 * machine-readable `reason` and the last verified authoritative durable
 * state (`null` when the intervention never reached a verified durable
 * state). The composed layers' failures pass through by identity and
 * never take this shape.
 */
export class PipelineV2ContinueStageInterventionControllerError extends Error {
  readonly reason: PipelineV2ContinueStageInterventionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinueStageInterventionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinueStageInterventionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam shared with the three composed layers; the
 * production `PipelineV2RunStateSink` satisfies it without an adapter.
 * The intervention dispatches nothing itself.
 */
export interface PipelineV2ContinueStageInterventionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

/**
 * The single ops seam: exactly the three existing authoritative facades
 * and nothing else. No reducer, filesystem, store, publisher, serializer,
 * digest builder, registry, coordinator, runner or CLI capability is
 * reachable through it.
 */
export interface PipelineV2ContinueStageInterventionOps {
  readonly acceptIntent: typeof acceptPipelineV2ContinueStageIntent;
  readonly restoreAcceptedPlan: typeof restorePipelineV2AcceptedRunPlan;
  readonly openContinuedStage: typeof openPipelineV2ContinuedStage;
}

/**
 * The frozen production ops: the three existing facades bound by
 * identity; no installer and no mutable module-global seam.
 */
export const productionContinueStageInterventionOps: PipelineV2ContinueStageInterventionOps = deepFreezeValue({
  acceptIntent: acceptPipelineV2ContinueStageIntent,
  restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
  openContinuedStage: openPipelineV2ContinuedStage,
}) as unknown as PipelineV2ContinueStageInterventionOps;

export interface ApplyPipelineV2ContinueStageInterventionOptions {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRoot: string;
  readonly sink: PipelineV2ContinueStageInterventionControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly initialBudget: number;
}

export interface AppliedPipelineV2ContinueStageIntervention {
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly request_sha256: string;
  readonly response_sha256: string;
  readonly additional_iterations: number;
  readonly action_id: "continue_stage";
  readonly action_to: string;
  readonly closed_iteration_index: number;
  readonly iteration_index: number;
  readonly generation_index: number;
  readonly compiled_stage: CompiledPipelineV2RunPlanStage;
  readonly state: PipelineV2RunState;
}

const CONTINUE_STAGE_ACTION_ID = "continue_stage";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function controllerError(
  reason: PipelineV2ContinueStageInterventionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageInterventionControllerError {
  return new PipelineV2ContinueStageInterventionControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2ContinueStageInterventionControllerError {
  return controllerError("invalid_options", message, null);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageInterventionControllerError {
  return controllerError("invalid_result", message, state);
}

/**
 * The fixed caller policy, captured once before the first side effect:
 * the intent's binding scalars and the caller-owned budget. The compiled
 * plan and its stage are not part of the capture — they come only from
 * the restore.
 */
interface InterventionPolicy {
  readonly runId: string;
  readonly waitIndex: number;
  readonly intentSha256: string;
  readonly stageId: string;
  readonly expectedPlanSha256: string;
  readonly additionalIterations: number;
  readonly initialBudget: number;
}

/**
 * The verified intent-acceptance boundary: the authoritative durable
 * state, the target wait's request digest and transition anchor, and the
 * declared `continue_stage` routing target.
 */
interface VerifiedAcceptance {
  readonly state: PipelineV2RunState;
  readonly waitIndex: number;
  readonly waitTransitionCount: number;
  readonly requestSha256: string;
  readonly actionTo: string;
}

/**
 * The verified restore boundary: the real provenance-backed compiled plan
 * of the same pipeline identity and authoritative accepted plan, the
 * exact compiled stage of the intent, its declaration position, the
 * granted generation's durable opening anchor, and the unchanged
 * authoritative durable state.
 */
interface VerifiedRestore {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly compiledStage: CompiledPipelineV2RunPlanStage;
  readonly stagePosition: number;
  readonly generationAnchor: number;
  readonly state: PipelineV2RunState;
}

/**
 * The target wait of the policy inside one durable state: the last and
 * the only record of its index, carrying the exact accepted intent
 * digest, the declared `continue_stage` action target and the request
 * digest. Total for malformed journals; a null return is a failed
 * binding, never an exception.
 */
function findTargetWait(
  state: Record<string, unknown>,
  policy: InterventionPolicy,
): { wait: PipelineV2WaitRecord; transitionCount: number; requestSha256: string; actionTo: string } | null {
  if (state["run_id"] !== policy.runId) {
    return null;
  }
  const waits = state["waits"];
  if (!Array.isArray(waits) || waits.length === 0) {
    return null;
  }
  let position = -1;
  for (let index = 0; index < waits.length; index += 1) {
    const entry = waits[index];
    if (!isRecord(entry)) {
      return null;
    }
    if (entry["index"] === policy.waitIndex) {
      if (position !== -1) {
        return null;
      }
      position = index;
    }
  }
  if (position !== waits.length - 1) {
    return null;
  }
  const wait = waits[position] as unknown as PipelineV2WaitRecord;
  const transitionCount = wait["transition_count"] as unknown;
  if (!isPositiveSafeInteger(transitionCount)) {
    return null;
  }
  const requestSha256 = wait["request_sha256"] as unknown;
  if (!isString(requestSha256)) {
    return null;
  }
  const actions = wait["actions"] as unknown;
  if (!Array.isArray(actions)) {
    return null;
  }
  let declaredTo: string | undefined;
  let declaredCount = 0;
  for (const action of actions) {
    if (!isRecord(action)) {
      return null;
    }
    if (action["id"] === CONTINUE_STAGE_ACTION_ID) {
      declaredCount += 1;
      if (isString(action["to"])) {
        declaredTo = action["to"];
      }
    }
  }
  if (declaredCount !== 1 || declaredTo === undefined) {
    return null;
  }
  const intentRecord = wait["intent"] as unknown;
  if (!isRecord(intentRecord) || intentRecord["intent_sha256"] !== policy.intentSha256) {
    return null;
  }
  return { wait, transitionCount, requestSha256, actionTo: declaredTo };
}

/**
 * Whether the last open generation's last iteration carries the exact
 * grant closure of the target wait: the durable evidence that the
 * iteration of this intervention's (generation, wait) pair is already
 * closed by the grant.
 */
function carriesExactGrantClosure(
  state: Record<string, unknown>,
  policy: InterventionPolicy,
  target: { transitionCount: number },
): boolean {
  const generations = state["generations"];
  if (!Array.isArray(generations) || generations.length === 0) {
    return false;
  }
  const last = generations[generations.length - 1];
  if (!isRecord(last) || last["closed"] !== undefined) {
    return false;
  }
  const iterations = last["iterations"];
  if (!Array.isArray(iterations) || iterations.length === 0) {
    return false;
  }
  const lastIteration = iterations[iterations.length - 1];
  if (!isRecord(lastIteration)) {
    return false;
  }
  const closed = lastIteration["closed"];
  return (
    isRecord(closed) &&
    closed["by"] === "grant" &&
    closed["wait_index"] === policy.waitIndex &&
    closed["closed_transition_count"] === target.transitionCount
  );
}

/**
 * The full defensive verification of the intent controller's successful
 * result (the waiting form): the result is bound to the exact accepted
 * intent and its authoritative durable waiting state, whose target wait
 * is the last and only record of its index with the exact intent digest,
 * no response and the declared `continue_stage` action.
 */
function verifyAcceptanceResult(resultValue: unknown, policy: InterventionPolicy): VerifiedAcceptance {
  if (!isRecord(resultValue)) {
    throw invalidResult("the accepted continue-stage intent result is not a record", null);
  }
  const result = resultValue as Record<string, unknown>;
  if (!isPositiveSafeInteger(result["wait_index"]) || !isString(result["intent_sha256"])) {
    throw invalidResult("the accepted continue-stage intent result carries malformed result fields", null);
  }
  if (result["wait_index"] !== policy.waitIndex || result["intent_sha256"] !== policy.intentSha256) {
    throw invalidResult("the accepted continue-stage intent result does not match the accepted intent", null);
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the accepted continue-stage intent result carries no durable run state", null);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (state.status !== "waiting" || state.phase !== "waiting") {
    throw invalidResult("the accepted continue-stage intent state is not at the waiting boundary", state);
  }
  if (!isString(state.run_id) || state.run_id !== policy.runId) {
    throw invalidResult("the accepted continue-stage intent state belongs to a different run", state);
  }
  const target = findTargetWait(state as unknown as Record<string, unknown>, policy);
  if (target === null) {
    throw invalidResult(
      "the accepted continue-stage intent state does not carry the exact intent on the last wait record",
      state,
    );
  }
  if (target.wait["response"] !== undefined) {
    throw invalidResult("the accepted continue-stage intent wait already carries a response", state);
  }
  return {
    state,
    waitIndex: policy.waitIndex,
    waitTransitionCount: target.transitionCount,
    requestSha256: target.requestSha256,
    actionTo: target.actionTo,
  };
}

/**
 * The single narrow progressed-retry recognition (never message parsing):
 * the authoritative state of the intent controller's `invalid_state`
 * proves the target wait is the last and only record of its index with
 * the exact accepted intent digest, and the durable lifecycle has
 * provably moved beyond the intent-acceptance boundary — the exact grant
 * closure of the target wait, or the recorded `continue_stage` response —
 * with the journals sitting exactly at the wait boundary (no successor
 * execution started, no transition committed). A later stage execution is
 * never an intervention retry; a merely broken waiting boundary never is.
 */
function recognizeProgressedAcceptance(
  stateValue: unknown,
  policy: InterventionPolicy,
): VerifiedAcceptance | null {
  if (!isRecord(stateValue)) {
    return null;
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (!isString(state.run_id) || state.run_id !== policy.runId) {
    return null;
  }
  const stateRecord = stateValue as Record<string, unknown>;
  const target = findTargetWait(stateRecord, policy);
  if (target === null) {
    return null;
  }
  let progressed = false;
  const response = target.wait["response"] as unknown;
  if (
    isRecord(response) &&
    response["action_id"] === CONTINUE_STAGE_ACTION_ID &&
    isString(response["response_sha256"])
  ) {
    // The recorded continue_stage response is the durable evidence that
    // the run resumed; it covers the answered and the already-reopened
    // windows.
    progressed = true;
  } else if (response === undefined && carriesExactGrantClosure(stateRecord, policy, target)) {
    // Without a response the only progression is the exact grant closure
    // of the target wait; a boundary answered with another action is
    // never a continue-stage retry.
    progressed = true;
  }
  if (!progressed) {
    return null;
  }
  const transitions = stateRecord["transitions"];
  const executions = stateRecord["executions"];
  if (!Array.isArray(transitions) || transitions.length !== target.transitionCount) {
    return null;
  }
  if (!Array.isArray(executions) || executions.length !== target.transitionCount) {
    return null;
  }
  return {
    state,
    waitIndex: policy.waitIndex,
    waitTransitionCount: target.transitionCount,
    requestSha256: target.requestSha256,
    actionTo: target.actionTo,
  };
}

/**
 * The ordered declared-action list equality: same length, same order,
 * same ids and targets. Total for malformed values.
 */
function orderedActionsEqual(before: unknown, after: unknown): boolean {
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    const beforeAction = before[position];
    const afterAction = after[position];
    if (
      !isRecord(beforeAction) ||
      !isRecord(afterAction) ||
      beforeAction["id"] !== afterAction["id"] ||
      beforeAction["to"] !== afterAction["to"]
    ) {
      return false;
    }
  }
  return true;
}

/**
 * The exact field equality of one target wait record: the identity,
 * binding and declaration fields plus the accepted intent and the
 * response projection. Total for malformed values.
 */
function waitRecordEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before["index"] ||
    after["transition_count"] !== before["transition_count"] ||
    after["state_id"] !== before["state_id"] ||
    after["reason"] !== before["reason"] ||
    after["request_sha256"] !== before["request_sha256"]
  ) {
    return false;
  }
  if (!orderedActionsEqual(before["actions"], after["actions"])) {
    return false;
  }
  const beforeIntent = before["intent"];
  if (beforeIntent === undefined) {
    if (after["intent"] !== undefined) {
      return false;
    }
  } else {
    if (!isRecord(beforeIntent) || !isRecord(after["intent"])) {
      return false;
    }
    if (after["intent"]["intent_sha256"] !== beforeIntent["intent_sha256"]) {
      return false;
    }
  }
  const beforeResponse = before["response"];
  if (beforeResponse === undefined) {
    return after["response"] === undefined;
  }
  return (
    isRecord(after["response"]) &&
    isRecord(beforeResponse) &&
    after["response"]["action_id"] === beforeResponse["action_id"] &&
    after["response"]["response_sha256"] === beforeResponse["response_sha256"]
  );
}

/**
 * The exact field equality of one plan ledger record. Total for malformed
 * values.
 */
function planRecordEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["revision"] === before["revision"] &&
    after["sha256"] === before["sha256"] &&
    after["previous_sha256"] === before["previous_sha256"] &&
    after["origin_execution"] === before["origin_execution"]
  );
}

/**
 * The exact field equality of one iteration closure projection (the
 * wait-bound `wait_index` present exactly for wait-bound closures).
 */
function closureProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["by"] === before["by"] &&
    after["closed_transition_count"] === before["closed_transition_count"] &&
    after["wait_index"] === before["wait_index"]
  );
}

/**
 * The open-iteration projection equality.
 */
function openIterationProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["opened_transition_count"] === before["opened_transition_count"]
  );
}

/**
 * Whether the restored state is the unchanged authoritative state the
 * acceptance returned: the identity scalars, the cursor, the durable
 * pipeline identity through the single shared comparator, the journal
 * lengths, the absence of terminal/publication/failure projections and
 * the live contract records — the last plan revision, the target wait and
 * the last generation — pinned field by field. The historical elements of
 * the execution/transition/input/task/grant journals are pinned by
 * length; their contents are inert for this layer (they feed no decision
 * here and stay owned by the durable ledger itself).
 */
function restoreStateUnchanged(after: Record<string, unknown>, before: Record<string, unknown>): boolean {
  if (
    after["schema_version"] !== before["schema_version"] ||
    after["revision"] !== before["revision"] ||
    after["run_id"] !== before["run_id"] ||
    after["status"] !== before["status"] ||
    after["phase"] !== before["phase"] ||
    after["started_at"] !== before["started_at"] ||
    after["updated_at"] !== before["updated_at"] ||
    after["terminal"] !== undefined ||
    after["run_outputs"] !== undefined ||
    after["failure"] !== undefined
  ) {
    return false;
  }
  if (!isRecord(after["pipeline"]) || !isRecord(before["pipeline"])) {
    return false;
  }
  if (
    comparePipelineV2RunIdentity(
      before["pipeline"] as unknown as PipelineV2RunPipelineIdentity,
      after["pipeline"] as unknown as PipelineV2RunPipelineIdentity,
    ).kind !== "match"
  ) {
    return false;
  }
  const beforeCursor = before["cursor"];
  const afterCursor = after["cursor"];
  if (
    !isRecord(beforeCursor) ||
    !isRecord(afterCursor) ||
    afterCursor["current_state"] !== beforeCursor["current_state"] ||
    afterCursor["transition_count"] !== beforeCursor["transition_count"]
  ) {
    return false;
  }
  for (const region of ["inputs", "executions", "transitions", "waits", "generations", "task_revisions", "plan_revisions", "grants"] as const) {
    const afterList = after[region];
    const beforeList = before[region];
    if (!Array.isArray(afterList) || !Array.isArray(beforeList) || afterList.length !== beforeList.length) {
      return false;
    }
  }
  const afterPlanRevisions = after["plan_revisions"] as unknown[];
  const beforePlanRevisions = before["plan_revisions"] as unknown[];
  if (
    beforePlanRevisions.length > 0 &&
    !planRecordEquals(beforePlanRevisions[beforePlanRevisions.length - 1], afterPlanRevisions[afterPlanRevisions.length - 1])
  ) {
    return false;
  }
  const afterWaits = after["waits"] as unknown[];
  const beforeWaits = before["waits"] as unknown[];
  if (beforeWaits.length > 0 && !waitRecordEquals(beforeWaits[beforeWaits.length - 1], afterWaits[afterWaits.length - 1])) {
    return false;
  }
  const afterGenerations = after["generations"] as unknown[];
  const beforeGenerations = before["generations"] as unknown[];
  if (beforeGenerations.length > 0) {
    const beforeGeneration = beforeGenerations[beforeGenerations.length - 1];
    const afterGeneration = afterGenerations[afterGenerations.length - 1];
    if (
      !isRecord(beforeGeneration) ||
      !isRecord(afterGeneration) ||
      afterGeneration["index"] !== beforeGeneration["index"] ||
      afterGeneration["stage_id"] !== beforeGeneration["stage_id"] ||
      afterGeneration["stage_position"] !== beforeGeneration["stage_position"] ||
      afterGeneration["template_id"] !== beforeGeneration["template_id"] ||
      afterGeneration["plan_sha256"] !== beforeGeneration["plan_sha256"] ||
      afterGeneration["initial_budget"] !== beforeGeneration["initial_budget"] ||
      afterGeneration["opened_transition_count"] !== beforeGeneration["opened_transition_count"] ||
      afterGeneration["iteration_count"] !== beforeGeneration["iteration_count"] ||
      !openIterationProjectionEquals(beforeGeneration["open_iteration"], afterGeneration["open_iteration"]) ||
      !closureProjectionEquals(beforeGeneration["closed"], afterGeneration["closed"]) ||
      !Array.isArray(afterGeneration["iterations"]) ||
      !Array.isArray(beforeGeneration["iterations"]) ||
      (afterGeneration["iterations"] as unknown[]).length !== (beforeGeneration["iterations"] as unknown[]).length
    ) {
      return false;
    }
    const afterIterations = afterGeneration["iterations"] as unknown[];
    const beforeIterations = beforeGeneration["iterations"] as unknown[];
    const beforeLastIteration = beforeIterations[beforeIterations.length - 1];
    const afterLastIteration = afterIterations[afterIterations.length - 1];
    if (
      !isRecord(beforeLastIteration) ||
      !isRecord(afterLastIteration) ||
      afterLastIteration["index"] !== beforeLastIteration["index"] ||
      afterLastIteration["opened_transition_count"] !== beforeLastIteration["opened_transition_count"] ||
      !closureProjectionEquals(beforeLastIteration["closed"], afterLastIteration["closed"])
    ) {
      return false;
    }
  }
  return true;
}

/**
 * The full defensive verification of the restore's successful result: the
 * unchanged authoritative durable state, the real provenance-backed
 * compiled plan of the same pipeline identity and authoritative accepted
 * plan, and the exact compiled stage of the intent. A forged or foreign
 * compiled plan is this layer's `invalid_result`, never healed
 * downstream.
 */
function verifyRestoreResult(
  resultValue: unknown,
  accepted: VerifiedAcceptance,
  policy: InterventionPolicy,
): VerifiedRestore {
  if (!isRecord(resultValue)) {
    throw invalidResult("the restored run plan result is not a record", accepted.state);
  }
  const result = resultValue as Record<string, unknown>;
  const compiledPlanValue = result["compiled_plan"];
  if (!isRecord(compiledPlanValue)) {
    throw invalidResult("the restored run plan result carries no compiled plan", accepted.state);
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the restored run plan result carries no durable run state", accepted.state);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (!restoreStateUnchanged(stateValue, accepted.state as unknown as Record<string, unknown>)) {
    throw invalidResult("the restored run plan result does not carry the unchanged authoritative durable state", state);
  }
  const compiledPlan = compiledPlanValue as unknown as CompiledPipelineV2RunPlan;
  let compiledStage: CompiledPipelineV2RunPlanStage;
  let originIdentity: PipelineV2RunPipelineIdentity;
  try {
    // The single public stage resolver is the provenance probe and the
    // only stage resolution: a forged, cloned or foreign compiled plan
    // never resolves the intent's stage.
    compiledStage = compiledPipelineV2RunPlanStageFor(compiledPlan, policy.stageId);
    originIdentity = compiledRunPlanOriginIdentity(compiledPlan);
  } catch {
    throw invalidResult(
      "the restored run plan result does not carry the real compiled plan of the accepted intent's stage",
      state,
    );
  }
  if (
    comparePipelineV2RunIdentity(
      originIdentity,
      state.pipeline as unknown as PipelineV2RunPipelineIdentity,
    ).kind !== "match"
  ) {
    throw invalidResult("the restored compiled plan belongs to a different pipeline identity", state);
  }
  const planRevisions = state.plan_revisions;
  if (!Array.isArray(planRevisions) || planRevisions.length === 0) {
    throw invalidResult("the restored state carries no accepted plan revision", state);
  }
  const planRecord = planRevisions[planRevisions.length - 1] as unknown as PipelineV2PlanRevisionState;
  if (
    compiledPlan.run_id !== policy.runId ||
    compiledPlan.plan_revision !== planRecord.revision ||
    compiledPlan.plan_sha256 !== planRecord.sha256 ||
    compiledPlan.plan_sha256 !== policy.expectedPlanSha256 ||
    compiledPlan.origin_execution !== planRecord.origin_execution
  ) {
    throw invalidResult("the restored compiled plan does not match the authoritative accepted plan revision", state);
  }
  if (!Array.isArray(compiledPlan.stages)) {
    throw new Error("pipeline v2 continue stage intervention invariant violated: the restored plan carries no stages");
  }
  const declarationIndex = compiledPlan.stages.findIndex((stage) => stage.id === compiledStage.id);
  if (declarationIndex < 0) {
    throw new Error("pipeline v2 continue stage intervention invariant violated: the compiled stage is not in the restored plan");
  }
  // The granted generation's durable opening anchor: the restore state's
  // last generation record is the pinned authoritative pre-intervention
  // value (the generation was opened before the wait, so the anchor is
  // the generation's own opening transition count, never the wait's).
  const generations = state.generations;
  if (!Array.isArray(generations) || generations.length === 0) {
    throw invalidResult("the restored state carries no stage generation", state);
  }
  const restoredGeneration = generations[generations.length - 1] as unknown as Record<string, unknown>;
  if (
    !isRecord(restoredGeneration) ||
    restoredGeneration["closed"] !== undefined ||
    !isNonNegativeSafeInteger(restoredGeneration["opened_transition_count"])
  ) {
    throw invalidResult("the restored state does not carry the last open granted generation", state);
  }
  return {
    compiledPlan,
    compiledStage,
    stagePosition: declarationIndex + 1,
    generationAnchor: restoredGeneration["opened_transition_count"] as number,
    state,
  };
}

/**
 * The full defensive verification of the composition's successful result
 * (targeted binding, never a duplicate of the composition's own full
 * verification): the flat fields equal the accepted intent's bindings and
 * the durable wait declaration, the iteration index is the exact
 * successor of the closed one, the compiled stage is the exact restored
 * object by identity, and the durable state is the active running
 * boundary with the answered target wait, the exact grant pair, the
 * granted last open generation bound to the restored plan, the caller
 * budget and the wait anchor, the exact grant closure and the exact open
 * successor iteration.
 */
function verifyOpenResult(
  resultValue: unknown,
  accepted: VerifiedAcceptance,
  restored: VerifiedRestore,
  policy: InterventionPolicy,
): OpenedPipelineV2ContinuedStage {
  if (!isRecord(resultValue)) {
    throw invalidResult("the opened continued-stage result is not a record", restored.state);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    !isPositiveSafeInteger(result["wait_index"]) ||
    !isString(result["intent_sha256"]) ||
    !isString(result["request_sha256"]) ||
    !isString(result["response_sha256"]) ||
    !isPositiveSafeInteger(result["additional_iterations"]) ||
    result["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
    !isString(result["action_to"]) ||
    !isPositiveSafeInteger(result["closed_iteration_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    !isPositiveSafeInteger(result["generation_index"])
  ) {
    throw invalidResult("the opened continued-stage result carries malformed result fields", restored.state);
  }
  if (
    result["wait_index"] !== policy.waitIndex ||
    result["intent_sha256"] !== policy.intentSha256 ||
    result["additional_iterations"] !== policy.additionalIterations
  ) {
    throw invalidResult("the opened continued-stage result does not match the accepted intent", restored.state);
  }
  if (result["request_sha256"] !== accepted.requestSha256 || result["action_to"] !== accepted.actionTo) {
    throw invalidResult("the opened continued-stage result does not match the durable wait declaration", restored.state);
  }
  if (result["iteration_index"] !== result["closed_iteration_index"] + 1) {
    throw invalidResult(
      "the opened continued-stage result does not open the successor iteration of the grant-closed iteration",
      restored.state,
    );
  }
  if (result["compiled_stage"] !== restored.compiledStage) {
    throw invalidResult("the opened continued-stage result does not carry the exact restored compiled stage", restored.state);
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the opened continued-stage result carries no durable run state", restored.state);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (state.status !== "active" || state.phase !== "running") {
    throw invalidResult("the opened continued-stage state is not at the active running boundary", state);
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw invalidResult(
      "the opened continued-stage state already carries a terminal, publication or failure projection",
      state,
    );
  }
  if (!isString(state.run_id) || state.run_id !== policy.runId) {
    throw invalidResult("the opened continued-stage state belongs to a different run", state);
  }
  const target = findTargetWait(state as unknown as Record<string, unknown>, policy);
  if (target === null) {
    throw invalidResult("the opened continued-stage state does not carry the answered target wait", state);
  }
  const response = target.wait["response"] as unknown;
  if (
    !isRecord(response) ||
    response["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
    response["response_sha256"] !== result["response_sha256"]
  ) {
    throw invalidResult("the opened continued-stage state does not carry the exact durable continue_stage response", state);
  }
  const cursor = state["cursor"];
  if (
    !isRecord(cursor) ||
    cursor["current_state"] !== accepted.actionTo ||
    cursor["transition_count"] !== accepted.waitTransitionCount
  ) {
    throw invalidResult("the opened continued-stage cursor is not at the declared action target on the wait boundary", state);
  }
  const transitions = state["transitions"];
  const executions = state["executions"];
  if (!Array.isArray(transitions) || transitions.length !== accepted.waitTransitionCount) {
    throw invalidResult("the opened continued-stage transition journal does not sit at the wait boundary", state);
  }
  if (!Array.isArray(executions) || executions.length !== accepted.waitTransitionCount) {
    throw invalidResult("the opened continued-stage execution journal does not sit at the wait boundary", state);
  }
  const grants = state["grants"];
  if (!Array.isArray(grants)) {
    throw invalidResult("the opened continued-stage state carries no grant ledger", state);
  }
  let pairCount = 0;
  let pairGrant: Record<string, unknown> | undefined;
  for (const entry of grants) {
    if (!isRecord(entry)) {
      throw invalidResult("the opened continued-stage state carries a malformed grant record", state);
    }
    if (entry["generation_index"] === result["generation_index"] && entry["wait_index"] === policy.waitIndex) {
      pairCount += 1;
      pairGrant = entry;
    }
  }
  if (
    pairCount !== 1 ||
    pairGrant === undefined ||
    pairGrant["intent_sha256"] !== policy.intentSha256 ||
    pairGrant["additional_iterations"] !== policy.additionalIterations
  ) {
    throw invalidResult(
      "the opened continued-stage boundary does not carry exactly one grant of the granted generation and wait pair",
      state,
    );
  }
  const generations = state["generations"];
  if (!Array.isArray(generations) || generations.length !== result["generation_index"]) {
    throw invalidResult("the granted generation is not the last durable generation", state);
  }
  const generation = generations[(result["generation_index"] as number) - 1];
  if (!isRecord(generation) || generation["index"] !== result["generation_index"] || generation["closed"] !== undefined) {
    throw invalidResult("the opened continued-stage boundary does not carry the last open granted generation", state);
  }
  if (
    generation["stage_id"] !== policy.stageId ||
    generation["stage_position"] !== restored.stagePosition ||
    generation["template_id"] !== restored.compiledStage.template ||
    generation["plan_sha256"] !== restored.compiledPlan.plan_sha256 ||
    generation["initial_budget"] !== policy.initialBudget ||
    generation["opened_transition_count"] !== restored.generationAnchor
  ) {
    throw invalidResult(
      "the granted generation does not match the restored plan, the accepted intent and the caller budget on the wait anchor",
      state,
    );
  }
  const iterations = generation["iterations"];
  if (!Array.isArray(iterations) || generation["iteration_count"] !== iterations.length) {
    throw invalidResult("the granted generation carries no coherent iteration history", state);
  }
  for (const iteration of iterations) {
    if (!isRecord(iteration)) {
      throw invalidResult("the granted generation carries a malformed iteration record", state);
    }
  }
  const closedIterationIndex = result["closed_iteration_index"] as number;
  if (iterations.length !== closedIterationIndex + 1) {
    throw invalidResult("the granted generation's iteration history does not end at the open successor iteration", state);
  }
  const closedIteration = iterations[closedIterationIndex - 1];
  if (!isRecord(closedIteration) || closedIteration["index"] !== closedIterationIndex) {
    throw invalidResult("the granted generation does not carry the grant-closed iteration", state);
  }
  const closed = closedIteration["closed"];
  if (
    !isRecord(closed) ||
    closed["by"] !== "grant" ||
    closed["wait_index"] !== policy.waitIndex ||
    closed["closed_transition_count"] !== accepted.waitTransitionCount
  ) {
    throw invalidResult("the grant-closed iteration does not carry the exact grant closure", state);
  }
  const openIteration = generation["open_iteration"];
  if (
    !isRecord(openIteration) ||
    openIteration["index"] !== result["iteration_index"] ||
    openIteration["opened_transition_count"] !== accepted.waitTransitionCount
  ) {
    throw invalidResult("the granted generation's open iteration is not the exact successor on the wait anchor", state);
  }
  const lastIteration = iterations[iterations.length - 1];
  if (
    !isRecord(lastIteration) ||
    lastIteration["index"] !== result["iteration_index"] ||
    lastIteration["closed"] !== undefined
  ) {
    throw invalidResult("the granted generation's last iteration is not the exact open successor", state);
  }
  return resultValue as unknown as OpenedPipelineV2ContinuedStage;
}

/**
 * Validate, accept, restore, compose and verify one full `continue_stage`
 * intervention through the three existing facades (see the module
 * docstring for the full order, retry and verification semantics).
 */
export async function applyPipelineV2ContinueStageInterventionWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<AppliedPipelineV2ContinueStageIntervention> {
  // Capture boundary: the options shape, every options field and the ops
  // record shape and its three members are read exactly once, all before
  // the first await. No field of the intent is read before its provenance
  // gate; a later mutation of the caller's options or ops cannot change
  // this intervention's policy.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot = options["runRoot"];
  const sink = options["sink"] as unknown as PipelineV2ContinueStageInterventionControllerSink;
  const intent = options["intent"];
  const initialBudget = options["initialBudget"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const acceptIntent = ops["acceptIntent"];
  const restoreAcceptedPlan = ops["restoreAcceptedPlan"];
  const openContinuedStage = ops["openContinuedStage"];
  if (
    typeof acceptIntent !== "function" ||
    typeof restoreAcceptedPlan !== "function" ||
    typeof openContinuedStage !== "function"
  ) {
    throw invalidOptions(
      "applyPipelineV2ContinueStageIntervention requires the three composed facade functions",
    );
  }
  if (!isString(runRoot) || runRoot === "") {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires a non-empty runRoot string");
  }
  if (!isRecord(sink)) {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires a state sink");
  }
  if (!isRecord(intent)) {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires a prepared wait intent object");
  }
  if (!isPositiveSafeInteger(initialBudget)) {
    throw invalidOptions("applyPipelineV2ContinueStageIntervention requires a positive safe integer initial budget");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any field of the intent or of a durable state
  // is read.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 continue stage intervention");
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the continue-stage kind.
  if (!hasPreparedRunPlanProvenance(intent, "continue_stage_intent")) {
    throw invalidOptions("the prepared wait intent is not a provenance-registered continue_stage_intent");
  }
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  const manifestValue: unknown = preparedIntent.manifest;
  if (
    !isRecord(manifestValue) ||
    manifestValue["kind"] !== "continue_stage_intent" ||
    !isString(manifestValue["run_id"]) ||
    !isPositiveSafeInteger(manifestValue["wait_index"]) ||
    !isString(manifestValue["stage_id"]) ||
    !isString(manifestValue["expected_plan_sha256"]) ||
    !isPositiveSafeInteger(manifestValue["additional_iterations"])
  ) {
    throw invalidOptions("the prepared wait intent does not carry the continue_stage contract fields");
  }
  const manifest = manifestValue as unknown as PipelineV2ContinueStageIntentManifest;
  const policy: InterventionPolicy = {
    runId: manifest.run_id,
    waitIndex: manifest.wait_index,
    intentSha256: preparedIntent.sha256,
    stageId: manifest.stage_id,
    expectedPlanSha256: manifest.expected_plan_sha256,
    additionalIterations: manifest.additional_iterations,
    initialBudget,
  };

  // Step 1: accept the intent through the existing controller; its typed
  // errors keep their identity — except the single narrow progressed-retry
  // classification below.
  let accepted: VerifiedAcceptance;
  try {
    const acceptedResult: AcceptedPipelineV2ContinueStageIntent = await (
      acceptIntent as typeof acceptPipelineV2ContinueStageIntent
    )({
      runRoot,
      sink,
      intent: preparedIntent,
    });
    accepted = verifyAcceptanceResult(acceptedResult, policy);
  } catch (cause) {
    let recognized: VerifiedAcceptance | null = null;
    if (cause instanceof PipelineV2ContinueStageIntentControllerError && cause.reason === "invalid_state") {
      recognized = recognizeProgressedAcceptance(cause.state, policy);
    }
    if (recognized === null) {
      throw cause;
    }
    accepted = recognized;
  }

  // Step 2: restore the compiled plan from the acceptance's authoritative
  // state; the restore stays the single owner of the compiled plan
  // reconstruction, and its read-only state is verified unchanged.
  const restored = verifyRestoreResult(
    await (restoreAcceptedPlan as typeof restorePipelineV2AcceptedRunPlan)({
      pipeline,
      runRoot,
      state: accepted.state,
    }),
    accepted,
    policy,
  );

  // Step 3: open the continued stage through the existing composition —
  // the exact restored compiled plan, the original intent, the caller
  // budget and the same sink; the composition's full C2–C5 lifecycle
  // verification and its typed errors stay authoritative.
  const opened = verifyOpenResult(
    await (openContinuedStage as typeof openPipelineV2ContinuedStage)({
      runRoot,
      sink,
      intent: preparedIntent,
      compiledPlan: restored.compiledPlan,
      initialBudget,
    }),
    accepted,
    restored,
    policy,
  );

  return deepFreezeValue({
    wait_index: opened.wait_index,
    intent_sha256: opened.intent_sha256,
    request_sha256: opened.request_sha256,
    response_sha256: opened.response_sha256,
    additional_iterations: opened.additional_iterations,
    action_id: opened.action_id,
    action_to: opened.action_to,
    closed_iteration_index: opened.closed_iteration_index,
    iteration_index: opened.iteration_index,
    generation_index: opened.generation_index,
    compiled_stage: restored.compiledStage,
    state: opened.state,
  }) as unknown as AppliedPipelineV2ContinueStageIntervention;
}
