import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  completePipelineV2ContinueStage,
} from "./pipeline_v2_continue_stage_completion_controller.ts";
import { PipelineV2ContinueStageGrantControllerError } from "./pipeline_v2_continue_stage_grant_controller.ts";
import {
  ensurePipelineV2StageIteration,
  type EnsuredPipelineV2StageIteration,
} from "./pipeline_v2_stage_iteration_controller.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import type {
  PipelineV2ContinueStageIntentManifest,
  PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";
import type {
  PipelineV2AgentOutputState,
  PipelineV2CommittedTransitionState,
  PipelineV2ExecutionState,
  PipelineV2IterationGrantState,
  PipelineV2RunCommand,
  PipelineV2RunInputState,
  PipelineV2RunState,
  PipelineV2StageGenerationRecord,
  PipelineV2StageIterationRecord,
  PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";

/**
 * Production-neutral continued-stage composition controller
 * (production-reachable transitively through the continue-stage
 * intervention controller).
 *
 * This controller closes exactly one gap of the continue-stage branch:
 * after `completePipelineV2ContinueStage` has durably applied (or
 * confirmed) the iteration grant, closed the grant-bound iteration and
 * recorded the user's `continue_stage` response — but deliberately not
 * opened the next iteration — this controller opens the next iteration of
 * the SAME generation through the existing
 * `ensurePipelineV2StageIteration` and returns one unified, fully
 * verified result. The composition performs no filesystem work of its
 * own, never dispatches a reducer command itself, never adds a reducer
 * event or state field, never serializes or digests anything, and never
 * starts the next stage execution: the two composed controllers remain
 * the only owners of durable side effects, and the graph transition that
 * follows on the opened iteration is a later increment.
 *
 * Fixed sequence: `completePipelineV2ContinueStage` → the full defensive
 * verification of its successful result → `ensurePipelineV2StageIteration`
 * → the full defensive verification of its successful result → the
 * unified result. The ensure call is impossible until the completed
 * boundary has passed every check; a mismatching or malformed completion
 * result is the composition's own `invalid_result` with zero ensure
 * calls.
 *
 * Capture and caller policy (fail-closed, before the first side effect):
 * the options shape; then `runRoot` → `sink` → `intent` → `compiledPlan`
 * → `initialBudget` each read exactly once; then both ops getters
 * (`completeStage`, `ensureStageIteration`) read exactly once before the
 * first await; `runRoot` must be a string, the sink an object and
 * `initialBudget` a positive safe integer (`invalid_options`). The intent
 * is gated through the single existing provenance registry
 * (`hasPreparedRunPlanProvenance`, strictly the `continue_stage_intent`
 * kind) before any of its fields is read, and the stage is resolved only
 * through the single trusted compiled resolver
 * `compiledPipelineV2RunPlanStageFor(compiledPlan, intent.stage_id)` — its
 * provenance gate, stage existence and template binding belong to that
 * resolver alone and its typed errors pass by identity. The stage
 * position is derived only from the compiled plan's declaration order.
 * The policy is then fixed as captured scalars (`stageId`, `stagePosition`
 * , `compiledStage`, the compiled plan digest, the caller
 * `initialBudget`, and the intent's `wait_index`/`additional_iterations`/
 * `intent_sha256`/`run_id`), so no later mutation of the caller's options
 * or objects — and no hostile result — can reinterpret it. No second
 * compiled-plan parser or validator exists: the compiled projection is
 * consumed exactly as the acceptance produced it.
 *
 * Completed-boundary verification (the shared defensive shape check,
 * contract-owned values only, never the hostile result alone): the run is
 * active/running with no terminal, publication or failure projection and
 * the intent's `run_id`; the declared wait is the LAST wait record and
 * the only record of its index, carrying the exact accepted intent
 * digest, exactly one declared `continue_stage` action whose target is
 * exactly the selected compiled stage's `entry_state`, and the exact
 * durable `continue_stage` response; the cursor sits at that declared
 * target with its transition count at the wait's boundary; the
 * transition and execution journals sit exactly at the wait boundary
 * (no execution or transition advanced past the answered wait); for the
 * granted (generation, wait) pair exactly one durable grant record
 * exists and exactly it carries the exact intent digest and
 * `additional_iterations` (a second conflicting record of the same pair
 * is never a valid boundary); the target generation is the LAST
 * open generation bound exactly to the intent's stage id, the derived
 * stage position, the compiled stage template, the current compiled plan
 * digest and the caller `initialBudget` on the wait's anchor; and the
 * grant closed exactly one iteration of that generation with the exact
 * `by: "grant"` closure (`wait_index` and `closed_transition_count`
 * exact). Exactly two generation retry shapes are admissible: the next
 * iteration not yet opened (the grant-closed iteration is the last one,
 * `open_iteration` absent) and the exact next iteration already opened by
 * a concurrent or previous successful call (the grant-closed iteration is
 * the second-to-last one and the open iteration is exactly
 * `closed_iteration_index + 1` on the same anchor with matching
 * `iteration_count` and `open_iteration` projection). Any other open
 * iteration or result is not a success.
 *
 * C5 recognition: the grant controller pins the completed boundary BEFORE
 * any next iteration (`an open next iteration is a typed conflict, never
 * a retry of that boundary`), so the completion controller refuses — with
 * zero dispatch and zero filesystem work — exactly the state a previous
 * fully successful call (or a racing identical call that already opened
 * the next iteration) produced. When the completion fails with the grant
 * controller's `lifecycle_conflict`, the composition re-recognizes the
 * exact completed-and-opened shape on the authoritative snapshot against
 * the fixed policy and continues with the ensure only on that exact
 * match; every other failure — a deviating open iteration, a genuine
 * grant/lifecycle conflict, any other downstream or unexpected error —
 * is re-thrown unchanged (the same object identity). A hostile sink
 * presentation can never take the C5 shortcut: every shape check must
 * hold exactly against the fixed policy, and an exact match makes the
 * recognized boundary the composition's verified completed boundary.
 *
 * Completion-result verification (the normal path): the result is a
 * record with the exact positive safe `wait_index`/`generation_index`/
 * `iteration_index`/`additional_iterations`, the string digests, the
 * literal `action_id: "continue_stage"`, the `action_to` equal to the
 * selected compiled stage's `entry_state`, and a record state that
 * satisfies the completed-boundary shape above; the flat
 * `generation_index`/`iteration_index` are bound exactly to the verified
 * durable grant boundary (the granted generation and the grant-closed
 * iteration). Every field access is
 * defensive: a hostile or structurally inconsistent successful result
 * (a mismatching stage, plan digest, budget, grant, closure, wait anchor
 * or action; a malformed nested document) is the composition's own typed
 * `invalid_result` — never a leaked `TypeError`, with zero ensure calls —
 * and diagnostics are content-free (they never echo unchecked hostile
 * values, bodies or digests).
 *
 * Ensure call: the existing `ensurePipelineV2StageIteration` is invoked
 * exactly once with the original trusted `compiledPlan`, the fixed
 * `stageId`, the caller-owned `initialBudget` and the same sink. The
 * effective iteration budget is never computed here: its source of truth
 * stays the reducer (the immutable generation `initial_budget` plus the
 * generation's durable grants), and a real conflict the stage-iteration
 * controller detects itself (a budget/stage/plan/lifecycle race) passes
 * through by identity — the composition never pre-classifies it.
 *
 * Ensure-result verification (defensive, targeted, against the verified
 * completed state, never against itself): the exact `compiled_stage`
 * object the trusted resolver returned (identity, never a clone); the
 * same `generation_index` and `iteration_index === closed_iteration_index
 * + 1`; and the exact durable delta — when the completed state had no
 * open next iteration, the revision advances by exactly +1 and the ONLY
 * permitted change is the single `stage_iteration_opened` outcome (the
 * target generation appends exactly one iteration record on the wait
 * anchor and projects it open; every other durable region is pinned
 * positionally); when the completed state already carried the exact open
 * iteration, the ensure is the zero-dispatch recognition: zero revision
 * delta and every durable region unchanged. The wait, response, grant,
 * closure, cursor, journals, ledgers, generation prefix and identity
 * bindings must be identical before/after; the preserved iteration
 * closures are compared over their exact `by`/`closed_transition_count`/
 * `wait_index` triple (the wait index absent exactly for non-wait-bound
 * closures), and the zero-delta recognition additionally requires the
 * exact unchanged `updated_at`; a hostile ensure result is the
 * composition's own `invalid_result`, never a `TypeError`.
 *
 * Durability: the composed controllers' typed failures
 * (`state_persist_failed` windows, poisoned-sink refusals, grant/lifecycle
 * conflicts) pass through unchanged by identity; the suffix after a
 * failed command is never executed, a fresh retry executes only the
 * missing durable suffix, and nothing is ever rolled back or dispatched
 * twice. Two identical concurrent calls converge to one durable state
 * through the children's own reconciliations. The composition's own
 * failure reasons are exactly `invalid_options` and `invalid_result`;
 * unexpected errors keep their class and identity.
 *
 * The unified result is flat and content-free:
 * `{wait_index, intent_sha256, request_sha256, response_sha256,
 * additional_iterations, action_id: "continue_stage", action_to,
 * closed_iteration_index, iteration_index, generation_index,
 * compiled_stage, state}` — `closed_iteration_index` names the iteration
 * the grant closed, `iteration_index` the next iteration the ensure
 * opened (or found open), and `state` is the ensure controller's
 * authoritative post-ensure state. No manifests, canonical JSON, paths,
 * bodies or caller-owned objects beyond the exact frozen
 * `compiled_stage` enter the result.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ContinuedStageControllerError`,
 * `openPipelineV2ContinuedStageWithIo` and the frozen
 * `productionContinuedStageOps`; the public module exports exactly
 * `PipelineV2ContinuedStageControllerError` and
 * `openPipelineV2ContinuedStage` (types are not runtime keys).
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, model profile replacement, the automatic intervention
 * loop, the default-pipeline bundle, migrations/API/T3 and multi-process
 * locking (the graph transition and the next stage execution are the
 * coordinator resume's).
 */

export type PipelineV2ContinuedStageControllerFailureReason = "invalid_options" | "invalid_result";

/**
 * A failure of the composition layer itself with its stable
 * machine-readable `reason` and the last authoritative durable state
 * (`null` when the composition never reached a verified durable state).
 * Downstream controller failures pass through by identity and never take
 * this shape.
 */
export class PipelineV2ContinuedStageControllerError extends Error {
  readonly reason: PipelineV2ContinuedStageControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinuedStageControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinuedStageControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam shared with the two composed controllers; the
 * production `PipelineV2RunStateSink` satisfies it without an adapter.
 * The composition dispatches nothing itself.
 */
export interface PipelineV2ContinuedStageControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

/**
 * The single ops seam: exactly the two composed authoritative controllers
 * and nothing else. No reducer, filesystem, store, publisher, serializer,
 * registry, coordinator, runner or CLI capability is reachable through
 * it.
 */
export interface PipelineV2ContinuedStageOps {
  readonly completeStage: typeof completePipelineV2ContinueStage;
  readonly ensureStageIteration: typeof ensurePipelineV2StageIteration;
}

/**
 * The frozen production ops: the two existing authoritative controllers
 * bound by identity; no installer and no mutable module-global seam.
 */
export const productionContinuedStageOps: PipelineV2ContinuedStageOps = deepFreezeValue({
  completeStage: completePipelineV2ContinueStage,
  ensureStageIteration: ensurePipelineV2StageIteration,
}) as unknown as PipelineV2ContinuedStageOps;

export interface OpenPipelineV2ContinuedStageOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2ContinuedStageControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly initialBudget: number;
}

export interface OpenedPipelineV2ContinuedStage {
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

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function controllerError(
  reason: PipelineV2ContinuedStageControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinuedStageControllerError {
  return new PipelineV2ContinuedStageControllerError(reason, message, state);
}

/**
 * The fixed caller policy, captured once before the first side effect:
 * the stage resolved from the compiled plan through the intent's
 * `stage_id`, the derived declaration position, the compiled plan digest
 * and the intent's binding scalars. Nothing later — caller mutation,
 * hostile results, a hostile sink — can reinterpret these values.
 */
interface ContinuedStagePolicy {
  readonly runId: string;
  readonly waitIndex: number;
  readonly intentSha256: string;
  readonly stageId: string;
  readonly stagePosition: number;
  readonly compiledStage: CompiledPipelineV2RunPlanStage;
  readonly planSha256: string;
  readonly expectedPlanSha256: string;
  readonly initialBudget: number;
  readonly additionalIterations: number;
}

/**
 * The verified completed boundary, produced either by the full defensive
 * verification of the completion controller's successful result or by the
 * exact C5 recognition on the authoritative snapshot after the grant
 * controller's completed-boundary refusal.
 */
interface VerifiedCompletion {
  readonly state: PipelineV2RunState;
  readonly wait: PipelineV2WaitRecord;
  readonly waitIndex: number;
  readonly generationIndex: number;
  readonly closedIterationIndex: number;
  readonly openedNextIteration: boolean;
  readonly intentSha256: string;
  readonly additionalIterations: number;
  readonly requestSha256: string;
  readonly responseSha256: string;
  readonly actionTo: string;
}

/**
 * The contract-owned ordered action list equality: same length, same
 * order, same ids and targets. Total for malformed `before` values.
 */
function orderedActionsEqual(before: unknown, after: unknown): boolean {
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    const beforeAction = before[position];
    const afterAction = after[position];
    if (!isRecord(beforeAction) || !isRecord(afterAction) || beforeAction["id"] !== afterAction["id"] || beforeAction["to"] !== afterAction["to"]) {
      return false;
    }
  }
  return true;
}

function stringListEquals(before: unknown, after: unknown): boolean {
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (before[position] !== after[position]) {
      return false;
    }
  }
  return true;
}

function inputStateEquals(before: PipelineV2RunInputState, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["id"] === before.id &&
    after["type"] === before.type &&
    after["protected"] === before.protected &&
    after["digest"] === before.digest
  );
}

function transitionEquals(before: PipelineV2CommittedTransitionState, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before.index &&
    after["from"] === before.from &&
    after["outcome"] === before.outcome &&
    after["to"] === before.to &&
    after["execution_index"] === before.execution_index
  );
}

function grantEquals(before: PipelineV2IterationGrantState, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before.index &&
    after["generation_index"] === before.generation_index &&
    after["wait_index"] === before.wait_index &&
    after["intent_sha256"] === before.intent_sha256 &&
    after["additional_iterations"] === before.additional_iterations
  );
}

function agentOutputEquals(before: PipelineV2AgentOutputState, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["id"] === before.id &&
    after["digest"] === before.digest
  );
}

function agentOutputListEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (!agentOutputEquals(before[position], after[position])) {
      return false;
    }
  }
  return true;
}

function sessionCleanupEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["execution"] === before["execution"] &&
    after["tool"] === before["tool"]
  );
}

function decisionResultEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!isRecord(before) || !isRecord(after) || after["status"] !== before["status"]) {
    return false;
  }
  if (before["status"] === "selected") {
    return (
      after["outcome"] === before["outcome"] &&
      after["decision"] === before["decision"] &&
      after["rule_id"] === before["rule_id"] &&
      stringListEquals(before["active_constraint_ids"], after["active_constraint_ids"])
    );
  }
  if (before["status"] === "uncovered") {
    return (
      after["outcome"] === before["outcome"] &&
      stringListEquals(before["active_constraint_ids"], after["active_constraint_ids"])
    );
  }
  if (before["status"] === "inconsistent_facts") {
    return (
      after["outcome"] === before["outcome"] &&
      stringListEquals(before["violated_relation_ids"], after["violated_relation_ids"])
    );
  }
  return (
    after["outcome"] === before["outcome"] &&
    after["reason"] === before["reason"] &&
    after["fact_id"] === before["fact_id"] &&
    after["actual_type"] === before["actual_type"]
  );
}

function executionRecordEquals(before: PipelineV2ExecutionState, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before.index ||
    after["type"] !== before.type ||
    after["state_id"] !== before.state_id ||
    after["execution_role"] !== before.execution_role ||
    after["phase"] !== before.phase ||
    after["iteration_index"] !== before.iteration_index
  ) {
    return false;
  }
  if (before.type === "agent") {
    return (
      after["attempt"] === before.attempt &&
      after["profile"] === before.profile &&
      after["execution_session_id"] === before.execution_session_id &&
      after["tool_session_id"] === before.tool_session_id &&
      sessionCleanupEquals(before.session_cleanup, after["session_cleanup"]) &&
      agentOutputListEquals(before.outputs, after["outputs"]) &&
      after["failure_reason"] === before.failure_reason
    );
  }
  return (
    after["input_digest"] === before.input_digest &&
    decisionResultEquals(before.result, after["result"]) &&
    after["failure_reason"] === before.failure_reason
  );
}

function waitRecordEquals(before: PipelineV2WaitRecord, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before.index ||
    after["transition_count"] !== before.transition_count ||
    after["state_id"] !== before.state_id ||
    after["reason"] !== before.reason ||
    after["request_sha256"] !== before.request_sha256
  ) {
    return false;
  }
  if (!orderedActionsEqual(before.actions, after["actions"])) {
    return false;
  }
  const beforeIntent = before.intent;
  if (beforeIntent === undefined) {
    if (after["intent"] !== undefined) {
      return false;
    }
  } else if (!isRecord(after["intent"]) || after["intent"]["intent_sha256"] !== beforeIntent.intent_sha256) {
    return false;
  }
  const beforeResponse = before.response;
  if (beforeResponse === undefined) {
    return after["response"] === undefined;
  }
  return (
    isRecord(after["response"]) &&
    after["response"]["action_id"] === beforeResponse.action_id &&
    after["response"]["response_sha256"] === beforeResponse.response_sha256
  );
}

function taskLedgerEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.task_revisions) || !Array.isArray(before.task_revisions) || after.task_revisions.length !== before.task_revisions.length) {
    return false;
  }
  return before.task_revisions.every((beforeEntry, position) => {
    const afterEntry = after.task_revisions[position];
    return (
      isRecord(beforeEntry) &&
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

function planLedgerEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.plan_revisions) || !Array.isArray(before.plan_revisions) || after.plan_revisions.length !== before.plan_revisions.length) {
    return false;
  }
  return before.plan_revisions.every((beforeEntry, position) => {
    const afterEntry = after.plan_revisions[position];
    return (
      isRecord(beforeEntry) &&
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["origin_execution"] === beforeEntry.origin_execution
    );
  });
}

function iterationProjectionEquals(
  before: unknown,
  after: unknown,
): boolean {
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

function closureProjectionEquals(
  before: unknown,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["by"] === before["by"] &&
    after["closed_transition_count"] === before["closed_transition_count"] &&
    // The schema-owned wait index is part of the exact closure identity:
    // present exactly for wait-bound closures and absent otherwise, so
    // the comparison includes it exactly.
    after["wait_index"] === before["wait_index"]
  );
}

function iterationRecordEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (after["index"] !== before["index"] || after["opened_transition_count"] !== before["opened_transition_count"]) {
    return false;
  }
  return closureProjectionEquals(before["closed"], after["closed"]);
}

/**
 * The exact positional equality of one durable generation record as the
 * ensure step must leave it. Total for malformed values.
 */
function generationUnchanged(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before["index"] ||
    after["stage_id"] !== before["stage_id"] ||
    after["stage_position"] !== before["stage_position"] ||
    after["template_id"] !== before["template_id"] ||
    after["plan_sha256"] !== before["plan_sha256"] ||
    after["initial_budget"] !== before["initial_budget"] ||
    after["opened_transition_count"] !== before["opened_transition_count"] ||
    after["iteration_count"] !== before["iteration_count"] ||
    !iterationProjectionEquals(before["open_iteration"], after["open_iteration"]) ||
    !closureProjectionEquals(before["closed"], after["closed"]) ||
    !Array.isArray(before["iterations"]) ||
    !Array.isArray(after["iterations"]) ||
    after["iterations"].length !== before["iterations"].length
  ) {
    return false;
  }
  const beforeIterations = before["iterations"] as unknown[];
  const afterIterations = after["iterations"] as unknown[];
  for (let position = 0; position < beforeIterations.length; position += 1) {
    if (!iterationRecordEquals(beforeIterations[position], afterIterations[position])) {
      return false;
    }
  }
  return true;
}

/**
 * The exact positional equality of the durable regions the ensure step
 * must leave unchanged, compared against the verified completed state.
 */
function inputListEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(before.inputs) || !Array.isArray(after.inputs) || after.inputs.length !== before.inputs.length) {
    return false;
  }
  return before.inputs.every((beforeEntry, position) => inputStateEquals(beforeEntry, after.inputs[position]));
}

function transitionListEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(before.transitions) || !Array.isArray(after.transitions) || after.transitions.length !== before.transitions.length) {
    return false;
  }
  return before.transitions.every((beforeEntry, position) => transitionEquals(beforeEntry, after.transitions[position]));
}

function executionListEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(before.executions) || !Array.isArray(after.executions) || after.executions.length !== before.executions.length) {
    return false;
  }
  return before.executions.every((beforeEntry, position) => executionRecordEquals(beforeEntry, after.executions[position]));
}

function waitListEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(before.waits) || !Array.isArray(after.waits) || after.waits.length !== before.waits.length) {
    return false;
  }
  return before.waits.every((beforeEntry, position) => waitRecordEquals(beforeEntry, after.waits[position]));
}

function grantListEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(before.grants) || !Array.isArray(after.grants) || after.grants.length !== before.grants.length) {
    return false;
  }
  return before.grants.every((beforeEntry, position) => grantEquals(beforeEntry, after.grants[position]));
}

/**
 * The full defensive verification of the completed boundary state against
 * the fixed caller policy. Used by the completion-result verification
 * (throwing the composition's own `invalid_result`) and — wrapped in a
 * fall-through catch — by the exact C5 recognition on the authoritative
 * snapshot after the grant controller's completed-boundary refusal. Every
 * field access is guarded: a malformed document can never
 * escape as a `TypeError`; diagnostics are fixed content-free strings
 * that never echo unchecked hostile values.
 */
function verifyCompletedShape(stateValue: unknown, policy: ContinuedStagePolicy): VerifiedCompletion {
  if (!isRecord(stateValue)) {
    throw controllerError("invalid_result", "the completed continue-stage result carries no durable run state", null);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (state.status !== "active" || state.phase !== "running") {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state is not at the active running boundary",
      state,
    );
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state already carries a terminal, publication or failure projection",
      state,
    );
  }
  if (!isString(state.run_id) || state.run_id !== policy.runId) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state belongs to a different run than the accepted intent",
      state,
    );
  }
  // The declared wait: the last record and the only record of its index,
  // carrying the exact accepted intent, the declared continue_stage
  // action and the exact durable response. An open last wait (a new
  // unresponded wait) can never satisfy this.
  const waits: unknown = state.waits;
  if (!Array.isArray(waits)) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state carries no wait journal",
      state,
    );
  }
  let occurrences = 0;
  let waitPosition = -1;
  for (let position = 0; position < waits.length; position += 1) {
    if (!isRecord(waits[position])) {
      throw controllerError(
        "invalid_result",
        "the completed continue-stage state carries a malformed wait record",
        state,
      );
    }
    if ((waits[position] as Record<string, unknown>)["index"] === policy.waitIndex) {
      occurrences += 1;
      waitPosition = position;
    }
  }
  if (occurrences !== 1 || waitPosition !== waits.length - 1) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage wait is not the last and only record of its index",
      state,
    );
  }
  const wait = waits[waitPosition] as PipelineV2WaitRecord;
  if (!isRecord(wait["intent"]) || wait["intent"]["intent_sha256"] !== policy.intentSha256) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage wait does not carry the exact accepted intent",
      state,
    );
  }
  if (!Array.isArray(wait["actions"])) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage wait declares no ordered actions",
      state,
    );
  }
  let declaredTo: string | undefined;
  let declaredCount = 0;
  for (const action of wait["actions"] as unknown[]) {
    if (!isRecord(action)) {
      throw controllerError(
        "invalid_result",
        "the completed continue-stage wait declares a malformed action",
        state,
      );
    }
    if (action["id"] === CONTINUE_STAGE_ACTION_ID) {
      declaredCount += 1;
      if (isString(action["to"])) {
        declaredTo = action["to"];
      }
    }
  }
  if (
    declaredCount !== 1 ||
    declaredTo === undefined ||
    declaredTo !== policy.compiledStage.entry_state
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage wait does not declare the continue_stage action onto the compiled stage entry exactly",
      state,
    );
  }
  if (
    !isRecord(wait["response"]) ||
    wait["response"]["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
    !isString(wait["response"]["response_sha256"])
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage wait does not carry the exact durable continue_stage response",
      state,
    );
  }
  // The cursor and the journals sit exactly at the wait boundary; the
  // execution and transition history has not advanced past the answered
  // wait.
  const cursor: unknown = state.cursor;
  if (
    !isRecord(cursor) ||
    cursor["current_state"] !== declaredTo ||
    cursor["transition_count"] !== wait["transition_count"]
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage cursor is not at the declared action target on the wait boundary",
      state,
    );
  }
  if (
    !Array.isArray(state.transitions) ||
    state.transitions.length !== wait["transition_count"] ||
    !state.transitions.every((entry) => isRecord(entry))
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage transition journal does not sit at the wait boundary",
      state,
    );
  }
  if (
    !Array.isArray(state.executions) ||
    state.executions.length !== wait["transition_count"] ||
    !state.executions.every((entry) => isRecord(entry))
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage execution journal does not sit at the wait boundary",
      state,
    );
  }
  // Exactly one grant matches the (generation, wait) pair with the exact
  // intent digest and additional iteration count.
  if (!Array.isArray(state.grants)) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state carries no grant ledger",
      state,
    );
  }
  for (const entry of state.grants) {
    if (!isRecord(entry)) {
      throw controllerError(
        "invalid_result",
        "the completed continue-stage state carries a malformed grant record",
        state,
      );
    }
  }
  let grantCount = 0;
  let generationIndex = 0;
  for (const entry of state.grants as PipelineV2IterationGrantState[]) {
    if (
      entry.wait_index === policy.waitIndex &&
      entry.intent_sha256 === policy.intentSha256 &&
      entry.additional_iterations === policy.additionalIterations
    ) {
      grantCount += 1;
      generationIndex = entry.generation_index;
    }
  }
  if (grantCount !== 1 || !isPositiveSafeInteger(generationIndex)) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage boundary does not carry exactly one matching durable grant",
      state,
    );
  }
  // The exact grant-pair uniqueness: for the granted (generation, wait)
  // pair exactly one durable grant record exists, and exactly that
  // record carries the exact intent digest and additional iteration
  // count — a second conflicting record of the same pair is never a
  // valid boundary, even when one of the copies matches exactly.
  let pairCount = 0;
  let pairGrant: PipelineV2IterationGrantState | undefined;
  for (const entry of state.grants as PipelineV2IterationGrantState[]) {
    if (entry.generation_index === generationIndex && entry.wait_index === policy.waitIndex) {
      pairCount += 1;
      pairGrant = entry;
    }
  }
  if (
    pairCount !== 1 ||
    pairGrant === undefined ||
    pairGrant.intent_sha256 !== policy.intentSha256 ||
    pairGrant.additional_iterations !== policy.additionalIterations
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage boundary does not carry exactly one grant of the granted generation and wait pair",
      state,
    );
  }
  // The target generation: the last open one, bound exactly to the fixed
  // stage/plan/budget policy on the wait's anchor.
  if (!Array.isArray(state.generations)) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage state carries no generation journal",
      state,
    );
  }
  if (state.generations.length !== generationIndex) {
    throw controllerError(
      "invalid_result",
      "the granted generation is not the last durable generation",
      state,
    );
  }
  const generationValue: unknown = state.generations[generationIndex - 1];
  if (
    !isRecord(generationValue) ||
    generationValue["index"] !== generationIndex ||
    generationValue["closed"] !== undefined
  ) {
    throw controllerError(
      "invalid_result",
      "the completed continue-stage boundary does not carry the last open granted generation",
      state,
    );
  }
  const generation = generationValue as unknown as PipelineV2StageGenerationRecord;
  if (
    generation.stage_id !== policy.stageId ||
    generation.stage_position !== policy.stagePosition ||
    generation.template_id !== policy.compiledStage.template ||
    generation.plan_sha256 !== policy.planSha256 ||
    generation.plan_sha256 !== policy.expectedPlanSha256 ||
    generation.initial_budget !== policy.initialBudget
  ) {
    throw controllerError(
      "invalid_result",
      "the granted generation does not match the fixed stage, plan and budget policy on the wait anchor",
      state,
    );
  }
  // The grant closed exactly one iteration of this generation with the
  // exact grant closure; the open next iteration, if any, is exactly
  // `closed_iteration_index + 1` on the same anchor.
  if (!Array.isArray(generation.iterations)) {
    throw controllerError(
      "invalid_result",
      "the granted generation carries no iteration history",
      state,
    );
  }
  for (const iteration of generation.iterations) {
    if (!isRecord(iteration)) {
      throw controllerError(
        "invalid_result",
        "the granted generation carries a malformed iteration record",
        state,
      );
    }
  }
  const iterations = generation.iterations as PipelineV2StageIterationRecord[];
  let closedIteration: PipelineV2StageIterationRecord | undefined;
  let closedCount = 0;
  for (const iteration of iterations) {
    const closed: unknown = iteration["closed"];
    if (
      isRecord(closed) &&
      closed["by"] === "grant" &&
      closed["wait_index"] === policy.waitIndex &&
      closed["closed_transition_count"] === wait["transition_count"]
    ) {
      closedCount += 1;
      closedIteration = iteration;
    }
  }
  if (closedCount !== 1 || closedIteration === undefined || !isPositiveSafeInteger(closedIteration.index)) {
    throw controllerError(
      "invalid_result",
      "the granted generation does not carry exactly one iteration closed by the exact grant closure",
      state,
    );
  }
  const closedIterationIndex = closedIteration.index;
  const openIteration: unknown = generation["open_iteration"];
  if (openIteration === undefined) {
    // Retry shape (a): the next iteration is not yet opened; the
    // grant-closed iteration is the last one and the counts agree.
    if (
      generation.iteration_count !== iterations.length ||
      iterations.length !== closedIterationIndex ||
      iterations[iterations.length - 1] !== closedIteration
    ) {
      throw controllerError(
        "invalid_result",
        "the granted generation does not close with its exact grant-closed iteration",
        state,
      );
    }
  } else {
    // Retry shape (b): the exact next iteration is already opened by a
    // concurrent or previous successful call.
    if (
      !isRecord(openIteration) ||
      openIteration["index"] !== closedIterationIndex + 1 ||
      openIteration["opened_transition_count"] !== wait["transition_count"] ||
      generation.iteration_count !== iterations.length ||
      iterations.length !== closedIterationIndex + 1 ||
      generation.iteration_count !== closedIterationIndex + 1
    ) {
      throw controllerError(
        "invalid_result",
        "the granted generation's open iteration is not the exact successor of the grant-closed iteration",
        state,
      );
    }
    const lastIteration: unknown = iterations[iterations.length - 1];
    if (
      !isRecord(lastIteration) ||
      lastIteration["index"] !== closedIterationIndex + 1 ||
      lastIteration["opened_transition_count"] !== wait["transition_count"] ||
      lastIteration["closed"] !== undefined
    ) {
      throw controllerError(
        "invalid_result",
        "the granted generation's last iteration is not the exact open successor of the grant-closed iteration",
        state,
      );
    }
  }
  return {
    state,
    wait,
    waitIndex: policy.waitIndex,
    generationIndex,
    closedIterationIndex,
    openedNextIteration: openIteration !== undefined,
    intentSha256: policy.intentSha256,
    additionalIterations: policy.additionalIterations,
    requestSha256: wait.request_sha256,
    responseSha256: wait["response"]["response_sha256"] as string,
    actionTo: declaredTo,
  };
}

/**
 * The full verification of the completion controller's successful result
 * against the fixed policy: the flat result fields, the intent bindings
 * and the completed-boundary state shape. Any mismatch is the
 * composition's own `invalid_result` before the ensure call; diagnostics
 * are content-free.
 */
function verifyCompletionResult(resultValue: unknown, policy: ContinuedStagePolicy): VerifiedCompletion {
  if (!isRecord(resultValue)) {
    throw controllerError("invalid_result", "the continue-stage completion result is not a record", null);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    !isPositiveSafeInteger(result["wait_index"]) ||
    !isPositiveSafeInteger(result["generation_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    !isPositiveSafeInteger(result["additional_iterations"]) ||
    !isString(result["intent_sha256"]) ||
    !isString(result["request_sha256"]) ||
    !isString(result["response_sha256"]) ||
    result["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
    !isString(result["action_to"])
  ) {
    throw controllerError(
      "invalid_result",
      "the continue-stage completion result carries malformed result fields",
      null,
    );
  }
  if (
    result["wait_index"] !== policy.waitIndex ||
    result["intent_sha256"] !== policy.intentSha256 ||
    result["additional_iterations"] !== policy.additionalIterations ||
    result["action_to"] !== policy.compiledStage.entry_state
  ) {
    throw controllerError(
      "invalid_result",
      "the continue-stage completion result does not match the accepted intent and the selected stage",
      null,
    );
  }
  const completed = verifyCompletedShape(result["state"], policy);
  if (completed.responseSha256 !== result["response_sha256"] || completed.requestSha256 !== result["request_sha256"]) {
    throw controllerError(
      "invalid_result",
      "the continue-stage completion result digests do not match the durable response record",
      completed.state,
    );
  }
  // The flat result indexes are bound exactly to the verified durable
  // grant boundary: the generation the grant extended and the iteration
  // the grant closed, as the completed-boundary verification derived
  // them from the durable records.
  if (
    result["generation_index"] !== completed.generationIndex ||
    result["iteration_index"] !== completed.closedIterationIndex
  ) {
    throw controllerError(
      "invalid_result",
      "the continue-stage completion result indexes do not match the verified durable grant boundary",
      completed.state,
    );
  }
  return completed;
}

/**
 * The full verification of the ensure controller's successful result
 * against the verified completed state: the exact trusted compiled stage
 * object (identity), the same generation and the exact successor
 * iteration, and the exact durable delta — one appended open iteration
 * (+1 revision) or the zero-dispatch recognition of the already open one
 * (zero revision delta). Everything else is pinned positionally against
 * the verified completed state; a hostile or malformed result is the
 * composition's own `invalid_result`, never a `TypeError`.
 */
function verifyEnsureResult(
  resultValue: unknown,
  before: VerifiedCompletion,
  policy: ContinuedStagePolicy,
): EnsuredPipelineV2StageIteration {
  if (!isRecord(resultValue)) {
    throw controllerError("invalid_result", "the stage iteration result is not a record", before.state);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    result["compiled_stage"] !== policy.compiledStage ||
    !isPositiveSafeInteger(result["generation_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"])
  ) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result does not carry the exact trusted compiled stage",
      before.state,
    );
  }
  if (result["generation_index"] !== before.generationIndex || result["iteration_index"] !== before.closedIterationIndex + 1) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result does not open the exact successor iteration of the granted generation",
      before.state,
    );
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw controllerError("invalid_result", "the stage iteration result carries no durable run state", before.state);
  }
  const after = stateValue as unknown as PipelineV2RunState;
  const beforeState = before.state;
  const mismatch = (): PipelineV2ContinuedStageControllerError =>
    controllerError(
      "invalid_result",
      "the stage iteration result state does not carry the exact next-iteration opening over the verified completion boundary",
      after,
    );
  // The exact revision delta of the ensure step: from a not-yet-opened
  // boundary the single iteration opening is committed (+1); from the
  // already-opened retry shape the ensure is the zero-dispatch
  // recognition (no revision change).
  const expectedRevisionDelta = before.openedNextIteration ? 0 : 1;
  if (after.revision !== beforeState.revision + expectedRevisionDelta) {
    throw mismatch();
  }
  if (after.status !== beforeState.status || after.phase !== beforeState.phase) {
    throw mismatch();
  }
  if (after.schema_version !== beforeState.schema_version || after.run_id !== beforeState.run_id || after.started_at !== beforeState.started_at) {
    throw mismatch();
  }
  if (
    !isRecord(after.pipeline) ||
    !isRecord(beforeState.pipeline) ||
    comparePipelineV2RunIdentity(beforeState.pipeline, after.pipeline).kind !== "match"
  ) {
    throw mismatch();
  }
  const beforeCursor: unknown = beforeState.cursor;
  const afterCursor: unknown = after.cursor;
  if (
    !isRecord(beforeCursor) ||
    !isRecord(afterCursor) ||
    afterCursor["current_state"] !== beforeCursor["current_state"] ||
    afterCursor["transition_count"] !== beforeCursor["transition_count"]
  ) {
    throw mismatch();
  }
  if (!inputListEquals(beforeState, after) || !transitionListEquals(beforeState, after) || !executionListEquals(beforeState, after)) {
    throw mismatch();
  }
  if (!waitListEquals(beforeState, after) || !taskLedgerEquals(beforeState, after) || !planLedgerEquals(beforeState, after) || !grantListEquals(beforeState, after)) {
    throw mismatch();
  }
  if (after.terminal !== undefined || after.run_outputs !== undefined || after.failure !== undefined) {
    throw mismatch();
  }
  if (!Array.isArray(after.generations)) {
    throw mismatch();
  }
  if (after.generations.length !== beforeState.generations.length) {
    throw mismatch();
  }
  const targetPosition = before.generationIndex - 1;
  for (let position = 0; position < after.generations.length; position += 1) {
    if (position === targetPosition) {
      continue;
    }
    if (!generationUnchanged(beforeState.generations[position], after.generations[position])) {
      throw mismatch();
    }
  }
  // The target generation: the identity bindings and closures stay
  // exactly as the verified completed state carried them; the only
  // permitted change is the single appended open iteration (or nothing).
  const beforeGenerationValue: unknown = beforeState.generations[targetPosition];
  const afterGenerationValue: unknown = after.generations[targetPosition];
  if (!isRecord(beforeGenerationValue) || !isRecord(afterGenerationValue)) {
    throw mismatch();
  }
  const beforeGeneration = beforeGenerationValue as unknown as PipelineV2StageGenerationRecord;
  const afterGeneration = afterGenerationValue as unknown as PipelineV2StageGenerationRecord;
  if (
    afterGeneration.index !== beforeGeneration.index ||
    afterGeneration.stage_id !== beforeGeneration.stage_id ||
    afterGeneration.stage_position !== beforeGeneration.stage_position ||
    afterGeneration.template_id !== beforeGeneration.template_id ||
    afterGeneration.plan_sha256 !== beforeGeneration.plan_sha256 ||
    afterGeneration.initial_budget !== beforeGeneration.initial_budget ||
    afterGeneration.opened_transition_count !== beforeGeneration.opened_transition_count ||
    afterGeneration.closed !== undefined ||
    beforeGeneration.closed !== undefined
  ) {
    throw mismatch();
  }
  if (
    !Array.isArray(beforeGeneration.iterations) ||
    !Array.isArray(afterGeneration.iterations) ||
    beforeGeneration.iteration_count !== beforeGeneration.iterations.length ||
    afterGeneration.iteration_count !== afterGeneration.iterations.length
  ) {
    throw mismatch();
  }
  const beforeIterations = beforeGeneration.iterations as PipelineV2StageIterationRecord[];
  const afterIterations = afterGeneration.iterations as PipelineV2StageIterationRecord[];
  if (expectedRevisionDelta === 0) {
    // The already-opened retry: the whole durable generation record stays
    // exactly as the verified completed state carried it, and the
    // zero-dispatch recognition changes no durable byte — the exact
    // `updated_at` included. (The +1 opening path deliberately does not
    // pin `updated_at`: the reducer refreshes it on the committed
    // opening.)
    if (
      beforeGeneration.iteration_count !== before.closedIterationIndex + 1 ||
      !generationUnchanged(beforeGeneration, afterGeneration) ||
      after.updated_at !== beforeState.updated_at
    ) {
      throw mismatch();
    }
    return resultValue as unknown as EnsuredPipelineV2StageIteration;
  }
  // The opening path: exactly one iteration appended on the wait anchor.
  if (
    beforeGeneration.iteration_count !== before.closedIterationIndex ||
    afterGeneration.iteration_count !== before.closedIterationIndex + 1 ||
    afterIterations.length !== beforeIterations.length + 1
  ) {
    throw mismatch();
  }
  for (let position = 0; position < beforeIterations.length; position += 1) {
    if (!iterationRecordEquals(beforeIterations[position], afterIterations[position])) {
      throw mismatch();
    }
  }
  const appended = afterIterations[afterIterations.length - 1];
  const anchor = before.wait["transition_count"];
  if (
    !isRecord(appended) ||
    appended["index"] !== before.closedIterationIndex + 1 ||
    appended["opened_transition_count"] !== anchor ||
    appended["closed"] !== undefined
  ) {
    throw mismatch();
  }
  const openProjection: unknown = afterGeneration["open_iteration"];
  if (
    !isRecord(openProjection) ||
    openProjection["index"] !== before.closedIterationIndex + 1 ||
    openProjection["opened_transition_count"] !== anchor ||
    beforeGeneration["open_iteration"] !== undefined
  ) {
    throw mismatch();
  }
  return resultValue as unknown as EnsuredPipelineV2StageIteration;
}

/**
 * The fixed caller policy capture: every options field is read exactly
 * once, the intent's provenance is gated before any of its fields is
 * read, and the stage is resolved only through the single trusted
 * compiled resolver. Resolver errors keep their classes; every other
 * rejection is the composition's own `invalid_options` before the first
 * side effect.
 */
function capturePolicy(optionsValue: unknown): {
  readonly policy: ContinuedStagePolicy;
  readonly runRoot: string;
  readonly sink: PipelineV2ContinuedStageControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
} {
  if (!isRecord(optionsValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires an options object", null);
  }
  const options = optionsValue as Record<string, unknown>;
  const runRootValue = options["runRoot"];
  const sinkValue = options["sink"];
  const intentValue = options["intent"];
  const compiledPlanValue = options["compiledPlan"];
  const initialBudgetValue = options["initialBudget"];
  if (!isString(runRootValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires a runRoot string", null);
  }
  if (!isRecord(sinkValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires a state sink", null);
  }
  if (!isRecord(intentValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires a prepared wait intent object", null);
  }
  if (!isRecord(compiledPlanValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires a compiled run plan object", null);
  }
  if (!isPositiveSafeInteger(initialBudgetValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires a positive safe integer initial budget", null);
  }
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the continue-stage kind. Hand
  // -built, cast, spread, cloned and Proxy look-alikes are rejected here,
  // before any intent field is read.
  if (!hasPreparedRunPlanProvenance(intentValue, "continue_stage_intent")) {
    throw controllerError(
      "invalid_options",
      "the prepared wait intent is not a provenance-registered continue_stage_intent",
      null,
    );
  }
  const intent = intentValue as unknown as PreparedPipelineV2RunWaitIntent;
  const manifestValue: unknown = intent.manifest;
  if (
    !isRecord(manifestValue) ||
    manifestValue["kind"] !== "continue_stage_intent" ||
    !isString(manifestValue["run_id"]) ||
    !isPositiveSafeInteger(manifestValue["wait_index"]) ||
    !isString(manifestValue["stage_id"]) ||
    !isString(manifestValue["expected_plan_sha256"]) ||
    !isPositiveSafeInteger(manifestValue["additional_iterations"])
  ) {
    throw controllerError(
      "invalid_options",
      "the prepared wait intent does not carry the continue_stage contract fields",
      null,
    );
  }
  const manifest = manifestValue as unknown as PipelineV2ContinueStageIntentManifest;
  const compiledPlan = compiledPlanValue as unknown as CompiledPipelineV2RunPlan;
  // The single trusted compiled resolver: its provenance gate, stage
  // existence and template binding belong to it alone; its errors pass
  // by identity.
  const compiledStage = compiledPipelineV2RunPlanStageFor(compiledPlan, manifest.stage_id);
  if (!Array.isArray(compiledPlan.stages)) {
    throw new Error("pipeline v2 continued stage controller invariant violated: the compiled plan carries no stages");
  }
  const declarationIndex = compiledPlan.stages.findIndex((stage) => stage.id === compiledStage.id);
  if (declarationIndex < 0) {
    throw new Error("pipeline v2 continued stage controller invariant violated: the compiled stage is not in the compiled plan");
  }
  return {
    policy: {
      runId: manifest.run_id,
      waitIndex: manifest.wait_index,
      intentSha256: intent.sha256,
      stageId: compiledStage.id,
      stagePosition: declarationIndex + 1,
      compiledStage,
      planSha256: compiledPlan.plan_sha256,
      expectedPlanSha256: manifest.expected_plan_sha256,
      initialBudget: initialBudgetValue,
      additionalIterations: manifest.additional_iterations,
    },
    runRoot: runRootValue,
    sink: sinkValue as unknown as PipelineV2ContinuedStageControllerSink,
    intent,
    compiledPlan,
  };
}

/**
 * Validate, compose and open the continued stage through the existing
 * controllers (see the module docstring for the full order and retry
 * semantics).
 */
export async function openPipelineV2ContinuedStageWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<OpenedPipelineV2ContinuedStage> {
  // Capture boundary: the options shape and every options field through
  // the fixed capture, then both ops getters, each read exactly once
  // before the first await; the caller policy is fixed from the captured
  // values, so later caller mutations cannot influence the execution.
  const captured = capturePolicy(optionsValue);
  const { policy, runRoot, sink, intent, compiledPlan } = captured;
  if (!isRecord(opsValue)) {
    throw controllerError("invalid_options", "openPipelineV2ContinuedStage requires an ops object", null);
  }
  const ops = opsValue as Record<string, unknown>;
  const completeStage = ops["completeStage"];
  const ensureStageIteration = ops["ensureStageIteration"];
  if (typeof completeStage !== "function" || typeof ensureStageIteration !== "function") {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ContinuedStage requires the two composed controller functions",
      null,
    );
  }

  // Step 1: complete the durable continue-stage flow through the
  // existing controller; its provenance gates, state validation,
  // reconciliation, dispatch verification and durability semantics are
  // authoritative and its typed errors keep their identity.
  let completed: VerifiedCompletion | null = null;
  try {
    const completionResult = await (completeStage as typeof completePipelineV2ContinueStage)({
      runRoot,
      sink: sink as unknown as PipelineV2ContinuedStageControllerSink,
      intent,
    });
    // Step 2: the full defensive verification of the successful
    // completion result — every binding against the fixed policy; a
    // mismatch or malformed shape is this composition's `invalid_result`
    // before the ensure call.
    completed = verifyCompletionResult(completionResult, policy);
  } catch (cause) {
    // The C5 recognition: the grant controller pins the completed
    // boundary before any next iteration, so it refuses — with zero
    // dispatch and zero filesystem work — exactly the state a previous
    // fully successful call (or a racing identical call that already
    // opened the next iteration) produced. The exact completed-and-opened
    // shape against the fixed policy continues the composition with the
    // ensure; every other failure — including a deviating open iteration,
    // a genuine grant/lifecycle conflict and any other downstream or
    // unexpected error — is re-thrown unchanged (the same object
    // identity).
    if (
      cause instanceof PipelineV2ContinueStageGrantControllerError &&
      cause.reason === "lifecycle_conflict"
    ) {
      const snapshotValue: unknown = (sink as unknown as Record<string, unknown>)["snapshot"];
      try {
        const recognized = verifyCompletedShape(snapshotValue, policy);
        if (recognized.openedNextIteration) {
          completed = recognized;
        }
      } catch {
        // Not the exact completed-and-opened shape: the original failure
        // stands.
      }
    }
    if (completed === null) {
      throw cause;
    }
  }

  // Step 3: open (or recognize) the next iteration of the same
  // generation through the existing stage-iteration controller, with the
  // fixed stage id, the caller-owned initial budget and the same sink.
  // The effective budget is never computed here; the controller's own
  // conflicts pass by identity.
  const ensureResult = await (ensureStageIteration as typeof ensurePipelineV2StageIteration)({
    compiledPlan,
    stageId: policy.stageId,
    initialBudget: policy.initialBudget,
    sink: sink as unknown as PipelineV2ContinuedStageControllerSink,
  });
  // Step 4: the full defensive verification of the successful ensure
  // result against the verified completed boundary.
  const ensured = verifyEnsureResult(ensureResult, completed, policy);

  // The unified flat content-free result over the ensure controller's
  // authoritative state.
  return deepFreezeValue({
    wait_index: completed.waitIndex,
    intent_sha256: completed.intentSha256,
    request_sha256: completed.requestSha256,
    response_sha256: completed.responseSha256,
    additional_iterations: completed.additionalIterations,
    action_id: CONTINUE_STAGE_ACTION_ID,
    action_to: completed.actionTo,
    closed_iteration_index: completed.closedIterationIndex,
    iteration_index: ensured.iteration_index,
    generation_index: completed.generationIndex,
    compiled_stage: policy.compiledStage,
    state: ensured.state,
  }) as OpenedPipelineV2ContinuedStage;
}
