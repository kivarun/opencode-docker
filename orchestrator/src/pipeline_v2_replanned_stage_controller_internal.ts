import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { closePipelineV2ReplannedGeneration, type ClosedPipelineV2ReplannedGeneration } from "./pipeline_v2_replanned_generation_controller.ts";
import { ensurePipelineV2StageIteration, type EnsuredPipelineV2StageIteration } from "./pipeline_v2_stage_iteration_controller.ts";
import { compiledPipelineV2RunPlanStageFor, type CompiledPipelineV2RunPlan, type CompiledPipelineV2RunPlanStage } from "./pipeline_v2_run_plan_compiled.ts";
import type { PreparedPipelineV2RunWaitIntent } from "./pipeline_v2_run_plan_manifests.ts";
import type {
  PipelineV2RunCommand,
  PipelineV2RunState,
  PipelineV2WaitRecord,
  PipelineV2RunInputState,
  PipelineV2CommittedTransitionState,
  PipelineV2IterationGrantState,
  PipelineV2AgentOutputState,
  PipelineV2SessionCleanupPair,
  PipelineDecisionStateRecord,
  PipelineV2ExecutionState,
  PipelineV2RunOutputState,
} from "./pipeline_v2_state.ts";

/**
 * Production-neutral replanned-stage composition controller (unwired).
 *
 * After a NEW plan revision has already been durably accepted, this
 * controller composes the two existing authoritative controllers in one
 * fixed order to move the run onto the caller-selected stage of that
 * plan: `closePipelineV2ReplannedGeneration` closes the old generation
 * `by: "replanned"` (C0 dispatch, C1/C2 zero-dispatch retries), and
 * `ensurePipelineV2StageIteration` guarantees the open generation bound
 * to the selected compiled stage plus its open iteration 1. The
 * composition never runs a reducer, never builds or accepts a plan
 * candidate, never publishes any manifest, never performs filesystem
 * work, never commits a graph transition, and never resumes the run.
 * It adds no second provenance registry, no second state validator and
 * no second cursor: everything durable flows through the composed
 * controllers' own verification and the single state validation they
 * perform.
 *
 * `stageId` and `initialBudget` are explicit caller-policy decisions;
 * the controller selects nothing itself. `compiledPlan` must be the
 * exact provenance-backed compiled plan the existing run-plan
 * acceptance returned; `intent` must be the exact provenance-backed
 * prepared `revise_task_intent` that already carries the accepted task
 * revision.
 *
 * Capture and pre-side-effect validation (fail-closed, tested): the
 * options shape; then `sink` → `intent` → `compiledPlan` → `stageId` →
 * `initialBudget` each read exactly once; then both ops getters
 * (`closeGeneration`, `ensureStageIteration`) read exactly once before
 * the first await; the `initialBudget` must be a positive safe integer
 * and the stage id must resolve through the existing trusted compiled
 * resolver `compiledPipelineV2RunPlanStageFor` (its provenance gate,
 * stage existence and template binding belong to that resolver alone;
 * compiled-resolver errors pass by identity) — any invalid shape,
 * forged or cloned compiled plan or invalid budget refuses before any
 * durable dispatch, before any close call and with zero composed
 * effects. The stage position is derived only from the trusted
 * `compiledPlan.stages` declaration order. The controller never reads
 * or binds `sink.dispatch` itself: dispatch belongs to the two
 * composed controllers.
 *
 * Composition ordering (tested): `closeGeneration` runs first; its
 * returned result is verified COMPLETELY before `ensureStageIteration`
 * is called (a hostile, malformed or binding-mismatched close result
 * never reaches the ensure call); only then the ensure result is
 * verified against the trusted compiled stage, the verified close
 * result and the intent/plan bindings; the result is built from the
 * ensure controller's authoritative state without any additional
 * `sink.snapshot` read.
 *
 * Close-result verification (defensive, contract-owned, targeted):
 * the positive safe wait/generation/iteration/task/plan indexes; the
 * exact `intent_sha256`; the exact plan revision, digest and origin
 * against the compiled plan; the old generation at its exact durable
 * index with the exact replanned iteration closure and the exact
 * replanned generation closure; the wait, task-ledger and plan-ledger
 * bindings agreeing with the result; and the admissible state form —
 * C1: the old generation is the last durable generation; C2: exactly
 * one still-open generation of the current compiled plan follows it in
 * one of the two immediate opening forms. Malformed nested results
 * fail as the controller's own `invalid_result`, never a `TypeError`.
 *
 * Ensure-result verification (defensive, targeted): `compiled_stage`
 * is the exact object the trusted resolver returned (identity, never a
 * clone); the generation index is exactly the old index + 1 and the
 * iteration index exactly 1; the final generation is the last durable
 * generation, still open, with the exact stage id, stage position,
 * template, current plan digest, initial budget and the wait-boundary
 * opening anchor; exactly one iteration (index 1, open on the same
 * anchor, exact `open_iteration` projection); the old generation and
 * its historical iteration prefix/closure are unchanged; the wait,
 * task, plan, execution and transition boundaries did not advance
 * (positional equality against the verified close state, with the
 * routine `updated_at` refresh allowed); the final state remains
 * active and running on the settled-but-unbound planning execution. A
 * hostile coherent result with simultaneously mutated result fields
 * and nested state is compared against the verified close state and
 * the trusted compiled stage, never against itself. Malformed shapes
 * fail as `invalid_result`, never a `TypeError`.
 *
 * Durability: the composed controllers' typed failures
 * (`state_persist_failed` with `not_committed`/`durability_unknown`
 * semantics, poisoned-sink refusals, lifecycle/plan/revision
 * conflicts) pass through unchanged by identity; no rollback, no
 * automatic retry, no second dispatch. The composition's own failure
 * reasons are exactly `invalid_options` and `invalid_result`;
 * unexpected errors propagate with their class and identity.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ReplannedStageControllerError`,
 * `openPipelineV2ReplannedStageWithOps` and the frozen
 * `productionReplannedStageOps`; the public module exports exactly
 * `PipelineV2ReplannedStageControllerError` and
 * `openPipelineV2ReplannedStage` (types are not runtime keys).
 *
 * The full revise-cycle order this controller completes:
 * accepted revised task → replanned iteration closure → revise_task
 * response → settled planning execution → accepted next plan revision
 * → old generation closed by replanned → selected new-plan generation
 * opened → iteration 1 opened → future transition commit.
 *
 * Not implemented (stays unwired): the architect output parsing, plan
 * candidate construction and acceptance, the stage/budget selection
 * policy, the graph transition commit, automatic resume,
 * coordinator/runner/CLI/default-pipeline wiring, schema/reducer
 * changes, migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ReplannedStageControllerFailureReason = "invalid_options" | "invalid_result";


/**
 * A failure of the composition layer itself with its stable
 * machine-readable `reason` and the last authoritative durable state
 * (`null` when the composition never reached a verified durable
 * state). Downstream controller failures pass through by identity and
 * never take this shape.
 */
export class PipelineV2ReplannedStageControllerError extends Error {
  readonly reason: PipelineV2ReplannedStageControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReplannedStageControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReplannedStageControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam shared with the two composed controllers;
 * the production `PipelineV2RunStateSink` satisfies it without an
 * adapter. The composition never reads or binds `dispatch` itself.
 */
export interface PipelineV2ReplannedStageControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

/**
 * The single ops seam: exactly the two composed authoritative
 * controllers and nothing else. No reducer, filesystem, store,
 * publisher, serializer, registry, coordinator, runner or CLI
 * capability is reachable through it.
 */
export interface PipelineV2ReplannedStageOps {
  readonly closeGeneration: typeof closePipelineV2ReplannedGeneration;
  readonly ensureStageIteration: typeof ensurePipelineV2StageIteration;
}

export interface OpenPipelineV2ReplannedStageOptions {
  readonly sink: PipelineV2ReplannedStageControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly initialBudget: number;
}

export interface OpenedPipelineV2ReplannedStage {
  readonly wait_index: number;
  readonly previous_generation_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly intent_sha256: string;
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly origin_execution: number;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly initial_budget: number;
  readonly state: PipelineV2RunState;
}

/**
 * The frozen production ops: the two existing authoritative controllers
 * bound by identity; no installer and no mutable module-global seam.
 */
export const productionReplannedStageOps: PipelineV2ReplannedStageOps = deepFreezeValue({
  closeGeneration: closePipelineV2ReplannedGeneration,
  ensureStageIteration: ensurePipelineV2StageIteration,
}) as unknown as PipelineV2ReplannedStageOps;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function controllerError(
  reason: PipelineV2ReplannedStageControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReplannedStageControllerError {
  return new PipelineV2ReplannedStageControllerError(reason, message, state);
}

const REVISE_TASK_ACTION_ID = "revise_task";

interface VerifiedCloseResult {
  readonly state: PipelineV2RunState;
  readonly wait: PipelineV2WaitRecord;
  readonly oldGenerationIndex: number;
}

/**
 * The full targeted verification of the close controller's returned
 * result. Defensive with record/array guards before every field
 * access, so malformed nested results fail as the controller's own
 * `invalid_result`, never a `TypeError`.
 */
function verifyCloseResult(
  resultValue: unknown,
  intentSha256: string,
  compiledPlan: CompiledPipelineV2RunPlan,
  initialBudget: number,
): VerifiedCloseResult {
  if (!isRecord(resultValue)) {
    throw controllerError(
      "invalid_result",
      "the replanned generation closure result is not a record",
      null,
    );
  }
  const result = resultValue as Record<string, unknown>;
  if (
    !isPositiveSafeInteger(result["wait_index"]) ||
    !isPositiveSafeInteger(result["generation_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    !isPositiveSafeInteger(result["task_revision"]) ||
    !isPositiveSafeInteger(result["previous_plan_revision"]) ||
    !isPositiveSafeInteger(result["plan_revision"]) ||
    !isPositiveSafeInteger(result["origin_execution"]) ||
    typeof result["intent_sha256"] !== "string" ||
    typeof result["task_id"] !== "string" ||
    typeof result["task_sha256"] !== "string" ||
    typeof result["previous_plan_sha256"] !== "string" ||
    typeof result["plan_sha256"] !== "string"
  ) {
    throw controllerError(
      "invalid_result",
      "the replanned generation closure result carries malformed result fields",
      null,
    );
  }
  if (
    result["plan_revision"] !== compiledPlan.plan_revision ||
    result["plan_sha256"] !== compiledPlan.plan_sha256 ||
    result["origin_execution"] !== compiledPlan.origin_execution ||
    result["intent_sha256"] !== intentSha256
  ) {
    throw controllerError(
      "invalid_result",
      "the replanned generation closure result does not match the accepted plan revision and revise intent",
      null,
    );
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw controllerError(
      "invalid_result",
      "the replanned generation closure result carries no durable run state",
      null,
    );
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (state.status !== "active" || state.phase !== "running") {
    throw controllerError(
      "invalid_result",
      "the closure result state is not at the active running boundary",
      state,
    );
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw controllerError(
      "invalid_result",
      "the closure result state already carries a terminal, publication or failure projection",
      state,
    );
  }
  if (!Array.isArray(state.waits)) {
    throw controllerError(
      "invalid_result",
      "the closure result state carries no wait journal",
      state,
    );
  }
  let occurrences = 0;
  let waitPosition = -1;
  for (let position = 0; position < state.waits.length; position += 1) {
    const entry = state.waits[position];
    if (!isRecord(entry)) {
      throw controllerError(
        "invalid_result",
        "the closure result state carries a malformed wait record",
        state,
      );
    }
    if (entry["index"] === result["wait_index"]) {
      occurrences += 1;
      waitPosition = position;
    }
  }
  if (occurrences !== 1 || waitPosition !== state.waits.length - 1) {
    throw controllerError(
      "invalid_result",
      "the closure result wait is not the last and only record of its index",
      state,
    );
  }
  const wait = state.waits[waitPosition] as unknown as PipelineV2WaitRecord;
  if (
    !isRecord(wait.intent) ||
    wait.intent["intent_sha256"] !== result["intent_sha256"] ||
    !isRecord(wait.response) ||
    wait.response["action_id"] !== REVISE_TASK_ACTION_ID
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result wait does not carry the accepted revise intent and the revise_task response",
      state,
    );
  }
  if (!Array.isArray(state.task_revisions)) {
    throw controllerError(
      "invalid_result",
      "the closure result state carries no task ledger",
      state,
    );
  }
  let taskBound: Record<string, unknown> | undefined;
  let taskBoundCount = 0;
  for (const entry of state.task_revisions) {
    if (!isRecord(entry)) {
      throw controllerError(
        "invalid_result",
        "the closure result state carries a malformed task ledger record",
        state,
      );
    }
    if (entry["wait_index"] === wait["index"]) {
      taskBoundCount += 1;
      taskBound = entry;
    }
  }
  if (
    taskBoundCount !== 1 ||
    taskBound === undefined ||
    taskBound["task_id"] !== result["task_id"] ||
    taskBound["revision"] !== result["task_revision"] ||
    taskBound["sha256"] !== result["task_sha256"] ||
    taskBound["intent_sha256"] !== result["intent_sha256"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result state does not carry the accepted task revision of the revise wait",
      state,
    );
  }
  if (!Array.isArray(state.plan_revisions) || !isPositiveSafeInteger(result["plan_revision"]) || state.plan_revisions.length !== result["plan_revision"]) {
    throw controllerError(
      "invalid_result",
      "the closure result plan ledger does not carry the accepted plan revision",
      state,
    );
  }
  const lastPlan = state.plan_revisions[state.plan_revisions.length - 1];
  const previousPlan = state.plan_revisions[state.plan_revisions.length - 2];
  if (
    !isRecord(lastPlan) ||
    lastPlan["revision"] !== result["plan_revision"] ||
    lastPlan["sha256"] !== result["plan_sha256"] ||
    lastPlan["origin_execution"] !== result["origin_execution"] ||
    !isRecord(previousPlan) ||
    previousPlan["revision"] !== result["previous_plan_revision"] ||
    previousPlan["sha256"] !== result["previous_plan_sha256"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result plan ledger does not agree with the accepted plan revision and its predecessor",
      state,
    );
  }
  if (!Array.isArray(state.generations)) {
    throw controllerError(
      "invalid_result",
      "the closure result state carries no generation journal",
      state,
    );
  }
  const oldGeneration = state.generations[result["generation_index"] as number - 1];
  if (!isRecord(oldGeneration)) {
    throw controllerError(
      "invalid_result",
      "the closure result old generation is missing from the generation journal",
      state,
    );
  }
  if (
    oldGeneration["index"] !== result["generation_index"] ||
    oldGeneration["plan_sha256"] !== result["previous_plan_sha256"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result old generation does not agree with its recorded index and plan binding",
      state,
    );
  }
  if (!Array.isArray(oldGeneration["iterations"]) || (oldGeneration["iterations"] as unknown[]).length === 0) {
    throw controllerError(
      "invalid_result",
      "the closure result old generation records no iterations",
      state,
    );
  }
  const oldIterations = oldGeneration["iterations"] as unknown[];
  const lastIteration = oldIterations[oldIterations.length - 1];
  if (
    !isRecord(lastIteration) ||
    !isRecord(lastIteration["closed"]) ||
    lastIteration["closed"]["by"] !== "replanned" ||
    lastIteration["closed"]["wait_index"] !== wait["index"] ||
    lastIteration["closed"]["closed_transition_count"] !== wait["transition_count"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result old generation carries no exact replanned iteration closure",
      state,
    );
  }
  if (
    !isRecord(oldGeneration["closed"]) ||
    oldGeneration["closed"]["by"] !== "replanned" ||
    oldGeneration["closed"]["closed_transition_count"] !== wait["transition_count"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result old generation carries no exact replanned generation closure",
      state,
    );
  }
  if (state.generations.length === result["generation_index"]) {
    // C1: the old generation is the last durable generation.
    return { state, wait, oldGenerationIndex: result["generation_index"] as number };
  }
  if (state.generations.length !== result["generation_index"] + 1) {
    throw controllerError(
      "invalid_result",
      "the closure result generation journal does not carry the admissible C1 or C2 shape",
      state,
    );
  }
  // C2: the immediate new-generation retry form follows the closed old
  // generation.
  const newGeneration = state.generations[state.generations.length - 1];
  if (
    !isRecord(newGeneration) ||
    newGeneration["closed"] !== undefined ||
    newGeneration["index"] !== result["generation_index"] + 1 ||
    newGeneration["plan_sha256"] !== compiledPlan.plan_sha256 ||
    !isPositiveSafeInteger(newGeneration["initial_budget"]) ||
    newGeneration["opened_transition_count"] !== wait["transition_count"]
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result new generation is not an open current-plan generation on the wait boundary anchor",
      state,
    );
  }
  if (typeof newGeneration["stage_id"] !== "string") {
    throw controllerError(
      "invalid_result",
      "the closure result new generation carries no stage id",
      state,
    );
  }
  if (!Array.isArray(compiledPlan.stages)) {
    throw controllerError(
      "invalid_result",
      "the compiled plan carries no stages",
      state,
    );
  }
  let matches = 0;
  let matchingStage: CompiledPipelineV2RunPlanStage | undefined;
  for (const stage of compiledPlan.stages) {
    if (stage["id"] === newGeneration["stage_id"]) {
      matches += 1;
      matchingStage = stage as unknown as CompiledPipelineV2RunPlanStage;
    }
  }
  if (matches !== 1 || matchingStage === undefined) {
    throw controllerError(
      "invalid_result",
      "the closure result new generation does not name exactly one compiled plan stage",
      state,
    );
  }
  if (
    newGeneration["stage_position"] !== compiledPlan.stages.indexOf(matchingStage) + 1 ||
    newGeneration["template_id"] !== matchingStage.template
  ) {
    throw controllerError(
      "invalid_result",
      "the closure result new generation does not match the compiled stage position and template",
      state,
    );
  }
  const newIterations = newGeneration["iterations"];
  if (newGeneration["iteration_count"] === 0) {
    if (!Array.isArray(newIterations) || newIterations.length !== 0 || newGeneration["open_iteration"] !== undefined) {
      throw controllerError(
        "invalid_result",
        "the closure result new generation carries unexpected iteration history",
        state,
      );
    }
    void initialBudget;
    return { state, wait, oldGenerationIndex: result["generation_index"] as number };
  }
  if (newGeneration["iteration_count"] === 1) {
    if (!Array.isArray(newIterations) || newIterations.length !== 1) {
      throw controllerError(
        "invalid_result",
        "the closure result new generation carries unexpected iteration history",
        state,
      );
    }
    const iteration = newIterations[0];
    if (
      !isRecord(iteration) ||
      iteration["index"] !== 1 ||
      iteration["opened_transition_count"] !== wait["transition_count"] ||
      iteration["closed"] !== undefined ||
      !isRecord(newGeneration["open_iteration"]) ||
      newGeneration["open_iteration"]["index"] !== 1 ||
      newGeneration["open_iteration"]["opened_transition_count"] !== wait["transition_count"]
    ) {
      throw controllerError(
        "invalid_result",
        "the closure result new generation's first iteration is not open on the wait boundary anchor",
        state,
      );
    }
    void initialBudget;
    return { state, wait, oldGenerationIndex: result["generation_index"] as number };
  }
  throw controllerError(
    "invalid_result",
    "the closure result new generation already opened more than its first iteration",
    state,
  );
}

/**
 * The exact positional equality of the durable regions the ensure step
 * must leave unchanged, compared against the verified close state.
 */
function inputStateEquals(before: PipelineV2RunInputState, afterValue: unknown): boolean {
  return (
    isRecord(afterValue) &&
    afterValue["id"] === before.id &&
    afterValue["type"] === before.type &&
    afterValue["protected"] === before.protected &&
    afterValue["digest"] === before.digest
  );
}

function transitionEquals(before: PipelineV2CommittedTransitionState, afterValue: unknown): boolean {
  return (
    isRecord(afterValue) &&
    afterValue["index"] === before.index &&
    afterValue["from"] === before.from &&
    afterValue["outcome"] === before.outcome &&
    afterValue["to"] === before.to &&
    afterValue["execution_index"] === before.execution_index
  );
}

function grantEquals(before: PipelineV2IterationGrantState, afterValue: unknown): boolean {
  return (
    isRecord(afterValue) &&
    afterValue["index"] === before.index &&
    afterValue["generation_index"] === before.generation_index &&
    afterValue["wait_index"] === before.wait_index &&
    afterValue["intent_sha256"] === before.intent_sha256 &&
    afterValue["additional_iterations"] === before.additional_iterations
  );
}

function agentOutputEquals(before: PipelineV2AgentOutputState, afterValue: unknown): boolean {
  return (
    isRecord(afterValue) &&
    afterValue["id"] === before.id &&
    afterValue["digest"] === before.digest
  );
}

function agentOutputListEquals(before: PipelineV2AgentOutputState[] | undefined, afterValue: unknown): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  if (!Array.isArray(afterValue) || afterValue.length !== before.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    const beforeOutput = before[position];
    if (beforeOutput === undefined || !agentOutputEquals(beforeOutput, afterValue[position])) {
      return false;
    }
  }
  return true;
}

function sessionCleanupEquals(before: PipelineV2SessionCleanupPair | undefined, afterValue: unknown): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  return (
    isRecord(afterValue) &&
    afterValue["execution"] === before.execution &&
    afterValue["tool"] === before.tool
  );
}

function decisionResultEquals(before: PipelineDecisionStateRecord | undefined, afterValue: unknown): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  if (!isRecord(afterValue) || afterValue["status"] !== before.status) {
    return false;
  }
  if (before.status === "selected") {
    return (
      afterValue["outcome"] === before.outcome &&
      afterValue["decision"] === before.decision &&
      afterValue["rule_id"] === before.rule_id &&
      stringListEquals(before.active_constraint_ids, afterValue["active_constraint_ids"])
    );
  }
  if (before.status === "uncovered") {
    return (
      afterValue["outcome"] === before.outcome &&
      stringListEquals(before.active_constraint_ids, afterValue["active_constraint_ids"])
    );
  }
  if (before.status === "inconsistent_facts") {
    return (
      afterValue["outcome"] === before.outcome &&
      stringListEquals(before.violated_relation_ids, afterValue["violated_relation_ids"])
    );
  }
  return (
    afterValue["outcome"] === before.outcome &&
    afterValue["reason"] === before.reason &&
    afterValue["fact_id"] === before.fact_id &&
    afterValue["actual_type"] === before.actual_type
  );
}

function stringListEquals(before: readonly string[], after: unknown): boolean {
  if (!Array.isArray(after) || after.length !== before.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (before[position] !== after[position]) {
      return false;
    }
  }
  return true;
}

function executionRecordEquals(before: PipelineV2ExecutionState, afterValue: unknown): boolean {
  if (!isRecord(afterValue)) {
    return false;
  }
  if (
    afterValue["index"] !== before.index ||
    afterValue["type"] !== before.type ||
    afterValue["state_id"] !== before.state_id ||
    afterValue["execution_role"] !== before.execution_role ||
    afterValue["phase"] !== before.phase ||
    afterValue["iteration_index"] !== before.iteration_index
  ) {
    return false;
  }
  if (before.type === "agent") {
    return (
      afterValue["attempt"] === before.attempt &&
      afterValue["profile"] === before.profile &&
      afterValue["execution_session_id"] === before.execution_session_id &&
      afterValue["tool_session_id"] === before.tool_session_id &&
      sessionCleanupEquals(before.session_cleanup, afterValue["session_cleanup"]) &&
      agentOutputListEquals(before.outputs, afterValue["outputs"]) &&
      afterValue["failure_reason"] === before.failure_reason
    );
  }
  return (
    afterValue["input_digest"] === before.input_digest &&
    decisionResultEquals(before.result, afterValue["result"]) &&
    afterValue["failure_reason"] === before.failure_reason
  );
}

function waitRecordEquals(before: PipelineV2WaitRecord, afterValue: unknown): boolean {
  if (!isRecord(afterValue)) {
    return false;
  }
  const after = afterValue as unknown as PipelineV2WaitRecord;
  if (
    after.index !== before.index ||
    after.transition_count !== before.transition_count ||
    after.state_id !== before.state_id ||
    after.reason !== before.reason ||
    after.request_sha256 !== before.request_sha256
  ) {
    return false;
  }
  if (!Array.isArray(after.actions) || after.actions.length !== before.actions.length) {
    return false;
  }
  for (let position = 0; position < before.actions.length; position += 1) {
    const beforeAction = before.actions[position];
    const afterAction = after.actions[position];
    if (
      beforeAction === undefined ||
      !isRecord(afterAction) ||
      afterAction["id"] !== beforeAction.id ||
      afterAction["to"] !== beforeAction.to
    ) {
      return false;
    }
  }
  if (before.intent === undefined) {
    if (after.intent !== undefined) {
      return false;
    }
  } else if (!isRecord(after.intent) || after.intent["intent_sha256"] !== before.intent.intent_sha256) {
    return false;
  }
  if (before.response === undefined) {
    return after.response === undefined;
  }
  return (
    isRecord(after.response) &&
    after.response["action_id"] === before.response.action_id &&
    after.response["response_sha256"] === before.response.response_sha256
  );
}

function taskLedgerEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.task_revisions) || after.task_revisions.length !== before.task_revisions.length) {
    return false;
  }
  return before.task_revisions.every((beforeEntry, position) => {
    const afterEntry = after.task_revisions[position];
    return (
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
  if (!Array.isArray(after.plan_revisions) || after.plan_revisions.length !== before.plan_revisions.length) {
    return false;
  }
  return before.plan_revisions.every((beforeEntry, position) => {
    const afterEntry = after.plan_revisions[position];
    return (
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["origin_execution"] === beforeEntry.origin_execution
    );
  });
}

function terminalEquals(
  before: { readonly state_id: string; readonly result: string } | undefined,
  afterValue: unknown,
): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  return (
    isRecord(afterValue) &&
    afterValue["state_id"] === before.state_id &&
    afterValue["result"] === before.result
  );
}

function runOutputsEquals(before: PipelineV2RunOutputState[] | undefined, afterValue: unknown): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  if (!Array.isArray(afterValue) || afterValue.length !== before.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    const beforeOutput = before[position];
    if (beforeOutput === undefined || !isRecord(afterValue[position])) {
      return false;
    }
    const afterOutput = afterValue[position] as Record<string, unknown>;
    if (
      afterOutput["id"] !== beforeOutput.id ||
      afterOutput["type"] !== beforeOutput.type ||
      afterOutput["required"] !== beforeOutput.required ||
      afterOutput["present"] !== beforeOutput.present ||
      (beforeOutput.present ? afterOutput["digest"] !== beforeOutput.digest : false)
    ) {
      return false;
    }
  }
  return true;
}

function failureEquals(before: { readonly reason: string } | undefined, afterValue: unknown): boolean {
  if (before === undefined) {
    return afterValue === undefined;
  }
  return isRecord(afterValue) && afterValue["reason"] === before.reason;
}

function iterationProjectionEquals(
  before: { readonly index: number; readonly opened_transition_count: number } | undefined,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(after) &&
    after["index"] === before.index &&
    after["opened_transition_count"] === before.opened_transition_count
  );
}

function closureProjectionEquals(
  before: { readonly by: string; readonly closed_transition_count: number } | undefined,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(after) &&
    after["by"] === before.by &&
    after["closed_transition_count"] === before.closed_transition_count
  );
}

function iterationRecordEquals(
  before: { readonly index: number; readonly opened_transition_count: number; readonly closed?: { readonly by: string; readonly wait_index?: number; readonly closed_transition_count: number } },
  after: unknown,
): boolean {
  if (!isRecord(after) || after["index"] !== before.index || after["opened_transition_count"] !== before.opened_transition_count) {
    return false;
  }
  const beforeClosed = before.closed;
  const afterClosed = after["closed"];
  if (beforeClosed === undefined) {
    return afterClosed === undefined;
  }
  return (
    isRecord(afterClosed) &&
    afterClosed["by"] === beforeClosed.by &&
    afterClosed["wait_index"] === beforeClosed.wait_index &&
    afterClosed["closed_transition_count"] === beforeClosed.closed_transition_count
  );
}

/**
 * The exact positional equality of one durable generation record as the
 * ensure step must leave it.
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
    !iterationProjectionEquals(before["open_iteration"] as { readonly index: number; readonly opened_transition_count: number } | undefined, after["open_iteration"]) ||
    !closureProjectionEquals(before["closed"] as { readonly by: string; readonly closed_transition_count: number } | undefined, after["closed"]) ||
    !Array.isArray(after["iterations"]) ||
    !Array.isArray(before["iterations"]) ||
    after["iterations"].length !== before["iterations"].length
  ) {
    return false;
  }
  for (let position = 0; position < before["iterations"].length; position += 1) {
    if (!iterationRecordEquals(before["iterations"][position], after["iterations"][position])) {
      return false;
    }
  }
  return true;
}

/**
 * The full targeted verification of the ensure controller's returned
 * result against the trusted compiled stage, the verified close result
 * and the verified close state. Defensive with record/array guards
 * before every field access, so malformed nested results fail as the
 * controller's own `invalid_result`, never a `TypeError`.
 */
function verifyEnsureResult(
  resultValue: unknown,
  close: VerifiedCloseResult,
  compiledPlan: CompiledPipelineV2RunPlan,
  compiledStage: CompiledPipelineV2RunPlanStage,
  stageId: string,
  initialBudget: number,
  stagePosition: number,
): void {
  if (!isRecord(resultValue)) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result is not a record",
      close.state,
    );
  }
  const result = resultValue as Record<string, unknown>;
  if (
    result["compiled_stage"] !== compiledStage ||
    !isPositiveSafeInteger(result["generation_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"])
  ) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result does not carry the exact trusted compiled stage",
      close.state,
    );
  }
  if (result["generation_index"] !== close.oldGenerationIndex + 1 || result["iteration_index"] !== 1) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result does not continue the replanned generation and open its first iteration",
      close.state,
    );
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result carries no durable run state",
      close.state,
    );
  }
  const after = stateValue as unknown as PipelineV2RunState;
  const before = close.state;
  const mismatch = (): PipelineV2ReplannedStageControllerError =>
    controllerError(
      "invalid_result",
      "the stage iteration result state does not carry the exact planned stage opening over the verified closure boundary",
      isRecord(stateValue) ? (stateValue as unknown as PipelineV2RunState) : close.state,
    );
  if (after.status !== "active" || after.phase !== "running") {
    throw controllerError(
      "invalid_result",
      "the stage iteration result state is not at the active running boundary",
      after,
    );
  }
  if (
    after.schema_version !== before.schema_version ||
    after.run_id !== before.run_id ||
    after.started_at !== before.started_at
  ) {
    throw mismatch();
  }
  if (
    !isRecord(after.cursor) ||
    !isRecord(before.cursor) ||
    after.cursor["current_state"] !== before.cursor["current_state"] ||
    after.cursor["transition_count"] !== before.cursor["transition_count"]
  ) {
    throw mismatch();
  }
  if (!Array.isArray(after.inputs) || after.inputs.length !== before.inputs.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.inputs.length; position += 1) {
    const beforeInput = before.inputs[position];
    if (beforeInput === undefined || !inputStateEquals(beforeInput, after.inputs[position])) {
      throw mismatch();
    }
  }
  if (!Array.isArray(after.transitions) || after.transitions.length !== before.transitions.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.transitions.length; position += 1) {
    const beforeTransition = before.transitions[position];
    if (beforeTransition === undefined || !transitionEquals(beforeTransition, after.transitions[position])) {
      throw mismatch();
    }
  }
  if (!Array.isArray(after.executions) || after.executions.length !== before.executions.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.executions.length; position += 1) {
    const beforeExecution = before.executions[position];
    if (beforeExecution === undefined || !executionRecordEquals(beforeExecution, after.executions[position])) {
      throw mismatch();
    }
  }
  if (!Array.isArray(after.executions) || after.executions.length !== after.transitions.length + 1) {
    throw mismatch();
  }
  const lastExecution = after.executions[after.executions.length - 1];
  if (
    !isRecord(lastExecution) ||
    lastExecution["type"] !== "agent" ||
    lastExecution["execution_role"] !== "planning" ||
    lastExecution["phase"] !== "cleanup_completed"
  ) {
    throw controllerError(
      "invalid_result",
      "the stage iteration result state does not rest on the settled-but-unbound planning execution",
      after,
    );
  }
  if (!Array.isArray(after.waits) || after.waits.length !== before.waits.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.waits.length; position += 1) {
    const beforeWait = before.waits[position];
    if (beforeWait === undefined || !waitRecordEquals(beforeWait, after.waits[position])) {
      throw mismatch();
    }
  }
  if (!taskLedgerEquals(before, after) || !planLedgerEquals(before, after)) {
    throw mismatch();
  }
  if (!Array.isArray(after.grants) || after.grants.length !== before.grants.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.grants.length; position += 1) {
    const beforeGrant = before.grants[position];
    if (beforeGrant === undefined || !grantEquals(beforeGrant, after.grants[position])) {
      throw mismatch();
    }
  }
  if (!terminalEquals(before.terminal, after.terminal) || !runOutputsEquals(before.run_outputs, after.run_outputs) || !failureEquals(before.failure, after.failure)) {
    throw mismatch();
  }
  // The ensure step either opened exactly one new generation on top of
  // the verified close state (the C0/C1 flows) or recognized the already
  // open current-plan generation of the C2 retry form (zero dispatch);
  // both shapes end with the new generation last and everything before
  // it unchanged.
  if (
    !Array.isArray(after.generations) ||
    (after.generations.length !== before.generations.length &&
      after.generations.length !== before.generations.length + 1)
  ) {
    throw mismatch();
  }
  const unchangedPrefixLength = after.generations.length === before.generations.length
    ? before.generations.length - 1
    : before.generations.length;
  for (let position = 0; position < unchangedPrefixLength; position += 1) {
    if (!generationUnchanged(before.generations[position], after.generations[position])) {
      throw mismatch();
    }
  }
  // The last generation is verified by the exact new-generation check
  // below in both shapes: in the opened shape it is new, and in the
  // zero-dispatch retry shape the ensure step may have appended its
  // first iteration to the already-open generation.
  const newGeneration = after.generations[after.generations.length - 1];
  if (!isRecord(newGeneration)) {
    throw mismatch();
  }
  const wait = close.wait;
  if (
    newGeneration["index"] !== result["generation_index"] ||
    newGeneration["stage_id"] !== stageId ||
    newGeneration["stage_position"] !== stagePosition ||
    newGeneration["template_id"] !== compiledStage.template ||
    newGeneration["plan_sha256"] !== compiledPlan.plan_sha256 ||
    newGeneration["initial_budget"] !== initialBudget ||
    newGeneration["opened_transition_count"] !== wait["transition_count"] ||
    newGeneration["closed"] !== undefined ||
    newGeneration["iteration_count"] !== 1 ||
    !Array.isArray(newGeneration["iterations"]) ||
    newGeneration["iterations"].length !== 1
  ) {
    throw mismatch();
  }
  const iteration = newGeneration["iterations"][0];
  if (
    !isRecord(iteration) ||
    iteration["index"] !== 1 ||
    iteration["opened_transition_count"] !== wait["transition_count"] ||
    iteration["closed"] !== undefined ||
    !isRecord(newGeneration["open_iteration"]) ||
    newGeneration["open_iteration"]["index"] !== 1 ||
    newGeneration["open_iteration"]["opened_transition_count"] !== wait["transition_count"]
  ) {
    throw mismatch();
  }
}

export async function openPipelineV2ReplannedStageWithOps(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<OpenedPipelineV2ReplannedStage> {
  // Capture boundary: the options shape, then every options field and
  // both ops getters each read exactly once as opaque references before
  // the first await; later caller mutations cannot influence the run.
  if (!isRecord(optionsValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires an options object",
      null,
    );
  }
  const sinkValue = optionsValue["sink"];
  const intentValue = optionsValue["intent"];
  const compiledPlanValue = optionsValue["compiledPlan"];
  const stageIdValue = optionsValue["stageId"];
  const initialBudgetValue = optionsValue["initialBudget"];
  if (!isRecord(sinkValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires a state sink",
      null,
    );
  }
  if (!isRecord(intentValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires a prepared wait intent object",
      null,
    );
  }
  if (!isRecord(compiledPlanValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires a compiled run plan object",
      null,
    );
  }
  if (typeof stageIdValue !== "string") {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires the stage id selected by the caller policy",
      null,
    );
  }
  if (!isPositiveSafeInteger(initialBudgetValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires a positive safe integer initial budget",
      null,
    );
  }
  if (!isRecord(opsValue)) {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires an ops object",
      null,
    );
  }
  const closeGeneration = opsValue["closeGeneration"];
  const ensureStageIteration = opsValue["ensureStageIteration"];
  if (typeof closeGeneration !== "function" || typeof ensureStageIteration !== "function") {
    throw controllerError(
      "invalid_options",
      "openPipelineV2ReplannedStage requires the two composed controller functions",
      null,
    );
  }

  // Pre-side-effect validation: the stage id resolves through the single
  // existing trusted compiled resolver (its provenance gate, stage
  // existence and template binding belong to it alone); its errors pass
  // by identity. The stage position is derived only from the trusted
  // compiled plan's declaration order.
  const compiledPlan = compiledPlanValue as unknown as CompiledPipelineV2RunPlan;
  const compiledStage = compiledPipelineV2RunPlanStageFor(compiledPlan, stageIdValue);
  if (!Array.isArray(compiledPlan.stages)) {
    throw new Error("pipeline v2 replanned stage controller invariant violated: the compiled plan carries no stages");
  }
  const declarationIndex = compiledPlan.stages.findIndex((stage) => stage["id"] === compiledStage["id"]);
  if (declarationIndex < 0) {
    throw new Error("pipeline v2 replanned stage controller invariant violated: the compiled stage is not in the compiled plan");
  }
  const stagePosition = declarationIndex + 1;

  // Composition: close the old generation first and verify the returned
  // result COMPLETELY before the ensure call; a hostile, malformed or
  // binding-mismatched close result never reaches the ensure call.
  const intent = intentValue as unknown as PreparedPipelineV2RunWaitIntent;
  const closeResult = await closeGeneration({
    sink: sinkValue as unknown as PipelineV2ReplannedStageControllerSink,
    intent,
    compiledPlan,
  });
  const intentSha256 = (intentValue as Record<string, unknown>)["sha256"];
  if (typeof intentSha256 !== "string") {
    throw controllerError(
      "invalid_result",
      "the prepared wait intent carries no digest",
      null,
    );
  }
  const close = verifyCloseResult(closeResult, intentSha256, compiledPlan, initialBudgetValue);

  // The ensure step runs only after the verified close result; its own
  // verification compares the returned state against the verified close
  // state and the trusted compiled stage. The result is built from the
  // ensure controller's authoritative state without any additional
  // snapshot read.
  const ensureResult = await ensureStageIteration({
    sink: sinkValue as unknown as PipelineV2ReplannedStageControllerSink,
    compiledPlan,
    stageId: stageIdValue,
    initialBudget: initialBudgetValue,
  });
  verifyEnsureResult(ensureResult, close, compiledPlan, compiledStage, stageIdValue, initialBudgetValue, stagePosition);

  const verifiedResult = ensureResult as EnsuredPipelineV2StageIteration;
  const verifiedClose = closeResult as ClosedPipelineV2ReplannedGeneration;
  return deepFreezeValue({
    wait_index: verifiedClose.wait_index,
    previous_generation_index: close.oldGenerationIndex,
    generation_index: verifiedResult.generation_index,
    iteration_index: verifiedResult.iteration_index,
    intent_sha256: intentSha256,
    plan_revision: compiledPlan.plan_revision,
    plan_sha256: compiledPlan.plan_sha256,
    origin_execution: compiledPlan.origin_execution,
    stage_id: stageIdValue,
    stage_position: stagePosition,
    template_id: compiledStage.template,
    initial_budget: initialBudgetValue,
    state: verifiedResult.state,
  }) as OpenedPipelineV2ReplannedStage;
}
