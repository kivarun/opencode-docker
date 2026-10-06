/**
 * Production-neutral composition of the planning-run-plan handoff followed
 * by the coordinator's resume entrypoint (unwired).
 *
 * This controller is the single layer that binds the two existing
 * authoritative facades into one fixed sequence:
 *
 * 1. `applyPipelineV2PlanningRunPlanHandoff` — the restart-aware
 *    planning-run-plan handoff (the planning-output composition, the plan
 *    acceptance, the replanned stage opening and the committed planning
 *    transition; its own Branch A/B windows, idempotent retries and full
 *    verification stay authoritative);
 * 2. the defensive verification of the successful handoff — the flat
 *    result carries exactly the typed contract fields, `result.state` is
 *    structurally equal (a local recursive own-key comparator, never a
 *    serialization, identity not required) to the post-handoff
 *    authoritative sink snapshot read exactly once, and the boundary
 *    carries the targeted durable bindings: the last committed transition
 *    belongs to the last settled planning execution and leads to the
 *    selected stage's compiled template entry state, the new generation is
 *    the last open one bound to the caller policy and the last accepted
 *    plan record, the last wait is the answered `revise_task` wait of the
 *    actual last replanned closure, and no successor execution has started
 *    (`executions.length === transitions.length`). Historical waits and
 *    generations never participate: the binding is only to the actual last
 *    replanned closure and the last handoff transition;
 * 3. `resumePipelineV2Run` — the coordinator's production-neutral resume
 *    entrypoint, called with the captured pipeline, the run id derived
 *    exclusively from the verified authoritative state, the captured run
 *    root and sink, and stable runtime/control adapters built from the
 *    contract functions captured exactly once in the preflight (so a
 *    mutation of the caller-owned runtime or control objects during the
 *    pending handoff cannot change the resume). No proposal, compiled
 *    plan/stage, wait intent, plan digest/revision, generation/iteration/
 *    transition index or entry state is accepted from the caller.
 *
 * The controller owns no durable side effect of its own: it dispatches
 * nothing, publishes nothing, loads nothing, never opens the sink and
 * never loads the pipeline (the caller opens the sink and loads the
 * pipeline exclusively from the durable `state.pipeline.bundle_root`;
 * after a process crash the caller opens a fresh sink and repeats the
 * whole facade — no reopen happens inside this layer), never calls the
 * reducer, validator, store, manifest loader, serializer or digest
 * machinery, and never starts a worker. The handoff's typed errors pass
 * through by object identity and the resume is never started after one;
 * the coordinator's thrown errors pass through by object identity as well
 * and are never caught for a second handoff inside the same call. The
 * signal lifecycle (acceptance and cutoff) stays owned by the coordinator
 * through the captured control functions; this layer interprets no signal
 * itself.
 *
 * The authoritative snapshot is read exactly once per verification phase:
 * once immediately after the handoff (the handoff-result verification's
 * binding anchor and its failure-reporting state) and once immediately
 * after the resume (the coordinator-union verification's identity anchor).
 * A malformed post-call snapshot (a non-record value) is this layer's own
 * `invalid_result` and never enters the error state: after the handoff the
 * error state is `null` (no verified authoritative snapshot exists), after
 * the resume it is the verified post-handoff snapshot. A hostile or
 * malformed successful handoff result is this layer's own `invalid_result`
 * with the post-handoff authoritative snapshot — never a leaked
 * `TypeError`, never healed downstream; when the post-handoff snapshot
 * itself is not a valid completed handoff boundary (for example a state
 * with an already started successor execution), it cannot serve as the
 * error state and the error carries `null`.
 *
 * Capture and preflight (all before the first await and before any durable
 * intervention): the options shape; then the seven option fields
 * (`pipeline`, `runRoot`, `sink`, `runtime`, `control`, `stageId`,
 * `initialBudget`) each read exactly once in this contract order; then the
 * ops record shape and its two members (`applyHandoff`, `resumeRun`) each
 * read exactly once with function checks; then the pipeline provenance
 * gate (clones, casts, spreads and Proxies are rejected with zero getter
 * or trap invocations, before any scalar validation, sink read or facade
 * call; its own typed error propagates unchanged); then the
 * scalar/structural validations — a non-empty absolute `runRoot`, a
 * structural sink (a record with a `dispatch` function; the
 * `snapshot`/`poisoned` getters stay owned by the composed layers and the
 * coordinator), a safe `stageId` and a positive safe integer
 * `initialBudget`; then the runtime and control contract functions
 * `createExecutionSession`/`createToolSession` and
 * `currentSignal`/`freezeSignal` are read and bound exactly once into
 * stable deep-frozen adapters. Hostile extra option fields are never read;
 * caller-owned objects are never frozen or modified.
 *
 * The successfully returned coordinator result is verified defensively —
 * only the exact runtime shapes of the coordinator's
 * `PipelineV2ResumeCoordinationResult` are accepted (success: exactly the
 * own keys `ok`,`state` with a state identical to the authoritative
 * snapshot; refusal: exactly `ok`,`refused`,`reason`,`state` with
 * `refused === true` and a reason from the coordinator's refusal
 * vocabulary; ordinary failure: exactly `ok`,`reason`,`state` with a
 * reason from the canonical `PIPELINE_V2_FAILURE_REASONS`; failure and
 * refusal states null or exactly the authoritative snapshot) — and
 * returned unchanged by object identity: refusal, worker failure, signal
 * failure and persistence failure stay coordinator-owned classifications,
 * and diagnostics never echo a hostile reason, key or value.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2PlanningRunPlanResumeControllerError`,
 * `applyPipelineV2PlanningRunPlanResumeWithIo` and the frozen
 * `productionPlanningRunPlanResumeOps`; the public module exports exactly
 * the error and `resumePipelineV2RunAfterPlanningRunPlanHandoff` (types
 * are not runtime keys).
 *
 * Not implemented (stays unwired): the stage/budget selection policy, the
 * runner, the CLI, the default pipeline bundle, automatic resume,
 * schema/reducer changes, migrations/API/T3 and multi-process locking.
 */
import { isAbsolute } from "node:path";
import { applyPipelineV2PlanningRunPlanHandoff } from "./pipeline_v2_planning_run_plan_handoff_controller.ts";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
} from "./pipeline_v2_coordinator.ts";
import { compiledStageTemplateFor } from "./pipeline_v2_orchestration.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import { PIPELINE_V2_FAILURE_REASONS } from "./pipeline_v2_state.ts";

/**
 * The coordinator's refusal reason vocabulary, pinned to the coordinator's
 * own `PipelineV2ResumeRefusalReason` type: the compile-time assertions
 * below fail the build if the list is invalid or incomplete. There is no
 * runtime export of the vocabulary from the coordinator, so this typed
 * literal list is the vocabulary itself, never a weaker local subset of
 * some other list.
 */
const PIPELINE_V2_RESUME_REFUSAL_REASONS = [
  "missing_state",
  "sink_poisoned",
  "run_id_mismatch",
  "invalid_state",
  "pipeline_mismatch",
  "run_layout_invalid",
  "run_input_modified",
  "accepted_output_modified",
  "internal_error",
] as const;

type ListedRefusalReason = (typeof PIPELINE_V2_RESUME_REFUSAL_REASONS)[number];
type ListedRefusalReasonsValid =
  ListedRefusalReason extends PipelineV2ResumeRefusalReason ? true : never;
const LISTED_REFUSAL_REASONS_VALID: ListedRefusalReasonsValid = true;
void LISTED_REFUSAL_REASONS_VALID;
type AllRefusalReasonsListed =
  Exclude<PipelineV2ResumeRefusalReason, ListedRefusalReason> extends never ? true : never;
const ALL_REFUSAL_REASONS_LISTED: AllRefusalReasonsListed = true;
void ALL_REFUSAL_REASONS_LISTED;

const PIPELINE_V2_RESUME_REFUSAL_REASON_SET: ReadonlySet<string> = new Set(
  PIPELINE_V2_RESUME_REFUSAL_REASONS,
);

const PIPELINE_V2_RESUME_FAILURE_REASON_SET: ReadonlySet<string> = new Set(
  PIPELINE_V2_FAILURE_REASONS,
);

export type PipelineV2PlanningRunPlanResumeControllerFailureReason =
  | "invalid_options"
  | "invalid_result";

/**
 * A failure of the handoff composition layer itself with its stable
 * machine-readable `reason` and the last verified authoritative durable
 * state (`null` when none was established). The composed facades' typed
 * errors pass through by identity and never take this shape.
 */
export class PipelineV2PlanningRunPlanResumeControllerError extends Error {
  readonly reason: PipelineV2PlanningRunPlanResumeControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2PlanningRunPlanResumeControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2PlanningRunPlanResumeControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The single ops seam: exactly the two existing authoritative facades and
 * nothing else. No reducer, filesystem, store, publisher, serializer,
 * digest builder, registry or CLI/runner capability is reachable through
 * it.
 */
export interface PipelineV2PlanningRunPlanResumeOps {
  readonly applyHandoff: typeof applyPipelineV2PlanningRunPlanHandoff;
  readonly resumeRun: typeof resumePipelineV2Run;
}

/**
 * The frozen production ops: the two existing facades bound by identity;
 * no installer and no mutable module-global seam.
 */
export const productionPlanningRunPlanResumeOps: PipelineV2PlanningRunPlanResumeOps = deepFreezeValue({
  applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
  resumeRun: resumePipelineV2Run,
}) as unknown as PipelineV2PlanningRunPlanResumeOps;

export interface ApplyPipelineV2PlanningRunPlanResumeOptions {
  /** The exact deep-frozen snapshot a successful `loadPipelineV2` returned. */
  readonly pipeline: ResolvedPipelineV2;
  /** Canonical orchestrator-owned run root of the same run as the sink. */
  readonly runRoot: string;
  /**
   * The opened existing state sink the handoff and the resume share; the
   * production `PipelineV2RunStateSink` satisfies it structurally.
   */
  readonly sink: PipelineV2CoordinatorStateSink;
  /** The agent runtime the coordinator consumes for the resumed execution. */
  readonly runtime: PipelineV2AgentRuntime;
  /** The signal control boundary; acceptance/cutoff stay coordinator-owned. */
  readonly control: PipelineV2CoordinatorControl;
  /** The caller-selected stage of the newly accepted plan. */
  readonly stageId: string;
  /** The caller-owned stage iteration budget; no policy is applied here. */
  readonly initialBudget: number;
}

const REVISE_TASK_ACTION_ID = "revise_task";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

function controllerError(
  reason: PipelineV2PlanningRunPlanResumeControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanningRunPlanResumeControllerError {
  return new PipelineV2PlanningRunPlanResumeControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2PlanningRunPlanResumeControllerError {
  return controllerError("invalid_options", message, null);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanningRunPlanResumeControllerError {
  return controllerError("invalid_result", message, state);
}

/**
 * The captured caller policy: the two scalars the handoff facade is called
 * with and its result is verified against. The run id the resume consumes
 * is derived exclusively from the verified authoritative state — no
 * proposal, compiled plan/stage, wait intent, plan digest/revision,
 * generation/iteration/transition index or entry state is a caller field.
 */
interface PlanningRunPlanResumePolicy {
  readonly stageId: string;
  readonly initialBudget: number;
}

/**
 * Captures one contract function of an injected object exactly once,
 * before any side effect: the function is read one time, checked against
 * the contract, and bound to its owner. Reassigning the property later
 * cannot change the dispatch, and the user object is never frozen or
 * modified. Diagnostics carry no values.
 */
function captureContractFunction(
  owner: Record<string, unknown>,
  name: "createExecutionSession" | "createToolSession" | "currentSignal" | "freezeSignal",
  ownerLabel: string,
): unknown {
  let value: unknown;
  try {
    value = owner[name];
  } catch {
    throw invalidOptions(`${ownerLabel} contract violated: ${name} must be a function`);
  }
  if (typeof value !== "function") {
    throw invalidOptions(`${ownerLabel} contract violated: ${name} must be a function`);
  }
  return (value as (...args: never[]) => unknown).bind(owner);
}

function hasExactOwnKeys(record: Record<string, unknown>, ...expected: string[]): boolean {
  const keys = Object.keys(record);
  return keys.length === expected.length && expected.every((key) => keys.includes(key));
}

/**
 * Strict structural equality over plain JSON state documents: own
 * enumerable keys, element-wise lists, scalars by `===`. No serialization,
 * no key-order sensitivity, no undefined-key tolerance.
 */
function statesStructurallyEqual(left: unknown, right: unknown): boolean {
  if (left === right) {
    return true;
  }
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    if (Array.isArray(left) || Array.isArray(right)) {
      return false;
    }
    return left === right;
  }
  if (Array.isArray(left) !== Array.isArray(right)) {
    return false;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) {
      return false;
    }
    for (let index = 0; index < left.length; index += 1) {
      if (!statesStructurallyEqual(left[index], right[index])) {
        return false;
      }
    }
    return true;
  }
  const leftKeys = Object.keys(left as object).sort();
  const rightKeys = Object.keys(right as object).sort();
  if (leftKeys.length !== rightKeys.length) {
    return false;
  }
  for (let index = 0; index < leftKeys.length; index += 1) {
    if (leftKeys[index] !== rightKeys[index]) {
      return false;
    }
  }
  for (const key of leftKeys) {
    if (
      !statesStructurallyEqual(
        (left as Record<string, unknown>)[key],
        (right as Record<string, unknown>)[key],
      )
    ) {
      return false;
    }
  }
  return true;
}

/**
 * The durable anchors of the completed handoff boundary, derived only from
 * the authoritative snapshot: the cursor, the last committed transition,
 * the last settled execution, the last accepted plan record, the new open
 * generation with its open successor iteration, the actual last replanned
 * iteration closure and the answered `revise_task` wait it is bound to.
 * Historical waits and generations never participate.
 */
interface HandoffBoundaryAnchors {
  readonly cursorState: unknown;
  readonly transitionCount: number;
  readonly lastTransition: Record<string, unknown>;
  readonly lastExecution: Record<string, unknown>;
  readonly lastPlan: Record<string, unknown>;
  readonly lastGeneration: Record<string, unknown>;
  readonly openIteration: Record<string, unknown>;
  readonly closureGeneration: Record<string, unknown>;
  readonly closureIteration: Record<string, unknown>;
  readonly closure: Record<string, unknown>;
  readonly lastWait: Record<string, unknown>;
}

/**
 * The targeted completed-handoff-boundary check of the authoritative
 * snapshot itself, before any result field is bound to it: the active
 * running run with no terminal/publication/failure projection, the cursor
 * and both journals exactly at the committed planning transition with no
 * successor execution started, the last settled planning execution, the
 * last open generation ending at its open successor iteration, the actual
 * last replanned iteration closure (never a historical one) anchoring to
 * the answered last `revise_task` wait, and the generation closure of the
 * predecessor. Every failure is a typed `invalid_result` whose error state
 * is `null`: the snapshot itself is unusable, so it can never serve as the
 * authoritative error state.
 */
function verifyHandoffStateBoundary(state: PipelineV2RunState): HandoffBoundaryAnchors {
  if (!isNonEmptyString(state.run_id)) {
    throw invalidResult("the post-handoff durable state carries no run identity", null);
  }
  if (state.status !== "active" || state.phase !== "running") {
    throw invalidResult("the handoff boundary is not the active running run", null);
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw invalidResult("the handoff boundary carries a terminal, publication or failure projection", null);
  }
  const cursor = state.cursor as unknown;
  if (
    !isRecord(cursor) ||
    !isNonEmptyString(cursor["current_state"]) ||
    !isNonNegativeSafeInteger(cursor["transition_count"])
  ) {
    throw invalidResult("the handoff boundary cursor is malformed", null);
  }
  const transitionCount = cursor["transition_count"] as number;
  const transitions = state.transitions as unknown;
  const executions = state.executions as unknown;
  if (!Array.isArray(transitions) || !Array.isArray(executions)) {
    throw invalidResult("the handoff boundary journals are malformed", null);
  }
  if (transitions.length !== transitionCount || executions.length !== transitionCount) {
    throw invalidResult("the handoff boundary journals do not sit at the committed planning transition", null);
  }
  for (const entry of transitions) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed transition record", null);
    }
  }
  for (const entry of executions) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed execution record", null);
    }
  }
  const lastTransition = transitions[transitions.length - 1] as Record<string, unknown>;
  if (
    !isNonNegativeSafeInteger(lastTransition["index"]) ||
    !isNonEmptyString(lastTransition["from"]) ||
    !isNonEmptyString(lastTransition["to"]) ||
    lastTransition["outcome"] !== "completed" ||
    !isPositiveSafeInteger(lastTransition["execution_index"])
  ) {
    throw invalidResult("the last committed transition is malformed", null);
  }
  const lastExecution = executions[executions.length - 1] as Record<string, unknown>;
  if (
    lastExecution["index"] !== lastTransition["execution_index"] ||
    lastExecution["type"] !== "agent" ||
    lastExecution["state_id"] !== lastTransition["from"] ||
    lastExecution["phase"] !== "cleanup_completed" ||
    lastExecution["execution_role"] !== "planning" ||
    lastExecution["iteration_index"] !== undefined
  ) {
    throw invalidResult("the last committed transition does not belong to the settled planning execution", null);
  }
  if (cursor["current_state"] !== lastTransition["to"]) {
    throw invalidResult("the handoff boundary cursor is not at the committed transition target", null);
  }
  const planRevisions = state.plan_revisions as unknown;
  if (!Array.isArray(planRevisions) || planRevisions.length === 0) {
    throw invalidResult("the handoff boundary carries no accepted plan revision", null);
  }
  for (const entry of planRevisions) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed plan revision record", null);
    }
  }
  const lastPlan = planRevisions[planRevisions.length - 1] as Record<string, unknown>;
  if (
    !isPositiveSafeInteger(lastPlan["revision"]) ||
    lastPlan["revision"] !== planRevisions.length ||
    !isLowercaseSha256(lastPlan["sha256"])
  ) {
    throw invalidResult("the last accepted plan revision is malformed", null);
  }
  const generations = state.generations as unknown;
  if (!Array.isArray(generations) || generations.length === 0) {
    throw invalidResult("the handoff boundary carries no stage generation", null);
  }
  for (const entry of generations) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed generation record", null);
    }
    const iterations = entry["iterations"] as unknown;
    if (!Array.isArray(iterations)) {
      throw invalidResult("the handoff boundary carries a generation without its iteration history", null);
    }
    for (const iteration of iterations) {
      if (!isRecord(iteration)) {
        throw invalidResult("the handoff boundary carries a malformed iteration record", null);
      }
    }
  }
  const lastGeneration = generations[generations.length - 1] as Record<string, unknown>;
  if (lastGeneration["closed"] !== undefined) {
    throw invalidResult("the handoff boundary's last generation is not open", null);
  }
  if (
    !isPositiveSafeInteger(lastGeneration["index"]) ||
    !isNonEmptyString(lastGeneration["stage_id"]) ||
    !isPositiveSafeInteger(lastGeneration["stage_position"]) ||
    !isNonEmptyString(lastGeneration["template_id"]) ||
    !isLowercaseSha256(lastGeneration["plan_sha256"]) ||
    !isPositiveSafeInteger(lastGeneration["initial_budget"]) ||
    !isNonNegativeSafeInteger(lastGeneration["opened_transition_count"]) ||
    !isPositiveSafeInteger(lastGeneration["iteration_count"])
  ) {
    throw invalidResult("the handoff boundary's last generation is malformed", null);
  }
  const openIteration = lastGeneration["open_iteration"] as unknown;
  if (
    !isRecord(openIteration) ||
    !isPositiveSafeInteger(openIteration["index"]) ||
    !isNonNegativeSafeInteger(openIteration["opened_transition_count"])
  ) {
    throw invalidResult("the handoff boundary's last generation carries no open iteration projection", null);
  }
  const lastGenerationIterations = lastGeneration["iterations"] as Array<unknown>;
  if (
    lastGenerationIterations.length === 0 ||
    lastGenerationIterations.length !== lastGeneration["iteration_count"]
  ) {
    throw invalidResult("the handoff boundary's last generation does not end at its open iteration", null);
  }
  const lastIteration = lastGenerationIterations[lastGenerationIterations.length - 1] as Record<string, unknown>;
  if (
    lastIteration["index"] !== openIteration["index"] ||
    lastIteration["closed"] !== undefined ||
    lastIteration["opened_transition_count"] !== openIteration["opened_transition_count"]
  ) {
    throw invalidResult("the handoff boundary's last generation does not end at its open iteration", null);
  }
  let closureGeneration: Record<string, unknown> | undefined;
  let closureIteration: Record<string, unknown> | undefined;
  let closure: Record<string, unknown> | undefined;
  for (const entryValue of generations as Array<unknown>) {
    const generation = entryValue as Record<string, unknown>;
    for (const iterationValue of generation["iterations"] as Array<unknown>) {
      const iteration = iterationValue as Record<string, unknown>;
      const closed = iteration["closed"] as unknown;
      if (isRecord(closed) && closed["by"] === "replanned") {
        closureGeneration = generation;
        closureIteration = iteration;
        closure = closed;
      }
    }
  }
  if (closureGeneration === undefined || closureIteration === undefined || closure === undefined) {
    throw invalidResult("the handoff boundary carries no replanned iteration closure", null);
  }
  const predecessor = generations[generations.length - 2] as unknown;
  if (!isRecord(predecessor) || predecessor !== closureGeneration) {
    throw invalidResult("the replanned iteration closure does not belong to the predecessor generation", null);
  }
  const predecessorClosed = predecessor["closed"] as unknown;
  if (
    !isRecord(predecessorClosed) ||
    predecessorClosed["by"] !== "replanned" ||
    !isNonNegativeSafeInteger(predecessorClosed["closed_transition_count"])
  ) {
    throw invalidResult("the predecessor generation is not closed by the replanned handoff", null);
  }
  const closureGenerationIterations = closureGeneration["iterations"] as Array<unknown>;
  if (closureGenerationIterations[closureGenerationIterations.length - 1] !== closureIteration) {
    throw invalidResult("the replanned iteration closure does not close the generation's last iteration", null);
  }
  if (
    !isPositiveSafeInteger(closure["wait_index"]) ||
    closure["closed_transition_count"] !== predecessorClosed["closed_transition_count"]
  ) {
    throw invalidResult("the replanned iteration closure is malformed", null);
  }
  const waits = state.waits as unknown;
  if (!Array.isArray(waits) || waits.length === 0) {
    throw invalidResult("the handoff boundary carries no wait record", null);
  }
  for (const entry of waits) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed wait record", null);
    }
  }
  const lastWait = waits[waits.length - 1] as Record<string, unknown>;
  if (
    !isPositiveSafeInteger(lastWait["index"]) ||
    lastWait["index"] !== closure["wait_index"] ||
    lastWait["transition_count"] !== closure["closed_transition_count"] ||
    !isNonEmptyString(lastWait["state_id"]) ||
    lastWait["state_id"] !== lastTransition["from"]
  ) {
    throw invalidResult("the replanned iteration closure does not anchor to the answered revise_task wait", null);
  }
  if (
    lastGeneration["opened_transition_count"] !== lastWait["transition_count"] ||
    openIteration["opened_transition_count"] !== lastWait["transition_count"]
  ) {
    throw invalidResult("the opened stage generation does not anchor to the answered revise_task wait", null);
  }
  const response = lastWait["response"] as unknown;
  if (
    !isRecord(response) ||
    response["action_id"] !== REVISE_TASK_ACTION_ID ||
    !isLowercaseSha256(response["response_sha256"])
  ) {
    throw invalidResult("the handoff boundary carries no revise_task wait response", null);
  }
  const intentRecord = lastWait["intent"] as unknown;
  if (!isRecord(intentRecord) || !isLowercaseSha256(intentRecord["intent_sha256"])) {
    throw invalidResult("the answered revise_task wait carries no accepted intent", null);
  }
  const actions = lastWait["actions"] as unknown;
  if (!Array.isArray(actions)) {
    throw invalidResult("the answered revise_task wait declares no actions", null);
  }
  let declaredTo: unknown;
  let declaredCount = 0;
  for (const actionValue of actions) {
    const action = actionValue as Record<string, unknown>;
    if (!isRecord(action) || !isNonEmptyString(action["id"]) || !isNonEmptyString(action["to"])) {
      throw invalidResult("the answered revise_task wait carries a malformed declared action", null);
    }
    if (action["id"] === REVISE_TASK_ACTION_ID) {
      declaredCount += 1;
      declaredTo = action["to"];
    }
  }
  if (declaredCount !== 1 || declaredTo !== lastTransition["from"]) {
    throw invalidResult("the answered revise_task wait does not declare the planning execution's state", null);
  }
  return {
    cursorState: cursor["current_state"],
    transitionCount,
    lastTransition,
    lastExecution,
    lastPlan,
    lastGeneration,
    openIteration,
    closureGeneration,
    closureIteration,
    closure,
    lastWait,
  };
}

/**
 * The defensive verification of one successful handoff result against the
 * verified boundary anchors: the exact typed contract field set, the flat
 * field types, the structural equality of `result.state` with the
 * authoritative snapshot, and every flat binding to the durable records
 * and the caller policy. Every failure is a typed `invalid_result` whose
 * error state is the verified authoritative snapshot — never the hostile
 * presentation.
 */
function verifyHandoffResultShape(
  resultValue: unknown,
  policy: PlanningRunPlanResumePolicy,
  state: PipelineV2RunState,
  anchors: HandoffBoundaryAnchors,
  pipeline: ResolvedPipelineV2,
): void {
  if (!isRecord(resultValue)) {
    throw invalidResult("the planning-run-plan handoff result is not a record", state);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    !hasExactOwnKeys(
      result,
      "wait_index",
      "from_state",
      "to_state",
      "transition_index",
      "execution_index",
      "generation_index",
      "iteration_index",
      "stage_id",
      "stage_position",
      "template_id",
      "initial_budget",
      "plan_revision",
      "plan_sha256",
      "state",
    )
  ) {
    throw invalidResult("the planning-run-plan handoff result carries foreign fields", state);
  }
  if (
    !isPositiveSafeInteger(result["wait_index"]) ||
    !isNonEmptyString(result["from_state"]) ||
    !isNonEmptyString(result["to_state"]) ||
    !isNonNegativeSafeInteger(result["transition_index"]) ||
    !isPositiveSafeInteger(result["execution_index"]) ||
    !isPositiveSafeInteger(result["generation_index"]) ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    !isNonEmptyString(result["stage_id"]) ||
    !isPositiveSafeInteger(result["stage_position"]) ||
    !isNonEmptyString(result["template_id"]) ||
    !isPositiveSafeInteger(result["initial_budget"]) ||
    !isPositiveSafeInteger(result["plan_revision"]) ||
    !isLowercaseSha256(result["plan_sha256"]) ||
    !isRecord(result["state"])
  ) {
    throw invalidResult("the planning-run-plan handoff result does not carry the exact contract fields", state);
  }
  if (!statesStructurallyEqual(result["state"], state)) {
    throw invalidResult("the planning-run-plan handoff result does not match the authoritative durable state", state);
  }
  if (result["stage_id"] !== policy.stageId || result["initial_budget"] !== policy.initialBudget) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the caller stage policy", state);
  }
  if (
    result["plan_revision"] !== anchors.lastPlan["revision"] ||
    result["plan_sha256"] !== anchors.lastPlan["sha256"]
  ) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the last accepted plan revision", state);
  }
  if (
    result["generation_index"] !== anchors.lastGeneration["index"] ||
    result["stage_id"] !== anchors.lastGeneration["stage_id"] ||
    result["stage_position"] !== anchors.lastGeneration["stage_position"] ||
    result["template_id"] !== anchors.lastGeneration["template_id"] ||
    result["plan_sha256"] !== anchors.lastGeneration["plan_sha256"] ||
    result["initial_budget"] !== anchors.lastGeneration["initial_budget"]
  ) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the opened stage generation", state);
  }
  if (result["iteration_index"] !== anchors.openIteration["index"]) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the open successor iteration", state);
  }
  if (
    result["wait_index"] !== anchors.closure["wait_index"] ||
    result["wait_index"] !== anchors.lastWait["index"]
  ) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the answered revise task wait", state);
  }
  if (
    result["transition_index"] !== anchors.lastTransition["index"] ||
    result["from_state"] !== anchors.lastTransition["from"] ||
    result["to_state"] !== anchors.lastTransition["to"] ||
    result["execution_index"] !== anchors.lastTransition["execution_index"] ||
    result["execution_index"] !== anchors.lastExecution["index"] ||
    result["to_state"] !== anchors.cursorState
  ) {
    throw invalidResult("the planning-run-plan handoff result does not bind to the committed planning transition", state);
  }
  const template = compiledStageTemplateFor(pipeline, result["template_id"] as string);
  if (template.entry_state !== result["to_state"]) {
    throw invalidResult("the committed transition does not lead to the selected stage's template entry state", state);
  }
}

/**
 * The defensive verification of one successfully returned coordinator
 * result union: only the exact runtime shapes of the coordinator's
 * `PipelineV2ResumeCoordinationResult` are accepted — a success carries
 * exactly the own enumerable keys `ok`,`state` (state record identical to
 * the authoritative snapshot); a refusal carries exactly
 * `ok`,`refused`,`reason`,`state` with `refused === true` and a reason
 * from the pinned refusal vocabulary; an ordinary failure carries exactly
 * `ok`,`reason`,`state` with no own `refused` field and a reason from the
 * canonical `PIPELINE_V2_FAILURE_REASONS` (the imported state-vocabulary
 * list, never a local copy); failure and refusal states are null or
 * exactly the authoritative sink snapshot. The verified result is
 * returned unchanged by object identity — refusal, worker failure,
 * signal failure and persistence failure stay coordinator-owned
 * classifications, and diagnostics never echo a hostile reason, key or
 * value.
 */
function verifyResumeResult(
  resultValue: unknown,
  authoritative: PipelineV2RunState | null,
): PipelineV2ResumeCoordinationResult {
  try {
    if (!isRecord(resultValue)) {
      throw invalidResult("the resume coordinator result is not a record", authoritative);
    }
    const result = resultValue as Record<string, unknown>;
    const keys = Object.keys(result);
    const ok = result["ok"];
    if (ok === true) {
      if (!hasExactOwnKeys(result, "ok", "state")) {
        throw invalidResult("the resume coordinator success result carries foreign fields", authoritative);
      }
      const state = result["state"];
      if (state !== authoritative || !isRecord(state)) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
    }
    if (ok !== false) {
      throw invalidResult("the resume coordinator result carries no valid ok discriminant", authoritative);
    }
    if (keys.includes("waiting")) {
      // The exact controlled-suspension branch of the coordinator: the run
      // is durably waiting at the trusted stage-wait boundary and is
      // returned by identity — never reclassified as a failure or refusal.
      if (!hasExactOwnKeys(result, "ok", "waiting", "state")) {
        throw invalidResult("the resume coordinator waiting result carries foreign fields", authoritative);
      }
      if (result["waiting"] !== true) {
        throw invalidResult("the resume coordinator result carries a malformed waiting discriminant", authoritative);
      }
      const state = result["state"];
      if (state !== authoritative || !isRecord(state)) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
    }
    if (keys.includes("refused")) {
      if (!hasExactOwnKeys(result, "ok", "refused", "reason", "state")) {
        throw invalidResult("the resume coordinator refusal result carries foreign fields", authoritative);
      }
      if (result["refused"] !== true) {
        throw invalidResult("the resume coordinator result carries a malformed refusal discriminant", authoritative);
      }
      const reason = result["reason"];
      if (!isString(reason) || !PIPELINE_V2_RESUME_REFUSAL_REASON_SET.has(reason)) {
        throw invalidResult("the resume coordinator refusal result carries no valid refusal reason", authoritative);
      }
      const state = result["state"];
      if (state !== null && state !== authoritative) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
    }
    if (!hasExactOwnKeys(result, "ok", "reason", "state")) {
      throw invalidResult("the resume coordinator failure result carries foreign fields", authoritative);
    }
    const reason = result["reason"];
    if (!isString(reason) || !PIPELINE_V2_RESUME_FAILURE_REASON_SET.has(reason)) {
      throw invalidResult("the resume coordinator failure result carries no valid failure reason", authoritative);
    }
    const state = result["state"];
    if (state !== null && state !== authoritative) {
      throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
    }
    return resultValue as unknown as PipelineV2ResumeCoordinationResult;
  } catch (cause) {
    if (cause instanceof PipelineV2PlanningRunPlanResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the resume coordinator result could not be verified", authoritative);
  }
}

/**
 * Validate, compose and verify one full planning-run-plan handoff followed
 * by the coordinator's resume entrypoint (see the module docstring for the
 * full order, capture boundary and verification semantics).
 */
export async function applyPipelineV2PlanningRunPlanResumeWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<PipelineV2ResumeCoordinationResult> {
  // Capture boundary: the options shape, every options field and the ops
  // record shape and its two members are read exactly once, all before the
  // first await and before any durable intervention. A later mutation of
  // the caller's options, runtime, control or ops cannot change this
  // handoff.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot: unknown = options["runRoot"];
  const sink: unknown = options["sink"];
  const runtime: unknown = options["runtime"];
  const control: unknown = options["control"];
  const stageId: unknown = options["stageId"];
  const initialBudget: unknown = options["initialBudget"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const applyHandoff: unknown = ops["applyHandoff"];
  const resumeRun: unknown = ops["resumeRun"];
  if (typeof applyHandoff !== "function" || typeof resumeRun !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires the two composed facade functions");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any scalar validation, sink read or facade call.
  // Its own typed error propagates unchanged.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 planning run plan resume controller");
  if (!isString(runRoot) || runRoot === "" || !isAbsolute(runRoot)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires an absolute runRoot string");
  }
  if (!isRecord(sink) || typeof (sink as Record<string, unknown>)["dispatch"] !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires a structural state sink");
  }
  if (!isRecord(runtime)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires an agent runtime object");
  }
  if (!isRecord(control)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires a signal control object");
  }
  if (!isPipelineV2SafeId(stageId)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires a safe stage id");
  }
  if (!isPositiveSafeInteger(initialBudget)) {
    throw invalidOptions("resumePipelineV2RunAfterPlanningRunPlanHandoff requires a positive safe integer initial budget");
  }
  const policy: PlanningRunPlanResumePolicy = { stageId, initialBudget };

  // The runtime and control contract functions are captured exactly once
  // and bound to their owners; the stable adapters the coordinator
  // consumes are built from the captured functions only, so a mutation of
  // the caller-owned runtime or control objects during the pending
  // handoff cannot change the resume. The signal lifecycle stays
  // coordinator-owned through the captured control functions.
  const capturedCreateExecutionSession = captureContractFunction(
    runtime as Record<string, unknown>,
    "createExecutionSession",
    "the agent runtime",
  );
  const capturedCreateToolSession = captureContractFunction(
    runtime as Record<string, unknown>,
    "createToolSession",
    "the agent runtime",
  );
  const capturedCurrentSignal = captureContractFunction(
    control as Record<string, unknown>,
    "currentSignal",
    "the signal control",
  );
  const capturedFreezeSignal = captureContractFunction(
    control as Record<string, unknown>,
    "freezeSignal",
    "the signal control",
  );
  const adaptedRuntime = deepFreezeValue({
    createExecutionSession: (state: Parameters<PipelineV2AgentRuntime["createExecutionSession"]>[0], activation: Parameters<PipelineV2AgentRuntime["createExecutionSession"]>[1]) =>
      (capturedCreateExecutionSession as PipelineV2AgentRuntime["createExecutionSession"])(state, activation),
    createToolSession: (state: Parameters<PipelineV2AgentRuntime["createToolSession"]>[0], activation: Parameters<PipelineV2AgentRuntime["createToolSession"]>[1]) =>
      (capturedCreateToolSession as PipelineV2AgentRuntime["createToolSession"])(state, activation),
  }) as unknown as PipelineV2AgentRuntime;
  const adaptedControl = deepFreezeValue({
    currentSignal: () => (capturedCurrentSignal as PipelineV2CoordinatorControl["currentSignal"])(),
    freezeSignal: () => (capturedFreezeSignal as PipelineV2CoordinatorControl["freezeSignal"])(),
  }) as unknown as PipelineV2CoordinatorControl;
  const structuralSink = sink as unknown as PipelineV2CoordinatorStateSink;

  // Step 1: the restart-aware planning-run-plan handoff facade — the plan
  // acceptance, the replanned stage opening and the committed planning
  // transition. Its typed errors pass through by object identity; the
  // resume is never started after one.
  const handoffResult: unknown = await (applyHandoff as typeof applyPipelineV2PlanningRunPlanHandoff)({
    pipeline,
    runRoot,
    sink: structuralSink,
    stageId,
    initialBudget,
  });

  // Step 2: the defensive verification of the successful handoff. The
  // authoritative snapshot is read exactly once, immediately after the
  // handoff and before the resume. The snapshot itself is verified as the
  // completed handoff boundary first; a snapshot that is not a record or
  // not a valid boundary can never serve as the error state, so those
  // failures carry `null`.
  const authoritativeBeforeResume: PipelineV2RunState | null = structuralSink.snapshot;
  if (!isRecord(authoritativeBeforeResume)) {
    throw invalidResult("the post-handoff durable state is not a record", null);
  }
  const postHandoffState = authoritativeBeforeResume as PipelineV2RunState;
  try {
    const anchors = verifyHandoffStateBoundary(postHandoffState);
    verifyHandoffResultShape(handoffResult, policy, postHandoffState, anchors, pipeline);
  } catch (cause) {
    if (cause instanceof PipelineV2PlanningRunPlanResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the planning-run-plan handoff result could not be verified", postHandoffState);
  }

  // Step 3: the coordinator's resume entrypoint — the captured pipeline,
  // run root and sink, the run id derived exclusively from the verified
  // authoritative state, and the stable runtime/control adapters. A thrown
  // error passes through by object identity and is never caught for a
  // second handoff inside this call.
  const resumeResult = await (resumeRun as typeof resumePipelineV2Run)(
    {
      pipeline,
      runId: postHandoffState.run_id,
      runRoot,
      sink: structuralSink,
      runtime: adaptedRuntime,
    },
    adaptedControl,
  );

  // Step 4: the defensive verification of the coordinator result union;
  // the last authoritative snapshot is read exactly once for the identity
  // binding and as the verification failures' error state. A malformed
  // post-resume snapshot never enters the error state: the verified
  // post-handoff snapshot is reported instead.
  const authoritativeAfterResume: PipelineV2RunState | null = structuralSink.snapshot;
  if (!isRecord(authoritativeAfterResume)) {
    throw invalidResult("the post-resume durable state is not a record", postHandoffState);
  }
  return verifyResumeResult(resumeResult, authoritativeAfterResume);
}
