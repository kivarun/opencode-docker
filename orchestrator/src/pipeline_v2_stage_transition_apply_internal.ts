import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import {
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import type { TransitionStep } from "./pipeline_engine.ts";

/**
 * The shared transition-application kernel of the stage-transition
 * lifecycle controllers.
 *
 * The kernel is the ONE dispatch site for the terminal planning
 * transition of a stage handoff: given a controller-validated
 * authoritative pre-state, the exact expected transition step and the
 * execution index the transition binds, it pre-checks the single
 * `transition_committed` command through the reducer on a local
 * snapshot, dispatches exactly once through the structural sink,
 * applies the shared durability mapping, classifies a racing dispatch
 * and verifies the exact post-transition delta against the
 * authoritative snapshot.
 *
 * The kernel owns NO lifecycle policy: it selects no boundary, resolves
 * no edge, derives no binding and builds no result beyond the verified
 * post-state. The owning controllers keep their capture, provenance,
 * state-validation, binding and classification logic and hand the
 * kernel a fully validated request together with their own error
 * factory and per-call diagnostics wording, so every thrown error keeps
 * the owning controller's own class and message.
 *
 * The durability semantics are exactly the shared production semantics:
 * a `PipelineV2RunStateDurabilityError` adopts the visible candidate
 * snapshot (`state_persist_failed`); a plain store error keeps the
 * previous snapshot authoritative (`state_persist_failed`); a reducer
 * rejection on the racing path is an idempotent success only on the
 * full exact verification of the authoritative snapshot and is
 * otherwise classified as a mismatch; nothing is ever rolled back and
 * no second dispatch follows a failure.
 */

export type StageTransitionApplyFailureReason =
  | "invalid_state"
  | "lifecycle_conflict"
  | "state_persist_failed";

/**
 * The structural sink seam shared by the transition controllers; the
 * production `PipelineV2RunStateSink` satisfies it without an adapter.
 */
export interface StageTransitionApplySink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

/**
 * The kernel's request: the controller-validated authoritative
 * pre-state, the exact expected transition step, the execution index
 * the transition binds and the transition count at the boundary (the
 * cursor count the committed transition must move from one past).
 */
export interface StageTransitionApplyRequest {
  readonly preState: PipelineV2RunState;
  readonly step: TransitionStep;
  readonly executionIndex: number;
  readonly priorTransitionCount: number;
}

/**
 * Per-controller diagnostics wording; every entry is the owning
 * controller's full message, built per call so owner-specific details
 * (for example the target wait index) stay byte-identical.
 */
export interface StageTransitionApplyWording {
  readonly precheckRejected: string;
  readonly notDurable: string;
  readonly notCommitted: string;
  readonly raceConflict: string;
  readonly missingTransition: string;
}

/** The kernel outcome: the verified authoritative post-transition state. */
export interface AppliedStageTransitionCommit {
  readonly state: PipelineV2RunState;
}

/**
 * The owner's error factory; every kernel failure is built through it so
 * the thrown error keeps the owning controller's own class and state
 * payload.
 */
export type StageTransitionApplyFail = (
  reason: StageTransitionApplyFailureReason,
  message: string,
  state: PipelineV2RunState | null,
) => Error;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * The reducer pre-check of the transition command on a local snapshot,
 * before the dispatch; a reducer rejection is a typed `invalid_state`
 * with zero dispatch and any other cause propagates unchanged.
 */
function precheckTransition(
  state: PipelineV2RunState,
  command: PipelineV2RunCommand,
  fail: StageTransitionApplyFail,
  wording: StageTransitionApplyWording,
): void {
  try {
    reducePipelineV2RunCommand(state, command, new Date());
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw fail("invalid_state", wording.precheckRejected, state);
    }
    throw cause;
  }
}

/**
 * The per-position equality of one wait record: identity fields, ordered
 * `{id,to}` actions, the accepted intent and the response. Every viewed
 * entry is shape-checked before any field access.
 */
function waitRecordBindingsMatch(before: PipelineV2RunState["waits"][number], after: unknown): boolean {
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
 * One generation record unchanged by position: identity bindings, the
 * closure (absence/presence with `by`/wait index/boundary) and every
 * iteration (index, opening anchor, exact closed projection).
 */
function generationRecordUnchanged(before: PipelineV2RunState["generations"][number], after: unknown): boolean {
  if (!isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before.index ||
    after["stage_id"] !== before.stage_id ||
    after["stage_position"] !== before.stage_position ||
    after["template_id"] !== before.template_id ||
    after["plan_sha256"] !== before.plan_sha256 ||
    after["initial_budget"] !== before.initial_budget ||
    after["opened_transition_count"] !== before.opened_transition_count ||
    after["iteration_count"] !== before.iteration_count
  ) {
    return false;
  }
  const afterIterations = after["iterations"];
  if (!Array.isArray(afterIterations) || afterIterations.length !== before.iterations.length) {
    return false;
  }
  for (let position = 0; position < before.iterations.length; position += 1) {
    const beforeIteration = before.iterations[position]!;
    const entry = afterIterations[position];
    if (entry === undefined || !isRecord(entry)) {
      return false;
    }
    if (
      entry["index"] !== beforeIteration.index ||
      entry["opened_transition_count"] !== beforeIteration.opened_transition_count
    ) {
      return false;
    }
    const afterClosed = entry["closed"];
    const beforeClosed = beforeIteration.closed;
    const closedEqual =
      beforeClosed === undefined
        ? afterClosed === undefined
        : isRecord(afterClosed) &&
          afterClosed["by"] === beforeClosed.by &&
          afterClosed["wait_index"] === beforeClosed.wait_index &&
          afterClosed["closed_transition_count"] === beforeClosed.closed_transition_count;
    if (!closedEqual) {
      return false;
    }
  }
  const afterOpenIteration = after["open_iteration"];
  if (before.open_iteration === undefined) {
    if (afterOpenIteration !== undefined) {
      return false;
    }
  } else if (
    !isRecord(afterOpenIteration) ||
    afterOpenIteration["index"] !== before.open_iteration.index ||
    afterOpenIteration["opened_transition_count"] !== before.open_iteration.opened_transition_count
  ) {
    return false;
  }
  const afterClosed = after["closed"];
  if (before.closed === undefined) {
    return afterClosed === undefined;
  }
  return (
    isRecord(afterClosed) &&
    afterClosed["by"] === before.closed.by &&
    afterClosed["closed_transition_count"] === before.closed.closed_transition_count
  );
}

/**
 * One execution record unchanged by position: every contract field the
 * transition is not allowed to touch.
 */
function executionUnchanged(before: PipelineV2RunState["executions"][number], after: unknown): boolean {
  if (!isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before.index ||
    after["type"] !== before.type ||
    after["state_id"] !== before.state_id ||
    after["execution_role"] !== before.execution_role ||
    after["phase"] !== before.phase
  ) {
    return false;
  }
  if (before.type === "agent") {
    if (
      after["attempt"] !== before.attempt ||
      after["profile"] !== before.profile ||
      after["iteration_index"] !== before.iteration_index ||
      after["execution_session_id"] !== before.execution_session_id ||
      after["tool_session_id"] !== before.tool_session_id ||
      after["failure_reason"] !== before.failure_reason
    ) {
      return false;
    }
    const beforeCleanup = before.session_cleanup;
    const afterCleanup = after["session_cleanup"];
    const cleanupEqual =
      beforeCleanup === undefined
        ? afterCleanup === undefined
        : isRecord(afterCleanup) &&
          afterCleanup["execution"] === beforeCleanup.execution &&
          afterCleanup["tool"] === beforeCleanup.tool;
    if (!cleanupEqual) {
      return false;
    }
    const beforeOutputs = before.outputs;
    const afterOutputs = after["outputs"];
    if (beforeOutputs === undefined) {
      return afterOutputs === undefined;
    }
    if (!Array.isArray(afterOutputs) || afterOutputs.length !== beforeOutputs.length) {
      return false;
    }
    return beforeOutputs.every(
      (output, position) =>
        afterOutputs[position] !== undefined &&
        isRecord(afterOutputs[position]) &&
        (afterOutputs[position] as Record<string, unknown>)["id"] === output.id &&
        (afterOutputs[position] as Record<string, unknown>)["digest"] === output.digest,
    );
  }
  return (
    after["input_digest"] === before.input_digest &&
    after["iteration_index"] === before.iteration_index &&
    decisionResultUnchanged(before.result, after["result"]) &&
    after["failure_reason"] === before.failure_reason
  );
}

/**
 * The historical decision result unchanged by its schema-owned fields
 * (narrow per-field comparison; no deep comparator, no revalidation).
 */
function decisionResultUnchanged(
  before: unknown,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  const scalarEqual = (field: string) => after[field] === before[field];
  if (
    !scalarEqual("status") ||
    !scalarEqual("outcome") ||
    !scalarEqual("decision") ||
    !scalarEqual("rule_id") ||
    !scalarEqual("reason") ||
    !scalarEqual("fact_id") ||
    !scalarEqual("actual_type")
  ) {
    return false;
  }
  const listEqual = (field: string) => {
    const beforeList = before[field];
    const afterList = after[field];
    if (beforeList === undefined) {
      return afterList === undefined;
    }
    return (
      Array.isArray(beforeList) &&
      Array.isArray(afterList) &&
      afterList.length === beforeList.length &&
      beforeList.every((entry, position) => afterList[position] === entry)
    );
  };
  return listEqual("active_constraint_ids") && listEqual("violated_relation_ids");
}

/**
 * One committed transition record unchanged by position.
 */
function transitionRecordUnchanged(
  before: PipelineV2RunState["transitions"][number],
  after: unknown,
): boolean {
  return (
    isRecord(after) &&
    after["index"] === before.index &&
    after["from"] === before.from &&
    after["outcome"] === before.outcome &&
    after["to"] === before.to &&
    after["execution_index"] === before.execution_index
  );
}

/**
 * The exact post-transition verification: the only allowed changes are
 * the exact new transition record, the moved cursor and the expected
 * state revision increment; every other durable field is unchanged
 * except the routine `updated_at` refresh. Every array and nested entry
 * is shape-checked before any field read, so a hostile malformed
 * snapshot yields `false` and a typed error, never a `TypeError`.
 */
function transitionAppliedExactly(
  after: PipelineV2RunState,
  before: PipelineV2RunState,
  request: StageTransitionApplyRequest,
): boolean {
  if (!isRecord(after)) {
    return false;
  }
  if (
    after.run_id !== before.run_id ||
    after.revision !== before.revision + 1 ||
    after.schema_version !== before.schema_version ||
    after.status !== before.status ||
    after.phase !== before.phase ||
    after.started_at !== before.started_at
  ) {
    return false;
  }
  if (
    after.terminal !== undefined ||
    after.run_outputs !== undefined ||
    after.failure !== undefined
  ) {
    return false;
  }
  if (comparePipelineV2RunIdentity(before.pipeline, after.pipeline).kind !== "match") {
    return false;
  }
  if (!Array.isArray(after.inputs) || after.inputs.length !== before.inputs.length) {
    return false;
  }
  for (let position = 0; position < before.inputs.length; position += 1) {
    const beforeInput = before.inputs[position]!;
    const afterEntry = after.inputs[position];
    if (
      afterEntry === undefined ||
      !isRecord(afterEntry) ||
      afterEntry["id"] !== beforeInput.id ||
      afterEntry["type"] !== beforeInput.type ||
      afterEntry["protected"] !== beforeInput.protected ||
      afterEntry["digest"] !== beforeInput.digest
    ) {
      return false;
    }
  }
  if (!Array.isArray(after.transitions) || after.transitions.length !== before.transitions.length + 1) {
    return false;
  }
  for (let position = 0; position < before.transitions.length; position += 1) {
    if (!transitionRecordUnchanged(before.transitions[position]!, after.transitions[position])) {
      return false;
    }
  }
  const last = after.transitions[after.transitions.length - 1]!;
  if (
    !isRecord(last) ||
    last["index"] !== request.step.transition_index ||
    last["from"] !== request.step.from ||
    last["outcome"] !== request.step.outcome ||
    last["to"] !== request.step.to ||
    last["execution_index"] !== request.executionIndex
  ) {
    return false;
  }
  if (
    !isRecord(after.cursor) ||
    after.cursor["current_state"] !== request.step.to ||
    after.cursor["transition_count"] !== request.priorTransitionCount + 1
  ) {
    return false;
  }
  if (!Array.isArray(after.executions) || after.executions.length !== before.executions.length) {
    return false;
  }
  for (let position = 0; position < before.executions.length; position += 1) {
    if (!executionUnchanged(before.executions[position]!, after.executions[position])) {
      return false;
    }
  }
  if (
    !Array.isArray(after.waits) ||
    after.waits.length !== before.waits.length ||
    !before.waits.every((beforeEntry, position) => waitRecordBindingsMatch(beforeEntry, after.waits[position]))
  ) {
    return false;
  }
  if (
    !Array.isArray(after.task_revisions) ||
    after.task_revisions.length !== before.task_revisions.length ||
    !before.task_revisions.every((beforeEntry, position) => {
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
    })
  ) {
    return false;
  }
  if (
    !Array.isArray(after.plan_revisions) ||
    after.plan_revisions.length !== before.plan_revisions.length ||
    !before.plan_revisions.every((beforeEntry, position) => {
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
    })
  ) {
    return false;
  }
  if (!Array.isArray(after.grants) || after.grants.length !== before.grants.length) {
    return false;
  }
  for (let position = 0; position < before.grants.length; position += 1) {
    const beforeGrant = before.grants[position]!;
    const afterEntry = after.grants[position];
    if (
      afterEntry === undefined ||
      !isRecord(afterEntry) ||
      afterEntry["index"] !== beforeGrant.index ||
      afterEntry["generation_index"] !== beforeGrant.generation_index ||
      afterEntry["wait_index"] !== beforeGrant.wait_index ||
      afterEntry["intent_sha256"] !== beforeGrant.intent_sha256 ||
      afterEntry["additional_iterations"] !== beforeGrant.additional_iterations
    ) {
      return false;
    }
  }
  if (
    !Array.isArray(after.generations) ||
    after.generations.length !== before.generations.length ||
    !before.generations.every((beforeEntry, position) => generationRecordUnchanged(beforeEntry, after.generations[position]))
  ) {
    return false;
  }
  return true;
}

/**
 * The typed classification of a failed racing-dispatch verification: the
 * reducer rejected the command because another dispatch already moved the
 * run, so the presented snapshot is searched for a different committed
 * transition at the boundary. Used only on the racing `PipelineV2StateError`
 * path; the normal post-dispatch resolve path reports a mismatched
 * presentation as `invalid_state` directly (the dispatch succeeded, so
 * only the exact change or a lying presentation is possible).
 */
function raceOrMismatch(
  after: PipelineV2RunState | null,
  request: StageTransitionApplyRequest,
  fail: StageTransitionApplyFail,
  wording: StageTransitionApplyWording,
): never {
  if (
    after !== null &&
    Array.isArray(after.transitions) &&
    after.transitions.length === request.priorTransitionCount + 1
  ) {
    const last = after.transitions[after.transitions.length - 1]!;
    if (isRecord(last) && last["execution_index"] === request.executionIndex) {
      throw fail("lifecycle_conflict", wording.raceConflict, after);
    }
  }
  throw fail("invalid_state", wording.missingTransition, after);
}

const REQUEST_KEYS = ["executionIndex", "preState", "priorTransitionCount", "step"] as const;
const STEP_KEYS = ["from", "outcome", "to", "transition_index"] as const;
const WORDING_KEYS = [
  "missingTransition",
  "notCommitted",
  "notDurable",
  "precheckRejected",
  "raceConflict",
] as const;

function exactKeysSorted(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const own = Object.keys(value).sort();
  return own.length === keys.length && keys.every((key, position) => own[position] === key);
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

/**
 * Validate, bind and commit the exact expected transition through the
 * one shared dispatch site (see the module docstring for the full order
 * and durability semantics).
 */
export async function applyStageTransitionCommit(
  sink: unknown,
  request: unknown,
  fail: StageTransitionApplyFail,
  wording: unknown,
): Promise<AppliedStageTransitionCommit> {
  // The error factory must be callable before anything else is read.
  if (typeof fail !== "function") {
    throw new TypeError("the transition application error factory is not a function");
  }
  // The sink shape: a record carrying a boolean poisoned flag and a
  // dispatch function. The kernel reads each member exactly once and
  // binds the dispatch to the sink immediately: a later reassignment of
  // the sink's member cannot change the dispatch target.
  if (!isRecord(sink)) {
    throw fail("invalid_state", "the transition application sink is not a record", null);
  }
  const poisoned = sink["poisoned"];
  const dispatch = sink["dispatch"];
  if (typeof poisoned !== "boolean" || typeof dispatch !== "function") {
    throw fail("invalid_state", "the transition application sink is malformed", null);
  }
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no transition.
  if (poisoned) {
    throw fail(
      "invalid_state",
      "the run state sink is poisoned; no stage transition can be committed",
      null,
    );
  }
  // The request shape: the exact contract fields only.
  if (!isRecord(request)) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  if (!exactKeysSorted(request, REQUEST_KEYS)) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  const preState = request["preState"];
  const stepValue = request["step"];
  const executionIndex = request["executionIndex"];
  if (!isRecord(preState) || !isRecord(stepValue) || !isPositiveSafeInteger(executionIndex)) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  if (!exactKeysSorted(stepValue, STEP_KEYS)) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  const from = stepValue["from"];
  const outcome = stepValue["outcome"];
  const to = stepValue["to"];
  const transitionIndex = stepValue["transition_index"];
  if (
    !isPipelineV2SafeId(from) ||
    !nonEmptyString(outcome) ||
    !isPipelineV2SafeId(to) ||
    !isNonNegativeSafeInteger(transitionIndex)
  ) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  if (!isRecord(wording) || !exactKeysSorted(wording, WORDING_KEYS)) {
    throw fail("invalid_state", "the transition application wording is malformed", null);
  }
  for (const key of WORDING_KEYS) {
    if (!nonEmptyString(wording[key])) {
      throw fail("invalid_state", "the transition application wording is malformed", null);
    }
  }
  const validatedWording = wording as unknown as StageTransitionApplyWording;
  const priorTransitionCount = request["priorTransitionCount"];
  if (!isNonNegativeSafeInteger(priorTransitionCount)) {
    throw fail("invalid_state", "the transition application request is malformed", null);
  }
  // The pre-state must carry the journals the exact post-transition
  // verification compares by position.
  if (
    !Array.isArray(preState.transitions) ||
    !Array.isArray(preState.inputs) ||
    !Array.isArray(preState.executions) ||
    !Array.isArray(preState.waits) ||
    !Array.isArray(preState.task_revisions) ||
    !Array.isArray(preState.plan_revisions) ||
    !Array.isArray(preState.grants) ||
    !Array.isArray(preState.generations)
  ) {
    throw fail("invalid_state", "the transition application pre-state is malformed", preState as unknown as PipelineV2RunState);
  }
  const validatedRequest: StageTransitionApplyRequest = {
    preState: preState as unknown as PipelineV2RunState,
    step: { from, outcome, to, transition_index: transitionIndex },
    executionIndex,
    priorTransitionCount,
  };
  // The single transition command; the kernel dispatches nothing else.
  const transitionCommand: PipelineV2RunCommand = {
    kind: "transition_committed",
    step: {
      from: validatedRequest.step.from,
      outcome: validatedRequest.step.outcome,
      to: validatedRequest.step.to,
      transition_index: validatedRequest.step.transition_index,
    },
    executionIndex: validatedRequest.executionIndex,
  };
  // The reducer pre-check of the single transition command precedes the
  // dispatch; a rejection is a typed `invalid_state` with zero dispatch.
  precheckTransition(validatedRequest.preState, transitionCommand, fail, validatedWording);
  try {
    await dispatchCommand(transitionCommand);
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw fail(
        "state_persist_failed",
        validatedWording.notDurable,
        (sink as unknown as StageTransitionApplySink).snapshot,
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw fail(
        "state_persist_failed",
        validatedWording.notCommitted,
        (sink as unknown as StageTransitionApplySink).snapshot,
      );
    }
    if (cause instanceof PipelineV2StateError) {
      // A racing identical dispatch is idempotent success only on the
      // full exact verification of the authoritative snapshot.
      const after = (sink as unknown as StageTransitionApplySink).snapshot;
      if (after !== null && transitionAppliedExactly(after, validatedRequest.preState, validatedRequest)) {
        return deepFreezeResult(after);
      }
      raceOrMismatch(after, validatedRequest, fail, validatedWording);
    }
    throw cause;
  }
  const after = (sink as unknown as StageTransitionApplySink).snapshot;
  if (after === null || !transitionAppliedExactly(after, validatedRequest.preState, validatedRequest)) {
    // The dispatch itself just succeeded, so a full mismatch can only be
    // a hostile or non-authoritative snapshot presentation: the exact
    // change is missing, never a different lifecycle step.
    throw fail("invalid_state", validatedWording.missingTransition, after);
  }
  return deepFreezeResult(after);
}

function deepFreezeResult(state: PipelineV2RunState): AppliedStageTransitionCommit {
  return deepFreezeValue({ state });
}
