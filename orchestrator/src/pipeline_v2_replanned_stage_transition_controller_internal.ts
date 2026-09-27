import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { compiledTransitionFor } from "./pipeline_engine.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "./pipeline_v2_run_plan_compiled_internal.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import type {
  PreparedPipelineV2RunWaitIntent,
  PipelineV2ReviseTaskIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral replanned-stage transition controller (unwired).
 *
 * The controller commits the single planning transition that follows a
 * successful replanned-stage composition: the exact boundary `old
 * generation closed by "replanned" on the target wait's anchor → next
 * generation of the accepted plan open with iteration 1 open on the same
 * anchor → transition_committed {from: declared revise target, outcome:
 * "completed", to: the selected stage's entry state, transition_index:
 * the declared edge's index, execution_index: the settled planning
 * execution}`. It performs no filesystem work, never builds or accepts a
 * plan candidate, never publishes any manifest, never runs a worker and
 * never starts the next stage execution; it dispatches through the
 * structural sink (satisfied by the production `PipelineV2RunStateSink`
 * without an adapter) and commits AT MOST ONE `transition_committed`.
 *
 * Capture order (fail-closed): the options shape; the fields `pipeline`
 * → `sink` → `intent` → `compiledPlan` → `stageId` → `initialBudget`
 * each read exactly once; the sink's `poisoned`, `dispatch` and initial
 * `snapshot` members captured exactly once as opaque references with
 * `dispatch` bound to the sink before the first await (no re-read of
 * `sink.dispatch`); the poison latch; the intent provenance gate (the
 * shared manifest registry — hand-built, cast, spread, `structuredClone`
 * and Proxy look-alikes are rejected before any field of the intent, of
 * the compiled plan or of the durable snapshot is read, Proxy traps
 * never invoked); the compiled stage resolved through the single
 * trusted compiled resolver `compiledPipelineV2RunPlanStageFor`
 * (compiled-resolver errors pass by identity) with the stage position
 * derived from the compiled plan's declaration order; `initialBudget`
 * validated as a positive safe integer (an `invalid_options` failure);
 * only then the single `validatePipelineV2RunState` (a missing or
 * invalid snapshot is the controller's own typed `invalid_state` with
 * `state: null`; unexpected causes propagate unchanged), the hidden
 * originating-identity comparison (`compiledRunPlanOriginIdentity` plus
 * the single `comparePipelineV2RunIdentity`; any mismatch is
 * `lifecycle_conflict`), and the durable bindings. A hostile extra
 * options field is ignored. Unexpected getters and errors propagate by
 * identity and are never classified from message text.
 *
 * Durable bindings (the C0 boundary, all before any dispatch):
 * - active/running, no terminal, run outputs or failure; the target
 *   wait (the intent's wait index) exactly once in the journal, keeping
 *   the exact accepted intent and answered exactly with the
 *   `revise_task` action; the cursor at the declared action's target
 *   with the transition journal exactly at the wait's `transition_count`
 *   and one settled-but-unbound execution beyond it;
 * - the last durable plan revision equals the compiled plan exactly
 *   (revision, digest, origin execution) and its predecessor exists;
 * - the OLD generation — the predecessor of the last — closed exactly
 *   `by: "replanned"` with `closed_transition_count === wait.transition_count`,
 *   bound to the PREDECESSOR plan digest, and its last iteration bound
 *   to the target wait by the exact `replanned` closure (`wait_index`
 *   and `closed_transition_count` exact; the generation's `closed.by`
 *   alone is never sufficient);
 * - the NEW generation — strictly the last — open, its exact index
 *   `old.index + 1`, bound exactly to the caller-selected stage
 *   (`stage_id`, `stage_position`, `template_id`, the CURRENT plan
 *   digest) and to the caller-selected `initial_budget`, with the wait
 *   anchor `opened_transition_count === wait.transition_count`, exactly
 *   one iteration (index 1) open on the same anchor with the exact
 *   `open_iteration` projection;
 * - the last execution: agent, `execution_role === "planning"`,
 *   `cleanup_completed`, at the cursor, with
 *   `index === compiledPlan.origin_execution` — otherwise typed
 *   failures (`invalid_state` for a not-on-boundary state,
 *   `lifecycle_conflict` for foreign bindings).
 *
 * The PREMATURE call (before the composition) is rejected before any
 * dispatch: the old iteration is already closed `by: "replanned"` at
 * that point, but the old GENERATION is still open and no next
 * generation of the accepted plan exists — the controller rejects the
 * boundary with `invalid_state`, never describing it as a stage
 * execution inside the old iteration.
 *
 * The transition step is derived ONLY from the durable cursor, the last
 * settled planning execution and the engine-owned edge resolver:
 * `from = cursor.current_state`, `outcome = "completed"` (the fixed v2
 * agent lifecycle outcome), `{to, transition_index} =
 * compiledTransitionFor(pipeline, from, "completed)`, `executionIndex =
 * lastExecution.index`; `to` must equal the compiled stage's
 * `entry_state` (`lifecycle_conflict` otherwise) — two stages may share
 * one entry state, so the step alone never proves the stage binding.
 * Engine resolution failures (`unknown_outcome`/`missing_state`) pass
 * through by identity. The single command is pre-checked through the
 * one reducer on a local snapshot before the dispatch (a rejection is a
 * typed `invalid_state` with zero dispatch; defense-in-depth), then
 * dispatched exactly once.
 *
 * C1 (the exact durable retry): the last transition record carries the
 * exact five fields, the cursor is at `to`, `transitions.length ===
 * wait.transition_count + 1`, `executions.length ===
 * transitions.length` (the planning execution is bound and the next
 * stage execution is not started), and every other binding above is
 * unchanged — zero dispatch, the authoritative state returned. A
 * durable transition changed in ANY field, a cursor not at `to`, a
 * started next execution or a run moved past the boundary is typed
 * `lifecycle_conflict`; a partial match is never an idempotent success.
 *
 * Post-dispatch verification is the same targeted check on the normal
 * resolve path and the racing `PipelineV2StateError` path: the only
 * allowed changes are the exact new transition record, the moved cursor
 * and the expected state revision increment (`after.revision ===
 * before.revision + 1`, run identity pinned); every other durable field
 * is unchanged except the routine `updated_at` refresh. An authoritative
 * `dispatch` that resolves without this exact durable change is typed
 * `invalid_state`, never a success. Diagnostics are content-free
 * (validated safe ids and indexes only); errors are never classified
 * from message text; unexpected causes propagate unchanged.
 *
 * Durability: a sink `not_committed` keeps the previous state
 * authoritative (a fresh retry dispatches the transition again); a sink
 * `durability_unknown` adopts the visible candidate (the transition is
 * durable), poisons the sink and dispatches nothing further (a fresh
 * reopened sink recognizes the durable transition with zero dispatch).
 * Nothing is ever rolled back.
 *
 * What remains policy (outside this controller): the moment of the
 * call, the caller-selected `stageId`/`initialBudget` (the same values
 * the composition received), and the next stage execution.
 *
 * Not implemented (stays unwired): the stage/budget selection policy,
 * the graph execution engine, starting the next stage execution,
 * automatic resume, retries, coordinator/runner/CLI/default-pipeline
 * wiring, schema/reducer changes, migrations/API/T3 and multi-process
 * locking.
 */

export type PipelineV2ReplannedStageTransitionControllerFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "lifecycle_conflict"
  | "state_persist_failed";

export class PipelineV2ReplannedStageTransitionControllerError extends Error {
  readonly reason: PipelineV2ReplannedStageTransitionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReplannedStageTransitionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReplannedStageTransitionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam shared with the composed controllers; the
 * production `PipelineV2RunStateSink` satisfies it without an adapter.
 * The controller binds `dispatch` once at capture and never re-reads the
 * sink member.
 */
export interface PipelineV2ReplannedStageTransitionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface OpenPipelineV2ReplannedStageTransitionOptions {
  readonly pipeline: import("./pipeline_v2.ts").ResolvedPipelineV2;
  readonly sink: PipelineV2ReplannedStageTransitionControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly initialBudget: number;
}

export interface OpenedPipelineV2ReplannedStageTransition {
  readonly wait_index: number;
  readonly from_state: string;
  readonly to_state: string;
  readonly transition_index: number;
  readonly execution_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly initial_budget: number;
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2ReplannedStageTransitionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReplannedStageTransitionControllerError {
  return new PipelineV2ReplannedStageTransitionControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2ReplannedStageTransitionControllerError {
  return controllerError("invalid_options", message, null);
}

const REVISE_TASK_ACTION_ID = "revise_task";
const AGENT_COMPLETED_OUTCOME = "completed";

interface TransitionBindings {
  readonly kind: "c0" | "c1";
  readonly manifest: PipelineV2ReviseTaskIntentManifest;
  readonly intentSha256: string;
  readonly wait: PipelineV2RunState["waits"][number];
  readonly declaredTarget: string;
  readonly oldGeneration: PipelineV2RunState["generations"][number];
  readonly oldIteration: PipelineV2RunState["generations"][number]["iterations"][number];
  readonly newGeneration: PipelineV2RunState["generations"][number];
  readonly lastPlanRecord: PipelineV2RunState["plan_revisions"][number];
  readonly compiledStage: CompiledPipelineV2RunPlanStage;
  readonly stagePosition: number;
  readonly fromState: string;
  readonly executionIndex: number;
  readonly toState: string;
  readonly transitionIndex: number;
}

/**
 * The target wait record must exist exactly once in the journal; a
 * duplicated wait index is never a valid verification target. Defensive
 * against structurally hostile snapshots: a non-array journal or a
 * non-record entry yields no match.
 */
function findWaitRecord(state: PipelineV2RunState, waitIndex: number): PipelineV2RunState["waits"][number] | undefined {
  if (!Array.isArray(state.waits)) {
    return undefined;
  }
  let found: PipelineV2RunState["waits"][number] | undefined;
  let count = 0;
  for (const record of state.waits) {
    if (isRecord(record) && record["index"] === waitIndex) {
      found = record as PipelineV2RunState["waits"][number];
      count += 1;
    }
  }
  return count === 1 ? found : undefined;
}

/**
 * The compiled stage of the caller-selected id with its declaration
 * position, both derived from the trusted compiled plan. The stage
 * lookup belongs to the single trusted compiled resolver; its errors
 * pass by identity.
 */
function resolveCompiledStage(
  compiledPlan: CompiledPipelineV2RunPlan,
  stageId: string,
): { readonly compiledStage: CompiledPipelineV2RunPlanStage; readonly stagePosition: number } {
  const compiledStage = compiledPipelineV2RunPlanStageFor(compiledPlan, stageId);
  const position = compiledPlan.stages.findIndex((stage) => stage.id === stageId) + 1;
  return { compiledStage, stagePosition: position };
}

/**
 * The full targeted C0 binding verification, all before any dispatch.
 * Defensive with record/array guards before every field access, so
 * malformed nested snapshots yield typed errors, never a `TypeError`.
 * The premature pre-composition boundary (the old iteration already
 * closed `by: "replanned"` while the old generation is still open and
 * no next generation of the accepted plan exists) is rejected here,
 * before the step resolution.
 */
function requireTransitionBindings(
  state: PipelineV2RunState,
  pipeline: OpenPipelineV2ReplannedStageTransitionOptions["pipeline"],
  intent: PreparedPipelineV2RunWaitIntent,
  compiledPlan: CompiledPipelineV2RunPlan,
  stageId: string,
  initialBudget: number,
): TransitionBindings {
  if (state.status !== "active" || state.phase !== "running") {
    throw controllerError(
      "invalid_state",
      "the run is not active and running; the replanned stage transition applies only at the composition boundary",
      state,
    );
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run already carries a terminal, publication or failure projection",
      state,
    );
  }
  const manifest = intent.manifest as PipelineV2ReviseTaskIntentManifest;
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
      `the run records no wait ${manifest.wait_index} for the replanned stage transition`,
      state,
    );
  }
  if (wait.intent === undefined || wait.intent.intent_sha256 !== intent.sha256) {
    throw controllerError(
      "invalid_state",
      `wait ${wait.index} accepted a different intent; one intent belongs to one wait`,
      state,
    );
  }
  const response = wait.response;
  if (response === undefined) {
    throw controllerError(
      "invalid_state",
      `wait record ${wait.index} is not answered; the replanned stage transition follows the recorded response`,
      state,
    );
  }
  if (response.action_id !== REVISE_TASK_ACTION_ID) {
    throw controllerError(
      "lifecycle_conflict",
      `wait ${wait.index} was answered with another action; this is not the revise-task transition boundary`,
      state,
    );
  }
  const declared = wait.actions.find((action) => action.id === REVISE_TASK_ACTION_ID);
  if (declared === undefined) {
    throw controllerError(
      "invalid_state",
      "the target wait record does not declare the revise_task action",
      state,
    );
  }
  const planRevisions = state.plan_revisions;
  const lastPlanRecord = planRevisions[planRevisions.length - 1];
  if (
    lastPlanRecord === undefined ||
    lastPlanRecord.revision !== compiledPlan.plan_revision ||
    lastPlanRecord.sha256 !== compiledPlan.plan_sha256 ||
    lastPlanRecord.origin_execution !== compiledPlan.origin_execution
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the last durable plan revision does not match the compiled plan",
      state,
    );
  }
  const previousPlanRecord = planRevisions[planRevisions.length - 2];
  if (previousPlanRecord === undefined) {
    throw controllerError(
      "invalid_state",
      "the accepted plan revision carries no predecessor plan revision",
      state,
    );
  }
  const generations = state.generations;
  const newGeneration = generations[generations.length - 1];
  if (newGeneration === undefined) {
    throw controllerError(
      "invalid_state",
      "the run records no stage generation; the replanned stage composition has not run",
      state,
    );
  }
  if (newGeneration.plan_sha256 !== compiledPlan.plan_sha256) {
    throw controllerError(
      "invalid_state",
      "the last stage generation does not belong to the accepted plan revision; the replanned stage composition has not run",
      state,
    );
  }
  if (newGeneration.closed !== undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the last stage generation ${newGeneration.index} of the accepted plan is closed`,
      state,
    );
  }
  const oldGeneration = generations[generations.length - 2];
  if (
    oldGeneration === undefined ||
    oldGeneration.index !== newGeneration.index - 1
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${newGeneration.index} of the accepted plan carries no directly preceding generation`,
      state,
    );
  }
  const oldClosed = oldGeneration.closed;
  if (
    !isRecord(oldClosed) ||
    oldClosed["by"] !== "replanned" ||
    oldClosed["closed_transition_count"] !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the previous stage generation ${oldGeneration.index} is not closed by the replanned closure on the wait boundary`,
      state,
    );
  }
  if (oldGeneration.plan_sha256 !== previousPlanRecord.sha256) {
    throw controllerError(
      "lifecycle_conflict",
      `the previous stage generation ${oldGeneration.index} does not belong to the predecessor plan revision ${previousPlanRecord.revision}`,
      state,
    );
  }
  const oldIterations = oldGeneration.iterations;
  const oldIteration = oldIterations[oldIterations.length - 1];
  if (oldIteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the previous stage generation ${oldGeneration.index} records no iterations`,
      state,
    );
  }
  const oldIterationClosed = oldIteration.closed;
  if (
    oldIterationClosed === undefined ||
    oldIterationClosed.by !== "replanned" ||
    oldIterationClosed.wait_index !== wait.index ||
    oldIterationClosed.closed_transition_count !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the last iteration of the previous stage generation ${oldGeneration.index} is not closed against wait ${wait.index} on the wait boundary`,
      state,
    );
  }
  const { compiledStage, stagePosition } = resolveCompiledStage(compiledPlan, stageId);
  if (
    newGeneration.stage_id !== compiledStage.id ||
    newGeneration.stage_position !== stagePosition ||
    newGeneration.template_id !== compiledStage.template ||
    newGeneration.initial_budget !== initialBudget
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the open stage generation ${newGeneration.index} does not match the caller-selected stage and budget`,
      state,
    );
  }
  if (newGeneration.opened_transition_count !== wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      `the open stage generation ${newGeneration.index} does not open on the wait boundary`,
      state,
    );
  }
  const iterations = newGeneration.iterations;
  if (
    newGeneration.iteration_count !== 1 ||
    !Array.isArray(iterations) ||
    iterations.length !== 1
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the open stage generation ${newGeneration.index} does not carry exactly one iteration`,
      state,
    );
  }
  const iteration = iterations[0]!;
  if (
    iteration.index !== 1 ||
    iteration.opened_transition_count !== wait.transition_count ||
    iteration.closed !== undefined
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${newGeneration.index} does not open iteration 1 on the wait boundary`,
      state,
    );
  }
  const openIteration = newGeneration.open_iteration;
  if (
    !isRecord(openIteration) ||
    openIteration["index"] !== 1 ||
    openIteration["opened_transition_count"] !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${newGeneration.index} does not project its open iteration 1 on the wait boundary`,
      state,
    );
  }
  // The transition journal relative to the wait boundary decides the
  // form; anything beyond the boundary is typed `lifecycle_conflict`.
  const transitions = state.transitions;
  if (!Array.isArray(transitions)) {
    throw controllerError(
      "lifecycle_conflict",
      "the run state carries no transition journal",
      state,
    );
  }
  if (transitions.length !== wait.transition_count && transitions.length !== wait.transition_count + 1) {
    throw controllerError(
      "lifecycle_conflict",
      "the run moved past the planning transition boundary",
      state,
    );
  }
  const lastExecution = state.executions[state.executions.length - 1];
  if (
    lastExecution === undefined ||
    lastExecution.type !== "agent" ||
    lastExecution.execution_role !== "planning" ||
    lastExecution.phase !== "cleanup_completed" ||
    lastExecution.state_id !== declared.to
  ) {
    throw controllerError(
      "invalid_state",
      "the last execution is not the settled planning execution at the composition boundary",
      state,
    );
  }
  if (lastExecution.index !== compiledPlan.origin_execution) {
    throw controllerError(
      "lifecycle_conflict",
      `the settled planning execution ${lastExecution.index} does not match the compiled plan origin execution ${compiledPlan.origin_execution}`,
      state,
    );
  }
  // The transition step is derived ONLY here: from the durable cursor
  // (C0) or the exact durable transition (C1), the last settled planning
  // execution and the engine-owned edge resolver. The engine errors pass
  // through by identity; a resolution whose edge does not lead to the
  // selected stage's entry state is a typed `lifecycle_conflict`.
  let fromState: string;
  if (transitions.length === wait.transition_count) {
    // C0: the transition is not committed yet; the cursor is the only
    // source of the step's origin.
    if (state.cursor.current_state !== declared.to) {
      throw controllerError(
        "lifecycle_conflict",
        "the cursor is not at the revise_task action target of the composition boundary",
        state,
      );
    }
    if (state.cursor.transition_count !== wait.transition_count) {
      throw controllerError(
        "lifecycle_conflict",
        "the cursor does not match the composition boundary",
        state,
      );
    }
    if (state.executions.length !== wait.transition_count + 1) {
      throw controllerError(
        "lifecycle_conflict",
        "the execution journal does not carry exactly one settled execution beyond the composition boundary",
        state,
      );
    }
    fromState = state.cursor.current_state;
  } else {
    // C1: the exact durable transition is the only recognizable retry;
    // a changed field is never an idempotent success.
    const last = transitions[transitions.length - 1]!;
    if (last.from !== declared.to || last.outcome !== AGENT_COMPLETED_OUTCOME) {
      throw controllerError(
        "lifecycle_conflict",
        "the durable planning transition does not match the exact step of the composition boundary",
        state,
      );
    }
    if (state.executions.length !== transitions.length) {
      throw controllerError(
        "lifecycle_conflict",
        "an execution was started after the committed planning transition",
        state,
      );
    }
    fromState = last.from;
  }
  const resolved = compiledTransitionFor(pipeline, fromState, AGENT_COMPLETED_OUTCOME);
  if (resolved.to !== compiledStage.entry_state) {
    throw controllerError(
      "lifecycle_conflict",
      `the completed edge of the planning state does not lead to the entry state of the selected stage ${JSON.stringify(compiledStage.id)}`,
      state,
    );
  }
  if (transitions.length === wait.transition_count + 1) {
    const last = transitions[transitions.length - 1]!;
    if (
      last.index !== resolved.transition_index ||
      last.to !== resolved.to ||
      !isRecord(state.cursor) ||
      state.cursor["current_state"] !== resolved.to
    ) {
      throw controllerError(
        "lifecycle_conflict",
        "the durable planning transition does not match the exact step of the composition boundary",
        state,
      );
    }
  }
  return {
    kind: transitions.length === wait.transition_count ? "c0" : "c1",
    manifest,
    intentSha256: intent.sha256,
    wait,
    declaredTarget: declared.to,
    oldGeneration,
    oldIteration,
    newGeneration,
    lastPlanRecord,
    compiledStage,
    stagePosition,
    fromState,
    executionIndex: lastExecution.index,
    toState: resolved.to,
    transitionIndex: resolved.transition_index,
  };
}

/**
 * The reducer pre-check of the transition command on a local snapshot,
 * before the dispatch; a reducer rejection is a typed `invalid_state`
 * with zero dispatch and any other cause propagates unchanged. Internal
 * to this module: not a runtime export and not a test seam (the
 * pre-check-before-dispatch ordering is proven by the source-order test
 * of the flow).
 */
function precheckTransition(
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
        "the current run state does not accept the replanned stage transition",
        snapshot,
      );
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
  bindings: TransitionBindings,
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
    last["index"] !== bindings.transitionIndex ||
    last["from"] !== bindings.fromState ||
    last["outcome"] !== AGENT_COMPLETED_OUTCOME ||
    last["to"] !== bindings.toState ||
    last["execution_index"] !== bindings.executionIndex
  ) {
    return false;
  }
  if (
    !isRecord(after.cursor) ||
    after.cursor["current_state"] !== bindings.toState ||
    after.cursor["transition_count"] !== bindings.wait.transition_count + 1
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
 * presentation as `invalid_state` directly (the controller's own dispatch
 * succeeded, so only the exact change or a lying presentation is possible).
 */
function raceOrMismatch(
  after: PipelineV2RunState | null,
  bindings: TransitionBindings,
): never {
  if (after !== null && Array.isArray(after.transitions) && after.transitions.length === bindings.wait.transition_count + 1) {
    const last = after.transitions[after.transitions.length - 1]!;
    if (isRecord(last) && last["execution_index"] === bindings.executionIndex) {
      throw controllerError(
        "lifecycle_conflict",
        "the durable planning transition does not match the exact step of the composition boundary",
        after,
      );
    }
  }
  throw controllerError(
    "invalid_state",
    `the run state does not carry the committed planning transition of wait ${bindings.wait.index}`,
    after,
  );
}

/**
 * Validate, bind and commit the single planning transition through the
 * existing reducer (see the module docstring for the full order and
 * durability semantics).
 */
export async function openPipelineV2ReplannedStageTransitionInternal(
  options: unknown,
): Promise<OpenedPipelineV2ReplannedStageTransition> {
  // Capture boundary: every options field is read exactly once (`pipeline`
  // → `sink` → `intent` → `compiledPlan` → `stageId` → `initialBudget`),
  // and the sink's `poisoned`, `dispatch` and initial `snapshot` members
  // are read exactly once as opaque references. No field of the pipeline,
  // the intent, the compiled plan or the durable snapshot is read here.
  if (!isRecord(options)) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires an options object");
  }
  const pipeline = options["pipeline"];
  const sink = options["sink"];
  const intent = options["intent"];
  const compiledPlan = options["compiledPlan"];
  const stageId = options["stageId"];
  const initialBudget = options["initialBudget"];
  if (!isRecord(sink)) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires a sink object");
  }
  if (!isRecord(intent)) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires a prepared wait intent object");
  }
  if (!isRecord(compiledPlan)) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires a compiled plan object");
  }
  if (typeof stageId !== "string") {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires a string stage id");
  }
  if (!isRecord(pipeline)) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires the trusted pipeline snapshot object");
  }
  if (typeof initialBudget !== "number" || !Number.isSafeInteger(initialBudget) || initialBudget <= 0) {
    throw invalidOptions("openPipelineV2ReplannedStageTransition requires a positive safe integer initial budget");
  }
  const poisoned = sink["poisoned"];
  const dispatch = sink["dispatch"];
  const initialSnapshot = sink["snapshot"];
  if (typeof poisoned !== "boolean") {
    throw invalidOptions("the run state sink requires a boolean poisoned flag");
  }
  if (typeof dispatch !== "function") {
    throw invalidOptions("the run state sink requires a dispatch function");
  }
  const sinkRef = sink as unknown as PipelineV2ReplannedStageTransitionControllerSink;
  // The dispatch is bound to the sink immediately at capture: a later
  // reassignment of the sink's member cannot change the dispatch target,
  // and the sink member is never read again.
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no transition.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no replanned stage transition can be committed",
      null,
    );
  }
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the revise kind. Hand-built,
  // cast, spread, cloned and Proxy look-alikes are rejected here, before
  // any field of the intent or of the durable snapshot is read.
  if (!hasPreparedRunPlanProvenance(intent, "revise_task_intent")) {
    throw invalidOptions("the intent is not a provenance-registered revise_task_intent");
  }
  // The compiled stage through the single trusted compiled resolver; its
  // provenance gate, stage existence and template binding belong to that
  // resolver alone. Its errors pass by identity.
  const { compiledStage, stagePosition } = resolveCompiledStage(compiledPlan as unknown as CompiledPipelineV2RunPlan, stageId);
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
  // The hidden originating-identity comparison: a compiled plan compiled
  // from a foreign pipeline is never accepted, even when run id, plan
  // revision and plan digest coincide.
  const originIdentity = compiledRunPlanOriginIdentity(compiledPlan as unknown as CompiledPipelineV2RunPlan);
  const comparison = comparePipelineV2RunIdentity(originIdentity, state.pipeline);
  if (comparison.kind === "mismatch") {
    throw controllerError(
      "lifecycle_conflict",
      `the compiled plan identity does not match the durable pipeline identity: ${comparison.field}`,
      state,
    );
  }
  const bindings = requireTransitionBindings(
    state,
    pipeline as unknown as OpenPipelineV2ReplannedStageTransitionOptions["pipeline"],
    intent as unknown as PreparedPipelineV2RunWaitIntent,
    compiledPlan as unknown as CompiledPipelineV2RunPlan,
    stageId,
    initialBudget,
  );
  if (bindings.kind === "c1") {
    // C1: the exact durable transition; the authoritative state is the
    // verified result — zero dispatch.
    return finishResult(state, bindings);
  }
  // C0: the reducer pre-check of the single transition command precedes
  // the dispatch; a rejection is typed `invalid_state` with zero
  // dispatch.
  const transitionCommand: PipelineV2RunCommand = {
    kind: "transition_committed",
    step: {
      from: bindings.fromState,
      outcome: AGENT_COMPLETED_OUTCOME,
      to: bindings.toState,
      transition_index: bindings.transitionIndex,
    },
    executionIndex: bindings.executionIndex,
  };
  precheckTransition(state, transitionCommand, state);
  try {
    await dispatchCommand(transitionCommand);
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "the replanned stage transition could not be confirmed durable",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "the replanned stage transition could not be committed",
        sinkRef.snapshot,
      );
    }
    if (cause instanceof PipelineV2StateError) {
      // A racing identical dispatch is idempotent success only on the
      // full exact verification of the authoritative snapshot.
      const after = sinkRef.snapshot;
      if (after !== null && transitionAppliedExactly(after, state, bindings)) {
        return finishResult(after, bindings);
      }
      raceOrMismatch(after, bindings);
    }
    throw cause;
  }
  const after = sinkRef.snapshot;
  if (after === null || !transitionAppliedExactly(after, state, bindings)) {
    // The dispatch itself just succeeded, so a full mismatch can only be
    // a hostile or non-authoritative snapshot presentation: the exact
    // change is missing, never a different lifecycle step.
    throw controllerError(
      "invalid_state",
      `the run state does not carry the committed planning transition of wait ${bindings.wait.index}`,
      after,
    );
  }
  return finishResult(after, bindings);
}

function finishResult(
  state: PipelineV2RunState,
  bindings: TransitionBindings,
): OpenedPipelineV2ReplannedStageTransition {
  return deepFreezeValue({
    wait_index: bindings.wait.index,
    from_state: bindings.fromState,
    to_state: bindings.toState,
    transition_index: bindings.transitionIndex,
    execution_index: bindings.executionIndex,
    generation_index: bindings.newGeneration.index,
    iteration_index: 1,
    stage_id: bindings.compiledStage.id,
    stage_position: bindings.stagePosition,
    template_id: bindings.compiledStage.template,
    initial_budget: bindings.newGeneration.initial_budget,
    plan_revision: bindings.lastPlanRecord.revision,
    plan_sha256: bindings.lastPlanRecord.sha256,
    state,
  });
}
