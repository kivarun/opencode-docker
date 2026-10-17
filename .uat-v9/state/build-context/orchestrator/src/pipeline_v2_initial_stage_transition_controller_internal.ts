import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { compiledTransitionFor } from "./pipeline_engine.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "./pipeline_v2_run_plan_compiled_internal.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import {
  applyStageTransitionCommit,
  type StageTransitionApplyFailureReason,
} from "./pipeline_v2_stage_transition_apply_internal.ts";

/**
 * Production-neutral initial stage transition controller (unwired).
 *
 * The controller commits the single planning transition of the INITIAL
 * plan-ready handoff — the exact boundary `accepted initial plan
 * revision → optional generation 1 / iteration 1 of the selected stage
 * → transition_committed {from: the planning state, outcome:
 * "completed", to: the selected stage's compiled entry state,
 * transition_index: the declared edge's index, execution_index: the
 * settled planning execution}`. It is the initial-boundary sibling of
 * the replanned-stage transition controller and shares the one
 * transition-application kernel with it; the lifecycle policy (which
 * boundary admits the step, which stage and budget the caller selected)
 * stays here.
 *
 * The controller performs no filesystem work: it never publishes a
 * manifest, never accepts a plan candidate, never opens a generation or
 * iteration (the ensure controller does, through the handoff), never
 * runs a worker and never starts the next stage execution; it
 * dispatches through the structural sink (satisfied by the production
 * `PipelineV2RunStateSink` without an adapter) and commits AT MOST ONE
 * `transition_committed`. The step is derived only from the durable
 * cursor (C0) or the exact durable transition (C1) and the compiled
 * edge of the planning state; `to` must equal the selected stage's
 * compiled entry state.
 *
 * Capture order (fail-closed): the options shape; the fields `pipeline`
 * → `sink` → `compiledPlan` → `stageId` → `initialBudget` each read
 * exactly once; the sink's `poisoned`, `dispatch` and initial
 * `snapshot` members captured exactly once as opaque references; the
 * poison latch; the pipeline provenance gate (before any field of the
 * pipeline is read, Proxy traps never invoked); the compiled stage
 * resolved through the single trusted compiled resolver
 * `compiledPipelineV2RunPlanStageFor` (compiled-resolver errors pass by
 * identity) with the stage position derived from the compiled plan's
 * declaration order; `initialBudget` validated as a positive safe
 * integer (an `invalid_options` failure); only then the single
 * `validatePipelineV2RunState` (a missing or invalid snapshot is the
 * controller's own typed `invalid_state` with `state: null`; unexpected
 * causes propagate unchanged), the hidden originating-identity
 * comparison (`compiledRunPlanOriginIdentity` plus the single
 * `comparePipelineV2RunIdentity`), the exact boundary bindings and the
 * completed-edge gate.
 *
 * C0 bindings before any dispatch: active/running with no
 * terminal/publication/failure; no wait journal, no grants and no
 * historical replanned closure (the initial boundary carries none);
 * `executions.length === transitions.length + 1` with the last
 * execution the settled-but-unbound planning execution (agent, role
 * `planning`, phase `cleanup_completed`, no `iteration_index`, on the
 * cursor); exactly one durable plan revision matching the compiled plan
 * (revision, digest, `origin_execution` = the last execution index);
 * exactly the one open generation bound to the selected stage, position,
 * template, plan digest and caller budget with exactly iteration 1 open
 * on the cursor anchor — the handoff always ensures the stage before the
 * transition, so a fresh form without the generation is refused
 * (`lifecycle_conflict`) because it could never be recovered; and the
 * compiled `completed` edge of the planning state targeting exactly the
 * selected stage's entry state (`lifecycle_conflict` otherwise — a wrong
 * caller stage never writes a generation).
 *
 * C1 recognizes only the exact durable transition by every field with
 * the cursor at `to`, the same one-generation/one-iteration binding and
 * no started next execution (a changed field is `lifecycle_conflict`,
 * never an idempotent success); anything beyond the boundary is
 * `lifecycle_conflict`.
 *
 * Durability: `not_committed` keeps the previous snapshot authoritative
 * (a fresh retry re-dispatches); `durability_unknown` adopts the visible
 * candidate, poisons the sink and a fresh reopened sink recognizes the
 * durable transition with zero dispatch; nothing is ever rolled back.
 * Diagnostics are content-free; unexpected causes propagate by identity.
 * What remains policy: the moment of the call, the caller-selected
 * `stageId`/`initialBudget`, and the next stage execution.
 */

export type PipelineV2InitialStageTransitionControllerFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "lifecycle_conflict"
  | "state_persist_failed";

export class PipelineV2InitialStageTransitionControllerError extends Error {
  readonly reason: PipelineV2InitialStageTransitionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2InitialStageTransitionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2InitialStageTransitionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam shared with the composed controllers; the
 * production `PipelineV2RunStateSink` satisfies it without an adapter.
 */
export interface PipelineV2InitialStageTransitionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: import("./pipeline_v2_state.ts").PipelineV2RunCommand) => void | Promise<void>;
}

export interface OpenPipelineV2InitialStageTransitionOptions {
  readonly pipeline: ResolvedPipelineV2;
  readonly sink: PipelineV2InitialStageTransitionControllerSink;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly initialBudget: number;
}

/**
 * The exact result of the initial planning transition: the initial
 * bindings only — no wait index, no revise-only fields — with the
 * authoritative post-transition state.
 */
export interface OpenedPipelineV2InitialStageTransition {
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly initial_budget: number;
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly origin_execution: number;
  readonly from_state: string;
  readonly to_state: string;
  readonly transition_index: number;
  readonly execution_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2InitialStageTransitionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2InitialStageTransitionControllerError {
  return new PipelineV2InitialStageTransitionControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2InitialStageTransitionControllerError {
  return controllerError("invalid_options", message, null);
}

const AGENT_COMPLETED_OUTCOME = "completed";

function resolveCompiledStage(
  compiledPlan: CompiledPipelineV2RunPlan,
  stageId: string,
): { readonly compiledStage: CompiledPipelineV2RunPlanStage; readonly stagePosition: number } {
  const compiledStage = compiledPipelineV2RunPlanStageFor(compiledPlan, stageId);
  const position = compiledPlan.stages.findIndex((stage) => stage.id === stageId) + 1;
  return { compiledStage, stagePosition: position };
}

/**
 * The exact one-generation form shared by the C0 retry recognition and
 * the C1 boundary: generation 1 open, bound to the selected stage,
 * position, template, plan digest and caller budget, anchored on the
 * boundary's transition count, with exactly iteration 1 open on the
 * same anchor. Defensive with record/array guards before every field
 * access, so malformed nested snapshots yield `false`, never a
 * `TypeError`.
 */
function generationMatchesInitialStage(
  generations: PipelineV2RunState["generations"],
  expected: {
    readonly stageId: string;
    readonly stagePosition: number;
    readonly templateId: string;
    readonly planSha256: string;
    readonly initialBudget: number;
    readonly anchor: number;
  },
): boolean {
  if (generations.length !== 1) {
    return false;
  }
  const generation = generations[0];
  if (generation === undefined || !isRecord(generation)) {
    return false;
  }
  if (
    generation["index"] !== 1 ||
    generation["stage_id"] !== expected.stageId ||
    generation["stage_position"] !== expected.stagePosition ||
    generation["template_id"] !== expected.templateId ||
    generation["plan_sha256"] !== expected.planSha256 ||
    generation["initial_budget"] !== expected.initialBudget ||
    generation["opened_transition_count"] !== expected.anchor ||
    generation["iteration_count"] !== 1 ||
    generation["closed"] !== undefined
  ) {
    return false;
  }
  const iterations = generation["iterations"];
  if (!Array.isArray(iterations) || iterations.length !== 1) {
    return false;
  }
  const iteration = iterations[0];
  if (iteration === undefined || !isRecord(iteration)) {
    return false;
  }
  if (
    iteration["index"] !== 1 ||
    iteration["opened_transition_count"] !== expected.anchor ||
    iteration["closed"] !== undefined
  ) {
    return false;
  }
  const openIteration = generation["open_iteration"];
  return (
    isRecord(openIteration) &&
    openIteration["index"] === 1 &&
    openIteration["opened_transition_count"] === expected.anchor
  );
}

/**
 * The settled planning execution: the last execution record must be an
 * agent execution with the planning role, the completed cleanup phase
 * and no iteration index. Defensive with record guards; returns `null`
 * when the boundary does not carry it. The cursor binding is checked
 * separately: unbound-on-cursor for C0, transition-bound for C1.
 */
function settledPlanningExecution(
  state: PipelineV2RunState,
): { readonly index: number; readonly stateId: string } | null {
  const executions = state.executions;
  if (!Array.isArray(executions) || executions.length === 0) {
    return null;
  }
  const last = executions[executions.length - 1];
  if (last === undefined || !isRecord(last)) {
    return null;
  }
  if (
    last["type"] !== "agent" ||
    last["execution_role"] !== "planning" ||
    last["phase"] !== "cleanup_completed" ||
    last["iteration_index"] !== undefined
  ) {
    return null;
  }
  const index = last["index"];
  const stateId = last["state_id"];
  if (typeof index !== "number" || !Number.isSafeInteger(index) || index <= 0 || typeof stateId !== "string") {
    return null;
  }
  return { index, stateId };
}

/**
 * The full targeted boundary verification, all before any dispatch.
 * Defensive with record/array guards before every field access, so
 * malformed nested snapshots yield typed errors, never a `TypeError`.
 */
function requireInitialTransitionBindings(
  state: PipelineV2RunState,
  pipeline: ResolvedPipelineV2,
  compiledPlan: CompiledPipelineV2RunPlan,
  compiledStage: CompiledPipelineV2RunPlanStage,
  stagePosition: number,
  initialBudget: number,
): {
  readonly kind: "c0" | "c1";
  readonly fromState: string;
  readonly toState: string;
  readonly transitionIndex: number;
  readonly executionIndex: number;
  readonly planRecord: PipelineV2RunState["plan_revisions"][number];
  readonly priorTransitionCount: number;
} {
  if (state.status !== "active" || state.phase !== "running") {
    throw controllerError(
      "invalid_state",
      "the initial stage transition requires an active running run",
      state,
    );
  }
  if (
    state.terminal !== undefined ||
    state.run_outputs !== undefined ||
    state.failure !== undefined
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the initial stage transition requires a run without a terminal, published outputs or a failure",
      state,
    );
  }
  if (!Array.isArray(state.waits) || state.waits.length !== 0) {
    throw controllerError(
      "lifecycle_conflict",
      "the initial stage transition requires a run without a wait journal",
      state,
    );
  }
  if (!Array.isArray(state.grants) || state.grants.length !== 0) {
    throw controllerError(
      "lifecycle_conflict",
      "the initial stage transition requires a run without iteration grants",
      state,
    );
  }
  const executions = state.executions;
  const transitions = state.transitions;
  if (!Array.isArray(executions) || !Array.isArray(transitions)) {
    throw controllerError(
      "invalid_state",
      "the durable run state does not carry the execution and transition journals",
      state,
    );
  }
  const settled = settledPlanningExecution(state);
  if (settled === null) {
    throw controllerError(
      "invalid_state",
      "the initial stage transition requires the settled unbound planning execution on the cursor",
      state,
    );
  }
  // The exactly-one accepted plan revision of the initial boundary,
  // matching the compiled plan.
  const planRevisions = state.plan_revisions;
  if (!Array.isArray(planRevisions) || planRevisions.length !== 1) {
    throw controllerError(
      planRevisions !== undefined && Array.isArray(planRevisions) && planRevisions.length > 1
        ? "lifecycle_conflict"
        : "invalid_state",
      "the initial stage transition requires exactly one accepted plan revision",
      state,
    );
  }
  const planRecord = planRevisions[0]!;
  if (
    !isRecord(planRecord) ||
    planRecord["index"] !== 1 ||
    planRecord["revision"] !== compiledPlan.plan_revision ||
    planRecord["sha256"] !== compiledPlan.plan_sha256 ||
    planRecord["previous_sha256"] !== null ||
    planRecord["origin_execution"] !== settled.index
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the accepted plan revision does not match the compiled plan of the initial boundary",
      state,
    );
  }  // The boundary form: C0 (the settled-but-unbound planning execution,
  // one transition short) or C1 (the exact durable transition). The
  // anchor of the generation/iteration opening is the transition count
  // at the boundary: the cursor count for C0 and one behind the moved
  // cursor for C1.
  const isC0 = executions.length === transitions.length + 1;
  const isC1 = executions.length === transitions.length;
  if (!isC0 && !isC1) {
    throw controllerError(
      "invalid_state",
      "the initial stage transition requires the settled unbound planning execution on the cursor",
      state,
    );
  }
  const cursor = state.cursor;
  if (!isRecord(cursor)) {
    throw controllerError(
      "invalid_state",
      "the durable run state does not carry the cursor",
      state,
    );
  }
  const anchor = isC0 ? cursor["transition_count"] : (cursor["transition_count"] as number) - 1;
  // The generation form: exactly the one open generation bound to the
  // selected stage, position, template, plan digest and caller budget
  // with exactly iteration 1 open on the boundary anchor — the ensure
  // controller opens it before this controller is called (the handoff
  // order) and its recognized retry form is the same record; a missing
  // generation before the transition would leave the transition
  // unrecoverable, so both forms require it. Anything else is durable
  // history that does not belong to the initial boundary.
  const generations = state.generations;
  if (!Array.isArray(generations) || generations.length !== 1) {
    throw controllerError(
      "lifecycle_conflict",
      "the initial stage transition requires the opened generation of the selected stage",
      state,
    );
  }
  if (generations.length === 1) {
    const generation = generations[0];
    if (
      generation === undefined ||
      !generationMatchesInitialStage(generations, {
        stageId: compiledStage.id,
        stagePosition,
        templateId: compiledStage.template,
        planSha256: planRecord.sha256 as string,
        initialBudget,
        anchor,
      })
    ) {
      throw controllerError(
        "lifecycle_conflict",
        "the durable stage generation does not match the selected initial stage and budget",
        state,
      );
    }
  }
  // The completed edge of the planning state, resolved through the
  // engine-owned resolver; the target must be the selected stage's
  // compiled entry state.
  const resolved = compiledTransitionFor(
    pipeline,
    settled.stateId,
    AGENT_COMPLETED_OUTCOME,
  );
  if (resolved.to !== compiledStage.entry_state) {
    throw controllerError(
      "lifecycle_conflict",
      "the completed planning transition does not target the selected stage's entry state",
      state,
    );
  }
  if (isC0) {
    // The settled-but-unbound planning execution sits on the cursor;
    // the transition has not been committed yet.
    if (cursor["current_state"] !== settled.stateId) {
      throw controllerError(
        "invalid_state",
        "the initial stage transition requires the settled unbound planning execution on the cursor",
        state,
      );
    }
    return {
      kind: "c0",
      fromState: settled.stateId,
      toState: resolved.to,
      transitionIndex: resolved.transition_index,
      executionIndex: settled.index,
      planRecord: planRecord as PipelineV2RunState["plan_revisions"][number],
      priorTransitionCount: anchor,
    };
  }
  // C1: the exact durable transition; every field must match.
  const last = transitions[transitions.length - 1];
  if (
    last === undefined ||
    !isRecord(last) ||
    last["index"] !== resolved.transition_index ||
    last["from"] !== settled.stateId ||
    last["outcome"] !== AGENT_COMPLETED_OUTCOME ||
    last["to"] !== resolved.to ||
    last["execution_index"] !== settled.index
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable initial planning transition does not match the exact step of the initial handoff boundary",
      state,
    );
  }
  if (cursor["current_state"] !== resolved.to) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable cursor does not carry the committed initial planning transition",
      state,
    );
  }
  return {
    kind: "c1",
    fromState: settled.stateId,
    toState: resolved.to,
    transitionIndex: resolved.transition_index,
    executionIndex: settled.index,
    planRecord: planRecord as PipelineV2RunState["plan_revisions"][number],
    priorTransitionCount: anchor,
  };
}

/**
 * Validate, bind and commit the single initial planning transition
 * through the existing reducer (see the module docstring for the full
 * order and durability semantics).
 */
export async function openPipelineV2InitialStageTransitionInternal(
  options: unknown,
): Promise<OpenedPipelineV2InitialStageTransition> {
  // Capture boundary: every options field is read exactly once (`pipeline`
  // → `sink` → `compiledPlan` → `stageId` → `initialBudget`), and the
  // sink's `poisoned`, `dispatch` and initial `snapshot` members are read
  // exactly once as opaque references. No field of the pipeline, the
  // compiled plan or the durable snapshot is read here.
  if (!isRecord(options)) {
    throw invalidOptions("openPipelineV2InitialStageTransition requires an options object");
  }
  const pipeline = options["pipeline"];
  const sink = options["sink"];
  const compiledPlan = options["compiledPlan"];
  const stageId = options["stageId"];
  const initialBudget = options["initialBudget"];
  if (!isRecord(pipeline)) {
    throw invalidOptions("openPipelineV2InitialStageTransition requires the trusted pipeline snapshot object");
  }
  if (!isRecord(sink)) {
    throw invalidOptions("openPipelineV2InitialStageTransition requires a sink object");
  }
  if (!isRecord(compiledPlan)) {
    throw invalidOptions("openPipelineV2InitialStageTransition requires a compiled plan object");
  }
  if (typeof stageId !== "string") {
    throw invalidOptions("openPipelineV2InitialStageTransition requires a string stage id");
  }
  if (typeof initialBudget !== "number" || !Number.isSafeInteger(initialBudget) || initialBudget <= 0) {
    throw invalidOptions("openPipelineV2InitialStageTransition requires a positive safe integer initial budget");
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
  // The fail-closed poison latch: a poisoned sink accepts no transition.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no initial stage transition can be committed",
      null,
    );
  }
  // The pipeline provenance gate: before any field of the pipeline is
  // read; hand-built, cast, spread, cloned and Proxy look-alikes are
  // rejected here with Proxy traps never invoked.
  requireResolvedPipelineV2Provenance(
    pipeline as unknown as ResolvedPipelineV2,
    "the initial stage transition controller",
  );
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
  const bindings = requireInitialTransitionBindings(
    state,
    pipeline as unknown as ResolvedPipelineV2,
    compiledPlan as unknown as CompiledPipelineV2RunPlan,
    compiledStage,
    stagePosition,
    initialBudget,
  );
  if (bindings.kind === "c1") {
    // C1: the exact durable transition; the authoritative state is the
    // verified result — zero dispatch.
    return finishResult(state, bindings, compiledStage, stagePosition, initialBudget);
  }
  // C0: the single transition application through the shared kernel —
  // the reducer pre-check, the one dispatch, the durability mapping,
  // the racing classification and the exact post-transition
  // verification are the kernel's; the wording keeps this controller's
  // messages.
  const applied = await applyStageTransitionCommit(
    sink,
    {
      preState: state,
      step: {
        from: bindings.fromState,
        outcome: AGENT_COMPLETED_OUTCOME,
        to: bindings.toState,
        transition_index: bindings.transitionIndex,
      },
      executionIndex: bindings.executionIndex,
      priorTransitionCount: bindings.priorTransitionCount,
    },
    (reason, message, failState) =>
      controllerError(reason as PipelineV2InitialStageTransitionControllerFailureReason, message, failState),
    {
      precheckRejected: "the current run state does not accept the initial planning transition",
      notDurable: "the initial stage transition could not be confirmed durable",
      notCommitted: "the initial stage transition could not be committed",
      raceConflict: "the durable initial planning transition does not match the exact step of the initial handoff boundary",
      missingTransition: "the run state does not carry the committed initial planning transition",
    },
  );
  return finishResult(applied.state, bindings, compiledStage, stagePosition, initialBudget);
}

function finishResult(
  state: PipelineV2RunState,
  bindings: {
    readonly fromState: string;
    readonly toState: string;
    readonly transitionIndex: number;
    readonly executionIndex: number;
    readonly planRecord: PipelineV2RunState["plan_revisions"][number];
  },
  compiledStage: CompiledPipelineV2RunPlanStage,
  stagePosition: number,
  initialBudget: number,
): OpenedPipelineV2InitialStageTransition {
  return deepFreezeValue({
    stage_id: compiledStage.id,
    stage_position: stagePosition,
    template_id: compiledStage.template,
    initial_budget: initialBudget,
    plan_revision: bindings.planRecord.revision,
    plan_sha256: bindings.planRecord.sha256,
    origin_execution: bindings.executionIndex,
    from_state: bindings.fromState,
    to_state: bindings.toState,
    transition_index: bindings.transitionIndex,
    execution_index: bindings.executionIndex,
    generation_index: 1,
    iteration_index: 1,
    state,
  });
}
