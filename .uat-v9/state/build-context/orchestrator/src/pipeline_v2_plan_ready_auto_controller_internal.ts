/**
 * Internal core of the trusted automatic plan-ready continuation
 * controller for pipeline schema v2.
 *
 * This controller is the single layer that turns a controlled `planReady`
 * suspension (or its durable restart forms) into the automatic continuation
 * of the run, using only the existing authoritative facades. It owns no
 * successor rule of its own: the routing between the two entry facades is
 * decided by one structural bit read from the authoritative snapshot, and
 * every durable validation stays with the existing acceptance, restore,
 * handoff and coordinator facades.
 *
 * Fixed sequence (see `applyPipelineV2PlanReadyContinuationWithIo`):
 *
 * 1. the options and ops capture (every field read exactly once, hostile
 *    extras never read, all before the first await);
 * 2. the pipeline provenance gate — clones, casts, spreads and Proxies are
 *    rejected before any scalar validation, sink read or facade call;
 * 3. ONE authoritative `sink.snapshot` read;
 * 4. the trusted plan-ready policy, extracted only from the compiled
 *    planning-role metadata of the state of the last durable execution
 *    (`pipelineV2PlanReadyPolicyFor` — absent policy or a non-planning
 *    boundary is a typed `invalid_state`, never a heuristic and never a
 *    default); the routing branch is then decided only by the structural
 *    bit `executions.length === transitions.length + 1` (the
 *    settled-but-unbound acceptance boundary) versus
 *    `executions.length === transitions.length` (the committed handoff
 *    boundary) — the bit is a routing discriminator only, never a second
 *    state validator;
 * 5. the matching facade call — `acceptPipelineV2PlanningRunPlan` on the
 *    acceptance boundary (its reconciliation owns the fresh, partial and
 *    already-durable acceptance windows A0–A4) or
 *    `restorePipelineV2AcceptedRunPlan` on the committed boundary (the
 *    read-only plan restoration feeding the handoff's Branch B/C1
 *    zero-dispatch recognition at A5);
 * 6. the stage taken strictly as `compiled_plan.stages[stage_position - 1]`
 *    of the CURRENT accepted plan revision (an out-of-range position is a
 *    typed fail-closed error; the acceptance may already be durable at that
 *    point — the confirmed contract boundary);
 * 7. the stage confirmed through the single existing provenance-checked
 *    resolver `compiledPipelineV2RunPlanStageFor` (its typed errors pass
 *    through by identity; the confirmed stage must be the exact object of
 *    the projection);
 * 8. `resumePipelineV2RunAfterPlanningRunPlanHandoff` called with the
 *    confirmed `stageId` and the trusted `initial_budget`, and the exact
 *    downstream coordination result returned by object identity — waiting,
 *    failure, refusal and success stay coordinator-owned classifications
 *    with no new envelope and no reclassification.
 *
 * There is no catch-based boundary classification anywhere: downstream
 * typed errors and unexpected errors propagate unchanged by object
 * identity, and the composition implements no second restore, parser,
 * proposal reader, plan compiler, retry machine or state validator.
 * Diagnostics are content-free: safe ids and structural counts only — no
 * proposal/task/output bodies, no digests, no paths, no hostile values.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2PlanReadyAutoControllerError`,
 * `applyPipelineV2PlanReadyContinuationWithIo`, the frozen
 * `productionPlanReadyAutoOps`, the policy type
 * `PipelineV2PlanReadyAutoPolicy` and the pure routing/policy helper
 * `pipelineV2PlanReadyPolicyFor`; the public module exports the error, the
 * continuation facade and the policy helper (types are not runtime keys).
 *
 * Not implemented (stays out of scope): automatic wait-action selection,
 * the automatic revise/continue policy, retries beyond the facades' own
 * idempotent windows, multi-process locking, migrations/API/T3.
 */
import { isAbsolute } from "node:path";
import { acceptPipelineV2PlanningRunPlan } from "./pipeline_v2_planning_run_plan_controller.ts";
import { resumePipelineV2RunAfterPlanningRunPlanHandoff } from "./pipeline_v2_planning_run_plan_resume_controller.ts";
import { compiledPipelineV2RunPlanStageFor, type CompiledPipelineV2RunPlan, type CompiledPipelineV2RunPlanStage } from "./pipeline_v2_run_plan_compiled.ts";
import { restorePipelineV2AcceptedRunPlan } from "./pipeline_v2_run_plan_restore.ts";
import {
  deepFreezeValue,
} from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";
import type {
  PipelineV2AgentRuntime,
  PipelineV2CoordinatorControl,
  PipelineV2CoordinatorStateSink,
  PipelineV2ResumeCoordinationResult,
} from "./pipeline_v2_coordinator.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";

export type PipelineV2PlanReadyAutoControllerFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "invalid_result";

/**
 * A failure of the automatic plan-ready continuation layer itself with its
 * stable machine-readable `reason` and the last authoritative durable
 * state (`null` when none was established). The composed facades' typed
 * errors pass through by identity and never take this shape.
 */
export class PipelineV2PlanReadyAutoControllerError extends Error {
  declare readonly reason: PipelineV2PlanReadyAutoControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2PlanReadyAutoControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.state = state;
    this.name = "PipelineV2PlanReadyAutoControllerError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

/**
 * The trusted plan-ready continuation policy resolved from the compiled
 * planning-role metadata: the 1-based stage position of the current
 * accepted plan and the initial budget of the opened stage generation.
 */
export interface PipelineV2PlanReadyAutoPolicy {
  readonly stage_position: number;
  readonly initial_budget: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2PlanReadyAutoControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanReadyAutoControllerError {
  return new PipelineV2PlanReadyAutoControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2PlanReadyAutoControllerError {
  return controllerError("invalid_options", message, null);
}

function invalidState(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanReadyAutoControllerError {
  return controllerError("invalid_state", message, state);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanReadyAutoControllerError {
  return controllerError("invalid_result", message, state);
}

/**
 * The pure routing/policy helper of the automatic plan-ready continuation.
 * It answers one question read-only: does this durable snapshot sit on a
 * settled planning execution whose compiled planning role carries the
 * trusted `plan_ready` policy, and does that boundary belong to the
 * automatic continuation at all?
 *
 * The pipeline must be the exact deep-frozen snapshot a successful
 * `loadPipelineV2` returned (the provenance gate runs first; a forged
 * pipeline is a typed error, never a silent `null`). Every structural
 * doubt — a non-record snapshot, a non-active run, a terminal/publication/
 * failure projection, malformed journals, a non-planning or in-flight last
 * execution, a committed planning transition that does not lead to a
 * stage-role state, missing orchestration metadata, an undeclared planning
 * role, or an absent `plan_ready` policy — yields `null`: the caller then
 * takes the ordinary (manual) path, which keeps the exact established
 * refusal and suspension semantics. The helper never throws on durable
 * state shapes and never validates the state beyond what routing needs;
 * the authoritative validation stays with the facades.
 */
export function pipelineV2PlanReadyPolicyFor(
  pipeline: ResolvedPipelineV2,
  snapshot: unknown,
): PipelineV2PlanReadyAutoPolicy | null {
  requireResolvedPipelineV2Provenance(pipeline, "pipelineV2PlanReadyPolicyFor");
  if (!isRecord(snapshot)) {
    return null;
  }
  if (snapshot["status"] !== "active" || snapshot["phase"] !== "running") {
    return null;
  }
  if (
    snapshot["terminal"] !== undefined ||
    snapshot["run_outputs"] !== undefined ||
    snapshot["failure"] !== undefined
  ) {
    return null;
  }
  const executions = snapshot["executions"];
  const transitions = snapshot["transitions"];
  if (!Array.isArray(executions) || !Array.isArray(transitions) || executions.length === 0) {
    return null;
  }
  const lastExecution = executions[executions.length - 1];
  if (!isRecord(lastExecution)) {
    return null;
  }
  if (
    lastExecution["type"] !== "agent" ||
    lastExecution["execution_role"] !== "planning" ||
    lastExecution["phase"] !== "cleanup_completed"
  ) {
    return null;
  }
  if (executions.length === transitions.length) {
    // The committed handoff boundary (A5): the planning transition is
    // already durable, so the continuation applies only when that
    // transition leads to a stage-role state — every other target (a
    // control decision, a further planning state) is ordinary resume
    // progress, never a handoff window.
    const lastTransition = transitions[transitions.length - 1];
    if (!isRecord(lastTransition) || lastTransition["execution_index"] !== lastExecution["index"]) {
      return null;
    }
    const targetState = lastTransition["to"];
    const orchestration = pipeline.orchestration;
    if (
      typeof targetState !== "string" ||
      orchestration === undefined ||
      !orchestration.execution_roles.some(
        (role) => role.state_id === targetState && role.role === "stage",
      )
    ) {
      return null;
    }
  }
  const orchestration = pipeline.orchestration;
  if (orchestration === undefined) {
    return null;
  }
  const stateId = lastExecution["state_id"];
  if (typeof stateId !== "string") {
    return null;
  }
  const role = orchestration.execution_roles.find((entry) => entry.state_id === stateId);
  if (role === undefined || role.role !== "planning" || role.plan_ready === undefined) {
    return null;
  }
  return {
    stage_position: role.plan_ready.stage_position,
    initial_budget: role.plan_ready.initial_budget,
  };
}

/**
 * The composition capabilities: exactly the existing public facades and
 * resolvers, nothing else. Every member is read exactly once before the
 * first await.
 */
export interface PipelineV2PlanReadyAutoOps {
  readonly acceptPlanningRunPlan: typeof acceptPipelineV2PlanningRunPlan;
  readonly restoreAcceptedRunPlan: typeof restorePipelineV2AcceptedRunPlan;
  readonly compiledStageFor: typeof compiledPipelineV2RunPlanStageFor;
  readonly resumeAfterHandoff: typeof resumePipelineV2RunAfterPlanningRunPlanHandoff;
}

/**
 * The single frozen production ops object over the existing public
 * facades; every member is fixed at construction and can never be
 * reassigned through this object.
 */
export const productionPlanReadyAutoOps: PipelineV2PlanReadyAutoOps = deepFreezeValue({
  acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
  restoreAcceptedRunPlan: restorePipelineV2AcceptedRunPlan,
  compiledStageFor: compiledPipelineV2RunPlanStageFor,
  resumeAfterHandoff: resumePipelineV2RunAfterPlanningRunPlanHandoff,
}) as unknown as PipelineV2PlanReadyAutoOps;

export interface ApplyPipelineV2PlanReadyContinuationOptions {
  /** The exact deep-frozen snapshot a successful `loadPipelineV2` returned. */
  readonly pipeline: ResolvedPipelineV2;
  /** Canonical orchestrator-owned run root of the same run as the sink. */
  readonly runRoot: string;
  /**
   * The opened state sink the facades share; the production
   * `PipelineV2RunStateSink` satisfies it structurally.
   */
  readonly sink: PipelineV2CoordinatorStateSink;
  /** The agent runtime the coordinator consumes for the resumed execution. */
  readonly runtime: PipelineV2AgentRuntime;
  /** The signal control boundary; acceptance/cutoff stay coordinator-owned. */
  readonly control: PipelineV2CoordinatorControl;
}

/**
 * The defensive extraction of the compiled run plan from one composed
 * facade result: the exact two-key result shape with a record
 * `compiled_plan`. A hostile or malformed result is this layer's own
 * `invalid_result` — never a leaked `TypeError`; the deep provenance and
 * binding verification is delegated to the existing resolver and facades.
 */
function compiledPlanOf(resultValue: unknown, state: PipelineV2RunState): CompiledPipelineV2RunPlan {
  if (!isRecord(resultValue)) {
    throw invalidResult("the composed plan facade result is not a record", state);
  }
  const compiledPlan = (resultValue as Record<string, unknown>)["compiled_plan"];
  if (!isRecord(compiledPlan)) {
    throw invalidResult("the composed plan facade result carries no compiled run plan", state);
  }
  return compiledPlan as unknown as CompiledPipelineV2RunPlan;
}

/**
 * Validate, compose and verify one automatic plan-ready continuation (see
 * the module docstring for the full order, capture boundary and routing
 * semantics).
 */
export async function applyPipelineV2PlanReadyContinuationWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<PipelineV2ResumeCoordinationResult> {
  // Capture boundary: the options shape, every options field and the ops
  // record shape and its members are read exactly once, all before the
  // first await and before any durable intervention. A later mutation of
  // the caller's options or ops cannot change this continuation.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("applyPipelineV2PlanReadyContinuation requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot: unknown = options["runRoot"];
  const sink: unknown = options["sink"];
  const runtime: unknown = options["runtime"];
  const control: unknown = options["control"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("applyPipelineV2PlanReadyContinuation requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const acceptPlanningRunPlan: unknown = ops["acceptPlanningRunPlan"];
  const restoreAcceptedRunPlan: unknown = ops["restoreAcceptedRunPlan"];
  const compiledStageFor: unknown = ops["compiledStageFor"];
  const resumeAfterHandoff: unknown = ops["resumeAfterHandoff"];
  if (
    typeof acceptPlanningRunPlan !== "function" ||
    typeof restoreAcceptedRunPlan !== "function" ||
    typeof compiledStageFor !== "function" ||
    typeof resumeAfterHandoff !== "function"
  ) {
    throw invalidOptions("applyPipelineV2PlanReadyContinuation requires the four composed facade functions");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any scalar validation, sink read or facade call.
  // Its own typed error propagates unchanged.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 plan-ready continuation controller");
  if (!isAbsolute(runRoot as string) || typeof runRoot !== "string" || runRoot === "") {
    throw invalidOptions("applyPipelineV2PlanReadyContinuation requires an absolute runRoot string");
  }
  if (!isRecord(sink) || typeof (sink as Record<string, unknown>)["dispatch"] !== "function") {
    throw invalidOptions("applyPipelineV2PlanReadyContinuation requires a structural state sink");
  }
  const structuralSink = sink as unknown as PipelineV2CoordinatorStateSink;

  // One authoritative snapshot read: the routing bit and the trusted
  // policy are derived from this capture only.
  const snapshot: unknown = structuralSink.snapshot;
  if (!isRecord(snapshot)) {
    throw invalidState("the durable run state is not a record", null);
  }
  const state = snapshot as unknown as PipelineV2RunState;

  // The trusted policy, extracted only from the compiled planning-role
  // metadata of the last durable execution's state. An absent policy or a
  // non-planning boundary is a typed fail-closed error: the runner routes
  // only genuine plan-ready boundaries here.
  const policy = pipelineV2PlanReadyPolicyFor(pipeline, state);
  if (policy === null) {
    throw invalidState(
      "the durable run is not on a trusted plan-ready continuation boundary",
      state,
    );
  }

  // The structural bit routing: only the two plan-ready continuation
  // shapes are admitted, and the bit never validates the state — the
  // matching facade owns the authoritative boundary verification.
  let compiledPlan: CompiledPipelineV2RunPlan;
  if (state.executions.length === state.transitions.length + 1) {
    compiledPlan = compiledPlanOf(
      await (acceptPlanningRunPlan as typeof acceptPipelineV2PlanningRunPlan)({
        pipeline,
        runRoot: runRoot as string,
        sink: structuralSink as never,
      }),
      state,
    );
  } else if (state.executions.length === state.transitions.length) {
    compiledPlan = compiledPlanOf(
      await (restoreAcceptedRunPlan as typeof restorePipelineV2AcceptedRunPlan)({
        pipeline,
        runRoot: runRoot as string,
        state,
      }),
      state,
    );
  } else {
    throw invalidState(
      "the durable run is not on a plan-ready continuation boundary; the transition journals match neither the acceptance nor the committed handoff shape",
      state,
    );
  }

  // The stage taken strictly by the trusted 1-based position of the
  // CURRENT accepted plan revision; an out-of-range position fails closed
  // (the acceptance may already be durable at this point — the confirmed
  // contract boundary).
  const stages: unknown = (compiledPlan as unknown as Record<string, unknown>)["stages"];
  if (!Array.isArray(stages)) {
    throw invalidResult("the compiled run plan carries no stage list", state);
  }
  const selectedStage: unknown = stages[policy.stage_position - 1];
  if (
    !isRecord(selectedStage) ||
    typeof selectedStage["id"] !== "string" ||
    !isPipelineV2SafeId(selectedStage["id"])
  ) {
    throw invalidState(
      `the trusted plan-ready policy names stage position ${policy.stage_position}; the accepted plan declares ${stages.length} stages`,
      state,
    );
  }

  // The stage is confirmed through the single existing provenance-checked
  // resolver; its typed errors pass through by identity, and the confirmed
  // stage must be the exact object of the compiled projection.
  const selectedStageTyped = selectedStage as unknown as CompiledPipelineV2RunPlanStage;
  const confirmedStage = (compiledStageFor as typeof compiledPipelineV2RunPlanStageFor)(
    compiledPlan,
    selectedStageTyped.id,
  );
  if (confirmedStage !== selectedStageTyped) {
    throw invalidResult(
      "the confirmed stage does not match the compiled plan's stage at the trusted position",
      state,
    );
  }

  // The handoff followed by the coordinator's resume, with the confirmed
  // stage id and the trusted initial budget; the exact downstream result
  // is returned by object identity — no new envelope, no reclassification.
  return await (resumeAfterHandoff as typeof resumePipelineV2RunAfterPlanningRunPlanHandoff)({
    pipeline,
    runRoot: runRoot as string,
    sink: structuralSink,
    runtime: runtime as PipelineV2AgentRuntime,
    control: control as PipelineV2CoordinatorControl,
    stageId: confirmedStage.id,
    initialBudget: policy.initial_budget,
  });
}
