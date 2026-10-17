import {
  applyPipelineV2PlanningRunPlanResumeWithIo,
  productionPlanningRunPlanResumeOps,
  type ApplyPipelineV2PlanningRunPlanResumeOptions,
} from "./pipeline_v2_planning_run_plan_resume_controller_internal.ts";
import type { PipelineV2ResumeCoordinationResult } from "./pipeline_v2_coordinator.ts";

/**
 * Production-neutral composition of the completed planning-run-plan handoff
 * followed by the coordinator's resume entrypoint (unwired).
 *
 * The public API composes exactly the two existing authoritative facades —
 * `applyPipelineV2PlanningRunPlanHandoff` (the plan acceptance, the
 * replanned stage opening and the committed planning transition) and
 * `resumePipelineV2Run` (the coordinator's production-neutral resume) —
 * into one fixed sequence with a defensive verification of the successful
 * handoff between them. The verification binds the handoff's flat result
 * exactly to the captured caller policy (`stageId`, `initialBudget`) and
 * to the authoritative durable state, which `result.state` must match
 * structurally (identity not required) against the post-handoff sink
 * snapshot read exactly once. The run id the resume consumes is derived
 * exclusively from the verified authoritative state — no proposal, compiled
 * plan/stage, wait intent, plan digest/revision, generation/iteration/
 * transition index or entry state is accepted from the caller. The
 * coordinator's returned union is verified defensively and returned
 * unchanged by object identity — refusal, worker failure, signal and
 * persistence failure stay coordinator-owned classifications.
 *
 * The controller owns no durable side effect of its own, never opens the
 * sink and never loads the pipeline itself (the caller opens the sink and
 * loads the pipeline exclusively from the durable
 * `state.pipeline.bundle_root`; after a process crash the caller opens a
 * fresh sink and repeats the whole facade), interprets no signal itself
 * (acceptance and cutoff stay coordinator-owned through the captured
 * control functions), and applies no stage/budget policy of its own.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2PlanningRunPlanResumeControllerError` and
 * `resumePipelineV2RunAfterPlanningRunPlanHandoff`. The full algorithm,
 * capture order, crash windows and verification contracts are documented
 * in `pipeline_v2_planning_run_plan_resume_controller_internal.ts`.
 *
 * Not implemented (stays unwired): the stage/budget selection policy, the
 * runner, the CLI, the default pipeline bundle, automatic resume,
 * schema/reducer changes, migrations/API/T3 and multi-process locking.
 */
export { PipelineV2PlanningRunPlanResumeControllerError } from "./pipeline_v2_planning_run_plan_resume_controller_internal.ts";
export type {
  ApplyPipelineV2PlanningRunPlanResumeOptions,
  PipelineV2PlanningRunPlanResumeControllerFailureReason,
} from "./pipeline_v2_planning_run_plan_resume_controller_internal.ts";

/**
 * Composes one full planning-run-plan handoff through the two existing
 * facades (see the module docstring). The returned value is the exact
 * verified `PipelineV2ResumeCoordinationResult` the coordinator returned —
 * no new envelope, same object identity.
 */
export function resumePipelineV2RunAfterPlanningRunPlanHandoff(
  options: ApplyPipelineV2PlanningRunPlanResumeOptions,
): Promise<PipelineV2ResumeCoordinationResult> {
  return applyPipelineV2PlanningRunPlanResumeWithIo(productionPlanningRunPlanResumeOps, options);
}
