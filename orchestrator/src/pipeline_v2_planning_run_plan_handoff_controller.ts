/**
 * Public facade of the restart-aware planning-run-plan handoff controller
 * for pipeline schema v2 (production-neutral, production-reachable
 * transitively through the runner `resumePipelineV2PlanningRunPlan` and
 * CLI `orchestrator resume-plan`).
 *
 * `applyPipelineV2PlanningRunPlanHandoff({pipeline, runRoot, sink,
 * stageId, initialBudget})` composes the existing authoritative chain into
 * one fixed sequence that moves an existing durable run from its planning
 * boundary onto the caller-selected stage of the newly accepted plan and
 * commits the planning transition:
 *
 * - Initial Branch A (the initial plan-ready boundary of a fresh run:
 *   the settled-but-unbound first planning execution with no wait
 *   journal and no grants): the planning-output composition accepts the
 *   run plan, the selected stage's generation and iteration 1 are opened,
 *   and the initial stage transition controller commits the planning
 *   transition.
 * - Initial Branch B (the exact committed initial boundary, the crash
 *   seam after the initial transition became durable while the result
 *   was lost): the run plan is restored read-only, the ensure recognizes
 *   the open generation/iteration with zero dispatch and the initial
 *   transition controller recognizes the exact C1 zero-dispatch boundary.
 * - Branch A (the settled-but-unbound planning acceptance boundary after
 *   a revise cycle): the planning-output composition accepts the run
 *   plan, the accepted `revise_task` intent is restored from the durable
 *   run through the public loader, the replanned stage opens and the
 *   planning transition is committed — the S0–S4 restart windows converge
 *   through the idempotency of the composed controllers alone.
 * - Branch B (the exact committed handoff boundary, the crash seam after
 *   the planning transition became durable while the result was lost):
 *   the accepted run plan is restored read-only and the transition
 *   controller alone recognizes the exact C1 zero-dispatch boundary.
 *
 * Caller policy is exactly `stageId` and `initialBudget`, captured once
 * before the first await and passed unchanged to every downstream call —
 * the durable state does not pin the caller budget until the new
 * generation exists, so only the caller replay determines it. The branch
 * is selected once from the captured authoritative snapshot, never from a
 * caught downstream error, and every composed layer's typed error passes
 * through unchanged by object identity. The exact downstream transition
 * result is returned by identity (no new envelope).
 *
 * The controller performs no filesystem work of its own, no reducer or
 * store calls, and never touches the coordinator/runner/CLI.
 *
 * Runtime export surface is exactly `PipelineV2PlanningRunPlanHandoffControllerError`
 * and `applyPipelineV2PlanningRunPlanHandoff`; types are erased. The
 * facade always runs the single frozen production ops over the existing
 * public facades and resolvers; fault injection goes through the internal
 * core's per-call ops, so an injected call cannot influence a parallel
 * production call and there is no mutable module-global seam.
 */
import {
  PipelineV2PlanningRunPlanHandoffControllerError,
  applyPipelineV2PlanningRunPlanHandoffWithIo,
  productionPlanningRunPlanHandoffOps,
  type ApplyPipelineV2PlanningRunPlanHandoffOptions,
  type AppliedPipelineV2PlanningRunPlanHandoff,
} from "./pipeline_v2_planning_run_plan_handoff_controller_internal.ts";

export {
  PipelineV2PlanningRunPlanHandoffControllerError,
  type PipelineV2PlanningRunPlanHandoffFailureReason,
} from "./pipeline_v2_planning_run_plan_handoff_controller_internal.ts";

/**
 * Composes the restart-aware planning-run-plan handoff for the existing
 * durable run at its planning boundary and returns the exact downstream
 * transition result by identity.
 */
export async function applyPipelineV2PlanningRunPlanHandoff(
  options: ApplyPipelineV2PlanningRunPlanHandoffOptions,
): Promise<AppliedPipelineV2PlanningRunPlanHandoff> {
  return await applyPipelineV2PlanningRunPlanHandoffWithIo(
    options,
    productionPlanningRunPlanHandoffOps,
  );
}
