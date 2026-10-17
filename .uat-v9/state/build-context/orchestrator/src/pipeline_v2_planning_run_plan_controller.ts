/**
 * Public facade of the restart-aware planning-output → accepted-run-plan
 * composition controller for pipeline schema v2 (production-neutral,
 * unwired).
 *
 * `acceptPipelineV2PlanningRunPlan({pipeline, runRoot, sink})` composes
 * the existing authoritative chain into one fixed sequence over the
 * settled-but-unbound planning acceptance boundary of an existing durable
 * run: the read-only planning-acceptance context restoration, the exact
 * planning execution and its compiled role, the single proposal read from
 * the restored accepted output, the anchored retry-aware construction,
 * the existing acceptance verifier as the provenance/binding gate, and
 * the existing crash-safe acceptance — every downstream result verified
 * defensively between the calls, and the exact downstream result returned
 * by identity (no new envelope).
 *
 * The caller passes no state, no execution index, no state id, no output
 * id, no proposal, no manifests, no candidate, no revision and no digest;
 * everything is derived from the durable snapshot and the trusted
 * pipeline. The controller performs no filesystem work of its own and
 * never touches the coordinator/runner/CLI.
 *
 * Runtime export surface is exactly `PipelineV2PlanningRunPlanControllerError`
 * and `acceptPipelineV2PlanningRunPlan`; types are erased. The facade
 * always runs the single frozen production ops over the existing public
 * facades and resolvers; fault injection goes through the internal core's
 * per-call ops, so an injected call cannot influence a parallel
 * production call and there is no mutable module-global seam.
 */
import {
  PipelineV2PlanningRunPlanControllerError,
  acceptPipelineV2PlanningRunPlanInternal,
  productionPlanningRunPlanOps,
  type AcceptPipelineV2PlanningRunPlanOptions,
} from "./pipeline_v2_planning_run_plan_controller_internal.ts";
import type { AcceptedPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_controller_internal.ts";

export {
  PipelineV2PlanningRunPlanControllerError,
  type PipelineV2PlanningRunPlanFailureReason,
} from "./pipeline_v2_planning_run_plan_controller_internal.ts";

/**
 * Composes the planning-output → accepted-run-plan chain for the existing
 * durable run at its planning acceptance boundary and returns the exact
 * downstream acceptance result (`{compiled_plan, state}` by identity).
 */
export async function acceptPipelineV2PlanningRunPlan(
  options: AcceptPipelineV2PlanningRunPlanOptions,
): Promise<AcceptedPipelineV2RunPlanCandidate> {
  return await acceptPipelineV2PlanningRunPlanInternal(
    options,
    productionPlanningRunPlanOps,
  );
}
