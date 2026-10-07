/**
 * Public facade of the trusted automatic plan-ready continuation
 * controller for pipeline schema v2.
 *
 * `applyPipelineV2PlanReadyContinuation({pipeline, runRoot, sink, runtime,
 * control})` continues a durable pipeline v2 run from its controlled
 * `planReady` suspension (or its committed restart form) automatically,
 * using only the existing authoritative facades: the routing between the
 * planning-run-plan acceptance and the read-only accepted-plan restoration
 * is decided by one structural bit of the authoritative snapshot, the
 * continued stage is taken strictly from the trusted `plan_ready` policy
 * of the compiled planning-role metadata (1-based position of the CURRENT
 * accepted plan revision) and confirmed through the single
 * provenance-checked stage resolver, and the exact downstream coordination
 * result of `resumePipelineV2RunAfterPlanningRunPlanHandoff` is returned
 * by object identity — no new envelope, no reclassification.
 *
 * The caller passes only the production objects; no proposal body, no
 * accepted-output bytes, no plan digest, no wait index and no journal
 * index is ever accepted. There is no catch-based boundary classification:
 * downstream typed errors and unexpected errors propagate unchanged by
 * object identity.
 *
 * Runtime export surface is exactly `PipelineV2PlanReadyAutoControllerError`,
 * `applyPipelineV2PlanReadyContinuation` and the pure routing/policy
 * helper `pipelineV2PlanReadyPolicyFor` (types are not runtime keys). The
 * facade always runs the single frozen production ops over the existing
 * public facades; fault injection goes through the internal core's
 * per-call ops, so an injected call cannot influence a parallel production
 * call and there is no mutable module-global seam.
 */
import {
  applyPipelineV2PlanReadyContinuationWithIo,
  PipelineV2PlanReadyAutoControllerError,
  productionPlanReadyAutoOps,
  type ApplyPipelineV2PlanReadyContinuationOptions,
} from "./pipeline_v2_plan_ready_auto_controller_internal.ts";
import type { PipelineV2ResumeCoordinationResult } from "./pipeline_v2_coordinator.ts";

export {
  PipelineV2PlanReadyAutoControllerError,
  pipelineV2PlanReadyPolicyFor,
  type PipelineV2PlanReadyAutoControllerFailureReason,
  type PipelineV2PlanReadyAutoPolicy,
} from "./pipeline_v2_plan_ready_auto_controller_internal.ts";

/**
 * Composes one automatic plan-ready continuation for the existing durable
 * run and returns the exact verified `PipelineV2ResumeCoordinationResult`
 * the coordinator returned — no new envelope, same object identity.
 */
export function applyPipelineV2PlanReadyContinuation(
  options: ApplyPipelineV2PlanReadyContinuationOptions,
): Promise<PipelineV2ResumeCoordinationResult> {
  return applyPipelineV2PlanReadyContinuationWithIo(productionPlanReadyAutoOps, options);
}
