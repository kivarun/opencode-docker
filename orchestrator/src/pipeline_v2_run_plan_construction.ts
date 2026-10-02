/**
 * The public facade of the read-only proposal → candidate construction
 * layer for pipeline schema v2 (production-neutral, unwired).
 *
 * `constructPipelineV2RunPlanCandidateFromProposal({runRoot, state,
 * proposal})` builds one provenance-backed `PreparedPipelineV2RunPlanCandidate`
 * from an agent-authored run plan proposal and the durable run state,
 * using only the existing manifest, store and candidate chains. The caller
 * passes no pipeline, no sink, no compiled plan, no digests, no revisions,
 * no execution index and no prepared manifests — every durable field is
 * derived from the validated state and the proposal by the internal core.
 *
 * The layer is strictly read-only: it publishes nothing, dispatches
 * nothing, and never compiles or accepts the candidate; the future
 * planning-transition hook compiles it through the existing compiled layer
 * and the acceptance controller accepts it. Loaded prepared plan and task
 * objects are opaque; their provenance and every identity and chain
 * correspondence is verified by the existing candidate preparation.
 *
 * Runtime export surface is exactly `PipelineV2RunPlanConstructionError`
 * and `constructPipelineV2RunPlanCandidateFromProposal`; types are erased.
 * The facade always runs the single frozen production ops over the
 * existing store loaders; fault injection goes through the internal core's
 * per-call ops, so an injected call cannot influence a parallel production
 * call and there is no mutable module-global seam.
 */
import {
  PipelineV2RunPlanConstructionError,
  constructPipelineV2RunPlanCandidateFromProposalInternal,
  productionRunPlanConstructionOps,
  type ConstructPipelineV2RunPlanCandidateOptions,
} from "./pipeline_v2_run_plan_construction_internal.ts";
import type { PreparedPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_candidate.ts";

export {
  PipelineV2RunPlanConstructionError,
  type PipelineV2RunPlanConstructionFailureReason,
} from "./pipeline_v2_run_plan_construction_internal.ts";

/**
 * Constructs one run plan candidate from the prepared proposal and the
 * durable run state: the exact provenance-backed
 * `PreparedPipelineV2RunPlanCandidate` of the existing candidate
 * preparation, returned directly (never copied or wrapped).
 */
export async function constructPipelineV2RunPlanCandidateFromProposal(
  options: ConstructPipelineV2RunPlanCandidateOptions,
): Promise<PreparedPipelineV2RunPlanCandidate> {
  return await constructPipelineV2RunPlanCandidateFromProposalInternal(
    options,
    productionRunPlanConstructionOps,
  );
}
