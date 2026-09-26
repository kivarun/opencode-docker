import {
  PipelineV2ContinueStageGrantControllerError,
  applyPipelineV2ContinueStageGrantInternal,
  type AppliedPipelineV2ContinueStageGrant,
  type ApplyPipelineV2ContinueStageGrantOptions,
  type PipelineV2ContinueStageGrantControllerFailureReason,
  type PipelineV2ContinueStageGrantControllerSink,
} from "./pipeline_v2_continue_stage_grant_controller_internal.ts";

/**
 * Production-neutral application controller for the continue-stage
 * iteration grant (unwired).
 *
 * The public API is the durable step that follows
 * `acceptPipelineV2ContinueStageIntent`: for an already durably accepted
 * provenance-registered prepared `continue_stage_intent` it binds the
 * intent against the open wait, the open generation and its target
 * iteration, records the iteration grant durably and closes the current
 * iteration with the wait-bound grant closure through the structural
 * sink (satisfied by the production `PipelineV2RunStateSink` without an
 * adapter). The controller performs no filesystem work, never
 * republishes the intent and never calls the acceptance controller; it
 * owns no successor rules, no policy and no routing.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ContinueStageGrantControllerError` and
 * `applyPipelineV2ContinueStageGrant`. The full algorithm, capture order,
 * reconciliation, durability semantics and honest boundaries are
 * documented in `pipeline_v2_continue_stage_grant_controller_internal.ts`.
 */
export {
  PipelineV2ContinueStageGrantControllerError,
  type PipelineV2ContinueStageGrantControllerFailureReason,
  type PipelineV2ContinueStageGrantControllerSink,
  type ApplyPipelineV2ContinueStageGrantOptions,
  type AppliedPipelineV2ContinueStageGrant,
};

/**
 * Applies the continue-stage iteration grant for the accepted intent of
 * the open wait through the existing reducer (see the module docstring).
 * The returned result is deep-frozen and content-free: the wait index,
 * the generation and iteration indexes, the additional iteration count,
 * the accepted intent digest and the last authoritative durable state.
 */
export async function applyPipelineV2ContinueStageGrant(
  options: ApplyPipelineV2ContinueStageGrantOptions,
): Promise<AppliedPipelineV2ContinueStageGrant> {
  return await applyPipelineV2ContinueStageGrantInternal(options);
}
