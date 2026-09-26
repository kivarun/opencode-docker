import {
  PipelineV2ContinueStageCompletionControllerError,
  completePipelineV2ContinueStageWithIo,
  productionContinueStageCompletionOps,
  type CompletedPipelineV2ContinueStage,
  type CompletePipelineV2ContinueStageOptions,
  type PipelineV2ContinueStageCompletionControllerFailureReason,
  type PipelineV2ContinueStageCompletionControllerSink,
} from "./pipeline_v2_continue_stage_completion_controller_internal.ts";

/**
 * Production-neutral completion controller for the continue-stage
 * intervention (unwired).
 *
 * The public API composes the two existing authoritative steps in the
 * fixed order — `applyPipelineV2ContinueStageGrant` (the grant and the
 * grant-bound iteration closure) and then the existing generic wait
 * controller's structured `continue_stage` action path
 * (`recordPipelineV2WaitAction`, which synthesizes, publishes and durably
 * records the user response through the existing wait-manifest
 * substrate). The controller performs no filesystem work, never calls the
 * reducer or a store, never calls the intent acceptance controller, does
 * not open the next iteration and does not resume the run; no new
 * response manifest, serializer, digest builder, store protocol or
 * response dispatcher is introduced.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ContinueStageCompletionControllerError` and
 * `completePipelineV2ContinueStage`. The full algorithm, capture order,
 * verification contracts, retry windows and honest boundaries are
 * documented in
 * `pipeline_v2_continue_stage_completion_controller_internal.ts`.
 */
export {
  PipelineV2ContinueStageCompletionControllerError,
  type PipelineV2ContinueStageCompletionControllerFailureReason,
  type PipelineV2ContinueStageCompletionControllerSink,
  type CompletePipelineV2ContinueStageOptions,
  type CompletedPipelineV2ContinueStage,
};

/**
 * Completes the continue-stage intervention for the accepted intent of
 * the durable run (see the module docstring). The returned result is
 * deep-frozen and content-free: the wait/generation/iteration indexes,
 * the additional iteration count, the accepted intent digest, the
 * response request/response digests, the fixed action id and its declared
 * routing target, and the response controller's authoritative state.
 */
export async function completePipelineV2ContinueStage(
  options: CompletePipelineV2ContinueStageOptions,
): Promise<CompletedPipelineV2ContinueStage> {
  return await completePipelineV2ContinueStageWithIo(productionContinueStageCompletionOps, options);
}
