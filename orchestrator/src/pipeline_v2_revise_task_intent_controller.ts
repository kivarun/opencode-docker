import {
  PipelineV2ReviseTaskIntentControllerError,
  acceptPipelineV2ReviseTaskIntentWithIo,
  productionReviseTaskIntentOps,
  type AcceptPipelineV2ReviseTaskIntentOptions,
  type AcceptedPipelineV2ReviseTaskIntent,
  type PipelineV2ReviseTaskIntentControllerFailureReason,
  type PipelineV2ReviseTaskIntentControllerSink,
} from "./pipeline_v2_revise_task_intent_controller_internal.ts";

/**
 * Production-neutral acceptance controller for `revise_task_intent` wait
 * intents (unwired).
 *
 * The public API accepts one provenance-registered prepared
 * `revise_task_intent` together with one provenance-registered prepared
 * candidate task revision, publishes the immutable wait-intent and
 * task-revision manifests through the existing run-plan filesystem store
 * and durably records the exact sequence `plan_intent_accepted` →
 * `task_revision_accepted` through the structural sink. The increment
 * ends at the accepted task revision: the controller never closes the
 * iteration, never records a wait response, never creates a plan revision
 * and never resumes the run.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ReviseTaskIntentControllerError` and
 * `acceptPipelineV2ReviseTaskIntent`. The full algorithm, capture order,
 * binding chain, reconciliation and durability semantics are documented
 * in `pipeline_v2_revise_task_intent_controller_internal.ts`.
 */
export {
  PipelineV2ReviseTaskIntentControllerError,
  type PipelineV2ReviseTaskIntentControllerFailureReason,
  type PipelineV2ReviseTaskIntentControllerSink,
  type AcceptPipelineV2ReviseTaskIntentOptions,
  type AcceptedPipelineV2ReviseTaskIntent,
};

/**
 * Accepts the revise-task intent and its candidate task revision for the
 * durable run (see the module docstring). The returned result is
 * deep-frozen and content-free: the wait index, the accepted intent
 * digest, the task identity (safe id, revision, digest) and the response
 * controller's authoritative state.
 */
export async function acceptPipelineV2ReviseTaskIntent(
  options: AcceptPipelineV2ReviseTaskIntentOptions,
): Promise<AcceptedPipelineV2ReviseTaskIntent> {
  return await acceptPipelineV2ReviseTaskIntentWithIo(productionReviseTaskIntentOps, options);
}
