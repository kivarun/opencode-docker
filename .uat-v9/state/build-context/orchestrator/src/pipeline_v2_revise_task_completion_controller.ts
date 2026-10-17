import {
  PipelineV2ReviseTaskCompletionControllerError,
  completePipelineV2ReviseTaskWithIo,
  productionReviseTaskCompletionOps,
  type CompletedPipelineV2ReviseTask,
  type CompletePipelineV2ReviseTaskOptions,
  type PipelineV2ReviseTaskCompletionControllerFailureReason,
  type PipelineV2ReviseTaskCompletionControllerSink,
} from "./pipeline_v2_revise_task_completion_controller_internal.ts";

/**
 * Production-neutral completion controller for the revise-task
 * intervention (production-reachable transitively through the
 * revise-task intervention controller).
 *
 * The public API composes the two existing authoritative steps in the
 * fixed order — `applyPipelineV2ReviseTaskClosure` (the wait-bound
 * `replanned` iteration closure of the accepted revise intent, with its
 * own provenance gate, bindings, reconciliation and durability mapping)
 * and then the existing generic wait controller's structured `revise_task`
 * action path (`recordPipelineV2WaitAction`, which synthesizes, publishes
 * and durably records the user response through the existing
 * wait-manifest substrate). Both results are fully verified by the
 * completion before it proceeds — the closure result against the accepted
 * intent, the response result against the verified pre-response state.
 * The controller performs no filesystem work, never calls the reducer or
 * a store, never calls the intent acceptance controller, never loads a
 * task or plan manifest, never creates a plan revision, never closes the
 * generation, never opens the next generation or iteration, never runs
 * the architect/replanning execution and never resumes the run; no new
 * response manifest, serializer, digest builder, store protocol or
 * response dispatcher is introduced.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ReviseTaskCompletionControllerError` and
 * `completePipelineV2ReviseTask`. The full algorithm, capture order,
 * verification contracts, retry windows and honest boundaries are
 * documented in
 * `pipeline_v2_revise_task_completion_controller_internal.ts`.
 */
export {
  PipelineV2ReviseTaskCompletionControllerError,
  type PipelineV2ReviseTaskCompletionControllerFailureReason,
  type PipelineV2ReviseTaskCompletionControllerSink,
  type CompletePipelineV2ReviseTaskOptions,
  type CompletedPipelineV2ReviseTask,
};

/**
 * Completes the revise-task intervention for the accepted intent of the
 * durable run (see the module docstring). The returned result is
 * deep-frozen and content-free: the wait/generation/iteration indexes,
 * the accepted task identity from the durable ledger, the accepted intent
 * digest, the response request/response digests, the fixed action id and
 * its declared routing target, and the response controller's
 * authoritative state.
 */
export async function completePipelineV2ReviseTask(
  options: CompletePipelineV2ReviseTaskOptions,
): Promise<CompletedPipelineV2ReviseTask> {
  return await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, options);
}
