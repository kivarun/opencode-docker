import {
  PipelineV2ReviseTaskClosureControllerError,
  applyPipelineV2ReviseTaskClosureInternal,
  type AppliedPipelineV2ReviseTaskClosure,
  type ApplyPipelineV2ReviseTaskClosureOptions,
  type PipelineV2ReviseTaskClosureControllerFailureReason,
  type PipelineV2ReviseTaskClosureControllerSink,
} from "./pipeline_v2_revise_task_closure_controller_internal.ts";

/**
 * Production-neutral durable closure controller for an already accepted
 * `revise_task_intent` (unwired).
 *
 * The public API is the durable step of the revise flow: for an already
 * durably accepted provenance-registered prepared `revise_task_intent`
 * whose accepted task revision is recorded in the durable ledger, it
 * closes the current stage iteration with the wait-bound `replanned`
 * closure through the structural sink (satisfied by the production
 * `PipelineV2RunStateSink` without an adapter). The controller performs
 * no filesystem work at all — the ledger is the only source of the
 * accepted task revision; it never publishes a manifest, never records a
 * wait response, never creates a task or plan revision, never closes a
 * generation and never opens the next generation or iteration; it owns no
 * successor rules beyond its own single command. An already answered
 * target wait on the active run is recognized only as the exact
 * completed-boundary retry (zero dispatch, no state restoration), which
 * lets a future completion retry succeed after a durable
 * `wait_response_recorded`.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ReviseTaskClosureControllerError` and
 * `applyPipelineV2ReviseTaskClosure`. The full algorithm, capture order,
 * reconciliation, durability semantics and honest boundaries are
 * documented in `pipeline_v2_revise_task_closure_controller_internal.ts`.
 */
export {
  PipelineV2ReviseTaskClosureControllerError,
  type PipelineV2ReviseTaskClosureControllerFailureReason,
  type PipelineV2ReviseTaskClosureControllerSink,
  type ApplyPipelineV2ReviseTaskClosureOptions,
  type AppliedPipelineV2ReviseTaskClosure,
};

/**
 * Applies the durable `replanned` closure of the current stage iteration
 * for the accepted revise intent of the target wait through the existing
 * reducer (see the module docstring). The returned result is deep-frozen
 * and content-free: the wait, generation and iteration indexes, the
 * accepted task identity from the durable ledger, the accepted intent
 * digest and the last authoritative durable state.
 */
export async function applyPipelineV2ReviseTaskClosure(
  options: ApplyPipelineV2ReviseTaskClosureOptions,
): Promise<AppliedPipelineV2ReviseTaskClosure> {
  return await applyPipelineV2ReviseTaskClosureInternal(options);
}
