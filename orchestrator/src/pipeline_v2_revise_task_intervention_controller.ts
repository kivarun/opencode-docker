import {
  applyPipelineV2ReviseTaskInterventionWithIo,
  productionReviseTaskInterventionOps,
  type AppliedPipelineV2ReviseTaskIntervention,
  type ApplyPipelineV2ReviseTaskInterventionOptions,
} from "./pipeline_v2_revise_task_intervention_controller_internal.ts";

/**
 * Production-neutral restart-aware composition controller for the first
 * half of the `revise_task` user intervention
 * (production-reachable transitively through the revise-task resume
 * handoff).
 *
 * The public API owns the minimal user-policy contract
 * `{runId, waitIndex, taskId, taskBody}` plus the runtime resources
 * (`pipeline`, `runRoot`, `sink`) — it never opens the state sink and
 * never loads the pipeline itself; a future runner opens the sink and
 * loads the pipeline exclusively from the durable
 * `state.pipeline.bundle_root`. The controller accepts only the
 * provenance-backed `ResolvedPipelineV2` and composes the existing
 * authoritative layers into one intervention that survives a process
 * restart: the read-only restoration of the last durably accepted plan
 * (`restorePipelineV2AcceptedRunPlan`) on the single captured
 * authoritative state, the derivation of the open generation's compiled
 * stage, the task pointer, the next task revision and the exact
 * `revise_task_intent` (the caller passes none of them), then the
 * existing `acceptPipelineV2ReviseTaskIntent` and
 * `completePipelineV2ReviseTask` with the fixed retry classification —
 * the acceptance owns the R0/R1/R2 windows and its conflicts; the exact
 * progressed `replanned`-closure and answered-response windows skip the
 * acceptance and run only the completion. Every successful composed
 * result is verified completely before the next layer; an acceptance
 * typed `invalid_state` continues only after the same full
 * exact-progressed verification of its authoritative state; every other
 * failure is re-thrown unchanged by object identity.
 *
 * The controller dispatches nothing itself, performs no filesystem work
 * of its own, never calls the reducer and owns no store traversal,
 * parser, compiler, digest builder, publisher or response path. The
 * unified result is flat, deep-frozen and content-free: the wait index,
 * the accepted intent digest, the request/response digests, the task
 * identity, the generation and iteration indexes, the fixed action id and
 * its routing target, and the completion's authoritative state — the task
 * body, prepared manifests, canonical JSON, the pipeline and the compiled
 * plan, paths and caller objects never enter it.
 *
 * The controller ends its work at the active/running planning boundary;
 * the architect execution, the next plan revision and the replanned
 * generation/stage/transition opening and resume are the revise-task
 * resume handoff's.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ReviseTaskInterventionControllerError` and
 * `applyPipelineV2ReviseTaskIntervention`. The full algorithm, capture
 * order, retry classification, verification contracts and honest
 * boundaries are documented in
 * `pipeline_v2_revise_task_intervention_controller_internal.ts`.
 */
export { PipelineV2ReviseTaskInterventionControllerError } from "./pipeline_v2_revise_task_intervention_controller_internal.ts";
export type {
  AppliedPipelineV2ReviseTaskIntervention,
  ApplyPipelineV2ReviseTaskInterventionOptions,
  PipelineV2ReviseTaskInterventionControllerFailureReason,
  PipelineV2ReviseTaskInterventionControllerSink,
} from "./pipeline_v2_revise_task_intervention_controller_internal.ts";

/**
 * Applies one full restart-aware `revise_task` intervention through the
 * existing facades (see the module docstring). The returned result is
 * deep-frozen and content-free: the wait index, the accepted intent
 * digest, the request/response digests, the task identity, the generation
 * and iteration indexes, the fixed action id and its routing target, and
 * the completion's authoritative durable state.
 */
export function applyPipelineV2ReviseTaskIntervention(
  options: ApplyPipelineV2ReviseTaskInterventionOptions,
): Promise<AppliedPipelineV2ReviseTaskIntervention> {
  return applyPipelineV2ReviseTaskInterventionWithIo(
    productionReviseTaskInterventionOps,
    options,
  );
}
