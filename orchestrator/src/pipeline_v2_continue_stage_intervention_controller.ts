import {
  applyPipelineV2ContinueStageInterventionWithIo,
  productionContinueStageInterventionOps,
  type AppliedPipelineV2ContinueStageIntervention,
  type ApplyPipelineV2ContinueStageInterventionOptions,
} from "./pipeline_v2_continue_stage_intervention_controller_internal.ts";

/**
 * Production-neutral restart-aware continue-stage intervention controller
 * for the full `continue_stage` user intervention (unwired).
 *
 * The public API connects the three existing authoritative layers into
 * one intervention that survives a process restart: it accepts the
 * prepared `continue_stage_intent` through the existing
 * `acceptPipelineV2ContinueStageIntent`, verifies the successful
 * acceptance result, restores the provenance-backed compiled run plan
 * through the existing `restorePipelineV2AcceptedRunPlan` from the
 * acceptance result's authoritative durable state (the restore stays the
 * single owner of the compiled plan reconstruction; `compiledPlan` is not
 * a caller field), verifies the restore result, and opens the continued
 * stage through the existing `openPipelineV2ContinuedStage` with the
 * exact restored compiled plan, the original intent, the caller budget
 * and the same sink. Each successful composed result is verified
 * completely before the next layer; one narrow progressed-retry
 * reconciliation recognizes the intent controller's `invalid_state` on an
 * already-progressed durable lifecycle and continues with the restore and
 * the composition, whose own C2–C5 windows and full lifecycle
 * verification stay authoritative. Every unrecognized failure is
 * re-thrown unchanged by object identity.
 *
 * The controller dispatches nothing itself, performs no filesystem work
 * of its own, never calls the reducer and never starts a worker; the
 * three composed layers remain the only owners of durable side effects.
 * The unified result is flat, deep-frozen and not wider than the
 * composition's result.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ContinueStageInterventionControllerError` and
 * `applyPipelineV2ContinueStageIntervention`. The full algorithm, capture
 * order, progressed-retry reconciliation, verification contracts and
 * honest boundaries are documented in
 * `pipeline_v2_continue_stage_intervention_controller_internal.ts`.
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, the revise-task branch (`revise_task_intent`), the
 * graph transition on the opened iteration and the next stage execution,
 * automatic resume, coordinator/runner/CLI/default-pipeline wiring,
 * schema/reducer changes, migrations/API/T3 and multi-process locking.
 */
export { PipelineV2ContinueStageInterventionControllerError } from "./pipeline_v2_continue_stage_intervention_controller_internal.ts";
export type {
  AppliedPipelineV2ContinueStageIntervention,
  ApplyPipelineV2ContinueStageInterventionOptions,
  PipelineV2ContinueStageInterventionControllerFailureReason,
  PipelineV2ContinueStageInterventionControllerSink,
} from "./pipeline_v2_continue_stage_intervention_controller_internal.ts";

/**
 * Applies one full restart-aware `continue_stage` intervention through
 * the three existing facades (see the module docstring). The returned
 * result is deep-frozen and content-free: the wait index, the accepted
 * intent digest, the request/response digests, the additional iteration
 * count, the fixed action id and its routing target, the iteration the
 * grant closed, the iteration the composition opened, the generation
 * index, the exact frozen compiled stage object and the composition's
 * authoritative state.
 */
export function applyPipelineV2ContinueStageIntervention(
  options: ApplyPipelineV2ContinueStageInterventionOptions,
): Promise<AppliedPipelineV2ContinueStageIntervention> {
  return applyPipelineV2ContinueStageInterventionWithIo(productionContinueStageInterventionOps, options);
}
