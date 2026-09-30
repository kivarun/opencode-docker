import {
  applyPipelineV2ContinueStageResumeWithIo,
  productionContinueStageResumeOps,
  type ApplyPipelineV2ContinueStageResumeOptions,
} from "./pipeline_v2_continue_stage_resume_controller_internal.ts";
import type { PipelineV2ResumeCoordinationResult } from "./pipeline_v2_coordinator.ts";

/**
 * Production-neutral composition of the full `continue_stage` handoff
 * (unwired): the restart-aware continue-stage intervention followed by the
 * coordinator's resume entrypoint.
 *
 * The public API composes exactly the two existing authoritative facades —
 * `applyPipelineV2ContinueStageIntervention` (the intent acceptance, the
 * authoritative run-plan restoration and the continued-stage composition)
 * and `resumePipelineV2Run` (the coordinator's production-neutral resume)
 * — into one fixed sequence with a defensive verification of the
 * successful handoff between them. The verification binds the
 * intervention's flat result exactly to the captured provenance-backed
 * intent and caller budget and to the authoritative durable state, which
 * the real intervention result carries as the exact sink snapshot object
 * (proven by identity). The coordinator's returned union is verified
 * defensively and returned unchanged by object identity — refusal, worker
 * failure, signal and persistence failure stay coordinator-owned
 * classifications.
 *
 * The controller owns no durable side effect of its own, interprets no
 * signal itself (acceptance and cutoff stay coordinator-owned through the
 * captured control functions), and applies no policy: the run id is
 * derived exclusively from the verified intent, the caller budget is
 * passed through, and `compiledPlan` is not a caller field (only the
 * restore obtains it).
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ContinueStageResumeControllerError` and
 * `resumePipelineV2RunAfterContinueStageIntervention`. The full algorithm,
 * capture order, retry windows and verification contracts are documented
 * in `pipeline_v2_continue_stage_resume_controller_internal.ts`.
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, the revise-task branch (`revise_task_intent`), the
 * runner, the CLI, the default pipeline bundle, automatic resume,
 * schema/reducer changes, migrations/API/T3 and multi-process locking.
 */
export { PipelineV2ContinueStageResumeControllerError } from "./pipeline_v2_continue_stage_resume_controller_internal.ts";
export type {
  ApplyPipelineV2ContinueStageResumeOptions,
  PipelineV2ContinueStageResumeControllerFailureReason,
} from "./pipeline_v2_continue_stage_resume_controller_internal.ts";

/**
 * Composes one full `continue_stage` handoff through the two existing
 * facades (see the module docstring). The returned value is the exact
 * verified `PipelineV2ResumeCoordinationResult` the coordinator returned —
 * no new envelope, same object identity.
 */
export function resumePipelineV2RunAfterContinueStageIntervention(
  options: ApplyPipelineV2ContinueStageResumeOptions,
): Promise<PipelineV2ResumeCoordinationResult> {
  return applyPipelineV2ContinueStageResumeWithIo(productionContinueStageResumeOps, options);
}
