import {
  applyPipelineV2ReviseTaskResumeWithIo,
  productionReviseTaskResumeOps,
  type ApplyPipelineV2ReviseTaskResumeOptions,
} from "./pipeline_v2_revise_task_resume_controller_internal.ts";
import type { PipelineV2ResumeCoordinationResult } from "./pipeline_v2_coordinator.ts";

/**
 * Production-neutral composition of the full `revise_task` handoff
 * (unwired): the restart-aware revise-task intervention followed by the
 * coordinator's resume entrypoint.
 *
 * The public API composes exactly the two existing authoritative facades —
 * `applyPipelineV2ReviseTaskIntervention` (the plan restoration, the
 * derivation of the compiled stage/pointer/next revision/`revise_task_intent`,
 * the acceptance and the completion) and `resumePipelineV2Run` (the
 * coordinator's production-neutral resume) — into one fixed sequence with
 * a defensive verification of the successful handoff between them. The
 * verification binds the intervention's flat result exactly to the
 * captured caller scalars (`waitIndex`, `taskId`) and to the authoritative
 * durable state, which the real intervention result carries as the exact
 * sink snapshot object (proven by identity). No intent, candidate, digest,
 * stage, plan or budget field is accepted from the caller — every one of
 * them is derived from the durable data by the intervention facade. The
 * coordinator's returned union is verified defensively and returned
 * unchanged by object identity — refusal, worker failure, signal and
 * persistence failure stay coordinator-owned classifications.
 *
 * The controller owns no durable side effect of its own, never opens the
 * sink and never loads the pipeline itself (the caller opens the sink and
 * loads the pipeline exclusively from the durable
 * `state.pipeline.bundle_root`; after a process crash the caller opens a
 * fresh sink and repeats the whole facade), interprets no signal itself
 * (acceptance and cutoff stay coordinator-owned through the captured
 * control functions), and applies no policy.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ReviseTaskResumeControllerError` and
 * `resumePipelineV2RunAfterReviseTaskIntervention`. The full algorithm,
 * capture order and verification contracts are documented in
 * `pipeline_v2_revise_task_resume_controller_internal.ts`.
 *
 * Not implemented (stays unwired): the revise-task intervention selection
 * policy, the runner, the CLI, the default pipeline bundle, automatic
 * resume, schema/reducer changes, migrations/API/T3 and multi-process
 * locking.
 */
export { PipelineV2ReviseTaskResumeControllerError } from "./pipeline_v2_revise_task_resume_controller_internal.ts";
export type {
  ApplyPipelineV2ReviseTaskResumeOptions,
  PipelineV2ReviseTaskResumeControllerFailureReason,
} from "./pipeline_v2_revise_task_resume_controller_internal.ts";

/**
 * Composes one full `revise_task` handoff through the two existing
 * facades (see the module docstring). The returned value is the exact
 * verified `PipelineV2ResumeCoordinationResult` the coordinator returned —
 * no new envelope, same object identity.
 */
export function resumePipelineV2RunAfterReviseTaskIntervention(
  options: ApplyPipelineV2ReviseTaskResumeOptions,
): Promise<PipelineV2ResumeCoordinationResult> {
  return applyPipelineV2ReviseTaskResumeWithIo(productionReviseTaskResumeOps, options);
}
