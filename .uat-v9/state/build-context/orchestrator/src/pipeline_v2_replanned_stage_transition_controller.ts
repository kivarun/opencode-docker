import {
  PipelineV2ReplannedStageTransitionControllerError,
  openPipelineV2ReplannedStageTransitionInternal,
  type OpenPipelineV2ReplannedStageTransitionOptions,
  type OpenedPipelineV2ReplannedStageTransition,
} from "./pipeline_v2_replanned_stage_transition_controller_internal.ts";

/**
 * Production-neutral replanned-stage transition controller (unwired).
 *
 * Commits the single planning transition that follows a successful
 * replanned-stage composition; the C0/C1 reconciliation, the exact
 * durable bindings (the old generation closed `by: "replanned"` on the
 * target wait's anchor with its last iteration bound to the wait, the
 * next generation of the accepted plan open with iteration 1 on the
 * same anchor under the caller-selected stage and budget) and the
 * durability semantics are owned by the internal core. The controller
 * accepts no transition fields from the caller and commits at most one
 * `transition_committed`. No wiring into the coordinator, runner or
 * CLI; the stage/budget selection policy and the next stage execution
 * stay outside this controller.
 */
export { PipelineV2ReplannedStageTransitionControllerError } from "./pipeline_v2_replanned_stage_transition_controller_internal.ts";
export type {
  OpenedPipelineV2ReplannedStageTransition,
  OpenPipelineV2ReplannedStageTransitionOptions,
  PipelineV2ReplannedStageTransitionControllerFailureReason,
} from "./pipeline_v2_replanned_stage_transition_controller_internal.ts";

export function openPipelineV2ReplannedStageTransition(
  options: OpenPipelineV2ReplannedStageTransitionOptions,
): Promise<OpenedPipelineV2ReplannedStageTransition> {
  return openPipelineV2ReplannedStageTransitionInternal(options);
}
