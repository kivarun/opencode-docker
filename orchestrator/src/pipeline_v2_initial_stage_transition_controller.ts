import {
  PipelineV2InitialStageTransitionControllerError,
  openPipelineV2InitialStageTransitionInternal,
  type OpenPipelineV2InitialStageTransitionOptions,
  type OpenedPipelineV2InitialStageTransition,
} from "./pipeline_v2_initial_stage_transition_controller_internal.ts";

/**
 * Production-neutral initial stage transition controller.
 *
 * Commits the single planning transition of the initial plan-ready
 * handoff; the C0/C1 reconciliation, the exact durable bindings (no
 * wait journal, no grants, exactly one accepted plan revision matching
 * the compiled plan, exactly the one open generation of the selected
 * stage with iteration 1 open on the cursor anchor, the compiled
 * `completed` edge targeting exactly the selected stage's entry state)
 * and the durability semantics are owned by the internal core over the
 * one shared transition-application kernel. The controller accepts no
 * transition fields from the caller and commits at most one
 * `transition_committed`. Production-reachable transitively through the
 * planning-run-plan handoff (the runner `resumePipelineV2PlanningRunPlan`
 * and CLI `orchestrator resume-plan`); the stage/budget selection policy
 * and the next stage execution stay outside this controller.
 */
export { PipelineV2InitialStageTransitionControllerError } from "./pipeline_v2_initial_stage_transition_controller_internal.ts";
export type {
  OpenedPipelineV2InitialStageTransition,
  OpenPipelineV2InitialStageTransitionOptions,
  PipelineV2InitialStageTransitionControllerFailureReason,
} from "./pipeline_v2_initial_stage_transition_controller_internal.ts";

export function openPipelineV2InitialStageTransition(
  options: OpenPipelineV2InitialStageTransitionOptions,
): Promise<OpenedPipelineV2InitialStageTransition> {
  return openPipelineV2InitialStageTransitionInternal(options);
}
