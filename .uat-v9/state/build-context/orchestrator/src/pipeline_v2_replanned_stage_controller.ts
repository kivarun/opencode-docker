import {
  PipelineV2ReplannedStageControllerError,
  openPipelineV2ReplannedStageWithOps,
  productionReplannedStageOps,
  type OpenedPipelineV2ReplannedStage,
  type OpenPipelineV2ReplannedStageOptions,
} from "./pipeline_v2_replanned_stage_controller_internal.ts";

/**
 * Production-neutral replanned-stage composition controller (unwired).
 *
 * After a NEW plan revision has already been durably accepted, this
 * controller composes the two existing authoritative controllers in one
 * fixed order to move the run onto the caller-selected stage of that
 * plan: `closePipelineV2ReplannedGeneration` closes the old generation
 * `by: "replanned"` (C0 dispatch, C1/C2 zero-dispatch retries), and
 * `ensurePipelineV2StageIteration` guarantees the open generation bound
 * to the selected compiled stage plus its open iteration 1. The
 * composition never runs a reducer, never builds or accepts a plan
 * candidate, never publishes any manifest, never performs filesystem
 * work, never commits a graph transition, and never resumes the run.
 *
 * `stageId` and `initialBudget` are explicit caller-policy decisions;
 * the controller selects nothing itself. `compiledPlan` must be the
 * exact provenance-backed compiled plan the existing run-plan
 * acceptance returned; `intent` must be the exact provenance-backed
 * prepared `revise_task_intent` that already carries the accepted task
 * revision.
 *
 * Capture and pre-side-effect validation: the options shape; then
 * `sink` → `intent` → `compiledPlan` → `stageId` → `initialBudget`
 * each read exactly once; then both composed ops captured exactly once
 * before the first await; `initialBudget` must be a positive safe
 * integer and the stage id must resolve through the existing trusted
 * compiled resolver `compiledPipelineV2RunPlanStageFor` (its
 * provenance gate, stage existence and template binding belong to that
 * resolver alone; compiled-resolver errors pass by identity) — any
 * invalid shape, forged or cloned compiled plan or invalid budget
 * refuses before any durable dispatch. The controller never reads or
 * binds `sink.dispatch` itself.
 *
 * Composition ordering: `closeGeneration` runs first; its returned
 * result is verified COMPLETELY before `ensureStageIteration` is
 * called (a hostile, malformed or binding-mismatched close result
 * never reaches the ensure call); only then the ensure result is
 * verified against the trusted compiled stage, the verified close
 * result and the intent/plan bindings; the result is built from the
 * ensure controller's authoritative state without any additional
 * `sink.snapshot` read. Close-result verification covers the positive
 * safe indexes, the exact intent digest and plan revision/digest/origin
 * bindings, the old generation at its exact durable index with the
 * exact replanned iteration and generation closures, the wait, task
 * and plan ledger bindings, and the admissible state form (C1: the old
 * generation is the last durable generation; C2: exactly one
 * still-open current-plan generation follows it in an immediate
 * opening form). Ensure-result verification covers the exact trusted
 * `compiled_stage` object (identity), the generation/iteration
 * continuation, the exact stage bindings and opening anchor, the
 * unchanged old generation and historical iteration prefix, the
 * non-advancing wait/task/plan/execution/transition boundaries, and
 * the active running state on the settled-but-unbound planning
 * execution; hostile coherent results are compared against the
 * verified close state and the trusted compiled stage, never against
 * themselves. Malformed nested results fail as the controller's own
 * `invalid_result`, never a `TypeError`.
 *
 * Durability: the composed controllers' typed failures pass through
 * unchanged by identity; no rollback, no automatic retry, no second
 * dispatch. The composition's own failure reasons are exactly
 * `invalid_options` and `invalid_result`; unexpected errors propagate
 * with their class and identity.
 *
 * The full revise-cycle order this controller completes:
 * accepted revised task → replanned iteration closure → revise_task
 * response → settled planning execution → accepted next plan revision
 * → old generation closed by replanned → selected new-plan generation
 * opened → iteration 1 opened → future transition commit.
 *
 * Runtime export surface is exactly `PipelineV2ReplannedStageControllerError`
 * and `openPipelineV2ReplannedStage` (types are not runtime keys).
 *
 * Not implemented (stays unwired): the architect output parsing, plan
 * candidate construction and acceptance, the stage/budget selection
 * policy, the graph transition commit, automatic resume,
 * coordinator/runner/CLI/default-pipeline wiring, schema/reducer
 * changes, migrations/API/T3 and multi-process locking.
 */
export { PipelineV2ReplannedStageControllerError } from "./pipeline_v2_replanned_stage_controller_internal.ts";
export type {
  OpenedPipelineV2ReplannedStage,
  OpenPipelineV2ReplannedStageOptions,
  PipelineV2ReplannedStageControllerFailureReason,
} from "./pipeline_v2_replanned_stage_controller_internal.ts";

export function openPipelineV2ReplannedStage(
  options: OpenPipelineV2ReplannedStageOptions,
): Promise<OpenedPipelineV2ReplannedStage> {
  return openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, options);
}
