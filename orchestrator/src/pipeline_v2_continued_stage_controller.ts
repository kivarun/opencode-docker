import {
  openPipelineV2ContinuedStageWithIo,
  productionContinuedStageOps,
  type OpenedPipelineV2ContinuedStage,
  type OpenPipelineV2ContinuedStageOptions,
} from "./pipeline_v2_continued_stage_controller_internal.ts";

/**
 * Production-neutral continued-stage composition controller for the
 * continue-stage intervention (unwired).
 *
 * The public API closes exactly one gap of the durable continue-stage
 * flow: it runs `completePipelineV2ContinueStage` (the grant, the
 * grant-bound iteration closure and the `continue_stage` response),
 * verifies its successful result completely against the caller policy
 * and the accepted intent, then opens the NEXT iteration of the same
 * generation through the existing `ensurePipelineV2StageIteration`,
 * verifies that result, and returns one unified flat result. The
 * controller dispatches nothing itself, performs no filesystem work of
 * its own, never computes the effective iteration budget (the reducer
 * stays its source of truth), never commits a graph transition, never
 * starts the next stage execution and never resumes the run; the two
 * composed controllers remain the only owners of durable side effects.
 *
 * Caller policy: `stageId` is not a caller field — the stage is resolved
 * only through the single trusted compiled resolver
 * `compiledPipelineV2RunPlanStageFor(compiledPlan, intent.stage_id)`
 * before the first side effect; `initialBudget` is the one caller-owned
 * policy scalar; the resolved stage, its declaration position, the plan
 * digest and the intent's binding scalars are fixed at capture, so no
 * later mutation of the caller's options or objects can reinterpret the
 * policy checks. `compiledPlan` must be the exact provenance-backed
 * compiled plan the run-plan acceptance returned; `intent` the exact
 * provenance-backed prepared `continue_stage_intent`.
 *
 * Retry semantics: the completion controller recognizes its own C1–C4
 * partial retries and is always called first; because the grant
 * controller pins the completed boundary before any next iteration (an
 * open next iteration is its typed conflict, never a retry of that
 * boundary), the C5 full retry surfaces as the grant controller's
 * `lifecycle_conflict` — the composition then re-recognizes the exact
 * completed-and-opened shape on the authoritative snapshot against the
 * fixed policy and continues with the ensure only on that exact match,
 * while every other failure is re-thrown unchanged by identity. The
 * ensure step opens exactly the successor iteration of the granted
 * generation (`closed_iteration_index + 1`) or recognizes the exact
 * already-open one with zero dispatch.
 *
 * Durability: the composed controllers' typed failures pass through
 * unchanged by identity; no rollback, no automatic retry, no second
 * dispatch. The composition's own failure reasons are exactly
 * `invalid_options` and `invalid_result`; unexpected errors keep their
 * class and identity. Diagnostics are content-free.
 *
 * Runtime export surface is exactly
 * `PipelineV2ContinuedStageControllerError` and
 * `openPipelineV2ContinuedStage` (types are not runtime keys). The full
 * algorithm, capture order, verification contracts, retry windows and
 * honest boundaries are documented in
 * `pipeline_v2_continued_stage_controller_internal.ts`.
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, the graph transition on the opened iteration and the
 * next stage execution, the architect/replanning branch
 * (`revise_task_intent`), model profile replacement, automatic resume,
 * coordinator/runner/CLI/default-pipeline wiring, schema/reducer changes,
 * migrations/API/T3 and multi-process locking.
 */
export { PipelineV2ContinuedStageControllerError } from "./pipeline_v2_continued_stage_controller_internal.ts";
export type {
  OpenedPipelineV2ContinuedStage,
  OpenPipelineV2ContinuedStageOptions,
  PipelineV2ContinuedStageControllerFailureReason,
  PipelineV2ContinuedStageControllerSink,
} from "./pipeline_v2_continued_stage_controller_internal.ts";

/**
 * Completes the durable continue-stage flow and opens the next iteration
 * of the same generation (see the module docstring). The returned result
 * is deep-frozen and content-free: the wait index, the accepted intent
 * digest, the request/response digests, the additional iteration count,
 * the fixed action id and its routing target, the iteration the grant
 * closed, the iteration the ensure opened, the generation index, the
 * exact frozen compiled stage object and the ensure controller's
 * authoritative state.
 */
export function openPipelineV2ContinuedStage(
  options: OpenPipelineV2ContinuedStageOptions,
): Promise<OpenedPipelineV2ContinuedStage> {
  return openPipelineV2ContinuedStageWithIo(productionContinuedStageOps, options);
}
