/**
 * Production-neutral run plan acceptance controller for pipeline schema v2
 * (unwired).
 *
 * This module is the single layer that binds the existing run-plan layers
 * into one crash-safe order:
 *
 *   compiled/state validation
 *   → reducer pre-check of the whole missing durable sequence
 *   → publication of the task manifests (candidate order, strictly
 *     sequential)
 *   → publication of the plan manifest as the filesystem commit marker
 *   → durable `task_revision_accepted` for the missing task records
 *   → durable `plan_revision_accepted` as the state commit marker
 *
 * `acceptPipelineV2RunPlanCandidate({pipeline, runRoot, sink, candidate})`
 * verifies the candidate against the durable run state through the single
 * existing acceptance verifier
 * (`verifyPipelineV2RunPlanCandidateForAcceptance`), reconciles the
 * candidate's task and plan revision chains with the durable ledgers,
 * pre-checks the whole missing durable sequence against the reducer on a
 * local snapshot, publishes the candidate through the single existing
 * candidate publisher (idempotent adoption, tasks before the plan commit
 * marker), and then dispatches only the missing durable records through
 * the sink — the task records in candidate order and the plan record
 * last. After every dispatch the authoritative sink snapshot is re-read
 * and must structurally carry exactly the expected record, so a hostile
 * or racing dispatch can never be mistaken for a durable fact. A racing
 * identical dispatch rejected by the reducer is idempotent success only
 * when the authoritative snapshot now carries the exact expected record.
 *
 * The result is deep-frozen and content-free: the exact provenance-backed
 * compiled plan object the acceptance verifier returned (never copied,
 * never re-compiled) and the last authoritative durable state. Prepared
 * manifests, canonical JSON, task bodies, filesystem paths and the
 * caller's candidate never enter the result or the diagnostics.
 *
 * Capture boundary: every options field is read exactly once, the sink's
 * `dispatch` is captured once and bound to the sink before the first
 * await, caller objects are never frozen or modified, and the production
 * path always runs through the single frozen ops object — there is no
 * mutable module-global seam.
 *
 * Durability semantics: a sink `not_committed` keeps the previous
 * snapshot authoritative with the published manifests as orphans and
 * returns `state_persist_failed` without an automatic retry; a sink
 * `durability_unknown` adopts the visible candidate, poisons the sink,
 * returns `state_persist_failed` with the adopted state and stops every
 * further dispatch; a fresh retry reuses the published artifacts,
 * recognizes the already durable prefix and dispatches only the missing
 * records. Nothing is ever rolled back.
 *
 * Errors stay with their owners: acceptance, orchestration, compiled
 * plan, manifest, binding and publication failures keep their original
 * typed classes and identities. Only this layer's own failures are
 * `PipelineV2RunPlanControllerError` with the closed reason set
 * (`invalid_state`, `candidate_conflict`, `state_persist_failed`) and the
 * last authoritative `state`. Unexpected errors propagate unchanged.
 * Diagnostics are content-free.
 *
 * Not implemented (stays unwired): the coordinator, runner, CLI, the
 * stage generation/iteration lifecycle controller, wait/replanning
 * policy, automatic resume and multi-process locking are later
 * increments.
 */
import type {
  AcceptPipelineV2RunPlanCandidateOptions,
  AcceptedPipelineV2RunPlanCandidate,
} from "./pipeline_v2_run_plan_controller_internal.ts";
import {
  acceptPipelineV2RunPlanCandidateCore,
  realPipelineV2RunPlanControllerOps,
} from "./pipeline_v2_run_plan_controller_internal.ts";

export {
  PipelineV2RunPlanControllerError,
  type AcceptedPipelineV2RunPlanCandidate,
  type AcceptPipelineV2RunPlanCandidateOptions,
  type PipelineV2RunPlanControllerFailureReason,
  type PipelineV2RunPlanControllerSink,
} from "./pipeline_v2_run_plan_controller_internal.ts";

/**
 * Accepts one prepared run plan candidate for the durable run: validates
 * it, reconciles the durable ledgers, pre-checks the missing durable
 * sequence, publishes the manifests (tasks first, plan last as the
 * filesystem commit marker) and dispatches the missing durable records
 * through the sink. The returned `compiled_plan` is the exact
 * provenance-backed compiled plan object and `state` the last
 * authoritative durable state.
 */
export async function acceptPipelineV2RunPlanCandidate(
  options: AcceptPipelineV2RunPlanCandidateOptions,
): Promise<AcceptedPipelineV2RunPlanCandidate> {
  return await acceptPipelineV2RunPlanCandidateCore(
    realPipelineV2RunPlanControllerOps,
    options,
  );
}
