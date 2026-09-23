import {
  preparePipelineV2RunPlanCandidateCore,
  publishPipelineV2RunPlanCandidateWithOps,
  realPipelineV2RunPlanCandidatePublicationOps,
} from "./pipeline_v2_run_plan_candidate_internal.ts";
import type {
  PreparedPipelineV2RunPlanRevision,
  PreparedPipelineV2RunTaskRevision,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Public run plan candidate layer for pipeline v2 (unwired,
 * production-neutral).
 *
 * `preparePipelineV2RunPlanCandidate` validates one coherent run plan
 * candidate — one prepared plan revision plus exactly the prepared task
 * revisions its task pointers declare — before any filesystem side
 * effect: the full provenance gates, the protected TASK input binding,
 * the plan revision chain, the plan↔task bindings and every task revision
 * chain (predecessor set). It returns a deep-frozen candidate whose task
 * revisions are in the plan's deterministic order (stage declaration
 * order, then the plan's normalized task order inside each stage); the
 * caller's array order is never semantic.
 *
 * `publishPipelineV2RunPlanCandidate` publishes the candidate with the
 * production run-plan store: every task revision first, strictly
 * sequentially in candidate order, then the plan revision — the plan
 * artifact is the filesystem commit marker of the whole candidate. The
 * publication is idempotent (an exact retry adopts the existing
 * artifacts); a failure leaves immutable orphan task artifacts, never a
 * rollback, and the caller retries with a new call. A published plan
 * artifact alone is not durable acceptance: the candidate layer never
 * reads or writes durable state and never dispatches a reducer command.
 *
 * Options are captured exactly once at the start of the call (each field
 * read one time, in declaration order); later mutations of the options
 * object or the passed arrays cannot influence the result, and caller
 * objects are never frozen or modified.
 *
 * Error classes stay with their owners: manifest failures remain
 * `PipelineV2RunPlanManifestError`, consistency/provenance failures
 * remain `PipelineV2RunPlanBindingError`, filesystem failures remain
 * `PipelineV2RunPlanStoreError`. The runtime export surface is exactly
 * the two functions; types are erased.
 */
export interface PreparePipelineV2RunPlanCandidateOptions {
  readonly plan: PreparedPipelineV2RunPlanRevision;
  readonly taskRevisions: readonly PreparedPipelineV2RunTaskRevision[];
  readonly previousPlan: PreparedPipelineV2RunPlanRevision | null;
  readonly previousTaskRevisions: readonly PreparedPipelineV2RunTaskRevision[];
  readonly protectedInputDigest: string;
}

export interface PreparedPipelineV2RunPlanCandidate {
  readonly plan: PreparedPipelineV2RunPlanRevision;
  readonly task_revisions: readonly PreparedPipelineV2RunTaskRevision[];
}

/**
 * Prepares one run plan candidate: full validation before any filesystem
 * side effect, the exact prepared objects in the plan's deterministic
 * task order, deep-frozen and provenance-registered for the publisher.
 */
export function preparePipelineV2RunPlanCandidate(
  options: PreparePipelineV2RunPlanCandidateOptions,
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidateCore(
    options.plan,
    options.taskRevisions,
    options.previousPlan,
    options.previousTaskRevisions,
    options.protectedInputDigest,
  );
}

/**
 * Publishes one prepared run plan candidate under the fixed run-plan
 * layout: task revisions first (candidate order, strictly sequential),
 * then the plan revision as the commit marker. Returns the same
 * candidate object; an exact retry adopts the existing artifacts.
 */
export async function publishPipelineV2RunPlanCandidate(
  runRoot: string,
  candidate: PreparedPipelineV2RunPlanCandidate,
): Promise<PreparedPipelineV2RunPlanCandidate> {
  return await publishPipelineV2RunPlanCandidateWithOps(
    realPipelineV2RunPlanCandidatePublicationOps,
    runRoot,
    candidate,
  );
}
