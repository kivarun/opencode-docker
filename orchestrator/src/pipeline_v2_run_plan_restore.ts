/**
 * The public facade of the read-only restoration of the last
 * durable-accepted compiled pipeline v2 run plan.
 *
 * Runtime export surface is exactly `PipelineV2RunPlanRestoreError` and
 * `restorePipelineV2AcceptedRunPlan`; types are erased. The facade always
 * runs the single frozen production ops over the existing store loaders;
 * fault injection goes through the internal core's per-call ops, so an
 * injected call cannot influence a parallel production call and there is
 * no mutable module-global seam.
 *
 * The restoration is the single official bridge from the durable plan
 * ledger and the immutable run-plan manifests to a real provenance-backed
 * `CompiledPipelineV2RunPlan` of the existing chain — the exact object
 * the run-plan acceptance controller compiles — so a restart can hand the
 * continued-stage composition a plan it did not keep in memory. The
 * CLI/runner/coordinator wiring of this bridge stays a later increment.
 */
import {
  PipelineV2RunPlanRestoreError,
  productionRunPlanRestoreOps,
  restorePipelineV2AcceptedRunPlanInternal,
  type RestorePipelineV2AcceptedRunPlanOptions,
  type RestoredPipelineV2AcceptedRunPlan,
} from "./pipeline_v2_run_plan_restore_internal.ts";

export {
  PipelineV2RunPlanRestoreError,
  type PipelineV2RunPlanRestoreFailureReason,
} from "./pipeline_v2_run_plan_restore_internal.ts";

export async function restorePipelineV2AcceptedRunPlan(
  options: RestorePipelineV2AcceptedRunPlanOptions,
): Promise<RestoredPipelineV2AcceptedRunPlan> {
  return await restorePipelineV2AcceptedRunPlanInternal(options, productionRunPlanRestoreOps);
}
