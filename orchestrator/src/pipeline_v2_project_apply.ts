import {
  PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS,
  PipelineV2ProjectApplyError,
  type PipelineV2ProjectApplyFailureReason,
  type PipelineV2ProjectApplyOptions,
  type PreparedPipelineV2ProjectApplyPackage,
  type PipelineV2ProjectApplyOperationProjection,
  preparePipelineV2ProjectApplyCore,
  loadPreparedPackageCore,
  inspectOperationCore,
} from "./pipeline_v2_project_apply_internal.ts";
import { realImmutableDocumentIo } from "./pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo } from "./run_snapshot_store.ts";
import { realProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";
import { realProjectFileStreamIo } from "./pipeline_v2_project_stream_internal.ts";
import { realMetadataProbeIo } from "./pipeline_v2_apply_metadata_probe_internal.ts";
import type { PipelineV2ApplyPlan } from "./pipeline_v2_apply_plan_internal.ts";

/**
 * The public production facade of the durable Project Apply preparation
 * package. It binds the fixed real filesystem capabilities once and
 * exposes exactly the three surfaces the next increment's filesystem
 * writer needs:
 *
 *   - `preparePipelineV2ProjectApply` — create, resume or retry one
 *     operation identity into a fully self-contained prepared package
 *     (immutable plan + sealed payloads + crash-safe journal) with zero
 *     writes to the registered project;
 *   - `loadPreparedPipelineV2ProjectApplyPackage` — the read-only loader
 *     that verifies a prepared package without the run workspace;
 *   - `inspectPipelineV2ProjectApplyOperation` — the read-only recovery
 *     projection of one operation's journal state.
 *
 * There is deliberately no HTTP route, no CLI command, no Browser UI
 * surface, no control-plane command and no deployment flag in this
 * increment: the facade is consumed by the permanent integration tests
 * and by the next writer increment.
 */

export {
  PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS,
  PipelineV2ProjectApplyError,
};
export type {
  PipelineV2ProjectApplyFailureReason,
  PipelineV2ProjectApplyOptions,
  PreparedPipelineV2ProjectApplyPackage,
  PipelineV2ProjectApplyOperationProjection,
};
export type { PipelineV2ApplyPlan };

const productionComponents = {
  docIo: realImmutableDocumentIo,
  stateIo: defaultPipelineStateIo,
  verifyIo: realProjectWorkspaceVerifyIo,
  fileStreamIo: realProjectFileStreamIo,
  probeIo: realMetadataProbeIo,
} as const;

export async function preparePipelineV2ProjectApply(
  options: PipelineV2ProjectApplyOptions,
): Promise<PreparedPipelineV2ProjectApplyPackage> {
  return await preparePipelineV2ProjectApplyCore(options, productionComponents);
}

export async function loadPreparedPipelineV2ProjectApplyPackage(options: {
  readonly applyStoreRoot: string;
  readonly applyId: string;
}): Promise<PreparedPipelineV2ProjectApplyPackage> {
  return await loadPreparedPackageCore(productionComponents, options.applyStoreRoot, options.applyId);
}

export async function inspectPipelineV2ProjectApplyOperation(options: {
  readonly applyStoreRoot: string;
  readonly applyId: string;
}): Promise<PipelineV2ProjectApplyOperationProjection> {
  return await inspectOperationCore(productionComponents, options.applyStoreRoot, options.applyId);
}
