import {
  PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS,
  PipelineV2ProjectApplyError,
  type PipelineV2ProjectApplyOptions,
  type PreparedPipelineV2ProjectApplyPackage,
  type PipelineV2ProjectApplyOperationProjection,
  preparePipelineV2ProjectApplyCore,
  loadPreparedPackageCore,
  inspectOperationCore,
  type PipelineV2ProjectApplyFailureReason,
} from "./pipeline_v2_project_apply_internal.ts";
import {
  PIPELINE_V2_APPLY_EXECUTION_REASONS,
  PipelineV2ApplyExecutionError,
  applyPreparedProjectCore,
  type PipelineV2ApplyExecutionComponents,
  type PipelineV2ApplyExecutionOptions,
  type PipelineV2ApplyExecutionReason,
  type PipelineV2ApplyExecutionResult,
} from "./pipeline_v2_project_apply_writer_internal.ts";
import { realApplyFdOps } from "./pipeline_v2_apply_fd_internal.ts";
import { realImmutableDocumentIo } from "./pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo } from "./run_snapshot_store.ts";
import { realProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";
import { realProjectFileStreamIo } from "./pipeline_v2_project_stream_internal.ts";
import { realMetadataProbeIo } from "./pipeline_v2_apply_metadata_probe_internal.ts";
import type { PipelineV2ApplyPlan } from "./pipeline_v2_apply_plan_internal.ts";

/**
 * The public production facade of the durable Project Apply package. It
 * binds the fixed real filesystem capabilities once and exposes exactly
 * the four surfaces the package lifecycle needs:
 *
 *   - `preparePipelineV2ProjectApply` — create, resume or retry one
 *     operation identity into a fully self-contained prepared package
 *     (immutable plan + sealed payloads + crash-safe journal) with zero
 *     writes to the registered project;
 *   - `loadPreparedPipelineV2ProjectApplyPackage` — the read-only loader
 *     that verifies a prepared package without the run workspace;
 *   - `applyPreparedPipelineV2Project` — the filesystem writer: applies
 *     the prepared package to the freshly resolved registered project
 *     under the project lock, with per-step durable progress, crash
 *     recovery and the closed typed refusal vocabulary; it never reads
 *     the run workspace;
 *   - `inspectPipelineV2ProjectApplyOperation` — the read-only recovery
 *     projection of one operation's journal state.
 *
 * There is deliberately no HTTP route, no CLI command, no Browser UI
 * surface, no control-plane command and no deployment flag in this
 * increment: the facade is consumed by the permanent integration tests.
 * The registered project deployment mount stays read-only.
 */

export {
  PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS,
  PIPELINE_V2_APPLY_EXECUTION_REASONS,
  PipelineV2ProjectApplyError,
  PipelineV2ApplyExecutionError,
};
export type {
  PipelineV2ProjectApplyFailureReason,
  PipelineV2ProjectApplyOptions,
  PreparedPipelineV2ProjectApplyPackage,
  PipelineV2ProjectApplyOperationProjection,
  PipelineV2ApplyExecutionOptions,
  PipelineV2ApplyExecutionResult,
  PipelineV2ApplyExecutionReason,
};
export type { PipelineV2ApplyPlan };

const productionComponents = {
  docIo: realImmutableDocumentIo,
  stateIo: defaultPipelineStateIo,
  verifyIo: realProjectWorkspaceVerifyIo,
  fileStreamIo: realProjectFileStreamIo,
  probeIo: realMetadataProbeIo,
} as const;

const productionExecutionComponents: PipelineV2ApplyExecutionComponents = {
  docIo: realImmutableDocumentIo,
  stateIo: defaultPipelineStateIo,
  fileStreamIo: realProjectFileStreamIo,
  fdOps: realApplyFdOps,
};

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

export async function applyPreparedPipelineV2Project(
  options: PipelineV2ApplyExecutionOptions,
): Promise<PipelineV2ApplyExecutionResult> {
  return await applyPreparedProjectCore(options, productionExecutionComponents);
}

export async function inspectPipelineV2ProjectApplyOperation(options: {
  readonly applyStoreRoot: string;
  readonly applyId: string;
}): Promise<PipelineV2ProjectApplyOperationProjection> {
  return await inspectOperationCore(productionComponents, options.applyStoreRoot, options.applyId);
}
