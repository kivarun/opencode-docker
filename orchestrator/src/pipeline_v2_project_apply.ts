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
import type { PipelineV2ApplyOperationStatus } from "./pipeline_v2_apply_state_internal.ts";
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

/**
 * The one public reason vocabulary of the explicit Project Apply API:
 * derived once from the two existing typed owners (the preparation
 * vocabulary and the execution vocabulary) — never a copied literal
 * list. Every CLI, subprocess-client and HTTP layer validates against
 * this single derived array.
 */
export const PIPELINE_V2_PROJECT_APPLY_PUBLIC_REASONS: readonly string[] = Object.freeze(
  Array.from(new Set<string>([...PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS, ...PIPELINE_V2_APPLY_EXECUTION_REASONS])),
);

export type PipelineV2ProjectApplyPublicReason = PipelineV2ProjectApplyFailureReason | PipelineV2ApplyExecutionReason;

/** The CLI-facing outcome of one explicit Project Apply command. */
export type PipelineV2ProjectApplyCommandOutcome =
  | {
      readonly ok: true;
      readonly exitCode: 0;
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly applyId: string;
      readonly projectId: string;
      readonly observation: string;
      readonly revision: number;
      readonly status: PipelineV2ApplyOperationStatus;
      readonly completedStepCount: number;
      readonly totalStepCount: number;
    }
  | {
      readonly ok: false;
      readonly exitCode: 1;
      readonly runId: string;
      readonly applyId: string;
      readonly reason: PipelineV2ProjectApplyPublicReason;
    };

function applyCommandFailure(
  runId: string,
  applyId: string,
  cause: unknown,
): PipelineV2ProjectApplyCommandOutcome {
  const reason: PipelineV2ProjectApplyPublicReason =
    cause instanceof PipelineV2ProjectApplyError
      ? cause.reason
      : cause instanceof PipelineV2ApplyExecutionError
        ? cause.reason
        : "internal_error";
  return { ok: false, exitCode: 1, runId, applyId, reason };
}

/**
 * The one explicit Apply composition: terminal-run eligibility, exact
 * observation verification and identity/root binding belong to the
 * existing preparation core; the filesystem execution and recovery to
 * the existing writer; the final document to the existing read-only
 * inspection projection. No second planner, writer, verifier or state
 * machine exists here.
 */
export async function applyPipelineV2ProjectOperation(options: PipelineV2ProjectApplyOptions): Promise<PipelineV2ProjectApplyCommandOutcome> {
  try {
    await preparePipelineV2ProjectApply(options);
  } catch (cause) {
    // The filesystem writer owns every post-preparation status: an
    // operation that already left the preparation phase (applied,
    // conflict, recovery_required, applying, failed) goes straight to
    // the writer, which returns its stored typed result or continues
    // the recovery. Every other preparation refusal is terminal.
    if (!(cause instanceof PipelineV2ProjectApplyError && cause.reason === "apply_not_prepared")) {
      return applyCommandFailure(options.runId, options.applyId, cause);
    }
  }
  let execution: PipelineV2ApplyExecutionResult;
  try {
    execution = await applyPreparedPipelineV2Project({
      applyStoreRoot: options.applyStoreRoot,
      applyId: options.applyId,
      projectId: options.projectId,
      projectRoot: options.projectRoot,
    });
  } catch (cause) {
    return applyCommandFailure(options.runId, options.applyId, cause);
  }
  if (!execution.ok) {
    return { ok: false, exitCode: 1, runId: options.runId, applyId: options.applyId, reason: execution.reason };
  }
  try {
    const projection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: options.applyStoreRoot, applyId: options.applyId });
    return {
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: options.runId,
      applyId: options.applyId,
      projectId: options.projectId,
      observation: projection.observation,
      revision: projection.revision,
      status: projection.status,
      completedStepCount: projection.execution?.completed_step_count ?? 0,
      totalStepCount: projection.plan_step_count ?? 0,
    };
  } catch (cause) {
    return applyCommandFailure(options.runId, options.applyId, cause);
  }
}

/**
 * The one read-only operation inspection bound to one run/project: a
 * foreign applyId is refused with the same reason as a missing one, so
 * the existence of somebody else's operation is never disclosed.
 */
export async function inspectPipelineV2ProjectApplyOperationForRun(options: {
  readonly applyStoreRoot: string;
  readonly applyId: string;
  readonly runId: string;
  readonly projectId: string;
}): Promise<PipelineV2ProjectApplyCommandOutcome> {
  try {
    const projection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: options.applyStoreRoot, applyId: options.applyId });
    if (projection.run_id !== options.runId || projection.project_id !== options.projectId) {
      return { ok: false, exitCode: 1, runId: options.runId, applyId: options.applyId, reason: "artifact_missing" };
    }
    return {
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: projection.run_id,
      applyId: projection.apply_id,
      projectId: projection.project_id,
      observation: projection.observation,
      revision: projection.revision,
      status: projection.status,
      completedStepCount: projection.execution?.completed_step_count ?? 0,
      totalStepCount: projection.plan_step_count ?? 0,
    };
  } catch (cause) {
    return applyCommandFailure(options.runId, options.applyId, cause);
  }
}

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
