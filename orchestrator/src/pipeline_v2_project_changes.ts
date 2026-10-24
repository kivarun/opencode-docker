/**
 * The versioned read-only Project Changes API: the stable external
 * projection of the "run workspace changes relative to the durable
 * project baseline" comparison for one finalized pipeline v2 run.
 *
 * This module is the single public facade over the internal change-set
 * owner (`pipeline_v2_project_changes_internal.ts`). It reuses — never
 * duplicates — the established owners: the shared read-only durable-run
 * load chain (`loadDurablePipelineV2Run`, also serving `inspect`,
 * `events` and `logs`), the shared finalized-run-status predicate of the
 * state module, the single schema-v10 provenance parser/digest/binding
 * owner through its verified store load, and the provenance scanner's
 * read-only tree-observation IO seam. There is no second provenance
 * parser, no second digest implementation, no reducer or store mutation,
 * no mutation lock, no Docker Helper / Launcher / Session access and no
 * write of any kind on any path: a repeated read over unchanged trees is
 * byte-identical, and a damaged or missing baseline is a typed refusal,
 * never a repair.
 *
 * The comparison is an observation, not an authorization: it never
 * writes to the operator project, never applies anything and carries no
 * `safe_to_apply` field. The observation identity is an ephemeral read
 * snapshot digest — never durable state, never a revision.
 */
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2FinalizedRunStatus } from "./pipeline_v2_state.ts";
import { isCleanAbsolutePath } from "./clean_path.ts";
import {
  compareProjectChangesWithIo,
  PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS,
  PipelineV2ProjectChangesError,
  type PipelineV2ProjectChangeEntry,
  type PipelineV2ProjectChangesFailureReason,
  type PipelineV2ProjectChangesSummary,
} from "./pipeline_v2_project_changes_internal.ts";
import { realProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";

export {
  PIPELINE_V2_PROJECT_CHANGE_KINDS,
  PIPELINE_V2_PROJECT_SOURCE_RELATIONS,
  PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS,
  PipelineV2ProjectChangesError,
} from "./pipeline_v2_project_changes_internal.ts";
export type {
  PipelineV2ProjectChangeEntry,
  PipelineV2ProjectChangeKind,
  PipelineV2ProjectChangeNode,
  PipelineV2ProjectChangesFailureReason,
  PipelineV2ProjectChangesSummary,
  PipelineV2ProjectSourceRelation,
} from "./pipeline_v2_project_changes_internal.ts";

/** The default and maximum page size of the changes projection. */
export const PIPELINE_V2_PROJECT_CHANGES_DEFAULT_LIMIT = 100;
export const PIPELINE_V2_PROJECT_CHANGES_MAX_LIMIT = 200;

const HEX_DIGEST_PATTERN = /^[0-9a-f]{64}$/;

/** The options of one read-only `orchestrator project-changes` invocation. */
export interface PipelineV2ProjectChangesOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly projectPath: string;
  readonly after?: number;
  readonly limit?: number;
  readonly observation?: string;
}

/** The failure document (the events/inspect failure shape). */
export interface PipelineV2ProjectChangesFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly runRoot: string | null;
  readonly reason: PipelineV2ProjectChangesFailureReason;
}

/** The versioned success document of the changes projection. */
export interface PipelineV2ProjectChangesReport {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly runRoot: string;
  readonly revision: number;
  readonly observation: string;
  readonly summary: PipelineV2ProjectChangesSummary;
  readonly changes: readonly PipelineV2ProjectChangeEntry[];
  readonly nextAfter: number | null;
}

export type PipelineV2ProjectChangesOutcome =
  | PipelineV2ProjectChangesReport
  | PipelineV2ProjectChangesFailure;

function failure(
  runId: string,
  runRoot: string | null,
  reason: PipelineV2ProjectChangesFailureReason,
): PipelineV2ProjectChangesFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, runRoot, reason });
}

/**
 * Reads the run workspace changes of one finalized pipeline v2 run
 * relative to its durable provenance baseline and the observed current
 * operator project, and returns the versioned projection or exactly one
 * typed failure. Every path is read-only: no run mutation lock, no Docker
 * Helper involvement, no durable change, no temporary file and no
 * filesystem write.
 */
export async function projectChangesPipelineV2Run(
  options: PipelineV2ProjectChangesOptions,
): Promise<PipelineV2ProjectChangesOutcome> {
  // 1. The option capture: exactly one read per field, validated before
  //    any path is built. An invalid run id is never echoed.
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  const projectPath = options.projectPath;
  const after = options.after ?? 0;
  const limit = options.limit ?? PIPELINE_V2_PROJECT_CHANGES_DEFAULT_LIMIT;
  const observation = options.observation;
  if (typeof stateRoot !== "string" || stateRoot === "" || !isCleanAbsolutePath(stateRoot)) {
    return failure("", null, "invalid_options");
  }
  if (typeof projectPath !== "string" || projectPath === "" || !isCleanAbsolutePath(projectPath)) {
    return failure("", null, "invalid_options");
  }
  if (!Number.isSafeInteger(after) || after < 0) {
    return failure("", null, "invalid_options");
  }
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > PIPELINE_V2_PROJECT_CHANGES_MAX_LIMIT) {
    return failure("", null, "invalid_options");
  }
  if (observation !== undefined && (typeof observation !== "string" || !HEX_DIGEST_PATTERN.test(observation))) {
    return failure("", null, "invalid_options");
  }
  if (after > 0 && observation === undefined) {
    return failure("", null, "invalid_options");
  }

  // 2. The shared read-only load chain (run root, state, pipeline
  //    identity): a missing run is never created and a damaged run is
  //    never repaired.
  const read = await loadDurablePipelineV2Run({ runId, stateRoot });
  if (!read.ok) {
    return failure(read.runId, read.runRoot, read.reason);
  }
  const { runId: verifiedRunId, runRoot, state } = read;

  // 3. Eligibility: only finalized terminal runs carry a comparable
  //    workspace. Every other boundary — running, plan-ready, waiting,
  //    resumable and every terminal recovery window before the final
  //    status — is a typed zero-write refusal.
  if (!isPipelineV2FinalizedRunStatus(state.status)) {
    return failure(verifiedRunId, runRoot, "changes_not_ready");
  }

  // 4. The full comparison through the single internal owner.
  try {
    const comparison = await compareProjectChangesWithIo(
      realProjectWorkspaceVerifyIo,
      runRoot,
      projectPath,
      state,
    );

    // 5. The ephemeral observation identity: a provided observation must
    //    match the recomputed digest exactly, otherwise the read saw a
    //    different pair of trees and no partial page is returned.
    if (observation !== undefined && observation !== comparison.observation) {
      return failure(verifiedRunId, runRoot, "stale_observation");
    }

    // 6. The cursor: `after` counts already viewed entries; a cursor
    //    beyond the total set is invalid.
    const total = comparison.changes.length;
    if (after > total) {
      return failure(verifiedRunId, runRoot, "invalid_cursor");
    }
    const page = comparison.changes.slice(after, after + limit);
    const nextAfter = after + page.length < total ? after + page.length : null;
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId: verifiedRunId,
      runRoot,
      revision: state.revision,
      observation: comparison.observation,
      summary: comparison.summary,
      changes: page,
      nextAfter,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2ProjectChangesError) {
      return failure(verifiedRunId, runRoot, cause.reason);
    }
    return failure(verifiedRunId, runRoot, "internal_error");
  }
}
