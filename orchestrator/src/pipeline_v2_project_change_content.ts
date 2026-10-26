/**
 * The versioned read-only Project Change Content API: the stable
 * external projection of one bounded raw-byte slice of one side of one
 * verified Project Changes entry for one finalized pipeline v2 run.
 *
 * This module is the single public facade over the internal content core
 * (`pipeline_v2_project_change_content_internal.ts`). It reuses — never
 * duplicates — the established owners: the shared read-only durable-run
 * load chain, the single Project Changes comparison owner (the full
 * observation is recomputed and compared on every request), the pure
 * side-availability resolver and the one streaming regular-file
 * observation owner. There is no mutation lock, no Docker Helper /
 * Launcher / Session access and no write of any kind on any path.
 *
 * Authority: the caller names run id, change index, side, observation,
 * offset and limit — never a filesystem path. The observed path is
 * derived exclusively from the verified change entry.
 *
 * The content is secret-bearing and served verbatim by design; only the
 * `dataBase64` field carries bytes, and no status other than
 * `available` carries content keys.
 */
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2FinalizedRunStatus } from "./pipeline_v2_state.ts";
import { isCleanAbsolutePath } from "./clean_path.ts";
import { realProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";
import { realProjectFileStreamIo } from "./pipeline_v2_project_stream_internal.ts";
import {
  acquirePipelineV2SharedProjectLock,
  PipelineV2ApplyProjectLockError,
  type PipelineV2ApplyProjectLock,
} from "./pipeline_v2_apply_project_lock_internal.ts";
import { PipelineV2ProjectChangesError, type PipelineV2ProjectChangeNode, type PipelineV2ProjectChangesFailureReason } from "./pipeline_v2_project_changes.ts";
import {
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES,
  readProjectChangeContentWithIo,
  type PipelineV2ProjectChangeContentResult,
  type PipelineV2ProjectChangeContentSide,
} from "./pipeline_v2_project_change_content_internal.ts";

export {
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_STATUSES,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_UNAVAILABLE_REASONS,
} from "./pipeline_v2_project_change_content_internal.ts";
export type {
  PipelineV2ProjectChangeContentSide,
  PipelineV2ProjectChangeContentStatus,
  PipelineV2ProjectChangeContentUnavailableReason,
} from "./pipeline_v2_project_change_content_internal.ts";

/** The default and maximum raw-byte chunk limit (the CLI/HTTP contract). */
export const PIPELINE_V2_PROJECT_CHANGE_CONTENT_DEFAULT_CHUNK_LIMIT = 65_536;
export const PIPELINE_V2_PROJECT_CHANGE_CONTENT_MAX_CHUNK_LIMIT = 262_144;

const HEX_DIGEST_PATTERN = /^[0-9a-f]{64}$/;

/** The options of one read-only `orchestrator project-change-content`
 * invocation. The caller never passes a filesystem path. */
export interface PipelineV2ProjectChangeContentOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly projectPath: string;
  readonly changeIndex: number;
  readonly side: PipelineV2ProjectChangeContentSide;
  readonly observation: string;
  readonly offset?: number;
  readonly limit?: number;
}

/** The failure document (the Project Changes failure shape). */
export interface PipelineV2ProjectChangeContentFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly runRoot: string | null;
  readonly reason: PipelineV2ProjectChangesFailureReason;
}

/** The versioned success document of the content projection. */
export type PipelineV2ProjectChangeContentReport = PipelineV2ProjectChangeContentResult & {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runRoot: string;
};

export type PipelineV2ProjectChangeContentOutcome =
  | PipelineV2ProjectChangeContentReport
  | PipelineV2ProjectChangeContentFailure;

function failure(
  runId: string,
  runRoot: string | null,
  reason: PipelineV2ProjectChangesFailureReason,
): PipelineV2ProjectChangeContentFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, runRoot, reason });
}

/**
 * Reads one bounded raw-byte slice of one side of one verified Project
 * Changes entry, or returns exactly one typed failure. Every path is
 * read-only: no mutation lock, no Docker Helper involvement, no durable
 * change, no temporary file and no filesystem write. The operator-side
 * read runs under the cooperative shared project lock (`LOCK_SH` on the
 * canonical root), so no payload byte is ever served from a mid-apply
 * state; contention is the typed `project_busy` refusal with zero bytes.
 */
export async function projectChangeContentPipelineV2Run(
  options: PipelineV2ProjectChangeContentOptions,
): Promise<PipelineV2ProjectChangeContentOutcome> {
  // 1. The option capture: exactly one read per field, validated before
  //    any path is built. An invalid run id is never echoed.
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  const projectPath = options.projectPath;
  const changeIndex = options.changeIndex;
  const side = options.side;
  const observation = options.observation;
  const offset = options.offset ?? 0;
  const limit = options.limit ?? PIPELINE_V2_PROJECT_CHANGE_CONTENT_DEFAULT_CHUNK_LIMIT;
  if (typeof stateRoot !== "string" || stateRoot === "" || !isCleanAbsolutePath(stateRoot)) {
    return failure("", null, "invalid_options");
  }
  if (typeof projectPath !== "string" || projectPath === "" || !isCleanAbsolutePath(projectPath)) {
    return failure("", null, "invalid_options");
  }
  if (typeof changeIndex !== "number" || !Number.isSafeInteger(changeIndex) || changeIndex < 1) {
    return failure(typeof runId === "string" ? runId : "", null, "invalid_options");
  }
  if (typeof side !== "string" || !(PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES as readonly string[]).includes(side)) {
    return failure(typeof runId === "string" ? runId : "", null, "invalid_options");
  }
  if (typeof observation !== "string" || !HEX_DIGEST_PATTERN.test(observation)) {
    return failure(typeof runId === "string" ? runId : "", null, "invalid_options");
  }
  if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0) {
    return failure(typeof runId === "string" ? runId : "", null, "invalid_options");
  }
  if (
    typeof limit !== "number" ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > PIPELINE_V2_PROJECT_CHANGE_CONTENT_MAX_CHUNK_LIMIT
  ) {
    return failure(typeof runId === "string" ? runId : "", null, "invalid_options");
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
  //    workspace and observable content.
  if (!isPipelineV2FinalizedRunStatus(state.status)) {
    return failure(verifiedRunId, runRoot, "changes_not_ready");
  }

  // 4. The content read through the single internal owner, under the
  //    cooperative shared project lock: the full observation recompute
  //    and the single-pass read of the operator-side file run with LOCK_SH
  //    held on the canonical operator project root, so no payload byte is
  //    ever served from a mid-apply state. Contention is the typed busy
  //    refusal with zero bytes; a root the lock cannot resolve falls
  //    through to the established content classification; a
  //    lock-infrastructure failure refuses fail-closed.
  let lock: PipelineV2ApplyProjectLock | undefined;
  try {
    try {
      const acquisition = acquirePipelineV2SharedProjectLock(projectPath);
      if (acquisition.busy) {
        return failure(verifiedRunId, runRoot, "project_busy");
      }
      lock = acquisition.lock;
    } catch (cause) {
      if (!(cause instanceof PipelineV2ApplyProjectLockError)) {
        throw cause;
      }
      if (cause.reason !== "root_invalid") {
        return failure(verifiedRunId, runRoot, "internal_error");
      }
      // root_invalid: the content read below classifies exactly this
      // project root through its own observation recompute.
    }
    const result = await readProjectChangeContentWithIo(
      realProjectWorkspaceVerifyIo,
      realProjectFileStreamIo,
      runRoot,
      projectPath,
      state,
      { changeIndex, side: side as PipelineV2ProjectChangeContentSide, observation, offset, limit },
    );
    return deepFreezeValue({
      ...result,
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runRoot,
    }) as PipelineV2ProjectChangeContentReport;
  } catch (cause) {
    if (cause instanceof PipelineV2ProjectChangesError) {
      return failure(verifiedRunId, runRoot, cause.reason);
    }
    return failure(verifiedRunId, runRoot, "internal_error");
  } finally {
    lock?.release();
  }
}

// The status/side unions are re-exported above; the node type is part of
// the public result shape.
export type { PipelineV2ProjectChangeNode };