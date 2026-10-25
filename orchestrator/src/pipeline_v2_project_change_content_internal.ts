/**
 * The internal core of the versioned read-only Project Change Content
 * API: the pure side-availability resolver over one verified Project
 * Changes entry and the single-pass verified byte stream that serves one
 * bounded slice of one side of one change.
 *
 * Ownership boundary: this module reuses — never duplicates — the
 * established owners: the shared read-only durable-run load chain, the
 * single Project Changes comparison owner (the full observation is
 * recomputed and compared on every content request) and the one
 * streaming regular-file observation owner. There is no second scanner,
 * no second digest implementation, no mutation lock, no Docker Helper /
 * Launcher / Session access, no temporary file and no write of any kind
 * on any path.
 *
 * Authority: the caller never passes a filesystem path. The request
 * names run id, change index, side, observation, offset and limit only;
 * the observed path is derived exclusively from the verified change
 * entry of the freshly recomputed observation.
 *
 * The four success statuses are deliberate and distinct:
 * - `available` — the side is a regular file whose bytes were proven to
 *   belong to the expected digest by one sequential verified pass;
 * - `implicit_empty` — the baseline of an added path or the result of a
 *   deleted path: a full logical empty side of a future diff, never an
 *   error and never a fabricated digest;
 * - `metadata_only` — the side is a directory or a symlink; it has no
 *   file content by definition and its metadata already lives in the
 *   change node; a symlink is never dereferenced;
 * - `unavailable` — the baseline regular-file bytes can no longer be
 *   proven; the only reason is `source_no_longer_matches_baseline`.
 *
 * Diagnostics are content-free: no path, no bytes, no digest values of
 * artifacts, no hostile values.
 */
import { join } from "node:path";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  compareProjectChangesWithIo,
  PipelineV2ProjectChangesError,
  type PipelineV2ProjectChangeEntry,
  type PipelineV2ProjectChangesFailureReason,
  type PipelineV2ProjectChangeNode,
} from "./pipeline_v2_project_changes_internal.ts";
import type { ProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";
import {
  observeRegularFileStream,
  ProjectFileStreamError,
  type ProjectFileStreamIo,
} from "./pipeline_v2_project_stream_internal.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";

/** The closed success-status vocabulary of the content projection. */
export const PIPELINE_V2_PROJECT_CHANGE_CONTENT_STATUSES = [
  "available",
  "implicit_empty",
  "metadata_only",
  "unavailable",
] as const;
export type PipelineV2ProjectChangeContentStatus = (typeof PIPELINE_V2_PROJECT_CHANGE_CONTENT_STATUSES)[number];

/** The closed side vocabulary. */
export const PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES = ["baseline", "result"] as const;
export type PipelineV2ProjectChangeContentSide = (typeof PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES)[number];

/** The closed unavailable-reason vocabulary (exactly one member). */
export const PIPELINE_V2_PROJECT_CHANGE_CONTENT_UNAVAILABLE_REASONS = [
  "source_no_longer_matches_baseline",
] as const;
export type PipelineV2ProjectChangeContentUnavailableReason =
  (typeof PIPELINE_V2_PROJECT_CHANGE_CONTENT_UNAVAILABLE_REASONS)[number];

/** The verified change entry plus the observation identity it came from. */
interface VerifiedComparison {
  readonly observation: string;
  readonly revision: number;
  readonly changes: readonly PipelineV2ProjectChangeEntry[];
}

/** One resolved side of one verified change entry. */
type SideResolution =
  | { readonly kind: "available"; readonly from: "workspace" | "operator"; readonly expected: { readonly byteCount: number; readonly sha256: string } }
  | { readonly kind: "implicit_empty" }
  | { readonly kind: "metadata_only"; readonly node: PipelineV2ProjectChangeNode }
  | { readonly kind: "unavailable"; readonly node: PipelineV2ProjectChangeNode };

function isRegularFile(node: PipelineV2ProjectChangeNode | null): node is Extract<PipelineV2ProjectChangeNode, { kind: "regular_file" }> {
  return node !== null && node.kind === "regular_file";
}

function metadataOnly(node: PipelineV2ProjectChangeNode): SideResolution {
  return { kind: "metadata_only", node };
}

/**
 * The one pure side-availability resolver. It decides the status and the
 * read source exclusively from the verified change entry — never from a
 * caller path, digest or relation guess. A violated structural contract
 * (for example a mode-only change whose digests differ) fails closed as
 * an internal error instead of serving unprovable bytes.
 */
export function resolveProjectChangeContentSide(
  entry: PipelineV2ProjectChangeEntry,
  side: PipelineV2ProjectChangeContentSide,
): SideResolution {
  const node = side === "baseline" ? entry.baseline : entry.result;
  if (node === null) {
    return { kind: "implicit_empty" };
  }
  switch (entry.change) {
    case "added": {
      if (side === "baseline") {
        return { kind: "implicit_empty" };
      }
      return isRegularFile(node)
        ? { kind: "available", from: "workspace", expected: { byteCount: node.byte_count, sha256: node.sha256 } }
        : metadataOnly(node);
    }
    case "deleted": {
      if (side === "result") {
        return { kind: "implicit_empty" };
      }
      if (!isRegularFile(node)) {
        return metadataOnly(node);
      }
      return entry.source_relation === "matches_baseline"
        ? { kind: "available", from: "operator", expected: { byteCount: node.byte_count, sha256: node.sha256 } }
        : { kind: "unavailable", node };
    }
    case "content_modified":
    case "content_and_mode_modified": {
      if (!isRegularFile(node)) {
        return metadataOnly(node);
      }
      if (side === "result") {
        return { kind: "available", from: "workspace", expected: { byteCount: node.byte_count, sha256: node.sha256 } };
      }
      return entry.source_relation === "matches_baseline"
        ? { kind: "available", from: "operator", expected: { byteCount: node.byte_count, sha256: node.sha256 } }
        : { kind: "unavailable", node };
    }
    case "mode_modified": {
      if (node.kind !== "regular_file") {
        return metadataOnly(node);
      }
      const other = side === "baseline" ? entry.result : entry.baseline;
      if (
        !isRegularFile(node) ||
        !isRegularFile(other) ||
        node.sha256 !== other.sha256 ||
        node.byte_count !== other.byte_count
      ) {
        throw new PipelineV2ProjectChangesError(
          "internal_error",
          "the mode-only change does not carry the proven equal-content contract",
        );
      }
      // Both sides carry logically identical bytes; the verified result
      // workspace node is the read source for either side, independent of
      // the current operator relation.
      return { kind: "available", from: "workspace", expected: { byteCount: node.byte_count, sha256: node.sha256 } };
    }
    case "symlink_target_modified": {
      return metadataOnly(node);
    }
    case "type_changed": {
      if (!isRegularFile(node)) {
        return metadataOnly(node);
      }
      if (side === "result") {
        return { kind: "available", from: "workspace", expected: { byteCount: node.byte_count, sha256: node.sha256 } };
      }
      return entry.source_relation === "matches_baseline"
        ? { kind: "available", from: "operator", expected: { byteCount: node.byte_count, sha256: node.sha256 } }
        : { kind: "unavailable", node };
    }
  }
}

/** The deep-frozen content result of one verified side. */
export type PipelineV2ProjectChangeContentResult =
  | {
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly changeIndex: number;
      readonly side: PipelineV2ProjectChangeContentSide;
      readonly status: "available";
      readonly byteCount: number;
      readonly sha256: string;
      readonly offset: number;
      readonly dataBase64: string;
      readonly nextOffset: number | null;
    }
  | {
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly changeIndex: number;
      readonly side: PipelineV2ProjectChangeContentSide;
      readonly status: "implicit_empty";
      readonly byteCount: 0;
      readonly offset: 0;
      readonly dataBase64: "";
      readonly nextOffset: null;
    }
  | {
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly changeIndex: number;
      readonly side: PipelineV2ProjectChangeContentSide;
      readonly status: "metadata_only";
      readonly node: PipelineV2ProjectChangeNode;
    }
  | {
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly changeIndex: number;
      readonly side: PipelineV2ProjectChangeContentSide;
      readonly status: "unavailable";
      readonly reason: PipelineV2ProjectChangeContentUnavailableReason;
      readonly node: PipelineV2ProjectChangeNode;
    };

export interface PipelineV2ProjectChangeContentRequest {
  readonly changeIndex: number;
  readonly side: PipelineV2ProjectChangeContentSide;
  readonly observation: string;
  readonly offset: number;
  readonly limit: number;
}

function contentError(reason: PipelineV2ProjectChangesFailureReason, message: string): PipelineV2ProjectChangesError {
  return new PipelineV2ProjectChangesError(reason, message);
}

/**
 * Reads one side of one verified change entry: the full Project Changes
 * observation is recomputed and compared first (a mismatch is
 * `stale_observation` before any byte is read), the exact entry is
 * resolved by index, the side availability is decided by the pure
 * resolver, and an available side is served through one single-pass
 * verified stream whose digest covers the same bytes the captured slice
 * came from. No partial response exists on any mismatch.
 */
export async function readProjectChangeContentWithIo(
  io: ProjectWorkspaceVerifyIo,
  fileStreamIo: ProjectFileStreamIo,
  runRoot: string,
  projectPath: string,
  state: PipelineV2RunState,
  request: PipelineV2ProjectChangeContentRequest,
): Promise<PipelineV2ProjectChangeContentResult> {
  // 1. The full observation recompute through the single changes owner.
  let comparison: VerifiedComparison;
  try {
    const full = await compareProjectChangesWithIo(io, runRoot, projectPath, state, fileStreamIo);
    comparison = { observation: full.observation, revision: state.revision, changes: full.changes };
  } catch (cause) {
    // The typed changes failures keep their established reasons
    // (artifact_missing/artifact_modified for the provenance manifest,
    // workspace_unstable/project_unstable for scan races, and so on).
    throw cause;
  }

  // 2. The observation identity: a mismatch means the caller's metadata
  //    page no longer describes these trees — refused before any content.
  if (request.observation !== comparison.observation) {
    throw contentError("stale_observation", "the observation does not match the current trees");
  }

  // 3. The exact change entry by index.
  if (request.changeIndex < 1 || request.changeIndex > comparison.changes.length) {
    throw contentError("invalid_cursor", "the requested change index does not name a verified change");
  }
  const entry = comparison.changes[request.changeIndex - 1]!;

  // 4. The side availability from the verified entry.
  const resolution = resolveProjectChangeContentSide(entry, request.side);
  const common = {
    runId: state.run_id,
    revision: state.revision,
    observation: comparison.observation,
    changeIndex: request.changeIndex,
    side: request.side,
  } as const;

  if (resolution.kind === "implicit_empty") {
    if (request.offset !== 0) {
      throw contentError("invalid_cursor", "an empty logical side serves offset 0 only");
    }
    return deepFreezeValue({
      ...common,
      status: "implicit_empty" as const,
      byteCount: 0 as const,
      offset: 0 as const,
      dataBase64: "" as const,
      nextOffset: null,
    });
  }
  if (resolution.kind === "metadata_only") {
    if (request.offset !== 0) {
      throw contentError("invalid_cursor", "a metadata-only side serves offset 0 only");
    }
    return deepFreezeValue({ ...common, status: "metadata_only" as const, node: resolution.node });
  }
  if (resolution.kind === "unavailable") {
    if (request.offset !== 0) {
      throw contentError("invalid_cursor", "an unavailable side serves offset 0 only");
    }
    return deepFreezeValue({
      ...common,
      status: "unavailable" as const,
      reason: "source_no_longer_matches_baseline" as const,
      node: resolution.node,
    });
  }

  // 5. The cursor of an available side, in raw bytes, checked before any
  //    byte is read.
  const expected = resolution.expected;
  if (request.offset > expected.byteCount) {
    throw contentError("invalid_cursor", "the requested offset is beyond the verified content");
  }

  // 6. The single-pass verified stream from the derived trusted path.
  const fromPath =
    resolution.from === "workspace"
      ? join(runRoot, "project", entry.path)
      : join(projectPath, entry.path);
  const unstableReason = resolution.from === "workspace" ? "workspace_unstable" : "project_unstable";
  const observed = await observeRegularFileStream(fileStreamIo, fromPath, {
    expected,
    capture: { offset: request.offset, limit: request.limit },
  }).catch((cause: unknown) => {
    if (cause instanceof ProjectFileStreamError) {
      if (cause.reason === "unstable") {
        throw contentError(unstableReason, "the verified content changed during the read");
      }
      throw contentError("internal_error", "the verified content could not be read");
    }
    throw cause;
  });
  const captured = observed.captured!;
  const end = request.offset + captured.byteLength;
  return deepFreezeValue({
    ...common,
    status: "available" as const,
    byteCount: observed.byteCount,
    sha256: observed.sha256,
    offset: request.offset,
    dataBase64: captured.toString("base64"),
    nextOffset: end < observed.byteCount ? end : null,
  });
}

