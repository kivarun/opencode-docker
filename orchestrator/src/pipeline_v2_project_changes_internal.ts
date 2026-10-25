/**
 * The internal core of the versioned read-only Project Changes API: the
 * read-only tree observation, the change classification against the
 * durable schema-v10 project provenance, the source-relation projection
 * and the ephemeral observation digest.
 *
 * Ownership boundary: this module is the single production owner of the
 * "run workspace changes relative to the durable project baseline"
 * projection. It reuses — never duplicates — the established owners: the
 * shared read-only durable-run load chain, the single schema-v10
 * provenance parser/digest/binding owner (`pipeline_v2_project_provenance`
 * through its store facade), and the provenance scanner's read-only
 * tree-observation IO seam (`ProjectWorkspaceVerifyIo`). There is no
 * second provenance parser, no second digest implementation, no reducer or
 * store mutation, no mutation lock, no Docker Helper / Launcher / Session
 * access, no temporary file and no write of any kind on any path.
 *
 * Semantics (deliberately honest):
 * - the projection compares the OBSERVED current content of the final run
 *   workspace (`<runRoot>/project`) against the durable provenance
 *   baseline; it does NOT claim the final workspace is a sealed
 *   worker-authored artifact and it does NOT claim every post-snapshot
 *   modification was made by the worker (a trusted host operator can
 *   technically reach the same tree);
 * - the `baseline` node of a change describes the source observation
 *   recorded by the durable provenance; the `result` node describes the
 *   result a future Apply increment would potentially produce on the
 *   operator project — it is an observation, never an authorization;
 * - the observation digest is an ephemeral read snapshot identity: it is
 *   never written to durable state, it is not a durable revision and it
 *   is not permission to Apply anything.
 */
import { createHash } from "node:crypto";
import { join } from "node:path";
import { canonicalJson } from "./canonical_json.ts";
import {
  observeRegularFileStream,
  ProjectFileStreamError,
  realProjectFileStreamIo,
  type ProjectFileStreamIo,
} from "./pipeline_v2_project_stream_internal.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  PipelineV2ProjectProvenanceError,
  type PipelineV2ProjectProvenanceDirectoryEntry,
  type PipelineV2ProjectProvenanceEntry,
  type PipelineV2ProjectProvenanceRegularFileEntry,
  type PipelineV2ProjectProvenanceSymlinkEntry,
  type PreparedPipelineV2ProjectProvenance,
  requireProvenanceBindingMatch,
} from "./pipeline_v2_project_provenance.ts";
import { loadPipelineV2ProjectProvenance } from "./pipeline_v2_project_provenance_store.ts";
import {
  PipelineV2ProjectProvenanceStoreError,
  realProjectWorkspaceVerifyIo,
  type ProjectWorkspaceVerifyIo,
} from "./pipeline_v2_project_provenance_store_internal.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";

/** The closed change-kind vocabulary of the projection. */
export const PIPELINE_V2_PROJECT_CHANGE_KINDS = [
  "added",
  "deleted",
  "content_modified",
  "mode_modified",
  "content_and_mode_modified",
  "symlink_target_modified",
  "type_changed",
] as const;
export type PipelineV2ProjectChangeKind = (typeof PIPELINE_V2_PROJECT_CHANGE_KINDS)[number];

/** The closed source-relation vocabulary of the projection. */
export const PIPELINE_V2_PROJECT_SOURCE_RELATIONS = [
  "matches_baseline",
  "matches_result",
  "diverged",
] as const;
export type PipelineV2ProjectSourceRelation = (typeof PIPELINE_V2_PROJECT_SOURCE_RELATIONS)[number];

/** One baseline/result node of the public wire shape. */
export type PipelineV2ProjectChangeNode =
  | {
      readonly kind: "regular_file";
      readonly mode: number;
      readonly byte_count: number;
      readonly sha256: string;
      readonly multiple_links: boolean;
      readonly hardlink_group?: number;
    }
  | { readonly kind: "directory"; readonly mode: number }
  | { readonly kind: "symlink"; readonly target: string };

/** One ordered change entry of the public wire shape. */
export interface PipelineV2ProjectChangeEntry {
  readonly index: number;
  readonly path: string;
  readonly change: PipelineV2ProjectChangeKind;
  readonly source_relation: PipelineV2ProjectSourceRelation;
  readonly baseline: PipelineV2ProjectChangeNode | null;
  readonly result: PipelineV2ProjectChangeNode | null;
}

/** The exact summary counts of one full comparison. */
export interface PipelineV2ProjectChangesSummary {
  readonly total: number;
  readonly added: number;
  readonly deleted: number;
  readonly content_modified: number;
  readonly mode_modified: number;
  readonly content_and_mode_modified: number;
  readonly symlink_target_modified: number;
  readonly type_changed: number;
  readonly matches_baseline: number;
  readonly matches_result: number;
  readonly diverged: number;
}

/** The closed failure vocabulary of the read-only changes projection. */
export const PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS = [
  "run_not_found",
  "invalid_options",
  "run_state_invalid",
  "pipeline_mismatch",
  "artifact_missing",
  "artifact_modified",
  "changes_not_ready",
  "project_invalid",
  "project_unstable",
  "workspace_unstable",
  "unsupported_project_entry",
  "invalid_cursor",
  "stale_observation",
  "internal_error",
] as const;
export type PipelineV2ProjectChangesFailureReason =
  (typeof PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS)[number];

/** The typed failure of the read-only changes projection. */
export class PipelineV2ProjectChangesError extends Error {
  declare readonly reason: PipelineV2ProjectChangesFailureReason;

  constructor(reason: PipelineV2ProjectChangesFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ProjectChangesError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

const OBSERVATION_DIGEST_DOMAIN = "pipeline-v2-project-changes-observation\0";

/** One observed tree entry (internal; identity fields never serialize). */
interface ObservedEntry {
  readonly path: string;
  readonly kind: "directory" | "regular_file" | "symlink";
  readonly mode: number;
  readonly byteCount: number;
  readonly sha256: string;
  readonly target: string;
  readonly nlink: number;
  readonly dev: number;
  readonly ino: number;
  readonly mtimeMs: number;
  readonly ctimeMs: number;
}

/** The internal failure of one read-only tree scan; the reason is already
 * the final typed failure reason of the owning tree. */
class ProjectTreeScanError extends Error {
  declare readonly reason: PipelineV2ProjectChangesFailureReason;
  constructor(reason: PipelineV2ProjectChangesFailureReason, message: string) {
    super(message);
    this.name = "ProjectTreeScanError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

interface ScanFailureLabels {
  readonly invalidRoot: PipelineV2ProjectChangesFailureReason;
  readonly unstable: PipelineV2ProjectChangesFailureReason;
  readonly unsupported: PipelineV2ProjectChangesFailureReason;
  readonly io: PipelineV2ProjectChangesFailureReason;
}

function scanFailure(cause: unknown): PipelineV2ProjectChangesFailureReason {
  if (cause instanceof ProjectTreeScanError) {
    return cause.reason;
  }
  throw cause;
}

const WORKSPACE_SCAN_LABELS: ScanFailureLabels = Object.freeze({
  invalidRoot: "artifact_missing",
  unstable: "workspace_unstable",
  unsupported: "unsupported_project_entry",
  io: "internal_error",
});

const PROJECT_SCAN_LABELS: ScanFailureLabels = Object.freeze({
  invalidRoot: "project_invalid",
  unstable: "project_unstable",
  unsupported: "unsupported_project_entry",
  io: "project_invalid",
});

function metadataChanged(before: { mode: number; size: number; nlink: number; mtimeMs: number; ctimeMs: number }, after: { mode: number; size: number; nlink: number; mtimeMs: number; ctimeMs: number }): boolean {
  return (
    before.mode !== after.mode ||
    before.size !== after.size ||
    before.nlink !== after.nlink ||
    before.mtimeMs !== after.mtimeMs ||
    before.ctimeMs !== after.ctimeMs
  );
}

/**
 * One read-only tree observation. The scan fixes the root identity,
 * checks every entry's kind/mode/identity/size/content timestamps before
 * and after each file read (the regular-file reads are one streaming
 * observation pass each, via the shared streaming owner), re-reads every
 * directory's entry set after walking it and never dereferences a leaf
 * symlink. There is no write, chmod, repair or temporary file on any
 * path, and no whole file is ever held in memory: the scan reads each
 * file once from offset 0 to EOF through a fixed 128 KiB chunk.
 */
async function scanTreeWithIo(
  io: ProjectWorkspaceVerifyIo,
  fileStreamIo: ProjectFileStreamIo,
  root: string,
  labels: ScanFailureLabels,
): Promise<Map<string, ObservedEntry>> {
  const rootInfo = await io.lstatOrNull(root).catch((cause: unknown) => {
    throw new ProjectTreeScanError(labels.io, "the tree root could not be inspected");
  });
  if (rootInfo === null) {
    throw new ProjectTreeScanError(labels.invalidRoot, "the tree root does not exist");
  }
  if (rootInfo.isSymbolicLink() || !rootInfo.isDirectory()) {
    throw new ProjectTreeScanError(labels.invalidRoot, "the tree root is not a real directory");
  }
  const canonical = await io.realpath(root).catch((cause: unknown) => {
    throw new ProjectTreeScanError(labels.invalidRoot, "the tree root is not canonical");
  });
  if (canonical !== root) {
    throw new ProjectTreeScanError(labels.invalidRoot, "the tree root is not canonical");
  }
  const rootIdentity = { dev: rootInfo.dev, ino: rootInfo.ino };
  const entries = new Map<string, ObservedEntry>();
  const walk = async (directory: string, relative: string): Promise<void> => {
    const beforeNames = (await io.readdirNames(directory).catch((cause: unknown) => {
      throw new ProjectTreeScanError(labels.io, "a directory could not be read");
    })).sort();
    for (const name of beforeNames) {
      const childRelative = relative === "" ? name : `${relative}/${name}`;
      const childPath = join(directory, name);
      const before = await io.lstatOrNull(childPath).catch((cause: unknown) => {
        throw new ProjectTreeScanError(labels.io, "an entry could not be inspected");
      });
      if (before === null) {
        throw new ProjectTreeScanError(labels.unstable, "a tree entry disappeared during the scan");
      }
      if (before.isSymbolicLink()) {
        const target = await io.readlink(childPath).catch((cause: unknown) => {
          throw new ProjectTreeScanError(labels.io, "a symbolic link target could not be read");
        });
        const after = await io.lstatOrNull(childPath).catch((cause: unknown) => {
          throw new ProjectTreeScanError(labels.io, "an entry could not be inspected");
        });
        if (after === null || after.dev !== before.dev || after.ino !== before.ino || !after.isSymbolicLink()) {
          throw new ProjectTreeScanError(labels.unstable, "a symbolic link was replaced during the scan");
        }
        entries.set(childRelative, {
          path: childRelative,
          kind: "symlink",
          mode: before.mode & 0o777,
          byteCount: 0,
          sha256: "",
          target,
          nlink: 1,
          dev: before.dev,
          ino: before.ino,
          mtimeMs: before.mtimeMs,
          ctimeMs: before.ctimeMs,
        });
        continue;
      }
      if (before.isDirectory()) {
        const directoryIdentity = { dev: before.dev, ino: before.ino };
        const directoryMode = before.mode & 0o777;
        await walk(childPath, childRelative);
        const after = await io.lstatOrNull(childPath).catch((_cause: unknown) => {
          throw new ProjectTreeScanError(labels.io, "an entry could not be inspected");
        });
        if (
          after === null ||
          after.dev !== directoryIdentity.dev ||
          after.ino !== directoryIdentity.ino ||
          after.isSymbolicLink() ||
          !after.isDirectory()
        ) {
          throw new ProjectTreeScanError(labels.unstable, "a directory was replaced during the scan");
        }
        entries.set(childRelative, {
          path: childRelative,
          kind: "directory",
          mode: directoryMode,
          byteCount: 0,
          sha256: "",
          target: "",
          nlink: 1,
          dev: directoryIdentity.dev,
          ino: directoryIdentity.ino,
          mtimeMs: before.mtimeMs,
          ctimeMs: before.ctimeMs,
        });
        continue;
      }
      if (!before.isFile()) {
        throw new ProjectTreeScanError(labels.unsupported, "a tree entry is an unsupported object kind");
      }
      const beforeMetadata = {
        mode: before.mode & 0o777,
        size: before.size,
        nlink: before.nlink,
        mtimeMs: before.mtimeMs,
        ctimeMs: before.ctimeMs,
      };
      const observed = await observeRegularFileStream(fileStreamIo, childPath).catch((cause: unknown) => {
        if (cause instanceof ProjectFileStreamError) {
          if (cause.reason === "unstable") {
            throw new ProjectTreeScanError(labels.unstable, "a regular file was replaced or changed during the scan");
          }
          throw new ProjectTreeScanError(labels.io, "a regular file could not be read");
        }
        throw cause;
      });
      if (
        observed.identity.dev !== before.dev ||
        observed.identity.ino !== before.ino ||
        metadataChanged(beforeMetadata, {
          mode: observed.mode,
          size: observed.stat.size,
          nlink: observed.stat.nlink,
          mtimeMs: observed.stat.mtimeMs,
          ctimeMs: observed.stat.ctimeMs,
        }) ||
        observed.byteCount !== beforeMetadata.size
      ) {
        throw new ProjectTreeScanError(labels.unstable, "a regular file changed during the scan");
      }
      entries.set(childRelative, {
        path: childRelative,
        kind: "regular_file",
        mode: beforeMetadata.mode,
        byteCount: observed.byteCount,
        sha256: observed.sha256,
        target: "",
        nlink: beforeMetadata.nlink,
        dev: before.dev,
        ino: before.ino,
        mtimeMs: beforeMetadata.mtimeMs,
        ctimeMs: beforeMetadata.ctimeMs,
      });
    }
    // The directory's own entry set is re-read after its children were
    // walked: an entry added or removed during the scan fails closed.
    const afterNames = (await io.readdirNames(directory).catch((_cause: unknown) => {
      throw new ProjectTreeScanError(labels.io, "a directory could not be read");
    })).sort();
    let changed = afterNames.length !== beforeNames.length;
    if (!changed) {
      for (let index = 0; index < afterNames.length; index++) {
        if (afterNames[index] !== beforeNames[index]) {
          changed = true;
          break;
        }
      }
    }
    if (changed) {
      throw new ProjectTreeScanError(labels.unstable, "a directory entry set changed during the scan");
    }
  };
  await walk(root, "");
  const rootAfter = await io.lstatOrNull(root).catch((cause: unknown) => {
    throw new ProjectTreeScanError(labels.io, "the tree root could not be inspected");
  });
  if (rootAfter === null || rootAfter.dev !== rootIdentity.dev || rootAfter.ino !== rootIdentity.ino) {
    throw new ProjectTreeScanError(labels.unstable, "the tree root was replaced during the scan");
  }
  const canonicalAfter = await io.realpath(root).catch((cause: unknown) => {
    throw new ProjectTreeScanError(labels.invalidRoot, "the tree root is not canonical");
  });
  if (canonicalAfter !== root) {
    throw new ProjectTreeScanError(labels.unstable, "the tree root was replaced during the scan");
  }
  return entries;
}

/** The canonical hardlink topology of one observed tree. */
interface ObservedHardlinkTopology {
  /** path → the sorted member paths sharing the file's identity (groups of two or more). */
  readonly membersByPath: Map<string, readonly string[]>;
  /** path → the canonical 1-based group index (serialization only). */
  readonly indexByPath: Map<string, number>;
  /** canonical 1-based group index → the sorted member paths. */
  readonly membersByGroup: Map<number, readonly string[]>;
}

function hardlinkTopologyOf(entries: Map<string, ObservedEntry>): ObservedHardlinkTopology {
  const byIdentity = new Map<string, string[]>();
  for (const entry of entries.values()) {
    if (entry.kind !== "regular_file") {
      continue;
    }
    const key = `${entry.dev}:${entry.ino}`;
    const members = byIdentity.get(key);
    if (members === undefined) {
      byIdentity.set(key, [entry.path]);
    } else {
      members.push(entry.path);
    }
  }
  const membersByPath = new Map<string, readonly string[]>();
  const indexByPath = new Map<string, number>();
  const membersByGroup = new Map<number, readonly string[]>();
  let nextIndex = 1;
  for (const members of byIdentity.values()) {
    if (members.length < 2) {
      continue;
    }
    members.sort();
    for (const member of members) {
      membersByPath.set(member, members);
      indexByPath.set(member, nextIndex);
    }
    membersByGroup.set(nextIndex, members);
    nextIndex += 1;
  }
  return { membersByPath, indexByPath, membersByGroup };
}

function nodeFromBaselineEntry(entry: PipelineV2ProjectProvenanceEntry): PipelineV2ProjectChangeNode {
  if (entry.kind === "directory") {
    return deepFreezeValue({ kind: "directory", mode: entry.source_mode });
  }
  if (entry.kind === "symlink") {
    return deepFreezeValue({ kind: "symlink", target: entry.target });
  }
  const node: {
    kind: "regular_file";
    mode: number;
    byte_count: number;
    sha256: string;
    multiple_links: boolean;
    hardlink_group?: number;
  } = {
    kind: "regular_file",
    mode: entry.source_mode,
    byte_count: entry.byte_count,
    sha256: entry.sha256,
    multiple_links: entry.multiple_links,
  };
  if (entry.hardlink_group !== undefined) {
    node.hardlink_group = entry.hardlink_group;
  }
  return deepFreezeValue(node);
}

function nodeFromObserved(
  entry: ObservedEntry,
  groups: Map<string, number>,
): PipelineV2ProjectChangeNode {
  if (entry.kind === "directory") {
    return deepFreezeValue({ kind: "directory", mode: entry.mode });
  }
  if (entry.kind === "symlink") {
    return deepFreezeValue({ kind: "symlink", target: entry.target });
  }
  const group = groups.get(entry.path);
  const node: {
    kind: "regular_file";
    mode: number;
    byte_count: number;
    sha256: string;
    multiple_links: boolean;
    hardlink_group?: number;
  } = {
    kind: "regular_file",
    mode: entry.mode,
    byte_count: entry.byteCount,
    sha256: entry.sha256,
    multiple_links: entry.nlink > 1,
  };
  if (group !== undefined) {
    node.hardlink_group = group;
  }
  return deepFreezeValue(node);
}

interface Classification {
  readonly changes: readonly PipelineV2ProjectChangeEntry[];
  readonly summary: PipelineV2ProjectChangesSummary;
}

/**
 * Classifies the union of the provenance workspace-baseline paths and the
 * observed final workspace paths into the closed change vocabulary and
 * computes each entry's source relation against the observed current
 * operator project. Renames are never inferred: a moved path is exactly
 * one `deleted` and one `added` entry.
 */
function classifyChanges(
  baseline: PreparedPipelineV2ProjectProvenance,
  workspace: Map<string, ObservedEntry>,
  operator: Map<string, ObservedEntry>,
  workspaceTopology: ObservedHardlinkTopology,
  operatorTopology: ObservedHardlinkTopology,
): Classification {
  const baselineByPath = new Map<string, PipelineV2ProjectProvenanceEntry>();
  for (const entry of baseline.manifest.entries) {
    baselineByPath.set(entry.path, entry);
  }
  const workspaceGroupMembers = workspaceTopology.membersByGroup;
  const paths = new Set<string>([...baselineByPath.keys(), ...workspace.keys()]);
  const ordered = [...paths].sort();
  const changes: PipelineV2ProjectChangeEntry[] = [];
  const sourceMatchesBaseline = (
    observed: ObservedEntry,
    entry: PipelineV2ProjectProvenanceEntry,
  ): boolean => {
    if (entry.kind === "directory") {
      return observed.kind === "directory" && observed.mode === entry.source_mode;
    }
    if (entry.kind === "symlink") {
      return observed.kind === "symlink" && observed.target === entry.target;
    }
    const file = entry as PipelineV2ProjectProvenanceRegularFileEntry;
    if (observed.kind !== "regular_file" || observed.mode !== file.source_mode) {
      return false;
    }
    if (observed.byteCount !== file.byte_count || observed.sha256 !== file.sha256) {
      return false;
    }
    if (!file.multiple_links) {
      return observed.nlink === 1;
    }
    const baselineMembers = baseline.manifest.entries
      .filter((candidate) => candidate.kind === "regular_file" && candidate.hardlink_group === file.hardlink_group)
      .map((candidate) => candidate.path)
      .sort();
    const operatorMembers = operatorTopology.membersByPath.get(observed.path) ?? [observed.path];
    if (operatorMembers.length !== baselineMembers.length) {
      return false;
    }
    for (let index = 0; index < baselineMembers.length; index++) {
      if (baselineMembers[index] !== operatorMembers[index]) {
        return false;
      }
    }
    return observed.nlink === operatorMembers.length;
  };
  const matchesResult = (observed: ObservedEntry, result: PipelineV2ProjectChangeNode): boolean => {
    if (observed.kind !== result.kind) {
      return false;
    }
    if (result.kind === "directory") {
      return observed.mode === result.mode;
    }
    if (result.kind === "symlink") {
      return observed.target === result.target;
    }
    if (observed.mode !== result.mode || observed.byteCount !== result.byte_count || observed.sha256 !== result.sha256) {
      return false;
    }
    if (!result.multiple_links) {
      return observed.nlink === 1;
    }
    // A hardlinked result keeps its group semantics: the observed operator
    // file must share exactly the result group's member paths.
    const group = result.hardlink_group;
    if (group === undefined) {
      return true;
    }
    const members = workspaceGroupMembers.get(group);
    if (members === undefined) {
      return false;
    }
    const operatorMembers = operatorTopology.membersByPath.get(observed.path) ?? [observed.path];
    if (operatorMembers.length !== members.length || observed.nlink !== members.length) {
      return false;
    }
    for (let index = 0; index < members.length; index++) {
      if (members[index] !== operatorMembers[index]) {
        return false;
      }
    }
    return true;
  };
  const ancestorsCompatible = (path: string): boolean => {
    const segments = path.split("/");
    for (let index = 1; index < segments.length; index++) {
      const ancestor = segments.slice(0, index).join("/");
      const observed = operator.get(ancestor);
      if (observed !== undefined && observed.kind !== "directory") {
        return false;
      }
    }
    return true;
  };
  const relationFor = (
    change: PipelineV2ProjectChangeKind,
    baselineEntry: PipelineV2ProjectProvenanceEntry | undefined,
    resultNode: PipelineV2ProjectChangeNode | null,
    path: string,
  ): PipelineV2ProjectSourceRelation => {
    const observed = operator.get(path);
    if (change === "deleted") {
      if (observed === undefined) {
        return "matches_result";
      }
      if (baselineEntry !== undefined && sourceMatchesBaseline(observed, baselineEntry)) {
        return "matches_baseline";
      }
      return "diverged";
    }
    if (change === "added") {
      if (observed === undefined) {
        return ancestorsCompatible(path) ? "matches_baseline" : "diverged";
      }
      if (resultNode !== null && matchesResult(observed, resultNode)) {
        return "matches_result";
      }
      return "diverged";
    }
    if (observed === undefined) {
      return "diverged";
    }
    if (baselineEntry !== undefined && sourceMatchesBaseline(observed, baselineEntry)) {
      return "matches_baseline";
    }
    if (resultNode !== null && matchesResult(observed, resultNode)) {
      return "matches_result";
    }
    return "diverged";
  };
  for (const path of ordered) {
    const baselineEntry = baselineByPath.get(path);
    const finalEntry = workspace.get(path);
    let change: PipelineV2ProjectChangeKind;
    let resultNode: PipelineV2ProjectChangeNode | null;
    let baselineNode: PipelineV2ProjectChangeNode | null;
    if (baselineEntry !== undefined && finalEntry !== undefined) {
      if (baselineEntry.kind !== finalEntry.kind) {
        change = "type_changed";
        baselineNode = nodeFromBaselineEntry(baselineEntry);
        resultNode = nodeFromObserved(finalEntry, workspaceTopology.indexByPath);
      } else if (finalEntry.kind === "directory") {
        const baselineDirectory = baselineEntry as PipelineV2ProjectProvenanceDirectoryEntry;
        if (finalEntry.mode === baselineDirectory.workspace_mode) {
          continue;
        }
        change = "mode_modified";
        baselineNode = nodeFromBaselineEntry(baselineDirectory);
        resultNode = nodeFromObserved(finalEntry, workspaceTopology.indexByPath);
      } else if (finalEntry.kind === "symlink") {
        const baselineLink = baselineEntry as PipelineV2ProjectProvenanceSymlinkEntry;
        if (finalEntry.target === baselineLink.target) {
          continue;
        }
        change = "symlink_target_modified";
        baselineNode = nodeFromBaselineEntry(baselineLink);
        resultNode = nodeFromObserved(finalEntry, workspaceTopology.indexByPath);
      } else {
        const file = baselineEntry as PipelineV2ProjectProvenanceRegularFileEntry;
        const contentChanged = finalEntry.sha256 !== file.sha256;
        const modeChanged = finalEntry.mode !== file.workspace_mode;
        if (!contentChanged && !modeChanged) {
          continue;
        }
        change = contentChanged && modeChanged
          ? "content_and_mode_modified"
          : contentChanged
            ? "content_modified"
            : "mode_modified";
        baselineNode = nodeFromBaselineEntry(baselineEntry);
        resultNode = nodeFromObserved(finalEntry, workspaceTopology.indexByPath);
        if (change === "content_modified") {
          // The final workspace mode equals the workspace baseline mode,
          // so the projected result keeps the original source mode.
          const group = workspaceTopology.indexByPath.get(finalEntry.path);
          const override: {
            kind: "regular_file";
            mode: number;
            byte_count: number;
            sha256: string;
            multiple_links: boolean;
            hardlink_group?: number;
          } = {
            kind: "regular_file",
            mode: file.source_mode,
            byte_count: finalEntry.byteCount,
            sha256: finalEntry.sha256,
            multiple_links: finalEntry.nlink > 1,
          };
          if (group !== undefined) {
            override.hardlink_group = group;
          }
          resultNode = deepFreezeValue(override);
        }
      }
    } else if (finalEntry !== undefined) {
      change = "added";
      baselineNode = null;
      resultNode = nodeFromObserved(finalEntry, workspaceTopology.indexByPath);
    } else {
      change = "deleted";
      baselineNode = nodeFromBaselineEntry(baselineEntry!);
      resultNode = null;
    }
    changes.push(deepFreezeValue({
      index: changes.length + 1,
      path,
      change,
      source_relation: relationFor(change, baselineEntry, resultNode, path),
      baseline: baselineNode,
      result: resultNode,
    }));
  }
  const summary: PipelineV2ProjectChangesSummary = deepFreezeValue({
    total: changes.length,
    added: changes.filter((entry) => entry.change === "added").length,
    deleted: changes.filter((entry) => entry.change === "deleted").length,
    content_modified: changes.filter((entry) => entry.change === "content_modified").length,
    mode_modified: changes.filter((entry) => entry.change === "mode_modified").length,
    content_and_mode_modified: changes.filter((entry) => entry.change === "content_and_mode_modified").length,
    symlink_target_modified: changes.filter((entry) => entry.change === "symlink_target_modified").length,
    type_changed: changes.filter((entry) => entry.change === "type_changed").length,
    matches_baseline: changes.filter((entry) => entry.source_relation === "matches_baseline").length,
    matches_result: changes.filter((entry) => entry.source_relation === "matches_result").length,
    diverged: changes.filter((entry) => entry.source_relation === "diverged").length,
  });
  return { changes: deepFreezeValue(changes), summary };
}

/** The observation document serialization of one observed tree. */
function serializeObservation(
  entries: Map<string, ObservedEntry>,
  groups: Map<string, number>,
): readonly Record<string, unknown>[] {
  const ordered = [...entries.values()].sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return ordered.map((entry) => {
    if (entry.kind === "directory") {
      return { path: entry.path, kind: entry.kind, mode: entry.mode };
    }
    if (entry.kind === "symlink") {
      return { path: entry.path, kind: entry.kind, target: entry.target };
    }
    const group = groups.get(entry.path);
    const serialized: Record<string, unknown> = {
      path: entry.path,
      kind: entry.kind,
      mode: entry.mode,
      byte_count: entry.byteCount,
      sha256: entry.sha256,
      multiple_links: entry.nlink > 1,
    };
    if (group !== undefined) {
      serialized.hardlink_group = group;
    }
    return serialized;
  });
}

/** The full comparison of one finalized run: scan, classify, observe.
 * The optional file-stream capability defaults to the real streaming
 * owner; tests inject per-call seams. */
export async function compareProjectChangesWithIo(
  io: ProjectWorkspaceVerifyIo,
  runRoot: string,
  projectPath: string,
  state: PipelineV2RunState,
  fileStreamIo: ProjectFileStreamIo = realProjectFileStreamIo,
): Promise<{
  observation: string;
  summary: PipelineV2ProjectChangesSummary;
  changes: readonly PipelineV2ProjectChangeEntry[];
}> {
  // 1. The durable provenance baseline through the single verified load.
  const stored = await loadPipelineV2ProjectProvenance(runRoot).catch(
    (cause: unknown): PreparedPipelineV2ProjectProvenance | null => {
      if (cause instanceof PipelineV2ProjectProvenanceStoreError) {
        if (cause.reason === "conflict") {
          throw new PipelineV2ProjectChangesError("artifact_modified", "the project provenance manifest does not match its durable binding");
        }
        throw new PipelineV2ProjectChangesError("internal_error", "the project provenance manifest layout could not be read");
      }
      if (cause instanceof PipelineV2ProjectProvenanceError) {
        throw new PipelineV2ProjectChangesError("artifact_modified", "the project provenance manifest is not a valid provenance document");
      }
      throw cause;
    },
  );
  if (stored === null) {
    throw new PipelineV2ProjectChangesError("artifact_missing", "the project provenance manifest does not exist");
  }
  try {
    requireProvenanceBindingMatch(stored, state.project_provenance);
  } catch (cause) {
    if (cause instanceof PipelineV2ProjectProvenanceError) {
      throw new PipelineV2ProjectChangesError("artifact_modified", "the project provenance manifest does not match its durable binding");
    }
    throw cause;
  }

  // 2. The two read-only tree observations.
  let workspace: Map<string, ObservedEntry>;
  try {
    workspace = await scanTreeWithIo(io, fileStreamIo, join(runRoot, "project"), WORKSPACE_SCAN_LABELS);
  } catch (cause) {
    throw new PipelineV2ProjectChangesError(scanFailure(cause), cause instanceof Error ? cause.message : "the final run workspace could not be observed");
  }
  let operator: Map<string, ObservedEntry>;
  try {
    operator = await scanTreeWithIo(io, fileStreamIo, projectPath, PROJECT_SCAN_LABELS);
  } catch (cause) {
    throw new PipelineV2ProjectChangesError(scanFailure(cause), cause instanceof Error ? cause.message : "the operator project could not be observed");
  }

  // 3. The classification and the ephemeral observation digest.
  const workspaceTopology = hardlinkTopologyOf(workspace);
  const operatorTopology = hardlinkTopologyOf(operator);
  const { changes, summary } = classifyChanges(stored, workspace, operator, workspaceTopology, operatorTopology);
  const observationDocument = deepFreezeValue({
    schema_version: 1,
    kind: "pipeline_v2_project_changes_observation",
    run_id: state.run_id,
    provenance: {
      digest: state.project_provenance.digest,
      entry_count: state.project_provenance.entry_count,
      copy_policy_version: state.project_provenance.copy_policy_version,
    },
    revision: state.revision,
    workspace: serializeObservation(workspace, workspaceTopology.indexByPath),
    project: serializeObservation(operator, operatorTopology.indexByPath),
    changes,
    summary,
  });
  const observation = createHash("sha256")
    .update(OBSERVATION_DIGEST_DOMAIN)
    .update(canonicalJson(observationDocument), "utf8")
    .digest("hex");
  return { observation, summary, changes };
}
