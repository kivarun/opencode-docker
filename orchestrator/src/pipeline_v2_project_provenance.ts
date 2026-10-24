/**
 * Durable project snapshot provenance (pure substrate, state schema v10).
 *
 * Every pipeline v2 run now owns one immutable provenance manifest that is
 * produced by the single owner of the project copy — the run-owned project
 * copy traversal in `pipeline_v2_project_copy_internal.ts` — from exactly
 * the objects and bytes that traversal read and copied. The manifest never
 * re-reads the operator source after the copy; it is the durable record of
 *
 *   1. the source observation — what was read from the operator project
 *      (source permission modes, hardlink characteristics), and
 *   2. the workspace baseline — what was materialized into the initial
 *      `<runRoot>/project` tree (normalized workspace modes, the exact
 *      copied byte digests, verbatim symlink targets).
 *
 * The manifest allows a later increment to compute an exact worker-attributed
 * change set (add/modify/delete/type/mode) between the initial workspace
 * baseline and the final run workspace, and to compare the current operator
 * project against the original source observation, without confusing the
 * copy policy's mode normalization with a worker change. Nothing in this
 * module implements a diff, an Apply, or any public projection.
 *
 * Copy policy (version 1, bound by `copy_policy_version`): real directories
 * are copied 0700, regular files 0600 (0700 when the source carries any
 * execute bit), symlinks are copied verbatim (target text never resolved),
 * uid/gid/timestamps/xattrs/ACLs are not preserved, and hardlinks are
 * split into independent regular files with the split recorded durably
 * (`multiple_links` plus a canonical per-manifest `hardlink_group`).
 *
 * Digests: the manifest digest is SHA-256 over the dedicated closed domain
 * `pipeline-v2-project-provenance\0` followed by the manifest's canonical
 * JSON (key-sorted, no trailing newline). Regular-file entries carry the
 * plain SHA-256 of the exact copied file bytes; they are bound by the
 * manifest digest, so no second framing domain is needed for them.
 *
 * Absolute source/project/run paths, uid/gid, timestamps, xattrs/ACLs and
 * host inode numbers never enter the document. `dev`/`ino` are used only
 * as ephemeral copy-time identity for the hardlink groups and are never
 * serialized.
 */

import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical_json.ts";

export const PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION = 1;
export const PIPELINE_V2_PROJECT_PROVENANCE_KIND = "pipeline_v2_project_provenance" as const;
export const PIPELINE_V2_PROJECT_COPY_POLICY_VERSION = 1;

const DIGEST_DOMAIN = "pipeline-v2-project-provenance\0";

/** The three object kinds the copy contract materializes. */
export type PipelineV2ProjectProvenanceEntryKind = "directory" | "regular_file" | "symlink";

/** One directory entry of the manifest. */
export interface PipelineV2ProjectProvenanceDirectoryEntry {
  readonly path: string;
  readonly kind: "directory";
  readonly source_mode: number;
  readonly workspace_mode: number;
}

/** One regular-file entry of the manifest. */
export interface PipelineV2ProjectProvenanceRegularFileEntry {
  readonly path: string;
  readonly kind: "regular_file";
  readonly source_mode: number;
  readonly workspace_mode: number;
  readonly byte_count: number;
  readonly sha256: string;
  /**
   * True exactly when the source object carried more than one hard link at
   * copy time. The copy split the link into an independent regular file;
   * a future Apply increment must resolve this explicitly instead of
   * silently destroying the link.
   */
  readonly multiple_links: boolean;
  /**
   * The canonical per-manifest hardlink group index (1-based, assigned in
   * deterministic entry order to every set of regular-file entries whose
   * source objects shared one copy-time inode identity). Present exactly
   * when `multiple_links` is true; never a host inode number.
   */
  readonly hardlink_group?: number;
}

/** One symlink entry of the manifest. */
export interface PipelineV2ProjectProvenanceSymlinkEntry {
  readonly path: string;
  readonly kind: "symlink";
  /** The verbatim link target text, never dereferenced. */
  readonly target: string;
  /**
   * Copy-time confirmation that the created workspace symlink's target
   * text equals the source link target text byte for byte.
   */
  readonly targets_identical: true;
}

export type PipelineV2ProjectProvenanceEntry =
  | PipelineV2ProjectProvenanceDirectoryEntry
  | PipelineV2ProjectProvenanceRegularFileEntry
  | PipelineV2ProjectProvenanceSymlinkEntry;

/** The immutable manifest document. */
export interface PipelineV2ProjectProvenanceManifest {
  readonly schema_version: typeof PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION;
  readonly kind: typeof PIPELINE_V2_PROJECT_PROVENANCE_KIND;
  readonly copy_policy_version: typeof PIPELINE_V2_PROJECT_COPY_POLICY_VERSION;
  readonly entries: readonly PipelineV2ProjectProvenanceEntry[];
}

/** The prepared, deep-frozen manifest snapshot with its canonical bytes. */
export interface PreparedPipelineV2ProjectProvenance {
  readonly manifest: PipelineV2ProjectProvenanceManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

/** The minimal immutable state binding of one run's provenance manifest. */
export interface PipelineV2ProjectProvenanceBinding {
  readonly digest: string;
  readonly entry_count: number;
  readonly copy_policy_version: typeof PIPELINE_V2_PROJECT_COPY_POLICY_VERSION;
}

/** Typed failure of the pure provenance substrate. */
export type PipelineV2ProjectProvenanceFailureReason =
  | "invalid_manifest"
  | "invalid_entry"
  | "invalid_path"
  | "workspace_mismatch";

export class PipelineV2ProjectProvenanceError extends Error {
  declare readonly reason: PipelineV2ProjectProvenanceFailureReason;

  constructor(reason: PipelineV2ProjectProvenanceFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ProjectProvenanceError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

const MAX_RELATIVE_PATH_LENGTH = 4096;
const MAX_SYMLINK_TARGET_LENGTH = 4096;
const MAX_ENTRIES = 1_000_000;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ownKeys(value: Record<string, unknown>): string[] {
  return Object.getOwnPropertyNames(value).filter((name) => Object.prototype.hasOwnProperty.call(value, name));
}

function expectExactKeys(
  value: Record<string, unknown>,
  what: string,
  reason: PipelineV2ProjectProvenanceFailureReason,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      throw new PipelineV2ProjectProvenanceError(reason, `${what} is missing the required field "${key}"`);
    }
  }
  for (const key of ownKeys(value)) {
    if (!allowed.has(key)) {
      throw new PipelineV2ProjectProvenanceError(reason, `${what} carries an unknown field`);
    }
  }
}

/**
 * The safe relative-path grammar of the manifest: non-empty, not absolute,
 * no NUL byte, and no empty, `.` or `..` component. The filename spelling
 * is preserved exactly within the supported filesystem/string model.
 */
export function isSafeProjectRelativePath(value: unknown): value is string {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_RELATIVE_PATH_LENGTH) {
    return false;
  }
  if (value.includes("\0") || value.startsWith("/")) {
    return false;
  }
  for (const component of value.split("/")) {
    if (component === "" || component === "." || component === "..") {
      return false;
    }
  }
  return true;
}

const HEX_64_PATTERN = /^[0-9a-f]{64}$/;

function expectPermissionMode(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > 0o777) {
    throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} must be a permission mode (0..0o777)`);
  }
  return value;
}

function expectByteCount(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} must be a non-negative byte count`);
  }
  return value;
}

function expectEntryDigest(value: unknown, what: string): string {
  if (typeof value !== "string" || !HEX_64_PATTERN.test(value)) {
    throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} must be a lowercase 64-hex sha256 digest`);
  }
  return value;
}

function validateEntry(raw: unknown, index: number): PipelineV2ProjectProvenanceEntry {
  if (!isPlainObject(raw)) {
    throw new PipelineV2ProjectProvenanceError("invalid_entry", `entry ${index} is not an object`);
  }
  const what = `entry ${index}`;
  if (!Object.prototype.hasOwnProperty.call(raw, "path") || !Object.prototype.hasOwnProperty.call(raw, "kind")) {
    throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} is missing the required fields "path" and "kind"`);
  }
  const path = raw["path"];
  if (!isSafeProjectRelativePath(path)) {
    throw new PipelineV2ProjectProvenanceError("invalid_path", `${what} path is not a safe relative path`);
  }
  const kind = raw["kind"];
  if (kind === "directory") {
    expectExactKeys(raw, what, "invalid_entry", ["path", "kind", "source_mode", "workspace_mode"]);
    return Object.freeze({
      path,
      kind,
      source_mode: expectPermissionMode(raw["source_mode"], `${what} source_mode`),
      workspace_mode: expectPermissionMode(raw["workspace_mode"], `${what} workspace_mode`),
    });
  }
  if (kind === "symlink") {
    expectExactKeys(raw, what, "invalid_entry", ["path", "kind", "target", "targets_identical"]);
    const target = raw["target"];
    if (
      typeof target !== "string" ||
      target.length > MAX_SYMLINK_TARGET_LENGTH ||
      target.includes("\0")
    ) {
      throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} symlink target is not a safe target text`);
    }
    if (raw["targets_identical"] !== true) {
      throw new PipelineV2ProjectProvenanceError(
        "invalid_entry",
        `${what} must confirm identical source and workspace symlink targets`,
      );
    }
    return Object.freeze({ path, kind, target, targets_identical: true });
  }
  if (kind === "regular_file") {
    expectExactKeys(raw, what, "invalid_entry", ["path", "kind", "source_mode", "workspace_mode", "byte_count", "sha256", "multiple_links"], ["hardlink_group"]);
    const multipleLinks = raw["multiple_links"];
    if (typeof multipleLinks !== "boolean") {
      throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} multiple_links must be a boolean`);
    }
    let hardlinkGroup: number | undefined;
    if (Object.prototype.hasOwnProperty.call(raw, "hardlink_group")) {
      const value = raw["hardlink_group"];
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || !multipleLinks) {
        throw new PipelineV2ProjectProvenanceError(
          "invalid_entry",
          `${what} hardlink_group must be a positive safe integer present exactly when multiple_links is true`,
        );
      }
      hardlinkGroup = value;
    } else if (multipleLinks) {
      throw new PipelineV2ProjectProvenanceError(
        "invalid_entry",
        `${what} carries multiple_links without its canonical hardlink_group`,
      );
    }
    return Object.freeze({
      path,
      kind,
      source_mode: expectPermissionMode(raw["source_mode"], `${what} source_mode`),
      workspace_mode: expectPermissionMode(raw["workspace_mode"], `${what} workspace_mode`),
      byte_count: expectByteCount(raw["byte_count"], `${what} byte_count`),
      sha256: expectEntryDigest(raw["sha256"], `${what} sha256`),
      multiple_links: multipleLinks,
      ...(hardlinkGroup === undefined ? {} : { hardlink_group: hardlinkGroup }),
    });
  }
  throw new PipelineV2ProjectProvenanceError("invalid_entry", `${what} has an unsupported kind`);
}

/**
 * Structural coherence of the entry set: unique paths, every proper path
 * ancestor present as a directory entry, and the entries sorted in
 * deterministic code-unit order of their relative paths.
 */
function requireCoherentEntries(entries: readonly PipelineV2ProjectProvenanceEntry[]): void {
  if (entries.length > MAX_ENTRIES) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the manifest exceeds the supported entry count");
  }
  const directories = new Set<string>();
  const seen = new Set<string>();
  for (const entry of entries) {
    if (seen.has(entry.path)) {
      throw new PipelineV2ProjectProvenanceError("invalid_manifest", `the manifest declares ${JSON.stringify(entry.path)} more than once`);
    }
    seen.add(entry.path);
    if (entry.kind === "directory") {
      directories.add(entry.path);
    }
  }
  for (const entry of entries) {
    const components = entry.path.split("/");
    for (let index = 1; index < components.length; index += 1) {
      const ancestor = components.slice(0, index).join("/");
      if (!directories.has(ancestor)) {
        throw new PipelineV2ProjectProvenanceError(
          "invalid_manifest",
          `the manifest entry ${JSON.stringify(entry.path)} has no directory entry for its ancestor ${JSON.stringify(ancestor)}`,
        );
      }
    }
  }
}

function deepFreezeManifest(manifest: PipelineV2ProjectProvenanceManifest): PipelineV2ProjectProvenanceManifest {
  Object.freeze(manifest.entries);
  for (const entry of manifest.entries) {
    Object.freeze(entry);
  }
  return Object.freeze(manifest);
}

/**
 * Builds the prepared manifest from the copy-time capture records. The
 * input records are the exact capture of the single copy traversal; the
 * preparer is the only validation/normalization chain (fail closed on any
 * unsafe path, unsupported shape, incoherent structure or unordered
 * input). The entries are sorted into deterministic code-unit order of
 * their relative paths; the canonical JSON and the manifest digest are
 * computed over the sorted document.
 */
export function preparePipelineV2ProjectProvenance(
  rawEntries: readonly unknown[],
): PreparedPipelineV2ProjectProvenance {
  if (!Array.isArray(rawEntries)) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the provenance capture must be an array");
  }
  const entries = rawEntries.map((entry, index) => validateEntry(entry, index));
  entries.sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
  requireCoherentEntries(entries);
  const manifest = deepFreezeManifest({
    schema_version: PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION,
    kind: PIPELINE_V2_PROJECT_PROVENANCE_KIND,
    copy_policy_version: PIPELINE_V2_PROJECT_COPY_POLICY_VERSION,
    entries,
  });
  const canonical = canonicalJson(manifest);
  const digest = createHash("sha256").update(DIGEST_DOMAIN, "utf8").update(canonical, "utf8").digest("hex");
  return Object.freeze({ manifest, canonical_json: canonical, sha256: digest });
}

/** The manifest digest over the dedicated closed domain. */
export function pipelineV2ProjectProvenanceDigest(manifest: PipelineV2ProjectProvenanceManifest): string {
  const canonical = canonicalJson(manifest);
  return createHash("sha256").update(DIGEST_DOMAIN, "utf8").update(canonical, "utf8").digest("hex");
}

/** The minimal immutable state binding of one prepared manifest. */
export function pipelineV2ProjectProvenanceBinding(
  prepared: PreparedPipelineV2ProjectProvenance,
): PipelineV2ProjectProvenanceBinding {
  return Object.freeze({
    digest: prepared.sha256,
    entry_count: prepared.manifest.entries.length,
    copy_policy_version: prepared.manifest.copy_policy_version,
  });
}

/**
 * Validates one raw state binding value (`state.project_provenance`)
 * against the exact minimal contract. Shared with the durable state
 * module; no second binding validator may exist.
 */
export function validatePipelineV2ProjectProvenanceBinding(
  value: unknown,
  what: string,
): PipelineV2ProjectProvenanceBinding {
  if (!isPlainObject(value)) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", `${what} must be an object`);
  }
  expectExactKeys(value, what, "invalid_manifest", ["digest", "entry_count", "copy_policy_version"]);
  const digest = value["digest"];
  if (typeof digest !== "string" || !HEX_64_PATTERN.test(digest)) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", `${what} digest must be a lowercase 64-hex sha256 digest`);
  }
  const entryCount = value["entry_count"];
  if (typeof entryCount !== "number" || !Number.isSafeInteger(entryCount) || entryCount < 0) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", `${what} entry_count must be a non-negative safe integer`);
  }
  if (value["copy_policy_version"] !== PIPELINE_V2_PROJECT_COPY_POLICY_VERSION) {
    throw new PipelineV2ProjectProvenanceError(
      "invalid_manifest",
      `${what} copy_policy_version ${JSON.stringify(value["copy_policy_version"])} is not the supported copy policy version`,
    );
  }
  return Object.freeze({
    digest,
    entry_count: entryCount,
    copy_policy_version: PIPELINE_V2_PROJECT_COPY_POLICY_VERSION,
  });
}

/**
 * Parses one stored manifest document (canonical JSON bytes as text) into
 * the prepared snapshot. The parser runs the same validation chain as the
 * preparer and additionally requires the stored bytes to equal the
 * manifest's own canonical JSON — a non-canonical stored document fails
 * closed. Malformed JSON is a stable content-free diagnostic without
 * parser fragments.
 */
export function parsePipelineV2ProjectProvenance(raw: string): PreparedPipelineV2ProjectProvenance {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the project provenance manifest is not valid JSON");
  }
  if (!isPlainObject(value)) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the project provenance manifest is not an object");
  }
  expectExactKeys(value, "the project provenance manifest", "invalid_manifest", ["schema_version", "kind", "copy_policy_version", "entries"]);
  if (value["schema_version"] !== PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION) {
    throw new PipelineV2ProjectProvenanceError(
      "invalid_manifest",
      `the project provenance manifest has an unsupported schema_version ${JSON.stringify(value["schema_version"])}`,
    );
  }
  if (value["kind"] !== PIPELINE_V2_PROJECT_PROVENANCE_KIND) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the project provenance manifest has an unsupported kind");
  }
  if (value["copy_policy_version"] !== PIPELINE_V2_PROJECT_COPY_POLICY_VERSION) {
    throw new PipelineV2ProjectProvenanceError(
      "invalid_manifest",
      `the project provenance manifest copy_policy_version ${JSON.stringify(value["copy_policy_version"])} is not the supported copy policy version`,
    );
  }
  if (!Array.isArray(value["entries"])) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the project provenance manifest entries must be an array");
  }
  const prepared = preparePipelineV2ProjectProvenance(value["entries"] as unknown[]);
  if (prepared.canonical_json !== raw) {
    throw new PipelineV2ProjectProvenanceError("invalid_manifest", "the project provenance manifest is not in canonical form");
  }
  return prepared;
}

/**
 * Confirms that one prepared manifest agrees with a state binding: the
 * digest, the entry count and the copy policy version must match exactly.
 */
export function requireProvenanceBindingMatch(
  prepared: PreparedPipelineV2ProjectProvenance,
  binding: PipelineV2ProjectProvenanceBinding,
): void {
  const expected = pipelineV2ProjectProvenanceBinding(prepared);
  if (
    expected.digest !== binding.digest ||
    expected.entry_count !== binding.entry_count ||
    expected.copy_policy_version !== binding.copy_policy_version
  ) {
    throw new PipelineV2ProjectProvenanceError(
      "invalid_manifest",
      "the durable project provenance binding does not match the stored provenance manifest",
    );
  }
}
