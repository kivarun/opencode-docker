/**
 * Neutral filesystem primitives for the trusted path-pair projection
 * contract shared by the pipeline v2 runner (state-root and run-root
 * ownership) and the Docker Helper runtime adapter (daemon-visible run-root
 * projection). This module owns the single implementation of the actual
 * checks — lstat, real non-symlink kind verification and canonical
 * self-resolution through realpath — and exposes them as pure inspect
 * results. It never throws for an expected divergence and never builds
 * layer diagnostics: each caller maps a failure discriminant onto its own
 * typed error and message text. No layer owns a second copy of these
 * checks.
 */
import { lstat, realpath } from "node:fs/promises";
import { relative } from "node:path";

export type ProjectionObjectKind = "directory" | "file";

/** dev/ino identity of one filesystem object. */
export interface ProjectionIdentity {
  readonly dev: number;
  readonly ino: number;
}

/**
 * Discriminated failure of one object inspection. `symlink` and
 * `wrong_kind` are kept apart so callers can render their existing
 * diagnostics without re-inspecting; `resolves_elsewhere` carries the
 * canonical path the object actually resolved to.
 */
export type ProjectionInspectFailure =
  | "missing"
  | "symlink"
  | "wrong_kind"
  | "cannot_resolve"
  | "resolves_elsewhere";

export interface ProjectionInspectResult {
  /** null exactly when the object passed every check. */
  readonly failure: ProjectionInspectFailure | null;
  /** dev/ino identity, present only on success. */
  readonly identity: ProjectionIdentity | null;
  /** The canonical path when the object resolved elsewhere. */
  readonly resolved: string | null;
}

/**
 * Inspect one path for the exact expected object kind: it must exist, be a
 * real non-symlink object of the expected kind, and resolve canonically to
 * itself (`realpath(path) === path`), which also rejects a symlinked
 * parent component (a redirected tree whose descendants otherwise look
 * normal fails here).
 */
export async function inspectProjectionObject(
  path: string,
  expected: ProjectionObjectKind,
): Promise<ProjectionInspectResult> {
  let info;
  try {
    info = await lstat(path);
  } catch {
    return { failure: "missing", identity: null, resolved: null };
  }
  if (info.isSymbolicLink()) {
    return { failure: "symlink", identity: null, resolved: null };
  }
  if (expected === "directory" ? !info.isDirectory() : !info.isFile()) {
    return { failure: "wrong_kind", identity: null, resolved: null };
  }
  let resolved: string;
  try {
    resolved = await realpath(path);
  } catch {
    return { failure: "cannot_resolve", identity: null, resolved: null };
  }
  if (resolved !== path) {
    return { failure: "resolves_elsewhere", identity: null, resolved };
  }
  return { failure: null, identity: { dev: info.dev, ino: info.ino }, resolved: null };
}

/** Whether two inspected identities denote the same filesystem object. */
export function sameProjectionIdentity(
  left: ProjectionIdentity,
  right: ProjectionIdentity,
): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

/**
 * One side of a projection pair failed: which side, the object-level
 * failure discriminant and the resolved path when it resolved elsewhere.
 */
export interface ProjectionPairSideFailure {
  readonly side: "local" | "daemon";
  readonly failure: ProjectionInspectFailure;
  readonly resolved: string | null;
}

/** The two sides exist with the expected kinds but denote different objects. */
export interface ProjectionPairIdentityFailure {
  readonly side: null;
  readonly failure: "identity_mismatch";
  readonly resolved: null;
}

export type ProjectionPairFailure =
  | ProjectionPairSideFailure
  | ProjectionPairIdentityFailure;

/**
 * Inspect one projection pair — the same logical object under both roots:
 * exactly the expected kind on both sides, canonically itself on both
 * sides, and identical dev/ino across the pair (same-kind substitutions —
 * a symlinked pair, a swapped file/directory pair, a FIFO/socket pair —
 * fail). Returns null when the pair is proven.
 */
export async function inspectProjectionPair(
  localPath: string,
  daemonPath: string,
  expected: ProjectionObjectKind,
): Promise<ProjectionPairFailure | null> {
  const local = await inspectProjectionObject(localPath, expected);
  if (local.failure !== null) {
    return { side: "local", failure: local.failure, resolved: local.resolved };
  }
  const daemon = await inspectProjectionObject(daemonPath, expected);
  if (daemon.failure !== null) {
    return { side: "daemon", failure: daemon.failure, resolved: daemon.resolved };
  }
  if (!sameProjectionIdentity(local.identity!, daemon.identity!)) {
    return { side: null, failure: "identity_mismatch", resolved: null };
  }
  return null;
}

/** Translation succeeded; the daemon-side absolute path. */
export interface ProjectionTranslation {
  readonly ok: true;
  readonly daemonPath: string;
}

/** Translation refused: the relative suffix is not clean. */
export interface ProjectionTranslationRefused {
  ok: false;
  suffix: string;
}

export type ProjectionTranslationResult =
  | ProjectionTranslation
  | ProjectionTranslationRefused;

/**
 * Translate one orchestrator-side canonical path into the daemon
 * namespace: strictly `daemonRoot + relative(localRoot, localPath)`. The
 * relative suffix must be clean — non-empty segments only, no `.` or
 * `..`. Arbitrary per-path mappings do not exist.
 */
export function translateProjectionPath(
  localRoot: string,
  daemonRoot: string,
  localPath: string,
): ProjectionTranslationResult {
  const suffix = relative(localRoot, localPath);
  if (suffix === "") {
    return { ok: true, daemonPath: daemonRoot };
  }
  const segments = suffix.split("/");
  if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
    return { ok: false, suffix };
  }
  return { ok: true, daemonPath: `${daemonRoot}/${suffix}` };
}
