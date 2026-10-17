import { isAbsolute } from "node:path";
import { type Stats } from "node:fs";
import { lstat, realpath } from "node:fs/promises";
import { describeError } from "./docker_helper.ts";
import { PipelineError } from "./pipeline.ts";

/**
 * Shared real-filesystem checks used by the pipeline v2 data plane and the
 * internal project-copy core. They are deliberately small and neutral: no
 * pipeline, session, Docker or credential semantics live here.
 */

/**
 * Whether a caught cause carries exactly the expected system errno code.
 * Reading `code` is getter-safe: a throwing getter (or a Proxy trap) can
 * never replace the caller's original failure or leak its own text; the
 * answer is simply "not this code".
 */
export function isErrnoException(cause: unknown, code: string): boolean {
  if (typeof cause !== "object" || cause === null) {
    return false;
  }
  try {
    return (cause as NodeJS.ErrnoException).code === code;
  } catch {
    return false;
  }
}

/**
 * Wrap an unexpected filesystem failure into a `PipelineError` whose
 * message appends the system's own description. Callers that must never
 * leak system text build sanitized diagnostics instead.
 */
export function fail(what: string, cause?: unknown): PipelineError {
  if (cause === undefined) {
    return new PipelineError(what);
  }
  return new PipelineError(`${what}: ${describeError(cause)}`);
}

/** Human-safe kind description of an inspected filesystem object. */
export function describeEntry(info: Stats): string {
  return info.isSymbolicLink()
    ? "a symbolic link"
    : info.isDirectory()
      ? "an existing directory"
      : info.isFile()
        ? "an existing regular file"
        : "an unexpected object";
}

/** `lstat` with ENOENT mapped to absence; every other failure is typed. */
export async function lstatOrNull(path: string): Promise<Stats | null> {
  try {
    return await lstat(path);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      return null;
    }
    throw fail(`filesystem entry ${path} cannot be inspected`, cause);
  }
}

/**
 * Ensure a real non-symlink directory at `path` (created 0700 when absent).
 * An existing object that is not a real directory is rejected; creation is
 * exclusive, so a concurrent creator loses instead of being adopted.
 */
export async function requireRealDirectory(path: string, what: string): Promise<void> {
  const info = await lstatOrNull(path);
  if (info === null) {
    throw new PipelineError(`${what} ${path} does not exist`);
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new PipelineError(`${what} ${path} exists but is ${describeEntry(info)}`);
  }
}

/**
 * Canonicalize an absolute run root that must be a real directory. Symlinked
 * ancestors resolve through realpath (the returned canonical path is used
 * exclusively); a symlinked final component is rejected.
 */
export async function requireCanonicalRunRoot(runRoot: string, what: string): Promise<string> {
  if (!isAbsolute(runRoot)) {
    throw new PipelineError(`${what} must be an absolute path, got ${JSON.stringify(runRoot)}`);
  }
  await requireRealDirectory(runRoot, what);
  try {
    return await realpath(runRoot);
  } catch (cause) {
    throw fail(`${what} ${runRoot} cannot be canonicalized`, cause);
  }
}

/** Strict prefix containment below a canonical root (no sibling prefixes). */
export function isInsideRoot(rootCanonical: string, canonicalPath: string): boolean {
  return canonicalPath.startsWith(`${rootCanonical}/`);
}
