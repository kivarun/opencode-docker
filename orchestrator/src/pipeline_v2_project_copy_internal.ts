import { isAbsolute, join } from "node:path";
import { constants, type Stats } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readlink,
  readdir,
  realpath,
  rename,
  rm,
  symlink,
  type FileHandle,
} from "node:fs/promises";
import { PipelineError } from "./pipeline.ts";
import type { PipelineV2ApplyProjectLock } from "./pipeline_v2_apply_project_lock_internal.ts";
import {
  describeEntry,
  isErrnoException,
  isInsideRoot,
  requireCanonicalRunRoot,
} from "./fs_checks.ts";
import {
  PipelineV2RuntimeError,
  type PipelineV2RuntimeFailureReason,
} from "./pipeline_v2_runtime_error.ts";

/**
 * Internal per-call core of the run-owned project copy.
 *
 * This module is explicitly internal: it is not part of the public
 * data-plane API of `pipeline_v2_runtime.ts`. The production wrapper
 * `prepareRunProject(projectSourcePath, runRoot)` lives there and always
 * calls `prepareProjectCopy` with the single fixed, immutable
 * `realProjectCopyIo` defined here; tests call the same core with their own
 * per-call IO object. The IO is a per-call capability: it is passed as an
 * argument through the whole call chain and there is no mutable
 * module-global IO and no installer — a fault-injected test call can never
 * change the behavior of any parallel production call.
 *
 * Every fault-injectable filesystem operation of the copy stream and of the
 * staging lifecycle is a hook of `ProjectCopyIo`; the real implementation
 * performs exactly the node filesystem call named by the hook. All other
 * filesystem operations (exclusive `mkdir` of the staging directory, source
 * scans, opens, reads, closes, fsyncs, renames, publication) stay on the
 * real filesystem in every configuration: the seam exists to make
 * deterministic failure-order tests possible, never to let tests replace
 * the copy logic itself.
 */

/** Stable failure reason of every expected failure of the copy core. */
const COPY_FAILURE_REASON: PipelineV2RuntimeFailureReason = "run_input_invalid";

/**
 * Module-private retag boundary of the copy core: the whole operation
 * carries the single stable reason `run_input_invalid` (the project source
 * is a run-level input). Converts only its own `PipelineError` diagnostics,
 * preserving the message byte-for-byte; already-typed failures keep their
 * reason; exceptions that are not `PipelineError`s propagate unchanged.
 */
async function withCopyReason<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2RuntimeError) {
      throw cause;
    }
    if (cause instanceof PipelineError) {
      throw new PipelineV2RuntimeError(COPY_FAILURE_REASON, cause.message);
    }
    throw cause;
  }
}

/**
 * The errno code of a filesystem failure, rendered separately and without
 * the system message, so no path or payload text can leak into a project
 * copy diagnostic. Only the limited system-code form
 * `[A-Z][A-Z0-9_]{0,31}` is accepted — anything else (absolute paths,
 * spaces, newlines, canary words, lowercase, oversized codes, non-strings)
 * renders no suffix at all. Reading `cause.code` is getter-safe: a throwing
 * getter never replaces the original failure and never leaks its own text.
 */
const ERRNO_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,31}$/;

function errnoSuffix(cause: unknown): string {
  if (typeof cause !== "object" || cause === null) {
    return "";
  }
  let code: unknown;
  try {
    code = (cause as { readonly code?: unknown }).code;
  } catch {
    return "";
  }
  if (typeof code !== "string" || !ERRNO_CODE_PATTERN.test(code)) {
    return "";
  }
  return ` (errno ${code})`;
}

/**
 * A run-level project-copy failure whose diagnostic carries only the safe
 * operation class and an optional errno code — never an absolute source
 * path, a file body or a raw system message.
 */
function projectCopyFailure(what: string, cause?: unknown): PipelineError {
  return new PipelineError(cause === undefined ? what : `${what}${errnoSuffix(cause)}`);
}

/**
 * A project-copy failure for one source entry, named only by its
 * `JSON.stringify`-encoded relative path and a safe operation class.
 */
function projectEntryFailure(
  what: string,
  relativePath: string,
  cause?: unknown,
): PipelineError {
  const suffix = cause === undefined ? "" : errnoSuffix(cause);
  return new PipelineError(`${what} ${JSON.stringify(relativePath)}${suffix}`);
}

/**
 * lstat for the project copy: ENOENT is absence; every other failure is a
 * sanitized run-level input failure that never embeds an absolute path.
 */
async function projectCopyLstatOrNull(
  path: string,
  what: string,
  relativePath?: string,
): Promise<Stats | null> {
  try {
    return await lstat(path);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      return null;
    }
    if (relativePath === undefined) {
      throw projectCopyFailure(what, cause);
    }
    throw projectEntryFailure(what, relativePath, cause);
  }
}

const PROJECT_ROOT_NAME = "project";
const PROJECT_STAGING_PREFIX = ".project-staging-";
const MAX_STAGING_ATTEMPTS = 8;
const PROJECT_COPY_CHUNK = 128 * 1024;

/**
 * Per-call filesystem capability of the copy core: the production wrapper
 * always passes the fixed immutable `realProjectCopyIo`; tests inject their
 * own object per call. No hook ever reads or stores module-global state.
 */
export interface ProjectCopyIo {
  /** One write call on one destination chunk of the copy stream. */
  readonly destinationWrite: (
    handle: FileHandle,
    chunk: Buffer,
    offset: number,
    length: number,
    position: number,
  ) => Promise<number>;
  /**
   * Primary identity probe of one freshly created staging directory
   * (lstat semantics: ENOENT is absence). Its outcome is the ownership
   * proof every later staging step re-checks; without it nothing is ever
   * removed.
   */
  readonly stagingIdentityInspect: (path: string) => Promise<Stats | null>;
  /** The post-creation mode fix of the staging directory. */
  readonly stagingChmod: (path: string, mode: number) => Promise<void>;
  /** Identity probe of the staging tree before cleanup (lstat semantics). */
  readonly stagingInspect: (path: string) => Promise<Stats | null>;
  /** Canonical resolution of the staging tree before cleanup. */
  readonly stagingRealpath: (path: string) => Promise<string>;
  /** The single recursive removal of one owned staging tree. */
  readonly stagingRm: (
    path: string,
    options: { recursive: boolean; force: boolean },
  ) => Promise<void>;
}

/** The fixed immutable real-filesystem implementation of `ProjectCopyIo`. */
const realProjectCopyIoImplementation: ProjectCopyIo = {
  destinationWrite: async (handle, chunk, offset, length, position) => {
    const result = await handle.write(chunk, offset, length, position);
    return result.bytesWritten;
  },
  stagingIdentityInspect: async (path) => {
    try {
      return await lstat(path);
    } catch (cause) {
      if (isErrnoException(cause, "ENOENT")) {
        return null;
      }
      throw cause;
    }
  },
  stagingChmod: (path, mode) => chmod(path, mode),
  stagingInspect: async (path) => {
    try {
      return await lstat(path);
    } catch (cause) {
      if (isErrnoException(cause, "ENOENT")) {
        return null;
      }
      throw cause;
    }
  },
  stagingRealpath: (path) => realpath(path),
  stagingRm: (path, options) => rm(path, options),
};

/**
 * The single production IO of the copy core: fixed at module load,
 * deep-immutable, and never replaced — the wrapper passes exactly this
 * object on every call.
 */
export const realProjectCopyIo: ProjectCopyIo = Object.freeze(
  realProjectCopyIoImplementation,
);

/**
 * Run-owned project copy metadata. The source path is deliberately not
 * part of the object: after preparation the runtime works only inside the
 * orchestrator-owned copy and never reads or names the user source again.
 */
export interface PreparedRunProject {
  readonly run_root: string;
  readonly project_root: string;
}

/**
 * One copy-time provenance capture record of the single project copy
 * traversal. The record describes exactly the object this traversal read
 * and copied: its source permission mode (the scanned `lstat`), the
 * normalized workspace permission mode, and — for regular files — the
 * byte count and the SHA-256 of the exact copied bytes plus the source
 * hardlink characteristic. `dev`/`ino` are used only as ephemeral
 * copy-time identity to derive the canonical per-manifest hardlink groups
 * and are never serialized into the durable manifest. The records are
 * consumed by the provenance manifest builder; this module never
 * publishes or stores them itself.
 */
export interface ProjectCopyCaptureEntry {
  readonly path: string;
  readonly kind: "directory" | "regular_file" | "symlink";
  readonly source_mode?: number;
  readonly workspace_mode?: number;
  readonly byte_count?: number;
  readonly sha256?: string;
  readonly multiple_links?: boolean;
  readonly target?: string;
  /**
   * Copy-time confirmation that the created workspace symlink's target
   * text equals the source link target byte for byte (symlink records
   * only).
   */
  readonly targets_identical?: true;
  /**
   * Ephemeral copy-time identity of the source object, used only to
   * derive the canonical hardlink groups and stripped from every record
   * before the capture leaves this module. Never serialized.
   */
  source_identity_dev?: number;
  source_identity_ino?: number;
}

/**
 * One staged, fully copied but not yet published project tree: the
 * staging directory with its ownership identity and the capture records
 * of the single traversal that filled it. The staging tree is owned by
 * the holder of this object until it is published or discarded.
 */
export interface StagedProjectCopy {
  readonly run_root: string;
  readonly staging_path: string;
  readonly staging_identity: { readonly dev: number; readonly ino: number };
  readonly entries: readonly ProjectCopyCaptureEntry[];
}

interface ValidatedProjectCopyInputs {
  readonly runRootCanonical: string;
  readonly projectPath: string;
  readonly sourceCanonical: string;
  readonly sourceIdentity: { readonly dev: number; readonly ino: number };
}

/**
 * The shared validation prefix of every project-copy entry point: the
 * run root must be an existing absolute canonical real non-symlink
 * directory, `<runRoot>/project` must be absent, the source must be an
 * absolute real non-symlink directory, and source and run root must not
 * overlap in either direction. When the caller holds the cooperative
 * shared project lock, the scanned source identity is proven here —
 * immediately after the source fixation and before the copy traversal —
 * to be exactly the directory the lock was taken on, so a directory
 * replaced between lock acquisition and fixation can never be scanned
 * under the wrong lock.
 */
async function validateProjectCopyInputs(
  projectSourcePath: string,
  runRoot: string,
  lock?: PipelineV2ApplyProjectLock,
): Promise<ValidatedProjectCopyInputs> {
  if (typeof projectSourcePath !== "string" || projectSourcePath === "") {
    throw new PipelineError("project source path must be a non-empty string");
  }
  if (!isAbsolute(projectSourcePath)) {
    throw new PipelineError(
      "project source path must be an absolute path, got a relative path",
    );
  }
  if (!isAbsolute(runRoot)) {
    throw new PipelineError("run root must be an absolute path");
  }
  const runRootCanonical = await requireCanonicalRunRoot(runRoot, "run root");
  if (runRootCanonical !== runRoot) {
    throw new PipelineError(
      `run root ${runRoot} is not canonical; pass the canonical path ${runRootCanonical}`,
    );
  }

  const projectPath = join(runRootCanonical, PROJECT_ROOT_NAME);
  const existingProject = await projectCopyLstatOrNull(
    projectPath,
    "run project root could not be inspected",
  );
  if (existingProject !== null) {
    throw new PipelineError(
      `run project root already exists at the fixed run-root location and is ${describeEntry(existingProject)}`,
    );
  }

  const sourceInfo = await projectCopyLstatOrNull(
    projectSourcePath,
    "project source could not be inspected",
  );
  if (sourceInfo === null) {
    throw new PipelineError("project source does not exist");
  }
  if (sourceInfo.isSymbolicLink() || !sourceInfo.isDirectory()) {
    throw new PipelineError(
      `project source exists but is ${describeEntry(sourceInfo)}; pass a real non-symlink directory`,
    );
  }
  let sourceCanonical: string;
  try {
    sourceCanonical = await realpath(projectSourcePath);
  } catch (cause) {
    throw projectCopyFailure("project source cannot be canonicalized", cause);
  }
  if (
    sourceCanonical === runRootCanonical ||
    isInsideRoot(runRootCanonical, sourceCanonical) ||
    isInsideRoot(sourceCanonical, runRootCanonical)
  ) {
    throw new PipelineError(
      "project source and the canonical run root must not overlap in either direction",
    );
  }
  const sourceIdentity = Object.freeze({ dev: sourceInfo.dev, ino: sourceInfo.ino });
  if (lock !== undefined) {
    if (lock.identity.dev !== sourceIdentity.dev || lock.identity.ino !== sourceIdentity.ino) {
      throw new PipelineError(
        "the held project lock does not cover the scanned project source directory",
      );
    }
  }
  return { runRootCanonical, projectPath, sourceCanonical, sourceIdentity };
}

/**
 * Prepare the run-owned shared project directory `<runRoot>/project` as
 * an orchestrator-owned copy of the user's project source directory.
 *
 * Ownership: the caller (the user today, a future API or SCM provider
 * tomorrow) hands over only the source directory path. The source is never
 * modified, renamed, cleared or read beyond this copy; agents work only on
 * the run-owned copy. The source path is not part of the returned object,
 * never enters the execution document, worker env/argv, durable state or
 * results, and the internal run-root layout is not presented to the user.
 *
 * Validation, fail-closed: `runRoot` must be an existing absolute canonical
 * real non-symlink directory (`realpath(runRoot) === runRoot`) and is never
 * created or removed by this function; `<runRoot>/project` must not exist
 * as an object of any kind. The source must be an absolute real non-symlink
 * directory whose canonical path is fixed before copying; source and run
 * root may not overlap in either direction. Repeated calls fail closed and
 * never touch an existing project.
 *
 * Staging creation order (fail-closed): exclusive `mkdir` first; then the
 * primary identity of the created real non-symlink directory is fixed
 * immediately (the `dev`/`ino` ownership proof); only afterwards run the
 * fallible post-creation checks (`chmod`). Once the identity is fixed, any
 * subsequent failure runs the ownership-checked cleanup of exactly this
 * staging tree, and a cleanup failure never replaces the original error.
 * If the primary identity fixation itself failed, a recursive removal
 * without an ownership proof is forbidden: the staging directory may stay
 * behind (the documented absence of crash recovery) and the original
 * sanitized failure is surfaced.
 *
 * Copy contract (current, explicit): real directories (created 0700),
 * regular files (0600, or 0700 when the source carries any execute bit),
 * symlinks copied as symlinks through `readlink` — the target text is
 * copied verbatim and never resolved — hidden entries (including `.git`)
 * and empty directories, in deterministic relative-path code-unit sorted
 * order. uid/gid, timestamps, xattrs, ACLs and hardlink identity are not
 * preserved: hardlinks become independent regular files. Sources are
 * opened `O_NOFOLLOW|O_NONBLOCK` and destinations `O_CREAT|O_EXCL|O_NOFOLLOW`;
 * each block is fully written by an internal write-all loop (a partial
 * write advances the buffer offset and the file position; a zero-progress
 * or impossible write count fails the operation), streamed without shell,
 * `cp` or `tar`, and fsynced before publication. Close errors never
 * replace an already failing copy; a close error after an otherwise
 * successful copy is the typed failure itself. FIFOs, unix sockets,
 * devices and unknown kinds are rejected, as is an object that changed
 * kind or inode between scan and open. Absolute source paths, absolute
 * paths of their descendants, file contents and raw system error messages
 * never enter diagnostics: source entries are named only by their
 * `JSON.stringify`-encoded relative path and a safe operation class, and
 * errno codes are rendered separately.
 *
 * Publication: the tree is staged in an exclusive staging directory inside
 * the canonical run root and published with one `rename` after a fresh
 * absence re-check of `<runRoot>/project`. Any failure before the rename
 * removes exactly the created staging tree — and only after an ownership
 * proof succeeds: the recorded `dev`/`ino` must still identify a real
 * non-symlink directory canonically resolving to the expected path inside
 * the canonical run root; a vanished tree is left alone, a substituted
 * object is never removed, and a cleanup failure never replaces the
 * original operation failure. Existing objects and the source stay
 * byte-identical. After the rename the copy is authoritative and is never
 * removed by this function, even on later run failures.
 *
 * Honest boundaries: the portable `rename()` can replace a concurrently
 * created empty directory at the target; there is no protection against a
 * trusted host process mutating the source while it is being read, and
 * there is no crash recovery.
 *
 * Every expected failure of this operation is a run-level input failure:
 * a `PipelineV2RuntimeError` with reason `run_input_invalid` and a
 * content-free diagnostic. No new failure reason is introduced and the
 * state schema stays v4. This is a convenience wrapper, not the
 * production boundary: the production fresh-run path is
 * `prepareRunProject`, which owns the cooperative shared project lock for
 * the whole fixation→copy→provenance→publication→verification boundary.
 * A caller that already holds the shared lock passes it here so the
 * staged copy is proven to belong to the locked directory.
 */
export async function prepareProjectCopy(
  io: ProjectCopyIo,
  projectSourcePath: string,
  runRoot: string,
  lock?: PipelineV2ApplyProjectLock,
): Promise<PreparedRunProject> {
  return await withCopyReason(async () => {
    const staged = await stageProjectCopy(io, projectSourcePath, runRoot, lock);
    try {
      await publishStagedProjectCopy(io, staged);
    } catch (cause) {
      await discardStagedProjectCopy(io, staged);
      throw cause;
    }
    const projectPath = join(staged.run_root, PROJECT_ROOT_NAME);
    return Object.freeze({ run_root: staged.run_root, project_root: projectPath });
  });
}

/**
 * Stage the run-owned project copy without publishing it: validation
 * prefix, exclusive staging directory, and the single copy traversal that
 * both fills the staging tree and records the provenance capture of
 * exactly the objects and bytes it copied. Every failure after the
 * staging identity was fixed cleans exactly the own staging tree; the
 * source and every existing object stay untouched. The provenance capture
 * is a by-product of this traversal — there is no second source scan.
 */
export async function stageProjectCopy(
  io: ProjectCopyIo,
  projectSourcePath: string,
  runRoot: string,
  lock?: PipelineV2ApplyProjectLock,
): Promise<StagedProjectCopy> {
  return await withCopyReason(async () => {
    const { runRootCanonical, sourceCanonical } = await validateProjectCopyInputs(
      projectSourcePath,
      runRoot,
      lock,
    );
    const staging = await createProjectStagingDirectory(io, runRootCanonical);
    const capture: ProjectCopyCaptureEntry[] = [];
    try {
      await copyProjectTree(io, sourceCanonical, staging.path, capture);
    } catch (cause) {
      await removeProjectStagingTree(io, staging.path, staging, runRootCanonical);
      throw cause;
    }
    finalizeCaptureHardlinkGroups(capture);
    return Object.freeze({
      run_root: runRootCanonical,
      staging_path: staging.path,
      staging_identity: Object.freeze({ dev: staging.dev, ino: staging.ino }),
      entries: Object.freeze(capture.map((entry) => Object.freeze({ ...entry }))),
    });
  });
}

/**
 * Publish one staged project copy: a fresh absence re-check of
 * `<runRoot>/project` immediately before the atomic rename, then exactly
 * one `rename()`. On a rename failure the staging tree is removed with
 * its ownership proof and the sanitized failure is surfaced; the staging
 * tree of a successful publication no longer exists.
 */
export async function publishStagedProjectCopy(
  io: ProjectCopyIo,
  staged: StagedProjectCopy,
): Promise<void> {
  return await withCopyReason(async () => {
    const projectPath = join(staged.run_root, PROJECT_ROOT_NAME);
    try {
      const beforeRename = await projectCopyLstatOrNull(
        projectPath,
        "run project root could not be inspected",
      );
      if (beforeRename !== null) {
        throw new PipelineError(
          `run project root appeared at the fixed run-root location during preparation and is ${describeEntry(beforeRename)}`,
        );
      }
      try {
        await rename(staged.staging_path, projectPath);
      } catch (cause) {
        throw projectCopyFailure("run project copy could not be published", cause);
      }
    } catch (cause) {
      await removeProjectStagingTree(
        io,
        staged.staging_path,
        staged.staging_identity,
        staged.run_root,
      );
      throw cause;
    }
  });
}

/**
 * Ownership-checked removal of one staged project copy that will not be
 * published (the caller's recovery path after a failure between staging
 * and publication). Best effort: a cleanup failure never replaces the
 * caller's original failure and a vanished or substituted tree is left
 * alone.
 */
export async function discardStagedProjectCopy(
  io: ProjectCopyIo,
  staged: StagedProjectCopy,
): Promise<void> {
  await removeProjectStagingTree(
    io,
    staged.staging_path,
    staged.staging_identity,
    staged.run_root,
  );
}

/**
 * One exclusively created staging directory with its recorded filesystem
 * identity (`dev`/`ino`): the identity is the ownership proof the cleanup
 * later re-checks before removing anything.
 */
interface ProjectStagingDirectory {
  readonly path: string;
  readonly dev: number;
  readonly ino: number;
}

/**
 * Create one exclusive staging directory inside the canonical run root,
 * fixing its filesystem identity before anything fallible happens:
 *
 * 1. exclusive `mkdir` (a concurrent creator loses instead of being
 *    adopted, retried under a fresh hidden name);
 * 2. immediate fixation of the created object's identity as a real
 *    non-symlink directory — the ownership proof for every later step;
 * 3. only then the fallible post-creation checks (`chmod`).
 *
 * Once the identity is fixed, any subsequent failure runs the
 * ownership-checked cleanup of exactly this staging directory before the
 * failure propagates; a cleanup failure never replaces the original error.
 * A failure of the primary identity fixation itself is the honest
 * boundary: without an ownership proof no recursive removal is attempted
 * (the staging directory may remain behind) and the original sanitized
 * error is surfaced. Every failure is a sanitized run-level input failure.
 */
async function createProjectStagingDirectory(
  io: ProjectCopyIo,
  runRootCanonical: string,
): Promise<ProjectStagingDirectory> {
  let lastCause: unknown = undefined;
  for (let attempt = 0; attempt < MAX_STAGING_ATTEMPTS; attempt += 1) {
    const name = `${PROJECT_STAGING_PREFIX}${randomBytes(8).toString("hex")}`;
    const path = join(runRootCanonical, name);
    try {
      await mkdir(path, { mode: 0o700 });
    } catch (cause) {
      if (isErrnoException(cause, "EEXIST")) {
        lastCause = cause;
        continue;
      }
      throw projectCopyFailure("staging directory could not be created", cause);
    }
    // 2. Immediate identity fixation: without a captured identity the
    //    recursive removal below would have no ownership proof, so a
    //    fixation failure never removes anything and leaves the staging
    //    directory behind.
    let info: Stats | null;
    try {
      info = await io.stagingIdentityInspect(path);
    } catch (cause) {
      throw projectCopyFailure(
        "staging directory could not be inspected after creation",
        cause,
      );
    }
    if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
      throw projectCopyFailure("staging directory is not a real directory after creation");
    }
    const identity = { dev: info.dev, ino: info.ino };
    try {
      // 3. Fallible post-creation checks run only after the identity is
      //    fixed; every failure here cleans the own staging tree.
      await io.stagingChmod(path, 0o700);
    } catch (cause) {
      // 4./5. Ownership-checked cleanup; a cleanup failure never replaces
      //    the original error.
      await removeProjectStagingTree(io, path, identity, runRootCanonical);
      throw projectCopyFailure("staging directory could not be created", cause);
    }
    return { path, dev: identity.dev, ino: identity.ino };
  }
  throw projectCopyFailure("exclusive staging directory could not be created", lastCause);
}

/**
 * Best-effort removal of exactly the staging tree this operation created.
 * The removal runs only after the ownership proof succeeds: the object at
 * the expected path must still be a real non-symlink directory with the
 * recorded `dev`/`ino`, resolving canonically to the expected path inside
 * the canonical run root. A vanished tree is left alone, a substituted
 * object is never removed, and without a confirmed canonical resolution
 * the recursive `rm` is skipped. Cleanup failures never replace the
 * original operation failure; a leftover staging tree stays the documented
 * absence of crash recovery.
 */
async function removeProjectStagingTree(
  io: ProjectCopyIo,
  stagingPath: string,
  identity: { readonly dev: number; readonly ino: number },
  runRootCanonical: string,
): Promise<void> {
  try {
    const info = await io.stagingInspect(stagingPath);
    if (info === null) {
      // The staging tree vanished; there is nothing to remove.
      return;
    }
    if (info.isSymbolicLink() || !info.isDirectory()) {
      // A substituted object is never removed.
      return;
    }
    if (info.dev !== identity.dev || info.ino !== identity.ino) {
      // A replaced directory is never removed.
      return;
    }
    let canonical: string;
    try {
      canonical = await io.stagingRealpath(stagingPath);
    } catch {
      // Without a confirmed canonical resolution the removal is skipped.
      return;
    }
    if (canonical !== stagingPath) {
      return;
    }
    if (canonical !== runRootCanonical && !isInsideRoot(runRootCanonical, canonical)) {
      return;
    }
    await io.stagingRm(stagingPath, { recursive: true, force: true });
  } catch {
    // Best-effort: a cleanup failure may leave the staging tree behind as
    // the documented absence of crash recovery; it never replaces the
    // original operation failure.
  }
}

/**
 * Copy the whole project source tree into the staging root: real
 * directories, regular files and symlinks (target text verbatim, never
 * resolved), including hidden entries and empty directories, in
 * deterministic code-unit sorted order. Any other object kind fails
 * closed. The same traversal records the provenance capture of exactly
 * the objects it reads and the bytes it copies — one traversal, one
 * capture, no second source scan.
 */
async function copyProjectTree(
  io: ProjectCopyIo,
  sourceCanonical: string,
  destinationRoot: string,
  capture: ProjectCopyCaptureEntry[],
): Promise<void> {
  const copyDirectory = async (sourceDir: string, relativeDir: string): Promise<void> => {
    let dirents;
    try {
      dirents = await readdir(sourceDir, { withFileTypes: true });
    } catch (cause) {
      throw projectCopyFailure("project source directory could not be listed", cause);
    }
    dirents.sort((left, right) =>
      left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
    );
    for (const dirent of dirents) {
      const childRelative = relativeDir === "" ? dirent.name : `${relativeDir}/${dirent.name}`;
      const childSource = join(sourceDir, dirent.name);
      const childDestination = join(destinationRoot, childRelative);
      // The scan-time kind comes from lstat, never from the dirent type,
      // so a replaced object is caught here and again after open.
      const info = await projectCopyLstatOrNull(
        childSource,
        "project source entry could not be inspected",
        childRelative,
      );
      if (info === null) {
        throw new PipelineError(
          `project source entry ${JSON.stringify(childRelative)} disappeared during the copy`,
        );
      }
      if (info.isSymbolicLink()) {
        let target: string;
        try {
          target = await readlink(childSource);
        } catch (cause) {
          throw projectEntryFailure(
            "project source symlink entry could not be read",
            childRelative,
            cause,
          );
        }
        try {
          await symlink(target, childDestination);
        } catch (cause) {
          throw projectEntryFailure(
            "project source symlink entry could not be copied",
            childRelative,
            cause,
          );
        }
        // Copy-time confirmation that the created workspace symlink's
        // target text equals the source link target byte for byte.
        let workspaceTarget: string;
        try {
          workspaceTarget = await readlink(childDestination);
        } catch (cause) {
          throw projectEntryFailure(
            "project source symlink entry could not be confirmed in the workspace",
            childRelative,
            cause,
          );
        }
        if (workspaceTarget !== target) {
          throw new PipelineError(
            `project source symlink entry ${JSON.stringify(childRelative)} was not copied with its verbatim target`,
          );
        }
        capture.push({
          path: childRelative,
          kind: "symlink",
          target,
          targets_identical: true,
        });
      } else if (info.isDirectory()) {
        try {
          await mkdir(childDestination, { mode: 0o700 });
          await chmod(childDestination, 0o700);
        } catch (cause) {
          throw projectEntryFailure(
            "project source directory entry could not be copied",
            childRelative,
            cause,
          );
        }
        capture.push({
          path: childRelative,
          kind: "directory",
          source_mode: info.mode & 0o777,
          workspace_mode: 0o700,
        });
        await copyDirectory(childSource, childRelative);
      } else if (info.isFile()) {
        capture.push(
          await copyProjectRegularFile(io, childSource, childDestination, childRelative, info),
        );
      } else {
        throw new PipelineError(
          `project source entry ${JSON.stringify(childRelative)} is a FIFO, socket, device or another unsupported object`,
        );
      }
    }
  };
  await copyDirectory(sourceCanonical, "");
}

/**
 * The canonical per-manifest hardlink groups: entries whose source objects
 * shared one copy-time inode identity are assigned one group index (1-based)
 * in deterministic sorted-entry order. The grouping is computed after the
 * traversal over the sorted capture, so identical copies always produce
 * identical group numbers. After the assignment the ephemeral source
 * identity fields are stripped from every record: they never leave this
 * module and are never serialized.
 */
function finalizeCaptureHardlinkGroups(capture: ProjectCopyCaptureEntry[]): void {
  const sorted = [...capture]
    .filter((entry) => entry.kind === "regular_file" && entry.multiple_links === true)
    .sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
  const groups = new Map<string, number>();
  const result = new Map<string, number>();
  let nextGroup = 1;
  for (const entry of sorted) {
    const key = `${entry.source_identity_dev}:${entry.source_identity_ino}`;
    let group = groups.get(key);
    if (group === undefined) {
      group = nextGroup;
      nextGroup += 1;
      groups.set(key, group);
    }
    result.set(entry.path, group);
  }
  for (const entry of capture) {
    const group = result.get(entry.path);
    const mutable = entry as {
      hardlink_group?: number;
      source_identity_dev?: number;
      source_identity_ino?: number;
    };
    if (group !== undefined) {
      mutable.hardlink_group = group;
    }
    delete mutable.source_identity_dev;
    delete mutable.source_identity_ino;
  }
}

/**
 * Stream one regular file into the exclusive destination: the source is
 * opened `O_NOFOLLOW|O_NONBLOCK` (a FIFO substituted between scan and open
 * cannot block the run), the open object must still be the scanned regular
 * file (same kind, same dev/ino, same permission mode and same content
 * timestamps — a source object touched between the scan and the open is
 * rejected), and the destination is created `O_CREAT|O_EXCL|O_NOFOLLOW`
 * with the contract mode. Every block is fully written with an internal
 * write-all loop: partial writes advance the buffer offset and the file
 * position, a zero-progress or impossible `bytesWritten` fails the
 * operation, and only fully written blocks advance the read position. The
 * SHA-256 of the exact copied bytes and the total byte count are captured
 * incrementally from the same read loop that fills the destination. After
 * the read the open object is re-statted: a source object whose size,
 * timestamps, mode or identity changed while it was being read fails the
 * operation closed — the copy must be a coherent read of one stable
 * object, and the provenance capture must describe exactly the bytes that
 * were copied. This is a best-effort coherence proof at millisecond
 * timestamp granularity, not a guarantee against an adversarial
 * same-millisecond same-size rewrite; a coherent copy is still never a
 * whole-tree atomic snapshot without project-level cooperation. Every
 * expected filesystem failure is a sanitized run-level input failure;
 * close errors never replace an already failing copy, and a close error
 * after an otherwise successful copy becomes the typed failure itself.
 * The returned capture record carries the source permission mode, the
 * normalized workspace permission mode, the byte count, the content
 * digest, the source hardlink characteristic and the ephemeral source
 * identity for the hardlink grouping.
 */
async function copyProjectRegularFile(
  io: ProjectCopyIo,
  sourcePath: string,
  destinationPath: string,
  childRelative: string,
  scannedInfo: Stats,
): Promise<ProjectCopyCaptureEntry> {
  const sourceMode = scannedInfo.mode & 0o777;
  const mode = (scannedInfo.mode & 0o111) !== 0 ? 0o700 : 0o600;
  let source: FileHandle;
  try {
    source = await open(
      sourcePath,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
  } catch (cause) {
    throw projectEntryFailure(
      "project source file entry could not be opened for reading",
      childRelative,
      cause,
    );
  }
  let sourceCloseFailure: unknown = undefined;
  let captureEntry: ProjectCopyCaptureEntry | undefined = undefined;
  try {
    const opened = await source.stat();
    if (opened.isSymbolicLink() || !opened.isFile()) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} changed kind between scan and open`,
      );
    }
    if (opened.dev !== scannedInfo.dev || opened.ino !== scannedInfo.ino) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} was replaced between scan and open`,
      );
    }
    if ((opened.mode & 0o777) !== sourceMode) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} changed permission mode between scan and open`,
      );
    }
    if (opened.nlink !== scannedInfo.nlink) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} changed its hardlink count between scan and open`,
      );
    }
    if (
      opened.size !== scannedInfo.size ||
      opened.mtimeMs !== scannedInfo.mtimeMs ||
      opened.ctimeMs !== scannedInfo.ctimeMs
    ) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} was modified between scan and open`,
      );
    }
    let destination: FileHandle;
    try {
      destination = await open(
        destinationPath,
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        mode,
      );
    } catch (cause) {
      throw projectEntryFailure(
        "project source file entry could not be opened for writing",
        childRelative,
        cause,
      );
    }
    const hasher = createHash("sha256");
    let byteCount = 0;
    let destinationCompleted = false;
    try {
      try {
        await destination.chmod(mode);
        const chunk = Buffer.allocUnsafe(PROJECT_COPY_CHUNK);
        let position = 0;
        for (;;) {
          const { bytesRead } = await source.read(chunk, 0, chunk.length, position);
          if (bytesRead === 0) {
            break;
          }
          hasher.update(chunk.subarray(0, bytesRead));
          byteCount += bytesRead;
          let written = 0;
          while (written < bytesRead) {
            const remaining = bytesRead - written;
            const bytesWritten = await io.destinationWrite(
              destination,
              chunk,
              written,
              remaining,
              position + written,
            );
            if (
              !Number.isSafeInteger(bytesWritten) ||
              bytesWritten <= 0 ||
              bytesWritten > remaining
            ) {
              throw projectEntryFailure(
                "project source file entry could not be written without progress",
                childRelative,
              );
            }
            written += bytesWritten;
          }
          position += bytesRead;
        }
        await destination.sync();
        destinationCompleted = true;
      } finally {
        try {
          await destination.close();
        } catch (closeCause) {
          if (destinationCompleted) {
            // An otherwise successful copy that cannot be closed becomes
            // the typed failure itself.
            throw projectEntryFailure(
              "project source file entry could not be closed after copying",
              childRelative,
              closeCause,
            );
          }
          // A close failure after an already failing copy is swallowed:
          // the original failure stays authoritative.
        }
      }
    } catch (cause) {
      if (cause instanceof PipelineError) {
        throw cause;
      }
      throw projectEntryFailure(
        "project source file entry could not be copied",
        childRelative,
        cause,
      );
    }
    // Copy-time coherence proof: the source object must be unchanged from
    // its opened identity through the whole read (identity, size,
    // permission mode, hardlink count and content timestamps). A source
    // object that was written while it was being read cannot be proven
    // coherent and fails the whole preparation closed.
    const after = await source.stat();
    if (
      after.dev !== opened.dev ||
      after.ino !== opened.ino ||
      after.isFile() !== true ||
      (after.mode & 0o777) !== sourceMode ||
      after.nlink !== opened.nlink ||
      after.size !== opened.size ||
      after.mtimeMs !== opened.mtimeMs ||
      after.ctimeMs !== opened.ctimeMs
    ) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} was modified while it was being read`,
      );
    }
    if (after.size !== byteCount) {
      throw new PipelineError(
        `project source entry ${JSON.stringify(childRelative)} was not read completely`,
      );
    }
    captureEntry = {
      path: childRelative,
      kind: "regular_file",
      source_mode: sourceMode,
      workspace_mode: mode,
      byte_count: byteCount,
      sha256: hasher.digest("hex"),
      multiple_links: opened.nlink > 1,
      source_identity_dev: opened.dev,
      source_identity_ino: opened.ino,
    };
  } catch (cause) {
    if (cause instanceof PipelineError) {
      throw cause;
    }
    throw projectEntryFailure(
      "project source file entry could not be copied",
      childRelative,
      cause,
    );
  } finally {
    try {
      await source.close();
    } catch (closeCause) {
      sourceCloseFailure = closeCause;
    }
  }
  if (sourceCloseFailure !== undefined) {
    // An otherwise successful copy whose source cannot be closed is still
    // a typed run-level input failure; a close failure after an already
    // failing copy never reaches this point (the original failure already
    // propagates from above).
    throw projectEntryFailure(
      "project source file entry could not be closed after reading",
      childRelative,
      sourceCloseFailure,
    );
  }
  if (captureEntry === undefined) {
    throw projectEntryFailure(
      "project source file entry could not be copied",
      childRelative,
    );
  }
  return captureEntry;
}
