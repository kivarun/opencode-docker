import { isAbsolute, join } from "node:path";
import { constants, type Stats } from "node:fs";
import { randomBytes } from "node:crypto";
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
 * state schema stays v4.
 */
export async function prepareProjectCopy(
  io: ProjectCopyIo,
  projectSourcePath: string,
  runRoot: string,
): Promise<PreparedRunProject> {
  return await withCopyReason(async () => {
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

    let staging: ProjectStagingDirectory | null = null;
    try {
      staging = await createProjectStagingDirectory(io, runRootCanonical);
      await copyProjectTree(io, sourceCanonical, staging.path);
      // Fresh absence re-check immediately before the atomic publication.
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
        await rename(staging.path, projectPath);
      } catch (cause) {
        throw projectCopyFailure("run project copy could not be published", cause);
      }
    } catch (cause) {
      if (staging !== null) {
        await removeProjectStagingTree(io, staging.path, staging, runRootCanonical);
      }
      throw cause;
    }
    return Object.freeze({ run_root: runRootCanonical, project_root: projectPath });
  });
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
 * closed.
 */
async function copyProjectTree(
  io: ProjectCopyIo,
  sourceCanonical: string,
  destinationRoot: string,
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
        await copyDirectory(childSource, childRelative);
      } else if (info.isFile()) {
        await copyProjectRegularFile(io, childSource, childDestination, childRelative, info);
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
 * Stream one regular file into the exclusive destination: the source is
 * opened `O_NOFOLLOW|O_NONBLOCK` (a FIFO substituted between scan and open
 * cannot block the run), the open object must still be the scanned regular
 * file (same kind, same dev/ino), and the destination is created
 * `O_CREAT|O_EXCL|O_NOFOLLOW` with the contract mode. Every block is fully
 * written with an internal write-all loop: partial writes advance the
 * buffer offset and the file position, a zero-progress or impossible
 * `bytesWritten` fails the operation, and only fully written blocks
 * advance the read position. Every expected filesystem failure is a
 * sanitized run-level input failure; close errors never replace an
 * already failing copy, and a close error after an otherwise successful
 * copy becomes the typed failure itself.
 */
async function copyProjectRegularFile(
  io: ProjectCopyIo,
  sourcePath: string,
  destinationPath: string,
  childRelative: string,
  scannedInfo: Stats,
): Promise<void> {
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
}
