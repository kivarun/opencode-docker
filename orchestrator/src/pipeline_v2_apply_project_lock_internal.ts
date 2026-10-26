import { closeSync, constants, fstatSync, openSync } from "node:fs";
import { dlopen, FFIType } from "bun:ffi";
import { isCleanAbsolutePath } from "./clean_path.ts";

/**
 * The single owner of the project-level apply lock: one kernel-owned
 * advisory exclusive nonblocking `flock` over an open file description of
 * the canonical registered project root directory itself.
 *
 * Mechanism (the same proven discipline as the run mutation lock): the
 * lock is `flock(fd, LOCK_EX | LOCK_NB)` on a descriptor opened
 * `O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`. There is no lock
 * artifact anywhere — no lock file inside the project, no chmod, no
 * sentinel — and the lock key is the project root's open file
 * description, so two apply operations over two different run ids but one
 * project serialize, while two different projects stay independent.
 *
 * The kernel releases the lock when the owning process exits or dies
 * (SIGKILL included): no stale-lock metadata, no TTL, no PID heuristic,
 * no recovery protocol. Contention is a typed busy result, never an error
 * classification. The held handle exposes the locked descriptor and its
 * fixed `dev`/`ino` identity so the writer can re-verify the root after
 * the lock and drive all fd-relative operations through the same
 * descriptor (the identity the lock protects is the identity it holds).
 *
 * Honest boundary: this is a cooperative lock. IDEs, git, or any host
 * writer that does not take it are not excluded, and the verify→mutation
 * window of a non-cooperative writer remains a documented limitation of
 * the whole apply design.
 */

/** `flock(2)` operation values (Linux). */
const LOCK_EX = 2;
const LOCK_NB = 4;
/** `EWOULDBLOCK`/`EAGAIN` on Linux: the nonblocking lock is contended. */
const EWOULDBLOCK = 11;
/** `O_CLOEXEC` — spelled numerically; the pinned type definitions omit it. */
const O_CLOEXEC_FLAG = 0o2000000;

export type ApplyProjectLockFailureReason = "root_invalid" | "lock_infrastructure";

export class PipelineV2ApplyProjectLockError extends Error {
  declare readonly reason: ApplyProjectLockFailureReason;

  constructor(reason: ApplyProjectLockFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ApplyProjectLockError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** The held project lock. `release()` is idempotent and best-effort. */
export interface PipelineV2ApplyProjectLock {
  /** The locked project-root descriptor (root dirfd for fd-relative work). */
  readonly fd: number;
  /** The `dev`/`ino` identity fixed from the locked descriptor. */
  readonly identity: { readonly dev: number; readonly ino: number };
  release(): void;
}

export type PipelineV2ApplyProjectLockAcquisition =
  | { readonly busy: false; readonly lock: PipelineV2ApplyProjectLock }
  | { readonly busy: true };

interface ProjectLockLibc {
  flock(fd: number, operation: number): number;
  errnoLocation(): unknown;
  memcpy(destination: unknown, source: unknown, length: number): unknown;
}

let libcBinding: ProjectLockLibc | null = null;

function projectLockLibc(): ProjectLockLibc {
  if (libcBinding !== null) {
    return libcBinding;
  }
  const symbols = {
    flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
    __errno_location: { args: [], returns: FFIType.ptr },
    memcpy: { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.ptr },
  } as const;
  const muslArch = process.arch === "arm64" ? "aarch64" : "x86_64";
  const candidates = [
    "libc.so.6",
    "libc.so",
    `/lib/ld-musl-${muslArch}.so.1`,
    `/lib/libc.musl-${muslArch}.so.1`,
  ];
  let lastFailure: unknown = null;
  for (const candidate of candidates) {
    try {
      const library = dlopen(candidate, symbols);
      const record = library.symbols as unknown as Record<string, unknown>;
      const flock = record["flock"];
      const errnoLocation = record["__errno_location"];
      const memcpy = record["memcpy"];
      if (typeof flock !== "function" || typeof errnoLocation !== "function" || typeof memcpy !== "function") {
        throw new Error(`libc candidate ${candidate} is missing the required symbols`);
      }
      libcBinding = {
        flock: flock as ProjectLockLibc["flock"],
        errnoLocation: errnoLocation as ProjectLockLibc["errnoLocation"],
        memcpy: memcpy as ProjectLockLibc["memcpy"],
      };
      return libcBinding;
    } catch (cause) {
      lastFailure = cause;
    }
  }
  const detail = lastFailure instanceof Error ? lastFailure.message : String(lastFailure ?? "");
  throw new PipelineV2ApplyProjectLockError(
    "lock_infrastructure",
    `the project apply lock could not bind libc (${detail})`,
  );
}

function readErrno(libc: ProjectLockLibc): number {
  const buffer = new Uint8Array(8);
  libc.memcpy(buffer, libc.errnoLocation(), 4);
  return new DataView(buffer.buffer).getInt32(0, true);
}

/**
 * Acquires the project apply lock nonblocking. The project root must be a
 * clean absolute path; the descriptor is opened with `O_NOFOLLOW`, so a
 * symlinked root is refused as `root_invalid`. Contention reports
 * `busy: true` and touches nothing. Fully synchronous — the caller holds
 * the lock from this moment to its single release with no await between.
 */
export function acquirePipelineV2ApplyProjectLock(projectRoot: string): PipelineV2ApplyProjectLockAcquisition {
  if (typeof projectRoot !== "string" || !isCleanAbsolutePath(projectRoot)) {
    throw new PipelineV2ApplyProjectLockError("root_invalid", "the project root must be a clean absolute path");
  }
  const libc = projectLockLibc();
  let fd: number;
  try {
    fd = openSync(
      projectRoot,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW | O_CLOEXEC_FLAG,
    );
  } catch {
    throw new PipelineV2ApplyProjectLockError(
      "root_invalid",
      "the project root could not be opened as a real directory",
    );
  }
  let acquired = false;
  try {
    const result = libc.flock(fd, LOCK_EX | LOCK_NB);
    if (result === 0) {
      acquired = true;
    } else {
      const errno = readErrno(libc);
      if (errno !== EWOULDBLOCK) {
        throw new PipelineV2ApplyProjectLockError(
          "lock_infrastructure",
          `the kernel project lock could not be taken (errno ${errno})`,
        );
      }
      return { busy: true };
    }
  } finally {
    if (!acquired) {
      try {
        closeSync(fd);
      } catch {
        // best effort: the kernel releases it at process death anyway
      }
    }
  }
  const identity = (() => {
    try {
      const stat = fstatSync(fd);
      return { dev: stat.dev, ino: stat.ino };
    } catch {
      throw new PipelineV2ApplyProjectLockError("lock_infrastructure", "the locked project root could not be inspected");
    }
  })();
  let released = false;
  const lock: PipelineV2ApplyProjectLock = Object.freeze({
    fd,
    identity,
    release(): void {
      if (released) {
        return;
      }
      released = true;
      try {
        closeSync(fd);
      } catch {
        // best effort: the kernel releases it at process death anyway
      }
    },
  });
  return { busy: false, lock };
}
