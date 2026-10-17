/**
 * The single owner of the inter-process mutation lock of one pipeline v2
 * run: a kernel-owned advisory exclusive nonblocking lock over the run
 * root directory, held for the whole mutating command.
 *
 * Mechanism (verified on Bun 1.3.14 and 1.4.2, Linux only): the lock is a
 * `flock(fd, LOCK_EX | LOCK_NB)` advisory file lock taken on an open file
 * descriptor of the run root directory itself (`O_RDONLY | O_DIRECTORY |
 * O_NOFOLLOW | O_CLOEXEC`). There is no lock artifact anywhere: the locked
 * object is the already verified run root, so the run tree, the run layout
 * contract and `inspect` stay untouched, and the lock can never become a
 * source of truth about the existence of a run (only the kernel lock
 * arbitrates; the directory exists and is verified by the callers
 * independently).
 *
 * Kernel-owned automatic release: the lock lives exactly as long as the
 * open file description. It is released by `release()` (one `closeSync`),
 * by any process exit path, and by process death (SIGKILL included) — the
 * kernel closes the descriptors of a dying process, so a killed holder
 * frees the run for the next mutating command without any repair, unlock
 * or stale-cleanup step. There is no TTL, no PID check, no mtime
 * heuristic and no writable stale metadata anywhere in this design.
 *
 * Contention is a typed result, never an error classification: a
 * nonblocking `flock` that fails with `EWOULDBLOCK` (`errno 11` on Linux,
 * read through libc's `__errno_location` because Bun's FFI has no errno
 * surface of its own) reports `busy: true`. Every other failure of the
 * acquisition is a typed `PipelineV2RunMutationLockError`; no diagnostic
 * ever depends on message text.
 *
 * The runtime adapter for the libc symbols is a small frozen internal
 * binding: `flock`, `__errno_location` and `memcpy` resolved through
 * Bun's `dlopen` from the platform libc (the first resolvable candidate
 * of a fixed list — glibc's `libc.so.6`, then the musl names; the libc is
 * always present because the Bun binary itself is dynamically linked
 * against it). There is no mutable test seam, no second lock
 * implementation, no daemon, no network service and no in-process-only
 * lock registry; the per-run in-process serialization the sink already
 * provides is unchanged and now backed by the same kernel lock, because
 * two acquisitions in one process use two open file descriptions and
 * therefore also conflict.
 *
 * Mutating commands acquire the lock once, at their single acquisition
 * site in the runner (the fresh runner after the atomic run-root claim,
 * the shared existing-run core after the read-only run-root verification,
 * and the response command after its read-only layout verification), and
 * release it when the outcome is constructed — on every return, throw and
 * recorded-signal path. Internal composed controllers never acquire the
 * lock themselves.
 */
import { closeSync, constants, openSync, statSync } from "node:fs";
import { join } from "node:path";
import { dlopen, FFIType } from "bun:ffi";
import { expectSafeId } from "./pipeline_v2_state.ts";
import type { PipelineV2PublicReason } from "./pipeline_v2_public_reason.ts";

/** `flock(2)` operation values (Linux). */
const LOCK_EX = 2;
const LOCK_NB = 4;
/**
 * `open(2)` flag values for the lock descriptor. `O_DIRECTORY` and
 * `O_NOFOLLOW` exist on `fs.constants`; `O_CLOEXEC` is spelled by its
 * Linux numeric value because the pinned Node type definitions omit it —
 * the numeric value is stable ABI on every Linux architecture this
 * project targets and keeps the descriptor out of every spawned child
 * process, so a docker-helper subprocess can never inherit a held lock.
 */
const O_DIRECTORY_FLAG = constants.O_DIRECTORY;
const O_NOFOLLOW_FLAG = constants.O_NOFOLLOW;
const O_CLOEXEC_FLAG = 0o2000000;
/** `EWOULDBLOCK`/`EAGAIN` on Linux: the nonblocking lock is contended. */
const EWOULDBLOCK = 11;

/** The closed failure vocabulary of the lock acquisition itself. */
export type PipelineV2RunMutationLockFailureReason =
  | "run_layout_invalid"
  | "state_document_missing"
  | "lock_infrastructure";

/**
 * Thrown when the acquisition could not even be attempted safely: the run
 * root is not a real directory (a symlink is refused by `O_NOFOLLOW` and
 * any other object kind by `O_DIRECTORY`), the existing-run state document
 * precondition failed, or the libc binding could not be resolved. The
 * runner maps these reasons onto the public refusal vocabulary; no
 * diagnostic ever embeds file contents or hostile values.
 */
export class PipelineV2RunMutationLockError extends Error {
  readonly reason: PipelineV2RunMutationLockFailureReason;
  constructor(reason: PipelineV2RunMutationLockFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2RunMutationLockError";
    this.reason = reason;
  }
}

/**
 * The held lock. `release()` is idempotent and best-effort: it closes the
 * directory descriptor exactly once; a close error is swallowed because
 * the kernel releases the lock at process death regardless, and a release
 * on a finally path must never mask the command's own outcome.
 */
export interface PipelineV2RunMutationLock {
  release(): void;
}

export type PipelineV2RunMutationLockAcquisition =
  | { readonly busy: false; readonly lock: PipelineV2RunMutationLock }
  | { readonly busy: true };

export interface AcquirePipelineV2RunMutationLockOptions {
  /** The verified canonical local run root of the run. */
  readonly runRoot: string;
  /** The validated run id (safe id; re-validated here fail-closed). */
  readonly runId: string;
  /**
   * `fresh` — the caller just claimed the run root exclusively and no
   * state document exists yet; `existing` — the caller verified the run
   * layout and the acquisition additionally requires the state document
   * to be present, so an existing-run command can never take the lock of
   * a run root a concurrent fresh runner has claimed but not yet
   * initialized (the fresh runner holds its lock before the first durable
   * command, so once `state.json` exists the fresh runner always wins).
   */
  readonly mode: "fresh" | "existing";
}

interface RunMutationLockLibc {
  flock(fd: number, operation: number): number;
  errnoLocation(): unknown;
  memcpy(destination: unknown, source: unknown, length: number): unknown;
}

let libcBinding: RunMutationLockLibc | null = null;

/**
 * Resolves the libc binding once per process. The candidate list is fixed:
 * glibc systems resolve the first entry, musl systems accept any libc
 * name (its loader maps libc aliases to the loaded libc itself). A
 * process without a resolvable libc fails closed on every acquisition —
 * it can never silently skip the lock.
 */
function mutationLockLibc(): RunMutationLockLibc {
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
      const symbolsRecord = library.symbols as unknown as Record<string, unknown>;
      const flock = symbolsRecord["flock"];
      const errnoLocation = symbolsRecord["__errno_location"];
      const memcpy = symbolsRecord["memcpy"];
      if (
        typeof flock !== "function" ||
        typeof errnoLocation !== "function" ||
        typeof memcpy !== "function"
      ) {
        throw new Error(`libc candidate ${candidate} is missing the required symbols`);
      }
      libcBinding = {
        flock: flock as RunMutationLockLibc["flock"],
        errnoLocation: errnoLocation as RunMutationLockLibc["errnoLocation"],
        memcpy: memcpy as RunMutationLockLibc["memcpy"],
      };
      return libcBinding;
    } catch (cause) {
      lastFailure = cause;
    }
  }
  const detail = lastFailure instanceof Error ? lastFailure.message : String(lastFailure ?? "");
  throw new PipelineV2RunMutationLockError(
    "lock_infrastructure",
    `pipeline v2 run mutation lock: no resolvable libc flock binding (${detail})`,
  );
}

/**
 * Acquires the per-run mutation lock, nonblocking. Contention reports
 * `busy: true` and touches nothing; every other outcome either holds the
 * lock or throws a typed error. The call is fully synchronous, so the
 * caller can hold the lock from this moment to its single release with no
 * interleaving await in between.
 */
export function acquirePipelineV2RunMutationLock(
  options: AcquirePipelineV2RunMutationLockOptions,
): PipelineV2RunMutationLockAcquisition {
  expectSafeId(options.runId, "pipeline v2 run mutation lock run id");
  const libc = mutationLockLibc();
  if (options.mode === "existing") {
    // The existing-run precondition: the state document must already
    // exist. A stateless run root belongs to a fresh runner between its
    // exclusive claim and its first durable command; the existing-run
    // command refuses it (missing state) without ever taking the lock,
    // which removes the only window in which an existing-run command
    // could otherwise win the lock of a run the fresh runner is about to
    // initialize.
    try {
      statSync(join(options.runRoot, "state.json"));
    } catch (cause) {
      if (isErrnoExceptionCode(cause, "ENOENT")) {
        throw new PipelineV2RunMutationLockError(
          "state_document_missing",
          `pipeline v2 run mutation lock: the run has no state document under ${JSON.stringify(options.runRoot)}`,
        );
      }
      throw new PipelineV2RunMutationLockError(
        "lock_infrastructure",
        "pipeline v2 run mutation lock: the run state document could not be inspected",
      );
    }
  }
  let fd: number;
  try {
    fd = openSync(
      options.runRoot,
      constants.O_RDONLY | O_DIRECTORY_FLAG | O_NOFOLLOW_FLAG | O_CLOEXEC_FLAG,
    );
  } catch (cause) {
    if (isErrnoExceptionCode(cause, "ENOENT")) {
      throw new PipelineV2RunMutationLockError(
        "run_layout_invalid",
        `pipeline v2 run mutation lock: the run root ${JSON.stringify(options.runRoot)} does not exist`,
      );
    }
    throw new PipelineV2RunMutationLockError(
      "run_layout_invalid",
      `pipeline v2 run mutation lock: the run root ${JSON.stringify(options.runRoot)} is not a real directory`,
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
        throw new PipelineV2RunMutationLockError(
          "lock_infrastructure",
          `pipeline v2 run mutation lock: the kernel lock could not be taken (errno ${errno})`,
        );
      }
      return { busy: true };
    }
  } finally {
    if (!acquired) {
      // The descriptor of a lost or failed acquisition is closed
      // immediately; the close itself never changes the outcome.
      try {
        closeSync(fd);
      } catch {
        // best effort: the kernel releases it at process death anyway
      }
    }
  }
  let released = false;
  const lock: PipelineV2RunMutationLock = Object.freeze({
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

function readErrno(libc: RunMutationLockLibc): number {
  const buffer = new Uint8Array(8);
  libc.memcpy(buffer, libc.errnoLocation(), 4);
  return new DataView(buffer.buffer).getInt32(0, true);
}

function isErrnoExceptionCode(cause: unknown, code: string): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as { code?: unknown }).code === code
  );
}

/**
 * Maps one failed lock acquisition onto the public refusal vocabulary:
 * a stateless run root is the established missing-state refusal, a run
 * root that is no longer a real directory is the established run-layout
 * refusal, and an unusable libc binding is an unexpected internal
 * failure. Returns `null` for every cause that is not a lock error, so
 * the callers keep their own site fallbacks. Contention is not mapped —
 * it is a typed busy result, never an error.
 */
export function pipelineV2RunMutationLockPublicReason(cause: unknown): PipelineV2PublicReason | null {
  if (!(cause instanceof PipelineV2RunMutationLockError)) {
    return null;
  }
  if (cause.reason === "state_document_missing") {
    return "missing_state";
  }
  if (cause.reason === "run_layout_invalid") {
    return "run_layout_invalid";
  }
  return "internal_error";
}
