import { isAbsolute, basename, join } from "node:path";
import { constants, type Stats } from "node:fs";
import { randomBytes } from "node:crypto";
import {
  chmod,
  link,
  lstat,
  mkdir,
  open,
  rmdir,
  realpath,
  unlink,
  type FileHandle,
} from "node:fs/promises";
import { isErrnoException, isInsideRoot } from "./fs_checks.ts";
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  PipelineV2WaitManifestError,
  acceptPipelineV2WaitResponse,
  parsePipelineV2WaitRequest,
  preparePipelineV2WaitRequest,
  type AcceptedPipelineV2WaitResponse,
  type PreparedPipelineV2WaitRequest,
} from "./pipeline_v2_wait_manifest.ts";

/**
 * Internal per-call core of the wait manifest filesystem publication.
 *
 * This module is explicitly internal: the public wrapper
 * `pipeline_v2_wait_store.ts` always calls the core functions with the
 * single fixed, immutable `realWaitStoreIo` defined here; tests call the
 * same core with their own per-call IO object. The IO is a per-call
 * capability passed through the whole call chain — there is no mutable
 * module-global IO and no installer, so a fault-injected test call can
 * never change the behavior of any parallel production call.
 *
 * Layout (fixed, flat):
 *
 *   <runRoot>/waits/<waitIndex>.request.json
 *   <runRoot>/waits/<waitIndex>.response.json
 *
 * `waits/` is a 0700 real non-symlink directory inside the canonical run
 * root; manifest files are 0600 regular files whose content is exactly the
 * canonical JSON of the manifest — no trailing newline. The paths never
 * enter the manifests or the durable state, and the manifests carry no
 * filesystem provenance.
 *
 * Publication algorithm per manifest file:
 *
 *   1. validate the run root (absolute, canonical, real non-symlink
 *      directory) and the waits directory (created exclusively with mode
 *      0700, identity-fixed, chmod-enforced, canonically verified; on
 *      creation the run root itself is fsynced before any manifest file);
 *   2. create the temp file inside `waits/` with `O_CREAT|O_EXCL|O_NOFOLLOW`
 *      mode 0600 and record its `dev`/`ino` ownership identity;
 *   3. write the canonical bytes with a full write-all loop (partial writes
 *      advance the position; a zero-progress, negative or impossible write
 *      count fails after exactly one attempt — no loop);
 *   4. `fsync(file)`, then `close(file)`;
 *   5. `link(temp, final)` — the exclusive publication; `rename()` is never
 *      used for final files, so a concurrently created object can never be
 *      replaced;
 *   6. ownership-checked removal of the temp file (the recorded `dev`/`ino`
 *      must still identify the object; a substituted object is never
 *      removed) — a post-link failure here is a durability error, not a
 *      rollback;
 *   7. `fsync(waits directory)`.
 *
 * Idempotent retry: if `link()` reports `EEXIST`, the temp file is removed
 * (best effort) and the existing object is adopted instead: it must be a
 * real non-symlink regular file with mode 0600; its bytes are read through
 * `O_NOFOLLOW` and compared with the new canonical bytes. Equal bytes mean
 * success — the file is never replaced, its mode, inode, mtime and content
 * stay untouched — and the waits directory is fsynced again before the
 * return, so an exact retry after a former `durability_unknown` confirms
 * the durability of the already visible file. Different bytes, a wrong
 * mode, or a non-file object (symlink, directory, FIFO, socket) fail closed
 * as a typed conflict: nothing is overwritten or repaired, external
 * sentinels are untouched, and a target is never replaced.
 *
 * Concurrency: two publishers of the same manifest may both succeed (the
 * loser adopts the winner's identical bytes); publishers of different
 * manifests race for one path — exactly one wins, the loser gets a typed
 * conflict, and the target always keeps the winner's bytes.
 *
 * Error contract (closed, typed — no message parsing downstream):
 *
 *   - pre-publication failures throw `PipelineV2WaitStoreError` with
 *     `outcome: "not_published"` and a stable `reason`
 *     (`invalid_layout`, `conflict`, `io_failure`);
 *   - after a successful `link()`, if the unlink/fsync/close durability
 *     phase cannot be confirmed, the final file is never rolled back: the
 *     error carries `outcome: "durability_unknown"` with reason
 *     `io_failure` and an immutable deep-frozen `candidate` (kind, wait
 *     index, final path, canonical JSON, digest) — the final file exists
 *     with its full canonical bytes and an exact retry confirms it;
 *   - manifest validation failures keep their `PipelineV2WaitManifestError`
 *     class and are never retagged by message text.
 *
 * Diagnostics are content-free: only a controlled operation class and an
 * optional errno suffix (limited `[A-Z][A-Z0-9_]{0,31}` code form, read
 * getter-safe) — never a manifest body, an action value, a canary or raw
 * parser output.
 *
 * Semantic boundary with the durable state (future, not wired here):
 *
 *   publish request  → dispatch run_waiting(request_sha256)
 *   publish response → dispatch wait_response_recorded(response_sha256)
 *
 * A manifest file published without the corresponding durable commit is an
 * orphan, not part of the history; a retry with the same canonical bytes
 * safely reuses it (idempotent adoption), and a different manifest on a
 * busy path is a conflict. This module never dispatches reducer commands
 * and never mutates the durable state, the project copy, inputs or
 * outputs — it only creates and maintains `<runRoot>/waits/`.
 *
 * Honest boundaries: the run root is never created or removed here; there
 * is no protection against a trusted host process racing the
 * verify-then-use steps, and there is no crash recovery for a leftover
 * temp file (the next publication uses a fresh exclusive temp name).
 */

export type PipelineV2WaitStoreFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type PipelineV2WaitStoreOutcome = "not_published" | "durability_unknown";

/**
 * The published candidate of a durability-unknown outcome: the final file
 * exists with exactly these canonical bytes and this digest, but its crash
 * survival is unconfirmed. An exact retry re-verifies and re-fsyncs it.
 */
export interface PipelineV2WaitStoreCandidate {
  readonly kind: "request" | "response";
  readonly wait_index: number;
  readonly final_path: string;
  readonly canonical_json: string;
  readonly sha256: string;
}

export class PipelineV2WaitStoreError extends Error {
  readonly outcome: PipelineV2WaitStoreOutcome;
  readonly reason: PipelineV2WaitStoreFailureReason;
  readonly candidate?: PipelineV2WaitStoreCandidate;

  constructor(
    outcome: PipelineV2WaitStoreOutcome,
    reason: PipelineV2WaitStoreFailureReason,
    message: string,
    candidate?: PipelineV2WaitStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2WaitStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreeze(candidate);
    }
  }
}

export interface PublishedPipelineV2WaitRequest {
  readonly request: PreparedPipelineV2WaitRequest;
  readonly request_path: string;
}

export interface PublishedPipelineV2WaitResponse {
  readonly request: PreparedPipelineV2WaitRequest;
  readonly response: AcceptedPipelineV2WaitResponse;
  readonly request_path: string;
  readonly response_path: string;
}

export interface WaitStoreFileHandle {
  readonly write: (
    chunk: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ) => Promise<number>;
  readonly read: (
    chunk: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ) => Promise<number>;
  readonly stat: () => Promise<Stats>;
  readonly sync: () => Promise<void>;
  readonly close: () => Promise<void>;
}

export interface WaitStoreDirHandle {
  readonly sync: () => Promise<void>;
  readonly close: () => Promise<void>;
}

/**
 * Per-call filesystem capability of the publication core. The production
 * wrapper always passes the fixed immutable `realWaitStoreIo`; tests
 * inject their own object per call. No hook ever reads module-global state.
 */
export interface WaitStoreIo {
  /** Exclusive `mkdir`, mode 0700 (the caller handles `EEXIST`). */
  readonly mkdirExclusive: (path: string) => Promise<void>;
  /** The post-creation mode enforcement of the waits directory. */
  readonly chmod: (path: string, mode: number) => Promise<void>;
  /** lstat: ENOENT is absence; every other failure rejects. */
  readonly lstatOrNull: (path: string) => Promise<Stats | null>;
  /** Canonical resolution of an existing path. */
  readonly realpath: (path: string) => Promise<string>;
  /** `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600. */
  readonly openTempExclusive: (path: string) => Promise<WaitStoreFileHandle>;
  /** `O_RDONLY|O_NOFOLLOW`. */
  readonly openReadNoFollow: (path: string) => Promise<WaitStoreFileHandle>;
  /** Open a directory for fsync (`"r"` on the directory path). */
  readonly openDir: (path: string) => Promise<WaitStoreDirHandle>;
  /** The exclusive publication: `link(existingPath, newPath)`. */
  readonly link: (existingPath: string, newPath: string) => Promise<void>;
  /** Unlink of an owned temp file (the caller checks identity first). */
  readonly unlink: (path: string) => Promise<void>;
  /** rmdir of the just-created empty waits directory on chmod failure. */
  readonly rmdir: (path: string) => Promise<void>;
}

class RealWaitStoreFileHandle implements WaitStoreFileHandle {
  constructor(private readonly handle: FileHandle) {}

  async write(
    chunk: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): Promise<number> {
    const result = await this.handle.write(chunk, offset, length, position);
    return result.bytesWritten;
  }

  async read(
    chunk: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): Promise<number> {
    const result = await this.handle.read(chunk, offset, length, position);
    return result.bytesRead;
  }

  async stat(): Promise<Stats> {
    return await this.handle.stat();
  }

  async sync(): Promise<void> {
    await this.handle.sync();
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}

class RealWaitStoreDirHandle implements WaitStoreDirHandle {
  constructor(private readonly handle: FileHandle) {}

  async sync(): Promise<void> {
    await this.handle.sync();
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}

const realWaitStoreIoImplementation: WaitStoreIo = {
  mkdirExclusive: (path) => mkdir(path, { mode: 0o700 }),
  chmod: (path, mode) => chmod(path, mode),
  lstatOrNull: async (path) => {
    try {
      return await lstat(path);
    } catch (cause) {
      if (isErrnoException(cause, "ENOENT")) {
        return null;
      }
      throw cause;
    }
  },
  realpath: (path) => realpath(path),
  openTempExclusive: async (path) =>
    new RealWaitStoreFileHandle(
      await open(
        path,
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        0o600,
      ),
    ),
  openReadNoFollow: async (path) =>
    new RealWaitStoreFileHandle(await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)),
  openDir: async (path) => new RealWaitStoreDirHandle(await open(path, "r")),
  link: (existingPath, newPath) => link(existingPath, newPath),
  unlink: (path) => unlink(path),
  rmdir: (path) => rmdir(path),
};

/**
 * The single production IO of the publication core: fixed at module load,
 * deep-immutable, and never replaced — the wrapper passes exactly this
 * object on every call.
 */
export const realWaitStoreIo: WaitStoreIo = Object.freeze(realWaitStoreIoImplementation);

const WAITS_DIR_NAME = "waits";
const REQUEST_SUFFIX = "request.json";
const RESPONSE_SUFFIX = "response.json";
const TEMP_PREFIX = ".wait-publish-";
const MAX_TEMP_ATTEMPTS = 8;
const WAIT_STORE_CHUNK = 128 * 1024;

const ERRNO_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,31}$/;

/**
 * The errno code of a filesystem failure, rendered separately and without
 * the system message, so no path or payload text can leak into a wait
 * store diagnostic. Only the limited system-code form
 * `[A-Z][A-Z0-9_]{0,31}` is accepted; reading `cause.code` is getter-safe.
 */
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

function notPublishedIoFailure(what: string, cause?: unknown): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError(
    "not_published",
    "io_failure",
    cause === undefined ? what : `${what}${errnoSuffix(cause)}`,
  );
}

function invalidLayout(message: string): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError("not_published", "conflict", message);
}

function durabilityUnknown(
  target: FilePublicationTarget,
  finalPath: string,
  message: string,
): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError(
    "durability_unknown",
    "io_failure",
    message,
    {
      kind: target.kind,
      wait_index: target.waitIndex,
      final_path: finalPath,
      canonical_json: target.canonicalJson,
      sha256: target.sha256,
    },
  );
}

function describeKind(info: Stats): string {
  return info.isSymbolicLink()
    ? "a symbolic link"
    : info.isDirectory()
      ? "a directory"
      : info.isFile()
        ? "a regular file"
        : info.isFIFO()
          ? "a FIFO"
          : info.isSocket()
            ? "a unix socket"
            : "an unexpected object";
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreeze(entry);
    }
    Object.freeze(value);
    return value;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

/**
 * Final typed boundary: only `PipelineV2WaitStoreError` and
 * `PipelineV2WaitManifestError` pass unchanged; any other failure becomes a
 * sanitized not-published io failure. The classification is by class only —
 * no message text is ever parsed.
 */
async function withWaitStoreGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw cause;
    }
    if (cause instanceof PipelineV2WaitManifestError) {
      throw cause;
    }
    throw notPublishedIoFailure("the wait manifest publication failed");
  }
}

async function inspectOrNull(io: WaitStoreIo, path: string, what: string): Promise<Stats | null> {
  try {
    return await io.lstatOrNull(path);
  } catch (cause) {
    throw notPublishedIoFailure(`${what} could not be inspected`, cause);
  }
}

/**
 * `fsync` one directory: open (`"r"`), sync, close. A close failure after
 * an otherwise successful sync is the failure itself; a close failure after
 * a failed sync never replaces the original failure. The caller assigns
 * the typed outcome.
 */
async function fsyncDirectoryRaw(io: WaitStoreIo, path: string): Promise<void> {
  const dir = await io.openDir(path);
  let synced = false;
  try {
    await dir.sync();
    synced = true;
  } finally {
    try {
      await dir.close();
    } catch (closeCause) {
      if (synced) {
        throw closeCause;
      }
    }
  }
}

/**
 * The canonical run root of a wait publication: absolute, existing, real
 * non-symlink directory, canonically equal to its own path. Never created
 * or removed.
 */
async function requireCanonicalRunRootDirectory(io: WaitStoreIo, runRoot: string): Promise<string> {
  if (typeof runRoot !== "string" || !isAbsolute(runRoot)) {
    throw invalidLayout("the run root must be an absolute canonical path");
  }
  const info = await inspectOrNull(io, runRoot, "the run root");
  if (info === null) {
    throw invalidLayout("the run root does not exist");
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw invalidLayout(`the run root exists but is ${describeKind(info)}`);
  }
  let canonical: string;
  try {
    canonical = await io.realpath(runRoot);
  } catch (cause) {
    throw notPublishedIoFailure("the run root cannot be canonicalized", cause);
  }
  if (canonical !== runRoot) {
    throw invalidLayout("the run root is not canonical");
  }
  return canonical;
}

/**
 * Ensure the fixed flat waits directory inside the canonical run root:
 * exclusive creation (a concurrent creator loses into the same
 * verification), immediate identity fixation, chmod enforcement of mode
 * 0700 (never trusted to the umask), canonical identity verification, and
 * — on first creation — one fsync of the run root before any manifest file
 * is published. An existing object that is not a real directory, or a
 * directory with the wrong mode, fails closed as an invalid layout; the
 * mode is verified, never silently fixed by adoption.
 */
async function ensureWaitsDirectory(io: WaitStoreIo, runRootCanonical: string): Promise<string> {
  const waitsPath = join(runRootCanonical, WAITS_DIR_NAME);
  let info = await inspectOrNull(io, waitsPath, "the waits directory");
  let created = false;
  if (info === null) {
    try {
      await io.mkdirExclusive(waitsPath);
    } catch (cause) {
      if (!isErrnoException(cause, "EEXIST")) {
        throw notPublishedIoFailure("the waits directory could not be created", cause);
      }
    }
    const createdInfo = await inspectOrNull(io, waitsPath, "the waits directory");
    if (createdInfo === null || createdInfo.isSymbolicLink() || !createdInfo.isDirectory()) {
      throw notPublishedIoFailure("the waits directory is not a real directory after creation");
    }
    created = true;
    try {
      await io.chmod(waitsPath, 0o700);
    } catch (cause) {
      await removeOwnedEmptyWaitsDirectory(io, waitsPath, createdInfo, runRootCanonical);
      throw notPublishedIoFailure("the waits directory could not be created", cause);
    }
    info = await inspectOrNull(io, waitsPath, "the waits directory");
    if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
      throw notPublishedIoFailure("the waits directory is not a real directory after creation");
    }
  } else if (info.isSymbolicLink() || !info.isDirectory()) {
    throw invalidLayout(`the waits directory exists but is ${describeKind(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o700) {
    throw invalidLayout("the waits directory does not have the required mode 0700");
  }
  let canonical: string;
  try {
    canonical = await io.realpath(waitsPath);
  } catch (cause) {
    throw notPublishedIoFailure("the waits directory cannot be canonicalized", cause);
  }
  if (canonical !== waitsPath) {
    throw invalidLayout("the waits directory does not resolve to its canonical path");
  }
  if (created) {
    try {
      await fsyncDirectoryRaw(io, runRootCanonical);
    } catch (cause) {
      throw notPublishedIoFailure("the run root could not be synced", cause);
    }
  }
  return waitsPath;
}

/**
 * Best-effort removal of the just-created empty waits directory after a
 * chmod failure. The removal runs only with the recorded ownership proof
 * (same real non-symlink directory, same `dev`/`ino`, canonically inside
 * the run root); every failure is swallowed and never replaces the
 * original failure.
 */
async function removeOwnedEmptyWaitsDirectory(
  io: WaitStoreIo,
  waitsPath: string,
  identity: Stats,
  runRootCanonical: string,
): Promise<void> {
  try {
    const info = await io.lstatOrNull(waitsPath);
    if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
      return;
    }
    if (info.dev !== identity.dev || info.ino !== identity.ino) {
      return;
    }
    const canonical = await io.realpath(waitsPath);
    if (canonical !== waitsPath || !isInsideRoot(runRootCanonical, canonical)) {
      return;
    }
    await io.rmdir(waitsPath);
  } catch {
    // Best-effort: a leftover empty waits directory is harmless and is
    // verified (and adopted) by any later publication attempt.
  }
}

interface FilePublicationTarget {
  readonly kind: "request" | "response";
  readonly waitIndex: number;
  readonly canonicalJson: string;
  readonly sha256: string;
}

interface OwnedTempFile {
  readonly path: string;
  readonly dev: number;
  readonly ino: number;
}

/**
 * Best-effort removal of exactly the temp file this call created. The
 * removal runs only after the ownership proof succeeds: the object at the
 * temp path must still be a real non-symlink regular file with the
 * recorded `dev`/`ino`, canonically resolving inside the waits directory.
 * A vanished temp is already gone; a substituted object is never removed;
 * every failure is swallowed and never masks the original outcome.
 */
async function removeOwnedTempFile(
  io: WaitStoreIo,
  tempPath: string,
  identity: OwnedTempFile,
  waitsPath: string,
): Promise<void> {
  try {
    const info = await io.lstatOrNull(tempPath);
    if (info === null) {
      return;
    }
    if (info.isSymbolicLink() || !info.isFile()) {
      return;
    }
    if (info.dev !== identity.dev || info.ino !== identity.ino) {
      return;
    }
    const canonical = await io.realpath(tempPath);
    if (canonical !== tempPath && !isInsideRoot(waitsPath, canonical)) {
      return;
    }
    await io.unlink(tempPath);
  } catch {
    // Best-effort: a leftover temp file is the documented absence of crash
    // recovery; the next publication uses a fresh exclusive temp name.
  }
}

/**
 * Read one whole regular file through its already opened `O_NOFOLLOW`
 * handle: a full read loop (EOF ends it; a negative read count fails).
 * Close failures after a failed read never replace the original failure.
 */
async function readWholeFile(
  path: string,
  handle: WaitStoreFileHandle,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let failure: PipelineV2WaitStoreError | undefined;
  try {
    const stat = await handle.stat();
    if (!stat.isFile()) {
      throw conflict(`the wait manifest file at ${path} is not a regular file after open`);
    }
    const chunk = Buffer.allocUnsafe(WAIT_STORE_CHUNK);
    let position = 0;
    for (;;) {
      let bytesRead: number;
      try {
        bytesRead = await handle.read(chunk, 0, chunk.length, position);
      } catch (cause) {
        throw notPublishedIoFailure("the wait manifest file could not be read", cause);
      }
      if (!Number.isSafeInteger(bytesRead) || bytesRead < 0) {
        throw notPublishedIoFailure("the wait manifest file could not be read");
      }
      if (bytesRead === 0) {
        break;
      }
      chunks.push(Buffer.from(chunk.subarray(0, bytesRead)));
      position += bytesRead;
    }
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      failure = cause;
    } else {
      failure = notPublishedIoFailure("the wait manifest file could not be read", cause);
    }
  }
  let closeFailure: unknown = undefined;
  try {
    await handle.close();
  } catch (cause) {
    closeFailure = cause;
  }
  if (failure !== undefined) {
    throw failure;
  }
  if (closeFailure !== undefined) {
    throw notPublishedIoFailure(
      "the wait manifest file could not be closed after reading",
      closeFailure,
    );
  }
  return Buffer.concat(chunks);
}

/**
 * Idempotent adoption of an existing final file: it must be a real
 * non-symlink regular file with mode 0600 carrying exactly the canonical
 * bytes of this manifest. Equal bytes mean success — nothing is replaced,
 * repaired or re-moded — and the waits directory is fsynced again before
 * the return so an exact retry confirms durability. Anything else fails
 * closed as a typed conflict (or an io failure while inspecting); the
 * existing object is never touched.
 */
async function adoptExistingFinalFile(
  io: WaitStoreIo,
  waitsPath: string,
  finalPath: string,
  target: FilePublicationTarget,
): Promise<void> {
  const info = await inspectOrNull(io, finalPath, "the wait manifest target");
  if (info === null) {
    throw notPublishedIoFailure("the wait manifest target disappeared while it was being checked");
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw conflict(`the wait manifest target exists but is ${describeKind(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw conflict("the existing wait manifest file does not have the required mode 0600");
  }
  let handle: WaitStoreFileHandle;
  try {
    handle = await io.openReadNoFollow(finalPath);
  } catch (cause) {
    throw notPublishedIoFailure("the wait manifest file could not be opened for reading", cause);
  }
  const stored = await readWholeFile(finalPath, handle);
  if (!stored.equals(Buffer.from(target.canonicalJson, "utf8"))) {
    throw conflict("the existing wait manifest file carries different canonical bytes");
  }
  try {
    await fsyncDirectoryRaw(io, waitsPath);
  } catch (cause) {
    throw durabilityUnknown(
      target,
      finalPath,
      "the waits directory could not be synced after adoption",
    );
  }
}

/**
 * Publish one manifest file into the waits directory: temp file with
 * recorded ownership identity, full write-all loop, fsync, close, then the
 * exclusive `link()` publication, ownership-checked temp removal and the
 * waits-directory fsync. On `EEXIST` the existing object is adopted
 * (idempotent). Pre-link failures clean exactly the owned temp file and
 * keep the outcome `not_published`; post-link failures never roll the
 * final file back and become `durability_unknown` with the candidate.
 */
async function publishManifestFile(
  io: WaitStoreIo,
  waitsPath: string,
  target: FilePublicationTarget,
): Promise<string> {
  const suffix = target.kind === "request" ? REQUEST_SUFFIX : RESPONSE_SUFFIX;
  const finalPath = join(waitsPath, `${target.waitIndex}.${suffix}`);
  const bytes = Buffer.from(target.canonicalJson, "utf8");
  let tempPath = "";
  let identity: OwnedTempFile | null = null;
  for (let attempt = 0; attempt < MAX_TEMP_ATTEMPTS; attempt += 1) {
    tempPath = join(
      waitsPath,
      `${TEMP_PREFIX}${target.kind}-${target.waitIndex}-${randomBytes(8).toString("hex")}`,
    );
    let handle: WaitStoreFileHandle;
    try {
      handle = await io.openTempExclusive(tempPath);
    } catch (cause) {
      if (isErrnoException(cause, "EEXIST")) {
        continue;
      }
      throw notPublishedIoFailure("the wait manifest temp file could not be created", cause);
    }
    let workFailure: PipelineV2WaitStoreError | undefined;
    try {
      let stat: Stats;
      try {
        stat = await handle.stat();
      } catch (cause) {
        throw notPublishedIoFailure("the wait manifest temp file could not be inspected", cause);
      }
      if (!stat.isFile()) {
        throw notPublishedIoFailure("the wait manifest temp file is not a regular file after creation");
      }
      if ((stat.mode & 0o7777) !== 0o600) {
        throw notPublishedIoFailure("the wait manifest temp file does not have the required mode 0600");
      }
      identity = { path: tempPath, dev: stat.dev, ino: stat.ino };
      let written = 0;
      while (written < bytes.byteLength) {
        const remaining = bytes.byteLength - written;
        let bytesWritten: number;
        try {
          bytesWritten = await handle.write(bytes, written, remaining, written);
        } catch (cause) {
          throw notPublishedIoFailure("the wait manifest file could not be written", cause);
        }
        if (!Number.isSafeInteger(bytesWritten) || bytesWritten <= 0 || bytesWritten > remaining) {
          throw notPublishedIoFailure("the wait manifest file could not be written without progress");
        }
        written += bytesWritten;
      }
      try {
        await handle.sync();
      } catch (cause) {
        throw notPublishedIoFailure("the wait manifest temp file could not be synced", cause);
      }
    } catch (cause) {
      workFailure =
        cause instanceof PipelineV2WaitStoreError
          ? cause
          : notPublishedIoFailure("the wait manifest file could not be prepared", cause);
    }
    let closeFailure: unknown = undefined;
    try {
      await handle.close();
    } catch (cause) {
      closeFailure = cause;
    }
    if (workFailure !== undefined) {
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, waitsPath);
      }
      throw workFailure;
    }
    if (closeFailure !== undefined) {
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, waitsPath);
      }
      throw notPublishedIoFailure(
        "the wait manifest temp file could not be closed",
        closeFailure,
      );
    }
    try {
      await io.link(tempPath, finalPath);
    } catch (cause) {
      if (isErrnoException(cause, "EEXIST")) {
        if (identity !== null) {
          await removeOwnedTempFile(io, tempPath, identity, waitsPath);
        }
        await adoptExistingFinalFile(io, waitsPath, finalPath, target);
        return finalPath;
      }
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, waitsPath);
      }
      throw notPublishedIoFailure("the wait manifest file could not be published", cause);
    }
    // Post-link durability phase: the final file is authoritative and is
    // never rolled back. Every failure here becomes durability_unknown.
    try {
      const tempInfo = await io.lstatOrNull(tempPath);
      if (tempInfo !== null) {
        if (tempInfo.isSymbolicLink() || !tempInfo.isFile()) {
          throw durabilityUnknown(
            target,
            finalPath,
            "the published wait manifest temp file could not be confirmed as owned",
          );
        }
        if (identity === null || tempInfo.dev !== identity.dev || tempInfo.ino !== identity.ino) {
          throw durabilityUnknown(
            target,
            finalPath,
            "the published wait manifest temp file could not be confirmed as owned",
          );
        }
        try {
          await io.unlink(tempPath);
        } catch (unlinkCause) {
          if (!isErrnoException(unlinkCause, "ENOENT")) {
            throw durabilityUnknown(
              target,
              finalPath,
              "the published wait manifest temp file could not be removed",
            );
          }
        }
      }
      try {
        await fsyncDirectoryRaw(io, waitsPath);
      } catch (cause) {
        throw durabilityUnknown(
          target,
          finalPath,
          "the waits directory could not be synced after publication",
        );
      }
    } catch (cause) {
      if (cause instanceof PipelineV2WaitStoreError && cause.outcome === "durability_unknown") {
        throw cause;
      }
      throw durabilityUnknown(
        target,
        finalPath,
        "the wait manifest publication durability could not be confirmed",
      );
    }
    return finalPath;
  }
  throw notPublishedIoFailure("the wait manifest temp file could not be created exclusively");
}

/**
 * Publish one wait request manifest: prepare (the manifest validator is
 * the single authority — its failures propagate unchanged), bind the
 * canonical run root (its basename must be the manifest run id), ensure
 * the waits directory, then publish `<waitIndex>.request.json`.
 */
export async function publishWaitRequestWithIo(
  io: WaitStoreIo,
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2WaitRequest> {
  return await withWaitStoreGuard(async () => {
    const prepared = preparePipelineV2WaitRequest(value);
    const runRootCanonical = await requireCanonicalRunRootDirectory(io, runRoot);
    if (basename(runRootCanonical) !== prepared.manifest.run_id) {
      throw invalidLayout("the run root does not match the wait manifest run identifier");
    }
    const waitsPath = await ensureWaitsDirectory(io, runRootCanonical);
    const requestPath = join(waitsPath, `${prepared.manifest.wait_index}.${REQUEST_SUFFIX}`);
    await publishManifestFile(io, waitsPath, {
      kind: "request",
      waitIndex: prepared.manifest.wait_index,
      canonicalJson: prepared.canonical_json,
      sha256: prepared.sha256,
    });
    return deepFreeze({ request: prepared, request_path: requestPath });
  });
}

/**
 * Load the stored request file for a response publication: the object must
 * be a real non-symlink regular file with mode 0600; its bytes are read
 * through `O_NOFOLLOW`, parsed by the manifest module (its failures
 * propagate unchanged) and must carry exactly the canonical JSON of their
 * own manifest, the requested wait index and the run root's run id. A
 * damaged, foreign or noncanonical file fails closed before anything is
 * written.
 */
async function loadStoredWaitRequest(
  io: WaitStoreIo,
  requestPath: string,
  waitsPath: string,
  waitIndex: number,
  runId: string,
): Promise<PreparedPipelineV2WaitRequest> {
  const info = await inspectOrNull(io, requestPath, "the published wait request file");
  if (info === null) {
    throw invalidLayout("the published wait request file does not exist");
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw conflict(`the published wait request file exists but is ${describeKind(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw conflict("the published wait request file does not have the required mode 0600");
  }
  let handle: WaitStoreFileHandle;
  try {
    handle = await io.openReadNoFollow(requestPath);
  } catch (cause) {
    throw notPublishedIoFailure("the published wait request file could not be opened", cause);
  }
  const stored = await readWholeFile(requestPath, handle);
  const raw = stored.toString("utf8");
  const prepared = parsePipelineV2WaitRequest(raw);
  if (prepared.manifest.wait_index !== waitIndex) {
    throw conflict("the stored wait request manifest names another wait index");
  }
  if (prepared.manifest.run_id !== runId) {
    throw conflict("the stored wait request manifest does not belong to this run root");
  }
  if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
    throw conflict("the stored wait request file does not carry its own canonical JSON");
  }
  return prepared;
}

/**
 * Publish one wait response: validate the wait index, bind the canonical
 * run root, load and verify the stored request file, accept the user's
 * response through the manifest module (its failures propagate
 * unchanged), then publish `<waitIndex>.response.json`.
 */
export async function publishWaitResponseWithIo(
  io: WaitStoreIo,
  runRoot: string,
  waitIndex: number,
  raw: string,
): Promise<PublishedPipelineV2WaitResponse> {
  return await withWaitStoreGuard(async () => {
    if (!isPositiveSafeInteger(waitIndex)) {
      throw invalidLayout("the wait index must be a positive safe integer");
    }
    const runRootCanonical = await requireCanonicalRunRootDirectory(io, runRoot);
    const runId = basename(runRootCanonical);
    const waitsPath = await ensureWaitsDirectory(io, runRootCanonical);
    const requestPath = join(waitsPath, `${waitIndex}.${REQUEST_SUFFIX}`);
    const prepared = await loadStoredWaitRequest(io, requestPath, waitsPath, waitIndex, runId);
    const accepted = acceptPipelineV2WaitResponse(prepared, raw);
    const responsePath = join(waitsPath, `${waitIndex}.${RESPONSE_SUFFIX}`);
    await publishManifestFile(io, waitsPath, {
      kind: "response",
      waitIndex,
      canonicalJson: accepted.canonical_json,
      sha256: accepted.sha256,
    });
    return deepFreeze({
      request: prepared,
      response: accepted,
      request_path: requestPath,
      response_path: responsePath,
    });
  });
}
