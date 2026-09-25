import { isAbsolute } from "node:path";
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
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/**
 * Internal neutral substrate for immutable canonical document publication
 * (unwired): the single owner of the low-level filesystem protocol shared
 * by every immutable-document store adapter.
 *
 * This module is explicitly internal. It owns ONLY the protocol:
 *
 *   1. canonical run-root verification (absolute, real non-symlink
 *      directory, canonically equal to its own path; never created or
 *      removed);
 *   2. store-owned directory components (exclusive `mkdir`, immediate
 *      `dev`/`ino` identity fixation, chmod enforcement of mode 0700 —
 *      never trusted to the umask — canonical identity verification, and
 *      one parent fsync before every successful return; only the
 *      directory this call created is chmodded and ownership-checked-
 *      removed on a chmod failure, while a concurrently created
 *      (`EEXIST`) or pre-existing directory is verified and adopted
 *      without modification; an existing object that is not a real
 *      directory, or a directory with the wrong mode, fails closed);
 *   3. the immutable file publication: exclusive temp file
 *      (`O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600) in the final parent, full
 *      write-all loop (partial writes advance the position; a
 *      zero-progress, negative or impossible write count fails after
 *      exactly one attempt), file fsync, close, exclusive `link(temp,
 *      final)` — never a replace-capable `rename()` — ownership-checked
 *      temp removal, parent fsync;
 *   4. idempotent adoption: on `link()` `EEXIST` the existing object must
 *      be a real non-symlink regular file with mode 0600 carrying exactly
 *      the new canonical bytes; equal bytes succeed without touching
 *      inode/mode/mtime/content and re-fsync the parent (an exact retry
 *      after a former durability-unknown confirms it); anything else is a
 *      typed conflict and nothing is ever overwritten, repaired or
 *      removed;
 *   5. the read side of a stored document: real non-symlink regular file,
 *      mode 0600, read through `O_NOFOLLOW` with a full read loop.
 *
 * There is no second copy of this protocol anywhere: the wait store
 * (`pipeline_v2_wait_store_internal.ts`) and the run-plan store
 * (`pipeline_v2_run_plan_store.ts`) are thin adapters that bind their own
 * layout, names and binding checks around these functions; neither
 * adapter implements its own `O_EXCL` temp/open/link/dir-fsync sequence.
 *
 * The IO is a per-call capability (`ImmutableDocumentIo`): the production
 * wrappers always pass the single fixed, immutable `realImmutableDocumentIo`
 * defined here; tests inject their own object per call. No hook ever reads
 * module-global state, there is no mutable module-global IO and no
 * installer, so a fault-injected test call can never change the behavior
 * of any parallel production call, and no public module exports an IO or
 * test seam.
 *
 * Diagnostics are built from per-call wording (`ImmutableDocumentWording`,
 * plus the per-directory and per-file nouns the adapters pass), so every
 * adapter keeps its own exact diagnostic strings while sharing one
 * algorithm. The messages carry only a controlled operation class and an
 * optional errno suffix (limited `[A-Z][A-Z0-9_]{0,31}` code form, read
 * getter-safe) — never a document body, a caller path beyond the
 * caller-supplied fixed path, raw JSON, env values or credentials.
 *
 * Failure contract (closed, typed — no message parsing downstream):
 *
 *   - pre-publication failures throw `ImmutableDocumentStoreError` with
 *     `outcome: "not_published"` and a stable `reason`
 *     (`invalid_layout`, `conflict`, `io_failure`);
 *   - after a successful `link()`, if the unlink/fsync durability phase
 *     cannot be confirmed, the final file is never rolled back: the error
 *     carries `outcome: "durability_unknown"` with reason `io_failure`
 *     and an immutable deep-frozen `candidate` ({identity, final_path,
 *     canonical_json, sha256}) — the final file exists with its full
 *     canonical bytes and an exact retry confirms it;
 *   - unexpected programmer errors propagate unchanged from the protocol
 *     functions; the adapters own the final typed boundary and never
 *     classify by message text.
 *
 * Honest boundaries: the run root is never created or removed here; there
 * is no protection against a trusted host process racing the
 * verify-then-use steps, and there is no crash recovery for a leftover
 * temp file (the next publication uses a fresh exclusive temp name).
 */

export type ImmutableDocumentFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type ImmutableDocumentOutcome = "not_published" | "durability_unknown";

/**
 * The published candidate of a durability-unknown outcome: the final file
 * exists with exactly these canonical bytes and this digest, but its crash
 * survival is unconfirmed. `identity` is the deep-frozen, content-free
 * identity descriptor the adapter bound at publication time (its own
 * candidate fields); an exact retry re-verifies and re-fsyncs the file.
 */
export interface ImmutableDocumentCandidate {
  readonly identity: ReadonlyJsonValue;
  readonly final_path: string;
  readonly canonical_json: string;
  readonly sha256: string;
}

export class ImmutableDocumentStoreError extends Error {
  readonly outcome: ImmutableDocumentOutcome;
  readonly reason: ImmutableDocumentFailureReason;
  readonly candidate?: ImmutableDocumentCandidate;

  constructor(
    outcome: ImmutableDocumentOutcome,
    reason: ImmutableDocumentFailureReason,
    message: string,
    candidate?: ImmutableDocumentCandidate,
  ) {
    super(message);
    this.name = "ImmutableDocumentStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreezeValue(candidate);
    }
  }
}

export type ReadonlyJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly ReadonlyJsonValue[]
  | { readonly [key: string]: ReadonlyJsonValue };

/**
 * Per-call diagnostic wording of one adapter: the noun of the document
 * itself and the full fallback message for unexpected failures. The
 * directory and stored-file nouns are passed per call, because one store
 * may maintain several named directories.
 */
export interface ImmutableDocumentWording {
  /** e.g. "wait manifest", "task revision manifest", "plan revision manifest". */
  readonly document: string;
  /** e.g. "the wait manifest publication failed". */
  readonly publicationFailed: string;
}

export interface ImmutableDocumentFileHandle {
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

export interface ImmutableDocumentDirHandle {
  readonly sync: () => Promise<void>;
  readonly close: () => Promise<void>;
}

/**
 * Per-call filesystem capability of the publication protocol. The
 * production wrappers always pass the fixed immutable
 * `realImmutableDocumentIo`; tests inject their own object per call.
 */
export interface ImmutableDocumentIo {
  /** Exclusive `mkdir`, mode 0700 (the caller handles `EEXIST`). */
  readonly mkdirExclusive: (path: string) => Promise<void>;
  /** The post-creation mode enforcement of a store-owned directory. */
  readonly chmod: (path: string, mode: number) => Promise<void>;
  /** lstat: ENOENT is absence; every other failure rejects. */
  readonly lstatOrNull: (path: string) => Promise<Stats | null>;
  /** Canonical resolution of an existing path. */
  readonly realpath: (path: string) => Promise<string>;
  /** `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600. */
  readonly openTempExclusive: (path: string) => Promise<ImmutableDocumentFileHandle>;
  /** `O_RDONLY|O_NOFOLLOW`. */
  readonly openReadNoFollow: (path: string) => Promise<ImmutableDocumentFileHandle>;
  /** Open a directory for fsync (`"r"` on the directory path). */
  readonly openDir: (path: string) => Promise<ImmutableDocumentDirHandle>;
  /** The exclusive publication: `link(existingPath, newPath)`. */
  readonly link: (existingPath: string, newPath: string) => Promise<void>;
  /** Unlink of an owned temp file (the caller checks identity first). */
  readonly unlink: (path: string) => Promise<void>;
  /** rmdir of the empty directory this call created, on chmod failure. */
  readonly rmdir: (path: string) => Promise<void>;
}

class RealImmutableDocumentFileHandle implements ImmutableDocumentFileHandle {
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

class RealImmutableDocumentDirHandle implements ImmutableDocumentDirHandle {
  constructor(private readonly handle: FileHandle) {}

  async sync(): Promise<void> {
    await this.handle.sync();
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}

const realImmutableDocumentIoImplementation: ImmutableDocumentIo = {
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
    new RealImmutableDocumentFileHandle(
      await open(
        path,
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        0o600,
      ),
    ),
  openReadNoFollow: async (path) =>
    new RealImmutableDocumentFileHandle(await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)),
  openDir: async (path) => new RealImmutableDocumentDirHandle(await open(path, "r")),
  link: (existingPath, newPath) => link(existingPath, newPath),
  unlink: (path) => unlink(path),
  rmdir: (path) => rmdir(path),
};

/**
 * The single production IO of the immutable publication protocol: fixed at
 * module load, deep-immutable, and never replaced — the production
 * wrappers pass exactly this object on every call.
 */
export const realImmutableDocumentIo: ImmutableDocumentIo = Object.freeze(
  realImmutableDocumentIoImplementation,
);

const DOCUMENT_CHUNK = 128 * 1024;
const MAX_TEMP_ATTEMPTS = 8;

const ERRNO_CODE_PATTERN = /^[A-Z][A-Z0-9_]{0,31}$/;

/**
 * The errno code of a filesystem failure, rendered separately and without
 * the system message, so no path or payload text can leak into a store
 * diagnostic. Only the limited system-code form `[A-Z][A-Z0-9_]{0,31}` is
 * accepted; reading `cause.code` is getter-safe.
 */
export function immutableDocumentErrnoSuffix(cause: unknown): string {
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

export function immutableDocumentIoFailure(what: string, cause?: unknown): ImmutableDocumentStoreError {
  return new ImmutableDocumentStoreError(
    "not_published",
    "io_failure",
    cause === undefined ? what : `${what}${immutableDocumentErrnoSuffix(cause)}`,
  );
}

export function immutableDocumentInvalidLayout(message: string): ImmutableDocumentStoreError {
  return new ImmutableDocumentStoreError("not_published", "invalid_layout", message);
}

export function immutableDocumentConflict(message: string): ImmutableDocumentStoreError {
  return new ImmutableDocumentStoreError("not_published", "conflict", message);
}

export function describeImmutableObject(info: Stats): string {
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

/**
 * `fsync` one directory: open (`"r"`), sync, close. A close failure after
 * an otherwise successful sync is the failure itself; a close failure after
 * a failed sync never replaces the original failure. The caller assigns
 * the typed outcome.
 */
export async function fsyncImmutableDirectory(io: ImmutableDocumentIo, path: string): Promise<void> {
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
 * The canonical run root of an immutable document store: absolute,
 * existing, real non-symlink directory, canonically equal to its own
 * path. Never created or removed.
 */
export async function requireImmutableDocumentRunRoot(
  io: ImmutableDocumentIo,
  runRoot: string,
): Promise<string> {
  if (typeof runRoot !== "string" || !isAbsolute(runRoot)) {
    throw immutableDocumentInvalidLayout("the run root must be an absolute canonical path");
  }
  const info = await inspectOrNull(io, runRoot, "the run root");
  if (info === null) {
    throw immutableDocumentInvalidLayout("the run root does not exist");
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw immutableDocumentInvalidLayout(`the run root exists but is ${describeImmutableObject(info)}`);
  }
  let canonical: string;
  try {
    canonical = await io.realpath(runRoot);
  } catch (cause) {
    throw immutableDocumentIoFailure("the run root cannot be canonicalized", cause);
  }
  if (canonical !== runRoot) {
    throw immutableDocumentInvalidLayout("the run root is not canonical");
  }
  return canonical;
}

async function inspectOrNull(io: ImmutableDocumentIo, path: string, what: string): Promise<Stats | null> {
  try {
    return await io.lstatOrNull(path);
  } catch (cause) {
    throw immutableDocumentIoFailure(`${what} could not be inspected`, cause);
  }
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
 * recorded `dev`/`ino`, canonically resolving inside the parent
 * directory. A vanished temp is already gone; a substituted object is
 * never removed; every failure is swallowed and never masks the original
 * outcome.
 */
async function removeOwnedTempFile(
  io: ImmutableDocumentIo,
  tempPath: string,
  identity: OwnedTempFile,
  parentPath: string,
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
    if (canonical !== tempPath && !isInsideRoot(parentPath, canonical)) {
      return;
    }
    await io.unlink(tempPath);
  } catch {
    // Best-effort: a leftover temp file is the documented absence of crash
    // recovery; the next publication uses a fresh exclusive temp name.
  }
}

/**
 * Ensure one store-owned directory component inside a canonical parent:
 * exclusive creation with immediate identity fixation. Only a directory
 * that `mkdir` created in this very call is owned by the call: it is
 * chmod-enforced to mode 0700 (never trusted to the umask) and, on a
 * chmod failure, ownership-checked-removed. A directory created
 * concurrently by someone else (`mkdir` `EEXIST`) or found pre-existing
 * is adopted for verification only — real non-symlink directory, mode
 * 0700, canonical identity — and is never chmodded or removed. An
 * existing object that is not a real directory, or a directory with the
 * wrong mode, fails closed as an invalid layout; the mode is verified,
 * never silently fixed by adoption. Before every successful return the
 * parent directory is fsynced — whether the component was created here or
 * adopted — so the directory entry's crash durability is confirmed on
 * every accepted ensure; a parent fsync failure is a pre-publication
 * (not published) io failure and may leave the directory behind, which
 * the next call adopts without chmod and re-syncs before any document
 * file is published into it.
 */
export async function ensureImmutableDirectory(
  io: ImmutableDocumentIo,
  parentCanonical: string,
  dirName: string,
  dirNoun: string,
  parentNoun: string,
): Promise<string> {
  const dirPath = `${parentCanonical}/${dirName}`;
  let info = await inspectOrNull(io, dirPath, `the ${dirNoun}`);
  let mkdirSucceededHere = false;
  if (info === null) {
    try {
      await io.mkdirExclusive(dirPath);
      mkdirSucceededHere = true;
    } catch (cause) {
      if (!isErrnoException(cause, "EEXIST")) {
        throw immutableDocumentIoFailure(`the ${dirNoun} could not be created`, cause);
      }
    }
    const createdInfo = await inspectOrNull(io, dirPath, `the ${dirNoun}`);
    if (createdInfo === null) {
      throw immutableDocumentIoFailure(`the ${dirNoun} is not a real directory after creation`);
    }
    if (createdInfo.isSymbolicLink() || !createdInfo.isDirectory()) {
      if (mkdirSucceededHere) {
        throw immutableDocumentIoFailure(`the ${dirNoun} is not a real directory after creation`);
      }
      throw immutableDocumentInvalidLayout(
        `the ${dirNoun} exists but is ${describeImmutableObject(createdInfo)}`,
      );
    }
    info = createdInfo;
    if (mkdirSucceededHere) {
      try {
        await io.chmod(dirPath, 0o700);
      } catch (cause) {
        await removeOwnedEmptyDirectory(io, dirPath, createdInfo, parentCanonical);
        throw immutableDocumentIoFailure(`the ${dirNoun} could not be created`, cause);
      }
      info = await inspectOrNull(io, dirPath, `the ${dirNoun}`);
      if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
        throw immutableDocumentIoFailure(`the ${dirNoun} is not a real directory after creation`);
      }
    }
  } else if (info.isSymbolicLink() || !info.isDirectory()) {
    throw immutableDocumentInvalidLayout(`the ${dirNoun} exists but is ${describeImmutableObject(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o700) {
    throw immutableDocumentInvalidLayout(`the ${dirNoun} does not have the required mode 0700`);
  }
  let canonical: string;
  try {
    canonical = await io.realpath(dirPath);
  } catch (cause) {
    throw immutableDocumentIoFailure(`the ${dirNoun} cannot be canonicalized`, cause);
  }
  if (canonical !== dirPath) {
    throw immutableDocumentInvalidLayout(`the ${dirNoun} does not resolve to its canonical path`);
  }
  try {
    await fsyncImmutableDirectory(io, parentCanonical);
  } catch (cause) {
    throw immutableDocumentIoFailure(`the ${parentNoun} could not be synced`, cause);
  }
  return dirPath;
}

/**
 * Best-effort removal of the empty store-owned directory this call
 * created, after a chmod failure. The removal runs only with the recorded
 * ownership proof (same real non-symlink directory, same `dev`/`ino`,
 * canonically inside the parent); every failure is swallowed and never
 * replaces the original failure.
 */
async function removeOwnedEmptyDirectory(
  io: ImmutableDocumentIo,
  dirPath: string,
  identity: Stats,
  parentCanonical: string,
): Promise<void> {
  try {
    const info = await io.lstatOrNull(dirPath);
    if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
      return;
    }
    if (info.dev !== identity.dev || info.ino !== identity.ino) {
      return;
    }
    const canonical = await io.realpath(dirPath);
    if (canonical !== dirPath || !isInsideRoot(parentCanonical, canonical)) {
      return;
    }
    await io.rmdir(dirPath);
  } catch {
    // Best-effort: a leftover empty directory is harmless and is verified
    // (and adopted) by any later publication attempt.
  }
}

/**
 * Read-only verification of one store-owned directory component of a load
 * path: the component must be absent (→ `null`), or a real non-symlink
 * directory with mode 0700 canonically resolving to its own path. Nothing
 * is created, chmodded, linked, renamed or removed.
 */
export async function verifyStoredDirectoryComponent(
  io: ImmutableDocumentIo,
  parentCanonical: string,
  dirName: string,
  dirNoun: string,
): Promise<string | null> {
  const dirPath = `${parentCanonical}/${dirName}`;
  const info = await inspectOrNull(io, dirPath, `the ${dirNoun}`);
  if (info === null) {
    return null;
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw immutableDocumentInvalidLayout(
      `the ${dirNoun} exists but is ${describeImmutableObject(info)}`,
    );
  }
  if ((info.mode & 0o7777) !== 0o700) {
    throw immutableDocumentInvalidLayout(`the ${dirNoun} does not have the required mode 0700`);
  }
  let canonical: string;
  try {
    canonical = await io.realpath(dirPath);
  } catch (cause) {
    throw immutableDocumentIoFailure(`the ${dirNoun} cannot be canonicalized`, cause);
  }
  if (canonical !== dirPath) {
    throw immutableDocumentInvalidLayout(`the ${dirNoun} does not resolve to its canonical path`);
  }
  return dirPath;
}

/**
 * The immutable publication target of one document file: the final file
 * name inside the already ensured parent directory, the exact canonical
 * bytes, the digest, the exclusive temp-name prefix and stem, and the
 * deep-frozen content-free identity descriptor the adapter binds for its
 * candidates.
 */
export interface ImmutableDocumentTarget {
  readonly fileName: string;
  readonly tempPrefix: string;
  readonly tempStem: string;
  readonly canonicalJson: string;
  readonly sha256: string;
  readonly identity: ReadonlyJsonValue;
}

/**
 * Read one whole regular file through its already opened `O_NOFOLLOW`
 * handle: a full read loop (EOF ends it; a negative read count fails).
 * Close failures after a failed read never replace the original failure.
 */
async function readWholeFile(
  path: string,
  handle: ImmutableDocumentFileHandle,
  wording: ImmutableDocumentWording,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let failure: ImmutableDocumentStoreError | undefined;
  try {
    const stat = await handle.stat();
    if (!stat.isFile()) {
      throw immutableDocumentConflict(
        `the ${wording.document} file at ${path} is not a regular file after open`,
      );
    }
    const chunk = Buffer.allocUnsafe(DOCUMENT_CHUNK);
    let position = 0;
    for (;;) {
      let bytesRead: number;
      try {
        bytesRead = await handle.read(chunk, 0, chunk.length, position);
      } catch (cause) {
        throw immutableDocumentIoFailure(`the ${wording.document} file could not be read`, cause);
      }
      if (!Number.isSafeInteger(bytesRead) || bytesRead < 0) {
        throw immutableDocumentIoFailure(`the ${wording.document} file could not be read`);
      }
      if (bytesRead === 0) {
        break;
      }
      chunks.push(Buffer.from(chunk.subarray(0, bytesRead)));
      position += bytesRead;
    }
  } catch (cause) {
    if (cause instanceof ImmutableDocumentStoreError) {
      failure = cause;
    } else {
      failure = immutableDocumentIoFailure(`the ${wording.document} file could not be read`, cause);
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
    throw immutableDocumentIoFailure(
      `the ${wording.document} file could not be closed after reading`,
      closeFailure,
    );
  }
  return Buffer.concat(chunks);
}

/**
 * Read-only existence check of one stored document artifact: returns
 * `null` when the object at the fixed path is absent, or its lstat
 * otherwise. A non-ENOENT inspection failure is a typed io failure.
 */
export async function inspectStoredDocumentOrNull(
  io: ImmutableDocumentIo,
  path: string,
  fileNoun: string,
): Promise<Stats | null> {
  return await inspectOrNull(io, path, `the ${fileNoun}`);
}

/**
 * Read one stored document file with the full stored-file contract: it
 * must be a real non-symlink regular file with mode 0600; its bytes are
 * read through `O_NOFOLLOW`. `fileNoun` carries the adapter's own noun for
 * the stored-file messages; the read-loop messages use the wording's
 * document noun. The adapter owns all content binding (parse, digests,
 * run-id/identity checks) on the returned bytes.
 */
export async function readStoredImmutableDocument(
  io: ImmutableDocumentIo,
  path: string,
  fileNoun: string,
  wording: ImmutableDocumentWording,
): Promise<Buffer> {
  const info = await inspectOrNull(io, path, `the ${fileNoun}`);
  if (info === null) {
    throw immutableDocumentInvalidLayout(`the ${fileNoun} does not exist`);
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw immutableDocumentConflict(`the ${fileNoun} exists but is ${describeImmutableObject(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw immutableDocumentConflict(`the ${fileNoun} does not have the required mode 0600`);
  }
  let handle: ImmutableDocumentFileHandle;
  try {
    handle = await io.openReadNoFollow(path);
  } catch (cause) {
    throw immutableDocumentIoFailure(`the ${fileNoun} could not be opened`, cause);
  }
  return await readWholeFile(path, handle, wording);
}

/**
 * Idempotent adoption of an existing final file: it must be a real
 * non-symlink regular file with mode 0600 carrying exactly the canonical
 * bytes of this document. Equal bytes mean success — nothing is replaced,
 * repaired or re-moded — and the parent directory is fsynced again before
 * the return so an exact retry confirms durability. Anything else fails
 * closed as a typed conflict (or an io failure while inspecting); the
 * existing object is never touched.
 */
async function adoptExistingFinalFile(
  io: ImmutableDocumentIo,
  parentPath: string,
  finalPath: string,
  target: ImmutableDocumentTarget,
  wording: ImmutableDocumentWording,
  parentNoun: string,
): Promise<void> {
  const info = await inspectOrNull(io, finalPath, `the ${wording.document} target`);
  if (info === null) {
    throw immutableDocumentIoFailure(
      `the ${wording.document} target disappeared while it was being checked`,
    );
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw immutableDocumentConflict(
      `the ${wording.document} target exists but is ${describeImmutableObject(info)}`,
    );
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw immutableDocumentConflict(
      `the existing ${wording.document} file does not have the required mode 0600`,
    );
  }
  let handle: ImmutableDocumentFileHandle;
  try {
    handle = await io.openReadNoFollow(finalPath);
  } catch (cause) {
    throw immutableDocumentIoFailure(
      `the ${wording.document} file could not be opened for reading`,
      cause,
    );
  }
  const stored = await readWholeFile(finalPath, handle, wording);
  if (!stored.equals(Buffer.from(target.canonicalJson, "utf8"))) {
    throw immutableDocumentConflict(
      `the existing ${wording.document} file carries different canonical bytes`,
    );
  }
  try {
    await fsyncImmutableDirectory(io, parentPath);
  } catch {
    throw new ImmutableDocumentStoreError(
      "durability_unknown",
      "io_failure",
      `the ${parentNoun} could not be synced after adoption`,
      {
        identity: target.identity,
        final_path: finalPath,
        canonical_json: target.canonicalJson,
        sha256: target.sha256,
      },
    );
  }
}

/**
 * The durability-unknown error of the publication: the final file is
 * authoritative and is never rolled back; the candidate carries the
 * identity, the final path, the canonical JSON and the digest.
 */
function publicationDurabilityUnknown(
  target: ImmutableDocumentTarget,
  finalPath: string,
  message: string,
): ImmutableDocumentStoreError {
  return new ImmutableDocumentStoreError(
    "durability_unknown",
    "io_failure",
    message,
    {
      identity: target.identity,
      final_path: finalPath,
      canonical_json: target.canonicalJson,
      sha256: target.sha256,
    },
  );
}

/**
 * Publish one immutable document file into the ensured parent directory:
 * temp file with recorded ownership identity, full write-all loop, fsync,
 * close, then the exclusive `link()` publication, ownership-checked temp
 * removal and the parent-directory fsync. On `EEXIST` the existing object
 * is adopted (idempotent). Pre-link failures clean exactly the owned temp
 * file and keep the outcome `not_published`; post-link failures never roll
 * the final file back and become `durability_unknown` with the candidate.
 * Returns the final canonical path.
 */
export async function publishImmutableDocumentFile(
  io: ImmutableDocumentIo,
  parentPath: string,
  target: ImmutableDocumentTarget,
  wording: ImmutableDocumentWording,
  parentNoun: string,
): Promise<string> {
  const finalPath = `${parentPath}/${target.fileName}`;
  const bytes = Buffer.from(target.canonicalJson, "utf8");
  let tempPath = "";
  let identity: OwnedTempFile | null = null;
  for (let attempt = 0; attempt < MAX_TEMP_ATTEMPTS; attempt += 1) {
    tempPath = `${parentPath}/${target.tempPrefix}${target.tempStem}-${randomBytes(8).toString("hex")}`;
    let handle: ImmutableDocumentFileHandle;
    try {
      handle = await io.openTempExclusive(tempPath);
    } catch (cause) {
      if (isErrnoException(cause, "EEXIST")) {
        continue;
      }
      throw immutableDocumentIoFailure(
        `the ${wording.document} temp file could not be created`,
        cause,
      );
    }
    let workFailure: ImmutableDocumentStoreError | undefined;
    try {
      let stat: Stats;
      try {
        stat = await handle.stat();
      } catch (cause) {
        throw immutableDocumentIoFailure(
          `the ${wording.document} temp file could not be inspected`,
          cause,
        );
      }
      if (!stat.isFile()) {
        throw immutableDocumentIoFailure(
          `the ${wording.document} temp file is not a regular file after creation`,
        );
      }
      if ((stat.mode & 0o7777) !== 0o600) {
        throw immutableDocumentIoFailure(
          `the ${wording.document} temp file does not have the required mode 0600`,
        );
      }
      identity = { path: tempPath, dev: stat.dev, ino: stat.ino };
      let written = 0;
      while (written < bytes.byteLength) {
        const remaining = bytes.byteLength - written;
        let bytesWritten: number;
        try {
          bytesWritten = await handle.write(bytes, written, remaining, written);
        } catch (cause) {
          throw immutableDocumentIoFailure(`the ${wording.document} file could not be written`, cause);
        }
        if (!Number.isSafeInteger(bytesWritten) || bytesWritten <= 0 || bytesWritten > remaining) {
          throw immutableDocumentIoFailure(
            `the ${wording.document} file could not be written without progress`,
          );
        }
        written += bytesWritten;
      }
      try {
        await handle.sync();
      } catch (cause) {
        throw immutableDocumentIoFailure(
          `the ${wording.document} temp file could not be synced`,
          cause,
        );
      }
    } catch (cause) {
      workFailure =
        cause instanceof ImmutableDocumentStoreError
          ? cause
          : immutableDocumentIoFailure(`the ${wording.document} file could not be prepared`, cause);
    }
    let closeFailure: unknown = undefined;
    try {
      await handle.close();
    } catch (cause) {
      closeFailure = cause;
    }
    if (workFailure !== undefined) {
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, parentPath);
      }
      throw workFailure;
    }
    if (closeFailure !== undefined) {
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, parentPath);
      }
      throw immutableDocumentIoFailure(
        `the ${wording.document} temp file could not be closed`,
        closeFailure,
      );
    }
    try {
      await io.link(tempPath, finalPath);
    } catch (cause) {
      if (isErrnoException(cause, "EEXIST")) {
        if (identity !== null) {
          await removeOwnedTempFile(io, tempPath, identity, parentPath);
        }
        await adoptExistingFinalFile(io, parentPath, finalPath, target, wording, parentNoun);
        return finalPath;
      }
      if (identity !== null) {
        await removeOwnedTempFile(io, tempPath, identity, parentPath);
      }
      throw immutableDocumentIoFailure(`the ${wording.document} file could not be published`, cause);
    }
    // Post-link durability phase: the final file is authoritative and is
    // never rolled back. Every failure here becomes durability_unknown.
    try {
      const tempInfo = await io.lstatOrNull(tempPath);
      if (tempInfo !== null) {
        if (tempInfo.isSymbolicLink() || !tempInfo.isFile()) {
          throw publicationDurabilityUnknown(
            target,
            finalPath,
            `the published ${wording.document} temp file could not be confirmed as owned`,
          );
        }
        if (identity === null || tempInfo.dev !== identity.dev || tempInfo.ino !== identity.ino) {
          throw publicationDurabilityUnknown(
            target,
            finalPath,
            `the published ${wording.document} temp file could not be confirmed as owned`,
          );
        }
        try {
          await io.unlink(tempPath);
        } catch (unlinkCause) {
          if (!isErrnoException(unlinkCause, "ENOENT")) {
            throw publicationDurabilityUnknown(
              target,
              finalPath,
              `the published ${wording.document} temp file could not be removed`,
            );
          }
        }
      }
      try {
        await fsyncImmutableDirectory(io, parentPath);
      } catch {
        throw publicationDurabilityUnknown(
          target,
          finalPath,
          `the ${parentNoun} could not be synced after publication`,
        );
      }
    } catch (cause) {
      if (cause instanceof ImmutableDocumentStoreError && cause.outcome === "durability_unknown") {
        throw cause;
      }
      throw publicationDurabilityUnknown(
        target,
        finalPath,
        `the ${wording.document} publication durability could not be confirmed`,
      );
    }
    return finalPath;
  }
  throw immutableDocumentIoFailure(
    `the ${wording.document} temp file could not be created exclusively`,
  );
}
