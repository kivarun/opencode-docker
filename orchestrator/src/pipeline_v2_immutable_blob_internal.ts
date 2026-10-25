import { type Stats } from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import {
  isErrnoException,
} from "./fs_checks.ts";
import {
  fsyncImmutableDirectory,
  immutableDocumentErrnoSuffix,
  describeImmutableObject,
  removeOwnedTempFile,
  type ImmutableDocumentIo,
  type ImmutableDocumentFileHandle,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import type { ReadonlyJsonValue } from "./pipeline_v2_immutable_document_store_internal.ts";

/**
 * The single owner of streaming immutable-blob publication: one shared
 * low-level protocol for large raw payload artifacts whose bytes must
 * never be held whole in memory. It reuses — never duplicates — the
 * immutable-document substrate's IO capability, directory semantics,
 * diagnostic discipline and durability contract:
 *
 *   1. an exclusive owned temp file (`O_CREAT|O_EXCL|O_NOFOLLOW`, mode
 *      0600) inside the final parent with immediate `dev`/`ino` identity
 *      fixation and a write-all loop per chunk (partial writes advance
 *      the position; a zero-progress, negative or impossible count fails
 *      after exactly one attempt);
 *   2. file fsync, close, the exclusive `link()` publication (never a
 *      replace-capable rename), ownership-checked temp removal and the
 *      parent-directory fsync;
 *   3. idempotent adoption on `link()` `EEXIST`: the existing object must
 *      be a real non-symlink regular file with mode 0600 whose streaming
 *      digest and byte count equal the expected content contract; an
 *      exact retry after a former durability-unknown confirms it;
 *      anything else is a typed conflict and nothing is overwritten;
 *   4. `durability_unknown` after the link: the final blob is
 *      authoritative and never rolled back; the error carries the frozen
 *      candidate (identity, final path, digest, byte count);
 *   5. the read side: real non-symlink regular file, mode 0600, read
 *      through `O_NOFOLLOW` in one bounded-chunk pass whose digest must
 *      equal the expectation — the verification used both for read-back
 *      after publication and by the package loader.
 *
 * There is no second copy of this protocol anywhere; the only per-call
 * capability is the shared `ImmutableDocumentIo`. Diagnostics carry only
 * a controlled operation class and the limited errno suffix — never blob
 * bytes, host paths beyond the caller-supplied fixed paths, or
 * credentials.
 */

export type ImmutableBlobFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type ImmutableBlobOutcome = "not_published" | "durability_unknown";

export interface ImmutableBlobCandidate {
  readonly identity: ReadonlyJsonValue;
  readonly final_path: string;
  readonly sha256: string;
  readonly byte_count: number;
}

export class ImmutableBlobStoreError extends Error {
  readonly outcome: ImmutableBlobOutcome;
  readonly reason: ImmutableBlobFailureReason;
  readonly candidate?: ImmutableBlobCandidate;

  constructor(
    outcome: ImmutableBlobOutcome,
    reason: ImmutableBlobFailureReason,
    message: string,
    candidate?: ImmutableBlobCandidate,
  ) {
    super(message);
    this.name = "ImmutableBlobStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreezeValue(candidate);
    }
  }
}

function immutableBlobConflict(message: string): ImmutableBlobStoreError {
  return new ImmutableBlobStoreError("not_published", "conflict", message);
}

function immutableBlobIoFailure(message: string, cause?: unknown): ImmutableBlobStoreError {
  if (cause === undefined) {
    return new ImmutableBlobStoreError("not_published", "io_failure", message);
  }
  return new ImmutableBlobStoreError("not_published", "io_failure", `${message}${immutableDocumentErrnoSuffix(cause)}`);
}

export interface ImmutableBlobContentContract {
  readonly sha256: string;
  readonly byteCount: number;
}

export interface ImmutableBlobTemp {
  readonly tempPath: string;
  readonly identity: { readonly dev: number; readonly ino: number };
  /** Streams one chunk into the temp file (write-all loop). */
  readonly writeChunk: (chunk: Uint8Array) => Promise<void>;
  /** fsync + close of the fully written temp file. */
  readonly syncAndClose: () => Promise<void>;
  /** Best-effort ownership-checked removal after a failure before publication. */
  readonly discard: () => Promise<void>;
}

const BLOB_CHUNK = 128 * 1024;
const MAX_TEMP_ATTEMPTS = 8;

export async function openImmutableBlobTemp(
  io: ImmutableDocumentIo,
  parentPath: string,
  tempPrefix: string,
  tempStem: string,
  wording: ImmutableDocumentWording,
): Promise<ImmutableBlobTemp> {
  let tempPath = "";
  let identity: { dev: number; ino: number } | null = null;
  let handle: ImmutableDocumentFileHandle | null = null;
  for (let attempt = 0; attempt < MAX_TEMP_ATTEMPTS; attempt += 1) {
    tempPath = `${parentPath}/${tempPrefix}${tempStem}-${randomBytes(8).toString("hex")}`;
    try {
      handle = await io.openTempExclusive(tempPath);
      break;
    } catch (cause) {
      handle = null;
      if (isErrnoException(cause, "EEXIST")) {
        continue;
      }
      throw immutableBlobIoFailure(`the ${wording.document} temp file could not be created`, cause);
    }
  }
  if (handle === null) {
    throw immutableBlobIoFailure(`the ${wording.document} temp file could not be created exclusively`);
  }
  let stat: Stats;
  try {
    stat = await handle.stat();
  } catch (cause) {
    await safeClose(handle);
    throw immutableBlobIoFailure(`the ${wording.document} temp file could not be inspected`, cause);
  }
  if (!stat.isFile()) {
    await safeClose(handle);
    throw immutableBlobIoFailure(`the ${wording.document} temp file is not a regular file after creation`);
  }
  if ((stat.mode & 0o7777) !== 0o600) {
    await safeClose(handle);
    throw immutableBlobIoFailure(`the ${wording.document} temp file does not have the required mode 0600`);
  }
  identity = { dev: stat.dev, ino: stat.ino };
  let filePosition = 0;
  return {
    tempPath,
    identity,
    writeChunk: async (chunk: Uint8Array) => {
      let written = 0;
      while (written < chunk.byteLength) {
        const remaining = chunk.byteLength - written;
        let bytesWritten: number;
        try {
          bytesWritten = await handle!.write(chunk, written, remaining, filePosition + written);
        } catch (cause) {
          throw immutableBlobIoFailure(`the ${wording.document} temp file could not be written`, cause);
        }
        if (!Number.isSafeInteger(bytesWritten) || bytesWritten <= 0 || bytesWritten > remaining) {
          throw immutableBlobIoFailure(
            `the ${wording.document} temp file could not be written without progress`,
          );
        }
        written += bytesWritten;
      }
      filePosition += chunk.byteLength;
    },
    discard: async () => {
      // The handle must be closed before (or without) the unlink: an open
      // FileHandle left to the garbage collector is an error on newer
      // runtimes, and every pre-publication failure path discards.
      try {
        await handle!.close();
      } catch {
        // best effort — the unlink below still runs on its own proof
      }
      if (identity === null) {
        return;
      }
      await removeOwnedTempFile(io, tempPath, { path: tempPath, dev: identity.dev, ino: identity.ino }, parentPath);
    },
    syncAndClose: async () => {
      try {
        await handle!.sync();
      } catch (cause) {
        await safeClose(handle);
        throw immutableBlobIoFailure(`the ${wording.document} temp file could not be synced`, cause);
      }
      try {
        await handle!.close();
      } catch (cause) {
        throw immutableBlobIoFailure(`the ${wording.document} temp file could not be closed`, cause);
      }
    },
  };
}

async function safeClose(handle: ImmutableDocumentFileHandle): Promise<void> {
  try {
    await handle.close();
  } catch {
    // best effort on an already-failing path
  }
}

function blobDurabilityUnknown(
  expected: ImmutableBlobContentContract,
  identity: ReadonlyJsonValue,
  finalPath: string,
  message: string,
): ImmutableBlobStoreError {
  return new ImmutableBlobStoreError("durability_unknown", "io_failure", message, {
    identity,
    final_path: finalPath,
    sha256: expected.sha256,
    byte_count: expected.byteCount,
  });
}

/**
 * Publishes a fully written and fsynced owned temp blob: exclusive link,
 * `EEXIST` adoption by digest/size verification, ownership-checked temp
 * removal and the parent-directory fsync. The temp is cleaned up on every
 * pre-publication failure (ownership proof first); post-link failures
 * become `durability_unknown` and never roll the final blob back.
 */
export async function publishImmutableBlob(
  io: ImmutableDocumentIo,
  parentPath: string,
  temp: ImmutableBlobTemp,
  finalName: string,
  expected: ImmutableBlobContentContract,
  wording: ImmutableDocumentWording,
  parentNoun: string,
): Promise<string> {
  const finalPath = `${parentPath}/${finalName}`;
  try {
    await io.link(temp.tempPath, finalPath);
  } catch (cause) {
    if (isErrnoException(cause, "EEXIST")) {
      await removeOwnedTempFile(io, temp.tempPath, { path: temp.tempPath, dev: temp.identity.dev, ino: temp.identity.ino }, parentPath);
      await adoptExistingBlob(io, parentPath, finalPath, expected, wording, parentNoun);
      return finalPath;
    }
    await removeOwnedTempFile(io, temp.tempPath, { path: temp.tempPath, dev: temp.identity.dev, ino: temp.identity.ino }, parentPath);
    throw immutableBlobIoFailure(`the ${wording.document} blob could not be published`, cause);
  }
  try {
    const tempInfo = await io.lstatOrNull(temp.tempPath);
    if (tempInfo !== null) {
      if (tempInfo.isSymbolicLink() || !tempInfo.isFile()) {
        throw blobDurabilityUnknown(expected, deepFreezeValue({ dev: temp.identity.dev, ino: temp.identity.ino }), finalPath, `the published ${wording.document} temp file could not be confirmed as owned`);
      }
      if (tempInfo.dev !== temp.identity.dev || tempInfo.ino !== temp.identity.ino) {
        throw blobDurabilityUnknown(expected, deepFreezeValue({ dev: temp.identity.dev, ino: temp.identity.ino }), finalPath, `the published ${wording.document} temp file could not be confirmed as owned`);
      }
      try {
        await io.unlink(temp.tempPath);
      } catch (unlinkCause) {
        if (!isErrnoException(unlinkCause, "ENOENT")) {
          throw blobDurabilityUnknown(expected, deepFreezeValue({ dev: temp.identity.dev, ino: temp.identity.ino }), finalPath, `the published ${wording.document} temp file could not be removed`);
        }
      }
    }
    try {
      await fsyncImmutableDirectory(io, parentPath);
    } catch {
      throw blobDurabilityUnknown(expected, deepFreezeValue({ dev: temp.identity.dev, ino: temp.identity.ino }), finalPath, `the ${parentNoun} could not be synced after publication`);
    }
  } catch (cause) {
    if (cause instanceof ImmutableBlobStoreError && cause.outcome === "durability_unknown") {
      throw cause;
    }
    throw blobDurabilityUnknown(expected, deepFreezeValue({ dev: temp.identity.dev, ino: temp.identity.ino }), finalPath, `the ${wording.document} publication durability could not be confirmed`);
  }
  return finalPath;
}

async function adoptExistingBlob(
  io: ImmutableDocumentIo,
  parentPath: string,
  finalPath: string,
  expected: ImmutableBlobContentContract,
  wording: ImmutableDocumentWording,
  parentNoun: string,
): Promise<void> {
  await verifyStoredImmutableBlob(io, finalPath, expected, wording);
  try {
    await fsyncImmutableDirectory(io, parentPath);
  } catch {
    throw blobDurabilityUnknown(
      expected,
      { adopted: true },
      finalPath,
      `the ${parentNoun} could not be synced after adoption`,
    );
  }
}

/**
 * The full stored-blob contract: a real non-symlink regular file with mode
 * 0600 whose one bounded-chunk read pass produces exactly the expected
 * digest and byte count. No whole blob is ever held in memory.
 */
export async function verifyStoredImmutableBlob(
  io: ImmutableDocumentIo,
  path: string,
  expected: ImmutableBlobContentContract,
  wording: ImmutableDocumentWording,
): Promise<void> {
  const info = await io.lstatOrNull(path).catch((cause: unknown) => {
    throw immutableBlobIoFailure(`the ${wording.document} blob could not be inspected`, cause);
  });
  if (info === null) {
    throw immutableBlobConflict(`the ${wording.document} blob does not exist`);
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw immutableBlobConflict(`the ${wording.document} blob exists but is ${describeImmutableObject(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw immutableBlobConflict(`the ${wording.document} blob does not have the required mode 0600`);
  }
  if (info.size !== expected.byteCount) {
    throw immutableBlobConflict(`the ${wording.document} blob does not have the expected byte count`);
  }
  let handle: ImmutableDocumentFileHandle;
  try {
    handle = await io.openReadNoFollow(path);
  } catch (cause) {
    throw immutableBlobIoFailure(`the ${wording.document} blob could not be opened`, cause);
  }
  const hasher = createHash("sha256");
  const chunk = Buffer.allocUnsafe(BLOB_CHUNK);
  let byteCount = 0;
  let failure: unknown = undefined;
  try {
    for (;;) {
      let bytesRead: number;
      try {
        bytesRead = await handle.read(chunk, 0, chunk.byteLength, byteCount);
      } catch (cause) {
        throw immutableBlobIoFailure(`the ${wording.document} blob could not be read`, cause);
      }
      if (!Number.isSafeInteger(bytesRead) || bytesRead < 0) {
        throw immutableBlobIoFailure(`the ${wording.document} blob could not be read`);
      }
      if (bytesRead === 0) {
        break;
      }
      hasher.update(chunk.subarray(0, bytesRead));
      byteCount += bytesRead;
    }
  } catch (cause) {
    failure = cause;
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
    throw immutableBlobIoFailure(`the ${wording.document} blob could not be closed after reading`, closeFailure);
  }
  if (byteCount !== expected.byteCount) {
    throw immutableBlobConflict(`the ${wording.document} blob does not have the expected byte count`);
  }
  const sha256 = hasher.digest("hex");
  if (sha256 !== expected.sha256) {
    throw immutableBlobConflict(`the ${wording.document} blob does not match its expected digest`);
  }
}

