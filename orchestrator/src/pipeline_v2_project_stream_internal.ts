/**
 * The one streaming regular-file observation owner of the read-only
 * Project Changes / Project Change Content layer.
 *
 * Contract: the observed file is opened exactly once with `O_NOFOLLOW`,
 * fstat'ed before the first read, read sequentially from offset 0 to EOF
 * in bounded chunks while a SHA-256 digest and the byte count are
 * computed over the actual stream, an optional bounded slice is copied
 * out of that same stream, and the file is fstat'ed again after the last
 * read. The result — including any captured bytes — leaves this owner
 * only after every check passed: the post-read identity (dev, ino, kind,
 * mode, size, nlink, mtime, ctime) must match the pre-read identity and,
 * when an expectation is given, the full digest and byte count of the
 * actually read stream must equal it. A failure destroys the captured
 * bytes and surfaces one typed error; a partial response is impossible
 * by construction.
 *
 * Memory bound: one fixed read chunk (128 KiB) plus, when a capture is
 * requested, one buffer bounded by the requested limit. No whole-file
 * array is ever built.
 *
 * Residual honesty: a trusted host process that writes the file within
 * the same clock millisecond as the reads, without changing its size,
 * can defeat the mtime/ctime comparison — the same best-effort window
 * the project provenance scanner documents. The digest is computed over
 * the bytes this pass actually read, so a slice can never be paired with
 * a digest of different bytes.
 */
import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { open } from "node:fs/promises";
import type { Stats } from "node:fs";

/** The fixed read chunk of every streaming observation (128 KiB). */
export const PROJECT_FILE_STREAM_CHUNK_BYTES = 128 * 1024;

/** The typed failure of one streaming observation. `unstable` marks a
 * detected change or substitution of the observed object; `io` marks an
 * unexpected filesystem error. Diagnostics are content-free. */
export class ProjectFileStreamError extends Error {
  declare readonly reason: "unstable" | "io";

  constructor(reason: "unstable" | "io", message: string) {
    super(message);
    this.name = "ProjectFileStreamError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** The minimal file-handle capability the observation needs. */
export interface ProjectFileStreamHandle {
  fstat(): Promise<Stats>;
  read(buffer: Buffer, offset: number, length: number, position: number): Promise<{ readonly bytesRead: number }>;
  close(): Promise<void>;
}

/** The per-call filesystem capability of the streaming observation. */
export interface ProjectFileStreamIo {
  readonly openNoFollow: (path: string) => Promise<ProjectFileStreamHandle>;
}

class RealProjectFileStreamHandle implements ProjectFileStreamHandle {
  constructor(private readonly handle: import("node:fs/promises").FileHandle) {}

  fstat(): Promise<Stats> {
    return this.handle.stat();
  }

  read(buffer: Buffer, offset: number, length: number, position: number): Promise<{ readonly bytesRead: number }> {
    return this.handle.read(buffer, offset, length, position);
  }

  close(): Promise<void> {
    return this.handle.close();
  }
}

const realProjectFileStreamIoImplementation: ProjectFileStreamIo = {
  openNoFollow: async (path: string) => new RealProjectFileStreamHandle(await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW)),
};

/** The fixed immutable real implementation of the stream IO. */
export const realProjectFileStreamIo: ProjectFileStreamIo = Object.freeze(realProjectFileStreamIoImplementation);

/** The verified identity of one streamed regular file. */
export interface ObservedRegularFileStream {
  readonly identity: { readonly dev: number; readonly ino: number };
  readonly mode: number;
  readonly byteCount: number;
  readonly sha256: string;
  /** The post-read fstat metadata (used by the scanner's own comparison). */
  readonly stat: { readonly size: number; readonly nlink: number; readonly mtimeMs: number; readonly ctimeMs: number };
  /** Present only when a capture window was requested; possibly empty. */
  readonly captured?: Buffer;
}

export interface ObserveRegularFileStreamOptions {
  /** The expected content contract; a mismatch is an unstable failure. */
  readonly expected?: { readonly byteCount: number; readonly sha256: string };
  /** The bounded raw-byte window copied from the same stream. */
  readonly capture?: { readonly offset: number; limit: number };
  /**
   * Optional streaming sink receiving every read chunk exactly once, in
   * order, before the digest is updated. A sink failure is an `io` failure
   * of the observation; the caller owns any artifact the sink writes into
   * (a failed observation never confirms the sink's bytes).
   */
  readonly sink?: (chunk: Uint8Array) => Promise<void>;
}

function identityFieldsOf(info: Stats): string {
  return `${info.dev}:${info.ino}:${info.mode & 0o777}:${info.size}:${info.nlink}:${info.mtimeMs}:${info.ctimeMs}`;
}

function isErrnoExceptionWith(value: unknown, code: string): boolean {
  return typeof value === "object" && value !== null && (value as { code?: unknown }).code === code;
}

function asStreamError(cause: unknown, message: string): ProjectFileStreamError {
  if (cause instanceof ProjectFileStreamError) {
    return cause;
  }
  return new ProjectFileStreamError("io", message);
}

/**
 * Observes one regular file with a single sequential verified pass. See
 * the module contract above.
 */
export async function observeRegularFileStream(
  io: ProjectFileStreamIo,
  path: string,
  options: ObserveRegularFileStreamOptions = {},
): Promise<ObservedRegularFileStream> {
  let handle: ProjectFileStreamHandle;
  try {
    handle = await io.openNoFollow(path);
  } catch (cause) {
    if (isErrnoExceptionWith(cause, "ENOENT") || isErrnoExceptionWith(cause, "ELOOP") || isErrnoExceptionWith(cause, "ENOTDIR")) {
      throw new ProjectFileStreamError("unstable", "the observed file was replaced or disappeared before it could be opened");
    }
    throw new ProjectFileStreamError("io", "the observed file could not be opened");
  }
  try {
    const before = await handle.fstat().catch((cause: unknown) => {
      throw asStreamError(cause, "the observed file could not be inspected");
    });
    if (!before.isFile()) {
      throw new ProjectFileStreamError("unstable", "the observed object is not a regular file");
    }
    if (options.expected !== undefined && before.size !== options.expected.byteCount) {
      throw new ProjectFileStreamError("unstable", "the observed file size no longer matches the verified expectation");
    }
    const hasher = createHash("sha256");
    const chunk = Buffer.allocUnsafe(PROJECT_FILE_STREAM_CHUNK_BYTES);
    const capture = options.capture;
    const sink = options.sink;
    const captured = capture === undefined ? undefined : Buffer.alloc(capture.limit);
    let capturedLength = 0;
    let byteCount = 0;
    for (;;) {
      const { bytesRead } = await handle.read(chunk, 0, chunk.byteLength, byteCount).catch((cause: unknown) => {
        throw asStreamError(cause, "the observed file could not be read");
      });
      if (bytesRead === 0) {
        break;
      }
      if (bytesRead < 0) {
        throw new ProjectFileStreamError("io", "the observed file could not be read");
      }
      const window = chunk.subarray(0, bytesRead);
      if (sink !== undefined) {
        try {
          await sink(window);
        } catch (cause) {
          throw asStreamError(cause, "the streaming sink rejected a chunk of the observed file");
        }
      }
      hasher.update(window);
      if (captured !== undefined && capture !== undefined) {
        const windowStart = capture.offset;
        const windowEnd = capture.offset + capture.limit;
        const chunkStart = byteCount;
        const chunkEnd = byteCount + bytesRead;
        const from = Math.max(chunkStart, windowStart);
        const to = Math.min(chunkEnd, windowEnd);
        if (to > from) {
          chunk.copy(captured, capturedLength, from - chunkStart, to - chunkStart);
          capturedLength += to - from;
        }
      }
      byteCount += bytesRead;
    }
    const after = await handle.fstat().catch((cause: unknown) => {
      throw asStreamError(cause, "the observed file could not be inspected");
    });
    if (!after.isFile()) {
      throw new ProjectFileStreamError("unstable", "the observed object was replaced during the read");
    }
    if (identityFieldsOf(before) !== identityFieldsOf(after)) {
      throw new ProjectFileStreamError("unstable", "the observed file changed during the read");
    }
    const sha256 = hasher.digest("hex");
    if (options.expected !== undefined && (byteCount !== options.expected.byteCount || sha256 !== options.expected.sha256)) {
      throw new ProjectFileStreamError("unstable", "the observed file content no longer matches the verified expectation");
    }
    const result: {
      identity: { dev: number; ino: number };
      mode: number;
      byteCount: number;
      sha256: string;
      stat: { size: number; nlink: number; mtimeMs: number; ctimeMs: number };
      captured?: Buffer;
    } = {
      identity: { dev: before.dev, ino: before.ino },
      mode: before.mode & 0o777,
      byteCount,
      sha256,
      stat: { size: after.size, nlink: after.nlink, mtimeMs: after.mtimeMs, ctimeMs: after.ctimeMs },
    };
    if (captured !== undefined) {
      result.captured = captured.subarray(0, capturedLength);
    }
    return result;
  } finally {
    await handle.close();
  }
}
