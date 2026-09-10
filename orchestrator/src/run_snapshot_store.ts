import { lstat as lstatRaw, mkdir, open, rename, unlink, type FileHandle } from "node:fs/promises";
import { dirname } from "node:path";
import { describeError } from "./docker_helper.ts";

/**
 * Schema-agnostic substrate for durable, atomically updated revisioned JSON
 * snapshot stores. Every schema-specific store (pipeline v1 and pipeline v2)
 * is a thin typed adapter over this one file; there is exactly one
 * filesystem protocol implementation.
 *
 * Layout: `<state-root>/pipeline-runs/<run-id>/state.json` under the
 * operator-owned orchestrator state root (never inside the worker workspace,
 * the config root, or any directory mounted into a worker).
 *
 * Commit protocol per snapshot:
 *  1. private directories (`pipeline-runs`, `<run-id>`) created with 0700;
 *  2. a unique temporary file in the same directory, exclusively created
 *     (flag "wx") with mode 0600 (enforced again with an explicit chmod);
 *  3. the full JSON document written with a trailing newline;
 *  4. `fsync` on the file, then close;
 *  5. an atomic rename over the previous snapshot;
 *  6. `fsync` on the parent directory.
 * Any failure before the rename removes the temporary file and leaves the
 * previous committed snapshot byte-for-byte intact. The loader sees either
 * the previous or the new complete snapshot, never a partial one. Residual
 * temporary files are not run state and are ignored by the loader.
 *
 * Commit failures are reported as one of two typed outcomes:
 *
 * - `RunSnapshotStoreError` (`not_committed`): the rename did not happen;
 *   the previous committed snapshot is guaranteed to remain the authoritative
 *   one, byte-for-byte.
 * - `RunSnapshotDurabilityError` (`durability_unknown`): the rename
 *   succeeded, but the post-rename durability confirmation (`openDir`,
 *   directory `fsync`, or close) failed. The candidate revision is already
 *   visible at the state path, yet whether the previous or the candidate
 *   revision survives a crash is not guaranteed. The rename is never rolled
 *   back and no automatic recovery is implemented.
 *
 * The first write refuses to clobber an existing run. Later commits verify
 * the on-disk revision against the expected revision. Commits of one run are
 * serialized inside the process. Multi-process locking is intentionally out
 * of scope.
 */

export class RunSnapshotStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RunSnapshotStoreError";
  }
}

/**
 * `durability_unknown` commit outcome: the atomic rename over the previous
 * snapshot succeeded, but the post-rename directory fsync that would make it
 * durable failed. The candidate snapshot is already visible at the state
 * path; whether it survives a crash is unknown. Carries the exact candidate
 * revision and snapshot so the caller can adopt the visible state instead of
 * pretending the previous snapshot still survived.
 */
export class RunSnapshotDurabilityError<S> extends RunSnapshotStoreError {
  readonly revision: number;
  readonly candidate: S;

  constructor(revision: number, candidate: S, message: string) {
    super(message);
    this.name = "RunSnapshotDurabilityError";
    this.revision = revision;
    this.candidate = candidate;
  }
}

export interface StateFileHandle {
  chmod(mode: number): Promise<void>;
  writeAll(bytes: Uint8Array): Promise<void>;
  sync(): Promise<void>;
  close(): Promise<void>;
}

export interface StateDirHandle {
  sync(): Promise<void>;
  close(): Promise<void>;
}

/**
 * IO seam for the store. The default implementation uses the real filesystem;
 * tests inject deterministic failures and blocking points without sleeps.
 */
export interface PipelineStateIo {
  ensureDir(path: string, mode: number): Promise<void>;
  /** lstat: must be a directory and not a symlink, or rejects. */
  assertRealDirectory(path: string): Promise<void>;
  /** lstat, or null when the path does not exist. */
  lstat(path: string): Promise<{ isFile: boolean; isSymbolicLink: boolean } | null>;
  openExclusive(path: string, mode: number): Promise<StateFileHandle>;
  rename(from: string, to: string): Promise<void>;
  openDir(path: string): Promise<StateDirHandle>;
  unlinkIfExists(path: string): Promise<void>;
  readText(path: string): Promise<string>;
}

class RealStateFileHandle implements StateFileHandle {
  constructor(private readonly handle: FileHandle) {}

  async chmod(mode: number): Promise<void> {
    await this.handle.chmod(mode);
  }

  async writeAll(bytes: Uint8Array): Promise<void> {
    let written = 0;
    while (written < bytes.byteLength) {
      const result = await this.handle.write(
        bytes,
        written,
        bytes.byteLength - written,
      );
      if (result.bytesWritten <= 0) {
        throw new Error(`short write at offset ${written}`);
      }
      written += result.bytesWritten;
    }
  }

  async sync(): Promise<void> {
    await this.handle.sync();
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}

class RealStateDirHandle implements StateDirHandle {
  constructor(private readonly handle: FileHandle) {}

  async sync(): Promise<void> {
    await this.handle.sync();
  }

  async close(): Promise<void> {
    await this.handle.close();
  }
}

export const defaultPipelineStateIo: PipelineStateIo = {
  async ensureDir(path, mode) {
    await mkdir(path, { recursive: true, mode });
  },
  async assertRealDirectory(path) {
    const info = await lstatRaw(path, { throwIfNoEntry: false });
    if (info === undefined) {
      throw new RunSnapshotStoreError(`pipeline run state directory ${path} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new RunSnapshotStoreError(
        `pipeline run state directory ${path} is a symlink; symlinked state directories are rejected`,
      );
    }
    if (!info.isDirectory()) {
      throw new RunSnapshotStoreError(`pipeline run state directory ${path} is not a directory`);
    }
  },
  async lstat(path) {
    const info = await lstatRaw(path, { throwIfNoEntry: false });
    if (info === undefined) {
      return null;
    }
    return { isFile: info.isFile(), isSymbolicLink: info.isSymbolicLink() };
  },
  async openExclusive(path, mode) {
    return new RealStateFileHandle(await open(path, "wx", mode));
  },
  async rename(from, to) {
    await rename(from, to);
  },
  async openDir(path) {
    return new RealStateDirHandle(await open(path, "r"));
  },
  async unlinkIfExists(path) {
    try {
      await unlink(path);
    } catch (cause) {
      const code = (cause as { code?: string }).code;
      if (code !== "ENOENT") {
        throw cause;
      }
    }
  },
  async readText(path) {
    return await Bun.file(path).text();
  },
};

export function runSnapshotStatePath(stateRoot: string, runId: string): string {
  return `${stateRoot.replace(/\/+$/, "")}/pipeline-runs/${runId}/state.json`;
}

export interface RunSnapshotStoreOptions<S> {
  /** Parser for the load path; its own errors propagate unchanged. */
  parseSnapshot: (raw: string) => S;
  /** Document label used verbatim in every diagnostic message. */
  documentLabel: string;
}

interface WriteOptions {
  requireAbsent?: boolean;
  expectedRevision?: number;
}

export class RunSnapshotStore<S extends { revision: number }> {
  private readonly io: PipelineStateIo;
  private readonly parseSnapshot: (raw: string) => S;
  private readonly documentLabel: string;
  private chain: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly stateRoot: string,
    private readonly runId: string,
    options: RunSnapshotStoreOptions<S>,
    io: PipelineStateIo = defaultPipelineStateIo,
  ) {
    this.parseSnapshot = options.parseSnapshot;
    this.documentLabel = options.documentLabel;
    this.io = io;
  }

  get path(): string {
    return runSnapshotStatePath(this.stateRoot, this.runId);
  }

  private get runDir(): string {
    return dirname(this.path);
  }

  /**
   * Loads the committed snapshot, or null when no run state exists yet.
   * Rejects symlinks and every document the injected parser rejects.
   */
  async load(): Promise<S | null> {
    return await this.enqueue(async () => await this.loadNow());
  }

  private async loadNow(): Promise<S | null> {
    const file = this.path;
    const label = this.documentLabel;
    let info;
    try {
      info = await this.io.lstat(file);
    } catch (cause) {
      throw new RunSnapshotStoreError(
        `cannot inspect ${label} ${file}: ${describeError(cause)}`,
      );
    }
    if (info === null) {
      return null;
    }
    if (info.isSymbolicLink) {
      throw new RunSnapshotStoreError(
        `${label} ${file} is a symlink; symlinked state targets are rejected`,
      );
    }
    if (!info.isFile) {
      throw new RunSnapshotStoreError(`${label} ${file} is not a regular file`);
    }
    let raw: string;
    try {
      raw = await this.io.readText(file);
    } catch (cause) {
      throw new RunSnapshotStoreError(
        `cannot read ${label} ${file}: ${describeError(cause)}`,
      );
    }
    return this.parseSnapshot(raw);
  }

  /** First write of a run; refuses to overwrite an existing run. */
  async create(snapshot: S): Promise<void> {
    await this.enqueue(() => this.writeSnapshot(snapshot, { requireAbsent: true }));
  }

  /**
   * Subsequent write: the committed on-disk revision must equal
   * `expectedRevision` and `snapshot.revision` must be exactly one higher.
   */
  async commit(snapshot: S, expectedRevision: number): Promise<void> {
    await this.enqueue(async () => {
      const label = this.documentLabel;
      const committed = await this.loadNow();
      if (committed === null) {
        throw new RunSnapshotStoreError(
          `cannot commit revision ${snapshot.revision}: no ${label} exists at ${this.path}`,
        );
      }
      if (committed.revision !== expectedRevision) {
        throw new RunSnapshotStoreError(
          `cannot commit revision ${snapshot.revision}: the committed snapshot has revision ${committed.revision}, expected ${expectedRevision}`,
        );
      }
      if (snapshot.revision !== expectedRevision + 1) {
        throw new RunSnapshotStoreError(
          `cannot commit revision ${snapshot.revision}: it must be exactly ${expectedRevision + 1}`,
        );
      }
      await this.writeSnapshot(snapshot, {});
    });
  }

  /** Serializes commits of this run inside the process. */
  private async enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.chain.then(operation);
    this.chain = result.catch(() => undefined);
    return await result;
  }

  private async writeSnapshot(snapshot: S, options: WriteOptions): Promise<void> {
    const file = this.path;
    const dir = this.runDir;
    const label = this.documentLabel;
    if (options.requireAbsent === true) {
      let existing: { isFile: boolean; isSymbolicLink: boolean } | null;
      try {
        existing = await this.io.lstat(file);
      } catch (cause) {
        throw new RunSnapshotStoreError(
          `cannot inspect ${label} ${file}: ${describeError(cause)}`,
        );
      }
      if (existing !== null) {
        throw new RunSnapshotStoreError(
          `refusing to overwrite an existing ${label} at ${file}`,
        );
      }
    }
    const bytes = new TextEncoder().encode(`${JSON.stringify(snapshot, null, 2)}\n`);
    const tempPath = `${this.runDir}/state.json.tmp-${crypto.randomUUID()}`;
    let renamed = false;
    try {
      await this.io.ensureDir(dir, 0o700);
      await this.io.assertRealDirectory(dir);
      const handle = await this.io.openExclusive(tempPath, 0o600);
      try {
        await handle.chmod(0o600);
        await handle.writeAll(bytes);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await this.io.rename(tempPath, file);
      renamed = true;
      const dirHandle = await this.io.openDir(this.runDir);
      try {
        await dirHandle.sync();
      } finally {
        await dirHandle.close();
      }
    } catch (cause) {
      if (renamed) {
        // `durability_unknown`: the rename already happened, so the candidate
        // revision is visible at the state path, but its durability could not
        // be confirmed. Never roll the rename back, never claim the previous
        // snapshot survived, and report the candidate so the caller can adopt
        // the visible state.
        throw new RunSnapshotDurabilityError<S>(
          snapshot.revision,
          snapshot,
          `${label} ${file} was renamed to revision ${snapshot.revision}, but its durability could not be confirmed: ${describeError(cause)}`,
        );
      }
      try {
        await this.io.unlinkIfExists(tempPath);
      } catch {
        // best effort: the previous snapshot is intact either way
      }
      throw cause instanceof RunSnapshotStoreError
        ? cause
        : new RunSnapshotStoreError(
            `cannot commit ${label} at ${file}: ${describeError(cause)}`,
          );
    }
  }
}

/**
 * Translates a substrate store error into a schema-specific typed outcome:
 * `not_committed` errors keep their message, `durability_unknown` errors keep
 * revision, candidate, and message, and any other cause (including parser and
 * validator errors) propagates unchanged.
 */
export function retargetSnapshotStoreError<S>(
  cause: unknown,
  kit: {
    notCommitted(message: string): Error;
    durabilityUnknown(revision: number, candidate: S, message: string): Error;
  },
): unknown {
  if (cause instanceof RunSnapshotDurabilityError) {
    const durability = cause as RunSnapshotDurabilityError<S>;
    return kit.durabilityUnknown(durability.revision, durability.candidate, durability.message);
  }
  if (cause instanceof RunSnapshotStoreError) {
    return kit.notCommitted(cause.message);
  }
  return cause;
}
