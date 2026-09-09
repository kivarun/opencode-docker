import { lstat as lstatRaw, mkdir, open, rename, stat, unlink, type FileHandle } from "node:fs/promises";
import { dirname } from "node:path";
import { describeError } from "./docker_helper.ts";
import {
  parsePipelineRunState,
  type PipelineRunState,
} from "./pipeline_state.ts";

/**
 * Durable, atomically updated store for the pipeline run state.
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
 * - `PipelineStateStoreError` (`not_committed`): the rename did not happen;
 *   the previous committed snapshot is guaranteed to remain the authoritative
 *   one, byte-for-byte.
 * - `PipelineStateDurabilityError` (`durability_unknown`): the rename
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

export class PipelineStateStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineStateStoreError";
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
export class PipelineStateDurabilityError extends PipelineStateStoreError {
  readonly revision: number;
  readonly candidate: PipelineRunState;

  constructor(revision: number, candidate: PipelineRunState, message: string) {
    super(message);
    this.name = "PipelineStateDurabilityError";
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
      throw new PipelineStateStoreError(`pipeline run state directory ${path} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new PipelineStateStoreError(
        `pipeline run state directory ${path} is a symlink; symlinked state directories are rejected`,
      );
    }
    if (!info.isDirectory()) {
      throw new PipelineStateStoreError(`pipeline run state directory ${path} is not a directory`);
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

export function pipelineRunStatePath(stateRoot: string, runId: string): string {
  return `${stateRoot.replace(/\/+$/, "")}/pipeline-runs/${runId}/state.json`;
}

interface WriteOptions {
  requireAbsent?: boolean;
  expectedRevision?: number;
}

export class PipelineStateStore {
  private readonly io: PipelineStateIo;
  private chain: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly stateRoot: string,
    private readonly runId: string,
    io: PipelineStateIo = defaultPipelineStateIo,
  ) {
    this.io = io;
  }

  get path(): string {
    return pipelineRunStatePath(this.stateRoot, this.runId);
  }

  private get runDir(): string {
    return dirname(this.path);
  }

  /**
   * Loads the committed snapshot, or null when no run state exists yet.
   * Rejects symlinks and every document the exact-field validator rejects.
   */
  async load(): Promise<PipelineRunState | null> {
    return await this.enqueue(async () => await this.loadNow());
  }

  private async loadNow(): Promise<PipelineRunState | null> {
    const file = this.path;
    let info;
    try {
      info = await this.io.lstat(file);
    } catch (cause) {
      throw new PipelineStateStoreError(
        `cannot inspect pipeline run state ${file}: ${describeError(cause)}`,
      );
    }
    if (info === null) {
      return null;
    }
    if (info.isSymbolicLink) {
      throw new PipelineStateStoreError(
        `pipeline run state ${file} is a symlink; symlinked state targets are rejected`,
      );
    }
    if (!info.isFile) {
      throw new PipelineStateStoreError(`pipeline run state ${file} is not a regular file`);
    }
    let raw: string;
    try {
      raw = await this.io.readText(file);
    } catch (cause) {
      throw new PipelineStateStoreError(
        `cannot read pipeline run state ${file}: ${describeError(cause)}`,
      );
    }
    return parsePipelineRunState(raw);
  }

  /** First write of a run; refuses to overwrite an existing run. */
  async create(state: PipelineRunState): Promise<void> {
    await this.enqueue(() => this.writeSnapshot(state, { requireAbsent: true }));
  }

  /**
   * Subsequent write: the committed on-disk revision must equal
   * `expectedRevision` and `state.revision` must be exactly one higher.
   */
  async commit(state: PipelineRunState, expectedRevision: number): Promise<void> {
    await this.enqueue(async () => {
      const committed = await this.loadNow();
      if (committed === null) {
        throw new PipelineStateStoreError(
          `cannot commit revision ${state.revision}: no pipeline run state exists at ${this.path}`,
        );
      }
      if (committed.revision !== expectedRevision) {
        throw new PipelineStateStoreError(
          `cannot commit revision ${state.revision}: the committed snapshot has revision ${committed.revision}, expected ${expectedRevision}`,
        );
      }
      if (state.revision !== expectedRevision + 1) {
        throw new PipelineStateStoreError(
          `cannot commit revision ${state.revision}: it must be exactly ${expectedRevision + 1}`,
        );
      }
      await this.writeSnapshot(state, {});
    });
  }

  /** Serializes commits of this run inside the process. */
  private async enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.chain.then(operation);
    this.chain = result.catch(() => undefined);
    return await result;
  }

  private async writeSnapshot(state: PipelineRunState, options: WriteOptions): Promise<void> {
    const file = this.path;
    const dir = this.runDir;
    if (options.requireAbsent === true) {
      let existing: { isFile: boolean; isSymbolicLink: boolean } | null;
      try {
        existing = await this.io.lstat(file);
      } catch (cause) {
        throw new PipelineStateStoreError(
          `cannot inspect pipeline run state ${file}: ${describeError(cause)}`,
        );
      }
      if (existing !== null) {
        throw new PipelineStateStoreError(
          `refusing to overwrite an existing pipeline run state at ${file}`,
        );
      }
    }
    const bytes = new TextEncoder().encode(`${JSON.stringify(state, null, 2)}\n`);
    const tempPath = `${dir}/state.json.tmp-${crypto.randomUUID()}`;
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
      const dirHandle = await this.io.openDir(dir);
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
        throw new PipelineStateDurabilityError(
          state.revision,
          state,
          `pipeline run state ${file} was renamed to revision ${state.revision}, but its durability could not be confirmed: ${describeError(cause)}`,
        );
      }
      try {
        await this.io.unlinkIfExists(tempPath);
      } catch {
        // best effort: the previous snapshot is intact either way
      }
      throw cause instanceof PipelineStateStoreError
        ? cause
        : new PipelineStateStoreError(
            `cannot commit pipeline run state at ${file}: ${describeError(cause)}`,
          );
    }
  }
}
