import { basename } from "node:path";
import {
  defaultPipelineStateIo,
  type PipelineStateIo,
  type StateFileHandle,
} from "../src/pipeline_state_store.ts";

/**
 * Test-only IO seam helpers around the real pipeline state IO: deterministic
 * failure injection and rename gates without sleeps.
 */

export type FaultStep =
  | "open"
  | "write"
  | "sync"
  | "close"
  | "rename"
  | "dirsync"
  | "dirfsync"
  | "dirclose";

export function isTempStatePath(path: string): boolean {
  return basename(path).startsWith("state.json.tmp-");
}

/**
 * Wraps the real IO so that exactly one commit (1-based ordinal) fails at the
 * given protocol step. Every earlier commit and every later commit works.
 * Steps: `open`/`write`/`sync`/`close`/`rename` fail before the rename
 * (`not_committed`); `dirsync` fails opening the run directory after the
 * rename, `dirfsync` fails the directory fsync, and `dirclose` fails closing
 * the directory handle (`durability_unknown` outcomes).
 */
export function faultIo(options: {
  failCommit: number;
  failStep?: FaultStep;
  message?: string;
  error?: Error;
}): PipelineStateIo {
  const failStep = options.failStep ?? "write";
  const fault = options.error ?? new Error(
    options.message ?? `injected pipeline state failure at ${failStep} of commit ${options.failCommit}`,
  );
  let commit = 0;
  const io: PipelineStateIo = {
    ...defaultPipelineStateIo,
    async openExclusive(path, mode) {
      const handle = await defaultPipelineStateIo.openExclusive(path, mode);
      if (!isTempStatePath(path)) {
        return handle;
      }
      commit += 1;
      if (commit === options.failCommit && failStep === "open") {
        try {
          await handle.close();
        } catch {
          // best effort
        }
        throw fault;
      }
      if (commit !== options.failCommit) {
        return handle;
      }
      return {
        chmod: (mode) => handle.chmod(mode),
        writeAll: async (bytes) => {
          if (failStep === "write") {
            throw fault;
          }
          return await handle.writeAll(bytes);
        },
        sync: async () => {
          if (failStep === "sync") {
            throw fault;
          }
          return await handle.sync();
        },
        close: async () => {
          if (failStep === "close") {
            // Close the real handle first so no descriptor leaks; the fault
            // is still observed by the caller as a failed close.
            try {
              await handle.close();
            } catch {
              // best effort
            }
            throw fault;
          }
          return await handle.close();
        },
      };
    },
    async rename(from, to) {
      if (isTempStatePath(from) && commit === options.failCommit && failStep === "rename") {
        throw fault;
      }
      return await defaultPipelineStateIo.rename(from, to);
    },
    async openDir(path) {
      if (commit === options.failCommit && failStep === "dirsync") {
        throw fault;
      }
      const handle = await defaultPipelineStateIo.openDir(path);
      if (commit !== options.failCommit) {
        return handle;
      }
      return {
        sync: async () => {
          if (failStep === "dirfsync") {
            throw fault;
          }
          return await handle.sync();
        },
        close: async () => {
          if (failStep === "dirclose") {
            // Close the real handle first so no descriptor leaks; the fault
            // is still observed by the caller as a failed close.
            try {
              await handle.close();
            } catch {
              // best effort
            }
            throw fault;
          }
          return await handle.close();
        },
      };
    },
  };
  return io;
}

export interface IoCounts {
  /** Exclusive temp-file opens (one per write attempt). */
  tempOpens: number;
  /** Renames of a temp file over the state path (one per successful write). */
  renames: number;
  /** Directory fsyncs (one per successful commit). */
  dirSyncs: number;
}

/** Real IO that counts temp opens, renames, and directory fsyncs. */
export function countingIo(base: PipelineStateIo = defaultPipelineStateIo): {
  io: PipelineStateIo;
  counts: IoCounts;
} {
  const counts: IoCounts = { tempOpens: 0, renames: 0, dirSyncs: 0 };
  const io: PipelineStateIo = {
    ...base,
    async openExclusive(path, mode) {
      if (isTempStatePath(path)) {
        counts.tempOpens += 1;
      }
      return await base.openExclusive(path, mode);
    },
    async rename(from, to) {
      if (isTempStatePath(from)) {
        counts.renames += 1;
      }
      return await base.rename(from, to);
    },
    async openDir(path) {
      const handle = await base.openDir(path);
      return {
        sync: async () => {
          await handle.sync();
          counts.dirSyncs += 1;
        },
        close: () => handle.close(),
      };
    },
  };
  return { io, counts };
}

export interface GateControl {
  io: PipelineStateIo;
  /** Resolves when the gated commit reached its rename (write still in flight). */
  reached: Promise<void>;
  release(): void;
}

/**
 * Blocks the rename of exactly one commit (1-based ordinal) until released,
 * deterministically: the test awaits `reached`, then acts (e.g. records a
 * signal), then releases.
 */
export function gateIoAtRename(commitNumber: number): GateControl {
  let commit = 0;
  let notifyReached: () => void = () => {};
  const reached = new Promise<void>((resolve) => {
    notifyReached = resolve;
  });
  let releaseGate: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });
  const io: PipelineStateIo = {
    ...defaultPipelineStateIo,
    async rename(from, to) {
      if (isTempStatePath(from)) {
        commit += 1;
        if (commit === commitNumber) {
          notifyReached();
          await gate;
        }
      }
      return await defaultPipelineStateIo.rename(from, to);
    },
  };
  return { io, reached, release: releaseGate };
}

export type { StateFileHandle };
