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

export type FaultStep = "open" | "write" | "sync" | "rename" | "dirsync";

export function isTempStatePath(path: string): boolean {
  return basename(path).startsWith("state.json.tmp-");
}

/**
 * Wraps the real IO so that exactly one commit (1-based ordinal) fails at the
 * given protocol step. Every earlier commit and every later commit works.
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
        close: () => handle.close(),
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
      return await defaultPipelineStateIo.openDir(path);
    },
  };
  return io;
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
