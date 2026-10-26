/**
 * Child helper for the apply-writer race matrix. Runs one execution of a
 * prepared operation (or holds the project lock) in a separate OS
 * process, with stdin-gated pausing for mid-execution observation.
 *
 * usage: bun apply_writer_race_child.ts <mode> <applyStoreRoot> <applyId> <projectRoot>
 *   modes: execute | execute-gated | hold
 */
import { applyPreparedPipelineV2Project } from "../src/pipeline_v2_project_apply.ts";
import { defaultPipelineStateIo, type PipelineStateIo, type StateFileHandle } from "../src/run_snapshot_store.ts";
import { isApplyCrashFault } from "../src/pipeline_v2_apply_crash_internal.ts";

const [, , mode, applyStoreRoot, applyId, projectRoot] = process.argv as [string, string, string, string, string, string];

function readStdinLine(): Promise<string> {
  return new Promise((resolve) => {
    process.stdin.resume();
    process.stdin.once("data", (chunk) => {
      process.stdin.pause();
      resolve(String(chunk).trim());
    });
  });
}

function gatedStateIo(): PipelineStateIo {
  const inner = defaultPipelineStateIo;
  let gateArmed = true;
  let gatePending = false;
  return {
    ...inner,
    openExclusive: async (path: string, m: number): Promise<StateFileHandle> => {
      const handle = await inner.openExclusive(path, m);
      return {
        chmod: (mm: number) => handle.chmod(mm),
        writeAll: async (bytes: Uint8Array) => {
          const document = JSON.parse(Buffer.from(bytes).toString("utf8")) as Record<string, unknown>;
          const execution = document["execution"] as Record<string, unknown> | null;
          const gated =
            gateArmed &&
            document["status"] === "applying" &&
            execution !== null &&
            execution["active_step"] === null &&
            execution["completed_step_count"] === 1;
          if (gated) {
            gateArmed = false;
          }
          await handle.writeAll(bytes);
          if (gated) {
            gatePending = true;
          }
        },
        sync: () => handle.sync(),
        close: () => handle.close(),
      };
    },
    rename: async (from: string, to: string) => {
      await inner.rename(from, to);
      if (gatePending) {
        gatePending = false;
        process.stdout.write("GATED\n");
        await readStdinLine();
      }
    },
  } as PipelineStateIo;
}

async function main(): Promise<void> {
  if (mode === "hold") {
    const { acquirePipelineV2ApplyProjectLock } = await import("../src/pipeline_v2_apply_project_lock_internal.ts");
    const acquisition = acquirePipelineV2ApplyProjectLock(projectRoot);
    if (acquisition.busy) {
      process.stdout.write("BUSY\n");
      return;
    }
    process.stdout.write("HOLD\n");
    const line = await readStdinLine();
    if (line === "kill") {
      process.kill(process.pid, "SIGKILL");
    }
    acquisition.lock.release();
    process.stdout.write("RELEASED\n");
    return;
  }
  const options = { applyStoreRoot, applyId, projectId: "proj1", projectRoot };
  if (mode === "execute-gated") {
    const { applyPreparedProjectCore } = await import("../src/pipeline_v2_project_apply_writer_internal.ts");
    const { realApplyFdOps } = await import("../src/pipeline_v2_apply_fd_internal.ts");
    const { realImmutableDocumentIo, realProjectFileStreamIo } = await import("./apply_fixtures.ts");
    const result = await applyPreparedProjectCore(options, {
      docIo: realImmutableDocumentIo,
      stateIo: gatedStateIo(),
      fileStreamIo: realProjectFileStreamIo,
      fdOps: realApplyFdOps,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    return;
  }
  if (mode.startsWith("execute-crash-")) {
    const nth = Number(mode.slice("execute-crash-".length));
    const { applyPreparedProjectCore } = await import("../src/pipeline_v2_project_apply_writer_internal.ts");
    const { faultFdOps } = await import("./apply_fixtures.ts");
    const { realImmutableDocumentIo, realProjectFileStreamIo } = await import("./apply_fixtures.ts");
    const fops = faultFdOps();
    let mutations = 0;
    const crash = (real: unknown) => (...args: unknown[]) => {
      mutations += 1;
      if (mutations === nth) {
        throw Object.assign(new Error("injected crash"), { applyCrashFault: true });
      }
      return (real as (...a: unknown[]) => unknown)(...args);
    };
    const fdOps = {
      ...fops.ops,
      unlinkat: crash(fops.ops.unlinkat),
      mkdirat: crash(fops.ops.mkdirat),
      renameat: crash(fops.ops.renameat),
      renameat2: crash(fops.ops.renameat2),
      symlinkat: crash(fops.ops.symlinkat),
    } as typeof fops.ops;
    const result = await applyPreparedProjectCore(options, {
      docIo: realImmutableDocumentIo,
      stateIo: defaultPipelineStateIo,
      fileStreamIo: realProjectFileStreamIo,
      fdOps,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    return;
  }
  const result = await applyPreparedPipelineV2Project(options);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch((cause: unknown) => {
  if (isApplyCrashFault(cause)) {
    process.stdout.write("CRASH\n");
    process.exit(3);
  }
  process.stdout.write(`ERROR:${String(cause)}\n`);
  process.exit(1);
});
