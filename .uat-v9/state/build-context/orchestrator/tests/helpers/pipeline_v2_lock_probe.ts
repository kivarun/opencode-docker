/**
 * Test-only subprocess helper for the kernel lock-release proof: holds the
 * production per-run mutation lock in a separate OS process (`hold`,
 * releasing only when a stdin line arrives or the process is killed) or
 * probes whether the lock is free (`probe`).
 *
 * This file is test infrastructure only: production code never imports it.
 */
import { acquirePipelineV2RunMutationLock } from "../../src/pipeline_v2_run_mutation_lock.ts";

const mode = process.argv[2] ?? "";
const runRoot = process.argv[3] ?? "";
const runId = process.argv[4] ?? "";

if (mode === "hold") {
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId, mode: "fresh" });
  if (acquisition.busy) {
    process.stdout.write("BUSY\n");
    process.exit(1);
  }
  process.stdout.write("HELD\n");
  const deadline = setTimeout(() => process.exit(0), 30000);
  const stream = Bun.stdin.stream() as unknown as AsyncIterable<Uint8Array>;
  for await (const _chunk of stream) {
    clearTimeout(deadline);
    process.stdout.write("RELEASED\n");
    acquisition.lock.release();
    process.exit(0);
  }
} else if (mode === "probe") {
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId, mode: "fresh" });
  if (acquisition.busy) {
    process.stdout.write("BUSY\n");
    process.exit(0);
  }
  acquisition.lock.release();
  process.stdout.write("FREE\n");
  process.exit(0);
} else {
  process.stderr.write("usage: lock_probe hold|probe <runRoot> <runId>\n");
  process.exit(2);
}
