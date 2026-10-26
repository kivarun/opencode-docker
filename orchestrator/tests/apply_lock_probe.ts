import { acquirePipelineV2ApplyProjectLock } from "../src/pipeline_v2_apply_project_lock_internal.ts";

const [, , mode, root] = process.argv as [string, string, string, string];
if (mode === "hold") {
  const acquired = acquirePipelineV2ApplyProjectLock(root);
  if (acquired.busy) {
    process.stdout.write("busy\n");
  } else {
    process.stdout.write("held\n");
    process.stdin.resume();
    process.stdin.once("end", () => {
      acquired.lock.release();
    });
  }
} else if (mode === "probe") {
  const acquired = acquirePipelineV2ApplyProjectLock(root);
  if (acquired.busy) {
    process.stdout.write("busy\n");
  } else {
    acquired.lock.release();
    process.stdout.write("free\n");
  }
}
