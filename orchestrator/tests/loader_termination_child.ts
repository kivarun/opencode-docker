/**
 * Child entrypoint of the bounded loader-termination regression in
 * `pipeline_v2_state.test.ts`. The test suite never runs the potentially
 * hanging run-state validator directly for the answered-wait document;
 * this child process does, with a finite wall-clock timeout enforced by
 * the parent. The child validates one document given by path, reports the
 * outcome on stdout, and always exits: an accepted document prints
 * `ACCEPTED`, a typed `PipelineV2StateError` rejection — the only expected
 * failure — prints `REJECTED:<message>` and exits 0, and any other error
 * is never marked as an expected rejection: it is printed to stderr and
 * the child exits non-zero, so a programmer error or an unexpected runtime
 * failure can never look like a successful typed-rejection proof. The
 * reserved `--hang` argument is a test-only mode used by the parent's
 * bounded-hang regression: the child blocks forever so the parent's
 * spawnSync timeout must kill only this child and fail the test normally.
 */
import { PipelineV2StateError, validatePipelineV2RunState } from "../src/pipeline_v2_state.ts";

const path = process.argv[2];
if (path === undefined) {
  console.error("CHILD-ERROR:no document path was passed");
  process.exit(1);
}
if (path === "--hang") {
  await new Promise<never>(() => {});
}
try {
  const raw = await Bun.file(path).text();
  validatePipelineV2RunState(JSON.parse(raw));
  console.log("ACCEPTED");
} catch (cause) {
  if (cause instanceof PipelineV2StateError) {
    console.log(`REJECTED:${cause.message}`);
  } else {
    console.error(cause instanceof Error ? cause.stack ?? cause.message : String(cause));
    process.exit(1);
  }
}
