/**
 * Child entrypoint of the bounded loader-termination regression in
 * `pipeline_v2_state.test.ts`. The test suite never runs the potentially
 * hanging run-state validator directly for the answered-wait document;
 * this child process does, with a finite wall-clock timeout enforced by
 * the parent. The child validates one document given by path, reports the
 * outcome on stdout, and always exits: an accepted document prints
 * `ACCEPTED`, a typed rejection prints `REJECTED:<message>`, and an
 * unexpected child failure exits non-zero with the error on stderr.
 */
import { validatePipelineV2RunState } from "../src/pipeline_v2_state.ts";

const path = process.argv[2];
if (path === undefined) {
  console.log("CHILD-ERROR:no document path was passed");
  process.exit(1);
}
try {
  const raw = await Bun.file(path).text();
  validatePipelineV2RunState(JSON.parse(raw));
  console.log("ACCEPTED");
} catch (cause) {
  console.log(`REJECTED:${(cause as Error).message}`);
}
