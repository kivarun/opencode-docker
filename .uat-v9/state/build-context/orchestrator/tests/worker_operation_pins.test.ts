/**
 * Source-scan pins for the adapter-owned worker operations increment.
 *
 * These scans pin the structural invariants of the increment in the
 * production sources: the operation client owns the four endpoints
 * exactly once, the CLI-run path is gone from the production worker
 * path, no text classification or unbounded accumulation exists, the
 * durable state contract is untouched and downstream consumers never
 * import the operation client.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";

const src = (name: string): string =>
  readFileSync(join(import.meta.dir, "..", "src", name), "utf8");

const OPERATION_SOURCES = ["worker_operation.ts", "helper_http.ts", "pipeline_v2_docker_runtime.ts"];

test("the production worker path no longer spawns docker-helper run", () => {
  const runtime = src("pipeline_v2_docker_runtime.ts");
  // the adapter's only CLI call site is the image pull
  expect(runtime.match(/await cli\(/g)?.length).toBe(1);
  expect(runtime).toContain('["pull", "--endpoint"');
  // the former CLI-run argv shapes are gone; the only remaining
  // `"run"` literal is the command entry inside the POST /run body
  expect(runtime).not.toContain('"--workdir"');
  expect(runtime).not.toContain('"--env-from"');
  expect(runtime).not.toContain('"--helper-socket"');
  expect(runtime).not.toContain("signalOnAbort");
  for (const name of OPERATION_SOURCES) {
    expect(src(name)).not.toContain("killActive");
    expect(src(name)).not.toContain('"SIGKILL"');
  }
  // session create/delete and the auth preflight stay owned by the CLI layers
  expect(src("launcher.ts")).toContain('operatorArgs(["create"]');
  expect(src("launcher.ts")).toContain('operatorArgs(["delete"]');
  expect(src("docker_helper.ts")).toContain("fetchAuthOverSocket");
});

test("exactly one owner per operation endpoint", () => {
  const operation = src("worker_operation.ts");
  // one POST /run request site
  expect(operation.match(/path: "\/run"/g)?.length).toBe(1);
  // one drain request site
  expect(operation.match(/\/logs\?offset=/g)?.length).toBe(1);
  // one status poll site and one cancel site
  expect(operation.match(/path: `\/operations\/\$\{operationId\}`/g)?.length).toBe(1);
  expect(operation.match(/\/cancel`/g)?.length).toBe(1);
  // the drain/status/cancel helpers exist exactly once as owners
  expect(operation.match(/drainOnce/g)?.length).toBe(3); // definition + 2 loop calls
  expect(operation.match(/pollStatus/g)?.length).toBe(2); // definition + 1 call
  expect(operation.match(/sendCancel/g)?.length).toBe(3); // definition + abort + terminal-check calls
  // the transport module carries no endpoint ownership
  expect(src("helper_http.ts")).not.toContain('"/run"');
  expect(src("helper_http.ts")).not.toContain('"/logs"');
  expect(src("helper_http.ts")).not.toContain('"/cancel"');
});

test("the operation client never classifies diagnostics by text", () => {
  for (const name of OPERATION_SOURCES) {
    const text = src(name);
    expect(text).not.toContain(".includes(");
    expect(text).not.toContain(".indexOf(");
    expect(text).not.toContain(".match(");
    // `.test(` appears only on the compiled safe-id/digit patterns
    const testCalls = text.match(/\.test\(/g)?.length ?? 0;
    const patternTests = (text.match(/[A-Z_]+_PATTERN\.test\(/g)?.length ?? 0);
    expect(testCalls).toBe(patternTests);
  }
});

test("the capture buffer is the single bounded accumulation site", () => {
  const operation = src("worker_operation.ts");
  expect(operation.match(/chunks\.push\(/g)?.length).toBe(2); // the oversized single chunk + the regular chunk
  expect(operation).toContain("WORKER_LOG_CAPTURE_BOUND_BYTES");
  expect(operation).toContain("const appendChunk");
  // no other array or string accumulation exists
  expect(operation).not.toContain(".push(text");
  expect(operation).not.toContain("logs.push");
});

test("POST /run is never retried", () => {
  const operation = src("worker_operation.ts");
  expect(operation.match(/await transport\(/g)?.length).toBe(4); // create, drain, status, cancel
  // the create is awaited exactly once and its failure path never loops
  expect(operation).toContain("The create request is deliberately NOT wired");
  expect(operation).not.toContain("for (let attempt");
  expect(operation).not.toContain("retry");
});

test("no process-local operation registry exists outside the operation lifetime", () => {
  const operation = src("worker_operation.ts");
  expect(operation).not.toContain("new Map(");
  expect(operation).not.toContain("globalThis");
  expect(operation).not.toContain("registry");
});

test("no run-root files are written and no state schema changes exist", () => {
  const operation = src("worker_operation.ts");
  expect(operation).not.toContain("writeFile");
  expect(operation).not.toContain("mkdir");
  expect(operation).not.toContain("rename");
  // the durable state module keeps its schema version and command union untouched
  expect(src("pipeline_v2_state.ts")).toContain(
    "export const PIPELINE_V2_RUN_STATE_SCHEMA_VERSION = 9",
  );
});

test("control-plane, UI and events never import the operation client", () => {
  const forbidden = ["worker_operation.ts", "worker_signals.ts", "helper_http.ts"];
  const consumers = [
    "pipeline_v2_events.ts",
    "control_plane/routing.ts",
    "control_plane/subprocess_client.ts",
    "control_plane/http_server.ts",
    "control_plane/project_registry.ts",
    "control_plane/run_binding_store.ts",
  ];
  for (const consumer of consumers) {
    const text = src(consumer);
    for (const name of forbidden) {
      expect(text).not.toContain(name);
    }
  }
});

test("the adapter is the lifecycle owner of the operation runner seam", () => {
  const runtime = src("pipeline_v2_docker_runtime.ts");
  expect(runtime).toContain("operationRunner");
  expect(runtime).toContain("runWorkerOperation");
  expect(runtime).toContain("workerSignals");
  expect(runtime).toContain("executionRecord.capture");
});
