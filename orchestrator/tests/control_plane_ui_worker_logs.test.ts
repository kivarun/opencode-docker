/**
 * Permanent HTTP integration of the browser UI v5 worker-log viewer:
 * the real `app.js` viewer consumes the real logs endpoints (the real
 * CLI subprocess over the loopback service) against runs with durable
 * schema-v9 worker-log artifacts — captured, truncated, pending and
 * unavailable — through the bounded chunk flow, with the capture gate,
 * zero-write fingerprints, authority failures and the leakage scan.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createLogsTransport,
  createWorkerLogViewer,
  decodeLogChunkText,
  logsCaptureGate,
  parseWorkerLogDocument,
  parseWorkerLogsDocument,
  workerLogsRequestPath,
} from "../ui/app.js";
import { createUiHarness, type UiHarness } from "./ui_dom_harness.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { publishPipelineV2WorkerLog } from "../src/pipeline_v2_worker_log_store.ts";
import { workerLogPayloadDigest } from "../src/pipeline_v2_worker_log.ts";

const CANARY = "uiworkerlogs";

interface RawResponse {
  readonly status: number;
  readonly body: string;
}

function rawRequest(port: number, method: string, path: string, body?: string): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      { host: "127.0.0.1", port, method, path, headers: { host: `127.0.0.1:${port}`, ...(body !== undefined ? { "content-type": "application/json" } : {}) } },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
    if (body !== undefined) {
      req.write(body);
    }
    req.end();
  });
}

function fingerprintTree(root: string): string {
  const rows: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    const entries = readdirSync(dir, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const child = join(dir, entry.name);
      const rel = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = lstatSync(child);
      rows.push(`${rel}|${info.isDirectory() ? "d" : "f"}|${info.size}`);
      if (info.isDirectory()) {
        walk(child, rel);
      }
    }
  };
  walk(root, "");
  return rows.join("\n");
}

function hex64(seed: string): string {
  let text = "";
  while (text.length < 64) {
    text += seed;
  }
  return text.slice(0, 64);
}

const COVERAGE_YAML = `
schema_version: 2
entry_state: architect
max_transitions: 8
inputs: []
outputs: []
states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: gate
  - id: gate
    type: decision
    model: models/gate.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: architect
            output: plan
    transitions:
      - outcome: go
        to: worker
      - outcome: uncovered
        to: done
      - outcome: inconsistent_facts
        to: done
      - outcome: invalid_facts
        to: done
  - id: worker
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const GATE_MODEL_YAML = `
schema_version: 1
facts:
  - id: ready
decisions:
  - id: go
relations: []
constraints: []
rules:
  - id: r1
    when:
      fact: ready
      equals: true
    decision: go
`;

const suffix = Math.random().toString(36).slice(2, 8);
const mainRun = `wl-main-${suffix}`;
const failedRun = `wl-failed-${suffix}`;
const ghostRun = `wl-ghost-${suffix}`;

// The main run: execution 1 captured (artifact published), execution 2
// started and left in flight (its worker_log stays pending). The run
// boundary is `in_flight`, so the capture gate admits chunk loads.
const MAIN_CAPTURE_PAYLOAD = new TextEncoder().encode("captured-output\0with\x1b[31mcontrol\x1b[0m-bytes");
const MAIN_COMMANDS = [
  { kind: "create_run", inputs: [], outputs: [] },
  { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: `sess-${suffix}-1` },
  { kind: "agent_tool_session_created", sessionId: `tool-${suffix}-1` },
  { kind: "agent_running" },
  {
    kind: "agent_outputs_accepted",
    outputs: [{ id: "plan", digest: hex64("cd") }],
    workerLog: { status: "captured", sha256: workerLogPayloadDigest(MAIN_CAPTURE_PAYLOAD), byte_count: MAIN_CAPTURE_PAYLOAD.length, truncated_daemon: false, truncated_local: false },
  },
  { kind: "agent_cleanup_completed" },
  { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
  { kind: "start_decision_execution", stateId: "gate", inputDigest: hex64("ab"), executionRole: "control" },
  { kind: "decision_evaluated", result: { status: "selected", outcome: "go", decision: "go", rule_id: "r1", active_constraint_ids: [] } },
  { kind: "transition_committed", step: { from: "gate", outcome: "go", to: "worker", transition_index: 0 }, executionIndex: 2 },
  { kind: "start_agent_execution", stateId: "worker", profile: "coder", executionRole: "planning" },
  { kind: "agent_data_prepared" },
] as const;

// The failed run: execution 1 truncated (artifact published, local tail
// truncation), run failed. A settled run's captured/truncated payloads
// are never rendered through the capture gate.
const FAILED_PAYLOAD = new TextEncoder().encode("truncated-tail");
const FAILED_COMMANDS = [
  { kind: "create_run", inputs: [], outputs: [] },
  { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: `sess-${suffix}-2` },
  { kind: "agent_tool_session_created", sessionId: `tool-${suffix}-2` },
  { kind: "agent_running" },
  {
    kind: "agent_outputs_accepted",
    outputs: [{ id: "plan", digest: hex64("ef") }],
    workerLog: { status: "truncated", sha256: workerLogPayloadDigest(FAILED_PAYLOAD), byte_count: FAILED_PAYLOAD.length, truncated_daemon: false, truncated_local: true },
  },
  { kind: "agent_cleanup_completed" },
  { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
  { kind: "start_decision_execution", stateId: "gate", inputDigest: hex64("ab"), executionRole: "control" },
  { kind: "decision_evaluated", result: { status: "selected", outcome: "go", decision: "go", rule_id: "r1", active_constraint_ids: [] } },
  { kind: "transition_committed", step: { from: "gate", outcome: "go", to: "worker", transition_index: 0 }, executionIndex: 2 },
  { kind: "start_agent_execution", stateId: "worker", profile: "coder", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: `sess-${suffix}-3` },
  { kind: "agent_tool_session_created", sessionId: `tool-${suffix}-3` },
  { kind: "agent_running" },
  { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" }, workerLog: { status: "unavailable" } },
  { kind: "run_failed", reason: "worker_failed" },
] as const;


async function waitFor(check: () => boolean, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) {
      return;
    }
    await Bun.sleep(50);
  }
  throw new Error("waitFor condition not reached");
}

describe("control-plane ui worker logs: the real logs endpoints through the UI viewer", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-wlogs-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService | null = null;
  let port = 0;
  let daemonPid = 0;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-WORKER-LOGS-TASK\n", { mode: 0o600 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(fixtureRoot, "config"),
            pipeline_root: join(fixtureRoot, "bundle"),
            launcher_id: null,
            input_roots: [{ id: "tasks", root: inputRoot }],
          },
        ],
      }),
      { mode: 0o600 },
    );
    const log = join(smokeRoot, "daemon.log");
    const daemonLogFd = openSync(log, "a");
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_daemon.ts"), socketPath, "9000"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("ui-worker-logs daemon socket never appeared");
    service = await startControlPlaneHttpService({
      registryPath,
      bindingStoreRoot: storeRoot,
      client: {
        bunExecutable: process.execPath,
        orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
        workingDirectory: smokeRoot,
        baseEnv: {
          PATH: "/usr/bin:/bin",
          HOME: smokeRoot,
          XDG_CONFIG_HOME: credDir,
          ORCHESTRATOR_STATE_ROOT: stateRoot,
          DOCKER_HELPER_SOCKET_PATH: socketPath,
        },
        stdoutLimitBytes: 4_000_000,
        stderrLimitBytes: 4_000_000,
        sigintGraceMs: 3000,
      },
      port: 0,
      bodyLimitBytes: 4096,
    });
    port = service.port;

    // The synthetic runs: real reducer lifecycles in the service's state
    // root over a real loadable bundle, real worker-log artifacts for the
    // captured/truncated bindings, and the durable project binding.
    const coverageBundle = join(smokeRoot, "coverage-bundle");
    await mkdir(join(coverageBundle, "models"), { recursive: true });
    await writeFile(join(coverageBundle, "pipeline.yaml"), COVERAGE_YAML);
    await mkdir(join(coverageBundle, "prompts"), { recursive: true });
    await writeFile(join(coverageBundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
    await writeFile(join(coverageBundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
    await mkdir(join(coverageBundle, "schemas"), { recursive: true });
    await writeFile(join(coverageBundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(coverageBundle, "models", "gate.yaml"), GATE_MODEL_YAML);
    const pipeline = await loadPipelineV2(coverageBundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    for (const [runId, commands] of [
      [mainRun, MAIN_COMMANDS],
      [failedRun, FAILED_COMMANDS],
    ] as const) {
      const sink = new PipelineV2RunStateSink({ stateRoot, runId });
      for (const command of commands) {
        if (command.kind === "create_run") {
          await sink.dispatch({ ...command, runId, pipeline: identity } as never);
          continue;
        }
        await sink.dispatch(command as never);
      }
      const claim = await store.claim({ runId, projectId: "project-a" });
      expect(["created", "existing"]).toContain(claim.kind);
    }
    const publishMain = await publishPipelineV2WorkerLog(join(stateRoot, "pipeline-runs", mainRun), 1, {
      status: "captured",
      byteCount: MAIN_CAPTURE_PAYLOAD.length,
      tailBytes: MAIN_CAPTURE_PAYLOAD,
      truncatedDaemon: false,
      truncatedLocal: false,
    });
    expect(publishMain).not.toBeNull();
    const publishFailed = await publishPipelineV2WorkerLog(join(stateRoot, "pipeline-runs", failedRun), 1, {
      status: "truncated",
      byteCount: FAILED_PAYLOAD.length,
      tailBytes: FAILED_PAYLOAD,
      truncatedDaemon: false,
      truncatedLocal: true,
    });
    expect(publishFailed).not.toBeNull();
    const ghost = await store.claim({ runId: ghostRun, projectId: "project-a" });
    expect(["created", "existing"]).toContain(ghost.kind);
  }, 60000);

  afterAll(async () => {
    if (service !== null) await service.stop();
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test("l1. the metadata list parses through the real endpoint with every status", async () => {
    const before = fingerprintTree(stateRoot);
    const response = await rawRequest(port, "GET", `/v1/runs/${mainRun}/logs`);
    expect(response.status).toBe(200);
    const document = JSON.parse(response.body) as Record<string, unknown>;
    const model = parseWorkerLogsDocument(document, mainRun);
    expect(model.logs).toHaveLength(2);
    expect(model.logs[0]).toMatchObject({ executionIndex: 1, stateId: "architect", status: "captured", byteCount: MAIN_CAPTURE_PAYLOAD.length, truncatedDaemon: false, truncatedLocal: false });
    // the decision execution carries no worker log; the worker agent keeps
    // its global execution index 3
    expect(model.logs[1]).toEqual({ executionIndex: 3, stateId: "worker", status: "pending" });
    expect(fingerprintTree(stateRoot)).toBe(before);
  });

  test("l2. one bounded chunk decodes to the exact secret-bearing bytes", async () => {
    const before = fingerprintTree(stateRoot);
    const response = await rawRequest(port, "GET", workerLogsRequestPath(mainRun, 1, 0, 65536));
    expect(response.status).toBe(200);
    const model = parseWorkerLogDocument(JSON.parse(response.body), mainRun);
    expect(model.status).toBe("captured");
    expect(model.offset).toBe(0);
    expect(model.nextOffset).toBeNull();
    expect(decodeLogChunkText(model.dataBase64!)).toBe(new TextDecoder().decode(MAIN_CAPTURE_PAYLOAD));
    expect(fingerprintTree(stateRoot)).toBe(before);
  });

  test("l3. the UI harness drives the full manual flow over real HTTP", async () => {
    // the CLI subprocess roundtrips are real: allow a generous test budget

    const before = fingerprintTree(stateRoot);
    const realFetch = globalThis.fetch;
    const harness = await createUiHarness({
      fetchImpl: ((path: string, init?: RequestInit) => realFetch(`http://127.0.0.1:${port}${path}`, init)) as never,
    });
    harness.click("refresh-runs");
    await waitFor(() => harness.runRows().includes(mainRun));
    harness.clickRunInspect(harness.runRows().indexOf(mainRun));
    await waitFor(() => harness.detailStatus() === "running");
    expect(harness.element("worker-logs-section").hidden).toBe(false);
    // nothing loads automatically
    harness.click("logs-load");
    await waitFor(() => harness.logsRows().length === 2);
    expect(harness.logsRows()[0]).toBe(`1|architect|captured|${MAIN_CAPTURE_PAYLOAD.length}||`);
    expect(harness.logsRows()[1]).toBe("3|worker|pending|||");
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true); // no selection yet
    harness.clickButton("logs-body", "View");
    await waitFor(() => !harness.logsButtonState("logs-next-chunk").hidden); // in_flight admits the chunk
    harness.click("logs-next-chunk");
    await waitFor(() => harness.logsContent() !== "");
    expect(harness.logsContent()).toBe(new TextDecoder().decode(MAIN_CAPTURE_PAYLOAD));
    expect(harness.logsCursor()).toContain(`bytes 0–${MAIN_CAPTURE_PAYLOAD.length} (end)`);
    // the pending execution is selectable but never chunkable
    const secondView = harness.element("logs-body").children[1]!;
    const secondButton = secondView.querySelectorAll("button").find((node) => node.textContent === "View")!;
    secondButton.click();
    await waitFor(() => harness.logsButtonState("logs-next-chunk").hidden);
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true);
    expect(fingerprintTree(stateRoot)).toBe(before);
    harness.dispose();
  }, 20000);

  test("l4. a settled run renders metadata but the capture gate refuses chunks", async () => {
    // the CLI subprocess roundtrips are real: allow a generous test budget

    const realFetch = globalThis.fetch;
    const harness = await createUiHarness({
      fetchImpl: ((path: string, init?: RequestInit) => realFetch(`http://127.0.0.1:${port}${path}`, init)) as never,
    });
    harness.click("refresh-runs");
    await waitFor(() => harness.runRows().includes(failedRun));
    harness.clickRunInspect(harness.runRows().indexOf(failedRun));
    await waitFor(() => {
      if (harness.element("status").textContent.includes("could not be inspected")) {
        throw new Error(`inspect failed: ${harness.element("error").textContent}`);
      }
      return harness.detailStatus() !== "";
    });
    console.log("DEBUG l4 detailStatus:", JSON.stringify(harness.detailStatus()));
    harness.click("logs-load");
    await waitFor(() => harness.logsRows().length === 2);
    // the agent executions only: truncated (1) and unavailable (3); the
    // decision execution carries no worker log
    expect(harness.logsRows()).toHaveLength(2);
    expect(harness.logsRows()[0]).toContain("truncated");
    expect(harness.logsRows()[1]).toContain("unavailable");
    harness.clickButton("logs-body", "View"); // selects execution 1 (truncated)
    await Bun.sleep(50);
    harness.click("logs-next-chunk");
    await waitFor(() => harness.logsStatus().includes("capture_gate_refused"));
    expect(harness.logsStatus()).toContain("viewer: capture_gate_refused");
    expect(harness.logsContent()).toBe("");
    harness.dispose();
  }, 20000);

  test("l5. an unbound run fails closed through the CLI outcome surface", async () => {
    const before = fingerprintTree(stateRoot);
    // the binding exists but the run has no durable state: the correctly
    // received failed logs outcome is a 200 command result
    const response = await rawRequest(port, "GET", `/v1/runs/${ghostRun}/logs`);
    expect(response.status).toBe(200);
    const document = JSON.parse(response.body) as Record<string, unknown>;
    expect(document.kind).toBe("command_result");
    expect(document.outcome).toBe("failed");
    expect(document.reason).toBe("run_not_found");
    expect(fingerprintTree(stateRoot)).toBe(before);
  });

  test("l6. the chunk query grammar rejects malformed offsets through the request layer", async () => {
    const response = await rawRequest(port, "GET", `/v1/runs/${mainRun}/logs/1?offset=-1`);
    expect(response.status).toBe(400);
    const document = JSON.parse(response.body) as Record<string, unknown>;
    expect(document.kind).toBe("error");
  });

  test("l7. no host path, credential or canary ever enters the logs responses", async () => {
    const list = await rawRequest(port, "GET", `/v1/runs/${mainRun}/logs`);
    const chunk = await rawRequest(port, "GET", workerLogsRequestPath(mainRun, 1, 0, 65536));
    const failed = await rawRequest(port, "GET", `/v1/runs/${failedRun}/logs`);
    expectNoLeak(list.body + chunk.body + failed.body, secrets);
  });

  test("l8. the capture gate is a pure function of the boundary and the mutation state", () => {
    expect(logsCaptureGate(null, "in_flight", () => false)).toBe(false);
    const state = {
      runId: mainRun,
      snapshotRevision: 24,
      logs: [{ executionIndex: 1, stateId: "architect", status: "captured" as const }],
      selectedExecutionIndex: 1,
      content: null,
      requestState: "ready" as const,
      error: null,
    };
    expect(logsCaptureGate(state, "in_flight", () => false)).toBe(true);
    expect(logsCaptureGate(state, "waiting", () => false)).toBe(true);
    expect(logsCaptureGate(state, "succeeded", () => false)).toBe(false);
  });
});

function expectNoLeak(text: string, forbidden: readonly string[]): void {
  for (const secret of forbidden) {
    expect(text.includes(secret)).toBe(false);
  }
}
