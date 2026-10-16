/**
 * Permanent HTTP integration of the browser UI v4 lifecycle timeline:
 * the real `app.js` parser and merge consume the real events endpoint
 * (real CLI subprocess over the loopback service), the full 66-event
 * lifecycle paginates through the UI model byte-for-byte, the 28-kind
 * public vocabulary is pinned against the real endpoint, concurrent
 * appends continue without duplicates or gaps, authority failures fail
 * closed, and every events GET is zero-write over the run tree.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PIPELINE_V2_EVENT_KINDS, applyEventsPage, createTimelineState, parseEventsDocument } from "../ui/app.js";
import { createUiHarness, type UiHarness } from "./ui_dom_harness.ts";
import { lifecycleCommands } from "./pipeline_v2_commit_history.test.ts";
import { E2E_YAML } from "./pipeline_v2_events.test.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";

const CANARY = "uieventshostpath";

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

function expectNoLeak(text: string, secrets: readonly string[]): void {
  for (const secret of secrets) {
    expect(text.includes(secret)).toBe(false);
  }
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

function hex64(seed: string): string {
  let text = "";
  while (text.length < 64) {
    text += seed;
  }
  return text.slice(0, 64);
}

const COVERAGE_B_COMMANDS = [
  { kind: "create_run", inputs: [], outputs: [] },
  { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-b-1" },
  { kind: "agent_tool_session_created", sessionId: "tool-b-1" },
  { kind: "agent_running" },
  { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex64("b") }] },
  { kind: "agent_cleanup_completed" },
  { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
  { kind: "start_decision_execution", stateId: "gate", inputDigest: hex64("d"), executionRole: "control" },
  { kind: "decision_evaluated", result: { status: "selected", outcome: "go", decision: "go", rule_id: "r1", active_constraint_ids: [] } },
  { kind: "transition_committed", step: { from: "gate", outcome: "go", to: "worker", transition_index: 0 }, executionIndex: 2 },
  { kind: "start_agent_execution", stateId: "worker", profile: "coder", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-b-2" },
  { kind: "agent_tool_session_created", sessionId: "tool-b-2" },
  { kind: "agent_running" },
  { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
  { kind: "run_failed", reason: "worker_failed" },
] as const;

const COVERAGE_C_COMMANDS = [
  { kind: "create_run", inputs: [], outputs: [] },
  { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-c-1" },
  { kind: "agent_tool_session_created", sessionId: "tool-c-1" },
  { kind: "agent_running" },
  { kind: "agent_failed", reason: "session_cleanup_failed", sessionCleanup: { execution: "completed", tool: "failed" } },
  { kind: "run_cleanup_failed" },
] as const;

const COVERAGE_D_COMMANDS = [
  { kind: "create_run", inputs: [], outputs: [] },
  { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-d-1" },
  { kind: "agent_tool_session_created", sessionId: "tool-d-1" },
  { kind: "agent_running" },
  { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex64("e") }] },
  { kind: "agent_cleanup_completed" },
  { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
  { kind: "start_decision_execution", stateId: "gate", inputDigest: hex64("f"), executionRole: "control" },
  { kind: "decision_failed", reason: "decision_input_invalid" },
  { kind: "run_failed", reason: "decision_input_invalid" },
] as const;

interface EventsPage {
  readonly schema_version: 1;
  readonly kind: "events";
  readonly run_id: string;
  readonly revision: number;
  readonly events: readonly Record<string, unknown>[];
  readonly next_after: number | null;
}

describe("control-plane ui events: the real events endpoint through the UI model", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-events-${CANARY}-`));
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
  const synthFull = `ev-full-${Math.random().toString(36).slice(2, 8)}`;
  const synthOpen = `ev-open-${Math.random().toString(36).slice(2, 8)}`;
  const ghostRunId = `ev-ghost-${Math.random().toString(36).slice(2, 8)}`;
  const runSuffix = Math.random().toString(36).slice(2, 8);
  const covB = `ev-cov-b-${runSuffix}`;
  const covC = `ev-cov-c-${runSuffix}`;
  const covD = `ev-cov-d-${runSuffix}`;
  const natural = `ui-ev-${Math.random().toString(36).slice(2, 8)}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-EVENTS-TASK\n", { mode: 0o600 });
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
    if (!existsSync(socketPath)) throw new Error("ui-events daemon socket never appeared");
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
          ARCHITECT_SOURCE_VAR_1: "flow-a",
          CODER_SOURCE_VAR_1: "flow-c",
        },
        stdoutLimitBytes: 4_000_000,
        stderrLimitBytes: 4_000_000,
        sigintGraceMs: 3000,
      },
      port: 0,
      bodyLimitBytes: 4096,
    });
    port = service.port;

    // The synthetic runs: the 66-command reducer lifecycle written into
    // the service's state root over a real loadable bundle directory,
    // then bound to the registry project.
    const bundle = join(smokeRoot, "synth-bundle");
    await mkdir(bundle, { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), E2E_YAML);
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await writeFile(join(bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    const pipeline = await loadPipelineV2(bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const all = lifecycleCommands();
    const openCommands = all.slice(0, all.findIndex((command) => command.kind === "run_outputs_published") + 1);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    const coverageBundle = join(smokeRoot, "coverage-bundle");
    await mkdir(join(coverageBundle, "models"), { recursive: true });
    await writeFile(join(coverageBundle, "pipeline.yaml"), COVERAGE_YAML);
    await mkdir(join(coverageBundle, "prompts"), { recursive: true });
    await writeFile(join(coverageBundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
    await writeFile(join(coverageBundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
    await mkdir(join(coverageBundle, "schemas"), { recursive: true });
    await writeFile(join(coverageBundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(coverageBundle, "models", "gate.yaml"), GATE_MODEL_YAML);
    const coveragePipeline = await loadPipelineV2(coverageBundle);
    const coverageIdentity = pipelineV2RunPipelineIdentity(coveragePipeline);
    const coverageRuns = [
      [`ev-cov-b-${runSuffix}`, COVERAGE_B_COMMANDS, coverageIdentity],
      [`ev-cov-c-${runSuffix}`, COVERAGE_C_COMMANDS, identity],
      [`ev-cov-d-${runSuffix}`, COVERAGE_D_COMMANDS, coverageIdentity],
    ] as const;
    for (const [runId, commands, runIdentity] of coverageRuns) {
      const sink = new PipelineV2RunStateSink({ stateRoot, runId });
      for (const command of commands) {
        if (command.kind === "create_run") {
          await sink.dispatch({ ...command, runId, pipeline: runIdentity } as never);
          continue;
        }
        await sink.dispatch(command as never);
      }
      const claim = await store.claim({ runId, projectId: "project-a" });
      expect(["created", "existing"]).toContain(claim.kind);
    }
    for (const [runId, commands] of [
      [synthFull, all],
      [synthOpen, openCommands],
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
    // a bound run id with no durable state at all
    const ghost = await store.claim({ runId: ghostRunId, projectId: "project-a" });
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

  async function readEventsPage(runId: string, after: number, limit: number): Promise<{ status: number; page: Record<string, unknown> }> {
    const response = await rawRequest(port, "GET", `/v1/runs/${runId}/events?after=${after}&limit=${limit}`);
    return { status: response.status, page: JSON.parse(response.body) as Record<string, unknown> };
  }

  test("e1. the 66-event lifecycle paginates through the real endpoint into the UI model byte-for-byte", async () => {
    let timeline = createTimelineState(synthFull);
    const pages: Record<string, unknown>[] = [];
    let cursor = 0;
    for (;;) {
      const { status, page } = await readEventsPage(synthFull, cursor, 20);
      expect(status).toBe(200);
      expect(page.kind).toBe("events");
      const model = parseEventsDocument(page, synthFull);
      timeline = applyEventsPage(timeline, model, cursor);
      pages.push(page);
      if (model.nextAfter === null) break;
      cursor = model.nextAfter;
    }
    expect(pages.length).toBe(4);
    for (const page of pages) {
      expect(page.revision).toBe(66);
    }
    expect(timeline.cursor).toBe(66);
    expect(timeline.events.length).toBe(66);
    // the merged UI history equals the durable commit history byte-for-byte
    const durable = parsePipelineV2RunState(
      readFileSync(join(stateRoot, "pipeline-runs", synthFull, "state.json"), "utf8"),
    ) as PipelineV2RunState;
    expect(JSON.stringify(timeline.events)).toBe(JSON.stringify(durable.commit_history));
    // the 28-kind public vocabulary is pinned against the real endpoint:
    // the 66-command lifecycle plus the three failure/decision coverage
    // runs together carry every public kind
    const kinds = new Set(timeline.events.map((entry) => entry.kind));
    for (const coverageRun of [covB, covC, covD]) {
      let coverageTimeline = createTimelineState(coverageRun);
      let coverageCursor = 0;
      for (;;) {
        const { page } = await readEventsPage(coverageRun, coverageCursor, 100);
        const model = parseEventsDocument(page, coverageRun);
        coverageTimeline = applyEventsPage(coverageTimeline, model, coverageCursor);
        if (model.nextAfter === null) break;
        coverageCursor = model.nextAfter;
      }
      for (const entry of coverageTimeline.events) {
        kinds.add(entry.kind);
      }
    }
    expect([...kinds].sort()).toEqual([...PIPELINE_V2_EVENT_KINDS].sort());
    // a repeated read over the unchanged run returns the same document
    const again = await readEventsPage(synthFull, 40, 20);
    expect(JSON.stringify(again.page)).toBe(JSON.stringify(pages[2]));
    expectNoLeak(JSON.stringify(pages), secrets);
  }, 60000);

  test("e2. the caught-up empty page and the invalid cursor follow the cursor contract", async () => {
    const caught = await readEventsPage(synthFull, 66, 100);
    expect(caught.status).toBe(200);
    const model = parseEventsDocument(caught.page, synthFull);
    expect(model.events).toEqual([]);
    expect(model.nextAfter).toBeNull();
    // the UI merge accepts the empty page on a caught-up timeline
    let timeline = createTimelineState(synthFull);
    const full = await readEventsPage(synthFull, 0, 100);
    timeline = applyEventsPage(timeline, parseEventsDocument(full.page, synthFull), 0);
    expect(timeline.cursor).toBe(66);
    timeline = applyEventsPage(timeline, model, 66);
    expect(timeline.events.length).toBe(66);
    // a cursor beyond the snapshot is the typed request-layer failure
    const beyond = await readEventsPage(synthFull, 67, 100);
    expect(beyond.status).toBe(400);
    expect(beyond.page).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
  }, 60000);

  test("e3. an unbound run and a bound-but-missing run fail closed", async () => {
    const unbound = await rawRequest(port, "GET", "/v1/runs/never-bound-x/events?after=0&limit=100");
    expect(unbound.status).toBe(404);
    expect(JSON.parse(unbound.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    const missing = await readEventsPage(ghostRunId, 0, 100);
    expect(missing.status).toBe(200);
    // the events endpoint's failed outcome arrives as the documented
    // command_result envelope; the events endpoint classifier owns it
    expect(missing.page).toMatchObject({
      schema_version: 1,
      kind: "command_result",
      run_id: ghostRunId,
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });
  }, 60000);

  test("e5. a concurrent append between pages continues without duplicates or gaps", async () => {
    let timeline = createTimelineState(synthOpen);
    let cursor = 0;
    let pages = 0;
    for (;;) {
      const { page } = await readEventsPage(synthOpen, cursor, 20);
      const model = parseEventsDocument(page, synthOpen);
      timeline = applyEventsPage(timeline, model, cursor);
      pages += 1;
      if (pages === 3) {
        // the durable run moves while the reader is between pages
        const sink = await PipelineV2RunStateSink.open({ stateRoot, runId: synthOpen });
        await sink.dispatch({ kind: "run_succeeded" } as never);
        expect(sink.snapshot?.revision).toBe(66);
      }
      if (model.nextAfter === null) break;
      cursor = model.nextAfter;
    }
    expect(pages).toBe(4);
    expect(timeline.events.length).toBe(66);
    expect(timeline.events.map((entry) => entry.revision)).toEqual(Array.from({ length: 66 }, (_, index) => index + 1));
    const kinds = new Set(timeline.events.map((entry) => entry.kind));
    expect(kinds.has("run_succeeded")).toBe(true);
  }, 60000);

  test("e6. every events GET is zero-write over the run tree", async () => {
    const runTree = join(stateRoot, "pipeline-runs", synthFull);
    const before = fingerprintTree(runTree);
    for (const after of [0, 20, 40, 60, 66]) {
      const { status } = await readEventsPage(synthFull, after, 20);
      expect(status).toBe(200);
    }
    expect(fingerprintTree(runTree)).toBe(before);
  }, 60000);


  test(
    "e7. the wiring lifecycle over the real service: selection, lost mutation recovery, in-flight tails, isolation",
    async () => {
      // the natural run through the service reaches the plan-ready boundary
      const created = await rawRequest(port, "POST", "/v1/projects/project-a/runs", JSON.stringify({
        run_id: natural,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      }));
      expect(created.status).toBe(200);
      expect(JSON.parse(created.body)).toMatchObject({ kind: "command_result", outcome: "plan_ready" });

      const eventsGets: string[] = [];
      const posts: string[] = [];
      const realFetch = fetch.bind(globalThis);
      const fetchImpl = (path: string, init?: RequestInit): Promise<Response> => {
        const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
        if (method === "GET" && path.includes("/events")) {
          eventsGets.push(path);
        }
        if (method === "POST") {
          posts.push(path);
          // the resume-plan POST is delivered but its response is lost:
          // the orchestration continues server-side while the UI sees a
          // network failure and recovers through exactly one inspect
          if (path.endsWith("/resume-plan")) {
            // the POST is delivered; the client connection is dropped
            // before the response, so the UI sees the loss while the
            // stage worker is still held in flight server-side
            return new Promise<Response>((resolve, reject) => {
              const controller = new AbortController();
              realFetch(`http://127.0.0.1:${port}${path}`, { ...(init ?? {}), signal: controller.signal })
                .then(resolve, reject);
              setTimeout(() => controller.abort(), 2200);
            });
          }
        }
        return realFetch(`http://127.0.0.1:${port}${path}`, init);
      };

      const harness: UiHarness = await createUiHarness({ fetchImpl });
      try {
        harness.click("refresh-projects");
        harness.click("refresh-runs");
        await Bun.sleep(300);
        expect(harness.runRows()).toContain(natural);
        const rowIndex = harness.runRows().indexOf(natural);

        // selection loads the first page through the real endpoint
        harness.clickRunInspect(rowIndex);
        await Bun.sleep(1500);
        expect(harness.detailRunId()).toBe(`Run ${natural}`);
        expect(harness.eventsRows().length).toBe(8);
        expect(harness.eventsMeta()).toContain("caught up");
        expect(eventsGets).toEqual([`/v1/runs/${natural}/events?after=0&limit=100`]);

        // the resume-plan POST is sent and its response is lost
        const controls = harness.element("run-controls");
        const stageInput = controls.querySelector(".form-stage-id");
        const budgetInput = controls.querySelector(".form-initial-budget");
        expect(stageInput).not.toBeNull();
        expect(budgetInput).not.toBeNull();
        (stageInput as unknown as { value: string }).value = "stage-1";
        (budgetInput as unknown as { value: string }).value = "1";
        const form = controls.querySelector("form");
        expect(form).not.toBeNull();
        (form as unknown as { dispatch: (type: string) => void }).dispatch("submit");

        // the lost response is recovered through exactly one inspect,
        // whose applied boundary pulls exactly one tail; while the stage
        // worker is held in flight the live polls pull further tails
        let reachedWaiting = false;
        for (let index = 0; index < 60; index += 1) {
          await Bun.sleep(500);
          if (harness.detailStatus() === "waiting") {
            reachedWaiting = true;
            break;
          }
        }
        expect(reachedWaiting).toBe(true);
        expect(posts).toEqual([`/v1/runs/${natural}/resume-plan`]);
        expect(eventsGets.length).toBeGreaterThanOrEqual(3);
        const afters = eventsGets.map((path) => Number(/after=(\d+)/.exec(path)?.[1]));
        expect(afters[0]).toBe(0);
        for (let index = 1; index < afters.length; index += 1) {
          expect(afters[index]).toBeGreaterThan(afters[index - 1] ?? 0);
        }
        expect(harness.eventsMeta()).toContain("caught up");
        const rowsAfterWaiting = harness.eventsRows().length;
        expect(rowsAfterWaiting).toBeGreaterThanOrEqual(18);
        // let the final tail settle before the isolation probe
        await Bun.sleep(1500);

        // a timeline error never POSTs and never breaks the inspect flow
        harness.element("detail-run-id").textContent = "Run ghost-run-x";
        harness.click("inspect-selected");
        await Bun.sleep(1200);
        expect(harness.eventsStatus()).toBe("Lifecycle events error: binding: binding_not_found.");
        expect(posts).toEqual([`/v1/runs/${natural}/resume-plan`]);
        // re-selecting the natural run recovers the timeline
        harness.clickRunInspect(rowIndex);
        await Bun.sleep(1500);
        expect(harness.detailRunId()).toBe(`Run ${natural}`);
        expect(harness.eventsRows().length).toBeGreaterThanOrEqual(rowsAfterWaiting);
        expect(harness.eventsMeta()).toContain("caught up");
        expect(harness.eventsStatus()).toContain("Added");
      } finally {
        harness.dispose();
      }
    },
    120000,
  );

  test("e4. a removed registry project is project_unavailable with a zero-write tree", async () => {
    const runTree = join(stateRoot, "pipeline-runs", synthFull);
    const before = fingerprintTree(runTree);
    await service?.stop();
    // the registry must stay non-empty: project-a is removed and a
    // different project remains, so the bound project is unavailable
    writeFileSync(
      join(smokeRoot, "registry-removed.json"),
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-b",
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
    service = await startControlPlaneHttpService({
      registryPath: join(smokeRoot, "registry-removed.json"),
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
    const response = await rawRequest(port, "GET", `/v1/runs/${synthFull}/events?after=0&limit=100`);
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
    expect(fingerprintTree(runTree)).toBe(before);
  }, 60000);
});
