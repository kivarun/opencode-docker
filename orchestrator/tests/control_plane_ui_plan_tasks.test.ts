/**
 * Permanent HTTP integration of the browser UI "Plan and tasks" slice
 * (S6): the real `app.js` identity, task-body and plan-content parsers
 * and merges consume the real loopback-service endpoints over a real
 * published schema-v7 run (real task/plan manifests in the run-plan
 * store), the reassembled task text is byte-exact with the durable
 * SHA-256 body digest recomputed from the served chunk bytes, the plan
 * pointer verdict resolves from the real ledger, the authority failures
 * fail closed, and every GET is zero-write over the run tree.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { lstatSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyPlanTasksIdentityPage,
  applyTaskChunk,
  createIdentityTransport,
  createPlanContentTransport,
  createPlanTasksState,
  createPlanTasksViewer,
  createTaskContentTransport,
  derivePlanTasksView,
  parseIdentityDocument,
  parsePlanContentDocument,
  parsePlanContentEndpointDocument,
  parseTaskContentDocument,
  parseTaskContentEndpointDocument,
  verifyTaskBodyDigest,
} from "../ui/app.js";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { stageTaskBodyDigest } from "../src/pipeline_v2_stage_task.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";

const CANARY = "uitaskservice";
const V2_PROFILE_EPOCH_BINDING = { digest: "d".repeat(64), profile_count: 1 };
const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };
const PROTECTED_DIGEST = "a".repeat(64);
const hex = (char: string): string => char.repeat(64);

const PIPELINE_YAML = `
schema_version: 2
entry_state: architect
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: dev_entry
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

interface RawResponse {
  readonly status: number;
  readonly body: string;
}

function rawRequest(port: number, path: string): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      { host: "127.0.0.1", port, method: "GET", path, headers: { host: `127.0.0.1:${port}` } },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
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

describe("control-plane ui plan/tasks: the real endpoints through the S6 model", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-pt-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const projectDir = join(smokeRoot, "project");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const bundle = join(smokeRoot, "synth-bundle");
  const runSuffix = Math.random().toString(36).slice(2, 8);
  const runId = `ui-pt-${runSuffix}`;
  const ghostRunId = `ui-pt-ghost-${runSuffix}`;
  let service: RunningControlPlaneHttpService | null = null;
  let port = 0;
  const secrets = [smokeRoot, stateRoot, credDir, storeRoot, projectDir, "smoke-credential-token"];

  interface ldirs {
    root: string;
    runRoot: string;
    taskA1Body: string;
    taskA2Body: string;
    taskB1Body: string;
  }
  const dirs: ldirs = { root: smokeRoot, runRoot: join(stateRoot, "pipeline-runs", runId), taskA1Body: "", taskA2Body: "", taskB1Body: "" };

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(join(smokeRoot, "config"), { mode: 0o700 });
    mkdirSync(join(smokeRoot, "inputs"), { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(smokeRoot, "config"),
            pipeline_root: bundle,
            launcher_id: null,
            input_roots: [{ id: "tasks", root: join(smokeRoot, "inputs") }],
          },
        ],
      }),
      { mode: 0o600 },
    );

    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    writeFileSync(join(bundle, "pipeline.yaml"), PIPELINE_YAML);
    writeFileSync(join(bundle, "prompts", "coder.md"), "PLAN-THE-WORK\n");
    writeFileSync(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    const pipeline = await loadPipelineV2(bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);

    const clock = { value: 0 };
    const now = (): Date => {
      clock.value += 1;
      return new Date(Date.UTC(2026, 0, 3, 0, 0, clock.value));
    };
    const sink = new PipelineV2RunStateSink({ stateRoot, runId, now });
    await sink.dispatch({
      kind: "create_run",
      runId,
      pipeline: identity,
      inputs: [{ id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST }],
      projectProvenance: { ...V2_PROJECT_PROVENANCE_BINDING },
      profileEpoch: { ...V2_PROFILE_EPOCH_BINDING },
    });
    let session = 0;
    const planning = async (): Promise<number> => {
      session += 1;
      await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
      await sink.dispatch({ kind: "agent_data_prepared" });
      await sink.dispatch({ kind: "agent_execution_session_created", sessionId: `sess-${session}` });
      await sink.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${session}` });
      await sink.dispatch({ kind: "agent_running" });
      await sink.dispatch({ kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } });
      await sink.dispatch({ kind: "agent_cleanup_completed" });
      return sink.snapshot !== null ? sink.snapshot.executions.length : 0;
    };

    const execution1 = await planning();
    const taskA1Body = `BUILD the ledger worker for ${CANARY}.\nSecond revision line.\n`;
    const taskB1Body = "THE SERVICE TASK\n";
    const taskA1 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: runId,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: taskA1Body,
    });
    const taskB1 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: runId,
      task_id: "task-b",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: taskB1Body,
    });
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: runId,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: execution1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 1, sha256: taskA1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: taskB1.sha256, depends_on: ["task-a"] },
          ],
        },
      ],
    });
    await acceptPipelineV2RunPlanCandidate({
      pipeline,
      runRoot: dirs.runRoot,
      sink,
      candidate: preparePipelineV2RunPlanCandidate({
        plan: plan1,
        taskRevisions: [taskA1, taskB1],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      }),
    });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: execution1,
    });
    const waitRequest = hex("e");
    const waitIntent = hex("c");
    const waitResponse = hex("f");
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "dev_entry",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: waitRequest,
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: waitIntent });
    const taskA2Body = `BUILD the revised worker for ${CANARY}.\nSecond revision line.\n`;
    const taskA2 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: runId,
      task_id: "task-a",
      revision: 2,
      previous_sha256: taskA1.sha256,
      origin: "user_response",
      body: taskA2Body,
    });
    await sink.dispatch({
      kind: "task_revision_accepted",
      taskId: "task-a",
      revision: 2,
      taskSha256: taskA2.sha256,
      waitIndex: 1,
      intentSha256: waitIntent,
    });
    await sink.dispatch({
      kind: "wait_response_recorded",
      waitIndex: 1,
      expectedRequestSha256: waitRequest,
      actionId: "revise_task",
      responseSha256: waitResponse,
    });
    const execution2 = await planning();
    const plan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: runId,
      revision: 2,
      previous_sha256: plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: execution2,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 2, sha256: taskA2.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: taskB1.sha256, depends_on: ["task-a"] },
          ],
        },
      ],
    });
    await acceptPipelineV2RunPlanCandidate({
      pipeline,
      runRoot: dirs.runRoot,
      sink,
      candidate: preparePipelineV2RunPlanCandidate({
        plan: plan2,
        taskRevisions: [taskA2, taskB1],
        previousPlan: plan1,
        previousTaskRevisions: [taskA1],
        protectedInputDigest: PROTECTED_DIGEST,
      }),
    });

    dirs.taskA1Body = taskA1Body;
    dirs.taskA2Body = taskA2Body;
    dirs.taskB1Body = taskB1Body;

    // the bound ghost run with no durable state
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    const claim = await store.claim({ runId, projectId: "project-a" });
    expect(["created", "existing"]).toContain(claim.kind);
    const ghost = await store.claim({ runId: ghostRunId, projectId: "project-a" });
    expect(["created", "existing"]).toContain(ghost.kind);

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
        },
        stdoutLimitBytes: 4_000_000,
        stderrLimitBytes: 4_000_000,
        sigintGraceMs: 3000,
      },
      port: 0,
      bodyLimitBytes: 4096,
    });
    port = service.port;
  }, 60000);

  afterAll(async () => {
    if (service !== null) await service.stop();
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  async function readIdentityPage(runIdArg: string, after: number, limit: number): Promise<{ status: number; page: Record<string, unknown> }> {
    const response = await rawRequest(port, `/v1/runs/${runIdArg}/identity?after=${after}&limit=${limit}`);
    return { status: response.status, page: JSON.parse(response.body) as Record<string, unknown> };
  }

  async function readPlanPage(runIdArg: string, revision: number, after: number, limit: number): Promise<{ status: number; page: Record<string, unknown> | null; body: string }> {
    const response = await rawRequest(port, `/v1/runs/${runIdArg}/plans/${revision}?after=${after}&limit=${limit}`);
    return { status: response.status, body: response.body, page: response.status === 200 ? JSON.parse(response.body) as Record<string, unknown> : null };
  }

  async function readTaskChunk(runIdArg: string, taskId: string, revision: number, offset: number, limit: number): Promise<{ status: number; document: Record<string, unknown> | null; body: string }> {
    const response = await rawRequest(port, `/v1/runs/${runIdArg}/tasks/${taskId}/revisions/${revision}?offset=${offset}&limit=${limit}`);
    return { status: response.status, body: response.body, document: response.status === 200 ? JSON.parse(response.body) as Record<string, unknown> : null };
  }

  async function streamIdentity(runIdArg: string, revision: number): Promise<{ records: readonly Record<string, unknown>[]; pages: number }> {
    let state = createPlanTasksState(runIdArg, revision);
    const records: Record<string, unknown>[] = [];
    let cursor = 0;
    for (;;) {
      const { status, page } = await readIdentityPage(runIdArg, cursor, 20);
      expect(status).toBe(200);
      expect(page.kind).toBe("identity");
      const document = parseIdentityDocument(page as never, runIdArg);
      const merged = applyPlanTasksIdentityPage(state, document, cursor) as ReturnType<typeof createPlanTasksState>;
      if (merged === null) throw new Error("the pinned identity page must apply");
      state = merged;
      for (const record of document.records as readonly Record<string, unknown>[]) {
        records.push(record);
      }
      if (document.nextAfter === null) break;
      cursor = document.nextAfter;
    }
    return { records, pages: Math.ceil(records.length / 20) };
  }

  test("p1. the real identity ledgers paginate into the S6 state", async () => {
    const first = await readIdentityPage(runId, 0, 20);
    expect(first.status).toBe(200);
    expect(first.page.kind).toBe("identity");
    const revision = first.page.revision as number;
    const { records } = await streamIdentity(runId, revision);
    const taskRecords = records.filter((record) => record.record === "task_revision");
    const planRecords = records.filter((record) => record.record === "plan_revision");
    expect(taskRecords.map((record) => `${record.taskId}@${record.revision}`)).toEqual([
      "task-a@1",
      "task-b@1",
      "task-a@2",
    ]);
    expect(planRecords.map((record) => record.revision)).toEqual([1, 2]);
    expectNoLeak(JSON.stringify(taskRecords), secrets);
  }, 60000);

  function viewerOverReal(runIdArg: string): { viewer: ReturnType<typeof createPlanTasksViewer>; states: unknown[] } {
    const states: unknown[] = [];
    const identity = createIdentityTransport({
      fetchIdentity: async (path: string, runIdArg: string): Promise<unknown> => {
        const served = await rawRequest(port, path);
        const payload = JSON.parse(served.body) as Record<string, unknown>;
        if (payload.kind === "identity") {
          return { ok: true, model: parseIdentityDocument(payload as never, runIdArg) };
        }
        return { ok: true, model: payload };
      },
      onStarted: () => {},
      onSettled: () => {},
    } as never);
    const task = createTaskContentTransport({
      fetchTaskContent: async (path: string, _runId: string, signal: AbortSignal, binding: unknown): Promise<unknown> => {
        void signal;
        const served = await rawRequest(port, path);
        const payload = JSON.parse(served.body) as Record<string, unknown>;
        return { ok: true, model: parseTaskContentEndpointDocument(payload as never, binding as never) };
      },
      onStarted: () => {},
      onSettled: () => {},
    } as never);
    const plan = createPlanContentTransport({
      fetchPlanContent: async (path: string, _runId: string, signal: AbortSignal, binding: unknown): Promise<unknown> => {
        void signal;
        const served = await rawRequest(port, path);
        const payload = JSON.parse(served.body) as Record<string, unknown>;
        return { ok: true, model: parsePlanContentEndpointDocument(payload as never, binding as never) };
      },
      onStarted: () => {},
      onSettled: () => {},
    } as never);
    const viewer = createPlanTasksViewer({
      identityTransport: identity,
      taskTransport: task,
      planTransport: plan,
      onState: (state: unknown) => {
        states.push(state);
      },
    } as never);
    return { viewer, states };
  }

  function makeDelay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  test("p2. the real task endpoint streams the byte-exact verified body through the real transports", async () => {
    const first = await readIdentityPage(runId, 0, 200);
    const revision = first.page.revision as number;
    const { viewer } = viewerOverReal(runId);
    viewer.inspectApplied({ kind: "run", runId, revision });
    await makeDelay(400);
    const pinned = viewer.currentState() as Record<string, unknown> | null;
    expect(pinned).not.toBeNull();
    const rows = ((pinned?.identity as Record<string, unknown>).records as readonly Record<string, unknown>[]).filter(
      (record) => record.record === "task_revision" && `${record.taskId}@${record.revision}` === "task-a@1",
    );
    expect(rows.length).toBe(1);
    viewer.dispose();
    const fresh = viewerOverReal(runId);
    fresh.viewer.inspectApplied({ kind: "run", runId, revision });
    await makeDelay(400);
    fresh.viewer.selectTask("task-a", 1);
    await makeDelay(400);
    const state = fresh.viewer.currentState() as Record<string, unknown> | null;
    const target = state?.taskTarget as Record<string, unknown> | null;
    expect(target?.status).toBe("ready");
    const view = derivePlanTasksView(state as never) as { taskTarget: { text: string | null; durable: string; integrity: string; byteCount: number; bodySha256: string } };
    expect(view.taskTarget.text).toBe(dirs.taskA1Body);
    expect(view.taskTarget.integrity).toBe("verified");
    expect(view.taskTarget.durable).toBe("absent");
    expect(view.taskTarget.byteCount).toBe(65);
    const storedDocument = target?.envelope as Record<string, unknown>;
    const wireDigest = storedDocument.bodySha256 as string;
    // a tampered payload recomputes to null against the served digest
    const bogus = await verifyTaskBodyDigest(new TextEncoder().encode("tampered"), wireDigest);
    expect(bogus).toBe(null);
    expectNoLeak(JSON.stringify(storedDocument), secrets);
    fresh.viewer.dispose();
  }, 60000);

  test("p3. the real chunked task stream reassembles the full body", async () => {
    const parts: Uint8Array[] = [];
    let offset = 0;
    for (;;) {
      const chunk = await readTaskChunk(runId, "task-a", 1, offset, 10);
      expect(chunk.status).toBe(200);
      const document = chunk.document as Record<string, unknown>;
      const parsed = parseTaskContentDocument(document as never, { runId, taskId: "task-a", revision: 1 }) as { body: { data: Uint8Array; nextOffset: number | null } };
      parts.push(parsed.body.data);
      if (parsed.body.nextOffset === null) {
        break;
      }
      offset = parsed.body.nextOffset;
    }
    const whole = Buffer.concat(parts);
    const bodyShape = JSON.parse(await rawRequest(port, `/v1/runs/${runId}/tasks/task-a/revisions/1?offset=0&limit=65536`).then((r) => r.body)) as Record<string, unknown>;
    const bodyDoc = bodyShape.body as { sha256: string };
    const composed = await verifyTaskBodyDigest(whole, bodyDoc.sha256 as string);
    expect(composed).toBe(bodyDoc.sha256);
    expect(stageTaskBodyDigest(whole)).toBe(bodyDoc.sha256);
    expect(whole.toString("utf8")).toBe(dirs.taskA1Body);
  }, 60000);

  test("p4. the real plan endpoint derives the pointer verdict through the state", async () => {
    const first = await readIdentityPage(runId, 0, 200);
    const revision = first.page.revision as number;
    const { viewer } = viewerOverReal(runId);
    viewer.inspectApplied({ kind: "run", runId, revision });
    await makeDelay(400);
    viewer.selectPlan(1);
    await makeDelay(400);
    const state = viewer.currentState() as Record<string, unknown> | null;
    const planTarget = (derivePlanTasksView(state as never) as { planTarget: Record<string, unknown> }).planTarget;
    expect(planTarget.status).toBe("ready");
    expect(planTarget.revision).toBe(1);
    expect(planTarget.pointerStatus).toBe("verified");
    const planRecord = (state?.identity as Record<string, unknown>).records as readonly Record<string, unknown>[];
    const planLedgerRecord = planRecord.find((record) => record.record === "plan_revision" && record.revision === 1) as Record<string, unknown>;
    expect(planTarget.manifestSha256).toBe(planLedgerRecord.sha256);
    expect(typeof planTarget.originExecution).toBe("number");
    expectNoLeak(JSON.stringify(planTarget), secrets);
    viewer.dispose();
  }, 60000);

  test("p5. the ghost run fails closed at the identity endpoint", async () => {
    const served = await rawRequest(port, `/v1/runs/${ghostRunId}/identity?after=0&limit=200`);
    expect(served.status).toBe(200);
    const document = JSON.parse(served.body) as Record<string, unknown>;
    expect(document.kind).toBe("command_result");
    expect(document.outcome).toBe("failed");
    expect(document.reason).toBe("run_not_found");
    const ghost = await rawRequest(port, `/v1/runs/${ghostRunId}/tasks/ghost-task/revisions/1?offset=0&limit=10`);
    expect(ghost.status).toBe(200);
    const ghostDocument = JSON.parse(ghost.body) as Record<string, unknown>;
    expect(ghostDocument.kind).toBe("command_result");
    expect(ghostDocument.outcome).toBe("failed");
  }, 60000);

  test("p6. every identity/plan/task GET is zero-write and byte-identical across reads", async () => {
    const runTree = dirs.runRoot;
    const before = fingerprintTree(runTree);
    const firstRound = await readIdentityPage(runId, 0, 200);
    const secondRound = await rawRequest(port, `/v1/runs/${runId}/identity?after=0&limit=200`);
    expect(JSON.stringify(firstRound.page)).toBe(secondRound.body);
    const taskRoundA = await rawRequest(port, `/v1/runs/${runId}/tasks/task-a/revisions/1?offset=0&limit=65536`);
    const taskRoundB = await rawRequest(port, `/v1/runs/${runId}/tasks/task-a/revisions/1?offset=0&limit=65536`);
    expect(taskRoundA.body).toBe(taskRoundB.body);
    const planRoundA = await rawRequest(port, `/v1/runs/${runId}/plans/1?after=0&limit=200`);
    const planRoundB = await rawRequest(port, `/v1/runs/${runId}/plans/1?after=0&limit=200`);
    expect(planRoundA.body).toBe(planRoundB.body);
    expect(fingerprintTree(runTree)).toBe(before);
    expectNoLeak(taskRoundA.body, secrets);
  }, 60000);
});
