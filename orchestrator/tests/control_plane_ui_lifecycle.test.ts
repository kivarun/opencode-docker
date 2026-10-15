/**
 * Permanent lifecycle suite of the browser UI v2 manual controls: the
 * four mutating flows (resume-plan, continue-stage, revise-task and the
 * fresh run) are driven end to end through the REAL HTTP handlers, the
 * real CLI subprocess and the fake Docker Helper daemon, while every
 * decision is derived exclusively from the inspect projection through
 * the pure UI model (`parseApiDocument` → `buildRunActionModel` →
 * `buildMutationRequest`) — exactly what the browser does.
 *
 * Matrix: A the full lifecycle
 * `run → plan_ready → resume-plan → waiting → continue-stage → waiting →
 *  revise-task → plan_ready → resume-plan → succeeded` with revision and
 * boundary evidence at every step, B the kernel run_busy authority
 * through UI-built parallel requests, C discovery and inspect zero-write
 * fingerprints, D the project/input-root authority of revise-task,
 * E the leakage battery.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildFreshRunBody,
  buildMutationRequest,
  buildRunActionModel,
  createRunsState,
  applyRunsPage,
  parseApiDocument,
  type UiApiModel,
  type UiRunActionModel,
} from "../ui/app.js";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";

const CANARY = "uilifecyclehostpath";

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

async function fingerprintTree(root: string): Promise<string> {
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

/** The UI inspect step: strictly the versioned run document. */
async function uiInspect(port: number, runId: string): Promise<UiApiModel & { kind: "run" }> {
  const response = await rawRequest(port, "GET", `/v1/runs/${runId}`);
  expect(response.status).toBe(200);
  const model = parseApiDocument(JSON.parse(response.body));
  expect(model.kind).toBe("run");
  return model as UiApiModel & { kind: "run" };
}

/** The UI mutation step: the builder owns path and body; one POST. */
async function uiMutate(port: number, action: unknown, formValues: unknown): Promise<Record<string, unknown>> {
  const request = buildMutationRequest(action, formValues);
  const response = await rawRequest(port, "POST", request.path, JSON.stringify(request.body));
  expect(response.status).toBe(200);
  const model = parseApiDocument(JSON.parse(response.body));
  expect(["command_result", "error"]).toContain(model.kind);
  return model as unknown as Record<string, unknown>;
}

function expectNoLeak(text: string, secrets: readonly string[]): void {
  for (const secret of secrets) {
    expect(text.includes(secret)).toBe(false);
  }
}

describe("control-plane UI lifecycle: boundary-driven manual controls", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-lifecycle-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let daemonPid = 0;
  const runId = `ui-${Math.random().toString(36).slice(2, 8)}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-LIFECYCLE-TASK\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "UI-LIFECYCLE-TASK-V2\n", { mode: 0o600 });
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
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_daemon.ts"), socketPath], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("lifecycle daemon socket never appeared");
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
  }, 30000);

  afterAll(async () => {
    if (service !== undefined) await service.stop();
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test(
    "l1. the full lifecycle through UI-built decisions and requests",
    async () => {
      // fresh run: the caller-owned run id and the exact discovery-bound body
      const body = buildFreshRunBody("project-a", runId, [{ id: "task", rootId: "tasks", ref: "task.md" }]);
      const created = await rawRequest(port, "POST", "/v1/projects/project-a/runs", JSON.stringify(body));
      expect(created.status).toBe(200);
      const createdModel = parseApiDocument(JSON.parse(created.body));
      expect(createdModel.kind).toBe("command_result");
      expect((createdModel as { outcome: string }).outcome).toBe("plan_ready");

      // discovery: the run entry and the project input roots are the only authority
      const runsPage = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/runs?limit=100")).body));
      expect(runsPage.kind).toBe("runs");
      const runsState = applyRunsPage(createRunsState(), runsPage as never);
      const entry = runsState.runs.find((run) => run.runId === runId);
      expect(entry).toEqual({ runId, projectId: "project-a", projectAvailable: true });
      const projects = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/projects")).body)) as UiApiModel & { kind: "projects" };
      expect(projects.kind).toBe("projects");

      // boundary 1: plan_ready / resume_plan → resume-plan form
      const first = await uiInspect(port, runId);
      expect(first.status).toBe("running");
      expect(first.boundary).toEqual({ kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume_plan" });
      const revisionAfterCreate = first.revision;
      const action1 = buildRunActionModel(first, { projectsModel: projects, runEntry: entry ?? null });
      expect((action1 as UiRunActionModel).kind).toBe("resume_plan");
      const step1 = await uiMutate(port, action1, { stageId: "stage-1", initialBudget: "1" });
      expect(step1["outcome"]).toBe("waiting");

      // boundary 2: waiting with the server action order → continue-stage form
      const second = await uiInspect(port, runId);
      expect(second.status).toBe("waiting");
      expect(second.revision).toBeGreaterThan(revisionAfterCreate);
      expect(second.boundary).toMatchObject({ kind: "waiting", waitIndex: 1, reason: "stage_iteration_completed" });
      expect((second.boundary as { actions: { id: string }[] }).actions.map((action) => action.id)).toEqual(["continue_stage", "revise_task"]);
      const action2 = buildRunActionModel(second, { projectsModel: projects, runEntry: entry ?? null });
      expect((action2 as UiRunActionModel).kind).toBe("waiting");
      const continueControl = (action2 as unknown as { controls: { kind: string; waitIndex: number }[] }).controls[0];
      expect(continueControl).toMatchObject({ kind: "continue_stage", waitIndex: 1 });
      const step2 = await uiMutate(port, continueControl, { additionalIterations: "1" });
      expect(step2["outcome"]).toBe("waiting");

      // boundary 3: waiting again (the grant opened iteration 2) → revise-task form
      const third = await uiInspect(port, runId);
      expect(third.boundary).toMatchObject({ kind: "waiting", waitIndex: 2 });
      const action3 = buildRunActionModel(third, { projectsModel: projects, runEntry: entry ?? null });
      const reviseControl = (action3 as unknown as { controls: { kind: string; waitIndex: number; projectId: string; inputRootIds: string[] }[] }).controls.find(
        (control) => control.kind === "revise_task",
      );
      expect(reviseControl).toMatchObject({ waitIndex: 2, projectId: "project-a", inputRootIds: ["tasks"] });
      const step3 = await uiMutate(port, reviseControl, { taskId: "task-a", rootId: "tasks", ref: "task-a-v2.md" });
      expect(step3["outcome"]).toBe("plan_ready");

      // boundary 4: replanned plan_ready / resume_plan → the second stage
      const fourth = await uiInspect(port, runId);
      expect(fourth.boundary).toMatchObject({ kind: "plan_ready", continuation: "resume_plan" });
      // executions: architect, dev_entry (iteration 1), dev_entry (iteration 2), planner2
      expect((fourth.boundary as { planningExecutionIndex: number }).planningExecutionIndex).toBe(4);
      const action4 = buildRunActionModel(fourth, { projectsModel: projects, runEntry: entry ?? null });
      expect((action4 as UiRunActionModel).kind).toBe("resume_plan");
      const step4 = await uiMutate(port, action4, { stageId: "stage-2", initialBudget: "1" });
      expect(step4["outcome"]).toBe("succeeded");

      // terminal: outputs projection only, no manual controls
      const final = await uiInspect(port, runId);
      expect(final.status).toBe("succeeded");
      expect(final.boundary).toEqual({ kind: "succeeded", terminalStateId: "done" });
      expect(final.outputs).toEqual([
        { id: "report", type: "file", required: true, present: true, digest: expect.stringMatching(/^[0-9a-f]{64}$/) },
      ]);
      const finalAction = buildRunActionModel(final, { projectsModel: projects, runEntry: entry ?? null });
      expect(finalAction).toEqual({ kind: "terminal", runId, status: "succeeded" });
      expect(() => buildMutationRequest(finalAction, {})).toThrow();

      expectNoLeak(JSON.stringify([first, second, third, fourth, final]), secrets);
    },
    120000,
  );

  test(
    "l2. the kernel run mutation lock stays authoritative for UI-built parallel requests",
    async () => {
      // a second run at the same wait boundary: two parallel continue-stage
      // requests — exactly one mutating continuation wins
      const body = buildFreshRunBody("project-a", `${runId}-busy`, [{ id: "task", rootId: "tasks", ref: "task.md" }]);
      await rawRequest(port, "POST", "/v1/projects/project-a/runs", JSON.stringify(body));
      const model = await uiInspect(port, `${runId}-busy`);
      const action = buildRunActionModel(model, { projectsModel: null, runEntry: null });
      expect((action as UiRunActionModel).kind).toBe("resume_plan");
      const winner = uiMutate(port, action, { stageId: "stage-1", initialBudget: "1" });
      await Bun.sleep(300);
      const loser = rawRequest(port, "POST", `/v1/runs/${runId}-busy/resume-plan`, JSON.stringify({ stage_id: "stage-1", initial_budget: 1 }));
      const [winnerResult, loserResult] = await Promise.all([winner, loser]);
      expect(winnerResult["outcome"]).toBe("waiting");
      const loserModel = parseApiDocument(JSON.parse((await loserResult).body));
      expect(loserModel.kind).toBe("command_result");
      expect((loserModel as { outcome: string; reason: string | null })).toMatchObject({ outcome: "failed", reason: "run_busy" });
    },
    60000,
  );

  test("l3. discovery and inspect are zero-write and never leak host material", async () => {
    const runTree = join(stateRoot, "pipeline-runs", runId);
    const before = await fingerprintTree(runTree);
    const discovery = await rawRequest(port, "GET", "/v1/runs?limit=100");
    expect(discovery.status).toBe(200);
    const inspect = await uiInspect(port, runId);
    expect(inspect.kind).toBe("run");
    expect(await fingerprintTree(runTree)).toBe(before);
    expectNoLeak(discovery.body, secrets);
    expectNoLeak(JSON.stringify(inspect), secrets);
  });
});
