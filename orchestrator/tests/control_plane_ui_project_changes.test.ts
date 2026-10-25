/**
 * Permanent end-to-end suite of the browser UI v6 read-only Project
 * changes panel over the REAL production stack: the loopback HTTP
 * service, the project registry, the durable run binding store, the
 * real orchestrator CLI subprocess, the schema-v10 provenance and the
 * fake Docker Helper daemon whose stage workers write into the shared
 * run project workspace — so a terminal run carries genuine run
 * workspace changes relative to the durable project baseline.
 *
 * Matrix: the automatic one-shot initial GET after the terminal
 * inspect, the added/modified projection with exact metadata, the
 * pagination and stale-observation battery, the failed terminal run,
 * the provenance tamper refusal, the restart identity, zero-write
 * fingerprints and the leakage scan.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildFreshRunBody,
  buildMutationRequest,
  buildRunActionModel,
  parseApiDocument,
  type UiApiModel,
  type UiRunActionModel,
} from "../ui/app.js";
import { createUiHarness, type UiHarness } from "./ui_dom_harness.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";

const CANARY = "uichgshostpath";

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

async function uiInspect(port: number, runId: string): Promise<UiApiModel & { kind: "run" }> {
  const response = await rawRequest(port, "GET", `/v1/runs/${runId}`);
  expect(response.status).toBe(200);
  const model = parseApiDocument(JSON.parse(response.body));
  expect(model.kind).toBe("run");
  return model as UiApiModel & { kind: "run" };
}

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

const wait = (ms = 30): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** The real-service inspect is a CLI subprocess: wait for the applied
 * inspect (detail rendered or the safe error line) before asserting. */
async function waitForInspect(harness: UiHarness): Promise<void> {
  const deadline = Date.now() + 15000;
  while (harness.detailStatus() === "" && harness.error() === "") {
    if (Date.now() > deadline) {
      throw new Error("waitForInspect timeout");
    }
    await wait(25);
  }
}

/** Waits until the changes panel settled (loading states left). */
async function waitForChanges(harness: UiHarness): Promise<void> {
  await waitFor(() => harness.changesStatus() !== "Loading…" && (harness.changesStatus() !== "" || harness.changesMeta() !== ""), "changes settle");
}

/** Waits until the predicate holds (real-service latency). */
async function waitFor(predicate: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 15000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`waitFor timeout: ${what}`);
    await wait(25);
  }
}

describe("control-plane UI project changes: the read-only panel over the real stack", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-changes-${CANARY}-`));
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
  const suffix = Math.random().toString(36).slice(2, 8);
  const runId = `chg-a-${suffix}`;
  const manyRunId = `chg-b-${suffix}`;
  const failRunId = `chg-c-${suffix}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];
  let firstObservation = "";

  const startService = async (): Promise<void> => {
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
  };

  /** The full UI-driven lifecycle of one run through the real stack. */
  const driveRun = async (id: string, taskRef: string, reviseRef: string): Promise<"succeeded" | "failed"> => {
    const body = buildFreshRunBody("project-a", id, [{ id: "task", rootId: "tasks", ref: taskRef }]);
    const created = await rawRequest(port, "POST", `/v1/projects/project-a/runs`, JSON.stringify(body));
    expect(created.status).toBe(200);
    const createdModel = parseApiDocument(JSON.parse(created.body)) as { kind: string; outcome?: string };
    expect(createdModel.outcome).toBe("plan_ready");
    const projects = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/projects")).body)) as UiApiModel & { kind: "projects" };
    const runsPage = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/runs?limit=100")).body));
    const entry = ((runsPage as unknown as { runs: { runId: string }[] }).runs.find((run) => run.runId === id) ?? null);
    let model = await uiInspect(port, id);
    let usedContinue = false;
    for (let round = 0; round < 8; round++) {
      const action = buildRunActionModel(model, { projectsModel: projects, runEntry: entry as never });
      const kind = (action as UiRunActionModel).kind;
      if (kind === "terminal") {
        return model.status === "succeeded" ? "succeeded" : "failed";
      }
      if (kind === "resume_plan") {
        const planningIndex = (model.boundary as { planningExecutionIndex?: number }).planningExecutionIndex;
        const step = await uiMutate(port, action, { stageId: planningIndex === 1 ? "stage-1" : "stage-2", initialBudget: "1" });
        expect(["waiting", "failed", "succeeded"]).toContain(step["outcome"] as string);
        if (step["outcome"] === "failed") {
          model = await uiInspect(port, id);
          continue;
        }
      } else if (kind === "waiting") {
        const controls = (action as unknown as { controls: Array<Record<string, unknown>> }).controls;
        const continueControl = controls.find((control) => control["kind"] === "continue_stage");
        const reviseControl = controls.find((control) => control["kind"] === "revise_task");
        if (continueControl !== undefined && !usedContinue) {
          usedContinue = true;
          const step = await uiMutate(port, continueControl, { additionalIterations: "1" });
          expect(step["outcome"]).toBe("waiting");
        } else if (reviseControl !== undefined) {
          const step = await uiMutate(port, reviseControl, { taskId: "task-a", rootId: "tasks", ref: reviseRef });
          expect(step["outcome"]).toBe("plan_ready");
        } else {
          throw new Error(`no usable waiting control: ${JSON.stringify(controls)}`);
        }
      } else {
        throw new Error(`unexpected action kind ${kind}`);
      }
      model = await uiInspect(port, id);
    }
    throw new Error("the run never reached a terminal boundary");
  };

  /** A fake-DOM harness whose fetch goes to the real loopback service. */
  const makeHarness = async (): Promise<{ harness: UiHarness; gets: string[]; posts: string[] }> => {
    const gets: string[] = [];
    const posts: string[] = [];
    const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
      const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
      if (method === "GET") {
        gets.push(path);
      } else {
        posts.push(path);
      }
      const response = await rawRequest(port, method, path);
      return new Response(response.body, { status: response.status, headers: { "content-type": "application/json" } });
    };
    const harness = await createUiHarness({ fetchImpl });
    return { harness, gets, posts };
  };

  const rowIndex = (harness: UiHarness, id: string): number => {
    const rows = harness.runRows();
    const index = rows.findIndex((row) => row === id);
    if (index < 0) {
      throw new Error(`run row missing for ${id}: ${JSON.stringify(rows)}`);
    }
    return index;
  };

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "CHANGES-TASK\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-many.md"), "CHANGES-MANY\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-fail.md"), "CHANGES-FAIL\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "UI-CHANGES-TASK-V2\n", { mode: 0o600 });
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
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_project_changes_daemon.ts"), socketPath, "200"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("changes daemon socket never appeared");
    await startService();
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
    "d1. the succeeded run: the automatic initial GET, the added/modified projection and zero side effects",
    async () => {
      expect(await driveRun(runId, "task.md", "task-a-v2.md")).toBe("succeeded");
      const projectFingerprint = await fingerprintTree(projectDir);
      const runTree = join(stateRoot, "pipeline-runs", runId);
      const treeBefore = await fingerprintTree(runTree);
      const { harness, gets, posts } = await makeHarness();
      try {
        harness.click("refresh-runs");
        await wait(50);
        await harness.clickRunInspect(rowIndex(harness, runId));
        await waitForInspect(harness);
        await waitForChanges(harness);
        // exactly one automatic initial GET right after the terminal inspect
        expect(gets.filter((path) => path.includes("/changes"))).toEqual([`/v1/runs/${runId}/changes?after=0&limit=100`]);
        expect(posts).toEqual([]);
        expect(harness.detailStatus()).toBe("succeeded");
        expect(harness.changesMeta()).toContain("snapshot revision");
        expect(harness.changesMeta()).toContain("pending");
        expect(harness.changesMeta()).toContain("loaded 3 of 3");
        const rows = harness.changesRows();
        expect(rows.length).toBe(3);
        expect(rows[0]).toMatch(/^README\.md\|content_modified\|matches_baseline\|file \u00b7 mode 0644 \u00b7 7 B \u00b7 sha256 [0-9a-f]{64}\|file \u00b7 mode 0644 \u00b7 15 B \u00b7 sha256 [0-9a-f]{64}$/);
        expect(rows[1]).toMatch(/^dev-notes\.txt\|added\|matches_baseline\|\|file \u00b7 mode 0644 \u00b7 10 B \u00b7 sha256 [0-9a-f]{64}$/);
        expect(rows[2]).toMatch(/^report-copy\.txt\|added\|matches_baseline\|\|file \u00b7 mode 0644 \u00b7 12 B \u00b7 sha256 [0-9a-f]{64}$/);
        expect(harness.changesSummary()).toContain("total 3");
        expect(harness.changesSummary()).toContain("added 2");
        expect(harness.changesSummary()).toContain("content 1");
        expect(harness.changesSummary()).toContain("baseline 3");
        expect(harness.changesButtonState("changes-load-more").hidden).toBe(true);
        expect(harness.changesButtonState("changes-reload").hidden).toBe(false);
        // zero content GETs: the only read paths are the documented ones
        for (const path of gets) {
          expect(path.startsWith("/v1/")).toBe(true);
        }
        expect(harness.changesStatus()).toBe("");
        // the operator project and the durable run tree stay untouched
        expect(await fingerprintTree(projectDir)).toBe(projectFingerprint);
        expect(await fingerprintTree(runTree)).toBe(treeBefore);
        await wait(250);
      } finally {
        harness.dispose();
      }
      // the observation identity is deterministic over unchanged trees
      const changes = JSON.parse((await rawRequest(port, "GET", `/v1/runs/${runId}/changes?after=0&limit=100`)).body) as { observation: string };
      firstObservation = changes.observation;
      expect(firstObservation).toMatch(/^[0-9a-f]{64}$/);
    },
    180000,
  );

  test(
    "d2. the paginated run: dense pages, the stale observation and the explicit reload",
    async () => {
      writeFileSync(join(projectDir, "mode.txt"), "MANY\n", { mode: 0o644 });
      expect(await driveRun(manyRunId, "task.md", "task-a-v2.md")).toBe("succeeded");
      const projectFingerprint = await fingerprintTree(projectDir);
      const { harness, gets, posts } = await makeHarness();
      try {
        harness.click("refresh-runs");
        await wait(50);
        await harness.clickRunInspect(rowIndex(harness, manyRunId));
        await waitForInspect(harness);
        await waitForChanges(harness);
        expect(harness.changesMeta()).toContain("loaded 100 of 153");
        expect(harness.changesButtonState("changes-load-more").hidden).toBe(false);
        const firstPageRows = harness.changesRows();
        expect(firstPageRows.length).toBe(100);
        // dense indexes from 1; the sorted-path order starts at README.md
        expect(firstPageRows[0]?.startsWith("README.md|content_modified|")).toBe(true);
        const beforeContinuation = JSON.parse((await rawRequest(port, "GET", `/v1/runs/${manyRunId}/changes?after=0&limit=100`)).body) as { observation: string };
        harness.click("changes-load-more");
        await waitForChanges(harness);
        expect(gets.filter((path) => path.includes("/changes"))).toEqual([
          `/v1/runs/${manyRunId}/changes?after=0&limit=100`,
          `/v1/runs/${manyRunId}/changes?after=100&limit=100&observation=${beforeContinuation.observation}`,
        ]);
        expect(harness.changesRows().length).toBe(153);
        expect(harness.changesMeta()).toContain("loaded 153 of 153");
        expect(posts).toEqual([]);
        expect(await fingerprintTree(projectDir)).toBe(projectFingerprint);
        await wait(250);
      } finally {
        harness.dispose();
      }
      // the stale battery: the registered project changes between pages
      const { harness: staleHarness, gets: staleGets } = await makeHarness();
      try {
        staleHarness.click("refresh-runs");
        await wait(50);
        await staleHarness.clickRunInspect(rowIndex(staleHarness, manyRunId));
        await waitForInspect(staleHarness);
        await waitForChanges(staleHarness);
        expect(staleHarness.changesRows().length).toBe(100);
        const preMutationObservation = JSON.parse((await rawRequest(port, "GET", `/v1/runs/${manyRunId}/changes?after=0&limit=100`)).body) as { observation: string };
        writeFileSync(join(projectDir, "README.md"), "README-THIRD-PARTY\n", { mode: 0o644 });
        staleHarness.click("changes-load-more");
        await waitFor(() => staleHarness.changesStatus().includes("Stale"), "stale state");
        expect(staleHarness.changesStatus()).toBe("Stale. The run workspace or registered project changed after this view was created. Reload the changes.");
        // the old coherent page stays visible
        expect(staleHarness.changesRows().length).toBe(100);
        expect(staleHarness.changesButtonState("changes-load-more").hidden).toBe(true);
        expect(staleHarness.changesButtonState("changes-reload").hidden).toBe(false);
        // no automatic after=0 request follows the stale state
        await wait(50);
        expect(staleGets.filter((path) => path.includes("/changes")).length).toBe(2);
        // the explicit reload re-derives everything from the new trees
        staleHarness.click("changes-reload");
        // a reload replaces the snapshot with its own fresh first page
        await waitFor(() => staleHarness.changesStatus() === "" && staleHarness.changesRows().length === 100 && staleHarness.changesMeta().includes("diverged"), "reload recovery");
        expect(staleHarness.changesStatus()).toBe("");
        expect(staleHarness.changesRows().length).toBe(100);
        expect(staleHarness.changesMeta()).toContain("diverged");
        expect(staleHarness.changesMeta()).toContain("loaded 100 of 153");
        const reloaded = JSON.parse((await rawRequest(port, "GET", `/v1/runs/${manyRunId}/changes?after=0&limit=100`)).body) as { observation: string; summary: { diverged: number } };
        expect(reloaded.observation).not.toBe(preMutationObservation.observation);
        expect(reloaded.summary.diverged).toBe(1);
        // restore the operator project for the later tests
        writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
        await wait(250);
      } finally {
        staleHarness.dispose();
      }
    },
    180000,
  );

  test(
    "d3. the failed terminal run: the partial changes stay visible",
    async () => {
      writeFileSync(join(projectDir, "mode.txt"), "FAIL\n", { mode: 0o644 });
      expect(await driveRun(failRunId, "task.md", "task-a-v2.md")).toBe("failed");
      const { harness, gets, posts } = await makeHarness();
      try {
        harness.click("refresh-runs");
        await wait(50);
        await harness.clickRunInspect(rowIndex(harness, failRunId));
        await waitForInspect(harness);
        await waitForChanges(harness);
        expect(gets.filter((path) => path.includes("/changes"))).toEqual([`/v1/runs/${failRunId}/changes?after=0&limit=100`]);
        expect(harness.detailStatus()).toBe("failed");
        expect(harness.changesRows().length).toBe(2);
        expect(harness.changesRows()[0]?.startsWith("README.md|content_modified|")).toBe(true);
        expect(harness.changesRows()[1]?.startsWith("dev-notes.txt|added|")).toBe(true);
        expect(harness.changesMeta()).toContain("pending");
        expect(posts).toEqual([]);
        // restore the operator project for the later tests
        writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
        await wait(250);
      } finally {
        harness.dispose();
      }
    },
    180000,
  );

  test(
    "d4. the tampered provenance manifest: the terminal outcome stays visible, the changes refuse",
    async () => {
      const manifestPath = join(stateRoot, "pipeline-runs", failRunId, "project-provenance.json");
      const original = readFileSync(manifestPath, "utf8");
      writeFileSync(manifestPath, `${original}\n`, { mode: 0o600 });
      const { harness, gets } = await makeHarness();
      try {
        harness.click("refresh-runs");
        await wait(50);
        await harness.clickRunInspect(rowIndex(harness, failRunId));
        await waitForInspect(harness);
        await waitFor(() => harness.changesStatus().includes("artifact_modified"), "artifact refusal");
        // the terminal execution outcome is untouched
        expect(harness.detailStatus()).toBe("failed");
        // the changes panel refuses with zero rows and no partial list
        expect(harness.changesStatus()).toBe("failed (orchestrator: artifact_modified)");
        expect(harness.changesRows()).toEqual([]);
        expect(gets.filter((path) => path.includes("/changes"))).toEqual([`/v1/runs/${failRunId}/changes?after=0&limit=100`]);
        await wait(250);
      } finally {
        harness.dispose();
      }
      writeFileSync(manifestPath, original, { mode: 0o600 });
    },
    60000,
  );

  test(
    "d5. the restart: the same observation over unchanged trees with zero writes",
    async () => {
      const runTree = join(stateRoot, "pipeline-runs", runId);
      const treeBefore = await fingerprintTree(runTree);
      await service.stop();
      await startService();
      const { harness, gets } = await makeHarness();
      try {
        harness.click("refresh-runs");
        await wait(50);
        await harness.clickRunInspect(rowIndex(harness, runId));
        await waitForInspect(harness);
        await waitForChanges(harness);
        expect(harness.detailStatus()).toBe("succeeded");
        expect(harness.changesMeta()).toContain("snapshot revision");
        expect(gets.filter((path) => path.includes("/changes"))).toEqual([`/v1/runs/${runId}/changes?after=0&limit=100`]);
        const changes = JSON.parse((await rawRequest(port, "GET", `/v1/runs/${runId}/changes?after=0&limit=100`)).body) as { observation: string };
        expect(changes.observation).toBe(firstObservation);
        expect(await fingerprintTree(runTree)).toBe(treeBefore);
        expectNoLeak(JSON.stringify(changes), secrets);
        await wait(250);
      } finally {
        harness.dispose();
      }
    },
    60000,
  );
});
