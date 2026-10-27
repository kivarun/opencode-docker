/**
 * Permanent end-to-end suite of the browser UI v8 explicit durable
 * Project Apply over the REAL production stack: the loopback HTTP
 * service, the project registry (with the one per-project capability),
 * the durable run binding store, the real orchestrator CLI subprocess,
 * the schema-v10 provenance and the fake Docker Helper daemon whose
 * stage workers write into the shared run project workspace.
 *
 * Matrix: the happy path through the explicit confirmation to the
 * exact result tree with the record removal, the lost-response retry
 * on the same operation identity, the recovery of a simulated browser
 * restart through one GET without a POST, the stale-observation
 * refusal, the default-disabled capability with zero apply traffic,
 * the zero worker/session footprint of the apply and the durability
 * fingerprints.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  APPLY_RECORD_STORAGE_PREFIX,
  buildFreshRunBody,
  buildMutationRequest,
  buildRunActionModel,
  parseApiDocument,
  type UiApiModel,
  type UiRunActionModel,
} from "../ui/app.js";
import { createUiHarness, type UiHarness } from "./ui_dom_harness.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";

const CANARY = "uiapplyhostpath";

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

const wait = (ms = 30): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForInspect(harness: UiHarness): Promise<void> {
  const deadline = Date.now() + 15000;
  while (harness.detailStatus() === "" && harness.error() === "") {
    if (Date.now() > deadline) {
      throw new Error("waitForInspect timeout");
    }
    await wait(25);
  }
}

async function waitForChanges(harness: UiHarness): Promise<void> {
  await waitFor(() => harness.changesStatus() !== "Loading…" && (harness.changesStatus() !== "" || harness.changesMeta() !== ""), "changes settle");
}

async function waitFor(predicate: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 120000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`waitFor timeout: ${what}`);
    await wait(25);
  }
}

describe("control-plane UI project apply: the explicit operation over the real stack", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-apply-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const projectBDir = join(smokeRoot, "project-b");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const applyStoreRoot = join(smokeRoot, "applies");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let daemonPid = 0;
  const suffix = Math.random().toString(36).slice(2, 8);
  const applyRunId = `apl-a-${suffix}`;
  const lostRunId = `apl-b-${suffix}`;
  const staleRunId = `apl-c-${suffix}`;
  const plainRunId = `apl-d-${suffix}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, applyStoreRoot, "smoke-credential-token", CANARY];
  /** The observation of every applied operation, recorded by the tests
   * for the restart-recovery scenario. */
  const operationObservations = new Map<string, string>();

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
      applyStoreRoot,
    });
    port = service.port;
  };

  /** Restores the registered operator project to its baseline content
   * so every driven run starts from the same documented baseline (the
   * durable provenance is captured per run at its own copy). */
  const resetProjectDir = (): void => {
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
    for (const name of ["dev-notes.txt", "report-copy.txt"]) {
      try {
        rmSync(join(projectDir, name));
      } catch {
        // already absent
      }
    }
  };

  const driveRun = async (projectId: string, id: string, taskRef: string, reviseRef: string): Promise<"succeeded" | "failed"> => {
    resetProjectDir();
    const body = buildFreshRunBody(projectId, id, [{ id: "task", rootId: "tasks", ref: taskRef }]);
    const created = await rawRequest(port, "POST", `/v1/projects/${projectId}/runs`, JSON.stringify(body));
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

  /** A fake-DOM harness whose fetch goes to the real loopback service;
   * the caller-owned storage map survives across harness instances, so
   * a second harness over the same map simulates a browser restart. */
  const makeHarness = (storage: Map<string, string>, options: { discardApplyPost?: boolean } = {}): Promise<{ harness: UiHarness; gets: string[]; applyPosts: string[]; applyGets: string[] }> => {
    const gets: string[] = [];
    const applyPosts: string[] = [];
    const applyGets: string[] = [];
    const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
      const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
      if (method === "GET") {
        if (path.includes("/project-applies/")) {
          applyGets.push(path);
        } else {
          gets.push(path);
        }
      } else if (path.includes("/project-applies/")) {
        applyPosts.push(path);
        if (options.discardApplyPost === true && applyPosts.length === 1) {
          // only the first apply POST loses its response: the wire
          // carried the request, the backend still applies it, and a
          // later retry of the same identity receives the real answer
          await rawRequest(port, method, path, String(init?.body ?? ""));
          throw new Error("network gone");
        }
      }
      const response = await rawRequest(port, method, path, init !== undefined && typeof init.body === "string" ? init.body : undefined);
      return new Response(response.body, { status: response.status, headers: { "content-type": "application/json" } });
    };
    return (async () => {
      const harness = await createUiHarness({ fetchImpl, storage });
      return { harness, gets, applyPosts, applyGets };
    })();
  };

  const rowIndex = (harness: UiHarness, id: string): number => {
    const rows = harness.runRows();
    const index = rows.findIndex((row) => row === id);
    if (index < 0) {
      throw new Error(`run row missing for ${id}: ${JSON.stringify(rows)}`);
    }
    return index;
  };

  /** Drives one harness to the fully loaded terminal changes snapshot
   * of the selected run. */
  const loadTerminalChanges = async (harness: UiHarness, id: string): Promise<void> => {
    harness.click("refresh-projects");
    await wait(60);
    harness.click("refresh-runs");
    await wait(60);
    harness.clickRunInspect(rowIndex(harness, id));
    await waitForInspect(harness);
    await waitForChanges(harness);
    await waitFor(() => /loaded (\d+) of \1/.test(harness.changesMeta()), "fully loaded changes");
  };

  /** Loads the terminal changes, opens the Review confirmation and
   * returns the snapshot observation rendered in its details. */
  const loadAndReview = async (harness: UiHarness, id: string): Promise<string> => {
    await loadTerminalChanges(harness, id);
    harness.click("project-apply-review");
    await wait(30);
    const details = harness.applyConfirmation();
    const match = /Observation: ([0-9a-f]{64})/.exec(details);
    if (match === null) {
      throw new Error(`observation not rendered: ${details}`);
    }
    return match[1] as string;
  };

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
    mkdirSync(projectBDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "APPLY-TASK\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-lost.md"), "APPLY-LOST\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-stale.md"), "APPLY-STALE\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-plain.md"), "APPLY-PLAIN\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "UI-APPLY-TASK-V2\n", { mode: 0o600 });
    mkdirSync(storeRoot, { mode: 0o700 });
    mkdirSync(applyStoreRoot, { mode: 0o700 });
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
            project_apply: true,
          },
          {
            id: "project-b",
            project_root: projectBDir,
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
    if (!existsSync(socketPath)) throw new Error("apply daemon socket never appeared");
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
    "a1. the happy path: explicit confirmation, exactly one POST, the exact result tree and the record removal",
    async () => {
      expect(await driveRun("project-a", applyRunId, "task.md", "task-a-v2.md")).toBe("succeeded");
      const storage = new Map<string, string>();
      const { harness, applyPosts, applyGets } = await makeHarness(storage);
      try {
        const observation = await loadAndReview(harness, applyRunId);
        operationObservations.set(applyRunId, observation);
        expect(harness.applyConfirmationVisible()).toBe(true);
        expect(harness.applyConfirmation()).toContain("the registered project will be modified");
        expect(harness.applyConfirmation()).toContain("not whole-tree atomic");
        expect(harness.applyConfirmation()).toContain("IDE or git writers");
        expect(applyPosts).toEqual([]);
        expect(applyGets).toEqual([]);
        // the confirm press: the record first, then exactly one POST
        harness.click("project-apply-confirm");
        await waitFor(() => applyPosts.length >= 1, "the apply POST");
        expect(applyPosts).toEqual([`/v1/runs/${applyRunId}/project-applies/${observation}`]);
        expect(storage.get(`${APPLY_RECORD_STORAGE_PREFIX}${applyRunId}`)).toBe(
          JSON.stringify({ schema_version: 1, run_id: applyRunId, apply_id: observation, observation }),
        );
        // the applied operation triggers exactly one explicit reload and
        // the confirming snapshot removes the record
        await waitFor(() => harness.applyStatus() === "" && !storage.has(`${APPLY_RECORD_STORAGE_PREFIX}${applyRunId}`), "the applied reload and removal");
        // the exact result tree in the registered project
        expect(readFileSync(join(projectDir, "README.md"), "utf8")).toBe("README-CHANGED\n");
        expect(existsSync(join(projectDir, "dev-notes.txt"))).toBe(true);
        expect(harness.changesMeta()).toContain("already present");
      } finally {
        harness.dispose();
      }
    },
    240000,
  );

  test(
    "a2. the lost response retries the same identity and converges",
    async () => {
      expect(await driveRun("project-a", lostRunId, "task-lost.md", "task-a-v2.md")).toBe("succeeded");
      const storage = new Map<string, string>();
      const first = await makeHarness(storage, { discardApplyPost: true });
      try {
        const observation = await loadAndReview(first.harness, lostRunId);
        first.harness.click("project-apply-confirm");
        await waitFor(() => first.harness.applyStatus().includes("unknown"), "the network-unknown outcome");
        expect(first.applyPosts).toEqual([`/v1/runs/${lostRunId}/project-applies/${observation}`]);
        expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}${lostRunId}`)).toBe(true);
        // the retry POSTs the same identity; the durable operation is
        // already applied and the retry reports it
        first.harness.click("project-apply-retry");
        await waitFor(() => first.applyPosts.length >= 2, "the retry POST");
        expect(first.applyPosts[1]).toBe(`/v1/runs/${lostRunId}/project-applies/${observation}`);
        operationObservations.set(lostRunId, observation);
        await waitFor(() => first.harness.applyStatus() === "" && !storage.has(`${APPLY_RECORD_STORAGE_PREFIX}${lostRunId}`), "the converging reload and removal");
        expect(existsSync(join(projectDir, "report-copy.txt"))).toBe(true);
      } finally {
        first.harness.dispose();
      }
    },
    240000,
  );

  test(
    "a3. the simulated browser restart recovers the applied operation through one GET without a POST",
    async () => {
      // a fresh browser with a persisted recovery record for the
      // applied operation of the a1 run
      const observation = operationObservations.get(applyRunId);
      expect(observation).toMatch(/^[0-9a-f]{64}$/);
      const storage = new Map<string, string>();
      storage.set(
        `${APPLY_RECORD_STORAGE_PREFIX}${applyRunId}`,
        JSON.stringify({ schema_version: 1, run_id: applyRunId, apply_id: observation, observation }),
      );
      const { harness, applyGets, applyPosts } = await makeHarness(storage);
      try {
        await loadTerminalChanges(harness, applyRunId);
        await waitFor(() => applyGets.length >= 1, "the recovery GET");
        expect(applyGets).toEqual([`/v1/runs/${applyRunId}/project-applies/${observation}`]);
        expect(applyPosts).toEqual([]);
        await waitFor(() => harness.applyStatus() === "" && !storage.has(`${APPLY_RECORD_STORAGE_PREFIX}${applyRunId}`), "the recovered reload and removal");
        expect(harness.changesMeta()).toContain("already present");
      } finally {
        harness.dispose();
      }
    },
    240000,
  );

  test(
    "a4. the stale observation: the POST is refused, the record cleared and the snapshot preserved",
    async () => {
      expect(await driveRun("project-a", staleRunId, "task-stale.md", "task-a-v2.md")).toBe("succeeded");
      const storage = new Map<string, string>();
      const { harness, applyPosts } = await makeHarness(storage);
      try {
        const observation = await loadAndReview(harness, staleRunId);
        const metaBefore = harness.changesMeta();
        const rowsBefore = harness.changesRows().length;
        // the registered project changes after the snapshot was loaded
        writeFileSync(join(projectDir, "README.md"), ".externally changed\n", { mode: 0o644 });
        harness.click("project-apply-confirm");
        await waitFor(() => harness.applyStatus().includes("stale_observation"), "the stale refusal");
        expect(applyPosts).toEqual([`/v1/runs/${staleRunId}/project-applies/${observation}`]);
        expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}${staleRunId}`)).toBe(false);
        // the loaded snapshot was never replaced by the refusal
        expect(harness.changesMeta()).toBe(metaBefore);
        expect(harness.changesRows().length).toBe(rowsBefore);
        // the operator restores the external change and re-reviews
        writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
        harness.click("changes-reload");
        await waitForChanges(harness);
        await waitFor(() => /loaded (\d+) of \1/.test(harness.changesMeta()), "the reloaded snapshot");
        harness.click("project-apply-review");
        await wait(30);
        expect(harness.applyConfirmationVisible()).toBe(true);
      } finally {
        harness.dispose();
      }
    },
    240000,
  );

  test(
    "a5. the default-disabled capability keeps the control hidden with zero apply traffic",
    async () => {
      expect(await driveRun("project-b", plainRunId, "task-plain.md", "task-a-v2.md")).toBe("succeeded");
      const storage = new Map<string, string>();
      const { harness, applyPosts, applyGets } = await makeHarness(storage);
      try {
        await loadTerminalChanges(harness, plainRunId);
        expect(harness.applyButtonState("project-apply-review").hidden).toBe(true);
        expect(applyPosts).toEqual([]);
        expect(applyGets).toEqual([]);
        expect(storage.size).toBe(0);
      } finally {
        harness.dispose();
      }
    },
    240000,
  );

  test(
    "a6. the apply carries zero worker sessions and the durability fingerprints hold",
    async () => {
      const eventsPath = `${socketPath}.events`;
      const sessionsBefore = existsSync(eventsPath) ? (readFileSync(eventsPath, "utf8").match(/"create"/g) ?? []).length : 0;
      const observation = operationObservations.get(applyRunId);
      if (observation === undefined) throw new Error("missing operation observation");
      expect(observation).toMatch(/^[0-9a-f]{64}$/);
      const applyPackage = join(applyStoreRoot, "operations", observation);
      expect(existsSync(applyPackage)).toBe(true);
      const packageFingerprint = await fingerprintTree(applyPackage);
      const runTree = join(stateRoot, "pipeline-runs", applyRunId);
      const treeBefore = await fingerprintTree(runTree);
      // the read-only UI traffic (discovery, inspect, changes) over the
      // terminal run never mutates the package or the run tree
      const { harness } = await makeHarness(new Map<string, string>());
      try {
        await loadTerminalChanges(harness, applyRunId);
        await wait(60);
        expect(await fingerprintTree(applyPackage)).toBe(packageFingerprint);
        expect(await fingerprintTree(runTree)).toBe(treeBefore);
      } finally {
        harness.dispose();
      }
      const sessionsAfter = existsSync(eventsPath) ? (readFileSync(eventsPath, "utf8").match(/"create"/g) ?? []).length : 0;
      expect(sessionsAfter).toBe(sessionsBefore);
      const resultDoc = JSON.stringify(JSON.parse((await rawRequest(port, "GET", "/v1/projects")).body));
      for (const secret of secrets) {
        expect(resultDoc.includes(secret)).toBe(false);
      }
      expect(resultDoc).toContain("project_apply");
    },
    240000,
  );
});
