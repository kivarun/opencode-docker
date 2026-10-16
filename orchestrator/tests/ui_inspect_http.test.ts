/**
 * Permanent HTTP integration of the browser UI inspect single-flight.
 * Part A drives the real `app.js` wiring with a real `fetch` against a
 * real HTTP fixture whose inspect GET handlers can be deferred, so the
 * wire overlap is measured across genuine HTTP round-trips. Part B
 * drives the wiring against the real control-plane service (real CLI
 * subprocess inspects) and proves that every inspect GET is zero-write
 * over the run tree while takeover bursts coalesce into at most one
 * outstanding request.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createUiHarness,
  runDocument,
  runsPageDocument,
  type UiHarness,
} from "./ui_dom_harness.ts";
import { startControlPlaneHttpService } from "../src/control_plane/http_server.ts";

/** The real fetch, captured before any harness installs its own. */
const realFetch = fetch.bind(globalThis);

interface DeferredGet {
  release(): void;
}

function runDocumentFor(runId: string, revision: number): Record<string, unknown> {
  return runDocument(runId, revision, "running", { kind: "in_flight" });
}

describe("ui inspect http: the deferred fixture server", () => {
  let server: { stop(closeActiveConnections?: boolean): Promise<void>; port?: number } | null = null;
  let port = 0;
  const held = new Map<string, DeferredGet>();
  let revision = 5;
  let inspectCount = 0;
  let clientEvents: { kind: "start" | "finish"; seq: number }[] = [];
  let clientActive = 0;
  let clientMaxActive = 0;
  let clientStarts = 0;

  beforeAll(async () => {
    const serverInstance = Bun.serve({
      port: 0,
      idleTimeout: 120,
      async fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === "/v1/runs") {
          return new Response(JSON.stringify(runsPageDocument(["run-a"])), {
            headers: { "content-type": "application/json" },
          });
        }
        const inspect = /^\/v1\/runs\/([^/]+)$/.exec(url.pathname);
        if (inspect !== null && request.method === "GET") {
          inspectCount += 1;
          const call = inspectCount;
          const runId = inspect[1] as string;
          if (call === 1) {
            await new Promise<void>((resolve) => {
              held.set(runId, { release: () => {
                held.delete(runId);
                resolve();
              } });
            });
          }
          return new Response(JSON.stringify(runDocumentFor(runId, revision + call - 1)), {
            headers: { "content-type": "application/json" },
          });
        }
        if (request.method === "POST") {
          return new Response(JSON.stringify({
            schema_version: 1,
            kind: "command_result",
            run_id: "run-a",
            outcome: "succeeded",
            exit_code: 0,
          }), { headers: { "content-type": "application/json" } });
        }
        return new Response("not found", { status: 404 });
      },
    });
    server = serverInstance;
    port = serverInstance.port ?? 0;
  });

  afterAll(async () => {
    if (server !== null) {
      await server.stop(true);
    }
  });

  let inspectActive = 0;
  let inspectMaxActive = 0;
  let inspectStarts = 0;
  const INSPECT_PATH = /\/v1\/runs\/[^/?]+$/;

  /** The client-side wire: measures the unsettled-fetch span exactly.
   * The inspect GETs carry their own counters: since UI v4 the events
   * transport runs its own single-flight GETs beside the inspect
   * transport, so the inspect single-flight pins must not count them. */
  function harnessFetch(path: string, init?: RequestInit): Promise<Response> {
    clientEvents.push({ kind: "start", seq: clientEvents.length + 1 });
    clientActive += 1;
    clientStarts += 1;
    if (clientActive > clientMaxActive) clientMaxActive = clientActive;
    const isInspect = (init === undefined || typeof init.method !== "string" || init.method === "GET") && INSPECT_PATH.test(path);
    if (isInspect) {
      inspectActive += 1;
      inspectStarts += 1;
      if (inspectActive > inspectMaxActive) inspectMaxActive = inspectActive;
    }
    const seq = clientEvents.length;
    return realFetch(`http://127.0.0.1:${port}${path}`, init).finally(() => {
      clientActive -= 1;
      if (isInspect) inspectActive -= 1;
      clientEvents.push({ kind: "finish", seq });
    });
  }

  function startCount(): number {
    return inspectStarts;
  }

  /** Waits until every outstanding client fetch has settled. */
  async function waitForClientSettled(): Promise<void> {
    for (let index = 0; index < 200 && clientActive !== 0; index += 1) {
      await Bun.sleep(5);
    }
    expect(clientActive).toBe(0);
  }

  function resetClientProbe(): void {
    clientEvents = [];
    clientActive = 0;
    clientMaxActive = 0;
    clientStarts = 0;
  }

  test("h1. the manual takeover holds single flight across real HTTP round-trips", async () => {
    resetClientProbe();
    const harness = await createUiHarness({ fetchImpl: harnessFetch });
    harness.click("refresh-runs");
    await Bun.sleep(5);
    expect(harness.runRows()).toEqual(["run-a"]);
    // the first inspect enters the wire and the fixture holds its handler
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    expect(held.has("run-a")).toBe(true);
    expect(inspectActive).toBe(1);
    // the Refresh takeover aborts the held request: its fetch settles
    // (rejection is a settlement), the slot frees and exactly one
    // follow-up GET runs over real HTTP
    const startsBefore = startCount();
    harness.click("inspect-selected");
    await Bun.sleep(10);
    // the superseded handler is orphaned server-side (its socket is
    // closed by the abort): release the fixture gate so it can finish
    expect(held.has("run-a")).toBe(true);
    held.get("run-a")?.release();
    await waitForClientSettled();
    expect(inspectMaxActive).toBe(1);
    // exactly one follow-up GET runs after the aborted slot is released
    expect(startCount() - startsBefore).toBe(1);
    expect(harness.element("detail-revision").textContent).toBe("6");
    harness.dispose();
  });

  test("h2. the superseded response never renders over real HTTP", async () => {
    revision = 50;
    inspectCount = 0;
    resetClientProbe();
    const harness = await createUiHarness({ fetchImpl: harnessFetch });
    harness.click("refresh-runs");
    await Bun.sleep(5);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    expect(held.has("run-a")).toBe(true);
    expect(inspectActive).toBe(1);
    const startsBefore = startCount();
    harness.click("inspect-selected");
    // synchronously after the takeover nothing has settled yet and the
    // held first response has never rendered
    expect(harness.element("run-detail").hidden).toBe(true);
    expect(harness.element("detail-revision").textContent).toBe("");
    // the superseded handler is orphaned server-side: release its gate
    held.get("run-a")?.release();
    await waitForClientSettled();
    expect(inspectMaxActive).toBe(1);
    // exactly one follow-up GET runs after the aborted slot is released
    expect(startCount() - startsBefore).toBe(1);
    // the settled detail carries exactly the follow-up response, never
    // the superseded one
    expect(harness.element("detail-revision").textContent).toBe("51");
    harness.dispose();
  });
});

describe("ui inspect http: the real control-plane service", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-inspect-http-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: Awaited<ReturnType<typeof startControlPlaneHttpService>> | null = null;
  let port = 0;
  let daemonPid = 0;
  let runId = "";
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token"];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-INSPECT-HTTP-TASK\n", { mode: 0o600 });
    runId = `ui-inspect-http-${Math.random().toString(36).slice(2, 8)}`;
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
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_daemon.ts"), socketPath, "100000"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("inspect-http daemon socket never appeared");
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

  let clientActive = 0;
  let clientMaxActive = 0;
  let clientStarts = 0;
  let inspectActive = 0;
  let inspectMaxActive = 0;
  let inspectStarts = 0;
  const INSPECT_PATH = /\/v1\/runs\/[^/?]+$/;

  function probeFetch(path: string, init?: RequestInit): Promise<Response> {
    clientActive += 1;
    clientStarts += 1;
    if (clientActive > clientMaxActive) clientMaxActive = clientActive;
    const isInspect = (init === undefined || typeof init.method !== "string" || init.method === "GET") && INSPECT_PATH.test(path);
    if (isInspect) {
      inspectActive += 1;
      inspectStarts += 1;
      if (inspectActive > inspectMaxActive) inspectMaxActive = inspectActive;
    }
    return realFetch(`http://127.0.0.1:${port}${path}`, init).finally(() => {
      clientActive -= 1;
      if (isInspect) inspectActive -= 1;
    });
  }

  function startCount(): number {
    return inspectStarts;
  }

  /** Bounded wait for a client-side condition (100 x 200 ms). */
  async function waitFor(check: () => boolean): Promise<void> {
    for (let index = 0; index < 100; index += 1) {
      if (check()) return;
      await Bun.sleep(200);
    }
    throw new Error("condition not reached within the wait budget");
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

  test("h3. real CLI-backed inspects are zero-write and single-flight under a takeover burst", async () => {
    // the run is created through the service; the CLI child reaches the
    // plan-ready boundary and exits, so the run tree is quiescent below
    const created = await realFetch(`http://127.0.0.1:${port}/v1/projects/project-a/runs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        run_id: runId,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      }),
    });
    expect(created.status).toBe(200);

    const harness = await createUiHarness({ fetchImpl: probeFetch });
    harness.click("refresh-runs");
    await waitFor(() => harness.runRows().length === 1);
    expect(harness.runRows()).toEqual([runId]);

    const runTree = join(stateRoot, "pipeline-runs", runId);
    // the first real inspect GET: zero-write over the run tree; the
    // applied detail (run-detail unhidden with a revision) is the real
    // settle signal, unlike the synchronously written run id line
    const before = fingerprintTree(runTree);
    harness.clickRunInspect(0);
    await waitFor(() => harness.element("run-detail").hidden === false && harness.element("detail-revision").textContent !== "");
    expect(inspectMaxActive).toBe(1);
    expect(fingerprintTree(runTree)).toBe(before);

    // a takeover burst while a real GET is on the wire: with the burst
    // clicks issued in one synchronous turn, the first click supersedes
    // the unsettled request and the other three coalesce into it, so
    // exactly one follow-up GET runs after the wire slot is released
    const startsBefore = startCount();
    harness.click("inspect-selected");
    harness.click("inspect-selected");
    harness.click("inspect-selected");
    harness.click("inspect-selected");
    await waitFor(() => inspectActive === 0);
    expect(inspectMaxActive).toBe(1);
    // exactly one superseded request and one coalesced follow-up ran
    expect(startCount() - startsBefore).toBe(2);
    expect(harness.element("detail-revision").textContent !== "").toBe(true);
    expect(harness.detailStatus()).toBe("running");
    expect(fingerprintTree(runTree)).toBe(before);
    harness.dispose();
  }, 60000);
});
