/**
 * Permanent HTTP/lifecycle integration of the browser UI v3 bounded
 * read-only live observation. A fixture daemon holds the stage worker
 * in an honest long-running execution, a mutating continue-stage POST
 * is lost through a client abort while the orchestration continues, and
 * the observation then runs the exact machine-driven GET-only loop the
 * browser runs: sequential non-overlapping inspects of the one selected
 * run, each zero-write over the run tree, until the first non-in_flight
 * boundary stops the scheduler. No mutating request is ever issued by
 * the observation.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createLiveObservationScheduler,
  createSchedulerTimerAdapter,
  parseApiDocument,
  type UiObservationEffect,
  type UiRunModel,
} from "../ui/app.js";
import { startControlPlaneHttpService } from "../src/control_plane/http_server.ts";

const CANARY = "uilivehostpath";
const POLL_DELAY_MS = 2000;

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
    return req;
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
async function uiInspect(port: number, runId: string): Promise<UiRunModel> {
  const response = await rawRequest(port, "GET", `/v1/runs/${runId}`);
  expect(response.status).toBe(200);
  const model = parseApiDocument(JSON.parse(response.body));
  expect(model.kind).toBe("run");
  return model as UiRunModel;
}

function snapshotOf(model: UiRunModel) {
  return { status: model.status, boundaryKind: model.boundary.kind as string, revision: model.revision };
}

describe("control-plane UI live observation: bounded read-only polling", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-live-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: Awaited<ReturnType<typeof startControlPlaneHttpService>>;
  let port = 0;
  let daemonPid = 0;
  const runId = `ui-live-${Math.random().toString(36).slice(2, 8)}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-LIVE-TASK\n", { mode: 0o600 });
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
    // the stage worker is held for five seconds: an honest in_flight window
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_daemon.ts"), socketPath, "5000"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("live-observation daemon socket never appeared");
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
    "iv1. the observation loop is GET-only, sequential and stops at the next boundary",
    async () => {
      // fresh run → plan_ready
      const created = await rawRequest(port, "POST", "/v1/projects/project-a/runs", JSON.stringify({
        run_id: runId,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      }));
      expect(created.status).toBe(200);
      expect((parseApiDocument(JSON.parse(created.body)) as { outcome: string }).outcome).toBe("plan_ready");

      // resume-plan waits through the first five-second stage execution
      const resumed = await rawRequest(port, "POST", `/v1/runs/${runId}/resume-plan`, JSON.stringify({ stage_id: "stage-1", initial_budget: 1 }));
      expect(resumed.status).toBe(200);
      expect((parseApiDocument(JSON.parse(resumed.body)) as { outcome: string }).outcome).toBe("waiting");

      // the machine is seeded from a manual inspect of the waiting boundary
      const scheduler = createLiveObservationScheduler();
      const timerAdapter = createSchedulerTimerAdapter(
        {
          setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
          clearTimeout: (handle) => clearTimeout(handle as number),
        },
        () => undefined,
      );
      const apply = (effect: UiObservationEffect): UiObservationEffect => {
        timerAdapter.apply(effect);
        return effect;
      };
      apply(scheduler.runSelected(runId));
      const seed = await uiInspect(port, runId);
      expect(seed.boundary).toMatchObject({ kind: "waiting", waitIndex: 1 });
      apply(scheduler.inspectStarted(runId));
      expect(apply(scheduler.inspectSucceeded(runId, snapshotOf(seed)))).toEqual({ effect: "render_state" });
      expect(timerAdapter.hasPendingTimer()).toBe(false);

      // the continue-stage mutation starts; the observation pauses
      const mutationController = new AbortController();
      const pendingPost = fetch(`http://127.0.0.1:${port}/v1/runs/${runId}/continue-stage`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ wait_index: 1, additional_iterations: 1 }),
        signal: mutationController.signal,
      });
      apply(scheduler.mutationStarted());
      expect(timerAdapter.hasPendingTimer()).toBe(false);
      // the successor execution starts inside the lost POST; the response
      // is lost while the orchestration continues server-side
      const inFlightDeadline = Date.now() + 20000;
      for (;;) {
        const model = await uiInspect(port, runId);
        if (model.boundary.kind === "in_flight") {
          break;
        }
        if (Date.now() > inFlightDeadline) {
          throw new Error(`the worker never entered in_flight: ${JSON.stringify(model.boundary)}`);
        }
        await Bun.sleep(300);
      }
      mutationController.abort();
      await pendingPost.catch(() => undefined);

      // exactly one recovery inspect of the lost mutation observes in_flight
      const recovered = await uiInspect(port, runId);
      expect(recovered.status).toBe("running");
      expect(recovered.boundary.kind).toBe("in_flight");
      apply(scheduler.inspectStarted(runId));
      expect(apply(scheduler.inspectSucceeded(runId, snapshotOf(recovered)))).toEqual({ effect: "render_state" });
      expect(apply(scheduler.mutationCompleted())).toEqual({ effect: "schedule_timer", delayMs: POLL_DELAY_MS });
      expect(timerAdapter.hasPendingTimer()).toBe(true);

      // the machine-driven GET-only observation loop
      const runTree = join(stateRoot, "pipeline-runs", runId);
      let inFlightObservations = 0;
      let previousRevision = recovered.revision;
      const observationDeadline = Date.now() + 60000;
      let lastEffect: UiObservationEffect = { effect: "no_op" };
      for (;;) {
        if (Date.now() > observationDeadline) {
          throw new Error("the observation loop never stopped at a new boundary");
        }
        // the fixed period runs from the previous completion; the adapter
        // holds exactly one pending timer until it fires
        expect(timerAdapter.hasPendingTimer()).toBe(true);
        await Bun.sleep(POLL_DELAY_MS);
        expect(timerAdapter.hasPendingTimer()).toBe(false);
        const before = await fingerprintTree(runTree);
        const model = await uiInspect(port, runId);
        expect(await fingerprintTree(runTree)).toBe(before);
        expect(model.revision).toBeGreaterThanOrEqual(previousRevision);
        previousRevision = model.revision;
        for (const secret of secrets) {
          expect(JSON.stringify(model).includes(secret)).toBe(false);
        }
        apply(scheduler.inspectStarted(runId));
        lastEffect = apply(scheduler.inspectSucceeded(runId, snapshotOf(model)));
        if (model.boundary.kind === "in_flight") {
          inFlightObservations += 1;
          expect(lastEffect.effect).toBe("schedule_timer");
        } else {
          break;
        }
      }
      // the first non-in_flight boundary stopped the scheduler
      expect(lastEffect.effect).not.toBe("schedule_timer");
      expect(timerAdapter.hasPendingTimer()).toBe(false);
      expect(inFlightObservations).toBeGreaterThanOrEqual(2);
      const finalModel = await uiInspect(port, runId);
      expect(finalModel.boundary).toMatchObject({ kind: "waiting", waitIndex: 2 });
      expect(finalModel.status).toBe("waiting");
      expect(scheduler.presentationState()).toBe("off");
      scheduler.disposed();
      expect(timerAdapter.hasPendingTimer()).toBe(false);
    },
    120000,
  );
});
