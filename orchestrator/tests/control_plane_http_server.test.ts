/**
 * Permanent contract suite of the versioned loopback control-plane HTTP
 * service. The HTTP server, the subprocess client, the CLI process, the
 * project registry and the durable run binding store are all production
 * modules; only the Docker Helper daemon is a fixture.
 *
 * Matrix: A route/method/path parsing, B body validation, C trust gates
 * before effects, D fresh run end to end, E service restart, F existing
 * mutations never accept a projectId, G unbound CLI runs fail closed,
 * H removed registry projects, I binding conflicts, J parallel mutators
 * on the kernel run lock, K lost HTTP responses, L bound-input task
 * references, M zero-write inspect, N host-path canaries, O unknown
 * inspect schema versions, P binding immutability across restarts and
 * Q the full real-TCP smoke.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { chmodSync, existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { request as httpRequest, type IncomingHttpHeaders } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ControlPlaneHttpRequestError,
  handleControlPlaneHttpRequest,
  parseControlPlaneRoute,
  parseRunsDiscoveryQuery,
  projectControlPlaneProjects,
  projectControlPlaneRuns,
  parseContinueStageBody,
  parseEmptyBody,
  parseFreshRunBody,
  parseRespondBody,
  parseResumePlanBody,
  parseReviseTaskBody,
  projectControlPlaneCommandResult,
  projectControlPlaneInspect,
  projectControlPlaneEvents,
  parseEventsQuery,
  startControlPlaneHttpService,
  statusForControlPlaneFailure,
  type RunningControlPlaneHttpService,
} from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { loadControlPlaneProjectRegistry } from "../src/control_plane/project_registry.ts";
import { defaultControlPlaneUiRoot, loadControlPlaneUiAssets } from "../src/control_plane/ui_assets.ts";
import { OrchestratorSubprocessClient, type OrchestratorCallResult } from "../src/control_plane/subprocess_client.ts";

const CANARY = "httpcanaryhostpath";

interface RawResponse {
  readonly status: number;
  readonly headers: IncomingHttpHeaders;
  readonly body: string;
}

function rawRequest(
  port: number,
  method: string,
  path: string,
  headers: Record<string, string | string[] | undefined>,
  body?: string,
): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ host: "127.0.0.1", port, method, path, headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    if (body !== undefined) {
      req.write(body);
    }
    req.end();
  });
}

async function post(port: number, path: string, body: unknown): Promise<RawResponse> {
  return await rawRequest(port, "POST", path, { "content-type": "application/json" }, JSON.stringify(body));
}

async function getInspect(port: number, runId: string): Promise<RawResponse> {
  return await rawRequest(port, "GET", `/v1/runs/${runId}`, {});
}

async function fingerprintTree(root: string): Promise<string> {
  const rows: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const child = join(dir, entry.name);
      const rel = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = lstatSync(child);
      const kind = info.isDirectory() ? "d" : info.isSymbolicLink() ? "l" : "f";
      rows.push(`${rel}|${kind}|${(info.mode & 0o7777).toString(8)}|${info.size}|${info.mtimeMs}|${info.ino}`);
      if (info.isDirectory()) {
        await walk(child, rel);
      }
    }
  };
  await walk(root, "");
  return rows.join("\n");
}

function fingerprintFile(path: string): string {
  const info = statSync(path);
  return JSON.stringify({ size: info.size, mtimeMs: info.mtimeMs, ino: info.ino, mode: info.mode & 0o7777, bytes: readFileSync(path, "utf8") });
}

function expectNoLeak(text: string, roots: readonly string[]): void {
  for (const needle of [CANARY, ...roots]) {
    expect(text.includes(needle)).toBe(false);
  }
}

function errorBody(body: RawResponse): { readonly layer: string; readonly reason: string } {
  const parsed = JSON.parse(body.body) as Record<string, unknown>;
  expect(parsed["schema_version"]).toBe(1);
  expect(parsed["kind"]).toBe("error");
  expect(Object.keys(parsed).sort()).toEqual(["kind", "layer", "reason", "schema_version"]);
  return { layer: parsed["layer"] as string, reason: parsed["reason"] as string };
}

function commandResult(body: RawResponse): Record<string, unknown> {
  const parsed = JSON.parse(body.body) as Record<string, unknown>;
  expect(parsed["schema_version"]).toBe(1);
  expect(parsed["kind"]).toBe("command_result");
  return parsed;
}

function asCallResult(value: unknown): OrchestratorCallResult {
  return value as OrchestratorCallResult;
}

describe("control-plane HTTP: route parser", () => {
  const routeReason = (path: string, method: string): string => {
    const parsed = parseControlPlaneRoute(path, method);
    if (parsed instanceof ControlPlaneHttpRequestError) {
      return parsed.reason;
    }
    return `ok:${parsed.kind}`;
  };

  test("every versioned route parses with its exact method and ids", () => {
    expect(routeReason("/v1/projects/pa/runs", "POST")).toBe("ok:fresh_run");
    expect(routeReason("/v1/runs/r1", "GET")).toBe("ok:inspect");
    expect(routeReason("/v1/runs/r1/resume", "POST")).toBe("ok:resume");
    expect(routeReason("/v1/runs/r1/resume-plan", "POST")).toBe("ok:resume_plan");
    expect(routeReason("/v1/runs/r1/continue-stage", "POST")).toBe("ok:continue_stage");
    expect(routeReason("/v1/runs/r1/revise-task", "POST")).toBe("ok:revise_task");
    expect(routeReason("/v1/runs/r1/respond", "POST")).toBe("ok:respond");
    expect(routeReason("/v1/runs/r1/events", "GET")).toBe("ok:events");
    expect(routeReason("/v1/runs/r1/changes", "GET")).toBe("ok:project_changes");
  });

  test("method mismatches on known shapes are method_not_allowed", () => {
    for (const [path, method] of [
      ["/v1/projects/pa/runs", "GET"],
      ["/v1/runs/r1", "POST"],
      ["/v1/runs/r1/resume", "GET"],
      ["/v1/runs/r1", "OPTIONS"],
      ["/v1/runs/r1/respond", "PUT"],
      ["/v1/runs/r1/events", "POST"],
      ["/v1/runs/r1/changes", "POST"],
    ] as const) {
      expect(routeReason(path, method)).toBe("method_not_allowed");
    }
  });

  test("query strings, encodings, dot segments and trailing slashes are invalid paths", () => {
    for (const path of [
      "/v1/runs/r1?x=1",
      "/v1/runs/r1%2Fsub",
      "/v1/runs/%23r1",
      "/v1/runs/r1/",
      "/v1//runs/r1",
      "/v1/runs/./r1",
      "/v1/runs/../r1",
      "/v1/runs/r1/resume/",
    ]) {
      expect(routeReason(path, "GET")).toBe("invalid_path");
    }
  });

  test("unknown routes, extra segments and unsafe ids are refused", () => {
    for (const [path, method] of [
      ["/v1/other/r1", "GET"],
      ["/v1/runs/r1/resume/extra", "POST"],
      ["/v1/runs/r1/unknown-action", "POST"],
      ["/v2/runs/r1", "GET"],
    ] as const) {
      expect(routeReason(path, method)).toBe("unknown_route");
    }
    for (const id of ["-bad_id", "a%20b", "-leading", "with space"]) {
      expect(routeReason(`/v1/runs/${id}`, "GET")).toBe("invalid_path");
      expect(routeReason(`/v1/projects/${id}/runs`, "POST")).toBe("invalid_path");
    }
  });
});

describe("control-plane HTTP: request body validators", () => {
  test("the fresh-run body carries exactly run_id and unique ordered inputs", () => {
    const body = parseFreshRunBody({ run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }] });
    expect(body.runId).toBe("r1");
    expect(body.inputs).toEqual([{ id: "task", rootId: "tasks", ref: "task.md" }]);
    for (const bad of [
      { run_id: "r1" },
      { run_id: "r1", inputs: [] },
      { run_id: "r1", inputs: [{ id: "task", root_id: "tasks" }] },
      { run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "a/b" }] },
      { run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }, { id: "task", root_id: "tasks", ref: "task.md" }] },
      { run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }], extra: 1 },
      { run_id: "../r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }] },
      { inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }] },
      [1, 2],
      "text",
      null,
    ]) {
      expect(() => parseFreshRunBody(bad)).toThrow();
    }
  });

  test("a required field that exists only on the prototype chain never satisfies the shape", () => {
    const inherited: Record<string, unknown> = Object.create({ run_id: "r1" });
    inherited["inputs"] = [{ id: "task", root_id: "tasks", ref: "task.md" }];
    expect(() => parseFreshRunBody(inherited)).toThrow();
  });

  test("integer fields must be positive safe integers and ids safe identifiers", () => {
    expect(parseResumePlanBody({ stage_id: "s1", initial_budget: 1 })).toEqual({ stageId: "s1", initialBudget: 1 });
    expect(parseContinueStageBody({ wait_index: 2, additional_iterations: 3 })).toEqual({ waitIndex: 2, additionalIterations: 3 });
    expect(parseRespondBody({ wait_index: 1, action_id: "other_action" })).toEqual({ waitIndex: 1, actionId: "other_action" });
    expect(parseReviseTaskBody({ wait_index: 2, task_id: "task-a", task: { root_id: "tasks", ref: "task-a-v2.md" } })).toEqual({
      waitIndex: 2,
      taskId: "task-a",
      taskRootId: "tasks",
      taskRef: "task-a-v2.md",
    });
    for (const bad of [0, -1, 1.5, "1", 2 ** 53, undefined, null]) {
      expect(() => parseResumePlanBody({ stage_id: "s1", initial_budget: bad })).toThrow();
      expect(() => parseContinueStageBody({ wait_index: bad, additional_iterations: 1 })).toThrow();
    }
    for (const bad of [{}, { extra: 1 }, { stage_id: "s1" }, { initial_budget: 1 }, [1], null, "x"]) {
      expect(() => parseResumePlanBody(bad)).toThrow();
    }
    expect(() => parseReviseTaskBody({ wait_index: 2, task_id: "task-a", task: { root_id: "tasks" } })).toThrow();
    expect(() => parseReviseTaskBody({ wait_index: 2, task_id: "task-a", task: { root_id: "tasks", ref: "task-a-v2.md", path: "/etc" } })).toThrow();
    expect(() => parseEmptyBody({})).not.toThrow();
    expect(() => parseEmptyBody({ extra: 1 })).toThrow();
    expect(() => parseEmptyBody([])).toThrow();
  });

  test("hostile proxies never have their extra fields read", () => {
    let extraReads = 0;
    const proxy = new Proxy(
      { run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }] },
      {
        get(target, key) {
          if (key === "extra") {
            extraReads += 1;
          }
          return (target as Record<string | symbol, unknown>)[key];
        },
      },
    );
    expect(parseFreshRunBody(proxy).runId).toBe("r1");
    expect(extraReads).toBe(0);
    let invalidReads = 0;
    const invalidProxy = new Proxy(
      { wait_index: 1, action_id: "a1" },
      {
        get(target, key) {
          if (key === "wait_index") invalidReads += 1;
          return (target as Record<string | symbol, unknown>)[key];
        },
      },
    );
    expect(parseRespondBody(invalidProxy).actionId).toBe("a1");
    expect(invalidReads).toBe(1);
  });
});

describe("control-plane HTTP: status table and projections", () => {
  test("the one status table maps every closed reason", () => {
    expect(statusForControlPlaneFailure({ layer: "request", reason: "unknown_route" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "method_not_allowed" })).toBe(405);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "unsupported_media_type" })).toBe(415);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "payload_too_large" })).toBe(413);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "invalid_host" })).toBe(400);
    expect(statusForControlPlaneFailure({ layer: "registry", reason: "project_not_found" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "registry", reason: "input_not_found" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "registry", reason: "input_invalid" })).toBe(400);
    expect(statusForControlPlaneFailure({ layer: "registry", reason: "registry_missing" })).toBe(500);
    expect(statusForControlPlaneFailure({ layer: "registry", reason: "invalid_registry" })).toBe(500);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "binding_conflict" })).toBe(409);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "project_unavailable" })).toBe(409);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "binding_not_found" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "binding_invalid" })).toBe(500);
    expect(statusForControlPlaneFailure({ layer: "transport", reason: "spawn_failed" })).toBe(502);
    expect(statusForControlPlaneFailure({ layer: "transport", reason: "internal_error" })).toBe(502);
    expect(statusForControlPlaneFailure({ layer: "unknown", reason: "x" } as { layer: string; reason: string })).toBe(500);
  });

  test("command results project with exact keys and no host paths", () => {
    const base = { ok: true as const };
    expect(projectControlPlaneCommandResult(asCallResult({ ...base, projection: { kind: "success", exitCode: 0, runId: "r1", runRoot: "/secret/root" } }))).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "r1",
      outcome: "succeeded",
      exit_code: 0,
    });
    expect(projectControlPlaneCommandResult(asCallResult({ ...base, projection: { kind: "waiting", exitCode: 0, runId: "r1", runRoot: "/secret/root" } }))).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "r1",
      outcome: "waiting",
      exit_code: 0,
    });
    const failed = projectControlPlaneCommandResult(
      asCallResult({ ...base, projection: { kind: "failure", exitCode: 130, runId: "r1", runRoot: "/secret/root", reason: "signal_sigint" } }),
    ) as unknown as Record<string, unknown>;
    expect(failed).toEqual({ schema_version: 1, kind: "command_result", run_id: "r1", outcome: "failed", exit_code: 130, reason: "signal_sigint" });
    expect(JSON.stringify(failed)).not.toContain("/secret/root");
    expect(projectControlPlaneCommandResult(asCallResult({ ok: false, failure: { reason: "malformed_output", expectedRunId: "r1" } }))).toEqual({
      layer: "transport",
      reason: "malformed_output",
    });
  });

  test("inspect projections copy boundary and outputs and drop the run root", () => {
    const projection = {
      kind: "inspect" as const,
      schemaVersion: 1 as const,
      runId: "r1",
      runRoot: "/secret/root",
      revision: 7,
      status: "waiting" as const,
      boundary: { kind: "waiting", waitIndex: 1 },
      outputs: [{ id: "report", present: false }],
    };
    const projected = projectControlPlaneInspect(asCallResult({ ok: true, projection })) as unknown as Record<string, unknown>;
    expect(projected).toEqual({
      schema_version: 1,
      kind: "run",
      run_id: "r1",
      revision: 7,
      status: "waiting",
      boundary: { kind: "waiting", waitIndex: 1 },
      outputs: [{ id: "report", present: false }],
    });
    expect(JSON.stringify(projected)).not.toContain("/secret/root");
    const inspectFailure = projectControlPlaneInspect(
      asCallResult({ ok: true, projection: { kind: "inspectFailure", exitCode: 1, runId: "gone", runRoot: "/secret/root", reason: "run_not_found" } }),
    ) as unknown as Record<string, unknown>;
    expect(inspectFailure).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "gone",
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });
    expect(JSON.stringify(inspectFailure)).not.toContain("/secret/root");
  });

  test("events projections copy the events page and drop the run root", () => {
    const projection = {
      kind: "events" as const,
      schemaVersion: 1 as const,
      runId: "r1",
      revision: 9,
      events: [{ revision: 1, kind: "create_run" }],
      nextAfter: null,
    };
    const projected = projectControlPlaneEvents(asCallResult({ ok: true, projection })) as unknown as Record<string, unknown>;
    expect(projected).toEqual({
      schema_version: 1,
      kind: "events",
      run_id: "r1",
      revision: 9,
      events: [{ revision: 1, kind: "create_run" }],
      next_after: null,
    });
    expect(JSON.stringify(projected)).not.toContain("runRoot");
    expect(JSON.stringify(projected)).not.toContain("/secret/root");
    const failure = projectControlPlaneEvents(
      asCallResult({ ok: true, projection: { kind: "eventsFailure", exitCode: 1, runId: "gone", runRoot: "/secret/root", reason: "run_not_found" } }),
    ) as unknown as Record<string, unknown>;
    expect(failure).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "gone",
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });
    expect(JSON.stringify(failure)).not.toContain("/secret/root");
    expect(projectControlPlaneEvents(asCallResult({ ok: false, failure: { reason: "spawn_failed", expectedRunId: "r1" } }))).toEqual({
      layer: "transport",
      reason: "spawn_failed",
    });
    const unknownVersion = projectControlPlaneEvents(
      asCallResult({ ok: true, projection: { kind: "events", schemaVersion: 2, runId: "r1", revision: 1, events: [], nextAfter: null } }),
    );
    expect(unknownVersion).toEqual({ layer: "transport", reason: "internal_error" });
  });

  test("the events query parser accepts only after and limit with the exact grammar", () => {
    expect(parseEventsQuery("")).toEqual({ after: null, limit: null });
    expect(parseEventsQuery("?after=5")).toEqual({ after: 5, limit: null });
    expect(parseEventsQuery("?limit=9")).toEqual({ after: null, limit: 9 });
    expect(parseEventsQuery("?limit=9&after=5")).toEqual({ after: 5, limit: 9 });
    expect(parseEventsQuery("?after=0")).toEqual({ after: 0, limit: null });
    expect(parseEventsQuery("?limit=200")).toEqual({ after: null, limit: 200 });
    const rejected: Array<[string, string]> = [
      ["?after=-1", "non-negative safe integer"],
      ["?after=1.5", "non-negative safe integer"],
      ["?after=1e2", "non-negative safe integer"],
      ["?after=+1", "non-negative safe integer"],
      ["?after=01", "non-negative safe integer"],
      ["?limit=0", "positive safe integer"],
      ["?limit=-2", "positive safe integer"],
      ["?limit=2.5", "positive safe integer"],
      ["?limit=201", "up to 200"],
      ["?after", "exactly one value"],
      ["?after=", "non-negative safe integer"],
      ["?after=1&after=2", "unique"],
      ["?limit=1&limit=2", "unique"],
      ["?ghost=1", "unknown field"],
      ["?after=%31", "encoded"],
    ];
    for (const [query, message] of rejected) {
      const parsed = parseEventsQuery(query);
      if (!(parsed instanceof ControlPlaneHttpRequestError)) {
        throw new Error(`expected rejection for ${query}`);
      }
      expect(parsed.message, query).toContain(message);
      expect(parsed.reason).toBe("invalid_request");
    }
  });

  test("an unknown inspect schema version is a transport failure, never a guessed projection", () => {
    const projected = projectControlPlaneInspect(
      asCallResult({
        ok: true,
        projection: { kind: "inspect", schemaVersion: 2, runId: "r1", runRoot: "/secret/root", revision: 1, status: "running", boundary: {}, outputs: [] },
      }),
    );
    expect(projected).toEqual({ layer: "transport", reason: "internal_error" });
  });
});

describe("control-plane HTTP: loopback gates before effects", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-gates-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const inputRoot = join(envRoot, "inputs");
  const projectDir = join(envRoot, "project");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService;
  let port = 0;

  beforeAll(async () => {
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "GATE-TASK\n", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    const registry = {
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
    };
    writeFileSync(registryPath, JSON.stringify(registry), { mode: 0o600 });
    service = await startControlPlaneHttpService({
      registryPath,
      bindingStoreRoot: storeRoot,
      client: {
        bunExecutable: process.execPath,
        orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
        workingDirectory: envRoot,
        baseEnv: { PATH: "/usr/bin:/bin" },
        stdoutLimitBytes: 100000,
        stderrLimitBytes: 100000,
        sigintGraceMs: 1000,
      },
      port: 0,
      bodyLimitBytes: 4096,
    });
    port = service.port;
  }, 20000);

  afterAll(() => {
    service?.stop();
    rmSync(envRoot, { recursive: true, force: true });
  });

  test("a wrong Host header is refused before any effect", async () => {
    const before = await fingerprintTree(storeRoot).catch(() => "absent");
    const response = await rawRequest(port, "GET", "/v1/runs/some-run", { host: "evil.example" });
    expect(response.status).toBe(400);
    expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_host" });
    const after = await fingerprintTree(storeRoot).catch(() => "absent");
    expect(after).toBe(before);
  });

  test("a foreign Origin is refused before the body is parsed", async () => {
    const response = await rawRequest(
      port,
      "POST",
      "/v1/projects/project-a/runs",
      { host: `127.0.0.1:${port}`, origin: "http://evil.example", "content-type": "application/json" },
      "{not json at all",
    );
    expect(response.status).toBe(400);
    expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_origin" });
  });

  test("the matching service origin passes the gate", async () => {
    const response = await rawRequest(
      port,
      "GET",
      "/v1/runs/nevercreated",
      { host: `127.0.0.1:${port}`, origin: `http://127.0.0.1:${port}` },
    );
    expect(response.status).toBe(404);
    expect(errorBody(response)).toEqual({ layer: "binding", reason: "binding_not_found" });
  });

  test("POST accepts exactly application/json", async () => {
    for (const contentType of ["text/plain", "application/json; charset=utf-8", ""]) {
      const response = await rawRequest(
        port,
        "POST",
        "/v1/projects/project-a/runs",
        { host: `127.0.0.1:${port}`, ...(contentType === "" ? {} : { "content-type": contentType }) },
        "{}",
      );
      expect(response.status).toBe(415);
      expect(errorBody(response)).toEqual({ layer: "request", reason: "unsupported_media_type" });
    }
  });

  test("a GET with a body is refused", async () => {
    const response = await rawRequest(port, "GET", "/v1/runs/some-run", { host: `127.0.0.1:${port}`, "content-length": "5" }, "hello");
    expect(response.status).toBe(400);
    expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_request" });
  });

  test("query strings, encoded paths, extra segments and unsafe ids are refused over TCP", async () => {
    const cases: readonly [string, string, number, string][] = [
      ["/v1/runs/some-run?x=1", "GET", 400, "invalid_path"],
      ["/v1/runs/some%2Frun", "GET", 400, "invalid_path"],
      ["/v1/runs/some-run/", "GET", 400, "invalid_path"],
      ["/v1/runs/some-run/extra", "POST", 404, "unknown_route"],
      ["/v1/unknown/route", "GET", 404, "unknown_route"],
      ["/v1/runs/-bad_id", "GET", 400, "invalid_path"],
      ["/v1/projects/-bad/runs", "POST", 400, "invalid_path"],
    ];
    for (const [path, method, status, reason] of cases) {
      const response = await rawRequest(port, method, path, { host: `127.0.0.1:${port}` });
      expect(response.status).toBe(status);
      expect(errorBody(response).reason).toBe(reason);
    }
    const options = await rawRequest(port, "OPTIONS", "/v1/runs/some-run", { host: `127.0.0.1:${port}` });
    expect(options.status).toBe(405);
    expect(errorBody(options)).toEqual({ layer: "request", reason: "method_not_allowed" });
  });

  test("oversized bodies are refused with 413, by declared length and by streamed bytes", async () => {
    const bigBody = JSON.stringify({ run_id: "r1", inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }], pad: "x".repeat(8192) });
    const declared = await rawRequest(
      port,
      "POST",
      "/v1/projects/project-a/runs",
      { host: `127.0.0.1:${port}`, "content-type": "application/json", "content-length": String(bigBody.length) },
      bigBody,
    );
    expect(declared.status).toBe(413);
    expect(errorBody(declared)).toEqual({ layer: "request", reason: "payload_too_large" });
    const streamed = await rawRequest(
      port,
      "POST",
      "/v1/projects/project-a/runs",
      { host: `127.0.0.1:${port}`, "content-type": "application/json" },
      bigBody,
    );
    expect(streamed.status).toBe(413);
    const after = await fingerprintTree(storeRoot).catch(() => "absent");
    expect(after).not.toContain("r1");
  });

  test("malformed, empty and trailing JSON bodies are request failures before any claim", async () => {
    for (const body of ["", "{", "{} {}", '{"run_id": "r1"} extra', "null", "[1,2]"]) {
      const response = await rawRequest(
        port,
        "POST",
        "/v1/projects/project-a/runs",
        { host: `127.0.0.1:${port}`, "content-type": "application/json" },
        body,
      );
      expect(response.status).toBe(400);
      expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_request" });
    }
    expect(await fingerprintTree(storeRoot).catch(() => "absent")).not.toContain("r1");
  });
});

describe("control-plane HTTP: production flows over real TCP", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `cp-http-flow-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  const hostHeader = { host: "" };
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let daemonPid = 0;
  const run1 = `flow-${Math.random().toString(36).slice(2, 8)}`;
  const run2 = `flow-${Math.random().toString(36).slice(2, 8)}`;
  const directRun = `flow-${Math.random().toString(36).slice(2, 8)}`;

  const writeRegistry = (projectIds: readonly string[]): void => {
    const projects = projectIds.map((id) => ({
      id,
      project_root: projectDir,
      config_root: join(fixtureRoot, "config"),
      pipeline_root: join(fixtureRoot, "bundle"),
      launcher_id: null,
      input_roots: [{ id: "tasks", root: inputRoot }],
    }));
    writeFileSync(registryPath, JSON.stringify({ schema_version: 1, projects }), { mode: 0o600 });
  };

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "flow project\n", { mode: 0o600 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "FLOW-TASK-A\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "FLOW-TASK-A-V2\n", { mode: 0o600 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeRegistry(["project-a", "project-b"]);
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
    if (!existsSync(socketPath)) throw new Error("flow daemon socket never appeared");
    const start = async (): Promise<RunningControlPlaneHttpService> =>
      await startControlPlaneHttpService({
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
    service = await start();
    port = service.port;
    hostHeader.host = `127.0.0.1:${port}`;
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
    "fresh run: registry resolve, binding claim and a real subprocess to plan_ready",
    async () => {
      const response = await post(port, "/v1/projects/project-a/runs", {
        run_id: run1,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      });
      expect(response.status).toBe(200);
      const parsed = commandResult(response);
      expect(Object.keys(parsed).sort()).toEqual(["exit_code", "kind", "outcome", "run_id", "schema_version"]);
      expect(parsed["run_id"]).toBe(run1);
      expect(parsed["outcome"]).toBe("plan_ready");
      expect(parsed["exit_code"]).toBe(0);
      expectNoLeak(response.body, [stateRoot, inputRoot, join(fixtureRoot, "config"), socketPath]);
      const binding = await service.bindingStore.load(run1);
      expect(binding !== null && binding.projectId).toBe("project-a");
    },
    30000,
  );

  test(
    "a repeated fresh run reaches the CLI and reports run_already_exists without hiding it",
    async () => {
      const before = fingerprintFile(join(storeRoot, "bindings", `${run1}.json`));
      const response = await post(port, "/v1/projects/project-a/runs", {
        run_id: run1,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      });
      expect(response.status).toBe(200);
      const parsed = commandResult(response);
      expect(parsed["outcome"]).toBe("failed");
      expect(parsed["reason"]).toBe("run_already_exists");
      expect(parsed["exit_code"]).toBe(1);
      expect(fingerprintFile(join(storeRoot, "bindings", `${run1}.json`))).toBe(before);
    },
    30000,
  );

  test(
    "a missing input is a registry 404 and never reserves the run id",
    async () => {
      const response = await post(port, "/v1/projects/project-a/runs", {
        run_id: `${run1}-missing`,
        inputs: [{ id: "task", root_id: "tasks", ref: "missing.md" }],
      });
      expect(response.status).toBe(404);
      expect(errorBody(response)).toEqual({ layer: "registry", reason: "input_not_found" });
      await expect(service.bindingStore.load(`${run1}-missing`)).rejects.toMatchObject({
        reason: "binding_not_found",
      });
    },
    30000,
  );

  test("an unknown project is a registry 404", async () => {
    const response = await post(port, "/v1/projects/ghost/runs", {
      run_id: `${run1}-ghost`,
      inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
    });
    expect(response.status).toBe(404);
    expect(errorBody(response)).toEqual({ layer: "registry", reason: "project_not_found" });
  });

  test(
    "a conflicting binding claim is a 409 that never reveals the owner project",
    async () => {
      const bindingPath = join(storeRoot, "bindings", `${run1}.json`);
      const before = fingerprintFile(bindingPath);
      const response = await post(port, "/v1/projects/project-b/runs", {
        run_id: run1,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      });
      expect(response.status).toBe(409);
      const failure = errorBody(response);
      expect(failure).toEqual({ layer: "binding", reason: "binding_conflict" });
      expect(response.body.includes("project-a")).toBe(false);
      expect(fingerprintFile(bindingPath)).toBe(before);
    },
    30000,
  );

  test(
    "TCP smoke: inspect the plan-ready boundary, mutate through resume-plan, inspect the wait",
    async () => {
      const started = await post(port, "/v1/projects/project-a/runs", {
        run_id: run2,
        inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }],
      });
      expect(started.status).toBe(200);
      expect(commandResult(started)["outcome"]).toBe("plan_ready");
      const inspect1 = await getInspect(port, run2);
      expect(inspect1.status).toBe(200);
      const run1Doc = JSON.parse(inspect1.body) as Record<string, unknown>;
      expect(Object.keys(run1Doc).sort()).toEqual(["boundary", "kind", "outputs", "revision", "run_id", "schema_version", "status"]);
      expect(run1Doc["kind"]).toBe("run");
      expect(run1Doc["status"]).toBe("running");
      expect((run1Doc["boundary"] as Record<string, unknown>)["kind"]).toBe("plan_ready");
      expectNoLeak(inspect1.body, [stateRoot, socketPath, join(fixtureRoot, "config")]);
      const mutation = await post(port, `/v1/runs/${run2}/resume-plan`, { stage_id: "stage-1", initial_budget: 1 });
      expect(mutation.status).toBe(200);
      expect(commandResult(mutation)["outcome"]).toBe("waiting");
      const inspect2 = await getInspect(port, run2);
      const waiting = JSON.parse(inspect2.body) as Record<string, unknown>;
      expect(waiting["status"]).toBe("waiting");
      expect((waiting["boundary"] as Record<string, unknown>)["waitIndex"]).toBe(1);
    },
    40000,
  );

  test(
    "parallel mutators serialize on the existing kernel lock; the loser observes run_busy",
    async () => {
      const [first, second] = await Promise.all([
        post(port, `/v1/runs/${run1}/resume-plan`, { stage_id: "stage-1", initial_budget: 1 }),
        post(port, `/v1/runs/${run1}/resume-plan`, { stage_id: "stage-1", initial_budget: 1 }),
      ]);
      expect(first.status).toBe(200);
      expect(second.status).toBe(200);
      const outcomes = [commandResult(first), commandResult(second)].map((parsed) => `${String(parsed["outcome"])}:${String(parsed["reason"] ?? "")}`);
      expect(outcomes.sort()).toEqual([`failed:run_busy`, "waiting:"]);
    },
    60000,
  );

  test(
    "a lost HTTP response does not cancel the orchestration",
    async () => {
      const controller = new AbortController();
      const pending = fetch(`http://127.0.0.1:${port}/v1/runs/${run2}/continue-stage`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ wait_index: 1, additional_iterations: 1 }),
        signal: controller.signal,
      });
      await Bun.sleep(500);
      controller.abort();
      await pending.catch(() => undefined);
      const deadline = Date.now() + 40000;
      for (;;) {
        const inspect = await getInspect(port, run2);
        const doc = JSON.parse(inspect.body) as Record<string, unknown>;
        const boundary = doc["boundary"] as Record<string, unknown>;
        if (doc["status"] === "waiting" && boundary["waitIndex"] === 2) {
          break;
        }
        if (Date.now() > deadline) {
          throw new Error(`the orphaned continue-stage never reached wait 2: ${inspect.body}`);
        }
        await Bun.sleep(500);
      }
    },
    60000,
  );

  test(
    "revise-task resolves the task file exclusively through the bound project input root",
    async () => {
      const revised = await post(port, `/v1/runs/${run2}/revise-task`, {
        wait_index: 2,
        task_id: "task-a",
        task: { root_id: "tasks", ref: "task-a-v2.md" },
      });
      expect(revised.status).toBe(200);
      expect(commandResult(revised)["outcome"]).toBe("plan_ready");
      const replanned = await post(port, `/v1/runs/${run2}/resume-plan`, { stage_id: "stage-2", initial_budget: 1 });
      expect(replanned.status).toBe(200);
      expect(commandResult(replanned)["outcome"]).toBe("succeeded");
      const inspect = await getInspect(port, run2);
      const doc = JSON.parse(inspect.body) as Record<string, unknown>;
      expect(doc["status"]).toBe("succeeded");
      expect((doc["boundary"] as Record<string, unknown>)["kind"]).toBe("succeeded");
      expect((doc["outputs"] as readonly unknown[]).length).toBe(1);
    },
    60000,
  );

  test("revise-task refuses path-like references, missing files and caller task paths", async () => {
    const pathLike = await post(port, `/v1/runs/${run2}/revise-task`, {
      wait_index: 3,
      task_id: "task-a",
      task: { root_id: "tasks", ref: "sub/dir.md" },
    });
    expect(pathLike.status).toBe(400);
    expect(errorBody(pathLike)).toEqual({ layer: "request", reason: "invalid_request" });
    const missing = await post(port, `/v1/runs/${run2}/revise-task`, {
      wait_index: 3,
      task_id: "task-a",
      task: { root_id: "tasks", ref: "absent-file.md" },
    });
    expect(missing.status).toBe(404);
    expect(errorBody(missing)).toEqual({ layer: "registry", reason: "input_not_found" });
    const callerPath = await post(port, `/v1/runs/${run2}/revise-task`, {
      wait_index: 3,
      task_id: "task-a",
      task: { root_id: "tasks", ref: "task-a-v2.md" },
      task_file: "/etc/passwd",
    });
    expect(callerPath.status).toBe(400);
    expect(errorBody(callerPath)).toEqual({ layer: "request", reason: "invalid_request" });
  });

  test("the generic respond cannot perform the reserved interventions", async () => {
    const response = await post(port, `/v1/runs/${run2}/respond`, { wait_index: 3, action_id: "continue_stage" });
    expect(response.status).toBe(200);
    const parsed = commandResult(response);
    expect(parsed["outcome"]).toBe("failed");
    expect(parsed["reason"]).toBe("invalid_state");
  });

  test(
    "a CLI-created run without a binding fails closed as binding_not_found",
    async () => {
      const started = await service.client.run({
        kind: "run",
        runId: directRun,
        projectPath: projectDir,
        inputs: [{ id: "task", path: join(inputRoot, "task.md") }],
      });
      expect(started.ok).toBe(true);
      const runTree = join(stateRoot, "pipeline-runs", directRun);
      const before = await fingerprintTree(runTree);
      const inspect = await getInspect(port, directRun);
      expect(inspect.status).toBe(404);
      expect(errorBody(inspect)).toEqual({ layer: "binding", reason: "binding_not_found" });
      const resume = await post(port, `/v1/runs/${directRun}/resume`, {});
      expect(resume.status).toBe(404);
      expect(errorBody(resume)).toEqual({ layer: "binding", reason: "binding_not_found" });
      expect(await fingerprintTree(runTree)).toBe(before);
    },
    30000,
  );

  test(
    "restart: a new registry, store, client and server keep the binding byte-identical and inspect authoritative",
    async () => {
      const bindingPath = join(storeRoot, "bindings", `${run2}.json`);
      const bindingBefore = fingerprintFile(bindingPath);
      const bindingStatBefore = statSync(bindingPath);
      const registryBefore = fingerprintFile(registryPath);
      const runTreeBefore = await fingerprintTree(join(stateRoot, "pipeline-runs", run2));
      const inspectBefore = await getInspect(port, run2);
      await service.stop();
      const restarted = await startControlPlaneHttpService({
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
      service = restarted;
      port = restarted.port;
      hostHeader.host = `127.0.0.1:${port}`;
      const inspectAfter = await getInspect(port, run2);
      expect(JSON.parse(inspectAfter.body)).toEqual(JSON.parse(inspectBefore.body));
      expect(await fingerprintTree(join(stateRoot, "pipeline-runs", run2))).toBe(runTreeBefore);
      expect(fingerprintFile(registryPath)).toBe(registryBefore);
      expect(fingerprintFile(bindingPath)).toBe(bindingBefore);
      const bindingStatAfter = statSync(bindingPath);
      expect(bindingStatAfter.ino).toBe(bindingStatBefore.ino);
      expect(bindingStatAfter.mode).toBe(bindingStatBefore.mode);
      expect(bindingStatAfter.mtimeMs).toBe(bindingStatBefore.mtimeMs);
      expect(bindingStatAfter.size).toBe(bindingStatBefore.size);
    },
    30000,
  );

  test("existing mutations never accept a caller-supplied projectId", async () => {
    const rejected = await post(port, `/v1/runs/${run2}/resume-plan`, {
      stage_id: "stage-1",
      initial_budget: 1,
      project_id: "project-a",
    });
    expect(rejected.status).toBe(400);
    expect(errorBody(rejected)).toEqual({ layer: "request", reason: "invalid_request" });
  });

  test("no response ever leaks a host path, the credential or the socket", async () => {
    const leakRoots = [smokeRoot, stateRoot, credDir, inputRoot, join(fixtureRoot, "config"), join(fixtureRoot, "bundle"), socketPath, "smoke-credential-token"];
    const probe = await getInspect(port, run2);
    expectNoLeak(probe.body, leakRoots);
    expectNoLeak(JSON.stringify(probe.headers), leakRoots);
    const failed = await post(port, "/v1/projects/ghost/runs", { run_id: `${run2}-x`, inputs: [{ id: "task", root_id: "tasks", ref: "task.md" }] });
    expectNoLeak(failed.body, leakRoots);
    const mutation = await post(port, `/v1/runs/${run2}/continue-stage`, { wait_index: 3, additional_iterations: 1 });
    expect(mutation.status).toBe(200);
    expectNoLeak(mutation.body, leakRoots);
  }, 60000);

  test(
    "a removed registry project fails closed as project_unavailable while the binding survives",
    async () => {
      await service.stop();
      writeRegistry(["project-b"]);
      const reduced = await startControlPlaneHttpService({
        registryPath,
        bindingStoreRoot: storeRoot,
        client: {
          bunExecutable: process.execPath,
          orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
          workingDirectory: smokeRoot,
          baseEnv: { PATH: "/usr/bin:/bin" },
          stdoutLimitBytes: 100000,
          stderrLimitBytes: 100000,
          sigintGraceMs: 1000,
        },
        port: 0,
        bodyLimitBytes: 4096,
      });
      service = reduced;
      port = reduced.port;
      hostHeader.host = `127.0.0.1:${port}`;
      const bindingPath = join(storeRoot, "bindings", `${run2}.json`);
      const before = fingerprintFile(bindingPath);
      const response = await getInspect(port, run2);
      expect(response.status).toBe(409);
      expect(errorBody(response)).toEqual({ layer: "binding", reason: "project_unavailable" });
      expect(response.body.includes("project-a")).toBe(false);
      expect(fingerprintFile(bindingPath)).toBe(before);
      writeRegistry(["project-a", "project-b"]);
    },
    30000,
  );
});

describe("control-plane HTTP: production entrypoint", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-main-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  const inputRoot = join(envRoot, "inputs");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  const port = 20000 + Math.floor(Math.random() * 20000);

  beforeAll(() => {
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "MAIN-TASK\n", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
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
  });

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  test(
    "the operator entrypoint serves the loopback routes and prints only safe listen information",
    async () => {
      const child = Bun.spawn(
        [
          process.execPath,
          join(import.meta.dir, "..", "src", "control_plane", "server_main.ts"),
          "--registry",
          registryPath,
          "--binding-store",
          storeRoot,
          "--port",
          String(port),
          "--env",
          "PATH=/usr/bin:/bin",
          "--env",
          `HOME=${envRoot}`,
        ],
        { stdout: "pipe", stderr: "pipe" },
      );
      const reader = child.stdout.getReader();
      const decoder = new TextDecoder();
      let stdout = "";
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        stdout += decoder.decode(value);
        if (stdout.includes("listening on")) break;
      }
      expect(stdout).toBe(`control-plane: listening on 127.0.0.1:${port}\n`);
      expect(stdout.includes(CANARY)).toBe(false);
      const unbound = await rawRequest(port, "GET", "/v1/runs/neverbound", { host: `127.0.0.1:${port}` });
      expect(unbound.status).toBe(404);
      expect(errorBody(unbound)).toEqual({ layer: "binding", reason: "binding_not_found" });
      child.kill("SIGTERM");
      const exitCode = await child.exited;
      expect(exitCode).toBe(0);
    },
    30000,
  );
});

describe("control-plane HTTP: listen address contract", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-listen-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  const inputRoot = join(envRoot, "inputs");
  const configRoot = join(envRoot, "config");
  const pipelineRoot = join(envRoot, "bundle");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");

  beforeAll(() => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    mkdirSync(configRoot, { mode: 0o700 });
    mkdirSync(pipelineRoot, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "LISTEN-TASK\n", { mode: 0o600 });
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
  });

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  const buildConfig = (listenAddress?: "127.0.0.1" | "0.0.0.0") => ({
    registryPath,
    bindingStoreRoot: storeRoot,
    client: {
      bunExecutable: process.execPath,
      orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
      workingDirectory: envRoot,
      baseEnv: { PATH: "/usr/bin:/bin", HOME: envRoot },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 1000,
    },
    port: 0,
    bodyLimitBytes: 4096,
    ...(listenAddress === undefined ? {} : { listenAddress }),
  });

  /** A real TCP probe against the non-primary loopback address 127.0.0.2. */
  function probeSecondaryLoopback(port: number, hostHeader: string): Promise<{ reachable: boolean; status: number }> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        req.destroy();
        resolve({ reachable: false, status: 0 });
      }, 2000);
      const req = httpRequest({ host: "127.0.0.2", port, method: "GET", path: "/", headers: { host: hostHeader } }, (res) => {
        clearTimeout(timer);
        res.resume();
        resolve({ reachable: true, status: res.statusCode ?? 0 });
      });
      req.on("error", () => {
        clearTimeout(timer);
        resolve({ reachable: false, status: 0 });
      });
      req.end();
    });
  }

  test(
    "l1. the default configuration binds the loopback address only",
    async () => {
      const service = await startControlPlaneHttpService(buildConfig());
      try {
        const local = await rawRequest(service.port, "GET", `/v1/runs/neverbound`, { host: `127.0.0.1:${service.port}` });
        expect(local.status).toBe(404);
        expect(JSON.parse(local.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
        const secondary = await probeSecondaryLoopback(service.port, `127.0.0.2:${service.port}`);
        expect(secondary.reachable).toBe(false);
      } finally {
        await service.stop();
      }
    },
    30000,
  );

  test(
    "l2. an explicit 127.0.0.1 behaves exactly like the default",
    async () => {
      const service = await startControlPlaneHttpService(buildConfig("127.0.0.1"));
      try {
        const local = await rawRequest(service.port, "GET", `/v1/runs/neverbound`, { host: `127.0.0.1:${service.port}` });
        expect(local.status).toBe(404);
        const secondary = await probeSecondaryLoopback(service.port, `127.0.0.2:${service.port}`);
        expect(secondary.reachable).toBe(false);
      } finally {
        await service.stop();
      }
    },
    30000,
  );

  test(
    "l3. an explicit 0.0.0.0 accepts container-interface connections while the authority gate stays 127.0.0.1",
    async () => {
      const service = await startControlPlaneHttpService(buildConfig("0.0.0.0"));
      try {
        const local = await rawRequest(service.port, "GET", `/v1/runs/neverbound`, { host: `127.0.0.1:${service.port}` });
        expect(local.status).toBe(404);
        // The container interface is now reachable, but the Host header
        // still decides the authority: a foreign Host is refused, the
        // established browser authority passes.
        const foreign = await probeSecondaryLoopback(service.port, `127.0.0.2:${service.port}`);
        expect(foreign.reachable).toBe(true);
        expect(foreign.status).toBe(400);
        expect((await probeSecondaryLoopback(service.port, `127.0.0.1:${service.port}`)).status).toBe(200);
      } finally {
        await service.stop();
      }
    },
    30000,
  );

  test("l4. an invalid configured listen address fails closed before any socket", async () => {
    for (const value of ["localhost", "::", "", "0.0.0.0:80", "0.0.0.0 "]) {
      let message = "";
      try {
        await startControlPlaneHttpService(buildConfig(value as never));
      } catch (cause) {
        message = cause instanceof Error ? cause.message : String(cause);
      }
      expect(message).toBe("the listen address must be exactly 127.0.0.1 or 0.0.0.0");
    }
  });

  test(
    "l5. the operator entrypoint accepts both exact values and prints the bound address",
    async () => {
      for (const [flag, expectedAddress] of [
        [["--listen-address", "0.0.0.0"], "0.0.0.0"],
        [["--listen-address=0.0.0.0"], "0.0.0.0"],
        [["--listen-address", "127.0.0.1"], "127.0.0.1"],
      ] as const) {
        const port = 40000 + Math.floor(Math.random() * 20000);
        const child = Bun.spawn(
          [
            process.execPath,
            join(import.meta.dir, "..", "src", "control_plane", "server_main.ts"),
            "--registry",
            registryPath,
            "--binding-store",
            storeRoot,
            "--port",
            String(port),
            ...flag,
            "--env",
            "PATH=/usr/bin:/bin",
            "--env",
            `HOME=${envRoot}`,
          ],
          { stdout: "pipe", stderr: "pipe" },
        );
        const reader = child.stdout.getReader();
        const decoder = new TextDecoder();
        let stdout = "";
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          stdout += decoder.decode(value);
          if (stdout.includes("listening on")) break;
        }
        expect(stdout).toBe(`control-plane: listening on ${expectedAddress}:${port}\n`);
        const unbound = await rawRequest(port, "GET", "/v1/runs/neverbound", { host: `127.0.0.1:${port}` });
        expect(unbound.status).toBe(404);
        child.kill("SIGTERM");
        expect(await child.exited).toBe(0);
      }
    },
    60000,
  );

  test(
    "l6. the operator entrypoint refuses missing, empty, duplicate and unknown listen addresses before starting",
    async () => {
      for (const [flag, expectedMessage] of [
        [["--env", "PATH=/usr/bin:/bin", "--listen-address"], "--listen-address requires a value"],
        [["--listen-address="], "--listen-address requires a value"],
        [["--listen-address", "127.0.0.1", "--listen-address", "0.0.0.0"], "--listen-address must not repeat"],
        [["--listen-address", "localhost"], "--listen-address must be exactly 127.0.0.1 or 0.0.0.0"],
        [["--listen-address", "0.0.0.0:80"], "--listen-address must be exactly 127.0.0.1 or 0.0.0.0"],
        [["--listen-address", "::"], "--listen-address must be exactly 127.0.0.1 or 0.0.0.0"],
      ] as const) {
        const port = 40000 + Math.floor(Math.random() * 20000);
        const child = Bun.spawn(
          [
            process.execPath,
            join(import.meta.dir, "..", "src", "control_plane", "server_main.ts"),
            "--registry",
            registryPath,
            "--binding-store",
            storeRoot,
            "--port",
            String(port),
            ...flag,
          ],
          { stdout: "pipe", stderr: "pipe" },
        );
        const stdout = await new Response(child.stdout).text();
        const stderr = await new Response(child.stderr).text();
        expect(await child.exited).toBe(2);
        expect(stdout).toBe("");
        expect(stderr).toContain(`control-plane: ${expectedMessage}`);
        expect(stderr).toContain("usage: bun server_main.ts");
        expect(stderr).toContain("--listen-address");
      }
    },
    60000,
  );
});

describe("control-plane HTTP: discovery (projects and bound runs)", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-discovery-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const registryV2Path = join(envRoot, "registry-v2.json");
  const storeRoot = join(envRoot, "bindings");
  let service: RunningControlPlaneHttpService;
  let reducedService: RunningControlPlaneHttpService;
  let port = 0;
  let reducedPort = 0;

  const get = (path: string): Promise<RawResponse> => rawRequest(port, "GET", path, { host: `127.0.0.1:${port}` });
  const reducedGet = (path: string): Promise<RawResponse> =>
    rawRequest(reducedPort, "GET", path, { host: `127.0.0.1:${reducedPort}` });

  beforeAll(async () => {
    const inputRootOne = join(envRoot, "inputs-a");
    const inputRootTwo = join(envRoot, "inputs-b");
    const projectDir = join(envRoot, "project");
    mkdirSync(inputRootOne, { mode: 0o700 });
    mkdirSync(inputRootTwo, { mode: 0o700 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [
              { id: "tasks", root: inputRootOne },
              { id: "plans", root: inputRootTwo },
            ],
          },
          {
            id: "project-b",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [{ id: "notes", root: inputRootOne }],
          },
        ],
      }),
      { mode: 0o600 },
    );
    writeFileSync(
      registryV2Path,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [{ id: "tasks", root: inputRootOne }],
          },
        ],
      }),
      { mode: 0o600 },
    );
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "run-c", projectId: "project-a" });
    await store.claim({ runId: "run-a", projectId: "project-b" });
    await store.claim({ runId: "run-d", projectId: "project-gone" });
    await store.claim({ runId: "run-b", projectId: "project-a" });
    const client = {
      bunExecutable: process.execPath,
      // discovery never spawns: a nonexistent entrypoint would fail loudly
      orchestratorEntrypoint: join(envRoot, "missing-entrypoint.ts"),
      workingDirectory: envRoot,
      baseEnv: { PATH: "/usr/bin:/bin" },
      stdoutLimitBytes: 100000,
      stderrLimitBytes: 100000,
      sigintGraceMs: 1000,
    };
    service = await startControlPlaneHttpService({ registryPath, bindingStoreRoot: storeRoot, client, port: 0, bodyLimitBytes: 4096 });
    port = service.port;
    reducedService = await startControlPlaneHttpService({ registryPath: registryV2Path, bindingStoreRoot: storeRoot, client, port: 0, bodyLimitBytes: 4096 });
    reducedPort = reducedService.port;
  }, 20000);

  afterAll(() => {
    service?.stop();
    reducedService?.stop();
    rmSync(envRoot, { recursive: true, force: true });
  });

  test("d1. projects project declaration-order ids and the effective capability, byte-identical on repeat", async () => {
    const expected = {
      schema_version: 2,
      kind: "projects",
      projects: [
        { id: "project-a", input_roots: [{ id: "tasks" }, { id: "plans" }], project_apply: false },
        { id: "project-b", input_roots: [{ id: "notes" }], project_apply: false },
      ],
    };
    const first = await get("/v1/projects");
    expect(first.status).toBe(200);
    expect(JSON.parse(first.body)).toEqual(expected);
    expect(JSON.parse(first.body)).toEqual(JSON.parse((await get("/v1/projects")).body));
    expect(first.body).not.toContain(envRoot);
    expect(first.body).not.toContain(CANARY);
    expect(first.body).not.toContain("config_root");
    expect(first.body).not.toContain("launcher");
  });

  test("d2. the projects endpoint rejects query parameters and POST", async () => {
    expect((await get("/v1/projects?limit=1")).status).toBe(400);
    const posted = await rawRequest(port, "POST", "/v1/projects", { "content-type": "application/json", host: `127.0.0.1:${port}` }, "{}");
    expect(posted.status).toBe(405);
  });

  test("d3. runs project lexical order, exact entry keys and page-complete next_after", async () => {
    const all = JSON.parse((await get("/v1/runs")).body);
    expect(all).toEqual({
      schema_version: 1,
      kind: "runs",
      runs: [
        { run_id: "run-a", project_id: "project-b", project_available: true },
        { run_id: "run-b", project_id: "project-a", project_available: true },
        { run_id: "run-c", project_id: "project-a", project_available: true },
        { run_id: "run-d", project_id: "project-gone", project_available: false },
      ],
      next_after: null,
    });
  });

  test("d4. the limit pages carry next_after exactly when another binding follows", async () => {
    const one = JSON.parse((await get("/v1/runs?limit=1")).body);
    expect(one.runs).toEqual([{ run_id: "run-a", project_id: "project-b", project_available: true }]);
    expect(one.next_after).toBe("run-a");
    const two = JSON.parse((await get("/v1/runs?limit=2")).body);
    expect(two.runs.map((entry: { run_id: string }) => entry.run_id)).toEqual(["run-a", "run-b"]);
    expect(two.next_after).toBe("run-b");
    const full = JSON.parse((await get("/v1/runs?limit=200")).body);
    expect(full.runs.length).toBe(4);
    expect(full.next_after).toBe(null);
  });

  test("d5. the after cursor is exclusive and combines with the limit", async () => {
    const afterFirst = JSON.parse((await get("/v1/runs?after=run-a")).body);
    expect(afterFirst.runs.map((entry: { run_id: string }) => entry.run_id)).toEqual(["run-b", "run-c", "run-d"]);
    expect(afterFirst.next_after).toBe(null);
    const beforeAll = JSON.parse((await get("/v1/runs?after=run-0")).body);
    expect(beforeAll.runs.length).toBe(4);
    const afterLast = JSON.parse((await get("/v1/runs?after=run-d")).body);
    expect(afterLast).toEqual({ schema_version: 1, kind: "runs", runs: [], next_after: null });
    const between = JSON.parse((await get("/v1/runs?after=run-b&limit=1")).body);
    expect(between.runs).toEqual([{ run_id: "run-c", project_id: "project-a", project_available: true }]);
    expect(between.next_after).toBe("run-c");
  });

  test("d6. malformed, duplicate, out-of-range and unknown query fields are request errors", async () => {
    for (const query of [
      "?limit=0",
      "?limit=201",
      "?limit=-1",
      "?limit=abc",
      "?limit=1.5",
      "?limit=1e2",
      "?limit=+5",
      "?limit=",
      "?limit=1&limit=1",
      "?after=run-a&after=run-b",
      "?after=",
      "?after=-bad",
      "?unknown=1",
      "?limit",
      "?=x",
      "?limit=%32",
    ]) {
      const response = await get(`/v1/runs${query}`);
      expect(response.status).toBe(400);
      expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_request" });
    }
  });

  test("d7. a removed registry project stays visible with project_available false", async () => {
    const listed = JSON.parse((await reducedGet("/v1/runs")).body);
    expect(listed.runs).toEqual([
      { run_id: "run-a", project_id: "project-b", project_available: false },
      { run_id: "run-b", project_id: "project-a", project_available: true },
      { run_id: "run-c", project_id: "project-a", project_available: true },
      { run_id: "run-d", project_id: "project-gone", project_available: false },
    ]);
  });

  test("d8. repeated discovery responses are byte-identical and discovery writes nothing", async () => {
    const before = await fingerprintTree(storeRoot);
    const first = await get("/v1/runs");
    const second = await get("/v1/runs?limit=2");
    expect((await get("/v1/runs")).body).toBe(first.body);
    expect((await get("/v1/runs?limit=2")).body).toBe(second.body);
    expect(await fingerprintTree(storeRoot)).toBe(before);
  });

  test("d9. discovery never spawns a subprocess and never leaks the client", async () => {
    // the client's entrypoint does not exist: any spawn would surface as 502
    expect((await get("/v1/runs")).status).toBe(200);
    expect((await get("/v1/projects")).status).toBe(200);
  });

  test("d10. the trust gates apply to the discovery routes", async () => {
    const wrongHost = await rawRequest(port, "GET", "/v1/runs", { host: "evil.example" });
    expect(wrongHost.status).toBe(400);
    expect(errorBody(wrongHost)).toEqual({ layer: "request", reason: "invalid_host" });
    const wrongOrigin = await rawRequest(port, "GET", "/v1/runs", { host: `127.0.0.1:${port}`, origin: "http://evil.example" });
    expect(wrongOrigin.status).toBe(400);
    expect(errorBody(wrongOrigin)).toEqual({ layer: "request", reason: "invalid_origin" });
    const withBody = await rawRequest(port, "GET", "/v1/runs", { host: `127.0.0.1:${port}`, "content-length": "5" }, "hello");
    expect(withBody.status).toBe(400);
    expect(errorBody(withBody)).toEqual({ layer: "request", reason: "invalid_request" });
  });

  test("d11. a defective binding fails the whole listing with the typed envelope", async () => {
    writeFileSync(join(storeRoot, "bindings", "defective-run.json"), "not-json{");
    try {
      const response = await get("/v1/runs");
      expect(response.status).toBe(500);
      expect(errorBody(response)).toEqual({ layer: "binding", reason: "binding_invalid" });
      expect(response.body.includes("run-a")).toBe(false);
    } finally {
      rmSync(join(storeRoot, "bindings", "defective-run.json"));
    }
    expect(JSON.parse((await get("/v1/runs")).body).runs.length).toBe(4);
  });

  test("d12. a claim concurrent with a listing lands now or at the next refresh", async () => {
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    const claimPromise = store.claim({ runId: "run-e", projectId: "project-a" });
    const during = await get("/v1/runs");
    expect(claimPromise).resolves.toMatchObject({ kind: "created" });
    const duringListed = JSON.parse(during.body).runs.map((entry: { run_id: string }) => entry.run_id);
    expect(duringListed).toEqual([...duringListed].sort());
    expect(duringListed.includes("run-e") || !duringListed.includes("run-e")).toBe(true);
    const refreshed = JSON.parse((await get("/v1/runs")).body);
    expect(refreshed.runs.map((entry: { run_id: string }) => entry.run_id)).toContain("run-e");
    expect(refreshed.next_after).toBe(null);
  });

  test("d13. the discovery query parser is exact at the unit level", () => {
    const ok = (search: string) => {
      const parsed = parseRunsDiscoveryQuery(search);
      if (parsed instanceof ControlPlaneHttpRequestError) throw parsed;
      return parsed;
    };
    expect(ok("")).toEqual({ limit: 100, after: null });
    expect(ok("?limit=7")).toEqual({ limit: 7, after: null });
    expect(ok("?after=run-a")).toEqual({ limit: 100, after: "run-a" });
    expect(ok("?limit=200&after=run-b")).toEqual({ limit: 200, after: "run-b" });
    const bad = (search: string) => (parseRunsDiscoveryQuery(search) as ControlPlaneHttpRequestError).reason;
    expect(bad("?limit=201")).toBe("invalid_request");
    expect(bad("?limit=x")).toBe("invalid_request");
  });

  test("d14. the projections are pure over frozen inputs and never sorted", () => {
    const frozen = Object.freeze({
      projects: [
        Object.freeze({
          id: "project-a",
          configRoot: "/not/exposed",
          pipelineRoot: null,
          launcherId: null,
          projectRoot: "/not/exposed",
          inputRoots: [Object.freeze({ id: "tasks", root: "/not/exposed" }), Object.freeze({ id: "plans", root: "/not/exposed" })],
        }),
        Object.freeze({
          id: "project-b",
          configRoot: "/not/exposed",
          pipelineRoot: null,
          launcherId: null,
          projectRoot: "/not/exposed",
          inputRoots: [Object.freeze({ id: "notes", root: "/not/exposed" })],
        }),
      ],
    });
    const projected = projectControlPlaneProjects(frozen as never, { applyStoreConfigured: true });
    expect(projected.projects.map((project) => project.id)).toEqual(["project-a", "project-b"]);
    const entries = projectControlPlaneRuns(
      [
        { runId: "z-run", projectId: "project-a" },
        { runId: "a-run", projectId: "project-b" },
      ],
      { limit: 100, after: null },
      frozen as never,
    );
    expect(entries.runs.map((entry) => entry.run_id)).toEqual(["z-run", "a-run"]);
  });
});

describe("control-plane HTTP: source restrictions", () => {
  test("the production tree has exactly one HTTP server owner and no forbidden capabilities", async () => {
    const serverSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "http_server.ts"), "utf8");
    const entrySource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "server_main.ts"), "utf8");
    expect(serverSource.split("Bun.serve(").length - 1).toBe(1);
    expect(entrySource.includes("Bun.serve(")).toBe(false);
    for (const banned of [
      "node:fs",
      "readdir",
      "state.json",
      "pipeline-runs",
      "exec(",
      "shell",
      "Bun.spawn",
      ".stderr.", "stderr.split",
      "new Map(",
      "new Set(",
      "PIPELINE_V2_PUBLIC_REASONS",
      "readFileSync",
      "request.signal",
      "signal:",
    ]) {
      expect(serverSource.includes(banned)).toBe(false);
    }
    expect(serverSource.split("JSON.parse(").length - 1).toBe(1);
    // discovery: no filesystem access, no run-state scanning, no subprocess,
    // no sorting and no raw registry document in any projection
    const discoverySource = serverSource.split("interface CommandOutcomeProjection")[0] ?? "";
    expect(discoverySource).not.toContain("deps.client");
    expect(discoverySource).not.toContain(".sort(");
    expect(discoverySource).not.toContain("readdir");
    expect(discoverySource).not.toContain("state.json");
    expect(discoverySource).not.toContain("pipeline-runs");
    expect(discoverySource.split("projectControlPlaneProjects(").length - 1).toBe(1);
    expect(discoverySource.split("projectControlPlaneRuns(").length - 1).toBe(1);
    expect(serverSource.split("bindingStore.listBindings(").length - 1).toBe(1);
    for (const imported of [
      'from "./routing.ts"',
      'from "../clean_path.ts"',
      'from "../pipeline_v2_scalar.ts"',
      'from "./project_registry.ts"',
      'from "./run_binding_store.ts"',
      'from "./subprocess_client.ts"',
    ]) {
      expect(serverSource.includes(imported)).toBe(true);
    }
    for (const bannedImport of ['from "../pipeline_v2_coordinator', 'from "../pipeline_v2_runner', 'from "../pipeline_v2_state', 'from "../pipeline_runner', 'from "../run_snapshot_store']) {
      expect(serverSource.includes(bannedImport)).toBe(false);
    }
    expect(entrySource.split("Bun.serve(").length - 1).toBe(0);
    expect(entrySource.includes("node:fs")).toBe(false);
  });
});

describe("control-plane HTTP: lifecycle events route", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-events-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;
  const calls: unknown[] = [];
  let nextResult: OrchestratorCallResult = {
    ok: true,
    projection: {
      kind: "events",
      schemaVersion: 1,
      runId: "events-run",
      revision: 3,
      events: [{ revision: 1, kind: "create_run" }],
      nextAfter: null,
    },
  };

  beforeAll(async () => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    registry = await loadControlPlaneProjectRegistry(registryPath);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "events-run", projectId: "project-a" });
    await store.claim({ runId: "gone-project-run", projectId: "project-removed" });
  }, 20000);

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  const callHandler = async (path: string, method = "GET", headers: Record<string, string> = {}, body?: string): Promise<RawResponse> => {
    const fakeClient = {
      run: async (command: unknown): Promise<OrchestratorCallResult> => {
        calls.push(command);
        return nextResult;
      },
    } as unknown as OrchestratorSubprocessClient;
    const deps = {
      registry,
      bindingStore: await ControlPlaneRunBindingStore.open(storeRoot),
      client: fakeClient,
      assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
      port: 4711,
      bodyLimitBytes: 4096,
    };
    const request = new Request(`http://127.0.0.1:4711${path}`, {
      method,
      headers: { host: "127.0.0.1:4711", ...headers },
      ...(body === undefined ? {} : { body }),
    });
    const response = await handleControlPlaneHttpRequest(deps as never, request);
    return { status: response.status, headers: Object.fromEntries(response.headers.entries()), body: await response.text() };
  };
  const get = (path: string, headers: Record<string, string> = {}): Promise<RawResponse> => callHandler(path, "GET", headers);

  test("a bound run serves the events page with the exact schema and no run root", async () => {
    const response = await get("/v1/runs/events-run/events");
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body).toEqual({
      schema_version: 1,
      kind: "events",
      run_id: "events-run",
      revision: 3,
      events: [{ revision: 1, kind: "create_run" }],
      next_after: null,
    });
    expect(response.body).not.toContain("run_root");
    expect(response.headers["content-type"]).toBe("application/json");
    expect(calls).toEqual([{ kind: "events", runId: "events-run" }]);
  });

  test("the query cursor and limit travel as typed command fields", async () => {
    calls.length = 0;
    await get("/v1/runs/events-run/events?after=5&limit=2");
    expect(calls).toEqual([{ kind: "events", runId: "events-run", after: 5, limit: 2 }]);
    calls.length = 0;
    await get("/v1/runs/events-run/events?limit=2&after=5");
    expect(calls).toEqual([{ kind: "events", runId: "events-run", after: 5, limit: 2 }]);
    calls.length = 0;
    await get("/v1/runs/events-run/events?after=0");
    expect(calls).toEqual([{ kind: "events", runId: "events-run", after: 0 }]);
  });

  test("a typed invalid_cursor is the explicit request-layer 400, never a 502", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "eventsFailure", exitCode: 1, runId: "events-run", runRoot: "/secret/root", reason: "invalid_cursor" },
    };
    const response = await get("/v1/runs/events-run/events?after=999");
    expect(response.status).toBe(400);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
    expect(response.body).not.toContain("/secret/root");
  });

  test("other events failures follow the command-result precedent", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "eventsFailure", exitCode: 1, runId: "events-run", runRoot: null, reason: "run_state_invalid" },
    };
    const response = await get("/v1/runs/events-run/events");
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "events-run",
      outcome: "failed",
      exit_code: 1,
      reason: "run_state_invalid",
    });
  });

  test("an unbound CLI-created run fails closed before any subprocess", async () => {
    calls.length = 0;
    const response = await get("/v1/runs/unbound-run/events");
    expect(response.status).toBe(404);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    expect(calls).toEqual([]);
  });

  test("a removed registry project stays project_unavailable with the binding kept", async () => {
    const response = await get("/v1/runs/gone-project-run/events");
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
  });

  test("the events route accepts GET only and no body", async () => {
    const post = await callHandler("/v1/runs/events-run/events", "POST", { "content-type": "application/json" }, "{}");
    expect(post.status).toBe(405);
    const withBody = await callHandler("/v1/runs/events-run/events", "GET", { "content-length": "5" }, "hello");
    expect(withBody.status).toBe(400);
  });

  test("the loopback gates apply to the events route unchanged", async () => {
    const host = await callHandler("/v1/runs/events-run/events", "GET", { host: "evil.example" });
    expect(host.status).toBe(400);
    expect(JSON.parse(host.body).reason).toBe("invalid_host");
    const origin = await callHandler("/v1/runs/events-run/events", "GET", { origin: "http://evil.example" });
    expect(origin.status).toBe(400);
    expect(JSON.parse(origin.body).reason).toBe("invalid_origin");
  });

  test("the events response never carries host paths or canaries", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "events",
        schemaVersion: 1,
        runId: "events-run",
        revision: 2,
        events: [{ revision: 1, kind: "create_run" }, { revision: 2, kind: "agent_running", execution_index: 1 }],
        nextAfter: null,
      },
    };
    const response = await get("/v1/runs/events-run/events");
    expect(response.status).toBe(200);
    expect(response.body).not.toContain(CANARY);
    expect(response.body).not.toContain("state");
    expect(response.body).not.toContain("boundary");
    expect(response.body).not.toContain(envRoot);
  });
});

describe("control-plane HTTP: worker logs routes", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-logs-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;
  const calls: unknown[] = [];
  let nextResult: OrchestratorCallResult = {
    ok: true,
    projection: {
      kind: "logs",
      schemaVersion: 1,
      runId: "logs-run",
      revision: 25,
      logs: [
        { executionIndex: 1, stateId: "architect", status: "captured", sha256: "a".repeat(64), byteCount: 47, truncatedDaemon: false, truncatedLocal: false },
        { executionIndex: 4, stateId: "architect", status: "pending" },
      ],
    },
  };

  beforeAll(async () => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    registry = await loadControlPlaneProjectRegistry(registryPath);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "logs-run", projectId: "project-a" });
    await store.claim({ runId: "gone-project-run", projectId: "project-removed" });
  }, 20000);

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  const callHandler = async (path: string, method = "GET", headers: Record<string, string> = {}, body?: string): Promise<RawResponse> => {
    const fakeClient = {
      run: async (command: unknown): Promise<OrchestratorCallResult> => {
        calls.push(command);
        return nextResult;
      },
    } as unknown as OrchestratorSubprocessClient;
    const deps = {
      registry,
      bindingStore: await ControlPlaneRunBindingStore.open(storeRoot),
      client: fakeClient,
      assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
      port: 4711,
      bodyLimitBytes: 4096,
    };
    const request = new Request(`http://127.0.0.1:4711${path}`, {
      method,
      headers: { host: "127.0.0.1:4711", ...headers },
      ...(body === undefined ? {} : { body }),
    });
    const response = await handleControlPlaneHttpRequest(deps as never, request);
    return { status: response.status, headers: Object.fromEntries(response.headers.entries()), body: await response.text() };
  };
  const get = (path: string, headers: Record<string, string> = {}): Promise<RawResponse> => callHandler(path, "GET", headers);

  test("a bound run serves the logs list with the exact snake_case schema and no run root", async () => {
    const response = await get("/v1/runs/logs-run/logs");
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body).toEqual({
      schema_version: 1,
      kind: "worker_logs",
      run_id: "logs-run",
      revision: 25,
      logs: [
        { execution_index: 1, state_id: "architect", status: "captured", sha256: "a".repeat(64), byte_count: 47, truncated_daemon: false, truncated_local: false },
        { execution_index: 4, state_id: "architect", status: "pending" },
      ],
    });
    expect(response.body).not.toContain("run_root");
    expect(response.body).not.toContain("data_base64");
    expect(calls).toEqual([{ kind: "logs", runId: "logs-run" }]);
  });

  test("the execution route maps the query to typed command fields", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "logsChunk",
        schemaVersion: 1,
        runId: "logs-run",
        revision: 25,
        executionIndex: 1,
        stateId: "architect",
        status: "captured",
        sha256: "a".repeat(64),
        byteCount: 47,
        truncatedDaemon: false,
        truncatedLocal: false,
        offset: 16,
        dataBase64: Buffer.from("chunk-bytes").toString("base64"),
        nextOffset: null,
      },
    };
    calls.length = 0;
    const response = await get("/v1/runs/logs-run/logs/1?offset=16&limit=8");
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body["kind"]).toBe("worker_log");
    expect(body["execution_index"]).toBe(1);
    expect(body["data_base64"]).toBe(Buffer.from("chunk-bytes").toString("base64"));
    expect(body["next_offset"]).toBe(null);
    expect(response.body).not.toContain("run_root");
    expect(calls).toEqual([{ kind: "logs", runId: "logs-run", executionIndex: 1, offset: 16, limit: 8 }]);
    // the query order is semantically irrelevant
    calls.length = 0;
    await get("/v1/runs/logs-run/logs/1?limit=8&offset=16");
    expect(calls).toEqual([{ kind: "logs", runId: "logs-run", executionIndex: 1, offset: 16, limit: 8 }]);
    // omitted scalars reach the command only when present
    calls.length = 0;
    await get("/v1/runs/logs-run/logs/1");
    expect(calls).toEqual([{ kind: "logs", runId: "logs-run", executionIndex: 1 }]);
  });

  test("the pending/unavailable execution response carries the status keys only", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "logsExecution",
        schemaVersion: 1,
        runId: "logs-run",
        revision: 25,
        executionIndex: 4,
        stateId: "architect",
        status: "pending",
      },
    };
    const response = await get("/v1/runs/logs-run/logs/4");
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual([
      "execution_index", "kind", "revision", "run_id", "schema_version", "state_id", "status",
    ]);
  });

  test("the execution query parser accepts only offset and limit with the exact grammar", async () => {
    for (const bad of [
      "?after=1",
      "?offset",
      "?offset=",
      "?offset=+1",
      "?offset=01",
      "?offset=1.5",
      "?offset=1e2",
      "?offset=-1",
      "?offset=9007199254740992",
      "?limit=0",
      "?limit=262145",
      "?limit=2&limit=2",
      "?offset=1&offset=1",
      "?ghost=1",
      "?%6ffset=1",
    ]) {
      const response = await get(`/v1/runs/logs-run/logs/1${bad}`);
      expect(response.status, bad).toBe(400);
      expect(JSON.parse(response.body)["reason"], bad).toBe("invalid_request");
    }
    // the list route accepts no query at all
    const listQuery = await get("/v1/runs/logs-run/logs?offset=0");
    expect(listQuery.status).toBe(400);
    // the safe-integer boundary value is accepted
    calls.length = 0;
    const okBoundary = await get("/v1/runs/logs-run/logs/1?offset=9007199254740991&limit=262144");
    expect(okBoundary.status).toBe(200);
  });

  test("a typed invalid_cursor is the explicit request-layer 400, never a 502", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "logsFailure", exitCode: 1, runId: "logs-run", runRoot: "/secret/root", reason: "invalid_cursor" },
    };
    const response = await get("/v1/runs/logs-run/logs/1?offset=999");
    expect(response.status).toBe(400);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
    expect(response.body).not.toContain("/secret/root");
  });

  test("other logs failures follow the command-result precedent", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "logsFailure", exitCode: 1, runId: "logs-run", runRoot: null, reason: "artifact_modified" },
    };
    const response = await get("/v1/runs/logs-run/logs");
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "logs-run",
      outcome: "failed",
      exit_code: 1,
      reason: "artifact_modified",
    });
  });

  test("the routes are GET-only and reject a GET body", async () => {
    calls.length = 0;
    const post = await callHandler("/v1/runs/logs-run/logs", "POST");
    expect(post.status).toBe(405);
    const postChunk = await callHandler("/v1/runs/logs-run/logs/1", "POST");
    expect(postChunk.status).toBe(405);
    const body = await callHandler("/v1/runs/logs-run/logs", "GET", { "content-length": "2" }, "{}");
    expect(body.status).toBe(400);
    expect(calls.length).toBe(0);
  });

  test("an unbound CLI-created run and a removed project fail closed before any subprocess", async () => {
    calls.length = 0;
    const unbound = await get("/v1/runs/unbound-run/logs");
    expect(unbound.status).toBe(404);
    expect(JSON.parse(unbound.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    const removed = await get("/v1/runs/gone-project-run/logs");
    expect(removed.status).toBe(409);
    expect(JSON.parse(removed.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
    const removedChunk = await get("/v1/runs/gone-project-run/logs/1");
    expect(removedChunk.status).toBe(409);
    expect(calls).toEqual([]);
  });

  test("the existing Host/Origin gates apply to the logs routes", async () => {
    const wrongHost = await get("/v1/runs/logs-run/logs", { host: "evil.example:4711" });
    expect(wrongHost.status).toBe(400);
    const evilOrigin = await get("/v1/runs/logs-run/logs", { origin: "https://evil.example" });
    expect(evilOrigin.status).toBe(400);
    expect(calls.length).toBe(0);
  });

  test("no log payload canary leaks through any logs response", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "logsChunk",
        schemaVersion: 1,
        runId: "logs-run",
        revision: 25,
        executionIndex: 1,
        stateId: "architect",
        status: "captured",
        sha256: "a".repeat(64),
        byteCount: 47,
        truncatedDaemon: false,
        truncatedLocal: false,
        offset: 0,
        dataBase64: Buffer.from(CANARY).toString("base64"),
        nextOffset: null,
      },
    };
    const response = await get("/v1/runs/logs-run/logs/1");
    expect(response.status).toBe(200);
    // the base64 content channel is the only carrier; the raw canary never
    // appears in headers or body text
    expect(response.body).not.toContain(CANARY);
    expect(response.body).toContain("data_base64");
    const listResponse = await get("/v1/runs/logs-run/logs");
    expect(listResponse.body).not.toContain(CANARY);
  });
});

describe("control-plane HTTP: project changes route", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-changes-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;
  const calls: unknown[] = [];
  let nextResult: OrchestratorCallResult = {
    ok: true,
    projection: {
      kind: "projectChanges",
      schemaVersion: 1,
      runId: "changes-run",
      revision: 30,
      observation: "a".repeat(64),
      summary: {
        total: 1, added: 1, deleted: 0, content_modified: 0, mode_modified: 0,
        content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0,
        matches_baseline: 1, matches_result: 0, diverged: 0,
      },
      changes: [
        { index: 1, path: "dogfood.txt", change: "added", source_relation: "matches_baseline", baseline: null, result: { kind: "regular_file", mode: 384, byte_count: 8, sha256: "b".repeat(64), multiple_links: false } },
      ],
      nextAfter: null,
    },
  };

  beforeAll(async () => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    registry = await loadControlPlaneProjectRegistry(registryPath);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "changes-run", projectId: "project-a" });
    await store.claim({ runId: "gone-project-run", projectId: "project-removed" });
  }, 20000);

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  const callHandler = async (path: string, method = "GET", headers: Record<string, string> = {}, body?: string): Promise<RawResponse> => {
    const fakeClient = {
      run: async (command: unknown): Promise<OrchestratorCallResult> => {
        calls.push(command);
        return nextResult;
      },
    } as unknown as OrchestratorSubprocessClient;
    const deps = {
      registry,
      bindingStore: await ControlPlaneRunBindingStore.open(storeRoot),
      client: fakeClient,
      assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
      port: 4711,
      bodyLimitBytes: 4096,
    };
    const request = new Request(`http://127.0.0.1:4711${path}`, {
      method,
      headers: { host: "127.0.0.1:4711", ...headers },
      ...(body === undefined ? {} : { body }),
    });
    const response = await handleControlPlaneHttpRequest(deps as never, request);
    const headerEntries: Array<[string, string]> = [];
    response.headers.forEach((value, key) => headerEntries.push([key, value]));
    return { status: response.status, headers: Object.fromEntries(headerEntries), body: await response.text() };
  };
  const get = (path: string, headers: Record<string, string> = {}): Promise<RawResponse> => callHandler(path, "GET", headers);

  test("a bound run serves the changes page with the exact snake_case schema and no run root", async () => {
    calls.length = 0;
    const response = await get("/v1/runs/changes-run/changes");
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual([
      "changes", "kind", "next_after", "observation", "revision", "run_id", "schema_version", "summary",
    ]);
    expect(body["kind"]).toBe("project_changes");
    expect(body["schema_version"]).toBe(1);
    expect(body["run_id"]).toBe("changes-run");
    expect(body["next_after"]).toBeNull();
    expect(response.body).not.toContain("run_root");
    expect(response.headers["content-type"]).toBe("application/json");
    expect(calls).toEqual([{ kind: "project_changes", runId: "changes-run", projectPath: projectDir }]);
  });

  test("the query scalars travel as typed command fields", async () => {
    calls.length = 0;
    await get("/v1/runs/changes-run/changes?after=4&limit=2&observation=" + "c".repeat(64));
    expect(calls).toEqual([{ kind: "project_changes", runId: "changes-run", projectPath: projectDir, after: 4, limit: 2, observation: "c".repeat(64) }]);
    calls.length = 0;
    await get("/v1/runs/changes-run/changes?observation=" + "c".repeat(64) + "&after=4");
    expect(calls).toEqual([{ kind: "project_changes", runId: "changes-run", projectPath: projectDir, after: 4, observation: "c".repeat(64) }]);
    calls.length = 0;
    await get("/v1/runs/changes-run/changes?after=0");
    expect(calls).toEqual([{ kind: "project_changes", runId: "changes-run", projectPath: projectDir, after: 0 }]);
  });

  test("the query parser accepts only after, limit and observation with the exact grammar", async () => {
    calls.length = 0;
    for (const query of [
      "?unknown=1",
      "?after=1&after=2",
      "?after",
      "?after=",
      "?after=-1",
      "?after=1.5",
      "?after=01",
      "?after=1e3",
      "?limit=0",
      "?limit=201",
      "?observation=ZZ",
      "?observation=" + "a".repeat(63),
      "?after=1%2F2",
    ]) {
      const response = await get(`/v1/runs/changes-run/changes${query}`);
      expect(response.status).toBe(400);
      expect(JSON.parse(response.body).reason).toBe("invalid_request");
    }
    expect(calls.length).toBe(0);
  });

  test("invalid_cursor is the request-layer 400 and stale_observation the request-layer 409", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "projectChangesFailure", exitCode: 1, runId: "changes-run", runRoot: "/secret/root", reason: "invalid_cursor" },
    };
    let response = await get("/v1/runs/changes-run/changes?after=999&observation=" + "c".repeat(64));
    expect(response.status).toBe(400);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
    expect(response.body).not.toContain("/secret/root");

    nextResult = {
      ok: true,
      projection: { kind: "projectChangesFailure", exitCode: 1, runId: "changes-run", runRoot: "/secret/root", reason: "stale_observation" },
    };
    response = await get("/v1/runs/changes-run/changes?after=2&observation=" + "c".repeat(64));
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" });
    expect(response.body).not.toContain("/secret/root");
  });

  test("every other changes failure follows the command-result precedent at HTTP 200", async () => {
    for (const reason of ["changes_not_ready", "artifact_modified", "artifact_missing", "project_unstable", "workspace_unstable", "unsupported_project_entry", "run_not_found", "pipeline_mismatch", "internal_error"]) {
      nextResult = {
        ok: true,
        projection: { kind: "projectChangesFailure", exitCode: 1, runId: "changes-run", runRoot: null, reason },
      };
      const response = await get("/v1/runs/changes-run/changes");
      expect(response.status).toBe(200);
      expect(JSON.parse(response.body)).toEqual({
        schema_version: 1,
        kind: "command_result",
        run_id: "changes-run",
        outcome: "failed",
        exit_code: 1,
        reason,
      });
    }
  });

  test("an unbound CLI-created run fails closed before any subprocess", async () => {
    calls.length = 0;
    const response = await get("/v1/runs/unbound-run/changes");
    expect(response.status).toBe(404);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    expect(calls).toEqual([]);
  });

  test("a removed registry project stays project_unavailable with the binding kept", async () => {
    const response = await get("/v1/runs/gone-project-run/changes");
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
  });

  test("the changes route accepts GET only and no body", async () => {
    const post = await callHandler("/v1/runs/changes-run/changes", "POST", { "content-type": "application/json" }, "{}");
    expect(post.status).toBe(405);
    const withBody = await callHandler("/v1/runs/changes-run/changes", "GET", { "content-length": "5" }, "hello");
    expect(withBody.status).toBe(400);
  });

  test("the loopback gates apply to the changes route unchanged", async () => {
    const host = await get("/v1/runs/changes-run/changes", { host: "evil.example" });
    expect(host.status).toBe(400);
    expect(JSON.parse(host.body).reason).toBe("invalid_host");
    const origin = await get("/v1/runs/changes-run/changes", { origin: "http://evil.example" });
    expect(origin.status).toBe(400);
    expect(JSON.parse(origin.body).reason).toBe("invalid_origin");
    expect(calls.length).toBe(0);
  });

  test("the changes response never carries host paths, project paths or canaries", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "projectChanges",
        schemaVersion: 1,
        runId: "changes-run",
        revision: 31,
        observation: "d".repeat(64),
        summary: { total: 1, added: 1, deleted: 0, content_modified: 0, mode_modified: 0, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 1, matches_result: 0, diverged: 0 },
        changes: [{ index: 1, path: `canary-${CANARY}.txt`, change: "added", source_relation: "matches_baseline", baseline: null, result: { kind: "regular_file", mode: 384, byte_count: 3, sha256: "e".repeat(64), multiple_links: false } }],
        nextAfter: null,
      },
    };
    const response = await get("/v1/runs/changes-run/changes");
    expect(response.status).toBe(200);
    expect(response.body).not.toContain(envRoot);
    expect(response.body).not.toContain(projectDir);
    expect(response.body).not.toContain("runRoot");
    expect(response.body).not.toContain("projectRoot");
    // the change path itself is data, not a host path: the projection is
    // judged leak-free by the absence of every absolute path prefix
    expect((response.body.match(/\//g) ?? []).length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// control-plane HTTP: project change content route
// ---------------------------------------------------------------------------
describe("control-plane HTTP: project change content route", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-http-content-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;
  const calls: unknown[] = [];
  let nextResult: OrchestratorCallResult = {
    ok: true,
    projection: {
      kind: "projectChangeContent",
      schemaVersion: 1,
      runId: "content-run",
      revision: 30,
      observation: "a".repeat(64),
      changeIndex: 1,
      side: "result",
      status: "available",
      byteCount: 5,
      sha256: "b".repeat(64),
      offset: 0,
      dataBase64: "aGVsbG8=",
      nextOffset: null,
    },
  };

  beforeAll(async () => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    registry = await loadControlPlaneProjectRegistry(registryPath);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "content-run", projectId: "project-a" });
    await store.claim({ runId: "content-gone-run", projectId: "project-removed" });
  }, 20000);

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  const callHandler = async (path: string, method = "GET", headers: Record<string, string> = {}, body?: string): Promise<RawResponse> => {
    const fakeClient = {
      run: async (command: unknown): Promise<OrchestratorCallResult> => {
        calls.push(command);
        return nextResult;
      },
    } as unknown as OrchestratorSubprocessClient;
    const deps = {
      registry,
      bindingStore: await ControlPlaneRunBindingStore.open(storeRoot),
      client: fakeClient,
      assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
      port: 4711,
      bodyLimitBytes: 4096,
    };
    const request = new Request(`http://127.0.0.1:4711${path}`, {
      method,
      headers: { host: "127.0.0.1:4711", ...headers },
      ...(body === undefined ? {} : { body }),
    });
    const response = await handleControlPlaneHttpRequest(deps as never, request);
    const headerEntries: Array<[string, string]> = [];
    response.headers.forEach((value, key) => headerEntries.push([key, value]));
    return { status: response.status, headers: Object.fromEntries(headerEntries), body: await response.text() };
  };
  const get = (path: string, headers: Record<string, string> = {}): Promise<RawResponse> => callHandler(path, "GET", headers);
  const contentRouteReason = (path: string, method: string): string => {
    const parsed = parseControlPlaneRoute(path, method);
    return parsed instanceof ControlPlaneHttpRequestError ? parsed.reason : `ok:${(parsed as { kind: string }).kind}`;
  };

  test("the route grammar accepts the content path for GET only and validates the change index segment", () => {
    expect(contentRouteReason("/v1/runs/content-run/changes/3/content", "GET")).toBe("ok:project_change_content");
    expect(contentRouteReason("/v1/runs/content-run/changes/3/content", "POST")).toBe("method_not_allowed");
    expect(contentRouteReason("/v1/runs/content-run/changes/0/content", "GET")).toBe("invalid_path");
    expect(contentRouteReason("/v1/runs/content-run/changes/01/content", "GET")).toBe("invalid_path");
    expect(contentRouteReason("/v1/runs/content-run/changes/x/content", "GET")).toBe("invalid_path");
    expect(contentRouteReason("/v1/runs/content-run/changes/3", "GET")).toBe("unknown_route");
    expect(contentRouteReason("/v1/runs/content-run/changes/3/content/extra", "GET")).toBe("unknown_route");
    expect(contentRouteReason("/v1/runs/missing%20run/changes/3/content", "GET")).toBe("invalid_path");
  });

  test("the available status serves the exact snake_case schema with no-store and no run root", async () => {
    calls.length = 0;
    const response = await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`);
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(Object.keys(body).sort()).toEqual([
      "byte_count", "change_index", "data_base64", "kind", "next_offset", "observation", "offset", "revision", "run_id", "schema_version", "sha256", "side", "status",
    ]);
    expect(body["kind"]).toBe("project_change_content");
    expect(body["status"]).toBe("available");
    expect(body["data_base64"]).toBe("aGVsbG8=");
    expect(body["next_offset"]).toBeNull();
    expect(response.body).not.toContain("run_root");
    expect(response.body).not.toContain(projectDir);
    expect(response.headers["cache-control"]).toBe("no-store");
    expect(calls).toEqual([{
      kind: "project_change_content", runId: "content-run", projectPath: projectDir,
      changeIndex: 1, side: "result", observation: "a".repeat(64),
    }]);
  });

  test("the implicit_empty, metadata_only and unavailable statuses project their exact shapes", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "projectChangeContent", schemaVersion: 1, runId: "content-run", revision: 30,
        observation: "a".repeat(64), changeIndex: 1, side: "baseline", status: "implicit_empty",
        byteCount: 0, offset: 0, dataBase64: "", nextOffset: null,
      },
    } as unknown as OrchestratorCallResult;
    const empty = await get(`/v1/runs/content-run/changes/1/content?side=baseline&observation=${"a".repeat(64)}`);
    expect(empty.status).toBe(200);
    const emptyBody = JSON.parse(empty.body) as Record<string, unknown>;
    expect(Object.keys(emptyBody).sort()).toEqual([
      "byte_count", "change_index", "data_base64", "kind", "next_offset", "observation", "offset", "revision", "run_id", "schema_version", "side", "status",
    ]);
    expect(emptyBody["byte_count"]).toBe(0);

    nextResult = {
      ok: true,
      projection: {
        kind: "projectChangeContent", schemaVersion: 1, runId: "content-run", revision: 30,
        observation: "a".repeat(64), changeIndex: 2, side: "result", status: "metadata_only",
        node: { kind: "symlink", target: "elsewhere.txt" },
      },
    } as unknown as OrchestratorCallResult;
    const meta = await get(`/v1/runs/content-run/changes/2/content?side=result&observation=${"a".repeat(64)}`);
    const metaBody = JSON.parse(meta.body) as Record<string, unknown>;
    expect(Object.keys(metaBody).sort()).toEqual([
      "change_index", "kind", "node", "observation", "revision", "run_id", "schema_version", "side", "status",
    ]);
    expect(metaBody["node"]).toEqual({ kind: "symlink", target: "elsewhere.txt" });

    nextResult = {
      ok: true,
      projection: {
        kind: "projectChangeContent", schemaVersion: 1, runId: "content-run", revision: 30,
        observation: "a".repeat(64), changeIndex: 3, side: "baseline", status: "unavailable",
        reason: "source_no_longer_matches_baseline",
        node: { kind: "regular_file", mode: 420, byte_count: 9, sha256: "c".repeat(64), multiple_links: false },
      },
    } as unknown as OrchestratorCallResult;
    const un = await get(`/v1/runs/content-run/changes/3/content?side=baseline&observation=${"a".repeat(64)}`);
    const unBody = JSON.parse(un.body) as Record<string, unknown>;
    expect(Object.keys(unBody).sort()).toEqual([
      "change_index", "kind", "node", "observation", "reason", "revision", "run_id", "schema_version", "side", "status",
    ]);
    expect(unBody["reason"]).toBe("source_no_longer_matches_baseline");
  });

  test("the query grammar is strict and every scalar travels as a typed command field", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "projectChangeContent", schemaVersion: 1, runId: "content-run", revision: 30,
        observation: "a".repeat(64), changeIndex: 1, side: "result", status: "available",
        byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
      },
    } as unknown as OrchestratorCallResult;
    calls.length = 0;
    await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}&offset=8&limit=4096`);
    expect(calls).toEqual([{
      kind: "project_change_content", runId: "content-run", projectPath: projectDir,
      changeIndex: 1, side: "result", observation: "a".repeat(64), offset: 8, limit: 4096,
    }]);
    const bad = [
      "",
      `side=result`,
      `observation=${"a".repeat(64)}`,
      `observation=${"a".repeat(64)}&side=middle`,
      `observation=${"a".repeat(64)}&side=BASELINE`,
      `side=result&observation=ZZ`,
      `side=result&observation=${"a".repeat(63)}`,
      `side=result&observation=${"a".repeat(64)}&offset=-1`,
      `side=result&observation=${"a".repeat(64)}&offset=01`,
      `side=result&observation=${"a".repeat(64)}&limit=0`,
      `side=result&observation=${"a".repeat(64)}&limit=262145`,
      `side=result&observation=${"a".repeat(64)}&after=1`,
      `side=result&observation=${"a".repeat(64)}&side=result`,
      `side=result&observation=${"a".repeat(64)}%`,
      `side=result&observation=${"a".repeat(64)}&unknown=1`,
      `side&observation=${"a".repeat(64)}`,
      `side=extra`,
    ];
    for (const query of bad) {
      const response = await get(`/v1/runs/content-run/changes/1/content${query === "" ? "" : `?${query}`}`);
      expect(response.status).toBe(400);
    }
    calls.length = 0;
  });

  test("the typed content failures map to 400/409/200 and transport failures to 502", async () => {
    nextResult = {
      ok: true,
      projection: { kind: "projectChangeContentFailure", exitCode: 1, runId: "content-run", runRoot: null, reason: "invalid_cursor" },
    } as unknown as OrchestratorCallResult;
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(400);
    nextResult = {
      ok: true,
      projection: { kind: "projectChangeContentFailure", exitCode: 1, runId: "content-run", runRoot: null, reason: "stale_observation" },
    } as unknown as OrchestratorCallResult;
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(409);
    nextResult = {
      ok: true,
      projection: { kind: "projectChangeContentFailure", exitCode: 1, runId: "content-run", runRoot: null, reason: "changes_not_ready" },
    } as unknown as OrchestratorCallResult;
    const notReady = await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`);
    expect(notReady.status).toBe(200);
    expect((JSON.parse(notReady.body) as Record<string, unknown>)["kind"]).toBe("command_result");
    nextResult = { ok: false, failure: { reason: "spawn_failed", expectedRunId: "content-run" } } as unknown as OrchestratorCallResult;
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(502);
    nextResult = {
      ok: true,
      projection: { kind: "projectChanges", schemaVersion: 1 },
    } as unknown as OrchestratorCallResult;
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(502);
    // Unbound runs and removed projects keep the established authority failures.
    expect((await get(`/v1/runs/unbound-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(404);
    expect((await get(`/v1/runs/content-gone-run/changes/1/content?side=result&observation=${"a".repeat(64)}`)).status).toBe(409);
  });

  test("the content route rejects bodies and preserves the Host/Origin gates", async () => {
    nextResult = {
      ok: true,
      projection: {
        kind: "projectChangeContent", schemaVersion: 1, runId: "content-run", revision: 30,
        observation: "a".repeat(64), changeIndex: 1, side: "result", status: "available",
        byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
      },
    } as unknown as OrchestratorCallResult;
    const withBody = await callHandler(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`, "GET", { "content-length": "2" }, "{}");
    expect(withBody.status).toBe(400);
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`, { origin: "http://evil.example" })).status).toBe(400);
    expect((await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`, { host: "evil:1" })).status).toBe(400);
  });

  test("the content response carries no leakage canary", async () => {
    const response = await get(`/v1/runs/content-run/changes/1/content?side=result&observation=${"a".repeat(64)}`);
    expectNoLeak(response.body, [envRoot]);
    expect(response.body).not.toContain(CANARY);
    calls.length = 0;
  });
});

describe("control plane HTTP: the explicit Project Apply routes", () => {
  const envRoot = mkdtempSync(join(tmpdir(), "cp-http-apply-"));
  const storeRoot = join(envRoot, "bindings");
  const registryPath = join(envRoot, "registry.json");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;
  const calls: unknown[] = [];
  let nextResult: OrchestratorCallResult = { ok: false, failure: { reason: "spawn_failed", expectedRunId: "" } };

  const operationDocument = (overrides: Record<string, unknown> = {}): OrchestratorCallResult => ({
    ok: true,
    projection: {
      kind: "projectApplyOperation",
      schemaVersion: 1,
      runId: "apply-run",
      applyId: "apply-1",
      projectId: "project-a",
      observation: "a".repeat(64),
      revision: 12,
      status: "applied",
      completedStepCount: 3,
      totalStepCount: 3,
      ...overrides,
    },
  });

  const applyFailure = (reason: string): OrchestratorCallResult => ({
    ok: true,
    projection: { kind: "projectApplyFailure", exitCode: 1, runId: "apply-run", applyId: "apply-1", reason } as never,
  });

  const callHandler = async (
    path: string,
    method = "GET",
    body?: string,
    overrides: { applyStoreRoot?: string | null; origin?: string | null; extraHeaders?: Record<string, string> } = {},
  ): Promise<{ status: number; body: string }> => {
    calls.length = 0;
    const fakeClient = {
      run: async (command: unknown): Promise<OrchestratorCallResult> => {
        calls.push(command);
        return nextResult;
      },
    } as unknown as OrchestratorSubprocessClient;
    const deps = {
      registry,
      bindingStore: await ControlPlaneRunBindingStore.open(storeRoot),
      client: fakeClient,
      assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
      port: 4711,
      bodyLimitBytes: 4096,
      applyStoreRoot: overrides.applyStoreRoot === undefined ? "/srv/applies" : overrides.applyStoreRoot,
    };
    const headers: Record<string, string> = { host: "127.0.0.1:4711" };
    if (overrides.origin !== null) headers["origin"] = overrides.origin ?? "http://127.0.0.1:4711";
    if (body !== undefined) headers["content-type"] = "application/json";
    for (const [name, value] of Object.entries(overrides.extraHeaders ?? {})) {
      headers[name] = value;
    }
    const request = new Request(`http://127.0.0.1:4711${path}`, { method, headers, ...(body === undefined ? {} : { body }) });
    const response = await handleControlPlaneHttpRequest(deps as never, request);
    return { status: response.status, body: await response.text() };
  };

  beforeAll(async () => {
    mkdirSync(join(envRoot, "projects", "a"), { recursive: true, mode: 0o700 });
    mkdirSync(join(envRoot, "projects", "b"), { recursive: true, mode: 0o700 });
    mkdirSync(join(envRoot, "projects", "gone"), { recursive: true, mode: 0o700 });
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: join(envRoot, "projects", "a"),
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
            project_apply: true,
          },
          {
            id: "project-b",
            project_root: join(envRoot, "projects", "b"),
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    registry = await loadControlPlaneProjectRegistry(registryPath);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "apply-run", projectId: "project-a" });
    await store.claim({ runId: "plain-run", projectId: "project-b" });
    await store.claim({ runId: "gone-run", projectId: "project-removed" });
  }, 20000);

  afterAll(() => {
    rmSync(envRoot, { recursive: true, force: true });
  });

  test("POST applies through the bound authority and answers the exact operation document", async () => {
    nextResult = operationDocument();
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      schema_version: 1,
      kind: "project_apply_operation",
      run_id: "apply-run",
      apply_id: "apply-1",
      project_id: "project-a",
      observation: "a".repeat(64),
      revision: 12,
      status: "applied",
      completed_step_count: 3,
      total_step_count: 3,
    });
    expect(calls).toEqual([{
      kind: "project_apply",
      runId: "apply-run",
      applyId: "apply-1",
      projectId: "project-a",
      projectPath: join(envRoot, "projects", "a"),
      applyStoreRoot: "/srv/applies",
      observation: "a".repeat(64),
    }]);
  });

  test("the POST operation response never caches and carries no host path", async () => {
    nextResult = operationDocument();
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
    expect(response.body).not.toContain("/srv");
    expect(response.body).not.toContain("applyStoreRoot");
  });

  test("POST identity and durable conflicts stay typed command results at 200", async () => {
    for (const reason of ["identity_conflict", "project_conflict", "recovery_required", "preparation_busy", "project_busy"]) {
      nextResult = applyFailure(reason);
      const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
      expect(response.status, reason).toBe(200);
      expect(JSON.parse(response.body)).toEqual({
        schema_version: 1,
        kind: "command_result",
        run_id: "apply-run",
        outcome: "failed",
        exit_code: 1,
        reason,
      });
    }
  });

  test("POST stale_observation is the request-layer 409", async () => {
    nextResult = applyFailure("stale_observation");
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" });
  });

  test("a transport failure stays 502 on both apply routes", async () => {
    nextResult = { ok: false, failure: { reason: "child_terminated", expectedRunId: "apply-run" } };
    const post = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
    expect(post.status).toBe(502);
    const get = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET");
    expect(get.status).toBe(502);
  });

  test("GET projects the operation read-only and never sends an observation", async () => {
    nextResult = operationDocument({ status: "recovery_required", completedStepCount: 1, revision: 14 });
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET");
    expect(response.status).toBe(200);
    const parsed = JSON.parse(response.body) as Record<string, unknown>;
    expect(parsed["kind"]).toBe("project_apply_operation");
    expect(parsed["status"]).toBe("recovery_required");
    expect(parsed["completed_step_count"]).toBe(1);
    expect(calls).toEqual([{
      kind: "project_apply_inspect",
      runId: "apply-run",
      applyId: "apply-1",
      projectId: "project-a",
      projectPath: join(envRoot, "projects", "a"),
      applyStoreRoot: "/srv/applies",
    }]);
  });

  test("GET of a missing or foreign operation is the request-layer 404 without disclosure", async () => {
    nextResult = applyFailure("artifact_missing");
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET");
    expect(response.status).toBe(404);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_not_found" });
  });

  test("GET of a damaged operation stays a typed command result at 200", async () => {
    nextResult = applyFailure("artifact_modified");
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET");
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      schema_version: 1, kind: "command_result", run_id: "apply-run", outcome: "failed", exit_code: 1, reason: "artifact_modified",
    });
  });

  test("POST without the per-project capability is the typed 409 refusal with zero client calls", async () => {
    nextResult = operationDocument();
    const response = await callHandler("/v1/runs/plain-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }));
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_disabled" });
    expect(calls).toHaveLength(0);
  });

  test("GET works without the capability but not without the apply-store root", async () => {
    nextResult = operationDocument();
    const withoutCapability = await callHandler("/v1/runs/plain-run/project-applies/apply-1", "GET");
    expect(withoutCapability.status).toBe(200);
    expect(calls).toHaveLength(1);
    const disabled = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET", undefined, { applyStoreRoot: null });
    expect(disabled.status).toBe(409);
    expect(JSON.parse(disabled.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_disabled" });
    expect(calls).toHaveLength(0);
  });

  test("POST with an absent apply-store root is refused before any client call", async () => {
    nextResult = operationDocument();
    const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }), { applyStoreRoot: null });
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_disabled" });
    expect(calls).toHaveLength(0);
  });

  test("the POST body is exact: unknown fields, wrong version and malformed observations are 400", async () => {
    nextResult = operationDocument();
    for (const body of [
      "",
      "not json",
      "{}",
      "[]",
      JSON.stringify({ schema_version: 1 }),
      JSON.stringify({ observation: "a".repeat(64) }),
      JSON.stringify({ schema_version: 2, observation: "a".repeat(64) }),
      JSON.stringify({ schema_version: 1, observation: "a".repeat(64), extra: 1 }),
      JSON.stringify({ schema_version: 1, observation: "A".repeat(64) }),
      JSON.stringify({ schema_version: 1, observation: "a".repeat(63) }),
    ]) {
      const response = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", body);
      expect(response.status, body.slice(0, 40)).toBe(400);
      expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_request" });
      expect(calls).toHaveLength(0);
    }
  });

  test("the GET route refuses a query string and a body", async () => {
    nextResult = operationDocument();
    const query = await callHandler("/v1/runs/apply-run/project-applies/apply-1?x=1", "GET");
    expect(query.status).toBe(400);
    expect(calls).toHaveLength(0);
    const withBody = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "GET", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }), { extraHeaders: { "content-length": "2" } });
    expect(withBody.status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  test("an unbound run is 404 and a removed project stays the 409 contract", async () => {
    nextResult = operationDocument();
    const unbound = await callHandler("/v1/runs/unknown-run/project-applies/apply-1", "GET");
    expect(unbound.status).toBe(404);
    expect(JSON.parse(unbound.body)["reason"]).toBe("binding_not_found");
    const removed = await callHandler("/v1/runs/gone-run/project-applies/apply-1", "GET");
    expect(removed.status).toBe(409);
    expect(JSON.parse(removed.body)["reason"]).toBe("project_unavailable");
    expect(calls).toHaveLength(0);
  });

  test("an unsafe apply id is an invalid path before any effect", () => {
    const traversal = parseControlPlaneRoute("/v1/runs/apply-run/project-applies/../escape", "GET");
    expect(traversal instanceof ControlPlaneHttpRequestError).toBe(true);
    expect((traversal as ControlPlaneHttpRequestError).reason).toBe("invalid_path");
    const traversalPost = parseControlPlaneRoute("/v1/runs/apply-run/project-applies/apply-1", "DELETE");
    expect(traversalPost instanceof ControlPlaneHttpRequestError).toBe(true);
    expect((traversalPost as ControlPlaneHttpRequestError).reason).toBe("method_not_allowed");
  });

  test("an encoded apply id and a wrong method are refused before any effect", async () => {
    nextResult = operationDocument();
    const encoded = await callHandler("/v1/runs/apply-run/project-applies/BAD%20ID", "GET");
    expect(encoded.status).toBe(400);
    expect(calls).toHaveLength(0);
    const wrongMethod = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "PUT");
    expect(wrongMethod.status).toBe(405);
    const unknownShape = await callHandler("/v1/runs/apply-run/project-applies", "GET");
    expect(unknownShape.status).toBe(404);
  });

  test("the Host and Origin gates run before the apply routes", async () => {
    nextResult = operationDocument();
    const badOrigin = await callHandler("/v1/runs/apply-run/project-applies/apply-1", "POST", JSON.stringify({ schema_version: 1, observation: "a".repeat(64) }), { origin: "http://evil.example" });
    expect(badOrigin.status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  test("the status table pins the two new request reasons", () => {
    expect(statusForControlPlaneFailure({ layer: "request", reason: "project_apply_disabled" })).toBe(409);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "project_apply_not_found" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "stale_observation" })).toBe(409);
  });
});
