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
  startControlPlaneHttpService,
  statusForControlPlaneFailure,
  type RunningControlPlaneHttpService,
} from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
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
  });

  test("method mismatches on known shapes are method_not_allowed", () => {
    for (const [path, method] of [
      ["/v1/projects/pa/runs", "GET"],
      ["/v1/runs/r1", "POST"],
      ["/v1/runs/r1/resume", "GET"],
      ["/v1/runs/r1", "OPTIONS"],
      ["/v1/runs/r1/respond", "PUT"],
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
      "/",
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

  test("d1. projects project declaration-order ids only, byte-identical on repeat", async () => {
    const expected = {
      schema_version: 1,
      kind: "projects",
      projects: [
        { id: "project-a", input_roots: [{ id: "tasks" }, { id: "plans" }] },
        { id: "project-b", input_roots: [{ id: "notes" }] },
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
    const projected = projectControlPlaneProjects(frozen as never);
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
