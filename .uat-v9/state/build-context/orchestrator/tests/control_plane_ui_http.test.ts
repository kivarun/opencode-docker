/**
 * Permanent contract suite of the browser UI routes of the control-plane
 * HTTP service.
 *
 * Matrix: A the three UI routes with exact content types, B the fixed
 * security headers and Cache-Control on every UI response, C the
 * loopback gates and path grammar on the UI routes, D foreign methods,
 * E assets captured once (no per-request disk access), F the existing
 * JSON routes unchanged, G the leakage battery.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { request as httpRequest, type IncomingHttpHeaders } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  handleControlPlaneHttpRequest,
  parseControlPlaneRoute,
  startControlPlaneHttpService,
  type ControlPlaneHttpServiceDeps,
  type RunningControlPlaneHttpService,
} from "../src/control_plane/http_server.ts";
import { defaultControlPlaneUiRoot, loadControlPlaneUiAssets } from "../src/control_plane/ui_assets.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";

const CANARY = "uihttpcanaryhostpath";

const CSP = [
  "default-src 'none'",
  "script-src 'self'",
  "style-src 'self'",
  "connect-src 'self'",
  "img-src 'self'",
  "base-uri 'none'",
  "frame-ancestors 'none'",
  "form-action 'self'",
  "object-src 'none'",
].join("; ");

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

function expectUiSecurityHeaders(headers: IncomingHttpHeaders): void {
  expect(headers["content-security-policy"]).toBe(CSP);
  expect(headers["x-content-type-options"]).toBe("nosniff");
  expect(headers["referrer-policy"]).toBe("no-referrer");
  expect(headers["cross-origin-opener-policy"]).toBe("same-origin");
  expect(headers["cross-origin-resource-policy"]).toBe("same-origin");
  expect(headers["cache-control"]).toBe("no-store");
}

function errorBody(body: RawResponse): { readonly layer: string; readonly reason: string } {
  const parsed = JSON.parse(body.body) as Record<string, unknown>;
  expect(parsed["schema_version"]).toBe(1);
  expect(parsed["kind"]).toBe("error");
  return { layer: parsed["layer"] as string, reason: parsed["reason"] as string };
}

describe("control-plane HTTP: browser UI routes", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-ui-http-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  const projectDir = join(envRoot, "project");
  const configRoot = join(envRoot, "config");
  const inputRoot = join(envRoot, "inputs");
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let hostHeader = "";

  const get = (path: string, extra: Record<string, string> = {}): Promise<RawResponse> =>
    rawRequest(port, "GET", path, { host: hostHeader, ...extra });

  beforeAll(async () => {
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(configRoot, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: configRoot,
            pipeline_root: null,
            launcher_id: null,
            input_roots: [{ id: "tasks", root: inputRoot }],
          },
        ],
      }),
      { mode: 0o600 },
    );
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    await store.claim({ runId: "run-a", projectId: "project-a" });
    service = await startControlPlaneHttpService({
      registryPath,
      bindingStoreRoot: storeRoot,
      client: {
        bunExecutable: process.execPath,
        orchestratorEntrypoint: join(envRoot, "missing-entrypoint.ts"),
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
    hostHeader = `127.0.0.1:${port}`;
  }, 20000);

  afterAll(() => {
    service?.stop();
    rmSync(envRoot, { recursive: true, force: true });
  });

  test("u1. the three UI routes serve the exact captured assets with exact content types", async () => {
    const assets = loadControlPlaneUiAssets(defaultControlPlaneUiRoot());
    const document = await get("/");
    expect(document.status).toBe(200);
    expect(document.headers["content-type"]).toBe("text/html; charset=utf-8");
    expect(document.body).toBe(Buffer.from(assets.ui_root.bytes).toString("utf8"));
    const script = await get("/ui/app.js");
    expect(script.status).toBe(200);
    expect(script.headers["content-type"]).toBe("text/javascript; charset=utf-8");
    expect(script.body).toBe(Buffer.from(assets.ui_app_js.bytes).toString("utf8"));
    const style = await get("/ui/style.css");
    expect(style.status).toBe(200);
    expect(style.headers["content-type"]).toBe("text/css; charset=utf-8");
    expect(style.body).toBe(Buffer.from(assets.ui_style_css.bytes).toString("utf8"));
  });

  test("u2. every UI response carries the fixed security headers and no-store", async () => {
    for (const response of [await get("/"), await get("/ui/app.js"), await get("/ui/style.css")]) {
      expectUiSecurityHeaders(response.headers);
    }
  });

  test("u3. the document links the assets as same-origin module and stylesheet", async () => {
    const document = await get("/");
    expect(document.body).toContain('<link rel="stylesheet" href="/ui/style.css">');
    expect(document.body).toContain('<script type="module" src="/ui/app.js"></script>');
    expect(document.body.startsWith("<!DOCTYPE html>")).toBe(true);
  });

  test("u4. repeated asset responses are byte-identical", async () => {
    const first = await get("/");
    expect((await get("/")).body).toBe(first.body);
  });

  test("u5. foreign methods are refused on every UI route", async () => {
    for (const path of ["/", "/ui/app.js", "/ui/style.css"]) {
      for (const method of ["POST", "PUT", "DELETE", "OPTIONS", "HEAD"]) {
        const response = await rawRequest(port, method, path, {
          host: hostHeader,
          ...(method === "POST" ? { "content-type": "application/json" } : {}),
        }, method === "POST" ? "{}" : undefined);
        expect(response.status).toBe(405);
        if (method !== "HEAD") {
          expect(errorBody(response)).toEqual({ layer: "request", reason: "method_not_allowed" });
        }
      }
    }
  });

  test("u6. the loopback gates apply to the UI routes", async () => {
    const wrongHost = await rawRequest(port, "GET", "/", { host: "evil.example" });
    expect(wrongHost.status).toBe(400);
    expect(errorBody(wrongHost)).toEqual({ layer: "request", reason: "invalid_host" });
    const wrongOrigin = await get("/", { origin: "http://evil.example" });
    expect(wrongOrigin.status).toBe(400);
    expect(errorBody(wrongOrigin)).toEqual({ layer: "request", reason: "invalid_origin" });
    const rightOrigin = await get("/", { origin: `http://${hostHeader}` });
    expect(rightOrigin.status).toBe(200);
    expectUiSecurityHeaders(rightOrigin.headers);
  });

  test("u7. the path grammar applies to the UI routes", async () => {
    // dot segments are normalized away by the URL parser before the
    // route parser runs ("/ui/./app.js" literally names the asset
    // route); every other deviation stays refused. The empty first
    // segment ("//") is pinned at the parser level: an HTTP client may
    // normalize it before the request ever reaches the server.
    for (const path of [
      "/?x=1",
      "/ui/app.js?x=1",
      "/ui/app.js/",
      "/ui//app.js",
      "/ui/",
    ]) {
      const response = await get(path);
      expect(response.status).toBe(400);
      expect(errorBody(response).reason).toBe("invalid_path");
    }
    expect((await get("/ui/../app.js")).status).toBe(404);
    expect(parseControlPlaneRoute("//", "GET")).toMatchObject({ reason: "invalid_path" });
    for (const path of ["/ui/other.js", "/index.html", "/ui", "/ui/app.js/extra"]) {
      const response = await get(path);
      expect(response.status).toBe(404);
      expect(errorBody(response).reason).toBe("unknown_route");
    }
  });

  test("u8. a GET with a body is refused on the UI routes", async () => {
    const response = await rawRequest(port, "GET", "/", { host: hostHeader, "content-length": "5" }, "hello");
    expect(response.status).toBe(400);
    expect(errorBody(response)).toEqual({ layer: "request", reason: "invalid_request" });
    expectUiSecurityHeaders(response.headers);
  });

  test("u9. the existing JSON routes are unchanged and carry no UI headers", async () => {
    const projects = JSON.parse((await get("/v1/projects")).body) as Record<string, unknown>;
    expect(projects["kind"]).toBe("projects");
    expect(projects["schema_version"]).toBe(1);
    const runs = JSON.parse((await get("/v1/runs")).body) as Record<string, unknown>;
    expect(runs["kind"]).toBe("runs");
    expect(runs["runs"]).toEqual([{ run_id: "run-a", project_id: "project-a", project_available: true }]);
    const runsResponse = await get("/v1/runs");
    expect(runsResponse.headers["content-type"]).toBe("application/json");
    expect(runsResponse.headers["content-security-policy"]).toBeUndefined();
    expect(runsResponse.headers["cache-control"]).toBeUndefined();
  });

  test("u10. a POST to a UI route is a JSON error, not an asset", async () => {
    const response = await rawRequest(port, "POST", "/", { host: hostHeader, "content-type": "application/json" }, "{}");
    expect(response.status).toBe(405);
    expect(response.headers["content-type"]).toBe("application/json");
    expectUiSecurityHeaders(response.headers);
  });
});

describe("control-plane HTTP: UI assets captured once", () => {
  test("u11. the handler serves the captured assets without any disk access", async () => {
    const root = mkdtempSync(join(tmpdir(), `cp-ui-captured-${CANARY}-`));
    try {
      const uiRoot = join(root, "ui");
      mkdirSync(uiRoot, { mode: 0o700 });
      for (const name of ["index.html", "app.js", "style.css"]) {
        writeFileSync(join(uiRoot, name), readFileSync(join(import.meta.dir, "..", "ui", name)), { mode: 0o600 });
      }
      const assets = loadControlPlaneUiAssets(uiRoot);
      // The source tree disappears: the handler must keep serving the
      // captured bytes.
      rmSync(root, { recursive: true, force: true });
      const deps = {
        registry: { projects: [] },
        bindingStore: {},
        client: {},
        assets,
        port: 4711,
        bodyLimitBytes: 4096,
      } as unknown as ControlPlaneHttpServiceDeps;
      const makeRequest = (path: string, method = "GET"): Request =>
        new Request(`http://127.0.0.1:4711${path}`, { method, headers: { host: "127.0.0.1:4711" } });
      const document = await handleControlPlaneHttpRequest(deps, makeRequest("/"));
      expect(document.status).toBe(200);
      expect(document.headers.get("content-type")).toBe("text/html; charset=utf-8");
      expect(await document.text()).toBe(Buffer.from(assets.ui_root.bytes).toString("utf8"));
      const script = await handleControlPlaneHttpRequest(deps, makeRequest("/ui/app.js"));
      expect(await script.text()).toBe(Buffer.from(assets.ui_app_js.bytes).toString("utf8"));
      const style = await handleControlPlaneHttpRequest(deps, makeRequest("/ui/style.css"));
      expect(await style.text()).toBe(Buffer.from(assets.ui_style_css.bytes).toString("utf8"));
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("control-plane HTTP: UI leakage battery", () => {
  const envRoot = mkdtempSync(join(tmpdir(), `cp-ui-leak-${CANARY}-`));
  const registryPath = join(envRoot, "registry.json");
  const storeRoot = join(envRoot, "bindings");
  let service: RunningControlPlaneHttpService;
  let port = 0;

  beforeAll(async () => {
    mkdirSync(join(envRoot, "config"), { mode: 0o700 });
    mkdirSync(join(envRoot, "project"), { mode: 0o700 });
    mkdirSync(join(envRoot, "inputs"), { mode: 0o700 });
    mkdirSync(storeRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: join(envRoot, "project"),
            config_root: join(envRoot, "config"),
            pipeline_root: null,
            launcher_id: null,
            input_roots: [{ id: "tasks", root: join(envRoot, "inputs") }],
          },
        ],
      }),
      { mode: 0o600 },
    );
    service = await startControlPlaneHttpService({
      registryPath,
      bindingStoreRoot: storeRoot,
      client: {
        bunExecutable: process.execPath,
        orchestratorEntrypoint: join(envRoot, "missing-entrypoint.ts"),
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

  test("u12. no UI response or UI-route error ever carries a host path or HTML/script canary", async () => {
    const secrets = [envRoot, CANARY, join(envRoot, "project"), join(envRoot, "config"), "credential", "launcher_id", "state.json", "pipeline-runs"];
    const responses: RawResponse[] = [];
    for (const path of ["/", "/ui/app.js", "/ui/style.css", "/v1/projects", "/v1/runs", "/v1/runs/missing-run"]) {
      responses.push(await rawRequest(port, "GET", path, { host: `127.0.0.1:${port}` }));
    }
    responses.push(await rawRequest(port, "POST", "/", { host: `127.0.0.1:${port}`, "content-type": "application/json" }, "{}"));
    responses.push(await rawRequest(port, "GET", "/?x=1", { host: `127.0.0.1:${port}` }));
    for (const response of responses) {
      for (const secret of secrets) {
        expect(response.body.includes(secret)).toBe(false);
      }
      expect(response.body.includes("<script>alert")).toBe(false);
      const headerBlob = JSON.stringify(response.headers);
      for (const secret of secrets) {
        expect(headerBlob.includes(secret)).toBe(false);
      }
    }
  });
});
