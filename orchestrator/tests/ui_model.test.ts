/**
 * Permanent contract suite of the browser UI v1 model: the pure,
 * dependency-free ES module `orchestrator/ui/app.js`.
 *
 * Matrix: A document classification (schema_version/kind/shape), B the
 * projects and runs models, C run pagination and refresh, D the inspect
 * run detail, E command results, F error documents, G the fresh-run
 * body, H the lost-response contract, I the one API client (content
 * type, bounded JSON, no header classification, raw response dropped),
 * J hostile content as plain text, K the export surface, L source
 * restrictions.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  type UiApiFailure,
  UiContractError,
  applyRunsPage,
  buildFreshRunBody,
  createRunsState,
  formatFailure,
  freshRunPath,
  isSafeId,
  lostResponseRecovery,
  parseApiDocument,
  requestApiJson,
  runsPagePath,
  type UiApiModel,
} from "../ui/app.js";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");
const htmlSource = readFileSync(join(import.meta.dir, "..", "ui", "index.html"), "utf8");
const cssSource = readFileSync(join(import.meta.dir, "..", "ui", "style.css"), "utf8");

const failureOf = (result: { readonly ok: boolean; readonly failure?: unknown }): UiApiFailure =>
  result.failure as UiApiFailure;

const modelOf = (result: { readonly ok: boolean; readonly model?: unknown }): UiApiModel => result.model as UiApiModel;

const contractReason = (run: () => unknown): string => {
  try {
    run();
  } catch (error) {
    if (error instanceof UiContractError) {
      return error.reason;
    }
    throw error;
  }
  return "accepted";
};

describe("ui model: document classification", () => {
  test("a document that is not an object, has an unknown schema version or an unknown kind is a contract error", () => {
    expect(contractReason(() => parseApiDocument(null))).toBe("not_a_document");
    expect(contractReason(() => parseApiDocument("text"))).toBe("not_a_document");
    expect(contractReason(() => parseApiDocument([1]))).toBe("not_a_document");
    expect(contractReason(() => parseApiDocument({ kind: "runs" }))).toBe("unknown_schema_version");
    expect(contractReason(() => parseApiDocument({ schema_version: 2, kind: "runs" }))).toBe("unknown_schema_version");
    expect(contractReason(() => parseApiDocument({ schema_version: "1", kind: "runs" }))).toBe("unknown_schema_version");
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "mystery" }))).toBe("unknown_kind");
    expect(contractReason(() => parseApiDocument({ schema_version: 1 }))).toBe("unknown_kind");
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "run", extra: 1, run_id: "r", revision: 1, status: "running", boundary: {}, outputs: [] }))).toBe("invalid_shape");
  });

  test("the model is frozen and repeated parses are structurally identical", () => {
    const model = parseApiDocument({ schema_version: 1, kind: "projects", projects: [] });
    expect(Object.isFrozen(model)).toBe(true);
    expect(model).toEqual(parseApiDocument({ projects: [], kind: "projects", schema_version: 1 }));
  });
});

describe("ui model: projects", () => {
  test("projects keep declaration order and input-root ids only", () => {
    const model = parseApiDocument({
      schema_version: 1,
      kind: "projects",
      projects: [
        { id: "project-a", input_roots: [{ id: "tasks" }, { id: "plans" }] },
        { id: "project-b", input_roots: [] },
      ],
    });
    expect(model.kind).toBe("projects");
    expect((model as UiApiModel & { kind: "projects" }).projects).toEqual([
      { id: "project-a", inputRootIds: ["tasks", "plans"] },
      { id: "project-b", inputRootIds: [] },
    ]);
  });

  test("projects reject unknown fields, inherited fields and hostile shapes", () => {
    const inherited = Object.create({ schema_version: 1, kind: "projects", projects: [] });
    expect(contractReason(() => parseApiDocument(inherited))).toBe("invalid_shape");
    for (const bad of [
      { schema_version: 1, kind: "projects", projects: [], extra: 1 },
      { schema_version: 1, kind: "projects" },
      { schema_version: 1, kind: "projects", projects: [{ id: "p" }] },
      { schema_version: 1, kind: "projects", projects: [{ id: "p", input_roots: [{ id: "t", root: "/secret" }] }] },
      { schema_version: 1, kind: "projects", projects: [{ id: "", input_roots: [] }] },
      { schema_version: 1, kind: "projects", projects: "all" },
    ]) {
      expect(contractReason(() => parseApiDocument(bad))).toBe("invalid_shape");
    }
  });
});

describe("ui model: runs pagination and refresh", () => {
  const page = (ids: readonly string[], nextAfter: string | null, available = true) =>
    parseApiDocument({
      schema_version: 1,
      kind: "runs",
      runs: ids.map((id) => ({ run_id: id, project_id: "project-a", project_available: available })),
      next_after: nextAfter,
    }) as UiApiModel & { kind: "runs" };

  test("a page keeps run identity, project availability and the cursor", () => {
    const model = page(["run-a", "run-b"], "run-b");
    expect(model.runs).toEqual([
      { runId: "run-a", projectId: "project-a", projectAvailable: true },
      { runId: "run-b", projectId: "project-a", projectAvailable: true },
    ]);
    expect(model.nextAfter).toBe("run-b");
  });

  test("an unavailable project stays visible with project_available false", () => {
    const model = parseApiDocument({
      schema_version: 1,
      kind: "runs",
      runs: [{ run_id: "run-x", project_id: "gone", project_available: false }],
      next_after: null,
    }) as UiApiModel & { kind: "runs" };
    expect(model.runs[0]).toEqual({ runId: "run-x", projectId: "gone", projectAvailable: false });
  });

  test("load more appends pages and keeps the newest cursor; refresh resets", () => {
    let state = createRunsState();
    expect(state).toEqual({ runs: [], nextAfter: null });
    state = applyRunsPage(state, page(["run-a", "run-b"], "run-b") as never);
    state = applyRunsPage(state, page(["run-c"], null) as never);
    expect(state.runs.map((run) => run.runId)).toEqual(["run-a", "run-b", "run-c"]);
    expect(state.nextAfter).toBe(null);
    state = applyRunsPage(createRunsState(), page(["run-z"], null) as never);
    expect(state.runs.map((run) => run.runId)).toEqual(["run-z"]);
  });

  test("the page paths carry the fixed limit and the exclusive cursor", () => {
    expect(runsPagePath(null)).toBe("/v1/runs?limit=100");
    expect(runsPagePath("run-b")).toBe("/v1/runs?limit=100&after=run-b");
  });

  test("runs pages reject hostile shapes", () => {
    for (const bad of [
      { schema_version: 1, kind: "runs", runs: [{ run_id: "r" }], next_after: null },
      { schema_version: 1, kind: "runs", runs: [{ run_id: "r", project_id: "p", project_available: "yes" }], next_after: null },
      { schema_version: 1, kind: "runs", runs: [], next_after: "../escape" },
      { schema_version: 1, kind: "runs", runs: [], next_after: 5 },
    ]) {
      expect(contractReason(() => parseApiDocument(bad))).toBe("invalid_shape");
    }
  });
});

describe("ui model: run detail", () => {
  const runDocument = {
    schema_version: 1,
    kind: "run",
    run_id: "run-a",
    revision: 7,
    status: "waiting",
    boundary: { kind: "waiting", waitIndex: 1, reason: "stage_wait", actions: [{ id: "continue_stage", to: "stage-entry" }] },
    outputs: [
      { id: "report", type: "json", required: true, present: true, digest: "a".repeat(64) },
      { id: "extra", type: "file", required: false, present: false },
    ],
  };

  test("the detail keeps status, revision, boundary and validated outputs", () => {
    const model = parseApiDocument(runDocument) as UiApiModel & { kind: "run" };
    expect(model.kind).toBe("run");
    expect(model.runId).toBe("run-a");
    expect(model.revision).toBe(7);
    expect(model.status).toBe("waiting");
    expect(model.boundary).toEqual(runDocument.boundary);
    expect(model.outputs).toEqual([
      { id: "report", type: "json", required: true, present: true, digest: "a".repeat(64) },
      { id: "extra", type: "file", required: false, present: false, digest: null },
    ]);
  });

  test("the inspect status vocabulary is closed", () => {
    for (const status of ["running", "waiting", "succeeded", "failed"] as const) {
      const model = parseApiDocument({ ...runDocument, status }) as UiApiModel & { kind: "run" };
      expect(model.status).toBe(status);
    }
    expect(contractReason(() => parseApiDocument({ ...runDocument, status: "stalled" }))).toBe("invalid_shape");
  });

  test("output entries reject unknown shapes, unknown types and malformed digests", () => {
    for (const outputs of [
      [{ id: "report" }],
      [{ id: "report", type: "json", required: true, present: true, digest: "a".repeat(64), extra: 1 }],
      [{ id: "report", type: "binary", required: true, present: true }],
      [{ id: "report", type: "json", required: true, present: true, digest: "A".repeat(64) }],
      [{ id: "report", type: "json", required: true, present: true, digest: "short" }],
      "all",
    ]) {
      expect(contractReason(() => parseApiDocument({ ...runDocument, outputs }))).toBe("invalid_shape");
    }
  });

  test("unknown schema versions and kinds in a run document are contract errors", () => {
    expect(contractReason(() => parseApiDocument({ ...runDocument, schema_version: 2 }))).toBe("unknown_schema_version");
    expect(contractReason(() => parseApiDocument({ ...runDocument, kind: "run_v2" }))).toBe("unknown_kind");
  });
});

describe("ui model: command results", () => {
  test("success, waiting, plan-ready, responded and failed results are modeled with exit codes", () => {
    for (const [outcome, exitCode] of [
      ["succeeded", 0],
      ["waiting", 0],
      ["plan_ready", 0],
      ["responded", 0],
      ["failed", 1],
      ["failed", 130],
      ["failed", 143],
    ] as const) {
      const document: Record<string, unknown> = {
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome,
        exit_code: exitCode,
      };
      if (outcome === "failed") {
        document["reason"] = "run_busy";
      }
      const model = parseApiDocument(document) as UiApiModel & { kind: "command_result" };
      expect(model.outcome).toBe(outcome);
      expect(model.exitCode).toBe(exitCode);
      expect(model.reason).toBe(outcome === "failed" ? "run_busy" : null);
    }
  });

  test("exit codes, outcome vocabulary and the failed/reason pairing are closed", () => {
    for (const bad of [
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "succeeded", exit_code: 1 },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "failed", exit_code: 0 },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "failed", exit_code: 2 },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "stalled", exit_code: 0 },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "succeeded", exit_code: 0, reason: "surprise" },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "failed", exit_code: 1, reason: "" },
      { schema_version: 1, kind: "command_result", run_id: "r", outcome: "succeeded", exit_code: 0, extra: 1 },
    ]) {
      expect(contractReason(() => parseApiDocument(bad))).toBe("invalid_shape");
    }
  });
});

describe("ui model: error documents", () => {
  test("registry, binding, transport and request errors are displayed as layer: reason", () => {
    for (const [layer, reason] of [
      ["registry", "input_not_found"],
      ["binding", "binding_not_found"],
      ["binding", "binding_conflict"],
      ["transport", "spawn_failed"],
      ["request", "invalid_request"],
    ] as const) {
      const model = parseApiDocument({ schema_version: 1, kind: "error", layer, reason }) as UiApiModel & { kind: "error" };
      expect(formatFailure(model)).toBe(`${layer}: ${reason}`);
    }
  });

  test("error documents reject unknown layers and empty reasons without echoing values", () => {
    for (const bad of [
      { schema_version: 1, kind: "error", layer: "galaxy", reason: "r" },
      { schema_version: 1, kind: "error", layer: "binding", reason: "" },
      { schema_version: 1, kind: "error", layer: "binding" },
      { schema_version: 1, kind: "error", layer: "binding", reason: "r", message: "secret detail" },
    ]) {
      expect(contractReason(() => parseApiDocument(bad))).toBe("invalid_shape");
    }
    expect(formatFailure({ layer: "binding", reason: 5 })).toBe("transport: unknown_failure");
    expect(formatFailure(null)).toBe("transport: unknown_failure");
  });
});

describe("ui model: fresh-run body", () => {
  test("the body carries exactly the wire fields in declaration order", () => {
    const body = buildFreshRunBody("project-a", "caller-owned-id", [
      { id: "task", rootId: "tasks", ref: "task-1" },
      { id: "plan", rootId: "plans", ref: "plan-a" },
    ]);
    expect(JSON.parse(JSON.stringify(body))).toEqual({
      run_id: "caller-owned-id",
      inputs: [
        { id: "task", root_id: "tasks", ref: "task-1" },
        { id: "plan", root_id: "plans", ref: "plan-a" },
      ],
    });
    expect(freshRunPath("project-a")).toBe("/v1/projects/project-a/runs");
  });

  test("uuid-shaped run ids satisfy the safe-id grammar", () => {
    expect(isSafeId("550e8400-e29b-41d4-a716-446655440000")).toBe(true);
    expect(isSafeId("task.md")).toBe(true);
    expect(isSafeId("")).toBe(false);
    expect(isSafeId("-leading")).toBe(false);
    expect(isSafeId("with space")).toBe(false);
    expect(isSafeId("../escape")).toBe(false);
    expect(isSafeId("a/b")).toBe(false);
  });

  test("paths, credentials and hostile fields are never accepted into the body", () => {
    expect(contractReason(() => buildFreshRunBody("project-a", "../etc/passwd", [{ id: "task", rootId: "tasks", ref: "t" }]))).toBe("invalid_run_id");
    expect(contractReason(() => buildFreshRunBody("/secret/root", "r1", [{ id: "task", rootId: "tasks", ref: "t" }]))).toBe("invalid_project");
    expect(contractReason(() => buildFreshRunBody("project-a", "r1", []))).toBe("invalid_inputs");
    expect(contractReason(() => buildFreshRunBody("project-a", "r1", [{ id: "task", rootId: "/absolute/root", ref: "t" }]))).toBe("invalid_inputs");
    expect(contractReason(() => buildFreshRunBody("project-a", "r1", [{ id: "task", rootId: "tasks", ref: "a/b" }]))).toBe("invalid_inputs");
    expect(
      contractReason(() => buildFreshRunBody("project-a", "r1", [{ id: "task", rootId: "tasks", ref: "t" }, { id: "task", rootId: "tasks", ref: "u" }])),
    ).toBe("invalid_inputs");
  });
});

describe("ui model: lost-response contract", () => {
  test("the recovery plan is inspect-only and never a second POST", () => {
    const recovery = lostResponseRecovery("caller-owned-id");
    expect(recovery).toEqual({ kind: "inspect_only", runId: "caller-owned-id", postAgain: false, inspectPath: "/v1/runs/caller-owned-id" });
    expect(contractReason(() => lostResponseRecovery(""))).toBe("invalid_run_id");
  });

  test("a network failure on a POST is lost; on a GET it is an ordinary failure", async () => {
    const failing = async (): Promise<Response> => {
      throw new TypeError("fetch failed");
    };
    const lostPost = await requestApiJson(failing, "/v1/projects/p/runs", { method: "POST" });
    expect(lostPost.ok).toBe(false);
    expect(lostPost.lost).toBe(true);
    expect(failureOf(lostPost)).toEqual({
      layer: "transport",
      reason: "network_error",
    });
    const lostGet = await requestApiJson(failing, "/v1/runs/r1");
    expect(lostGet.ok).toBe(false);
    expect(lostGet.lost).toBe(false);
    expect(failureOf(lostGet)).toEqual({
      layer: "transport",
      reason: "network_error",
    });
  });
});

describe("ui model: the one API client", () => {
  const jsonResponse = (document: unknown, status = 200): Response =>
    new Response(JSON.stringify(document), { status, headers: { "content-type": "application/json" } });

  test("the content type is checked before any body is read", async () => {
    const result = await requestApiJson(async () => new Response("<html>proxy error</html>", { headers: { "content-type": "text/html" } }), "/v1/projects");
    expect(result.ok).toBe(false);
    expect(failureOf(result)).toEqual({ layer: "transport", reason: "unexpected_content_type" });
  });

  test("a malformed JSON body is a transport failure without parser fragments", async () => {
    const result = await requestApiJson(
      async () =>
        new Response('{"schema_version":1,"kind":"run",', {
          headers: { "content-type": "application/json" },
        }),
      "/v1/runs/r1",
    );
    expect(result.ok).toBe(false);
    expect(failureOf(result)).toEqual({ layer: "transport", reason: "malformed_response" });
  });

  test("the body read is bounded", async () => {
    const huge = "x".repeat(2 * 1048576);
    const result = await requestApiJson(async () => new Response(huge, { headers: { "content-type": "application/json" } }), "/v1/projects");
    expect(result.ok).toBe(false);
    expect(failureOf(result)).toEqual({ layer: "transport", reason: "response_too_large" });
  });

  test("documents are classified only by kind, never by status or headers", async () => {
    const errorResult = await requestApiJson(
      async () => jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" }, 404),
      "/v1/runs/r1",
    );
    expect(errorResult.ok).toBe(true);
    expect(modelOf(errorResult).kind).toBe("error");
    expect(formatFailure(modelOf(errorResult))).toBe("binding: binding_not_found");
    const runOnOddStatus = await requestApiJson(
      async () =>
        jsonResponse(
          { schema_version: 1, kind: "run", run_id: "r1", revision: 1, status: "running", boundary: {}, outputs: [] },
          502,
        ),
      "/v1/runs/r1",
    );
    expect(runOnOddStatus.ok).toBe(true);
    expect(modelOf(runOnOddStatus).kind).toBe("run");
  });

  test("a contract violation becomes a contract failure with the stable reason", async () => {
    const result = await requestApiJson(async () => jsonResponse({ schema_version: 3, kind: "runs", runs: [], next_after: null }), "/v1/runs");
    expect(result.ok).toBe(false);
    expect(failureOf(result)).toEqual({ layer: "contract", reason: "unknown_schema_version" });
  });

  test("the POST init carries the exact wire body built before the request", async () => {
    let captured: { path: string; init: RequestInit | undefined } | null = null;
    const recording = async (path: string, init?: RequestInit): Promise<Response> => {
      captured = { path, init };
      return jsonResponse({ schema_version: 1, kind: "command_result", run_id: "fixed-run", outcome: "waiting", exit_code: 0 });
    };
    const body = buildFreshRunBody("project-a", "fixed-run", [{ id: "task", rootId: "tasks", ref: "task-1" }]);
    const result = await requestApiJson(recording, freshRunPath("project-a"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(result.ok).toBe(true);
    expect((captured as unknown as { path: string }).path).toBe("/v1/projects/project-a/runs");
    expect(JSON.parse(String((captured as unknown as { init: RequestInit }).init?.body))).toEqual({
      run_id: "fixed-run",
      inputs: [{ id: "task", root_id: "tasks", ref: "task-1" }],
    });
  });
});

describe("ui model: hostile content stays plain text", () => {
  test("hostile strings pass through the model verbatim for textContent rendering", () => {
    const hostile = '<img src=x onerror="alert(1)">';
    const model = parseApiDocument({
      schema_version: 1,
      kind: "projects",
      projects: [{ id: hostile, input_roots: [{ id: hostile }] }],
    }) as UiApiModel & { kind: "projects" };
    expect(model.projects[0]?.id).toBe(hostile);
    expect(model.projects[0]?.inputRootIds[0]).toBe(hostile);
    expect(formatFailure({ layer: hostile, reason: hostile })).toContain(hostile);
  });
});

describe("ui model: export surface and source restrictions", () => {
  test("the runtime export surface is exactly the declared model API", async () => {
    const runtime = (await import("../ui/app.js")) as unknown as Record<string, unknown>;
    const declared = readFileSync(join(import.meta.dir, "..", "ui", "app.d.ts"), "utf8");
    const declaredNames = [...declared.matchAll(/export declare (?:class|function) (\w+)/g)].map((match) => match[1] as string);
    expect(Object.keys(runtime).sort()).toEqual(declaredNames.slice().sort());
    expect(Object.keys(runtime).sort()).toEqual(
      [
        "UiContractError",
        "isSafeId",
        "parseApiDocument",
        "createRunsState",
        "applyRunsPage",
        "runsPagePath",
        "freshRunPath",
        "buildFreshRunBody",
        "lostResponseRecovery",
        "formatFailure",
        "requestApiJson",
      ].sort(),
    );
  });

  test("the browser module contains no injection, eval, external-origin, storage or stream capabilities", () => {
    for (const banned of [
      "innerHTML",
      "insertAdjacentHTML",
      "document.write",
      "outerHTML",
      "eval(",
      "new Function",
      "Function(",
      "import(",
      "http://",
      "https://",
      "localStorage",
      "sessionStorage",
      "document.cookie",
      "indexedDB",
      "WebSocket",
      "EventSource",
      "serviceWorker",
      "fetch(",
      "XMLHttpRequest",
      "sendBeacon",
      "stderr",
      "state.json",
      "runRoot",
      "run_root",
      "POST_RETRY",
      "retryPost",
    ]) {
      expect(appSource.includes(banned)).toBe(false);
    }
    expect(appSource.split("await fetchImpl(").length - 1).toBe(1);
    // exactly one mutating request site (the fresh-run POST); the GETs
    // are the discovery and inspect reads
    expect(appSource.split('method: "POST"').length - 1).toBe(1);
    expect(appSource.split('method: "GET"').length - 1).toBe(0);
    for (const line of appSource.split("\n")) {
      expect(line.startsWith("import ")).toBe(false);
    }
    expect(appSource.includes("typeof document")).toBe(true);
  });

  test("the shipped document and stylesheet carry no inline code or external references", () => {
    expect(htmlSource.split("<script").length - 1).toBe(1);
    expect(htmlSource).toContain('<script type="module" src="/ui/app.js"></script>');
    for (const banned of ["<script>", "style=", "onclick", "onerror", "onload", "javascript:", "http://", "https://"]) {
      expect(htmlSource.includes(banned)).toBe(false);
    }
    for (const banned of ["http://", "https://", "@import", "url(", "expression("]) {
      expect(cssSource.includes(banned)).toBe(false);
    }
  });
});
