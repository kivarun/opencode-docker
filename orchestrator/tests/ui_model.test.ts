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
  buildMutationRequest,
  buildRunActionModel,
  createGenerationGuard,
  createMutationGate,
  createRunsState,
  expectDiscoveryModel,
  expectMutationResult,
  expectRunOrError,
  formatFailure,
  freshRunPath,
  isSafeId,
  lostResponseRecovery,
  parseApiDocument,
  parseEventsDocument,
  requestApiJson,
  runMutationFlow,
  runsPagePath,
  type UiApiModel,
  type UiRunActionModel,
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
    // the general API client boundary: the events feed stays owned by
    // the dedicated events client; an events document never satisfies
    // an inspect, discovery or mutation request
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "events", run_id: "r", revision: 1, events: [], next_after: null }))).toBe("unknown_kind");
    // the content document stays owned by a future dedicated viewer: the
    // general API client keeps rejecting it fail-closed
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "project_change_content", run_id: "r", revision: 1, observation: "a".repeat(64), change_index: 1, side: "result", status: "available", byte_count: 1, sha256: "b".repeat(64), offset: 0, data_base64: "aGk=", next_offset: null }))).toBe("unknown_kind");
    // the explicit Project Apply operation document stays CLI/API-only:
    // the browser UI has no Apply surface and the general API client
    // keeps rejecting the new kinds fail-closed
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "project_apply_operation", run_id: "r", apply_id: "a", project_id: "p", observation: "a".repeat(64), revision: 1, status: "applied", completed_step_count: 1, total_step_count: 1 }))).toBe("unknown_kind");
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "project_apply_failure", run_id: "r", apply_id: "a", reason: "project_conflict" }))).toBe("unknown_kind");
  });

  test("the general client keeps rejecting the events kind; the events client owns the endpoint", async () => {
    const jsonResponse = (document: unknown): Response =>
      new Response(JSON.stringify(document), { status: 200, headers: { "content-type": "application/json" } });
    const eventsDocument = {
      schema_version: 1,
      kind: "events",
      run_id: "run-a",
      revision: 1,
      events: [{ revision: 1, kind: "create_run" }],
      next_after: null,
    };
    const result = await requestApiJson(async () => jsonResponse(eventsDocument), "/v1/runs/run-a/events?after=0&limit=100");
    expect(result.ok).toBe(false);
    expect((result as { failure: { layer: string; reason: string } }).failure).toEqual({ layer: "contract", reason: "unknown_kind" });
    // the endpoint acceptance helpers never accept an events model either
    const eventsModel = parseEventsDocument(eventsDocument, "run-a");
    expect(eventsModel.kind).toBe("events");
    expect(expectRunOrError({ ok: true, model: eventsModel } as unknown as Parameters<typeof expectRunOrError>[0])).toEqual({
      ok: false,
      failure: { layer: "contract", reason: "unexpected_kind" },
    });
    expect(expectDiscoveryModel({ ok: true, model: eventsModel } as unknown as Parameters<typeof expectDiscoveryModel>[0], "runs")).toEqual({
      ok: false,
      failure: { layer: "contract", reason: "unexpected_kind" },
    });
    expect(expectMutationResult({ ok: true, model: eventsModel } as unknown as Parameters<typeof expectMutationResult>[0])).toEqual({
      ok: false,
      failure: { layer: "contract", reason: "unexpected_kind" },
    });
  });

  test("the model is frozen and repeated parses are structurally identical", () => {
    const model = parseApiDocument({ schema_version: 2, kind: "projects", projects: [] });
    expect(Object.isFrozen(model)).toBe(true);
    expect(model).toEqual(parseApiDocument({ projects: [], kind: "projects", schema_version: 2 }));
  });
});

describe("ui model: projects", () => {
  test("projects keep declaration order and input-root ids only", () => {
    const model = parseApiDocument({
      schema_version: 2,
      kind: "projects",
      projects: [
        { id: "project-a", input_roots: [{ id: "tasks" }, { id: "plans" }], project_apply: false },
        { id: "project-b", input_roots: [], project_apply: false },
      ],
    });
    expect(model.kind).toBe("projects");
    expect((model as UiApiModel & { kind: "projects" }).projects).toEqual([
      { id: "project-a", inputRootIds: ["tasks", "plans"], projectApply: false },
      { id: "project-b", inputRootIds: [], projectApply: false },
    ]);
  });

  test("projects reject unknown fields, inherited fields and hostile shapes", () => {
    const inherited = Object.create({ schema_version: 2, kind: "projects", projects: [] });
    expect(contractReason(() => parseApiDocument(inherited))).toBe("invalid_shape");
    for (const bad of [
      { schema_version: 2, kind: "projects", projects: [], extra: 1 },
      { schema_version: 2, kind: "projects" },
      { schema_version: 2, kind: "projects", projects: [{ id: "p" }] },
      { schema_version: 2, kind: "projects", projects: [{ id: "p", input_roots: [{ id: "t", root: "/secret" }] }] },
      { schema_version: 2, kind: "projects", projects: [{ id: "", input_roots: [] }] },
      { schema_version: 2, kind: "projects", projects: "all" },
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
      schema_version: 2,
      kind: "projects",
      projects: [{ id: hostile, input_roots: [{ id: hostile }], project_apply: false }],
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
    const declaredNames = [...declared.matchAll(/export declare (?:class|function|const) (\w+)/g)].map((match) => match[1] as string);
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
        "buildRunActionModel",
        "buildMutationRequest",
        "expectRunOrError",
        "expectDiscoveryModel",
        "expectMutationResult",
        "createMutationGate",
        "createGenerationGuard",
        "runMutationFlow",
        "createInspectTransport",
        "LIVE_OBSERVATION_POLL_DELAY_MS",
        "createLiveObservationScheduler",
        "createSchedulerTimerAdapter",
        "liveObservationDocumentKey",
        "livePollAnnouncement",
        "liveStateLabel",
        "EVENTS_PAGE_LIMIT",
        "PIPELINE_V2_EVENT_KINDS",
        "eventReferenceKeys",
        "parseEventsDocument",
        "requestEventsJson",
        "eventsRequestPath",
        "createTimelineState",
        "applyEventsPage",
        "stopTimeline",
        "timelineRetryAvailable",
        "timelineSummary",
        "timelineAnnouncement",
        "createEventsTransport",
        "parseWorkerLogsDocument",
        "parseWorkerLogDocument",
        "requestWorkerLogsJson",
        "workerLogsRequestPath",
        "createWorkerLogsState",
        "createLogsTransport",
        "createWorkerLogViewer",
        "createLogChunkDecoder",
        "parseProjectChangesDocument",
        "createProjectChangesState",
        "applyProjectChangesPage",
        "projectChangesRequestPath",
        "requestProjectChangesJson",
        "createProjectChangesTransport",
        "createProjectChangesViewer",
        "CONTENT_PREVIEW_CHUNK_LIMIT",
        "CONTENT_PREVIEW_MAX_BYTES",
        "CONTENT_HEX_DISPLAY_BYTES",
        "DIFF_MAX_LINES_PER_SIDE",
        "DIFF_MAX_WORK_CELLS",
        "parseProjectChangeContentDocument",
        "requestProjectChangeContentJson",
        "projectChangeContentRequestPath",
        "createProjectChangeContentTransport",
        "createProjectChangeContentViewer",
        "decodePreviewText",
        "hexPreviewRows",
        "computeBoundedLineDiff",
        "PROJECT_APPLY_OPERATION_STATUSES",
        "PROJECT_APPLY_UI_STATES",
        "parseProjectApplyOperationDocument",
        "requestProjectApplyJson",
        "projectApplyRequestPath",
        "projectApplyPostBody",
        "PROJECT_APPLY_INELIGIBILITY_REASONS",
        "projectApplyEligibility",
        "APPLY_RECORD_STORAGE_PREFIX",
        "parseApplyRecoveryRecord",
        "createApplyRecoveryStore",
        "createProjectApplyTransport",
        "createProjectApplyViewer",
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
      "setInterval",
      "/respond",
      "stage-1",
      "stage-2",
      "task-a",
      "task-b",
      '"tasks"',
      "/opt/",
      "/home/",
      "file://",
      "stdout",
    ]) {
      expect(appSource.includes(banned)).toBe(false);
    }
    expect(appSource.split("await fetchImpl(").length - 1).toBe(1);
    // exactly two mutating POST sites, both through the one shared UI
    // mutation gate (the shared run-mutation owner and the apply
    // owner); the GETs are the discovery and inspect reads
    expect(appSource.split('method: "POST"').length - 1).toBe(2);
    expect(appSource.split('method: "GET"').length - 1).toBe(0);
    expect(appSource.split("buildMutationRequest(").length - 1).toBe(2);
    expect(appSource.split("buildRunActionModel(").length - 1).toBe(2);
    expect(appSource.split("runMutationFlow(").length - 1).toBe(2);
    expect(appSource.split("postMutation(").length - 1).toBe(3);
    for (const line of appSource.split("\n")) {
      expect(line.startsWith("import ")).toBe(false);
    }
    expect(appSource.includes("typeof document")).toBe(true);
    // the one closed storage owner: exactly one runtime access site
    // (`typeof localStorage`) plus the documented owner mentions — no
    // sessionStorage, cookie, indexedDB or other storage API anywhere
    expect(appSource.split("localStorage").length - 1).toBe(6);
    expect(appSource.split("APPLY_RECORD_STORAGE_PREFIX").length - 1).toBe(2);
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

/* ------------------------------------------------------------------ */
/* Browser UI v2: manual boundary-driven lifecycle controls.           */
/* ------------------------------------------------------------------ */

const RUNNING_BOUNDARIES = {
  in_flight: { kind: "in_flight", executionIndex: 3, stateId: "dev_entry", executionRole: "stage", phase: "running" },
  resumable: { kind: "resumable", stateId: "next_state", transitionCount: 4 },
  plan_ready_resume: { kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume" },
  plan_ready_manual: { kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume_plan" },
} as const;

function runModelOf(status: string, boundary: unknown): UiApiModel {
  return parseApiDocument({
    schema_version: 1,
    kind: "run",
    run_id: "run-a",
    revision: 5,
    status,
    boundary,
    outputs: [],
  });
}

type UiRunActionAuthorityFixture = Parameters<typeof buildRunActionModel>[1];

const AUTHORITY_AVAILABLE = {
  projectsModel: parseApiDocument({
    schema_version: 2,
    kind: "projects",
    projects: [{ id: "project-a", input_roots: [{ id: "tasks" }, { id: "plans" }], project_apply: false }],
  }) as UiApiModel & { kind: "projects" },
  runEntry: { runId: "run-a", projectId: "project-a", projectAvailable: true },
} as const;

function waitingRun(actionIds: readonly string[]): UiApiModel {
  return runModelOf("running", { kind: "waiting" }) === null
    ? (undefined as never)
    : runModelOf("waiting", {
        kind: "waiting",
        waitIndex: 2,
        reason: "stage_iteration_completed",
        actions: actionIds.map((id) => ({ id, to: "architect" })),
      });
}

describe("ui v2: boundary-to-control matrix", () => {
  test("in_flight allows refresh only; resumable offers resume", () => {
    expect(buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.in_flight), null)).toEqual({
      kind: "observe_only",
      runId: "run-a",
    });
    expect(buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.resumable), null)).toEqual({
      kind: "resume",
      runId: "run-a",
    });
  });

  test("plan_ready distinguishes the inspect-declared continuation only", () => {
    expect(buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.plan_ready_resume), null)).toEqual({
      kind: "resume",
      runId: "run-a",
    });
    expect(buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.plan_ready_manual), null)).toEqual({
      kind: "resume_plan",
      runId: "run-a",
    });
  });

  test("waiting boundaries expose the declared actions in server order", () => {
    const model = buildRunActionModel(waitingRun(["continue_stage", "revise_task"]), AUTHORITY_AVAILABLE) as UiRunActionModel & {
      kind: "waiting";
    };
    expect(model.kind).toBe("waiting");
    expect(model.controls.map((control) => control.kind)).toEqual(["continue_stage", "revise_task"]);
    const reversed = buildRunActionModel(waitingRun(["revise_task", "continue_stage"]), AUTHORITY_AVAILABLE) as UiRunActionModel & {
      kind: "waiting";
    };
    expect(reversed.controls.map((control) => control.kind)).toEqual(["revise_task", "continue_stage"]);
    const single = buildRunActionModel(waitingRun(["continue_stage"]), null) as UiRunActionModel & { kind: "waiting" };
    expect(single.controls.map((control) => control.kind)).toEqual(["continue_stage"]);
  });

  test("the revise-task control carries the project and its input-root ids from discovery only", () => {
    const model = buildRunActionModel(waitingRun(["revise_task"]), AUTHORITY_AVAILABLE) as UiRunActionModel & { kind: "waiting" };
    const control = model.controls[0] as { kind: string; projectId: string; inputRootIds: readonly string[]; waitIndex: number };
    expect(control).toMatchObject({ kind: "revise_task", projectId: "project-a", waitIndex: 2 });
    expect(control.inputRootIds).toEqual(["tasks", "plans"]);
  });

  test("an unavailable or unresolvable project disables revise-task fail closed", () => {
    const blocked = (authority: UiRunActionAuthorityFixture): string => {
      const model = buildRunActionModel(waitingRun(["revise_task"]), authority) as UiRunActionModel & { kind: "waiting" };
      const control = model.controls[0] as { kind: string; cause?: string };
      return control.kind === "revise_task_blocked" ? (control.cause as string) : control.kind;
    };
    expect(blocked({ ...AUTHORITY_AVAILABLE, runEntry: { runId: "run-a", projectId: "project-a", projectAvailable: false } })).toBe(
      "project_unavailable",
    );
    expect(blocked({ projectsModel: null, runEntry: AUTHORITY_AVAILABLE.runEntry })).toBe("projects_not_loaded");
    expect(blocked({ projectsModel: AUTHORITY_AVAILABLE.projectsModel, runEntry: null })).toBe("project_missing");
    expect(
      blocked({
        projectsModel: parseApiDocument({
          schema_version: 2,
          kind: "projects",
          projects: [{ id: "other", input_roots: [{ id: "tasks" }], project_apply: false }],
        }) as UiApiModel & { kind: "projects" },
        runEntry: AUTHORITY_AVAILABLE.runEntry,
      }),
    ).toBe("project_missing");
    expect(
      blocked({
        projectsModel: parseApiDocument({
          schema_version: 2,
          kind: "projects",
          projects: [{ id: "project-a", input_roots: [], project_apply: false }],
        }) as UiApiModel & { kind: "projects" },
        runEntry: AUTHORITY_AVAILABLE.runEntry,
      }),
    ).toBe("no_input_roots");
  });

  test("terminal boundaries project the outcome only", () => {
    expect(buildRunActionModel(runModelOf("succeeded", { kind: "succeeded", terminalStateId: "done" }), null)).toEqual({
      kind: "terminal",
      runId: "run-a",
      status: "succeeded",
    });
    expect(buildRunActionModel(runModelOf("failed", { kind: "failed", reason: "worker_failed" }), null)).toEqual({
      kind: "terminal",
      runId: "run-a",
      status: "failed",
    });
  });

  test("unknown statuses, boundaries, continuations and actions fail closed", () => {
    const reasonOf = (status: string, boundary: unknown): string =>
      (buildRunActionModel(runModelOf(status, boundary), null) as UiRunActionModel & { reason?: string }).reason ?? "none";
    expect(reasonOf("running", { kind: "mystery" })).toBe("unknown_boundary");
    expect(reasonOf("running", { kind: "waiting", waitIndex: 1, reason: "r", actions: [{ id: "continue_stage", to: "x" }] })).toBe(
      "status_boundary_mismatch",
    );
    expect(reasonOf("waiting", RUNNING_BOUNDARIES.resumable)).toBe("status_boundary_mismatch");
    expect(reasonOf("succeeded", { kind: "resumable", stateId: "s", transitionCount: 1 })).toBe("status_boundary_mismatch");
    expect(reasonOf("failed", { kind: "failed", reason: "r" }) === "none").toBe(true);
    expect(reasonOf("running", { kind: "plan_ready", planningExecutionIndex: 1, continuation: "auto" })).toBe("unknown_continuation");
    expect(reasonOf("waiting", { kind: "waiting", waitIndex: 1, reason: "r", actions: [{ id: "other_action", to: "x" }] })).toBe("unknown_action");
    expect(reasonOf("waiting", { kind: "waiting", waitIndex: 0, reason: "r", actions: [{ id: "continue_stage", to: "x" }] })).toBe(
      "invalid_boundary_shape",
    );
    expect(reasonOf("waiting", { kind: "waiting", waitIndex: 1, reason: "r", actions: [] })).toBe("invalid_boundary_shape");
    expect(reasonOf("waiting", { kind: "waiting", waitIndex: 1, reason: "r", actions: [{ id: "continue_stage" }] })).toBe(
      "invalid_boundary_shape",
    );
    expect(() => buildRunActionModel({ kind: "command_result" }, null)).toThrow(UiContractError);
  });
});

describe("ui v2: the one mutation request builder", () => {
  test("resume sends an empty object to the resume route", () => {
    const action = buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.resumable), null);
    expect(buildMutationRequest(action, {})).toEqual({ endpoint: "resume", path: "/v1/runs/run-a/resume", body: {} });
  });

  test("resume-plan sends exactly stage_id and initial_budget", () => {
    const action = buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.plan_ready_manual), null);
    expect(buildMutationRequest(action, { stageId: "stage-1", initialBudget: "2" })).toEqual({
      endpoint: "resume_plan",
      path: "/v1/runs/run-a/resume-plan",
      body: { stage_id: "stage-1", initial_budget: 2 },
    });
  });

  test("continue-stage takes the wait index only from the inspect projection", () => {
    const model = buildRunActionModel(waitingRun(["continue_stage"]), null) as UiRunActionModel & { kind: "waiting" };
    const control = model.controls[0];
    expect(buildMutationRequest(control, { additionalIterations: "1", waitIndex: 99 })).toEqual({
      endpoint: "continue_stage",
      path: "/v1/runs/run-a/continue-stage",
      body: { wait_index: 2, additional_iterations: 1 },
    });
  });

  test("revise-task sends the wait index, the task id and the registry-controlled reference", () => {
    const model = buildRunActionModel(waitingRun(["revise_task"]), AUTHORITY_AVAILABLE) as UiRunActionModel & { kind: "waiting" };
    expect(buildMutationRequest(model.controls[0], { taskId: "task-a", rootId: "tasks", ref: "task-a-v2.md" })).toEqual({
      endpoint: "revise_task",
      path: "/v1/runs/run-a/revise-task",
      body: { wait_index: 2, task_id: "task-a", task: { root_id: "tasks", ref: "task-a-v2.md" } },
    });
  });

  test("validation completes before any fetch and never echoes values", () => {
    const resumePlan = buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.plan_ready_manual), null);
    for (const values of [
      { stageId: "stage-1", initialBudget: "0" },
      { stageId: "stage-1", initialBudget: "-1" },
      { stageId: "stage-1", initialBudget: "1.5" },
      { stageId: "stage-1", initialBudget: "abc" },
      { stageId: "stage-1", initialBudget: String(2 ** 53) },
      { stageId: "stage-1", initialBudget: "" },
      { stageId: "", initialBudget: "1" },
      { stageId: "../escape", initialBudget: "1" },
      {},
    ]) {
      expect(contractReason(() => buildMutationRequest(resumePlan, values))).toBe(
        values.stageId === undefined || !isSafeId(values.stageId) ? "invalid_stage_id" : "invalid_initial_budget",
      );
    }
    const model = buildRunActionModel(waitingRun(["continue_stage"]), null) as UiRunActionModel & { kind: "waiting" };
    for (const values of [{ additionalIterations: "0" }, { additionalIterations: "x" }, {}]) {
      expect(contractReason(() => buildMutationRequest(model.controls[0], values))).toBe("invalid_additional_iterations");
    }
    const revise = buildRunActionModel(waitingRun(["revise_task"]), AUTHORITY_AVAILABLE) as UiRunActionModel & { kind: "waiting" };
    for (const values of [
      { taskId: "task-a", rootId: "tasks", ref: "a/b" },
      { taskId: "task-a", rootId: "tasks", ref: "" },
      { taskId: "", rootId: "tasks", ref: "t.md" },
      { taskId: "task-a", rootId: "other-root", ref: "t.md" },
      { taskId: "task-a", rootId: "/secret/root", ref: "t.md" },
      {},
    ]) {
      expect(contractReason(() => buildMutationRequest(revise.controls[0], values))).toBe("invalid_task_reference");
    }
  });

  test("non-mutating action models never build a request", () => {
    for (const action of [
      buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.in_flight), null),
      buildRunActionModel(runModelOf("succeeded", { kind: "succeeded", terminalStateId: "done" }), null),
      buildRunActionModel(waitingRun(["revise_task"]), { projectsModel: null, runEntry: null }).kind === "waiting"
        ? (buildRunActionModel(waitingRun(["revise_task"]), { projectsModel: null, runEntry: null }) as UiRunActionModel & { kind: "waiting" })
            .controls[0]
        : null,
      buildRunActionModel(runModelOf("running", { kind: "mystery" }), null),
    ]) {
      expect(contractReason(() => buildMutationRequest(action, {}))).toBe("no_mutation");
    }
    expect(contractReason(() => buildMutationRequest(buildRunActionModel(waitingRun(["continue_stage"]), null), {}))).toBe("no_mutation");
  });
});

describe("ui v2: endpoint-specific response acceptance", () => {
  const asResult = (model: unknown) => ({ ok: true, lost: false, model }) as never;
  const asFailure = (failure: unknown) => ({ ok: false, lost: false, failure }) as never;

  test("an inspect accepts only run or error documents", () => {
    expect(expectRunOrError(asResult(runModelOf("running", RUNNING_BOUNDARIES.resumable))).ok).toBe(true);
    expect(expectRunOrError(asResult({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" })).ok).toBe(true);
    const wrong = expectRunOrError(asResult({ schema_version: 1, kind: "command_result", run_id: "r", outcome: "waiting", exit_code: 0 }));
    expect(wrong.ok).toBe(false);
    expect(wrong.failure).toEqual({ layer: "contract", reason: "unexpected_kind" });
    expect(expectRunOrError(asFailure({ layer: "transport", reason: "network_error" })).failure).toEqual({
      layer: "transport",
      reason: "network_error",
    });
  });

  test("discovery accepts only its own kind or an error", () => {
    const projects = { schema_version: 2, kind: "projects", projects: [] };
    const runs = { schema_version: 1, kind: "runs", runs: [], next_after: null };
    expect(expectDiscoveryModel(asResult(projects), "projects").ok).toBe(true);
    expect(expectDiscoveryModel(asResult(runs), "projects").failure).toEqual({ layer: "contract", reason: "unexpected_kind" });
    expect(expectDiscoveryModel(asResult(runs), "runs").ok).toBe(true);
    expect(expectDiscoveryModel(asResult(projects), "runs").failure).toEqual({ layer: "contract", reason: "unexpected_kind" });
    expect(expectDiscoveryModel(asResult({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_request" }), "runs").ok).toBe(true);
    expect(() => expectDiscoveryModel(asResult(runs), "command_result" as never)).toThrow(UiContractError);
  });

  test("a mutation accepts only command_result or error documents", () => {
    expect(expectMutationResult(asResult({ schema_version: 1, kind: "command_result", run_id: "r", outcome: "waiting", exit_code: 0 })).ok).toBe(true);
    expect(expectMutationResult(asResult({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_conflict" })).ok).toBe(true);
    expect(expectMutationResult(asResult(runModelOf("running", RUNNING_BOUNDARIES.resumable))).failure).toEqual({
      layer: "contract",
      reason: "unexpected_kind",
    });
  });
});

describe("ui v2: one mutation in flight and stale GET suppression", () => {
  test("the mutation gate admits exactly one mutating POST", () => {
    const gate = createMutationGate();
    expect(gate.isInFlight()).toBe(false);
    expect(gate.begin()).toBe(true);
    expect(gate.isInFlight()).toBe(true);
    expect(gate.begin()).toBe(false);
    gate.end();
    expect(gate.begin()).toBe(true);
    gate.end();
    expect(gate.isInFlight()).toBe(false);
  });

  test("the generation guard drops stale page responses", () => {
    const guard = createGenerationGuard();
    const first = guard.next();
    expect(guard.isCurrent(first)).toBe(true);
    const second = guard.next();
    expect(guard.isCurrent(first)).toBe(false);
    expect(guard.isCurrent(second)).toBe(true);
  });
});

describe("ui v2: the one mutation flow", () => {
  interface FlowLog {
    readonly send: number;
    readonly inspect: number;
    readonly events: string[];
  }

  const makeFlow = (sendResult: () => unknown, inspectResult: () => unknown, runId = "run-a") => {
    const log = { send: 0, inspect: 0, events: [] as string[] };
    const handlers = {
      onCommandResult: (model: unknown) => {
        log.events.push(`command:${JSON.stringify(model)}`);
      },
      onLost: () => {
        log.events.push("lost");
      },
      onRecovered: (model: unknown) => {
        log.events.push(`recovered:${(model as { boundary: { kind: string } }).boundary.kind}`);
      },
      onRecoveryUnavailable: (presented: unknown) => {
        log.events.push(`recovery_unavailable:${JSON.stringify(presented)}`);
      },
      onError: (presented: unknown) => {
        log.events.push(`error:${JSON.stringify(presented)}`);
      },
    };
    const flow = {
      send: async () => {
        log.send += 1;
        return sendResult();
      },
      inspect: async () => {
        log.inspect += 1;
        return inspectResult();
      },
      runId,
      handlers,
    };
    return { log, flow };
  };

  const succeededRun = () => runModelOf("succeeded", { kind: "succeeded", terminalStateId: "done" });
  const waitingRunModel = () => waitingRun(["continue_stage", "revise_task"]);
  const command = (outcome: string, reason?: string) => {
    const document: Record<string, unknown> = { schema_version: 1, kind: "command_result", run_id: "run-a", outcome, exit_code: outcome === "failed" ? 1 : 0 };
    if (reason !== undefined) {
      document["reason"] = reason;
    }
    return document;
  };

  test("a command result is shown, followed by exactly one authoritative inspect", async () => {
    const { log, flow } = makeFlow(() => asResultLike(command("waiting")), () => asResultLike(waitingRunModel()));
    await runMutationFlow(flow as never);
    expect(log.send).toBe(1);
    expect(log.inspect).toBe(1);
    expect(log.events[0]?.startsWith("command:")).toBe(true);
    expect(log.events[1]).toBe("recovered:waiting");
  });

  test("run_busy and invalid_state are surfaced once and never retried", async () => {
    for (const reason of ["run_busy", "invalid_state"]) {
      const { log, flow } = makeFlow(() => asResultLike(command("failed", reason)), () => asResultLike(waitingRunModel()));
      await runMutationFlow(flow as never);
      expect(log.send).toBe(1);
      expect(log.events[0]).toBe(`command:${JSON.stringify(command("failed", reason))}`);
      expect(log.events[1]).toBe("recovered:waiting");
    }
  });

  test("an error document or a contract kind stops the flow with zero inspect", async () => {
    const errorFlow = makeFlow(() => asResultLike({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_conflict" }), () => {
      throw new Error("inspect must not run");
    });
    await runMutationFlow(errorFlow.flow as never);
    expect(errorFlow.log.send).toBe(1);
    expect(errorFlow.log.inspect).toBe(0);
    expect(errorFlow.log.events[0]).toContain("binding_conflict");
    const contractFlow = makeFlow(() => asResultLike(succeededRun()), () => {
      throw new Error("inspect must not run");
    });
    await runMutationFlow(contractFlow.flow as never);
    expect(contractFlow.log.inspect).toBe(0);
    expect(contractFlow.log.events[0]).toContain("unexpected_kind");
  });

  test("a lost mutation response means outcome unknown with exactly one inspect GET and zero re-POST", async () => {
    const lostResult = { ok: false, lost: true, failure: { layer: "transport", reason: "network_error" } };
    const notFound = { schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" };
    for (const endpoint of ["resume", "resume_plan", "continue_stage", "revise_task"]) {
      const action =
        endpoint === "resume"
          ? buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.resumable), null)
          : endpoint === "resume_plan"
            ? buildRunActionModel(runModelOf("running", RUNNING_BOUNDARIES.plan_ready_manual), null)
            : endpoint === "continue_stage"
              ? (buildRunActionModel(waitingRun(["continue_stage"]), null) as UiRunActionModel & { kind: "waiting" }).controls[0]
              : (buildRunActionModel(waitingRun(["revise_task"]), AUTHORITY_AVAILABLE) as UiRunActionModel & { kind: "waiting" }).controls[0];
      const request = buildMutationRequest(action, {
        stageId: "stage-1",
        initialBudget: "1",
        additionalIterations: "1",
        taskId: "task-a",
        rootId: "tasks",
        ref: "task-a-v2.md",
      });
      expect(request.path.startsWith("/v1/runs/run-a/")).toBe(true);
      for (const inspectOutcome of ["found", "not_found", "network"] as const) {
        const log = { send: 0, inspect: 0, events: [] as string[] };
        await runMutationFlow({
          send: async () => {
            log.send += 1;
            return lostResult;
          },
          inspect: async () => {
            log.inspect += 1;
            if (inspectOutcome === "found") {
              return asResultLike(waitingRunModel());
            }
            if (inspectOutcome === "not_found") {
              return asResultLike(notFound);
            }
            return { ok: false, lost: false, failure: { layer: "transport", reason: "network_error" } };
          },
          runId: "run-a",
          handlers: {
            onCommandResult: () => log.events.push("command"),
            onLost: () => log.events.push("lost"),
            onRecovered: () => log.events.push("recovered"),
            onRecoveryUnavailable: (presented: unknown) => log.events.push(`recovery_unavailable:${JSON.stringify(presented)}`),
            onError: (presented: unknown) => log.events.push(`error:${JSON.stringify(presented)}`),
          },
        } as never);
        expect(log.send).toBe(1);
        expect(log.inspect).toBe(1);
        expect(log.events[0]).toBe("lost");
        expect(
          log.events[1] === "recovered" ||
            log.events[1]?.startsWith("recovery_unavailable:") ||
            log.events[1]?.startsWith("error:"),
        ).toBe(true);
        if (inspectOutcome === "found") {
          expect(log.events[1]).toBe("recovered");
        }
        if (inspectOutcome === "not_found") {
          expect(log.events[1]).toContain("binding_not_found");
        }
        if (inspectOutcome === "network") {
          expect(log.events[1]).toContain("network_error");
        }
      }
    }
  });
});

function asResultLike(model: unknown): { ok: true; lost: false; model: unknown } {
  return { ok: true, lost: false, model };
}

test("the worker-log kinds stay fail-closed for the general UI client", () => {
  // The worker logs are consumed only through the dedicated logs endpoint
  // client (v5): the general inspect/discovery/mutation client still
  // accepts no worker_logs or worker_log document, and the UI asset never
  // touches the durable artifact envelope or its payload field.
  expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "worker_logs", run_id: "r", revision: 1, logs: [] }))).toBe("unknown_kind");
  expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "worker_log", run_id: "r", revision: 1, execution_index: 1, state_id: "s", status: "captured" }))).toBe("unknown_kind");
  const appJs = readFileSync(new URL("../ui/app.js", import.meta.url).pathname, "utf8");
  expect(appJs).not.toContain("payload_base64");
  expect(appJs).not.toContain("workerlog.json");
});
