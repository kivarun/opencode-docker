/**
 * Permanent wiring suite of the browser UI v5 worker-log viewer: the
 * fake-DOM harness boots the shipped `app.js` and drives the production
 * call chains (run selection → inspect → the automatic inspect-driven
 * metadata refresh → execution selection → the explicit `Load log`
 * chunk flow → Retry), with an instrumented fetch and the real logs
 * endpoint documents. Every test proves the inspect-driven automatic
 * metadata path, terminal log access on every boundary (the viewer
 * carries no boundary classifier), the stale-response isolation and the
 * safe render on the real wiring.
 */
import { describe, expect, test } from "bun:test";
import { eventReferenceKeys } from "../ui/app.js";
import {
  createUiHarness,
  jsonResponse,
  projectsDocument,
  runDocument,
  runsPageDocument,
  type FakeElement,
} from "./ui_dom_harness.ts";

const b64 = (text: string): string => Buffer.from(text, "utf8").toString("base64");

interface LogsPlan {
  readonly revision: number;
  readonly metadata?: readonly Record<string, unknown>[];
  readonly chunk?: Record<string, unknown>;
  readonly fail?: "network" | "not_found" | "removed_project" | "contract";
  readonly kind?: string;
}

interface InspectPlan {
  readonly revision: number;
  readonly boundary: Record<string, unknown>;
}

interface EventsPlan {
  readonly revision: number;
  readonly kinds: readonly string[];
}

const defaultMetadata = (status = "captured"): Record<string, unknown>[] => {
  const base = { execution_index: 1, state_id: "architect", status };
  if (status === "captured" || status === "truncated") {
    return [{ ...base, sha256: "a".repeat(64), byte_count: 11, truncated_daemon: false, truncated_local: false }];
  }
  return [base];
};

const capturedMetadataAt = (index: number, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  execution_index: index,
  state_id: index === 1 ? "architect" : "execute",
  status: "captured",
  sha256: index === 1 ? "a".repeat(64) : "b".repeat(64),
  byte_count: 11,
  truncated_daemon: false,
  truncated_local: false,
  ...overrides,
});

const chunkBody = (executionIndex: number, text: string, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  schema_version: 1,
  kind: "worker_log",
  run_id: "run-a",
  revision: 24,
  execution_index: executionIndex,
  state_id: executionIndex === 1 ? "architect" : "execute",
  status: "captured",
  sha256: executionIndex === 1 ? "a".repeat(64) : "b".repeat(64),
  byte_count: Buffer.from(text, "utf8").length,
  truncated_daemon: false,
  truncated_local: false,
  offset: 0,
  data_base64: b64(text),
  next_offset: null,
  ...overrides,
});

const viewButtonOfRow = (harness: Awaited<ReturnType<typeof createUiHarness>>, row: number): FakeElement => {
  const buttons = harness.element("logs-body").children[row]!.querySelectorAll("button");
  const button = buttons.find((node) => node.textContent === "View");
  if (button === undefined) {
    throw new Error(`no View button on logs row ${row}`);
  }
  return button;
};

function makeLogsFetch(plans: {
  readonly list?: LogsPlan | readonly LogsPlan[];
  readonly chunks?: readonly LogsPlan[];
  readonly inspect?: InspectPlan | readonly InspectPlan[];
  readonly events?: EventsPlan;
}) {
  const listSeq = { index: 0 };
  const chunkSeq = { index: 0 };
  const inspectSeq = { index: 0 };
  const listGets: string[] = [];
  const chunkGets: string[] = [];
  const posts: string[] = [];
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      posts.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome: "succeeded",
        exit_code: 0,
      });
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(["run-a"]));
    }
    if (/^\/v1\/runs\/run-a\/events\?after=(\d+)&limit=\d+$/.test(path)) {
      const after = Number(/after=(\d+)/.exec(path)![1]);
      const plan = plans.events ?? { revision: 2, kinds: ["create_run", "start_agent_execution"] };
      const events = plan.kinds.map((kind, index) => eventEntry(kind, index + 1)).filter((entry) => (entry.revision as number) > after);
      return jsonResponse({ schema_version: 1, kind: "events", run_id: "run-a", revision: plan.revision, events, next_after: null });
    }
    if (path.startsWith("/v1/runs/run-a/logs")) {
      if (path === "/v1/runs/run-a/logs") {
        listGets.push(path);
        const plan = Array.isArray(plans.list)
          ? (plans.list[listSeq.index] ?? plans.list[plans.list.length - 1])
          : (plans.list ?? { revision: 24 });
        listSeq.index += 1;
        if (plan === undefined) {
          return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
        }
        if (plan.fail === "network") {
          throw new Error("network gone");
        }
        if (plan.fail === "not_found") {
          return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
        }
        if (plan.fail === "removed_project") {
          return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
        }
        if (plan.fail === "contract") {
          return jsonResponse({ schema_version: 1, kind: "worker_logs", run_id: "run-b", revision: 1, logs: [] });
        }
        return jsonResponse({
          schema_version: 1,
          kind: "worker_logs",
          run_id: "run-a",
          revision: plan.revision,
          logs: plan.metadata ?? defaultMetadata(),
        });
      }
      chunkGets.push(path);
      const plan = plans.chunks?.[chunkSeq.index] ?? plans.chunks?.[plans.chunks.length - 1] ?? { revision: 24 };
      chunkSeq.index += 1;
      if (plan === undefined) {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
      }
      if (plan.fail === "network") {
        throw new Error("network gone");
      }
      if (plan.fail === "not_found") {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
      }
      if (plan.fail === "removed_project") {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "project_unavailable" });
      }
      if (plan.fail === "contract") {
        return jsonResponse({
          schema_version: 1,
          kind: "worker_log",
          run_id: "run-b",
          revision: 1,
          execution_index: 1,
          state_id: "architect",
          status: "captured",
          sha256: "a".repeat(64),
          byte_count: 5,
          truncated_daemon: false,
          truncated_local: false,
          offset: 0,
          data_base64: b64("other"),
          next_offset: null,
        });
      }
      return jsonResponse(plan.chunk ?? {
        schema_version: 1,
        kind: "worker_log",
        run_id: "run-a",
        revision: plan.revision,
        execution_index: 1,
        state_id: "architect",
        status: "captured",
        sha256: "a".repeat(64),
        byte_count: 11,
        truncated_daemon: false,
        truncated_local: false,
        offset: 0,
        data_base64: b64("hello world"),
        next_offset: null,
      });
    }
    // the inspect endpoint (exact run path; logs and events are matched above)
    if (path === "/v1/runs/run-a") {
      const plan = Array.isArray(plans.inspect)
        ? (plans.inspect[inspectSeq.index] ?? plans.inspect[plans.inspect.length - 1])
        : (plans.inspect ?? { revision: 24, boundary: { kind: "in_flight" } });
      inspectSeq.index += 1;
      const status = plan.boundary.kind === "succeeded" ? "succeeded" : plan.boundary.kind === "failed" ? "failed" : "running";
      return jsonResponse(runDocument("run-a", plan.revision, status, plan.boundary));
    }
    throw new Error(`unexpected fetch path: ${path}`);
  };
  return { fetchImpl, listGets, chunkGets, posts };
}

function eventEntry(kind: string, revision: number): Record<string, unknown> {
  const keys = eventReferenceKeys(kind);
  if (keys === null) {
    throw new Error(`unknown test kind ${kind}`);
  }
  const entry: Record<string, unknown> = { revision, kind };
  for (const key of keys) {
    entry[key] = key === "state_id" ? "dev_entry" : revision;
  }
  return entry;
}

async function bootWithInspect(boundary: Record<string, unknown>) {
  const transport = makeLogsFetch({ inspect: { revision: 24, boundary } });
  const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
  harness.click("refresh-runs");
  await Bun.sleep(10);
  harness.clickRunInspect(0);
  await Bun.sleep(10);
  return { harness, transport };
}

describe("worker logs wiring", () => {
  test("a failed inspect loads no logs: the safe inspect/error projection stays", async () => {
    const transport = makeLogsFetch({ inspect: { revision: 24, boundary: { kind: "unusable" } } });
    // the inspect endpoint itself fails: the run document is malformed
    const harness = await createUiHarness({
      fetchImpl: async (path: string, init?: RequestInit) => {
        if (path === "/v1/runs/run-a") {
          return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
        }
        return transport.fetchImpl(path, init);
      },
    });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    // the inspect error projection is shown; the logs section never loads
    expect(harness.error()).toContain("binding: binding_not_found");
    expect(transport.listGets).toEqual([]); // zero automatic metadata GETs
    expect(transport.chunkGets).toEqual([]);
    expect(harness.logsRows()).toEqual([]); // the section stayed unpopulated
    expect(harness.logsStatus()).toBe("");
    harness.dispose();
  });
  test("the first successful inspect loads the metadata automatically exactly once", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    expect(harness.element("worker-logs-section").hidden).toBe(false);
    expect(transport.listGets).toEqual(["/v1/runs/run-a/logs"]); // exactly one automatic GET
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]);
    expect(harness.logsStatus()).toContain("snapshot revision 24");
    expect(harness.logsEmpty()).toBe("");
    // the chunk button stays hidden until an execution is selected
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true);
    harness.dispose();
  });

  test("a re-inspect at the same revision issues no further automatic GET", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(transport.listGets).toHaveLength(1); // the unchanged revision never re-fetches
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]);
    harness.dispose();
  });

  test("a strictly greater inspect revision refreshes the metadata automatically", async () => {
    const transport = makeLogsFetch({
      inspect: [
        { revision: 24, boundary: { kind: "in_flight" } },
        { revision: 30, boundary: { kind: "in_flight" } },
      ],
      list: [
        { revision: 24, metadata: defaultMetadata("pending") },
        { revision: 30, metadata: defaultMetadata("captured") },
      ],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    expect(harness.logsRows()).toEqual(["1|architect|pending|||"]);
    // the applied inspect at revision 30 drives exactly one automatic GET
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(transport.listGets).toHaveLength(2);
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]);
    expect(harness.logsStatus()).toContain("snapshot revision 30");
    harness.dispose();
  });

  test("the manual Load logs refresh stays safe beside the automatic path", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(transport.listGets).toEqual(["/v1/runs/run-a/logs", "/v1/runs/run-a/logs"]);
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]);
    expect(harness.logsStatus()).toContain("snapshot revision 24");
    expect(harness.logsEmpty()).toBe("");
    harness.dispose();
  });

  test("a pending execution is selectable but the chunk button stays hidden", async () => {
    const transport = makeLogsFetch({ inspect: { revision: 24, boundary: { kind: "in_flight" } }, list: { revision: 24, metadata: defaultMetadata("pending") } });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true);
    expect(transport.chunkGets).toHaveLength(0);
    harness.dispose();
  });

  test("the captured chunk loads one bounded GET and renders the safe text", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(transport.chunkGets).toEqual(["/v1/runs/run-a/logs/1?offset=0&limit=65536"]);
    expect(harness.logsContent()).toBe("hello world");
    expect(harness.logsCursor()).toContain("execution 1");
    expect(harness.logsCursor()).toContain("bytes 0–11 (end)");
    expect(harness.logsHint()).toBe("Captured artifact; upstream completeness is not guaranteed");
    harness.dispose();
  });

  test("a settled succeeded run loads the durable log exactly like a running one", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "succeeded" });
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]); // the automatic metadata
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(transport.chunkGets).toEqual(["/v1/runs/run-a/logs/1?offset=0&limit=65536"]);
    expect(harness.logsContent()).toBe("hello world");
    expect(harness.logsStatus()).not.toContain("failed");
    harness.dispose();
  });

  test("a succeeded run with two captured logs serves and loads both", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 40, boundary: { kind: "succeeded" } },
      list: { revision: 40, metadata: [capturedMetadataAt(1), capturedMetadataAt(2)] },
      chunks: [
        { revision: 40, chunk: { ...chunkBody(1, "architect-plan"), next_offset: null } },
        { revision: 40, chunk: { ...chunkBody(2, "execute-result"), next_offset: null } },
      ],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    // both captured executions render on the succeeded boundary
    expect(harness.logsRows()).toEqual([
      `1|architect|captured|11||`,
      `2|execute|captured|11||`,
    ]);
    expect(harness.logsStatus()).toContain("snapshot revision 40");
    // the first execution loads its own artifact
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("architect-plan");
    expect(transport.chunkGets).toEqual(["/v1/runs/run-a/logs/1?offset=0&limit=65536"]);
    // the second execution loads its own artifact through the same flow
    viewButtonOfRow(harness, 1).click();
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("execute-result");
    expect(transport.chunkGets).toEqual([
      "/v1/runs/run-a/logs/1?offset=0&limit=65536",
      "/v1/runs/run-a/logs/2?offset=0&limit=65536",
    ]);
    harness.dispose();
  });

  test("a failed run serves captured, truncated and unavailable executions accordingly", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 50, boundary: { kind: "failed" } },
      list: {
        revision: 50,
        metadata: [
          capturedMetadataAt(1),
          { execution_index: 2, state_id: "review", status: "truncated", sha256: "c".repeat(64), byte_count: 40, truncated_daemon: true, truncated_local: false },
          { execution_index: 3, state_id: "fix", status: "unavailable" },
        ],
      },
      chunks: [
        { revision: 50, chunk: { ...chunkBody(1, "captured-text"), next_offset: null } },
        { revision: 50, chunk: { ...chunkBody(2, "truncated-tail"), status: "truncated", truncated_daemon: true } },
      ],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    expect(harness.logsRows()).toEqual([
      "1|architect|captured|11||",
      "2|review|truncated|40|yes|",
      "3|fix|unavailable|||",
    ]);
    // captured: loads
    viewButtonOfRow(harness, 0).click();
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("captured-text");
    // truncated: loads its tail
    viewButtonOfRow(harness, 1).click();
    await Bun.sleep(10);
    expect(harness.logsHint()).toBe("Captured tail; earlier bytes were discarded");
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("truncated-tail");
    // unavailable: never chunkable
    viewButtonOfRow(harness, 2).click();
    await Bun.sleep(10);
    expect(harness.logsHint()).toBe("content was not durably captured");
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true);
    expect(transport.chunkGets).toHaveLength(2); // only captured and truncated
    harness.dispose();
  });

  test("a logs failure stays isolated from inspect, events and mutations", async () => {
    const transport = makeLogsFetch({
      inspect: [
        { revision: 24, boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 1 } },
        { revision: 30, boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 1 } },
        { revision: 31, boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 1 } },
      ],
      list: { revision: 24, fail: "network" },
      events: { revision: 2, kinds: ["create_run", "start_agent_execution"] },
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    // the automatic metadata failed; the failure is the logs section's own
    expect(harness.logsStatus()).toContain("transport: network_error");
    // (a) a later inspect still renders the run detail
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(harness.detailStatus()).toBe("running");
    expect(harness.element("detail-revision").textContent).toBe("30");
    // (b) the events timeline still loaded its initial page
    expect(harness.eventsRows().length).toBe(2);
    expect(harness.eventsRows()[0]).toBe("1|create_run|");
    // (c) a mutating POST still works end to end
    harness.clickButton("run-controls", "Resume run");
    await Bun.sleep(10);
    expect(transport.posts).toEqual(["/v1/runs/run-a/resume"]);
    expect(harness.detailStatus()).toBe("running"); // the recovery inspect rendered
    // the logs failure never leaked into the other surfaces
    expect(harness.error()).toBe("");
    expect(harness.logsStatus()).toContain("transport: network_error");
    harness.dispose();
  });

  test("the full logs flow issues zero mutating POSTs", async () => {
    const transport = makeLogsFetch({
      inspect: [
        { revision: 24, boundary: { kind: "in_flight" } },
        { revision: 30, boundary: { kind: "in_flight" } },
      ],
      list: [{ revision: 24 }, { revision: 30 }],
      chunks: [{ revision: 24, fail: "network" }, { revision: 24 }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk"); // fails
    await Bun.sleep(10);
    harness.click("logs-retry"); // succeeds
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("hello world");
    harness.click("inspect-selected"); // revision 30: one more automatic metadata refresh
    await Bun.sleep(10);
    expect(transport.listGets).toHaveLength(2); // the boot GET + the revision-30 refresh
    expect(transport.chunkGets).toHaveLength(2);
    expect(transport.posts).toEqual([]); // the read-only viewer never mutates
    harness.dispose();
  });

  test("waiting and plan-ready boundaries read the durable log the same way", async () => {
    for (const boundary of [{ kind: "waiting" }, { kind: "plan_ready" }]) {
      const { harness, transport } = await bootWithInspect(boundary);
      harness.clickButton("logs-body", "View");
      await Bun.sleep(10);
      harness.click("logs-next-chunk");
      await Bun.sleep(10);
      expect(transport.chunkGets).toHaveLength(1);
      expect(harness.logsContent()).toBe("hello world");
      harness.dispose();
    }
  });

  test("failed and resumable boundaries read the durable log the same way", async () => {
    for (const boundary of [{ kind: "failed" }, { kind: "resumable" }]) {
      const { harness, transport } = await bootWithInspect(boundary);
      expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]); // the automatic metadata
      harness.clickButton("logs-body", "View");
      await Bun.sleep(10);
      harness.click("logs-next-chunk");
      await Bun.sleep(10);
      expect(transport.chunkGets).toHaveLength(1);
      expect(harness.logsContent()).toBe("hello world");
      harness.dispose();
    }
  });

  test("a network failure on the list shows the typed failure and no content", async () => {
    const transport = makeLogsFetch({ inspect: { revision: 24, boundary: { kind: "in_flight" } }, list: { revision: 24, fail: "network" } });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(harness.logsStatus()).toContain("transport: network_error");
    expect(harness.logsRows()).toEqual([]);
    harness.dispose();
  });

  test("endpoint failures surface as layer: reason without content", async () => {
    const transport = makeLogsFetch({ inspect: { revision: 24, boundary: { kind: "in_flight" } }, list: { revision: 24, fail: "removed_project" } });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(harness.logsStatus()).toContain("binding: project_unavailable");
    harness.dispose();
  });

  test("a chunk network failure offers Retry, which re-requests exactly once", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      chunks: [{ revision: 24, fail: "network" }, { revision: 24 }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsStatus()).toContain("transport: network_error");
    expect(harness.logsButtonState("logs-retry").hidden).toBe(false);
    harness.click("logs-retry");
    await Bun.sleep(10);
    expect(transport.chunkGets).toHaveLength(2);
    expect(harness.logsContent()).toBe("hello world");
    expect(harness.logsButtonState("logs-retry").hidden).toBe(true);
    harness.dispose();
  });

  test("re-inspecting the same run keeps the loaded logs state", async () => {
    const transport = makeLogsFetch({ inspect: { revision: 24, boundary: { kind: "in_flight" } } });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(harness.logsRows()).toHaveLength(1);
    // a re-inspect of the same run never resets the section and never
    // re-fetches at the same revision (automatic: 1 + manual: 1)
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(harness.logsRows()).toHaveLength(1);
    expect(transport.listGets).toHaveLength(2);
    harness.dispose();
  });

  test("an unchanged immutable binding preserves the loaded content across the automatic refresh", async () => {
    const transport = makeLogsFetch({
      inspect: [
        { revision: 24, boundary: { kind: "in_flight" } },
        { revision: 30, boundary: { kind: "in_flight" } },
      ],
      list: [{ revision: 24 }, { revision: 30 }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("hello world");
    // the applied inspect at revision 30 observes the identical binding
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(transport.listGets).toHaveLength(2);
    expect(harness.logsStatus()).toContain("snapshot revision 30");
    expect(harness.logsContent()).toBe("hello world"); // the loaded content survived
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true); // fully read
    harness.dispose();
  });

  test("a changed immutable binding fails closed as invalid_log_binding", async () => {
    const transport = makeLogsFetch({
      inspect: [
        { revision: 24, boundary: { kind: "in_flight" } },
        { revision: 30, boundary: { kind: "in_flight" } },
      ],
      list: [
        { revision: 24 },
        {
          revision: 30,
          metadata: defaultMetadata("captured").map((entry) => ({ ...entry, sha256: "b".repeat(64) })),
        },
      ],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsContent()).toBe("hello world");
    // the applied inspect observes a different artifact digest: the
    // mixed-bytes hazard fails closed before the list is accepted
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(harness.logsStatus()).toContain("viewer: invalid_log_binding");
    expect(harness.logsContent()).toBe("hello world"); // never silently mixed
    harness.dispose();
  });

  test("a stale chunk response of another run never renders", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      chunks: [{ revision: 24, fail: "contract" }, { revision: 24 }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    // the foreign-run document settles as a typed contract failure
    expect(harness.logsStatus()).toContain("contract: run_mismatch");
    expect(harness.logsContent()).toBe("");
    harness.dispose();
  });

  test("the metadata rows render every status of the union", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      list: {
        revision: 24,
        metadata: [
          ...defaultMetadata("pending").map((entry) => ({ ...entry, execution_index: 1 })),
          ...defaultMetadata("unavailable").map((entry) => ({ ...entry, execution_index: 2 })),
          ...defaultMetadata("captured").map((entry) => ({ ...entry, execution_index: 3 })),
          ...defaultMetadata("truncated").map((entry) => ({ ...entry, execution_index: 4, truncated_daemon: true })),
        ],
      },
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(harness.logsRows()).toEqual([
      "1|architect|pending|||",
      "2|architect|unavailable|||",
      "3|architect|captured|11||",
      "4|architect|truncated|11|yes|",
    ]);
    harness.dispose();
  });

  test("the status hint explains every union status verbatim", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      list: {
        revision: 24,
        metadata: [
          ...defaultMetadata("pending").map((entry) => ({ ...entry, execution_index: 1 })),
          ...defaultMetadata("unavailable").map((entry) => ({ ...entry, execution_index: 2 })),
          ...defaultMetadata("captured").map((entry) => ({ ...entry, execution_index: 3 })),
          ...defaultMetadata("truncated").map((entry) => ({ ...entry, execution_index: 4, truncated_daemon: true })),
        ],
      },
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    const buttons = harness.element("logs-body").querySelectorAll("button");
    const expectations = [
      "execution has no terminal durable log binding yet",
      "content was not durably captured",
      "Captured artifact; upstream completeness is not guaranteed",
      "Captured tail; earlier bytes were discarded",
    ];
    for (let i = 0; i < buttons.length; i += 1) {
      buttons[i]!.click();
      await Bun.sleep(5);
      expect(harness.logsHint()).toBe(expectations[i]!);
    }
    harness.dispose();
  });

  test("the truncated flag renders and the chunk content stays bounded", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      list: { revision: 24, metadata: defaultMetadata("truncated").map((entry) => ({ ...entry, byte_count: 100, truncated_daemon: true })) },
      chunks: [{
        revision: 24,
        chunk: {
          schema_version: 1,
          kind: "worker_log",
          run_id: "run-a",
          revision: 24,
          execution_index: 1,
          state_id: "architect",
          status: "truncated",
          sha256: "a".repeat(64),
          byte_count: 100,
          truncated_daemon: true,
          truncated_local: false,
          offset: 0,
          data_base64: b64("part"),
          next_offset: 4,
        },
      }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(harness.logsRows()).toEqual(["1|architect|truncated|100|yes|"]);
    expect(harness.logsContent()).toBe("part");
    expect(harness.logsCursor()).toContain("bytes 0–4 of 100");
    expect(harness.logsCursor()).toContain("truncated");
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(false); // a tail exists
    harness.dispose();
  });

  test("the safe render contract holds on the real wiring: no markup from the payload", async () => {
    const transport = makeLogsFetch({
      inspect: { revision: 24, boundary: { kind: "in_flight" } },
      chunks: [{
        revision: 24,
        chunk: {
          schema_version: 1,
          kind: "worker_log",
          run_id: "run-a",
          revision: 24,
          execution_index: 1,
          state_id: "architect",
          status: "captured",
          sha256: "a".repeat(64),
          byte_count: 31,
          truncated_daemon: false,
          truncated_local: false,
          offset: 0,
          data_base64: b64("<script>alert(1)</script>\u0000\u001b[31m"),
          next_offset: null,
        },
      }],
    });
    const harness = await createUiHarness({ fetchImpl: transport.fetchImpl });
    harness.click("refresh-runs");
    await Bun.sleep(10);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    const text = harness.logsContent();
    expect(text).toContain("<script>alert(1)</script>"); // rendered as text, never markup
    expect(text).toContain("\u0000");
    expect(text).toContain("\u001b[31m");
    harness.dispose();
  });
});
