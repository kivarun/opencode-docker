/**
 * Permanent wiring suite of the browser UI v5 worker-log viewer: the
 * fake-DOM harness boots the shipped `app.js` and drives the production
 * call chains (run selection → explicit Load logs → execution
 * selection → explicit Load next chunk under the capture gate → Retry),
 * with an instrumented fetch and the real logs endpoint documents.
 * Every test proves the manual-only loading, the capture gate, the
 * stale-response isolation and the safe render on the real wiring.
 */
import { describe, expect, test } from "bun:test";
import {
  createUiHarness,
  jsonResponse,
  projectsDocument,
  runDocument,
  runsPageDocument,
} from "./ui_dom_harness.ts";

const b64 = (text: string): string => Buffer.from(text, "utf8").toString("base64");

interface LogsPlan {
  readonly revision: number;
  readonly metadata?: readonly Record<string, unknown>[];
  readonly chunk?: Record<string, unknown>;
  readonly fail?: "network" | "not_found" | "removed_project" | "contract";
  readonly kind?: string;
}

const defaultMetadata = (status = "captured"): Record<string, unknown>[] => {
  const base = { execution_index: 1, state_id: "architect", status };
  if (status === "captured" || status === "truncated") {
    return [{ ...base, sha256: "a".repeat(64), byte_count: 11, truncated_daemon: false, truncated_local: false }];
  }
  return [base];
};

function makeLogsFetch(plans: {
  readonly list?: LogsPlan;
  readonly chunks?: readonly LogsPlan[];
  readonly inspect?: { revision: number; boundary: Record<string, unknown> };
}) {
  const listSeq = { index: 0 };
  const chunkSeq = { index: 0 };
  const listGets: string[] = [];
  const chunkGets: string[] = [];
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(["run-a"]));
    }
    if (path.startsWith("/v1/runs/run-a/logs")) {
      if (path === "/v1/runs/run-a/logs") {
        listGets.push(path);
        const plan = plans.list ?? { revision: 24 };
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
      const plan = plans.inspect ?? { revision: 24, boundary: { kind: "in_flight" } };
      const status = plan.boundary.kind === "succeeded" ? "succeeded" : plan.boundary.kind === "failed" ? "failed" : "running";
      return jsonResponse(runDocument("run-a", plan.revision, status, plan.boundary));
    }
    throw new Error(`unexpected fetch path: ${path}`);
  };
  return { fetchImpl, listGets, chunkGets };
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
  test("the section appears on selection, empty, and never loads automatically", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    expect(harness.element("worker-logs-section").hidden).toBe(false);
    expect(transport.listGets).toHaveLength(0); // nothing loads until the button
    expect(harness.logsEmpty()).toContain("not loaded yet");
    expect(harness.logsStatus()).toBe("");
    harness.dispose();
  });

  test("Load logs renders the metadata table and the snapshot revision", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "in_flight" });
    harness.click("logs-load");
    await Bun.sleep(10);
    expect(transport.listGets).toEqual(["/v1/runs/run-a/logs"]);
    expect(harness.logsRows()).toEqual(["1|architect|captured|11||"]);
    expect(harness.logsStatus()).toContain("snapshot revision 24");
    expect(harness.logsEmpty()).toBe("");
    // the chunk button stays hidden until an execution is selected
    expect(harness.logsButtonState("logs-next-chunk").hidden).toBe(true);
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
    console.log("DEBUG cursor:", JSON.stringify(harness.logsCursor()), "status:", JSON.stringify(harness.logsStatus()), "pre hidden:", harness.element("logs-content").hidden);
    expect(harness.logsContent()).toBe("hello world");
    expect(harness.logsCursor()).toContain("execution 1");
    expect(harness.logsCursor()).toContain("bytes 0–11 (end)");
    harness.dispose();
  });

  test("the chunk gate refuses a settled run without a network call", async () => {
    const { harness, transport } = await bootWithInspect({ kind: "succeeded" });
    harness.click("logs-load");
    await Bun.sleep(10);
    harness.clickButton("logs-body", "View");
    await Bun.sleep(10);
    harness.click("logs-next-chunk");
    await Bun.sleep(10);
    expect(transport.chunkGets).toHaveLength(0);
    expect(harness.logsStatus()).toContain("viewer: capture_gate_refused");
    harness.dispose();
  });

  test("the chunk gate admits waiting and plan-ready boundaries", async () => {
    for (const boundary of [{ kind: "waiting" }, { kind: "plan_ready" }]) {
      const { harness, transport } = await bootWithInspect(boundary);
      harness.click("logs-load");
      await Bun.sleep(10);
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
    // a re-inspect of the same run never resets the section
    harness.click("inspect-selected");
    await Bun.sleep(10);
    expect(harness.logsRows()).toHaveLength(1);
    expect(transport.listGets).toHaveLength(1); // no automatic reload
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
