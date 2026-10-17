/**
 * Permanent contract suite of the browser UI v5 worker-log viewer
 * model: the one logs document parsers (`worker_logs` metadata union
 * and `worker_log` status/chunk shapes with the run binding and the
 * raw-byte cursor coherence), the one URL builder, the bounded
 * single-flight logs transport (superseding aborts, disposal, no
 * auto-retry), the closed viewer state machine (explicit list load,
 * explicit chunk load under the capture gate, stale-response drops,
 * one-click retry) and the safe render contract — plus the source
 * restrictions of the logs layer.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  UiContractError,
  createLogsTransport,
  createWorkerLogViewer,
  createWorkerLogsState,
  decodeLogChunkText,
  logsCaptureGate,
  parseApiDocument,
  parseWorkerLogDocument,
  parseWorkerLogsDocument,
  workerLogsRequestPath,
} from "../ui/app.js";
import type { UiWorkerLogMetadata, UiWorkerLogsState } from "../ui/app.js";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

const contractReason = (run: () => unknown): string => {
  try {
    run();
    return "no_error";
  } catch (error) {
    if (error instanceof UiContractError) {
      return error.reason;
    }
    return `unexpected: ${String(error)}`;
  }
};

const metadata = (status: string, extra: Record<string, unknown> = {}): UiWorkerLogMetadata =>
  ({
    execution_index: 1,
    state_id: "architect",
    status,
    ...extra,
  }) as unknown as UiWorkerLogMetadata;

const b64 = (text: string): string => Buffer.from(text, "utf8").toString("base64");

const chunkDocument = (overrides: Record<string, unknown> = {}): Record<string, unknown> => {
  const data = (overrides["__data"] as string | undefined) ?? "hello\0world";
  const base: Record<string, unknown> = {
    schema_version: 1,
    kind: "worker_log",
    run_id: "run-a",
    revision: 24,
    execution_index: 1,
    state_id: "architect",
    status: "captured",
    sha256: "a".repeat(64),
    byte_count: data.length,
    truncated_daemon: false,
    truncated_local: false,
    offset: 0,
    data_base64: b64(data),
    next_offset: null,
  };
  delete base["__data"];
  return { ...base, ...overrides };
};

describe("worker logs document parsers", () => {
  test("the metadata list parses with the run binding and exact keys", () => {
    const model = parseWorkerLogsDocument(
      {
        schema_version: 1,
        kind: "worker_logs",
        run_id: "run-a",
        revision: 24,
        logs: [metadata("pending"), metadata("captured", { sha256: "b".repeat(64), byte_count: 10, truncated_daemon: false, truncated_local: true })],
      },
      "run-a",
    );
    expect(model.kind).toBe("worker_logs");
    expect(model.runId).toBe("run-a");
    expect(model.revision).toBe(24);
    expect(model.logs).toHaveLength(2);
    expect(model.logs[0]).toEqual({ executionIndex: 1, stateId: "architect", status: "pending" });
    expect(model.logs[1]).toEqual({
      executionIndex: 1,
      stateId: "architect",
      status: "captured",
      sha256: "b".repeat(64),
      byteCount: 10,
      truncatedDaemon: false,
      truncatedLocal: true,
    });
  });

  test("a foreign run id is rejected as run_mismatch", () => {
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "other", revision: 1, logs: [] }, "run-a"))).toBe("run_mismatch");
  });

  test("the metadata union rejects unknown statuses and key drift", () => {
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [metadata("complete")] }, "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [{ ...metadata("pending"), digest: "x" }] }, "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [{ ...metadata("captured", { sha256: "b".repeat(64) }) }] }, "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [metadata("captured", { sha256: "B".repeat(64), byte_count: 1, truncated_daemon: false, truncated_local: false })] }, "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogsDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [metadata("captured", { sha256: "b".repeat(64), byte_count: -1, truncated_daemon: false, truncated_local: false })] }, "run-a"))).toBe("invalid_shape");
  });

  test("the general API client still rejects both logs kinds", () => {
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 1, logs: [] }))).toBe("unknown_kind");
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "worker_log", run_id: "run-a", revision: 1, execution_index: 1, state_id: "s", status: "captured" }))).toBe("unknown_kind");
  });

  test("a pending/unavailable status document carries no content keys", () => {
    const model = parseWorkerLogDocument(
      { schema_version: 1, kind: "worker_log", run_id: "run-a", revision: 7, execution_index: 2, state_id: "execute", status: "unavailable" },
      "run-a",
    );
    expect(model).toEqual({ kind: "worker_log", runId: "run-a", revision: 7, executionIndex: 2, stateId: "execute", status: "unavailable" });
    expect(contractReason(() => parseWorkerLogDocument({ schema_version: 1, kind: "worker_log", run_id: "run-a", revision: 7, execution_index: 2, state_id: "execute", status: "pending", data_base64: b64("x") }, "run-a"))).toBe("invalid_shape");
  });

  test("a full-chunk document parses with the raw-byte cursor fields", () => {
    const model = parseWorkerLogDocument(chunkDocument(), "run-a");
    expect(model.status).toBe("captured");
    expect(model.offset).toBe(0);
    expect(model.byteCount).toBe(11);
    expect(model.nextOffset).toBe(null);
    expect(model.dataBase64).toBe(b64("hello\0world"));
  });

  test("a continuation chunk validates the next_offset coherence", () => {
    // offset 4, five bytes, tail exists → next_offset must be exactly 9
    const model = parseWorkerLogDocument(chunkDocument({ offset: 4, byte_count: 11, data_base64: b64("hello"), next_offset: 9 }), "run-a");
    expect(model.nextOffset).toBe(9);
    // a next_offset that is not the end of the slice is rejected
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ offset: 4, byte_count: 11, data_base64: b64("hello"), next_offset: 10 }), "run-a"))).toBe("invalid_shape");
    // an empty chunk with a tail is rejected
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ offset: 4, byte_count: 11, data_base64: b64(""), next_offset: 9 }), "run-a"))).toBe("invalid_shape");
    // a chunk longer than the remaining bytes is rejected
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ offset: 8, byte_count: 11, data_base64: b64("12345"), next_offset: null }), "run-a"))).toBe("invalid_shape");
    // a final chunk must either end at byte_count or fill the tail
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ offset: 8, byte_count: 11, data_base64: b64("ab"), next_offset: null }), "run-a"))).toBe("invalid_shape");
    expect(parseWorkerLogDocument(chunkDocument({ offset: 8, byte_count: 11, data_base64: b64("abc"), next_offset: null }), "run-a").nextOffset).toBe(null);
    // offset beyond byte_count is rejected
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ offset: 12, byte_count: 11, data_base64: b64(""), next_offset: null }), "run-a"))).toBe("invalid_shape");
  });

  test("malformed base64 and foreign fields are rejected", () => {
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ data_base64: "not!base64" }), "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogDocument({ ...chunkDocument(), extra: 1 }, "run-a"))).toBe("invalid_shape");
    expect(contractReason(() => parseWorkerLogDocument(chunkDocument({ run_id: "other" }), "run-a"))).toBe("run_mismatch");
  });
});

describe("worker logs URL builder", () => {
  test("the list path and the chunk path are exact", () => {
    expect(workerLogsRequestPath("run-a", null, 0, 0)).toBe("/v1/runs/run-a/logs");
    expect(workerLogsRequestPath("run-a", 2, 65536, 65536)).toBe("/v1/runs/run-a/logs/2?offset=65536&limit=65536");
  });

  test("invalid scalars are refused without building a path", () => {
    expect(contractReason(() => workerLogsRequestPath("../escape", null, 0, 0))).toBe("invalid_run_id");
    expect(contractReason(() => workerLogsRequestPath("run-a", 0, 0, 0))).toBe("invalid_execution_index");
    expect(contractReason(() => workerLogsRequestPath("run-a", 1, -1, 0))).toBe("invalid_offset");
    expect(contractReason(() => workerLogsRequestPath("run-a", 1, 0, 0))).toBe("invalid_limit");
    expect(contractReason(() => workerLogsRequestPath("run-a", 1, 0, 262145))).toBe("invalid_limit");
  });
});

describe("the bounded single-flight logs transport", () => {
  type Call = { runId: string; path: string; signal: AbortSignal };
  const makeIo = () => {
    const calls: Call[] = [];
    const gates: ((result: unknown) => void)[] = [];
    const started: string[] = [];
    const settled: string[] = [];
    const io = {
      fetchLogs: (runId: string, path: string, signal: AbortSignal) => {
        calls.push({ runId, path, signal });
        return new Promise((resolve) => {
          gates.push((result) => resolve(result));
        }) as never;
      },
      onStarted: (runId: string) => started.push(runId),
      onSettled: (runId: string) => settled.push(runId),
    };
    return { calls, gates, started, settled, io };
  };

  test("one desire at a time; a second desire supersedes the first and runs after it", async () => {
    const { calls, gates, started, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const first = transport.request({ runId: "run-a", path: "/v1/runs/run-a/logs", onApplied: (r) => applied.push(`first:${String(r !== undefined)}`) });
    const second = transport.request({ runId: "run-a", path: "/v1/runs/run-a/logs", onApplied: (r) => applied.push(`second:${String(r !== undefined)}`) });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.signal.aborted).toBe(true);
    gates[0]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 1, logs: [] } });
    await first;
    expect(applied).toEqual([]); // the superseded response is dropped
    expect(calls).toHaveLength(2); // the coalesced follow-up starts after the slot frees
    gates[1]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 1, logs: [] } });
    await second;
    expect(applied).toEqual(["second:true"]);
    expect(started).toEqual(["run-a", "run-a"]);
  });

  test("invalidate withdraws the active and queued desires without effects", async () => {
    const { calls, gates, settled, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const first = transport.request({ runId: "run-a", path: "/p", onApplied: () => applied.push("first") });
    transport.request({ runId: "run-a", path: "/p", onApplied: () => applied.push("second") });
    transport.invalidate();
    gates[0]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 1, logs: [] } });
    await first;
    expect(applied).toEqual([]);
    expect(calls).toHaveLength(1); // the queued desire was withdrawn
    // the superseded settlement still frees the wire slot (buttons re-enable)
    expect(settled).toEqual(["run-a"]);
  });

  test("dispose refuses every later desire", async () => {
    const { calls, io } = makeIo();
    const transport = createLogsTransport(io);
    transport.dispose();
    const result = await transport.request({ runId: "run-a", path: "/p", onApplied: () => undefined });
    expect(result).toBeUndefined();
    expect(calls).toHaveLength(0);
  });

  test("invalid io shapes are refused before any state", () => {
    expect(contractReason(() => createLogsTransport({} as never))).toBe("invalid_transport_io");
    expect(contractReason(() => createLogsTransport({ fetchLogs: 1, onStarted: () => undefined, onSettled: () => undefined } as never))).toBe("invalid_transport_io");
  });
});

describe("the closed viewer state machine", () => {
  type Harness = {
    viewer: ReturnType<typeof createWorkerLogViewer>;
    states: (UiWorkerLogsState | null)[];
    snapshot: { boundaryKind: string | null };
    mutating: { inFlight: boolean };
    gates: ((result: unknown) => void)[];
    transport: ReturnType<typeof createLogsTransport>;
  };
  const makeViewer = (runId = "run-a"): Harness => {
    const gates: ((result: unknown) => void)[] = [];
    const transport = createLogsTransport({
      fetchLogs: () => new Promise((resolve) => { gates.push(resolve as (r: unknown) => void); }) as never,
      onStarted: () => undefined,
      onSettled: () => undefined,
    });
    const states: (UiWorkerLogsState | null)[] = [];
    const snapshot = { boundaryKind: "in_flight" as string | null };
    const mutating = { inFlight: false };
    const viewer = createWorkerLogViewer({
      transport,
      onState: (state) => states.push(state),
      snapshot: () => ({ boundaryKind: snapshot.boundaryKind }),
      mutatingInFlight: () => mutating.inFlight,
    });
    viewer.transitionToLogsView(runId);
    return { viewer, states, snapshot, mutating, gates, transport };
  };

  const okList = (revision = 24): unknown => ({
    ok: true,
    model: {
      kind: "worker_logs",
      runId: "run-a",
      revision,
      logs: [
        { executionIndex: 1, stateId: "architect", status: "captured", sha256: "a".repeat(64), byteCount: 11, truncatedDaemon: false, truncatedLocal: false },
        { executionIndex: 2, stateId: "execute", status: "pending" },
      ],
    },
  });

  test("the transition creates the empty state and publishes it", () => {
    const { viewer, states } = makeViewer();
    expect(states).toHaveLength(1);
    expect(states[0]).toEqual(createWorkerLogsState("run-a"));
    expect(viewer.currentState()).toEqual(createWorkerLogsState("run-a"));
  });

  test("the list loads only on the explicit press and binds run/revision", async () => {
    const { viewer, states, gates } = makeViewer();
    viewer.fetchLogsList();
    expect(states.at(-1)!.requestState).toBe("loading");
    gates[0]!(okList());
    await Bun.sleep(1);
    const state = viewer.currentState()!;
    expect(state.requestState).toBe("ready");
    expect(state.snapshotRevision).toBe(24);
    expect(state.logs).toHaveLength(2);
    expect(states.at(-1)!.requestState).toBe("ready");
  });

  test("a list response of another run is dropped before it becomes state", async () => {
    const { viewer, states, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!({
      ok: true,
      model: { kind: "worker_logs", runId: "run-b", revision: 5, logs: [{ executionIndex: 9, stateId: "x", status: "pending" }] },
    });
    await Bun.sleep(1);
    const state = viewer.currentState()!;
    expect(state.logs).toHaveLength(0); // the stale response never applied
    expect(state.requestState).toBe("failed"); // settled as a typed contract failure, never stuck
    expect(state.error).toEqual({ layer: "contract", reason: "unexpected_kind" });
  });

  test("the selection clears content and rejects unknown executions", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList());
    await Bun.sleep(1);
    viewer.selectExecution(1);
    expect(viewer.currentState()!.selectedExecutionIndex).toBe(1);
    viewer.selectExecution(9);
    expect(viewer.currentState()!.selectedExecutionIndex).toBe(1);
    viewer.selectExecution(2); // pending: selectable, but never chunkable
    expect(viewer.currentState()!.selectedExecutionIndex).toBe(2);
  });

  test("the chunk loads only under the capture gate and only for captured/truncated", async () => {
    const { viewer, states, gates, snapshot, mutating } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList());
    await Bun.sleep(1);
    viewer.selectExecution(2); // pending
    viewer.fetchChunk();
    expect(viewer.currentState()!.requestState).toBe("ready"); // refused silently: no content for pending
    viewer.selectExecution(1);
    mutating.inFlight = true;
    viewer.fetchChunk();
    expect(viewer.currentState()!.requestState).toBe("ready"); // refused while a mutation is in flight
    mutating.inFlight = false;
    snapshot.boundaryKind = "succeeded";
    viewer.fetchChunk();
    const failed = viewer.currentState()!;
    expect(failed.requestState).toBe("failed");
    expect(failed.error).toEqual({ layer: "viewer", reason: "capture_gate_refused" });
    expect(states.filter((s) => s!.requestState === "failed")).toHaveLength(1);
    snapshot.boundaryKind = "in_flight";
    viewer.fetchChunk();
    expect(viewer.currentState()!.requestState).toBe("loading");
    gates[1]!({ ok: true, model: parseWorkerLogDocument({ ...chunkDocument(), offset: 0, byte_count: 11, data_base64: b64("hello\0world"), next_offset: null }, "run-a") });
    await Bun.sleep(5);
    const state = viewer.currentState()!;
    expect(state.content).not.toBeNull();
    expect(state.content!.offset).toBe(0);
    expect(state.requestState).toBe("ready");
  });

  test("a chunk response of another execution or run is dropped", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList());
    await Bun.sleep(1);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: true, model: parseWorkerLogDocument({ ...chunkDocument(), execution_index: 2 }, "run-a") });
    await Bun.sleep(5);
    expect(viewer.currentState()!.content).toBeNull();
    expect(viewer.currentState()!.requestState).toBe("failed");
    viewer.fetchChunk();
    gates[2]!({ ok: true, model: parseWorkerLogDocument({ ...chunkDocument(), run_id: "run-b" }, "run-b") });
    await Bun.sleep(5);
    expect(viewer.currentState()!.content).toBeNull();
  });

  test("a terminal chunk disables the next request", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList());
    await Bun.sleep(1);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: true, model: parseWorkerLogDocument(chunkDocument(), "run-a") });
    await Bun.sleep(5);
    const gate = logsCaptureGate(viewer.currentState(), "in_flight", () => false);
    expect(gate).toBe(false); // next_offset === null: no further chunk
  });

  test("the gate admits only active boundaries and refuses mutations", () => {
    const state = createWorkerLogsState("run-a") as UiWorkerLogsState;
    const loaded = { ...state, logs: [{ executionIndex: 1, stateId: "s", status: "captured" }], selectedExecutionIndex: 1 } as UiWorkerLogsState;
    expect(logsCaptureGate(loaded, "in_flight", () => false)).toBe(true);
    expect(logsCaptureGate(loaded, "plan_ready", () => false)).toBe(true);
    expect(logsCaptureGate(loaded, "waiting", () => false)).toBe(true);
    expect(logsCaptureGate(loaded, "resumable", () => false)).toBe(false);
    expect(logsCaptureGate(loaded, "succeeded", () => false)).toBe(false);
    expect(logsCaptureGate(loaded, "failed", () => false)).toBe(false);
    expect(logsCaptureGate(loaded, "in_flight", () => true)).toBe(false);
    expect(logsCaptureGate(null, "in_flight", () => false)).toBe(false);
  });

  test("a failed chunk desire retries exactly once per click", async () => {
    const { viewer, gates, states } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList());
    await Bun.sleep(1);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: false, failure: { layer: "transport", reason: "network_error" } });
    await Bun.sleep(5);
    expect(viewer.currentState()!.requestState).toBe("failed");
    expect(viewer.currentState()!.error).toEqual({ layer: "transport", reason: "network_error" });
    viewer.retryLastChunkRequest();
    expect(viewer.currentState()!.requestState).toBe("loading");
    gates[2]!({ ok: true, model: parseWorkerLogDocument(chunkDocument(), "run-a") });
    await Bun.sleep(5);
    expect(viewer.currentState()!.content).not.toBeNull();
    // list ready + the selection re-publish of the ready state + retry ready
    expect(states.filter((s) => s!.requestState === "ready")).toHaveLength(3);
    // a retry outside a failed state is a no-op
    viewer.retryLastChunkRequest();
    expect(viewer.currentState()!.content).not.toBeNull();
  });

  test("a run change discards the viewer state and the wire", async () => {
    const { viewer, gates, transport } = makeViewer();
    viewer.fetchLogsList();
    viewer.transitionToLogsView("run-b");
    const state = viewer.currentState()!;
    expect(state.runId).toBe("run-b");
    expect(state.logs).toHaveLength(0);
    expect(state.requestState).toBe("idle");
    // the in-flight list of run-a settles after the change: superseded, dropped
    gates[0]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 9, logs: [{ executionIndex: 3, stateId: "s", status: "pending" }] } });
    await Bun.sleep(5);
    expect(viewer.currentState()!.logs).toHaveLength(0);
    expect(viewer.currentState()!.requestState).toBe("idle"); // the new run's state owns the UI
    void transport;
  });

  test("the viewer refuses invalid io and unsafe run ids", () => {
    expect(contractReason(() => createWorkerLogViewer({} as never))).toBe("invalid_viewer_io");
    const { viewer } = makeViewer();
    expect(contractReason(() => viewer.transitionToLogsView("../escape"))).toBe("invalid_run_id");
  });
});

describe("the safe render contract", () => {
  test("the decoded text preserves NUL, ANSI escapes and invalid UTF-8 bytes", () => {
    const raw = new Uint8Array([0x61, 0x00, 0x1b, 0x5b, 0x33, 0x31, 0x6d, 0xff, 0xfe, 0x62]);
    const text = decodeLogChunkText(Buffer.from(raw).toString("base64"));
    expect(text).toContain("a");
    expect(text).toContain("\u001b[31m");
    expect(text).toContain("\u0000");
    expect(text).toContain("\uFFFD"); // invalid UTF-8 becomes the replacement character
    expect(text).toContain("b");
  });

  test("the rendered text is safe by construction: the source never builds markup from the payload", () => {
    expect(appSource).not.toContain("innerHTML");
    expect(appSource).not.toContain("insertAdjacentHTML");
    expect(appSource).not.toContain("document.write");
    // the content frame renders only through textContent
    expect(appSource).toContain('pre.textContent = decodeLogChunkText(content.dataBase64)');
  });
});

describe("the logs layer source restrictions", () => {
  test("the logs section is manual-only: no auto-load, no polling, no streaming", () => {
    expect(appSource).not.toContain("setInterval");
    expect(appSource).not.toContain("EventSource");
    expect(appSource).not.toContain("WebSocket");
    // the logs GET is the only method used by the logs client
    expect(appSource).toContain('requestWorkerLogsJson(fetchImplementation, path, { signal }, runId)');
  });

  test("no log bytes ever leave the viewer: no payload in diagnostics or storage", () => {
    expect(appSource).not.toContain("localStorage");
    expect(appSource).not.toContain("sessionStorage");
    expect(appSource).not.toContain("history.pushState");
    // the failure diagnostics carry only layer/reason pairs
    expect(appSource).toContain('error: Object.freeze({ layer: result.failure.layer, reason: result.failure.reason })');
  });

  test("the worker-log artifact envelope never enters the browser layer", () => {
    expect(appSource).not.toContain("payload_base64");
    expect(appSource).not.toContain("workerlog.json");
    expect(appSource).not.toContain(".workerlog");
  });
});
