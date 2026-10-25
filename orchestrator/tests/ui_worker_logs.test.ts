/**
 * Permanent contract suite of the browser UI v5 worker-log viewer
 * (corrective): the one logs document parsers (`worker_logs` metadata
 * union and `worker_log` status/chunk shapes with the run binding and
 * the raw-byte cursor coherence), the one URL builder, the bounded
 * single-flight logs transport with kind-aware arbitration (a metadata
 * desire never cancels an in-flight manual content request), the closed
 * viewer state machine (inspect-driven automatic metadata, terminal log
 * access on every boundary, the immutable binding check, stale-cursor
 * drops, one-click retry), the one incremental UTF-8 decoder owner and
 * the safe render contract — plus the source restrictions of the logs
 * layer.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  UiContractError,
  createLogChunkDecoder,
  createLogsTransport,
  createWorkerLogViewer,
  createWorkerLogsState,
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

describe("the bounded single-flight logs transport with kind arbitration", () => {
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

  const metadataDesire = (onApplied: (r: unknown) => void) => ({ runId: "run-a", path: "/v1/runs/run-a/logs", kind: "metadata" as const, onApplied: onApplied as never });
  const contentDesire = (path: string, onApplied: (r: unknown) => void) => ({ runId: "run-a", path, kind: "content" as const, onApplied: onApplied as never });

  test("a metadata desire never cancels an in-flight manual content request", async () => {
    const { calls, gates, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const chunk = transport.request(contentDesire("/v1/runs/run-a/logs/1?offset=0&limit=65536", (r) => applied.push(`chunk:${String(r !== undefined)}`)));
    const meta = transport.request(metadataDesire((r) => applied.push(`meta:${String(r !== undefined)}`)));
    expect(calls).toHaveLength(1); // the chunk request is on the wire
    expect(calls[0]!.signal.aborted).toBe(false); // the metadata did not cancel it
    const listModel = { kind: "worker_logs", runId: "run-a", revision: 25, logs: [] };
    gates[0]!({ ok: true, model: chunkModelFor("/v1/runs/run-a/logs/1?offset=0&limit=65536") });
    await chunk;
    expect(applied).toEqual(["chunk:true"]); // the chunk applied first
    expect(calls).toHaveLength(2); // then exactly one coalesced metadata follow-up
    gates[1]!({ ok: true, model: listModel });
    await meta;
    expect(applied).toEqual(["chunk:true", "meta:true"]);
  });

  test("a content desire supersedes an active metadata refresh", async () => {
    const { calls, gates, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const meta = transport.request(metadataDesire(() => applied.push("meta")));
    const chunk = transport.request(contentDesire("/p1", () => applied.push("chunk")));
    expect(calls[0]!.signal.aborted).toBe(true); // the metadata was superseded
    gates[0]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 1, logs: [] } });
    await meta;
    expect(applied).toEqual([]); // the superseded metadata never applies
    gates[1]!({ ok: true, model: chunkModelFor("/p1") });
    await chunk;
    expect(applied).toEqual(["chunk"]);
    expect(calls).toHaveLength(2);
  });

  test("repeated metadata desires coalesce to the latest", async () => {
    const { calls, gates, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const first = transport.request(metadataDesire(() => applied.push("meta1")));
    const second = transport.request(metadataDesire(() => applied.push("meta2")));
    const third = transport.request(metadataDesire(() => applied.push("meta3")));
    expect(calls).toHaveLength(1);
    gates[0]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 1, logs: [] } });
    await first;
    await second;
    expect(applied).toEqual([]); // both superseded desires settle without applying
    expect(calls).toHaveLength(2); // only the coalesced latest runs
    gates[1]!({ ok: true, model: { kind: "worker_logs", runId: "run-a", revision: 2, logs: [] } });
    await third;
    expect(applied).toEqual(["meta3"]);
  });

  test("a content desire supersedes an active content request", async () => {
    const { calls, gates, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const first = transport.request(contentDesire("/p1", () => applied.push("first")));
    const second = transport.request(contentDesire("/p2", () => applied.push("second")));
    expect(calls[0]!.signal.aborted).toBe(true);
    gates[0]!({ ok: true, model: chunkModelFor("/p1") });
    await first;
    expect(applied).toEqual([]);
    gates[1]!({ ok: true, model: chunkModelFor("/p2") });
    await second;
    expect(applied).toEqual(["second"]);
  });

  test("invalidate withdraws the active and queued desires without effects", async () => {
    const { calls, gates, io } = makeIo();
    const transport = createLogsTransport(io);
    const applied: string[] = [];
    const first = transport.request(contentDesire("/p", () => applied.push("first")));
    transport.request(metadataDesire(() => applied.push("meta")));
    transport.invalidate();
    gates[0]!({ ok: true, model: chunkModelFor("/p") });
    await first;
    expect(applied).toEqual([]);
    expect(calls).toHaveLength(1); // the queued desire was withdrawn
  });

  test("dispose refuses every later desire", async () => {
    const { calls, io } = makeIo();
    const transport = createLogsTransport(io);
    transport.dispose();
    const result = await transport.request(metadataDesire(() => undefined));
    expect(result).toBeUndefined();
    expect(calls).toHaveLength(0);
  });

  test("invalid io shapes and desire kinds are refused before any state", () => {
    expect(contractReason(() => createLogsTransport({} as never))).toBe("invalid_transport_io");
    const { io, calls } = makeIo();
    const transport = createLogsTransport(io);
    void transport.request({ runId: "run-a", path: "/p", kind: "other" as never, onApplied: () => undefined });
    expect(calls).toHaveLength(0);
  });

  function chunkModelFor(path: string): Record<string, unknown> {
    const offset = Number(new URL(path, "http://x").searchParams.get("offset") ?? 0);
    return {
      kind: "worker_log",
      runId: "run-a",
      revision: 24,
      executionIndex: 1,
      stateId: "architect",
      status: "captured",
      sha256: "a".repeat(64),
      byteCount: 11,
      truncatedDaemon: false,
      truncatedLocal: false,
      offset,
      dataBase64: b64("hello world"),
      nextOffset: null,
    };
  }
});

describe("the closed viewer state machine", () => {
  type Harness = {
    viewer: ReturnType<typeof createWorkerLogViewer>;
    states: (UiWorkerLogsState | null)[];
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
    const viewer = createWorkerLogViewer({
      transport,
      onState: (state) => states.push(state),
    });
    viewer.transitionToLogsView(runId);
    return { viewer, states, gates, transport };
  };

  const okList = (logs: readonly Record<string, unknown>[], revision = 24): unknown => ({
    ok: true,
    model: { kind: "worker_logs", runId: "run-a", revision, logs },
  });

  const capturedEntry = (index = 1, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
    executionIndex: index,
    stateId: "architect",
    status: "captured",
    sha256: "a".repeat(64),
    byteCount: 11,
    truncatedDaemon: false,
    truncatedLocal: false,
    ...overrides,
  });

  const chunkModel = (offset = 0, overrides: Record<string, unknown> = {}): unknown => ({
    ok: true,
    model: {
      kind: "worker_log",
      runId: "run-a",
      revision: 24,
      executionIndex: 1,
      stateId: "architect",
      status: "captured",
      sha256: "a".repeat(64),
      byteCount: 11,
      truncatedDaemon: false,
      truncatedLocal: false,
      offset,
      dataBase64: b64("hello world"),
      nextOffset: null,
      ...overrides,
    },
  });

  test("the transition creates the empty state and publishes it", () => {
    const { viewer, states } = makeViewer();
    expect(states).toHaveLength(1);
    expect(states[0]).toEqual(createWorkerLogsState("run-a"));
    expect(viewer.currentState()).toEqual(createWorkerLogsState("run-a"));
  });

  test("the list loads on the explicit press and binds run/revision", async () => {
    const { viewer, states, gates } = makeViewer();
    viewer.fetchLogsList();
    expect(states.at(-1)!.requestState).toBe("loading");
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    const state = viewer.currentState()!;
    expect(state.requestState).toBe("ready");
    expect(state.snapshotRevision).toBe(24);
    expect(state.logs).toHaveLength(1);
    expect(states.at(-1)!.requestState).toBe("ready");
  });

  test("a list response of another run is dropped before it becomes state", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!({
      ok: true,
      model: { kind: "worker_logs", runId: "run-b", revision: 5, logs: [{ executionIndex: 9, stateId: "x", status: "pending" }] },
    });
    await Bun.sleep(2);
    const state = viewer.currentState()!;
    expect(state.logs).toHaveLength(0); // the stale response never applied
    expect(state.requestState).toBe("failed"); // settled as a typed contract failure, never stuck
    expect(state.error).toEqual({ layer: "contract", reason: "unexpected_kind" });
  });

  test("the first applied authoritative inspect issues exactly one automatic metadata GET", async () => {
    const { viewer, gates, states } = makeViewer();
    viewer.inspectApplied({ runId: "run-a", revision: 24 });
    expect(gates).toHaveLength(1);
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    expect(viewer.currentState()!.snapshotRevision).toBe(24);
    // the same revision never issues a second GET
    viewer.inspectApplied({ runId: "run-a", revision: 24 });
    expect(gates).toHaveLength(1);
    // a lower revision never issues a GET
    viewer.inspectApplied({ runId: "run-a", revision: 10 });
    expect(gates).toHaveLength(1);
    // a foreign run never issues a GET
    viewer.inspectApplied({ runId: "run-b", revision: 99 });
    expect(gates).toHaveLength(1);
    // a higher revision issues exactly one more GET
    viewer.inspectApplied({ runId: "run-a", revision: 30 });
    expect(gates).toHaveLength(2);
    gates[1]!(okList([capturedEntry()], 30));
    await Bun.sleep(2);
    expect(viewer.currentState()!.snapshotRevision).toBe(30);
    expect(states.at(-1)!.requestState).toBe("ready");
  });

  test("pending metadata becomes captured automatically after a revision advance", async () => {
    const { viewer, gates } = makeViewer();
    viewer.inspectApplied({ runId: "run-a", revision: 10 });
    gates[0]!(okList([{ executionIndex: 1, stateId: "architect", status: "pending" }], 10));
    await Bun.sleep(2);
    expect(viewer.currentState()!.logs[0]!.status).toBe("pending");
    viewer.inspectApplied({ runId: "run-a", revision: 24 });
    gates[1]!(okList([capturedEntry()], 24));
    await Bun.sleep(2);
    expect(viewer.currentState()!.logs[0]!.status).toBe("captured");
    expect(viewer.currentState()!.snapshotRevision).toBe(24);
  });

  test("a failed automatic metadata application does not re-trigger for the same revision", async () => {
    const { viewer, gates } = makeViewer();
    viewer.inspectApplied({ runId: "run-a", revision: 24 });
    gates[0]!({ ok: false, failure: { layer: "transport", reason: "network_error" } });
    await Bun.sleep(2);
    expect(viewer.currentState()!.requestState).toBe("failed");
    // the same revision again: zero further GETs
    viewer.inspectApplied({ runId: "run-a", revision: 24 });
    expect(gates).toHaveLength(1);
    // a higher revision recovers the automatic path
    viewer.inspectApplied({ runId: "run-a", revision: 30 });
    expect(gates).toHaveLength(2);
    gates[1]!(okList([capturedEntry()], 30));
    await Bun.sleep(2);
    expect(viewer.currentState()!.requestState).toBe("ready");
  });

  test("an unchanged immutable binding preserves the loaded content, selection and text", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel(0, { nextOffset: 11 }));
    await Bun.sleep(2);
    const before = viewer.currentState()!.content;
    expect(before!.text).toBe("hello world");
    // a metadata refresh at a higher revision with the identical binding
    viewer.inspectApplied({ runId: "run-a", revision: 30 });
    gates[2]!(okList([capturedEntry()], 30));
    await Bun.sleep(2);
    const after = viewer.currentState()!.content;
    expect(after).toEqual(before); // the loaded content survived the refresh
    expect(viewer.currentState()!.selectedExecutionIndex).toBe(1);
    // and the next chunk continues the same cursor
    viewer.fetchChunk();
    expect(gates).toHaveLength(4); // list, chunk, refresh, next chunk
  });

  test("a changed immutable binding fails closed as invalid_log_binding", async () => {
    const { viewer, gates, states } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel());
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).not.toBeNull();
    // the refresh observes a different artifact digest for the same execution
    viewer.inspectApplied({ runId: "run-a", revision: 30 });
    gates[2]!(okList([capturedEntry(1, { sha256: "b".repeat(64) })], 30));
    await Bun.sleep(2);
    const state = viewer.currentState()!;
    expect(state.requestState).toBe("failed");
    expect(state.error).toEqual({ layer: "viewer", reason: "invalid_log_binding" });
    expect(states.filter((s) => s!.error?.reason === "invalid_log_binding")).toHaveLength(1);
    // the loaded content is preserved, never silently mixed
    expect(state.content).not.toBeNull();
  });

  test("terminal log access on every boundary: the viewer carries no boundary classifier", async () => {
    // the viewer io accepts no snapshot and no mutating predicate at all:
    // chunk access is independent of the run boundary, the mutation
    // action model and the mutating POST state
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel());
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).not.toBeNull();
    expect(viewer.currentState()!.requestState).toBe("ready");
    expect(viewer.currentState()!.error).toBeNull();
  });

  test("a manual chunk survives an automatic metadata refresh and one follow-up runs", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk(); // the manual content request goes on the wire
    viewer.inspectApplied({ runId: "run-a", revision: 25 }); // the automatic metadata arrives
    gates[1]!(chunkModel());
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).not.toBeNull();
    expect(gates).toHaveLength(3); // list, chunk, one coalesced metadata follow-up
    gates[2]!(okList([capturedEntry()], 25));
    await Bun.sleep(2);
    expect(viewer.currentState()!.snapshotRevision).toBe(25);
    expect(viewer.currentState()!.content!.text).toBe("hello world");
  });

  test("the selection clears content and resets the decoder; unknown executions are refused", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry(), capturedEntry(2, { stateId: "execute" })]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel(0, { nextOffset: 7, dataBase64: b64("hello w") }));
    await Bun.sleep(2);
    expect(viewer.currentState()!.content!.text).toBe("hello w");
    viewer.selectExecution(2);
    expect(viewer.currentState()!.content).toBeNull(); // fresh content
    viewer.selectExecution(9);
    expect(viewer.currentState()!.selectedExecutionIndex).toBe(2);
    // back to execution 1: the decoder is fresh, the reload starts at 0
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[2]!(chunkModel(0, { nextOffset: 7, dataBase64: b64("hello w") }));
    await Bun.sleep(2);
    expect(viewer.currentState()!.content!.text).toBe("hello w"); // no duplicated prefix
  });

  test("a chunk response of another execution or run is dropped", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: true, model: { ...chunkDocument(), execution_index: 2 } });
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).toBeNull();
    viewer.fetchChunk();
    gates[2]!({ ok: true, model: parseWorkerLogDocument({ ...chunkDocument(), run_id: "run-b" }, "run-b") });
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).toBeNull();
  });

  test("a stale or duplicated chunk offset settles as a typed contract failure without decoding", async () => {
    const { viewer, gates, states } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel(0, { nextOffset: 7, dataBase64: b64("hello w") }));
    await Bun.sleep(2);
    expect(viewer.currentState()!.content!.text).toBe("hello w");
    // a retried offset 0 while the cursor is at 8: never decoded, never duplicated
    viewer.fetchChunk(); // refused: the artifact is not complete but the state is loading? no: ready → issues offset 8
    gates[2]!(chunkModel(0, { nextOffset: 7 })); // a stale response for offset 0 arrives at the offset-8 request
    await Bun.sleep(2);
    expect(viewer.currentState()!.requestState).toBe("failed");
    expect(viewer.currentState()!.error).toEqual({ layer: "contract", reason: "unexpected_kind" });
    expect(viewer.currentState()!.content!.text).toBe("hello w"); // unchanged, no duplication
    expect(states.filter((s) => s!.error?.reason === "unexpected_kind")).toHaveLength(1);
  });

  test("the terminal chunk disables the next request", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!(chunkModel());
    await Bun.sleep(2);
    const requestsBefore = gates.length;
    viewer.fetchChunk(); // next_offset === null: a no-op
    expect(gates).toHaveLength(requestsBefore);
  });

  test("a failed chunk desire retries exactly once per click", async () => {
    const { viewer, gates, states } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry()]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: false, failure: { layer: "transport", reason: "network_error" } });
    await Bun.sleep(2);
    expect(viewer.currentState()!.requestState).toBe("failed");
    expect(viewer.currentState()!.error).toEqual({ layer: "transport", reason: "network_error" });
    viewer.retryLastChunkRequest();
    expect(viewer.currentState()!.requestState).toBe("loading");
    gates[2]!(chunkModel());
    await Bun.sleep(2);
    expect(viewer.currentState()!.content).not.toBeNull();
    expect(viewer.currentState()!.content!.text).toBe("hello world"); // no duplicated prefix from the failed attempt
    // a retry outside a failed state is a no-op
    viewer.retryLastChunkRequest();
    expect(viewer.currentState()!.content!.text).toBe("hello world");
    expect(states.filter((s) => s!.requestState === "ready").length).toBeGreaterThanOrEqual(2);
  });

  test("a retry of a foreign selection is refused", async () => {
    const { viewer, gates } = makeViewer();
    viewer.fetchLogsList();
    gates[0]!(okList([capturedEntry(), capturedEntry(2, { stateId: "execute" })]));
    await Bun.sleep(2);
    viewer.selectExecution(1);
    viewer.fetchChunk();
    gates[1]!({ ok: false, failure: { layer: "transport", reason: "network_error" } });
    await Bun.sleep(2);
    viewer.selectExecution(2);
    const requestsBefore = gates.length;
    viewer.retryLastChunkRequest();
    expect(gates).toHaveLength(requestsBefore); // the failed desire belongs to execution 1
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
    await Bun.sleep(2);
    expect(viewer.currentState()!.logs).toHaveLength(0);
    expect(viewer.currentState()!.requestState).toBe("idle"); // the new run's state owns the UI
    // and the automatic path restarts from zero for the new run
    viewer.inspectApplied({ runId: "run-b", revision: 5 });
    expect(gates).toHaveLength(2);
    void transport;
  });

  test("the viewer refuses invalid io and unsafe run ids", () => {
    expect(contractReason(() => createWorkerLogViewer({} as never))).toBe("invalid_viewer_io");
    expect(contractReason(() => createWorkerLogViewer({ transport: 1, onState: () => undefined } as never))).toBe("invalid_viewer_io");
    const { viewer } = makeViewer();
    expect(contractReason(() => viewer.transitionToLogsView("../escape"))).toBe("invalid_run_id");
  });
});

describe("the incremental UTF-8 decoder", () => {
  test("multi-byte sequences split across chunks decode correctly at every valid position", () => {
    // 2-byte: é = C3 A9; 3-byte: € = E2 82 AC; 4-byte: 🙂 = F0 9F 99 82
    const cases: { readonly name: string; readonly bytes: readonly number[]; readonly expected: string }[] = [
      { name: "2-byte é", bytes: [0x61, 0xc3, 0xa9, 0x62], expected: "aéb" },
      { name: "3-byte €", bytes: [0x61, 0xe2, 0x82, 0xac, 0x62], expected: "a€b" },
      { name: "4-byte 🙂", bytes: [0x61, 0xf0, 0x9f, 0x99, 0x82, 0x62], expected: "a🙂b" },
    ];
    for (const testCase of cases) {
      const bytes = testCase.bytes;
      // split after every byte position that is inside the multi-byte sequence
      for (let split = 1; split < bytes.length - 1; split += 1) {
        const decoder = createLogChunkDecoder();
        const head = Buffer.from(bytes.slice(0, split)).toString("base64");
        const tail = Buffer.from(bytes.slice(split)).toString("base64");
        const text = decoder.decodeChunk(head, false) + decoder.decodeChunk(tail, true);
        expect(text).toBe(testCase.expected);
      }
    }
  });

  test("invalid UTF-8 becomes the replacement character", () => {
    const decoder = createLogChunkDecoder();
    const text = decoder.decodeChunk(Buffer.from([0x61, 0xff, 0xfe, 0x62]).toString("base64"), true);
    expect(text).toBe("a\uFFFD\uFFFDb");
  });

  test("reset clears the pending multi-byte state", () => {
    const decoder = createLogChunkDecoder();
    decoder.decodeChunk(Buffer.from([0x61, 0xc3]).toString("base64"), false); // a pending 2-byte lead
    decoder.reset();
    const text = decoder.decodeChunk(Buffer.from([0xa9]).toString("base64"), true);
    expect(text).toBe("\uFFFD"); // the orphaned byte is invalid on its own
  });

  test("NUL, ANSI escapes and markup-looking text stay plain text", () => {
    const decoder = createLogChunkDecoder();
    const raw = "<script>alert(1)</script>\u0000\u001b[31m";
    const text = decoder.decodeChunk(Buffer.from(raw, "utf8").toString("base64"), true);
    expect(text).toBe(raw);
  });
});

describe("the safe render contract", () => {
  test("the rendered text is safe by construction: the source never builds markup from the payload", () => {
    expect(appSource).not.toContain("innerHTML");
    expect(appSource).not.toContain("insertAdjacentHTML");
    expect(appSource).not.toContain("document.write");
    // the content frame renders only the accumulated decoded text
    expect(appSource).toContain("pre.textContent = content.text");
  });
});

describe("the logs layer source restrictions", () => {
  test("the corrective viewer carries no capture gate and no boundary classifier", () => {
    expect(appSource).not.toContain("capture_gate_refused");
    expect(appSource).not.toContain("logsCaptureGate");
    expect(appSource).not.toContain("decodeLogChunkText");
    // the logs subsystem references no mutation/action classifier and no
    // allowlist of run boundaries
    const logsStart = appSource.indexOf("Worker logs (v5)");
    // the logs owner ends where the v7 content preview region begins
    const logsEnd = appSource.indexOf("Project change content (v7)");
    const region = appSource.slice(logsStart, logsEnd);
    for (const banned of ["buildRunActionModel", "RESERVED_WAIT_ACTIONS", "buildMutationRequest", "runMutationFlow", "createMutationGate"]) {
      expect(region.includes(banned)).toBe(false);
    }
    for (const boundary of ["in_flight", "plan_ready", "waiting", "resumable"]) {
      expect(region.includes(`"${boundary}"`)).toBe(false);
    }
  });

  test("one logs transport, one incremental decoder owner, one metadata trigger", () => {
    expect(appSource.split("export function createLogsTransport(").length - 1).toBe(1);
    expect(appSource.split("export function createLogChunkDecoder(").length - 1).toBe(1);
    // both TextDecoder sites of the logs layer live inside the one
    // decoder owner (the bounded response reader elsewhere in the module
    // owns its own unrelated decoder)
    const logsStart = appSource.indexOf("Worker logs (v5)");
    // the logs owner ends where the v7 content preview region begins
    const logsEnd = appSource.indexOf("Project change content (v7)");
    const logsRegion = appSource.slice(logsStart, logsEnd);
    expect(logsRegion.split("new TextDecoder(").length - 1).toBe(2);
    const decoderStart = logsRegion.indexOf("export function createLogChunkDecoder(");
    const decoderEnd = logsRegion.indexOf("export function createWorkerLogViewer(");
    expect(logsRegion.slice(0, decoderStart).includes("new TextDecoder(")).toBe(false);
    expect(logsRegion.slice(decoderEnd).includes("new TextDecoder(")).toBe(false);
    // the automatic metadata refresh is invoked only from the applied
    // authoritative inspect path (the v6 project-changes viewer adds its
    // own inspectApplied declaration and its own call site beside the
    // logs ones)
    expect(appSource.split("inspectApplied(").length - 1).toBe(4); // two declarations + two call sites
    // the content fetch is invoked only from the user action path
    expect(appSource.split("fetchChunk()").length - 1).toBe(2); // declaration + one click site
  });

  test("the logs section is manual-only for content: no auto-load of payload bytes", () => {
    expect(appSource).not.toContain("setInterval");
    expect(appSource).not.toContain("EventSource");
    expect(appSource).not.toContain("WebSocket");
    // the logs GET is the only method used by the logs client
    expect(appSource).toContain('requestWorkerLogsJson(fetchImplementation, path, { signal }, runId)');
    // the logs subsystem owns zero timers: no scheduling primitive of any
    // kind appears in the logs region (the v3 inspect scheduler lives
    // outside it and is the module's only timer owner)
    const logsStart = appSource.indexOf("Worker logs (v5)");
    // the logs owner ends where the v7 content preview region begins
    const logsEnd = appSource.indexOf("Project change content (v7)");
    const logsRegion = appSource.slice(logsStart, logsEnd);
    expect(logsRegion.includes("setTimeout")).toBe(false);
    expect(logsRegion.includes("setInterval")).toBe(false);
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
