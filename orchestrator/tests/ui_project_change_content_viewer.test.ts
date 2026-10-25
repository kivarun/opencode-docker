/**
 * Permanent suite of the browser UI v7 Project Change Content viewer:
 * the closed state machine over the single-flight content transport.
 * Zero content GETs exist before an explicit Review; one Review issues
 * up to two sequential GETs (baseline, then result) bound to the
 * current run, observation, change index and side; every response is
 * verified against the selected change entry's metadata and fails
 * closed with zero rendered bytes on any mismatch; a stale observation
 * discards the bytes of both sides; invalidations and selection
 * changes drop late responses.
 */
import { describe, expect, test } from "bun:test";
import {
  CONTENT_PREVIEW_CHUNK_LIMIT,
  CONTENT_PREVIEW_MAX_BYTES,
  UiContractError,
  createProjectChangeContentViewer,
  type UiProjectChangeContentState,
} from "../ui/app.js";

const OBS = "a".repeat(64);
const OBS2 = "b".repeat(64);
const SHA = "c".repeat(64);
const SHA2 = "d".repeat(64);

function fileNode(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { kind: "regular_file", mode: 0o600, byte_count: 5, sha256: SHA, multiple_links: false, ...overrides };
}

interface EntrySpec {
  readonly index: number;
  readonly path: string;
  readonly change: string;
  readonly baseline: Record<string, unknown> | null;
  readonly result: Record<string, unknown> | null;
}

function entry(spec: EntrySpec) {
  return { source_relation: "matches_baseline", ...spec };
}

function availableModel(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_change_content",
      runId: "run-a",
      revision: 9,
      observation: OBS,
      changeIndex: 1,
      side: "baseline",
      status: "available",
      byteCount: 5,
      sha256: SHA,
      offset: 0,
      data: new TextEncoder().encode("hello"),
      nextOffset: null,
      ...overrides,
    },
  };
}

function implicitEmptyModel(side: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_change_content",
      runId: "run-a",
      revision: 9,
      observation: OBS,
      changeIndex: 1,
      side,
      status: "implicit_empty",
      byteCount: 0,
      offset: 0,
      data: new Uint8Array(0),
      nextOffset: null,
      ...overrides,
    },
  };
}

function metadataOnlyModel(side: string, node: Record<string, unknown>): Record<string, unknown> {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_change_content",
      runId: "run-a",
      revision: 9,
      observation: OBS,
      changeIndex: 1,
      side,
      status: "metadata_only",
      node,
    },
  };
}

function unavailableModel(node: Record<string, unknown>): Record<string, unknown> {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_change_content",
      runId: "run-a",
      revision: 9,
      observation: OBS,
      changeIndex: 1,
      side: "baseline",
      status: "unavailable",
      reason: "source_no_longer_matches_baseline",
      node,
    },
  };
}

function endpointError(reason: string, layer = "request"): Record<string, unknown> {
  return { ok: true, lost: false, model: { kind: "error", layer, reason } };
}

/** The scripted transport: desires are recorded and settled by the
 * test; a declined desire resolves undefined without onApplied. */
interface FakeDesire {
  readonly path: string;
  readonly side: string;
  readonly kind: string;
  readonly onApplied: (result: unknown) => void;
}

function makeFakeTransport() {
  const pending: FakeDesire[] = [];
  const resolvers: Array<(result: unknown) => void> = [];
  let invalidated = 0;
  const transport = {
    request(desire: FakeDesire) {
      pending.push(desire);
      return new Promise((resolve) => {
        resolvers.push(resolve);
      });
    },
    settle(result: unknown) {
      const desire = pending.shift();
      const resolve = resolvers.shift();
      desire?.onApplied(result);
      resolve?.(result);
    },
    decline() {
      pending.shift();
      resolvers.shift()?.(undefined);
    },
    invalidate() {
      invalidated += 1;
      while (pending.length > 0) {
        pending.shift();
        resolvers.shift()?.(undefined);
      }
    },
    dispose() {
      this.invalidate();
    },
    invalidated: () => invalidated,
    pending: () => pending.slice(),
  };
  return transport;
}

function makeViewer() {
  const transport = makeFakeTransport();
  const states: Array<UiProjectChangeContentState | null> = [];
  const viewer = createProjectChangeContentViewer({
    transport: transport as unknown as Parameters<typeof createProjectChangeContentViewer>[0]["transport"],
    onState: (state) => {
      states.push(state);
    },
  });
  viewer.transitionToRun("run-a");
  return { viewer, transport, states, current: () => states[states.length - 1]! };
}

function bootViewer() {
  const harness = makeViewer();
  harness.viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() })] });
  return harness;
}

describe("v7 viewer: no GET before the explicit Review", () => {
  test("transition and metadata application issue zero content GETs", () => {
    const { transport, current } = makeViewer();
    expect(transport.pending().length).toBe(0);
    expect(current().requestState).toBe("idle");
    expect(current().observation).toBeNull();
    bootViewer();
    const booted = bootViewer();
    expect(booted.transport.pending().length).toBe(0);
    expect(booted.current().observation).toBe(OBS);
    void current;
  });

  test("a Review without a loaded observation is refused and fetches nothing", () => {
    const { viewer, transport } = makeViewer();
    expect(() => viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }))).toThrow(UiContractError);
    expect(transport.pending().length).toBe(0);
  });

  test("one Review issues the exact sequential baseline then result GETs", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    const desires = transport.pending();
    expect(desires.length).toBe(1);
    expect(desires[0]!.side).toBe("baseline");
    expect(desires[0]!.path).toBe(`/v1/runs/run-a/changes/1/content?side=baseline&observation=${OBS}`);
    expect(desires[0]!.kind).toBe("initial");
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(2);
    // the result side follows sequentially after the baseline settles
    const second = transport.pending();
    expect(second.length).toBe(1);
    expect(second[0]!.side).toBe("result");
    expect(second[0]!.path).toBe(`/v1/runs/run-a/changes/1/content?side=result&observation=${OBS}`);
    transport.settle(availableModel({ side: "result", byteCount: 5, data: new TextEncoder().encode("hello") }));
    await Bun.sleep(2);
    expect(current().requestState).toBe("ready");
    expect(current().baseline.phase).toBe("implicit_empty");
    expect(current().result.phase).toBe("available");
    expect(current().result.loadedBytes).toBe(5);
  });
});

describe("v7 viewer: the response binding and status matrix", () => {
  test("added: baseline implicit_empty and result available; the entry nodes must agree", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result" }));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("implicit_empty");
    expect(current().baseline.byteCount).toBe(0);
    // a fabricated digest on an implicit empty side is a contract error
    const bad = makeViewer();
    bad.viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() })] });
    bad.viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    bad.transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    expect(bad.current().baseline.phase).toBe("error");
    expect(bad.current().baseline.error).toEqual({ layer: "contract", reason: "invalid_shape" });
    expect(bad.current().baseline.data).toBeNull();
  });

  test("deleted: result implicit_empty and the baseline is served from the operator project", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "old.txt", change: "deleted", baseline: fileNode(), result: null })] });
    viewer.review(entry({ index: 1, path: "old.txt", change: "deleted", baseline: fileNode(), result: null }));
    transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    transport.settle(implicitEmptyModel("result"));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("available");
    expect(current().result.phase).toBe("implicit_empty");
  });

  test("unavailable baseline carries the closed reason and no bytes", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "old.txt", change: "deleted", baseline: fileNode(), result: null })] });
    viewer.review(entry({ index: 1, path: "old.txt", change: "deleted", baseline: fileNode(), result: null }));
    transport.settle(unavailableModel(fileNode()));
    await Bun.sleep(1);
    transport.settle(implicitEmptyModel("result"));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("unavailable");
    expect(current().baseline.data).toBeNull();
  });

  test("metadata-only sides for directories and symlinks", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "l", change: "symlink_target_modified", baseline: { kind: "symlink", target: "a" }, result: { kind: "symlink", target: "b" } })] });
    viewer.review(entry({ index: 1, path: "l", change: "symlink_target_modified", baseline: { kind: "symlink", target: "a" }, result: { kind: "symlink", target: "b" } }));
    transport.settle(metadataOnlyModel("baseline", { kind: "symlink", target: "a" }));
    await Bun.sleep(1);
    transport.settle(metadataOnlyModel("result", { kind: "symlink", target: "b" }));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("metadata_only");
    expect(current().result.phase).toBe("metadata_only");
    // a regular-file node served as metadata_only is a contract error
    const bad = bootViewer();
    bad.viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "l", change: "symlink_target_modified", baseline: { kind: "symlink", target: "a" }, result: { kind: "symlink", target: "b" } })] });
    bad.viewer.review(entry({ index: 1, path: "l", change: "symlink_target_modified", baseline: { kind: "symlink", target: "a" }, result: { kind: "symlink", target: "b" } }));
    bad.transport.settle(metadataOnlyModel("baseline", fileNode()));
    await Bun.sleep(1);
    expect(bad.current().baseline.phase).toBe("error");
  });

  test("mode-only identical content: both sides carry the proven equal digest", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.metadataApplied({ runId: "run-a", observation: OBS, changes: [entry({ index: 1, path: "f", change: "mode_modified", baseline: fileNode(), result: fileNode({ mode: 0o700 }) })] });
    viewer.review(entry({ index: 1, path: "f", change: "mode_modified", baseline: fileNode(), result: fileNode({ mode: 0o700 }) }));
    transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result" }));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("available");
    expect(current().result.sha256).toBe(SHA);
  });

  test("a response for a foreign run, observation, index or side fails closed with zero bytes", async () => {
    for (const overrides of [
      { runId: "other" },
      { observation: OBS2 },
      { changeIndex: 2 },
      { side: "result" },
    ]) {
      const { viewer, transport, current } = bootViewer();
      viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
      transport.settle(availableModel({ side: "baseline", ...overrides }));
      await Bun.sleep(1);
      expect(current().baseline.phase).toBe("error");
      expect(current().baseline.data).toBeNull();
    }
  });

  test("a digest/byte-count mismatch against the entry node fails closed", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result", byteCount: 6, sha256: SHA2 }));
    await Bun.sleep(1);
    expect(current().result.phase).toBe("error");
    expect(current().result.data).toBeNull();
  });
});

describe("v7 viewer: the bounded raw-byte cursor", () => {
  function chunkModel(side: string, offset: number, length: number, byteCount: number): Record<string, unknown> {
    return availableModel({
      side,
      byteCount,
      offset,
      data: new Uint8Array(length).map((_, i) => (offset + i) % 256),
      nextOffset: offset + length < byteCount ? offset + length : null,
    });
  }

  test("load more continues at the raw-byte cursor and accumulates the prefix", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "big.bin", change: "added", baseline: null, result: fileNode({ byte_count: CONTENT_PREVIEW_MAX_BYTES + 1, sha256: SHA }) }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(chunkModel("result", 0, CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_MAX_BYTES + 1));
    await Bun.sleep(1);
    expect(current().result.loadedBytes).toBe(CONTENT_PREVIEW_CHUNK_LIMIT);
    expect(current().result.nextOffset).toBe(CONTENT_PREVIEW_CHUNK_LIMIT);
    expect(current().result.capped).toBe(false);
    viewer.loadMoreSide("result");
    const desire = transport.pending()[0]!;
    expect(desire.path).toBe(`/v1/runs/run-a/changes/1/content?side=result&observation=${OBS}&offset=${CONTENT_PREVIEW_CHUNK_LIMIT}`);
    transport.settle(chunkModel("result", CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_MAX_BYTES + 1));
    await Bun.sleep(1);
    expect(current().result.loadedBytes).toBe(2 * CONTENT_PREVIEW_CHUNK_LIMIT);
    expect(Array.from(current().result.data!).slice(CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_CHUNK_LIMIT + 3)).toEqual([0, 1, 2]);
  });

  test("the preview cap stops further loads even though the file is longer", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "big.bin", change: "added", baseline: null, result: fileNode({ byte_count: CONTENT_PREVIEW_MAX_BYTES * 3, sha256: SHA }) }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(chunkModel("result", 0, CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_MAX_BYTES * 3));
    await Bun.sleep(1);
    for (let index = 1; index < 4; index += 1) {
      viewer.loadMoreSide("result");
      expect(transport.pending().length).toBe(1);
      transport.settle(chunkModel("result", index * CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_MAX_BYTES * 3));
      await Bun.sleep(1);
    }
    expect(current().result.loadedBytes).toBe(CONTENT_PREVIEW_MAX_BYTES);
    expect(current().result.capped).toBe(true);
    viewer.loadMoreSide("result");
    expect(transport.pending().length).toBe(0);
  });

  test("a declined continuation does not strand the side in loading", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "big.bin", change: "added", baseline: null, result: fileNode({ byte_count: CONTENT_PREVIEW_MAX_BYTES + 1, sha256: SHA }) }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(chunkModel("result", 0, CONTENT_PREVIEW_CHUNK_LIMIT, CONTENT_PREVIEW_MAX_BYTES + 1));
    await Bun.sleep(1);
    viewer.loadMoreSide("result");
    transport.decline();
    await Bun.sleep(2);
    expect(current().result.phase).toBe("available");
    expect(current().requestState).toBe("ready");
  });

  test("retry is only offered to a transport-failed side and re-requests the same offset", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(endpointError("network_error", "transport"));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result" }));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("error");
    expect(current().baseline.error).toEqual({ layer: "transport", reason: "network_error" });
    viewer.retrySide("baseline");
    const desire = transport.pending()[0]!;
    expect(desire.side).toBe("baseline");
    expect(desire.path).toBe(`/v1/runs/run-a/changes/1/content?side=baseline&observation=${OBS}`);
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    expect(current().baseline.phase).toBe("implicit_empty");
    // a contract failure is not recoverable by retry
    const bad = bootViewer();
    bad.viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    bad.transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    bad.viewer.retrySide("baseline");
    expect(bad.transport.pending().length).toBe(1); // only the pending result-side request remains
  });
});

describe("v7 viewer: staleness and invalidation", () => {
  test("a stale response discards the bytes of both sides and offers only the reload", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode({ byte_count: 10 }) }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result", byteCount: 10, nextOffset: 5 }));
    await Bun.sleep(1);
    expect(current().result.data).not.toBeNull();
    // a stale response arrives on a later chunk continuation
    viewer.loadMoreSide("result");
    transport.settle(endpointError("stale_observation"));
    await Bun.sleep(1);
    expect(current().requestState).toBe("stale");
    expect(current().stale).toBe(true);
    expect(current().baseline.data).toBeNull();
    expect(current().result.data).toBeNull();
    expect(current().baseline.phase).toBe("unrequested");
    expect(current().result.phase).toBe("unrequested");
    expect(transport.invalidated()).toBeGreaterThan(0);
  });

  test("an observation change clears the loaded bytes without re-fetching", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(implicitEmptyModel("baseline"));
    await Bun.sleep(1);
    transport.settle(availableModel({ side: "result" }));
    await Bun.sleep(1);
    expect(current().result.data).not.toBeNull();
    viewer.metadataApplied({ runId: "run-a", observation: OBS2, changes: [] });
    await Bun.sleep(1);
    expect(current().observation).toBe(OBS2);
    expect(current().result.data).toBeNull();
    expect(current().changeIndex).toBeNull();
    expect(transport.pending().length).toBe(0);
  });

  test("a new Review drops the late response of the previous selection", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.metadataApplied({
      runId: "run-a",
      observation: OBS,
      changes: [
        entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }),
        entry({ index: 2, path: "b.txt", change: "added", baseline: null, result: fileNode({ sha256: SHA2 }) }),
      ],
    });
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    // the chained result-side desire of the first selection is the one
    // a new Review supersedes; its late response never applies
    const superseded = transport.pending()[0]!;
    expect(superseded.side).toBe("result");
    viewer.review(entry({ index: 2, path: "b.txt", change: "added", baseline: null, result: fileNode({ sha256: SHA2 }) }));
    superseded.onApplied(availableModel({ side: "result", changeIndex: 1 }));
    await Bun.sleep(1);
    expect(current().changeIndex).toBe(2);
    expect(current().result.phase).toBe("unrequested");
    const pending = transport.pending();
    expect(pending.length).toBe(1);
    expect(pending[0]!.path).toBe(`/v1/runs/run-a/changes/2/content?side=baseline&observation=${OBS}`);
  });

  test("closeReview discards the selection and the bytes; the transport is withdrawn", async () => {
    const { viewer, transport, current } = bootViewer();
    viewer.review(entry({ index: 1, path: "a.txt", change: "added", baseline: null, result: fileNode() }));
    transport.settle(availableModel({ side: "baseline" }));
    await Bun.sleep(1);
    viewer.closeReview();
    await Bun.sleep(1);
    expect(current().changeIndex).toBeNull();
    expect(current().baseline.data).toBeNull();
    transport.settle(availableModel({ side: "result", changeIndex: 1 }));
    await Bun.sleep(1);
    expect(current().result.phase).toBe("unrequested");
  });

  test("a run selection change resets everything", () => {
    const { viewer, transport, current } = bootViewer();
    viewer.transitionToRun("run-b");
    expect(current().runId).toBe("run-b");
    expect(current().observation).toBeNull();
    expect(current().baseline.data).toBeNull();
    expect(transport.pending().length).toBe(0);
  });
});
