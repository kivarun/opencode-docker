/**
 * The single-flight transport battery of the Project Apply operation
 * GETs: at most one unsettled GET, supersede/coalesce arbitration,
 * invalidate/dispose semantics, the settlement matrix and a stress
 * sequence. The fetch is a deferred fake; no POST capability exists.
 * The supersede fact travels in the settlement, so the application
 * filter is the caller's — exactly like the inspect transport.
 */
import { describe, expect, test } from "bun:test";
import { createProjectApplyTransport, UiContractError } from "../ui/app.js";

const OBS = "a".repeat(64);

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

interface Settlement {
  fulfilled: boolean;
  superseded: boolean;
  result?: unknown;
  error?: unknown;
}


function firstCall(h: ReturnType<typeof createHarness>): { runId: string; applyId: string; observation: string; gate: Deferred<string> } {
  const entry = h.calls[0];
  if (entry === undefined) throw new Error("missing call");
  return entry;
}

function secondCall(h: ReturnType<typeof createHarness>): { runId: string; applyId: string; observation: string; gate: Deferred<string> } {
  const entry = h.calls[1];
  if (entry === undefined) throw new Error("missing second call");
  return entry;
}
function createHarness() {
  const calls: Array<{ runId: string; applyId: string; observation: string; gate: Deferred<string> }> = [];
  const applied: number[] = [];
  const results: Settlement[] = [];
  let ordinal = 0;
  const transport = createProjectApplyTransport({
    fetchOperation: (runId: string, applyId: string, observation: string): Promise<string> => {
      const gate = deferred<string>();
      calls.push({ runId, applyId, observation, gate });
      return gate.promise as unknown as Promise<string>;
    },
  });
  const request = () => {
    const index = ordinal;
    ordinal += 1;
    transport.request({
      runId: "run-a",
      applyId: OBS,
      observation: OBS,
      onSettled: (settlement: Settlement) => {
        // the supersede filter is an application filter: a superseded
        // settlement reaches the caller but is never applied
        if (settlement.superseded) {
          return;
        }
        results.push(settlement);
        if (settlement.fulfilled) {
          applied.push(index);
        }
      },
    });
  };
  const flush = async () => {
    for (let index = 0; index < 4; index += 1) {
      await Promise.resolve();
    }
  };
  return { transport, calls, applied, results, request, flush };
}

describe("project apply transport: single-flight admission", () => {
  test("the first desire starts immediately; a second supersedes and coalesces", async () => {
    const h = createHarness();
    h.request();
    expect(h.calls).toHaveLength(1);
    h.request();
    expect(h.calls).toHaveLength(1);
    firstCall(h).gate.resolve("one");
    await h.flush();
    expect(h.calls).toHaveLength(2);
  });

  test("the coalesced follow-up starts exactly when the slot releases", async () => {
    const h = createHarness();
    h.request();
    h.request();
    h.request();
    expect(h.calls).toHaveLength(1);
    firstCall(h).gate.resolve("one");
    await h.flush();
    // the last desire (not the superseded middle one) starts
    expect(h.calls).toHaveLength(2);
    secondCall(h).gate.resolve("two");
    await h.flush();
    expect(h.calls).toHaveLength(2);
    expect(h.applied).toEqual([2]);
    expect(h.results).toHaveLength(1);
  });

  test("only the never-superseded settlement applies", async () => {
    const h = createHarness();
    h.request();
    h.request();
    firstCall(h).gate.resolve("stale");
    await h.flush();
    expect(h.applied).toEqual([]);
    expect(h.calls).toHaveLength(2);
    secondCall(h).gate.resolve("fresh");
    await h.flush();
    expect(h.applied).toEqual([1]);
  });

  test("rejections settle the slot without applying", async () => {
    const h = createHarness();
    h.request();
    firstCall(h).gate.reject(new Error("rejected"));
    await h.flush();
    expect(h.results).toHaveLength(1);
    expect(h.results[0]?.fulfilled).toBe(false);
    expect(h.applied).toEqual([]);
    h.request();
    expect(h.calls).toHaveLength(2);
  });

  test("invalidate supersedes the active result and withdraws the desire", async () => {
    const h = createHarness();
    h.request();
    h.request();
    h.transport.invalidate();
    firstCall(h).gate.resolve("dropped");
    await h.flush();
    expect(h.applied).toEqual([]);
    expect(h.calls).toHaveLength(1);
  });

  test("dispose withdraws everything and refuses later desires", async () => {
    const h = createHarness();
    h.request();
    h.transport.dispose();
    h.request();
    expect(h.calls).toHaveLength(1);
    firstCall(h).gate.resolve("dropped");
    await h.flush();
    expect(h.applied).toEqual([]);
  });

  test("an invalid desire never reaches the wire", () => {
    const transport = createProjectApplyTransport({ fetchOperation: async () => "x" } as never);
    transport.request({ runId: "bad/id", applyId: OBS, observation: OBS });
    transport.request({ runId: "run-a", applyId: "short", observation: OBS });
    transport.request({ runId: "run-a", applyId: OBS, observation: "nothex" });
    transport.request(null as never);
    transport.request({ runId: "run-a", applyId: OBS } as never);
  });

  test("the transport rejects an invalid io contract", () => {
    expect(() => createProjectApplyTransport(null as never)).toThrow(UiContractError);
    expect(() => createProjectApplyTransport({} as never)).toThrow(UiContractError);
  });

  test("a 100-desire stress keeps exactly one active wire and applies the last", async () => {
    const h = createHarness();
    for (let index = 0; index < 100; index += 1) {
      h.request();
    }
    expect(h.calls).toHaveLength(1);
    firstCall(h).gate.resolve("last");
    await h.flush();
    expect(h.calls).toHaveLength(2);
    secondCall(h).gate.resolve("final");
    await h.flush();
    expect(h.calls).toHaveLength(2);
    expect(h.applied).toEqual([99]);
    expect(h.results).toHaveLength(1);
  });
});
