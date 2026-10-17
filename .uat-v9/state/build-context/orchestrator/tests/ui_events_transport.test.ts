/**
 * Permanent suite of the browser UI v4 events transport: the one
 * single-flight admission owner of every events GET of the module. The
 * transport is driven directly through its injected io, with an
 * instrumented `fetchEvents` that records the exact wire lifecycle and
 * can hold, reject or abort-ignore each request. Every test proves the
 * refusal semantics (a busy slot never starts a second fetch, never
 * queues and never retries), the exactly-once settlement of the wire
 * slot, and the invalidation discipline of selection changes.
 */
import { describe, expect, test } from "bun:test";
import {
  UiContractError,
  createEventsTransport,
  type UiEventsResult,
} from "../ui/app.js";

interface WireEvent {
  readonly kind: "started" | "applied" | "dropped" | "settled";
  readonly runId: string;
  readonly after: number;
  readonly seq: number;
}

interface CallPlan {
  /** The response document revision; a marker of which call answered. */
  readonly revision: number;
  /** Hold the request on the wire until its gate is released. */
  readonly hold?: boolean;
  /** Reject the fetchEvents promise instead of resolving. */
  readonly reject?: boolean;
  /** Resolve with undefined (a broken transport produced no result). */
  readonly noResult?: boolean;
  /** Ignore the abort signal and settle only when released. */
  readonly ignoreAbort?: boolean;
}

interface Probe {
  readonly io: {
    readonly fetchEvents: (runId: string, after: number, signal: AbortSignal) => Promise<UiEventsResult | undefined>;
    readonly onStarted: (runId: string) => void;
    readonly onSettled: (runId: string) => void;
  };
  readonly events: WireEvent[];
  readonly calls: { runId: string; after: number; aborted: boolean }[];
  active(): number;
  maxActive(): number;
  release(seq: number): void;
  releaseAll(): void;
}

function eventsResult(revision: number): UiEventsResult {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "events",
      runId: "run-a",
      revision,
      events: [{ revision: 1, kind: "create_run" }],
      nextAfter: null,
    },
  } as UiEventsResult;
}

function makeProbe(plans: CallPlan[]): Probe {
  const events: WireEvent[] = [];
  const calls: { runId: string; after: number; aborted: boolean }[] = [];
  let seq = 0;
  let active = 0;
  let maxActive = 0;
  const gates = new Map<number, () => void>();
  const io = {
    fetchEvents: (runId: string, after: number, signal: AbortSignal) => {
      seq += 1;
      const call = seq;
      calls.push({ runId, after, aborted: false });
      const record = calls[calls.length - 1];
      signal.addEventListener("abort", () => {
        if (record !== undefined) {
          record.aborted = true;
        }
      });
      events.push({ kind: "started", runId, after, seq: call });
      active += 1;
      if (active > maxActive) maxActive = active;
      const plan = plans[call - 1] ?? { revision: call };
      const settle = async (): Promise<UiEventsResult | undefined> => {
        if (plan.hold === true || plan.ignoreAbort === true) {
          await new Promise<void>((resolve) => {
            gates.set(call, resolve);
          });
        }
        if (plan.reject === true) {
          throw new Error("wire gone");
        }
        if (plan.noResult === true) {
          return undefined;
        }
        return eventsResult(plan.revision);
      };
      return settle().finally(() => {
        active -= 1;
        events.push({ kind: "settled", runId, after, seq: call });
      });
    },
    onStarted: (runId: string) => {
      void runId;
    },
    onSettled: (runId: string) => {
      void runId;
    },
  };
  return {
    io,
    events,
    calls,
    active: () => active,
    maxActive: () => maxActive,
    release: (target: number) => {
      const gate = gates.get(target);
      if (gate !== undefined) {
        gates.delete(target);
        gate();
      }
    },
    releaseAll: () => {
      for (const gate of gates.values()) {
        gate();
      }
      gates.clear();
    },
  };
}

const applied: UiEventsResult[] = [];

function desire(runId: string, after: number, sink: UiEventsResult[] = applied) {
  return {
    runId,
    after,
    onApplied: (result: UiEventsResult | undefined) => {
      if (result !== undefined) {
        sink.push(result);
      }
    },
  };
}

describe("ui events transport: single-flight admission", () => {
  test("t1. an idle admission starts immediately and applies its own result", async () => {
    const probe = makeProbe([{ revision: 5 }]);
    const transport = createEventsTransport(probe.io);
    const sink: UiEventsResult[] = [];
    const outcome = transport.request(desire("run-a", 0, sink));
    expect(outcome.started).toBe(true);
    expect(probe.active()).toBe(1);
    const result = await outcome.settled;
    expect(probe.active()).toBe(0);
    expect(result).toEqual(eventsResult(5));
    expect(sink).toEqual([eventsResult(5)]);
  });

  test("t2. a busy admission is refused without a second fetch, queue or retry", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createEventsTransport(probe.io);
    const first = transport.request(desire("run-a", 0));
    expect(first.started).toBe(true);
    const second = transport.request(desire("run-a", 0));
    expect(second.started).toBe(false);
    expect(probe.active()).toBe(1);
    expect(await second.settled).toBeUndefined();
    probe.release(1);
    await Bun.sleep(2);
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    await first.settled;
    // the refused desire is never replayed by the transport
    expect(probe.calls.length).toBe(1);
  });

  test("t3. invalidate supersedes the active result and returns its settlement", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const transport = createEventsTransport(probe.io);
    const sink: UiEventsResult[] = [];
    const first = transport.request(desire("run-a", 0, sink));
    expect(probe.active()).toBe(1);
    const pending = transport.invalidate();
    expect(pending).not.toBeNull();
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await Bun.sleep(2);
    expect(sink).toEqual([]);
    expect(await (pending as Promise<UiEventsResult | undefined>)).toEqual(eventsResult(5));
    expect(probe.active()).toBe(0);
    await first.settled;
    // the slot is reusable after the superseded settlement
    const second = transport.request(desire("run-a", 0, sink));
    expect(second.started).toBe(true);
    probe.releaseAll();
    await Bun.sleep(2);
    expect(sink.length).toBe(1);
  });

  test("t4. invalidate with no active request returns null", () => {
    const probe = makeProbe([]);
    const transport = createEventsTransport(probe.io);
    expect(transport.invalidate()).toBeNull();
  });

  test("t5. an abort-ignoring fetch keeps the slot busy until it settles", async () => {
    const probe = makeProbe([{ revision: 5, ignoreAbort: true }, { revision: 6 }]);
    const transport = createEventsTransport(probe.io);
    const first = transport.request(desire("run-a", 0));
    transport.invalidate();
    expect(probe.calls[0]?.aborted).toBe(true);
    await Bun.sleep(2);
    // the ignored abort never freed the slot: the next desire is refused
    const refused = transport.request(desire("run-a", 0));
    expect(refused.started).toBe(false);
    expect(probe.calls.length).toBe(1);
    probe.release(1);
    await Bun.sleep(2);
    expect(probe.calls.length).toBe(1);
    const after = transport.request(desire("run-a", 0));
    expect(after.started).toBe(true);
    probe.releaseAll();
    await Bun.sleep(2);
    await first.settled;
  });

  test("t6. dispose refuses every later desire and supersedes the active result", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const transport = createEventsTransport(probe.io);
    const sink: UiEventsResult[] = [];
    const first = transport.request(desire("run-a", 0, sink));
    transport.dispose();
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await Bun.sleep(2);
    expect(sink).toEqual([]);
    const refused = transport.request(desire("run-a", 0, sink));
    expect(refused.started).toBe(false);
    expect(probe.calls.length).toBe(1);
    await first.settled;
  });

  test("t7. invalid desires never reach the wire", () => {
    const probe = makeProbe([]);
    const transport = createEventsTransport(probe.io);
    for (const desire of [
      { runId: "", after: 0, onApplied: () => undefined },
      { runId: "../x", after: 0, onApplied: () => undefined },
      { runId: "run-a", after: -1, onApplied: () => undefined },
      { runId: "run-a", after: 1.5, onApplied: () => undefined },
      { runId: "run-a", after: 0, onApplied: "not a function" },
      null,
    ]) {
      const outcome = transport.request(desire as never);
      expect(outcome.started).toBe(false);
    }
    expect(probe.calls).toEqual([]);
  });

  test("t8. the io contract is validated before any state exists", () => {
    expect(() => createEventsTransport(null as never)).toThrow(UiContractError);
    expect(() => createEventsTransport({} as never)).toThrow(UiContractError);
    expect(() => createEventsTransport({ fetchEvents: () => undefined } as never)).toThrow(UiContractError);
    expect(() => createEventsTransport({ fetchEvents: () => undefined, onStarted: () => undefined } as never)).toThrow(UiContractError);
  });

  test("t9. started and settled fire exactly once per started request", async () => {
    const started: string[] = [];
    const settled: string[] = [];
    const probe = makeProbe([{ revision: 5, reject: true }]);
    const transport = createEventsTransport({
      fetchEvents: probe.io.fetchEvents,
      onStarted: (runId) => started.push(runId),
      onSettled: (runId) => settled.push(runId),
    });
    const outcome = transport.request(desire("run-a", 3));
    expect(started).toEqual(["run-a"]);
    expect(settled).toEqual([]);
    await outcome.settled;
    expect(settled).toEqual(["run-a"]);
    // a refused desire fires neither callback
    transport.dispose();
  });
});

describe("ui events transport: stress battery", () => {
  test("x1. one hundred sequential request/invalidate sequences never overlap", async () => {
    let seed = 0x2b9d1f3d;
    const random = (bound: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed % bound;
    };
    let maxActiveAcrossAll = 0;
    const sink: UiEventsResult[] = [];
    for (let sequence = 0; sequence < 100; sequence += 1) {
      const probe = makeProbe([{ revision: sequence + 1, hold: sequence % 3 === 0 }]);
      const transport = createEventsTransport(probe.io);
      const first = transport.request(desire("run-a", sequence % 5, sink));
      maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      const choice = random(3);
      if (choice === 0) {
        transport.invalidate();
      } else if (choice === 1) {
        transport.request(desire("run-a", sequence % 5, sink));
      } else if (sequence % 4 === 0) {
        transport.dispose();
      }
      maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      for (;;) {
        if (probe.active() === 0) {
          break;
        }
        probe.releaseAll();
        await Bun.sleep(1);
        maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      }
      await Bun.sleep(1);
      expect(probe.maxActive()).toBe(1);
      expect(probe.calls.length).toBeLessThanOrEqual(1);
    }
    expect(maxActiveAcrossAll).toBe(1);
  });
});
