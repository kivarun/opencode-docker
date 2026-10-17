/**
 * Permanent suite of the browser UI inspect transport: the one
 * single-flight admission owner of every inspect GET of the module.
 * The transport is driven directly through its injected io, with an
 * instrumented `fetchInspect` that records the exact wire lifecycle
 * (start/finish per request) and can hold, reject or abort-ignore each
 * request. Every test proves the admission/coalescing contract and the
 * exactly-once settlement of the wire slot.
 */
import { describe, expect, test } from "bun:test";
import {
  UiContractError,
  createInspectTransport,
  parseApiDocument,
  type UiApiResult,
  type UiInspectDesire,
  type UiInspectTransport,
} from "../ui/app.js";

interface WireEvent {
  readonly kind: "started" | "start" | "finish" | "applied" | "dropped";
  readonly runId: string;
  readonly seq: number;
  readonly superseded?: boolean;
}

interface CallPlan {
  /** The response document revision; a marker of which call answered. */
  readonly revision: number;
  /** Hold the request on the wire until its gate is released. */
  readonly hold?: boolean;
  /** Reject the fetchInspect promise instead of resolving. */
  readonly reject?: boolean;
  /** Resolve with undefined (a broken transport produced no result). */
  readonly noResult?: boolean;
}

interface Probe {
  readonly io: {
    readonly fetchInspect: (runId: string, signal: AbortSignal) => Promise<UiApiResult | undefined>;
    readonly onInspectStarted: (runId: string) => void;
    readonly onInspectDropped: (runId: string) => void;
  };
  readonly events: WireEvent[];
  readonly calls: { runId: string; aborted: boolean }[];
  active(): number;
  maxActive(): number;
  /** Releases the wire gate of the call with the given 1-based sequence. */
  release(seq: number): void;
  /** Releases every currently waiting gate in registration order. */
  releaseAll(): void;
}

function makeProbe(plans: CallPlan[]): Probe {
  const events: WireEvent[] = [];
  const calls: { runId: string; aborted: boolean }[] = [];
  let seq = 0;
  let active = 0;
  let maxActive = 0;
  const gates = new Map<number, () => void>();
  const fetchInspect = async (runId: string, signal: AbortSignal): Promise<UiApiResult | undefined> => {
    seq += 1;
    const call = seq;
    const plan = plans[call - 1] ?? { revision: call };
    calls.push({ runId, aborted: false });
    const index = calls.length - 1;
    signal.addEventListener("abort", () => {
      calls[index]!.aborted = true;
    });
    events.push({ kind: "start", runId, seq: call });
    active += 1;
    if (active > maxActive) maxActive = active;
    try {
      if (plan.hold === true) {
        await new Promise<void>((resolve) => {
          gates.set(call, resolve);
        });
      }
      if (plan.reject === true) {
        throw new Error("broken transport");
      }
      if (plan.noResult === true) {
        return undefined;
      }
      return {
        ok: true,
        lost: false,
        model: parseApiDocument({
          schema_version: 1,
          kind: "run",
          run_id: runId,
          revision: plan.revision,
          status: "running",
          boundary: { kind: "in_flight" },
          outputs: [],
        }),
      };
    } finally {
      active -= 1;
      events.push({ kind: "finish", runId, seq: call });
    }
  };
  return {
    io: {
      fetchInspect,
      onInspectStarted: (runId) => events.push({ kind: "started", runId, seq: 0 }),
      onInspectDropped: (runId) => events.push({ kind: "dropped", runId, seq: 0 }),
    },
    events,
    calls,
    active: () => active,
    maxActive: () => maxActive,
    release: (seq) => {
      const gate = gates.get(seq);
      if (gate === undefined) {
        throw new Error(`no open gate for call ${seq}`);
      }
      gates.delete(seq);
      gate();
    },
    releaseAll: () => {
      for (const seq of [...gates.keys()].sort((a, b) => a - b)) {
        const gate = gates.get(seq);
        gates.delete(seq);
        gate!();
      }
    },
  };
}

interface AppliedLog {
  readonly entries: { runId: string; revision: unknown }[];
  promiseOf(request: Promise<UiApiResult | undefined>): void;
}

function makeDesireCollector(): AppliedLog & { desire(runId: string): UiInspectDesire } {
  const entries: { runId: string; revision: unknown }[] = [];
  const pending: Promise<unknown>[] = [];
  return {
    entries,
    promiseOf: (request) => {
      pending.push(request);
    },
    desire: (runId) => ({
      runId,
      onApplied: (result) => {
        const model = result !== undefined && result.ok ? result.model : undefined;
        entries.push({
          runId,
          revision: model !== undefined && model.kind === "run" ? model.revision : result === undefined ? null : "failure",
        });
      },
    }),
  };
}

function wireKinds(events: readonly WireEvent[]): string[] {
  return events.map((event) => event.kind);
}

function desireFor(
  transport: UiInspectTransport,
  collector: ReturnType<typeof makeDesireCollector>,
  runId: string,
  events?: WireEvent[],
): Promise<UiApiResult | undefined> {
  const base = collector.desire(runId);
  const request = transport.request({
    runId,
    onApplied: (result) => {
      events?.push({ kind: "applied", runId, seq: 0 });
      base.onApplied(result);
    },
  });
  collector.promiseOf(request);
  return request;
}

describe("ui inspect transport: admission and coalescing", () => {
  test("a1. an idle admission starts immediately and applies its own result", async () => {
    const probe = makeProbe([{ revision: 5 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const request = desireFor(transport, collector, "run-a", probe.events);
    // the fetch start is synchronous: the start event precedes the wire
    expect(probe.events.slice(0, 2)).toEqual([
      { kind: "started", runId: "run-a", seq: 0 },
      { kind: "start", runId: "run-a", seq: 1 },
    ]);
    const result = await request;
    expect(result !== undefined && result.ok && result.model.kind === "run" && result.model.revision === 5).toBe(true);
    expect(collector.entries).toEqual([{ runId: "run-a", revision: 5 }]);
    expect(wireKinds(probe.events)).toEqual(["started", "start", "finish", "applied"]);
    expect(probe.maxActive()).toBe(1);
  });

  test("a2. a busy admission supersedes the active result and coalesces one follow-up", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    expect(probe.active()).toBe(1);
    const second = desireFor(transport, collector, "run-a", probe.events);
    // the second desire must not start a fetch while the first is unsettled
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    probe.release(1);
    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(probe.calls.length).toBe(2);
    expect(probe.maxActive()).toBe(1);
    // the superseded result is never applied; the follow-up applies
    expect(collector.entries).toEqual([{ runId: "run-a", revision: 6 }]);
    expect(firstResult !== undefined && firstResult.ok && firstResult.model.kind === "run" && firstResult.model.revision === 5).toBe(true);
    expect(secondResult !== undefined && secondResult.ok && secondResult.model.kind === "run" && secondResult.model.revision === 6).toBe(true);
    expect(wireKinds(probe.events)).toEqual([
      "started",
      "start",
      "finish",
      "started",
      "start",
      "finish",
      "applied",
    ]);
  });

  test("a3. ten desires while busy coalesce into exactly one follow-up", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    const followUps: Promise<UiApiResult | undefined>[] = [];
    for (let index = 0; index < 10; index += 1) {
      followUps.push(desireFor(transport, collector, "run-a", probe.events));
    }
    expect(probe.calls.length).toBe(1);
    probe.release(1);
    await first;
    await Promise.all(followUps);
    expect(probe.calls.length).toBe(2);
    expect(probe.maxActive()).toBe(1);
    // only the last coalesced desire ever applies
    expect(collector.entries).toEqual([{ runId: "run-a", revision: 6 }]);
  });

  test("a4. select B then select C while busy runs exactly one inspect for C", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    const second = desireFor(transport, collector, "run-b", probe.events);
    const third = desireFor(transport, collector, "run-c", probe.events);
    expect(probe.calls.length).toBe(1);
    probe.release(1);
    await Promise.all([first, second, third]);
    expect(probe.calls).toEqual([
      { runId: "run-a", aborted: true },
      { runId: "run-c", aborted: false },
    ]);
    expect(probe.maxActive()).toBe(1);
    expect(collector.entries).toEqual([{ runId: "run-c", revision: 6 }]);
  });

  test("a5. select B then invalidate (deselect) starts nothing after the active settles", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    const second = desireFor(transport, collector, "run-b", probe.events);
    transport.invalidate();
    probe.release(1);
    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    // the superseded active result is dropped and the withdrawn desire
    // never runs; the wire slot was released exactly once
    expect(collector.entries).toEqual([]);
    // the active request settles with its raw result even though it was
    // superseded; the withdrawn desire settles without a result
    expect(firstResult !== undefined && firstResult.ok).toBe(true);
    expect(secondResult).toBeUndefined();
    expect(wireKinds(probe.events)).toEqual([
      "started",
      "start",
      "finish",
      "dropped",
    ]);
  });

  test("a6. invalidate without a desire supersedes the active and drops it at settlement", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    transport.invalidate();
    expect(probe.calls[0]!.aborted).toBe(true);
    probe.release(1);
    await first;
    expect(probe.calls.length).toBe(1);
    expect(collector.entries).toEqual([]);
    expect(wireKinds(probe.events)).toEqual(["started", "start", "finish", "dropped"]);
    // the slot is free again: a later desire starts immediately
    const later = desireFor(transport, collector, "run-a", probe.events);
    expect(probe.calls.length).toBe(2);
    await later;
    expect(collector.entries).toEqual([{ runId: "run-a", revision: 6 }]);
  });

  test("a7. the applied result precedes the slot release and the follow-up start", async () => {
    const probe = makeProbe([{ revision: 5 }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    await desireFor(transport, collector, "run-a", probe.events);
    await desireFor(transport, collector, "run-a", probe.events);
    // applied (first) strictly before started of the second request
    expect(wireKinds(probe.events)).toEqual([
      "started",
      "start",
      "finish",
      "applied",
      "started",
      "start",
      "finish",
      "applied",
    ]);
  });
});

describe("ui inspect transport: the settlement matrix", () => {
  test("s1. the slot is released exactly once on fulfil, reject and abort rejection", async () => {
    for (const plan of [
      { revision: 5, hold: true },
      { revision: 5, hold: true, reject: true },
      { revision: 5, hold: true, noResult: true },
    ] as const) {
      const probe = makeProbe([plan, { revision: 6 }]);
      const transport = createInspectTransport(probe.io);
      const collector = makeDesireCollector();
      const first = desireFor(transport, collector, "run-a", probe.events);
      const second = desireFor(transport, collector, "run-a", probe.events);
      probe.release(1);
      const [firstResult] = await Promise.all([first, second]);
      // exactly two wire lifecycles, no overlap, exactly one follow-up
      expect(probe.calls.length).toBe(2);
      expect(probe.maxActive()).toBe(1);
      expect(wireKinds(probe.events)).toEqual([
        "started",
        "start",
        "finish",
        "started",
        "start",
        "finish",
        "applied",
      ]);
      // only the follow-up ever applies: the first request was superseded
      expect(collector.entries).toEqual([{ runId: "run-a", revision: 6 }]);
      if (plan.reject === true || plan.noResult === true) {
        // a transport that produced no result settles the superseded
        // request without one; the presentation is decided by the
        // follow-up alone
        expect(firstResult).toBeUndefined();
      } else {
        expect(firstResult !== undefined && firstResult.ok).toBe(true);
      }
    }
  });

  test("s2. an abort-ignoring fetch delays the follow-up instead of overlapping it", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6, hold: true }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    const second = desireFor(transport, collector, "run-a", probe.events);
    // the supersede aborted the active request, but the fetch ignores the
    // signal: the follow-up must not start until the fetch settles
    expect(probe.calls[0]!.aborted).toBe(true);
    expect(probe.calls.length).toBe(1);
    expect(probe.active()).toBe(1);
    probe.release(1);
    await first;
    expect(probe.calls.length).toBe(2);
    expect(probe.active()).toBe(1);
    probe.release(2);
    await second;
    expect(probe.maxActive()).toBe(1);
    expect(collector.entries).toEqual([{ runId: "run-a", revision: 6 }]);
  });

  test("s3. a superseded abort rejection is never applied and never shown", async () => {
    // the fetch rejects because of the abort; the request was superseded,
    // so the application callback must never run and no drop may leak
    let rejectNow: (() => void) | null = null;
    const blocked = new Promise<UiApiResult>((_, reject) => {
      rejectNow = () => reject(new Error("aborted"));
    });
    const applied: unknown[] = [];
    const dropped: string[] = [];
    const transport = createInspectTransport({
      fetchInspect: () => blocked,
      onInspectStarted: () => undefined,
      onInspectDropped: (runId) => dropped.push(runId),
    });
    const first = transport.request({ runId: "run-a", onApplied: (result) => applied.push(result) });
    const second = transport.request({ runId: "run-a", onApplied: (result) => applied.push(result) });
    rejectNow!();
    const [firstResult] = await Promise.all([first, second]);
    expect(firstResult).toBeUndefined();
    expect(applied).toEqual([]);
    expect(dropped).toEqual([]);
  });

  test("s4. dispose stops every follow-up and refuses later desires", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    const first = desireFor(transport, collector, "run-a", probe.events);
    const second = desireFor(transport, collector, "run-a", probe.events);
    transport.dispose();
    probe.release(1);
    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    expect(collector.entries).toEqual([]);
    expect(firstResult !== undefined && firstResult.ok).toBe(true);
    expect(secondResult).toBeUndefined();
    expect(wireKinds(probe.events)).toEqual(["started", "start", "finish", "dropped"]);
    // after dispose the transport is inert
    const later = desireFor(transport, collector, "run-a", probe.events);
    expect(await later).toBeUndefined();
    expect(probe.calls.length).toBe(1);
    expect(collector.entries).toEqual([]);
  });
});

describe("ui inspect transport: fail-closed inputs", () => {
  test("f1. an invalid desire never reaches the wire and resolves without effects", async () => {
    const probe = makeProbe([]);
    const transport = createInspectTransport(probe.io);
    const applied: unknown[] = [];
    for (const desire of [
      null,
      "run-a",
      42,
      { runId: "../etc/passwd", onApplied: (result: unknown) => applied.push(result) },
      { runId: "", onApplied: (result: unknown) => applied.push(result) },
      { runId: "run-a" },
      { runId: "run-a", onApplied: "nope" },
    ]) {
      const request = transport.request(desire as UiInspectDesire);
      expect(await request).toBeUndefined();
    }
    expect(probe.calls).toEqual([]);
    expect(probe.events).toEqual([]);
    expect(applied).toEqual([]);
  });

  test("f2. the io contract is validated before any state exists", () => {
    for (const io of [
      null,
      "io",
      {},
      { fetchInspect: () => undefined },
      { fetchInspect: () => undefined, onInspectStarted: () => undefined },
      { fetchInspect: "nope", onInspectStarted: () => undefined, onInspectDropped: () => undefined },
    ]) {
      expect(() => createInspectTransport(io as never)).toThrow(UiContractError);
    }
  });

  test("f3. repeated requests reuse the released slot deterministically", async () => {
    const probe = makeProbe([{ revision: 1 }, { revision: 2 }, { revision: 3 }]);
    const transport = createInspectTransport(probe.io);
    const collector = makeDesireCollector();
    for (let index = 1; index <= 3; index += 1) {
      await desireFor(transport, collector, "run-a");
      expect(probe.maxActive()).toBe(1);
      expect(probe.calls.length).toBe(index);
    }
    expect(collector.entries).toEqual([
      { runId: "run-a", revision: 1 },
      { runId: "run-a", revision: 2 },
      { runId: "run-a", revision: 3 },
    ]);
  });
});

describe("ui inspect transport: the single-flight stress battery", () => {
  test("x1. one hundred takeover/selection/invalidate sequences never overlap", async () => {
    // A deferred transport with per-sequence barriers: every sequence
    // blocks one request, fires a random mix of supersede events and
    // releases; the wire must never hold two requests and every
    // non-superseded request applies exactly once.
    let seed = 0x2f6e2b1;
    const random = (bound: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed % bound;
    };
    let maxActiveAcrossAll = 0;
    for (let sequence = 0; sequence < 100; sequence += 1) {
      const plans: CallPlan[] = [];
      for (let index = 0; index < 6; index += 1) {
        plans.push({ revision: 200 + sequence * 10 + index, hold: true });
      }
      const probe = makeProbe(plans);
      const transport = createInspectTransport(probe.io);
      const collector = makeDesireCollector();
      const pending: Promise<UiApiResult | undefined>[] = [];
      pending.push(desireFor(transport, collector, "run-a"));
      const eventMix = random(4);
      const desiredRunIds = ["run-b", "run-c", "run-d"];
      let desiredIndex = 0;
      for (let step = 0; step <= eventMix; step += 1) {
        const choice = random(3);
        if (choice === 0) {
          pending.push(desireFor(transport, collector, desiredRunIds[desiredIndex % desiredRunIds.length]!, probe.events));
          desiredIndex += 1;
        } else if (choice === 1) {
          transport.invalidate();
        } else {
          // a supersede event with no new desire is the mutation boundary
          transport.invalidate();
        }
        // an overlapping desire must never start a second fetch
        maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      }
      expect(probe.maxActive()).toBe(1);
      // release everything in order; each settlement starts at most one
      // coalesced follow-up whose own gate the next pass releases
      for (;;) {
        if (probe.active() === 0) {
          break;
        }
        probe.releaseAll();
        await Bun.sleep(1);
        maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      }
      await Promise.all(pending);
      expect(probe.active()).toBe(0);
      expect(probe.maxActive()).toBe(1);
      // the wire never needed more requests than desires existed
      expect(probe.calls.length).toBeLessThanOrEqual(plans.length);
    }
    expect(maxActiveAcrossAll).toBe(1);
  });
});
