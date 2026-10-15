/**
 * Permanent suite of the browser UI inspect scheduling on the REAL
 * module wiring: the fake-DOM harness boots the shipped `app.js` and
 * drives the production call chains (event listeners → adapter
 * functions → inspect transport → `requestApiJson`) with an
 * instrumented fetch. Every test proves the single-flight invariant —
 * a new inspect fetch never begins while the previous one is unsettled,
 * even when the previous result is already superseded — plus the exact
 * coalescing semantics, the preserved polling contract and the
 * never-applied superseded response.
 */
import { describe, expect, test } from "bun:test";
import {
  createUiHarness,
  jsonResponse,
  projectsDocument,
  runDocument,
  runsPageDocument,
  type UiHarness,
  type UiHarnessClock,
} from "./ui_dom_harness.ts";

interface WireEvent {
  readonly kind: "start" | "finish";
  readonly runId: string;
  readonly seq: number;
  readonly method: string;
  readonly path: string;
}

interface InspectPlan {
  readonly revision: number;
  readonly status?: string;
  readonly boundary?: Record<string, unknown>;
  readonly hold?: boolean;
}

interface WiringProbe {
  readonly fetchImpl: (path: string, init?: RequestInit) => Promise<Response>;
  readonly events: WireEvent[];
  readonly posts: string[];
  active(): number;
  maxActive(): number;
  /** The number of inspect GETs that entered the wire so far. */
  callsCount(): number;
  releaseInspects(): void;
  releasePosts(): void;
}

/**
 * The instrumented wiring fetch: inspect GETs (`/v1/runs/:id`) carry
 * the single-flight measurement; every other request is recorded but
 * never counted. Plans are consumed per inspect call in order; a held
 * plan registers a release gate for `releaseInspects`.
 */
function makeWiringFetch(
  runs: readonly string[],
  plans: readonly InspectPlan[],
  options: {
    postResults?: Record<string, Record<string, unknown>>;
    postHold?: boolean;
    postFails?: boolean;
  } = {},
): WiringProbe {
  const events: WireEvent[] = [];
  const posts: string[] = [];
  let seq = 0;
  let inspectSeq = 0;
  let active = 0;
  let maxActive = 0;
  const inspectGates: (() => void)[] = [];
  const postGates: (() => void)[] = [];
  const fetchImpl = async (path: string, init?: RequestInit) => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    seq += 1;
    const match = /^\/v1\/runs\/([^/?]+)$/.exec(path);
    if (match !== null && method === "GET") {
      inspectSeq += 1;
      const call = inspectSeq;
      events.push({ kind: "start", runId: match[1] as string, seq: call, method, path });
      active += 1;
      if (active > maxActive) maxActive = active;
      try {
        const plan = plans[call - 1] ?? { revision: call, status: "running", boundary: { kind: "in_flight" } };
        if (plan.hold === true) {
          await new Promise<void>((resolve) => {
            inspectGates.push(resolve);
          });
        }
        return jsonResponse(
          runDocument(match[1] as string, plan.revision, plan.status ?? "running", plan.boundary ?? { kind: "in_flight" }),
        );
      } finally {
        active -= 1;
        events.push({ kind: "finish", runId: match[1] as string, seq: call, method, path });
      }
    }
    if (method === "POST") {
      posts.push(`${method} ${path}`);
      if (options.postFails === true) {
        throw new Error("network gone");
      }
      if (options.postHold === true) {
        await new Promise<void>((resolve) => {
          postGates.push(resolve);
        });
      }
      const result = options.postResults?.[path] ?? {
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome: "succeeded",
        exit_code: 0,
      };
      return jsonResponse(result);
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(runs));
    }
    throw new Error(`unexpected harness request ${method} ${path}`);
  };
  return {
    fetchImpl,
    events,
    posts,
    active: () => active,
    maxActive: () => maxActive,
    callsCount: () => inspectSeq,
    releaseInspects: () => {
      for (const gate of inspectGates.splice(0)) {
        gate();
      }
    },
    releasePosts: () => {
      for (const gate of postGates.splice(0)) {
        gate();
      }
    },
  };
}

function fakeClock(): UiHarnessClock {
  let now = 0;
  let pending: { fireAt: number; callback: () => void; id: number } | null = null;
  let fired = 0;
  let nextId = 1;
  return {
    setTimeout(callback: () => void, delayMs: number): unknown {
      if (pending !== null) {
        throw new Error("second pending timer");
      }
      pending = { fireAt: now + delayMs, callback, id: nextId };
      return nextId++;
    },
    clearTimeout(handle: unknown): void {
      if (pending !== null && pending.id === handle) {
        pending = null;
      }
    },
    advance(ms: number): number {
      now += ms;
      let firedNow = 0;
      if (pending !== null && pending.fireAt <= now) {
        const callback = pending.callback;
        pending = null;
        fired += 1;
        firedNow = 1;
        callback();
      }
      return firedNow;
    },
    hasPending: () => pending !== null,
  };
}

async function boot(
  probe: WiringProbe,
  clock: UiHarnessClock | null,
  runs: readonly string[],
): Promise<UiHarness> {
  const harness = await createUiHarness(clock === null ? { fetchImpl: probe.fetchImpl } : { fetchImpl: probe.fetchImpl, clock });
  harness.click("refresh-runs");
  await Bun.sleep(2);
  expect(harness.runRows()).toEqual([...runs]);
  return harness;
}

function inspectKinds(probe: WiringProbe): string[] {
  return probe.events.map((event) => event.kind);
}

describe("ui inspect wiring: the single-flight invariant on the real module", () => {
  test("w1. a manual takeover never overlaps and never applies the superseded response", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5, hold: true },
      { revision: 6 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    // inspect A enters the wire and blocks
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    expect(probe.active()).toBe(1);
    // the user presses Refresh; the old response becomes superseded
    harness.click("inspect-selected");
    await Bun.sleep(2);
    expect(probe.maxActive()).toBe(1);
    expect(probe.events.length).toBe(1);
    // A settles superseded: the follow-up (the manual inspect) starts
    probe.releaseInspects();
    await Bun.sleep(5);
    expect(probe.maxActive()).toBe(1);
    expect(inspectKinds(probe)).toEqual(["start", "finish", "start", "finish"]);
    // the superseded revision 5 was never rendered; B's revision 6 was
    expect(harness.element("detail-revision").textContent).toBe("6");
    expect(harness.detailStatus()).toBe("running");
    harness.dispose();
  });

  test("w2. a selection change drops the blocked response and inspects the new run", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a", "run-b"], [
      { revision: 5, hold: true },
      { revision: 6 },
    ]);
    const harness = await boot(probe, clock, ["run-a", "run-b"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    expect(probe.active()).toBe(1);
    harness.clickRunInspect(1);
    await Bun.sleep(2);
    expect(probe.maxActive()).toBe(1);
    expect(harness.detailRunId()).toBe("Run run-b");
    probe.releaseInspects();
    await Bun.sleep(5);
    expect(probe.maxActive()).toBe(1);
    expect(probe.events.map((event) => `${event.kind}:${event.runId}`)).toEqual([
      "start:run-a",
      "finish:run-a",
      "start:run-b",
      "finish:run-b",
    ]);
    // run-a's stale response never applied to run-b
    expect(harness.element("detail-revision").textContent).toBe("6");
    harness.dispose();
  });

  test("w3. the timer-driven poll respects the 2000 ms period measured from completion", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5 },
      { revision: 6 },
      { revision: 7 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    // the applied in_flight snapshot armed exactly one 2000 ms timer
    expect(clock.hasPending()).toBe(true);
    expect(clock.advance(1999)).toBe(0);
    expect(clock.advance(1)).toBe(1);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    expect(probe.maxActive()).toBe(1);
    // the completed poll re-arms a full period from its own completion
    expect(clock.hasPending()).toBe(true);
    expect(clock.advance(1999)).toBe(0);
    expect(clock.advance(1)).toBe(1);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(3);
    expect(harness.element("detail-revision").textContent).toBe("7");
    harness.dispose();
  });

  test("w4. a slow poll never queues a second one", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5 },
      { revision: 6, hold: true },
      { revision: 7 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    clock.advance(2000);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    expect(probe.active()).toBe(1);
    // the timer fired while the poll was on the wire: no second poll
    clock.advance(10000);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    expect(clock.hasPending()).toBe(false);
    probe.releaseInspects();
    await Bun.sleep(5);
    expect(probe.callsCount()).toBe(2);
    expect(clock.hasPending()).toBe(true);
    harness.dispose();
  });

  test("w5. hidden cancels the timer; visible runs exactly one immediate inspect", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5 },
      { revision: 6 },
      { revision: 7 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    expect(clock.hasPending()).toBe(true);
    harness.setVisibility(false);
    expect(clock.hasPending()).toBe(false);
    // the tab comes back: exactly one immediate inspect
    harness.setVisibility(true);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    expect(clock.hasPending()).toBe(true);
    // hidden again: nothing starts after the pending timer is cancelled
    harness.setVisibility(false);
    expect(clock.hasPending()).toBe(false);
    clock.advance(10000);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    harness.dispose();
  });

  test("w6. live updates off then on run exactly one inspect on an eligible boundary", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5 },
      { revision: 6 },
      { revision: 7 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    harness.setLiveToggle(false);
    expect(clock.hasPending()).toBe(false);
    harness.setLiveToggle(true);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    // on a non-eligible boundary the toggle never starts anything
    const stopped = makeWiringFetch(["run-a"], [
      { revision: 5, status: "waiting", boundary: { kind: "waiting", waitIndex: 1, reason: "r", actions: [{ id: "continue_stage", to: "architect" }] } },
      { revision: 6 },
    ]);
    const harness2 = await boot(stopped, clock, ["run-a"]);
    harness2.clickRunInspect(0);
    await Bun.sleep(2);
    harness2.setLiveToggle(false);
    harness2.setLiveToggle(true);
    await Bun.sleep(2);
    expect(stopped.callsCount()).toBe(1);
    harness2.dispose();
    harness.dispose();
  });

  test("w7. a mutation superseding a blocked inspect waits for its settlement", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5, status: "running", boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 3 } },
      { revision: 6, hold: true },
      { revision: 7, status: "running", boundary: { kind: "in_flight" } },
    ], {
      postHold: true,
    });
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    expect(harness.controlsText()).toContain("Resume run");
    // the mutation starts; no inspect is on the wire yet
    harness.clickButton("run-controls", "Resume run");
    await Bun.sleep(2);
    expect(probe.posts).toEqual(["POST /v1/runs/run-a/resume"]);
    expect(probe.callsCount()).toBe(1);
    // during the POST the user inspects again: the manual inspect runs
    harness.click("inspect-selected");
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(2);
    expect(probe.active()).toBe(1);
    // the POST completes: the authoritative recovery inspect coalesces
    // behind the blocked manual inspect and must never overlap it
    probe.releasePosts();
    await Bun.sleep(5);
    expect(probe.callsCount()).toBe(2);
    expect(probe.maxActive()).toBe(1);
    // the manual inspect settles superseded; the recovery inspect runs
    probe.releaseInspects();
    await Bun.sleep(5);
    expect(probe.callsCount()).toBe(3);
    expect(probe.maxActive()).toBe(1);
    expect(harness.element("detail-revision").textContent).toBe("7");
    expect(clock.hasPending()).toBe(true);
    harness.dispose();
  });

  test("w8. run_busy is inspect-only: one recovery inspect follows the failed result", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5, status: "running", boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 3 } },
      { revision: 6, status: "running", boundary: { kind: "in_flight" } },
    ], {
      postResults: {
        "/v1/runs/run-a/resume": {
          schema_version: 1,
          kind: "command_result",
          run_id: "run-a",
          outcome: "failed",
          exit_code: 1,
          reason: "run_busy",
        },
      },
    });
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    harness.clickButton("run-controls", "Resume run");
    await Bun.sleep(5);
    expect(probe.posts.length).toBe(1);
    expect(probe.callsCount()).toBe(2);
    expect(probe.maxActive()).toBe(1);
    // the failed result never retried the POST; the recovery inspect
    // applied the authoritative state instead
    expect(harness.element("detail-revision").textContent).toBe("6");
    expect(harness.error()).toBe("");
    harness.dispose();
  });

  test("w9. a lost POST is recovered through exactly one inspect, never a retry", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5, status: "running", boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 3 } },
      { revision: 6, status: "running", boundary: { kind: "in_flight" } },
    ], {
      postFails: true,
    });
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    harness.clickButton("run-controls", "Resume run");
    await Bun.sleep(5);
    expect(probe.posts.length).toBe(1);
    expect(probe.callsCount()).toBe(2);
    expect(probe.maxActive()).toBe(1);
    harness.dispose();
  });

  test("w10. ten refresh clicks during one blocked inspect coalesce into one follow-up", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5, hold: true },
      { revision: 6 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    for (let index = 0; index < 10; index += 1) {
      harness.click("inspect-selected");
    }
    await Bun.sleep(2);
    expect(probe.callsCount()).toBe(1);
    expect(probe.maxActive()).toBe(1);
    probe.releaseInspects();
    await Bun.sleep(5);
    expect(probe.callsCount()).toBe(2);
    expect(probe.maxActive()).toBe(1);
    expect(harness.element("detail-revision").textContent).toBe("6");
    harness.dispose();
  });

  test("w11. the observation machinery never sends a mutating request", async () => {
    const clock = fakeClock();
    const probe = makeWiringFetch(["run-a"], [
      { revision: 5 },
      { revision: 6 },
      { revision: 7 },
      { revision: 8 },
    ]);
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    clock.advance(2000);
    await Bun.sleep(2);
    clock.advance(2000);
    await Bun.sleep(2);
    harness.setVisibility(false);
    harness.setVisibility(true);
    await Bun.sleep(2);
    harness.setLiveToggle(false);
    harness.setLiveToggle(true);
    await Bun.sleep(2);
    expect(probe.callsCount()).toBeGreaterThanOrEqual(3);
    expect(probe.posts).toEqual([]);
    harness.dispose();
  });
});

describe("ui inspect wiring: the single-flight stress battery on the real module", () => {
  test("x1. one hundred takeover/selection/visibility sequences never overlap", async () => {
    let seed = 0x1b8735af;
    const random = (bound: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed % bound;
    };
    let maxActiveAcrossAll = 0;
    for (let sequence = 0; sequence < 100; sequence += 1) {
      const plans: InspectPlan[] = [];
      for (let index = 0; index < 8; index += 1) {
        plans.push({ revision: 100 + sequence * 10 + index, hold: true });
      }
      const probe = makeWiringFetch(["run-a"], plans);
      const harness = await createUiHarness({ fetchImpl: probe.fetchImpl });
      harness.click("refresh-runs");
      await Bun.sleep(1);
      harness.clickRunInspect(0);
      await Bun.sleep(1);
      const events = random(4);
      for (let step = 0; step <= events; step += 1) {
        const choice = random(3);
        if (choice === 0) {
          harness.click("inspect-selected");
        } else if (choice === 1) {
          harness.clickRunInspect(0);
        } else {
          harness.setVisibility(random(2) === 0);
        }
        maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      }
      expect(probe.maxActive()).toBe(1);
      for (;;) {
        if (probe.active() === 0) {
          break;
        }
        probe.releaseInspects();
        await Bun.sleep(1);
        maxActiveAcrossAll = Math.max(maxActiveAcrossAll, probe.maxActive());
      }
      await Bun.sleep(1);
      expect(probe.maxActive()).toBe(1);
      expect(probe.callsCount()).toBeLessThanOrEqual(plans.length);
      harness.dispose();
    }
    expect(maxActiveAcrossAll).toBe(1);
  });
});
