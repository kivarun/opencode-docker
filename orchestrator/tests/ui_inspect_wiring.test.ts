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
import { eventReferenceKeys } from "../ui/app.js";

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
    /** Exact runs documents served per `/v1/runs?` GET (cycled). */
    runsPages?: Record<string, unknown>[];
    /** The exact projects document served for `/v1/projects`. */
    projectsDoc?: Record<string, unknown>;
    /** Exact events documents served per events GET (cycled). */
    eventsPages?: Record<string, unknown>[];
  } = {},
): WiringProbe {
  const events: WireEvent[] = [];
  const posts: string[] = [];
  let seq = 0;
  let inspectSeq = 0;
  let runsPageSeq = 0;
  let eventsPageSeq = 0;
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
      return jsonResponse(options.projectsDoc ?? projectsDocument());
    }
    const eventsGet = /^\/v1\/runs\/([^/]+)\/events\?after=(\d+)&limit=(\d+)$/.exec(path);
    if (eventsGet !== null && options.eventsPages !== undefined) {
      const page =
        options.eventsPages[Math.min(eventsPageSeq, options.eventsPages.length - 1)] ?? null;
      eventsPageSeq += 1;
      if (page !== null) {
        return jsonResponse(page);
      }
    }
    if (path.startsWith("/v1/runs?")) {
      const page =
        options.runsPages === undefined
          ? runsPageDocument(runs)
          : (options.runsPages[Math.min(runsPageSeq, options.runsPages.length - 1)] ?? runsPageDocument(runs));
      runsPageSeq += 1;
      return jsonResponse(page);
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

/* ------------------------------------------------------------------ */
/* S1 run rail on the real module wiring: the identity line across the */
/* boundary families (w1), the capability chip authority matrix (w3),  */
/* the zero-POST transport counters (w4) and the run-list shape (w5).  */
/* ------------------------------------------------------------------ */

function railEventEntry(kind: string, revision: number, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const keys = eventReferenceKeys(kind);
  if (keys === null) {
    throw new Error(`unknown test kind ${kind}`);
  }
  const entry: Record<string, unknown> = { revision, kind };
  for (const key of keys) {
    entry[key] = key === "state_id" ? "dev_entry" : 1;
  }
  for (const key of Object.keys(overrides)) {
    entry[key] = overrides[key];
  }
  return entry;
}

/** The exact 66-event oracle of the S1 preflight (position-exact). */
function railOracle(): Record<string, unknown>[] {
  const entries: Record<string, unknown>[] = [];
  const filler = () => {
    entries.push(railEventEntry("transition_committed", entries.length + 1));
  };
  const start = (stateId: string) => {
    entries.push(railEventEntry("start_agent_execution", entries.length + 1, { execution_index: 1, state_id: stateId }));
  };
  entries.push(railEventEntry("create_run", 1));
  start("architect");
  for (let index = 0; index < 9; index += 1) filler();
  entries.push(railEventEntry("stage_generation_opened", 12, { generation_index: 1 }));
  entries.push(railEventEntry("stage_iteration_opened", 13, { generation_index: 1, iteration_index: 1 }));
  filler();
  start("dev_entry");
  for (let index = 0; index < 6; index += 1) filler();
  entries.push(railEventEntry("transition_committed", 22, { execution_index: 2, transition_index: 2 }));
  entries.push(railEventEntry("run_waiting", 23, { wait_index: 1 }));
  filler();
  entries.push(railEventEntry("iteration_grant_recorded", 25, { grant_index: 1 }));
  entries.push(railEventEntry("stage_iteration_closed", 26, { generation_index: 1, iteration_index: 1 }));
  entries.push(railEventEntry("wait_response_recorded", 27, { wait_index: 1 }));
  entries.push(railEventEntry("stage_iteration_opened", 28, { generation_index: 1, iteration_index: 2 }));
  start("dev_entry");
  for (let index = 0; index < 6; index += 1) filler();
  entries.push(railEventEntry("transition_committed", 36, { execution_index: 3, transition_index: 3 }));
  entries.push(railEventEntry("run_waiting", 37, { wait_index: 2 }));
  filler();
  filler();
  entries.push(railEventEntry("stage_iteration_closed", 40, { generation_index: 1, iteration_index: 2 }));
  entries.push(railEventEntry("wait_response_recorded", 41, { wait_index: 2 }));
  start("architect");
  for (let index = 0; index < 9; index += 1) filler();
  entries.push(railEventEntry("stage_generation_closed", 52, { generation_index: 1 }));
  entries.push(railEventEntry("stage_generation_opened", 53, { generation_index: 2 }));
  entries.push(railEventEntry("stage_iteration_opened", 54, { generation_index: 2, iteration_index: 1 }));
  entries.push(railEventEntry("transition_committed", 55, { execution_index: 4, transition_index: 4 }));
  start("dev_entry");
  for (let index = 0; index < 6; index += 1) filler();
  entries.push(railEventEntry("transition_committed", 63, { execution_index: 5, transition_index: 5 }));
  entries.push(railEventEntry("terminal_reached", 64));
  filler();
  entries.push(railEventEntry("run_succeeded", 66));
  if (entries.length !== 66) {
    throw new Error(`rail oracle drift: ${entries.length}`);
  }
  return entries;
}

const ORACLE_EVENTS_PAGE = (): Record<string, unknown> => ({
  schema_version: 1,
  kind: "events",
  run_id: "run-a",
  revision: 66,
  events: railOracle(),
  next_after: null,
});

async function railBoot(
  probe: WiringProbe,
  clock: UiHarnessClock | null,
  runs: readonly string[],
  loadProjects: boolean,
): Promise<UiHarness> {
  const harness = await createUiHarness(
    clock === null ? { fetchImpl: probe.fetchImpl } : { fetchImpl: probe.fetchImpl, clock },
  );
  harness.click("refresh-runs");
  await Bun.sleep(2);
  expect(harness.runRows()).toEqual([...runs]);
  if (loadProjects) {
    harness.click("refresh-projects");
    await Bun.sleep(2);
  }
  return harness;
}

describe("ui inspect wiring: the S1 run rail", () => {
  test("w1. the rail renders family by family (in-flight, waiting, terminal success, terminal failure)", async () => {
    const fixtures: {
      name: string;
      status: string;
      boundary: Record<string, unknown>;
      revision: number;
      identity: string;
    }[] = [
      {
        name: "in_flight",
        status: "running",
        boundary: { kind: "in_flight", executionIndex: 3, stateId: "dev_entry", executionRole: "stage", phase: "running" },
        revision: 39,
        identity: "Generation 1 · Iteration 2 · State dev_entry",
      },
      {
        name: "waiting",
        status: "waiting",
        boundary: {
          kind: "waiting",
          waitIndex: 2,
          reason: "stage_iteration_limit_exhausted",
          actions: [
            { id: "continue_stage", to: "dev_entry" },
            { id: "revise_task", to: "architect" },
          ],
        },
        revision: 37,
        identity: "Generation 1 · Iteration 2 · State dev_entry",
      },
      {
        name: "terminal success",
        status: "succeeded",
        boundary: { kind: "succeeded", terminalStateId: "success" },
        revision: 66,
        identity: "Generation 2 · Iteration 1 · State dev_entry",
      },
      {
        name: "terminal failure",
        status: "failed",
        boundary: { kind: "failed", reason: "worker_failed" },
        revision: 66,
        identity: "Generation 2 · Iteration 1 · State dev_entry",
      },
    ];
    for (const fixture of fixtures) {
      const probe = makeWiringFetch(
        ["run-a"],
        [{ revision: fixture.revision, status: fixture.status, boundary: fixture.boundary }],
        { eventsPages: [ORACLE_EVENTS_PAGE()] },
      );
      const harness = await railBoot(probe, null, ["run-a"], false);
      harness.clickRunInspect(0);
      await Bun.sleep(5);
      expect(harness.detailStatus(), fixture.name).toBe(fixture.status);
      expect(harness.railIdentityText(), fixture.name).toBe(fixture.identity);
      expect(probe.posts, fixture.name).toEqual([]);
      expect(harness.eventsRows().length, fixture.name).toBe(66);
      harness.dispose();
    }
  });

  test("w3. the chip follows the four-condition authority matrix", async () => {
    const fullOptions = (apply: boolean) => ({
      runsPages: [runsPageDocument(["run-a"], "project-a", true)],
      projectsDoc: projectsDocument(apply),
      eventsPages: [ORACLE_EVENTS_PAGE()],
    });
    // all four authority conditions hold with project_apply true
    const available = makeWiringFetch(["run-a"], [{ revision: 39 }], fullOptions(true));
    const harnessA = await railBoot(available, null, ["run-a"], true);
    harnessA.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harnessA.railCapabilityText()).toBe("Project apply: available");
    expect(harnessA.railCapabilityRendered()).toBe(true);
    expect(available.posts).toEqual([]);
    harnessA.dispose();
    // all four authority conditions hold with project_apply false
    const unavailable = makeWiringFetch(["run-a"], [{ revision: 39 }], fullOptions(false));
    const harnessB = await railBoot(unavailable, null, ["run-a"], true);
    harnessB.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harnessB.railCapabilityText()).toBe("Project apply: Unavailable in effective configuration");
    expect(harnessB.railCapabilityRendered()).toBe(true);
    expect(unavailable.posts).toEqual([]);
    harnessB.dispose();
    // the discovery model never arrived: no claim at all
    const undiscovered = makeWiringFetch(["run-a"], [{ revision: 39 }], { eventsPages: [ORACLE_EVENTS_PAGE()] });
    const harnessC = await railBoot(undiscovered, null, ["run-a"], false);
    harnessC.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harnessC.railCapabilityText()).toBe("");
    expect(harnessC.railCapabilityRendered()).toBe(false);
    harnessC.dispose();
  });

  test("w3. an authority loss removes the claim and never touches the identity line", async () => {
    // the project_available flip on the selected run's binding
    const probe = makeWiringFetch(["run-a"], [{ revision: 39 }], {
      runsPages: [runsPageDocument(["run-a"], "project-a", true), runsPageDocument(["run-a"], "project-a", false)],
      projectsDoc: projectsDocument(true),
      eventsPages: [ORACLE_EVENTS_PAGE()],
    });
    const harness = await railBoot(probe, null, ["run-a"], true);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harness.railCapabilityText()).toBe("Project apply: available");
    expect(harness.railCapabilityRendered()).toBe(true);
    const identityBefore = harness.railIdentityText();
    harness.click("refresh-runs");
    await Bun.sleep(5);
    // the claim is removed with zero residual text while the identity
    // line stays byte-identical across the same transition
    expect(harness.railCapabilityText()).toBe("");
    expect(harness.railCapabilityRendered()).toBe(false);
    expect(harness.railIdentityText()).toBe(identityBefore);
    // a fresh discovery application (the stale matching entry) never
    // resurrects the claim against the unavailable binding
    harness.click("refresh-projects");
    await Bun.sleep(5);
    expect(harness.railCapabilityText()).toBe("");
    expect(harness.railCapabilityRendered()).toBe(false);
    expect(harness.railIdentityText()).toBe(identityBefore);
    expect(probe.posts).toEqual([]);
    harness.dispose();
    // the same authority loss through the discovery side (the project
    // id disappears from the applied discovery model)
    const options2: Parameters<typeof makeWiringFetch>[2] = {
      runsPages: [runsPageDocument(["run-a"], "project-a", true)],
      projectsDoc: projectsDocument(true),
      eventsPages: [ORACLE_EVENTS_PAGE()],
    };
    const probe2 = makeWiringFetch(["run-a"], [{ revision: 39 }], options2);
    const harness2 = await railBoot(probe2, null, ["run-a"], true);
    harness2.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harness2.railCapabilityText()).toBe("Project apply: available");
    options2.projectsDoc = {
      schema_version: 2,
      kind: "projects",
      projects: [{ id: "project-b", input_roots: [{ id: "tasks" }], project_apply: true }],
    };
    harness2.click("refresh-projects");
    await Bun.sleep(5);
    expect(harness2.railCapabilityText()).toBe("");
    expect(harness2.railCapabilityRendered()).toBe(false);
    expect(harness2.railIdentityText()).toBe(identityBefore);
    harness2.dispose();
  });

  test("w3. the chip refreshes after renderProjects from the absent to the available claim", async () => {
    const options: Parameters<typeof makeWiringFetch>[2] = {
      runsPages: [runsPageDocument(["run-a"], "project-a", true)],
      projectsDoc: projectsDocument(false),
      eventsPages: [ORACLE_EVENTS_PAGE()],
    };
    const probe = makeWiringFetch(["run-a"], [{ revision: 39 }], options);
    const harness = await railBoot(probe, null, ["run-a"], true);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harness.railCapabilityText()).toBe("Project apply: Unavailable in effective configuration");
    options.projectsDoc = projectsDocument(true);
    harness.click("refresh-projects");
    await Bun.sleep(5);
    expect(harness.railCapabilityText()).toBe("Project apply: available");
    expect(probe.posts).toEqual([]);
    harness.dispose();
  });

  test("w4+w5. zero POST on every rail site and the run list keeps its exact shape", async () => {
    const probe = makeWiringFetch(["run-a", "run-b"], [{ revision: 39 }], {
      runsPages: [runsPageDocument(["run-a", "run-b"], "project-a", true)],
      projectsDoc: projectsDocument(true),
      eventsPages: [ORACLE_EVENTS_PAGE()],
    });
    const harness = await railBoot(probe, null, ["run-a", "run-b"], true);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(harness.railIdentityText()).toBe("Generation 1 · Iteration 2 · State dev_entry");
    harness.click("refresh-runs");
    await Bun.sleep(5);
    expect(harness.railIdentityText()).toBe("Generation 1 · Iteration 2 · State dev_entry");
    harness.click("refresh-projects");
    await Bun.sleep(5);
    expect(harness.railIdentityText()).toBe("Generation 1 · Iteration 2 · State dev_entry");
    // no mutating request ever left the wire for the rail
    expect(probe.posts).toEqual([]);
    // the run list keeps its exact current shape: four cells
    // (Run / Project / Project available / actions) and no status column
    for (const row of harness.element("runs-body").children) {
      expect(row.children.length).toBe(4);
    }
    expect(harness.element("runs-body").children[1]?.children[2]?.textContent).toBe("yes");
    harness.dispose();
  });
});
