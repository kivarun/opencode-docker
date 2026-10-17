/**
 * Permanent wiring suite of the browser UI v4 lifecycle timeline: the
 * fake-DOM harness boots the shipped `app.js` and drives the production
 * call chains (selection → inspect + initial events page, applied
 * inspects → at most one tail, Load more / Reload / Retry actions) with
 * an instrumented fetch. Every test proves the cursor contract, the
 * fail-closed error isolation and the never-applied stale response on
 * the real module wiring.
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

interface RunPlan {
  readonly revision: number;
  readonly status?: string;
  readonly boundary?: Record<string, unknown>;
  readonly hold?: boolean;
}

interface PagePlan {
  readonly revision: number;
  readonly kinds?: readonly string[];
  readonly nextAfter?: number | null;
  readonly startRevision?: number;
  readonly hold?: boolean;
  readonly fail?: "network" | "invalid_cursor" | "not_found" | "contract";
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

interface Probe {
  readonly fetchImpl: (path: string, init?: RequestInit) => Promise<Response>;
  readonly eventsGets: { path: string; after: number }[];
  readonly posts: string[];
  callsCount(): number;
  eventsCount(): number;
  maxActive(): number;
  releaseInspects(): void;
  releaseEvents(): void;
}

function makeProbe(
  runs: readonly string[],
  inspects: readonly RunPlan[],
  pages: readonly PagePlan[],
  options: { postFails?: boolean; postResults?: Record<string, Record<string, unknown>> } = {},
): Probe {
  let inspectSeq = 0;
  let eventsSeq = 0;
  let active = 0;
  let maxActive = 0;
  const inspectGates: (() => void)[] = [];
  const eventsGates: (() => void)[] = [];
  const eventsGets: { path: string; after: number }[] = [];
  const posts: string[] = [];
  const fetchImpl = async (path: string, init?: RequestInit) => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      posts.push(path);
      if (options.postFails === true) {
        throw new Error("network gone");
      }
      return jsonResponse(options.postResults?.[path] ?? {
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome: "succeeded",
        exit_code: 0,
      });
    }
    const inspect = /^\/v1\/runs\/([^/?]+)$/.exec(path);
    if (inspect !== null) {
      inspectSeq += 1;
      const plan = inspects[inspectSeq - 1] ?? { revision: inspectSeq + 8 };
      active += 1;
      if (active > maxActive) maxActive = active;
      try {
        if (plan.hold === true) {
          await new Promise<void>((resolve) => {
            inspectGates.push(resolve);
            init?.signal?.addEventListener("abort", () => resolve());
          });
        }
        return jsonResponse(
          runDocument(inspect[1] as string, plan.revision, plan.status ?? "running", plan.boundary ?? { kind: "in_flight" }),
        );
      } finally {
        active -= 1;
      }
    }
    const events = /^\/v1\/runs\/([^/?]+)\/events\?after=(\d+)&limit=(\d+)$/.exec(path);
    if (events !== null) {
      eventsSeq += 1;
      eventsGets.push({ path, after: Number(events[2]) });
      const plan = pages[eventsSeq - 1] ?? { revision: 1, kinds: ["create_run"], nextAfter: null };
      active += 1;
      if (active > maxActive) maxActive = active;
      try {
        if (plan.hold === true) {
          await new Promise<void>((resolve) => {
            eventsGates.push(resolve);
            init?.signal?.addEventListener("abort", () => resolve());
          });
        }
        if (plan.fail === "network") {
          throw new Error("network gone");
        }
        if (plan.fail === "invalid_cursor") {
          return new Response(JSON.stringify({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" }), {
            status: 400,
            headers: { "content-type": "application/json" },
          });
        }
        if (plan.fail === "not_found") {
          return jsonResponse({
            schema_version: 1,
            kind: "command_result",
            run_id: events[1],
            outcome: "failed",
            exit_code: 1,
            reason: "run_not_found",
          });
        }
        if (plan.fail === "contract") {
          return jsonResponse({ schema_version: 1, kind: "events", run_id: events[1], revision: 4, events: [{ revision: 2, kind: "create_run" }], next_after: null });
        }
        const kinds = plan.kinds ?? ["create_run"];
        const start = plan.startRevision ?? 1;
        return jsonResponse({
          schema_version: 1,
          kind: "events",
          run_id: events[1],
          revision: plan.revision,
          events: kinds.map((kind, index) => eventEntry(kind, start + index)),
          next_after: plan.nextAfter === undefined ? null : plan.nextAfter,
        });
      } finally {
        active -= 1;
      }
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
    eventsGets,
    posts,
    callsCount: () => inspectSeq,
    eventsCount: () => eventsSeq,
    maxActive: () => maxActive,
    releaseInspects: () => {
      for (const gate of inspectGates.splice(0)) gate();
    },
    releaseEvents: () => {
      for (const gate of eventsGates.splice(0)) gate();
    },
  };
}

function fakeClock(): UiHarnessClock {
  let now = 0;
  let pending: { fireAt: number; callback: () => void; id: number } | null = null;
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
      if (pending !== null && pending.fireAt <= now) {
        const callback = pending.callback;
        pending = null;
        callback();
        return 1;
      }
      return 0;
    },
    hasPending: () => pending !== null,
  };
}

async function boot(probe: Probe, clock: UiHarnessClock | null, runs: readonly string[]): Promise<UiHarness> {
  const harness = await createUiHarness(clock === null ? { fetchImpl: probe.fetchImpl } : { fetchImpl: probe.fetchImpl, clock });
  harness.click("refresh-runs");
  await Bun.sleep(2);
  expect(harness.runRows()).toEqual([...runs]);
  return harness;
}

const NINE_EVENTS = ["create_run", "start_agent_execution", "agent_data_prepared", "agent_execution_session_created", "agent_tool_session_created", "agent_running", "agent_outputs_accepted", "agent_cleanup_completed", "plan_revision_accepted"];

describe("ui events wiring: selection, tails and pagination on the real module", () => {
  test("w1. selection loads the first page and renders the caught-up timeline", async () => {
    const probe = makeProbe(["run-a"], [{ revision: 9 }], [{ revision: 9, kinds: NINE_EVENTS, nextAfter: null }]);
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(probe.eventsGets).toEqual([{ path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 }]);
    expect(harness.eventsRows().length).toBe(9);
    expect(harness.eventsRows()[0]).toBe("1|create_run|");
    expect(harness.eventsRows()[1]).toBe("2|start_agent_execution|execution_index=2, state_id=dev_entry");
    expect(harness.eventsMeta()).toBe("Detail revision: 9 · Events revision: 9 · Cursor: 9 · caught up");
    expect(harness.eventsButtonState("events-load-more")).toEqual({ hidden: true, disabled: false });
    expect(harness.eventsButtonState("events-retry")).toEqual({ hidden: true, disabled: false });
    expect(harness.eventsStatus()).toBe("Added 9 lifecycle events. Caught up.");
    expect(harness.element("events-section").hidden).toBe(false);
    harness.dispose();
  });

  test("w2. an applied inspect with a higher revision pulls exactly one tail", async () => {
    const clock = fakeClock();
    const probe = makeProbe(
      ["run-a"],
      [
        { revision: 9, hold: true },
        { revision: 11 },
        { revision: 11 },
      ],
      [
        { revision: 9, kinds: NINE_EVENTS, nextAfter: null },
        { revision: 11, kinds: ["stage_generation_opened", "stage_iteration_opened"], startRevision: 10, nextAfter: null },
      ],
    );
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(2);
    // the initial page applies while the inspect is still on the wire
    probe.releaseEvents();
    expect(probe.eventsGets.length).toBe(1);
    probe.releaseInspects();
    await Bun.sleep(3);
    // the applied inspect (revision 9) equals the cursor: no tail
    expect(probe.eventsGets.length).toBe(1);
    expect(clock.hasPending()).toBe(true);
    clock.advance(2000);
    await Bun.sleep(3);
    // the poll advanced the revision: exactly one tail at the cursor
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-a/events?after=9&limit=100", after: 9 },
    ]);
    expect(harness.eventsRows().length).toBe(11);
    expect(harness.eventsMeta()).toBe("Detail revision: 11 · Events revision: 11 · Cursor: 11 · caught up");
    // an unchanged poll pulls no further tail
    clock.advance(2000);
    await Bun.sleep(3);
    expect(probe.eventsGets.length).toBe(2);
    harness.dispose();
  });

  test("w3. a partial page never tails automatically and requires Load more", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2 },
        { revision: 4, kinds: NINE_EVENTS.slice(2, 4), startRevision: 3, nextAfter: null },
      ],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(probe.eventsGets.length).toBe(1);
    expect(harness.eventsRows().length).toBe(2);
    expect(harness.eventsMeta()).toBe("Detail revision: 4 · Events revision: 4 · Cursor: 2 · more available");
    expect(harness.eventsButtonState("events-load-more")).toEqual({ hidden: false, disabled: false });
    harness.click("events-load-more");
    await Bun.sleep(5);
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-a/events?after=2&limit=100", after: 2 },
    ]);
    expect(harness.eventsRows().length).toBe(4);
    expect(harness.eventsMeta()).toContain("caught up");
    harness.dispose();
  });

  test("w4. a double Load more during one GET never issues a second fetch", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2 },
        { revision: 4, kinds: NINE_EVENTS.slice(2, 4), startRevision: 3, nextAfter: null, hold: true },
      ],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    probe.releaseEvents();
    await Bun.sleep(3);
    harness.click("events-load-more");
    expect(probe.eventsCount()).toBe(2);
    harness.click("events-load-more");
    await Bun.sleep(2);
    expect(probe.eventsCount()).toBe(2);
    expect(harness.eventsButtonState("events-load-more").disabled).toBe(true);
    probe.releaseEvents();
    await Bun.sleep(3);
    expect(probe.eventsCount()).toBe(2);
    expect(harness.eventsButtonState("events-load-more")).toEqual({ hidden: true, disabled: false });
    harness.dispose();
  });

  test("w5. invalid_cursor stops fail-closed without retry; reload restarts", async () => {
    const clock = fakeClock();
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }, { revision: 6 }],
      [
        { revision: 2, kinds: NINE_EVENTS.slice(0, 2), nextAfter: null },
        { revision: 2, fail: "invalid_cursor" },
        { revision: 6, kinds: NINE_EVENTS.slice(0, 6), nextAfter: null },
      ],
    );
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    // the tail after the first inspect hits the invalid cursor
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-a/events?after=2&limit=100", after: 2 },
    ]);
    expect(harness.eventsStatus()).toBe("Lifecycle events error: request: invalid_cursor.");
    expect(harness.eventsMeta()).toContain("stopped");
    expect(harness.eventsButtonState("events-retry")).toEqual({ hidden: true, disabled: false });
    expect(harness.eventsButtonState("events-reload")).toEqual({ hidden: false, disabled: false });
    // a later poll applies but never resumes the automatic tail
    clock.advance(2000);
    await Bun.sleep(3);
    expect(probe.eventsGets.length).toBe(2);
    expect(harness.detailStatus()).toBe("running");
    expect(harness.controlsText()).not.toContain("contract");
    // the explicit reload restarts from the beginning
    harness.click("events-reload");
    await Bun.sleep(4);
    expect(probe.eventsGets.length).toBe(3);
    expect(probe.eventsGets[2]).toEqual({ path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 });
    expect(harness.eventsStatus()).toBe("Added 6 lifecycle events. Caught up.");
    harness.dispose();
  });

  test("w6. a network failure stops the timeline and Retry re-issues the same request", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2 },
        { revision: 4, fail: "network" },
        { revision: 4, kinds: NINE_EVENTS.slice(2, 4), startRevision: 3, nextAfter: null },
      ],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    harness.click("events-load-more");
    await Bun.sleep(3);
    expect(harness.eventsStatus()).toBe("Lifecycle events error: transport: network_error.");
    expect(harness.eventsButtonState("events-retry")).toEqual({ hidden: false, disabled: false });
    expect(harness.eventsRows().length).toBe(2);
    harness.click("events-retry");
    await Bun.sleep(3);
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-a/events?after=2&limit=100", after: 2 },
      { path: "/v1/runs/run-a/events?after=2&limit=100", after: 2 },
    ]);
    expect(harness.eventsRows().length).toBe(4);
    expect(harness.eventsStatus()).toBe("Added 2 lifecycle events. Caught up.");
    harness.dispose();
  });

  test("w7. a timeline error never POSTs and never breaks the inspect detail", async () => {
    const probe = makeProbe(["run-a"], [{ revision: 4 }], [{ revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2, fail: "not_found" }]);
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    expect(harness.eventsStatus()).toBe("Lifecycle events error: orchestrator: run_not_found.");
    expect(harness.detailStatus()).toBe("running");
    expect(harness.detailRunId()).toBe("Run run-a");
    expect(harness.element("run-controls").children.length).toBeGreaterThan(0);
    harness.click("events-retry");
    await Bun.sleep(3);
    expect(probe.posts).toEqual([]);
    harness.dispose();
  });

  test("w8. a selection change drops the stale events response of the previous run", async () => {
    const probe = makeProbe(
      ["run-a", "run-b"],
      [
        { revision: 9 },
        { revision: 5, hold: true },
      ],
      [
        { revision: 9, kinds: NINE_EVENTS, nextAfter: null, hold: true },
        { revision: 2, kinds: ["create_run", "start_agent_execution"], nextAfter: null },
      ],
    );
    const harness = await boot(probe, null, ["run-a", "run-b"]);
    harness.clickRunInspect(0);
    await Bun.sleep(3);
    expect(probe.eventsGets.length).toBe(1);
    // selecting run-b: its initial events request is refused while
    // run-a's held GET still owns the wire slot
    harness.clickRunInspect(1);
    await Bun.sleep(3);
    expect(probe.eventsGets.length).toBe(1);
    expect(harness.detailRunId()).toBe("Run run-b");
    // run-a's held response settles superseded and is never applied
    probe.releaseEvents();
    await Bun.sleep(2);
    // run-b's inspect applies and pulls the timeline as its tail
    probe.releaseInspects();
    await Bun.sleep(3);
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-b/events?after=0&limit=100", after: 0 },
    ]);
    expect(harness.eventsRows().length).toBe(2);
    expect(harness.eventsRows()[0]).toBe("1|create_run|");
    expect(harness.eventsMeta()).toBe("Detail revision: 5 · Events revision: 2 · Cursor: 2 · caught up");
    harness.dispose();
  });

  test("w9. timeline actions never create a timer and never POST", async () => {
    const clock = fakeClock();
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2 },
        { revision: 4, kinds: NINE_EVENTS.slice(2, 4), startRevision: 3, nextAfter: null },
        { revision: 4, kinds: NINE_EVENTS.slice(0, 4), nextAfter: null },
      ],
    );
    const harness = await boot(probe, clock, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    const pendingAfterInspect = clock.hasPending();
    harness.click("events-load-more");
    await Bun.sleep(3);
    harness.click("events-reload");
    await Bun.sleep(3);
    expect(clock.hasPending()).toBe(pendingAfterInspect);
    expect(probe.posts).toEqual([]);
    harness.dispose();
  });

  test("w10. a mutation recovery inspect pulls exactly one tail", async () => {
    const probe = makeProbe(
      ["run-a"],
      [
        { revision: 4, status: "running", boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 3 } },
        { revision: 6 },
      ],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 4), nextAfter: null },
        { revision: 6, kinds: ["agent_running", "agent_outputs_accepted"], startRevision: 5, nextAfter: null },
      ],
      {
        postResults: {
          "/v1/runs/run-a/resume": {
            schema_version: 1,
            kind: "command_result",
            run_id: "run-a",
            outcome: "succeeded",
            exit_code: 0,
          },
        },
      },
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    expect(harness.controlsText()).toContain("Resume run");
    harness.clickButton("run-controls", "Resume run");
    await Bun.sleep(6);
    expect(probe.posts).toEqual(["/v1/runs/run-a/resume"]);
    expect(probe.eventsGets).toEqual([
      { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
      { path: "/v1/runs/run-a/events?after=4&limit=100", after: 4 },
    ]);
    expect(harness.eventsRows().length).toBe(6);
    harness.dispose();
  });

  test("w11. an events snapshot may run ahead of the inspect without recursion", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [{ revision: 9, kinds: NINE_EVENTS, nextAfter: null }],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    // the events snapshot (revision 9) is newer than the inspect (4)
    expect(harness.eventsMeta()).toBe("Detail revision: 4 · Events revision: 9 · Cursor: 9 · caught up");
    expect(probe.callsCount()).toBe(1);
    // the newer snapshot triggered no inspect and no recursive tail
    await Bun.sleep(3);
    expect(probe.callsCount()).toBe(1);
    expect(probe.eventsCount()).toBe(1);
    harness.dispose();
  });

  test("w12. rows are appended without a full rebuild and buttons disable during a GET", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 4, kinds: NINE_EVENTS.slice(0, 2), nextAfter: 2 },
        { revision: 4, kinds: NINE_EVENTS.slice(2, 4), startRevision: 3, nextAfter: null, hold: true },
      ],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    const firstRows = harness.element("events-body").children.slice();
    expect(firstRows.length).toBe(2);
    harness.click("events-load-more");
    await Bun.sleep(2);
    expect(harness.eventsButtonState("events-reload").disabled).toBe(true);
    expect(harness.eventsButtonState("events-retry").disabled).toBe(true);
    probe.releaseEvents();
    await Bun.sleep(3);
    const rows = harness.element("events-body").children;
    expect(rows.length).toBe(4);
    expect(rows[0]).toBe(firstRows[0]);
    expect(rows[1]).toBe(firstRows[1]);
    expect(harness.eventsButtonState("events-load-more").disabled).toBe(false);
    harness.dispose();
  });

  test("w13. a contract-breaking page stops the timeline with the contract reason", async () => {
    const probe = makeProbe(
      ["run-a"],
      [{ revision: 4 }],
      [
        { revision: 2, kinds: NINE_EVENTS.slice(0, 2), nextAfter: null },
        { revision: 4, fail: "contract" },
      ],
    );
    const harness = await boot(probe, null, ["run-a"]);
    harness.clickRunInspect(0);
    await Bun.sleep(4);
    // the tail page repeats revision 2: a dense-merge contract error
    expect(harness.eventsStatus()).toBe("Lifecycle events error: contract: invalid_page.");
    expect(harness.eventsButtonState("events-retry")).toEqual({ hidden: false, disabled: false });
    expect(harness.eventsRows().length).toBe(2);
    harness.dispose();
  });
});

describe("ui events wiring: the stale-response stress battery", () => {
  test("x1. one hundred selection sequences never apply a stale events page", async () => {
    for (let sequence = 0; sequence < 100; sequence += 1) {
      const probe = makeProbe(
        ["run-a", "run-b"],
        [
          { revision: 9 },
          { revision: 5, hold: true },
        ],
        [
          { revision: 9, kinds: NINE_EVENTS, nextAfter: null, hold: true },
          { revision: 3, kinds: ["create_run", "start_agent_execution", "agent_data_prepared"], nextAfter: null },
        ],
      );
      const harness = await createUiHarness({ fetchImpl: probe.fetchImpl });
      harness.click("refresh-runs");
      await Bun.sleep(1);
      harness.clickRunInspect(0);
      await Bun.sleep(1);
      harness.clickRunInspect(1);
      await Bun.sleep(1);
      probe.releaseEvents();
      await Bun.sleep(1);
      probe.releaseInspects();
      await Bun.sleep(2);
      expect(probe.maxActive()).toBeLessThanOrEqual(2);
      // run-a's held page never applied; run-b's tail loaded its own page
      expect(harness.detailRunId()).toBe("Run run-b");
      expect(harness.eventsRows().length).toBe(3);
      expect(harness.eventsRows()[0]).toBe("1|create_run|");
      expect(probe.eventsGets).toEqual([
        { path: "/v1/runs/run-a/events?after=0&limit=100", after: 0 },
        { path: "/v1/runs/run-b/events?after=0&limit=100", after: 0 },
      ]);
      harness.dispose();
    }
  });
});
