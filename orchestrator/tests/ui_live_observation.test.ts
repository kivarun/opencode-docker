/**
 * Permanent suite of the browser UI v3 bounded read-only live
 * observation: the pure scheduling state machine, the single timer
 * adapter under a fake clock, the pure presentation helpers and the
 * source restrictions of the module. The HTTP/lifecycle side of live
 * observation lives in `control_plane_ui_live.test.ts`.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  LIVE_OBSERVATION_POLL_DELAY_MS,
  UiContractError,
  createLiveObservationScheduler,
  createSchedulerTimerAdapter,
  liveObservationDocumentKey,
  livePollAnnouncement,
  liveStateLabel,
  parseApiDocument,
  type UiLiveObservationScheduler,
  type UiObservationEffect,
  type UiRunModel,
  type UiSchedulerTimers,
} from "../ui/app.js";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

function snap(status: string, boundaryKind: string, revision: number) {
  return { status, boundaryKind, revision };
}

function effectKinds(effects: readonly UiObservationEffect[]): string[] {
  return effects.map((effect) => effect.effect);
}

function runModel(runId: string, revision: number, status: string, boundary: unknown): UiRunModel {
  return parseApiDocument({
    schema_version: 1,
    kind: "run",
    run_id: runId,
    revision,
    status,
    boundary,
    outputs: [],
  }) as UiRunModel;
}

describe("ui v3: the live observation state machine", () => {
  test("m1. an eligible in_flight boundary schedules exactly one timer", () => {
    const machine = createLiveObservationScheduler();
    expect(machine.runSelected("run-a")).toEqual({ effect: "render_state" });
    expect(machine.inspectStarted("run-a")).toEqual({ effect: "no_op" });
    const effect = machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(effect).toEqual({ effect: "schedule_timer", delayMs: LIVE_OBSERVATION_POLL_DELAY_MS });
    expect(LIVE_OBSERVATION_POLL_DELAY_MS).toBe(2000);
    expect(machine.presentationState()).toBe("live");
  });

  test("m2. the timer fires into exactly one GET; a stale timer never fires twice", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.timerFired()).toEqual({ effect: "start_inspect" });
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
  });

  test("m3. no timer exists before the inspect completes", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    expect(machine.inspectStarted("run-a")).toEqual({ effect: "no_op" });
    expect(machine.presentationState()).toBe("off");
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)).effect).toBe("schedule_timer");
  });

  test("m4. every successful in_flight inspect rearms the next timer", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)).effect).toBe("schedule_timer");
    machine.timerFired();
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)).effect).toBe("schedule_timer");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)).effect).toBe("schedule_timer");
  });

  test("m5. a revision change is applied through the new snapshot", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("waiting", "waiting", 5));
    expect(machine.presentationState()).toBe("off");
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 6)).effect).toBe("schedule_timer");
    expect(machine.presentationState()).toBe("live");
  });

  test("m6. every non-in_flight boundary stops the observation", () => {
    for (const [status, boundaryKind] of [
      ["running", "resumable"],
      ["running", "plan_ready"],
      ["waiting", "waiting"],
      ["succeeded", "succeeded"],
      ["failed", "failed"],
    ] as const) {
      const machine = createLiveObservationScheduler();
      machine.runSelected("run-a");
      machine.inspectStarted("run-a");
      machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
      expect(machine.inspectSucceeded("run-a", snap(status, boundaryKind, 6))).toEqual({ effect: "cancel_timer" });
      expect(machine.presentationState()).toBe("off");
      // from a stopped state the same boundary never schedules
      machine.inspectStarted("run-a");
      expect(machine.inspectSucceeded("run-a", snap(status, boundaryKind, 7)).effect).toBe("render_state");
    }
  });

  test("m7. an inspect failure stops the observation without backoff or retry", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.inspectFailed("run-a")).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("stopped");
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    machine.inspectStarted("run-a");
    expect(machine.inspectFailed("run-a")).toEqual({ effect: "render_state" });
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
  });

  test("m8. a manual refresh after an error resumes only on a confirmed in_flight inspect", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    machine.inspectFailed("run-a");
    expect(machine.admitInspect("run-a")).toBe(true);
    expect(machine.inspectStarted("run-a")).toEqual({ effect: "no_op" });
    expect(machine.inspectSucceeded("run-a", snap("waiting", "waiting", 6)).effect).toBe("render_state");
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 7)).effect).toBe("schedule_timer");
  });

  test("m9. disabling live updates cancels the timer; enabling runs one inspect", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.liveToggleChanged(false)).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("paused");
    expect(machine.liveToggleChanged(true)).toEqual({ effect: "start_inspect" });
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 6)).effect).toBe("schedule_timer");
    // enabling on a non-eligible boundary never starts anything
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("waiting", "waiting", 7));
    expect(machine.liveToggleChanged(false)).toEqual({ effect: "render_state" });
    expect(machine.liveToggleChanged(true)).toEqual({ effect: "render_state" });
  });

  test("m10. hidden cancels the timer; visible resumes once only on an eligible boundary", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.visibilityChanged(false)).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("paused");
    expect(machine.visibilityChanged(true)).toEqual({ effect: "start_inspect" });
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 6)).effect).toBe("schedule_timer");
    // hidden while a non-eligible boundary is applied: no timer to cancel
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("waiting", "waiting", 7));
    expect(machine.visibilityChanged(false)).toEqual({ effect: "render_state" });
    expect(machine.visibilityChanged(true)).toEqual({ effect: "render_state" });
    // visible never resumes after an error
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 8));
    machine.inspectFailed("run-a");
    expect(machine.visibilityChanged(false)).toEqual({ effect: "render_state" });
    expect(machine.visibilityChanged(true)).toEqual({ effect: "render_state" });
  });

  test("m11. selecting another run cancels the timer and drops the stale response", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.runSelected("run-b")).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("off");
    expect(machine.admitInspect("run-a")).toBe(false);
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 6))).toEqual({ effect: "no_op" });
    expect(machine.inspectStarted("run-a")).toEqual({ effect: "no_op" });
    expect(machine.inspectFailed("run-a")).toEqual({ effect: "no_op" });
    expect(machine.inspectSucceeded("run-b", snap("running", "in_flight", 6)).effect).toBe("schedule_timer");
  });

  test("m12. dispose cancels the timer and every later event is inert", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.disposed()).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("off");
    expect(machine.admitInspect("run-a")).toBe(false);
    for (const effect of [
      machine.runSelected("run-a"),
      machine.inspectStarted("run-a"),
      machine.inspectSucceeded("run-a", snap("running", "in_flight", 6)),
      machine.inspectFailed("run-a"),
      machine.inspectCancelled("run-a"),
      machine.timerFired(),
      machine.visibilityChanged(false),
      machine.visibilityChanged(true),
      machine.liveToggleChanged(false),
      machine.liveToggleChanged(true),
      machine.mutationStarted(),
      machine.mutationCompleted(),
      machine.disposed(),
    ]) {
      expect(effect).toEqual({ effect: "no_op" });
    }
  });

  test("m13. a mutation pauses the observation; completion re-evaluates the boundary", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.mutationStarted()).toEqual({ effect: "cancel_timer" });
    expect(machine.presentationState()).toBe("paused");
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    // the authoritative post-mutation inspect applies but never schedules
    // the authoritative post-mutation inspect applies but never schedules
    machine.inspectStarted("run-a");
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 6)).effect).toBe("render_state");
    expect(machine.mutationCompleted().effect).toBe("schedule_timer");
    // a mutation that ends on another boundary never schedules
    machine.mutationStarted();
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("waiting", "waiting", 7));
    expect(machine.mutationCompleted()).toEqual({ effect: "render_state" });
  });

  test("m14. run_busy is inspect-only: the mutation is never retried by the observation", () => {
    const machine = createLiveObservationScheduler();
    const effects: UiObservationEffect[] = [];
    const apply = (effect: UiObservationEffect) => {
      effects.push(effect);
    };
    apply(machine.runSelected("run-a"));
    apply(machine.inspectStarted("run-a"));
    // the selected run is waiting: the observation is stopped before the mutation
    apply(machine.inspectSucceeded("run-a", snap("waiting", "waiting", 5)));
    apply(machine.mutationStarted());
    // the busy POST failed; the prescribed read-only inspect observes in_flight
    apply(machine.inspectStarted("run-a"));
    apply(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)));
    expect(effectKinds(effects)).not.toContain("schedule_timer");
    apply(machine.mutationCompleted());
    expect(effectKinds(effects)).toContain("schedule_timer");
    apply(machine.timerFired());
    // the closed effect vocabulary: no scheduler transition ever produces a mutation
    for (const effect of effects) {
      expect(["no_op", "cancel_timer", "start_inspect", "render_state", "schedule_timer"]).toContain(effect.effect);
    }
  });

  test("m15. a lost response stops on failure and continues GET-only on in_flight", () => {
    // lost fresh run before the binding exists: one inspect, then stop
    const fresh = createLiveObservationScheduler();
    fresh.runSelected("run-a");
    fresh.mutationStarted();
    expect(fresh.inspectFailed("run-a")).toEqual({ effect: "render_state" });
    expect(fresh.mutationCompleted()).toEqual({ effect: "render_state" });
    expect(fresh.timerFired()).toEqual({ effect: "no_op" });
    // lost resume/continue/revise with a confirmed in_flight inspect: GET-only polling
    const live = createLiveObservationScheduler();
    live.runSelected("run-a");
    live.mutationStarted();
    live.inspectStarted("run-a");
    live.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(live.mutationCompleted().effect).toBe("schedule_timer");
    live.timerFired();
    live.inspectStarted("run-a");
    expect(live.inspectSucceeded("run-a", snap("running", "in_flight", 6)).effect).toBe("schedule_timer");
    // the reached waiting boundary stops the polling (the timer already fired)
    live.timerFired();
    live.inspectStarted("run-a");
    expect(live.inspectSucceeded("run-a", snap("waiting", "waiting", 7))).toEqual({ effect: "render_state" });
    expect(live.timerFired()).toEqual({ effect: "no_op" });
  });

  test("m16. the single-flight owner admits exactly one inspect at a time", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    expect(machine.admitInspect("run-a")).toBe(true);
    machine.inspectStarted("run-a");
    expect(machine.admitInspect("run-a")).toBe(false);
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.admitInspect("run-a")).toBe(true);
    machine.inspectStarted("run-a");
    expect(machine.admitInspect("run-a")).toBe(false);
    machine.inspectFailed("run-a");
    expect(machine.admitInspect("run-a")).toBe(true);
    machine.inspectStarted("run-a");
    machine.inspectCancelled("run-a");
    expect(machine.admitInspect("run-a")).toBe(true);
    expect(machine.admitInspect("run-b")).toBe(false);
  });

  test("m16b. a manual inspect takes the single-flight slot over and drops the superseded response", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    expect(machine.admitInspect("run-a")).toBe(false);
    // the user clicks Inspect while the scheduled GET is outstanding
    expect(machine.takeOverInspect("run-a")).toEqual({ effect: "no_op" });
    expect(machine.admitInspect("run-a")).toBe(false);
    // the superseded response is dropped silently by the token
    // discipline and never releases the slot; only the takeover's own
    // inspect completion does
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.admitInspect("run-a")).toBe(true);
    // takeover also cancels a pending timer (manual refresh semantics)
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 6));
    expect(machine.takeOverInspect("run-a")).toEqual({ effect: "cancel_timer" });
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    expect(machine.inspectSucceeded("run-a", snap("running", "in_flight", 7)).effect).toBe("schedule_timer");
    // takeover for another run is refused
    expect(machine.takeOverInspect("run-b")).toEqual({ effect: "no_op" });
  });

  test("m17. a slow GET never queues a second one and never shortens the delay", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    // the timer fires and the adapter starts the scheduled GET
    expect(machine.timerFired()).toEqual({ effect: "start_inspect" });
    machine.inspectStarted("run-a");
    expect(machine.admitInspect("run-a")).toBe(false);
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    // the slow GET completes: the next period is a full 2000 ms from completion
    const effect = machine.inspectSucceeded("run-a", snap("running", "in_flight", 6));
    expect(effect).toEqual({ effect: "schedule_timer", delayMs: 2000 });
  });

  test("m18. the presentation state matrix", () => {
    const machine = createLiveObservationScheduler();
    expect(machine.presentationState()).toBe("off");
    machine.runSelected("run-a");
    expect(machine.presentationState()).toBe("off");
    machine.inspectStarted("run-a");
    machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(machine.presentationState()).toBe("live");
    machine.visibilityChanged(false);
    expect(machine.presentationState()).toBe("paused");
    machine.visibilityChanged(true);
    machine.inspectStarted("run-a");
    expect(machine.presentationState()).toBe("live");
    machine.inspectSucceeded("run-a", snap("waiting", "waiting", 6));
    expect(machine.presentationState()).toBe("off");
    machine.inspectStarted("run-a");
    machine.inspectFailed("run-a");
    expect(machine.presentationState()).toBe("stopped");
    machine.disposed();
    expect(machine.presentationState()).toBe("off");
  });

  test("m19. every effect is a frozen declarative value", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    for (const effect of [
      machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)),
      machine.inspectFailed("run-a"),
      machine.runSelected("run-b"),
      machine.disposed(),
    ]) {
      expect(Object.isFrozen(effect)).toBe(true);
    }
  });
});

describe("ui v3: the single timer adapter under a fake clock", () => {
  function fakeClock() {
    let now = 0;
    let pending: { fireAt: number; callback: () => void; id: number } | null = null;
    let fired = 0;
    let nextId = 1;
    const timers = {
      setTimeout(callback: () => void, delayMs: number) {
        if (pending !== null) {
          throw new Error("second pending timer");
        }
        pending = { fireAt: now + delayMs, callback, id: nextId };
        return nextId++;
      },
      clearTimeout(handle: unknown) {
        if (pending !== null && pending.id === handle) {
          pending = null;
        }
      },
    };
    return {
      timers,
      advance(ms: number) {
        now += ms;
        if (pending !== null && pending.fireAt <= now) {
          const callback = pending.callback;
          pending = null;
          fired += 1;
          callback();
        }
      },
      pendingIsScheduled: () => pending !== null,
      firedCount: () => fired,
    };
  }

  function liveMachine(): {
    machine: UiLiveObservationScheduler;
    clock: ReturnType<typeof fakeClock>;
    adapter: ReturnType<typeof createSchedulerTimerAdapter>;
    apply: (effect: UiObservationEffect) => void;
    applied: UiObservationEffect[];
  } {
    const clock = fakeClock();
    const machine = createLiveObservationScheduler();
    const applied: UiObservationEffect[] = [];
    const apply = (effect: UiObservationEffect) => {
      applied.push(effect);
      adapter.apply(effect);
    };
    const adapter = createSchedulerTimerAdapter(clock.timers, () => {
      apply(machine.timerFired());
    });
    apply(machine.runSelected("run-a"));
    apply(machine.inspectStarted("run-a"));
    apply(machine.inspectSucceeded("run-a", snap("running", "in_flight", 5)));
    return { machine, clock, adapter, apply, applied };
  }

  test("t1. the exact 2000 ms period starts after the inspect completes", () => {
    const harness = liveMachine();
    expect(harness.clock.pendingIsScheduled()).toBe(true);
    harness.clock.advance(1999);
    expect(harness.clock.firedCount()).toBe(0);
    harness.clock.advance(1);
    expect(harness.clock.firedCount()).toBe(1);
    expect(harness.applied.at(-1)).toEqual({ effect: "start_inspect" });
  });

  test("t2. the delay is always the full period, never shortened by a slow GET", () => {
    const machine = createLiveObservationScheduler();
    machine.runSelected("run-a");
    machine.inspectStarted("run-a");
    // a long-running GET is simulated by simply not completing it; the
    // machine schedules nothing until the completion event arrives
    expect(machine.timerFired()).toEqual({ effect: "no_op" });
    const effect = machine.inspectSucceeded("run-a", snap("running", "in_flight", 5));
    expect(effect).toEqual({ effect: "schedule_timer", delayMs: 2000 });
  });

  test("t3. a repeated schedule replaces the single pending timer", () => {
    const clock = fakeClock();
    let fired = 0;
    const adapter = createSchedulerTimerAdapter(clock.timers, () => {
      fired += 1;
    });
    adapter.apply({ effect: "schedule_timer", delayMs: 2000 });
    adapter.apply({ effect: "schedule_timer", delayMs: 2000 });
    expect(clock.pendingIsScheduled()).toBe(true);
    clock.advance(2000);
    expect(fired).toBe(1);
    expect(adapter.hasPendingTimer()).toBe(false);
  });

  test("t4. a cancelled timer never executes its effect", () => {
    const clock = fakeClock();
    let fired = 0;
    const adapter = createSchedulerTimerAdapter(clock.timers, () => {
      fired += 1;
    });
    adapter.apply({ effect: "schedule_timer", delayMs: 2000 });
    adapter.apply({ effect: "cancel_timer" });
    expect(adapter.hasPendingTimer()).toBe(false);
    clock.advance(10000);
    expect(fired).toBe(0);
    adapter.apply({ effect: "cancel_timer" });
    adapter.apply({ effect: "no_op" });
    adapter.apply({ effect: "render_state" });
    adapter.apply({ effect: "start_inspect" });
  });

  test("t5. invalid timers or effects are contract errors", () => {
    expect(() => createSchedulerTimerAdapter({} as unknown as UiSchedulerTimers, () => undefined)).toThrow(UiContractError);
    expect(() => createSchedulerTimerAdapter({ setTimeout: () => 1, clearTimeout: () => undefined }, null as never)).toThrow(UiContractError);
    const clock = fakeClock();
    const adapter = createSchedulerTimerAdapter(clock.timers, () => undefined);
    expect(() => adapter.apply({ effect: "schedule_timer", delayMs: 0 })).toThrow(UiContractError);
    expect(() => adapter.apply({ effect: "unknown" })).toThrow(UiContractError);
    expect(() => adapter.apply(null)).toThrow(UiContractError);
  });
});

describe("ui v3: the presentation helpers", () => {
  test("p1. the document key separates runs, revisions, boundaries and outputs", () => {
    const model = runModel("run-a", 5, "running", { kind: "in_flight" });
    const key = liveObservationDocumentKey(model);
    expect(liveObservationDocumentKey(runModel("run-a", 5, "running", { kind: "in_flight" }))).toBe(key);
    expect(liveObservationDocumentKey(runModel("run-a", 6, "running", { kind: "in_flight" }))).not.toBe(key);
    expect(liveObservationDocumentKey(runModel("run-a", 5, "waiting", { kind: "waiting", waitIndex: 1, reason: "r", actions: [] }))).not.toBe(key);
    expect(liveObservationDocumentKey(runModel("run-b", 5, "running", { kind: "in_flight" }))).not.toBe(key);
    const withOutputs = parseApiDocument({
      schema_version: 1,
      kind: "run",
      run_id: "run-a",
      revision: 5,
      status: "running",
      boundary: { kind: "in_flight" },
      outputs: [{ id: "report", type: "file", required: true, present: true, digest: "a".repeat(64) }],
    }) as UiRunModel;
    expect(liveObservationDocumentKey(withOutputs)).not.toBe(key);
    expect(() => liveObservationDocumentKey({ kind: "error" })).toThrow(UiContractError);
  });

  test("p2. an unchanged poll is silent; a changed poll announces once", () => {
    const model = runModel("run-a", 5, "running", { kind: "in_flight" });
    expect(livePollAnnouncement(liveObservationDocumentKey(model), model)).toBeNull();
    expect(livePollAnnouncement(null, model)).toBe(`Run run-a updated (revision 5).`);
    expect(livePollAnnouncement("other", model)).toBe(`Run run-a updated (revision 5).`);
  });

  test("p3. the live state labels", () => {
    expect(liveStateLabel("live")).toBe("Live");
    expect(liveStateLabel("paused")).toBe("Paused");
    expect(liveStateLabel("stopped")).toBe("Stopped after error");
    expect(liveStateLabel("off")).toBe("off");
    expect(liveStateLabel("unknown")).toBe("off");
  });
});

describe("ui v3: source restrictions of the live observation", () => {
  function observationRegion(): string {
    const start = appSource.indexOf("Live observation (v3)");
    const end = appSource.indexOf("Thin DOM adapter");
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    return appSource.slice(start, end);
  }

  test("s1. the module keeps the closed scheduling vocabulary and one owner", () => {
    expect(appSource.includes("setInterval")).toBe(false);
    expect(appSource.split("createLiveObservationScheduler(").length - 1).toBe(2);
    expect(appSource.split("timers.setTimeout(").length - 1).toBe(1);
    // both clear sites live inside the one adapter (replace-on-schedule and cancel)
    expect(appSource.split("timers.clearTimeout(").length - 1).toBe(2);
    expect(appSource.split("visibilitychange").length - 1).toBe(1);
    expect(appSource.split('id="live-updates"').length - 1).toBe(0);
    expect(appSource.includes('getElementById("live-updates")')).toBe(true);
    // the established single owners are unchanged
    expect(appSource.split("export async function requestApiJson").length - 1).toBe(1);
    expect(appSource.split("function buildRunActionModel").length - 1).toBe(1);
    expect(appSource.split("function buildMutationRequest").length - 1).toBe(1);
    expect(appSource.split("async function postMutation").length - 1).toBe(1);
    // the one inspect transport admission owner: one declaration, one
    // module wiring, one abort-controller site, one fetch start
    expect(appSource.split("export function createInspectTransport(").length - 1).toBe(1);
    expect(appSource.split("createInspectTransport({").length - 1).toBe(1);
    // four abort-controller sites: the inspect, events, logs and
    // project-changes transports each own their request's abort controller
    expect(appSource.split("new AbortController(").length - 1).toBe(4);
    expect(appSource.split("await fetchInspect(").length - 1).toBe(1);
    expect(appSource.split("fetchInspect:").length - 1).toBe(1);
    // the inspect GET path is constructed only for the transport and the
    // lost-response recovery description
    expect(appSource.split("`/v1/runs/${runId}`").length - 1).toBe(2);
    // the incidental promise-chain single flight and the token
    // machinery are gone; supersede is the transport's own state
    for (const gone of ["enqueueInspect", "inspectChain", "beginInspectToken", "activeInspectToken", "invalidateActiveInspectToken"]) {
      expect(appSource.includes(gone)).toBe(false);
    }
  });

  test("s2. the scheduler region contains no mutation, transport or persistence capability", () => {
    const region = observationRegion();
    for (const banned of [
      "setInterval",
      '"POST"',
      "`POST`",
      "/resume",
      "/continue-stage",
      "/revise-task",
      "/resume-plan",
      "freshRunPath",
      "fetch(",
      "await fetchImpl",
      "backoff",
      "localStorage",
      "sessionStorage",
      "document.cookie",
      "runRoot",
      "state.json",
      "stderr",
      "import ",
      "require(",
    ]) {
      expect(region.includes(banned)).toBe(false);
    }
  });

  test("s3. the scheduled poll path delegates to the one transport owner", () => {
    const start = appSource.indexOf("function scheduledInspect");
    const end = appSource.indexOf("function inspectRun");
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    const region = appSource.slice(start, end);
    expect(region.includes("inspectTransport.request(")).toBe(true);
    for (const banned of ["method", "requestApiJson", "/v1/runs/", "fetch", "admitInspect"]) {
      expect(region.includes(banned)).toBe(false);
    }
  });

  test("s4. the transport region owns admission without queues, timers or retries", () => {
    const start = appSource.indexOf("Inspect transport: the one single-flight admission owner");
    // the worker-logs section (v5) sits between the inspect transport and
    // the live-observation region; the region ends at its header
    const end = appSource.indexOf("Worker logs (v5)");
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    const region = appSource.slice(start, end);
    for (const banned of [
      "setTimeout",
      "setInterval",
      "clearTimeout",
      ".push(",
      "new Map(",
      "new Set(",
      '"POST"',
      "`POST`",
      "retry",
      "backoff",
      "while (",
      "for (",
      ".then(",
      "await fetchImpl",
      "import ",
      "require(",
    ]) {
      expect(region.includes(banned)).toBe(false);
    }
    // supersede aborts at most the three owner sites (busy desire,
    // invalidate, dispose); the abort never frees the slot itself
    expect(region.split(".abort()").length - 1).toBe(3);
    // the slot is released exactly once, from the single settlement owner:
    // the slot variable is declared once and nulled only there
    expect(region.split("let active = null").length - 1).toBe(1);
    expect(region.split("active = null").length - 1).toBe(2);
    expect(region.split("const startRequest = (request)").length - 1).toBe(1);
    expect(region.split("startRequest(").length - 1).toBe(2);
  });
});
