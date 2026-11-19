/**
 * Permanent pure-model suite of the S1 run-rail identity derivation
 * (`deriveRunRailIdentity`) and the capability derivation
 * (`deriveRunRailCapability`) over the already-applied browser models.
 * The vectors run against the 66-event lifecycle oracle of the
 * preflight report (§5/§6.2): a dense applied journal prefix bound to
 * the applied inspect revision, an open stage generation, its open
 * iteration and the state id of the execution started inside that
 * iteration. Every gate failure and contradiction renders the one
 * fixed fallback string; nothing else about the models is consumed.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  applyEventsPage,
  createTimelineState,
  deriveRunRailCapability,
  deriveRunRailIdentity,
  eventReferenceKeys,
  parseEventsDocument,
  stopTimeline,
  type UiRunRailDerivation,
  type UiTimelineState,
} from "../ui/app.js";

const RUN_ID = "uat-inc5-2";

interface RefOverrides {
  readonly [key: string]: string | number;
}

/** One oracle event with the exact contract fields the production
 * parser enforces; `overrides` pins the identity-relevant references. */
function oracleEvent(kind: string, revision: number, overrides: RefOverrides = {}): Record<string, unknown> {
  const entry: Record<string, unknown> = { revision, kind };
  const keys = eventReferenceKeys(kind);
  if (keys === null) {
    throw new Error(`unknown kind ${kind}`);
  }
  for (const key of keys) {
    entry[key] =
      key === "state_id" ? "dev_entry" : key === "execution_index" ? 1 : key === "transition_index" ? 1 : revision;
  }
  for (const key of Object.keys(overrides)) {
    entry[key] = overrides[key];
  }
  return entry;
}

/** The identity-relevant passage of the 66-event oracle; every other
 * position is an identity-inert transition event. */
function oracleSpec(): { kind: string; overrides?: RefOverrides }[] {
  const inert = (executionIndex: number, transitionIndex: number): RefOverrides => ({
    execution_index: executionIndex,
    transition_index: transitionIndex,
  });
  const filler = inert(1, 1);
  return [
    { kind: "create_run" },
    { kind: "start_agent_execution", overrides: { execution_index: 1, state_id: "architect" } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "stage_generation_opened", overrides: { generation_index: 1 } },
    { kind: "stage_iteration_opened", overrides: { generation_index: 1, iteration_index: 1 } },
    { kind: "transition_committed", overrides: inert(1, 1) },
    { kind: "start_agent_execution", overrides: { execution_index: 2, state_id: "dev_entry" } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: inert(2, 2) },
    { kind: "run_waiting", overrides: { wait_index: 1 } },
    { kind: "transition_committed", overrides: filler },
    { kind: "iteration_grant_recorded", overrides: { grant_index: 1 } },
    { kind: "stage_iteration_closed", overrides: { generation_index: 1, iteration_index: 1 } },
    { kind: "wait_response_recorded", overrides: { wait_index: 1 } },
    { kind: "stage_iteration_opened", overrides: { generation_index: 1, iteration_index: 2 } },
    { kind: "start_agent_execution", overrides: { execution_index: 3, state_id: "dev_entry" } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: inert(3, 3) },
    { kind: "run_waiting", overrides: { wait_index: 2 } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "stage_iteration_closed", overrides: { generation_index: 1, iteration_index: 2 } },
    { kind: "wait_response_recorded", overrides: { wait_index: 2 } },
    { kind: "start_agent_execution", overrides: { execution_index: 4, state_id: "architect" } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "stage_generation_closed", overrides: { generation_index: 1 } },
    { kind: "stage_generation_opened", overrides: { generation_index: 2 } },
    { kind: "stage_iteration_opened", overrides: { generation_index: 2, iteration_index: 1 } },
    { kind: "transition_committed", overrides: inert(4, 4) },
    { kind: "start_agent_execution", overrides: { execution_index: 5, state_id: "dev_entry" } },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: filler },
    { kind: "transition_committed", overrides: inert(5, 5) },
    { kind: "terminal_reached" },
    { kind: "transition_committed", overrides: filler },
    { kind: "run_succeeded" },
  ];
}

/** A parsed oracle over the spec prefix `1..cursor`, one applied page
 * (page revision 66, `nextAfter` as requested). */
function oracleTimeline(cursor: number, nextAfter: number | null): UiTimelineState {
  const spec = oracleSpec();
  if (spec.length !== 66) {
    throw new Error(`oracle length drift: ${spec.length}`);
  }
  if (nextAfter !== null && nextAfter !== cursor) {
    throw new Error("partial pages must end at the requested cursor");
  }
  const entries = spec.slice(0, cursor).map((entry, index) => oracleEvent(entry.kind, index + 1, entry.overrides ?? {}));
  const page = parseEventsDocument(
    { schema_version: 1, kind: "events", run_id: RUN_ID, revision: nextAfter ?? cursor, next_after: nextAfter, events: entries },
    RUN_ID,
  );
  return applyEventsPage(createTimelineState(RUN_ID), page, 0);
}

/** A raw timeline-shaped state over a custom entry list (the
 * derivation consumes only the well-shaped frozen model fields). */
function rawTimeline(entries: Record<string, unknown>[], cursor: number, runId = RUN_ID): UiTimelineState {
  return Object.freeze({
    runId,
    cursor,
    snapshotRevision: cursor,
    events: Object.freeze(entries),
    nextAfter: null,
    stopped: false,
    error: null,
  } as UiTimelineState);
}

const RESOLVED_1_1: UiRunRailDerivation = {
  kind: "resolved",
  generation: 1,
  iteration: 1,
  state: "dev_entry",
};
const RESOLVED_1_2: UiRunRailDerivation = {
  kind: "resolved",
  generation: 1,
  iteration: 2,
  state: "dev_entry",
};
const RESOLVED_2_1: UiRunRailDerivation = {
  kind: "resolved",
  generation: 2,
  iteration: 1,
  state: "dev_entry",
};

describe("ui rail identity: timeline health (§3.2.3 item 2)", () => {
  test("r6. a stopped timeline falls back; a reset falls back with the zero-event label; a re-applied page re-resolves", () => {
    const timeline = oracleTimeline(66, null);
    expect(deriveRunRailIdentity(timeline, 39)).toEqual(RESOLVED_1_2);
    const stopped = stopTimeline(timeline, { layer: "transport", reason: "network_error" });
    const stoppedDerived = deriveRunRailIdentity(stopped, 39);
    expect(stoppedDerived.kind).toBe("fallback");
    if (stoppedDerived.kind !== "fallback") return;
    expect(stoppedDerived.label).toBe("identity: from journal (0 events)");
    // a RESET timeline keeps the exact byte-identity zero-event label
    const reset = createTimelineState(RUN_ID);
    const resetDerived = deriveRunRailIdentity(reset, null);
    if (resetDerived.kind !== "fallback") throw new Error("expected the reset fallback");
    expect(resetDerived.label).toBe("identity: from journal (0 events)");
    // a successful page application over the same prefix re-resolves
    const recovered = oracleTimeline(39, null);
    expect(deriveRunRailIdentity(recovered, 39)).toEqual(RESOLVED_1_2);
  });
});

describe("ui rail identity: revision binding (the dense prefix rule)", () => {
  test("r1. inspect revision 39 over a journal loaded to 66 resolves {1, 2, dev_entry}, never {2, 1}", () => {
    const timeline = oracleTimeline(66, null);
    const derived = deriveRunRailIdentity(timeline, 39);
    if (derived.kind !== "resolved") throw new Error("expected the resolved triple at revision 39");
    expect(derived).toEqual(RESOLVED_1_2);
    expect(derived.generation).toBe(1);
    expect(derived.iteration).toBe(2);
    expect(derived.state).toBe("dev_entry");
    expect(Object.is(derived.generation, 2)).toBe(false);
    expect(Object.is(derived.iteration, 1)).toBe(false);
  });

  test("r2. a contradiction after the inspect revision never poisons the prefix result", () => {
    const spec = oracleSpec();
    spec[49] = { kind: "stage_generation_opened", overrides: { generation_index: 1 } };
    const entries = spec.map((entry, index) => oracleEvent(entry.kind, index + 1, entry.overrides ?? {}));
    const timeline = rawTimeline(entries, 66);
    expect(deriveRunRailIdentity(timeline, 39)).toEqual(RESOLVED_1_2);
    // the same forged entry inside the prefix fails the derivation closed
    expect(deriveRunRailIdentity(timeline, 55)).toEqual({
      kind: "fallback",
      label: "identity: from journal (55 events)",
      events: 55,
    });
  });

  test("r3. a partial page (nextAfter non-null) with cursor >= inspect revision still resolves", () => {
    const timeline = oracleTimeline(30, 30);
    expect(deriveRunRailIdentity(timeline, 30)).toEqual(RESOLVED_1_2);
  });

  test("r4. cursor < inspect revision falls back", () => {
    const timeline = oracleTimeline(25, 25);
    expect(deriveRunRailIdentity(timeline, 39)).toEqual({
      kind: "fallback",
      label: "identity: from journal (25 events)",
      events: 25,
    });
  });

  test("r5. the fallback count n is the considered-prefix length, never the loaded length", () => {
    const timeline = oracleTimeline(66, null);
    // revision 47 sits in the 40-55 fallback window with a dense prefix of 47
    const derived = deriveRunRailIdentity(timeline, 47);
    expect(derived.kind).toBe("fallback");
    if (derived.kind !== "fallback") return;
    expect(derived.label).toBe("identity: from journal (47 events)");
    expect(derived.events).toBe(47);
  });
});

describe("ui rail identity: iteration-owned state (§3.2.2)", () => {
  test("s1. the oracle window vectors (13-14, 15-25, 26-28, 29-39, 40-55, 56-66)", () => {
    const timeline = oracleTimeline(66, null);
    const expectedFor = (revision: number): "fallback" | typeof RESOLVED_1_1 | typeof RESOLVED_1_2 | typeof RESOLVED_2_1 => {
      if (revision <= 14) return "fallback";
      if (revision <= 25) return RESOLVED_1_1;
      if (revision <= 28) return "fallback";
      if (revision <= 39) return RESOLVED_1_2;
      if (revision <= 55) return "fallback";
      return RESOLVED_2_1;
    };
    for (let revision = 1; revision <= 66; revision += 1) {
      const derived = deriveRunRailIdentity(timeline, revision);
      const expected = expectedFor(revision);
      if (expected === "fallback") {
        expect(derived.kind, `revision ${revision}`).toBe("fallback");
      } else {
        expect(derived, `revision ${revision}`).toEqual(expected);
      }
    }
  });

  test("s2. an execution start outside an open iteration is ignored (synthetic vector)", () => {
    const start = oracleEvent("start_agent_execution", 1, { execution_index: 1, state_id: "architect" });
    const openGeneration = oracleEvent("stage_generation_opened", 2, { generation_index: 1 });
    const openIteration = oracleEvent("stage_iteration_opened", 3, { generation_index: 1, iteration_index: 1 });
    const inside = oracleEvent("start_agent_execution", 4, { execution_index: 2, state_id: "architect" });
    const timeline = rawTimeline([start, openGeneration, openIteration], 3);
    // the pre-generation start never supplies the iteration's state
    expect(deriveRunRailIdentity(timeline, 3)).toEqual({
      kind: "fallback",
      label: "identity: from journal (3 events)",
      events: 3,
    });
    // a start inside the open iteration supplies the state
    const timeline2 = rawTimeline([start, openGeneration, openIteration, inside], 4);
    expect(deriveRunRailIdentity(timeline2, 4)).toEqual({ kind: "resolved", generation: 1, iteration: 1, state: "architect" });
  });

  test("s3. a newly opened iteration without its own execution start falls back", () => {
    // the oracle revision-54 vector: generation 2 opens at 53 and its
    // iteration at 54 with an empty state slot
    const timeline = oracleTimeline(66, null);
    expect(deriveRunRailIdentity(timeline, 53)).toEqual({
      kind: "fallback",
      label: "identity: from journal (53 events)",
      events: 53,
    });
    expect(deriveRunRailIdentity(timeline, 54)).toEqual({
      kind: "fallback",
      label: "identity: from journal (54 events)",
      events: 54,
    });
    // the dedicated synthetic vector (start before, iteration without)
    const openGeneration = oracleEvent("stage_generation_opened", 1, { generation_index: 1 });
    const openIteration = oracleEvent("stage_iteration_opened", 2, { generation_index: 1, iteration_index: 1 });
    const timeline2 = rawTimeline([openGeneration, openIteration], 2);
    expect(deriveRunRailIdentity(timeline2, 2).kind).toBe("fallback");
  });

  test("s4. a decision execution inside the open iteration sets the state id", () => {
    const openGeneration = oracleEvent("stage_generation_opened", 1, { generation_index: 1 });
    const openIteration = oracleEvent("stage_iteration_opened", 2, { generation_index: 1, iteration_index: 1 });
    const decision = oracleEvent("start_decision_execution", 3, { execution_index: 2, state_id: "decision_state" });
    const timeline = rawTimeline([openGeneration, openIteration, decision], 3);
    expect(deriveRunRailIdentity(timeline, 3)).toEqual({
      kind: "resolved",
      generation: 1,
      iteration: 1,
      state: "decision_state",
    });
  });

  test("s5. an execution started before its iteration opened never leaks into the triple", () => {
    const timeline = oracleTimeline(66, null);
    // revisions 13-14: the revision-2 architect start predates the iteration
    for (const revision of [13, 14]) {
      expect(deriveRunRailIdentity(timeline, revision).kind, `revision ${revision}`).toBe("fallback");
    }
  });

  test("s6. iteration/generation close always clears the held state", () => {
    const openGeneration = oracleEvent("stage_generation_opened", 1, { generation_index: 1 });
    const openIteration = oracleEvent("stage_iteration_opened", 2, { generation_index: 1, iteration_index: 1 });
    const start = oracleEvent("start_agent_execution", 3, { execution_index: 1, state_id: "architect" });
    const closeIteration = oracleEvent("stage_iteration_closed", 4, { generation_index: 1, iteration_index: 1 });
    const reopenIteration = oracleEvent("stage_iteration_opened", 5, { generation_index: 1, iteration_index: 2 });
    const closeGeneration = oracleEvent("stage_generation_closed", 6, { generation_index: 1 });
    const timeline = rawTimeline([openGeneration, openIteration, start], 3);
    expect(deriveRunRailIdentity(timeline, 3)).toEqual({ kind: "resolved", generation: 1, iteration: 1, state: "architect" });
    const timelineClosed = rawTimeline([...[openGeneration, openIteration, start, closeIteration]], 4);
    expect(deriveRunRailIdentity(timelineClosed, 4).kind).toBe("fallback");
    const timelineReopened = rawTimeline(
      [...[openGeneration, openIteration, start, closeIteration, reopenIteration]],
      5,
    );
    expect(deriveRunRailIdentity(timelineReopened, 5).kind).toBe("fallback");
    const timelineClosedGeneration = rawTimeline(
      [...[openGeneration, openIteration, start, closeIteration, reopenIteration, closeGeneration]],
      6,
    );
    expect(deriveRunRailIdentity(timelineClosedGeneration, 6).kind).toBe("fallback");
  });
});

describe("ui rail identity: generic contract", () => {
  test("g1. the empty journal and the unbound derivation fall back with the zero-event label", () => {
    const fresh = createTimelineState(RUN_ID);
    expect(deriveRunRailIdentity(fresh, null).kind).toBe("fallback");
    expect(deriveRunRailIdentity(fresh, null)).toEqual({
      kind: "fallback",
      label: "identity: from journal (0 events)",
      events: 0,
    });
    expect(deriveRunRailIdentity(null, 39)).toEqual({
      kind: "fallback",
      label: "identity: from journal (0 events)",
      events: 0,
    });
    expect(deriveRunRailIdentity({}, 39)).toEqual({
      kind: "fallback",
      label: "identity: from journal (0 events)",
      events: 0,
    });
  });

  test("g2. contradiction vectors inside the prefix fall back", () => {
    const cases: Record<string, unknown>[][] = [
      // double generation open
      [
        oracleEvent("stage_generation_opened", 1, { generation_index: 1 }),
        oracleEvent("stage_generation_opened", 2, { generation_index: 1 }),
      ],
      // iteration open while the generation is closed
      [oracleEvent("stage_iteration_opened", 1, { generation_index: 2, iteration_index: 1 })],
      // iteration of a foreign generation
      [
        oracleEvent("stage_generation_opened", 1, { generation_index: 1 }),
        oracleEvent("stage_iteration_opened", 2, { generation_index: 2, iteration_index: 1 }),
      ],
      // double iteration open
      [
        oracleEvent("stage_generation_opened", 1, { generation_index: 1 }),
        oracleEvent("stage_iteration_opened", 2, { generation_index: 1, iteration_index: 1 }),
        oracleEvent("stage_iteration_opened", 3, { generation_index: 1, iteration_index: 2 }),
      ],
      // close of a non-open pair
      [
        oracleEvent("stage_generation_opened", 1, { generation_index: 1 }),
        oracleEvent("stage_iteration_opened", 2, { generation_index: 1, iteration_index: 1 }),
        oracleEvent("stage_iteration_closed", 3, { generation_index: 1, iteration_index: 2 }),
      ],
      // generation close without its pair
      [oracleEvent("stage_generation_closed", 1, { generation_index: 1 })],
    ];
    for (const entries of cases) {
      const timeline = rawTimeline(entries, entries.length);
      const derived = deriveRunRailIdentity(timeline, entries.length);
      expect(derived.kind).toBe("fallback");
    }
  });

  test("g3. determinism: the same input yields the same frozen result", () => {
    const timeline = oracleTimeline(66, null);
    const first = deriveRunRailIdentity(timeline, 66);
    const second = deriveRunRailIdentity(timeline, 66);
    expect(Object.isFrozen(first)).toBe(true);
    expect(first).toEqual(second);
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
  });

  test("g4. deep freeze and caller mutation isolation", () => {
    const timeline = oracleTimeline(66, null);
    const derived = deriveRunRailIdentity(timeline, 66);
    expect(Object.isFrozen(derived)).toBe(true);
    const capabilityEntry = { runId: "run-a", projectId: "project-a", projectAvailable: true };
    const projectsModel = {
      kind: "projects",
      projects: [{ id: "project-a", inputRootIds: ["tasks"], projectApply: true }],
    };
    const capability = deriveRunRailCapability(projectsModel, capabilityEntry);
    expect(Object.isFrozen(capability)).toBe(true);
    // caller mutation after the result cannot rewrite the result
    capabilityEntry.projectId = "other";
    const firstProject = projectsModel.projects[0];
    if (firstProject !== undefined) {
      firstProject.projectApply = false;
    }
    expect(capability.state).toBe("available");
    expect(capability.projectApply).toBe(true);
  });

  test("g5. in-flight agreement: the derived state equals boundary.in_flight.stateId", () => {
    const timeline = oracleTimeline(39, null);
    const derived = deriveRunRailIdentity(timeline, 39);
    if (derived.kind !== "resolved") {
      throw new Error("expected the resolved triple at revision 39");
    }
    const boundary = { kind: "in_flight", executionIndex: 3, stateId: derived.state, executionRole: "stage", phase: "running" };
    expect(derived.state).toBe("dev_entry");
    expect(derived.state).toBe(boundary.stateId);
  });

  test("g6. export surface: both derivations are exported functions", async () => {
    const runtime = (await import("../ui/app.js")) as unknown as Record<string, unknown>;
    expect(typeof runtime["deriveRunRailIdentity"]).toBe("function");
    expect(typeof runtime["deriveRunRailCapability"]).toBe("function");
  });

  test("g7. source scan: one derivation owner, no fetch/time/randomness in the S1 region", () => {
    const source = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");
    expect(source.split("export function deriveRunRailIdentity").length - 1).toBe(1);
    expect(source.split("export function deriveRunRailCapability").length - 1).toBe(1);
    const regionStart = source.indexOf("/* S1 REGION START");
    const regionEnd = source.indexOf("/* S1 REGION END");
    expect(regionStart).toBeGreaterThan(-1);
    expect(regionEnd).toBeGreaterThan(regionStart);
    const region = source.slice(regionStart, regionEnd);
    for (const banned of ["fetch(", "setTimeout", "Date.now", "Math.random", "new Request", "XMLHttpRequest"]) {
      expect(region.includes(banned), banned).toBe(false);
    }
  });
});

describe("ui rail identity: the capability derivation (§9.4)", () => {
  const entry = Object.freeze({ runId: "run-a", projectId: "project-a", projectAvailable: true });
  const entryUnavailable = Object.freeze({ runId: "run-a", projectId: "project-a", projectAvailable: false });
  const projects = (projectApply: boolean) => ({
    kind: "projects",
    projects: [Object.freeze({ id: "project-a", inputRootIds: Object.freeze(["tasks"]), projectApply })],
  });

  test("all four authority conditions with project_apply true render available", () => {
    expect(deriveRunRailCapability(projects(true), entry)).toEqual({ state: "available", projectApply: true });
  });

  test("all four authority conditions with project_apply false render the unavailable string source", () => {
    expect(deriveRunRailCapability(projects(false), entry)).toEqual({ state: "unavailable", projectApply: false });
  });

  test("any failing authority condition yields no claim", () => {
    expect(deriveRunRailCapability(projects(true), null).state).toBe("none");
    expect(deriveRunRailCapability(projects(true), entryUnavailable).state).toBe("none");
    expect(deriveRunRailCapability(null, entry).state).toBe("none");
    expect(deriveRunRailCapability({ kind: "runs", runs: [] , nextAfter: null }, entry).state).toBe("none");
    const foreign = {
      kind: "projects",
      projects: [Object.freeze({ id: "project-b", inputRootIds: Object.freeze(["tasks"]), projectApply: true })],
    };
    expect(deriveRunRailCapability(foreign, entry).state).toBe("none");
    const untypedFlag = {
      kind: "projects",
      projects: [Object.freeze({ id: "project-a", inputRootIds: Object.freeze(["tasks"]), projectApply: "yes" })],
    };
    expect(deriveRunRailCapability(untypedFlag, entry).state).toBe("none");
  });
});



