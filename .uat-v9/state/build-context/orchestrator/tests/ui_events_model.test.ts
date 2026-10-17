/**
 * Permanent contract suite of the browser UI v4 lifecycle timeline
 * model: the one events document parser (exact own keys at every
 * level, the 28 public event kinds with their exact reference shapes,
 * the run binding, positive-safe-integer revisions), the one dense
 * cursor/pagination merge, the fail-closed stop and retry policy, the
 * separated revision summary and the aria-live announcement policy —
 * plus the source restrictions of the events layer.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  EVENTS_PAGE_LIMIT,
  PIPELINE_V2_EVENT_KINDS,
  UiContractError,
  applyEventsPage,
  createTimelineState,
  eventReferenceKeys,
  parseEventsDocument,
  stopTimeline,
  timelineAnnouncement,
  timelineRetryAvailable,
  timelineSummary,
} from "../ui/app.js";
import type { UiApiFailure, UiEventsModel, UiTimelineState } from "../ui/app.js";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

const contractReason = (run: () => unknown): string => {
  try {
    run();
  } catch (error) {
    if (error instanceof UiContractError) {
      return error.reason;
    }
    throw error;
  }
  return "accepted";
};

/** One canonical event entry for a kind, built from the module's own
 * reference-key table (the single vocabulary source of the tests). */
function buildEvent(kind: string, revision: number): Record<string, unknown> {
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

function eventsDocument(
  runId: string,
  revision: number,
  kinds: readonly string[],
  nextAfter: number | null,
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "events",
    run_id: runId,
    revision,
    events: kinds.map((kind, index) => buildEvent(kind, index + 1)),
    next_after: nextAfter,
  };
}

describe("ui events model: the one events document parser", () => {
  test("m1. a canonical page parses to the frozen verbatim projection", () => {
    const document = eventsDocument("run-a", 3, ["create_run", "start_agent_execution", "agent_data_prepared"], null);
    const model = parseEventsDocument(document, "run-a");
    expect(model.kind).toBe("events");
    expect(model.runId).toBe("run-a");
    expect(model.revision).toBe(3);
    expect(model.nextAfter).toBeNull();
    expect(model.events).toEqual([
      { revision: 1, kind: "create_run" },
      { revision: 2, kind: "start_agent_execution", execution_index: 2, state_id: "dev_entry" },
      { revision: 3, kind: "agent_data_prepared", execution_index: 3 },
    ]);
    expect(Object.isFrozen(model)).toBe(true);
    expect(Object.isFrozen(model.events)).toBe(true);
    for (const entry of model.events) {
      expect(Object.isFrozen(entry)).toBe(true);
    }
    expect(parseEventsDocument(document, "run-a")).toEqual(model);
  });

  test("m2. the closed vocabulary carries all 28 public kinds with their exact references", () => {
    expect([...PIPELINE_V2_EVENT_KINDS].sort()).toEqual(
      [
        "create_run",
        "start_agent_execution",
        "agent_data_prepared",
        "agent_execution_session_created",
        "agent_tool_session_created",
        "agent_running",
        "agent_outputs_accepted",
        "agent_cleanup_completed",
        "agent_failed",
        "start_decision_execution",
        "decision_evaluated",
        "decision_failed",
        "transition_committed",
        "run_waiting",
        "wait_response_recorded",
        "terminal_reached",
        "run_outputs_published",
        "run_succeeded",
        "run_failed",
        "run_cleanup_failed",
        "stage_generation_opened",
        "stage_iteration_opened",
        "stage_iteration_closed",
        "stage_generation_closed",
        "plan_intent_accepted",
        "task_revision_accepted",
        "plan_revision_accepted",
        "iteration_grant_recorded",
      ].sort(),
    );
    expect(PIPELINE_V2_EVENT_KINDS.length).toBe(28);
    expect(eventReferenceKeys("create_run")).toEqual([]);
    expect(eventReferenceKeys("start_agent_execution")).toEqual(["execution_index", "state_id"]);
    expect(eventReferenceKeys("start_decision_execution")).toEqual(["execution_index", "state_id"]);
    expect(eventReferenceKeys("transition_committed")).toEqual(["execution_index", "transition_index"]);
    expect(eventReferenceKeys("run_waiting")).toEqual(["wait_index"]);
    expect(eventReferenceKeys("stage_iteration_opened")).toEqual(["generation_index", "iteration_index"]);
    expect(eventReferenceKeys("stage_iteration_closed")).toEqual(["generation_index", "iteration_index"]);
    expect(eventReferenceKeys("task_revision_accepted")).toEqual(["task_index"]);
    expect(eventReferenceKeys("plan_revision_accepted")).toEqual(["plan_index"]);
    expect(eventReferenceKeys("iteration_grant_recorded")).toEqual(["grant_index"]);
    expect(eventReferenceKeys("mystery")).toBeNull();
    // every kind parses with exactly its own keys, in history order
    const document = eventsDocument("run-a", 28, PIPELINE_V2_EVENT_KINDS, null);
    const model = parseEventsDocument(document, "run-a");
    expect(model.events.length).toBe(28);
    for (let index = 0; index < 28; index += 1) {
      const entry = model.events[index];
      expect(entry?.kind).toBe(PIPELINE_V2_EVENT_KINDS[index]);
      expect(Object.keys(entry ?? {}).sort()).toEqual(
        ["kind", "revision", ...(eventReferenceKeys(PIPELINE_V2_EVENT_KINDS[index] ?? "") ?? [])].sort(),
      );
    }
  });

  test("m3. unknown versions, kinds, extra, inherited and missing keys fail closed", () => {
    expect(contractReason(() => parseEventsDocument(null, "run-a"))).toBe("not_a_document");
    expect(contractReason(() => parseEventsDocument("text", "run-a"))).toBe("not_a_document");
    expect(contractReason(() => parseEventsDocument({ kind: "events" }, "run-a"))).toBe("unknown_schema_version");
    expect(contractReason(() => parseEventsDocument({ schema_version: 2, kind: "events" }, "run-a"))).toBe("unknown_schema_version");
    expect(contractReason(() => parseEventsDocument({ schema_version: 1, kind: "run" }, "run-a"))).toBe("unknown_kind");
    expect(
      contractReason(() =>
        parseEventsDocument(
          { ...eventsDocument("run-a", 1, ["create_run"], null), extra: 1 },
          "run-a",
        ),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument(
          { schema_version: 1, kind: "events", run_id: "run-a", revision: 1, events: [], next_after: null },
          "run-b",
        ),
      ),
    ).toBe("unexpected_run");
    // inherited-only shapes never satisfy the document shape
    const inherited = Object.create({ schema_version: 1, kind: "events", run_id: "run-a", revision: 1, events: [], next_after: null });
    expect(contractReason(() => parseEventsDocument(inherited, "run-a"))).toBe("invalid_shape");
    // entries: extra key, missing key, unknown kind, hostile shapes
    const base = eventsDocument("run-a", 2, ["create_run"], null);
    expect(
      contractReason(() =>
        parseEventsDocument(
          { ...base, events: [{ revision: 1, kind: "create_run", execution_index: 1 }] },
          "run-a",
        ),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1, kind: "agent_running" }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ kind: "create_run" }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1, kind: "mystery" }] }, "run-a"),
      ),
    ).toBe("unknown_event_kind");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1, kind: "start_agent_execution", execution_index: 1, state_id: "../escape" }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1, kind: "run_waiting", wait_index: 0 }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1.5, kind: "create_run" }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
  });

  test("m4. scalar boundaries of the document are exact", () => {
    const base = eventsDocument("run-a", 1, ["create_run"], null);
    for (const revision of [0, -1, 1.5, "1", Number.MAX_SAFE_INTEGER + 1]) {
      expect(contractReason(() => parseEventsDocument({ ...base, revision }, "run-a"))).toBe("invalid_shape");
    }
    for (const nextAfter of [0, -1, 1.5, "1", Number.MAX_SAFE_INTEGER + 1]) {
      expect(contractReason(() => parseEventsDocument({ ...base, next_after: nextAfter }, "run-a"))).toBe("invalid_shape");
    }
    for (const runId of ["", "../x", "a b", 1]) {
      expect(contractReason(() => parseEventsDocument({ ...base, run_id: runId }, "run-a"))).toBe("invalid_shape");
    }
    expect(parseEventsDocument({ ...base, revision: Number.MAX_SAFE_INTEGER }, "run-a").revision).toBe(Number.MAX_SAFE_INTEGER);
  });

  test("m5. hostile strings never pass the closed vocabulary", () => {
    const base = eventsDocument("run-a", 1, ["create_run"], null);
    for (const kind of ["<img src=x>", "create_run ", "CREATE_RUN", "create_run;drop"]) {
      expect(contractReason(() => parseEventsDocument({ ...base, events: [{ revision: 1, kind }] }, "run-a"))).toBe("unknown_event_kind");
    }
    expect(
      contractReason(() =>
        parseEventsDocument({ ...base, events: [{ revision: 1, kind: "start_agent_execution", execution_index: 1, state_id: "<script>" }] }, "run-a"),
      ),
    ).toBe("invalid_shape");
  });
});

describe("ui events model: the one dense merge", () => {
  const page = (runId: string, revision: number, kinds: readonly string[], nextAfter: number | null, startRevision = 1) =>
    parseEventsDocument(
      {
        schema_version: 1,
        kind: "events",
        run_id: runId,
        revision,
        events: kinds.map((kind, index) => buildEvent(kind, startRevision + index)),
        next_after: nextAfter,
      },
      runId,
    );

  test("m6. a dense first page applies and advances the cursor", () => {
    const state = createTimelineState("run-a");
    const next = applyEventsPage(state, page("run-a", 3, ["create_run", "start_agent_execution", "agent_data_prepared"], null), 0);
    expect(next.cursor).toBe(3);
    expect(next.snapshotRevision).toBe(3);
    expect(next.nextAfter).toBeNull();
    expect(next.events.length).toBe(3);
    expect(next.stopped).toBe(false);
    expect(next.error).toBeNull();
  });

  test("m7. several pages concatenate in verbatim history order", () => {
    let state = createTimelineState("run-a");
    state = applyEventsPage(state, page("run-a", 2, ["create_run", "start_agent_execution"], 2), 0);
    expect(state.nextAfter).toBe(2);
    expect(state.cursor).toBe(2);
    state = applyEventsPage(state, page("run-a", 4, ["agent_data_prepared", "agent_running"], 4, 3), 2);
    expect(state.events.map((entry) => entry.revision)).toEqual([1, 2, 3, 4]);
    expect(state.cursor).toBe(4);
    state = applyEventsPage(state, page("run-a", 4, [], null), 4);
    expect(state.events.length).toBe(4);
    expect(state.cursor).toBe(4);
    expect(state.snapshotRevision).toBe(4);
  });

  test("m8. duplicates, gaps, regressions and out-of-order pages are contract errors", () => {
    const state = applyEventsPage(createTimelineState("run-a"), page("run-a", 2, ["create_run", "start_agent_execution"], null), 0);
    // duplicate revision
    expect(
      contractReason(() => applyEventsPage(state, page("run-a", 3, ["agent_data_prepared", "agent_data_prepared"], null), 2)),
    ).toBe("invalid_page");
    // gap
    expect(contractReason(() => applyEventsPage(state, page("run-a", 4, ["agent_running", "agent_outputs_accepted"], null), 2))).toBe("invalid_page");
    // out of order
    expect(
      contractReason(() =>
        applyEventsPage(
          state,
          parseEventsDocument(
            {
              schema_version: 1,
              kind: "events",
              run_id: "run-a",
              revision: 4,
              events: [buildEvent("agent_data_prepared", 4), buildEvent("agent_running", 3)],
              next_after: null,
            },
            "run-a",
          ),
          2,
        ),
      ),
    ).toBe("invalid_page");
    // regression: the requested cursor is behind the applied cursor
    expect(contractReason(() => applyEventsPage(state, page("run-a", 3, ["agent_data_prepared"], null), 0))).toBe("stale_page");
    // a foreign run page never merges
    expect(contractReason(() => applyEventsPage(state, page("run-b", 3, ["agent_data_prepared"], null), 2))).toBe("unexpected_run");
  });

  test("m9. next_after is bound to the last event and to the document revision", () => {
    const state = createTimelineState("run-a");
    // nextAfter must equal the last event revision
    expect(
      contractReason(() => applyEventsPage(state, page("run-a", 5, ["create_run", "start_agent_execution"], 3), 0)),
    ).toBe("invalid_page");
    // nextAfter null requires the page to reach the document revision
    expect(
      contractReason(() => applyEventsPage(state, page("run-a", 5, ["create_run", "start_agent_execution"], null), 0)),
    ).toBe("invalid_page");
    // a page behind its own requested cursor is invalid
    const ahead = applyEventsPage(state, page("run-a", 2, ["create_run", "start_agent_execution"], 2), 0);
    expect(contractReason(() => applyEventsPage(ahead, page("run-a", 1, ["agent_data_prepared"], null), 2))).toBe("invalid_page");
  });

  test("m10. an empty page is legal only when the cursor reached the document revision", () => {
    const state = applyEventsPage(createTimelineState("run-a"), page("run-a", 4, ["create_run", "start_agent_execution", "agent_data_prepared", "agent_running"], null), 0);
    const caughtUp = applyEventsPage(state, page("run-a", 4, [], null), 4);
    expect(caughtUp.events.length).toBe(4);
    expect(caughtUp.cursor).toBe(4);
    // an empty page below the revision is invalid
    expect(contractReason(() => applyEventsPage(state, page("run-a", 3, [], null), 4))).toBe("invalid_page");
    // an empty page with a next cursor is invalid
    expect(contractReason(() => applyEventsPage(state, page("run-a", 4, [], 4), 4))).toBe("invalid_page");
  });

  test("m11. the merge validates its inputs fail-closed", () => {
    const state = createTimelineState("run-a");
    expect(contractReason(() => applyEventsPage(null as unknown as UiTimelineState, page("run-a", 1, ["create_run"], null), 0))).toBe("invalid_state");
    expect(contractReason(() => applyEventsPage(state, { kind: "run" } as unknown as UiEventsModel, 0))).toBe("invalid_state");
    for (const after of [-1, 1.5, "0", Number.MAX_SAFE_INTEGER + 1]) {
      expect(contractReason(() => applyEventsPage(state, page("run-a", 1, ["create_run"], null), after as never))).toBe("invalid_cursor");
    }
  });

  test("m12. the merged state is frozen and the input state is never mutated", () => {
    const state = createTimelineState("run-a");
    const next = applyEventsPage(state, page("run-a", 2, ["create_run", "start_agent_execution"], null), 0);
    expect(Object.isFrozen(next)).toBe(true);
    expect(Object.isFrozen(next.events)).toBe(true);
    expect(state.events.length).toBe(0);
    expect(state.cursor).toBe(0);
  });
});

describe("ui events model: stop, retry policy, summary and announcements", () => {
  const loaded = applyEventsPage(
    createTimelineState("run-a"),
    parseEventsDocument(eventsDocument("run-a", 2, ["create_run", "start_agent_execution"], 2), "run-a"),
    0,
  );

  test("m13. the stop keeps the state and classifies the retry path", () => {
    const stopped = stopTimeline(loaded, { layer: "request", reason: "invalid_cursor" });
    expect(stopped.stopped).toBe(true);
    expect(stopped.error).toEqual({ layer: "request", reason: "invalid_cursor" });
    expect(stopped.events).toBe(loaded.events);
    expect(stopped.cursor).toBe(loaded.cursor);
    expect(timelineRetryAvailable(stopped)).toBe(false);
    const retryable = stopTimeline(loaded, { layer: "transport", reason: "network_error" });
    expect(timelineRetryAvailable(retryable)).toBe(true);
    const contract = stopTimeline(loaded, { layer: "contract", reason: "invalid_page" });
    expect(timelineRetryAvailable(contract)).toBe(true);
    expect(timelineRetryAvailable(loaded)).toBe(false);
    expect(contractReason(() => stopTimeline(loaded, { layer: "transport" } as unknown as UiApiFailure))).toBe("invalid_failure");
    expect(contractReason(() => stopTimeline(null as unknown as UiTimelineState, { layer: "transport", reason: "x" }))).toBe("invalid_state");
  });

  test("m14. the summary separates the inspect revision from the event snapshot", () => {
    const summary = timelineSummary(loaded, 9);
    expect(summary).toEqual({
      detailRevision: 9,
      eventsRevision: 2,
      cursor: 2,
      caughtUp: false,
      hasMore: true,
      stopped: false,
    });
    expect(timelineSummary(loaded, null).detailRevision).toBeNull();
    const caught = applyEventsPage(loaded, parseEventsDocument(eventsDocument("run-a", 2, [], null), "run-a"), 2);
    expect(timelineSummary(caught, 2).caughtUp).toBe(true);
    expect(timelineSummary(caught, 2).hasMore).toBe(false);
    expect(timelineSummary(createTimelineState("run-a"), 5).caughtUp).toBe(false);
    expect(contractReason(() => timelineSummary(null as unknown as UiTimelineState, 1))).toBe("invalid_state");
  });

  test("m15. announcements cover added events, catch-up and errors, never rows", () => {
    const fresh = createTimelineState("run-a");
    expect(timelineAnnouncement(fresh, loaded)).toBe("Added 2 lifecycle events.");
    const complete = applyEventsPage(loaded, parseEventsDocument(eventsDocument("run-a", 2, [], null), "run-a"), 2);
    expect(timelineAnnouncement(loaded, complete)).toBe("Lifecycle events caught up.");
    const stopped = stopTimeline(loaded, { layer: "request", reason: "invalid_cursor" });
    expect(timelineAnnouncement(loaded, stopped)).toBe("Lifecycle events error: request: invalid_cursor.");
    expect(timelineAnnouncement(stopped, stopped)).toBeNull();
    // an unchanged repeated page is silent
    expect(timelineAnnouncement(complete, applyEventsPage(complete, parseEventsDocument(eventsDocument("run-a", 2, [], null), "run-a"), 2))).toBeNull();
    // a page that appends and catches up announces both facts once
    const partial = parseEventsDocument(
      eventsDocument("run-a", 4, ["create_run", "start_agent_execution"], 2),
      "run-a",
    );
    const grown = applyEventsPage(createTimelineState("run-a"), partial, 0);
    const grownComplete = applyEventsPage(
      grown,
      parseEventsDocument(
        {
          schema_version: 1,
          kind: "events",
          run_id: "run-a",
          revision: 4,
          events: [buildEvent("agent_data_prepared", 3), buildEvent("agent_running", 4)],
          next_after: null,
        },
        "run-a",
      ),
      2,
    );
    expect(timelineAnnouncement(grown, grownComplete)).toBe("Added 2 lifecycle events. Caught up.");
    expect(contractReason(() => timelineAnnouncement(null as unknown as UiTimelineState, loaded))).toBe("invalid_state");
  });
});

describe("ui events model: source restrictions of the events layer", () => {
  test("s1. one owner per events concern and no new timers or mutations", () => {
    expect(appSource.split("export function parseEventsDocument(").length - 1).toBe(1);
    expect(appSource.split("export async function requestEventsJson(").length - 1).toBe(1);
    expect(appSource.split("export function createEventsTransport(").length - 1).toBe(1);
    expect(appSource.split("export function eventsRequestPath(").length - 1).toBe(1);
    expect(appSource.split("export function createTimelineState(").length - 1).toBe(1);
    expect(appSource.split("export function applyEventsPage(").length - 1).toBe(1);
    expect(appSource.split("createEventsTransport({").length - 1).toBe(1);
    // the single URL builder is the only events path site
    expect(appSource.split("/events?").length - 1).toBe(1);
    // the events fetch is a GET: no method override anywhere in the
    // events wire owner and exactly one POST site in the whole module
    expect(appSource.split('method: "POST"').length - 1).toBe(1);
    expect(appSource.split('method: "GET"').length - 1).toBe(0);
    // no event-path timer, polling loop or retry machinery
    expect(appSource.split("setTimeout(").length - 1).toBe(1);
    expect(appSource.includes("setInterval(")).toBe(false);
    for (const banned of ["EventSource", "WebSocket", "backoff", "retryTimeout", "scheduleRetry"]) {
      expect(appSource.includes(banned)).toBe(false);
    }
    // the events path never mutates: no mutating endpoint names there
    const eventsRegion = appSource.slice(appSource.indexOf("Lifecycle events (v4)"), appSource.indexOf("Manual lifecycle controls"));
    for (const banned of ["/resume", "continue-stage", "revise-task", "/respond", "method: \"POST\""]) {
      expect(eventsRegion.includes(banned)).toBe(false);
    }
  });

  test("s2. no timestamps, storage, raw state or host material anywhere", () => {
    expect(appSource.includes("new Date(")).toBe(false);
    expect(appSource.includes("Date.now(")).toBe(false);
    expect(appSource.includes("localStorage")).toBe(false);
    expect(appSource.includes("document.cookie")).toBe(false);
    for (const banned of ["stderr", "state.json", "runRoot", "run_root", "/opt/", "/home/"]) {
      expect(appSource.includes(banned)).toBe(false);
    }
    for (const line of appSource.split("\n")) {
      expect(line.startsWith("import ")).toBe(false);
    }
  });

  test("s3. the timeline renders through safe DOM APIs only", () => {
    const timelineRegion = appSource.slice(appSource.indexOf("Lifecycle timeline (v4)"), appSource.indexOf("Thin DOM adapter"));
    const domRegion = appSource.slice(appSource.indexOf("Lifecycle timeline (v4): the thin DOM adapter"));
    for (const region of [timelineRegion, domRegion]) {
      expect(region.includes("innerHTML")).toBe(false);
      expect(region.includes("insertAdjacentHTML")).toBe(false);
      expect(region.includes("outerHTML")).toBe(false);
    }
    // every event cell is written through the shared element/textContent helper
    expect(domRegion.split("element(\"td\"").length - 1).toBeGreaterThanOrEqual(3);
  });

  test("s4. the page limit is the fixed module constant", () => {
    expect(EVENTS_PAGE_LIMIT).toBe(100);
    expect(appSource.includes(`limit=${"EVENTS_PAGE_LIMIT"}`) || appSource.includes("limit=${EVENTS_PAGE_LIMIT}")).toBe(true);
  });
});
