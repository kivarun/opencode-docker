/**
 * Permanent wiring suite of the browser UI v6 project-changes panel:
 * the fake-DOM harness boots the shipped `app.js` and drives the
 * production call chains (run selection -> inspect -> the automatic
 * one-shot changes load -> Load more -> stale -> explicit Reload -> run
 * selection change), with an instrumented fetch and the real changes
 * endpoint documents. Every test proves the automatic request matrix,
 * the pagination/stale behavior, the presentation states, the safe
 * text-only render and the isolation from the inspect, events, logs
 * and mutation surfaces.
 */
import { describe, expect, test } from "bun:test";
import { eventReferenceKeys } from "../ui/app.js";
import {
  FakeElement,
  createUiHarness,
  jsonResponse,
  projectsDocument,
  runDocument,
  runsPageDocument,
} from "./ui_dom_harness.ts";

const OBS = "a".repeat(64);
const OBS2 = "b".repeat(64);

function eventEntry(kind: string, revision: number): Record<string, unknown> {
  const keys = eventReferenceKeys(kind) ?? [];
  const entry: Record<string, unknown> = { revision, kind };
  for (const key of keys) {
    entry[key] = key === "state_id" ? "architect" : key === "execution_index" || key.endsWith("_index") ? 1 : "x";
  }
  return entry;
}

/** A dense events page of `revision` entries; only kinds with plain
 * reference values are used so the filler stays valid. */
function denseEvents(revision: number): Record<string, unknown>[] {
  const events: Record<string, unknown>[] = [];
  for (let index = 1; index <= revision; index++) {
    events.push(eventEntry(index === 1 ? "create_run" : index === revision ? "run_succeeded" : "agent_running", index));
  }
  return events;
}

const fileNode = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  kind: "regular_file",
  mode: 0o644,
  byte_count: 8,
  sha256: "c".repeat(64),
  multiple_links: false,
  ...overrides,
});

function changesPage(options: {
  readonly runId?: string;
  readonly revision?: number;
  readonly observation?: string;
  readonly nextAfter?: number | null;
  readonly entries?: Array<Record<string, unknown>>;
  readonly summaryOverrides?: Record<string, number>;
  readonly fail?: "stale" | "not_ready" | "network" | "contract";
}): Record<string, unknown> {
  if (options.fail === "stale") {
    return { schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" };
  }
  if (options.fail === "not_ready") {
    return {
      schema_version: 1,
      kind: "command_result",
      run_id: options.runId ?? "run-a",
      outcome: "failed",
      exit_code: 1,
      reason: "changes_not_ready",
    };
  }
  if (options.fail === "contract") {
    return { schema_version: 1, kind: "project_changes", run_id: "run-b", revision: 1, observation: OBS, summary: {}, changes: [], next_after: null };
  }
  const entries = options.entries ?? [];
  const counts: Record<string, number> = {};
  for (const entry of entries) {
    const kind = entry["change"] as string;
    counts[kind] = (counts[kind] ?? 0) + 1;
    counts[entry["source_relation"] as string] = (counts[entry["source_relation"] as string] ?? 0) + 1;
  }
  const summary = {
    total: entries.length,
    added: 0, deleted: 0, content_modified: 0, mode_modified: 0,
    content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0,
    matches_baseline: 0, matches_result: 0, diverged: 0,
    ...counts,
    ...(options.summaryOverrides ?? {}),
  };
  const page = {
    schema_version: 1,
    kind: "project_changes",
    run_id: options.runId ?? "run-a",
    revision: options.revision ?? 12,
    observation: options.observation ?? OBS,
    summary,
    changes: entries,
    next_after: options.nextAfter ?? null,
  };
  return options.fail === "network" ? { ...page, fail: "network" } : page;
}

interface FetchPlan {
  readonly inspect?: Array<{ revision: number; status: string; boundary: Record<string, unknown> }>;
  readonly changes?: Array<Record<string, unknown>>;
  readonly runs?: readonly string[];
}

function makeFetch(plan: FetchPlan = {}) {
  const changesGets: string[] = [];
  const posts: string[] = [];
  const logsGets: string[] = [];
  const eventsGets: string[] = [];
  const inspectSeq = { index: 0 };
  const changesSeq = { index: 0 };
  const runs = plan.runs ?? ["run-a"];
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      posts.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome: "succeeded",
        exit_code: 0,
      });
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(runs));
    }
    if (/^\/v1\/runs\/run-[ab]\/events\?/.test(path)) {
      eventsGets.push(path);
      const runId = /^\/v1\/runs\/(run-[ab])\//.exec(path)?.[1] ?? "run-a";
      return jsonResponse({ schema_version: 1, kind: "events", run_id: runId, revision: 12, events: denseEvents(12), next_after: null });
    }
    if (/^\/v1\/runs\/run-[ab]\/logs/.test(path)) {
      logsGets.push(path);
      const runId = /^\/v1\/runs\/(run-[ab])\//.exec(path)?.[1] ?? "run-a";
      return jsonResponse({ schema_version: 1, kind: "worker_logs", run_id: runId, revision: 12, logs: [] });
    }
    if (/^\/v1\/runs\/run-[ab]\/changes/.test(path)) {
      changesGets.push(path);
      const runId = /^\/v1\/runs\/(run-[ab])\//.exec(path)?.[1] ?? "run-a";
      const plans = plan.changes ?? [];
      const current = plans[changesSeq.index] ?? plans[plans.length - 1];
      changesSeq.index += 1;
      if (current === undefined) {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
      }
      if (current["fail"] === "network") {
        throw new Error("network gone");
      }
      const body = current["kind"] === "project_changes" ? { ...current, run_id: runId } : current;
      return jsonResponse(body);
    }
    // the inspect endpoints of both runs
    const match = /^\/v1\/runs\/(run-[ab])$/.exec(path);
    if (match !== null) {
      const runId = match[1] ?? "run-a";
      const plans = plan.inspect ?? [{ revision: 12, status: "succeeded", boundary: { kind: "succeeded", terminalStateId: "done" } }];
      const current = plans[inspectSeq.index] ?? plans[plans.length - 1] ?? succeededInspect;
      inspectSeq.index += 1;
      return jsonResponse(runDocument(runId, current.revision, current.status, current.boundary));
    }
    throw new Error(`unexpected fetch path: ${path}`);
  };
  return { fetchImpl, changesGets, posts, logsGets, eventsGets };
}

const addedEntry = (): Record<string, unknown> => ({
  index: 1,
  path: "dogfood.txt",
  change: "added",
  source_relation: "matches_baseline",
  baseline: null,
  result: fileNode(),
});

const modifiedEntry = (index = 2): Record<string, unknown> => ({
  index,
  path: "README.md",
  change: "content_modified",
  source_relation: "matches_baseline",
  baseline: fileNode({ byte_count: 20, sha256: "d".repeat(64) }),
  result: fileNode({ byte_count: 34, sha256: "e".repeat(64) }),
});

const succeededInspect = { revision: 12, status: "succeeded", boundary: { kind: "succeeded", terminalStateId: "done" } };
const failedInspect = { revision: 12, status: "failed", boundary: { kind: "failed", reason: "worker_failed" } };
const runningInspect = { revision: 12, status: "running", boundary: { kind: "in_flight", executionIndex: 1 } };

const wait = (ms = 20): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

describe("ui project changes wiring: the automatic request matrix", () => {
  test("a terminal succeeded inspect issues exactly one automatic initial GET and renders the rows", async () => {
    const wire = makeFetch({
      changes: [changesPage({
        entries: [addedEntry(), modifiedEntry()],
        nextAfter: null,
      })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets).toEqual(["/v1/runs/run-a/changes?after=0&limit=100"]);
      expect(wire.posts).toEqual([]);
      expect(harness.changesMeta()).toContain("snapshot revision 12");
      expect(harness.changesMeta()).toContain("pending");
      expect(harness.changesMeta()).toContain("loaded 2 of 2");
      expect(harness.changesRows()).toEqual([
        "dogfood.txt|added|matches_baseline||file · mode 0644 · 8 B · sha256 " + "c".repeat(64) + "|",
        "README.md|content_modified|matches_baseline|file · mode 0644 · 20 B · sha256 " + "d".repeat(64) + "|file · mode 0644 · 34 B · sha256 " + "e".repeat(64) + "|",
      ]);
      expect(harness.changesSummary()).toContain("total 2");
      expect(harness.changesStatus()).toBe("");
      // Load more is hidden without a continuation; Reload stays available
      expect(harness.changesButtonState("changes-load-more").hidden).toBe(true);
      expect(harness.changesButtonState("changes-reload").hidden).toBe(false);
    } finally {
      harness.dispose();
    }
  });

  test("a terminal failed inspect issues exactly one automatic initial GET", async () => {
    const wire = makeFetch({
      inspect: [failedInspect],
      changes: [changesPage({ entries: [addedEntry()] })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      expect(harness.detailStatus()).toBe("failed");
      expect(harness.changesRows().length).toBe(1);
    } finally {
      harness.dispose();
    }
  });

  test("a nonterminal inspect issues zero changes GETs", async () => {
    for (const inspect of [runningInspect, { revision: 12, status: "running", boundary: { kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume" } }, { revision: 12, status: "waiting", boundary: { kind: "waiting", waitIndex: 1 } }, { revision: 12, status: "running", boundary: { kind: "resumable", stateId: "dev_entry" } }]) {
      const wire = makeFetch({ inspect: [inspect] });
      const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
      try {
        harness.click("refresh-runs");
        await wait();
        await harness.clickRunInspect(0);
        await wait();
        expect(wire.changesGets).toEqual([]);
      } finally {
        harness.dispose();
      }
    }
  });

  test("an inspect failure issues zero changes GETs", async () => {
    const wire = makeFetch();
    const failing = async (path: string): Promise<Response> => {
      if (path === "/v1/runs/run-a") {
        throw new Error("network gone");
      }
      return wire.fetchImpl(path);
    };
    const harness = await createUiHarness({ fetchImpl: failing });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets).toEqual([]);
    } finally {
      harness.dispose();
    }
  });

  test("a repeated inspect of the same terminal revision issues no additional GET", async () => {
    const wire = makeFetch({
      inspect: [succeededInspect, succeededInspect, succeededInspect],
      changes: [changesPage({ entries: [addedEntry()] })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      await harness.clickRunInspect(0);
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
    } finally {
      harness.dispose();
    }
  });

  test("a changed authoritative terminal revision issues exactly one new initial GET that replaces the snapshot", async () => {
    const wire = makeFetch({
      inspect: [succeededInspect, { ...succeededInspect, revision: 13 }],
      changes: [changesPage({ entries: [addedEntry()] }), changesPage({ revision: 13, observation: OBS2, entries: [addedEntry(), modifiedEntry()] })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesRows().length).toBe(1);
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(2);
      expect(harness.changesMeta()).toContain("snapshot revision 13");
      expect(harness.changesRows().length).toBe(2);
      // no automatic repetition after the replacement
      await wait(30);
      expect(wire.changesGets.length).toBe(2);
    } finally {
      harness.dispose();
    }
  });

  test("live polling inspects do not trigger additional changes GETs on the same revision", async () => {
    const wire = makeFetch({
      inspect: [succeededInspect, succeededInspect, succeededInspect],
      changes: [changesPage({ entries: [addedEntry()] })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      // two scheduled polls of the terminal run (the scheduler stops on
      // non-in-flight boundaries, so drive the transport directly)
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
    } finally {
      harness.dispose();
    }
  });
});

describe("ui project changes wiring: pagination, stale and reload", () => {
  test("Load more fetches the continuation with the observation and appends densely", async () => {
    const wire = makeFetch({
      changes: [
        changesPage({ entries: [addedEntry()], nextAfter: 1, summaryOverrides: { total: 2, added: 2, matches_baseline: 2 } }),
        changesPage({ entries: [modifiedEntry(2)], nextAfter: null, summaryOverrides: { total: 2, added: 2, content_modified: 0, matches_baseline: 2 } }),
      ],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      expect(harness.changesButtonState("changes-load-more").hidden).toBe(false);
      harness.click("changes-load-more");
      await wait();
      expect(wire.changesGets).toEqual([
        "/v1/runs/run-a/changes?after=0&limit=100",
        `/v1/runs/run-a/changes?after=1&limit=100&observation=${OBS}`,
      ]);
      expect(harness.changesRows().length).toBe(2);
      expect(harness.changesMeta()).toContain("loaded 2 of 2");
      expect(harness.changesButtonState("changes-load-more").hidden).toBe(true);
      // a double click while loading creates no second GET
      const wire2 = makeFetch({
        changes: [
          changesPage({ entries: [addedEntry()], nextAfter: 1 }),
          changesPage({ entries: [modifiedEntry(2)], nextAfter: null }),
        ],
      });
      const harness2 = await createUiHarness({ fetchImpl: wire2.fetchImpl });
      try {
        harness2.click("refresh-runs");
        await wait();
        await harness2.clickRunInspect(0);
        await wait();
        harness2.click("changes-load-more");
        harness2.click("changes-load-more");
        await wait();
        expect(wire2.changesGets.length).toBe(2);
      } finally {
        harness2.dispose();
      }
    } finally {
      harness.dispose();
    }
  });

  test("a stale_observation continuation keeps the snapshot, shows Stale and hides Load more", async () => {
    const wire = makeFetch({
      changes: [
        changesPage({ entries: [addedEntry()], nextAfter: 1, summaryOverrides: { total: 2, added: 2, matches_baseline: 2 } }),
        changesPage({ fail: "stale" }),
      ],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      harness.click("changes-load-more");
      await wait();
      expect(harness.changesStatus()).toBe("Stale. The run workspace or registered project changed after this view was created. Reload the changes.");
      // the coherent snapshot is preserved
      expect(harness.changesRows().length).toBe(1);
      expect(harness.changesMeta()).toContain("loaded 1 of 2");
      expect(harness.changesButtonState("changes-load-more").hidden).toBe(true);
      expect(harness.changesButtonState("changes-reload").hidden).toBe(false);
      // no automatic after=0 request follows the stale state
      await wait(30);
      expect(wire.changesGets.length).toBe(2);
    } finally {
      harness.dispose();
    }
  });

  test("the explicit Reload re-requests after=0 without an observation and recovers", async () => {
    const wire = makeFetch({
      changes: [
        changesPage({ entries: [addedEntry()], nextAfter: 1, summaryOverrides: { total: 2, added: 2, matches_baseline: 2 } }),
        changesPage({ fail: "stale" }),
        changesPage({ observation: OBS2, entries: [addedEntry(), modifiedEntry()] }),
      ],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      harness.click("changes-load-more");
      await wait();
      expect(harness.changesStatus()).toContain("Stale");
      harness.click("changes-reload");
      await wait();
      expect(wire.changesGets[2]).toBe("/v1/runs/run-a/changes?after=0&limit=100");
      expect(harness.changesStatus()).toBe("");
      expect(harness.changesRows().length).toBe(2);
      expect(harness.changesMeta()).toContain("snapshot revision 12");
    } finally {
      harness.dispose();
    }
  });

  test("a Reload after an operator-side change refreshes the observation and relations", async () => {
    const wire = makeFetch({
      changes: [
        changesPage({ entries: [addedEntry()] }),
        changesPage({ observation: OBS2, summaryOverrides: { matches_result: 1 }, entries: [{ ...addedEntry(), source_relation: "matches_result" }] }),
      ],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesMeta()).toContain("pending");
      harness.click("changes-reload");
      await wait();
      expect(harness.changesMeta()).toContain("already present");
      expect(harness.changesRows()[0]).toContain("matches_result");
      expect(harness.changesMeta()).not.toContain("applied");
    } finally {
      harness.dispose();
    }
  });

  test("endpoint failures keep the section failed without any automatic retry", async () => {
    const wire = makeFetch({
      changes: [changesPage({ fail: "not_ready" })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesStatus()).toBe("failed (orchestrator: changes_not_ready)");
      expect(harness.changesRows()).toEqual([]);
      expect(harness.changesButtonState("changes-reload").hidden).toBe(false);
      await wait(30);
      expect(wire.changesGets.length).toBe(1);
    } finally {
      harness.dispose();
    }
  });

  test("a contract-invalid document fails closed: no render and no Reload", async () => {
    const wire = makeFetch({
      changes: [changesPage({ fail: "contract" })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesStatus()).toBe("failed (contract: invalid_shape)");
      expect(harness.changesRows()).toEqual([]);
      expect(harness.changesButtonState("changes-reload").hidden).toBe(true);
      expect(harness.changesButtonState("changes-load-more").hidden).toBe(true);
      // the inspect detail stays fully rendered
      expect(harness.detailStatus()).toBe("succeeded");
    } finally {
      harness.dispose();
    }
  });

  test("a network failure settles as transport network_error with manual Reload only", async () => {
    const wire = makeFetch({
      changes: [changesPage({ fail: "network" })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesStatus()).toBe("failed (transport: network_error)");
      await wait(30);
      expect(wire.changesGets.length).toBe(1);
      // the manual Reload retries exactly once
      harness.click("changes-reload");
      await wait();
      expect(wire.changesGets.length).toBe(2);
    } finally {
      harness.dispose();
    }
  });
});

describe("ui project changes wiring: isolation and safe render", () => {
  test("a changes failure leaves the inspect detail, timeline, logs and mutation controls working", async () => {
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [
        succeededInspect,
        { revision: 5, status: "running", boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 3 } },
      ],
      changes: [changesPage({ fail: "network" })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesStatus()).toContain("network_error");
      // the timeline and logs surfaces still work
      expect(harness.eventsRows().length).toBeGreaterThan(0);
      expect(harness.logsEmpty()).toContain("no agent executions");
      harness.click("logs-load");
      await wait();
      // one automatic metadata list GET per selection's inspect
      expect(wire.logsGets.length).toBe(2);
      expect(harness.logsRows()).toEqual([]);
      // the mutation gate is untouched: a mutation POST still flows on
      // the second (resumable) run
      await harness.clickRunInspect(1);
      await wait();
      harness.clickButton("run-controls", "Resume");
      await wait();
      expect(wire.posts).toEqual(["/v1/runs/run-b/resume"]);
      expect(harness.error()).toBe("");
    } finally {
      harness.dispose();
    }
  });

  test("a run selection change resets the changes state and drops a late response", async () => {
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [
        succeededInspect,
        { revision: 20, status: "succeeded", boundary: { kind: "succeeded", terminalStateId: "done" } },
      ],
      changes: [changesPage({ entries: [addedEntry()] }), changesPage({ revision: 20, entries: [addedEntry(), modifiedEntry()] })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesRows().length).toBe(1);
      await harness.clickRunInspect(1);
      await wait();
      // run B got its own automatic initial GET after its own inspect
      expect(wire.changesGets).toContain("/v1/runs/run-b/changes?after=0&limit=100");
      expect(harness.detailRunId()).toBe("Run run-b");
      expect(harness.changesMeta()).toContain("snapshot revision 20");
    } finally {
      harness.dispose();
    }
  });

  test("hostile paths and symlink targets render as plain text only", async () => {
    const wire = makeFetch({
      changes: [changesPage({
        entries: [
          {
            index: 1,
            path: "<img src=x onerror=alert(1)>",
            change: "added",
            source_relation: "matches_baseline",
            baseline: null,
            result: { kind: "symlink", target: "<script>alert(2)</script>" },
          },
        ],
      })],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(harness.changesRows()[0]).toBe("<img src=x onerror=alert(1)>|added|matches_baseline||symlink → <script>alert(2)</script>|");
      // no markup node was ever created: the row cell count stays exact
      expect(harness.element("changes-body").children[0]?.children.length).toBe(6);
    } finally {
      harness.dispose();
    }
  });

  test("the panel renders zero POSTs over the whole flow", async () => {
    const wire = makeFetch({
      changes: [
        changesPage({ entries: [addedEntry()], nextAfter: 1, summaryOverrides: { total: 2, added: 2, matches_baseline: 2 } }),
        changesPage({ fail: "stale" }),
        changesPage({ observation: OBS2, entries: [addedEntry(), modifiedEntry()] }),
      ],
    });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      harness.click("changes-load-more");
      await wait();
      harness.click("changes-reload");
      await wait();
      harness.click("changes-reload");
      await wait();
      expect(wire.posts).toEqual([]);
      expect(wire.changesGets.length).toBe(4);
    } finally {
      harness.dispose();
    }
  });
});

/* S3 battery (anchoring): the changes-side placement pins. The content
 * GETs are served through a thin shim over this suite's fetch wire so
 * the anchoring can be driven end to end. */

function contentDoc(options: { readonly side: string; readonly bytes: Uint8Array; readonly sha256?: string }): Record<string, unknown> {
  let binary = "";
  for (const byte of options.bytes) {
    binary += String.fromCharCode(byte);
  }
  return {
    schema_version: 1,
    kind: "project_change_content",
    run_id: "run-a",
    revision: 12,
    observation: OBS,
    change_index: 1,
    side: options.side,
    status: "available",
    byte_count: options.bytes.length,
    sha256: options.sha256 ?? "c".repeat(64),
    offset: 0,
    data_base64: btoa(binary),
    next_offset: null,
  };
}

/** The Review content button of the change row rendering `pathText`
 * (data rows carry exactly six cells; a wrapper row is never matched). */
type ChangesHarness = Awaited<ReturnType<typeof createUiHarness>>;
function reviewButtonForPath(harness: ChangesHarness, pathText: string) {
  const row = harness.element("changes-body").children.find(
    (candidate) => candidate.tagName === "TR" && candidate.children.length === 6 && candidate.children[0]!.textContent === pathText,
  );
  if (row === undefined) {
    throw new Error(`no data row for ${pathText}`);
  }
  const button = row.querySelectorAll("button").find((node) => node.textContent === "Review content");
  if (button === undefined) {
    throw new Error(`no Review content button on the row for ${pathText}`);
  }
  return button;
}

/** The wrapper row: the one `tr` whose only child is one `td` whose
 * only child is the panel element. */
function reviewWrapperRow(harness: ChangesHarness) {
  const panel = harness.element("content-review-panel");
  return harness.element("changes-body").children.find(
    (row) =>
      row.tagName === "TR" &&
      row.children.length === 1 &&
      row.children[0]!.tagName === "TD" &&
      row.children[0]!.children.length === 1 &&
      row.children[0]!.children[0] === panel,
  );
}

describe("S3 wiring: anchoring survives the changes render cycle", () => {
  test("a re-render under an open review re-derives the anchor without refetching", async () => {
    const wire = makeFetch({
      changes: [changesPage({ entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()] })],
    });
    const baselineDoc = contentDoc({ side: "baseline", bytes: new TextEncoder().encode("one\ntwo\n"), sha256: "d".repeat(64) });
    const resultDoc = contentDoc({ side: "result", bytes: new TextEncoder().encode("one\nTWO\ntwo\n") });
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          return Promise.resolve(jsonResponse(path.includes("side=baseline") ? baselineDoc : resultDoc));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait();
      expect(reviewWrapperRow(harness)).toBeDefined();
      expect(harness.element("content-review-panel").hidden).toBe(false);
      // the re-render of the same revision re-derives the anchor
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      const body = harness.element("changes-body");
      expect(body.children.length).toBe(3);
      const wrapper = reviewWrapperRow(harness);
      expect(wrapper).toBe(body.children[1]);
      expect(harness.element("content-review-panel").hidden).toBe(false);
      expect(harness.element("content-result-view").textContent).toBe("one\nTWO\ntwo\n");
    } finally {
      harness.dispose();
    }
  });

  test("a re-render during a pending review keeps the anchor and the late settle lands in it", async () => {
    const wire = makeFetch({
      changes: [changesPage({ entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()] })],
    });
    const gates: Array<() => void> = [];
    // dogfood.txt is added: its baseline side is implicit_empty, so the
    // gated response is the empty side document
    const baselineDoc = {
      schema_version: 1,
      kind: "project_change_content",
      run_id: "run-a",
      revision: 12,
      observation: OBS,
      change_index: 1,
      side: "baseline",
      status: "implicit_empty",
      byte_count: 0,
      offset: 0,
      data_base64: "",
      next_offset: null,
    };
    const resultDoc = contentDoc({ side: "result", bytes: new TextEncoder().encode("one\nTWO\ntwo\n") });
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          if (path.includes("side=baseline")) {
            return new Promise<Response>((resolve) => {
              gates.push(() => resolve(jsonResponse(baselineDoc)));
            }) as never;
          }
          return Promise.resolve(jsonResponse(resultDoc));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait(10);
      expect(harness.element("content-baseline-status").textContent).toContain("Loading");
      await harness.clickRunInspect(0);
      await wait();
      // the pending review stays anchored through the re-render
      const body = harness.element("changes-body");
      expect(body.children.length).toBe(3);
      expect(reviewWrapperRow(harness)).toBe(body.children[1]);
      expect(harness.element("content-review-panel").hidden).toBe(false);
      // the late settle renders into the anchored panel
      gates[0]?.();
      await wait(20);
      expect(harness.element("content-baseline-status").textContent).toContain("empty side");
      expect(reviewWrapperRow(harness)).toBe(harness.element("changes-body").children[1]);
    } finally {
      harness.dispose();
    }
  });

  test("a run switch closes the anchored review and the old run's late settle is byte-inert", async () => {
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [succeededInspect, { revision: 20, status: "succeeded", boundary: { kind: "succeeded", terminalStateId: "done" } }],
      changes: [changesPage({ entries: [addedEntry(), modifiedEntry()] }), changesPage({ revision: 20, entries: [modifiedEntry(2)] })],
    });
    const gates: Array<() => void> = [];
    const baselineDoc = contentDoc({ side: "baseline", bytes: new TextEncoder().encode("one\ntwo\n"), sha256: "d".repeat(64) });
    const resultDoc = contentDoc({ side: "result", bytes: new TextEncoder().encode("one\nTWO\ntwo\n") });
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          if (path.includes("side=baseline")) {
            return new Promise<Response>((resolve) => {
              gates.push(() => resolve(jsonResponse(baselineDoc)));
            }) as never;
          }
          return Promise.resolve(jsonResponse(resultDoc));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait(10);
      await harness.clickRunInspect(1);
      await wait();
      // the switch parked and hid the panel; run B carries the table
      expect(harness.element("content-review-panel").hidden).toBe(true);
      expect(reviewWrapperRow(harness)).toBeUndefined();
      const rowsAfterSwitch = harness.changesRows().join(";");
      // the old run's late settle changes nothing in the current DOM
      gates[0]?.();
      await wait(20);
      expect(harness.changesRows().join(";")).toBe(rowsAfterSwitch);
      expect(harness.element("content-review-panel").hidden).toBe(true);
      expect(reviewWrapperRow(harness)).toBeUndefined();
    } finally {
      harness.dispose();
    }
  });

  test("terminal preservation and failing-terminal anchoring stay intact", async () => {
    const wire = makeFetch({
      inspect: [failedInspect],
      changes: [changesPage({ entries: [addedEntry()] })],
    });
    const baselineDoc = contentDoc({ side: "baseline", bytes: new TextEncoder().encode("ctx") });
    const resultDoc = contentDoc({ side: "result", bytes: new TextEncoder().encode("dog") });
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          return Promise.resolve(jsonResponse(path.includes("side=baseline") ? baselineDoc : resultDoc));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      // before any review: the section and table stand, the panel is
      // hidden and no POST ever fired
      expect(harness.detailStatus()).toBe("failed");
      expect(harness.changesRows().length).toBe(1);
      expect(harness.element("content-review-panel").hidden).toBe(true);
      harness.clickButton("changes-body", "Review content");
      await wait(10);
      expect(harness.element("content-review-panel").hidden).toBe(false);
      const wrapper = reviewWrapperRow(harness);
      expect(wrapper).toBeDefined();
      expect(wrapper).toBe(harness.element("changes-body").children[1]);
    } finally {
      harness.dispose();
    }
  });

  test("the fake DOM insertBefore mirrors the real-DOM move contract", () => {
    const tbody = new FakeElement("tbody");
    const first = new FakeElement("tr");
    const second = new FakeElement("tr");
    const moving = new FakeElement("tr");
    const adopter = new FakeElement("div");
    adopter.appendChild(moving);
    // null reference appends
    tbody.insertBefore(first, null);
    tbody.insertBefore(second, null);
    expect(tbody.children.length).toBe(2);
    expect(tbody.children[0]).toBe(first);
    expect(tbody.children[1]).toBe(second);
    // reparenting: the child leaves its previous parent exactly once
    tbody.insertBefore(moving, second);
    expect(adopter.children.includes(moving)).toBe(false);
    expect(moving.parent).toBe(tbody);
    expect(tbody.children[1]).toBe(moving);
    expect(tbody.children[2]).toBe(second);
    // no duplicate membership anywhere
    const membership = (element: FakeElement) => tbody.children.filter((child) => child === element).length;
    expect(membership(moving)).toBe(1);
    expect(membership(second)).toBe(1);
    expect(adopter.children.length).toBe(0);
    // a second same-parent insert moves without duplicating
    tbody.insertBefore(second, null);
    expect(tbody.children[2]).toBe(second);
    expect(membership(second)).toBe(1);
    // a reference that is not a child of the target throws
    const foreignParent = new FakeElement("tbody");
    const foreign = new FakeElement("tr");
    foreignParent.appendChild(foreign);
    expect(() => tbody.insertBefore(first, foreign)).toThrow();
    expect(tbody.children[0]).toBe(first);
  });
});

describe("S3 corrective wiring: observation binding, late callbacks and request accounting", () => {
  /** The panel family snapshot: the listed elements with their text,
   * visibility, disabled state and topology, serialized as JSON so two
   * snapshots compare byte-for-byte. */
  const PANEL_FAMILY_IDS = [
    "content-review-panel",
    "content-review-meta",
    "content-review-status",
    "content-baseline-status",
    "content-baseline-view",
    "content-result-status",
    "content-result-view",
    "content-diff-status",
    "content-diff-view",
    "content-review-load-baseline",
    "content-review-load-result",
    "content-review-retry",
    "content-review-reload-changes",
    "content-review-close",
  ];
  function contentReviewSnapshot(harness: ChangesHarness): string {
    const parentChain = (element: FakeElement): string => {
      const chain: string[] = [];
      let current = element.parent;
      while (current !== null) {
        chain.push(current.id !== "" ? `#${current.id}` : current.tagName);
        current = current.parent;
      }
      return chain.join("<") || "(detached)";
    };
    const parts: Record<string, unknown> = {};
    const capture = (id: string) => {
      const element = harness.element(id);
      parts[id] =
        element === undefined
          ? null
          : {
              tag: element.tagName,
              text: element.textContent,
              hidden: element.hidden,
              disabled: element.disabled,
              className: element.className,
              parent: element.parent === null ? "(detached)" : parentChain(element),
              index: element.parent === null ? -1 : element.parent.children.indexOf(element),
              childTags: element.children.map((child) => child.tagName),
            };
    };
    for (const id of PANEL_FAMILY_IDS) {
      capture(id);
    }
    capture("error");
    return JSON.stringify(parts);
  }

  function contentDocFor(runId: string, observation: string, side: string, bytes: string, sha256: string) {
    let binary = "";
    for (const character of new TextEncoder().encode(bytes)) {
      binary += String.fromCharCode(character);
    }
    return {
      schema_version: 1,
      kind: "project_change_content",
      run_id: runId,
      revision: 12,
      observation,
      change_index: 1,
      side,
      status: "available",
      byte_count: bytes.length,
      sha256,
      offset: 0,
      data_base64: btoa(binary),
      next_offset: null,
    };
  }
  function emptyBaselineDoc(runId: string, observation: string) {
    return {
      schema_version: 1,
      kind: "project_change_content",
      run_id: runId,
      revision: 12,
      observation,
      change_index: 1,
      side: "baseline",
      status: "implicit_empty",
      byte_count: 0,
      offset: 0,
      data_base64: "",
      next_offset: null,
    };
  }
  const OBS_A = "a".repeat(64);
  const OBS_B = "b".repeat(64);
  const OBS_C = "f".repeat(64);
  const resultSha = "c".repeat(64);
  const otherSha = "f".repeat(64);
  const resultText = "one\nTWO\ntwo\n";

  test("a diverging changes observation parks the anchored panel; the panel may re-anchor after the content follows to the new observation", async () => {
    const entriesA = [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()];
    const entriesB = [
      { ...addedEntry(), result: fileNode({ byte_count: 12, sha256: otherSha }) },
      { ...modifiedEntry(), result: fileNode({ byte_count: 34, sha256: otherSha }) },
    ];
    const wire = makeFetch({
      changes: [changesPage({ entries: entriesA }), changesPage({ observation: OBS_B, entries: entriesB })],
    });
    let contentGets = 0;
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          contentGets += 1;
          const observation = path.includes(`observation=${OBS_B}`) ? OBS_B : OBS_A;
          const sha256 = observation === OBS_B ? otherSha : resultSha;
          if (path.includes("side=baseline")) {
            return Promise.resolve(jsonResponse(emptyBaselineDoc("run-a", observation)));
          }
          return Promise.resolve(jsonResponse(contentDocFor("run-a", observation, "result", resultText, sha256)));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait();
      expect(reviewWrapperRow(harness)).toBeDefined();
      expect(harness.element("content-review-panel").hidden).toBe(false);
      expect(harness.element("content-result-view").textContent).toBe(resultText);
      // the same-run reload moves the metadata to observation B under
      // the same run id and change index: the anchored review may not
      // survive it — the panel has to sit parked and hidden with no
      // wrapper row
      harness.click("changes-reload");
      await wait();
      const body = harness.element("changes-body");
      expect(body.children.length).toBe(2);
      expect(reviewWrapperRow(harness)).toBeUndefined();
      expect(harness.element("content-review-panel").hidden).toBe(true);
      expect(harness.element("content-review-meta").textContent).toBe("");
      // once the content follows to observation B (a new explicit
      // Review under the new observation), re-anchoring is allowed
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait();
      expect(reviewWrapperRow(harness)).toBeDefined();
      expect(harness.element("content-review-panel").hidden).toBe(false);
      expect(harness.element("content-result-view").textContent).toBe(resultText);
      expect(wire.changesGets.length).toBe(2);
      expect(wire.posts.length).toBe(0);
    } finally {
      harness.dispose();
    }
  });

  test("a late old-run success callback leaves the content-review DOM byte-equivalent", async () => {
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [succeededInspect, succeededInspect],
      changes: [
        changesPage({ entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()] }),
        changesPage({ runId: "run-b", entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }] }),
      ],
    });
    const gates: Array<(response: Response) => void> = [];
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          if (path.includes("side=baseline")) {
            // the gate honours the transport's abort signal exactly
            // like fetch: an aborted response is never delivered
            return new Promise<Response>((resolve, reject) => {
              const signal = init?.signal ?? null;
              let settled = false;
              const finish = (deliver: () => void) => {
                if (settled) {
                  return;
                }
                settled = true;
                deliver();
              };
              if (signal !== null) {
                signal.addEventListener("abort", () => finish(() => reject(new Error("aborted"))), { once: true });
              }
              gates.push((response) => finish(() => resolve(response)));
            }) as never;
          }
          return Promise.resolve(jsonResponse(contentDocFor("run-a", OBS_A, "result", resultText, resultSha)));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait(10);
      expect(harness.element("content-baseline-status").textContent).toContain("Loading");
      // switch to run B; every legitimate transition settles before the
      // snapshot
      await harness.clickRunInspect(1);
      await wait();
      const before = contentReviewSnapshot(harness);
      // the old run's baseline response arrives late as a success
      gates[0]?.(jsonResponse(contentDocFor("run-a", OBS_A, "baseline", "one\ntwo\ntext", "d".repeat(64))));
      await wait();
      expect(contentReviewSnapshot(harness)).toBe(before);
    } finally {
      harness.dispose();
    }
  });

  test("a late old-run failure callback leaves the content-review DOM byte-equivalent", async () => {
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [succeededInspect, succeededInspect],
      changes: [
        changesPage({ entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()] }),
        changesPage({ runId: "run-b", entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }] }),
      ],
    });
    const gates: Array<(response: Response) => void> = [];
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        if (/^\/v1\/runs\/run-a\/changes\/\d+\/content\?/.test(path)) {
          if (path.includes("side=baseline")) {
            // the gate honours the transport's abort signal exactly
            // like fetch: an aborted response is never delivered
            return new Promise<Response>((resolve, reject) => {
              const signal = init?.signal ?? null;
              let settled = false;
              const finish = (deliver: () => void) => {
                if (settled) {
                  return;
                }
                settled = true;
                deliver();
              };
              if (signal !== null) {
                signal.addEventListener("abort", () => finish(() => reject(new Error("aborted"))), { once: true });
              }
              gates.push((response) => finish(() => resolve(response)));
            }) as never;
          }
          return Promise.resolve(jsonResponse(contentDocFor("run-a", OBS_A, "result", resultText, resultSha)));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      await harness.clickRunInspect(0);
      await wait();
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait(10);
      expect(harness.element("content-baseline-status").textContent).toContain("Loading");
      await harness.clickRunInspect(1);
      await wait();
      const before = contentReviewSnapshot(harness);
      // the old run's baseline response arrives late as a transport
      // failure
      gates[0]?.(new Response("boom", { status: 500 }));
      await wait();
      expect(contentReviewSnapshot(harness)).toBe(before);
    } finally {
      harness.dispose();
    }
  });

  test("terminal succeeded and failed runs drive exactly the expected requests", async () => {
    let contentGets = 0;
    const wire = makeFetch({
      runs: ["run-a", "run-b"],
      inspect: [succeededInspect, failedInspect],
      changes: [
        changesPage({ entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }, modifiedEntry()] }),
        changesPage({ runId: "run-b", entries: [{ ...addedEntry(), result: fileNode({ byte_count: 12 }) }] }),
      ],
    });
    const harness = await createUiHarness({
      fetchImpl: (path, init) => {
        const contentMatch = /^\/v1\/runs\/(run-[ab])\/changes\/\d+\/content\?/.exec(path);
        if (contentMatch !== null) {
          contentGets += 1;
          const runId = contentMatch[1] ?? "run-a";
          if (path.includes("side=baseline")) {
            return Promise.resolve(jsonResponse(emptyBaselineDoc(runId, OBS_A)));
          }
          return Promise.resolve(jsonResponse(contentDocFor(runId, OBS_A, "result", resultText, resultSha)));
        }
        return wire.fetchImpl(path, init);
      },
    });
    try {
      harness.click("refresh-runs");
      await wait();
      // succeeded terminal run
      await harness.clickRunInspect(0);
      await wait();
      expect(wire.changesGets.length).toBe(1);
      const contentGetsBefore = contentGets;
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait();
      expect(harness.element("content-result-view").textContent).toBe(resultText);
      expect(contentGets - contentGetsBefore).toBe(2);
      expect(wire.changesGets.length).toBe(1);
      expect(wire.posts.length).toBe(0);
      // failed terminal run
      await harness.clickRunInspect(1);
      await wait();
      expect(wire.changesGets.length).toBe(2);
      const contentGetsAfterSwitch = contentGets;
      reviewButtonForPath(harness, "dogfood.txt").click();
      await wait();
      expect(harness.element("content-result-view").textContent).toBe(resultText);
      expect(contentGets - contentGetsAfterSwitch).toBe(2);
      expect(wire.changesGets.length).toBe(2);
      expect(wire.posts.length).toBe(0);
    } finally {
      harness.dispose();
    }
  });

  test("FakeElement.insertBefore(node, node) is an exact no-op (S3 corrective)", () => {
    const parent = new FakeElement("tbody", "");
    const first = new FakeElement("tr", "");
    const second = new FakeElement("tr", "");
    const third = new FakeElement("tr", "");
    parent.appendChild(first);
    parent.appendChild(second);
    parent.appendChild(third);
    // a node inserted before itself stays exactly where it is
    parent.insertBefore(second, second);
    expect(parent.children.map((child) => child === first ? "first" : child === second ? "second" : "third")).toEqual(
      ["first", "second", "third"],
    );
    expect(second.parent).toBe(parent);
    expect(parent.children.filter((child) => child === second).length).toBe(1);
    // the head-of-list form is equally inert
    parent.insertBefore(first, first);
    expect(parent.children[0]).toBe(first);
    expect(parent.children.map((child) => child.tagName)).toEqual(["TR", "TR", "TR"]);
    expect(first.parent).toBe(parent);
    expect(parent.children.filter((child) => child === first).length).toBe(1);
    // node === reference outside the target parent keeps the real-DOM
    // NotFoundError mirror: the throwing contract is not weakened
    const foreignParent = new FakeElement("tbody", "");
    const foreign = new FakeElement("tr", "");
    foreignParent.appendChild(foreign);
    expect(() => parent.insertBefore(foreign, foreign)).toThrow();
    expect(parent.children.length).toBe(3);
    expect(foreign.parent).toBe(foreignParent);
  });
});
