/**
 * Permanent wiring suite of the browser UI v8 Project apply panel: the
 * fake-DOM harness boots the shipped `app.js` and drives the production
 * call chains (run selection -> inspect -> the automatic changes load
 * -> eligibility -> Review -> Confirm -> the gated POST -> the applied
 * reload -> the record removal; recovery on re-selection; lost
 * responses; stale refusals) with an instrumented fetch, the real
 * endpoint documents and a controllable in-memory storage.
 */
import { describe, expect, test } from "bun:test";
import { APPLY_RECORD_STORAGE_PREFIX } from "../ui/app.js";
import {
  createUiHarness,
  jsonResponse,
  runDocument,
  runsPageDocument,
} from "./ui_dom_harness.ts";

const OBS = "a".repeat(64);
const OBS2 = "b".repeat(64);

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
  readonly entries?: Array<Record<string, unknown>>;
  readonly summaryOverrides?: Record<string, number>;
}): Record<string, unknown> {
  const entries = options.entries ?? [];
  const counts: Record<string, number> = {};
  for (const entry of entries) {
    counts[entry["change"] as string] = (counts[entry["change"] as string] ?? 0) + 1;
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
  return {
    schema_version: 1,
    kind: "project_changes",
    run_id: options.runId ?? "run-a",
    revision: options.revision ?? 12,
    observation: options.observation ?? OBS,
    summary,
    changes: entries,
    next_after: null,
  };
}

const pendingEntry = (path: string): Record<string, unknown> => ({
  index: 1,
  path,
  change: "content_modified",
  source_relation: "matches_baseline",
  baseline: fileNode(),
  result: fileNode({ sha256: "d".repeat(64) }),
});

const presentEntry = (path: string, index: number): Record<string, unknown> => ({
  index,
  path,
  change: "mode_modified",
  source_relation: "matches_result",
  baseline: fileNode(),
  result: fileNode(),
});

function operationDocument(status: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "project_apply_operation",
    run_id: "run-a",
    apply_id: OBS,
    project_id: "project-a",
    observation: OBS,
    revision: 17,
    status,
    completed_step_count: 0,
    total_step_count: 3,
    ...overrides,
  };
}

interface ApplyPlan {
  /** The documents the operation POST returns, in order. */
  readonly posts?: Array<Record<string, unknown>>;
  /** The documents the operation GET returns, in order. */
  readonly gets?: Array<Record<string, unknown>>;
  /** The changes pages, in request order. */
  readonly changes?: Array<Record<string, unknown>>;
  /** When true the apply POST throws after being recorded. */
  readonly lostPost?: boolean;
  readonly projectsApply?: boolean;
  readonly runStatus?: string;
  /** When set, a POST marked `hold: true` waits on this gate. */
  readonly postGate?: { readonly promise: Promise<void> };
}

function makeFetch(plan: ApplyPlan = {}) {
  const changesGets: string[] = [];
  const applyPosts: Array<{ path: string; body: string }> = [];
  const applyGets: string[] = [];
  const inspectGets: string[] = [];
  const runMutationPosts: string[] = [];
  const changesSeq = { index: 0 };
  const applyPostSeq = { index: 0 };
  const applyGetSeq = { index: 0 };
  const postGate = plan.postGate ?? null;
  const capability = plan.projectsApply ?? false;
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      if (path.includes("/project-applies/")) {
        applyPosts.push({ path, body: String(init?.body ?? "") });
        if (plan.lostPost === true) {
          throw new Error("network gone");
        }
        const plans = plan.posts ?? [];
        const current = plans[applyPostSeq.index] ?? plans[plans.length - 1];
        applyPostSeq.index += 1;
        if (current === undefined) {
          return jsonResponse({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_disabled" });
        }
        if (current["hold"] === true) {
          await postGate?.promise;
          const { hold: _hold, ...document } = current;
          return jsonResponse(document);
        }
        return jsonResponse(current);
      }
      runMutationPosts.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "command_result",
        run_id: "run-a",
        outcome: "succeeded",
        exit_code: 0,
      });
    }
    if (path === "/v1/projects") {
      return jsonResponse({
        schema_version: 2,
        kind: "projects",
        projects: [{ id: "project-a", input_roots: [{ id: "tasks" }], project_apply: capability }],
      });
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(["run-a", "run-b"]));
    }
    if (path.includes("/project-applies/")) {
      applyGets.push(path);
      const plans = plan.gets ?? [];
      const current = plans[applyGetSeq.index] ?? plans[plans.length - 1];
      applyGetSeq.index += 1;
      if (current === undefined) {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_not_found" });
      }
      return jsonResponse(current);
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
      return jsonResponse({ ...current, run_id: runId });
    }
    const match = /^\/v1\/runs\/(run-[ab])$/.exec(path);
    if (match !== null) {
      inspectGets.push(path);
      return jsonResponse(runDocument(match[1] ?? "run-a", 12, plan.runStatus ?? "succeeded", { kind: "succeeded", terminalStateId: "done" }));
    }
    if (/^\/v1\/runs\/run-[ab]\/events\?/.test(path)) {
      return jsonResponse({ schema_version: 1, kind: "events", run_id: "run-a", revision: 12, events: [], next_after: null });
    }
    if (/^\/v1\/runs\/run-[ab]\/logs/.test(path)) {
      return jsonResponse({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 12, logs: [] });
    }
    throw new Error(`unexpected fetch path: ${path}`);
  };
  return { fetchImpl, changesGets, applyPosts, applyGets, inspectGets, runMutationPosts };
}

function applyPostAt(wire: ReturnType<typeof makeFetch>, index: number): { path: string; body: string } {
  const entry = wire.applyPosts[index];
  if (entry === undefined) throw new Error(`missing apply post at index ${index}`);
  return entry;
}

const wait = (ms = 20): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function bootWith(plan: ApplyPlan = {}, storage?: Map<string, string>) {
  const wire = makeFetch(plan);
  const harness = await createUiHarness({ fetchImpl: wire.fetchImpl, storage });
  harness.click("refresh-projects");
  await wait();
  harness.click("refresh-runs");
  await wait();
  return { harness, wire };
}

async function selectTerminalRun(harness: Awaited<ReturnType<typeof createUiHarness>>): Promise<void> {
  harness.clickRunInspect(0);
  await wait();
}

describe("project apply wiring: eligibility and the review confirmation", () => {
  test("capability false keeps the review control hidden with zero apply traffic", async () => {
    const { harness, wire } = await bootWith({ changes: [changesPage({ entries: [pendingEntry("a.txt")] })] });
    await selectTerminalRun(harness);
    await wait();
    expect(harness.applyButtonState("project-apply-review").hidden).toBe(true);
    expect(wire.applyPosts).toHaveLength(0);
    expect(wire.applyGets).toHaveLength(0);
    harness.dispose();
  });

  test("a terminal run with a fully loaded pending snapshot shows the review control", async () => {
    const { harness, wire } = await bootWith({
      projectsApply: true,
      changes: [changesPage({ entries: [pendingEntry("a.txt"), presentEntry("b.txt", 2)] })],
    });
    await selectTerminalRun(harness);
    await wait();
    expect(wire.changesGets.length).toBeGreaterThanOrEqual(1);
    expect(harness.applyButtonState("project-apply-review").hidden).toBe(false);
    expect(wire.applyPosts).toHaveLength(0);
    harness.dispose();
  });

  test("the first review press shows the inline confirmation with zero POST", async () => {
    const { harness, wire } = await bootWith({
      projectsApply: true,
      changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
    });
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    expect(harness.applyConfirmationVisible()).toBe(true);
    const details = harness.applyConfirmation();
    expect(details).toContain("run-a");
    expect(details).toContain(OBS);
    expect(details).toContain("1 pending");
    expect(details).toContain("the registered project will be modified");
    expect(details).toContain("not whole-tree atomic");
    expect(details).toContain("IDE or git writers");
    expect(wire.applyPosts).toHaveLength(0);
    // cancel: zero effects
    harness.click("project-apply-cancel");
    await wait();
    expect(harness.applyConfirmationVisible()).toBe(false);
    expect(wire.applyPosts).toHaveLength(0);
    harness.dispose();
  });

  test("a partially loaded snapshot keeps the apply disabled", async () => {
    const entries = Array.from({ length: 3 }, (_, index) => presentEntry(`f${index}.txt`, index + 1));
    const { harness } = await bootWith({
      projectsApply: true,
      changes: [{ ...changesPage({ entries }), next_after: 3 }],
    });
    await selectTerminalRun(harness);
    await wait();
    expect(harness.applyButtonState("project-apply-review").hidden).toBe(true);
    harness.dispose();
  });

  test("a diverged snapshot keeps the apply disabled", async () => {
    const { harness } = await bootWith({
      projectsApply: true,
      changes: [changesPage({ entries: [pendingEntry("a.txt")], summaryOverrides: { diverged: 1 } })],
    });
    await selectTerminalRun(harness);
    await wait();
    expect(harness.applyButtonState("project-apply-review").hidden).toBe(true);
    harness.dispose();
  });

  test("a snapshot without pending changes keeps the apply disabled", async () => {
    const { harness } = await bootWith({
      projectsApply: true,
      changes: [changesPage({ entries: [presentEntry("b.txt", 1)] })],
    });
    await selectTerminalRun(harness);
    await wait();
    expect(harness.applyButtonState("project-apply-review").hidden).toBe(true);
    harness.dispose();
  });
});

describe("project apply wiring: the confirm POST and the applied reload", () => {
  test("confirm writes the recovery record before exactly one POST and shows the status", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [
          changesPage({ entries: [pendingEntry("a.txt")] }),
          changesPage({ entries: [presentEntry("a.txt", 1)], observation: OBS2, summaryOverrides: { matches_result: 1 } }),
        ],
        posts: [operationDocument("applying")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    expect(wire.applyPosts).toHaveLength(1);
    expect(applyPostAt(wire, 0).path).toBe(`/v1/runs/run-a/project-applies/${OBS}`);
    expect(JSON.parse(applyPostAt(wire, 0).body)).toEqual({ schema_version: 1, observation: OBS });
    expect(storage.get(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(
      JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }),
    );
    expect(harness.applyStatus()).toContain("applying");
    harness.dispose();
  });

  test("an applied POST triggers exactly one explicit changes reload and removes the record", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [
          changesPage({ entries: [pendingEntry("a.txt")] }),
          changesPage({ entries: [presentEntry("a.txt", 1)], observation: OBS2, summaryOverrides: { matches_result: 1 } }),
        ],
        posts: [operationDocument("applied")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    // the explicit reload ran exactly once more (two changes GETs total)
    // and the confirming snapshot removed the record back to idle
    expect(wire.changesGets).toHaveLength(2);
    expect(wire.changesGets[1]).toBe(`/v1/runs/run-a/changes?after=0&limit=100`);
    expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(false);
    expect(harness.applyStatus()).toBe("");
    expect(harness.applyMeta()).toBe("");
    harness.dispose();
  });

  test("a failed reload after applied keeps the applied status and the record", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [
          changesPage({ entries: [pendingEntry("a.txt")] }),
          { schema_version: 1, kind: "error", layer: "transport", reason: "network_error" },
        ],
        posts: [operationDocument("applied")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    expect(harness.applyStatus()).toContain("applied");
    expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    void wire;
    harness.dispose();
  });

  test("a lost POST response shows network_unknown and the retry reuses the identity", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        lostPost: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    expect(harness.applyStatus()).toContain("unknown");
    expect(wire.applyPosts).toHaveLength(1);
    harness.click("project-apply-retry");
    await wait();
    expect(wire.applyPosts).toHaveLength(2);
    expect(applyPostAt(wire, 1).path).toBe(`/v1/runs/run-a/project-applies/${OBS}`);
    harness.dispose();
  });

  test("a stale refusal clears the confirmation and the provably uncreated record", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        posts: [{ schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" }],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    expect(harness.applyStatus()).toContain("stale_observation");
    expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(false);
    expect(wire.changesGets).toHaveLength(1);
    harness.dispose();
  });

  test("a double confirm creates exactly one POST", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        posts: [operationDocument("applying")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    harness.click("project-apply-confirm");
    await wait();
    expect(wire.applyPosts).toHaveLength(1);
    harness.dispose();
  });

  test("the mutation gate serializes the apply against run mutations", async () => {
    let gateResolve: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      gateResolve = resolve;
    });
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        posts: [{ ...operationDocument("applying"), hold: true }],
        postGate: { promise: gate },
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.element("submit-run").className = "mutation-control";
    harness.element("project-apply-review").className = "mutation-control";
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    await wait();
    // while the apply POST is unsettled the run mutation controls are
    // disabled by the shared control set
    expect(harness.element("submit-run").disabled).toBe(true);
    expect(harness.element("project-apply-review").disabled).toBe(true);
    expect(wire.applyPosts).toHaveLength(1);
    gateResolve();
    await wait();
    expect(harness.element("submit-run").disabled).toBe(false);
    expect(harness.applyStatus()).toContain("Operation applying");
    harness.dispose();
  });
});

describe("project apply wiring: recovery on load and selection changes", () => {
  test("an existing record triggers exactly one operation GET after the changes load", async () => {
    const storage = new Map<string, string>();
    storage.set(
      `${APPLY_RECORD_STORAGE_PREFIX}run-a`,
      JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }),
    );
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        gets: [operationDocument("recovery_required")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    expect(wire.applyGets).toHaveLength(1);
    expect(wire.applyGets[0]).toBe(`/v1/runs/run-a/project-applies/${OBS}`);
    expect(wire.applyPosts).toHaveLength(0);
    expect(harness.applyStatus()).toContain("recovery");
    // the explicit continuation POSTs the same identity
    harness.click("project-apply-continue");
    await wait();
    expect(wire.applyPosts).toHaveLength(1);
    expect(JSON.parse(applyPostAt(wire, 0).body)).toEqual({ schema_version: 1, observation: OBS });
    harness.dispose();
  });

  test("a lost durable operation shows the 404 and keeps the record", async () => {
    const storage = new Map<string, string>();
    storage.set(
      `${APPLY_RECORD_STORAGE_PREFIX}run-a`,
      JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }),
    );
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        gets: [],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    expect(harness.applyStatus()).toContain("project_apply_not_found");
    expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    void wire;
    harness.dispose();
  });

  test("a selection change preserves the record and drops the late response", async () => {
    const storage = new Map<string, string>();
    const { harness, wire } = await bootWith(
      {
        projectsApply: true,
        changes: [changesPage({ entries: [pendingEntry("a.txt")] })],
        posts: [operationDocument("applying")],
      },
      storage,
    );
    await selectTerminalRun(harness);
    await wait();
    harness.click("project-apply-review");
    await wait();
    harness.click("project-apply-confirm");
    harness.clickRunInspect(1);
    await wait();
    expect(storage.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(harness.applyStatus()).not.toContain("applying");
    void wire;
    harness.dispose();
  });
});

async function flush(): Promise<void> {
  for (let index = 0; index < 6; index += 1) {
    await Promise.resolve();
  }
}
