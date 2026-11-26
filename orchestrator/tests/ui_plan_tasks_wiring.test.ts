/**
 * Permanent wiring suite of the browser UI "Plan and tasks" slice (S6):
 * the fake-DOM harness boots the shipped `app.js` and drives the
 * production call chains (run selection -> applied inspect -> the
 * pinned identity-ledger GET -> the explicit ledger Open selections ->
 * the content chunk/page GETs -> the manual controls), with an
 * instrumented fetch and the real endpoint documents. Every test
 * proves the pinned request shapes, the single-flight disable window,
 * the safe text-only render and the isolation from the inspect, events,
 * logs, changes and mutation surfaces.
 */
import { describe, expect, test } from "bun:test";
import { stageTaskBodyDigest } from "../src/pipeline_v2_stage_task.ts";
import {
  createUiHarness,
  jsonResponse,
  projectsDocument,
  runDocument,
  runsPageDocument,
  type FakeElement,
} from "./ui_dom_harness.ts";

const RUN_A = "run-pt-first";
const RUN_B = "run-pt-second";
const sha = (char: string) => char.repeat(64);
const b64 = (text: string) => Buffer.from(text, "utf8").toString("base64");
const BODY_TEXT_A = "task body one";
const BODY_TEXT_B = " task body two";

/** The identity-ledger records of the pinned fixture (run selection
 * revision 5): plan r1, task-alpha@1, plan r2, task-beta@2. */
const IDENTITY_RECORDS = [
  { record: "plan_revision", index: 1, revision: 1, sha256: sha("3"), previous_sha256: null, origin_execution: 4 },
  { record: "task_revision", index: 2, task_id: "task-alpha", revision: 1, sha256: sha("1"), previous_sha256: null, wait_index: null, intent_sha256: null },
  { record: "plan_revision", index: 3, revision: 2, sha256: sha("4"), previous_sha256: sha("3"), origin_execution: 8 },
  { record: "task_revision", index: 4, task_id: "task-beta", revision: 2, sha256: sha("2"), previous_sha256: sha("1"), wait_index: null, intent_sha256: null },
];

interface DeferredRoute {
  readonly gate: { promise: Promise<Response>; resolve: (model: unknown) => void };
}

type DeferredEntry = DeferredRoute | Record<string, unknown>;

interface FetchPlan {
  readonly inspect?: Array<Record<string, unknown>>;
  /** Identity pages keyed by the requested `after` cursor; the two
   * identity consumers (lifecycle history and plan/tasks) share the
   * endpoint and each requests its own page by cursor. */
  readonly identityPages?: Record<string, DeferredEntry>;
  readonly task?: Array<DeferredEntry>;
  readonly plan?: Array<DeferredEntry>;
}

function deferred(): DeferredRoute["gate"] & {
  settle: (model: unknown) => void;
} {
  let resolve!: (model: unknown) => void;
  const promise = new Promise<Response>((accept) => {
    resolve = (model: unknown) => accept(jsonResponse(model as Record<string, unknown>));
  });
  return { promise, resolve, settle: resolve };
}

function makeFetch(plan: FetchPlan = {}) {
  const identityGets: string[] = [];
  const taskGets: string[] = [];
  const planGets: string[] = [];
  const eventsGets: string[] = [];
  const logsGets: string[] = [];
  const changesGets: string[] = [];
  const bindingsGets: string[] = [];
  const posts: string[] = [];
  const inspectSeq = { index: 0 };
  const taskSeq = { index: 0 };
  const planSeq = { index: 0 };
  const runs = plan.inspect !== undefined && plan.inspect.length > 1 ? [RUN_A, RUN_B] : [RUN_A];
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      posts.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "command_result",
        run_id: RUN_A,
        outcome: "succeeded",
        exit_code: 0,
      });
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(runs as never[]));
    }
    if (/^\/v1\/runs\/[^/]+$/.test(path)) {
      const inspect = plan.inspect ?? [];
      const current = inspect[inspectSeq.index] ?? inspect[inspect.length - 1];
      inspectSeq.index += 1;
      if (current === undefined) {
        return jsonResponse(runDocument(RUN_A, 5, "succeeded", { kind: "succeeded", terminalStateId: "done" }));
      }
      return jsonResponse(current as never);
    }
    if (/^\/v1\/runs\/[^/]+\/events\?/.test(path)) {
      eventsGets.push(path);
      return jsonResponse({ schema_version: 1, kind: "events", run_id: RUN_A, revision: 5, events: [], next_after: null });
    }
    if (/^\/v1\/runs\/[^/]+\/logs/.test(path)) {
      logsGets.push(path);
      return jsonResponse({ schema_version: 1, kind: "worker_logs", run_id: RUN_A, revision: 5, logs: [] });
    }
    if (/^\/v1\/runs\/[^/]+\/changes/.test(path)) {
      changesGets.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "error",
        layer: "binding",
        reason: "binding_not_found",
      });
    }
    if (/^\/v1\/runs\/[^/]+\/execution-bindings\?/.test(path)) {
      bindingsGets.push(path);
      return jsonResponse({
        schema_version: 1,
        kind: "execution_bindings",
        run_id: RUN_A,
        revision: 5,
        after: 0,
        bindings: [],
        next_after: null,
      });
    }
    if (/^\/v1\/runs\/([^/]+)\/identity\?/.test(path)) {
      identityGets.push(path);
      const afterMatch = /[?&]after=(\d+)/.exec(path);
      const revMatch = /[?&]revision=(\d+)/.exec(path);
      const runMatch = /^\/v1\/runs\/([^/]+)\/identity/.exec(path);
      const afterKey = afterMatch === null ? "0" : (afterMatch[1] as string);
      const requestedRun = runMatch === null ? RUN_A : (runMatch[1] as string);
      const requestedRevision = revMatch === null ? 5 : Number(revMatch[1]);
      const pages = plan.identityPages ?? {};
      const current = pages[afterKey] ?? {
        schema_version: 1,
        kind: "identity",
        run_id: requestedRun,
        revision: requestedRevision,
        after: 0,
        records: IDENTITY_RECORDS,
        next_after: null,
      };
      if (current !== undefined && current !== null && "gate" in current) {
        return (current as { gate: { promise: Promise<Response> } }).gate.promise;
      }
      if (current !== undefined && current !== null && current["run_id"] === undefined) {
        current["run_id"] = requestedRun;
        current["revision"] = requestedRevision;
      }
      return jsonResponse(current as never);
    }
    if (/^\/v1\/runs\/[^/]+\/tasks\/[^/]+\/revisions\/\d+\?/.test(path)) {
      taskGets.push(path);
      const entries = plan.task ?? [];
      const current = entries[taskSeq.index] ?? entries[entries.length - 1];
      taskSeq.index += 1;
      if (current !== undefined && current !== null && "gate" in current) {
        return (current as { gate: { promise: Promise<Response> } }).gate.promise;
      }
      if (current === undefined) {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "request", reason: "task_content_missing" });
      }
      return jsonResponse(current as never);
    }
    if (/^\/v1\/runs\/[^/]+\/plans\/\d+\?/.test(path)) {
      planGets.push(path);
      const entries = plan.plan ?? [];
      const current = entries[planSeq.index] ?? entries[entries.length - 1];
      planSeq.index += 1;
      if (current !== undefined && current !== null && "gate" in current) {
        return (current as { gate: { promise: Promise<Response> } }).gate.promise;
      }
      if (current === undefined) {
        return jsonResponse({ schema_version: 1, kind: "error", layer: "request", reason: "plan_content_missing" });
      }
      return jsonResponse(current as never);
    }
    throw new Error(`harness fetch not routed: ${path}`);
  };
  return { fetchImpl, identityGets, taskGets, planGets, eventsGets, logsGets, changesGets, bindingsGets, posts };
}

/** The failed command result the identity endpoint returns for a
 * typed stream failure. */
function identityFailure(): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "command_result",
    run_id: RUN_A,
    outcome: "failed",
    exit_code: 1,
    reason: "run_state_invalid",
  };
}

const DIGEST_A = stageTaskBodyDigest(new TextEncoder().encode(BODY_TEXT_A));
const DIGEST_FULL = stageTaskBodyDigest(new TextEncoder().encode(BODY_TEXT_A + BODY_TEXT_B));

/** The complete one-chunk task-content document for task-alpha@1. */
function taskDocument() {
  return {
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_A,
    task_id: "task-alpha",
    revision: 1,
    manifest_sha256: sha("1"),
    previous_sha256: null,
    origin: "planning_proposal",
    body: {
      byte_count: BODY_TEXT_A.length,
      sha256: DIGEST_A,
      offset: 0,
      limit: 65536,
      returned_byte_count: BODY_TEXT_A.length,
      next_offset: null,
      data_base64: b64(BODY_TEXT_A),
    },
  };
}

/** The two-chunk task-content documents of the streaming fixture. */
function taskStreamDocuments() {
  const chunk = (offset: number, text: string, next: number | null) => ({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_A,
    task_id: "task-alpha",
    revision: 1,
    manifest_sha256: sha("1"),
    previous_sha256: null,
    origin: "planning_proposal",
    body: {
      byte_count: BODY_TEXT_A.length + BODY_TEXT_B.length,
      sha256: DIGEST_FULL,
      offset,
      limit: 65536,
      returned_byte_count: text.length,
      next_offset: next,
      data_base64: b64(text),
    },
  });
  return [chunk(0, BODY_TEXT_A, BODY_TEXT_A.length), chunk(BODY_TEXT_A.length, BODY_TEXT_B, null)];
}

/** The one-page plan-content document for plan revision 2. */
function planDocument() {
  return {
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_A,
    revision: 2,
    manifest_sha256: sha("4"),
    previous_sha256: sha("3"),
    origin_execution: 8,
    after: 0,
    records: [
      { record: "stage", stage_position: 1, id: "stage-one", template: "development" },
      { record: "task", stage_position: 1, task_position: 1, id: "task-alpha", revision: 1, sha256: sha("1") },
      { record: "task_dependency", stage_position: 1, task_position: 1, dependency_position: 1, task_id: "task-alpha" },
    ],
    next_after: null,
  };
}

const SUCCESS_INSPECT = runDocument(RUN_A, 5, "succeeded", { kind: "succeeded", terminalStateId: "done" });
const SUCCESS_INSPECT_B = runDocument(RUN_B, 3, "succeeded", { kind: "succeeded", terminalStateId: "done" });

const wait = () => new Promise((resolve) => setTimeout(resolve, 0));

interface HarnessLike {
  click(id: string): void;
  clickRunInspect(index: number): void;
  runRows(): string[];
  planTasksStatus(): string;
  planTasksMeta(): string;
  planTasksTaskRows(): string[];
  planTasksPlanRows(): string[];
  planTasksPlanRecordRows(): string[];
  planTasksTaskTitle(): string;
  planTasksTaskStatusText(): string;
  planTasksTaskMeta(): string;
  planTasksTaskBodyText(): string;
  planTasksPlanTitle(): string;
  planTasksPlanStatusText(): string;
  planTasksPlanMeta(): string;
  planTasksTaskPanelHidden(): boolean;
  planTasksPlanPanelHidden(): boolean;
  planTasksButtonState(id: string): { hidden: boolean; disabled: boolean };
  planTasksOpenTaskRow(index: number): void;
  planTasksOpenPlanRow(index: number): void;
  element(id: string): FakeElement;
  dispose(): void;
}

async function selectedHarness(plan: FetchPlan = {}): Promise<{ wire: ReturnType<typeof makeFetch>; harness: HarnessLike }> {
  const wire = makeFetch(plan);
  const selected = await createUiHarness({ fetchImpl: wire.fetchImpl as never }) as unknown as HarnessLike;
  selected.click("refresh-runs");
  await wait();
  selected.clickRunInspect(0);
  await wait();
  return { wire, harness: selected };
}

describe("plan/tasks S6 wiring: the pinned selection and ledger render", () => {
  test("a selected run issues the pinned identity GETs and renders both ledgers", async () => {
    const wire = makeFetch({});
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl as never }) as unknown as HarnessLike;
    try {
      harness.click("refresh-runs");
      await wait();
      harness.clickRunInspect(0);
      await wait();
      const pinned = `/v1/runs/${RUN_A}/identity?after=0&revision=5&limit=100`;
      // one GET per identity consumer (lifecycle history + plan/tasks)
      expect(wire.identityGets.filter((path) => path === pinned).length).toBe(2);
      expect(wire.posts).toEqual([]);
      expect(harness.planTasksStatus()).toBe("");
      expect(harness.planTasksMeta()).toContain(`revision 5`);
      expect(harness.planTasksMeta()).toContain("loaded to 4");
      expect(harness.planTasksMeta()).toContain("(complete)");
      expect(harness.planTasksTaskRows()).toEqual([
        `task-alpha|r1|${sha("1").slice(0, 16)}|Open`,
        `task-beta|r2|${sha("2").slice(0, 16)}|Open`,
      ]);
      expect(harness.planTasksPlanRows()).toEqual([
        `r1|${sha("3").slice(0, 16)}|Open`,
        `r2|${sha("4").slice(0, 16)}|Open`,
      ]);
      expect(harness.planTasksTaskPanelHidden()).toBe(true);
      expect(harness.planTasksPlanPanelHidden()).toBe(true);
      expect(harness.planTasksButtonState("pt-identity-load-more").hidden).toBe(true);
      expect(harness.planTasksButtonState("pt-reload").hidden).toBe(false);
    } finally {
      harness.dispose();
    }
  });

  test("a partial identity page holds the ledger, Load more completes it", async () => {
    const partial = {
      schema_version: 1,
      kind: "identity",
      run_id: RUN_A,
      revision: 5,
      after: 0,
      records: IDENTITY_RECORDS.slice(0, 2),
      next_after: 2,
    };
    const rest = {
      schema_version: 1,
      kind: "identity",
      run_id: RUN_A,
      revision: 5,
      after: 2,
      records: IDENTITY_RECORDS.slice(2),
      next_after: null,
    };
    const wire = makeFetch({ identityPages: { "0": partial, "2": rest } });
    const harness = await createUiHarness({ fetchImpl: wire.fetchImpl as never }) as unknown as HarnessLike;
    try {
      harness.click("refresh-runs");
      await wait();
      harness.clickRunInspect(0);
      await wait();
      expect(harness.planTasksTaskRows()).toEqual(
        [`The pinned ledger is not complete yet; selections open once it is.`],
      );
      expect(harness.planTasksButtonState("pt-identity-load-more").hidden).toBe(false);
      harness.click("pt-identity-load-more");
      await wait();
      expect(wire.identityGets).toContain(`/v1/runs/${RUN_A}/identity?after=2&revision=5&limit=100`);
      expect(harness.planTasksTaskRows()).toEqual([
        `task-alpha|r1|${sha("1").slice(0, 16)}|Open`,
        `task-beta|r2|${sha("2").slice(0, 16)}|Open`,
      ]);
      expect(harness.planTasksPlanRows().length).toBe(2);
      expect(harness.planTasksButtonState("pt-identity-load-more").hidden).toBe(true);
    } finally {
      harness.dispose();
    }
  });
});

describe("plan/tasks S6 wiring: the task body surface", () => {
  test("Open runs the pinned chunk GET, disables the section controls, publishes the verified body", async () => {
    const gate = deferred();
    const { wire, harness } = await selectedHarness({ task: [{ gate }] });
    try {
      expect(wire.taskGets).toEqual([]);
      harness.planTasksOpenTaskRow(0);
      await wait();
      expect(wire.taskGets).toEqual([`/v1/runs/${RUN_A}/tasks/task-alpha/revisions/1?offset=0&limit=65536`]);
      expect(harness.planTasksTaskPanelHidden()).toBe(false);
      for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
        expect(harness.planTasksButtonState(id).disabled, id).toBe(true);
      }
      gate.settle(taskDocument());
      await wait();
      await wait();
      expect(harness.planTasksTaskTitle()).toBe(`task task-alpha revision 1`);
      expect(harness.planTasksTaskStatusText()).toContain("Body verified (sha256");
      expect(harness.planTasksTaskStatusText()).toContain(DIGEST_A.slice(0, 16));
      expect(harness.planTasksTaskBodyText()).toBe(BODY_TEXT_A);
      expect(harness.planTasksTaskMeta()).toContain(`manifest sha256 ${sha("1").slice(0, 16)}`);
      expect(harness.planTasksTaskStatusText()).toContain(`${DIGEST_A.slice(0, 16)}…`);
      expect(harness.planTasksTaskMeta()).toContain("durable evidence: absent");
      for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
        expect(harness.planTasksButtonState(id).disabled, id).toBe(false);
      }
      expect(harness.planTasksButtonState("pt-task-load-more").hidden).toBe(true);
      expect(harness.planTasksButtonState("pt-task-retry").hidden).toBe(true);
      expect(wire.posts).toEqual([]);
      expect(wire.taskGets.length).toBe(1);
    } finally {
      harness.dispose();
    }
  });

  test("a truncated body keeps Load more; the next chunk completes the verified text", async () => {
    const documents = taskStreamDocuments();
    const { wire, harness } = await selectedHarness({ task: documents as never[] });
    try {
      harness.planTasksOpenTaskRow(0);
      await wait();
      await wait();
      expect(harness.planTasksTaskStatusText()).toContain("Streaming body (");
      // the text publishes only after the terminal chunk verifies
      expect(harness.element("pt-task-body" as never).hidden).toBe(true);
      expect(harness.planTasksButtonState("pt-task-load-more").hidden).toBe(false);
      harness.click("pt-task-load-more");
      await wait();
      await wait();
      expect(wire.taskGets).toEqual([
        `/v1/runs/${RUN_A}/tasks/task-alpha/revisions/1?offset=0&limit=65536`,
        `/v1/runs/${RUN_A}/tasks/task-alpha/revisions/1?offset=${BODY_TEXT_A.length}&limit=65536`,
      ]);
      expect(harness.planTasksTaskBodyText()).toBe(BODY_TEXT_A + BODY_TEXT_B);
      expect(harness.planTasksTaskStatusText()).toContain("Body verified (sha256");
      expect(harness.planTasksButtonState("pt-task-load-more").hidden).toBe(true);
    } finally {
      harness.dispose();
    }
  });

  test("a failed chunk stream shows the failure and Retry re-issues the same offset", async () => {
    const failure = { schema_version: 1, kind: "error", layer: "request", reason: "task_content_missing" };
    const { wire, harness } = await selectedHarness({ task: [failure, taskDocument()] });
    try {
      harness.planTasksOpenTaskRow(0);
      await wait();
      await wait();
      expect(harness.planTasksTaskStatusText()).toContain("Failed (request: task_content_missing)");
      expect(harness.planTasksTaskStatusText()).toContain("Use Retry body");
      expect(harness.planTasksTaskBodyText()).toBe("");
      expect(harness.planTasksButtonState("pt-task-retry").hidden).toBe(false);
      harness.click("pt-task-retry");
      await wait();
      await wait();
      expect(wire.taskGets).toEqual([
        `/v1/runs/${RUN_A}/tasks/task-alpha/revisions/1?offset=0&limit=65536`,
        `/v1/runs/${RUN_A}/tasks/task-alpha/revisions/1?offset=0&limit=65536`,
      ]);
      expect(harness.planTasksTaskBodyText()).toBe(BODY_TEXT_A);
    } finally {
      harness.dispose();
    }
  });
});

describe("plan/tasks S6 wiring: the plan surface and the reset discipline", () => {
  test("Open renders the projected plan records with the ledger pointer verdict", async () => {
    const { wire, harness } = await selectedHarness({ plan: [planDocument()] });
    try {
      harness.planTasksOpenPlanRow(1);
      await wait();
      await wait();
      expect(wire.planGets).toEqual([`/v1/runs/${RUN_A}/plans/2?after=0&limit=100`]);
      expect(harness.planTasksPlanPanelHidden()).toBe(false);
      expect(harness.planTasksPlanTitle()).toBe("plan revision 2");
      expect(harness.planTasksPlanMeta()).toContain(`manifest sha256 ${sha("4").slice(0, 16)}`);
      expect(harness.planTasksPlanMeta()).toContain(`origin execution 8`);
      expect(harness.planTasksPlanRecordRows()).toEqual([
        "stage|1|stage-one (template development)",
        "task|1.1|task-alpha r1",
        "task_dependency|1.1.1|task-alpha",
      ]);
      expect(harness.planTasksPlanStatusText()).toContain("Loaded completely.");
      expect(harness.planTasksPlanStatusText()).toContain("Every task pointer matches the pinned ledger.");
      expect(harness.planTasksButtonState("pt-plan-load-more").hidden).toBe(true);
      expect(harness.planTasksButtonState("pt-plan-retry").hidden).toBe(true);
      expect(wire.posts).toEqual([]);
    } finally {
      harness.dispose();
    }
  });

  test("a failed plan stream offers Retry plan with the same cursor", async () => {
    const failure = { schema_version: 1, kind: "error", layer: "request", reason: "plan_content_missing" };
    const { wire, harness } = await selectedHarness({ plan: [failure, planDocument()] });
    try {
      harness.planTasksOpenPlanRow(1);
      await wait();
      await wait();
      expect(harness.planTasksPlanStatusText()).toContain("Failed (request: plan_content_missing)");
      expect(harness.planTasksButtonState("pt-plan-retry").hidden).toBe(false);
      harness.click("pt-plan-retry");
      await wait();
      await wait();
      expect(wire.planGets).toEqual([
        `/v1/runs/${RUN_A}/plans/2?after=0&limit=100`,
        `/v1/runs/${RUN_A}/plans/2?after=0&limit=100`,
      ]);
      expect(harness.planTasksPlanRecordRows().length).toBe(3);
    } finally {
      harness.dispose();
    }
  });

  test("a failed identity stream clears the view; Reload re-issues the pinned GET", async () => {
    const { wire, harness } = await selectedHarness({ identityPages: { "0": identityFailure() } });
    try {
      harness.click("refresh-runs");
      await wait();
      harness.clickRunInspect(0);
      await wait();
      // one initial pinned GET per identity consumer (lifecycle + S6)
      const pinned = `/v1/runs/${RUN_A}/identity?after=0&revision=5&limit=100`;
      expect(wire.identityGets.filter((path) => path === pinned).length).toBe(2);
      expect(harness.planTasksStatus()).toContain("The ledger data was cleared");
      expect(harness.planTasksMeta()).toContain("cleared after a stale snapshot or a failed load");
      expect(harness.planTasksTaskRows()).toEqual([]);
      expect(harness.planTasksPlanRows()).toEqual([]);
      expect(harness.planTasksTaskPanelHidden()).toBe(true);
      expect(harness.planTasksButtonState("pt-reload").hidden).toBe(false);
      harness.click("pt-reload");
      await wait();
      await wait();
      // the explicit reload re-issues the pinned GET for the pt stream only
      expect(wire.identityGets.filter((path) => path === pinned).length).toBe(3);
      expect(harness.planTasksStatus()).toContain("The ledger data was cleared");
      expect(wire.posts).toEqual([]);
    } finally {
      harness.dispose();
    }
  });

  test("selecting another run resets the section and re-pins the ledger", async () => {
    const { wire, harness } = await selectedHarness({ inspect: [SUCCESS_INSPECT, SUCCESS_INSPECT_B] });
    try {
      harness.click("refresh-runs");
      await wait();
      harness.clickRunInspect(0);
      await wait();
      expect(harness.planTasksTaskRows().length).toBe(2);
      harness.clickRunInspect(1);
      await wait();
      await wait();
      expect(wire.identityGets.filter((path) => path.includes(RUN_B) && path.endsWith("after=0&revision=3&limit=100")).length).toBe(2);
      // the pinned fixture identity renders both task revisions
      expect(harness.planTasksTaskRows().length).toBe(2);
      expect(harness.planTasksTaskPanelHidden()).toBe(true);
      expect(harness.planTasksStatus()).toBe("");
      expect(wire.posts).toEqual([]);
    } finally {
      harness.dispose();
    }
  });
});
