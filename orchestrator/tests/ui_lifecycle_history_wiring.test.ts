/**
 * Permanent wiring suite of the browser UI v9 "Lifecycle history"
 * section: the fake-DOM harness boots the shipped `app.js` and drives
 * the production call chains (applied inspect → the pinned initial
 * identity/execution-bindings GET pair → Load more per stream →
 * compare → historical terminal rendering → reload → stale restart)
 * with an instrumented fetch. Every GET carries the applied inspect
 * revision pin; the section never POSTs and never fires without an
 * applied authoritative inspect.
 */
import { describe, expect, test } from "bun:test";
import { createUiHarness, jsonResponse, projectsDocument, runDocument, runsPageDocument, type UiHarness } from "./ui_dom_harness.ts";

const RUN = "run-lh-wire";
const sha = (char: string) => char.repeat(64);

const IDENTITY_RECORDS_A = [
  { record: "plan_revision", index: 1, revision: 1, sha256: sha("3"), previous_sha256: null, origin_execution: 1 },
  { record: "task_revision", index: 1, task_id: "task-a", revision: 1, sha256: sha("1"), previous_sha256: null, wait_index: null, intent_sha256: null },
  { record: "task_revision", index: 2, task_id: "task-b", revision: 1, sha256: sha("2"), previous_sha256: null, wait_index: null, intent_sha256: null },
  {
    record: "generation",
    index: 1,
    stage_id: "stage-1",
    stage_position: 1,
    template_id: "development",
    plan_sha256: sha("3"),
    initial_budget: 2,
    opened_transition_count: 0,
    iteration_count: 2,
    open_iteration: null,
    closed: { by: "replanned", closed_transition_count: 3 },
  },
  { record: "iteration", generation_index: 1, index: 1, opened_transition_count: 0, closed: { by: "grant", wait_index: 1, closed_transition_count: 2 } },
  { record: "iteration", generation_index: 1, index: 2, opened_transition_count: 2, closed: { by: "replanned", wait_index: 2, closed_transition_count: 3 } },
  { record: "grant", index: 1, generation_index: 1, wait_index: 1, intent_sha256: sha("9"), additional_iterations: 1 },
  {
    record: "wait",
    index: 1,
    transition_count: 2,
    state_id: "architect",
    reason: "stage_iteration_limit_exhausted",
    request_sha256: sha("7"),
    intent_sha256: sha("9"),
    response: { action_id: "continue_stage", response_sha256: sha("8") },
  },
  { record: "wait_action", wait_index: 1, id: "continue_stage", to: "dev_entry" },
  { record: "wait_action", wait_index: 1, id: "revise_task", to: "architect" },
  { record: "execution_agent", index: 1, type: "agent", state_id: "architect", execution_role: "planning", iteration_index: null, phase: "cleanup_completed", stage_task: null },
  { record: "agent_output", execution_index: 1, id: "plan", digest: sha("d") },
  { record: "execution_agent", index: 2, type: "agent", state_id: "dev_entry", execution_role: "stage", iteration_index: 1, phase: "cleanup_completed", stage_task: { task_id: "task-a", task_revision: 1, task_manifest_sha256: sha("5"), body_sha256: sha("6") } },
  { record: "agent_output", execution_index: 2, id: "report", digest: sha("e") },
];

const IDENTITY_RECORDS_B = [
  {
    record: "generation",
    index: 2,
    stage_id: "stage-1",
    stage_position: 1,
    template_id: "development",
    plan_sha256: sha("4"),
    initial_budget: 3,
    opened_transition_count: 3,
    iteration_count: 1,
    open_iteration: { index: 1, opened_transition_count: 3 },
    closed: null,
  },
  { record: "iteration", generation_index: 2, index: 1, opened_transition_count: 3, closed: null },
];

const BINDING_RECORDS = [
  { record: "execution_binding", execution_index: 1, generation_index: null, iteration_index: null },
  { record: "execution_binding", execution_index: 2, generation_index: 1, iteration_index: 1 },
];

const identityPageA = (revision: number) => ({
  schema_version: 1,
  kind: "identity",
  run_id: RUN,
  revision,
  after: 0,
  records: IDENTITY_RECORDS_A,
  next_after: IDENTITY_RECORDS_A.length,
});
const identityPageFinal = (revision: number) => ({
  schema_version: 1,
  kind: "identity",
  run_id: RUN,
  revision,
  after: IDENTITY_RECORDS_A.length,
  records: IDENTITY_RECORDS_B,
  next_after: null,
});
const bindingsPage = (revision: number) => ({
  schema_version: 1,
  kind: "execution_bindings",
  run_id: RUN,
  revision,
  after: 0,
  records: BINDING_RECORDS,
  next_after: null,
});

interface Probe {
  readonly fetchImpl: (path: string, init?: RequestInit) => Promise<Response>;
  /** Recorded GET paths only (identity/bindings/inspect/events). */
  readonly gets: string[];
  readonly posts: string[];
  /** Replace the identity or bindings answer for the next request. */
  queue: { identity: unknown[]; bindings: unknown[] };
}

function makeProbe(options: {
  inspect?: { revision: number; status?: string; boundary?: Record<string, unknown> };
  onIdentity?: () => unknown;
  onBindings?: () => unknown;
  terminalRuns?: { revision: number; status?: string; boundary?: Record<string, unknown> }[];
}): Probe {
  let inspectSeq = 0;
  const gets: string[] = [];
  const posts: string[] = [];
  const queue = { identity: [] as unknown[], bindings: [] as unknown[] };
  const fetchImpl = async (path: string, init?: RequestInit) => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    if (method === "POST") {
      posts.push(path);
      throw new Error("no POST is expected in the lifecycle-history suite");
    }
    gets.push(path);
    const identity = /^\/v1\/runs\/([^/?]+)\/identity\?/.exec(path);
    if (identity !== null) {
      const queued = queue.identity.shift();
      if (queued !== undefined) {
        return queued instanceof Response ? queued : jsonResponse(queued as Record<string, unknown>);
      }
      return options.onIdentity === undefined ? jsonResponse(identityPageA(24)) : jsonResponse(options.onIdentity() as Record<string, unknown>);
    }
    const bindings = /^\/v1\/runs\/([^/?]+)\/execution-bindings\?/.exec(path);
    if (bindings !== null) {
      const queued = queue.bindings.shift();
      if (queued !== undefined) {
        return queued instanceof Response ? queued : jsonResponse(queued as Record<string, unknown>);
      }
      return options.onBindings === undefined ? jsonResponse(bindingsPage(24)) : jsonResponse(options.onBindings() as Record<string, unknown>);
    }
    const runMatch = /^\/v1\/runs\/([^/?]+)$/.exec(path);
    if (runMatch !== null) {
      const plan = options.terminalRuns?.[inspectSeq] ?? options.inspect ?? { revision: 24 };
      inspectSeq += 1;
      return jsonResponse(runDocument(runMatch[1] ?? RUN, plan.revision, plan.status ?? "running", plan.boundary ?? { kind: "in_flight" }));
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument([RUN]));
    }
    throw new Error(`unexpected probe request ${method} ${path}`);
  };
  return { fetchImpl, gets, posts, queue };
}

const harnessOf = async (probe: Probe): Promise<UiHarness> => {
  const harness = await createUiHarness({ fetchImpl: probe.fetchImpl });
  harness.click("refresh-runs");
  await Bun.sleep(2);
  return harness;
};

const lhSectionText = (harness: UiHarness): string[] => {
  // the fake DOM registers elements by id without a parent tree, so the
  // readers walk the section's own flat containers; kv lines join their
  // label and value spans into one readable line
  const lines: string[] = [];
  const walk = (node: { children: unknown[]; textContent: string; className: string }) => {
    if (node.className.split(" ").includes("lh-kv") && node.children.length > 0) {
      const joined = node.children.map((c) => String((c as { textContent: string }).textContent)).join(" ");
      if (joined !== "") lines.push(joined);
      return;
    }
    if (node.children.length === 0) {
      if (node.textContent !== "") lines.push(node.textContent);
      return;
    }
    for (const child of node.children) walk(child as { children: unknown[]; textContent: string; className: string });
  };
  for (const id of ["lh-status", "lh-identity-meta", "lh-bindings-meta", "lh-iterations", "lh-compare", "lh-historical"]) {
    walk(harness.element(id) as unknown as { children: unknown[]; textContent: string; className: string });
  }
  return lines;
};

const lhStatus = (harness: UiHarness): string => harness.element("lh-status").textContent;

const pinnedGets = (probe: Probe) => ({
  identity: probe.gets.filter((path) => path.includes("/identity?")),
  bindings: probe.gets.filter((path) => path.includes("/execution-bindings?")),
});

describe("ui lifecycle-history wiring: the real module under the fake DOM", () => {
  test("w1. the applied inspect issues exactly the two pinned initial GETs", async () => {
    const probe = makeProbe({ inspect: { revision: 24 } });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    const pins = pinnedGets(probe);
    // since S6 the plan/tasks slice is the second pinned identity
    // consumer: both viewers issue their own identical initial GET
    expect(pins.identity).toEqual([
      `/v1/runs/${RUN}/identity?after=0&revision=24&limit=100`,
      `/v1/runs/${RUN}/identity?after=0&revision=24&limit=100`,
    ]);
    expect(pins.bindings).toEqual([`/v1/runs/${RUN}/execution-bindings?after=0&revision=24&limit=100`]);
    expect(harness.element("lifecycle-history-section").hidden).toBe(false);
    expect(lhStatus(harness)).toBe("Waiting for more stream pages.");
    expect(probe.posts).toEqual([]);
  });

  test("w2. Load more identity completes the streams and renders the cards", async () => {
    const probe = makeProbe({ inspect: { revision: 24 } });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    probe.queue.identity = [identityPageFinal(24)];
    harness.element("lh-identity-load-more").click();
    await Bun.sleep(5);
    // identity[0]/[1] are the two pinned initials (lifecycle history and
    // the S6 plan/tasks consumer); the load-more continuation follows
    expect(pinnedGets(probe).identity[2]).toBe(`/v1/runs/${RUN}/identity?after=${IDENTITY_RECORDS_A.length}&revision=24&limit=100`);
    expect(lhStatus(harness)).toBe("Streams complete.");
    const lines = lhSectionText(harness);
    expect(lines.some((line) => line.includes("Generation 1 — stage stage-1 (position 1, template development)"))).toBe(true);
    expect(lines.some((line) => line.includes("Generation 2 — stage stage-1"))).toBe(true);
    expect(lines.some((line) => line.includes("task-a r1 · manifest "))).toBe(true);
    expect(lines.some((line) => line.includes("execution 2 · agent dev_entry"))).toBe(true);
    expect(probe.posts).toEqual([]);
  });

  test("w3. the compare selects only same-template pairs and renders both sides", async () => {
    const probe = makeProbe({ inspect: { revision: 24 } });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    probe.queue.identity = [identityPageFinal(24)];
    harness.element("lh-identity-load-more").click();
    await Bun.sleep(5);
    const selectA = harness.element("lh-compare-a");
    const selectB = harness.element("lh-compare-b");
    selectA.value = "1";
    selectB.value = "2";
    harness.element("lh-compare-apply").click();
    await Bun.sleep(5);
    const lines = lhSectionText(harness);
    expect(lines.some((line) => line.startsWith("Side A: Generation 1"))).toBe(true);
    expect(lines.some((line) => line.startsWith("Side B: Generation 2"))).toBe(true);
    expect(lines.some((line) => line.includes("different plan revisions"))).toBe(true);
    expect(probe.posts).toEqual([]);
  });

  test("w4. the historical group appears only on a terminal inspect and never issues a fetch of its own", async () => {
    const probe = makeProbe({ inspect: { revision: 24, status: "succeeded" } });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    // the pinned terminal inspect issues the pinned identity GET per
    // consumer (the history stream and the S6 plan/tasks ledger)
    expect(probe.gets.filter((path) => path.endsWith("/identity?after=0&revision=24&limit=100"))).toHaveLength(2);
    expect(probe.gets.filter((path) => path.includes("/execution-bindings?after=0"))).toHaveLength(1);
    const lines = lhSectionText(harness);
    expect(lines.some((line) => line.includes("Wait 1 at transition 2 — stage_iteration_limit_exhausted"))).toBe(true);
    expect(lines.some((line) => line.includes("continue_stage → dev_entry"))).toBe(true);
    expect(lines.some((line) => line.includes("revise_task → architect"))).toBe(true);
    expect(lines.some((line) => line.startsWith("response") && line.includes("continue_stage"))).toBe(true);
    expect(lines.some((line) => line.includes("#1 · 1 additional iteration · wait 1"))).toBe(true);
  });

  test("w5. a revision change restarts both streams under the new pin", async () => {
    const probe = makeProbe({
      inspect: { revision: 24 },
      terminalRuns: [
        { revision: 24 },
        { revision: 25, status: "running" },
      ],
    });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    harness.click("inspect-selected");
    await Bun.sleep(5);
    // both identity consumers restart under the new pin; the single
    // bindings stream restarts into the same entry (order across the
    // two transports is not pinned by the fixture)
    const pinnedRestarts = probe.gets.filter((path) => path.endsWith("/identity?after=0&revision=25&limit=100"));
    expect(pinnedRestarts).toHaveLength(2);
    expect(probe.gets.filter((path) => path.endsWith("/execution-bindings?after=0&revision=25&limit=100"))).toHaveLength(1);
    expect(probe.posts).toEqual([]);
  });

  test("w6. a failed-command identity answer fails the stream only, never the section or a POST", async () => {
    const probe = makeProbe({
      inspect: { revision: 24, status: "running" },
      onIdentity: () => ({
        schema_version: 1,
        kind: "command_result",
        run_id: RUN,
        outcome: "failed",
        exit_code: 1,
        reason: "run_state_invalid",
      }),
    });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(lhSectionText(harness).some((line) => line.startsWith("identity stream: orchestrator: run_state_invalid"))).toBe(true);
    expect(probe.posts).toEqual([]);
  });

  test("w7. a network failure of the bindings stream is a typed transport failure", async () => {
    const failing = new Response("gone", { status: 503 });
    const probe = makeProbe({ inspect: { revision: 24, status: "running" } });
    probe.queue.bindings = [failing];
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(5);
    expect(lhSectionText(harness).some((line) => line.startsWith("bindings stream:"))).toBe(true);
    expect(probe.posts).toEqual([]);
  });

  test("w8. a stale snapshot answer restarts both streams from the same pin", async () => {
    // the probe answers every identity request with the 409 envelope;
    // the viewer must restart but must not loop endlessly (the probe
    // then answers normally), and the restarts reuse the same pin until
    // an applied inspect establishes a new one
    let identityCalls = 0;
    const probe = makeProbe({
      inspect: { revision: 24, status: "running" },
      onIdentity: () => {
        identityCalls += 1;
        return identityCalls <= 2
          ? { schema_version: 1, kind: "error", layer: "request", reason: "stale_snapshot" }
          : identityPageA(24);
      },
    });
    const harness = await harnessOf(probe);
    harness.clickRunInspect(0);
    await Bun.sleep(10);
    // every stale answer clears the semantic view and restarts both
    // streams from the same pin, so two stale answers cost three calls
    expect(identityCalls).toBe(3); // two stale answers, then the page
    // page A alone leaves more identity records available
    expect(lhStatus(harness)).toBe("Waiting for more stream pages.");
    expect(probe.posts).toEqual([]);
  });
});
