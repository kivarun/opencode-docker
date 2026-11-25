/**
 * Permanent contract suite of the browser UI v9 "Lifecycle history"
 * model: the P-3 identity stream parser (schema 1, 12 record kinds),
 * the S5-P0 execution-bindings stream parser, the revision-pinned
 * two-stream merge, the frozen derivation and the two single-flight
 * transports. The documents mirror the shipped endpoint shapes
 * (`GET /v1/runs/:runId/identity`, `GET /v1/runs/:runId/execution-bindings`)
 * over a real reducer-built two-generation lifecycle.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  type UiContractError,
  applyBindingsPage,
  applyIdentityPage,
  createExecutionBindingsTransport,
  createIdentityTransport,
  createLifecycleHistoryState,
  deriveLifecycleHistory,
  executionBindingsRequestPath,
  identityRequestPath,
  parseExecutionBindingsDocument,
  parseIdentityDocument,
  parseIdentityEndpointDocument,
  parseExecutionBindingsEndpointDocument,
} from "../ui/app.js";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");
const EXPECTED_KINDS = [
  "plan_revision",
  "task_revision",
  "generation",
  "iteration",
  "grant",
  "wait",
  "wait_action",
  "execution_agent",
  "execution_decision",
  "agent_output",
  "decision_active_constraint",
  "decision_violated_relation",
];

const RUN = "run-lh-model";

const sha = (char: string) => char.repeat(64);

/** The identity page 1 of the two-generation 66-command fixture (records
 * 1..N) — exactly the shapes the shipped identity projector emits. */
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
  { record: "execution_decision", index: 3, type: "decision", state_id: "gate", execution_role: "control", iteration_index: null, phase: "evaluated", input_digest: sha("4"), result: { status: "selected", outcome: "continue", decision: "gate", rule_id: "r-continue" } },
  { record: "decision_active_constraint", execution_index: 3, position: 1, id: "hc-1" },
  { record: "decision_violated_relation", execution_index: 3, position: 2, id: "fc-2" },
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
  { record: "wait", index: 2, transition_count: 3, state_id: "architect", reason: "stage_iteration_limit_exhausted", request_sha256: sha("b"), intent_sha256: sha("c"), response: { action_id: "revise_task", response_sha256: sha("0") } },
  { record: "execution_agent", index: 4, type: "agent", state_id: "architect", execution_role: "planning", iteration_index: null, phase: "cleanup_completed", stage_task: null },
  { record: "execution_agent", index: 5, type: "agent", state_id: "dev_entry", execution_role: "stage", iteration_index: 1, phase: "cleanup_completed", stage_task: { task_id: "task-a", task_revision: 2, task_manifest_sha256: sha("a"), body_sha256: sha("f") } },
  { record: "agent_output", execution_index: 5, id: "report", digest: sha("e") },
];

const BINDING_RECORDS = [
  { record: "execution_binding", execution_index: 1, generation_index: null, iteration_index: null },
  { record: "execution_binding", execution_index: 2, generation_index: 1, iteration_index: 1 },
  { record: "execution_binding", execution_index: 3, generation_index: null, iteration_index: null },
  { record: "execution_binding", execution_index: 4, generation_index: null, iteration_index: null },
  { record: "execution_binding", execution_index: 5, generation_index: 1, iteration_index: 2 },
];

const identityPageA = {
  schema_version: 1,
  kind: "identity",
  run_id: RUN,
  revision: 24,
  after: 0,
  records: IDENTITY_RECORDS_A,
  next_after: IDENTITY_RECORDS_A.length,
};
const identityPageB = {
  schema_version: 1,
  kind: "identity",
  run_id: RUN,
  revision: 24,
  after: IDENTITY_RECORDS_A.length,
  records: IDENTITY_RECORDS_B,
  next_after: null,
};
const bindingsPage = {
  schema_version: 1,
  kind: "execution_bindings",
  run_id: RUN,
  revision: 24,
  after: 0,
  records: BINDING_RECORDS,
  next_after: null,
};

const contractReason = (run: () => unknown): string => {
  try {
    run();
  } catch (error) {
    return (error as UiContractError).reason;
  }
  return "no-error";
};

function buildRevised(overrides: Record<string, unknown>): Record<string, unknown> {
  return JSON.parse(JSON.stringify({ ...identityPageA, ...overrides })) as Record<string, unknown>;
}

describe("ui lifecycle-history model: the identity document parser", () => {
  test("the full document parses with every record kind preserved in order", () => {
    const page = parseIdentityDocument(identityPageA, RUN);
    expect(page.kind).toBe("identity");
    expect(page.runId).toBe(RUN);
    expect(page.revision).toBe(24);
    expect(page.records.map((entry) => entry.record)).toEqual(IDENTITY_RECORDS_A.map((entry) => entry.record));
    expect(page.nextAfter).toBe(IDENTITY_RECORDS_A.length);
  });

  test("every record kind of the shipped vocabulary parses", () => {
    const page = parseIdentityDocument(identityPageA, RUN);
    const seen = new Set(page.records.map((entry) => entry.record));
    expect([...seen].sort()).toEqual([...EXPECTED_KINDS].sort());
  });

  test("unknown versions, kinds, shapes and run bindings fail closed", () => {
    expect(contractReason(() => parseIdentityDocument({ ...identityPageA, schema_version: 2 }, RUN))).toBe("unknown_schema_version");
    expect(contractReason(() => parseIdentityDocument({ ...identityPageA, kind: "events" }, RUN))).toBe("unknown_kind");
    expect(contractReason(() => parseIdentityDocument(identityPageA, "other-run"))).toBe("unexpected_run");
    expect(contractReason(() => parseIdentityDocument(identityPageA, null as unknown as string))).toBe("unexpected_run");
    const missingKey = buildRevised({});
    const missingRecords = missingKey.records as Record<string, unknown>[];
    delete missingRecords[0]?.origin_execution;
    expect(contractReason(() => parseIdentityDocument(missingKey, RUN))).toBe("invalid_shape");
    const extra = buildRevised({});
    (extra.records as Record<string, unknown>[])[0]!.hostile = 1;
    expect(contractReason(() => parseIdentityDocument(extra, RUN))).toBe("invalid_shape");
    expect(contractReason(() => parseIdentityDocument([identityPageA] as unknown as Record<string, unknown>, RUN))).toBe("not_a_document");
    const badEnvelope = buildRevised({ extras: true });
    expect(contractReason(() => parseIdentityDocument(badEnvelope, RUN))).toBe("invalid_shape");
  });

  test("unknown record kinds fail closed and hostile field classes are refused", () => {
    const unknown = buildRevised({});
    (unknown.records as Record<string, unknown>[])[0] = { record: "ledger_row", index: 1 };
    expect(contractReason(() => parseIdentityDocument(unknown, RUN))).toBe("unknown_record_kind");
    const badDigest = buildRevised({});
    (badDigest.records as Record<string, unknown>[])[0]!.sha256 = "not-a-digest";
    expect(contractReason(() => parseIdentityDocument(badDigest, RUN))).toBe("invalid_shape");
    const badId = buildRevised({});
    (badId.records as Record<string, unknown>[])[2]!.task_id = "../escape";
    expect(contractReason(() => parseIdentityDocument(badId, RUN))).toBe("invalid_shape");
    const badIndex = buildRevised({});
    (badIndex.records as Record<string, unknown>[])[5]!.index = 0;
    expect(contractReason(() => parseIdentityDocument(badIndex, RUN))).toBe("invalid_shape");
  });

  test("the nullable members accept exactly the null forms the projector carries", () => {
    const page = parseIdentityDocument(identityPageB, RUN);
    const generation = page.records.find((entry) => entry.record === "generation") as Record<string, unknown>;
    expect(generation["open_iteration"]).not.toBeNull();
    const iteration = page.records.find((entry) => entry.record === "iteration") as Record<string, unknown>;
    expect(iteration["closed"]).toBeNull();
    const wait = page.records.find((entry) => entry.record === "wait") as Record<string, unknown>;
    expect((wait["response"] as Record<string, unknown>)["actionId"]).toBe("revise_task");
  });

  test("decision result variants parse with the exact id lists bound to their execution", () => {
    const page = parseIdentityDocument(identityPageA, RUN);
    const selected = page.records.find((entry) => entry.record === "execution_decision") as Record<string, unknown>;
    expect((selected["result"] as Record<string, unknown>)["ruleId"]).toBe("r-continue");
    const constraints = page.records.filter((entry) => entry.record === "decision_active_constraint");
    expect(constraints).toHaveLength(1);
    expect((constraints[0] as Record<string, unknown>)["executionIndex"]).toBe(3);
  });

  test("the page cursor contract binds next_after to after + records or null", () => {
    expect(contractReason(() => parseIdentityDocument(buildRevised({ next_after: 99 }), RUN))).toBe("invalid_shape");
    expect(contractReason(() => parseIdentityDocument(buildRevised({ next_after: 0 }), RUN))).toBe("invalid_shape");
    expect(parseIdentityDocument(buildRevised({ next_after: null }), RUN).nextAfter).toBeNull();
    expect(contractReason(() => parseIdentityDocument(buildRevised({ after: -1 }), RUN))).toBe("invalid_shape");
  });
});

describe("ui lifecycle-history model: the execution-bindings document parser", () => {
  test("the one-kind page parses with null-normalized placements", () => {
    const page = parseExecutionBindingsDocument(bindingsPage, RUN);
    expect(page.kind).toBe("execution_bindings");
    expect(page.records).toHaveLength(5);
    expect(page.records[0]).toEqual({ record: "execution_binding", executionIndex: 1, generationIndex: null, iterationIndex: null });
    expect(page.records[1]).toEqual({ record: "execution_binding", executionIndex: 2, generationIndex: 1, iterationIndex: 1 });
  });

  test("foreign run ids, malformed records and cursor drift fail closed", () => {
    expect(contractReason(() => parseExecutionBindingsDocument(bindingsPage, "other"))).toBe("unexpected_run");
    expect(contractReason(() => parseExecutionBindingsDocument({ ...bindingsPage, kind: "identity" }, RUN))).toBe("unknown_kind");
    const bad = JSON.parse(JSON.stringify(bindingsPage));
    bad.records[1].iteration_index = 0;
    expect(contractReason(() => parseExecutionBindingsDocument(bad, RUN))).toBe("invalid_shape");
    const wrongKind = JSON.parse(JSON.stringify(bindingsPage));
    wrongKind.records[1].record = "identity_record";
    expect(contractReason(() => parseExecutionBindingsDocument(wrongKind, RUN))).toBe("invalid_shape");
  });
});

describe("ui lifecycle-history model: the URL builders and endpoint classifiers", () => {
  test("the URL builders carry the pin and never leak a host path", () => {
    expect(identityRequestPath(RUN, 0, 24)).toBe(`/v1/runs/${RUN}/identity?after=0&revision=24&limit=100`);
    expect(executionBindingsRequestPath(RUN, 7, 24, 200)).toBe(`/v1/runs/${RUN}/execution-bindings?after=7&revision=24&limit=200`);
    expect(identityRequestPath("run", 3, null)).toBe("/v1/runs/run/identity?after=3&limit=100");
    expect(contractReason(() => identityRequestPath("../x", 0, null))).toBe("invalid_run_id");
    expect(contractReason(() => identityRequestPath(RUN, -1, null))).toBe("invalid_cursor");
  });

  test("the classifiers accept only their own kind plus the failure envelopes", () => {
    const identityError = { schema_version: 1, kind: "error", layer: "request", reason: "stale_snapshot" };
    const failedResult = { schema_version: 1, kind: "command_result", run_id: RUN, outcome: "failed", exit_code: 1, reason: "run_state_invalid" };
    expect(contractReason(() => parseIdentityDocument(identityError as unknown as Record<string, unknown>, RUN))).toBe("unknown_kind");
    expect((parseIdentityEndpointDocument(identityError, RUN) as { reason: string }).reason).toBe("stale_snapshot");
    expect((parseIdentityEndpointDocument(failedResult, RUN) as { reason: string }).reason).toBe("run_state_invalid");
    const succeeded = { schema_version: 1, kind: "command_result", run_id: RUN, outcome: "succeeded", exit_code: 0 };
    expect(contractReason(() => parseIdentityEndpointDocument(succeeded, RUN))).toBe("unexpected_kind");
    expect(contractReason(() => parseIdentityEndpointDocument({ ...identityError, kind: "events" }, RUN))).toBe("unknown_kind");
  });
});

describe("ui lifecycle-history model: the revision-pinned two-stream merge", () => {
  test("both streams complete under the same pin", () => {
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), identityPageB.after);
    state = applyBindingsPage(state, parseExecutionBindingsDocument(bindingsPage, RUN), 0);
    expect(state.identity.nextAfter).toBeNull();
    expect(state.bindings.nextAfter).toBeNull();
    expect(state.identity.records).toHaveLength(IDENTITY_RECORDS_A.length + IDENTITY_RECORDS_B.length);
  });

  test("a page of a different revision is stale and a wrong cursor is rejected", () => {
    let state = createLifecycleHistoryState(RUN, 24);
    expect(contractReason(() => applyIdentityPage(state, parseIdentityDocument(buildRevised({ revision: 25 }), RUN), 0))).toBe("stale_page");
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    expect(contractReason(() => applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), 0))).toBe("stale_page");
    expect(contractReason(() => applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), 99))).toBe("stale_page");
    expect(contractReason(() => applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0))).toBe("stale_page");
  });

  test("hostile states and wrong kinds fail closed", () => {
    expect(contractReason(() => createLifecycleHistoryState("../x", 1))).toBe("invalid_run_id");
    expect(contractReason(() => createLifecycleHistoryState(RUN, 0))).toBe("invalid_shape");
    expect(contractReason(() => applyIdentityPage(createLifecycleHistoryState(RUN, 1), bindingsPage as unknown as Parameters<typeof applyIdentityPage>[1], 0))).toBe("invalid_state");
  });

  test("the derived view is frozen and isolated from caller mutation", () => {
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), identityPageB.after);
    state = applyBindingsPage(state, parseExecutionBindingsDocument(bindingsPage, RUN), 0);
    const view = deriveLifecycleHistory(state, true);
    expect(view.error).toBeNull();
    expect(view.complete).toBe(true);
    expect(Object.isFrozen(view)).toBe(true);
    expect(Object.isFrozen(view.generations[0])).toBe(true);
  });
});

describe("ui lifecycle-history model: the placement and derivation", () => {
  const fullState = () => {
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), identityPageB.after);
    state = applyBindingsPage(state, parseExecutionBindingsDocument(bindingsPage, RUN), 0);
    return state;
  };

  test("stage executions land inside their bound generation card", () => {
    const view = deriveLifecycleHistory(fullState(), true);
    expect(view.error).toBeNull();
    expect(view.generations).toHaveLength(2);
    const generation1 = view.generations[0] as { generation: { index: number }; executions: { record: { index: number; stateId: string; stageTask: unknown } }[] };
    const generation2 = view.generations[1] as typeof generation1;
    expect(generation1.executions.map((entry) => entry.record.index).sort()).toEqual([2, 5]);
    expect(generation2.executions).toHaveLength(0);
    // the two unbound planning executions (1 and 4) stay outside cards
    expect(view.unplacedExecutions).toHaveLength(3);
    // iterations of generation 1 carry the closure reasons of the fixture
    const card1 = view.generations[0] as unknown as { iterations: { closed: { by: string } | null }[] };
    expect(card1.iterations.map((iteration) => iteration.closed?.by)).toEqual(["grant", "replanned"]);
  });

  test("the historical group reports the whole intervention chain", () => {
    const view = deriveLifecycleHistory(fullState(), true);
    const historical = view.historical as {
      waits: { index: number; response: { actionId: string } | null }[];
      actions: { waitIndex: number; action: { id: string; to: string } }[];
      grants: { additionalIterations: number }[];
    };
    expect(historical.waits).toHaveLength(2);
    expect(historical.waits[0]?.response?.actionId).toBe("continue_stage");
    expect(historical.waits[1]?.response?.actionId).toBe("revise_task");
    expect(historical.actions.map((entry) => `${entry.waitIndex}:${entry.action.id}`)).toEqual(["1:continue_stage", "1:revise_task"]);
    expect(historical.grants).toHaveLength(1);
    expect(historical.grants[0]?.additionalIterations).toBe(1);
  });

  test("the historical group is withheld for a non-terminal inspect", () => {
    const view = deriveLifecycleHistory(fullState(), false);
    expect(view.historical).toBeNull();
  });

  test("a missing binding after completion is the typed structural failure", () => {
    const damaged = JSON.parse(JSON.stringify({ records: BINDING_RECORDS.slice(1) }));
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), identityPageB.after);
    state = applyBindingsPage(state, parseExecutionBindingsDocument({ ...bindingsPage, records: damaged.records }, RUN), 0);
    const view = deriveLifecycleHistory(state, true);
    expect(view.complete).toBe(false);
    expect(view.error).toEqual({ layer: "contract", reason: "missing_binding" });
  });

  test("a binding naming an unknown generation after completion is typed", () => {
    const damaged = JSON.parse(JSON.stringify(bindingsPage));
    damaged.records[1].generation_index = 9;
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageB, RUN), identityPageB.after);
    state = applyBindingsPage(state, parseExecutionBindingsDocument(damaged, RUN), 0);
    const view = deriveLifecycleHistory(state, true);
    expect(view.error).toEqual({ layer: "contract", reason: "unresolved_generation" });
  });

  test("an incomplete stream keeps placements honest instead of claiming completeness", () => {
    let state = createLifecycleHistoryState(RUN, 24);
    state = applyIdentityPage(state, parseIdentityDocument(identityPageA, RUN), 0);
    // bindings partially loaded: one record, more to come
    const partial = { ...bindingsPage, records: [BINDING_RECORDS[0]], next_after: 1 };
    state = applyBindingsPage(state, parseExecutionBindingsDocument(partial, RUN), 0);
    const view = deriveLifecycleHistory(state, false);
    expect(view.complete).toBe(false);
    expect(view.error).toBeNull();
    expect(view.unplacedExecutions.length).toBeGreaterThan(0);
  });
});

describe("ui lifecycle-history model: the two single-flight transports", () => {
  test("admission, supersede and the refusal of a busy continuation", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const paths: string[] = [];
    const transport = createIdentityTransport({
      fetchIdentity: async (path, _runId, signal) => {
        paths.push(path);
        await gate;
        if (signal?.aborted) {
          throw new Error("aborted");
        }
        return { ok: true, model: identityPageA };
      },
      onStarted: () => undefined,
      onSettled: () => undefined,
    });
    const applied: unknown[] = [];
    const firstSettled = transport.request({ runId: RUN, kind: "initial", path: identityRequestPath(RUN, 0, 24), onApplied: (result) => applied.push(result) });
    // an initial desire while busy supersedes the active one and becomes
    // the single coalesced follow-up
    const secondSettled = transport.request({ runId: RUN, kind: "initial", path: identityRequestPath(RUN, 0, 24), onApplied: () => applied.push("second") });
    // a continuation while busy is refused without a second fetch
    const continuationSettled = transport.request({ runId: RUN, kind: "continuation", path: identityRequestPath(RUN, 5, 24), onApplied: () => applied.push("continuation") });
    await continuationSettled;
    release!();
    await Promise.all([firstSettled, secondSettled]);
    await Bun.sleep(10);
    expect(applied).toHaveLength(1);
    expect(paths).toHaveLength(2); // the superseded attempt + its coalesced follow-up
    transport.dispose();
  });

  test("invalidate withdraws the queued desire and dispose settles everything", async () => {
    const transport = createExecutionBindingsTransport({
      fetchBindings: async () => {
        await new Promise<void>(() => undefined);
        return { ok: true };
      },
      onStarted: () => undefined,
      onSettled: () => undefined,
    });
    const applied: unknown[] = [];
    const firstSettled = transport.request({ runId: RUN, kind: "initial", path: executionBindingsRequestPath(RUN, 0, 1), onApplied: (result) => applied.push(result) });
    const queuedSettled = transport.request({ runId: RUN, kind: "initial", path: executionBindingsRequestPath(RUN, 0, 1), onApplied: () => applied.push("queued") });
    transport.invalidate();
    expect(await queuedSettled).toBeUndefined();
    transport.dispose();
    await Bun.sleep(10);
    expect(applied).toHaveLength(0);
  });
});

describe("ui lifecycle-history model: source restrictions and canaries", () => {
  test("the section never POSTs and owns no timers or storage", () => {
    const historyStart = appSource.indexOf("Lifecycle history (v9): the two stream transports");
    const historyEnd = appSource.indexOf("Thin DOM adapter", historyStart);
    const region = appSource.slice(historyStart, historyEnd);
    expect(region).not.toContain("method: \"POST\"");
    expect(region).not.toContain("setTimeout");
    expect(region).not.toContain("setInterval");
    expect(region).not.toContain("localStorage");
  });

  test("the parsers bind the two distinct kinds; no second serializer exists", () => {
    expect(appSource.split(`kind: "identity"`).length - 1).toBeLessThanOrEqual(2);
    expect(appSource.split(`kind: "execution_bindings"`).length - 1).toBeLessThanOrEqual(2);
  });
});
