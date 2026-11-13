/**
 * The versioned read-only durable-identity projection (P-3): the stable
 * external projection of the run's durable identity and ledgers.
 *
 * These tests pin: the single projection owner (the twelve record kinds
 * with their exact own-key shapes and in-band discriminators, the fixed
 * deterministic stream order, every nullable normalization rule, the
 * flattened decision id lists and stage-task binding), the
 * revision-pinned cursor contract (revision pin before cursor-range
 * checks, typed stale_snapshot/invalid_cursor, no partial page), the
 * full-document byte cap with the deterministic stop-before partition
 * (computed on the larger CLI envelope) and the pagination exhaust
 * equality, the restart-stable byte-identical pages through the real
 * store, the path-free CLI runner over the one shared read-only load
 * chain (failure shapes, cross-field grammar), and the source
 * restrictions of the identity layer.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { canonicalJson } from "../src/canonical_json.ts";
import {
  buildPipelineV2IdentityRecords,
  identityPipelineV2Run,
  PIPELINE_V2_IDENTITY_MAX_LIMIT,
  PIPELINE_V2_IDENTITY_PAGE_BYTE_LIMIT,
  projectPipelineV2IdentityPage,
  PIPELINE_V2_IDENTITY_RECORD_KINDS,
  PipelineV2IdentityError,
  type PipelineV2IdentityRecord,
} from "../src/pipeline_v2_identity.ts";
import { PipelineV2RunStateStore } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import type { PipelineV2RunCommand, PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import {
  apply,
  hex,
  IDENTITY,
  lifecycleCommands,
  planningLadder,
  stageLadder,
  TICKS,
  transition,
} from "./pipeline_v2_commit_history_fixture.ts";
import { E2E_YAML } from "./pipeline_v2_events_fixture.ts";
import { isCleanAbsolutePath } from "../src/clean_path.ts";

const V2_PROFILE_EPOCH_BINDING = { digest: "d".repeat(64), profile_count: 1 };
const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

const ROOTS: string[] = [];
afterEach(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

/** The reducer-built 66-command lifecycle state, validated by the single loader. */
function lifecycleState(): PipelineV2RunState {
  return apply(lifecycleCommands());
}

/** The last record matching a discriminator of the lifecycle stream. */
function only<K extends PipelineV2IdentityRecord["record"]>(
  records: readonly PipelineV2IdentityRecord[],
  kind: K,
): Extract<PipelineV2IdentityRecord, { record: K }> {
  const found = records.filter((record): record is Extract<PipelineV2IdentityRecord, { record: K }> => record.record === kind);
  expect(found.length).toBe(1);
  const record = found[0]!;
  expect(record.record).toBe(kind);
  return record;
}

const ownKeys = (record: unknown): string[] => Object.keys(record as object).sort();

describe("identity record stream (12 kinds, deterministic durable order)", () => {
  test("the twelve kinds are covered by the lifecycle and the kind vocabulary is closed", () => {
    const records = buildPipelineV2IdentityRecords(lifecycleState());
    const kinds = new Set(records.map((record) => record.record));
    // The lifecycle stream covers ten of the twelve kinds; the two
    // decision-side kinds are covered by the dedicated decision battery.
    expect(PIPELINE_V2_IDENTITY_RECORD_KINDS.length).toBe(12);
    const missing = [...kinds].filter(
      (kind) => !PIPELINE_V2_IDENTITY_RECORD_KINDS.includes(kind as never),
    );
    expect(missing).toEqual([]);
    expect(kinds.has("decision_active_constraint")).toBe(false);
    expect(kinds.has("decision_violated_relation")).toBe(false);
    for (const record of records) {
      expect(record.record).toEqual(expect.any(String));
      expect(ownKeys(record)).toContain("record");
      expect(PIPELINE_V2_IDENTITY_RECORD_KINDS).toContain(record.record);
    }
  });

  test("plan revisions project the fixed five-field ledger shape with the null predecessor", () => {
    const plans = buildPipelineV2IdentityRecords(lifecycleState()).filter(
      (record) => record.record === "plan_revision",
    );
    expect(plans.length).toBe(2);
    const plan1 = plans[0] as Record<string, unknown>;
    expect(plan1).toEqual({
      record: "plan_revision",
      index: 1,
      revision: 1,
      sha256: hex("3"),
      previous_sha256: null,
      origin_execution: 1,
    });
  });

  test("task revisions project the exact wait-bound fields with null normalization", () => {
    const records = buildPipelineV2IdentityRecords(lifecycleState()).filter(
      (record) => record.record === "task_revision",
    );
    expect(records.map((record) => (record as { task_id: string; revision: number }).task_id)).toEqual([
      "task-a",
      "task-b",
      "task-a",
      "task-c",
      "task-d",
    ]);
    const first = records[0] as Record<string, unknown>;
    expect(first).toEqual({
      record: "task_revision",
      index: 1,
      task_id: "task-a",
      revision: 1,
      sha256: hex("1"),
      previous_sha256: null,
      wait_index: null,
      intent_sha256: null,
    });
    const revised = records[2] as Record<string, unknown>;
    expect(revised).toEqual({
      record: "task_revision",
      index: 3,
      task_id: "task-a",
      revision: 2,
      sha256: hex("f"),
      previous_sha256: hex("1"),
      wait_index: 2,
      intent_sha256: hex("c"),
    });
  });

  test("generations project the exact stage binding, lifecycle projection and closed reason", () => {
    const generations = buildPipelineV2IdentityRecords(lifecycleState()).filter(
      (record) => record.record === "generation",
    );
    expect(generations.length).toBe(2);
    expect(generations[0]).toEqual({
      record: "generation",
      index: 1,
      stage_id: "stage-1",
      stage_position: 1,
      template_id: "development",
      plan_sha256: hex("3"),
      initial_budget: 2,
      opened_transition_count: 0,
      iteration_count: 2,
      open_iteration: null,
      closed: { by: "replanned", closed_transition_count: 3 },
    });
    expect(generations[1]).toEqual({
      record: "generation",
      index: 2,
      stage_id: "stage-1",
      stage_position: 1,
      template_id: "development",
      plan_sha256: hex("4"),
      initial_budget: 3,
      opened_transition_count: 3,
      iteration_count: 1,
      open_iteration: { index: 1, opened_transition_count: 3 },
      closed: null,
    });
  });

  test("iterations project flattened with the generation back-reference and the wait binding", () => {
    const iterations = buildPipelineV2IdentityRecords(lifecycleState()).filter(
      (record) => record.record === "iteration",
    );
    expect(iterations).toEqual([
      {
        record: "iteration",
        generation_index: 1,
        index: 1,
        opened_transition_count: 0,
        closed: { by: "grant", wait_index: 1, closed_transition_count: 2 },
      },
      {
        record: "iteration",
        generation_index: 1,
        index: 2,
        opened_transition_count: 2,
        closed: { by: "replanned", wait_index: 2, closed_transition_count: 3 },
      },
      {
        record: "iteration",
        generation_index: 2,
        index: 1,
        opened_transition_count: 3,
        closed: null,
      },
    ]);
  });

  test("grants project the exact linkage and amount", () => {
    expect(only(buildPipelineV2IdentityRecords(lifecycleState()), "grant")).toEqual({
      record: "grant",
      index: 1,
      generation_index: 1,
      wait_index: 1,
      intent_sha256: hex("9"),
      additional_iterations: 1,
    });
  });

  test("waits project the record plus flattened declared actions in order", () => {
    const records = buildPipelineV2IdentityRecords(lifecycleState());
    const wait1 = records.filter((record) => record.record === "wait")[0] as Record<string, unknown>;
    expect(wait1).toEqual({
      record: "wait",
      index: 1,
      transition_count: 2,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      request_sha256: hex("7"),
      intent_sha256: hex("9"),
      response: { action_id: "continue_stage", response_sha256: hex("8") },
    });
    const actions = records.filter((record) => record.record === "wait_action");
    expect(actions).toEqual([
      { record: "wait_action", wait_index: 1, id: "continue_stage", to: "dev_entry" },
      { record: "wait_action", wait_index: 1, id: "revise_task", to: "architect" },
      { record: "wait_action", wait_index: 2, id: "continue_stage", to: "dev_entry" },
      { record: "wait_action", wait_index: 2, id: "revise_task", to: "architect" },
    ]);
    const wait2 = records.filter((record) => record.record === "wait")[1] as Record<string, unknown>;
    expect(wait2.response).toEqual({ action_id: "revise_task", response_sha256: hex("0") });
  });

  test("agent executions project the scalar identity fields and flattened outputs", () => {
    const records = buildPipelineV2IdentityRecords(lifecycleState());
    const agents = records.filter((record) => record.record === "execution_agent");
    expect(agents.length).toBe(5);
    const firstPlanning = agents[0] as Record<string, unknown>;
    expect(firstPlanning).toEqual({
      record: "execution_agent",
      index: 1,
      type: "agent",
      state_id: "architect",
      execution_role: "planning",
      iteration_index: null,
      phase: "cleanup_completed",
      stage_task: null,
    });
    const firstStage = agents[1] as Record<string, unknown>;
    expect(firstStage).toEqual({
      record: "execution_agent",
      index: 2,
      type: "agent",
      state_id: "dev_entry",
      execution_role: "stage",
      iteration_index: 1,
      phase: "cleanup_completed",
      stage_task: null,
    });
    const outputs = records.filter((record) => record.record === "agent_output");
    expect(outputs).toEqual([
      { record: "agent_output", execution_index: 1, id: "plan", digest: hex("d") },
      { record: "agent_output", execution_index: 2, id: "report", digest: hex("e") },
      { record: "agent_output", execution_index: 3, id: "report", digest: hex("e") },
      { record: "agent_output", execution_index: 4, id: "plan", digest: hex("d") },
      { record: "agent_output", execution_index: 5, id: "report", digest: hex("e") },
    ]);
  });

  test("the stream order is the fixed journal concatenation and repeated builds are identical", () => {
    const state = lifecycleState();
    const records = [...buildPipelineV2IdentityRecords(state)];
    const kindsInOrder = records.map((record) => record.record);
    const expectedPlanBlock: PipelineV2IdentityRecord["record"][] = ["plan_revision", "plan_revision"];
    expect(kindsInOrder.slice(0, 2)).toEqual(expectedPlanBlock);
    expect(records[2]!.record).toBe("task_revision");
    const generationPositions = kindsInOrder
      .map((kind, index) => (kind === "generation" ? index : -1))
      .filter((index) => index >= 0);
    expect(generationPositions.length).toBe(2);
    expect(kindsInOrder[generationPositions[0]! + 1]).toBe("iteration");
    expect(kindsInOrder[generationPositions[1]! + 1]).toBe("iteration");
    expect(buildPipelineV2IdentityRecords(state)).toEqual(records);
  });
});

/** A decision-state run prefix through the exact command payload. */
function decisionCommands(
  result: PipelineV2RunCommand extends never ? never : Extract<PipelineV2RunCommand, { kind: "decision_evaluated" }> | null,
): PipelineV2RunCommand[] {
  const prefix: PipelineV2RunCommand[] = [
    { kind: "create_run", runId: "run-decision", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING, profileEpoch: V2_PROFILE_EPOCH_BINDING },
    { kind: "start_decision_execution", stateId: "architect", inputDigest: hex("2"), executionRole: "control" },
  ];
  if (result !== null) {
    prefix.push(result);
  }
  return prefix;
}

describe("decision execution records (scalars inline, id lists flattened)", () => {
  test("selected projects the scalars and its active constraints in durable order", () => {
    const records = buildPipelineV2IdentityRecords(apply(decisionCommands({
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "accept",
        decision: "accept",
        rule_id: "R7",
        active_constraint_ids: ["HC2", "HC1", "HC5"],
      },
    })));
    const decision = only(records, "execution_decision") as Record<string, unknown>;
    expect(decision).toEqual({
      record: "execution_decision",
      index: 1,
      type: "decision",
      state_id: "architect",
      execution_role: "control",
      iteration_index: null,
      phase: "evaluated",
      input_digest: hex("2"),
      result: { status: "selected", outcome: "accept", decision: "accept", rule_id: "R7" },
    });
    const constraints = records.filter((record) => record.record === "decision_active_constraint");
    expect(constraints).toEqual([
      { record: "decision_active_constraint", execution_index: 1, position: 0, id: "HC2" },
      { record: "decision_active_constraint", execution_index: 1, position: 1, id: "HC1" },
      { record: "decision_active_constraint", execution_index: 1, position: 2, id: "HC5" },
    ]);
    expect(records.some((record) => record.record === "decision_violated_relation")).toBe(false);
  });

  test("uncovered and inconsistent flatten their own id lists with durable positions", () => {
    const uncovered = buildPipelineV2IdentityRecords(apply(decisionCommands({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: ["HC3"] },
    })));
    expect(only(uncovered, "execution_decision").result).toEqual({ status: "uncovered", outcome: "uncovered" });
    expect(uncovered.filter((record) => record.record === "decision_active_constraint")).toEqual([
      { record: "decision_active_constraint", execution_index: 1, position: 0, id: "HC3" },
    ]);

    const inconsistent = buildPipelineV2IdentityRecords(apply(decisionCommands({
      kind: "decision_evaluated",
      result: { status: "inconsistent_facts", outcome: "inconsistent_facts", violated_relation_ids: ["FC2", "FC1"] },
    })));
    expect(only(inconsistent, "execution_decision").result).toEqual({
      status: "inconsistent_facts",
      outcome: "inconsistent_facts",
    });
    expect(inconsistent.filter((record) => record.record === "decision_violated_relation")).toEqual([
      { record: "decision_violated_relation", execution_index: 1, position: 0, id: "FC2" },
      { record: "decision_violated_relation", execution_index: 1, position: 1, id: "FC1" },
    ]);
  });

  test("invalid_facts projects the structured scalars with null coercion and no id records", () => {
    const records = buildPipelineV2IdentityRecords(apply(decisionCommands({
      kind: "decision_evaluated",
      result: { status: "invalid_facts", outcome: "invalid_facts", reason: "non_boolean_fact", fact_id: "ready", actual_type: "string" },
    })));
    expect(only(records, "execution_decision").result).toEqual({
      status: "invalid_facts",
      outcome: "invalid_facts",
      reason: "non_boolean_fact",
      fact_id: "ready",
      actual_type: "string",
    });
    expect(records.some((record) => record.record === "decision_active_constraint")).toBe(false);
    expect(records.some((record) => record.record === "decision_violated_relation")).toBe(false);
    const bare = buildPipelineV2IdentityRecords(apply(decisionCommands({
      kind: "decision_evaluated",
      result: { status: "invalid_facts", outcome: "invalid_facts", reason: "unknown_fact" },
    })));
    expect(only(bare, "execution_decision").result).toEqual({
      status: "invalid_facts",
      outcome: "invalid_facts",
      reason: "unknown_fact",
      fact_id: null,
      actual_type: null,
    });
  });

  test("a started-but-unevaluated decision execution projects result null and no flattened records", () => {
    const records = buildPipelineV2IdentityRecords(apply(decisionCommands(null)));
    const decision = only(records, "execution_decision") as Record<string, unknown>;
    expect(decision.phase).toBe("evaluating");
    expect(decision.result).toBeNull();
    expect(records.some((record) => record.record === "decision_active_constraint")).toBe(false);
    expect(records.some((record) => record.record === "decision_violated_relation")).toBe(false);
  });
});

/** A stage-task run: one accepted task, one open generation/iteration, one bound stage execution. */
function stageTaskCommands(): PipelineV2RunCommand[] {
  return [
    { kind: "create_run", runId: "run-stage-task", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING, profileEpoch: V2_PROFILE_EPOCH_BINDING },
    ...planningLadder("sess-st-1", "tool-st-1"),
    { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1") },
    { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1 },
    transition("architect", "dev_entry", 1),
    {
      kind: "stage_generation_opened",
      stageId: "stage-1",
      stagePosition: 1,
      templateId: "development",
      planSha256: hex("3"),
      initialBudget: 1,
      transitionCount: 1,
    },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 },
    { kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 },
    {
      kind: "agent_data_prepared",
      stageTask: { taskId: "task-a", taskRevision: 1, taskManifestSha256: hex("1"), bodySha256: hex("2") },
    },
  ];
}

describe("the mandatory stage-task binding on execution_agent records", () => {
  test("a bound stage execution projects the exact four-member binding", () => {
    const records = buildPipelineV2IdentityRecords(apply(stageTaskCommands()));
    const agents = records.filter((record) => record.record === "execution_agent");
    expect(agents.length).toBe(2);
    const agent = agents[1] as Record<string, unknown>;
    expect(agent).toEqual({
      record: "execution_agent",
      index: 2,
      type: "agent",
      state_id: "dev_entry",
      execution_role: "stage",
      iteration_index: 1,
      phase: "data_prepared",
      stage_task: {
        task_id: "task-a",
        task_revision: 1,
        task_manifest_sha256: hex("1"),
        body_sha256: hex("2"),
      },
    });
  });

  test("the lifecycle's unbound executions keep stage_task null (exact shape, not absence)", () => {
    for (const agent of buildPipelineV2IdentityRecords(lifecycleState()).filter(
      (record) => record.record === "execution_agent",
    )) {
      expect((agent as Record<string, unknown>).stage_task).toBeNull();
      expect(ownKeys(agent)).toContain("stage_task");
    }
  });
});

describe("revision-pinned cursor contract", () => {
  const state = () => lifecycleState();

  test("the default page fits the lifecycle stream and is byte-identical across repeats", () => {
    const page = projectPipelineV2IdentityPage(state(), { after: 0, limit: 100 });
    expect(page.revision).toBe(66);
    expect(page.records.length).toBe(29);
    expect(page.nextAfter).toBeNull();
    expect(projectPipelineV2IdentityPage(state(), {})).toEqual(page);
    expect(Object.isFrozen(page)).toBe(true);
    expect(Object.isFrozen(page.records)).toBe(true);
  });

  test("limit slices the stream with the exact exclusive cursor and nextAfter", () => {
    const state = lifecycleState();
    const all = [...buildPipelineV2IdentityRecords(state)];
    const first = projectPipelineV2IdentityPage(state, { limit: 10 });
    expect(first.after).toBe(0);
    expect(first.records.length).toBe(10);
    expect(first.nextAfter).toBe(10);
    expect(first.records[9]).toEqual(all[9]);
    const second = projectPipelineV2IdentityPage(state, { after: 10, revision: 66, limit: 10 });
    expect(second.records[0]).toEqual(all[10]);
    expect(second.nextAfter).toBe(20);
  });

  test("pagination exhausts exactly: concatenation equals the builder stream", () => {
    const state = lifecycleState();
    const all = [...buildPipelineV2IdentityRecords(state)];
    const walked: PipelineV2IdentityRecord[] = [];
    let after = 0;
    for (;;) {
      const page = projectPipelineV2IdentityPage(state, { after, limit: 8 });
      walked.push(...page.records);
      if (page.nextAfter === null) break;
      after = page.nextAfter;
    }
    expect(walked).toEqual(all);
    const finalPage = projectPipelineV2IdentityPage(state, { after: all.length, limit: 8 });
    expect(finalPage.records).toEqual([]);
    expect(finalPage.nextAfter).toBeNull();
  });

  test("after at the stream end is an empty page, never an error", () => {
    const page = projectPipelineV2IdentityPage(state(), { after: 29, revision: 66 });
    expect(page.records).toEqual([]);
    expect(page.nextAfter).toBeNull();
  });

  test("a cursor beyond the stream is the one typed invalid_cursor, never a partial page", () => {
    let error: unknown;
    try {
      projectPipelineV2IdentityPage(state(), { after: 30, revision: 66 });
    } catch (cause) {
      error = cause;
    }
    expect(error).toBeInstanceOf(PipelineV2IdentityError);
    expect((error as PipelineV2IdentityError).reason).toBe("invalid_cursor");
    const descriptor = Object.getOwnPropertyDescriptor(error as PipelineV2IdentityError, "reason");
    expect(descriptor?.writable).toBe(false);
  });

  test("grammar refusals are typed invalid_cursor on both cursor and limit members", () => {
    expect(() => projectPipelineV2IdentityPage(state(), { after: -1 })).toThrow(PipelineV2IdentityError);
    let caught: PipelineV2IdentityError | null = null;
    try {
      projectPipelineV2IdentityPage(state(), { after: -1 });
    } catch (cause) {
      caught = cause as PipelineV2IdentityError;
    }
    expect(caught?.reason).toBe("invalid_cursor");
    for (const limit of [0, 201, 1.5]) {
      let limitError: PipelineV2IdentityError | null = null;
      try {
        projectPipelineV2IdentityPage(state(), { limit });
      } catch (cause) {
        limitError = cause as PipelineV2IdentityError;
      }
      expect(limitError?.reason).toBe("invalid_cursor");
    }
  });

  test("a matching revision pin is accepted; a mismatch is stale_snapshot BEFORE cursor-range checks", () => {
    const matching = projectPipelineV2IdentityPage(state(), { revision: 66 });
    expect(matching.records.length).toBe(29);
    let mismatch: PipelineV2IdentityError | null = null;
    try {
      projectPipelineV2IdentityPage(state(), { revision: 65 });
    } catch (cause) {
      mismatch = cause as PipelineV2IdentityError;
    }
    expect(mismatch?.reason).toBe("stale_snapshot");
    // The precedence proof: an out-of-range cursor with an unmatched
    // revision is stale_snapshot, never invalid_cursor.
    let error: PipelineV2IdentityError | null = null;
    try {
      projectPipelineV2IdentityPage(state(), { after: 999, revision: 65 });
    } catch (cause) {
      error = cause as PipelineV2IdentityError;
    }
    expect(error?.reason).toBe("stale_snapshot");
    // Even the first page re-proves an explicitly given pin.
    let firstPageError: PipelineV2IdentityError | null = null;
    try {
      projectPipelineV2IdentityPage(state(), { after: 0, revision: 65 });
    } catch (cause) {
      firstPageError = cause as PipelineV2IdentityError;
    }
    expect(firstPageError?.reason).toBe("stale_snapshot");
  });
});

/** One accepted task revision per synthetic id, enough pages to cross the byte cap. */
function wideTaskState(): PipelineV2RunState {
  const commands: PipelineV2RunCommand[] = [
    { kind: "create_run", runId: "run-wide", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING, profileEpoch: V2_PROFILE_EPOCH_BINDING },
  ];
  for (let index = 0; index < 1400; index += 1) {
    commands.push({
      kind: "task_revision_accepted",
      taskId: `task-${String(index).padStart(5, "0")}`,
      revision: 1,
      taskSha256: hex(String.fromCharCode(48 + (index % 10))),
    });
  }
  return apply(commands);
}

describe("the full-document byte cap and deterministic partition", () => {
  test("1400 task revisions cross the byte cap; every page fits and the walk exhausts exactly", () => {
    const state = wideTaskState();
    const total = buildPipelineV2IdentityRecords(state).length;
    expect(total).toBe(1400);
    const walked: PipelineV2IdentityRecord[] = [];
    let after = 0;
    let pages = 0;
    for (;;) {
      const page = projectPipelineV2IdentityPage(state, { after, revision: undefined, limit: PIPELINE_V2_IDENTITY_MAX_LIMIT });
      pages += 1;
      const cliDocument = `${canonicalJson({
        ok: true,
        exitCode: 0,
        schemaVersion: 1,
        runId: "wide-run-id",
        revision: page.revision,
        after: page.after,
        records: page.records,
        nextAfter: page.nextAfter,
      })}\n`;
      expect(Buffer.byteLength(cliDocument, "utf8")).toBeLessThanOrEqual(PIPELINE_V2_IDENTITY_PAGE_BYTE_LIMIT);
      expect(page.records.length).toBeGreaterThan(0);
      walked.push(...page.records);
      if (page.nextAfter === null) break;
      after = page.nextAfter;
    }
    expect(pages).toBeGreaterThan(1);
    expect(walked).toEqual([...buildPipelineV2IdentityRecords(state)]);
  });

  test("the partition is deterministic: identical limits rebuild identical pages", () => {
    const state = wideTaskState();
    const a = projectPipelineV2IdentityPage(state, { after: 0, limit: PIPELINE_V2_IDENTITY_MAX_LIMIT });
    const b = projectPipelineV2IdentityPage(state, { after: 0, limit: PIPELINE_V2_IDENTITY_MAX_LIMIT });
    expect(canonicalJson({ records: a.records })).toEqual(canonicalJson({ records: b.records }));
  });
});

async function makeDirs(prefix: string): Promise<{ stateRoot: string; bundle: string }> {
  const root = await mkdtemp(join(tmpdir(), `identity-${prefix}-`));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  const bundle = join(root, "bundle");
  await import("node:fs/promises").then((fs) => Promise.all([fs.mkdir(stateRoot), fs.mkdir(bundle)]));
  return { stateRoot, bundle };
}

async function writeSimpleBundle(bundle: string): Promise<void> {
  const { mkdir, writeFile } = await import("node:fs/promises");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), E2E_YAML);
  await writeFile(join(bundle, "prompts", "architect.md"), "architect prompt body");
  await writeFile(join(bundle, "prompts", "coder.md"), "coder prompt body");
  await writeFile(
    join(bundle, "schemas", "plan.schema.json"),
    JSON.stringify({ type: "object" }),
  );
}

async function seedE2ERun(stateRoot: string, bundle: string, runId: string): Promise<void> {
  const pipeline = await loadPipelineV2(bundle);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: [],
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  for (const command of lifecycleCommands().slice(1)) {
    await sink.dispatch(command as never);
  }
}

describe("the CLI runner over the shared read-only chain (path-free documents)", () => {
  test("a missing run refuses run_not_found with the exact path-free failure shape", async () => {
    const dirs = await makeDirs("missing");
    const outcome = await identityPipelineV2Run({ runId: "no-such", stateRoot: dirs.stateRoot });
    expect(outcome.ok).toBe(false);
    expect(ownKeys(outcome)).toEqual(["exitCode", "ok", "reason", "runId"]);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("run_not_found");
    expect(JSON.stringify(outcome)).not.toContain("pipeline-runs");
  });

  test("a continuation without its revision pin refuses invalid_cursor before any run access", async () => {
    const dirs = await makeDirs("no-pin");
    const outcome = await identityPipelineV2Run({ runId: "no-such", stateRoot: dirs.stateRoot, after: 5 });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("invalid_cursor");
  });

  test("success pages carry the exact path-free envelope and revision-pin continuations", async () => {
    const dirs = await makeDirs("cli");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-run");
    const first = await identityPipelineV2Run({ runId: "id-run", stateRoot: dirs.stateRoot });
    if (!first.ok) throw new Error(`unreachable: ${first.reason}`);
    expect(ownKeys(first)).toEqual([
      "after",
      "exitCode",
      "nextAfter",
      "ok",
      "records",
      "revision",
      "runId",
      "schemaVersion",
    ]);
    expect(first.schemaVersion).toBe(1);
    expect(first.revision).toBe(66);
    expect(first.after).toBe(0);
    expect(first.records.length).toBe(29);
    expect(first.nextAfter).toBeNull();
    // A matching pin on the first page is accepted.
    const pinned = await identityPipelineV2Run({ runId: "id-run", stateRoot: dirs.stateRoot, revision: 66 });
    if (!pinned.ok) throw new Error("unreachable");
    expect(pinned.records.length).toBe(29);
    // A continuation pins its revision; a wrong pin is stale_snapshot.
    const continuation = await identityPipelineV2Run({
      runId: "id-run",
      stateRoot: dirs.stateRoot,
      after: 10,
      revision: 66,
      limit: 5,
    });
    if (!continuation.ok) throw new Error("unreachable");
    expect(continuation.records.length).toBe(5);
    expect(continuation.nextAfter).toBe(15);
    const stale = await identityPipelineV2Run({
      runId: "id-run",
      stateRoot: dirs.stateRoot,
      after: 10,
      revision: 65,
    });
    if (stale.ok) throw new Error("unreachable");
    expect(stale.reason).toBe("stale_snapshot");
    const afterOutOfRange = await identityPipelineV2Run({
      runId: "id-run",
      stateRoot: dirs.stateRoot,
      after: 999,
      revision: 66,
    });
    if (afterOutOfRange.ok) throw new Error("unreachable");
    expect(afterOutOfRange.reason).toBe("invalid_cursor");
  });

  test("a broken bundle behind a valid state refuses pipeline_mismatch with no path leaked", async () => {
    const dirs = await makeDirs("mismatch");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-mis");
    const { writeFile } = await import("node:fs/promises");
    await writeFile(join(dirs.bundle, "prompts", "architect.md"), "drifted architect prompt body");
    const outcome = await identityPipelineV2Run({ runId: "id-mis", stateRoot: dirs.stateRoot });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("pipeline_mismatch");
    expect(JSON.stringify(outcome)).not.toContain(dirs.bundle);
  });

  test("the deep-frozen surface: outputs and records resist caller mutation", async () => {
    const dirs = await makeDirs("freeze");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-freeze");
    const outcome = await identityPipelineV2Run({ runId: "id-freeze", stateRoot: dirs.stateRoot });
    if (!outcome.ok) throw new Error("unreachable");
    expect(Object.isFrozen(outcome)).toBe(true);
    expect(Object.isFrozen(outcome.records)).toBe(true);
    const records = buildPipelineV2IdentityRecords(lifecycleState());
    expect(Object.isFrozen(records)).toBe(true);
  });
});

describe("restart-stable byte-identity through the real store", () => {
  test("a page rebuilt after reopen is byte-identical to the committed page", async () => {
    const dirs = await makeDirs("restart");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-restart");
    const load = async (): Promise<PipelineV2RunState | null> => {
      const store = new PipelineV2RunStateStore({ stateRoot: dirs.stateRoot, runId: "id-restart" });
      return await store.load();
    };
    const state = await load();
    if (state === null) throw new Error("unreachable");
    const pageOne = projectPipelineV2IdentityPage(state, { limit: 12 });
    const reopened = await load();
    if (reopened === null) throw new Error("unreachable");
    const pageTwo = projectPipelineV2IdentityPage(reopened, { limit: 12 });
    expect(canonicalJson(pageOne)).toEqual(canonicalJson(pageTwo));
    const serializedOne = canonicalJson((await identityPipelineV2Run({ runId: "id-restart", stateRoot: dirs.stateRoot })) as object);
    const serializedTwo = canonicalJson((await identityPipelineV2Run({ runId: "id-restart", stateRoot: dirs.stateRoot })) as object);
    expect(serializedOne).toEqual(serializedTwo);
  });
});

describe("leakage and source restrictions", () => {
  test("no identity document carries a path, credential, session id, config or body", async () => {
    const docs: string[] = [];
    docs.push(JSON.stringify(buildPipelineV2IdentityRecords(lifecycleState())));
    docs.push(JSON.stringify(buildPipelineV2IdentityRecords(apply(stageTaskCommands()))));
    const dirs = await makeDirs("leak");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-leak");
    const outcome = await identityPipelineV2Run({ runId: "id-leak", stateRoot: dirs.stateRoot });
    docs.push(JSON.stringify(outcome));
    docs.push(JSON.stringify(await identityPipelineV2Run({ runId: "no-such", stateRoot: dirs.stateRoot })));
    const joined = docs.join("\n");
    for (const canary of [
      "/opt/",
      "/tmp/",
      "pipeline-runs",
      "state.json",
      "bundle_root",
      "runRoot",
      "credential",
      "dhl_",
      "sess-",
      "tool-",
      "OPENCODE",
      "socket",
      "prompt body",
      "worker_log",
      "worker-log",
      "commit_history",
      "profile_epoch",
      "started_at",
      "updated_at",
      "execution_session_id",
      "tool_session_id",
    ]) {
      expect(joined).not.toContain(canary);
    }
  });

  test("the identity layer owns no second parser, event reconstruction or artifact read", async () => {
    const source = await import("node:fs/promises").then((fs) => fs.readFile(
      join(import.meta.dir, "..", "src", "pipeline_v2_identity.ts"),
      "utf8",
    ));
    expect(source).toContain("loadDurablePipelineV2Run");
    expect(source).not.toContain("reducePipelineV2RunCommand");
    expect(source).not.toContain("pipeline_v2_events");
    expect(source).not.toContain("readFile");
    expect(source).not.toContain("readdir");
    expect(source).not.toContain("dispatch(");
    expect(source).not.toContain("projectPipelineV2Events");
    expect(source).not.toContain("publishPipelineV2");
    expect(source).not.toContain("workerLog");
    expect(isCleanAbsolutePath("/tmp")).toBe(true);
  });
});
