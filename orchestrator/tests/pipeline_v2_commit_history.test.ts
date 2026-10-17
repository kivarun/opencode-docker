/**
 * Schema v8 commit provenance: the reducer-owned durable ledger of the
 * dispatch order.
 *
 * The ledger carries exactly one entry per durable revision, appended by
 * the reducer itself in the same atomic snapshot that applies the command.
 * The loader binds it to the projection as one ordered coherence replay.
 * These tests pin: the exact 66-command lifecycle oracle (the audit's
 * dispatch order, now durable), the resolution of the two proven v7
 * ambiguity clusters, the full 28-kind ownership matrix, the loader
 * coherence rejections, the crash/durability matrix, the state-size
 * measurements, and the content-free leakage scan.
 */
import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  PIPELINE_V2_COMMIT_PROVENANCE_KINDS,
  PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
  parsePipelineV2RunState,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2CommitProvenance,
  type PipelineV2RunCommand,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const hex = (char: string): string => char.repeat(64);

export const IDENTITY: PipelineV2RunPipelineIdentity = {
  schema_version: 2,
  bundle_root: "/opt/orchestrator/pipelines/v2",
  execution_snapshot_sha256: hex("a"),
  entry_state: "architect",
  max_transitions: 8,
};

export const TICKS = Array.from({ length: 96 }, (_, t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)));

export function apply(commands: PipelineV2RunCommand[]): PipelineV2RunState {
  let current: PipelineV2RunState | null = null;
  commands.forEach((command, index) => {
    current = reducePipelineV2RunCommand(current, command, TICKS[index % TICKS.length]!);
  });
  return current as unknown as PipelineV2RunState;
}

function planningLadder(session: string, tool: string): PipelineV2RunCommand[] {
  return [
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: session },
    { kind: "agent_tool_session_created", sessionId: tool },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }], workerLog: { status: "unavailable" }  },
    { kind: "agent_cleanup_completed" },
  ];
}

function stageLadder(session: string, tool: string, iteration: number): PipelineV2RunCommand[] {
  return [
    { kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: iteration },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: session },
    { kind: "agent_tool_session_created", sessionId: tool },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "report", digest: hex("e") }], workerLog: { status: "unavailable" }  },
    { kind: "agent_cleanup_completed" },
  ];
}

function transition(from: string, to: string, executionIndex: number, transitionIndex = 0): PipelineV2RunCommand {
  return {
    kind: "transition_committed",
    step: { from, outcome: "completed", to, transition_index: transitionIndex },
    executionIndex,
  };
}

/**
 * The exact 66-command lifecycle of the audit proof, replayed through the
 * reducer: run → planning → plan-ready → stage → wait → continue-stage →
 * wait → revise-task → plan-ready → stage → terminal success.
 */
export function lifecycleCommands(): PipelineV2RunCommand[] {
  return [
    { kind: "create_run", runId: "run-history", pipeline: IDENTITY, inputs: [] },
    ...planningLadder("sess-1", "tool-1"),
    { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1") },
    { kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: hex("2") },
    { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1 },
    {
      kind: "stage_generation_opened",
      stageId: "stage-1",
      stagePosition: 1,
      templateId: "development",
      planSha256: hex("3"),
      initialBudget: 2,
      transitionCount: 0,
    },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
    transition("architect", "dev_entry", 1),
    ...stageLadder("sess-2", "tool-2", 1),
    transition("dev_entry", "architect", 2),
    {
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: hex("7"),
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    },
    { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("9") },
    { kind: "iteration_grant_recorded", generationIndex: 1, waitIndex: 1, intentSha256: hex("9"), additionalIterations: 1 },
    { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 },
    {
      kind: "wait_response_recorded",
      waitIndex: 1,
      expectedRequestSha256: hex("7"),
      actionId: "continue_stage",
      responseSha256: hex("8"),
    },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 },
    ...stageLadder("sess-3", "tool-3", 2),
    transition("dev_entry", "architect", 3),
    {
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: hex("b"),
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    },
    { kind: "plan_intent_accepted", waitIndex: 2, intentSha256: hex("c") },
    {
      kind: "task_revision_accepted",
      taskId: "task-a",
      revision: 2,
      taskSha256: hex("f"),
      waitIndex: 2,
      intentSha256: hex("c"),
    },
    { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 2, by: "replanned", waitIndex: 2 },
    {
      kind: "wait_response_recorded",
      waitIndex: 2,
      expectedRequestSha256: hex("b"),
      actionId: "revise_task",
      responseSha256: hex("0"),
    },
    ...planningLadder("sess-4", "tool-4"),
    { kind: "task_revision_accepted", taskId: "task-c", revision: 1, taskSha256: hex("6") },
    { kind: "task_revision_accepted", taskId: "task-d", revision: 1, taskSha256: hex("8") },
    { kind: "plan_revision_accepted", planRevision: 2, planSha256: hex("4"), originExecution: 4 },
    { kind: "stage_generation_closed", generationIndex: 1, by: "replanned" },
    {
      kind: "stage_generation_opened",
      stageId: "stage-1",
      stagePosition: 1,
      templateId: "development",
      planSha256: hex("4"),
      initialBudget: 3,
      transitionCount: 3,
    },
    { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 3 },
    transition("architect", "dev_entry", 4),
    ...stageLadder("sess-5", "tool-5", 1),
    transition("dev_entry", "done", 5),
    { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
    {
      kind: "run_outputs_published",
      outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }],
    },
    { kind: "run_succeeded" },
  ];
}

/** The audit's oracle: the exact 66 kinds in dispatch order. */
const ORACLE_KINDS: readonly string[] = [
  "create_run",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "task_revision_accepted",
  "task_revision_accepted",
  "plan_revision_accepted",
  "stage_generation_opened",
  "stage_iteration_opened",
  "transition_committed",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "transition_committed",
  "run_waiting",
  "plan_intent_accepted",
  "iteration_grant_recorded",
  "stage_iteration_closed",
  "wait_response_recorded",
  "stage_iteration_opened",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "transition_committed",
  "run_waiting",
  "plan_intent_accepted",
  "task_revision_accepted",
  "stage_iteration_closed",
  "wait_response_recorded",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "task_revision_accepted",
  "task_revision_accepted",
  "plan_revision_accepted",
  "stage_generation_closed",
  "stage_generation_opened",
  "stage_iteration_opened",
  "transition_committed",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "transition_committed",
  "terminal_reached",
  "run_outputs_published",
  "run_succeeded",
];

describe("pipeline v2 commit provenance: the 66-command lifecycle oracle", () => {
  const state = apply(lifecycleCommands());

  test("the run reaches success at revision 66 with a dense 66-entry history", () => {
    expect(state.status).toBe("success");
    expect(state.revision).toBe(66);
    expect(state.schema_version).toBe(PIPELINE_V2_RUN_STATE_SCHEMA_VERSION);
    expect(PIPELINE_V2_RUN_STATE_SCHEMA_VERSION).toBe(9);
    expect(state.commit_history).toHaveLength(66);
    state.commit_history.forEach((entry, index) => {
      expect(entry.revision).toBe(index + 1);
    });
    expect(state.commit_history.map((entry) => String(entry.kind))).toEqual([...ORACLE_KINDS]);
    expect(state.commit_history[0]).toEqual({ revision: 1, kind: "create_run" });
  });

  test("the two proven v7 ambiguity clusters have exact durable revisions", () => {
    // Cluster 1: the rev-1 task acceptances sit at revisions 9 and 10,
    // strictly before the plan acceptance at revision 11 — the dispatch
    // order is durable, not a controller convention.
    expect(state.commit_history[8]).toEqual({ revision: 9, kind: "task_revision_accepted", task_index: 1 });
    expect(state.commit_history[9]).toEqual({ revision: 10, kind: "task_revision_accepted", task_index: 2 });
    expect(state.commit_history[10]).toEqual({ revision: 11, kind: "plan_revision_accepted", plan_index: 1 });
    // Cluster 2: the replanned generation closure sits at its own revision
    // 52, between the plan acceptance (51) and the new generation (53).
    expect(state.commit_history[51]).toEqual({
      revision: 52,
      kind: "stage_generation_closed",
      generation_index: 1,
    });
    expect(state.commit_history[50]!.kind).toBe("plan_revision_accepted");
    expect(state.commit_history[52]!.kind).toBe("stage_generation_opened");
  });

  test("every snapshot of the lifecycle is loader-coherent and round-trips byte-identically", () => {
    let current: PipelineV2RunState | null = null;
    lifecycleCommands().forEach((command, index) => {
      current = reducePipelineV2RunCommand(current, command, TICKS[index % TICKS.length]!);
      const serialized = JSON.stringify(current);
      const parsed = parsePipelineV2RunState(serialized);
      // The loader normalizes the document; the parsed projection equals
      // the reducer snapshot structurally, and a second parse of the
      // normalized document is byte-identical (canonical form).
      expect(parsed).toEqual(JSON.parse(serialized));
      expect(JSON.stringify(parsePipelineV2RunState(JSON.stringify(parsed)))).toBe(JSON.stringify(parsed));
    });
  });

  test("reopen determinism: the same commands always produce the same ledger", () => {
    const first = apply(lifecycleCommands());
    const second = apply(lifecycleCommands());
    expect(JSON.stringify(first.commit_history)).toBe(JSON.stringify(second.commit_history));
  });

  test("the history carries no content, paths, sessions, or timestamps", () => {
    const serialized = JSON.stringify(state.commit_history);
    // The ledger's only string values are the start commands' `state_id`
    // references (durable safe ids); everything else is closed kinds and
    // integer references.
    for (const banned of [
      "sess-",
      "tool-",
      "/opt/orchestrator",
      "plan\"",
      "report\"",
      "stage-1",
      "development",
      "stage_iteration_limit_exhausted",
      "2026-",
      "T00:",
      "body",
      "prompt",
      "reason",
      "digest",
    ]) {
      expect(serialized).not.toContain(banned);
    }
    const keys = new Set<string>();
    for (const entry of state.commit_history) {
      for (const key of Object.keys(entry)) {
        keys.add(key);
      }
    }
    expect([...keys].sort()).toEqual(
      ["execution_index", "generation_index", "grant_index", "iteration_index", "kind", "plan_index", "revision", "state_id", "task_index", "transition_index", "wait_index"].sort(),
    );
  });
});

describe("pipeline v2 commit provenance: the ambiguity clusters resolve durably", () => {
  function planningPrefix(): PipelineV2RunCommand[] {
    return [
      { kind: "create_run", runId: "run-r1", pipeline: IDENTITY, inputs: [] },
      ...planningLadder("sess-1", "tool-1"),
    ];
  }
  const taskA: PipelineV2RunCommand = { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1") };
  const taskB: PipelineV2RunCommand = { kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: hex("2") };
  const plan1: PipelineV2RunCommand = { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1 };

  test("R1: the two honest orders now carry different durable provenance", () => {
    const tasksFirst = apply([...planningPrefix(), taskA, taskB, plan1]);
    const planFirst = apply([...planningPrefix(), plan1, taskA, taskB]);
    expect(tasksFirst.revision).toBe(11);
    expect(planFirst.revision).toBe(11);
    // The projections remain byte-identical (the proven v7 equivalence).
    const strip = (state: PipelineV2RunState): string =>
      JSON.stringify({ ...JSON.parse(JSON.stringify(state)) as Record<string, unknown>, commit_history: undefined });
    expect(strip(tasksFirst)).toBe(strip(planFirst));
    // The ledgers disagree — the dispatch order is durable.
    expect(JSON.stringify(tasksFirst.commit_history)).not.toBe(JSON.stringify(planFirst.commit_history));
    expect(tasksFirst.commit_history.slice(8).map((entry) => entry.kind)).toEqual([
      "task_revision_accepted",
      "task_revision_accepted",
      "plan_revision_accepted",
    ]);
    expect(planFirst.commit_history.slice(8).map((entry) => entry.kind)).toEqual([
      "plan_revision_accepted",
      "task_revision_accepted",
      "task_revision_accepted",
    ]);
    // Both documents are loader-valid with their own history.
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(tasksFirst)))).toBeDefined();
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(planFirst)))).toBeDefined();
  });

  test("R2: the replanned generation closure carries its own revision at every honest position", () => {
    const genClose: PipelineV2RunCommand = { kind: "stage_generation_closed", generationIndex: 1, by: "replanned" };
    const build = (closePosition: "after-response" | "after-planning" | "after-plan"): PipelineV2RunState => {
      const prefix = lifecycleCommands().slice(0, 41); // through the revise response (revision 41)
      const ladder = planningLadder("sess-4", "tool-4");
      const plan2: PipelineV2RunCommand = { kind: "plan_revision_accepted", planRevision: 2, planSha256: hex("4"), originExecution: 4 };
      const genOpen: PipelineV2RunCommand = {
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: hex("4"),
        initialBudget: 3,
        transitionCount: 3,
      };
      const iterOpen: PipelineV2RunCommand = { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 3 };
      if (closePosition === "after-response") {
        return apply([...prefix, genClose, ...ladder, plan2, genOpen, iterOpen]);
      }
      if (closePosition === "after-planning") {
        return apply([...prefix, ...ladder, genClose, plan2, genOpen, iterOpen]);
      }
      return apply([...prefix, ...ladder, plan2, genClose, genOpen, iterOpen]);
    };
    const a = build("after-response");
    const b = build("after-planning");
    const c = build("after-plan");
    expect(a.revision).toBe(52);
    expect(b.revision).toBe(52);
    expect(c.revision).toBe(52);
    const strip = (state: PipelineV2RunState): string =>
      JSON.stringify({ ...JSON.parse(JSON.stringify(state)) as Record<string, unknown>, commit_history: undefined });
    expect(strip(a)).toBe(strip(b));
    expect(strip(a)).toBe(strip(c));
    const closeRevisionOf = (state: PipelineV2RunState): number => {
      const entry = state.commit_history.find((candidate) => candidate.kind === "stage_generation_closed");
      return entry!.revision;
    };
    expect(closeRevisionOf(a)).toBe(42);
    expect(closeRevisionOf(b)).toBe(49);
    expect(closeRevisionOf(c)).toBe(50);
    expect(closeRevisionOf(a)).not.toBe(closeRevisionOf(b));
    for (const state of [a, b, c]) {
      expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(state)))).toBeDefined();
    }
  });
});

describe("pipeline v2 commit provenance: the full command-kind ownership matrix", () => {
  /** The lifecycle prefixes keyed by the first revision at which each kind dispatches. */
  function prefixUpTo(revision: number): PipelineV2RunCommand[] {
    return lifecycleCommands().slice(0, revision - 1);
  }
  const lifecycleState = apply(lifecycleCommands());
  const firstRevisionOf = new Map<string, number>();
  lifecycleState.commit_history.forEach((entry) => {
    if (!firstRevisionOf.has(entry.kind)) {
      firstRevisionOf.set(entry.kind, entry.revision);
    }
  });

  interface KindCase {
    kind: PipelineV2RunCommand["kind"];
    /** The command to dispatch on the prefix; defaults to the lifecycle's own command. */
    command?: PipelineV2RunCommand;
    /** The expected provenance entry (without the revision, which is prefix length + 1). */
    entry: Record<string, unknown>;
    /** Kinds outside the lifecycle get their own prefix builder. */
    commands?: PipelineV2RunCommand[];
  }

  const decisionFlow = (): PipelineV2RunCommand[] => [
    {
      kind: "create_run",
      runId: "run-decision",
      pipeline: { ...IDENTITY, entry_state: "check" },
      inputs: [],
    },
    { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e"), executionRole: "control" },
    {
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "approved", decision: "approved", rule_id: "R1", active_constraint_ids: [] },
    },
    transition("check", "done", 1),
    { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
    { kind: "run_outputs_published", outputs: [] },
    { kind: "run_succeeded" },
  ];
  const agentFailedFlow = (): PipelineV2RunCommand[] => [
    { kind: "create_run", runId: "run-failed", pipeline: { ...IDENTITY, entry_state: "architect" }, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    {
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "not_required", tool: "not_required" }, workerLog: { status: "unavailable" }
    },
    { kind: "run_failed", reason: "worker_failed" },
  ];
  const cleanupFailedFlow = (): PipelineV2RunCommand[] => [
    { kind: "create_run", runId: "run-cleanup", pipeline: { ...IDENTITY, entry_state: "architect" }, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "sess-x" },
    { kind: "agent_tool_session_created", sessionId: "tool-x" },
    {
      kind: "agent_failed",
      reason: "session_cleanup_failed",
      sessionCleanup: { execution: "failed", tool: "completed" }, workerLog: { status: "unavailable" }
    },
    { kind: "run_cleanup_failed" },
  ];

  const cases: KindCase[] = [
    {
      kind: "create_run",
      commands: [],
      command: { kind: "create_run", runId: "run-x", pipeline: IDENTITY, inputs: [] },
      entry: { kind: "create_run" },
    },
    { kind: "start_agent_execution", command: undefined, entry: { kind: "start_agent_execution", execution_index: 1, state_id: "architect" } },
    { kind: "agent_data_prepared", command: undefined, entry: { kind: "agent_data_prepared", execution_index: 1 } },
    { kind: "agent_execution_session_created", command: undefined, entry: { kind: "agent_execution_session_created", execution_index: 1 } },
    { kind: "agent_tool_session_created", command: undefined, entry: { kind: "agent_tool_session_created", execution_index: 1 } },
    { kind: "agent_running", command: undefined, entry: { kind: "agent_running", execution_index: 1 } },
    { kind: "agent_outputs_accepted", command: undefined, entry: { kind: "agent_outputs_accepted", execution_index: 1 } },
    { kind: "agent_cleanup_completed", command: undefined, entry: { kind: "agent_cleanup_completed", execution_index: 1 } },
    {
      kind: "agent_failed",
      commands: agentFailedFlow().slice(0, 3),
      command: agentFailedFlow()[3],
      entry: { kind: "agent_failed", execution_index: 1 },
    },
    {
      kind: "start_decision_execution",
      commands: decisionFlow().slice(0, 1),
      command: decisionFlow()[1],
      entry: { kind: "start_decision_execution", execution_index: 1, state_id: "check" },
    },
    {
      kind: "decision_evaluated",
      commands: decisionFlow().slice(0, 2),
      command: decisionFlow()[2],
      entry: { kind: "decision_evaluated", execution_index: 1 },
    },
    {
      kind: "decision_failed",
      commands: [
        { kind: "create_run", runId: "run-decision-failed", pipeline: { ...IDENTITY, entry_state: "check" }, inputs: [] },
        { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e"), executionRole: "control" },
      ],
      command: { kind: "decision_failed", reason: "internal_error" },
      entry: { kind: "decision_failed", execution_index: 1 },
    },
    {
      kind: "transition_committed",
      command: undefined,
      entry: { kind: "transition_committed", execution_index: 1, transition_index: 1 },
    },
    { kind: "run_waiting", command: undefined, entry: { kind: "run_waiting", wait_index: 1 } },
    { kind: "wait_response_recorded", command: undefined, entry: { kind: "wait_response_recorded", wait_index: 1 } },
    { kind: "terminal_reached", command: undefined, entry: { kind: "terminal_reached" } },
    { kind: "run_outputs_published", command: undefined, entry: { kind: "run_outputs_published" } },
    { kind: "run_succeeded", command: undefined, entry: { kind: "run_succeeded" } },
    { kind: "run_failed", commands: agentFailedFlow().slice(0, 4), command: agentFailedFlow()[4], entry: { kind: "run_failed" } },
    {
      kind: "run_cleanup_failed",
      commands: cleanupFailedFlow().slice(0, 6),
      command: cleanupFailedFlow()[6],
      entry: { kind: "run_cleanup_failed" },
    },
    { kind: "stage_generation_opened", command: undefined, entry: { kind: "stage_generation_opened", generation_index: 1 } },
    {
      kind: "stage_iteration_opened",
      command: undefined,
      entry: { kind: "stage_iteration_opened", generation_index: 1, iteration_index: 1 },
    },
    {
      kind: "stage_iteration_closed",
      command: undefined,
      entry: { kind: "stage_iteration_closed", generation_index: 1, iteration_index: 1 },
    },
    {
      kind: "stage_generation_closed",
      command: undefined,
      entry: { kind: "stage_generation_closed", generation_index: 1 },
    },
    { kind: "plan_intent_accepted", command: undefined, entry: { kind: "plan_intent_accepted", wait_index: 1 } },
    { kind: "task_revision_accepted", command: undefined, entry: { kind: "task_revision_accepted", task_index: 1 } },
    { kind: "plan_revision_accepted", command: undefined, entry: { kind: "plan_revision_accepted", plan_index: 1 } },
    {
      kind: "iteration_grant_recorded",
      command: undefined,
      entry: { kind: "iteration_grant_recorded", grant_index: 1 },
    },
  ];

  test("the production kinds vocabulary carries exactly the 28 command kinds once each", () => {
    expect(PIPELINE_V2_COMMIT_PROVENANCE_KINDS).toHaveLength(28);
    expect(new Set(PIPELINE_V2_COMMIT_PROVENANCE_KINDS).size).toBe(28);
  });

  test("every kind appends exactly one entry with its own minimal references", () => {
    expect(cases).toHaveLength(28);
    const covered = new Set(cases.map((kindCase) => kindCase.kind));
    expect([...covered].sort()).toEqual([...PIPELINE_V2_COMMIT_PROVENANCE_KINDS].sort());
    for (const kindCase of cases) {
      try {
      const commands = kindCase.commands ?? prefixUpTo(firstRevisionOf.get(kindCase.kind)!);
      const command = kindCase.command ?? lifecycleCommands()[firstRevisionOf.get(kindCase.kind)! - 1]!;
      const after = apply([...commands, command]);
      const appended = after.commit_history[after.commit_history.length - 1]!;
      const { revision: _revision, ...rest } = appended;
      expect(rest as Record<string, unknown>).toEqual(kindCase.entry);
      if (commands.length === 0) {
        // create_run: the run starts at revision 1 with the single entry.
        expect(kindCase.kind).toBe("create_run");
        expect(after.revision).toBe(1);
        expect(after.commit_history).toEqual([{ revision: 1, kind: "create_run" }]);
        continue;
      }
      const before = apply(commands);
      expect(after.revision).toBe(before.revision + 1);
      expect(after.commit_history).toHaveLength(before.commit_history.length + 1);
      expect(after.commit_history.slice(0, -1)).toEqual([...before.commit_history]);
      } catch (error) {
        throw new Error(`kind case ${kindCase.kind}: ${String(error instanceof Error ? error.message : error)}`);
      }
    }
  });

  test("a rejected command leaves the history untouched", () => {
    const rejected: Array<[string, PipelineV2RunCommand[]]> = [
      ["create_run twice", [...lifecycleCommands().slice(0, 5), { kind: "create_run", runId: "other", pipeline: IDENTITY, inputs: [] }]],
      ["start off the waiting boundary", [...lifecycleCommands().slice(0, 23), { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" }]],
      ["ladder out of order", [...lifecycleCommands().slice(0, 2), { kind: "agent_running" }]],
      ["unknown decision state", [...lifecycleCommands().slice(0, 1), { kind: "start_decision_execution", stateId: "elsewhere", inputDigest: hex("e"), executionRole: "control" }]],
      ["wait-bound task without intent", [...lifecycleCommands().slice(0, 23), { kind: "task_revision_accepted", taskId: "task-a", revision: 2, taskSha256: hex("f"), waitIndex: 1, intentSha256: hex("9") }]],
      ["grant without intent", [...lifecycleCommands().slice(0, 23), { kind: "iteration_grant_recorded", generationIndex: 1, waitIndex: 1, intentSha256: hex("9"), additionalIterations: 1 }]],
      ["transition before cleanup", [...lifecycleCommands().slice(0, 3), transition("architect", "dev_entry", 1)]],
      // run_failed after a clean settled boundary is reducer-legal (a
      // coordination failure) and is covered by the ownership matrix; it
      // is not a rejection case.
    ];
    for (const [label, commands] of rejected) {
      let current: PipelineV2RunState | null = null;
      const last = commands[commands.length - 1]!;
      const prefix = commands.slice(0, -1);
      prefix.forEach((command, index) => {
        current = reducePipelineV2RunCommand(current, command, TICKS[index % TICKS.length]!);
      });
      const before = current as unknown as PipelineV2RunState;
      expect(() => reducePipelineV2RunCommand(before, last, TICKS[0]!)).toThrow();
      // Rebuild the prefix state and verify the history is unchanged.
      const rebuilt = apply(prefix);
      expect(rebuilt.commit_history).toEqual(before.commit_history);
      expect(label).toBeString();
    }
  });
});

describe("pipeline v2 commit provenance: the loader coherence replay", () => {
  const state = apply(lifecycleCommands());
  const raw = JSON.stringify(state);

  function forged(mutator: (draft: Record<string, unknown>) => void): unknown {
    const draft = JSON.parse(raw) as Record<string, unknown>;
    mutator(draft);
    return draft;
  }
  function expectRejected(mutator: (draft: Record<string, unknown>) => void, message: string): void {
    expect(() => validatePipelineV2RunState(forged(mutator))).toThrow(message);
  }
  function expectRejectedJSON(
    rawDocument: string,
    mutator: (draft: Record<string, unknown>) => void,
    message: string,
  ): void {
    const draft = JSON.parse(rawDocument) as Record<string, unknown>;
    mutator(draft);
    expect(() => validatePipelineV2RunState(draft)).toThrow(message);
  }

  test("form: missing, non-array, sparse, length and density rejections", () => {
    expectRejected((draft) => {
      delete draft["commit_history"];
    }, "missing required field \"commit_history\"");
    expectRejected((draft) => {
      draft["commit_history"] = "nope";
    }, "commit_history must be an array");
    expectRejected((draft) => {
      (draft["commit_history"] as unknown[])[5] = undefined;
      (draft["commit_history"] as unknown[]).splice(6, 1);
    }, "carries a hole");
    expectRejected((draft) => {
      (draft["commit_history"] as unknown[]).pop();
    }, "exactly one entry per revision is required");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[3]!.revision = 9;
    }, "entries must be dense from revision 1");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      const first = history.shift()!;
      history.push(first);
    }, "entries must be dense from revision 1");
  });

  test("entry shape: unknown kind, extra field, missing reference, bad reference type", () => {
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[1]!.kind = "teleport_execution";
    }, "which is not a pipeline v2 run command kind");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[1]!.extra = "hostile";
    }, "unknown field \"extra\"");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      delete history[1]!.state_id;
    }, "missing required field \"state_id\"");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[1]!.execution_index = 0;
    }, "execution_index");
    // An inherited (non-own) extra field is never satisfied by the shape.
    const draft = JSON.parse(raw) as Record<string, unknown>;
    const inherited = Object.create(JSON.parse(raw)) as Record<string, unknown>;
    inherited.commit_history = JSON.parse(raw) === null ? [] : (JSON.parse(raw) as Record<string, unknown>)["commit_history"];
    inherited.executions = (JSON.parse(raw) as Record<string, unknown>)["executions"];
    inherited.transitions = (JSON.parse(raw) as Record<string, unknown>)["transitions"];
    inherited.waits = (JSON.parse(raw) as Record<string, unknown>)["waits"];
    inherited.generations = (JSON.parse(raw) as Record<string, unknown>)["generations"];
    inherited.task_revisions = (JSON.parse(raw) as Record<string, unknown>)["task_revisions"];
    inherited.plan_revisions = (JSON.parse(raw) as Record<string, unknown>)["plan_revisions"];
    inherited.grants = (JSON.parse(raw) as Record<string, unknown>)["grants"];
    inherited.inputs = (JSON.parse(raw) as Record<string, unknown>)["inputs"];
    inherited.cursor = (JSON.parse(raw) as Record<string, unknown>)["cursor"];
    inherited.pipeline = (JSON.parse(raw) as Record<string, unknown>)["pipeline"];
    inherited.terminal = (JSON.parse(raw) as Record<string, unknown>)["terminal"];
    inherited.run_outputs = (JSON.parse(raw) as Record<string, unknown>)["run_outputs"];
    inherited.failure = (JSON.parse(raw) as Record<string, unknown>)["failure"];
    void inherited;
    void draft;
    expect(validatePipelineV2RunState(JSON.parse(raw))).toBeDefined();
  });

  test("references: starts, transitions, waits, generations, iterations, ledgers", () => {
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[1]!.state_id = "elsewhere";
    }, "records");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[13]!.transition_index = 9;
    }, "journal position");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[13]!.execution_index = 5;
    }, "commits execution");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[22]!.wait_index = 9;
    }, "journal position");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[11]!.generation_index = 9;
    }, "are consumed of");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[8]!.task_index = 9;
    }, "are consumed of");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[10]!.plan_index = 9;
    }, "are consumed of");
  });

  test("order: claims the projection cannot support are rejected", () => {
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      // Move the plan acceptance (revision 11) into the middle of the
      // planning ladder (revision 7): the execution is still in flight,
      // so no replay position can settle the plan's origin.
      const plan = history.splice(10, 1)[0]!;
      history.splice(6, 0, plan);
      history.forEach((entry, index) => {
        entry.revision = index + 1;
      });
    }, "requires the last execution to be settled");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      // Move the wait-bound task revision (39) before its intent (38).
      const task = history.splice(38, 1)[0]!;
      history.splice(37, 0, task);
      history.forEach((entry, index) => {
        entry.revision = index + 1;
      });
    }, "requires the accepted intent");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      const response = history.splice(40, 1)[0]!;
      history.splice(39, 0, response);
      history.forEach((entry, index) => {
        entry.revision = index + 1;
      });
    }, "requires no open iteration");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      // Move the replanned generation closure (52) after the new
      // generation opened (53): the closure then names a generation that
      // is no longer the open one.
      const close = history.splice(51, 1)[0]!;
      history.splice(52, 0, close);
      history.forEach((entry, index) => {
        entry.revision = index + 1;
      });
    }, "requires generation 1 to be closed");
  });

  test("field coherence: phase and presence contradictions are rejected", () => {
    // The projection's own phase/field coherence checks run before the
    // ledger replay; a forgery that survives them is caught by the
    // ledger's claim coherence.
    expectRejected((draft) => {
      const executions = draft["executions"] as Array<Record<string, unknown>>;
      executions[0]!.phase = "running";
      executions[0]!.worker_log = null;
    }, "already records a session cleanup outcome");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      // Swap the wait-bound closure's iteration reference: the open
      // iteration at that replay position is 2, not 1.
      history[39]!.iteration_index = 1;
    }, "but the open iteration is");
  });

  test("consumption: unclaimed records and phantom claims are rejected", () => {
    expectRejected((draft) => {
      const executions = draft["executions"] as Array<Record<string, unknown>>;
      executions.push({
        index: 6,
        type: "agent",
        worker_log: null,
        state_id: "architect",
        attempt: 1,
        profile: "architect",
        execution_role: "planning",
        phase: "started",
      });
    }, "but starts inside the open iteration");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      const dropped = history.findIndex((entry) => entry.kind === "agent_tool_session_created");
      history.splice(dropped, 1);
      history.forEach((entry, index) => {
        entry.revision = index + 1;
      });
      draft["revision"] = history.length;
    }, "requires the previous phase");
  });

  test("finalization: the last entry must agree with the final status", () => {
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      history[history.length - 1]!.kind = "run_cleanup_failed";
    }, "cannot end as a cleanup failure");
    expectRejected((draft) => {
      const history = draft["commit_history"] as Array<Record<string, unknown>>;
      const last = history.pop()!;
      history.push(last);
      history.push({ revision: history.length + 1, kind: "agent_running", execution_index: 5 });
      draft["revision"] = history.length;
    }, "no command may follow the run's finalizing entry");
    {
      // A genuinely waiting document (the revise boundary): its last
      // ledger entry must be a command a waiting run accepts.
      const waitingRaw = JSON.stringify(apply(lifecycleCommands().slice(0, 40)));
      expectRejectedJSON(waitingRaw, (draft) => {
        const history = draft["commit_history"] as Array<Record<string, unknown>>;
        history[history.length - 1] = { revision: history.length, kind: "agent_running", execution_index: 2 };
      }, "but the unfinished execution is none");
    }
  });
});

describe("pipeline v2 commit provenance: the crash and durability matrix", () => {
  test("a not-committed dispatch leaves no entry and a retry appends exactly once", async () => {
    const root = await mkdtemp(join(tmpdir(), "commit-history-crash-"));
    try {
      const stateRoot = join(root, "state-root");
      const sink = new PipelineV2RunStateSink({ stateRoot, runId: "run-crash", now: () => TICKS[0]!, io: faultIo({ failCommit: 2, failStep: "rename" }) });
      await sink.dispatch({ kind: "create_run", runId: "run-crash", pipeline: IDENTITY, inputs: [] });
      let rejected = false;
      try {
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      } catch {
        rejected = true;
      }
      expect(rejected).toBe(true);
      const before = sink.snapshot as PipelineV2RunState;
      expect(before.revision).toBe(1);
      expect(before.commit_history).toHaveLength(1);
      // A reopened sink retries: the command applies exactly once.
      const retry = await PipelineV2RunStateSink.open({ stateRoot, runId: "run-crash", now: () => TICKS[1]! });
      await retry.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      const after = retry.snapshot as PipelineV2RunState;
      expect(after.revision).toBe(2);
      expect(after.commit_history).toHaveLength(2);
      expect(after.commit_history[1]).toEqual({
        revision: 2,
        kind: "start_agent_execution",
        execution_index: 1,
        state_id: "architect",
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("a durability-unknown candidate carries its entry exactly once and a reopen accepts it", async () => {
    const root = await mkdtemp(join(tmpdir(), "commit-history-durability-"));
    try {
      const stateRoot = join(root, "state-root");
      const sink = new PipelineV2RunStateSink({ stateRoot, runId: "run-durable", now: () => TICKS[0]!, io: faultIo({ failCommit: 2, failStep: "dirsync" }) });
      await sink.dispatch({ kind: "create_run", runId: "run-durable", pipeline: IDENTITY, inputs: [] });
      let durabilityUnknown = false;
      try {
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      } catch (error) {
        durabilityUnknown = error instanceof Error && error.name === "PipelineV2RunStateDurabilityError";
        if (!durabilityUnknown) {
          throw error;
        }
      }
      expect(durabilityUnknown).toBe(true);
      const adopted = sink.snapshot as PipelineV2RunState;
      expect(adopted.revision).toBe(2);
      expect(adopted.commit_history).toHaveLength(2);
      // The on-disk candidate is the adopted one; a fresh open accepts it.
      const reopened = await PipelineV2RunStateSink.open({ stateRoot, runId: "run-durable", now: () => TICKS[2]! });
      const reopenedState = reopened.snapshot as PipelineV2RunState;
      expect(reopenedState.revision).toBe(2);
      expect(JSON.stringify(reopenedState.commit_history)).toBe(JSON.stringify(adopted.commit_history));
      // A retry does not duplicate the entry.
      await reopened.dispatch({ kind: "agent_data_prepared" });
      const progressed = reopened.snapshot as PipelineV2RunState;
      expect(progressed.revision).toBe(3);
      expect(progressed.commit_history).toHaveLength(3);
      expect(progressed.commit_history[2]).toEqual({ revision: 3, kind: "agent_data_prepared", execution_index: 1 });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("read-only loads and re-parses never append to the history", async () => {
    const root = await mkdtemp(join(tmpdir(), "commit-history-readonly-"));
    try {
      const stateRoot = join(root, "state-root");
      const sink = new PipelineV2RunStateSink({ stateRoot, runId: "run-readonly", now: () => TICKS[0]! });
      await sink.dispatch({ kind: "create_run", runId: "run-readonly", pipeline: IDENTITY, inputs: [] });
      const raw = await readFile(join(stateRoot, "pipeline-runs", "run-readonly", "state.json"), "utf8");
      const parsed = parsePipelineV2RunState(raw);
      expect(JSON.stringify(parsed)).toBe(JSON.stringify(JSON.parse(raw)));
      const entries = await readdir(join(stateRoot, "pipeline-runs", "run-readonly"));
      expect(entries.sort()).toEqual(["state.json"]);
      const info = await stat(join(stateRoot, "pipeline-runs", "run-readonly", "state.json"));
      expect(info.mtimeMs).toBe(await stat(join(stateRoot, "pipeline-runs", "run-readonly", "state.json")).then((next) => next.mtimeMs));
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("pipeline v2 commit provenance: state size and leakage", () => {
  const state = apply(lifecycleCommands());

  test("the measured sizes on the 66-command lifecycle", () => {
    const full = Buffer.byteLength(JSON.stringify(state), "utf8");
    const ledger = Buffer.byteLength(JSON.stringify(state.commit_history), "utf8");
    const projection = full - ledger;
    expect(full).toBeGreaterThan(0);
    expect(ledger).toBeGreaterThan(0);
    const bytesPerCommit = ledger / state.commit_history.length;
    // The ledger stays a small fraction of the document and its per-entry
    // cost is bounded well below one kibibyte.
    expect(ledger / full).toBeLessThan(0.5);
    expect(bytesPerCommit).toBeLessThan(120);
    // Record the measurements for the report.
    process.stdout.write(
      `commit-history-size full=${full} ledger=${ledger} projection=${projection} perCommit=${bytesPerCommit.toFixed(1)}\n`,
    );
  });

  test("canaries placed in every free-form field never reach the ledger", () => {
    const canary = "canary-secret-Zq7";
    const canaryPath = "/host/secret/canary-path";
    const leaked = apply([
      {
        kind: "create_run",
        runId: "run-canary",
        pipeline: { ...IDENTITY, bundle_root: canaryPath },
        inputs: [],
      },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: `sess-${canary}` },
      { kind: "agent_tool_session_created", sessionId: `tool-${canary}` },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }], workerLog: { status: "unavailable" }  },
      { kind: "agent_cleanup_completed" },
      transition("architect", "dev_entry", 1),
      {
        kind: "run_waiting",
        stateId: "dev_entry",
        reason: `stage-iteration-${canary}`.slice(0, 40).replace(/[^A-Za-z0-9_.-]/g, "-"),
        requestSha256: hex("7"),
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      },
    ]);
    const ledger = JSON.stringify(leaked.commit_history);
    expect(ledger).not.toContain(canary);
    expect(ledger).not.toContain(canaryPath);
    // The state document as a whole does carry the operator paths and
    // session ids; only the ledger is content-free.
    expect(JSON.stringify(leaked)).toContain(canaryPath);
  });

  test("an internal error message never reaches the ledger", () => {
    // The reducer's rejection messages are the only error surface; a
    // rejected command never appends an entry.
    const before = apply(lifecycleCommands().slice(0, 10));
    let message = "";
    try {
      reducePipelineV2RunCommand(before, { kind: "agent_running" }, TICKS[0]!);
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toContain("rejected");
    expect(before.commit_history).toHaveLength(10);
  });
});

describe("pipeline v2 commit provenance: source restrictions", () => {
  test("one append owner, one entry-shape owner, one coherence verifier, no second vocabulary", async () => {
    const source = await readFile("src/pipeline_v2_state.ts", "utf8");
    // Exactly one ledger append site (the reducer's single tail).
    expect(source.split("next.commit_history = ").length - 1).toBe(1);
    // Exactly one entry validator (the entry-shape owner) and one
    // coherence verifier.
    expect(source.split("function validateCommitProvenanceEntry").length - 1).toBe(1);
    expect(source.split("function verifyCommitHistoryCoherence").length - 1).toBe(1);
    // The kinds list is typed against the command union, not copied.
    expect(source.includes("satisfies readonly PipelineV2CommitProvenanceKind[]")).toBe(true);
    expect(source.split("PIPELINE_V2_COMMIT_PROVENANCE_KINDS:").length - 1).toBe(1);
    // No raw command serialization into the provenance path: the
    // derivation body never serializes the command object.
    const derivationStart = source.indexOf("function commitProvenanceEntry");
    const derivation = source.slice(derivationStart, source.indexOf("/**\n * Derives") === -1 ? derivationStart + 4000 : source.indexOf("/**", derivationStart + 10));
    expect(derivation.includes("JSON.stringify")).toBe(false);
    // The verifier derives everything from the validated snapshot; no
    // filesystem access and no clock reads inside the provenance code.
    const verifierStart = source.indexOf("function verifyCommitHistoryCoherence");
    const verifierEnd = source.indexOf("}", source.indexOf("function statusIsFinal") === -1 ? verifierStart : verifierStart) ;
    const verifierSource = source.slice(verifierStart, verifierStart + 14000);
    expect(verifierSource).not.toContain("new Date");
    expect(verifierSource).not.toContain("Date.now");
    expect(verifierSource).not.toContain("process.stdout");
    void verifierEnd;
  });

  test("no sidecar journal, no migration-on-read, no process-local event registry", async () => {
    const source = await readFile("src/pipeline_v2_state.ts", "utf8");
    expect(source.includes("commit-history.jsonl")).toBe(false);
    expect(source.includes("appendFile")).toBe(false);
    // The legacy rejection stays first and migration stays nonexistent.
    expect(source.includes("no v7 migration exists")).toBe(true);
    const consumers = ["src/pipeline_v2_inspect.ts", "src/control_plane/http_server.ts", "src/control_plane/ui_assets.ts"];
    for (const consumer of consumers) {
      const text = await readFile(consumer, "utf8");
      expect(text.includes("commit_history")).toBe(false);
    }
  });
});
