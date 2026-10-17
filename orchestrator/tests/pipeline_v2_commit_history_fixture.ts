/**
 * Shared reducer fixture for the schema v9 commit provenance suites: the
 * synthetic pipeline identity, the deterministic clock and the exact
 * 66-command lifecycle of the audit proof. Not a test file — the test
 * suites import these helpers so that no test file ever imports another
 * test file (transitive re-evaluation would duplicate test registration
 * under the file-level isolated parallel runner).
 */
import {
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";

export const hex = (char: string): string => char.repeat(64);

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

export function planningLadder(session: string, tool: string): PipelineV2RunCommand[] {
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

export function stageLadder(session: string, tool: string, iteration: number): PipelineV2RunCommand[] {
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

export function transition(from: string, to: string, executionIndex: number, transitionIndex = 0): PipelineV2RunCommand {
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
