import {
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";

/** Shared v2 run-state fixtures for the store and sink tests. */

export const hex = (char: string): string => char.repeat(64);

export const V2_IDENTITY: PipelineV2RunPipelineIdentity = {
  schema_version: 2,
  bundle_root: "/opt/orchestrator/pipelines/v2",
  execution_snapshot_sha256: hex("a"),
  entry_state: "implement",
  max_transitions: 6,
};

export const V2_INPUTS: PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: hex("b") },
  { id: "notes", type: "json", protected: false, digest: hex("c") },
];

const TICKS = Array.from({ length: 32 }, (_, t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)));

export function tick(index: number): Date {
  const value = TICKS[index % TICKS.length];
  if (value === undefined) {
    throw new Error(`tick ${index} out of range`);
  }
  return value;
}

/**
 * The full happy path: implement -> check (decision) -> ship -> done
 * (terminal success) -> publish -> succeed. 21 commands, revisions 1..21.
 */
export function successCommands(runId = "run-1"): PipelineV2RunCommand[] {
  return [
    { kind: "create_run", runId, pipeline: V2_IDENTITY, inputs: V2_INPUTS },
    { kind: "start_agent_execution", stateId: "implement", profile: "coder" },
    { kind: "agent_data_prepared" },
    { kind: "agent_session_created", sessionId: "sess-1" },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
    { kind: "agent_cleanup_completed" },
    {
      kind: "transition_committed",
      step: { from: "implement", outcome: "completed", to: "check", transition_index: 0 },
      executionIndex: 1,
    },
    { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") },
    {
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "approved",
        decision: "approved",
        rule_id: "R1",
        active_constraint_ids: [],
      },
    },
    {
      kind: "transition_committed",
      step: { from: "check", outcome: "approved", to: "ship", transition_index: 0 },
      executionIndex: 2,
    },
    { kind: "start_agent_execution", stateId: "ship", profile: "coder" },
    { kind: "agent_data_prepared" },
    { kind: "agent_session_created", sessionId: "sess-2" },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
    {
      kind: "transition_committed",
      step: { from: "ship", outcome: "completed", to: "done", transition_index: 0 },
      executionIndex: 3,
    },
    { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
    {
      kind: "run_outputs_published",
      outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }],
    },
    { kind: "run_succeeded" },
  ];
}

export function buildStates(runId = "run-1"): PipelineV2RunState[] {
  const states: PipelineV2RunState[] = [];
  let current: PipelineV2RunState | null = null;
  successCommands(runId).forEach((command, index) => {
    current = reducePipelineV2RunCommand(current, command, tick(index));
    states.push(current as PipelineV2RunState);
  });
  return states;
}
