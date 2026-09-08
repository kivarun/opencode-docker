import { describe, expect, test } from "bun:test";
import type { ResolvedAgentState, ResolvedPipeline, ResolvedState } from "../src/pipeline.ts";
import {
  canonicalJson,
  pipelineExecutionDigest,
  pipelineExecutionSnapshotJson,
} from "../src/pipeline_digest.ts";
import {
  parsePipelineRunState,
  reducePipelineRunCommand,
  SESSION_CLEANUP_FAILURE_REASON,
  validatePipelineRunState,
  type PipelineIdentityState,
  type PipelineRunCommand,
  type PipelineRunState,
  type ProtectedInputState,
} from "../src/pipeline_state.ts";

const IDENTITY: PipelineIdentityState = {
  schema_version: 1,
  bundle_root: "/opt/orchestrator/pipelines/default",
  execution_snapshot_sha256: "a".repeat(64),
  entry_state: "execute",
  max_transitions: 1,
};

const PROTECTED_INPUTS: ProtectedInputState[] = [
  { id: "task", path: "TASK.md", sha256: "b".repeat(64) },
];

const STEP = Object.freeze({
  from: "execute",
  outcome: "completed",
  to: "completed",
  transition_index: 0,
});

const TICKS = Array.from({ length: 16 }, (_, t) =>
  new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)),
);

function tick(index: number): Date {
  const value = TICKS[index];
  if (value === undefined) {
    throw new Error(`tick ${index} out of range`);
  }
  return value;
}

function createRun(
  overrides: Partial<Extract<PipelineRunCommand, { kind: "create_run" }>> = {},
): PipelineRunCommand {
  return {
    kind: "create_run",
    runId: "run-1",
    workspace: "/work",
    identity: IDENTITY,
    protectedInputs: PROTECTED_INPUTS,
    ...overrides,
  };
}

function happyPathCommands(): PipelineRunCommand[] {
  return [
    createRun(),
    { kind: "start_activation", stateId: "execute", profile: "default" },
    { kind: "activation_session_created", sessionId: "dhs_child" },
    { kind: "activation_agent_running" },
    { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] },
    { kind: "activation_cleanup_completed" },
    { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] },
    { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    { kind: "run_succeeded" },
  ];
}

/** Two activations of the same state (a revisit), one transition each. */
function revisitPathCommands(): PipelineRunCommand[] {
  return [
    createRun({ identity: { ...IDENTITY, max_transitions: 2 } }),
    { kind: "start_activation", stateId: "execute", profile: "default" },
    { kind: "activation_session_created", sessionId: "s1" },
    { kind: "activation_agent_running" },
    { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out/a.txt"] },
    { kind: "activation_cleanup_completed" },
    // the transition loops back to the same state
    { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: ["out/a.txt"] },
    { kind: "start_activation", stateId: "execute", profile: "default" },
    { kind: "activation_session_created", sessionId: "s2" },
    { kind: "activation_agent_running" },
    { kind: "activation_result_accepted", resultSha256: "d".repeat(64), artifacts: ["out/b.txt"] },
    { kind: "activation_cleanup_completed" },
    { kind: "transition_committed", step: STEP, activationIndex: 2, resultSha256: "d".repeat(64), artifacts: ["out/b.txt"] },
    { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    { kind: "run_succeeded" },
  ];
}

function runCommands(
  commands: PipelineRunCommand[],
  initial: PipelineRunState | null = null,
): PipelineRunState | null {
  let current = initial;
  commands.forEach((command, index) => {
    current = reducePipelineRunCommand(current, command, tick(index));
  });
  return current;
}

function expectCommandRejection(
  current: PipelineRunState | null,
  command: PipelineRunCommand,
  pattern: RegExp,
): void {
  expect(() => reducePipelineRunCommand(current, command, tick(15))).toThrow(pattern);
}

describe("pipeline run state reducer", () => {
  test("happy path to terminal success", () => {
    const state = runCommands(happyPathCommands());
    expect(state).not.toBeNull();
    if (state === null) {
      throw new Error("unreachable");
    }
    expect(state.schema_version).toBe(2);
    expect(state.revision).toBe(9);
    expect(state.status).toBe("success");
    expect(state.phase).toBe("finished");
    expect(state.run_id).toBe("run-1");
    expect(state.workspace).toBe("/work");
    expect(state.started_at).toBe("2026-01-01T00:00:01.000Z");
    expect(state.updated_at).toBe("2026-01-01T00:00:09.000Z");
    expect(state.pipeline).toEqual(IDENTITY);
    expect(state.protected_inputs).toEqual(PROTECTED_INPUTS);
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.activations).toEqual([
      {
        index: 1,
        state_id: "execute",
        attempt: 1,
        profile: "default",
        phase: "session_cleanup_completed",
        session_id: "dhs_child",
        session_cleanup: "completed",
        result_sha256: "c".repeat(64),
        artifacts: ["out/product.txt"],
      },
    ]);
    expect(state.transitions).toEqual([
      {
        index: 0,
        from: "execute",
        outcome: "completed",
        to: "completed",
        activation_index: 1,
        result_sha256: "c".repeat(64),
        artifacts: ["out/product.txt"],
      },
    ]);
    expect(state.terminal).toEqual({ state_id: "completed", result: "success" });
    expect(state.failure).toBeUndefined();
    expect(state.events.map((event) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
    ]);
    expect(state.events.map((event) => event.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(state.events[2]).toEqual({
      sequence: 3,
      kind: "session_created",
      state_id: "execute",
      activation_index: 1,
      session_id: "dhs_child",
      at: "2026-01-01T00:00:03.000Z",
    });
    expect(state.events[6]).toEqual({
      sequence: 7,
      kind: "transition_committed",
      from: "execute",
      outcome: "completed",
      to: "completed",
      transition_index: 0,
      activation_index: 1,
      at: "2026-01-01T00:00:07.000Z",
    });
    // the document round-trips through the exact-field validator
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(state)))).toEqual(state);
  });

  test("a revisit of the same state uses a new activation and a new session", () => {
    const state = runCommands(revisitPathCommands())!;
    expect(state.activations.map((activation) => activation.index)).toEqual([1, 2]);
    expect(state.activations.map((activation) => activation.session_id)).toEqual(["s1", "s2"]);
    expect(state.activations.map((activation) => activation.state_id)).toEqual(["execute", "execute"]);
    expect(state.transitions.map((transition) => transition.activation_index)).toEqual([1, 2]);
    expect(state.transitions.map((transition) => transition.to)).toEqual(["execute", "completed"]);
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 2 });
    expect(state.revision).toBe(15);
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(state)))).toEqual(state);
  });

  test("revision grows by exactly one per commit and event sequences stay contiguous", () => {
    const commands = happyPathCommands();
    let current: PipelineRunState | null = null;
    commands.forEach((command, index) => {
      const next = reducePipelineRunCommand(current, command, tick(index));
      expect(next.revision).toBe(index + 1);
      expect(next.events.length).toBe(index + 1);
      expect(next.events[next.events.length - 1]?.sequence).toBe(index + 1);
      current = next;
    });
  });

  test("worker failure: activation failed, no transition, run failed", () => {
    const state = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "worker_failed" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.phase).toBe("finished");
    expect(state?.failure).toEqual({ reason: "worker_failed" });
    expect(state?.transitions).toEqual([]);
    expect(state?.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(state?.activations[0]?.phase).toBe("failed");
    expect(state?.activations[0]?.failure_reason).toBe("worker_failed");
    expect(state?.activations[0]?.session_cleanup).toBe("completed");
    expect(state?.terminal).toBeUndefined();
    expect(state?.events.map((event) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "activation_failed",
      "run_failed",
    ]);
  });

  test("unknown outcome before any transition: failure recorded, graph never moved", () => {
    const state = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_failed", reason: "unknown_outcome", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "unknown_outcome" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.failure).toEqual({ reason: "unknown_outcome" });
    expect(state?.transitions).toEqual([]);
    expect(state?.cursor.current_state).toBe("execute");
  });

  test("user signal keeps a normalized signal reason and failed status", () => {
    const state = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_failed", reason: "signal_sigterm", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "signal_sigterm" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.failure).toEqual({ reason: "signal_sigterm" });
  });

  test("cleanup failure is recorded with its dedicated reason", () => {
    const state = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_failed", reason: SESSION_CLEANUP_FAILURE_REASON, sessionCleanup: "failed" },
      { kind: "run_cleanup_failed", reason: SESSION_CLEANUP_FAILURE_REASON },
    ]);
    expect(state?.status).toBe("cleanup_failed");
    expect(state?.failure).toEqual({ reason: SESSION_CLEANUP_FAILURE_REASON });
    expect(state?.activations[0]?.session_cleanup).toBe("failed");
    expect(state?.activations[0]?.failure_reason).toBe(SESSION_CLEANUP_FAILURE_REASON);
  });

  test("a failed terminal is a normal graph result; success can never follow it", () => {
    const state = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "failed" },
      { kind: "run_failed", reason: "execution_failed" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.terminal).toEqual({ state_id: "completed", result: "failed" });
    expect(state?.failure).toEqual({ reason: "execution_failed" });
    expectCommandRejection(state, { kind: "run_succeeded" }, /requires an active run/);
  });

  test("illegal ordering and phase requirements are rejected", () => {
    const created = runCommands([createRun()])!;
    // the activation must match the cursor
    expectCommandRejection(created, { kind: "start_activation", stateId: "elsewhere", profile: "default" }, /does not match the cursor/);
    // the session belongs to the creating_session activation
    const started = runCommands([createRun(), { kind: "start_activation", stateId: "execute", profile: "default" }])!;
    expectCommandRejection(started, { kind: "activation_agent_running" }, /starting the agent requires activation phase "session_created"/);
    expectCommandRejection(started, { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] }, /accepting a result requires activation phase "agent_running"/);
    expectCommandRejection(started, { kind: "activation_cleanup_completed" }, /recording the session cleanup requires activation phase "result_accepted"/);
    // the agent needs the session first
    const session = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
    ])!;
    expectCommandRejection(session, { kind: "activation_session_created", sessionId: "dhs_other" }, /requires activation phase "creating_session", got "session_created"/);
    // the result needs the agent running
    expectCommandRejection(session, { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] }, /requires activation phase "agent_running"/);
    // cleanup needs the accepted result
    const running = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
    ])!;
    expectCommandRejection(running, { kind: "activation_cleanup_completed" }, /requires activation phase "result_accepted"/);
    // cleanup needs the session
    const accepting = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    // terminal needs the cursor at the terminal state
    expectCommandRejection(
      running,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /does not match the cursor/,
    );
    // success without a terminal
    expectCommandRejection(running, { kind: "run_succeeded" }, /requires a reached terminal state/);
  });

  test("a new activation requires the cleaned and transitioned previous one", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
    ])!;
    // the transition of activation 1 must commit first
    expectCommandRejection(
      cleaned,
      { kind: "start_activation", stateId: "execute", profile: "default" },
      /requires the previous activation's transition to be committed/,
    );
    // ...but is allowed after the transition commits
    const transitioned = runCommands([
      ...cleaned ? [] : [],
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    const second = reducePipelineRunCommand(transitioned, { kind: "start_activation", stateId: "execute", profile: "default" }, tick(15));
    expect(second.activations.length).toBe(2);
    expect(second.activations[1]?.index).toBe(2);
  });

  test("a session belongs to exactly one activation", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "start_activation", stateId: "execute", profile: "default" },
    ])!;
    expectCommandRejection(cleaned, { kind: "activation_session_created", sessionId: "s1" }, /already belongs to activation/);
  });

  test("the transition budget rejects the next transition and the rejected command changes nothing", () => {
    const exhausted = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      // the single budgeted transition commits at the boundary
      { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    expect(exhausted.cursor).toEqual({ current_state: "execute", transition_count: 1 });
    const before = JSON.stringify(exhausted);
    // a second transition would exceed the budget of 1
    const overBudget: PipelineRunCommand = {
      kind: "transition_committed",
      step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 },
      activationIndex: 1,
      resultSha256: "c".repeat(64),
      artifacts: [],
    };
    expect(() => reducePipelineRunCommand(exhausted, overBudget, tick(15))).toThrow(
      /would exceed the pipeline transition budget 1/,
    );
    expect(JSON.stringify(exhausted)).toBe(before);
    // with a raised budget the same transition commits
    const raised = runCommands([
      createRun({ identity: { ...IDENTITY, max_transitions: 2 } }),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    const second = reducePipelineRunCommand(raised, overBudget, tick(15));
    expect(second.cursor).toEqual({ current_state: "execute", transition_count: 2 });
  });

  test("the terminal cannot be reached directly after a cleaned activation without a committed transition", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
    ])!;
    expectCommandRejection(
      cleaned,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /does not match the cursor "execute"/,
    );
  });

  test("an entry-terminal run with zero activations and transitions round-trips", () => {
    const entryTerminal = runCommands([
      createRun({ identity: { ...IDENTITY, entry_state: "completed" } }),
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      { kind: "run_succeeded" },
    ])!;
    expect(entryTerminal.activations).toEqual([]);
    expect(entryTerminal.transitions).toEqual([]);
    expect(entryTerminal.cursor).toEqual({ current_state: "completed", transition_count: 0 });
    expect(entryTerminal.terminal).toEqual({ state_id: "completed", result: "success" });
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(entryTerminal)))).toEqual(entryTerminal);
  });

  test("duplicate transitions and events are rejected", () => {
    const withTransition = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    // the same transition again: the cursor has moved, so it cannot re-apply
    expectCommandRejection(
      withTransition,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /cursor is at/,
    );
    // double terminal
    const terminal = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    ])!;
    expectCommandRejection(
      terminal,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /requires phase "validating" or "running"/,
    );
    // ...and after the run succeeded, the terminal is frozen entirely
    const succeeded = reducePipelineRunCommand(terminal, { kind: "run_succeeded" }, tick(15));
    expectCommandRejection(
      succeeded,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /requires an active run/,
    );
    // double failure
    const failed = runCommands([createRun(), { kind: "run_failed", reason: "worker_failed" }])!;
    expectCommandRejection(failed, { kind: "run_failed", reason: "worker_failed" }, /cannot overwrite status/);
  });

  test("immutable identity: no command can change identity, workspace, run id, or protected inputs", () => {
    const state = runCommands(happyPathCommands())!;
    expect(state.pipeline).toEqual(IDENTITY);
    expect(state.protected_inputs).toEqual(PROTECTED_INPUTS);
    expect(state.run_id).toBe("run-1");
    expect(state.workspace).toBe("/work");
    expect(state.schema_version).toBe(2);
  });

  test("mutations after the terminal commit are forbidden", () => {
    const terminal = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    ])!;
    expectCommandRejection(
      terminal,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires phase "running"/,
    );
    expectCommandRejection(terminal, { kind: "start_activation", stateId: "execute", profile: "default" }, /requires phase "validating" or "running"/);
    expectCommandRejection(terminal, { kind: "activation_session_created", sessionId: "dhs_x" }, /already finished with phase/);
    // only the final lifecycle events remain legal
    expect(reducePipelineRunCommand(terminal, { kind: "run_succeeded" }, tick(15)).status).toBe("success");
  });

  test("the terminal run status is immutable: success is final and cannot be rewritten", () => {
    const succeeded = runCommands(happyPathCommands())!;
    expectCommandRejection(
      succeeded,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires an active run/,
    );
    expectCommandRejection(
      succeeded,
      { kind: "run_failed", reason: "worker_failed" },
      /cannot overwrite status "success"/,
    );
    expectCommandRejection(
      succeeded,
      { kind: "run_failed", reason: "signal_sigterm" },
      /cannot overwrite status "success"/,
    );
    expectCommandRejection(succeeded, { kind: "run_succeeded" }, /requires an active run/);
    expectCommandRejection(
      succeeded,
      { kind: "run_cleanup_failed", reason: "session_cleanup_failed" },
      /cannot overwrite status "success"/,
    );
    expectCommandRejection(succeeded, { kind: "start_activation", stateId: "execute", profile: "default" }, /requires an active run/);
    // a failed run status is equally immutable
    const failed = runCommands([createRun(), { kind: "run_failed", reason: "worker_failed" }])!;
    expectCommandRejection(
      failed,
      { kind: "run_failed", reason: "signal_sigterm" },
      /cannot overwrite status "failed"/,
    );
    expectCommandRejection(failed, { kind: "run_succeeded" }, /requires an active run/);
  });

  test("cursor, transition append, and event appear in one snapshot; rejected commands change nothing", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "dhs_child" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      { kind: "activation_cleanup_completed" },
    ])!;
    // deep-freeze the input: the pure reducer must not mutate it
    const frozen = JSON.parse(JSON.stringify(cleaned));
    deepFreeze(frozen);
    const next = reducePipelineRunCommand(
      frozen,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      tick(15),
    );
    expect(next.transitions.length).toBe(1);
    expect(next.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(next.events[next.events.length - 1]?.kind).toBe("transition_committed");
    expect(next.revision).toBe(cleaned.revision + 1);
    // the input snapshot is untouched
    expect(cleaned.transitions.length).toBe(0);
    expect(cleaned.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(cleaned.revision).toBe(6);
    expect(cleaned.events.length).toBe(6);
  });

  test("the reducer validates command payloads fail-closed", () => {
    expectCommandRejection(null, { kind: "start_activation", stateId: "x", profile: "p" }, /no pipeline run state exists/);
    expectCommandRejection(
      null,
      {
        kind: "create_run",
        runId: "run-1",
        workspace: "/work",
        identity: { ...IDENTITY, execution_snapshot_sha256: "nothex" },
        protectedInputs: PROTECTED_INPUTS,
      },
      /lowercase hex execution snapshot digest/,
    );
    expectCommandRejection(
      null,
      {
        kind: "create_run",
        runId: "run-1",
        workspace: "/work",
        identity: IDENTITY,
        protectedInputs: [{ id: "task", path: "../escape", sha256: "b".repeat(64) }],
      },
      /clean workspace-relative protected input paths/,
    );
    expectCommandRejection(
      null,
      {
        kind: "create_run",
        runId: "run-1",
        workspace: "/work",
        identity: IDENTITY,
        protectedInputs: [
          { id: "task", path: "A.md", sha256: "b".repeat(64) },
          { id: "task", path: "B.md", sha256: "b".repeat(64) },
        ],
      },
      /more than once/,
    );
    const created = runCommands([createRun()])!;
    expectCommandRejection(
      created,
      { kind: "create_run", runId: "run-2", workspace: "/w", identity: IDENTITY, protectedInputs: PROTECTED_INPUTS },
      /already exists/,
    );
    expectCommandRejection(
      created,
      { kind: "start_activation", stateId: "execute", profile: "" },
      /non-empty profile name/,
    );
    const started = runCommands([createRun(), { kind: "start_activation", stateId: "execute", profile: "default" }])!;
    expectCommandRejection(started, { kind: "activation_session_created", sessionId: "" }, /non-empty session id/);
    const accepting = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
    ])!;
    expectCommandRejection(
      accepting,
      { kind: "activation_result_accepted", resultSha256: "nothex", artifacts: [] },
      /lowercase hex result digest/,
    );
    expectCommandRejection(
      accepting,
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["../escape"] },
      /clean workspace-relative paths/,
    );
    expectCommandRejection(accepting, { kind: "activation_failed", reason: "not_a_reason" as never, sessionCleanup: "completed" }, /normalized failure reason/);
    expectCommandRejection(accepting, { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "weird" as never }, /sessionCleanup/);
    // the cleanup-failure reason requires a failed cleanup outcome
    expectCommandRejection(accepting, { kind: "activation_failed", reason: SESSION_CLEANUP_FAILURE_REASON, sessionCleanup: "completed" }, /requires sessionCleanup "failed"/);
  });

  test("transitions must reference the accepted and cleaned activation", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      { kind: "activation_cleanup_completed" },
    ])!;
    expectCommandRejection(
      cleaned,
      { kind: "transition_committed", step: STEP, activationIndex: 2, resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      /the cleaned activation is 1/,
    );
    expectCommandRejection(
      cleaned,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "d".repeat(64), artifacts: ["out.txt"] },
      /but activation 1 accepted/,
    );
    expectCommandRejection(
      cleaned,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: ["other.txt"] },
      /do not match the artifacts accepted/,
    );
    // a transition cannot commit before the activation is cleaned up
    const running = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
    ])!;
    expectCommandRejection(
      running,
      { kind: "transition_committed", step: STEP, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires the last activation to be cleaned up/,
    );
  });
});

function deepFreeze(value: unknown): void {
  if (value !== null && typeof value === "object") {
    for (const key of Object.keys(value as Record<string, unknown>)) {
      deepFreeze((value as Record<string, unknown>)[key]);
    }
    Object.freeze(value);
  }
}

describe("exact-field loader validation", () => {
  const valid = (): PipelineRunState => runCommands(happyPathCommands())!;

  test("a reducer-produced snapshot round-trips", () => {
    const state = valid();
    const parsed = parsePipelineRunState(JSON.stringify(state));
    expect(parsed).toEqual(state);
  });

  test("a v1 document is rejected as an unsupported version", () => {
    const v1 = {
      schema_version: 1,
      revision: 7,
      run_id: "run-1",
      status: "success",
      phase: "finished",
      started_at: "2026-01-01T00:00:01.000Z",
      updated_at: "2026-01-01T00:00:07.000Z",
      workspace: "/work",
      pipeline: IDENTITY,
      protected_input: { id: "task", path: "TASK.md", sha256: "b".repeat(64) },
      session_id: "dhs_child",
      cursor: { current_state: "completed", transition_count: 1 },
      attempt: {
        state_id: "execute",
        attempt: 1,
        profile: "default",
        session_id: "dhs_child",
        phase: "completed",
      },
      transitions: [
        {
          index: 0,
          from: "execute",
          outcome: "completed",
          to: "completed",
          attempt: 1,
          result_sha256: "c".repeat(64),
          artifacts: ["out/product.txt"],
        },
      ],
      terminal: { state_id: "completed", result: "success" },
      events: [],
    };
    expect(() => validatePipelineRunState(v1)).toThrow(/schema_version 1, which is unsupported/);
    expect(() => parsePipelineRunState(JSON.stringify(v1))).toThrow(/schema_version 1, which is unsupported/);
  });

  test("malformed and truncated documents are rejected", () => {
    expect(() => parsePipelineRunState("{ not json")).toThrow(/not valid JSON/);
    expect(() => parsePipelineRunState('{"schema_version":2')).toThrow(/not valid JSON/);
    expect(() => parsePipelineRunState("[]")).toThrow(/is not a JSON object/);
    expect(() => parsePipelineRunState("null")).toThrow(/is not a JSON object/);
  });

  test("unknown fields are rejected", () => {
    const state = JSON.parse(JSON.stringify(valid()));
    state.worker_image = "alpine:3.22";
    expect(() => validatePipelineRunState(state)).toThrow(/unknown field "worker_image"/);
    const nested = JSON.parse(JSON.stringify(valid()));
    nested.pipeline.extra = 1;
    expect(() => validatePipelineRunState(nested)).toThrow(/unknown field "extra"/);
    const eventExtra = JSON.parse(JSON.stringify(valid()));
    eventExtra.events[2].note = "hi";
    expect(() => validatePipelineRunState(eventExtra)).toThrow(/unknown field "note"/);
    const activationExtra = JSON.parse(JSON.stringify(valid()));
    activationExtra.activations[0].note = "hi";
    expect(() => validatePipelineRunState(activationExtra)).toThrow(/unknown field "note"/);
  });

  test("missing fields are rejected", () => {
    const state = JSON.parse(JSON.stringify(valid()));
    delete state.cursor;
    expect(() => validatePipelineRunState(state)).toThrow(/missing required field "cursor"/);
    const noActivations = JSON.parse(JSON.stringify(valid()));
    delete noActivations.activations;
    expect(() => validatePipelineRunState(noActivations)).toThrow(/missing required field "activations"/);
    const noProtectedInputs = JSON.parse(JSON.stringify(valid()));
    delete noProtectedInputs.protected_inputs;
    expect(() => validatePipelineRunState(noProtectedInputs)).toThrow(/missing required field "protected_inputs"/);
  });

  test("wrong types and enums are rejected", () => {
    const state = JSON.parse(JSON.stringify(valid()));
    state.revision = "9";
    expect(() => validatePipelineRunState(state)).toThrow(/revision must be a positive safe integer/);
    const status = JSON.parse(JSON.stringify(valid()));
    status.status = "ok";
    expect(() => validatePipelineRunState(status)).toThrow(/status must be one of/);
    const phase = JSON.parse(JSON.stringify(valid()));
    phase.phase = "weird";
    expect(() => validatePipelineRunState(phase)).toThrow(/phase must be one of/);
    const sha = JSON.parse(JSON.stringify(valid()));
    sha.pipeline.execution_snapshot_sha256 = "XYZ";
    expect(() => validatePipelineRunState(sha)).toThrow(/lowercase hex SHA-256/);
    const timestamp = JSON.parse(JSON.stringify(valid()));
    timestamp.started_at = "yesterday";
    expect(() => validatePipelineRunState(timestamp)).toThrow(/ISO-8601/);
    const badSchema = JSON.parse(JSON.stringify(valid()));
    badSchema.schema_version = 3;
    expect(() => validatePipelineRunState(badSchema)).toThrow(/schema_version 3, expected 2/);
    const artifact = JSON.parse(JSON.stringify(valid()));
    artifact.transitions[0].artifacts = ["/etc/passwd"];
    expect(() => validatePipelineRunState(artifact)).toThrow(/clean workspace-relative/);
    const attempt = JSON.parse(JSON.stringify(valid()));
    attempt.activations[0].attempt = 2;
    expect(() => validatePipelineRunState(attempt)).toThrow(/only attempt 1 is supported/);
  });

  test("inconsistent revision, sequence, cursor, and trace are rejected", () => {
    const revisionGap = JSON.parse(JSON.stringify(valid()));
    revisionGap.revision = 12;
    expect(() => validatePipelineRunState(revisionGap)).toThrow(/revision 12 must equal the number of events 9/);
    const sequenceGap = JSON.parse(JSON.stringify(valid()));
    sequenceGap.events[3].sequence = 9;
    expect(() => validatePipelineRunState(sequenceGap)).toThrow(/sequence must be 4/);
    const cursorGap = JSON.parse(JSON.stringify(valid()));
    cursorGap.cursor.transition_count = 0;
    expect(() => validatePipelineRunState(cursorGap)).toThrow(/does not match 1 committed transitions/);
    const cursorState = JSON.parse(JSON.stringify(valid()));
    cursorState.cursor.current_state = "execute";
    expect(() => validatePipelineRunState(cursorState)).toThrow(/does not match the expected cursor "completed"/);
    const brokenChain = JSON.parse(JSON.stringify(valid()));
    brokenChain.transitions[0].from = "nowhere";
    expect(() => validatePipelineRunState(brokenChain)).toThrow(/starts at "nowhere", expected the entry state/);
    const gapIndex = JSON.parse(JSON.stringify(valid()));
    gapIndex.activations[0].index = 5;
    expect(() => validatePipelineRunState(gapIndex)).toThrow(/activation indexes must be contiguous from 1/);
  });

  test("activation, session, and transition references are enforced", () => {
    const sessionReuse = runCommands(revisitPathCommands())!;
    const mutated = JSON.parse(JSON.stringify(sessionReuse));
    mutated.activations[1].session_id = "s1";
    expect(() => validatePipelineRunState(mutated)).toThrow(/belongs to exactly one activation/);
    const transitionActivation = JSON.parse(JSON.stringify(valid()));
    transitionActivation.transitions[0].activation_index = 2;
    expect(() => validatePipelineRunState(transitionActivation)).toThrow(/references activation 2 which does not exist/);
    // a failed run keeps the transition-reference rule visible on load
    const failedRun = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      { kind: "activation_cleanup_completed" },
      { kind: "transition_committed", step: { from: "execute", outcome: "completed", to: "execute", transition_index: 0 }, activationIndex: 1, resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s2" },
      { kind: "activation_agent_running" },
      { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    // the failed second activation failed the run before its transition
    expect(failedRun.transitions.length).toBe(1);
    const uncleanedReference = JSON.parse(JSON.stringify(failedRun));
    uncleanedReference.transitions[0].activation_index = 2;
    expect(() => validatePipelineRunState(uncleanedReference)).toThrow(/transitions must reference activations in order/);
    // an artificial second transition first trips the transition budget
    const uncleanedSecond = JSON.parse(JSON.stringify(failedRun));
    uncleanedSecond.transitions.push({ ...uncleanedSecond.transitions[0], activation_index: 2 });
    uncleanedSecond.cursor.transition_count = 2;
    expect(() => validatePipelineRunState(uncleanedSecond)).toThrow(
      /records 2 committed transitions, more than the pipeline transition budget 1/,
    );
    // with a raised budget the uncleaned reference is rejected on its own
    const uncleanedSecondWithBudget = JSON.parse(JSON.stringify(failedRun));
    uncleanedSecondWithBudget.pipeline.max_transitions = 2;
    uncleanedSecondWithBudget.transitions.push({ ...uncleanedSecondWithBudget.transitions[0], activation_index: 2 });
    uncleanedSecondWithBudget.cursor.transition_count = 2;
    expect(() => validatePipelineRunState(uncleanedSecondWithBudget)).toThrow(
      /references activation 2 whose phase "failed" is not a cleaned activation/,
    );
    const stateIdMismatch = JSON.parse(JSON.stringify(valid()));
    stateIdMismatch.activations[0].state_id = "other";
    expect(() => validatePipelineRunState(stateIdMismatch)).toThrow(/its activation ran state "other"/);
    const digestMismatch = JSON.parse(JSON.stringify(valid()));
    digestMismatch.activations[0].result_sha256 = "d".repeat(64);
    expect(() => validatePipelineRunState(digestMismatch)).toThrow(/but its activation accepted/);
    const missingCleanupOutcome = JSON.parse(JSON.stringify(valid()));
    delete missingCleanupOutcome.activations[0].session_cleanup;
    expect(() => validatePipelineRunState(missingCleanupOutcome)).toThrow(/does not record its session cleanup outcome/);
    const twoActive = (() => {
      const state = runCommands(revisitPathCommands())!;
      const copy = JSON.parse(JSON.stringify(state));
      copy.activations[0].phase = "agent_running";
      return copy;
    })();
    expect(() => validatePipelineRunState(twoActive)).toThrow(/only the last activation may still be active/);
    const failureAndResult = JSON.parse(JSON.stringify(valid()));
    failureAndResult.activations[0].failure_reason = "worker_failed";
    expect(() => validatePipelineRunState(failureAndResult)).toThrow(/records a failure reason but has phase/);
  });

  test("illegal event orders are rejected", () => {
    const noCreateFirst = JSON.parse(JSON.stringify(valid()));
    noCreateFirst.events.reverse();
    for (let i = 0; i < noCreateFirst.events.length; i++) {
      noCreateFirst.events[i].sequence = i + 1;
    }
    expect(() => validatePipelineRunState(noCreateFirst)).toThrow(/first pipeline run event must be run_created/);
    const skip = JSON.parse(JSON.stringify(valid()));
    // drop the session_created event: the activation cannot reach
    // agent_running without it
    skip.events.splice(2, 1);
    skip.events.forEach((event: { sequence: number }, index: number) => {
      event.sequence = index + 1;
    });
    skip.revision = skip.events.length;
    expect(() => validatePipelineRunState(skip)).toThrow(/may not follow/);
    const doubleEnd = JSON.parse(JSON.stringify(valid()));
    doubleEnd.events.push({ sequence: 10, kind: "run_succeeded", at: doubleEnd.updated_at });
    doubleEnd.revision = 10;
    expect(() => validatePipelineRunState(doubleEnd)).toThrow(/may not follow/);
    // a success -> failure rewrite stays illegal on load
    const rewriteJournal = JSON.parse(JSON.stringify(valid()));
    rewriteJournal.events.push({ sequence: 10, kind: "run_failed", at: rewriteJournal.updated_at });
    rewriteJournal.revision = 10;
    rewriteJournal.status = "failed";
    rewriteJournal.failure = { reason: "signal_sigterm" };
    expect(() => validatePipelineRunState(rewriteJournal)).toThrow(/may not follow/);
  });

  test("status, phase, terminal, activation, and failure coherence is enforced", () => {
    const successWithoutTerminal = JSON.parse(JSON.stringify(valid()));
    delete successWithoutTerminal.terminal;
    expect(() => validatePipelineRunState(successWithoutTerminal)).toThrow(/requires a reached terminal/);
    const activeAfterTerminalEvent = JSON.parse(JSON.stringify(valid()));
    activeAfterTerminalEvent.status = "active";
    activeAfterTerminalEvent.failure = undefined;
    activeAfterTerminalEvent.phase = "finalizing";
    activeAfterTerminalEvent.events[8].kind = "run_failed";
    expect(() => validatePipelineRunState(activeAfterTerminalEvent)).toThrow(
      /an active run must not end with the terminal event run_failed/,
    );
    const failedWithoutReason = JSON.parse(JSON.stringify(valid()));
    failedWithoutReason.status = "failed";
    failedWithoutReason.failure = undefined;
    failedWithoutReason.events[8].kind = "run_failed";
    expect(() => validatePipelineRunState(failedWithoutReason)).toThrow(/requires a normalized failure reason/);
    const cleanupWithWrongReason = JSON.parse(JSON.stringify(valid()));
    cleanupWithWrongReason.status = "cleanup_failed";
    cleanupWithWrongReason.failure = { reason: "worker_failed" };
    cleanupWithWrongReason.events[8].kind = "run_cleanup_failed";
    expect(() => validatePipelineRunState(cleanupWithWrongReason)).toThrow(/requires the failure reason "session_cleanup_failed"/);
    const terminalCursorMismatch = JSON.parse(JSON.stringify(valid()));
    terminalCursorMismatch.terminal.state_id = "elsewhere";
    expect(() => validatePipelineRunState(terminalCursorMismatch)).toThrow(/does not match the cursor/);
    const successUncleaned = JSON.parse(JSON.stringify(valid()));
    successUncleaned.activations[0].phase = "failed";
    successUncleaned.activations[0].failure_reason = "worker_failed";
    successUncleaned.activations[0].session_cleanup = "completed";
    delete successUncleaned.activations[0].result_sha256;
    expect(() => validatePipelineRunState(successUncleaned)).toThrow(/requires every activation to be cleaned up/);
    const finishedActive = JSON.parse(JSON.stringify(valid()));
    finishedActive.status = "active";
    delete finishedActive.failure;
    finishedActive.events.pop();
    finishedActive.revision = 8;
    finishedActive.phase = "finished";
    expect(() => validatePipelineRunState(finishedActive)).toThrow(/must not be in the finished phase/);
    // an entry-terminal run never has activations and goes terminal_reached
    // straight from run_created
    const entryTerminal = runCommands([
      createRun(),
      { kind: "terminal_reached", terminalStateId: "execute", terminalResult: "success" },
      { kind: "run_succeeded" },
    ])!;
    expect(entryTerminal.status).toBe("success");
    expect(entryTerminal.activations).toEqual([]);
    expect(entryTerminal.phase).toBe("finished");
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(entryTerminal)))).toEqual(entryTerminal);
    // ...and a failed entry terminal fails the run with exit-relevant status
    const failedEntry = runCommands([
      createRun(),
      { kind: "terminal_reached", terminalStateId: "execute", terminalResult: "failed" },
      { kind: "run_failed", reason: "execution_failed" },
    ])!;
    expect(failedEntry.status).toBe("failed");
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(failedEntry)))).toEqual(failedEntry);
  });

  test("an artificial document with more transitions than the budget is rejected", () => {
    const overBudget = JSON.parse(JSON.stringify(valid()));
    overBudget.pipeline.max_transitions = 1;
    overBudget.transitions.push({ ...overBudget.transitions[0], from: "completed", to: "completed", activation_index: 2 });
    overBudget.cursor.transition_count = 2;
    expect(() => validatePipelineRunState(overBudget)).toThrow(
      /records 2 committed transitions, more than the pipeline transition budget 1/,
    );
    // the budget boundary itself is proven by the reducer tests: a run with
    // max_transitions 2 commits two transitions (see the revisit path)
  });

  test("event payloads must name exactly the activation, session, transition, or terminal they belong to", () => {
    const mutate = (mutator: (document: Record<string, any>) => void, pattern: RegExp) => {
      const broken = JSON.parse(JSON.stringify(valid()));
      mutator(broken);
      expect(() => validatePipelineRunState(broken)).toThrow(pattern);
    };

    mutate(
      (document) => {
        document.events[1].state_id = "elsewhere";
      },
      /activation_started.*does not match the next activation record/s,
    );
    mutate(
      (document) => {
        document.events[1].activation_index = 2;
      },
      /activation_started.*does not match the next activation record/s,
    );
    mutate(
      (document) => {
        document.events[2].session_id = "s-elsewhere";
      },
      /session_created.*names session "s-elsewhere", which is not the session recorded by activation 1/s,
    );
    mutate(
      (document) => {
        document.events[2].state_id = "elsewhere";
      },
      /session_created.*is not the activation in progress/s,
    );
    mutate(
      (document) => {
        document.events[3].activation_index = 2;
      },
      /agent_running.*is not the activation in progress/s,
    );
    mutate(
      (document) => {
        document.events[4].state_id = "elsewhere";
      },
      /result_accepted.*is not the activation in progress/s,
    );
    mutate(
      (document) => {
        document.events[5].activation_index = 2;
      },
      /session_cleanup_completed.*is not the activation in progress/s,
    );
    mutate(
      (document) => {
        document.events[6].transition_index = 5;
      },
      /transition_committed.*does not match the next committed transition record/s,
    );
    mutate(
      (document) => {
        document.events[6].to = "elsewhere";
      },
      /transition_committed.*does not match the next committed transition record/s,
    );
    mutate(
      (document) => {
        document.events[6].activation_index = 2;
      },
      /transition_committed.*does not match the next committed transition record/s,
    );
    mutate(
      (document) => {
        document.events[7].state_id = "elsewhere";
      },
      /terminal_reached.*names terminal state "elsewhere", which is not the reached terminal/s,
    );
    // the terminal record itself mutated to match the event is caught by the
    // cursor check
    mutate(
      (document) => {
        document.terminal.state_id = "elsewhere";
      },
      /does not match the cursor/,
    );
  });

  test("session_cleanup_completed followed by terminal_reached without a transition is rejected", () => {
    const cleaned = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_cleanup_completed" },
    ])!;
    expect(() =>
      reducePipelineRunCommand(
        cleaned,
        { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
        tick(15),
      ),
    ).toThrow(/does not match the cursor "execute"/);
    // an equivalent artificial document (cursor and terminal forged) is
    // rejected because the cleaned activation's transition is missing
    const forged = JSON.parse(JSON.stringify(cleaned));
    forged.revision = 7;
    forged.phase = "finalizing";
    forged.events = [
      ...forged.events.slice(0, 6),
      { sequence: 7, kind: "terminal_reached", state_id: "completed", at: "2026-01-01T00:00:07.000Z" },
    ];
    // the forged document is cursor-consistent (no transitions, the entry
    // state claims to be the terminal), so the missing transition is what is
    // actually caught
    forged.pipeline.entry_state = "completed";
    forged.cursor = { current_state: "completed", transition_count: 0 };
    forged.terminal = { state_id: "completed", result: "success" };
    expect(() => validatePipelineRunState(forged)).toThrow(
      /the terminal was reached but activation 1's transition was never committed/,
    );
  });

  test("a phantom failed activation without activation_started is rejected", () => {
    const failedRun = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    expect(failedRun.activations).toHaveLength(1);
    const phantom = JSON.parse(JSON.stringify(failedRun));
    phantom.activations.push({
      index: 2,
      state_id: "execute",
      attempt: 1,
      profile: "default",
      phase: "failed",
      failure_reason: "worker_failed",
      session_cleanup: "completed",
    });
    expect(() => validatePipelineRunState(phantom)).toThrow(
      /activation record 2 \("execute"\) is not represented by any activation_started event/,
    );
  });

  test("an added transition record with an adjusted cursor but no transition event is rejected", () => {
    const cleaned = runCommands(happyPathCommands())!;
    expect(cleaned.transitions).toHaveLength(1);
    const phantom = JSON.parse(JSON.stringify(cleaned));
    // a second cleaned activation and a second transition, cursor adjusted,
    // but the event journal is untouched: both records stay unconsumed
    phantom.pipeline.max_transitions = 2;
    phantom.activations.push({
      index: 2,
      state_id: "completed",
      attempt: 1,
      profile: "default",
      phase: "session_cleanup_completed",
      session_id: "s2",
      session_cleanup: "completed",
      result_sha256: cleaned.activations[0]!.result_sha256,
      artifacts: [],
    });
    phantom.transitions.push({
      index: 0,
      from: "completed",
      outcome: "completed",
      to: "completed",
      activation_index: 2,
      result_sha256: cleaned.activations[0]!.result_sha256,
      artifacts: [],
    });
    phantom.cursor = { current_state: "completed", transition_count: 2 };
    expect(() => validatePipelineRunState(phantom)).toThrow(
      /transition record 2 \(completed -> completed\) is not represented by any transition_committed event/,
    );
    // a phantom cleaned activation without any transition or terminal is
    // caught as well
    const phantomActivation = JSON.parse(JSON.stringify(cleaned));
    phantomActivation.activations.push({
      index: 2,
      state_id: "execute",
      attempt: 1,
      profile: "default",
      phase: "session_cleanup_completed",
      session_id: "s2",
      session_cleanup: "completed",
      result_sha256: cleaned.activations[0]!.result_sha256,
      artifacts: [],
    });
    // strip the terminal: the forged activation hides in an active run
    phantomActivation.terminal = undefined;
    phantomActivation.events = phantomActivation.events.filter(
      (event: any) => event.kind !== "terminal_reached" && event.kind !== "run_succeeded",
    );
    phantomActivation.revision = phantomActivation.events.length;
    phantomActivation.events.forEach((event: any, index: number) => {
      event.sequence = index + 1;
    });
    phantomActivation.phase = "running";
    phantomActivation.status = "active";
    expect(() => validatePipelineRunState(phantomActivation)).toThrow(
      /activation record 2 \("execute"\) is not represented by any activation_started event/,
    );
  });

  test("an injected terminal record on a run that failed before the terminal is rejected", () => {
    const failedEarly = runCommands([
      createRun({ identity: { ...IDENTITY, entry_state: "execute" } }),
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    expect(failedEarly.terminal).toBeUndefined();
    expect(failedEarly.activations).toHaveLength(0);
    const injected = JSON.parse(JSON.stringify(failedEarly));
    injected.terminal = { state_id: "execute", result: "failed" };
    expect(() => validatePipelineRunState(injected)).toThrow(
      /terminal record "execute" is not represented by exactly one terminal_reached event/,
    );
    // the same document without the injected terminal record stays valid
    expect(validatePipelineRunState(JSON.parse(JSON.stringify(failedEarly)))).toEqual(failedEarly);
  });

  test("an activation with a recorded session but no session_created event is rejected", () => {
    // a session-less failed activation (failed before the session existed);
    // the forged document claims a session the journal never recorded
    const failedRun = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    const injected = JSON.parse(JSON.stringify(failedRun));
    injected.activations[0].session_id = "s-forged";
    expect(() => validatePipelineRunState(injected)).toThrow(
      /activation 1 records session "s-forged" without a corresponding session_created event/,
    );
  });

  test("recorded stages must be confirmed by the event journal in both directions", () => {
    // an accepted digest whose result_accepted event was dropped
    const digestWithoutEvent = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "activation_failed", reason: "worker_failed", sessionCleanup: "completed" },
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    const dropped = JSON.parse(JSON.stringify(digestWithoutEvent));
    dropped.events = dropped.events.filter((event: any) => event.kind !== "result_accepted");
    dropped.revision = dropped.events.length;
    dropped.events.forEach((event: any, index: number) => {
      event.sequence = index + 1;
    });
    expect(() => validatePipelineRunState(dropped)).toThrow(
      /records an accepted result digest without a result_accepted event/,
    );
    // a cleanup-completed phase whose cleanup event never happened: the
    // journal stops at result_accepted while the record claims the cleanup
    const truncated = runCommands([
      createRun(),
      { kind: "start_activation", stateId: "execute", profile: "default" },
      { kind: "activation_session_created", sessionId: "s1" },
      { kind: "activation_agent_running" },
      { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    const forged = JSON.parse(JSON.stringify(truncated));
    forged.activations[0].phase = "session_cleanup_completed";
    forged.activations[0].session_cleanup = "completed";
    expect(() => validatePipelineRunState(forged)).toThrow(
      /has phase "session_cleanup_completed" but the event journal does not confirm the stage session_cleanup_completed/,
    );
  });
});

describe("pipeline execution digest", () => {
  function agentState(
    id: string,
    transitions: { outcome: string; to: string }[],
    overrides: Partial<ResolvedAgentState> = {},
  ): ResolvedAgentState {
    return {
      id,
      type: "agent",
      profile: "default",
      promptPath: `/bundle/${id}.md`,
      promptContent: `prompt for ${id}`,
      inputs: ["task"],
      resultSchemaPath: `/bundle/${id}.schema.json`,
      resultSchema: { type: "object" },
      timeout_seconds: 60,
      max_attempts: 1,
      transitions,
      ...overrides,
    };
  }

  function terminalState(id: string, result: "success" | "failed"): ResolvedState {
    return { id, type: "terminal", result };
  }

  function pipeline(states: ResolvedState[], overrides: Partial<ResolvedPipeline> = {}): ResolvedPipeline {
    return {
      schema_version: 1,
      bundleRoot: "/bundle",
      entry_state: states[0]?.id ?? "a",
      max_transitions: 1,
      inputs: [{ id: "task", path: "TASK.md", protected: true }],
      states,
      ...overrides,
    };
  }

  const oneStep = () =>
    pipeline([
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]);

  test("object key order does not change the digest", () => {
    const schemaA = JSON.parse('{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"number"}}}');
    const schemaB = JSON.parse('{"properties":{"b":{"type":"number"},"a":{"type":"string"}},"type":"object"}');
    const first = pipeline([agentState("a", [{ outcome: "completed", to: "done" }], { resultSchema: schemaA }), terminalState("done", "success")]);
    const second = pipeline([agentState("a", [{ outcome: "completed", to: "done" }], { resultSchema: schemaB }), terminalState("done", "success")]);
    expect(pipelineExecutionDigest(first)).toBe(pipelineExecutionDigest(second));
    // canonical JSON is key-sorted at every level
    expect(canonicalJson({ b: 1, a: { y: 2, x: [3, { d: 4, c: 5 }] } })).toBe(
      '{"a":{"x":[3,{"c":5,"d":4}],"y":2},"b":1}',
    );
  });

  test("array order changes the digest", () => {
    const reordered = pipeline([
      terminalState("done", "success"),
      agentState("a", [{ outcome: "completed", to: "done" }]),
    ]);
    expect(pipelineExecutionDigest(oneStep())).not.toBe(pipelineExecutionDigest(reordered));
    const reversedTransitions = pipeline([
      agentState("a", [
        { outcome: "left", to: "halt" },
        { outcome: "right", to: "done" },
      ]),
      terminalState("halt", "failed"),
      terminalState("done", "success"),
    ]);
    const flipped = pipeline([
      agentState("a", [
        { outcome: "right", to: "done" },
        { outcome: "left", to: "halt" },
      ]),
      terminalState("halt", "failed"),
      terminalState("done", "success"),
    ]);
    expect(pipelineExecutionDigest(reversedTransitions)).not.toBe(pipelineExecutionDigest(flipped));
  });

  test("the bundle root is excluded from the digest", () => {
    // the same bundle at a different location: paths move with the root, so
    // the bundle-relative references stay identical and so does the digest
    const moved = pipeline([
      agentState("a", [{ outcome: "completed", to: "done" }], {
        promptPath: "/elsewhere/bundle/a.md",
        resultSchemaPath: "/elsewhere/bundle/a.schema.json",
      }),
      terminalState("done", "success"),
    ], { bundleRoot: "/elsewhere/bundle" });
    expect(pipelineExecutionDigest(moved)).toBe(pipelineExecutionDigest(oneStep()));
    expect(pipelineExecutionSnapshotJson(oneStep())).not.toContain("/bundle");
  });

  test("executable content changes the digest", () => {
    const base = oneStep();
    const promptChange = oneStep();
    promptChange.states[0] = agentState("a", [{ outcome: "completed", to: "done" }], {
      promptContent: "different prompt",
    });
    expect(pipelineExecutionDigest(promptChange)).not.toBe(pipelineExecutionDigest(base));
    const schemaChange = oneStep();
    schemaChange.states[0] = agentState("a", [{ outcome: "completed", to: "done" }], {
      resultSchema: { type: "object", minProperties: 1 },
    });
    expect(pipelineExecutionDigest(schemaChange)).not.toBe(pipelineExecutionDigest(base));
    const timeoutChange = oneStep();
    timeoutChange.states[0] = agentState("a", [{ outcome: "completed", to: "done" }], { timeout_seconds: 61 });
    expect(pipelineExecutionDigest(timeoutChange)).not.toBe(pipelineExecutionDigest(base));
    const budgetChange = oneStep();
    budgetChange.max_transitions = 2;
    expect(pipelineExecutionDigest(budgetChange)).not.toBe(pipelineExecutionDigest(base));
    const inputChange = oneStep();
    inputChange.inputs = [{ id: "task", path: "OTHER.md", protected: true }];
    expect(pipelineExecutionDigest(inputChange)).not.toBe(pipelineExecutionDigest(base));
    const entryChange = oneStep();
    entryChange.entry_state = "b";
    expect(pipelineExecutionDigest(entryChange)).not.toBe(pipelineExecutionDigest(base));
  });

  test("the digest is a stable hex sha-256 and deterministic", () => {
    const digest = pipelineExecutionDigest(oneStep());
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    expect(pipelineExecutionDigest(oneStep())).toBe(digest);
  });

  test("hostile schema keys are hashed by content, not by prototype behavior", () => {
    const hostile = oneStep();
    hostile.states[0] = agentState("a", [{ outcome: "completed", to: "done" }], {
      resultSchema: JSON.parse('{"type":"object","__proto__":{"x":1},"constructor":{"y":2}}'),
    });
    const digest = pipelineExecutionDigest(hostile);
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    const sameContent = oneStep();
    sameContent.states[0] = agentState("a", [{ outcome: "completed", to: "done" }], {
      resultSchema: JSON.parse('{"constructor":{"y":2},"type":"object","__proto__":{"x":1}}'),
    });
    expect(pipelineExecutionDigest(sameContent)).toBe(digest);
    expect(pipelineExecutionSnapshotJson(hostile)).toContain('"__proto__"');
  });
});
