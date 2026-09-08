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

const PROTECTED_INPUT: ProtectedInputState = {
  id: "task",
  path: "TASK.md",
  sha256: "b".repeat(64),
};

const STEP = Object.freeze({
  from: "execute",
  outcome: "completed",
  to: "completed",
  transition_index: 0,
});

const TICKS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t)));

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
    protectedInput: PROTECTED_INPUT,
    initialPhase: "validating",
    ...overrides,
  };
}

function happyPathCommands(): PipelineRunCommand[] {
  return [
    createRun(),
    { kind: "enter_phase", phase: "creating_session" },
    { kind: "session_created", sessionId: "dhs_child" },
    { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
    { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] },
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
  expect(() => reducePipelineRunCommand(current, command, tick(9))).toThrow(pattern);
}

describe("pipeline run state reducer", () => {
  test("happy path to terminal success", () => {
    const state = runCommands(happyPathCommands());
    expect(state).not.toBeNull();
    if (state === null) {
      throw new Error("unreachable");
    }
    expect(state.schema_version).toBe(1);
    expect(state.revision).toBe(7);
    expect(state.status).toBe("success");
    expect(state.phase).toBe("finished");
    expect(state.run_id).toBe("run-1");
    expect(state.workspace).toBe("/work");
    expect(state.started_at).toBe("2026-01-01T00:00:01.000Z");
    expect(state.updated_at).toBe("2026-01-01T00:00:07.000Z");
    expect(state.pipeline).toEqual(IDENTITY);
    expect(state.protected_input).toEqual(PROTECTED_INPUT);
    expect(state.session_id).toBe("dhs_child");
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.attempt).toEqual({
      state_id: "execute",
      attempt: 1,
      profile: "default",
      session_id: "dhs_child",
      phase: "completed",
    });
    expect(state.transitions).toEqual([
      {
        index: 0,
        from: "execute",
        outcome: "completed",
        to: "completed",
        attempt: 1,
        result_sha256: "c".repeat(64),
        artifacts: ["out/product.txt"],
      },
    ]);
    expect(state.terminal).toEqual({ state_id: "completed", result: "success" });
    expect(state.failure).toBeUndefined();
    expect(state.events.map((event) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
    ]);
    expect(state.events.map((event) => event.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    // the document round-trips through the exact-field validator
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

  test("worker failure without transition", () => {
    const state = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "run_failed", reason: "worker_failed" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.phase).toBe("finished");
    expect(state?.failure).toEqual({ reason: "worker_failed" });
    expect(state?.transitions).toEqual([]);
    expect(state?.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(state?.attempt?.phase).toBe("failed");
    expect(state?.terminal).toBeUndefined();
    expect(state?.events.map((event) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "run_failed",
    ]);
  });

  test("unknown outcome before any transition: failure recorded, graph never moved", () => {
    const state = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "run_failed", reason: "unknown_outcome" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.failure).toEqual({ reason: "unknown_outcome" });
    expect(state?.transitions).toEqual([]);
    expect(state?.cursor.current_state).toBe("execute");
  });

  test("timeout is an ordinary failure", () => {
    const state = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "run_failed", reason: "worker_timeout" },
    ]);
    expect(state?.status).toBe("failed");
    expect(state?.failure).toEqual({ reason: "worker_timeout" });
  });

  test("user signal keeps a normalized signal reason and failed status", () => {
    for (const [reason, signal] of [
      ["signal_sigint", "SIGINT"],
      ["signal_sigterm", "SIGTERM"],
    ] as const) {
      const state = runCommands([
        createRun(),
        { kind: "enter_phase", phase: "creating_session" },
        { kind: "session_created", sessionId: "dhs_child" },
        { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
        { kind: "run_failed", reason },
      ]);
      expect(state?.status).toBe("failed");
      expect(state?.failure).toEqual({ reason });
      expect(signal).toBeDefined();
    }
  });

  test("cleanup failure is recorded with its dedicated reason and wins over nothing after commit", () => {
    const state = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "run_cleanup_failed", reason: SESSION_CLEANUP_FAILURE_REASON },
    ]);
    expect(state?.status).toBe("cleanup_failed");
    expect(state?.failure).toEqual({ reason: "session_cleanup_failed" });
    expect(state?.phase).toBe("finished");
    // cleanup failure keeps priority: the lifecycle commits only the
    // cleanup_failed status, so a failed status after it is rejected
    expectCommandRejection(state!, { kind: "run_failed", reason: "worker_failed" }, /cannot overwrite status/);
  });

  test("illegal phase and ordering are rejected", () => {
    const created = runCommands([createRun()])!;
    // enter_phase must go to the immediate successor
    expectCommandRejection(created, { kind: "enter_phase", phase: "agent_running" }, /illegal phase transition/);
    expectCommandRejection(created, { kind: "enter_phase", phase: "finished" }, /illegal phase transition/);
    // session creation needs the creating_session phase
    expectCommandRejection(created, { kind: "session_created", sessionId: "dhs_child" }, /requires phase/);
    // attempt needs the session first
    const entered = runCommands([createRun(), { kind: "enter_phase", phase: "creating_session" }])!;
    expectCommandRejection(entered, { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" }, /requires phase/);
    // transition needs the attempt
    const session = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
    ])!;
    expectCommandRejection(
      session,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires phase "agent_running"/,
    );
    // terminal needs the cursor at the terminal state
    const attempt = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
    ])!;
    expectCommandRejection(
      attempt,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /does not match the cursor/,
    );
    // success without a terminal
    expectCommandRejection(attempt, { kind: "run_succeeded" }, /requires a reached terminal state/);
  });

  test("duplicate transitions and events are rejected", () => {
    const withTransition = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ])!;
    // the same transition again: the cursor has moved, so it cannot re-apply
    expectCommandRejection(
      withTransition,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /cursor is at/,
    );
    // a second transition would be index 1 but the graph has no such step;
    // the reducer only accepts the engine's next step from the current cursor
    expectCommandRejection(
      withTransition,
      {
        kind: "transition_committed",
        step: { from: "execute", outcome: "completed", to: "completed", transition_index: 1 },
        attempt: 1,
        resultSha256: "c".repeat(64),
        artifacts: [],
      },
      /cursor is at/,
    );
    // double terminal
    const transitioned: PipelineRunCommand[] = [
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
    ];
    const terminal = runCommands([
      ...transitioned,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    ])!;
    expectCommandRejection(
      terminal,
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
      /requires phase "agent_running"/,
    );
    // double failure
    const failed = runCommands([
      createRun(),
      { kind: "run_failed", reason: "worker_failed" },
    ])!;
    expectCommandRejection(failed, { kind: "run_failed", reason: "worker_failed" }, /cannot overwrite status/);
  });

  test("immutable identity: no command can change identity, workspace, run id, or protected input", () => {
    const state = runCommands(happyPathCommands())!;
    expect(state.pipeline).toEqual(IDENTITY);
    expect(state.protected_input).toEqual(PROTECTED_INPUT);
    expect(state.run_id).toBe("run-1");
    expect(state.workspace).toBe("/work");
    expect(state.schema_version).toBe(1);
  });

  test("mutations after the terminal commit are forbidden", () => {
    const terminal = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    ])!;
    expectCommandRejection(
      terminal,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires phase "agent_running"/,
    );
    expectCommandRejection(
      terminal,
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      /requires phase/,
    );
    expectCommandRejection(terminal, { kind: "enter_phase", phase: "finished" }, /illegal phase transition/);
    expectCommandRejection(terminal, { kind: "session_created", sessionId: "dhs_x" }, /requires phase/);
    // only the final lifecycle events remain legal
    expect(reducePipelineRunCommand(terminal, { kind: "run_succeeded" }, tick(9)).status).toBe("success");
  });

  test("success is only legal after a success terminal, and a late signal can rewrite it", () => {
    const succeeded = runCommands(happyPathCommands())!;
    expectCommandRejection(
      succeeded,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      /requires an active run/,
    );
    expectCommandRejection(
      succeeded,
      { kind: "run_failed", reason: "worker_failed" },
      /only be rewritten to failed by a signal reason/,
    );
    const rewritten = reducePipelineRunCommand(succeeded, { kind: "run_failed", reason: "signal_sigterm" }, tick(9));
    expect(rewritten.status).toBe("failed");
    expect(rewritten.phase).toBe("finished");
    expect(rewritten.failure).toEqual({ reason: "signal_sigterm" });
    expect(rewritten.events.map((event) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
      "run_failed",
    ]);
    // and the rewrite is final
    expectCommandRejection(rewritten, { kind: "run_failed", reason: "signal_sigterm" }, /cannot overwrite status/);
    // a failed terminal can never become success
    const failedTerminal = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: [] },
      { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "failed" },
    ])!;
    expectCommandRejection(failedTerminal, { kind: "run_succeeded" }, /result success/);
  });

  test("cursor, transition append, and event appear in one snapshot; rejected commands change nothing", () => {
    const attempt = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
    ])!;
    // deep-freeze the input: the pure reducer must not mutate it
    const frozen = JSON.parse(JSON.stringify(attempt));
    deepFreeze(frozen);
    const next = reducePipelineRunCommand(
      frozen,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: ["out.txt"] },
      tick(9),
    );
    expect(next.transitions.length).toBe(1);
    expect(next.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(next.events[next.events.length - 1]?.kind).toBe("transition_committed");
    expect(next.revision).toBe(attempt.revision + 1);
    // the input snapshot is untouched
    expect(attempt.transitions.length).toBe(0);
    expect(attempt.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(attempt.revision).toBe(4);
    expect(attempt.events.length).toBe(4);
  });

  test("the reducer validates command payloads fail-closed", () => {
    expectCommandRejection(null, { kind: "session_created", sessionId: "x" }, /no pipeline run state exists/);
    expectCommandRejection(
      null,
      {
        kind: "create_run",
        runId: "run-1",
        workspace: "/work",
        identity: { ...IDENTITY, execution_snapshot_sha256: "nothex" },
        protectedInput: PROTECTED_INPUT,
        initialPhase: "validating",
      },
      /lowercase hex execution snapshot digest/,
    );
    const created = runCommands([createRun()])!;
    expectCommandRejection(
      created,
      { kind: "create_run", runId: "run-2", workspace: "/w", identity: IDENTITY, protectedInput: PROTECTED_INPUT, initialPhase: "validating" },
      /already exists/,
    );
    const entered = runCommands([createRun(), { kind: "enter_phase", phase: "creating_session" }])!;
    expectCommandRejection(
      entered,
      { kind: "session_created", sessionId: "" },
      /non-empty session id/,
    );
    const attempt = runCommands([
      createRun(),
      { kind: "enter_phase", phase: "creating_session" },
      { kind: "session_created", sessionId: "dhs_child" },
      { kind: "attempt_started", stateId: "execute", attempt: 1, profile: "default" },
    ])!;
    expectCommandRejection(
      attempt,
      { kind: "transition_committed", step: STEP, attempt: 2, resultSha256: "c".repeat(64), artifacts: [] },
      /the running attempt is 1/,
    );
    expectCommandRejection(
      attempt,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "nothex", artifacts: [] },
      /lowercase hex accepted result digest/,
    );
    expectCommandRejection(
      attempt,
      { kind: "transition_committed", step: STEP, attempt: 1, resultSha256: "c".repeat(64), artifacts: ["../escape"] },
      /clean workspace-relative paths/,
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

  test("malformed and truncated documents are rejected", () => {
    expect(() => parsePipelineRunState("{ not json")).toThrow(/not valid JSON/);
    expect(() => parsePipelineRunState('{"schema_version":1')).toThrow(/not valid JSON/);
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
  });

  test("missing fields are rejected", () => {
    const state = JSON.parse(JSON.stringify(valid()));
    delete state.cursor;
    expect(() => validatePipelineRunState(state)).toThrow(/missing required field "cursor"/);
    const noEvents = JSON.parse(JSON.stringify(valid()));
    delete noEvents.events;
    expect(() => validatePipelineRunState(noEvents)).toThrow(/missing required field "events"/);
  });

  test("wrong types and enums are rejected", () => {
    const state = JSON.parse(JSON.stringify(valid()));
    state.revision = "7";
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
    badSchema.schema_version = 2;
    expect(() => validatePipelineRunState(badSchema)).toThrow(/schema_version 2, expected 1/);
    const artifact = JSON.parse(JSON.stringify(valid()));
    artifact.transitions[0].artifacts = ["/etc/passwd"];
    expect(() => validatePipelineRunState(artifact)).toThrow(/clean workspace-relative/);
  });

  test("inconsistent revision, sequence, cursor, and trace are rejected", () => {
    const revisionGap = JSON.parse(JSON.stringify(valid()));
    revisionGap.revision = 9;
    expect(() => validatePipelineRunState(revisionGap)).toThrow(/revision 9 must equal the number of events 7/);
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
    const indexGap = JSON.parse(JSON.stringify(valid()));
    indexGap.transitions[0].index = 3;
    expect(() => validatePipelineRunState(indexGap)).toThrow(/declares index 3/);
  });

  test("illegal event orders are rejected", () => {
    const noCreateFirst = JSON.parse(JSON.stringify(valid()));
    noCreateFirst.events.reverse();
    for (let i = 0; i < noCreateFirst.events.length; i++) {
      noCreateFirst.events[i].sequence = i + 1;
    }
    expect(() => validatePipelineRunState(noCreateFirst)).toThrow(/first pipeline run event must be run_created/);
    const skip = JSON.parse(JSON.stringify(valid()));
    skip.events[1].kind = "attempt_started";
    expect(() => validatePipelineRunState(skip)).toThrow(/may not follow/);
    const postTerminal = JSON.parse(JSON.stringify(valid()));
    postTerminal.events[6].kind = "transition_committed";
    postTerminal.status = "active";
    delete postTerminal.failure;
    expect(() => validatePipelineRunState(postTerminal)).toThrow(/may not follow/);
    const doubleEnd = JSON.parse(JSON.stringify(valid()));
    doubleEnd.events.push({ sequence: 8, kind: "run_succeeded", at: doubleEnd.updated_at });
    doubleEnd.revision = 8;
    expect(() => validatePipelineRunState(doubleEnd)).toThrow(/may not follow/);
  });

  test("status, phase, terminal, attempt, and failure coherence is enforced", () => {
    const successWithoutTerminal = JSON.parse(JSON.stringify(valid()));
    delete successWithoutTerminal.terminal;
    expect(() => validatePipelineRunState(successWithoutTerminal)).toThrow(/requires a reached terminal/);
    const activeAfterTerminalEvent = JSON.parse(JSON.stringify(valid()));
    activeAfterTerminalEvent.status = "active";
    activeAfterTerminalEvent.failure = undefined;
    activeAfterTerminalEvent.phase = "finalizing";
    activeAfterTerminalEvent.events[6].kind = "run_failed";
    expect(() => validatePipelineRunState(activeAfterTerminalEvent)).toThrow(
      /an active run must not end with the terminal event run_failed/,
    );
    const failedWithoutReason = JSON.parse(JSON.stringify(valid()));
    failedWithoutReason.status = "failed";
    failedWithoutReason.failure = undefined;
    failedWithoutReason.events[6].kind = "run_failed";
    expect(() => validatePipelineRunState(failedWithoutReason)).toThrow(/requires a normalized failure reason/);
    const cleanupWithWrongReason = JSON.parse(JSON.stringify(valid()));
    cleanupWithWrongReason.status = "cleanup_failed";
    cleanupWithWrongReason.failure = { reason: "worker_failed" };
    cleanupWithWrongReason.events[6].kind = "run_cleanup_failed";
    expect(() => validatePipelineRunState(cleanupWithWrongReason)).toThrow(/requires the failure reason "session_cleanup_failed"/);
    const attemptSessionMismatch = JSON.parse(JSON.stringify(valid()));
    attemptSessionMismatch.attempt.session_id = "dhs_other";
    expect(() => validatePipelineRunState(attemptSessionMismatch)).toThrow(/attempt.session_id must equal the run session_id/);
    const runningAttemptWithTerminal = JSON.parse(JSON.stringify(valid()));
    runningAttemptWithTerminal.attempt.phase = "running";
    expect(() => validatePipelineRunState(runningAttemptWithTerminal)).toThrow(/must be "completed"/);
    const terminalCursorMismatch = JSON.parse(JSON.stringify(valid()));
    terminalCursorMismatch.terminal.state_id = "elsewhere";
    expect(() => validatePipelineRunState(terminalCursorMismatch)).toThrow(/does not match the cursor/);
    const transitionWithoutAttempt = JSON.parse(JSON.stringify(valid()));
    delete transitionWithoutAttempt.attempt;
    expect(() => validatePipelineRunState(transitionWithoutAttempt)).toThrow(/committed transitions require a recorded attempt/);
    const futureAttempt = JSON.parse(JSON.stringify(valid()));
    futureAttempt.transitions[0].attempt = 2;
    expect(() => validatePipelineRunState(futureAttempt)).toThrow(/which was never started/);
    const finishedActive = JSON.parse(JSON.stringify(valid()));
    finishedActive.status = "active";
    delete finishedActive.failure;
    finishedActive.events.pop();
    finishedActive.revision = 6;
    finishedActive.phase = "finished";
    expect(() => validatePipelineRunState(finishedActive)).toThrow(/must not be in the finished phase/);
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
