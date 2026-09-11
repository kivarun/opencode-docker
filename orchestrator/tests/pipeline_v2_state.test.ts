import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
  PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
  PIPELINE_V2_TERMINAL_FAILURE_REASON,
  PipelineV2StateError,
  parsePipelineV2RunState,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineDecisionStateRecord,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";

const hex = (char: string): string => char.repeat(64);

const IDENTITY: PipelineV2RunPipelineIdentity = {
  schema_version: 2,
  bundle_root: "/opt/orchestrator/pipelines/v2",
  execution_snapshot_sha256: hex("a"),
  entry_state: "implement",
  max_transitions: 6,
};

const ENTRY_TERMINAL_IDENTITY: PipelineV2RunPipelineIdentity = { ...IDENTITY, entry_state: "done" };

const DECISION_ENTRY_IDENTITY: PipelineV2RunPipelineIdentity = {
  ...IDENTITY,
  entry_state: "check",
  max_transitions: 1,
};

const CYCLE_IDENTITY: PipelineV2RunPipelineIdentity = { ...IDENTITY, entry_state: "a", max_transitions: 4 };

const INPUTS: PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: hex("b") },
  { id: "notes", type: "json", protected: false, digest: hex("c") },
];

const TICKS = Array.from({ length: 64 }, (_, t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)));

function tick(index: number): Date {
  const value = TICKS[index % TICKS.length];
  if (value === undefined) {
    throw new Error(`tick ${index} out of range`);
  }
  return value;
}

interface Driver {
  apply(command: PipelineV2RunCommand): PipelineV2RunState;
  reject(command: PipelineV2RunCommand, messagePart: string): void;
  readonly current: PipelineV2RunState | null;
  /** A per-driver counter for deterministic default session ids. */
  nextSessionNumber(): number;
}

function createDriver(
  identity: PipelineV2RunPipelineIdentity = IDENTITY,
  inputs: readonly PipelineV2RunInputState[] = INPUTS,
  runId = "run-1",
): Driver {
  let state: PipelineV2RunState | null = null;
  let tickIndex = 0;
  let sessionCounter = 0;
  const nextTick = (): Date => {
    const at = tick(tickIndex);
    tickIndex += 1;
    return at;
  };
  return {
    apply(command) {
      state = reducePipelineV2RunCommand(state, command, nextTick());
      return state as PipelineV2RunState;
    },
    nextSessionNumber() {
      sessionCounter += 1;
      return sessionCounter;
    },
    reject(command, messagePart) {
      const before = state === null ? null : JSON.parse(JSON.stringify(state));
      let message = "";
      try {
        reducePipelineV2RunCommand(state, command, nextTick());
      } catch (cause) {
        expect(cause).toBeInstanceOf(PipelineV2StateError);
        message = (cause as Error).message;
      }
      expect(message).toContain(messagePart);
      expect(state === null ? null : JSON.parse(JSON.stringify(state))).toEqual(before);
    },
    get current() {
      return state;
    },
  };
}

function createRun(
  identity: PipelineV2RunPipelineIdentity = IDENTITY,
  inputs: readonly PipelineV2RunInputState[] = INPUTS,
  runId = "run-1",
): PipelineV2RunCommand {
  return { kind: "create_run", runId, pipeline: identity, inputs };
}

/**
 * Drives one agent activation up to "running". Both durable sessions are
 * created by default with distinct ids derived from the driver's own
 * counter; `sessions` renames them and `"none"` stops right after data
 * preparation (no sessions at all).
 */
function startAgent(
  driver: Driver,
  stateId: string,
  sessions?: { execution?: string; tool?: string } | "none",
): void {
  driver.apply({ kind: "start_agent_execution", stateId, profile: "coder" });
  driver.apply({ kind: "agent_data_prepared" });
  if (sessions === "none") {
    return;
  }
  const number = driver.nextSessionNumber();
  driver.apply({
    kind: "agent_execution_session_created",
    sessionId: sessions?.execution ?? `exec-${number}`,
  });
  driver.apply({
    kind: "agent_tool_session_created",
    sessionId: sessions?.tool ?? `tool-${number}`,
  });
  driver.apply({ kind: "agent_running" });
}

function acceptOutputs(driver: Driver, outputs: readonly { id: string; digest: string }[]): void {
  driver.apply({ kind: "agent_outputs_accepted", outputs: [...outputs] });
  driver.apply({ kind: "agent_cleanup_completed" });
}

function commitTransition(
  driver: Driver,
  from: string,
  outcome: string,
  to: string,
  executionIndex: number,
  transitionIndex = 0,
): void {
  driver.apply({
    kind: "transition_committed",
    step: { from, outcome, to, transition_index: transitionIndex },
    executionIndex,
  });
}

/**
 * The main happy path: implement (agent) -> check (decision) -> ship (agent)
 * -> done (terminal success) -> publish -> succeed. Execution indexes are
 * 1, 2, 3; the agent executions reuse the shared execution index as their
 * activation index (1 and 3, with the decision occupying 2).
 */
function playSuccessRun(driver: Driver): void {
  driver.apply(createRun());
  startAgent(driver, "implement", { execution: "sess-1" });
  acceptOutputs(driver, [{ id: "plan", digest: hex("d") }]);
  commitTransition(driver, "implement", "completed", "check", 1);
  driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
  driver.apply({
    kind: "decision_evaluated",
    result: {
      status: "selected",
      outcome: "approved",
      decision: "approved",
      rule_id: "R1",
      active_constraint_ids: ["HC1"],
    },
  });
  commitTransition(driver, "check", "approved", "ship", 2);
  startAgent(driver, "ship", { execution: "sess-2" });
  acceptOutputs(driver, []);
  commitTransition(driver, "ship", "completed", "done", 3);
  driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  driver.apply({
    kind: "run_outputs_published",
    outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }],
  });
  driver.apply({ kind: "run_succeeded" });
}

/**
 * Drives the graph up to the cursor sitting on the terminal state "done"
 * with three committed transitions, without recording the terminal yet.
 */
function playUpToTerminal(driver: Driver): void {
  driver.apply(createRun());
  startAgent(driver, "implement", { execution: "sess-1" });
  acceptOutputs(driver, [{ id: "plan", digest: hex("d") }]);
  commitTransition(driver, "implement", "completed", "check", 1);
  driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
  driver.apply({
    kind: "decision_evaluated",
    result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
  });
  commitTransition(driver, "check", "uncovered", "ship", 2);
  startAgent(driver, "ship", { execution: "sess-2" });
  acceptOutputs(driver, []);
  commitTransition(driver, "ship", "completed", "done", 3);
}

/** Deep-clone a reduced state into a plain, writable draft for loader tests. */
function draftOf(state: PipelineV2RunState): any {
  return JSON.parse(JSON.stringify(state));
}

function expectInvalid(
  state: PipelineV2RunState,
  mutate: (draft: any) => void,
  messagePart: string,
): void {
  const draft = draftOf(state);
  mutate(draft);
  let message = "";
  try {
    validatePipelineV2RunState(draft);
  } catch (cause) {
    expect(cause).toBeInstanceOf(PipelineV2StateError);
    message = (cause as Error).message;
  }
  expect(message).toContain(messagePart);
}

/** The reducer never produces a document the loader rejects: check after serialization. */
function requireLoadableSnapshot(state: PipelineV2RunState): void {
  const draft = draftOf(state);
  const loaded = validatePipelineV2RunState(draft);
  expect(loaded).toEqual(draft);
}

/** Driver wrapper asserting every accepted command stays loader-compatible after JSON serialization. */
function loadableDriver(driver: Driver): Driver {
  return {
    apply(command) {
      const state = driver.apply(command);
      requireLoadableSnapshot(state);
      return state;
    },
    reject(command, messagePart) {
      driver.reject(command, messagePart);
    },
    nextSessionNumber() {
      return driver.nextSessionNumber();
    },
    get current() {
      return driver.current;
    },
  };
}

function expectDeepFrozen(value: unknown, path = "state"): void {
  if (Array.isArray(value)) {
    expect(Object.isFrozen(value), `array ${path} is frozen`).toBe(true);
    value.forEach((entry, index) => expectDeepFrozen(entry, `${path}[${index}]`));
    return;
  }
  if (value !== null && typeof value === "object") {
    expect(Object.isFrozen(value), `object ${path} is frozen`).toBe(true);
    for (const child of Object.values(value)) {
      expectDeepFrozen(child, `${path}.*`);
    }
  }
}

function collectKeys(value: unknown, into: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectKeys(entry, into);
    }
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value)) {
      into.add(key);
      collectKeys(child, into);
    }
  }
}

describe("pipeline v2 run state schema v6", () => {
  test("reduces agent -> decision -> agent -> terminal success with the shared execution index", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expect(state.schema_version).toBe(6);
    expect(state.revision).toBe(23);
    expect(state.status).toBe("success");
    expect(state.phase).toBe("finished");
    expect(state.pipeline.schema_version).toBe(2);
    expect(state.cursor).toEqual({ current_state: "done", transition_count: 3 });
    expect(state.executions.map((execution) => execution.index)).toEqual([1, 2, 3]);
    expect(state.executions.map((execution) => execution.type)).toEqual(["agent", "decision", "agent"]);
    const agents = state.executions.filter((execution) => execution.type === "agent");
    expect(agents.map((execution) => execution.index)).toEqual([1, 3]);
    expect(state.executions[1]!.phase).toBe("evaluated");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.run_outputs).toEqual([
      { id: "plan", type: "file", required: true, present: true, digest: hex("d") },
    ]);
    expect(state.failure).toBeUndefined();
    expect(state.transitions).toEqual([
      { index: 0, from: "implement", outcome: "completed", to: "check", execution_index: 1 },
      { index: 0, from: "check", outcome: "approved", to: "ship", execution_index: 2 },
      { index: 0, from: "ship", outcome: "completed", to: "done", execution_index: 3 },
    ]);
    expect(PIPELINE_V2_RUN_STATE_SCHEMA_VERSION).toBe(6);
  });

  test("every accepted command grows the revision by exactly one and refreshes updated_at", () => {
    const driver = createDriver();
    driver.apply(createRun());
    let revision = 1;
    const state = driver.current as PipelineV2RunState;
    expect(state.revision).toBe(revision);
    expect(state.updated_at).toBe(tick(0).toISOString());
    expect(state.started_at).toBe(tick(0).toISOString());
    revision += 1;
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    expect((driver.current as PipelineV2RunState).revision).toBe(revision);
    expect((driver.current as PipelineV2RunState).updated_at).toBe(tick(1).toISOString());
    revision += 1;
    driver.apply({ kind: "agent_data_prepared" });
    expect((driver.current as PipelineV2RunState).revision).toBe(revision);
    revision += 1;
    driver.apply({ kind: "agent_execution_session_created", sessionId: "sess-1" });
    expect((driver.current as PipelineV2RunState).revision).toBe(revision);
    revision += 1;
    driver.apply({ kind: "agent_tool_session_created", sessionId: "tool-1" });
    expect((driver.current as PipelineV2RunState).revision).toBe(revision);
    revision += 1;
    driver.apply({ kind: "agent_running" });
    expect((driver.current as PipelineV2RunState).revision).toBe(revision);
    expect((driver.current as PipelineV2RunState).updated_at).toBe(tick(5).toISOString());
  });

  test("reduces an entry terminal with zero executions and an empty output publication", () => {
    const driver = createDriver(ENTRY_TERMINAL_IDENTITY, []);
    driver.apply(createRun(ENTRY_TERMINAL_IDENTITY, []));
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.apply({ kind: "run_succeeded" });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("success");
    expect(state.executions).toEqual([]);
    expect(state.transitions).toEqual([]);
    expect(state.run_outputs).toEqual([]);
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.cursor).toEqual({ current_state: "done", transition_count: 0 });
  });

  test("finalizes a failed terminal with published outputs as run_failed with terminal_failed", () => {
    const identity = { ...IDENTITY, entry_state: "work", max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "work", { execution: "sess-1" });
    acceptOutputs(driver, [{ id: "report", digest: hex("f") }]);
    commitTransition(driver, "work", "completed", "reject", 1);
    driver.apply({ kind: "terminal_reached", terminalStateId: "reject", terminalResult: "failed" });
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.reject(
      { kind: "run_succeeded" },
      "success requires a terminal state with result success; a failed terminal cannot become success",
    );
    driver.reject(
      { kind: "run_failed", reason: "internal_error" },
      `a failed terminal with published run_outputs finalizes with failure reason ${JSON.stringify(PIPELINE_V2_TERMINAL_FAILURE_REASON)}`,
    );
    driver.apply({ kind: "run_failed", reason: PIPELINE_V2_TERMINAL_FAILURE_REASON });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: PIPELINE_V2_TERMINAL_FAILURE_REASON });
    expect(state.terminal).toEqual({ state_id: "reject", result: "failed" });
    expect(state.run_outputs).toEqual([]);
  });

  test("runs an agent cycle and revisit inside the transition budget", () => {
    const driver = createDriver(CYCLE_IDENTITY, INPUTS);
    driver.apply(createRun(CYCLE_IDENTITY, INPUTS));
    startAgent(driver, "a", { execution: "sess-a1" });
    acceptOutputs(driver, [{ id: "plan-a", digest: hex("1") }]);
    commitTransition(driver, "a", "completed", "b", 1);
    startAgent(driver, "b", { execution: "sess-b" });
    acceptOutputs(driver, []);
    commitTransition(driver, "b", "retry", "a", 2);
    startAgent(driver, "a", { execution: "sess-a2" });
    acceptOutputs(driver, [{ id: "plan-b", digest: hex("2") }]);
    commitTransition(driver, "a", "done", "done", 3);
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.apply({ kind: "run_succeeded" });
    const state = driver.current as PipelineV2RunState;
    expect(state.transitions.length).toBe(3);
    expect(state.transitions.length).toBeLessThanOrEqual(CYCLE_IDENTITY.max_transitions);
    expect(state.executions.map((execution) => `${execution.index}:${execution.state_id}`)).toEqual([
      "1:a",
      "2:b",
      "3:a",
    ]);
    const sessionIds = state.executions.flatMap((execution) => {
      if (execution.type !== "agent") {
        return [];
      }
      return [execution.execution_session_id, execution.tool_session_id];
    }).filter((id): id is string => id !== undefined);
    expect(sessionIds).toHaveLength(6);
    expect(new Set(sessionIds).size).toBe(6);
    const revisited = state.executions.filter(
      (execution) => execution.type === "agent" && execution.state_id === "a",
    );
    expect(revisited.length).toBe(2);
  });

  test("accepts an agent execution with zero output ports", () => {
    const identity = { ...IDENTITY, entry_state: "work", max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "work", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "work", "completed", "done", 1);
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.apply({ kind: "run_succeeded" });
    const state = driver.current as PipelineV2RunState;
    const execution = state.executions[0] as Extract<
      PipelineV2RunState["executions"][number],
      { type: "agent" }
    >;
    expect(execution.outputs).toEqual([]);
  });

  test("routes all four decision result forms through matching transitions", () => {
    const cases: { result: PipelineDecisionStateRecord; outcome: string }[] = [
      {
        result: {
          status: "selected",
          outcome: "approved",
          decision: "approved",
          rule_id: "R1",
          active_constraint_ids: ["HC1", "HC2"],
        },
        outcome: "approved",
      },
      { result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] }, outcome: "uncovered" },
      {
        result: {
          status: "inconsistent_facts",
          outcome: "inconsistent_facts",
          violated_relation_ids: ["R2"],
        },
        outcome: "inconsistent_facts",
      },
      {
        result: {
          status: "invalid_facts",
          outcome: "invalid_facts",
          reason: "non_boolean_fact",
          fact_id: "implement",
          actual_type: "string",
        },
        outcome: "invalid_facts",
      },
    ];
    for (const { result, outcome } of cases) {
      const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
      driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
      driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
      driver.apply({ kind: "decision_evaluated", result });
      driver.apply({
        kind: "transition_committed",
        step: { from: "check", outcome, to: "done", transition_index: 0 },
        executionIndex: 1,
      });
      driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
      driver.apply({ kind: "run_outputs_published", outputs: [] });
      driver.apply({ kind: "run_succeeded" });
      const state = driver.current as PipelineV2RunState;
      const execution = state.executions[0]!;
      expect(execution.type).toBe("decision");
      expect(execution.type === "decision" ? execution.result : undefined).toEqual(result);
      expect(state.transitions).toEqual([
        { index: 0, from: "check", outcome, to: "done", execution_index: 1 },
      ]);
    }
  });

  test("rejects a decision transition whose outcome does not match the recorded result", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "approved",
        decision: "approved",
        rule_id: "R1",
        active_constraint_ids: [],
      },
    });
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "check", outcome: "rejected", to: "done", transition_index: 0 },
        executionIndex: 1,
      },
      'transition carries outcome "rejected", but execution 1 recorded decision outcome "approved"',
    );
  });

  test("rejects unsafe transition steps and terminal ids in the reducer", () => {
    const driver = createDriver();
    driver.apply(createRun());
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, [{ id: "plan", digest: hex("d") }]);
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "../bad", outcome: "completed", to: "check", transition_index: 0 },
        executionIndex: 1,
      },
      "transition_committed step.from must be a safe non-empty identifier",
    );
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "completed", to: "../bad", transition_index: 0 },
        executionIndex: 1,
      },
      "transition_committed step.to must be a safe non-empty identifier",
    );
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "", to: "check", transition_index: 0 },
        executionIndex: 1,
      },
      "transition_committed step.outcome must be a non-empty string",
    );
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "completed", to: "check", transition_index: -1 },
        executionIndex: 1,
      },
      "transition_committed step.transition_index must be a non-negative safe integer",
    );
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "approved",
        decision: "approved",
        rule_id: "R1",
        active_constraint_ids: [],
      },
    });
    commitTransition(driver, "check", "approved", "done", 2);
    driver.reject(
      { kind: "terminal_reached", terminalStateId: "../bad", terminalResult: "success" },
      "terminal_reached terminal state id must be a safe non-empty identifier",
    );
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.apply({ kind: "run_succeeded" });
    const state = driver.current as PipelineV2RunState;
    expect(state.terminal?.state_id).toBe("done");
    expect(state.transitions[1]).toEqual({
      index: 0,
      from: "check",
      outcome: "approved",
      to: "done",
      execution_index: 2,
    });
    expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  });

  test("a selected decision record must carry outcome equal to decision", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "selected",
          outcome: "alpha",
          decision: "beta",
          rule_id: "R1",
          active_constraint_ids: [],
        },
      },
      'records the selected decision "beta", but its outcome is "alpha"',
    );
    driver.apply({
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "alpha",
        decision: "alpha",
        rule_id: "R1",
        active_constraint_ids: [],
      },
    });
    commitTransition(driver, "check", "alpha", "done", 1);
    const state = driver.current as PipelineV2RunState;
    const execution = state.executions[0]!;
    if (execution.type !== "decision" || execution.result === undefined) {
      throw new Error("expected a settled decision execution with a recorded result");
    }
    if (execution.result.status !== "selected") {
      throw new Error("expected a selected decision record");
    }
    expect(execution.result.outcome).toBe("alpha");
    expect(execution.result.decision).toBe("alpha");
    expect(state.transitions).toEqual([
      { index: 0, from: "check", outcome: "alpha", to: "done", execution_index: 1 },
    ]);
  });

  test("the loader rejects a persisted selected record whose outcome differs from its decision", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: {
        status: "selected",
        outcome: "alpha",
        decision: "alpha",
        rule_id: "R1",
        active_constraint_ids: [],
      },
    });
    const state = driver.current as PipelineV2RunState;
    expectInvalid(
      state,
      (draft) => {
        const execution = draft.executions[0]!;
        execution.result.decision = "beta";
      },
      'records the selected decision "beta", but its outcome is "alpha"',
    );
  });

  test("rejects a transition before the agent execution is cleaned up", () => {
    const driver = createDriver();
    driver.apply(createRun());
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_outputs_accepted", outputs: [] });
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "completed", to: "check", transition_index: 0 },
        executionIndex: 1,
      },
      'committing a transition requires the agent execution to be cleaned up, execution 1 has phase "outputs_accepted"',
    );
  });

  test("rejects a transition before the decision result is recorded", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "check", outcome: "approved", to: "done", transition_index: 0 },
        executionIndex: 1,
      },
      'committing a transition requires the decision execution to be evaluated, execution 1 has phase "evaluating"',
    );
  });

  test("a failed execution never receives a transition", () => {
    const driver = createDriver();
    driver.apply(createRun());
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "completed", to: "check", transition_index: 0 },
        executionIndex: 1,
      },
      'committing a transition requires the agent execution to be cleaned up, execution 1 has phase "failed"',
    );
    driver.reject(
      { kind: "start_agent_execution", stateId: "implement", profile: "coder" },
      "a new execution requires the previous execution's transition to be committed",
    );
    driver.apply({ kind: "run_failed", reason: "worker_failed" });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("failed");
    expect(state.transitions).toEqual([]);
  });

  test("records a decision failure and rejects a follow-up execution", () => {
    const identity = { ...IDENTITY, entry_state: "check", max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({ kind: "decision_failed", reason: "decision_input_invalid" });
    driver.reject(
      { kind: "start_agent_execution", stateId: "check", profile: "coder" },
      "a new execution requires the previous execution's transition to be committed",
    );
    driver.apply({ kind: "run_failed", reason: "decision_input_invalid" });
    const state = driver.current as PipelineV2RunState;
    expect(state.executions[0]).toMatchObject({
      type: "decision",
      phase: "failed",
      failure_reason: "decision_input_invalid",
    });
  });

  test("rejects a second transition for the same execution", () => {
    const identity = { ...IDENTITY, max_transitions: 4 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "implement", outcome: "approved", to: "done", transition_index: 1 },
        executionIndex: 1,
      },
      "execution 1 already carries a committed transition",
    );
  });

  test("rejects skip and duplicate execution indexes when loading", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.executions[1].index = 5;
    }, "execution indexes must be contiguous from 1");
    expectInvalid(state, (draft) => {
      draft.executions[0].index = 2;
    }, "execution at position 0 declares index 2; execution indexes must be contiguous from 1");
  });

  test("enforces the transition budget at commit and start time and when loading", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    commitTransition(driver, "check", "uncovered", "ship", 2);
    driver.reject(
      { kind: "start_agent_execution", stateId: "ship", profile: "coder" },
      "starting execution 3 would exceed the pipeline transition budget 2 (2 transitions already committed)",
    );
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "ship", outcome: "completed", to: "done", transition_index: 0 },
        executionIndex: 3,
      },
      "transition references execution 3, but the last execution is 2",
    );
    driver.apply({ kind: "run_failed", reason: "transition_budget_exhausted" });
    const state = driver.current as PipelineV2RunState;
    expect(state.failure).toEqual({ reason: "transition_budget_exhausted" });
    expect(state.transitions.length).toBe(2);

    const successDriver = createDriver();
    playSuccessRun(successDriver);
    const success = successDriver.current as PipelineV2RunState;
    expectInvalid(success, (draft) => {
      draft.pipeline.max_transitions = 2;
    }, "records 3 committed transitions, more than the pipeline transition budget 2");
  });

  test("rejects phantom terminals and phantom run outputs when loading", () => {
    const upToTerminal = createDriver();
    playUpToTerminal(upToTerminal);
    const cursorAtTerminal = upToTerminal.current as PipelineV2RunState;

    expectInvalid(cursorAtTerminal, (draft) => {
      draft.terminal = { state_id: "elsewhere", result: "success" };
    }, 'terminal state "elsewhere" does not match the cursor');

    // a terminal while the last execution's transition was never committed
    const awaitingCommit = createDriver();
    awaitingCommit.apply(createRun());
    startAgent(awaitingCommit, "implement", { execution: "sess-1" });
    acceptOutputs(awaitingCommit, []);
    commitTransition(awaitingCommit, "implement", "completed", "check", 1);
    awaitingCommit.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    awaitingCommit.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    commitTransition(awaitingCommit, "check", "uncovered", "ship", 2);
    startAgent(awaitingCommit, "ship", { execution: "sess-2" });
    acceptOutputs(awaitingCommit, []);
    expectInvalid(awaitingCommit.current as PipelineV2RunState, (draft) => {
      draft.terminal = { state_id: "ship", result: "success" };
    }, "execution 3's transition was never committed");

    // run outputs without a reached terminal
    expectInvalid(cursorAtTerminal, (draft) => {
      draft.run_outputs = [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }];
      draft.phase = "publishing_outputs";
    }, "run_outputs exist but the terminal state was never reached");
  });

  test("run_succeeded requires published run_outputs including the empty list", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "done", 1);
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.reject({ kind: "run_succeeded" }, 'success requires phase "publishing_outputs", got "running"');
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.apply({ kind: "run_succeeded" });
    const state = driver.current as PipelineV2RunState;
    expect(state.run_outputs).toEqual([]);

    expectInvalid(state, (draft) => {
      delete draft.run_outputs;
    }, "run status success requires published run_outputs (an empty list is valid)");
  });

  test("run_cleanup_failed is reserved for a real failed session cleanup", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    driver.reject(
      { kind: "run_cleanup_failed" },
      "a cleanup failure requires the last agent execution to have failed with an unconfirmed session cleanup",
    );
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
    driver.reject(
      { kind: "run_cleanup_failed" },
      "a cleanup failure requires the last agent execution to have failed with an unconfirmed session cleanup",
    );
    driver.apply({ kind: "run_failed", reason: "worker_failed" });
  });

  test("run_cleanup_failed accepts a failed tool slot with a completed execution slot", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "completed", tool: "failed" },
    });
    driver.apply({ kind: "run_cleanup_failed" });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("cleanup_failed");
    expect(state.failure).toEqual({ reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON });
  });

  test("records the cleanup failure priority path", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    driver.reject(
      { kind: "run_failed", reason: "worker_failed" },
      "a failed session cleanup finalizes with run_cleanup_failed",
    );
    driver.reject(
      { kind: "run_failed", reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON },
      "run_failed cannot use the session cleanup failure reason",
    );
    driver.apply({ kind: "run_cleanup_failed" });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("cleanup_failed");
    expect(state.phase).toBe("finished");
    expect(state.failure).toEqual({ reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON });
    const execution = state.executions[0] as Extract<
      PipelineV2RunState["executions"][number],
      { type: "agent" }
    >;
    expect(execution.phase).toBe("failed");
    expect(execution.session_cleanup).toEqual({ execution: "failed", tool: "completed" });
    expect(execution.failure_reason).toBe(PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON);

    expectInvalid(state, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "records the session cleanup failure reason but no cleanup outcome failed");
    expectInvalid(state, (draft) => {
      draft.executions.length = 0;
    }, "requires the last agent execution to have failed with an unconfirmed session cleanup");
    expectInvalid(state, (draft) => {
      draft.terminal = { state_id: "implement", result: "failed" };
    }, "execution 1's transition was never committed");
  });

  test("rejects forbidden fields in every agent execution phase", () => {
    const driver = createDriver();
    driver.apply(createRun());
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    const started = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_data_prepared" });
    const dataPrepared = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_execution_session_created", sessionId: "sess-1" });
    const executionSessionCreated = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_tool_session_created", sessionId: "tool-1" });
    const sessionsCreated = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_running" });
    const running = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] });
    const outputsAccepted = driver.current as PipelineV2RunState;
    driver.apply({ kind: "agent_cleanup_completed" });
    const cleanupCompleted = driver.current as PipelineV2RunState;
    commitTransition(driver, "implement", "completed", "check", 1);

    // both phases before any session must not record a session
    for (const snapshot of [started, dataPrepared]) {
      expectInvalid(snapshot, (draft) => {
        draft.executions[0].execution_session_id = "sneaky";
      }, 'already records a session');
      expectInvalid(snapshot, (draft) => {
        draft.executions[0].tool_session_id = "sneaky";
      }, 'already records a session');
      expectInvalid(snapshot, (draft) => {
        draft.executions[0].outputs = [];
      }, "already records accepted outputs");
    }
    expectInvalid(started, (draft) => {
      draft.executions[0].failure_reason = "internal_error";
    }, 'records a failure reason but has phase "started"');
    expectInvalid(dataPrepared, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "already records a session cleanup outcome");
    // only the execution session exists in "execution_session_created"
    expectInvalid(executionSessionCreated, (draft) => {
      draft.executions[0].tool_session_id = "tool-1";
    }, 'already records a tool session');
    expectInvalid(executionSessionCreated, (draft) => {
      draft.executions[0].outputs = [];
    }, "already records accepted outputs");
    expectInvalid(executionSessionCreated, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "already records a session cleanup outcome");
    expectInvalid(executionSessionCreated, (draft) => {
      draft.executions[0].failure_reason = "worker_failed";
    }, 'records a failure reason but has phase "execution_session_created"');
    expectInvalid(sessionsCreated, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "already records a session cleanup outcome");
    expectInvalid(sessionsCreated, (draft) => {
      draft.executions[0].outputs = [];
    }, "already records accepted outputs");
    expectInvalid(running, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "already records a session cleanup outcome");
    expectInvalid(running, (draft) => {
      draft.executions[0].outputs = [];
    }, "already records accepted outputs");
    expectInvalid(outputsAccepted, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
    }, "already records a session cleanup outcome");
    expectInvalid(outputsAccepted, (draft) => {
      draft.executions[0].failure_reason = "internal_error";
    }, 'records a failure reason but has phase "outputs_accepted"');
    expectInvalid(outputsAccepted, (draft) => {
      delete draft.executions[0].outputs;
    }, 'has phase "outputs_accepted" but does not record accepted outputs');
    expectInvalid(cleanupCompleted, (draft) => {
      draft.executions[0].session_cleanup = { execution: "failed", tool: "completed" };
    }, 'has phase "cleanup_completed" but records session cleanup');
    expectInvalid(cleanupCompleted, (draft) => {
      draft.executions[0].failure_reason = "worker_failed";
    }, 'records a failure reason but has phase "cleanup_completed"');
    expectInvalid(cleanupCompleted, (draft) => {
      delete draft.executions[0].outputs;
    }, 'has phase "cleanup_completed" but does not record accepted outputs');
  });

  test("rejects forbidden fields on failed agent executions", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
    const failed = driver.current as PipelineV2RunState;
    expectInvalid(failed, (draft) => {
      delete draft.executions[0].failure_reason;
    }, 'has phase "failed" but no failure reason');
    expectInvalid(failed, (draft) => {
      delete draft.executions[0].session_cleanup;
    }, "failed but does not record its session cleanup outcome");
    // per-slot biconditional: a durable id exists -> completed/failed
    expectInvalid(failed, (draft) => {
      delete draft.executions[0].execution_session_id;
    }, 'records no execution session, so its cleanup outcome must be "not_required"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].execution_session_id = undefined;
      draft.executions[0].session_cleanup = { execution: "not_required", tool: "not_required" };
    }, 'records a tool session but marks its cleanup not_required');
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "not_required", tool: "completed" };
    }, 'records an execution session but marks its cleanup not_required');
    // no durable id -> only not_required
    expectInvalid(failed, (draft) => {
      draft.executions[0].execution_session_id = undefined;
      draft.executions[0].tool_session_id = undefined;
      draft.executions[0].session_cleanup = { execution: "completed", tool: "not_required" };
    }, 'records no execution session, so its cleanup outcome must be "not_required"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].tool_session_id = undefined;
    }, 'records no tool session, so its cleanup outcome must be "not_required"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "failed", tool: "failed" };
      draft.executions[0].failure_reason = "worker_failed";
    }, 'records a failed session cleanup but carries failure reason "worker_failed"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed" };
      draft.executions[0].failure_reason = PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON;
    }, "records the session cleanup failure reason but no cleanup outcome failed");
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "unknown", tool: "completed" };
    }, "session_cleanup.execution must be one of");
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed" };
    }, 'session_cleanup is missing required field "tool"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "completed", extra: 1 };
    }, 'session_cleanup has unknown field "extra"');
    expectInvalid(failed, (draft) => {
      draft.executions[0].session_cleanup = "completed";
    }, "session_cleanup is not a JSON object");
  });

  test("rejects forbidden fields in every decision execution phase", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    const evaluating = driver.current as PipelineV2RunState;
    driver.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    const evaluated = driver.current as PipelineV2RunState;

    driver.reject(
      {
        kind: "decision_evaluated",
        result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
      },
      'decision_evaluated requires decision execution phase "evaluating", got "evaluated"',
    );
    driver.apply({
      kind: "transition_committed",
      step: { from: "check", outcome: "uncovered", to: "done", transition_index: 0 },
      executionIndex: 1,
    });

    expectInvalid(evaluating, (draft) => {
      draft.executions[0].result = { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] };
    }, "is still evaluating but already records a decision result");
    expectInvalid(evaluating, (draft) => {
      draft.executions[0].failure_reason = "internal_error";
    }, "is still evaluating but already records a failure reason");
    expectInvalid(evaluated, (draft) => {
      delete draft.executions[0].result;
    }, 'has phase "evaluated" but no decision result');
    expectInvalid(evaluated, (draft) => {
      draft.executions[0].phase = "failed";
      delete draft.executions[0].result;
      draft.executions[0].failure_reason = undefined;
    }, 'has phase "failed" but no failure reason');
    expectInvalid(evaluated, (draft) => {
      draft.executions[0].failure_reason = "internal_error";
    }, 'has phase "evaluated" but records a failure reason');
    expectInvalid(evaluated, (draft) => {
      draft.executions[0].phase = "failed";
      draft.executions[0].failure_reason = "internal_error";
    }, 'has phase "failed" but records a decision result');
  });

  test("rejects duplicate session ids in the reducer and the loader", () => {
    const identity = { ...IDENTITY, max_transitions: 4 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    commitTransition(driver, "check", "uncovered", "ship", 2);
    startAgent(driver, "ship", "none");
    // a reused id is rejected for the execution command
    driver.reject(
      { kind: "agent_execution_session_created", sessionId: "sess-1" },
      'session "sess-1" already belongs to execution 1 as an execution session',
    );
    driver.apply({ kind: "agent_execution_session_created", sessionId: "sess-3" });
    // a reused id is rejected for the tool command, and the tool session of
    // the same execution cannot reuse that execution's id either
    driver.reject(
      { kind: "agent_tool_session_created", sessionId: "sess-1" },
      'session "sess-1" already belongs to execution 1 as an execution session',
    );
    driver.reject(
      { kind: "agent_tool_session_created", sessionId: "sess-3" },
      'session "sess-3" already belongs to execution 3 as an execution session',
    );
    driver.apply({ kind: "agent_tool_session_created", sessionId: "tool-3" });

    const loaderDriver = createDriver();
    playSuccessRun(loaderDriver);
    // a duplicate id across executions
    expectInvalid(loaderDriver.current as PipelineV2RunState, (draft) => {
      draft.executions[2].execution_session_id = draft.executions[0].execution_session_id;
    }, 'reuses session "sess-1"; a session id belongs to exactly one durable session slot');
    // the same execution reusing its own execution id as the tool id
    expectInvalid(loaderDriver.current as PipelineV2RunState, (draft) => {
      draft.executions[0].tool_session_id = draft.executions[0].execution_session_id;
    }, 'reuses session "sess-1"; a session id belongs to exactly one durable session slot');
  });

  test("the loader enforces the two-session phase and cleanup invariants", () => {
    // (a) forged phase: "running" with only one durable session.
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      delete draft.executions[0].tool_session_id;
    }, 'but does not record both durable sessions');
    expectInvalid(state, (draft) => {
      delete draft.executions[0].execution_session_id;
    }, 'but does not record both durable sessions');

    // (b) "execution_session_created" phase coherence in both directions.
    expectInvalid(state, (draft) => {
      draft.executions[0].phase = "execution_session_created";
      draft.executions[0].execution_session_id = undefined;
      draft.executions[0].tool_session_id = undefined;
      draft.executions[0].session_cleanup = undefined;
      draft.executions[0].outputs = undefined;
    }, 'has phase "execution_session_created" but records no execution session');
    expectInvalid(state, (draft) => {
      draft.executions[0].phase = "execution_session_created";
      draft.executions[0].session_cleanup = undefined;
      draft.executions[0].outputs = undefined;
    }, 'has phase "execution_session_created" but already records a tool session');

    // (c) forged successful cleanup with a not_required slot: the
    // cleanup_completed phase check fires first.
    expectInvalid(state, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "not_required" };
    }, 'has phase "cleanup_completed" but records session cleanup');
    expectInvalid(state, (draft) => {
      draft.executions[0].session_cleanup = { execution: "not_required", tool: "completed" };
    }, 'has phase "cleanup_completed" but records session cleanup');

    // (d) a settled failed execution without a cleanup pair.
    const identity = { ...IDENTITY, max_transitions: 1 };
    const failedDriver = createDriver(identity, []);
    failedDriver.apply(createRun(identity, []));
    startAgent(failedDriver, "implement", { execution: "sess-1" });
    failedDriver.apply({
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "completed", tool: "completed" },
    });
    failedDriver.apply({ kind: "run_failed", reason: "worker_failed" });
    const failedState = failedDriver.current as PipelineV2RunState;
    expectInvalid(failedState, (draft) => {
      delete draft.executions[0].session_cleanup;
    }, "failed but does not record its session cleanup outcome");
    // cleanup outcomes without the matching durable ids
    expectInvalid(failedState, (draft) => {
      delete draft.executions[0].tool_session_id;
    }, 'records no tool session, so its cleanup outcome must be "not_required"');
    expectInvalid(failedState, (draft) => {
      delete draft.executions[0].execution_session_id;
    }, 'records no execution session, so its cleanup outcome must be "not_required"');
    // a not_required slot beside its durable id is rejected in the failed
    // phase too
    expectInvalid(failedState, (draft) => {
      draft.executions[0].session_cleanup = { execution: "completed", tool: "not_required" };
    }, 'records a tool session but marks its cleanup not_required');
    expectInvalid(failedState, (draft) => {
      draft.executions[0].session_cleanup = { execution: "not_required", tool: "completed" };
    }, 'records an execution session but marks its cleanup not_required');

    // (e) a failed execution with no durable sessions at all is valid: both
    // slots are not_required and the failure reason stays a normal one.
    const noSessionDriver = createDriver(identity, []);
    noSessionDriver.apply(createRun(identity, []));
    noSessionDriver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    noSessionDriver.apply({ kind: "agent_data_prepared" });
    noSessionDriver.apply({
      kind: "agent_failed",
      reason: "internal_error",
      sessionCleanup: { execution: "not_required", tool: "not_required" },
    });
    noSessionDriver.apply({ kind: "run_failed", reason: "internal_error" });
    const noSessionState = noSessionDriver.current as PipelineV2RunState;
    const loaded = validatePipelineV2RunState(draftOf(noSessionState));
    const noSessionExecution = loaded.executions[0];
    if (noSessionExecution === undefined || noSessionExecution.type !== "agent") {
      throw new Error("expected an agent execution");
    }
    expect(noSessionExecution.session_cleanup).toEqual({
      execution: "not_required",
      tool: "not_required",
    });

    // (f) a successful cleanup pair with an earlier failed slot impossible:
    // cleanup_completed must record completed/completed.
    expectInvalid(state, (draft) => {
      draft.executions[0].session_cleanup = { execution: "failed", tool: "failed" };
    }, 'has phase "cleanup_completed" but records session cleanup');
  });

  test("loader re-derives the cursor and transition chain from the authoritative records", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;

    expectInvalid(state, (draft) => {
      draft.transitions[0].from = "elsewhere";
      draft.executions[0].state_id = "elsewhere";
    }, 'transition at position 0 starts at "elsewhere", expected the replay cursor "implement"');
    expectInvalid(state, (draft) => {
      draft.transitions[2].from = "check";
      draft.executions[2].state_id = "check";
    }, 'transition at position 2 starts at "check", expected the replay cursor "ship"');
    expectInvalid(state, (draft) => {
      draft.transitions[1].execution_index = 3;
    }, "transitions must reference executions in order");
    expectInvalid(state, (draft) => {
      draft.cursor.transition_count = 2;
    }, "cursor.transition_count 2 does not match 3 committed transitions");
    expectInvalid(state, (draft) => {
      draft.cursor.current_state = "ship";
    }, 'cursor.current_state "ship" does not match the replayed cursor "done"');
    expectInvalid(state, (draft) => {
      draft.transitions[0].to = "elsewhere";
    }, 'transition at position 1 starts at "check", expected the replay cursor "elsewhere"');
    expectInvalid(state, (draft) => {
      draft.transitions[1].outcome = "rejected";
    }, 'carries outcome "rejected", but its decision execution recorded outcome "approved"');
    expectInvalid(state, (draft) => {
      draft.transitions[0].result_sha256 = hex("f");
    }, 'has unknown field "result_sha256"');
    expectInvalid(state, (draft) => {
      draft.trans = [];
    }, 'has unknown field "trans"');
  });

  test("rejects attempt values other than 1 and malformed digests and ids when loading", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.executions[0].attempt = 2;
    }, "only attempt 1 is supported");
    expectInvalid(state, (draft) => {
      draft.executions[1].input_digest = "not-a-digest";
    }, "input_digest must be a lowercase hex SHA-256 digest");
    expectInvalid(state, (draft) => {
      draft.executions[0].profile = "";
    }, "profile must be a non-empty string");
    expectInvalid(state, (draft) => {
      draft.executions[0].state_id = "../escape";
    }, "state_id must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.pipeline.bundle_root = "relative/path";
    }, "bundle_root must be an absolute canonical directory path");
    expectInvalid(state, (draft) => {
      draft.pipeline.bundle_root = "/opt/../escape";
    }, "bundle_root must be an absolute canonical directory path");
    expectInvalid(state, (draft) => {
      draft.pipeline.schema_version = 1;
    }, "pipeline.schema_version must be 2, got 1");
    expectInvalid(state, (draft) => {
      draft.inputs[0].protected = "yes";
    }, "protected must be a boolean");
    expectInvalid(state, (draft) => {
      draft.inputs[1].id = "task";
    }, "more than once");
    expectInvalid(state, (draft) => {
      draft.run_outputs[0].required = true;
      draft.run_outputs[0].present = false;
      delete draft.run_outputs[0].digest;
    }, "is absent, so its required flag must be false");
    expectInvalid(state, (draft) => {
      draft.started_at = "2026-01-01";
    }, "started_at must be an ISO-8601 UTC timestamp");
    expectInvalid(state, (draft) => {
      draft.revision = 0;
    }, "revision must be a positive safe integer");
    expectInvalid(state, (draft) => {
      draft.failure = { reason: "made_up_reason" };
      draft.status = "failed";
    }, "must be one of");
  });

  test("rejects a failed run carrying the session cleanup reason or a cleanup-failed execution", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.status = "failed";
      draft.failure = { reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON };
    }, 'run status failed must not carry the session cleanup failure reason "session_cleanup_failed"');
    expectInvalid(state, (draft) => {
      draft.status = "failed";
      draft.failure = { reason: "worker_failed" };
      draft.terminal.result = "failed";
    }, 'finalizes with failure reason "terminal_failed"');

    const identity = { ...IDENTITY, max_transitions: 1 };
    const cleanupDriver = createDriver(identity, []);
    cleanupDriver.apply(createRun(identity, []));
    startAgent(cleanupDriver, "implement", { execution: "sess-1" });
    cleanupDriver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    cleanupDriver.apply({ kind: "run_cleanup_failed" });
    const cleanupFailed = cleanupDriver.current as PipelineV2RunState;
    expectInvalid(cleanupFailed, (draft) => {
      draft.status = "failed";
      draft.failure = { reason: "worker_failed" };
    }, 'execution 1 records a failed session cleanup, so the run must finalize as status "cleanup_failed"');
  });

  test("parse rejects schema versions 1, 2, 3, 4 and 5 without any migration", () => {
    expect(() => parsePipelineV2RunState('{"schema_version":1}')).toThrow(
      "pipeline v2 run state has schema_version 1, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v1 migration exists)",
    );
    expect(() => parsePipelineV2RunState('{"schema_version":2}')).toThrow(
      "pipeline v2 run state has schema_version 2, which is the production pipeline v1 run-state contract, not a pipeline v2 run state (schema version 6 is the supported contract; no v2 migration exists)",
    );
    expect(() => validatePipelineV2RunState({ schema_version: 2 })).toThrow(
      "not a pipeline v2 run state (schema version 6 is the supported contract; no v2 migration exists)",
    );
    expect(() => parsePipelineV2RunState('{"schema_version":3}')).toThrow(
      "pipeline v2 run state has schema_version 3, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v3 migration exists)",
    );
    expect(() => validatePipelineV2RunState({ schema_version: 3 })).toThrow(
      "pipeline v2 run state has schema_version 3, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v3 migration exists)",
    );
    expect(() => parsePipelineV2RunState('{"schema_version":4}')).toThrow(
      "pipeline v2 run state has schema_version 4, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v4 migration exists)",
    );
    expect(() => validatePipelineV2RunState({ schema_version: 4 })).toThrow(
      "pipeline v2 run state has schema_version 4, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v4 migration exists)",
    );
    expect(() => parsePipelineV2RunState('{"schema_version":5}')).toThrow(
      "pipeline v2 run state has schema_version 5, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v5 migration exists)",
    );
    expect(() => validatePipelineV2RunState({ schema_version: 5 })).toThrow(
      "pipeline v2 run state has schema_version 5, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v5 migration exists)",
    );
    expect(() => parsePipelineV2RunState('{"schema_version":7}')).toThrow(
      "pipeline v2 run state has schema_version 7, expected 6",
    );
  });

  test("a stored schema v3 document is rejected and left byte-for-byte identical", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-state-v3-"));
    const path = join(root, "state.json");
    // A plausible v3 document: single session_id, old phases, old cleanup.
    const v3Document = JSON.stringify({
      schema_version: 3,
      revision: 5,
      run_id: "old-run",
      status: "active",
      phase: "running",
      started_at: "2026-01-01T00:00:00.000Z",
      updated_at: "2026-01-01T00:00:00.000Z",
      pipeline: IDENTITY,
      inputs: [],
      cursor: { current_state: "implement", transition_count: 0 },
      executions: [
        {
          index: 1,
          type: "agent",
          state_id: "implement",
          attempt: 1,
          profile: "coder",
          phase: "session_created",
          session_id: "sess-1",
        },
      ],
      transitions: [],
    });
    await writeFile(path, v3Document);
    expect(() => parsePipelineV2RunState(v3Document)).toThrow(
      "pipeline v2 run state has schema_version 3, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v3 migration exists)",
    );
    expect(await readFile(path, "utf8")).toBe(v3Document);
    await rm(root, { recursive: true, force: true });
  });

  test("a stored schema v4 document is rejected and left byte-for-byte identical", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-state-v4-"));
    const path = join(root, "state.json");
    // A plausible v4 document: full v4 shape without the wait record.
    const v4Document = JSON.stringify({
      schema_version: 4,
      revision: 5,
      run_id: "old-run",
      status: "active",
      phase: "running",
      started_at: "2026-01-01T00:00:00.000Z",
      updated_at: "2026-01-01T00:00:00.000Z",
      pipeline: IDENTITY,
      inputs: [],
      cursor: { current_state: "implement", transition_count: 0 },
      executions: [],
      transitions: [],
    });
    await writeFile(path, v4Document);
    expect(() => parsePipelineV2RunState(v4Document)).toThrow(
      "pipeline v2 run state has schema_version 4, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v4 migration exists)",
    );
    expect(await readFile(path, "utf8")).toBe(v4Document);
    await rm(root, { recursive: true, force: true });
  });

  test("a stored schema v5 document is rejected and left byte-for-byte identical", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-state-v5-"));
    const path = join(root, "state.json");
    // A plausible v5 document: the supported predecessor with its single
    // optional top-level wait record and no wait journal.
    const v5Document = JSON.stringify({
      schema_version: 5,
      revision: 3,
      run_id: "old-run",
      status: "waiting",
      phase: "waiting",
      started_at: "2026-01-01T00:00:00.000Z",
      updated_at: "2026-01-01T00:00:01.000Z",
      pipeline: IDENTITY,
      inputs: [],
      cursor: { current_state: "architect", transition_count: 0 },
      executions: [],
      transitions: [],
      wait: {
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        request_sha256: hex("7"),
        actions: [{ id: "continue_stage", to: "coder" }],
      },
    });
    await writeFile(path, v5Document);
    expect(() => parsePipelineV2RunState(v5Document)).toThrow(
      "pipeline v2 run state has schema_version 5, which is unsupported by this orchestrator (schema version 6 is the supported contract; no v5 migration exists)",
    );
    expect(await readFile(path, "utf8")).toBe(v5Document);
    await rm(root, { recursive: true, force: true });
  });

  test("parse hides malformed JSON content and parser fragments", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const malformed = `{"run_id": "${canary}", "schema_version":3, "revision":`;
    let message = "";
    try {
      parsePipelineV2RunState(malformed);
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2StateError);
      message = (cause as Error).message;
    }
    expect(message).toBe("pipeline v2 run state document is not valid JSON");
    expect(message).not.toContain(canary);
    expect(message.toLowerCase()).not.toContain("position");
    expect(message.toLowerCase()).not.toContain("unexpected");
  });

  test("rejects unknown fields at every level of the document", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.extra = true;
    }, 'pipeline v2 run state has unknown field "extra"');
    expectInvalid(state, (draft) => {
      draft.pipeline.workspace = "/host/path";
    }, 'pipeline v2 run state pipeline has unknown field "workspace"');
    expectInvalid(state, (draft) => {
      draft.inputs[0].path = "TASK.md";
    }, 'pipeline v2 run state inputs[0] has unknown field "path"');
    expectInvalid(state, (draft) => {
      draft.cursor.transition_index = 1;
    }, 'pipeline v2 run state cursor has unknown field "transition_index"');
    expectInvalid(state, (draft) => {
      draft.executions[0].result_sha256 = hex("f");
    }, 'pipeline v2 run state executions[0] has unknown field "result_sha256"');
    expectInvalid(state, (draft) => {
      draft.executions[1].facts = { implement: true };
    }, 'pipeline v2 run state executions[1] has unknown field "facts"');
    expectInvalid(state, (draft) => {
      draft.transitions[0].result_sha256 = hex("f");
    }, 'pipeline v2 run state transitions[0] has unknown field "result_sha256"');
    expectInvalid(state, (draft) => {
      draft.terminal.phase = "finalizing";
    }, 'pipeline v2 run state terminal has unknown field "phase"');
    expectInvalid(state, (draft) => {
      draft.run_outputs[0].snapshot_path = "/run/outputs/plan";
    }, 'pipeline v2 run state run_outputs[0] has unknown field "snapshot_path"');
    expectInvalid(state, (draft) => {
      draft.executions[1].result.summary = "all checks passed";
    }, 'pipeline v2 run state executions[1].result has unknown field "summary"');
    expectInvalid(state, (draft) => {
      draft.failure = { reason: "internal_error", message: "boom" };
      draft.status = "failed";
    }, 'pipeline v2 run state failure has unknown field "message"');
  });

  test("the reducer never mutates its inputs and always freezes the snapshot", () => {
    const driver = createDriver();
    driver.apply(createRun());
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, [{ id: "plan", digest: hex("d") }]);
    commitTransition(driver, "implement", "completed", "check", 1);
    const before = driver.current as PipelineV2RunState;

    const writable = draftOf(before);
    const command: PipelineV2RunCommand = {
      kind: "start_decision_execution",
      stateId: "check",
      inputDigest: hex("e"),
    };
    const commandSnapshot = JSON.parse(JSON.stringify(command));
    const next = reducePipelineV2RunCommand(writable as unknown as PipelineV2RunState, command, tick(40));
    expect(next).not.toBe(writable);
    expect(JSON.parse(JSON.stringify(writable))).toEqual(draftOf(before));
    expect(JSON.parse(JSON.stringify(command))).toEqual(commandSnapshot);

    expectDeepFrozen(next);
    expectDeepFrozen(before);

    const raw = JSON.parse(JSON.stringify(next));
    const loaded = validatePipelineV2RunState(raw);
    raw.revision = 9999;
    raw.executions.length = 0;
    raw.inputs.length = 0;
    expect(loaded.revision).toBe(next.revision);
    expect(loaded.executions.length).toBe(next.executions.length);
    expect(loaded.inputs.length).toBe(next.inputs.length);
    expect(loaded).toEqual(JSON.parse(JSON.stringify(next)));
    expectDeepFrozen(loaded);
  });

  test("identical commands with an identical clock produce structurally identical states", () => {
    const first = createDriver();
    playSuccessRun(first);
    const second = createDriver();
    playSuccessRun(second);
    const a = first.current as PipelineV2RunState;
    const b = second.current as PipelineV2RunState;
    expect(a).not.toBe(b);
    expect(JSON.stringify(a)).toBe(JSON.stringify(b));
    const reloaded = parsePipelineV2RunState(JSON.stringify(a));
    expect(reloaded).toEqual(a);
    expect(reloaded.revision).toBe(23);
  });

  test("round-trips the loader through parse", () => {
    const driver = createDriver();
    playSuccessRun(driver);
    const state = driver.current as PipelineV2RunState;
    const parsed = parsePipelineV2RunState(JSON.stringify(state));
    expect(parsed).toEqual(state);
  });

  test("every accepted command in the happy path serializes into a document the loader accepts", () => {
    const driver = createDriver();
    playSuccessRun(loadableDriver(driver));
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("success");
    requireLoadableSnapshot(state);
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  });

  test("rejects create_run on an existing run and foreign commands without a run", () => {
    const driver = createDriver();
    driver.apply(createRun());
    driver.reject(createRun(), 'create_run rejected: run "run-1" already exists (revision 1)');

    const empty = createDriver();
    empty.reject({ kind: "agent_data_prepared" }, "no pipeline v2 run state exists yet");
    empty.reject(
      { kind: "start_agent_execution", stateId: "implement", profile: "coder" },
      "no pipeline v2 run state exists yet",
    );
  });

  test("rejects execution-scope commands that do not match the in-flight execution type or phase", () => {
    const identity = { ...IDENTITY, max_transitions: 4 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.reject(
      { kind: "agent_data_prepared" },
      "agent_data_prepared applies to an agent execution, but execution 2 is a decision execution",
    );
    driver.reject(
      { kind: "agent_execution_session_created", sessionId: "sess-2" },
      "agent_execution_session_created applies to an agent execution, but execution 2 is a decision execution",
    );
    driver.reject(
      { kind: "agent_outputs_accepted", outputs: [] },
      "agent_outputs_accepted applies to an agent execution, but execution 2 is a decision execution",
    );

    const agentDriver = createDriver();
    agentDriver.apply(createRun());
    agentDriver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    agentDriver.apply({ kind: "agent_data_prepared" });
    agentDriver.reject(
      { kind: "decision_evaluated", result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] } },
      "decision_evaluated applies to a decision execution, but execution 1 is an agent execution",
    );
    agentDriver.reject(
      { kind: "agent_running" },
      'starting the agent requires execution phase "sessions_created", got "data_prepared"',
    );
    driver.reject(
      { kind: "decision_failed", reason: "worker_failed" },
      "decision_failed reason must be one of",
    );
  });

  test("rejects execution failure reasons that cannot happen to that execution type", () => {
    const driver = createDriver();
    driver.apply(createRun());
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    driver.apply({ kind: "agent_data_prepared" });
    driver.reject(
      { kind: "agent_failed", reason: "decision_input_invalid", sessionCleanup: { execution: "not_required", tool: "not_required" } },
      "agent_failed reason must be one of",
    );
    driver.reject(
      { kind: "agent_failed", reason: "state_persist_failed", sessionCleanup: { execution: "not_required", tool: "not_required" } },
      "agent_failed reason must be one of",
    );
    driver.reject(
      { kind: "agent_failed", reason: "terminal_failed", sessionCleanup: { execution: "not_required", tool: "not_required" } },
      "agent_failed reason must be one of",
    );

    const identity = { ...IDENTITY, entry_state: "check", max_transitions: 1 };
    const decisionDriver = createDriver(identity, []);
    decisionDriver.apply(createRun(identity, []));
    decisionDriver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    decisionDriver.reject(
      { kind: "decision_failed", reason: "worker_failed" },
      "decision_failed reason must be one of",
    );
    decisionDriver.reject(
      { kind: "decision_failed", reason: "session_cleanup_failed" },
      "decision_failed reason must be one of",
    );
    decisionDriver.apply({ kind: "decision_failed", reason: "decision_input_invalid" });
    decisionDriver.apply({ kind: "run_failed", reason: "decision_input_invalid" });
  });

  test("the loader rejects a transition that binds a failed execution", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
    driver.apply({ kind: "run_failed", reason: "worker_failed" });
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.transitions = [
        { index: 0, from: "implement", outcome: "completed", to: "check", execution_index: 1 },
      ];
      draft.cursor = { current_state: "check", transition_count: 1 };
    }, 'transition at position 0 references execution 1 whose phase "failed" is not a cleaned agent execution');
  });

  test("the loader rejects an execution that never received its transition", () => {
    const identity = { ...IDENTITY, max_transitions: 4 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 1);
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.transitions.length = 0;
      draft.cursor = { current_state: "implement", transition_count: 0 };
    }, "execution 1 has no committed transition; a new execution starts only after the previous execution's transition is committed");
  });

  test("validates decision record shapes fail-closed", () => {
    const driver = createDriver(DECISION_ENTRY_IDENTITY, []);
    driver.apply(createRun(DECISION_ENTRY_IDENTITY, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "selected",
          outcome: "approved",
          decision: "approved",
          rule_id: "R1",
          active_constraint_ids: [],
          facts: { implement: true },
        } as unknown as PipelineDecisionStateRecord,
      },
      'decision_evaluated result has unknown field "facts"',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "uncovered",
          outcome: "approved",
          active_constraint_ids: [],
        } as unknown as PipelineDecisionStateRecord,
      },
      'outcome must be "uncovered"',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "invalid_facts",
          outcome: "invalid_facts",
          reason: "unknown_fact",
          fact_id: "implement",
        } as unknown as PipelineDecisionStateRecord,
      },
      'with reason "unknown_fact" must not carry fact_id or actual_type',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "invalid_facts",
          outcome: "invalid_facts",
          reason: "non_boolean_fact",
          fact_id: "implement",
        } as unknown as PipelineDecisionStateRecord,
      },
      'with reason "non_boolean_fact" requires the fact_id and the value\'s actual_type',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "invalid_facts",
          outcome: "invalid_facts",
          reason: "missing_fact",
        } as unknown as PipelineDecisionStateRecord,
      },
      'with reason "missing_fact" must name the model-declared fact_id',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "invalid_facts",
          outcome: "invalid_facts",
          reason: "not_mapping",
          actual_type: "string",
        } as unknown as PipelineDecisionStateRecord,
      },
      'with reason "not_mapping" must not carry fact_id or actual_type',
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: {
          status: "selected",
          outcome: "",
          decision: "approved",
          rule_id: "R1",
          active_constraint_ids: [],
        } as unknown as PipelineDecisionStateRecord,
      },
      "must be a safe non-empty identifier",
    );
    driver.reject(
      {
        kind: "decision_evaluated",
        result: { status: "rejected", outcome: "x" } as unknown as PipelineDecisionStateRecord,
      },
      ".status must be one of",
    );
    driver.apply({
      kind: "decision_evaluated",
      result: {
        status: "invalid_facts",
        outcome: "invalid_facts",
        reason: "missing_fact",
        fact_id: "implement",
      },
    });
    driver.apply({
      kind: "transition_committed",
      step: { from: "check", outcome: "invalid_facts", to: "done", transition_index: 0 },
      executionIndex: 1,
    });
  });

  test("validates run inputs and accepted output digests fail-closed", () => {
    const driver = createDriver();
    driver.reject(
      createRun(IDENTITY, [
        { id: "task", type: "file", protected: true, digest: hex("b") },
        { id: "task", type: "json", protected: false, digest: hex("c") },
      ]),
      'create_run declares run input "task" more than once',
    );
    driver.reject(
      createRun(IDENTITY, [
        { id: "task", type: "symlink" as never, protected: true, digest: hex("b") },
      ]),
      "inputs[0].type must be one of",
    );
    driver.reject(
      createRun(IDENTITY, [
        { id: "task", type: "file", protected: "yes" as never, digest: hex("b") },
      ]),
      "inputs[0].protected must be a boolean",
    );
    driver.reject(
      createRun(IDENTITY, [{ id: "task", type: "file", protected: true, digest: "zz" }]),
      "inputs[0].digest must be a lowercase hex SHA-256 digest",
    );

    driver.apply(createRun());
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    driver.apply({ kind: "agent_data_prepared" });
    driver.apply({ kind: "agent_execution_session_created", sessionId: "sess-1" });
    driver.apply({ kind: "agent_tool_session_created", sessionId: "tool-1" });
    driver.apply({ kind: "agent_running" });
    driver.reject(
      {
        kind: "agent_outputs_accepted",
        outputs: [
          { id: "plan", digest: hex("d") },
          { id: "plan", digest: hex("e") },
        ],
      },
      'agent_outputs_accepted declares output id "plan" more than once',
    );
    driver.reject(
      {
        kind: "agent_outputs_accepted",
        outputs: [{ id: "plan", digest: "not-a-digest" }],
      },
      "digest must be a lowercase hex SHA-256 digest",
    );
    driver.apply({ kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] });
    driver.apply({ kind: "agent_cleanup_completed" });

    const loaderDriver = createDriver();
    playSuccessRun(loaderDriver);
    expectInvalid(loaderDriver.current as PipelineV2RunState, (draft) => {
      draft.executions[0].outputs = [
        { id: "plan", digest: hex("d") },
        { id: "plan", digest: hex("e") },
      ];
    }, 'outputs declares output id "plan" more than once');
  });

  test("keeps accepted outputs on an execution that later fails", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({ kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] });
    driver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    driver.apply({ kind: "run_cleanup_failed" });
    const state = driver.current as PipelineV2RunState;
    const execution = state.executions[0] as Extract<
      PipelineV2RunState["executions"][number],
      { type: "agent" }
    >;
    expect(execution.phase).toBe("failed");
    expect(execution.outputs).toEqual([{ id: "plan", digest: hex("d") }]);
    expect(execution.session_cleanup).toEqual({ execution: "failed", tool: "completed" });
    const loaded = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
    expect(loaded.executions[0]).toEqual(execution);
  });

  test("the terminal and the output publication are each recorded exactly once", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "done", 1);
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.reject(
      { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
      "the terminal state is already reached and immutable",
    );
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.reject(
      { kind: "run_outputs_published", outputs: [] },
      'publishing run outputs requires phase "running", got "publishing_outputs"',
    );
    driver.apply({ kind: "run_succeeded" });
    driver.reject(
      { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
      'the run is already finalized with status "success"; the terminal run status is immutable',
    );
  });

  test("the loader rejects published run outputs on a cleanup-failed run", () => {
    const identity = { ...IDENTITY, max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    driver.apply({ kind: "run_cleanup_failed" });
    const state = driver.current as PipelineV2RunState;
    expectInvalid(state, (draft) => {
      draft.run_outputs = [];
    }, "run_outputs exist but the terminal state was never reached");
  });

  test("negative secret scan: no bearer, credential, env, config, prompt, fact, worker output, summary or host path appears in the state", () => {
    const canaries = [
      "dht_session_bearer_token",
      "dhcr_launcher_credential",
      "dhc_personal_token",
      "OPENCODE_CONFIG_CONTENT",
      "sk-live-provider-secret",
      "Please implement the feature and write result.json with status completed",
      '{"implement": true, "tests": false}',
      "worker stdout panic: exit code 3",
      "All seven acceptance criteria were verified by the agent",
      "/home/michael/work/git/opencode-docker",
      "/var/lib/orchestrator/runs/run-1",
    ];

    const successDriver = createDriver();
    playSuccessRun(successDriver);
    const success = successDriver.current as PipelineV2RunState;

    const identity = { ...IDENTITY, max_transitions: 1 };
    const failedDriver = createDriver(identity, INPUTS);
    failedDriver.apply(createRun(identity, INPUTS));
    startAgent(failedDriver, "implement", { execution: "sess-1" });
    failedDriver.apply({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
    failedDriver.apply({ kind: "run_failed", reason: "worker_failed" });
    const failed = failedDriver.current as PipelineV2RunState;

    const text = `${JSON.stringify(success)}\n${JSON.stringify(failed)}`;
    for (const canary of canaries) {
      expect(text).not.toContain(canary);
    }

    const keys = new Set<string>();
    collectKeys(success, keys);
    collectKeys(failed, keys);
    for (const banned of [
      "workspace",
      "run_root",
      "project_root",
      "snapshot_path",
      "activation_path",
      "prompt",
      "summary",
      "facts",
      "env",
      "events",
      "artifacts",
      "message",
    ]) {
      expect(keys.has(banned), `state must not carry a ${banned} field`).toBe(false);
    }
  });

  test("agent-state contract alignment: every agent execution is attempt 1 and every agent transition carries the fixed completed outcome", () => {
    const driver = loadableDriver(createDriver());
    playSuccessRun(driver);
    const state = driver.current;
    if (state === null) {
      throw new Error("expected a reduced state");
    }

    // every start_agent_execution creates exactly attempt 1: retries are
    // not implemented, so the coordinator needs no retry mapping
    let agentExecutions = 0;
    for (const execution of state.executions) {
      if (execution.type === "agent") {
        agentExecutions += 1;
        expect(execution.attempt).toBe(1);
        expect(execution.state_id).not.toBe("check");
      }
    }
    expect(agentExecutions).toBe(2);

    // the engine's fixed agent lifecycle outcome "completed" is the only
    // outcome agent executions ever transition with
    const agentOutcomes = state.transitions
      .filter((transition) => transition.outcome === "completed")
      .map((transition) => transition.from);
    expect(agentOutcomes).toEqual(["implement", "ship"]);

    // a fabricated attempt 2 is rejected by the loader: no retry mapping
    // exists in state schema v3
    expectInvalid(
      state,
      (draft) => {
        const first = draft.executions[0];
        if (first?.state_id !== "implement") {
          throw new Error("expected the first agent execution");
        }
        first.attempt = 2;
      },
      "only attempt 1 is supported",
    );
  });
});

describe("pipeline v2 run state loader: the unbound execution is bound to the replayed cursor", () => {
  /**
   * Pre-fix regression note: on commit bf72ccd (before this invariant) the
   * loader accepted every forged document below — the negative cases
   * validate() without throwing — because nothing tied the one execution
   * without a committed transition to the replayed cursor. The positive
   * cases passed unchanged. The negatives here fail only because of the
   * unbound-execution/cursor invariant in `validatePipelineV2RunState`.
   */

  const WAIT_IDENTITY: PipelineV2RunPipelineIdentity = {
    ...IDENTITY,
    entry_state: "architect",
    max_transitions: 6,
  };

  function runWaiting(): PipelineV2RunCommand {
    return {
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: hex("7"),
      actions: [
        { id: "continue_stage", to: "coder" },
        { id: "revise_task", to: "architect" },
      ],
    };
  }

  function waitResponded(): PipelineV2RunCommand {
    return {
      kind: "wait_response_recorded",
      waitIndex: 1,
      expectedRequestSha256: hex("7"),
      actionId: "continue_stage",
      responseSha256: hex("8"),
    };
  }

  /** The cursor's unbound execution: the last execution, no transition of its own. */
  function forgeUnboundStateId(
    state: PipelineV2RunState,
    stateId: string,
  ): { draft: any; snapshot: string } {
    const draft = draftOf(state);
    const unbound = draft.executions[draft.executions.length - 1];
    if (unbound === undefined) {
      throw new Error("expected an unbound execution");
    }
    if (draft.transitions.length !== draft.executions.length - 1) {
      throw new Error("expected the last execution to be the only unbound one");
    }
    unbound.state_id = stateId;
    return { draft, snapshot: JSON.stringify(draft) };
  }

  test("the response target execution round-trips: agent start at the chosen action target (positive in-flight)", () => {
    const identity = { ...WAIT_IDENTITY, max_transitions: 6 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("active");
    expect(state.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    expect(state.executions[0]).toMatchObject({ index: 1, state_id: "coder", phase: "started" });
    // every accepted command already serialized into a loader-accepted document
    const parsed = parsePipelineV2RunState(JSON.stringify(state));
    expect(parsed).toEqual(state);
  });

  test("a forged unbound agent state id is rejected against the replayed response target", () => {
    const identity = { ...WAIT_IDENTITY, max_transitions: 6 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
    const state = driver.current as PipelineV2RunState;
    const { draft, snapshot } = forgeUnboundStateId(state, "elsewhere");
    let message = "";
    try {
      validatePipelineV2RunState(draft);
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2StateError);
      message = (cause as Error).message;
    }
    // the diagnostic names only the execution index and safe state ids
    expect(message).toBe(
      'execution 1 ran state "elsewhere", expected the replayed cursor "coder"',
    );
    // the validator never mutates the document it rejects
    expect(JSON.stringify(draft)).toBe(snapshot);
  });

  test("a forged unbound agent state id is rejected without any wait (entry cursor)", () => {
    const driver = createDriver();
    driver.apply(createRun());
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    const state = driver.current as PipelineV2RunState;
    const { draft } = forgeUnboundStateId(state, "elsewhere");
    let message = "";
    try {
      validatePipelineV2RunState(draft);
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2StateError);
      message = (cause as Error).message;
    }
    expect(message).toBe(
      'execution 1 ran state "elsewhere", expected the replayed cursor "implement"',
    );
  });

  test("a forged unbound decision state id is rejected the same way", () => {
    const identity = { ...IDENTITY, entry_state: "check", max_transitions: 1 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    const state = driver.current as PipelineV2RunState;
    const { draft } = forgeUnboundStateId(state, "elsewhere");
    let message = "";
    try {
      validatePipelineV2RunState(draft);
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2StateError);
      message = (cause as Error).message;
    }
    expect(message).toBe(
      'execution 1 ran state "elsewhere", expected the replayed cursor "check"',
    );
  });

  test("a forged unbound failed execution state id is rejected", () => {
    const driver = createDriver();
    driver.apply(createRun());
    startAgent(driver, "implement", { execution: "sess-1" });
    driver.apply({
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "completed", tool: "completed" },
    });
    const state = driver.current as PipelineV2RunState;
    expect(state.executions[0]).toMatchObject({ index: 1, phase: "failed" });
    const { draft } = forgeUnboundStateId(state, "elsewhere");
    let message = "";
    try {
      validatePipelineV2RunState(draft);
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2StateError);
      message = (cause as Error).message;
    }
    expect(message).toBe(
      'execution 1 ran state "elsewhere", expected the replayed cursor "implement"',
    );
  });

  test("in-flight, settled-but-unbound and failed executions with the exact cursor state round-trip", () => {
    // in-flight
    const inFlight = createDriver();
    inFlight.apply(createRun());
    inFlight.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    requireLoadableSnapshot(inFlight.current as PipelineV2RunState);

    // settled but unbound (cleanup recorded, transition not committed yet)
    const settled = createDriver();
    settled.apply(createRun());
    startAgent(settled, "implement", { execution: "sess-1" });
    acceptOutputs(settled, [{ id: "plan", digest: hex("d") }]);
    const settledState = settled.current as PipelineV2RunState;
    expect(settledState.executions[0]).toMatchObject({ index: 1, phase: "cleanup_completed" });
    expect(settledState.transitions).toHaveLength(0);
    requireLoadableSnapshot(settledState);

    // failed
    const failed = createDriver();
    failed.apply(createRun());
    startAgent(failed, "implement", { execution: "sess-1" });
    failed.apply({
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "completed", tool: "completed" },
    });
    requireLoadableSnapshot(failed.current as PipelineV2RunState);
  });
});

describe("pipeline v2 run state schema v6: durable user wait and response", () => {
  /** The default-pipeline policy graph around the P01 wait: architect/coder. */
  const WAIT_IDENTITY: PipelineV2RunPipelineIdentity = {
    ...IDENTITY,
    entry_state: "architect",
    max_transitions: 6,
  };

  /** P01 wait actions in oracle order: continue_stage routes to coder, revise_task to architect. */
  const ORACLE_WAIT_ACTIONS = [
    { id: "continue_stage", to: "coder" },
    { id: "revise_task", to: "architect" },
  ];

  function runWaiting(
    overrides: Partial<{ stateId: string; reason: string; requestSha256: string; actions: { id: string; to: string }[] }> = {},
  ): PipelineV2RunCommand {
    return {
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: hex("7"),
      actions: [...ORACLE_WAIT_ACTIONS],
      ...overrides,
    };
  }

  function waitResponded(
    overrides: Partial<{
      waitIndex: number;
      expectedRequestSha256: string;
      actionId: string;
      responseSha256: string;
    }> = {},
  ): PipelineV2RunCommand {
    return {
      kind: "wait_response_recorded",
      waitIndex: 1,
      expectedRequestSha256: hex("7"),
      actionId: "continue_stage",
      responseSha256: hex("8"),
      ...overrides,
    };
  }

  /** Produces a valid waiting state: entry-state wait, zero executions, zero transitions. */
  function produceWaitingState(): PipelineV2RunState {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    return driver.current as PipelineV2RunState;
  }

  /** Produces a valid responded state: the entry wait answered at the coder target. */
  function produceRespondedState(): PipelineV2RunState {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    return driver.current as PipelineV2RunState;
  }

  test("run_waiting: entry-state wait without executions and transitions (P01-S01..S04 as durable wait entry)", () => {
    const driver = loadableDriver(createDriver(WAIT_IDENTITY, []));
    driver.apply(createRun(WAIT_IDENTITY, []));
    const state = driver.apply(runWaiting());
    expect(state.status).toBe("waiting");
    expect(state.phase).toBe("waiting");
    expect(state.revision).toBe(2);
    expect(state.waits).toEqual([
      {
        index: 1,
        transition_count: 0,
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        request_sha256: hex("7"),
        actions: ORACLE_WAIT_ACTIONS,
      },
    ]);
    // the wait record is content-free: exactly these six fields, no response yet
    expect(Object.keys(state.waits[0] as object)).toEqual([
      "index",
      "transition_count",
      "state_id",
      "reason",
      "request_sha256",
      "actions",
    ]);
    expect(state.cursor).toEqual({ current_state: "architect", transition_count: 0 });
    expect(state.executions).toEqual([]);
    expect(state.transitions).toEqual([]);
    expect(state.terminal).toBeUndefined();
    expect(state.failure).toBeUndefined();
    expect(state.run_outputs).toBeUndefined();
    expect(state.started_at).toBe(tick(0).toISOString());
    expect(state.updated_at).toBe(tick(1).toISOString());
    expectDeepFrozen(state);
  });

  test("wait_response_recorded: entry-cursor response returns the run to active at the action target", () => {
    const driver = loadableDriver(createDriver(WAIT_IDENTITY, []));
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    const state = driver.apply(waitResponded());
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.revision).toBe(3);
    expect(state.waits).toHaveLength(1);
    expect(state.waits[0]).toEqual({
      index: 1,
      transition_count: 0,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      request_sha256: hex("7"),
      actions: ORACLE_WAIT_ACTIONS,
      response: { action_id: "continue_stage", response_sha256: hex("8") },
    });
    // the response is content-free: exactly these two fields
    expect(Object.keys(state.waits[0]?.response as object)).toEqual(["action_id", "response_sha256"]);
    // the cursor moves to the declared action target; the transition count,
    // the budget and the history stay untouched
    expect(state.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    expect(state.executions).toEqual([]);
    expect(state.transitions).toEqual([]);
    expect(state.started_at).toBe(tick(0).toISOString());
    expect(state.updated_at).toBe(tick(2).toISOString());
    expectDeepFrozen(state);
  });

  test("run_waiting: wait after a decision transition commits, at the new cursor", () => {
    const identity = { ...IDENTITY, entry_state: "check", max_transitions: 2 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.apply({
      kind: "decision_evaluated",
      result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
    });
    commitTransition(driver, "check", "uncovered", "architect", 1);
    const state = driver.apply(runWaiting());
    expect(state.status).toBe("waiting");
    expect(state.phase).toBe("waiting");
    expect(state.waits[0]?.state_id).toBe("architect");
    expect(state.waits[0]?.transition_count).toBe(1);
    expect(state.waits[0]?.index).toBe(1);
    expect(state.cursor).toEqual({ current_state: "architect", transition_count: 1 });
    expect(state.executions.map((execution) => `${execution.index}:${execution.state_id}`)).toEqual([
      "1:check",
    ]);
    expect(state.transitions).toEqual([
      { index: 0, from: "check", outcome: "uncovered", to: "architect", execution_index: 1 },
    ]);
  });

  test("wait_response_recorded after a committed transition: history untouched, execution restarts at the target with the next global index", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "architect", 1);
    driver.apply(runWaiting());
    const state = driver.apply(waitResponded());
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.waits[0]?.transition_count).toBe(1);
    expect(state.cursor).toEqual({ current_state: "coder", transition_count: 1 });
    // the prior history is unchanged
    expect(state.executions).toHaveLength(1);
    expect(state.executions[0]).toMatchObject({ index: 1, state_id: "implement", phase: "cleanup_completed" });
    expect(state.transitions).toEqual([
      { index: 0, from: "implement", outcome: "completed", to: "architect", execution_index: 1 },
    ]);
    // the next execution starts at the chosen target with the next global index
    startAgent(driver, "coder", { execution: "sess-2" });
    const resumed = driver.current as PipelineV2RunState;
    expect(resumed.executions).toHaveLength(2);
    expect(resumed.executions[1]).toMatchObject({ index: 2, state_id: "coder", phase: "running" });
    // a graph transition after the response must start at the chosen target
    acceptOutputs(driver, []);
    commitTransition(driver, "coder", "completed", "done", 2);
    const finished = driver.current as PipelineV2RunState;
    expect(finished.transitions[1]).toMatchObject({ from: "coder", to: "done", execution_index: 2 });
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  });

  test("two wait/response cycles at one clean boundary keep the prior history unchanged", () => {
    const driver = loadableDriver(createDriver(WAIT_IDENTITY, []));
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    // second wait at the coder target, still zero committed transitions
    driver.apply(
      runWaiting({
        stateId: "coder",
        requestSha256: hex("9"),
        actions: [{ id: "continue", to: "architect" }],
      }),
    );
    const waitingAgain = driver.current as PipelineV2RunState;
    expect(waitingAgain.status).toBe("waiting");
    expect(waitingAgain.waits).toHaveLength(2);
    expect(waitingAgain.waits[0]).toMatchObject({ index: 1, transition_count: 0, response: { action_id: "continue_stage" } });
    expect(waitingAgain.waits[1]).toMatchObject({ index: 2, transition_count: 0, state_id: "coder" });
    expect(waitingAgain.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    expect(waitingAgain.executions).toEqual([]);
    expect(waitingAgain.transitions).toEqual([]);
    const state = driver.apply(
      waitResponded({
        waitIndex: 2,
        expectedRequestSha256: hex("9"),
        actionId: "continue",
        responseSha256: hex("a"),
      }),
    );
    expect(state.status).toBe("active");
    expect(state.cursor).toEqual({ current_state: "architect", transition_count: 0 });
    expect(state.waits[1]?.response).toEqual({ action_id: "continue", response_sha256: hex("a") });
    // the first cycle is untouched
    expect(state.waits[0]).toMatchObject({ index: 1, response: { action_id: "continue_stage", response_sha256: hex("8") } });
  });

  test("sequential waits after one committed transition share the same transition_count", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "architect", 1);
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply(
      runWaiting({
        stateId: "coder",
        requestSha256: hex("9"),
        actions: [{ id: "back", to: "implement" }],
      }),
    );
    const state = driver.apply(
      waitResponded({
        waitIndex: 2,
        expectedRequestSha256: hex("9"),
        actionId: "back",
        responseSha256: hex("b"),
      }),
    );
    expect(state.waits.map((wait) => `${wait.index}@t${wait.transition_count}`)).toEqual(["1@t1", "2@t1"]);
    expect(state.cursor).toEqual({ current_state: "implement", transition_count: 1 });
    // the next graph transition must start at the final response target
    startAgent(driver, "implement", { execution: "sess-2" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "check", 2);
    const final = driver.current as PipelineV2RunState;
    expect(final.transitions[1]).toMatchObject({ from: "implement", to: "check" });
  });

  test("the loader replays transitions and waits jointly after every accepted command", () => {
    const driver = loadableDriver(createDriver(WAIT_IDENTITY, []));
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply(
      runWaiting({
        stateId: "coder",
        requestSha256: hex("9"),
        actions: [{ id: "continue", to: "architect" }],
      }),
    );
    driver.apply(
      waitResponded({
        waitIndex: 2,
        expectedRequestSha256: hex("9"),
        actionId: "continue",
        responseSha256: hex("a"),
      }),
    );
    startAgent(driver, "architect", { execution: "sess-1" });
    acceptOutputs(driver, [{ id: "plan", digest: hex("d") }]);
    commitTransition(driver, "architect", "completed", "coder", 1);
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("active");
    expect(state.cursor).toEqual({ current_state: "coder", transition_count: 1 });
    expect(state.waits).toHaveLength(2);
    const parsed = parsePipelineV2RunState(JSON.stringify(state));
    expect(parsed).toEqual(state);
    expectDeepFrozen(parsed);
  });

  test("wait_response_recorded rejects a stale wait index, a stale request digest, an unknown action and a repeated response", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    driver.reject(
      waitResponded({ waitIndex: 2 }),
      "wait_response_recorded targets wait index 2, but the open wait record is 1",
    );
    driver.reject(
      waitResponded({ expectedRequestSha256: hex("9") }),
      "the expected request digest does not match open wait record 1",
    );
    driver.reject(
      waitResponded({ actionId: "sneak" }),
      'action "sneak" is not declared in wait record 1',
    );
    const state = driver.apply(waitResponded());
    expect(state.status).toBe("active");
    // a repeated response targets a closed record
    driver.reject(waitResponded(), "recording a wait response requires a waiting run; the run is active with no open wait record");
    driver.reject(
      waitResponded({ waitIndex: 1 }),
      "recording a wait response requires a waiting run; the run is active with no open wait record",
    );
    const unchanged = driver.current as PipelineV2RunState;
    expect(unchanged.revision).toBe(3);
    expect(unchanged.waits[0]?.response).toEqual({ action_id: "continue_stage", response_sha256: hex("8") });
  });

  test("wait_response_recorded rejects malformed digests, ids and exact fields before any state check", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    driver.reject(
      waitResponded({ waitIndex: 0 }),
      "wait_response_recorded wait index must be a positive safe integer",
    );
    driver.reject(
      waitResponded({ waitIndex: -1 }),
      "wait_response_recorded wait index must be a positive safe integer",
    );
    driver.reject(
      waitResponded({ waitIndex: 1.5 }),
      "wait_response_recorded wait index must be a positive safe integer",
    );
    driver.reject(
      waitResponded({ expectedRequestSha256: "abc" }),
      "wait_response_recorded expected request digest must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      waitResponded({ responseSha256: hex("A") }),
      "wait_response_recorded response digest must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      waitResponded({ actionId: "../bad" }),
      "wait_response_recorded action id must be a safe non-empty identifier",
    );
  });

  test("wait_response_recorded is rejected on an active run without a wait and on finalized runs", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.reject(
      waitResponded(),
      "recording a wait response requires a waiting run; the run is active with no open wait record",
    );
    const successDriver = createDriver();
    playSuccessRun(successDriver);
    successDriver.reject(
      waitResponded(),
      'the run is already finalized with status "success"; the terminal run status is immutable',
    );
  });

  test("several wait actions may target the same state, in declaration order", () => {
    const driver = loadableDriver(createDriver(WAIT_IDENTITY, []));
    driver.apply(createRun(WAIT_IDENTITY, []));
    const state = driver.apply(
      runWaiting({
        actions: [
          { id: "continue_stage", to: "coder" },
          { id: "continue_stage_stronger", to: "coder" },
          { id: "revise_task", to: "architect" },
        ],
      }),
    );
    expect(state.waits[0]?.actions).toEqual([
      { id: "continue_stage", to: "coder" },
      { id: "continue_stage_stronger", to: "coder" },
      { id: "revise_task", to: "architect" },
    ]);
  });

  test("run_waiting rejects zero actions and duplicate action ids", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.reject(runWaiting({ actions: [] }), "run_waiting requires at least one action");
    driver.reject(
      runWaiting({ actions: "continue_stage" as never }),
      "run_waiting requires an actions array",
    );
    driver.reject(
      runWaiting({
        actions: [
          { id: "continue_stage", to: "coder" },
          { id: "continue_stage", to: "architect" },
        ],
      }),
      'run_waiting declares action id "continue_stage" more than once',
    );
  });

  test("run_waiting rejects unsafe state ids, reasons, action ids, targets and unknown action fields", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.reject(runWaiting({ stateId: "../bad" }), "run_waiting state id must be a safe non-empty identifier");
    driver.reject(runWaiting({ stateId: "a..b" }), "run_waiting state id must be a safe non-empty identifier");
    driver.reject(runWaiting({ reason: "" }), "run_waiting reason must be a safe non-empty identifier");
    driver.reject(
      runWaiting({ reason: "Stage Limit!" }),
      "run_waiting reason must be a safe non-empty identifier",
    );
    driver.reject(
      runWaiting({ actions: [{ id: "../bad", to: "coder" }] }),
      "run_waiting actions[0].id must be a safe non-empty identifier",
    );
    driver.reject(
      runWaiting({ actions: [{ id: "continue_stage", to: "/coder" }] }),
      "run_waiting actions[0].to must be a safe non-empty identifier",
    );
    driver.reject(
      runWaiting({ actions: [{ id: "continue_stage", to: "coder", extra: 1 } as never] }),
      'run_waiting actions[0] has unknown field "extra"',
    );
    driver.reject(
      runWaiting({ actions: ["continue_stage" as never] }),
      "run_waiting actions[0] is not a JSON object",
    );
  });

  test("run_waiting rejects malformed request digests", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.reject(
      runWaiting({ requestSha256: hex("A") }),
      "run_waiting request_sha256 must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      runWaiting({ requestSha256: "abc" }),
      "run_waiting request_sha256 must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      runWaiting({ requestSha256: "a".repeat(65) }),
      "run_waiting request_sha256 must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      runWaiting({ requestSha256: "z".repeat(64) }),
      "run_waiting request_sha256 must be a lowercase hex SHA-256 digest",
    );
    driver.reject(
      runWaiting({ requestSha256: 123 as never }),
      "run_waiting request_sha256 must be a lowercase hex SHA-256 digest",
    );
  });

  test("run_waiting rejects an unfinished agent execution in every in-flight phase", () => {
    const driver = createDriver();
    driver.apply(createRun());
    driver.apply({ kind: "start_agent_execution", stateId: "implement", profile: "coder" });
    driver.reject(
      runWaiting({ stateId: "implement" }),
      'entering the wait requires execution 1 to be finished, it has phase "started"',
    );
    driver.apply({ kind: "agent_data_prepared" });
    driver.reject(
      runWaiting({ stateId: "implement" }),
      'entering the wait requires execution 1 to be finished, it has phase "data_prepared"',
    );
    driver.apply({ kind: "agent_execution_session_created", sessionId: "sess-1" });
    driver.reject(
      runWaiting({ stateId: "implement" }),
      'entering the wait requires execution 1 to be finished, it has phase "execution_session_created"',
    );
    driver.apply({ kind: "agent_tool_session_created", sessionId: "tool-1" });
    driver.reject(
      runWaiting({ stateId: "implement" }),
      'entering the wait requires execution 1 to be finished, it has phase "sessions_created"',
    );
    driver.apply({ kind: "agent_running" });
    driver.reject(
      runWaiting({ stateId: "implement" }),
      'entering the wait requires execution 1 to be finished, it has phase "running"',
    );
  });

  test("run_waiting rejects an evaluating decision execution", () => {
    const identity = { ...IDENTITY, entry_state: "check", max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    driver.apply({ kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
    driver.reject(
      runWaiting({ stateId: "check" }),
      'entering the wait requires execution 1 to be finished, it has phase "evaluating"',
    );
  });

  test("run_waiting rejects a settled execution whose transition was never committed", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    driver.reject(
      runWaiting({ stateId: "implement" }),
      "entering the wait requires every execution's transition to be committed",
    );
  });

  test("run_waiting rejects a state id that does not match the cursor", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.reject(
      runWaiting({ stateId: "coder" }),
      'wait state "coder" does not match the cursor "architect"',
    );
  });

  test("run_waiting is rejected after the terminal, after publication and after finalization", () => {
    const driver = createDriver();
    playUpToTerminal(driver);
    driver.apply({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    driver.reject(
      runWaiting({ stateId: "done" }),
      "the terminal state is already reached; a waiting run cannot be recorded",
    );
    driver.apply({ kind: "run_outputs_published", outputs: [] });
    driver.reject(
      runWaiting({ stateId: "done" }),
      'entering the wait requires phase "running", got "publishing_outputs"',
    );
    driver.apply({ kind: "run_succeeded" });
    driver.reject(
      runWaiting({ stateId: "done" }),
      'the run is already finalized with status "success"; the terminal run status is immutable',
    );

    const failedDriver = createDriver();
    failedDriver.apply(createRun());
    startAgent(failedDriver, "implement", { execution: "sess-1" });
    failedDriver.apply({
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "completed", tool: "completed" },
    });
    failedDriver.apply({ kind: "run_failed", reason: "worker_failed" });
    failedDriver.reject(
      runWaiting({ stateId: "implement" }),
      'the run is already finalized with status "failed"; the terminal run status is immutable',
    );
  });

  test("while waiting only wait_response_recorded advances the run; every other command is rejected", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    const waitingMessage =
      "the run is waiting for an explicit user response; only wait_response_recorded advances a waiting run";
    driver.reject(runWaiting(), waitingMessage);
    driver.reject({ kind: "start_agent_execution", stateId: "architect", profile: "coder" }, waitingMessage);
    driver.reject({ kind: "start_decision_execution", stateId: "architect", inputDigest: hex("e") }, waitingMessage);
    driver.reject({ kind: "agent_data_prepared" }, waitingMessage);
    driver.reject(
      {
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "coder", transition_index: 0 },
        executionIndex: 1,
      },
      waitingMessage,
    );
    driver.reject({ kind: "terminal_reached", terminalStateId: "architect", terminalResult: "success" }, waitingMessage);
    driver.reject({ kind: "run_outputs_published", outputs: [] }, waitingMessage);
    driver.reject({ kind: "run_succeeded" }, waitingMessage);
    driver.reject({ kind: "run_failed", reason: "worker_failed" }, waitingMessage);
    driver.reject({ kind: "run_cleanup_failed" }, waitingMessage);
    // create_run has its own dedicated rejection path
    driver.reject(createRun(WAIT_IDENTITY, []), 'create_run rejected: run "run-1" already exists (revision 2)');
    const state = driver.current as PipelineV2RunState;
    expect(state.status).toBe("waiting");
    expect(state.revision).toBe(2);
    // the one response command advances the waiting run
    const responded = driver.apply(waitResponded());
    expect(responded.status).toBe("active");
    expect(responded.revision).toBe(3);
  });

  test("the reducer never mutates the wait command or the input state; later command mutation cannot change the record", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    const before = driver.current as PipelineV2RunState;

    const actions: { id: string; to: string }[] = [
      { id: "continue_stage", to: "coder" },
      { id: "revise_task", to: "architect" },
    ];
    const command = runWaiting({ actions });
    const commandSnapshot = JSON.parse(JSON.stringify(command));
    const next = reducePipelineV2RunCommand(before, command, tick(40));
    expect(JSON.parse(JSON.stringify(command))).toEqual(commandSnapshot);
    expect(JSON.parse(JSON.stringify(before))).toEqual(draftOf(before));

    // mutating the caller's command array afterwards cannot change the record
    actions.push({ id: "sneak", to: "coder" });
    actions[0]!.id = "mutated";
    expect(next.waits[0]).toEqual({
      index: 1,
      transition_count: 0,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      request_sha256: hex("7"),
      actions: [
        { id: "continue_stage", to: "coder" },
        { id: "revise_task", to: "architect" },
      ],
    });
    expectDeepFrozen(next);
    expectDeepFrozen(before);
  });

  test("the response command never mutates its inputs and always freezes the snapshot", () => {
    const driver = createDriver(WAIT_IDENTITY, []);
    driver.apply(createRun(WAIT_IDENTITY, []));
    driver.apply(runWaiting());
    const before = driver.current as PipelineV2RunState;
    const command = waitResponded();
    const commandSnapshot = JSON.parse(JSON.stringify(command));
    const next = reducePipelineV2RunCommand(before, command, tick(40));
    expect(JSON.parse(JSON.stringify(command))).toEqual(commandSnapshot);
    expect(JSON.parse(JSON.stringify(before))).toEqual(draftOf(before));
    expect(next.waits[0]?.response).toEqual({ action_id: "continue_stage", response_sha256: hex("8") });
    expect(next.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    expectDeepFrozen(next);
    expectDeepFrozen(before);
  });

  test("the loader enforces the status/phase/open-wait biconditional in both directions", () => {
    const state = produceWaitingState();
    expectInvalid(state, (draft) => {
      draft.status = "active";
      draft.phase = "waiting";
    }, 'run phase "waiting" requires run status "waiting"');
    expectInvalid(state, (draft) => {
      draft.status = "active";
      draft.phase = "running";
    }, 'an open wait record requires run status "waiting"');
    expectInvalid(state, (draft) => {
      draft.phase = "running";
    }, 'run status "waiting" requires phase "waiting"');
    expectInvalid(state, (draft) => {
      draft.waits[0].response = { action_id: "continue_stage", response_sha256: hex("8") };
    }, 'run status "waiting" requires an open wait record as the last wait record');
    expectInvalid(state, (draft) => {
      draft.waits = [];
    }, 'run status "waiting" requires an open wait record as the last wait record');
    expectInvalid(state, (draft) => {
      delete draft.waits;
    }, 'is missing required field "waits"');

    // every non-waiting status forbids an open wait record
    const successDriver = createDriver();
    playSuccessRun(successDriver);
    expectInvalid(successDriver.current as PipelineV2RunState, (draft) => {
      draft.waits = [
        {
          index: 1,
          transition_count: 3,
          state_id: "done",
          reason: "stage_iteration_limit_exhausted",
          request_sha256: hex("7"),
          actions: ORACLE_WAIT_ACTIONS,
        },
      ];
    }, 'an open wait record requires run status "waiting"');
    expectInvalid(successDriver.current as PipelineV2RunState, (draft) => {
      draft.waits = [
        {
          index: 1,
          transition_count: 3,
          state_id: "elsewhere",
          reason: "stage_iteration_limit_exhausted",
          request_sha256: hex("7"),
          actions: ORACLE_WAIT_ACTIONS,
          response: { action_id: "continue_stage", response_sha256: hex("8") },
        },
      ];
    }, 'wait record 1 names state "elsewhere", which does not match the replay cursor "done"');

    const identity = { ...IDENTITY, max_transitions: 1 };
    const failedDriver = createDriver(identity, []);
    failedDriver.apply(createRun(identity, []));
    startAgent(failedDriver, "implement", { execution: "sess-1" });
    failedDriver.apply({
      kind: "agent_failed",
      reason: "worker_failed",
      sessionCleanup: { execution: "completed", tool: "completed" },
    });
    failedDriver.apply({ kind: "run_failed", reason: "worker_failed" });
    expectInvalid(failedDriver.current as PipelineV2RunState, (draft) => {
      draft.waits = [
        {
          index: 1,
          transition_count: 0,
          state_id: "implement",
          reason: "stage_iteration_limit_exhausted",
          request_sha256: hex("7"),
          actions: ORACLE_WAIT_ACTIONS,
        },
      ];
    }, 'an open wait record requires run status "waiting"');

    const cleanupDriver = createDriver(identity, []);
    cleanupDriver.apply(createRun(identity, []));
    startAgent(cleanupDriver, "implement", { execution: "sess-1" });
    cleanupDriver.apply({
      kind: "agent_failed",
      reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    cleanupDriver.apply({ kind: "run_cleanup_failed" });
    expectInvalid(cleanupDriver.current as PipelineV2RunState, (draft) => {
      draft.waits = [
        {
          index: 1,
          transition_count: 0,
          state_id: "implement",
          reason: "stage_iteration_limit_exhausted",
          request_sha256: hex("7"),
          actions: ORACLE_WAIT_ACTIONS,
        },
      ];
    }, 'an open wait record requires run status "waiting"');

    // an active responded state with a re-opened wait record is rejected too
    const responded = produceRespondedState();
    expectInvalid(responded, (draft) => {
      delete draft.waits[0].response;
    }, 'an open wait record requires run status "waiting"');
  });

  test("a waiting run must not carry a terminal, run outputs or a failure reason", () => {
    const state = produceWaitingState();
    expectInvalid(state, (draft) => {
      draft.terminal = { state_id: "architect", result: "success" };
    }, "a waiting run must not carry a reached terminal state");
    // run outputs without a terminal hit the general coherence check first
    expectInvalid(state, (draft) => {
      draft.run_outputs = [];
    }, "run_outputs exist but the terminal state was never reached");
    // terminal + run outputs: the waiting terminal guard fires first
    expectInvalid(state, (draft) => {
      draft.terminal = { state_id: "architect", result: "success" };
      draft.run_outputs = [];
    }, "a waiting run must not carry a reached terminal state");
    expectInvalid(state, (draft) => {
      draft.failure = { reason: "worker_failed" };
    }, "a waiting run must not carry a failure reason");
  });

  test("the wait record must name the replay cursor state", () => {
    const state = produceWaitingState();
    expectInvalid(state, (draft) => {
      draft.waits[0].state_id = "coder";
    }, 'wait record 1 names state "coder", which does not match the replay cursor "architect"');
  });

  test("the loader validates the wait record fail-closed", () => {
    const state = produceWaitingState();
    expectInvalid(state, (draft) => {
      draft.waits[0].state_id = "../bad";
    }, "waits[0].state_id must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.waits[0].reason = "Stage Limit!";
    }, "waits[0].reason must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.waits[0].reason = "";
    }, "waits[0].reason must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.waits[0].request_sha256 = hex("A");
    }, "waits[0].request_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(state, (draft) => {
      draft.waits[0].request_sha256 = "abc";
    }, "waits[0].request_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(state, (draft) => {
      draft.waits[0].request_sha256 = "a".repeat(65);
    }, "waits[0].request_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(state, (draft) => {
      draft.waits[0].request_sha256 = "z".repeat(64);
    }, "waits[0].request_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(state, (draft) => {
      draft.waits[0].actions = [];
    }, "waits[0].actions must not be empty");
    expectInvalid(state, (draft) => {
      draft.waits[0].actions = "continue_stage";
    }, "waits[0].actions must be an array");
    expectInvalid(state, (draft) => {
      draft.waits[0].actions = [
        { id: "continue_stage", to: "coder" },
        { id: "continue_stage", to: "architect" },
      ];
    }, 'waits[0] declares action id "continue_stage" more than once');
    expectInvalid(state, (draft) => {
      draft.waits[0].actions[0].id = "../bad";
    }, "waits[0].actions[0].id must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.waits[0].actions[1].to = "/architect";
    }, "waits[0].actions[1].to must be a safe non-empty identifier");
    expectInvalid(state, (draft) => {
      draft.waits[0].actions[0].target = "coder";
    }, 'waits[0].actions[0] has unknown field "target"');
    expectInvalid(state, (draft) => {
      draft.waits[0].evidence = "acceptance test 7 fails";
    }, 'waits[0] has unknown field "evidence"');
    expectInvalid(state, (draft) => {
      draft.waits[0].manifest = { intent: "continue_stage" };
    }, 'waits[0] has unknown field "manifest"');
    expectInvalid(state, (draft) => {
      draft.waits[0].index = 0;
    }, "waits[0].index must be a positive safe integer");
    expectInvalid(state, (draft) => {
      draft.waits[0].transition_count = -1;
    }, "waits[0].transition_count must be a non-negative safe integer");
    expectInvalid(state, (draft) => {
      draft.waits[0].transition_count = 1.5;
    }, "waits[0].transition_count must be a non-negative safe integer");
    expectInvalid(state, (draft) => {
      draft.waits[0] = { state_id: "architect", reason: "stage_iteration_limit_exhausted", request_sha256: hex("7") };
    }, 'waits[0] is missing required field "index"');
    expectInvalid(state, (draft) => {
      draft.waits[0] = {
        index: 1,
        transition_count: 0,
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        request_sha256: hex("7"),
      };
    }, 'waits[0] is missing required field "actions"');
    expectInvalid(state, (draft) => {
      draft.waits[0] = "waiting";
    }, "waits[0] is not a JSON object");
    // the response record is validated fail-closed as well
    const responded = produceRespondedState();
    expectInvalid(responded, (draft) => {
      draft.waits[0].response.action_id = "../bad";
    }, "waits[0].response.action_id must be a safe non-empty identifier");
    expectInvalid(responded, (draft) => {
      draft.waits[0].response.response_sha256 = hex("A");
    }, "waits[0].response.response_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(responded, (draft) => {
      draft.waits[0].response.response_sha256 = "abc";
    }, "waits[0].response.response_sha256 must be a lowercase hex SHA-256 digest");
    expectInvalid(responded, (draft) => {
      draft.waits[0].response.body = "user answer text";
    }, 'waits[0].response has unknown field "body"');
    expectInvalid(responded, (draft) => {
      draft.waits[0].response = { action_id: "continue_stage" };
    }, 'waits[0].response is missing required field "response_sha256"');
    expectInvalid(responded, (draft) => {
      draft.waits[0].response = "continue_stage";
    }, "waits[0].response is not a JSON object");
  });

  test("the loader rejects a waiting run with an unfinished or unbound execution", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = createDriver(identity, []);
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "architect", 1);
    driver.apply(runWaiting());
    const state = driver.current as PipelineV2RunState;

    // an in-flight execution under a waiting run is rejected by the
    // open-wait coherence rule (no execution may follow an open wait)
    expectInvalid(state, (draft) => {
      draft.transitions = [];
      draft.cursor = { current_state: "implement", transition_count: 0 };
      draft.waits[0].transition_count = 0;
      draft.waits[0].state_id = "implement";
    }, "wait record 1 is open while execution 1 has no committed transition; an open wait record requires a fully settled and committed history");
    // a settled but unbound execution is rejected by the same open-wait
    // coherence rule
    expectInvalid(state, (draft) => {
      draft.executions.push({
        index: 2,
        type: "agent",
        state_id: "architect",
        attempt: 1,
        profile: "coder",
        phase: "cleanup_completed",
        execution_session_id: "sess-2",
        tool_session_id: "tool-2",
        session_cleanup: { execution: "completed", tool: "completed" },
        outputs: [],
      });
      draft.waits[0].state_id = "architect";
    }, "wait record 1 is open while execution 2 has no committed transition; an open wait record requires a fully settled and committed history");
  });

  test("the loader rejects wait journal gaps, duplicates and out-of-range transition counts", () => {
    const state = produceWaitingState();
    expectInvalid(state, (draft) => {
      draft.waits[0].index = 2;
    }, "wait record at position 0 declares index 2; wait record indexes must be contiguous from 1");
    expectInvalid(state, (draft) => {
      draft.waits[0].index = 5;
    }, "wait record at position 0 declares index 5; wait record indexes must be contiguous from 1");
    expectInvalid(state, (draft) => {
      draft.waits[0].transition_count = 1;
    }, "wait record 1 declares transition_count 1, which exceeds the 0 committed transitions");

    // a decreasing journal across two boundaries
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "architect", 1);
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply(
      runWaiting({
        stateId: "coder",
        requestSha256: hex("9"),
        actions: [{ id: "back", to: "implement" }],
      }),
    );
    driver.apply(
      waitResponded({
        waitIndex: 2,
        expectedRequestSha256: hex("9"),
        actionId: "back",
        responseSha256: hex("b"),
      }),
    );
    const twoCycle = driver.current as PipelineV2RunState;
    expectInvalid(twoCycle, (draft) => {
      draft.waits[1].transition_count = 0;
    }, "wait record 2 declares transition_count 0, below the replay boundary 1 already consumed by earlier wait records; wait records must be ordered by transition_count");
    expectInvalid(twoCycle, (draft) => {
      draft.waits[1].transition_count = 2;
    }, "wait record 2 declares transition_count 2, which exceeds the 1 committed transitions");
  });

  test("the loader rejects duplicate wait indexes and an open wait record that is not the last", () => {
    const identity = { ...IDENTITY, max_transitions: 2 };
    const driver = loadableDriver(createDriver(identity, []));
    driver.apply(createRun(identity, []));
    startAgent(driver, "implement", { execution: "sess-1" });
    acceptOutputs(driver, []);
    commitTransition(driver, "implement", "completed", "architect", 1);
    driver.apply(runWaiting());
    driver.apply(waitResponded());
    driver.apply(
      runWaiting({
        stateId: "coder",
        requestSha256: hex("9"),
        actions: [{ id: "back", to: "implement" }],
      }),
    );
    const twoWait = driver.current as PipelineV2RunState;

    // duplicate journal index
    expectInvalid(twoWait, (draft) => {
      draft.waits[1].index = 1;
    }, "wait record at position 1 declares index 1; wait record indexes must be contiguous from 1");
    // a gap in the journal index
    expectInvalid(twoWait, (draft) => {
      draft.waits[1].index = 3;
    }, "wait record at position 1 declares index 3; wait record indexes must be contiguous from 1");

    // an open record before a later record
    const responded = produceRespondedState();
    expectInvalid(responded, (draft) => {
      draft.status = "waiting";
      draft.phase = "waiting";
      delete draft.waits[0].response;
      draft.waits.push({
        index: 2,
        transition_count: 0,
        state_id: "coder",
        reason: "stage_iteration_limit_exhausted",
        request_sha256: hex("9"),
        actions: [{ id: "continue", to: "architect" }],
      });
      draft.cursor = { current_state: "coder", transition_count: 0 };
    }, "wait record 1 is open but is not the last wait record; an open wait record may only end the wait journal");

    // committed transitions after an open record
    const waiting = produceWaitingState();
    expectInvalid(waiting, (draft) => {
      draft.transitions.push({
        index: 0,
        from: "architect",
        outcome: "completed",
        to: "coder",
        execution_index: 1,
      });
      draft.executions.push({
        index: 1,
        type: "agent",
        state_id: "architect",
        attempt: 1,
        profile: "coder",
        phase: "cleanup_completed",
        execution_session_id: "sess-1",
        tool_session_id: "tool-1",
        session_cleanup: { execution: "completed", tool: "completed" },
        outputs: [],
      });
      draft.cursor = { current_state: "architect", transition_count: 1 };
    }, "wait record 1 is open but 1 committed transitions follow it; no transition may follow an open wait record");
  });

  test("the loader rejects a response for an undeclared action and a cursor that ignores the response target", () => {
    const responded = produceRespondedState();
    expectInvalid(responded, (draft) => {
      draft.waits[0].response.action_id = "sneak";
    }, 'wait record 1 records response action "sneak", which it does not declare');
    expectInvalid(responded, (draft) => {
      draft.cursor.current_state = "architect";
    }, 'cursor.current_state "architect" does not match the replayed cursor "coder"');
    // a forged persisted cursor must not bypass the replayed action target
    expectInvalid(responded, (draft) => {
      draft.cursor.current_state = "implement";
    }, 'cursor.current_state "implement" does not match the replayed cursor "coder"');
  });

  test("a loaded waiting state is deep-frozen and round-trips through parse", () => {
    const state = produceWaitingState();
    const parsed = parsePipelineV2RunState(JSON.stringify(state));
    expect(parsed).toEqual(state);
    expectDeepFrozen(parsed);
  });

  test("negative secret scan of the waiting run: no manifest body, evidence, task/plan bodies, profile bindings, paths or credentials", () => {
    const canaries = [
      '{"intent":"continue_stage","additional_iterations":2}',
      "unmet acceptance criteria: acceptance test 7 fails",
      "reviewer_alternative",
      "coder_stronger",
      "TASK.md revision 2: rewrite the parser",
      "PLAN: stage 3 of 5",
      "stage_iteration=4_of_4",
      "/var/lib/orchestrator/runs/run-1/wait-request.json",
      "dht_session_bearer_token",
      "OPENCODE_CONFIG_CONTENT",
    ];
    const state = produceWaitingState();
    const text = JSON.stringify(state);
    for (const canary of canaries) {
      expect(text).not.toContain(canary);
    }
    const keys = new Set<string>();
    collectKeys(state, keys);
    for (const banned of [
      "evidence",
      "manifest",
      "request",
      "user_response",
      "task",
      "plan",
      "profile_bindings",
      "endpoint",
      "token",
    ]) {
      expect(keys.has(banned), `state must not carry a ${banned} field`).toBe(false);
    }
    // the response record is content-free: only action_id and response_sha256
    const responded = produceRespondedState();
    const respondedKeys = new Set<string>();
    collectKeys(responded, respondedKeys);
    expect(respondedKeys.has("response")).toBe(true);
    expect(respondedKeys.has("user_response")).toBe(false);
    expect(respondedKeys.has("body")).toBe(false);
    const respondedText = JSON.stringify(responded);
    for (const canary of ["user answer text", '{"intent":"revise_task"}', "TASK.md"]) {
      expect(respondedText).not.toContain(canary);
    }
  });
});
