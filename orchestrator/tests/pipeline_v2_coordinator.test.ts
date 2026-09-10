import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  coordinatePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2AgentSession,
  type PipelineV2CoordinationResult,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2WorkerRunResult,
} from "../src/pipeline_v2_coordinator.ts";
import {
  acceptedOutputDigest,
  evaluatePreparedDecisionState,
  prepareDecisionStateData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
} from "../src/pipeline_v2_runtime.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  PipelineV2RunStateStoreError,
} from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import type {
  PipelineV2AgentExecutionState,
  PipelineV2DecisionExecutionState,
  PipelineV2FailureReason,
  PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import type { PipelineStateIo } from "../src/run_snapshot_store.ts";
import { countingIo, faultIo, type IoCounts } from "./state_io_test_helpers.ts";

/**
 * Tests for the production-neutral pipeline v2 coordinator: one
 * orchestration flow over `executePipelineV2Graph`, the v2 data plane, the
 * decision evaluator, the execution digest, the durable state schema v3,
 * the run state sink and the typed runtime failures — with an injected
 * fake agent runtime and a real durable sink. Everything is deterministic:
 * no sleeps, no LLM, no Docker Helper, no launcher credential. The
 * production loader keeps rejecting schema v2; nothing here wires
 * `agent-smoke`, the CLI, Docker Helper or real Sessions.
 */

const LOOSE_SCHEMA = {};

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations:
  - id: r1
    assert:
      not:
        all:
          - {fact: f1, equals: true}
          - {fact: f2, equals: true}
constraints:
  - id: c1
    when: {fact: f1, equals: true}
    forbid: [beta]
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const DECISION_TRANSITIONS = `      - outcome: alpha
        to: done
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

/** agent coder -> decision check -> done/failed_end; one json agent output. */
const PIPELINE_AGENT_DECISION = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json
  - id: source
    type: file
    protected: true

outputs:
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: report
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${DECISION_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** Two agent states in a row; the second one has zero ports. */
const PIPELINE_TWO_AGENTS = `
schema_version: 2
entry_state: first
max_transitions: 20

inputs:
  - id: source
    type: file
    protected: true

outputs:
  - id: report
    required: true
    source:
      state_output:
        state: first
        output: report

states:
  - id: first
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: report
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: second
  - id: second
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

/** Entry success terminal: zero executions, outputs from a pipeline input. */
const PIPELINE_ENTRY_SUCCESS = `
schema_version: 2
entry_state: done
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs:
  - id: report
    required: true
    source:
      pipeline_input: facts_seed

states:
  - id: done
    type: terminal
    result: success
`;

/** Entry failed terminal: zero executions, outputs still published. */
const PIPELINE_ENTRY_FAILED = `
schema_version: 2
entry_state: failed_end
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs:
  - id: report
    required: true
    source:
      pipeline_input: facts_seed

states:
  - id: failed_end
    type: terminal
    result: failed
`;

/** One agent with zero output ports and no run outputs. */
const PIPELINE_ZERO_PORTS = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []
outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

/** Bounded cycle: coder -> coder2 -> coder with a transition budget of 2. */
const PIPELINE_CYCLE_BUDGET = `
schema_version: 2
entry_state: coder
max_transitions: 2

inputs: []
outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder2
  - id: coder2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder
  - id: done
    type: terminal
    result: success
`;

/** The decision input is fed by an accepted agent output (json, loose). */
const PIPELINE_DECISION_FROM_OUTPUT = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    transitions:
${DECISION_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

// --- tests -----------------------------------------------------------------

test("1. agent -> decision -> success terminal records the exact durable command order", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION);
  const fake = fakeRuntime([{}]);
  const result = await coordinate(harness, fake.runtime);
  const state = expectOk(result);

  expect(state.status).toBe("success");
  expect(state.phase).toBe("finished");
  expect(kinds(harness.recording)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "start_decision_execution",
    "decision_evaluated",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
  const commands = harness.recording.commands;
  const created = commands[0];
  expect(created?.kind).toBe("create_run");
  expect(created?.runId).toBe("coord-run");
  const createdPipeline = created?.pipeline as Record<string, unknown> | undefined;
  expect(createdPipeline?.schema_version).toBe(2);
  expect(createdPipeline?.bundle_root).toBe(harness.pipeline.bundleRoot);
  expect(isHexDigest(createdPipeline?.execution_snapshot_sha256)).toBe(true);
  expect(createdPipeline?.entry_state).toBe("coder");
  expect(createdPipeline?.max_transitions).toBe(20);
  const inputs = created?.inputs as Array<Record<string, unknown>> | undefined;
  expect((inputs ?? []).map((input) => [input.id, input.type, input.protected])).toEqual([
    ["facts_seed", "json", false],
    ["source", "file", true],
  ]);
  expect((inputs ?? []).every((input) => isHexDigest(input.digest))).toBe(true);
  const started = commands[1];
  expect(started?.stateId).toBe("coder");
  expect(started?.profile).toBe("coder");
  expect(commands[3]?.sessionId).toBe("session-1");
  expect(commands[7]).toEqual({
    kind: "transition_committed",
    step: { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
    executionIndex: 1,
  });
  const decisionStarted = commands[8];
  expect(decisionStarted?.stateId).toBe("check");
  expect(isHexDigest(decisionStarted?.inputDigest)).toBe(true);
  expect(commands[10]).toEqual({
    kind: "transition_committed",
    step: { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
    executionIndex: 2,
  });
  expect(commands[11]).toEqual({
    kind: "terminal_reached",
    terminalStateId: "done",
    terminalResult: "success",
  });
  const publishedOutputs = commands[12]?.outputs as Array<Record<string, unknown>> | undefined;
  expect(publishedOutputs).toHaveLength(1);
  expect(publishedOutputs?.[0]?.id).toBe("report");
  expect(publishedOutputs?.[0]?.present).toBe(true);
  expect(isHexDigest(publishedOutputs?.[0]?.digest)).toBe(true);

  expect(state.started_at).toMatch(ISO_TIMESTAMP);
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  expect(state.executions).toHaveLength(2);
  const agentExecution = agentAt(state, 0);
  const decisionExecution = decisionAt(state, 1);
  expect(agentExecution.index).toBe(1);
  expect(agentExecution.type).toBe("agent");
  expect(agentExecution.state_id).toBe("coder");
  expect(agentExecution.attempt).toBe(1);
  expect(agentExecution.profile).toBe("coder");
  expect(agentExecution.phase).toBe("cleanup_completed");
  expect(agentExecution.session_id).toBe("session-1");
  expect(agentExecution.session_cleanup).toBe("completed");
  const agentOutputs = agentExecution.outputs ?? [];
  expect(agentOutputs).toHaveLength(1);
  expect(agentOutputs[0]?.id).toBe("report");
  expect(isHexDigest(agentOutputs[0]?.digest)).toBe(true);
  expect(decisionExecution.index).toBe(2);
  expect(decisionExecution.type).toBe("decision");
  expect(decisionExecution.state_id).toBe("check");
  expect(decisionExecution.phase).toBe("evaluated");
  expect(decisionExecution.input_digest).toMatch(SHA256_HEX);
  expect(decisionExecution.result).toEqual({
    status: "selected",
    outcome: "alpha",
    decision: "alpha",
    rule_id: "rule-a",
    active_constraint_ids: ["c1"],
  });
  expect(state.transitions).toEqual([
    { index: 0, from: "coder", outcome: "completed", to: "check", execution_index: 1 },
    { index: 0, from: "check", outcome: "alpha", to: "done", execution_index: 2 },
  ]);
  expect(state.failure).toBeUndefined();

  expect(fake.createCalls).toEqual([{ stateId: "coder", activationIndex: 1 }]);
  expect(fake.sessions[0]?.runCount).toBe(1);
  expect(fake.sessions[0]?.cleanupCount).toBe(1);
});

test("2. two agent states run two sessions, each deleted exactly once, in order", async () => {
  const harness = await setupHarness(PIPELINE_TWO_AGENTS);
  const fake = fakeRuntime([{}, {}]);
  const result = await coordinate(harness, fake.runtime);
  const state = expectOk(result);

  expect(fake.createCalls).toEqual([
    { stateId: "first", activationIndex: 1 },
    { stateId: "second", activationIndex: 2 },
  ]);
  expect(fake.sessions.map((session) => session.runCount)).toEqual([1, 1]);
  expect(fake.sessions.map((session) => session.cleanupCount)).toEqual([1, 1]);

  const commandKinds = kinds(harness.recording);
  const firstSessionIndex = commandKinds.indexOf("agent_session_created");
  const firstCleanupIndex = commandKinds.indexOf("agent_cleanup_completed");
  const firstTransitionIndex = commandKinds.indexOf("transition_committed");
  const secondStartIndex = commandKinds.indexOf("start_agent_execution", firstSessionIndex + 1);
  expect(firstCleanupIndex).toBeGreaterThan(firstSessionIndex);
  expect(firstTransitionIndex).toBe(firstCleanupIndex + 1);
  expect(secondStartIndex).toBeGreaterThan(firstTransitionIndex);

  expect(state.executions.map((execution) => execution.state_id)).toEqual(["first", "second"]);
  expect(state.transitions).toEqual([
    { index: 0, from: "first", outcome: "completed", to: "second", execution_index: 1 },
    { index: 0, from: "second", outcome: "completed", to: "done", execution_index: 2 },
  ]);
});

test("3. entry success terminal publishes outputs with zero executions and sessions", async () => {
  const harness = await setupHarness(PIPELINE_ENTRY_SUCCESS);
  const fake = fakeRuntime([]);
  const result = await coordinate(harness, fake.runtime);
  const state = expectOk(result);

  expect(state.status).toBe("success");
  expect(state.executions).toEqual([]);
  expect(state.transitions).toEqual([]);
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  const published = state.run_outputs ?? [];
  expect(published).toHaveLength(1);
  expect(published[0]?.present).toBe(true);
  expect(published[0]?.id).toBe("report");
  expect(fake.sessions).toHaveLength(0);
  expect(kinds(harness.recording)).toEqual([
    "create_run",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("4. failed terminal publishes outputs, reports ok:false with the terminal failure, and finalizes durably", async () => {
  const harness = await setupHarness(PIPELINE_ENTRY_FAILED);
  const fake = fakeRuntime([]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("terminal_failed");
  const state = result.state;
  expect(state.status).toBe("failed");
  expect(state.phase).toBe("finished");
  expect(state.failure).toEqual({ reason: "terminal_failed" });
  expect(state.terminal).toEqual({ state_id: "failed_end", result: "failed" });
  const published = state.run_outputs ?? [];
  expect(published).toHaveLength(1);
  expect(published[0]?.present).toBe(true);
  expect(kinds(harness.recording)).toEqual([
    "create_run",
    "terminal_reached",
    "run_outputs_published",
    "run_failed",
  ]);
  expect(harness.recording.commands[3]?.reason).toBe("terminal_failed");
  // no second finalize after the committed run_failed{terminal_failed}
  expect(kinds(harness.recording).filter((kind) => kind === "run_failed")).toHaveLength(1);
});

test("5. an agent with zero output ports accepts an empty output list", async () => {
  const harness = await setupHarness(PIPELINE_ZERO_PORTS);
  const fake = fakeRuntime([{}]);
  const result = await coordinate(harness, fake.runtime);
  const state = expectOk(result);

  expect(state.status).toBe("success");
  const agentExecution = agentAt(state, 0);
  expect(agentExecution.phase).toBe("cleanup_completed");
  expect(agentExecution.outputs).toEqual([]);
  const accepted = harness.recording.commands.find((command) => command.kind === "agent_outputs_accepted");
  expect(accepted?.outputs).toEqual([]);
  expect(state.run_outputs).toEqual([]);
});

test("6. typed worker failures fail the execution and the run with cleanup exactly once", async () => {
  for (const reason of ["worker_failed", "worker_timeout"] as const) {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ run: reason }]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe(reason);
    expect(result.state.status).toBe("failed");
    expect(result.state.failure).toEqual({ reason });
    const agentExecution = agentAt(result.state, 0);
    expect(agentExecution.phase).toBe("failed");
    expect(agentExecution.failure_reason).toBe(reason);
    expect(agentExecution.session_cleanup).toBe("completed");
    expect(result.state.transitions).toEqual([]);
    expect(result.state.terminal).toBeUndefined();
    expect(kinds(harness.recording)).not.toContain("run_cleanup_failed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
  }
});

test("7. a failure before the Session records sessionCleanup not_required", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION);
  await mkdir(join(harness.dirs.runRoot, "activations", "1-coder"), { recursive: true });
  const fake = fakeRuntime([]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("activation_prepare_failed");
  expect(result.state.failure).toEqual({ reason: "activation_prepare_failed" });
  const agentExecution = agentAt(result.state, 0);
  expect(agentExecution.phase).toBe("failed");
  expect(agentExecution.failure_reason).toBe("activation_prepare_failed");
  expect(agentExecution.session_cleanup).toBe("not_required");
  expect(agentExecution.session_id).toBeUndefined();
  expect(fake.sessions).toHaveLength(0);
});

test("8. a failure after Session creation still cleans the session up exactly once", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
    faults: new Map([
      [
        "agent_running",
        () => {
          throw new PipelineV2RunStateStoreError("injected store failure at agent_running");
        },
      ],
    ]),
  });
  const fake = fakeRuntime([{}]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("state_persist_failed");
  expect(result.state.failure).toEqual({ reason: "state_persist_failed" });
  const agentExecution = agentAt(result.state, 0);
  expect(agentExecution.phase).toBe("failed");
  expect(agentExecution.failure_reason).toBe("internal_error");
  expect(agentExecution.session_cleanup).toBe("completed");
  expect(fake.sessions[0]?.runCount).toBe(0);
  expect(fake.sessions[0]?.cleanupCount).toBe(1);
  expect(result.state.transitions).toEqual([]);
});

test("9. a cleanup failure finalizes as cleanup_failed without any transition", async () => {
  const harness = await setupHarness(PIPELINE_TWO_AGENTS);
  const fake = fakeRuntime([
    { run: "completed", cleanup: "throw" },
    {},
  ]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("session_cleanup_failed");
  expect(result.state.status).toBe("cleanup_failed");
  expect(result.state.failure).toEqual({ reason: "session_cleanup_failed" });
  expect(agentAt(result.state, 0).phase).toBe("failed");
  expect(agentAt(result.state, 0).failure_reason).toBe("session_cleanup_failed");
  expect(agentAt(result.state, 0).session_cleanup).toBe("failed");
  expect(result.state.transitions).toEqual([]);
  expect(result.state.terminal).toBeUndefined();
  expect(kinds(harness.recording)).toContain("run_cleanup_failed");
  expect(kinds(harness.recording)).not.toContain("run_failed");
  expect(fake.createCalls).toEqual([{ stateId: "first", activationIndex: 1 }]);
  expect(fake.sessions[0]?.cleanupCount).toBe(1);
});

test("10. decision results produce exact content-free records and route through the engine", async () => {
  const cases: Array<{
    facts: string;
    expectedRecord: Record<string, unknown>;
    finalStatus: string;
    terminal: [string, string];
    finalOutcome: string;
  }> = [
    {
      facts: JSON.stringify({ f1: true, f2: false }),
      expectedRecord: {
        status: "selected",
        outcome: "alpha",
        decision: "alpha",
        rule_id: "rule-a",
        active_constraint_ids: ["c1"],
      },
      finalStatus: "success",
      terminal: ["done", "success"],
      finalOutcome: "alpha",
    },
    {
      facts: JSON.stringify({ f1: false, f2: false }),
      expectedRecord: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
      finalStatus: "failed",
      terminal: ["failed_end", "failed"],
      finalOutcome: "uncovered",
    },
    {
      facts: JSON.stringify({ f1: true, f2: true }),
      expectedRecord: {
        status: "inconsistent_facts",
        outcome: "inconsistent_facts",
        violated_relation_ids: ["r1"],
      },
      finalStatus: "failed",
      terminal: ["failed_end", "failed"],
      finalOutcome: "inconsistent_facts",
    },
    {
      facts: JSON.stringify({ f1: "banana", f2: false }),
      expectedRecord: {
        status: "invalid_facts",
        outcome: "invalid_facts",
        reason: "non_boolean_fact",
        fact_id: "f1",
        actual_type: "string",
      },
      finalStatus: "failed",
      terminal: ["failed_end", "failed"],
      finalOutcome: "invalid_facts",
    },
  ];
  for (const testCase of cases) {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, { facts: testCase.facts });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    let state: PipelineV2RunState;
    if (testCase.finalStatus === "success") {
      state = expectOk(result);
    } else {
      // a failed terminal is a durable failed run, never ok:true
      expect(result.ok).toBe(false);
      if (result.ok || result.state === null) {
        throw new Error("unexpected result shape");
      }
      expect(result.reason).toBe("terminal_failed");
      expect(result.state.status).toBe("failed");
      expect(result.state.failure).toEqual({ reason: "terminal_failed" });
      state = result.state;
    }
    const decisionExecution = decisionAt(state, 1);
    expect(decisionExecution.phase).toBe("evaluated");
    expect(decisionExecution.result as unknown).toEqual(testCase.expectedRecord);
    expect<string>(state.status).toBe(testCase.finalStatus);
    expect(state.terminal?.state_id).toBe(testCase.terminal[0]);
    expect(state.terminal?.result === "success" || state.terminal?.result === "failed").toBe(true);
    expect(state.transitions).toHaveLength(2);
    const finalTransition = state.transitions[1];
    expect(finalTransition?.outcome).toBe(testCase.finalOutcome);
    // the failed terminal published the outputs too
    expect((state.run_outputs ?? []).every((entry) => entry.present)).toBe(true);
  }
});

test("11. decision input failures stay typed after start_decision_execution", async () => {
  // (a) adapter level: the evaluation phase parses the saved bytes only; a
  // malformed saved value fails decision_input_invalid after preparation.
  const dirs = await makeDirs();
  await writeBundle(dirs, PIPELINE_DECISION_FROM_OUTPUT);
  const pipeline = await loadPipelineV2(dirs.bundle);
  const runInputs = await snapshotRunInputs(pipeline, bindingsFor(dirs, pipeline), dirs.runRoot);
  const activationDir = join(dirs.runRoot, "activations", "1-coder", "data", "outputs");
  await mkdir(activationDir, { recursive: true });
  const factsPath = join(activationDir, "facts");
  await writeFile(factsPath, "{not-json");
  const digest = await acceptedOutputDigest("json", factsPath, "decision facts");
  const accepted: AcceptedStateOutput[] = [
    { state: "coder", output: "facts", activation_index: 1, digest },
  ];
  const prepared = await prepareDecisionStateData(pipeline, runInputs, accepted, "check", 2);
  expect(prepared.state_id).toBe("check");
  expect(prepared.execution_index).toBe(2);
  expect(prepared.input_digest).toMatch(SHA256_HEX);
  expect(Object.keys(prepared).sort()).toEqual(["execution_index", "input_digest", "state_id"]);
  let evaluationFailure: unknown;
  try {
    evaluatePreparedDecisionState(pipeline, prepared);
  } catch (cause) {
    evaluationFailure = cause;
  }
  const message = String((evaluationFailure as Error | undefined)?.message ?? "");
  expect(message).toContain("is not valid JSON");
  expect(message).not.toContain("not-json");
  expect(message).not.toContain("{not-json");

  // (b) coordinator level: a state-store failure at decision_evaluated
  // records decision_failed and no transition.
  const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
    faults: new Map([
      [
        "decision_evaluated",
        () => new PipelineV2RunStateStoreError("injected store failure at decision_evaluated"),
      ],
    ]),
  });
  const fake = fakeRuntime([{}]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("state_persist_failed");
  const decisionExecution = result.state.executions[1];
  expect(decisionExecution?.type).toBe("decision");
  expect(decisionExecution?.phase).toBe("failed");
  expect(decisionExecution?.failure_reason).toBe("internal_error");
  expect(result.state.transitions).toHaveLength(1);
  expect(result.state.terminal).toBeUndefined();
  expect(result.state.status).toBe("failed");
});

test("12. transition budget exhaustion fails before the next callback", async () => {
  const harness = await setupHarness(PIPELINE_CYCLE_BUDGET);
  const fake = fakeRuntime([{}, {}, {}]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("transition_budget_exhausted");
  expect(result.state.status).toBe("failed");
  expect(result.state.failure).toEqual({ reason: "transition_budget_exhausted" });
  expect(fake.sessions).toHaveLength(2);
  expect(fake.sessions.every((session) => session.cleanupCount === 1)).toBe(true);
  expect(result.state.executions).toHaveLength(2);
  expect(result.state.transitions).toHaveLength(2);
  expect(result.state.terminal).toBeUndefined();
});

test("13. a transition-hook failure stops the graph before the next callback", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
    faults: new Map([
      [
        "transition_committed",
        () => new PipelineV2RunStateStoreError("injected store failure at the transition commit"),
      ],
    ]),
  });
  const fake = fakeRuntime([{}, {}]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("state_persist_failed");
  expect(result.state.status).toBe("failed");
  expect(result.state.transitions).toEqual([]);
  expect(result.state.terminal).toBeUndefined();
  expect(result.state.executions).toHaveLength(1);
  expect(agentAt(result.state, 0).phase).toBe("cleanup_completed");
  expect(fake.sessions).toHaveLength(1);
});

test("14. typed data-plane failures keep their exact reason", async () => {
  // (a) the snapshot of a protected input is modified during the run: the
  // next activation preparation fails with run_input_modified.
  const harness = await setupHarness(PIPELINE_TWO_AGENTS);
  const fake = fakeRuntime([
    {
      onRun: async () => {
        await writeFile(join(harness.dirs.runRoot, "data", "inputs", "source"), "TAMPERED-SOURCE");
      },
    },
    {},
  ]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("run_input_modified");
  expect(agentAt(result.state, 0).failure_reason).toBeUndefined();
  expect(agentAt(result.state, 1).failure_reason).toBe("run_input_modified");
  expect(agentAt(result.state, 1).session_cleanup).toBe("not_required");
  expect(fake.sessions).toHaveLength(1);

  // (b) an accepted output modified after acceptance: the next
  // preparation fails with accepted_output_modified.
  const harness2 = await setupHarness(PIPELINE_TWO_AGENTS);
  const fake2 = fakeRuntime([
    {
      onCleanup: async (session) => {
        const reportPort = session.activation.output_ports[0];
        if (reportPort === undefined) {
          throw new Error("the first agent has no output port");
        }
        await writeFile(reportPort.path, "TAMPERED-REPORT");
      },
    },
    {},
  ]);
  const result2 = await coordinate(harness2, fake2.runtime);
  expect(result2.ok).toBe(false);
  if (result2.ok || result2.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result2.reason).toBe("accepted_output_modified");
  expect(result2.state.executions[1]?.failure_reason).toBe("accepted_output_modified");
  expect(fake2.sessions).toHaveLength(1);
});

test("15. plain unexpected errors normalize to internal_error", async () => {
  // (a) the runtime contract explodes before any Session exists.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const explodingRuntime = {
      createSession: async () => {
        throw new Error("RUNTIME-EXPLODED");
      },
    } as unknown as PipelineV2AgentRuntime;
    const result = await coordinate(harness, explodingRuntime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("internal_error");
    expect(agentAt(result.state, 0).failure_reason).toBe("internal_error");
    expect(agentAt(result.state, 0).session_cleanup).toBe("not_required");
    expect(JSON.stringify(result)).not.toContain("RUNTIME-EXPLODED");
  }

  // (b) the worker throws while running: cleanup still happens once.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ run: "throw" }]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("internal_error");
    expect(agentAt(result.state, 0).failure_reason).toBe("internal_error");
    expect(agentAt(result.state, 0).session_cleanup).toBe("completed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(JSON.stringify(result)).not.toContain("WORKER-EXPLODED");
  }

  // (c) an invalid worker result shape is an internal error too.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ run: "invalid" }]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("internal_error");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(agentAt(result.state, 0).session_cleanup).toBe("completed");
  }
});

test("16. pre-rename state failures and post-rename durability unknown behave exactly", async () => {
  // (a) pre-rename failure at agent_running: not committed; the failure is
  // recorded durably afterwards and the run finalizes normally.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      io: faultIo({ failCommit: 5, failStep: "write" }),
    });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state.status).toBe("failed");
    expect(result.state.failure).toEqual({ reason: "state_persist_failed" });
    expect(agentAt(result.state, 0).phase).toBe("failed");
    expect(agentAt(result.state, 0).session_cleanup).toBe("completed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
  }

  // (b) durability-unknown at agent_running: the sink adopts the visible
  // candidate, all further writes stop, and the run reports
  // state_persist_failed with the adopted snapshot.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      io: faultIo({ failCommit: 5, failStep: "dirfsync" }),
    });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(agentAt(result.state, 0).phase).toBe("running");
    expect(result.state.executions).toHaveLength(1);
    expect(result.state.transitions).toEqual([]);
    expect(result.state.failure).toBeUndefined();
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(harness.sink.poisoned).toBe(true);
  }

  // (c) durability-unknown at create_run: the visible candidate is the
  // returned state.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_SUCCESS, {
      io: faultIo({ failCommit: 1, failStep: "dirsync" }),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state).not.toBeNull();
    expect(result.state?.revision).toBe(1);
    expect(harness.sink.poisoned).toBe(true);
  }

  // (d) a plain create_run failure leaves no durable document at all.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_SUCCESS, {
      io: faultIo({ failCommit: 1, failStep: "write" }),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state).toBeNull();
  }
});

test("17. after a poisoned sink no further state writes or side effects happen", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
    io: faultIo({ failCommit: 5, failStep: "dirfsync" }),
  });
  const fake = fakeRuntime([{}, {}]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("state_persist_failed");
  expect(harness.sink.poisoned).toBe(true);
  // the adopted candidate is the last visible revision; no later command
  // (agent_failed/run_failed) was written after the poisoning
  expect(agentAt(result.state, 0).phase).toBe("running");
  expect(result.state.failure).toBeUndefined();
  expect(result.state.revision).toBe(5);
  // the existing session was still cleaned up exactly once and no new
  // Session was created
  expect(fake.sessions).toHaveLength(1);
  expect(fake.sessions[0]?.cleanupCount).toBe(1);
});

test("18. run-output publication and terminal-write faults are contained", async () => {
  // (a) the publication record fails after the outputs were published.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      faults: new Map([
        [
          "run_outputs_published",
          () => new PipelineV2RunStateStoreError("injected store failure at run_outputs_published"),
        ],
      ]),
    });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(result.state.run_outputs).toBeUndefined();
    expect(result.state.status).toBe("failed");
  }

  // (b) the terminal_reached write fails: no terminal is recorded.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_SUCCESS, {
      faults: new Map([
        [
          "terminal_reached",
          () => new PipelineV2RunStateStoreError("injected store failure at terminal_reached"),
        ],
      ]),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state.terminal).toBeUndefined();
    expect(result.state.run_outputs).toBeUndefined();
  }

  // (c) the run_succeeded write fails: the one bounded retry records the
  // normalized failure from the committed snapshot.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_SUCCESS, {
      faults: new Map([
        [
          "run_succeeded",
          () => new PipelineV2RunStateStoreError("injected store failure at run_succeeded"),
        ],
      ]),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    expect(result.ok).toBe(false);
    if (result.ok || result.state === null) {
      throw new Error("unexpected result shape");
    }
    expect(result.reason).toBe("state_persist_failed");
    expect(result.state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(result.state.run_outputs).toBeDefined();
    expect(result.state.status).toBe("failed");
  }
});

test("19. runtime method reassignment during a pending callback cannot change the captured callbacks", async () => {
  const harness = await setupHarness(PIPELINE_TWO_AGENTS);
  let releaseFirstSession: (() => void) | undefined;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirstSession = resolve;
  });
  const fake = fakeRuntime([
    { gate: firstGate, sessionId: "session-A" },
    {},
  ]);
  const coordination = coordinate(harness, fake.runtime);

  // While the first createSession is pending, sabotage the runtime object.
  expect(fake.createCalls).toHaveLength(0);
  (fake.runtime as unknown as { createSession: unknown }).createSession = async () => {
    throw new Error("ROGUE-RUNTIME");
  };
  releaseFirstSession?.();
  const result = await coordination;
  const state = expectOk(result);
  expect(state.executions.map((execution) => {
    if (execution.type !== "agent") {
      throw new Error("expected an agent execution");
    }
    return execution.session_id;
  })).toEqual([
    "session-A",
    "session-2",
  ]);
  expect(state.status).toBe("success");
});

test("20. protected inputs and accepted outputs are re-verified through the existing data plane", async () => {
  // (a) the snapshot of a protected input disappears: the next activation
  // preparation fails with run_input_modified before any new Session.
  const harness = await setupHarness(PIPELINE_TWO_AGENTS);
  const fake = fakeRuntime([
    {
      onRun: async () => {
        await rm(join(harness.dirs.runRoot, "data", "inputs", "source"), {
          recursive: true,
          force: true,
        });
      },
    },
    {},
  ]);
  const result = await coordinate(harness, fake.runtime);
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("run_input_modified");
  expect(fake.sessions).toHaveLength(1);
  // (b) the accepted-output tamper case is covered by test 14b.
});

test("21. durable state and results carry no secrets, prompts, facts, bodies, error messages or worker output", async () => {
  const harness = await setupHarness(PIPELINE_AGENT_DECISION, { source: "SECRET-SOURCE-BODY\n" });
  const fake = fakeRuntime([{}]);
  const result = await coordinate(harness, fake.runtime);
  const state = expectOk(result);
  const stateJson = JSON.stringify(state);
  const resultJson = JSON.stringify(result);
  for (const canary of [
    "IMPLEMENT-THE-TASK",
    "SECRET-SOURCE-BODY",
    "banana",
    '{"f1"',
    "RUNTIME-EXPLODED",
    "WORKER-EXPLODED",
    "CLEANUP-EXPLODED",
    harness.dirs.sources,
  ]) {
    expect(stateJson.includes(canary)).toBe(false);
    expect(resultJson.includes(canary)).toBe(false);
  }
});

test("22. the existing evaluateDecisionStateFromData stays the wrapper over prepare + evaluate", async () => {
  const dirs = await makeDirs();
  await writeBundle(dirs, PIPELINE_DECISION_FROM_OUTPUT, {
    facts: JSON.stringify({ f1: false, f2: false }),
  });
  const pipeline = await loadPipelineV2(dirs.bundle);
  const runInputs = await snapshotRunInputs(pipeline, bindingsFor(dirs, pipeline), dirs.runRoot);
  // The decision input is fed by an accepted agent output living at the
  // fixed activation location; the file carries valid facts.
  const activationDir = join(dirs.runRoot, "activations", "1-coder", "data", "outputs");
  await mkdir(activationDir, { recursive: true });
  const outputPath = join(activationDir, "facts");
  await writeFile(outputPath, JSON.stringify({ f1: false, f2: true }));
  const digest = await acceptedOutputDigest("json", outputPath, "decision facts");
  const accepted: AcceptedStateOutput[] = [
    { state: "coder", output: "facts", activation_index: 1, digest },
  ];
  const prepared = await prepareDecisionStateData(pipeline, runInputs, accepted, "check", 2);
  expect(prepared.state_id).toBe("check");
  expect(prepared.execution_index).toBe(2);
  // After preparation the accepted output file changes; the evaluation
  // still uses the saved bytes — the file is never re-read.
  await writeFile(outputPath, JSON.stringify({ f1: true, f2: false }));
  const result = evaluatePreparedDecisionState(pipeline, prepared);
  if (result.status !== "selected") {
    throw new Error(`expected a selected decision, got ${result.status}`);
  }
  expect(result.outcome).toBe("beta");
  expect(result.decision).toBe("beta");
  expect(result.rule_id).toBe("rule-b");
});

test("23. the result reason and the durable failure reason stay consistent on every write outcome", async () => {
  // (a) a normal worker failure: the result reason and the durable
  // state.failure.reason are both worker_failed; no extra filesystem work.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ run: "worker_failed" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "worker_failed");
    expect(state.failure).toEqual({ reason: "worker_failed" });
    expect(agentAt(state, 0).failure_reason).toBe("worker_failed");
    expect(agentAt(state, 0).session_cleanup).toBe("completed");
    expect(state.status).toBe("failed");
    expect(harness.ioCounts).toEqual({ tempOpens: 7, renames: 7, dirSyncs: 7 });
  }

  // (b) the first run_failed{worker_failed} is not committed; the one
  // bounded normalized retry commits: both reasons reflect
  // state_persist_failed while the execution keeps its own reason.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      faults: new Map([["run_failed", failOnce("injected store failure at the first run_failed")]]),
    });
    const fake = fakeRuntime([{ run: "worker_failed" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.failure).toEqual({ reason: "state_persist_failed" });
    expect(agentAt(state, 0).failure_reason).toBe("worker_failed");
    expect(agentAt(state, 0).session_cleanup).toBe("completed");
    expect(kinds(harness.recording).filter((kind) => kind === "run_failed")).toHaveLength(2);
    expect(harness.recording.commands.at(-1)?.reason).toBe("state_persist_failed");
    // the faulted attempt consumed no reducer/filesystem work
    expect(harness.ioCounts).toEqual({ tempOpens: 7, renames: 7, dirSyncs: 7 });
  }

  // (c) the normalized retry is not committed either: the result reports
  // state_persist_failed and the last authoritative snapshot stays active
  // with the execution recorded as failed.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      faults: new Map([
        ["run_failed", () => new PipelineV2RunStateStoreError("injected store failure at run_failed")],
      ]),
    });
    const fake = fakeRuntime([{ run: "worker_failed" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    expect(agentAt(state, 0).phase).toBe("failed");
    expect(agentAt(state, 0).failure_reason).toBe("worker_failed");
    expect(agentAt(state, 0).session_cleanup).toBe("completed");
    expect(kinds(harness.recording).filter((kind) => kind === "run_failed")).toHaveLength(2);
    expect(kinds(harness.recording)).not.toContain("run_cleanup_failed");
    // both rejected attempts stayed off the filesystem and the reducer
    expect(harness.ioCounts).toEqual({ tempOpens: 6, renames: 6, dirSyncs: 6 });
  }

  // (d) agent_failed is not committed: the execution stays unfinished, no
  // incompatible run_failed is attempted, the result is state_persist_failed.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      faults: new Map([
        ["agent_failed", () => new PipelineV2RunStateStoreError("injected store failure at agent_failed")],
      ]),
    });
    const fake = fakeRuntime([{ run: "worker_failed" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    expect(agentAt(state, 0).phase).toBe("running");
    expect(agentAt(state, 0).failure_reason).toBeUndefined();
    expect(state.transitions).toEqual([]);
    expect(kinds(harness.recording).at(-1)).toBe("agent_failed");
    expect(kinds(harness.recording)).not.toContain("run_failed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(harness.ioCounts).toEqual({ tempOpens: 5, renames: 5, dirSyncs: 5 });
  }

  // (e) the same for decision_failed: the decision execution stays
  // unfinished and no run-level write follows.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      faults: new Map([
        [
          "decision_evaluated",
          () => new PipelineV2RunStateStoreError("injected store failure at decision_evaluated"),
        ],
        [
          "decision_failed",
          () => new PipelineV2RunStateStoreError("injected store failure at decision_failed"),
        ],
      ]),
    });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    const decisionExecution = decisionAt(state, 1);
    expect(decisionExecution.phase).toBe("evaluating");
    expect(decisionExecution.failure_reason).toBeUndefined();
    expect(state.transitions).toHaveLength(1);
    expect(kinds(harness.recording).at(-1)).toBe("decision_failed");
    expect(kinds(harness.recording)).not.toContain("run_failed");
    expect(harness.ioCounts).toEqual({ tempOpens: 9, renames: 9, dirSyncs: 9 });
  }

  // (f) a not-committed run_cleanup_failed: no incompatible run_failed is
  // attempted; the result is state_persist_failed on the active snapshot.
  {
    const harness = await setupHarness(PIPELINE_TWO_AGENTS, {
      faults: new Map([
        [
          "run_cleanup_failed",
          () => new PipelineV2RunStateStoreError("injected store failure at run_cleanup_failed"),
        ],
      ]),
    });
    const fake = fakeRuntime([{ run: "completed", cleanup: "throw" }, {}]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    const agentExecution = agentAt(state, 0);
    expect(agentExecution.phase).toBe("failed");
    expect(agentExecution.failure_reason).toBe("session_cleanup_failed");
    expect(agentExecution.session_cleanup).toBe("failed");
    expect(kinds(harness.recording).filter((kind) => kind === "run_cleanup_failed")).toHaveLength(1);
    expect(kinds(harness.recording)).not.toContain("run_failed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(fake.createCalls).toEqual([{ stateId: "first", activationIndex: 1 }]);
    expect(harness.ioCounts).toEqual({ tempOpens: 7, renames: 7, dirSyncs: 7 });
  }

  // (g) a not-committed run_failed{terminal_failed}: the outputs stay
  // published, the run stays active, and no normalized fallback is written.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_FAILED, {
      faults: new Map([
        ["run_failed", () => new PipelineV2RunStateStoreError("injected store failure at run_failed")],
      ]),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(state.status).toBe("active");
    expect(state.phase).toBe("publishing_outputs");
    expect(state.failure).toBeUndefined();
    expect(state.terminal).toEqual({ state_id: "failed_end", result: "failed" });
    expect(state.run_outputs ?? []).toHaveLength(1);
    expect((state.run_outputs ?? [])[0]?.present).toBe(true);
    expect(kinds(harness.recording)).toEqual([
      "create_run",
      "terminal_reached",
      "run_outputs_published",
      "run_failed",
    ]);
    expect(harness.recording.commands[3]?.reason).toBe("terminal_failed");
    expect(harness.ioCounts).toEqual({ tempOpens: 3, renames: 3, dirSyncs: 3 });
  }
});

test("24. durability unknown at every failure-write level stops all further dispatches", async () => {
  // (a) during the agent_failed write: the adopted candidate is visible,
  // the sink is poisoned, and no command follows the poisoned dispatch.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      io: faultIo({ failCommit: 6, failStep: "dirfsync" }),
    });
    const fake = fakeRuntime([{ run: "worker_failed" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(harness.sink.poisoned).toBe(true);
    expect(kinds(harness.recording)).toEqual([
      "create_run",
      "start_agent_execution",
      "agent_data_prepared",
      "agent_session_created",
      "agent_running",
      "agent_failed",
    ]);
    expect(state.revision).toBe(6);
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    expect(agentAt(state, 0).phase).toBe("failed");
    expect(agentAt(state, 0).failure_reason).toBe("worker_failed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(harness.ioCounts).toEqual({ tempOpens: 6, renames: 6, dirSyncs: 5 });
  }

  // (b) during the decision_failed write: same containment, no further
  // dispatch, decision execution settled only in the adopted candidate.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION, {
      io: faultIo({ failCommit: 10, failStep: "dirfsync" }),
      faults: new Map([
        [
          "decision_evaluated",
          () => new PipelineV2RunStateStoreError("injected store failure at decision_evaluated"),
        ],
      ]),
    });
    const fake = fakeRuntime([{}]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(harness.sink.poisoned).toBe(true);
    expect(kinds(harness.recording).at(-1)).toBe("decision_failed");
    expect(kinds(harness.recording).filter((kind) => kind === "decision_failed")).toHaveLength(1);
    expect(state.revision).toBe(10);
    expect(state.status).toBe("active");
    expect(state.failure).toBeUndefined();
    expect(decisionAt(state, 1).phase).toBe("failed");
    expect(state.transitions).toHaveLength(1);
    expect(harness.ioCounts).toEqual({ tempOpens: 10, renames: 10, dirSyncs: 9 });
  }

  // (c) during the run_cleanup_failed write: the adopted candidate already
  // carries the cleanup_failed status, and nothing follows.
  {
    const harness = await setupHarness(PIPELINE_TWO_AGENTS, {
      io: faultIo({ failCommit: 8, failStep: "dirfsync" }),
    });
    const fake = fakeRuntime([{ run: "completed", cleanup: "throw" }, {}]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(harness.sink.poisoned).toBe(true);
    expect(kinds(harness.recording).at(-1)).toBe("run_cleanup_failed");
    expect(kinds(harness.recording)).not.toContain("run_failed");
    expect(state.revision).toBe(8);
    expect(state.status).toBe("cleanup_failed");
    expect(state.failure).toEqual({ reason: "session_cleanup_failed" });
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(harness.ioCounts).toEqual({ tempOpens: 8, renames: 8, dirSyncs: 7 });
  }

  // (d) during the terminal run_failed{terminal_failed} write: the outputs
  // stay published in the adopted candidate and nothing follows.
  {
    const harness = await setupHarness(PIPELINE_ENTRY_FAILED, {
      io: faultIo({ failCommit: 4, failStep: "dirfsync" }),
    });
    const fake = fakeRuntime([]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "state_persist_failed");
    expect(harness.sink.poisoned).toBe(true);
    expect(kinds(harness.recording)).toEqual([
      "create_run",
      "terminal_reached",
      "run_outputs_published",
      "run_failed",
    ]);
    expect(state.revision).toBe(4);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "terminal_failed" });
    expect(state.run_outputs ?? []).toHaveLength(1);
    expect(harness.ioCounts).toEqual({ tempOpens: 4, renames: 4, dirSyncs: 3 });
  }
});

test("25. the worker run result is accepted only in its two exact shapes", async () => {
  const cases: Array<{ run: NonNullable<FakeSessionSpec["run"]>; canaries: string[] }> = [
    { run: "rogue_completed", canaries: ["rogue"] },
    { run: "extra_failed", canaries: ["extra"] },
    { run: "missing_reason", canaries: [] },
    { run: "array", canaries: [] },
    { run: "null", canaries: [] },
    { run: "string", canaries: [] },
    { run: "getter_throw", canaries: ["GETTER-EXPLODED"] },
  ];
  for (const testCase of cases) {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ run: testCase.run }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "internal_error");
    const agentExecution = agentAt(state, 0);
    expect(agentExecution.phase).toBe("failed");
    expect(agentExecution.failure_reason).toBe("internal_error");
    expect(agentExecution.session_cleanup).toBe("completed");
    expect(state.failure).toEqual({ reason: "internal_error" });
    expect(state.transitions).toEqual([]);
    expect(fake.sessions[0]?.runCount).toBe(1);
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    const resultJson = JSON.stringify(result);
    const stateJson = JSON.stringify(state);
    for (const canary of testCase.canaries) {
      expect(resultJson.includes(canary)).toBe(false);
      expect(stateJson.includes(canary)).toBe(false);
    }
  }
});

test("26. a damaged session handle still cleans up exactly once through the captured cleanup", async () => {
  // (a) no run member, valid cleanup: internal_error, the captured cleanup
  // runs exactly once, no durable session record.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ damaged: "no_run" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "internal_error");
    expect(agentAt(state, 0).session_cleanup).toBe("not_required");
    expect(agentAt(state, 0).session_id).toBeUndefined();
    expect(kinds(harness.recording)).not.toContain("agent_session_created");
    expect(fake.sessions[0]?.runCount).toBe(0);
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
  }

  // (b) an empty session id with a valid cleanup: cleanup exactly once.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ damaged: "empty_session_id" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "internal_error");
    expect(kinds(harness.recording)).not.toContain("agent_session_created");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(agentAt(state, 0).session_cleanup).toBe("not_required");
  }

  // (c) a non-string session id behaves the same.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ damaged: "nonstring_session_id" }]);
    const result = await coordinate(harness, fake.runtime);
    expectFailedState(result, "internal_error");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(kinds(harness.recording)).not.toContain("agent_session_created");
  }

  // (d) no cleanup member at all is a trusted runtime contract violation:
  // nothing runnable exists, so no cleanup is claimed or confirmed.
  {
    const harness = await setupHarness(PIPELINE_AGENT_DECISION);
    const fake = fakeRuntime([{ damaged: "no_cleanup" }]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "internal_error");
    expect(agentAt(state, 0).session_cleanup).toBe("not_required");
    expect(agentAt(state, 0).session_id).toBeUndefined();
    expect(kinds(harness.recording)).not.toContain("agent_session_created");
    expect(fake.sessions[0]?.runCount).toBe(0);
    expect(fake.sessions[0]?.cleanupCount).toBe(0);
  }

  // (e) reassigning the session methods after the capture cannot change
  // the calls: the captured cleanup runs exactly once, the rogue never.
  {
    const harness = await setupHarness(PIPELINE_TWO_AGENTS);
    const fake = fakeRuntime([
      { run: "worker_failed", sabotageDuringRun: true },
      {},
    ]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectFailedState(result, "worker_failed");
    expect(fake.sessions[0]?.cleanupCount).toBe(1);
    expect(fake.sessions[0]?.rogueCount).toBe(0);
    expect(agentAt(state, 0).session_cleanup).toBe("completed");
  }

  // (f) the same sabotage on the success path: the captured cleanup is
  // still the one invoked, exactly once.
  {
    const harness = await setupHarness(PIPELINE_TWO_AGENTS);
    const fake = fakeRuntime([{ sabotageDuringRun: true }, {}]);
    const result = await coordinate(harness, fake.runtime);
    const state = expectOk(result);
    expect(state.status).toBe("success");
    expect(fake.sessions.map((session) => session.cleanupCount)).toEqual([1, 1]);
    expect(fake.sessions[0]?.rogueCount).toBe(0);
  }
});

// --- helpers used above ----------------------------------------------------

interface BundleDirs {
  root: string;
  bundle: string;
  sources: string;
  runRoot: string;
  stateRoot: string;
}

async function makeDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-coordinator-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  const runRoot = join(root, "runs", "coord-run");
  await mkdir(join(runRoot, "project"), { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, sources, runRoot, stateRoot };
}

async function writeBundle(
  dirs: BundleDirs,
  yaml: string,
  options: { facts?: string; source?: string } = {},
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  await writeFile(join(dirs.bundle, "schemas", "loose.schema.json"), JSON.stringify(LOOSE_SCHEMA));
  await writeFile(join(dirs.bundle, "decisions", "model.yaml"), MODEL_YAML);
  await writeFile(
    join(dirs.sources, "facts.json"),
    options.facts ?? JSON.stringify({ f1: true, f2: false }),
  );
  await writeFile(join(dirs.sources, "source.txt"), options.source ?? "SOURCE-BODY\n");
}

const SOURCE_FILE_NAMES: Record<string, string> = {
  facts_seed: "facts.json",
  source: "source.txt",
};

function bindingsFor(dirs: BundleDirs, pipeline: ResolvedPipelineV2): Array<{ id: string; path: string }> {
  return pipeline.inputs.map((input) => {
    const file = SOURCE_FILE_NAMES[input.id];
    if (file === undefined) {
      throw new Error(`test harness has no source file for run input ${input.id}`);
    }
    return { id: input.id, path: join(dirs.sources, file) };
  });
}

interface FakeSessionSpec {
  sessionId?: string;
  /**
   * createSession hands the coordinator a damaged facade: `no_cleanup`
   * has no cleanup member, `no_run` has no run member, and the
   * `*_session_id` variants carry an invalid session id.
   */
  damaged?: "no_cleanup" | "no_run" | "empty_session_id" | "nonstring_session_id";
  run?:
    | "completed"
    | "worker_failed"
    | "worker_timeout"
    | "throw"
    | "invalid"
    | "rogue_completed"
    | "extra_failed"
    | "missing_reason"
    | "array"
    | "null"
    | "string"
    | "getter_throw";
  runError?: unknown;
  cleanup?: "throw";
  onRun?: (session: FakeAgentSession) => void | Promise<void>;
  onCleanup?: (session: FakeAgentSession) => void | Promise<void>;
  /** Replaces the session's own run/cleanup members while run executes. */
  sabotageDuringRun?: boolean;
  /** The createSession call waits for this promise before resolving. */
  gate?: Promise<void>;
}

class FakeAgentSession {
  runCount = 0;
  cleanupCount = 0;
  rogueCount = 0;
  readonly sessionId: string;

  constructor(
    readonly spec: FakeSessionSpec,
    readonly stateId: string,
    readonly activation: PreparedActivationData,
    fallbackSessionId: string,
  ) {
    this.sessionId = spec.sessionId ?? fallbackSessionId;
  }

  async run(): Promise<PipelineV2WorkerRunResult> {
    this.runCount += 1;
    if (this.spec.sabotageDuringRun) {
      // Reassign both members while the coordinator holds its captured
      // bindings; the captures must stay in charge.
      const rogue = async () => {
        this.rogueCount += 1;
        throw new Error("ROGUE-MEMBER");
      };
      const mutable = this as unknown as { run: unknown; cleanup: unknown };
      mutable.run = rogue;
      mutable.cleanup = rogue;
    }
    await this.spec.onRun?.(this);
    // A real worker writes exactly its declared outputs; the fake does the
    // same for every declared port before reporting completion.
    for (const port of this.activation.output_ports) {
      if (port.type === "directory") {
        await mkdir(port.path, { recursive: true });
      } else if (port.type === "json") {
        await writeFile(port.path, JSON.stringify({ ok: true, port: port.id }));
      } else {
        await writeFile(port.path, `${port.id} body`);
      }
    }
    switch (this.spec.run) {
      case "worker_failed":
        return { status: "failed", reason: "worker_failed" };
      case "worker_timeout":
        return { status: "failed", reason: "worker_timeout" };
      case "throw":
        throw this.spec.runError ?? new Error("WORKER-EXPLODED");
      case "invalid":
        return { status: "failed", reason: "exploded" } as unknown as PipelineV2WorkerRunResult;
      case "rogue_completed":
        return { status: "completed", outcome: "rogue" } as unknown as PipelineV2WorkerRunResult;
      case "extra_failed":
        return {
          status: "failed",
          reason: "worker_failed",
          extra: 1,
        } as unknown as PipelineV2WorkerRunResult;
      case "missing_reason":
        return { status: "failed" } as unknown as PipelineV2WorkerRunResult;
      case "array":
        return [] as unknown as PipelineV2WorkerRunResult;
      case "null":
        return null as unknown as PipelineV2WorkerRunResult;
      case "string":
        return "completed" as unknown as PipelineV2WorkerRunResult;
      case "getter_throw":
        return {
          get status(): string {
            throw new Error("GETTER-EXPLODED");
          },
        } as unknown as PipelineV2WorkerRunResult;
      default:
        return { status: "completed" };
    }
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
    await this.spec.onCleanup?.(this);
    if (this.spec.cleanup === "throw") {
      throw new Error("CLEANUP-EXPLODED");
    }
  }
}

interface FakeRuntimeHandle {
  runtime: PipelineV2AgentRuntime;
  sessions: FakeAgentSession[];
  createCalls: Array<{ stateId: string; activationIndex: number }>;
}

function fakeRuntime(specs: readonly FakeSessionSpec[]): FakeRuntimeHandle {
  const sessions: FakeAgentSession[] = [];
  const createCalls: Array<{ stateId: string; activationIndex: number }> = [];
  const runtime = {
    createSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const index = sessions.length;
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index });
      const spec = specs[index] ?? {};
      if (spec.gate !== undefined) {
        await spec.gate;
      }
      const session = new FakeAgentSession(spec, state.id, activation, `session-${index + 1}`);
      sessions.push(session);
      if (spec.damaged === undefined) {
        return session;
      }
      // A damaged facade: the named member is missing or damaged; every
      // other member delegates to the real session (whose counters stay
      // observable).
      const facade: Record<string, unknown> = { sessionId: session.sessionId };
      if (spec.damaged !== "no_cleanup") {
        facade.cleanup = () => session.cleanup();
      }
      if (spec.damaged !== "no_run") {
        facade.run = () => session.run();
      }
      if (spec.damaged === "empty_session_id") {
        facade.sessionId = "";
      } else if (spec.damaged === "nonstring_session_id") {
        facade.sessionId = 42;
      }
      return facade as unknown as PipelineV2AgentSession;
    },
  };
  return { runtime: runtime as unknown as PipelineV2AgentRuntime, sessions, createCalls };
}

type CommandRecord = Record<string, unknown>;

class RecordingSink implements PipelineV2CoordinatorStateSink {
  readonly commands: CommandRecord[] = [];

  constructor(
    private readonly inner: PipelineV2RunStateSink,
    private readonly faults?: ReadonlyMap<string, () => Error | undefined>,
  ) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2CoordinatorStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    const fault = this.faults?.get(command.kind);
    if (fault !== undefined) {
      const failure = fault();
      if (failure !== undefined) {
        throw failure;
      }
    }
    await this.inner.dispatch(command);
  }
}

interface Harness {
  dirs: BundleDirs;
  pipeline: ResolvedPipelineV2;
  sink: PipelineV2RunStateSink;
  recording: RecordingSink;
  ioCounts: IoCounts;
}

async function setupHarness(
  yaml: string,
  options: {
    facts?: string;
    source?: string;
    io?: PipelineStateIo;
    faults?: ReadonlyMap<string, () => Error | undefined>;
  } = {},
): Promise<Harness> {
  const dirs = await makeDirs();
  await writeBundle(dirs, yaml, options);
  const pipeline = await loadPipelineV2(dirs.bundle);
  const counted = countingIo(options.io);
  const sink = new PipelineV2RunStateSink({
    stateRoot: dirs.stateRoot,
    runId: "coord-run",
    io: counted.io,
  });
  return {
    dirs,
    pipeline,
    sink,
    recording: new RecordingSink(sink, options.faults),
    ioCounts: counted.counts,
  };
}

async function coordinate(
  harness: Harness,
  runtime: PipelineV2AgentRuntime,
  options: { runId?: string } = {},
): Promise<PipelineV2CoordinationResult> {
  return await coordinatePipelineV2Run({
    pipeline: harness.pipeline,
    runId: options.runId ?? "coord-run",
    runRoot: harness.dirs.runRoot,
    inputBindings: bindingsFor(harness.dirs, harness.pipeline),
    sink: harness.recording,
    runtime,
  });
}

function kinds(recording: RecordingSink): string[] {
  return recording.commands.map((command) => command.kind as string);
}

function expectOk(result: PipelineV2CoordinationResult): PipelineV2RunState {
  if (!result.ok) {
    throw new Error(`expected a successful coordination, got ${String(result.reason)}`);
  }
  return result.state;
}

function expectFailedState(
  result: PipelineV2CoordinationResult,
  reason: PipelineV2FailureReason,
): PipelineV2RunState {
  expect(result.ok).toBe(false);
  if (result.ok || result.state === null) {
    throw new Error("unexpected coordination result shape");
  }
  expect(result.reason).toBe(reason);
  // whenever the reason is not state_persist_failed it must equal the
  // final durable failure reason
  if (reason !== "state_persist_failed") {
    expect(result.state.failure).toEqual({ reason });
  }
  return result.state;
}

/** A recording fault that throws only on its first invocation. */
function failOnce(message: string): () => Error | undefined {
  let seen = 0;
  return () => {
    seen += 1;
    if (seen === 1) {
      return new PipelineV2RunStateStoreError(message);
    }
    return undefined;
  };
}

function agentAt(state: PipelineV2RunState, index: number): PipelineV2AgentExecutionState {
  const execution = state.executions[index];
  if (execution === undefined || execution.type !== "agent") {
    throw new Error(`expected an agent execution at ${index}`);
  }
  return execution;
}

function decisionAt(state: PipelineV2RunState, index: number): PipelineV2DecisionExecutionState {
  const execution = state.executions[index];
  if (execution === undefined || execution.type !== "decision") {
    throw new Error(`expected a decision execution at ${index}`);
  }
  return execution;
}

const SHA256_HEX = /^[0-9a-f]{64}$/;
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function isHexDigest(value: unknown): boolean {
  return typeof value === "string" && SHA256_HEX.test(value);
}
