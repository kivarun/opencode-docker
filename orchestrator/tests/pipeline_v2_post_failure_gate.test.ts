import { describe, expect, test } from "bun:test";
import {
  PipelineV2StateError,
  parsePipelineV2RunState,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  compilePipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const RUN_ID = "run-1";
const hex = (c: string): string => c.repeat(64);
const PIPELINE_IDENTITY = {
  schema_version: 2 as const,
  bundle_root: "/opt/orchestrator/pipelines/default",
  execution_snapshot_sha256: hex("c"),
  entry_state: "architect",
  max_transitions: 40,
};

let tick = 0;
function nextTick(): Date {
  tick += 1;
  return new Date(Date.UTC(2026, 8, 25, 0, 0, tick));
}

/** A fixed ISO timestamp for Date-like stand-ins (no counter involvement). */
function isoAt(seconds: number): string {
  return new Date(Date.UTC(2026, 8, 25, 0, 0, seconds)).toISOString();
}

/** Removes // line and /* block comments so a source pin never counts them. */
function stripComments(source: string): string {
  let out = "";
  let index = 0;
  while (index < source.length) {
    const char = source[index];
    const next = source[index + 1];
    if (char === "/" && next === "/") {
      while (index < source.length && source[index] !== "\n") {
        index += 1;
      }
      continue;
    }
    if (char === "/" && next === "*") {
      const end = source.indexOf("*/", index + 2);
      index = end === -1 ? source.length : end + 2;
      continue;
    }
    out += char;
    index += 1;
  }
  return out;
}

/** A reducer-only flow builder with a fixed identity and zero inputs. */
function build(commands: PipelineV2RunCommand[]): PipelineV2RunState {
  let state: PipelineV2RunState | null = null;
  for (const command of commands) {
    state = reducePipelineV2RunCommand(state, command, nextTick());
  }
  return state as PipelineV2RunState;
}

const PLANNING_PHASES: readonly PipelineV2RunCommand[] = [
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-1" },
  { kind: "agent_tool_session_created", sessionId: "tool-1" },
  { kind: "agent_running" },
  { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
  { kind: "agent_cleanup_completed" },
];

const STAGE_PHASES: readonly PipelineV2RunCommand[] = [
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-2" },
  { kind: "agent_tool_session_created", sessionId: "tool-2" },
  { kind: "agent_running" },
];

/** Planning settled, plan accepted, transition, generation 1 + iteration 1, stage agent failed (ordinary). */
function failedStageState(): PipelineV2RunState {
  return build([
    { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    ...PLANNING_PHASES,
    { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
    { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 }, executionIndex: 1 },
    { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 1 },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 },
    { kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 },
    ...STAGE_PHASES,
    { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
  ]);
}

/** Planning agent failed (ordinary) with no plan accepted. */
function failedPlanningState(): PipelineV2RunState {
  return build([
    { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "sess-1" },
    { kind: "agent_tool_session_created", sessionId: "tool-1" },
    { kind: "agent_running" },
    { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
  ]);
}

/** Control decision failed. */
function failedDecisionState(): PipelineV2RunState {
  return build([
    { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    ...PLANNING_PHASES,
    { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
    { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
    { kind: "decision_failed", reason: "decision_input_invalid" },
  ]);
}

/** Stage agent failed with an unconfirmed (failed) session cleanup. */
function cleanupFailedState(): PipelineV2RunState {
  return build([
    { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "sess-1" },
    { kind: "agent_tool_session_created", sessionId: "tool-1" },
    { kind: "agent_running" },
    { kind: "agent_failed", reason: "session_cleanup_failed", sessionCleanup: { execution: "failed", tool: "completed" } },
  ]);
}

const GATE_MESSAGE_PART =
  "a failed execution allows only the run failure finalization (run_failed or run_cleanup_failed)";

/** One plausible command per non-finalizer, non-create kind (payloads are never read by the gate). */
const NON_FINALIZER_COMMANDS: readonly PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 },
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId: "sess-x" },
  { kind: "agent_tool_session_created", sessionId: "tool-x" },
  { kind: "agent_running" },
  { kind: "agent_outputs_accepted", outputs: [] },
  { kind: "agent_cleanup_completed" },
  { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
  { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
  {
    kind: "decision_evaluated",
    result: { status: "selected", outcome: "d_close", decision: "d_close", rule_id: "r_close", active_constraint_ids: [] },
  },
  { kind: "decision_failed", reason: "decision_input_invalid" },
  { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 }, executionIndex: 1 },
  { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 1 },
  { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 },
  { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" },
  { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" },
  { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("e") },
  { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("3") },
  { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
  { kind: "iteration_grant_recorded", generationIndex: 1, waitIndex: 1, intentSha256: hex("3"), additionalIterations: 1 },
  { kind: "run_waiting", stateId: "architect", reason: "stage_iteration_limit_exhausted", requestSha256: hex("1"), actions: [{ id: "revise_task", to: "architect" }] },
  { kind: "wait_response_recorded", waitIndex: 1, expectedRequestSha256: hex("1"), actionId: "revise_task", responseSha256: hex("2") },
  { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
  { kind: "run_outputs_published", outputs: [] },
  { kind: "run_succeeded" },
];

const FINALIZER_COMMANDS: readonly PipelineV2RunCommand[] = [
  { kind: "run_failed", reason: "worker_failed" },
  { kind: "run_cleanup_failed" },
];

describe("pipeline v2 post-failure successor gate", () => {
  test("1. after an ordinary agent failure every non-finalizer command is rejected by the unified gate", () => {
    const failed = failedStageState();
    const before = JSON.stringify(failed);
    for (const command of NON_FINALIZER_COMMANDS) {
      let message: string | undefined;
      try {
        reducePipelineV2RunCommand(failed, command, nextTick());
      } catch (cause) {
        expect(cause).toBeInstanceOf(PipelineV2StateError);
        message = (cause as Error).message;
      }
      expect(message, `command ${command.kind} must be rejected`).toBeDefined();
      expect(message!).toContain(`command ${JSON.stringify(command.kind)} rejected`);
      expect(message!).toContain(GATE_MESSAGE_PART);
    }
    expect(JSON.stringify(failed)).toBe(before);
  });

  test("2. after a decision failure every non-finalizer command is rejected by the unified gate", () => {
    const failed = failedDecisionState();
    for (const command of NON_FINALIZER_COMMANDS) {
      let message: string | undefined;
      try {
        reducePipelineV2RunCommand(failed, command, nextTick());
      } catch (cause) {
        expect(cause).toBeInstanceOf(PipelineV2StateError);
        message = (cause as Error).message;
      }
      expect(message, `command ${command.kind} must be rejected`).toBeDefined();
      expect(message!).toContain(GATE_MESSAGE_PART);
    }
  });

  test("3. table-driven pin: the two finalizers pass the gate, everything else is rejected at the gate", () => {
    const failed = failedStageState();
    for (const command of FINALIZER_COMMANDS) {
      let message: string | undefined;
      try {
        reducePipelineV2RunCommand(failed, command, nextTick());
      } catch (cause) {
        message = (cause as Error).message;
      }
      // the finalizers reach their own case rules; the gate message is absent
      if (message !== undefined) {
        expect(message).not.toContain(GATE_MESSAGE_PART);
      }
    }
    for (const command of NON_FINALIZER_COMMANDS) {
      let message: string | undefined;
      try {
        reducePipelineV2RunCommand(failed, command, nextTick());
      } catch (cause) {
        message = (cause as Error).message;
      }
      expect(message).toBeDefined();
      expect(message!).toContain(GATE_MESSAGE_PART);
    }
    // create_run keeps its own already-exists rejection
    let createMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] }, nextTick());
    } catch (cause) {
      createMessage = (cause as Error).message;
    }
    expect(createMessage).toContain("already exists");
    expect(createMessage).not.toContain(GATE_MESSAGE_PART);
  });

  test("4. the gate reads only the command discriminator: a payload getter trap is never fired", () => {
    const failed = failedStageState();
    const reads: string[] = [];
    const hostileCommand = new Proxy({ kind: "agent_data_prepared" } as unknown as PipelineV2RunCommand, {
      get(target, property, receiver) {
        reads.push(String(property));
        return Reflect.get(target, property, receiver);
      },
    });
    let message: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, hostileCommand, nextTick());
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain(GATE_MESSAGE_PART);
    // every property read is the discriminator; no payload field is touched
    expect(new Set(reads)).toEqual(new Set(["kind"]));
  });

  test("5. the timestamp boundary is the reducer's own now.toISOString() read", () => {
    // The reducer receives an already-created `now`; the exact contract is
    // that a gate refusal never reads `now.toISOString()`. A gate refusal
    // therefore also succeeds against a `Date`-like object whose timestamp
    // read throws.
    const failed = failedStageState();
    const before = JSON.stringify(failed);
    let toIsoCalls = 0;
    const canary = new Error("TO_ISO_CALLED_AT_GATE");
    const hostileNow = {
      toISOString() {
        toIsoCalls += 1;
        throw canary;
      },
    } as unknown as Date;
    let caught: unknown;
    try {
      reducePipelineV2RunCommand(failed, { kind: "agent_data_prepared" }, hostileNow);
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(PipelineV2StateError);
    expect((caught as Error).message).toContain(GATE_MESSAGE_PART);
    expect(caught).not.toBe(canary);
    expect(toIsoCalls).toBe(0);
    expect(JSON.stringify(failed)).toBe(before);
    // positive control: an allowed finalizer reaches the timestamp boundary
    // exactly once and finalizes successfully with a normal return
    let finalizeCalls = 0;
    const finalizeNow = {
      toISOString() {
        finalizeCalls += 1;
        return isoAt(99);
      },
    } as unknown as Date;
    const finalized = reducePipelineV2RunCommand(
      failedStageState(),
      { kind: "run_failed", reason: "worker_failed" },
      finalizeNow,
    );
    expect(finalizeCalls).toBe(1);
    expect(finalized.status).toBe("failed");
    expect(finalized.phase).toBe("finished");
    expect(finalized.updated_at).toBe(isoAt(99));
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
    // a command that passes the gate but fails inside its own case still
    // reads the timestamp exactly once before the case failure
    let caseCalls = 0;
    const caseNow = {
      toISOString() {
        caseCalls += 1;
        return isoAt(98);
      },
    } as unknown as Date;
    let caseMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(failedStageState(), { kind: "run_cleanup_failed" }, caseNow);
    } catch (cause) {
      caseMessage = (cause as Error).message;
    }
    expect(caseMessage).toContain(
      "a cleanup failure requires the last agent execution to have failed with an unconfirmed session cleanup",
    );
    expect(caseCalls).toBe(1);
  });

  test("6. a gate refusal leaves the state document byte-identical", () => {
    const failed = failedStageState();
    const before = JSON.stringify(failed);
    for (const command of NON_FINALIZER_COMMANDS.slice(0, 6)) {
      try {
        reducePipelineV2RunCommand(failed, command, nextTick());
      } catch {
        // expected
      }
    }
    expect(JSON.stringify(failed)).toBe(before);
    expect(failed.revision).toBe(18);
  });

  test("7. an ordinary agent failure finalizes with run_failed", () => {
    const failed = failedStageState();
    const finalized = reducePipelineV2RunCommand(failed, { kind: "run_failed", reason: "worker_failed" }, nextTick());
    expect(finalized.status).toBe("failed");
    expect(finalized.phase).toBe("finished");
    expect(finalized.failure).toEqual({ reason: "worker_failed" });
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
  });

  test("8. a decision failure finalizes with run_failed", () => {
    const failed = failedDecisionState();
    const finalized = reducePipelineV2RunCommand(failed, { kind: "run_failed", reason: "decision_input_invalid" }, nextTick());
    expect(finalized.status).toBe("failed");
    expect(finalized.failure).toEqual({ reason: "decision_input_invalid" });
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
  });

  test("9. an agent cleanup failure finalizes with run_cleanup_failed", () => {
    const failed = cleanupFailedState();
    const finalized = reducePipelineV2RunCommand(failed, { kind: "run_cleanup_failed" }, nextTick());
    expect(finalized.status).toBe("cleanup_failed");
    expect(finalized.phase).toBe("finished");
    expect(finalized.failure).toEqual({ reason: "session_cleanup_failed" });
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
  });

  test("10. an agent cleanup failure still rejects run_failed by the existing case rule", () => {
    const failed = cleanupFailedState();
    let message: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, { kind: "run_failed", reason: "worker_failed" }, nextTick());
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain("a failed session cleanup finalizes with run_cleanup_failed");
    expect(message).not.toContain(GATE_MESSAGE_PART);
  });

  test("11. an ordinary failure still rejects run_cleanup_failed by the existing case rule", () => {
    const failed = failedStageState();
    let message: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, { kind: "run_cleanup_failed" }, nextTick());
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain("a cleanup failure requires the last agent execution to have failed with an unconfirmed session cleanup");
    expect(message).not.toContain(GATE_MESSAGE_PART);
  });

  test("12. the stage lifecycle openings reject an in-flight execution, keeping the loader rule sound", () => {
    // the honest contract order: the settled planning execution, then the
    // generation and iteration openings at the same boundary
    const settled = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
    ]);
    const generation = reducePipelineV2RunCommand(settled, { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 0 }, nextTick());
    const iteration = reducePipelineV2RunCommand(generation, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 }, nextTick());
    expect(iteration.generations[0]?.open_iteration?.index).toBe(1);
    // the in-flight variants: a decision execution evaluating at the
    // boundary, without a generation (the generation probe) and with one
    // (the iteration probe)
    const inFlightWithoutGeneration = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
      { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
      { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
    ]);
    let generationMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(inFlightWithoutGeneration, { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 1 }, nextTick());
    } catch (cause) {
      generationMessage = (cause as Error).message;
    }
    expect(generationMessage).toContain("opening a stage generation requires the run's last execution 2 to be settled, got the in-flight phase \"evaluating\"");
    const inFlightWithGeneration = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
      { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
      { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 1 },
      { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
    ]);
    let iterationMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(inFlightWithGeneration, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 }, nextTick());
    } catch (cause) {
      iterationMessage = (cause as Error).message;
    }
    expect(iterationMessage).toContain("opening a stage iteration requires the run's last execution 2 to be settled, got the in-flight phase \"evaluating\"");
  });

  test("13. the successful stage-boundary flow (settled planning -> generation -> iteration -> transition) is not broken", () => {
    const state = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
      { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 0 },
      { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
      { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 }, executionIndex: 1 },
      { kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 },
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-2" },
      { kind: "agent_tool_session_created", sessionId: "tool-2" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [{ id: "result", digest: hex("5") }] },
      { kind: "agent_cleanup_completed" },
    ]);
    // serialize -> loader round-trip of the successful flow
    validatePipelineV2RunState(JSON.parse(JSON.stringify(state)) as never);
    expect(state.generations[0]?.open_iteration?.index).toBe(1);
  });

  test("14/18. a reducer-produced failed stage state with the still-open iteration passes the serialize->loader round-trip", () => {
    const failed = failedStageState();
    const parsed = validatePipelineV2RunState(JSON.parse(JSON.stringify(failed)) as never);
    expect(parsed.executions[1]?.phase).toBe("failed");
    expect(parsed.generations[0]?.open_iteration?.index).toBe(1);
    expect(parsed.status).toBe("active");
    expect(parsed.failure).toBeUndefined();
    // the parsed document is the loader-valid form of the same run
    const reparsed = parsePipelineV2RunState(JSON.stringify(parsed));
    expect(reparsed.revision).toBe(failed.revision);
  });

  test("15/16. the loader rejects a plan revision whose planning origin execution failed", () => {
    const clean = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
    ]);
    // sanity: the clean document loads
    parsePipelineV2RunState(JSON.stringify(clean));
    const forgedRaw = JSON.parse(JSON.stringify(clean)) as Record<string, unknown>;
    const execution = (forgedRaw.executions as Record<string, unknown>[])[0] as Record<string, unknown>;
    execution["phase"] = "failed";
    execution["failure_reason"] = "worker_failed";
    execution["session_cleanup"] = { execution: "completed", tool: "completed" };
    let message: string | undefined;
    try {
      parsePipelineV2RunState(JSON.stringify(forgedRaw));
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain('plan revision record 1 names origin execution 1 with phase "failed"');
    expect(message).toContain('plan acceptance requires the planning execution to be cleanly settled ("cleanup_completed")');
  });

  test("16/17. the loader rejects a failed stage execution whose iteration is closed at its failure boundary", () => {
    const failed = failedStageState();
    const forgedRaw = JSON.parse(JSON.stringify(failed)) as Record<string, unknown>;
    const generation = (forgedRaw.generations as Record<string, unknown>[])[0] as Record<string, unknown>;
    generation["iterations"] = [
      { index: 1, opened_transition_count: 1, closed: { by: "normal_close", closed_transition_count: 1 } },
    ];
    delete generation["open_iteration"];
    let message: string | undefined;
    try {
      parsePipelineV2RunState(JSON.stringify(forgedRaw));
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain("execution 2 has failed, so its referenced iteration 1 must still be open");
  });

  test("17/19. the loader rejects a generation and an iteration opening anchored at a failed planning/control start boundary", () => {
    // honest reducer flow: the control execution starts and settles at
    // boundary 1 with the generation and iteration opened at boundary 1;
    // the forged document turns the settled control execution into a
    // failed one, which the successor gate can never produce
    const controlSettled = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
      { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
      { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 1 },
      { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
      { kind: "decision_evaluated", result: { status: "selected", outcome: "d_close", decision: "d_close", rule_id: "r_close", active_constraint_ids: [] } },
    ]);
    // sanity: the settled-control document loads (the same-anchor hook order)
    parsePipelineV2RunState(JSON.stringify(controlSettled));
    const forgedRaw = JSON.parse(JSON.stringify(controlSettled)) as Record<string, unknown>;
    const control = (forgedRaw.executions as Record<string, unknown>[])[1] as Record<string, unknown>;
    control["phase"] = "failed";
    control["failure_reason"] = "unknown_outcome";
    delete control["result"];
    let message: string | undefined;
    try {
      parsePipelineV2RunState(JSON.stringify(forgedRaw));
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain(
      'generation 1 opens at committed transition count 1, where execution 2 has failed with the "control" role',
    );
    // the iteration-opening variant: the generation opened at the planning
    // boundary, the iteration at the control's start boundary, forged to a
    // failed control execution
    const iterationAtControl = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
      ...PLANNING_PHASES,
      { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("a"), originExecution: 1 },
      { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("a"), initialBudget: 3, transitionCount: 0 },
      { kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 }, executionIndex: 1 },
      { kind: "start_decision_execution", stateId: "gate", inputDigest: hex("f"), executionRole: "control" },
      { kind: "decision_evaluated", result: { status: "selected", outcome: "d_close", decision: "d_close", rule_id: "r_close", active_constraint_ids: [] } },
      { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 },
    ]);
    parsePipelineV2RunState(JSON.stringify(iterationAtControl));
    const forgedIteration = JSON.parse(JSON.stringify(iterationAtControl)) as Record<string, unknown>;
    const controlExec = (forgedIteration.executions as Record<string, unknown>[])[1] as Record<string, unknown>;
    controlExec["phase"] = "failed";
    controlExec["failure_reason"] = "unknown_outcome";
    delete controlExec["result"];
    let iterationMessage: string | undefined;
    try {
      parsePipelineV2RunState(JSON.stringify(forgedIteration));
    } catch (cause) {
      iterationMessage = (cause as Error).message;
    }
    expect(iterationMessage).toContain(
      'iteration 1 of generation 1 opens at committed transition count 1, where execution 2 has failed with the "control" role',
    );
  });

  test("20. the public runtime export surfaces of the state and controller modules are unchanged", async () => {
    const stateModule = (await import("../src/pipeline_v2_state.ts")) as Record<string, unknown>;
    expect(Object.keys(stateModule).sort()).toContain("reducePipelineV2RunCommand");
    expect(Object.keys(stateModule).sort()).toContain("validatePipelineV2RunState");
    const controllerModule = (await import("../src/pipeline_v2_stage_iteration_controller.ts")) as Record<string, unknown>;
    expect(Object.keys(controllerModule).sort()).toEqual([
      "PipelineV2StageIterationControllerError",
      "ensurePipelineV2StageIteration",
    ]);
  });

  test("21. source proof: one joint replay, no second topology validator, no event journal", () => {
    const { readFileSync } = require("node:fs") as { readFileSync: (path: string, encoding: string) => string };
    const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_state.ts"), "utf8");
    // the loader keeps exactly one joint replay loop
    expect((source.match(/for \(let ordinal = 0/g) ?? []).length).toBe(1);
    // the state document carries no event journal
    expect(source.includes("events:")).toBe(false);
    expect(source.includes("event_journal")).toBe(false);
  });

  test("22. a changing-discriminator command cannot bypass the gate: exactly one kind read, typed rejection", () => {
    // On the pre-fix reducer this exact accessor sequence — create_run
    // probe, waiting probe, finalizer probe, switch kind — walked past the
    // post-failure gate and accepted a forbidden revision-1
    // task_revision_accepted on a failed planning state (revision grew,
    // the task ledger grew). The single capture routes every check through
    // the first read, so the command is now rejected by the typed gate.
    const failed = failedPlanningState();
    const before = JSON.stringify(failed);
    const sequence = ["agent_data_prepared", "agent_data_prepared", "run_failed", "task_revision_accepted"];
    let kindReads = 0;
    const hostile = new Proxy(
      { taskId: "task-b", revision: 1, taskSha256: hex("e") } as unknown as PipelineV2RunCommand,
      {
        get(target, property, receiver) {
          if (property === "kind") {
            const value = sequence[Math.min(kindReads, sequence.length - 1)];
            kindReads += 1;
            return value;
          }
          return Reflect.get(target, property, receiver);
        },
      },
    );
    let message: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, hostile, nextTick());
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain(`command ${JSON.stringify("agent_data_prepared")} rejected`);
    expect(message).toContain(GATE_MESSAGE_PART);
    expect(kindReads).toBe(1);
    // the state is byte-identical: no revision, no task ledger record
    expect(JSON.stringify(failed)).toBe(before);
    expect(failed.revision).toBe(7);
    expect(failed.task_revisions).toEqual([]);
  });

  test("23. the gate reads the discriminator exactly once: a second kind read can never happen", () => {
    const failed = failedPlanningState();
    let kindReads = 0;
    const hostile = new Proxy({} as unknown as PipelineV2RunCommand, {
      get(target, property, receiver) {
        if (property === "kind") {
          kindReads += 1;
          if (kindReads >= 2) {
            throw new Error("SECOND_KIND_READ");
          }
          return "agent_data_prepared";
        }
        return Reflect.get(target, property, receiver);
      },
    });
    let message: string | undefined;
    try {
      reducePipelineV2RunCommand(failed, hostile, nextTick());
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain(GATE_MESSAGE_PART);
    expect(message).not.toContain("SECOND_KIND_READ");
    expect(kindReads).toBe(1);
  });

  test("24. the allowed run_failed finalizer reads the discriminator exactly once", () => {
    const failed = failedStageState();
    let kindReads = 0;
    const hostile = new Proxy({ reason: "worker_failed" } as unknown as PipelineV2RunCommand, {
      get(target, property, receiver) {
        if (property === "kind") {
          kindReads += 1;
          return "run_failed";
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const finalized = reducePipelineV2RunCommand(failed, hostile, nextTick());
    expect(kindReads).toBe(1);
    expect(finalized.status).toBe("failed");
    expect(finalized.phase).toBe("finished");
    expect(finalized.failure).toEqual({ reason: "worker_failed" });
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
  });

  test("25. the allowed run_cleanup_failed finalizer reads the discriminator exactly once", () => {
    const failed = cleanupFailedState();
    let kindReads = 0;
    const hostile = new Proxy({} as unknown as PipelineV2RunCommand, {
      get(target, property, receiver) {
        if (property === "kind") {
          kindReads += 1;
          return "run_cleanup_failed";
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const finalized = reducePipelineV2RunCommand(failed, hostile, nextTick());
    expect(kindReads).toBe(1);
    expect(finalized.status).toBe("cleanup_failed");
    expect(finalized.phase).toBe("finished");
    expect(finalized.failure).toEqual({ reason: "session_cleanup_failed" });
    validatePipelineV2RunState(JSON.parse(JSON.stringify(finalized)) as never);
  });

  test("26. a discriminator accessor that throws on the first read propagates by identity and reads no payload", () => {
    const failed = failedPlanningState();
    const before = JSON.stringify(failed);
    const canary = new Error("FIRST_KIND_READ_THROWS");
    let payloadReads = 0;
    const hostile = new Proxy(
      { taskId: "task-b", revision: 1, taskSha256: hex("e") } as unknown as PipelineV2RunCommand,
      {
        get(target, property, receiver) {
          if (property === "kind") {
            throw canary;
          }
          payloadReads += 1;
          return Reflect.get(target, property, receiver);
        },
      },
    );
    let caught: unknown;
    try {
      reducePipelineV2RunCommand(failed, hostile, nextTick());
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBe(canary);
    expect(JSON.stringify(failed)).toBe(before);
    expect(payloadReads).toBe(0);
  });

  test("27. plain create_run works and reads the discriminator exactly once", () => {
    let kindReads = 0;
    const plain = new Proxy(
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] } as PipelineV2RunCommand,
      {
        get(target, property, receiver) {
          if (property === "kind") {
            kindReads += 1;
          }
          return Reflect.get(target, property, receiver);
        },
      },
    );
    const created = reducePipelineV2RunCommand(null, plain, nextTick());
    expect(kindReads).toBe(1);
    expect(created.run_id).toBe(RUN_ID);
    expect(created.revision).toBe(1);
    expect(created.status).toBe("active");
    validatePipelineV2RunState(JSON.parse(JSON.stringify(created)) as never);
  });

  test("28. plain no-state, waiting and finalized diagnostics are byte-identical", () => {
    // no-state diagnostic (the pre-switch message carries the captured kind)
    let noStateMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(null, { kind: "agent_data_prepared" }, nextTick());
    } catch (cause) {
      noStateMessage = (cause as Error).message;
    }
    expect(noStateMessage).toBe("command agent_data_prepared rejected: no pipeline v2 run state exists yet");
    // waiting diagnostic
    const waiting = build([
      { kind: "create_run", runId: RUN_ID, pipeline: PIPELINE_IDENTITY, inputs: [] },
      {
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("1"),
        actions: [{ id: "revise_task", to: "architect" }],
      },
    ]);
    expect(waiting.status).toBe("waiting");
    let waitingMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(waiting, { kind: "agent_data_prepared" }, nextTick());
    } catch (cause) {
      waitingMessage = (cause as Error).message;
    }
    expect(waitingMessage).toBe(
      `command rejected for run ${JSON.stringify(RUN_ID)} (revision 2, status waiting, phase waiting): the run is waiting for an explicit user response; only the wait response and the durable intervention records advance a waiting run`,
    );
    // finalized diagnostic
    const finalized = reducePipelineV2RunCommand(
      failedPlanningState(),
      { kind: "run_failed", reason: "worker_failed" },
      nextTick(),
    );
    expect(finalized.status).toBe("failed");
    let finalizedMessage: string | undefined;
    try {
      reducePipelineV2RunCommand(finalized, { kind: "agent_data_prepared" }, nextTick());
    } catch (cause) {
      finalizedMessage = (cause as Error).message;
    }
    expect(finalizedMessage).toBe(
      `command rejected for run ${JSON.stringify(RUN_ID)} (revision 8, status failed, phase finished): the run is already finalized with status "failed"; the terminal run status is immutable`,
    );
  });

  test("29. source pin: the reducer reads the command discriminator exactly once", () => {
    const { readFileSync } = require("node:fs") as { readFileSync: (path: string, encoding: string) => string };
    const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_state.ts"), "utf8");
    const code = stripComments(source);
    // exactly one discriminator read in production code: the capture
    expect((code.match(/command\.kind/g) ?? []).length).toBe(1);
    expect(code).toContain("const commandKind = command.kind;");
    // every consumer below the capture goes through the captured value
    expect(code).toContain("if (commandKind === \"create_run\") {");
    expect(code).toContain("switch (commandKind) {");
  });
});
