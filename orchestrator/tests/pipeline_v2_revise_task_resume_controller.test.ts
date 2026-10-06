import { describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PipelineError } from "../src/pipeline.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  PIPELINE_V2_FAILURE_REASONS,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { preparePlanRevisionManifest, prepareTaskRevisionManifest } from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { PipelineV2ReviseTaskInterventionControllerError } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ExecutionSession,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ToolSession,
} from "../src/pipeline_v2_coordinator.ts";
import { hex, startRoleArgs } from "./pipeline_v2_state_fixtures.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import {
  applyPipelineV2ReviseTaskResumeWithIo,
  productionReviseTaskResumeOps,
  type PipelineV2ReviseTaskResumeOps,
} from "../src/pipeline_v2_revise_task_resume_controller_internal.ts";
import {
  PipelineV2ReviseTaskResumeControllerError,
  resumePipelineV2RunAfterReviseTaskIntervention,
  type ApplyPipelineV2ReviseTaskResumeOptions,
} from "../src/pipeline_v2_revise_task_resume_controller.ts";

/**
 * Tests for the unwired production-neutral composition of the full
 * `revise_task` handoff: the restart-aware revise-task intervention facade
 * followed by the coordinator's resume entrypoint
 * (`resumePipelineV2RunAfterReviseTaskIntervention`). The prefix of every
 * proof is built with the real substrate itself (the run-owned project
 * copy, the run-input snapshot, the real data-plane activation preparation
 * and acceptance, the real plan acceptance, the real stage-iteration
 * controller, the real wait manifest/store) and the simulated restarts go
 * through the ordinary `PipelineV2RunStateSink.open` — never hand-built
 * snapshots. Everything is deterministic: no sleeps, no LLM, no Docker
 * Helper, no launcher credential. The controller stays unwired: the
 * runner, the CLI and the default pipeline are untouched.
 */

const PIPELINE_YAML = `
schema_version: 2
entry_state: architect
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: dev_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect
  - id: done
    type: terminal
    result: success
`;

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 9, 30, 0, 0, clock));
}

const RUN_ID = "revise-resume-run";
const TASK_BODY = "REVISED-PLAN-TASK-BODY";

// --- the honest prefix fixture (coordinates only; the pipeline is loaded
// from the durable bundle root after each restart) ---------------------------

interface ReviseResumeFixture {
  root: string;
  bundleRoot: string;
  stateRoot: string;
  runRoot: string;
  runId: string;
}

interface ReviseResumeCtx {
  fixture: ReviseResumeFixture;
  /** The durable revision at the revise wait boundary (R0). */
  revisionAtBoundary: number;
  /** The exact request manifest digest of the revise wait. */
  requestSha256: string;
  /** For the advanced (R4) fixture: the exact completed-boundary snapshot. */
  completedState?: PipelineV2RunState;
}

interface RecordingSinkShape {
  commands: PipelineV2RunCommand[];
}

class RecordingSink implements PipelineV2CoordinatorStateSink {
  readonly commands: PipelineV2RunCommand[] = [];
  snapshotReads = 0;

  constructor(private readonly inner: PipelineV2RunStateSink) {}

  get snapshot(): PipelineV2RunState | null {
    this.snapshotReads += 1;
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: PipelineV2RunCommand): Promise<void> {
    this.commands.push({ ...command });
    await this.inner.dispatch(command);
  }
}

function stateProfile(pipeline: ResolvedPipelineV2, stateId: string): string {
  const state = pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type !== "agent") {
    throw new Error(`no agent state ${stateId}`);
  }
  return state.profile;
}

function transitionTarget(pipeline: ResolvedPipelineV2, stateId: string): { to: string; index: number } {
  const state = pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type !== "agent") {
    throw new Error(`no agent state ${stateId}`);
  }
  const transition = state.transitions[0];
  if (transition === undefined) {
    throw new Error(`no transition for ${stateId}`);
  }
  return { to: transition.to, index: 0 };
}

class PrefixWorker {
  runCount = 0;
  cleanupCount = 0;

  constructor(private readonly activation: PreparedActivationData) {}

  async run(): Promise<{ status: "completed" }> {
    this.runCount += 1;
    for (const port of this.activation.output_ports) {
      if (port.type === "directory") {
        await mkdir(port.path, { recursive: true });
      } else if (port.type === "json") {
        await writeFile(port.path, JSON.stringify({ ok: true, port: port.id }));
      } else {
        await writeFile(port.path, `${port.id} body`);
      }
    }
    return { status: "completed" };
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
  }
}

async function prefixAgentStep(
  pipeline: ResolvedPipelineV2,
  sink: RecordingSink,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
): Promise<void> {
  const activation = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile: stateProfile(pipeline, stateId),
    ...startRoleArgs(pipeline, stateId, sink.snapshot),
  });
  await sink.dispatch({ kind: "agent_data_prepared" });
  await sink.dispatch({ kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` });
  await sink.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` });
  await sink.dispatch({ kind: "agent_running" });
  const worker = new PrefixWorker(activation);
  await worker.run();
  const records = await acceptActivationOutputs(pipeline, activation);
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await worker.cleanup();
  await sink.dispatch({ kind: "agent_cleanup_completed" });
  const target = transitionTarget(pipeline, stateId);
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: stateId, outcome: "completed", to: target.to, transition_index: target.index },
    executionIndex,
  });
  accepted.push(...records);
}

/**
 * The honest prefix through production facades/reducer/sink to the
 * revise_task wait boundary (R0). Returns only filesystem coordinates and
 * scalars: every pipeline load goes through the durable bundle root after
 * a restart, and no pre-restart compiled plan, intent, snapshot or
 * intervention result is ever handed back.
 */
async function reviseResumeReady(options: { advance?: "r0" | "r4" } = {}): Promise<ReviseResumeCtx> {
  const advance = options.advance ?? "r0";
  const root = await mkdtemp(join(tmpdir(), "revise-resume-"));
  try {
    const bundleRoot = join(root, "bundle");
    await mkdir(join(bundleRoot, "prompts"), { recursive: true });
        await mkdir(join(bundleRoot, "schemas"), { recursive: true });
    await writeFile(join(bundleRoot, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(bundleRoot, "pipeline.yaml"), PIPELINE_YAML);
    await writeFile(join(bundleRoot, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
    const projectSource = join(root, "project-source");
    await mkdir(projectSource, { recursive: true });
    const taskSource = join(root, "userdata", "task.md");
    await mkdir(join(root, "userdata"), { recursive: true });
    await writeFile(taskSource, "TASK-BODY\n");
    const stateRoot = join(root, "state");
    await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true });
    const runId = RUN_ID;
    const runRoot = join(stateRoot, "pipeline-runs", runId);
    await mkdir(runRoot, { mode: 0o700 });
    const fixture: ReviseResumeFixture = { root, bundleRoot, stateRoot, runRoot, runId: RUN_ID };

    // create_run with the coordinator's own project copy + input snapshot order
    const pipeline = await loadPipelineV2(bundleRoot);
    await prepareRunProject(projectSource, runRoot);
    const runInputs = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: taskSource }] as readonly RunInputBinding[],
      runRoot,
    );
    const sink = new PipelineV2RunStateSink({ stateRoot, runId, now: nextTick });
    const recording = new RecordingSink(sink);
    await recording.dispatch({
      kind: "create_run",
      runId,
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: runInputs.inputs.map((entry) => ({
        id: entry.id,
        type: entry.type,
        protected: entry.protected,
        digest: entry.digest,
      })),
    });
    const protectedInput = runInputs.inputs[0];
    if (protectedInput === undefined) {
      throw new Error("the prefix fixture lost its protected run input");
    }

    // the planning execution 1 settles unbound
    const accepted: AcceptedStateOutput[] = [];
    const activation1 = await prepareActivationData(pipeline, runInputs, accepted, "architect", 1);
    await recording.dispatch({
      kind: "start_agent_execution",
      stateId: "architect",
      profile: stateProfile(pipeline, "architect"),
      ...startRoleArgs(pipeline, "architect"),
    });
    await recording.dispatch({ kind: "agent_data_prepared" });
    await recording.dispatch({ kind: "agent_execution_session_created", sessionId: "exec-1" });
    await recording.dispatch({ kind: "agent_tool_session_created", sessionId: "tool-1" });
    await recording.dispatch({ kind: "agent_running" });
    const worker1 = new PrefixWorker(activation1);
    await worker1.run();
    const records1 = await acceptActivationOutputs(pipeline, activation1);
    await recording.dispatch({
      kind: "agent_outputs_accepted",
      outputs: records1.map((record) => ({ id: record.output, digest: record.digest })),
    });
    await worker1.cleanup();
    await recording.dispatch({ kind: "agent_cleanup_completed" });
    accepted.push(...records1);

    // the real plan r1 acceptance
    const taskA = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "PLAN-TASK-BODY",
    });
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: protectedInput.digest },
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }],
        },
      ],
    });
    const planCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: protectedInput.digest,
    });
    const acceptedPlan = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot, sink: recording, candidate: planCandidate });

    // generation 1 / iteration 1
    await ensurePipelineV2StageIteration({
      compiledPlan: acceptedPlan.compiled_plan,
      stageId: "stage-1",
      initialBudget: 2,
      sink: recording,
    });

    // the transition into the stage entry
    const architectTarget = transitionTarget(pipeline, "architect");
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: architectTarget.to, transition_index: architectTarget.index },
      executionIndex: 1,
    });

    // the stage execution 2 and the durable transition back
    await prefixAgentStep(pipeline, recording, runInputs, accepted, "dev_entry", 2);

    // the real wait request with the declared revise_task action
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await recording.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await publishPipelineV2WaitRequest(runRoot, request.manifest);

    const revisionAtBoundary = (sink.snapshot as PipelineV2RunState).revision;
    if (advance === "r0") {
      return { fixture, revisionAtBoundary, requestSha256: request.sha256 };
    }

    // the completed revise boundary through the PUBLIC intervention facade
    // on the reopened run, with the pipeline loaded from the durable bundle
    // root — the prefix helper returns no in-memory objects for reuse
    const reopened = await PipelineV2RunStateSink.open({ stateRoot, runId, now: nextTick });
    const durableBundleRoot = (reopened.snapshot as PipelineV2RunState).pipeline.bundle_root;
    const reloaded = await loadPipelineV2(durableBundleRoot);
    const interventionRecording = new RecordingSink(reopened);
    const intervention = await applyPipelineV2ReviseTaskIntervention({
      pipeline: reloaded,
      runRoot,
      sink: interventionRecording,
      runId,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: TASK_BODY,
    });
    expect(interventionRecording.commands.map((command) => command.kind)).toEqual([
      "plan_intent_accepted",
      "task_revision_accepted",
      "stage_iteration_closed",
      "wait_response_recorded",
    ]);
    const completedState = reopened.snapshot as PipelineV2RunState;
    // the identity contract of the real intervention result, proven here on
    // every fixture build: the result carries the exact sink snapshot object
    expect(intervention.state).toBe(completedState);
    void completedState;

    // the ctx carries only filesystem coordinates and scalars: no
    // pipeline, compiled plan, prepared intent, snapshot or result object
    // is ever handed back for reuse
    return { fixture, revisionAtBoundary, requestSha256: request.sha256, completedState };
  } catch (cause) {
    await rm(root, { recursive: true, force: true });
    throw cause;
  }
}

async function disposeReviseResume(ctx: ReviseResumeCtx): Promise<void> {
  await rm(ctx.fixture.root, { recursive: true, force: true });
}

/** Reopens the durable run and loads the pipeline from its durable bundle root. */
async function reopenAtR4(ctx: ReviseResumeCtx): Promise<{ opened: PipelineV2RunStateSink; recording: RecordingSink; pipeline: ResolvedPipelineV2 }> {
  const opened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
  const pipeline = await loadPipelineV2((opened.snapshot as PipelineV2RunState).pipeline.bundle_root);
  return { opened, recording: new RecordingSink(opened), pipeline };
}

// --- the fake agent runtime ---------------------------------------------------

interface FakePair {
  stateId: string;
  activationIndex: number;
  execution: FakeAgentSession;
  tool: FakeAgentSession;
}

interface FakeRuntimeHandle {
  runtime: PipelineV2AgentRuntime;
  pairs: FakePair[];
  createCalls: Array<{ stateId: string; activationIndex: number; session: "execution" | "tool" }>;
  events: string[];
}

class FakeAgentSession {
  runCount = 0;
  cleanupCount = 0;
  readonly runToolIds: string[] = [];
  private readonly log: (message: string) => void;

  constructor(
    readonly stateId: string,
    readonly kind: "execution" | "tool",
    readonly sessionId: string,
    log: (message: string) => void,
  ) {
    this.log = log;
  }

  async runAgent(toolSession: PipelineV2ToolSession): Promise<{ status: "failed"; reason: "worker_failed" }> {
    this.runCount += 1;
    this.runToolIds.push(toolSession.sessionId);
    return { status: "failed", reason: "worker_failed" as const };
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
    this.log(`cleanup-${this.kind === "execution" ? "exec" : "tool"}:${this.stateId}`);
  }
}

function fakeRuntime(): FakeRuntimeHandle {
  const pairs: FakePair[] = [];
  const createCalls: FakeRuntimeHandle["createCalls"] = [];
  const events: string[] = [];
  const logEvent = (message: string): void => {
    events.push(message);
  };
  const runtime = {
    createExecutionSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const execution = new FakeAgentSession(state.id, "execution", `resumed-exec-${pairs.length + 1}`, logEvent);
      events.push(`create-exec:${state.id}:${activation.activation_index}`);
      const tool = new FakeAgentSession(state.id, "tool", `resumed-tool-${pairs.length + 1}`, logEvent);
      events.push(`create-tool:${state.id}:${activation.activation_index}`);
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "execution" });
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "tool" });
      pairs.push({ stateId: state.id, activationIndex: activation.activation_index, execution, tool });
      return execution as unknown as PipelineV2ExecutionSession;
    },
    createToolSession: async (state: { id: string }, _activation: PreparedActivationData) => {
      const pair = pairs[pairs.length - 1];
      if (pair === undefined) {
        throw new Error("no execution session was created for this activation");
      }
      void state;
      return pair.tool as unknown as PipelineV2ToolSession;
    },
  };
  return { runtime: runtime as unknown as PipelineV2AgentRuntime, pairs, createCalls, events };
}

const NEUTRAL_CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
  freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
};

interface SpyHandoffOps {
  ops: PipelineV2ReviseTaskResumeOps;
  counts: () => Record<"applyIntervention" | "resumeRun", number>;
}

function spyHandoffOps(overrides: Partial<PipelineV2ReviseTaskResumeOps> = {}): SpyHandoffOps {
  const counts = { applyIntervention: 0, resumeRun: 0 };
  const ops: PipelineV2ReviseTaskResumeOps = {
    applyIntervention: async (options) => {
      counts.applyIntervention += 1;
      return await (overrides.applyIntervention ?? productionReviseTaskResumeOps.applyIntervention)(options);
    },
    resumeRun: async (params, control) => {
      counts.resumeRun += 1;
      return await (overrides.resumeRun ?? productionReviseTaskResumeOps.resumeRun)(params, control);
    },
  };
  return { ops, counts: () => counts };
}

function interventionOptionsOf(
  ctx: ReviseResumeCtx,
  pipeline: ResolvedPipelineV2,
  sink: unknown,
): ApplyPipelineV2ReviseTaskResumeOptions {
  return {
    pipeline,
    runRoot: ctx.fixture.runRoot,
    sink: sink as PipelineV2CoordinatorStateSink,
    runtime: fakeRuntime().runtime,
    control: NEUTRAL_CONTROL,
    runId: ctx.fixture.runId,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: TASK_BODY,
  };
}

async function catchHandoff(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectHandoffError(
  cause: unknown,
  reason: "invalid_options" | "invalid_result",
): PipelineV2ReviseTaskResumeControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReviseTaskResumeControllerError);
  const error = cause as PipelineV2ReviseTaskResumeControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

function expectResumeFailedWorkerFailed(result: PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(false);
  expect("refused" in result).toBe(false);
  if ("refused" in result || result.ok || "waiting" in result) {
    throw new Error(`expected an ordinary worker failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe("worker_failed");
  if (result.state === null) {
    throw new Error("the failure result carries no durable state");
  }
  return result.state;
}

function expectCompletedProjection(state: PipelineV2RunState, ctx?: ReviseResumeCtx): void {
  expect(state.status).toBe("failed");
  expect(state.phase).toBe("finished");
  expect(state.failure).toEqual({ reason: "worker_failed" });
  expect(state.cursor).toEqual({ current_state: "architect", transition_count: 2 });
  expect(state.transitions).toHaveLength(2);
  expect(state.executions.map((execution) => execution.index)).toEqual([1, 2, 3]);
  const execution3 = state.executions[2] as unknown as Record<string, unknown>;
  expect(execution3["state_id"]).toBe("architect");
  expect(execution3["execution_role"]).toBe("planning");
  expect(execution3["iteration_index"]).toBeUndefined();
  expect(execution3["failure_reason"]).toBe("worker_failed");
  expect(execution3["session_cleanup"]).toEqual({ execution: "completed", tool: "completed" });
  expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
    "task-a@1",
    "task-a@2",
  ]);
  expect(state.plan_revisions).toHaveLength(1);
  const generation = state.generations[0] as unknown as Record<string, unknown>;
  expect(generation["closed"]).toBeUndefined();
  expect(generation["open_iteration"]).toBeUndefined();
  expect((generation["iterations"] as Array<{ closed?: unknown }>)[0]?.closed).toEqual({
    by: "replanned",
    wait_index: 1,
    closed_transition_count: 2,
  });
  expect(state.waits[0]?.response).toEqual({ action_id: "revise_task", response_sha256: state.waits[0]!.response!.response_sha256 });
  expect(state.waits[0]?.intent).toEqual({ intent_sha256: state.waits[0]!.intent!.intent_sha256 });
  if (ctx?.completedState !== undefined) {
    expect(state.generations).toEqual(ctx.completedState.generations);
    expect(state.waits).toEqual(ctx.completedState.waits);
    expect(state.task_revisions).toEqual(ctx.completedState.task_revisions);
    expect(state.plan_revisions).toEqual(ctx.completedState.plan_revisions);
    expect(state.grants).toEqual(ctx.completedState.grants);
  }
}

describe("resumePipelineV2RunAfterReviseTaskIntervention", () => {
  test("1. honest C0 through the public facade: the exact four-command intervention suffix then the exact seven-command resume", async () => {
    const ctx = await reviseResumeReady();
    try {
      // the pipeline for the handoff call is loaded from the durable bundle
      // root; the prefix helper returned no pipeline object
      const reopened = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: ctx.fixture.runId,
        now: nextTick,
      });
      const durableBundleRoot = (reopened.snapshot as PipelineV2RunState).pipeline.bundle_root;
      const pipeline = await loadPipelineV2(durableBundleRoot);
      const recording = new RecordingSink(reopened);
      const fake = fakeRuntime();
      const result = await resumePipelineV2RunAfterReviseTaskIntervention({
        pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runtime: fake.runtime,
        control: NEUTRAL_CONTROL,
        runId: ctx.fixture.runId,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: TASK_BODY,
      });
      const state = expectResumeFailedWorkerFailed(result);
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "plan_intent_accepted",
        "task_revision_accepted",
        "stage_iteration_closed",
        "wait_response_recorded",
        "start_agent_execution",
        "agent_data_prepared",
        "agent_execution_session_created",
        "agent_tool_session_created",
        "agent_running",
        "agent_failed",
        "run_failed",
      ]);
      const firstCommand = recording.commands[4] as unknown as Record<string, unknown>;
      expect(firstCommand["stateId"]).toBe("architect");
      expect(firstCommand["executionRole"]).toBe("planning");
      expect("iterationIndex" in firstCommand).toBe(false);
      // exactly one new session pair, each cleanup exactly once, tool first
      expect(fake.createCalls).toEqual([
        { stateId: "architect", activationIndex: 3, session: "execution" },
        { stateId: "architect", activationIndex: 3, session: "tool" },
      ]);
      expect(fake.pairs).toHaveLength(1);
      expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
      expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
      expect(fake.pairs[0]?.execution.runCount).toBe(1);
      expect(fake.events).toEqual([
        "create-exec:architect:3",
        "create-tool:architect:3",
        "cleanup-tool:architect",
        "cleanup-exec:architect",
      ]);
      // the revision grew by exactly 11 (4 intervention + 7 resume)
      expect(state.revision).toBe(ctx.revisionAtBoundary + 11);
      expectCompletedProjection(state, ctx);
      // the coordinator result carries the authoritative reopened snapshot
      expect(result.state).toBe(reopened.snapshot);
      // loader round-trip of the final durable state
      const durableBytes = await readFile(join(ctx.fixture.runRoot, "state.json"));
      expect(await parsePipelineV2RunState(durableBytes.toString())).toEqual(state);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- crash seam / R4 ---------------------------------------------------------

  test("2. crash seam: the durable intervention survives the resume sentinel; the fresh call resumes with zero intervention dispatch", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      // the first handoff call: the intervention recognizes the exact
      // completed boundary with zero dispatch; the injected resume throws a
      // sentinel before any resume effect
      const opened1 = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      const recording1 = new RecordingSink(opened1);
      const pipeline1 = await loadPipelineV2((opened1.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const sentinel = new Error("RESUME-SENTINEL");
      const faultedOps: PipelineV2ReviseTaskResumeOps = {
        ...productionReviseTaskResumeOps,
        resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
          throw sentinel;
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const cause1 = await catchHandoff(() =>
        applyPipelineV2ReviseTaskResumeWithIo(faultedOps, interventionOptionsOf(ctx, pipeline1, recording1)),
      );
      expect(cause1).toBe(sentinel);
      expect(recording1.commands).toEqual([]);
      expect((opened1.snapshot as PipelineV2RunState).revision).toBe((ctx.completedState as PipelineV2RunState).revision);

      // the ordinary reopen: the intervention is already fully durable
      const opened2 = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      expect((opened2.snapshot as PipelineV2RunState)).toEqual(ctx.completedState as PipelineV2RunState);
      const pipeline2 = await loadPipelineV2((opened2.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const recording2 = new RecordingSink(opened2);
      const fake = fakeRuntime();
      const result = await resumePipelineV2RunAfterReviseTaskIntervention({
        pipeline: pipeline2,
        runRoot: ctx.fixture.runRoot,
        sink: recording2,
        runtime: fake.runtime,
        control: NEUTRAL_CONTROL,
        runId: ctx.fixture.runId,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: TASK_BODY,
      });
      const state = expectResumeFailedWorkerFailed(result);
      // only the seven resume commands; the intervention records are not
      // rewritten
      expect(recording2.commands.map((command) => command.kind)).toEqual([
        "start_agent_execution",
        "agent_data_prepared",
        "agent_execution_session_created",
        "agent_tool_session_created",
        "agent_running",
        "agent_failed",
        "run_failed",
      ]);
      expect(state.revision).toBe(ctx.revisionAtBoundary + 11);
      expectCompletedProjection(state, ctx);
      expect(result.state).toBe(opened2.snapshot);
      const durableBytes = await readFile(join(ctx.fixture.runRoot, "state.json"));
      expect(await parsePipelineV2RunState(durableBytes.toString())).toEqual(state);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- identity pass-through ---------------------------------------------------

  test("3. an intervention typed error passes through by identity; the resume never starts", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const interventionError = new PipelineV2ReviseTaskInterventionControllerError(
        "invalid_state",
        "injected intervention failure",
        null,
      );
      let resumeCalls = 0;
      const fakeOps: PipelineV2ReviseTaskResumeOps = {
        applyIntervention: (async (): Promise<unknown> => {
          throw interventionError;
        }) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
        resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
          resumeCalls += 1;
          throw new Error("RESUME-NOT-EXPECTED");
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const { opened, recording, pipeline } = await reopenAtR4(ctx);
      const cause = await catchHandoff(() =>
        applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording)),
      );
      expect(cause).toBe(interventionError);
      expect(resumeCalls).toBe(0);
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("4. a resume thrown error passes through by identity", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const opened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      const recording = new RecordingSink(opened);
      const pipeline = await loadPipelineV2((opened.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const thrown = new Error("RESUME-EXPLODED");
      const fakeOps: PipelineV2ReviseTaskResumeOps = {
        ...productionReviseTaskResumeOps,
        resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
          throw thrown;
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const cause = await catchHandoff(() =>
        applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording)),
      );
      expect(cause).toBe(thrown);
      // the intervention recognized the completed boundary with zero dispatch
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- malformed intervention results and durable-binding near-misses ----------

  test("5. a malformed intervention result or a hostile durable boundary is the layer's invalid_result; the resume never starts", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const completed = ctx.completedState as PipelineV2RunState;
      const mutate = (fn: (record: Record<string, unknown>) => void): PipelineV2RunState => {
        const clone = structuredClone(completed) as unknown as Record<string, unknown>;
        fn(clone);
        return clone as unknown as PipelineV2RunState;
      };
      const fakeResultWith = (state: unknown, overrides: Record<string, unknown> = {}): unknown => ({
        wait_index: 1,
        intent_sha256: (completed.waits[0] as { intent: { intent_sha256: string } }).intent.intent_sha256,
        request_sha256: (completed.waits[0] as { request_sha256: string }).request_sha256,
        response_sha256: (completed.waits[0] as { response: { response_sha256: string } }).response.response_sha256,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: (completed.task_revisions[completed.task_revisions.length - 1] as { sha256: string }).sha256,
        generation_index: 1,
        iteration_index: 1,
        action_id: "revise_task",
        action_to: "architect",
        state,
        ...overrides,
      });

      // (a) the malformed flat-result matrix: the fake result carries the
      // authoritative sink snapshot (so the identity check passes and the
      // flat-field checks are the ones that fire); the last case is the
      // explicit structural-clone identity violation
      const { opened, recording, pipeline } = await reopenAtR4(ctx);
      const snapshotOf = (options: unknown): unknown =>
        (options as { sink: { snapshot: unknown } }).sink.snapshot;
      const malformedOverrides: Array<Record<string, unknown>> = [
        { wait_index: 2 },
        { task_id: "task-b" },
        { task_revision: 0 },
        { intent_sha256: "NOT-A-DIGEST" },
        { request_sha256: "NOT-A-DIGEST" },
        { response_sha256: "NOT-A-DIGEST" },
        { task_sha256: "NOT-A-DIGEST" },
        { action_id: "continue_stage" },
        { action_to: "" },
        { generation_index: 0 },
        { iteration_index: 0 },
      ];
      for (const overrides of malformedOverrides) {
        let resumeCalls = 0;
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((options: unknown) =>
            Promise.resolve(fakeResultWith(snapshotOf(options), overrides))) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
            resumeCalls += 1;
            throw new Error("RESUME-NOT-EXPECTED");
          }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording)),
        );
        const error = expectHandoffError(cause, "invalid_result");
        expect(error.message.length).toBeGreaterThan(0);
        expect(resumeCalls).toBe(0);
      }
      // the identity violations: a bare record and a structural clone
      const identityViolations: Array<(options: unknown) => unknown> = [
        () => ({ state: completed }),
        (options: unknown) => fakeResultWith(structuredClone(snapshotOf(options))),
      ];
      for (const build of identityViolations) {
        let resumeCalls = 0;
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((options: unknown) => Promise.resolve(build(options))) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
            resumeCalls += 1;
            throw new Error("RESUME-NOT-EXPECTED");
          }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording)),
        );
        const error = expectHandoffError(cause, "invalid_result");
        expect(error.message.length).toBeGreaterThan(0);
        expect(resumeCalls).toBe(0);
      }

      // (b) the durable-binding near-miss matrix through a hostile sink
      // snapshot; the fake result carries the hostile snapshot by identity,
      // so the targeted durable-binding checks are the ones that fire
      const hostileSinkWith = (state: PipelineV2RunState): PipelineV2CoordinatorStateSink => ({
        get snapshot(): PipelineV2RunState | null {
          return state;
        },
        get poisoned(): boolean {
          return false;
        },
        dispatch: async (): Promise<void> => {
          throw new Error("CANARY-DISPATCH");
        },
      });
      const lastTaskRecord = completed.task_revisions[completed.task_revisions.length - 1] as { index: number };
      const nearMisses: Array<[string, PipelineV2RunState]> = [
        ["a foreign run id", mutate((record) => { record["run_id"] = "other-run"; })],
        ["a waiting status", mutate((record) => { record["status"] = "waiting"; record["phase"] = "waiting"; })],
        ["a cursor off the routing target", mutate((record) => { (record["cursor"] as Record<string, unknown>)["current_state"] = "dev_entry"; })],
        ["a cursor off the wait boundary", mutate((record) => { (record["cursor"] as Record<string, unknown>)["transition_count"] = 3; })],
        ["a continue_stage response", mutate((record) => { ((record["waits"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["response"] = { action_id: "continue_stage", response_sha256: "f".repeat(64) }; })],
        ["a foreign response digest", mutate((record) => { ((record["waits"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["response"] = { action_id: "revise_task", response_sha256: "e".repeat(64) }; })],
        ["a foreign intent digest", mutate((record) => { ((record["waits"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["intent"] = { intent_sha256: "d".repeat(64) }; })],
        ["a foreign request digest", mutate((record) => { ((record["waits"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["request_sha256"] = "c".repeat(64); })],
        ["a shifted wait index", mutate((record) => { ((record["waits"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["index"] = 2; })],
        ["a mismatching task revision", mutate((record) => { ((record["task_revisions"] as Array<Record<string, unknown>>)[1] as Record<string, unknown>)["revision"] = 3; })],
        ["a later revision of the revised task", mutate((record) => { record["task_revisions"] = [...(record["task_revisions"] as Array<Record<string, unknown>>), { index: (lastTaskRecord as { index: number }).index + 1, task_id: "task-a", revision: 3, sha256: "b".repeat(64), previous_sha256: "a".repeat(64), wait_index: 1, intent_sha256: "c".repeat(64) }]; })],
        ["a closed generation", mutate((record) => { ((record["generations"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["closed"] = { by: "next_stage", closed_transition_count: 2 }; })],
        ["a grant-closed iteration", mutate((record) => { (((record["generations"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["iterations"] as Array<Record<string, unknown>>)[0]!["closed"] = { by: "grant", wait_index: 1, closed_transition_count: 2 }; })],
        ["an open successor iteration", mutate((record) => { ((record["generations"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["open_iteration"] = { index: 2, opened_transition_count: 2 }; })],
        ["a foreign generation plan binding", mutate((record) => { ((record["generations"] as Array<Record<string, unknown>>)[0] as Record<string, unknown>)["plan_sha256"] = "9".repeat(64); })],
        ["a new plan revision", mutate((record) => { record["plan_revisions"] = [...(record["plan_revisions"] as Array<{ sha256: string }>), { index: 2, revision: 2, sha256: "8".repeat(64), previous_sha256: (record["plan_revisions"] as Array<{ sha256: string }>)[0]!.sha256, origin_execution: 3 }]; })],
        ["a new execution", mutate((record) => { record["executions"] = [...(record["executions"] as Array<Record<string, unknown>>), { index: 3, type: "agent", state_id: "architect", attempt: 1, profile: "coder", execution_role: "planning", phase: "started" }]; })],
        ["a new transition", mutate((record) => { record["transitions"] = [...(record["transitions"] as Array<Record<string, unknown>>), { index: 0, from: "architect", outcome: "completed", to: "dev_entry", execution_index: 3 }]; })],
        ["a new wait record", mutate((record) => { record["waits"] = [...(record["waits"] as Array<Record<string, unknown>>), { index: 2, transition_count: 2, state_id: "architect", reason: "stage_iteration_limit_exhausted", request_sha256: "7".repeat(64), actions: [{ id: "continue_stage", to: "dev_entry" }] }]; })],
        ["a duplicate wait record", mutate((record) => { record["waits"] = [...(record["waits"] as Array<Record<string, unknown>>), { index: 1, transition_count: 2, state_id: "architect", reason: "stage_iteration_limit_exhausted", request_sha256: "7".repeat(64), actions: [{ id: "revise_task", to: "architect" }] }]; })],
        ["a malformed state shape", mutate((record) => { record["waits"] = null; })],
      ];
      for (const [label, hostileState] of nearMisses) {
        let resumeCalls = 0;
        const hostileSink = hostileSinkWith(hostileState);
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((options: unknown) =>
            Promise.resolve(fakeResultWith(snapshotOf(options)))) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
            resumeCalls += 1;
            throw new Error("RESUME-NOT-EXPECTED");
          }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, hostileSink)),
        );
        const error = expectHandoffError(cause, "invalid_result");
        expect(error.state).toBe(hostileState);
        expect(resumeCalls).toBe(0);
        expect(label.length).toBeGreaterThan(0);
      }

      // (c) the corrective battery: one extra own enumerable field on the
      // flat result, and the wait-bound task revision's previous digest or
      // ledger index mutated — every case the layer's own invalid_result
      // with zero resume calls, never a TypeError, and content-free
      // diagnostics (the fake resume would return a valid success if the
      // verification let it through)
      const correctiveCases: Array<[string, (options: unknown) => unknown, PipelineV2RunState, string]> = [
        ["a waiting result with an extra own field",
          (options: unknown) => ({ ok: false, waiting: true, state: snapshotOf(options), hostile_extra: "NEVER-READ" }),
          completed,
          "carries foreign fields"],
        ["a waiting result with a false discriminant",
          (options: unknown) => ({ ok: false, waiting: false, state: snapshotOf(options) }),
          completed,
          "carries foreign fields"],
        ["a waiting result without a state",
          () => ({ ok: false, waiting: true }),
          completed,
          "carries foreign fields"],
        ["an extra own enumerable field",
          (options: unknown) => ({ ...(fakeResultWith(snapshotOf(options)) as Record<string, unknown>), hostile_extra: "NEVER-READ" }),
          completed,
          "carries foreign fields"],
        ["a mutated previous digest",
          (options: unknown) => fakeResultWith(snapshotOf(options)),
          mutate((record) => { ((record["task_revisions"] as Array<Record<string, unknown>>)[1] as Record<string, unknown>)["previous_sha256"] = "9".repeat(64); }),
          "does not chain to its recorded predecessor"],
        ["a mutated ledger index",
          (options: unknown) => fakeResultWith(snapshotOf(options)),
          mutate((record) => { ((record["task_revisions"] as Array<Record<string, unknown>>)[1] as Record<string, unknown>)["index"] = 9; }),
          "does not carry its ledger position"],
      ];
      for (const [label, buildResult, hostileState, messagePart] of correctiveCases) {
        let resumeCalls = 0;
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((options: unknown) => Promise.resolve(buildResult(options))) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: ((params: unknown): Promise<PipelineV2ResumeCoordinationResult> => {
            resumeCalls += 1;
            const successState = (params as { sink: { snapshot: unknown } }).sink.snapshot;
            return Promise.resolve({ ok: true as const, state: successState }) as unknown as Promise<PipelineV2ResumeCoordinationResult>;
          }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, hostileSinkWith(hostileState))),
        );
        const error = expectHandoffError(cause, "invalid_result");
        expect(error.state).toBe(hostileState);
        expect(error.message).toContain(messagePart);
        expect(resumeCalls).toBe(0);
        expect(cause).not.toBeInstanceOf(TypeError);
        expect(error.message).not.toContain("hostile_extra");
        expect(error.message).not.toContain("9".repeat(64));
        expect(label.length).toBeGreaterThan(0);
      }
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("6. the honest multi-task ledger with a non-adjacent predecessor is accepted and returned by identity", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const { opened, pipeline } = await reopenAtR4(ctx);
      const completed = ctx.completedState as PipelineV2RunState;
      const mutate = (fn: (record: Record<string, unknown>) => void): PipelineV2RunState => {
        const clone = structuredClone(completed) as unknown as Record<string, unknown>;
        fn(clone);
        return clone as unknown as PipelineV2RunState;
      };
      // task-a@2's predecessor task-a@1 sits two records back: an honest
      // task-b revision was accepted in between; the ledger position and
      // the same-task revision chain stay exact
      const originalTaskRecord = completed.task_revisions[completed.task_revisions.length - 1] as unknown as Record<string, unknown>;
      const firstRecord = completed.task_revisions[0] as unknown as Record<string, unknown>;
      const multiTaskState = mutate((record) => {
        record["task_revisions"] = [
          firstRecord,
          { index: 2, task_id: "task-b", revision: 1, sha256: "b".repeat(64), previous_sha256: null },
          { ...originalTaskRecord, index: 3 },
        ];
      });
      const flatFrom = (state: unknown): Record<string, unknown> => {
        const record = state as PipelineV2RunState;
        const wait = record.waits[0] as { intent: { intent_sha256: string }; request_sha256: string; response: { response_sha256: string } };
        const taskRecord = record.task_revisions[record.task_revisions.length - 1] as { sha256: string };
        return {
          wait_index: 1,
          intent_sha256: wait.intent.intent_sha256,
          request_sha256: wait.request_sha256,
          response_sha256: wait.response.response_sha256,
          task_id: "task-a",
          task_revision: 2,
          task_sha256: taskRecord.sha256,
          generation_index: 1,
          iteration_index: 1,
          action_id: "revise_task",
          action_to: "architect",
          state,
        };
      };
      let resumeCalls = 0;
      const fakeOps: PipelineV2ReviseTaskResumeOps = {
        applyIntervention: ((options: unknown) =>
          Promise.resolve(flatFrom((options as { sink: { snapshot: unknown } }).sink.snapshot))) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
        resumeRun: ((params: unknown): Promise<PipelineV2ResumeCoordinationResult> => {
          resumeCalls += 1;
          const successState = (params as { sink: { snapshot: unknown } }).sink.snapshot;
          return Promise.resolve({ ok: true as const, state: successState }) as unknown as Promise<PipelineV2ResumeCoordinationResult>;
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const hostileSink: PipelineV2CoordinatorStateSink = {
        get snapshot(): PipelineV2RunState | null {
          return multiTaskState;
        },
        get poisoned(): boolean {
          return false;
        },
        dispatch: async (): Promise<void> => {
          throw new Error("CANARY-DISPATCH");
        },
      };
      const result = await applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, hostileSink));
      expect(result.ok).toBe(true);
      expect((result as { state: unknown }).state).toBe(multiTaskState);
      expect(resumeCalls).toBe(1);
      expect((opened.snapshot as PipelineV2RunState).revision).toBe(completed.revision);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- the resume union contract -----------------------------------------------

  test("7. the resume union is verified defensively and returned by identity", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const { opened, recording, pipeline } = await reopenAtR4(ctx);
      const authoritative = opened.snapshot as PipelineV2RunState;
      // the honest real intervention result captured once (zero dispatch on
      // the completed boundary); every fake call reuses it for the
      // successful-verification prefix
      const honestIntervention = await applyPipelineV2ReviseTaskIntervention({
        pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: opened,
        runId: ctx.fixture.runId,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: TASK_BODY,
      });
      expect(recording.commands).toEqual([]);
      const hostiles: unknown[] = [
        null,
        "result",
        [],
        { ok: true, state: authoritative, refused: false },
        { ok: true, state: structuredClone(authoritative) },
        { ok: "yes", state: authoritative },
        { ok: true },
        { ok: false, refused: true, reason: "invalid_state", state: authoritative, extra: 1 },
        { ok: false, refused: false, reason: "invalid_state", state: authoritative },
        { ok: false, refused: true, reason: "not-a-reason", state: authoritative },
        { ok: false, refused: true, reason: "invalid_state", state: structuredClone(authoritative) },
        { ok: false, reason: "worker_failed", state: authoritative, extra: 1 },
        { ok: false, reason: "not-a-reason", state: authoritative },
        { ok: false, reason: "worker_failed", state: structuredClone(authoritative) },
        { ok: false, reason: "worker_failed" },
      ];
      for (const hostile of hostiles) {
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((): Promise<unknown> => Promise.resolve(honestIntervention)) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: ((): Promise<unknown> => Promise.resolve(hostile)) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording)),
        );
        const error = expectHandoffError(cause, "invalid_result");
        expect(error.state).toBe(authoritative);
      }

      // the positive vocabulary table: every valid coordinator result is
      // returned unchanged by identity
      const results: unknown[] = [
        { ok: true, state: authoritative },
        { ok: false, waiting: true, state: authoritative },
      ];
      for (const reason of ["missing_state", "sink_poisoned", "run_id_mismatch", "invalid_state", "pipeline_mismatch", "run_layout_invalid", "run_input_modified", "accepted_output_modified", "internal_error"] as const) {
        results.push({ ok: false, refused: true, reason, state: null });
        results.push({ ok: false, refused: true, reason, state: authoritative });
      }
      for (const reason of PIPELINE_V2_FAILURE_REASONS) {
        results.push({ ok: false, reason, state: null });
        results.push({ ok: false, reason, state: authoritative });
      }
      for (const valid of results) {
        const fakeOps: PipelineV2ReviseTaskResumeOps = {
          applyIntervention: ((): Promise<unknown> => Promise.resolve(honestIntervention)) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
          resumeRun: ((): Promise<unknown> => Promise.resolve(valid)) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
        };
        const returned = await applyPipelineV2ReviseTaskResumeWithIo(fakeOps, interventionOptionsOf(ctx, pipeline, recording));
        expect(returned).toBe(valid as PipelineV2ResumeCoordinationResult);
      }
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- capture and provenance ---------------------------------------------------

  test("8. capture boundary: options read once in the fixed order, hostile extras unread, ops members once", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const reads: string[] = [];
      const openedReal = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      const openedPipeline = await loadPipelineV2((openedReal.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const innerSink = new RecordingSink(openedReal);
      const innerRuntime = fakeRuntime();
      const readsRuntime = {
        get createExecutionSession() {
          reads.push("runtime.createExecutionSession");
          return innerRuntime.runtime.createExecutionSession;
        },
        get createToolSession() {
          reads.push("runtime.createToolSession");
          return innerRuntime.runtime.createToolSession;
        },
      };
      const readsControl = {
        get currentSignal() {
          reads.push("control.currentSignal");
          return NEUTRAL_CONTROL.currentSignal;
        },
        get freezeSignal() {
          reads.push("control.freezeSignal");
          return NEUTRAL_CONTROL.freezeSignal;
        },
      };
      const proxied = new Proxy(
        {} as Record<string, unknown>,
        {
          get(_target, prop: string) {
            reads.push(prop);
            if (prop === "pipeline") {
              return openedPipeline;
            }
            if (prop === "runRoot") {
              return ctx.fixture.runRoot;
            }
            if (prop === "sink") {
              return innerSink;
            }
            if (prop === "runtime") {
              return readsRuntime;
            }
            if (prop === "control") {
              return readsControl;
            }
            if (prop === "runId") {
              return ctx.fixture.runId;
            }
            if (prop === "waitIndex") {
              return 1;
            }
            if (prop === "taskId") {
              return "task-a";
            }
            if (prop === "taskBody") {
              return TASK_BODY;
            }
            return "HOSTILE-EXTRA";
          },
        },
      );
      const result = await resumePipelineV2RunAfterReviseTaskIntervention(proxied as unknown as ApplyPipelineV2ReviseTaskResumeOptions);
      expectResumeFailedWorkerFailed(result);
      expect(reads.slice(0, 9)).toEqual([
        "pipeline",
        "runRoot",
        "sink",
        "runtime",
        "control",
        "runId",
        "waitIndex",
        "taskId",
        "taskBody",
      ]);
      expect(reads.filter((read) => read === "runtime.createExecutionSession")).toHaveLength(1);
      expect(reads.filter((read) => read === "runtime.createToolSession")).toHaveLength(1);
      expect(reads.filter((read) => read === "control.currentSignal")).toHaveLength(1);
      expect(reads.filter((read) => read === "control.freezeSignal")).toHaveLength(1);
      expect(reads).not.toContain("evil");
      expect(reads).not.toContain("intent");
      expect(reads).not.toContain("initialBudget");

      // the ops members are read exactly once; the first part's handoff
      // already finalized the first fixture's run, so this part builds its
      // own fresh fixture
      const ctx2 = await reviseResumeReady({ advance: "r4" });
      try {
        const opsGets: Record<string, number> = {};
        const proxiedOps = new Proxy(productionReviseTaskResumeOps, {
          get(target, prop: string) {
            opsGets[prop] = (opsGets[prop] ?? 0) + 1;
            return target[prop as keyof PipelineV2ReviseTaskResumeOps];
          },
        });
        const opened = await PipelineV2RunStateSink.open({ stateRoot: ctx2.fixture.stateRoot, runId: ctx2.fixture.runId, now: nextTick });
        const recording = new RecordingSink(opened);
        const pipeline = await loadPipelineV2((opened.snapshot as PipelineV2RunState).pipeline.bundle_root);
        await applyPipelineV2ReviseTaskResumeWithIo(proxiedOps, interventionOptionsOf(ctx2, pipeline, recording));
        expect(opsGets["applyIntervention"]).toBe(1);
        expect(opsGets["resumeRun"]).toBe(1);
      } finally {
        await disposeReviseResume(ctx2);
      }
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("9. the controller reads the authoritative snapshot exactly once per verification phase", async () => {
    const ctx = await reviseResumeReady();
    try {
      // the fake intervention dispatches the real four commands through the
      // recording sink and reads its own state through the raw inner sink,
      // so the recording wrapper's snapshot reads are the controller's alone
      const reopenedReal = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      const reopened = new RecordingSink(reopenedReal);
      const pipeline = await loadPipelineV2((reopenedReal.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const intentDigest = hex("e");
      const taskDigest = hex("f");
      const responseDigest = hex("d");
      const ops: PipelineV2ReviseTaskResumeOps = {
        applyIntervention: (async () => {
          await reopened.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: intentDigest });
          await reopened.dispatch({
            kind: "task_revision_accepted",
            taskId: "task-a",
            revision: 2,
            taskSha256: taskDigest,
            waitIndex: 1,
            intentSha256: intentDigest,
          });
          await reopened.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 });
          await reopened.dispatch({
            kind: "wait_response_recorded",
            waitIndex: 1,
            expectedRequestSha256: ctx.requestSha256,
            actionId: "revise_task",
            responseSha256: responseDigest,
          });
          return {
            wait_index: 1,
            intent_sha256: intentDigest,
            request_sha256: ctx.requestSha256,
            response_sha256: responseDigest,
            task_id: "task-a",
            task_revision: 2,
            task_sha256: taskDigest,
            generation_index: 1,
            iteration_index: 1,
            action_id: "revise_task",
            action_to: "architect",
            state: (reopenedReal.snapshot as PipelineV2RunState),
          };
        }) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
        resumeRun: ((): Promise<PipelineV2ResumeCoordinationResult> =>
          Promise.resolve({ ok: true as const, state: reopenedReal.snapshot as PipelineV2RunState })) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const result = await applyPipelineV2ReviseTaskResumeWithIo(ops, interventionOptionsOf(ctx, pipeline, reopened));
      expect(result.ok).toBe(true);
      // exactly one read after the intervention and one after the resume
      expect(reopened.snapshotReads).toBe(2);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("10. caller mutation after the pending intervention cannot change the resume", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      let releaseIntervention!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseIntervention = resolve;
      });
      let createCalls = 0;
      const innerRuntime = fakeRuntime();
      const hostileRuntime = {
        createExecutionSession: async (): Promise<unknown> => {
          createCalls += 1;
          throw new Error("HOSTILE-RUNTIME-REPLACED");
        },
        createToolSession: innerRuntime.runtime.createToolSession,
      };
      let signalReads = 0;
      const hostileControl = {
        currentSignal: (): "SIGINT" | "SIGTERM" | null => {
          signalReads += 1;
          return "SIGINT";
        },
        freezeSignal: (): "SIGINT" | "SIGTERM" | null => "SIGINT",
      };
      const { ops } = spyHandoffOps({
        applyIntervention: (async (options: unknown) => {
          await gate;
          return await productionReviseTaskResumeOps.applyIntervention(options as Parameters<PipelineV2ReviseTaskResumeOps["applyIntervention"]>[0]);
        }) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
      });
      const opened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: ctx.fixture.runId, now: nextTick });
      const recording = new RecordingSink(opened);
      const pipeline = await loadPipelineV2((opened.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const options = {
        pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runtime: innerRuntime.runtime,
        control: NEUTRAL_CONTROL,
        runId: ctx.fixture.runId,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: TASK_BODY,
      };
      const pending = applyPipelineV2ReviseTaskResumeWithIo(ops, options);
      options.runtime = hostileRuntime as unknown as PipelineV2AgentRuntime;
      options.control = hostileControl as unknown as PipelineV2CoordinatorControl;
      options.taskBody = "MUTATED-AFTER-START";
      options.runId = "other-run";
      releaseIntervention();
      const result = (await pending) as PipelineV2ResumeCoordinationResult;
      const state = expectResumeFailedWorkerFailed(result);
      // the captured runtime/control adapters drove the resume; the
      // hostile replacements were never called
      expect(createCalls).toBe(0);
      expect(signalReads).toBe(0);
      expect(state.failure).toEqual({ reason: "worker_failed" });
      expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
        "task-a@1",
        "task-a@2",
      ]);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("11. a forged pipeline fails the provenance gate before any facade call; a Proxy pipeline causes no traps", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      const { opened, recording, pipeline } = await reopenAtR4(ctx);
      const clone = { ...pipeline } as unknown as ResolvedPipelineV2;
      let facadeCalls = 0;
      const fakeOps: PipelineV2ReviseTaskResumeOps = {
        applyIntervention: (async (): Promise<unknown> => {
          facadeCalls += 1;
          throw new Error("INTERVENTION-NOT-EXPECTED");
        }) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
        resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
          facadeCalls += 1;
          throw new Error("RESUME-NOT-EXPECTED");
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const clonedCause = await catchHandoff(() =>
        applyPipelineV2ReviseTaskResumeWithIo(fakeOps, { ...interventionOptionsOf(ctx, pipeline, recording), pipeline: clone }),
      );
      expect(clonedCause).toBeInstanceOf(PipelineError);
      expect(facadeCalls).toBe(0);

      let traps = 0;
      const proxy = new Proxy(pipeline as unknown as Record<string, unknown>, {
        get() {
          traps += 1;
          return undefined;
        },
      }) as unknown as ResolvedPipelineV2;
      const proxiedCause = await catchHandoff(() =>
        applyPipelineV2ReviseTaskResumeWithIo(fakeOps, { ...interventionOptionsOf(ctx, pipeline, recording), pipeline: proxy }),
      );
      expect(proxiedCause).toBeInstanceOf(PipelineError);
      expect(traps).toBe(0);
      expect(facadeCalls).toBe(0);
      // the run is untouched by both failures
      expect(recording.commands).toEqual([]);
      expect((opened.snapshot as PipelineV2RunState).revision).toBe((ctx.completedState as PipelineV2RunState).revision);
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  test("12. invalid runtime or control contract refuses before any facade call", async () => {
    const ctx = await reviseResumeReady({ advance: "r4" });
    try {
      let facadeCalls = 0;
      const fakeOps: PipelineV2ReviseTaskResumeOps = {
        applyIntervention: (async (): Promise<unknown> => {
          facadeCalls += 1;
          throw new Error("INTERVENTION-NOT-EXPECTED");
        }) as unknown as PipelineV2ReviseTaskResumeOps["applyIntervention"],
        resumeRun: (async (): Promise<PipelineV2ResumeCoordinationResult> => {
          facadeCalls += 1;
          throw new Error("RESUME-NOT-EXPECTED");
        }) as unknown as PipelineV2ReviseTaskResumeOps["resumeRun"],
      };
      const brokenShapes: Array<[string, Record<string, unknown>, string]> = [
        ["runtime without createExecutionSession", { runtime: { createToolSession: () => undefined }, control: NEUTRAL_CONTROL }, "createExecutionSession must be a function"],
        ["runtime without createToolSession", { runtime: { createExecutionSession: () => undefined }, control: NEUTRAL_CONTROL }, "createToolSession must be a function"],
        ["control without currentSignal", { runtime: fakeRuntime().runtime, control: { freezeSignal: () => null } }, "currentSignal must be a function"],
        ["control without freezeSignal", { runtime: fakeRuntime().runtime, control: { currentSignal: () => null } }, "freezeSignal must be a function"],
        ["runtime not a record", { runtime: "runtime", control: NEUTRAL_CONTROL }, "requires an agent runtime object"],
        ["control not a record", { runtime: fakeRuntime().runtime, control: null }, "requires a signal control object"],
      ];
      for (const [label, extra, messagePart] of brokenShapes) {
        const { opened, recording, pipeline } = await reopenAtR4(ctx);
        const base = interventionOptionsOf(ctx, pipeline, recording) as unknown as Record<string, unknown>;
        const options = { ...base, ...extra };
        const cause = await catchHandoff(() =>
          applyPipelineV2ReviseTaskResumeWithIo(fakeOps, options as unknown as ApplyPipelineV2ReviseTaskResumeOptions),
        );
        const error = expectHandoffError(cause, "invalid_options");
        expect(error.message).toContain(messagePart);
        expect(facadeCalls).toBe(0);
        expect(recording.commands).toEqual([]);
        expect(label.length).toBeGreaterThan(0);
      }
    } finally {
      await disposeReviseResume(ctx);
    }
  });

  // --- export surfaces and source scan ------------------------------------------

  test("13. the runtime export surfaces are exactly the contracted keys", async () => {
    const publicModule = await import("../src/pipeline_v2_revise_task_resume_controller.ts");
    expect(Object.keys(publicModule).sort()).toEqual([
      "PipelineV2ReviseTaskResumeControllerError",
      "resumePipelineV2RunAfterReviseTaskIntervention",
    ]);
    const internalModule = await import("../src/pipeline_v2_revise_task_resume_controller_internal.ts");
    expect(Object.keys(internalModule).sort()).toEqual([
      "PipelineV2ReviseTaskResumeControllerError",
      "applyPipelineV2ReviseTaskResumeWithIo",
      "productionReviseTaskResumeOps",
    ]);
    expect(Object.isFrozen(productionReviseTaskResumeOps)).toBe(true);
    expect(productionReviseTaskResumeOps.applyIntervention).toBe(applyPipelineV2ReviseTaskIntervention);
    expect(productionReviseTaskResumeOps.resumeRun).toBe(resumePipelineV2Run);
  });

  test("14. the controller composes the two facades only (source scan)", async () => {
    const { readFile: readSource } = await import("node:fs/promises");
    for (const name of [
      "pipeline_v2_revise_task_resume_controller.ts",
      "pipeline_v2_revise_task_resume_controller_internal.ts",
    ]) {
      const source = await readSource(join(import.meta.dir, "..", "src", name), "utf8");
      const countOf = (needle: string): number => source.split(needle).length - 1;
      // no reducer, validator, store, filesystem, manifest machinery of its own
      expect(countOf("reducePipelineV2RunCommand(")).toBe(0);
      expect(countOf("validatePipelineV2RunState(")).toBe(0);
      expect(countOf("JSON.stringify")).toBe(0);
      expect(countOf("JSON.parse")).toBe(0);
      expect(countOf("createHash")).toBe(0);
      expect(countOf("CryptoHasher")).toBe(0);
      expect(countOf("canonicalJson(")).toBe(0);
      expect(countOf("new WeakMap")).toBe(0);
      expect(countOf("new WeakSet")).toBe(0);
      expect(countOf("O_EXCL")).toBe(0);
      expect(countOf("O_CREAT")).toBe(0);
      expect(countOf("O_NOFOLLOW")).toBe(0);
      expect(countOf("node:fs")).toBe(0);
      expect(countOf("pipeline_v2_state_store")).toBe(0);
      expect(countOf("pipeline_v2_state_sink")).toBe(0);
      expect(countOf("pipeline_v2_wait_store")).toBe(0);
      expect(countOf("pipeline_v2_wait_manifest")).toBe(0);
      expect(countOf("pipeline_v2_wait_controller")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_store")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_candidate")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_controller")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_restore")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_compiled")).toBe(0);
      expect(countOf("pipeline_v2_run_plan_manifests")).toBe(0);
      expect(countOf("pipeline_v2_revise_task_intent_controller")).toBe(0);
      expect(countOf("pipeline_v2_revise_task_completion_controller")).toBe(0);
      expect(countOf("pipeline_v2_revise_task_closure_controller")).toBe(0);
      expect(countOf("pipeline_v2_revise_task_intervention_controller_internal")).toBe(0);
      expect(countOf("pipeline_v2_runner")).toBe(0);
      expect(countOf("main.ts")).toBe(0);
      expect(countOf("cli_")).toBe(0);
      expect(countOf("docker")).toBe(0);
      expect(countOf("launcher")).toBe(0);
      // no mutable module-global seam and no message parsing
      expect(countOf("let production")).toBe(0);
      expect(countOf(".match(")).toBe(0);
      expect(countOf(".test(")).toBe(0);
      expect(countOf("cause.message")).toBe(0);
      expect(countOf(".message.includes")).toBe(0);
    }
    // the internal core imports the two composed facades, each exactly once
    const internalSource = await readSource(join(import.meta.dir, "..", "src", "pipeline_v2_revise_task_resume_controller_internal.ts"), "utf8");
    expect(internalSource.split('from "./pipeline_v2_revise_task_intervention_controller.ts"').length - 1).toBe(1);
    expect(internalSource.split('from "./pipeline_v2_coordinator.ts"').length - 1).toBe(1);
  });
});
