import { createHash } from "node:crypto";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import type { Stats } from "node:fs";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ExecutionSession,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
  type PipelineV2ToolSession,
  type PipelineV2WorkerRunResult,
} from "../src/pipeline_v2_coordinator.ts";
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
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { parsePipelineV2RunState, type PipelineV2RunCommand, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { pipelineV2RunStatePath } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { PipelineV2ContinuedStageControllerError } from "../src/pipeline_v2_continued_stage_controller.ts";
import {
  applyPipelineV2ContinueStageIntervention,
  type AppliedPipelineV2ContinueStageIntervention,
} from "../src/pipeline_v2_continue_stage_intervention_controller.ts";
import {
  productionContinueStageResumeOps,
  applyPipelineV2ContinueStageResumeWithIo,
  PipelineV2ContinueStageResumeControllerError,
} from "../src/pipeline_v2_continue_stage_resume_controller_internal.ts";
import { resumePipelineV2RunAfterContinueStageIntervention } from "../src/pipeline_v2_continue_stage_resume_controller.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";

/**
 * Tests for the unwired production-neutral composition of the full
 * `continue_stage` handoff: the restart-aware intervention facade followed
 * by the coordinator's resume entrypoint
 * (`resumePipelineV2RunAfterContinueStageIntervention`). The prefix of
 * every proof is built with the real substrate itself (the run-owned
 * project copy, the run-input snapshot, the real data-plane activation
 * preparation and acceptance, the real plan acceptance, the real
 * stage-iteration controller, the real wait manifest/store) and the
 * simulated restarts go through the ordinary `PipelineV2RunStateSink.open`
 * — never hand-built snapshots. Everything is deterministic: no sleeps, no
 * LLM, no Docker Helper, no launcher credential. The controller stays
 * unwired: the runner, the CLI and the default pipeline are untouched.
 */

const PIPELINE_CONTINUED_STAGE_RESUME = `
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
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
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

const INITIAL_BUDGET = 2;

// --- harness ---------------------------------------------------------------

interface BundleDirs {
  root: string;
  bundle: string;
  sources: string;
  projectSource: string;
  runRoot: string;
  stateRoot: string;
}

async function makeDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-resume-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), PIPELINE_CONTINUED_STAGE_RESUME);
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.md"), "TASK-BODY\n");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const runRoot = join(root, "runs", "resume-run");
  await mkdir(runRoot, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, sources, projectSource, runRoot, stateRoot };
}

// --- clock -----------------------------------------------------------------

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock));
}

// --- recording sink --------------------------------------------------------

type CommandRecord = Record<string, unknown>;

class RecordingSink implements PipelineV2CoordinatorStateSink {
  readonly commands: CommandRecord[] = [];
  snapshotReads = 0;
  onCommand?: (kind: string) => void;

  constructor(private readonly inner: PipelineV2RunStateSink) {}

  get snapshot(): PipelineV2RunState | null {
    this.snapshotReads += 1;
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2CoordinatorStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    this.onCommand?.(command.kind);
    await this.inner.dispatch(command);
  }
}

interface Harness {
  dirs: BundleDirs;
  pipeline: ResolvedPipelineV2;
  runId: string;
  sink: PipelineV2RunStateSink;
  recording: RecordingSink;
}

async function setupHarness(): Promise<Harness> {
  const dirs = await makeDirs();
  const pipeline = await loadPipelineV2(dirs.bundle);
  clock = 0;
  const sink = new PipelineV2RunStateSink({
    stateRoot: dirs.stateRoot,
    runId: "resume-run",
    now: nextTick,
  });
  return {
    dirs,
    pipeline,
    runId: "resume-run",
    sink,
    recording: new RecordingSink(sink),
  };
}

/** Reopens the durable run after a simulated process restart. */
async function reopenHarness(
  harness: Harness,
): Promise<RecordingSink> {
  const real = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: harness.runId,
    now: nextTick,
  });
  return new RecordingSink(real);
}

// --- the honest prefix (real substrate, real facades only) ------------------

function transitionTarget(pipeline: ResolvedPipelineV2, stateId: string): { to: string; index: number } {
  const state = pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type !== "agent") {
    throw new Error(`the prefix fixture has no agent state ${JSON.stringify(stateId)}`);
  }
  const match = state.transitions[0];
  if (match === undefined) {
    throw new Error(`state ${JSON.stringify(stateId)} declares no transition`);
  }
  return { to: match.to, index: 0 };
}

/** The deterministic prefix worker: the fixture states declare no outputs. */
class PrefixWorker {
  runCount = 0;
  cleanupCount = 0;

  constructor(private readonly activation: PreparedActivationData) {}

  async run(): Promise<PipelineV2WorkerRunResult> {
    this.runCount += 1;
    for (const port of this.activation.output_ports) {
      await writeFile(port.path, `${port.id} body`);
    }
    return { status: "completed" };
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
  }
}

async function prefixCreateRun(harness: Harness): Promise<RunInputsSnapshot> {
  await prepareRunProject(harness.dirs.projectSource, harness.dirs.runRoot);
  const runInputs = await snapshotRunInputs(
    harness.pipeline,
    [{ id: "task", path: join(harness.dirs.sources, "task.md") }] as readonly RunInputBinding[],
    harness.dirs.runRoot,
  );
  await harness.recording.dispatch({
    kind: "create_run",
    runId: harness.runId,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  return runInputs;
}

/**
 * One real agent activation through the real data plane and the real
 * reducer: activation preparation, the durable phases, the worker run, the
 * output acceptance and (optionally) the graph transition. The planning
 * execution settles unbound: its transition commits only after the
 * generation and iteration opened (the contract hook order).
 */
async function prefixAgentStep(
  harness: Harness,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
  options: { commitTransition: boolean; sessionLabel?: number } = { commitTransition: true },
): Promise<PreparedActivationData> {
  const sessionLabel = options.sessionLabel ?? executionIndex;
  const activation = await prepareActivationData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  await harness.recording.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile: "coder",
    ...startRoleArgs(harness.pipeline, stateId, harness.recording.snapshot),
  });
  await harness.recording.dispatch({ kind: "agent_data_prepared" });
  await harness.recording.dispatch({ kind: "agent_execution_session_created", sessionId: `exec-${sessionLabel}` });
  await harness.recording.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${sessionLabel}` });
  await harness.recording.dispatch({ kind: "agent_running" });
  const worker = new PrefixWorker(activation);
  await worker.run();
  const records = await acceptActivationOutputs(harness.pipeline, activation);
  await harness.recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await worker.cleanup();
  await harness.recording.dispatch({ kind: "agent_cleanup_completed" });
  if (options.commitTransition) {
    const target = transitionTarget(harness.pipeline, stateId);
    await harness.recording.dispatch({
      kind: "transition_committed",
      step: { from: stateId, outcome: "completed", to: target.to, transition_index: target.index },
      executionIndex,
    });
  }
  accepted.push(...records);
  return activation;
}

/**
 * The honest prefix through the existing production facades only: the real
 * run/project/input snapshot, the real planning execution, the real run
 * plan acceptance, generation 1 / iteration 1, the planning transition,
 * the real stage execution with the durable transition back to the
 * planning state, the real published wait request and the prepared
 * provenance-backed `continue_stage_intent`.
 */
async function driveToWaitBoundary(): Promise<
  Harness & { runInputs: RunInputsSnapshot; intent: PreparedPipelineV2RunWaitIntent; requestSha256: string }
> {
  const harness = await setupHarness();
  const runInputs = await prefixCreateRun(harness);
  const protectedInput = runInputs.inputs[0];
  if (protectedInput === undefined) {
    throw new Error("the prefix fixture lost its protected run input");
  }
  const accepted: AcceptedStateOutput[] = [];
  await prefixAgentStep(harness, runInputs, accepted, "architect", 1, { commitTransition: false });

  const taskA = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: harness.runId,
    task_id: "task-a",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "PLAN-TASK-BODY",
  });
  const plan1 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: harness.runId,
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
  const candidate = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: protectedInput.digest,
  });
  const acceptedPlan = await acceptPipelineV2RunPlanCandidate({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    sink: harness.recording,
    candidate,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: acceptedPlan.compiled_plan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: harness.recording,
  });
  const architectTarget = transitionTarget(harness.pipeline, "architect");
  await harness.recording.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: architectTarget.to, transition_index: architectTarget.index },
    executionIndex: 1,
  });
  await prefixAgentStep(harness, runInputs, accepted, "dev_entry", 2);

  const request = preparePipelineV2WaitRequest({
    schema_version: 1,
    run_id: harness.runId,
    wait_index: 1,
    transition_count: 2,
    state_id: "architect",
    reason: "stage_iteration_limit_exhausted",
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await harness.recording.dispatch({
    kind: "run_waiting",
    stateId: "architect",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: request.sha256,
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await publishPipelineV2WaitRequest(harness.dirs.runRoot, request.manifest);
  const intent = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: harness.runId,
    wait_index: 1,
    stage_id: "stage-1",
    expected_plan_sha256: acceptedPlan.compiled_plan.plan_sha256,
    additional_iterations: 2,
  });
  return { ...harness, runInputs, intent, requestSha256: request.sha256 };
}

// --- the fake agent runtime (copied contract from the coordinator suite) ---

interface FakeSessionSpec {
  executionId?: string;
  toolId?: string;
  run?: "completed" | "worker_failed" | "worker_timeout" | "throw";
}

class FakeAgentSession {
  runCount = 0;
  cleanupCount = 0;
  readonly sessionId: string;

  constructor(
    readonly spec: FakeSessionSpec,
    readonly stateId: string,
    readonly activation: PreparedActivationData,
    readonly kind: "execution" | "tool",
    fallbackSessionId: string,
    private readonly log: (message: string) => void,
  ) {
    this.sessionId = (kind === "execution" ? spec.executionId : spec.toolId) ?? fallbackSessionId;
  }

  async runAgent(_toolSession: PipelineV2ToolSession): Promise<PipelineV2WorkerRunResult> {
    this.runCount += 1;
    this.log(`run:${this.stateId}`);
    for (const port of this.activation.output_ports) {
      await writeFile(port.path, `${port.id} body`);
    }
    switch (this.spec.run) {
      case "worker_failed":
        return { status: "failed", reason: "worker_failed" };
      case "worker_timeout":
        return { status: "failed", reason: "worker_timeout" };
      case "throw":
        throw new Error("WORKER-EXPLODED");
      default:
        return { status: "completed" };
    }
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
    this.log(`cleanup-${this.kind === "execution" ? "exec" : "tool"}:${this.stateId}`);
  }
}

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

function fakeRuntime(specs: readonly FakeSessionSpec[], idPrefix = ""): FakeRuntimeHandle {
  const pairs: FakePair[] = [];
  const createCalls: FakeRuntimeHandle["createCalls"] = [];
  const events: string[] = [];
  const logEvent = (message: string): void => {
    events.push(message);
  };
  const runtime = {
    createExecutionSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const index = pairs.length;
      const spec = specs[index] ?? {};
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "execution" });
      const execution = new FakeAgentSession(spec, state.id, activation, "execution", `${idPrefix}exec-${index + 1}`, logEvent);
      events.push(`create-exec:${state.id}:${activation.activation_index}`);
      const tool = new FakeAgentSession(spec, state.id, activation, "tool", `${idPrefix}tool-${index + 1}`, logEvent);
      const pair: FakePair = { stateId: state.id, activationIndex: activation.activation_index, execution, tool };
      pairs.push(pair);
      return execution as unknown as PipelineV2ExecutionSession;
    },
    createToolSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const index = pairs.length - 1;
      const pair = pairs[index];
      if (pair === undefined) {
        throw new Error("no execution session was created for this activation");
      }
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "tool" });
      events.push(`create-tool:${state.id}:${activation.activation_index}`);
      return pair.tool as unknown as PipelineV2ToolSession;
    },
  };
  return { runtime: runtime as unknown as PipelineV2AgentRuntime, pairs, createCalls, events };
}

// --- tests -----------------------------------------------------------------

/**
 * The identity contract the composition layer's verification builds on,
 * proven directly against the real facade first: a real successful
 * intervention result carries the exact immutable state object the
 * reopened sink's authoritative `snapshot` returns — never a structural
 * re-comparison target and never a fresh normalized copy.
 */
test("identity contract: a real intervention result carries the authoritative sink snapshot object", async () => {
  const ctx = await driveToWaitBoundary();
  const preIntervention = await reopenHarness(ctx);
  expect((preIntervention.snapshot as PipelineV2RunState)).toEqual(ctx.recording.snapshot as PipelineV2RunState);
  const intervention = await applyPipelineV2ContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: preIntervention,
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  const authoritative = preIntervention.snapshot as PipelineV2RunState;
  expect(intervention.state).toBe(authoritative);
  expect(Object.isFrozen(intervention.state)).toBe(true);
  assertCompletedBoundary(authoritative, ctx.intent.sha256, intervention.response_sha256);
});

// --- crash seam and C5 ------------------------------------------------------

/**
 * The real restart between the intervention and the resume: the first call
 * runs the real production intervention to full durability and its injected
 * resume throws a sentinel before any resume effect; after the ordinary
 * reopen a fresh call with production ops recognizes the exact completed
 * boundary with zero intervention dispatch (C5) and runs the resume exactly
 * once — the handoff projection is never rewritten.
 */
test("crash seam: the durable intervention is recognized with zero dispatch after the reopen and the resume runs once", async () => {
  const ctx = await driveToWaitBoundary();
  const preIntervention = await reopenHarness(ctx);

  const sentinel = new Error("RESUME-SENTINEL");
  let resumeCalls = 0;
  const crashedOps = {
    applyIntervention: applyPipelineV2ContinueStageIntervention,
    resumeRun: () => {
      resumeCalls += 1;
      throw sentinel;
    },
  };
  const firstError = await applyPipelineV2ContinueStageResumeWithIo(crashedOps, {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: preIntervention,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  // the sentinel passes through by object identity
  expect(firstError).toBe(sentinel);
  expect(resumeCalls).toBe(1);
  // the intervention ran fully and durably; no resume effect happened
  expect(kinds(preIntervention)).toEqual([...INTERVENTION_SUFFIX]);
  const boundaryState = preIntervention.snapshot as PipelineV2RunState;
  assertCompletedBoundary(boundaryState, ctx.intent.sha256);

  // the second simulated restart, then the production-ops call
  const reopened = await reopenHarness(ctx);
  expect((reopened.snapshot as PipelineV2RunState)).toEqual(boundaryState);
  const fake = fakeRuntime([{ run: "worker_failed" }], "again-");
  const result = await resumePipelineV2RunAfterContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused)) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe("worker_failed");
  const state = result.state as PipelineV2RunState;
  // C5: the intervention recognized the exact completed boundary with zero
  // dispatch — the reopened recording carries only the resume commands
  expect(kinds(reopened)).toEqual([...RESUME_FAILED_COMMANDS]);
  expect(reopened.commands[0]).toMatchObject({
    stateId: "dev_entry",
    executionRole: "stage",
    iterationIndex: 2,
  });
  expect(fake.createCalls).toEqual([
    { stateId: "dev_entry", activationIndex: 3, session: "execution" },
    { stateId: "dev_entry", activationIndex: 3, session: "tool" },
  ]);
  expect(fake.pairs).toHaveLength(1);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
  expect(fake.events).toEqual([
    "create-exec:dev_entry:3",
    "create-tool:dev_entry:3",
    "run:dev_entry",
    "cleanup-tool:dev_entry",
    "cleanup-exec:dev_entry",
  ]);
  // the handoff projection is untouched between the boundary and the final state
  expect(state.grants).toEqual(boundaryState.grants);
  expect(state.waits).toEqual(boundaryState.waits);
  expect(state.generations).toEqual(boundaryState.generations);
  expect(state.task_revisions).toEqual(boundaryState.task_revisions);
  expect(state.plan_revisions).toEqual(boundaryState.plan_revisions);
  expect(state.transitions).toEqual(boundaryState.transitions);
  expect(state.cursor).toEqual(boundaryState.cursor);
  // the boundary's execution records are untouched; the failure appends only
  // the new third record
  expect(state.executions.slice(0, 2)).toEqual(boundaryState.executions);
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "worker_failed" });
  // loader round-trip of the final durable state
  expect(await readDurableState(ctx)).toEqual(state);
});

/**
 * The C0 main proof: the honest prefix through the existing production
 * facades, the first simulated restart through the ordinary sink open, and
 * then ONLY the new public facade — the intervention commands run exactly
 * once in their fixed order on the reopened run and the coordinator starts
 * the successor stage execution (an ordinary `worker_failed` execution
 * failure, never a refusal), with no repeated intervention command, one
 * session pair cleaned exactly once each tool-first, the durable
 * handoff projection untouched and the final snapshot loader-round-tripped.
 */
test("C0: the full handoff through the new facade on the reopened run — intervention suffix, then the resumed successor execution", async () => {
  const ctx = await driveToWaitBoundary();
  const preIntervention = await reopenHarness(ctx);
  expect((preIntervention.snapshot as PipelineV2RunState)).toEqual(ctx.recording.snapshot as PipelineV2RunState);

  const fake = fakeRuntime([{ run: "worker_failed" }], "resumed-");
  const result = await resumePipelineV2RunAfterContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: preIntervention,
    runtime: fake.runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused)) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe("worker_failed");
  const state = result.state as PipelineV2RunState;
  expect(state.failure).toEqual({ reason: "worker_failed" });

  // the exact five-command intervention suffix, then the resumed execution
  expect(kinds(preIntervention)).toEqual([...INTERVENTION_SUFFIX, ...RESUME_FAILED_COMMANDS]);
  // the first and only new start carries the exact stage role and successor iteration
  expect(preIntervention.commands[5]).toMatchObject({
    stateId: "dev_entry",
    executionRole: "stage",
    iterationIndex: 2,
  });
  // exactly one session pair, cleaned exactly once each, tool first
  expect(fake.createCalls).toEqual([
    { stateId: "dev_entry", activationIndex: 3, session: "execution" },
    { stateId: "dev_entry", activationIndex: 3, session: "tool" },
  ]);
  expect(fake.pairs).toHaveLength(1);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
  expect(fake.events).toEqual([
    "create-exec:dev_entry:3",
    "create-tool:dev_entry:3",
    "run:dev_entry",
    "cleanup-tool:dev_entry",
    "cleanup-exec:dev_entry",
  ]);
  // the durable record: the next global execution index without a gap
  expect(state.executions.map((execution) => execution.index)).toEqual([1, 2, 3]);
  expect(state.executions[2]).toMatchObject({
    index: 3,
    type: "agent",
    state_id: "dev_entry",
    execution_role: "stage",
    iteration_index: 2,
    failure_reason: "worker_failed",
    session_cleanup: { execution: "completed", tool: "completed" },
  });
  // the intervention records are untouched; no new graph transition; the
  // cursor and transition count stay at the continued-stage boundary
  expect(state.transitions).toHaveLength(2);
  expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 2 });
  expect(state.status).toBe("failed");
  // the durable handoff projection: exact grant, wait, generation, ledgers
  expect(state.grants).toHaveLength(1);
  expect(state.grants[0]).toEqual({
    index: 1,
    generation_index: 1,
    wait_index: 1,
    intent_sha256: ctx.intent.sha256,
    additional_iterations: 2,
  });
  expect(state.waits).toHaveLength(1);
  expect(state.waits[0]?.intent).toEqual({ intent_sha256: ctx.intent.sha256 });
  expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(state.generations).toHaveLength(1);
  expect(state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
  expect(state.generations[0]?.iterations[0]?.closed).toEqual({
    by: "grant",
    wait_index: 1,
    closed_transition_count: 2,
  });
  expect(state.task_revisions).toHaveLength(1);
  expect(state.plan_revisions).toHaveLength(1);
  // loader round-trip of the final durable state
  expect(await readDurableState(ctx)).toEqual(state);
});

// --- helpers ---------------------------------------------------------------

function neutralControl(): PipelineV2CoordinatorControl {
  return {
    currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
    freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
  };
}

function kinds(recording: RecordingSink): string[] {
  return recording.commands.map((command) => command.kind as string);
}

const INTERVENTION_SUFFIX = [
  "plan_intent_accepted",
  "iteration_grant_recorded",
  "stage_iteration_closed",
  "wait_response_recorded",
  "stage_iteration_opened",
] as const;

const RESUME_FAILED_COMMANDS = [
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_failed",
  "run_failed",
] as const;

function expectInterventionFailure(
  cause: unknown,
): PipelineV2ContinueStageResumeControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinueStageResumeControllerError);
  return cause as PipelineV2ContinueStageResumeControllerError;
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  const raw = await readFile(pipelineV2RunStatePath(harness.dirs.stateRoot, harness.runId), "utf8");
  return parsePipelineV2RunState(raw);
}

function assertCompletedBoundary(state: PipelineV2RunState, intentSha256: string, responseSha256?: string): string {
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 2 });
  expect(state.generations).toHaveLength(1);
  const generation = state.generations[0];
  expect(generation?.index).toBe(1);
  expect(generation?.stage_id).toBe("stage-1");
  expect(generation?.initial_budget).toBe(INITIAL_BUDGET);
  expect(generation?.iterations[0]?.closed).toEqual({
    by: "grant",
    wait_index: 1,
    closed_transition_count: 2,
  });
  expect(generation?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
  expect(generation?.iterations[1]).toEqual({ index: 2, opened_transition_count: 2 });
  expect(state.waits).toHaveLength(1);
  expect(state.waits[0]?.transition_count).toBe(2);
  expect(state.waits[0]?.intent).toEqual({ intent_sha256: intentSha256 });
  expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
  const digest = state.waits[0]?.response?.response_sha256;
  expect(digest).toMatch(/^[0-9a-f]{64}$/);
  if (responseSha256 !== undefined) {
    expect(digest).toBe(responseSha256);
  }
  expect(state.grants).toHaveLength(1);
  expect(state.grants[0]).toEqual({
    index: 1,
    generation_index: 1,
    wait_index: 1,
    intent_sha256: intentSha256,
    additional_iterations: 2,
  });
  return digest as string;
}

// --- intervention/resume error identity --------------------------------------

test("an intervention failure passes through by identity with zero resume calls and zero durable effects", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const sentinel = new Error("INTERVENTION-SENTINEL");
  let resumeCalls = 0;
  const ops = {
    applyIntervention: () => {
      throw sentinel;
    },
    resumeRun: () => {
      resumeCalls += 1;
      throw new Error("must never run");
    },
  };
  const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  expect(caught).toBe(sentinel);
  expect(resumeCalls).toBe(0);
  expect(kinds(reopened)).toEqual([]);
});

test("a resume thrown error passes through by object identity after the real intervention", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const sentinel = new Error("RESUME-THROW-SENTINEL");
  const ops = {
    applyIntervention: applyPipelineV2ContinueStageIntervention,
    resumeRun: () => {
      throw sentinel;
    },
  };
  const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  expect(caught).toBe(sentinel);
  expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX]);
});

// --- malformed intervention-result matrix ------------------------------------

function foreignDigest(char: string): string {
  return char.repeat(64);
}

/**
 * A hand-built successful-looking intervention result against the still
 * waiting authoritative state: every flat/identity violation is this
 * layer's own `invalid_result` with the authoritative snapshot as the
 * error state and zero resume calls — never a `TypeError`.
 */
test("malformed intervention results on the waiting boundary: the typed invalid_result matrix", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const authoritative = reopened.snapshot as PipelineV2RunState;
  const flat = {
    wait_index: 1,
    intent_sha256: ctx.intent.sha256,
    request_sha256: ctx.requestSha256,
    response_sha256: foreignDigest("a"),
    additional_iterations: 2,
    action_id: "continue_stage",
    action_to: "dev_entry",
    closed_iteration_index: 1,
    iteration_index: 2,
    generation_index: 1,
    compiled_stage: { id: "stage-1", template: "development", entry_state: "dev_entry", state_ids: ["dev_entry"], tasks: [] },
  };
  const cases: ReadonlyArray<readonly [string, unknown]> = [
    ["a number", 42],
    ["a string", "nope"],
    ["null", null],
    ["undefined", undefined],
    ["an array", [flat]],
    ["a missing state", { ...flat }],
    ["a foreign state object", { ...flat, state: { ...authoritative } }],
    ["a foreign wait index", { ...flat, state: authoritative, wait_index: 2 }],
    ["a foreign intent digest", { ...flat, state: authoritative, intent_sha256: foreignDigest("b") }],
    ["a foreign additional iterations", { ...flat, state: authoritative, additional_iterations: 3 }],
    ["a foreign action id", { ...flat, state: authoritative, action_id: "revise_task" }],
    ["a successor not after the closed iteration", { ...flat, state: authoritative, iteration_index: 3 }],
    ["a missing compiled stage", { ...flat, state: authoritative, compiled_stage: undefined }],
    ["a malformed response digest", { ...flat, state: authoritative, response_sha256: "NOT-A-DIGEST" }],
  ];
  for (const [name, hostile] of cases) {
    let resumeCalls = 0;
    const ops = {
      applyIntervention: () => hostile,
      resumeRun: () => {
        resumeCalls += 1;
        throw new Error("must never run");
      },
    };
    const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
      pipeline: ctx.pipeline,
      runRoot: ctx.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime([]).runtime,
      control: neutralControl(),
      intent: ctx.intent,
      initialBudget: INITIAL_BUDGET,
    }).then(
      () => null,
      (cause: unknown) => cause,
    );
    const error = expectInterventionFailure(caught);
    expect(error.reason).toBe("invalid_result");
    expect(error.state).toBe(authoritative);
    expect(resumeCalls).toBe(0);
    expect(kinds(reopened)).toEqual([]);
  }
});

/**
 * The durable-binding mutations against the real completed boundary: the
 * fake intervention runs the real facade and returns a mutated clone, so
 * the identity binding holds and the exact durable bindings are what
 * reject the hostile result.
 */
test("mutated flat fields against the real completed boundary: the durable binding matrix", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const realResult = await applyPipelineV2ContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  const authoritative = reopened.snapshot as PipelineV2RunState;
  expect(authoritative).toBe(realResult.state);
  expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX]);
  const mutated = (patch: Record<string, unknown>): unknown => ({ ...realResult, ...patch });
  const cases: ReadonlyArray<readonly [string, unknown]> = [
    ["a foreign request digest", mutated({ request_sha256: foreignDigest("c") })],
    ["a foreign routing target", mutated({ action_to: "architect" })],
    ["a foreign response digest", mutated({ response_sha256: foreignDigest("d") })],
    ["a foreign closed iteration", mutated({ closed_iteration_index: 2, iteration_index: 3 })],
    ["a foreign generation index", mutated({ generation_index: 2 })],
    ["a foreign additional iterations against the durable grant", mutated({ additional_iterations: 3 })],
  ];
  for (const [name, hostile] of cases) {
    let resumeCalls = 0;
    const ops = {
      applyIntervention: () => hostile,
      resumeRun: () => {
        resumeCalls += 1;
        throw new Error("must never run");
      },
    };
    const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
      pipeline: ctx.pipeline,
      runRoot: ctx.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime([]).runtime,
      control: neutralControl(),
      intent: ctx.intent,
      initialBudget: INITIAL_BUDGET,
    }).then(
      () => null,
      (cause: unknown) => cause,
    );
    const error = expectInterventionFailure(caught);
    expect(error.reason).toBe("invalid_result");
    expect(error.state).toBe(authoritative);
    expect(resumeCalls).toBe(0);
    // the hostile calls dispatch nothing: only the capture step's five commands
    expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX]);
  }
});

/**
 * A foreign caller budget (the durable generation was opened with a
 * different budget) is refused by the composed layers themselves; the
 * typed conflict passes through this layer by identity and the resume is
 * never started.
 */
test("a foreign caller budget is refused by the composed layers with zero resume calls", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  let resumeCalls = 0;
  const ops = {
    applyIntervention: applyPipelineV2ContinueStageIntervention,
    resumeRun: () => {
      resumeCalls += 1;
      throw new Error("must never run");
    },
  };
  const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET + 1,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  expect(caught).toBeInstanceOf(PipelineV2ContinuedStageControllerError);
  expect((caught as PipelineV2ContinuedStageControllerError).reason).toBe("invalid_result");
  expect(resumeCalls).toBe(0);
  // the grant/closure/response are durable; the successor iteration never opened
  expect(kinds(reopened)).toEqual([
    "plan_intent_accepted",
    "iteration_grant_recorded",
    "stage_iteration_closed",
    "wait_response_recorded",
  ]);
});

// --- malformed resume-result matrix and valid-outcome identity ---------------

/**
 * The real intervention reaches the resume; the injected resume returns one
 * malformed union per call. Every violation is this layer's own
 * `invalid_result` carrying the last authoritative snapshot — never a
 * `TypeError` — and the coordinator union is never reclassified.
 */
test("malformed resume coordinator results: the typed invalid_result matrix over all union branches", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const hostileResults: ReadonlyArray<readonly [string, unknown]> = [
    ["a number", 42],
    ["a string", "nope"],
    ["null", null],
    ["undefined", undefined],
    ["an array", []],
    ["an empty record", {}],
    ["a non-boolean ok", { ok: "yes", state: null }],
    ["an ok true with a null state", { ok: true, state: null }],
    ["an ok true with a foreign state", { ok: true, state: { ...({} as PipelineV2RunState) } }],
    ["an ok false with refused false", { ok: false, refused: false, reason: "worker_failed", state: null }],
    ["an ok false with a non-boolean refused", { ok: false, refused: "yes", reason: "worker_failed", state: null }],
    ["an ok false with an empty reason", { ok: false, reason: "", state: null }],
    ["an ok false without a reason", { ok: false, state: null }],
    ["an ok false with a missing state", { ok: false, reason: "worker_failed" }],
    ["an ok false with a foreign state", { ok: false, reason: "worker_failed", state: foreignDigest("e") }],
  ];
  for (const [name, hostile] of hostileResults) {
    let resumeCalls = 0;
    const ops = {
      applyIntervention: applyPipelineV2ContinueStageIntervention,
      resumeRun: () => {
        resumeCalls += 1;
        return hostile as never;
      },
    };
    const caught = await applyPipelineV2ContinueStageResumeWithIo(ops, {
      pipeline: ctx.pipeline,
      runRoot: ctx.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime([]).runtime,
      control: neutralControl(),
      intent: ctx.intent,
      initialBudget: INITIAL_BUDGET,
    }).then(
      () => null,
      (cause: unknown) => cause,
    );
    const error = expectInterventionFailure(caught);
    expect(error.reason).toBe("invalid_result");
    expect(error.state).toBe(reopened.snapshot as PipelineV2RunState);
    expect(resumeCalls).toBe(1);
  }
  // the hostile resume calls dispatch nothing durable: the boundary journal
  // stays exactly the five intervention commands
  expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX]);
});

/**
 * Valid coordinator unions are returned unchanged by object identity —
 * refusal, worker failure and success stay coordinator-owned
 * classifications; nothing is reclassified into a new envelope.
 */
test("valid coordinator outcomes are returned by object identity without reclassification", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const validBuilders: ReadonlyArray<readonly [string, (snapshot: PipelineV2RunState) => unknown]> = [
    ["a success union", (snapshot) => ({ ok: true, state: snapshot })],
    ["a refusal union", () => ({ ok: false, refused: true, reason: "missing_state", state: null })],
    ["a worker failure union", (snapshot) => ({ ok: false, reason: "worker_failed", state: snapshot })],
  ];
  for (const [name, build] of validBuilders) {
    let built: unknown;
    const ops = {
      applyIntervention: applyPipelineV2ContinueStageIntervention,
      resumeRun: (params: { sink: PipelineV2CoordinatorStateSink }) => {
        built = build(params.sink.snapshot as PipelineV2RunState);
        return built as never;
      },
    };
    const returned = await applyPipelineV2ContinueStageResumeWithIo(ops, {
      pipeline: ctx.pipeline,
      runRoot: ctx.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime([]).runtime,
      control: neutralControl(),
      intent: ctx.intent,
      initialBudget: INITIAL_BUDGET,
    });
    expect(returned).toBe(built as PipelineV2ResumeCoordinationResult);
  }
  expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX]);
});

// --- mutation after await ----------------------------------------------------

/**
 * The caller's options, runtime and control objects are mutated while the
 * real intervention is still pending: the captured policy, runtime and
 * control functions are immune, so the handoff verification and the resume
 * behave exactly as captured (the mutated replacements are never called
 * and the mutated budget never reaches the verification).
 */
test("mutation after await: the captured policy, runtime and control are immune during the pending intervention", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  let release!: () => void;
  const firstCommandDispatched = new Promise<void>((resolve) => {
    release = resolve;
  });
  reopened.onCommand = (kind) => {
    if (kind === "plan_intent_accepted") {
      reopened.onCommand = undefined;
      release();
    }
  };
  const fake = fakeRuntime([{ run: "worker_failed" }], "mutation-");
  const runtimeRecord = fake.runtime as unknown as Record<string, unknown>;
  const control = neutralControl();
  const controlRecord = control as unknown as Record<string, unknown>;
  let mutatedRuntimeCalls = 0;
  let mutatedSignalCalls = 0;
  const options = {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
    control,
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  };
  const callPromise = resumePipelineV2RunAfterContinueStageIntervention(options);
  await firstCommandDispatched;
  options.initialBudget = 99;
  runtimeRecord.createExecutionSession = () => {
    mutatedRuntimeCalls += 1;
    throw new Error("MUTATED-RUNTIME");
  };
  controlRecord.currentSignal = () => {
    mutatedSignalCalls += 1;
    return "SIGINT" as const;
  };
  const result = await callPromise;
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused)) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  // the mutated signal never reached the coordinator: the failure stays the
  // ordinary worker failure, not a signal abort
  expect(result.reason).toBe("worker_failed");
  expect(mutatedRuntimeCalls).toBe(0);
  expect(mutatedSignalCalls).toBe(0);
  // the resume still used the captured runtime functions
  expect(fake.createCalls).toEqual([
    { stateId: "dev_entry", activationIndex: 3, session: "execution" },
    { stateId: "dev_entry", activationIndex: 3, session: "tool" },
  ]);
  // the captured budget, not the mutated one, verified the handoff
  expect(kinds(reopened)).toEqual([...INTERVENTION_SUFFIX, ...RESUME_FAILED_COMMANDS]);
  expect(await readDurableState(ctx)).toEqual(result.state as PipelineV2RunState);
});

// --- capture order and exact read counts -------------------------------------

test("the capture boundary reads every option field and ops member exactly once, in order, and never reads hostile extras", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  let resumeCalls = 0;
  let interventionCalls = 0;
  const countingOps = new Proxy(
    {
      applyIntervention: () => {
        interventionCalls += 1;
        return { ...({} as PipelineV2RunState) } as unknown;
      },
      resumeRun: () => {
        resumeCalls += 1;
        throw new Error("must never run");
      },
    },
    {
      get(target, key) {
        if (typeof key === "string") {
          reads.push(`ops:${key}`);
        }
        return target[key as keyof typeof target];
      },
    },
  ) as unknown as Record<string, unknown>;
  const reads: string[] = [];
  const optionRecord = {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
    hostileExtra: "NEVER-READ",
  };
  const countingOptions = new Proxy(optionRecord, {
    get(target, key) {
      if (typeof key === "string") {
        reads.push(key);
      }
      return target[key as keyof typeof target];
    },
  }) as unknown;
  const caught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, countingOptions).then(
    () => null,
    (cause: unknown) => cause,
  );
  expectInterventionFailure(caught);
  expect(caught as PipelineV2ContinueStageResumeControllerError).toMatchObject({ reason: "invalid_result" });
  // the exact read order and counts: the seven options, then the two ops members
  expect(reads).toEqual([
    "pipeline",
    "runRoot",
    "sink",
    "runtime",
    "control",
    "intent",
    "initialBudget",
    "ops:applyIntervention",
    "ops:resumeRun",
  ]);
  expect(interventionCalls).toBe(1);
  expect(resumeCalls).toBe(0);
  expect(optionRecord.hostileExtra).toBe("NEVER-READ");
});

test("the runtime and control contract functions are read exactly once and the coordinator consumes the stable adapters", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  const runtimeReads: string[] = [];
  const controlReads: string[] = [];
  const fake = fakeRuntime([{ run: "worker_failed" }], "reads-");
  const proxiedRuntime = new Proxy(fake.runtime as unknown as Record<string, unknown>, {
    get(target, key) {
      if (typeof key === "string") {
        runtimeReads.push(key);
      }
      return target[key as keyof typeof target];
    },
  }) as unknown as PipelineV2AgentRuntime;
  const neutral = neutralControl();
  const proxiedControl = new Proxy(neutral as unknown as Record<string, unknown>, {
    get(target, key) {
      if (typeof key === "string") {
        controlReads.push(key);
      }
      return target[key as keyof typeof target];
    },
  }) as unknown as PipelineV2CoordinatorControl;
  const result = await resumePipelineV2RunAfterContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: proxiedRuntime,
    control: proxiedControl,
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused)) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe("worker_failed");
  expect(runtimeReads).toEqual(["createExecutionSession", "createToolSession"]);
  expect(controlReads).toEqual(["currentSignal", "freezeSignal"]);
  expect(fake.createCalls).toEqual([
    { stateId: "dev_entry", activationIndex: 3, session: "execution" },
    { stateId: "dev_entry", activationIndex: 3, session: "tool" },
  ]);
});

test("the controller reads the authoritative snapshot exactly once per verification phase", async () => {
  const ctx = await driveToWaitBoundary();
  const reopenedReal = await PipelineV2RunStateSink.open({
    stateRoot: ctx.dirs.stateRoot,
    runId: ctx.runId,
    now: nextTick,
  });
  const reopened = new RecordingSink(reopenedReal);
  const responseSha256 = foreignDigest("f");
  const compiledStage = { id: "stage-1", template: "development", entry_state: "dev_entry", state_ids: ["dev_entry"], tasks: [] };
  // the fake intervention dispatches the real five commands through the
  // recording sink and reads its own state through the raw inner sink, so
  // the recording wrapper's snapshot reads are the controller's alone
  const ops = {
    applyIntervention: async () => {
      await reopened.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: ctx.intent.sha256 });
      await reopened.dispatch({
        kind: "iteration_grant_recorded",
        generationIndex: 1,
        waitIndex: 1,
        intentSha256: ctx.intent.sha256,
        additionalIterations: 2,
      });
      await reopened.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 });
      await reopened.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: ctx.requestSha256,
        actionId: "continue_stage",
        responseSha256,
      });
      await reopened.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
      return {
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
        request_sha256: ctx.requestSha256,
        response_sha256: responseSha256,
        additional_iterations: 2,
        action_id: "continue_stage",
        action_to: "dev_entry",
        closed_iteration_index: 1,
        iteration_index: 2,
        generation_index: 1,
        compiled_stage: compiledStage,
        state: reopenedReal.snapshot,
      };
    },
    resumeRun: async () => ({ ok: true as const, state: reopenedReal.snapshot }),
  };
  const result = await applyPipelineV2ContinueStageResumeWithIo(ops, {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  });
  expect(result.ok).toBe(true);
  // exactly one read after the intervention and one after the resume
  expect(reopened.snapshotReads).toBe(2);
});

// --- invalid runtime/control and provenance failures before any effect -------

test("invalid runtime or control shapes are invalid_options with zero facade calls", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  let interventionCalls = 0;
  let resumeCalls = 0;
  const countingOps = {
    applyIntervention: () => {
      interventionCalls += 1;
      throw new Error("must never run");
    },
    resumeRun: () => {
      resumeCalls += 1;
      throw new Error("must never run");
    },
  };
  const base = {
    pipeline: ctx.pipeline,
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    intent: ctx.intent,
    initialBudget: INITIAL_BUDGET,
  };
  const cases: ReadonlyArray<readonly [string, Record<string, unknown>]> = [
    ["a null runtime", { ...base, runtime: null }],
    ["a runtime without functions", { ...base, runtime: {} }],
    ["a runtime with a non-function member", { ...base, runtime: { createExecutionSession: 1, createToolSession: () => null } }],
    ["a null control", { ...base, runtime: fakeRuntime([]).runtime, control: null }],
    ["a control without functions", { ...base, runtime: fakeRuntime([]).runtime, control: {} }],
    ["a control with one missing function", { ...base, runtime: fakeRuntime([]).runtime, control: { currentSignal: () => null } }],
  ];
  for (const [name, options] of cases) {
    const caught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, options).then(
      () => null,
      (cause: unknown) => cause,
    );
    const error = expectInterventionFailure(caught);
    expect(error.reason).toBe("invalid_options");
    expect(error.state).toBeNull();
    expect(interventionCalls).toBe(0);
    expect(resumeCalls).toBe(0);
  }
  expect(kinds(reopened)).toEqual([]);
});

test("pipeline and intent clone/proxy provenance failures happen before any facade call", async () => {
  const ctx = await driveToWaitBoundary();
  const reopened = await reopenHarness(ctx);
  let interventionCalls = 0;
  let resumeCalls = 0;
  const countingOps = {
    applyIntervention: () => {
      interventionCalls += 1;
      throw new Error("must never run");
    },
    resumeRun: () => {
      resumeCalls += 1;
      throw new Error("must never run");
    },
  };
  const base = {
    runRoot: ctx.dirs.runRoot,
    sink: reopened,
    runtime: fakeRuntime([]).runtime,
    control: neutralControl(),
    initialBudget: INITIAL_BUDGET,
  };
  let pipelineTraps = 0;
  const proxyPipeline = new Proxy(ctx.pipeline as unknown as Record<string, unknown>, {
    get(target, key) {
      pipelineTraps += 1;
      return target[key as keyof typeof target];
    },
  }) as unknown as ResolvedPipelineV2;
  const pipelineCaught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, {
    ...base,
    pipeline: proxyPipeline,
    intent: ctx.intent,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  expect(pipelineCaught).toBeInstanceOf(PipelineError);
  expect(pipelineTraps).toBe(0);
  const clonedPipelineCaught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, {
    ...base,
    pipeline: { ...ctx.pipeline } as unknown as ResolvedPipelineV2,
    intent: ctx.intent,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  expect(clonedPipelineCaught).toBeInstanceOf(PipelineError);
  expect(interventionCalls).toBe(0);
  expect(resumeCalls).toBe(0);

  let intentTraps = 0;
  const proxyIntent = new Proxy(ctx.intent as unknown as Record<string, unknown>, {
    get(target, key) {
      intentTraps += 1;
      return target[key as keyof typeof target];
    },
  }) as unknown as PreparedPipelineV2RunWaitIntent;
  const intentCaught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, {
    ...base,
    pipeline: ctx.pipeline,
    intent: proxyIntent,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  const intentError = expectInterventionFailure(intentCaught);
  expect(intentError.reason).toBe("invalid_options");
  expect(intentTraps).toBe(0);
  const clonedIntentCaught = await applyPipelineV2ContinueStageResumeWithIo(countingOps, {
    ...base,
    pipeline: ctx.pipeline,
    intent: { ...ctx.intent } as unknown as PreparedPipelineV2RunWaitIntent,
  }).then(
    () => null,
    (cause: unknown) => cause,
  );
  const clonedIntentError = expectInterventionFailure(clonedIntentCaught);
  expect(clonedIntentError.reason).toBe("invalid_options");
  expect(interventionCalls).toBe(0);
  expect(resumeCalls).toBe(0);
  expect(kinds(reopened)).toEqual([]);
});

// --- export surfaces and source scan -----------------------------------------

test("the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_continue_stage_resume_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinueStageResumeControllerError",
    "resumePipelineV2RunAfterContinueStageIntervention",
  ]);
  const internalModule = await import("../src/pipeline_v2_continue_stage_resume_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinueStageResumeControllerError",
    "applyPipelineV2ContinueStageResumeWithIo",
    "productionContinueStageResumeOps",
  ]);
  expect(productionContinueStageResumeOps.applyIntervention).toBe(applyPipelineV2ContinueStageIntervention);
  expect(productionContinueStageResumeOps.resumeRun).toBe(resumePipelineV2Run);
  expect(Object.isFrozen(productionContinueStageResumeOps)).toBe(true);
});

test("the handoff controller composes only the two existing facades (source scan)", async () => {
  const { readFile: readSource } = await import("node:fs/promises");
  for (const name of [
    "pipeline_v2_continue_stage_resume_controller.ts",
    "pipeline_v2_continue_stage_resume_controller_internal.ts",
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
    expect(countOf("pipeline_v2_run_plan_store")).toBe(0);
    expect(countOf("pipeline_v2_run_plan_candidate")).toBe(0);
    expect(countOf("pipeline_v2_run_plan_controller")).toBe(0);
    expect(countOf("pipeline_v2_run_plan_restore")).toBe(0);
    expect(countOf("pipeline_v2_run_plan_compiled")).toBe(0);
    expect(countOf("pipeline_v2_continue_stage_intent_controller")).toBe(0);
    expect(countOf("pipeline_v2_continued_stage_controller")).toBe(0);
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
});
