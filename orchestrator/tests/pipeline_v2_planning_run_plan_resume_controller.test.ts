import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  parsePipelineV2RunState,
  PIPELINE_V2_FAILURE_REASONS,
  type PipelineV2AgentExecutionState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  prepareRunProject,
  snapshotRunInputs,
  prepareActivationData,
  acceptActivationOutputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { restorePipelineV2RuntimeContext } from "../src/pipeline_v2_resume_context.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import {
  applyPipelineV2PlanningRunPlanHandoff,
  PipelineV2PlanningRunPlanHandoffControllerError,
} from "../src/pipeline_v2_planning_run_plan_handoff_controller.ts";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
  type PipelineV2ExecutionSession,
  type PipelineV2ToolSession,
  type PipelineV2WorkerRunResult,
} from "../src/pipeline_v2_coordinator.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import {
  PipelineV2PlanningRunPlanResumeControllerError,
  resumePipelineV2RunAfterPlanningRunPlanHandoff,
} from "../src/pipeline_v2_planning_run_plan_resume_controller.ts";
import {
  applyPipelineV2PlanningRunPlanResumeWithIo,
  productionPlanningRunPlanResumeOps,
  type PipelineV2PlanningRunPlanResumeOps,
} from "../src/pipeline_v2_planning_run_plan_resume_controller_internal.ts";

/**
 * Tests for the production composition controller
 * `resumePipelineV2RunAfterPlanningRunPlanHandoff`: the completed
 * planning-run-plan handoff followed by the coordinator's resume
 * entrypoint in one fixed sequence, with the defensive verification of
 * the handoff result and of the coordinator result union between them.
 * The prefix of every composed run is built with the real substrate
 * itself. Everything is deterministic: no sleeps, no LLM, no Docker
 * Helper, no launcher credential. The runner and the CLI stay untouched.
 */

const RUN_ID = "resume-handoff-run";
const INITIAL_BUDGET = 2;

const TWO_STAGE_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
    - id: review
      entry_state: review_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: planner2
      role: planning
      plan_output: plan2
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: review_entry
      role: stage
      stage_template: review

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
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
        to: planner2

  - id: planner2
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: review_entry

  - id: review_entry
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

const P1_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

const P2_POINTER_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [],
};

const P3_TWO_STAGE_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-c", depends_on: [] }, { id: "task-d", depends_on: ["task-c"] }] },
  ],
  new_tasks: [
    { id: "task-c", body: "Body C" },
    { id: "task-d", body: "Body D" },
  ],
};

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clockCounter));
}

let sessionCounter = 0;
function nextSession(label: string): string {
  sessionCounter += 1;
  return `sess-${sessionCounter}-${label}`;
}

interface Recording {
  commands: PipelineV2RunCommand[];
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
}

function recordingSink(inner: PipelineV2RunStateSink): Recording {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    get snapshot() {
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command) {
      commands.push({ ...command });
      await inner.dispatch(command);
    },
  };
}

function kindsOf(recording: Recording): string[] {
  return recording.commands.map((command) => command.kind as string);
}

interface ProofFixture {
  root: string;
  bundle: string;
  stateRoot: string;
  runRoot: string;
}

async function setupFixture(): Promise<ProofFixture> {
  const root = await mkdtemp(join(tmpdir(), "resume-handoff-controller-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "pipeline.yaml"), TWO_STAGE_YAML);
  await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.txt"), "TASK-BODY\n", { mode: 0o600 });
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const runRoot = join(root, "runs", RUN_ID);
  await mkdir(runRoot, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, stateRoot, runRoot };
}

/** The honest restart-boundary reopen: a fresh sink and a freshly loaded pipeline. */
async function reopen(fixture: ProofFixture): Promise<{ sink: PipelineV2RunStateSink; recording: Recording; pipeline: ResolvedPipelineV2; state: PipelineV2RunState }> {
  const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
  const state = sink.snapshot;
  if (state === null) {
    throw new Error("the reopened run lost its durable state");
  }
  const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
  return { sink, recording: recordingSink(sink), pipeline, state };
}

/** One planning activation through the real runtime data plane and reducer. */
async function runPlanningActivation(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  recording: Recording,
  stateId: string,
  profile: string,
  outputId: string,
  executionIndex: number,
  proposal: unknown,
  sessionLabel: string,
): Promise<AcceptedStateOutput[]> {
  const prep: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await recording.dispatch({ kind: "start_agent_execution", stateId, profile, executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: nextSession(sessionLabel) },
    { kind: "agent_tool_session_created", sessionId: nextSession(sessionLabel) },
    { kind: "agent_running" },
  ] as PipelineV2RunCommand[]) {
    await recording.dispatch(command);
  }
  await writeFile(join(prep.outputs_root, outputId), JSON.stringify(proposal), { mode: 0o600 });
  const records: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline, prep);
  await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records.map((record) => ({ id: record.output, digest: record.digest })) });
  await recording.dispatch({ kind: "agent_cleanup_completed" });
  return [...accepted, ...records];
}

/** One zero-output stage execution recorded through the real reducer. */
async function runRawStageActivation(
  recording: Recording,
  stateId: string,
  profile: string,
  iterationIndex: number,
  sessionLabel: string,
): Promise<void> {
  await recording.dispatch({ kind: "start_agent_execution", stateId, profile, executionRole: "stage", iterationIndex });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: nextSession(sessionLabel) },
    { kind: "agent_tool_session_created", sessionId: nextSession(sessionLabel) },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ] as PipelineV2RunCommand[]) {
    await recording.dispatch(command);
  }
}

async function enterWait(
  fixture: ProofFixture,
  recording: Recording,
  waitIndex: number,
  anchor: number,
  stateId: string,
  reviseTarget: string,
): Promise<void> {
  const actions = [
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: reviseTarget },
  ];
  const request = preparePipelineV2WaitRequest({
    schema_version: 1,
    run_id: RUN_ID,
    wait_index: waitIndex,
    transition_count: anchor,
    state_id: stateId,
    reason: "stage_iteration_limit_exhausted",
    actions,
  });
  await recording.dispatch({
    kind: "run_waiting",
    stateId,
    reason: "stage_iteration_limit_exhausted",
    requestSha256: request.sha256,
    actions,
  });
  await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
}

interface PrefixCoordinates {
  fixture: ProofFixture;
  runId: string;
}

/**
 * The honest durable prefix with two complete revise_task cycles, built
 * only through the existing facades, the real reducer and the runtime
 * data plane. Returns filesystem coordinates only — no pipeline object,
 * no compiled plan, no proposal, no accepted output, no snapshot.
 */
async function buildTwoCyclePrefix(): Promise<PrefixCoordinates> {
  const fixture = await setupFixture();
  try {
    const pipeline1 = await loadPipelineV2(fixture.bundle);
    clockCounter = 0;
    const sink1 = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const recording1 = recordingSink(sink1);
    await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
    const runInputs = await snapshotRunInputs(pipeline1, [{ id: "task", path: join(fixture.root, "userdata", "task.txt") }], fixture.runRoot);
    await recording1.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline1),
      inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    });
    let accepted: AcceptedStateOutput[] = [];
    accepted = await runPlanningActivation(pipeline1, runInputs, accepted, recording1, "architect", "architect", "plan", 1, P1_PROPOSAL, "planning-1");
    const accepted1 = await acceptPipelineV2PlanningRunPlan({ pipeline: pipeline1, runRoot: fixture.runRoot, sink: recording1 });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted1.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: recording1 });
    await recording1.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await runRawStageActivation(recording1, "dev_entry", "coder", 1, "stage-1");
    await recording1.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "planner2", transition_index: 0 },
      executionIndex: 2,
    });
    await enterWait(fixture, recording1, 1, 2, "planner2", "architect");
    await applyPipelineV2ReviseTaskIntervention({
      pipeline: pipeline1,
      runRoot: fixture.runRoot,
      sink: recording1,
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Body A revised",
    });

    // Planning execution 3 on architect (pointer-only r2 proposal).
    {
      const reopened = await reopen(fixture);
      const restored = await restorePipelineV2RuntimeContext(reopened.pipeline, reopened.state, fixture.runRoot);
      accepted = await runPlanningActivation(reopened.pipeline, restored.run_inputs, restored.accepted_outputs, reopened.recording, "architect", "architect", "plan", restored.next_execution_index, P2_POINTER_PROPOSAL, "planning-2");
    }

    // Handoff #1 selects the FIRST stage (the transition architect ->
    // dev_entry) and cycles the cursor back to dev_entry; then the second
    // stage cycle and the second revise intervention.
    {
      const reopened = await reopen(fixture);
      const firstHandoff = await applyPipelineV2PlanningRunPlanHandoff({
        pipeline: reopened.pipeline,
        runRoot: fixture.runRoot,
        sink: reopened.recording,
        stageId: "stage-1",
        initialBudget: INITIAL_BUDGET,
      });
      expect(firstHandoff.stage_id).toBe("stage-1");
      expect(firstHandoff.generation_index).toBe(2);
      await runRawStageActivation(reopened.recording, "dev_entry", "coder", 1, "stage-2");
      await reopened.recording.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "planner2", transition_index: 0 },
        executionIndex: 4,
      });
      await enterWait(fixture, reopened.recording, 2, 4, "planner2", "planner2");
      await applyPipelineV2ReviseTaskIntervention({
        pipeline: reopened.pipeline,
        runRoot: fixture.runRoot,
        sink: reopened.recording,
        runId: RUN_ID,
        waitIndex: 2,
        taskId: "task-b",
        taskBody: "Body B revised",
      });
    }

    // Planning execution 5 on planner2 (the two-stage r3 proposal).
    {
      const reopened = await reopen(fixture);
      const restored = await restorePipelineV2RuntimeContext(reopened.pipeline, reopened.state, fixture.runRoot);
      accepted = await runPlanningActivation(reopened.pipeline, restored.run_inputs, restored.accepted_outputs, reopened.recording, "planner2", "architect", "plan2", restored.next_execution_index, P3_TWO_STAGE_PROPOSAL, "planning-3");
      void accepted;
    }

    const finalCheck = await reopen(fixture);
    const validated = parsePipelineV2RunState(JSON.stringify(finalCheck.state));
    if (validated.executions.length !== validated.transitions.length + 1) {
      throw new Error(`the prefix did not reach the settled-unbound boundary (${validated.executions.length} executions, ${validated.transitions.length} transitions)`);
    }
    return { fixture, runId: RUN_ID };
  } catch (cause) {
    await rm(fixture.root, { recursive: true, force: true });
    throw cause;
  }
}

/** Full deterministic filesystem fingerprint of the run tree. */
async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
      const path = join(dir, entry.name);
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = await stat(path);
      if (info.isDirectory()) {
        lines.push(`${relative}/ dir ${info.mode.toString(8)}`);
        await walk(path, relative);
      } else {
        const bytes = info.isFile() ? (await readFile(path)).toString("base64") : "non-file";
        lines.push(`${relative} ${info.mode.toString(8)} ${bytes}`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

const NEUTRAL_CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
  freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
};

interface FakePair {
  stateId: string;
  activationIndex: number;
  runCount: number;
  executionCleanupCount: number;
  toolCleanupCount: number;
  toolSessionId: string;
}

/** The fake agent runtime whose worker run returns `worker_failed`. */
function fakeWorkerFailedRuntime(): { runtime: PipelineV2AgentRuntime; pairs: FakePair[]; events: string[] } {
  const pairs: FakePair[] = [];
  const events: string[] = [];
  const runtime = {
    createExecutionSession: async (state: { id: string }, activation: PreparedActivationData): Promise<PipelineV2ExecutionSession> => {
      const pair: FakePair = {
        stateId: state.id,
        activationIndex: activation.activation_index,
        runCount: 0,
        executionCleanupCount: 0,
        toolCleanupCount: 0,
        toolSessionId: `tool-resume-${pairs.length + 1}`,
      };
      pairs.push(pair);
      events.push(`create-exec:${state.id}:${activation.activation_index}`);
      return {
        sessionId: `exec-resume-${pairs.length}`,
        runAgent: async (toolSession: PipelineV2ToolSession): Promise<PipelineV2WorkerRunResult> => {
          pair.runCount += 1;
          pair.toolSessionId = toolSession.sessionId;
          events.push(`run:${state.id}`);
          return { status: "failed", reason: "worker_failed" };
        },
        cleanup: async (): Promise<void> => {
          pair.executionCleanupCount += 1;
          events.push(`cleanup-exec:${state.id}`);
        },
      } as unknown as PipelineV2ExecutionSession;
    },
    createToolSession: async (state: { id: string }, activation: PreparedActivationData): Promise<PipelineV2ToolSession> => {
      const pair = pairs[pairs.length - 1];
      if (pair === undefined) {
        throw new Error("no execution session was created for this activation");
      }
      events.push(`create-tool:${state.id}:${activation.activation_index}`);
      return {
        sessionId: pair.toolSessionId,
        cleanup: async (): Promise<void> => {
          pair.toolCleanupCount += 1;
          events.push(`cleanup-tool:${state.id}`);
        },
      } as unknown as PipelineV2ToolSession;
    },
  };
  return { runtime: runtime as unknown as PipelineV2AgentRuntime, pairs, events };
}

function expectControllerError(
  cause: unknown,
  reason: "invalid_options" | "invalid_result",
): PipelineV2PlanningRunPlanResumeControllerError {
  expect(cause).toBeInstanceOf(PipelineV2PlanningRunPlanResumeControllerError);
  const error = cause as PipelineV2PlanningRunPlanResumeControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

/**
 * Forwards to the real handoff and captures its result; the resume is a
 * stub — the case matrices below are presented against the unchanged
 * post-handoff boundary, so the first composed call must not consume it.
 */
function handoffCaptureOps(captured: { value: unknown }): PipelineV2PlanningRunPlanResumeOps {
  return {
    applyHandoff: (async (...args: unknown[]) => {
      const result = await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
      captured.value = result;
      return result;
    }) as typeof applyPipelineV2PlanningRunPlanHandoff,
    resumeRun: (async () => ({
      ok: false,
      refused: true,
      reason: "missing_state",
      state: null,
    })) as unknown as typeof resumePipelineV2Run,
  };
}

/** A resume stub that must never be called. */
function neverResumeOps(calls: { resume: number }): PipelineV2PlanningRunPlanResumeOps {
  return {
    applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
    resumeRun: (async () => {
      calls.resume += 1;
      throw new Error("the resume must not run");
    }) as unknown as typeof resumePipelineV2Run,
  };
}

interface CountingSink {
  sink: PipelineV2CoordinatorStateSink;
  reads: () => number;
}

/** Counts the authoritative snapshot getter reads of the wrapped sink. */
function countingSnapshotSink(inner: PipelineV2RunStateSink): CountingSink {
  let count = 0;
  const sink = {
    get snapshot() {
      count += 1;
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    dispatch: async (command: PipelineV2RunCommand) => {
      await inner.dispatch(command);
    },
  };
  return { sink: sink as unknown as PipelineV2CoordinatorStateSink, reads: () => count };
}

test("1. honest C0 after restart on the two-cycle prefix: the handoff suffix then exactly the seven resume commands", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    // Restart 1: the caller reopens the sink and loads the pipeline from
    // the durable bundle root; then the composed facade runs.
    const before = await reopen(fixture);
    const boundary = before.state;
    expect(boundary.executions).toHaveLength(5);
    expect(boundary.transitions).toHaveLength(4);
    expect(boundary.cursor).toEqual({ current_state: "planner2", transition_count: 4 });
    const revisionAtBoundary = boundary.revision;

    const counting = countingSnapshotSink(before.sink);
    const readsAfterCapture = counting.reads();
    expect(readsAfterCapture).toBe(0);
    const result = await resumePipelineV2RunAfterPlanningRunPlanHandoff({
      pipeline: before.pipeline,
      runRoot: fixture.runRoot,
      sink: counting.sink,
      runtime: fakeWorkerFailedRuntime().runtime,
      control: NEUTRAL_CONTROL,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
    });
    const readsAfterResume = counting.reads();

    // The ordinary worker failure, never a refusal.
    if (result.ok || "waiting" in result || "planReady" in result) {
      throw new Error("the composed resume was expected to fail with worker_failed");
    }
    expect("refused" in result).toBe(false);
    expect(result.reason).toBe("worker_failed");

    const finalState = counting.sink.snapshot as PipelineV2RunState;
    expect(finalState.executions).toHaveLength(6);
    const successor = finalState.executions[5] as PipelineV2AgentExecutionState;
    expect(successor.index).toBe(6);
    expect(successor.state_id).toBe("review_entry");
    expect(successor.execution_role).toBe("stage");
    expect(successor.iteration_index).toBe(1);
    expect(successor.phase).toBe("failed");
    expect(successor.failure_reason).toBe("worker_failed");
    expect(successor.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
    expect(finalState.failure).toEqual({ reason: "worker_failed" });
    expect(finalState.status).toBe("failed");
    expect(finalState.cursor).toEqual({ current_state: "review_entry", transition_count: 5 });
    expect(finalState.plan_revisions.map((plan) => plan.revision)).toEqual([1, 2, 3]);
    expect(finalState.generations).toHaveLength(3);
    expect(finalState.generations[2]!.stage_id).toBe("stage-2");
    expect(finalState.generations[2]!.template_id).toBe("review");
    expect(finalState.generations[2]!.open_iteration).toBeDefined();
    expect(finalState.waits.map((wait) => wait.response?.action_id)).toEqual(["revise_task", "revise_task"]);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("2. crash seam: handoff durable, sentinel resume, C1 zero-dispatch retry, then the seven resume commands", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    // The composed call's handoff part completes fully; the resume part is
    // an injected stub that throws a sentinel before its first effect.
    const first = await reopen(fixture);
    const capturedHandoff = { value: null as unknown };
    const sentinel: { code: string } = Object.freeze({ code: "SENTINEL" });
    let resumeCalls = 0;
    const crashingOps: PipelineV2PlanningRunPlanResumeOps = {
      applyHandoff: (async (...args: unknown[]) => {
        const result = await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
        capturedHandoff.value = result;
        return result;
      }) as typeof applyPipelineV2PlanningRunPlanHandoff,
      resumeRun: (async () => {
        resumeCalls += 1;
        throw sentinel;
      }) as unknown as typeof resumePipelineV2Run,
    };
    const firstRecording = recordingSink(first.sink);
    let firstError: unknown = null;
    try {
      await applyPipelineV2PlanningRunPlanResumeWithIo(crashingOps, {
        pipeline: first.pipeline,
        runRoot: fixture.runRoot,
        sink: firstRecording,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
    } catch (cause) {
      firstError = cause;
    }
    // The sentinel passes by object identity.
    expect(firstError).toBe(sentinel);
    expect(resumeCalls).toBe(1);
    // The handoff part wrote its full suffix; the resume wrote nothing.
    expect(kindsOf(firstRecording)).toEqual([
      "task_revision_accepted",
      "task_revision_accepted",
      "plan_revision_accepted",
      "stage_generation_closed",
      "stage_generation_opened",
      "stage_iteration_opened",
      "transition_committed",
    ]);

    const committed = await reopen(fixture);
    const boundaryState = committed.state;
    const boundaryRevision = boundaryState.revision;
    const boundaryFingerprint = await fingerprint(fixture.runRoot);

    // The repeated public facade call: Branch B/C1 zero dispatch, the
    // handoff projection and the filesystem untouched; then the seven
    // resume commands.
    const counting = countingSnapshotSink(committed.sink);
    const fake = fakeWorkerFailedRuntime();
    const result = await resumePipelineV2RunAfterPlanningRunPlanHandoff({
      pipeline: committed.pipeline,
      runRoot: fixture.runRoot,
      sink: counting.sink,
      runtime: fake.runtime,
      control: NEUTRAL_CONTROL,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
    });
    if (result.ok || "waiting" in result || "planReady" in result) {
      throw new Error("the composed resume was expected to fail with worker_failed");
    }
    expect("refused" in result).toBe(false);
    expect(result.reason).toBe("worker_failed");
    const finalState = counting.sink.snapshot as PipelineV2RunState;
    expect(finalState.revision).toBe(boundaryRevision + 7);
    // The handoff projection was not rewritten by the retry.
    expect(finalState.waits).toEqual(boundaryState.waits);
    expect(finalState.task_revisions).toEqual(boundaryState.task_revisions);
    expect(finalState.plan_revisions).toEqual(boundaryState.plan_revisions);
    expect(finalState.generations).toEqual(boundaryState.generations);
    expect(finalState.transitions).toEqual(boundaryState.transitions);
    expect(finalState.executions).toHaveLength(6);
    expect(finalState.executions[5]!.state_id).toBe("review_entry");
    expect(finalState.executions[5]!.execution_role).toBe("stage");
    expect(finalState.executions[5]!.iteration_index).toBe(1);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
    expect(fake.pairs).toHaveLength(1);
    expect(fake.pairs[0]!.runCount).toBe(1);
    expect(fake.pairs[0]!.executionCleanupCount).toBe(1);
    expect(fake.pairs[0]!.toolCleanupCount).toBe(1);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("3. handoff error identity: a typed handoff error passes through and the resume never runs", async () => {
  const fixture = await setupFixture();
  try {
    const pipeline = await loadPipelineV2(fixture.bundle);
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const handoffError = new PipelineV2PlanningRunPlanHandoffControllerError("invalid_state", "the injected handoff refusal", null);
    const calls = { resume: 0 };
    const ops: PipelineV2PlanningRunPlanResumeOps = {
      applyHandoff: (async () => {
        throw handoffError;
      }) as unknown as typeof applyPipelineV2PlanningRunPlanHandoff,
      resumeRun: (async () => {
        calls.resume += 1;
        throw new Error("the resume must not run");
      }) as unknown as typeof resumePipelineV2Run,
    };
    let caught: unknown = null;
    try {
      await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline,
        runRoot: fixture.runRoot,
        sink,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBe(handoffError);
    expect(calls.resume).toBe(0);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("4. resume thrown-error identity: a thrown resume error passes through unchanged", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    const before = await reopen(fixture);
    const sentinel = new Error("RESUME-EXPLODED");
    const calls = { handoff: 0 };
    const ops: PipelineV2PlanningRunPlanResumeOps = {
      applyHandoff: (async (...args: unknown[]) => {
        calls.handoff += 1;
        return await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
      }) as typeof applyPipelineV2PlanningRunPlanHandoff,
      resumeRun: (async () => {
        throw sentinel;
      }) as unknown as typeof resumePipelineV2Run,
    };
    let caught: unknown = null;
    try {
      await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: before.pipeline,
        runRoot: fixture.runRoot,
        sink: before.recording,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBe(sentinel);
    expect(calls.handoff).toBe(1);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("5. malformed handoff-result matrix: every hostile result is the controller's own invalid_result and the resume never runs", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    // The real handoff runs once; its result and the post-handoff
    // authoritative state are the mutation bases for every case.
    const captured = { value: null as unknown };
    const first = await reopen(fixture);
    await applyPipelineV2PlanningRunPlanResumeWithIo(handoffCaptureOps(captured), {
      pipeline: first.pipeline,
      runRoot: fixture.runRoot,
      sink: first.recording,
      runtime: fakeWorkerFailedRuntime().runtime,
      control: NEUTRAL_CONTROL,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
    });
    const realResult = captured.value as Record<string, unknown>;
    expect(realResult).not.toBeNull();
    const postHandoff = await reopen(fixture);
    const authoritative = postHandoff.state;
    const calls = { resume: 0 };

    const progressedState: PipelineV2RunState = {
      ...authoritative,
      executions: [
        ...authoritative.executions,
        {
          index: authoritative.executions.length + 1,
          type: "agent",
          state_id: "review_entry",
          attempt: 1,
          profile: "coder",
          execution_role: "stage",
          iteration_index: 1,
          phase: "running",
        } as unknown as PipelineV2AgentExecutionState,
      ],
    };

    const cases: [string, unknown, PipelineV2CoordinatorStateSink | null][] = [
      ["primitive", 42, null],
      ["array", ["array"], null],
      ["null", null, null],
      ["empty-record", {}, null],
      ["missing-field", (() => {
        const clone = { ...realResult };
        delete clone.template_id;
        return clone;
      })(), null],
      ["extra-field", { ...realResult, hostile: true }, null],
      ["hostile-state", { ...realResult, state: { ...(realResult.state as PipelineV2RunState), revision: (realResult.state as PipelineV2RunState).revision + 1 } }, null],
      ["foreign-stage", { ...realResult, stage_id: "stage-1" }, null],
      ["foreign-budget", { ...realResult, initial_budget: 3 }, null],
      ["foreign-plan-revision", { ...realResult, plan_revision: 2 }, null],
      ["foreign-plan-digest", { ...realResult, plan_sha256: "hostile-digest" }, null],
      ["foreign-generation", { ...realResult, generation_index: 2 }, null],
      ["foreign-iteration", { ...realResult, iteration_index: 2 }, null],
      ["foreign-transition-index", { ...realResult, transition_index: 1 }, null],
      ["foreign-execution", { ...realResult, execution_index: 4 }, null],
      ["foreign-to-state", { ...realResult, to_state: "dev_entry" }, null],
      ["progressed-state", { ...realResult, state: progressedState }, {
        get snapshot() {
          return progressedState;
        },
        poisoned: false,
        dispatch: async () => {
          throw new Error("no dispatch expected");
        },
      } as unknown as PipelineV2CoordinatorStateSink],
    ];
    for (const [label, hostile, sinkOverride] of cases) {
      const ops: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: (async () => hostile) as unknown as typeof applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async () => {
          calls.resume += 1;
          throw new Error("the resume must not run");
        }) as unknown as typeof resumePipelineV2Run,
      };
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
            pipeline: postHandoff.pipeline,
            runRoot: fixture.runRoot,
            sink: sinkOverride ?? (postHandoff.sink as unknown as PipelineV2CoordinatorStateSink),
            runtime: fakeWorkerFailedRuntime().runtime,
            control: NEUTRAL_CONTROL,
            stageId: "stage-2",
            initialBudget: INITIAL_BUDGET,
          });
        } catch (cause) {
          return cause;
        }
        throw new Error(`the hostile ${label} handoff result was expected to fail`);
      })();
      const controllerError = expectControllerError(error, "invalid_result");
      // The error state is the post-handoff authoritative snapshot —
      // never the hostile presentation (and null only when the sink
      // itself was replaced by the progressed-state hostile).
      if (sinkOverride === null) {
        expect(controllerError.state).toBe(authoritative);
      } else {
        expect(controllerError.state).toBeNull();
      }
      const message = controllerError.message;
      expect(message).not.toContain("Body");
      expect(message).not.toContain("hostile-digest");
      expect(message).not.toContain(String(join(fixture.root, "")));
      void label;
    }
    expect(calls.resume).toBe(0);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("6. malformed resume-union matrix and positive vocabulary table: valid results return by identity", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    // The real handoff runs once; every resume-union case is presented by
    // a stub against the unchanged post-handoff boundary.
    const captured = { value: null as unknown };
    const first = await reopen(fixture);
    await applyPipelineV2PlanningRunPlanResumeWithIo(handoffCaptureOps(captured), {
      pipeline: first.pipeline,
      runRoot: fixture.runRoot,
      sink: first.recording,
      runtime: fakeWorkerFailedRuntime().runtime,
      control: NEUTRAL_CONTROL,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
    });
    const postHandoff = await reopen(fixture);
    const authoritative = postHandoff.state;

    const malformed: [string, unknown][] = [
      ["primitive", 42],
      ["array", ["array"]],
      ["null", null],
      ["ok-string", { ok: "yes", state: authoritative }],
      ["success-extra-field", { ok: true, state: authoritative, extra: 1 }],
      ["success-missing-state", { ok: true }],
      ["success-clone-state", { ok: true, state: { ...authoritative } }],
      ["refusal-false-discriminant", { ok: false, refused: false, reason: "missing_state", state: null }],
      ["refusal-foreign-reason", { ok: false, refused: true, reason: "foreign_reason", state: null }],
      ["refusal-undefined-reason", { ok: false, refused: true, reason: undefined, state: null }],
      ["refusal-extra-field", { ok: false, refused: true, reason: "missing_state", state: null, extra: 1 }],
      ["refusal-foreign-state", { ok: false, refused: true, reason: "missing_state", state: { ...authoritative } }],
      ["failure-refusal-only-reason", { ok: false, reason: "pipeline_mismatch", state: null }],
      ["failure-foreign-reason", { ok: false, reason: "worker_lost", state: null }],
      ["failure-undefined-reason", { ok: false, reason: undefined, state: null }],
      ["failure-own-refused-field", { ok: false, reason: "worker_failed", state: null, refused: undefined }],
      ["failure-extra-field", { ok: false, reason: "worker_failed", state: null, extra: 1 }],
      ["failure-foreign-state", { ok: false, reason: "worker_failed", state: { ...authoritative } }],
      ["waiting-extra-field", { ok: false, waiting: true, state: authoritative, extra: 1 }],
      ["waiting-false-discriminant", { ok: false, waiting: false, state: authoritative }],
      ["waiting-missing-state", { ok: false, waiting: true }],
      ["waiting-foreign-state", { ok: false, waiting: true, state: { ...authoritative } }],
      ["waiting-own-reason-field", { ok: false, waiting: true, reason: "foreign_reason", state: authoritative }],
      ["plan-ready-extra-field", { ok: false, planReady: true, state: authoritative, extra: 1 }],
      ["plan-ready-false-discriminant", { ok: false, planReady: false, state: authoritative }],
      ["plan-ready-missing-state", { ok: false, planReady: true }],
      ["plan-ready-foreign-state", { ok: false, planReady: true, state: { ...authoritative } }],
      ["plan-ready-own-waiting-field", { ok: false, planReady: true, waiting: true, state: authoritative }],
      ["plan-ready-own-reason-field", { ok: false, planReady: true, reason: "worker_failed", state: authoritative }],
    ];
    for (const [label, hostile] of malformed) {
      const ops: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async () => hostile) as unknown as typeof resumePipelineV2Run,
      };
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
            pipeline: postHandoff.pipeline,
            runRoot: fixture.runRoot,
            sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
            runtime: fakeWorkerFailedRuntime().runtime,
            control: NEUTRAL_CONTROL,
            stageId: "stage-2",
            initialBudget: INITIAL_BUDGET,
          });
        } catch (cause) {
          return cause;
        }
        throw new Error(`the hostile ${label} resume result was expected to fail`);
      })();
      const controllerError = expectControllerError(error, "invalid_result");
      expect(controllerError.state).toBe(authoritative);
      expect(controllerError.message).not.toContain("foreign_reason");
      expect(controllerError.message).not.toContain("worker_lost");
      void label;
    }

    // The positive vocabulary table: every valid refusal reason and every
    // canonical failure reason, each with a null and with the
    // authoritative state, plus the success union — returned by identity.
    const refusalReasons: PipelineV2ResumeRefusalReason[] = [
      "missing_state",
      "sink_poisoned",
      "run_id_mismatch",
      "invalid_state",
      "pipeline_mismatch",
      "run_layout_invalid",
      "run_input_modified",
      "accepted_output_modified",
      "internal_error",
    ];
    for (const reason of refusalReasons) {
      for (const state of [null, authoritative] as const) {
        const valid = { ok: false, refused: true, reason, state } as unknown as PipelineV2ResumeCoordinationResult;
        const ops: PipelineV2PlanningRunPlanResumeOps = {
          applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
          resumeRun: (async () => valid) as unknown as typeof resumePipelineV2Run,
        };
        const returned = await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
          pipeline: postHandoff.pipeline,
          runRoot: fixture.runRoot,
          sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
          runtime: fakeWorkerFailedRuntime().runtime,
          control: NEUTRAL_CONTROL,
          stageId: "stage-2",
          initialBudget: INITIAL_BUDGET,
        });
        expect(returned).toBe(valid);
      }
    }
    for (const reason of PIPELINE_V2_FAILURE_REASONS) {
      for (const state of [null, authoritative] as const) {
        const valid = { ok: false, reason, state } as unknown as PipelineV2ResumeCoordinationResult;
        const ops: PipelineV2PlanningRunPlanResumeOps = {
          applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
          resumeRun: (async () => valid) as unknown as typeof resumePipelineV2Run,
        };
        const returned = await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
          pipeline: postHandoff.pipeline,
          runRoot: fixture.runRoot,
          sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
          runtime: fakeWorkerFailedRuntime().runtime,
          control: NEUTRAL_CONTROL,
          stageId: "stage-2",
          initialBudget: INITIAL_BUDGET,
        });
        expect(returned).toBe(valid);
      }
    }
    {
      const valid = { ok: false, planReady: true, state: authoritative } as unknown as PipelineV2ResumeCoordinationResult;
      const ops: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async () => valid) as unknown as typeof resumePipelineV2Run,
      };
      const returned = await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: postHandoff.pipeline,
        runRoot: fixture.runRoot,
        sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
      expect(returned).toBe(valid);
    }
    {
      const valid = { ok: false, waiting: true, state: authoritative } as unknown as PipelineV2ResumeCoordinationResult;
      const ops: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async () => valid) as unknown as typeof resumePipelineV2Run,
      };
      const returned = await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: postHandoff.pipeline,
        runRoot: fixture.runRoot,
        sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
      expect(returned).toBe(valid);
    }
    {
      const valid = { ok: true, state: authoritative } as unknown as PipelineV2ResumeCoordinationResult;
      const ops: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async () => valid) as unknown as typeof resumePipelineV2Run,
      };
      const returned = await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: postHandoff.pipeline,
        runRoot: fixture.runRoot,
        sink: postHandoff.sink as unknown as PipelineV2CoordinatorStateSink,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
      expect(returned).toBe(valid);
    }
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("7. capture battery: options and ops read exactly once in contract order; mutations and replacements never redirect", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    const before = await reopen(fixture);
    const runtime = fakeWorkerFailedRuntime();
    const controlReads: string[] = [];
    const control: PipelineV2CoordinatorControl = {
      currentSignal: () => {
        controlReads.push("currentSignal");
        return null;
      },
      freezeSignal: () => {
        controlReads.push("freezeSignal");
        return null;
      },
    };
    // The options Proxy records the exact read order; a hostile extra
    // field is never read.
    const optionReads: string[] = [];
    const options = new Proxy(
      {
        pipeline: before.pipeline,
        runRoot: fixture.runRoot,
        sink: before.recording,
        runtime: runtime.runtime,
        control,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
        hostile: "never-read",
      },
      {
        get(target, property) {
          optionReads.push(String(property));
          return (target as Record<string, unknown>)[property as keyof typeof target];
        },
      },
    );
    // The ops Proxy records the member reads while the pending handoff is
    // in flight; a hostile extra member is never read.
    const opsReads: string[] = [];
    let releaseHandoff: (() => void) | null = null;
    const barrier = new Promise<void>((resolve) => {
      releaseHandoff = resolve;
    });
    let realHandoffDone = false;
    const pendingOps = new Proxy(
      {
        applyHandoff: (async (...args: unknown[]) => {
          const result = await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
          realHandoffDone = true;
          await barrier;
          return result;
        }) as typeof applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: resumePipelineV2Run,
      },
      {
        get(target, property) {
          opsReads.push(String(property));
          return (target as Record<string, unknown>)[property as keyof typeof target];
        },
      },
    ) as unknown as PipelineV2PlanningRunPlanResumeOps;
    const pending = applyPipelineV2PlanningRunPlanResumeWithIo(pendingOps, options as never);
    await Promise.resolve();
    await Promise.resolve();
    if (!realHandoffDone) {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
    // Mutate everything the caller owns while the handoff is pending.
    (options as unknown as Record<string, unknown>)["stageId"] = "stage-1";
    (options as unknown as Record<string, unknown>)["initialBudget"] = 9;
    (options as unknown as Record<string, unknown>)["runRoot"] = "/elsewhere";
    (runtime.runtime as unknown as Record<string, unknown>)["createExecutionSession"] = async () => {
      throw new Error("the replacement runtime must not be called");
    };
    (control as unknown as Record<string, unknown>)["currentSignal"] = () => "SIGINT";
    releaseHandoff!();
    const result = await pending;

    // The options Proxy read exactly the seven contract fields in order.
    expect(optionReads).toEqual(["pipeline", "runRoot", "sink", "runtime", "control", "stageId", "initialBudget"]);
    // The ops Proxy read exactly the two members in order.
    expect(opsReads).toEqual(["applyHandoff", "resumeRun"]);
    if (result.ok || "waiting" in result || "planReady" in result) {
      throw new Error("the composed resume was expected to fail with worker_failed");
    }
    expect("refused" in result).toBe(false);
    expect(result.reason).toBe("worker_failed");
    // The replacement runtime function was never called: one pair came
    // from the original runtime.
    expect(runtime.pairs).toHaveLength(1);
    expect(runtime.pairs[0]!.runCount).toBe(1);
    // The captured control, not the replacement, served the coordinator.
    expect(controlReads[0]).toBe("currentSignal");
    const finalState = before.recording.snapshot as PipelineV2RunState;
    expect(finalState.executions).toHaveLength(6);
    expect(finalState.executions[5]!.state_id).toBe("review_entry");
    expect(finalState.failure).toEqual({ reason: "worker_failed" });
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("8. pipeline clone and Proxy are refused by the provenance gate with zero traps and zero facade calls", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    const before = await reopen(fixture);
    const calls = { handoff: 0, resume: 0 };
    const ops: PipelineV2PlanningRunPlanResumeOps = {
      applyHandoff: (async () => {
        calls.handoff += 1;
        throw new Error("the handoff must not run");
      }) as unknown as typeof applyPipelineV2PlanningRunPlanHandoff,
      resumeRun: (async () => {
        calls.resume += 1;
        throw new Error("the resume must not run");
      }) as unknown as typeof resumePipelineV2Run,
    };
    // The spread clone loses the provenance registration.
    const clone = { ...before.pipeline } as unknown as ResolvedPipelineV2;
    let cloneError: unknown = null;
    try {
      await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: clone,
        runRoot: fixture.runRoot,
        sink: before.recording,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
    } catch (cause) {
      cloneError = cause;
    }
    expect(cloneError).not.toBeInstanceOf(PipelineV2PlanningRunPlanResumeControllerError);
    expect(cloneError).toBeInstanceOf(Error);
    expect(calls).toEqual({ handoff: 0, resume: 0 });

    // The Proxy pipeline: the provenance gate rejects it with zero traps.
    let traps = 0;
    const proxied = new Proxy(before.pipeline as unknown as Record<string, unknown>, {
      get(target, property) {
        traps += 1;
        return target[property as keyof typeof target];
      },
      ownKeys(target) {
        traps += 1;
        return Reflect.ownKeys(target);
      },
    }) as unknown as ResolvedPipelineV2;
    let proxyError: unknown = null;
    try {
      await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
        pipeline: proxied,
        runRoot: fixture.runRoot,
        sink: before.recording,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
    } catch (cause) {
      proxyError = cause;
    }
    expect(proxyError).not.toBeInstanceOf(PipelineV2PlanningRunPlanResumeControllerError);
    expect(traps).toBe(0);
    expect(calls).toEqual({ handoff: 0, resume: 0 });
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("9. invalid runtime, control and sink shapes: typed own errors without native TypeErrors or facade calls", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    const before = await reopen(fixture);
    const calls = { handoff: 0, resume: 0 };
    const ops: PipelineV2PlanningRunPlanResumeOps = {
      applyHandoff: (async () => {
        calls.handoff += 1;
        throw new Error("the handoff must not run");
      }) as unknown as typeof applyPipelineV2PlanningRunPlanHandoff,
      resumeRun: (async () => {
        calls.resume += 1;
        throw new Error("the resume must not run");
      }) as unknown as typeof resumePipelineV2Run,
    };
    const base = {
      pipeline: before.pipeline,
      runRoot: fixture.runRoot,
      sink: before.recording,
      runtime: fakeWorkerFailedRuntime().runtime,
      control: NEUTRAL_CONTROL,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
    };
    const runtimes: unknown[] = [null, undefined, 42, "runtime", [], {}, { createExecutionSession: 42 }, { createExecutionSession: () => undefined }];
    for (const runtime of runtimes) {
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, { ...base, runtime } as never);
        } catch (cause) {
          return cause;
        }
        throw new Error("the hostile runtime was expected to be refused");
      })();
      expectControllerError(error, "invalid_options");
    }
    const controls: unknown[] = [null, undefined, 42, "control", [], {}, { currentSignal: 42 }, { currentSignal: () => null }, { currentSignal: () => null, freezeSignal: 42 }];
    for (const control of controls) {
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, { ...base, control } as never);
        } catch (cause) {
          return cause;
        }
        throw new Error("the hostile control was expected to be refused");
      })();
      expectControllerError(error, "invalid_options");
    }
    const sinks: unknown[] = [null, undefined, 42, "sink", [], {}, { snapshot: null, poisoned: false }, { snapshot: null, poisoned: false, dispatch: 42 }];
    for (const sink of sinks) {
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, { ...base, sink } as never);
        } catch (cause) {
          return cause;
        }
        throw new Error("the hostile sink was expected to be refused");
      })();
      expectControllerError(error, "invalid_options");
    }
    const optionShapes: unknown[] = [null, undefined, 42, "options", []];
    for (const options of optionShapes) {
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, options);
        } catch (cause) {
          return cause;
        }
        throw new Error("the hostile options were expected to be refused");
      })();
      expectControllerError(error, "invalid_options");
    }
    for (const [label, override] of [
      ["relative-runRoot", { runRoot: "runs/x" }],
      ["empty-runRoot", { runRoot: "" }],
      ["unsafe-stage-id", { stageId: "../escape" }],
      ["empty-stage-id", { stageId: "" }],
      ["zero-budget", { initialBudget: 0 }],
      ["negative-budget", { initialBudget: -1 }],
      ["fractional-budget", { initialBudget: 1.5 }],
      ["string-budget", { initialBudget: "2" }],
    ] as const) {
      const error = await (async () => {
        try {
          await applyPipelineV2PlanningRunPlanResumeWithIo(ops, { ...base, ...override } as never);
        } catch (cause) {
          return cause;
        }
        throw new Error(`the hostile ${label} was expected to be refused`);
      })();
      expectControllerError(error, "invalid_options");
      void label;
    }
    expect(calls).toEqual({ handoff: 0, resume: 0 });
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("10. snapshot reads: exactly one post-handoff and one post-resume read; malformed post-call snapshots are own refusals", async () => {
  const prefix = await buildTwoCyclePrefix();
  const { fixture } = prefix;
  try {
    // Part A: the honest flow reads exactly once after the handoff and
    // exactly once after the resume.
    {
      const before = await reopen(fixture);
      const counting = countingSnapshotSink(before.sink);
      let readsAfterHandoffCall = -1;
      let readsAtResumeStart = -1;
      let readsAfterResumeCall = -1;
      const measuringOps: PipelineV2PlanningRunPlanResumeOps = {
        applyHandoff: (async (...args: unknown[]) => {
          const result = await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
          readsAfterHandoffCall = counting.reads();
          return result;
        }) as typeof applyPipelineV2PlanningRunPlanHandoff,
        resumeRun: (async (...args: unknown[]) => {
          readsAtResumeStart = counting.reads();
          const result = await resumePipelineV2Run(...(args as Parameters<typeof resumePipelineV2Run>));
          readsAfterResumeCall = counting.reads();
          return result;
        }) as typeof resumePipelineV2Run,
      };
      await applyPipelineV2PlanningRunPlanResumeWithIo(measuringOps, {
        pipeline: before.pipeline,
        runRoot: fixture.runRoot,
        sink: counting.sink,
        runtime: fakeWorkerFailedRuntime().runtime,
        control: NEUTRAL_CONTROL,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
      expect(readsAtResumeStart - readsAfterHandoffCall).toBe(1);
      expect(counting.reads() - readsAfterResumeCall).toBe(1);
    }

    // Part B: a malformed post-handoff snapshot is the controller's own
    // invalid_result; the value never enters the error state and the
    // resume never runs. Part A's real resume consumed the shared prefix
    // (the run is durably failed there), so this part rebuilds its own;
    // its three iterations share it (the first handoff writes the suffix,
    // the later ones recognize Branch B with zero dispatch).
    {
      const prefixB = await buildTwoCyclePrefix();
      const fixtureB = prefixB.fixture;
      try {
        for (const value of [undefined, 42, ["array"]]) {
          const before = await reopen(fixtureB);
          const poison = { active: false, values: [value] as unknown[] };
          const poisonable = {
            get snapshot(): unknown {
              if (poison.active) {
                poison.active = false;
                return poison.values.shift();
              }
              return before.sink.snapshot;
            },
            get poisoned() {
              return before.sink.poisoned;
            },
            dispatch: async (command: PipelineV2RunCommand) => {
              await before.sink.dispatch(command);
            },
          };
          const calls = { resume: 0 };
          const ops: PipelineV2PlanningRunPlanResumeOps = {
            applyHandoff: (async (...args: unknown[]) => {
              const result = await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
              poison.active = true;
              return result;
            }) as typeof applyPipelineV2PlanningRunPlanHandoff,
            resumeRun: (async () => {
              calls.resume += 1;
              throw new Error("the resume must not run");
            }) as unknown as typeof resumePipelineV2Run,
          };
          const error = await (async () => {
            try {
              await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
                pipeline: before.pipeline,
                runRoot: fixtureB.runRoot,
                sink: poisonable as unknown as PipelineV2CoordinatorStateSink,
                runtime: fakeWorkerFailedRuntime().runtime,
                control: NEUTRAL_CONTROL,
                stageId: "stage-2",
                initialBudget: INITIAL_BUDGET,
              });
            } catch (cause) {
              return cause;
            }
            throw new Error("the malformed post-handoff snapshot was expected to be refused");
          })();
          const controllerError = expectControllerError(error, "invalid_result");
          expect(controllerError.state).toBeNull();
          expect(controllerError.message).not.toContain(String(42));
          expect(calls.resume).toBe(0);
        }
      } finally {
        await rm(fixtureB.root, { recursive: true, force: true });
      }
    }

    // Part C: a malformed post-resume snapshot is the controller's own
    // invalid_result carrying the verified post-handoff snapshot. Its own
    // prefix; its three iterations share it (the stub resume writes
    // nothing, so the handoff projection stays at the boundary).
    {
      const prefixC = await buildTwoCyclePrefix();
      const fixtureC = prefixC.fixture;
      try {
        for (const value of [undefined, 42, ["array"]]) {
          const before = await reopen(fixtureC);
          const poison = { active: false, values: [value] as unknown[] };
          const poisonable = {
            get snapshot(): unknown {
              if (poison.active) {
                poison.active = false;
                return poison.values.shift();
              }
              return before.sink.snapshot;
            },
            get poisoned() {
              return before.sink.poisoned;
            },
            dispatch: async (command: PipelineV2RunCommand) => {
              await before.sink.dispatch(command);
            },
          };
          const verifiedBeforeResume = { value: null as unknown };
          const calls = { resume: 0 };
          const ops: PipelineV2PlanningRunPlanResumeOps = {
            applyHandoff: (async (...args: unknown[]) => {
              return await applyPipelineV2PlanningRunPlanHandoff(...(args as Parameters<typeof applyPipelineV2PlanningRunPlanHandoff>));
            }) as typeof applyPipelineV2PlanningRunPlanHandoff,
            resumeRun: (async () => {
              calls.resume += 1;
              // The stub resume writes nothing; the verified post-handoff
              // snapshot stays authoritative.
              verifiedBeforeResume.value = before.sink.snapshot;
              poison.active = true;
              return { ok: false, reason: "worker_failed", state: null };
            }) as unknown as typeof resumePipelineV2Run,
          };
          const error = await (async () => {
            try {
              await applyPipelineV2PlanningRunPlanResumeWithIo(ops, {
                pipeline: before.pipeline,
                runRoot: fixtureC.runRoot,
                sink: poisonable as unknown as PipelineV2CoordinatorStateSink,
                runtime: fakeWorkerFailedRuntime().runtime,
                control: NEUTRAL_CONTROL,
                stageId: "stage-2",
                initialBudget: INITIAL_BUDGET,
              });
            } catch (cause) {
              return cause;
            }
            throw new Error("the malformed post-resume snapshot was expected to be refused");
          })();
          const controllerError = expectControllerError(error, "invalid_result");
          expect(controllerError.state).toBe(verifiedBeforeResume.value as PipelineV2RunState | null);
          expect(controllerError.message).not.toContain("array");
          expect(calls.resume).toBe(1);
        }
      } finally {
        await rm(fixtureC.root, { recursive: true, force: true });
      }
    }
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("11. export surfaces: the public and internal runtime keys are exact", async () => {
  const publicModule = await import("../src/pipeline_v2_planning_run_plan_resume_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual(["PipelineV2PlanningRunPlanResumeControllerError", "resumePipelineV2RunAfterPlanningRunPlanHandoff"]);
  const internalModule = await import("../src/pipeline_v2_planning_run_plan_resume_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2PlanningRunPlanResumeControllerError",
    "applyPipelineV2PlanningRunPlanResumeWithIo",
    "productionPlanningRunPlanResumeOps",
  ]);
});

test("12. source scan: only the two facades; no reducer/store/fs/parser/serializer/digest/registry/runner", async () => {
  const sourceRoot = join(import.meta.dir, "..", "src");
  const facadeSource = await readFile(join(sourceRoot, "pipeline_v2_planning_run_plan_resume_controller.ts"), "utf8");
  const internalSource = await readFile(join(sourceRoot, "pipeline_v2_planning_run_plan_resume_controller_internal.ts"), "utf8");
  for (const banned of [
    "reducePipelineV2RunCommand",
    "validatePipelineV2RunState",
    "pipeline_v2_state_store",
    "node:fs",
    "canonicalJson",
    "CryptoHasher",
    "createHash",
    "JSON.parse",
    "WeakSet",
    "WeakMap",
    "preparePipelineV2RunPlanCandidate",
    "prepareWaitIntent",
    "loadPipelineV2WaitIntent",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_state_sink",
    "snapshotRunInputs",
    "prepareActivationData",
    "acceptActivationOutputs",
    "pipeline_v2_run_plan_provenance",
    "pipeline_v2_runner",
    "main.ts",
    "runPipelineV2",
  ]) {
    expect(facadeSource.includes(banned), `the facade must not reference ${banned}`).toBe(false);
    expect(internalSource.includes(banned), `the internal core must not reference ${banned}`).toBe(false);
  }
  for (const required of [
    "applyPipelineV2PlanningRunPlanHandoff",
    "resumePipelineV2Run",
    "requireResolvedPipelineV2Provenance",
    "PIPELINE_V2_FAILURE_REASONS",
  ]) {
    expect(internalSource.includes(required), `the internal core must import ${required}`).toBe(true);
  }
});
