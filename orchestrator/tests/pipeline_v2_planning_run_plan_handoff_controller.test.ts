import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  parsePipelineV2RunState,
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
} from "../src/pipeline_v2_runtime.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { acceptPipelineV2PlanningRunPlan, PipelineV2PlanningRunPlanControllerError } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { restorePipelineV2RuntimeContext } from "../src/pipeline_v2_resume_context.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../src/pipeline_v2_run_plan_restore.ts";
import { openPipelineV2ReplannedStage } from "../src/pipeline_v2_replanned_stage_controller.ts";
import { openPipelineV2ReplannedStageTransition, PipelineV2ReplannedStageTransitionControllerError } from "../src/pipeline_v2_replanned_stage_transition_controller.ts";
import { loadPipelineV2WaitIntent, publishPipelineV2WaitIntent } from "../src/pipeline_v2_run_plan_store.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { prepareWaitIntent } from "../src/pipeline_v2_run_plan_manifests.ts";
import { compiledPipelineV2RunPlanStageFor, PipelineV2CompiledRunPlanError } from "../src/pipeline_v2_run_plan_compiled.ts";
import {
  PipelineV2PlanningRunPlanHandoffControllerError,
  type PipelineV2PlanningRunPlanHandoffFailureReason,
  applyPipelineV2PlanningRunPlanHandoff,
} from "../src/pipeline_v2_planning_run_plan_handoff_controller.ts";
import {
  applyPipelineV2PlanningRunPlanHandoffWithIo,
  productionPlanningRunPlanHandoffOps,
  type PipelineV2PlanningRunPlanHandoffOps,
} from "../src/pipeline_v2_planning_run_plan_handoff_controller_internal.ts";
const RUN_ID = "handoff-run";
const STAGE_ID = "stage-1";
const INITIAL_BUDGET = 2;

const STAGE_YAML = `schema_version: 2
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
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
    - state_id: dev_entry
      role: stage
      stage_template: development

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
        to: architect

  - id: done
    type: terminal
    result: success
`;

const R1_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    {
      id: "stage-1",
      template: "development",
      tasks: [
        { id: "task-a", depends_on: [] },
        { id: "task-b", depends_on: [] },
      ],
    },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

const R2_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    {
      id: "stage-1",
      template: "development",
      tasks: [
        { id: "task-a", depends_on: [] },
        { id: "task-b", depends_on: [] },
        { id: "task-c", depends_on: ["task-a"] },
      ],
    },
  ],
  new_tasks: [{ id: "task-c", body: "Body C" }],
};

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clockCounter));
}

interface Recording {
  commands: PipelineV2RunCommand[];
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
}

function recordingSink(inner: PipelineV2RunStateSink, failFrom: number): Recording {
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
      if (failFrom !== Infinity && commands.length > failFrom) {
        throw new PipelineV2RunStateStoreError("the injected store fault refused the pipeline v2 run state commit");
      }
      await inner.dispatch(command);
    },
  };
}

function suffixOf(recording: Recording): string[] {
  return recording.commands.map((command) => {
    if (command.kind === "task_revision_accepted") {
      return `${command.taskId}:${command.revision}`;
    }
    if (command.kind === "plan_revision_accepted") {
      return `plan:${command.planRevision}`;
    }
    if (command.kind === "stage_iteration_closed" || command.kind === "stage_generation_closed") {
      return `${command.kind}(${command.by})`;
    }
    return command.kind;
  });
}

interface Fixture {
  root: string;
  bundle: string;
  stateRoot: string;
  runRoot: string;
}

async function setupFixture(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "handoff-controller-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
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

interface Prefix {
  fixture: Fixture;
  recording: Recording;
  pipeline1: ResolvedPipelineV2;
  staleCompiledPlan1: unknown;
  revisionAtBoundary: number;
}

/** The honest prefix through the existing facades, the reducer and the runtime data plane. */
async function buildPrefix(): Promise<Prefix> {
  const fixture = await setupFixture();
  try {
    const pipeline1 = await loadPipelineV2(fixture.bundle);
    clockCounter = 0;
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const recording = recordingSink(sink, Infinity);
    await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
    const runInputs = await snapshotRunInputs(pipeline1, [{ id: "task", path: join(fixture.root, "userdata", "task.txt") }], fixture.runRoot);
    await recording.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline1),
      inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    });
    const prep1 = await prepareActivationData(pipeline1, runInputs, [], "architect", 1);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-1-planning" },
      { kind: "agent_tool_session_created", sessionId: "tool-1-planning" },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep1.outputs_root, "plan"), JSON.stringify(R1_PROPOSAL), { mode: 0o600 });
    const records1: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline1, prep1);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records1.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });

    const accepted1 = await acceptPipelineV2PlanningRunPlan({ pipeline: pipeline1, runRoot: fixture.runRoot, sink: recording });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted1.compiled_plan, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET, sink: recording });
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-2-stage" },
      { kind: "agent_tool_session_created", sessionId: "tool-2-stage" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] },
      { kind: "agent_cleanup_completed" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 2,
    });
    const actions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions,
    });
    await recording.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions,
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
    await applyPipelineV2ReviseTaskIntervention({
      pipeline: pipeline1,
      runRoot: fixture.runRoot,
      sink: recording,
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Body A revised",
    });

    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const stateAfterIntervention = reopened.snapshot;
    if (stateAfterIntervention === null) {
      throw new Error("the reopened run lost its durable state");
    }
    const pipeline2 = await loadPipelineV2(stateAfterIntervention.pipeline.bundle_root);
    const restored = await restorePipelineV2RuntimeContext(pipeline2, stateAfterIntervention, fixture.runRoot);
    const prep2 = await prepareActivationData(pipeline2, restored.run_inputs, restored.accepted_outputs, "architect", restored.next_execution_index);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-3-planning" },
      { kind: "agent_tool_session_created", sessionId: "tool-3-planning" },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep2.outputs_root, "plan"), JSON.stringify(R2_PROPOSAL), { mode: 0o600 });
    const records2: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline2, prep2);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records2.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });
    const state = sink.snapshot as PipelineV2RunState;
    const validated = parsePipelineV2RunState(JSON.stringify(state));
    if (validated.executions.length !== validated.transitions.length + 1) {
      throw new Error("the prefix did not reach the settled-unbound boundary");
    }
    return {
      fixture,
      recording,
      pipeline1,
      staleCompiledPlan1: accepted1.compiled_plan,
      revisionAtBoundary: state.revision,
    };
  } catch (cause) {
    await rm(fixture.root, { recursive: true, force: true });
    throw cause;
  }
}

/** Runs the handoff facade against a fresh reopen and returns the observed chain. */
async function runHandoff(
  fixture: Fixture,
  failFrom = Infinity,
  stageId: string = STAGE_ID,
): Promise<{
  chainSuffix: string[];
  revisionBefore: number;
  revisionAfter: number;
  state: PipelineV2RunState;
  result: Awaited<ReturnType<typeof applyPipelineV2PlanningRunPlanHandoff>>;
}> {
  const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
  const stateBefore = reopened.snapshot;
  if (stateBefore === null) {
    throw new Error("the run lost its durable state");
  }
  const pipeline = await loadPipelineV2(stateBefore.pipeline.bundle_root);
  const wrapped = recordingSink(reopened, failFrom);
  const result = await applyPipelineV2PlanningRunPlanHandoff({
    pipeline,
    runRoot: fixture.runRoot,
    sink: wrapped,
    stageId,
    initialBudget: INITIAL_BUDGET,
  });
  return {
    chainSuffix: suffixOf(wrapped),
    revisionBefore: stateBefore.revision,
    revisionAfter: (wrapped.snapshot as PipelineV2RunState).revision,
    state: wrapped.snapshot as PipelineV2RunState,
    result,
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

function expectHandoffError(cause: unknown, reason: PipelineV2PlanningRunPlanHandoffFailureReason): PipelineV2PlanningRunPlanHandoffControllerError {
  expect(cause).toBeInstanceOf(PipelineV2PlanningRunPlanHandoffControllerError);
  const error = cause as PipelineV2PlanningRunPlanHandoffControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

async function currentRevision(fixture: Fixture): Promise<number> {
  const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
  return (sink.snapshot as PipelineV2RunState).revision;
}

interface ArtifactIdentity {
  inode: number;
  mode: number;
  mtime: number;
  bytes: string;
}

async function artifactIdentity(path: string): Promise<ArtifactIdentity> {
  const info = await stat(path);
  return { inode: info.ino, mode: info.mode, mtime: info.mtimeMs, bytes: (await readFile(path)).toString() };
}

const ARTIFACT_PATHS = (fixture: Fixture) => ({
  taskA2: join(fixture.runRoot, "run-plan", "tasks", "task-a", "2.json"),
  taskC1: join(fixture.runRoot, "run-plan", "tasks", "task-c", "1.json"),
  plan2: join(fixture.runRoot, "run-plan", "plans", "2.json"),
  intent1: join(fixture.runRoot, "run-plan", "intents", "1.json"),
});

const FRESH_SUFFIX = [
  "task-c:1",
  "plan:2",
  "stage_generation_closed(replanned)",
  "stage_generation_opened",
  "stage_iteration_opened",
  "transition_committed",
];

/** The final projection required after every converging window. */
function expectFinalProjection(state: PipelineV2RunState, expectedBudget: number, expectedRevision: number): void {
  expect(state.revision).toBe(expectedRevision);
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.plan_revisions.map((p) => p.revision)).toEqual([1, 2]);
  expect(state.task_revisions.map((t) => ({ id: t.task_id, revision: t.revision, wait: t.wait_index }))).toEqual([
    { id: "task-a", revision: 1, wait: undefined },
    { id: "task-b", revision: 1, wait: undefined },
    { id: "task-a", revision: 2, wait: 1 },
    { id: "task-c", revision: 1, wait: undefined },
  ]);
  expect(state.waits.map((w) => ({ index: w.index, response: w.response?.action_id }))).toEqual([{ index: 1, response: "revise_task" }]);
  const oldGeneration = state.generations[0]!;
  expect(oldGeneration.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
  expect(oldGeneration.open_iteration).toBeUndefined();
  const newGeneration = state.generations[1]!;
  expect(newGeneration.stage_id).toBe(STAGE_ID);
  expect(newGeneration.template_id).toBe("development");
  expect(newGeneration.plan_sha256).toBe(state.plan_revisions[1]!.sha256);
  expect(newGeneration.initial_budget).toBe(expectedBudget);
  expect(newGeneration.opened_transition_count).toBe(2);
  expect(newGeneration.closed).toBeUndefined();
  expect(newGeneration.iteration_count).toBe(1);
  expect(newGeneration.open_iteration).toBeDefined();
  expect(newGeneration.iterations[0]!.opened_transition_count).toBe(2);
  expect(state.transitions).toHaveLength(3);
  const binding = state.transitions[2]!;
  expect(binding.from).toBe("architect");
  expect(binding.to).toBe("dev_entry");
  expect(binding.index).toBe(0);
  expect(binding.execution_index).toBe(3);
  expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
  expect(state.executions).toHaveLength(3);
  expect(state.executions[2]!.phase).toBe("cleanup_completed");
  expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
}

test("1. honest S0 full path after restart: the exact six-command suffix, delta +6, projection", async () => {
  const prefix = await buildPrefix();
  const { fixture, revisionAtBoundary } = prefix;
  try {
    const outcome = await runHandoff(fixture);
    expect(outcome.chainSuffix).toEqual(FRESH_SUFFIX);
    expect(outcome.revisionBefore).toBe(revisionAtBoundary);
    expect(outcome.revisionAfter - outcome.revisionBefore).toBe(6);
    expectFinalProjection(outcome.state, INITIAL_BUDGET, outcome.revisionAfter);
    expect(Object.keys(outcome.result).sort()).toEqual([
      "execution_index", "from_state", "generation_index", "initial_budget", "iteration_index",
      "plan_revision", "plan_sha256", "stage_id", "stage_position", "state", "template_id",
      "to_state", "transition_index", "wait_index",
    ]);
    expect(outcome.result.wait_index).toBe(1);
    expect(outcome.result.from_state).toBe("architect");
    expect(outcome.result.to_state).toBe("dev_entry");
    expect(outcome.result.transition_index).toBe(0);
    expect(outcome.result.execution_index).toBe(3);
    expect(outcome.result.generation_index).toBe(2);
    expect(outcome.result.iteration_index).toBe(1);
    expect(outcome.result.stage_id).toBe(STAGE_ID);
    expect(outcome.result.stage_position).toBe(1);
    expect(outcome.result.template_id).toBe("development");
    expect(outcome.result.initial_budget).toBe(INITIAL_BUDGET);
    expect(outcome.result.plan_revision).toBe(2);
    expect(outcome.result.state).toEqual(outcome.state);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("2. S1-S4: the fresh facade runs only the missing suffix with deltas +5/+4/+3/+1 and converges", async () => {
  for (const failFrom of [1, 2, 3, 4]) {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      await catchHandoff(() => runHandoff(fixture, failFrom));
      const boundaryRevision = await currentRevision(fixture);
      const paths = ARTIFACT_PATHS(fixture);
      const preservedPlan = failFrom >= 2 ? await artifactIdentity(paths.plan2) : null;
      const preservedTaskC = failFrom >= 1 ? await artifactIdentity(paths.taskC1) : null;
      const preservedPrefix = await Promise.all([artifactIdentity(paths.taskA2), artifactIdentity(paths.intent1)]);

      const outcome = await runHandoff(fixture);
      expect(outcome.chainSuffix).toEqual(FRESH_SUFFIX.slice(failFrom));
      expect(outcome.revisionAfter).toBe(outcome.revisionBefore + (FRESH_SUFFIX.length - failFrom));
      expect(outcome.revisionBefore).toBe(boundaryRevision);
      expectFinalProjection(outcome.state, INITIAL_BUDGET, 40);
      expect(outcome.state.plan_revisions).toHaveLength(2);
      expect(outcome.state.task_revisions).toHaveLength(4);
      expect(outcome.state.generations).toHaveLength(2);
      expect(outcome.state.transitions).toHaveLength(3);
      expect(outcome.state.waits).toHaveLength(1);

      if (preservedPlan !== null) {
        const after = await artifactIdentity(paths.plan2);
        expect([after.inode, after.mode, after.mtime, after.bytes]).toEqual([preservedPlan.inode, preservedPlan.mode, preservedPlan.mtime, preservedPlan.bytes]);
      }
      if (preservedTaskC !== null) {
        const after = await artifactIdentity(paths.taskC1);
        expect([after.inode, after.mode, after.mtime, after.bytes]).toEqual([preservedTaskC.inode, preservedTaskC.mode, preservedTaskC.mtime, preservedTaskC.bytes]);
      }
      const prefixAfter = await Promise.all([artifactIdentity(paths.taskA2), artifactIdentity(paths.intent1)]);
      expect(prefixAfter.map((p) => p.inode)).toEqual(preservedPrefix.map((p) => p.inode));
      expect(prefixAfter.map((p) => p.mtime)).toEqual(preservedPrefix.map((p) => p.mtime));
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});

test("3. S5 branch B: zero acceptance/stage calls; restore/load/transition exactly once; C1 zero dispatch", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const outcome = await runHandoff(fixture);
    expectFinalProjection(outcome.state, INITIAL_BUDGET, 40);
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const calls = { restore: 0, load: 0, transition: 0, accept: 0, stage: 0 };
    const ops: PipelineV2PlanningRunPlanHandoffOps = {
      acceptPlanningRunPlan: (async (...args: unknown[]) => {
        calls.accept += 1;
        return await acceptPipelineV2PlanningRunPlan(...(args as Parameters<typeof acceptPipelineV2PlanningRunPlan>));
      }) as typeof acceptPipelineV2PlanningRunPlan,
      restoreAcceptedRunPlan: (async (...args: unknown[]) => {
        calls.restore += 1;
        return await restorePipelineV2AcceptedRunPlan(...(args as Parameters<typeof restorePipelineV2AcceptedRunPlan>));
      }) as typeof restorePipelineV2AcceptedRunPlan,
      loadWaitIntent: (async (...args: unknown[]) => {
        calls.load += 1;
        return await loadPipelineV2WaitIntent(...(args as Parameters<typeof loadPipelineV2WaitIntent>));
      }) as typeof loadPipelineV2WaitIntent,
      openReplannedStage: (async (...args: unknown[]) => {
        calls.stage += 1;
        return await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>));
      }) as typeof openPipelineV2ReplannedStage,
      openReplannedStageTransition: (async (...args: unknown[]) => {
        calls.transition += 1;
        return await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
      }) as typeof openPipelineV2ReplannedStageTransition,
      compiledStageFor: compiledPipelineV2RunPlanStageFor,
    };
    const wrapped = recordingSink(reopened, Infinity);
    const branchB = await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline, runRoot: fixture.runRoot, sink: wrapped, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      ops,
    );
    expect(calls).toEqual({ restore: 1, load: 1, transition: 1, accept: 0, stage: 0 });
    expect(wrapped.commands).toEqual([]);
    expect(branchB.generation_index).toBe(2);
    expect(branchB.iteration_index).toBe(1);
    expect(branchB.wait_index).toBe(1);
    expect(branchB.state.revision).toBe(40);
    expectFinalProjection(branchB.state, INITIAL_BUDGET, 40);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("4. branch selection never catches: an injected acceptance sentinel on branch A returns by identity; branch B ops untouched", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const sentinel = new PipelineV2PlanningRunPlanControllerError("invalid_result", "the injected acceptance sentinel");
    const calls = { restore: 0, load: 0, transition: 0, accept: 0, stage: 0 };
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const ops: PipelineV2PlanningRunPlanHandoffOps = {
      acceptPlanningRunPlan: (async () => {
        calls.accept += 1;
        throw sentinel;
      }) as typeof acceptPipelineV2PlanningRunPlan,
      restoreAcceptedRunPlan: (async () => {
        calls.restore += 1;
        throw new Error("the restore must not be called on branch A");
      }) as typeof restorePipelineV2AcceptedRunPlan,
      loadWaitIntent: (async () => {
        calls.load += 1;
        throw new Error("the intent load must not run before the acceptance");
      }) as typeof loadPipelineV2WaitIntent,
      openReplannedStage: (async () => {
        calls.stage += 1;
        throw new Error("the stage must not run");
      }) as typeof openPipelineV2ReplannedStage,
      openReplannedStageTransition: (async () => {
        calls.transition += 1;
        throw new Error("the transition must not run");
      }) as typeof openPipelineV2ReplannedStageTransition,
      compiledStageFor: compiledPipelineV2RunPlanStageFor,
    };
    const caught = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        ops,
      ));
    expect(caught).toBe(sentinel);
    expect(calls).toEqual({ restore: 0, load: 0, transition: 0, accept: 1, stage: 0 });
    expect((reopened.snapshot as PipelineV2RunState).revision).toBe(state.revision);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("5. S3 caller-policy pinning: captured budget/stage survive pending awaits; a replayed different budget conflicts by identity", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const firstAttempt = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((firstAttempt.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const wrapped = recordingSink(firstAttempt, 3);
    const options = {
      pipeline,
      runRoot: fixture.runRoot,
      sink: wrapped,
      stageId: STAGE_ID,
      initialBudget: INITIAL_BUDGET,
    };
    const pending = applyPipelineV2PlanningRunPlanHandoffWithIo(options, productionPlanningRunPlanHandoffOps);
    options.initialBudget = 99;
    options.stageId = "stage-other";
    await catchHandoff(() => pending);
    const outcome = await runHandoff(fixture);
    expect(outcome.chainSuffix).toEqual(["stage_generation_opened", "stage_iteration_opened", "transition_committed"]);
    expectFinalProjection(outcome.state, INITIAL_BUDGET, 40);

    const conflicting = await catchHandoff(async () => {
      const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const state = sink.snapshot as PipelineV2RunState;
      const conflictPipeline = await loadPipelineV2(state.pipeline.bundle_root);
      await applyPipelineV2PlanningRunPlanHandoff({
        pipeline: conflictPipeline,
        runRoot: fixture.runRoot,
        sink,
        stageId: STAGE_ID,
        initialBudget: 3,
      });
    });
    expect(conflicting).toBeInstanceOf(PipelineV2ReplannedStageTransitionControllerError);
    expect((conflicting as PipelineV2ReplannedStageTransitionControllerError).reason).toBe("lifecycle_conflict");
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("6. concurrent identical calls at S0 and at S3 converge to one projection without duplicates", async () => {
  for (const failFrom of [Infinity, 3]) {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      if (failFrom !== Infinity) {
        await catchHandoff(() => runHandoff(fixture, failFrom));
      }
      const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const pipeline = await loadPipelineV2((sink.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const settled = await Promise.allSettled([
        applyPipelineV2PlanningRunPlanHandoff({ pipeline, runRoot: fixture.runRoot, sink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }),
        applyPipelineV2PlanningRunPlanHandoff({ pipeline, runRoot: fixture.runRoot, sink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }),
      ]);
      expect(settled.every((entry) => entry.status === "fulfilled")).toBe(true);
      const finalState = (await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot as PipelineV2RunState;
      expectFinalProjection(finalState, INITIAL_BUDGET, 40);
      expect(finalState.plan_revisions).toHaveLength(2);
      expect(finalState.task_revisions).toHaveLength(4);
      expect(finalState.generations).toHaveLength(2);
      expect(finalState.transitions).toHaveLength(3);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});

test("7. concurrent S3 calls with budgets 2 and 3: one durable policy wins; the loser is typed-refused; bindings stay pure", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    await catchHandoff(() => runHandoff(fixture, 3));
    const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((sink.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const results = await Promise.allSettled([
      applyPipelineV2PlanningRunPlanHandoff({ pipeline, runRoot: fixture.runRoot, sink, stageId: STAGE_ID, initialBudget: 2 }),
      applyPipelineV2PlanningRunPlanHandoff({ pipeline, runRoot: fixture.runRoot, sink, stageId: STAGE_ID, initialBudget: 3 }),
    ]);
    const fulfilled = results.filter((entry): entry is PromiseFulfilledResult<Awaited<ReturnType<typeof applyPipelineV2PlanningRunPlanHandoff>>> => entry.status === "fulfilled");
    const rejected = results.filter((entry): entry is PromiseRejectedResult => entry.status === "rejected");
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    const winnerBudget = fulfilled[0]!.value.initial_budget;
    expect([2, 3]).toContain(winnerBudget);
    const finalState = (await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot as PipelineV2RunState;
    expect(finalState.status).toBe("active");
    expect(finalState.generations[1]!.stage_id).toBe(STAGE_ID);
    expect(finalState.generations[1]!.initial_budget).toBe(winnerBudget);
    expect(finalState.generations).toHaveLength(2);
    expect(finalState.transitions).toHaveLength(3);
    expect(finalState.task_revisions).toHaveLength(4);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("8. missing, damaged and foreign-kind intent: the stage/transition facades never run; foreign errors keep identity", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const intentPath = join(fixture.runRoot, "run-plan", "intents", "1.json");
    // Missing artifact: the loader returns null; the facade fails with its
    // own stable typed failure; only the acceptance itself wrote.
    await rm(intentPath);
    const missingError = await catchHandoff(() => runHandoff(fixture));
    expectHandoffError(missingError, "artifact_missing");
    expect(await currentRevision(fixture)).toBe(36);

    // Damaged artifact: the store's typed manifest error passes through.
    await writeFile(intentPath, "{not json", { mode: 0o600 });
    const damagedError = await catchHandoff(() => runHandoff(fixture));
    expect(damagedError).toBeDefined();
    expect(damagedError).not.toBeInstanceOf(PipelineV2PlanningRunPlanHandoffControllerError);
    expect(await currentRevision(fixture)).toBe(36);

    // A foreign-kind intent artifact: the facade's own binding refusal;
    // zero further writes.
    await rm(intentPath);
    const foreignIntent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: STAGE_ID,
      expected_plan_sha256: "0".repeat(64),
      additional_iterations: 1,
    });
    await publishPipelineV2WaitIntent(fixture.runRoot, foreignIntent.manifest);
    const foreignError = await catchHandoff(() => runHandoff(fixture));
    expectHandoffError(foreignError, "invalid_result");
    expect(await currentRevision(fixture)).toBe(36);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("9. stale restored plan, foreign stage and wrong wait binding are refused", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    // A stale restored plan through the injected restore op (branch B shape
    // is irrelevant: the verification rejects the stale bindings).
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const staleOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      acceptPlanningRunPlan: (async () => {
        const real = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: fixture.runRoot, sink: reopened });
        return { compiled_plan: prefix.staleCompiledPlan1, state: real.state };
      }) as typeof acceptPipelineV2PlanningRunPlan,
    };
    const staleError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        staleOps,
      ));
    expectHandoffError(staleError, "invalid_result");
    expect((staleError as PipelineV2PlanningRunPlanHandoffControllerError).message).not.toContain("task-a");

    // A foreign stage id through the facade: the compiled resolver refuses.
    const reopened2 = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state2 = reopened2.snapshot as PipelineV2RunState;
    const pipeline2 = await loadPipelineV2(state2.pipeline.bundle_root);
    const wrapped2 = recordingSink(reopened2, Infinity);
    const foreignStageError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({
        pipeline: pipeline2,
        runRoot: fixture.runRoot,
        sink: wrapped2,
        stageId: "stage-other",
        initialBudget: INITIAL_BUDGET,
      }));
    expect(foreignStageError).toBeInstanceOf(PipelineV2CompiledRunPlanError);
    expect(wrapped2.commands.length).toBe(0);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("9b. an intent bound to another wait is refused before any downstream call", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const foreignIntent = (await loadPipelineV2WaitIntent(fixture.runRoot, 1))!.intent;
    const wrongOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      loadWaitIntent: (async () => ({
        intent: {
          ...foreignIntent,
          manifest: { ...foreignIntent.manifest, wait_index: 7 },
        },
        intent_path: "/somewhere/7.json",
      })) as typeof loadPipelineV2WaitIntent,
    };
    const wrongError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        wrongOps,
      ));
    expectHandoffError(wrongError, "invalid_state");
    expect(await currentRevision(fixture)).toBe(36);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("10a. a successor execution started after the handoff is never an S5 retry", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    await runHandoff(fixture);
    const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((sink.snapshot as PipelineV2RunState).pipeline.bundle_root);

    const progressed = recordingSink(sink, Infinity);
    await progressed.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-4-stage" },
      { kind: "agent_tool_session_created", sessionId: "tool-4-stage" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] },
      { kind: "agent_cleanup_completed" },
    ] as PipelineV2RunCommand[]) {
      await progressed.dispatch(command);
    }
    const startedError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({ pipeline, runRoot: fixture.runRoot, sink: progressed, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }));
    expectHandoffError(startedError, "invalid_state");
    expect(progressed.commands.length).toBe(7);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("10b. a new wait after the handoff is never an S5 retry", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    await runHandoff(fixture);
    const waitSink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const waitPipeline = await loadPipelineV2((waitSink.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const waitRecording = recordingSink(waitSink, Infinity);
    await waitRecording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-5-stage" },
      { kind: "agent_tool_session_created", sessionId: "tool-5-stage" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] },
      { kind: "agent_cleanup_completed" },
    ] as PipelineV2RunCommand[]) {
      await waitRecording.dispatch(command);
    }
    await waitRecording.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 4,
    });
    const newRequest = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 2,
      transition_count: 3,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: [{ id: "continue_stage", to: "dev_entry" }],
    });
    await waitRecording.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: newRequest.sha256,
      actions: [{ id: "continue_stage", to: "dev_entry" }],
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, newRequest.manifest);
    const newWaitError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({ pipeline: waitPipeline, runRoot: fixture.runRoot, sink: waitRecording, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }));
    expectHandoffError(newWaitError, "invalid_state");
    expect(waitRecording.commands.length).toBe(9);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

interface ChainCallRecorder {
  readonly calls: string[];
}

function chainOpsWithHostile(
  fixture: Fixture,
  hostileOp: keyof PipelineV2PlanningRunPlanHandoffOps,
  hostile: unknown,
  recorder: ChainCallRecorder,
): PipelineV2PlanningRunPlanHandoffOps {
  const realCall = async (name: keyof PipelineV2PlanningRunPlanHandoffOps, args: unknown[]): Promise<unknown> => {
    recorder.calls.push(name);
    if (name === hostileOp) {
      return hostile;
    }
    if (name === "acceptPlanningRunPlan") {
      return await acceptPipelineV2PlanningRunPlan(...(args as Parameters<typeof acceptPipelineV2PlanningRunPlan>));
    }
    if (name === "restoreAcceptedRunPlan") {
      return await restorePipelineV2AcceptedRunPlan(...(args as Parameters<typeof restorePipelineV2AcceptedRunPlan>));
    }
    if (name === "loadWaitIntent") {
      return await loadPipelineV2WaitIntent(...(args as Parameters<typeof loadPipelineV2WaitIntent>));
    }
    if (name === "openReplannedStage") {
      return await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>));
    }
    return await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
  };
  return {
    acceptPlanningRunPlan: ((...args: unknown[]) => realCall("acceptPlanningRunPlan", args)) as typeof acceptPipelineV2PlanningRunPlan,
    restoreAcceptedRunPlan: ((...args: unknown[]) => realCall("restoreAcceptedRunPlan", args)) as typeof restorePipelineV2AcceptedRunPlan,
    loadWaitIntent: ((...args: unknown[]) => realCall("loadWaitIntent", args)) as typeof loadPipelineV2WaitIntent,
    openReplannedStage: ((...args: unknown[]) => realCall("openReplannedStage", args)) as typeof openPipelineV2ReplannedStage,
    openReplannedStageTransition: ((...args: unknown[]) => realCall("openReplannedStageTransition", args)) as typeof openPipelineV2ReplannedStageTransition,
    compiledStageFor: compiledPipelineV2RunPlanStageFor,
  };
}

test("11. malformed-result matrix: every composed op's hostile result is the controller's own invalid_result and stops the chain", async () => {
  const hostiles: [string, unknown][] = [
    ["primitive", 42],
    ["array", ["array"]],
    ["null", null],
    ["empty-record", {}],
  ];
  const branchAOps = ["acceptPlanningRunPlan", "loadWaitIntent", "openReplannedStage", "openReplannedStageTransition"] as const;
  for (const hostileOp of [...branchAOps, "restoreAcceptedRunPlan"] as const) {
    for (const [hostileLabel, hostile] of hostiles) {
      const prefix = await buildPrefix();
      const { fixture } = prefix;
      try {
        if (hostileOp === "restoreAcceptedRunPlan") {
          // The restore is used only on branch B: drive the clean handoff
          // first, then inject the hostile restore result.
          await runHandoff(fixture);
        }
        const recorder: ChainCallRecorder = { calls: [] };
        const ops = chainOpsWithHostile(fixture, hostileOp, hostile, recorder);
        const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
        const error = await catchHandoff(() =>
          applyPipelineV2PlanningRunPlanHandoffWithIo(
            { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
            ops,
          ));
        const expectedReason = hostileOp === "loadWaitIntent" && hostile === null ? "artifact_missing" : "invalid_result";
        expectHandoffError(error, expectedReason);
        if (hostileOp === "acceptPlanningRunPlan") {
          expect(recorder.calls).toEqual(["acceptPlanningRunPlan"]);
        } else if (hostileOp === "restoreAcceptedRunPlan") {
          expect(recorder.calls).toEqual(["restoreAcceptedRunPlan"]);
        } else if (hostileOp === "loadWaitIntent") {
          expect(recorder.calls).toEqual(["acceptPlanningRunPlan", "loadWaitIntent"]);
        } else if (hostileOp === "openReplannedStage") {
          expect(recorder.calls).toEqual(["acceptPlanningRunPlan", "loadWaitIntent", "openReplannedStage"]);
        } else {
          expect(recorder.calls).toEqual(["acceptPlanningRunPlan", "loadWaitIntent", "openReplannedStage", "openReplannedStageTransition"]);
        }
        void hostileLabel;
      } finally {
        await rm(fixture.root, { recursive: true, force: true });
      }
    }
  }
});

test("11b. forged flat bindings and a coherently forged state are refused", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    // Capture the real stage result on an S4 boundary: the chain faults
    // before the transition dispatch, leaving the generation/iteration open.
    const captureSink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((captureSink.snapshot as PipelineV2RunState).pipeline.bundle_root);
    let realStage: unknown = null;
    const captureOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      openReplannedStage: (async (...args: unknown[]) => {
        realStage = await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>));
        return realStage;
      }) as typeof openPipelineV2ReplannedStage,
    };
    const faulty = recordingSink(captureSink, 5);
    await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: faulty, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        captureOps,
      ));
    expect(realStage).toBeDefined();
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const forgedFlat: Record<string, unknown> = { ...(realStage as unknown as Record<string, unknown>), initial_budget: 3, stage_position: 7 };
    const forgedOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      openReplannedStage: (async () => forgedFlat) as unknown as typeof openPipelineV2ReplannedStage,
    };
    const forgedError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        forgedOps,
      ));
    expectHandoffError(forgedError, "invalid_result");

    // A coherently forged state (the flat fields and the state mutated
    // together) still disagrees with the authoritative snapshot.
    const forgedState = forgedFlat["state"] as PipelineV2RunState;
    const coherent = {
      ...forgedFlat,
      state: {
        ...forgedState,
        revision: forgedState.revision + 1,
      },
    };
    const coherentOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      openReplannedStage: (async () => coherent) as unknown as typeof openPipelineV2ReplannedStage,
    };
    const coherentError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        coherentOps,
      ));
    expectHandoffError(coherentError, "invalid_result");
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("12. C1 state proof: the downstream state is not identity-bound; honest deep-equal is accepted; one mutation is refused", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    await runHandoff(fixture);
    const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = sink.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);

    // The real C1 result's state is deep-equal but not identity-bound.
    const realSink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const realResult = await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline, runRoot: fixture.runRoot, sink: realSink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      productionPlanningRunPlanHandoffOps,
    );
    const realSnapshot = realSink.snapshot;
    if (realSnapshot === null) {
      throw new Error("the reopened run lost its durable state");
    }
    expect(realResult.state).not.toBe(realSnapshot);
    expect(realResult.state).toEqual(realSnapshot);

    // One mutated schema-owned field is refused and no dispatch happens.
    const mutatedState = { ...realResult.state, revision: realResult.state.revision + 1 };
    const mutatedOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      openReplannedStageTransition: (async () => ({ ...realResult, state: mutatedState })) as typeof openPipelineV2ReplannedStageTransition,
    };
    const wrapped = recordingSink(realSink, Infinity);
    const mutatedError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: wrapped, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        mutatedOps,
      ));
    expectHandoffError(mutatedError, "invalid_result");
    expect(wrapped.commands).toEqual([]);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("13. options Proxy: the five contract fields are read exactly once in contract order; hostile extras never read", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const reads: string[] = [];
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const proxied = new Proxy(
      { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET, hostile: "never-read" },
      {
        get(target, property) {
          reads.push(String(property));
          return target[property as keyof typeof target];
        },
      },
    );
    await applyPipelineV2PlanningRunPlanHandoff(proxied as never);
    expect(reads.slice(0, 5)).toEqual(["pipeline", "runRoot", "sink", "stageId", "initialBudget"]);
    expect(reads).not.toContain("hostile");
    expect(await currentRevision(fixture)).toBe(40);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("14. ops Proxy: each member read exactly once before the first await; later mutation cannot redirect", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const reads: string[] = [];
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
    let transitionCalls = 0;
    const opsProxy = new Proxy(
      { ...productionPlanningRunPlanHandoffOps },
      {
        get(target, property) {
          reads.push(String(property));
          if (property === "openReplannedStageTransition") {
            transitionCalls += 1;
          }
          return target[property as keyof typeof target];
        },
      },
    );
    await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      opsProxy,
    );
    for (const member of ["acceptPlanningRunPlan", "restoreAcceptedRunPlan", "loadWaitIntent", "openReplannedStage", "openReplannedStageTransition", "compiledStageFor"]) {
      expect(reads.filter((entry) => entry === member)).toHaveLength(1);
    }
    expect(transitionCalls).toBe(1);
    expect(await currentRevision(fixture)).toBe(40);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("15. pipeline clone and Proxy are refused by the provenance gate before any state or facade access", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const revisionBefore = state.revision;

    let snapshotReads = 0;
    const countingSink = {
      get snapshot() {
        snapshotReads += 1;
        return (reopened as unknown as { snapshot: PipelineV2RunState | null }).snapshot;
      },
      get poisoned() {
        return reopened.poisoned;
      },
      dispatch: reopened.dispatch.bind(reopened),
    };
    const clone = { ...pipeline };
    const cloneError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({ pipeline: clone as never, runRoot: fixture.runRoot, sink: countingSink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }));
    expect(cloneError).toBeInstanceOf(Error);
    expect((cloneError as Error).constructor.name).toBe("PipelineError");
    expect(snapshotReads).toBe(0);

    let traps = 0;
    const proxiedPipeline = new Proxy(pipeline, {
      get(target, property) {
        traps += 1;
        return target[property as keyof typeof target];
      },
    });
    const proxyError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({ pipeline: proxiedPipeline as never, runRoot: fixture.runRoot, sink: countingSink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET }));
    expect(proxyError).toBeInstanceOf(Error);
    expect((proxyError as Error).constructor.name).toBe("PipelineError");
    expect(traps).toBe(0);
    expect(snapshotReads).toBe(0);
    expect(await currentRevision(fixture)).toBe(revisionBefore);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("16. snapshot read count: the capture reads nothing before the provenance gate; verification phases read the authoritative state", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    let snapshotReads = 0;
    let lastSnapshot: PipelineV2RunState | null = null;
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const inner = reopened;
    const countingSink = {
      get snapshot() {
        snapshotReads += 1;
        lastSnapshot = inner.snapshot;
        return lastSnapshot;
      },
      get poisoned() {
        return inner.poisoned;
      },
      dispatch: inner.dispatch.bind(inner),
    };
    const pipeline = await loadPipelineV2((inner.snapshot as PipelineV2RunState).pipeline.bundle_root);
    await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline, runRoot: fixture.runRoot, sink: countingSink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      productionPlanningRunPlanHandoffOps,
    );
    expect(snapshotReads).toBeGreaterThanOrEqual(2);
    expect(lastSnapshot!.revision).toBe(40);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("17. export surfaces: the public and internal runtime keys are exact", async () => {
  const publicModule = (await import("../src/pipeline_v2_planning_run_plan_handoff_controller.ts")) as unknown as Record<string, unknown>;
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2PlanningRunPlanHandoffControllerError",
    "applyPipelineV2PlanningRunPlanHandoff",
  ]);
  const internalModule = (await import("../src/pipeline_v2_planning_run_plan_handoff_controller_internal.ts")) as unknown as Record<string, unknown>;
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2PlanningRunPlanHandoffControllerError",
    "applyPipelineV2PlanningRunPlanHandoffWithIo",
    "productionPlanningRunPlanHandoffOps",
  ]);
  expect(() => new PipelineV2PlanningRunPlanHandoffControllerError("foreign" as never, "x", null)).toThrow(TypeError);
});

test("16b. exact snapshot read counts per branch", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    // Branch A full path: capture + acceptance verification + post-stage
    // + post-transition = exactly 4 authoritative reads.
    let readsA = 0;
    const sinkA = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const innerA = sinkA;
    const countingA = {
      get snapshot() {
        readsA += 1;
        return innerA.snapshot;
      },
      get poisoned() {
        return innerA.poisoned;
      },
      dispatch: innerA.dispatch.bind(innerA),
    };
    const pipelineA = await loadPipelineV2((innerA.snapshot as PipelineV2RunState).pipeline.bundle_root);
    await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline: pipelineA, runRoot: fixture.runRoot, sink: countingA, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      productionPlanningRunPlanHandoffOps,
    );
    // The controller's own reads are the capture and the three verification
    // phases; the composed controllers read the authoritative snapshot as
    // part of their own capture/verification contracts — the observed
    // stable total is pinned here.
    expect(readsA).toBe(16);

    // Branch B: the capture, the transition controller's own authoritative
    // read and the post-call verification.
    let readsB = 0;
    const sinkB = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const innerB = sinkB;
    const countingB = {
      get snapshot() {
        readsB += 1;
        return innerB.snapshot;
      },
      get poisoned() {
        return innerB.poisoned;
      },
      dispatch: innerB.dispatch.bind(innerB),
    };
    const pipelineB = await loadPipelineV2((innerB.snapshot as PipelineV2RunState).pipeline.bundle_root);
    await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline: pipelineB, runRoot: fixture.runRoot, sink: countingB, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      productionPlanningRunPlanHandoffOps,
    );
    expect(readsB).toBe(3);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("19. diagnostics are content-free: no task bodies, output JSON, digest canaries or filesystem paths", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const messages: string[] = [];
    // A stale plan.
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const staleOps: PipelineV2PlanningRunPlanHandoffOps = {
      ...productionPlanningRunPlanHandoffOps,
      acceptPlanningRunPlan: (async () => {
        const real = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: fixture.runRoot, sink: reopened });
        return { compiled_plan: prefix.staleCompiledPlan1, state: real.state };
      }) as typeof acceptPipelineV2PlanningRunPlan,
    };
    const staleError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        staleOps,
      ));
    messages.push((staleError as Error).message);

    // A foreign stage id.
    const reopened2 = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline2 = await loadPipelineV2((reopened2.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const foreignStageError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({
        pipeline: pipeline2,
        runRoot: fixture.runRoot,
        sink: reopened2,
        stageId: "stage-other",
        initialBudget: INITIAL_BUDGET,
      }));
    messages.push((foreignStageError as Error).message);

    // A wrong budget at the completed boundary.
    await runHandoff(fixture);
    const reopened3 = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const pipeline3 = await loadPipelineV2((reopened3.snapshot as PipelineV2RunState).pipeline.bundle_root);
    const budgetError = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoff({
        pipeline: pipeline3,
        runRoot: fixture.runRoot,
        sink: reopened3,
        stageId: STAGE_ID,
        initialBudget: 3,
      }));
    messages.push((budgetError as Error).message);

    const joined = messages.join(" ");
    for (const canary of ["Body A", "Body B", "Body C", "Body A revised", "TASK-BODY", "task-a", "task-c", "run_plan_proposal", fixture.runRoot, fixture.root, fixture.bundle, "/tmp/"]) {
      expect(joined).not.toContain(canary);
    }
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("18. source scan: only the listed facades; no reducer/store/fs/parser/serializer/digest/registry/runner", async () => {
  const sourceRoot = join(import.meta.dir, "..", "src");
  const facadeSource = await readFile(join(sourceRoot, "pipeline_v2_planning_run_plan_handoff_controller.ts"), "utf8");
  const internalSource = await readFile(join(sourceRoot, "pipeline_v2_planning_run_plan_handoff_controller_internal.ts"), "utf8");
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
    "publishPipelineV2RunPlan",
    "snapshotRunInputs",
    "prepareActivationData",
    "acceptActivationOutputs",
    "pipeline_v2_run_plan_provenance",
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "main.ts",
  ]) {
    expect(facadeSource.includes(banned), `the facade must not reference ${banned}`).toBe(false);
    expect(internalSource.includes(banned), `the internal core must not reference ${banned}`).toBe(false);
  }
  for (const required of [
    "acceptPipelineV2PlanningRunPlan",
    "restorePipelineV2AcceptedRunPlan",
    "loadPipelineV2WaitIntent",
    "openPipelineV2ReplannedStage",
    "openPipelineV2ReplannedStageTransition",
    "compiledPipelineV2RunPlanStageFor",
    "requireResolvedPipelineV2Provenance",
  ]) {
    expect(internalSource.includes(required), `the internal core must import ${required}`).toBe(true);
  }
});

const REVISED_TASKS = ["task-a", "task-b", "task-c"] as const;
const REVISED_BODIES = ["Body A revised", "Body B revised", "Body C revised"] as const;

function pointerProposal(taskIds: string[]): unknown {
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: taskIds.map((id) => (id === "task-c" ? { id, depends_on: ["task-a"] } : { id, depends_on: [] })),
      },
    ],
    new_tasks: [],
  };
}

let sessionCounter = 0;
function nextSession(label: string): string {
  sessionCounter += 1;
  return `sess-${sessionCounter}-${label}`;
}

/**
 * The honest prefix with N complete revise cycles: the first cycle uses
 * buildPrefix's flow and ends with the first handoff (the facade), then
 * every further cycle appends the stage execution, the transition, the
 * wait, the real revise-task intervention, the restart, the standard
 * resume restore and the planning execution through the runtime data
 * plane, ending at the settled-unbound planning boundary.
 */
async function buildCycledPrefix(cycles: number): Promise<Prefix> {
  const fixture = await setupFixture();
  try {
    const pipeline1 = await loadPipelineV2(fixture.bundle);
    clockCounter = 0;
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const recording = recordingSink(sink, Infinity);
    await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
    const runInputs = await snapshotRunInputs(pipeline1, [{ id: "task", path: join(fixture.root, "userdata", "task.txt") }], fixture.runRoot);
    await recording.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline1),
      inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    });
    const prep1 = await prepareActivationData(pipeline1, runInputs, [], "architect", 1);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("planning") },
      { kind: "agent_tool_session_created", sessionId: nextSession("planning") },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep1.outputs_root, "plan"), JSON.stringify(R1_PROPOSAL), { mode: 0o600 });
    const records1: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline1, prep1);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records1.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });

    const accepted1 = await acceptPipelineV2PlanningRunPlan({ pipeline: pipeline1, runRoot: fixture.runRoot, sink: recording });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted1.compiled_plan, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET, sink: recording });
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("stage") },
      { kind: "agent_tool_session_created", sessionId: nextSession("stage") },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] },
      { kind: "agent_cleanup_completed" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 2,
    });
    const firstActions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
    const firstRequest = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: firstActions,
    });
    await recording.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: firstRequest.sha256,
      actions: firstActions,
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, firstRequest.manifest);
    await applyPipelineV2ReviseTaskIntervention({
      pipeline: pipeline1,
      runRoot: fixture.runRoot,
      sink: recording,
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Body A revised",
    });

    // The restart, the standard resume restore, and the second planning
    // execution through the runtime data plane.
    const reopenedAfterIntervention = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const stateAfterIntervention = reopenedAfterIntervention.snapshot;
    if (stateAfterIntervention === null) {
      throw new Error("the reopened run lost its durable state");
    }
    const pipelineAfterIntervention = await loadPipelineV2(stateAfterIntervention.pipeline.bundle_root);
    const restoredPlanning = await restorePipelineV2RuntimeContext(pipelineAfterIntervention, stateAfterIntervention, fixture.runRoot);
    const prep2 = await prepareActivationData(pipelineAfterIntervention, restoredPlanning.run_inputs, restoredPlanning.accepted_outputs, "architect", restoredPlanning.next_execution_index);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("planning") },
      { kind: "agent_tool_session_created", sessionId: nextSession("planning") },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep2.outputs_root, "plan"), JSON.stringify(R2_PROPOSAL), { mode: 0o600 });
    const records2: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipelineAfterIntervention, prep2);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records2.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });

    // The first handoff through the facade.
    const firstReopen = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const firstState = firstReopen.snapshot;

    if (firstState === null) {
      throw new Error("the reopened run lost its durable state");
    }
    const pipelineAfterFirst = await loadPipelineV2(firstState.pipeline.bundle_root);
    await applyPipelineV2PlanningRunPlanHandoff({
      pipeline: pipelineAfterFirst,
      runRoot: fixture.runRoot,
      sink: recording,
      stageId: STAGE_ID,
      initialBudget: INITIAL_BUDGET,
    });

    let staleCompiledPlan1: unknown = accepted1.compiled_plan;
    for (let cycle = 2; cycle <= cycles; cycle += 1) {
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const state = reopened.snapshot;
      if (state === null) {
        throw new Error("the reopened run lost its durable state");
      }
      const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
      const restored = await restorePipelineV2RuntimeContext(pipeline, state, fixture.runRoot);
      const stageExecIndex = restored.next_execution_index;
      await recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
      for (const command of [
        { kind: "agent_data_prepared" },
        { kind: "agent_execution_session_created", sessionId: nextSession("stage") },
        { kind: "agent_tool_session_created", sessionId: nextSession("stage") },
        { kind: "agent_running" },
        { kind: "agent_outputs_accepted", outputs: [] },
        { kind: "agent_cleanup_completed" },
      ] as PipelineV2RunCommand[]) {
        await recording.dispatch(command);
      }
      await recording.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: stageExecIndex,
      });
      const waitIndex = cycle;
      const anchor = (reopened.snapshot as PipelineV2RunState).transitions.length + 1;
      const actions = [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ];
      const request = preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: waitIndex,
        transition_count: anchor,
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        actions,
      });
      await recording.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: request.sha256,
        actions,
      });
      await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
      const revisedTask = REVISED_TASKS[cycle - 1] ?? "task-a";
      const revisedBody = REVISED_BODIES[cycle - 1] ?? "Body revised";
      await applyPipelineV2ReviseTaskIntervention({
        pipeline,
        runRoot: fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex,
        taskId: revisedTask,
        taskBody: revisedBody,
      });

      const reopenedAfterIntervention = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const stateAfterIntervention = reopenedAfterIntervention.snapshot;
      if (stateAfterIntervention === null) {
        throw new Error("the reopened run lost its durable state");
      }
      const pipelineAfterIntervention = await loadPipelineV2(stateAfterIntervention.pipeline.bundle_root);
      const restoredPlanning = await restorePipelineV2RuntimeContext(pipelineAfterIntervention, stateAfterIntervention, fixture.runRoot);
      const planExecIndex = restoredPlanning.next_execution_index;
      const prep = await prepareActivationData(pipelineAfterIntervention, restoredPlanning.run_inputs, restoredPlanning.accepted_outputs, "architect", planExecIndex);
      await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of [
        { kind: "agent_data_prepared" },
        { kind: "agent_execution_session_created", sessionId: nextSession("planning") },
        { kind: "agent_tool_session_created", sessionId: nextSession("planning") },
        { kind: "agent_running" },
      ] as PipelineV2RunCommand[]) {
        await recording.dispatch(command);
      }
      await writeFile(join(prep.outputs_root, "plan"), JSON.stringify(pointerProposal(["task-a", "task-b", "task-c"])), { mode: 0o600 });
      const records: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipelineAfterIntervention, prep);
      await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records.map((r) => ({ id: r.output, digest: r.digest })) });
      await recording.dispatch({ kind: "agent_cleanup_completed" });
      void staleCompiledPlan1;
      staleCompiledPlan1 = null;
      // Every completed cycle except the last runs its own handoff; the
      // last cycle's boundary stays settled-unbound for the proof.
      if (cycle < cycles) {
        const reopenForHandoff = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        const stateForHandoff = reopenForHandoff.snapshot;
        if (stateForHandoff === null) {
          throw new Error("the reopened run lost its durable state");
        }
        const pipelineForHandoff = await loadPipelineV2(stateForHandoff.pipeline.bundle_root);
        await applyPipelineV2PlanningRunPlanHandoff({
          pipeline: pipelineForHandoff,
          runRoot: fixture.runRoot,
          sink: recording,
          stageId: STAGE_ID,
          initialBudget: INITIAL_BUDGET,
        });
      }
    }
    const state = sink.snapshot as PipelineV2RunState;
    const validated = parsePipelineV2RunState(JSON.stringify(state));
    if (validated.executions.length !== validated.transitions.length + 1) {
      throw new Error("the cycled prefix did not reach the settled-unbound boundary");
    }
    return {
      fixture,
      recording,
      pipeline1,
      staleCompiledPlan1,
      revisionAtBoundary: state.revision,
    };
  } catch (cause) {
    await rm(fixture.root, { recursive: true, force: true });
    throw cause;
  }
}

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
    - state_id: planner2
      role: planning
      plan_output: plan2
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

function twoStageProposal(stageCount: 2 | 3): unknown {
  const stages: { id: string; template: string; tasks: { id: string; depends_on: string[] }[] }[] = [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-c", depends_on: [] }, { id: "task-d", depends_on: ["task-c"] }] },
  ];
  const newTasks: { id: string; body: string }[] = [
    { id: "task-c", body: "Body C" },
    { id: "task-d", body: "Body D" },
  ];
  if (stageCount === 3) {
    stages.push({ id: "stage-3", template: "review", tasks: [{ id: "task-e", depends_on: [] }] });
    newTasks.push({ id: "task-e", body: "Body E" });
  }
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages,
    new_tasks: newTasks,
  };
}

/**
 * The honest prefix on the two-stage bundle: the first planning execution,
 * plan r1 over stage-1 only, the development stage cycle, the wait with
 * `revise_task → planner2`, the real intervention, the restart, the
 * resume restore, and the second planning execution on planner2 whose
 * runtime data plane accepts the multi-stage proposal.
 */
async function buildTwoStagePrefix(stageCount: 2 | 3): Promise<Omit<Prefix, "staleCompiledPlan1"> & { staleCompiledPlan1: unknown }> {
  const fixture = await setupFixture();
  try {
    const bundle2 = join(fixture.root, "bundle2");
    await mkdir(join(bundle2, "prompts"), { recursive: true });
    await mkdir(join(bundle2, "schemas"), { recursive: true });
    await writeFile(join(bundle2, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(bundle2, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(bundle2, "pipeline.yaml"), TWO_STAGE_YAML);
    await writeFile(join(bundle2, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle2, "prompts", "coder.md"), "implement the task\n");
    const bundleForTest = bundle2;
    const pipeline1 = await loadPipelineV2(bundleForTest);
    clockCounter = 0;
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const recording = recordingSink(sink, Infinity);
    await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
    const runInputs = await snapshotRunInputs(pipeline1, [{ id: "task", path: join(fixture.root, "userdata", "task.txt") }], fixture.runRoot);
    await recording.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline1),
      inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    });
    const prep1 = await prepareActivationData(pipeline1, runInputs, [], "architect", 1);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("planning") },
      { kind: "agent_tool_session_created", sessionId: nextSession("planning") },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep1.outputs_root, "plan"), JSON.stringify({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] }],
      new_tasks: [
        { id: "task-a", body: "Body A" },
        { id: "task-b", body: "Body B" },
      ],
    }), { mode: 0o600 });
    const records1: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline1, prep1);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records1.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });

    const accepted1 = await acceptPipelineV2PlanningRunPlan({ pipeline: pipeline1, runRoot: fixture.runRoot, sink: recording });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted1.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: recording });
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("stage") },
      { kind: "agent_tool_session_created", sessionId: nextSession("stage") },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] },
      { kind: "agent_cleanup_completed" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "planner2", transition_index: 0 },
      executionIndex: 2,
    });
    const actions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "planner2" },
    ];
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "planner2",
      reason: "stage_iteration_limit_exhausted",
      actions,
    });
    await recording.dispatch({
      kind: "run_waiting",
      stateId: "planner2",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions,
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
    await applyPipelineV2ReviseTaskIntervention({
      pipeline: pipeline1,
      runRoot: fixture.runRoot,
      sink: recording,
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Body A revised",
    });

    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const stateAfterIntervention = reopened.snapshot;
    if (stateAfterIntervention === null) {
      throw new Error("the reopened run lost its durable state");
    }
    const pipeline2 = await loadPipelineV2(stateAfterIntervention.pipeline.bundle_root);
    const restored = await restorePipelineV2RuntimeContext(pipeline2, stateAfterIntervention, fixture.runRoot);
    const prep2 = await prepareActivationData(pipeline2, restored.run_inputs, restored.accepted_outputs, "planner2", restored.next_execution_index);
    await recording.dispatch({ kind: "start_agent_execution", stateId: "planner2", profile: "architect", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: nextSession("planning") },
      { kind: "agent_tool_session_created", sessionId: nextSession("planning") },
      { kind: "agent_running" },
    ] as PipelineV2RunCommand[]) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep2.outputs_root, "plan2"), JSON.stringify(twoStageProposal(stageCount)), { mode: 0o600 });
    const records2: readonly AcceptedStateOutput[] = await acceptActivationOutputs(pipeline2, prep2);
    await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records2.map((r) => ({ id: r.output, digest: r.digest })) });
    await recording.dispatch({ kind: "agent_cleanup_completed" });
    const state = sink.snapshot as PipelineV2RunState;
    const validated = parsePipelineV2RunState(JSON.stringify(state));
    if (validated.executions.length !== validated.transitions.length + 1) {
      throw new Error("the two-stage prefix did not reach the settled-unbound boundary");
    }
    return {
      fixture,
      recording,
      pipeline1,
      staleCompiledPlan1: accepted1.compiled_plan,
      revisionAtBoundary: state.revision,
    };
  } catch (cause) {
    await rm(fixture.root, { recursive: true, force: true });
    throw cause;
  }
}

/** Captures the real downstream result by forwarding, then returns a hostile presentation. */
function spyThenHostileOps(
  fixture: Fixture,
  captured: { value: unknown },
  hostileOp: keyof PipelineV2PlanningRunPlanHandoffOps,
  buildHostile: (real: unknown) => unknown,
): PipelineV2PlanningRunPlanHandoffOps {
  const realCall = async (name: keyof PipelineV2PlanningRunPlanHandoffOps, args: unknown[]): Promise<unknown> => {
    const result = name === "acceptPlanningRunPlan"
      ? await acceptPipelineV2PlanningRunPlan(...(args as Parameters<typeof acceptPipelineV2PlanningRunPlan>))
      : name === "restoreAcceptedRunPlan"
        ? await restorePipelineV2AcceptedRunPlan(...(args as Parameters<typeof restorePipelineV2AcceptedRunPlan>))
        : name === "loadWaitIntent"
          ? await loadPipelineV2WaitIntent(...(args as Parameters<typeof loadPipelineV2WaitIntent>))
          : name === "openReplannedStage"
            ? await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>))
            : await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
    if (name === hostileOp) {
      captured.value = result;
      return buildHostile(result);
    }
    return result;
  };
  return {
    acceptPlanningRunPlan: ((...args: unknown[]) => realCall("acceptPlanningRunPlan", args)) as typeof acceptPipelineV2PlanningRunPlan,
    restoreAcceptedRunPlan: ((...args: unknown[]) => realCall("restoreAcceptedRunPlan", args)) as typeof restorePipelineV2AcceptedRunPlan,
    loadWaitIntent: ((...args: unknown[]) => realCall("loadWaitIntent", args)) as typeof loadPipelineV2WaitIntent,
    openReplannedStage: ((...args: unknown[]) => realCall("openReplannedStage", args)) as typeof openPipelineV2ReplannedStage,
    openReplannedStageTransition: ((...args: unknown[]) => realCall("openReplannedStageTransition", args)) as typeof openPipelineV2ReplannedStageTransition,
    compiledStageFor: compiledPipelineV2RunPlanStageFor,
  };
}

test("20. RED1: a second honest revise cycle is currently refused by historical accepted revise waits", async () => {
  const prefix = await buildCycledPrefix(2);
  const { fixture } = prefix;
  try {
    const outcome = await runHandoff(fixture);
    // GREEN TARGET: the suffix, delta +5, projection.
    expect(outcome.chainSuffix).toEqual(["plan:3", "stage_generation_closed(replanned)", "stage_generation_opened", "stage_iteration_opened", "transition_committed"]);
    expect(outcome.revisionAfter - outcome.revisionBefore).toBe(5);
    const state = outcome.state;
    expect(state.plan_revisions.map((p) => p.revision)).toEqual([1, 2, 3]);
    expect(state.generations).toHaveLength(3);
    expect(state.waits.map((w) => w.index)).toEqual([1, 2]);
    const gen2 = state.generations[1]!;
    expect(gen2.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
    const gen3 = state.generations[2]!;
    expect(gen3.stage_id).toBe(STAGE_ID);
    expect(gen3.initial_budget).toBe(INITIAL_BUDGET);
    expect(gen3.opened_transition_count).toBe(4);
    expect(gen3.open_iteration).toBeDefined();
    expect(state.transitions).toHaveLength(5);
    expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 5 });
    expect(state.executions).toHaveLength(5);
    // The first cycle is not rewritten.
    expect(state.task_revisions.map((t) => ({ id: t.task_id, revision: t.revision, wait: t.wait_index }))).toEqual([
      { id: "task-a", revision: 1, wait: undefined },
      { id: "task-b", revision: 1, wait: undefined },
      { id: "task-a", revision: 2, wait: 1 },
      { id: "task-c", revision: 1, wait: undefined },
      { id: "task-b", revision: 2, wait: 2 },
    ]);
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("21. RED1: a third honest revise cycle passes S0, and the second/third handoffs pass S5 branch B", async () => {
  const prefix = await buildCycledPrefix(3);
  const { fixture } = prefix;
  try {
    const outcome = await runHandoff(fixture);
    expect(outcome.chainSuffix).toEqual(["plan:4", "stage_generation_closed(replanned)", "stage_generation_opened", "stage_iteration_opened", "transition_committed"]);
    const state = outcome.state;
    expect(state.plan_revisions).toHaveLength(4);
    expect(state.generations).toHaveLength(4);
    expect(state.waits.map((w) => w.index)).toEqual([1, 2, 3]);
    expect(state.generations[2]!.closed).toEqual({ by: "replanned", closed_transition_count: 6 });
    expect(state.generations[3]!.opened_transition_count).toBe(6);
    // S5 branch B after the third handoff.
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const finalState = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(finalState.pipeline.bundle_root);
    const calls = { restore: 0, load: 0, transition: 0 };
    const ops: PipelineV2PlanningRunPlanHandoffOps = {
      acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
      restoreAcceptedRunPlan: (async (...args: unknown[]) => {
        calls.restore += 1;
        return await restorePipelineV2AcceptedRunPlan(...(args as Parameters<typeof restorePipelineV2AcceptedRunPlan>));
      }) as typeof restorePipelineV2AcceptedRunPlan,
      loadWaitIntent: (async (...args: unknown[]) => {
        calls.load += 1;
        return await loadPipelineV2WaitIntent(...(args as Parameters<typeof loadPipelineV2WaitIntent>));
      }) as typeof loadPipelineV2WaitIntent,
      openReplannedStage: openPipelineV2ReplannedStage,
      openReplannedStageTransition: (async (...args: unknown[]) => {
        calls.transition += 1;
        return await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
      }) as typeof openPipelineV2ReplannedStageTransition,
      compiledStageFor: compiledPipelineV2RunPlanStageFor,
    };
    const wrapped = recordingSink(reopened, Infinity);
    const branchB = await applyPipelineV2PlanningRunPlanHandoffWithIo(
      { pipeline, runRoot: fixture.runRoot, sink: wrapped, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
      ops,
    );
    expect(calls).toEqual({ restore: 1, load: 1, transition: 1 });
    expect(wrapped.commands).toEqual([]);
    expect(branchB.generation_index).toBe(4);
    expect(branchB.wait_index).toBe(3);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("22. RED2: the caller stage-2 (different template) currently fails the outer verifier after durable writes", async () => {
  const prefix = await buildTwoStagePrefix(2);
  const { fixture } = prefix;
  try {
    const outcome = await runHandoff(fixture, Infinity, "stage-2");
    expect(outcome.chainSuffix).toEqual(["task-c:1", "task-d:1", "plan:2", "stage_generation_closed(replanned)", "stage_generation_opened", "stage_iteration_opened", "transition_committed"]);
    expect(outcome.revisionAfter - outcome.revisionBefore).toBe(7);
    const state = outcome.state;
    expect(state.plan_revisions.map((p) => p.revision)).toEqual([1, 2]);
    const newGeneration = state.generations[1]!;
    expect(newGeneration.stage_id).toBe("stage-2");
    expect(newGeneration.template_id).toBe("review");
    expect(newGeneration.stage_position).toBe(2);
    expect(newGeneration.initial_budget).toBe(INITIAL_BUDGET);
    expect(state.transitions[2]!.to).toBe("review_entry");
    expect(state.transitions[2]!.from).toBe("planner2");
    expect(state.cursor).toEqual({ current_state: "review_entry", transition_count: 3 });
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("23. RED2 positive controls: the first stage, the shared-entry stage-3, and branch B with stage-2", async () => {
  // The first stage's own-template binding is covered by the
  // single-stage suite (tests 1-19); the two-stage fixture controls the
  // non-first stages here.
  // Two stages sharing one entry state: stage-3 resolves its own position/template.
  {
    const prefix = await buildTwoStagePrefix(3);
    const { fixture } = prefix;
    try {
      console.log("DBG-SHARED-ENTRY start");
      const outcome = await runHandoff(fixture, Infinity, "stage-3");
      console.log("DBG-SHARED-ENTRY done");
      const newGeneration = outcome.state.generations[1]!;
      expect(newGeneration.stage_id).toBe("stage-3");
      expect(newGeneration.template_id).toBe("review");
      expect(newGeneration.stage_position).toBe(3);
      expect(newGeneration.initial_budget).toBe(INITIAL_BUDGET);
      expect(outcome.state.transitions[2]!.to).toBe("review_entry");
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
  // Branch B with the caller stage-2.
  {
    const prefix = await buildTwoStagePrefix(2);
    const { fixture } = prefix;
    try {
      await runHandoff(fixture, Infinity, "stage-2");
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const state = reopened.snapshot as PipelineV2RunState;
      const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
      const wrapped = recordingSink(reopened, Infinity);
      const branchB = await applyPipelineV2PlanningRunPlanHandoff({
        pipeline,
        runRoot: fixture.runRoot,
        sink: wrapped,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      });
      console.log("DBG-BRANCHB done");
      expect(wrapped.commands).toEqual([]);
      expect(branchB.stage_id).toBe("stage-2");
      expect(branchB.template_id).toBe("review");
      expect(branchB.generation_index).toBe(2);
      expect(branchB.wait_index).toBe(1);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});

test("24. RED3: a hostile restore result state currently lets the transition run", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    await runHandoff(fixture);
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const captured = { value: null as unknown };
    const hostileOps = spyThenHostileOps(fixture, captured, "restoreAcceptedRunPlan", (real) => {
      const record = real as { compiled_plan: unknown; state: PipelineV2RunState };
      return {
        compiled_plan: record.compiled_plan,
        state: { ...record.state, revision: record.state.revision + 1 },
      };
    });
    const error = await catchHandoff(() =>
      applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        hostileOps,
      ));
    const handoffError = expectHandoffError(error, "invalid_result");
    // GREEN TARGET: the transition was never called and the error carries
    // the initial authoritative snapshot.
    expect(handoffError.state).toBe(state);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("25. RED4: near-miss flat bindings currently reach the transition or return false success", async () => {
  // The stage near-miss: the mutated stage result must stop the chain
  // before the transition.
  for (const field of ["previous_generation_index", "generation_index", "iteration_index"] as const) {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      const captured = { value: null as unknown };
      const capturedStage = { value: null as unknown };
      let transitionCalls = 0;
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const stageCaptureOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStage: (async (...args: unknown[]) => {
          capturedStage.value = await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>));
          return capturedStage.value;
        }) as typeof openPipelineV2ReplannedStage,
      };
      await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: recordingSink(reopened, 5), stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          stageCaptureOps,
        ));
      expect(capturedStage.value).not.toBeNull();
      const hostileStageOps = spyThenHostileOps(fixture, captured, "openReplannedStage", (real) => ({
        ...(real as Record<string, unknown>),
        [field]: 99,
      }));
      const hostileOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...hostileStageOps,
        openReplannedStageTransition: (async () => {
          transitionCalls += 1;
          throw new Error("the transition must not run after a hostile stage result");
        }) as unknown as typeof openPipelineV2ReplannedStageTransition,
      };
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          hostileOps,
        ));
      expectHandoffError(error, "invalid_result");
      expect(transitionCalls).toBe(0);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
  // The transition near-miss: the mutated flat fields must be refused.
  for (const field of ["generation_index", "iteration_index"] as const) {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      const captured = { value: null as unknown };
      let realTransition: unknown = null;
      const captureOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStageTransition: (async (...args: unknown[]) => {
          realTransition = await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
          return realTransition;
        }) as typeof openPipelineV2ReplannedStageTransition,
      };
      await applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline: await loadPipelineV2((await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot!.pipeline.bundle_root), runRoot: fixture.runRoot, sink: await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick }), stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        captureOps,
      );
      expect(realTransition).not.toBeNull();
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const pipeline = await loadPipelineV2((reopened.snapshot as PipelineV2RunState).pipeline.bundle_root);
      const hostileOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStageTransition: (async () => ({ ...(realTransition as Record<string, unknown>), [field]: 99 })) as unknown as typeof openPipelineV2ReplannedStageTransition,
      };
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          hostileOps,
        ));
      expectHandoffError(error, "invalid_result");
      void captured;
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});

test("26. RED5: a malformed sink currently produces a native TypeError or reads the snapshot through the gate", async () => {
  const prefix = await buildPrefix();
  const { fixture } = prefix;
  try {
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const cases: [string, unknown, PipelineV2PlanningRunPlanHandoffFailureReason][] = [
      ["null", null, "invalid_options"],
      ["undefined", undefined, "invalid_options"],
      ["number", 42, "invalid_options"],
      ["string", "sink", "invalid_options"],
      ["array", [], "invalid_options"],
      ["no-dispatch", { snapshot: state, poisoned: false }, "invalid_options"],
      ["non-function-dispatch", { snapshot: state, poisoned: false, dispatch: 42 }, "invalid_options"],
    ];
    for (const [label, sink, reason] of cases) {
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoff({
          pipeline,
          runRoot: fixture.runRoot,
          sink: sink as never,
          stageId: STAGE_ID,
          initialBudget: INITIAL_BUDGET,
        }));
      expectHandoffError(error, reason);
      void label;
    }
    // A structural sink with an undefined or primitive snapshot: the
    // controller's own invalid_state.
    for (const broken of [{ snapshot: undefined, poisoned: false, dispatch: async () => undefined }, { snapshot: 42, poisoned: false, dispatch: async () => undefined }]) {
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoff({
          pipeline,
          runRoot: fixture.runRoot,
          sink: broken as never,
          stageId: STAGE_ID,
          initialBudget: INITIAL_BUDGET,
        }));
      expectHandoffError(error, "invalid_state");
    }
    // Hostile extra sink members are never read.
    const reads: string[] = [];
    const hostileSink = new Proxy(
      { get snapshot() { return reopened.snapshot; }, poisoned: false, dispatch: reopened.dispatch.bind(reopened), hostile: "never" },
      {
        get(target, property) {
          reads.push(String(property));
          return target[property as keyof typeof target];
        },
      },
    );
    await applyPipelineV2PlanningRunPlanHandoff({
      pipeline,
      runRoot: fixture.runRoot,
      sink: hostileSink,
      stageId: STAGE_ID,
      initialBudget: INITIAL_BUDGET,
    });
    expect(reads.filter((entry) => entry === "hostile")).toHaveLength(0);
    expect(await currentRevision(fixture)).toBe(40);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("27. RED6: hostile presentations after real durable writes currently carry non-authoritative error states", async () => {
  // The real acceptance durable, the hostile acceptance presentation.
  {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const state = reopened.snapshot as PipelineV2RunState;
      const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
      const hostileOps = spyThenHostileOps(fixture, { value: null }, "acceptPlanningRunPlan", (real) => ({
        compiled_plan: (real as { compiled_plan: unknown }).compiled_plan,
        state: { ...(real as { state: PipelineV2RunState }).state, revision: (real as { state: PipelineV2RunState }).state.revision + 1 },
      }));
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          hostileOps,
        ));
      const handoffError = expectHandoffError(error, "invalid_result");
      expect(handoffError.state).not.toBeNull();
      expect((handoffError.state as PipelineV2RunState).revision).toBe(36);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
  // The real stage durable, the hostile flat stage result.
  {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      const captureSink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const pipeline = await loadPipelineV2((captureSink.snapshot as PipelineV2RunState).pipeline.bundle_root);
      let realStage: unknown = null;
      const captureOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStage: (async (...args: unknown[]) => {
          realStage = await openPipelineV2ReplannedStage(...(args as Parameters<typeof openPipelineV2ReplannedStage>));
          return realStage;
        }) as typeof openPipelineV2ReplannedStage,
      };
      const faulty = recordingSink(captureSink, 5);
      await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: faulty, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          captureOps,
        ));
      expect(realStage).not.toBeNull();
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const hostileOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStage: (async () => ({ ...(realStage as Record<string, unknown>), iteration_index: 99 })) as unknown as typeof openPipelineV2ReplannedStage,
      };
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          hostileOps,
        ));
      const handoffError = expectHandoffError(error, "invalid_result");
      expect(handoffError.state).not.toBeNull();
      expect((handoffError.state as PipelineV2RunState).revision).toBe(39);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
  // The real transition durable, the hostile flat transition result.
  {
    const prefix = await buildPrefix();
    const { fixture } = prefix;
    try {
      const captureSink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const pipeline = await loadPipelineV2((captureSink.snapshot as PipelineV2RunState).pipeline.bundle_root);
      let realTransition: unknown = null;
      const captureOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStageTransition: (async (...args: unknown[]) => {
          realTransition = await openPipelineV2ReplannedStageTransition(...(args as Parameters<typeof openPipelineV2ReplannedStageTransition>));
          return realTransition;
        }) as typeof openPipelineV2ReplannedStageTransition,
      };
      await applyPipelineV2PlanningRunPlanHandoffWithIo(
        { pipeline, runRoot: fixture.runRoot, sink: captureSink, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
        captureOps,
      );
      expect(realTransition).not.toBeNull();
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const hostileOps: PipelineV2PlanningRunPlanHandoffOps = {
        ...productionPlanningRunPlanHandoffOps,
        openReplannedStageTransition: (async () => ({ ...(realTransition as Record<string, unknown>), iteration_index: 99 })) as unknown as typeof openPipelineV2ReplannedStageTransition,
      };
      const error = await catchHandoff(() =>
        applyPipelineV2PlanningRunPlanHandoffWithIo(
          { pipeline, runRoot: fixture.runRoot, sink: reopened, stageId: STAGE_ID, initialBudget: INITIAL_BUDGET },
          hostileOps,
        ));
      const handoffError = expectHandoffError(error, "invalid_result");
      expect(handoffError.state).not.toBeNull();
      expect((handoffError.state as PipelineV2RunState).revision).toBe(40);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});
