import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  PipelineV2StateError,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { PipelineV2RunStateDurabilityError, PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  openPipelineV2InitialStageTransition,
  PipelineV2InitialStageTransitionControllerError as PublicError,
} from "../src/pipeline_v2_initial_stage_transition_controller.ts";
import { openPipelineV2InitialStageTransitionInternal, PipelineV2InitialStageTransitionControllerError } from "../src/pipeline_v2_initial_stage_transition_controller_internal.ts";
import { compilePipelineV2RunPlanCandidate, PipelineV2CompiledRunPlanError } from "../src/pipeline_v2_run_plan_compiled.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { preparePlanRevisionManifest, prepareTaskRevisionManifest } from "../src/pipeline_v2_run_plan_manifests.ts";
import { loadPipelineV2 as loadPipelineV2Alias } from "../src/pipeline_v2.ts";

const RUN_ID = "initial-transition";
const INITIAL_BUDGET = 2;
const STAGE_WAIT = `      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task`;

const PIPELINE = `
schema_version: 2
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
${STAGE_WAIT}
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: planner2
      role: planning
      plan_output: plan2
${STAGE_WAIT}

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
        to: planner2
  - id: planner2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const EDGE_PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

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
${STAGE_WAIT}
    - state_id: gate
      role: control
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: review_entry
      role: stage
      stage_template: review
    - state_id: planner2
      role: planning
      plan_output: plan2
${STAGE_WAIT}

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
        to: gate
  - id: gate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
        to: review_entry
      - outcome: beta
        to: dev_entry
      - outcome: uncovered
        to: dev_entry
      - outcome: inconsistent_facts
        to: dev_entry
      - outcome: invalid_facts
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
        to: planner2
  - id: planner2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
decisions:
  - id: alpha
  - id: beta
relations: []
constraints: []
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f1, equals: false}
    decision: beta
`;

const PROPOSAL_TWO_STAGES = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

interface Fixture {
  root: string;
  bundle: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
}

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clockCounter));
}

async function makeFixture(options: { edge?: boolean; control?: boolean } = {}): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "initial-transition-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), options.edge === true ? EDGE_PIPELINE : options.control === true ? CONTROL_PIPELINE : PIPELINE);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "WORK\n");
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.md"), "TASK-BODY\n", { mode: 0o600 });
  if (options.control === true || options.edge === true) {
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
    await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true }), { mode: 0o600 });
  }
  if (options.edge === true) {
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
    await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify({ type: "object" }));
    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true }), { mode: 0o600 });
  }
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  return { root, bundle, stateRoot, runRoot, statePath: join(runRoot, "state.json") };
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

/** A sink presenting a hostile clone of the authoritative snapshot on every read. */
function mutateSnapshotSink(
  inner: PipelineV2RunStateSink,
  mutate: (state: PipelineV2RunState) => void,
): Recording {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    get snapshot(): PipelineV2RunState | null {
      const snapshot = inner.snapshot;
      if (snapshot === null) {
        return null;
      }
      const clone = structuredClone(snapshot) as PipelineV2RunState;
      mutate(clone);
      return clone;
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

/** A sink committing every dispatch but presenting a hostile clone after the dispatch. */
function mutatedResultSink(
  inner: PipelineV2RunStateSink,
  mutate: (state: PipelineV2RunState) => void,
): Recording {
  const commands: PipelineV2RunCommand[] = [];
  let dispatched = false;
  return {
    commands,
    get snapshot(): PipelineV2RunState | null {
      const snapshot = inner.snapshot;
      if (snapshot === null || !dispatched) {
        return snapshot;
      }
      const clone = structuredClone(snapshot) as PipelineV2RunState;
      mutate(clone);
      return clone;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command) {
      commands.push({ ...command });
      await inner.dispatch(command);
      dispatched = true;
    },
  };
}

const CONTROL_PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
${STAGE_WAIT}
    - state_id: gate
      role: control
    - state_id: planner2
      role: planning
      plan_output: plan2
${STAGE_WAIT}
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
        to: gate
  - id: gate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
        to: planner2
      - outcome: beta
        to: planner2
      - outcome: uncovered
        to: planner2
      - outcome: inconsistent_facts
        to: planner2
      - outcome: invalid_facts
        to: planner2
  - id: planner2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
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
        to: done
  - id: done
    type: terminal
    result: success
`;

const PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
};

async function runPlanningActivation(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  recording: Recording,
  stateId: string,
  executionIndex: number,
  proposal: unknown = PROPOSAL,
  outputId = "plan",
): Promise<AcceptedStateOutput[]> {
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await recording.dispatch({ kind: "start_agent_execution", stateId, profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` },
    { kind: "agent_running" },
  ] as PipelineV2RunCommand[]) {
    await recording.dispatch(command);
  }
  await writeFile(join(activation.outputs_root, outputId), JSON.stringify(proposal), { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, activation);
  await recording.dispatch({ kind: "agent_outputs_accepted", outputs: records.map((r) => ({ id: r.output, digest: r.digest })) });
  await recording.dispatch({ kind: "agent_cleanup_completed" });
  return [...accepted, ...records];
}

/**
 * The honest accepted initial boundary through the real facades only:
 * the planning execution settled and unbound with the accepted initial
 * plan revision, before any generation/iteration/transition exists.
 */
async function acceptedBoundary(): Promise<{
  fixture: Fixture;
  recording: Recording;
  inner: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  compiledPlan: ReturnType<typeof compilePipelineV2RunPlanCandidate>;
  state: PipelineV2RunState;
  runInputs: RunInputsSnapshot;
  taskDigest: string;
}> {
  const fixture = await makeFixture();
  const pipeline = await loadPipelineV2(fixture.bundle);
  const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
  const recording = recordingSink(inner);
  await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
  const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: join(fixture.root, "userdata", "task.md") }], fixture.runRoot);
  await recording.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
  });
  await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1);
  const taskDigest = runInputs.inputs[0]!.digest;
  const { accepted } = await acceptInitialPlan(pipeline, fixture, recording, taskDigest);
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: recording as never,
  });
  return { fixture, recording, inner, pipeline, compiledPlan: accepted, state: recording.snapshot as PipelineV2RunState, runInputs, taskDigest };
}

async function acceptInitialPlan(
  pipeline: ResolvedPipelineV2,
  fixture: Fixture,
  recording: Recording,
  taskDigest: string,
): Promise<{ accepted: ReturnType<typeof compilePipelineV2RunPlanCandidate> }> {
  const taskManifest = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-a",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "Body A",
  });
  const planManifest = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: taskDigest },
    origin_execution: 1,
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }],
      },
    ],
  });
  const candidate = preparePipelineV2RunPlanCandidate({
    plan: planManifest,
    taskRevisions: [taskManifest],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: taskDigest,
  });
  await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording as never, candidate });
  return { accepted: compilePipelineV2RunPlanCandidate(pipeline, candidate) };
}

function expectTransitionError(cause: unknown): PipelineV2InitialStageTransitionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2InitialStageTransitionControllerError);
  return cause as PipelineV2InitialStageTransitionControllerError;
}

function callTransition(
  ctx: Awaited<ReturnType<typeof acceptedBoundary>>,
  sink: unknown = ctx.recording,
  overrides: Record<string, unknown> = {},
): Promise<unknown> {
  return openPipelineV2InitialStageTransitionInternal({
    pipeline: ctx.pipeline,
    sink: sink as never,
    compiledPlan: ctx.compiledPlan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    ...overrides,
  });
}

describe("openPipelineV2InitialStageTransition", () => {
  test("1. C0 commits exactly one exact initial planning transition", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const result = (await callTransition(ctx)) as Record<string, unknown>;
      expect(Object.keys(result).sort()).toEqual([
        "execution_index", "from_state", "generation_index", "initial_budget", "iteration_index",
        "origin_execution", "plan_revision", "plan_sha256", "stage_id", "stage_position",
        "state", "template_id", "to_state", "transition_index",
      ]);
      expect(result["from_state"]).toBe("architect");
      expect(result["to_state"]).toBe("dev_entry");
      expect(result["transition_index"]).toBe(0);
      expect(result["execution_index"]).toBe(1);
      expect(result["origin_execution"]).toBe(1);
      expect(result["generation_index"]).toBe(1);
      expect(result["iteration_index"]).toBe(1);
      expect(result["stage_id"]).toBe("stage-1");
      expect(result["stage_position"]).toBe(1);
      expect(result["template_id"]).toBe("development");
      expect(result["initial_budget"]).toBe(INITIAL_BUDGET);
      expect(result["plan_revision"]).toBe(1);
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([
        {
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
          executionIndex: 1,
        },
      ]);
      const state = ctx.recording.snapshot as PipelineV2RunState;
      expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 1 });
      expect(state.waits).toHaveLength(0);
      expect(state.generations).toHaveLength(1);
      expect(state.generations[0]?.stage_id).toBe("stage-1");
      expect(state.generations[0]?.initial_budget).toBe(INITIAL_BUDGET);
      expect(state.generations[0]?.opened_transition_count).toBe(0);
      expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("2. C1: the exact durable transition is a zero-dispatch retry with the identical result", async () => {
    const ctx = await acceptedBoundary();
    try {
      const first = (await callTransition(ctx)) as Record<string, unknown>;
      const commandsAfterFirst = ctx.recording.commands.length;
      const retry = (await callTransition(ctx)) as Record<string, unknown>;
      expect(ctx.recording.commands.slice(commandsAfterFirst)).toEqual([]);
      expect(retry).toEqual(first);
      expect(retry["state"]).toEqual(first["state"]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("3. the fresh acceptance boundary without the opened generation is refused with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      const hostile = mutateSnapshotSink(ctx.inner, (state) => {
        (state as unknown as Record<string, unknown>)["generations"] = [];
      });
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx, hostile).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toBe("the initial stage transition requires the opened generation of the selected stage");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("4. the control-prefix boundary commits the transition of the third execution", async () => {
    const fixture = await makeFixture({ control: true });
    try {
      const pipeline = await loadPipelineV2Alias(fixture.bundle);
      const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(inner);
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(fixture.root, "userdata", "task.md") },
          { id: "facts_seed", path: join(fixture.root, "userdata", "facts.json") },
        ],
        fixture.runRoot,
      );
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1, {}, "plan");
      await recording.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "gate", transition_index: 0 },
        executionIndex: 1,
      });
      await recording.dispatch({ kind: "start_decision_execution", stateId: "gate", inputDigest: "c".repeat(64), executionRole: "control" });
      await recording.dispatch({
        kind: "decision_evaluated",
        result: { status: "selected", outcome: "beta", decision: "beta", rule_id: "rule-b", active_constraint_ids: [] },
      });
      await recording.dispatch({
        kind: "transition_committed",
        step: { from: "gate", outcome: "beta", to: "planner2", transition_index: 1 },
        executionIndex: 2,
      });
      await runPlanningActivation(pipeline, runInputs, [], recording, "planner2", 3, {}, "plan2");
      const taskDigest = runInputs.inputs[0]!.digest;
      const taskManifest = prepareTaskRevisionManifest({
        schema_version: 1,
        kind: "task_revision",
        run_id: RUN_ID,
        task_id: "task-a",
        revision: 1,
        previous_sha256: null,
        origin: "planning_proposal",
        body: "Body A",
      });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 3,
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }],
          },
        ],
      });
      const candidate = preparePipelineV2RunPlanCandidate({
        plan: planManifest,
        taskRevisions: [taskManifest],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: taskDigest,
      });
      await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording as never, candidate });
      const compiledPlan = compilePipelineV2RunPlanCandidate(pipeline, candidate);
      await ensurePipelineV2StageIteration({ compiledPlan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: recording as never });
      const commandsBefore = recording.commands.length;
      const result = (await callTransition({ fixture, recording, inner, pipeline, compiledPlan, state: recording.snapshot as PipelineV2RunState } as never)) as Record<string, unknown>;
      expect(result["from_state"]).toBe("planner2");
      expect(result["to_state"]).toBe("dev_entry");
      expect(result["execution_index"]).toBe(3);
      expect(result["origin_execution"]).toBe(3);
      expect(result["transition_index"]).toBe(0);
      expect(result["generation_index"]).toBe(1);
      const state = recording.snapshot as PipelineV2RunState;
      expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
      expect(state.generations[0]?.opened_transition_count).toBe(2);
      expect(recording.commands.slice(commandsBefore)).toEqual([
        {
          kind: "transition_committed",
          step: { from: "planner2", outcome: "completed", to: "dev_entry", transition_index: 0 },
          executionIndex: 3,
        },
      ]);
      expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("5. the pipeline provenance gate rejects hand-built, cloned and Proxy pipelines before any read or effect", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const handBuilt = { schema_version: 2 } as unknown as ResolvedPipelineV2;
      const cause = await callTransition(ctx, ctx.recording, { pipeline: handBuilt }).catch((error) => error);
      expect(cause).toBeInstanceOf(PipelineError);
      expect((cause as Error).message).toContain("the initial stage transition controller requires the deep-frozen snapshot");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
      const spread = { ...(ctx.pipeline as unknown as unknown as Record<string, unknown>) } as unknown as ResolvedPipelineV2;
      const cause2 = await callTransition(ctx, ctx.recording, { pipeline: spread }).catch((error) => error);
      expect(cause2).toBeInstanceOf(PipelineError);
      let traps = 0;
      const proxied = new Proxy(ctx.pipeline as unknown as Record<string, unknown>, {
        get(target, property) {
          traps += 1;
          return target[property as keyof typeof target];
        },
      });
      const cause3 = await callTransition(ctx, ctx.recording, { pipeline: proxied }).catch((error) => error);
      expect(cause3).toBeInstanceOf(PipelineError);
      expect(traps).toBe(0);
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("6. the invalid_options matrix refuses before any read or effect", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const cases: Array<[string, Record<string, unknown>]> = [
        ["budget zero", { initialBudget: 0 }],
        ["budget negative", { initialBudget: -1 }],
        ["budget fraction", { initialBudget: 1.5 }],
        ["budget overflow", { initialBudget: Number.MAX_SAFE_INTEGER + 1 }],
        ["budget string", { initialBudget: "2" }],
        ["stage id number", { stageId: 7 }],
      ];
      for (const [label, overrides] of cases) {
        const cause = await callTransition(ctx, ctx.recording, overrides).catch((error) => error);
        const error = expectTransitionError(cause);
        expect(error.reason).toBe("invalid_options");
        expect(label + ":" + error.message.length).toBe(label + ":" + error.message.length);
      }
      // An unsafe stage id that is still a string reaches the trusted
      // compiled resolver; its typed error passes by identity.
      const causeUnsafe = await callTransition(ctx, ctx.recording, { stageId: "../escape" }).catch((error) => error);
      expect(causeUnsafe).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect((causeUnsafe as Error).message).toContain("safe stage id");
      const causeSink = await callTransition(ctx, { poisoned: false }).catch((error) => error);
      expect(expectTransitionError(causeSink).reason).toBe("invalid_options");
      const causeDispatch = await callTransition(ctx, { poisoned: false, snapshot: ctx.recording.snapshot, dispatch: 42 }).catch((error) => error);
      expect(expectTransitionError(causeDispatch).reason).toBe("invalid_options");
      const causeNonRecord = await callTransition(ctx, 42).catch((error) => error);
      expect(expectTransitionError(causeNonRecord).reason).toBe("invalid_options");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("7. the poisoned sink refuses with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const poisonedSink = {
        get snapshot() {
          return ctx.inner.snapshot;
        },
        get poisoned() {
          return true;
        },
        async dispatch() {
          throw new Error("must not dispatch");
        },
      };
      const cause = await callTransition(ctx, poisonedSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the run state sink is poisoned; no initial stage transition can be committed");
      expect(error.state).toBeNull();
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("8. a missing or invalid durable snapshot is a typed invalid_state with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const nullSink = {
        get snapshot() {
          return null;
        },
        get poisoned() {
          return false;
        },
        async dispatch() {
          throw new Error("must not dispatch");
        },
      };
      const cause = await callTransition(ctx, nullSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the durable run state is missing or not a valid pipeline v2 run state");
      expect(error.state).toBeNull();
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("9. a foreign compiled plan is a lifecycle_conflict naming the identity field", async () => {
    const ctx = await acceptedBoundary();
    try {
      // Compile the same candidate against a modified bundle: the hidden
      // originating identity no longer matches the durable pipeline.
      const foreignBundle = join(ctx.fixture.root, "foreign-bundle");
      await mkdir(join(foreignBundle, "prompts"), { recursive: true });
      await mkdir(join(foreignBundle, "schemas"), { recursive: true });
      await writeFile(join(foreignBundle, "pipeline.yaml"), PIPELINE.replace("WORK\n", "").replace("WORK", "OTHER-WORK"));
      await writeFile(join(foreignBundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
      await writeFile(join(foreignBundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
      await writeFile(join(foreignBundle, "prompts", "coder.md"), "OTHER-WORK\n");
      const foreignPipeline = await loadPipelineV2Alias(foreignBundle);
      const taskDigest = "e942adc4405e3a607ed742a45ac856c9b61995b731645713a3390b16ac032948";
      const taskManifest = prepareTaskRevisionManifest({
        schema_version: 1,
        kind: "task_revision",
        run_id: RUN_ID,
        task_id: "task-a",
        revision: 1,
        previous_sha256: null,
        origin: "planning_proposal",
        body: "Body A",
      });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 1,
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }] }],
      });
      const candidate = preparePipelineV2RunPlanCandidate({
        plan: planManifest,
        taskRevisions: [taskManifest],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: taskDigest,
      });
      const foreignPlan = compilePipelineV2RunPlanCandidate(foreignPipeline, candidate);
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx, ctx.recording, { compiledPlan: foreignPlan }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toContain("bundle_root");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("10. an unknown stage id is the compiled resolver's typed error with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx, ctx.recording, { stageId: "stage-9" }).catch((error) => error);
      expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect((cause as Error).message).toContain("stage-9");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("11. a failed run is refused with zero dispatch", async () => {
    const fixture = await makeFixture();
    try {
      const pipeline = await loadPipelineV2Alias(fixture.bundle);
      const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(inner);
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: join(fixture.root, "userdata", "task.md") }], fixture.runRoot);
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
      for (const command of [
        { kind: "agent_data_prepared" },
        { kind: "agent_execution_session_created", sessionId: "exec-1" },
        { kind: "agent_tool_session_created", sessionId: "tool-1" },
        { kind: "agent_running" },
      ] as PipelineV2RunCommand[]) {
        await recording.dispatch(command);
      }
      await recording.dispatch({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
      await recording.dispatch({ kind: "run_failed", reason: "worker_failed" });
      const taskDigest = runInputs.inputs[0]!.digest;
      const taskManifest = prepareTaskRevisionManifest({ schema_version: 1, kind: "task_revision", run_id: RUN_ID, task_id: "task-a", revision: 1, previous_sha256: null, origin: "planning_proposal", body: "Body A" });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 1,
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }] }],
      });
      const candidate = preparePipelineV2RunPlanCandidate({ plan: planManifest, taskRevisions: [taskManifest], previousPlan: null, previousTaskRevisions: [], protectedInputDigest: taskDigest });
      const unaccepted = compilePipelineV2RunPlanCandidate(pipeline, candidate);
      const commandsBefore = recording.commands.length;
      const cause = await openPipelineV2InitialStageTransitionInternal({
        pipeline,
        sink: recording as never,
        compiledPlan: unaccepted,
        stageId: "stage-1",
        initialBudget: INITIAL_BUDGET,
      }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the initial stage transition requires an active running run");
      expect(recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("12. the pre-acceptance boundary without a plan revision is refused", async () => {
    const fixture = await makeFixture();
    try {
      const pipeline = await loadPipelineV2Alias(fixture.bundle);
      const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(inner);
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: join(fixture.root, "userdata", "task.md") }], fixture.runRoot);
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1, {}, "plan");
      const commandsBefore = recording.commands.length;
      const taskDigest = runInputs.inputs[0]!.digest;
      const taskManifest = prepareTaskRevisionManifest({ schema_version: 1, kind: "task_revision", run_id: RUN_ID, task_id: "task-a", revision: 1, previous_sha256: null, origin: "planning_proposal", body: "Body A" });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 1,
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }] }],
      });
      const candidate = preparePipelineV2RunPlanCandidate({ plan: planManifest, taskRevisions: [taskManifest], previousPlan: null, previousTaskRevisions: [], protectedInputDigest: taskDigest });
      const unaccepted = compilePipelineV2RunPlanCandidate(pipeline, candidate);
      const cause = await callTransition({ fixture, recording, inner, pipeline, compiledPlan: unaccepted, state: recording.snapshot as PipelineV2RunState } as never).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the initial stage transition requires exactly one accepted plan revision");
      expect(recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("13. an in-flight planning execution is refused", async () => {
    const fixture = await makeFixture();
    try {
      const pipeline = await loadPipelineV2Alias(fixture.bundle);
      const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(inner);
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: join(fixture.root, "userdata", "task.md") }], fixture.runRoot);
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
      await recording.dispatch({ kind: "agent_data_prepared" });
      const taskDigest = runInputs.inputs[0]!.digest;
      const taskManifest = prepareTaskRevisionManifest({ schema_version: 1, kind: "task_revision", run_id: RUN_ID, task_id: "task-a", revision: 1, previous_sha256: null, origin: "planning_proposal", body: "Body A" });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 1,
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }] }],
      });
      const candidate = preparePipelineV2RunPlanCandidate({ plan: planManifest, taskRevisions: [taskManifest], previousPlan: null, previousTaskRevisions: [], protectedInputDigest: taskDigest });
      const unaccepted = compilePipelineV2RunPlanCandidate(pipeline, candidate);
      const cause = await callTransition({ fixture, recording, inner, pipeline, compiledPlan: unaccepted, state: recording.snapshot as PipelineV2RunState } as never).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the initial stage transition requires the settled unbound planning execution on the cursor");
      expect(recording.commands).toHaveLength(3);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("14. a settled stage execution (not planning) on the cursor is refused", async () => {
    const ctx = await acceptedBoundary();
    try {
      await callTransition(ctx);
      // The dev_entry stage execution runs to its settled-unbound boundary.
      const runInputs = ctx.runInputs;
      await ctx.recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
      for (const command of [
        { kind: "agent_data_prepared" },
        { kind: "agent_execution_session_created", sessionId: "exec-2" },
        { kind: "agent_tool_session_created", sessionId: "tool-2" },
        { kind: "agent_running" },
      ] as PipelineV2RunCommand[]) {
        await ctx.recording.dispatch(command);
      }
      await ctx.recording.dispatch({ kind: "agent_outputs_accepted", outputs: [] });
      await ctx.recording.dispatch({ kind: "agent_cleanup_completed" });
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the initial stage transition requires the settled unbound planning execution on the cursor");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("15. a compiled plan that does not match the accepted plan revision is a lifecycle_conflict", async () => {
    const ctx = await acceptedBoundary();
    try {
      // A different candidate (different task body) compiled but never
      // accepted; its plan digest differs from the durable record.
      const taskManifest = prepareTaskRevisionManifest({
        schema_version: 1,
        kind: "task_revision",
        run_id: RUN_ID,
        task_id: "task-a",
        revision: 1,
        previous_sha256: null,
        origin: "planning_proposal",
        body: "Body B",
      });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: ctx.taskDigest },
        origin_execution: 1,
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskManifest.sha256, depends_on: [] }] }],
      });
      const candidate = preparePipelineV2RunPlanCandidate({
        plan: planManifest,
        taskRevisions: [taskManifest],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: ctx.taskDigest,
      });
      const otherPlan = compilePipelineV2RunPlanCandidate(ctx.pipeline, candidate);
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx, ctx.recording, { compiledPlan: otherPlan }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toBe("the accepted plan revision does not match the compiled plan of the initial boundary");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("16. a durable generation bound to another budget is a lifecycle_conflict", async () => {
    const fixture = await makeFixture();
    const pipeline = await loadPipelineV2Alias(fixture.bundle);
    const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const recording = recordingSink(inner);
    try {
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: join(fixture.root, "userdata", "task.md") }], fixture.runRoot);
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1);
      const taskDigest = runInputs.inputs[0]!.digest;
      const { accepted } = await acceptInitialPlan(pipeline, fixture, recording, taskDigest);
      await ensurePipelineV2StageIteration({ compiledPlan: accepted, stageId: "stage-1", initialBudget: 3, sink: recording as never });
      const commandsBefore = recording.commands.length;
      const cause = await callTransition({ fixture, recording, inner, pipeline, compiledPlan: accepted, state: recording.snapshot as PipelineV2RunState } as never).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toBe("the durable stage generation does not match the selected initial stage and budget");
      expect(recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("17. a committed transition to a different target is a C1 lifecycle_conflict with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      // Commit the planning transition to a different declared target
      // through the raw reducer (loader-valid, but not the selected
      // stage's entry state).
      await ctx.recording.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "planner2", transition_index: 0 },
        executionIndex: 1,
      });
      const commandsBefore = ctx.recording.commands.length;
      const cause = await callTransition(ctx).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toBe("the durable initial planning transition does not match the exact step of the initial handoff boundary");
      expect(ctx.recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("18. the edge gate refuses a stage whose entry differs from the completed edge with zero generation writes", async () => {
    const fixture = await makeFixture({ edge: true });
    try {
      const pipeline = await loadPipelineV2Alias(fixture.bundle);
      const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(inner);
      await prepareRunProject(join(fixture.root, "project-source"), fixture.runRoot);
      const runInputs = await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(fixture.root, "userdata", "task.md") },
          { id: "facts_seed", path: join(fixture.root, "userdata", "facts.json") },
        ],
        fixture.runRoot,
      );
      await recording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(pipeline),
        inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
      });
      await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1, PROPOSAL_TWO_STAGES, "plan");
      const taskDigest = runInputs.inputs[0]!.digest;
      const taskA = prepareTaskRevisionManifest({ schema_version: 1, kind: "task_revision", run_id: RUN_ID, task_id: "task-a", revision: 1, previous_sha256: null, origin: "planning_proposal", body: "Body A" });
      const taskB = prepareTaskRevisionManifest({ schema_version: 1, kind: "task_revision", run_id: RUN_ID, task_id: "task-b", revision: 1, previous_sha256: null, origin: "planning_proposal", body: "Body B" });
      const planManifest = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: taskDigest },
        origin_execution: 1,
        stages: [
          { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }] },
          { id: "stage-2", template: "review", tasks: [{ id: "task-b", revision: 1, sha256: taskB.sha256, depends_on: [] }] },
        ],
      });
      const candidate = preparePipelineV2RunPlanCandidate({
        plan: planManifest,
        taskRevisions: [taskA, taskB],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: taskDigest,
      });
      await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording as never, candidate });
      const compiledPlan = compilePipelineV2RunPlanCandidate(pipeline, candidate);
      await ensurePipelineV2StageIteration({ compiledPlan, stageId: "stage-2", initialBudget: INITIAL_BUDGET, sink: recording as never });
      const commandsBefore = recording.commands.length;
      const cause = await openPipelineV2InitialStageTransitionInternal({
        pipeline,
        sink: recording as never,
        compiledPlan,
        stageId: "stage-2",
        initialBudget: INITIAL_BUDGET,
      }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toBe("the completed planning transition does not target the selected stage's entry state");
      expect(recording.commands.slice(commandsBefore)).toEqual([]);
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  test("19. not_committed keeps the previous snapshot and a fresh retry commits the transition", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const faultSink = {
        get snapshot() {
          return ctx.inner.snapshot;
        },
        get poisoned() {
          return ctx.inner.poisoned;
        },
        async dispatch() {
          throw new PipelineV2RunStateStoreError("the store refused the commit");
        },
      };
      const cause = await callTransition(ctx, faultSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("state_persist_failed");
      expect(error.message).toBe("the initial stage transition could not be committed");
      expect(error.state?.revision).toBe((ctx.recording.snapshot as PipelineV2RunState).revision);
      // A fresh retry through the real sink commits exactly once.
      const result = (await callTransition(ctx)) as Record<string, unknown>;
      expect(result["to_state"]).toBe("dev_entry");
      expect(ctx.recording.commands.slice(commandsBefore)).toHaveLength(1);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("20. durability_unknown adopts the candidate and a fresh reopened sink recognizes the transition with zero dispatch", async () => {
    const ctx = await acceptedBoundary();
    try {
      const commandsBefore = ctx.recording.commands.length;
      const faultSink = {
        get snapshot() {
          return ctx.inner.snapshot;
        },
        get poisoned() {
          return ctx.inner.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.inner.dispatch(command);
          throw new PipelineV2RunStateDurabilityError(0, ctx.inner.snapshot as PipelineV2RunState, "the commit could not be confirmed durable");
        },
      };
      const cause = await callTransition(ctx, faultSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("state_persist_failed");
      expect(error.message).toBe("the initial stage transition could not be confirmed durable");
      // The candidate is visible on disk (the store committed it).
      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      expect(reopened.snapshot?.cursor).toEqual({ current_state: "dev_entry", transition_count: 1 });
      const commandsAfterFault = ctx.recording.commands.length;
      const retry = (await callTransition({ ...ctx, recording: recordingSink(reopened), inner: reopened })) as Record<string, unknown>;
      expect(retry["to_state"]).toBe("dev_entry");
      expect(ctx.recording.commands.slice(commandsAfterFault)).toHaveLength(0);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("21. a racing identical dispatch is an idempotent success; a racing different transition is a lifecycle_conflict", async () => {
    const ctx = await acceptedBoundary();
    try {
      // Racing identical: the dispatch commits through the real sink and
      // then throws a reducer rejection; the exact verification accepts.
      const racing = {
        get snapshot() {
          return ctx.inner.snapshot;
        },
        get poisoned() {
          return ctx.inner.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.inner.dispatch(command);
          throw new PipelineV2StateError("command rejected for run (racing)");
        },
      };
      const result = (await callTransition(ctx, racing)) as Record<string, unknown>;
      expect(result["to_state"]).toBe("dev_entry");
      expect((ctx.recording.snapshot as PipelineV2RunState).cursor).toEqual({ current_state: "dev_entry", transition_count: 1 });
      // Racing different: a lying presentation with the right execution
      // index but a changed target is a lifecycle_conflict.
      const ctx2 = await acceptedBoundary();
      try {
        let raced = false;
        const lying = {
          get snapshot(): PipelineV2RunState | null {
            const snapshot = ctx2.inner.snapshot;
            if (snapshot === null || !raced) {
              return snapshot;
            }
            const clone = structuredClone(snapshot) as PipelineV2RunState;
            (clone.transitions[clone.transitions.length - 1] as unknown as Record<string, unknown>)["to"] = "planner2";
            (clone.cursor as unknown as Record<string, unknown>)["current_state"] = "planner2";
            return clone;
          },
          get poisoned() {
            return false;
          },
          async dispatch() {
            await ctx2.inner.dispatch({ kind: "transition_committed", step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 }, executionIndex: 1 });
            raced = true;
            throw new PipelineV2StateError("command rejected for run (racing)");
          },
        };
        const cause = await callTransition(ctx2, lying).catch((error) => error);
        const error = expectTransitionError(cause);
        expect(error.reason).toBe("lifecycle_conflict");
        expect(error.message).toBe("the durable initial planning transition does not match the exact step of the initial handoff boundary");
      } finally {
        await rm(ctx2.fixture.root, { recursive: true, force: true });
      }
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("22. a dispatch that resolves without any durable change is an invalid_state", async () => {
    const ctx = await acceptedBoundary();
    try {
      const silent = {
        get snapshot() {
          return ctx.inner.snapshot;
        },
        get poisoned() {
          return ctx.inner.poisoned;
        },
        async dispatch() {
          // Resolves without any durable change.
        },
      };
      const cause = await callTransition(ctx, silent).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.message).toBe("the run state does not carry the committed initial planning transition");
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("23. a hostile post-dispatch snapshot is an invalid_state per mutated field", async () => {
    const mutations: Array<[string, (state: PipelineV2RunState) => void]> = [
        ["revision", (state) => { (state as unknown as Record<string, unknown>)["revision"] = state.revision + 5; }],
        ["schema version", (state) => { (state as unknown as Record<string, unknown>)["schema_version"] = 6; }],
        ["status", (state) => { state.status = "failed"; }],
        ["historical profile", (state) => { (state.executions[0] as unknown as Record<string, unknown>)["profile"] = "other"; }],
        ["input digest", (state) => { (state.inputs[0] as unknown as Record<string, unknown>)["digest"] = "f".repeat(64); }],
        ["cursor", (state) => { (state.cursor as unknown as Record<string, unknown>)["current_state"] = "planner2"; }],
        ["transition outcome", (state) => { (state.transitions[0] as unknown as Record<string, unknown>)["outcome"] = "beta"; }],
        ["extra wait", (state) => { (state as unknown as Record<string, unknown>)["waits"] = [{ index: 1, transition_count: 1, state_id: "architect", reason: "r", request_sha256: "a".repeat(64), actions: [] }]; }],
      ];
      for (const [label, mutate] of mutations) {
        const ctx = await acceptedBoundary();
        try {
          const hostile = mutatedResultSink(ctx.inner, mutate);
          const cause = await callTransition(ctx, hostile).catch((error) => error);
          const error = expectTransitionError(cause);
          expect(error.reason).toBe("invalid_state");
          expect(error.message).toBe("the run state does not carry the committed initial planning transition");
          expect(label.length).toBeGreaterThan(0);
        } finally {
          await rm(ctx.fixture.root, { recursive: true, force: true });
        }
      }
  });

  test("24. caller mutation after the pending dispatch cannot redirect the captured policy", async () => {
    const ctx = await acceptedBoundary();
    try {
      const options = {
        pipeline: ctx.pipeline,
        sink: ctx.recording,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: INITIAL_BUDGET,
      };
      const pending = openPipelineV2InitialStageTransitionInternal(options as never);
      options["stageId"] = "stage-x";
      (options as unknown as Record<string, unknown>)["initialBudget"] = 99;
      const result = (await pending) as unknown as Record<string, unknown>;
      expect(result["stage_id"]).toBe("stage-1");
      expect(result["initial_budget"]).toBe(INITIAL_BUDGET);
    } finally {
      await rm(ctx.fixture.root, { recursive: true, force: true });
    }
  });

  test("25. the export surfaces are exactly the contract", async () => {
    const publicModule = await import("../src/pipeline_v2_initial_stage_transition_controller.ts");
    expect(Object.keys(publicModule).sort()).toEqual([
      "PipelineV2InitialStageTransitionControllerError",
      "openPipelineV2InitialStageTransition",
    ]);
    const internalModule = await import("../src/pipeline_v2_initial_stage_transition_controller_internal.ts");
    expect(Object.keys(internalModule).sort()).toEqual([
      "PipelineV2InitialStageTransitionControllerError",
      "openPipelineV2InitialStageTransitionInternal",
    ]);
  });

  test("26. the controller composes the shared kernel only (source scan)", async () => {
    const { readFile } = await import("node:fs/promises");
    const source = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_initial_stage_transition_controller_internal.ts"), "utf8");
    const countOf = (pattern: string): number => source.split(pattern).length - 1;
    expect(countOf("validatePipelineV2RunState(")).toBe(1);
    expect(countOf("applyStageTransitionCommit(")).toBe(1);
    expect(countOf("const resolved = compiledTransitionFor(")).toBe(1);
    expect(countOf("comparePipelineV2RunIdentity(")).toBe(1);
    expect(countOf("compiledPipelineV2RunPlanStageFor(")).toBe(1);
    expect(countOf("requireResolvedPipelineV2Provenance(")).toBe(1);
    expect(countOf(".message")).toBe(0);
    expect(countOf(".match(")).toBe(0);
    expect(countOf("RegExp(")).toBe(0);
    expect(countOf("JSON.parse")).toBe(0);
    expect(countOf("createHash")).toBe(0);
    expect(countOf("new WeakMap")).toBe(0);
    expect(countOf("new WeakSet")).toBe(0);
    expect(countOf("node:fs")).toBe(0);
    expect(countOf("node:path")).toBe(0);
    expect(countOf("Object.freeze")).toBe(0);
    expect(countOf("let production")).toBe(0);
    for (const banned of [
      "pipeline_v2_coordinator",
      "pipeline_v2_runner",
      "main.ts",
      "cli_",
      "docker",
      "launcher",
      "pipeline_state_store",
      "pipeline_v2_run_plan_store",
      "pipeline_v2_run_plan_candidate.ts",
      "pipeline_v2_run_plan_controller",
      "pipeline_v2_wait_",
      "pipeline_v2_revise_task",
      "pipeline_v2_continue_stage",
      "pipeline_v2_stage_iteration_controller",
      "pipeline_v2_replanned_",
    ]) {
      expect(source).not.toContain(banned);
    }
  });
});
