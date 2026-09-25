import { describe, expect, test } from "bun:test";
import { appendFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "../src/pipeline_v2_run_plan_compiled_internal.ts";
import {
  comparePipelineV2RunIdentity,
  type PipelineV2RunIdentityComparison,
  type PipelineV2RunIdentityField,
} from "../src/pipeline_v2_identity_compare.ts";
import {
  acceptPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_controller.ts";
import {
  ensurePipelineV2StageIteration,
  PipelineV2StageIterationControllerError,
  type PipelineV2StageIterationControllerSink,
} from "../src/pipeline_v2_stage_iteration_controller.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const CANARY = "CANARY_secret_task_body";

const FACTS_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const DISPATCH_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: d_next_stage
  - id: d_test_stage
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
  - id: r_test
    when:
      all:
        - fact: f1
          equals: false
        - fact: f2
          equals: true
    decision: d_test_stage
`;

/** Planning agent -> control dispatcher -> two stages (development, testing). */
const TWO_TEMPLATES_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
    - id: testing
      entry_state: test_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: test_entry
      role: stage
      stage_template: testing

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: dev_entry
      - outcome: d_test_stage
        to: test_entry
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

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

  - id: test_entry
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
  - id: failed
    type: terminal
    result: failed
`;

async function withPipeline(
  fn: (pipeline: ResolvedPipelineV2) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-iteration-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), TWO_TEMPLATES_YAML);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(bundle, "decisions", "dispatch.yaml"), DISPATCH_MODEL_YAML);
    await fn(await loadPipelineV2(bundle));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface RunFixture {
  root: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-iteration-run-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  return { root, stateRoot, runRoot, statePath: join(runRoot, "state.json") };
}

async function disposeRun(fixture: RunFixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 8, 25, 0, 0, clockCounter));
}

const BASE_INPUTS: readonly PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
];

let sessionCounter = 0;

function agentPhases(label: string): PipelineV2RunCommand[] {
  sessionCounter += 1;
  const n = sessionCounter;
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${n}-${label}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${n}-${label}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
    { kind: "agent_cleanup_completed" },
  ];
}

async function playPlanning(
  sink: PipelineV2RunStateSink,
  pipeline: ResolvedPipelineV2,
): Promise<void> {
  const identity = pipelineV2RunPipelineIdentity(pipeline);
  await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: identity, inputs: BASE_INPUTS });
  await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
  for (const command of agentPhases("planning")) {
    await sink.dispatch(command);
  }
}

function taskValue(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = RUN_ID,
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision,
    previous_sha256: previousSha256,
    origin,
    body,
  };
}

function preparedTask(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = RUN_ID,
): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest(taskValue(taskId, revision, previousSha256, origin, body, runId));
}

const A1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");

interface StageSpec {
  readonly id: string;
  readonly template: string;
  readonly tasks: readonly {
    readonly id: string;
    readonly revision: number;
    readonly sha256: string;
    readonly depends_on: readonly string[];
  }[];
}

function preparedPlan(
  stages: readonly StageSpec[],
  overrides: Partial<{ revision: number; previousSha256: string | null; originExecution: number; runId: string }> = {},
): PreparedPipelineV2RunPlanRevision {
  return preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: overrides.runId ?? RUN_ID,
    revision: overrides.revision ?? 1,
    previous_sha256: overrides.previousSha256 === undefined ? null : overrides.previousSha256,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: overrides.originExecution ?? 1,
    stages,
  });
}

function revisionOneCandidate(
  stages: readonly StageSpec[],
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  originExecution = 1,
  runId = RUN_ID,
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidate({
    plan: preparedPlan(stages, { originExecution, runId }),
    taskRevisions,
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

const STAGE_ONE_DEV: readonly StageSpec[] = [
  {
    id: "stage-1",
    template: "development",
    tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
  },
];

const TWO_STAGES: readonly StageSpec[] = [
  {
    id: "stage-1",
    template: "development",
    tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
  },
  {
    id: "stage-2",
    template: "testing",
    tasks: [{ id: "task-b", revision: 1, sha256: preparedTask("task-b", 1, null, "planning_proposal", "Body B one").sha256, depends_on: [] }],
  },
];

const REUSED_TEMPLATE_STAGES: readonly StageSpec[] = [
  {
    id: "stage-1",
    template: "development",
    tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
  },
  {
    id: "stage-2",
    template: "development",
    tasks: [{ id: "task-b", revision: 1, sha256: preparedTask("task-b", 1, null, "planning_proposal", "Body B one").sha256, depends_on: [] }],
  },
];

const B1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");

interface StageCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  compiledPlan: CompiledPipelineV2RunPlan;
  state: PipelineV2RunState;
}

/** A real sink driven to the accepted-plan boundary for the given plan stages. */
async function stageReady(
  stages: readonly StageSpec[],
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  pipeline: ResolvedPipelineV2,
): Promise<StageCtx> {
  const fixture = await setupRun();
  const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
  await playPlanning(sink, pipeline);
  const candidate = revisionOneCandidate(stages, taskRevisions);
  const accepted = await acceptPipelineV2RunPlanCandidate({
    pipeline,
    runRoot: fixture.runRoot,
    sink,
    candidate,
  });
  const state = sink.snapshot as PipelineV2RunState;
  return { fixture, sink, pipeline, compiledPlan: accepted.compiled_plan, state };
}

/** A delegating controller sink that validates every durable snapshot after each command. */
function recordingSink(
  inner: PipelineV2RunStateSink,
  fixture: RunFixture,
  validateEachStep: boolean,
): { sink: PipelineV2StageIterationControllerSink; commands: PipelineV2RunCommand[] } {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    sink: {
      get snapshot() {
        return inner.snapshot;
      },
      get poisoned() {
        return inner.poisoned;
      },
      async dispatch(command: PipelineV2RunCommand) {
        commands.push(command);
        await inner.dispatch(command);
        if (validateEachStep) {
          const raw = await readFile(fixture.statePath, "utf8");
          validatePipelineV2RunState(JSON.parse(raw));
        }
      },
    },
  };
}

function expectControllerError(
  cause: unknown,
  reason: "invalid_options" | "invalid_state" | "lifecycle_conflict" | "state_persist_failed",
): PipelineV2StageIterationControllerError {
  expect(cause).toBeInstanceOf(PipelineV2StageIterationControllerError);
  const error = cause as PipelineV2StageIterationControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

async function catchEnsure(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("expected the stage iteration controller call to reject");
}

describe("pipeline v2 stage iteration controller", () => {
  test("1. W1: an accepted plan opens generation 1 and iteration 1", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const recording = recordingSink(ctx.sink, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: recording.sink,
        });
        expect(recording.commands).toEqual([
          {
            kind: "stage_generation_opened",
            stageId: "stage-1",
            stagePosition: 1,
            templateId: "development",
            planSha256: ctx.compiledPlan.plan_sha256,
            initialBudget: 2,
            transitionCount: 0,
          },
          { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
        ]);
        expect(result.generation_index).toBe(1);
        expect(result.iteration_index).toBe(1);
        expect(result.state).toBe(recording.sink.snapshot as PipelineV2RunState);
        const state = result.state;
        expect(state.generations).toHaveLength(1);
        const generation = state.generations[0];
        expect(generation?.open_iteration).toEqual({ index: 1, opened_transition_count: 0 });
        expect(generation?.closed).toBeUndefined();
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("2. the generation record carries the exact stage id/position/template/plan digest/budget/anchor from the compiled plan", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 3,
          sink: ctx.sink,
        });
        const generation = result.state.generations[0];
        expect(generation).toMatchObject({
          index: 1,
          stage_id: "stage-1",
          stage_position: 1,
          template_id: "development",
          plan_sha256: ctx.compiledPlan.plan_sha256,
          initial_budget: 3,
          opened_transition_count: 0,
          iteration_count: 1,
        });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("3. a second declared stage gets declaration position 2", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(TWO_STAGES, [A1, B1], pipeline);
      try {
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-2",
          initialBudget: 2,
          sink: ctx.sink,
        });
        expect(result.compiled_stage.id).toBe("stage-2");
        expect(result.compiled_stage.template).toBe("testing");
        const generation = result.state.generations[0];
        expect(generation).toMatchObject({ stage_id: "stage-2", stage_position: 2, template_id: "testing" });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("4. a template reused by two stages keeps their stage ids and positions distinct", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(REUSED_TEMPLATE_STAGES, [A1, B1], pipeline);
      try {
        const first = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: ctx.sink,
        });
        expect(first.compiled_stage.template).toBe("development");
        expect(first.state.generations[0]).toMatchObject({ stage_id: "stage-1", stage_position: 1, template_id: "development" });
        // close the iteration and the generation through the reducer, then
        // ensure the second stage of the same reused template
        await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
        await ctx.sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
        const second = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-2",
          initialBudget: 2,
          sink: ctx.sink,
        });
        expect(second.compiled_stage.template).toBe("development");
        expect(second.generation_index).toBe(2);
        expect(second.state.generations[0]).toMatchObject({ stage_id: "stage-1", stage_position: 1, template_id: "development" });
        expect(second.state.generations[1]).toMatchObject({ stage_id: "stage-2", stage_position: 2, template_id: "development" });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("5. W2: an exact durable generation without an iteration is completed by the iteration dispatch only", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // the crash window: the generation committed, the iteration not
        await ctx.sink.dispatch({
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: ctx.compiledPlan.plan_sha256,
          initialBudget: 2,
          transitionCount: 0,
        });
        const recording = recordingSink(ctx.sink, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: recording.sink,
        });
        expect(recording.commands).toEqual([
          { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
        ]);
        expect(result).toMatchObject({ generation_index: 1, iteration_index: 1 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("6. W3: an exact open iteration is an idempotent success with zero dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const first = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: ctx.sink,
        });
        const recording = recordingSink(ctx.sink, ctx.fixture, true);
        const second = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: recording.sink,
        });
        expect(recording.commands).toEqual([]);
        expect(second.generation_index).toBe(first.generation_index);
        expect(second.iteration_index).toBe(1);
        // the exact frozen compiled stage object, not a copy
        expect(second.compiled_stage).toBe(ctx.compiledPlan.stages[0] as never);
        expect(second.state).toBe(ctx.sink.snapshot as PipelineV2RunState);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("7. a closed iteration opens the next index of the same generation", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 3, sink: ctx.sink });
        await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
        const recording = recordingSink(ctx.sink, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 3,
          sink: recording.sink,
        });
        expect(recording.commands).toEqual([
          { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 0 },
        ]);
        expect(result).toMatchObject({ generation_index: 1, iteration_index: 2 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("8. a mismatching initial budget against an existing open generation is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 5, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain('the open stage generation 1 does not match the compiled stage "stage-1"');
        expect(error.state).toBe(ctx.sink.snapshot as PipelineV2RunState);
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("9. an iteration beyond the effective budget is rejected by the reducer pre-check with zero dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 1, sink: ctx.sink });
        await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 1, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain("the current run state does not accept the stage lifecycle sequence");
        expect(error.state).toBe(ctx.sink.snapshot as PipelineV2RunState);
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("10. a compiled plan of a foreign run is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const foreignTask = preparedTask("task-a", 1, null, "planning_proposal", "Body A one", "run-2");
        const foreign = preparePipelineV2RunPlanCandidate({
          plan: preparedPlan(
            [
              {
                id: "stage-1",
                template: "development",
                tasks: [{ id: "task-a", revision: 1, sha256: foreignTask.sha256, depends_on: [] }],
              },
            ],
            { runId: "run-2" },
          ),
          taskRevisions: [foreignTask],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const compiled = compilePipelineV2RunPlanCandidate(pipeline, foreign);
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: compiled, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("the durable run does not belong to the compiled plan's run");
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("11. a compiled plan that is not the last durable plan revision is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // advance the durable plan ledger to revision 2 (linked, same origin)
        const second = preparePipelineV2RunPlanCandidate({
          plan: preparedPlan(STAGE_ONE_DEV, { revision: 2, previousSha256: ctx.compiledPlan.plan_sha256, originExecution: 1 }),
          taskRevisions: [A1],
          previousPlan: preparePlanRevisionManifest({
            schema_version: 1,
            kind: "plan_revision",
            run_id: RUN_ID,
            revision: 1,
            previous_sha256: null,
            root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
            origin_execution: 1,
            stages: STAGE_ONE_DEV,
          }),
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: ctx.fixture.runRoot, sink: ctx.sink, candidate: second });
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("the compiled plan is not the last durable plan revision");
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("12. a compiled plan whose digest does not match the durable last revision is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // a second revision-1 candidate with different plan content
        const other = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          [A1, B1],
        );
        const compiledOther = compilePipelineV2RunPlanCandidate(pipeline, other);
        expect(compiledOther.plan_revision).toBe(1);
        expect(compiledOther.plan_sha256).not.toBe(ctx.compiledPlan.plan_sha256);
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: compiledOther, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("the compiled plan is not the last durable plan revision");
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("13. an open generation for another stage/template/plan is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(TWO_STAGES, [A1, B1], pipeline);
      try {
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-2", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain('the open stage generation 1 does not match the compiled stage "stage-2"');
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("14. waiting, final and in-flight boundaries are invalid_state without any dispatch", async () => {
    await withPipeline(async (pipeline) => {
      for (const variant of ["waiting", "final", "in-flight"] as const) {
        const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
        try {
          if (variant === "waiting") {
            await ctx.sink.dispatch({
              kind: "transition_committed",
              step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
              executionIndex: 1,
            });
            await ctx.sink.dispatch({
              kind: "run_waiting",
              stateId: "stage_dispatch",
              reason: "stage_iteration_limit_exhausted",
              requestSha256: hex("1"),
              actions: [{ id: "revise_task", to: "architect" }],
            });
          } else if (variant === "final") {
            await ctx.sink.dispatch({
              kind: "transition_committed",
              step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
              executionIndex: 1,
            });
            await ctx.sink.dispatch({ kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" });
            await ctx.sink.dispatch({
              kind: "decision_evaluated",
              result: { status: "selected", outcome: "d_plan_complete", decision: "d_plan_complete", rule_id: "R1", active_constraint_ids: [] },
            });
            await ctx.sink.dispatch({
              kind: "transition_committed",
              step: { from: "stage_dispatch", outcome: "d_plan_complete", to: "done", transition_index: 1 },
              executionIndex: 2,
            });
            await ctx.sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
          } else {
            await ctx.sink.dispatch({
              kind: "transition_committed",
              step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
              executionIndex: 1,
            });
            await ctx.sink.dispatch({ kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" });
          }
          const recording = recordingSink(ctx.sink, ctx.fixture, false);
          const cause = await catchEnsure(() =>
            ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
          );
          const error = expectControllerError(cause, "invalid_state");
          expect(error.message).toContain("the run is not on a boundary that accepts a stage generation or iteration");
          expect(recording.commands).toEqual([]);
          expect((ctx.sink.snapshot as PipelineV2RunState).generations).toHaveLength(0);
        } finally {
          await disposeRun(ctx.fixture);
        }
      }
    });
  });

  test("15. an invalid or unknown stage id keeps the compiled-layer error", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const unknownCause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "no-such-stage", initialBudget: 2, sink: ctx.sink }),
        );
        expect(unknownCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        expect((unknownCause as PipelineV2CompiledRunPlanError).reason).toBe("stage_not_found");
        const unsafeCause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "bad id", initialBudget: 2, sink: ctx.sink }),
        );
        expect(unsafeCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        expect((unsafeCause as PipelineV2CompiledRunPlanError).reason).toBe("invalid_stage_id");
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("16. hand-built, spread, cloned and Proxy compiled plans are rejected with zero traps", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const compiled = ctx.compiledPlan;
        const handBuilt = {
          run_id: compiled.run_id,
          plan_revision: compiled.plan_revision,
          plan_sha256: compiled.plan_sha256,
          origin_execution: compiled.origin_execution,
          stages: [
            {
              id: "stage-1",
              template: "development",
              entry_state: "dev_entry",
              state_ids: ["dev_entry"],
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
        } as unknown as CompiledPipelineV2RunPlan;
        const spread = { ...compiled } as unknown as CompiledPipelineV2RunPlan;
        const cloned = JSON.parse(JSON.stringify(compiled)) as CompiledPipelineV2RunPlan;
        let traps = 0;
        const proxy = new Proxy(compiled, {
          get(target, property, receiver) {
            traps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        let dispatchCalls = 0;
        const countingSink: PipelineV2StageIterationControllerSink = {
          get snapshot() {
            return ctx.sink.snapshot;
          },
          get poisoned() {
            return ctx.sink.poisoned;
          },
          dispatch: async (command) => {
            dispatchCalls += 1;
            await ctx.sink.dispatch(command);
          },
        };
        for (const broken of [handBuilt, spread, cloned, proxy]) {
          traps = 0;
          const cause = await catchEnsure(() =>
            ensurePipelineV2StageIteration({ compiledPlan: broken, stageId: "stage-1", initialBudget: 2, sink: countingSink }),
          );
          expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
          expect(traps).toBe(0);
        }
        expect(dispatchCalls).toBe(0);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("17. the compiled-plan provenance gate runs before any snapshot field read", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        let snapshotTraps = 0;
        const proxySnapshot = new Proxy(ctx.sink.snapshot as PipelineV2RunState, {
          get(target, property, receiver) {
            snapshotTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        const proxiedSink: PipelineV2StageIterationControllerSink = {
          get snapshot() {
            return proxySnapshot;
          },
          get poisoned() {
            return ctx.sink.poisoned;
          },
          dispatch: async (command) => {
            await ctx.sink.dispatch(command);
          },
        };
        const fakePlan = { run_id: RUN_ID, plan_revision: 1, plan_sha256: hex("c"), origin_execution: 1, stages: [] } as unknown as CompiledPipelineV2RunPlan;
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: fakePlan, stageId: "stage-1", initialBudget: 2, sink: proxiedSink }),
        );
        expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        expect(snapshotTraps).toBe(0);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("18. a missing or invalid durable snapshot is invalid_state", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const nullSink: PipelineV2StageIterationControllerSink = {
          snapshot: null,
          poisoned: false,
          dispatch: async () => {},
        };
        const nullCause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: nullSink }),
        );
        const nullError = expectControllerError(nullCause, "invalid_state");
        expect(nullError.message).toContain("no durable pipeline v2 run state exists yet");
        const garbage = { schema_version: 6 } as unknown as PipelineV2RunState;
        const garbageSink: PipelineV2StageIterationControllerSink = {
          snapshot: garbage,
          poisoned: false,
          dispatch: async () => {},
        };
        const garbageCause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: garbageSink }),
        );
        const garbageError = expectControllerError(garbageCause, "invalid_state");
        expect(garbageError.message).toContain("requires a durable pipeline v2 run state document");
        expect(garbageError.state).toBeNull();
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("19. a poisoned sink is fail-closed with zero dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        let dispatchCalls = 0;
        const poisonedSink: PipelineV2StageIterationControllerSink = {
          snapshot: ctx.sink.snapshot,
          poisoned: true,
          dispatch: async () => {
            dispatchCalls += 1;
          },
        };
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: poisonedSink }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain("the run state sink is poisoned by a durability-unknown commit");
        expect(dispatchCalls).toBe(0);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("20. not_committed on the generation: the fresh retry dispatches generation and iteration", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // the opened faulted sink continues the durable run; its first
        // dispatch (the generation) fails at the rename
        const faulted = await PipelineV2RunStateSink.open({
          stateRoot: ctx.fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 1, failStep: "rename" }),
        });
        const recording = recordingSink(faulted, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        expectControllerError(cause, "state_persist_failed");
        const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
        expect(persisted.generations).toHaveLength(0);
        // the fresh retry opens the durable run and dispatches the whole sequence
        const fresh = await PipelineV2RunStateSink.open({
          stateRoot: ctx.fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
        });
        const retryRecording = recordingSink(fresh, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: retryRecording.sink,
        });
        expect(retryRecording.commands.map((command) => command.kind)).toEqual([
          "stage_generation_opened",
          "stage_iteration_opened",
        ]);
        expect(result).toMatchObject({ generation_index: 1, iteration_index: 1 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("21. not_committed on the iteration: the generation stays durable; the retry writes only the iteration", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // the opened faulted sink's second dispatch (the iteration) fails
        const faulted = await PipelineV2RunStateSink.open({
          stateRoot: ctx.fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 2, failStep: "rename" }),
        });
        const recording = recordingSink(faulted, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        expectControllerError(cause, "state_persist_failed");
        const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
        expect(persisted.generations).toHaveLength(1);
        expect(persisted.generations[0]?.iteration_count).toBe(0);
        const fresh = await PipelineV2RunStateSink.open({
          stateRoot: ctx.fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
        });
        const retryRecording = recordingSink(fresh, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: retryRecording.sink,
        });
        expect(retryRecording.commands).toEqual([
          { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
        ]);
        expect(result).toMatchObject({ generation_index: 1, iteration_index: 1 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("22. durability_unknown on the generation and on the iteration adopts the candidate and poisons the sink", async () => {
    await withPipeline(async (pipeline) => {
      for (const [label, failCommit] of [["generation", 1], ["iteration", 2]] as const) {
        const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
        try {
          const faulted = await PipelineV2RunStateSink.open({
            stateRoot: ctx.fixture.stateRoot,
            runId: RUN_ID,
            now: nextTick,
            io: faultIo({ failCommit, failStep: "dirfsync" }),
          });
          const recording = recordingSink(faulted, ctx.fixture, false);
          const cause = await catchEnsure(() =>
            ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
          );
          const error = expectControllerError(cause, "state_persist_failed");
          expect(faulted.poisoned).toBe(true);
          const adopted = faulted.snapshot as PipelineV2RunState;
          expect(error.state).toBe(adopted);
          if (label === "generation") {
            expect(adopted.generations).toHaveLength(1);
            expect(adopted.generations[0]?.iteration_count).toBe(0);
            // the iteration dispatch never happened
            expect(recording.commands.map((command) => command.kind)).toEqual(["stage_generation_opened"]);
          } else {
            expect(adopted.generations[0]?.iteration_count).toBe(1);
            expect(recording.commands.map((command) => command.kind)).toEqual([
              "stage_generation_opened",
              "stage_iteration_opened",
            ]);
          }
        } finally {
          await disposeRun(ctx.fixture);
        }
      }
    });
  });

  test("23. a hostile sink that resolves without the expected snapshot change fails closed", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        let dispatchCalls = 0;
        const hostileSink: PipelineV2StageIterationControllerSink = {
          get snapshot() {
            return ctx.sink.snapshot;
          },
          get poisoned() {
            return false;
          },
          dispatch: async () => {
            dispatchCalls += 1;
          },
        };
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: hostileSink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("the committed run state does not carry the expected stage lifecycle record");
        expect(dispatchCalls).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("24. identical concurrency opens exactly one generation and one iteration; both calls succeed", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const settled = await Promise.allSettled([
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink }),
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink }),
        ]);
        for (const entry of settled) {
          expect(entry.status).toBe("fulfilled");
        }
        const state = ctx.sink.snapshot as PipelineV2RunState;
        expect(state.generations).toHaveLength(1);
        expect(state.generations[0]?.iteration_count).toBe(1);
        expect(state.generations[0]?.open_iteration?.index).toBe(1);
        validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("25. conflicting concurrency for different stages: one winner, the other a typed conflict", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(TWO_STAGES, [A1, B1], pipeline);
      try {
        const settled = await Promise.allSettled([
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink }),
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-2", initialBudget: 2, sink: ctx.sink }),
        ]);
        const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
        const rejected = settled.filter((entry) => entry.status === "rejected");
        expect(fulfilled).toHaveLength(1);
        expect(rejected).toHaveLength(1);
        const loser = (rejected[0] as PromiseRejectedResult).reason;
        expectControllerError(loser, "lifecycle_conflict");
        const state = ctx.sink.snapshot as PipelineV2RunState;
        // the winner's generation is the only one, with its own stage identity
        expect(state.generations).toHaveLength(1);
        const winnerStage = (fulfilled[0] as PromiseFulfilledResult<{ compiled_stage: { id: string } }>).value.compiled_stage.id;
        expect(state.generations[0]?.stage_id).toBe(winnerStage);
        expect(state.generations[0]?.iteration_count).toBe(1);
        validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("26. every options field and the sink dispatch are captured exactly once; unexpected getter errors keep their identity", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const reads: string[] = [];
        const hostileOptions = new Proxy(
          { compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink },
          {
            get(target, property, receiver) {
              reads.push(String(property));
              return Reflect.get(target, property, receiver);
            },
          },
        );
        const result = await ensurePipelineV2StageIteration(hostileOptions);
        expect(result.iteration_index).toBe(1);
        expect(reads.sort()).toEqual(["compiledPlan", "initialBudget", "sink", "stageId"]);
        // a sink whose dispatch accessor throws on a second read still succeeds
        const guardFixture = await setupRun();
        try {
          const guardSink = new PipelineV2RunStateSink({ stateRoot: guardFixture.stateRoot, runId: RUN_ID, now: nextTick });
          await playPlanning(guardSink, pipeline);
          const guardAccepted = await acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: guardFixture.runRoot,
            sink: guardSink,
            candidate: revisionOneCandidate(STAGE_ONE_DEV, [A1]),
          });
          let dispatchReads = 0;
          const guardedSink: PipelineV2StageIterationControllerSink = {
            get snapshot() {
              return guardSink.snapshot;
            },
            get poisoned() {
              return guardSink.poisoned;
            },
            get dispatch() {
              dispatchReads += 1;
              if (dispatchReads > 1) {
                throw new Error("the dispatch accessor was read a second time");
              }
              return (command: PipelineV2RunCommand) => guardSink.dispatch(command);
            },
          };
          const guardedResult = await ensurePipelineV2StageIteration({
            compiledPlan: guardAccepted.compiled_plan,
            stageId: "stage-1",
            initialBudget: 2,
            sink: guardedSink,
          });
          expect(guardedResult.iteration_index).toBe(1);
          expect(dispatchReads).toBe(1);
          // unexpected getter errors propagate unchanged
          const injected = new Error("injected snapshot getter failure");
          const brokenSink: PipelineV2StageIterationControllerSink = {
            get snapshot(): PipelineV2RunState {
              throw injected;
            },
            get poisoned() {
              return false;
            },
            dispatch: async () => {},
          };
          const cause = await catchEnsure(() =>
            ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: brokenSink }),
          );
          expect(cause).toBe(injected);
          expect(cause).not.toBeInstanceOf(PipelineV2StageIterationControllerError);
        } finally {
          await disposeRun(guardFixture);
        }
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("27. the result is deep-frozen and caller objects are neither mutated nor frozen", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const options = { compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink };
        const result = await ensurePipelineV2StageIteration(options);
        expect(Object.isFrozen(result)).toBe(true);
        expect(Object.isFrozen(result.compiled_stage)).toBe(true);
        expect(Object.isFrozen(result.state)).toBe(true);
        expect(Object.isFrozen(options)).toBe(false);
        // the compiled stage is the exact frozen object of the compiled plan
        const stage = ctx.compiledPlan.stages[0];
        if (stage === undefined) {
          throw new Error("missing stage");
        }
        expect(result.compiled_stage).toBe(stage);
        expect(result.state).toBe(ctx.sink.snapshot as PipelineV2RunState);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("28. the result and the diagnostics carry no bodies, paths, digest values or credentials", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: ctx.sink,
        });
        const resultText = JSON.stringify(result);
        // the durable state legitimately carries its own content-free ledger
        // records (ids, indexes and digests are the schema v7 record fields);
        // bodies, canonical JSON, paths and credentials never appear
        for (const banned of [CANARY, "canonical_json", ctx.fixture.runRoot, "dht_session_bearer_token", "OPENCODE_CONFIG_CONTENT"]) {
          expect(resultText).not.toContain(banned);
        }
        expect(Object.keys(result).sort()).toEqual(["compiled_stage", "generation_index", "iteration_index", "state"]);
        // the conflict diagnostics name safe ids and indexes only
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 5, sink: ctx.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        for (const banned of [CANARY, ctx.fixture.runRoot, ctx.compiledPlan.plan_sha256, "dht_session_bearer_token"]) {
          expect(error.message).not.toContain(banned);
        }
        expect(Object.keys(error).sort()).toEqual(["name", "reason", "state"]);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("29. the runtime export surface is exactly the two keys", async () => {
    const namespace = (await import("../src/pipeline_v2_stage_iteration_controller.ts")) as Record<string, unknown>;
    expect(Object.keys(namespace).sort()).toEqual([
      "PipelineV2StageIterationControllerError",
      "ensurePipelineV2StageIteration",
    ]);
  });

  test("30. source proof: no forbidden imports, no second authority, no message parsing, no mutable seam", () => {
    const source = readFileSync(
      join(import.meta.dir, "..", "src", "pipeline_v2_stage_iteration_controller.ts"),
      "utf8",
    );
    const importTargets = [...source.matchAll(/from "([^"]+)"/g)].map((match) => match[1] ?? "");
    const allowed = [
      "./pipeline_v2_scalar.ts",
      "./pipeline_v2_state.ts",
      "./pipeline_v2_state_store.ts",
      "./pipeline_v2_run_plan_compiled.ts",
      "./pipeline_v2_run_plan_compiled_internal.ts",
      "./pipeline_v2_identity_compare.ts",
      "./pipeline_v2_freeze_internal.ts",
    ];
    expect(importTargets.length).toBeGreaterThan(0);
    for (const target of importTargets) {
      expect(allowed).toContain(target);
    }
    for (const forbiddenModule of [
      "pipeline_v2_coordinator",
      "pipeline_v2_runner",
      "pipeline_v2_resume_context",
      "pipeline_v2_wait",
      "pipeline_v2_docker",
      "pipeline_v2_runtime",
      "pipeline_v2_digest",
      "pipeline_v2_run_plan_store",
      "pipeline_v2_run_plan_manifests",
      "pipeline_v2_run_plan_bindings",
      "pipeline_v2_run_plan_controller",
      "pipeline_v2_orchestration",
      "pipeline_v2_immutable_document_store_internal",
      "run_snapshot_store",
      "pipeline_state_store",
      "agent_smoke",
      "docker_helper",
      "launcher",
      "main",
      "cli",
      "node:fs",
      "node:path",
    ]) {
      expect(source.includes(`from "${forbiddenModule}.ts"`)).toBe(false);
    }
    // the single validator and stage lookup are used; no second authority
    expect(source).toContain("validatePipelineV2RunState");
    expect(source).toContain("compiledPipelineV2RunPlanStageFor");
    expect(source).toContain("reducePipelineV2RunCommand");
    for (const banned of [
      "compilePipelineV2RunPlanCandidate(",
      "parsePipelineV2RunState(",
      "canonicalJson(",
      "CryptoHasher",
      "createHash",
      "cause.message",
      ".match(",
      "RegExp(",
      "node:fs",
    ]) {
      expect(source.includes(banned)).toBe(false);
    }
    // no mutable module-global seam
    expect(source.includes("let real")).toBe(false);
    expect(source.includes("installOps")).toBe(false);
  });

  test("31. a compiled plan compiled from a foreign pipeline is a lifecycle conflict with zero dispatch", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-foreign-"));
    try {
      const bundle = join(root, "bundle");
      await mkdir(join(bundle, "prompts"), { recursive: true });
      await mkdir(join(bundle, "schemas"), { recursive: true });
      await mkdir(join(bundle, "decisions"), { recursive: true });
      await writeFile(join(bundle, "pipeline.yaml"), TWO_TEMPLATES_YAML);
      await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
      await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
      await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
      await writeFile(join(bundle, "decisions", "dispatch.yaml"), DISPATCH_MODEL_YAML);
      // pipeline B drives the durable run; the same bundle content is then
      // changed and re-loaded as pipeline A: identical bundle root, run id,
      // plan manifest and template ids, different orchestration content and
      // therefore a different execution snapshot digest
      const pipelineB = await loadPipelineV2(bundle);
      await appendFile(join(bundle, "prompts", "architect.md"), "updated guidance\n");
      const pipelineA = await loadPipelineV2(bundle);
      const identityA = pipelineV2RunPipelineIdentity(pipelineA);
      const identityB = pipelineV2RunPipelineIdentity(pipelineB);
      expect(identityA.bundle_root).toBe(identityB.bundle_root);
      expect(identityA.entry_state).toBe(identityB.entry_state);
      expect(identityA.max_transitions).toBe(identityB.max_transitions);
      expect(identityA.execution_snapshot_sha256).not.toBe(identityB.execution_snapshot_sha256);

      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipelineB);
        const candidate = revisionOneCandidate(STAGE_ONE_DEV, [A1]);
        await acceptPipelineV2RunPlanCandidate({ pipeline: pipelineB, runRoot: fixture.runRoot, sink, candidate });
        // the compiled plan of the same candidate, compiled from pipeline A
        const compiledA = compilePipelineV2RunPlanCandidate(pipelineA, candidate);
        const acceptedPlan = sink.snapshot?.plan_revisions[0];
        if (acceptedPlan === undefined) {
          throw new Error("the accepted plan revision is missing");
        }
        expect(compiledA.plan_sha256).toBe(acceptedPlan.sha256);
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: compiledA, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("field execution_snapshot_sha256");
        expect(recording.commands).toEqual([]);
        const persisted = JSON.parse(await readFile(fixture.statePath, "utf8")) as PipelineV2RunState;
        expect(persisted.generations).toHaveLength(0);
      } finally {
        await disposeRun(fixture);
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("32. a content-equal bundle in another directory mismatches only bundle_root", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-bundles-"));
    try {
      const bundleFiles: Record<string, string> = {
        "pipeline.yaml": TWO_TEMPLATES_YAML,
        "prompts/architect.md": "plan the work\n",
        "prompts/coder.md": "implement the task\n",
        "schemas/facts.schema.json": JSON.stringify(FACTS_SCHEMA),
        "decisions/dispatch.yaml": DISPATCH_MODEL_YAML,
      };
      for (const name of ["bundle-b", "bundle-a"]) {
        const bundle = join(root, name);
        for (const [relative, content] of Object.entries(bundleFiles)) {
          const target = join(bundle, relative);
          await mkdir(join(target, ".."), { recursive: true });
          await writeFile(target, content);
        }
      }
      const pipelineB = await loadPipelineV2(join(root, "bundle-b"));
      const pipelineA = await loadPipelineV2(join(root, "bundle-a"));
      const identityA = pipelineV2RunPipelineIdentity(pipelineA);
      const identityB = pipelineV2RunPipelineIdentity(pipelineB);
      expect(identityA.execution_snapshot_sha256).toBe(identityB.execution_snapshot_sha256);
      expect(identityA.entry_state).toBe(identityB.entry_state);
      expect(identityA.max_transitions).toBe(identityB.max_transitions);
      expect(identityA.bundle_root).not.toBe(identityB.bundle_root);

      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipelineB);
        const candidate = revisionOneCandidate(STAGE_ONE_DEV, [A1]);
        await acceptPipelineV2RunPlanCandidate({ pipeline: pipelineB, runRoot: fixture.runRoot, sink, candidate });
        const compiledA = compilePipelineV2RunPlanCandidate(pipelineA, candidate);
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: compiledA, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "lifecycle_conflict");
        expect(error.message).toContain("field bundle_root");
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(fixture);
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("33. each of the five durable identity fields is a mismatch, compared in the fixed field order", () => {
    const identity = (overrides: Partial<PipelineV2RunState["pipeline"]>): PipelineV2RunState["pipeline"] => ({
      schema_version: 2,
      bundle_root: "/opt/orchestrator/pipelines/default",
      execution_snapshot_sha256: hex("a"),
      entry_state: "architect",
      max_transitions: 40,
      ...overrides,
    });
    const base = identity({});
    const cases: readonly (readonly [
      PipelineV2RunIdentityField,
      PipelineV2RunState["pipeline"],
    ])[] = [
      ["schema_version", identity({ schema_version: 3 } as unknown as Partial<PipelineV2RunState["pipeline"]>)],
      ["bundle_root", identity({ bundle_root: "/opt/orchestrator/pipelines/other" })],
      ["execution_snapshot_sha256", identity({ execution_snapshot_sha256: hex("c") })],
      ["entry_state", identity({ entry_state: "stage_dispatch" })],
      ["max_transitions", identity({ max_transitions: 50 })],
    ];
    for (const [field, durable] of cases) {
      const comparison: PipelineV2RunIdentityComparison = comparePipelineV2RunIdentity(base, durable);
      expect(comparison).toEqual({ kind: "mismatch", field });
    }
    expect(comparePipelineV2RunIdentity(base, identity({}))).toEqual({ kind: "match" });
  });

  test("34. the exact originating identity passes; the hidden identity is frozen and equals the pipeline and the durable record", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: ctx.sink,
        });
        expect(result.iteration_index).toBe(1);
        const hidden = compiledRunPlanOriginIdentity(ctx.compiledPlan);
        expect(Object.isFrozen(hidden)).toBe(true);
        expect(hidden).toEqual(pipelineV2RunPipelineIdentity(pipeline));
        expect(hidden).toEqual((ctx.sink.snapshot as PipelineV2RunState).pipeline);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("35. a failed stage execution is not a clean boundary: W3 returns invalid_state with zero dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        // open generation + iteration, run the stage agent to its failure
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        await ctx.sink.dispatch({
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
          executionIndex: 1,
        });
        await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
        for (const command of [
          { kind: "agent_data_prepared" },
          { kind: "agent_execution_session_created", sessionId: "sess-stage-1" },
          { kind: "agent_tool_session_created", sessionId: "tool-stage-1" },
          { kind: "agent_running" },
        ] as PipelineV2RunCommand[]) {
          await ctx.sink.dispatch(command);
        }
        await ctx.sink.dispatch({ kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } });
        const before = ctx.sink.snapshot as PipelineV2RunState;
        const recording = recordingSink(ctx.sink, ctx.fixture, false);
        const cause = await catchEnsure(() =>
          ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: recording.sink }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain("the run is not on a boundary that accepts a stage generation or iteration");
        expect(recording.commands).toEqual([]);
        // the snapshot, the hidden compiled identity and the plan binding are unchanged
        expect(JSON.stringify(ctx.sink.snapshot)).toBe(JSON.stringify(before));
        expect(compiledRunPlanOriginIdentity(ctx.compiledPlan)).toEqual(pipelineV2RunPipelineIdentity(pipeline));
        expect((before.plan_revisions[before.plan_revisions.length - 1])?.sha256).toBe(ctx.compiledPlan.plan_sha256);
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("36. a settled stage execution inside an open iteration is still a clean W3 boundary", async () => {
    await withPipeline(async (pipeline) => {
      const ctx = await stageReady(STAGE_ONE_DEV, [A1], pipeline);
      try {
        await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        await ctx.sink.dispatch({
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
          executionIndex: 1,
        });
        await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
        for (const command of [
          { kind: "agent_data_prepared" },
          { kind: "agent_execution_session_created", sessionId: "sess-stage-1" },
          { kind: "agent_tool_session_created", sessionId: "tool-stage-1" },
          { kind: "agent_running" },
          { kind: "agent_outputs_accepted", outputs: [{ id: "result", digest: hex("5") }] },
          { kind: "agent_cleanup_completed" },
        ] as PipelineV2RunCommand[]) {
          await ctx.sink.dispatch(command);
        }
        const recording = recordingSink(ctx.sink, ctx.fixture, true);
        const result = await ensurePipelineV2StageIteration({
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
          sink: recording.sink,
        });
        expect(recording.commands).toEqual([]);
        expect(result).toMatchObject({ generation_index: 1, iteration_index: 1 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });
});
