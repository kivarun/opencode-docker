import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  acceptPipelineV2ContinueStageIntent,
  PipelineV2ContinueStageIntentControllerError,
} from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../src/pipeline_v2_run_plan_restore.ts";
import {
  openPipelineV2ContinuedStage,
  PipelineV2ContinuedStageControllerError,
  type OpenedPipelineV2ContinuedStage,
} from "../src/pipeline_v2_continued_stage_controller.ts";
import { recordPipelineV2WaitAction } from "../src/pipeline_v2_wait_controller.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { PipelineError } from "../src/pipeline.ts";
import type { PipelineV2ContinueStageIntentManifest } from "../src/pipeline_v2_run_plan_manifests.ts";
import { faultIo } from "./state_io_test_helpers.ts";

function continueManifestOf(intent: PreparedPipelineV2RunWaitIntent): PipelineV2ContinueStageIntentManifest {
  return intent.manifest as PipelineV2ContinueStageIntentManifest;
}

function waitRecordAt(state: PipelineV2RunState, position: number): Record<string, unknown> {
  return state.waits[position] as unknown as Record<string, unknown>;
}

function planRecordAt(state: PipelineV2RunState, position: number): Record<string, unknown> {
  return state.plan_revisions[position] as unknown as Record<string, unknown>;
}

function generationRecordAt(state: PipelineV2RunState, position: number): Record<string, unknown> {
  return state.generations[position] as unknown as Record<string, unknown>;
}
import {
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import {
  applyPipelineV2ContinueStageIntervention,
  PipelineV2ContinueStageInterventionControllerError,
  type AppliedPipelineV2ContinueStageIntervention,
} from "../src/pipeline_v2_continue_stage_intervention_controller.ts";
import {
  applyPipelineV2ContinueStageInterventionWithIo,
  productionContinueStageInterventionOps,
  type PipelineV2ContinueStageInterventionOps,
} from "../src/pipeline_v2_continue_stage_intervention_controller_internal.ts";

const RUN_ID = "intervention-run";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const INITIAL_BUDGET = 2;

const STAGE_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts
    type: json
    protected: false
    schema: schemas/facts.json

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
    - state_id: decide_next
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
        to: decide_next

  - id: decide_next
    type: decision
    model: decisions/next.yaml
    inputs:
      - id: facts
        source: {pipeline_input: facts}
    transitions:
      - outcome: go
        to: architect
      - outcome: stop
        to: architect
      - outcome: uncovered
        to: architect
      - outcome: inconsistent_facts
        to: architect
      - outcome: invalid_facts
        to: architect

  - id: done
    type: terminal
    result: success
`;

const FACTS_SCHEMA = `{"type":"object","required":["f1"],"properties":{"f1":{"type":"boolean"}},"additionalProperties":false}`;

const NEXT_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: go
  - id: stop
relations: []
constraints: []
rules:
  - id: rule-go
    when: {fact: f1, equals: true}
    decision: go
  - id: rule-stop
    when: {fact: f1, equals: false}
    decision: stop
`;

const BASE_INPUTS = [
  { id: "task", type: "file" as const, protected: true, digest: PROTECTED_DIGEST },
  { id: "facts", type: "json" as const, protected: false, digest: hex("c") },
];

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 9, 29, 0, 0, clockCounter));
}

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

const TASK_A1 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-a",
  revision: 1,
  previous_sha256: null,
  origin: "planning_proposal",
  body: "Body A",
});

interface Fixture {
  root: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  inMemoryPlan: CompiledPipelineV2RunPlan;
  plan1: ReturnType<typeof preparePlanRevisionManifest>;
  candidate: PreparedPipelineV2RunPlanCandidate;
  reopened: PipelineV2RunStateSink;
  sink: PipelineV2RunStateSink;
}

interface ProgressOptions {
  acceptIntent?: boolean;
  recordGrant?: { additionalIterations: number };
  closeGrantIteration?: boolean;
  recordResponse?: boolean;
  openNextIteration?: boolean;
}

async function writeBundle(bundle: string): Promise<void> {
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
  await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "decisions", "next.yaml"), NEXT_MODEL_YAML);
  await writeFile(join(bundle, "schemas", "facts.json"), FACTS_SCHEMA);
}

/**
 * The honest prefix through the existing production facades only: real
 * run/project-free input state, real planning execution, real accepted
 * plan, real generation/iteration, real stage execution back to the
 * planning boundary, real wait request and publication, and the prepared
 * provenance-backed continue_stage intent — followed by the simulated
 * restart through the ordinary `PipelineV2RunStateSink.open`.
 */
let sharedPipeline: ResolvedPipelineV2 | null = null;

/**
 * The compiled pipeline is deep-frozen, provenance-registered and never
 * mutated by any test, so the bundle and its Ajv compilation are built
 * once and shared by every fixture; test 19 builds its own foreign
 * bundle.
 */
async function sharedBundlePipeline(): Promise<ResolvedPipelineV2> {
  if (sharedPipeline === null) {
    const bundle = join(tmpdir(), "pipeline-v2-stage-intervention-shared-bundle");
    await rm(bundle, { recursive: true, force: true });
    await writeBundle(bundle);
    sharedPipeline = await loadPipelineV2(bundle);
  }
  return sharedPipeline;
}

async function driveToWaitBoundary(): Promise<Omit<Fixture, "intent" | "reopened"> & { intent1: PreparedPipelineV2RunWaitIntent }> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-intervention-"));
  try {
    const pipeline = await sharedBundlePipeline();
    const stateRoot = join(root, "state-root");
    await mkdir(stateRoot, { mode: 0o700 });
    const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
    const statePath = join(runRoot, "state.json");
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: RUN_ID, now: nextTick });
    await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning")) {
      await sink.dispatch(command);
    }
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: TASK_A1.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [TASK_A1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const acceptedPlan = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot, sink, candidate });
    await ensurePipelineV2StageIteration({ compiledPlan: acceptedPlan.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "decide_next", transition_index: 0 },
      executionIndex: 2,
    });
    await sink.dispatch({
      kind: "start_decision_execution",
      stateId: "decide_next",
      inputDigest: hex("e"),
      executionRole: "stage",
      iterationIndex: 1,
    });
    await sink.dispatch({
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "go", decision: "go", rule_id: "rule-go", active_constraint_ids: [] },
    });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "decide_next", outcome: "go", to: "architect", transition_index: 0 },
      executionIndex: 3,
    });
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 3,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await sink.dispatch({
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
    const intent1 = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: "stage-1",
      expected_plan_sha256: acceptedPlan.compiled_plan.plan_sha256,
      additional_iterations: 2,
    });
    return { root, stateRoot, runRoot, statePath, pipeline, intent1, inMemoryPlan: acceptedPlan.compiled_plan, plan1, candidate, sink } as unknown as Omit<Fixture, "intent" | "reopened"> & { intent1: PreparedPipelineV2RunWaitIntent };
  } catch (cause) {
    await rm(root, { recursive: true, force: true });
    throw cause;
  }
}

async function driveToReopenedWait(options: ProgressOptions = {}): Promise<Fixture> {
  const ctx = await driveToWaitBoundary();
  const { runRoot, sink } = ctx;
  const intent = ctx.intent1;
  try {
    if (options.acceptIntent === true || options.recordGrant !== undefined) {
      await acceptPipelineV2ContinueStageIntent({ runRoot, sink, intent });
    }
    if (options.recordGrant !== undefined) {
      await sink.dispatch({
        kind: "iteration_grant_recorded",
        generationIndex: 1,
        waitIndex: 1,
        intentSha256: intent.sha256,
        additionalIterations: options.recordGrant.additionalIterations,
      });
    }
    if (options.closeGrantIteration === true) {
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 });
    }
    if (options.recordResponse === true) {
      await recordPipelineV2WaitAction({ runRoot, sink, waitIndex: 1, actionId: "continue_stage" });
    }
    if (options.openNextIteration === true) {
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.inMemoryPlan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink });
    }
    // The simulated process restart: the run is reopened through the
    // ordinary sink open; nothing in memory survives it.
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.stateRoot, runId: RUN_ID, now: nextTick });
    return { root: ctx.root, stateRoot: ctx.stateRoot, runRoot, statePath: ctx.statePath, pipeline: ctx.pipeline, intent, inMemoryPlan: ctx.inMemoryPlan, plan1: ctx.plan1, candidate: ctx.candidate, reopened, sink };
  } catch (cause) {
    await rm(ctx.root, { recursive: true, force: true });
    throw cause;
  }
}

/**
 * The two-cycle prefix for the healing and matrix tests: the first wait
 * is answered by a real first intervention through the public facade,
 * the stage work continues through the generation rollover (generation 1
 * closed `next_stage`, generation 2 opened with iteration 1) and the
 * second wait opens at the exhausted second-cycle decision boundary —
 * so the intervention under test operates on wait 2 with the historical
 * wait 1, the historical generation 1 (with its grant-closed iterations)
 * and the live generation 2 all durable.
 */
async function driveToSecondWait(): Promise<Fixture> {
  const ctx = await driveToWaitBoundary();
  const { runRoot, sink } = ctx;
  try {
    await applyPipelineV2ContinueStageIntervention({
      pipeline: ctx.pipeline,
      runRoot,
      sink,
      intent: ctx.intent1,
      initialBudget: INITIAL_BUDGET,
    });
    // The second cycle inside the granted iteration 2: the settled
    // decision execution closes the iteration and the generation (the
    // contract hook order), the transition moves the cursor back to the
    // stage entry, the new generation and its first iteration open at the
    // same boundary, and the second stage cycle runs inside it.
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 2 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "decide_next", transition_index: 0 },
      executionIndex: 4,
    });
    await sink.dispatch({
      kind: "start_decision_execution",
      stateId: "decide_next",
      inputDigest: hex("e"),
      executionRole: "stage",
      iterationIndex: 2,
    });
    await sink.dispatch({
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "go", decision: "go", rule_id: "rule-go", active_constraint_ids: [] },
    });
    await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 2, by: "normal_close" });
    await sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "decide_next", outcome: "go", to: "dev_entry", transition_index: 0 },
      executionIndex: 5,
    });
    await ensurePipelineV2StageIteration({ compiledPlan: ctx.inMemoryPlan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "decide_next", transition_index: 0 },
      executionIndex: 6,
    });
    await sink.dispatch({
      kind: "start_decision_execution",
      stateId: "decide_next",
      inputDigest: hex("e"),
      executionRole: "stage",
      iterationIndex: 1,
    });
    await sink.dispatch({
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "go", decision: "go", rule_id: "rule-go", active_constraint_ids: [] },
    });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "decide_next", outcome: "go", to: "architect", transition_index: 0 },
      executionIndex: 7,
    });
    const request2 = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 2,
      transition_count: 7,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request2.sha256,
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await publishPipelineV2WaitRequest(runRoot, request2.manifest);
    const intent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 2,
      stage_id: "stage-1",
      expected_plan_sha256: ctx.inMemoryPlan.plan_sha256,
      additional_iterations: 2,
    });
    // The simulated process restart: the run is reopened through the
    // ordinary sink open; nothing in memory survives it.
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.stateRoot, runId: RUN_ID, now: nextTick });
    return { root: ctx.root, stateRoot: ctx.stateRoot, runRoot, statePath: ctx.statePath, pipeline: ctx.pipeline, intent, inMemoryPlan: ctx.inMemoryPlan, plan1: ctx.plan1, candidate: ctx.candidate, reopened, sink };
  } catch (cause) {
    await rm(ctx.root, { recursive: true, force: true });
    throw cause;
  }
}

interface RecordingSink {
  commands: PipelineV2RunCommand[];
}

function recordSink(inner: PipelineV2RunStateSink): RecordingSink & {
  snapshot: PipelineV2RunState | null;
  poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
} {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    get snapshot() {
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      await inner.dispatch(command);
      commands.push(command);
    },
  };
}

test("1. the proof: the existing three-facade chain drives the reopened run through the full five-command suffix", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    // 1. the intent acceptance through the existing facade
    const accepted = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.runRoot, sink: recording, intent: ctx.intent });
    expect(accepted.wait_index).toBe(1);
    expect(accepted.intent_sha256).toBe(ctx.intent.sha256);
    // 2. the restore through the existing facade, from the authoritative
    // state after the acceptance; the in-memory compiled plan is never
    // consulted again from here on
    const restored = await restorePipelineV2AcceptedRunPlan({ pipeline: ctx.pipeline, runRoot: ctx.runRoot, state: accepted.state });
    expect(restored.compiled_plan).toEqual(ctx.inMemoryPlan);
    expect(restored.compiled_plan).not.toBe(ctx.inMemoryPlan);
    // 3. the composition through the existing facade
    const opened = await openPipelineV2ContinuedStage({
      runRoot: ctx.runRoot,
      sink: recording,
      intent: ctx.intent,
      compiledPlan: restored.compiled_plan,
      initialBudget: INITIAL_BUDGET,
    });
    expect(recording.commands.map((command) => command.kind)).toEqual([
      "plan_intent_accepted",
      "iteration_grant_recorded",
      "stage_iteration_closed",
      "wait_response_recorded",
      "stage_iteration_opened",
    ]);
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 5);
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
    expect(state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 3 });
    expect(state.waits[0]?.intent).toEqual({ intent_sha256: ctx.intent.sha256 });
    expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
    expect(state.grants).toEqual([
      { index: 1, generation_index: 1, wait_index: 1, intent_sha256: ctx.intent.sha256, additional_iterations: 2 },
    ]);
    expect(opened.wait_index).toBe(1);
    expect(opened.additional_iterations).toBe(2);
    expect(opened.action_id).toBe("continue_stage");
    expect(opened.action_to).toBe("dev_entry");
    expect(opened.closed_iteration_index).toBe(1);
    expect(opened.iteration_index).toBe(2);
    expect(opened.generation_index).toBe(1);
    // the loader accepts the composed durable document
    const raw = await readFile(ctx.statePath, "utf8");
    expect(parsePipelineV2RunState(raw)).toEqual(state);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- the intervention through the new controller -----------------------------

async function callIntervention(
  ctx: Fixture,
  sink: unknown,
  initialBudget: number = INITIAL_BUDGET,
): Promise<AppliedPipelineV2ContinueStageIntervention> {
  return await applyPipelineV2ContinueStageIntervention({
    pipeline: ctx.pipeline,
    runRoot: ctx.runRoot,
    sink: sink as never,
    intent: ctx.intent,
    initialBudget,
  });
}

async function catchApply(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the intervention call was expected to fail");
}

function expectApplyError(cause: unknown): PipelineV2ContinueStageInterventionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinueStageInterventionControllerError);
  return cause as PipelineV2ContinueStageInterventionControllerError;
}

function resultFields(
  result: AppliedPipelineV2ContinueStageIntervention | OpenedPipelineV2ContinuedStage,
): Record<string, unknown> {
  const fields: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(result)) {
    if (key !== "state" && key !== "compiled_stage") {
      fields[key] = value;
    }
  }
  return fields;
}

const SUFFIX = [
  "plan_intent_accepted",
  "iteration_grant_recorded",
  "stage_iteration_closed",
  "wait_response_recorded",
  "stage_iteration_opened",
] as const;

function assertIntervenedBoundary(state: PipelineV2RunState, intent: PreparedPipelineV2RunWaitIntent): void {
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
  expect(state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 3 });
  expect(state.generations[0]?.iterations[0]?.closed).toEqual({ by: "grant", wait_index: 1, closed_transition_count: 3 });
  expect(state.waits[0]?.intent).toEqual({ intent_sha256: intent.sha256 });
  expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(state.grants).toEqual([
    { index: 1, generation_index: 1, wait_index: 1, intent_sha256: intent.sha256, additional_iterations: 2 },
  ]);
}

test("2. the C0 intervention composes the exact five-command suffix with revision +5 and never uses an in-memory plan", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands.map((command) => command.kind)).toEqual([...SUFFIX]);
    expect(recording.commands[0]).toMatchObject({
      kind: "plan_intent_accepted",
      waitIndex: 1,
      intentSha256: ctx.intent.sha256,
    });
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 5);
    expect(result).toMatchObject({
      wait_index: 1,
      intent_sha256: ctx.intent.sha256,
      additional_iterations: 2,
      action_id: "continue_stage",
      action_to: "dev_entry",
      closed_iteration_index: 1,
      iteration_index: 2,
      generation_index: 1,
    });
    expect(typeof result.request_sha256).toBe("string");
    expect(typeof result.response_sha256).toBe("string");
    assertIntervenedBoundary(result.state, ctx.intent);
    // the loader accepts the composed durable document
    const raw = await readFile(ctx.statePath, "utf8");
    expect(parsePipelineV2RunState(raw)).toEqual(result.state);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("3. the intervention result is flat, deep-frozen, content-free and carries the restored compiled stage", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const result = await callIntervention(ctx, recordSink(ctx.reopened));
    expect(Object.keys(result).sort()).toEqual([
      "action_id",
      "action_to",
      "additional_iterations",
      "closed_iteration_index",
      "compiled_stage",
      "generation_index",
      "intent_sha256",
      "iteration_index",
      "request_sha256",
      "response_sha256",
      "state",
      "wait_index",
    ]);
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.state)).toBe(true);
    expect(Object.isFrozen(result.compiled_stage)).toBe(true);
    // the restored compiled stage is the stage of the plan content the
    // restore rebuilt from the durable ledger, not the in-memory object
    const inMemoryStage = compiledPipelineV2RunPlanStageFor(ctx.inMemoryPlan, "stage-1");
    expect(result.compiled_stage).not.toBe(inMemoryStage);
    expect(result.compiled_stage).toEqual(inMemoryStage);
    expect(result.compiled_stage.entry_state).toBe("dev_entry");
    // content-free: no canonical JSON, no run-root paths, no intent bodies
    const serialized = JSON.stringify(result);
    expect(serialized).not.toContain("canonical_json");
    expect(serialized).not.toContain(ctx.runRoot);
    expect(serialized).not.toContain("Body A");
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("4. the C1 window: the exact durable intent skips re-acceptance and the remaining four commands run", async () => {
  const ctx = await driveToReopenedWait({ acceptIntent: true });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands.map((command) => command.kind)).toEqual(SUFFIX.slice(1));
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 4);
    expect(result.closed_iteration_index).toBe(1);
    expect(result.iteration_index).toBe(2);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("5. the C2 window: the exact durable grant skips to closure, response and open", async () => {
  const ctx = await driveToReopenedWait({ recordGrant: { additionalIterations: 2 } });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands.map((command) => command.kind)).toEqual(SUFFIX.slice(2));
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
    expect(result.iteration_index).toBe(2);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("6. the C3 window: the exact grant closure is the progressed retry and runs response and open", async () => {
  const ctx = await driveToReopenedWait({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands.map((command) => command.kind)).toEqual(SUFFIX.slice(3));
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
    expect(result.iteration_index).toBe(2);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("7. the C4 window: the exact durable response is the progressed retry and only the successor opens", async () => {
  const ctx = await driveToReopenedWait({
    recordGrant: { additionalIterations: 2 },
    closeGrantIteration: true,
    recordResponse: true,
  });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands.map((command) => command.kind)).toEqual(SUFFIX.slice(4));
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
    expect(result.iteration_index).toBe(2);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("8. the C5 window: the exact open successor iteration is the zero-dispatch recognition", async () => {
  const ctx = await driveToReopenedWait({
    recordGrant: { additionalIterations: 2 },
    closeGrantIteration: true,
    recordResponse: true,
    openNextIteration: true,
  });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const result = await callIntervention(ctx, recording);
    expect(recording.commands).toEqual([]);
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
    expect(result.iteration_index).toBe(2);
    expect(result.state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 3 });
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- the narrow progressed-retry classification -----------------------------

interface CountingInterventionOps extends PipelineV2ContinueStageInterventionOps {
  restoreCalls: () => number;
  openCalls: () => number;
}

function countingDownstreamOps(): CountingInterventionOps {
  let restoreCalls = 0;
  let openCalls = 0;
  return {
    acceptIntent: async () => {
      throw new Error("the injected acceptance must be configured");
    },
    restoreAcceptedPlan: async () => {
      restoreCalls += 1;
      throw new Error("the restore must not be called");
    },
    openContinuedStage: async () => {
      openCalls += 1;
      throw new Error("the composition must not be called");
    },
    restoreCalls: () => restoreCalls,
    openCalls: () => openCalls,
  } as unknown as CountingInterventionOps;
}

function injectAcceptanceError(error: unknown): PipelineV2ContinueStageInterventionOps {
  return {
    acceptIntent: async () => {
      throw error;
    },
    restoreAcceptedPlan: productionContinueStageInterventionOps.restoreAcceptedPlan,
    openContinuedStage: productionContinueStageInterventionOps.openContinuedStage,
  } as unknown as PipelineV2ContinueStageInterventionOps;
}

test("9. an unrecognized acceptance error is re-thrown by object identity with zero downstream calls", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const ops = countingDownstreamOps();
    // a wrong failure class
    const plain = new Error("UNRECOGNIZED-PLAIN");
    const plainCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(plain), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(plainCause).toBe(plain);
    // a wrong reason of the right class
    const conflict = new PipelineV2ContinueStageIntentControllerError(
      "intent_conflict",
      "a different intent",
      ctx.reopened.snapshot,
    );
    const conflictCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(conflict), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(conflictCause).toBe(conflict);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("10. an invalid_state without an authoritative state is never a progressed retry", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const ops = countingDownstreamOps();
    const injected = new PipelineV2ContinueStageIntentControllerError(
      "invalid_state",
      "the durable run state is missing or not a valid pipeline v2 run state",
      null,
    );
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(injected), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(cause).toBe(injected);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("11. an intent that is absent or different on the target wait is never a progressed retry", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const ops = countingDownstreamOps();
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    // the intent-acceptance boundary with no durable intent: a broken
    // boundary, not a progression
    const withoutIntent = structuredClone(state) as PipelineV2RunState;
    delete waitRecordAt(withoutIntent, 0)["intent"];
    const noIntent = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", withoutIntent);
    const noIntentCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(noIntent), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(noIntentCause).toBe(noIntent);
    // a different accepted digest on the same wait
    const foreignIntentState = structuredClone(state) as PipelineV2RunState;
    waitRecordAt(foreignIntentState, 0)["intent"] = { intent_sha256: hex("1") };
    const foreignIntent = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", foreignIntentState);
    const foreignCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(foreignIntent), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(foreignCause).toBe(foreignIntent);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("12. a historical intent (the target wait not the last record) is never a progressed retry", async () => {
  const ctx = await driveToReopenedWait({ acceptIntent: true });
  try {
    const ops = countingDownstreamOps();
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    // a newer wait opened after the accepted one: the accepted intent
    // exists, but only somewhere in the wait history
    const newerWaitState = structuredClone(state) as PipelineV2RunState;
    newerWaitState.waits.push({
      index: 2,
      transition_count: 3,
      state_id: "dev_entry",
      reason: "stage_iteration_limit_exhausted",
      request_sha256: hex("2"),
      actions: [{ id: "continue_stage", to: "dev_entry" }],
    });
    const injected = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", newerWaitState);
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(injected), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(cause).toBe(injected);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("13. a boundary answered with another action is never a continue-stage retry", async () => {
  const ctx = await driveToReopenedWait({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
  try {
    const ops = countingDownstreamOps();
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    const revisedState = structuredClone(state) as PipelineV2RunState;
    waitRecordAt(revisedState, 0)["response"] = { action_id: "revise_task", response_sha256: hex("f") };
    const injected = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", revisedState);
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(injected), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(cause).toBe(injected);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("14. a state after an already started successor execution is never an intervention retry", async () => {
  const ctx = await driveToReopenedWait({
    recordGrant: { additionalIterations: 2 },
    closeGrantIteration: true,
    recordResponse: true,
  });
  try {
    const ops = countingDownstreamOps();
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    // a successor execution started after the response (in flight)
    const startedState = structuredClone(state) as PipelineV2RunState;
    startedState.executions.push({
      index: 4,
      type: "agent",
      state_id: "dev_entry",
      execution_role: "stage",
      iteration_index: 2,
      attempt: 1,
      profile: "coder",
      phase: "running",
      execution_session_id: "sess-3",
      tool_session_id: "tool-3",
    });
    const started = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", startedState);
    const startedCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(started), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(startedCause).toBe(started);
    // a committed transition after the response
    const transitionedState = structuredClone(state) as PipelineV2RunState;
    transitionedState.transitions.push({ index: 3, from: "dev_entry", outcome: "completed", to: "architect", execution_index: 4 });
    const transitioned = new PipelineV2ContinueStageIntentControllerError("invalid_state", "broken", transitionedState);
    const transitionedCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(injectAcceptanceError(transitioned), {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(transitionedCause).toBe(transitioned);
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- hostile successful results ---------------------------------------------

function mutatedAcceptanceOps(
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
): CountingInterventionOps {
  let restoreCalls = 0;
  let openCalls = 0;
  return {
    acceptIntent: async (args: unknown) => {
      const real = await acceptPipelineV2ContinueStageIntent(args as never);
      const clone = structuredClone(real) as unknown as Record<string, unknown>;
      mutate(clone, clone["state"] as PipelineV2RunState);
      return clone as never;
    },
    restoreAcceptedPlan: async () => {
      restoreCalls += 1;
      throw new Error("the restore must not be called");
    },
    openContinuedStage: async () => {
      openCalls += 1;
      throw new Error("the composition must not be called");
    },
    restoreCalls: () => restoreCalls,
    openCalls: () => openCalls,
  } as unknown as CountingInterventionOps;
}

/**
 * The real acceptance beside the real restore whose successful result is
 * mutated before it reaches the intervention verification; the shallow
 * result copy preserves the exact `compiled_plan` object identity unless
 * the test replaces it deliberately. The composition counts calls and
 * never runs on a rejected restore result.
 */
function mutatedRestoreOps(
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
  replacePlan = false,
): CountingInterventionOps {
  let openCalls = 0;
  return {
    acceptIntent: acceptPipelineV2ContinueStageIntent,
    restoreAcceptedPlan: async (args: unknown) => {
      const real = await restorePipelineV2AcceptedRunPlan(args as never);
      const clone: Record<string, unknown> = {
        ...real,
        state: structuredClone(real.state),
        compiled_plan: replacePlan ? structuredClone(real.compiled_plan) : real.compiled_plan,
      };
      mutate(clone, clone["state"] as PipelineV2RunState);
      return clone as never;
    },
    openContinuedStage: async () => {
      openCalls += 1;
      throw new Error("the composition must not be called");
    },
    restoreCalls: () => 0,
    openCalls: () => openCalls,
  } as unknown as CountingInterventionOps;
}

/**
 * The real acceptance and restore beside the real composition whose
 * successful result is mutated before it reaches the intervention
 * verification; the shallow result copy preserves the exact
 * `compiled_stage` object identity.
 */
function mutatedOpenOps(
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
): PipelineV2ContinueStageInterventionOps {
  return {
    acceptIntent: acceptPipelineV2ContinueStageIntent,
    restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
    openContinuedStage: async (args: unknown) => {
      const real = await openPipelineV2ContinuedStage(args as never);
      const clone = { ...real, state: structuredClone(real.state) } as unknown as Record<string, unknown>;
      mutate(clone, clone["state"] as PipelineV2RunState);
      return clone as never;
    },
  } as unknown as PipelineV2ContinueStageInterventionOps;
}

function injectingOps(
  stage: "acceptance" | "restore" | "open",
  value: unknown,
): CountingInterventionOps {
  let restoreCalls = 0;
  let openCalls = 0;
  const base = {
    acceptIntent: acceptPipelineV2ContinueStageIntent,
    restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
    openContinuedStage: openPipelineV2ContinuedStage,
  };
  const ops: Record<string, unknown> = { ...base };
  if (stage === "acceptance") {
    ops["acceptIntent"] = async () => value;
  } else if (stage === "restore") {
    ops["acceptIntent"] = acceptPipelineV2ContinueStageIntent;
    ops["restoreAcceptedPlan"] = async () => value;
    ops["openContinuedStage"] = async () => {
      openCalls += 1;
      throw new Error("the composition must not be called");
    };
  } else {
    ops["openContinuedStage"] = async () => value;
  }
  return { ...(ops as unknown as PipelineV2ContinueStageInterventionOps), restoreCalls: () => restoreCalls, openCalls: () => openCalls } as unknown as CountingInterventionOps;
}

test("15. a mutated successful acceptance result is invalid_result before the restore", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const mutations: Array<[string, (result: Record<string, unknown>, state: PipelineV2RunState) => void]> = [
      ["wait_index", (result) => { result["wait_index"] = 7; }],
      ["intent_sha256", (result) => { result["intent_sha256"] = hex("1"); }],
      ["run_id", (_result, state) => { state.run_id = "other-run"; }],
      ["status", (_result, state) => { state.status = "active"; }],
      ["accepted digest", (_result, state) => { waitRecordAt(state, 0)["intent"] = { intent_sha256: hex("2") }; }],
      ["injected response", (_result, state) => { waitRecordAt(state, 0)["response"] = { action_id: "continue_stage", response_sha256: hex("f") }; }],
      ["dropped wait", (_result, state) => { state.waits = []; }],
    ];
    for (const [name, mutate] of mutations) {
      const ops = mutatedAcceptanceOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason, name).toBe("invalid_result");
      expect(cause, name).not.toBeInstanceOf(TypeError);
      expect(error.message, name).not.toContain("HOSTILE");
      expect(ops.restoreCalls(), name).toBe(0);
      expect(ops.openCalls(), name).toBe(0);
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("16. malformed acceptance result shapes are invalid_result, never a TypeError", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const shapes: unknown[] = [
      null,
      undefined,
      42,
      "HOSTILE-ACCEPTANCE",
      [],
      {},
      { wait_index: 1 },
      { wait_index: 1, intent_sha256: ctx.intent.sha256 },
      { wait_index: 1, intent_sha256: ctx.intent.sha256, state: null },
      { wait_index: 1, intent_sha256: ctx.intent.sha256, state: "HOSTILE-STATE" },
      { wait_index: 1, intent_sha256: ctx.intent.sha256, state: { status: "waiting", waits: "HOSTILE-WAITS" } },
    ];
    for (const shape of shapes) {
      const ops = injectingOps("acceptance", shape);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
      expect(ops.restoreCalls()).toBe(0);
      expect(ops.openCalls()).toBe(0);
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("17. a mutated successful restore result is invalid_result before the composition", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const stateMutations: Array<[string, (result: Record<string, unknown>, state: PipelineV2RunState) => void]> = [
      ["state revision", (_result, state) => { state.revision = state.revision + 1; }],
      ["state status", (_result, state) => { state.status = "active"; }],
      ["state cursor", (_result, state) => { state.cursor = { current_state: "nowhere", transition_count: 3 }; }],
      ["state plan ledger", (_result, state) => { planRecordAt(state, 0)["sha256"] = hex("3"); }],
      ["state wait intent", (_result, state) => { waitRecordAt(state, 0)["intent"] = { intent_sha256: hex("4") }; }],
      ["dropped generations", (_result, state) => { state.generations = []; }],
    ];
    for (const [name, mutate] of stateMutations) {
      const ops = mutatedRestoreOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason, name).toBe("invalid_result");
      expect(cause, name).not.toBeInstanceOf(TypeError);
      expect(error.message, name).not.toContain("HOSTILE");
      expect(ops.openCalls(), name).toBe(0);
    }
    const planMutations: Array<[string, (result: Record<string, unknown>, state: PipelineV2RunState) => void]> = [
      ["plan digest", (result) => { (result["compiled_plan"] as Record<string, unknown>)["plan_sha256"] = hex("1"); }],
      ["plan revision", (result) => { (result["compiled_plan"] as Record<string, unknown>)["plan_revision"] = 9; }],
      ["plan origin", (result) => { (result["compiled_plan"] as Record<string, unknown>)["origin_execution"] = 9; }],
      ["plan run id", (result) => { (result["compiled_plan"] as Record<string, unknown>)["run_id"] = "other-run"; }],
    ];
    for (const [name, mutate] of planMutations) {
      const ops = mutatedRestoreOps(mutate, true);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason, name).toBe("invalid_result");
      expect(cause, name).not.toBeInstanceOf(TypeError);
      expect(error.message, name).not.toContain("HOSTILE");
      expect(ops.openCalls(), name).toBe(0);
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("18. a forged cloned compiled plan in the restore result is invalid_result", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const ops = mutatedRestoreOps(() => {}, true);
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const error = expectApplyError(cause);
    expect(error.reason).toBe("invalid_result");
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("19. a foreign-pipeline compiled plan in the restore result is invalid_result", async () => {
  const ctx = await driveToReopenedWait();
  try {
    // the same bundle content loaded from another directory: a different
    // durable pipeline identity
    const secondRoot = await mkdtemp(join(tmpdir(), "pipeline-v2-stage-intervention-foreign-"));
    try {
      const secondBundle = join(secondRoot, "bundle");
      await writeBundle(secondBundle);
      const foreignPipeline = await loadPipelineV2(secondBundle);
      const foreignPlan = compilePipelineV2RunPlanCandidate(foreignPipeline, ctx.candidate);
      const ops = {
        acceptIntent: acceptPipelineV2ContinueStageIntent,
        restoreAcceptedPlan: async (args: unknown) => {
          const real = await restorePipelineV2AcceptedRunPlan(args as never);
          return { ...real, compiled_plan: foreignPlan } as never;
        },
        openContinuedStage: async () => {
          throw new Error("the composition must not be called");
        },
      } as unknown as PipelineV2ContinueStageInterventionOps;
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await rm(secondRoot, { recursive: true, force: true });
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("20. a mutated successful open result is invalid_result", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const mutations: Array<[string, (result: Record<string, unknown>, state: PipelineV2RunState) => void]> = [
      ["wait_index", (result) => { result["wait_index"] = 7; }],
      ["intent_sha256", (result) => { result["intent_sha256"] = hex("1"); }],
      ["additional_iterations", (result) => { result["additional_iterations"] = 9; }],
      ["request digest", (result) => { result["request_sha256"] = hex("2"); }],
      ["action id", (result) => { result["action_id"] = "revise_task"; }],
      ["action target", (result) => { result["action_to"] = "architect"; }],
      ["closed index", (result) => { result["closed_iteration_index"] = 2; }],
      ["open index", (result) => { result["iteration_index"] = 3; }],
      ["generation index", (result) => { result["generation_index"] = 2; }],
      ["compiled stage clone", (result) => { result["compiled_stage"] = structuredClone(result["compiled_stage"]); }],
      ["state status", (_result, state) => { state.status = "waiting"; }],
      ["state response", (_result, state) => { waitRecordAt(state, 0)["response"] = { action_id: "continue_stage", response_sha256: hex("3") }; }],
      ["state cursor", (_result, state) => { state.cursor = { current_state: "architect", transition_count: 3 }; }],
      ["state grant", (_result, state) => { state.grants = []; }],
      ["state generation budget", (_result, state) => { generationRecordAt(state, 0)["initial_budget"] = 9; }],
      ["state generation closed", (_result, state) => { generationRecordAt(state, 0)["closed"] = { by: "next_stage", closed_transition_count: 3 }; }],
    ];
    for (const [name, mutate] of mutations) {
      const ops = mutatedOpenOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason, name).toBe("invalid_result");
      expect(cause, name).not.toBeInstanceOf(TypeError);
      expect(error.message, name).not.toContain("HOSTILE");
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("21. malformed open result shapes are invalid_result, never a TypeError", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const shapes: unknown[] = [
      null,
      undefined,
      42,
      "HOSTILE-OPEN",
      [],
      {},
      { wait_index: 1 },
      {
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
        request_sha256: hex("e"),
        response_sha256: hex("f"),
        additional_iterations: 2,
        action_id: "continue_stage",
        action_to: "dev_entry",
        closed_iteration_index: 1,
        iteration_index: 2,
        generation_index: 1,
        compiled_stage: "HOSTILE-STAGE",
      },
    ];
    for (const shape of shapes) {
      const ops = injectingOps("open", shape);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- healing red-before -----------------------------------------------------

/**
 * The real acceptance beside the real downstream layers whose calls are
 * counted: the successful acceptance result is shallow-copied with a
 * cloned state and mutated before it reaches the intervention
 * verification.
 */
function healedAcceptanceOps(
  mutate: (state: PipelineV2RunState) => void,
): CountingInterventionOps {
  let restoreCalls = 0;
  let openCalls = 0;
  return {
    acceptIntent: async (args: unknown) => {
      const real = await acceptPipelineV2ContinueStageIntent(args as never);
      const clone = { ...real, state: structuredClone(real.state) } as unknown as Record<string, unknown>;
      mutate(clone["state"] as PipelineV2RunState);
      return clone as never;
    },
    restoreAcceptedPlan: async (args: unknown) => {
      restoreCalls += 1;
      return await restorePipelineV2AcceptedRunPlan(args as never);
    },
    openContinuedStage: async (args: unknown) => {
      openCalls += 1;
      return await openPipelineV2ContinuedStage(args as never);
    },
    restoreCalls: () => restoreCalls,
    openCalls: () => openCalls,
  } as unknown as CountingInterventionOps;
}

/**
 * The real acceptance and restore; the successful restore result is
 * shallow-copied with a cloned state and mutated before it reaches the
 * intervention verification, with the compiled plan identity preserved.
 */
function healedRestoreOps(
  mutate: (state: PipelineV2RunState) => void,
): CountingInterventionOps {
  let openCalls = 0;
  return {
    acceptIntent: acceptPipelineV2ContinueStageIntent,
    restoreAcceptedPlan: async (args: unknown) => {
      const real = await restorePipelineV2AcceptedRunPlan(args as never);
      const clone = { ...real, state: structuredClone(real.state) } as unknown as Record<string, unknown>;
      mutate(clone["state"] as PipelineV2RunState);
      return clone as never;
    },
    openContinuedStage: async (args: unknown) => {
      openCalls += 1;
      return await openPipelineV2ContinuedStage(args as never);
    },
    restoreCalls: () => 0,
    openCalls: () => openCalls,
  } as unknown as CountingInterventionOps;
}

/**
 * The real acceptance and restore beside the real composition whose
 * successful result state is cloned and mutated before it reaches the
 * intervention verification.
 */
function healedOpenOps(
  mutate: (state: PipelineV2RunState) => void,
): PipelineV2ContinueStageInterventionOps {
  return {
    acceptIntent: acceptPipelineV2ContinueStageIntent,
    restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
    openContinuedStage: async (args: unknown) => {
      const real = await openPipelineV2ContinuedStage(args as never);
      const clone = { ...real, state: structuredClone(real.state) } as unknown as Record<string, unknown>;
      mutate(clone["state"] as PipelineV2RunState);
      return clone as never;
    },
  } as unknown as PipelineV2ContinueStageInterventionOps;
}

test("42. a successful acceptance result with a forged started_at heals downstream and is refused", async () => {
  const ctx = await driveToSecondWait();
  try {
    const ops = healedAcceptanceOps((state) => {
      state.started_at = "1999-01-01T00:00:00.000Z";
    });
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const error = expectApplyError(cause);
    expect(error.reason).toBe("invalid_result");
    expect(cause).not.toBeInstanceOf(TypeError);
    expect(error.message).not.toContain("HOSTILE");
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
    // the durable state stays untouched by the refused presentation
    expect((ctx.reopened.snapshot as PipelineV2RunState).waits[1]?.response).toBeUndefined();
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("43. a successful restore result with a forged input digest heals downstream and is refused", async () => {
  const ctx = await driveToSecondWait();
  try {
    const ops = healedRestoreOps((state) => {
      (state.inputs[0] as unknown as Record<string, unknown>)["digest"] = hex("9");
    });
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const error = expectApplyError(cause);
    expect(error.reason).toBe("invalid_result");
    expect(cause).not.toBeInstanceOf(TypeError);
    expect(error.message).not.toContain("HOSTILE");
    expect(ops.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("44. a forged final open state with a forged input digest is refused with the verified state", async () => {
  const ctx = await driveToSecondWait();
  try {
    const ops = healedOpenOps((state) => {
      (state.inputs[0] as unknown as Record<string, unknown>)["digest"] = hex("9");
    });
    const cause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const error = expectApplyError(cause);
    expect(error.reason).toBe("invalid_result");
    expect(cause).not.toBeInstanceOf(TypeError);
    expect(error.message).not.toContain("HOSTILE");
    // the authoritative error state is the verified restored state, never
    // the hostile presentation
    expect((error.state as PipelineV2RunState).inputs[0]?.digest).toBe(PROTECTED_DIGEST);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});


/**
 * Bounded concurrent runner for the independent matrix cases: every case
 * builds its own temp fixture, so the heavy matrix loops do not inflate
 * the suite's worker load.
 */
async function runBounded<T>(items: T[], limit: number, worker: (item: T) => Promise<void>): Promise<void> {
  const queue = [...items];
  await Promise.all(
    Array.from({ length: Math.min(limit, queue.length) }, async () => {
      for (;;) {
        const item = queue.shift();
        if (item === undefined) {
          return;
        }
        await worker(item);
      }
    }),
  );
}

// --- the full schema-owned regression matrix ---------------------------------

type StateMutation = (state: PipelineV2RunState) => void;

function mutationList(): [string, StateMutation][] {
  const grantRecord = {
    index: 2,
    generation_index: 2,
    wait_index: 2,
    intent_sha256: "",
    additional_iterations: 2,
  };
  return [
    ["schema version", (state) => { (state as unknown as Record<string, unknown>)["schema_version"] = 6; }],
    ["updated_at not a schema-v7 timestamp", (state) => { (state as unknown as Record<string, unknown>)["updated_at"] = 12345; }],
    ["revision delta", (state) => { (state as unknown as Record<string, unknown>)["revision"] = (state.revision as number) + 9; }],
    ["pipeline identity", (state) => { (state.pipeline as unknown as Record<string, unknown>)["execution_snapshot_sha256"] = hex("f"); }],
    ["input digest", (state) => { (state.inputs[0] as unknown as Record<string, unknown>)["digest"] = hex("9"); }],
    ["input type", (state) => { (state.inputs[1] as unknown as Record<string, unknown>)["type"] = "file"; }],
    ["agent execution profile", (state) => { (state.executions[0] as unknown as Record<string, unknown>)["profile"] = "stranger"; }],
    ["agent execution outputs", (state) => {
      const record = state.executions[0] as unknown as Record<string, unknown>;
      (record["outputs"] as unknown[])[0] = { id: "plan", digest: hex("9") };
    }],
    ["agent session cleanup", (state) => {
      const record = state.executions[1] as unknown as Record<string, unknown>;
      (record["session_cleanup"] as unknown as Record<string, unknown>)["tool"] = "failed";
    }],
    ["decision execution result", (state) => {
      const record = state.executions[2] as unknown as Record<string, unknown>;
      (record["result"] as unknown as Record<string, unknown>)["decision"] = "stop";
    }],
    ["decision execution rule", (state) => {
      const record = state.executions[2] as unknown as Record<string, unknown>;
      (record["result"] as unknown as Record<string, unknown>)["rule_id"] = "rule-stop";
    }],
    ["transition target", (state) => { (state.transitions[0] as unknown as Record<string, unknown>)["to"] = "done"; }],
    ["historical wait request digest", (state) => { (state.waits[0] as unknown as Record<string, unknown>)["request_sha256"] = hex("9"); }],
    ["target wait reason", (state) => { (state.waits[1] as unknown as Record<string, unknown>)["reason"] = "other_reason"; }],
    ["target wait actions", (state) => {
      (state.waits[1] as unknown as Record<string, unknown>)["actions"] = [{ id: "continue_stage", to: "architect" }];
    }],
    ["task revision digest", (state) => { (state.task_revisions[0] as unknown as Record<string, unknown>)["sha256"] = hex("9"); }],
    ["plan predecessor digest", (state) => { (state.plan_revisions[0] as unknown as Record<string, unknown>)["previous_sha256"] = hex("9"); }],
    ["grant append", (state) => { (state.grants as unknown[]).push({ ...grantRecord, intent_sha256: hex("9") }); }],
    ["live generation stage position", (state) => { (state.generations[1] as unknown as Record<string, unknown>)["stage_position"] = 2; }],
    ["live generation open iteration", (state) => {
      (state.generations[1] as unknown as Record<string, unknown>)["open_iteration"] = { index: 9, opened_transition_count: 7 };
    }],
    ["historical generation stage", (state) => { (state.generations[0] as unknown as Record<string, unknown>)["stage_id"] = "other"; }],
    ["historical iteration closure", (state) => {
      const generation = state.generations[0] as unknown as Record<string, unknown>;
      ((generation["iterations"] as unknown[])[1] as unknown as Record<string, unknown>)["closed"] = {
        by: "exhausted",
        closed_transition_count: 9,
      };
    }],
  ];
}

test("45. the acceptance matrix: every schema-owned mutation is refused before the restore", async () => {
  await runBounded(mutationList(), 5, async ([label, mutate]) => {
    const ctx = await driveToSecondWait();
    try {
      const ops = healedAcceptanceOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      if (error.reason !== "invalid_result") {
        throw new Error(`${label}: expected invalid_result, got ${error.reason}`);
      }
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
      expect(ops.restoreCalls()).toBe(0);
      expect(ops.openCalls()).toBe(0);
    } finally {
      await rm(ctx.root, { recursive: true, force: true });
    }
  });
}, 90000);


test("46. the restore matrix: every schema-owned mutation is refused before the open", async () => {
  await runBounded(mutationList(), 5, async ([label, mutate]) => {
    const ctx = await driveToSecondWait();
    try {
      const ops = healedRestoreOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      if (error.reason !== "invalid_result") {
        throw new Error(`${label}: expected invalid_result, got ${error.reason}`);
      }
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
      expect(ops.openCalls()).toBe(0);
    } finally {
      await rm(ctx.root, { recursive: true, force: true });
    }
  });
}, 90000);


test("47. the open matrix: every schema-owned mutation of the final state is refused with the verified state", async () => {
  await runBounded(mutationList(), 5, async ([label, mutate]) => {
    const ctx = await driveToSecondWait();
    try {
      const ops = healedOpenOps(mutate);
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      if (error.reason !== "invalid_result") {
        throw new Error(`${label}: expected invalid_result, got ${error.reason}`);
      }
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
      expect((error.state as PipelineV2RunState).inputs[0]?.digest).toBe(PROTECTED_DIGEST);
    } finally {
      await rm(ctx.root, { recursive: true, force: true });
    }
  });
}, 90000);


// --- capture, provenance and caller policy ----------------------------------

test("22. the options fields and the ops members are read exactly once in the fixed order", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const reads: string[] = [];
    const optionsProxy = new Proxy(
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
        hostile: "HOSTILE-OPTIONS-FIELD",
      },
      {
        get(target, property) {
          reads.push(String(property));
          return (target as Record<string, unknown>)[property as string];
        },
      },
    );
    const opsProxy = new Proxy(
      productionContinueStageInterventionOps as unknown as Record<string, unknown>,
      {
        get(target, property) {
          reads.push(`ops:${String(property)}`);
          return (target as Record<string, unknown>)[property as string];
        },
      },
    );
    const result = await applyPipelineV2ContinueStageInterventionWithIo(opsProxy, optionsProxy);
    expect(result.closed_iteration_index).toBe(1);
    // exactly the capture members; the hostile extra field is never read
    expect(reads).toEqual([
      "pipeline",
      "runRoot",
      "sink",
      "intent",
      "initialBudget",
      "ops:acceptIntent",
      "ops:restoreAcceptedPlan",
      "ops:openContinuedStage",
    ]);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("48. a different canonical updated_at at a non-zero suffix delta is not refused for the timestamp difference", async () => {
  // The acceptance result's state carries another schema-valid canonical
  // timestamp with a non-zero suffix delta; the real restore and the
  // composition proceed and the intervention succeeds.
  const ctxA = await driveToSecondWait();
  try {
    const revisionBefore = (ctxA.reopened.snapshot as PipelineV2RunState).revision;
    const result = await applyPipelineV2ContinueStageInterventionWithIo(healedAcceptanceOps((state) => {
      state.updated_at = "1999-01-01T00:00:00.000Z";
    }), {
      pipeline: ctxA.pipeline,
      runRoot: ctxA.runRoot,
      sink: recordSink(ctxA.reopened),
      intent: ctxA.intent,
      initialBudget: INITIAL_BUDGET,
    });
    expect(result.closed_iteration_index).toBe(1);
    expect(result.state.revision).toBe(revisionBefore + 5);
    expect(result.state.updated_at).not.toBe("1999-01-01T00:00:00.000Z");
  } finally {
    await rm(ctxA.root, { recursive: true, force: true });
  }
  // The open result's final state carries another schema-valid canonical
  // timestamp; the difference alone is never a refusal.
  const ctxB = await driveToSecondWait();
  try {
    const result = await applyPipelineV2ContinueStageInterventionWithIo(healedOpenOps((state) => {
      state.updated_at = "1999-01-01T00:00:00.000Z";
    }), {
      pipeline: ctxB.pipeline,
      runRoot: ctxB.runRoot,
      sink: recordSink(ctxB.reopened),
      intent: ctxB.intent,
      initialBudget: INITIAL_BUDGET,
    });
    expect(result.closed_iteration_index).toBe(1);
    expect(result.state.updated_at).toBe("1999-01-01T00:00:00.000Z");
  } finally {
    await rm(ctxB.root, { recursive: true, force: true });
  }
}, 90000);

test("23. forged intents and pipelines are rejected before any effect", async () => {
  const ctx = await driveToReopenedWait();
  try {
    // a hand-built intent look-alike
    const handBuilt = { manifest: ctx.intent.manifest, canonical_json: "x", sha256: ctx.intent.sha256 };
    const ops = countingDownstreamOps();
    const handCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: handBuilt,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const handError = expectApplyError(handCause);
    expect(handError.reason).toBe("invalid_options");
    expect(ops.restoreCalls()).toBe(0);
    expect(ops.openCalls()).toBe(0);
    // a structural clone of the prepared intent
    const cloned = structuredClone(ctx.intent);
    const cloneOps = countingDownstreamOps();
    const cloneCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(cloneOps, {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: cloned,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    const cloneError = expectApplyError(cloneCause);
    expect(cloneError.reason).toBe("invalid_options");
    expect(cloneOps.restoreCalls()).toBe(0);
    // a Proxy pipeline whose traps must never fire
    let pipelineTraps = 0;
    const pipelineProxy = new Proxy(ctx.pipeline as unknown as Record<string, unknown>, {
      get(target, property) {
        pipelineTraps += 1;
        return target[property as string];
      },
    }) as unknown as ResolvedPipelineV2;
    const proxyOps = countingDownstreamOps();
    const proxyCause = await catchApply(() =>
      applyPipelineV2ContinueStageInterventionWithIo(proxyOps, {
        pipeline: pipelineProxy,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    // the existing gate's error passes through unchanged
    expect(proxyCause).toBeInstanceOf(PipelineError);
    expect(proxyCause).not.toBeInstanceOf(PipelineV2ContinueStageInterventionControllerError);
    expect(pipelineTraps).toBe(0);
    expect(proxyOps.restoreCalls()).toBe(0);
    expect(proxyOps.openCalls()).toBe(0);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("24. malformed options are invalid_options before any effect", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const shapes: unknown[] = [
      null,
      undefined,
      42,
      "HOSTILE-OPTIONS",
      [],
      {},
      {
        pipeline: ctx.pipeline,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: "",
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: 42,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        intent: ctx.intent,
        initialBudget: INITIAL_BUDGET,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        initialBudget: INITIAL_BUDGET,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: 0,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: -1,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: 1.5,
      },
      {
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: recordSink(ctx.reopened),
        intent: ctx.intent,
        initialBudget: "2",
      },
    ];
    for (const shape of shapes) {
      const ops = countingDownstreamOps();
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(ops, shape),
      );
      const error = expectApplyError(cause);
      expect(error.reason).toBe("invalid_options");
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
      expect(ops.restoreCalls()).toBe(0);
      expect(ops.openCalls()).toBe(0);
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("25. malformed ops are invalid_options before any effect", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const shapes: unknown[] = [
      null,
      undefined,
      42,
      "HOSTILE-OPS",
      [],
      {},
      { acceptIntent: acceptPipelineV2ContinueStageIntent },
      {
        acceptIntent: acceptPipelineV2ContinueStageIntent,
        restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
      },
      {
        acceptIntent: "HOSTILE-NOT-A-FUNCTION",
        restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
        openContinuedStage: openPipelineV2ContinuedStage,
      },
      {
        acceptIntent: acceptPipelineV2ContinueStageIntent,
        restoreAcceptedPlan: 42,
        openContinuedStage: openPipelineV2ContinuedStage,
      },
      {
        acceptIntent: acceptPipelineV2ContinueStageIntent,
        restoreAcceptedPlan: restorePipelineV2AcceptedRunPlan,
        openContinuedStage: null,
      },
    ];
    for (const shape of shapes) {
      const cause = await catchApply(() =>
        applyPipelineV2ContinueStageInterventionWithIo(shape as unknown as PipelineV2ContinueStageInterventionOps, {
          pipeline: ctx.pipeline,
          runRoot: ctx.runRoot,
          sink: recordSink(ctx.reopened),
          intent: ctx.intent,
          initialBudget: INITIAL_BUDGET,
        }),
      );
      const error = expectApplyError(cause);
      expect(error.reason).toBe("invalid_options");
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("HOSTILE");
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("26. caller mutation after the pending acceptance cannot change the intervention policy", async () => {
  const ctx = await driveToReopenedWait();
  try {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const options: Record<string, unknown> = {
      pipeline: ctx.pipeline,
      runRoot: ctx.runRoot,
      sink: recordSink(ctx.reopened),
      intent: ctx.intent,
      initialBudget: INITIAL_BUDGET,
    };
    const ops: Record<string, unknown> = {
      ...productionContinueStageInterventionOps,
      acceptIntent: async (args: unknown) => {
        await gate;
        return await acceptPipelineV2ContinueStageIntent(args as never);
      },
    };
    const pending = applyPipelineV2ContinueStageInterventionWithIo(ops as never, options as never);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    // mutate the caller's options and reassign the ops members while the
    // acceptance is pending
    options["initialBudget"] = 99;
    options["intent"] = {};
    options["pipeline"] = null;
    (ops as Record<string, unknown>)["restoreAcceptedPlan"] = async () => {
      throw new Error("the reassigned restore must not be called");
    };
    (ops as Record<string, unknown>)["openContinuedStage"] = async () => {
      throw new Error("the reassigned composition must not be called");
    };
    release?.();
    const result = await pending;
    expect(result.additional_iterations).toBe(2);
    expect(result.closed_iteration_index).toBe(1);
    expect(result.iteration_index).toBe(2);
    expect(result.state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 3 });
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- durability windows -----------------------------------------------------

import { PipelineV2ContinueStageGrantControllerError } from "../src/pipeline_v2_continue_stage_grant_controller.ts";
import { PipelineV2WaitControllerError } from "../src/pipeline_v2_wait_controller.ts";
import { PipelineV2StageIterationControllerError } from "../src/pipeline_v2_stage_iteration_controller.ts";

const COMMAND_ERROR_CLASSES = [
  PipelineV2ContinueStageIntentControllerError,
  PipelineV2ContinueStageGrantControllerError,
  PipelineV2ContinueStageGrantControllerError,
  PipelineV2WaitControllerError,
  PipelineV2StageIterationControllerError,
] as const;

async function expectFaultWindow(
  ordinal: number,
  failStep: "rename" | "dirfsync",
): Promise<void> {
  const ctx = await driveToReopenedWait();
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const faulted = await PipelineV2RunStateSink.open({
      stateRoot: ctx.stateRoot,
      runId: RUN_ID,
      now: nextTick,
      io: faultIo({ failCommit: ordinal, failStep }),
    });
    const faultRecording = recordSink(faulted);
    const cause = await catchApply(() => callIntervention(ctx, faultRecording));
    // the owning layer's typed durability failure passes through by
    // identity, never re-classified
    const expectedErrorClass: (typeof COMMAND_ERROR_CLASSES)[number] = COMMAND_ERROR_CLASSES[ordinal - 1] as (typeof COMMAND_ERROR_CLASSES)[number];
    expect(cause, `${failStep} at command ${ordinal}`).toBeInstanceOf(expectedErrorClass);
    expect((cause as { readonly reason?: unknown }).reason, `${failStep} at command ${ordinal}`).toBe("state_persist_failed");
    // the recording sink logs only successful dispatches: the faulted
    // commit's attempt throws through it in both windows
    expect(faultRecording.commands.map((command) => command.kind), `${failStep} at command ${ordinal}`).toEqual(SUFFIX.slice(0, ordinal - 1));
    const committed = failStep === "rename" ? ordinal - 1 : ordinal;
    // the faulted sink owns the commits: the last good snapshot after a
    // pre-rename fault, the adopted candidate after a durability-unknown
    expect((faulted.snapshot as PipelineV2RunState).revision, `${failStep} at command ${ordinal}`).toBe(revisionBefore + committed);
    // the fresh retry: a reopened sink recognizes the durable prefix and
    // dispatches only the missing suffix
    const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.stateRoot, runId: RUN_ID, now: nextTick });
    const freshRecording = recordSink(fresh);
    const result = await callIntervention(ctx, freshRecording);
    const remaining = failStep === "rename" ? SUFFIX.slice(ordinal - 1) : SUFFIX.slice(ordinal);
    expect(freshRecording.commands.map((command) => command.kind), `${failStep} at command ${ordinal}`).toEqual([...remaining]);
    expect((fresh.snapshot as PipelineV2RunState).revision, `${failStep} at command ${ordinal}`).toBe(revisionBefore + 5);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
}

test("27. a fresh retry after a not_committed at the plan_intent_accepted commit completes the missing suffix", async () => {
  await expectFaultWindow(1, "rename");
});

test("28. a fresh retry after a not_committed at the iteration_grant_recorded commit completes the missing suffix", async () => {
  await expectFaultWindow(2, "rename");
});

test("29. a fresh retry after a not_committed at the stage_iteration_closed commit completes the missing suffix", async () => {
  await expectFaultWindow(3, "rename");
});

test("30. a fresh retry after a not_committed at the wait_response_recorded commit completes the missing suffix", async () => {
  await expectFaultWindow(4, "rename");
});

test("31. a fresh retry after a not_committed at the stage_iteration_opened commit completes the missing suffix", async () => {
  await expectFaultWindow(5, "rename");
});

test("32. a fresh retry after a durability_unknown at the plan_intent_accepted commit completes the missing suffix", async () => {
  await expectFaultWindow(1, "dirfsync");
});

test("33. a fresh retry after a durability_unknown at the iteration_grant_recorded commit completes the missing suffix", async () => {
  await expectFaultWindow(2, "dirfsync");
});

test("34. a fresh retry after a durability_unknown at the stage_iteration_closed commit completes the missing suffix", async () => {
  await expectFaultWindow(3, "dirfsync");
});

test("35. a fresh retry after a durability_unknown at the wait_response_recorded commit completes the missing suffix", async () => {
  await expectFaultWindow(4, "dirfsync");
});

test("36. a fresh retry after a durability_unknown at the stage_iteration_opened commit completes the missing suffix", async () => {
  await expectFaultWindow(5, "dirfsync");
});

// --- concurrency and conflicts ----------------------------------------------

test("37. two identical concurrent interventions converge to one durable suffix", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const firstRecording = recordSink(ctx.reopened);
    const secondRecording = recordSink(ctx.reopened);
    const [first, second] = await Promise.all([
      callIntervention(ctx, firstRecording),
      callIntervention(ctx, secondRecording),
    ]);
    // exactly one durable record of each suffix step; the dispatch
    // attempts may race, but the durable state converges
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 5);
    const state = ctx.reopened.snapshot as PipelineV2RunState;
    expect(state.grants).toHaveLength(1);
    expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
    expect(state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 3 });
    expect(resultFields(second)).toEqual(resultFields(first));
    for (const recording of [firstRecording, secondRecording]) {
      for (const kind of SUFFIX) {
        expect(recording.commands.filter((command) => command.kind === kind).length <= 2, kind).toBe(true);
      }
    }
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("38. a conflicting caller budget is never accepted as a retry", async () => {
  const ctx = await driveToReopenedWait();
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    const recording = recordSink(ctx.reopened);
    const cause = await catchApply(() => callIntervention(ctx, recording, 3));
    // the composed layers' typed refusal passes through by identity: the
    // honest completion succeeded and verified its result against the
    // caller budget, so the composition refuses before the ensure call
    expect(cause).toBeInstanceOf(PipelineV2ContinuedStageControllerError);
    expect((cause as PipelineV2ContinuedStageControllerError).reason).toBe("invalid_result");
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 4);
    expect((ctx.reopened.snapshot as PipelineV2RunState).generations[0]?.initial_budget).toBe(INITIAL_BUDGET);
    expect((ctx.reopened.snapshot as PipelineV2RunState).generations[0]?.open_iteration).toBeUndefined();
    // a fresh retry with the honest budget completes the full suffix from
    // the durable prefix
    const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.stateRoot, runId: RUN_ID, now: nextTick });
    const freshRecording = recordSink(fresh);
    const result = await callIntervention(ctx, freshRecording);
    expect(freshRecording.commands.map((command) => command.kind)).toEqual(["stage_iteration_opened"]);
    expect(result.iteration_index).toBe(2);
    assertIntervenedBoundary(result.state, ctx.intent);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

test("39. a conflicting intent (another stage or another budget) is never accepted as a retry", async () => {
  const ctx = await driveToReopenedWait({ acceptIntent: true });
  try {
    const revisionBefore = (ctx.reopened.snapshot as PipelineV2RunState).revision;
    // another stage id: a different digest for the same wait
    const otherStageIntent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: "stage-2",
      expected_plan_sha256: continueManifestOf(ctx.intent).expected_plan_sha256,
      additional_iterations: 2,
    });
    const stageRecording = recordSink(ctx.reopened);
    const stageCause = await catchApply(() =>
      applyPipelineV2ContinueStageIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: stageRecording as never,
        intent: otherStageIntent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(stageCause).toBeInstanceOf(PipelineV2ContinueStageIntentControllerError);
    // the stage binding precedes the digest reconciliation, so another
    // stage is the acceptance's own invalid_state — never a retry
    expect((stageCause as PipelineV2ContinueStageIntentControllerError).reason).toBe("invalid_state");
    expect(stageRecording.commands).toEqual([]);
    // another additional iteration count: a different digest again
    const otherBudgetIntent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: "stage-1",
      expected_plan_sha256: continueManifestOf(ctx.intent).expected_plan_sha256,
      additional_iterations: 5,
    });
    const budgetRecording = recordSink(ctx.reopened);
    const budgetCause = await catchApply(() =>
      applyPipelineV2ContinueStageIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.runRoot,
        sink: budgetRecording as never,
        intent: otherBudgetIntent,
        initialBudget: INITIAL_BUDGET,
      }),
    );
    expect(budgetCause).toBeInstanceOf(PipelineV2ContinueStageIntentControllerError);
    expect((budgetCause as PipelineV2ContinueStageIntentControllerError).reason).toBe("intent_conflict");
    expect(budgetRecording.commands).toEqual([]);
    expect((ctx.reopened.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
  } finally {
    await rm(ctx.root, { recursive: true, force: true });
  }
});

// --- export surface and source scan -----------------------------------------

test("40. the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_continue_stage_intervention_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinueStageInterventionControllerError",
    "applyPipelineV2ContinueStageIntervention",
  ]);
  const internalModule = await import("../src/pipeline_v2_continue_stage_intervention_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinueStageInterventionControllerError",
    "applyPipelineV2ContinueStageInterventionWithIo",
    "productionContinueStageInterventionOps",
  ]);
  expect(productionContinueStageInterventionOps.acceptIntent).toBe(acceptPipelineV2ContinueStageIntent);
  expect(productionContinueStageInterventionOps.restoreAcceptedPlan).toBe(restorePipelineV2AcceptedRunPlan);
  expect(productionContinueStageInterventionOps.openContinuedStage).toBe(openPipelineV2ContinuedStage);
  expect(Object.isFrozen(productionContinueStageInterventionOps)).toBe(true);
});

test("41. the intervention module composes only the three existing facades (source scan)", async () => {
  const { readFile: readSource } = await import("node:fs/promises");
  const source = await readSource(
    join(import.meta.dir, "..", "src", "pipeline_v2_continue_stage_intervention_controller_internal.ts"),
    "utf8",
  );
  const countOf = (needle: string): number => source.split(needle).length - 1;
  // no reducer, no validator, no store, no filesystem of its own
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
  expect(countOf("node:path")).toBe(0);
  expect(countOf("pipeline_v2_state_store")).toBe(0);
  expect(countOf("pipeline_v2_state_sink")).toBe(0);
  expect(countOf("pipeline_v2_wait_store")).toBe(0);
  expect(countOf("pipeline_v2_run_plan_store")).toBe(0);
  expect(countOf("pipeline_v2_run_plan_candidate")).toBe(0);
  expect(countOf("pipeline_v2_run_plan_controller")).toBe(0);
  expect(countOf("pipeline_v2_coordinator")).toBe(0);
  expect(countOf("pipeline_v2_runner")).toBe(0);
  expect(countOf("main.ts")).toBe(0);
  expect(countOf("cli_")).toBe(0);
  expect(countOf("docker")).toBe(0);
  expect(countOf("launcher")).toBe(0);
  // no mutable module-global seam and no message parsing
  expect(countOf("let production")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf(".test(")).toBe(0);
  // the message composition is explicit, never parsed from causes
  expect(countOf("cause.message")).toBe(0);
  expect(countOf(".message.includes")).toBe(0);
});
