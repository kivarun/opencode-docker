import { describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
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
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  compilePipelineV2RunPlanCandidate,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import {
  closePipelineV2StageIteration,
  ensurePipelineV2StageIteration,
  PipelineV2StageIterationControllerError,
} from "../src/pipeline_v2_stage_iteration_controller.ts";
import { compiledPipelineV2RunPlanStageFor } from "../src/pipeline_v2_run_plan_compiled.ts";
import { applyPipelineV2ReviseTaskClosure } from "../src/pipeline_v2_revise_task_closure_controller.ts";
import { completePipelineV2ReviseTask } from "../src/pipeline_v2_revise_task_completion_controller.ts";
import {
  closePipelineV2ReplannedGeneration,
  PipelineV2ReplannedGenerationControllerError,
  type PipelineV2ReplannedGenerationControllerSink,
} from "../src/pipeline_v2_replanned_generation_controller.ts";
import {
  openPipelineV2ReplannedStageWithOps,
  productionReplannedStageOps,
  PipelineV2ReplannedStageControllerError,
  type OpenedPipelineV2ReplannedStage,
  type PipelineV2ReplannedStageOps,
} from "../src/pipeline_v2_replanned_stage_controller_internal.ts";
import type { ClosedPipelineV2ReplannedGeneration } from "../src/pipeline_v2_replanned_generation_controller.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");

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
    - state_id: dev_entry
      role: stage
      stage_template: development

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

async function withPipeline<T>(fn: (pipeline: ResolvedPipelineV2) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-replanned-stage-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    return await fn(await loadPipelineV2(bundle));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface RunFixture {
  root: string;
  stateRoot: string;
  runRoot: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-replanned-stage-run-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  return { root, stateRoot, runRoot };
}

async function disposeRun(fixture: RunFixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 9, 26, 0, 0, clockCounter));
}

const BASE_INPUTS = [{ id: "task", type: "file" as const, protected: true, digest: PROTECTED_DIGEST }];

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

const A1 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-a",
  revision: 1,
  previous_sha256: null,
  origin: "planning_proposal",
  body: "Body A",
});

const A2 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-a",
  revision: 2,
  previous_sha256: A1.sha256,
  origin: "user_response",
  body: "Body A revised",
});

const INTENT = prepareWaitIntent({
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: RUN_ID,
  wait_index: 1,
  task_id: "task-a",
  expected_previous_task_sha256: A1.sha256,
  new_task_revision_sha256: A2.sha256,
});

const FAKE_DIGEST = "f".repeat(64);

/**
 * A composition ops object with the real close controller and a fake
 * ensure step; the returned ops type is erased so the fake can return
 * hostile shapes.
 */
function ensureOpsWith(behavior: () => Promise<unknown>): PipelineV2ReplannedStageOps {
  let ensureCalls = 0;
  const ops = {
    closeGeneration: closePipelineV2ReplannedGeneration,
    ensureStageIteration: async () => {
      ensureCalls += 1;
      return await behavior();
    },
    ensureCalls: () => ensureCalls,
  };
  return ops as unknown as PipelineV2ReplannedStageOps;
}

/**
 * A composition ops object with a fake close step that wraps the REAL
 * close controller and applies a narrow mutation to the returned result
 * (result fields and/or its durable state snapshot); the ensure step
 * records its calls and throws, so every negative close-result case
 * proves the ensure step is never reached.
 */
function mutatedCloseOps(
  ctx: ReadyCtx,
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
): PipelineV2ReplannedStageOps & { ensureCalls: () => number } {
  let ensureCalls = 0;
  const ops = {
    closeGeneration: async (args: unknown) => {
      const real = await closePipelineV2ReplannedGeneration(args as never);
      const clone = structuredClone(real) as unknown as Record<string, unknown>;
      const state = clone["state"] as PipelineV2RunState;
      mutate(clone, state);
      return clone;
    },
    ensureStageIteration: async () => {
      ensureCalls += 1;
      throw new Error("ensure must not be called");
    },
    ensureCalls: () => ensureCalls,
  };
  return ops as unknown as PipelineV2ReplannedStageOps & { ensureCalls: () => number };
}

interface ReadyCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  compiledPlan: CompiledPipelineV2RunPlan;
  plan2: ReturnType<typeof preparePlanRevisionManifest>;
}

/**
 * The real reducer/sink/controllers path to the replanned-stage boundary:
 * plan r1 accepted, generation/iteration opened, the revise intent and
 * task revision durably accepted, the iteration closed `by:"replanned"`,
 * the revise_task response recorded, the settled planning execution on
 * the declared action target, and the next plan r2 accepted — the old
 * generation is still open at this boundary (the composition's C0 start).
 */
interface ReplannedStageReadyOptions {
  withPrefixGeneration?: boolean;
}

async function replannedStageReady(options: ReplannedStageReadyOptions = {}): Promise<ReadyCtx> {
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
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
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ],
    });
    const plan1Candidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [A1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: plan1Candidate });
    const compiledPlan1 = compilePipelineV2RunPlanCandidate(pipeline, plan1Candidate);
    await ensurePipelineV2StageIteration({ compiledPlan: compiledPlan1, stageId: "stage-1", initialBudget: 2, sink });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    if (options.withPrefixGeneration === true) {
      // Close generation 1 through the real active-boundary closure (the
      // settled stage execution is still unbound), commit the transition,
      // and open generation 2 of the same plan as a bare generation; the
      // planning execution of the cycle then runs outside any iteration
      // and the stage cycle runs inside generation 2's open iteration 1,
      // so the replanned old generation carries a non-trivial historical
      // prefix (index 2).
      await closePipelineV2StageIteration({
        compiledPlan: compiledPlan1,
        stageId: "stage-1",
        iterationCloseReason: "normal_close",
        generationCloseReason: "next_stage",
        sink,
      });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
      await sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: plan1.sha256,
        initialBudget: 2,
        transitionCount: 2,
      });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning2")) {
        await sink.dispatch(command);
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 3 });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
      for (const command of agentPhases("stage2")) {
        await sink.dispatch(command);
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 4,
      });
    } else {
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
    }
    const actions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
    const waitTransitionCount = options.withPrefixGeneration === true ? 4 : 2;
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: waitTransitionCount,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions,
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions,
    });
    await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT.sha256 });
    await sink.dispatch({
      kind: "task_revision_accepted",
      taskId: "task-a",
      revision: 2,
      taskSha256: A2.sha256,
      waitIndex: 1,
      intentSha256: INTENT.sha256,
    });
    await applyPipelineV2ReviseTaskClosure({ sink, intent: INTENT });
    await completePipelineV2ReviseTask({ runRoot: fixture.runRoot, sink, intent: INTENT });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning3")) {
      await sink.dispatch(command);
    }
    const planningExecutionIndex = options.withPrefixGeneration === true ? 5 : 3;
    const plan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: planningExecutionIndex,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate2 = preparePipelineV2RunPlanCandidate({
      plan: plan2,
      taskRevisions: [A2],
      previousPlan: plan1,
      previousTaskRevisions: [A1],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidate2 });
    return { fixture, sink, pipeline, intent: INTENT, compiledPlan: accepted.compiled_plan, plan2 };
  });
}

interface RecordingSink extends PipelineV2ReplannedGenerationControllerSink {
  commands: PipelineV2RunCommand[];
}

function recordingSink(inner: PipelineV2RunStateSink): RecordingSink {
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

function mutateSnapshotSink(
  inner: PipelineV2RunStateSink,
  mutate: (state: PipelineV2RunState) => void,
): PipelineV2ReplannedGenerationControllerSink {
  return {
    get snapshot(): PipelineV2RunState | null {
      const snapshot = inner.snapshot;
      if (snapshot === null) {
        return null;
      }
      const clone = structuredClone(snapshot) as PipelineV2RunState;
      mutate(clone);
      return clone;
    },
    poisoned: inner.poisoned,
    dispatch: inner.dispatch.bind(inner),
  };
}

async function catchOpen(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectOpenError(cause: unknown): PipelineV2ReplannedStageControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReplannedStageControllerError);
  return cause as PipelineV2ReplannedStageControllerError;
}

interface CountingOps extends PipelineV2ReplannedStageOps {
  ensureCalls: number;
}

/**
 * The real close controller beside a counting ensure controller: used to
 * prove the composition order and the zero-ensure rule on a hostile
 * close result.
 */
function realCloseCountingEnsure(): CountingOps {
  let ensureCalls = 0;
  return {
    closeGeneration: closePipelineV2ReplannedGeneration,
    ensureStageIteration: async (options) => {
      ensureCalls += 1;
      return await ensurePipelineV2StageIteration(options);
    },
    get ensureCalls(): number {
      return ensureCalls;
    },
  };
}

describe("openPipelineV2ReplannedStage", () => {
  test("1. the C0 path composes the exact command order with revision +3", async () => {
    const ctx = await replannedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 3,
      });
      expect(recording.commands).toEqual([
        { kind: "stage_generation_closed", generationIndex: 1, by: "replanned" },
        {
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: ctx.plan2.sha256,
          initialBudget: 3,
          transitionCount: 2,
        },
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(2);
      expect(state.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      expect(state.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
      expect(state.generations[1]!.initial_budget).toBe(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the result is deep-frozen and content-free", async () => {
    const ctx = await replannedStageReady();
    try {
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(Object.keys(result).sort()).toEqual([
        "generation_index",
        "initial_budget",
        "intent_sha256",
        "iteration_index",
        "origin_execution",
        "plan_revision",
        "plan_sha256",
        "previous_generation_index",
        "stage_id",
        "stage_position",
        "state",
        "template_id",
        "wait_index",
      ]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      const serialized = JSON.stringify(result);
      expect(serialized).not.toContain("Body A");
      expect(serialized).not.toContain(ctx.fixture.runRoot);
      expect(serialized).not.toContain("canonical_json");
      expect(serialized).not.toContain('"prompt');
      expect(serialized).not.toContain("token");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the exact result fields and the loader round-trip", async () => {
    const ctx = await replannedStageReady();
    try {
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(result).toMatchObject({
        wait_index: 1,
        previous_generation_index: 1,
        generation_index: 2,
        iteration_index: 1,
        plan_revision: 2,
        plan_sha256: ctx.plan2.sha256,
        origin_execution: 3,
        stage_id: "stage-1",
        stage_position: 1,
        template_id: "development",
        initial_budget: 2,
      });
      expect(result.intent_sha256).toBe(ctx.intent.sha256);
      const state = result.state;
      const round = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      expect(round.generations).toHaveLength(2);
      expect(round.generations[1]!.open_iteration?.index).toBe(1);
      expect(round.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the C1 path: the closed old generation skips the close dispatch", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([
        {
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: ctx.plan2.sha256,
          initialBudget: 2,
          transitionCount: 2,
        },
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
      expect(result.previous_generation_index).toBe(1);
      expect(result.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the C2 retry path with the first iteration open is zero-dispatch through both controllers", async () => {
    const ctx = await replannedStageReady();
    try {
      const first = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect(result.previous_generation_index).toBe(first.previous_generation_index);
      expect(result.generation_index).toBe(first.generation_index);
      expect(result.iteration_index).toBe(first.iteration_index);
      expect(result.state.revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. the C2 retry path with the bare new generation opens only the iteration", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 3, failStep: "rename" }),
        now: nextTick,
      });
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((firstCause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      const afterFault = (await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot as PipelineV2RunState;
      expect(afterFault.generations).toHaveLength(2);
      expect(afterFault.generations[1]!.open_iteration).toBeUndefined();
      const revisionAfterFault = afterFault.revision;
      // The faulted sink advanced the on-disk state; the retry must run
      // through a sink that sees the durable revision.
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(retrySink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect(retrySink.snapshot!.revision).toBe(revisionAfterFault + 1);
      expect(result.generation_index).toBe(2);
      expect(result.iteration_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. the close not_committed fault window: identity refusal, zero ensure calls, full fresh retry", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
        now: nextTick,
      });
      const ops = realCloseCountingEnsure();
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2ReplannedGenerationControllerError);
      expect((firstCause as PipelineV2ReplannedGenerationControllerError).reason).toBe("state_persist_failed");
      expect(ops.ensureCalls).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.closed).toBeUndefined();
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toHaveLength(3);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
      expect(result.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. the close durability_unknown fault window: identity refusal, fresh sink continues from C1", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
        now: nextTick,
      });
      const ops = realCloseCountingEnsure();
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2ReplannedGenerationControllerError);
      expect((firstCause as PipelineV2ReplannedGenerationControllerError).reason).toBe("state_persist_failed");
      expect(ops.ensureCalls).toBe(0);
      const freshSink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      expect((freshSink.snapshot as PipelineV2RunState).generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      const recording = recordingSink(freshSink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([
        {
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: ctx.plan2.sha256,
          initialBudget: 2,
          transitionCount: 2,
        },
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect(result.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. the generation-open not_committed fault window: fresh retry continues from C1", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 2, failStep: "rename" }),
        now: nextTick,
      });
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((firstCause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      const afterFault = (await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot as PipelineV2RunState;
      expect(afterFault.generations).toHaveLength(1);
      expect(afterFault.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      const revisionAfterFault = afterFault.revision;
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(retrySink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([
        {
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: ctx.plan2.sha256,
          initialBudget: 2,
          transitionCount: 2,
        },
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect(retrySink.snapshot!.revision).toBe(revisionAfterFault + 2);
      expect(result.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. the generation-open durability_unknown fault window: fresh retry continues from C2", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 2, failStep: "dirfsync" }),
        now: nextTick,
      });
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((firstCause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      const freshSink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const afterFault = freshSink.snapshot as PipelineV2RunState;
      expect(afterFault.generations).toHaveLength(2);
      expect(afterFault.generations[1]!.open_iteration).toBeUndefined();
      const revisionAfterFault = afterFault.revision;
      const recording = recordingSink(freshSink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 },
      ]);
      expect((freshSink.snapshot as PipelineV2RunState).revision).toBe(revisionAfterFault + 1);
      expect(result.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. the iteration-open durability_unknown fault window: fresh retry is zero-dispatch through both controllers", async () => {
    const ctx = await replannedStageReady();
    try {
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 3, failStep: "dirfsync" }),
        now: nextTick,
      });
      const firstCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((firstCause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      const freshSink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const afterFault = freshSink.snapshot as PipelineV2RunState;
      expect(afterFault.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
      const revisionAfterFault = afterFault.revision;
      const recording = recordingSink(freshSink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([]);
      expect((freshSink.snapshot as PipelineV2RunState).revision).toBe(revisionAfterFault);
      expect(result.generation_index).toBe(2);
      expect(result.iteration_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. the exact complete retry dispatches nothing through either controller", async () => {
    const ctx = await replannedStageReady();
    try {
      const first = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      const revisionAfter = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const second = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([]);
      expect(second.state.revision).toBe(revisionAfter);
      expect(second.generation_index).toBe(first.generation_index);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. two identical C0 races both succeed with exactly one of each command and revision +3", async () => {
    const ctx = await replannedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const firstRecording = recordingSink(ctx.sink);
      const secondRecording = recordingSink(ctx.sink);
      const options = {
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      };
      const [first, second] = await Promise.all([
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, { sink: firstRecording, ...options }),
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, { sink: secondRecording, ...options }),
      ]);
      expect(second.generation_index).toBe(2);
      const all = [...firstRecording.commands, ...secondRecording.commands];
      expect(all.filter((command) => command.kind === "stage_generation_closed")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "stage_generation_opened")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "stage_iteration_opened")).toHaveLength(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(2);
      expect(state.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. two identical races after C1 both succeed with the generation and iteration written once", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const firstRecording = recordingSink(ctx.sink);
      const secondRecording = recordingSink(ctx.sink);
      const options = {
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      };
      const [first, second] = await Promise.all([
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, { sink: firstRecording, ...options }),
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, { sink: secondRecording, ...options }),
      ]);
      expect(first.generation_index).toBe(2);
      expect(second.iteration_index).toBe(1);
      const all = [...firstRecording.commands, ...secondRecording.commands];
      expect(all.filter((command) => command.kind === "stage_generation_opened")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "stage_iteration_opened")).toHaveLength(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a conflicting budget retry rewrites nothing and the winner state stays unchanged", async () => {
    const ctx = await replannedStageReady();
    try {
      const first = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const secondCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 5,
        }),
      );
      expect(secondCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((secondCause as PipelineV2StageIterationControllerError).reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[1]!.initial_budget).toBe(2);
      expect(first.generation_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. an unknown stage id or a forged compiled plan refuses through the existing resolver before any dispatch", async () => {
    const ctx = await replannedStageReady();
    try {
      const unknownStageCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-9",
          initialBudget: 2,
        }),
      );
      expect(unknownStageCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect((unknownStageCause as PipelineV2CompiledRunPlanError).reason).toBe("stage_not_found");
      const clonedPlan = structuredClone(ctx.compiledPlan) as unknown as CompiledPipelineV2RunPlan;
      const forgedCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: clonedPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(forgedCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect((forgedCause as PipelineV2CompiledRunPlanError).reason).toBe("invalid_plan");
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.closed).toBeUndefined();
      expect(revisionBefore).toBe((ctx.sink.snapshot as PipelineV2RunState).revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. a Proxy compiled plan is rejected with zero proxy traps and zero controller calls", async () => {
    const ctx = await replannedStageReady();
    try {
      let trapHits = 0;
      const proxied = new Proxy(structuredClone(ctx.compiledPlan) as object, {
        get(target, property) {
          trapHits += 1;
          return Reflect.get(target, property);
        },
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: proxied as unknown as CompiledPipelineV2RunPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect(trapHits).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.closed).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. invalid budgets and option shapes refuse as invalid_options with zero dispatches", async () => {
    const ctx = await replannedStageReady();
    try {
      const recording = recordingSink(ctx.sink);
      const invalidBudgetCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 0,
        }),
      );
      const zeroError = expectOpenError(invalidBudgetCause);
      expect(zeroError.reason).toBe("invalid_options");
      expect(recording.commands).toEqual([]);
      const fractionalCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 1.5,
        }),
      );
      expectOpenError(fractionalCause);
      const unsafeCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: Number.MAX_SAFE_INTEGER + 1,
        }),
      );
      expectOpenError(unsafeCause);
      const stageIdCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: 7 as unknown as string,
          initialBudget: 2,
        }),
      );
      const stageError = expectOpenError(stageIdCause);
      expect(stageError.reason).toBe("invalid_options");
      expect(recording.commands).toEqual([]);
      const optionsCause = await catchOpen(() => openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, null));
      expectOpenError(optionsCause);
      const opsCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps({ closeGeneration: () => undefined, ensureStageIteration: "no" }, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const opsError = expectOpenError(opsCause);
      expect(opsError.reason).toBe("invalid_options");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a hostile close result never reaches the ensure call", async () => {
    const ctx = await replannedStageReady();
    try {
      const realResult = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(realResult.generation_index).toBe(1);
      const ensureCalls: number[] = [];
      const makeOps = (mutate: (result: ClosedPipelineV2ReplannedGeneration) => void): PipelineV2ReplannedStageOps => ({
        closeGeneration: async () => {
          const clone = structuredClone(realResult) as ClosedPipelineV2ReplannedGeneration;
          mutate(clone);
          return clone;
        },
        ensureStageIteration: async () => {
          ensureCalls.push(1);
          throw new Error("ensure must not be called on a hostile close result");
        },
      });
      const cases: Array<[string, (result: ClosedPipelineV2ReplannedGeneration) => void]> = [
        ["wait index", (result) => { (result as unknown as Record<string, unknown>)["wait_index"] = 2; }],
        ["intent digest", (result) => { (result as unknown as Record<string, unknown>)["intent_sha256"] = hex("e"); }],
        ["plan digest", (result) => { (result as unknown as Record<string, unknown>)["plan_sha256"] = hex("9"); }],
        ["generation index", (result) => { (result as unknown as Record<string, unknown>)["generation_index"] = 5; }],
        ["state null", (result) => { (result as unknown as Record<string, unknown>)["state"] = null; }],
        ["waits malformed", (result) => { (result.state as unknown as Record<string, unknown>)["waits"] = null; }],
        ["old closure missing", (result) => { ((result.state as PipelineV2RunState).generations[0] as unknown as Record<string, unknown>)["closed"] = undefined; }],
        ["task digest", (result) => { (result as unknown as Record<string, unknown>)["task_sha256"] = hex("1"); }],
        ["malformed generations", (result) => { (result.state as unknown as Record<string, unknown>)["generations"] = null; }],
      ];
      for (const [label, mutate] of cases) {
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(makeOps(mutate), {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason, label).toBe("invalid_result");
        expect(ensureCalls, label).toHaveLength(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a hostile C2 close result with a closed new generation never reaches the ensure call", async () => {
    const ctx = await replannedStageReady();
    try {
      const full = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      const reopenIntent = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(
          {
            closeGeneration: async () => {
              const clone = structuredClone(full.state) as PipelineV2RunState;
              const generation = clone.generations[1] as unknown as Record<string, unknown>;
              const iteration = (generation["iterations"] as unknown[])[0] as Record<string, unknown>;
              (generation["iterations"] as unknown[])[0] = {
                ...iteration,
                closed: { by: "normal_close", closed_transition_count: 2 },
              };
              generation["open_iteration"] = undefined;
              generation["closed"] = { by: "next_stage", closed_transition_count: 2 };
              return {
                wait_index: full.wait_index,
                generation_index: full.previous_generation_index,
                iteration_index: 1,
                intent_sha256: full.intent_sha256,
                task_id: "task-a",
                task_revision: 2,
                task_sha256: A2.sha256,
                previous_plan_revision: 1,
                previous_plan_sha256: (clone.plan_revisions[0] as unknown as Record<string, unknown>)["sha256"],
                plan_revision: full.plan_revision,
                plan_sha256: full.plan_sha256,
                origin_execution: full.origin_execution,
                state: clone,
              };
            },
            ensureStageIteration: async () => {
              throw new Error("ensure must not be called");
            },
          },
          {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          },
        ),
      );
      const error = expectOpenError(reopenIntent);
      expect(error.reason).toBe("invalid_result");
      expect(error.message).toContain("the closure result new generation is not an open current-plan generation");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a hostile ensure result is compared against the verified close state, never itself", async () => {
    const ctx = await replannedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const makeOps = (mutate: (result: { compiled_stage: unknown; generation_index: number; iteration_index: number; state: PipelineV2RunState }) => void): PipelineV2ReplannedStageOps => ({
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async (options) => {
          const real = await ensurePipelineV2StageIteration(options);
          const clone = structuredClone(real) as { compiled_stage: unknown; generation_index: number; iteration_index: number; state: PipelineV2RunState };
          mutate(clone);
          return clone as unknown as { compiled_stage: unknown; generation_index: number; iteration_index: number; state: PipelineV2RunState } & Record<string, unknown> as never;
        },
      });
      const mutatedRecord = (state: PipelineV2RunState, position: number, mutate: (record: Record<string, unknown>) => void): void => {
        const record = state.generations[position] as unknown as Record<string, unknown>;
        (state.generations as unknown as unknown[])[position] = { ...record, ...mutatedFields(record, mutate) };
      };
      const mutatedFields = (record: Record<string, unknown>, mutate: (target: Record<string, unknown>) => void): Record<string, unknown> => {
        const target: Record<string, unknown> = { ...record };
        mutate(target);
        return target;
      };
      const cases: Array<[string, (result: { compiled_stage: unknown; generation_index: number; iteration_index: number; state: PipelineV2RunState }) => void]> = [
        ["compiled stage clone", (result) => { (result as unknown as Record<string, unknown>)["compiled_stage"] = structuredClone(ctx.compiledPlan.stages[0]); }],
        ["generation index", (result) => { result.generation_index = 3; }],
        ["iteration index", (result) => { result.iteration_index = 2; }],
        ["closed final generation", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["closed"] = { by: "next_stage", closed_transition_count: 2 };
          });
        }],
        ["stage position", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["stage_position"] = 5;
          });
        }],
        ["template id", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["template_id"] = "other";
          });
        }],
        ["plan digest", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["plan_sha256"] = hex("9");
          });
        }],
        ["initial budget", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["initial_budget"] = 7;
          });
        }],
        ["opening anchor", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["opened_transition_count"] = 3;
          });
        }],
        ["missing iteration", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            record["iteration_count"] = 0;
            record["iterations"] = [];
            record["open_iteration"] = undefined;
          });
        }],
        ["closed iteration", (result) => {
          mutatedRecord(result.state, 1, (record) => {
            const iteration = (record["iterations"] as Record<string, unknown>[])[0];
            (record["iterations"] as unknown[])[0] = {
              ...iteration,
              closed: { by: "normal_close", closed_transition_count: 2 },
            };
            record["open_iteration"] = undefined;
          });
        }],
        ["old generation changed", (result) => {
          mutatedRecord(result.state, 0, (record) => {
            record["stage_id"] = "stage-9";
          });
        }],
        ["boundary advanced: transitions", (result) => {
          (result.state.transitions as unknown[]).push({ index: 0, from: "architect", outcome: "completed", to: "dev_entry", execution_index: 3 });
        }],
        ["boundary advanced: executions", (result) => {
          (result.state.executions as unknown[]).push({ index: 4, type: "agent", state_id: "dev_entry", execution_role: "stage", phase: "started", iteration_index: 1, attempt: 1, profile: "coder" });
        }],
        ["boundary advanced: waits", (result) => {
          (result.state.waits as unknown[]).push({ index: 2, transition_count: 2, state_id: "dev_entry", reason: "stage_iteration_limit_exhausted", request_sha256: hex("a"), actions: [] });
        }],
        ["state null", (result) => { (result as unknown as Record<string, unknown>)["state"] = null; }],
        ["generations malformed", (result) => { (result.state as unknown as Record<string, unknown>)["generations"] = null; }],
      ];
      for (const [label, mutate] of cases) {
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(makeOps(mutate), {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason, label).toBe("invalid_result");
      }
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. a coherent hostile ensure result with simultaneously mutated fields and state is detected", async () => {
    const ctx = await replannedStageReady();
    try {
      const makeOps = (): PipelineV2ReplannedStageOps => ({
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async (options) => {
          const real = await ensurePipelineV2StageIteration(options);
          const clone = structuredClone(real) as unknown as { generation_index: number; state: PipelineV2RunState } & Record<string, unknown>;
          const generation = clone.state.generations[1] as unknown as Record<string, unknown>;
          (clone.state.generations as unknown as unknown[])[1] = { ...generation, index: 5 };
          clone.generation_index = 5;
          return clone as never;
        },
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(makeOps(), {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. option getter counts, ops getter counts and caller mutation isolation", async () => {
    const ctx = await replannedStageReady();
    try {
      const reads: Record<string, number> = {};
      const proxiedOptions = {
        get sink() {
          reads["sink"] = (reads["sink"] ?? 0) + 1;
          return ctx.sink;
        },
        get intent() {
          reads["intent"] = (reads["intent"] ?? 0) + 1;
          return ctx.intent;
        },
        get compiledPlan() {
          reads["compiledPlan"] = (reads["compiledPlan"] ?? 0) + 1;
          return ctx.compiledPlan;
        },
        get stageId() {
          reads["stageId"] = (reads["stageId"] ?? 0) + 1;
          return "stage-1";
        },
        get initialBudget() {
          reads["initialBudget"] = (reads["initialBudget"] ?? 0) + 1;
          return 2;
        },
      };
      let closeGenerationReads = 0;
      let ensureStageIterationReads = 0;
      const proxiedOps: PipelineV2ReplannedStageOps = {
        get closeGeneration() {
          closeGenerationReads += 1;
          return closePipelineV2ReplannedGeneration;
        },
        get ensureStageIteration() {
          ensureStageIterationReads += 1;
          return ensurePipelineV2StageIteration;
        },
      };
      const result = await openPipelineV2ReplannedStageWithOps(proxiedOps, proxiedOptions);
      for (const key of ["sink", "intent", "compiledPlan", "stageId", "initialBudget"]) {
        expect(reads[key], key).toBe(1);
      }
      expect(closeGenerationReads).toBe(1);
      expect(ensureStageIterationReads).toBe(1);
      expect(result.generation_index).toBe(2);
      // Caller mutation isolation: mutating the options object after the
      // composition cannot influence a second run's captured values.
      const mutableOptions = {
        sink: ctx.sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      };
      let resolveClose: (() => void) | undefined;
      const slowOps: PipelineV2ReplannedStageOps = {
        closeGeneration: async (options) => {
          await new Promise<void>((resolve) => {
            resolveClose = resolve;
          });
          return await closePipelineV2ReplannedGeneration(options);
        },
        ensureStageIteration: ensurePipelineV2StageIteration,
      };
      const pending = openPipelineV2ReplannedStageWithOps(slowOps, mutableOptions);
      await new Promise<void>((resolve) => setTimeout(resolve, 1));
      mutableOptions.initialBudget = 999;
      mutableOptions.stageId = "stage-9";
      resolveClose?.();
      const reopened = await pending;
      expect(reopened.initial_budget).toBe(2);
      expect(reopened.stage_id).toBe("stage-1");
      expect(reopened.state.generations[1]!.initial_budget).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. downstream and unexpected errors pass through by identity", async () => {
    const ctx = await replannedStageReady();
    try {
      const closeCanary = new Error("close canary");
      const closeThrowingOps: PipelineV2ReplannedStageOps = {
        closeGeneration: async () => {
          throw closeCanary;
        },
        ensureStageIteration: async () => {
          throw new Error("ensure must not be called");
        },
      };
      const closeCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(closeThrowingOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(closeCause).toBe(closeCanary);
      const ensureCanary = new Error("ensure canary");
      const ensureThrowingOps: PipelineV2ReplannedStageOps = {
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async () => {
          throw ensureCanary;
        },
      };
      const ensureCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ensureThrowingOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(ensureCause).toBe(ensureCanary);
      // A typed downstream failure from the real close controller (the
      // poisoned sink) passes through by identity.
      const poisoned: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: true,
        dispatch: ctx.sink.dispatch.bind(ctx.sink),
      };
      const closeTypedCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: poisoned,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(closeTypedCause).toBeInstanceOf(PipelineV2ReplannedGenerationControllerError);
      expect((closeTypedCause as PipelineV2ReplannedGenerationControllerError).reason).toBe("invalid_state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. diagnostics are content-free across the failure paths", async () => {
    const ctx = await replannedStageReady();
    try {
      const realResult = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(
          {
            closeGeneration: async () => {
              const clone = structuredClone(realResult) as unknown as Record<string, unknown>;
              clone["intent_sha256"] = ctx.intent.sha256.slice(0, 32) + "0".repeat(32);
              return clone as never;
            },
            ensureStageIteration: async () => {
              throw new Error("ensure must not be called");
            },
          },
          {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          },
        ),
      );
      const error = expectOpenError(cause);
      const message = error.message;
      expect(message).not.toContain(ctx.intent.sha256.slice(0, 32));
      expect(message).not.toContain("Body A");
      expect(message).not.toContain(ctx.fixture.runRoot);
      expect(message).not.toContain("canonical_json");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. the prefix C2-bare composition retry succeeds without changing the historical prefix", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      expect(before.generations).toHaveLength(2);
      const first = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(first.generation_index).toBe(2);
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 2, failStep: "rename" }),
        now: nextTick,
      });
      const faultCause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
          sink: faultedSink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      // The ensure step's typed store failure passes through by identity.
      expect(faultCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((faultCause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: retrySink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(result.previous_generation_index).toBe(2);
      expect(result.generation_index).toBe(3);
      const after = retrySink.snapshot as PipelineV2RunState;
      expect(after.generations).toHaveLength(3);
      // The historical prefix stays byte-identical.
      expect(after.generations[0]!.closed?.by).toBe("next_stage");
      expect(after.generations[1]!.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
      expect(after.generations[2]!.index).toBe(3);
      expect(after.generations[2]!.open_iteration).toEqual({ index: 1, opened_transition_count: 4 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. the prefix C2-open full composition is a zero-dispatch recognition on repeat", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await openPipelineV2ReplannedStageWithOps(productionReplannedStageOps, {
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      });
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect(result.previous_generation_index).toBe(2);
      expect(result.generation_index).toBe(3);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(3);
      expect(state.generations[2]!.open_iteration).toEqual({ index: 1, opened_transition_count: 4 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a hostile extra generation in the prefix scenario is refused by the close controller identity", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const third = state.generations[2]!;
        (state.generations as unknown as unknown[])[2] = {
          ...third,
          closed: { by: "next_stage", closed_transition_count: 4 },
          open_iteration: undefined,
          iterations: [{ index: 1, opened_transition_count: 4, closed: { by: "normal_close", closed_transition_count: 4 } }],
        };
        (state.generations as unknown as unknown[]).push({
          index: 4,
          stage_id: "stage-1",
          stage_position: 1,
          template_id: "development",
          plan_sha256: ctx.plan2.sha256,
          initial_budget: 2,
          opened_transition_count: 4,
          iteration_count: 0,
          iterations: [],
        });
      });
      let ensureCalls = 0;
      const ops = {
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async () => {
          ensureCalls += 1;
          throw new Error("ensure must not be called");
        },
      } as unknown as PipelineV2ReplannedStageOps;
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: hostile,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReplannedGenerationControllerError);
      expect((cause as PipelineV2ReplannedGenerationControllerError).reason).toBe("lifecycle_conflict");
      expect(ensureCalls).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. an ensure result with a changed revision at otherwise coherent fields is invalid_result", async () => {
    const ctx = await replannedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      void revisionBefore;
      const ops = ensureOpsWith(async () => {
        const ensured = await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
        (clone as unknown as Record<string, unknown>)["revision"] = ensured.state.revision + 1;
        return { ...ensured, state: clone };
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. an ensure result with a changed pipeline identity is invalid_result", async () => {
    const ctx = await replannedStageReady();
    try {
      const ops = ensureOpsWith(async () => {
        const ensured = await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
        ((clone as unknown as Record<string, unknown>)["pipeline"] as unknown as Record<string, unknown>)["entry_state"] = "dev_entry";
        return { ...ensured, state: clone };
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. a C2-bare hostile ensure cannot heal the generation binding under the caller-selected budget", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      // The bare new generation carries budget 5, not the caller-selected 2.
      await ctx.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: ctx.plan2.sha256,
        initialBudget: 5,
        transitionCount: 4,
      });
      const ops = ensureOpsWith(async () => {
        const ensured = await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 5, sink: ctx.sink });
        const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
        (clone.generations[2] as unknown as Record<string, unknown>)["initial_budget"] = 2;
        return { ...ensured, state: clone };
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. a C2-bare hostile ensure cannot heal plan digest, template or anchor bindings", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ctx.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: ctx.plan2.sha256,
        initialBudget: 2,
        transitionCount: 4,
      });
      const ops = ensureOpsWith(async () => {
        const ensured = await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
        const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
        (clone.generations[2] as unknown as Record<string, unknown>)["plan_sha256"] = FAKE_DIGEST;
        (clone.generations[2] as unknown as Record<string, unknown>)["template_id"] = "other-template";
        (clone.generations[2] as unknown as Record<string, unknown>)["opened_transition_count"] = 3;
        return { ...ensured, state: clone };
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. a C2-open hostile ensure rewriting an immutable generation binding is invalid_result", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      // The fake ensure wraps the real result shape with the exact
      // trusted compiled stage and a mutated durable state snapshot.
      const compiledStage = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, "stage-1");
      const hostileOps = {
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async () => {
          const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
          (clone.generations[2] as unknown as Record<string, unknown>)["stage_id"] = "stage-9";
          return { compiled_stage: compiledStage, generation_index: 3, iteration_index: 1, state: clone };
        },
      } as unknown as PipelineV2ReplannedStageOps;
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(hostileOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("36. a changed result iteration_index is invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const ops = mutatedCloseOps(ctx, (result) => {
        result["iteration_index"] = 2;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("37. a coherent hostile task id and digest off the intent manifest are invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const ops = mutatedCloseOps(ctx, (result, state) => {
        result["task_id"] = "task-b";
        result["task_sha256"] = FAKE_DIGEST;
        const lastTask = state.task_revisions[state.task_revisions.length - 1] as unknown as Record<string, unknown>;
        lastTask["task_id"] = "task-b";
        lastTask["sha256"] = FAKE_DIGEST;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("38. a changed predecessor task digest is invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const ops = mutatedCloseOps(ctx, (_result, state) => {
        const lastTask = state.task_revisions[state.task_revisions.length - 1] as unknown as Record<string, unknown>;
        lastTask["previous_sha256"] = FAKE_DIGEST;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("39. a broken plan predecessor digest is invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const ops = mutatedCloseOps(ctx, (_result, state) => {
        const lastPlan = state.plan_revisions[state.plan_revisions.length - 1] as unknown as Record<string, unknown>;
        lastPlan["previous_sha256"] = FAKE_DIGEST;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(ops, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("40. broken iteration count, last-iteration index or an open projection are invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      const variants: Array<(state: PipelineV2RunState) => void> = [
        (state) => {
          (state.generations[0] as unknown as Record<string, unknown>)["iteration_count"] = 2;
        },
        (state) => {
          const lastIteration = (state.generations[0] as unknown as Record<string, unknown>)["iterations"] as unknown[];
          (lastIteration[0] as unknown as Record<string, unknown>)["index"] = 2;
        },
        (state) => {
          (state.generations[0] as unknown as Record<string, unknown>)["open_iteration"] = { index: 1, opened_transition_count: 2 };
        },
      ];
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      for (const variant of variants) {
        const ops = mutatedCloseOps(ctx, (_result, state) => variant(state));
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(ops, {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason).toBe("invalid_result");
        expect(ops.ensureCalls()).toBe(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("41. a C2-bare hostile ensure preserving a mismatching caller budget never succeeds", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      // The bare new generation carries budget 5 while the caller
      // selects 2; the hostile injected ensure preserves the stored
      // budget, adds the exact iteration 1 and returns a success with
      // the correct compiled stage.
      await ctx.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: ctx.plan2.sha256,
        initialBudget: 5,
        transitionCount: 4,
      });
      const compiledStage = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, "stage-1");
      const hostileOps = {
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async () => {
          await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 5, sink: ctx.sink });
          const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
          expect(clone.generations[2]!.initial_budget).toBe(5);
          return { compiled_stage: compiledStage, generation_index: 3, iteration_index: 1, state: clone };
        },
      } as unknown as PipelineV2ReplannedStageOps;
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(hostileOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("42. a C2-open hostile ensure preserving mismatching stage and budget bindings never succeeds", async () => {
    const ctx = await replannedStageReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ctx.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: ctx.plan2.sha256,
        initialBudget: 5,
        transitionCount: 4,
      });
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 3, iterationIndex: 1, transitionCount: 4 });
      const compiledStage = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, "stage-1");
      // The fake ensure returns an UNCHANGED success result.
      const hostileOps = {
        closeGeneration: closePipelineV2ReplannedGeneration,
        ensureStageIteration: async () => {
          const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
          return { compiled_stage: compiledStage, generation_index: 3, iteration_index: 1, state: clone };
        },
      } as unknown as PipelineV2ReplannedStageOps;
      const cause = await catchOpen(() =>
        openPipelineV2ReplannedStageWithOps(hostileOps, {
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          stageId: "stage-1",
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("43. a malformed wait action declaration is invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      const variants: Array<(state: PipelineV2RunState) => void> = [
        (state) => {
          (state.waits[state.waits.length - 1] as unknown as Record<string, unknown>)["actions"] = null;
        },
        (state) => {
          const lastWait = state.waits[state.waits.length - 1] as unknown as Record<string, unknown>;
          ((lastWait["actions"] as unknown) as unknown[])[0] = null;
        },
      ];
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      for (const variant of variants) {
        const ops = mutatedCloseOps(ctx, (_result, state) => variant(state));
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(ops, {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason).toBe("invalid_result");
        expect(error.message).not.toContain("Body A");
        expect(ops.ensureCalls()).toBe(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("44. malformed inputs, transitions and executions journals are invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      const variants: Array<(state: PipelineV2RunState) => void> = [
        (state) => {
          (state as unknown as Record<string, unknown>)["inputs"] = null;
        },
        (state) => {
          (state.inputs as unknown as unknown[])[0] = null;
        },
        (state) => {
          (state as unknown as Record<string, unknown>)["transitions"] = null;
        },
        (state) => {
          (state.transitions as unknown as unknown[])[0] = null;
        },
        (state) => {
          (state as unknown as Record<string, unknown>)["executions"] = null;
        },
        (state) => {
          (state.executions as unknown as unknown[])[0] = null;
        },
      ];
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      for (const variant of variants) {
        const ops = mutatedCloseOps(ctx, (_result, state) => variant(state));
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(ops, {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason).toBe("invalid_result");
        expect(ops.ensureCalls()).toBe(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("45. malformed grants, early plan records, cursor, pipeline and the predecessor revision are invalid_result with zero ensure calls", async () => {
    const ctx = await replannedStageReady();
    try {
      const variants: Array<(result: Record<string, unknown>, state: PipelineV2RunState) => void> = [
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["grants"] = null;
        },
        (_result, state) => {
          (state.plan_revisions as unknown as unknown[])[0] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["cursor"] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["pipeline"] = null;
        },
        (result, state) => {
          // Coherent: the result names revision 2 and the early record
          // agrees — but it is not the accepted revision minus one.
          result["previous_plan_revision"] = 2;
          (state.plan_revisions[0] as unknown as Record<string, unknown>)["index"] = 2;
          (state.plan_revisions[0] as unknown as Record<string, unknown>)["revision"] = 2;
        },
      ];
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      for (const variant of variants) {
        const ops = mutatedCloseOps(ctx, (result, state) => variant(result, state));
        const cause = await catchOpen(() =>
          openPipelineV2ReplannedStageWithOps(ops, {
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            stageId: "stage-1",
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason).toBe("invalid_result");
        expect(ops.ensureCalls()).toBe(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

test("26. the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_replanned_stage_controller.ts");
  const internalModule = await import("../src/pipeline_v2_replanned_stage_controller_internal.ts");
  expect(Object.keys(publicModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedStageControllerError",
    "openPipelineV2ReplannedStage",
  ]);
  expect(Object.keys(internalModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedStageControllerError",
    "openPipelineV2ReplannedStageWithOps",
    "productionReplannedStageOps",
  ]);
  expect(Object.isFrozen(productionReplannedStageOps)).toBe(true);
  expect(Object.keys(productionReplannedStageOps).sort()).toEqual(["closeGeneration", "ensureStageIteration"]);
  expect(productionReplannedStageOps.closeGeneration).toBe(closePipelineV2ReplannedGeneration);
  expect(productionReplannedStageOps.ensureStageIteration).toBe(ensurePipelineV2StageIteration);
});

test("27. the composition module imports only the composed layers (source scan)", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile("orchestrator/src/pipeline_v2_replanned_stage_controller_internal.ts", "utf8");
  const countOf = (needle: string): number => source.split(needle).length - 1;
  expect(countOf("reducePipelineV2RunCommand")).toBe(0);
  expect(countOf("validatePipelineV2RunState")).toBe(0);
  expect(countOf("hasPreparedRunPlanProvenance")).toBe(0);
  expect(countOf("hasCompiledRunPlanProvenance")).toBe(0);
  expect(countOf("JSON.stringify")).toBe(0);
  expect(countOf("createHash")).toBe(0);
  expect(countOf("CryptoHasher")).toBe(0);
  expect(countOf("new WeakMap")).toBe(0);
  expect(countOf("new WeakSet")).toBe(0);
  expect(countOf("structuredClone")).toBe(0);
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf("pipeline_v2_state_store")).toBe(0);
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
  const imports = [...source.matchAll(/from "([^"]+)"/g)].map((match) => match[1]);
  expect(imports.sort()).toEqual([
    "./pipeline_v2_freeze_internal.ts",
    "./pipeline_v2_identity_compare.ts",
    "./pipeline_v2_replanned_generation_controller.ts",
    "./pipeline_v2_run_plan_compiled.ts",
    "./pipeline_v2_run_plan_manifests.ts",
    "./pipeline_v2_stage_iteration_controller.ts",
    "./pipeline_v2_state.ts",
  ]);
  expect(countOf("import ")).toBe(7);
});
