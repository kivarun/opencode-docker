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
import { preparePipelineV2RunPlanCandidate, type PreparedPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  compilePipelineV2RunPlanCandidate,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration, closePipelineV2StageIteration, PipelineV2StageIterationControllerError } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskClosure } from "../src/pipeline_v2_revise_task_closure_controller.ts";
import { completePipelineV2ReviseTask } from "../src/pipeline_v2_revise_task_completion_controller.ts";
import {
  closePipelineV2ReplannedGeneration,
  PipelineV2ReplannedGenerationControllerError,
  type PipelineV2ReplannedGenerationControllerFailureReason,
  type PipelineV2ReplannedGenerationControllerSink,
} from "../src/pipeline_v2_replanned_generation_controller.ts";
import { closePipelineV2ReplannedGenerationInternal } from "../src/pipeline_v2_replanned_generation_controller_internal.ts";
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-replanned-gen-"));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-replanned-gen-run-"));
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

const A3 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-a",
  revision: 3,
  previous_sha256: A2.sha256,
  origin: "user_response",
  body: "Body A third",
});

const B1 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-b",
  revision: 1,
  previous_sha256: null,
  origin: "planning_proposal",
  body: "Body B",
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

const INTENT2 = prepareWaitIntent({
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: RUN_ID,
  wait_index: 1,
  task_id: "task-a",
  expected_previous_task_sha256: A1.sha256,
  new_task_revision_sha256: A3.sha256,
});

interface ReplannedReadyOptions {
  withoutTaskRevision?: boolean;
  withoutPlanAcceptance?: boolean;
  withLaterTaskRevision?: boolean;
  withForeignPlanTask?: boolean;
  withShiftedIntent?: boolean;
  withContinueResponse?: boolean;
  withLaterWait?: boolean;
  withPrefixGeneration?: boolean;
}

interface ReplannedCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  compiledPlan: CompiledPipelineV2RunPlan;
  plan1: ReturnType<typeof preparePlanRevisionManifest>;
  plan2: ReturnType<typeof preparePlanRevisionManifest>;
  candidate2: PreparedPipelineV2RunPlanCandidate;
}

/**
 * The real reducer/sink/store/controllers path to the replanned-generation
 * boundary: the plan r1 accepted, the generation/iteration opened, the
 * revise intent accepted durably, the task r2 durably accepted, the
 * iteration closed `by:"replanned"`, the response recorded through the
 * existing completion controller, the settled planning execution on the
 * declared action target, and the next plan r2 (carrying exactly task-a
 * r2) accepted through `acceptPipelineV2RunPlanCandidate`. The negative
 * variants omit exactly one durable step and use the raw reducer
 * commands where the composition controllers would refuse by contract.
 */
async function replannedReady(options: ReplannedReadyOptions = {}): Promise<ReplannedCtx> {
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
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions,
    });
    if (options.withoutTaskRevision !== true) {
      await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
    }
    const intentUsed = options.withShiftedIntent === true ? INTENT2 : INTENT;
    await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: intentUsed.sha256 });
    if (options.withoutTaskRevision !== true) {
      await sink.dispatch({
        kind: "task_revision_accepted",
        taskId: "task-a",
        revision: 2,
        taskSha256: A2.sha256,
        waitIndex: 1,
        intentSha256: intentUsed.sha256,
      });
    }
    if (options.withShiftedIntent === true) {
      // The shifted-intent variant continues the whole revise flow under
      // INTENT2 (a revision-3 task chain), so the durable intent differs
      // from the caller's prepared intent while everything else holds.
      await sink.dispatch({
        kind: "task_revision_accepted",
        taskId: "task-a",
        revision: 3,
        taskSha256: A3.sha256,
        waitIndex: 1,
        intentSha256: intentUsed.sha256,
      });
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 });
      await sink.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: request.sha256,
        actionId: "revise_task",
        responseSha256: hex("f"),
      });
    } else if (options.withLaterTaskRevision === true) {
      await sink.dispatch({
        kind: "task_revision_accepted",
        taskId: "task-a",
        revision: 3,
        taskSha256: A3.sha256,
        waitIndex: 1,
        intentSha256: intentUsed.sha256,
      });
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 });
      await sink.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: request.sha256,
        actionId: "revise_task",
        responseSha256: hex("f"),
      });
    } else if (options.withContinueResponse === true) {
      await applyPipelineV2ReviseTaskClosure({ sink, intent: intentUsed });
      await sink.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: request.sha256,
        actionId: "continue_stage",
        responseSha256: hex("f"),
      });
    } else if (options.withoutTaskRevision !== true) {
      await applyPipelineV2ReviseTaskClosure({ sink, intent: intentUsed });
      await completePipelineV2ReviseTask({ runRoot: fixture.runRoot, sink, intent: intentUsed });
    }
    const planningExecutionIndex = options.withPrefixGeneration === true ? 5 : 3;
    const preparedPlan2Manifest = {
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
          tasks: options.withForeignPlanTask === true
            ? [{ id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] }]
            : [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
        },
      ],
    } as const;
    const plan2 = preparePlanRevisionManifest(preparedPlan2Manifest);
    const candidate2 = preparePipelineV2RunPlanCandidate({
      plan: plan2,
      taskRevisions: options.withForeignPlanTask === true ? [B1] : [A2],
      previousPlan: plan1,
      previousTaskRevisions: options.withForeignPlanTask === true ? [] : [A1],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    if (options.withoutTaskRevision === true) {
      // No response and no planning execution: the boundary is left before
      // the accepted task revision exists (the run stays waiting).
      return {
        fixture,
        sink,
        pipeline,
        intent: intentUsed,
        compiledPlan: compilePipelineV2RunPlanCandidate(pipeline, candidate2),
        plan1,
        plan2,
        candidate2,
      };
    }
    if (options.withLaterTaskRevision === true || options.withShiftedIntent === true || options.withContinueResponse === true) {
      // These variants stop before the plan acceptance; the compiled plan
      // stays a provenance-backed projection of the prepared candidate.
      return { fixture, sink, pipeline, intent: intentUsed, compiledPlan: compilePipelineV2RunPlanCandidate(pipeline, candidate2), plan1, plan2, candidate2 };
    }
    // The settled planning execution on the declared revise_task target.
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning3")) {
      await sink.dispatch(command);
    }
    let compiledPlan: CompiledPipelineV2RunPlan;
    if (options.withoutPlanAcceptance === true) {
      compiledPlan = compilePipelineV2RunPlanCandidate(pipeline, candidate2);
    } else {
      const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidate2 });
      compiledPlan = accepted.compiled_plan;
    }
    if (options.withLaterWait === true) {
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      const request2 = preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 2,
        transition_count: 3,
        state_id: "dev_entry",
        reason: "stage_iteration_limit_exhausted",
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
      await sink.dispatch({
        kind: "run_waiting",
        stateId: "dev_entry",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: request2.sha256,
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
    }
    return { fixture, sink, pipeline, intent: intentUsed, compiledPlan, plan1, plan2, candidate2 };
  });
}

/**
 * The real reducer path to a stale successor chain: a second plan
 * revision (r3) is accepted from the same settled planning execution, so
 * the immediately preceding durable plan record (r2) is no longer the old
 * generation's plan (r1).
 */
async function replannedReadyExtraPlanRevision(): Promise<ReplannedCtx & { plan3: ReturnType<typeof preparePlanRevisionManifest> }> {
  const ctx = await replannedReady();
  const plan3 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 3,
    previous_sha256: ctx.plan2.sha256,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 3,
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
      },
    ],
  });
  const candidate3 = preparePipelineV2RunPlanCandidate({
    plan: plan3,
    taskRevisions: [A2],
    previousPlan: ctx.plan2,
    previousTaskRevisions: [A1],
    protectedInputDigest: PROTECTED_DIGEST,
  });
  await ctx.sink.dispatch({
    kind: "plan_revision_accepted",
    planRevision: 3,
    planSha256: plan3.sha256,
    originExecution: 3,
  });
  const compiledPlan = compilePipelineV2RunPlanCandidate(ctx.pipeline, candidate3);
  return { ...ctx, compiledPlan, plan3 };
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
      commands.push(command);
      await inner.dispatch(command);
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

async function catchClose(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

/**
 * A sink whose post-dispatch snapshot is a narrow `structuredClone`
 * mutation of the REAL successful reducer-produced state (built after
 * the actual dispatch); the racing variant additionally throws
 * `PipelineV2StateError` after applying the command.
 */
function mutateAfterDispatchSink(
  inner: PipelineV2RunStateSink,
  mutate: (state: PipelineV2RunState) => void,
  throwRacing = false,
): { sink: PipelineV2ReplannedGenerationControllerSink; dispatchCount: () => number } {
  let mutateNext = false;
  let dispatchCount = 0;
  const sink: PipelineV2ReplannedGenerationControllerSink = {
    get snapshot(): PipelineV2RunState | null {
      const snapshot = inner.snapshot;
      if (snapshot === null || !mutateNext) {
        return snapshot;
      }
      const clone = structuredClone(snapshot) as PipelineV2RunState;
      mutate(clone);
      return clone;
    },
    poisoned: inner.poisoned,
    async dispatch(command: PipelineV2RunCommand) {
      dispatchCount += 1;
      await inner.dispatch(command);
      if (throwRacing) {
        mutateNext = true;
        throw new PipelineV2StateError("racing injected");
      }
      mutateNext = true;
    },
  };
  return { sink, dispatchCount: () => dispatchCount };
}

function expectCloseError(
  cause: unknown,
  reason: PipelineV2ReplannedGenerationControllerFailureReason,
): PipelineV2ReplannedGenerationControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReplannedGenerationControllerError);
  const error = cause as PipelineV2ReplannedGenerationControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

describe("closePipelineV2ReplannedGeneration", () => {
  test("1. C0 happy path: one exact command, revision +1, loader round-trip", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 1);
      expect(recording.commands).toEqual([
        { kind: "stage_generation_closed", generationIndex: 1, by: "replanned" },
      ]);
      const generation = state.generations[0]!;
      expect(generation.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      expect(generation.index).toBe(1);
      const round = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      expect(round.generations[0]!.closed?.by).toBe("replanned");
      expect(round.status).toBe("active");
      expect(result).toMatchObject({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: A2.sha256,
        previous_plan_revision: 1,
        previous_plan_sha256: ctx.plan1.sha256,
        plan_revision: 2,
        plan_sha256: ctx.plan2.sha256,
        origin_execution: 3,
      });
      expect(result.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the result is deep-frozen and content-free", async () => {
    const ctx = await replannedReady();
    try {
      const result = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(Object.keys(result).sort()).toEqual([
        "generation_index",
        "intent_sha256",
        "iteration_index",
        "origin_execution",
        "plan_revision",
        "plan_sha256",
        "previous_plan_revision",
        "previous_plan_sha256",
        "state",
        "task_id",
        "task_revision",
        "task_sha256",
        "wait_index",
      ]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      const serialized = JSON.stringify(result);
      expect(serialized).not.toContain("Body A");
      expect(serialized).not.toContain(ctx.fixture.runRoot);
      expect(serialized).not.toContain("canonical_json");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. C1 exact retry: zero dispatch, the same verified snapshot", async () => {
    const ctx = await replannedReady();
    try {
      const first = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect(result.intent_sha256).toBe(first.intent_sha256);
      expect(result.state.revision).toBe(revisionBefore);
      expect(result.task_revision).toBe(first.task_revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the next plan is not accepted yet: plan_conflict", async () => {
    const ctx = await replannedReady({ withoutPlanAcceptance: true });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "plan_conflict");
      expect(error.message).toContain("not the last durable plan revision");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the accepted plan is not the exact successor of the generation plan: plan_conflict", async () => {
    const ctx = await replannedReadyExtraPlanRevision();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "plan_conflict");
      expect(error.message).toContain("not the exact successor of the replanned generation's plan");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. a stale task revision or a missing revised task in the compiled plan is plan_conflict", async () => {
    const stale = await replannedReady();
    try {
      const plan2Stale = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 2,
        previous_sha256: stale.plan1.sha256,
        root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
        origin_execution: 3,
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
          },
        ],
      });
      const staleCandidate = preparePipelineV2RunPlanCandidate({
        plan: plan2Stale,
        taskRevisions: [A1],
        previousPlan: stale.plan1,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      });
      const stalePlan = compilePipelineV2RunPlanCandidate(stale.pipeline, staleCandidate);
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: stale.sink, intent: stale.intent, compiledPlan: stalePlan }),
      );
      expectCloseError(cause, "plan_conflict");
    } finally {
      await disposeRun(stale.fixture);
    }
    const foreignTask = await replannedReady({ withForeignPlanTask: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: foreignTask.sink, intent: foreignTask.intent, compiledPlan: foreignTask.compiledPlan }),
      );
      const error = expectCloseError(cause, "plan_conflict");
      expect(error.message).toContain("does not carry the accepted task revision exactly once");
    } finally {
      await disposeRun(foreignTask.fixture);
    }
  });

  test("7. a foreign compiled-plan pipeline identity is a lifecycle_conflict", async () => {
    const ctx = await replannedReady();
    try {
      const foreignPlan = await withPipeline(async (foreignPipeline) =>
        compilePipelineV2RunPlanCandidate(foreignPipeline, ctx.candidate2),
      );
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: foreignPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("bundle_root");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. fake, cloned and Proxy-wrapped compiled plans are rejected with plan_conflict and zero traps", async () => {
    const ctx = await replannedReady();
    try {
      for (const fake of [
        { ...(ctx.compiledPlan as unknown as Record<string, unknown>) } as unknown as CompiledPipelineV2RunPlan,
        structuredClone(ctx.compiledPlan) as unknown as CompiledPipelineV2RunPlan,
      ]) {
        const cause = await catchClose(() =>
          closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: fake }),
        );
        expectCloseError(cause, "plan_conflict");
      }
      let traps = 0;
      const proxyPlan = new Proxy(ctx.compiledPlan, {
        get(target, prop, receiver) {
          traps += 1;
          return Reflect.get(target, prop, receiver);
        },
      });
      const proxyCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: proxyPlan }),
      );
      expectCloseError(proxyCause, "plan_conflict");
      expect(traps).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. fake, cloned and Proxy-wrapped intents are rejected with invalid_intent and zero traps", async () => {
    const ctx = await replannedReady();
    try {
      for (const fake of [
        { ...(ctx.intent as unknown as Record<string, unknown>) } as unknown as PreparedPipelineV2RunWaitIntent,
        structuredClone(ctx.intent) as unknown as PreparedPipelineV2RunWaitIntent,
      ]) {
        const cause = await catchClose(() =>
          closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: fake, compiledPlan: ctx.compiledPlan }),
        );
        expectCloseError(cause, "invalid_intent");
      }
      let traps = 0;
      const proxyIntent = new Proxy(ctx.intent, {
        get(target, prop, receiver) {
          traps += 1;
          return Reflect.get(target, prop, receiver);
        },
      });
      const proxyCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: proxyIntent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(proxyCause, "invalid_intent");
      expect(traps).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a continue_stage intent kind is rejected with invalid_intent", async () => {
    const ctx = await replannedReady({ withoutTaskRevision: true });
    try {
      const continueIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: ctx.plan1.sha256,
        additional_iterations: 1,
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: continueIntent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "invalid_intent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. the whole revise flow under a different intent is invalid_intent at the durable wait binding", async () => {
    const ctx = await replannedReady({ withShiftedIntent: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: INTENT, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "invalid_intent");
      expect(error.message).toContain("exact accepted revise intent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. a wait answered with the continue_stage action is invalid_state at the response binding", async () => {
    const ctx = await replannedReady({ withContinueResponse: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "invalid_state");
      expect(error.message).toContain("exact revise_task response");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. a duplicate wait index is typed invalid_state", async () => {
    const ctx = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const waits = state.waits as unknown as unknown[];
        waits.splice(0, 0, structuredClone(waits[0]));
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a missing, conflicting or later wait-bound task revision is typed", async () => {
    const absent = await replannedReady({ withoutTaskRevision: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: absent.sink, intent: absent.intent, compiledPlan: absent.compiledPlan }),
      );
      const error = expectCloseError(cause, "invalid_state");
      expect(error.message).toContain("not at an active post-response boundary");
    } finally {
      await disposeRun(absent.fixture);
    }
    const later = await replannedReady({ withLaterTaskRevision: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: later.sink, intent: later.intent, compiledPlan: later.compiledPlan }),
      );
      expectCloseError(cause, "revision_conflict");
    } finally {
      await disposeRun(later.fixture);
    }
    const conflicting = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(conflicting.sink, (state) => {
        const record = state.task_revisions[1]!;
        (state.task_revisions as unknown as unknown[])[1] = { ...record, sha256: hex("9") };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: conflicting.intent, compiledPlan: conflicting.compiledPlan }),
      );
      const error = expectCloseError(cause, "revision_conflict");
      expect(error.message).toContain("contradicts the revise intent");
    } finally {
      await disposeRun(conflicting.fixture);
    }
  });

  test("15. a wrong iteration closure reason is a lifecycle_conflict; a wrong anchor is loader-typed", async () => {
    const wrongReason = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(wrongReason.sink, (state) => {
        const generation = state.generations[0]!;
        const iteration = generation.iterations[generation.iterations.length - 1]!;
        (generation.iterations as unknown as unknown[])[generation.iterations.length - 1] = {
          ...iteration,
          closed: { by: "normal_close", closed_transition_count: 2 },
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: wrongReason.intent, compiledPlan: wrongReason.compiledPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("not the exact replanned closure of the target wait");
    } finally {
      await disposeRun(wrongReason.fixture);
    }
    const wrongAnchor = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(wrongAnchor.sink, (state) => {
        const generation = state.generations[0]!;
        const iteration = generation.iterations[generation.iterations.length - 1]!;
        (generation.iterations as unknown as unknown[])[generation.iterations.length - 1] = {
          ...iteration,
          closed: { by: "replanned", wait_index: 1, closed_transition_count: 3 },
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: wrongAnchor.intent, compiledPlan: wrongAnchor.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(wrongAnchor.fixture);
    }
  });

  test("16. a generation closed with another reason is a lifecycle_conflict; a wrong anchor is loader-typed", async () => {
    const wrongReason = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(wrongReason.sink, (state) => {
        const generation = state.generations[0]!;
        (state.generations as unknown as unknown[])[0] = {
          ...generation,
          closed: { by: "next_stage", closed_transition_count: 2 },
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: wrongReason.intent, compiledPlan: wrongReason.compiledPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("the generation closure is not the exact replanned closure");
    } finally {
      await disposeRun(wrongReason.fixture);
    }
    const wrongAnchor = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(wrongAnchor.sink, (state) => {
        const generation = state.generations[0]!;
        (state.generations as unknown as unknown[])[0] = {
          ...generation,
          closed: { by: "replanned", closed_transition_count: 3 },
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: wrongAnchor.intent, compiledPlan: wrongAnchor.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(wrongAnchor.fixture);
    }
  });

  test("17. a replaced generation plan binding is loader-typed; the opened new plan's generation is the C2 retry form", async () => {
    const replaced = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(replaced.sink, (state) => {
        const generation = state.generations[0]!;
        (state.generations as unknown as unknown[])[0] = { ...generation, plan_sha256: hex("5") };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: replaced.intent, compiledPlan: replaced.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(replaced.fixture);
    }
    const later = await replannedReady();
    try {
      const revisionBefore = (later.sink.snapshot as PipelineV2RunState).revision;
      await later.sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "replanned" });
      await later.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: later.plan2.sha256,
        initialBudget: 2,
        transitionCount: 2,
      });
      const recording = recordingSink(later.sink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: later.intent, compiledPlan: later.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect((later.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
      expect(result.generation_index).toBe(1);
      expect(result.iteration_index).toBe(1);
      expect(result.plan_revision).toBe(2);
      const state = later.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(2);
      expect(state.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      expect(state.generations[1]!.open_iteration).toBeUndefined();
      expect(state.generations[1]!.iteration_count).toBe(0);
    } finally {
      await disposeRun(later.fixture);
    }
  });

  test("17b. several generations of the current plan after the replanned closure are fail-closed", async () => {
    const ctx = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const generation = state.generations[0]!;
        (state.generations as unknown as unknown[])[0] = {
          ...generation,
          closed: { by: "replanned", closed_transition_count: 2 },
        };
        (state.generations as unknown as unknown[]).push({
          index: 2,
          stage_id: "stage-1",
          stage_position: 1,
          template_id: "development",
          plan_sha256: ctx.plan2.sha256,
          initial_budget: 2,
          opened_transition_count: 2,
          iteration_count: 0,
          iterations: [],
        });
        (state.generations as unknown as unknown[]).push({
          index: 3,
          stage_id: "stage-1",
          stage_position: 1,
          template_id: "development",
          plan_sha256: ctx.plan2.sha256,
          initial_budget: 2,
          opened_transition_count: 2,
          iteration_count: 0,
          iterations: [],
        });
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. a later transition or a later wait past the boundary is typed fail-closed", async () => {
    const laterTransition = await replannedReady();
    try {
      await laterTransition.sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: laterTransition.sink, intent: laterTransition.intent, compiledPlan: laterTransition.compiledPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("advanced past the wait boundary");
    } finally {
      await disposeRun(laterTransition.fixture);
    }
    const laterWait = await replannedReady({ withLaterWait: true });
    try {
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: laterWait.sink, intent: laterWait.intent, compiledPlan: laterWait.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(laterWait.fixture);
    }
  });

  test("19. the pre-check precedes the dispatch (source-order proof)", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_replanned_generation_controller_internal.ts"), "utf8");
    const precheck = source.indexOf("reducePipelineV2RunCommand(");
    const dispatch = source.indexOf("await dispatchBound(command)");
    expect(precheck).toBeGreaterThan(0);
    expect(dispatch).toBeGreaterThan(precheck);
  });

  test("20. a racing bare reducer rejection without any state change is a failure", async () => {
    const ctx = await replannedReady();
    try {
      const race = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: ctx.sink.poisoned,
        dispatch: () => {
          throw new PipelineV2StateError("racing bare rejection");
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: race, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "lifecycle_conflict");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a resolve-without-change dispatch is a lifecycle_conflict", async () => {
    const ctx = await replannedReady();
    try {
      let dispatches = 0;
      const silent: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: ctx.sink.poisoned,
        async dispatch(command: PipelineV2RunCommand) {
          dispatches += 1;
          void command;
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: silent, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "lifecycle_conflict");
      expect(dispatches).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. a racing exact closure is an idempotent success through the same verification", async () => {
    const ctx = await replannedReady();
    try {
      const race = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: ctx.sink.poisoned,
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          throw new PipelineV2StateError("racing exact closure");
        },
      };
      const result = await closePipelineV2ReplannedGeneration({ sink: race, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      expect(result.state.revision).toBe(state.revision);
      expect(result.generation_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. a racing hostile closure is a failure, never a success", async () => {
    const ctx = await replannedReady();
    try {
      const race = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: ctx.sink.poisoned,
        async dispatch(command: PipelineV2RunCommand) {
          void command;
          await ctx.sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
          throw new PipelineV2StateError("racing hostile closure");
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: race, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "lifecycle_conflict");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a wrong revision delta or a foreign run id in the post-dispatch snapshot is a lifecycle_conflict", async () => {
    const wrongDelta = await replannedReady();
    try {
      let mutateNext = false;
      const sink: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          const snapshot = wrongDelta.sink.snapshot;
          if (snapshot === null || !mutateNext) {
            return snapshot;
          }
          const clone = structuredClone(snapshot) as PipelineV2RunState;
          clone.revision = clone.revision + 1;
          return clone;
        },
        poisoned: wrongDelta.sink.poisoned,
        async dispatch(command: PipelineV2RunCommand) {
          await wrongDelta.sink.dispatch(command);
          mutateNext = true;
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink, intent: wrongDelta.intent, compiledPlan: wrongDelta.compiledPlan }),
      );
      expectCloseError(cause, "lifecycle_conflict");
    } finally {
      await disposeRun(wrongDelta.fixture);
    }
    const foreignRun = await replannedReady();
    try {
      let mutateNext = false;
      const sink: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          const snapshot = foreignRun.sink.snapshot;
          if (snapshot === null || !mutateNext) {
            return snapshot;
          }
          const clone = structuredClone(snapshot) as PipelineV2RunState;
          clone.run_id = "run-foreign";
          return clone;
        },
        poisoned: foreignRun.sink.poisoned,
        async dispatch(command: PipelineV2RunCommand) {
          await foreignRun.sink.dispatch(command);
          mutateNext = true;
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink, intent: foreignRun.intent, compiledPlan: foreignRun.compiledPlan }),
      );
      expectCloseError(cause, "lifecycle_conflict");
    } finally {
      await disposeRun(foreignRun.fixture);
    }
  });

  test("25. a not_committed closure keeps the open state authoritative; the fresh retry dispatches again", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: faulted, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "state_persist_failed");
      expect(error.state).not.toBeNull();
      expect((error.state as PipelineV2RunState).revision).toBe(revisionBefore);
      expect((error.state as PipelineV2RunState).generations[0]!.closed).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([{ kind: "stage_generation_closed", generationIndex: 1, by: "replanned" }]);
      expect((fresh.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect(result.generation_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. a durability-unknown closure adopts the candidate and poisons the sink; a fresh sink recognizes C1", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: faulted, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "state_persist_failed");
      expect(faulted.poisoned).toBe(true);
      const adopted = error.state as PipelineV2RunState;
      expect(adopted.revision).toBe(revisionBefore + 1);
      expect(adopted.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect(result.state.revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. two identical concurrent closings both succeed with one durable closure", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const first = closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const second = closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const [resultA, resultB] = await Promise.all([first, second]);
      expect(resultA.intent_sha256).toBe(ctx.intent.sha256);
      expect(resultB.intent_sha256).toBe(ctx.intent.sha256);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. a conflicting retry with another intent rewrites nothing", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      const otherIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: A3.sha256,
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: otherIntent, compiledPlan: ctx.compiledPlan }),
      );
      expectCloseError(cause, "invalid_intent");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. capture counts, bound dispatch and caller mutation isolation", async () => {
    const ctx = await replannedReady();
    try {
      const memberReads: Record<string, number> = {};
      const inner = ctx.sink;
      const proxiedSink = new Proxy(inner as unknown as Record<string, unknown>, {
        get(target, prop: string) {
          memberReads[prop] = (memberReads[prop] ?? 0) + 1;
          return Reflect.get(target, prop);
        },
      }) as unknown as PipelineV2ReplannedGenerationControllerSink;
      const options: Record<string, unknown> = {
        sink: proxiedSink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
      };
      const pending = closePipelineV2ReplannedGeneration(options as never);
      // Caller mutations after the synchronous capture cannot influence
      // the run.
      options["sink"] = null;
      options["intent"] = null;
      options["compiledPlan"] = null;
      const result = await pending;
      expect(result.generation_index).toBe(1);
      // Each options field is read exactly once: an options getter object
      // counts the reads.
      const getterReads: Record<string, number> = {};
      const getterOptions = {
        sink: ctx.sink,
        get intent(): PreparedPipelineV2RunWaitIntent {
          getterReads["intent"] = (getterReads["intent"] ?? 0) + 1;
          return ctx.intent;
        },
        get compiledPlan(): CompiledPipelineV2RunPlan {
          getterReads["compiledPlan"] = (getterReads["compiledPlan"] ?? 0) + 1;
          return ctx.compiledPlan;
        },
      };
      const result1 = await closePipelineV2ReplannedGeneration(getterOptions);
      expect(result1.intent_sha256).toBe(ctx.intent.sha256);
      expect(getterReads["intent"]).toBe(1);
      expect(getterReads["compiledPlan"]).toBe(1);
      // C0: the initial snapshot read plus exactly one post-dispatch read.
      expect(memberReads["snapshot"]).toBe(2);
      expect(memberReads["poisoned"]).toBe(1);
      expect(memberReads["dispatch"]).toBe(1);
      // A C1 retry reads the authoritative snapshot exactly once.
      const c1Reads: Record<string, number> = {};
      const c1Sink = new Proxy(inner as unknown as Record<string, unknown>, {
        get(target, prop: string) {
          c1Reads[prop] = (c1Reads[prop] ?? 0) + 1;
          return Reflect.get(target, prop);
        },
      }) as unknown as PipelineV2ReplannedGenerationControllerSink;
      const retryResult = await closePipelineV2ReplannedGeneration({ sink: c1Sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(retryResult.intent_sha256).toBe(ctx.intent.sha256);
      expect(c1Reads["snapshot"]).toBe(1);
      expect(c1Reads["dispatch"]).toBe(1);
      expect(c1Reads["poisoned"]).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. malformed hostile snapshots are typed invalid_state, never a TypeError", async () => {
    const variants: Array<[string, (state: PipelineV2RunState) => void]> = [
      ["waits null", (state) => {
        (state as unknown as Record<string, unknown>)["waits"] = null;
      }],
      ["generations null", (state) => {
        (state as unknown as Record<string, unknown>)["generations"] = null;
      }],
      ["task ledger null", (state) => {
        (state as unknown as Record<string, unknown>)["task_revisions"] = null;
      }],
      ["plan ledger null", (state) => {
        (state as unknown as Record<string, unknown>)["plan_revisions"] = null;
      }],
      ["wait record null", (state) => {
        (state.waits as unknown as unknown[])[0] = null;
      }],
      ["wait actions null", (state) => {
        (state.waits[0] as unknown as Record<string, unknown>)["actions"] = null;
      }],
      ["generation record null", (state) => {
        (state.generations as unknown as unknown[])[0] = null;
      }],
      ["generation iterations null", (state) => {
        (state.generations[0] as unknown as Record<string, unknown>)["iterations"] = null;
      }],
      ["cursor null", (state) => {
        (state as unknown as Record<string, unknown>)["cursor"] = null;
      }],
      ["execution record null", (state) => {
        (state.executions as unknown as unknown[])[2] = null;
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await replannedReady();
      try {
        const hostile = mutateSnapshotSink(ctx.sink, mutate);
        const cause = await catchClose(() =>
          closePipelineV2ReplannedGeneration({ sink: hostile, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
        );
        expectCloseError(cause, "invalid_state");
        expect((cause as Error).name).toBe("PipelineV2ReplannedGenerationControllerError");
        expect((cause as Error).message).not.toContain(label);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("31. unexpected sink getter and dispatch errors keep their identity", async () => {
    const ctx = await replannedReady();
    try {
      const getterCanary = new Error("sink.snapshot getter canary");
      const throwingSink = {
        get snapshot(): PipelineV2RunState | null {
          throw getterCanary;
        },
        poisoned: false,
        dispatch: async () => undefined,
      };
      const getterCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: throwingSink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expect(getterCause).toBe(getterCanary);
      const dispatchCanary = new Error("sink.dispatch canary");
      const dispatchSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        poisoned: ctx.sink.poisoned,
        dispatch: () => {
          throw dispatchCanary;
        },
      };
      const dispatchCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: dispatchSink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expect(dispatchCause).toBe(dispatchCanary);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. diagnostics are content-free across the failure paths", async () => {
    const ctx = await replannedReady();
    try {
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const record = state.task_revisions[1]!;
        (state.task_revisions as unknown as unknown[])[1] = { ...record, sha256: hex("9") };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "revision_conflict");
      expect(error.message).not.toContain(hex("9"));
      expect(error.message).not.toContain(ctx.intent.sha256);
      expect(error.message).not.toContain("Body");
      expect(error.message).not.toContain(ctx.fixture.runRoot);
      expect(error.message).not.toContain(hex("9"));
      expect(error.message).not.toContain(ctx.intent.sha256);
      expect(error.message).not.toContain("Body");
      expect(error.message).not.toContain(ctx.fixture.runRoot);
      expect(error.message).not.toContain("run-1");
      expect(error.message).not.toContain("canonical");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. the post-dispatch hostile matrix: no false success through narrowed mutations", async () => {
    const variants: Array<[string, (state: PipelineV2RunState) => void]> = [
      ["last planning execution state_id", (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["state_id"] = "dev_entry";
      }],
      ["execution role changed", (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["execution_role"] = "control";
      }],
      ["execution phase changed", (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["phase"] = "agent_data_prepared";
      }],
      ["early execution record changed", (state) => {
        const first = state.executions[0] as unknown as Record<string, unknown>;
        first["profile"] = "coder";
      }],
      ["transition target changed", (state) => {
        const first = state.transitions[0] as unknown as Record<string, unknown>;
        first["to"] = "architect";
      }],
      ["transition execution index changed", (state) => {
        const first = state.transitions[0] as unknown as Record<string, unknown>;
        first["execution_index"] = 2;
      }],
      ["input digest changed", (state) => {
        const input = state.inputs[0] as unknown as Record<string, unknown>;
        input["digest"] = hex("9");
      }],
      ["grant ledger extended", (state) => {
        (state.grants as unknown as unknown[]).push({
          index: 1,
          generation_index: 1,
          wait_index: 1,
          intent_sha256: hex("a"),
          additional_iterations: 1,
        });
      }],
      ["unexpected terminal", (state) => {
        (state as unknown as Record<string, unknown>)["terminal"] = { state_id: "done", result: "success" };
      }],
      ["unexpected run outputs", (state) => {
        (state as unknown as Record<string, unknown>)["run_outputs"] = [
          { id: "report", type: "file", required: true, present: true, digest: hex("c") },
        ];
      }],
      ["unexpected failure", (state) => {
        (state as unknown as Record<string, unknown>)["failure"] = { reason: "internal_error" };
      }],
      ["pipeline null", (state) => {
        (state as unknown as Record<string, unknown>)["pipeline"] = null;
      }],
      ["execution entry null", (state) => {
        (state.executions as unknown as unknown[])[2] = null;
      }],
      ["transition entry null", (state) => {
        (state.transitions as unknown as unknown[])[0] = null;
      }],
      ["grant entry null", (state) => {
        (state.grants as unknown as unknown[])[0] = null;
      }],
      ["cleanup pair malformed", (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["session_cleanup"] = { execution: "completed" };
      }],
      ["agent outputs malformed", (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["outputs"] = [{ id: "plan" }];
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await replannedReady();
      try {
        const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
        const { sink, dispatchCount } = mutateAfterDispatchSink(ctx.sink, mutate);
        const cause = await catchClose(() =>
          closePipelineV2ReplannedGeneration({ sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
        );
        if (!(cause instanceof Error)) {
          throw new Error(`variant ${label}: unexpected success`);
        }
        const error = expectCloseError(cause, "lifecycle_conflict");
        expect(error.message).toContain("does not carry the exact replanned generation closure");
        expect(error.message).not.toContain(label);
        expect(error.message).not.toContain(hex("9"));
        expect(dispatchCount()).toBe(1);
        // The underlying sink still carries the real closure; the
        // controller never returns a success built from the hostile
        // snapshot.
        const underlying = ctx.sink.snapshot as PipelineV2RunState;
        expect(underlying.revision).toBe(revisionBefore + 1);
        expect(underlying.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("36. the same post-dispatch hostile matrix through the racing PipelineV2StateError path", async () => {
    const variants: Array<(state: PipelineV2RunState) => void> = [
      (state) => {
        const last = state.executions[2] as unknown as Record<string, unknown>;
        last["state_id"] = "dev_entry";
      },
      (state) => {
        (state.executions as unknown as unknown[])[2] = null;
      },
      (state) => {
        const first = state.transitions[0] as unknown as Record<string, unknown>;
        first["execution_index"] = 2;
      },
      (state) => {
        (state as unknown as Record<string, unknown>)["pipeline"] = null;
      },
    ];
    for (const mutate of variants) {
      const ctx = await replannedReady();
      try {
        const { sink, dispatchCount } = mutateAfterDispatchSink(ctx.sink, mutate, true);
        const cause = await catchClose(() =>
          closePipelineV2ReplannedGeneration({ sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
        );
        expectCloseError(cause, "lifecycle_conflict");
        expect(dispatchCount()).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("37. an injected PipelineV2RunStateStoreError keeps the single-validator contract", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const memberReads: Record<string, number> = {};
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      let dispatchCalls = 0;
      const proxied = new Proxy(faulted as unknown as Record<string, unknown>, {
        get(target, prop: string) {
          memberReads[prop] = (memberReads[prop] ?? 0) + 1;
          const value = Reflect.get(target, prop);
          if (prop === "dispatch" && typeof value === "function") {
            return (command: PipelineV2RunCommand) => {
              dispatchCalls += 1;
              return (value as (c: PipelineV2RunCommand) => Promise<void>).call(target, command);
            };
          }
          return value;
        },
      }) as unknown as PipelineV2ReplannedGenerationControllerSink;
      const initialSnapshot = ctx.sink.snapshot;
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: proxied, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "state_persist_failed");
      // The already validated initial snapshot stays authoritative: the
      // exact same revision and the open generation.
      expect(error.state).not.toBeNull();
      expect(error.state).toEqual(initialSnapshot);
      expect((error.state as PipelineV2RunState).revision).toBe(revisionBefore);
      expect((error.state as PipelineV2RunState).generations[0]!.closed).toBeUndefined();
      // The snapshot getter was read exactly once (at capture); no
      // re-read and no second validator run happened.
      expect(memberReads["snapshot"]).toBe(1);
      expect(dispatchCalls).toBe(1);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([{ kind: "stage_generation_closed", generationIndex: 1, by: "replanned" }]);
      expect((fresh.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect(result.generation_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("38. the poisoned-sink capture order: all three members read once, the latch last, no traversal after it", async () => {
    const ctx = await replannedReady();
    try {
      const memberReads: Record<string, number> = {};
      let dispatchCalls = 0;
      const poisonedSink: PipelineV2ReplannedGenerationControllerSink = {
        get poisoned() {
          memberReads["poisoned"] = (memberReads["poisoned"] ?? 0) + 1;
          return true;
        },
        get dispatch() {
          memberReads["dispatch"] = (memberReads["dispatch"] ?? 0) + 1;
          return async (command: PipelineV2RunCommand) => {
            dispatchCalls += 1;
            await ctx.sink.dispatch(command);
          };
        },
        get snapshot() {
          memberReads["snapshot"] = (memberReads["snapshot"] ?? 0) + 1;
          return ctx.sink.snapshot;
        },
      };
      let intentReads = 0;
      let planReads = 0;
      const options = {
        sink: poisonedSink,
        get intent(): PreparedPipelineV2RunWaitIntent {
          intentReads += 1;
          return ctx.intent;
        },
        get compiledPlan(): CompiledPipelineV2RunPlan {
          planReads += 1;
          return ctx.compiledPlan;
        },
      };
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration(options),
      );
      const error = expectCloseError(cause, "invalid_state");
      expect(error.message).toContain("poisoned");
      expect(error.state).toBeNull();
      expect(memberReads["poisoned"]).toBe(1);
      expect(memberReads["dispatch"]).toBe(1);
      expect(memberReads["snapshot"]).toBe(1);
      expect(dispatchCalls).toBe(0);
      // The intent and compiled-plan fields are read exactly once at
      // capture (before the latch, per the fixed capture order) and never
      // again: no provenance or state traversal follows the latch.
      expect(intentReads).toBe(1);
      expect(planReads).toBe(1);
      // A throwing getter at each capture position keeps its identity in
      // the capture order; no later member is read after the throw.
      const poisonedCanary = new Error("poisoned getter canary");
      const poisonedThrowing = {
        get poisoned() {
          throw poisonedCanary;
        },
        get dispatch() {
          throw new Error("dispatch must not be read after a throwing poisoned getter");
        },
        get snapshot() {
          throw new Error("snapshot must not be read after a throwing poisoned getter");
        },
      };
      const poisonedCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: poisonedThrowing as unknown as PipelineV2ReplannedGenerationControllerSink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expect(poisonedCause).toBe(poisonedCanary);
      const dispatchCanary = new Error("dispatch getter canary");
      const dispatchThrowing = {
        poisoned: false,
        get dispatch() {
          throw dispatchCanary;
        },
        get snapshot() {
          throw new Error("snapshot must not be read after a throwing dispatch getter");
        },
      };
      const dispatchCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: dispatchThrowing as unknown as PipelineV2ReplannedGenerationControllerSink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expect(dispatchCause).toBe(dispatchCanary);
      const snapshotCanary = new Error("snapshot getter canary");
      const snapshotThrowing = {
        poisoned: false,
        dispatch: async () => undefined,
        get snapshot() {
          throw snapshotCanary;
        },
      };
      const snapshotCause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: snapshotThrowing as unknown as PipelineV2ReplannedGenerationControllerSink, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      expect(snapshotCause).toBe(snapshotCanary);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("39. the C2 retry form with the first iteration open is a zero-dispatch success", async () => {
    const ctx = await replannedReady();
    try {
      const first = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect(result.generation_index).toBe(first.generation_index);
      expect(result.iteration_index).toBe(first.iteration_index);
      expect(result.wait_index).toBe(first.wait_index);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(2);
      expect(state.generations[0]!.closed).toEqual({ by: "replanned", closed_transition_count: 2 });
      expect(state.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("40. the racing path recognizes the full immediate suffix as an idempotent success", async () => {
    const ctx = await replannedReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      let racingAfter: PipelineV2RunState | null | undefined;
      const racing: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot() {
          if (racingAfter === undefined) {
            return ctx.sink.snapshot;
          }
          return racingAfter;
        },
        poisoned: ctx.sink.poisoned,
        async dispatch(command) {
          await ctx.sink.dispatch(command);
          await ctx.sink.dispatch({
            kind: "stage_generation_opened",
            stageId: "stage-1",
            stagePosition: 1,
            templateId: "development",
            planSha256: ctx.plan2.sha256,
            initialBudget: 2,
            transitionCount: 2,
          });
          await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 });
          racingAfter = ctx.sink.snapshot;
          throw new PipelineV2StateError("racing injected");
        },
      };
      const result = await closePipelineV2ReplannedGeneration({ sink: racing, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(result.generation_index).toBe(1);
      expect(result.state.revision).toBe(revisionBefore + 3);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(2);
      expect(state.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("41. hostile C2 shapes: a closed new generation, a closed or second iteration and a foreign anchor never pass", async () => {
    const closedNewGeneration = await replannedReady();
    try {
      const first = await closePipelineV2ReplannedGeneration({ sink: closedNewGeneration.sink, intent: closedNewGeneration.intent, compiledPlan: closedNewGeneration.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: closedNewGeneration.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: closedNewGeneration.sink });
      const hostile = mutateSnapshotSink(closedNewGeneration.sink, (state) => {
        const generation = state.generations[1]!;
        (state.generations as unknown as unknown[])[1] = {
          ...generation,
          closed: { by: "next_stage", closed_transition_count: 2 },
          open_iteration: undefined,
          iterations: [{ index: 1, opened_transition_count: 2, closed: { by: "normal_close", closed_transition_count: 2 } }],
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: closedNewGeneration.intent, compiledPlan: closedNewGeneration.compiledPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("the target iteration closure is not the exact replanned closure");
      expect(first).toBeDefined();
    } finally {
      await disposeRun(closedNewGeneration.fixture);
    }
    const secondIteration = await replannedReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: secondIteration.sink, intent: secondIteration.intent, compiledPlan: secondIteration.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: secondIteration.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: secondIteration.sink });
      const hostile = mutateSnapshotSink(secondIteration.sink, (state) => {
        const generation = state.generations[1]!;
        (generation.iterations as unknown as unknown[]).push({ index: 2, opened_transition_count: 2 });
        (state.generations as unknown as unknown[])[1] = {
          ...generation,
          iteration_count: 2,
          open_iteration: { index: 2, opened_transition_count: 2 },
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: secondIteration.intent, compiledPlan: secondIteration.compiledPlan }),
      );
      // A second open iteration is never loader-representable, so the
      // fail-closed boundary is the loader's, not the controller's own.
      const error2 = expectCloseError(cause, "invalid_state");
      expect(error2.message).toContain("requires a durable pipeline v2 run state document");
    } finally {
      await disposeRun(secondIteration.fixture);
    }
    const closedIteration = await replannedReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: closedIteration.sink, intent: closedIteration.intent, compiledPlan: closedIteration.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: closedIteration.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: closedIteration.sink });
      const hostile = mutateSnapshotSink(closedIteration.sink, (state) => {
        const generation = state.generations[1]!;
        (generation.iterations as unknown as unknown[])[0] = {
          ...(generation.iterations as unknown as { closed?: unknown }[])[0],
          closed: { by: "normal_close", closed_transition_count: 2 },
        };
        (state.generations as unknown as unknown[])[1] = { ...generation, open_iteration: undefined };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: closedIteration.intent, compiledPlan: closedIteration.compiledPlan }),
      );
      const error3 = expectCloseError(cause, "lifecycle_conflict");
      expect(error3.message).toContain("the new generation's first iteration is not open");
    } finally {
      await disposeRun(closedIteration.fixture);
    }
    const foreignAnchor = await replannedReady();
    try {
      await closePipelineV2ReplannedGeneration({ sink: foreignAnchor.sink, intent: foreignAnchor.intent, compiledPlan: foreignAnchor.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: foreignAnchor.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: foreignAnchor.sink });
      const hostile = mutateSnapshotSink(foreignAnchor.sink, (state) => {
        const generation = state.generations[1]!;
        (state.generations as unknown as unknown[])[1] = { ...generation, opened_transition_count: 3 };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: foreignAnchor.intent, compiledPlan: foreignAnchor.compiledPlan }),
      );
      expectCloseError(cause, "invalid_state");
    } finally {
      await disposeRun(foreignAnchor.fixture);
    }
  });

  test("42. the prefix C2 retry form (old generation index 2, bare new generation) is a zero-dispatch success", async () => {
    const ctx = await replannedReady({ withPrefixGeneration: true });
    try {
      const stateBefore = ctx.sink.snapshot as PipelineV2RunState;
      expect(stateBefore.generations).toHaveLength(2);
      expect(stateBefore.generations[0]!.closed?.by).toBe("next_stage");
      expect(stateBefore.generations[1]!.closed).toBeUndefined();
      const first = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(first.generation_index).toBe(2);
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 2, failStep: "rename" }),
        now: nextTick,
      });
      const faultCause = await catchClose(() =>
        ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: faultedSink }),
      );
      expect(faultCause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      const afterFault = (await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick })).snapshot as PipelineV2RunState;
      expect(afterFault.generations).toHaveLength(3);
      expect(afterFault.generations[2]!.open_iteration).toBeUndefined();
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(retrySink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect(result.generation_index).toBe(2);
      expect(result.iteration_index).toBe(1);
      expect(result.wait_index).toBe(1);
      const state = retrySink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(3);
      expect(state.generations[0]!.closed?.by).toBe("next_stage");
      expect(state.generations[1]!.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
      expect(state.generations[2]!.index).toBe(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("43. the prefix C2 retry form with the first iteration open is a zero-dispatch success", async () => {
    const ctx = await replannedReady({ withPrefixGeneration: true });
    try {
      const first = await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(first.generation_index).toBe(2);
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await closePipelineV2ReplannedGeneration({ sink: recording, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect(result.generation_index).toBe(2);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(3);
      expect(state.generations[1]!.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
      expect(state.generations[2]!.open_iteration).toEqual({ index: 1, opened_transition_count: 4 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("44. the racing path recognizes the prefix C2 suffix on an old generation with index > 1", async () => {
    const ctx = await replannedReady({ withPrefixGeneration: true });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      let racingAfter: PipelineV2RunState | null | undefined;
      const racing: PipelineV2ReplannedGenerationControllerSink = {
        get snapshot() {
          if (racingAfter === undefined) {
            return ctx.sink.snapshot;
          }
          return racingAfter;
        },
        poisoned: ctx.sink.poisoned,
        async dispatch(command) {
          await ctx.sink.dispatch(command);
          await ctx.sink.dispatch({
            kind: "stage_generation_opened",
            stageId: "stage-1",
            stagePosition: 1,
            templateId: "development",
            planSha256: ctx.plan2.sha256,
            initialBudget: 2,
            transitionCount: 4,
          });
          await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 3, iterationIndex: 1, transitionCount: 4 });
          racingAfter = ctx.sink.snapshot;
          throw new PipelineV2StateError("racing injected");
        },
      };
      const result = await closePipelineV2ReplannedGeneration({ sink: racing, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      expect(result.generation_index).toBe(2);
      expect(result.state.revision).toBe(revisionBefore + 3);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations).toHaveLength(3);
      expect(state.generations[1]!.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
      expect(state.generations[2]!.open_iteration).toEqual({ index: 1, opened_transition_count: 4 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("45. an extra generation after the prefix C2 form is fail-closed", async () => {
    const ctx = await replannedReady({ withPrefixGeneration: true });
    try {
      await closePipelineV2ReplannedGeneration({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan });
      await ensurePipelineV2StageIteration({ compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2, sink: ctx.sink });
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const generation = state.generations[2]!;
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
        (state.generations as unknown as unknown[])[2] = {
          ...generation,
          closed: { by: "next_stage", closed_transition_count: 4 },
          open_iteration: undefined,
          iterations: [{ index: 1, opened_transition_count: 4, closed: { by: "normal_close", closed_transition_count: 4 } }],
        };
      });
      const cause = await catchClose(() =>
        closePipelineV2ReplannedGeneration({ sink: hostile, intent: ctx.intent, compiledPlan: ctx.compiledPlan }),
      );
      const error = expectCloseError(cause, "lifecycle_conflict");
      expect(error.message).toContain("the target iteration closure is not the exact replanned closure");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

test("33. the runtime export surfaces are exact (public two keys, internal two keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_replanned_generation_controller.ts");
  const internalModule = await import("../src/pipeline_v2_replanned_generation_controller_internal.ts");
  expect(Object.keys(publicModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedGenerationControllerError",
    "closePipelineV2ReplannedGeneration",
  ]);
  expect(Object.keys(internalModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedGenerationControllerError",
    "closePipelineV2ReplannedGenerationInternal",
  ]);
});

test("34. the controller composes the existing layers only (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_replanned_generation_controller_internal.ts"), "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("comparePipelineV2RunIdentity(")).toBe(2);
  expect(countOf("hasPreparedRunPlanProvenance(")).toBe(1);
  expect(countOf("hasCompiledRunPlanProvenance(")).toBe(1);
  expect(countOf("compiledRunPlanOriginIdentity(")).toBe(1);
  expect(countOf("JSON.stringify")).toBe(0);
  expect(countOf("createHash")).toBe(0);
  expect(countOf("CryptoHasher")).toBe(0);
  expect(countOf("new WeakMap")).toBe(0);
  expect(countOf("new WeakSet")).toBe(0);
  expect(countOf("structuredClone")).toBe(0);
  expect(countOf("O_EXCL")).toBe(0);
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf("acceptPipelineV2RunPlanCandidate(")).toBe(0);
  expect(countOf("publishPipelineV2")).toBe(0);
  expect(countOf("loadPipelineV2")).toBe(0);
  expect(countOf("prepareWaitIntent(")).toBe(0);
  expect(countOf("deepFreezeValue")).toBe(2);
  for (const banned of [
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "main.ts",
    "cli_",
    "docker",
    "launcher",
    "pipeline_v2_wait_store",
    "pipeline_v2_wait_manifest",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_run_plan_candidate",
    "pipeline_v2_run_plan_controller",
    "pipeline_v2_run_plan_acceptance",
    "pipeline_v2_revise_task_intent_controller",
    "pipeline_v2_revise_task_closure_controller",
    "pipeline_v2_revise_task_completion_controller",
    "pipeline_v2_continue_stage",
    "pipeline_state_store",
  ]) {
    expect(source).not.toContain(banned);
  }
});
