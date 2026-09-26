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
  type PreparedPipelineV2RunTaskRevision,
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  applyPipelineV2ReviseTaskClosure,
  PipelineV2ReviseTaskClosureControllerError,
  type PipelineV2ReviseTaskClosureControllerFailureReason,
  type PipelineV2ReviseTaskClosureControllerSink,
} from "../src/pipeline_v2_revise_task_closure_controller.ts";
import { applyPipelineV2ReviseTaskClosureInternal } from "../src/pipeline_v2_revise_task_closure_controller_internal.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");

const DISPATCH_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: d_next_stage
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
`;

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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-closure-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "decisions", "dispatch.yaml"), DISPATCH_MODEL_YAML);
    return await fn(await loadPipelineV2(bundle));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-closure-run-"));
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

const B2 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-b",
  revision: 2,
  previous_sha256: B1.sha256,
  origin: "user_response",
  body: "Body B revised",
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

const INTENT = prepareWaitIntent({
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: RUN_ID,
  wait_index: 1,
  task_id: "task-a",
  expected_previous_task_sha256: A1.sha256,
  new_task_revision_sha256: A2.sha256,
});

const WRONG_PREV_INTENT = prepareWaitIntent({
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: RUN_ID,
  wait_index: 1,
  task_id: "task-a",
  expected_previous_task_sha256: hex("5"),
  new_task_revision_sha256: A2.sha256,
});

interface ClosureCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  candidate: PreparedPipelineV2RunTaskRevision;
}

interface ClosureReadyOptions {
  intent?: PreparedPipelineV2RunWaitIntent;
  acceptIntent?: boolean;
  acceptIntentSha256?: string;
  recordTaskRevision?: boolean;
  recordTaskSha256?: string;
  extraTaskRevision?: boolean;
  extraOtherTaskRevision?: boolean;
  noReviseAction?: boolean;
  closeIterationNormal?: boolean;
  closeGeneration?: boolean;
  secondPlanRevision?: boolean;
  secondIteration?: boolean;
  respondReviseTask?: boolean;
  answerOtherAction?: boolean;
  laterExecution?: boolean;
  newWait?: boolean;
  newGeneration?: boolean;
}

/**
 * A real sink driven through the real reducer and the existing
 * controllers to the revise-closure boundary: the revise intent accepted
 * durably inside the open wait with its accepted task revision in the
 * ledger.
 */
async function closureReady(options: ClosureReadyOptions = {}): Promise<ClosureCtx> {
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    const intent = options.intent ?? INTENT;
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
        {
          id: "stage-2",
          template: "development",
          tasks: [{ id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] }],
        },
      ],
    });
    const planCandidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [A1, B1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: planCandidate });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: 2, sink });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    if (options.secondPlanRevision === true) {
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning2")) {
        await sink.dispatch(command);
      }
      const plan2 = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 2,
        previous_sha256: plan1.sha256,
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
      const candidate2: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
        plan: plan2,
        taskRevisions: [A1],
        previousPlan: plan1,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      });
      await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidate2 });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 3 });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 2 });
      for (const command of agentPhases("stage2")) {
        await sink.dispatch(command);
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 4,
      });
    } else if (options.secondIteration === true) {
      // a second iteration of the same generation through the real graph:
      // the first iteration closed by the ordinary active-boundary
      // closure, the planning execution re-runs at the architect state,
      // and the second iteration runs a real stage execution
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
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
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 3 });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 2 });
      for (const command of agentPhases("stage2")) {
        await sink.dispatch(command);
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 4,
      });
    } else {
      if (options.closeIterationNormal === true) {
        await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
        if (options.closeGeneration === true) {
          await sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
        }
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
    }
    const actions = options.noReviseAction === true
      ? [{ id: "continue_stage", to: "dev_entry" }]
      : [
          { id: "continue_stage", to: "dev_entry" },
          { id: "revise_task", to: "architect" },
        ];
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: hex("1"),
      actions,
    });
    if (options.acceptIntent !== false) {
      await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: options.acceptIntentSha256 ?? intent.sha256 });
      if (options.recordTaskRevision !== false) {
        const acceptedIntentSha256 = options.acceptIntentSha256 ?? intent.sha256;
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: options.recordTaskSha256 ?? A2.sha256,
          waitIndex: 1,
          intentSha256: acceptedIntentSha256,
        });
        if (options.extraTaskRevision === true) {
          await sink.dispatch({
            kind: "task_revision_accepted",
            taskId: "task-a",
            revision: 3,
            taskSha256: A3.sha256,
            waitIndex: 1,
            intentSha256: acceptedIntentSha256,
          });
        }
        if (options.extraOtherTaskRevision === true) {
          await sink.dispatch({
            kind: "task_revision_accepted",
            taskId: "task-b",
            revision: 2,
            taskSha256: B2.sha256,
            waitIndex: 1,
            intentSha256: acceptedIntentSha256,
          });
        }
      }
    }
    if (options.respondReviseTask === true || options.answerOtherAction === true) {
      const snapshot = sink.snapshot as PipelineV2RunState;
      const waitRecord = snapshot.waits[snapshot.waits.length - 1]!;
      await sink.dispatch({
        kind: "stage_iteration_closed",
        generationIndex: 1,
        iterationIndex: 1,
        by: "replanned",
        waitIndex: 1,
      });
      await sink.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: waitRecord.request_sha256,
        actionId: options.answerOtherAction === true ? "continue_stage" : "revise_task",
        responseSha256: hex("e"),
      });
      if (options.laterExecution === true) {
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
        for (const command of agentPhases("later")) {
          await sink.dispatch(command);
        }
      }
      if (options.newWait === true) {
        await sink.dispatch({
          kind: "run_waiting",
          stateId: "architect",
          reason: "stage_iteration_limit_exhausted",
          requestSha256: hex("2"),
          actions,
        });
      }
      if (options.newGeneration === true) {
        await sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
        await ensurePipelineV2StageIteration({ compiledPlan: accepted.compiled_plan, stageId: "stage-2", initialBudget: 2, sink });
      }
    }
    return { fixture, sink, pipeline, intent, candidate: A2 };
  });
}

interface RecordingSink extends PipelineV2ReviseTaskClosureControllerSink {
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

interface MutationSinkOptions {
  race?: boolean;
  silent?: boolean;
  mutateAfterClosure?: (derived: PipelineV2RunState) => PipelineV2RunState;
}

function mutationSink(
  inner: PipelineV2RunStateSink,
  options: MutationSinkOptions = {},
): { sink: PipelineV2ReviseTaskClosureControllerSink; closureDispatches: () => number } {
  const counts = { closures: 0 };
  const sink: PipelineV2ReviseTaskClosureControllerSink = {
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      if (command.kind === "stage_iteration_closed") {
        counts.closures += 1;
      }
      if (options.silent === true) {
        return;
      }
      await inner.dispatch(command);
      if (options.race === true) {
        throw new PipelineV2StateError("simulated lost race");
      }
    },
    get snapshot() {
      const real = inner.snapshot as PipelineV2RunState;
      if (options.mutateAfterClosure === undefined) {
        return real;
      }
      const generation = real?.generations[real.generations.length - 1];
      const iteration = generation?.iterations[generation.iterations.length - 1];
      if (generation === undefined || iteration === undefined || iteration.closed === undefined) {
        return real;
      }
      const derived = structuredClone(real) as PipelineV2RunState;
      return options.mutateAfterClosure(derived);
    },
  };
  return { sink, closureDispatches: () => counts.closures };
}

async function catchAccept(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectClosureError(
  cause: unknown,
  reason: PipelineV2ReviseTaskClosureControllerFailureReason,
): PipelineV2ReviseTaskClosureControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
  const error = cause as PipelineV2ReviseTaskClosureControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

const EXPECTED_COMMAND: PipelineV2RunCommand = {
  kind: "stage_iteration_closed",
  generationIndex: 1,
  iterationIndex: 1,
  by: "replanned",
  waitIndex: 1,
};

describe("applyPipelineV2ReviseTaskClosure", () => {
  test("1. C0 happy path through the real reducer: exact command and exact result shape", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([EXPECTED_COMMAND]);
      expect(result).toEqual({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
        intent_sha256: ctx.intent.sha256,
        state: ctx.sink.snapshot as PipelineV2RunState,
      });
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.status).toBe("waiting");
      const generation = state.generations[0]!;
      expect(generation.closed).toBeUndefined();
      expect(generation.open_iteration).toBeUndefined();
      const iteration = generation.iterations[generation.iterations.length - 1]!;
      expect(iteration.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the post-closure state round-trips through the loader", async () => {
    const ctx = await closureReady();
    try {
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const round = validatePipelineV2RunState(JSON.parse(JSON.stringify(ctx.sink.snapshot)));
      expect(round.status).toBe("waiting");
      const generation = round.generations[0]!;
      expect(generation.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
      expect(generation.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the result is deep-frozen and content-free", async () => {
    const ctx = await closureReady();
    try {
      const result = await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      expect(Object.keys(result).sort()).toEqual([
        "generation_index",
        "intent_sha256",
        "iteration_index",
        "state",
        "task_id",
        "task_revision",
        "task_sha256",
        "wait_index",
      ]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      expect(JSON.stringify(result)).not.toContain("Body A");
      expect(JSON.stringify(result)).not.toContain('"kind"');
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. C1 zero-dispatch retry: the exact durable closure returns the same result", async () => {
    const ctx = await closureReady();
    try {
      const first = await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const recording = recordingSink(ctx.sink);
      const second = await applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(second).toEqual(first);
      expect((ctx.sink.snapshot as PipelineV2RunState).task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. active/answered immediate retry: zero dispatch on the answered boundary", async () => {
    const ctx = await closureReady({ respondReviseTask: true });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(result).toEqual({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
        intent_sha256: ctx.intent.sha256,
        state: ctx.sink.snapshot as PipelineV2RunState,
      });
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.status).toBe("active");
      expect(state.cursor.current_state).toBe("architect");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. the missing accepted intent is invalid state with zero dispatch", async () => {
    const ctx = await closureReady({ acceptIntent: false });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("has not accepted an intent");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. a different accepted intent is invalid state with zero dispatch", async () => {
    const ctx = await closureReady({ acceptIntentSha256: hex("9") });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("accepted a different intent");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. the missing revise_task action is invalid state", async () => {
    const ctx = await closureReady({ noReviseAction: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not declare the revise_task action");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. the missing accepted task record is invalid state with zero dispatch", async () => {
    const ctx = await closureReady({ recordTaskRevision: false });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("carries no accepted task revision");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a wrong task digest is revision conflict with zero dispatch", async () => {
    const ctx = await closureReady({ recordTaskSha256: hex("8") });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "revision_conflict");
      expect(error.message).toContain("contradicts the revise intent");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. a wrong expected predecessor is revision conflict", async () => {
    const ctx = await closureReady({ intent: WRONG_PREV_INTENT });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "revision_conflict");
      expect(error.message).toContain("contradicts the revise intent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. duplicate wait-bound task records are revision conflict", async () => {
    const ctx = await closureReady({ extraTaskRevision: true });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "revision_conflict");
      expect(error.message).toContain("carries several accepted task revisions");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. an iteration closed by normal_close is lifecycle conflict", async () => {
    const ctx = await closureReady({ closeIterationNormal: true });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("not by the replanned closure");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a closed generation is lifecycle conflict", async () => {
    const ctx = await closureReady({ closeIterationNormal: true, closeGeneration: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("is closed");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a generation bound to another plan digest is lifecycle conflict", async () => {
    const ctx = await closureReady({ secondPlanRevision: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("does not belong to the last durable plan revision");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. the active retry answered with another action is lifecycle conflict", async () => {
    const ctx = await closureReady({ answerOtherAction: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("answered with another action");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. the active retry with a later execution is lifecycle conflict", async () => {
    const ctx = await closureReady({ respondReviseTask: true, laterExecution: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("an execution was started after the settled wait boundary");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. the active retry with a new wait is lifecycle conflict", async () => {
    const ctx = await closureReady({ respondReviseTask: true, newWait: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message).toContain("no longer the last wait");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. the active retry with a new generation is lifecycle conflict", async () => {
    const ctx = await closureReady({ respondReviseTask: true, newGeneration: true });
    try {
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "lifecycle_conflict");
      expect(error.message.length).toBeGreaterThan(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. the reducer pre-check precedes the dispatch (source-order proof)", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync("orchestrator/src/pipeline_v2_revise_task_closure_controller_internal.ts", "utf8");
    const flow = source.slice(source.indexOf("export async function applyPipelineV2ReviseTaskClosureInternal"));
    const classification = flow.indexOf("classifyReviseClosure(state, bindings)");
    const precheck = flow.indexOf("precheckClosure(state, closureCommand, state)");
    const dispatch = flow.indexOf("await dispatchCommand(closureCommand)");
    expect(classification).toBeGreaterThan(0);
    expect(precheck).toBeGreaterThan(classification);
    expect(dispatch).toBeGreaterThan(precheck);
    // the exact command is the only dispatch of the C0 path
    expect(flow).toContain('kind: "stage_iteration_closed"');
    expect(flow).toContain('by: "replanned"');
  });

  test("21. a normal hostile post-dispatch snapshot with a removed or replaced wait intent is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        mutateAfterClosure: (derived) => {
          const waitRecord = derived.waits[derived.waits.length - 1];
          if (waitRecord !== undefined) {
            (derived.waits as unknown as PipelineV2RunState["waits"])[derived.waits.length - 1] = { ...waitRecord, intent: undefined };
          }
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("missing or replaced");
      expect(closureDispatches()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).waits[0]!.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. a normal hostile post-dispatch snapshot with changed wait bindings or actions is invalid state", async () => {
    for (const mutate of [
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const waitRecord = derived.waits[derived.waits.length - 1];
        if (waitRecord !== undefined) {
          (derived.waits as unknown as PipelineV2RunState["waits"])[derived.waits.length - 1] = { ...waitRecord, reason: "other_reason" };
        }
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const waitRecord = derived.waits[derived.waits.length - 1];
        if (waitRecord !== undefined) {
          (derived.waits as unknown as PipelineV2RunState["waits"])[derived.waits.length - 1] = {
            ...waitRecord,
            actions: [...waitRecord.actions, { id: "extra", to: "architect" }],
          };
        }
        return derived;
      },
    ]) {
      const ctx = await closureReady();
      try {
        const { sink } = mutationSink(ctx.sink, { mutateAfterClosure: mutate });
        const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
        const error = expectClosureError(cause, "invalid_state");
        expect(error.message).toContain("does not carry the applied closure");
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("23. a normal hostile post-dispatch snapshot with a changed task-ledger prefix is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const { sink } = mutationSink(ctx.sink, {
        mutateAfterClosure: (derived) => {
          const record = derived.task_revisions[1];
          if (record !== undefined) {
            (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"])[1] = { ...record, sha256: hex("6") };
          }
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect((ctx.sink.snapshot as PipelineV2RunState).task_revisions[1]!.sha256).toBe(B1.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a normal hostile post-dispatch snapshot with a changed plan or generation binding is invalid state", async () => {
    for (const mutate of [
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const record = derived.plan_revisions[0];
        if (record !== undefined) {
          (derived.plan_revisions as unknown as PipelineV2RunState["plan_revisions"])[0] = { ...record, sha256: hex("7") };
        }
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0];
        if (generation !== undefined) {
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, template_id: "other" };
        }
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0];
        if (generation !== undefined) {
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...generation,
            open_iteration: { index: 1, opened_transition_count: 1 },
          };
        }
        return derived;
      },
    ]) {
      const ctx = await closureReady();
      try {
        const { sink } = mutationSink(ctx.sink, { mutateAfterClosure: mutate });
        const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
        const error = expectClosureError(cause, "invalid_state");
        expect(error.message).toContain("does not carry the applied closure");
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("25. a normal hostile malformed post-dispatch snapshot is typed invalid state, never a TypeError", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["waits null", (derived) => {
        (derived as unknown as Record<string, unknown>)["waits"] = null;
      }],
      ["task ledger null entry", (derived) => {
        (derived.task_revisions as unknown as unknown[])[1] = null;
      }],
      ["task ledger primitive entry", (derived) => {
        (derived.task_revisions as unknown as unknown[])[0] = 7;
      }],
      ["cursor null", (derived) => {
        (derived as unknown as Record<string, unknown>)["cursor"] = null;
      }],
      ["open iteration projection primitive", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          open_iteration: 7 as unknown as PipelineV2RunState["generations"][number]["open_iteration"],
        };
      }],
      ["generations null entry", (derived) => {
        (derived.generations as unknown as unknown[])[0] = null;
      }],
      ["generation iterations null", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: null as unknown as PipelineV2RunState["generations"][number]["iterations"],
        };
      }],
      ["later iteration appended", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: [...generation.iterations, { index: 2, opened_transition_count: 2 }],
        };
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await closureReady();
      try {
        const { sink } = mutationSink(ctx.sink, {
          mutateAfterClosure: (derived) => {
            mutate(derived);
            return derived;
          },
        });
        const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
        const error = expectClosureError(cause, "invalid_state");
        expect((cause as Error).name).toBe("PipelineV2ReviseTaskClosureControllerError");
        expect(error.message).not.toContain("null");
        expect(error.message).not.toContain(label);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("26. a racing hostile post-dispatch snapshot is invalid state with zero additional dispatch", async () => {
    const ctx = await closureReady();
    try {
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        race: true,
        mutateAfterClosure: (derived) => {
          const waitRecord = derived.waits[derived.waits.length - 1];
          if (waitRecord !== undefined) {
            (derived.waits as unknown as PipelineV2RunState["waits"])[derived.waits.length - 1] = { ...waitRecord, intent: undefined };
          }
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("missing or replaced");
      expect(closureDispatches()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toBeDefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. the closure not_committed keeps the previous open state; the fresh retry dispatches the closure", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: faulted, intent: ctx.intent }));
      const error = expectClosureError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be committed");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toBeUndefined();
      const retryRecording = recordingSink(ctx.sink);
      const retried = await applyPipelineV2ReviseTaskClosure({ sink: retryRecording, intent: ctx.intent });
      expect(retryRecording.commands).toEqual([EXPECTED_COMMAND]);
      expect(retried.task_revision).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. the closure durability_unknown adopts the durable closure; the fresh sink recognizes zero dispatch", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: faulted, intent: ctx.intent }));
      const error = expectClosureError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be confirmed durable");
      expect(faulted.poisoned).toBe(true);
      // the rename succeeded: the durable file carries the closure, while
      // the original sink's in-memory copy stays at the pre-closure revision
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      expect((fresh.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect((fresh.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
      const recording = recordingSink(fresh);
      const result = await applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(result).toMatchObject({ task_revision: 2, wait_index: 1 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. a racing identical closure through the real dispatch is exact idempotent success", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { sink, closureDispatches } = mutationSink(ctx.sink, { race: true });
      const result = await applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent });
      expect(result).toMatchObject({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
        intent_sha256: ctx.intent.sha256,
      });
      expect(closureDispatches()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a resolve-without-change dispatch is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const { sink, closureDispatches } = mutationSink(ctx.sink, { silent: true });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect(closureDispatches()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. two identical concurrent attempts both succeed with one durable closure and exactly one revision increment", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const [first, second] = await Promise.all([
        applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }).catch((cause: unknown) => cause),
        applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }).catch((cause: unknown) => cause),
      ]);
      const firstResult = first as { task_revision?: number };
      const secondResult = second as { task_revision?: number };
      expect(firstResult.task_revision).toBe(2);
      expect(secondResult.task_revision).toBe(2);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 1);
      const closed = state.generations[0]!.iterations[0]!.closed;
      expect(closed).toEqual({ by: "replanned", wait_index: 1, closed_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. a conflicting retry with another intent is refused with zero additional dispatch", async () => {
    const ctx = await closureReady();
    try {
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const otherIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: A3.sha256,
      });
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: otherIntent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("accepted a different intent");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. the options and sink members are read exactly once", async () => {
    const ctx = await closureReady();
    try {
      const memberReads: Record<string, number> = {};
      const dispatchReads = { count: 0 };
      const innerSink = ctx.sink;
      const proxiedOptions = new Proxy(
        {} as { sink: PipelineV2ReviseTaskClosureControllerSink; intent: PreparedPipelineV2RunWaitIntent },
        {
          get(_target, prop: string) {
            memberReads[prop] = (memberReads[prop] ?? 0) + 1;
            if (prop === "sink") {
              return {
                get poisoned() {
                  return innerSink.poisoned;
                },
                get dispatch(): (command: PipelineV2RunCommand) => Promise<void> {
                  dispatchReads.count += 1;
                  if (dispatchReads.count > 1) {
                    throw new Error("the sink dispatch member must be read exactly once");
                  }
                  return (command) => innerSink.dispatch(command);
                },
                get snapshot() {
                  return innerSink.snapshot;
                },
              };
            }
            return ctx.intent;
          },
        },
      );
      const result = await applyPipelineV2ReviseTaskClosureInternal(proxiedOptions);
      expect(result).toMatchObject({ task_revision: 2 });
      expect(memberReads["sink"]).toBe(1);
      expect(memberReads["intent"]).toBe(1);
      expect(dispatchReads.count).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. a non-registered or Proxy intent is rejected before any field read", async () => {
    const ctx = await closureReady();
    try {
      const handBuilt = {
        manifest: {
          schema_version: 1,
          kind: "revise_task_intent",
          run_id: RUN_ID,
          wait_index: 1,
          task_id: "task-a",
          expected_previous_task_sha256: A1.sha256,
          new_task_revision_sha256: A2.sha256,
        },
        canonical_json: "{}",
        sha256: ctx.intent.sha256,
      };
      let snapshotReads = 0;
      const hostileSnapshotSink: PipelineV2ReviseTaskClosureControllerSink = {
        get snapshot() {
          snapshotReads += 1;
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await ctx.sink.dispatch(command);
        },
      };
      const handBuiltCause = await catchAccept(() =>
        applyPipelineV2ReviseTaskClosure({ sink: hostileSnapshotSink, intent: handBuilt as unknown as PreparedPipelineV2RunWaitIntent }),
      );
      expectClosureError(handBuiltCause, "invalid_intent");
      // the capture read the snapshot member exactly once (the capture
      // step precedes the gate); no field of the snapshot was read and no
      // further read happened before the rejection
      expect(snapshotReads).toBe(1);
      let trapHits = 0;
      const proxiedIntent = new Proxy(ctx.intent, {
        get() {
          trapHits += 1;
          return undefined;
        },
      });
      const proxiedCause = await catchAccept(() =>
        applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: proxiedIntent }),
      );
      expectClosureError(proxiedCause, "invalid_intent");
      expect(trapHits).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. caller mutation after the capture cannot influence the acceptance", async () => {
    const ctx = await closureReady();
    try {
      const otherIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: A3.sha256,
      });
      let intentReads = 0;
      const options = {
        sink: ctx.sink as PipelineV2ReviseTaskClosureControllerSink,
        get intent(): PreparedPipelineV2RunWaitIntent {
          intentReads += 1;
          return intentReads === 1 ? ctx.intent : otherIntent;
        },
      };
      const result = await applyPipelineV2ReviseTaskClosureInternal(options);
      expect(result).toMatchObject({ task_revision: 2, intent_sha256: ctx.intent.sha256 });
      expect(intentReads).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("36. unexpected downstream errors preserve their identity", async () => {
    const ctx = await closureReady();
    try {
      const fault = new Error("boom-identity");
      const failingSink: PipelineV2ReviseTaskClosureControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch() {
          throw fault;
        },
        get snapshot() {
          return ctx.sink.snapshot;
        },
      };
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: failingSink, intent: ctx.intent }));
      expect(cause).toBe(fault);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("37. diagnostics are content-free across the failure paths", async () => {
    const messages: string[] = [];
    const contexts: Array<() => Promise<unknown>> = [
      async () => {
        const ctx = await closureReady({ acceptIntent: false });
        try {
          return await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
      async () => {
        const ctx = await closureReady({ recordTaskSha256: hex("8") });
        try {
          return await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
      async () => {
        const ctx = await closureReady({ extraTaskRevision: true });
        try {
          return await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
      async () => {
        const ctx = await closureReady({ closeIterationNormal: true });
        try {
          return await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent }));
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
    ];
    for (const run of contexts) {
      const cause = await run();
      messages.push((cause as Error).message);
    }
    for (const message of messages) {
      expect(message).not.toContain(INTENT.sha256);
      expect(message).not.toContain(A2.sha256);
      expect(message).not.toContain("Body A");
      expect(message).not.toContain('"kind"');
      expect(message).not.toContain("/tmp");
      expect(message).not.toContain("run-plan");
      expect(message).not.toContain("state.json");
      expect(message).not.toContain("Bearer");
      expect(message).not.toContain("credential");
    }
  });

  test("40. a second task revision of another task accepted for the same wait is revision conflict", async () => {
    const ctx = await closureReady({ extraOtherTaskRevision: true });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent }));
      const error = expectClosureError(cause, "revision_conflict");
      expect(error.message).toContain("carries several accepted task revisions");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!.closed).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("41. a normal hostile post-dispatch snapshot with an unchanged or larger revision is invalid state", async () => {
    for (const revisionDelta of [0, 2]) {
      const ctx = await closureReady();
      try {
        const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
        const { sink, closureDispatches } = mutationSink(ctx.sink, {
          mutateAfterClosure: (derived): PipelineV2RunState => {
            (derived as unknown as Record<string, unknown>)["revision"] = revisionBefore + revisionDelta;
            return derived;
          },
        });
        const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
        const error = expectClosureError(cause, "invalid_state");
        expect(error.message).toContain("does not carry the applied closure");
        // exactly one closure dispatch; the closure is durably recorded by
        // the underlying production sink; no success is returned
        expect(closureDispatches()).toBe(1);
        const durable = await validatePipelineV2RunState(JSON.parse(JSON.stringify(ctx.sink.snapshot)));
        const generation = durable.generations[0]!;
        expect(generation.iterations[generation.iterations.length - 1]!.closed).toEqual({
          by: "replanned",
          wait_index: 1,
          closed_transition_count: 2,
        });
        expect(durable.revision).toBe(revisionBefore + 1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("42. a racing hostile post-dispatch snapshot with an unchanged revision is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        race: true,
        mutateAfterClosure: (derived): PipelineV2RunState => {
          (derived as unknown as Record<string, unknown>)["revision"] = revisionBefore;
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect(closureDispatches()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("43. a normal hostile post-dispatch snapshot with a foreign run id is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const { sink } = mutationSink(ctx.sink, {
        mutateAfterClosure: (derived): PipelineV2RunState => {
          (derived as unknown as Record<string, unknown>)["run_id"] = "run-other";
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect((ctx.sink.snapshot as PipelineV2RunState).run_id).toBe(RUN_ID);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("44. the multi-iteration boundary applies the closure to the last iteration and keeps the historical prefix", async () => {
    const ctx = await closureReady({ secondIteration: true });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskClosure({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([{
        kind: "stage_iteration_closed",
        generationIndex: 1,
        iterationIndex: 2,
        by: "replanned",
        waitIndex: 1,
      }]);
      expect(result).toMatchObject({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 2,
        task_revision: 2,
      });
      const generation = (ctx.sink.snapshot as PipelineV2RunState).generations[0]!;
      expect(generation.iterations).toHaveLength(2);
      // the historical prefix keeps its exact closed projection
      expect(generation.iterations[0]!.closed).toEqual({
        by: "normal_close",
        closed_transition_count: 1,
      });
      expect(generation.iterations[0]!.opened_transition_count).toBe(0);
      // only the last target iteration carries the replanned closure
      expect(generation.iterations[1]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 4,
      });
      expect(generation.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("45. a hostile post-dispatch snapshot that changes a historical iteration is invalid state", async () => {
    const ctx = await closureReady({ secondIteration: true });
    try {
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        mutateAfterClosure: (derived): PipelineV2RunState => {
          const generation = derived.generations[0]!;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...generation,
            iterations: iterationMutation(generation),
          };
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect(closureDispatches()).toBe(1);
      // the durable state keeps the historical iteration unchanged
      const generation = (ctx.sink.snapshot as PipelineV2RunState).generations[0]!;
      expect(generation.iterations[0]!.opened_transition_count).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("46. a racing hostile snapshot that changes a historical iteration is not an idempotent success", async () => {
    const ctx = await closureReady({ secondIteration: true });
    try {
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        race: true,
        mutateAfterClosure: (derived): PipelineV2RunState => {
          const generation = derived.generations[0]!;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...generation,
            iterations: iterationMutation(generation),
          };
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect(closureDispatches()).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("47. a hostile post-dispatch snapshot with a mutated generation index is invalid state", async () => {
    const ctx = await closureReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { sink, closureDispatches } = mutationSink(ctx.sink, {
        mutateAfterClosure: (derived): PipelineV2RunState => {
          const generation = derived.generations[0]!;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...generation,
            index: 2,
          };
          return derived;
        },
      });
      const cause = await catchAccept(() => applyPipelineV2ReviseTaskClosure({ sink, intent: ctx.intent }));
      const error = expectClosureError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
      expect(closureDispatches()).toBe(1);
      // the underlying authoritative state keeps the original generation
      // index and carries the exact replanned closure of the real dispatch
      const durable = await validatePipelineV2RunState(JSON.parse(JSON.stringify(ctx.sink.snapshot)));
      expect(durable.generations).toHaveLength(1);
      expect(durable.generations[0]!.index).toBe(1);
      expect(durable.generations[0]!.open_iteration).toBeUndefined();
      expect(durable.generations[0]!.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
      expect(durable.revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

/**
 * The narrow hostile mutation of the historical iteration prefix: the
 * first iteration's opening anchor is changed while everything else stays
 * exact.
 */
function iterationMutation(generation: PipelineV2RunState["generations"][number]): PipelineV2RunState["generations"][number]["iterations"] {
  const first = generation.iterations[0]!;
  const rest = generation.iterations.slice(1);
  return [{ ...first, opened_transition_count: 99 }, ...rest];
}

test("38. the runtime export surfaces are exact (public two keys, internal two keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_revise_task_closure_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ReviseTaskClosureControllerError",
    "applyPipelineV2ReviseTaskClosure",
  ]);
  const internalModule = await import("../src/pipeline_v2_revise_task_closure_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ReviseTaskClosureControllerError",
    "applyPipelineV2ReviseTaskClosureInternal",
  ]);
});

test("39. the closure controller is pure state-and-sink (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_revise_task_closure_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("publishPipelineV2")).toBe(0);
  expect(countOf("loadPipelineV2PlanRevision")).toBe(0);
  expect(countOf("loadPipelineV2TaskRevision")).toBe(0);
  expect(countOf("prepareWaitIntent(")).toBe(0);
  expect(countOf("parseWaitIntent(")).toBe(0);
  expect(countOf("canonicalJson(")).toBe(0);
  expect(countOf("CryptoHasher")).toBe(0);
  expect(countOf("createHash")).toBe(0);
  expect(countOf("new WeakMap")).toBe(0);
  expect(countOf("new WeakSet")).toBe(0);
  expect(countOf("JSON.parse")).toBe(0);
  expect(countOf("O_EXCL")).toBe(0);
  expect(countOf("O_CREAT")).toBe(0);
  expect(countOf("O_NOFOLLOW")).toBe(0);
  expect(countOf("lstat")).toBe(0);
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  expect(countOf("acceptPipelineV2ReviseTaskIntent(")).toBe(0);
  expect(countOf("acceptPipelineV2ContinueStageIntent(")).toBe(0);
  expect(countOf("recordPipelineV2Wait")).toBe(0);
  expect(countOf("completePipelineV2ContinueStage(")).toBe(0);
  expect(countOf("acceptPipelineV2RunPlanCandidate(")).toBe(0);
  for (const banned of [
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "main.ts",
    "cli_",
    "docker",
    "launcher",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_run_plan_candidate",
    "pipeline_v2_run_plan_controller",
    "pipeline_v2_wait_store",
    "pipeline_v2_wait_manifest",
    "pipeline_v2_wait_controller",
    "pipeline_v2_revise_task_intent_controller",
    "pipeline_v2_continue_stage",
    "pipeline_state_store",
  ]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("Object.freeze")).toBe(0);
  expect(countOf("let production")).toBe(0);
});
