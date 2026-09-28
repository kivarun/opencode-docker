import { describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
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
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  PipelineV2CompiledRunPlanError,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { acceptPipelineV2ContinueStageIntent } from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import { applyPipelineV2ContinueStageGrant } from "../src/pipeline_v2_continue_stage_grant_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { closePipelineV2ReplannedGeneration } from "../src/pipeline_v2_replanned_generation_controller.ts";
import { openPipelineV2ReplannedStage } from "../src/pipeline_v2_replanned_stage_controller.ts";
import {
  openPipelineV2ReplannedStageTransitionInternal,
  PipelineV2ReplannedStageTransitionControllerError,
  type PipelineV2ReplannedStageTransitionControllerSink,
} from "../src/pipeline_v2_replanned_stage_transition_controller_internal.ts";
import { openPipelineV2ReplannedStageTransition, PipelineV2ReplannedStageTransitionControllerError as PublicError } from "../src/pipeline_v2_replanned_stage_transition_controller.ts";
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-repl-stage-transition-"));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-repl-stage-transition-run-"));
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
  return new Date(Date.UTC(2026, 9, 27, 0, 0, clockCounter));
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

interface ReadyCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: typeof INTENT;
  compiledPlan: ReturnType<typeof compilePipelineV2RunPlanCandidate>;
}

interface TransitionReadyOptions {
  withStageTwoGeneration?: boolean;
  withGrantHistory?: boolean;
  withDecisionExecution?: boolean;
}

/**
 * The real reducer/sink/controllers path to the composition boundary:
 * plan r1 accepted, generation/iteration opened, the stage cycle, the
 * revise intent and task revision durably accepted, the iteration closed
 * `by:"replanned"`, the revise_task response recorded, the settled
 * planning execution on the declared action target, and the next plan r2
 * accepted — the old generation is still open at this boundary (the
 * premature transition call). `withStageTwoGeneration` closes the old
 * generation and opens the next one bound to stage 2 (same template, so
 * the entry state is shared with stage 1).
 */
async function transitionReady(options: TransitionReadyOptions = {}): Promise<ReadyCtx> {
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
    const decisionShift = options.withDecisionExecution === true ? 1 : 0;
    if (options.withDecisionExecution === true) {
      // A historical decision execution with a correct selected result:
      // every later index shifts by one.
      await sink.dispatch({ kind: "start_decision_execution", stateId: "architect", inputDigest: hex("c"), executionRole: "control" });
      await sink.dispatch({
        kind: "decision_evaluated",
        result: { status: "selected", outcome: "iterate", decision: "iterate", rule_id: "rule-1", active_constraint_ids: [] },
      });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "iterate", to: "architect", transition_index: 0 },
        executionIndex: 1,
      });
    }
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
      origin_execution: 1 + decisionShift,
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
      executionIndex: 1 + decisionShift,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 2 + decisionShift,
    });
    if (options.withGrantHistory === true) {
      // A real continue-stage intervention extends generation 1 by one
      // iteration: the durable grant ledger becomes non-empty before the
      // revise cycle starts (the revision-1 tasks in the ledgers stay
      // untouched by it).
      const actions = [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ];
      const grantRequest = preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 1,
        transition_count: 2 + decisionShift,
        state_id: "architect",
        reason: "user_intervention",
        actions,
      });
      await publishPipelineV2WaitRequest(fixture.runRoot, grantRequest.manifest);
      await sink.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "user_intervention",
        requestSha256: grantRequest.sha256,
        actions,
      });
      const continueIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: plan1.sha256,
        additional_iterations: 1,
      });
      await acceptPipelineV2ContinueStageIntent({ runRoot: fixture.runRoot, sink, intent: continueIntent });
      await applyPipelineV2ContinueStageGrant({ sink, intent: continueIntent });
      await sink.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: grantRequest.sha256,
        actionId: "continue_stage",
        responseSha256: hex("a"),
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 + decisionShift });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 2 });
      for (const command of agentPhases("stage2")) {
        await sink.dispatch(command);
      }
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 3 + decisionShift,
      });
    }
    const reviseWaitIndex = options.withGrantHistory === true ? 2 : 1;
    const reviseWaitTransitionCount = (options.withGrantHistory === true ? 3 : 2) + decisionShift;
    const actions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: reviseWaitIndex,
      transition_count: reviseWaitTransitionCount,
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
    const reviseIntent = options.withGrantHistory === true
      ? prepareWaitIntent({
          schema_version: 1,
          kind: "revise_task_intent",
          run_id: RUN_ID,
          wait_index: 2,
          task_id: "task-a",
          expected_previous_task_sha256: A1.sha256,
          new_task_revision_sha256: A2.sha256,
        })
      : INTENT;
    await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: reviseWaitIndex, intentSha256: reviseIntent.sha256 });
    await sink.dispatch({
      kind: "task_revision_accepted",
      taskId: "task-a",
      revision: 2,
      taskSha256: A2.sha256,
      waitIndex: reviseWaitIndex,
      intentSha256: reviseIntent.sha256,
    });
    await sink.dispatch({
      kind: "stage_iteration_closed",
      generationIndex: 1,
      iterationIndex: options.withGrantHistory === true ? 2 : 1,
      by: "replanned",
      waitIndex: reviseWaitIndex,
    });
    await sink.dispatch({
      kind: "wait_response_recorded",
      waitIndex: reviseWaitIndex,
      expectedRequestSha256: request.sha256,
      actionId: "revise_task",
      responseSha256: hex("e"),
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning3")) {
      await sink.dispatch(command);
    }
    const planningExecutionIndex = (options.withGrantHistory === true ? 4 : 3) + decisionShift;
    const stages2 = options.withStageTwoGeneration === true
      ? [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
          },
          {
            id: "stage-2",
            template: "development",
            tasks: [{ id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] }],
          },
        ]
      : [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
          },
        ];
    const plan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: planningExecutionIndex,
      stages: stages2,
    });
    const candidate2 = preparePipelineV2RunPlanCandidate({
      plan: plan2,
      taskRevisions: options.withStageTwoGeneration === true ? [A2, B1] : [A2],
      previousPlan: plan1,
      previousTaskRevisions: [A1],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidate2 });
    if (options.withStageTwoGeneration === true) {
      await closePipelineV2ReplannedGeneration({ sink, intent: reviseIntent, compiledPlan: accepted.compiled_plan });
      await sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-2",
        stagePosition: 2,
        templateId: "development",
        planSha256: plan2.sha256,
        initialBudget: 2,
        transitionCount: reviseWaitTransitionCount,
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: reviseWaitTransitionCount });
    }
    return { fixture, sink, pipeline, intent: reviseIntent, compiledPlan: accepted.compiled_plan };
  });
}

interface CountingSink extends PipelineV2ReplannedStageTransitionControllerSink {
  commands: PipelineV2RunCommand[];
}

function countingSink(inner: PipelineV2RunStateSink): CountingSink {
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
): PipelineV2ReplannedStageTransitionControllerSink {
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
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      await inner.dispatch(command);
    },
  };
}

function resolveWithoutChangeSink(snapshot: PipelineV2RunState | null): PipelineV2ReplannedStageTransitionControllerSink {
  return {
    snapshot,
    poisoned: false,
    async dispatch() {
      // Resolves without any durable change.
    },
  };
}

function expectTransitionError(cause: unknown): PipelineV2ReplannedStageTransitionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReplannedStageTransitionControllerError);
  return cause as PipelineV2ReplannedStageTransitionControllerError;
}

function callTransition(ctx: ReadyCtx, sink: PipelineV2ReplannedStageTransitionControllerSink): Promise<unknown> {
  return openPipelineV2ReplannedStageTransitionInternal({
    pipeline: ctx.pipeline,
    sink,
    intent: ctx.intent,
    compiledPlan: ctx.compiledPlan,
    stageId: "stage-1",
    initialBudget: 2,
  });
}

describe("openPipelineV2ReplannedStageTransition", () => {
  test("1. the premature call before the composition is invalid_state with zero dispatch and no durable change", async () => {
    const ctx = await transitionReady();
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      expect(before.generations).toHaveLength(1);
      expect(before.generations[0]!.closed).toBeUndefined();
      const recording = countingSink(ctx.sink);
      const cause = await callTransition(ctx, recording).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect(error.state).not.toBeNull();
      expect(recording.commands).toEqual([]);
      const after = ctx.sink.snapshot as PipelineV2RunState;
      expect(JSON.stringify(after)).toBe(JSON.stringify(before));
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. C0 after the composition commits exactly one exact planning transition", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const before = ctx.sink.snapshot as PipelineV2RunState;
      const recording = countingSink(ctx.sink);
      const result = (await callTransition(ctx, recording)) as {
        wait_index: number;
        from_state: string;
        to_state: string;
        transition_index: number;
        execution_index: number;
        generation_index: number;
        iteration_index: number;
        stage_id: string;
        stage_position: number;
        template_id: string;
        initial_budget: number;
        plan_revision: number;
        plan_sha256: string;
        state: PipelineV2RunState;
      };
      expect(recording.commands).toHaveLength(1);
      expect(recording.commands[0]).toEqual({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      expect(result.wait_index).toBe(1);
      expect(result.from_state).toBe("architect");
      expect(result.to_state).toBe("dev_entry");
      expect(result.transition_index).toBe(0);
      expect(result.execution_index).toBe(3);
      expect(result.generation_index).toBe(2);
      expect(result.iteration_index).toBe(1);
      expect(result.stage_id).toBe("stage-1");
      expect(result.stage_position).toBe(1);
      expect(result.template_id).toBe("development");
      expect(result.initial_budget).toBe(2);
      expect(result.plan_revision).toBe(2);
      expect(result.plan_sha256).toBe(ctx.compiledPlan.plan_sha256);
      // The loader round-trip accepts the committed state.
      const after = validatePipelineV2RunState(ctx.sink.snapshot as PipelineV2RunState);
      expect(after.revision).toBe(before.revision + 1);
      expect(after.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
      expect(after.transitions).toHaveLength(3);
      expect(after.transitions[2]).toEqual({
        index: 0,
        from: "architect",
        outcome: "completed",
        to: "dev_entry",
        execution_index: 3,
      });
      expect(after.executions).toHaveLength(3);
      expect(after.executions[2]!.phase).toBe("cleanup_completed");
      expect(after.generations).toHaveLength(2);
      expect(after.generations[0]!.closed?.by).toBe("replanned");
      expect(after.generations[1]!.open_iteration).toEqual({ index: 1, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. a foreign stage with a shared entry state is lifecycle_conflict before dispatch", async () => {
    const ctx = await transitionReady({ withStageTwoGeneration: true });
    try {
      // Both compiled stages carry the same template, so the resolved
      // `to` would be identical; only the exact stage binding separates
      // the calls.
      const stage1 = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, "stage-1");
      const stage2 = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, "stage-2");
      expect(stage1.entry_state).toBe(stage2.entry_state);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.generations[1]!.stage_id).toBe("stage-2");
      const recording = countingSink(ctx.sink);
      const cause = await openPipelineV2ReplannedStageTransitionInternal({
        pipeline: ctx.pipeline,
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. a foreign caller budget is lifecycle_conflict before dispatch", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const recording = countingSink(ctx.sink);
      const cause = await openPipelineV2ReplannedStageTransitionInternal({
        pipeline: ctx.pipeline,
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 3,
      }).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the exact durable transition is a zero-dispatch C1 retry", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const first = await callTransition(ctx, ctx.sink);
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = countingSink(ctx.sink);
      const second = await callTransition(ctx, recording);
      expect(recording.commands).toEqual([]);
      expect(second).toEqual(first);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. a durable transition changed in one field is lifecycle_conflict with zero dispatch", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      await callTransition(ctx, ctx.sink);
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        const last = state.transitions[state.transitions.length - 1] as unknown as Record<string, unknown>;
        // The outcome is not loader-checked for an agent transition, so
        // the mutation stays a structurally valid document while the
        // C1 recognition must still refuse the changed field.
        last["outcome"] = "other";
      });
      const recording = countingSink(hostile as unknown as PipelineV2RunStateSink) as unknown as CountingSink;
      const cause = await callTransition(ctx, recording).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. a dispatch that resolves without the durable change is invalid_state", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const before = ctx.sink.snapshot as PipelineV2RunState;
      const fake = resolveWithoutChangeSink(before);
      const cause = await callTransition(ctx, fake).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(before.revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. not_committed keeps the previous snapshot and a fresh retry commits the transition", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
        now: nextTick,
      });
      const cause = await callTransition(ctx, faultedSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("state_persist_failed");
      // The state document keeps the previous snapshot (no transition).
      const observerSink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      expect((observerSink.snapshot as PipelineV2RunState).transitions).toHaveLength(2);
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      await callTransition(ctx, retrySink);
      const after = validatePipelineV2RunState(retrySink.snapshot as PipelineV2RunState);
      expect(after.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
      expect(after.transitions).toHaveLength(3);
      expect(after.transitions[2]!.execution_index).toBe(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. durability_unknown adopts the candidate and a fresh retry is a zero-dispatch recognition", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const faultedSink = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
        now: nextTick,
      });
      const cause = await callTransition(ctx, faultedSink).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("state_persist_failed");
      // The candidate is visible on disk: the transition is durable.
      const observerSink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const durable = observerSink.snapshot as PipelineV2RunState;
      expect(durable.transitions).toHaveLength(3);
      expect(durable.cursor).toEqual({ current_state: "dev_entry", transition_count: 3 });
      const retrySink = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const revisionBefore = durable.revision;
      const result = await callTransition(ctx, retrySink);
      expect((result as { state: PipelineV2RunState }).state.revision).toBe(revisionBefore);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. provenance gates: forged intents and plans, Proxy pipelines and hostile budgets are rejected before any dispatch", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const recording = countingSink(ctx.sink);
      const base = {
        pipeline: ctx.pipeline,
        sink: recording,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        stageId: "stage-1",
        initialBudget: 2,
      };
      const handBuiltIntent = {
        manifest: (ctx.intent as unknown as { manifest: unknown }).manifest,
        sha256: (ctx.intent as unknown as { sha256: string }).sha256,
      };
      const causeIntent = await openPipelineV2ReplannedStageTransitionInternal({ ...base, intent: handBuiltIntent }).catch((error) => error);
      expect(causeIntent).toBeInstanceOf(PipelineV2ReplannedStageTransitionControllerError);
      expect((causeIntent as PipelineV2ReplannedStageTransitionControllerError).reason).toBe("invalid_options");
      const clonedPlan = structuredClone(ctx.compiledPlan) as unknown;
      const causePlan = await openPipelineV2ReplannedStageTransitionInternal({ ...base, compiledPlan: clonedPlan }).catch((error) => error);
      // The compiled plan is resolved through the trusted compiled
      // resolver, whose provenance gate rejects clones with its own
      // typed error (pass-through by identity).
      expect(causePlan).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      let proxyTraps = 0;
      const proxiedPipeline = new Proxy(ctx.pipeline, {
        get() {
          proxyTraps += 1;
          throw new Error("no trap may fire");
        },
      });
      const causePipeline = await openPipelineV2ReplannedStageTransitionInternal({ ...base, pipeline: proxiedPipeline }).catch((error) => error);
      // The pipeline is only read through the engine resolver, whose
      // provenance gate rejects the Proxy with zero traps.
      expect(causePipeline).toBeInstanceOf(PipelineError);
      expect(proxyTraps).toBe(0);
      const causeBudget = await openPipelineV2ReplannedStageTransitionInternal({ ...base, initialBudget: 0 }).catch((error) => error);
      expect(causeBudget).toBeInstanceOf(PipelineV2ReplannedStageTransitionControllerError);
      expect((causeBudget as PipelineV2ReplannedStageTransitionControllerError).reason).toBe("invalid_options");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

/**
 * A sink that really commits every dispatch but presents a hostile clone
 * of the authoritative snapshot AFTER the dispatch: the transition
 * record, cursor and revision delta stay exact while one injected field
 * is changed. The pre-dispatch snapshot stays the clean authoritative
 * state, so the comparison baseline is untouched.
 */
function mutatedResultSink(
  inner: PipelineV2RunStateSink,
  mutate: (state: PipelineV2RunState) => void,
): PipelineV2ReplannedStageTransitionControllerSink {
  let dispatched = false;
  return {
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
    async dispatch(command: PipelineV2RunCommand) {
      await inner.dispatch(command);
      dispatched = true;
    },
  };
}

describe("post-dispatch full verification", () => {
  test("13. a dispatch result with a changed historical profile is invalid_state", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const hostile = mutatedResultSink(ctx.sink, (state) => {
        ((state.executions[0] as unknown as Record<string, unknown>))["profile"] = "other";
      });
      const cause = await callTransition(ctx, hostile).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      // The real durable transition stayed committed (the sink really
      // dispatched); the controller refused the hostile presentation.
      expect((ctx.sink.snapshot as PipelineV2RunState).transitions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a changed schema_version is invalid_state", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const hostile = mutatedResultSink(ctx.sink, (state) => {
        (state as unknown as Record<string, unknown>)["schema_version"] = 6;
      });
      const cause = await callTransition(ctx, hostile).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a changed protected input digest is invalid_state", async () => {
    const ctx = await transitionReady();
    try {
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const hostile = mutatedResultSink(ctx.sink, (state) => {
        ((state.inputs[0] as unknown as Record<string, unknown>))["digest"] = hex("1");
      });
      const cause = await callTransition(ctx, hostile).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. a changed historical grant is invalid_state (real grant fixture)", async () => {
    const ctx = await transitionReady({ withGrantHistory: true });
    try {
      expect((ctx.sink.snapshot as PipelineV2RunState).grants).toHaveLength(1);
      await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
      const hostile = mutatedResultSink(ctx.sink, (state) => {
        ((state.grants[0] as unknown as Record<string, unknown>))["additional_iterations"] = 2;
      });
      const cause = await callTransition(ctx, hostile).catch((error) => error);
      const error = expectTransitionError(cause);
      expect(error.reason).toBe("invalid_state");
      expect((ctx.sink.snapshot as PipelineV2RunState).transitions).toHaveLength(4);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. hostile historical decision results are invalid_state", async () => {
    // Every variant needs its own fresh C0 boundary: the hostile sink
    // really dispatches, so the first attempt commits the transition.
    const variants: Array<(state: PipelineV2RunState) => void> = [
      (state) => {
        ((state.executions[0] as unknown as Record<string, unknown>))["result"] = null;
      },
      (state) => {
        const execution = state.executions[0] as unknown as Record<string, unknown>;
        execution["result"] = { status: "selected", outcome: "iterate", decision: "iterate", rule_id: "rule-1", active_constraint_ids: ["injected"] };
      },
      (state) => {
        const execution = state.executions[0] as unknown as Record<string, unknown>;
        execution["result"] = { status: "selected", outcome: "iterate", decision: "iterate", rule_id: "rule-1", active_constraint_ids: [], violated_relation_ids: ["fc-1"] };
      },
      (state) => {
        const execution = state.executions[0] as unknown as Record<string, unknown>;
        execution["result"] = { status: "selected", outcome: "iterate", decision: "iterate", rule_id: "rule-other", active_constraint_ids: [] };
      },
      (state) => {
        // A control-role decision execution carries no iteration_index;
        // injecting one is a change of a schema-owned execution field.
        ((state.executions[0] as unknown as Record<string, unknown>))["iteration_index"] = 2;
      },
    ];
    for (const variant of variants) {
      const ctx = await transitionReady({ withDecisionExecution: true });
      try {
        expect(((ctx.sink.snapshot as PipelineV2RunState).executions[0] as unknown as Record<string, unknown>)["type"]).toBe("decision");
        await openPipelineV2ReplannedStage({ sink: ctx.sink, intent: ctx.intent, compiledPlan: ctx.compiledPlan, stageId: "stage-1", initialBudget: 2 });
        const hostile = mutatedResultSink(ctx.sink, variant);
        const cause = await callTransition(ctx, hostile).catch((error) => error);
        const error = expectTransitionError(cause);
        expect(error.reason).toBe("invalid_state");
        // The real durable transition stayed committed (the sink really
        // dispatched); the controller refused the hostile presentation.
        expect((ctx.sink.snapshot as PipelineV2RunState).transitions).toHaveLength(4);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });
});

test("11. the runtime export surfaces are exact (public two keys, internal two keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_replanned_stage_transition_controller.ts");
  const internalModule = await import("../src/pipeline_v2_replanned_stage_transition_controller_internal.ts");
  expect(Object.keys(publicModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedStageTransitionControllerError",
    "openPipelineV2ReplannedStageTransition",
  ]);
  expect(Object.keys(internalModule).filter((key) => key !== "__esModule").sort()).toEqual([
    "PipelineV2ReplannedStageTransitionControllerError",
    "openPipelineV2ReplannedStageTransitionInternal",
  ]);
  expect(PublicError).toBe(PipelineV2ReplannedStageTransitionControllerError);
});

test("12. the controller composes the existing layers only (source scan)", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_replanned_stage_transition_controller_internal.ts"), "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("const resolved = compiledTransitionFor(")).toBe(1);
  expect(countOf("comparePipelineV2RunIdentity(")).toBe(2);
  expect(countOf("compiledRunPlanOriginIdentity(")).toBe(1);
  expect(countOf("hasPreparedRunPlanProvenance(")).toBe(1);
  expect(countOf("compiledPipelineV2RunPlanStageFor(")).toBe(1);
  expect(countOf(".message")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  expect(countOf("JSON.parse")).toBe(0);
  expect(countOf("createHash")).toBe(0);
  expect(countOf("new WeakMap")).toBe(0);
  expect(countOf("new WeakSet")).toBe(0);
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf("publishPipelineV2")).toBe(0);
  expect(countOf("acceptPipelineV2")).toBe(0);
  expect(countOf("recordPipelineV2")).toBe(0);
  expect(countOf("closePipelineV2")).toBe(0);
  expect(countOf("ensurePipelineV2StageIteration(")).toBe(0);
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
    "pipeline_v2_wait_store",
    "pipeline_v2_wait_manifest",
    "pipeline_v2_wait_controller",
    "pipeline_v2_wait_respond",
    "pipeline_v2_revise_task",
    "pipeline_v2_continue_stage",
    "pipeline_v2_stage_iteration_controller",
    "pipeline_v2_replanned_generation_controller",
    "pipeline_v2_replanned_stage_controller",
  ]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("Object.freeze")).toBe(0);
  expect(countOf("let production")).toBe(0);
});
