import { describe, expect, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  PipelineV2RunPlanManifestError,
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
import { compilePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_compiled.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  PipelineV2RunPlanStoreError,
  loadPipelineV2PlanRevision,
  publishPipelineV2WaitIntent,
} from "../src/pipeline_v2_run_plan_store.ts";
import {
  loadPipelineV2PlanRevisionWithIo,
  publishPipelineV2WaitIntentWithIo,
  realRunPlanStoreIo,
} from "../src/pipeline_v2_run_plan_store_internal.ts";
import { PipelineV2RunPlanBindingError } from "../src/pipeline_v2_run_plan_bindings.ts";
import {
  PipelineV2RunStateStoreError,
} from "../src/pipeline_v2_state_store.ts";
import type { ImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import {
  acceptPipelineV2ContinueStageIntent,
  PipelineV2ContinueStageIntentControllerError,
  type PipelineV2ContinueStageIntentControllerFailureReason,
  type PipelineV2ContinueStageIntentControllerSink,
} from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import {
  acceptPipelineV2ContinueStageIntentWithIo,
  precheckPlanIntentAcceptance,
  productionContinueStageIntentOps,
  type PipelineV2ContinueStageIntentControllerOps,
} from "../src/pipeline_v2_continue_stage_intent_controller_internal.ts";
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-intent-"));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-intent-run-"));
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

interface IntentCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  planSha256: string;
}

interface WaitingOptions {
  acceptPlan?: boolean;
  planRevision2?: boolean;
  ensureStage?: boolean;
  stageExecution?: boolean;
  closeIteration?: boolean;
  enterWait?: boolean;
  actions?: Array<{ id: string; to: string }>;
}

/**
 * A real sink driven through the real reducer to the waiting boundary:
 * the accepted plan, the open generation and iteration, the settled and
 * bound stage execution, and the open wait declaring `continue_stage`.
 */
async function waitingReady(
  options: WaitingOptions = {},
  sinkActions = [
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "architect" },
  ],
): Promise<IntentCtx> {
  const acceptPlan = options.acceptPlan ?? true;
  const ensureStage = options.ensureStage ?? acceptPlan;
  const stageExecution = options.stageExecution ?? ensureStage;
  const closeIteration = options.closeIteration ?? false;
  const enterWait = options.enterWait ?? true;
  const actions = sinkActions;
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning")) {
      await sink.dispatch(command);
    }
    let planSha256 = "";
    let plan1: ReturnType<typeof preparePlanRevisionManifest> | null = null;
    let compiledPlan: ReturnType<typeof compilePipelineV2RunPlanCandidate> | null = null;
    if (acceptPlan) {
      plan1 = preparePlanRevisionManifest({
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
      const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
        plan: plan1,
        taskRevisions: [A1],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      });
      const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
      planSha256 = plan1.sha256;
      compiledPlan = accepted.compiled_plan;
    }
    if (ensureStage) {
      if (compiledPlan === null || plan1 === null) {
        throw new Error("fixture plan revision missing");
      }
      await ensurePipelineV2StageIteration({ compiledPlan: compiledPlan, stageId: "stage-1", initialBudget: 2, sink });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
      if (stageExecution) {
        await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
        for (const command of agentPhases("stage")) {
          await sink.dispatch(command);
        }
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
          executionIndex: 2,
        });
      }
    } else {
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
    }
    if (closeIteration) {
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
    }
    // a second plan revision is accepted after the first iteration is
    // closed; the open generation keeps binding the first revision's
    // digest while a second iteration is opened afterwards
    if (options.planRevision2 === true) {
      if (plan1 === null) {
        throw new Error("fixture plan revision missing");
      }
      await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("replanning")) {
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
      await acceptPipelineV2RunPlanCandidate({
        pipeline,
        runRoot: fixture.runRoot,
        sink,
        candidate: preparePipelineV2RunPlanCandidate({
          plan: plan2,
          taskRevisions: [A1],
          previousPlan: plan1,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        }),
      });
      planSha256 = plan2.sha256;
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 3 });
    }
    const waitStateId = options.planRevision2 === true ? "dev_entry" : (ensureStage ? "architect" : "dev_entry");
    if (enterWait) {
      await sink.dispatch({
        kind: "run_waiting",
        stateId: waitStateId,
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("1"),
        actions,
      });
    }
    return { fixture, sink, pipeline, planSha256 };
  });
}

function intentValue(ctx: IntentCtx, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 1,
    stage_id: "stage-1",
    expected_plan_sha256: ctx.planSha256,
    additional_iterations: 2,
    ...overrides,
  };
}

function preparedIntent(ctx: IntentCtx, overrides: Record<string, unknown> = {}): PreparedPipelineV2RunWaitIntent {
  return prepareWaitIntent(intentValue(ctx, overrides));
}

interface RecordingSink extends PipelineV2ContinueStageIntentControllerSink {
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

async function catchAccept(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectControllerError(cause: unknown, reason: PipelineV2ContinueStageIntentControllerFailureReason): PipelineV2ContinueStageIntentControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinueStageIntentControllerError);
  const error = cause as PipelineV2ContinueStageIntentControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

describe("acceptPipelineV2ContinueStageIntent", () => {
  test("1. happy path: real reducer, sink and store accept the intent in the exact order", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const recording = recordingSink(ctx.sink);
      const result = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent });
      expect(recording.commands).toEqual([
        { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: intent.sha256 },
      ]);
      expect(result).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      const wait = result.state.waits[0];
      expect(wait?.intent).toEqual({ intent_sha256: intent.sha256 });
      expect(wait?.response).toBeUndefined();
      const stored = await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8");
      expect(stored).toBe(intent.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the accepted state round-trips through the loader", async () => {
    const ctx = await waitingReady();
    try {
      const result = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) });
      validatePipelineV2RunState(JSON.parse(JSON.stringify(result.state)) as never);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.waits[0]?.intent?.intent_sha256).toBe(result.intent_sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the result carries exactly the content-free fields and is deep-frozen", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const result = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      expect(Object.keys(result).sort()).toEqual(["intent_sha256", "state", "wait_index"]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      const resultText = JSON.stringify(result);
      expect(resultText).not.toContain("canonical_json");
      expect(resultText).not.toContain(ctx.fixture.runRoot);
      expect(resultText).not.toContain(intent.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

  test("4. the current plan is loaded only from the durable ledger through the store", async () => {
    const ctx = await waitingReady();
    try {
      const loadCalls: Array<[string, number]> = [];
      const publishCalls: Array<[string, unknown]> = [];
      const ops: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: async (runRoot, revision) => {
          loadCalls.push([runRoot, revision]);
          return await loadPipelineV2PlanRevision(runRoot, revision);
        },
        publishWaitIntent: async (runRoot, manifest) => {
          publishCalls.push([runRoot, manifest]);
          return await publishPipelineV2WaitIntent(runRoot, manifest);
        },
      };
      const intent = preparedIntent(ctx);
      await acceptPipelineV2ContinueStageIntentWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      expect(loadCalls).toEqual([[ctx.fixture.runRoot, 1]]);
      expect(publishCalls).toEqual([[ctx.fixture.runRoot, intent.manifest]]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the binding validator really catches a wrong expected plan digest", async () => {
    const ctx = await waitingReady();
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { expected_plan_sha256: hex("0") }) }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
      expect((cause as Error).message).toContain("expected_plan_sha256 does not name the prepared plan digest");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. an intent stage the plan does not declare keeps the binding error class", async () => {
    // the open generation's stage id is "stage-9" (the reducer's generation
    // opening checks the plan digest only); the intent names the same
    // stage, so the durable boundary passes and the binding validator is
    // the layer that rejects
    const ctx = await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
      await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning")) {
        await sink.dispatch(command);
      }
      const candidate = preparePipelineV2RunPlanCandidate({
        plan: preparePlanRevisionManifest({
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
        }),
        taskRevisions: [A1],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      });
      const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
      await sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-9",
        stagePosition: 1,
        templateId: "development",
        planSha256: accepted.compiled_plan.plan_sha256,
        initialBudget: 2,
        transitionCount: 0,
      });
      await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 });
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
      await sink.dispatch({
        kind: "run_waiting",
        stateId: "dev_entry",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("1"),
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
      return { fixture, sink, pipeline, planSha256: accepted.compiled_plan.plan_sha256 };
    });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { stage_id: "stage-9" }) }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
      expect((cause as Error).message).toContain("names a stage the plan does not declare");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. an intent stage differing from the open generation stage is rejected", async () => {
    const ctx = await waitingReady();
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { stage_id: "stage-2" }) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("belongs to stage");
      expect(error.state).not.toBeNull();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. an open generation not belonging to the last durable plan is rejected", async () => {
    // a second plan revision is accepted after the first iteration is
    // closed; the second iteration is reopened afterwards, so the open
    // generation carries an open iteration while binding the first
    // revision's digest
    const ctx = await waitingReady({ planRevision2: true, closeIteration: true });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("does not belong to the last durable plan revision");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. a run without a durable plan revision is rejected", async () => {
    const ctx = await waitingReady({ acceptPlan: false });
    try {
      const intent = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: hex("e"),
        additional_iterations: 2,
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("no durable plan revision");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. an unpublished durable plan revision is a typed invalid_state", async () => {
    const ctx = await waitingReady();
    try {
      await unlink(join(ctx.fixture.runRoot, "run-plan", "plans", "1.json"));
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("is not published on the run's data plane");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. malformed, noncanonical, wrong-mode and symlinked plan artifacts keep their store error classes", async () => {
    for (const corruption of ["malformed", "noncanonical", "wrong-mode", "symlink"] as const) {
      const ctx = await waitingReady();
      try {
        const planPath = join(ctx.fixture.runRoot, "run-plan", "plans", "1.json");
        if (corruption === "malformed") {
          await writeFile(planPath, "{not-json");
        } else if (corruption === "noncanonical") {
          await writeFile(planPath, `${await readFile(planPath, "utf8")}\n`);
        } else if (corruption === "wrong-mode") {
          await chmod(planPath, 0o644);
        } else {
          await unlink(planPath);
          const target = join(ctx.fixture.root, "plan-target.json");
          await writeFile(target, "{}");
          await symlink(target, planPath);
        }
        const cause = await catchAccept(() =>
          acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
        );
        if (corruption === "malformed") {
          expect(cause).toBeInstanceOf(PipelineV2RunPlanManifestError);
        } else {
          expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
        }
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("12. an intent of a foreign run is rejected", async () => {
    const ctx = await waitingReady();
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { run_id: "run-other" }) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("another run than the durable run state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. an intent for another wait index is rejected", async () => {
    const ctx = await waitingReady();
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { wait_index: 2 }) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("names wait index 2, but the open wait record is 1");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a wait without the declared continue_stage action is rejected", async () => {
    const ctx = await waitingReady({ enterWait: true }, [{ id: "revise_task", to: "architect" }]);
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("does not declare the continue_stage action");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a run without an open stage generation is rejected", async () => {
    const ctx = await waitingReady({ ensureStage: false });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("no open stage generation");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. an open generation without an open iteration is rejected", async () => {
    const ctx = await waitingReady({ closeIteration: true });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx) }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("carries no open iteration");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. the run is not waiting (active boundary and answered wait)", async () => {
    const active = await waitingReady({ enterWait: false });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: active.fixture.runRoot, sink: active.sink, intent: preparedIntent(active) }),
      );
      expectControllerError(cause, "invalid_state");
      expect((cause as Error).message).toContain("the run is not waiting");
    } finally {
      await disposeRun(active.fixture);
    }
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      // the response requires no open iteration; the durable intervention
      // closes it through the existing grant path
      await ctx.sink.dispatch({ kind: "iteration_grant_recorded", generationIndex: 1, waitIndex: 1, intentSha256: intent.sha256, additionalIterations: 2 });
      await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 });
      await ctx.sink.dispatch({ kind: "wait_response_recorded", waitIndex: 1, expectedRequestSha256: hex("1"), actionId: "continue_stage", responseSha256: hex("9") });
      expect(ctx.sink.snapshot?.status).toBe("active");
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent }),
      );
      expectControllerError(cause, "invalid_state");
      expect((cause as Error).message).toContain("the run is not waiting");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. an exact durable retry dispatches nothing and returns the authoritative state", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      const revision = ctx.sink.snapshot?.revision;
      if (typeof revision !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const recording = recordingSink(ctx.sink);
      const retry = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      expect(retry.state.revision).toBe(revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a durable intent with a removed intent file restores the publication without dispatch", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      const intentPath = join(ctx.fixture.runRoot, "run-plan", "intents", "1.json");
      const mode = (await lstat(intentPath)).mode;
      await unlink(intentPath);
      const recording = recordingSink(ctx.sink);
      const retry = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent });
      expect(recording.commands).toEqual([]);
      expect(retry.state.status).toBe("waiting");
      const restored = await lstat(intentPath);
      expect(restored.mode).toBe(mode);
      expect(await readFile(intentPath, "utf8")).toBe(intent.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a different durable intent is a conflict before any filesystem read", async () => {
    const ctx = await waitingReady();
    try {
      const first = preparedIntent(ctx, { additional_iterations: 2 });
      await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: first });
      const other = preparedIntent(ctx, { additional_iterations: 5 });
      const commands: PipelineV2RunCommand[] = [];
      let planLoads = 0;
      const hostileOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: async () => {
          planLoads += 1;
          throw new Error("the plan loader must not be called for a conflicting intent");
        },
        publishWaitIntent: async () => {
          throw new Error("the intent publisher must not be called for a conflicting intent");
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntentWithIo(hostileOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: other }),
      );
      expectControllerError(cause, "intent_conflict");
      expect((cause as Error).message).toContain("already accepted a different continue_stage intent");
      expect(planLoads).toBe(0);
      expect(commands).toEqual([]);
      expect(await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8")).toBe(first.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. the reducer pre-check rejection is a typed invalid_state with zero side effects", async () => {
    const active = await waitingReady({ enterWait: false });
    try {
      const state = active.sink.snapshot;
      if (state === null || state.status !== "active") {
        throw new Error("fixture boundary missing");
      }
      const command: PipelineV2RunCommand = {
        kind: "plan_intent_accepted",
        waitIndex: 1,
        intentSha256: hex("c"),
      };
      let caught: unknown;
      try {
        precheckPlanIntentAcceptance(state, command, state);
      } catch (cause) {
        caught = cause;
      }
      const error = expectControllerError(caught, "invalid_state");
      expect(error.message).toContain("does not accept the plan intent acceptance");
      expect(error.state).toBe(state);
    } finally {
      await disposeRun(active.fixture);
    }
  });

  test("22. a publication conflict dispatches nothing", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const other = preparedIntent(ctx, { additional_iterations: 3 });
      await publishPipelineV2WaitIntent(ctx.fixture.runRoot, other.manifest);
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
      expect(recording.commands).toEqual([]);
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. a hostile publisher result fails closed before the dispatch", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const hostileOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: (runRoot, revision) => loadPipelineV2PlanRevision(runRoot, revision),
        publishWaitIntent: async () => {
          const forged = preparedIntent(ctx, { additional_iterations: 4 });
          return { intent: forged, intent_path: "forged" };
        },
      };
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntentWithIo(hostileOps, { runRoot: ctx.fixture.runRoot, sink: recording, intent }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("does not match the accepted continue_stage intent");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a dispatch that resolves without a snapshot change is rejected", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const hostileSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        dispatch() {
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: hostileSink, intent }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted intent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. a racing identical dispatch is idempotent success on the exact durable record", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const realDispatch = ctx.sink.dispatch.bind(ctx.sink);
      const commands: PipelineV2RunCommand[] = [];
      const racingSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          commands.push(command);
          await realDispatch(command);
          throw new PipelineV2StateError("the intent is already accepted (simulated race)");
        },
      };
      const result = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: racingSink, intent });
      expect(result).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      const revision = ctx.sink.snapshot?.revision;
      if (typeof revision !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      expect(result.state.revision).toBe(revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. a not-committed intent acceptance leaves the orphan file and an exact retry commits", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent }),
      );
      const error = expectControllerError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be committed");
      const orphan = await lstat(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"));
      expect(orphan.mode & 0o777).toBe(0o600);
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      const retry = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      expect(retry).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      expect(retry.state.waits[0]?.intent).toEqual({ intent_sha256: intent.sha256 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. durability-unknown adopts the candidate, poisons the sink and a fresh retry dispatches nothing", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent }),
      );
      const error = expectControllerError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be confirmed durable");
      expect(error.state).not.toBeNull();
      expect(error.state?.waits[0]?.intent).toEqual({ intent_sha256: intent.sha256 });
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. two identical intents racing publish one inode and commit once", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      let release!: () => void;
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      let entered = 0;
      let openBarrier!: () => void;
      const barrier = new Promise<void>((resolve) => {
        openBarrier = resolve;
      });
      const barrierIo: ImmutableDocumentIo = {
        ...realRunPlanStoreIo,
        link: async (existingPath: string, newPath: string) => {
          entered += 1;
          if (entered === 2) {
            openBarrier();
          }
          await barrier;
          release();
          return await realRunPlanStoreIo.link(existingPath, newPath);
        },
      };
      const barrierOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: (runRoot, revision) => loadPipelineV2PlanRevision(runRoot, revision),
        publishWaitIntent: (runRoot, manifest) => publishPipelineV2WaitIntentWithIo(barrierIo, runRoot, manifest),
      };
      const first = acceptPipelineV2ContinueStageIntentWithIo(barrierOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      const second = acceptPipelineV2ContinueStageIntentWithIo(barrierOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent });
      const [one, two] = await Promise.all([first, second]);
      expect(one).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      expect(two).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      const stat = await lstat(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"));
      expect(stat.ino).toBeNumber();
      expect((await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8"))).toBe(intent.canonical_json);
      expect(ctx.sink.snapshot?.revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. two different intents racing: one winner, the loser conflicts without rewriting", async () => {
    const ctx = await waitingReady();
    try {
      const first = preparedIntent(ctx, { additional_iterations: 2 });
      const second = preparedIntent(ctx, { additional_iterations: 5 });
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      let entered = 0;
      let openBarrier!: () => void;
      const barrier = new Promise<void>((resolve) => {
        openBarrier = resolve;
      });
      const barrierIo: ImmutableDocumentIo = {
        ...realRunPlanStoreIo,
        link: async (existingPath: string, newPath: string) => {
          entered += 1;
          if (entered === 2) {
            openBarrier();
          }
          await barrier;
          return await realRunPlanStoreIo.link(existingPath, newPath);
        },
      };
      const barrierOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: (runRoot, revision) => loadPipelineV2PlanRevision(runRoot, revision),
        publishWaitIntent: (runRoot, manifest) => publishPipelineV2WaitIntentWithIo(barrierIo, runRoot, manifest),
      };
      const one = acceptPipelineV2ContinueStageIntentWithIo(barrierOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: first });
      const two = acceptPipelineV2ContinueStageIntentWithIo(barrierOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: second });
      const outcomes = await Promise.allSettled([one, two]);
      const winners = outcomes.filter((entry) => entry.status === "fulfilled");
      const losers = outcomes.filter((entry) => entry.status === "rejected");
      expect(winners).toHaveLength(1);
      expect(losers).toHaveLength(1);
      expect((losers[0] as PromiseRejectedResult).reason).toBeInstanceOf(PipelineV2RunPlanStoreError);
      const winnerIntent = winners[0] === undefined ? undefined : (winners[0] as PromiseFulfilledResult<{ intent_sha256: string }>).value;
      expect(ctx.sink.snapshot?.revision).toBe(revisionBefore + 1);
      const durable = ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256;
      const stored = await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8");
      expect(stored).toBe(durable === first.sha256 ? first.canonical_json : second.canonical_json);
      expect(durable).toBe(winnerIntent?.intent_sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30a. a null snapshot is the controller's typed invalid_state with zero side effects", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      let planLoads = 0;
      let publishes = 0;
      let dispatches: PipelineV2RunCommand[] = [];
      const countingOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: async (runRoot, revision) => {
          planLoads += 1;
          return await loadPipelineV2PlanRevision(runRoot, revision);
        },
        publishWaitIntent: async (runRoot, manifest) => {
          publishes += 1;
          return await publishPipelineV2WaitIntent(runRoot, manifest);
        },
      };
      const nullSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return null;
        },
        get poisoned() {
          return false;
        },
        dispatch(command: PipelineV2RunCommand) {
          dispatches.push(command);
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntentWithIo(countingOps, { runRoot: ctx.fixture.runRoot, sink: nullSink, intent }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toBe("the durable run state is missing or not a valid pipeline v2 run state");
      expect(error.state).toBeNull();
      expect(planLoads).toBe(0);
      expect(publishes).toBe(0);
      expect(dispatches).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30b. a structurally invalid snapshot is the controller's typed invalid_state without echoing caller values", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const brokenSnapshot = {
        schema_version: 7,
        run_id: "CANARY-RUN",
        revision: 1,
        status: "not-a-status",
        phase: "running",
        waits: [{ canary: true }],
      };
      let planLoads = 0;
      let publishes = 0;
      let dispatches: PipelineV2RunCommand[] = [];
      const countingOps: PipelineV2ContinueStageIntentControllerOps = {
        loadPlanRevision: async (runRoot, revision) => {
          planLoads += 1;
          return await loadPipelineV2PlanRevision(runRoot, revision);
        },
        publishWaitIntent: async (runRoot, manifest) => {
          publishes += 1;
          return await publishPipelineV2WaitIntent(runRoot, manifest);
        },
      };
      const brokenSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return brokenSnapshot as unknown as PipelineV2RunState;
        },
        get poisoned() {
          return false;
        },
        dispatch(command: PipelineV2RunCommand) {
          dispatches.push(command);
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntentWithIo(countingOps, { runRoot: ctx.fixture.runRoot, sink: brokenSink, intent }),
      );
      const error = expectControllerError(cause, "invalid_state");
      expect(error.message).toBe("the durable run state is missing or not a valid pipeline v2 run state");
      expect(error.state).toBeNull();
      expect(error.message).not.toContain("CANARY-RUN");
      expect(error.message).not.toContain("not-a-status");
      expect(error.message).not.toContain("canary");
      expect(planLoads).toBe(0);
      expect(publishes).toBe(0);
      expect(dispatches).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. hand-built, spread, cloned and proxied intents are rejected by provenance", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const handBuilt = { manifest: { ...intent.manifest }, canonical_json: intent.canonical_json, sha256: intent.sha256 };
      const spread = { ...intent };
      const cloned = structuredClone(intent);
      let proxyTraps = 0;
      const proxied = new Proxy(intent, {
        get() {
          proxyTraps += 1;
          return undefined;
        },
      });
      for (const [name, lookalike] of [["hand-built", handBuilt], ["spread", spread], ["clone", cloned], ["proxy", proxied]] as const) {
        const cause = await catchAccept(() =>
          acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: lookalike }),
        );
        const error = expectControllerError(cause, "invalid_intent");
        expect(error.message).toContain("not a provenance-registered continue_stage_intent");
        expect(error.state).toBeNull();
        if (name === "proxy") {
          expect(proxyTraps).toBe(0);
        }
      }
      expect(await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8").catch(() => "absent")).toBe("absent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. a proxied snapshot is not read before the provenance gate passes", async () => {
    const ctx = await waitingReady();
    try {
      let snapshotTraps = 0;
      const snapshot = ctx.sink.snapshot;
      if (snapshot === null) {
        throw new Error("fixture snapshot missing");
      }
      const snapshotProxy = new Proxy(snapshot as unknown as object, {
        get() {
          snapshotTraps += 1;
          return undefined;
        },
      });
      let intentTraps = 0;
      const intentProxy = new Proxy(preparedIntent(ctx), {
        get() {
          intentTraps += 1;
          return undefined;
        },
      });
      const proxiedSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot() {
          return snapshotProxy as unknown as PipelineV2RunState;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        dispatch: ctx.sink.dispatch,
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: proxiedSink, intent: intentProxy }),
      );
      expectControllerError(cause, "invalid_intent");
      expect(intentTraps).toBe(0);
      expect(snapshotTraps).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. options, sink and ops getters are read exactly once", async () => {
    const ctx = await waitingReady();
    try {
      const optionReads: string[] = [];
      const sinkReads: string[] = [];
      const opsReads: string[] = [];
      const intent = preparedIntent(ctx);
      const inner = recordingSink(ctx.sink);
      const sinkProxy = new Proxy(inner as unknown as Record<string, unknown>, {
        get(target, property) {
          sinkReads.push(String(property));
          return target[property as string];
        },
      });
      const optionsProxy = new Proxy({ runRoot: ctx.fixture.runRoot, sink: sinkProxy, intent } as Record<string, unknown>, {
        get(target, property) {
          optionReads.push(String(property));
          return target[property as string];
        },
      });
      const realOps = productionContinueStageIntentOps;
      const opsProxy = new Proxy(realOps as unknown as Record<string, unknown>, {
        get(target, property) {
          opsReads.push(String(property));
          return target[property as string];
        },
      });
      const result = await acceptPipelineV2ContinueStageIntentWithIo(opsProxy as unknown as PipelineV2ContinueStageIntentControllerOps, optionsProxy as unknown as Parameters<typeof acceptPipelineV2ContinueStageIntent>[0]);
      expect(result).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      expect(optionReads.filter((name) => name === "runRoot" || name === "sink" || name === "intent")).toEqual(["runRoot", "sink", "intent"]);
      expect(sinkReads.filter((name) => name === "poisoned" || name === "dispatch" || name === "snapshot")).toEqual(["poisoned", "dispatch", "snapshot", "snapshot"]);
      expect(opsReads.filter((name) => name === "loadPlanRevision" || name === "publishWaitIntent")).toEqual(["loadPlanRevision", "publishWaitIntent"]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. throwing getters propagate their own error identity", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const boom = new Error("the snapshot getter failed");
      const throwingSink: PipelineV2ContinueStageIntentControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          throw boom;
        },
        get poisoned() {
          return false;
        },
        dispatch() {
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: throwingSink, intent }),
      );
      expect(cause).toBe(boom);
      const opsBoom = new Error("the ops getter failed");
      const throwingOps = new Proxy(productionContinueStageIntentOps as unknown as Record<string, unknown>, {
        get() {
          throw opsBoom;
        },
      });
      const opsCause = await catchAccept(() =>
        acceptPipelineV2ContinueStageIntentWithIo(throwingOps as unknown as PipelineV2ContinueStageIntentControllerOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink as PipelineV2ContinueStageIntentControllerSink, intent }),
      );
      expect(opsCause).toBe(opsBoom);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. caller mutation after the capture cannot influence the run", async () => {
    const ctx = await waitingReady();
    try {
      const intent = preparedIntent(ctx);
      const options = { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent };
      const pending = acceptPipelineV2ContinueStageIntent(options);
      (options as { runRoot: string }).runRoot = "/replaced";
      const result = await pending;
      expect(result).toMatchObject({ wait_index: 1, intent_sha256: intent.sha256 });
      expect(await readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"), "utf8")).toBe(intent.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. diagnostics are content-free across every failure path", async () => {
    const ctx = await waitingReady();
    try {
      const messages: string[] = [];
      const intent = preparedIntent(ctx);
      const cases: Array<() => Promise<unknown>> = [
        () => acceptPipelineV2ContinueStageIntent({ runRoot: "/definitely/not/a/canary/run/root", sink: ctx.sink, intent }),
        () => acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { wait_index: 2 }) }),
        () => acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { stage_id: "stage-2" }) }),
        () => acceptPipelineV2ContinueStageIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: preparedIntent(ctx, { run_id: "run-other" }) }),
      ];
      for (const run of cases) {
        const cause = await catchAccept(run);
        messages.push((cause as Error).message);
      }
      for (const message of messages) {
        expect(message).not.toContain(ctx.fixture.runRoot);
        expect(message).not.toContain(intent.canonical_json);
        expect(message).not.toContain(intent.sha256);
        expect(message).not.toContain(ctx.planSha256);
        expect(message).not.toContain("Body A");
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

test("36. the runtime export surfaces are exact (public two keys, internal core)", async () => {
  const publicModule = await import("../src/pipeline_v2_continue_stage_intent_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinueStageIntentControllerError",
    "acceptPipelineV2ContinueStageIntent",
  ]);
  const internalModule = await import("../src/pipeline_v2_continue_stage_intent_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinueStageIntentControllerError",
    "acceptPipelineV2ContinueStageIntentWithIo",
    "precheckPlanIntentAcceptance",
    "productionContinueStageIntentOps",
  ]);
});

test("37. the controller reuses the existing layers only (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_continue_stage_intent_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("validateContinueIntentBinding(")).toBe(1);
  expect(countOf("loadPipelineV2PlanRevision(")).toBe(1);
  expect(countOf("publishPipelineV2WaitIntent(")).toBe(1);
  expect(countOf("hasPreparedRunPlanProvenance(")).toBe(1);
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
  expect(countOf("fsyncImmutableDirectory(")).toBe(0);
  expect(countOf("mkdirExclusive(")).toBe(0);
  expect(countOf("readWholeFile(")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  for (const banned of ["pipeline_v2_coordinator", "pipeline_v2_runner", "main.ts", "cli_", "docker", "launcher", "pipeline_v2_wait_store", "pipeline_v2_wait_controller"]) {
    expect(source).not.toContain(banned);
  }
});
