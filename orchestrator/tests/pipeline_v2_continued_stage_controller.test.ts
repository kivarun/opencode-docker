import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  compilePipelineV2RunPlanCandidate,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
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
import { ensurePipelineV2StageIteration, PipelineV2StageIterationControllerError } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { acceptPipelineV2ContinueStageIntent } from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import { applyPipelineV2ContinueStageGrant, PipelineV2ContinueStageGrantControllerError } from "../src/pipeline_v2_continue_stage_grant_controller.ts";
import { recordPipelineV2WaitAction, PipelineV2WaitControllerError } from "../src/pipeline_v2_wait_controller.ts";
import { completePipelineV2ContinueStage } from "../src/pipeline_v2_continue_stage_completion_controller.ts";
import {
  openPipelineV2ContinuedStage,
  PipelineV2ContinuedStageControllerError,
} from "../src/pipeline_v2_continued_stage_controller.ts";
import {
  openPipelineV2ContinuedStageWithIo,
  productionContinuedStageOps,
  type OpenedPipelineV2ContinuedStage,
  type PipelineV2ContinuedStageControllerSink,
  type PipelineV2ContinuedStageOps,
} from "../src/pipeline_v2_continued_stage_controller_internal.ts";
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

async function writeBundle(bundle: string): Promise<void> {
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
  await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
}

async function withPipeline<T>(fn: (pipeline: ResolvedPipelineV2) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continued-stage-"));
  try {
    const bundle = join(root, "bundle");
    await writeBundle(bundle);
    return await fn(await loadPipelineV2(bundle));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function withTwoPipelines<T>(fn: (first: ResolvedPipelineV2, second: ResolvedPipelineV2) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continued-stage-"));
  try {
    const first = join(root, "bundle-1");
    const second = join(root, "bundle-2");
    await writeBundle(first);
    await writeBundle(second);
    return await fn(await loadPipelineV2(first), await loadPipelineV2(second));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continued-stage-run-"));
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
  return new Date(Date.UTC(2026, 9, 28, 0, 0, clockCounter));
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

interface ContinuedStageCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  compiledPlan: CompiledPipelineV2RunPlan;
  plan1: ReturnType<typeof preparePlanRevisionManifest>;
  candidate: PreparedPipelineV2RunPlanCandidate;
}

interface ReadyOptions {
  recordGrant?: { additionalIterations: number };
  closeGrantIteration?: boolean;
  recordResponse?: boolean;
  openNextIteration?: boolean;
}

/**
 * A real sink driven through the real reducer and the existing controllers
 * to the requested continue-stage boundary.
 */
async function driveRun(pipeline: ResolvedPipelineV2, options: ReadyOptions = {}): Promise<ContinuedStageCtx> {
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
  const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [A1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
  const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
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
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
    executionIndex: 2,
  });
  const request = preparePipelineV2WaitRequest({
    schema_version: 1,
    run_id: RUN_ID,
    wait_index: 1,
    transition_count: 2,
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
  await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
  const intent = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 1,
    stage_id: "stage-1",
    expected_plan_sha256: accepted.compiled_plan.plan_sha256,
    additional_iterations: 2,
  });
  await acceptPipelineV2ContinueStageIntent({ runRoot: fixture.runRoot, sink, intent });
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
    await recordPipelineV2WaitAction({ runRoot: fixture.runRoot, sink, waitIndex: 1, actionId: "continue_stage" });
  }
  if (options.openNextIteration === true) {
    await ensurePipelineV2StageIteration({ compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: 2, sink });
  }
  return { fixture, sink, pipeline, intent, compiledPlan: accepted.compiled_plan, plan1, candidate };
}

async function continuedStageReady(options: ReadyOptions = {}): Promise<ContinuedStageCtx> {
  return await withPipeline(async (pipeline) => driveRun(pipeline, options));
}

interface RecordingSink {
  commands: PipelineV2RunCommand[];
}

function recordSink(inner: PipelineV2RunStateSink): RecordingSink & PipelineV2ContinuedStageControllerSink {
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
): PipelineV2ContinuedStageControllerSink {
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

interface BarrierSink extends RecordingSink, PipelineV2ContinuedStageControllerSink {
  waitHeld: () => Promise<void>;
  release: () => void;
}

/**
 * A barrier recording sink: the dispatch of a matching command holds until
 * the test releases it. No sleeps; deterministic interleaving.
 */
function barrierRecordingSink(
  inner: PipelineV2RunStateSink,
  holdOn: (command: PipelineV2RunCommand) => boolean,
): BarrierSink {
  const commands: PipelineV2RunCommand[] = [];
  let resolveHeld!: () => void;
  const held = new Promise<void>((resolve) => {
    resolveHeld = resolve;
  });
  let release!: () => void;
  const releasable = new Promise<void>((resolve) => {
    release = resolve;
  });
  let heldOnce = false;
  return {
    commands,
    waitHeld: () => held,
    release: () => release(),
    get snapshot() {
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      if (!heldOnce && holdOn(command)) {
        heldOnce = true;
        resolveHeld();
        await releasable;
      }
      await inner.dispatch(command);
      commands.push(command);
    },
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

function expectOpenError(cause: unknown): PipelineV2ContinuedStageControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinuedStageControllerError);
  return cause as PipelineV2ContinuedStageControllerError;
}

function resultFields(result: OpenedPipelineV2ContinuedStage): Record<string, unknown> {
  const fields: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(result)) {
    if (key !== "state" && key !== "compiled_stage") {
      fields[key] = value;
    }
  }
  return fields;
}

interface CountingOps extends PipelineV2ContinuedStageOps {
  ensureCalls: () => number;
}

/**
 * The real completion controller beside a counting ensure controller: used
 * to prove the composition order and the zero-ensure rule on a hostile
 * completion result.
 */
function realCompletionCountingEnsure(): CountingOps {
  let calls = 0;
  return {
    completeStage: completePipelineV2ContinueStage,
    ensureStageIteration: async () => {
      calls += 1;
      throw new Error("ensure must not be called");
    },
    ensureCalls: () => calls,
  } as unknown as CountingOps;
}

/**
 * The real completion whose successful result is mutated before it reaches
 * the composition verification; the ensure controller counts calls and
 * never runs.
 */
function mutatedCompletionOps(
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
): CountingOps {
  let calls = 0;
  return {
    completeStage: async (args: unknown) => {
      const real = await completePipelineV2ContinueStage(args as never);
      const clone = structuredClone(real) as unknown as Record<string, unknown>;
      mutate(clone, clone["state"] as PipelineV2RunState);
      return clone as never;
    },
    ensureStageIteration: async () => {
      calls += 1;
      throw new Error("ensure must not be called");
    },
    ensureCalls: () => calls,
  } as unknown as CountingOps;
}

/**
 * The real completion beside a real ensure whose successful result is
 * mutated before it reaches the composition verification. The shallow
 * result copy preserves the exact `compiled_stage` object identity (the
 * composition verifies it by identity), so a mutation is caught by the
 * comparison it attacks — never by an accidental clone of the trusted
 * compiled stage.
 */
function mutatedEnsureOps(
  mutate: (result: Record<string, unknown>, state: PipelineV2RunState) => void,
): PipelineV2ContinuedStageOps {
  return {
    completeStage: completePipelineV2ContinueStage,
    ensureStageIteration: async (args: unknown) => {
      const real = await ensurePipelineV2StageIteration(args as never);
      const clone = { ...real, state: structuredClone(real.state) } as unknown as Record<string, unknown>;
      mutate(clone, clone["state"] as PipelineV2RunState);
      return clone as never;
    },
  } as unknown as PipelineV2ContinuedStageOps;
}

function callComposition(ctx: ContinuedStageCtx, sink: unknown, initialBudget = 2): Promise<OpenedPipelineV2ContinuedStage> {
  return openPipelineV2ContinuedStageWithIo(productionContinuedStageOps, {
    runRoot: ctx.fixture.runRoot,
    sink,
    intent: ctx.intent,
    compiledPlan: ctx.compiledPlan,
    initialBudget,
  });
}

describe("openPipelineV2ContinuedStage", () => {
  test("1. the C0 path composes the exact command order with revision +4", async () => {
    const ctx = await continuedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const result = await callComposition(ctx, recording);
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "iteration_grant_recorded",
        "stage_iteration_closed",
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect(recording.commands[0]).toMatchObject({
        kind: "iteration_grant_recorded",
        generationIndex: 1,
        waitIndex: 1,
        intentSha256: ctx.intent.sha256,
        additionalIterations: 2,
      });
      expect(recording.commands[1]).toMatchObject({
        kind: "stage_iteration_closed",
        generationIndex: 1,
        iterationIndex: 1,
        by: "grant",
        waitIndex: 1,
      });
      expect(recording.commands[2]).toMatchObject({
        kind: "wait_response_recorded",
        waitIndex: 1,
        actionId: "continue_stage",
      });
      expect(recording.commands[3]).toMatchObject({
        kind: "stage_iteration_opened",
        generationIndex: 1,
        iterationIndex: 2,
        transitionCount: 2,
      });
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 4);
      expect(result).toMatchObject({
        wait_index: 1,
        additional_iterations: 2,
        intent_sha256: ctx.intent.sha256,
        action_id: "continue_stage",
        action_to: "dev_entry",
        closed_iteration_index: 1,
        iteration_index: 2,
        generation_index: 1,
      });
      expect(result.state.status).toBe("active");
      expect(result.state.phase).toBe("running");
      expect(result.state.cursor.current_state).toBe("dev_entry");
      const generation = result.state.generations[0];
      expect(generation?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
      expect(generation?.iterations[0]?.closed).toEqual({ by: "grant", wait_index: 1, closed_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the result is deep-frozen, content-free and round-trips through the loader", async () => {
    const ctx = await continuedStageReady();
    try {
      const result = await callComposition(ctx, ctx.sink);
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
      expect(JSON.stringify(result)).not.toContain(ctx.fixture.runRoot);
      validatePipelineV2RunState(JSON.parse(JSON.stringify(result.state)) as never);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.status).toBe("active");
      expect(persisted.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the C1 partial retry runs closure, response and open only", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const result = await callComposition(ctx, recording);
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
      expect(result.closed_iteration_index).toBe(1);
      expect(result.iteration_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the C2 partial retry runs response and open only", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const result = await callComposition(ctx, recording);
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
      expect(result.iteration_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the C3 orphan response retry: identity error, no open, fresh retry commits response and open once", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const recording = recordSink(faulted as unknown as PipelineV2RunStateSink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((cause as Error).message).toContain("could not be committed");
      expect(recording.commands.map((command) => command.kind)).not.toContain("stage_iteration_opened");
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual([
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect(result.state.status).toBe("active");
      expect(result.state.waits[0]?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. the C4 committed response retry: the completion is zero-dispatch and only the iteration opens", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const result = await callComposition(ctx, recording);
      expect(recording.commands.map((command) => command.kind)).toEqual(["stage_iteration_opened"]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect(result.closed_iteration_index).toBe(1);
      expect(result.iteration_index).toBe(2);
      expect(result.state.cursor.current_state).toBe("dev_entry");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. the C4 durability-unknown window: identity error, poisoned sink, fresh retry opens", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const recording = recordSink(faulted as unknown as PipelineV2RunStateSink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect(recording.commands.map((command) => command.kind)).not.toContain("stage_iteration_opened");
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual(["stage_iteration_opened"]);
      expect(result.state.status).toBe("active");
      expect(result.state.waits[0]?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. the C5 exact already-open retry is a zero-dispatch full retry", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const first = await callComposition(ctx, ctx.sink);
      const revisionAfterFirst = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const second = await callComposition(ctx, recording);
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionAfterFirst);
      expect(resultFields(second)).toEqual(resultFields(first));
      expect(second.state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. a deviating open iteration is never the C5 retry: the typed grant refusal passes by identity", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      // iteration 2 opened and closed, iteration 3 opened: the open
      // iteration is not the exact successor of the grant-closed one
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
      await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 2, by: "normal_close" });
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 3, transitionCount: 2 });
      const recording = recordSink(ctx.sink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
      expect(ctx.sink.snapshot?.generations[0]?.iterations).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a hostile presentation preserving a mismatching budget never succeeds", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      // The hostile sink presents the completed boundary with a stored
      // budget of 5 while the caller selects 2; the recognition falls
      // through, the completion returns the mutated presentation and the
      // composition refuses it before the ensure call.
      const hostile = mutateSnapshotSink(ctx.sink, (state) => {
        ((state.generations[0] as unknown as Record<string, unknown>))["initial_budget"] = 5;
      });
      const ops = realCompletionCountingEnsure();
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: hostile,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
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

  test("11. hostile successful completion results are invalid_result with zero ensure calls", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const variants: Array<(result: Record<string, unknown>, state: PipelineV2RunState) => void> = [
        (result) => {
          result["action_to"] = "architect";
        },
        (result) => {
          result["additional_iterations"] = 3;
        },
        (result) => {
          result["intent_sha256"] = hex("e");
        },
        (_result, state) => {
          ((state.generations[0] as unknown as Record<string, unknown>))["stage_id"] = "stage-2";
        },
        (_result, state) => {
          ((state.generations[0] as unknown as Record<string, unknown>))["plan_sha256"] = hex("f");
        },
        (_result, state) => {
          ((state.generations[0] as unknown as Record<string, unknown>))["initial_budget"] = 5;
        },
        (_result, state) => {
          ((state.grants[0] as unknown as Record<string, unknown>))["intent_sha256"] = hex("e");
        },
        (_result, state) => {
          ((state.grants[0] as unknown as Record<string, unknown>))["additional_iterations"] = 3;
        },
        (_result, state) => {
          const iterations = (state.generations[0] as unknown as Record<string, unknown>)["iterations"] as unknown[];
          ((iterations[0] as unknown as Record<string, unknown>)["closed"] as Record<string, unknown>)["by"] = "normal_close";
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>))["transition_count"] = 3;
        },
        (_result, state) => {
          const actions = (state.waits[0] as unknown as Record<string, unknown>)["actions"] as unknown[];
          ((actions[0] as unknown as Record<string, unknown>))["to"] = "architect";
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>)["response"] as Record<string, unknown>)["action_id"] = "revise_task";
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["cursor"] = { current_state: "architect", transition_count: 2 };
        },
      ];
      for (const variant of variants) {
        const ops = mutatedCompletionOps(variant);
        const cause = await catchOpen(() =>
          openPipelineV2ContinuedStageWithIo(ops, {
            runRoot: ctx.fixture.runRoot,
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
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

  test("12. malformed completion results are invalid_result, never a TypeError", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const variants: Array<(result: Record<string, unknown>, state: PipelineV2RunState) => void> = [
        (result) => {
          result["state"] = null;
        },
        (result) => {
          result["wait_index"] = 0;
        },
        (result) => {
          result["action_id"] = "revise_task";
        },
        (result) => {
          result["response_sha256"] = 42;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["waits"] = null;
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>))["actions"] = null;
        },
        (_result, state) => {
          const actions = (state.waits[0] as unknown as Record<string, unknown>)["actions"] as unknown[];
          actions[0] = null;
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>))["intent"] = null;
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>))["response"] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["cursor"] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["transitions"] = null;
        },
        (_result, state) => {
          (state.transitions as unknown as unknown[])[0] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["executions"] = null;
        },
        (_result, state) => {
          (state.executions as unknown as unknown[])[0] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["grants"] = null;
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["generations"] = null;
        },
        (_result, state) => {
          ((state.generations[0] as unknown as Record<string, unknown>))["iterations"] = null;
        },
        (_result, state) => {
          const iterations = ((state.generations[0] as unknown as Record<string, unknown>)["iterations"]) as unknown[];
          iterations[0] = null;
        },
      ];
      for (const variant of variants) {
        const ops = mutatedCompletionOps(variant);
        const cause = await catchOpen(() =>
          openPipelineV2ContinuedStageWithIo(ops, {
            runRoot: ctx.fixture.runRoot,
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
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

  test("13. hostile successful ensure results are invalid_result", async () => {
    const ctx = await continuedStageReady();
    try {
      const variants: Array<(result: Record<string, unknown>, state: PipelineV2RunState) => void> = [
        (result) => {
          result["compiled_stage"] = structuredClone(result["compiled_stage"]);
        },
        (result) => {
          result["generation_index"] = 2;
        },
        (result) => {
          result["iteration_index"] = 3;
        },
        (result) => {
          result["state"] = null;
        },
        (_result, state) => {
          state.revision = state.revision + 1;
        },
        (_result, state) => {
          state.revision = state.revision - 1;
        },
        (_result, state) => {
          ((state.waits[0] as unknown as Record<string, unknown>)["response"] as Record<string, unknown>)["response_sha256"] = hex("9");
        },
        (_result, state) => {
          ((state.grants[0] as unknown as Record<string, unknown>))["additional_iterations"] = 3;
        },
        (_result, state) => {
          ((state.generations[0] as unknown as Record<string, unknown>))["template_id"] = "other-template";
        },
        (_result, state) => {
          const generation = state.generations[0] as unknown as Record<string, unknown>;
          generation["iteration_count"] = 3;
          (generation["iterations"] as unknown[]).push({ index: 3, opened_transition_count: 2 });
        },
        (_result, state) => {
          const generation = state.generations[0] as unknown as Record<string, unknown>;
          delete generation["open_iteration"];
        },
        (_result, state) => {
          ((state.executions[0] as unknown as Record<string, unknown>))["profile"] = "other";
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["terminal"] = { state_id: "done", result: "success" };
        },
        (_result, state) => {
          (state as unknown as Record<string, unknown>)["cursor"] = { current_state: "architect", transition_count: 2 };
        },
      ];
      for (const variant of variants) {
        const ops = mutatedEnsureOps(variant);
        const cause = await catchOpen(() =>
          openPipelineV2ContinuedStageWithIo(ops, {
            runRoot: ctx.fixture.runRoot,
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: ctx.compiledPlan,
            initialBudget: 2,
          }),
        );
        const error = expectOpenError(cause);
        expect(error.reason).toBe("invalid_result");
        expect(error.message).not.toContain("Body A");
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a grant not-committed passes through and a fresh retry runs the full suffix", async () => {
    const ctx = await continuedStageReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const recording = recordSink(faulted as unknown as PipelineV2RunStateSink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("state_persist_failed");
      expect(recording.commands).toEqual([]);
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual([
        "iteration_grant_recorded",
        "stage_iteration_closed",
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect(result.state.grants).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a grant durability-unknown adopts the grant and a fresh retry completes the suffix", async () => {
    const ctx = await continuedStageReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchOpen(() => callComposition(ctx, faulted));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("state_persist_failed");
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect(result.state.grants).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. a closure not-committed keeps the grant and a fresh retry runs the remaining suffix", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchOpen(() => callComposition(ctx, faulted));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("state_persist_failed");
      expect(ctx.sink.snapshot?.generations[0]?.iterations[0]?.closed).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
        "stage_iteration_opened",
      ]);
      expect(result.closed_iteration_index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. an iteration-open not-committed passes through and a fresh retry opens once", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 2, failStep: "rename" }),
      });
      const recording = recordSink(faulted as unknown as PipelineV2RunStateSink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2StageIterationControllerError);
      expect((cause as PipelineV2StageIterationControllerError).reason).toBe("state_persist_failed");
      expect(recording.commands.map((command) => command.kind)).not.toContain("stage_iteration_opened");
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const freshRecording = recordSink(fresh);
      const result = await callComposition(ctx, freshRecording);
      expect(freshRecording.commands.map((command) => command.kind)).toEqual(["stage_iteration_opened"]);
      expect(result.state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. two identical concurrent calls converge to one durable state", async () => {
    const ctx = await continuedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const firstRecording = recordSink(ctx.sink);
      const secondRecording = recordSink(ctx.sink);
      const [first, second] = await Promise.all([
        callComposition(ctx, firstRecording),
        callComposition(ctx, secondRecording),
      ]);
      const all = [...firstRecording.commands, ...secondRecording.commands];
      expect(all.filter((command) => command.kind === "iteration_grant_recorded")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "stage_iteration_closed")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "wait_response_recorded")).toHaveLength(1);
      expect(all.filter((command) => command.kind === "stage_iteration_opened")).toHaveLength(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 4);
      expect(resultFields(second)).toEqual(resultFields(first));
      expect(first.state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a conflicting caller budget is refused by the completion policy verification before the ensure", async () => {
    const ctx = await continuedStageReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordSink(ctx.sink);
      const cause = await catchOpen(() => callComposition(ctx, recording, 3));
      // the required caller-budget binding: the honest completion succeeded
      // (the durable flow ran), but its result's generation is bound to the
      // caller's budget — a mismatch is this composition's invalid_result
      // before the ensure call
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "iteration_grant_recorded",
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 3);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]?.initial_budget).toBe(2);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a stage id that is not in the compiled plan is the compiled resolver's typed error before any dispatch", async () => {
    const ctx = await continuedStageReady();
    try {
      const recording = recordSink(ctx.sink);
      const foreignIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-2",
        expected_plan_sha256: ctx.compiledPlan.plan_sha256,
        additional_iterations: 2,
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(productionContinuedStageOps, {
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          intent: foreignIntent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a compiled plan of a foreign pipeline is the ensure controller's lifecycle_conflict by identity", async () => {
    await withTwoPipelines(async (first, second) => {
      const ctx = await driveRun(first);
      try {
        const candidate2 = preparePipelineV2RunPlanCandidate({
          plan: ctx.plan1,
          taskRevisions: [A1],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const foreignCompiledPlan = compilePipelineV2RunPlanCandidate(second, candidate2);
        expect(foreignCompiledPlan.plan_sha256).toBe(ctx.compiledPlan.plan_sha256);
        const cause = await catchOpen(() =>
          openPipelineV2ContinuedStageWithIo(productionContinuedStageOps, {
            runRoot: ctx.fixture.runRoot,
            sink: ctx.sink,
            intent: ctx.intent,
            compiledPlan: foreignCompiledPlan,
            initialBudget: 2,
          }),
        );
        expect(cause).toBeInstanceOf(PipelineV2StageIterationControllerError);
        expect((cause as PipelineV2StageIterationControllerError).reason).toBe("lifecycle_conflict");
        expect((cause as Error).message).toContain("bundle_root");
      } finally {
        await disposeRun(ctx.fixture);
      }
    });
  });

  test("22. a conflicting durable grant surfaces the typed grant conflict by identity", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 5 } });
    try {
      const cause = await catchOpen(() => callComposition(ctx, ctx.sink));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("grant_conflict");
      expect(ctx.sink.snapshot?.generations[0]?.iterations).toHaveLength(1);
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. a boundary answered with another action is re-thrown by identity, never reconciled", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      await recordPipelineV2WaitAction({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "revise_task" });
      const recording = recordSink(ctx.sink);
      const cause = await catchOpen(() => callComposition(ctx, recording));
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. caller mutation after the dispatch cannot change the captured policy or the captured ops", async () => {
    const ctx = await continuedStageReady();
    try {
      const sink = barrierRecordingSink(ctx.sink, (command) => command.kind === "iteration_grant_recorded");
      let receivedBudget: number | undefined;
      let fakeCalls = 0;
      const ops = {
        completeStage: completePipelineV2ContinueStage,
        ensureStageIteration: async (args: { initialBudget: number }) => {
          receivedBudget = args.initialBudget;
          return await ensurePipelineV2StageIteration(args as never);
        },
      } as unknown as PipelineV2ContinuedStageOps;
      const options: Record<string, unknown> = {
        runRoot: ctx.fixture.runRoot,
        sink,
        intent: ctx.intent,
        compiledPlan: ctx.compiledPlan,
        initialBudget: 2,
      };
      const call = openPipelineV2ContinuedStageWithIo(ops, options);
      await sink.waitHeld();
      options["initialBudget"] = 99;
      (ops as unknown as Record<string, unknown>)["ensureStageIteration"] = async () => {
        fakeCalls += 1;
        throw new Error("the reassigned ensure must not be called");
      };
      sink.release();
      const result = await call;
      expect(receivedBudget).toBe(2);
      expect(fakeCalls).toBe(0);
      expect(result.state.generations[0]?.initial_budget).toBe(2);
      expect(result.iteration_index).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. the completion result's generation_index is bound to the verified durable grant boundary", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const ops = mutatedCompletionOps((result) => {
        result["generation_index"] = 2;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("Body A");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. the completion result's iteration_index is bound to the grant-closed iteration", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const ops = mutatedCompletionOps((result) => {
        result["iteration_index"] = 2;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("Body A");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. a second conflicting grant of the granted pair is invalid_result with zero ensure calls", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const ops = mutatedCompletionOps((_result, state) => {
        (state.grants as unknown as unknown[]).push({
          index: 2,
          generation_index: 1,
          wait_index: 1,
          intent_sha256: ctx.intent.sha256,
          additional_iterations: 5,
        });
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(ops.ensureCalls()).toBe(0);
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("Body A");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. a hostile ensure result changing only the grant closure's wait_index is invalid_result; the exact closure is still accepted", async () => {
    const ctx = await continuedStageReady();
    try {
      const ops = mutatedEnsureOps((_result, state) => {
        const closed = (((state.generations[0] as unknown as Record<string, unknown>)["iterations"] as unknown[])[0] as unknown as Record<string, unknown>)["closed"] as Record<string, unknown>;
        closed["wait_index"] = 2;
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(error.message).not.toContain("Body A");
      expect(cause).not.toBeInstanceOf(TypeError);
    } finally {
      await disposeRun(ctx.fixture);
    }
    const fresh = await continuedStageReady();
    try {
      const result = await callComposition(fresh, fresh.sink);
      expect(result.closed_iteration_index).toBe(1);
      expect(result.state.generations[0]?.iterations[0]?.closed).toEqual({
        by: "grant",
        wait_index: 1,
        closed_transition_count: 2,
      });
    } finally {
      await disposeRun(fresh.fixture);
    }
  });

  test("29. a hostile C5 ensure result changing only updated_at is invalid_result", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true, openNextIteration: true });
    try {
      const ops = mutatedEnsureOps((_result, state) => {
        (state as unknown as Record<string, unknown>)["updated_at"] = "2020-01-01T00:00:00.000Z";
      });
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      const error = expectOpenError(cause);
      expect(error.reason).toBe("invalid_result");
      expect(error.message).not.toContain("Body A");
      expect(cause).not.toBeInstanceOf(TypeError);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a non-C5 completion error is re-thrown as the exact same object", async () => {
    const ctx = await continuedStageReady();
    try {
      const sentinel = new Error("sentinel completion failure");
      let ensureCalls = 0;
      const ops = {
        completeStage: async () => {
          throw sentinel;
        },
        ensureStageIteration: async () => {
          ensureCalls += 1;
          throw new Error("ensure must not be called");
        },
      } as unknown as PipelineV2ContinuedStageOps;
      const recording = recordSink(ctx.sink);
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      expect(cause).toBe(sentinel);
      expect(ensureCalls).toBe(0);
      expect(recording.commands).toEqual([]);
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. an ensure error is re-thrown as the exact same object", async () => {
    const ctx = await continuedStageReady();
    try {
      const sentinel = new Error("sentinel ensure failure");
      const ops = {
        completeStage: completePipelineV2ContinueStage,
        ensureStageIteration: async () => {
          throw sentinel;
        },
      } as unknown as PipelineV2ContinuedStageOps;
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      expect(cause).toBe(sentinel);
      expect(ctx.sink.snapshot?.waits[0]?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. a lifecycle_conflict that is not the exact C5 shape is re-thrown as the exact same object", async () => {
    const ctx = await continuedStageReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      const sentinel = new PipelineV2ContinueStageGrantControllerError("lifecycle_conflict", "sentinel lifecycle refusal", null);
      let ensureCalls = 0;
      const ops = {
        completeStage: async () => {
          throw sentinel;
        },
        ensureStageIteration: async () => {
          ensureCalls += 1;
          throw new Error("ensure must not be called");
        },
      } as unknown as PipelineV2ContinuedStageOps;
      const recording = recordSink(ctx.sink);
      const cause = await catchOpen(() =>
        openPipelineV2ContinuedStageWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          intent: ctx.intent,
          compiledPlan: ctx.compiledPlan,
          initialBudget: 2,
        }),
      );
      expect(cause).toBe(sentinel);
      expect(ensureCalls).toBe(0);
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

test("33. the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_continued_stage_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinuedStageControllerError",
    "openPipelineV2ContinuedStage",
  ]);
  const internalModule = await import("../src/pipeline_v2_continued_stage_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinuedStageControllerError",
    "openPipelineV2ContinuedStageWithIo",
    "productionContinuedStageOps",
  ]);
  expect(productionContinuedStageOps.completeStage).toBe(completePipelineV2ContinueStage);
  expect(productionContinuedStageOps.ensureStageIteration).toBe(ensurePipelineV2StageIteration);
  expect(Object.isFrozen(productionContinuedStageOps)).toBe(true);
});

test("34. the composition module imports only the composed layers (source scan)", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_continued_stage_controller_internal.ts"), "utf8");
  const countOf = (needle: string): number => source.split(needle).length - 1;
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
  expect(countOf("let production")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  const imports = [...source.matchAll(/from "([^"]+)"/g)].map((match) => match[1]);
  expect(imports.sort()).toEqual([
    "./pipeline_v2_continue_stage_completion_controller.ts",
    "./pipeline_v2_continue_stage_grant_controller.ts",
    "./pipeline_v2_freeze_internal.ts",
    "./pipeline_v2_identity_compare.ts",
    "./pipeline_v2_run_plan_compiled.ts",
    "./pipeline_v2_run_plan_manifests.ts",
    "./pipeline_v2_run_plan_provenance.ts",
    "./pipeline_v2_stage_iteration_controller.ts",
    "./pipeline_v2_state.ts",
  ]);
  expect(countOf("import ")).toBe(9);
});
