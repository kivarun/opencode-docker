import { describe, expect, test } from "bun:test";
import { lstat, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
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
import {
  publishPipelineV2PlanRevision,
  publishPipelineV2TaskRevision,
  publishPipelineV2WaitIntent,
} from "../src/pipeline_v2_run_plan_store.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  compilePipelineV2RunPlanCandidate,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { recordPipelineV2WaitAction } from "../src/pipeline_v2_wait_controller.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { PipelineError } from "../src/pipeline.ts";
import {
  applyPipelineV2ReviseTaskIntervention,
  PipelineV2ReviseTaskInterventionControllerError,
  type AppliedPipelineV2ReviseTaskIntervention,
  type PipelineV2ReviseTaskInterventionControllerFailureReason,
} from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import {
  applyPipelineV2ReviseTaskInterventionWithIo,
  productionReviseTaskInterventionOps,
  type PipelineV2ReviseTaskInterventionOps,
} from "../src/pipeline_v2_revise_task_intervention_controller_internal.ts";
import { acceptPipelineV2ReviseTaskIntent } from "../src/pipeline_v2_revise_task_intent_controller.ts";
import { completePipelineV2ReviseTask } from "../src/pipeline_v2_revise_task_completion_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../src/pipeline_v2_run_plan_restore.ts";
import { PipelineV2RunPlanRestoreError } from "../src/pipeline_v2_run_plan_restore.ts";
import { PipelineV2ReviseTaskIntentControllerError } from "../src/pipeline_v2_revise_task_intent_controller.ts";
import { PipelineV2ReviseTaskCompletionControllerError } from "../src/pipeline_v2_revise_task_completion_controller.ts";
import { PipelineV2ReviseTaskClosureControllerError } from "../src/pipeline_v2_revise_task_closure_controller.ts";
import { PipelineV2WaitControllerError } from "../src/pipeline_v2_wait_controller.ts";
import { PipelineV2RunPlanStoreError } from "../src/pipeline_v2_run_plan_store_internal.ts";
import { PipelineV2WaitStoreError } from "../src/pipeline_v2_wait_store_internal.ts";
import { PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "revise-run";
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

const BASE_INPUTS = [{ id: "task", type: "file" as const, protected: true, digest: PROTECTED_DIGEST }];

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 9, 30, 0, 0, clockCounter));
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

const POINTER_OF: Record<string, PreparedPipelineV2RunTaskRevision> = { "task-a": A1, "task-b": B1 };

interface RunFixture {
  root: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-intervention-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  return { root, stateRoot, runRoot, statePath: join(runRoot, "state.json") };
}

async function disposeRun(fixture: RunFixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

async function withPipeline<T>(fn: (pipeline: ResolvedPipelineV2) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-intervention-bundle-"));
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

let sharedPipeline: ResolvedPipelineV2 | null = null;

/**
 * The compiled pipeline is deep-frozen, provenance-registered and never
 * mutated by any test, so the bundle is built once and shared by every
 * fixture.
 */
async function sharedBundlePipeline(): Promise<ResolvedPipelineV2> {
  if (sharedPipeline === null) {
    const bundle = join(tmpdir(), "pipeline-v2-revise-intervention-shared-bundle");
    await rm(bundle, { recursive: true, force: true });
    const bundleRoot = join(bundle, "bundle");
    await mkdir(join(bundleRoot, "prompts"), { recursive: true });
    await writeFile(join(bundleRoot, "pipeline.yaml"), STAGE_YAML);
    await writeFile(join(bundleRoot, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundleRoot, "prompts", "coder.md"), "implement the task\n");
    sharedPipeline = await loadPipelineV2(bundleRoot);
  }
  return sharedPipeline;
}

/** The candidate task revision the controller derives for one caller body. */
function derivedCandidateFor(taskId: string, body: string): PreparedPipelineV2RunTaskRevision {
  const pointer = POINTER_OF[taskId]!;
  return prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: taskId,
    revision: pointer.manifest.revision + 1,
    previous_sha256: pointer.sha256,
    origin: "user_response",
    body,
  });
}

/** The revise intent the controller derives for one caller body. */
function derivedIntentFor(taskId: string, body: string): PreparedPipelineV2RunWaitIntent {
  const pointer = POINTER_OF[taskId]!;
  const candidate = derivedCandidateFor(taskId, body);
  return prepareWaitIntent({
    schema_version: 1,
    kind: "revise_task_intent",
    run_id: RUN_ID,
    wait_index: 1,
    task_id: taskId,
    expected_previous_task_sha256: pointer.sha256,
    new_task_revision_sha256: candidate.sha256,
  });
}

type ReviseWindow = "r0" | "r1" | "r2" | "r3" | "r4";

interface ReviseCtx {
  fixture: RunFixture;
  /** The reopened sink (a fresh process after the restart simulation). */
  sink: PipelineV2RunStateSink;
  /** The pre-restart sink (drives the fixture prefix). */
  originalSink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  request: ReturnType<typeof preparePipelineV2WaitRequest>;
  candidate: PreparedPipelineV2RunTaskRevision;
  intent: PreparedPipelineV2RunWaitIntent;
  /** The durable revision at the chosen window. */
  revisionAtWindow: number;
}

interface ReviseReadyOptions {
  window?: ReviseWindow;
  taskId?: "task-a" | "task-b";
  taskBody?: string;
  /** Skip the simulated restart (the fixture's own sink is returned). */
  reopen?: boolean;
}

/**
 * The honest prefix through the real reducer/sink/store/controllers to the
 * revise_task wait boundary (R0), optionally advanced to one of the
 * intervention windows R1–R4 through the same real primitives, then
 * optionally reopened through the ordinary `PipelineV2RunStateSink.open`
 * (the simulated process restart). The derived candidate/intent mirror
 * exactly what the controller derives from the caller body.
 */
async function reviseReady(options: ReviseReadyOptions = {}): Promise<ReviseCtx> {
  const window = options.window ?? "r0";
  const taskId = options.taskId ?? "task-a";
  const taskBody = options.taskBody ?? (taskId === "task-a" ? "Body A revised" : "Body B revised");
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    try {
      const originalSink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      await originalSink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
      await originalSink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning")) {
        await originalSink.dispatch(command);
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
            tasks: [
              { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
              { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] },
            ],
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
      const acceptedPlan = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: originalSink, candidate: planCandidate });
      await ensurePipelineV2StageIteration({ compiledPlan: acceptedPlan.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: originalSink });
      await originalSink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
      await originalSink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
      for (const command of agentPhases("stage")) {
        await originalSink.dispatch(command);
      }
      await originalSink.dispatch({
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
      await originalSink.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: request.sha256,
        actions,
      });
      await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
      const candidate = derivedCandidateFor(taskId, taskBody);
      const intent = derivedIntentFor(taskId, taskBody);
      if (window === "r1" || window === "r2" || window === "r3" || window === "r4") {
        await publishPipelineV2WaitIntent(fixture.runRoot, intent.manifest);
        await originalSink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: intent.sha256 });
      }
      if (window === "r2" || window === "r3" || window === "r4") {
        await publishPipelineV2TaskRevision(fixture.runRoot, candidate.manifest);
        await originalSink.dispatch({
          kind: "task_revision_accepted",
          taskId,
          revision: candidate.manifest.revision,
          taskSha256: candidate.sha256,
          waitIndex: 1,
          intentSha256: intent.sha256,
        });
      }
      if (window === "r3" || window === "r4") {
        await originalSink.dispatch({
          kind: "stage_iteration_closed",
          generationIndex: 1,
          iterationIndex: 1,
          by: "replanned",
          waitIndex: 1,
        });
      }
      if (window === "r4") {
        await recordPipelineV2WaitAction({ runRoot: fixture.runRoot, sink: originalSink, waitIndex: 1, actionId: "revise_task" });
      }
      const revisionAtWindow = (originalSink.snapshot as PipelineV2RunState).revision;
      const sink = options.reopen === false
        ? originalSink
        : await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
      return { fixture, sink, originalSink, pipeline, request, candidate, intent, revisionAtWindow };
    } catch (cause) {
      await disposeRun(fixture);
      throw cause;
    }
  });
}

interface RecordingSink {
  commands: PipelineV2RunCommand[];
}

/** Records every COMMITTED command (failed dispatches are not recorded). */
function recordingSink(inner: PipelineV2RunStateSink): RecordingSink & {
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

/** A sink whose first dispatch of the given command kind fails once. */
function faultOnceSink(
  inner: PipelineV2RunStateSink,
  failKind: PipelineV2RunCommand["kind"],
  error: Error,
): RecordingSink & {
  snapshot: PipelineV2RunState | null;
  poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
} {
  const commands: PipelineV2RunCommand[] = [];
  let failed = false;
  return {
    commands,
    get snapshot() {
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      if (!failed && command.kind === failKind) {
        failed = true;
        throw error;
      }
      await inner.dispatch(command);
      commands.push(command);
    },
  };
}

async function catchIntervention(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectInterventionError(
  cause: unknown,
  reason: PipelineV2ReviseTaskInterventionControllerFailureReason,
): PipelineV2ReviseTaskInterventionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReviseTaskInterventionControllerError);
  const error = cause as PipelineV2ReviseTaskInterventionControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

function durableResponseSha(state: PipelineV2RunState): string {
  const response = (state.waits[state.waits.length - 1] as unknown as Record<string, unknown>)["response"] as {
    response_sha256: string;
  };
  return response.response_sha256;
}

/** The expected flat result of a successful intervention for one fixture. */
function expectedResultShape(ctx: ReviseCtx, taskId: string, state: PipelineV2RunState): AppliedPipelineV2ReviseTaskIntervention {
  return {
    wait_index: 1,
    intent_sha256: ctx.intent.sha256,
    request_sha256: ctx.request.sha256,
    response_sha256: durableResponseSha(state),
    task_id: taskId,
    task_revision: ctx.candidate.manifest.revision,
    task_sha256: ctx.candidate.sha256,
    generation_index: 1,
    iteration_index: 1,
    action_id: "revise_task",
    action_to: "architect",
    state,
  };
}

/** The exact durable projection after a successful intervention (C0 suffix). */
function expectCompletedProjection(state: PipelineV2RunState, taskId: string, ctx: ReviseCtx): void {
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.cursor.current_state).toBe("architect");
  expect(state.cursor.transition_count).toBe(2);
  expect(state.transitions).toHaveLength(2);
  expect(state.executions).toHaveLength(2);
  const ledger = state.task_revisions.map((record) => `${record.task_id}@${record.revision}`);
  expect(ledger).toEqual(taskId === "task-a" ? ["task-a@1", "task-b@1", "task-a@2"] : ["task-a@1", "task-b@1", "task-b@2"]);
  const appended = state.task_revisions[state.task_revisions.length - 1]!;
  expect(appended.task_id).toBe(taskId);
  expect(appended.revision).toBe(2);
  expect(appended.sha256).toBe(ctx.candidate.sha256);
  expect(appended.previous_sha256).toBe(taskId === "task-a" ? A1.sha256 : B1.sha256);
  expect(appended.wait_index).toBe(1);
  expect(appended.intent_sha256).toBe(ctx.intent.sha256);
  expect(state.plan_revisions).toHaveLength(1);
  const generation = state.generations[state.generations.length - 1]!;
  expect(generation.closed).toBeUndefined();
  expect(generation.open_iteration).toBeUndefined();
  expect(generation.iterations[0]!.closed).toEqual({
    by: "replanned",
    wait_index: 1,
    closed_transition_count: 2,
  });
  const wait = state.waits[state.waits.length - 1] as unknown as Record<string, unknown>;
  expect(wait["index"]).toBe(1);
  expect((wait["intent"] as Record<string, unknown>)["intent_sha256"]).toBe(ctx.intent.sha256);
  expect((wait["response"] as Record<string, unknown>)["action_id"]).toBe("revise_task");
}

interface TreeEntry {
  path: string;
  kind: "file" | "directory" | "other";
  mode: number;
  ino: number;
  size: number;
  hash: string | null;
}

async function fingerprintTree(root: string): Promise<TreeEntry[]> {
  const entries: TreeEntry[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const names = (await readdir(dir)).sort();
    for (const name of names) {
      const full = join(dir, name);
      const rel = prefix === "" ? name : `${prefix}/${name}`;
      const st = await lstat(full);
      if (st.isDirectory()) {
        entries.push({ path: rel, kind: "directory", mode: st.mode, ino: st.ino, size: st.size, hash: null });
        await walk(full, rel);
      } else if (st.isFile()) {
        const bytes = await readFile(full);
        entries.push({ path: rel, kind: "file", mode: st.mode, ino: st.ino, size: st.size, hash: Bun.SHA256.hash(bytes, "hex") });
      } else {
        entries.push({ path: rel, kind: "other", mode: st.mode, ino: st.ino, size: st.size, hash: null });
      }
    }
  };
  await walk(root, "");
  return entries;
}

async function stateFileIdentity(statePath: string): Promise<{ bytes: Buffer; ino: number; mode: number; mtimeMs: number; size: number }> {
  const st = await lstat(statePath);
  return { bytes: await readFile(statePath), ino: st.ino, mode: st.mode, mtimeMs: st.mtimeMs, size: st.size };
}

// --- spy ops -----------------------------------------------------------------

interface SpyOps {
  ops: PipelineV2ReviseTaskInterventionOps;
  counts: () => Record<"restorePlan" | "prepareTaskRevision" | "prepareIntent" | "acceptIntent" | "completeTask", number>;
}

/**
 * Counting spies over the production ops. The two manifest preparers are
 * synchronous and stay synchronous (the derivation reads their results
 * immediately); the three facades are async.
 */
function spyOps(overrides: Partial<PipelineV2ReviseTaskInterventionOps> = {}): SpyOps {
  const counts = { restorePlan: 0, prepareTaskRevision: 0, prepareIntent: 0, acceptIntent: 0, completeTask: 0 };
  const ops: PipelineV2ReviseTaskInterventionOps = {
    restorePlan: async (options) => {
      counts.restorePlan += 1;
      return await (overrides.restorePlan ?? productionReviseTaskInterventionOps.restorePlan)(options);
    },
    prepareTaskRevision: (value) => {
      counts.prepareTaskRevision += 1;
      return (overrides.prepareTaskRevision ?? productionReviseTaskInterventionOps.prepareTaskRevision)(value);
    },
    prepareIntent: (value) => {
      counts.prepareIntent += 1;
      return (overrides.prepareIntent ?? productionReviseTaskInterventionOps.prepareIntent)(value);
    },
    acceptIntent: async (options) => {
      counts.acceptIntent += 1;
      return await (overrides.acceptIntent ?? productionReviseTaskInterventionOps.acceptIntent)(options);
    },
    completeTask: async (options) => {
      counts.completeTask += 1;
      return await (overrides.completeTask ?? productionReviseTaskInterventionOps.completeTask)(options);
    },
  };
  return { ops, counts: () => counts };
}

describe("applyPipelineV2ReviseTaskIntervention", () => {
  test("1. honest C0 on a reopened run: the exact four-command suffix, revision +4, exact result, loader round-trip", async () => {
    const ctx = await reviseReady({ window: "r0" });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 4);
      const responseSha = durableResponseSha(state);
      expect(recording.commands).toEqual([
        { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: ctx.intent.sha256 },
        {
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: ctx.candidate.sha256,
          waitIndex: 1,
          intentSha256: ctx.intent.sha256,
        },
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 },
        {
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: ctx.request.sha256,
          actionId: "revise_task",
          responseSha256: responseSha,
        },
      ]);
      expect(result).toEqual(expectedResultShape(ctx, "task-a", state));
      expectCompletedProjection(state, "task-a", ctx);
      const round = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      expect(round.status).toBe("active");
      expect(round.waits[0]!.response?.action_id).toBe("revise_task");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the unified result carries exactly the content-free fields and is deep-frozen", async () => {
    const ctx = await reviseReady({ window: "r0" });
    try {
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: ctx.sink,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect(Object.keys(result).sort()).toEqual([
        "action_id",
        "action_to",
        "generation_index",
        "intent_sha256",
        "iteration_index",
        "request_sha256",
        "response_sha256",
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
      expect(serialized).not.toContain('"kind"');
      expect(serialized).not.toContain(ctx.fixture.runRoot);
      expect(serialized).not.toContain("waits/1");
      expect(serialized).not.toContain("run-plan");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. R1 window: the durable intent skips the intent command; the exact +3 suffix", async () => {
    const ctx = await reviseReady({ window: "r1" });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 3);
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "task_revision_accepted",
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect(result).toEqual(expectedResultShape(ctx, "task-a", state));
      expectCompletedProjection(state, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. R2 window: the durable candidate skips both acceptance commands; the exact +2 suffix", async () => {
    const ctx = await reviseReady({ window: "r2" });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 2);
      expect(recording.commands.map((command) => command.kind)).toEqual(["stage_iteration_closed", "wait_response_recorded"]);
      expect(result).toEqual(expectedResultShape(ctx, "task-a", state));
      expectCompletedProjection(state, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. R3 window: the durable replanned closure skips the acceptance entirely; the response only (+1)", async () => {
    const ctx = await reviseReady({ window: "r3" });
    try {
      const { ops, counts } = spyOps();
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 1);
      expect(recording.commands.map((command) => command.kind)).toEqual(["wait_response_recorded"]);
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(1);
      expect(result).toEqual(expectedResultShape(ctx, "task-a", state));
      expectCompletedProjection(state, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. R4 window: the answered boundary is the exact completed retry; zero dispatch, +0", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      const { ops, counts } = spyOps();
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskInterventionWithIo(ops, {
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow);
      expect(recording.commands).toEqual([]);
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(1);
      expect(result).toEqual(expectedResultShape(ctx, "task-a", state));
      expectCompletedProjection(state, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. the task-b selection revises only task-b; the task-a neighbor record is untouched", async () => {
    const ctx = await reviseReady({ window: "r0", taskId: "task-b", taskBody: "Body B revised" });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-b",
        taskBody: "Body B revised",
      });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 4);
      expect(recording.commands).toEqual([
        { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: ctx.intent.sha256 },
        {
          kind: "task_revision_accepted",
          taskId: "task-b",
          revision: 2,
          taskSha256: ctx.candidate.sha256,
          waitIndex: 1,
          intentSha256: ctx.intent.sha256,
        },
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 },
        {
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: ctx.request.sha256,
          actionId: "revise_task",
          responseSha256: durableResponseSha(state),
        },
      ]);
      expect(result.task_id).toBe("task-b");
      expect(result.task_sha256).toBe(ctx.candidate.sha256);
      expectCompletedProjection(state, "task-b", ctx);
      // the neighbor's revision-1 record keeps every contract field
      const neighbor = state.task_revisions.find((record) => record.task_id === "task-a")!;
      expect(neighbor).toEqual({
        index: 1,
        task_id: "task-a",
        revision: 1,
        sha256: A1.sha256,
        previous_sha256: null,
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. C4 after the ordinary reopen: zero dispatch, the state file byte/inode/mtime identical, the tree unchanged", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      const beforeIdentity = await stateFileIdentity(ctx.fixture.statePath);
      const beforeTree = await fingerprintTree(ctx.fixture.stateRoot);
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect(recording.commands).toEqual([]);
      const afterIdentity = await stateFileIdentity(ctx.fixture.statePath);
      expect(afterIdentity.ino).toBe(beforeIdentity.ino);
      expect(afterIdentity.mode).toBe(beforeIdentity.mode);
      expect(afterIdentity.mtimeMs).toBe(beforeIdentity.mtimeMs);
      expect(afterIdentity.size).toBe(beforeIdentity.size);
      expect(afterIdentity.bytes).toEqual(beforeIdentity.bytes);
      expect(await fingerprintTree(ctx.fixture.stateRoot)).toEqual(beforeTree);
      expect(result.response_sha256).toBe(durableResponseSha(ctx.sink.snapshot as PipelineV2RunState));
      expect(result.state).toEqual(ctx.sink.snapshot as PipelineV2RunState);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- wrong-body retries ----------------------------------------------------

  test("9. a wrong body at R1 is the acceptance's intent conflict by identity; zero dispatch, tree unchanged", async () => {
    const ctx = await reviseReady({ window: "r1" });
    try {
      const beforeTree = await fingerprintTree(ctx.fixture.stateRoot);
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A differently revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
      expect((cause as PipelineV2ReviseTaskIntentControllerError).reason).toBe("intent_conflict");
      expect(recording.commands).toEqual([]);
      expect(await fingerprintTree(ctx.fixture.stateRoot)).toEqual(beforeTree);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a wrong body at R2 is the acceptance's intent conflict by identity; zero dispatch", async () => {
    const ctx = await reviseReady({ window: "r2" });
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A differently revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
      expect((cause as PipelineV2ReviseTaskIntentControllerError).reason).toBe("intent_conflict");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. a wrong body at R3 refuses before any facade call; zero writes", async () => {
    const ctx = await reviseReady({ window: "r3" });
    try {
      const beforeTree = await fingerprintTree(ctx.fixture.stateRoot);
      const { ops, counts } = spyOps();
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A differently revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the exact accepted revise task intent");
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(0);
      expect(recording.commands).toEqual([]);
      expect(await fingerprintTree(ctx.fixture.stateRoot)).toEqual(beforeTree);
      expect(error.state).toEqual(ctx.sink.snapshot);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. a wrong body at R4 refuses before any facade call; zero writes", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      const beforeTree = await fingerprintTree(ctx.fixture.stateRoot);
      const { ops, counts } = spyOps();
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A differently revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the exact accepted revise task intent");
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(0);
      expect(recording.commands).toEqual([]);
      expect(await fingerprintTree(ctx.fixture.stateRoot)).toEqual(beforeTree);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- hostile near-miss progressions ----------------------------------------

  test("13. a grant-closed iteration (not replanned) is never the revise retry window", async () => {
    const ctx = await reviseReady({ window: "r2" });
    try {
      await ctx.sink.dispatch({ kind: "iteration_grant_recorded", generationIndex: 1, waitIndex: 1, intentSha256: ctx.intent.sha256, additionalIterations: 1 });
      await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("carries no open iteration");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a mutated closure anchor is refused by the state validation before any effect", async () => {
    const ctx = await reviseReady({ window: "r3" });
    try {
      const hostile = {
        get snapshot(): PipelineV2RunState | null {
          const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
          const generation = clone.generations[0]! as unknown as { iterations: Array<Record<string, unknown>> };
          const iteration = generation.iterations[0] as unknown as { closed: { by: string; wait_index: number; closed_transition_count: number } };
          iteration.closed = { ...iteration.closed!, closed_transition_count: iteration.closed.closed_transition_count + 1 };
          return clone;
        },
        poisoned: false,
        dispatch: (command: PipelineV2RunCommand) => ctx.sink.dispatch(command),
      };
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: hostile,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      // The mutated anchor is loader-invalid (a wait-bound closure must
      // anchor at its wait's transition count), so the restoration's own
      // state validation refuses before the derivation; the intervention
      // passes the typed error through by identity with zero effects.
      expect(cause).toBeInstanceOf(PipelineV2RunPlanRestoreError);
      expect((cause as PipelineV2RunPlanRestoreError).reason).toBe("invalid_state");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. an answered wait with another action is never the revise_task boundary", async () => {
    const ctx = await reviseReady({ window: "r3" });
    try {
      // Built through the real wait controller from the replanned closure:
      // the reducer accepts the continue_stage response (the iteration is
      // closed), so the hostile boundary shape is loader-valid here.
      await recordPipelineV2WaitAction({
        runRoot: ctx.fixture.runRoot,
        sink: ctx.sink,
        waitIndex: 1,
        actionId: "continue_stage",
      });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("answered with another action");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. a shifted cursor at the answered boundary is refused by the state validation", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      const hostile = {
        get snapshot(): PipelineV2RunState | null {
          const clone = structuredClone(ctx.sink.snapshot) as PipelineV2RunState;
          clone.cursor = { current_state: "dev_entry", transition_count: clone.cursor.transition_count };
          return clone;
        },
        poisoned: false,
        dispatch: (command: PipelineV2RunCommand) => ctx.sink.dispatch(command),
      };
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: hostile,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      // The shifted cursor breaks the loader's joint cursor replay (the
      // persisted cursor must follow the answered wait's action target),
      // so the restoration refuses before the derivation.
      expect(cause).toBeInstanceOf(PipelineV2RunPlanRestoreError);
      expect((cause as PipelineV2RunPlanRestoreError).reason).toBe("invalid_state");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- post-boundary states are never retries (test area 8) -------------------

  test("17. a state after a new execution is never recognized as the retry", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      const { ops, counts } = spyOps();
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, {
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("journals are not exactly at the wait boundary");
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(0);
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. a state after a new committed transition is never recognized as the retry", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning-after")) {
        await ctx.sink.dispatch(command);
      }
      await ctx.sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("journals are not exactly at the wait boundary");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a state after a new wait is never recognized as the retry", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      const request2 = preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 2,
        transition_count: 2,
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
      await ctx.sink.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: request2.sha256,
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("as its last record declaring the revise_task action");
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a state after a new accepted plan revision is never recognized as the retry", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("planning-after")) {
        await ctx.sink.dispatch(command);
      }
      const plan2 = preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: RUN_ID,
        revision: 2,
        previous_sha256: (ctx.sink.snapshot as PipelineV2RunState).plan_revisions[0]!.sha256,
        root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
        origin_execution: 3,
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 2, sha256: ctx.candidate.sha256, depends_on: [] }],
          },
        ],
      });
      await publishPipelineV2PlanRevision(ctx.fixture.runRoot, plan2.manifest);
      await ctx.sink.dispatch({ kind: "plan_revision_accepted", planRevision: 2, planSha256: plan2.sha256, originExecution: 3 });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("does not belong to the last accepted plan revision");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a state after a new generation with an open iteration is never recognized as the retry", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      await ctx.sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
      await ctx.sink.dispatch({
        kind: "stage_generation_opened",
        stageId: "stage-1",
        stagePosition: 1,
        templateId: "development",
        planSha256: (ctx.sink.snapshot as PipelineV2RunState).plan_revisions[0]!.sha256,
        initialBudget: INITIAL_BUDGET,
        transitionCount: 2,
      });
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("carries no exact replanned iteration closure");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. an open successor iteration after the revise response is refused", async () => {
    const ctx = await reviseReady({ window: "r4" });
    try {
      // Loader-valid through the real reducer: the answered revise boundary
      // legally admits a next iteration; the last iteration is then open
      // and carries no replanned closure, so the intervention must refuse.
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("carries no exact replanned iteration closure");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- derivation refusals (test area 9) --------------------------------------

  test("23. a task the accepted plan does not carry is refused before any effect", async () => {
    const ctx = await reviseReady();
    try {
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-c",
          taskBody: "Body C",
        }),
      );
      const error = expectInterventionError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the caller task");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a stale plan artifact is the restoration's typed refusal; zero effects", async () => {
    const ctx = await reviseReady();
    try {
      await rm(join(ctx.fixture.runRoot, "run-plan"), { recursive: true, force: true });
      // Republished with different content: the digest no longer matches
      // the durable plan record, so the restoration refuses before the
      // derivation runs.
      const tampered = preparePlanRevisionManifest({
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
            tasks: [
              { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
              { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
            ],
          },
        ],
      });
      await publishPipelineV2PlanRevision(ctx.fixture.runRoot, tampered.manifest);
      const recording = recordingSink(ctx.sink);
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: recording,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanRestoreError);
      expect((cause as PipelineV2RunPlanRestoreError).reason).toBe("artifact_mismatch");
      expect(recording.commands).toEqual([]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- hostile successful results (test area 10) -------------------------------

  const interventionOptionsOf = (ctx: ReviseCtx, sink: unknown): Record<string, unknown> => ({
    pipeline: ctx.pipeline,
    runRoot: ctx.fixture.runRoot,
    sink,
    runId: RUN_ID,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "Body A revised",
  });

  test("25. a malformed restoration result is the controller's invalid_result; zero downstream calls", async () => {
    const ctx = await reviseReady();
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      const hostileRestores: unknown[] = [
        null,
        "restored",
        [],
        { state: before },
        { compiled_plan: {} },
        { compiled_plan: {}, state: null },
        {
          compiled_plan: {},
          state: { ...before, revision: before.revision + 5 },
        },
      ];
      for (const hostile of hostileRestores) {
        const fake = (): Promise<unknown> => Promise.resolve(hostile);
        const { ops, counts } = spyOps({
          restorePlan: fake as unknown as PipelineV2ReviseTaskInterventionOps["restorePlan"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
        );
        const error = expectInterventionError(cause, "invalid_result");
        expect(error.message).toContain("restored run plan result");
        expect(error.state).toEqual(before);
        expect(counts().prepareTaskRevision).toBe(0);
        expect(counts().acceptIntent).toBe(0);
        expect(counts().completeTask).toBe(0);
      }
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. a forged or foreign compiled plan is the controller's invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      // A hand-built look-alike fails the compiled-plan provenance probe.
      const forged = { stages: [], plan_revision: 1, plan_sha256: hex("1"), origin_execution: 1, run_id: RUN_ID };
      const forgedOps = spyOps({
        restorePlan: ((options: { state: PipelineV2RunState }) =>
          Promise.resolve({ compiled_plan: forged, state: options.state })) as unknown as PipelineV2ReviseTaskInterventionOps["restorePlan"],
      });
      const forgedCause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(forgedOps.ops, interventionOptionsOf(ctx, ctx.sink)),
      );
      const forgedError = expectInterventionError(forgedCause, "invalid_result");
      expect(forgedError.message).toContain("does not carry the real provenance-backed compiled plan");
      expect(forgedOps.counts().acceptIntent).toBe(0);

      // A real compiled plan of a different pipeline object fails the
      // hidden identity comparison.
      await withPipeline(async (foreignPipeline) => {
        const foreignPlan = preparePlanRevisionManifest({
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
              tasks: [
                { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] },
              ],
            },
          ],
        });
        const foreignCandidate = preparePipelineV2RunPlanCandidate({
          plan: foreignPlan,
          taskRevisions: [A1, B1],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const foreignCompiled = compilePipelineV2RunPlanCandidate(foreignPipeline, foreignCandidate);
        const fake = (options: { state: PipelineV2RunState }): Promise<unknown> =>
          Promise.resolve({ compiled_plan: foreignCompiled, state: options.state });
        const { ops, counts } = spyOps({
          restorePlan: fake as unknown as PipelineV2ReviseTaskInterventionOps["restorePlan"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
        );
        const error = expectInterventionError(cause, "invalid_result");
        expect(error.message).toContain("belongs to a different pipeline identity");
        expect(counts().acceptIntent).toBe(0);
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. a hostile acceptance result is the controller's invalid_result; the completion never runs", async () => {
    const ctx = await reviseReady();
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      const honestAccepted = {
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
        state: before,
      };
      const hostiles: unknown[] = [
        null,
        "accepted",
        { ...honestAccepted, wait_index: 2 },
        { ...honestAccepted, task_sha256: hex("f") },
        { ...honestAccepted, state: null },
        { ...honestAccepted, state: { ...before, cursor: { current_state: "dev_entry", transition_count: 2 } } },
      ];
      for (const hostile of hostiles) {
        const fake = (): Promise<unknown> => Promise.resolve(hostile);
        const { ops, counts } = spyOps({
          acceptIntent: fake as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
        );
        const error = expectInterventionError(cause, "invalid_result");
        expect(error.message).toContain("accepted revise task intent");
        expect(error.state).toEqual(before);
        expect(counts().completeTask).toBe(0);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. a healed acceptance state (wrong appended record) is refused", async () => {
    const ctx = await reviseReady();
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      // A revision-2 delta whose appended task record binds a different
      // wait index is not the exact progression.
      const healed = {
        ...before,
        revision: before.revision + 2,
        updated_at: "2026-09-30T00:00:09.000Z",
        task_revisions: [
          ...before.task_revisions,
          {
            index: before.task_revisions.length + 1,
            task_id: "task-a",
            revision: 2,
            sha256: ctx.candidate.sha256,
            previous_sha256: A1.sha256,
            wait_index: 2,
            intent_sha256: ctx.intent.sha256,
          },
        ],
      };
      const fake = (): Promise<unknown> =>
        Promise.resolve({
          wait_index: 1,
          intent_sha256: ctx.intent.sha256,
          task_id: "task-a",
          task_revision: 2,
          task_sha256: ctx.candidate.sha256,
          state: healed,
        });
      const { ops, counts } = spyOps({
        acceptIntent: fake as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
      );
      const error = expectInterventionError(cause, "invalid_result");
      expect(error.message).toContain("not the exact durable progression");
      expect(counts().completeTask).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. a hostile completion result is the controller's invalid_result", async () => {
    // At R2 the real acceptance is a zero-dispatch idempotent recognition,
    // so the completion's fake result is the only hostile input.
    const ctx = await reviseReady({ window: "r2" });
    try {
      const before = ctx.sink.snapshot as PipelineV2RunState;
      const hostiles: unknown[] = [
        null,
        "completed",
        { wait_index: 2 },
        {
          wait_index: 1,
          intent_sha256: ctx.intent.sha256,
          request_sha256: ctx.request.sha256,
          response_sha256: hex("e"),
          task_id: "task-a",
          task_revision: 2,
          task_sha256: ctx.candidate.sha256,
          generation_index: 1,
          iteration_index: 1,
          action_id: "revise_task",
          action_to: "dev_entry",
          state: before,
        },
        {
          wait_index: 1,
          intent_sha256: ctx.intent.sha256,
          request_sha256: ctx.request.sha256,
          response_sha256: hex("e"),
          task_id: "task-a",
          task_revision: 2,
          task_sha256: ctx.candidate.sha256,
          generation_index: 1,
          iteration_index: 1,
          action_id: "revise_task",
          action_to: "architect",
          state: null,
        },
      ];
      for (const hostile of hostiles) {
        const fake = (): Promise<unknown> => Promise.resolve(hostile);
        const { ops, counts } = spyOps({
          completeTask: fake as unknown as PipelineV2ReviseTaskInterventionOps["completeTask"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
        );
        const error = expectInterventionError(cause, "invalid_result");
        expect(error.message).toContain("completed revise task");
        expect(error.state).toEqual(before);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a malformed completion state is a typed invalid_result, never a TypeError", async () => {
    const ctx = await reviseReady({ window: "r2" });
    try {
      const verified = ctx.sink.snapshot as PipelineV2RunState;
      const malformedStates: unknown[] = [
        null,
        "state",
        { ...verified, waits: null },
        { ...verified, executions: null },
        { ...verified, task_revisions: null },
        { ...verified, generations: null },
        { ...verified, cursor: null },
      ];
      for (const malformed of malformedStates) {
        const fake = (): Promise<unknown> =>
          Promise.resolve({
            wait_index: 1,
            intent_sha256: ctx.intent.sha256,
            request_sha256: ctx.request.sha256,
            response_sha256: hex("e"),
            task_id: "task-a",
            task_revision: 2,
            task_sha256: ctx.candidate.sha256,
            generation_index: 1,
            iteration_index: 1,
            action_id: "revise_task",
            action_to: "architect",
            state: malformed,
          });
        const { ops, counts } = spyOps({
          completeTask: fake as unknown as PipelineV2ReviseTaskInterventionOps["completeTask"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
        );
        const error = expectInterventionError(cause, "invalid_result");
        expect(error.message).toContain("completed revise task");
        expect(error.state).toEqual(verified);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. a hostile racing acceptance state is never a retry; the cause keeps its identity", async () => {
    const ctx = await reviseReady({ window: "r2" });
    try {
      const real = ctx.sink.snapshot as PipelineV2RunState;
      const mutated = structuredClone(real) as PipelineV2RunState;
      const wait = mutated.waits[mutated.waits.length - 1] as unknown as Record<string, unknown>;
      wait["response"] = { action_id: "continue_stage", response_sha256: hex("f") };
      const racing = new PipelineV2ReviseTaskIntentControllerError(
        "invalid_state",
        "a concurrent durable step moved the revise task intervention forward",
        mutated,
      );
      const { ops, counts } = spyOps({
        acceptIntent: ((): Promise<unknown> => Promise.reject(racing)) as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
      );
      expect(cause).toBe(racing);
      expect(counts().completeTask).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- provenance and capture (test area 11) -----------------------------------

  test("32. a cloned pipeline fails the provenance gate before any effect", async () => {
    const ctx = await reviseReady();
    try {
      const clone = { ...ctx.pipeline } as unknown as ResolvedPipelineV2;
      const { ops, counts } = spyOps();
      let snapshotReads = 0;
      const counting = {
        get snapshot(): PipelineV2RunState | null {
          snapshotReads += 1;
          return ctx.sink.snapshot;
        },
        poisoned: false,
        dispatch: (command: PipelineV2RunCommand) => ctx.sink.dispatch(command),
      };
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, { ...interventionOptionsOf(ctx, counting), pipeline: clone }),
      );
      expect(cause).toBeInstanceOf(PipelineError);
      expect(counts().restorePlan).toBe(0);
      expect(counts().acceptIntent).toBe(0);
      expect(counts().completeTask).toBe(0);
      expect(snapshotReads).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. a Proxy pipeline causes no getter traps and no effect", async () => {
    const ctx = await reviseReady();
    try {
      let traps = 0;
      const proxy = new Proxy({} as unknown as ResolvedPipelineV2, {
        get() {
          traps += 1;
          return undefined;
        },
      });
      const { ops, counts } = spyOps();
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, { pipeline: proxy, runRoot: ctx.fixture.runRoot, sink: ctx.sink, runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: "Body A revised" }),
      );
      expect(cause).toBeInstanceOf(PipelineError);
      expect(traps).toBe(0);
      expect(counts().restorePlan).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. options are read exactly once in the fixed contract order; hostile extras never read", async () => {
    const ctx = await reviseReady();
    try {
      const reads: string[] = [];
      const hostile = new Proxy(
        {} as Record<string, unknown>,
        {
          get(_target, prop: string) {
            reads.push(prop);
            if (prop === "pipeline") return ctx.pipeline;
            if (prop === "runRoot") return ctx.fixture.runRoot;
            if (prop === "sink") return ctx.sink;
            if (prop === "runId") return RUN_ID;
            if (prop === "waitIndex") return 1;
            if (prop === "taskId") return "task-a";
            if (prop === "taskBody") return "Body A revised";
            return "HOSTILE-EXTRA";
          },
        },
      );
      const result = await applyPipelineV2ReviseTaskInterventionWithIo(productionReviseTaskInterventionOps, hostile);
      expect(result.task_revision).toBe(2);
      expect(reads.slice(0, 7)).toEqual(["pipeline", "runRoot", "sink", "runId", "waitIndex", "taskId", "taskBody"]);
      expect(reads).not.toContain("evil");
      expect(reads).not.toContain("actionId");
      expect(reads).not.toContain("stageId");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. ops members are read exactly once; a missing member refuses before any effect", async () => {
    const ctx = await reviseReady();
    try {
      const opsGets: Record<string, number> = {};
      const proxiedOps = new Proxy(productionReviseTaskInterventionOps, {
        get(target, prop: string) {
          opsGets[prop] = (opsGets[prop] ?? 0) + 1;
          return target[prop as keyof PipelineV2ReviseTaskInterventionOps];
        },
      });
      const result = await applyPipelineV2ReviseTaskInterventionWithIo(proxiedOps, interventionOptionsOf(ctx, ctx.sink));
      expect(result.task_revision).toBe(2);
      for (const member of ["restorePlan", "prepareTaskRevision", "prepareIntent", "acceptIntent", "completeTask"]) {
        expect(opsGets[member]).toBe(1);
      }

      const missing = { ...productionReviseTaskInterventionOps } as Record<string, unknown>;
      delete missing["prepareIntent"];
      let snapshotReads = 0;
      const counting = {
        get snapshot(): PipelineV2RunState | null {
          snapshotReads += 1;
          return ctx.sink.snapshot;
        },
        poisoned: false,
        dispatch: (command: PipelineV2RunCommand) => ctx.sink.dispatch(command),
      };
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(missing, interventionOptionsOf(ctx, counting)),
      );
      const error = expectInterventionError(cause, "invalid_options");
      expect(error.message).toContain("five composed facade functions");
      expect(snapshotReads).toBe(0);
      // The successful proxied call advanced the run; the missing-member
      // refusal changed nothing.
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("36. caller mutation after the pending start cannot influence the intervention", async () => {
    const ctx = await reviseReady();
    try {
      let releaseRestore!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseRestore = resolve;
      });
      const { ops } = spyOps({
        restorePlan: (async (options: Parameters<PipelineV2ReviseTaskInterventionOps["restorePlan"]>[0]) => {
          await gate;
          return await productionReviseTaskInterventionOps.restorePlan(options);
        }) as unknown as PipelineV2ReviseTaskInterventionOps["restorePlan"],
      });
      const options = {
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: ctx.sink,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      };
      const pending = applyPipelineV2ReviseTaskInterventionWithIo(ops, options);
      options.taskBody = "MUTATED AFTER START";
      releaseRestore();
      const result = (await pending) as AppliedPipelineV2ReviseTaskIntervention;
      expect(result.task_revision).toBe(2);
      expect(result.task_sha256).toBe(ctx.candidate.sha256);
      const appended = result.state.task_revisions[result.state.task_revisions.length - 1]!;
      expect(appended.sha256).toBe(ctx.candidate.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- durability windows (test area 12) ---------------------------------------

  test("37. a not-committed intent dispatch is a durable no-op; the fresh retry completes the full suffix", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = faultOnceSink(ctx.sink, "plan_intent_accepted", new PipelineV2RunStateStoreError("injected store failure"));
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
      const intentError = cause as PipelineV2ReviseTaskIntentControllerError;
      expect(intentError.reason).toBe("state_persist_failed");
      expect((intentError.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
      // The intent artifact stays published as an orphan for the retry.
      await expect(readFile(join(ctx.fixture.runRoot, "run-plan", "intents", "1.json"))).resolves.toBeTruthy();

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: reopened,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
      expect(await validatePipelineV2RunState(result.state)).toEqual(result.state);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("38. a not-committed candidate dispatch keeps the durable intent; the fresh retry runs the remaining suffix", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = faultOnceSink(ctx.sink, "task_revision_accepted", new PipelineV2RunStateStoreError("injected store failure"));
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
      expect((cause as PipelineV2ReviseTaskIntentControllerError).reason).toBe("state_persist_failed");
      expect(((cause as PipelineV2ReviseTaskIntentControllerError).state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 1);

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: reopened,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("39. a not-committed closure dispatch keeps the durable acceptance; the fresh retry completes it", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = faultOnceSink(ctx.sink, "stage_iteration_closed", new PipelineV2RunStateStoreError("injected store failure"));
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
      expect((cause as PipelineV2ReviseTaskClosureControllerError).reason).toBe("state_persist_failed");
      expect(((cause as PipelineV2ReviseTaskClosureControllerError).state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 2);

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: reopened,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("40. a not-committed response dispatch keeps the durable closure; the fresh retry records the response", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = faultOnceSink(ctx.sink, "wait_response_recorded", new PipelineV2RunStateStoreError("injected store failure"));
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((cause as PipelineV2WaitControllerError).reason).toBe("state_persist_failed");
      expect(((cause as PipelineV2WaitControllerError).state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 3);

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: reopened,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("41. a durability-unknown candidate dispatch adopts the candidate; the fresh retry finishes", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 2, failStep: "dirfsync" }),
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
      expect((cause as PipelineV2ReviseTaskIntentControllerError).reason).toBe("state_persist_failed");
      // The visible candidate carries the durable intent and candidate.
      expect(((cause as PipelineV2ReviseTaskIntentControllerError).state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 2);
      expect(faulted.poisoned).toBe(true);

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: reopened,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("42. a durability-unknown response dispatch adopts the candidate; the fresh retry is zero-dispatch", async () => {
    const ctx = await reviseReady({ reopen: false });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 4, failStep: "dirfsync" }),
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: faulted,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((cause as PipelineV2WaitControllerError).reason).toBe("state_persist_failed");
      expect(((cause as PipelineV2WaitControllerError).state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expect(faulted.poisoned).toBe(true);

      const reopened = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(reopened);
      const result = await applyPipelineV2ReviseTaskIntervention({
        pipeline: ctx.pipeline,
        runRoot: ctx.fixture.runRoot,
        sink: recording,
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "Body A revised",
      });
      expect(recording.commands).toEqual([]);
      expect((result.state as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(result.state as PipelineV2RunState, "task-a", ctx);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("43. publication failures pass through by identity; zero dispatch", async () => {
    const ctx = await reviseReady();
    try {
      const publicationFailure = new PipelineV2RunPlanStoreError("not_published", "io_failure", "injected publication failure");
      const { ops, counts } = spyOps({
        acceptIntent: ((): Promise<unknown> => Promise.reject(publicationFailure)) as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
      });
      const cause = await catchIntervention(() =>
        applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, ctx.sink)),
      );
      expect(cause).toBe(publicationFailure);
      expect(counts().completeTask).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- concurrency (test area 13) ----------------------------------------------

  test("44. two identical racing interventions converge to one durable state", async () => {
    const ctx = await reviseReady();
    try {
      const call = (): Promise<AppliedPipelineV2ReviseTaskIntervention> =>
        applyPipelineV2ReviseTaskIntervention({
          pipeline: ctx.pipeline,
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          runId: RUN_ID,
          waitIndex: 1,
          taskId: "task-a",
          taskBody: "Body A revised",
        });
      const [first, second] = await Promise.all([call(), call()]);
      expect(first.task_sha256).toBe(ctx.candidate.sha256);
      expect(second.task_sha256).toBe(ctx.candidate.sha256);
      expect(first.wait_index).toBe(1);
      expect(second.wait_index).toBe(1);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(state, "task-a", ctx);
      const candidates = state.task_revisions.filter((record) => record.task_id === "task-a" && record.revision === 2);
      expect(candidates).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- export surfaces and source scan (test area 14) ---------------------------

  test("45. the runtime export surfaces are exactly the contracted keys", async () => {
    const publicModule = await import("../src/pipeline_v2_revise_task_intervention_controller.ts");
    expect(Object.keys(publicModule).sort()).toEqual([
      "PipelineV2ReviseTaskInterventionControllerError",
      "applyPipelineV2ReviseTaskIntervention",
    ]);
    const internalModule = await import("../src/pipeline_v2_revise_task_intervention_controller_internal.ts");
    expect(Object.keys(internalModule).sort()).toEqual([
      "PipelineV2ReviseTaskInterventionControllerError",
      "applyPipelineV2ReviseTaskInterventionWithIo",
      "productionReviseTaskInterventionOps",
    ]);
    expect(Object.isFrozen(productionReviseTaskInterventionOps)).toBe(true);
    expect(productionReviseTaskInterventionOps.restorePlan).toBe(restorePipelineV2AcceptedRunPlan);
    expect(productionReviseTaskInterventionOps.prepareTaskRevision).toBe(prepareTaskRevisionManifest);
    expect(productionReviseTaskInterventionOps.prepareIntent).toBe(prepareWaitIntent);
    expect(productionReviseTaskInterventionOps.acceptIntent).toBe(acceptPipelineV2ReviseTaskIntent);
    expect(productionReviseTaskInterventionOps.completeTask).toBe(completePipelineV2ReviseTask);
  });

  test("46. the controller composes the existing layers only (source scan)", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_revise_task_intervention_controller_internal.ts"), "utf8");
    const countOf = (pattern: string): number => source.split(pattern).length - 1;
    expect(countOf("restorePipelineV2AcceptedRunPlan")).toBe(5);
    expect(countOf("prepareTaskRevisionManifest")).toBe(5);
    expect(countOf("prepareWaitIntent")).toBe(5);
    expect(countOf("acceptPipelineV2ReviseTaskIntent")).toBe(5);
    expect(countOf("completePipelineV2ReviseTask")).toBe(5);
    expect(countOf("reducePipelineV2RunCommand(")).toBe(0);
    expect(countOf("validatePipelineV2RunState(")).toBe(0);
    expect(countOf("canonicalJson(")).toBe(0);
    expect(countOf("CryptoHasher")).toBe(0);
    expect(countOf("createHash")).toBe(0);
    expect(countOf("JSON.parse")).toBe(0);
    expect(countOf("new WeakMap")).toBe(0);
    expect(countOf("new WeakSet")).toBe(0);
    expect(countOf("O_EXCL")).toBe(0);
    expect(countOf("O_NOFOLLOW")).toBe(0);
    expect(countOf("lstat")).toBe(0);
    expect(countOf("node:fs")).toBe(0);
    expect(countOf("node:path")).toBe(0);
    expect(countOf(".match(")).toBe(0);
    expect(countOf("RegExp(")).toBe(0);
    expect(countOf("publishPipelineV2")).toBe(0);
    expect(countOf("loadPipelineV2PlanRevision")).toBe(0);
    for (const banned of [
      "pipeline_v2_coordinator",
      "pipeline_v2_runner",
      "main.ts",
      "cli_",
      "docker",
      "launcher",
      "pipeline_v2_state_store",
      "pipeline_v2_run_plan_store",
      "pipeline_v2_run_plan_candidate.ts",
      "pipeline_v2_run_plan_controller",
      "pipeline_v2_wait_store",
      "pipeline_v2_wait_manifest",
      "pipeline_v2_wait_controller",
      "pipeline_v2_revise_task_intent_controller_internal",
      "pipeline_v2_revise_task_completion_controller_internal",
      "pipeline_v2_revise_task_closure_controller",
      "pipeline_state_store",
    ]) {
      expect(source).not.toContain(banned);
    }
    expect(countOf("let production")).toBe(0);
  });

  // --- invalid options (test area 15) -------------------------------------------

  test("47. malformed options refuse before any effect", async () => {
    const ctx = await reviseReady();
    try {
      const base = interventionOptionsOf(ctx, ctx.sink);
      const malformed: Array<[string, unknown]> = [
        ["null options", null],
        ["primitive options", "options"],
        ["empty runRoot", { ...base, runRoot: "" }],
        ["unsafe run id", { ...base, runId: "../escape" }],
        ["empty run id", { ...base, runId: "" }],
        ["zero wait index", { ...base, waitIndex: 0 }],
        ["negative wait index", { ...base, waitIndex: -1 }],
        ["fractional wait index", { ...base, waitIndex: 1.5 }],
        ["nan wait index", { ...base, waitIndex: Number.NaN }],
        ["unsafe task id", { ...base, taskId: "../task" }],
        ["empty task id", { ...base, taskId: "" }],
        ["empty task body", { ...base, taskBody: "" }],
        ["non-string task body", { ...base, taskBody: 42 }],
      ];
      for (const [label, options] of malformed) {
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskIntervention(options as Parameters<typeof applyPipelineV2ReviseTaskIntervention>[0]),
        );
        const error = expectInterventionError(cause, "invalid_options");
        expect(error.message.length).toBeGreaterThan(0);
        expect(label.length).toBeGreaterThan(0);
      }
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(ctx.revisionAtWindow);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  // --- the racing reconciliation never recovers an acceptance window ----------

  test("48. the racing reconciliation never recovers an acceptance window (R0/R1/R2)", async () => {
    // One honest R0 fixture; the exact R1 and R2 states are built through
    // the production reducer and the real manifest publisher. Each racing
    // sentinel carries the exact state; a recovering facade would call the
    // completion (the canary) instead of re-throwing the sentinel.
    const ctx = await reviseReady({ window: "r0", reopen: false });
    try {
      const sink = ctx.sink;
      const state0 = sink.snapshot as PipelineV2RunState;
      await publishPipelineV2WaitIntent(ctx.fixture.runRoot, ctx.intent.manifest);
      await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: ctx.intent.sha256 });
      const state1 = sink.snapshot as PipelineV2RunState;
      await publishPipelineV2TaskRevision(ctx.fixture.runRoot, ctx.candidate.manifest);
      await sink.dispatch({
        kind: "task_revision_accepted",
        taskId: "task-a",
        revision: ctx.candidate.manifest.revision,
        taskSha256: ctx.candidate.sha256,
        waitIndex: 1,
        intentSha256: ctx.intent.sha256,
      });
      const state2 = sink.snapshot as PipelineV2RunState;
      expect(state1.revision).toBe(state0.revision + 1);
      expect(state2.revision).toBe(state0.revision + 2);

      const scenarios: Array<[string, PipelineV2RunState]> = [
        ["r0", state0],
        ["r1", state1],
        ["r2", state2],
      ];
      for (const [label, presented] of scenarios) {
        let completeCalls = 0;
        let dispatchCalls = 0;
        const stub = {
          get snapshot(): PipelineV2RunState | null {
            return presented;
          },
          get poisoned(): boolean {
            return false;
          },
          async dispatch(): Promise<void> {
            dispatchCalls += 1;
            throw new Error("CANARY-DISPATCH");
          },
        };
        const sentinel = new PipelineV2ReviseTaskIntentControllerError(
          "invalid_state",
          `racing sentinel ${label}`,
          presented,
        );
        const { ops, counts } = spyOps({
          acceptIntent: ((): Promise<unknown> => Promise.reject(sentinel)) as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
          completeTask: (async (): Promise<unknown> => {
            completeCalls += 1;
            throw new Error("CANARY-COMPLETION");
          }) as unknown as PipelineV2ReviseTaskInterventionOps["completeTask"],
        });
        const cause = await catchIntervention(() =>
          applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, stub)),
        );
        expect(cause).toBe(sentinel);
        expect(completeCalls).toBe(0);
        expect(dispatchCalls).toBe(0);
        expect(counts().restorePlan).toBe(1);
        expect(counts().prepareTaskRevision).toBe(1);
        expect(counts().prepareIntent).toBe(1);
        expect(counts().acceptIntent).toBe(1);
        expect(counts().completeTask).toBe(0);
        expect((cause as PipelineV2ReviseTaskIntentControllerError).message).not.toContain("CANARY");
        expect((cause as Error).name).not.toBe("TypeError");
      }
      // No durable writes: the real state file still sits at the honest R2.
      const durable = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      expect((durable.snapshot as PipelineV2RunState).revision).toBe(state2.revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("49. the genuine racing progression still recovers: exact R3 records the response only", async () => {
    const ctx = await reviseReady({ window: "r0", reopen: false });
    try {
      let facadeDispatches = 0;
      let racingDone = false;
      const live = {
        get snapshot(): PipelineV2RunState | null {
          return ctx.sink.snapshot;
        },
        get poisoned(): boolean {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand): Promise<void> {
          if (racingDone) {
            facadeDispatches += 1;
          }
          await ctx.sink.dispatch(command);
        },
      };
      const { ops, counts } = spyOps({
        acceptIntent: (async (options: unknown) => {
          // The real acceptance through the production ops (intent + task).
          await productionReviseTaskInterventionOps.acceptIntent(
            options as Parameters<PipelineV2ReviseTaskInterventionOps["acceptIntent"]>[0],
          );
          // A concurrent durable step closes the iteration while the
          // acceptance is pending; the acceptance's authoritative state is
          // the exact R3 progression.
          await ctx.sink.dispatch({
            kind: "stage_iteration_closed",
            generationIndex: 1,
            iterationIndex: 1,
            by: "replanned",
            waitIndex: 1,
          });
          racingDone = true;
          throw new PipelineV2ReviseTaskIntentControllerError(
            "invalid_state",
            "racing sentinel r3",
            ctx.sink.snapshot as PipelineV2RunState,
          );
        }) as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
      });
      const result = (await applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, live))) as AppliedPipelineV2ReviseTaskIntervention;
      expect(result.wait_index).toBe(1);
      expect(result.action_id).toBe("revise_task");
      expect(result.state).toEqual(ctx.sink.snapshot as PipelineV2RunState);
      // The recovery ran the completion and recorded only the response.
      expect(counts().completeTask).toBe(1);
      expect(facadeDispatches).toBe(1);
      const state = result.state;
      expect(state.revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(state, "task-a", ctx);
      expect(await validatePipelineV2RunState(result.state)).toEqual(result.state);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("50. the genuine racing progression still recovers: exact R4 is the zero-dispatch completed retry", async () => {
    const ctx = await reviseReady({ window: "r0", reopen: false });
    try {
      let facadeDispatches = 0;
      let racingDone = false;
      const live = {
        get snapshot(): PipelineV2RunState | null {
          return ctx.sink.snapshot;
        },
        get poisoned(): boolean {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand): Promise<void> {
          if (racingDone) {
            facadeDispatches += 1;
          }
          await ctx.sink.dispatch(command);
        },
      };
      const { ops, counts } = spyOps({
        acceptIntent: (async (options: unknown) => {
          await productionReviseTaskInterventionOps.acceptIntent(
            options as Parameters<PipelineV2ReviseTaskInterventionOps["acceptIntent"]>[0],
          );
          // The concurrent progression closes the iteration and records the
          // revise_task response through the real wait controller; the
          // acceptance's authoritative state is the exact R4 progression.
          await ctx.sink.dispatch({
            kind: "stage_iteration_closed",
            generationIndex: 1,
            iterationIndex: 1,
            by: "replanned",
            waitIndex: 1,
          });
          await recordPipelineV2WaitAction({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "revise_task" });
          racingDone = true;
          throw new PipelineV2ReviseTaskIntentControllerError(
            "invalid_state",
            "racing sentinel r4",
            ctx.sink.snapshot as PipelineV2RunState,
          );
        }) as unknown as PipelineV2ReviseTaskInterventionOps["acceptIntent"],
      });
      const result = (await applyPipelineV2ReviseTaskInterventionWithIo(ops, interventionOptionsOf(ctx, live))) as AppliedPipelineV2ReviseTaskIntervention;
      expect(result.wait_index).toBe(1);
      expect(result.action_id).toBe("revise_task");
      expect(result.state).toEqual(ctx.sink.snapshot as PipelineV2RunState);
      // The recovery recognized the completed boundary with zero dispatch.
      expect(counts().completeTask).toBe(1);
      expect(facadeDispatches).toBe(0);
      const state = result.state;
      expect(state.revision).toBe(ctx.revisionAtWindow + 4);
      expectCompletedProjection(state, "task-a", ctx);
      expect(await validatePipelineV2RunState(result.state)).toEqual(result.state);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});
