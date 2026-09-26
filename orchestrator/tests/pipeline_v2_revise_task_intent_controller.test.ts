import { describe, expect, test } from "bun:test";
import { lstat, mkdtemp, readFile, rm, unlink, writeFile } from "node:fs/promises";
import { mkdir } from "node:fs/promises";
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
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  publishPipelineV2TaskRevision,
  publishPipelineV2WaitIntent,
  PipelineV2RunPlanStoreError,
} from "../src/pipeline_v2_run_plan_store.ts";
import { PipelineV2RunPlanBindingError } from "../src/pipeline_v2_run_plan_bindings.ts";
import { PipelineV2StateError } from "../src/pipeline_v2_state.ts";
import {
  publishPipelineV2WaitRequest,
} from "../src/pipeline_v2_wait_store.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import {
  acceptPipelineV2ReviseTaskIntent,
  PipelineV2ReviseTaskIntentControllerError,
  type PipelineV2ReviseTaskIntentControllerFailureReason,
  type PipelineV2ReviseTaskIntentControllerSink,
} from "../src/pipeline_v2_revise_task_intent_controller.ts";
import {
  acceptPipelineV2ReviseTaskIntentWithIo,
  productionReviseTaskIntentOps,
  type PipelineV2ReviseTaskIntentControllerOps,
} from "../src/pipeline_v2_revise_task_intent_controller_internal.ts";
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-intent-"));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-intent-run-"));
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

function prepareCandidate(body: string, revision = 2, previous: string | null = A1.sha256): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-a",
    revision,
    previous_sha256: previous,
    origin: revision === 1 ? "planning_proposal" : "user_response",
    body,
  });
}

const A2 = prepareCandidate("Body A revised");
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

function prepareReviseIntent(candidate: PreparedPipelineV2RunTaskRevision, currentDigest: string): PreparedPipelineV2RunWaitIntent {
  return prepareWaitIntent({
    schema_version: 1,
    kind: "revise_task_intent",
    run_id: RUN_ID,
    wait_index: 1,
    task_id: candidate.manifest.task_id,
    expected_previous_task_sha256: currentDigest,
    new_task_revision_sha256: candidate.sha256,
  });
}

const INTENT = prepareReviseIntent(A2, A1.sha256);

interface ReviseCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  candidate: PreparedPipelineV2RunTaskRevision;
}

interface ReadyOptions {
  acceptIntent?: boolean;
  recordTaskRevision?: { sha256?: string };
  extraTaskRevision?: { revision: number; previousSha256: string; sha256: string; body: string };
  noWait?: boolean;
  noReviseAction?: boolean;
  dropCurrentArtifact?: boolean;
  tamperCurrentArtifact?: boolean;
  secondPlanRevision?: boolean;
  dropPlanArtifact?: boolean;
  tamperPlanArtifact?: boolean;
  dropCandidateArtifact?: boolean;
  dropIntentArtifact?: boolean;
}

/**
 * A real sink driven through the real reducer and the existing
 * controllers to the requested revise-intent acceptance boundary.
 */
async function reviseReady(options: ReadyOptions = {}): Promise<ReviseCtx> {
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
        {
          id: "stage-2",
          template: "development",
          tasks: [{ id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [A1, B1],
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
    if (options.noWait !== true) {
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
      if (options.secondPlanRevision === true) {
        // close the iteration, run a second planning execution and accept
        // plan revision 2 while generation 1 stays open with the plan-1
        // digest, then open iteration 2 and run the stage execution again
        await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
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
      }
      const request = preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 1,
        transition_count: 2,
        state_id: "architect",
        reason: "stage_iteration_limit_exhausted",
        actions: options.noReviseAction === true
          ? [{ id: "continue_stage", to: "dev_entry" }]
          : [
              { id: "continue_stage", to: "dev_entry" },
              { id: "revise_task", to: "architect" },
            ],
      });
      await sink.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: request.sha256,
        actions: options.noReviseAction === true
          ? [{ id: "continue_stage", to: "dev_entry" }]
          : [
              { id: "continue_stage", to: "dev_entry" },
              { id: "revise_task", to: "architect" },
            ],
      });
      await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
      if (options.acceptIntent === true) {
        await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT.sha256 });
        await publishPipelineV2WaitIntent(fixture.runRoot, INTENT.manifest);
      }
      if (options.recordTaskRevision !== undefined) {
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: options.recordTaskRevision.sha256 ?? A2.sha256,
          waitIndex: 1,
          intentSha256: INTENT.sha256,
        });
        await publishPipelineV2TaskRevision(fixture.runRoot, A2.manifest);
      }
      if (options.extraTaskRevision !== undefined) {
        const A3 = prepareCandidate(options.extraTaskRevision.body, options.extraTaskRevision.revision, options.extraTaskRevision.previousSha256);
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: A3.manifest.revision,
          taskSha256: A3.sha256,
          waitIndex: 1,
          intentSha256: INTENT.sha256,
        });
        await publishPipelineV2TaskRevision(fixture.runRoot, A3.manifest);
      }
    }
    if (options.dropPlanArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "plans", "1.json"));
    }
    if (options.tamperPlanArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "plans", "1.json"));
      const plan1Tampered = preparePlanRevisionManifest({
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
            tasks: [{ id: "task-a", revision: 1, sha256: hex("7"), depends_on: [] }],
          },
        ],
      });
      await (await import("../src/pipeline_v2_run_plan_store.ts")).publishPipelineV2PlanRevision(fixture.runRoot, plan1Tampered.manifest);
    }
    if (options.dropCurrentArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "tasks", "task-a", "1.json"));
    }
    if (options.dropCandidateArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "tasks", "task-a", "2.json"));
    }
    if (options.dropIntentArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "intents", "1.json"));
    }
    if (options.tamperCurrentArtifact === true) {
      await unlink(join(fixture.runRoot, "run-plan", "tasks", "task-a", "1.json"));
      const tampered = prepareTaskRevisionManifest({
        schema_version: 1,
        kind: "task_revision",
        run_id: RUN_ID,
        task_id: "task-a",
        revision: 1,
        previous_sha256: null,
        origin: "planning_proposal",
        body: "Body A tampered",
      });
      await publishPipelineV2TaskRevision(fixture.runRoot, tampered.manifest);
    }
    return { fixture, sink, pipeline, intent: INTENT, candidate: A2 };
  });
}

function recordingSink(inner: PipelineV2RunStateSink): PipelineV2ReviseTaskIntentControllerSink & { commands: PipelineV2RunCommand[] } {
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

function expectReviseError(cause: unknown, reason: PipelineV2ReviseTaskIntentControllerFailureReason): PipelineV2ReviseTaskIntentControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReviseTaskIntentControllerError);
  const error = cause as PipelineV2ReviseTaskIntentControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

const INTENT_PATH = (runRoot: string): string => join(runRoot, "run-plan", "intents", "1.json");
const TASK2_PATH = (runRoot: string): string => join(runRoot, "run-plan", "tasks", "task-a", "2.json");

describe("acceptPipelineV2ReviseTaskIntent", () => {
  test("1. C0 happy path: both manifests published, both commands dispatched in order", async () => {
    const ctx = await reviseReady();
    try {
      const recording = recordingSink(ctx.sink);
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "plan_intent_accepted",
        "task_revision_accepted",
      ]);
      const taskCommand = recording.commands[1] as { taskId: string; revision: number; taskSha256: string; waitIndex: number; intentSha256: string };
      expect(taskCommand).toMatchObject({ taskId: "task-a", revision: 2, taskSha256: ctx.candidate.sha256, waitIndex: 1, intentSha256: ctx.intent.sha256 });
      expect(result).toMatchObject({
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
      });
      const wait = result.state.waits[0];
      expect(wait?.intent).toEqual({ intent_sha256: ctx.intent.sha256 });
      expect(result.state.task_revisions[result.state.task_revisions.length - 1]).toMatchObject({
        task_id: "task-a",
        revision: 2,
        sha256: ctx.candidate.sha256,
        previous_sha256: A1.sha256,
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
      });
      expect(result.state.status).toBe("waiting");
      expect(result.state.waits[0]?.response).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the published manifests carry exactly the canonical bytes at the fixed paths", async () => {
    const ctx = await reviseReady();
    try {
      await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      const intentBytes = await readFile(INTENT_PATH(ctx.fixture.runRoot), "utf8");
      const taskBytes = await readFile(TASK2_PATH(ctx.fixture.runRoot), "utf8");
      expect(intentBytes).toBe(ctx.intent.canonical_json);
      expect(taskBytes).toBe(ctx.candidate.canonical_json);
      expect((await lstat(INTENT_PATH(ctx.fixture.runRoot))).mode & 0o777).toBe(0o600);
      expect((await lstat(TASK2_PATH(ctx.fixture.runRoot))).mode & 0o777).toBe(0o600);
      expect(intentBytes).not.toContain("Body A revised");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the result shape is exact, deep-frozen and content-free", async () => {
    const ctx = await reviseReady();
    try {
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(Object.keys(result).sort()).toEqual([
        "intent_sha256",
        "state",
        "task_id",
        "task_revision",
        "task_sha256",
        "wait_index",
      ]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      expect(JSON.stringify(result)).not.toContain(ctx.fixture.runRoot);
      expect(JSON.stringify(result)).not.toContain("Body A revised");
      expect(JSON.stringify(result)).not.toContain(ctx.candidate.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the accepted state round-trips through the loader", async () => {
    const ctx = await reviseReady();
    try {
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      validatePipelineV2RunState(JSON.parse(JSON.stringify(result.state)) as never);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.task_revisions[persisted.task_revisions.length - 1]?.revision).toBe(2);
      expect(persisted.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. partial retry: the durable intent dispatches only the task revision", async () => {
    const ctx = await reviseReady({ acceptIntent: true });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted"]);
      expect(result.state.task_revisions).toHaveLength(3);
      expect(result.state.task_revisions[2]?.sha256).toBe(ctx.candidate.sha256);
      expect(result.state.revision).toBe((ctx.sink.snapshot?.revision ?? 0));
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. full idempotent retry: both durable, zero dispatch, files preserved", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {} });
    try {
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      const revision = ctx.sink.snapshot?.revision;
      if (typeof revision !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const intentStat = await lstat(INTENT_PATH(ctx.fixture.runRoot));
      const taskStat = await lstat(TASK2_PATH(ctx.fixture.runRoot));
      const intentBytes = await readFile(INTENT_PATH(ctx.fixture.runRoot));
      const taskBytes = await readFile(TASK2_PATH(ctx.fixture.runRoot));
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, task_revision: 2, task_sha256: ctx.candidate.sha256 });
      expect(retry.state.revision).toBe(revision);
      const intentAfter = await lstat(INTENT_PATH(ctx.fixture.runRoot));
      const taskAfter = await lstat(TASK2_PATH(ctx.fixture.runRoot));
      expect(intentAfter.ino).toBe(intentStat.ino);
      expect(intentAfter.mtimeMs).toBe(intentStat.mtimeMs);
      expect(taskAfter.ino).toBe(taskStat.ino);
      expect(await readFile(INTENT_PATH(ctx.fixture.runRoot))).toEqual(intentBytes);
      expect(await readFile(TASK2_PATH(ctx.fixture.runRoot))).toEqual(taskBytes);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. a different durable intent is an intent conflict with zero writes", async () => {
    const ctx = await reviseReady({ acceptIntent: false });
    try {
      await ctx.sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("e") });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "intent_conflict");
      expect(error.message).toContain("already accepted a different revise_task intent");
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(hex("e"));
      await expect(lstat(INTENT_PATH(ctx.fixture.runRoot))).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. stale candidate: a durable ledger past the candidate is a candidate conflict", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {}, extraTaskRevision: { revision: 3, previousSha256: A2.sha256, sha256: hex("3"), body: "Body A third" } });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "candidate_conflict");
      expect(error.message).toContain("already moved past revision 2");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. a different digest at the durable candidate revision is a candidate conflict", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: { sha256: hex("9") } });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "candidate_conflict");
      expect(error.message).toContain("already carries different content");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. the candidate binding failures keep the original binding error class", async () => {
    const ctx = await reviseReady();
    try {
      const variants: Array<[string, PreparedPipelineV2RunTaskRevision | PreparedPipelineV2RunWaitIntent, string]> = [
        [
          "broken chain",
          prepareCandidate("Body A revised", 2, hex("7")),
          "previous_sha256 does not name the predecessor digest",
        ],
        [
          "revision gap",
          prepareCandidate("Body A third", 3, A1.sha256),
          "revision numbers are not consecutive",
        ],
      ];
      for (const [label, prepared, expectedMessage] of variants) {
        const isTask = "body" in ((prepared as { manifest?: { body?: unknown } }).manifest ?? {});
        const intentForCase = isTask
          ? prepareReviseIntent(prepared as PreparedPipelineV2RunTaskRevision, A1.sha256)
          : ctx.intent;
        const candidateForCase = isTask ? (prepared as PreparedPipelineV2RunTaskRevision) : ctx.candidate;
        const cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: intentForCase, candidateTaskRevision: candidateForCase }),
        );
        expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
        expect((cause as Error).message).toContain(expectedMessage);
      }
      // the intent digest variants
      const wrongNewDigest = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: hex("8"),
      });
      let cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: wrongNewDigest, candidateTaskRevision: ctx.candidate }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
      expect((cause as Error).message).toContain("new_task_revision_sha256 does not name the candidate task digest");
      const wrongTaskId = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-b",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: ctx.candidate.sha256,
      });
      cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: wrongTaskId, candidateTaskRevision: ctx.candidate }),
      );
      // the plan-first order: an unknown task has no task pointer in the
      // open generation's stage of the last accepted plan revision
      const taskIdError = expectReviseError(cause, "invalid_state");
      expect(taskIdError.message).toContain("does not carry the task of the revise intent");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. a non-registered intent or candidate is rejected before any field read", async () => {
    const ctx = await reviseReady();
    try {
      const handBuilt = { manifest: ctx.intent.manifest, canonical_json: ctx.intent.canonical_json, sha256: ctx.intent.sha256 };
      let cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: handBuilt as never, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_intent");
      expect(error.message).toContain("not a provenance-registered revise_task_intent");
      const continueIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: hex("5"),
        additional_iterations: 1,
      });
      cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: continueIntent, candidateTaskRevision: ctx.candidate }),
      );
      expectReviseError(cause, "invalid_intent");
      const handBuiltCandidate = { manifest: ctx.candidate.manifest, canonical_json: ctx.candidate.canonical_json, sha256: ctx.candidate.sha256 };
      cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: handBuiltCandidate as never }),
      );
      const candidateError = expectReviseError(cause, "invalid_intent");
      expect(candidateError.message).toContain("candidate task revision is not a provenance-registered task revision");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. the durable boundary rejections", async () => {
    const ctx = await reviseReady();
    try {
      // not waiting
      const activeFixtureCtx = await reviseReady({ noWait: true });
      try {
        const cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: activeFixtureCtx.fixture.runRoot, sink: activeFixtureCtx.sink, intent: activeFixtureCtx.intent, candidateTaskRevision: activeFixtureCtx.candidate }),
        );
        const error = expectReviseError(cause, "invalid_state");
        expect(error.message).toContain("the run is not waiting");
      } finally {
        await disposeRun(activeFixtureCtx.fixture);
      }
      // foreign run
      const foreignIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: "run-2",
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: ctx.candidate.sha256,
      });
      let cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: foreignIntent, candidateTaskRevision: ctx.candidate }),
      );
      expect((cause as Error).message).toContain("another run");
      // wrong wait index
      const wrongWaitIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 2,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: ctx.candidate.sha256,
      });
      cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: wrongWaitIntent, candidateTaskRevision: ctx.candidate }),
      );
      expect((cause as Error).message).toContain("but the open wait record is 1");
      // no revise_task action
      const noActionCtx = await reviseReady({ noReviseAction: true });
      try {
        cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: noActionCtx.fixture.runRoot, sink: noActionCtx.sink, intent: noActionCtx.intent, candidateTaskRevision: noActionCtx.candidate }),
        );
        expect((cause as Error).message).toContain("does not declare the revise_task action");
      } finally {
        await disposeRun(noActionCtx.fixture);
      }
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. a missing or mismatched current task revision is an invalid state", async () => {
    // unknown task id
    const ctx = await reviseReady();
    try {
      const unknownTaskIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-z",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: ctx.candidate.sha256,
      });
      let cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: unknownTaskIntent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the task of the revise intent");
      // the dropped artifact
      const droppedCtx = await reviseReady({ dropCurrentArtifact: true });
      try {
        cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: droppedCtx.fixture.runRoot, sink: droppedCtx.sink, intent: droppedCtx.intent, candidateTaskRevision: droppedCtx.candidate }),
        );
        expect((cause as Error).message).toContain("is not published on the run's data plane");
      } finally {
        await disposeRun(droppedCtx.fixture);
      }
      // the tampered artifact (a different digest for the same revision)
      const tamperedCtx = await reviseReady({ tamperCurrentArtifact: true });
      try {
        cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: tamperedCtx.fixture.runRoot, sink: tamperedCtx.sink, intent: tamperedCtx.intent, candidateTaskRevision: tamperedCtx.candidate }),
        );
        expect((cause as Error).message).toContain("does not match the durable task record");
      } finally {
        await disposeRun(tamperedCtx.fixture);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a pre-placed conflicting manifest file is a store conflict with zero dispatch", async () => {
    const ctx = await reviseReady();
    try {
      // intent file conflict
      await mkdir(join(ctx.fixture.runRoot, "run-plan", "intents"), { recursive: true, mode: 0o700 });
      await writeFile(INTENT_PATH(ctx.fixture.runRoot), "{}", { mode: 0o600 });
      let cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      expect(await readFile(INTENT_PATH(ctx.fixture.runRoot), "utf8")).toBe("{}");
      // task file conflict: the intent publisher is never called and the
      // intent file stays absent
      const cleanCtx = await reviseReady();
      try {
        await mkdir(join(cleanCtx.fixture.runRoot, "run-plan", "tasks", "task-a"), { recursive: true, mode: 0o700 });
        await writeFile(TASK2_PATH(cleanCtx.fixture.runRoot), "{}", { mode: 0o600 });
        cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: cleanCtx.fixture.runRoot, sink: cleanCtx.sink, intent: cleanCtx.intent, candidateTaskRevision: cleanCtx.candidate }),
        );
        expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
        expect(cleanCtx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
        expect(cleanCtx.sink.snapshot?.task_revisions).toHaveLength(2);
        expect(await readFile(TASK2_PATH(cleanCtx.fixture.runRoot), "utf8")).toBe("{}");
        await expect(lstat(INTENT_PATH(cleanCtx.fixture.runRoot))).rejects.toMatchObject({ code: "ENOENT" });
      } finally {
        await disposeRun(cleanCtx.fixture);
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. the intent dispatch not-committed keeps both manifests as orphans and the retry commits both", async () => {
    const ctx = await reviseReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "state_persist_failed");
      expect(error.message).toContain("the revise task intent acceptance could not be committed");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
      expect((await lstat(INTENT_PATH(ctx.fixture.runRoot))).isFile()).toBe(true);
      expect((await lstat(TASK2_PATH(ctx.fixture.runRoot))).isFile()).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: fresh, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result.state.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
      expect(result.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. the intent dispatch durability-unknown adopts the intent and the retry dispatches only the task", async () => {
    const ctx = await reviseReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be confirmed durable");
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted"]);
      expect(result.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. the task dispatch not-committed keeps the intent durable and the retry dispatches only the task", async () => {
    const ctx = await reviseReady({ acceptIntent: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "state_persist_failed");
      expect(error.message).toContain("the task revision acceptance could not be committed");
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted"]);
      expect(result.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. the task dispatch durability-unknown adopts the revision and the full retry dispatches nothing", async () => {
    const ctx = await reviseReady({ acceptIntent: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      expectReviseError(cause, "state_persist_failed");
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(retry.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a racing identical dispatch is idempotent success on the exact durable record", async () => {
    const ctx = await reviseReady({ acceptIntent: true });
    try {
      const recording = recordingSink(ctx.sink);
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await recording.dispatch(command);
          throw new PipelineV2StateError("simulated lost race");
        },
      };
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result).toMatchObject({ task_revision: 2, task_sha256: ctx.candidate.sha256 });
      expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted"]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a resolve-without-change dispatch failure is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const lieSink: PipelineV2ReviseTaskIntentControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          throw new PipelineV2StateError("nothing happened");
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: lieSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry it in the open wait 1");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. two identical racing acceptances both succeed with one inode per file and +2 revisions", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const first = acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      const second = acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      const [one, two] = await Promise.all([first, second]);
      expect(one).toMatchObject({ task_revision: 2 });
      expect(two).toMatchObject({ task_revision: 2 });
      expect(ctx.sink.snapshot?.revision).toBe(revisionBefore + 2);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.task_revisions[2]?.sha256).toBe(ctx.candidate.sha256);
      expect((await lstat(INTENT_PATH(ctx.fixture.runRoot))).ino).toBeNumber();
      expect((await lstat(TASK2_PATH(ctx.fixture.runRoot))).ino).toBeNumber();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. options and ops getters are read exactly once", async () => {
    const ctx = await reviseReady();
    try {
      const optionReads: string[] = [];
      const opsReads: string[] = [];
      const optionsProxy = new Proxy({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate } as Record<string, unknown>, {
        get(target, property) {
          optionReads.push(String(property));
          return target[property as string];
        },
      });
      const opsProxy = new Proxy(productionReviseTaskIntentOps as unknown as Record<string, unknown>, {
        get(target, property) {
          opsReads.push(String(property));
          return target[property as string];
        },
      });
      const result = await acceptPipelineV2ReviseTaskIntentWithIo(opsProxy as unknown as PipelineV2ReviseTaskIntentControllerOps, optionsProxy as unknown as Parameters<typeof acceptPipelineV2ReviseTaskIntent>[0]);
      expect(result).toMatchObject({ task_revision: 2 });
      expect(optionReads.filter((name) => name === "runRoot" || name === "sink" || name === "intent" || name === "candidateTaskRevision")).toEqual([
        "runRoot",
        "sink",
        "intent",
        "candidateTaskRevision",
      ]);
      expect(opsReads.filter((name) => name === "loadTaskRevision" || name === "publishTaskRevision" || name === "publishWaitIntent")).toEqual([
        "loadTaskRevision",
        "publishTaskRevision",
        "publishWaitIntent",
      ]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. caller mutation after the capture cannot influence the acceptance", async () => {
    const ctx = await reviseReady();
    try {
      const options = { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate };
      const pending = acceptPipelineV2ReviseTaskIntent(options);
      (options as { runRoot: string }).runRoot = "/replaced";
      const result = await pending;
      expect(result).toMatchObject({ task_revision: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. unexpected downstream errors preserve their identity", async () => {
    const ctx = await reviseReady();
    try {
      const boom = new Error("the task loader failed unexpectedly");
      const throwingOps: PipelineV2ReviseTaskIntentControllerOps = {
        ...productionReviseTaskIntentOps,
        loadTaskRevision: async () => {
          throw boom;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntentWithIo(throwingOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      expect(cause).toBe(boom);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. diagnostics are content-free across the failure paths", async () => {
    const ctx = await reviseReady({ acceptIntent: false });
    try {
      await ctx.sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("e") });
      const messages: string[] = [];
      const conflict = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      messages.push((conflict as Error).message);
      const optionsBoom = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: undefined as unknown as PipelineV2ReviseTaskIntentControllerSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      messages.push((optionsBoom as Error).message);
      for (const message of messages) {
        expect(message).not.toContain(ctx.fixture.runRoot);
        expect(message).not.toContain(ctx.intent.canonical_json);
        expect(message).not.toContain(ctx.candidate.canonical_json);
        expect(message).not.toContain("Body A revised");
        expect(message).not.toContain(hex("e"));
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

  test("28. a task of another stage of the last plan is rejected as invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const otherStageIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-b",
        expected_previous_task_sha256: B1.sha256,
        new_task_revision_sha256: hex("4"),
      });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: otherStageIntent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the task of the revise intent");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. a generation bound to another plan digest is rejected as invalid state", async () => {
    const ctx = await reviseReady({ secondPlanRevision: true });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not belong to the last durable plan revision 2");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a missing or mismatched last plan artifact is rejected as invalid state", async () => {
    const dropped = await reviseReady({ dropPlanArtifact: true });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: dropped.fixture.runRoot, sink: dropped.sink, intent: dropped.intent, candidateTaskRevision: dropped.candidate }),
      );
      expect((cause as Error).message).toContain("is not published on the run's data plane");
    } finally {
      await disposeRun(dropped.fixture);
    }
    const tampered = await reviseReady({ tamperPlanArtifact: true });
    try {
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: tampered.fixture.runRoot, sink: tampered.sink, intent: tampered.intent, candidateTaskRevision: tampered.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not match the durable plan record");
      expect(tampered.sink.snapshot?.waits[0]?.intent).toBeUndefined();
    } finally {
      await disposeRun(tampered.fixture);
    }
  });

  test("31. the plan pointer, not the ledger tail, derives the current revision", async () => {
    // R2: the ledger's latest task-a record is the candidate (revision 2),
    // while the plan pointer names revision 1 — the current revision must
    // still come from the pointer, so the identical retry succeeds
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {} });
    try {
      const recording = recordingSink(ctx.sink);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ task_id: "task-a", task_revision: 2, task_sha256: ctx.candidate.sha256 });
    } finally {
      await disposeRun(ctx.fixture);
    }
    // and on a fresh run (no durable intent) a candidate whose
    // predecessor names the ledger tail instead of the pointer revision is
    // rejected by the chain validator
    const fresh = await reviseReady();
    try {
      const stalePredecessor = prepareCandidate("Body A fourth", 2, hex("6"));
      const staleIntent = prepareReviseIntent(stalePredecessor, A1.sha256);
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: fresh.fixture.runRoot, sink: fresh.sink, intent: staleIntent, candidateTaskRevision: stalePredecessor }),
      );
      expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
      expect((cause as Error).message).toContain("previous_sha256 does not name the predecessor digest");
    } finally {
      await disposeRun(fresh.fixture);
    }
  });

  test("32. an unchanged task body is invalid intent with zero publication and dispatch", async () => {
    const ctx = await reviseReady();
    try {
      const unchanged = prepareCandidate("Body A");
      const unchangedIntent = prepareReviseIntent(unchanged, A1.sha256);
      await expect(lstat(TASK2_PATH(ctx.fixture.runRoot))).rejects.toMatchObject({ code: "ENOENT" });
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: unchangedIntent, candidateTaskRevision: unchanged }),
      );
      const error = expectReviseError(cause, "invalid_intent");
      expect(error.message).toContain("does not change the current task body");
      expect(error.message).not.toContain("Body A");
      expect(ctx.sink.snapshot?.waits[0]?.intent).toBeUndefined();
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
      await expect(lstat(TASK2_PATH(ctx.fixture.runRoot))).rejects.toMatchObject({ code: "ENOENT" });
      await expect(lstat(INTENT_PATH(ctx.fixture.runRoot))).rejects.toMatchObject({ code: "ENOENT" });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. R2 with a removed candidate artifact restores it exactly with zero dispatch", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {}, dropCandidateArtifact: true });
    try {
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(await readFile(TASK2_PATH(ctx.fixture.runRoot), "utf8")).toBe(ctx.candidate.canonical_json);
      expect((await lstat(TASK2_PATH(ctx.fixture.runRoot))).mode & 0o777).toBe(0o600);
      expect(retry.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. R2 with a removed intent artifact restores it exactly with zero dispatch", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {}, dropIntentArtifact: true });
    try {
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(await readFile(INTENT_PATH(ctx.fixture.runRoot), "utf8")).toBe(ctx.intent.canonical_json);
      expect((await lstat(INTENT_PATH(ctx.fixture.runRoot))).mode & 0o777).toBe(0o600);
      expect(retry.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. R2 with both artifacts removed restores them in the task then intent order with zero dispatch", async () => {
    const ctx = await reviseReady({ acceptIntent: true, recordTaskRevision: {}, dropCandidateArtifact: true, dropIntentArtifact: true });
    try {
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(recording.commands).toEqual([]);
      expect(await readFile(TASK2_PATH(ctx.fixture.runRoot), "utf8")).toBe(ctx.candidate.canonical_json);
      expect(await readFile(INTENT_PATH(ctx.fixture.runRoot), "utf8")).toBe(ctx.intent.canonical_json);
      expect(retry.state.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("36. a hostile sink dispatch getter is read exactly once", async () => {
    const ctx = await reviseReady();
    try {
      let dispatchReads = 0;
      const hostileSink: PipelineV2ReviseTaskIntentControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        get dispatch(): (command: PipelineV2RunCommand) => Promise<void> {
          dispatchReads += 1;
          if (dispatchReads > 1) {
            throw new Error("the sink dispatch member must be read exactly once");
          }
          return (command) => ctx.sink.dispatch(command);
        },
      };
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: hostileSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result).toMatchObject({ task_revision: 2 });
      expect(dispatchReads).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("37. a hostile post-intent snapshot that closes the generation is invalid state without a task dispatch", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const generation = derived.generations[0];
          if (generation !== undefined) {
            (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, closed: { by: "next_stage", closed_transition_count: generation.opened_transition_count } };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted revise intent");
      expect(taskDispatches).toBe(0);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("37b. a racing intent whose authoritative snapshot closed the generation is rejected with zero task dispatch", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          await ctx.sink.dispatch(command);
          throw new PipelineV2StateError("simulated lost race");
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const generation = derived.generations[0];
          if (generation !== undefined) {
            (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, closed: { by: "next_stage", closed_transition_count: generation.opened_transition_count } };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry it in the open wait 1");
      expect(taskDispatches).toBe(0);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("38. a hostile post-task snapshot that removes or changes the wait intent is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const realDispatch = (command: PipelineV2RunCommand) => ctx.sink.dispatch(command);
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await realDispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.response !== undefined || real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const waitRecord = derived.waits[0];
          if (waitRecord !== undefined) {
            (derived.waits as unknown as PipelineV2RunState["waits"])[0] = { ...waitRecord, intent: undefined };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("39. a hostile post-task snapshot with a duplicate or later task record is not success", async () => {
    const ctx = await reviseReady();
    try {
      const realDispatch = (command: PipelineV2RunCommand) => ctx.sink.dispatch(command);
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await realDispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const last = derived.task_revisions[derived.task_revisions.length - 1];
          if (last !== undefined) {
            (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"]).push({
              ...last,
              index: last.index + 1,
              revision: last.revision + 1,
            });
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("40. a malformed nested post-dispatch snapshot is typed invalid state, never a TypeError", async () => {
    const ctx = await reviseReady();
    try {
      const realDispatch = (command: PipelineV2RunCommand) => ctx.sink.dispatch(command);
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await realDispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as unknown as Record<string, unknown>;
          derived["waits"] = "boom";
          return derived as unknown as PipelineV2RunState;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message.length).toBeGreaterThan(0);
      expect(error.message).not.toContain("boom");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("41. conflicting concurrent candidates: one winner, one typed loser, no winner rewrite", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const loserCandidate = prepareCandidate("Body A loser");
      const loserIntent = prepareReviseIntent(loserCandidate, A1.sha256);
      const winner = acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      // the loser's task publication is deterministically delayed behind
      // the winner's full completion (a promise barrier, no sleep)
      const winnerSettled = winner.then(() => undefined, () => undefined);
      const loserOps: PipelineV2ReviseTaskIntentControllerOps = {
        ...productionReviseTaskIntentOps,
        publishTaskRevision: async (runRoot: string, manifest: unknown) => {
          await winnerSettled;
          return await productionReviseTaskIntentOps.publishTaskRevision(runRoot, manifest);
        },
      };
      const loser = acceptPipelineV2ReviseTaskIntentWithIo(loserOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: loserIntent, candidateTaskRevision: loserCandidate });
      const [win, lose] = await Promise.all([winner.catch((cause) => cause), loser.catch((cause) => cause)]);
      const winnerResult = win as { task_revision?: number };
            expect(winnerResult.task_revision).toBe(2);
      // the loser's candidate file conflicts with the winner's at the same
      // task path: the typed store conflict wins and the winner is never
      // rewritten
      expect(lose).toBeInstanceOf(PipelineV2RunPlanStoreError);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.task_revisions[2]?.sha256).toBe(ctx.candidate.sha256);
      expect(ctx.sink.snapshot?.revision).toBe(revisionBefore + 2);
      expect(await readFile(TASK2_PATH(ctx.fixture.runRoot), "utf8")).toBe(ctx.candidate.canonical_json);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
test("26. the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_revise_task_intent_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ReviseTaskIntentControllerError",
    "acceptPipelineV2ReviseTaskIntent",
  ]);
  const internalModule = await import("../src/pipeline_v2_revise_task_intent_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ReviseTaskIntentControllerError",
    "acceptPipelineV2ReviseTaskIntentWithIo",
    "productionReviseTaskIntentOps",
  ]);
});

test("27. the revise acceptance composes the existing layers only (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_revise_task_intent_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("validateReviseIntentBinding(")).toBe(1);
  expect(countOf("validateTaskRevisionChain(")).toBe(1);
  expect(countOf("loadPipelineV2TaskRevision(")).toBe(1);
  expect(countOf("publishPipelineV2TaskRevision(")).toBe(1);
  expect(countOf("publishPipelineV2WaitIntent(")).toBe(1);
  expect(countOf("prepareTaskRevisionManifest(")).toBe(0);
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
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  for (const banned of ["pipeline_v2_coordinator", "pipeline_v2_runner", "main.ts", "cli_", "docker", "launcher", "pipeline_v2_wait_store", "pipeline_v2_wait_manifest", "pipeline_v2_wait_controller", "pipeline_v2_state_store_io", "pipeline_state_store"]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("let production")).toBe(0);
  // the reducer pre-check helper is internal: no runtime export and no
  // test seam (the pre-check ordering is proven by the source-order test)
  expect(countOf("export function precheckReviseSequence")).toBe(0);
});
  test("42. the reducer pre-check precedes both publishers (source-order proof)", async () => {
    const { readFileSync } = await import("node:fs");
    const source = readFileSync("orchestrator/src/pipeline_v2_revise_task_intent_controller_internal.ts", "utf8");
    const flow = source.slice(source.indexOf("export async function acceptPipelineV2ReviseTaskIntentWithIo"));
    const precheck = flow.indexOf("precheckReviseSequence(state, [intentCommand, taskCommand], state)");
    const precheckR1 = flow.indexOf("precheckReviseSequence(state, [taskCommand], state)");
    const publishTask = flow.indexOf("await publishTaskRevision(runRoot");
    const publishIntent = flow.indexOf("await publishWaitIntent(runRoot");
    expect(precheck).toBeGreaterThan(0);
    expect(precheckR1).toBeGreaterThan(precheck);
    expect(publishTask).toBeGreaterThan(precheckR1);
    expect(publishIntent).toBeGreaterThan(publishTask);
    // the explicit fail-closed branch for a durable candidate without the
    // exact accepted intent (unreachable through loader-valid states)
    expect(flow).toContain("if (candidateDurable && !intentDurable)");
    // the unified post-intent classification is used on both paths
    expect(flow.split("classifyPostIntentState(").length - 1).toBe(2);
  });

  test("44. a racing intent with the unchanged ledger dispatches the task suffix exactly once", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          await ctx.sink.dispatch(command);
          throw new PipelineV2StateError("simulated lost race");
        },
        get snapshot() {
          return ctx.sink.snapshot;
        },
      };
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result).toMatchObject({ task_revision: 2, task_sha256: ctx.candidate.sha256 });
      expect(taskDispatches).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("45. a racing intent that already appended the exact candidate is idempotent success with zero further dispatch", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          if (command.kind === "plan_intent_accepted") {
            // the racing full progression: both commands committed, the
            // task dispatch routed through this sink to be counted
            await ctx.sink.dispatch(command);
            await raceSink.dispatch({ kind: "task_revision_accepted", taskId: ctx.candidate.manifest.task_id, revision: ctx.candidate.manifest.revision, taskSha256: ctx.candidate.sha256, waitIndex: 1, intentSha256: ctx.intent.sha256 });
            throw new PipelineV2StateError("simulated lost race");
          }
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          return ctx.sink.snapshot;
        },
      };
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result).toMatchObject({ task_revision: 2, task_sha256: ctx.candidate.sha256 });
      expect(taskDispatches).toBe(1);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("46. a racing intent with a changed plan binding is rejected with zero task dispatch", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          await ctx.sink.dispatch(command);
          throw new PipelineV2StateError("simulated lost race");
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const generation = derived.generations[0];
          if (generation !== undefined) {
            (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, plan_sha256: hex("2") };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry it in the open wait 1");
      expect(taskDispatches).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("47. a post-task snapshot with a mutated predecessor digest is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const last = derived.task_revisions[derived.task_revisions.length - 1];
          if (last !== undefined) {
            (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"])[derived.task_revisions.length - 1] = { ...last, previous_sha256: hex("5") };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("48. a post-task snapshot with a mutated early foreign task record is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const foreign = derived.task_revisions[1];
          if (foreign !== undefined) {
            (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"])[1] = { ...foreign, sha256: hex("6") };
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("49. a post-task snapshot with a foreign record inserted before the exact candidate is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const last = derived.task_revisions[derived.task_revisions.length - 1];
          if (last !== undefined) {
            (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"]).splice(derived.task_revisions.length - 1, 0, { ...last, index: 99 });
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("50. a hostile post-intent snapshot with a second wait record is invalid state", async () => {
    const ctx = await reviseReady();
    try {
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const waitRecord = derived.waits[0];
          if (waitRecord !== undefined) {
            (derived.waits as unknown as PipelineV2RunState["waits"]).push({ ...waitRecord, index: 2 });
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted revise intent");
      expect(ctx.sink.snapshot?.waits).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("51. malformed post-dispatch snapshot shapes yield typed invalid state, never a TypeError", async () => {
    const firstRecord = (value: unknown): Record<string, unknown> | undefined => {
      if (!Array.isArray(value) || value.length < 1) {
        return undefined;
      }
      const entry = value[0];
      return typeof entry === "object" && entry !== null && !Array.isArray(entry)
        ? (entry as Record<string, unknown>)
        : undefined;
    };
    // Real reducer-produced snapshots narrowed by structuredClone
    // mutations: every viewed position is a `null` or primitive value the
    // verification helpers must fail closed on before any field access.
    const postIntentVariants: Array<[string, (derived: Record<string, unknown>) => void]> = [
      ["waits array with a null entry", (derived) => { derived["waits"] = [null]; }],
      ["target wait null", (derived) => {
        const waits = derived["waits"];
        if (Array.isArray(waits) && waits.length > 0) {
          (waits as unknown[])[0] = null;
        }
      }],
      ["wait actions null", (derived) => {
        const waitRecord = firstRecord(derived["waits"]);
        if (waitRecord !== undefined) {
          waitRecord["actions"] = null;
        }
      }],
      ["wait actions with a null entry", (derived) => {
        const waitRecord = firstRecord(derived["waits"]);
        const actions = waitRecord?.["actions"];
        if (Array.isArray(actions) && actions.length > 0) {
          (actions as unknown[])[0] = null;
        }
      }],
      ["wait intent null", (derived) => {
        const waitRecord = firstRecord(derived["waits"]);
        if (waitRecord !== undefined) {
          waitRecord["intent"] = null;
        }
      }],
      ["plan revisions with a null entry", (derived) => {
        const plans = derived["plan_revisions"];
        if (Array.isArray(plans) && plans.length > 0) {
          (plans as unknown[])[0] = null;
        }
      }],
      ["generations with a null entry", (derived) => {
        const generations = derived["generations"];
        if (Array.isArray(generations) && generations.length > 0) {
          (generations as unknown[])[0] = null;
        }
      }],
      ["generation iterations null", (derived) => {
        const generation = firstRecord(derived["generations"]);
        if (generation !== undefined) {
          generation["iterations"] = null;
        }
      }],
      ["generation iterations with a null entry", (derived) => {
        const generation = firstRecord(derived["generations"]);
        const iterations = generation?.["iterations"];
        if (Array.isArray(iterations) && iterations.length > 0) {
          (iterations as unknown[])[0] = null;
        }
      }],
      ["open iteration projection null", (derived) => {
        const generation = firstRecord(derived["generations"]);
        if (generation !== undefined) {
          generation["open_iteration"] = null;
        }
      }],
      ["cursor null", (derived) => { derived["cursor"] = null; }],
      ["task ledger prefix null entry", (derived) => {
        const ledger = derived["task_revisions"];
        if (Array.isArray(ledger) && ledger.length > 1) {
          (ledger as unknown[])[1] = null;
        }
      }],
      ["task ledger prefix primitive entry", (derived) => {
        const ledger = derived["task_revisions"];
        if (Array.isArray(ledger) && ledger.length > 0) {
          (ledger as unknown[])[0] = 7;
        }
      }],
      ["transitions null", (derived) => { derived["transitions"] = null; }],
      ["executions null", (derived) => { derived["executions"] = null; }],
    ];
    for (const [label, mutate] of postIntentVariants) {
      const ctx = await reviseReady();
      try {
        let taskDispatches = 0;
        const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
          get poisoned() {
            return ctx.sink.poisoned;
          },
          async dispatch(command: PipelineV2RunCommand) {
            if (command.kind === "task_revision_accepted") {
              taskDispatches += 1;
            }
            return await ctx.sink.dispatch(command);
          },
          get snapshot() {
            const real = ctx.sink.snapshot as PipelineV2RunState;
            if (real.waits[0]?.intent === undefined) {
              return real;
            }
            const derived = structuredClone(real) as unknown as Record<string, unknown>;
            mutate(derived);
            return derived as unknown as PipelineV2RunState;
          },
        };
        const cause = await catchAccept(() =>
          acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
        );
        const error = expectReviseError(cause, "invalid_state");
        expect((cause as Error).name).toBe("PipelineV2ReviseTaskIntentControllerError");
        expect(error.message).not.toContain("null");
        expect(error.message).not.toContain(label);
        // the malformed post-intent snapshot dispatches no task revision
        expect(taskDispatches).toBe(0);
        expect(ctx.sink.snapshot?.task_revisions).toHaveLength(2);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
    // The malformed appended candidate must actually reach
    // taskLedgerExactAppend through the post-task path, not be filtered
    // out earlier by another check.
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.task_revisions.length < 3) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          (derived.task_revisions as unknown as unknown[])[derived.task_revisions.length - 1] = null;
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect((cause as Error).name).toBe("PipelineV2ReviseTaskIntentControllerError");
      expect(error.message).toContain("does not carry the accepted task revision");
      expect(error.message).not.toContain("null");
      // exactly the one normal task dispatch ran; the malformed record is
      // rejected, not accepted
      expect(taskDispatches).toBe(1);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("52. the racing P2 result is exactly the classified snapshot with no re-read and zero further dispatch", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      let snapshotReads = 0;
      const raceSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          if (command.kind === "plan_intent_accepted") {
            // the racing full progression committed by the concurrent
            // writer directly through the real sink
            await ctx.sink.dispatch(command);
            await ctx.sink.dispatch({
              kind: "task_revision_accepted",
              taskId: ctx.candidate.manifest.task_id,
              revision: ctx.candidate.manifest.revision,
              taskSha256: ctx.candidate.sha256,
              waitIndex: 1,
              intentSha256: ctx.intent.sha256,
            });
            throw new PipelineV2StateError("simulated lost race");
          }
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          snapshotReads += 1;
          // the first read is the pre-dispatch capture, the second the
          // post-dispatch classification; any third read would be a
          // re-read of an unverified snapshot and must never happen
          if (snapshotReads > 2) {
            throw new Error("the controller must not read the snapshot again after the classification");
          }
          return ctx.sink.snapshot;
        },
      };
      const result = await acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: raceSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate });
      expect(result).toMatchObject({ task_revision: 2, task_sha256: ctx.candidate.sha256 });
      // exactly the two reads: the capture and the classified post-dispatch snapshot
      expect(snapshotReads).toBe(2);
      // zero further task dispatch: the racing progression is adopted as-is
      expect(taskDispatches).toBe(0);
      expect(ctx.sink.snapshot?.task_revisions).toHaveLength(3);
      expect(ctx.sink.snapshot?.waits[0]?.intent?.intent_sha256).toBe(ctx.intent.sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("53. a foreign wait record inserted before the target wait is rejected as invalid state", async () => {
    const ctx = await reviseReady();
    try {
      let taskDispatches = 0;
      const mutationSink: PipelineV2ReviseTaskIntentControllerSink = {
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          if (command.kind === "task_revision_accepted") {
            taskDispatches += 1;
          }
          return await ctx.sink.dispatch(command);
        },
        get snapshot() {
          const real = ctx.sink.snapshot as PipelineV2RunState;
          if (real.waits[0]?.intent === undefined) {
            return real;
          }
          const derived = structuredClone(real) as PipelineV2RunState;
          const waitRecord = derived.waits[0];
          if (waitRecord !== undefined) {
            // the target wait (index 1) stays the last element, but the
            // journal length grows: the length pin rejects the insertion
            (derived.waits as unknown as PipelineV2RunState["waits"]).splice(0, 0, { ...waitRecord, index: 0 });
          }
          return derived;
        },
      };
      const cause = await catchAccept(() =>
        acceptPipelineV2ReviseTaskIntent({ runRoot: ctx.fixture.runRoot, sink: mutationSink, intent: ctx.intent, candidateTaskRevision: ctx.candidate }),
      );
      const error = expectReviseError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the accepted revise intent");
      expect(taskDispatches).toBe(0);
      expect(ctx.sink.snapshot?.waits).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
