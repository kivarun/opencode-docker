import { describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2WaitRecord,
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
  type AppliedPipelineV2ReviseTaskClosure,
} from "../src/pipeline_v2_revise_task_closure_controller.ts";
import {
  recordPipelineV2WaitAction,
  PipelineV2WaitControllerError,
  type RecordedPipelineV2WaitResponse,
} from "../src/pipeline_v2_wait_controller.ts";
import {
  completePipelineV2ReviseTask,
  PipelineV2ReviseTaskCompletionControllerError,
  type PipelineV2ReviseTaskCompletionControllerFailureReason,
  type PipelineV2ReviseTaskCompletionControllerSink,
} from "../src/pipeline_v2_revise_task_completion_controller.ts";
import {
  completePipelineV2ReviseTaskWithIo,
  productionReviseTaskCompletionOps,
  type PipelineV2ReviseTaskCompletionOps,
} from "../src/pipeline_v2_revise_task_completion_controller_internal.ts";
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-completion-"));
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
  statePath: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-revise-completion-run-"));
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

const INTENT = prepareWaitIntent({
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: RUN_ID,
  wait_index: 1,
  task_id: "task-a",
  expected_previous_task_sha256: A1.sha256,
  new_task_revision_sha256: A2.sha256,
});

interface ReviseCompletionCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
  candidate: PreparedPipelineV2RunTaskRevision;
  request: ReturnType<typeof preparePipelineV2WaitRequest>;
}

interface ReviseReadyOptions {
  secondIteration?: boolean;
}

/**
 * A real sink driven through the real reducer and the existing controllers
 * to the revise completion boundary: the revise intent accepted durably
 * inside the open wait with its accepted task revision in the ledger and
 * the published canonical request manifest.
 */
async function reviseReady(options: ReviseReadyOptions = {}): Promise<ReviseCompletionCtx> {
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
    const planCandidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [A1],
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
    if (options.secondIteration === true) {
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
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
        executionIndex: 2,
      });
    }
    const waitTransitionCount = options.secondIteration === true ? 4 : 2;
    const actions = [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
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
    await publishPipelineV2WaitRequest(fixture.runRoot, request.manifest);
    await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT.sha256 });
    await sink.dispatch({
      kind: "task_revision_accepted",
      taskId: "task-a",
      revision: 2,
      taskSha256: A2.sha256,
      waitIndex: 1,
      intentSha256: INTENT.sha256,
    });
    return { fixture, sink, pipeline, intent: INTENT, candidate: A2, request };
  });
}

interface RecordingSink extends PipelineV2ReviseTaskCompletionControllerSink {
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

interface CompletionOpsOverrides {
  mutateClosureResult?: (result: AppliedPipelineV2ReviseTaskClosure) => AppliedPipelineV2ReviseTaskClosure;
  mutateClosureState?: (derived: PipelineV2RunState) => PipelineV2RunState;
  mutateResponseResult?: (response: RecordedPipelineV2WaitResponse) => RecordedPipelineV2WaitResponse;
}

/**
 * The per-call hostile ops wrapper over the real production operations:
 * the real closure and the real wait action run against the real sink,
 * and only their returned results are replaced by hostile clones.
 */
function completionOps(overrides: CompletionOpsOverrides = {}): {
  ops: PipelineV2ReviseTaskCompletionOps;
  responseCalls: () => number;
} {
  const counts = { responses: 0 };
  const closureResultMutator = overrides.mutateClosureResult;
  const closureStateMutator = overrides.mutateClosureState;
  const responseMutator = overrides.mutateResponseResult;
  const applyClosure = closureResultMutator === undefined && closureStateMutator === undefined
    ? productionReviseTaskCompletionOps.applyClosure
    : async (options: Parameters<typeof applyPipelineV2ReviseTaskClosure>[0]) => {
        let result = await productionReviseTaskCompletionOps.applyClosure(options);
        if (closureResultMutator !== undefined) {
          result = closureResultMutator({ ...result });
        }
        if (closureStateMutator !== undefined) {
          result = {
            ...result,
            state: closureStateMutator(structuredClone(result.state) as PipelineV2RunState),
          };
        }
        return result;
      };
  const recordWaitAction = responseMutator === undefined
    ? async (options: Parameters<typeof recordPipelineV2WaitAction>[0]) => {
        counts.responses += 1;
        return await recordPipelineV2WaitAction(options);
      }
    : async (options: Parameters<typeof recordPipelineV2WaitAction>[0]) => {
        counts.responses += 1;
        const result = await recordPipelineV2WaitAction(options);
        return responseMutator(structuredClone(result) as RecordedPipelineV2WaitResponse);
      };
  return { ops: { applyClosure, recordWaitAction }, responseCalls: () => counts.responses };
}

async function catchAccept(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectCompletionError(
  cause: unknown,
  reason: PipelineV2ReviseTaskCompletionControllerFailureReason,
): PipelineV2ReviseTaskCompletionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ReviseTaskCompletionControllerError);
  const error = cause as PipelineV2ReviseTaskCompletionControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

const waitResponsePath = (runRoot: string): string => join(runRoot, "waits", "1.response.json");
const waitRequestPath = (runRoot: string): string => join(runRoot, "waits", "1.request.json");

function durableResponseSha(state: PipelineV2RunState): string {
  const response = state.waits[0]!.response as { response_sha256: string };
  return response.response_sha256;
}

describe("completePipelineV2ReviseTask", () => {
  test("1. C0 happy path: exact command order, revision +2, loader round-trip", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      const responseSha = durableResponseSha(state);
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "replanned", waitIndex: 1 },
        {
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: ctx.request.sha256,
          actionId: "revise_task",
          responseSha256: responseSha,
        },
      ]);
      expect(result).toEqual({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        task_id: "task-a",
        task_revision: 2,
        task_sha256: ctx.candidate.sha256,
        intent_sha256: ctx.intent.sha256,
        request_sha256: ctx.request.sha256,
        response_sha256: responseSha,
        action_id: "revise_task",
        action_to: "architect",
        state,
      });
      expect(state.status).toBe("active");
      expect(state.phase).toBe("running");
      expect(state.cursor.current_state).toBe("architect");
      const round = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      expect(round.status).toBe("active");
      expect(round.waits[0]!.response?.action_id).toBe("revise_task");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the unified result carries exactly the content-free fields and is deep-frozen", async () => {
    const ctx = await reviseReady();
    try {
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
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
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. C1 retry: the durable closure dispatches the response only", async () => {
    const ctx = await reviseReady();
    try {
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual(["wait_response_recorded"]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1, task_revision: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. C2 orphan response retry: the exact file is adopted and committed once", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const firstCause = await catchAccept(() =>
        completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(firstCause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((firstCause as PipelineV2WaitControllerError).reason).toBe("state_persist_failed");
      // the closure stayed durable and the response file is the orphan
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 1);
      const orphanBytes = await readFile(waitResponsePath(ctx.fixture.runRoot));
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual(["wait_response_recorded"]);
      const state = fresh.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      expect(result.response_sha256).toBe(durableResponseSha(state));
      // the orphan was adopted byte-for-byte
      expect(await readFile(waitResponsePath(ctx.fixture.runRoot))).toEqual(orphanBytes);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. C3 durability-unknown response retry: a fresh retry dispatches nothing", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((cause as PipelineV2WaitControllerError).reason).toBe("state_persist_failed");
      expect(faulted.poisoned).toBe(true);
      // the rename succeeded: the durable file carries the response
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      const state = fresh.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      expect(state.status).toBe("active");
      expect(result.response_sha256).toBe(durableResponseSha(state));
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. C4 committed response retry: zero dispatch through both recognitions", async () => {
    const ctx = await reviseReady();
    try {
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const first = await recordPipelineV2WaitAction({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "revise_task" });
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(result.response_sha256).toBe(first.response_sha256);
      expect(result.request_sha256).toBe(first.request_sha256);
      expect(result.state.revision).toBe(revisionBefore);
      expect(await readFile(waitRequestPath(ctx.fixture.runRoot))).toEqual(await readFile(waitRequestPath(ctx.fixture.runRoot)));
      expect((ctx.sink.snapshot as PipelineV2RunState).status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. a closure not_committed stops the completion before the response; the fresh retry completes both", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { ops, responseCalls } = completionOps();
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
      expect((cause as PipelineV2ReviseTaskClosureControllerError).reason).toBe("state_persist_failed");
      expect((cause as PipelineV2ReviseTaskClosureControllerError).message).toContain("could not be committed");
      expect(responseCalls()).toBe(0);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore);
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual(["stage_iteration_closed", "wait_response_recorded"]);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
      expect(result.task_revision).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. a closure durability_unknown stops the completion; the fresh reopened sink records the response only", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { ops, responseCalls } = completionOps();
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
      expect((cause as PipelineV2ReviseTaskClosureControllerError).reason).toBe("state_persist_failed");
      expect((cause as PipelineV2ReviseTaskClosureControllerError).message).toContain("could not be confirmed durable");
      expect(faulted.poisoned).toBe(true);
      expect(responseCalls()).toBe(0);
      // the rename succeeded: the durable file carries the closure
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual(["wait_response_recorded"]);
      const state = fresh.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      expect(state.generations[0]!.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
      expect(result.task_revision).toBe(2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. two identical C0 completions racing: both succeed, one durable closure, one response, revision +2", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const [first, second] = await Promise.all([
        completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }).catch((cause: unknown) => cause),
        completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }).catch((cause: unknown) => cause),
      ]);
      expect((first as { task_revision?: number }).task_revision).toBe(2);
      expect((second as { task_revision?: number }).task_revision).toBe(2);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      expect(state.status).toBe("active");
      expect(state.waits[0]!.response?.action_id).toBe("revise_task");
      expect(state.generations[0]!.iterations[0]!.closed).toEqual({
        by: "replanned",
        wait_index: 1,
        closed_transition_count: 2,
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a conflicting retry with another intent rewrites nothing", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const first = await completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      expect(first.task_revision).toBe(2);
      const otherIntent = prepareWaitIntent({
        schema_version: 1,
        kind: "revise_task_intent",
        run_id: RUN_ID,
        wait_index: 1,
        task_id: "task-a",
        expected_previous_task_sha256: A1.sha256,
        new_task_revision_sha256: A3.sha256,
      });
      const { ops, responseCalls } = completionOps();
      const recording = recordingSink(ctx.sink);
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: recording, intent: otherIntent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
      expect((cause as PipelineV2ReviseTaskClosureControllerError).reason).toBe("invalid_state");
      expect((cause as PipelineV2ReviseTaskClosureControllerError).message).toContain("accepted a different intent");
      expect(recording.commands).toEqual([]);
      expect(responseCalls()).toBe(0);
      const state = ctx.sink.snapshot as PipelineV2RunState;
      expect(state.revision).toBe(revisionBefore + 2);
      expect(durableResponseSha(state)).toBe(first.response_sha256);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. a non-registered intent surfaces the closure controller's typed error", async () => {
    const ctx = await reviseReady();
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
      const { ops, responseCalls } = completionOps();
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, {
          runRoot: ctx.fixture.runRoot,
          sink: ctx.sink,
          intent: handBuilt as unknown as PreparedPipelineV2RunWaitIntent,
        }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ReviseTaskClosureControllerError);
      expect((cause as PipelineV2ReviseTaskClosureControllerError).reason).toBe("invalid_intent");
      expect(responseCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. a tampered request file surfaces the wait controller's typed error without rollback", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      await writeFile(waitRequestPath(ctx.fixture.runRoot), '{"tampered":true}\n');
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect((cause as PipelineV2WaitControllerError).reason).toBe("wait_conflict");
      const state = ctx.sink.snapshot as PipelineV2RunState;
      // the closure is not rolled back
      expect(state.revision).toBe(revisionBefore + 1);
      expect(state.generations[0]!.iterations[0]!.closed?.by).toBe("replanned");
      expect(state.status).toBe("waiting");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. a hostile closure result with a missing or different closure is invalid_result before any response work", async () => {
    const last = (generation: PipelineV2RunState["generations"][number]) =>
      generation.iterations[generation.iterations.length - 1]!;
    for (const mutate of [
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: generation.iterations.map((entry, position) =>
            position === generation.iterations.length - 1 ? { ...entry, closed: undefined } : entry,
          ),
        };
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0]!;
        const target = last(generation);
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: [
            ...generation.iterations.slice(0, -1),
            { ...target, closed: { ...target.closed!, by: "normal_close" } },
          ],
        };
        return derived;
      },
    ]) {
      const ctx = await reviseReady();
      try {
        const revisionAfterClosure = (ctx.sink.snapshot as PipelineV2RunState).revision;
        const { ops, responseCalls } = completionOps({ mutateClosureState: mutate });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect(error.message).toContain("does not carry the exact replanned closure");
        expect(responseCalls()).toBe(0);
        // the real closure of the hostile ops run stayed durable
        const state = ctx.sink.snapshot as PipelineV2RunState;
        expect(state.revision).toBe(revisionAfterClosure + 1);
        expect(state.generations[0]!.iterations[0]!.closed).toEqual({
          by: "replanned",
          wait_index: 1,
          closed_transition_count: 2,
        });
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("14. a hostile closure result with a mutated generation index is invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      const revisionAfterClosure = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { ops, responseCalls } = completionOps({
        mutateClosureState: (derived): PipelineV2RunState => {
          const generation = derived.generations[0]!;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, index: 2 };
          return derived;
        },
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("does not carry the last open target generation");
      expect(responseCalls()).toBe(0);
      expect(((ctx.sink.snapshot as PipelineV2RunState).generations[0]!).index).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a hostile closure result with a changed plan or stage binding is invalid_result", async () => {
    for (const mutate of [
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, plan_sha256: hex("7") };
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const record = derived.plan_revisions[0]!;
        (derived.plan_revisions as unknown as PipelineV2RunState["plan_revisions"])[0] = { ...record, sha256: hex("8") };
        return derived;
      },
      (derived: PipelineV2RunState): PipelineV2RunState => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, stage_id: "stage-9" };
        return derived;
      },
    ]) {
      const ctx = await reviseReady();
      try {
        const { ops, responseCalls } = completionOps({ mutateClosureState: mutate });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect(
          error.message.includes("does not belong to the last durable plan revision") ||
            error.message.includes("does not match the durable generation bindings"),
        ).toBe(true);
        expect(responseCalls()).toBe(0);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("16. a hostile closure result with a changed historical iteration prefix is invalid_result", async () => {
    const ctx = await reviseReady({ secondIteration: true });
    try {
      const { ops, responseCalls } = completionOps({
        mutateClosureState: (derived): PipelineV2RunState => {
          const generation = derived.generations[0]!;
          const first = generation.iterations[0]!;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...generation,
            iterations: [{ ...first, opened_transition_count: 99 }, ...generation.iterations.slice(1)],
          };
          return derived;
        },
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("does not match the durable iteration history");
      expect(responseCalls()).toBe(0);
      expect(((ctx.sink.snapshot as PipelineV2RunState).generations[0]!.iterations[0]!).opened_transition_count).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. a hostile closure result with a replaced wait intent is invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      const { ops, responseCalls } = completionOps({
        mutateClosureState: (derived): PipelineV2RunState => {
          const waitRecord = derived.waits[0]!;
          (derived.waits as unknown as PipelineV2RunState["waits"])[0] = {
            ...waitRecord,
            intent: { ...waitRecord.intent!, intent_sha256: hex("9") },
          };
          return derived;
        },
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("does not carry the exact accepted intent");
      expect(responseCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. a hostile closure result with an extra wait-bound task record of another task is invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      const { ops, responseCalls } = completionOps({
        mutateClosureState: (derived): PipelineV2RunState => {
          (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"]).push({
            index: 4,
            task_id: "task-b",
            revision: 2,
            sha256: B2.sha256,
            previous_sha256: B1.sha256,
            wait_index: 1,
            intent_sha256: INTENT.sha256,
          });
          return derived;
        },
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("exactly one accepted task revision");
      expect(responseCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a hostile closure result with a mismatching task revision field is invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      const { ops, responseCalls } = completionOps({
        mutateClosureResult: (result) => ({ ...result, task_revision: 3 }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("contradicts the revise intent");
      expect(responseCalls()).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a malformed closure result state is typed invalid_result, never a TypeError", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["task ledger null entry", (derived) => {
        (derived.task_revisions as unknown as unknown[])[1] = null;
      }],
      ["task ledger primitive entry", (derived) => {
        (derived.task_revisions as unknown as unknown[])[0] = 7;
      }],
      ["generation null", (derived) => {
        (derived.generations as unknown as unknown[])[0] = null;
      }],
      ["generation iterations null", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: null as unknown as PipelineV2RunState["generations"][number]["iterations"],
        };
      }],
      ["target iteration null", (derived) => {
        const generation = derived.generations[0]!;
        (generation.iterations as unknown as unknown[])[0] = null;
      }],
      ["wait record null", (derived) => {
        (derived.waits as unknown as unknown[])[0] = null;
      }],
      ["cursor null", (derived) => {
        (derived as unknown as Record<string, unknown>)["cursor"] = null;
      }],
      ["plan ledger null", (derived) => {
        (derived as unknown as Record<string, unknown>)["plan_revisions"] = null;
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        const { ops, responseCalls } = completionOps({
          mutateClosureState: (derived) => {
            mutate(derived);
            return derived;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect((cause as Error).name).toBe("PipelineV2ReviseTaskCompletionControllerError");
        expect(error.message).not.toContain("null");
        expect(error.message).not.toContain(label);
        expect(responseCalls()).toBe(0);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("21. hostile response results with a changed request digest, action target or wait anchor are invalid_result", async () => {
    const variants: Array<[string, (response: RecordedPipelineV2WaitResponse) => void]> = [
      ["request digest", (response) => {
        (response as { request_sha256: string }).request_sha256 = hex("3");
      }],
      ["action target", (response) => {
        (response as { action_to: string }).action_to = "dev_entry";
        (response.state.cursor as { current_state: string }).current_state = "dev_entry";
      }],
      ["wait anchor", (response) => {
        (response.state.waits[0] as { transition_count: number }).transition_count = 99;
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({ mutateResponseResult: (response) => {
          mutate(response);
          return response;
        } });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        expectCompletionError(cause, "invalid_result");
        expect(responseCalls()).toBe(1);
        const state = ctx.sink.snapshot as PipelineV2RunState;
        expect(state.revision).toBe(revisionBefore + 2);
        expect(state.waits[0]!.response?.action_id).toBe("revise_task");
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("22. a hostile response result with a wrong revision delta is invalid_result", async () => {
    const ctx = await reviseReady();
    try {
      const revisionBefore = (ctx.sink.snapshot as PipelineV2RunState).revision;
      await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
      const revisionAfterClosure = (ctx.sink.snapshot as PipelineV2RunState).revision;
      const { ops, responseCalls } = completionOps({
        mutateResponseResult: (response) => {
          (response.state as { revision: number }).revision = revisionAfterClosure + 2;
          return response;
        },
      });
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expectCompletionError(cause, "invalid_result");
      expect(responseCalls()).toBe(1);
      expect((ctx.sink.snapshot as PipelineV2RunState).revision).toBe(revisionBefore + 2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. hostile response results with changed early task, plan or wait records are invalid_result", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["task ledger", (derived) => {
        const record = derived.task_revisions[0]!;
        (derived.task_revisions as unknown as PipelineV2RunState["task_revisions"])[0] = { ...record, sha256: hex("6") };
      }],
      ["plan ledger", (derived) => {
        const record = derived.plan_revisions[0]!;
        (derived.plan_revisions as unknown as PipelineV2RunState["plan_revisions"])[0] = { ...record, sha256: hex("7") };
      }],
      ["wait record", (derived) => {
        const waitRecord = derived.waits[0]!;
        (derived.waits as unknown as PipelineV2RunState["waits"])[0] = { ...waitRecord, reason: "other_reason" };
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({
          mutateResponseResult: (response) => {
            mutate(response.state as PipelineV2RunState);
            return response;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        expectCompletionError(cause, "invalid_result");
        expect(responseCalls()).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("24. hostile response results with a changed generation index, binding or historical iteration are invalid_result", async () => {
    const variants: Array<[string, boolean, (derived: PipelineV2RunState) => void]> = [
      ["generation index", false, (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, index: 2 };
      }],
      ["generation binding", false, (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, stage_id: "stage-9" };
      }],
      ["historical iteration", true, (derived) => {
        const generation = derived.generations[0]!;
        const first = generation.iterations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: [{ ...first, opened_transition_count: 99 }, ...generation.iterations.slice(1)],
        };
      }],
    ];
    for (const [label, secondIteration, mutate] of variants) {
      const ctx = await reviseReady({ secondIteration });
      try {
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({
          mutateResponseResult: (response) => {
            mutate(response.state as PipelineV2RunState);
            return response;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        expectCompletionError(cause, "invalid_result");
        expect(responseCalls()).toBe(1);
        const generation = (ctx.sink.snapshot as PipelineV2RunState).generations[0]!;
        expect(generation.stage_id).toBe("stage-1");
        expect(generation.index).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("25. hostile response results with a removed, substituted closure or an appeared open_iteration are invalid_result", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["removed closure", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: generation.iterations.map((entry, position) =>
            position === generation.iterations.length - 1 ? { ...entry, closed: undefined } : entry,
          ),
        };
      }],
      ["substituted closure", (derived) => {
        const generation = derived.generations[0]!;
        const target = generation.iterations[generation.iterations.length - 1]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: [
            ...generation.iterations.slice(0, -1),
            { ...target, closed: { ...target.closed!, by: "normal_close" } },
          ],
        };
      }],
      ["open iteration", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          open_iteration: { index: 1, opened_transition_count: 2 },
        };
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({
          mutateResponseResult: (response) => {
            mutate(response.state as PipelineV2RunState);
            return response;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        expectCompletionError(cause, "invalid_result");
        expect(responseCalls()).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("26. hostile response results with a shifted cursor or journals are invalid_result", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["shifted cursor state", (derived) => {
        (derived.cursor as { current_state: string }).current_state = "dev_entry";
      }],
      ["shifted cursor count", (derived) => {
        (derived.cursor as { transition_count: number }).transition_count = 3;
      }],
      ["shifted transition journal", (derived) => {
        (derived.transitions as unknown as unknown[]).push({
          from: "dev_entry",
          outcome: "completed",
          to: "architect",
          transition_index: 0,
          execution_index: 3,
        });
      }],
      ["shifted execution journal", (derived) => {
        (derived.executions as unknown as unknown[]).pop();
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({
          mutateResponseResult: (response) => {
            mutate(response.state as PipelineV2RunState);
            return response;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        expectCompletionError(cause, "invalid_result");
        expect(responseCalls()).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("27. a malformed response result is typed invalid_result, never a TypeError", async () => {
    const variants: Array<[string, (derived: PipelineV2RunState) => void]> = [
      ["task ledger null entry", (derived) => {
        (derived.task_revisions as unknown as unknown[])[1] = null;
      }],
      ["wait record null", (derived) => {
        (derived.waits as unknown as unknown[])[0] = null;
      }],
      ["generation null", (derived) => {
        (derived.generations as unknown as unknown[])[0] = null;
      }],
      ["generation iterations null", (derived) => {
        const generation = derived.generations[0]!;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...generation,
          iterations: null as unknown as PipelineV2RunState["generations"][number]["iterations"],
        };
      }],
      ["cursor null", (derived) => {
        (derived as unknown as Record<string, unknown>)["cursor"] = null;
      }],
    ];
    for (const [label, mutate] of variants) {
      const ctx = await reviseReady();
      try {
        await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
        const { ops, responseCalls } = completionOps({
          mutateResponseResult: (response) => {
            mutate(response.state as PipelineV2RunState);
            return response;
          },
        });
        const cause = await catchAccept(() =>
          completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect((cause as Error).name).toBe("PipelineV2ReviseTaskCompletionControllerError");
        expect(error.message).not.toContain("null");
        expect(error.message).not.toContain(label);
        expect(responseCalls()).toBe(1);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

  test("28. options and ops getters are read exactly once", async () => {
    const ctx = await reviseReady();
    try {
      const memberReads: Record<string, number> = {};
      const opsGets: Record<string, number> = {};
      const proxiedOps = new Proxy(productionReviseTaskCompletionOps, {
        get(target, prop: string) {
          opsGets[prop] = (opsGets[prop] ?? 0) + 1;
          return target[prop as keyof PipelineV2ReviseTaskCompletionOps];
        },
      });
      const innerSink = ctx.sink;
      const proxiedOptions = new Proxy(
        {} as { runRoot: string; sink: PipelineV2ReviseTaskCompletionControllerSink; intent: PreparedPipelineV2RunWaitIntent },
        {
          get(_target, prop: string) {
            memberReads[prop] = (memberReads[prop] ?? 0) + 1;
            if (prop === "sink") {
              return innerSink;
            }
            if (prop === "runRoot") {
              return ctx.fixture.runRoot;
            }
            return ctx.intent;
          },
        },
      );
      const result = await completePipelineV2ReviseTaskWithIo(proxiedOps, proxiedOptions);
      expect(result).toMatchObject({ task_revision: 2 });
      expect(memberReads["runRoot"]).toBe(1);
      expect(memberReads["sink"]).toBe(1);
      expect(memberReads["intent"]).toBe(1);
      expect(opsGets["applyClosure"]).toBe(1);
      expect(opsGets["recordWaitAction"]).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. caller mutation after the capture cannot influence the completion", async () => {
    const ctx = await reviseReady();
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
        runRoot: ctx.fixture.runRoot,
        sink: ctx.sink as PipelineV2ReviseTaskCompletionControllerSink,
        get intent(): PreparedPipelineV2RunWaitIntent {
          intentReads += 1;
          return intentReads === 1 ? ctx.intent : otherIntent;
        },
      };
      const result = await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, options);
      expect(result).toMatchObject({ task_revision: 2, intent_sha256: ctx.intent.sha256 });
      expect(intentReads).toBe(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. downstream unexpected errors preserve their identity", async () => {
    const ctx = await reviseReady();
    try {
      const fault = new Error("boom-identity");
      const hostileOps: PipelineV2ReviseTaskCompletionOps = {
        applyClosure: productionReviseTaskCompletionOps.applyClosure,
        recordWaitAction: async () => {
          throw fault;
        },
      };
      const cause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(hostileOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBe(fault);
      const optionsFault = new Error("boom-options");
      const hostileOptions = new Proxy(
        {} as { runRoot: string; sink: PipelineV2ReviseTaskCompletionControllerSink; intent: PreparedPipelineV2RunWaitIntent },
        {
          get() {
            throw optionsFault;
          },
        },
      );
      const optionsCause = await catchAccept(() =>
        completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, hostileOptions),
      );
      expect(optionsCause).toBe(optionsFault);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. hostile option shapes are invalid_options", async () => {
    const runRoot = "/tmp/unused-run-root";
    await expect((async () => {
      await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, null);
    })()).rejects.toMatchObject({ reason: "invalid_options" });
    await expect((async () => {
      await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, { runRoot, sink: "x", intent: INTENT });
    })()).rejects.toMatchObject({ reason: "invalid_options" });
    await expect((async () => {
      await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, { runRoot: 5, sink: {}, intent: INTENT });
    })()).rejects.toMatchObject({ reason: "invalid_options" });
    await expect((async () => {
      await completePipelineV2ReviseTaskWithIo(productionReviseTaskCompletionOps, { runRoot, sink: {}, intent: 7 });
    })()).rejects.toMatchObject({ reason: "invalid_options" });
    await expect((async () => {
      await completePipelineV2ReviseTaskWithIo(
        { applyClosure: "x", recordWaitAction: "y" } as unknown as PipelineV2ReviseTaskCompletionOps,
        { runRoot, sink: {}, intent: INTENT },
      );
    })()).rejects.toMatchObject({ reason: "invalid_options" });
  });

  test("31b. diagnostics are content-free across the failure paths", async () => {
    const messages: string[] = [];
    const contexts: Array<() => Promise<unknown>> = [
      async () => {
        const ctx = await reviseReady();
        try {
          const { ops } = completionOps({
            mutateClosureState: (derived) => {
              const generation = derived.generations[0]!;
              (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...generation, index: 2 };
              return derived;
            },
          });
          return await catchAccept(() =>
            completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
          );
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
      async () => {
        const ctx = await reviseReady();
        try {
          await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
          const { ops } = completionOps({
            mutateResponseResult: (response) => {
              (response as { request_sha256: string }).request_sha256 = hex("3");
              return response;
            },
          });
          return await catchAccept(() =>
            completePipelineV2ReviseTaskWithIo(ops, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
          );
        } finally {
          await disposeRun(ctx.fixture);
        }
      },
      async () => {
        const ctx = await reviseReady();
        try {
          await applyPipelineV2ReviseTaskClosure({ sink: ctx.sink, intent: ctx.intent });
          await writeFile(waitRequestPath(ctx.fixture.runRoot), '{"tampered":true}\n');
          return await catchAccept(() =>
            completePipelineV2ReviseTask({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
          );
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
      expect(message).not.toContain("waits/1");
      expect(message).not.toContain("state.json");
      expect(message).not.toContain("Bearer");
      expect(message).not.toContain("credential");
    }
  });
});

test("32. the runtime export surfaces are exact (public two keys, internal three keys)", async () => {
  const publicModule = await import("../src/pipeline_v2_revise_task_completion_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ReviseTaskCompletionControllerError",
    "completePipelineV2ReviseTask",
  ]);
  const internalModule = await import("../src/pipeline_v2_revise_task_completion_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ReviseTaskCompletionControllerError",
    "completePipelineV2ReviseTaskWithIo",
    "productionReviseTaskCompletionOps",
  ]);
});

test("33. the completion composes the existing layers only (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_revise_task_completion_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("applyPipelineV2ReviseTaskClosure")).toBe(5);
  expect(countOf("recordPipelineV2WaitAction")).toBe(4);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(0);
  expect(countOf("validatePipelineV2RunState(")).toBe(0);
  expect(countOf("prepareWaitIntent(")).toBe(0);
  expect(countOf("parseWaitIntent(")).toBe(0);
  expect(countOf("preparePipelineV2WaitRequest(")).toBe(0);
  expect(countOf("acceptPipelineV2WaitResponse(")).toBe(0);
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
  expect(countOf("acceptPipelineV2RunPlanCandidate(")).toBe(0);
  expect(countOf("publishPipelineV2")).toBe(0);
  expect(countOf("loadPipelineV2PlanRevision")).toBe(0);
  expect(countOf("loadPipelineV2TaskRevision")).toBe(0);
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
    "pipeline_v2_revise_task_intent_controller",
    "pipeline_v2_continue_stage",
    "pipeline_state_store",
  ]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("let production")).toBe(0);
});
