import { describe, expect, test } from "bun:test";
import { chmod, lstat, mkdtemp, readFile, rm, unlink, writeFile } from "node:fs/promises";
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
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { acceptPipelineV2ContinueStageIntent } from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import {
  applyPipelineV2ContinueStageGrant,
  PipelineV2ContinueStageGrantControllerError,
} from "../src/pipeline_v2_continue_stage_grant_controller.ts";
import {
  recordPipelineV2WaitAction,
  PipelineV2WaitControllerError,
} from "../src/pipeline_v2_wait_controller.ts";
import {
  completePipelineV2ContinueStage,
  PipelineV2ContinueStageCompletionControllerError,
  type PipelineV2ContinueStageCompletionControllerFailureReason,
  type PipelineV2ContinueStageCompletionControllerSink,
} from "../src/pipeline_v2_continue_stage_completion_controller.ts";
import {
  completePipelineV2ContinueStageWithIo,
  productionContinueStageCompletionOps,
  type PipelineV2ContinueStageCompletionOps,
} from "../src/pipeline_v2_continue_stage_completion_controller_internal.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { faultIo } from "./state_io_test_helpers.ts";
import type { RecordedPipelineV2WaitResponse } from "../src/pipeline_v2_wait_controller.ts";
import type { PipelineV2WaitRecord } from "../src/pipeline_v2_state.ts";

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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-completion-"));
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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-completion-run-"));
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

interface CompletionCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
}

interface ReadyOptions {
  acceptIntent?: boolean;
  recordGrant?: { additionalIterations: number };
  closeGrantIteration?: boolean;
  recordResponse?: boolean;
  extraWaitActions?: boolean;
  continueStageTo?: string;
}

/**
 * A real sink driven through the real reducer and the existing controllers
 * to the requested completion boundary.
 */
async function completionReady(options: ReadyOptions = {}): Promise<CompletionCtx> {
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
        { id: "continue_stage", to: options.continueStageTo ?? "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await sink.dispatch({
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions: [
        { id: "continue_stage", to: options.continueStageTo ?? "dev_entry" },
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
    if (options.acceptIntent !== false) {
      await acceptPipelineV2ContinueStageIntent({ runRoot: fixture.runRoot, sink, intent });
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
      await recordPipelineV2WaitAction({ runRoot: fixture.runRoot, sink, waitIndex: 1, actionId: "continue_stage" });
    }
    return { fixture, sink, pipeline, intent };
  });
}

function preparedIntent(ctx: CompletionCtx, overrides: Record<string, unknown> = {}): PreparedPipelineV2RunWaitIntent {
  const base = ctx.intent.manifest as unknown as Record<string, unknown>;
  return prepareWaitIntent({ ...base, ...overrides });
}

interface RecordingSink extends PipelineV2ContinueStageCompletionControllerSink {
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

function expectCompletionError(cause: unknown, reason: PipelineV2ContinueStageCompletionControllerFailureReason): PipelineV2ContinueStageCompletionControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinueStageCompletionControllerError);
  const error = cause as PipelineV2ContinueStageCompletionControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

const RESPONSE_REQUEST_PATH = (runRoot: string): string => join(runRoot, "waits", "1.request.json");
const RESPONSE_RESPONSE_PATH = (runRoot: string): string => join(runRoot, "waits", "1.response.json");

describe("completePipelineV2ContinueStage", () => {
  test("1. C0 happy path: grant, closure and durable response through the real controllers", async () => {
    const ctx = await completionReady();
    try {
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "iteration_grant_recorded",
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect(result).toMatchObject({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        additional_iterations: 2,
        intent_sha256: ctx.intent.sha256,
        action_id: "continue_stage",
        action_to: "dev_entry",
      });
      expect(result.state.status).toBe("active");
      expect(result.state.phase).toBe("running");
      expect(result.state.cursor.current_state).toBe("dev_entry");
      const wait = result.state.waits[0];
      expect(wait?.intent).toEqual({ intent_sha256: ctx.intent.sha256 });
      expect(wait?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the unified result carries exactly the content-free fields and is deep-frozen", async () => {
    const ctx = await completionReady();
    try {
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      expect(Object.keys(result).sort()).toEqual([
        "action_id",
        "action_to",
        "additional_iterations",
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
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the completed state round-trips through the loader", async () => {
    const ctx = await completionReady();
    try {
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      validatePipelineV2RunState(JSON.parse(JSON.stringify(result.state)) as never);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.status).toBe("active");
      expect(persisted.waits[0]?.response?.action_id).toBe("continue_stage");
      expect(persisted.cursor.current_state).toBe("dev_entry");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the response routing target comes from the declared wait action", async () => {
    const ctx = await completionReady();
    try {
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const wait = result.state.waits[0];
      const declared = wait?.actions.find((action) => action.id === "continue_stage");
      const declaredTo = declared?.to;
      if (typeof declaredTo !== "string") {
        throw new Error("fixture action missing");
      }
      expect(result.action_to).toBe(declaredTo);
      expect(result.action_to).toBe("dev_entry");
      expect(result.state.cursor.current_state).toBe(result.action_to);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. C1 partial retry: the durable grant dispatches closure and response only", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect(result.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. C2 partial retry: grant and closure durable, the response only", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const prior = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      const revisionBefore = prior.revision;
      const recording = recordingSink(ctx.sink);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "wait_response_recorded",
      ]);
      expect(result.state.status).toBe("active");
      expect(result.state.revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. C3 orphan response retry: the exact response file is adopted and committed once", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      const orphan = await lstat(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      expect(orphan.mode & 0o777).toBe(0o600);
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: fresh, intent: ctx.intent });
      expect(result.state.status).toBe("active");
      expect(result.state.waits[0]?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. C4 committed response retry: zero dispatch through the answered S2 recognition", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      expect(ctx.sink.snapshot?.status).toBe("active");
      const revision = ctx.sink.snapshot?.revision;
      if (typeof revision !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1, action_id: "continue_stage" });
      expect(retry.state.revision).toBe(revision);
      expect(retry.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. a missing request file is restored through the existing controller", async () => {
    const ctx = await completionReady();
    try {
      await unlink(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot));
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      expect(result.state.status).toBe("active");
      const restored = await lstat(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot));
      expect(restored.mode & 0o777).toBe(0o600);
      expect((await readFile(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot), "utf8")).length).toBeGreaterThan(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a missing response file after a durable response is restored with zero dispatch", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      await unlink(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry.state.status).toBe("active");
      expect(await lstat(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot)).then((stat) => stat.mode & 0o777)).toBe(0o600);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. a conflicting response file is not overwritten", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const responsePath = RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot);
      await writeFile(responsePath, "{}", { mode: 0o600 });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect(await readFile(responsePath, "utf8")).toBe("{}");
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. a durable response with another action is not the completion boundary", async () => {
    // the wait is answered with revise_task; the grant controller's
    // answered recognition rejects the boundary before any response work
    const ctx = await completionReady();
    try {
      await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      await recordPipelineV2WaitAction({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "revise_task" });
      expect(ctx.sink.snapshot?.status).toBe("active");
      const afterWork = ctx.sink.snapshot?.revision;
      if (typeof afterWork !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      expect(ctx.sink.snapshot?.status).toBe("active");
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: fresh, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("lifecycle_conflict");
      expect((cause as Error).message).toContain("was answered with another action");
      expect(ctx.sink.snapshot?.revision).toBe(afterWork);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. an intent that was never accepted surfaces the typed grant error", async () => {
    const ctx = await completionReady({ acceptIntent: false });
    try {
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as Error).message).toContain("has not accepted an intent");
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. a grant amount mismatch surfaces the typed grant conflict", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 5 } });
    try {
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("grant_conflict");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. a foreign intent against the answered boundary surfaces the typed grant error", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      // the answered boundary recognizes only the exact accepted intent;
      // a foreign intent (a different digest, a different amount) is a
      // typed grant failure, never a retry
      const foreign = preparedIntent(ctx, { additional_iterations: 3 });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: foreign }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("invalid_state");
      expect((cause as Error).message).toContain("accepted a different intent");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("16. an active run without the exact completed boundary is rejected", async () => {
    // an active run whose last wait was never answered: the answered
    // recognition requires the recorded response
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      // force the run active by answering with the other action, then use
      // the continue intent — the answered recognition rejects the
      // boundary (the action mismatch)
      await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      await recordPipelineV2WaitAction({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "revise_task" });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as Error).message).toContain("was answered with another action");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. a later opened iteration is not treated as the same completion boundary", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. a later generation is not treated as the completed boundary retry", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      await ctx.sink.dispatch({ kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a later wait is not treated as the completed boundary retry", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true });
    try {
      // a second iteration and a second wait for the next boundary
      await ctx.sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
      await ctx.sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 2, by: "normal_close" });
      await ctx.sink.dispatch({
        kind: "run_waiting",
        stateId: "dev_entry",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("1"),
        actions: [{ id: "continue_stage", to: "dev_entry" }],
      });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("invalid_state");
      expect((cause as Error).message).toContain("but the open wait record is 2");
      expect(recording.commands).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a grant not-committed leaves the state unchanged and a fresh retry commits the full suffix", async () => {
    const ctx = await completionReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("state_persist_failed");
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: fresh, intent: ctx.intent });
      expect(result.state.status).toBe("active");
      expect(result.state.grants).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a grant durability-unknown adopts the grant and a fresh retry completes", async () => {
    const ctx = await completionReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect((cause as PipelineV2ContinueStageGrantControllerError).reason).toBe("state_persist_failed");
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect(result.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. a closure not-committed keeps the grant and a fresh retry completes", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect(ctx.sink.snapshot?.grants).toHaveLength(1);
      expect(ctx.sink.snapshot?.generations[0]?.open_iteration).toBeDefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "stage_iteration_closed",
        "wait_response_recorded",
      ]);
      expect(result.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("23. a closure durability-unknown adopts the closure and a fresh retry records the response", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands.map((command) => command.kind)).toEqual([
        "wait_response_recorded",
      ]);
      expect(result.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a response not-committed leaves the orphan and a fresh retry commits it once", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: fresh, intent: ctx.intent });
      expect(result.state.status).toBe("active");
      expect(result.state.waits[0]?.response?.action_id).toBe("continue_stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. a response durability-unknown is adopted and a fresh retry dispatches nothing", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: faulted, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
      expect(faulted.poisoned).toBe(true);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.waits[0]?.response?.action_id).toBe("continue_stage");
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry.state.status).toBe("active");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. the exact complete retry dispatches nothing and preserves the published files", async () => {
    const ctx = await completionReady();
    try {
      const result = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const requestStat = await lstat(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot));
      const responseStat = await lstat(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      const requestBytes = await readFile(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot));
      const responseBytes = await readFile(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      const requestAfter = await lstat(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot));
      const responseAfter = await lstat(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      expect(requestAfter.ino).toBe(requestStat.ino);
      expect(requestAfter.mode).toBe(requestStat.mode);
      expect(requestAfter.mtimeMs).toBe(requestStat.mtimeMs);
      expect(responseAfter.ino).toBe(responseStat.ino);
      expect(responseAfter.mtimeMs).toBe(responseStat.mtimeMs);
      expect(await readFile(RESPONSE_REQUEST_PATH(ctx.fixture.runRoot))).toEqual(requestBytes);
      expect(await readFile(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot))).toEqual(responseBytes);
      expect(retry.state.revision).toBe(result.state.revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. two identical C0 completions racing produce one grant, one closure, one response, +3 revisions", async () => {
    const ctx = await completionReady();
    try {
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const first = completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const second = completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const [one, two] = await Promise.all([first, second]);
      expect(one).toMatchObject({ wait_index: 1, action_id: "continue_stage" });
      expect(two).toMatchObject({ wait_index: 1, action_id: "continue_stage" });
      const state = ctx.sink.snapshot;
      expect(state?.grants).toHaveLength(1);
      expect(state?.generations[0]?.iterations[0]?.closed?.by).toBe("grant");
      expect(state?.waits[0]?.response?.action_id).toBe("continue_stage");
      expect(state?.revision).toBe(revisionBefore + 3);
      const responseStat = await lstat(RESPONSE_RESPONSE_PATH(ctx.fixture.runRoot));
      expect(responseStat.ino).toBeNumber();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. two identical C2 completions racing record the response exactly once", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const first = completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const second = completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent });
      const [one, two] = await Promise.all([first, second]);
      expect(one.state.status).toBe("active");
      expect(two.state.status).toBe("active");
      expect(ctx.sink.snapshot?.revision).toBe(revisionBefore + 1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29. a hostile applyGrant result is the controller's invalid_result without any response work", async () => {
    const ctx = await completionReady();
    try {
      let recordCalls = 0;
      const hostileOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: async () => ({ wait_index: 2, generation_index: 1, iteration_index: 1, additional_iterations: 2, intent_sha256: hex("c"), state: ctx.sink.snapshot as PipelineV2RunState }),
        recordWaitAction: async () => {
          recordCalls += 1;
          throw new Error("the response recorder must not be called for a hostile grant result");
        },
      };
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(hostileOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("does not match the accepted continue_stage intent");
      expect(recordCalls).toBe(0);
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. hostile response results with a wrong action, digest or target are invalid_result", async () => {
    const ctx = await completionReady();
    try {
      const realApply = productionContinueStageCompletionOps.applyGrant;
      const realRecord = productionContinueStageCompletionOps.recordWaitAction;
      const wrongActionOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: realApply,
        recordWaitAction: async () => ({ wait_index: 1, request_sha256: hex("1"), response_sha256: hex("9"), action_id: "revise_task", action_to: "architect", state: ctx.sink.snapshot as PipelineV2RunState }),
      };
      let cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(wrongActionOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expectCompletionError(cause, "invalid_result");
      const wrongDigestOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: realApply,
        recordWaitAction: async () => ({ wait_index: 1, request_sha256: hex("2"), response_sha256: hex("9"), action_id: "continue_stage", action_to: "dev_entry", state: ctx.sink.snapshot as PipelineV2RunState }),
      };
      cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(wrongDigestOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expectCompletionError(cause, "invalid_result");
      const wrongTargetOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: realApply,
        recordWaitAction: async () => ({ wait_index: 1, request_sha256: hex("1"), response_sha256: hex("9"), action_id: "continue_stage", action_to: "architect", state: ctx.sink.snapshot as PipelineV2RunState }),
      };
      cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(wrongTargetOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expectCompletionError(cause, "invalid_result");
      expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. a hostile response result that mutates the grant or closure bindings is invalid_result", async () => {
    const ctx = await completionReady();
    try {
      const realApply = productionContinueStageCompletionOps.applyGrant;
      const realRecord = productionContinueStageCompletionOps.recordWaitAction;
      const originalRecord = realRecord;
      const mutatedOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: realApply,
        recordWaitAction: async (options) => {
          const real = await originalRecord(options);
          const derived = structuredClone(real.state) as PipelineV2RunState;
          const generation = derived.generations[0];
          if (generation === undefined) {
            throw new Error("fixture generation missing");
          }
          const { open_iteration: _open, ...rest } = generation;
          (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
            ...rest,
            stage_id: "stage-9",
          };
          return { ...real, state: derived };
        },
      };
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(mutatedOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("changed the durable grant or iteration bindings");
      // the real durable state is unaffected
      expect(ctx.sink.snapshot?.generations[0]?.stage_id).toBe("stage-1");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. options and ops getters are read exactly once", async () => {
    const ctx = await completionReady();
    try {
      const optionReads: string[] = [];
      const opsReads: string[] = [];
      const optionsProxy = new Proxy({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent } as Record<string, unknown>, {
        get(target, property) {
          optionReads.push(String(property));
          return target[property as string];
        },
      });
      const opsProxy = new Proxy(productionContinueStageCompletionOps as unknown as Record<string, unknown>, {
        get(target, property) {
          opsReads.push(String(property));
          return target[property as string];
        },
      });
      const result = await completePipelineV2ContinueStageWithIo(opsProxy as unknown as PipelineV2ContinueStageCompletionOps, optionsProxy as unknown as Parameters<typeof completePipelineV2ContinueStage>[0]);
      expect(result).toMatchObject({ wait_index: 1, action_id: "continue_stage" });
      expect(optionReads.filter((name) => name === "runRoot" || name === "sink" || name === "intent")).toEqual(["runRoot", "sink", "intent"]);
      expect(opsReads.filter((name) => name === "applyGrant" || name === "recordWaitAction")).toEqual(["applyGrant", "recordWaitAction"]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. caller mutation after the capture cannot influence the completion", async () => {
    const ctx = await completionReady();
    try {
      const options = { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent };
      const pending = completePipelineV2ContinueStage(options);
      (options as { runRoot: string }).runRoot = "/replaced";
      const result = await pending;
      expect(result).toMatchObject({ wait_index: 1, action_id: "continue_stage", action_to: "dev_entry" });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. downstream unexpected errors preserve their identity", async () => {
    const ctx = await completionReady();
    try {
      const boom = new Error("the grant operation failed unexpectedly");
      const throwingOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: async () => {
          throw boom;
        },
        recordWaitAction: async () => {
          throw new Error("unreachable");
        },
      };
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(throwingOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      expect(cause).toBe(boom);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. diagnostics are content-free across the failure paths", async () => {
    const ctx = await completionReady({ acceptIntent: false });
    try {
      const messages: string[] = [];
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      messages.push((cause as Error).message);
      const optionsBoom = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: undefined as unknown as PipelineV2ContinueStageCompletionControllerSink, intent: ctx.intent }),
      );
      messages.push((optionsBoom as Error).message);
      for (const message of messages) {
        expect(message).not.toContain(ctx.fixture.runRoot);
        expect(message).not.toContain(ctx.intent.canonical_json);
        expect(message).not.toContain(ctx.intent.sha256);
        expect(message).not.toContain("Body A");
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

test("1b. the answered S2 refuses a later execution started at a planning target without a transition", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true, continueStageTo: "architect" });
    try {
      // later graph progress after the response: a planning execution
      // starts at the response target, no transition yet
      await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      const error = cause as PipelineV2ContinueStageGrantControllerError;
      expect(error.reason).toBe("lifecycle_conflict");
      expect(error.message).toContain("an execution was started after the settled wait boundary");
      expect(recording.commands).toEqual([]);
      expect(ctx.sink.snapshot?.waits[1]).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("1c. the answered S2 refuses a later execution and committed transition after the response", async () => {
    const ctx = await completionReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true, recordResponse: true, continueStageTo: "architect" });
    try {
      await ctx.sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
      for (const command of agentPhases("later")) {
        await ctx.sink.dispatch(command);
      }
      await ctx.sink.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 3,
      });
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStage({ runRoot: ctx.fixture.runRoot, sink: recording, intent: ctx.intent }),
      );
      expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
      const error = cause as PipelineV2ContinueStageGrantControllerError;
      expect(error.reason).toBe("lifecycle_conflict");
      expect(recording.commands).toEqual([]);
      // the wait journal and the grant closure records are unchanged
      expect(ctx.sink.snapshot?.waits).toHaveLength(1);
      expect(ctx.sink.snapshot?.generations[0]?.iterations[0]?.closed?.by).toBe("grant");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29b. a hostile grant result with a removed closure yields invalid_result before any response work", async () => {
    const ctx = await completionReady();
    try {
      const realApply = productionContinueStageCompletionOps.applyGrant;
      let recordCalls = 0;
      const hostileOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: async (options) => {
          const real = await realApply(options);
          const derived = structuredClone(real.state) as PipelineV2RunState;
          const generation = derived.generations[0];
          const iteration = generation?.iterations[0];
          if (generation === undefined || iteration === undefined) {
            throw new Error("fixture generation missing");
          }
          (generation.iterations[0] as { closed?: unknown }).closed = undefined;
          return { ...real, state: derived };
        },
        recordWaitAction: async () => {
          recordCalls += 1;
          throw new Error("the response recorder must not be called for a hostile grant result");
        },
      };
      const realRecord = productionContinueStageCompletionOps.recordWaitAction;
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(hostileOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message).toContain("does not carry the exact grant closure");
      expect(recordCalls).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29c. a hostile grant result with a replaced wait intent, stage or plan binding yields invalid_result", async () => {
    const ctx = await completionReady();
    try {
      const realApply = productionContinueStageCompletionOps.applyGrant;
      let recordCalls = 0;
      const realRecord = productionContinueStageCompletionOps.recordWaitAction;
      const makeHostile = (mutate: (derived: PipelineV2RunState) => void): PipelineV2ContinueStageCompletionOps => ({
        applyGrant: async (options) => {
          const real = await realApply(options);
          const derived = structuredClone(real.state) as PipelineV2RunState;
          mutate(derived);
          return { ...real, state: derived };
        },
        recordWaitAction: async () => {
          recordCalls += 1;
          return await realRecord({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "continue_stage" });
        },
      });
      const wait = (derived: PipelineV2RunState) => derived.waits[0] as PipelineV2WaitRecord;
      const generation = (derived: PipelineV2RunState) => derived.generations[0] as NonNullable<PipelineV2RunState["generations"][number]>;
      const variants: Array<[string, (derived: PipelineV2RunState) => void, string]> = [
        ["wait intent", (derived) => { (wait(derived).intent as { intent_sha256: string }).intent_sha256 = hex("e"); }, "does not carry the exact accepted intent"],
        ["stage binding", (derived) => { (generation(derived) as { stage_id: string }).stage_id = "stage-9"; }, "generation bindings do not match the accepted intent"],
        ["plan binding", (derived) => { (generation(derived) as { plan_sha256: string }).plan_sha256 = hex("f"); }, "generation bindings do not match the accepted intent"],
      ];
      for (const [label, mutate, expectedMessage] of variants) {
        recordCalls = 0;
        const cause = await catchAccept(() =>
          completePipelineV2ContinueStageWithIo(makeHostile(mutate), { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect(error.message).toContain(expectedMessage);
        expect(recordCalls).toBe(0);
        expect(ctx.sink.snapshot?.waits[0]?.response).toBeUndefined();
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("29d. a malformed nested grant result state yields typed invalid_result, never a leaked TypeError", async () => {
    const ctx = await completionReady();
    try {
      const realApply = productionContinueStageCompletionOps.applyGrant;
      const realRecord = productionContinueStageCompletionOps.recordWaitAction;
      let recordCalls = 0;
      const malformedOps: PipelineV2ContinueStageCompletionOps = {
        applyGrant: async (options) => {
          const real = await realApply(options);
          const derived = structuredClone(real.state) as unknown as Record<string, unknown>;
          derived["grants"] = null;
          return { ...real, state: derived as unknown as PipelineV2RunState };
        },
        recordWaitAction: async () => {
          recordCalls += 1;
          return await realRecord({ runRoot: ctx.fixture.runRoot, sink: ctx.sink, waitIndex: 1, actionId: "continue_stage" });
        },
      };
      const cause = await catchAccept(() =>
        completePipelineV2ContinueStageWithIo(malformedOps, { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectCompletionError(cause, "invalid_result");
      expect(error.message.length).toBeGreaterThan(0);
      expect(error.message).not.toContain("null");
      expect(recordCalls).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30b. coherent-hostile response results (the result fields and the cloned state changed together) are invalid_result", async () => {
    const realApply = productionContinueStageCompletionOps.applyGrant;
    const realRecord = productionContinueStageCompletionOps.recordWaitAction;
    const makeHostile = (mutate: (real: RecordedPipelineV2WaitResponse, derived: PipelineV2RunState) => void): PipelineV2ContinueStageCompletionOps => ({
      applyGrant: realApply,
      recordWaitAction: async (options) => {
        const real = await realRecord(options);
        const derived = structuredClone(real.state) as PipelineV2RunState;
        const realCopy = { ...real };
        mutate(realCopy, derived);
        return { ...realCopy, state: derived };
      },
    });
    const finalWait = (derived: PipelineV2RunState) => derived.waits[0] as PipelineV2WaitRecord;
    const variants: Array<[string, (real: RecordedPipelineV2WaitResponse, derived: PipelineV2RunState) => void, string]> = [
      [
        "coherent request digest",
        (real, derived) => {
          (finalWait(derived) as { request_sha256: string }).request_sha256 = hex("7");
          (real as { request_sha256: string }).request_sha256 = hex("7");
        },
        "changed the target wait bindings",
      ],
      [
        "coherent action target",
        (real, derived) => {
          (finalWait(derived).actions[0] as { to: string }).to = "architect";
          (real as { action_to: string }).action_to = "architect";
        },
        "changed the target wait bindings",
      ],
      [
        "coherent transition count and closure anchor",
        (real, derived) => {
          (finalWait(derived) as { transition_count: number }).transition_count = 5;
          const iteration = derived.generations[0]?.iterations[0];
          if (iteration?.closed === undefined) {
            throw new Error("fixture closure missing");
          }
          (iteration.closed as { closed_transition_count: number }).closed_transition_count = 5;
        },
        "changed the target wait bindings",
      ],
    ];
    for (const [, mutate, expectedMessage] of variants) {
      const ctx = await completionReady();
      try {
        const cause = await catchAccept(() =>
          completePipelineV2ContinueStageWithIo(makeHostile(mutate), { runRoot: ctx.fixture.runRoot, sink: ctx.sink, intent: ctx.intent }),
        );
        const error = expectCompletionError(cause, "invalid_result");
        expect(error.message).toContain(expectedMessage);
      } finally {
        await disposeRun(ctx.fixture);
      }
    }
  });

test("36. the runtime export surfaces are exact (public two keys, internal core)", async () => {
  const publicModule = await import("../src/pipeline_v2_continue_stage_completion_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinueStageCompletionControllerError",
    "completePipelineV2ContinueStage",
  ]);
  const internalModule = await import("../src/pipeline_v2_continue_stage_completion_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinueStageCompletionControllerError",
    "completePipelineV2ContinueStageWithIo",
    "productionContinueStageCompletionOps",
  ]);
});

test("37. the completion composes the existing layers only (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_continue_stage_completion_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("applyPipelineV2ContinueStageGrant")).toBe(6);
  expect(countOf("recordPipelineV2WaitAction")).toBe(5);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(0);
  expect(countOf("validatePipelineV2RunState(")).toBe(0);
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
  for (const banned of ["pipeline_v2_coordinator", "pipeline_v2_runner", "main.ts", "cli_", "docker", "launcher", "pipeline_v2_run_plan_store", "pipeline_v2_wait_store", "pipeline_v2_wait_manifest", "pipeline_v2_state_store"]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("let production")).toBe(0);
});
