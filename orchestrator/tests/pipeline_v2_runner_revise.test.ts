/**
 * The dedicated revise-task runner entrypoint (`revisePipelineV2Task`):
 * the external parameters are exactly the run id, the wait journal index,
 * the task id and the caller's revised task body plus the standard resume
 * configuration — the action is fixed by the entrypoint as the reserved
 * `revise_task`, and every internal intervention parameter (the stage id,
 * the expected plan digest, the budget, the prepared intent and candidate,
 * the pipeline and the compiled plan) is derived by the existing handoff
 * controller from the authoritative durable state after the reopen. The
 * proofs drive the honest prefix through the real production facades only
 * (the run-owned project copy, the run-input snapshot, the real data-plane
 * activations, the real plan acceptance, the real stage-iteration
 * controller and the real wait-entry controller), the simulated restarts
 * go through the ordinary `PipelineV2RunStateSink.open`, and the runner's
 * own preflight is the real one (the fake CLI transport is the only
 * fake). No LLM, no Docker Helper, no launcher credential, no sleeps. The
 * runner API stays unwired: the CLI, `main.ts` and the default pipeline
 * are untouched.
 */
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import type { AuthFetcher, CliRunner } from "../src/docker_helper.ts";
import {
  revisePipelineV2Task,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { applyPipelineV2ContinueStageIntervention } from "../src/pipeline_v2_continue_stage_intervention_controller.ts";
import { enterPipelineV2Wait } from "../src/pipeline_v2_wait_controller.ts";
import {
  mintRestoredRunInputsSnapshot,
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { hex, startRoleArgs } from "./pipeline_v2_state_fixtures.ts";

const PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 20

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
      plan_output: plan
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
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

const RUN_ID = "revise-runner-run";
const EXPECTED_LAUNCHER_ID = "dhl_revise";
const INITIAL_BUDGET = 2;
const TASK_BODY = "REVISED-PLAN-TASK-BODY";

interface Harness {
  root: string;
  stateRoot: string;
  bundle: string;
  configRoot: string;
  sources: string;
  projectSource: string;
  credentialFile: string;
  runRoot: string;
  statePath: string;
}

let clockValue = 0;
function nextTick(): Date {
  clockValue += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
}

const ROOTS_TO_DISPOSE: string[] = [];

afterAll(async () => {
  for (const root of ROOTS_TO_DISPOSE.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

async function makeHarness(prefix: string, runId: string = RUN_ID): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS_TO_DISPOSE.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), PIPELINE);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(
    join(configRoot, "profiles", "coder.yaml"),
    [
      "schema_version: 1",
      "image: ghcr.io/example/worker:1",
      "opencode_config: opencode/coder.json",
      "env:",
      "  MODEL_API_KEY:",
      "    from_env: CODER_SOURCE_VAR_1",
      "    required: true",
      "",
    ].join("\n"),
  );
  await writeFile(join(configRoot, "opencode", "coder.json"), JSON.stringify({ model: "glm53-flash" }));
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.md"), "TASK-BODY\n");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(join(stateRoot, "pipeline-runs"), { mode: 0o700 });
  await mkdir(runRoot, { mode: 0o700 });
  return { root, stateRoot, bundle, configRoot, sources, projectSource, credentialFile, runRoot, statePath: join(runRoot, "state.json") };
}

function commandSink(sink: PipelineV2RunStateSink, recording: Array<Record<string, unknown>>) {
  return {
    get snapshot() {
      return sink.snapshot;
    },
    get poisoned() {
      return sink.poisoned;
    },
    async dispatch(command: Parameters<typeof sink.dispatch>[0]) {
      recording.push({ ...command });
      await sink.dispatch(command);
    },
  };
}

/**
 * The honest revise-wait prefix through the real production facades only:
 * the run-owned project copy, the run-input snapshot, the planning
 * execution (settled unbound), the real plan acceptance, generation 1 /
 * iteration 1, the planning transition, the stage execution back to the
 * planning state, and the real wait entry declaring `revise_task` (plus
 * `continue_stage`). Returns nothing: the harness carries only filesystem
 * coordinates, so no in-memory pipeline, intent, candidate, compiled plan
 * or snapshot can ever be handed to the runner (the runner loads the
 * pipeline itself from the durable bundle root).
 */
async function drivePrefix(
  harness: Harness,
  waitActions: ReadonlyArray<{ id: string; to: string }> = [
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "architect" },
  ],
  options: { runId?: string } = {},
): Promise<void> {
  const runId = options.runId ?? RUN_ID;
  clockValue = 0;
  const pipeline = await loadPipelineV2(harness.bundle);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  await rm(runRoot, { recursive: true, force: true });
  await mkdir(runRoot, { mode: 0o700 });
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording: Array<Record<string, unknown>> = [];
  const rec = commandSink(sink, recording);
  await prepareRunProject(harness.projectSource, runRoot);
  const runInputs: RunInputsSnapshot = await snapshotRunInputs(
    pipeline,
    [{ id: "task", path: join(harness.sources, "task.md") }] as readonly RunInputBinding[],
    runRoot,
  );
  await rec.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  const accepted: AcceptedStateOutput[] = [];
  const runAgentStep = async (stateId: string, executionIndex: number, commit: boolean): Promise<void> => {
    const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
    await rec.dispatch({
      kind: "start_agent_execution",
      stateId,
      profile: "coder",
      ...startRoleArgs(pipeline, stateId, sink.snapshot),
    });
    await rec.dispatch({ kind: "agent_data_prepared" });
    await rec.dispatch({ kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` });
    await rec.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` });
    await rec.dispatch({ kind: "agent_running" });
    if (stateId === "architect") {
      await writeFile(join(activation.outputs_root, "plan"), "{}", { mode: 0o600 });
    }
    const records = await acceptActivationOutputs(pipeline, activation);
    await rec.dispatch({
      kind: "agent_outputs_accepted",
      outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
    });
    await rec.dispatch({ kind: "agent_cleanup_completed" });
    if (commit) {
      await rec.dispatch({
        kind: "transition_committed",
        step: { from: stateId, outcome: "completed", to: stateId === "architect" ? "dev_entry" : "architect", transition_index: 0 },
        executionIndex,
      });
    }
    accepted.push(...records);
  };
  await runAgentStep("architect", 1, false);
  const taskA = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: "task-a",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "PLAN-TASK-BODY",
  });
  const plan1 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: runId,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: runInputs.inputs[0]?.digest ?? "" },
    origin_execution: 1,
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }],
      },
    ],
  });
  const candidate = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: runInputs.inputs[0]?.digest ?? "",
  });
  const acceptedPlan = await acceptPipelineV2RunPlanCandidate({
    pipeline,
    runRoot,
    sink: rec as never,
    candidate,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: acceptedPlan.compiled_plan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: rec as never,
  });
  await rec.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  await runAgentStep("dev_entry", 2, true);
  await enterPipelineV2Wait({
    runRoot,
    sink: rec as never,
    reason: "stage_iteration_limit_exhausted",
    actions: waitActions,
  });
}

async function readDurableState(harness: Harness, runId: string = RUN_ID): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(
    await readFile(join(harness.stateRoot, "pipeline-runs", runId, "state.json"), "utf8"),
  );
}

interface Script {
  onAuth?: () => void | Promise<void>;
  /** Delivers the signal inside the auth callback (post-run-root). */
  signalAtAuth?: "SIGINT" | "SIGTERM";
  runCode?: number;
  runTimedOut?: boolean;
}

function fakeCli(script: Script, sessionCreates: string[], sessionDeletes: string[]): CliRunner {
  return async (args, _env, _stdio, _opts) => {
    const first = args[0];
    const second = args[1];
    if (first === "session" && second === "create") {
      sessionCreates.push(args.join(" "));
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${sessionCreates.length}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_${sessionCreates.length}`,
        }),
      };
    }
    if (first === "session" && second === "delete") {
      sessionDeletes.push(args[args.length - 1] as string);
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    if (first === "pull") {
      return { code: 1, stderr: "PULL-FAILED-BY-TEST" };
    }
    if (first === "run") {
      return { code: script.runCode ?? 0, timedOut: script.runTimedOut ?? false };
    }
    return { code: 1 };
  };
}

function runnerDeps(harness: Harness, script: Script = {}, sessionCreates: string[] = [], sessionDeletes: string[] = []): PipelineV2RunnerDeps {
  let handler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
  const fetchAuth: AuthFetcher = async () => {
    await script.onAuth?.();
    if (script.signalAtAuth !== undefined) {
      handler?.(script.signalAtAuth);
    }
    return {
      status: 200,
      body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
    };
  };
  return {
    cli: fakeCli(script, sessionCreates, sessionDeletes),
    fetchAuth,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "tester-secret" },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    onSignal: (installed) => {
      handler = installed;
    },
    now: nextTick,
  };
}

function captureDiagnostics(): { errors: string[]; restore: () => void } {
  const errors: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]): void => {
    errors.push(args.map((arg) => String(arg)).join(" "));
  };
  return { errors, restore: () => (console.error = original) };
}

async function runRevise(
  harness: Harness,
  options: Partial<{ runId: string; waitIndex: number; taskId: string; taskBody: string; configRoot: string; launcherId: string }> = {},
  script: Script = {},
  depsOverrides: Partial<PipelineV2RunnerDeps> = {},
): Promise<{ outcome: PipelineV2RunOutcome; diagnostics: string[]; sessionCreates: string[]; sessionDeletes: string[]; authCalls: number }> {
  const sessionCreates: string[] = [];
  const sessionDeletes: string[] = [];
  let authCalls = 0;
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  const deps = runnerDeps(harness, script, sessionCreates, sessionDeletes);
  const countedFetchAuth: AuthFetcher = async (socket, token) => {
    authCalls += 1;
    return await deps.fetchAuth(socket, token);
  };
  try {
    outcome = await revisePipelineV2Task(
      {
        runId: options.runId ?? RUN_ID,
        waitIndex: options.waitIndex ?? 1,
        taskId: options.taskId ?? "task-a",
        taskBody: options.taskBody ?? TASK_BODY,
        configRoot: options.configRoot ?? harness.configRoot,
        launcherId: options.launcherId ?? EXPECTED_LAUNCHER_ID,
      },
      { ...deps, fetchAuth: countedFetchAuth, ...depsOverrides },
    );
  } finally {
    captured.restore();
  }
  return { outcome, diagnostics: captured.errors, sessionCreates, sessionDeletes, authCalls };
}

type FailureOutcome = PipelineV2RunOutcome & { ok: false };

function expectFailure(outcome: PipelineV2RunOutcome): FailureOutcome {
  if (outcome.ok) {
    throw new Error(`expected a failure outcome, got ${JSON.stringify(outcome)}`);
  }
  expect(outcome.exitCode).toBe(1);
  return outcome as FailureOutcome;
}

function expectPostRunRootFailure(
  outcome: PipelineV2RunOutcome,
  harness: Harness,
  expectedStatus: "waiting" | "active" | "failed" = "waiting",
): FailureOutcome {
  const failed = expectFailure(outcome);
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  // the post-run-root failure shape carries no reason field
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect(failed.state?.status).toBe(expectedStatus);
  return failed;
}

function expectPreflightFailure(outcome: PipelineV2RunOutcome): void {
  const failed = expectFailure(outcome);
  expect(failed.runId).toBe("");
  expect(failed.runRoot).toBeNull();
  expect(failed.state).toBeNull();
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
}

/** Drives the revise intervention through the public first-half facade on a reopened run. */
async function completeReviseIntervention(harness: Harness, runId: string = RUN_ID): Promise<PipelineV2RunState> {
  clockValue = 0;
  const preSink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const preSnapshot = preSink.snapshot as PipelineV2RunState;
  const intervention = await applyPipelineV2ReviseTaskIntervention({
    pipeline: await loadPipelineV2(preSnapshot.pipeline.bundle_root),
    runRoot: join(harness.stateRoot, "pipeline-runs", runId),
    sink: preSink,
    runId,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: TASK_BODY,
  });
  if (intervention.state === null) {
    throw new Error("expected the revise intervention to record the durable state");
  }
  expect(intervention.state).toBe(preSink.snapshot as PipelineV2RunState);
  return preSink.snapshot as PipelineV2RunState;
}

// --- C0 ---------------------------------------------------------------------

test("C0: the runner routes the waiting revise boundary through the handoff facade; the exact eleven-command suffix", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-c0-");
  await drivePrefix(harness);
  const prefixState = await readDurableState(harness);
  const { outcome, diagnostics, sessionCreates, sessionDeletes } = await runRevise(harness);
  const failed = expectFailure(outcome);
  // the ordinary worker failure of the resumed planning execution — never
  // a refusal and never a pipeline mismatch
  expect(failed.reason).toBe("worker_failed");
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  expect(failed.state?.status).toBe("failed");
  // the diagnostics name no user path and no task body
  expect(diagnostics.join("\n")).not.toContain("userdata");
  expect(diagnostics.join("\n")).not.toContain("project-source");
  expect(diagnostics.join("\n")).not.toContain(TASK_BODY);
  expect(JSON.stringify(outcome)).not.toContain(TASK_BODY);

  // the durable projection of the exact eleven-command sequence: the four
  // intervention commands, then the seven resume commands
  const state = await readDurableState(harness);
  const wait0 = state.waits[0]!;
  expect(wait0.index).toBe(1);
  expect(wait0.intent?.intent_sha256).toBeDefined();
  expect(wait0.response?.action_id).toBe("revise_task");
  // the intervention's task revision
  expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-a@1", "task-a@2"]);
  // the replanned iteration closure
  const generation = state.generations[0]!;
  expect(generation.stage_id).toBe("stage-1");
  expect(generation.closed).toBeUndefined();
  expect(generation.iterations[0]?.closed).toEqual({ by: "replanned", wait_index: 1, closed_transition_count: 2 });
  expect(generation.open_iteration).toBeUndefined();
  // the resumed planning execution 3 at the revise routing target
  expect(state.cursor).toEqual({ current_state: "architect", transition_count: 2 });
  expect(state.transitions).toHaveLength(2);
  const execution3 = state.executions[2];
  expect(execution3).toMatchObject({
    index: 3,
    state_id: "architect",
    execution_role: "planning",
    phase: "failed",
    failure_reason: "worker_failed",
  });
  expect(execution3?.iteration_index).toBeUndefined();
  if (execution3?.type !== "agent") {
    throw new Error("expected the resumed execution to be an agent execution");
  }
  expect(execution3.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "worker_failed" });
  // one new session pair only (the resumed execution), cleaned exactly
  // once each and tool-first
  expect(sessionCreates).toHaveLength(2);
  expect(sessionDeletes).toEqual(["dhs_2", "dhs_1"]);
  // exactly eleven durable commits: four intervention + seven resume
  expect(state.revision).toBe(prefixState.revision + 11);
  // the pipeline was loaded from the durable bundle root (no in-memory
  // reuse: the prefix helper returned no pipeline object)
  expect(state.pipeline.bundle_root).toBe(harness.bundle);
  // the loader round-trip
  expect(await readDurableState(harness)).toEqual(state);
});

// --- R4 / crash retry -------------------------------------------------------

test("R4: after the durable completed intervention the runner repeats no intervention command", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-r4-");
  await drivePrefix(harness);
  const boundary = await completeReviseIntervention(harness);
  expect(boundary.status).toBe("active");
  expect(boundary.waits[0]?.intent?.intent_sha256).toBeDefined();
  expect(boundary.waits[0]?.response?.action_id).toBe("revise_task");

  const { outcome, diagnostics, sessionCreates } = await runRevise(harness);
  const failed = expectFailure(outcome);
  expect(failed.reason).toBe("worker_failed");
  expect(diagnostics.join("\n")).not.toContain("userdata");

  // only the seven resume commands: no intervention command repeated, and
  // the wait/task/plan/generation/closure projection is never rewritten
  const state = await readDurableState(harness);
  expect(state.revision).toBe(boundary.revision + 7);
  expect(state.waits).toEqual(boundary.waits);
  expect(state.task_revisions).toEqual(boundary.task_revisions);
  expect(state.plan_revisions).toEqual(boundary.plan_revisions);
  expect(state.generations).toEqual(boundary.generations);
  expect(state.grants).toEqual(boundary.grants);
  expect(state.pipeline).toEqual(boundary.pipeline);
  expect(state.executions).toHaveLength(3);
  expect(state.executions[2]).toMatchObject({
    index: 3,
    state_id: "architect",
    execution_role: "planning",
    phase: "failed",
  });
  expect(sessionCreates).toHaveLength(2);
  expect(await readDurableState(harness)).toEqual(state);
});

// --- invalid options matrix (preflight, before any run-root access) ----------

test("invalid options matrix: every refusal happens before any run-root access", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-contract-");
  await drivePrefix(harness);
  const prefixBytes = await readFile(harness.statePath);
  for (const options of [
    null,
    "options",
    [],
    { runId: "../escape", waitIndex: 1, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 0, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: -2, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1.5, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: Number.MAX_SAFE_INTEGER + 1, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: "../escape", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: "", taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: 42, taskBody: TASK_BODY, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: "", configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: 42, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: TASK_BODY, configRoot: "relative/config" },
    { runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: TASK_BODY, configRoot: harness.configRoot, launcherId: "not-dhl" },
    { runId: RUN_ID, waitIndex: 1, taskId: "task-a", taskBody: TASK_BODY },
  ] as ReadonlyArray<unknown>) {
    const captured = captureDiagnostics();
    let outcome: PipelineV2RunOutcome;
    try {
      outcome = await revisePipelineV2Task(options as never, runnerDeps(harness));
    } finally {
      captured.restore();
    }
    expectPreflightFailure(outcome);
  }
  // the durable tree is untouched by every contract refusal
  expect(await readFile(harness.statePath)).toEqual(prefixBytes);
});

// --- routing derivation matrix ----------------------------------------------

test("routing refusals happen before any pipeline, profile, authority or runtime work", async () => {
  // a missing wait index: no record carries index 2
  const missing = await makeHarness("pipeline-v2-revise-runner-missing-idx-");
  await drivePrefix(missing);
  const missingRun = await runRevise(missing, { waitIndex: 2 });
  expectPostRunRootFailure(missingRun.outcome, missing);
  expect(missingRun.sessionCreates).toEqual([]);
  expect(missingRun.authCalls).toBe(0);
  expect((await readDurableState(missing)).status).toBe("waiting");

  // the target wait is not the last: a second wait was entered after the
  // first one was answered by the revise intervention
  const twoWaits = await makeHarness("pipeline-v2-revise-runner-two-waits-");
  await drivePrefix(twoWaits);
  await completeReviseIntervention(twoWaits);
  clockValue = 0;
  const rec2 = commandSink(
    await PipelineV2RunStateSink.open({ stateRoot: twoWaits.stateRoot, runId: RUN_ID, now: nextTick }),
    [],
  );
  await enterPipelineV2Wait({
    runRoot: twoWaits.runRoot,
    sink: rec2 as never,
    reason: "stage_iteration_limit_exhausted",
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  const notLast = await runRevise(twoWaits, { waitIndex: 1 });
  const notLastOutcome = expectPostRunRootFailure(notLast.outcome, twoWaits, "waiting");
  expect(notLastOutcome.state?.waits).toHaveLength(2);
  expect(notLast.sessionCreates).toEqual([]);
  expect(notLast.authCalls).toBe(0);
  expect((await readDurableState(twoWaits)).waits[1]?.response).toBeUndefined();

  // no exact revise_task declaration in the target wait
  const ordinary = await makeHarness("pipeline-v2-revise-runner-ordinary-");
  await drivePrefix(ordinary, [{ id: "continue_stage", to: "dev_entry" }]);
  const ordinaryRun = await runRevise(ordinary);
  const ordinaryOutcome = expectPostRunRootFailure(ordinaryRun.outcome, ordinary);
  expect(ordinaryOutcome.state?.waits[0]?.actions.map((action) => action.id)).toEqual(["continue_stage"]);
  expect(ordinaryRun.sessionCreates).toEqual([]);
  expect(ordinaryRun.authCalls).toBe(0);

  // the exact action was already answered with another action: the
  // continue-stage intervention answered the same wait
  const answeredOther = await makeHarness("pipeline-v2-revise-runner-answered-");
  await drivePrefix(answeredOther);
  clockValue = 0;
  const preSink = await PipelineV2RunStateSink.open({ stateRoot: answeredOther.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot = preSink.snapshot as PipelineV2RunState;
  const openGeneration = preSnapshot.generations[0]!;
  await applyPipelineV2ContinueStageIntervention({
    pipeline: await loadPipelineV2(preSnapshot.pipeline.bundle_root),
    runRoot: answeredOther.runRoot,
    sink: preSink,
    intent: prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: openGeneration.stage_id,
      expected_plan_sha256: openGeneration.plan_sha256,
      additional_iterations: 2,
    }),
    initialBudget: openGeneration.initial_budget,
  });
  const answeredRun = await runRevise(answeredOther);
  const answeredOutcome = expectPostRunRootFailure(answeredRun.outcome, answeredOther, "active");
  expect(answeredOutcome.state?.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(answeredRun.sessionCreates).toEqual([]);
  expect(answeredRun.authCalls).toBe(0);

  // a mid-intervention window is not one of the two routing boundaries:
  // the accepted intent is durable but the response is not
  const midWindow = await makeHarness("pipeline-v2-revise-runner-mid-window-");
  await drivePrefix(midWindow);
  clockValue = 0;
  const rec3 = commandSink(
    await PipelineV2RunStateSink.open({ stateRoot: midWindow.stateRoot, runId: RUN_ID, now: nextTick }),
    [],
  );
  await rec3.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("e") });
  expect((await readDurableState(midWindow)).waits[0]?.intent).toBeDefined();
  const midRun = await runRevise(midWindow);
  const midOutcome = expectPostRunRootFailure(midRun.outcome, midWindow);
  expect(midOutcome.state?.waits[0]?.response).toBeUndefined();
  expect(midRun.sessionCreates).toEqual([]);
  expect(midRun.authCalls).toBe(0);
});

test("loader-invalid wait journals are refused at the sink open, before any routing", async () => {
  for (const [prefix, mutate] of [
    ["dup-idx", (clone: Record<string, unknown>) => {
      const waits = clone["waits"] as Array<Record<string, unknown>>;
      waits.push({ ...waits[0] });
    }],
    ["two-decls", (clone: Record<string, unknown>) => {
      const waits = clone["waits"] as Array<Record<string, unknown>>;
      (waits[0] as Record<string, unknown>)["actions"] = [
        { id: "revise_task", to: "architect" },
        { id: "revise_task", to: "dev_entry" },
      ];
    }],
    ["malformed-actions", (clone: Record<string, unknown>) => {
      const waits = clone["waits"] as Array<Record<string, unknown>>;
      (waits[0] as Record<string, unknown>)["actions"] = null;
    }],
  ] as ReadonlyArray<readonly [string, (clone: Record<string, unknown>) => void]>) {
    const harness = await makeHarness(`pipeline-v2-revise-runner-${prefix}-`);
    await drivePrefix(harness);
    const validBytes = await readFile(harness.statePath, "utf8");
    const clone = JSON.parse(validBytes) as Record<string, unknown>;
    mutate(clone);
    await writeFile(harness.statePath, JSON.stringify(clone));
    const sessionCreates: string[] = [];
    const { outcome, authCalls } = await runRevise(harness, {}, {}, {});
    void sessionCreates;
    const failed = expectFailure(outcome);
    // the sink open failed: the post-run-root failure shape with no state
    expect(failed.runId).toBe(RUN_ID);
    expect(failed.runRoot).toBe(harness.runRoot);
    expect(failed.state).toBeNull();
    expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(authCalls).toBe(0);
    // the malformed document is never repaired
    const raw = JSON.parse(await readFile(harness.statePath, "utf8")) as Record<string, unknown>;
    expect(raw).toEqual(clone);
  }
});

// --- wrong-body R4 ----------------------------------------------------------

test("wrong-body R4: the handoff refuses with its own typed error; the durable tree is byte-identical", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-wrong-body-");
  await drivePrefix(harness);
  const boundary = await completeReviseIntervention(harness);
  const boundaryBytes = await readFile(harness.statePath);

  const { outcome, sessionCreates } = await runRevise(harness, { taskBody: "MUTATED-BODY" });
  const failed = expectFailure(outcome);
  // the handoff's typed refusal passes through unclassified: no reason
  // field, the actual run identity and the completed boundary state
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  expect(failed.state?.status).toBe("active");
  expect(failed.state?.waits[0]?.response?.action_id).toBe("revise_task");
  // the resume never started
  expect(sessionCreates).toEqual([]);
  // the durable tree and state are byte-identical
  expect(await readFile(harness.statePath)).toEqual(boundaryBytes);
  expect((await readDurableState(harness)).revision).toBe(boundary.revision);
});

// --- signals ------------------------------------------------------------------

test("a signal accepted at the authority boundary yields 130/143 with zero intervention and session dispatch", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-signal-");
  await drivePrefix(harness);
  const prefixState = await readDurableState(harness);
  const sigint = await runRevise(harness, {}, { signalAtAuth: "SIGINT" });
  expect(sigint.outcome.ok).toBe(false);
  expect(sigint.outcome.exitCode).toBe(130);
  expect(sigint.outcome.reason).toBe("signal_sigint");
  expect(sigint.outcome.runId).toBe(RUN_ID);
  expect(sigint.outcome.runRoot).toBe(harness.runRoot);
  expect(sigint.outcome.state?.status).toBe("waiting");
  // zero intervention commands, zero session effects, zero durable writes
  expect(sigint.sessionCreates).toEqual([]);
  expect(sigint.authCalls).toBe(1);
  let state = await readDurableState(harness);
  expect(state.revision).toBe(prefixState.revision);
  expect(state.status).toBe("waiting");
  expect(state.waits[0]?.response).toBeUndefined();

  const sigterm = await runRevise(harness, {}, { signalAtAuth: "SIGTERM" });
  expect(sigterm.outcome.ok).toBe(false);
  expect(sigterm.outcome.exitCode).toBe(143);
  expect(sigterm.outcome.reason).toBe("signal_sigterm");
  expect(sigterm.outcome.state?.status).toBe("waiting");
  expect(sigterm.sessionCreates).toEqual([]);
  state = await readDurableState(harness);
  expect(state.revision).toBe(prefixState.revision);
});

// --- capture contract ---------------------------------------------------------

test("capture contract: the six revise options are read exactly once in the fixed order and forbidden fields are never read", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-readcount-");
  await drivePrefix(harness);
  const readKeys: string[] = [];
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: TASK_BODY,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
      pipelineRoot: "/forbidden",
      runRoot: "/forbidden",
      actionId: "forbidden",
      intent: "forbidden",
      candidateTaskRevision: "forbidden",
      stageId: "forbidden",
      expectedPlanSha256: "forbidden",
      initialBudget: 99,
      additionalIterations: 9,
      compiledPlan: "forbidden",
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        readKeys.push(String(property));
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await revisePipelineV2Task(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // each of the six fields is read exactly once, in the contract order
  expect(readKeys).toEqual(["runId", "waitIndex", "taskId", "taskBody", "configRoot", "launcherId"]);
  // no forbidden field was ever read
  for (const forbidden of ["pipelineRoot", "runRoot", "actionId", "intent", "candidateTaskRevision", "stageId", "expectedPlanSha256", "initialBudget", "additionalIterations", "compiledPlan"]) {
    expect(readKeys).not.toContain(forbidden);
  }
});

test("capture contract: a mutating taskId getter cannot redirect the revision to another task", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-task-select-");
  await drivePrefix(harness);
  let taskIdReads = 0;
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: TASK_BODY,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "taskId") {
          taskIdReads += 1;
          return taskIdReads === 1 ? "task-a" : "task-b";
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await revisePipelineV2Task(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // only the first validated task was revised
  const state = await readDurableState(harness);
  expect(state.task_revisions.map((record) => record.task_id)).toEqual(["task-a", "task-a"]);
});

test("capture contract: caller mutation after the pending auth cannot change the captured policy", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-policy-mut-");
  await drivePrefix(harness);
  await completeReviseIntervention(harness);
  let releaseAuth!: () => void;
  const gate = new Promise<void>((resolve) => {
    releaseAuth = resolve;
  });
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  const options = {
    runId: RUN_ID,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: TASK_BODY,
    configRoot: harness.configRoot,
    launcherId: EXPECTED_LAUNCHER_ID,
  };
  try {
    const pending = revisePipelineV2Task(options, runnerDeps(harness, {
      onAuth: async () => {
        await gate;
      },
    }));
    // mutate the caller options while the auth is pending: the captured
    // policy was fixed before the first await
    options.taskBody = "MUTATED-AFTER-CAPTURE";
    options.taskId = "task-b";
    options.waitIndex = 9;
    releaseAuth();
    outcome = await pending;
  } finally {
    captured.restore();
  }
  // the captured policy drove the R4 recognition (the mutated body would
  // have failed it) and the resume ran once
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  const state = await readDurableState(harness);
  expect(state.waits).toEqual((await readDurableState(harness)).waits);
  expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-a@1", "task-a@2"]);
});

test("capture contract: nested helper fields are read exactly once and replacement deps are never used", async () => {
  const harness = await makeHarness("pipeline-v2-revise-runner-deps-mut-");
  await drivePrefix(harness);
  let socketReads = 0;
  let credentialReads = 0;
  let nowReads = 0;
  const validatedClock = nextTick;
  const replacementClock = (): Date => new Date(Date.UTC(2030, 0, 1));
  const base = runnerDeps(harness);
  const hostileHelperConfig = new Proxy(
    { socketPath: "/run/socket-a.sock", credentialFile: harness.credentialFile } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "socketPath") {
          socketReads += 1;
          return socketReads === 1 ? "/run/socket-a.sock" : "/run/socket-b.sock";
        }
        if (property === "credentialFile") {
          credentialReads += 1;
          return Reflect.get(target, property, receiver);
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const hostileDeps = new Proxy(base as unknown as Record<string, unknown>, {
    get(target, property, receiver) {
      if (property === "now") {
        nowReads += 1;
        return nowReads === 1 ? validatedClock : replacementClock;
      }
      if (property === "helperConfig") {
        return hostileHelperConfig;
      }
      return Reflect.get(target, property, receiver);
    },
  });
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await revisePipelineV2Task(
      {
        runId: RUN_ID,
        waitIndex: 1,
        taskId: "task-a",
        taskBody: TASK_BODY,
        configRoot: harness.configRoot,
        launcherId: EXPECTED_LAUNCHER_ID,
      },
      hostileDeps as never,
    );
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // the nested helper fields were read exactly once each (at the capture)
  expect(socketReads).toBe(1);
  expect(credentialReads).toBe(1);
  expect(nowReads).toBe(1);
  // every durable timestamp came from the validated clock
  const state = await readDurableState(harness);
  expect(state.updated_at.startsWith("2026-01-01")).toBe(true);
  // the replacement socket was never published anywhere
  expect(captured.errors.join("\n")).not.toContain("socket-b");
  expect(JSON.stringify(outcome)).not.toContain("socket-b");
});

// --- export surface -----------------------------------------------------------

test("the runner export surface gains exactly one key for the revise-task entrypoint", async () => {
  const namespace = (await import("../src/pipeline_v2_runner.ts")) as Record<string, unknown>;
  expect(Object.keys(namespace).sort()).toEqual([
    "continuePipelineV2Stage",
    "resumePipelineV2",
    "revisePipelineV2Task",
    "runPipelineV2",
  ]);
  expect(typeof namespace["revisePipelineV2Task"]).toBe("function");
});
