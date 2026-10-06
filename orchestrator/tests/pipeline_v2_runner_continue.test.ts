/**
 * The dedicated continue-stage runner entrypoint
 * (`continuePipelineV2Stage`): the external parameters are exactly the
 * run id, the wait journal index, the one caller policy scalar
 * `additionalIterations` and the standard resume configuration — every
 * internal intervention parameter (the stage id, the expected plan
 * digest, the initial budget, the prepared intent, the pipeline and the
 * compiled plan) is derived from the authoritative durable state after
 * the reopen. The proofs drive the honest prefix through the real
 * production facades only (the run-owned project copy, the run-input
 * snapshot, the real data-plane activations, the real plan acceptance,
 * the real stage-iteration controller and the real wait-entry
 * controller), the simulated restarts go through the ordinary
 * `PipelineV2RunStateSink.open`, and the runner's own preflight is the
 * real one (the fake CLI transport is the only fake). No LLM, no Docker
 * Helper, no launcher credential, no sleeps.
 */
import { chmodSync, mkdirSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import type { AuthFetcher, CliRunner, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import {
  continuePipelineV2Stage,
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
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";

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
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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

const RUN_ID = "continue-run";
const EXPECTED_LAUNCHER_ID = "dhl_continue";
const INITIAL_BUDGET = 2;

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

async function makeHarness(prefix: string): Promise<Harness> {
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
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
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
 * The honest stage-wait prefix through the real production facades only:
 * the run-owned project copy, the run-input snapshot, the planning
 * execution (settled unbound), the real plan acceptance, generation 1 /
 * iteration 1, the planning transition, the stage execution back to the
 * planning state, and the real wait entry declaring `continue_stage`
 * (plus `revise_task`). Returns the directories only.
 */
async function drivePrefix(
  harness: Harness,
  waitActions: ReadonlyArray<{ id: string; to: string }> = [
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "architect" },
  ],
  options: { bare?: boolean; runId?: string } = {},
): Promise<void> {
  const runId = options.runId ?? RUN_ID;
  clockValue = 0;
  const pipeline = await loadPipelineV2(harness.bundle);
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
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
  if (options.bare === true) {
    // the bare waiting boundary: the run waits at the entry state with no
    // execution and no generation at all
    await enterPipelineV2Wait({
      runRoot,
      sink: rec as never,
      reason: "stage_iteration_limit_exhausted",
      actions: waitActions,
    });
    return;
  }
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

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

interface Script {
  onAuth?: () => void | Promise<void>;
  /** Delivers the signal inside the auth callback (post-run-root). */
  signalAtAuth?: "SIGINT" | "SIGTERM";
  runCode?: number;
  runTimedOut?: boolean;
  onSignal?: ((signal: "SIGINT" | "SIGTERM") => void) | null;
}

function fakeCli(script: Script, sessionCreates: string[]): CliRunner {
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

function runnerDeps(harness: Harness, script: Script = {}, sessionCreates: string[] = []): PipelineV2RunnerDeps {
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
    cli: fakeCli(script, sessionCreates),
    fetchAuth,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "tester-secret" },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    onSignal:
      script.onSignal === null
        ? undefined
        : (installed) => {
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

async function runContinue(
  harness: Harness,
  options: Partial<{ runId: string; waitIndex: number; additionalIterations: number; configRoot: string; launcherId: string }> = {},
  script: Script = {},
  depsOverrides: Partial<PipelineV2RunnerDeps> = {},
): Promise<{ outcome: PipelineV2RunOutcome; diagnostics: string[]; sessionCreates: string[] }> {
  const sessionCreates: string[] = [];
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(
      {
        runId: options.runId ?? RUN_ID,
        waitIndex: options.waitIndex ?? 1,
        additionalIterations: options.additionalIterations ?? 2,
        configRoot: options.configRoot ?? harness.configRoot,
        launcherId: options.launcherId ?? EXPECTED_LAUNCHER_ID,
      },
      { ...runnerDeps(harness, script, sessionCreates), ...depsOverrides },
    );
  } finally {
    captured.restore();
  }
  return { outcome, diagnostics: captured.errors, sessionCreates };
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
  expectedStatus: "waiting" | "active" | "failed" | "success" | "cleanup_failed" = "waiting",
): FailureOutcome {
  const failed = expectFailure(outcome);
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  // the post-run-root failure shape carries no reason field
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect(failed.state?.status).toBe(expectedStatus);
  return failed;
}

// --- C0 ---------------------------------------------------------------------

test("C0: the runner derives the full intervention policy from the durable state after the reopen", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-c0-");
  await drivePrefix(harness);
  const prefixState = await readDurableState(harness);
  const { outcome, diagnostics } = await runContinue(harness);
  const failed = expectFailure(outcome);
  expect(failed.reason).toBe("worker_failed");
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  expect(failed.state?.status).toBe("failed");
  // the diagnostics name no user path and no manifest body
  expect(diagnostics.join("\n")).not.toContain("userdata");
  expect(diagnostics.join("\n")).not.toContain("project-source");

  // the durable projection: the full intervention suffix + the successor
  const state = await readDurableState(harness);
  const wait0 = state.waits[0]!;
  expect(wait0.index).toBe(1);
  expect(wait0.intent?.intent_sha256).toBeDefined();
  expect(wait0.response?.action_id).toBe("continue_stage");
  expect(state.grants).toHaveLength(1);
  expect(state.grants[0]).toMatchObject({
    generation_index: 1,
    wait_index: 1,
    additional_iterations: 2,
    intent_sha256: wait0.intent?.intent_sha256,
  });
  const generation = state.generations[0]!;
  expect(generation.stage_id).toBe("stage-1");
  expect(generation.closed).toBeUndefined();
  expect(generation.iterations[0]?.closed).toEqual({ by: "grant", wait_index: 1, closed_transition_count: 2 });
  expect(generation.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
  const execution3 = state.executions[2];
  expect(execution3).toMatchObject({
    index: 3,
    state_id: "dev_entry",
    execution_role: "stage",
    iteration_index: 2,
    phase: "failed",
  });
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "worker_failed" });
  // exactly twelve durable commits: five intervention + seven resume
  expect(state.revision).toBe(prefixState.revision + 12);
  // the loader round-trip
  expect(await readDurableState(harness)).toEqual(state);
});

// --- C5 / crash retry -------------------------------------------------------

test("C5: after the durable completed boundary the runner reconstructs the exact intent and repeats no intervention command", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-c5-");
  await drivePrefix(harness);
  // the completed intervention boundary built by the existing public
  // facades only (the intervention facade stops before any resume)
  clockValue = 0;
  const preSink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot = preSink.snapshot as PipelineV2RunState;
  const openGeneration = preSnapshot.generations[0]!;
  const fixtureIntent = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 1,
    stage_id: openGeneration.stage_id,
    expected_plan_sha256: openGeneration.plan_sha256,
    additional_iterations: 2,
  });
  await applyPipelineV2ContinueStageIntervention({
    pipeline: await loadPipelineV2(preSnapshot.pipeline.bundle_root),
    runRoot: harness.runRoot,
    sink: preSink,
    intent: fixtureIntent,
    initialBudget: openGeneration.initial_budget,
  });
  const boundary = preSink.snapshot as PipelineV2RunState;
  expect(boundary.waits[0]?.intent?.intent_sha256).toBe(fixtureIntent.sha256);
  expect(boundary.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(boundary.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
  expect(boundary.executions).toHaveLength(2);

  // the new reopen happens inside the runner; the runner reconstructs the
  // exact intent digest itself
  const { outcome, diagnostics } = await runContinue(harness);
  const failed = expectFailure(outcome);
  expect(failed.reason).toBe("worker_failed");
  expect(diagnostics.join("\n")).not.toContain("userdata");

  // only the resume commands happened: seven durable commits, no
  // intervention command repeated, and the projection up to the successor
  // execution is never rewritten
  const state = await readDurableState(harness);
  expect(state.revision).toBe(boundary.revision + 7);
  expect(state.waits).toEqual(boundary.waits);
  expect(state.grants).toEqual(boundary.grants);
  expect(state.generations).toEqual(boundary.generations);
  expect(state.task_revisions).toEqual(boundary.task_revisions);
  expect(state.plan_revisions).toEqual(boundary.plan_revisions);
  expect(state.pipeline).toEqual(boundary.pipeline);
  expect(state.executions).toHaveLength(3);
  expect(state.executions[2]).toMatchObject({
    index: 3,
    state_id: "dev_entry",
    execution_role: "stage",
    iteration_index: 2,
    phase: "failed",
  });
  expect(await readDurableState(harness)).toEqual(state);
});

// --- derivation refusals ----------------------------------------------------

test("derivation refusals happen before any pipeline, profile, authority or runtime work", async () => {
  // wrong wait index: no record carries index 2
  const wrongIndex = await makeHarness("pipeline-v2-continue-runner-wrong-idx-");
  await drivePrefix(wrongIndex);
  let sessionCreates: string[] = [];
  const wrongOutcome = expectPostRunRootFailure((await runContinue(wrongIndex, { waitIndex: 2 }, {}, {})).outcome, wrongIndex);
  sessionCreates = (await runContinue(wrongIndex, { waitIndex: 2 })).sessionCreates;
  expect(sessionCreates).toEqual([]);
  expect(wrongOutcome.state?.waits).toHaveLength(1);
  const wrongDurable = await readDurableState(wrongIndex);
  expect(wrongDurable.status).toBe("waiting");
  expect(wrongDurable.waits[0]?.response).toBeUndefined();

  // the wait index option shape failures stay preflight (no run root)
  const zero = expectFailure((await runContinue(wrongIndex, { waitIndex: 0 })).outcome);
  expect(zero.runId).toBe("");
  expect(zero.runRoot).toBeNull();
  expect(zero.state).toBeNull();
  expect(Object.keys(zero).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);

  // the target wait is not the last: a second wait was entered later
  const twoWaits = await makeHarness("pipeline-v2-continue-runner-two-waits-");
  await drivePrefix(twoWaits);
  clockValue = 0;
  const preSink2 = await PipelineV2RunStateSink.open({ stateRoot: twoWaits.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot2 = preSink2.snapshot as PipelineV2RunState;
  const openGeneration2 = preSnapshot2.generations[0]!;
  const intent2 = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 1,
    stage_id: openGeneration2.stage_id,
    expected_plan_sha256: openGeneration2.plan_sha256,
    additional_iterations: 2,
  });
  await applyPipelineV2ContinueStageIntervention({
    pipeline: await loadPipelineV2(preSnapshot2.pipeline.bundle_root),
    runRoot: twoWaits.runRoot,
    sink: preSink2,
    intent: intent2,
    initialBudget: openGeneration2.initial_budget,
  });
  const pipeline2 = await loadPipelineV2(preSnapshot2.pipeline.bundle_root);
  // the run-owned input snapshot already exists on disk; the honest
  // restoration path is the public restoration helper over the durable
  // snapshot records (never a fresh user re-snapshot)
  const runInputs2 = await mintRestoredRunInputsSnapshot(
    pipeline2,
    twoWaits.runRoot,
    join(twoWaits.runRoot, "project"),
    preSnapshot2.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  );
  const accepted2: AcceptedStateOutput[] = [];
  const activation3 = await prepareActivationData(pipeline2, runInputs2, accepted2, "dev_entry", 3);
  const rec2 = commandSink(
    await PipelineV2RunStateSink.open({ stateRoot: twoWaits.stateRoot, runId: RUN_ID, now: nextTick }),
    [],
  );
  await rec2.dispatch({
    kind: "start_agent_execution",
    stateId: "dev_entry",
    profile: "coder",
    ...startRoleArgs(pipeline2, "dev_entry", rec2.snapshot),
  });
  await rec2.dispatch({ kind: "agent_data_prepared" });
  await rec2.dispatch({ kind: "agent_execution_session_created", sessionId: "exec-3" });
  await rec2.dispatch({ kind: "agent_tool_session_created", sessionId: "tool-3" });
  await rec2.dispatch({ kind: "agent_running" });
  const records3 = await acceptActivationOutputs(pipeline2, activation3);
  await rec2.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records3.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await rec2.dispatch({ kind: "agent_cleanup_completed" });
  await rec2.dispatch({
    kind: "transition_committed",
    step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
    executionIndex: 3,
  });
  accepted2.push(...records3);
  await enterPipelineV2Wait({
    runRoot: twoWaits.runRoot,
    sink: rec2 as never,
    reason: "stage_iteration_limit_exhausted",
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  const notLast = expectPostRunRootFailure((await runContinue(twoWaits, { waitIndex: 1 })).outcome, twoWaits);
  expect(notLast.state?.waits).toHaveLength(2);
  expect(notLast.state?.waits[0]?.response?.action_id).toBe("continue_stage");
  const notLastDurable = await readDurableState(twoWaits);
  expect(notLastDurable.waits[1]?.response).toBeUndefined();

  // no exact continue_stage declaration in the target wait
  const ordinary = await makeHarness("pipeline-v2-continue-runner-ordinary-");
  await drivePrefix(ordinary, [{ id: "ship_all", to: "dev_entry" }]);
  const ordinaryOutcome = expectPostRunRootFailure((await runContinue(ordinary)).outcome, ordinary);
  expect(ordinaryOutcome.state?.waits[0]?.actions.map((action) => action.id)).toEqual(["ship_all"]);

  // no open generation: the bare waiting boundary at the entry state
  // (create run, wait, no execution and no generation — loader-valid;
  // several simultaneously open generations are impossible loader-valid
  // because the reducer never opens a second generation while one is open)
  const bare = await makeHarness("pipeline-v2-continue-runner-bare-");
  await drivePrefix(bare, [{ id: "continue_stage", to: "architect" }, { id: "revise_task", to: "architect" }], { bare: true });
  const bareOutcome = expectPostRunRootFailure((await runContinue(bare)).outcome, bare);
  expect(bareOutcome.state?.generations).toEqual([]);

  // an existing durable intent with a different digest: the caller
  // additionalIterations disagree with the accepted intent
  const mismatch = await makeHarness("pipeline-v2-continue-runner-mismatch-");
  await drivePrefix(mismatch);
  clockValue = 0;
  const preSink3 = await PipelineV2RunStateSink.open({ stateRoot: mismatch.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot3 = preSink3.snapshot as PipelineV2RunState;
  const openGeneration3 = preSnapshot3.generations[0]!;
  const intent3 = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 1,
    stage_id: openGeneration3.stage_id,
    expected_plan_sha256: openGeneration3.plan_sha256,
    additional_iterations: 3,
  });
  await applyPipelineV2ContinueStageIntervention({
    pipeline: await loadPipelineV2(preSnapshot3.pipeline.bundle_root),
    runRoot: mismatch.runRoot,
    sink: preSink3,
    intent: intent3,
    initialBudget: openGeneration3.initial_budget,
  });
  const mismatched = expectPostRunRootFailure((await runContinue(mismatch, { additionalIterations: 2 })).outcome, mismatch, "active");
  expect(mismatched.state?.waits[0]?.intent?.intent_sha256).toBe(intent3.sha256);
  const mismatchDurable = await readDurableState(mismatch);
  // the intervention facade answered the wait and stopped before the
  // resume; the mismatch refusal changed nothing
  expect(mismatchDurable.status).toBe("active");
  expect(mismatchDurable.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(mismatchDurable.grants[0]?.additional_iterations).toBe(3);
  expect(mismatchDurable.revision).toBe(preSink3.snapshot!.revision);
});

// --- contract battery -------------------------------------------------------

test("contract battery: malformed options, additionalIterations bounds and hostile extras", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-contract-");
  await drivePrefix(harness);
  const preflightShape = (outcome: PipelineV2RunOutcome): void => {
    const failed = expectFailure(outcome);
    expect(failed.runId).toBe("");
    expect(failed.runRoot).toBeNull();
    expect(failed.state).toBeNull();
    expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  };
  for (const options of [
    { runId: RUN_ID, waitIndex: 1, additionalIterations: 0, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: -2, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: 1.5, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: Number.MAX_SAFE_INTEGER + 1, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: undefined, configRoot: harness.configRoot },
    { runId: "../escape", waitIndex: 1, additionalIterations: 2, configRoot: harness.configRoot },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: 2, configRoot: "relative/config" },
    { runId: RUN_ID, waitIndex: 1, additionalIterations: 2 },
  ] as ReadonlyArray<Record<string, unknown>>) {
    const captured = captureDiagnostics();
    let outcome: PipelineV2RunOutcome;
    try {
      outcome = await continuePipelineV2Stage(
        options as never,
        runnerDeps(harness),
      );
    } finally {
      captured.restore();
    }
    preflightShape(outcome);
  }
  // the durable state is untouched by every contract refusal
  const state = await readDurableState(harness);
  expect(state.status).toBe("waiting");
  expect(state.waits[0]?.response).toBeUndefined();

  // hostile extra fields are never read: the capture reads exactly the
  // five validated fields
  let trapHits = 0;
  const readKeys: string[] = [];
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      additionalIterations: 2,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
      pipelineRoot: "/forbidden",
      runRoot: "/forbidden",
      stageId: "forbidden",
      expectedPlanSha256: "forbidden",
      initialBudget: 99,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        trapHits += 1;
        readKeys.push(String(property));
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // the forbidden fields are never read
  for (const forbidden of ["pipelineRoot", "runRoot", "stageId", "expectedPlanSha256", "initialBudget", "actionId"]) {
    expect(readKeys).not.toContain(forbidden);
  }
  // the first five reads are exactly the validated fields in the contract
  // order; every later read is one of the same validated scalars (the
  // derivation and the assembly re-read the captured options)
  expect(readKeys.slice(0, 5)).toEqual(["runId", "waitIndex", "additionalIterations", "configRoot", "launcherId"]);
  for (const key of readKeys) {
    expect(["runId", "waitIndex", "additionalIterations", "configRoot", "launcherId"]).toContain(key);
  }
});

// --- signal before the intervention ----------------------------------------

test("a signal accepted before the facade yields the signal outcome with zero intervention commands", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-signal-");
  await drivePrefix(harness);
  const prefixState = await readDurableState(harness);
  const sessionCreates: string[] = [];
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(
      {
        runId: RUN_ID,
        waitIndex: 1,
        additionalIterations: 2,
        configRoot: harness.configRoot,
        launcherId: EXPECTED_LAUNCHER_ID,
      },
      runnerDeps(harness, { signalAtAuth: "SIGINT" }, sessionCreates),
    );
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(130);
  expect(outcome.reason).toBe("signal_sigint");
  expect(outcome.runId).toBe(RUN_ID);
  expect(outcome.runRoot).toBe(harness.runRoot);
  expect(outcome.state?.status).toBe("waiting");
  // zero intervention commands, zero session effects, zero durable writes
  expect(sessionCreates).toEqual([]);
  const state = await readDurableState(harness);
  expect(state.revision).toBe(prefixState.revision);
  expect(state.status).toBe("waiting");
  expect(state.waits[0]?.response).toBeUndefined();
  expect(state.grants).toEqual([]);

  // SIGTERM maps to 143
  const outcome2 = (
    await runContinue(harness, {}, { signalAtAuth: "SIGTERM" })
  ).outcome;
  expect(outcome2.ok).toBe(false);
  expect(outcome2.exitCode).toBe(143);
  expect(outcome2.reason).toBe("signal_sigterm");
});

// --- capture contract -------------------------------------------------------

test("capture contract: every continue option field is read exactly once in the fixed order", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-readcount-");
  await drivePrefix(harness);
  const readKeys: string[] = [];
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      additionalIterations: 2,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
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
    outcome = await continuePipelineV2Stage(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // each of the five fields is read exactly once, in the contract order
  expect(readKeys).toEqual(["runId", "waitIndex", "additionalIterations", "configRoot", "launcherId"]);
});

test("capture contract: a mutating additionalIterations getter cannot change the recorded grant", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-policy-mut-");
  await drivePrefix(harness);
  let policyReads = 0;
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      additionalIterations: 2,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "additionalIterations") {
          policyReads += 1;
          return policyReads === 1 ? 2 : 3;
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  const state = await readDurableState(harness);
  // the validated policy (2) is the only value the run ever sees
  expect(state.grants[0]?.additional_iterations).toBe(2);
});

test("capture contract: a mutating runId getter cannot redirect the continuation to another run", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-run-select-");
  await drivePrefix(harness, undefined, { runId: "run-a" });
  await drivePrefix(harness, undefined, { runId: "run-b" });
  let runIdReads = 0;
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      waitIndex: 1,
      additionalIterations: 2,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "runId") {
          runIdReads += 1;
          return runIdReads === 1 ? "run-a" : "run-b";
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // only the first validated run was continued
  const stateA = parsePipelineV2RunState(await readFile(join(harness.stateRoot, "pipeline-runs", "run-a", "state.json"), "utf8"));
  const stateB = parsePipelineV2RunState(await readFile(join(harness.stateRoot, "pipeline-runs", "run-b", "state.json"), "utf8"));
  expect(stateA.status).toBe("failed");
  expect(stateA.grants).toHaveLength(1);
  expect(stateA.waits[0]?.response?.action_id).toBe("continue_stage");
  // the second run stays untouched waiting
  expect(stateB.status).toBe("waiting");
  expect(stateB.waits[0]?.response).toBeUndefined();
  expect(stateB.grants).toEqual([]);
  expect(outcome.runId).toBe("run-a");
});

test("capture contract: a mutating deps.now getter cannot change the run clock", async () => {
  const harness = await makeHarness("pipeline-v2-continue-runner-deps-mut-");
  await drivePrefix(harness);
  let nowReads = 0;
  const validatedClock = nextTick;
  const replacementClock = () => new Date(Date.UTC(2030, 0, 1));
  const base = runnerDeps(harness);
  const hostileDeps = new Proxy(base as unknown as Record<string, unknown>, {
    get(target, property, receiver) {
      if (property === "now") {
        nowReads += 1;
        return nowReads === 1 ? validatedClock : replacementClock;
      }
      return Reflect.get(target, property, receiver);
    },
  });
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await continuePipelineV2Stage(
      {
        runId: RUN_ID,
        waitIndex: 1,
        additionalIterations: 2,
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
  const state = await readDurableState(harness);
  // every durable timestamp came from the validated clock
  expect(state.updated_at.startsWith("2026-01-01")).toBe(true);
  expect(nowReads).toBe(1);
});

// --- captured helper config -------------------------------------------------

test("capture contract: a mutating helperConfig getter cannot swap the socket or the credential after validation", async () => {
  const resumePipelineV2 = (await import("../src/pipeline_v2_runner.ts")).resumePipelineV2;
  const entries: ReadonlyArray<readonly [string, (harness: Harness, deps: PipelineV2RunnerDeps) => Promise<PipelineV2RunOutcome>]> = [
    [
      "resume",
      (harness, deps) =>
        resumePipelineV2(
          { runId: RUN_ID, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
          deps,
        ),
    ],
    [
      "continue-stage",
      (harness, deps) =>
        continuePipelineV2Stage(
          {
            runId: RUN_ID,
            waitIndex: 1,
            additionalIterations: 2,
            configRoot: harness.configRoot,
            launcherId: EXPECTED_LAUNCHER_ID,
          },
          deps,
        ),
    ],
  ];
  for (const [name, runCall] of entries) {
    const harness = await makeHarness(`pipeline-v2-capture-helper-${name}-`);
    await drivePrefix(harness);
    const credDirA = join(harness.root, "cred-a", "docker-helper");
    await mkdir(credDirA, { recursive: true, mode: 0o700 });
    const credA = join(credDirA, "credential.token");
    await writeFile(credA, "token-a-canary\n", { mode: 0o600 });
    const credDirB = join(harness.root, "cred-b", "docker-helper");
    await mkdir(credDirB, { recursive: true, mode: 0o700 });
    const credB = join(credDirB, "credential.token");
    await writeFile(credB, "token-b-canary\n", { mode: 0o600 });
    const socketA = "/run/socket-a.sock";
    const socketB = "/run/socket-b.sock";
    let socketReads = 0;
    let credentialReads = 0;
    const hostileHelperConfig = new Proxy(
      { socketPath: socketA, credentialFile: credA } as Record<string, unknown>,
      {
        get(target, property, receiver) {
          if (property === "socketPath") {
            socketReads += 1;
            return socketReads === 1 ? socketA : socketB;
          }
          if (property === "credentialFile") {
            credentialReads += 1;
            return credentialReads === 1 ? credA : credB;
          }
          return Reflect.get(target, property, receiver);
        },
      },
    );
    const authCalls: Array<{ socket: string; token: string }> = [];
    const sessionCreates: string[] = [];
    const captured = captureDiagnostics();
    let outcome: PipelineV2RunOutcome;
    try {
      outcome = await runCall(harness, {
        ...runnerDeps(harness, {}, sessionCreates),
        fetchAuth: async (socket: string, token: string) => {
          authCalls.push({ socket, token });
          return {
            status: 200,
            body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
          };
        },
        helperConfig: hostileHelperConfig as never,
      });
    } finally {
      captured.restore();
    }
    expect(outcome.ok).toBe(false);
    // the authority used the validated socket and the validated credential
    expect(authCalls).toEqual([{ socket: socketA, token: "token-a-canary" }]);
    // each nested field was read exactly once (at the capture)
    expect(socketReads).toBe(1);
    expect(credentialReads).toBe(1);
    // the replacement B was never read, never called, never published
    expect(captured.errors.join("\n")).not.toContain(socketB);
    expect(captured.errors.join("\n")).not.toContain(credB);
    expect(captured.errors.join("\n")).not.toContain("token-b-canary");
    expect(JSON.stringify(outcome)).not.toContain(socketB);
    // the session creates (continue-stage only) carry the validated endpoint
    const endpoints = sessionCreates
      .map((args) => {
        const index = args.split(" ").indexOf("--endpoint");
        return index >= 0 ? args.split(" ")[index + 1] : "";
      })
      .filter((endpoint) => endpoint !== "");
    for (const endpoint of endpoints) {
      expect(endpoint).toBe(socketA);
    }
  }
});

// --- export surface and source scan ----------------------------------------

test("the export surface gains exactly one runtime key and the module implements no second machinery", async () => {
  const namespace = (await import("../src/pipeline_v2_runner.ts")) as Record<string, unknown>;
  // the runtime export surface: interfaces are types and invisible at
  // runtime; the surface carries exactly four function keys (the fresh
  // runner and the three existing-run intervention entrypoints — resume,
  // continue-stage and revise-task) plus the planning-run-plan entrypoint
  expect(Object.keys(namespace).sort()).toEqual([
    "continuePipelineV2Stage",
    "resumePipelineV2",
    "resumePipelineV2PlanningRunPlan",
    "revisePipelineV2Task",
    "runPipelineV2",
  ]);
  expect(Object.keys(namespace).filter((key) => key === "continuePipelineV2Stage")).toEqual(["continuePipelineV2Stage"]);
  expect(typeof namespace["continuePipelineV2Stage"]).toBe("function");

  const source = await readFile(
    new URL("../src/pipeline_v2_runner.ts", import.meta.url).pathname,
    "utf8",
  );
  // no second state parser, layout verifier, compiler, plan restore,
  // reducer or intervention-suffix implementation
  for (const banned of [
    "reducePipelineV2RunCommand(",
    "validatePipelineV2RunState(",
    "restorePipelineV2AcceptedRunPlan",
    "openPipelineV2ContinuedStage",
    "acceptPipelineV2ContinueStageIntent",
    "applyPipelineV2ContinueStageIntervention(",
    "preparePlanRevisionManifest(",
    "prepareTaskRevisionManifest(",
    "preparePipelineV2WaitRequest(",
    "publishPipelineV2WaitRequest(",
    "publishPipelineV2WaitResponse(",
    '"plan_intent_accepted"',
    '"iteration_grant_recorded"',
    '"stage_iteration_closed"',
    '"wait_response_recorded"',
    '"stage_iteration_opened"',
  ]) {
    expect(source.includes(banned)).toBe(false);
  }
});
