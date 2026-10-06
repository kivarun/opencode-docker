/**
 * The dedicated planning-run-plan runner entrypoint
 * (`resumePipelineV2PlanningRunPlan`): the external parameters are exactly
 * the run id, the caller policy scalars `stageId` and `initialBudget` and
 * the standard resume configuration — every internal handoff parameter
 * (the plan acceptance, the replanned stage opening, the committed
 * planning transition and the coordinator resume) belongs to the existing
 * composition controller. The proofs drive the honest prefix through the
 * real production facades only (the run-owned project copy, the run-input
 * snapshot, the real data-plane activations, the real plan acceptance,
 * the real stage-iteration controller, the real wait-entry controller and
 * the real revise-task interventions), the simulated restarts go through
 * the ordinary `PipelineV2RunStateSink.open`, and the runner's own
 * preflight is the real one (the fake CLI transport is the only fake).
 * No LLM, no Docker Helper, no launcher credential, no sleeps. The runner
 * API stays unwired: the CLI, `main.ts`, the default pipeline and the
 * automatic stage/budget selection policy are untouched.
 */
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import type { AuthFetcher, CliRunner } from "../src/docker_helper.ts";
import {
  resumePipelineV2PlanningRunPlan,
  type PipelineV2PlanningRunPlanOptions,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { applyPipelineV2PlanningRunPlanHandoff } from "../src/pipeline_v2_planning_run_plan_handoff_controller.ts";
import { enterPipelineV2Wait } from "../src/pipeline_v2_wait_controller.ts";
import { restorePipelineV2RuntimeContext } from "../src/pipeline_v2_resume_context.ts";
import {
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
    - id: review
      entry_state: review_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
    - state_id: planner2
      role: planning
      plan_output: plan2
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: review_entry
      role: stage
      stage_template: review

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
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
        to: planner2
  - id: planner2
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: review_entry
  - id: review_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const RUN_ID = "planning-run-plan-run";
const OTHER_RUN_ID = "planning-run-plan-other";
const EXPECTED_LAUNCHER_ID = "dhl_planning";
const INITIAL_BUDGET = 2;

const P1_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

const P2_POINTER_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [],
};

const P3_TWO_STAGE_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-c", depends_on: [] }, { id: "task-d", depends_on: ["task-c"] }] },
  ],
  new_tasks: [
    { id: "task-c", body: "Body C" },
    { id: "task-d", body: "Body D" },
  ],
};

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
  await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  for (const profile of ["architect", "coder"]) {
    await writeFile(
      join(configRoot, "profiles", `${profile}.yaml`),
      [
        "schema_version: 1",
        "image: ghcr.io/example/worker:1",
        `opencode_config: opencode/${profile}.json`,
        "env:",
        `  MODEL_API_KEY:`,
        `    from_env: ${profile.toUpperCase()}_SOURCE_VAR_1`,
        "    required: true",
        "",
      ].join("\n"),
    );
    await writeFile(join(configRoot, "opencode", `${profile}.json`), JSON.stringify({ model: "glm53-flash" }));
  }
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

/** One planning activation through the real runtime data plane and reducer. */
async function runPlanningActivation(
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  sink: { dispatch: (command: never) => Promise<void>; snapshot: PipelineV2RunState | null },
  stateId: string,
  profile: string,
  outputId: string,
  executionIndex: number,
  proposal: unknown,
): Promise<AcceptedStateOutput[]> {
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile,
    ...startRoleArgs(pipeline, stateId, sink.snapshot),
  } as never);
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` },
    { kind: "agent_running" },
  ] as never[]) {
    await sink.dispatch(command);
  }
  await writeFile(join(activation.outputs_root, outputId), JSON.stringify(proposal), { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, activation);
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  } as never);
  await sink.dispatch({ kind: "agent_cleanup_completed" } as never);
  return [...accepted, ...records];
}

/** One zero-output stage execution recorded through the real reducer. */
async function runRawStageActivation(
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  sink: { dispatch: (command: never) => Promise<void>; snapshot: PipelineV2RunState | null },
  stateId: string,
  profile: string,
  executionIndex: number,
): Promise<void> {
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile,
    ...startRoleArgs(pipeline, stateId, sink.snapshot),
  } as never);
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ] as never[]) {
    await sink.dispatch(command);
  }
}

async function commitTransition(
  sink: { dispatch: (command: never) => Promise<void> },
  from: string,
  to: string,
  executionIndex: number,
): Promise<void> {
  await sink.dispatch({
    kind: "transition_committed",
    step: { from, outcome: "completed", to, transition_index: 0 },
    executionIndex,
  } as never);
}

interface PrefixCoordinates {
  /** The durable revision at the settled-unbound planning boundary. */
  revision: number;
}

/**
 * The honest durable prefix with two complete revise_task cycles and two
 * plan stages, built only through the existing facades, the real reducer
 * and the runtime data plane. Returns filesystem coordinates and scalars
 * only — no pipeline object, no compiled plan, no proposal, no accepted
 * output and no snapshot can ever be handed to the runner (the runner
 * loads the pipeline itself from the durable bundle root).
 */
async function driveTwoCyclePrefix(harness: Harness, runId: string = RUN_ID): Promise<PrefixCoordinates> {
  clockValue = 0;
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  await rm(runRoot, { recursive: true, force: true });
  await mkdir(runRoot, { mode: 0o700 });
  const pipeline = await loadPipelineV2(harness.bundle);
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const rec = commandSink(sink, []);
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
  } as never);
  let accepted: AcceptedStateOutput[] = [];
  accepted = await runPlanningActivation(pipeline, runInputs, accepted, rec as never, "architect", "architect", "plan", 1, P1_PROPOSAL);
  const acceptedPlan = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot, sink: rec as never });
  await ensurePipelineV2StageIteration({ compiledPlan: acceptedPlan.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: rec as never });
  await commitTransition(rec as never, "architect", "dev_entry", 1);
  await runRawStageActivation(pipeline, runInputs, accepted, rec as never, "dev_entry", "coder", 2);
  await commitTransition(rec as never, "dev_entry", "planner2", 2);
  await enterPipelineV2Wait({
    runRoot,
    sink: rec as never,
    reason: "stage_iteration_limit_exhausted",
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await applyPipelineV2ReviseTaskIntervention({
    pipeline,
    runRoot,
    sink: rec as never,
    runId,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "Body A revised",
  });

  // Planning execution 3 on architect (the pointer-only r2 proposal), then
  // the first handoff selects the FIRST stage and cycles the cursor back
  // to dev_entry for the second stage cycle.
  {
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
    const restored = await restorePipelineV2RuntimeContext(pipeline, reopened.snapshot as PipelineV2RunState, runRoot);
    accepted = await runPlanningActivation(pipeline, restored.run_inputs, restored.accepted_outputs, reopened as never, "architect", "architect", "plan", restored.next_execution_index, P2_POINTER_PROPOSAL);
    await applyPipelineV2PlanningRunPlanHandoff({
      pipeline,
      runRoot,
      sink: reopened as never,
      stageId: "stage-1",
      initialBudget: INITIAL_BUDGET,
    });
    await runRawStageActivation(pipeline, restored.run_inputs, restored.accepted_outputs, reopened as never, "dev_entry", "coder", 4);
    await commitTransition(reopened as never, "dev_entry", "planner2", 4);
    await enterPipelineV2Wait({
      runRoot,
      sink: reopened as never,
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "planner2" },
      ],
    });
    await applyPipelineV2ReviseTaskIntervention({
      pipeline,
      runRoot,
      sink: reopened as never,
      runId,
      waitIndex: 2,
      taskId: "task-b",
      taskBody: "Body B revised",
    });
  }

  // Planning execution 5 on planner2 (the two-stage r3 proposal).
  {
    const reopened = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
    const state = reopened.snapshot as PipelineV2RunState;
    const restored = await restorePipelineV2RuntimeContext(pipeline, state, runRoot);
    await runPlanningActivation(pipeline, restored.run_inputs, restored.accepted_outputs, reopened as never, "planner2", "architect", "plan2", restored.next_execution_index, P3_TWO_STAGE_PROPOSAL);
  }

  const finalState = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
  if (finalState.executions.length !== finalState.transitions.length + 1) {
    throw new Error(`the prefix did not reach the settled-unbound boundary (${finalState.executions.length} executions, ${finalState.transitions.length} transitions)`);
  }
  return { revision: finalState.revision };
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

/** Full deterministic filesystem fingerprint of the run tree. */
async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
      const path = join(dir, entry.name);
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = await stat(path);
      if (info.isDirectory()) {
        lines.push(`${relative}/ dir ${info.mode.toString(8)}`);
        await walk(path, relative);
      } else {
        const bytes = info.isFile() ? (await readFile(path)).toString("base64") : "non-file";
        lines.push(`${relative} ${info.mode.toString(8)} ${bytes}`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

interface Script {
  onAuth?: () => void | Promise<void>;
  /** Delivers the signal inside the auth callback (post-run-root). */
  signalAtAuth?: "SIGINT" | "SIGTERM";
  onSignal?: ((signal: "SIGINT" | "SIGTERM") => void) | null;
}

function fakeCli(sessionCreates: string[], sessionDeletes: string[]): CliRunner {
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
      sessionDeletes.push(args[args.length - 1] ?? "");
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    if (first === "pull") {
      return { code: 1, stderr: "PULL-FAILED-BY-TEST" };
    }
    if (first === "run") {
      return { code: 0 };
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
    cli: fakeCli(sessionCreates, sessionDeletes),
    fetchAuth,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "coder-secret", ARCHITECT_SOURCE_VAR_1: "architect-secret" },
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

async function runPlanning(
  harness: Harness,
  options: Partial<Omit<PipelineV2PlanningRunPlanOptions, "runId" | "stageId" | "initialBudget"> & { runId: string; stageId: string; initialBudget: number }> = {},
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
    outcome = await resumePipelineV2PlanningRunPlan(
      {
        runId: options.runId ?? RUN_ID,
        stageId: options.stageId ?? "stage-2",
        initialBudget: options.initialBudget ?? INITIAL_BUDGET,
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

function expectPreflightFailure(outcome: PipelineV2RunOutcome): void {
  const failed = expectFailure(outcome);
  expect(failed.runId).toBe("");
  expect(failed.runRoot).toBeNull();
  expect(failed.state).toBeNull();
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
}

function expectPostRunRootFailure(
  outcome: PipelineV2RunOutcome,
  harness: Harness,
  expectedStatus: "waiting" | "active" | "failed" | "success" | "cleanup_failed" = "active",
): FailureOutcome {
  const failed = expectFailure(outcome);
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  // the post-run-root failure shape carries no reason field
  expect(Object.keys(failed).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect(failed.state?.status).toBe(expectedStatus);
  return failed;
}

// --- C0 ----------------------------------------------------------------------

test("C0: the runner reopens the run, loads the pipeline from the durable bundle root and composes the handoff suffix plus the seven resume commands", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-c0-");
  const prefix = await driveTwoCyclePrefix(harness);
  const { outcome, diagnostics, sessionCreates, sessionDeletes } = await runPlanning(harness);
  const failed = expectFailure(outcome);
  // the ordinary worker failure of the resumed stage execution — never a
  // refusal and never a pipeline mismatch
  expect(failed.reason).toBe("worker_failed");
  expect(failed.runId).toBe(RUN_ID);
  expect(failed.runRoot).toBe(harness.runRoot);
  expect(failed.state?.status).toBe("failed");
  expect("refused" in failed).toBe(false);
  // the diagnostics name no user path and no proposal/task body
  expect(diagnostics.join("\n")).not.toContain("userdata");
  expect(diagnostics.join("\n")).not.toContain("project-source");
  expect(diagnostics.join("\n")).not.toContain("Body ");
  expect(JSON.stringify(outcome)).not.toContain("Body ");

  // the durable projection of the exact fourteen-command sequence: the
  // seven handoff commands, then the seven resume commands
  const state = await readDurableState(harness);
  expect(state.revision).toBe(prefix.revision + 14);
  expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
    "task-a@1",
    "task-b@1",
    "task-a@2",
    "task-b@2",
    "task-c@1",
    "task-d@1",
  ]);
  expect(state.plan_revisions.map((record) => record.revision)).toEqual([1, 2, 3]);
  expect(state.waits.map((wait) => wait.response?.action_id)).toEqual(["revise_task", "revise_task"]);
  // the handoff closed generation 2 by replanned and opened generation 3
  // on the selected non-first stage with the caller budget
  expect(state.generations).toHaveLength(3);
  const generation2 = state.generations[1]!;
  expect(generation2.stage_id).toBe("stage-1");
  expect(generation2.closed).toEqual({ by: "replanned", closed_transition_count: 4 });
  const generation3 = state.generations[2]!;
  expect(generation3.index).toBe(3);
  expect(generation3.stage_id).toBe("stage-2");
  expect(generation3.stage_position).toBe(2);
  expect(generation3.template_id).toBe("review");
  expect(generation3.initial_budget).toBe(INITIAL_BUDGET);
  expect(generation3.opened_transition_count).toBe(4);
  expect(generation3.closed).toBeUndefined();
  expect(generation3.open_iteration).toEqual({ index: 1, opened_transition_count: 4 });
  // the committed planning transition into the selected stage's entry
  expect(state.cursor).toEqual({ current_state: "review_entry", transition_count: 5 });
  expect(state.transitions).toHaveLength(5);
  expect(state.transitions[4]).toEqual({
    index: 0,
    from: "planner2",
    outcome: "completed",
    to: "review_entry",
    execution_index: 5,
  });
  // the resumed stage execution 6 on the selected entry
  expect(state.executions).toHaveLength(6);
  const successor = state.executions[5]!;
  expect(successor).toMatchObject({
    index: 6,
    state_id: "review_entry",
    execution_role: "stage",
    iteration_index: 1,
    phase: "failed",
    failure_reason: "worker_failed",
  });
  if (successor.type !== "agent") {
    throw new Error("expected the successor execution to be an agent execution");
  }
  expect(successor.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "worker_failed" });
  // one new session pair only (the resumed execution), cleaned exactly
  // once each and tool-first
  expect(sessionCreates).toHaveLength(2);
  expect(sessionDeletes).toEqual(["dhs_2", "dhs_1"]);
  // the pipeline came only from the durable bundle root
  expect(state.pipeline.bundle_root).toBe(harness.bundle);
  // the loader round-trip
  expect(await readDurableState(harness)).toEqual(state);
});

// --- C1 / crash retry --------------------------------------------------------

test("C1: after the durable handoff the runner repeats no handoff command and records only the seven resume commands", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-c1-");
  await driveTwoCyclePrefix(harness);
  // The handoff completes durably through the public facade on a reopened
  // run; the resume has not started (the crash seam).
  clockValue = 0;
  const preSink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot = preSink.snapshot as PipelineV2RunState;
  const handoff = await applyPipelineV2PlanningRunPlanHandoff({
    pipeline: await loadPipelineV2(preSnapshot.pipeline.bundle_root),
    runRoot: harness.runRoot,
    sink: preSink,
    stageId: "stage-2",
    initialBudget: INITIAL_BUDGET,
  });
  expect(handoff.stage_id).toBe("stage-2");
  expect(handoff.to_state).toBe("review_entry");
  const boundary = preSink.snapshot as PipelineV2RunState;
  expect(boundary.status).toBe("active");
  expect(boundary.executions).toHaveLength(5);
  expect(boundary.transitions).toHaveLength(5);
  const boundaryFingerprint = await fingerprint(harness.runRoot);

  const { outcome, sessionCreates, sessionDeletes } = await runPlanning(harness);
  const failed = expectFailure(outcome);
  expect(failed.reason).toBe("worker_failed");

  // only the seven resume commands: no handoff command repeated, and the
  // handoff projection is never rewritten
  const state = await readDurableState(harness);
  expect(state.revision).toBe(boundary.revision + 7);
  expect(state.waits).toEqual(boundary.waits);
  expect(state.task_revisions).toEqual(boundary.task_revisions);
  expect(state.plan_revisions).toEqual(boundary.plan_revisions);
  expect(state.generations).toEqual(boundary.generations);
  expect(state.grants).toEqual(boundary.grants);
  expect(state.transitions).toEqual(boundary.transitions);
  expect(state.executions).toHaveLength(6);
  expect(state.executions[5]).toMatchObject({
    index: 6,
    state_id: "review_entry",
    execution_role: "stage",
    iteration_index: 1,
    phase: "failed",
  });
  expect(sessionCreates).toHaveLength(2);
  expect(sessionDeletes).toEqual(["dhs_2", "dhs_1"]);
  expect(await readDurableState(harness)).toEqual(state);
  void boundaryFingerprint;
});

// --- capture contract --------------------------------------------------------

test("capture contract: every planning option field is read exactly once in the fixed order and hostile extras are never read", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-readcount-");
  await driveTwoCyclePrefix(harness);
  const readKeys: string[] = [];
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
      pipelineRoot: "/never-read",
      projectSourcePath: "/never-read",
      waitIndex: 1,
      intent: "never-read",
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
    outcome = await resumePipelineV2PlanningRunPlan(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // each of the five fields is read exactly once, in the contract order;
  // every forbidden/internal field stays unread
  expect(readKeys).toEqual(["runId", "stageId", "initialBudget", "configRoot", "launcherId"]);
});

test("capture contract: mutating stageId and initialBudget getters cannot change the recorded handoff policy", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-policy-mut-");
  await driveTwoCyclePrefix(harness);
  let stageReads = 0;
  let budgetReads = 0;
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "stageId") {
          stageReads += 1;
          return stageReads === 1 ? "stage-2" : "stage-1";
        }
        if (property === "initialBudget") {
          budgetReads += 1;
          return budgetReads === 1 ? INITIAL_BUDGET : 9;
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await resumePipelineV2PlanningRunPlan(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  const state = await readDurableState(harness);
  // the validated policy (stage-2, budget 2) is the only policy the run
  // ever sees: generation 3 is bound to the selected non-first stage
  expect(state.generations[2]?.stage_id).toBe("stage-2");
  expect(state.generations[2]?.template_id).toBe("review");
  expect(state.generations[2]?.initial_budget).toBe(INITIAL_BUDGET);
  expect(state.cursor).toEqual({ current_state: "review_entry", transition_count: 5 });
});

test("capture contract: a mutating runId getter cannot redirect the continuation to another run", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-run-select-");
  await driveTwoCyclePrefix(harness, RUN_ID);
  await driveTwoCyclePrefix(harness, OTHER_RUN_ID);
  let runIdReads = 0;
  const hostileOptions = new Proxy(
    {
      runId: RUN_ID,
      stageId: "stage-2",
      initialBudget: INITIAL_BUDGET,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    } as Record<string, unknown>,
    {
      get(target, property, receiver) {
        if (property === "runId") {
          runIdReads += 1;
          return runIdReads === 1 ? RUN_ID : OTHER_RUN_ID;
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await resumePipelineV2PlanningRunPlan(hostileOptions as never, runnerDeps(harness));
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  // only the first validated run was continued
  const stateA = parsePipelineV2RunState(await readFile(join(harness.stateRoot, "pipeline-runs", RUN_ID, "state.json"), "utf8"));
  const stateB = parsePipelineV2RunState(await readFile(join(harness.stateRoot, "pipeline-runs", OTHER_RUN_ID, "state.json"), "utf8"));
  expect(stateA.status).toBe("failed");
  expect(stateA.generations).toHaveLength(3);
  expect(stateA.executions).toHaveLength(6);
  // the second run stays untouched at its settled-unbound planning boundary
  expect(stateB.status).toBe("active");
  expect(stateB.executions).toHaveLength(5);
  expect(stateB.transitions).toHaveLength(4);
  expect(stateB.plan_revisions.map((record) => record.revision)).toEqual([1, 2]);
  expect(outcome.runId).toBe(RUN_ID);
});

test("capture contract: a mutation delivered during the pending auth and a replacement clock never reach the composed call", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-pending-mut-");
  await driveTwoCyclePrefix(harness);
  const validatedClock = nextTick;
  const replacementClock = (): Date => new Date(Date.UTC(2030, 0, 1));
  let nowReads = 0;
  const mutatedDuringAuth = {
    runId: RUN_ID,
    stageId: "stage-2",
    initialBudget: INITIAL_BUDGET,
    configRoot: harness.configRoot,
    launcherId: EXPECTED_LAUNCHER_ID,
  };
  const base = runnerDeps(harness, {
    onAuth: () => {
      // mutate everything the caller owns while the runner is inside the
      // pending auth await; the capture already held the values
      mutatedDuringAuth.runId = OTHER_RUN_ID;
      mutatedDuringAuth.stageId = "stage-1";
      mutatedDuringAuth.initialBudget = 9;
    },
  });
  const hostileDeps = new Proxy(base as unknown as Record<string, unknown>, {
    get(target, property, receiver) {
      if (property === "now") {
        nowReads += 1;
        return nowReads === 1
          ? (): Date => validatedClock()
          : replacementClock;
      }
      return Reflect.get(target, property, receiver);
    },
  });
  const captured = captureDiagnostics();
  let outcome: PipelineV2RunOutcome;
  try {
    outcome = await resumePipelineV2PlanningRunPlan(mutatedDuringAuth, hostileDeps as never);
  } finally {
    captured.restore();
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.reason).toBe("worker_failed");
  const state = await readDurableState(harness);
  // the captured policy (stage-2, budget 2) and the first validated run
  // are the only values the runner ever used
  expect(state.generations[2]?.stage_id).toBe("stage-2");
  expect(state.generations[2]?.initial_budget).toBe(INITIAL_BUDGET);
  expect(state.cursor).toEqual({ current_state: "review_entry", transition_count: 5 });
  expect(state.executions[5]?.state_id).toBe("review_entry");
  // the replacement clock was never read: the validated one served, so
  // every durable timestamp stayed on the 2026 clock
  expect(nowReads).toBe(1);
  expect(state.updated_at.startsWith("2026-01-01")).toBe(true);
});

// --- invalid options matrix (preflight, before any run-root access) ----------

test("invalid options matrix: every refusal happens before any run-root access, auth or session", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-contract-");
  await driveTwoCyclePrefix(harness);
  const prefixBytes = await readFile(harness.statePath);
  const valid: Record<string, unknown> = {
    runId: RUN_ID,
    stageId: "stage-2",
    initialBudget: INITIAL_BUDGET,
    configRoot: harness.configRoot,
    launcherId: EXPECTED_LAUNCHER_ID,
  };
  const cases: unknown[] = [
    null,
    "options",
    [],
    42,
    { ...valid, runId: "../escape" },
    { ...valid, runId: "" },
    { ...valid, runId: 42 },
    { ...valid, stageId: "../escape" },
    { ...valid, stageId: "" },
    { ...valid, stageId: 42 },
    { ...valid, initialBudget: 0 },
    { ...valid, initialBudget: -1 },
    { ...valid, initialBudget: 1.5 },
    { ...valid, initialBudget: Number.MAX_SAFE_INTEGER + 1 },
    { ...valid, initialBudget: undefined },
    { ...valid, initialBudget: "2" },
    { ...valid, configRoot: "relative/config" },
    { ...valid, configRoot: "" },
    { ...valid, configRoot: 42 },
    { ...valid, launcherId: "not-dhl" },
    { runId: RUN_ID, stageId: "stage-2", initialBudget: INITIAL_BUDGET },
  ];
  let sessionCreates: string[] = [];
  for (const options of cases) {
    sessionCreates = [];
    let authCalls = 0;
    const captured = captureDiagnostics();
    let outcome: PipelineV2RunOutcome;
    const deps = runnerDeps(harness, {}, sessionCreates, []);
    const countedFetchAuth: AuthFetcher = async (socket, token) => {
      authCalls += 1;
      return await deps.fetchAuth(socket, token);
    };
    try {
      outcome = await resumePipelineV2PlanningRunPlan(options as never, { ...deps, fetchAuth: countedFetchAuth });
    } finally {
      captured.restore();
    }
    expectPreflightFailure(outcome);
    expect(authCalls).toBe(0);
    expect(sessionCreates).toEqual([]);
  }
  // the durable state is byte-identical after every refused call
  expect(await readFile(harness.statePath)).toEqual(prefixBytes);
});

// --- existing-run precedence -------------------------------------------------

test("existing-run precedence: missing state and an invalid run-root layout are shared failures with zero sessions", async () => {
  // an existing run root without a durable state document: the shared
  // post-run-root failure shape with a null state and zero sessions
  const harness = await makeHarness("pipeline-v2-planning-runner-missing-state-");
  const missing = expectFailure((await runPlanning(harness)).outcome);
  expect(missing.runId).toBe(RUN_ID);
  expect(missing.runRoot).toBe(harness.runRoot);
  expect(Object.keys(missing).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect(missing.state).toBeNull();
  expect((await runPlanning(harness)).sessionCreates).toEqual([]);

  // a pre-existing run root with an unsafe mode is rejected unchanged
  const layout = await makeHarness("pipeline-v2-planning-runner-layout-");
  await rm(layout.runRoot, { recursive: true, force: true });
  await mkdir(layout.runRoot, { mode: 0o755 });
  const layoutOutcome = await runPlanning(layout);
  expectPreflightFailure(layoutOutcome.outcome);
  const info = await stat(layout.runRoot);
  expect(info.mode & 0o777).toBe(0o755);
});

test("existing-run precedence: pipeline load failure, pipeline identity mismatch, profile and auth failures carry no sessions and no handoff dispatch", async () => {
  // a broken pipeline bundle fails at the shared pipeline load
  const broken = await makeHarness("pipeline-v2-planning-runner-broken-pipeline-");
  await driveTwoCyclePrefix(broken);
  const brokenBefore = await readDurableState(broken);
  await writeFile(join(broken.bundle, "pipeline.yaml"), "schema_version: 2\nentry_state: [broken\n");
  const brokenRun = await runPlanning(broken);
  const brokenFailed = expectPostRunRootFailure(brokenRun.outcome, broken, "active");
  expect(brokenFailed.state?.revision).toBe(brokenBefore.revision);
  expect(brokenRun.sessionCreates).toEqual([]);
  expect(brokenRun.authCalls).toBe(0);
  expect((await readDurableState(broken)).revision).toBe(brokenBefore.revision);

  // a semantically valid but different bundle fails the durable pipeline
  // identity inside the composed controller — the ordinary runner mapping,
  // no reclassification, no own retry logic
  const mismatched = await makeHarness("pipeline-v2-planning-runner-pipeline-mismatch-");
  await driveTwoCyclePrefix(mismatched);
  const mismatchedBefore = await readDurableState(mismatched);
  await writeFile(join(mismatched.bundle, "prompts", "architect.md"), "CHANGED-PROMPT-BODY\n");
  const mismatchedRun = await runPlanning(mismatched);
  const mismatchedFailed = expectPostRunRootFailure(mismatchedRun.outcome, mismatched, "active");
  expect(mismatchedFailed.state?.revision).toBe(mismatchedBefore.revision);
  expect(mismatchedRun.sessionCreates).toEqual([]);
  expect((await readDurableState(mismatched)).revision).toBe(mismatchedBefore.revision);

  // a missing profile configuration root fails at the shared profile load
  const profiled = await makeHarness("pipeline-v2-planning-runner-profile-");
  await driveTwoCyclePrefix(profiled);
  const profiledBefore = await readDurableState(profiled);
  const profileRun = await runPlanning(profiled, { configRoot: join(profiled.root, "does-not-exist") });
  expectPostRunRootFailure(profileRun.outcome, profiled, "active");
  expect(profileRun.sessionCreates).toEqual([]);
  expect(profileRun.authCalls).toBe(0);
  expect((await readDurableState(profiled)).revision).toBe(profiledBefore.revision);

  // a failing Launcher authority fails at the shared auth boundary
  const auth = await makeHarness("pipeline-v2-planning-runner-auth-");
  await driveTwoCyclePrefix(auth);
  const authBefore = await readDurableState(auth);
  const authRun = await runPlanning(auth, {}, {}, {
    fetchAuth: (async () => ({ status: 403, body: {} })) as unknown as AuthFetcher,
  });
  expectPostRunRootFailure(authRun.outcome, auth, "active");
  expect(authRun.sessionCreates).toEqual([]);
  expect((await readDurableState(auth)).revision).toBe(authBefore.revision);
});

// --- completed-boundary wrong policy -----------------------------------------

test("wrong caller policy on the completed boundary is the typed downstream refusal, mapped without reclassification", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-wrong-policy-");
  await driveTwoCyclePrefix(harness);
  clockValue = 0;
  const preSink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId: RUN_ID, now: nextTick });
  const preSnapshot = preSink.snapshot as PipelineV2RunState;
  await applyPipelineV2PlanningRunPlanHandoff({
    pipeline: await loadPipelineV2(preSnapshot.pipeline.bundle_root),
    runRoot: harness.runRoot,
    sink: preSink,
    stageId: "stage-2",
    initialBudget: INITIAL_BUDGET,
  });
  const boundary = preSink.snapshot as PipelineV2RunState;

  // a foreign stage id on the C1 boundary
  const foreignStage = await runPlanning(harness, { stageId: "stage-1" });
  const foreignStageFailed = expectPostRunRootFailure(foreignStage.outcome, harness, "active");
  expect(foreignStageFailed.state?.revision).toBe(boundary.revision);
  expect(foreignStage.sessionCreates).toEqual([]);
  expect((await readDurableState(harness)).revision).toBe(boundary.revision);

  // a foreign caller budget on the C1 boundary
  const foreignBudget = await runPlanning(harness, { initialBudget: 3 });
  const foreignBudgetFailed = expectPostRunRootFailure(foreignBudget.outcome, harness, "active");
  expect(foreignBudgetFailed.state?.revision).toBe(boundary.revision);
  expect(foreignBudget.sessionCreates).toEqual([]);
  const afterForeignBudget = await readDurableState(harness);
  expect(afterForeignBudget.revision).toBe(boundary.revision);
  expect(afterForeignBudget.waits).toEqual(boundary.waits);
  expect(afterForeignBudget.generations).toEqual(boundary.generations);
});

// --- signal proof ------------------------------------------------------------

test("a signal accepted at the authority boundary yields the signal outcome with zero handoff and resume commands", async () => {
  const harness = await makeHarness("pipeline-v2-planning-runner-signal-");
  await driveTwoCyclePrefix(harness);
  const prefixState = await readDurableState(harness);
  const sigint = await runPlanning(harness, {}, { signalAtAuth: "SIGINT" });
  expect(sigint.outcome.ok).toBe(false);
  expect(sigint.outcome.exitCode).toBe(130);
  expect(sigint.outcome.reason).toBe("signal_sigint");
  expect(sigint.outcome.runId).toBe(RUN_ID);
  expect(sigint.outcome.runRoot).toBe(harness.runRoot);
  expect(sigint.outcome.state?.status).toBe("active");
  // zero handoff and resume commands, zero session effects, zero writes
  expect(sigint.sessionCreates).toEqual([]);
  const afterSigint = await readDurableState(harness);
  expect(afterSigint.revision).toBe(prefixState.revision);
  expect(afterSigint.status).toBe("active");
  expect(afterSigint.executions).toHaveLength(5);
  expect(afterSigint.plan_revisions.map((record) => record.revision)).toEqual([1, 2]);

  const sigterm = await runPlanning(harness, {}, { signalAtAuth: "SIGTERM" });
  expect(sigterm.outcome.ok).toBe(false);
  expect(sigterm.outcome.exitCode).toBe(143);
  expect(sigterm.outcome.reason).toBe("signal_sigterm");
  expect(sigterm.sessionCreates).toEqual([]);
  expect((await readDurableState(harness)).revision).toBe(prefixState.revision);
});

// --- export surfaces and source scan -----------------------------------------

test("the runner export surface gains exactly one new function key", async () => {
  const namespace = (await import("../src/pipeline_v2_runner.ts")) as Record<string, unknown>;
  expect(Object.keys(namespace).sort()).toEqual([
    "continuePipelineV2Stage",
    "resumePipelineV2",
    "resumePipelineV2PlanningRunPlan",
    "revisePipelineV2Task",
    "runPipelineV2",
  ]);
  expect(typeof namespace["resumePipelineV2PlanningRunPlan"]).toBe("function");
});

test("source scan: one existing-run core, one composed call, no second machinery", async () => {
  const source = await readFile(
    new URL("../src/pipeline_v2_runner.ts", import.meta.url).pathname,
    "utf8",
  );
  // the single composed call of this entrypoint
  const composedCalls = source.split("resumePipelineV2RunAfterPlanningRunPlanHandoff(").length - 1;
  expect(composedCalls).toBe(1);
  // no direct planning-acceptance/stage/transition facade call and no
  // second store/reducer/parser/serializer/digest/registry machinery
  // (the runner legitimately keeps the continue-stage intent derivation
  // and the shared state-sink import — every other machinery import is
  // banned)
  for (const banned of [
    "acceptPipelineV2PlanningRunPlan(",
    "ensurePipelineV2StageIteration(",
    "openPipelineV2ReplannedStageTransition(",
    "openPipelineV2ReplannedStage(",
    "restorePipelineV2AcceptedRunPlan(",
    "preparePipelineV2RunPlanCandidate(",
    "reducePipelineV2RunCommand(",
    "validatePipelineV2RunState(",
    "restorePipelineV2RuntimeContext(",
    "snapshotRunInputs(",
    "prepareActivationData(",
    "acceptActivationOutputs(",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_state_store",
  ]) {
    expect(source.includes(banned), `the runner must not reference ${banned}`).toBe(false);
  }
  // the existing-run core stays the single shared core: one definition
  // plus exactly the four existing-run entrypoints calling it
  expect(source.split("runExistingPipelineV2(").length - 1).toBe(5);
});
