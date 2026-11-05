/**
 * The production-path battery of the durable stage activation context.
 *
 * Every test drives the real production runner (`runPipelineV2`,
 * `revisePipelineV2Task`) over a fake Docker Helper transport — the same
 * runner, coordinator, data plane, reducer and store the CLI uses; the real
 * LLM and Docker Helper are not needed. The battery proves:
 *
 * - the first stage activation receives the exact bytes of task revision 1
 *   (`REVISION-ONE`), and after `revise-task` the next stage activation
 *   receives the exact bytes of task revision 2 (`REVISION-TWO`);
 * - the original protected root pipeline input is no longer the value of
 *   the `stage_task` port;
 * - the execution documents carry the deterministic content-free
 *   orchestration context (plan revision/digest, stage identity, task
 *   identity, generation and iteration indexes) and never the task body;
 * - the durable stage-task binding is written exactly once per stage
 *   execution and the task body never enters `state.json`, the inspect
 *   projection or any public document;
 * - a multi-task stage fails closed before the stage Session with zero
 *   Sessions of its own;
 * - an input artifact modified during the worker run is never accepted
 *   (the post-worker verification refuses before output acceptance);
 * - the empty-task-body revise attempt refuses before any effect.
 *
 * The two runner invocations are also the restart proof: each command
 * reopens the durable run through the ordinary sink factory, so the
 * revision-2 delivery across the revise boundary is a real restart path.
 */
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import {
  runPipelineV2,
  revisePipelineV2Task,
  type PipelineV2RunOptions,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import type { PipelineV2WorkerOperationRunner } from "../src/pipeline_v2_docker_runtime.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2AgentExecutionState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { inspectPipelineV2Run } from "../src/pipeline_v2_inspect.ts";
import {
  prepareActivationData,
  type PreparedActivationData,
  type RunInputBinding,
} from "../src/pipeline_v2_runtime.ts";
import {
  resolvePipelineV2StageTaskContext,
} from "../src/pipeline_v2_stage_task.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { prepareWaitIntent } from "../src/pipeline_v2_run_plan_manifests.ts";
import { applyPipelineV2ContinueStageIntervention } from "../src/pipeline_v2_continue_stage_intervention_controller.ts";

const INITIAL_BUDGET = 2;
const EXPECTED_LAUNCHER_ID = "dhl_stage_task_runtime";

function pipelineYaml(stageTaskSource: boolean): string {
  const taskInput = stageTaskSource
    ? "    inputs:\n      - id: task\n        source:\n          stage_task: {}\n"
    : "    inputs:\n      - id: task\n        source:\n          pipeline_input: task\n";
  return `
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
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
      plan_ready:
        stage_position: 1
        initial_budget: ${INITIAL_BUDGET}
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: planner2
      role: planning
      plan_output: plan2
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
      plan_ready:
        stage_position: 1
        initial_budget: ${INITIAL_BUDGET}

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
${taskInput}    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: planner2
  - id: planner2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan2
        type: json
        schema: schemas/plan2.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

interface Proposal {
  schema_version: 1;
  kind: "run_plan_proposal";
  stages: Array<{ id: string; template: string; tasks: Array<{ id: string; depends_on: string[] }> }>;
  new_tasks: Array<{ id: string; body: string }>;
}

function oneTaskProposal(body: string, tasks = 1): Proposal {
  const pointers = Array.from({ length: tasks }, (_, index) => ({ id: `task-${String.fromCharCode(97 + index)}`, depends_on: [] }));
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages: [{ id: "stage-1", template: "development", tasks: pointers }],
    new_tasks: pointers.map((pointer) => ({ id: pointer.id, body })),
  };
}

let clockValue = 0;
function nextTick(): Date {
  clockValue += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
}

const ROOTS: string[] = [];
afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

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

async function makeHarness(prefix: string, runId: string, stageTaskSource = true): Promise<Harness> {
  clockValue = 0;
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(stageTaskSource));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
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
  await writeFile(join(sources, "task.md"), "ORIGINAL-PROTECTED-TASK-BODY\n");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true, mode: 0o700 });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  return {
    root,
    stateRoot,
    bundle,
    configRoot,
    sources,
    projectSource,
    credentialFile,
    runRoot: join(stateRoot, "pipeline-runs", runId),
    statePath: join(stateRoot, "pipeline-runs", runId, "state.json"),
  };
}

const EMPTY_CAPTURE = {
  status: "unavailable" as const,
  byteCount: 0,
  tailBytes: new Uint8Array(0),
  truncatedDaemon: false,
  truncatedLocal: false,
};

interface DepsOptions {
  /** The initial planning proposal (default: one task with the given body). */
  proposal?: Proposal;
  /** A one-shot sabotage executed inside the first stage worker run. */
  tamperOnRunCall?: (runRoot: string, mounts: ReadonlyArray<{ source?: string; target?: string }>) => Promise<void>;
}

let sessionCounter = 0;

function runnerDeps(harness: Harness, runId: string, options: DepsOptions = {}): PipelineV2RunnerDeps {
  const runRootKnown = harness.runRoot;
  const proposal = options.proposal ?? oneTaskProposal("REVISION-ONE");
  const cli = async (args: readonly string[]) => {
    if (args[0] === "session" && args[1] === "create") {
      sessionCounter += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_str_${sessionCounter}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_str_${sessionCounter}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    return { code: 0 };
  };
  let runCalls = 0;
  let tamperFired = false;
  const operationRunner: PipelineV2WorkerOperationRunner = async (request) => {
    runCalls += 1;
    const body = JSON.parse(request.body) as {
      mounts?: Array<{ source?: string; target?: string }>;
    };
    const mounts = body.mounts ?? [];
    const tamperTarget = mounts.find((mount) => {
      if (mount.target !== "/pipeline/inputs" || typeof mount.source !== "string") {
        return false;
      }
      const activationDirectory = mount.source.split("/")[1] ?? "";
      return activationDirectory.endsWith("-dev_entry");
    });
    if (options.tamperOnRunCall !== undefined && tamperTarget !== undefined && !tamperFired) {
      tamperFired = true;
      await options.tamperOnRunCall(runRootKnown, mounts);
    }
    const outputsMount = mounts.find((mount) => mount.target === "/pipeline/outputs");
    if (outputsMount !== undefined && typeof outputsMount.source === "string") {
      const dash = outputsMount.source.split("/")[1] ?? "";
      const dashIndex = dash.indexOf("-");
      const activationIndex = dashIndex === -1 ? "" : dash.slice(0, dashIndex);
      const stateId = dashIndex === -1 ? dash : dash.slice(dashIndex + 1);
      if (stateId === "architect" || stateId === "planner2") {
        // The first architect activation proposes the initial plan; every
        // later planning activation is the pointer-only replanning form.
        const active: Proposal =
          stateId === "architect" && activationIndex === "1"
            ? proposal
            : {
                schema_version: 1,
                kind: "run_plan_proposal",
                stages: proposal.stages,
                new_tasks: [],
              };
        const dir = join(runRootKnown, outputsMount.source);
        await mkdir(dir, { recursive: true });
        await writeFile(join(dir, stateId === "architect" ? "plan" : "plan2"), JSON.stringify(active), { mode: 0o600 });
      }
    }
    return { outcome: "completed" as const, capture: EMPTY_CAPTURE };
  };
  return {
    cli: cli as never,
    operationRunner,
    fetchAuth: (async () => ({
      status: 200,
      body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
    })) as never,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "coder-secret" },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    now: nextTick,
    randomId: () => runId,
  };
}

function runOptions(harness: Harness): PipelineV2RunOptions {
  return {
    pipelineRoot: harness.bundle,
    configRoot: harness.configRoot,
    projectSourcePath: harness.projectSource,
    inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }] as RunInputBinding[],
    launcherId: EXPECTED_LAUNCHER_ID,
  };
}

function expectWaiting(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(false);
  expect(outcome.waiting).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect("reason" in outcome).toBe(false);
  return outcome.state as PipelineV2RunState;
}

function expectFailed(outcome: PipelineV2RunOutcome, reason: string): PipelineV2RunState {
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect((outcome as { reason?: string }).reason).toBe(reason);
  return outcome.state as PipelineV2RunState;
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

async function stageInputBytes(harness: Harness, activationIndex: number): Promise<string> {
  return await readFile(
    join(harness.runRoot, "activations", `${activationIndex}-dev_entry`, "data", "inputs", "task"),
    "utf8",
  );
}

async function executionDocument(harness: Harness, activationIndex: number): Promise<string> {
  return await readFile(
    join(harness.runRoot, "activations", `${activationIndex}-dev_entry`, "data", "inputs", ".orchestrator", "execution.md"),
    "utf8",
  );
}

function stageExecutions(state: PipelineV2RunState): PipelineV2AgentExecutionState[] {
  return state.executions.filter(
    (execution): execution is PipelineV2AgentExecutionState =>
      execution.type === "agent" && execution.state_id === "dev_entry",
  );
}

test("revision 1 reaches the first stage worker, revision 2 the next one, with the durable binding and the orchestration context", async () => {
  const runId = "stage-task-e2e";
  const harness = await makeHarness("stage-task-e2e", runId);
  const first = await runPipelineV2(runOptions(harness), runnerDeps(harness, runId));
  const firstState = expectWaiting(first);
  expect(firstState.revision).toBe(22);
  expect(await stageInputBytes(harness, 2)).toBe("REVISION-ONE");
  const firstDoc = await executionDocument(harness, 2);
  expect(firstDoc).toContain("## Orchestration context");
  expect(firstDoc).toContain("plan_revision: 1");
  expect(firstDoc).toContain('stage_id: "stage-1"');
  expect(firstDoc).toContain("stage_position: 1");
  expect(firstDoc).toContain('stage_template: "development"');
  expect(firstDoc).toContain('task_id: "task-a"');
  expect(firstDoc).toContain("task_revision: 1");
  expect(firstDoc).toContain("generation_index: 1");
  expect(firstDoc).toContain("iteration_index: 1");
  // The task body never enters the execution document.
  expect(firstDoc).not.toContain("REVISION-ONE");
  const firstStages = stageExecutions(firstState);
  expect(firstStages).toHaveLength(1);
  expect(firstStages[0]?.stage_task).toEqual({
    task_id: "task-a",
    task_revision: 1,
    task_manifest_sha256: expect.any(String),
    body_sha256: expect.any(String),
  });
  // The planning execution records no stage-task binding.
  const planner = firstState.executions.find((execution) => execution.state_id === "architect");
  expect(planner?.type === "agent" ? planner.stage_task : undefined).toBeUndefined();

  const second = await revisePipelineV2Task(
    {
      runId,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "REVISION-TWO",
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    },
    runnerDeps(harness, runId),
  );
  const secondState = expectWaiting(second);
  expect(secondState.revision).toBe(47);
  expect(secondState.task_revisions).toHaveLength(2);
  expect(await stageInputBytes(harness, 4)).toBe("REVISION-TWO");
  const secondDoc = await executionDocument(harness, 4);
  expect(secondDoc).toContain("plan_revision: 2");
  expect(secondDoc).toContain("task_revision: 2");
  expect(secondDoc).toContain("generation_index: 2");
  expect(secondDoc).toContain("iteration_index: 1");
  expect(secondDoc).not.toContain("REVISION-TWO");
  const secondStages = stageExecutions(secondState);
  expect(secondStages).toHaveLength(2);
  expect(secondStages[1]?.stage_task?.task_revision).toBe(2);
  expect(secondStages[1]?.stage_task?.task_id).toBe("task-a");

  // The task bodies never enter the durable state document.
  const rawState = await readFile(harness.statePath, "utf8");
  expect(rawState).not.toContain("REVISION-ONE");
  expect(rawState).not.toContain("REVISION-TWO");
  // The registered project source stays byte-identical.
  const projectEntries = await readdir(harness.projectSource);
  expect(projectEntries).toEqual([]);
  // The inspect projection of the waiting run carries no task body.
  const inspect = await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot });
  expect(inspect.ok).toBe(true);
  expect(JSON.stringify(inspect)).not.toContain("REVISION-ONE");
  expect(JSON.stringify(inspect)).not.toContain("REVISION-TWO");
}, 30000);

test("the body bytes survive verbatim: final newline, no final newline and non-ASCII", async () => {
  for (const [label, body] of [
    ["final-newline", "REVISION-ONE\n"],
    ["no-final-newline", "REVISION-ONE"],
    ["non-ascii", "задача τask ✓ — révisé\n"],
  ] as const) {
    const runId = `stage-task-bytes-${label}`;
    const harness = await makeHarness(`stage-task-bytes-${label}`, runId);
    const outcome = await runPipelineV2(runOptions(harness), runnerDeps(harness, runId, { proposal: oneTaskProposal(body) }));
    expectWaiting(outcome);
    const raw = await stageInputBytes(harness, 2);
    expect(raw).toBe(body);
    // The digest recorded durably is the digest of the exact bytes.
    const state = await readDurableState(harness);
    const stage = stageExecutions(state)[0];
    const { stageTaskBodyDigest } = await import("../src/pipeline_v2_stage_task.ts");
    expect(stage?.stage_task?.body_sha256).toBe(stageTaskBodyDigest(Buffer.from(body, "utf8")));
  }
}, 40000);

test("a multi-task stage fails closed before the stage Session with zero stage Sessions", async () => {
  const runId = "stage-task-multi";
  const harness = await makeHarness("stage-task-multi", runId);
  const sessionsBefore = sessionCounter;
  const outcome = await runPipelineV2(
    runOptions(harness),
    runnerDeps(harness, runId, { proposal: oneTaskProposal("REVISION-ONE", 2) }),
  );
  const state = expectFailed(outcome, "stage_task_invalid");
  expect(state.status).toBe("failed");
  const stage = stageExecutions(state)[0];
  expect(stage?.failure_reason).toBe("stage_task_invalid");
  expect(stage?.phase).toBe("failed");
  // The planning execution used its two sessions; the stage activation
  // created none.
  expect(sessionCounter - sessionsBefore).toBe(2);
  expect(stage?.execution_session_id).toBeUndefined();
  expect(stage?.tool_session_id).toBeUndefined();
}, 30000);

test("an input artifact modified during the worker run is never accepted", async () => {
  const runId = "stage-task-tamper-run";
  const harness = await makeHarness("stage-task-tamper-run", runId);
  const outcome = await runPipelineV2(
    runOptions(harness),
    runnerDeps(harness, runId, {
      tamperOnRunCall: async (runRoot, mounts) => {
        const inputsMount = mounts.find((mount) => mount.target === "/pipeline/inputs");
        if (inputsMount === undefined || typeof inputsMount.source !== "string") {
          return;
        }
        // The worker (or anything with its authority) rewrites the
        // immutable stage-task input during the run.
        await writeFile(join(runRoot, inputsMount.source, "task"), "TAMPERED-DURING-RUN", { mode: 0o600 });
      },
    }),
  );
  const state = expectFailed(outcome, "stage_task_invalid");
  const stage = stageExecutions(state)[0];
  expect(stage?.failure_reason).toBe("stage_task_invalid");
  // The outputs were never accepted: the tampered input refused them.
  const raw = await readFile(harness.statePath, "utf8");
  expect(raw.match(/"kind":"agent_outputs_accepted"/g) ?? []).toHaveLength(0);
}, 30000);

test("the pre-Session verification seam refuses a deleted, modified or substituted stage-task input", async () => {
  // The seam contract is artifact integrity: a prepared stage activation
  // plus a resolved context in, a typed refusal out. The resolver only
  // accepts an active running run (the production call happens at the
  // stage execution start), so this test first drives the honest run to
  // its waiting boundary, then moves the reopened run back to a durable
  // active boundary through the real continue-stage intervention — the
  // coordinator wiring (resolve → prepare → verify on the honest run) is
  // the e2e proof above; here the fresh activation and the verification
  // battery run against a real resolver context.
  const runId = "stage-task-verify";
  const harness = await makeHarness("stage-task-verify", runId);
  const first = await runPipelineV2(runOptions(harness), runnerDeps(harness, runId));
  expectWaiting(first);
  const pipeline = await loadPipelineV2(harness.bundle);
  const durable = await readDurableState(harness);
  const generation = durable.generations[durable.generations.length - 1];
  expect(generation?.stage_id).toBe("stage-1");
  const sink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId });
  const intent = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: runId,
    wait_index: 1,
    stage_id: generation!.stage_id,
    expected_plan_sha256: generation!.plan_sha256,
    additional_iterations: 1,
  });
  await applyPipelineV2ContinueStageIntervention({
    pipeline,
    runRoot: harness.runRoot,
    sink,
    intent,
    initialBudget: generation!.initial_budget,
  });
  const active = await readDurableState(harness);
  expect(active.status).toBe("active");
  const context = await resolvePipelineV2StageTaskContext({
    pipeline,
    state: active,
    runRoot: harness.runRoot,
  });
  expect(context.task_id).toBe("task-a");
  expect(context.task_revision).toBe(1);
  // The next unused activation index after the honest run (1 architect,
  // 2 stage, 3 planner2, 4 stage) is 5. The run-owned input snapshot
  // already exists on disk; the honest restoration path is the public
  // restoration helper over the durable snapshot records.
  const { mintRestoredRunInputsSnapshot } = await import("../src/pipeline_v2_runtime.ts");
  const activation: PreparedActivationData = await prepareActivationData(
    pipeline,
    await mintRestoredRunInputsSnapshot(
      pipeline,
      harness.runRoot,
      join(harness.runRoot, "project"),
      active.inputs.map((entry) => ({
        id: entry.id,
        type: entry.type,
        protected: entry.protected,
        digest: entry.digest,
      })),
    ),
    [],
    "dev_entry",
    5,
    context,
  );
  expect(await stageInputBytes(harness, 5)).toBe("REVISION-ONE");
  const { verifyStageTaskActivationInput } = await import("../src/pipeline_v2_runtime.ts");
  // The freshly published artifact verifies.
  await verifyStageTaskActivationInput(pipeline, activation, context);
  // A deleted artifact refuses.
  await rm(join(activation.inputs_root, "task"));
  await expect(verifyStageTaskActivationInput(pipeline, activation, context)).rejects.toThrow(/is missing/);
  // A modified artifact refuses.
  await writeFile(join(activation.inputs_root, "task"), "MODIFIED", { mode: 0o600 });
  await expect(verifyStageTaskActivationInput(pipeline, activation, context)).rejects.toThrow(
    /does not carry the digest/,
  );
  // A substituted final component (a non-regular file at the fixed path) refuses.
  await rm(join(activation.inputs_root, "task"));
  await mkdir(join(activation.inputs_root, "task"), { recursive: true });
  await expect(verifyStageTaskActivationInput(pipeline, activation, context)).rejects.toThrow(
    /is not a regular non-symlink file/,
  );
}, 30000);

test("an empty task body is refused by the existing revise contract before any effect", async () => {
  const runId = "stage-task-empty";
  const harness = await makeHarness("stage-task-empty", runId);
  const outcome = await runPipelineV2(runOptions(harness), runnerDeps(harness, runId));
  expectWaiting(outcome);
  const before = await readDurableState(harness);
  const revise = await revisePipelineV2Task(
    {
      runId,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "",
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    },
    runnerDeps(harness, runId),
  );
  expect(revise.ok).toBe(false);
  expect(revise.exitCode).toBe(1);
  expect((revise as { reason?: string }).reason).toBe("invalid_options");
  const after = await readDurableState(harness);
  expect(after.revision).toBe(before.revision);
  expect(after.task_revisions).toHaveLength(1);
}, 30000);
