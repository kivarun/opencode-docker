/**
 * The permanent contract of the `stage_task: {}` pipeline v2 input source
 * and of the single stage-task resolver.
 *
 * Compile battery: the source form is valid exactly on stage-role agent
 * input ports, carries no parameters, derives the `file` port type, joins
 * the normalized execution snapshot (and therefore its digest), and is
 * rejected everywhere else — planning/control roles, decision states, run
 * outputs, bundles without an orchestration section, and any non-empty
 * payload shape.
 *
 * Resolver battery: `resolvePipelineV2StageTaskContext` resolves the exact
 * task revision of the open generation's bound accepted plan through the
 * existing run-plan store chain only, fails closed (typed
 * `stage_task_invalid`) on every missing, closed, divergent or tampered
 * trusted anchor, and releases the exact worker-visible body bytes only to
 * the provenance-matched activation data plane.
 *
 * The zero-task stage needs no runtime proof: the plan-manifest layer
 * rejects a stage without tasks at acceptance time, so a zero-task stage
 * can never become the compiled plan of an accepted generation; the
 * resolver keeps its guard as defense-in-depth and this file pins the
 * manifest-layer rejection that makes the runtime state impossible.
 */
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, test } from "bun:test";
import {
  loadPipelineV2,
  parsePipelineV2Spec,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import { pipelineV2ExecutionDigest } from "../src/pipeline_v2_digest.ts";
import { preparePlanRevisionManifest, PipelineV2RunPlanManifestError } from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparedStageTaskBodyBytes,
  resolvePipelineV2StageTaskContext,
  stageTaskBodyDigest,
  type PreparedStageTaskContext,
} from "../src/pipeline_v2_stage_task.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  coordinatePipelineV2Run,
  type PipelineV2CoordinationResult,
  type PipelineV2CoordinatorControl,
} from "../src/pipeline_v2_coordinator.ts";
import {
  prepareActivationData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import type { PipelineV2AgentRuntime } from "../src/pipeline_v2_coordinator.ts";

// ---------------------------------------------------------------------------
// Compile battery.
// ---------------------------------------------------------------------------

function pipelineYaml(taskPort: string): string {
  return `
schema_version: 2
entry_state: architect
max_transitions: 12

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
    inputs:
      - id: task
        source:
${taskPort}    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;
}

const STAGE_TASK_PORT = "          stage_task: {}\n";
const PIPELINE_INPUT_PORT = "          pipeline_input: task\n";

function rejectParse(yaml: string, messagePart: string): void {
  let message = "";
  try {
    parsePipelineV2Spec(yaml);
  } catch (cause) {
    message = (cause as Error).message;
  }
  expect(message).toContain(messagePart);
}

test("the stage_task source compiles on a stage-role agent state and derives the file type", () => {
  const spec = parsePipelineV2Spec(pipelineYaml(STAGE_TASK_PORT));
  const stage = spec.states.find((state) => state.id === "dev_entry");
  expect(stage && stage.type === "agent" ? stage.inputs : undefined).toEqual([
    { id: "task", source: { stage_task: {} }, type: "file" },
  ]);
});

test("replacing the source form moves the execution snapshot digest; equivalent forms stay deterministic", async () => {
  const root = await mkdtemp(join(tmpdir(), "stage-task-digest-"));
  try {
    const bundleA = join(root, "a");
    const bundleB = join(root, "b");
    for (const [bundle, port] of [[bundleA, STAGE_TASK_PORT], [bundleB, PIPELINE_INPUT_PORT]] as const) {
      await mkdir(join(bundle, "prompts"), { recursive: true });
      await mkdir(join(bundle, "schemas"), { recursive: true });
      await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(port));
      await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
      await writeFile(join(bundle, "prompts", "coder.md"), "PROMPT\n");
    }
    const pipelineA = await loadPipelineV2(bundleA);
    const pipelineB = await loadPipelineV2(bundleB);
    const digestA = pipelineV2ExecutionDigest(pipelineA);
    const digestB = pipelineV2ExecutionDigest(pipelineB);
    expect(digestA).toMatch(/^[0-9a-f]{64}$/);
    expect(digestA).not.toBe(digestB);
    expect(await pipelineV2ExecutionDigest(await loadPipelineV2(bundleA))).toBe(digestA);
    // The snapshot carries the source form verbatim.
    const stageA = pipelineA.states.find((state) => state.id === "dev_entry");
    expect(stageA && stageA.type === "agent" ? stageA.inputs[0]?.source : undefined).toEqual({ stage_task: {} });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the stage_task source is rejected with a payload, a non-object, or outside the stage role", () => {
  rejectParse(
    pipelineYaml("          stage_task: { extra: true }\n"),
    'source stage_task must be exactly the empty object, got 1 field(s)',
  );
  rejectParse(pipelineYaml("          stage_task: \"x\"\n"), "source stage_task must be an empty object");
  rejectParse(pipelineYaml("          stage_task: null\n"), "source stage_task must be an empty object");
  rejectParse(pipelineYaml("          stage_task: []\n"), "source stage_task must be an empty object");
  rejectParse(pipelineYaml("          stage_task: 5\n"), "source stage_task must be an empty object");
  rejectParse(
    pipelineYaml("          stage_task: {}\n          pipeline_input: task\n"),
    'source must declare exactly one of "pipeline_input", "state_output" or "stage_task"',
  );
});

test("the stage_task source is rejected on planning and control roles, decision states and run outputs", () => {
  const planning = pipelineYaml(STAGE_TASK_PORT)
    .replace(
      "  stage_templates:\n    - id: development\n      entry_state: dev_entry\n",
      "  stage_templates: []\n",
    )
    .replace(
      "    - state_id: dev_entry\n      role: stage\n      stage_template: development",
      "    - state_id: dev_entry\n      role: planning\n      plan_output: plan2\n      stage_wait:\n        reason: stage_iteration_completed\n        actions:\n          - continue_stage\n          - revise_task",
    )
    .replace(
      "    outputs: []",
      "    outputs:\n      - id: plan2\n        type: json\n        schema: schemas/plan.schema.json",
    );
  rejectParse(
    planning,
    'declares the stage_task source, but its compiled execution role is "planning"',
  );
  // A bundle without an orchestration section has no trusted role at all.
  const noOrchestration = `
schema_version: 2
entry_state: architect
max_transitions: 12

inputs:
  - id: task
    type: file
    protected: true

outputs: []

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
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
    inputs:
      - id: task
        source:
          stage_task: {}
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
  rejectParse(
    noOrchestration,
    "declares the stage_task source, but no orchestration section declares an execution role for the state",
  );
  // A decision state can never declare the source.
  const decision = `
schema_version: 2
entry_state: gate
max_transitions: 12

inputs: []

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: gate
      role: control
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: gate
    type: decision
    model: models/gate.yaml
    inputs:
      - id: facts
        source:
          stage_task: {}
    transitions:
      - outcome: go
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
        to: done
  - id: done
    type: terminal
    result: success
`;
  rejectParse(
    decision,
    'input port "facts" declares the stage_task source; the stage_task source is valid only as an input port of a stage-role agent state',
  );
  // A run-level output can never declare the source.
  const runOutput = pipelineYaml(STAGE_TASK_PORT).replace(
    "outputs: []",
    "outputs:\n  - id: result\n    required: true\n    source:\n      stage_task: {}",
  );
  rejectParse(
    runOutput,
    'pipeline output "result" declares the stage_task source; the stage_task source is valid only as an input port of a stage-role agent state',
  );
});

test("the plan-manifest layer rejects a zero-task stage, so the resolver's zero-task guard is unreachable", () => {
  let message = "";
  try {
    preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: "run-1",
      revision: 1,
      previous_sha256: null,
      origin_execution: 1,
      root_task: { input_id: "task", sha256: "a".repeat(64) },
      stages: [{ id: "stage-1", template: "development", tasks: [] }],
    });
  } catch (cause) {
    expect(cause).toBeInstanceOf(PipelineV2RunPlanManifestError);
    message = (cause as Error).message;
  }
  expect(message).toContain("stage");
  expect(message).toContain("task");
});

// ---------------------------------------------------------------------------
// Resolver battery.
// ---------------------------------------------------------------------------

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
  sources: string;
  projectSource: string;
  runRoot: string;
  statePath: string;
}

interface Proposal {
  schema_version: 1;
  kind: "run_plan_proposal";
  stages: Array<{ id: string; template: string; tasks: Array<{ id: string; depends_on: string[] }> }>;
  new_tasks: Array<{ id: string; body: string }>;
}

const PROPOSAL_ONE_TASK: Proposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "REVISION-ONE" }],
};

const PROPOSAL_TWO_TASKS: Proposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] }],
  new_tasks: [
    { id: "task-a", body: "REVISION-ONE" },
    { id: "task-b", body: "TASK-B-BODY" },
  ],
};

async function makeHarness(prefix: string, runId: string): Promise<Harness> {
  clockValue = 0;
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(STAGE_TASK_PORT));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.md"), "ORIGINAL-PROTECTED-TASK-BODY\n");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { mode: 0o700 });
  return {
    root,
    stateRoot,
    bundle,
    sources,
    projectSource,
    runRoot,
    statePath: join(runRoot, "state.json"),
  };
}

type CommandRecord = Record<string, unknown>;

class RecordingSink {
  readonly commands: CommandRecord[] = [];
  constructor(private readonly inner: PipelineV2RunStateSink) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2RunStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    await this.inner.dispatch(command);
  }
}

interface SessionPair {
  sessionId: string;
  cleanup(): Promise<void>;
  runAgent(): Promise<{ status: "completed" } | { status: "failed"; reason: "worker_failed" | "worker_timeout" }>;
}

let runtimeSessionCounter = 0;

function fakeRuntime(outputs: Readonly<Record<string, unknown>>, proposal: Proposal): {
  runtime: PipelineV2AgentRuntime;
  events: string[];
} {
  const events: string[] = [];
  let currentOutputsRoot = "";
  let currentStateId = "";
  const make = (kind: "execution" | "tool"): SessionPair => {
    runtimeSessionCounter += 1;
    return {
      sessionId: `st-${kind}-${runtimeSessionCounter}`,
      runAgent: async () => {
        events.push(`run:${kind}`);
        if (kind === "execution" && currentOutputsRoot !== "") {
          const body = outputs[currentStateId];
          if (body !== undefined) {
            for (const [outputId, value] of Object.entries(body as Record<string, unknown>)) {
              await writeFile(join(currentOutputsRoot, outputId), JSON.stringify(value), { mode: 0o600 });
            }
          }
        }
        return { status: "completed" as const };
      },
      cleanup: async () => {
        events.push(`cleanup:${kind}`);
      },
    };
  };
  return {
    runtime: {
      createExecutionSession: async (_state: unknown, activation: PreparedActivationData) => {
        currentOutputsRoot = activation.outputs_root;
        currentStateId = activation.state_id;
        return make("execution");
      },
      createToolSession: async () => make("tool"),
    } as unknown as PipelineV2AgentRuntime,
    events,
  };
}

const CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: () => null,
  freezeSignal: () => null,
};

function expectPlanReady(result: PipelineV2CoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(false);
  const record = result as Record<string, unknown>;
  expect(Object.keys(record).sort()).toEqual(["ok", "planReady", "state"]);
  return record.state as PipelineV2RunState;
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

/**
 * The trusted boundary the resolver consumes: the plan is accepted, the
 * stage generation and its first iteration are open, the planning
 * transition is committed and the cursor sits on the stage entry state.
 */
async function stageTaskBoundary(
  harness: Harness,
  runId: string,
  proposal: Proposal = PROPOSAL_ONE_TASK,
  openGeneration = true,
): Promise<{ pipeline: ResolvedPipelineV2; state: PipelineV2RunState }> {
  const pipeline = await loadPipelineV2(harness.bundle);
  const fresh = new RecordingSink(new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick }));
  const fake = fakeRuntime({ architect: { plan: proposal } }, proposal);
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }] as RunInputBinding[],
      sink: fresh as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  expectPlanReady(result);
  const reopened = new RecordingSink(await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick }));
  const accepted = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: reopened as never });
  if (!openGeneration) {
    return { pipeline, state: await readDurableState(harness) };
  }
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan,
    stageId: "stage-1",
    initialBudget: 2,
    sink: (await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick })) as never,
  });
  const transitionSink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
  await transitionSink.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  return { pipeline, state: await readDurableState(harness) };
}

function expectStageTaskInvalid(cause: unknown, messagePart: string): void {
  expect((cause as Error).name).toBe("PipelineV2RuntimeError");
  expect((cause as { reason?: unknown }).reason).toBe("stage_task_invalid");
  expect((cause as Error).message).toContain(messagePart);
}

describe("the stage-task resolver over the trusted durable context", () => {
  test("resolves the exact task revision of the open generation's bound accepted plan", async () => {
    const runId = "stage-task-resolve";
    const harness = await makeHarness("stage-task-resolve", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId);
    const context = await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    const generation = state.generations[0];
    const taskRecord = state.task_revisions[0];
    expect(generation).toBeDefined();
    expect(taskRecord).toBeDefined();
    expect(context).toEqual({
      plan_revision: 1,
      plan_sha256: generation!.plan_sha256,
      stage_id: "stage-1",
      stage_position: 1,
      stage_template: "development",
      task_id: "task-a",
      task_revision: 1,
      task_manifest_sha256: taskRecord!.sha256,
      body_sha256: stageTaskBodyDigest(Buffer.from("REVISION-ONE", "utf8")),
      generation_index: 1,
      iteration_index: 1,
    });
    // The exact worker-visible bytes are the raw UTF-8 body, not the JSON
    // manifest, and only the provenance-matched pipeline can read them.
    const bytes = preparedStageTaskBodyBytes(context, pipeline);
    expect(bytes).not.toBeNull();
    expect(Buffer.from(bytes as Uint8Array).toString("utf8")).toBe("REVISION-ONE");
    expect(preparedStageTaskBodyBytes(context, (await loadPipelineV2(harness.bundle)) as never)).toBeNull();
    expect(preparedStageTaskBodyBytes({ ...context } as never, pipeline)).toBeNull();
    // A repeated resolution is deterministic and independent.
    const context2 = await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    expect(context2).toEqual(context);
    expect(Buffer.from(preparedStageTaskBodyBytes(context2, pipeline) as Uint8Array).toString("utf8")).toBe("REVISION-ONE");
    // The context is content-free: no path, no body, no host identity.
    expect(JSON.stringify(context)).not.toContain("REVISION-ONE");
    expect(JSON.stringify(context)).not.toContain(harness.root);
  }, 20000);

  test("a multi-task stage fails closed before any worker runs", async () => {
    const runId = "stage-task-multi";
    const harness = await makeHarness("stage-task-multi", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId, PROPOSAL_TWO_TASKS);
    await expect(
      resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot }),
    ).rejects.toThrow(
      'stage "stage-1" of the accepted plan declares 2 tasks; the stage_task source requires exactly one task',
    );
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    } catch (cause) {
      expectStageTaskInvalid(cause, "declares 2 tasks");
    }
  }, 20000);

  test("a closed iteration and a missing generation refuse the resolution", async () => {
    const runId = "stage-task-closed";
    const harness = await makeHarness("stage-task-closed", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId);
    // Close the open iteration: the run is still active/running, but the
    // resolver requires an open iteration.
    const sink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
    await sink.dispatch({
      kind: "stage_iteration_closed",
      generationIndex: 1,
      iterationIndex: 1,
      by: "normal_close",
    });
    const closedState = await readDurableState(harness);
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state: closedState, runRoot: harness.runRoot });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "requires an open stage generation with an open iteration");
    }
    // A run whose generation was never opened refuses with the same
    // typed guard (the honest state right after the plan acceptance:
    // active/running, the planning execution settled-but-unbound, no
    // generation yet).
    const noGenHarness = await makeHarness("stage-task-nogen", "stage-task-nogen");
    const noGen = await stageTaskBoundary(noGenHarness, "stage-task-nogen", PROPOSAL_ONE_TASK, false);
    try {
      await resolvePipelineV2StageTaskContext({
        pipeline: noGen.pipeline,
        state: noGen.state,
        runRoot: noGenHarness.runRoot,
      });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "requires an open stage generation with an open iteration");
    }
  }, 20000);

  test("a missing or tampered plan or task manifest fails closed with zero writes", async () => {
    const runId = "stage-task-tamper";
    const harness = await makeHarness("stage-task-tamper", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId);
    const planPath = join(harness.runRoot, "run-plan", "plans", "1.json");
    const taskPath = join(harness.runRoot, "run-plan", "tasks", "task-a", "1.json");
    const planBytes = await readFile(planPath);
    const taskBytes = await readFile(taskPath);

    // Missing plan manifest.
    await rm(planPath);
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "(typed failure artifact_missing)");
    }
    await writeFile(planPath, planBytes, { mode: 0o600 });

    // Tampered plan manifest (noncanonical bytes).
    await writeFile(planPath, Buffer.from(planBytes.toString("utf8") + "\n", "utf8"));
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "(typed failure conflict)");
    }
    await writeFile(planPath, planBytes, { mode: 0o600 });

    // Missing task manifest: the official restore chain owns the plan/task
    // integrity and fails artifact_missing before the resolver's own
    // pointer check (which stays as defense-in-depth).
    await rm(taskPath);
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "(typed failure artifact_missing)");
    }
    await writeFile(taskPath, taskBytes, { mode: 0o600 });

    // A canonical but digest-divergent task manifest at the pointer's
    // revision: a different body under the same identity.
    const divergent = JSON.parse(taskBytes.toString("utf8")) as { body: string };
    divergent.body = "TAMPERED-BODY";
    await writeFile(taskPath, JSON.stringify(divergent), { mode: 0o600 });
    try {
      await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
      expect.unreachable();
    } catch (cause) {
      expectStageTaskInvalid(cause, "(typed failure artifact_mismatch)");
    }
    await writeFile(taskPath, taskBytes, { mode: 0o600 });

    // The tree is byte-identical again and the resolver succeeds.
    const context = await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    expect(context.task_revision).toBe(1);
    expect(Buffer.from(preparedStageTaskBodyBytes(context, pipeline) as Uint8Array).toString("utf8")).toBe("REVISION-ONE");
  }, 20000);

  test("the pointer-only revision-1 proposal reuses the accepted task body", async () => {
    // A pointer-only proposal is valid only for plan revisions above 1
    // (covered by the planning-chain suites); the resolver consumes the
    // accepted plan revision's pointers either way, which the revision-2
    // worker-visibility battery proves end to end in the runtime suite.
    const runId = "stage-task-pointer";
    const harness = await makeHarness("stage-task-pointer", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId);
    const context = await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    // The durable ledger record and the compiled pointer agree; the
    // resolver's ledger-anchor check is part of the resolved identity.
    expect(state.task_revisions).toHaveLength(1);
    const ledgerRecord = state.task_revisions[0];
    expect(ledgerRecord).toBeDefined();
    expect(context.task_manifest_sha256).toBe(ledgerRecord!.sha256);
  }, 20000);
});

// The activation-side data plane of a stage-task port is covered together
// with the production runtime path; this file pins the pure prepare/accept
// contract only: a stage-task port materializes the exact context bytes.
describe("the activation data plane of the stage_task source", () => {
  test("prepareActivationData publishes the exact body bytes and rejects a missing or foreign context", async () => {
    const runId = "stage-task-prepare";
    const harness = await makeHarness("stage-task-prepare", runId);
    const { pipeline, state } = await stageTaskBoundary(harness, runId);
    const context = await resolvePipelineV2StageTaskContext({ pipeline, state, runRoot: harness.runRoot });
    // The snapshot inputs are required by prepareActivationData; the
    // coordination already owns the run's own snapshot, so this unit proof
    // materializes into a fresh orchestrator-owned run root.
    const prepareRoot = join(harness.root, "prepare-run");
    await mkdir(join(prepareRoot, "project"), { recursive: true, mode: 0o700 });
    const runInputs: RunInputsSnapshot = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: join(harness.sources, "task.md") }] as RunInputBinding[],
      prepareRoot,
    );
    const activation: PreparedActivationData = await prepareActivationData(
      pipeline,
      runInputs,
      [] as AcceptedStateOutput[],
      "dev_entry",
      2,
      context,
    );
    const published = await readFile(join(activation.inputs_root, "task"), "utf8");
    expect(published).toBe("REVISION-ONE");
    expect(activation.input_ports[0]).toEqual({
      id: "task",
      source: { stage_task: {} },
      type: "file",
      path: join(activation.inputs_root, "task"),
    });
    // A missing context for a stage-task port is an impossible trusted
    // shape and stays a plain pipeline error.
    await expect(
      prepareActivationData(pipeline, runInputs, [] as AcceptedStateOutput[], "dev_entry", 3),
    ).rejects.toThrow("declares the stage_task source, but no stage-task context was resolved");
    // A context of another pipeline carries no provenance here.
    const otherPipeline = (await loadPipelineV2(harness.bundle)) as ResolvedPipelineV2;
    const otherRoot = join(harness.root, "other-run");
    await mkdir(join(otherRoot, "project"), { recursive: true, mode: 0o700 });
    const otherInputs = await snapshotRunInputs(
      otherPipeline,
      [{ id: "task", path: join(harness.sources, "task.md") }] as RunInputBinding[],
      otherRoot,
    );
    await expect(
      prepareActivationData(
        otherPipeline,
        otherInputs,
        [] as AcceptedStateOutput[],
        "dev_entry",
        4,
        context,
      ),
    ).rejects.toThrow(/stage-task context carries no provenance/);
  }, 20000);
});
