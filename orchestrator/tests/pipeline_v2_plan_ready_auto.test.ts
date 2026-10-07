/**
 * The trusted automatic plan-ready continuation: the `plan_ready` policy of
 * the compiled planning-role metadata lets a controlled `planReady`
 * suspension (and its durable restart windows) continue automatically
 * through the composition controller, the runner and the CLI — while every
 * bundle without the policy keeps the exact established manual chain
 * (`planReady -> resume-plan`) byte for byte.
 *
 * Every prefix is built through the real facades, the real reducer, the
 * real data plane and the real store over real pipeline bundles; no LLM,
 * no Docker Helper, no launcher credential, no sleeps.
 */
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  coordinatePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinationResult,
  type PipelineV2CoordinatorControl,
  type PipelineV2ResumeCoordinationResult,
} from "../src/pipeline_v2_coordinator.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  applyPipelineV2PlanReadyContinuation,
  pipelineV2PlanReadyPolicyFor,
  PipelineV2PlanReadyAutoControllerError,
} from "../src/pipeline_v2_plan_ready_auto_controller.ts";
import {
  applyPipelineV2PlanReadyContinuationWithIo,
} from "../src/pipeline_v2_plan_ready_auto_controller_internal.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  runPipelineV2,
  resumePipelineV2,
  revisePipelineV2Task,
  resumePipelineV2PlanningRunPlan,
  type PipelineV2RunOptions,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
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

type PlanReadyPolicy = { readonly stage_position: number; readonly initial_budget: number };

const INITIAL_BUDGET = 2;

interface AutoPipelineOptions {
  /** The trusted plan_ready policy; `null` declares none. */
  planReady: PlanReadyPolicy | null;
  /** The completed target of the stage entry state. */
  devTarget: "planner2" | "done" | "review_entry";
  /** A second stage template with its own entry state (distinct entries). */
  reviewTemplate?: boolean;
  /** The completed target of the second planning state (when present). */
  plannerTarget?: "dev_entry" | "review_entry";
}

function pipelineYaml(options: AutoPipelineOptions): string {
  const planReadyBlock = (indent: string): string =>
    options.planReady === null
      ? ""
      : `${indent}plan_ready:\n${indent}  stage_position: ${options.planReady.stage_position}\n${indent}  initial_budget: ${options.planReady.initial_budget}\n`;
  const templates = options.reviewTemplate
    ? `  stage_templates:
    - id: development
      entry_state: dev_entry
    - id: review
      entry_state: review_entry
`
    : `  stage_templates:
    - id: development
      entry_state: dev_entry
`;
  const plannerRole = options.devTarget === "planner2"
    ? `    - state_id: planner2
      role: planning
      plan_output: plan2
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
${planReadyBlock("      ")}`
    : "";
  const plannerState = options.devTarget === "planner2"
    ? `  - id: planner2
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
        to: ${options.plannerTarget ?? "dev_entry"}
`
    : "";
  const reviewStates = options.reviewTemplate
    ? `  - id: review_entry
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
`
    : "";
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
${templates}  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
${planReadyBlock("      ")}${plannerRole}    - state_id: dev_entry
      role: stage
      stage_template: development
${options.reviewTemplate ? "    - state_id: review_entry\n      role: stage\n      stage_template: review\n" : ""}
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
        to: ${options.devTarget}
${plannerState}${reviewStates}  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

interface AutoProposal {
  schema_version: 1;
  kind: "run_plan_proposal";
  stages: Array<{ id: string; template: string; tasks: Array<{ id: string; depends_on: string[] }> }>;
  new_tasks: Array<{ id: string; body: string }>;
}

const PROPOSAL_ONE_TASK: AutoProposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
};

const PROPOSAL_TWO_TASKS_DISTINCT: AutoProposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

const PROPOSAL_TWO_TASKS_SHARED: AutoProposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
    { id: "stage-2", template: "development", tasks: [{ id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [
    { id: "task-a", body: "Body A" },
    { id: "task-b", body: "Body B" },
  ],
};

const POINTER_ONLY_R2: AutoProposal = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [],
};

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

interface AutoHarness {
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

async function makeAutoHarness(
  prefix: string,
  runId: string,
  options: AutoPipelineOptions,
  proposal: AutoProposal = PROPOSAL_ONE_TASK,
  createRunRoot = true,
): Promise<AutoHarness> {
  clockValue = 0;
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(options));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  if (options.devTarget === "planner2") {
    await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  }
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
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  if (createRunRoot) {
    await mkdir(runRoot, { mode: 0o700 });
  }
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  void proposal;
  return { root, stateRoot, bundle, configRoot, sources, projectSource, credentialFile, runRoot, statePath: join(runRoot, "state.json") };
}

type CommandRecord = Record<string, unknown>;

/** A recording sink with per-kind fault injection over a real sink. */
class RecordingSink {
  readonly commands: CommandRecord[] = [];
  constructor(
    private readonly inner: PipelineV2RunStateSink,
    private readonly faults?: ReadonlyMap<string, () => Error | undefined>,
  ) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2RunStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    const fault = this.faults?.get(command.kind);
    if (fault !== undefined) {
      const failure = fault();
      if (failure !== undefined) {
        throw failure;
      }
    }
    await this.inner.dispatch(command);
  }
}

async function readDurableState(harness: AutoHarness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

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

interface SessionPair {
  sessionId: string;
  cleanup(): Promise<void>;
  runAgent(): Promise<{ status: "completed" } | { status: "failed"; reason: "worker_failed" | "worker_timeout" }>;
}

/**
 * The fake runtime of the controller proofs: sessions with unique ids from
 * a module-level counter (the reducer enforces global session-id
 * uniqueness), an event log for the cleanup order, and per-state output
 * bodies written into the activation outputs root.
 */
let runtimeSessionCounter = 0;

function fakeAutoRuntime(outputs: Readonly<Record<string, unknown>>): {
  runtime: PipelineV2AgentRuntime;
  events: string[];
  sessionIds: string[];
} {
  const events: string[] = [];
  const sessionIds: string[] = [];
  let currentOutputsRoot = "";
  let currentStateId = "";
  const make = (kind: "execution" | "tool"): SessionPair => {
    runtimeSessionCounter += 1;
    const id = `auto-${kind}-${runtimeSessionCounter}`;
    sessionIds.push(id);
    return {
      sessionId: id,
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
    sessionIds,
  };
}

const CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: () => null,
  freezeSignal: () => null,
};

async function bindingsFor(harness: AutoHarness): Promise<readonly RunInputBinding[]> {
  return [{ id: "task", path: join(harness.sources, "task.md") }];
}

/** One planning activation through the real data plane and reducer. */
async function runPlanningActivation(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  sink: RecordingSink,
  stateId: string,
  executionIndex: number,
  proposal: unknown,
  outputId = "plan",
): Promise<AcceptedStateOutput[]> {
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile: "coder",
    ...startRoleArgs(pipeline, stateId, sink.snapshot),
  });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `auto-plan-exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `auto-plan-tool-${executionIndex}` },
    { kind: "agent_running" },
  ]) {
    await sink.dispatch(command as never);
  }
  await writeFile(join(activation.outputs_root, outputId), JSON.stringify(proposal), { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, activation);
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await sink.dispatch({ kind: "agent_cleanup_completed" });
  return [...accepted, ...records];
}

/** One zero-output stage activation through the real data plane and reducer. */
async function runStageActivation(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  accepted: readonly AcceptedStateOutput[],
  sink: RecordingSink,
  stateId: string,
  executionIndex: number,
): Promise<void> {
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, accepted, stateId, executionIndex);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile: "coder",
    ...startRoleArgs(pipeline, stateId, sink.snapshot),
  });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `auto-stage-exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `auto-stage-tool-${executionIndex}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ]) {
    await sink.dispatch(command as never);
  }
}

/** Opens the existing run through the ordinary sink factory. */
async function reopen(harness: AutoHarness, runId: string, faults?: ReadonlyMap<string, () => Error | undefined>): Promise<RecordingSink> {
  const raw = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
  return new RecordingSink(raw, faults);
}

function expectPlanReady(result: PipelineV2CoordinationResult | PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(false);
  const record = result as Record<string, unknown>;
  expect(Object.keys(record).sort()).toEqual(["ok", "planReady", "state"]);
  expect(record.planReady).toBe(true);
  expect("waiting" in record).toBe(false);
  expect("reason" in record).toBe(false);
  expect("refused" in record).toBe(false);
  const state = record.state as PipelineV2RunState;
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.terminal).toBeUndefined();
  expect(state.failure).toBeUndefined();
  expect(state.run_outputs).toBeUndefined();
  return state;
}

function expectWaiting(result: PipelineV2CoordinationResult | PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(false);
  const record = result as Record<string, unknown>;
  expect(Object.keys(record).sort()).toEqual(["ok", "state", "waiting"]);
  expect(record.waiting).toBe(true);
  const state = record.state as PipelineV2RunState;
  expect(state.status).toBe("waiting");
  return state;
}

function expectAutoSuccess(result: PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(true);
  const record = result as Record<string, unknown>;
  expect(Object.keys(record).sort()).toEqual(["ok", "state"]);
  const state = record.state as PipelineV2RunState;
  expect(state.status).toBe("success");
  return state;
}

/**
 * The honest plan-ready prefix: the fresh coordination suspends plan-ready
 * at the settled-but-unbound planning boundary (revision 8).
 */
async function freshPlanReady(
  harness: AutoHarness,
  runId: string,
  options: { outputs?: Readonly<Record<string, unknown>> } = {},
): Promise<{ recording: RecordingSink; pipeline: ResolvedPipelineV2 }> {
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  const fake = fakeAutoRuntime(options.outputs ?? { architect: { plan: PROPOSAL_ONE_TASK } });
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness),
      sink: recording as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  expectPlanReady(result);
  return { recording, pipeline };
}

const { acceptPipelineV2PlanningRunPlan } = await import("../src/pipeline_v2_planning_run_plan_controller.ts");
const { ensurePipelineV2StageIteration } = await import("../src/pipeline_v2_stage_iteration_controller.ts");
const { restorePipelineV2AcceptedRunPlan } = await import("../src/pipeline_v2_run_plan_restore.ts");

// ---------------------------------------------------------------------------
// Controller-level proofs.
// ---------------------------------------------------------------------------

test("1. the warm initial plan-ready boundary continues automatically to terminal success", async () => {
  const runId = "auto-warm-initial";
  const harness = await makeAutoHarness("auto-warm-initial", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline,
    runRoot: harness.runRoot,
    sink: reopened as never,
    runtime: fake.runtime,
    control: CONTROL,
  });
  const state = expectAutoSuccess(result);
  // the exact durable suffix: the accepted plan, the opened generation and
  // iteration of the trusted stage, the committed planning transition, the
  // stage execution and the terminal publication
  expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-a@1"]);
  expect(state.plan_revisions.map((record) => record.revision)).toEqual([1]);
  expect(state.generations).toHaveLength(1);
  expect(state.generations[0]).toMatchObject({
    index: 1,
    stage_id: "stage-1",
    stage_position: 1,
    template_id: "development",
    initial_budget: INITIAL_BUDGET,
    opened_transition_count: 0,
  });
  expect(state.executions.map((execution) => [execution.state_id, execution.execution_role])).toEqual([
    ["architect", "planning"],
    ["dev_entry", "stage"],
  ]);
  expect(state.transitions).toEqual([
    { index: 0, from: "architect", outcome: "completed", to: "dev_entry", execution_index: 1 },
    { index: 0, from: "dev_entry", outcome: "completed", to: "done", execution_index: 2 },
  ]);
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  expect(state.waits).toHaveLength(0);
  expect(state.revision).toBe(24);
  // the suffix commands after the reopen, in the exact order
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "task_revision_accepted",
    "plan_revision_accepted",
    "stage_generation_opened",
    "stage_iteration_opened",
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
  // the cleanup order inside the stage activation: tool first
  expect(fake.events).toEqual(["run:execution", "cleanup:tool", "cleanup:execution"]);
  // loader round-trip
  expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
});

test("2. the composition returns the exact downstream result by object identity and verifies the stage confirmation", async () => {
  const runId = "auto-identity";
  const harness = await makeAutoHarness("auto-identity", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const sentinel = { ok: true, state: { marker: "sentinel" } } as unknown as PipelineV2ResumeCoordinationResult;
  let handoffCalls = 0;
  const result = await applyPipelineV2PlanReadyContinuationWithIo(
    {
      acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
      restoreAcceptedRunPlan: restorePipelineV2AcceptedRunPlan,
      compiledStageFor: (await import("../src/pipeline_v2_run_plan_compiled.ts")).compiledPipelineV2RunPlanStageFor,
      resumeAfterHandoff: (options: Record<string, unknown>) => {
        handoffCalls += 1;
        expect(options["stageId"]).toBe("stage-1");
        expect(options["initialBudget"]).toBe(INITIAL_BUDGET);
        return Promise.resolve(sentinel);
      },
    },
    {
      pipeline,
      runRoot: harness.runRoot,
      sink: await reopen(harness, runId) as never,
      runtime: fakeAutoRuntime({}).runtime,
      control: CONTROL,
    },
  );
  expect(result).toBe(sentinel);
  expect(handoffCalls).toBe(1);
  // the acceptance suffix happened (the real facade ran); the fake resume
  // replaced only the handoff+resume step
  const durable = await readDurableState(harness);
  expect(durable.plan_revisions).toHaveLength(1);
});

test("3. hostile composed facade results are typed invalid_result, never a TypeError", async () => {
  const runId = "auto-hostile-results";
  const harness = await makeAutoHarness("auto-hostile-results", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const sink = await reopen(harness, runId);
  const runtime = fakeAutoRuntime({}).runtime;
  const base = { pipeline, runRoot: harness.runRoot, sink: sink as never, runtime, control: CONTROL };
  const realStageFor = (await import("../src/pipeline_v2_run_plan_compiled.ts")).compiledPipelineV2RunPlanStageFor;
  const cases: Array<{ name: string; accept: unknown; restore: unknown; stageFor?: unknown; expectClass?: string }> = [
    { name: "accept primitive", accept: async () => 7, restore: restorePipelineV2AcceptedRunPlan },
    { name: "accept missing plan", accept: async () => ({ state: {} }), restore: restorePipelineV2AcceptedRunPlan },
    { name: "accept plan primitive", accept: async () => ({ compiled_plan: 3, state: {} }), restore: restorePipelineV2AcceptedRunPlan },
    {
      name: "forged compiled plan passes through by identity",
      accept: async () => ({ compiled_plan: { stages: [{ id: "stage-1" }] }, state: {} }),
      restore: restorePipelineV2AcceptedRunPlan,
      expectClass: "PipelineV2CompiledRunPlanError",
    },
  ];
  for (const item of cases) {
    let error: unknown;
    try {
      await applyPipelineV2PlanReadyContinuationWithIo(
        {
          acceptPlanningRunPlan: item.accept as never,
          restoreAcceptedRunPlan: item.restore as never,
          compiledStageFor: (item.stageFor ?? realStageFor) as never,
          resumeAfterHandoff: async () => sentinelOf(),
        },
        { pipeline, runRoot: harness.runRoot, sink: sink as never, runtime, control: CONTROL },
      );
    } catch (cause) {
      error = cause;
    }
    expect(error instanceof Error, item.name).toBe(true);
    if (item.expectClass !== undefined) {
      expect((error as Error).name, item.name).toBe(item.expectClass);
    } else {
      expect(error instanceof PipelineV2PlanReadyAutoControllerError, item.name).toBe(true);
      expect(
        ["invalid_state", "invalid_result"],
        item.name,
      ).toContain((error as PipelineV2PlanReadyAutoControllerError).reason);
    }
  }
  function sentinelOf(): never {
    throw new Error("the resume facade must never be reached by a hostile result");
  }
});

test("4. A0 restart window: the pure plan-ready boundary continues with the full suffix", async () => {
  const runId = "auto-window-a0";
  const harness = await makeAutoHarness("auto-window-a0", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  expect((await readDurableState(harness)).revision).toBe(8);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "task_revision_accepted",
    "plan_revision_accepted",
    "stage_generation_opened",
    "stage_iteration_opened",
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("5. A1 restart window: the partial task acceptance is reconciled by the acceptance facade", async () => {
  const runId = "auto-window-a1";
  const harness = await makeAutoHarness("auto-window-a1", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  // the acceptance crashes on the plan dispatch: the task record is durable
  await reopen(harness, runId, new Map([["plan_revision_accepted", () => new Error("crash")]]) as never)
    .then(async (faulted) => {
      await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: faulted as never });
    })
    .catch((cause: unknown) => {
      expect((cause as Error).message).toBe("crash");
    });
  const partial = await readDurableState(harness);
  expect(partial.revision).toBe(9);
  expect(partial.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-a@1"]);
  expect(partial.plan_revisions).toHaveLength(0);
  // the automatic continuation reconciles the durable prefix
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "plan_revision_accepted",
    "stage_generation_opened",
    "stage_iteration_opened",
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("6. A2 restart window: the accepted plan is recognized with zero acceptance dispatch", async () => {
  const runId = "auto-window-a2";
  const harness = await makeAutoHarness("auto-window-a2", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  expect((await readDurableState(harness)).revision).toBe(10);
  void accepted;
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "stage_generation_opened",
    "stage_iteration_opened",
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("7. A3 restart window: the generation without its iteration opens only the iteration", async () => {
  const runId = "auto-window-a3";
  const harness = await makeAutoHarness("auto-window-a3", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  await reopen(harness, runId, new Map([["stage_iteration_opened", () => new Error("crash")]]))
    .then((faulted) =>
      ensurePipelineV2StageIteration({
        compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: faulted as never,
      }),
    )
    .catch((cause: unknown) => {
      expect((cause as Error).message).toBe("crash");
    });
  expect((await readDurableState(harness)).revision).toBe(11);
  expect((await readDurableState(harness)).generations[0]?.open_iteration).toBeUndefined();
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "stage_iteration_opened",
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("8. A4 restart window: the opened generation and iteration need only the planning transition", async () => {
  const runId = "auto-window-a4";
  const harness = await makeAutoHarness("auto-window-a4", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: await reopen(harness, runId) as never,
  });
  expect((await readDurableState(harness)).revision).toBe(12);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "transition_committed",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("9. A5 restart window: the committed planning transition is recognized by the handoff Branch B with zero dispatch", async () => {
  const runId = "auto-window-a5";
  const harness = await makeAutoHarness("auto-window-a5", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: await reopen(harness, runId) as never,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  expect((await readDurableState(harness)).revision).toBe(13);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  const result = await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  expectAutoSuccess(result);
  expect((await readDurableState(harness)).revision).toBe(24);
  // the committed handoff boundary: zero handoff dispatch, only the resumed
  // stage execution and the terminal publication
  expect(reopened.commands.map((command) => command.kind)).toEqual([
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

test("10. A6: a started successor execution is never re-handed off; the composition fails closed", async () => {
  const runId = "auto-window-a6";
  const harness = await makeAutoHarness("auto-window-a6", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: await reopen(harness, runId) as never,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  // the successor execution starts and stays in flight
  await (await reopen(harness, runId)).dispatch({
    kind: "start_agent_execution",
    stateId: "dev_entry",
    profile: "coder",
    ...startRoleArgs(pipeline, "dev_entry", (await readDurableState(harness))),
  });
  const before = await fingerprint(harness.runRoot);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  let error: unknown;
  try {
    await applyPipelineV2PlanReadyContinuation({
      pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
    });
  } catch (cause) {
    error = cause;
  }
  expect(error instanceof PipelineV2PlanReadyAutoControllerError).toBe(true);
  expect((error as PipelineV2PlanReadyAutoControllerError).reason).toBe("invalid_state");
  // zero facade effect: no command, no session
  expect(reopened.commands).toHaveLength(0);
  expect(fake.sessionIds).toHaveLength(0);
  expect(await fingerprint(harness.runRoot)).toBe(before);
});

test("11. shared-entry ambiguity: the trusted positions 1 and 2 select different stages of the same template", async () => {
  for (const position of [1, 2] as const) {
    const runId = `auto-shared-${position}`;
    const harness = await makeAutoHarness(`auto-shared-${position}`, runId, {
      planReady: { stage_position: position, initial_budget: INITIAL_BUDGET },
      devTarget: "done",
    }, PROPOSAL_TWO_TASKS_SHARED);
    const { pipeline } = await freshPlanReady(harness, runId, {
      outputs: { architect: { plan: PROPOSAL_TWO_TASKS_SHARED } },
    });
    const reopened = await reopen(harness, runId);
    const fake = fakeAutoRuntime({});
    const result = await applyPipelineV2PlanReadyContinuation({
      pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
    });
    expectAutoSuccess(result);
    const state = await readDurableState(harness);
    const selected = state.generations[0];
    expect(selected?.stage_id, `position ${position}`).toBe(position === 1 ? "stage-1" : "stage-2");
    expect(selected?.stage_position, `position ${position}`).toBe(position);
    expect(selected?.template_id, `position ${position}`).toBe("development");
    // both stages share the compiled entry state
    expect(selected?.opened_transition_count, `position ${position}`).toBe(0);
    expect(state.plan_revisions[0]?.origin_execution, `position ${position}`).toBe(1);
  }
});

test("12. an out-of-range trusted position fails closed after the durable acceptance", async () => {
  const runId = "auto-out-of-range";
  const harness = await makeAutoHarness("auto-out-of-range", runId, {
    planReady: { stage_position: 2, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  }, PROPOSAL_ONE_TASK);
  const { pipeline } = await freshPlanReady(harness, runId);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  let error: unknown;
  try {
    await applyPipelineV2PlanReadyContinuation({
      pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
    });
  } catch (cause) {
    error = cause;
  }
  expect(error instanceof PipelineV2PlanReadyAutoControllerError).toBe(true);
  expect((error as PipelineV2PlanReadyAutoControllerError).reason).toBe("invalid_state");
  // the acceptance is already durable — the confirmed contract boundary;
  // no stage lifecycle write happened
  const state = await readDurableState(harness);
  expect(state.revision).toBe(10);
  expect(state.plan_revisions).toHaveLength(1);
  expect(state.generations).toHaveLength(0);
  expect(state.transitions).toHaveLength(0);
  expect(reopened.commands.map((command) => command.kind)).toEqual(["task_revision_accepted", "plan_revision_accepted"]);
  expect(fake.sessionIds).toHaveLength(0);
});

test("13. an edge-incompatible selected stage is refused before any stage lifecycle write", async () => {
  const runId = "auto-edge-incompatible";
  const harness = await makeAutoHarness("auto-edge-incompatible", runId, {
    planReady: { stage_position: 2, initial_budget: INITIAL_BUDGET },
    devTarget: "planner2",
    plannerTarget: "review_entry",
    reviewTemplate: true,
  }, PROPOSAL_TWO_TASKS_DISTINCT);
  const { pipeline } = await freshPlanReady(harness, runId, {
    outputs: { architect: { plan: PROPOSAL_TWO_TASKS_DISTINCT } },
  });
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  let error: unknown;
  try {
    await applyPipelineV2PlanReadyContinuation({
      pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
    });
  } catch (cause) {
    error = cause;
  }
  // the handoff's own planning-edge refusal passes through by identity
  expect(error instanceof Error).toBe(true);
  expect((error as Error).name).not.toBe("PipelineV2PlanReadyAutoControllerError");
  expect((error as Error).message).toContain("planning");
  // the acceptance is durable; the stage lifecycle stayed untouched
  const state = await readDurableState(harness);
  expect(state.revision).toBe(11);
  expect(state.plan_revisions).toHaveLength(1);
  expect(state.generations).toHaveLength(0);
  expect(state.transitions).toHaveLength(0);
  expect(fake.sessionIds).toHaveLength(0);
});

test("14. the composition's option battery: shapes, hostile extras, provenance and the sink contract", async () => {
  const runId = "auto-options";
  const harness = await makeAutoHarness("auto-options", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const sink = await reopen(harness, runId);
  const runtime = fakeAutoRuntime({}).runtime;
  const base = { pipeline, runRoot: harness.runRoot, sink: sink as never, runtime, control: CONTROL };
  const realStageFor = (await import("../src/pipeline_v2_run_plan_compiled.ts")).compiledPipelineV2RunPlanStageFor;
  const productionOps = {
    acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
    restoreAcceptedRunPlan: restorePipelineV2AcceptedRunPlan,
    compiledStageFor: realStageFor,
    resumeAfterHandoff: async () => 1 as never,
  };
  // non-record options
  for (const options of [null, 7, "x", []]) {
    let error: unknown;
    try {
      await applyPipelineV2PlanReadyContinuationWithIo(productionOps, options);
    } catch (cause) {
      error = cause;
    }
    expect(error instanceof PipelineV2PlanReadyAutoControllerError, String(options)).toBe(true);
    expect((error as PipelineV2PlanReadyAutoControllerError).reason, String(options)).toBe("invalid_options");
  }
  // a relative runRoot and a broken sink are invalid options before any read
  for (const options of [
    { ...base, runRoot: "relative" },
    { ...base, sink: { snapshot: null, poisoned: false } },
  ]) {
    let error: unknown;
    try {
      await applyPipelineV2PlanReadyContinuation(options as never);
    } catch (cause) {
      error = cause;
    }
    expect(error instanceof PipelineV2PlanReadyAutoControllerError).toBe(true);
    expect((error as PipelineV2PlanReadyAutoControllerError).reason).toBe("invalid_options");
  }
  // a spread clone and a Proxy pipeline are rejected by the provenance gate
  const cloned = { ...pipeline } as unknown as ResolvedPipelineV2;
  let error: unknown;
  try {
    await applyPipelineV2PlanReadyContinuation({ ...base, pipeline: cloned });
  } catch (cause) {
    error = cause;
  }
  expect(error instanceof Error).toBe(true);
  expect((error as Error).name).not.toBe("PipelineV2PlanReadyAutoControllerError");
  let traps = 0;
  const hostilePipeline: ResolvedPipelineV2 = new Proxy({} as unknown as Record<string, unknown>, {
    get() {
      traps += 1;
      return undefined;
    },
  }) as unknown as ResolvedPipelineV2;
  error = undefined;
  try {
    await applyPipelineV2PlanReadyContinuation({ ...base, pipeline: hostilePipeline });
  } catch (cause) {
    error = cause;
  }
  expect(error instanceof Error).toBe(true);
  expect((error as Error).name).not.toBe("PipelineV2PlanReadyAutoControllerError");
  expect(traps).toBe(0);
  // a hand-built snapshot inside a fake structural sink: not a record -> typed
  const fakeSink = {
    snapshot: "not-a-record" as unknown as PipelineV2RunState,
    poisoned: false,
    dispatch: async () => {},
  };
  error = undefined;
  try {
    await applyPipelineV2PlanReadyContinuation({ ...base, sink: fakeSink });
  } catch (cause) {
    error = cause;
  }
  expect(error instanceof PipelineV2PlanReadyAutoControllerError).toBe(true);
  expect((error as PipelineV2PlanReadyAutoControllerError).reason).toBe("invalid_state");
  // hostile extras are never read: a Proxy options object reading only the
  // five contract fields
  const read: string[] = [];
  const hostileOptions = new Proxy({ ...base }, {
    get(target, property) {
      read.push(String(property));
      return Reflect.get(target, property);
    },
  }) as unknown as Record<string, unknown>;
  await applyPipelineV2PlanReadyContinuationWithIo(productionOps, hostileOptions);
  expect(read.sort()).toEqual(["control", "pipeline", "runRoot", "runtime", "sink"]);
});

test("15. the routing helper admits exactly the settled planning boundaries with a policy", async () => {
  const runId = "auto-routing-helper";
  const harness = await makeAutoHarness("auto-routing-helper", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "planner2",
  });
  const pipeline = await loadPipelineV2(harness.bundle);
  // no snapshot at all
  expect(pipelineV2PlanReadyPolicyFor(pipeline, null)).toBeNull();
  expect(pipelineV2PlanReadyPolicyFor(pipeline, 7)).toBeNull();
  expect(pipelineV2PlanReadyPolicyFor(pipeline, {})).toBeNull();
  // the fresh plan-ready boundary carries the policy
  const { recording } = await freshPlanReady(harness, runId);
  const ready = recording.snapshot as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, ready)).toEqual({ stage_position: 1, initial_budget: INITIAL_BUDGET });
  // a non-planning last execution is refused
  const stageShape = { ...ready, executions: [{ ...ready.executions[0], execution_role: "stage" }] } as unknown as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, stageShape)).toBeNull();
  // an in-flight planning execution is refused
  const inFlight = { ...ready, executions: [{ ...ready.executions[0], phase: "running" }] } as unknown as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, inFlight)).toBeNull();
  // a failed run is refused
  const failed = { ...ready, status: "failed", failure: { reason: "worker_failed" } } as unknown as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, failed)).toBeNull();
  // a committed planning transition into a control state is refused (A5 control shape)
  const controlBound = {
    ...ready,
    transitions: [{ index: 0, from: "architect", outcome: "completed", to: "gate", execution_index: 1 }],
    cursor: { current_state: "gate", transition_count: 1 },
  } as unknown as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, controlBound)).toBeNull();
  // a committed planning transition into a planning state is refused
  const planningBound = {
    ...ready,
    transitions: [{ index: 0, from: "architect", outcome: "completed", to: "planner2", execution_index: 1 }],
    cursor: { current_state: "planner2", transition_count: 1 },
  } as unknown as PipelineV2RunState;
  expect(pipelineV2PlanReadyPolicyFor(pipeline, planningBound)).toBeNull();
  // a pipeline without the policy is refused
  const plain = await loadPipelineV2(await (async () => {
    const bare = await makeAutoHarness("auto-routing-plain", "auto-routing-plain", {
      planReady: null,
      devTarget: "planner2",
    });
    return bare.bundle;
  })());
  expect(pipelineV2PlanReadyPolicyFor(plain, ready)).toBeNull();
});

test("16. the restore branch's hostile composed results are typed fail-closed at the committed boundary", async () => {
  const runId = "auto-restore-hostile";
  const harness = await makeAutoHarness("auto-restore-hostile", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({
    pipeline, runRoot: harness.runRoot, sink: await reopen(harness, runId) as never,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: INITIAL_BUDGET, sink: await reopen(harness, runId) as never,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  const before = await fingerprint(harness.runRoot);
  for (const restoreResult of [7, null, {}, { state: {} }, { compiled_plan: 3, state: {} }]) {
    const reopened = await reopen(harness, runId);
    let error: unknown;
    try {
      await applyPipelineV2PlanReadyContinuationWithIo(
        {
          acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
          restoreAcceptedRunPlan: async () => restoreResult,
          compiledStageFor: (await import("../src/pipeline_v2_run_plan_compiled.ts")).compiledPipelineV2RunPlanStageFor,
          resumeAfterHandoff: async () => {
            throw new Error("unreached");
          },
        },
        { pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fakeAutoRuntime({}).runtime, control: CONTROL },
      );
    } catch (cause) {
      error = cause;
    }
    expect(error instanceof PipelineV2PlanReadyAutoControllerError, String(JSON.stringify(restoreResult)?.slice(0, 40))).toBe(true);
  }
  expect(await fingerprint(harness.runRoot)).toBe(before);
});

test("17. no proposal, task or output body ever reaches the durable state or the controller diagnostics", async () => {
  const runId = "auto-canary";
  const harness = await makeAutoHarness("auto-canary", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const reopened = await reopen(harness, runId);
  const fake = fakeAutoRuntime({});
  await applyPipelineV2PlanReadyContinuation({
    pipeline, runRoot: harness.runRoot, sink: reopened as never, runtime: fake.runtime, control: CONTROL,
  });
  const stateJson = JSON.stringify(await readDurableState(harness));
  for (const canary of ["Body A", "run_plan_proposal", "IMPLEMENT-THE-TASK", "TASK-BODY", "cred-token"]) {
    expect(stateJson, canary).not.toContain(canary);
  }
  // every controller error message on this run stays content-free
  const messages: string[] = [];
  for (const options of [
    { pipeline, runRoot: "relative", sink: reopened as never, runtime: fake.runtime, control: CONTROL },
    { pipeline, runRoot: harness.runRoot, sink: { snapshot: null, poisoned: false }, runtime: fake.runtime, control: CONTROL },
  ]) {
    try {
      await applyPipelineV2PlanReadyContinuation(options as never);
    } catch (cause) {
      messages.push(cause instanceof Error ? cause.message : String(cause));
    }
  }
  for (const message of messages) {
    for (const canary of ["Body A", "TASK-BODY", "/tmp/", "cred-token"]) {
      expect(message, `${canary} in ${message}`).not.toContain(canary);
    }
  }
});

test("18. the public export surface is exactly the three runtime keys plus types", async () => {
  const facade = await import("../src/pipeline_v2_plan_ready_auto_controller.ts");
  expect(Object.keys(facade).sort()).toEqual([
    "PipelineV2PlanReadyAutoControllerError",
    "applyPipelineV2PlanReadyContinuation",
    "pipelineV2PlanReadyPolicyFor",
  ]);
});

test("19. the controller source scan: only the existing facades and resolvers; no second mechanism", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_plan_ready_auto_controller_internal.ts"), "utf8");
  // the composed facades are imported exactly once each
  expect((source.match(/from "\.\/pipeline_v2_planning_run_plan_controller\.ts"/g) ?? []).length).toBe(1);
  expect((source.match(/from "\.\/pipeline_v2_planning_run_plan_resume_controller\.ts"/g) ?? []).length).toBe(1);
  expect((source.match(/from "\.\/pipeline_v2_run_plan_compiled\.ts"/g) ?? []).length).toBe(1);
  expect((source.match(/from "\.\/pipeline_v2_run_plan_restore\.ts"/g) ?? []).length).toBe(1);
  // no second mechanism of any kind
  for (const banned of [
    "canonicalJson",
    "CryptoHasher",
    "reducePipelineV2RunCommand",
    "validatePipelineV2RunState",
    "readAcceptedJsonOutput",
    "preparePipelineV2RunPlanProposal",
    "preparePipelineV2RunPlanCandidate",
    "WeakSet",
    "WeakMap",
    "node:fs",
    "docker",
  ]) {
    expect(source, banned).not.toContain(banned);
  }
  // no message-text classification
  expect(source).not.toContain(".message.includes");
  expect(source).not.toContain("instanceof PipelineV2PlanningRunPlan");
  expect(source).not.toContain("instanceof PipelineV2RunPlanRestore");
});

// ---------------------------------------------------------------------------
// Runner-level proofs.
// ---------------------------------------------------------------------------

const EXPECTED_LAUNCHER_ID = "dhl_auto_plan_ready";
let runnerSessionCounter = 0;

interface RunnerScript {
  /** A nonzero worker exit classifies `worker_failed`. */
  runCode?: number;
  /** A signal delivered synchronously during the Nth worker run (1-based). */
  signalOnRunCall?: number;
  signalKind?: "SIGINT" | "SIGTERM";
}

function autoRunnerDeps(harness: AutoHarness, runId: string, script: RunnerScript = {}): PipelineV2RunnerDeps {
  const runRootKnown = harness.runRoot;
  let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
  let runCalls = 0;
  const cli = async (args: readonly string[]) => {
    if (args[0] === "session" && args[1] === "create") {
      runnerSessionCounter += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_auto_${runnerSessionCounter}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_auto_${runnerSessionCounter}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    if (args[0] === "pull") {
      return { code: 0 };
    }
    if (args[0] === "run") {
      runCalls += 1;
      if (script.signalOnRunCall !== undefined && runCalls === script.signalOnRunCall) {
        signalHandler?.(script.signalKind ?? "SIGINT");
      }
      let mountStart = -1;
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] === "--mount") {
          mountStart = i;
          break;
        }
      }
      for (let i = mountStart; i >= 0 && i < args.length && args[i] === "--mount"; i += 2) {
        const spec = args[i + 1] ?? "";
        const [source, target] = spec.split(":");
        if (target === "/pipeline/outputs") {
          const dash = (source ?? "").split("/")[1] ?? "";
          const dashIndex = dash.indexOf("-");
          const activationIndex = dashIndex === -1 ? "" : dash.slice(0, dashIndex);
          const stateId = dashIndex === -1 ? dash : dash.slice(dashIndex + 1);
          if (stateId === "architect" || stateId === "planner2") {
            // the first architect activation proposes the initial plan; every
            // later planning activation is the pointer-only replanning form
            const proposal = stateId === "architect" && activationIndex === "1" ? PROPOSAL_ONE_TASK : POINTER_ONLY_R2;
            const dir = join(runRootKnown, source ?? "");
            await mkdir(dir, { recursive: true });
            await writeFile(join(dir, stateId === "architect" ? "plan" : "plan2"), JSON.stringify(proposal), { mode: 0o600 });
          }
        }
      }
      return { code: script.runCode ?? 0 };
    }
    return { code: 0 };
  };
  return {
    cli: cli as never,
    fetchAuth: (async () => ({
      status: 200,
      body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
    })) as never,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "coder-secret" },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    onSignal: (handler) => {
      signalHandler = handler;
    },
    now: nextTick,
    randomId: () => runId,
  };
}

function autoRunOptions(harness: AutoHarness): PipelineV2RunOptions {
  return {
    pipelineRoot: harness.bundle,
    configRoot: harness.configRoot,
    projectSourcePath: harness.projectSource,
    inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }],
    launcherId: EXPECTED_LAUNCHER_ID,
  };
}

function expectRunnerSuccess(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
  expect("waiting" in outcome).toBe(false);
  expect("planReady" in outcome).toBe(false);
  expect("reason" in outcome).toBe(false);
  return outcome.state as PipelineV2RunState;
}

function expectRunnerWaiting(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(false);
  expect(outcome.waiting).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect("planReady" in outcome).toBe(false);
  expect("reason" in outcome).toBe(false);
  expect("refused" in outcome).toBe(false);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state", "waiting"]);
  return outcome.state as PipelineV2RunState;
}

test("20. the fresh runner run auto-continues through the policy to terminal success", async () => {
  const runId = "auto-runner-fresh";
  const harness = await makeAutoHarness("auto-runner-fresh", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  }, PROPOSAL_ONE_TASK, false);
  const outcome = await runPipelineV2(autoRunOptions(harness), autoRunnerDeps(harness, runId));
  const state = expectRunnerSuccess(outcome);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot);
  expect(state.revision).toBe(24);
  expect(state.status).toBe("success");
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  expect(state.executions).toHaveLength(2);
  expect(state.task_revisions).toHaveLength(1);
  expect(state.plan_revisions).toHaveLength(1);
  expect(state.generations).toHaveLength(1);
  expect(state.generations?.[0]?.open_iteration).toBeDefined();
  expect(state.terminal?.state_id).toBe("done");
  const ids: string[] = [];
  for (const execution of state.executions) {
    if (execution.type === "agent") {
      ids.push(execution.execution_session_id ?? "", execution.tool_session_id ?? "");
    }
  }
  expect(ids).toHaveLength(4);
  expect(new Set(ids).size).toBe(4);
  const durable = await readDurableState(harness);
  expect(durable.revision).toBe(24);
  expect(durable.status).toBe("success");
});

test("21. a bundle without the policy keeps the exact manual planReady runner outcome", async () => {
  const runId = "auto-runner-manual";
  const harness = await makeAutoHarness("auto-runner-manual", runId, { planReady: null, devTarget: "done" }, PROPOSAL_ONE_TASK, false);
  const outcome = await runPipelineV2(autoRunOptions(harness), autoRunnerDeps(harness, runId));
  expect(outcome.ok).toBe(false);
  expect(outcome.planReady).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect(outcome.waiting).toBeUndefined();
  expect("reason" in outcome).toBe(false);
  expect("refused" in outcome).toBe(false);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "planReady", "runId", "runRoot", "state"]);
  expect(outcome.state?.revision).toBe(8);
  expect(outcome.state?.task_revisions).toHaveLength(0);
  expect(outcome.state?.generations).toHaveLength(0);
});

test("22. the revise-task chain auto-continues the replanned boundary to the next waiting state", async () => {
  const runId = "auto-runner-revise";
  const harness = await makeAutoHarness("auto-runner-revise", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "planner2",
  }, PROPOSAL_ONE_TASK, false);
  const first = await runPipelineV2(autoRunOptions(harness), autoRunnerDeps(harness, runId));
  const firstState = expectRunnerWaiting(first);
  // the initial continuation ran to the stage -> planning wait boundary
  expect(firstState.revision).toBe(22);
  expect(firstState.cursor).toEqual({ current_state: "planner2", transition_count: 2 });
  expect(firstState.waits).toHaveLength(1);
  expect(firstState.waits[0]?.index).toBe(1);
  expect(firstState.waits[0]?.state_id).toBe("planner2");
  expect(firstState.waits[0]?.response).toBeUndefined();
  expect(firstState.generations).toHaveLength(1);
  expect(firstState.generations?.[0]?.open_iteration).toBeDefined();
  // the revise-task intervention replans and the continuation resumes the
  // replanned stage automatically up to the next wait boundary
  const second = await revisePipelineV2Task(
    {
      runId,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Revised body",
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    },
    autoRunnerDeps(harness, runId),
  );
  const state = expectRunnerWaiting(second);
  expect(state.revision).toBe(47);
  expect(state.cursor).toEqual({ current_state: "planner2", transition_count: 4 });
  expect(state.waits).toHaveLength(2);
  expect(state.waits[0]?.response?.action_id).toBe("revise_task");
  expect(state.waits[1]?.response).toBeUndefined();
  expect(state.waits[1]?.state_id).toBe("planner2");
  expect(state.task_revisions).toHaveLength(2);
  expect(state.plan_revisions).toHaveLength(2);
  expect(state.generations).toHaveLength(2);
  expect(state.generations?.[0]?.closed?.by).toBe("replanned");
  expect(state.generations?.[1]?.open_iteration).toBeDefined();
  expect(state.status).toBe("waiting");
  const durable = await readDurableState(harness);
  expect(durable.revision).toBe(47);
  expect(durable.status).toBe("waiting");
});

test("23. the plain resume auto-continues the durable plan-ready restart (A0)", async () => {
  const runId = "auto-runner-resume-a0";
  const harness = await makeAutoHarness("auto-runner-resume-a0", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  await freshPlanReady(harness, runId);
  const before = runnerSessionCounter;
  const outcome = await resumePipelineV2(
    { runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    autoRunnerDeps(harness, runId),
  );
  const state = expectRunnerSuccess(outcome);
  expect(state.revision).toBe(24);
  expect(runnerSessionCounter - before).toBe(2);
});

test("24. the plain resume auto-continues the accepted plan boundary (A2)", async () => {
  const runId = "auto-runner-resume-a2";
  const harness = await makeAutoHarness("auto-runner-resume-a2", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: (await reopen(harness, runId)) as never });
  expect((await readDurableState(harness)).revision).toBe(10);
  const before = runnerSessionCounter;
  const outcome = await resumePipelineV2(
    { runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    autoRunnerDeps(harness, runId),
  );
  const state = expectRunnerSuccess(outcome);
  expect(state.revision).toBe(24);
  expect(state.task_revisions).toHaveLength(1);
  expect(state.plan_revisions).toHaveLength(1);
  expect(runnerSessionCounter - before).toBe(2);
});

test("25. the plain resume auto-continues the committed boundary through the restore branch (A5)", async () => {
  const runId = "auto-runner-resume-a5";
  const harness = await makeAutoHarness("auto-runner-resume-a5", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: (await reopen(harness, runId)) as never });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: (await reopen(harness, runId)) as never,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  expect((await readDurableState(harness)).revision).toBe(13);
  const before = runnerSessionCounter;
  const outcome = await resumePipelineV2(
    { runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    autoRunnerDeps(harness, runId),
  );
  const state = expectRunnerSuccess(outcome);
  expect(state.revision).toBe(24);
  expect(runnerSessionCounter - before).toBe(2);
});

test("26. the plain resume on a started successor execution stays an ordinary typed refusal (A6)", async () => {
  const runId = "auto-runner-resume-a6";
  const harness = await makeAutoHarness("auto-runner-resume-a6", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  });
  const { pipeline } = await freshPlanReady(harness, runId);
  const accepted = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: (await reopen(harness, runId)) as never });
  await ensurePipelineV2StageIteration({
    compiledPlan: accepted.compiled_plan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: (await reopen(harness, runId)) as never,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  await (await reopen(harness, runId)).dispatch({
    kind: "start_agent_execution",
    stateId: "dev_entry",
    profile: "coder",
    ...startRoleArgs(pipeline, "dev_entry", await readDurableState(harness)),
  });
  const before = runnerSessionCounter;
  const outcome = await resumePipelineV2(
    { runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    autoRunnerDeps(harness, runId),
  );
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.reason).toBe("invalid_state");
  expect(outcome.state?.revision).toBe(14);
  expect(runnerSessionCounter - before).toBe(0);
  expect((await readDurableState(harness)).revision).toBe(14);
});

test("27. the plain resume on a plan-ready boundary without the policy keeps the ordinary refusal", async () => {
  const runId = "auto-runner-resume-plain";
  const harness = await makeAutoHarness("auto-runner-resume-plain", runId, { planReady: null, devTarget: "done" });
  await freshPlanReady(harness, runId);
  const before = runnerSessionCounter;
  const outcome = await resumePipelineV2(
    { runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    autoRunnerDeps(harness, runId),
  );
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.reason).toBe("invalid_state");
  expect(outcome.state?.revision).toBe(8);
  expect(runnerSessionCounter - before).toBe(0);
  expect((await readDurableState(harness)).revision).toBe(8);
});

test("28. a signal during the planning worker run finalizes 130 without any continuation", async () => {
  const runId = "auto-runner-signal";
  const harness = await makeAutoHarness("auto-runner-signal", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  }, PROPOSAL_ONE_TASK, false);
  const outcome = await runPipelineV2(
    autoRunOptions(harness),
    autoRunnerDeps(harness, runId, { signalOnRunCall: 1 }),
  );
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(130);
  expect(outcome.reason).toBe("signal_sigint");
  const state = outcome.state;
  expect(state?.status).toBe("failed");
  expect(state?.failure?.reason).toBe("signal_sigint");
  expect(state?.task_revisions).toHaveLength(0);
  expect(state?.plan_revisions).toHaveLength(0);
  expect(state?.generations).toHaveLength(0);
  expect(state?.transitions).toHaveLength(0);
  expect(state?.executions[0]?.phase).toBe("cleanup_completed");
  expect((await readDurableState(harness)).revision).toBe(9);
});

test("29. no secret, body or host path reaches the durable auto-continued state", async () => {
  const runId = "auto-runner-canary";
  const harness = await makeAutoHarness("auto-runner-canary", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  }, PROPOSAL_ONE_TASK, false);
  const outcome = await runPipelineV2(autoRunOptions(harness), autoRunnerDeps(harness, runId));
  expectRunnerSuccess(outcome);
  const serialized = JSON.stringify(outcome.state ?? {});
  for (const canary of [
    "Body A",
    "IMPLEMENT-THE-TASK",
    "coder-secret",
    "CODER_SOURCE_VAR_1",
    "cred-token-not-real",
    "dhc_auto_",
    harness.runRoot,
    harness.stateRoot,
    harness.sources,
    harness.projectSource,
    harness.configRoot,
    "glm53-flash",
  ]) {
    expect(serialized, canary).not.toContain(canary);
  }
});

test("30. a signal inside the auto continuation finalizes 130 with the handoff durable", async () => {
  const runId = "auto-runner-signal-mid";
  const harness = await makeAutoHarness("auto-runner-signal-mid", runId, {
    planReady: { stage_position: 1, initial_budget: INITIAL_BUDGET },
    devTarget: "done",
  }, PROPOSAL_ONE_TASK, false);
  const outcome = await runPipelineV2(
    autoRunOptions(harness),
    autoRunnerDeps(harness, runId, { signalOnRunCall: 2 }),
  );
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(130);
  expect(outcome.reason).toBe("signal_sigint");
  const state = outcome.state;
  // the handoff and the completed stage execution are durable; the signal
  // is caught at the post-engine checkpoint before the terminal record
  expect(state?.task_revisions).toHaveLength(1);
  expect(state?.plan_revisions).toHaveLength(1);
  expect(state?.generations).toHaveLength(1);
  expect(state?.transitions).toHaveLength(2);
  expect(state?.cursor).toEqual({ current_state: "done", transition_count: 2 });
  const stage = state?.executions[1];
  expect(stage?.state_id).toBe("dev_entry");
  expect(stage?.phase).toBe("cleanup_completed");
  if (stage?.type === "agent") {
    expect(stage.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  }
  expect(state?.status).toBe("failed");
  expect(state?.failure?.reason).toBe("signal_sigint");
  expect(state?.terminal).toBeUndefined();
  expect((await readDurableState(harness)).revision).toBe(22);
});
