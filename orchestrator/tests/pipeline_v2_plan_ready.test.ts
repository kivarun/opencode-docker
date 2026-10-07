/**
 * The production plan-ready suspension of the pipeline v2 coordinator: a
 * fully successful planning execution whose declared `completed` transition
 * targets a stage state stops the coordination after the durable
 * `agent_cleanup_completed` and before any transition commit — the
 * settled-but-unbound planning acceptance boundary that
 * `orchestrator resume-plan` consumes. The suspension is verified through
 * the single existing public `restorePipelineV2PlanningAcceptanceContext`
 * and returned as the exact plan-ready result branch by the coordinator,
 * the runner and the CLI. No plan output is read and no plan is accepted
 * automatically; the operator chain `run -> resume-plan -> waiting` stays
 * manual and `stageId`/`initialBudget` remain explicit operator policy.
 *
 * Every prefix is built through the real facades, the real reducer, the
 * real data plane and the real store over a real pipeline bundle; no LLM,
 * no Docker Helper, no launcher credential, no sleeps.
 */
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  coordinatePipelineV2Run,
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinationResult,
  type PipelineV2CoordinatorControl,
  type PipelineV2ResumeCoordinationResult,
} from "../src/pipeline_v2_coordinator.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { runPipelineV2, resumePipelineV2, revisePipelineV2Task, resumePipelineV2PlanningRunPlan, type PipelineV2RunnerDeps, type PipelineV2RunOutcome } from "../src/pipeline_v2_runner.ts";
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

type StageWaitPolicy = { readonly reason: string; readonly actions: readonly string[] };

const FULL_POLICY: StageWaitPolicy = { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] };

function pipelineYaml(shape: "cycle" | "control" = "cycle"): string {
  const gate = shape === "control"
    ? `  - id: gate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
        to: done
      - outcome: beta
        to: dev_entry
      - outcome: uncovered
        to: dev_entry
      - outcome: inconsistent_facts
        to: dev_entry
      - outcome: invalid_facts
        to: dev_entry
`
    : "";
  const inputs = shape === "control"
    ? `inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json
`
    : `inputs:
  - id: task
    type: file
    protected: true
`;
  const devTarget = shape === "cycle" ? "planner2" : "gate";
  const plannerRole = shape === "cycle"
    ? `    - state_id: planner2
      role: planning
      plan_output: plan2
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
`
    : "    - state_id: gate\n      role: control\n";
  const plannerState = shape === "cycle"
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
        to: dev_entry
`
    : "";
  return `
schema_version: 2
entry_state: architect
max_transitions: 40

${inputs}
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
        reason: ${FULL_POLICY.reason}
        actions:
${FULL_POLICY.actions.map((action) => `          - ${action}`).join("\n")}
${plannerRole}    - state_id: dev_entry
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
        to: ${shape === "control" ? "gate" : "dev_entry"}
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
        to: ${devTarget}
${plannerState}${gate}  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
decisions:
  - id: alpha
  - id: beta
relations: []
constraints: []
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f1, equals: false}
    decision: beta
`;

const PROPOSAL_R1 = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
};

const INITIAL_BUDGET = 2;

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

async function makeHarness(
  prefix: string,
  runId: string,
  shape: "cycle" | "control" = "cycle",
  options: { createRunRoot?: boolean } = {},
): Promise<Harness> {
  clockValue = 0;
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  if (shape === "control") {
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  }
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(shape));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  if (shape === "cycle") {
    await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  }
  await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify({ type: "object" }));
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
  if (shape === "control") {
    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true }), { mode: 0o600 });
  }
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  if (options.createRunRoot !== false) {
    await mkdir(runRoot, { mode: 0o700 });
  }
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
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

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
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
 * The fake runtime of the coordinator proofs: sessions with unique ids, an
 * event log for the cleanup order, and a configurable worker result. The
 * tool session is created only after the execution session, mirroring the
 * production runtime.
 */
function fakeRuntime(prefix: string, runResults: "completed" | "worker_failed" | "worker_timeout" | "damage-outputs" = "completed"): {
  runtime: PipelineV2AgentRuntime;
  events: string[];
  sessionIds: string[];
} {
  const events: string[] = [];
  const sessionIds: string[] = [];
  let n = 0;
  let currentOutputsRoot = "";
  let currentStateId = "";
  const make = (kind: "execution" | "tool"): SessionPair => {
    n += 1;
    const id = `${prefix}-${kind}-${n}`;
    sessionIds.push(id);
    return {
      sessionId: id,
      runAgent: async () => {
        events.push(`run:${kind}`);
        if (runResults === "worker_failed") {
          return { status: "failed" as const, reason: "worker_failed" as const };
        }
        if (runResults === "worker_timeout") {
          return { status: "failed" as const, reason: "worker_timeout" as const };
        }
        if (kind === "execution" && currentOutputsRoot !== "") {
          const { writeFile: wf } = await import("node:fs/promises");
          if (currentStateId === "architect") {
            await wf(join(currentOutputsRoot, "plan"), JSON.stringify(PROPOSAL_R1), { mode: 0o600 });
          }
          if (currentStateId === "planner2") {
            await wf(join(currentOutputsRoot, "plan2"), JSON.stringify({
            schema_version: 1,
            kind: "run_plan_proposal",
            stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
            new_tasks: [],
          }), { mode: 0o600 });
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

async function bindingsFor(harness: Harness, shape: "cycle" | "control"): Promise<readonly RunInputBinding[]> {
  const bindings: RunInputBinding[] = [{ id: "task", path: join(harness.sources, "task.md") }];
  if (shape === "control") {
    bindings.push({ id: "facts_seed", path: join(harness.sources, "facts.json") });
  }
  return bindings;
}

/**
 * The honest plan-ready prefix used by the resume proofs: the plan r1
 * accepted, generation 1 / iteration 1 open and the planning transition
 * committed. No stage execution has run yet and no wait exists.
 */
async function prefixStageEntry(
  harness: Harness,
  runId: string,
  proposal: unknown = PROPOSAL_R1,
  options: { enterWait?: boolean } = {},
): Promise<{ recording: RecordingSink; raw: PipelineV2RunStateSink; runInputs: RunInputsSnapshot; pipeline: ResolvedPipelineV2 }> {
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  await prepareRunProject(harness.projectSource, harness.runRoot);
  const runInputs = await snapshotRunInputs(pipeline, await bindingsFor(harness, "cycle"), harness.runRoot);
  await recording.dispatch({
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
  await runPlanningActivation(pipeline, runInputs, [], recording, "architect", 1, proposal);
  const { acceptPipelineV2PlanningRunPlan } = await import("../src/pipeline_v2_planning_run_plan_controller.ts");
  const acceptedPlan = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: recording as never });
  const { ensurePipelineV2StageIteration } = await import("../src/pipeline_v2_stage_iteration_controller.ts");
  await ensurePipelineV2StageIteration({
    compiledPlan: acceptedPlan.compiled_plan,
    stageId: "stage-1",
    initialBudget: INITIAL_BUDGET,
    sink: recording as never,
  });
  await recording.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  if (options.enterWait === true) {
    // the stage execution inside iteration 1, then the stage->planning
    // transition and the trusted wait entry at the planner2 boundary
    await runStageActivation(pipeline, runInputs, [], recording, "dev_entry", 2);
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "planner2", transition_index: 0 },
      executionIndex: 2,
    });
    const { enterPipelineV2Wait } = await import("../src/pipeline_v2_wait_controller.ts");
    await enterPipelineV2Wait({
      runRoot: harness.runRoot,
      sink: recording as never,
      reason: FULL_POLICY.reason,
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        // the revise_task target is the destination planning state itself
        // (the production derivation's shape)
        { id: "revise_task", to: "planner2" },
      ],
    });
  }
  return { recording, raw, runInputs, pipeline };
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
    { kind: "agent_execution_session_created", sessionId: `plan-exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `plan-tool-${executionIndex}` },
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
    { kind: "agent_execution_session_created", sessionId: `stage-exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `stage-tool-${executionIndex}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ]) {
    await sink.dispatch(command as never);
  }
}

/** Opens the existing run through the ordinary sink factory. */
async function reopen(harness: Harness, runId: string): Promise<PipelineV2RunStateSink> {
  return await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
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

// --- coordinator-level proofs ------------------------------------------------

test("1/2/3/13/16/22. the fresh planning run suspends plan-ready: exact commands, no transition, no stage session, no wait, cleanup tool-first", async () => {
  const runId = "plan-ready-fresh";
  const harness = await makeHarness("plan-ready-fresh", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  const fake = fakeRuntime("fresh");
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  const state = expectPlanReady(result);
  expect(state.executions).toHaveLength(1);
  const execution = state.executions[0];
  expect(execution?.type).toBe("agent");
  expect(execution?.state_id).toBe("architect");
  expect(execution?.execution_role).toBe("planning");
  expect(execution?.phase).toBe("cleanup_completed");
  expect(execution?.iteration_index).toBeUndefined();
  expect(execution !== undefined && execution.type === "agent" ? execution.outputs?.map((output) => output.id) : null).toEqual(["plan"]);
  expect(state.transitions).toHaveLength(0);
  expect(state.cursor).toEqual({ current_state: "architect", transition_count: 0 });
  expect(state.generations).toHaveLength(0);
  expect(state.waits).toHaveLength(0);
  expect(recording.commands.map((command) => command.kind)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
  ]);
  expect(recording.commands.every((command) => command.kind !== "run_waiting")).toBe(true);
  expect(recording.commands.every((command) => command.kind !== "transition_committed")).toBe(true);
  expect(recording.commands.every((command) => command.kind !== "run_failed")).toBe(true);
  expect(recording.commands.every((command) => command.kind !== "run_succeeded")).toBe(true);
  // the exact worker run/cleanup order inside the activation: the worker
  // runs, then the tool cleanup before the execution cleanup
  expect(fake.events).toEqual([
    "run:execution",
    "cleanup:tool",
    "cleanup:execution",
  ]);
  expect(fake.sessionIds).toEqual(["fresh-execution-1", "fresh-tool-2"]);
  const acceptedCommand = recording.commands.find((command) => command.kind === "agent_outputs_accepted") as { outputs: { id: string }[] } | undefined;
  expect(acceptedCommand?.outputs.map((output) => output.id)).toEqual(["plan"]);
  // the plan-ready boundary round-trips through the loader
  const durable = await readDurableState(harness);
  expect(durable.status).toBe("active");
  expect(durable.transitions).toHaveLength(0);
});

test("12. a signal accepted before the plan-ready checkpoint wins with the previous outcome and no transition", async () => {
  const runId = "plan-ready-signal";
  const harness = await makeHarness("plan-ready-signal", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  let signalAccepted = false;
  const control: PipelineV2CoordinatorControl = {
    currentSignal: () => (signalAccepted ? "SIGINT" : null),
    freezeSignal: () => (signalAccepted ? "SIGINT" : null),
  };
  const fake = fakeRuntime("signal");
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime: ({
        ...fake.runtime,
        createExecutionSession: (async (_view: unknown, activation: PreparedActivationData) => {
          const pair = (await fake.runtime.createExecutionSession({ id: "architect" } as never, activation)) as unknown as { runAgent: () => Promise<unknown> };
          const originalRunAgent = pair.runAgent.bind(pair);
          pair.runAgent = async () => {
            // the signal is accepted while the worker run is in flight
            signalAccepted = true;
            await writeFile(join(activation.outputs_root, "plan"), JSON.stringify(PROPOSAL_R1), { mode: 0o600 });
            return await originalRunAgent();
          };
          return pair as never;
        }) as never,
      }) as unknown as PipelineV2AgentRuntime,
    },
    control,
  );
  expect(result.ok).toBe(false);
  expect("planReady" in result).toBe(false);
  expect("waiting" in result).toBe(false);
  if (result.ok || "waiting" in result || "planReady" in result || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("signal_sigint");
  expect(result.state.status).toBe("failed");
  expect(result.state.failure).toEqual({ reason: "signal_sigint" });
  expect(result.state.transitions).toHaveLength(0);
  // the exact lifecycle through the cleanup, then the signal failure — no
  // transition, no suspension
  expect(recording.commands.map((command) => command.kind)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "run_failed",
  ]);
});

test("9. a failed planning execution stays an ordinary worker failure with no plan-ready suspension", async () => {
  const runId = "plan-ready-worker-failed";
  const harness = await makeHarness("plan-ready-worker-failed", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  const fake = fakeRuntime("failed", "worker_failed");
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  expect(result.ok).toBe(false);
  expect("planReady" in result).toBe(false);
  expect("waiting" in result).toBe(false);
  if (result.ok || "waiting" in result || "planReady" in result || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("worker_failed");
  expect(result.state.status).toBe("failed");
  expect(result.state.failure).toEqual({ reason: "worker_failed" });
  const execution = result.state.executions[0];
  expect(execution?.type).toBe("agent");
  expect(execution?.execution_role).toBe("planning");
  expect(execution?.phase).toBe("failed");
  expect(execution?.failure_reason).toBe("worker_failed");
  expect(result.state.transitions).toHaveLength(0);
  expect(recording.commands.map((command) => command.kind)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_failed",
    "run_failed",
  ]);
});

test("10/11. planning->control and planning->terminal flows keep committing their transitions and running to the terminal", async () => {
  // The control shape: architect -> gate (control decision) -> done. The
  // gate's facts come from the pipeline input; no stage template state is
  // ever started, so the planning transition commits normally.
  const runId = "plan-ready-control";
  const harness = await makeHarness("plan-ready-control", runId, "control");
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  const fake = fakeRuntime("control-flow");
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "control"),
      sink: recording as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  const state = expectOkResult(result);
  expect(state.status).toBe("success");
  expect(state.transitions).toEqual([
    { index: 0, from: "architect", outcome: "completed", to: "gate", execution_index: 1 },
    { index: 0, from: "gate", outcome: "alpha", to: "done", execution_index: 2 },
  ]);
  expect(state.executions.map((execution) => execution.state_id)).toEqual(["architect", "gate"]);
  expect(recording.commands.map((command) => command.kind)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "start_decision_execution",
    "decision_evaluated",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
});

function expectOkResult(result: PipelineV2CoordinationResult): PipelineV2RunState {
  if (!result.ok || "waiting" in result || "planReady" in result) {
    throw new Error("expected a successful coordination");
  }
  return result.state;
}

test("14. a verification failure inside the plan-ready seam propagates through the existing failure chain with no transition", async () => {
  const runId = "plan-ready-damaged";
  const harness = await makeHarness("plan-ready-damaged", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  // the fake runtime damages the accepted planning output during the
  // cleanup phase — the accepted output no longer matches its digest when
  // the plan-ready seam verifies the acceptance boundary
  const runtime = {
    createExecutionSession: (async (_view: unknown, activation: PreparedActivationData) => ({
      sessionId: "damaged-exec-1",
      runAgent: async () => {
        await writeFile(join(activation.outputs_root, "plan"), JSON.stringify(PROPOSAL_R1), { mode: 0o600 });
        return { status: "completed" as const };
      },
      cleanup: async () => {
        await writeFile(join(activation.outputs_root, "plan"), "{}", { mode: 0o600 });
      },
    })) as never,
    createToolSession: (async () => ({
      sessionId: "damaged-tool-1",
      cleanup: async () => {},
    })) as never,
  } as unknown as PipelineV2AgentRuntime;
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime,
    },
    CONTROL,
  );
  expect(result.ok).toBe(false);
  expect("planReady" in result).toBe(false);
  expect("waiting" in result).toBe(false);
  if (result.ok || "waiting" in result || "planReady" in result || result.state === null) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("accepted_output_modified");
  expect(result.state.status).toBe("failed");
  expect(result.state.failure).toEqual({ reason: "accepted_output_modified" });
  expect(result.state.transitions).toHaveLength(0);
  expect(recording.commands.map((command) => command.kind)).toEqual([
    "create_run",
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "run_failed",
  ]);
});

test("15. a forged pipeline is rejected by the provenance gate before any effect", async () => {
  const runId = "plan-ready-forged";
  const harness = await makeHarness("plan-ready-forged", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  const forged = { ...pipeline } as unknown as ResolvedPipelineV2;
  const fake = fakeRuntime("forged");
  const result = await coordinatePipelineV2Run(
    {
      pipeline: forged,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime: fake.runtime,
    },
    CONTROL,
  );
  expect(result.ok).toBe(false);
  expect("planReady" in result).toBe(false);
  expect("waiting" in result).toBe(false);
  if (result.ok || "waiting" in result || "planReady" in result) {
    throw new Error("unexpected result shape");
  }
  expect(result.reason).toBe("internal_error");
  expect(result.state).toBeNull();
  expect(recording.commands).toHaveLength(0);
  expect(fake.sessionIds).toHaveLength(0);
});

// --- runner-level proofs -----------------------------------------------------

let runnerSessionCounter = 0;

function runnerDeps(harness: Harness, runId: string, options: { planBody?: string } = {}): PipelineV2RunnerDeps {
  const planBody = options.planBody ?? "Body A";
  const cli = async (args: readonly string[]) => {
    if (args[0] === "session" && args[1] === "create") {
      runnerSessionCounter += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${runnerSessionCounter}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_${runnerSessionCounter}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    if (args[0] === "run") {
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
          const parts = (source ?? "").split("/");
          const activationIndex = parts[1]?.split("-")[0] ?? "";
          const stateId = parts[1]?.split("-").slice(1).join("-") ?? "";
          const dir = join(harness.runRoot, source ?? "");
          await mkdir(dir, { recursive: true });
          if (stateId === "architect") {
            // the first planning execution proposes the initial plan; every
            // later planning execution is the pointer-only replanning form
            const proposal = activationIndex === "1"
              ? {
                  schema_version: 1,
                  kind: "run_plan_proposal",
                  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
                  new_tasks: [{ id: "task-a", body: planBody }],
                }
              : {
                  schema_version: 1,
                  kind: "run_plan_proposal",
                  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
                  new_tasks: [],
                };
            await writeFile(join(dir, "plan"), JSON.stringify(proposal), { mode: 0o600 });
          }
          if (stateId === "planner2") {
            await writeFile(join(dir, "plan2"), JSON.stringify({
            schema_version: 1,
            kind: "run_plan_proposal",
            stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
            new_tasks: [],
          }), { mode: 0o600 });
          }
        }
      }
      return { code: 0 };
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
    now: nextTick,
  };
}

const EXPECTED_LAUNCHER_ID = "dhl_plan_ready";

function expectPlanReadyOutcome(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(false);
  expect(outcome.planReady).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect(outcome.waiting).toBeUndefined();
  expect("reason" in outcome).toBe(false);
  expect("refused" in outcome).toBe(false);
  expect(Object.keys(outcome).sort()).toEqual([
    "exitCode",
    "ok",
    "planReady",
    "runId",
    "runRoot",
    "state",
  ]);
  expect(outcome.runId).not.toBe("");
  expect(outcome.runRoot).not.toBeNull();
  expect(outcome.state?.status).toBe("active");
  return outcome.state as PipelineV2RunState;
}

function expectWaitingOutcome(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(false);
  expect(outcome.waiting).toBe(true);
  expect(outcome.planReady).toBeUndefined();
  expect(outcome.exitCode).toBe(0);
  expect("reason" in outcome).toBe(false);
  expect(outcome.state?.status).toBe("waiting");
  return outcome.state as PipelineV2RunState;
}

test("17. the fresh runner run suspends plan-ready with the exact outcome shape and no stage session", async () => {
  const runId = "runner-plan-ready";
  const harness = await makeHarness("runner-plan-ready", runId, "cycle", { createRunRoot: false });
  const outcome = await runPipelineV2(
    {
      pipelineRoot: harness.bundle,
      configRoot: harness.configRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }],
    },
    { ...runnerDeps(harness, runId), randomId: () => runId },
  );
  const state = expectPlanReadyOutcome(outcome);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot);
  expect(state.transitions).toHaveLength(0);
  expect(state.executions).toHaveLength(1);
  expect(state.executions[0]?.execution_role).toBe("planning");
  expect(state.waits).toHaveLength(0);
  // no request manifest exists at the plan-ready boundary
  const waitsDir = join(harness.runRoot, "waits");
  const entries: Array<{ name: string }> = await readdir(waitsDir, { withFileTypes: true }).catch(() => []);
  expect(entries.some((entry) => entry.name.endsWith(".request.json"))).toBe(false);
  const durable = await readDurableState(harness);
  expect(durable.status).toBe("active");
});

test("20/21. the ordinary resume refuses the plan-ready boundary with zero durable writes", async () => {
  const runId = "runner-plan-ready-refusal";
  const harness = await makeHarness("runner-plan-ready-refusal", runId, "cycle", { createRunRoot: false });
  const planReady = await runPipelineV2(
    {
      pipelineRoot: harness.bundle,
      configRoot: harness.configRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }],
    },
    { ...runnerDeps(harness, runId), randomId: () => runId },
  );
  expectPlanReadyOutcome(planReady);
  const before = await fingerprint(harness.runRoot);
  const refused = await resumePipelineV2(
    { runId, configRoot: harness.configRoot },
    runnerDeps(harness, runId),
  );
  expect(refused.ok).toBe(false);
  expect(refused.planReady).toBeUndefined();
  expect(refused.waiting).toBeUndefined();
  expect(refused.exitCode).toBe(1);
  expect(refused.reason).toBe("invalid_state");
  expect(refused.state?.status).toBe("active");
  expect(refused.runId).toBe(runId);
  expect(await fingerprint(harness.runRoot)).toBe(before);
});

test("7div. the initial plan-ready boundary continues through resume-plan: acceptance, generation, planning transition, stage execution and the stage wait", async () => {
  const runId = "runner-plan-ready-initial";
  const harness = await makeHarness("runner-plan-ready-initial", runId, "cycle", { createRunRoot: false });
  const ready = await runPipelineV2(
    {
      pipelineRoot: harness.bundle,
      configRoot: harness.configRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }],
    },
    { ...runnerDeps(harness, runId), randomId: () => runId },
  );
  expectPlanReadyOutcome(ready);
  const outcome = await resumePipelineV2PlanningRunPlan(
    { runId, stageId: "stage-1", initialBudget: INITIAL_BUDGET, configRoot: harness.configRoot },
    runnerDeps(harness, runId),
  );
  const state = expectWaitingOutcome(outcome);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot);
  // The exact durable initial handoff suffix: the accepted plan, the
  // opened generation/iteration of the selected stage, the committed
  // planning transition, the stage execution and the trusted stage wait.
  expect(state.plan_revisions).toHaveLength(1);
  expect(state.plan_revisions[0]?.revision).toBe(1);
  expect(state.task_revisions.map((task) => [task.task_id, task.revision])).toEqual([["task-a", 1]]);
  expect(state.generations).toHaveLength(1);
  expect(state.generations[0]?.stage_id).toBe("stage-1");
  expect(state.generations[0]?.initial_budget).toBe(INITIAL_BUDGET);
  expect(state.generations[0]?.opened_transition_count).toBe(0);
  expect(state.executions.map((execution) => [execution.state_id, execution.execution_role, execution.iteration_index ?? null])).toEqual([
    ["architect", "planning", null],
    ["dev_entry", "stage", 1],
  ]);
  expect(state.transitions.map((transition) => [transition.from, transition.to])).toEqual([
    ["architect", "dev_entry"],
    ["dev_entry", "planner2"],
  ]);
  expect(state.waits).toHaveLength(1);
  await stat(join(harness.runRoot, "waits", "1.request.json"));
  expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  // The lost-result retry: the run is waiting (the handoff boundary has
  // been passed by the resumed stage execution), so a repeated resume-plan
  // refuses with zero durable writes and the run tree byte-identical.
  const before = await fingerprint(harness.runRoot);
  const refused = await resumePipelineV2PlanningRunPlan(
    { runId, stageId: "stage-1", initialBudget: INITIAL_BUDGET, configRoot: harness.configRoot },
    runnerDeps(harness, runId),
  );
  expect(refused.ok).toBe(false);
  expect(refused.planReady).toBeUndefined();
  expect(refused.waiting).toBeUndefined();
  expect(refused.exitCode).toBe(1);
  expect("reason" in refused).toBe(false);
  expect(refused.runId).toBe(runId);
  expect(refused.state?.status).toBe("waiting");
  expect(await fingerprint(harness.runRoot)).toBe(before);
});

test("6/7/8/4/5. the revise cycle through the real entrypoints: revise-task -> plan-ready -> reopen -> resume-plan -> waiting", async () => {
  const runId = "runner-plan-ready-revise";
  const harness = await makeHarness("runner-plan-ready-revise", runId, "cycle");
  // The revise boundary prefix through the real facades (the same honesty
  // level the existing runner suites use): plan r1 accepted, generation
  // 1 / iteration 1 open, the planning transition committed, the stage
  // execution run to the stage->planning wait, the revise intent accepted.
  await prefixStageEntry(harness, runId, PROPOSAL_R1, { enterWait: true });
  const { applyPipelineV2ReviseTaskIntervention } = await import("../src/pipeline_v2_revise_task_intervention_controller.ts");
  const reopenedForIntervention = await reopen(harness, runId);
  const intervention = await applyPipelineV2ReviseTaskIntervention({
    pipeline: (await loadPipelineV2(harness.bundle)),
    runRoot: harness.runRoot,
    sink: reopenedForIntervention,
    runId,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "REVISED-TASK-BODY",
  });
  expect(intervention.state.status).toBe("active");
  expect(intervention.state.cursor.current_state).toBe("planner2");

  // the revise-task runner entrypoint: the planning execution runs and the
  // coordination suspends plan-ready (no transition, no stage start)
  clockValue = 0;
  const revised = await revisePipelineV2Task(
    { runId, waitIndex: 1, taskId: "task-a", taskBody: "REVISED-TASK-BODY", configRoot: harness.configRoot },
    runnerDeps(harness, runId, { planBody: "Body A" }),
  );
  const revisedState = expectPlanReadyOutcome(revised);
  expect(revisedState.executions.map((execution) => execution.state_id)).toEqual(["architect", "dev_entry", "planner2"]);
  expect(revisedState.executions[2]?.execution_role).toBe("planning");
  expect(revisedState.executions[2]?.phase).toBe("cleanup_completed");
  expect(revisedState.transitions).toHaveLength(2);
  expect(revisedState.transitions[1]?.from).toBe("dev_entry");
  expect(revisedState.transitions[1]?.to).toBe("planner2");
  expect(revisedState.cursor).toEqual({ current_state: "planner2", transition_count: 2 });
  expect(revisedState.waits).toHaveLength(1);
  expect(revisedState.waits[0]?.response?.action_id).toBe("revise_task");

  // the crash seam: the plan-ready result is lost; the reopened run is
  // recognized by resume-plan (the revise cycle is complete)
  const before = await fingerprint(harness.runRoot);
  await reopen(harness, runId);
  expect(await fingerprint(harness.runRoot)).toBe(before);

  clockValue = 0;
  const resumed = await resumePipelineV2PlanningRunPlan(
    { runId, stageId: "stage-1", initialBudget: INITIAL_BUDGET, configRoot: harness.configRoot },
    runnerDeps(harness, runId),
  );
  const waitingState = expectWaitingOutcome(resumed);
  // no duplicate planning execution: exactly one dev_entry stage execution
  // and the architect replanning execution already durable
  expect(waitingState.executions.map((execution) => execution.state_id)).toEqual([
    "architect",
    "dev_entry",
    "planner2",
    "dev_entry",
  ]);
  expect(waitingState.executions[3]?.execution_role).toBe("stage");
  expect(waitingState.executions[3]?.iteration_index).toBe(1);
  expect(waitingState.transitions.map((transition) => [transition.from, transition.to])).toEqual([
    ["architect", "dev_entry"],
    ["dev_entry", "planner2"],
    ["planner2", "dev_entry"],
    ["dev_entry", "planner2"],
  ]);
  expect(waitingState.generations).toHaveLength(2);
  expect(waitingState.waits).toHaveLength(2);
  const durable = await readDurableState(harness);
  expect(durable.status).toBe("waiting");
  expect(durable.failure).toBeUndefined();
  expect(durable.executions.filter((execution) => execution.state_id === "dev_entry")).toHaveLength(2);
  expect(durable.executions.filter((execution) => execution.state_id === "planner2")).toHaveLength(1);
});

test("4/5(crash seam of the served boundary). the revise-cycle crash: reopen writes nothing and resume-plan repeats no planning execution", async () => {
  // Covered in full by the revise-cycle proof below (the reopen, the
  // unchanged fingerprint and the zero-duplicate-planning resume-plan
  // continuation); this harness stays for the initial-boundary crash
  // semantics: the reopen alone writes nothing.
  const runId = "plan-ready-crash";
  const harness = await makeHarness("plan-ready-crash", runId, "cycle", { createRunRoot: false });
  const ready = await runPipelineV2(
    {
      pipelineRoot: harness.bundle,
      configRoot: harness.configRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: [{ id: "task", path: join(harness.sources, "task.md") }],
    },
    { ...runnerDeps(harness, runId), randomId: () => runId },
  );
  const readyState = expectPlanReadyOutcome(ready);
  expect(readyState.transitions).toHaveLength(0);
  const before = await fingerprint(harness.runRoot);
  await reopen(harness, runId);
  expect(await fingerprint(harness.runRoot)).toBe(before);
  const durable = await readDurableState(harness);
  expect(durable.executions.filter((execution) => execution.state_id === "architect")).toHaveLength(1);
  expect(durable.transitions).toHaveLength(0);
});
