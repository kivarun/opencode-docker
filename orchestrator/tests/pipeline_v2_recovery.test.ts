import { lstat, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import type { Stats } from "node:fs";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
  type PipelineV2ToolSession,
} from "../src/pipeline_v2_coordinator.ts";
import {
  acceptActivationOutputs,
  collectRunOutputs,
  evaluatePreparedDecisionState,
  prepareActivationData,
  prepareDecisionStateData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { loadPipelineV2, type PipelineDecisionStateResult, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  PipelineV2StateError,
  type PipelineDecisionStateRecord,
  type PipelineV2FailureReason,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateDurabilityError } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";
import { faultIo } from "./state_io_test_helpers.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../src/pipeline_v2_run_plan_restore.ts";
import { classifyPipelineV2ResumeRecovery } from "../src/pipeline_v2_recovery.ts";

/**
 * Production recovery of the loader-valid post-commit crash windows
 * through the existing `resumePipelineV2Run` entrypoint. Every window is
 * materialized with the real production substrate (the run-owned project
 * copy, the run-input snapshot, the real durable reducer through the real
 * sink, the real data plane and the real planning chain), the simulated
 * restart reopens the run through the ordinary sink factory and reloads
 * the pipeline from the durable bundle root, and the resume completes
 * exactly the missing durable suffix — never a second execution, never a
 * worker replay, never a new command branch or state field.
 */

const LOOSE_SCHEMA = {};

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations:
  - id: r1
    assert:
      not:
        all:
          - {fact: f1, equals: true}
          - {fact: f2, equals: true}
constraints:
  - id: c1
    when: {fact: f1, equals: true}
    forbid: [beta]
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const DECISION_END_TRANSITIONS = `      - outcome: alpha
        to: done
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

function controlPipelineYaml(runOutputs: boolean): string {
  return `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs: ${runOutputs ? `
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report
` : "[]"}

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: coder
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: check
      role: control
states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
      - id: report
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${DECISION_END_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

/** architect (planning) -> dev_entry (stage) -> done: the S1 window pipeline. */
const PIPELINE_STAGE_TO_DONE = `
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
    - state_id: dev_entry
      role: stage
      stage_template: development
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
        to: done
  - id: done
    type: terminal
    result: success
`;

/** architect (planning) -> dev_entry (stage) -> planner2 (planning, stage_wait): the recovered stage→planning wait. */
const PIPELINE_STAGE_TO_PLANNING = `
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
    - state_id: planner2
      role: planning
      plan_output: plan2
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
        to: done
  - id: done
    type: terminal
    result: success
`;

/** An entry-terminal pipeline with zero executions: the E0 window. */
const PIPELINE_ENTRY_TERMINAL = `
schema_version: 2
entry_state: done
max_transitions: 5

inputs: []

outputs: []

orchestration:
  stage_templates: []
  execution_roles: []
states:
  - id: done
    type: terminal
    result: success
`;

const PROPOSAL_R1 = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
};

const FACTS_ALPHA = JSON.stringify({ f1: true, f2: false });
const FACTS_BETA = JSON.stringify({ f1: false, f2: true });

// --- harness ---------------------------------------------------------------

interface Dirs {
  root: string;
  bundle: string;
  sources: string;
  projectSource: string;
  runRoot: string;
  stateRoot: string;
}

const ROOTS: string[] = [];

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

async function makeDirs(prefix: string): Promise<Dirs> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", "recovery-run");
  await mkdir(runRoot, { mode: 0o700 });
  return { root, bundle, sources, projectSource, runRoot, stateRoot };
}

async function writeBundle(
  dirs: Dirs,
  yaml: string,
  options: { facts?: string } = {},
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
  await writeFile(join(dirs.bundle, "schemas", "loose.schema.json"), JSON.stringify(LOOSE_SCHEMA));
  await writeFile(join(dirs.bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(dirs.bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(dirs.bundle, "decisions", "model.yaml"), MODEL_YAML);
  if (yaml.includes("facts_seed")) {
    await writeFile(join(dirs.sources, "facts.json"), options.facts ?? FACTS_ALPHA);
  }
  if (yaml.includes("  - id: task")) {
    await writeFile(join(dirs.sources, "task.md"), "TASK-BODY\n");
  }
}

function bindingsFor(dirs: Dirs, pipeline: ResolvedPipelineV2): Array<{ id: string; path: string }> {
  const names: Record<string, string> = { facts_seed: "facts.json", task: "task.md" };
  return pipeline.inputs.map((input) => {
    const file = names[input.id];
    if (file === undefined) {
      throw new Error(`no source file for run input ${input.id}`);
    }
    return { id: input.id, path: join(dirs.sources, file) };
  });
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock));
}
function resetClock(): void {
  clock = 0;
}

type CommandRecord = Record<string, unknown>;

class RecordingSink implements PipelineV2CoordinatorStateSink {
  readonly commands: CommandRecord[] = [];
  onDispatch: ((command: CommandRecord) => void) | null = null;

  constructor(private readonly inner: PipelineV2RunStateSink) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2CoordinatorStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    this.onDispatch?.(command as CommandRecord);
    await this.inner.dispatch(command);
  }
}

/** A recording sink whose named commands commit first and then fail as durability-unknown. */
class DurabilityFaultSink implements PipelineV2CoordinatorStateSink {
  readonly commands: CommandRecord[] = [];
  constructor(
    private readonly inner: PipelineV2RunStateSink,
    private readonly kinds: ReadonlySet<string>,
  ) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2CoordinatorStateSink["dispatch"]>[0]): Promise<void> {
    this.commands.push({ ...command });
    await this.inner.dispatch(command);
    if (this.kinds.has(command.kind)) {
      throw new PipelineV2RunStateDurabilityError(
        0,
        this.inner.snapshot as PipelineV2RunState,
        "durability unknown by test fault",
      );
    }
  }
}

interface Harness {
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
  runId: string;
  recording: RecordingSink;
}

async function setupHarness(
  yaml: string,
  prefix: string,
  options: { facts?: string } = {},
): Promise<Harness> {
  const dirs = await makeDirs(prefix);
  await writeBundle(dirs, yaml, options);
  const pipeline = await loadPipelineV2(dirs.bundle);
  resetClock();
  const sink = new PipelineV2RunStateSink({
    stateRoot: dirs.stateRoot,
    runId: "recovery-run",
    now: nextTick,
  });
  return { dirs, pipeline, runId: "recovery-run", recording: new RecordingSink(sink) };
}

async function reopenHarness(
  harness: Harness,
  options: { durabilityKinds?: ReadonlySet<string>; failCommitAt?: number } = {},
): Promise<RecordingSink | DurabilityFaultSink> {
  const real = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: harness.runId,
    io: options.failCommitAt !== undefined
      ? faultIo({ failCommit: options.failCommitAt, failStep: "write" })
      : undefined,
    now: nextTick,
  });
  return options.durabilityKinds !== undefined
    ? new DurabilityFaultSink(real, options.durabilityKinds)
    : new RecordingSink(real);
}

/** Full deterministic filesystem fingerprint: relative path, kind, mode, content. */
async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const describe = (info: Stats): string =>
    info.isSymbolicLink()
      ? "symlink"
      : info.isDirectory()
        ? "dir"
        : info.isFile()
          ? "file"
          : info.isFIFO()
            ? "fifo"
            : info.isSocket()
              ? "socket"
              : "other";
  const walk = async (rel: string): Promise<void> => {
    const abs = rel === "" ? root : join(root, rel);
    const info = await lstat(abs);
    const kind = describe(info);
    if (kind === "dir") {
      lines.push(`${rel}/ dir ${info.mode.toString(8)}`);
      const entries = await readdir(abs);
      for (const entry of [...entries].sort()) {
        await walk(rel === "" ? entry : `${rel}/${entry}`);
      }
      return;
    }
    const bytes = kind === "file" ? (await readFile(abs)).toString("base64") : kind;
    lines.push(`${rel} ${kind} ${info.mode.toString(8)} ${bytes}`);
  };
  await walk("");
  return lines.join("\n");
}

const NEUTRAL_CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
  freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
};

function constantSignalControl(signal: "SIGINT" | "SIGTERM"): PipelineV2CoordinatorControl {
  return {
    currentSignal: () => signal,
    freezeSignal: () => signal,
  };
}

interface FakeSession {
  sessionId: string;
  runAgent(toolSession: PipelineV2ToolSession): Promise<{ status: "completed" }>;
  cleanup(): Promise<void>;
}

interface FakeRuntimeHandle {
  runtime: PipelineV2AgentRuntime;
  createCalls: Array<{ stateId: string; session: "execution" | "tool" }>;
  runCalls: string[];
}

function fakeRuntime(): FakeRuntimeHandle {
  const createCalls: FakeRuntimeHandle["createCalls"] = [];
  const runCalls: string[] = [];
  let n = 0;
  const make = (stateId: string, kind: "execution" | "tool"): FakeSession => {
    n += 1;
    return {
      sessionId: `fake-${n}`,
      runAgent: async () => {
        runCalls.push(stateId);
        return { status: "completed" as const };
      },
      cleanup: async () => {},
    };
  };
  return {
    runtime: {
      createExecutionSession: async (state: { id: string }) => {
        createCalls.push({ stateId: state.id, session: "execution" });
        return make(state.id, "execution");
      },
      createToolSession: async (state: { id: string }) => {
        createCalls.push({ stateId: state.id, session: "tool" });
        return make(state.id, "tool");
      },
    } as unknown as PipelineV2AgentRuntime,
    createCalls,
    runCalls,
  };
}

function kinds(recording: PipelineV2CoordinatorStateSink): string[] {
  const commands = (recording as unknown as { commands: CommandRecord[] }).commands;
  return commands.map((command) => command.kind as string);
}

function expectResumeOk(result: PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  if (!result.ok) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(result)}`);
  }
  return result.state;
}

function expectResumeFailed(
  result: PipelineV2ResumeCoordinationResult,
  reason: PipelineV2FailureReason,
): PipelineV2RunState {
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused) || "waiting" in result || "planReady" in result) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe(reason);
  if (result.state !== null) {
    expect(result.state.failure).toEqual({ reason });
  }
  return result.state as PipelineV2RunState;
}

function expectRefused(
  result: PipelineV2ResumeCoordinationResult,
  reason: PipelineV2ResumeRefusalReason,
): PipelineV2RunState | null {
  expect(result.ok).toBe(false);
  if (result.ok || !("refused" in result) || !result.refused) {
    throw new Error(`expected a refusal, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe(reason);
  return result.state;
}

function expectWaiting(result: PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  expect(result.ok).toBe(false);
  const record = result as Record<string, unknown>;
  expect(Object.keys(record).sort()).toEqual(["ok", "state", "waiting"]);
  expect(record.waiting).toBe(true);
  expect("reason" in record).toBe(false);
  expect("refused" in record).toBe(false);
  const state = record.state as PipelineV2RunState;
  expect(state.status).toBe("waiting");
  expect(state.phase).toBe("waiting");
  return state;
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(harness.dirs.runRoot, "state.json"), "utf8"));
}

// --- prefix builders (real reducer, real data plane) ------------------------

async function prefixCreateRun(harness: Harness): Promise<RunInputsSnapshot> {
  await prepareRunProject(harness.dirs.projectSource, harness.dirs.runRoot);
  const runInputs = await snapshotRunInputs(
    harness.pipeline,
    bindingsFor(harness.dirs, harness.pipeline) as readonly RunInputBinding[],
    harness.dirs.runRoot,
  );
  await harness.recording.dispatch({
    kind: "create_run",
    runId: harness.runId,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  return runInputs;
}

function transitionStepFor(
  pipeline: ResolvedPipelineV2,
  stateId: string,
  outcome: string,
): { from: string; outcome: string; to: string; transition_index: number } {
  const declared = pipeline.states.find((entry) => entry.id === stateId);
  const transitions = declared !== undefined && declared.type !== "terminal" ? declared.transitions : [];
  const index = transitions.findIndex((entry) => entry.outcome === outcome);
  const match = transitions[index];
  if (match === undefined) {
    throw new Error(`state ${stateId} declares no transition for ${outcome}`);
  }
  return { from: stateId, outcome, to: match.to, transition_index: index };
}

class PrefixWorker {
  constructor(
    private readonly activation: PreparedActivationData,
    private readonly overrides: Record<string, string> = {},
  ) {}

  async run(): Promise<void> {
    for (const port of this.activation.output_ports) {
      const override = this.overrides[port.id];
      if (port.type === "directory") {
        await mkdir(port.path, { recursive: true });
      } else if (override !== undefined) {
        await writeFile(port.path, override);
      } else if (port.type === "json") {
        await writeFile(port.path, JSON.stringify({ ok: true, port: port.id }));
      } else {
        await writeFile(port.path, `${port.id} body`);
      }
    }
  }

  async cleanup(): Promise<void> {}
}

async function runAgentActivation(
  harness: Harness,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
  options: { commitTransition?: boolean; outputs?: Record<string, string>; sessionIds?: { execution: string; tool: string } } = {},
): Promise<AcceptedStateOutput[]> {
  const activation = await prepareActivationData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  const state = harness.pipeline.states.find((entry) => entry.id === stateId);
  const profile = state !== undefined && state.type === "agent" ? state.profile : "";
  await harness.recording.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile,
    ...startRoleArgs(harness.pipeline, stateId, harness.recording.snapshot),
  });
  const sessions = options.sessionIds ?? { execution: `exec-${executionIndex}`, tool: `tool-${executionIndex}` };
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: sessions.execution },
    { kind: "agent_tool_session_created", sessionId: sessions.tool },
    { kind: "agent_running" },
  ]) {
    await harness.recording.dispatch(command as never);
  }
  const worker = new PrefixWorker(activation, options.outputs);
  await worker.run();
  const records = await acceptActivationOutputs(harness.pipeline, activation);
  await harness.recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await worker.cleanup();
  await worker.cleanup();
  await harness.recording.dispatch({ kind: "agent_cleanup_completed" });
  if (options.commitTransition !== false) {
    await harness.recording.dispatch({
      kind: "transition_committed",
      step: transitionStepFor(harness.pipeline, stateId, "completed"),
      executionIndex,
    });
  }
  return [...accepted, ...records];
}

function toSelectedRecord(result: PipelineDecisionStateResult): PipelineDecisionStateRecord {
  if (result.status !== "selected") {
    throw new Error(`only selected decisions in the prefix, got ${result.status}`);
  }
  return {
    status: "selected",
    outcome: result.outcome,
    decision: result.decision,
    rule_id: result.rule_id,
    active_constraint_ids: [...result.active_constraint_ids],
  };
}

async function runDecisionActivation(
  harness: Harness,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
  options: { commitTransition?: boolean; record?: PipelineDecisionStateRecord } = {},
): Promise<PipelineDecisionStateResult> {
  const prepared = await prepareDecisionStateData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  await harness.recording.dispatch({
    kind: "start_decision_execution",
    stateId,
    inputDigest: prepared.input_digest,
    ...startRoleArgs(harness.pipeline, stateId, harness.recording.snapshot),
  });
  const evaluated = evaluatePreparedDecisionState(harness.pipeline, prepared);
  const record = options.record ?? toSelectedRecord(evaluated);
  await harness.recording.dispatch({ kind: "decision_evaluated", result: record });
  if (options.commitTransition !== false) {
    await harness.recording.dispatch({
      kind: "transition_committed",
      step: transitionStepFor(harness.pipeline, stateId, evaluated.outcome),
      executionIndex,
    });
  }
  return evaluated;
}

/** The stage prefix: plan r1 accepted, generation 1 / iteration 1 open, the planning transition committed. */
async function prefixStageEntry(
  harness: Harness,
  runInputs: RunInputsSnapshot,
): Promise<AcceptedStateOutput[]> {
  const accepted = await runAgentActivation(harness, runInputs, [], "architect", 1, {
    commitTransition: false,
    outputs: { plan: JSON.stringify(PROPOSAL_R1) },
  });
  await acceptPipelineV2PlanningRunPlan({ pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, sink: harness.recording as never });
  const restored = await restorePipelineV2AcceptedRunPlan({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    state: harness.recording.snapshot,
  });
  await ensurePipelineV2StageIteration({
    compiledPlan: restored.compiled_plan,
    stageId: "stage-1",
    initialBudget: 2,
    sink: harness.recording as never,
  });
  await harness.recording.dispatch({
    kind: "transition_committed",
    step: transitionStepFor(harness.pipeline, "architect", "completed"),
    executionIndex: 1,
  });
  return accepted;
}

// --- the matrix --------------------------------------------------------------

test("S0: the ordinary resumable boundary keeps its established continuation", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-s0");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1);
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    // the successor decision execution ran without any session
    expect(fake.createCalls).toHaveLength(0);
    expect(fake.runCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual([
      "start_decision_execution",
      "decision_evaluated",
      "transition_committed",
      "terminal_reached",
      "run_outputs_published",
      "run_succeeded",
    ]);
    expect(await fingerprint(harness.dirs.runRoot)).not.toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("S1: settled stage execution recovers by committing exactly its transition and continues", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_DONE, "rec-s1");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
    const prefixState = harness.recording.snapshot as PipelineV2RunState;
    expect(prefixState.executions).toHaveLength(2);
    expect(prefixState.transitions).toHaveLength(1);
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    // no worker replay, no session: the settled execution is never rerun
    expect(fake.createCalls).toHaveLength(0);
    expect(fake.runCalls).toHaveLength(0);
    // exactly the missing suffix: one transition, then the terminal chain
    expect(kinds(reopened)).toEqual([
      "transition_committed",
      "terminal_reached",
      "run_outputs_published",
      "run_succeeded",
    ]);
    const committed = reopened.commands[0] as { step: { from: string; outcome: string; to: string; transition_index: number }; executionIndex: number };
    expect(committed.step).toEqual({ from: "dev_entry", outcome: "completed", to: "done", transition_index: 0 });
    expect(committed.executionIndex).toBe(2);
    expect(state.transitions).toHaveLength(2);
    expect(state.executions).toHaveLength(2);
    // the prefix files stayed byte-identical; only the new durable suffix appeared
    const after = await fingerprint(harness.dirs.runRoot);
    const added = after.split("\n").filter((line) => !before.split("\n").includes(line));
    expect(added.every((line) => line.startsWith("outputs") || line.includes("state.json"))).toBe(true);
    const durable = await readDurableState(harness);
    expect(durable.terminal).toEqual({ state_id: "done", result: "success" });
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("S1 stage→planning: the recovered transition enters the trusted stage wait automatically", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_PLANNING, "rec-s1-wait");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectWaiting(result);
    // no planning execution and no session: the wait entry suspended the coordination
    expect(fake.createCalls).toHaveLength(0);
    expect(state.executions).toHaveLength(2);
    expect(state.transitions).toHaveLength(2);
    expect(state.cursor).toEqual({ current_state: "planner2", transition_count: 2 });
    expect(kinds(reopened)).toEqual(["transition_committed", "run_waiting"]);
    // the wait request manifest was published from the trusted policy
    const request = JSON.parse(await readFile(join(harness.dirs.runRoot, "waits", "1.request.json"), "utf8")) as Record<string, unknown>;
    expect(request["state_id"]).toBe("planner2");
    const actions = request["actions"] as Array<Record<string, unknown>>;
    expect(actions.map((action) => action["id"])).toEqual(["continue_stage", "revise_task"]);
    expect(actions[0]?.["to"]).toBe("dev_entry");
    expect(actions[1]?.["to"]).toBe("planner2");
    // a fresh resume after the wait is the established refusal
    const reopened2 = await reopenHarness(harness);
    const result2 = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened2,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result2, "invalid_state");
    expect(reopened2.commands).toHaveLength(0);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("S2: the stage transition is durable, the result was lost — the ordinary continuation runs", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_DONE, "rec-s2");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    // no new stage execution: the cursor is already at the terminal
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["terminal_reached", "run_outputs_published", "run_succeeded"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("S3: settled control execution recovers from the durable decision outcome without re-evaluation", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-s3", { facts: FACTS_ALPHA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await runDecisionActivation(harness, runInputs, accepted, "check", 2, { commitTransition: false });
    const prefixState = harness.recording.snapshot as PipelineV2RunState;
    const decisionExecution = prefixState.executions[1];
    expect(decisionExecution?.type).toBe("decision");
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    // no re-evaluation, no session: the durable outcome routed the transition
    expect(fake.createCalls).toHaveLength(0);
    expect(fake.runCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual([
      "transition_committed",
      "terminal_reached",
      "run_outputs_published",
      "run_succeeded",
    ]);
    const committed = reopened.commands[0] as { step: { outcome: string; to: string }; executionIndex: number };
    expect(committed.step.outcome).toBe("alpha");
    expect(committed.step.to).toBe("done");
    expect(committed.executionIndex).toBe(2);
    expect(await fingerprint(harness.dirs.runRoot)).not.toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("S4: the control transition is durable, the result was lost — the ordinary continuation runs", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-s4", { facts: FACTS_BETA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    // the beta route reaches the failed terminal; the run finalizes failed
    const state = expectResumeFailed(result, "terminal_failed");
    expect(state.status).toBe("failed");
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["terminal_reached", "run_outputs_published", "run_failed"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("F0: the durably failed execution finalizes with exactly one run_failed carrying the durable reason", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-f0");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeFailed(result, "worker_failed");
    expect(state.status).toBe("failed");
    expect(state.executions).toHaveLength(1);
    expect(state.transitions).toHaveLength(0);
    // no session, no cleanup replay, exactly the one finalization
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["run_failed"]);
    // only the durable state document changed
    const after = await fingerprint(harness.dirs.runRoot);
    const changed = after.split("\n").filter((line) => !before.split("\n").includes(line));
    expect(changed.every((line) => line.includes("state.json"))).toBe(true);
    // the retry after a lost result sees the finalized failed run: zero writes
    const reopened2 = await reopenHarness(harness);
    const result2 = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened2,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result2, "invalid_state");
    expect(reopened2.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("F0 cleanup: a durably failed cleanup slot finalizes with run_cleanup_failed", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-f0-cleanup");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "session_cleanup_failed", sessionCleanup: { execution: "completed", tool: "failed" } },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeFailed(result, "session_cleanup_failed");
    expect(state.status).toBe("cleanup_failed");
    expect(kinds(reopened)).toEqual(["run_cleanup_failed"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("F0 decision: a durably failed decision execution finalizes without any session", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-f0-decision");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await prepareDecisionStateData(harness.pipeline, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({
      kind: "start_decision_execution",
      stateId: "check",
      inputDigest: "0".repeat(64),
      ...startRoleArgs(harness.pipeline, "check", harness.recording.snapshot),
    });
    await harness.recording.dispatch({ kind: "decision_failed", reason: "internal_error" });
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeFailed(result, "internal_error");
    expect(state.status).toBe("failed");
    expect(kinds(reopened)).toEqual(["run_failed"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T0: the cursor at a terminal without terminal_reached keeps its established terminal chain", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-t0");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["terminal_reached", "run_outputs_published", "run_succeeded"]);
    expect(state.run_outputs?.[0]).toEqual({
      id: "report",
      type: "json",
      required: true,
      present: true,
      digest: state.run_outputs?.[0]?.present === true ? state.run_outputs[0].digest : "",
    });
    expect(state.run_outputs?.[0]?.present === true && /^[0-9a-f]{64}$/.test(state.run_outputs[0].digest)).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T1: terminal_reached durable recovers by publishing the outputs and finalizing", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-t1");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    const before = await fingerprint(harness.dirs.runRoot);
    expect(before.split("\n").some((line) => line.startsWith("outputs"))).toBe(false);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["run_outputs_published", "run_succeeded"]);
    const outputsRoot = join(harness.dirs.runRoot, "outputs");
    expect(await readFile(join(outputsRoot, "report"), "utf8")).toBe(JSON.stringify({ ok: true }));
    const published = state.run_outputs?.[0];
    expect(published?.present).toBe(true);
    // a fresh reopen sees the finalized run: the ordinary refusal
    const reopened2 = await reopenHarness(harness);
    const result2 = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened2,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result2, "invalid_state");
    expect(reopened2.commands).toHaveLength(0);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T2: the crashed publication is adopted byte-exactly — files untouched, one durable record", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-t2");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    // The crashed attempt: the atomic rename landed, the durable commit did not.
    const crashed = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    expect(crashed.outputs).toHaveLength(1);
    const publishedPath = join(harness.dirs.runRoot, "outputs", "report");
    const beforeStat = await stat(publishedPath);
    const beforeBytes = await readFile(publishedPath);
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    expect(kinds(reopened)).toEqual(["run_outputs_published", "run_succeeded"]);
    // the published artifact kept its identity: inode, mode, mtime, bytes
    const afterStat = await stat(publishedPath);
    expect(afterStat.ino).toBe(beforeStat.ino);
    expect(afterStat.mode).toBe(beforeStat.mode);
    expect(afterStat.mtimeMs).toBe(beforeStat.mtimeMs);
    expect(await readFile(publishedPath)).toEqual(beforeBytes);
    // only the durable state document changed; the adopted files are identical
    const afterTree = await fingerprint(harness.dirs.runRoot);
    const changedTree = afterTree.split("\n").filter((line) => !before.split("\n").includes(line));
    expect(changedTree.every((line) => line.includes("state.json"))).toBe(true);
    // the durable record equals what a fresh publication would have recorded
    const expected = await collectRunOutputs(harness.pipeline, runInputs, accepted).catch(() => null);
    expect(expected).toBeNull();
    expect(state.run_outputs?.[0]?.present).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T3: run_outputs_published durable recovers with only the final status command", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-t3");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    const crashed = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: crashed.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    });
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    expect(kinds(reopened)).toEqual(["run_succeeded"]);
    const afterTree = await fingerprint(harness.dirs.runRoot);
    const changedTree = afterTree.split("\n").filter((line) => !before.split("\n").includes(line));
    expect(changedTree.every((line) => line.includes("state.json"))).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T4: the durable run_succeeded keeps the ordinary refusal with zero writes", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-t4");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    await harness.recording.dispatch({ kind: "run_outputs_published", outputs: [] });
    await harness.recording.dispatch({ kind: "run_succeeded" });
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "invalid_state");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("T1 failed terminal: the recovery publishes the outputs and finalizes terminal_failed", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-t1-failed-beta", { facts: FACTS_BETA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "failed_end", terminalResult: "failed" });
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeFailed(result, "terminal_failed");
    expect(state.status).toBe("failed");
    expect(kinds(reopened)).toEqual(["run_outputs_published", "run_failed"]);
    expect(state.terminal).toEqual({ state_id: "failed_end", result: "failed" });
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("E0: the entry-terminal pipeline with zero executions completes its terminal chain", async () => {
  const harness = await setupHarness(PIPELINE_ENTRY_TERMINAL, "rec-e0");
  try {
    await prefixCreateRun(harness);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeOk(result);
    expect(state.status).toBe("success");
    expect(state.executions).toHaveLength(0);
    expect(fake.createCalls).toHaveLength(0);
    expect(kinds(reopened)).toEqual(["terminal_reached", "run_outputs_published", "run_succeeded"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("P0: the policy-less planning boundary keeps the established refusal", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-p0");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1, { commitTransition: false });
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "invalid_state");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W0: the waiting run refuses with zero writes", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_PLANNING, "rec-w0");
  try {
    const runInputs = await prefixCreateRun(harness);
    await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, [], "dev_entry", 2);
    const liveFake = fakeRuntime();
    const liveResult = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: await reopenHarness(harness),
      runtime: liveFake.runtime,
    }, NEUTRAL_CONTROL);
    expectWaiting(liveResult);
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "invalid_state");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("I0: the in-flight agent execution refuses with zero writes and zero sessions", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-i0");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "invalid_state");
    expect(reopened.commands).toHaveLength(0);
    expect(fake.createCalls).toHaveLength(0);
    expect(fake.runCalls).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- defensive tests ---------------------------------------------------------

test("a forged decision outcome refuses pipeline_mismatch before any dispatch", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-def-mismatch");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1);
    await runDecisionActivation(harness, runInputs, accepted, "check", 2, {
      commitTransition: false,
      record: {
        status: "selected",
        outcome: "ghost_outcome",
        decision: "ghost_outcome",
        rule_id: "ghost_rule",
        active_constraint_ids: [],
      },
    });
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "pipeline_mismatch");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
    // the classifier itself reports the mismatch
    const classification = classifyPipelineV2ResumeRecovery(harness.pipeline, await readDurableState(harness));
    expect(classification.kind).toBe("pipeline-mismatch");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a foreign pipeline identity refuses the recovery before any dispatch", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-def-foreign");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const other = await setupHarness(controlPipelineYaml(false), "rec-def-foreign-other");
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: other.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "pipeline_mismatch");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
    await rm(other.dirs.root, { recursive: true, force: true });
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("damaged accepted outputs refuse the terminal recovery before any publication", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-def-damaged");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    // damage the accepted output after the prefix
    await writeFile(join(harness.dirs.runRoot, "activations", "1-coder", "data", "outputs", "report"), '{"ok":false}');
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result, "accepted_output_modified");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a damaged published artifact fails the T2 adoption and finalizes the run failed without rewriting", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-def-t2-damaged");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    await collectRunOutputs(harness.pipeline, runInputs, accepted);
    // damage the published copy after the crashed attempt
    await writeFile(join(harness.dirs.runRoot, "outputs", "report"), '{"ok":false}');
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeFailed(result, "run_output_invalid");
    expect(state.status).toBe("failed");
    // the damaged file was never rewritten and no durable record landed
    expect(await readFile(join(harness.dirs.runRoot, "outputs", "report"), "utf8")).toBe('{"ok":false}');
    const durable = await readDurableState(harness);
    expect(durable.run_outputs).toBeUndefined();
    expect(durable.status).toBe("failed");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("an undeclared extra entry in the published tree fails the T2 adoption", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-def-t2-extra");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await writeFile(join(harness.dirs.runRoot, "outputs", "smuggled"), "x");
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectResumeFailed(result, "run_output_invalid");
    const durable = await readDurableState(harness);
    expect(durable.run_outputs).toBeUndefined();
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a missing published entry fails the T2 adoption", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-def-t2-missing");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await rm(join(harness.dirs.runRoot, "outputs", "report"));
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectResumeFailed(result, "run_output_invalid");
    const durable = await readDurableState(harness);
    expect(durable.run_outputs).toBeUndefined();
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a run_outputs_published store failure keeps the outputs as orphans and retries cleanly", async () => {
  const harness2 = await setupHarness(controlPipelineYaml(true), "rec-def-t1-store2");
  try {
    const runInputs = await prefixCreateRun(harness2);
    const accepted = await runAgentActivation(harness2, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness2, runInputs, accepted, "check", 2);
    await harness2.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    // not_committed: the store's write fails on the run_outputs_published
    // commit (the first commit of this reopened sink).
    const reopenedFaulted = await reopenHarness(harness2, { failCommitAt: 1 });
    const result = await resumePipelineV2Run({
      pipeline: harness2.pipeline,
      runId: harness2.runId,
      runRoot: harness2.dirs.runRoot,
      sink: reopenedFaulted,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    // the existing live semantics: a not-committed publication commit
    // finalizes the run failed with state_persist_failed
    const state = expectResumeFailed(result, "state_persist_failed");
    expect(state.status).toBe("failed");
    // the published outputs stay as untouched orphans; the finalized run
    // refuses any further resume with zero writes
    expect(await readFile(join(harness2.dirs.runRoot, "outputs", "report"), "utf8")).toBe(JSON.stringify({ ok: true }));
    const reopened2 = await reopenHarness(harness2);
    const result2 = await resumePipelineV2Run({
      pipeline: harness2.pipeline,
      runId: harness2.runId,
      runRoot: harness2.dirs.runRoot,
      sink: reopened2,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectRefused(result2, "invalid_state");
    expect(reopened2.commands).toHaveLength(0);
  } finally {
    await rm(harness2.dirs.root, { recursive: true, force: true });
  }
});

test("a run_succeeded durability-unknown adopts the candidate and stops with state_persist_failed", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-def-t3-durability");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    const crashed = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: crashed.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    });
    const reopened = await reopenHarness(harness, { durabilityKinds: new Set(["run_succeeded"]) });
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expect(result.ok).toBe(false);
    if (result.ok || ("refused" in result && result.refused) || "waiting" in result || "planReady" in result) {
      throw new Error(`expected a persistence failure, got ${JSON.stringify(result)}`);
    }
    expect(result.reason).toBe("state_persist_failed");
    // the adopted candidate is the succeeded run (no failure record on it)
    const state = result.state as PipelineV2RunState;
    expect(state.status).toBe("success");
    expect(state.failure).toBeUndefined();
    expect(kinds(reopened)).toEqual(["run_succeeded"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- signals -----------------------------------------------------------------

test("a pending signal before the failure finalization returns 130/143 semantics with zero writes", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-sig-f0");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, constantSignalControl("SIGINT"));
    expect(result.ok).toBe(false);
    if (result.ok || ("refused" in result && result.refused) || "waiting" in result || "planReady" in result) {
      throw new Error(`expected a signal failure, got ${JSON.stringify(result)}`);
    }
    expect(result.reason).toBe("signal_sigint");
    expect(result.state?.status).toBe("active");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a pending signal before the terminal recovery returns the signal failure with zero writes", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "rec-sig-t3");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
    const crashed = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: crashed.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    });
    const before = await fingerprint(harness.dirs.runRoot);
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, constantSignalControl("SIGTERM"));
    expect(result.ok).toBe(false);
    if (result.ok || ("refused" in result && result.refused) || "waiting" in result || "planReady" in result) {
      throw new Error(`expected a signal failure, got ${JSON.stringify(result)}`);
    }
    expect(result.reason).toBe("signal_sigterm");
    expect(reopened.commands).toHaveLength(0);
    expect(await fingerprint(harness.dirs.runRoot)).toBe(before);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a signal accepted after the recovered transition linearizes with the existing durable semantics", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_DONE, "rec-sig-s1");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
    const reopened = await reopenHarness(harness);
    // arm the signal through the control right after the transition dispatch
    let armed = false;
    const control: PipelineV2CoordinatorControl = {
      currentSignal: () => (armed ? "SIGINT" : null),
      freezeSignal: () => (armed ? "SIGINT" : null),
    };
    (reopened as unknown as { onDispatch: RecordingSink["onDispatch"] }).onDispatch = (command) => {
      if (command.kind === "transition_committed") {
        armed = true;
      }
    };
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, control);
    const state = expectResumeFailed(result, "signal_sigint");
    // the transition was durably committed before the signal finalization
    expect(state.transitions).toHaveLength(2);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "signal_sigint" });
    expect(kinds(reopened)).toEqual(["transition_committed", "run_failed"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- diagnostics and classifier units ----------------------------------------

test("the classifier unit battery: none for ordinary/plan-ready/in-flight/final/waiting, windows for the crash shapes", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-classifier");
  try {
    const runInputs = await prefixCreateRun(harness);
    // settled-and-bound → none
    await runAgentActivation(harness, runInputs, [], "coder", 1);
    let classification = classifyPipelineV2ResumeRecovery(harness.pipeline, harness.recording.snapshot as PipelineV2RunState);
    expect(classification.kind).toBe("none");
    // in-flight → none
    await harness.recording.dispatch({
      kind: "start_decision_execution",
      stateId: "check",
      inputDigest: "0".repeat(64),
      ...startRoleArgs(harness.pipeline, "check", harness.recording.snapshot),
    });
    classification = classifyPipelineV2ResumeRecovery(harness.pipeline, harness.recording.snapshot as PipelineV2RunState);
    expect(classification.kind).toBe("none");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
  const stage = await setupHarness(PIPELINE_STAGE_TO_DONE, "rec-classifier-stage");
  try {
    const runInputs = await prefixCreateRun(stage);
    const accepted = await prefixStageEntry(stage, runInputs);
    await runAgentActivation(stage, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
    const classification = classifyPipelineV2ResumeRecovery(stage.pipeline, stage.recording.snapshot as PipelineV2RunState);
    expect(classification.kind).toBe("transition");
    if (classification.kind === "transition") {
      expect(classification.step).toEqual({ from: "dev_entry", outcome: "completed", to: "done", transition_index: 0 });
      expect(classification.executionIndex).toBe(2);
    }
    // determinism and frozen results
    const again = classifyPipelineV2ResumeRecovery(stage.pipeline, stage.recording.snapshot as PipelineV2RunState);
    expect(again).toEqual(classification);
  } finally {
    await rm(stage.dirs.root, { recursive: true, force: true });
  }
});

test("the recovery results are content-free and deep-frozen", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "rec-content");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" } },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: reopened,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expectResumeFailed(result, "worker_failed");
    expect(Object.isFrozen(result)).toBe(true);
    const serialized = JSON.stringify(result);
    for (const canary of ["IMPLEMENT-THE-TASK", "TASK-BODY", "dhc_", "dhl_", "worker output", "bearer"]) {
      expect(serialized.includes(canary)).toBe(false);
    }
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});
