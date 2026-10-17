import { lstat, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import type { Stats } from "node:fs";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
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
  type PipelineDecisionStateRecord,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../src/pipeline_v2_run_plan_restore.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import {
  inspectPipelineV2Run,
  type PipelineV2InspectOutcome,
} from "../src/pipeline_v2_inspect.ts";
import {
  restorePipelineV2PlanningAcceptanceContext,
  restorePipelineV2RuntimeContext,
} from "../src/pipeline_v2_resume_context.ts";
import { classifyPipelineV2ResumeRecovery } from "../src/pipeline_v2_recovery.ts";
import type { WorkerLogCapture } from "../src/worker_operation.ts";
import type { PipelineV2WorkerLogState } from "../src/pipeline_v2_state.ts";
import { parseWorkerLogArtifact } from "../src/pipeline_v2_worker_log.ts";
import { publishPipelineV2WorkerLog } from "../src/pipeline_v2_worker_log_store.ts";

/**
 * The versioned read-only `orchestrator inspect` command. Every state of
 * the proof matrix is materialized with the real production substrate
 * (the run-owned project copy, the run-input snapshot, the real durable
 * reducer through the real sink, the real data plane, the real planning
 * and intervention chains), the in-memory objects are dropped, and the
 * read-only inspect classifies the durable state through the single
 * authoritative recovery classifier and the authoritative restores. Every
 * test proves the zero-write guarantee with a full deterministic run-tree
 * fingerprint taken before and after the inspect call.
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

function decisionTransitions(alphaTo: string, betaTo: string): string {
  return `      - outcome: alpha
        to: ${alphaTo}
      - outcome: beta
        to: ${betaTo}
      - outcome: uncovered
        to: ${betaTo}
      - outcome: inconsistent_facts
        to: ${betaTo}
      - outcome: invalid_facts
        to: ${betaTo}`;
}

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
${decisionTransitions("done", "failed_end")}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

/** The absent-optional-output pipeline: `bonus` only runs on the beta route. */
function optionalOutputPipelineYaml(): string {
  return `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs:
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report
  - id: extra
    required: false
    source:
      state_output:
        state: bonus
        output: extra

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
    - state_id: bonus
      role: planning
      plan_output: extra
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
${decisionTransitions("done", "bonus")}
  - id: bonus
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: extra
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

function stageToDoneYaml(planReady: boolean): string {
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
      plan_output: plan${planReady ? `
      plan_ready:
        stage_position: 1
        initial_budget: 1` : ""}
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
}

/** architect (planning) -> dev_entry (stage) -> planner2 (planning, stage_wait). */
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

/** An entry-terminal pipeline with zero executions. */
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

const RUN_ID = "inspect-run";

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
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
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

class RecordingSink {
  readonly commands: CommandRecord[] = [];
  constructor(private readonly inner: PipelineV2RunStateSink) {}
  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }
  get poisoned(): boolean {
    return this.inner.poisoned;
  }
  async dispatch(command: never): Promise<void> {
    this.commands.push({ ...(command as CommandRecord) });
    await this.inner.dispatch(command);
  }
}

interface Harness {
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
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
    runId: RUN_ID,
    now: nextTick,
  });
  return { dirs, pipeline, recording: new RecordingSink(sink) };
}

async function reopenHarness(harness: Harness): Promise<RecordingSink> {
  const real = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: RUN_ID,
    now: nextTick,
  });
  return new RecordingSink(real);
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

interface FakeSession {
  sessionId: string;
  runAgent(toolSession: PipelineV2ToolSession): Promise<{ status: "completed" }>;
  cleanup(): Promise<void>;
}

interface FakeRuntimeHandle {
  runtime: PipelineV2AgentRuntime;
  createCalls: Array<{ stateId: string; session: "execution" | "tool" }>;
  runCalls: string[];
  cleanupCalls: Array<{ execution: string; tool: string }>;
}

function fakeRuntime(cleanup: { failExecution?: boolean; failTool?: boolean } = {}): FakeRuntimeHandle {
  const createCalls: FakeRuntimeHandle["createCalls"] = [];
  const runCalls: string[] = [];
  const cleanupCalls: FakeRuntimeHandle["cleanupCalls"] = [];
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
      cleanupSessionsByIds: async (executionSessionId: string, toolSessionId: string) => {
        cleanupCalls.push({ execution: executionSessionId, tool: toolSessionId });
        return {
          execution: cleanup.failExecution ? ("failed" as const) : ("completed" as const),
          tool: cleanup.failTool ? ("failed" as const) : ("completed" as const),
        };
      },
    } as unknown as PipelineV2AgentRuntime,
    createCalls,
    runCalls,
    cleanupCalls,
  };
}

async function readDurableState(harness: Harness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(harness.dirs.runRoot, "state.json"), "utf8"));
}

/** The one read-only inspect call under test, with its fingerprint window. */
async function inspectRun(
  harness: Harness,
): Promise<{ before: string; after: string; outcome: PipelineV2InspectOutcome }> {
  const before = await fingerprint(harness.dirs.runRoot);
  const outcome = await inspectPipelineV2Run({ runId: RUN_ID, stateRoot: harness.dirs.stateRoot });
  const after = await fingerprint(harness.dirs.runRoot);
  return { before, after, outcome };
}

function expectReport(outcome: PipelineV2InspectOutcome): Extract<PipelineV2InspectOutcome, { ok: true }> {
  if (!outcome.ok) {
    throw new Error(`expected an inspect report, got ${JSON.stringify(outcome)}`);
  }
  return outcome;
}

function expectFailure(
  outcome: PipelineV2InspectOutcome,
  reason: Extract<PipelineV2InspectOutcome, { ok: false }>["reason"],
  options: { runRoot: string | null } = { runRoot: null },
): void {
  if (outcome.ok) {
    throw new Error(`expected an inspect failure, got ${JSON.stringify(outcome)}`);
  }
  expect(outcome.reason).toBe(reason);
  expect(outcome.exitCode).toBe(1);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "reason", "runId", "runRoot"]);
  expect(outcome.runRoot).toBe(options.runRoot);
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
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  } as never);
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
  options: {
    commitTransition?: boolean;
    outputs?: Record<string, string>;
    phases?: number;
    stopAfter?: "running" | "accepted";
    publishCapture?: WorkerLogCapture;
    bindingFromArtifact?: boolean;
  } = {},
): Promise<AcceptedStateOutput[]> {
  const activation = await prepareActivationData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  const state = harness.pipeline.states.find((entry) => entry.id === stateId);
  const profile = state !== undefined && state.type === "agent" ? state.profile : "";
  await harness.recording.dispatch({
    kind: "start_agent_execution",
    stateId,
    profile,
    ...startRoleArgs(harness.pipeline, stateId, harness.recording.snapshot),
  } as never);
  const all: CommandRecord[] = [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` },
    { kind: "agent_running" },
  ];
  for (const command of all.slice(0, options.phases ?? all.length)) {
    await harness.recording.dispatch(command as never);
  }
  if (options.phases !== undefined) {
    return accepted;
  }
  const worker = new PrefixWorker(activation, options.outputs);
  await worker.run();
  if (options.publishCapture !== undefined) {
    await publishPipelineV2WorkerLog(harness.dirs.runRoot, executionIndex, options.publishCapture);
  }
  if (options.stopAfter === "running") {
    return accepted;
  }
  const records = await acceptActivationOutputs(harness.pipeline, activation);
  let workerLog: PipelineV2WorkerLogState = { status: "unavailable" };
  if (options.bindingFromArtifact === true) {
    const artifact = parseWorkerLogArtifact(
      await readFile(join(harness.dirs.runRoot, "logs", `${executionIndex}.workerlog.json`), "utf8"),
      executionIndex,
    );
    workerLog = {
      status: artifact.status,
      sha256: artifact.sha256,
      byte_count: artifact.byte_count,
      truncated_daemon: artifact.truncated_daemon,
      truncated_local: artifact.truncated_local,
    };
  }
  await harness.recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
    workerLog,
  } as never);
  if (options.stopAfter === "accepted") {
    return [...accepted, ...records];
  }
  await harness.recording.dispatch({ kind: "agent_cleanup_completed" } as never);
  if (options.commitTransition !== false) {
    await harness.recording.dispatch({
      kind: "transition_committed",
      step: transitionStepFor(harness.pipeline, stateId, "completed"),
      executionIndex,
    } as never);
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
  options: { commitTransition?: boolean } = {},
): Promise<PipelineDecisionStateResult> {
  const prepared = await prepareDecisionStateData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  await harness.recording.dispatch({
    kind: "start_decision_execution",
    stateId,
    inputDigest: prepared.input_digest,
    ...startRoleArgs(harness.pipeline, stateId, harness.recording.snapshot),
  } as never);
  const evaluated = evaluatePreparedDecisionState(harness.pipeline, prepared);
  await harness.recording.dispatch({ kind: "decision_evaluated", result: toSelectedRecord(evaluated) } as never);
  if (options.commitTransition !== false) {
    await harness.recording.dispatch({
      kind: "transition_committed",
      step: transitionStepFor(harness.pipeline, stateId, evaluated.outcome),
      executionIndex,
    } as never);
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
  } as never);
  return accepted;
}

/** The waiting state of the stage→planning flow: the settled stage window, then the recovery resume. */
async function prefixWaiting(harness: Harness): Promise<RunInputsSnapshot> {
  const runInputs = await prefixCreateRun(harness);
  const accepted = await prefixStageEntry(harness, runInputs);
  await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
  const reopened = await reopenHarness(harness);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: RUN_ID,
    runRoot: harness.dirs.runRoot,
    sink: reopened as never,
    runtime: fakeRuntime().runtime,
  }, NEUTRAL_CONTROL);
  if (result.ok || !("waiting" in result) || result.waiting !== true) {
    throw new Error(`expected the waiting suspension, got ${JSON.stringify(result)}`);
  }
  return runInputs;
}

// --- the proof matrix -------------------------------------------------------

test("A: the ordinary resumable boundary inspects as resumable with the cursor binding", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-a");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1);
    const durable = await readDurableState(harness);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.schemaVersion).toBe(1);
    expect(report.exitCode).toBe(0);
    expect(report.status).toBe("running");
    expect(report.revision).toBe(durable.revision);
    expect(report.runId).toBe(RUN_ID);
    expect(report.runRoot).toBe(harness.dirs.runRoot);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "check", transitionCount: 1 });
    expect(report.outputs).toEqual([]);
    expect(Object.keys(report).sort()).toEqual([
      "boundary", "exitCode", "ok", "outputs", "revision", "runId", "runRoot", "schemaVersion", "status",
    ]);
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("B: the initial plan-ready boundary with the trusted auto policy reports the resume continuation", async () => {
  const harness = await setupHarness(stageToDoneYaml(true), "insp-b");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "architect", 1, {
      commitTransition: false,
      outputs: { plan: JSON.stringify(PROPOSAL_R1) },
    });
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.status).toBe("running");
    expect(report.boundary).toEqual({ kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume" });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("C: the policy-less plan-ready boundary keeps the manual resume_plan continuation", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-c");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1, { commitTransition: false });
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume_plan" });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("D: the replanned plan-ready boundary after a revise cycle reports plan_ready", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_PLANNING, "insp-d");
  try {
    const runInputs = await prefixWaiting(harness);
    const reopened = await reopenHarness(harness);
    await applyPipelineV2ReviseTaskIntervention({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      sink: reopened as never,
      runId: RUN_ID,
      waitIndex: 1,
      taskId: "task-a",
      taskBody: "Body A revised",
    });
    const state = reopened.snapshot as PipelineV2RunState;
    expect(state.status).toBe("active");
    expect(state.cursor.current_state).toBe("planner2");
    // the planner2 activation must dispatch through the reopened sink: the
    // original harness sink's in-memory snapshot is stale after the restart
    harness.recording = reopened;
    await runAgentActivation(harness, runInputs, [], "planner2", 3, {
      commitTransition: false,
      outputs: { plan2: JSON.stringify(PROPOSAL_R1) },
    });
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.status).toBe("running");
    expect(report.boundary).toEqual({ kind: "plan_ready", planningExecutionIndex: 3, continuation: "resume_plan" });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("E: the waiting run exposes the open wait record with verbatim action order", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_PLANNING, "insp-e");
  try {
    await prefixWaiting(harness);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.status).toBe("waiting");
    expect(report.boundary).toEqual({
      kind: "waiting",
      waitIndex: 1,
      reason: "stage_iteration_completed",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "planner2" },
      ],
    });
    expect(report.outputs).toEqual([]);
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("F: genuinely in-flight executions are the only in_flight boundary", async () => {
  const phases: Array<{ prefix: string; phases: number; phase: string; sessions: number }> = [
    { prefix: "insp-f1", phases: 1, phase: "data_prepared", sessions: 0 },
    { prefix: "insp-f2", phases: 2, phase: "execution_session_created", sessions: 0 },
    { prefix: "insp-f3", phases: 4, phase: "running", sessions: 0 },
  ];
  for (const spec of phases) {
    const harness = await setupHarness(controlPipelineYaml(false), spec.prefix);
    try {
      const runInputs = await prefixCreateRun(harness);
      await runAgentActivation(harness, runInputs, [], "coder", 1, { phases: spec.phases, commitTransition: false });
      const { before, after, outcome } = await inspectRun(harness);
      const report = expectReport(outcome);
      expect(report.status).toBe("running");
      expect(report.boundary).toEqual({
        kind: "in_flight",
        executionIndex: 1,
        stateId: "coder",
        executionRole: "planning",
        phase: spec.phase,
      });
      expect(before).toBe(after);
    } finally {
      await rm(harness.dirs.root, { recursive: true, force: true });
    }
  }
});

test("G: settled stage and control executions awaiting their transition are resumable, never in-flight", async () => {
  const stage = await setupHarness(stageToDoneYaml(false), "insp-g-stage");
  try {
    const runInputs = await prefixCreateRun(stage);
    const accepted = await prefixStageEntry(stage, runInputs);
    await runAgentActivation(stage, runInputs, accepted, "dev_entry", 2, { commitTransition: false });
    const { before, after, outcome } = await inspectRun(stage);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "dev_entry", transitionCount: 1 });
    expect(report.status).toBe("running");
    expect(before).toBe(after);
  } finally {
    await rm(stage.dirs.root, { recursive: true, force: true });
  }

  const control = await setupHarness(controlPipelineYaml(false), "insp-g-control", { facts: FACTS_ALPHA });
  try {
    const runInputs = await prefixCreateRun(control);
    const accepted = await runAgentActivation(control, runInputs, [], "coder", 1);
    await runDecisionActivation(control, runInputs, accepted, "check", 2, { commitTransition: false });
    const { before, after, outcome } = await inspectRun(control);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "check", transitionCount: 1 });
    expect(before).toBe(after);
  } finally {
    await rm(control.dirs.root, { recursive: true, force: true });
  }
});

test("H: a durably failed execution awaiting finalization is resumable", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-h");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    } as never);
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" }, workerLog: { status: "unavailable" }  },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "coder", transitionCount: 0 });
    expect(report.status).toBe("running");
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("I: every incomplete terminal lifecycle window is resumable", async () => {
  const buildT = async (prefix: string, stage: "t0" | "t1" | "t2" | "t3") => {
    const harness = await setupHarness(controlPipelineYaml(true), prefix, { facts: FACTS_ALPHA });
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    if (stage !== "t0") {
      await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    }
    if (stage === "t2" || stage === "t3") {
      const crashed = await collectRunOutputs(harness.pipeline, runInputs, accepted);
      if (stage === "t3") {
        await harness.recording.dispatch({
          kind: "run_outputs_published",
          outputs: crashed.outputs.map((entry) =>
            entry.present
              ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
              : { id: entry.id, type: entry.type, required: entry.required, present: false },
          ),
        } as never);
      }
    }
    return harness;
  };
  for (const stage of ["t0", "t1", "t2", "t3"] as const) {
    const harness = await buildT(`insp-i-${stage}`, stage);
    try {
      const { before, after, outcome } = await inspectRun(harness);
      const report = expectReport(outcome);
      expect(report.boundary).toEqual({ kind: "resumable", stateId: "done", transitionCount: 2 });
      expect(report.status).toBe("running");
      // only the durable record projects outputs: the crashed t2 publication
      // has no durable record yet, so its files are invisible to the projection
      if (stage === "t3") {
        expect(report.outputs).toHaveLength(1);
        expect(report.outputs[0]?.present).toBe(true);
      } else {
        expect(report.outputs).toEqual([]);
      }
      expect(before).toBe(after);
    } finally {
      await rm(harness.dirs.root, { recursive: true, force: true });
    }
  }
});

test("J: a succeeded run with a present required output carries the digest projection", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "insp-j", { facts: FACTS_ALPHA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    const published = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: published.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    } as never);
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.exitCode).toBe(0);
    expect(report.status).toBe("succeeded");
    expect(report.boundary).toEqual({ kind: "succeeded", terminalStateId: "done" });
    expect(report.outputs).toHaveLength(1);
    const output = report.outputs[0];
    expect(output?.present).toBe(true);
    if (output?.present === true) {
      expect(Object.keys(output).sort()).toEqual(["digest", "id", "present", "required", "type"]);
      expect(output.id).toBe("report");
      expect(output.type).toBe("json");
      expect(output.required).toBe(true);
      expect(output.digest).toMatch(/^[0-9a-f]{64}$/);
    }
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("K: a succeeded run with an absent optional output projects it without a digest", async () => {
  const harness = await setupHarness(optionalOutputPipelineYaml(), "insp-k", { facts: FACTS_ALPHA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    const published = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: published.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    } as never);
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    const { outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.status).toBe("succeeded");
    expect(report.outputs).toHaveLength(2);
    const [first, second] = report.outputs;
    expect(first?.present).toBe(true);
    if (first?.present === true) {
      expect(Object.keys(first).sort()).toEqual(["digest", "id", "present", "required", "type"]);
    }
    expect(second).toEqual({ id: "extra", type: "json", required: false, present: false });
    expect(Object.keys(second as Record<string, unknown>).sort()).toEqual(["id", "present", "required", "type"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("L: a finalized failed run inspects successfully with the failed boundary", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-l");
  try {
    await prefixCreateRun(harness);
    await harness.recording.dispatch({
      kind: "start_agent_execution",
      stateId: "coder",
      profile: "coder",
      ...startRoleArgs(harness.pipeline, "coder", harness.recording.snapshot),
    } as never);
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "exec-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" }, workerLog: { status: "unavailable" }  },
    ]) {
      await harness.recording.dispatch(command as never);
    }
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: RUN_ID,
      runRoot: harness.dirs.runRoot,
      sink: reopened as never,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expect(result.ok).toBe(false);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.exitCode).toBe(0);
    expect(report.status).toBe("failed");
    expect(report.boundary).toEqual({ kind: "failed", reason: "worker_failed" });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("M: the entry-terminal pipeline with zero executions is resumable at the entry terminal", async () => {
  const harness = await setupHarness(PIPELINE_ENTRY_TERMINAL, "insp-m");
  try {
    await prefixCreateRun(harness);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "done", transitionCount: 0 });
    expect(report.outputs).toEqual([]);
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("N: a missing run is run_not_found and creates nothing", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-n");
  try {
    const missingRoot = join(harness.dirs.stateRoot, "pipeline-runs", "no-such-run");
    const stateRootBefore = await fingerprint(harness.dirs.stateRoot);
    const outcome = await inspectPipelineV2Run({ runId: "no-such-run", stateRoot: harness.dirs.stateRoot });
    expectFailure(outcome, "run_not_found", { runRoot: null });
    expect((outcome as { runId: string }).runId).toBe("no-such-run");
    let exists = false;
    try {
      await lstat(missingRoot);
      exists = true;
    } catch {
      exists = false;
    }
    expect(exists).toBe(false);
    expect(await fingerprint(harness.dirs.stateRoot)).toBe(stateRootBefore);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("O: a damaged state document is run_state_invalid and never repaired", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-o");
  try {
    await prefixCreateRun(harness);
    const statePath = join(harness.dirs.runRoot, "state.json");
    const good = await readFile(statePath, "utf8");

    // garbage bytes
    await writeFile(statePath, "this is not json at all");
    const garbage = await inspectRun(harness);
    expectFailure(garbage.outcome, "run_state_invalid", { runRoot: harness.dirs.runRoot });
    expect(garbage.before).toBe(garbage.after);

    // valid JSON of the wrong shape
    await writeFile(statePath, JSON.stringify({ hello: "world" }));
    const wrongShape = await inspectRun(harness);
    expectFailure(wrongShape.outcome, "run_state_invalid", { runRoot: harness.dirs.runRoot });

    // the state document removed entirely
    await rm(statePath);
    const missing = await inspectRun(harness);
    expectFailure(missing.outcome, "run_state_invalid", { runRoot: harness.dirs.runRoot });

    // the honest document still inspects fine afterwards (nothing was repaired or rewritten)
    await writeFile(statePath, good);
    const restored = await inspectRun(harness);
    expectReport(restored.outcome);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("P: a bundle changed after the run is a pipeline_mismatch, never a mutation", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-p");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1);
    await writeFile(join(harness.dirs.bundle, "prompts", "coder.md"), "CHANGED-CONTENT\n");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "pipeline_mismatch", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("Q1: a modified accepted output at the ordinary boundary is artifact_modified", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-q1");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1);
    await writeFile(join(harness.dirs.runRoot, "activations", "1-coder", "data", "outputs", "plan"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("Q2: a modified accepted output at the plan-ready boundary is artifact_modified", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-q2");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1, { commitTransition: false });
    await writeFile(join(harness.dirs.runRoot, "activations", "1-coder", "data", "outputs", "plan"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("Q3: a modified published artifact at the terminal window is artifact_modified; the recovery detects it too", async () => {
  const harness = await setupHarness(controlPipelineYaml(true), "insp-q3", { facts: FACTS_ALPHA });
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await writeFile(join(harness.dirs.runRoot, "outputs", "report"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
    // the corresponding recovery verification detects the same damage at
    // the mutating step: the adoption refuses and the run finalizes failed
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: RUN_ID,
      runRoot: harness.dirs.runRoot,
      sink: reopened as never,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    expect(result.ok).toBe(false);
    if (result.ok || "waiting" in result || "planReady" in result || ("refused" in result && result.refused)) {
      throw new Error(`expected an ordinary failure, got ${JSON.stringify(result)}`);
    }
    expect(result.reason).toBe("run_output_invalid");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- defensive battery ------------------------------------------------------

test("the projection is deep-frozen and deterministic across repeated inspects", async () => {
  const harness = await setupHarness(PIPELINE_STAGE_TO_PLANNING, "insp-def-freeze");
  try {
    await prefixWaiting(harness);
    const first = await inspectPipelineV2Run({ runId: RUN_ID, stateRoot: harness.dirs.stateRoot });
    const second = await inspectPipelineV2Run({ runId: RUN_ID, stateRoot: harness.dirs.stateRoot });
    expectReport(first);
    expectReport(second);
    expect(JSON.stringify(second)).toBe(JSON.stringify(first));
    expect(Object.isFrozen(first)).toBe(true);
    const report = first as Extract<PipelineV2InspectOutcome, { ok: true }>;
    expect(Object.isFrozen(report.boundary)).toBe(true);
    expect(Object.isFrozen(report.outputs)).toBe(true);
    const boundary = report.boundary as Record<string, unknown>;
    expect(Object.isFrozen(boundary.actions)).toBe(true);
    for (const action of boundary.actions as unknown[]) {
      expect(Object.isFrozen(action)).toBe(true);
    }
    expect(Object.isFrozen(report)).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("the failure document is frozen and carries only its exact five keys", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-def-failure");
  try {
    // the harness created no state document for this run id: a missing run
    const outcome = await inspectPipelineV2Run({ runId: "absent-run", stateRoot: harness.dirs.stateRoot });
    expectFailure(outcome, "run_not_found", { runRoot: null });
    expect(Object.isFrozen(outcome)).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("the contract fields of the options object are read exactly once in order; extras never", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-def-proxy");
  try {
    await prefixCreateRun(harness);
    const reads: string[] = [];
    const options = new Proxy({} as Record<string, unknown>, {
      get(target, prop) {
        if (prop === "runId") {
          reads.push("runId");
          return RUN_ID;
        }
        if (prop === "stateRoot") {
          reads.push("stateRoot");
          return harness.dirs.stateRoot;
        }
        if (prop === "extra") {
          reads.push("extra");
          return "hostile";
        }
        return Reflect.get(target, prop, target);
      },
    });
    const outcome = await inspectPipelineV2Run(options as never);
    expectReport(outcome);
    expect(reads).toEqual(["runId", "stateRoot"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("an invalid run id is never echoed and a non-absolute state root is invalid_options", async () => {
  const bad = await inspectPipelineV2Run({ runId: "../escape", stateRoot: "/tmp" });
  expectFailure(bad, "invalid_options", { runRoot: null });
  expect((bad as { runId: string }).runId).toBe("");

  const relative = await inspectPipelineV2Run({ runId: "valid-id", stateRoot: "relative/path" });
  expectFailure(relative, "invalid_options", { runRoot: null });
  expect((relative as { runId: string }).runId).toBe("valid-id");
});

test("the restores inspect delegates to reject forged pipelines with zero state traps", async () => {
  const harness = await setupHarness(controlPipelineYaml(false), "insp-def-provenance");
  try {
    const runInputs = await prefixCreateRun(harness);
    await runAgentActivation(harness, runInputs, [], "coder", 1, { commitTransition: false });
    const durable = await readDurableState(harness);
    let stateTraps = 0;
    const stateProxy = new Proxy(durable as unknown as Record<string, unknown>, {
      get(target, prop) {
        stateTraps += 1;
        return Reflect.get(target, prop, target);
      },
    });
    const cloned = structuredClone(harness.pipeline) as unknown as ResolvedPipelineV2;
    let planningError: unknown = null;
    try {
      await restorePipelineV2PlanningAcceptanceContext(cloned, stateProxy as never, harness.dirs.runRoot);
    } catch (cause) {
      planningError = cause;
    }
    expect(planningError).toBeInstanceOf(Error);
    expect((planningError as Error).message).toContain("hand-built objects, casts, clones and Proxies are rejected");
    expect(stateTraps).toBe(0);

    let runtimeError: unknown = null;
    try {
      await restorePipelineV2RuntimeContext(cloned, durable, harness.dirs.runRoot);
    } catch (cause) {
      runtimeError = cause;
    }
    expect(runtimeError).toBeInstanceOf(Error);
    expect((runtimeError as Error).message).toContain("hand-built objects, casts, clones and Proxies are rejected");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("export surface: exactly the inspect module's public runtime names", async () => {
  const mod = await import("../src/pipeline_v2_inspect.ts");
  expect(Object.keys(mod).sort()).toEqual([
    "inspectPipelineV2Run",
    "loadDurablePipelineV2Run",
  ]);
});

test("source scan: one classifier call, read-only store load only, no sink/dispatch/reducer/publisher/runtime/launcher imports", async () => {
  const source = await readFile(new URL("../src/pipeline_v2_inspect.ts", import.meta.url), "utf8");
  // exactly one authoritative recovery classifier call
  const classifierUses = source.split("classifyPipelineV2ResumeRecovery(").length - 1;
  expect(classifierUses).toBe(1);
  // the single shared published-tree verifier is used by both inspect faces
  expect(source.includes("verifyPublishedRunOutputs(")).toBe(true);
  expect(source.includes("verifyPublishedRunOutputsAgainstRecord(")).toBe(true);
  // no mutable sink, no reducer dispatch, no publisher, no runtime adapter,
  // no Launcher/Docker Helper machinery (the verifier faces from the
  // runtime module are the one sanctioned import)
  for (const banned of [
    "pipeline_v2_state_sink",
    "reducePipelineV2RunCommand",
    "pipeline_v2_wait_store",
    "pipeline_v2_run_plan_candidate",
    "pipeline_v2_docker_runtime",
    "launcher.ts",
    "docker_helper",
    ".dispatch(",
    "createExecutionSession",
    "createToolSession",
    "openChildSession",
    "collectRunOutputs",
    "collectOrAdoptRunOutputs",
  ]) {
    expect(source.includes(banned)).toBe(false);
  }
  // the only store usage is the read-only load
  expect(source.includes("new PipelineV2RunStateStore(")).toBe(true);
  expect(source.includes(".load()")).toBe(true);
  expect(source.includes(".create(")).toBe(false);
  expect(source.includes(".commit(")).toBe(false);
  // no mutation primitives anywhere in the module
  for (const mutation of ["rename", "unlink", "chmod", "mkdir", "writeFile", "rmdir"]) {
    expect(source.includes(mutation)).toBe(false);
  }
});

test("the runtime module keeps one published-tree verifier shared by the adoption and the inspect", async () => {
  const source = await readFile(new URL("../src/pipeline_v2_runtime.ts", import.meta.url), "utf8");
  // exactly one defect classification and one tree verification body
  expect(source.split("new PipelineV2RunOutputTreeDefectError(").length - 1).toBeGreaterThan(0);
  expect(source.split("async function verifyPublishedRunOutputsTree(").length - 1).toBe(1);
  // the adoption consumes the verifier: no second inline per-output check loop
  const adoptStart = source.indexOf("async function adoptPublishedRunOutputs(");
  const adoptEnd = source.indexOf("/**\n * Compatibility wrapper over the two-phase decision-data adapter:");
  expect(adoptStart).toBeGreaterThan(0);
  expect(adoptEnd).toBeGreaterThan(adoptStart);
  const adoptBody = source.slice(adoptStart, adoptEnd);
  expect(adoptBody).toContain("verifyPublishedRunOutputsInternal");
  expect(adoptBody).not.toContain("scanDirectoryTree");
  expect(adoptBody).not.toContain("runOutputDigestHasher");
  // one digest domain owner: the published hashing helpers stay singular
  expect(source.split("function runOutputDigestHasher(").length - 1).toBe(1);
  expect(source.split("function hashDirectoryEntry(").length - 1).toBe(1);
  // the verifier never writes: no creation, rename or removal inside it
  const verifyStart = source.indexOf("async function verifyPublishedRunOutputsTree(");
  const verifyEnd = source.indexOf("async function adoptPublishedRunOutputs(");
  const verifyBody = source.slice(verifyStart, verifyEnd);
  for (const banned of ["rename(", "mkdir(", "unlink(", "rmdir(", "writeFile", "link("]) {
    expect(verifyBody.includes(banned)).toBe(false);
  }
});

// --- published-output integrity battery -------------------------------------

function expectResumeSuccess(result: unknown): PipelineV2RunState {
  if (
    typeof result !== "object" || result === null ||
    (result as { ok?: unknown }).ok !== true
  ) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(result)}`);
  }
  return (result as { state: PipelineV2RunState }).state;
}

function commandKinds(recording: RecordingSink): string[] {
  return recording.commands.map((command) => command.kind as string);
}

/** The control pipeline whose report output carries a strict JSON schema. */
function strictReportPipelineYaml(): string {
  return controlPipelineYaml(true).replace(
    `      - id: report
        type: json
        schema: schemas/loose.schema.json`,
    `      - id: report
        type: json
        schema: schemas/report.schema.json`,
  );
}

/**
 * Builds a terminal-window run with an honest publication (the T2 orphan
 * shape when `commit` is false, the T3 shape when the record is durably
 * committed) and returns the harness plus the accepted records.
 */
async function buildPublishedWindow(
  prefix: string,
  options: { commitRecord?: boolean; yaml?: string } = {},
): Promise<{ harness: Harness; outputsRoot: string }> {
  const harness = await setupHarness(options.yaml ?? controlPipelineYaml(true), prefix, { facts: FACTS_ALPHA });
  const runInputs = await prefixCreateRun(harness);
  const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
    outputs: { report: JSON.stringify({ ok: true }) },
  });
  await runDecisionActivation(harness, runInputs, accepted, "check", 2);
  await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
  const published = await collectRunOutputs(harness.pipeline, runInputs, accepted);
  if (options.commitRecord !== false) {
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: published.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    } as never);
  }
  return { harness, outputsRoot: join(harness.dirs.runRoot, "outputs") };
}

test("the correct T2 orphan is resumable and the resume adoption keeps the artifact identity", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t2-ok", { commitRecord: false });
  try {
    const publishedPath = join(outputsRoot, "report");
    const beforeStat = await stat(publishedPath);
    const beforeBytes = await readFile(publishedPath);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({ kind: "resumable", stateId: "done", transitionCount: 2 });
    expect(report.outputs).toEqual([]);
    expect(before).toBe(after);
    // the promised resume adoption keeps inode, mode, mtime and bytes
    const reopened = await reopenHarness(harness);
    const result = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: RUN_ID,
      runRoot: harness.dirs.runRoot,
      sink: reopened as never,
      runtime: fakeRuntime().runtime,
    }, NEUTRAL_CONTROL);
    const state = expectResumeSuccess(result);
    expect(commandKinds(reopened)).toEqual(["run_outputs_published", "run_succeeded"]);
    const afterStat = await stat(publishedPath);
    expect(afterStat.ino).toBe(beforeStat.ino);
    expect(afterStat.mode).toBe(beforeStat.mode);
    expect(afterStat.mtimeMs).toBe(beforeStat.mtimeMs);
    expect(await readFile(publishedPath)).toEqual(beforeBytes);
    expect(state.run_outputs?.[0]?.present).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a T2 orphan with modified bytes is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t2-mod", { commitRecord: false });
  try {
    await writeFile(join(outputsRoot, "report"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a T2 orphan with a missing required artifact is artifact_missing", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t2-miss", { commitRecord: false });
  try {
    await rm(join(outputsRoot, "report"));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_missing", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a T2 orphan with an extra undeclared entry is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t2-extra", { commitRecord: false });
  try {
    await writeFile(join(outputsRoot, "smuggled"), "extra");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a T3 record with modified bytes is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t3-mod");
  try {
    await writeFile(join(outputsRoot, "report"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a T3 record with a missing durably-present artifact is artifact_missing", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-t3-miss");
  try {
    await rm(join(outputsRoot, "report"));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_missing", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a finalized succeeded run with modified bytes is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-fin-mod");
  try {
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    await writeFile(join(outputsRoot, "report"), "TAMPERED");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a finalized succeeded run with a missing durably-present artifact is artifact_missing", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-fin-miss");
  try {
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    await rm(join(outputsRoot, "report"));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_missing", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a finalized succeeded run with an intact publication keeps the succeeded projection", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-fin-ok");
  try {
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.status).toBe("succeeded");
    expect(report.boundary).toEqual({ kind: "succeeded", terminalStateId: "done" });
    expect(report.outputs).toHaveLength(1);
    expect(report.outputs[0]?.present).toBe(true);
    expect(before).toBe(after);
    void outputsRoot;
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a symlinked published artifact is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-symlink");
  try {
    await rm(join(outputsRoot, "report"));
    await writeFile(join(harness.dirs.root, "outside-target"), "elsewhere");
    await symlink(join(harness.dirs.root, "outside-target"), join(outputsRoot, "report"));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a malformed published JSON artifact is artifact_modified", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-badjson");
  try {
    await writeFile(join(outputsRoot, "report"), "{not json");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("a schema-violating published JSON artifact is artifact_modified", async () => {
  const dirs = await makeDirs("insp-pub-schema");
  try {
    const strictYaml = strictReportPipelineYaml();
    await writeBundle(dirs, strictYaml, { facts: FACTS_ALPHA });
    await writeFile(
      join(dirs.bundle, "schemas", "report.schema.json"),
      JSON.stringify({ type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"], additionalProperties: false }),
    );
    const pipeline = await loadPipelineV2(dirs.bundle);
    resetClock();
    const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId: RUN_ID, now: nextTick });
    const harness: Harness = { dirs, pipeline, recording: new RecordingSink(sink) };
    const runInputs = await prefixCreateRun(harness);
    const accepted = await runAgentActivation(harness, runInputs, [], "coder", 1, {
      outputs: { report: JSON.stringify({ ok: true }) },
    });
    await runDecisionActivation(harness, runInputs, accepted, "check", 2);
    await harness.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    const published = await collectRunOutputs(harness.pipeline, runInputs, accepted);
    await harness.recording.dispatch({
      kind: "run_outputs_published",
      outputs: published.outputs.map((entry) =>
        entry.present
          ? { id: entry.id, type: entry.type, required: entry.required, present: true, digest: entry.digest }
          : { id: entry.id, type: entry.type, required: entry.required, present: false },
      ),
    } as never);
    await harness.recording.dispatch({ kind: "run_succeeded" } as never);
    // schema-valid at publication, violating after a post-publication change
    await writeFile(join(harness.dirs.runRoot, "outputs", "report"), JSON.stringify({ ok: "nope" }));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

test("the repeated inspect over a damaged publication returns byte-identical errors", async () => {
  const { harness, outputsRoot } = await buildPublishedWindow("insp-pub-repeat");
  try {
    await writeFile(join(outputsRoot, "report"), "TAMPERED");
    const first = await inspectPipelineV2Run({ runId: RUN_ID, stateRoot: harness.dirs.stateRoot });
    const second = await inspectPipelineV2Run({ runId: RUN_ID, stateRoot: harness.dirs.stateRoot });
    expectFailure(first, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(JSON.stringify(second)).toBe(JSON.stringify(first));
    expect(Object.isFrozen(first)).toBe(true);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- schema v9 worker-log integrity verification (read-only) ----------------

const INSP_LOG_CAPTURE: WorkerLogCapture = {
  status: "captured",
  byteCount: 10,
  tailBytes: new TextEncoder().encode("worker-log"),
  truncatedDaemon: false,
  truncatedLocal: false,
};

async function prefixSettledStage(
  harness: Harness,
  options: { workerLog?: "artifact" } = {},
): Promise<void> {
  const runInputs = await prefixCreateRun(harness);
  const accepted = await prefixStageEntry(harness, runInputs);
  await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, {
    stopAfter: "accepted",
    publishCapture: options.workerLog === "artifact" ? INSP_LOG_CAPTURE : undefined,
    bindingFromArtifact: options.workerLog === "artifact",
  });
}

test("W1: a captured stage binding inspects as resumable with the artifact verified", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w1");
  try {
    await prefixSettledStage(harness, { workerLog: "artifact" });
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({
      kind: "in_flight",
      executionIndex: 2,
      stateId: "dev_entry",
      executionRole: "stage",
      phase: "outputs_accepted",
    });
    expect(before).toBe(after);
    // the public projection carries no worker-log fields
    expect(JSON.stringify(report)).not.toContain("worker_log");
    expect(JSON.stringify(report)).not.toContain("workerlog");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W2: a missing recorded artifact fails inspect as artifact_missing with zero writes", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w2");
  try {
    await prefixSettledStage(harness, { workerLog: "artifact" });
    await rm(join(harness.dirs.runRoot, "logs", "2.workerlog.json"));
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_missing", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W3: a modified artifact fails inspect as artifact_modified with zero writes", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w3");
  try {
    await prefixSettledStage(harness, { workerLog: "artifact" });
    await writeFile(join(harness.dirs.runRoot, "logs", "2.workerlog.json"), '{"damaged":true}');
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W4: an extra logs entry fails inspect as artifact_modified with zero writes", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w4");
  try {
    await prefixSettledStage(harness, { workerLog: "artifact" });
    await writeFile(join(harness.dirs.runRoot, "logs", "99.workerlog.json"), "{}");
    const { before, after, outcome } = await inspectRun(harness);
    expectFailure(outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W5: an unavailable record without an artifact inspects normally", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w5");
  try {
    await prefixSettledStage(harness);
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({
      kind: "in_flight",
      executionIndex: 2,
      stateId: "dev_entry",
      executionRole: "stage",
      phase: "outputs_accepted",
    });
    expect(before).toBe(after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W6: an in-flight stage execution with a published orphan stays in_flight with the artifact verified", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w6");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, {
      stopAfter: "running",
      publishCapture: INSP_LOG_CAPTURE,
    });
    const { before, after, outcome } = await inspectRun(harness);
    const report = expectReport(outcome);
    expect(report.boundary).toEqual({
      kind: "in_flight",
      executionIndex: 2,
      stateId: "dev_entry",
      executionRole: "stage",
      phase: "running",
    });
    expect(before).toBe(after);
    // a damaged tolerated orphan still refuses typed and zero-write
    await writeFile(join(harness.dirs.runRoot, "logs", "2.workerlog.json"), '{"x":1}');
    const damaged = await inspectRun(harness);
    expectFailure(damaged.outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(damaged.before).toBe(damaged.after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("W7: a finalized succeeded run verifies its log artifacts read-only before the succeeded boundary", async () => {
  const harness = await setupHarness(stageToDoneYaml(false), "insp-w7");
  try {
    const runInputs = await prefixCreateRun(harness);
    const accepted = await prefixStageEntry(harness, runInputs);
    await runAgentActivation(harness, runInputs, accepted, "dev_entry", 2, {
      stopAfter: "accepted",
      publishCapture: INSP_LOG_CAPTURE,
      bindingFromArtifact: true,
    });
    // finish the run through the ordinary resume (cleanup + transition + terminal)
    const reopened = await reopenHarness(harness);
    const fake = fakeRuntime();
    const resumed = await resumePipelineV2Run({
      pipeline: harness.pipeline,
      runId: RUN_ID,
      runRoot: harness.dirs.runRoot,
      sink: reopened as never,
      runtime: fake.runtime,
    }, NEUTRAL_CONTROL);
    if (!resumed.ok) {
      throw new Error(`expected a successful resume, got ${JSON.stringify(resumed)}`);
    }
    // the intact artifact inspects as the succeeded boundary
    const intact = await inspectRun(harness);
    expect(expectReport(intact.outcome).boundary).toEqual({ kind: "succeeded", terminalStateId: "done" });
    expect(intact.before).toBe(intact.after);
    // a tampered artifact is the typed zero-write failure, never a false success
    await writeFile(join(harness.dirs.runRoot, "logs", "2.workerlog.json"), '{"tampered":true}');
    const damaged = await inspectRun(harness);
    expectFailure(damaged.outcome, "artifact_modified", { runRoot: harness.dirs.runRoot });
    expect(damaged.before).toBe(damaged.after);
    // the same for a missing artifact
    await rm(join(harness.dirs.runRoot, "logs", "2.workerlog.json"));
    const missing = await inspectRun(harness);
    expectFailure(missing.outcome, "artifact_missing", { runRoot: harness.dirs.runRoot });
    expect(missing.before).toBe(missing.after);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});
