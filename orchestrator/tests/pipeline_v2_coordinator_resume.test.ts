import { createHash } from "node:crypto";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { expect, test } from "bun:test";
import type { Stats } from "node:fs";
import {
  coordinatePipelineV2Run,
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ExecutionSession,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
  type PipelineV2ToolSession,
  type PipelineV2WorkerRunResult,
} from "../src/pipeline_v2_coordinator.ts";
import {
  acceptActivationOutputs,
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
import { parsePipelineV2RunState, PipelineV2StateError, type PipelineV2AgentExecutionState, type PipelineV2FailureReason, type PipelineV2RunCommand, type PipelineV2RunState, type PipelineDecisionStateRecord } from "../src/pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
  pipelineV2RunStatePath,
} from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { countingIo, faultIo, type IoCounts } from "./state_io_test_helpers.ts";
import type { PipelineStateIo } from "../src/pipeline_state_store.ts";

/**
 * Tests for the production-neutral resume entrypoint
 * `resumePipelineV2Run`: continuation of an already durable pipeline v2
 * run from its clean active boundary through the same internal
 * continuation chain the fresh coordinator uses. The prefix of every
 * resumed run is built with the real substrate itself (the run-owned
 * project copy, the run-input snapshot, the data-plane activation
 * preparation and acceptance, and the real durable reducer through the
 * real sink), so the resume restores real state — never hand-built
 * snapshots. Everything is deterministic: no sleeps, no LLM, no Docker
 * Helper, no launcher credential. Production resume stays unwired: the
 * runner, the CLI and the default pipeline are untouched.
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

/** agent coder -> decision check -> done / failed_end; decision from a json run input. */
const PIPELINE_DECISION_INPUT = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
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

/** coder (json out facts) -> check(beta -> coder revisit, alpha -> probe) -> probe (reads coder.facts) -> done. */
const PIPELINE_DECISION_FROM_OUTPUT = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
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
          state_output:
            state: coder
            output: facts
    transitions:
      - outcome: alpha
        to: probe
      - outcome: beta
        to: coder
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: probe
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    outputs:
      - id: report
        type: file
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

/** Bounded cycle: coder -> coder2 -> coder with a transition budget of 2. */
const PIPELINE_CYCLE_BUDGET = `
schema_version: 2
entry_state: coder
max_transitions: 2

inputs: []

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder2
  - id: coder2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder
  - id: done
    type: terminal
    result: success
`;

const FACTS_ALPHA = JSON.stringify({ f1: true, f2: false });
const FACTS_BETA = JSON.stringify({ f1: false, f2: true });

// --- harness ---------------------------------------------------------------

interface BundleDirs {
  root: string;
  bundle: string;
  sources: string;
  projectSource: string;
  runRoot: string;
  stateRoot: string;
}

async function makeDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-resume-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const runRoot = join(root, "runs", "resume-run");
  await mkdir(runRoot, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, sources, projectSource, runRoot, stateRoot };
}

async function writeBundle(
  dirs: BundleDirs,
  yaml: string,
  options: { facts?: string } = {},
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  await writeFile(join(dirs.bundle, "schemas", "loose.schema.json"), JSON.stringify(LOOSE_SCHEMA));
  await writeFile(join(dirs.bundle, "decisions", "model.yaml"), MODEL_YAML);
  if (yaml.includes("facts_seed")) {
    await writeFile(join(dirs.sources, "facts.json"), options.facts ?? FACTS_ALPHA);
  }
}

const SOURCE_FILE_NAMES: Record<string, string> = {
  facts_seed: "facts.json",
};

function bindingsFor(dirs: BundleDirs, pipeline: ResolvedPipelineV2): Array<{ id: string; path: string }> {
  return pipeline.inputs.map((input) => {
    const file = SOURCE_FILE_NAMES[input.id];
    if (file === undefined) {
      throw new Error(`test harness has no source file for run input ${input.id}`);
    }
    return { id: input.id, path: join(dirs.sources, file) };
  });
}

// --- clock -----------------------------------------------------------------

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock));
}

function resetClock(): void {
  clock = 0;
}

// --- sink ------------------------------------------------------------------

type CommandRecord = Record<string, unknown>;

class RecordingSink implements PipelineV2CoordinatorStateSink {
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

  async dispatch(command: Parameters<PipelineV2CoordinatorStateSink["dispatch"]>[0]): Promise<void> {
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

interface ResumeHarness {
  dirs: BundleDirs;
  pipeline: ResolvedPipelineV2;
  runId: string;
  /** The fresh prefix sink (with its recording). */
  sink: PipelineV2RunStateSink;
  recording: RecordingSink;
  ioCounts: IoCounts;
}

async function setupHarness(
  yaml: string,
  options: { facts?: string } = {},
): Promise<ResumeHarness> {
  const dirs = await makeDirs();
  await writeBundle(dirs, yaml, options);
  const pipeline = await loadPipelineV2(dirs.bundle);
  resetClock();
  const counted = countingIo();
  const sink = new PipelineV2RunStateSink({
    stateRoot: dirs.stateRoot,
    runId: "resume-run",
    io: counted.io,
    now: nextTick,
  });
  return {
    dirs,
    pipeline,
    runId: "resume-run",
    sink,
    recording: new RecordingSink(sink),
    ioCounts: counted.counts,
  };
}

/** Reopens the durable run after the simulated process restart. */
async function reopenHarness(
  harness: ResumeHarness,
  options: { io?: PipelineStateIo; faults?: ReadonlyMap<string, () => Error | undefined> } = {},
): Promise<RecordingSink> {
  const counted = countingIo(options.io);
  const real = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: harness.runId,
    io: counted.io,
    now: nextTick,
  });
  return new RecordingSink(real, options.faults);
}

// --- signal controls -------------------------------------------------------

const NEUTRAL_CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
  freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
};

function alwaysSignalControl(
  signal: "SIGINT" | "SIGTERM",
): { control: PipelineV2CoordinatorControl; freezes: () => number } {
  let freezeCalls = 0;
  return {
    control: {
      currentSignal: () => signal,
      freezeSignal: () => {
        freezeCalls += 1;
        return signal;
      },
    },
    freezes: () => freezeCalls,
  };
}

function armableSignalControl(): {
  control: PipelineV2CoordinatorControl;
  arm: () => void;
  freezes: () => number;
} {
  let armed = false;
  let freezeCalls = 0;
  return {
    control: {
      currentSignal: () => (armed ? "SIGINT" : null),
      freezeSignal: () => {
        freezeCalls += 1;
        return armed ? "SIGINT" : null;
      },
    },
    arm: () => {
      armed = true;
    },
    freezes: () => freezeCalls,
  };
}

// --- the prefix runner (real substrate, real reducer, real data plane) ------

function stateOf(pipeline: ResolvedPipelineV2, stateId: string): { profile: string; transitions: ReadonlyArray<{ outcome: string; to: string }> } {
  const state = pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type === "terminal") {
    throw new Error(`the prefix fixture has no executable state ${JSON.stringify(stateId)}`);
  }
  const profile: string = state.type === "agent" ? state.profile : "";
  return { profile, transitions: state.transitions };
}

function transitionTarget(pipeline: ResolvedPipelineV2, stateId: string, outcome: string): { to: string; index: number } {
  const transitions = stateOf(pipeline, stateId).transitions;
  const index = transitions.findIndex((transition) => transition.outcome === outcome);
  const match = transitions[index];
  if (match === undefined) {
    throw new Error(`state ${JSON.stringify(stateId)} declares no transition for ${JSON.stringify(outcome)}`);
  }
  return { to: match.to, index };
}

interface PrefixWorkerSpec {
  /** Exact json bytes per declared json output port. */
  jsonContent?: Record<string, string>;
}

/** The deterministic prefix worker: writes exactly its declared outputs. */
class PrefixWorker {
  runCount = 0;
  cleanupCount = 0;

  constructor(
    private readonly spec: PrefixWorkerSpec,
    private readonly activation: PreparedActivationData,
  ) {}

  async run(): Promise<PipelineV2WorkerRunResult> {
    this.runCount += 1;
    for (const port of this.activation.output_ports) {
      if (port.type === "directory") {
        await mkdir(port.path, { recursive: true });
      } else if (port.type === "json") {
        const body = this.spec.jsonContent?.[port.id];
        await writeFile(port.path, body ?? JSON.stringify({ ok: true, port: port.id }));
      } else {
        await writeFile(port.path, `${port.id} body`);
      }
    }
    return { status: "completed" };
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
  }
}

/** Dispatches `create_run` after preparing the run-owned project copy and the input snapshot. */
async function prefixCreateRun(harness: ResumeHarness): Promise<RunInputsSnapshot> {
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

async function prefixAgentStep(
  harness: ResumeHarness,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
  spec: PrefixWorkerSpec = {},
  sessionIds: { execution: string; tool: string } = { execution: `exec-${executionIndex}`, tool: `tool-${executionIndex}` },
): Promise<PreparedActivationData> {
  const { profile } = stateOf(harness.pipeline, stateId);
  const activation = await prepareActivationData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  await harness.recording.dispatch({ kind: "start_agent_execution", stateId, profile });
  await harness.recording.dispatch({ kind: "agent_data_prepared" });
  await harness.recording.dispatch({ kind: "agent_execution_session_created", sessionId: sessionIds.execution });
  await harness.recording.dispatch({ kind: "agent_tool_session_created", sessionId: sessionIds.tool });
  await harness.recording.dispatch({ kind: "agent_running" });
  const worker = new PrefixWorker(spec, activation);
  await worker.run();
  const records = await acceptActivationOutputs(harness.pipeline, activation);
  await harness.recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await worker.cleanup();
  await worker.cleanup();
  await harness.recording.dispatch({ kind: "agent_cleanup_completed" });
  const target = transitionTarget(harness.pipeline, stateId, "completed");
  await harness.recording.dispatch({
    kind: "transition_committed",
    step: { from: stateId, outcome: "completed", to: target.to, transition_index: target.index },
    executionIndex,
  });
  accepted.push(...records);
  return activation;
}

function toSelectedRecord(result: PipelineDecisionStateResult): PipelineDecisionStateRecord {
  if (result.status !== "selected") {
    throw new Error(`the prefix fixture only routes selected decisions, got ${result.status}`);
  }
  return {
    status: "selected",
    outcome: result.outcome,
    decision: result.decision,
    rule_id: result.rule_id,
    active_constraint_ids: [...result.active_constraint_ids],
  };
}

async function prefixDecisionStep(
  harness: ResumeHarness,
  runInputs: RunInputsSnapshot,
  accepted: AcceptedStateOutput[],
  stateId: string,
  executionIndex: number,
): Promise<PipelineDecisionStateResult> {
  const prepared = await prepareDecisionStateData(harness.pipeline, runInputs, accepted, stateId, executionIndex);
  await harness.recording.dispatch({
    kind: "start_decision_execution",
    stateId,
    inputDigest: prepared.input_digest,
  });
  const result = evaluatePreparedDecisionState(harness.pipeline, prepared);
  await harness.recording.dispatch({ kind: "decision_evaluated", result: toSelectedRecord(result) });
  const target = transitionTarget(harness.pipeline, stateId, result.outcome);
  await harness.recording.dispatch({
    kind: "transition_committed",
    step: { from: stateId, outcome: result.outcome, to: target.to, transition_index: target.index },
    executionIndex,
  });
  return result;
}

// --- the fake agent runtime (copied contract from the fresh suite) ---------

interface FakeSessionSpec {
  executionId?: string;
  toolId?: string;
  run?: "completed" | "worker_failed" | "worker_timeout" | "throw";
  executionCreate?: "throw";
  onRun?: (session: FakeAgentSession) => void | Promise<void>;
  onExecutionCleanup?: (session: FakeAgentSession) => void | Promise<void>;
}

class FakeAgentSession {
  runCount = 0;
  cleanupCount = 0;
  readonly runToolIds: unknown[] = [];
  readonly sessionId: string;

  constructor(
    readonly spec: FakeSessionSpec,
    readonly stateId: string,
    readonly activation: PreparedActivationData,
    readonly kind: "execution" | "tool",
    fallbackSessionId: string,
    private readonly log: (message: string) => void,
  ) {
    this.sessionId = (kind === "execution" ? spec.executionId : spec.toolId) ?? fallbackSessionId;
  }

  async runAgent(toolSession: PipelineV2ToolSession): Promise<PipelineV2WorkerRunResult> {
    this.runCount += 1;
    this.runToolIds.push(toolSession.sessionId);
    this.log(`run:${this.stateId}`);
    await this.spec.onRun?.(this);
    for (const port of this.activation.output_ports) {
      if (port.type === "directory") {
        await mkdir(port.path, { recursive: true });
      } else if (port.type === "json") {
        await writeFile(port.path, JSON.stringify({ ok: true, port: port.id }));
      } else {
        await writeFile(port.path, `${port.id} body`);
      }
    }
    switch (this.spec.run) {
      case "worker_failed":
        return { status: "failed", reason: "worker_failed" };
      case "worker_timeout":
        return { status: "failed", reason: "worker_timeout" };
      case "throw":
        throw new Error("WORKER-EXPLODED");
      default:
        return { status: "completed" };
    }
  }

  async cleanup(): Promise<void> {
    this.cleanupCount += 1;
    this.log(`cleanup-${this.kind === "execution" ? "exec" : "tool"}:${this.stateId}`);
    if (this.kind === "tool") {
      return;
    }
    await this.spec.onExecutionCleanup?.(this);
  }
}

interface FakePair {
  stateId: string;
  activationIndex: number;
  execution: FakeAgentSession;
  tool: FakeAgentSession;
}

interface FakeRuntimeHandle {
  runtime: PipelineV2AgentRuntime;
  pairs: FakePair[];
  createCalls: Array<{ stateId: string; activationIndex: number; session: "execution" | "tool" }>;
  events: string[];
}

function fakeRuntime(specs: readonly FakeSessionSpec[]): FakeRuntimeHandle {
  const pairs: FakePair[] = [];
  const createCalls: FakeRuntimeHandle["createCalls"] = [];
  const events: string[] = [];
  const logEvent = (message: string): void => {
    events.push(message);
  };
  const runtime = {
    createExecutionSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const index = pairs.length;
      const spec = specs[index] ?? {};
      if (spec.executionCreate === "throw") {
        throw new Error("EXEC-CREATE-EXPLODED");
      }
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "execution" });
      const execution = new FakeAgentSession(spec, state.id, activation, "execution", `exec-${index + 1}`, logEvent);
      events.push(`create-exec:${state.id}:${activation.activation_index}`);
      const tool = new FakeAgentSession(spec, state.id, activation, "tool", `tool-${index + 1}`, logEvent);
      const pair: FakePair = { stateId: state.id, activationIndex: activation.activation_index, execution, tool };
      pairs.push(pair);
      return execution as unknown as PipelineV2ExecutionSession;
    },
    createToolSession: async (state: { id: string }, activation: PreparedActivationData) => {
      const index = pairs.length - 1;
      const pair = pairs[index];
      if (pair === undefined) {
        throw new Error("no execution session was created for this activation");
      }
      createCalls.push({ stateId: state.id, activationIndex: activation.activation_index, session: "tool" });
      events.push(`create-tool:${state.id}:${activation.activation_index}`);
      return pair.tool as unknown as PipelineV2ToolSession;
    },
  };
  return { runtime: runtime as unknown as PipelineV2AgentRuntime, pairs, createCalls, events };
}

// --- helpers ---------------------------------------------------------------

function kinds(recording: RecordingSink): string[] {
  return recording.commands.map((command) => command.kind as string);
}

function agentAt(state: PipelineV2RunState, index: number): PipelineV2AgentExecutionState {
  const execution = state.executions[index];
  if (execution === undefined || execution.type !== "agent") {
    throw new Error(`expected an agent execution at ${index}`);
  }
  return execution;
}

function isHexDigest(value: unknown): boolean {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function expectResumeOk(result: PipelineV2ResumeCoordinationResult): PipelineV2RunState {
  if (!result.ok) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(result)}`);
  }
  return result.state;
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

function expectResumeFailed(
  result: PipelineV2ResumeCoordinationResult,
  reason: PipelineV2FailureReason,
): PipelineV2RunState {
  expect(result.ok).toBe(false);
  if (result.ok || ("refused" in result && result.refused)) {
    throw new Error(`expected an ordinary execution failure, got ${JSON.stringify(result)}`);
  }
  expect(result.reason).toBe(reason);
  if (reason !== "state_persist_failed" && result.state !== null) {
    expect(result.state.failure).toEqual({ reason });
  }
  return result.state as PipelineV2RunState;
}

/** Full deterministic filesystem fingerprint: relative path, kind, mode, content digest. */
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
    let extra = "";
    if (info.isFile()) {
      extra = createHash("sha256").update(await readFile(abs)).digest("hex");
    } else if (info.isSymbolicLink()) {
      extra = await (await import("node:fs/promises")).readlink(abs);
    }
    lines.push(`${rel}\t${describe(info)}\t${info.mode & 0o7777}\t${extra}`);
    if (info.isDirectory() && !info.isSymbolicLink()) {
      for (const entry of (await readdir(abs)).sort()) {
        await walk(rel === "" ? entry : `${rel}/${entry}`);
      }
    }
  };
  await walk("");
  return lines.join("\n");
}

async function readDurableState(harness: ResumeHarness): Promise<PipelineV2RunState> {
  const raw = await readFile(pipelineV2RunStatePath(harness.dirs.stateRoot, harness.runId), "utf8");
  return parsePipelineV2RunState(raw);
}

// --- tests -----------------------------------------------------------------

test("1. create_run boundary: reopen the sink and resume to success", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  expect((harness.recording.snapshot as PipelineV2RunState).revision).toBe(1);

  const reopened = await reopenHarness(harness);
  expect((reopened.snapshot as PipelineV2RunState).revision).toBe(1);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.status).toBe("success");
  expect(state.revision).toBe(15);
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  expect(state.executions.map((execution) => [execution.index, execution.state_id])).toEqual([
    [1, "coder"],
    [2, "check"],
  ]);
  // the reopened recording carries exactly the resumed suffix
  expect(kinds(reopened)).toEqual([
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
  const durable = await readDurableState(harness);
  expect(durable).toEqual(state);
  // one execution session pair, cleaned exactly once each
  expect(fake.pairs).toHaveLength(1);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
  expect(fake.events).toEqual([
    "create-exec:coder:1",
    "create-tool:coder:1",
    "run:coder",
    "cleanup-tool:coder",
    "cleanup-exec:coder",
  ]);
});

test("2. committed agent transition: the resumed decision takes the next global execution index", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs = await prefixCreateRun(harness);
  const accepted: AcceptedStateOutput[] = [];
  await prefixAgentStep(harness, runInputs, accepted, "coder", 1);
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.cursor).toEqual({ current_state: "check", transition_count: 1 });

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  // the decision is the second global execution; no activation exists for it
  const decision = state.executions[1];
  expect(decision?.type).toBe("decision");
  expect(decision?.index).toBe(2);
  expect(kinds(reopened)).toEqual([
    "start_decision_execution",
    "decision_evaluated",
    "transition_committed",
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
  const decisionStarted = reopened.commands[0];
  expect(decisionStarted?.stateId).toBe("check");
  expect(isHexDigest(decisionStarted?.inputDigest)).toBe(true);
  expect(reopened.commands[2]).toEqual({
    kind: "transition_committed",
    step: { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
    executionIndex: 2,
  });
  expect(fake.createCalls).toHaveLength(0);
});

test("3. agent -> decision -> agent: the resumed activation index skips the decision gap", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_FROM_OUTPUT);
  const runInputs = await prefixCreateRun(harness);
  const accepted: AcceptedStateOutput[] = [];
  await prefixAgentStep(harness, runInputs, accepted, "coder", 1, {
    jsonContent: { facts: FACTS_ALPHA },
  });
  await prefixDecisionStep(harness, runInputs, accepted, "check", 2); // alpha -> probe
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.cursor).toEqual({ current_state: "probe", transition_count: 2 });

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{ executionId: "exec-3", toolId: "tool-3" }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 3 });
  // the resumed agent execution is the third global one, sharing the index
  // space with the decision execution in between
  const resumed = state.executions[2];
  expect(resumed?.type).toBe("agent");
  expect(resumed?.index).toBe(3);
  expect(resumed?.state_id).toBe("probe");
  // the activation leaf carries the global index, decision included
  const leaf = await lstat(join(harness.dirs.runRoot, "activations", "3-probe"));
  expect(leaf.isDirectory()).toBe(true);
  expect(fake.pairs[0]?.activationIndex).toBe(3);
});

test("4. revisits of one agent state: old and winning accepted outputs are restored in order", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_FROM_OUTPUT);
  const runInputs = await prefixCreateRun(harness);
  const accepted: AcceptedStateOutput[] = [];
  // activation 1: facts say alpha (f1 true) ... but the decision routes beta -> coder
  // (the model forbids beta when f1 is true? no: c1 forbids beta when f1 true -> inconsistent?
  //  facts {f1:true,f2:false}: rule-a matches first -> alpha. To revisit we need beta,
  //  which requires f1 false / f2 true.)
  await prefixAgentStep(harness, runInputs, accepted, "coder", 1, {
    jsonContent: { facts: FACTS_BETA },
  });
  await prefixDecisionStep(harness, runInputs, accepted, "check", 2); // beta -> coder (revisit)
  await prefixAgentStep(harness, runInputs, accepted, "coder", 3, {
    jsonContent: { facts: FACTS_ALPHA },
  });
  await prefixDecisionStep(harness, runInputs, accepted, "check", 4); // alpha -> probe
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.cursor).toEqual({ current_state: "probe", transition_count: 4 });
  expect(prefixState.executions.map((execution) => execution.index)).toEqual([1, 2, 3, 4]);

  // the resumed probe must read the winning facts of the highest activation
  const reopened = await reopenHarness(harness);
  let observedFacts: string | undefined;
  const fake = fakeRuntime([{
    executionId: "exec-5",
    toolId: "tool-5",
    onRun: async (session: FakeAgentSession) => {
      const port = session.activation.input_ports.find((entry) => entry.id === "facts");
      if (port !== undefined) {
        observedFacts = await readFile(port.path, "utf8");
      }
    },
  }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  // the winner is the second coder activation (highest activation index)
  expect(observedFacts).toBe(FACTS_ALPHA);
  // both old (non-winning) and winning records are restored, in execution order
  const oldRecord = agentAt(state, 0).outputs?.[0]?.digest;
  const winningRecord = agentAt(state, 2).outputs?.[0]?.digest;
  const oldPrefix = agentAt(prefixState, 0).outputs?.[0]?.digest;
  const winningPrefix = agentAt(prefixState, 2).outputs?.[0]?.digest;
  expect(oldRecord).toEqual(oldPrefix);
  expect(winningRecord).toEqual(winningPrefix);
  expect(oldRecord).not.toEqual(winningRecord);
  // both activation trees still exist, old and winning
  expect((await lstat(join(harness.dirs.runRoot, "activations", "1-coder"))).isDirectory()).toBe(true);
  expect((await lstat(join(harness.dirs.runRoot, "activations", "3-coder"))).isDirectory()).toBe(true);
  expect(kinds(reopened)[0]).toBe("start_agent_execution");
});

test("5. uninterrupted run and pause/resume produce equivalent durable states", async () => {
  // One trusted pipeline and one bundle root for both variants: the durable
  // pipeline identity (bundle root included) must be identical.
  const dirs = await makeDirs();
  await writeBundle(dirs, PIPELINE_DECISION_INPUT);
  const pipeline = await loadPipelineV2(dirs.bundle);
  const runRootA = join(dirs.root, "runs-a", "resume-run");
  const runRootB = join(dirs.root, "runs-b", "resume-run");
  const stateRootA = join(dirs.root, "state-a");
  const stateRootB = join(dirs.root, "state-b");
  await mkdir(runRootA, { recursive: true });
  await mkdir(runRootB, { recursive: true });
  await mkdir(stateRootA, { recursive: true });
  await mkdir(stateRootB, { recursive: true });
  resetClock();

  // Variant A: one uninterrupted fresh coordination, fixed sessions/clock.
  const freshA = new PipelineV2RunStateSink({ stateRoot: stateRootA, runId: "resume-run", io: countingIo().io, now: nextTick });
  const fakeA = fakeRuntime([{ executionId: "exec-1", toolId: "tool-1" }]);
  const resultA = await coordinatePipelineV2Run({
    pipeline,
    runId: "resume-run",
    runRoot: runRootA,
    projectSourcePath: dirs.projectSource,
    inputBindings: bindingsFor(dirs, pipeline) as readonly RunInputBinding[],
    sink: new RecordingSink(freshA),
    runtime: fakeA.runtime,
  }, NEUTRAL_CONTROL);
  if (!resultA.ok) {
    throw new Error(`variant A failed: ${resultA.reason}`);
  }
  const stateA = resultA.state;

  // Variant B: prefix (agent execution), simulated restart, resume. The
  // clock restarts with the variant so both runs share identical stamps.
  resetClock();
  const freshB = new PipelineV2RunStateSink({ stateRoot: stateRootB, runId: "resume-run", io: countingIo().io, now: nextTick });
  const prefixRec = new RecordingSink(freshB);
  await prepareRunProject(dirs.projectSource, runRootB);
  const runInputs = await snapshotRunInputs(pipeline, bindingsFor(dirs, pipeline) as readonly RunInputBinding[], runRootB);
  await prefixRec.dispatch({
    kind: "create_run",
    runId: "resume-run",
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
  });
  const acceptedB: AcceptedStateOutput[] = [];
  await prefixAgentStep(
    { pipeline, recording: prefixRec } as ResumeHarness,
    runInputs,
    acceptedB,
    "coder",
    1,
    {},
    { execution: "exec-1", tool: "tool-1" },
  );
  const realB = await PipelineV2RunStateSink.open({ stateRoot: stateRootB, runId: "resume-run", io: countingIo().io, now: nextTick });
  const fakeB = fakeRuntime([]);
  const resultB = await resumePipelineV2Run({
    pipeline,
    runId: "resume-run",
    runRoot: runRootB,
    sink: new RecordingSink(realB),
    runtime: fakeB.runtime,
  }, NEUTRAL_CONTROL);
  if (!resultB.ok) {
    throw new Error(`variant B failed: ${JSON.stringify(resultB)}`);
  }
  const stateB = resultB.state;

  // the final durable states are equivalent under the fixed clock and the
  // fixed session ids; runtime-independent fields included
  expect(stateB).toEqual(stateA);
  const durableB = parsePipelineV2RunState(
    await readFile(join(stateRootB, "pipeline-runs", "resume-run", "state.json"), "utf8"),
  );
  expect(durableB).toEqual(stateB);
});

test("6. deleted original input bindings and project source are never re-read", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs = await prefixCreateRun(harness);
  await prefixAgentStep(harness, runInputs, [], "coder", 1);
  // the process restart loses the original user sources entirely
  await rm(harness.dirs.sources, { recursive: true, force: true });
  await rm(harness.dirs.projectSource, { recursive: true, force: true });

  const fingerprintBefore = await fingerprint(harness.dirs.root);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.status).toBe("success");
  // the original sources stay deleted: nothing re-read or re-created them
  const after = await fingerprint(harness.dirs.root);
  const sourceLines = after.split("\n").filter((line) => line.startsWith("userdata/") || line.startsWith("project-source"));
  expect(sourceLines).toEqual([]);
  expect(await lstatOrNull(join(harness.dirs.sources, "facts.json"))).toBeNull();
  expect(await lstatOrNull(harness.dirs.projectSource)).toBeNull();
});

test("7. a damaged old non-winning output refuses the resume before any write or Session", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_FROM_OUTPUT);
  const runInputs = await prefixCreateRun(harness);
  const accepted: AcceptedStateOutput[] = [];
  await prefixAgentStep(harness, runInputs, accepted, "coder", 1, { jsonContent: { facts: FACTS_BETA } });
  await prefixDecisionStep(harness, runInputs, accepted, "check", 2); // beta -> coder
  await prefixAgentStep(harness, runInputs, accepted, "coder", 3, { jsonContent: { facts: FACTS_ALPHA } });
  await prefixDecisionStep(harness, runInputs, accepted, "check", 4); // alpha -> probe
  // damage the OLD, non-winning accepted output of activation 1
  const oldFacts = join(harness.dirs.runRoot, "activations", "1-coder", "data", "outputs", "facts");
  await writeFile(oldFacts, '{"f1":false,"f2":false}');
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(result, "accepted_output_modified");
  expect(reopened.commands).toHaveLength(0);
  expect(fake.createCalls).toHaveLength(0);
  expect(await fingerprint(harness.dirs.runRoot)).toBe(fingerprintBefore);
});

test("8. a damaged run input refuses the resume before any write or Session", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  // damage the orchestrator-owned input snapshot bytes (digest mismatch)
  const snapshotFile = join(harness.dirs.runRoot, "data", "inputs", "facts_seed");
  await writeFile(snapshotFile, FACTS_BETA);
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(result, "run_input_modified");
  expect(reopened.commands).toHaveLength(0);
  expect(fake.createCalls).toHaveLength(0);
  expect(await fingerprint(harness.dirs.runRoot)).toBe(fingerprintBefore);

  // a substituted symlinked snapshot object is refused the same way
  const harness2 = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness2);
  const snapshotFile2 = join(harness2.dirs.runRoot, "data", "inputs", "facts_seed");
  await rm(snapshotFile2);
  await writeFile(join(harness2.dirs.root, "elsewhere.json"), FACTS_ALPHA);
  await (await import("node:fs/promises")).symlink(join(harness2.dirs.root, "elsewhere.json"), snapshotFile2);
  const fingerprintBefore2 = await fingerprint(harness2.dirs.runRoot);
  const reopened2 = await reopenHarness(harness2);
  const fake2 = fakeRuntime([{}]);
  const result2 = await resumePipelineV2Run({
    pipeline: harness2.pipeline,
    runId: harness2.runId,
    runRoot: harness2.dirs.runRoot,
    sink: reopened2,
    runtime: fake2.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(result2, "run_input_modified");
  expect(reopened2.commands).toHaveLength(0);
  expect(await fingerprint(harness2.dirs.runRoot)).toBe(fingerprintBefore2);
});

test("9. cursor at a terminal without terminal_reached: zero callbacks, ordinary terminal chain", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs = await prefixCreateRun(harness);
  await prefixAgentStep(harness, runInputs, [], "coder", 1);
  await prefixDecisionStep(harness, runInputs, [], "check", 2); // alpha -> done
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.cursor).toEqual({ current_state: "done", transition_count: 2 });
  expect(prefixState.terminal).toBeUndefined();
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.status).toBe("success");
  // zero executor callbacks ran
  expect(fake.createCalls).toHaveLength(0);
  expect(state.executions).toHaveLength(2);
  // the ordinary terminal chain ran: record, publish, succeed
  expect(kinds(reopened)).toEqual([
    "terminal_reached",
    "run_outputs_published",
    "run_succeeded",
  ]);
  expect(state.terminal?.state_id).toBe("done");
  expect(state.run_outputs).toHaveLength(0);
  // the run root only gained the outputs directory
  const after = await fingerprint(harness.dirs.runRoot);
  const added = after.split("\n").filter((line) => !fingerprintBefore.split("\n").includes(line));
  expect(added.every((line) => line.startsWith("outputs"))).toBe(true);
});

test("10. shared budget exhausted at an executable cursor: zero callbacks, typed engine reason", async () => {
  const harness = await setupHarness(PIPELINE_CYCLE_BUDGET);
  const runInputs = await prefixCreateRun(harness);
  await prefixAgentStep(harness, runInputs, [], "coder", 1); // coder -> coder2
  await prefixAgentStep(harness, runInputs, [], "coder2", 2); // coder2 -> coder
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.cursor).toEqual({ current_state: "coder", transition_count: 2 });
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "transition_budget_exhausted");
  // zero callbacks ran: the engine rejects before the first agent callback
  expect(fake.createCalls).toHaveLength(0);
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "transition_budget_exhausted" });
  // no new execution, no transition: the failure is only the run status
  expect(state.executions).toHaveLength(2);
  expect(state.transitions).toHaveLength(2);
  // the run root is unchanged except the failure status inside state.json
  expect(await fingerprint(harness.dirs.runRoot)).toBe(fingerprintBefore);
});

test("11. a commit hook failure stops the graph before the next callback", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness, {
    faults: new Map([
      ["transition_committed", () => new PipelineV2RunStateStoreError("HOOK-FAULT")],
    ]),
  });
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "state_persist_failed");
  // the agent ran fully and was cleaned, but its transition was never
  // committed, so the decision callback never started
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
  expect(state.executions).toHaveLength(1);
  expect(state.transitions).toHaveLength(0);
  expect(state.executions[0]?.phase).toBe("cleanup_completed");
  // the run is finalized durably with the classified persist failure
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "state_persist_failed" });
  expect(kinds(reopened)).toEqual([
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
    "agent_cleanup_completed",
    "transition_committed",
    "run_failed",
  ]);
});

test("12. worker and decision failures after resume use the existing durable failure semantics", async () => {
  // 12a: a typed worker failure after resume
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{ run: "worker_failed" }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "worker_failed");
  expect(state.status).toBe("failed");
  const execution = agentAt(state, 0);
  expect(execution.failure_reason).toBe("worker_failed");
  expect(execution.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(state.transitions).toHaveLength(0);
  expect(kinds(reopened)).toEqual([
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_failed",
    "run_failed",
  ]);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);

  // 12b: a decision data failure after resume (damaged run input) records
  // the typed reason at the run level; the execution never started
  const harness2 = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs2b = await prefixCreateRun(harness2);
  await prefixAgentStep(harness2, runInputs2b, [], "coder", 1);
  const snapshotFile = join(harness2.dirs.runRoot, "data", "inputs", "facts_seed");
  await writeFile(snapshotFile, FACTS_BETA);
  const reopened2 = await reopenHarness(harness2);
  const fake2 = fakeRuntime([]);
  const result2 = await resumePipelineV2Run({
    pipeline: harness2.pipeline,
    runId: harness2.runId,
    runRoot: harness2.dirs.runRoot,
    sink: reopened2,
    runtime: fake2.runtime,
  }, NEUTRAL_CONTROL);
  // the restore verifies every input before the chain runs: the damaged
  // snapshot refuses the resume before any write or Session
  expectRefused(result2, "run_input_modified");
  expect(reopened2.commands).toHaveLength(0);
  expect(fake2.createCalls).toHaveLength(0);

  // 12c: an invalid_facts decision outcome is an ordinary graph result: it
  // routes to the failed terminal, publishes outputs and finalizes durably
  const harness3 = await setupHarness(PIPELINE_DECISION_INPUT, { facts: "{}" });
  await prefixCreateRun(harness3);
  const reopened3 = await reopenHarness(harness3);
  const fake3 = fakeRuntime([{}]);
  const result3 = await resumePipelineV2Run({
    pipeline: harness3.pipeline,
    runId: harness3.runId,
    runRoot: harness3.dirs.runRoot,
    sink: reopened3,
    runtime: fake3.runtime,
  }, NEUTRAL_CONTROL);
  const state3 = expectResumeFailed(result3, "terminal_failed");
  expect(state3.terminal?.state_id).toBe("failed_end");
  expect(state3.run_outputs).toHaveLength(0);
});

test("13. the tool-first cleanup order and exactly-once cleanup survive the resume", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  expectResumeOk(result);
  expect(fake.events).toEqual([
    "create-exec:coder:1",
    "create-tool:coder:1",
    "run:coder",
    "cleanup-tool:coder",
    "cleanup-exec:coder",
  ]);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
});

test("14. signals before and during the resumed execution keep the existing priorities and one cutoff", async () => {
  // 14a: a signal accepted before the first resumed execution
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const signal = alwaysSignalControl("SIGINT");
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, signal.control);
  const state = expectResumeFailed(result, "signal_sigint");
  expect(fake.createCalls).toHaveLength(0);
  expect(state.status).toBe("failed");
  expect(state.failure).toEqual({ reason: "signal_sigint" });
  expect(state.executions).toHaveLength(0);
  expect(signal.freezes()).toBe(1);
  expect(await fingerprint(harness.dirs.runRoot)).toBe(fingerprintBefore);

  // 14b: a signal armed during the worker run stops before the decision
  const harness2 = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness2);
  const reopened2 = await reopenHarness(harness2);
  const armable = armableSignalControl();
  const fake2 = fakeRuntime([{
    onRun: async () => {
      armable.arm();
    },
  }]);
  const result2 = await resumePipelineV2Run({
    pipeline: harness2.pipeline,
    runId: harness2.runId,
    runRoot: harness2.dirs.runRoot,
    sink: reopened2,
    runtime: fake2.runtime,
  }, armable.control);
  const state2 = expectResumeFailed(result2, "signal_sigint");
  expect(state2.status).toBe("failed");
  expect(state2.failure).toEqual({ reason: "signal_sigint" });
  // the agent execution settled and its transition is durable; the
  // decision callback never started
  expect(state2.executions).toHaveLength(1);
  expect(state2.transitions).toHaveLength(1);
  expect(state2.executions[0]?.phase).toBe("cleanup_completed");
  expect(armable.freezes()).toBe(1);
});

test("15. a durability-unknown commit adopts the candidate, poisons the sink, and stops every further dispatch", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  // the reopened sink's io faults the 6th resumed commit
  // (start_agent_execution .. agent_outputs_accepted) after its rename
  const reopened = await reopenHarness(harness, {
    io: faultIo({ failCommit: 6, failStep: "dirsync" }),
  });
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "state_persist_failed");
  expect(reopened.poisoned).toBe(true);
  // the adopted candidate is the visible state with the accepted outputs
  const adopted = reopened.snapshot;
  if (adopted === null) {
    throw new Error("the poisoned sink lost its adopted candidate");
  }
  expect(state).toEqual(adopted);
  expect(state.executions[0]?.phase).toBe("outputs_accepted");
  const onDisk = await readDurableState(harness);
  expect(onDisk.revision).toBe(7);
  expect(onDisk.executions[0]?.phase).toBe("outputs_accepted");
  // and the coordination stopped: no cleanup or finalize was dispatched
  expect(kinds(reopened)).toEqual([
    "start_agent_execution",
    "agent_data_prepared",
    "agent_execution_session_created",
    "agent_tool_session_created",
    "agent_running",
    "agent_outputs_accepted",
  ]);
  // zero further dispatch after the poison
  const commandsAfterPoison = reopened.commands.length;
  await expect(reopened.dispatch({ kind: "agent_data_prepared" } as unknown as PipelineV2RunCommand)).rejects.toThrow(/poisoned/);
  expect(reopened.commands.length).toBe(commandsAfterPoison + 1);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
  expect(fake.pairs[0]?.tool.cleanupCount).toBe(1);
});

test("16. an open waiting run is refused with zero mutations, callbacks or Sessions", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs = await prefixCreateRun(harness);
  await prefixAgentStep(harness, runInputs, [], "coder", 1);
  // the wait controller is the only writer of wait records; the prefix
  // records one open wait through the ordinary command
  const target = transitionTarget(harness.pipeline, "coder", "completed");
  await harness.recording.dispatch({
    kind: "run_waiting",
    stateId: target.to,
    reason: "stage_iteration_limit_exhausted",
    requestSha256: "a".repeat(64),
    actions: [{ id: "continue_stage", to: target.to }],
  });
  const prefixState = harness.recording.snapshot as PipelineV2RunState;
  expect(prefixState.status).toBe("waiting");
  const fingerprintBefore = await fingerprint(harness.dirs.runRoot);

  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
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
  // the durable document, its run root and the wait record are untouched
  expect(await fingerprint(harness.dirs.runRoot)).toBe(fingerprintBefore);
  const durable = await readDurableState(harness);
  expect(durable.status).toBe("waiting");
  expect(durable.waits).toHaveLength(1);
  expect(durable.waits[0]?.response).toBeUndefined();
});

test("17. in-flight, unbound, publishing, final, missing, mismatched and poisoned states are refused without side effects", async () => {
  // 17a: an in-flight agent execution
  const inFlight = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(inFlight);
  await inFlight.recording.dispatch({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
  const fpInFlight = await fingerprint(inFlight.dirs.runRoot);
  const reopenedInFlight = await reopenHarness(inFlight);
  const fakeInFlight = fakeRuntime([{}]);
  const resultInFlight = await resumePipelineV2Run({
    pipeline: inFlight.pipeline,
    runId: inFlight.runId,
    runRoot: inFlight.dirs.runRoot,
    sink: reopenedInFlight,
    runtime: fakeInFlight.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultInFlight, "invalid_state");
  expect(reopenedInFlight.commands).toHaveLength(0);
  expect(fakeInFlight.createCalls).toHaveLength(0);
  expect(await fingerprint(inFlight.dirs.runRoot)).toBe(fpInFlight);

  // 17b: a settled but unbound execution (the prefix stops after the
  // confirmed cleanup, before the transition commit)
  const unbound2 = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs2 = await prefixCreateRun(unbound2);
  const activation = await prepareActivationData(unbound2.pipeline, runInputs2, [], "coder", 1);
  await unbound2.recording.dispatch({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
  await unbound2.recording.dispatch({ kind: "agent_data_prepared" });
  await unbound2.recording.dispatch({ kind: "agent_execution_session_created", sessionId: "exec-1" });
  await unbound2.recording.dispatch({ kind: "agent_tool_session_created", sessionId: "tool-1" });
  await unbound2.recording.dispatch({ kind: "agent_running" });
  await (new PrefixWorker({}, activation)).run();
  const records = await acceptActivationOutputs(unbound2.pipeline, activation);
  await unbound2.recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await unbound2.recording.dispatch({ kind: "agent_cleanup_completed" });
  const fpUnbound = await fingerprint(unbound2.dirs.runRoot);
  const reopenedUnbound = await reopenHarness(unbound2);
  const fakeUnbound = fakeRuntime([{}]);
  const resultUnbound = await resumePipelineV2Run({
    pipeline: unbound2.pipeline,
    runId: unbound2.runId,
    runRoot: unbound2.dirs.runRoot,
    sink: reopenedUnbound,
    runtime: fakeUnbound.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultUnbound, "invalid_state");
  expect(reopenedUnbound.commands).toHaveLength(0);
  expect(fakeUnbound.createCalls).toHaveLength(0);
  expect(await fingerprint(unbound2.dirs.runRoot)).toBe(fpUnbound);

  // 17c: a publishing phase
  const publishing = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputsC = await prefixCreateRun(publishing);
  await prefixAgentStep(publishing, runInputsC, [], "coder", 1);
  await prefixDecisionStep(publishing, runInputsC, [], "check", 2);
  await publishing.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  const fpPublishing = await fingerprint(publishing.dirs.runRoot);
  const reopenedPublishing = await reopenHarness(publishing);
  const fakePublishing = fakeRuntime([]);
  const resultPublishing = await resumePipelineV2Run({
    pipeline: publishing.pipeline,
    runId: publishing.runId,
    runRoot: publishing.dirs.runRoot,
    sink: reopenedPublishing,
    runtime: fakePublishing.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultPublishing, "invalid_state");
  expect(reopenedPublishing.commands).toHaveLength(0);
  expect(await fingerprint(publishing.dirs.runRoot)).toBe(fpPublishing);

  // 17d: a final success status
  const finished = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputsD = await prefixCreateRun(finished);
  await prefixAgentStep(finished, runInputsD, [], "coder", 1);
  await prefixDecisionStep(finished, runInputsD, [], "check", 2);
  await finished.recording.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  await finished.recording.dispatch({ kind: "run_outputs_published", outputs: [] });
  await finished.recording.dispatch({ kind: "run_succeeded" });
  const fpFinished = await fingerprint(finished.dirs.runRoot);
  const reopenedFinished = await reopenHarness(finished);
  const fakeFinished = fakeRuntime([]);
  const resultFinished = await resumePipelineV2Run({
    pipeline: finished.pipeline,
    runId: finished.runId,
    runRoot: finished.dirs.runRoot,
    sink: reopenedFinished,
    runtime: fakeFinished.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultFinished, "invalid_state");
  expect(reopenedFinished.commands).toHaveLength(0);
  expect(await fingerprint(finished.dirs.runRoot)).toBe(fpFinished);

  // 17e: a final failed status
  const failed = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(failed);
  const reopenedFailedPrefix = await reopenHarness(failed);
  const fakeFailedPrefix = fakeRuntime([{ run: "worker_failed" }]);
  const failedResult = await resumePipelineV2Run({
    pipeline: failed.pipeline,
    runId: failed.runId,
    runRoot: failed.dirs.runRoot,
    sink: reopenedFailedPrefix,
    runtime: fakeFailedPrefix.runtime,
  }, NEUTRAL_CONTROL);
  const failedState = expectResumeFailed(failedResult, "worker_failed");
  expect(failedState.status).toBe("failed");
  const fpFailed = await fingerprint(failed.dirs.runRoot);
  const reopenedFailed = await reopenHarness(failed);
  const fakeFailed = fakeRuntime([]);
  const resultFailed = await resumePipelineV2Run({
    pipeline: failed.pipeline,
    runId: failed.runId,
    runRoot: failed.dirs.runRoot,
    sink: reopenedFailed,
    runtime: fakeFailed.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultFailed, "invalid_state");
  expect(reopenedFailed.commands).toHaveLength(0);
  expect(await fingerprint(failed.dirs.runRoot)).toBe(fpFailed);

  // 17f: a missing state (fresh, unopened sink)
  const missing = await setupHarness(PIPELINE_DECISION_INPUT);
  const fakeMissing = fakeRuntime([{}]);
  const resultMissing = await resumePipelineV2Run({
    pipeline: missing.pipeline,
    runId: missing.runId,
    runRoot: missing.dirs.runRoot,
    sink: missing.recording,
    runtime: fakeMissing.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultMissing, "missing_state");
  expect(resultMissing.state).toBeNull();
  expect(missing.recording.commands).toHaveLength(0);
  expect(fakeMissing.createCalls).toHaveLength(0);

  // 17g: a mismatched run id
  const mismatched = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(mismatched);
  const fpMismatched = await fingerprint(mismatched.dirs.runRoot);
  const reopenedMismatched = await reopenHarness(mismatched);
  const fakeMismatched = fakeRuntime([{}]);
  const resultMismatched = await resumePipelineV2Run({
    pipeline: mismatched.pipeline,
    runId: "other-run",
    runRoot: mismatched.dirs.runRoot,
    sink: reopenedMismatched,
    runtime: fakeMismatched.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultMismatched, "run_id_mismatch");
  expect(reopenedMismatched.commands).toHaveLength(0);
  expect(fakeMismatched.createCalls).toHaveLength(0);
  expect(await fingerprint(mismatched.dirs.runRoot)).toBe(fpMismatched);

  // 17h: a poisoned sink
  const poisoned = await setupHarness(PIPELINE_DECISION_INPUT);
  const poisonedCounted = countingIo(faultIo({ failCommit: 2, failStep: "dirsync" }));
  const poisonedFresh = new PipelineV2RunStateSink({
    stateRoot: poisoned.dirs.stateRoot,
    runId: poisoned.runId,
    io: poisonedCounted.io,
    now: nextTick,
  });
  const poisonedRecording = new RecordingSink(poisonedFresh);
  await prepareRunProject(poisoned.dirs.projectSource, poisoned.dirs.runRoot);
  const poisonedInputs = await snapshotRunInputs(
    poisoned.pipeline,
    bindingsFor(poisoned.dirs, poisoned.pipeline) as readonly RunInputBinding[],
    poisoned.dirs.runRoot,
  );
  await poisonedRecording.dispatch({
    kind: "create_run",
    runId: poisoned.runId,
    pipeline: pipelineV2RunPipelineIdentity(poisoned.pipeline),
    inputs: poisonedInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  const poisonError = await poisonedRecording
    .dispatch({ kind: "start_agent_execution", stateId: "coder", profile: "coder" })
    .catch((cause: unknown) => cause);
  expect(poisonError).toBeInstanceOf(PipelineV2RunStateDurabilityError);
  expect(poisonedFresh.poisoned).toBe(true);
  const fpPoisoned = await fingerprint(poisoned.dirs.runRoot);
  const fakePoisoned = fakeRuntime([{}]);
  const resultPoisoned = await resumePipelineV2Run({
    pipeline: poisoned.pipeline,
    runId: poisoned.runId,
    runRoot: poisoned.dirs.runRoot,
    sink: poisonedRecording,
    runtime: fakePoisoned.runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(resultPoisoned, "sink_poisoned");
  expect(fakePoisoned.createCalls).toHaveLength(0);
  expect(await fingerprint(poisoned.dirs.runRoot)).toBe(fpPoisoned);
});

test("18. a forged pipeline is refused before any sink, runtime or control read; Proxy traps stay zero", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);

  let sinkReads = 0;
  const guardedSink = Object.defineProperties({}, {
    snapshot: {
      get() {
        sinkReads += 1;
        throw new Error("SINK-READ");
      },
      enumerable: true,
    },
    poisoned: {
      get() {
        sinkReads += 1;
        throw new Error("SINK-READ");
      },
      enumerable: true,
    },
    dispatch: {
      get() {
        sinkReads += 1;
        throw new Error("SINK-READ");
      },
      enumerable: true,
    },
  }) as unknown as PipelineV2CoordinatorStateSink;

  let runtimeReads = 0;
  const guardedRuntime = Object.defineProperties({}, {
    createExecutionSession: {
      get() {
        runtimeReads += 1;
        throw new Error("RUNTIME-READ");
      },
      enumerable: true,
    },
    createToolSession: {
      get() {
        runtimeReads += 1;
        throw new Error("RUNTIME-READ");
      },
      enumerable: true,
    },
  }) as unknown as PipelineV2AgentRuntime;

  let controlReads = 0;
  const guardedControl = Object.defineProperties({}, {
    currentSignal: {
      get() {
        controlReads += 1;
        throw new Error("CONTROL-READ");
      },
      enumerable: true,
    },
    freezeSignal: {
      get() {
        controlReads += 1;
        throw new Error("CONTROL-READ");
      },
      enumerable: true,
    },
  }) as unknown as PipelineV2CoordinatorControl;

  let trapHits = 0;
  const proxy = new Proxy(harness.pipeline, {
    get(target, property, receiver) {
      trapHits += 1;
      return Reflect.get(target, property, receiver);
    },
  });

  const fingerprintBefore = await fingerprint(harness.dirs.root);
  for (const forged of [
    { ...harness.pipeline } as unknown as ResolvedPipelineV2,
    structuredClone(harness.pipeline) as unknown as ResolvedPipelineV2,
    proxy,
  ]) {
    const result = await resumePipelineV2Run({
      pipeline: forged,
      runId: harness.runId,
      runRoot: harness.dirs.runRoot,
      sink: guardedSink,
      runtime: guardedRuntime,
    }, guardedControl);
    expectRefused(result, "internal_error");
    expect(result.state).toBeNull();
  }
  expect(sinkReads).toBe(0);
  expect(runtimeReads).toBe(0);
  expect(controlReads).toBe(0);
  expect(trapHits).toBe(0);
  expect(await fingerprint(harness.dirs.root)).toBe(fingerprintBefore);
});

test("19. every pre-resume failure group leaves the filesystem fingerprint identical", async () => {
  // covered in depth by tests 7, 8, 10, 14a, 16 and 17; this test pins the
  // pipeline_mismatch group explicitly: a different trusted pipeline
  const harnessA = await setupHarness(PIPELINE_DECISION_INPUT);
  const runInputs = await prefixCreateRun(harnessA);
  await prefixAgentStep(harnessA, runInputs, [], "coder", 1);
  const fingerprintBefore = await fingerprint(harnessA.dirs.root);

  // load the same bundle directory as a fresh loader run of a DIFFERENT
  // pipeline bundle (the two-agents shape) to force an identity mismatch
  const harnessB = await setupHarness(PIPELINE_DECISION_FROM_OUTPUT);
  const result = await resumePipelineV2Run({
    pipeline: harnessB.pipeline,
    runId: harnessA.runId,
    runRoot: harnessA.dirs.runRoot,
    sink: await reopenHarness(harnessA),
    runtime: fakeRuntime([]).runtime,
  }, NEUTRAL_CONTROL);
  expectRefused(result, "pipeline_mismatch");
  expect(await fingerprint(harnessA.dirs.root)).toBe(fingerprintBefore);
});

test("20. the resume result is deep-frozen and content-free", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  // deep-frozen
  expect(Object.isFrozen(result)).toBe(true);
  expect(Object.isFrozen(state)).toBe(true);
  expect(Object.isFrozen(state.executions)).toBe(true);
  expect(Object.isFrozen(state.cursor)).toBe(true);
  expect(() => {
    (result as { ok: boolean }).ok = false;
  }).toThrow();
  // content-free: no source paths, no project source, no prompt or fact
  // bodies, no environment values or credentials anywhere in the outcome
  const resultJson = JSON.stringify(result);
  expect(resultJson).not.toContain(harness.dirs.sources);
  expect(resultJson).not.toContain(harness.dirs.projectSource);
  expect(resultJson).not.toContain("facts.json");
  expect(resultJson).not.toContain("IMPLEMENT-THE-TASK");
  expect(resultJson).not.toContain(FACTS_ALPHA);
  expect(resultJson).not.toContain("LLM_KEY");
  expect(resultJson).not.toContain("credential");
  // digests are the only content carriers
  expect(state.inputs.every((input) => isHexDigest(input.digest))).toBe(true);
});

async function lstatOrNull(path: string): Promise<Stats | null> {
  try {
    return await lstat(path);
  } catch (cause: unknown) {
    const code = (cause as { code?: string }).code;
    if (code === "ENOENT") {
      return null;
    }
    throw cause;
  }
}

test("the reducer's state schema stays v6 and resume never dispatches create_run", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  expectResumeOk(result);
  const allCommands = [...harness.recording.commands, ...reopened.commands];
  expect(allCommands.filter((command) => command.kind === "create_run")).toHaveLength(1);
  expect(allCommands.filter((command) => String(command.kind).startsWith("wait_"))).toHaveLength(0);
  const durable = await readDurableState(harness);
  expect(durable.schema_version).toBe(6);
});

test("a malformed durable state document refuses the reopen without side effects", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const statePath = pipelineV2RunStatePath(harness.dirs.stateRoot, harness.runId);
  await writeFile(statePath, "{ damaged");
  const fingerprintBefore = await fingerprint(harness.dirs.stateRoot);
  const error = await reopenHarness(harness).catch((cause: unknown) => cause);
  expect(error).toBeInstanceOf(PipelineV2StateError);
  // the damaged document is not finalized or repaired by the failed open
  expect(await readFile(statePath, "utf8")).toBe("{ damaged");
  expect(await fingerprint(harness.dirs.stateRoot)).toBe(fingerprintBefore);
});

test("the resumed run keeps the declared pipeline identity it was created with", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const durableBefore = await readDurableState(harness);
  expect(durableBefore.pipeline).toEqual(pipelineV2RunPipelineIdentity(harness.pipeline));
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.pipeline).toEqual(durableBefore.pipeline);
  expect(state.pipeline.schema_version).toBe(2);
  expect(basename(harness.dirs.runRoot)).toBe(harness.runId);
});

test("the resumed run reuses the restored input snapshot, not a new one", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const inputsBefore = (harness.recording.snapshot as PipelineV2RunState).inputs;
  // the original binding path is gone; the restored snapshot still works
  await rm(join(harness.dirs.sources, "facts.json"));
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{}]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.inputs).toEqual(inputsBefore);
  expect(await lstatOrNull(join(harness.dirs.sources, "facts.json"))).toBeNull();
});

test("a resumed decision from an accepted agent output routes through the shared chain", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_FROM_OUTPUT);
  const runInputs = await prefixCreateRun(harness);
  await prefixAgentStep(harness, runInputs, [], "coder", 1, {
    jsonContent: { facts: FACTS_ALPHA },
  });
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{ executionId: "exec-3", toolId: "tool-3" }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeOk(result);
  expect(state.status).toBe("success");
  expect(state.cursor).toEqual({ current_state: "done", transition_count: 3 });
  expect(kinds(reopened)).toEqual([
    "start_decision_execution",
    "decision_evaluated",
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
  const evaluated = reopened.commands[1];
  expect((evaluated?.result as Record<string, unknown>)?.status).toBe("selected");
  expect((evaluated?.result as Record<string, unknown>)?.outcome).toBe("alpha");
});

test("a resumed worker timeout fails with the existing timeout semantics", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{ run: "worker_timeout" }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "worker_timeout");
  const timedOut = agentAt(state, 0);
  expect(timedOut.failure_reason).toBe("worker_timeout");
  expect(timedOut.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(state.transitions).toHaveLength(0);
});

test("an execution-session create failure after resume cleans the tool slot as not_required", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  const fake = fakeRuntime([{ executionCreate: "throw" }]);
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: fake.runtime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "internal_error");
  const failedExecution = agentAt(state, 0);
  expect(failedExecution.phase).toBe("failed");
  expect(failedExecution.session_cleanup).toEqual({ execution: "not_required", tool: "not_required" });
  expect(fake.createCalls).toHaveLength(0);
});

test("a durably failed session cleanup after resume finalizes run_cleanup_failed", async () => {
  const harness = await setupHarness(PIPELINE_DECISION_INPUT);
  await prefixCreateRun(harness);
  const reopened = await reopenHarness(harness);
  // the tool cleanup explodes; the coordinator must still clean the
  // execution session exactly once and finalize the run honestly
  const fake = fakeRuntime([{}]);
  const failingRuntime: PipelineV2AgentRuntime = {
    createExecutionSession: (state, activation) => fake.runtime.createExecutionSession(state, activation),
    createToolSession: async (state, activation) => {
      const tool = await fake.runtime.createToolSession(state, activation);
      return {
        sessionId: tool.sessionId,
        cleanup: async () => {
          throw new Error("TOOL-CLEANUP-EXPLODED");
        },
      };
    },
  };
  const result = await resumePipelineV2Run({
    pipeline: harness.pipeline,
    runId: harness.runId,
    runRoot: harness.dirs.runRoot,
    sink: reopened,
    runtime: failingRuntime,
  }, NEUTRAL_CONTROL);
  const state = expectResumeFailed(result, "session_cleanup_failed");
  expect(agentAt(state, 0).session_cleanup).toEqual({ execution: "completed", tool: "failed" });
  expect(state.failure).toEqual({ reason: "session_cleanup_failed" });
  expect(state.transitions).toHaveLength(0);
  expect(fake.pairs[0]?.execution.cleanupCount).toBe(1);
});
