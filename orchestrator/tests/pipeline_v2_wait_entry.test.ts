/**
 * The production wait-entry at the trusted stage→planning boundary: the
 * coordinator recognizes the exact boundary after its own committed
 * transition (live) and on the restart path (an already durable transition
 * without the wait), derives the wait request exclusively from the trusted
 * compiled `stage_wait` policy of the destination planning role and the
 * durable run (no caller policy), enters the wait through the single
 * public `enterPipelineV2Wait`, and suspends with the exact waiting result
 * branch — never a durable `run_failed`.
 *
 * Every prefix is built through the real facades, the real reducer, the
 * real data plane and the real wait store over a real pipeline bundle whose
 * planning roles carry the compiled `stage_wait` policy; no reason or
 * action is ever injected into the coordinator. Restarts go through the
 * ordinary `PipelineV2RunStateSink.open`. No LLM, no Docker Helper, no
 * launcher credential, no sleeps.
 */
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  resumePipelineV2Run,
  coordinatePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinationResult,
  type PipelineV2CoordinatorControl,
  type PipelineV2ResumeCoordinationResult,
} from "../src/pipeline_v2_coordinator.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { compiledExecutionRoleFor } from "../src/pipeline_v2_orchestration.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  parsePipelineV2RunState,
  pipelineV2OpenStageIteration,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { acceptPipelineV2PlanningRunPlan } from "../src/pipeline_v2_planning_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { applyPipelineV2PlanningRunPlanHandoff } from "../src/pipeline_v2_planning_run_plan_handoff_controller.ts";
import { enterPipelineV2Wait } from "../src/pipeline_v2_wait_controller.ts";
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

function pipelineYaml(plannerWait: StageWaitPolicy, shape: "cycle" | "terminal" | "control" = "cycle"): string {
  const plannerBlock = shape === "cycle"
    ? `  - id: planner2
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
        to: dev_entry
`
    : "";
  const plannerRole = shape === "cycle"
    ? `    - state_id: planner2
      role: planning
      plan_output: plan2
      stage_wait:
        reason: ${plannerWait.reason}
        actions:
${plannerWait.actions.map((action) => `          - ${action}`).join("\n")}
`
    : "";
  const devTarget = shape === "terminal" ? "done" : shape === "control" ? "gate" : "planner2";
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
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
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
        reason: initial_planning_completed
        actions:
          - continue_stage
          - revise_task
${plannerRole}    - state_id: dev_entry
      role: stage
      stage_template: development
${shape === "control" ? `    - state_id: gate
      role: control
` : ""}
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
        to: ${devTarget}
${plannerBlock}${gate}  - id: done
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

const FULL_POLICY: StageWaitPolicy = { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] };

const PROPOSAL_R1 = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
};

const PROPOSAL_R1_TWO_STAGES = {
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

const PROPOSAL_POINTER_TWO_STAGES = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
    { id: "stage-2", template: "development", tasks: [{ id: "task-b", depends_on: [] }] },
  ],
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

interface Harness {
  root: string;
  stateRoot: string;
  bundle: string;
  sources: string;
  projectSource: string;
  runRoot: string;
  statePath: string;
}

async function makeHarness(
  prefix: string,
  runId: string,
  plannerWait: StageWaitPolicy = FULL_POLICY,
  shape: "cycle" | "terminal" | "control" = "cycle",
): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  if (shape === "control") {
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  }
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml(plannerWait, shape));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "schemas", "plan2.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
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
  await mkdir(runRoot, { mode: 0o700 });
  return { root, stateRoot, bundle, sources, projectSource, runRoot, statePath: join(runRoot, "state.json") };
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

/** A recording sink whose named commands commit first and then fail as durability-unknown. */
class DurabilityFaultSink {
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

  async dispatch(command: Parameters<PipelineV2RunStateSink["dispatch"]>[0]): Promise<void> {
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

async function requestFileIdentity(harness: Harness, waitIndex: number): Promise<{ inode: number; mode: number; mtimeMs: number; bytes: string }> {
  const path = join(harness.runRoot, "waits", `${waitIndex}.request.json`);
  const info = await stat(path);
  return { inode: info.ino, mode: info.mode, mtimeMs: info.mtimeMs, bytes: await readFile(path, "utf8") };
}

interface SessionPair {
  sessionId: string;
  cleanup(): Promise<void>;
  runAgent(): Promise<{ status: "completed" }>;
}

/**
 * The fake runtime of the coordinator proofs: sessions with unique ids,
 * an event log for the cleanup order, and a completed worker result. The
 * tool session is created only after the execution session, mirroring the
 * production runtime.
 */
function fakeRuntime(prefix: string): {
  runtime: PipelineV2AgentRuntime;
  events: string[];
  sessionIds: string[];
} {
  const events: string[] = [];
  const sessionIds: string[] = [];
  let n = 0;
  const make = (kind: "execution" | "tool"): SessionPair => {
    n += 1;
    const id = `${prefix}-${kind}-${n}`;
    sessionIds.push(id);
    return {
      sessionId: id,
      runAgent: async () => {
        events.push(`run:${kind}`);
        return { status: "completed" as const };
      },
      cleanup: async () => {
        events.push(`cleanup:${kind}`);
      },
    };
  };
  return {
    runtime: {
      createExecutionSession: async () => make("execution"),
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

async function bindingsFor(harness: Harness, shape: "cycle" | "terminal" | "control"): Promise<readonly RunInputBinding[]> {
  const bindings: RunInputBinding[] = [{ id: "task", path: join(harness.sources, "task.md") }];
  if (shape === "control") {
    bindings.push({ id: "facts_seed", path: join(harness.sources, "facts.json") });
  }
  return bindings;
}

/**
 * The honest prefix up to the stage-entry cursor: the plan r1 accepted,
 * generation 1 / iteration 1 open, the planning transition committed. No
 * stage execution has run yet and no wait exists.
 */
async function prefixStageEntry(
  harness: Harness,
  runId: string,
  shape: "cycle" | "terminal" | "control" = "cycle",
  proposal: unknown = PROPOSAL_R1,
): Promise<{ recording: RecordingSink; raw: PipelineV2RunStateSink; runInputs: RunInputsSnapshot; revision: number; pipeline: ResolvedPipelineV2 }> {
  clockValue = 0;
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  await prepareRunProject(harness.projectSource, harness.runRoot);
  const runInputs = await snapshotRunInputs(pipeline, await bindingsFor(harness, shape), harness.runRoot);
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
  const acceptedPlan = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: harness.runRoot, sink: recording as never });
  await ensurePipelineV2StageIteration({
    compiledPlan: acceptedPlan.compiled_plan,
    stageId: "stage-1",
    initialBudget: 2,
    sink: recording as never,
  });
  await recording.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  return { recording, raw, runInputs, revision: recording.snapshot?.revision ?? 0, pipeline };
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
    profile: "architect",
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

async function commitTransition(sink: RecordingSink, from: string, to: string, executionIndex: number): Promise<void> {
  await sink.dispatch({
    kind: "transition_committed",
    step: { from, outcome: "completed", to, transition_index: 0 },
    executionIndex,
  });
}

/** Opens the existing run through the ordinary sink factory. */
async function reopen(harness: Harness, runId: string): Promise<PipelineV2RunStateSink> {
  return await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId, now: nextTick });
}

interface ResumeCall {
  recording: RecordingSink;
  result: PipelineV2ResumeCoordinationResult;
  events: string[];
  sessionIds: string[];
}

/** One real coordinator resume over the reopened run with the fake runtime. */
async function resumeWith(
  harness: Harness,
  runId: string,
  pipeline: ResolvedPipelineV2,
  options: { sink?: PipelineV2RunStateSink; runtimePrefix?: string } = {},
): Promise<ResumeCall> {
  const raw = options.sink ?? (await reopen(harness, runId));
  const recording = new RecordingSink(raw);
  const fake = fakeRuntime(options.runtimePrefix ?? "live");
  const result = await resumePipelineV2Run(
    { pipeline, runId, runRoot: harness.runRoot, sink: recording as never, runtime: fake.runtime },
    CONTROL,
  );
  return { recording, result, events: fake.events, sessionIds: fake.sessionIds };
}

function expectWaiting(result: PipelineV2CoordinationResult | PipelineV2ResumeCoordinationResult): PipelineV2RunState {
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

const LIVE_STAGE_COMMANDS = [
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "transition_committed",
  "run_waiting",
];

test("1. live stage→planning: exact lifecycle, transition, run_waiting, waiting result, no planning execution", async () => {
  const runId = "wait-entry-live";
  const harness = await makeHarness("wait-entry-live", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  const before = prefix.recording.snapshot as PipelineV2RunState;
  expect(before.status).toBe("active");

  const call = await resumeWith(harness, runId, pipeline);
  const state = expectWaiting(call.result);
  expect(call.recording.commands.map((command) => command.kind)).toEqual(LIVE_STAGE_COMMANDS);
  expect(state.revision).toBe(before.revision + 9);
  expect(state.executions).toHaveLength(2);
  expect(state.executions[1]?.type).toBe("agent");
  expect(state.executions[1]?.state_id).toBe("dev_entry");
  expect(state.executions[1]?.execution_role).toBe("stage");
  expect(state.executions[1]?.iteration_index).toBe(1);
  const stageExecution = state.executions[1];
  if (stageExecution?.type !== "agent") {
    throw new Error("expected an agent execution");
  }
  expect(stageExecution.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  // exactly one transition, into the planning state; the planning state
  // never started an execution and never got a session
  expect(state.transitions).toHaveLength(2);
  expect(state.transitions[1]?.to).toBe("planner2");
  expect(state.executions.every((execution) => execution.state_id !== "planner2")).toBe(true);
  expect(state.failure).toBeUndefined();
  expect(state.terminal).toBeUndefined();

  // the published request manifest: the trusted policy, verbatim
  const manifest = JSON.parse(await readFile(join(harness.runRoot, "waits", "1.request.json"), "utf8")) as Record<string, unknown>;
  expect(manifest.reason).toBe("stage_iteration_completed");
  expect(manifest.actions).toEqual([
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "planner2" },
  ]);
  expect(manifest.transition_count).toBe(2);
  expect(manifest.state_id).toBe("planner2");
  expect(manifest.wait_index).toBe(1);

  // the durable wait record carries the same derivation
  const durable = state.waits[0];
  expect(durable?.reason).toBe("stage_iteration_completed");
  expect(durable?.actions).toEqual([
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "planner2" },
  ]);

  // session cleanup: each exactly once, tool first; only the execution
  // session's runAgent drives the worker
  expect(call.events).toEqual(["run:execution", "cleanup:tool", "cleanup:execution"]);
  expect(call.sessionIds).toHaveLength(2);

  // the returned state is the authoritative sink snapshot by identity
  expect(call.result.state === call.recording.snapshot).toBe(true);

  // loader round-trip
  expect((await readDurableState(harness)).status).toBe("waiting");
});

test("2. restart after the durable transition: only run_waiting, no repeated transition", async () => {
  const runId = "wait-entry-restart";
  const harness = await makeHarness("wait-entry-restart", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  const raw = prefix.raw;
  // the stage execution and the transition ran in a previous process; the
  // wait is absent
  await runStageActivation(pipeline, prefix.runInputs, [], new RecordingSink(raw), "dev_entry", 2);
  await commitTransition(new RecordingSink(raw), "dev_entry", "planner2", 2);
  const before = (await readDurableState(harness)).revision;

  const call = await resumeWith(harness, runId, pipeline);
  const state = expectWaiting(call.result);
  expect(call.recording.commands.map((command) => command.kind)).toEqual(["run_waiting"]);
  expect(state.revision).toBe(before + 1);
  expect(state.transitions).toHaveLength(2);
  expect(state.executions).toHaveLength(2);
  expect(state.cursor).toEqual({ current_state: "planner2", transition_count: 2 });
  expect(state.waits).toHaveLength(1);
  expect((await readDurableState(harness)).status).toBe("waiting");
});

test("3. orphan request adoption: the faulted entry leaves the request, the retry adopts it byte-identically", async () => {
  const runId = "wait-entry-orphan";
  const harness = await makeHarness("wait-entry-orphan", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  const before = (await readDurableState(harness)).revision;

  // the first attempt: the run_waiting commit fails (not committed) after
  // the request was published — an orphan on an active boundary
  const raw = await reopen(harness, runId);
  const recording = new RecordingSink(raw, new Map([["run_waiting", () => new PipelineV2RunStateStoreError("commit refused by test")]]));
  const fake = fakeRuntime("orphan");
  const failed = await resumePipelineV2Run(
    { pipeline, runId, runRoot: harness.runRoot, sink: recording as never, runtime: fake.runtime },
    CONTROL,
  );
  expect(failed.ok).toBe(false);
  const failure = failed as Record<string, unknown>;
  expect(failure.reason).toBe("state_persist_failed");
  expect("waiting" in failure).toBe(false);
  // no durable run_failed: the boundary stays active and retryable
  const afterFault = await readDurableState(harness);
  expect(afterFault.status).toBe("active");
  expect(afterFault.revision).toBe(before);
  expect(afterFault.failure).toBeUndefined();
  expect(afterFault.transitions).toHaveLength(2);
  // the orphan request exists
  const orphan = await requestFileIdentity(harness, 1);

  // the retry adopts the orphan byte-identically and commits the wait once
  const retry = await resumeWith(harness, runId, pipeline);
  const state = expectWaiting(retry.result);
  expect(retry.recording.commands.map((command) => command.kind)).toEqual(["run_waiting"]);
  expect(state.revision).toBe(before + 1);
  const adopted = await requestFileIdentity(harness, 1);
  expect(adopted.inode).toBe(orphan.inode);
  expect(adopted.mode).toBe(orphan.mode);
  expect(adopted.mtimeMs).toBe(orphan.mtimeMs);
  expect(adopted.bytes).toBe(orphan.bytes);
  expect((await readDurableState(harness)).status).toBe("waiting");
});

test("4. durability-unknown at run_waiting: adopted waiting candidate, no run_failed, reopen sees the wait", async () => {
  const runId = "wait-entry-durability";
  const harness = await makeHarness("wait-entry-durability", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  const before = (await readDurableState(harness)).revision;

  const raw = await reopen(harness, runId);
  const recording = new DurabilityFaultSink(raw, new Set(["run_waiting"]));
  const fake = fakeRuntime("durability");
  const failed = await resumePipelineV2Run(
    { pipeline, runId, runRoot: harness.runRoot, sink: recording as never, runtime: fake.runtime },
    CONTROL,
  );
  // the rename landed: the adopted candidate is the waiting state, the
  // failure is reported honestly, and no run_failed was written
  expect(failed.ok).toBe(false);
  const failure = failed as Record<string, unknown>;
  expect(failure.reason).toBe("state_persist_failed");
  expect("waiting" in failure).toBe(false);
  const adopted = failure.state as PipelineV2RunState;
  expect(adopted.status).toBe("waiting");
  expect(recording.commands.map((command) => command.kind)).toEqual(["run_waiting"]);
  // the durable state on disk is waiting; a fresh reopen sees it
  const onDisk = await readDurableState(harness);
  expect(onDisk.status).toBe("waiting");
  expect(onDisk.revision).toBe(before + 1);
  const reopened = await reopen(harness, runId);
  expect(reopened.snapshot?.status).toBe("waiting");
  // a fresh resume refuses the waiting run (the operator applies the
  // declared intervention first) — never an automatic response
  const refused = await resumeWith(harness, runId, pipeline);
  expect(refused.result.ok).toBe(false);
  const refusal = refused.result as Record<string, unknown>;
  expect(refusal.refused).toBe(true);
  expect(refusal.reason).toBe("invalid_state");
  expect(refused.recording.commands).toHaveLength(0);
});

test("5. repeated identical entry through the derived policy: zero dispatch, request file identity preserved", async () => {
  const runId = "wait-entry-repeat";
  const harness = await makeHarness("wait-entry-repeat", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  const first = await resumeWith(harness, runId, pipeline);
  expectWaiting(first.result);
  const identity = await requestFileIdentity(harness, 1);
  const revisionBefore = (await readDurableState(harness)).revision;

  // the same derived policy re-enters the open wait through the public
  // controller: zero dispatch, the publication is adopted untouched
  const role = compiledExecutionRoleFor(pipeline, "planner2");
  if (role.role !== "planning") {
    throw new Error("expected the planning role");
  }
  const counting = new RecordingSink(await reopen(harness, runId));
  const entered = await enterPipelineV2Wait({
    runRoot: harness.runRoot,
    sink: counting as never,
    reason: role.stage_wait.reason,
    actions: role.stage_wait.actions.map((id) =>
      id === "continue_stage" ? { id, to: "dev_entry" } : { id, to: "planner2" },
    ),
  });
  expect(entered.wait_index).toBe(1);
  expect(counting.commands).toHaveLength(0);
  expect((await readDurableState(harness)).revision).toBe(revisionBefore);
  const again = await requestFileIdentity(harness, 1);
  expect(again.inode).toBe(identity.inode);
  expect(again.mode).toBe(identity.mode);
  expect(again.mtimeMs).toBe(identity.mtimeMs);
  expect(again.bytes).toBe(identity.bytes);
});

test("6. conflicting policy on the already waiting boundary: typed conflict, zero writes", async () => {
  const runId = "wait-entry-conflict";
  const harness = await makeHarness("wait-entry-conflict", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  await resumeWith(harness, runId, pipeline);
  const before = await fingerprint(harness.runRoot);
  const revisionBefore = (await readDurableState(harness)).revision;

  const counting = new RecordingSink(await reopen(harness, runId));
  let conflict: unknown = null;
  try {
    await enterPipelineV2Wait({
      runRoot: harness.runRoot,
      sink: counting as never,
      reason: "a_different_reason",
      actions: [{ id: "continue_stage", to: "dev_entry" }],
    });
  } catch (cause) {
    conflict = cause;
  }
  expect((conflict as Error).name).toBe("PipelineV2WaitControllerError");
  expect((conflict as { reason: string }).reason).toBe("wait_conflict");
  expect(counting.commands).toHaveLength(0);
  expect((await readDurableState(harness)).revision).toBe(revisionBefore);
  expect(await fingerprint(harness.runRoot)).toBe(before);
});

test("9. action subsets and the reversed full order are preserved verbatim from the trusted policy", async () => {
  const variants: readonly { readonly label: string; readonly policy: StageWaitPolicy; readonly expected: readonly { id: string; to: string }[] }[] = [
    {
      label: "continue-only",
      policy: { reason: "stage_iteration_completed", actions: ["continue_stage"] },
      expected: [{ id: "continue_stage", to: "dev_entry" }],
    },
    {
      label: "revise-only",
      policy: { reason: "stage_iteration_completed", actions: ["revise_task"] },
      expected: [{ id: "revise_task", to: "planner2" }],
    },
    {
      label: "reversed-full",
      policy: { reason: "stage_iteration_completed", actions: ["revise_task", "continue_stage"] },
      expected: [
        { id: "revise_task", to: "planner2" },
        { id: "continue_stage", to: "dev_entry" },
      ],
    },
  ];
  for (const variant of variants) {
    const runId = `wait-entry-order-${variant.label}`;
    const harness = await makeHarness(`wait-entry-order-${variant.label}`, runId, variant.policy);
    const prefix = await prefixStageEntry(harness, runId);
    const pipeline = prefix.pipeline;
    await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
    await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
    const call = await resumeWith(harness, runId, pipeline);
    const state = expectWaiting(call.result);
    expect(call.recording.commands.map((command) => command.kind)).toEqual(["run_waiting"]);
    expect(state.waits[0]?.actions).toEqual(variant.expected);
    const manifest = JSON.parse(await readFile(join(harness.runRoot, "waits", "1.request.json"), "utf8")) as Record<string, unknown>;
    expect(manifest.actions).toEqual(variant.expected);
  }
});

test("10. stage→control keeps the old invalid_graph/run_failed and never enters a wait", async () => {
  const runId = "wait-entry-control";
  const harness = await makeHarness("wait-entry-control", runId, FULL_POLICY, "control");
  const prefix = await prefixStageEntry(harness, runId, "control");
  const pipeline = prefix.pipeline;
  const before = prefix.recording.snapshot as PipelineV2RunState;

  const call = await resumeWith(harness, runId, pipeline);
  expect(call.result.ok).toBe(false);
  const failure = call.result as Record<string, unknown>;
  expect("waiting" in failure).toBe(false);
  expect(failure.reason).toBe("invalid_graph");
  expect(call.recording.commands.map((command) => command.kind)).toEqual([
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
  const state = await readDurableState(harness);
  expect(state.status).toBe("failed");
  expect(state.failure?.reason).toBe("invalid_graph");
  expect(state.revision).toBe(before.revision + 9);
  expect(state.waits).toHaveLength(0);
});

test("11. a terminal edge from a stage state keeps the ordinary success path", async () => {
  const runId = "wait-entry-terminal";
  const harness = await makeHarness("wait-entry-terminal", runId, FULL_POLICY, "terminal");
  const prefix = await prefixStageEntry(harness, runId, "terminal");
  const pipeline = prefix.pipeline;

  const call = await resumeWith(harness, runId, pipeline);
  expect(call.result.ok).toBe(true);
  const state = (call.result as { state: PipelineV2RunState }).state;
  expect(state.status).toBe("success");
  expect(state.phase).toBe("finished");
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  expect(state.waits).toHaveLength(0);
  expect(call.recording.commands.map((command) => command.kind)).toEqual([
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

test("12. a fresh planning run suspends plan-ready before any transition: no stage start, no wait, no run_failed", async () => {
  const runId = "wait-entry-fresh";
  const harness = await makeHarness("wait-entry-fresh", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  const raw = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  const recording = new RecordingSink(raw);
  // the fake worker writes the declared planning output for the architect
  // (the entry state); the completed planning execution suspends at the
  // plan-ready boundary instead of committing the stage-bound transition
  const freshRuntime: PipelineV2AgentRuntime = {
    createExecutionSession: async (_state: unknown, activation: PreparedActivationData) => ({
      sessionId: "fresh-exec-1",
      runAgent: async () => {
        await writeFile(join(activation.outputs_root, "plan"), JSON.stringify(PROPOSAL_R1), { mode: 0o600 });
        return { status: "completed" as const };
      },
      cleanup: async () => {},
    }),
    createToolSession: async () => ({
      sessionId: "fresh-tool-1",
      cleanup: async () => {},
    }),
  } as unknown as PipelineV2AgentRuntime;
  void fakeRuntime;
  const result = await coordinatePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      projectSourcePath: harness.projectSource,
      inputBindings: await bindingsFor(harness, "cycle"),
      sink: recording as never,
      runtime: freshRuntime,
    },
    CONTROL,
  );
  expect(result.ok).toBe(false);
  const ready = result as Record<string, unknown>;
  expect("waiting" in ready).toBe(false);
  expect(ready.planReady).toBe(true);
  const state = ready.state as PipelineV2RunState;
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.executions).toHaveLength(1);
  expect(state.executions[0]?.state_id).toBe("architect");
  expect(state.executions[0]?.execution_role).toBe("planning");
  expect(state.executions[0]?.phase).toBe("cleanup_completed");
  expect(state.transitions).toHaveLength(0);
  expect(state.generations).toHaveLength(0);
  expect(state.waits).toHaveLength(0);
  expect(state.terminal).toBeUndefined();
  expect(state.failure).toBeUndefined();
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
});

test("7+8. historical waits and generations: the current open generation is selected and the stage is bound by generation.stage_id", async () => {
  const runId = "wait-entry-history";
  const harness = await makeHarness("wait-entry-history", runId);
  const prefix = await prefixStageEntry(harness, runId, "cycle", PROPOSAL_R1_TWO_STAGES);
  const pipeline = prefix.pipeline;
  const raw = prefix.raw;
  const live = new RecordingSink(raw);

  // cycle 1: the live wait entry at the stage-1 boundary (generation 1)
  const first = await resumeWith(harness, runId, pipeline, { sink: raw, runtimePrefix: "cycle1" });
  const waiting1 = expectWaiting(first.result);
  expect(waiting1.waits).toHaveLength(1);
  expect(waiting1.generations).toHaveLength(1);
  expect(waiting1.generations[0]?.stage_id).toBe("stage-1");

  // the revise_task intervention on wait 1 revises task-a and replans
  await applyPipelineV2ReviseTaskIntervention({
    pipeline,
    runRoot: harness.runRoot,
    sink: live as never,
    runId,
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "Body A revised",
  });
  // planning execution 3 on planner2 with the pointer-only proposal
  const reopened = await reopen(harness, runId);
  const restoredState = reopened.snapshot as PipelineV2RunState;
  const { restorePipelineV2RuntimeContext } = await import("../src/pipeline_v2_resume_context.ts");
  const restored = await restorePipelineV2RuntimeContext(pipeline, restoredState, harness.runRoot);
  await runPlanningActivation(
    pipeline,
    restored.run_inputs,
    restored.accepted_outputs,
    new RecordingSink(reopened),
    "planner2",
    restored.next_execution_index,
    PROPOSAL_POINTER_TWO_STAGES,
    "plan2",
  );
  // the handoff selects stage-2 of the accepted plan r2: generation 2
  // (stage-2, position 2, the same development template) is opened and the
  // planning transition commits
  await applyPipelineV2PlanningRunPlanHandoff({
    pipeline,
    runRoot: harness.runRoot,
    sink: (await reopen(harness, runId)) as never,
    stageId: "stage-2",
    initialBudget: 2,
  });
  const midState = await readDurableState(harness);
  expect(midState.generations).toHaveLength(2);
  expect(midState.generations[0]?.closed?.by).toBe("replanned");
  expect(midState.generations[1]?.stage_id).toBe("stage-2");
  expect(midState.generations[1]?.stage_position).toBe(2);
  expect(midState.waits).toHaveLength(1);

  // cycle 2: the stage execution of generation 2 runs and the transition
  // into the planning state commits — the recognizer must select the
  // CURRENT open generation 2 (stage-2, position 2), never the historical
  // generation 1 or the historical answered wait 1
  const second = await resumeWith(harness, runId, pipeline, { runtimePrefix: "cycle2" });
  const waiting2 = expectWaiting(second.result);
  expect(second.recording.commands.map((command) => command.kind)).toEqual(LIVE_STAGE_COMMANDS);
  expect(waiting2.waits).toHaveLength(2);
  expect(waiting2.waits[1]?.transition_count).toBe(midState.cursor.transition_count + 1);
  expect(waiting2.waits[1]?.state_id).toBe("planner2");
  expect(waiting2.generations).toHaveLength(2);
  expect(waiting2.generations[1]?.stage_id).toBe("stage-2");
  expect(waiting2.generations[1]?.stage_position).toBe(2);
  const open = pipelineV2OpenStageIteration(waiting2);
  expect(open?.stage_id).toBe("stage-2");
  expect(open?.generation_index).toBe(2);
  // the request manifest of wait 2 derives from generation 2's binding
  const manifest2 = JSON.parse(await readFile(join(harness.runRoot, "waits", "2.request.json"), "utf8")) as Record<string, unknown>;
  expect(manifest2.actions).toEqual([
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "planner2" },
  ]);
  // the historical wait 1 record is untouched (the intervention's intent
  // and response remain exactly as they were durably recorded)
  expect(waiting2.waits[0]).toEqual(midState.waits[0]);
});

test("13/14. signals before the wait boundary keep their semantics; sessions are cleaned exactly once, tool first", async () => {
  const runId = "wait-entry-signal";
  const harness = await makeHarness("wait-entry-signal", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;

  // a signal accepted during the worker run fails the execution durably;
  // no transition commits and no wait is entered
  let delivered = false;
  const runtime = {
    createExecutionSession: async () => ({
      sessionId: "sig-exec-1",
      runAgent: async () => {
        delivered = true;
        return { status: "completed" as const };
      },
      cleanup: async () => {},
    }),
    createToolSession: async () => ({
      sessionId: "sig-tool-1",
      cleanup: async () => {},
    }),
  } as unknown as PipelineV2AgentRuntime;
  const raw = await reopen(harness, runId);
  const recording = new RecordingSink(raw);
  const result = await resumePipelineV2Run(
    {
      pipeline,
      runId,
      runRoot: harness.runRoot,
      sink: recording as never,
      runtime,
    },
    {
      currentSignal: () => (delivered ? "SIGINT" : null),
      freezeSignal: () => {
        delivered = false;
        return "SIGINT";
      },
    },
  );
  expect(result.ok).toBe(false);
  const failure = result as Record<string, unknown>;
  expect("waiting" in failure).toBe(false);
  expect(failure.reason).toBe("signal_sigint");
  const state = await readDurableState(harness);
  expect(state.status).toBe("failed");
  expect(state.failure?.reason).toBe("signal_sigint");
  expect(state.waits).toHaveLength(0);
  expect(recording.commands.map((command) => command.kind)).toEqual([
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
  void prefix;
});

test("15. a tampered request file conflicts before any dispatch and never writes run_failed", async () => {
  const runId = "wait-entry-tampered";
  const harness = await makeHarness("wait-entry-tampered", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  const revisionBefore = (await readDurableState(harness)).revision;

  // a hostile pre-placed request file with different bytes
  const waitsDir = join(harness.runRoot, "waits");
  await mkdir(waitsDir, { mode: 0o700 });
  await writeFile(join(waitsDir, "1.request.json"), '{"schema_version":1,"run_id":"other"}', { mode: 0o600 });

  const call = await resumeWith(harness, runId, pipeline);
  expect(call.result.ok).toBe(false);
  const failure = call.result as Record<string, unknown>;
  expect("waiting" in failure).toBe(false);
  expect(failure.reason).toBe("internal_error");
  expect(call.recording.commands).toHaveLength(0);
  expect((await readDurableState(harness)).revision).toBe(revisionBefore);
  expect((await readDurableState(harness)).failure).toBeUndefined();
  // the tampered file is untouched (never overwritten)
  expect(await readFile(join(waitsDir, "1.request.json"), "utf8")).toBe('{"schema_version":1,"run_id":"other"}');
});

// --- the production runner entrypoints ---------------------------------------
//
// `resumePipelineV2` and `continuePipelineV2Stage` prove the shared
// existing-run outcome mapping (the single `runExistingPipelineV2` core);
// `resumePipelineV2PlanningRunPlan` and `revisePipelineV2Task` return
// through the very same mapping, so the waiting shape is proven for all
// four existing-run entrypoints by these two runners. A fresh `run` can
// never reach the wait boundary (proof 12).

import {
  continuePipelineV2Stage,
  resumePipelineV2,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import type { AuthFetcher, CliRunner } from "../src/docker_helper.ts";
import { preparePlanRevisionManifest, prepareTaskRevisionManifest } from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";

interface RunnerHarness {
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

const RUNNER_PIPELINE = `
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

const EXPECTED_LAUNCHER_ID = "dhl_wait_entry";

async function makeRunnerHarness(prefix: string, runId: string): Promise<RunnerHarness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), RUNNER_PIPELINE);
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
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { mode: 0o700 });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  return { root, stateRoot, bundle, configRoot, sources, projectSource, credentialFile, runRoot, statePath: join(runRoot, "state.json") };
}

let runnerSessionCounter = 0;

function runnerDeps(harness: RunnerHarness): PipelineV2RunnerDeps {
  const cli: CliRunner = async (args) => {
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
    return { code: 0 };
  };
  const fetchAuth: AuthFetcher = async () => ({
    status: 200,
    body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
  });
  return {
    cli,
    fetchAuth,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", CODER_SOURCE_VAR_1: "coder-secret" },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    now: nextTick,
  };
}

/**
 * The honest runner prefix up to the stage-entry cursor: plan r1 accepted,
 * generation 1 / iteration 1 open, the planning transition committed — no
 * stage execution, no wait.
 */
async function runnerPrefixStageEntry(harness: RunnerHarness, runId: string): Promise<void> {
  clockValue = 0;
  const pipeline = await loadPipelineV2(harness.bundle);
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  await prepareRunProject(harness.projectSource, harness.runRoot);
  const runInputs = await snapshotRunInputs(
    pipeline,
    [{ id: "task", path: join(harness.sources, "task.md") }] as readonly RunInputBinding[],
    harness.runRoot,
  );
  await sink.dispatch({
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
  const activation: PreparedActivationData = await prepareActivationData(pipeline, runInputs, [], "architect", 1);
  await sink.dispatch({
    kind: "start_agent_execution",
    stateId: "architect",
    profile: "coder",
    ...startRoleArgs(pipeline, "architect", sink.snapshot),
  });
  await sink.dispatch({ kind: "agent_data_prepared" });
  await sink.dispatch({ kind: "agent_execution_session_created", sessionId: "plan-exec-1" });
  await sink.dispatch({ kind: "agent_tool_session_created", sessionId: "plan-tool-1" });
  await sink.dispatch({ kind: "agent_running" });
  await writeFile(join(activation.outputs_root, "plan"), "{}", { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, activation);
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await sink.dispatch({ kind: "agent_cleanup_completed" });
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
      { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }] },
    ],
  });
  const candidate = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: runInputs.inputs[0]?.digest ?? "",
  });
  const acceptedPlan = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: harness.runRoot, sink, candidate });
  await ensurePipelineV2StageIteration({
    compiledPlan: acceptedPlan.compiled_plan,
    stageId: "stage-1",
    initialBudget: 2,
    sink,
  });
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
}

function expectWaitingOutcome(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  const record = outcome as unknown as Record<string, unknown>;
  expect(record.ok).toBe(false);
  expect(record.waiting).toBe(true);
  expect(record.exitCode).toBe(0);
  expect(Object.keys(record).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state", "waiting"]);
  expect("reason" in record).toBe(false);
  expect("refused" in record).toBe(false);
  const state = record.state as PipelineV2RunState;
  expect(state.status).toBe("waiting");
  return state;
}

test("runner: `orchestrator resume` reaches the stage-wait boundary and reports the waiting outcome (exit 0)", async () => {
  const runId = "wait-entry-runner-resume";
  const harness = await makeRunnerHarness("wait-entry-runner-resume", runId);
  await runnerPrefixStageEntry(harness, runId);
  const before = (await readDurableState2(harness)).revision;

  const outcome = await resumePipelineV2({ runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID }, runnerDeps(harness));
  const state = expectWaitingOutcome(outcome);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot);
  expect(state.revision).toBe(before + 9);
  expect(state.waits).toHaveLength(1);
  expect(state.waits[0]?.actions).toEqual([
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "architect" },
  ]);
  // one session pair, cleaned exactly once each (tool first is the
  // docker runtime's fixed order)
  expect((await readDurableState2(harness)).status).toBe("waiting");
});

test("runner: `orchestrator continue-stage` completes the intervention and the resumed stage execution suspends at the next wait (exit 0)", async () => {
  const runId = "wait-entry-runner-continue";
  const harness = await makeRunnerHarness("wait-entry-runner-continue", runId);
  const pipeline = await loadPipelineV2(harness.bundle);
  await runnerPrefixStageEntry(harness, runId);
  // the first wait cycle: the stage execution ran in a previous process
  // and the transition is durable — the real resume enters the wait
  const first = await resumePipelineV2({ runId, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID }, runnerDeps(harness));
  const waiting1 = expectWaitingOutcome(first);
  expect(waiting1.waits).toHaveLength(1);

  // the operator continues the stage: the full intervention through the
  // dedicated entrypoint, then the successor stage execution runs and
  // suspends at the next stage→planning boundary
  const cliLog: string[] = [];
  const outcome = await continuePipelineV2Stage(
    { runId, waitIndex: 1, additionalIterations: 1, configRoot: harness.configRoot, launcherId: EXPECTED_LAUNCHER_ID },
    runnerDeps(harness),
  );
  const waiting2 = expectWaitingOutcome(outcome);
  expect(waiting2.waits).toHaveLength(2);
  // wait 2 is entered after the second stage→planning transition
  expect(waiting2.waits[1]?.transition_count).toBe(3);
  const durable = await readDurableState2(harness);
  expect(durable.status).toBe("waiting");
  expect(durable.generations[0]?.iterations).toHaveLength(2);
  expect(durable.generations[0]?.iterations[0]?.closed?.by).toBe("grant");
  expect(durable.generations[0]?.open_iteration?.index).toBe(2);
});

async function readDurableState2(harness: RunnerHarness): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
}

test("15b. a missing accepted plan artifact fails the derivation before any dispatch, without run_failed", async () => {
  const runId = "wait-entry-no-plan";
  const harness = await makeHarness("wait-entry-no-plan", runId);
  const prefix = await prefixStageEntry(harness, runId);
  const pipeline = prefix.pipeline;
  await runStageActivation(pipeline, prefix.runInputs, [], prefix.recording, "dev_entry", 2);
  await commitTransition(prefix.recording, "dev_entry", "planner2", 2);
  const revisionBefore = (await readDurableState(harness)).revision;
  const treeBefore = await fingerprint(harness.runRoot);

  // the accepted plan manifest disappears (a damaged run tree)
  await rm(join(harness.runRoot, "run-plan", "plans", "1.json"));

  const call = await resumeWith(harness, runId, pipeline);
  expect(call.result.ok).toBe(false);
  const failure = call.result as Record<string, unknown>;
  expect("waiting" in failure).toBe(false);
  expect(failure.reason).toBe("internal_error");
  // no dispatch at all: the recognizer's derivation failed before the
  // wait entry, the boundary stays retryable, nothing was written
  expect(call.recording.commands).toHaveLength(0);
  expect((await readDurableState(harness)).revision).toBe(revisionBefore);
  expect((await readDurableState(harness)).failure).toBeUndefined();
  // the missing artifact is the only tree change
  const treeAfter = await fingerprint(harness.runRoot);
  expect(treeAfter.split("\n").length).toBe(treeBefore.split("\n").length - 1);
});

test("source restrictions: no new machinery, the sentinel stays private, no waiting failure reason", async () => {
  const coordinator = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_coordinator.ts"), "utf8");
  const runner = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_runner.ts"), "utf8");
  const main = await readFile(join(import.meta.dir, "..", "src", "main.ts"), "utf8");
  const state = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_state.ts"), "utf8");
  const waitController = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_wait_controller.ts"), "utf8");

  // the private suspension sentinel never appears in the export surface
  expect(coordinator).toContain("class CoordinatorSuspension extends Error");
  expect(coordinator).not.toMatch(/export[^]{0,80}CoordinatorSuspension/);
  // the single public wait controller entry; no store import and no
  // manual manifest parsing in the coordinator (the manifest module is
  // imported only for the action type)
  expect(coordinator.match(/from "\.\/pipeline_v2_wait_controller\.ts"/g)).toHaveLength(1);
  expect(coordinator).not.toContain('from "./pipeline_v2_wait_store.ts"');
  expect(coordinator.match(/from "\.\/pipeline_v2_wait_manifest\.ts"/g)).toEqual([
    'from "./pipeline_v2_wait_manifest.ts"',
  ]);
  expect(coordinator).not.toContain("reducePipelineV2RunCommand");
  expect(coordinator).not.toContain("publishPipelineV2WaitRequest");
  expect(coordinator).not.toContain("preparePipelineV2WaitRequest");
  // the two single restores: the compiled plan of the wait derivation and
  // the planning-acceptance context of the plan-ready seam — each exactly
  // one import and one call, no second restore mechanism
  expect(coordinator.match(/restorePipelineV2AcceptedRunPlan/g)).toHaveLength(2);
  expect(coordinator.match(/await restorePipelineV2PlanningAcceptanceContext\(/g)).toHaveLength(1);
  expect(coordinator.match(/import \{ restorePipelineV2PlanningAcceptanceContext \}/g)).toHaveLength(1);
  // no new failure reason: the state vocabulary is untouched
  expect(state).toContain('"terminal_failed",\n] as const;');
  expect(state).not.toContain('"waiting",\n] as const;\nexport type PipelineV2FailureReason');
  // the runner checks waiting before the refusal/failure branches
  const resumeMapping = runner.slice(runner.indexOf('if ("waiting" in result) {', runner.indexOf("runExistingPipelineV2")));
  expect(resumeMapping.indexOf('"refused"')).toBeGreaterThan(0);
  expect(resumeMapping.indexOf('"refused"')).toBeLessThan(resumeMapping.indexOf("signal_sigint"));
  // the CLI reporter checks waiting before the ordinary failure branch
  const reporter = main.slice(main.indexOf("function reportPipelineV2Outcome"));
  expect(reporter.indexOf("outcome.waiting === true")).toBeGreaterThan(-1);
  expect(reporter.indexOf("outcome.waiting === true")).toBeLessThan(reporter.indexOf('outcome.runRoot !== null'));
  // the wait controller module is untouched by this increment
  expect(waitController).not.toContain("stage_wait");
});
