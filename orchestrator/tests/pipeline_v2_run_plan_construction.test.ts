import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  parsePipelineV2RunState,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  readAcceptedJsonOutput,
  snapshotRunInputs,
  type AcceptedStateOutput,
} from "../src/pipeline_v2_runtime.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  loadPipelineV2PlanRevision,
  loadPipelineV2TaskRevision,
  publishPipelineV2PlanRevision,
  publishPipelineV2TaskRevision,
  publishPipelineV2WaitIntent,
} from "../src/pipeline_v2_run_plan_store.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { verifyPipelineV2RunPlanCandidateForAcceptance } from "../src/pipeline_v2_run_plan_acceptance.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest as publishWaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { preparePipelineV2RunPlanProposal } from "../src/pipeline_v2_run_plan_proposal.ts";
import { PipelineV2RunPlanBindingError } from "../src/pipeline_v2_run_plan_bindings.ts";
import { PipelineV2RunPlanManifestError } from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  constructPipelineV2RunPlanCandidateFromProposal,
  PipelineV2RunPlanConstructionError,
  type PipelineV2RunPlanConstructionFailureReason,
} from "../src/pipeline_v2_run_plan_construction.ts";
import {
  constructPipelineV2RunPlanCandidateFromProposalInternal,
  productionRunPlanConstructionOps,
  type PipelineV2RunPlanConstructionOps,
} from "../src/pipeline_v2_run_plan_construction_internal.ts";

const RUN_ID = "construct-run";
const PROTECTED_DIGEST = "a".repeat(64);
const CANARY_BODY = "CANARY_secret_body_value";

const hex = (char: string): string => char.repeat(64);

const CONSTRUCTION_PIPELINE = `
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
        to: architect
  - id: done
    type: terminal
    result: success
`;

/**
 * The two-planning-state topology of the replanning fixture: the second
 * settled planning execution runs on `implement` after the planning
 * transition, so the constructed plan r2 carries it as its origin.
 */
const TWO_PLANNING_PIPELINE = `
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
    - state_id: implement
      role: planning
      plan_output: plan
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
        to: implement
  - id: implement
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

const BASE_INPUTS: readonly PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
];

const PROPOSAL_DOC = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    {
      id: "stage-1",
      template: "development",
      tasks: [
        { id: "task-a", depends_on: [] },
        { id: "task-b", depends_on: ["task-a"] },
      ],
    },
  ],
  new_tasks: [
    { id: "task-a", body: "PLAN-TASK-A-BODY" },
    { id: "task-b", body: "PLAN-TASK-B-BODY" },
  ],
};

// --- harness ----------------------------------------------------------------

interface Dirs {
  root: string;
  bundle: string;
  sources: string;
  projectSource: string;
  runRoot: string;
  stateRoot: string;
}

async function makeDirs(prefix: string, runId: string): Promise<Dirs> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  const runRoot = join(root, "runs", runId);
  await mkdir(runRoot, { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, sources, projectSource, runRoot, stateRoot };
}

async function writeBundle(dirs: Dirs, yaml: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clock));
}
function resetClock(): void {
  clock = 0;
}

class RecordingSink {
  readonly commands: PipelineV2RunCommand[] = [];

  constructor(private readonly inner: PipelineV2RunStateSink) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: PipelineV2RunCommand): Promise<void> {
    this.commands.push({ ...command });
    await this.inner.dispatch(command);
  }
}

interface Harness {
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
  runId: string;
  sink: PipelineV2RunStateSink;
  recording: RecordingSink;
}

async function setupHarness(yaml: string, runId = RUN_ID): Promise<Harness> {
  const dirs = await makeDirs("pipeline-v2-plan-construction-", runId);
  await writeBundle(dirs, yaml);
  await writeFile(join(dirs.sources, "task.txt"), "TASK-BODY\n");
  const pipeline = await loadPipelineV2(dirs.bundle);
  resetClock();
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  return { dirs, pipeline, runId, sink, recording: new RecordingSink(sink) };
}

async function dispatch(harness: Harness, command: PipelineV2RunCommand): Promise<void> {
  await harness.recording.dispatch(command);
}

/** Reopens the durable run through the real loader/store path. */
async function reopen(harness: Harness): Promise<RecordingSink> {
  const reopened = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: harness.runId,
    now: nextTick,
  });
  return new RecordingSink(reopened);
}

// --- durable execution fixtures ---------------------------------------------

function executionPhases(sessionIndex: number): PipelineV2RunCommand[] {
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${sessionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${sessionIndex}` },
    { kind: "agent_running" },
  ];
}

let sessionCounter = 0;

/** One settled-but-unbound planning execution on `architect`. */
async function playPlanningExecution(recording: RecordingSink): Promise<number> {
  sessionCounter += 1;
  const state = recording.snapshot as PipelineV2RunState;
  const index = state.executions.length + 1;
  await recording.dispatch({
    kind: "start_agent_execution",
    stateId: "architect",
    profile: "architect",
    executionRole: "planning",
  });
  for (const command of executionPhases(sessionCounter)) {
    await recording.dispatch(command);
  }
  await recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: [{ id: "plan", digest: hex("d") }],
  });
  await recording.dispatch({ kind: "agent_cleanup_completed" });
  return index;
}

// --- prepared-manifest fixtures ---------------------------------------------

function preparedTask(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: "planning_proposal" | "user_response",
  body: string,
  runId = RUN_ID,
): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision,
    previous_sha256: previousSha256,
    origin,
    body,
  });
}

function stageSpec(
  id: string,
  tasks: ReadonlyArray<{ id: string; revision: number; sha256: string; depends_on?: readonly string[] }>,
): { id: string; template: string; tasks: { id: string; revision: number; sha256: string; depends_on: readonly string[] }[] } {
  return {
    id,
    template: "development",
    tasks: tasks.map((task) => ({
      id: task.id,
      revision: task.revision,
      sha256: task.sha256,
      depends_on: task.depends_on === undefined ? [] : [...task.depends_on],
    })),
  };
}

const taskA1Prepared = (): PreparedPipelineV2RunTaskRevision =>
  preparedTask("task-a", 1, null, "planning_proposal", "Body A one");

function planValue(options: {
  revision: number;
  previousSha256: string | null;
  rootTaskInputId: string;
  rootTaskSha256: string;
  originExecution: number;
  pointerTasks: ReadonlyArray<{ id: string; revision: number; sha256: string; depends_on?: readonly string[] }>;
  runId?: string;
}): PreparedPipelineV2RunPlanRevision {
  return preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: options.runId ?? RUN_ID,
    revision: options.revision,
    previous_sha256: options.previousSha256,
    root_task: { input_id: options.rootTaskInputId, sha256: options.rootTaskSha256 },
    origin_execution: options.originExecution,
    stages: [stageSpec("stage-1", options.pointerTasks)],
  });
}

// --- fixture: plan r1 accepted honestly (empty ledger before it) -------------

interface RevisionOneFixture {
  harness: Harness;
  plan1: PreparedPipelineV2RunPlanRevision;
  taskA1: PreparedPipelineV2RunTaskRevision;
}

async function setupRevisionOne(): Promise<RevisionOneFixture> {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  await dispatch(harness, {
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: BASE_INPUTS.map((input) => ({ ...input })),
  });
  await playPlanningExecution(harness.recording);
  const taskA1 = taskA1Prepared();
  const plan1 = planValue({
    revision: 1,
    previousSha256: null,
    rootTaskInputId: "task",
    rootTaskSha256: PROTECTED_DIGEST,
    originExecution: 1,
    pointerTasks: [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }],
  });
  const candidate1 = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    sink: harness.recording,
    candidate: candidate1,
  });
  return { harness, plan1, taskA1 };
}

// --- fixture: the real revise boundary (task-a@2 inside the open wait) -------

interface ReviseBoundaryFixture extends RevisionOneFixture {
  taskA2: PreparedPipelineV2RunTaskRevision;
  intent: ReturnType<typeof prepareWaitIntent>;
}

async function setupReviseBoundary(): Promise<ReviseBoundaryFixture> {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  await dispatch(harness, {
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: BASE_INPUTS.map((input) => ({ ...input })),
  });
  await playPlanningExecution(harness.recording);
  const taskA1 = taskA1Prepared();
  const plan1 = planValue({
    revision: 1,
    previousSha256: null,
    rootTaskInputId: "task",
    rootTaskSha256: PROTECTED_DIGEST,
    originExecution: 1,
    pointerTasks: [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }],
  });
  const candidate1 = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    sink: harness.recording,
    candidate: candidate1,
  });
  await dispatch(harness, {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  const actions = [
    { id: "continue_stage", to: "dev_entry" },
    { id: "revise_task", to: "architect" },
  ];
  const request = preparePipelineV2WaitRequest({
    schema_version: 1,
    run_id: RUN_ID,
    wait_index: 1,
    transition_count: 1,
    state_id: "dev_entry",
    reason: "stage_iteration_limit_exhausted",
    actions,
  });
  await dispatch(harness, {
    kind: "run_waiting",
    stateId: "dev_entry",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: request.sha256,
    actions,
  });
  await publishWaitRequest(harness.dirs.runRoot, request.manifest);
  const taskA2 = preparedTask("task-a", 2, taskA1.sha256, "user_response", "Body A two");
  const intent = prepareWaitIntent({
    schema_version: 1,
    kind: "revise_task_intent",
    run_id: RUN_ID,
    wait_index: 1,
    task_id: "task-a",
    expected_previous_task_sha256: taskA1.sha256,
    new_task_revision_sha256: taskA2.sha256,
  });
  await publishPipelineV2WaitIntent(harness.dirs.runRoot, intent.manifest);
  await dispatch(harness, { kind: "plan_intent_accepted", waitIndex: 1, intentSha256: intent.sha256 });
  await publishPipelineV2TaskRevision(harness.dirs.runRoot, taskA2.manifest);
  await dispatch(harness, {
    kind: "task_revision_accepted",
    taskId: "task-a",
    revision: 2,
    taskSha256: taskA2.sha256,
    waitIndex: 1,
    intentSha256: intent.sha256,
  });
  return { harness, plan1, taskA1, taskA2, intent };
}

// --- fixture: the honest runtime prefix (real accepted proposal output) ------

interface RuntimeProposalFixture {
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
  recording: RecordingSink;
  protectedDigest: string;
  records: Awaited<ReturnType<typeof acceptActivationOutputs>>;
}

async function setupRuntimeProposal(): Promise<RuntimeProposalFixture> {
  const dirs = await makeDirs("pipeline-v2-plan-construction-runtime-", RUN_ID);
  await writeBundle(dirs, CONSTRUCTION_PIPELINE);
  await writeFile(join(dirs.sources, "task.txt"), "TASK-BODY\n");
  const pipeline = await loadPipelineV2(dirs.bundle);
  resetClock();
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId: RUN_ID, now: nextTick });
  const recording = new RecordingSink(sink);
  await prepareRunProject(dirs.projectSource, dirs.runRoot);
  const runInputs = await snapshotRunInputs(
    pipeline,
    [{ id: "task", path: join(dirs.sources, "task.txt") }],
    dirs.runRoot,
  );
  const protectedInput = runInputs.inputs[0];
  if (protectedInput === undefined) {
    throw new Error("the runtime fixture lost its protected run input");
  }
  await recording.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  const accepted: AcceptedStateOutput[] = [];
  const prep = await prepareActivationData(pipeline, runInputs, accepted, "architect", 1);
  await recording.dispatch({
    kind: "start_agent_execution",
    stateId: "architect",
    profile: "architect",
    executionRole: "planning",
  });
  for (const command of executionPhases(1)) {
    await recording.dispatch(command);
  }
  await writeFile(join(prep.outputs_root, "plan"), JSON.stringify(PROPOSAL_DOC), { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, prep);
  accepted.push(...records);
  await recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await recording.dispatch({ kind: "agent_cleanup_completed" });
  return { dirs, pipeline, recording, protectedDigest: protectedInput.digest, records };
}

// --- fixture: plan r1 accepted, restart, second settled planning execution ---

interface ReplanningFixture extends RevisionOneFixture {
  state: PipelineV2RunState;
}

async function setupReplanning(): Promise<ReplanningFixture> {
  const harness = await setupHarness(TWO_PLANNING_PIPELINE);
  await dispatch(harness, {
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: BASE_INPUTS.map((input) => ({ ...input })),
  });
  await playPlanningExecution(harness.recording);
  const taskA1 = taskA1Prepared();
  const plan1 = planValue({
    revision: 1,
    previousSha256: null,
    rootTaskInputId: "task",
    rootTaskSha256: PROTECTED_DIGEST,
    originExecution: 1,
    pointerTasks: [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }],
  });
  const candidate1 = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    sink: harness.recording,
    candidate: candidate1,
  });
  // the simulated process restart
  const reopened = await reopen(harness);
  await reopened.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "implement", transition_index: 0 },
    executionIndex: 1,
  });
  sessionCounter += 1;
  await reopened.dispatch({
    kind: "start_agent_execution",
    stateId: "implement",
    profile: "architect",
    executionRole: "planning",
  });
  for (const command of executionPhases(sessionCounter)) {
    await reopened.dispatch(command);
  }
  await reopened.dispatch({
    kind: "agent_outputs_accepted",
    outputs: [{ id: "plan", digest: hex("e") }],
  });
  await reopened.dispatch({ kind: "agent_cleanup_completed" });
  return { harness, plan1, taskA1, state: reopened.snapshot as PipelineV2RunState };
}

// --- construction helpers ----------------------------------------------------

const { loadPlanRevision: loadPlanRevisionReal, loadTaskRevision: loadTaskRevisionReal } =
  productionRunPlanConstructionOps;

interface RecordedOps {
  ops: PipelineV2RunPlanConstructionOps;
  calls: string[];
}

/** Wraps the real production ops with an exact ordered load log. */
function recordingOps(): RecordedOps {
  const calls: string[] = [];
  const ops: PipelineV2RunPlanConstructionOps = {
    loadPlanRevision: async (runRoot, revision) => {
      calls.push(`plan:${revision}`);
      return await loadPlanRevisionReal(runRoot, revision);
    },
    loadTaskRevision: async (runRoot, taskId, revision) => {
      calls.push(`task:${taskId}:${revision}`);
      return await loadTaskRevisionReal(runRoot, taskId, revision);
    },
  };
  return { ops, calls };
}

function expectConstructionError(
  cause: unknown,
  reason: PipelineV2RunPlanConstructionFailureReason,
): PipelineV2RunPlanConstructionError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanConstructionError);
  const error = cause as PipelineV2RunPlanConstructionError;
  expect(error.reason).toBe(reason);
  return error;
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, rel: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    entries.sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
    for (const entry of entries) {
      const childRel = rel === "" ? entry.name : `${rel}/${entry.name}`;
      const childPath = join(dir, entry.name);
      const info = await lstat(childPath);
      if (info.isSymbolicLink()) {
        lines.push(`${childRel} symlink ${await readlink(childPath)}`);
      } else if (info.isDirectory()) {
        lines.push(`${childRel} dir ${(info.mode & 0o7777).toString(8)} ${info.ino}`);
        await walk(childPath, childRel);
      } else if (info.isFile()) {
        lines.push(
          `${childRel} file ${(info.mode & 0o7777).toString(8)} ${info.ino} ${(await readFile(childPath)).toString("base64")}`,
        );
      } else {
        lines.push(`${childRel} other`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

// --- 1. the initial plan through the real accepted planning output -----------

test("1. the real accepted planning JSON output becomes the exact candidate r1 with two new tasks and nothing is published", async () => {
  const fixture = await setupRuntimeProposal();
  const { dirs, pipeline, recording, protectedDigest, records } = fixture;
  try {
    // the honest reader → proposal chain over the real accepted output
    const read = await readAcceptedJsonOutput(pipeline, dirs.runRoot, records, "architect", "plan", 1);
    const proposal = preparePipelineV2RunPlanProposal(read.value);
    expect(proposal.new_tasks.map((entry) => entry.id)).toEqual(["task-a", "task-b"]);

    const before = await fingerprint(dirs.runRoot);
    const { ops, calls } = recordingOps();
    const candidate = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state: recording.snapshot, proposal },
      ops,
    );

    // zero store loads: an empty plan ledger and only new task pointers
    expect(calls).toEqual([]);
    // construction publishes nothing: the run tree is byte-identical and
    // carries no run-plan store at all
    expect(await fingerprint(dirs.runRoot)).toBe(before);
    await expect(lstat(join(dirs.runRoot, "run-plan"))).rejects.toThrow(/ENOENT/);

    // the exact candidate, built here through the same public chain
    const expectedTaskA = preparedTask("task-a", 1, null, "planning_proposal", "PLAN-TASK-A-BODY");
    const expectedTaskB = preparedTask("task-b", 1, null, "planning_proposal", "PLAN-TASK-B-BODY");
    const expectedPlan = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: protectedDigest },
      origin_execution: 1,
      stages: [
        stageSpec("stage-1", [
          { id: "task-a", revision: 1, sha256: expectedTaskA.sha256 },
          { id: "task-b", revision: 1, sha256: expectedTaskB.sha256, depends_on: ["task-a"] },
        ]),
      ],
    });
    const expectedCandidate = preparePipelineV2RunPlanCandidate({
      plan: expectedPlan,
      taskRevisions: [expectedTaskA, expectedTaskB],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: protectedDigest,
    });
    expect(candidate).toEqual(expectedCandidate);
    expect(candidate.plan.manifest.revision).toBe(1);
    expect(candidate.plan.manifest.previous_sha256).toBeNull();
    expect(candidate.plan.manifest.origin_execution).toBe(1);
    expect(candidate.plan.manifest.root_task).toEqual({ input_id: "task", sha256: protectedDigest });
    expect(
      candidate.plan.manifest.stages[0]?.tasks.map((pointer) => ({
        id: pointer.id,
        revision: pointer.revision,
        sha256: pointer.sha256,
        depends_on: [...pointer.depends_on],
      })),
    ).toEqual([
      { id: "task-a", revision: 1, sha256: expectedTaskA.sha256, depends_on: [] },
      { id: "task-b", revision: 1, sha256: expectedTaskB.sha256, depends_on: ["task-a"] },
    ]);
    expect(candidate.task_revisions.map((task) => task.manifest.task_id)).toEqual(["task-a", "task-b"]);
    for (const task of candidate.task_revisions) {
      expect(task.manifest.revision).toBe(1);
      expect(task.manifest.previous_sha256).toBeNull();
      expect(task.manifest.origin).toBe("planning_proposal");
      expect(task.manifest.run_id).toBe(RUN_ID);
    }
    expect(candidate.task_revisions[0]?.manifest.body).toBe("PLAN-TASK-A-BODY");
    expect(candidate.task_revisions[1]?.manifest.body).toBe("PLAN-TASK-B-BODY");

    // the constructed candidate is provenance-backed and acceptance-ready
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(
      pipeline,
      recording.snapshot as PipelineV2RunState,
      candidate,
    );
    expect(compiled.plan_revision).toBe(1);
    expect(compiled.origin_execution).toBe(1);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 2. replanning across the restart ---------------------------------------

test("2. after the restart an existing task r1 and a new task-c build the exact plan r2 candidate", async () => {
  const fixture = await setupReplanning();
  const { harness, state, plan1, taskA1 } = fixture;
  const { dirs } = harness;
  try {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", depends_on: [] },
            { id: "task-c", depends_on: ["task-a"] },
          ],
        },
      ],
      new_tasks: [{ id: "task-c", body: "Body C" }],
    });
    const before = await fingerprint(dirs.runRoot);
    const { ops, calls } = recordingOps();
    const candidate = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state, proposal },
      ops,
    );

    // the exact sequential load order: the previous plan, then the
    // pointered existing task; the new task reads nothing
    expect(calls).toEqual(["plan:1", "task:task-a:1"]);
    // construction publishes nothing
    expect(await fingerprint(dirs.runRoot)).toBe(before);

    expect(candidate.plan.manifest.revision).toBe(2);
    expect(candidate.plan.manifest.previous_sha256).toBe(plan1.sha256);
    expect(candidate.plan.manifest.origin_execution).toBe(2);
    expect(candidate.plan.manifest.root_task).toEqual({ input_id: "task", sha256: PROTECTED_DIGEST });
    const taskC = candidate.task_revisions[1];
    if (taskC === undefined) {
      throw new Error("the constructed candidate lost its new task revision");
    }
    expect(
      candidate.plan.manifest.stages[0]?.tasks.map((pointer) => ({
        id: pointer.id,
        revision: pointer.revision,
        sha256: pointer.sha256,
      })),
    ).toEqual([
      { id: "task-a", revision: 1, sha256: taskA1.sha256 },
      { id: "task-c", revision: 1, sha256: taskC.sha256 },
    ]);
    // the loaded prepared task is opaque and equals the fixture's prepared
    // revision; the new task body comes only from the proposal
    expect(candidate.task_revisions[0]).toEqual(taskA1);
    expect(taskC.manifest).toMatchObject({
      task_id: "task-c",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "Body C",
      run_id: RUN_ID,
    });
    expect(candidate.plan.manifest.stages[0]?.tasks[1]?.sha256).toBe(taskC.sha256);

    // the constructed candidate is provenance-backed and acceptance-ready
    // at the second settled planning execution boundary
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(harness.pipeline, state, candidate);
    expect(compiled.plan_revision).toBe(2);
    expect(compiled.origin_execution).toBe(2);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 3. the real revise boundary --------------------------------------------

test("3. at the real revise boundary the proposal pointer selects the latest durable task-a@2, loads it and its predecessor, and the new plan r2 points task-a@2", async () => {
  const fixture = await setupReviseBoundary();
  const { harness, plan1, taskA1, taskA2 } = fixture;
  const { dirs } = harness;
  try {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", depends_on: [] }],
        },
      ],
      new_tasks: [],
    });
    const before = await fingerprint(dirs.runRoot);
    const { ops, calls } = recordingOps();
    const candidate = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state: harness.recording.snapshot, proposal },
      ops,
    );

    // the exact load order: the previous plan, the ledger-selected current
    // revision r2, then its immediate durable predecessor r1 — the newer
    // pointer of the accepted plan r1 never wins
    expect(calls).toEqual(["plan:1", "task:task-a:2", "task:task-a:1"]);
    expect(await fingerprint(dirs.runRoot)).toBe(before);

    expect(candidate.plan.manifest.revision).toBe(2);
    expect(candidate.plan.manifest.previous_sha256).toBe(plan1.sha256);
    expect(candidate.plan.manifest.origin_execution).toBe(1);
    const pointer = candidate.plan.manifest.stages[0]?.tasks[0];
    expect(pointer).toMatchObject({ id: "task-a", revision: 2, sha256: taskA2.sha256 });
    // the loaded prepared objects are the exact durable revisions
    expect(candidate.task_revisions[0]).toEqual(taskA2);
    expect(candidate.plan.manifest.stages[0]?.tasks[0]?.sha256).toBe(candidate.task_revisions[0]?.sha256);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 4. the ledger ∩ new_tasks policy rule ----------------------------------

test("4. a new task id already recorded in the durable task ledger is a construction conflict with zero loads", async () => {
  const fixture = await setupReviseBoundary();
  const { harness } = fixture;
  const { dirs } = harness;
  try {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", depends_on: [] }],
        },
      ],
      new_tasks: [{ id: "task-a", body: "REWRITE-ATTEMPT" }],
    });
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state: harness.recording.snapshot, proposal },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "construction_conflict");
    expect((error as Error).message).toContain("already records");
    expect(error.state).not.toBeNull();
    expect(calls).toEqual([]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 5. the unused new_tasks policy rule ------------------------------------

test("5. an unreferenced new task entry is a construction conflict with zero loads", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", depends_on: [] }],
        },
      ],
      new_tasks: [
        { id: "task-a", body: "Body A" },
        { id: "task-unused", body: "SILENTLY-DROPPED-BODY" },
      ],
    });
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state: harness.recording.snapshot, proposal },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "construction_conflict");
    expect((error as Error).message).toContain("no plan stage references");
    expect((error as Error).message).not.toContain("SILENTLY-DROPPED-BODY");
    expect(calls).toEqual([]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 6. the unresolvable pointer policy rule --------------------------------

test("6. a pointer absent from both the ledger and new_tasks is a construction conflict with zero loads", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-ghost", depends_on: [] }],
        },
      ],
      new_tasks: [],
    });
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state: harness.recording.snapshot, proposal },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "construction_conflict");
    expect((error as Error).message).toContain("neither the durable task ledger nor a proposed new task");
    expect(calls).toEqual([]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 7. the manifest validator keeps its graph semantics --------------------

test("7. duplicate pointers, unknown/cross-stage dependencies, cycles and empty stages/tasks keep the existing manifest error by identity", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const state = harness.recording.snapshot as PipelineV2RunState;

    const build = (stages: unknown[], newTasks: unknown[]): unknown => ({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages,
      new_tasks: newTasks,
    });

    const cases: ReadonlyArray<{ name: string; proposal: unknown }> = [
      {
        name: "duplicate task pointers",
        proposal: build(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", depends_on: [] },
                { id: "task-a", depends_on: [] },
              ],
            },
          ],
          [{ id: "task-a", body: "Body A" }],
        ),
      },
      {
        name: "unknown dependency",
        proposal: build(
          [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: ["task-unknown"] }] }],
          [{ id: "task-a", body: "Body A" }],
        ),
      },
      {
        name: "cross-stage dependency",
        proposal: build(
          [
            { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
            { id: "stage-2", template: "development", tasks: [{ id: "task-b", depends_on: ["task-a"] }] },
          ],
          [
            { id: "task-a", body: "Body A" },
            { id: "task-b", body: "Body B" },
          ],
        ),
      },
      {
        name: "dependency cycle",
        proposal: build(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", depends_on: ["task-c"] },
                { id: "task-b", depends_on: ["task-a"] },
                { id: "task-c", depends_on: ["task-b"] },
              ],
            },
          ],
          [
            { id: "task-a", body: "Body A" },
            { id: "task-b", body: "Body B" },
            { id: "task-c", body: "Body C" },
          ],
        ),
      },
      { name: "empty stages", proposal: build([], []) },
      {
        name: "empty stage tasks",
        proposal: build([{ id: "stage-1", template: "development", tasks: [] }], []),
      },
    ];

    for (const item of cases) {
      const proposal = preparePipelineV2RunPlanProposal(item.proposal);
      const { ops, calls } = recordingOps();
      const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
        { runRoot: dirs.runRoot, state, proposal },
        ops,
      ).catch((error) => error);
      expect(cause, item.name).toBeInstanceOf(PipelineV2RunPlanManifestError);
      expect(cause, item.name).not.toBeInstanceOf(PipelineV2RunPlanConstructionError);
      expect(calls, item.name).toEqual([]);
    }
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 8. the missing-artifact matrix -----------------------------------------

test("8. missing current plan, current task and task predecessor manifests are typed artifact_missing", async () => {
  // (a) the current plan manifest of the accepted ledger revision
  const revise = await setupReviseBoundary();
  try {
    await rm(join(revise.harness.dirs.runRoot, "run-plan", "plans", "1.json"));
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      {
        runRoot: revise.harness.dirs.runRoot,
        state: revise.harness.recording.snapshot,
        proposal: preparePipelineV2RunPlanProposal({
          schema_version: 1,
          kind: "run_plan_proposal",
          stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
          new_tasks: [],
        }),
      },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "artifact_missing");
    expect((error as Error).message).toContain("plan revision 1 manifest is missing");
    expect(calls).toEqual(["plan:1"]);
  } finally {
    await rm(revise.harness.dirs.root, { recursive: true, force: true });
  }

  // (b) the current task manifest of a pointered existing task
  const replanning = await setupReplanning();
  try {
    await rm(join(replanning.harness.dirs.runRoot, "run-plan", "tasks", "task-a", "1.json"));
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      {
        runRoot: replanning.harness.dirs.runRoot,
        state: replanning.state,
        proposal: preparePipelineV2RunPlanProposal({
          schema_version: 1,
          kind: "run_plan_proposal",
          stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
          new_tasks: [],
        }),
      },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "artifact_missing");
    expect((error as Error).message).toContain("task revision 1 manifest of task \"task-a\" is missing");
    expect(calls).toEqual(["plan:1", "task:task-a:1"]);
  } finally {
    await rm(replanning.harness.dirs.root, { recursive: true, force: true });
  }

  // (c) the immediate durable predecessor of a revised task
  const boundary = await setupReviseBoundary();
  try {
    await rm(join(boundary.harness.dirs.runRoot, "run-plan", "tasks", "task-a", "1.json"));
    const { ops, calls } = recordingOps();
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      {
        runRoot: boundary.harness.dirs.runRoot,
        state: boundary.harness.recording.snapshot,
        proposal: preparePipelineV2RunPlanProposal({
          schema_version: 1,
          kind: "run_plan_proposal",
          stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
          new_tasks: [],
        }),
      },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "artifact_missing");
    expect((error as Error).message).toContain("predecessor task revision 1 manifest");
    expect(calls).toEqual(["plan:1", "task:task-a:2", "task:task-a:1"]);
  } finally {
    await rm(boundary.harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 9. the malformed loader-result matrix -----------------------------------

test("9. hostile loader results are typed construction failures, never TypeErrors, and never echo the hostile value", async () => {
  const fixture = await setupReviseBoundary();
  const { harness } = fixture;
  const { dirs } = harness;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
      new_tasks: [],
    });
    const runWith = async (overrides: Partial<PipelineV2RunPlanConstructionOps>): Promise<unknown> =>
      await constructPipelineV2RunPlanCandidateFromProposalInternal(
        { runRoot: dirs.runRoot, state, proposal },
        { ...productionRunPlanConstructionOps, ...overrides },
      ).catch((error) => error);

    // null where the previous plan artifact is expected
    const nullPlan = expectConstructionError(
      await runWith({ loadPlanRevision: async () => null }),
      "artifact_missing",
    );
    expect((nullPlan as Error).message).toContain("plan revision 1 manifest is missing");

    // a primitive where the wrapper is expected
    const numberPlan = expectConstructionError(
      await runWith({ loadPlanRevision: async () => 42 }),
      "malformed_loader_result",
    );
    expect((numberPlan as Error).message).toContain("not a published manifest wrapper");

    // an array wrapper
    expectConstructionError(
      await runWith({ loadPlanRevision: async () => [] }),
      "malformed_loader_result",
    );

    // a wrapper without the prepared manifest
    const emptyPlan = expectConstructionError(
      await runWith({ loadPlanRevision: async () => ({}) }),
      "malformed_loader_result",
    );
    expect((emptyPlan as Error).message).toContain("carries no plan prepared manifest");

    // a prepared manifest without the manifest record
    const noManifest = expectConstructionError(
      await runWith({ loadPlanRevision: async () => ({ plan: {} }) }),
      "malformed_loader_result",
    );
    expect((noManifest as Error).message).toContain("carries no manifest record");

    // a prepared manifest of the wrong kind
    const wrongKind = expectConstructionError(
      await runWith({ loadPlanRevision: async () => ({ plan: { manifest: { kind: "task_revision" } } }) }),
      "malformed_loader_result",
    );
    expect((wrongKind as Error).message).toContain("different manifest kind");

    // a malformed task loader result
    const malformedTask = expectConstructionError(
      await runWith({ loadTaskRevision: async () => ({ task: { manifest: { kind: "plan_revision" } } }) }),
      "malformed_loader_result",
    );
    expect((malformedTask as Error).message).toContain("different manifest kind");

    // a shape-OK fake projection passes the wrapper form and is rejected by
    // the existing candidate provenance gate — the deep verification is
    // delegated, never duplicated
    const fakeProjection = await runWith({
      loadPlanRevision: async () => ({ plan: { manifest: { kind: "plan_revision" }, sha256: hex("f") } }),
    });
    expect(fakeProjection).toBeInstanceOf(PipelineV2RunPlanBindingError);
    expect(fakeProjection).not.toBeInstanceOf(PipelineV2RunPlanConstructionError);

    // an unexpected loader error keeps its identity
    const loaderFailure = new Error("LOADER-EXPLODED");
    const loaderCause = await runWith({
      loadPlanRevision: async () => {
        throw loaderFailure;
      },
    });
    expect(loaderCause).toBe(loaderFailure);

    // no hostile value in any diagnostic
    for (const cause of [numberPlan, emptyPlan, noManifest, wrongKind, malformedTask]) {
      expect(String((cause as Error).message)).not.toContain("42");
      expect(String((cause as Error).message)).not.toContain(CANARY_BODY);
      expect(String((cause as Error).message)).not.toContain("HOSTILE");
    }
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 10. the ledger, never the filesystem, selects the revision --------------

test("10. newer orphan plan and task artifacts on disk are ignored in favor of the durable ledger with the exact load order", async () => {
  const fixture = await setupReplanning();
  const { harness, state, plan1, taskA1 } = fixture;
  const { dirs } = harness;
  try {
    // newer orphan artifacts the durable ledger never accepted
    const orphanPlan = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 1,
      stages: [stageSpec("stage-1", [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }])],
    });
    await publishPipelineV2PlanRevision(dirs.runRoot, orphanPlan.manifest);
    const orphanTask = preparedTask("task-a", 2, taskA1.sha256, "user_response", "ORPHAN-TASK-BODY");
    await publishPipelineV2TaskRevision(dirs.runRoot, orphanTask.manifest);

    const { ops, calls } = recordingOps();
    const candidate = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      {
        runRoot: dirs.runRoot,
        state,
        proposal: preparePipelineV2RunPlanProposal({
          schema_version: 1,
          kind: "run_plan_proposal",
          stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
          new_tasks: [],
        }),
      },
      ops,
    );

    // only the ledger-named revisions were read; the orphans were never
    // loaded and never selected the revision
    expect(calls).toEqual(["plan:1", "task:task-a:1"]);
    expect(candidate.plan.manifest.revision).toBe(2);
    expect(candidate.plan.manifest.previous_sha256).toBe(plan1.sha256);
    expect(candidate.plan.manifest.stages[0]?.tasks[0]).toMatchObject({
      id: "task-a",
      revision: 1,
      sha256: taskA1.sha256,
    });
    expect(candidate.task_revisions[0]?.manifest.body).toBe("Body A one");
    expect(JSON.stringify(candidate)).not.toContain("ORPHAN-TASK-BODY");
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 11. capture, provenance and mutation batteries --------------------------

test("11a. the options fields are read exactly once in the fixed order and hostile extras are never read", async () => {
  const fixture = await setupReplanning();
  const { harness, state } = fixture;
  const { dirs } = harness;
  try {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", depends_on: [] },
            { id: "task-c", depends_on: ["task-a"] },
          ],
        },
      ],
      new_tasks: [{ id: "task-c", body: "Body C" }],
    });
    const reads: string[] = [];
    const optionsProxy = new Proxy(
      {
        runRoot: dirs.runRoot,
        state,
        proposal,
        hostile: "HOSTILE-OPTIONS-FIELD",
      } as unknown as Record<string, unknown>,
      {
        get(target, property) {
          reads.push(String(property));
          return target[property as string];
        },
      },
    );
    const { ops } = recordingOps();
    const candidate = await constructPipelineV2RunPlanCandidateFromProposalInternal(optionsProxy, ops);
    expect(candidate.plan.manifest.revision).toBe(2);
    expect(reads).toEqual(["runRoot", "state", "proposal"]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

test("11b. a Proxy or cloned proposal is rejected through the existing provenance registry with zero traps and zero reads", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const state = harness.recording.snapshot as PipelineV2RunState;
    const { ops, calls } = recordingOps();

    const handBuilt = {
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
      new_tasks: [{ id: "task-a", body: "Body A" }],
    };
    let traps = 0;
    let reads = 0;
    const proposalProxy = new Proxy(handBuilt as unknown as Record<string, unknown>, {
      get(target, property) {
        traps += 1;
        reads += 1;
        return target[property as string];
      },
    });
    const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state, proposal: proposalProxy as never },
      ops,
    ).catch((error) => error);
    const error = expectConstructionError(cause, "invalid_options");
    expect((error as Error).message).toContain("prepared run plan proposal");
    expect(traps).toBe(0);
    expect(reads).toBe(0);
    expect(calls).toEqual([]);

    // a structural clone is equally rejected by the identity-bound registry
    const clone = JSON.parse(JSON.stringify(handBuilt));
    let cloneReads = 0;
    const cloneProxy = new Proxy(clone as Record<string, unknown>, {
      get(target, property) {
        cloneReads += 1;
        return target[property as string];
      },
    });
    const cloneCause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: dirs.runRoot, state, proposal: cloneProxy as never },
      recordingOps().ops,
    ).catch((err) => err);
    expectConstructionError(cloneCause, "invalid_options");
    expect(cloneReads).toBe(0);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

test("11c. mutating the caller's state and options after the pending load cannot change the construction", async () => {
  const fixture = await setupReplanning();
  const { harness, state, plan1 } = fixture;
  const { dirs } = harness;
  try {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
      new_tasks: [],
    });
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ops: PipelineV2RunPlanConstructionOps = {
      ...productionRunPlanConstructionOps,
      loadPlanRevision: async (runRoot, revision) => {
        await gate;
        return await productionRunPlanConstructionOps.loadPlanRevision(runRoot, revision);
      },
    };
    // a plain mutable state document (a faithful copy of the durable one)
    const mutableState = JSON.parse(JSON.stringify(state)) as Record<string, unknown>;
    expect(() => validatePipelineV2RunState(mutableState)).not.toThrow();
    const options: { runRoot: string; state: unknown; proposal: unknown } = {
      runRoot: dirs.runRoot,
      state: mutableState,
      proposal,
    };
    const pending = constructPipelineV2RunPlanCandidateFromProposalInternal(options, ops);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    // mutate the caller's state document and options while the load is
    // pending: the validated snapshot was taken before any await
    const ledger = mutableState["plan_revisions"] as Array<Record<string, unknown>>;
    ledger[0]!["sha256"] = hex("9");
    (mutableState["task_revisions"] as unknown[]).push({
      index: 2,
      task_id: "task-forge",
      revision: 1,
      sha256: hex("b"),
      previous_sha256: null,
    });
    options.state = null;
    options.proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [],
      new_tasks: [],
    });
    release?.();
    const candidate = await pending;
    expect(candidate.plan.manifest.revision).toBe(2);
    expect(candidate.plan.manifest.previous_sha256).toBe(plan1.sha256);
    expect(candidate.task_revisions.map((task) => task.manifest.task_id)).toEqual(["task-a"]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

test("11d. malformed options and ops are typed invalid_options at the capture boundary, before any state read or store call", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const state = harness.recording.snapshot as PipelineV2RunState;
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
      new_tasks: [],
    });

    const cases: ReadonlyArray<{ name: string; options: unknown; ops: unknown }> = [
      { name: "null options", options: null, ops: productionRunPlanConstructionOps },
      { name: "primitive options", options: 42, ops: productionRunPlanConstructionOps },
      { name: "array options", options: ["HOSTILE"], ops: productionRunPlanConstructionOps },
      {
        name: "empty run root",
        options: { runRoot: "", state, proposal },
        ops: productionRunPlanConstructionOps,
      },
      {
        name: "non-string run root",
        options: { runRoot: 42, state, proposal },
        ops: productionRunPlanConstructionOps,
      },
      {
        name: "null ops",
        options: { runRoot: dirs.runRoot, state, proposal },
        ops: null,
      },
      {
        name: "primitive ops",
        options: { runRoot: dirs.runRoot, state, proposal },
        ops: "HOSTILE-OPS",
      },
      {
        name: "ops with no loaders",
        options: { runRoot: dirs.runRoot, state, proposal },
        ops: { hostile: "HOSTILE-OPS-FIELD" },
      },
      {
        name: "ops with non-function loaders",
        options: { runRoot: dirs.runRoot, state, proposal },
        ops: { loadPlanRevision: 42, loadTaskRevision: null },
      },
    ];

    for (const item of cases) {
      let stateReads = 0;
      const stateProxy = new Proxy(state as unknown as Record<string, unknown>, {
        get(target, property) {
          stateReads += 1;
          return target[property as string];
        },
      });
      const optionReads: string[] = [];
      const optionsValue = item.options;
      const isRecordOptions =
        optionsValue !== null && typeof optionsValue === "object" && !Array.isArray(optionsValue);
      const withStateProxy: Record<string, unknown> = isRecordOptions
        ? { ...(optionsValue as Record<string, unknown>), state: stateProxy }
        : (optionsValue as Record<string, unknown>);
      const optionsArg: unknown = isRecordOptions
        ? new Proxy(withStateProxy, {
            get(target, property) {
              optionReads.push(String(property));
              return target[property as string];
            },
          })
        : optionsValue;
      const cause = await constructPipelineV2RunPlanCandidateFromProposalInternal(
        optionsArg,
        item.ops as PipelineV2RunPlanConstructionOps,
      ).catch((error) => error);
      const error = expectConstructionError(cause, "invalid_options");
      expect(cause, item.name).not.toBeInstanceOf(TypeError);
      expect((error as Error).message, item.name).not.toContain("HOSTILE");
      expect((error as Error).message, item.name).not.toContain("CANARY");
      // the state document is never read through: every case fails before
      // the state validation
      expect(stateReads, item.name).toBe(0);
      // record-shaped options are captured field by field; non-record
      // options read nothing at all
      if (optionsValue !== null && typeof optionsValue === "object" && !Array.isArray(optionsValue)) {
        expect(optionReads, item.name).toEqual(["runRoot", "state", "proposal"]);
      } else {
        expect(optionReads, item.name).toEqual([]);
      }
    }
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 12. the state and run-root boundaries ----------------------------------

test("12. the state validation, run-root binding and root-task derivation boundaries are typed own failures", async () => {
  const harness = await setupHarness(CONSTRUCTION_PIPELINE);
  const { dirs } = harness;
  try {
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    await playPlanningExecution(harness.recording);
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
      new_tasks: [],
    });

    // a malformed state document
    for (const badState of [null, 42, "{}", { schema_version: 6 }]) {
      const cause = await constructPipelineV2RunPlanCandidateFromProposal({
        runRoot: join(dirs.runRoot),
        state: badState,
        proposal,
      }).catch((error) => error);
      const error = expectConstructionError(cause, "invalid_state");
      expect(error.state).toBeNull();
    }

    // a durable state with no execution at all
    const noExecutionRoot = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-construction-b1-"));
    try {
      const freshSink = new PipelineV2RunStateSink({
        stateRoot: noExecutionRoot,
        runId: RUN_ID,
        now: nextTick,
      });
      const freshRecording = new RecordingSink(freshSink);
      await freshRecording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
        inputs: BASE_INPUTS.map((input) => ({ ...input })),
      });
      const causeNoExecution = await constructPipelineV2RunPlanCandidateFromProposal({
        runRoot: dirs.runRoot,
        state: freshRecording.snapshot,
        proposal,
      }).catch((error) => error);
      const errorNoExecution = expectConstructionError(causeNoExecution, "invalid_state");
      expect((errorNoExecution as Error).message).toContain("records no execution");
    } finally {
      await rm(noExecutionRoot, { recursive: true, force: true });
    }

    // the run-root basename binding, before any store load
    const foreignRoot = join(dirs.root, "runs", "other-run");
    await mkdir(foreignRoot, { recursive: true });
    const { ops, calls } = recordingOps();
    const causeRoot = await constructPipelineV2RunPlanCandidateFromProposalInternal(
      { runRoot: foreignRoot, state: harness.recording.snapshot, proposal },
      ops,
    ).catch((error) => error);
    const errorRoot = expectConstructionError(causeRoot, "run_root_mismatch");
    expect((errorRoot as Error).message).toContain("does not belong to this run");
    expect(calls).toEqual([]);

    // the root-task input boundaries: no input named `task`
    const noTaskRoot = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-construction-b2-"));
    try {
      const noTaskSink = new PipelineV2RunStateSink({
        stateRoot: noTaskRoot,
        runId: RUN_ID,
        now: nextTick,
      });
      const noTaskRecording = new RecordingSink(noTaskSink);
      await noTaskRecording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
        inputs: [{ id: "brief", type: "file", protected: true, digest: PROTECTED_DIGEST }],
      });
      await playPlanningExecution(noTaskRecording);
      const causeNoRootTask = await constructPipelineV2RunPlanCandidateFromProposal({
        runRoot: dirs.runRoot,
        state: noTaskRecording.snapshot,
        proposal,
      }).catch((error) => error);
      const errorNoRootTask = expectConstructionError(causeNoRootTask, "invalid_state");
      expect((errorNoRootTask as Error).message).toContain("exactly one input");
    } finally {
      await rm(noTaskRoot, { recursive: true, force: true });
    }

    // an unprotected root task input
    const unprotectedRoot = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-construction-b3-"));
    try {
      const unprotectedSink = new PipelineV2RunStateSink({
        stateRoot: unprotectedRoot,
        runId: RUN_ID,
        now: nextTick,
      });
      const unprotectedRecording = new RecordingSink(unprotectedSink);
      await unprotectedRecording.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
        inputs: [{ id: "task", type: "file", protected: false, digest: PROTECTED_DIGEST }],
      });
      await playPlanningExecution(unprotectedRecording);
      const causeUnprotected = await constructPipelineV2RunPlanCandidateFromProposal({
        runRoot: dirs.runRoot,
        state: unprotectedRecording.snapshot,
        proposal,
      }).catch((error) => error);
      const errorUnprotected = expectConstructionError(causeUnprotected, "invalid_state");
      expect((errorUnprotected as Error).message).toContain("not protected");
    } finally {
      await rm(unprotectedRoot, { recursive: true, force: true });
    }
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 13. the candidate is returned directly by identity ----------------------

test("13. the construction returns the existing candidate preparation's exact result — never a copy, an envelope or a cached object", async () => {
  const fixture = await setupRuntimeProposal();
  const { dirs, pipeline, recording, protectedDigest, records } = fixture;
  try {
    const read = await readAcceptedJsonOutput(pipeline, dirs.runRoot, records, "architect", "plan", 1);
    const proposal = preparePipelineV2RunPlanProposal(read.value);
    const first = await constructPipelineV2RunPlanCandidateFromProposal({
      runRoot: dirs.runRoot,
      state: recording.snapshot,
      proposal,
    });
    const second = await constructPipelineV2RunPlanCandidateFromProposal({
      runRoot: dirs.runRoot,
      state: recording.snapshot,
      proposal,
    });
    // no cached shared object: every construction prepares its own candidate
    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    // the exact two-key candidate shape, deep-frozen, with no envelope
    expect(Object.keys(first).sort()).toEqual(["plan", "task_revisions"]);
    expect(Object.isFrozen(first)).toBe(true);
    expect(Object.isFrozen(first.plan)).toBe(true);
    expect(Object.isFrozen(first.plan.manifest)).toBe(true);
    expect(Object.isFrozen(first.task_revisions)).toBe(true);
    for (const task of first.task_revisions) {
      expect(Object.isFrozen(task)).toBe(true);
      expect(Object.keys(task).sort()).toEqual(["canonical_json", "manifest", "sha256"]);
    }
    expect(Object.keys(first.plan).sort()).toEqual(["canonical_json", "manifest", "sha256"]);
    // the exact object is registered in the existing candidate provenance
    // registry: the compiled acceptance boundary accepts it without any
    // re-validation
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(
      pipeline,
      recording.snapshot as PipelineV2RunState,
      first,
    );
    expect(compiled.plan_revision).toBe(1);
    expect(JSON.stringify(first)).not.toContain(dirs.root);
    expect(JSON.stringify(first)).not.toContain(CANARY_BODY);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 14. the derivation never re-checks the acceptance boundary --------------

test("14. the origin is the last durable execution regardless of its role — the acceptance boundary is never re-checked here", async () => {
  const fixture = await setupRevisionOne();
  const { harness, plan1 } = fixture;
  const { dirs } = harness;
  try {
    // a real stage execution becomes the last durable execution
    const acceptedPlan = verifyPipelineV2RunPlanCandidateForAcceptance(
      harness.pipeline,
      harness.recording.snapshot as PipelineV2RunState,
      preparePipelineV2RunPlanCandidate({
        plan: plan1,
        taskRevisions: [fixture.taskA1],
        previousPlan: null,
        previousTaskRevisions: [],
        protectedInputDigest: PROTECTED_DIGEST,
      }),
    );
    await ensurePipelineV2StageIteration({
      compiledPlan: acceptedPlan,
      stageId: "stage-1",
      initialBudget: 2,
      sink: harness.recording,
    });
    await dispatch(harness, {
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    sessionCounter += 1;
    await dispatch(harness, {
      kind: "start_agent_execution",
      stateId: "dev_entry",
      profile: "coder",
      executionRole: "stage",
      iterationIndex: 1,
    });
    for (const command of executionPhases(sessionCounter)) {
      await dispatch(harness, command);
    }
    await dispatch(harness, {
      kind: "agent_outputs_accepted",
      outputs: [],
    });
    await dispatch(harness, { kind: "agent_cleanup_completed" });

    const state = harness.recording.snapshot as PipelineV2RunState;
    const lastExecution = state.executions[state.executions.length - 1];
    expect(lastExecution).toMatchObject({ index: 2, state_id: "dev_entry", execution_role: "stage" });

    const candidate = await constructPipelineV2RunPlanCandidateFromProposal({
      runRoot: dirs.runRoot,
      state,
      proposal: preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
        new_tasks: [],
      }),
    });
    expect(candidate.plan.manifest.revision).toBe(2);
    expect(candidate.plan.manifest.origin_execution).toBe(2);
    expect(candidate.plan.manifest.previous_sha256).toBe(plan1.sha256);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 15. export surfaces and the source scan --------------------------------

test("15a. the facade exports exactly the error class and the construction function", async () => {
  const facade = await import("../src/pipeline_v2_run_plan_construction.ts");
  expect(Object.keys(facade).sort()).toEqual([
    "PipelineV2RunPlanConstructionError",
    "constructPipelineV2RunPlanCandidateFromProposal",
  ]);
  const internalModule = await import("../src/pipeline_v2_run_plan_construction_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2RunPlanConstructionError",
    "constructPipelineV2RunPlanCandidateFromProposalInternal",
    "productionRunPlanConstructionOps",
  ]);
  const error = new PipelineV2RunPlanConstructionError("artifact_missing", "message", null);
  expect(error.reason).toBe("artifact_missing");
  expect(error.state).toBeNull();
  expect(error.name).toBe("PipelineV2RunPlanConstructionError");
  expect(() => new PipelineV2RunPlanConstructionError("no_such_reason" as never, "message", null)).toThrow(TypeError);
});

test("15b. the construction layer is strictly read-only: one manifest preparer path, one candidate preparer, two store loaders, no second machinery", async () => {
  const facade = await readFile("/workspace/orchestrator/src/pipeline_v2_run_plan_construction.ts", "utf8");
  const internal = await readFile(
    "/workspace/orchestrator/src/pipeline_v2_run_plan_construction_internal.ts",
    "utf8",
  );
  for (const [name, text] of [["facade", facade], ["internal", internal]] as const) {
    // no publication, no durable dispatch, no reducer, no sink
    expect(text, name).not.toMatch(/publishPipelineV2/);
    expect(text, name).not.toMatch(/PipelineV2RunStateSink/);
    expect(text, name).not.toMatch(/reducePipelineV2RunCommand/);
    expect(text, name).not.toMatch(/dispatch\(/);
    // no coordinator, runner or CLI
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_coordinator\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_runner\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/main\.ts"/);
    // no filesystem writes
    expect(text, name).not.toMatch(/writeFile|mkdir|rename\(|rm\(|chmod/);
    // no second serializer or digest builder
    expect(text, name).not.toMatch(/canonicalJson/);
    expect(text, name).not.toMatch(/CryptoHasher|createHash/);
    // no compiled, acceptance, restore or stage-iteration consumption
    expect(text, name).not.toMatch(/acceptPipelineV2|openPipelineV2|ensurePipelineV2|closePipelineV2|compilePipelineV2|restorePipelineV2/);
  }
  // the internal core: no second parser, no provenance minter, no registry
  expect(internal).not.toMatch(/JSON\.parse/);
  expect(internal).not.toMatch(/registerPreparedRunPlanObject/);
  expect(internal).not.toMatch(/WeakMap|WeakSet/);
  // the single manifest preparer path: only the two prepare* functions
  expect(internal).toMatch(/prepareTaskRevisionManifest\(/);
  expect(internal).toMatch(/preparePlanRevisionManifest\(/);
  expect(internal).not.toMatch(
    /parseTaskRevisionManifest\(|parsePlanRevisionManifest\(|prepareWaitIntent\(|parseWaitIntent\(/,
  );
  // the single candidate preparer
  expect(internal).toMatch(/preparePipelineV2RunPlanCandidate\(/);
  expect(internal).not.toMatch(/publishPipelineV2RunPlanCandidate\(/);
  // exactly the two store loaders
  expect(internal).toMatch(/loadPipelineV2PlanRevision/);
  expect(internal).toMatch(/loadPipelineV2TaskRevision/);
  // the proposal arrives prepared; the layer never prepares or parses one
  expect(internal).not.toMatch(/preparePipelineV2RunPlanProposal\(|parsePipelineV2RunPlanProposal\(/);
  expect(internal).toMatch(
    /import type \{[^}]*\} from "\.\/pipeline_v2_run_plan_proposal\.ts";/,
  );
  expect(internal.split('from "./pipeline_v2_run_plan_proposal.ts"')).toHaveLength(2);
  // the facade delegates to the internal core with the frozen production ops
  expect(facade).toMatch(/constructPipelineV2RunPlanCandidateFromProposalInternal/);
  expect(facade).toMatch(/productionRunPlanConstructionOps/);
});
