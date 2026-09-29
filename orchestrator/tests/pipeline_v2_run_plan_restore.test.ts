import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  validatePipelineV2RunState,
  parsePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type RunInputsSnapshot,
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
} from "../src/pipeline_v2_run_plan_store.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { acceptPipelineV2ContinueStageIntent } from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import { openPipelineV2ContinuedStage } from "../src/pipeline_v2_continued_stage_controller.ts";
import {
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";
import { publishPipelineV2WaitRequest as publishWaitRequest } from "../src/pipeline_v2_wait_store.ts";
import { PipelineError } from "../src/pipeline.ts";
import {
  restorePipelineV2AcceptedRunPlan,
  PipelineV2RunPlanRestoreError,
  type PipelineV2RunPlanRestoreFailureReason,
} from "../src/pipeline_v2_run_plan_restore.ts";
import {
  productionRunPlanRestoreOps,
  restorePipelineV2AcceptedRunPlanInternal,
  type PipelineV2RunPlanRestoreOps,
} from "../src/pipeline_v2_run_plan_restore_internal.ts";

const RUN_ID = "restore-run";
const PROTECTED_DIGEST = "a".repeat(64);
const CANARY_BODY = "CANARY_secret_body_value";

const hex = (char: string): string => char.repeat(64);

const RESTORE_PIPELINE = `
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
    - state_id: dev_entry
      role: stage
      stage_template: development

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

const BASE_INPUTS: readonly PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
];

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

async function writeBundle(dirs: Dirs, yaml: string, promptBody: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), promptBody);
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

interface RestoreHarness {
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
  runId: string;
  sink: PipelineV2RunStateSink;
  recording: RecordingSink;
}

async function setupHarness(yaml: string, runId = RUN_ID): Promise<RestoreHarness> {
  const dirs = await makeDirs("pipeline-v2-plan-restore-", runId);
  await writeBundle(dirs, yaml, "IMPLEMENT-THE-TASK\n");
  await writeFile(join(dirs.sources, "task.md"), "TASK-BODY\n");
  const pipeline = await loadPipelineV2(dirs.bundle);
  resetClock();
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  return { dirs, pipeline, runId, sink, recording: new RecordingSink(sink) };
}

async function dispatch(harness: RestoreHarness, command: PipelineV2RunCommand): Promise<void> {
  await harness.recording.dispatch(command);
}

/** Reopens the durable run through the real loader/store path. */
async function reopen(harness: RestoreHarness): Promise<RecordingSink> {
  const reopened = await PipelineV2RunStateSink.open({
    stateRoot: harness.dirs.stateRoot,
    runId: harness.runId,
    now: nextTick,
  });
  return new RecordingSink(reopened);
}

// --- fixtures ---------------------------------------------------------------

function executionPhases(sessionIndex: number): PipelineV2RunCommand[] {
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${sessionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${sessionIndex}` },
    { kind: "agent_running" },
  ];
}

/** The zero-output planning phases: the plain execution phases plus the accepted-and-settled pair. */
function planningPhases(sessionIndex: number): PipelineV2RunCommand[] {
  return [
    ...executionPhases(sessionIndex),
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ];
}

let planningSession = 0;

async function playPlanningExecution(recording: RecordingSink): Promise<number> {
  planningSession += 1;
  const state = recording.snapshot as PipelineV2RunState;
  const index = state.executions.length + 1;
  await recording.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
  for (const command of planningPhases(planningSession)) {
    await recording.dispatch(command);
  }
  return index;
}

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
  tasks: ReadonlyArray<{ id: string; revision: number; sha256: string }>,
): { id: string; template: string; tasks: { id: string; revision: number; sha256: string; depends_on: readonly string[] }[] } {
  return {
    id,
    template: "development",
    tasks: tasks.map((task) => ({ id: task.id, revision: task.revision, sha256: task.sha256, depends_on: [] })),
  };
}

function taskA1Prepared(): PreparedPipelineV2RunTaskRevision {
  return preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
}

function planValue(options: {
  revision: number;
  previousSha256: string | null;
  rootTaskInputId: string;
  rootTaskSha256: string;
  originExecution: number;
  pointerTasks: ReadonlyArray<{ id: string; revision: number; sha256: string }>;
}): PreparedPipelineV2RunPlanRevision {
  return preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: options.revision,
    previous_sha256: options.previousSha256,
    root_task: { input_id: options.rootTaskInputId, sha256: options.rootTaskSha256 },
    origin_execution: options.originExecution,
    stages: [stageSpec("stage-1", options.pointerTasks)],
  });
}

interface RevisionOneOptions {
  /** A durable input id different from the format-pinned root task binding. */
  durableInputId?: string;
  rootTaskSha256?: string;
  protectedInput?: boolean;
}

interface Fixture {
  harness: RestoreHarness;
  accepted: { compiled_plan: CompiledPipelineV2RunPlan };
  plan1: PreparedPipelineV2RunPlanRevision;
  taskA1: PreparedPipelineV2RunTaskRevision;
}

/**
 * Revision-1 fixture: one settled planning execution and the real plan r1
 * acceptance. When the plan's root task binding cannot pass the
 * acceptance controller's digest check, the same ledger record is
 * produced through the real reducer directly beside the real published
 * artifact.
 */
async function setupRevisionOne(options: RevisionOneOptions = {}): Promise<Fixture> {
  const durableInputId = options.durableInputId ?? "task";
  const rootTaskSha256 = options.rootTaskSha256 ?? PROTECTED_DIGEST;
  const protectedInput = options.protectedInput ?? true;
  const harness = await setupHarness(RESTORE_PIPELINE);
  const inputs: readonly PipelineV2RunInputState[] = [
    {
      id: durableInputId,
      type: "file",
      protected: protectedInput,
      digest: PROTECTED_DIGEST,
    },
  ];
  await harness.recording.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
    inputs: inputs.map((input) => ({ ...input })),
  });
  await playPlanningExecution(harness.recording);
  const taskA1 = taskA1Prepared();
  const plan1 = planValue({
    revision: 1,
    previousSha256: null,
    rootTaskInputId: "task",
    rootTaskSha256,
    originExecution: 1,
    pointerTasks: [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }],
  });
  if (rootTaskSha256 === PROTECTED_DIGEST && durableInputId === "task") {
    const candidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      sink: harness.recording,
      candidate,
    });
    return { harness, accepted: { compiled_plan: accepted.compiled_plan }, plan1, taskA1 };
  }
  // the same ledger record through the real reducer beside the real artifact
  await publishPipelineV2TaskRevision(harness.dirs.runRoot, taskA1.manifest);
  await publishPipelineV2PlanRevision(harness.dirs.runRoot, plan1.manifest);
  await harness.recording.dispatch({
    kind: "task_revision_accepted",
    taskId: "task-a",
    revision: 1,
    taskSha256: taskA1.sha256,
  });
  await harness.recording.dispatch({
    kind: "plan_revision_accepted",
    planRevision: 1,
    planSha256: plan1.sha256,
    originExecution: 1,
  });
  const candidateForCompile = preparePipelineV2RunPlanCandidate({
    plan: plan1,
    taskRevisions: [taskA1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: rootTaskSha256,
  });
  return {
    harness,
    accepted: { compiled_plan: compilePipelineV2RunPlanCandidate(harness.pipeline, candidateForCompile) },
    plan1,
    taskA1,
  };
}

interface ReviseFixture extends Fixture {
  taskA2: PreparedPipelineV2RunTaskRevision;
  taskB1: PreparedPipelineV2RunTaskRevision | null;
  plan2: PreparedPipelineV2RunPlanRevision;
  accepted2: { compiled_plan: CompiledPipelineV2RunPlan };
}

interface RevisionTwoOptions {
  extraTaskB?: boolean;
  forgePlan?: { previousSha256?: string; originExecution?: number };
}

/**
 * The two-revision fixture: plan r1 accepted honestly; the user-response
 * task revision of task-a accepted through the real reducer inside the
 * open wait; the second planning execution; plan r2 either accepted
 * through the real acceptance controller or, when `forgePlan` is set,
 * recorded through the real reducer beside a deliberately published plan
 * artifact variant.
 */
async function setupRevisionTwo(options: RevisionTwoOptions = {}): Promise<ReviseFixture> {
  const harness = await setupHarness(RESTORE_PIPELINE);
  await harness.recording.dispatch({
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
  const accepted1 = await acceptPipelineV2RunPlanCandidate({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    sink: harness.recording,
    candidate: candidate1,
  });

  // the planning transition commits before the wait opens at the cursor
  await harness.recording.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  await harness.recording.dispatch({
    kind: "run_waiting",
    stateId: "dev_entry",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: hex("e"),
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await harness.recording.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("c") });
  const taskA2 = preparedTask("task-a", 2, taskA1.sha256, "user_response", "Body A two");
  await harness.recording.dispatch({
    kind: "task_revision_accepted",
    taskId: "task-a",
    revision: 2,
    taskSha256: taskA2.sha256,
    waitIndex: 1,
    intentSha256: hex("c"),
  });
  await publishPipelineV2TaskRevision(harness.dirs.runRoot, taskA2.manifest);
  const pointerTasks: { id: string; revision: number; sha256: string }[] = [
    { id: "task-a", revision: 2, sha256: taskA2.sha256 },
  ];
  const currentTasks = [taskA2];
  let taskB1: PreparedPipelineV2RunTaskRevision | null = null;
  if (options.extraTaskB === true) {
    taskB1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");
    pointerTasks.push({ id: "task-b", revision: 1, sha256: taskB1.sha256 });
    currentTasks.push(taskB1);
  }
  await harness.recording.dispatch({
    kind: "wait_response_recorded",
    waitIndex: 1,
    expectedRequestSha256: hex("e"),
    actionId: "revise_task",
    responseSha256: hex("f"),
  });
  const secondExecution = await playPlanningExecution(harness.recording);
  const artifactPlan = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 2,
    previous_sha256: options.forgePlan?.previousSha256 ?? plan1.sha256,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: options.forgePlan?.originExecution ?? secondExecution,
    stages: [stageSpec("stage-1", pointerTasks)],
  });
  let accepted2: { compiled_plan: CompiledPipelineV2RunPlan };
  if (options.forgePlan === undefined) {
    const candidate2 = preparePipelineV2RunPlanCandidate({
      plan: artifactPlan,
      taskRevisions: currentTasks,
      previousPlan: plan1,
      previousTaskRevisions: [taskA1],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      sink: harness.recording,
      candidate: candidate2,
    });
    accepted2 = { compiled_plan: accepted.compiled_plan };
  } else {
    // the compiled reference comes from the honest plan manifest and is
    // never published or accepted; the durable ledger and the published
    // artifact variant are forged deliberately for the negative tests
    const honestPlan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: secondExecution,
      stages: [stageSpec("stage-1", pointerTasks)],
    });
    const candidateForCompile = preparePipelineV2RunPlanCandidate({
      plan: honestPlan2,
      taskRevisions: currentTasks,
      previousPlan: plan1,
      previousTaskRevisions: [taskA1],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    accepted2 = { compiled_plan: compilePipelineV2RunPlanCandidate(harness.pipeline, candidateForCompile) };
    await publishPipelineV2PlanRevision(harness.dirs.runRoot, artifactPlan.manifest);
    await harness.recording.dispatch({
      kind: "plan_revision_accepted",
      planRevision: 2,
      planSha256: artifactPlan.sha256,
      originExecution: secondExecution,
    });
  }
  return {
    harness,
    accepted: { compiled_plan: accepted1.compiled_plan },
    plan1,
    taskA1,
    taskA2,
    taskB1,
    plan2: artifactPlan,
    accepted2,
  };
}

// --- restore helpers --------------------------------------------------------

const { loadPlanRevision: loadPlanRevisionReal, loadTaskRevision: loadTaskRevisionReal } = productionRunPlanRestoreOps;

interface RecordedOps {
  ops: PipelineV2RunPlanRestoreOps;
  calls: string[];
}

/** Wraps the real production ops with an exact ordered call log. */
function recordingOps(): RecordedOps {
  const calls: string[] = [];
  const ops: PipelineV2RunPlanRestoreOps = {
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

function expectRestoreError(
  cause: unknown,
  reason: PipelineV2RunPlanRestoreFailureReason,
): PipelineV2RunPlanRestoreError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanRestoreError);
  const error = cause as PipelineV2RunPlanRestoreError;
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

// --- 1. the revision-1 happy path -------------------------------------------

test("1. the revision-1 accepted plan restores through the real store and proves provenance", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(state)).not.toThrow();
    const { ops, calls } = recordingOps();
    const restored = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state },
      ops,
    );
    // the restored compiled plan is structurally the acceptance's compiled
    // plan and a genuinely different object of the same chain
    expect(restored.compiled_plan).toEqual(fixture.accepted.compiled_plan);
    expect(restored.compiled_plan).not.toBe(fixture.accepted.compiled_plan);
    expect(restored.state).toEqual(state);
    // the result is deep-frozen
    expect(Object.isFrozen(restored)).toBe(true);
    expect(Object.isFrozen(restored.compiled_plan)).toBe(true);
    expect(Object.isFrozen(restored.compiled_plan.stages)).toBe(true);
    expect(Object.isFrozen(restored.state)).toBe(true);
    // the exact load order: current plan, current task; no predecessor loads
    expect(calls).toEqual(["plan:1", "task:task-a:1"]);
    // the restored object is accepted by the existing compiled-plan stage
    // lookup, proving real provenance registration
    const stage = compiledPipelineV2RunPlanStageFor(restored.compiled_plan, "stage-1");
    expect(stage.id).toBe("stage-1");
    expect(stage.tasks[0]?.id).toBe("task-a");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 2. the revision-2 happy path -------------------------------------------

test("2. the revision-2 accepted plan restores the predecessor plan and the revised task chain", async () => {
  const fixture = await setupRevisionTwo();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(state)).not.toThrow();
    const { ops, calls } = recordingOps();
    const restored = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state },
      ops,
    );
    expect(restored.compiled_plan).toEqual(fixture.accepted2.compiled_plan);
    expect(restored.compiled_plan.plan_revision).toBe(2);
    // the fixed sequential load order: current plan, predecessor plan,
    // current task, its immediate predecessor
    expect(calls).toEqual(["plan:2", "plan:1", "task:task-a:2", "task:task-a:1"]);
    const stage = compiledPipelineV2RunPlanStageFor(restored.compiled_plan, "stage-1");
    expect(stage.tasks[0]).toMatchObject({ id: "task-a", revision: 2 });
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 3. revision 2 with a brand-new task revision 1 -------------------------

test("3. a new task revision 1 reads no predecessor while the old revised task's predecessor is read", async () => {
  const fixture = await setupRevisionTwo({ extraTaskB: true });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const { ops, calls } = recordingOps();
    const restored = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state },
      ops,
    );
    expect(restored.compiled_plan).toEqual(fixture.accepted2.compiled_plan);
    // task-b (revision 1) reads no predecessor; task-a (revision 2) reads its own
    expect(calls).toEqual(["plan:2", "plan:1", "task:task-a:2", "task:task-a:1", "task:task-b:1"]);
    const stage = compiledPipelineV2RunPlanStageFor(restored.compiled_plan, "stage-1");
    expect(stage.tasks.map((task) => task.id)).toEqual(["task-a", "task-b"]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 4. the durable ledger, never the filesystem, selects the revision ------

test("4. orphan plan and task artifacts on disk are ignored in favor of the durable ledger", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    // newer orphan artifacts that the durable ledger never accepted
    const orphanPlan = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: fixture.plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 1,
      stages: [stageSpec("stage-1", [{ id: "task-a", revision: 1, sha256: fixture.taskA1.sha256 }])],
    });
    await publishPipelineV2PlanRevision(harness.dirs.runRoot, orphanPlan.manifest);
    const orphanTask = preparedTask("task-a", 2, fixture.taskA1.sha256, "user_response", "Orphan body");
    await publishPipelineV2TaskRevision(harness.dirs.runRoot, orphanTask.manifest);
    const state = harness.recording.snapshot as PipelineV2RunState;
    const { ops, calls } = recordingOps();
    const restored = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state },
      ops,
    );
    // only the durable revision-1 artifacts were read
    expect(calls).toEqual(["plan:1", "task:task-a:1"]);
    expect(restored.compiled_plan).toEqual(fixture.accepted.compiled_plan);
    expect(restored.compiled_plan.plan_revision).toBe(1);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 5. the missing-artifact matrix -----------------------------------------

async function restoreExpectingMissing(
  harness: RestoreHarness,
  mutateRoot: (runRoot: string) => Promise<void>,
): Promise<PipelineV2RunPlanRestoreError> {
  const state = harness.recording.snapshot as PipelineV2RunState;
  await mutateRoot(harness.dirs.runRoot);
  const cause = await restorePipelineV2AcceptedRunPlan({
    pipeline: harness.pipeline,
    runRoot: harness.dirs.runRoot,
    state,
  }).catch((error) => error);
  const error = expectRestoreError(cause, "artifact_missing");
  expect(error.state).not.toBeNull();
  expect((error as Error).message).not.toContain(CANARY_BODY);
  return error;
}

test("5a. a missing current plan manifest is artifact_missing", async () => {
  const fixture = await setupRevisionOne();
  try {
    await restoreExpectingMissing(fixture.harness, async (runRoot) => {
      await rm(join(runRoot, "run-plan", "plans", "1.json"));
    });
  } finally {
    await rm(fixture.harness.dirs.root, { recursive: true, force: true });
  }
});

test("5b. a missing predecessor plan manifest is artifact_missing", async () => {
  const fixture = await setupRevisionTwo();
  try {
    await restoreExpectingMissing(fixture.harness, async (runRoot) => {
      await rm(join(runRoot, "run-plan", "plans", "1.json"));
    });
  } finally {
    await rm(fixture.harness.dirs.root, { recursive: true, force: true });
  }
});

test("5c. a missing current task manifest is artifact_missing", async () => {
  const fixture = await setupRevisionOne();
  try {
    await restoreExpectingMissing(fixture.harness, async (runRoot) => {
      await rm(join(runRoot, "run-plan", "tasks", "task-a", "1.json"));
    });
  } finally {
    await rm(fixture.harness.dirs.root, { recursive: true, force: true });
  }
});

test("5d. a missing predecessor task manifest is artifact_missing", async () => {
  const fixture = await setupRevisionTwo();
  try {
    await restoreExpectingMissing(fixture.harness, async (runRoot) => {
      await rm(join(runRoot, "run-plan", "tasks", "task-a", "1.json"));
    });
  } finally {
    await rm(fixture.harness.dirs.root, { recursive: true, force: true });
  }
});

// --- forged-ledger fixture helpers ------------------------------------------

/**
 * Builds a fresh durable state on a separate state root (the restore takes
 * the state as an argument; only the run-root binding and the artifacts
 * are shared with the fixture's run root) and forges the plan r1 ledger
 * record's digest through the real reducer, while the real artifacts stay
 * on disk.
 */
async function forgeFreshState(
  pipeline: ResolvedPipelineV2,
  planningExecutions: number,
  forge: (recording: RecordingSink) => Promise<void>,
): Promise<PipelineV2RunState> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-restore-forge-"));
  try {
    const stateRoot = join(root, "state");
    await mkdir(stateRoot, { recursive: true });
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: RUN_ID, now: nextTick });
    const recording = new RecordingSink(sink);
    await recording.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: BASE_INPUTS.map((input) => ({ ...input })),
    });
    for (let index = 0; index < planningExecutions; index += 1) {
      await playPlanningExecution(recording);
    }
    await forge(recording);
    return recording.snapshot as PipelineV2RunState;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

// --- 6. the mismatch matrix ---------------------------------------------------

test("6a. a durable plan record digest differing from the loaded plan manifest is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = await forgeFreshState(harness.pipeline, 1, async (recording) => {
      await recording.dispatch({
        kind: "plan_revision_accepted",
        planRevision: 1,
        planSha256: hex("f"),
        originExecution: 1,
      });
    });
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("plan manifest digest differs");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6b. a plan manifest whose previous digest differs from the durable chain is artifact_mismatch", async () => {
  const fixture = await setupRevisionTwo({ forgePlan: { previousSha256: hex("9") } });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("previous digest differs from the durable plan record");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6c. a plan manifest whose origin execution differs from the durable record is artifact_mismatch", async () => {
  const fixture = await setupRevisionTwo({ forgePlan: { originExecution: 1 } });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("different origin execution");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6d. a predecessor plan manifest digest differing from the durable predecessor record is artifact_mismatch", async () => {
  const fixture = await setupRevisionTwo();
  const { harness } = fixture;
  try {
    // a fresh run root carries the honest revision-1 artifacts and the
    // stored current plan artifact chains to the forged predecessor
    // digest, so the first divergence is the predecessor record's digest
    const storeRoot = join(harness.dirs.root, "runs-2", RUN_ID);
    await mkdir(storeRoot, { recursive: true });
    await publishPipelineV2PlanRevision(storeRoot, fixture.plan1.manifest);
    await publishPipelineV2TaskRevision(storeRoot, fixture.taskA1.manifest);
    const forgedChainPlan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: hex("e"),
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 2,
      stages: [stageSpec("stage-1", [{ id: "task-a", revision: 1, sha256: fixture.taskA1.sha256 }])],
    });
    await publishPipelineV2PlanRevision(storeRoot, forgedChainPlan2.manifest);
    const state = await forgeFreshState(harness.pipeline, 1, async (recording) => {
      await recording.dispatch({
        kind: "plan_revision_accepted",
        planRevision: 1,
        planSha256: hex("e"),
        originExecution: 1,
      });
      // the second planning execution starts at the moved cursor; the
      // reducer never checks the role against the compiled pipeline
      await recording.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
      await recording.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "planning" });
      for (const command of planningPhases(2)) {
        await recording.dispatch(command);
      }
      await recording.dispatch({
        kind: "plan_revision_accepted",
        planRevision: 2,
        planSha256: forgedChainPlan2.sha256,
        originExecution: 2,
      });
    });
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: storeRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("plan manifest digest differs");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6e. a durable task record digest differing from the plan pointer is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = await forgeFreshState(harness.pipeline, 1, async (recording) => {
      await recording.dispatch({ kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("d") });
      await recording.dispatch({ kind: "plan_revision_accepted", planRevision: 1, planSha256: fixture.plan1.sha256, originExecution: 1 });
    });
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("differs from the plan pointer");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6f. a plan pointer naming a task the ledger does not record is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = await forgeFreshState(harness.pipeline, 1, async (recording) => {
      await recording.dispatch({ kind: "task_revision_accepted", taskId: "task-z", revision: 1, taskSha256: hex("d") });
      await recording.dispatch({ kind: "plan_revision_accepted", planRevision: 1, planSha256: fixture.plan1.sha256, originExecution: 1 });
    });
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("does not carry exactly one");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6g. a predecessor task manifest digest differing from the durable predecessor record is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    // the ledger chains task-a r2 to a forged predecessor digest; the
    // stored predecessor artifact is the real revision-1 manifest
    const taskA2 = preparedTask("task-a", 2, hex("e"), "user_response", "Forged-chain body");
    const plan2 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 2,
      previous_sha256: fixture.plan1.sha256,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 2,
      stages: [stageSpec("stage-1", [{ id: "task-a", revision: 2, sha256: taskA2.sha256 }])],
    });
    const state = await forgeFreshState(harness.pipeline, 1, async (recording) => {
      await recording.dispatch({
        kind: "plan_revision_accepted",
        planRevision: 1,
        planSha256: fixture.plan1.sha256,
        originExecution: 1,
      });
      // the forged predecessor task digest is recorded while the phase is
      // still running; the successor revision chains to it inside the wait
      await recording.dispatch({ kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("e") });
      await recording.dispatch({
        kind: "transition_committed",
        step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
        executionIndex: 1,
      });
      await recording.dispatch({
        kind: "run_waiting",
        stateId: "dev_entry",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("e"),
        actions: [{ id: "revise_task", to: "architect" }],
      });
      await recording.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: hex("c") });
      await recording.dispatch({
        kind: "task_revision_accepted",
        taskId: "task-a",
        revision: 2,
        taskSha256: taskA2.sha256,
        waitIndex: 1,
        intentSha256: hex("c"),
      });
      await publishPipelineV2TaskRevision(fixture.harness.dirs.runRoot, taskA2.manifest);
      await recording.dispatch({
        kind: "wait_response_recorded",
        waitIndex: 1,
        expectedRequestSha256: hex("e"),
        actionId: "revise_task",
        responseSha256: hex("f"),
      });
      await playPlanningExecution(recording);
      await recording.dispatch({
        kind: "plan_revision_accepted",
        planRevision: 2,
        planSha256: plan2.sha256,
        originExecution: 2,
      });
    });
    await publishPipelineV2PlanRevision(fixture.harness.dirs.runRoot, plan2.manifest);
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: fixture.harness.pipeline,
      runRoot: fixture.harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("digest differs from the plan pointer");
  } finally {
    await rm(fixture.harness.dirs.root, { recursive: true, force: true });
  }
});

test("6h. a plan root task binding a durable input id that does not exist is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne({ durableInputId: "brief" });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("do not carry exactly one input");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6i. a plan root task binding an unprotected durable input is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne({ protectedInput: false });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("is not protected");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6j. a plan root task digest differing from the durable protected input digest is artifact_mismatch", async () => {
  const fixture = await setupRevisionOne({ rootTaskSha256: hex("8") });
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    const error = expectRestoreError(cause, "artifact_mismatch");
    expect((error as Error).message).toContain("differs from the plan's root task binding");
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6k. a run root of another run is a pipeline_mismatch before any store load", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const otherRunRoot = join(harness.dirs.root, "runs", "other-run");
    await mkdir(otherRunRoot, { recursive: true });
    const state = harness.recording.snapshot as PipelineV2RunState;
    const { ops, calls } = recordingOps();
    const cause = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: harness.pipeline, runRoot: otherRunRoot, state },
      ops,
    ).catch((error) => error);
    const error = expectRestoreError(cause, "pipeline_mismatch");
    expect((error as Error).message).toContain("does not belong to this run");
    expect(calls).toEqual([]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("6l. a foreign pipeline identity is a pipeline_mismatch before any store load", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const foreignBundle = join(harness.dirs.root, "foreign-bundle");
    await mkdir(join(foreignBundle, "prompts"), { recursive: true });
    await writeFile(join(foreignBundle, "pipeline.yaml"), RESTORE_PIPELINE);
    await writeFile(join(foreignBundle, "prompts", "coder.md"), "DIFFERENT-PROMPT-BODY\n");
    const foreign = await loadPipelineV2(foreignBundle);
    const state = harness.recording.snapshot as PipelineV2RunState;
    const { ops, calls } = recordingOps();
    const cause = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: foreign, runRoot: harness.dirs.runRoot, state },
      ops,
    ).catch((error) => error);
    const error = expectRestoreError(cause, "pipeline_mismatch");
    expect((error as Error).message).toContain("different pipeline");
    expect(calls).toEqual([]);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 7. the malformed loader-result matrix ----------------------------------

test("7. malformed loader results are typed restore failures, never TypeErrors", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const runRestoreWith = async (ops: PipelineV2RunPlanRestoreOps): Promise<unknown> =>
      await restorePipelineV2AcceptedRunPlanInternal(
        { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state },
        ops,
      ).catch((error) => error);

    // null where the current plan artifact is expected: artifact_missing
    const nullPlan = expectRestoreError(
      await runRestoreWith({ ...productionRunPlanRestoreOps, loadPlanRevision: async () => null }),
      "artifact_missing",
    );
    expect((nullPlan as Error).message).toContain("plan revision 1 manifest is missing");

    // a primitive where the wrapper is expected
    const numberPlan = expectRestoreError(
      await runRestoreWith({ ...productionRunPlanRestoreOps, loadPlanRevision: async () => 42 }),
      "artifact_mismatch",
    );
    expect((numberPlan as Error).message).toContain("not a published manifest wrapper");

    // an array wrapper
    expectRestoreError(
      await runRestoreWith({ ...productionRunPlanRestoreOps, loadPlanRevision: async () => [] }),
      "artifact_mismatch",
    );

    // a wrapper without the prepared manifest
    const emptyPlan = expectRestoreError(
      await runRestoreWith({ ...productionRunPlanRestoreOps, loadPlanRevision: async () => ({}) }),
      "artifact_mismatch",
    );
    expect((emptyPlan as Error).message).toContain("carries no plan prepared manifest");

    // a prepared manifest without the manifest record
    const noManifest = expectRestoreError(
      await runRestoreWith({ ...productionRunPlanRestoreOps, loadPlanRevision: async () => ({ plan: {} }) }),
      "artifact_mismatch",
    );
    expect((noManifest as Error).message).toContain("carries no manifest record");

    // a prepared manifest of the wrong kind
    const wrongKind = expectRestoreError(
      await runRestoreWith({
        ...productionRunPlanRestoreOps,
        loadPlanRevision: async () => ({ plan: { manifest: { kind: "task_revision" } } }),
      }),
      "artifact_mismatch",
    );
    expect((wrongKind as Error).message).toContain("different manifest kind");

    // a malformed manifest projection: fields missing at the first comparison
    const malformedProjection = expectRestoreError(
      await runRestoreWith({
        ...productionRunPlanRestoreOps,
        loadPlanRevision: async () => ({ plan: { manifest: { kind: "plan_revision" }, sha256: hex("f") } }),
      }),
      "artifact_mismatch",
    );
    expect((malformedProjection as Error).message).toContain("belongs to a different run");

    // a malformed task loader result
    const malformedTask = expectRestoreError(
      await runRestoreWith({
        ...productionRunPlanRestoreOps,
        loadTaskRevision: async () => ({ task: { manifest: { kind: "plan_revision" } } }),
      }),
      "artifact_mismatch",
    );
    expect((malformedTask as Error).message).toContain("different manifest kind");

    // a task manifest whose previous digest differs from the durable record
    const wrongPrevious = expectRestoreError(
      await runRestoreWith({
        ...productionRunPlanRestoreOps,
        loadTaskRevision: async () => ({
          task: {
            manifest: {
              kind: "task_revision",
              run_id: RUN_ID,
              task_id: "task-a",
              revision: 1,
              previous_sha256: hex("9"),
              origin: "planning_proposal",
              body: "Injected body",
            },
            canonical_json: "{}",
            sha256: fixture.taskA1.sha256,
          },
        }),
      }),
      "artifact_mismatch",
    );
    expect((wrongPrevious as Error).message).toContain("previous digest differs from the durable task record");

    // a task manifest carrying a different revision than the pointer
    const wrongRevision = expectRestoreError(
      await runRestoreWith({
        ...productionRunPlanRestoreOps,
        loadTaskRevision: async () => ({
          task: {
            manifest: {
              kind: "task_revision",
              run_id: RUN_ID,
              task_id: "task-a",
              revision: 7,
              previous_sha256: null,
              origin: "planning_proposal",
              body: "Injected body",
            },
            canonical_json: "{}",
            sha256: fixture.taskA1.sha256,
          },
        }),
      }),
      "artifact_mismatch",
    );
    expect((wrongRevision as Error).message).toContain("carries a different revision");

    // an unexpected loader error keeps its identity
    const loaderFailure = new Error("LOADER-EXPLODED");
    const loaderCause = await runRestoreWith({
      ...productionRunPlanRestoreOps,
      loadPlanRevision: async () => {
        throw loaderFailure;
      },
    });
    expect(loaderCause).toBe(loaderFailure);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 8. capture and provenance ----------------------------------------------

test("8a. the options fields and the ops getters are read exactly once in the fixed order", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const reads: string[] = [];
    const optionsProxy = new Proxy(
      { pipeline: harness.pipeline, runRoot: harness.dirs.runRoot, state, hostile: "HOSTILE-FIELD" },
      {
        get(target, property) {
          reads.push(String(property));
          return (target as Record<string, unknown>)[property as string];
        },
      },
    );
    let opsReads = 0;
    const opsProxy: PipelineV2RunPlanRestoreOps = new Proxy(productionRunPlanRestoreOps as unknown as Record<string, unknown>, {
      get(target, property) {
        opsReads += 1;
        reads.push(`ops:${String(property)}`);
        return (target as Record<string, unknown>)[property as string];
      },
    }) as unknown as PipelineV2RunPlanRestoreOps;
    const restored = await restorePipelineV2AcceptedRunPlanInternal(optionsProxy, opsProxy);
    expect(restored.compiled_plan).toEqual(fixture.accepted.compiled_plan);
    // exactly the capture members; the hostile extra field is never read
    expect(reads).toEqual(["pipeline", "runRoot", "state", "ops:loadPlanRevision", "ops:loadTaskRevision"]);
    expect(opsReads).toBe(2);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("8b. caller mutation after the pending load cannot change the restoration policy", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ops: PipelineV2RunPlanRestoreOps = {
      ...productionRunPlanRestoreOps,
      loadPlanRevision: async (runRoot, revision) => {
        await gate;
        return await productionRunPlanRestoreOps.loadPlanRevision(runRoot, revision);
      },
    };
    const options: { pipeline: ResolvedPipelineV2; runRoot: string; state: unknown } = {
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    };
    const pending = restorePipelineV2AcceptedRunPlanInternal(options, ops);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    // mutate the caller's options while the load is pending
    options.pipeline = {} as unknown as ResolvedPipelineV2;
    options.runRoot = "/nowhere";
    options.state = null;
    release?.();
    const restored = await pending;
    expect(restored.compiled_plan).toEqual(fixture.accepted.compiled_plan);
    expect(restored.compiled_plan.plan_revision).toBe(1);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("8c. cloned, spread and Proxy pipelines are rejected by the existing provenance gate before any state read or store call", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    // a Proxy state that must never be read through
    let stateReads = 0;
    const stateProxy = new Proxy(state as unknown as Record<string, unknown>, {
      get(target, property) {
        stateReads += 1;
        return target[property as string];
      },
    });
    // a Proxy pipeline whose traps must never fire
    let pipelineTraps = 0;
    const pipelineProxy = new Proxy(harness.pipeline as unknown as Record<string, unknown>, {
      get(target, property) {
        pipelineTraps += 1;
        return target[property as string];
      },
    }) as unknown as ResolvedPipelineV2;
    const { ops, calls } = recordingOps();
    const cause = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: pipelineProxy, runRoot: harness.dirs.runRoot, state: stateProxy },
      ops,
    ).catch((error) => error);
    // the existing gate's error passes through unchanged
    expect(cause).toBeInstanceOf(PipelineError);
    expect(cause).not.toBeInstanceOf(PipelineV2RunPlanRestoreError);
    expect(pipelineTraps).toBe(0);
    expect(stateReads).toBe(0);
    expect(calls).toEqual([]);

    // a structural clone is equally rejected by the identity-bound gate
    const clone = JSON.parse(JSON.stringify(harness.pipeline));
    const cloneCause = await restorePipelineV2AcceptedRunPlanInternal(
      { pipeline: clone, runRoot: harness.dirs.runRoot, state: stateProxy },
      recordingOps().ops,
    ).catch((error) => error);
    expect(cloneCause).toBeInstanceOf(PipelineError);
    expect(stateReads).toBe(0);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("8d. malformed ops are typed invalid_options at the capture boundary, before any downstream verification", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    // The downstream poison: a pipeline clone trips the provenance gate
    // with a PipelineError, a foreign run root trips the pipeline_mismatch
    // binding and a Proxy state trips reads at validation — the malformed
    // ops must be rejected at the capture boundary first, every time.
    const clonePipeline = JSON.parse(JSON.stringify(harness.pipeline)) as ResolvedPipelineV2;
    const foreignRunRoot = join(harness.dirs.root, "runs", "foreign-run");
    const cases: ReadonlyArray<{ name: string; ops: unknown; expectedOpsReads: readonly string[] }> = [
      { name: "null", ops: null, expectedOpsReads: [] },
      { name: "undefined", ops: undefined, expectedOpsReads: [] },
      { name: "primitive number", ops: 42, expectedOpsReads: [] },
      { name: "primitive string", ops: "HOSTILE-OPS-PRIMITIVE", expectedOpsReads: [] },
      { name: "array", ops: ["HOSTILE-OPS-ARRAY"], expectedOpsReads: [] },
      {
        name: "record with no loaders",
        ops: { hostile: "HOSTILE-OPS-FIELD" },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
      {
        name: "record with only loadPlanRevision",
        ops: { loadPlanRevision: async () => null, hostile: "HOSTILE-OPS-FIELD" },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
      {
        name: "record with only loadTaskRevision",
        ops: { loadTaskRevision: async () => null, hostile: "HOSTILE-OPS-FIELD" },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
      {
        name: "non-function loadPlanRevision",
        ops: { loadPlanRevision: "HOSTILE-NOT-A-FUNCTION", loadTaskRevision: async () => null },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
      {
        name: "non-function loadTaskRevision",
        ops: { loadPlanRevision: async () => null, loadTaskRevision: 42 },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
      {
        name: "both loaders non-functions",
        ops: { loadPlanRevision: null, loadTaskRevision: null, hostile: "HOSTILE-OPS-FIELD" },
        expectedOpsReads: ["loadPlanRevision", "loadTaskRevision"],
      },
    ];

    for (const item of cases) {
      const optionReads: string[] = [];
      let stateReads = 0;
      const stateProxy = new Proxy(state as unknown as Record<string, unknown>, {
        get(target, property) {
          stateReads += 1;
          return target[property as string];
        },
      });
      const optionsProxy = new Proxy(
        { pipeline: clonePipeline, runRoot: foreignRunRoot, state: stateProxy, hostile: "HOSTILE-OPTIONS-FIELD" },
        {
          get(target, property) {
            optionReads.push(String(property));
            return (target as Record<string, unknown>)[property as string];
          },
        },
      );
      const opsReads: string[] = [];
      const opsValue = item.ops;
      const opsArg: unknown =
        opsValue !== null && typeof opsValue === "object"
          ? new Proxy(opsValue as Record<string, unknown>, {
              get(target, property) {
                opsReads.push(String(property));
                return target[property as string];
              },
            })
          : opsValue;
      const cause = await restorePipelineV2AcceptedRunPlanInternal(
        optionsProxy,
        opsArg as PipelineV2RunPlanRestoreOps,
      ).catch((error) => error);
      const error = expectRestoreError(cause, "invalid_options");
      expect(cause).not.toBeInstanceOf(TypeError);
      expect(error.message).not.toContain("CANARY");
      expect(error.message).not.toContain("HOSTILE");
      expect(optionReads).toEqual(["pipeline", "runRoot", "state"]);
      expect(stateReads).toBe(0);
      expect(opsReads).toEqual([...item.expectedOpsReads]);
    }
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

// --- 9. the read-only proof --------------------------------------------------

test("9a. the restoration never mutates the filesystem on success or on failure", async () => {
  const fixture = await setupRevisionOne();
  const { harness } = fixture;
  try {
    const state = harness.recording.snapshot as PipelineV2RunState;
    const before = await fingerprint(harness.dirs.root);
    await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    });
    expect(await fingerprint(harness.dirs.root)).toBe(before);

    // a representative failure: the current plan manifest removed
    await rm(join(harness.dirs.runRoot, "run-plan", "plans", "1.json"));
    const afterRemoval = await fingerprint(harness.dirs.root);
    const cause = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state,
    }).catch((error) => error);
    expectRestoreError(cause, "artifact_missing");
    expect(await fingerprint(harness.dirs.root)).toBe(afterRemoval);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});

test("9b. the restore layer imports no writer, reducer, sink, controller, coordinator, runner or CLI", async () => {
  const facade = await readFile("/workspace/orchestrator/src/pipeline_v2_run_plan_restore.ts", "utf8");
  const internal = await readFile("/workspace/orchestrator/src/pipeline_v2_run_plan_restore_internal.ts", "utf8");
  for (const [name, text] of [["facade", facade], ["internal", internal]] as const) {
    expect(text, name).not.toMatch(/publishPipelineV2/);
    expect(text, name).not.toMatch(/PipelineV2RunStateSink/);
    expect(text, name).not.toMatch(/dispatch\(/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_coordinator\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_runner\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/main\.ts"/);
    expect(text, name).not.toMatch(/writeFile|mkdir|rename\(|rm\(|chmod/);
    expect(text, name).not.toMatch(/canonicalJson/);
    expect(text, name).not.toMatch(/reducePipelineV2RunCommand/);
    expect(text, name).not.toMatch(/acceptPipelineV2|openPipelineV2|ensurePipelineV2|closePipelineV2/);
  }
  // no second parser, serializer, digest builder or provenance registry
  expect(internal).not.toMatch(/JSON\.parse/);
  expect(internal).not.toMatch(/createHash/);
  expect(internal).not.toMatch(/WeakMap|WeakSet/);
});

// --- 10. the export surface ---------------------------------------------------

test("10. the public facade exports exactly the error and the restore function", async () => {
  const facade = await import("../src/pipeline_v2_run_plan_restore.ts");
  expect(Object.keys(facade).sort()).toEqual(["PipelineV2RunPlanRestoreError", "restorePipelineV2AcceptedRunPlan"]);
  const internalModule = await import("../src/pipeline_v2_run_plan_restore_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2RunPlanRestoreError",
    "productionRunPlanRestoreOps",
    "restorePipelineV2AcceptedRunPlanInternal",
  ]);
  const error = new PipelineV2RunPlanRestoreError("artifact_missing", "message", null);
  expect(error.reason).toBe("artifact_missing");
  expect(error.state).toBeNull();
  expect(error.name).toBe("PipelineV2RunPlanRestoreError");
});

// --- the integration bridge proof --------------------------------------------

test("11. the bridge: the durable ledger and manifests restore the compiled plan that drives the continued-stage composition", async () => {
  const harness = await setupHarness(RESTORE_PIPELINE);
  try {
    // real run: project copy, protected input snapshot, create_run
    await prepareRunProject(harness.dirs.projectSource, harness.dirs.runRoot);
    const runInputs = await snapshotRunInputs(harness.pipeline, [{ id: "task", path: join(harness.dirs.sources, "task.md") }], harness.dirs.runRoot);
    const protectedInput = runInputs.inputs[0];
    if (protectedInput === undefined) {
      throw new Error("the bridge fixture lost its protected run input");
    }
    await dispatch(harness, {
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(harness.pipeline),
      inputs: runInputs.inputs.map((entry) => ({
        id: entry.id,
        type: entry.type,
        protected: entry.protected,
        digest: entry.digest,
      })),
    });

    // real planning execution and real plan r1 acceptance
    const accepted: AcceptedStateOutput[] = [];
    const architectIndex = (harness.recording.snapshot as PipelineV2RunState).executions.length + 1;
    const architectActivation = await prepareActivationData(harness.pipeline, runInputs, accepted, "architect", architectIndex);
    await dispatch(harness, { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
    for (const command of executionPhases(1)) {
      await dispatch(harness, command);
    }
    const architectRecords = await acceptActivationOutputs(harness.pipeline, architectActivation);
    accepted.push(...architectRecords);
    await dispatch(harness, {
      kind: "agent_outputs_accepted",
      outputs: architectRecords.map((record) => ({ id: record.output, digest: record.digest })),
    });
    await dispatch(harness, { kind: "agent_cleanup_completed" });

    const taskA1 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "PLAN-TASK-BODY",
    });
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: protectedInput.digest },
      origin_execution: 1,
      stages: [stageSpec("stage-1", [{ id: "task-a", revision: 1, sha256: taskA1.sha256 }])],
    });
    const candidate1 = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: protectedInput.digest,
    });
    const acceptedPlan = await acceptPipelineV2RunPlanCandidate({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      sink: harness.recording,
      candidate: candidate1,
    });

    // real generation 1 / iteration 1 and the planning transition
    await ensurePipelineV2StageIteration({
      compiledPlan: acceptedPlan.compiled_plan,
      stageId: "stage-1",
      initialBudget: 2,
      sink: harness.recording,
    });
    await dispatch(harness, {
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });

    // real stage execution of iteration 1 with the durable transition back
    // to the planning/wait state
    const stageActivation = await prepareActivationData(harness.pipeline, runInputs, accepted, "dev_entry", 2);
    await dispatch(harness, {
      kind: "start_agent_execution",
      stateId: "dev_entry",
      profile: "coder",
      executionRole: "stage",
      iterationIndex: 1,
    });
    for (const command of executionPhases(2)) {
      await dispatch(harness, command);
    }
    const stageRecords = await acceptActivationOutputs(harness.pipeline, stageActivation);
    await dispatch(harness, {
      kind: "agent_outputs_accepted",
      outputs: stageRecords.map((record) => ({ id: record.output, digest: record.digest })),
    });
    await dispatch(harness, { kind: "agent_cleanup_completed" });
    accepted.push(...stageRecords);
    await dispatch(harness, {
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 2,
    });

    // real wait request and real continue-stage intent acceptance
    const request = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "architect",
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await dispatch(harness, {
      kind: "run_waiting",
      stateId: "architect",
      reason: "stage_iteration_limit_exhausted",
      requestSha256: request.sha256,
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await publishWaitRequest(harness.dirs.runRoot, request.manifest);
    const intent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: "stage-1",
      expected_plan_sha256: plan1.sha256,
      additional_iterations: 2,
    });
    await acceptPipelineV2ContinueStageIntent({ runRoot: harness.dirs.runRoot, sink: harness.recording, intent });

    // the simulated process restart: the run is reopened through the real
    // loader/store path
    const reopened = await reopen(harness);
    const reopenedState = reopened.snapshot as PipelineV2RunState;

    // THE BRIDGE: no in-memory compiled plan is handed over; the accepted
    // plan is restored exclusively from the durable ledger and the
    // immutable manifests
    const restored = await restorePipelineV2AcceptedRunPlan({
      pipeline: harness.pipeline,
      runRoot: harness.dirs.runRoot,
      state: reopenedState,
    });
    expect(restored.compiled_plan).toEqual(acceptedPlan.compiled_plan);
    expect(restored.compiled_plan).not.toBe(acceptedPlan.compiled_plan);
    expect(restored.state).toEqual(reopenedState);
    expect(compiledPipelineV2RunPlanStageFor(restored.compiled_plan, "stage-1").id).toBe("stage-1");

    // the existing continued-stage composition consumes exactly the
    // restored compiled plan
    const compositionCommands: string[] = [];
    const compositionSink = {
      get snapshot() {
        return reopened.snapshot;
      },
      get poisoned() {
        return reopened.poisoned;
      },
      async dispatch(command: PipelineV2RunCommand) {
        compositionCommands.push(command.kind as string);
        await reopened.dispatch(command);
      },
    };
    const composed = await openPipelineV2ContinuedStage({
      runRoot: harness.dirs.runRoot,
      sink: compositionSink,
      intent,
      compiledPlan: restored.compiled_plan,
      initialBudget: 2,
    });
    expect(compositionCommands).toEqual([
      "iteration_grant_recorded",
      "stage_iteration_closed",
      "wait_response_recorded",
      "stage_iteration_opened",
    ]);

    // the exact composition boundary on the reopened snapshot
    const state = reopened.snapshot as PipelineV2RunState;
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.cursor).toEqual({ current_state: "dev_entry", transition_count: 2 });
    expect(state.generations).toHaveLength(1);
    expect(state.generations[0]?.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    expect(state.generations[0]?.iterations[0]?.closed).toEqual({ by: "grant", wait_index: 1, closed_transition_count: 2 });
    expect(state.waits[0]?.intent).toEqual({ intent_sha256: intent.sha256 });
    expect(state.waits[0]?.response).toEqual({ action_id: "continue_stage", response_sha256: composed.response_sha256 });
    expect(state.grants).toEqual([
      { index: 1, generation_index: 1, wait_index: 1, intent_sha256: intent.sha256, additional_iterations: 2 },
    ]);
    expect(composed.state).toEqual(state);
    // the loader accepts the composed document
    const raw = await readFile(join(harness.dirs.stateRoot, "pipeline-runs", RUN_ID, "state.json"), "utf8");
    expect(parsePipelineV2RunState(raw)).toEqual(state);
  } finally {
    await rm(harness.dirs.root, { recursive: true, force: true });
  }
});
