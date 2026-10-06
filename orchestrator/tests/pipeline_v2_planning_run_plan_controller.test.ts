import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  parsePipelineV2RunState,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
} from "../src/pipeline_v2_runtime.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { PipelineError } from "../src/pipeline.ts";
import {
  acceptPipelineV2PlanningRunPlan,
  PipelineV2PlanningRunPlanControllerError,
} from "../src/pipeline_v2_planning_run_plan_controller.ts";
import {
  acceptPipelineV2PlanningRunPlanInternal,
  productionPlanningRunPlanOps,
  type PlanningRunPlanControllerOps,
} from "../src/pipeline_v2_planning_run_plan_controller_internal.ts";
import type { AcceptedPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller_internal.ts";
import {
  restorePipelineV2PlanningAcceptanceContext,
} from "../src/pipeline_v2_resume_context.ts";
import { compiledExecutionRoleFor } from "../src/pipeline_v2_orchestration.ts";
import { readAcceptedJsonOutput } from "../src/pipeline_v2_runtime.ts";
import { preparePipelineV2RunPlanProposal } from "../src/pipeline_v2_run_plan_proposal.ts";
import { PipelineV2RunPlanProposalError } from "../src/pipeline_v2_run_plan_proposal.ts";
import { constructPipelineV2RunPlanCandidateFromProposal } from "../src/pipeline_v2_run_plan_construction.ts";
import { PipelineV2RunPlanConstructionError } from "../src/pipeline_v2_run_plan_construction_internal.ts";
import { verifyPipelineV2RunPlanCandidateForAcceptance } from "../src/pipeline_v2_run_plan_acceptance.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { PipelineV2RunPlanControllerError } from "../src/pipeline_v2_run_plan_controller_internal.ts";

const RUN_ID = "planning-run";

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
        to: architect
  - id: done
    type: terminal
    result: success
`;

/**
 * The two-planning-state topology for the pointer-only completed control:
 * the second settled planning execution runs on `implement` after the
 * planning transition.
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
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: implement
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

const POINTER_ONLY_DOC = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [],
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

function executionPhases(sessionIndex: number): PipelineV2RunCommand[] {
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${sessionIndex}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${sessionIndex}` },
    { kind: "agent_running" },
  ];
}

let sessionCounter = 0;

/**
 * One settled-but-unbound planning execution: prepares the activation,
 * writes the proposal as the accepted JSON output and accepts it through
 * the runtime. Returns the activation's accepted records.
 */
async function playPlanningExecution(
  recording: RecordingSink,
  pipeline: ResolvedPipelineV2,
  runInputs: Awaited<ReturnType<typeof snapshotRunInputs>>,
  options: { stateId: string; proposalDoc: unknown; priorAccepted: Awaited<ReturnType<typeof acceptActivationOutputs>> },
): Promise<Awaited<ReturnType<typeof acceptActivationOutputs>>> {
  sessionCounter += 1;
  const state = recording.snapshot as PipelineV2RunState;
  const index = state.executions.length + 1;
  const prep = await prepareActivationData(pipeline, runInputs, options.priorAccepted, options.stateId, index);
  await recording.dispatch({
    kind: "start_agent_execution",
    stateId: options.stateId,
    profile: "architect",
    executionRole: "planning",
  });
  for (const command of executionPhases(sessionCounter)) {
    await recording.dispatch(command);
  }
  await writeFile(join(prep.outputs_root, "plan"), JSON.stringify(options.proposalDoc), { mode: 0o600 });
  const records = await acceptActivationOutputs(pipeline, prep);
  await recording.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await recording.dispatch({ kind: "agent_cleanup_completed" });
  return records;
}

/** The honest runtime prefix: create_run plus one settled planning execution. */
async function setupPlanningPrefix(
  pipelineYaml: string = CONSTRUCTION_PIPELINE,
  proposalDoc: unknown = PROPOSAL_DOC,
): Promise<{
  dirs: Dirs;
  pipeline: ResolvedPipelineV2;
  recording: RecordingSink;
  records: Awaited<ReturnType<typeof acceptActivationOutputs>>;
  runInputs: Awaited<ReturnType<typeof snapshotRunInputs>>;
}> {
  const dirs = await makeDirs("pipeline-v2-planning-controller-", RUN_ID);
  await writeBundle(dirs, pipelineYaml);
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
  const records = await playPlanningExecution(recording, pipeline, runInputs, {
    stateId: "architect",
    proposalDoc,
    priorAccepted: [],
  });
  return { dirs, pipeline, recording, records, runInputs };
}

/** inode/mode/mtime/bytes identity of the three plan-store artifacts. */
async function artifactStats(runRoot: string): Promise<string> {
  const files = [
    join(runRoot, "run-plan", "tasks", "task-a", "1.json"),
    join(runRoot, "run-plan", "tasks", "task-b", "1.json"),
    join(runRoot, "run-plan", "plans", "1.json"),
  ];
  const lines: string[] = [];
  for (const path of files) {
    const info = await lstat(path);
    lines.push(
      `${path} ${info.ino} ${(info.mode & 0o777).toString(8)} ${info.mtimeMs} ${(await readFile(path)).toString("base64")}`,
    );
  }
  return lines.join("\n");
}

function dispatchSuffix(recording: { commands: PipelineV2RunCommand[] }): string[] {
  return recording.commands.map((command) => {
    if (command.kind === "task_revision_accepted") {
      return `${command.taskId}:${command.revision}`;
    }
    if (command.kind === "plan_revision_accepted") {
      return `plan:${command.planRevision}`;
    }
    return command.kind;
  });
}

interface FaultySink {
  snapshot: PipelineV2RunState | null;
  poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  commands: PipelineV2RunCommand[];
}

/** Records every dispatch and forwards to the real sink; refuses from `failFrom` on. */
function faultySinkRecording(inner: PipelineV2RunStateSink, failFrom: number): FaultySink {
  const commands: PipelineV2RunCommand[] = [];
  return {
    get snapshot(): PipelineV2RunState | null {
      return inner.snapshot;
    },
    get poisoned(): boolean {
      return inner.poisoned;
    },
    commands,
    async dispatch(command: PipelineV2RunCommand): Promise<void> {
      commands.push({ ...command });
      if (commands.length > failFrom) {
        throw new PipelineV2RunStateStoreError(
          "the injected store fault refused the pipeline v2 run state commit",
        );
      }
      await inner.dispatch(command);
    },
  };
}

/** A snapshot-read-counting sink wrapper. */
function countingSnapshotSink(inner: { snapshot: PipelineV2RunState | null; poisoned: boolean; dispatch: (command: PipelineV2RunCommand) => Promise<void> }): { snapshot: PipelineV2RunState | null; poisoned: boolean; dispatch: (command: PipelineV2RunCommand) => Promise<void>; reads: () => number } {
  let reads = 0;
  return {
    get snapshot(): PipelineV2RunState | null {
      reads += 1;
      return inner.snapshot;
    },
    get poisoned(): boolean {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand): Promise<void> {
      await inner.dispatch(command);
    },
    reads: () => reads,
  };
}

// --- 1. the production-chain matrix ------------------------------------------

test("1. the composition returns the exact accepted plan on fresh, partial and completed boundaries through the production chain", async () => {
  // Fresh: the prefix only; the controller runs the whole chain.
  {
    const prefix = await setupPlanningPrefix();
    const { dirs, pipeline, recording } = prefix;
    try {
      const result = await acceptPipelineV2PlanningRunPlan({
        pipeline,
        runRoot: dirs.runRoot,
        sink: recording,
      });
      expect(dispatchSuffix(recording).slice(-3)).toEqual(["task-a:1", "task-b:1", "plan:1"]);
      const finalState = recording.snapshot as PipelineV2RunState;
      expect(finalState.revision).toBe(11);
      expect(finalState.plan_revisions.map((plan) => plan.revision)).toEqual([1]);
      expect(finalState.task_revisions.map((task) => ({ id: task.task_id, revision: task.revision }))).toEqual([
        { id: "task-a", revision: 1 },
        { id: "task-b", revision: 1 },
      ]);
      expect(result.compiled_plan.plan_revision).toBe(1);
      expect(result.compiled_plan.origin_execution).toBe(1);
      expect(result.state === finalState).toBe(true);
      expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
    } finally {
      await rm(dirs.root, { recursive: true, force: true });
    }
  }

  const scenarios: ReadonlyArray<{ label: string; failFrom: number; retrySuffix: string[]; retryDelta: number; retryLoads: string[] }> = [
    { label: "W1 artifacts orphan, ledger empty", failFrom: 0, retrySuffix: ["task-a:1", "task-b:1", "plan:1"], retryDelta: 3, retryLoads: ["plan:1", "task:task-a:1", "task:task-b:1"] },
    { label: "W2 task-a durable", failFrom: 1, retrySuffix: ["task-b:1", "plan:1"], retryDelta: 2, retryLoads: ["plan:1", "task:task-a:1", "task:task-b:1"] },
    { label: "W3 both tasks durable", failFrom: 2, retrySuffix: ["plan:1"], retryDelta: 1, retryLoads: ["plan:1", "task:task-a:1", "task:task-b:1"] },
    { label: "W4 plan durable, result lost", failFrom: 3, retrySuffix: [], retryDelta: 0, retryLoads: ["plan:1", "task:task-a:1", "task:task-b:1"] },
  ];

  for (const scenario of scenarios) {
    const prefix = await setupPlanningPrefix();
    const { dirs, pipeline } = prefix;
    try {
      // The faulted first attempt through the composition itself.
      const openSink = await PipelineV2RunStateSink.open({
        stateRoot: dirs.stateRoot,
        runId: RUN_ID,
        now: nextTick,
      });
      const faulty = faultySinkRecording(openSink, scenario.failFrom);
      const firstCause = await acceptPipelineV2PlanningRunPlan({
        pipeline,
        runRoot: dirs.runRoot,
        sink: faulty,
      }).catch((error) => error);
      if (scenario.failFrom < 3) {
        expect(firstCause, scenario.label).toBeInstanceOf(PipelineV2RunPlanControllerError);
        expect((firstCause as PipelineV2RunPlanControllerError).reason, scenario.label).toBe("state_persist_failed");
      } else {
        expect(firstCause, scenario.label).not.toBeInstanceOf(Error);
      }

      // The restart and the clean composition.
      const reopened = await PipelineV2RunStateSink.open({
        stateRoot: dirs.stateRoot,
        runId: RUN_ID,
        now: nextTick,
      });
      const state = reopened.snapshot;
      if (state === null) {
        throw new Error("the reopened run lost its durable state");
      }
      const retryPipeline = await loadPipelineV2(state.pipeline.bundle_root);
      const retryRecording = new RecordingSink(reopened);
      const result = await acceptPipelineV2PlanningRunPlan({
        pipeline: retryPipeline,
        runRoot: dirs.runRoot,
        sink: retryRecording,
      });
      expect(dispatchSuffix(retryRecording), scenario.label).toEqual(scenario.retrySuffix);
      const revisionBefore = state.revision;
      const finalState = reopened.snapshot as PipelineV2RunState;
      expect(finalState.revision - revisionBefore, scenario.label).toBe(scenario.retryDelta);
      expect(finalState.revision, scenario.label).toBe(8 + Math.min(scenario.failFrom, 3) + scenario.retryDelta);
      expect(finalState.plan_revisions.map((plan) => plan.revision), scenario.label).toEqual([1]);
      expect(
        finalState.task_revisions.map((task) => ({ id: task.task_id, revision: task.revision })),
        scenario.label,
      ).toEqual([
        { id: "task-a", revision: 1 },
        { id: "task-b", revision: 1 },
      ]);
      // The exact accepted plan identity and the artifact preservation.
      expect(result.compiled_plan.plan_revision, scenario.label).toBe(1);
      expect(result.compiled_plan.origin_execution, scenario.label).toBe(1);
      expect(result.state === finalState, scenario.label).toBe(true);
      expect(await artifactStats(dirs.runRoot), scenario.label).toBeDefined();
      expect(parsePipelineV2RunState(JSON.stringify(finalState)), scenario.label).toEqual(finalState);
    } finally {
      await rm(dirs.root, { recursive: true, force: true });
    }
  }
});

test("1b. the composition's retry preserves the published artifacts byte-for-byte across the restart", async () => {
  // W2 scenario with the exact artifact inode/mode/mtime/bytes proof.
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline } = prefix;
  try {
    const openSink = await PipelineV2RunStateSink.open({
      stateRoot: dirs.stateRoot,
      runId: RUN_ID,
      now: nextTick,
    });
    const faulty = faultySinkRecording(openSink, 1);
    await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: dirs.runRoot, sink: faulty }).catch(() => undefined);
    const statsBefore = await artifactStats(dirs.runRoot);
    const reopened = await PipelineV2RunStateSink.open({
      stateRoot: dirs.stateRoot,
      runId: RUN_ID,
      now: nextTick,
    });
    const state = reopened.snapshot;
    if (state === null) {
      throw new Error("the reopened run lost its durable state");
    }
    const retryPipeline = await loadPipelineV2(state.pipeline.bundle_root);
    await acceptPipelineV2PlanningRunPlan({
      pipeline: retryPipeline,
      runRoot: dirs.runRoot,
      sink: reopened,
    });
    expect(await artifactStats(dirs.runRoot)).toBe(statsBefore);
    const finalState = reopened.snapshot as PipelineV2RunState;
    expect(finalState.revision).toBe(11);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 2. the pointer-only completed control -----------------------------------

test("2. the pointer-only completed boundary recognizes the accepted plan; r3 is never created", async () => {
  const prefix = await setupPlanningPrefix(TWO_PLANNING_PIPELINE, PROPOSAL_DOC);
  const { dirs, pipeline, recording, runInputs } = prefix;
  try {
    const result1 = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: dirs.runRoot, sink: recording });
    expect(result1.compiled_plan.plan_revision).toBe(1);
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "implement", transition_index: 0 },
      executionIndex: 1,
    });
    // The second settled planning execution on `implement`, pointer-only.
    const prep2 = await prepareActivationData(pipeline, prefix.runInputs, [], "implement", 2);
    sessionCounter += 1;
    await recording.dispatch({
      kind: "start_agent_execution",
      stateId: "implement",
      profile: "architect",
      executionRole: "planning",
    });
    for (const command of executionPhases(sessionCounter)) {
      await recording.dispatch(command);
    }
    await writeFile(join(prep2.outputs_root, "plan"), JSON.stringify(POINTER_ONLY_DOC), { mode: 0o600 });
    const records2 = await acceptActivationOutputs(pipeline, prep2);
    await recording.dispatch({
      kind: "agent_outputs_accepted",
      outputs: records2.map((record) => ({ id: record.output, digest: record.digest })),
    });
    await recording.dispatch({ kind: "agent_cleanup_completed" });
    const result2 = await acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: dirs.runRoot, sink: recording });
    expect(result2.compiled_plan.plan_revision).toBe(2);
    expect(result2.compiled_plan.origin_execution).toBe(2);
    const suffix = dispatchSuffix(recording);
    expect(suffix.slice(-1)).toEqual(["plan:2"]);

    // The restart: the completed recognition returns the exact r2 with
    // zero dispatch; r3 is never created.
    const reopened = await PipelineV2RunStateSink.open({
      stateRoot: dirs.stateRoot,
      runId: RUN_ID,
      now: nextTick,
    });
    const state = reopened.snapshot;
    if (state === null) {
      throw new Error("the reopened control run lost its durable state");
    }
    const revisionAfterAcceptance = state.revision;
    const retryPipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const retryRecording = new RecordingSink(reopened);
    const retry = await acceptPipelineV2PlanningRunPlan({
      pipeline: retryPipeline,
      runRoot: dirs.runRoot,
      sink: retryRecording,
    });
    expect(retryRecording.commands).toEqual([]);
    expect(retry.compiled_plan.plan_revision).toBe(2);
    expect(retry.compiled_plan.plan_sha256).toBe(result2.compiled_plan.plan_sha256);
    const finalState = reopened.snapshot as PipelineV2RunState;
    expect(finalState.revision).toBe(revisionAfterAcceptance);
    expect(finalState.plan_revisions.map((plan) => plan.revision)).toEqual([1, 2]);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 3. identical concurrency convergence ------------------------------------

test("3. two identical concurrent compositions converge to one accepted plan with no duplicates", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const [result1, result2] = await Promise.all([
      acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: dirs.runRoot, sink: recording }),
      acceptPipelineV2PlanningRunPlan({ pipeline, runRoot: dirs.runRoot, sink: recording }),
    ]);
    // Both succeeded with the exact same accepted plan.
    expect(result1.compiled_plan.plan_sha256).toBe(result2.compiled_plan.plan_sha256);
    expect(result1.compiled_plan).toEqual(result2.compiled_plan);
    expect(result1.state).toEqual(result2.state);
    // The durable ledger carries no duplicates: exactly one record of each.
    const finalState = recording.snapshot as PipelineV2RunState;
    expect(finalState.plan_revisions).toHaveLength(1);
    expect(finalState.task_revisions).toHaveLength(2);
    const suffix = dispatchSuffix(recording).filter((entry) => entry !== "create_run");
    const acceptedOnly = new Set(
      suffix.filter((entry) => entry.startsWith("task-") || entry.startsWith("plan:")),
    );
    expect([...acceptedOnly].sort()).toEqual(["plan:1", "task-a:1", "task-b:1"]);
    expect(finalState.revision).toBe(11);
    expect(parsePipelineV2RunState(JSON.stringify(finalState))).toEqual(finalState);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 4. the restore failure: zero downstream calls, error identity -----------

test("4. a restore failure calls none of the downstream facades and propagates the error by identity", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    // The planning execution's transition is committed: the planning
    // acceptance boundary is gone.
    await recording.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    let restoreError: unknown;
    const calls: string[] = [];
    const ops: PlanningRunPlanControllerOps = {
      ...productionPlanningRunPlanOps,
      restorePlanningContext: async (pipelineArg, state, runRootArg) => {
        try {
          return await restorePipelineV2PlanningAcceptanceContext(pipelineArg, state, runRootArg);
        } catch (cause) {
          restoreError = cause;
          throw cause;
        }
      },
      compiledExecutionRoleFor: (...args: [ResolvedPipelineV2, string]) => {
        calls.push("compiledExecutionRoleFor");
        return compiledExecutionRoleFor(...args);
      },
      readAcceptedJsonOutput: async (...args: Parameters<typeof readAcceptedJsonOutput>) => {
        calls.push("readAcceptedJsonOutput");
        return await readAcceptedJsonOutput(...args);
      },
      prepareProposal: (value) => {
        calls.push("prepareProposal");
        return preparePipelineV2RunPlanProposal(value);
      },
      constructCandidate: async (options) => {
        calls.push("constructCandidate");
        return await constructPipelineV2RunPlanCandidateFromProposal(options);
      },
      verifyCandidate: (pipelineArg, state, candidate) => {
        calls.push("verifyCandidate");
        return verifyPipelineV2RunPlanCandidateForAcceptance(pipelineArg, state, candidate);
      },
      acceptCandidate: async (options) => {
        calls.push("acceptCandidate");
        return await acceptPipelineV2RunPlanCandidate(options);
      },
    };
    const cause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      ops,
    ).catch((error) => error);
    expect(cause).toBe(restoreError);
    expect(calls).toEqual([]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 5. the composed layers' typed failures pass by identity -----------------

test("5. the reader, proposal, construction, verifier and acceptance typed failures pass by identity", async () => {
  const order: string[] = [];
  const opsWithSpy = (overrides: Record<string, unknown>): PlanningRunPlanControllerOps => {
    const spyOps: Record<string, unknown> = { ...productionPlanningRunPlanOps };
    for (const key of ["restorePlanningContext", "compiledExecutionRoleFor", "readAcceptedJsonOutput", "prepareProposal", "constructCandidate", "verifyCandidate", "acceptCandidate", "compiledStageFor"] as const) {
      const original = productionPlanningRunPlanOps[key];
      const override = overrides[key];
      if (override !== undefined) {
        spyOps[key] = override;
      } else {
        spyOps[key] = (...args: unknown[]) => {
          order.push(key);
          return (original as (...a: unknown[]) => unknown)(...args);
        };
      }
    }
    return spyOps as unknown as PlanningRunPlanControllerOps;
  };

  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    // (a) the reader's typed error passes by identity; the later layers
    // are never called
    const readerError = new PipelineError("the reader exploded");
    const readerCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsWithSpy({ readAcceptedJsonOutput: async () => { throw readerError; } }),
    ).catch((error) => error);
    expect(readerCause).toBe(readerError);
    expect(order.filter((entry) => entry !== "readAcceptedJsonOutput" && entry !== "restorePlanningContext" && entry !== "compiledExecutionRoleFor")).toEqual([]);
    order.length = 0;

    // (b) the proposal layer's typed error passes by identity
    const proposalError = new PipelineV2RunPlanProposalError("the proposal layer exploded");
    const proposalCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsWithSpy({ prepareProposal: () => { throw proposalError; } }),
    ).catch((error) => error);
    expect(proposalCause).toBe(proposalError);
    expect(order.filter((entry) => entry.startsWith("construct") || entry.startsWith("verify") || entry.startsWith("accept"))).toEqual([]);
    order.length = 0;

    // (c) the construction's typed error passes by identity
    const constructionError = new PipelineV2RunPlanConstructionError(
      "construction_conflict",
      "the construction exploded",
      null,
    );
    const constructionCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsWithSpy({ constructCandidate: async () => { throw constructionError; } }),
    ).catch((error) => error);
    expect(constructionCause).toBe(constructionError);
    expect(order.filter((entry) => entry.startsWith("verify") || entry.startsWith("accept"))).toEqual([]);
    order.length = 0;

    // (d) the verifier's typed error passes by identity
    const verifierError = new Error("the acceptance verifier exploded");
    const verifierCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsWithSpy({ verifyCandidate: () => { throw verifierError; } }),
    ).catch((error) => error);
    expect(verifierCause).toBe(verifierError);
    expect(order.filter((entry) => entry.startsWith("accept"))).toEqual([]);
    order.length = 0;

    // (e) the acceptance's typed error passes by identity
    const acceptanceError = new PipelineV2RunPlanControllerError("state_persist_failed", "the acceptance exploded", null);
    const acceptanceCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsWithSpy({ acceptCandidate: async () => { throw acceptanceError; } }),
    ).catch((error) => error);
    expect(acceptanceCause).toBe(acceptanceError);
    order.length = 0;
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 6. the malformed result matrix for every boundary -----------------------

test("6. malformed results at every composed boundary are the composition's own invalid_result; the next layer is never called", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const noCalls = (): string[] => [];
    const order: string[] = [];
    const opsWith = (overrides: Record<string, unknown>): PlanningRunPlanControllerOps => {
      const spyOps: Record<string, unknown> = { ...productionPlanningRunPlanOps };
      for (const key of ["restorePlanningContext", "compiledExecutionRoleFor", "readAcceptedJsonOutput", "prepareProposal", "constructCandidate", "verifyCandidate", "acceptCandidate", "compiledStageFor"] as const) {
        const original = productionPlanningRunPlanOps[key];
        const override = overrides[key];
        if (override !== undefined) {
          spyOps[key] = override;
        } else {
          spyOps[key] = (...args: unknown[]) => {
            order.push(key);
            return (original as (...a: unknown[]) => unknown)(...args);
          };
        }
      }
      return spyOps as unknown as PlanningRunPlanControllerOps;
    };
    const run = async (overrides: Record<string, unknown>): Promise<PipelineV2PlanningRunPlanControllerError> => {
      order.length = 0;
      const cause = await acceptPipelineV2PlanningRunPlanInternal(
        { pipeline, runRoot: dirs.runRoot, sink: recording },
        opsWith(overrides),
      ).catch((error) => error);
      expect(cause).toBeInstanceOf(PipelineV2PlanningRunPlanControllerError);
      expect((cause as PipelineV2PlanningRunPlanControllerError).reason).toBe("invalid_result");
      return cause as PipelineV2PlanningRunPlanControllerError;
    };

    // (a) the restore boundary
    expect((await run({ restorePlanningContext: async () => null })).message).toContain("not a record");
    expect((await run({ restorePlanningContext: async () => 42 })).message).toContain("not a record");
    expect((await run({ restorePlanningContext: async () => [] })).message).toContain("not a record");
    expect((await run({ restorePlanningContext: async () => ({}) })).message).toContain("different key set");
    expect(order).toEqual([]);
    const validShape = await restorePipelineV2PlanningAcceptanceContext(pipeline, recording.snapshot, dirs.runRoot);
    expect(
      (await run({
        restorePlanningContext: async () => ({
          ...validShape,
          planning_execution_index: 99,
          state: validShape.state,
        }),
      })).message,
    ).toContain("does not name the last durable execution");
    expect(
      (await run({
        restorePlanningContext: async () => ({
          ...validShape,
          cursor: { current_state: "forged", transition_count: 0 },
        }),
      })).message,
    ).toContain("cursor projection does not match");
    expect(
      (await run({
        restorePlanningContext: async () => ({
          ...validShape,
          state: { ...validShape.state, run_id: "forged-run" },
        }),
      })).message,
    ).toContain("does not belong to this run");
    expect(order).toEqual([]);

    // (b) the compiled role boundary
    expect(
      (await run({ compiledExecutionRoleFor: () => ({ state_id: "architect", role: "stage", plan_output: "plan" }) })).message,
    ).toContain("not the exact planning role");
    expect(
      (await run({ compiledExecutionRoleFor: () => ({ state_id: "architect", role: "planning" }) })).message,
    ).toContain("not the exact planning role");
    expect(
      (await run({ compiledExecutionRoleFor: () => ({ state_id: "forged", role: "planning", plan_output: "plan" }) })).message,
    ).toContain("not the exact planning role");
    expect(
      (await run({ compiledExecutionRoleFor: () => ({ state_id: "architect", role: "planning", plan_output: "not safe!" }) })).message,
    ).toContain("not the exact planning role");
    expect(order).toEqual(["restorePlanningContext"]);

    // (c) the reader boundary
    expect((await run({ readAcceptedJsonOutput: async () => null })).message).toContain("not a record");
    expect((await run({ readAcceptedJsonOutput: async () => ({ state: "architect", output: "plan", activation_index: 1, digest: hex("a"), value: {}, hostile: 1 }) })).message).toContain("different key set");
    expect(
      (await run({ readAcceptedJsonOutput: async () => ({ state: "forged", output: "plan", activation_index: 1, digest: hex("a"), value: {} }) })).message,
    ).toContain("does not bind the requested state and output");
    expect(
      (await run({ readAcceptedJsonOutput: async () => ({ state: "architect", output: "plan", activation_index: 99, digest: hex("a"), value: {} }) })).message,
    ).toContain("does not bind the requested activation index");
    expect(
      (await run({ readAcceptedJsonOutput: async () => ({ state: "architect", output: "plan", activation_index: 1, digest: "NOHASH", value: {} }) })).message,
    ).toContain("lowercase SHA-256");
    expect(order).toEqual(["restorePlanningContext", "compiledExecutionRoleFor"]);

    // (d) the verifier boundary (the expectation itself)
    expect((await run({ verifyCandidate: () => null })).message).toContain("not a record");
    expect((await run({ verifyCandidate: () => 42 })).message).toContain("not a record");
    expect((await run({ verifyCandidate: () => ({ run_id: "x" }) })).message).toContain("different key set");
    expect(
      (await run({
        verifyCandidate: () => ({ run_id: "x", plan_revision: 1, plan_sha256: hex("a"), origin_execution: 1, stages: "not-a-list" }),
      })).message,
    ).toContain("carries no stages list");
    expect(order).toEqual([
      "restorePlanningContext",
      "compiledExecutionRoleFor",
      "readAcceptedJsonOutput",
      "prepareProposal",
      "constructCandidate",
    ]);

    // (e) the acceptance boundary
    expect((await run({ acceptCandidate: async () => null })).message).toContain("not a record");
    expect((await run({ acceptCandidate: async () => ({ compiled_plan: {} }) })).message).toContain("different key set");
    expect(
      (await run({ acceptCandidate: async () => ({ compiled_plan: validShape, state: recording.snapshot }) })).message,
    ).toContain("compiled projection differs");
    const compiled = await (async () => {
      const proposal = preparePipelineV2RunPlanProposal(PROPOSAL_DOC);
      const candidate = await constructPipelineV2RunPlanCandidateFromProposal({
        runRoot: dirs.runRoot,
        state: recording.snapshot,
        proposal,
      });
      return verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, recording.snapshot as PipelineV2RunState, candidate);
    })();
    expect(
      (await run({
        acceptCandidate: async () => ({
          compiled_plan: JSON.parse(JSON.stringify(compiled)),
          state: JSON.parse(JSON.stringify(recording.snapshot)),
        }),
      })).message,
    ).toContain("no compiled-plan provenance");
    expect(
      (await run({
        acceptCandidate: async () => ({ compiled_plan: compiled, state: { ...(recording.snapshot as PipelineV2RunState) } }),
      })).message,
    ).toContain("not the authoritative sink snapshot");
    void noCalls;
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 7. hostile sinks and results cannot heal --------------------------------

test("7. a hostile sink snapshot cannot heal the result's state identity", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    let snapshotSwapped = false;
    const hostileSink = {
      get snapshot(): PipelineV2RunState | null {
        if (snapshotSwapped) {
          return JSON.parse(JSON.stringify(recording.snapshot)) as PipelineV2RunState;
        }
        return recording.snapshot;
      },
      get poisoned(): boolean {
        return recording.poisoned;
      },
      async dispatch(command: PipelineV2RunCommand): Promise<void> {
        snapshotSwapped = true;
        await recording.dispatch(command);
      },
    };
    const cause = await acceptPipelineV2PlanningRunPlan({
      pipeline,
      runRoot: dirs.runRoot,
      sink: hostileSink,
    }).catch((error) => error);
    expect(cause).toBeInstanceOf(PipelineV2PlanningRunPlanControllerError);
    expect((cause as PipelineV2PlanningRunPlanControllerError).reason).toBe("invalid_result");
    expect((cause as Error).message).toContain("not the authoritative sink snapshot");
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 8. the options Proxy reads ----------------------------------------------

test("8. the options fields are read exactly once in the fixed order and hostile extras are never read", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const reads: string[] = [];
    const optionsProxy = new Proxy(
      {
        pipeline,
        runRoot: dirs.runRoot,
        sink: recording,
        hostile: "HOSTILE-OPTIONS-FIELD",
      } as unknown as Record<string, unknown>,
      {
        get(target, property) {
          reads.push(String(property));
          return target[property as string];
        },
      },
    );
    const result = await acceptPipelineV2PlanningRunPlanInternal(optionsProxy, productionPlanningRunPlanOps);
    expect(result.compiled_plan.plan_revision).toBe(1);
    expect(reads).toEqual(["pipeline", "runRoot", "sink"]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 9. the ops Proxy reads --------------------------------------------------

test("9. every ops member is read exactly once", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const reads: string[] = [];
    const opsProxy = new Proxy(
      productionPlanningRunPlanOps as unknown as Record<string, unknown>,
      {
        get(target, property) {
          reads.push(String(property));
          return target[property as string];
        },
      },
    );
    const result = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: recording },
      opsProxy as unknown as PlanningRunPlanControllerOps,
    );
    expect(result.compiled_plan.plan_revision).toBe(1);
    expect(reads.sort()).toEqual([
      "acceptCandidate",
      "compiledExecutionRoleFor",
      "compiledStageFor",
      "constructCandidate",
      "prepareProposal",
      "readAcceptedJsonOutput",
      "restorePlanningContext",
      "verifyCandidate",
    ]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 10. mutation after the first await cannot change the composition --------

test("10. mutating the caller's options after the first await cannot change the captured references", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    let release: (() => void) | undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ops: PlanningRunPlanControllerOps = {
      ...productionPlanningRunPlanOps,
      restorePlanningContext: async (pipelineArg, state, runRootArg) => {
        const result = await restorePipelineV2PlanningAcceptanceContext(pipelineArg, state, runRootArg);
        await gate;
        return result;
      },
    };
    const options: { pipeline: ResolvedPipelineV2; runRoot: string; sink: RecordingSink } = {
      pipeline,
      runRoot: dirs.runRoot,
      sink: recording,
    };
    const pending = acceptPipelineV2PlanningRunPlanInternal(options, ops);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    (options as unknown as Record<string, unknown>)["pipeline"] = null;
    (options as unknown as Record<string, unknown>)["runRoot"] = "/forged/other-run";
    (options as unknown as Record<string, unknown>)["sink"] = null;
    release?.();
    const result = await pending;
    expect(result.compiled_plan.plan_revision).toBe(1);
    expect(result.state === (recording.snapshot as PipelineV2RunState)).toBe(true);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 11. the pipeline clone/Proxy provenance gate ----------------------------

test("11. a cloned or Proxy pipeline is rejected through the provenance gate before any state read or facade call", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const stateReads: string[] = [];
    const stateProxy = new Proxy(recording.snapshot as unknown as Record<string, unknown>, {
      get(target, property) {
        stateReads.push(String(property));
        return target[property as string];
      },
    });
    const calls: string[] = [];
    const ops: PlanningRunPlanControllerOps = {
      ...productionPlanningRunPlanOps,
      restorePlanningContext: async (...args: Parameters<typeof restorePipelineV2PlanningAcceptanceContext>) => {
        calls.push("restore");
        return await restorePipelineV2PlanningAcceptanceContext(...args);
      },
    };
    // A structural clone of the pipeline: the gate rejects it.
    const clone = JSON.parse(JSON.stringify(pipeline)) as unknown;
    const cloneCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline: clone as never, runRoot: dirs.runRoot, sink: stateProxy as never },
      ops,
    ).catch((error) => error);
    expect(cloneCause).toBeInstanceOf(PipelineError);
    expect(cloneCause).not.toBeInstanceOf(PipelineV2PlanningRunPlanControllerError);
    expect(stateReads).toEqual([]);
    expect(calls).toEqual([]);

    // A Proxy pipeline: the gate rejects it with zero traps.
    let traps = 0;
    const pipelineProxy = new Proxy(pipeline as unknown as Record<string, unknown>, {
      get(target, property) {
        traps += 1;
        return target[property as string];
      },
    });
    const proxyCause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline: pipelineProxy as never, runRoot: dirs.runRoot, sink: recording },
      ops,
    ).catch((error) => error);
    expect(proxyCause).toBeInstanceOf(PipelineError);
    expect(traps).toBe(0);
    expect(calls).toEqual([]);
    expect(stateReads).toEqual([]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 12. the sink snapshot read count ----------------------------------------

test("12. the composition's own snapshot reads are exactly one initial and one post-acceptance verification read", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const counting = countingSnapshotSink(recording);
    let readsAtAcceptanceEntry = -1;
    let readsAtAcceptanceReturn = -1;
    const ops: PlanningRunPlanControllerOps = {
      ...productionPlanningRunPlanOps,
      acceptCandidate: async (options) => {
        readsAtAcceptanceEntry = counting.reads();
        const result = await acceptPipelineV2RunPlanCandidate(options);
        readsAtAcceptanceReturn = counting.reads();
        return result;
      },
    };
    const result = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: counting },
      ops,
    );
    expect(result.compiled_plan.plan_revision).toBe(1);
    // Exactly one initial read before the acceptance is ever entered.
    expect(readsAtAcceptanceEntry).toBe(1);
    // Exactly one post-acceptance verification read after it returns.
    expect(counting.reads() - readsAtAcceptanceReturn).toBe(1);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});

// --- 13. the export surfaces -------------------------------------------------

test("13. the runtime export surfaces are exactly the controller error and the composition function", async () => {
  const facade = await import("../src/pipeline_v2_planning_run_plan_controller.ts");
  expect(Object.keys(facade).sort()).toEqual([
    "PipelineV2PlanningRunPlanControllerError",
    "acceptPipelineV2PlanningRunPlan",
  ]);
  const internalModule = await import("../src/pipeline_v2_planning_run_plan_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2PlanningRunPlanControllerError",
    "acceptPipelineV2PlanningRunPlanInternal",
    "productionPlanningRunPlanOps",
  ]);
  const error = new PipelineV2PlanningRunPlanControllerError("invalid_result", "message");
  expect(error.reason).toBe("invalid_result");
  expect(error.name).toBe("PipelineV2PlanningRunPlanControllerError");
  expect(() => new PipelineV2PlanningRunPlanControllerError("no_such_reason" as never, "message")).toThrow(TypeError);
});

// --- 14. the source scan -----------------------------------------------------

test("14. the composition layer imports only the existing facades and resolvers and adds no second machinery", async () => {
  for (const name of [
    "pipeline_v2_planning_run_plan_controller.ts",
    "pipeline_v2_planning_run_plan_controller_internal.ts",
  ] as const) {
    const text = await readFile(join(import.meta.dir, "..", "src", name), "utf8");
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_coordinator\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_runner\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/main\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_state_store\.ts"/);
    expect(text, name).not.toMatch(/from "\.\/pipeline_v2_state_sink\.ts"/);
    expect(text, name).not.toMatch(/from "node:fs/);
    expect(text, name).not.toMatch(/from "node:fs\/promises"/);
    expect(text, name).not.toMatch(/writeFile|mkdir|rename\(|rm\(|chmod/);
    expect(text, name).not.toMatch(/validatePipelineV2RunState|reducePipelineV2RunCommand/);
    expect(text, name).not.toMatch(/canonicalJson|CryptoHasher|createHash/);
    expect(text, name).not.toMatch(/JSON\.parse/);
    expect(text, name).not.toMatch(/WeakMap|WeakSet/);
    expect(text, name).not.toMatch(/publishPipelineV2RunPlanCandidate\(|publishPipelineV2TaskRevision\(|publishPipelineV2PlanRevision\(/);
    expect(text, name).not.toMatch(/snapshotRunInputs\(|prepareActivationData\(|acceptActivationOutputs\(/);
    expect(text, name).not.toMatch(/preparePipelineV2RunPlanProposal\(/);
    expect(text, name).not.toMatch(/constructPipelineV2RunPlanCandidateFromProposalInternal/);
  }
  // The internal core: exactly the eight composed facades/resolvers.
  const internal = await readFile(
    join(import.meta.dir, "..", "src", "pipeline_v2_planning_run_plan_controller_internal.ts"),
    "utf8",
  );
  for (const name of [
    "restorePipelineV2PlanningAcceptanceContext",
    "compiledExecutionRoleFor",
    "readAcceptedJsonOutput",
    "preparePipelineV2RunPlanProposal",
    "constructPipelineV2RunPlanCandidateFromProposal",
    "verifyPipelineV2RunPlanCandidateForAcceptance",
    "acceptPipelineV2RunPlanCandidate",
    "compiledPipelineV2RunPlanStageFor",
    "requireResolvedPipelineV2Provenance",
  ]) {
    expect(internal, name).toContain(name);
  }
});

test("12b. an acceptance failure performs no post-failure snapshot reads", async () => {
  const prefix = await setupPlanningPrefix();
  const { dirs, pipeline, recording } = prefix;
  try {
    const counting = countingSnapshotSink(recording);
    const acceptanceError = new PipelineV2RunPlanControllerError("state_persist_failed", "the acceptance exploded", null);
    const cause = await acceptPipelineV2PlanningRunPlanInternal(
      { pipeline, runRoot: dirs.runRoot, sink: counting },
      { ...productionPlanningRunPlanOps, acceptCandidate: async () => { throw acceptanceError; } },
    ).catch((error) => error);
    expect(cause).toBe(acceptanceError);
    // Only the initial read; the failed path never reads again.
    expect(counting.reads()).toBe(1);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
});
