import { describe, expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunInputState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateDurabilityError, PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunStatePath } from "../src/pipeline_v2_state_store.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  publishPipelineV2RunPlanCandidateWithOps,
  type PipelineV2RunPlanCandidatePublicationOps,
} from "../src/pipeline_v2_run_plan_candidate_internal.ts";
import { publishPipelineV2PlanRevision, publishPipelineV2TaskRevision, PipelineV2RunPlanStoreError } from "../src/pipeline_v2_run_plan_store.ts";
import {
  PipelineV2RunPlanAcceptanceError,
  verifyPipelineV2RunPlanCandidateForAcceptance,
} from "../src/pipeline_v2_run_plan_acceptance.ts";
import {
  PipelineV2CompiledRunPlanError,
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import {
  acceptPipelineV2RunPlanCandidate,
  PipelineV2RunPlanControllerError,
  type PipelineV2RunPlanControllerSink,
} from "../src/pipeline_v2_run_plan_controller.ts";
import {
  acceptPipelineV2RunPlanCandidateCore,
  realPipelineV2RunPlanControllerOps,
  type PipelineV2RunPlanControllerOps,
} from "../src/pipeline_v2_run_plan_controller_internal.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const CANARY = "CANARY_secret_task_body";
const INTENT = hex("3");
const PLAN_WAIT_REQUEST = hex("1");
const PLAN_WAIT_RESPONSE = hex("2");

const FACTS_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const DISPATCH_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: d_next_stage
  - id: d_plan_complete
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
  - id: r_done
    when:
      fact: f1
      equals: false
    decision: d_plan_complete
`;

const DISPATCH_MODEL_YAML_TWO_TEMPLATES = `schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: d_next_stage
  - id: d_test_stage
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
  - id: r_test
    when:
      all:
        - fact: f1
          equals: false
        - fact: f2
          equals: true
    decision: d_test_stage
`;

/** Planning agent -> control dispatcher -> one development stage. */
const ORCHESTRATED_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: dev_entry
      - outcome: d_plan_complete
        to: done
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

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
  - id: failed
    type: terminal
    result: failed
`;

/** Planning agent -> control dispatcher -> development stage then testing stage. */
const TWO_TEMPLATES_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
    - id: testing
      entry_state: test_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: dev_entry
      role: stage
      stage_template: development
    - state_id: test_entry
      role: stage
      stage_template: testing

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: dev_entry
      - outcome: d_test_stage
        to: test_entry
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

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

  - id: test_entry
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
  - id: failed
    type: terminal
    result: failed
`;

async function withPipeline(
  fn: (pipeline: ResolvedPipelineV2) => Promise<void>,
  yaml: string = ORCHESTRATED_YAML,
  dispatchModelYaml: string = DISPATCH_MODEL_YAML,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-controller-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), yaml);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(bundle, "decisions", "dispatch.yaml"), dispatchModelYaml);
    await fn(await loadPipelineV2(bundle));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface RunFixture {
  root: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-plan-controller-run-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  return { root, stateRoot, runRoot, statePath: join(runRoot, "state.json") };
}

async function disposeRun(fixture: RunFixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 8, 25, 0, 0, clockCounter));
}

const BASE_INPUTS: readonly PipelineV2RunInputState[] = [
  { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
];

let sessionCounter = 0;

/** The six agent-run commands between `start_agent_execution` and the settled unbound phase. */
function agentPhases(label: string): PipelineV2RunCommand[] {
  sessionCounter += 1;
  const n = sessionCounter;
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${n}-${label}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${n}-${label}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
    { kind: "agent_cleanup_completed" },
  ];
}

/** Drives a fresh real sink to the planning acceptance boundary (settled unbound planning execution). */
async function playPlanning(
  sink: PipelineV2RunStateSink,
  pipeline: ResolvedPipelineV2,
): Promise<PipelineV2RunState> {
  const identity = pipelineV2RunPipelineIdentity(pipeline);
  await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: identity, inputs: BASE_INPUTS });
  await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
  for (const command of agentPhases("planning")) {
    await sink.dispatch(command);
  }
  return sink.snapshot as PipelineV2RunState;
}

function taskValue(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = RUN_ID,
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision,
    previous_sha256: previousSha256,
    origin,
    body,
  };
}

function preparedTask(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = RUN_ID,
): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest(taskValue(taskId, revision, previousSha256, origin, body, runId));
}

const A1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
const B1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");
const C1 = preparedTask("task-c", 1, null, "planning_proposal", "Body C one");

interface StageSpec {
  readonly id: string;
  readonly template: string;
  readonly tasks: readonly {
    readonly id: string;
    readonly revision: number;
    readonly sha256: string;
    readonly depends_on: readonly string[];
  }[];
}

function preparedPlan(
  stages: readonly StageSpec[],
  overrides: Partial<{ revision: number; previousSha256: string | null; originExecution: number }> = {},
): PreparedPipelineV2RunPlanRevision {
  return preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: overrides.revision ?? 1,
    previous_sha256: overrides.previousSha256 === undefined ? null : overrides.previousSha256,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: overrides.originExecution ?? 1,
    stages,
  });
}

function revisionOneCandidate(
  stages: readonly StageSpec[],
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  originExecution = 1,
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidate({
    plan: preparedPlan(stages, { originExecution }),
    taskRevisions,
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

function revisionTwoCandidate(
  stages: readonly StageSpec[],
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  previousPlan: PreparedPipelineV2RunPlanRevision,
  previousTaskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  originExecution: number,
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidate({
    plan: preparedPlan(stages, {
      revision: previousPlan.manifest.revision + 1,
      previousSha256: previousPlan.sha256,
      originExecution,
    }),
    taskRevisions,
    previousPlan,
    previousTaskRevisions,
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

const SINGLE_STAGE: readonly StageSpec[] = [
  {
    id: "stage-1",
    template: "development",
    tasks: [
      { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
      { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
    ],
  },
];

const TWO_STAGE: readonly StageSpec[] = [
  {
    id: "stage-1",
    template: "development",
    tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
  },
  {
    id: "stage-2",
    template: "testing",
    tasks: [{ id: "task-c", revision: 1, sha256: C1.sha256, depends_on: [] }],
  },
];

/**
 * A delegating controller sink over the real sink: records every
 * dispatched command and proves the loader round-trip of the durable
 * document after every durable step.
 */
interface Recording {
  readonly sink: PipelineV2RunPlanControllerSink;
  readonly commands: PipelineV2RunCommand[];
}

function recordingSink(
  inner: PipelineV2RunStateSink,
  fixture: RunFixture,
  validateEachStep: boolean,
): Recording {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    sink: {
      get snapshot() {
        return inner.snapshot;
      },
      get poisoned() {
        return inner.poisoned;
      },
      async dispatch(command: PipelineV2RunCommand) {
        commands.push(command);
        await inner.dispatch(command);
        if (validateEachStep) {
          const raw = await readFile(fixture.statePath, "utf8");
          validatePipelineV2RunState(JSON.parse(raw));
        }
      },
    },
  };
}

function expectControllerError(
  cause: unknown,
  reason: "invalid_state" | "candidate_conflict" | "state_persist_failed",
): PipelineV2RunPlanControllerError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanControllerError);
  const error = cause as PipelineV2RunPlanControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

async function catchControllerCall(
  fn: () => Promise<unknown>,
): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("expected the controller call to reject");
}

async function fileIdentity(
  path: string,
): Promise<{ dev: number; ino: number; mode: number; mtimeMs: number; bytes: string }> {
  const info = await lstat(path);
  return {
    dev: info.dev,
    ino: info.ino,
    mode: info.mode & 0o777,
    mtimeMs: info.mtimeMs,
    bytes: (await readFile(path)).toString("utf8"),
  };
}

/** All paths under `<runRoot>/run-plan`, deterministically sorted; empty when the tree is absent. */
async function runPlanPaths(runRoot: string): Promise<string[]> {
  const base = join(runRoot, "run-plan");
  const out: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        out.push(`${relative}/`);
        await walk(join(dir, entry.name), relative);
      } else {
        out.push(relative);
      }
    }
  };
  await walk(base, "");
  return out.sort();
}

async function expectNoRunPlan(runRoot: string): Promise<void> {
  let caught: unknown = null;
  try {
    await readdir(join(runRoot, "run-plan"));
  } catch (cause) {
    caught = cause;
  }
  expect(caught).toBeInstanceOf(Error);
  expect((caught as Error & { code?: string }).code).toBe("ENOENT");
}

function taskManifestPath(fixture: RunFixture, taskId: string, revision: number): string {
  return join(fixture.runRoot, "run-plan", "tasks", taskId, `${revision}.json`);
}

function planManifestPath(fixture: RunFixture, revision: number): string {
  return join(fixture.runRoot, "run-plan", "plans", `${revision}.json`);
}

describe("pipeline v2 run plan acceptance controller", () => {
  test("1. revision-1 happy path: publication order, ledger order and loader round-trips", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, true);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: recording.sink,
          candidate,
        });
        // the task manifests then the plan manifest as the commit marker
        expect(await readFile(taskManifestPath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
        expect(await readFile(taskManifestPath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
        expect(await readFile(planManifestPath(fixture, 1), "utf8")).toBe(candidate.plan.canonical_json);
        // the controller dispatched exactly the missing sequence in candidate order
        expect(recording.commands).toEqual([
          { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: A1.sha256 },
          { kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: B1.sha256 },
          { kind: "plan_revision_accepted", planRevision: 1, planSha256: candidate.plan.sha256, originExecution: 1 },
        ]);
        // the durable task ledger is in candidate order, the plan record last
        const state = recording.sink.snapshot as PipelineV2RunState;
        expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
          "task-a@1",
          "task-b@1",
        ]);
        expect(state.plan_revisions).toEqual([
          { index: 1, revision: 1, sha256: candidate.plan.sha256, previous_sha256: null, origin_execution: 1 },
        ]);
        // the result carries the exact compiled plan projection and the last authoritative state
        expect(result.state).toBe(state);
        expect(result.compiled_plan).toEqual({
          run_id: RUN_ID,
          plan_revision: 1,
          plan_sha256: candidate.plan.sha256,
          origin_execution: 1,
          stages: [
            {
              id: "stage-1",
              template: "development",
              entry_state: "dev_entry",
              state_ids: ["dev_entry"],
              tasks: [
                { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
        });
        // the durable document loader round-trips
        validatePipelineV2RunState(JSON.parse(await readFile(fixture.statePath, "utf8")));
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("2. a multi-stage candidate keeps the deterministic candidate order across stages", async () => {
    await withPipeline(
      async (pipeline) => {
        const fixture = await setupRun();
        try {
          const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
          await playPlanning(sink, pipeline);
          const recording = recordingSink(sink, fixture, true);
          const candidate = revisionOneCandidate(TWO_STAGE, [A1, C1]);
          const result = await acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate,
          });
          expect(recording.commands.map((command) => (command.kind === "task_revision_accepted" ? command.taskId : command.kind))).toEqual([
            "task-a",
            "task-c",
            "plan_revision_accepted",
          ]);
          const state = result.state;
          expect(state.task_revisions.map((record) => record.task_id)).toEqual(["task-a", "task-c"]);
          expect(result.compiled_plan.stages.map((stage) => stage.id)).toEqual(["stage-1", "stage-2"]);
          expect(result.compiled_plan.stages.map((stage) => stage.template)).toEqual(["development", "testing"]);
        } finally {
          await disposeRun(fixture);
        }
      },
      TWO_TEMPLATES_YAML,
      DISPATCH_MODEL_YAML_TWO_TEMPLATES,
    );
  });

  test("3. a revision-2 plan skips the durable user-response task and accepts only the missing records", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink,
          candidate: first,
        });
        // the user revise flow: wait, intent, user-response task revision 2, response
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
          executionIndex: 1,
        });
        await sink.dispatch({
          kind: "run_waiting",
          stateId: "stage_dispatch",
          reason: "stage_iteration_limit_exhausted",
          requestSha256: PLAN_WAIT_REQUEST,
          actions: [{ id: "revise_task", to: "architect" }],
        });
        const A2 = preparedTask("task-a", 2, A1.sha256, "user_response", "Body A two");
        await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT });
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: A2.sha256,
          waitIndex: 1,
          intentSha256: INTENT,
        });
        await sink.dispatch({
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: PLAN_WAIT_REQUEST,
          actionId: "revise_task",
          responseSha256: PLAN_WAIT_RESPONSE,
        });
        // the planning execution restarts on the response target
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
        for (const command of agentPhases("replanning")) {
          await sink.dispatch(command);
        }
        // the revision-2 candidate: task-a at its durable revision 2 plus a new revision-1 task
        const second = revisionTwoCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          [A2, B1],
          first.plan,
          [A1],
          2,
        );
        const recording = recordingSink(sink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: recording.sink,
          candidate: second,
        });
        // only the missing records: the new revision-1 task, then the plan
        expect(recording.commands).toEqual([
          { kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: B1.sha256 },
          { kind: "plan_revision_accepted", planRevision: 2, planSha256: second.plan.sha256, originExecution: 2 },
        ]);
        const state = result.state;
        expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
          "task-a@1",
          "task-a@2",
          "task-b@1",
        ]);
        expect(state.plan_revisions.map((record) => record.revision)).toEqual([1, 2]);
        expect(await readFile(planManifestPath(fixture, 2), "utf8")).toBe(second.plan.canonical_json);
        expect(await readFile(taskManifestPath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("4. a candidate that fails compilation or acceptance has zero filesystem and zero dispatch effects", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        // a hand-built candidate is rejected by the compiled layer before anything
        const handBuilt = {
          plan: preparedPlan(SINGLE_STAGE),
          task_revisions: [A1, B1],
        } as unknown as PreparedPipelineV2RunPlanCandidate;
        const compiledCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate: handBuilt,
          }),
        );
        expect(compiledCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        // an origin beyond the last execution is rejected by the acceptance verifier
        const staleOrigin = revisionOneCandidate(SINGLE_STAGE, [A1, B1], 99);
        const originCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate: staleOrigin,
          }),
        );
        expect(originCause).toBeInstanceOf(PipelineV2RunPlanAcceptanceError);
        expect((originCause as PipelineV2RunPlanAcceptanceError).reason).toBe("origin_execution_mismatch");
        // zero filesystem effects and zero durable dispatch for both failures
        expect(recording.commands).toEqual([]);
        await expectNoRunPlan(fixture.runRoot);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("5. a reducer pre-check failure performs no publication and no dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        // a stage generation and its iteration open on the active boundary
        // (the anchor is the cursor count 0; the settled planning execution
        // stays unbound) — the acceptance verifier passes, but the reducer
        // rejects a plan acceptance while a stage iteration is open
        await sink.dispatch({
          kind: "stage_generation_opened",
          stageId: "stage-1",
          stagePosition: 1,
          templateId: "development",
          planSha256: first.plan.sha256,
          initialBudget: 2,
          transitionCount: 0,
        });
        await sink.dispatch({ kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 });
        const recording = recordingSink(sink, fixture, false);
        const second = revisionTwoCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-c", revision: 1, sha256: C1.sha256, depends_on: [] }],
            },
          ],
          [C1],
          first.plan,
          [],
          1,
        );
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate: second,
          }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain("the current run state does not accept the missing durable sequence");
        expect(error.state).toBe(sink.snapshot);
        expect(recording.commands).toEqual([]);
        // no task-c manifest and no plan-2 manifest were published
        const paths = await runPlanPaths(fixture.runRoot);
        expect(paths).toEqual(["plans/", "plans/1.json", "tasks/", "tasks/task-a/", "tasks/task-a/1.json", "tasks/task-b/", "tasks/task-b/1.json"]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("6. a task publication failure dispatches nothing and leaves the earlier task artifact as an orphan", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
                { id: "task-c", revision: 1, sha256: C1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          [A1, B1, C1],
        );
        const injected = new PipelineV2RunPlanStoreError("not_published", "io_failure", "injected task publication failure");
        const failingOps: PipelineV2RunPlanCandidatePublicationOps = {
          publishTaskRevision: async (runRoot, value) => {
            const taskId = (value as { task_id: string }).task_id;
            if (taskId === "task-b") {
              throw injected;
            }
            return await publishPipelineV2TaskRevision(runRoot, value);
          },
          publishPlanRevision: publishPipelineV2PlanRevision,
        };
        const controllerOps: PipelineV2RunPlanControllerOps = {
          verifyCandidateForAcceptance: verifyPipelineV2RunPlanCandidateForAcceptance,
          publishCandidate: (runRoot, value) => publishPipelineV2RunPlanCandidateWithOps(failingOps, runRoot, value),
        };
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidateCore(controllerOps, {
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate,
          }),
        );
        // the store error keeps its original class and identity
        expect(cause).toBe(injected);
        expect(recording.commands).toEqual([]);
        // the published task-a artifact stays as an immutable orphan; no plan manifest
        expect(await readFile(taskManifestPath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
        let planCaught: unknown = null;
        try {
          await readFile(planManifestPath(fixture, 1), "utf8");
        } catch (planError) {
          planCaught = planError;
        }
        expect((planCaught as Error & { code?: string }).code).toBe("ENOENT");
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("7. a plan publication failure dispatches nothing; the published task orphans remain", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const injected = new PipelineV2RunPlanStoreError("not_published", "conflict", "injected plan publication conflict");
        const failingOps: PipelineV2RunPlanCandidatePublicationOps = {
          publishTaskRevision: publishPipelineV2TaskRevision,
          publishPlanRevision: async () => {
            throw injected;
          },
        };
        const controllerOps: PipelineV2RunPlanControllerOps = {
          verifyCandidateForAcceptance: verifyPipelineV2RunPlanCandidateForAcceptance,
          publishCandidate: (runRoot, value) => publishPipelineV2RunPlanCandidateWithOps(failingOps, runRoot, value),
        };
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidateCore(controllerOps, {
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate,
          }),
        );
        expect(cause).toBe(injected);
        expect(recording.commands).toEqual([]);
        expect(await readFile(taskManifestPath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
        expect(await readFile(taskManifestPath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
        let planCaught: unknown = null;
        try {
          await readFile(planManifestPath(fixture, 1), "utf8");
        } catch (planError) {
          planCaught = planError;
        }
        expect((planCaught as Error & { code?: string }).code).toBe("ENOENT");
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("8. a partial durable task prefix is skipped by the retry; the suffix and the plan are appended", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        // 8 planning commits + task-a (9); the controller's task-b commit fails
        const sink = new PipelineV2RunStateSink({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 10, failStep: "rename" }),
        });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        expectControllerError(cause, "state_persist_failed");
        expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted", "task_revision_accepted"]);
        const persisted = JSON.parse(await readFile(fixture.statePath, "utf8")) as PipelineV2RunState;
        // the durable prefix is exactly task-a; task-b and the plan never committed
        expect(persisted.task_revisions.map((record) => record.task_id)).toEqual(["task-a"]);
        expect(persisted.plan_revisions).toEqual([]);
        // the fresh retry recognizes the durable task-a prefix, skips it, and
        // dispatches only the missing suffix and the plan
        const retryRecording = recordingSink(sink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: retryRecording.sink,
          candidate,
        });
        expect(retryRecording.commands).toEqual([
          { kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: B1.sha256 },
          { kind: "plan_revision_accepted", planRevision: 1, planSha256: candidate.plan.sha256, originExecution: 1 },
        ]);
        expect(result.state.task_revisions.map((record) => record.task_id)).toEqual(["task-a", "task-b"]);
        expect(result.state.plan_revisions).toHaveLength(1);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("9. not_committed on the first task: state unchanged; a fresh retry completes the acceptance", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        // 8 planning commits; the controller's task-a commit (9) fails
        const sink = new PipelineV2RunStateSink({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 9, failStep: "write" }),
        });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        const error = expectControllerError(cause, "state_persist_failed");
        // the previous snapshot stays authoritative: no task or plan record is durable
        const persisted = JSON.parse(await readFile(fixture.statePath, "utf8")) as PipelineV2RunState;
        expect(persisted.task_revisions).toEqual([]);
        expect(persisted.plan_revisions).toEqual([]);
        expect(error.state).toEqual(persisted);
        expect(recording.commands.map((command) => command.kind)).toEqual(["task_revision_accepted"]);
        // the whole candidate's manifests were published before any dispatch:
        // task-a, task-b and the plan manifest exist; the plan manifest is the
        // filesystem commit marker, published before the durable acceptance
        expect(await readFile(taskManifestPath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
        expect(await readFile(taskManifestPath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
        expect(await readFile(planManifestPath(fixture, 1), "utf8")).toBe(candidate.plan.canonical_json);
        // the fresh retry reuses the published manifests and completes the acceptance
        const retryRecording = recordingSink(sink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: retryRecording.sink,
          candidate,
        });
        expect(retryRecording.commands.map((command) => (command.kind === "task_revision_accepted" ? command.taskId : command.kind))).toEqual([
          "task-a",
          "task-b",
          "plan_revision_accepted",
        ]);
        expect(result.state.plan_revisions).toHaveLength(1);
        expect(await readFile(planManifestPath(fixture, 1), "utf8")).toBe(candidate.plan.canonical_json);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("10. not_committed on the plan: all task records durable; the retry dispatches only the plan", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        // 8 planning commits + task-a (9) + task-b (10); the plan commit (11) fails
        const sink = new PipelineV2RunStateSink({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 11, failStep: "rename" }),
        });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        expectControllerError(cause, "state_persist_failed");
        const persisted = JSON.parse(await readFile(fixture.statePath, "utf8")) as PipelineV2RunState;
        expect(persisted.task_revisions.map((record) => record.task_id)).toEqual(["task-a", "task-b"]);
        expect(persisted.plan_revisions).toEqual([]);
        expect(await readFile(planManifestPath(fixture, 1), "utf8")).toBe(candidate.plan.canonical_json);
        // the fresh retry recognizes the durable task prefix and dispatches only the plan
        const retryRecording = recordingSink(sink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: retryRecording.sink,
          candidate,
        });
        expect(retryRecording.commands).toEqual([
          { kind: "plan_revision_accepted", planRevision: 1, planSha256: candidate.plan.sha256, originExecution: 1 },
        ]);
        expect(result.state.plan_revisions).toEqual([
          { index: 1, revision: 1, sha256: candidate.plan.sha256, previous_sha256: null, origin_execution: 1 },
        ]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("11. durability_unknown on a task: the adopted candidate is authoritative, the sink is poisoned, no further dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 9, failStep: "dirfsync" }),
        });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        const error = expectControllerError(cause, "state_persist_failed");
        expect(sink.poisoned).toBe(true);
        // the adopted candidate snapshot carries the task-a record
        const adopted = sink.snapshot as PipelineV2RunState;
        expect(adopted.task_revisions.map((record) => record.task_id)).toEqual(["task-a"]);
        expect(error.state).toBe(adopted);
        // no further dispatch happened (no task-b, no plan)
        expect(recording.commands.map((command) => (command.kind === "task_revision_accepted" ? command.taskId : command.kind))).toEqual(["task-a"]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("12. durability_unknown on the plan: the adopted plan record is authoritative; a fresh retry recognizes exact durable success", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
          io: faultIo({ failCommit: 11, failStep: "dirfsync" }),
        });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        expectControllerError(cause, "state_persist_failed");
        expect(sink.poisoned).toBe(true);
        const adopted = sink.snapshot as PipelineV2RunState;
        expect(adopted.plan_revisions.map((record) => record.revision)).toEqual([1]);
        // the fresh sink loads the adopted durable candidate from disk
        const freshSink = await PipelineV2RunStateSink.open({
          stateRoot: fixture.stateRoot,
          runId: RUN_ID,
          now: nextTick,
        });
        const retryRecording = recordingSink(freshSink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: retryRecording.sink,
          candidate,
        });
        expect(retryRecording.commands).toEqual([]);
        expect(result.state.plan_revisions).toEqual(adopted.plan_revisions);
        expect(result.state.task_revisions.map((record) => record.task_id)).toEqual(["task-a", "task-b"]);
        expect(result.compiled_plan.plan_revision).toBe(1);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("13. an exact full retry dispatches nothing and preserves every manifest identity", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const first = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
        const paths = [
          taskManifestPath(fixture, "task-a", 1),
          taskManifestPath(fixture, "task-b", 1),
          planManifestPath(fixture, 1),
        ];
        const identitiesBefore = await Promise.all(paths.map(fileIdentity));
        const retryRecording = recordingSink(sink, fixture, true);
        const second = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: retryRecording.sink,
          candidate,
        });
        expect(retryRecording.commands).toEqual([]);
        const identitiesAfter = await Promise.all(paths.map(fileIdentity));
        expect(identitiesAfter).toEqual(identitiesBefore);
        // the same compiled plan projection (each controller call verifies
        // through the same acceptance chain; the projection is structurally
        // identical, the object freshly compiled per call)
        expect(second.compiled_plan).toEqual(first.compiled_plan);
        expect(second.state.plan_revisions).toEqual(first.state.plan_revisions);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("14. a durable task revision with the same id and revision but a different chain is a conflict without writes", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
        const stateBefore = sink.snapshot as PipelineV2RunState;
        const digestBefore = JSON.stringify(stateBefore.task_revisions);
        // a candidate that re-declares task-a at revision 1 with a different body digest
        const A1_OTHER = preparedTask("task-a", 1, null, "planning_proposal", "Body A other");
        const conflicting = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 1, sha256: A1_OTHER.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          [A1_OTHER, B1],
        );
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate: conflicting }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain('the durable task revision 1 of "task-a" does not match the candidate chain');
        expect(error.state).toBe(stateBefore);
        expect(recording.commands).toEqual([]);
        expect(JSON.stringify((sink.snapshot as PipelineV2RunState).task_revisions)).toBe(digestBefore);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("15. a durable plan revision with different content is a conflict", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        const stateBefore = sink.snapshot as PipelineV2RunState;
        // a same-run candidate at the same plan revision with a different plan
        // digest (an extra declared task changes the canonical plan snapshot)
        const differentPlan = preparedPlan(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          { originExecution: 1 },
        );
        const conflicting = preparePipelineV2RunPlanCandidate({
          plan: differentPlan,
          taskRevisions: [A1, B1],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        expect(conflicting.plan.sha256).not.toBe(first.plan.sha256);
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate: conflicting }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain("the durable plan revision 1 does not match the candidate plan");
        expect(error.state).toBe(stateBefore);
        expect(recording.commands).toEqual([]);
        expect((sink.snapshot as PipelineV2RunState).plan_revisions).toEqual(stateBefore.plan_revisions);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("16. a candidate revision ahead of the durable plan ledger is an ahead/gap conflict", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        // a candidate for plan revision 3 while the durable ledger is at
        // revision 1: the chains are internally coherent (r3 chains a
        // prepared r2) but the candidate revision is ahead of the ledger
        // (next expected 2) — an ahead/gap conflict, not a stale one
        const planR2 = preparedPlan(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          { revision: 2, previousSha256: first.plan.sha256, originExecution: 1 },
        );
        const planR3 = preparedPlan(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          { revision: 3, previousSha256: planR2.sha256, originExecution: 1 },
        );
        const stale = preparePipelineV2RunPlanCandidate({
          plan: planR3,
          taskRevisions: [A1],
          previousPlan: planR2,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate: stale }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain("the candidate plan revision 3 is ahead of the durable plan ledger; the next expected revision is 2");
        expect(error.message).not.toContain("past the candidate");
        expect(error.state).toBe(sink.snapshot);
        expect(recording.commands).toEqual([]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("17. missing revision>1 tasks on the active planning boundary are conflicts, not dispatches", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const A2 = preparedTask("task-a", 2, A1.sha256, "user_response", "Body A two");
        const candidate = preparePipelineV2RunPlanCandidate({
          plan: preparedPlan([
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
            },
          ]),
          taskRevisions: [A2],
          previousPlan: null,
          previousTaskRevisions: [A1],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const recording = recordingSink(sink, fixture, false);
        // no durable record for the task at all: the user-response revision
        // cannot be created on the active planning boundary
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain(
          'the durable ledger records no task revision 2 of "task-a"; a user-response revision cannot be created on the active planning boundary',
        );
        expect(recording.commands).toEqual([]);
        // the same shape with a durable lower revision of the same task id:
        // the candidate declares task-a at revision 2 with its prepared
        // predecessor while only revision 1 is durable
        const withPrefix = preparePipelineV2RunPlanCandidate({
          plan: preparedPlan([
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
            },
          ]),
          taskRevisions: [A2],
          previousPlan: null,
          previousTaskRevisions: [A1],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        await sink.dispatch({ kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: A1.sha256 });
        const recording2 = recordingSink(sink, fixture, false);
        const cause2 = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording2.sink, candidate: withPrefix }),
        );
        const error2 = expectControllerError(cause2, "candidate_conflict");
        expect(error2.message).toContain(
          'the durable task ledger already records a different revision of task "task-a" than the candidate declares',
        );
        expect(recording2.commands).toEqual([]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("18. identical in-process concurrency: both calls succeed, each durable record appears exactly once", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const settled = await Promise.allSettled([
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate }),
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate }),
        ]);
        for (const entry of settled) {
          expect(entry.status).toBe("fulfilled");
        }
        const state = sink.snapshot as PipelineV2RunState;
        expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
          "task-a@1",
          "task-b@1",
        ]);
        expect(state.plan_revisions).toHaveLength(1);
        validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("19. conflicting concurrency: one winner accepts, the loser fails as a typed store conflict", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const planA = preparedPlan([
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
              { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
            ],
          },
        ]);
        const planB = preparedPlan([
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
              { id: "task-c", revision: 1, sha256: C1.sha256, depends_on: ["task-a"] },
            ],
          },
        ]);
        const candidateA = preparePipelineV2RunPlanCandidate({
          plan: planA,
          taskRevisions: [A1, B1],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const candidateB = preparePipelineV2RunPlanCandidate({
          plan: planB,
          taskRevisions: [A1, C1],
          previousPlan: null,
          previousTaskRevisions: [],
          protectedInputDigest: PROTECTED_DIGEST,
        });
        const settled = await Promise.allSettled([
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidateA }),
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: candidateB }),
        ]);
        const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
        const rejected = settled.filter((entry) => entry.status === "rejected");
        expect(fulfilled).toHaveLength(1);
        expect(rejected).toHaveLength(1);
        const loser = (rejected[0] as PromiseRejectedResult).reason;
        expect(loser).toBeInstanceOf(PipelineV2RunPlanStoreError);
        expect((loser as PipelineV2RunPlanStoreError).outcome).toBe("not_published");
        expect((loser as PipelineV2RunPlanStoreError).reason).toBe("conflict");
        // the accepted state carries exactly one coherent candidate's records
        const state = sink.snapshot as PipelineV2RunState;
        expect(state.plan_revisions).toHaveLength(1);
        const winnerPlanBytes = await readFile(planManifestPath(fixture, 1), "utf8");
        expect(state.plan_revisions[0]?.sha256).toBe(
          winnerPlanBytes === planA.canonical_json ? planA.sha256 : planB.sha256,
        );
        expect(state.task_revisions.every((record) => record.revision === 1)).toBe(true);
        expect(state.task_revisions.map((record) => record.task_id)).toEqual(
          winnerPlanBytes === planA.canonical_json ? ["task-a", "task-b"] : ["task-a", "task-c"],
        );
        validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("20. a hostile sink that resolves without the expected snapshot change fails closed", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const inner = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(inner, pipeline);
        const frozenState = inner.snapshot as PipelineV2RunState;
        let dispatchCalls = 0;
        const hostileSink: PipelineV2RunPlanControllerSink = {
          get snapshot() {
            return frozenState;
          },
          get poisoned() {
            return false;
          },
          dispatch: async () => {
            dispatchCalls += 1;
          },
        };
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: hostileSink, candidate }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain('the committed run state does not carry the task revision 1 of "task-a"');
        expect(error.state).toBe(frozenState);
        expect(dispatchCalls).toBe(1);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("21. a poisoned sink is rejected before the acceptance verification, publication and dispatch", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        let dispatchCalls = 0;
        const poisonedSink: PipelineV2RunPlanControllerSink = {
          snapshot: null,
          poisoned: true,
          dispatch: async () => {
            dispatchCalls += 1;
          },
        };
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        // the pipeline is not even provenance-checked: the capture boundary
        // fires before the acceptance verifier
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline: { bundleRoot: "/nowhere" } as unknown as ResolvedPipelineV2,
            runRoot: fixture.runRoot,
            sink: poisonedSink,
            candidate,
          }),
        );
        const error = expectControllerError(cause, "invalid_state");
        expect(error.message).toContain("the run state sink is poisoned by a durability-unknown commit");
        expect(error.state).toBeNull();
        expect(dispatchCalls).toBe(0);
        await expectNoRunPlan(fixture.runRoot);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("22. every options field and the sink dispatch accessor are captured exactly once", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        // a Proxy over the options records every property read
        const reads: string[] = [];
        const hostileOptions = new Proxy(
          { pipeline, runRoot: fixture.runRoot, sink, candidate },
          {
            get(target, property, receiver) {
              reads.push(String(property));
              return Reflect.get(target, property, receiver);
            },
          },
        );
        const result = await acceptPipelineV2RunPlanCandidate(hostileOptions);
        expect(result.compiled_plan.plan_revision).toBe(1);
        expect(reads.sort()).toEqual(["candidate", "pipeline", "runRoot", "sink"]);
        // a sink whose dispatch accessor throws on a second read still succeeds
        const guardFixture = await setupRun();
        try {
          const inner = new PipelineV2RunStateSink({ stateRoot: guardFixture.stateRoot, runId: RUN_ID, now: nextTick });
          await playPlanning(inner, pipeline);
          let dispatchReads = 0;
          const guardedSink: PipelineV2RunPlanControllerSink = {
            get snapshot() {
              return inner.snapshot;
            },
            get poisoned() {
              return inner.poisoned;
            },
            get dispatch() {
              dispatchReads += 1;
              if (dispatchReads > 1) {
                throw new Error("the dispatch accessor was read a second time");
              }
              return (command: PipelineV2RunCommand) => inner.dispatch(command);
            },
          };
          const guardedCandidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
          const guardedResult = await acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: guardFixture.runRoot,
            sink: guardedSink,
            candidate: guardedCandidate,
          });
          expect(guardedResult.state.plan_revisions).toHaveLength(1);
          expect(dispatchReads).toBe(1);
        } finally {
          await disposeRun(guardFixture);
        }
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("23. the pipeline and candidate provenance gates run with zero Proxy traps and zero effects", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const recording = recordingSink(sink, fixture, false);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        // a Proxy pipeline: the provenance gate rejects before any getter or trap
        let pipelineTraps = 0;
        const proxyPipeline = new Proxy(pipeline, {
          get(target, property, receiver) {
            pipelineTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        const pipelineCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline: proxyPipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate,
          }),
        );
        expect(pipelineCause).toBeInstanceOf(PipelineError);
        expect(pipelineTraps).toBe(0);
        // a Proxy candidate: the compiled layer rejects before any trap
        let candidateTraps = 0;
        const proxyCandidate = new Proxy(candidate, {
          get(target, property, receiver) {
            candidateTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        const candidateCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: recording.sink,
            candidate: proxyCandidate,
          }),
        );
        expect(candidateCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        expect(candidateTraps).toBe(0);
        // zero durable dispatch and zero filesystem effects for both
        expect(recording.commands).toEqual([]);
        await expectNoRunPlan(fixture.runRoot);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("24. the result is deep-frozen and the caller objects are neither mutated nor frozen", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const options = { pipeline, runRoot: fixture.runRoot, sink, candidate };
        const result = await acceptPipelineV2RunPlanCandidate(options);
        expect(Object.isFrozen(result)).toBe(true);
        expect(Object.isFrozen(result.compiled_plan)).toBe(true);
        expect(Object.isFrozen(result.state)).toBe(true);
        expect(Object.isFrozen(options)).toBe(false);
        expect(Object.isFrozen(candidate)).toBe(true); // frozen by its own preparation, not by the controller
        // the compiled plan is the exact verifier object: it is provenance-backed
        // (the stage lookup accepts it) and structurally identical to a fresh verify
        const stage = result.compiled_plan.stages[0];
        if (stage === undefined) {
          throw new Error("missing stage");
        }
        const compiledStage = compiledPipelineV2RunPlanStageFor(result.compiled_plan, "stage-1");
        expect(compiledStage).toBe(stage);
        expect(result.compiled_plan).toEqual(verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, sink.snapshot as PipelineV2RunState, candidate));
        expect(result.state).toBe(sink.snapshot as PipelineV2RunState);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("25. the result and the diagnostics carry no bodies, paths or credentials", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const canaryTask = preparedTask("task-a", 1, null, "planning_proposal", CANARY);
        const candidate = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: canaryTask.sha256, depends_on: [] }],
            },
          ],
          [canaryTask],
        );
        const result = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
        const resultText = JSON.stringify(result);
        for (const banned of [CANARY, "canonical_json", "runRoot", "task_revisions/", "dht_session_bearer_token", "OPENCODE_CONFIG_CONTENT"]) {
          expect(resultText).not.toContain(banned);
        }
        expect(Object.keys(result).sort()).toEqual(["compiled_plan", "state"]);
        // the conflict diagnostics are content-free as well
        const conflicting = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", revision: 1, sha256: preparedTask("task-a", 1, null, "planning_proposal", "other body").sha256, depends_on: [] },
                { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
              ],
            },
          ],
          [preparedTask("task-a", 1, null, "planning_proposal", "other body"), B1],
        );
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: conflicting }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        for (const banned of [CANARY, "other body", fixture.runRoot, "dht_session_bearer_token"]) {
          expect(error.message).not.toContain(banned);
        }
        expect(Object.keys(error).sort()).toEqual(["name", "reason", "state"]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("26. the runtime export surfaces of the public and internal modules are pinned", async () => {
    const publicNamespace = (await import("../src/pipeline_v2_run_plan_controller.ts")) as Record<string, unknown>;
    expect(Object.keys(publicNamespace).sort()).toEqual([
      "PipelineV2RunPlanControllerError",
      "acceptPipelineV2RunPlanCandidate",
    ]);
    const internalNamespace = (await import("../src/pipeline_v2_run_plan_controller_internal.ts")) as Record<string, unknown>;
    expect(Object.keys(internalNamespace).sort()).toEqual([
      "PipelineV2RunPlanControllerError",
      "acceptPipelineV2RunPlanCandidateCore",
      "realPipelineV2RunPlanControllerOps",
    ]);
    expect(Object.isFrozen(realPipelineV2RunPlanControllerOps)).toBe(true);
    expect((realPipelineV2RunPlanControllerOps as { verifyCandidateForAcceptance?: unknown }).verifyCandidateForAcceptance).toBe(
      verifyPipelineV2RunPlanCandidateForAcceptance,
    );
  });

  test("28. a repeated plan candidate r1 after a linked r2 is a stale conflict with zero effects", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        // plan revision 1 with task-a@1 over the unbound planning execution
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        // a linked plan-only revision 2 over the same unbound planning
        // execution keeps the acceptance boundary valid
        const second = revisionTwoCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
          first.plan,
          [],
          1,
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: second });
        const stateAtR2 = sink.snapshot as PipelineV2RunState;
        expect(stateAtR2.plan_revisions.map((record) => record.revision)).toEqual([1, 2]);
        // repeating candidate r1: the exact r1 record exists but is not the
        // last durable plan revision — a stale candidate, not idempotent success
        const paths = [
          planManifestPath(fixture, 1),
          planManifestPath(fixture, 2),
          taskManifestPath(fixture, "task-a", 1),
        ];
        const identitiesBefore = await Promise.all(paths.map(fileIdentity));
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate: first }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain("the durable plan ledger has moved past the candidate plan revision 1; the candidate is stale");
        expect(error.state).toBe(stateAtR2);
        expect(recording.commands).toEqual([]);
        // the authoritative state stays at r2 and nothing was published
        expect((sink.snapshot as PipelineV2RunState).plan_revisions.map((record) => record.revision)).toEqual([1, 2]);
        expect(await Promise.all(paths.map(fileIdentity))).toEqual(identitiesBefore);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("29. a candidate task revision older than the durable latest is a downgrade conflict", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        // the user revise flow makes task-a@2 durable and restarts the planner
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
          executionIndex: 1,
        });
        await sink.dispatch({
          kind: "run_waiting",
          stateId: "stage_dispatch",
          reason: "stage_iteration_limit_exhausted",
          requestSha256: PLAN_WAIT_REQUEST,
          actions: [{ id: "revise_task", to: "architect" }],
        });
        const A2 = preparedTask("task-a", 2, A1.sha256, "user_response", "Body A two");
        await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT });
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: A2.sha256,
          waitIndex: 1,
          intentSha256: INTENT,
        });
        await sink.dispatch({
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: PLAN_WAIT_REQUEST,
          actionId: "revise_task",
          responseSha256: PLAN_WAIT_RESPONSE,
        });
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
        for (const command of agentPhases("replanning")) {
          await sink.dispatch(command);
        }
        // plan revision 2 pointing back at task-a@1 while task-a@2 is durable
        const downgrade = revisionTwoCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
          first.plan,
          [],
          2,
        );
        const recording = recordingSink(sink, fixture, false);
        const cause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: recording.sink, candidate: downgrade }),
        );
        const error = expectControllerError(cause, "candidate_conflict");
        expect(error.message).toContain(
          'the durable task ledger already records a newer revision 2 of "task-a" than the candidate\'s revision 1',
        );
        expect(recording.commands).toEqual([]);
        // the conflict fired before any publication: no plan-2 manifest exists
        const paths = await runPlanPaths(fixture.runRoot);
        expect(paths).toEqual(["plans/", "plans/1.json", "tasks/", "tasks/task-a/", "tasks/task-a/1.json"]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("30. a candidate at the durable latest task revision stays valid beside unrelated records", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const first = revisionOneCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
            },
          ],
          [A1],
        );
        await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate: first });
        // the revise flow: task-a@2 durable, then the planner restarts
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
          executionIndex: 1,
        });
        await sink.dispatch({
          kind: "run_waiting",
          stateId: "stage_dispatch",
          reason: "stage_iteration_limit_exhausted",
          requestSha256: PLAN_WAIT_REQUEST,
          actions: [{ id: "revise_task", to: "architect" }],
        });
        const A2 = preparedTask("task-a", 2, A1.sha256, "user_response", "Body A two");
        await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: INTENT });
        await sink.dispatch({
          kind: "task_revision_accepted",
          taskId: "task-a",
          revision: 2,
          taskSha256: A2.sha256,
          waitIndex: 1,
          intentSha256: INTENT,
        });
        await sink.dispatch({
          kind: "wait_response_recorded",
          waitIndex: 1,
          expectedRequestSha256: PLAN_WAIT_REQUEST,
          actionId: "revise_task",
          responseSha256: PLAN_WAIT_RESPONSE,
        });
        await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
        for (const command of agentPhases("replanning")) {
          await sink.dispatch(command);
        }
        // an unrelated durable task record for another task id
        const Z1 = preparedTask("task-z", 1, null, "planning_proposal", "Body Z one");
        await sink.dispatch({ kind: "task_revision_accepted", taskId: "task-z", revision: 1, taskSha256: Z1.sha256 });
        // the candidate points at the durable latest revision of task-a
        const current = revisionTwoCandidate(
          [
            {
              id: "stage-1",
              template: "development",
              tasks: [{ id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] }],
            },
          ],
          [A2],
          first.plan,
          [A1],
          2,
        );
        const recording = recordingSink(sink, fixture, true);
        const result = await acceptPipelineV2RunPlanCandidate({
          pipeline,
          runRoot: fixture.runRoot,
          sink: recording.sink,
          candidate: current,
        });
        // the unrelated task-z record never conflicted; only the plan moved
        expect(recording.commands).toEqual([
          { kind: "plan_revision_accepted", planRevision: 2, planSha256: current.plan.sha256, originExecution: 2 },
        ]);
        expect(result.state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual([
          "task-a@1",
          "task-a@2",
          "task-z@1",
        ]);
        expect(result.state.plan_revisions.map((record) => record.revision)).toEqual([1, 2]);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("31. the provenance gates run before any snapshot field read; Proxy traps stay at zero", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        let snapshotTraps = 0;
        const proxySnapshot = new Proxy(sink.snapshot as PipelineV2RunState, {
          get(target, property, receiver) {
            snapshotTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        let candidateTraps = 0;
        const proxyCandidate = new Proxy(candidate, {
          get(target, property, receiver) {
            candidateTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        const proxiedSink: PipelineV2RunPlanControllerSink = {
          get snapshot() {
            return proxySnapshot;
          },
          get poisoned() {
            return sink.poisoned;
          },
          dispatch: async (command: PipelineV2RunCommand) => {
            await sink.dispatch(command);
          },
        };
        // fake, spread and Proxy pipelines are rejected by the pipeline
        // provenance gate before any snapshot or candidate field is read
        const fakePipeline = { bundleRoot: "/nowhere" } as unknown as ResolvedPipelineV2;
        const spreadPipeline = { ...pipeline } as unknown as ResolvedPipelineV2;
        let pipelineTraps = 0;
        const proxyPipeline = new Proxy(pipeline, {
          get(target, property, receiver) {
            pipelineTraps += 1;
            return Reflect.get(target, property, receiver);
          },
        });
        for (const [label, brokenPipeline] of [["fake", fakePipeline], ["spread", spreadPipeline], ["proxy", proxyPipeline]] as const) {
          snapshotTraps = 0;
          candidateTraps = 0;
          pipelineTraps = 0;
          const cause = await catchControllerCall(() =>
            acceptPipelineV2RunPlanCandidate({
              pipeline: brokenPipeline,
              runRoot: fixture.runRoot,
              sink: proxiedSink,
              candidate: proxyCandidate,
            }),
          );
          expect(`${label}:${cause instanceof PipelineError}`).toBe(`${label}:true`);
          expect(snapshotTraps).toBe(0);
          expect(candidateTraps).toBe(0);
        }
        // the real pipeline with a Proxy candidate: the compiled gate fires
        // before any snapshot field read and with zero candidate traps
        snapshotTraps = 0;
        candidateTraps = 0;
        const candidateCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({
            pipeline,
            runRoot: fixture.runRoot,
            sink: proxiedSink,
            candidate: proxyCandidate,
          }),
        );
        expect(candidateCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        expect(snapshotTraps).toBe(0);
        expect(candidateTraps).toBe(0);
        await expectNoRunPlan(fixture.runRoot);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("32. an unexpected sink getter error propagates unchanged", async () => {
    await withPipeline(async (pipeline) => {
      const fixture = await setupRun();
      try {
        const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
        await playPlanning(sink, pipeline);
        const candidate = revisionOneCandidate(SINGLE_STAGE, [A1, B1]);
        const snapshotFailure = new Error("injected snapshot getter failure");
        const snapshotSink: PipelineV2RunPlanControllerSink = {
          get snapshot(): PipelineV2RunState {
            throw snapshotFailure;
          },
          get poisoned() {
            return false;
          },
          dispatch: async () => {},
        };
        const snapshotCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: snapshotSink, candidate }),
        );
        expect(snapshotCause).toBe(snapshotFailure);
        expect(snapshotCause).not.toBeInstanceOf(PipelineV2RunPlanControllerError);
        const poisonedFailure = new Error("injected poisoned getter failure");
        const poisonedSink: PipelineV2RunPlanControllerSink = {
          snapshot: null,
          get poisoned(): boolean {
            throw poisonedFailure;
          },
          dispatch: async () => {},
        };
        const poisonedCause = await catchControllerCall(() =>
          acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink: poisonedSink, candidate }),
        );
        expect(poisonedCause).toBe(poisonedFailure);
        expect(poisonedCause).not.toBeInstanceOf(PipelineV2RunPlanControllerError);
        await expectNoRunPlan(fixture.runRoot);
      } finally {
        await disposeRun(fixture);
      }
    });
  });

  test("27. source proof: the single layers are reused; no second authority, message parsing, forbidden imports or mutable seam", () => {
    for (const name of ["pipeline_v2_run_plan_controller.ts", "pipeline_v2_run_plan_controller_internal.ts"]) {
      const source = readFileSync(join(import.meta.dir, "..", "src", name), "utf8");
      const importTargets = [...source.matchAll(/from "([^"]+)"/g)].map((match) => match[1] ?? "");
      const allowed = [
        "./pipeline_v2_run_plan_controller_internal.ts",
        "node:path",
        "./pipeline_v2_state.ts",
        "./pipeline_v2_state_store.ts",
        "./pipeline_v2_run_plan_acceptance.ts",
        "./pipeline_v2_run_plan_candidate.ts",
        "./pipeline_v2_run_plan_compiled.ts",
        "./pipeline_v2.ts",
        "./pipeline_v2_immutable_document_store_internal.ts",
      ];
      expect(importTargets.length).toBeGreaterThan(0);
      for (const target of importTargets) {
        expect(allowed).toContain(target);
      }
      // no forbidden production or parallel-controller imports
      for (const forbiddenModule of [
        "pipeline_v2_coordinator",
        "pipeline_v2_runner",
        "pipeline_v2_resume_context",
        "pipeline_v2_wait",
        "pipeline_v2_docker",
        "pipeline_v2_runtime",
        "pipeline_v2_digest",
        "pipeline_v2_identity_compare",
        "pipeline_v2_run_plan_store",
        "pipeline_v2_run_plan_manifests",
        "pipeline_v2_run_plan_bindings",
        "pipeline_v2_run_plan_candidate_internal",
        "agent_smoke",
        "docker_helper",
        "launcher",
        "main",
        "cli",
        "node:fs",
      ]) {
        expect(source.includes(`from "./${forbiddenModule}.ts"`)).toBe(false);
        expect(source.includes(`from "${forbiddenModule}.ts"`)).toBe(false);
      }
      // no second authority and no message parsing
      for (const banned of [
        "validatePipelineV2RunState(",
        "comparePipelineV2RunIdentity(",
        "compilePipelineV2RunPlanCandidate(",
        "canonicalJson(",
        "CryptoHasher",
        "createHash",
        "cause.message",
        ".match(",
        "RegExp(",
        "node:fs",
      ]) {
        expect(source.includes(banned)).toBe(false);
      }
      // no mutable module-global seam in the internal module
      if (name === "pipeline_v2_run_plan_controller_internal.ts") {
        expect(source).toContain("Object.freeze({");
        expect(source.includes("let real")).toBe(false);
        expect(source.includes("installOps")).toBe(false);
      }
    }
  });
});
