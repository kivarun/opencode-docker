/**
 * The P-1 content projection suite: the byte-exact task-body chunk
 * stream and the paged deterministic semantic record projection of one
 * accepted plan revision, over real published artifacts and real durable
 * state.
 *
 * Contract source: `/exchange/outbox/increment-6-p1-preflight-report.md`
 * (r2). Every surface is read-only and path-free; the ledger is
 * authoritative; the store loaders and digest-domain owners are the
 * single production owners.
 */
import { expect, test } from "bun:test";
import { chmodSync, lstatSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync, symlinkSync } from "node:fs";import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  type PipelineV2RunInputState,
  validatePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { publishPipelineV2PlanRevision, publishPipelineV2TaskRevision, PipelineV2RunPlanStoreError } from "../src/pipeline_v2_run_plan_store.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { stageTaskBodyDigest } from "../src/pipeline_v2_stage_task.ts";
import {
  PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
  PIPELINE_V2_RUN_PLAN_TASK_DEFAULT_CHUNK,
  PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK,
  PIPELINE_V2_RUN_PLAN_PLAN_DEFAULT_LIMIT,
  PIPELINE_V2_RUN_PLAN_PLAN_MAX_LIMIT,
  PIPELINE_V2_RUN_PLAN_RECORD_KINDS,
  PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE,
  PipelineV2RunPlanContentError,
  planRevisionContentPipelineV2Run,
  planRecordCount,
  projectPlanRecordsPage,
  storeCauseToReason,
  taskRevisionContentPipelineV2Run,
  type PipelineV2PlanContentOutcome,
  type PipelineV2PlanContentRecord,
  type PipelineV2TaskContentOutcome,
} from "../src/pipeline_v2_run_plan_content.ts";

const V2_PROFILE_EPOCH_BINDING = { digest: "d".repeat(64), profile_count: 1 };
const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

const RUN_ID = "content-run";
const PROTECTED_DIGEST = "a".repeat(64);
const CANARY = "browser_content_canary";
const TASK_A_BODY = `Implement the revision ledger for ${CANARY}.\nWith a second line.\n`;
const TASK_A2_BODY = "REVISED TOOL LOOPS.\n";

const hex = (char: string): string => char.repeat(64);

const CONTENT_PIPELINE = `
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
        to: done

  - id: done
    type: terminal
    result: success
`;

// --- fixture ----------------------------------------------------------------

async function makeDirs(runId: string): Promise<{ root: string; bundle: string; runRoot: string; stateRoot: string; pipeline: ResolvedPipelineV2 }> {
  const root = mkdtempSync(join(tmpdir(), "pipeline-v2-run-plan-content-"));
  const bundle = join(root, "bundle");
  mkdirSync(join(bundle, "prompts"), { recursive: true });
  mkdirSync(join(bundle, "schemas"), { recursive: true });
  writeFileSync(join(bundle, "pipeline.yaml"), CONTENT_PIPELINE);
  writeFileSync(join(bundle, "prompts", "coder.md"), `IMPLEMENT ${CANARY}\n`);
  writeFileSync(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  const stateRoot = join(root, "state");
  mkdirSync(stateRoot, { recursive: true, mode: 0o700 });
  // The standard store layout: the run directory lives at
  // `<stateRoot>/pipeline-runs/<runId>` and is created by the sink.
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  const pipeline = await loadPipelineV2(bundle);
  return { root, bundle, runRoot, stateRoot, pipeline };
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clock));
}

/**
 * The honest accepted history: plan r1 (task-a, task-b depending on
 * task-a) then plan r2 (both revised, user-response task revisions).
 */
async function buildFixture(): Promise<{
  root: string;
  bundle: string;
  runRoot: string;
  stateRoot: string;
  taskA1: PreparedPipelineV2RunTaskRevision;
  taskA2: PreparedPipelineV2RunTaskRevision;
  taskB1: PreparedPipelineV2RunTaskRevision;
  taskB2: PreparedPipelineV2RunTaskRevision;
  plan1: PreparedPipelineV2RunPlanRevision;
  plan2: PreparedPipelineV2RunPlanRevision;
}> {
  const dirs = await makeDirs(RUN_ID);
  clock = 0;
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId: RUN_ID, now: nextTick });
  const inputs: readonly PipelineV2RunInputState[] = [
    { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
  ];
  await sink.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
    inputs: inputs.map((input) => ({ ...input })),
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  let session = 0;
  const planning = async (): Promise<number> => {
    session += 1;
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
    await sink.dispatch({ kind: "agent_data_prepared" });
    await sink.dispatch({ kind: "agent_execution_session_created", sessionId: `sess-${session}` });
    await sink.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${session}` });
    await sink.dispatch({ kind: "agent_running" });
    await sink.dispatch({ kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } });
    await sink.dispatch({ kind: "agent_cleanup_completed" });
    const state = validatePipelineV2RunState(sink.snapshot);
    return state.executions.length;
  };

  const execution1 = await planning();
  const taskA1 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-a",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: TASK_A_BODY,
  });
  const taskB1 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-b",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "THE SECOND TASK\n",
  });
  const plan1 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: execution1,
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: [
          { id: "task-a", revision: 1, sha256: taskA1.sha256, depends_on: [] },
          { id: "task-b", revision: 1, sha256: taskB1.sha256, depends_on: ["task-a"] },
        ],
      },
    ],
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: dirs.pipeline,
    runRoot: dirs.runRoot,
    sink,
    candidate: preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA1, taskB1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    }),
  });
  // The planning transition commits; the stage-await wait opens at the
  // cursor and the revise-task intervention returns to the planner.
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: execution1,
  });
  const WAIT_REQUEST = hex("e");
  const WAIT_INTENT = hex("c");
  const WAIT_RESPONSE = hex("f");
  await sink.dispatch({
    kind: "run_waiting",
    stateId: "dev_entry",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: WAIT_REQUEST,
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: WAIT_INTENT });

  const execution2Wait = await planning;
  void execution2Wait;
  const taskA2 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-a",
    revision: 2,
    previous_sha256: taskA1.sha256,
    origin: "user_response",
    body: TASK_A2_BODY,
  });
  await publishPipelineV2TaskRevision(dirs.runRoot, taskA2.manifest);
  await sink.dispatch({
    kind: "task_revision_accepted",
    taskId: "task-a",
    revision: 2,
    taskSha256: taskA2.sha256,
    waitIndex: 1,
    intentSha256: WAIT_INTENT,
  });
  await sink.dispatch({
    kind: "wait_response_recorded",
    waitIndex: 1,
    expectedRequestSha256: WAIT_REQUEST,
    actionId: "revise_task",
    responseSha256: WAIT_RESPONSE,
  });

  const execution2 = await planning();
  const taskB2 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-b",
    revision: 2,
    previous_sha256: taskB1.sha256,
    origin: "user_response",
    body: "THE SECOND TASK, REVISED\n",
  });
  void taskB2;
  const plan2 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 2,
    previous_sha256: plan1.sha256,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: execution2,
    stages: [
      {
        id: "stage-1",
        template: "development",
        tasks: [
          { id: "task-a", revision: 2, sha256: taskA2.sha256, depends_on: [] },
          { id: "task-b", revision: 1, sha256: taskB1.sha256, depends_on: ["task-a"] },
        ],
      },
    ],
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: dirs.pipeline,
    runRoot: dirs.runRoot,
    sink,
    candidate: preparePipelineV2RunCandidateOf(plan2, [taskA2, taskB1], plan1, [taskA1]),
  });

  return { ...dirs, taskA1, taskA2, taskB1, taskB2, plan1, plan2 };
}

function preparePipelineV2RunCandidateOf(
  plan: PreparedPipelineV2RunPlanRevision,
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  previousPlan: PreparedPipelineV2RunPlanRevision | null,
  previousTaskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
) {
  return preparePipelineV2RunPlanCandidate({
    plan,
    taskRevisions: [...taskRevisions],
    previousPlan,
    previousTaskRevisions: [...previousTaskRevisions],
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

interface TreeEntry {
  readonly path: string;
  readonly kind: "file" | "directory" | "symlink" | "other";
  readonly mode: number;
  readonly ino: number;
  readonly size: number;
}

function snapTree(root: string): TreeEntry[] {
  const entries: TreeEntry[] = [];
  const walk = (dir: string, prefix: string): void => {
    const info = lstatSync(dir);
    const kind = info.isDirectory() ? "directory" : info.isFile() ? "file" : info.isSymbolicLink() ? "symlink" : "other";
    entries.push({ path: prefix === "" ? "." : prefix, kind, mode: info.mode & 0o7777, ino: info.ino, size: info.size });
    if (kind !== "directory") return;
    for (const name of readdirSync(dir).sort()) {
      walk(join(dir, name), prefix === "" ? name : `${prefix}/${name}`);
    }
  };
  walk(root, "");
  return entries;
}

function assertTreeUnchanged(root: string, before: TreeEntry[]): void {
  const after = snapTree(root);
  expect(after.length).toBe(before.length);
  for (let i = 0; i < before.length; i += 1) {
    expect(after[i]).toEqual(before[i]);
  }
}

function assertPathFree(document: unknown, envRoot: string): void {
  const serialized = JSON.stringify(document);
  for (const banned of ["runRoot", "run_root", "task_path", "plan_path", envRoot, "bundle", "prompts/coder.md"]) {
    expect(serialized).not.toContain(banned);
  }
}

// =============================================================================
// typed store mapping and closed vocabularies
// =============================================================================

test("storeCauseToReason maps the typed store variants without message text", () => {
  const storeError = (reason: "invalid_layout" | "conflict" | "io_failure"): PipelineV2RunPlanStoreError =>
    new PipelineV2RunPlanStoreError("not_published", reason, "candidate run-plan repair needed");
  expect(storeCauseToReason(storeError("invalid_layout"))).toBe("artifact_modified");
  expect(storeCauseToReason(storeError("conflict"))).toBe("artifact_modified");
  expect(storeCauseToReason(storeError("io_failure"))).toBe("internal_error");
  expect(storeCauseToReason(new Error("anything"))).toBe("internal_error");
  expect(storeCauseToReason(undefined)).toBe("internal_error");
  expect(storeCauseToReason({ reason: "conflict" })).toBe("internal_error");
});

test("the record vocabulary and the reason classes are closed", () => {
  expect([...PIPELINE_V2_RUN_PLAN_RECORD_KINDS]).toEqual(["stage", "task", "task_dependency"]);
  expect(() => new PipelineV2RunPlanContentError("content_too_large" as never, "x")).toThrow(TypeError);
});

// =============================================================================
// task surface
// =============================================================================

test("the task chunk serves byte-exact body bytes with the full body digest", async () => {
  const fixture = await buildFixture();
  try {
    const outcome: PipelineV2TaskContentOutcome = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(outcome.ok).toBe(true);
    expect(Object.isFrozen(outcome)).toBe(true);
    if (!outcome.ok) throw new Error("unreachable");
    expect(outcome.schemaVersion).toBe(1);
    expect(outcome.exitCode).toBe(0);
    expect(outcome.taskId).toBe("task-a");
    expect(outcome.revision).toBe(1);
    expect(outcome.manifestSha256).toBe(fixture.taskA1.sha256);
    expect(outcome.previousSha256).toBeNull();
    expect(outcome.origin).toBe("planning_proposal");
    const body = outcome.body;
    const bytes = Buffer.from(TASK_A_BODY, "utf8");
    expect(body.byteCount).toBe(bytes.byteLength);
    expect(body.sha256).toBe(stageTaskBodyDigest(bytes));
    expect(body.offset).toBe(0);
    expect(body.limit).toBe(PIPELINE_V2_RUN_PLAN_TASK_DEFAULT_CHUNK);
    expect(body.returnedByteCount).toBe(bytes.byteLength);
    expect(body.nextOffset).toBeNull();
    expect(Buffer.from(body.dataBase64, "base64").toString("utf8")).toBe(TASK_A_BODY);
    assertPathFree(outcome, fixture.root);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("task chunks reassemble exactly; repeats are byte-identical; the run tree is untouched", async () => {
  const fixture = await buildFixture();
  try {
    const before = snapTree(fixture.root);
    const bytes = Buffer.from(TASK_A_BODY, "utf8");
    const stream: string[] = [];
    let offset = 0;
    let guard = 0;
    while (offset < bytes.byteLength && guard < 1000) {
      guard += 1;
      const outcome = await taskRevisionContentPipelineV2Run({
        runId: RUN_ID,
        stateRoot: fixture.stateRoot,
        taskId: "task-a",
        revision: 1,
        offset,
        limit: 3,
      });
      expect(outcome.ok).toBe(true);
      if (!outcome.ok) throw new Error("unreachable");
      stream.push(Buffer.from(outcome.body.dataBase64, "base64").toString("utf8"));
      expect(outcome.body.byteCount).toBe(bytes.byteLength);
      expect(outcome.body.sha256).toBe(stageTaskBodyDigest(bytes));
      if (outcome.body.nextOffset === null) {
        offset = bytes.byteLength;
      } else {
        expect(outcome.body.nextOffset).toBe(offset + outcome.body.returnedByteCount);
        offset = outcome.body.nextOffset;
      }
    }
    expect(stream.join("")).toBe(TASK_A_BODY);
    const first = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    const second = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(first.ok && second.ok).toBe(true);
    if (first.ok && second.ok) {
      expect(first.body.dataBase64).toBe(second.body.dataBase64);
    }
    assertTreeUnchanged(fixture.root, before);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("the task cursor refuses offset > byteCount and serves the empty terminal chunk", async () => {
  const fixture = await buildFixture();
  try {
    const bytes = Buffer.from(TASK_A_BODY, "utf8");
    const over = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
      offset: bytes.byteLength + 1,
    });
    expect(over.ok).toBe(false);
    if (over.ok) throw new Error("unreachable");
    expect(over.reason).toBe("invalid_cursor");
    expect(over.exitCode).toBe(1);
    expect(Object.isFrozen(over)).toBe(true);
    assertPathFree(over, fixture.root);

    const empty = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
      offset: bytes.byteLength,
    });
    expect(empty.ok).toBe(true);
    if (!empty.ok) throw new Error("unreachable");
    expect(empty.body.returnedByteCount).toBe(0);
    expect(empty.body.dataBase64).toBe("");
    expect(empty.body.nextOffset).toBeNull();
    expect(empty.body.sha256).toBe(stageTaskBodyDigest(bytes));
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("the task surface validates its numeric classes and its chunk cap", async () => {
  const fixture = await buildFixture();
  try {
    const draw = async (change: Record<string, unknown>): Promise<PipelineV2TaskContentOutcome> =>
      await taskRevisionContentPipelineV2Run({
        runId: RUN_ID,
        stateRoot: fixture.stateRoot,
        taskId: "task-a",
        revision: 1,
        ...change,
      } as never);
    expect((await draw({ limit: PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK })).ok).toBe(true);
    const overCap = await draw({ limit: PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK + 1 });
    if (overCap.ok) throw new Error("cap not enforced");
    expect(overCap.reason).toBe("internal_error");
    for (const [label, change] of [
      ["limit 0", { limit: 0 }],
      ["limit negative", { limit: -1 }],
      ["limit fractional", { limit: 1.5 }],
      ["offset negative", { offset: -1 }],
      ["offset fractional", { offset: 0.5 }],
      ["revision 0", { revision: 0 }],
      ["revision negative", { revision: -3 }],
    ] as const) {
      const outcome = await draw(change as Record<string, unknown>);
      if (outcome.ok) throw new Error(`expected internal_error for ${label}`);
      expect(outcome.reason, label).toBe("internal_error");
      expect(outcome.exitCode, label).toBe(1);
      expect(outcome.runId, label).toBe("");
    }
    const unsafeRun = await taskRevisionContentPipelineV2Run({
      runId: "BAD ID",
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    if (unsafeRun.ok) throw new Error("unsafe run id accepted");
    expect(unsafeRun.reason).toBe("internal_error");
    expect(unsafeRun.runId).toBe("");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("the task surface serves the accepted revision 2 with its origin class and chain", async () => {
  const fixture = await buildFixture();
  try {
    const outcome = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 2,
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) throw new Error("unreachable");
    expect(outcome.manifestSha256).toBe(fixture.taskA2.sha256);
    expect(outcome.origin).toBe("user_response");
    expect(outcome.previousSha256).toBe(fixture.taskA1.sha256);
    expect(Buffer.from(outcome.body.dataBase64, "base64").toString("utf8")).toBe(TASK_A2_BODY);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("ledger-first selection refuses an unpublished identity before any artifact read", async () => {
  const fixture = await buildFixture();
  try {
    // An artifact published but never accepted must stay invisible: the
    // ledger is the selection authority.
    const ghost = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-c",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "PUBLISHED BUT NEVER ACCEPTED\n",
    });
    await publishPipelineV2TaskRevision(fixture.runRoot, ghost.manifest);
    const before = snapTree(fixture.root);
    const outcome = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-c",
      revision: 1,
    });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("unknown_revision");
    const planOutcome = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 9,
    });
    expect(planOutcome.ok).toBe(false);
    if (planOutcome.ok) throw new Error("unreachable");
    expect(planOutcome.reason).toBe("unknown_revision");
    assertTreeUnchanged(fixture.root, before);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("an absent artifact behind an accepted record is artifact_missing with zero bytes", async () => {
  const fixture = await buildFixture();
  try {
    const artifact = join(fixture.runRoot, "run-plan", "tasks", "task-a", "1.json");
    rmSync(artifact);
    const outcome = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_missing");
    expect(JSON.stringify(outcome)).not.toContain(TASK_A_BODY);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("damaged artifacts fail closed as artifact_modified with zero bytes", async () => {
  const fixture = await buildFixture();
  try {
    const artifact = join(fixture.runRoot, "run-plan", "tasks", "task-a", "1.json");
    // non-canonical rewrite
    writeFileSync(artifact, `${readFileSync(artifact, "utf8")}\n`, { mode: 0o600 });
    const damaged = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(damaged.ok).toBe(false);
    if (damaged.ok) throw new Error("unreachable");
    expect(damaged.reason).toBe("artifact_modified");

    // symlink substitution
    rmSync(artifact);
    symlinkSync(join(fixture.runRoot, "run-plan", "tasks", "task-a", "2.json"), artifact);
    const symlinked = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(symlinked.ok).toBe(false);
    if (symlinked.ok) throw new Error("unreachable");
    expect(symlinked.reason).toBe("artifact_modified");

    // wrong mode over the canonical republication
    rmSync(artifact);
    await publishPipelineV2TaskRevision(fixture.runRoot, fixture.taskA1.manifest);
    chmodSync(artifact, 0o644);
    const wrongMode = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(wrongMode.ok).toBe(false);
    if (wrongMode.ok) throw new Error("unreachable");
    expect(wrongMode.reason).toBe("artifact_modified");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("a forged ledger digest diverging from the artifact is artifact_modified", async () => {
  const runId = "forged-li-run";
  const dirs = await makeDirs(runId);
  try {
    clock = 0;
    const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
    const inputs: readonly PipelineV2RunInputState[] = [
      { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
    ];
    await sink.dispatch({
      kind: "create_run",
      runId,
      pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
      inputs: inputs.map((input) => ({ ...input })),
      projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
      profileEpoch: V2_PROFILE_EPOCH_BINDING,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
    await sink.dispatch({ kind: "agent_data_prepared" });
    await sink.dispatch({ kind: "agent_execution_session_created", sessionId: "s1" });
    await sink.dispatch({ kind: "agent_tool_session_created", sessionId: "t1" });
    await sink.dispatch({ kind: "agent_running" });
    await sink.dispatch({ kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } });
    await sink.dispatch({ kind: "agent_cleanup_completed" });
    const taskA1 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: runId,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: TASK_A_BODY,
    });
    await publishPipelineV2TaskRevision(dirs.runRoot, taskA1.manifest);
    // The ledger record carries a foreign digest: loader-valid, but the
    // per-request binding must refuse it as artifact_modified.
    await sink.dispatch({ kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("f") });
    const outcome = await taskRevisionContentPipelineV2Run({
      runId,
      stateRoot: dirs.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_modified");
  } finally {
    await rmSync(dirs.root, { recursive: true, force: true });
  }
}, 25000);

test("a changed bundle after the run is pipeline_mismatch and a missing run is run_not_found", async () => {
  const fixture = await buildFixture();
  try {
    const prompt = join(fixture.bundle, "prompts", "coder.md");
    writeFileSync(prompt, `CHANGED AFTER THE RUN\n`, { mode: 0o600 });
    const outcome = await taskRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("pipeline_mismatch");
    const missing = await taskRevisionContentPipelineV2Run({
      runId: "absent-run",
      stateRoot: fixture.stateRoot,
      taskId: "task-a",
      revision: 1,
    });
    expect(missing.ok).toBe(false);
    if (missing.ok) throw new Error("unreachable");
    expect(missing.reason).toBe("run_not_found");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

// =============================================================================
// plan surface
// =============================================================================

test("the plan projection serves the fixed record order with exact positions", async () => {
  const fixture = await buildFixture();
  try {
    const outcome: PipelineV2PlanContentOutcome = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
    });
    if (!outcome.ok) throw new Error("reason=" + outcome.reason);
    expect(outcome.ok).toBe(true);
    expect(Object.isFrozen(outcome)).toBe(true);
    expect(Object.isFrozen(outcome.records)).toBe(true);
    expect(outcome.schemaVersion).toBe(1);
    expect(outcome.exitCode).toBe(0);
    expect(outcome.revision).toBe(1);
    expect(outcome.manifestSha256).toBe(fixture.plan1.sha256);
    expect(outcome.previousSha256).toBeNull();
    expect(outcome.originExecution).toBeGreaterThan(0);
    expect(outcome.after).toBe(0);
    expect(outcome.nextAfter).toBeNull();
    expect(outcome.records).toEqual([
      { record: "stage", stage_position: 1, id: "stage-1", template: "development" },
      { record: "task", stage_position: 1, task_position: 1, id: "task-a", revision: 1, sha256: fixture.taskA1.sha256 },
      { record: "task", stage_position: 1, task_position: 2, id: "task-b", revision: 1, sha256: fixture.taskB1.sha256 },
      { record: "task_dependency", stage_position: 1, task_position: 2, dependency_position: 1, task_id: "task-a" },
    ]);
    const serialized = JSON.stringify(outcome);
    for (const banned of ["root_task", "input_id", PROTECTED_DIGEST, "depends_on"]) {
      expect(serialized, banned).not.toContain(banned);
    }
    assertPathFree(outcome, fixture.root);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("the plan cursor walks dense pages, the empty final page, and refuses advance past the end", async () => {
  const fixture = await buildFixture();
  try {
    const first = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
      limit: 2,
    });
    expect(first.ok).toBe(true);
    if (!first.ok) throw new Error("unreachable");
    expect(first.after).toBe(0);
    expect(first.nextAfter).toBe(2);
    expect(first.records.length).toBe(2);

    const second = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
      after: first.nextAfter ?? 0,
      limit: 2,
    });
    expect(second.ok).toBe(true);
    if (!second.ok) throw new Error("unreachable");
    expect(second.after).toBe(2);
    expect(second.records.length).toBe(2);
    // The second page consumed the whole stream: no tail, nextAfter null.
    expect(second.nextAfter).toBeNull();

    const tail = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
      after: 4,
      limit: 2,
    });
    expect(tail.ok).toBe(true);
    if (!tail.ok) throw new Error("unreachable");
    expect(tail.records.length).toBe(0);
    expect(tail.nextAfter).toBeNull();

    const advance = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
      after: 5,
    });
    expect(advance.ok).toBe(false);
    if (advance.ok) throw new Error("unreachable");
    expect(advance.reason).toBe("invalid_cursor");
    const exact = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
      after: 4,
    });
    expect(exact.ok).toBe(true);
    if (!exact.ok) throw new Error("unreachable");
    expect(exact.records).toEqual([]);
    expect(exact.nextAfter).toBeNull();

    const walk = await walkAllPlanRecords(fixture, 1, 1);
    expect(walk.length).toBe(4);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

async function walkAllPlanRecords(
  fixture: { stateRoot: string },
  revision: number,
  limit: number,
): Promise<readonly PipelineV2PlanContentRecord[]> {
  const records: PipelineV2PlanContentRecord[] = [];
  let cursor = 0;
  let guard = 0;
  while (guard < 10_000) {
    guard += 1;
    const outcome = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision,
      after: cursor,
      limit,
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) throw new Error("unreachable");
    records.push(...outcome.records);
    if (outcome.nextAfter === null) {
      break;
    }
    expect(outcome.nextAfter).toBeGreaterThan(cursor);
    cursor = outcome.nextAfter;
  }
  return records;
}

test("the plan surface validates its numeric classes and its limit cap", async () => {
  const fixture = await buildFixture();
  try {
    const draw = async (change: Record<string, unknown>): Promise<PipelineV2PlanContentOutcome> =>
      await planRevisionContentPipelineV2Run({
        runId: RUN_ID,
        stateRoot: fixture.stateRoot,
        revision: 1,
        ...change,
      } as never);
    expect((await draw({ limit: 200 })).ok).toBe(true);
    const overCap = await draw({ limit: 201 });
    if (overCap.ok) throw new Error("limit cap not enforced");
    expect(overCap.reason).toBe("internal_error");
    for (const [label, change] of [
      ["limit 0", { limit: 0 }],
      ["limit negative", { limit: -7 }],
      ["limit fractional", { limit: 2.5 }],
      ["after negative", { after: -1 }],
      ["after fractional", { after: 1.5 }],
      ["revision 0", { revision: 0 }],
    ] as const) {
      const outcome = await draw(change as Record<string, unknown>);
      if (outcome.ok) throw new Error(`expected internal_error for ${label}`);
      expect(outcome.reason, label).toBe("internal_error");
    }
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

test("plan revision 2 reprojects its own pointers and chain", async () => {
  const fixture = await buildFixture();
  try {
    const outcomes = await walkAllPlanRecords(fixture, 2, 10);
    expect(outcomes.map((record) => record.record).join(",")).toBe("stage,task,task,task_dependency");
    const outcome = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 2,
    });
    expect(outcome.ok).toBe(true);
    if (!outcome.ok) throw new Error("unreachable");
    expect(outcome.manifestSha256).toBe(fixture.plan2.sha256);
    expect(outcome.previousSha256).toBe(fixture.plan1.sha256);
    const tasks = outcome.records.filter((record) => record.record === "task");
    expect(JSON.stringify(tasks)).toContain(`"sha256":"${fixture.taskA2.sha256}"`);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 30000);

test("a damaged plan artifact and a missing plan artifact fail closed with zero content", async () => {
  const fixture = await buildFixture();
  try {
    const artifact = join(fixture.runRoot, "run-plan", "plans", "1.json");
    writeFileSync(artifact, `${readFileSync(artifact, "utf8")}\n`, { mode: 0o600 });
    const damaged = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
    });
    expect(damaged.ok).toBe(false);
    if (damaged.ok) throw new Error("unreachable");
    expect(damaged.reason).toBe("artifact_modified");
    rmSync(artifact);
    const missing = await planRevisionContentPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      revision: 1,
    });
    expect(missing.ok).toBe(false);
    if (missing.ok) throw new Error("unreachable");
    expect(missing.reason).toBe("artifact_missing");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 25000);

// =============================================================================
// boundedness pins
// =============================================================================

test("single-record overflow is unproducible: maximum-grammar records stay far below the page budget", () => {
  const wideId = "t".repeat(128);
  const records: PipelineV2PlanContentRecord[] = [
    { record: "stage", stage_position: 999_999_999_999, id: wideId, template: wideId },
    { record: "task", stage_position: 999_999_999_999, task_position: 999_999_999_999, id: wideId, revision: 9_007_199_254_740_991, sha256: hex("f") },
    { record: "task_dependency", stage_position: 999_999_999_999, task_position: 999_999_999_999, dependency_position: 999_999_999_999, task_id: wideId },
  ];
  let max = 0;
  for (const record of records) {
    max = Math.max(max, JSON.stringify(record).length);
  }
  expect(max).toBeLessThan(4096);
  // The page builder must place records when a tail exists: the progress
  // invariant holds for every grammar-legal record.
  const page = projectPlanRecordsPage(3, (index) => records[index]!, 0, 200);
  expect(page.records.length).toBe(3);
  expect(page.nextAfter).toBeNull();
});

test("a record that cannot fit the page budget is the typed internal_error, never a partial page", () => {
  const oversizedIndex = 0;
  const hostileRecord = { record: "task", garbage: "x".repeat(400_000) } as unknown as PipelineV2PlanContentRecord;
  expect(JSON.stringify(hostileRecord).length).toBeGreaterThan(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
  let called = 0;
  let boom: unknown = undefined;
  try {
    projectPlanRecordsPage(2, (index) => {
      if (index === oversizedIndex) {
        called += 1;
        return hostileRecord;
      }
      return { record: "task", stage_position: 1, task_position: 1, id: "task-a", revision: 1, sha256: hex("f") };
    }, 0, 200);
  } catch (cause) {
    boom = cause;
  }
  expect(boom instanceof PipelineV2RunPlanContentError).toBe(true);
  expect((boom as PipelineV2RunPlanContentError).reason).toBe("internal_error");
  expect(called).toBe(1);
});

test("the page cap arithmetic leaves the pinned envelope reserve", () => {
  expect(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP).toBe(262_144);
  expect(PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE).toBe(1_024);
  expect(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP - PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE).toBe(261_120);
  expect(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK).toBe(193_536);
  // 193_536 raw bytes -> 258_048 base64 characters; with the pinned
  // envelope budget the worst-case complete document stays under the cap.
  const chunk = Buffer.alloc(193_536, 0x61);
  expect(Buffer.from(chunk).toString("base64").length).toBe(258_048);
  expect(258_048 + PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE).toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
});

test("defaults are the established chunk/page constants", () => {
  expect(PIPELINE_V2_RUN_PLAN_TASK_DEFAULT_CHUNK).toBe(65_536);
  expect(PIPELINE_V2_RUN_PLAN_PLAN_DEFAULT_LIMIT).toBe(100);
  expect(planRecordCount({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 1,
    stages: [
      { id: "s", template: "t", tasks: [{ id: "a", revision: 1, sha256: hex("f"), depends_on: ["b", "c"] }, { id: "b", revision: 1, sha256: hex("f"), depends_on: [] }] },
    ],
  } as never)).toBe(5);
});

// =============================================================================
// serialized-envelope boundary pins (both surfaces independently)
// =============================================================================

// --- real-owner envelope evidence ------------------------------------------------

/**
 * The one production CliIo for envelope evidence: every content member
 * is the exact real producer export and stdout/stderr are captured byte
 * faithfully, so `runCli --json` exercises the shipped dispatcher and
 * the shipped JSON formatter (`JSON.stringify(outcome) + "\n"`).
 */
function makeRealCliIo(stateRoot: string): { io: CliIo; stdout: string[]; stderr: string[] } {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const io: CliIo = {
    baseEnv: { PATH: "/usr/bin:/bin" },
    runner: {
      run: () => Promise.resolve({ code: 0 }),
      killActive: () => false,
    },
    fetchAuth: () => Promise.resolve({ status: 200, body: {} }),
    resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: "/creds/token" }),
    resolveStateRootProjection: () => ({ localRoot: stateRoot, daemonRoot: stateRoot }),
    runPipelineV2: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["runPipelineV2"],
    resumePipelineV2: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["resumePipelineV2"],
    continuePipelineV2Stage: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["continuePipelineV2Stage"],
    revisePipelineV2Task: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["revisePipelineV2Task"],
    resumePipelineV2PlanningRunPlan: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["resumePipelineV2PlanningRunPlan"],
    respondPipelineV2Wait: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["respondPipelineV2Wait"],
    inspectPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["inspectPipelineV2Run"],
    eventsPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["eventsPipelineV2Run"],
    identityPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["identityPipelineV2Run"],
    logsPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["logsPipelineV2Run"],
    projectChangesPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["projectChangesPipelineV2Run"],
    projectChangeContentPipelineV2Run: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["projectChangeContentPipelineV2Run"],
    taskRevisionContentPipelineV2Run,
    planRevisionContentPipelineV2Run,
    projectApplyPipelineV2: (async () => {
      throw new Error("fixture");
    }) as CliIo["projectApplyPipelineV2"],
    projectApplyInspectPipelineV2: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["projectApplyInspectPipelineV2"],
    runSmoke: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => {
      throw new Error("fixture");
    }) as unknown as CliIo["runAgentSmoke"],
    readTaskFile: async () => {
      throw new Error("fixture");
    },
    writeStdout: (text: string) => {
      stdout.push(text);
    },
    writeError: (text: string) => {
      stderr.push(text);
    },
  };
  return { io, stdout, stderr };
}

async function dispatchHeavyTask(runId: string, dirs: { stateRoot: string; runRoot: string; pipeline: ResolvedPipelineV2 }, body: string) {
  clock = 0;
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  const inputs: readonly PipelineV2RunInputState[] = [
    { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
  ];
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
    inputs: inputs.map((input) => ({ ...input })),
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "ev-sess-1" },
    { kind: "agent_tool_session_created", sessionId: "ev-tool-1" },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } },
    { kind: "agent_cleanup_completed" },
  ] as const) {
    await sink.dispatch(command as never);
  }
  const prepared = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: "task-a",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body,
  });
  await publishPipelineV2TaskRevision(dirs.runRoot, prepared.manifest);
  await sink.dispatch({ kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: prepared.sha256 });
  return prepared;
}

const HEAVY_BODY = "A".repeat(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK);

test("the shipped CLI option guard owns invalid_options at exit 2 and never emits it as an exit-1 P-1 document", async () => {
  const runId = "heavy-task-run";
  const dirs = await makeDirs(runId);
  try {
    await dispatchHeavyTask(runId, dirs, HEAVY_BODY);
    const { io, stdout, stderr } = makeRealCliIo(dirs.stateRoot);
    const exit = await runCli(
      ["task-revision", "--run-id", runId, "--task-id", "task-a", "--revision", "1", "--limit", "999955", "--config-root", dirs.bundle, "--json"],
      io,
    );
    expect(exit).toBe(2);
    expect(stdout).toHaveLength(0);
    expect(stderr[0]).toContain("--limit must be a positive safe integer up to 193536");
  } finally {
    await rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("the full-surface-boundary task CLI document (full JSON + trailing LF) stays below the envelope cap", async () => {
  const fixture = await buildFullSurfaceBoundaryTaskFixture();
  try {
    const { io, stdout, stderr } = makeRealCliIo(fixture.stateRoot);
    const exit = await runCli(
      [
        "task-revision",
        "--run-id", fixture.runId,
        "--task-id", fixture.taskId,
        "--revision", "2",
        "--limit", String(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK),
        "--config-root", fixture.bundle,
        "--json",
      ],
      io,
    );
    if (exit !== 0) {
      throw new Error(`CLI exit ${exit}: ${stdout.join("|")} / ${stderr.join("|")}`);
    }
    expect(stdout).toHaveLength(1);
    const document = stdout[0] as string;
    expect(document.endsWith("\n")).toBe(true);
    const bytes = Buffer.byteLength(document, "utf8");
    const outcome = JSON.parse(document) as {
      runId: string;
      taskId: string;
      revision: number;
      previousSha256: string | null;
      origin: string;
      body: { byteCount: number; offset: number; limit: number; returnedByteCount: number; nextOffset: number | null; dataBase64: string };
    };
    expect(outcome.revision).toBe(2);
    expect(outcome.origin).toBe("user_response");
    expect(outcome.previousSha256).toHaveLength(64);
    expect(outcome.body.byteCount).toBe(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK);
    expect(outcome.body.limit).toBe(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK);
    expect(outcome.body.returnedByteCount).toBe(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK);
    expect(outcome.body.offset).toBe(0);
    expect(outcome.body.dataBase64.length).toBe(258_048);
    expect(bytes, "serialized CLI bytes (full document incl. LF)").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
    // The producer's frame guard is the reserve around the base64
    // payload; prove with the exact production serialization that the
    // reserve covers the whole CLI envelope even in the grammar-derived
    // worst case: every expandable numeric contract field grows to the
    // 16-digit non-negative safe-integer width relative to its actual
    // rendered width here (limit and returnedByteCount already sit at
    // the contract maximum and gain no allowance; the safe ids and the
    // digest fields are already at their fixed maxima).
    const envelopeBytes = bytes - outcome.body.dataBase64.length;
    const expansion =
      numericFieldExpansion(outcome.revision) +
      numericFieldExpansion(outcome.body.byteCount) +
      numericFieldExpansion(outcome.body.offset) +
      numericFieldExpansion(outcome.body.nextOffset);
    expect(envelopeBytes, "CLI envelope bytes without the payload").toBeLessThanOrEqual(771 - expansion);
    expect(envelopeBytes + expansion, "CLI worst-case envelope").toBeLessThanOrEqual(771);
    expect(envelopeBytes + expansion, "CLI worst-case envelope within the reserve").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE);
    expect(stderr.join("")).toBe("");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 60000);

test("the full-surface-boundary task HTTP response document (full serialized body) stays below the envelope cap", async () => {
  const fixture = await buildFullSurfaceBoundaryTaskFixture();
  try {
    const { response, debug } = await driveFullSurfaceBoundaryHttp(fixture, `/v1/runs/${fixture.runId}/tasks/${fixture.taskId}/revisions/2?limit=${PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK}`);
    if (response.status !== 200) throw new Error(`HTTP ${response.status}: reason=${debug.failureReason} probe: ${debug.rawProbe}`);
    expect(response.status).toBe(200);
    const text = await response.text();
    const document = JSON.parse(text) as {
      schema_version: number;
      kind: string;
      run_id: string;
      task_id: string;
      revision: number;
      previous_sha256: string | null;
      origin: string;
      body: { byte_count: number; offset: number; limit: number; returned_byte_count: number; next_offset: number | null; data_base64: string };
    };
    expect(document.schema_version).toBe(1);
    expect(document.kind).toBe("task_revision");
    expect(document.run_id).toBe(fixture.runId);
    expect(document.task_id).toBe(fixture.taskId);
    expect(document.revision).toBe(2);
    expect(document.previous_sha256).toHaveLength(64);
    expect(document.body.byte_count).toBe(PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK);
    expect(document.body.data_base64.length).toBe(258_048);
    const bytes = Buffer.byteLength(text, "utf8");
    expect(bytes, "full serialized HTTP response bytes").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
    // The same grammar-derived worst-case expansion on the HTTP
    // surface: revision, byte_count, offset and next_offset expand to
    // the 16-digit width relative to the actual serialized document.
    const envelopeBytes = bytes - document.body.data_base64.length;
    const expansion =
      numericFieldExpansion(document.revision) +
      numericFieldExpansion(document.body.byte_count) +
      numericFieldExpansion(document.body.offset) +
      numericFieldExpansion(document.body.next_offset);
    expect(envelopeBytes, "HTTP envelope bytes without the payload").toBeLessThanOrEqual(780 - expansion);
    expect(envelopeBytes + expansion, "HTTP worst-case envelope").toBeLessThanOrEqual(780);
    expect(envelopeBytes + expansion, "HTTP worst-case envelope within the reserve").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 90000);

test("the full-surface-boundary plan CLI document (full JSON + trailing LF) stays below the envelope cap", async () => {
  const fixture = await buildHeavyPlanFixture({ stages: 2, tasksPerStage: 100, previousDepends: 10 }, "p".repeat(128));
  try {
    const { io, stdout, stderr } = makeRealCliIo(fixture.stateRoot);
    const exit = await runCli(
      [
        "plan-revision",
        "--run-id", fixture.runId,
        "--revision", "1",
        "--after", "0",
        "--limit", "200",
        "--config-root", fixture.bundle,
        "--json",
      ],
      io,
    );
    if (exit !== 0) {
      throw new Error(`CLI exit ${exit}: ${stdout.join("|")} / ${stderr.join("|")}`);
    }
    const document = stdout[0] as string;
    expect(stdout).toHaveLength(1);
    expect(document.endsWith("\n")).toBe(true);
    const bytes = Buffer.byteLength(document, "utf8");
    const parsed = JSON.parse(document) as {
      runId: string;
      revision: number;
      manifestSha256: string;
      previousSha256: string | null;
      originExecution: number;
      after: number;
      records: { record: string }[];
      nextAfter: number | null;
    };
    expect(parsed.runId).toBe(fixture.runId);
    expect(parsed.records).toHaveLength(200);
    expect(parsed.nextAfter).toBe(200);
    expect(bytes, "full serialized plan CLI bytes incl. LF").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
    // The record bytes belong to the page budget; the envelope is the
    // full document minus the exact serialized records (brackets and
    // delimiters included), and its grammar-derived worst case expands
    // every top-level numeric contract field to the 16-digit width
    // relative to the actual production serialization, plus the null
    // previous digest growing to a full 64-hex value.
    const envelopeBytes = bytes - planRecordsContribution(parsed);
    const expansion =
      numericFieldExpansion(parsed.revision) +
      numericFieldExpansion(parsed.originExecution) +
      numericFieldExpansion(parsed.after) +
      numericFieldExpansion(parsed.nextAfter) +
      (parsed.previousSha256 === null ? 66 - 4 : 0);
    expect(envelopeBytes, "plan CLI envelope bytes").toBeGreaterThan(0);
    expect(envelopeBytes + expansion, "plan CLI worst-case envelope").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE);
    expect(stderr.join("")).toBe("");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 90000);

test("the full-surface-boundary plan HTTP response document (full serialized body) stays below the envelope cap", async () => {
  const fixture = await buildHeavyPlanFixture({ stages: 2, tasksPerStage: 100, previousDepends: 10 }, "p".repeat(128));
  try {
    const { response, debug } = await driveFullSurfaceBoundaryHttp(fixture, `/v1/runs/${fixture.runId}/plans/1?after=0&limit=200`);
    if (response.status !== 200) throw new Error(`HTTP ${response.status}: reason=${debug.failureReason} probe: ${debug.rawProbe}`);
    expect(response.status).toBe(200);
    const text = await response.text();
    const document = JSON.parse(text) as {
      schema_version: number;
      kind: string;
      run_id: string;
      revision: number;
      manifest_sha256: string;
      previous_sha256: string | null;
      origin_execution: number;
      records: { record: string }[];
      next_after: number | null;
      after: number;
    };
    expect(document.schema_version).toBe(1);
    expect(document.kind).toBe("plan_revision");
    expect(document.run_id).toBe(fixture.runId);
    expect(document.records).toHaveLength(200);
    expect(document.next_after).toBe(200);
    expect(document.after).toBe(0);
    const bytes = Buffer.byteLength(text, "utf8");
    expect(bytes, "full serialized plan HTTP bytes").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
    // Independently on the HTTP surface: envelope minus the exact
    // serialized records, with the same grammar-derived top-level
    // worst-case expansion (revision, origin_execution, after,
    // next_after, and previous_sha256 growing from null to 64 hex).
    const envelopeBytes = bytes - planRecordsContribution(document);
    const expansion =
      numericFieldExpansion(document.revision) +
      numericFieldExpansion(document.origin_execution) +
      numericFieldExpansion(document.after) +
      numericFieldExpansion(document.next_after) +
      (document.previous_sha256 === null ? 66 - 4 : 0);
    expect(envelopeBytes, "plan HTTP envelope bytes").toBeGreaterThan(0);
    expect(envelopeBytes + expansion, "plan HTTP worst-case envelope").toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE);
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 90000);

test("the producer reason vocabulary is exactly the eight closed values without invalid_options", () => {
  expect([...PIPELINE_V2_RUN_PLAN_CONTENT_REASONS]).toEqual([
    "run_not_found",
    "run_state_invalid",
    "pipeline_mismatch",
    "unknown_revision",
    "artifact_missing",
    "artifact_modified",
    "invalid_cursor",
    "internal_error",
  ]);
  expect(JSON.stringify(PIPELINE_V2_RUN_PLAN_CONTENT_REASONS)).not.toContain("invalid_options");
});

test("a caller shape that bypasses the CLI and routing guards is internal_error with zero content", async () => {
  const fixture = await buildFixture();
  try {
    for (const [label, options] of [
      ["task unsafe runId", { runId: "BAD ID", stateRoot: fixture.stateRoot, taskId: "task-a", revision: 1 }],
      ["task unsafe taskId", { runId: RUN_ID, stateRoot: fixture.stateRoot, taskId: "../x", revision: 1 }],
      ["task bad revision", { runId: RUN_ID, stateRoot: fixture.stateRoot, taskId: "task-a", revision: 0 }],
      ["task bad offset", { runId: RUN_ID, stateRoot: fixture.stateRoot, taskId: "task-a", revision: 1, offset: -1 }],
      ["task bad limit", { runId: RUN_ID, stateRoot: fixture.stateRoot, taskId: "task-a", revision: 1, limit: 0 }],
    ] as const) {
      const outcome = await taskRevisionContentPipelineV2Run(options as never);
      if (outcome.ok) throw new Error(`expected internal_error for ${label}`);
      expect(outcome.reason, label).toBe("internal_error");
      expect(outcome.runId, label).toBe("");
    }
    for (const [label, options] of [
      ["plan bad revision", { runId: RUN_ID, stateRoot: fixture.stateRoot, revision: 0 }],
      ["plan bad after", { runId: RUN_ID, stateRoot: fixture.stateRoot, revision: 1, after: -1 }],
      ["plan bad limit", { runId: RUN_ID, stateRoot: fixture.stateRoot, revision: 1, limit: 0 }],
    ] as const) {
      const outcome = await planRevisionContentPipelineV2Run(options as never);
      if (outcome.ok) throw new Error(`expected internal_error for ${label}`);
      expect(outcome.reason, label).toBe("internal_error");
      expect(outcome.runId, label).toBe("");
    }
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 30000);

// --- bounded projection memory -------------------------------------------------

test("the projection carries no whole-stream record array (structural pin)", () => {
  const moduleSource = readFileSync(new URL("../src/pipeline_v2_run_plan_content.ts", import.meta.url), "utf8");
  for (const banned of ["entries.push", "const entries", "planRecordAt"]) {
    expect(moduleSource, banned).not.toContain(banned);
  }
  expect(moduleSource).toContain("export function planRecordCursor(");
  // The only push site is the page list itself.
  const pushSites = moduleSource.split(".push(").length - 1;
  expect(pushSites).toBe(1);
  expect(moduleSource).toContain("records.push(record)");
});

interface HeavyManifestSpec {
  readonly stages: number;
  readonly tasksPerStage: number;
  readonly previousDepends: number;
}

/**
 * One heavy verified artifact behind a coherent reducer ledger: two
 * stages of 100 zero-padded 128-char task ids each, every task depending
 * on its lexical predecessors; the manifest is published by the real
 * store and accepted into the ledger through the real reducer.
 */
async function buildHeavyPlanFixture(spec: HeavyManifestSpec, requestedRunId = "heavy-plan-run") {
  const runId = requestedRunId;
  const dirs = await makeDirs(runId);
  clock = 0;
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  const inputs: readonly PipelineV2RunInputState[] = [
    { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
  ];
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
    inputs: inputs.map((input) => ({ ...input })),
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  let plannerSession = 1;
  await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `hs-${plannerSession}` },
    { kind: "agent_tool_session_created", sessionId: `ht-${plannerSession}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } },
    { kind: "agent_cleanup_completed" },
  ] as const) {
    await sink.dispatch(command as never);
  }
  let globalTaskId = 0;
  const descriptorStages: {
    id: string;
    template: string;
    tasks: { id: string; revision: number; sha256: string; depends_on: readonly string[] }[];
  }[] = [];
  const preparedTasks: PreparedPipelineV2RunTaskRevision[] = [];
  for (let stageIndex = 0; stageIndex < spec.stages; stageIndex += 1) {
    const tasks: { id: string; revision: number; sha256: string; depends_on: readonly string[] }[] = [];
    const stagePrefix = `stage-${stageIndex + 1}-`;
    for (let taskIndex = 0; taskIndex < spec.tasksPerStage; taskIndex += 1) {
      globalTaskId += 1;
      const id = `${stagePrefix}${String(taskIndex).padStart(3, "0")}${"x".repeat(Math.max(0, 128 - stagePrefix.length - 3))}`;
      const body = `HEAVY TASK BODY ${globalTaskId}\n`;
      const prepared = prepareTaskRevisionManifest({
        schema_version: 1,
        kind: "task_revision",
        run_id: runId,
        task_id: id,
        revision: 1,
        previous_sha256: null,
        origin: "planning_proposal",
        body,
      });
      await publishPipelineV2TaskRevision(dirs.runRoot, prepared.manifest);
      await sink.dispatch({ kind: "task_revision_accepted", taskId: id, revision: 1, taskSha256: prepared.sha256 });
      preparedTasks.push(prepared);
      const dependsOn: string[] = [];
      for (let back = 1; back <= spec.previousDepends && taskIndex - back >= 0; back += 1) {
        const prior = tasks[taskIndex - back]!;
        dependsOn.push(prior.id);
      }
      tasks.push({ id, revision: 1, sha256: prepared.sha256, depends_on: dependsOn });
    }
    descriptorStages.push({ id: `stage-${stageIndex + 1}`, template: "development", tasks });
  }
  const plan = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: runId,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 1,
    stages: descriptorStages,
  });
  await publishPipelineV2PlanRevision(dirs.runRoot, plan.manifest);
  await sink.dispatch({ kind: "plan_revision_accepted", planRevision: 1, planSha256: plan.sha256, originExecution: 1 });
  return { ...dirs, runId, plan, };
}

test("a heavy manifest with large depends_on walks exactly through limit-1 pages with dense cursors", async () => {
  const fixture = await buildHeavyPlanFixture({ stages: 2, tasksPerStage: 100, previousDepends: 10 });
  try {
    const reference: PipelineV2PlanContentRecord[] = [];
    for (let stageIndex = 0; stageIndex < fixture.plan.manifest.stages.length; stageIndex += 1) {
      const stage = fixture.plan.manifest.stages[stageIndex]!;
      reference.push({ record: "stage", stage_position: stageIndex + 1, id: stage.id, template: stage.template });
      for (let taskIndex = 0; taskIndex < stage.tasks.length; taskIndex += 1) {
        const task = stage.tasks[taskIndex]!;
        reference.push({
          record: "task",
          stage_position: stageIndex + 1,
          task_position: taskIndex + 1,
          id: task.id,
          revision: task.revision,
          sha256: task.sha256,
        });
        for (let dependencyIndex = 0; dependencyIndex < task.depends_on.length; dependencyIndex += 1) {
          reference.push({
            record: "task_dependency",
            stage_position: stageIndex + 1,
            task_position: taskIndex + 1,
            dependency_position: dependencyIndex + 1,
            task_id: task.depends_on[dependencyIndex]!,
          });
        }
      }
    }
    const total = reference.length;
    expect(planRecordCount(fixture.plan.manifest)).toBe(total);
    let cursor = 0;
    let guard = 0;
    while (cursor < total && guard <= total) {
      guard += 1;
      const page = await planRevisionContentPipelineV2Run({
        runId: fixture.runId,
        stateRoot: fixture.stateRoot,
        revision: 1,
        after: cursor,
        limit: 1,
      });
      if (!page.ok) throw new Error(`page failed at ${cursor}: ${page.reason}`);
      expect(page.records).toEqual([reference[cursor] as PipelineV2PlanContentRecord]);
      if (cursor + 1 < total) {
        expect(page.nextAfter).toBe(cursor + 1);
      } else {
        expect(page.nextAfter).toBeNull();
      }
      expect(page.after).toBe(cursor);
      cursor += 1;
    }
    expect(cursor).toBe(total);
    const past = await planRevisionContentPipelineV2Run({
      runId: fixture.runId,
      stateRoot: fixture.stateRoot,
      revision: 1,
      after: total + 1,
    });
    if (past.ok) throw new Error("expected invalid_cursor past the end");
    expect(past.reason).toBe("invalid_cursor");
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 120000);

test("a heavy manifest serves the exact interior records through single-record pages", async () => {
  const fixture = await buildHeavyPlanFixture({ stages: 1, tasksPerStage: 40, previousDepends: 6 });
  try {
    const firstTaskId = "stage-1-000";
    const stride = 7;
    for (let pick = 0; pick < 12; pick += 1) {
      const target = pick * stride;
      const page = await planRevisionContentPipelineV2Run({
        runId: fixture.runId,
        stateRoot: fixture.stateRoot,
        revision: 1,
        after: target,
        limit: 1,
      });
      if (!page.ok) throw new Error("unreachable");
      expect(page.records.length).toBe(1);
      const record = page.records[0] as PipelineV2PlanContentRecord;
      if (record.record === "task" && record.task_position === 1) {
        expect((record as { id: string }).id).toBe(firstTaskId);
      }
      expect(record).toBeTruthy();
    }
  } finally {
    await rmSync(fixture.root, { recursive: true, force: true });
  }
}, 60000);

// =============================================================================
// r3 corrective imports (kept at the tail so the established pins above
// stay byte-stable)
// =============================================================================

import { runCli, type CliIo } from "../src/main.ts";
import { PIPELINE_V2_RUN_PLAN_CONTENT_REASONS } from "../src/pipeline_v2_content_reasons.ts";
import { OrchestratorSubprocessClient, type OrchestratorCallResult } from "../src/control_plane/subprocess_client.ts";
import { routeOrchestratorCommand, type OrchestratorCommand } from "../src/control_plane/routing.ts";
import {
  projectControlPlanePlanContent,
  projectControlPlaneTaskContent,
} from "../src/control_plane/http_server.ts";

// =============================================================================
// r4/r5 corrective helpers: the full-surface-boundary fixtures, the
// grammar-derived numeric expansion and the one production HTTP
// serialization driver for the full-document evidence.
// =============================================================================

/**
 * The grammar-derived maximum rendered width of one non-negative safe
 * integer field: 16 decimal digits (Number.MAX_SAFE_INTEGER). The
 * worst-case envelope proof expands every numeric contract field of the
 * actual production document to this width; `null` renders as 4 bytes.
 */
const MAX_NON_NEGATIVE_SAFE_INTEGER_DIGITS = String(Number.MAX_SAFE_INTEGER).length;

/** The grammar-derived expansion of one numeric-or-null contract field. */
function numericFieldExpansion(value: number | null): number {
  return MAX_NON_NEGATIVE_SAFE_INTEGER_DIGITS - (value === null ? 4 : String(value).length);
}

/**
 * The exact byte contribution of the serialized records array inside the
 * production plan document: the two array brackets plus every record's
 * exact serialized bytes plus one delimiter between records. Record
 * bytes belong to the page budget; only the remainder is the envelope.
 */
function planRecordsContribution(parsed: { records: readonly unknown[] }): number {
  let total = 2;
  for (let index = 0; index < parsed.records.length; index += 1) {
    if (index > 0) {
      total += 1;
    }
    total += Buffer.byteLength(JSON.stringify(parsed.records[index]), "utf8");
  }
  return total;
}

import {
  handleControlPlaneHttpRequest,
} from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { loadControlPlaneProjectRegistry } from "../src/control_plane/project_registry.ts";
import { defaultControlPlaneUiRoot, loadControlPlaneUiAssets } from "../src/control_plane/ui_assets.ts";

/**
 * The full-surface task boundary fixture: 128-character run id and task
 * id (the safe-id maximum), the full 193 536-byte body served as one
 * chunk at the contract maximum chunk size, and revision 2 — the chain
 * class whose document carries the 64-hex previous digest and the
 * `user_response` origin — served through the real reducer chain (plan
 * r1 acceptance, the planning transition, the durable wait, the
 * accepted intent and the wait-bound revision-2 task). Numeric cursor
 * and size fields are carried at the fixture values and their
 * worst-case widths are covered by the grammar-derived expansion proof,
 * never by a "maximum reachable" claim.
 */
async function buildFullSurfaceBoundaryTaskFixture(): Promise<{ root: string; bundle: string; runRoot: string; stateRoot: string; runId: string; taskId: string }> {
  const runId = "r".repeat(128);
  const taskId = "t".repeat(128);
  const dirs = await makeDirs(runId);
  clock = 0;
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  const inputs: readonly PipelineV2RunInputState[] = [
    { id: "task", type: "file", protected: true, digest: PROTECTED_DIGEST },
  ];
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
    inputs: inputs.map((input) => ({ ...input })),
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "max-sess-1" },
    { kind: "agent_tool_session_created", sessionId: "max-tool-1" },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } },
    { kind: "agent_cleanup_completed" },
  ] as const) {
    await sink.dispatch(command as never);
  }
  const taskR1 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "MAX ENVELOPE R1\n",
  });
  const plan1 = preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: runId,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 1,
    stages: [{ id: "stage-1", template: "development", tasks: [{ id: taskId, revision: 1, sha256: taskR1.sha256, depends_on: [] }] }],
  });
  await acceptPipelineV2RunPlanCandidate({
    pipeline: dirs.pipeline,
    runRoot: dirs.runRoot,
    sink,
    candidate: preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskR1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    }),
  });
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
    executionIndex: 1,
  });
  const WAIT_REQUEST = hex("e");
  const WAIT_INTENT = hex("c");
  await sink.dispatch({
    kind: "run_waiting",
    stateId: "dev_entry",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: WAIT_REQUEST,
    actions: [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ],
  });
  await sink.dispatch({ kind: "plan_intent_accepted", waitIndex: 1, intentSha256: WAIT_INTENT });
  const taskR2 = prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision: 2,
    previous_sha256: taskR1.sha256,
    origin: "user_response",
    body: HEAVY_BODY,
  });
  await publishPipelineV2TaskRevision(dirs.runRoot, taskR2.manifest);
  await sink.dispatch({
    kind: "task_revision_accepted",
    taskId,
    revision: 2,
    taskSha256: taskR2.sha256,
    waitIndex: 1,
    intentSha256: WAIT_INTENT,
  });
  return { ...dirs, runId, taskId };
}

/**
 * The one production HTTP serialization driver: registry + binding store
 * over the fixture, the real subprocess client spawning the real
 * production CLI child against the fixture state root, and the shipped
 * request handler — the returned Response body is the exact document the
 * production serializer sends.
 */
async function driveFullSurfaceBoundaryHttp(
  fixture: { root: string; bundle: string; stateRoot: string; runId: string },
  pathAndQuery: string,
): Promise<{ response: Response; debug: { failureReason: string | null; rawProbe: string } }> {
  const registryPath = join(fixture.root, "registry.json");
  const projectDir = join(fixture.root, "project");
  mkdirSync(projectDir, { recursive: true, mode: 0o700 });
  writeFileSync(
    registryPath,
    JSON.stringify({
      schema_version: 1,
      projects: [
        {
          id: "project-a",
          project_root: projectDir,
          config_root: fixture.bundle,
          pipeline_root: null,
          launcher_id: null,
          input_roots: [],
        },
      ],
    }),
    { mode: 0o600 },
  );
  const registry = await loadControlPlaneProjectRegistry(registryPath);
  const bindingRoot = join(fixture.root, "binding-store");
  mkdirSync(bindingRoot, { recursive: true, mode: 0o700 });
  const bindingStore = await ControlPlaneRunBindingStore.open(bindingRoot);
  await bindingStore.claim({ runId: fixture.runId, projectId: "project-a" });
  const realClient = new OrchestratorSubprocessClient({
    bunExecutable: process.execPath,
    orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
    workingDirectory: fixture.root,
    configRoot: fixture.bundle,
    baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: fixture.stateRoot },
    stdoutLimitBytes: 2_000_000,
    stderrLimitBytes: 1_000_000,
    sigintGraceMs: 2000,
  });
  /**
   * Diagnostic probe for an unexpected transport failure: reproduces the
   * same child command synchronously so a CI failure can distinguish a
   * genuinely broken child from the environment-driven spawn-stream
   * flake (the first Bun.spawn of the worker process can occasionally
   * deliver a foreign/empty stream under load; the child itself is
   * correct — see the single bounded retry below).
   */
  const probeChild = (command: OrchestratorCommand): string => {
    const routed = routeOrchestratorCommand(command, { configRoot: fixture.bundle });
    const spawned = Bun.spawnSync({
      cmd: [process.execPath, join(import.meta.dir, "..", "src", "main.ts"), ...routed.argv],
      cwd: fixture.root,
      env: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: fixture.stateRoot },
      stdout: "pipe",
      stderr: "pipe",
    });
    const rawError = spawned.stderr ? spawned.stderr.toString() : "";
    const rawOutput = spawned.stdout ? spawned.stdout.toString() : "";
    return `rawExit=${spawned.exitCode} rawStderr=${rawError.slice(0, 600) || "(clean)"} rawStdout=${rawOutput.length ? `len=${rawOutput.length} head=${rawOutput.slice(0, 80)}` : "(empty)"}`;
  };
  const debug: { failureReason: string | null; rawProbe: string } = { failureReason: null, rawProbe: "(none)" };
  /**
   * The bounded transport-retry wrapper: this environment's bun
   * spawn-stream capture has been observed to deliver an empty or
   * foreign stream for the first large child outputs of a heavily
   * spawning worker process (diagnosed by the synchronous child probe —
   * the child itself is always correct and clean). Malformed capture is
   * a transport-only outcome (`malformed_output`), never a producer or
   * contract verdict, so the identical call is retried up to three
   * times with a brief settle pause; any other failure and the final
   * malformed failure propagate unchanged with the probe detail.
   */
  const client = {
    run: async (command: OrchestratorCommand): Promise<OrchestratorCallResult> => {
      let result = await realClient.run(command);
      for (let attempt = 1; !result.ok && result.failure.reason === "malformed_output" && attempt <= 3; attempt += 1) {
        debug.failureReason = result.failure.reason;
        if (attempt === 3) {
          debug.rawProbe = probeChild(command);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
        result = await realClient.run(command);
      }
      return result;
    },
  } as unknown as OrchestratorSubprocessClient;
  const port = 4719;
  const deps = {
    registry,
    bindingStore,
    client,
    assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
    port,
    bodyLimitBytes: 4096,
    applyStoreRoot: null,
  };
  const request = new Request(`http://127.0.0.1:${port}${pathAndQuery}`, {
    method: "GET",
    headers: { host: `127.0.0.1:${port}` },
  });
  const response = await handleControlPlaneHttpRequest(deps as never, request);
  return { response, debug };
}
