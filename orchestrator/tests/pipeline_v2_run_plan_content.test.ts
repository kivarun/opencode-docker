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
import { publishPipelineV2TaskRevision, PipelineV2RunPlanStoreError } from "../src/pipeline_v2_run_plan_store.ts";
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
    expect(overCap.reason).toBe("invalid_options");
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
      if (outcome.ok) throw new Error(`expected invalid_options for ${label}`);
      expect(outcome.reason, label).toBe("invalid_options");
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
    expect(unsafeRun.reason).toBe("invalid_options");
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
    expect(overCap.reason).toBe("invalid_options");
    for (const [label, change] of [
      ["limit 0", { limit: 0 }],
      ["limit negative", { limit: -7 }],
      ["limit fractional", { limit: 2.5 }],
      ["after negative", { after: -1 }],
      ["after fractional", { after: 1.5 }],
      ["revision 0", { revision: 0 }],
    ] as const) {
      const outcome = await draw(change as Record<string, unknown>);
      if (outcome.ok) throw new Error(`expected invalid_options for ${label}`);
      expect(outcome.reason, label).toBe("invalid_options");
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

test("the worst-case serialized CLI task document stays under the envelope cap", () => {
  // Mirrors the exact shipped CLI serialization shape pinned by the
  // dispatcher tests (the JSON.stringify(outcome) + "\n" contract).
  const maxId = "r".repeat(128);
  const chunk = Buffer.alloc(193_536, 0x61);
  const cliTaskDoc = {
    ok: true,
    exitCode: 0,
    schemaVersion: 1,
    runId: maxId,
    taskId: "t".repeat(128),
    revision: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    manifestSha256: hex("f"),
    previousSha256: null,
    origin: "user_response",
    body: {
      byteCount: chunk.byteLength,
      sha256: hex("e"),
      offset: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP - 1,
      limit: PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK,
      returnedByteCount: chunk.byteLength,
      nextOffset: null,
      dataBase64: chunk.toString("base64"),
    },
  };
  const cliBytes = Buffer.byteLength(`${JSON.stringify(cliTaskDoc)}\n`, "utf8");
  expect(cliBytes).toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
  const httpTaskDoc = {
    schema_version: 1,
    kind: "task_revision",
    run_id: maxId,
    task_id: "h".repeat(128),
    revision: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    manifest_sha256: hex("f"),
    previous_sha256: null,
    origin: "user_response",
    body: {
      byte_count: chunk.byteLength,
      sha256: hex("e"),
      offset: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP - 1,
      limit: PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK,
      returned_byte_count: chunk.byteLength,
      next_offset: null,
      data_base64: chunk.toString("base64"),
    },
  };
  const httpBytes = Buffer.byteLength(JSON.stringify(httpTaskDoc), "utf8");
  expect(httpBytes).toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
  expect(httpBytes).toBeGreaterThan(Number(cliBytes) - Math.max(cliBytes - 10_000, 0));
}, 30000);

test("the worst-case serialized CLI and HTTP plan pages stay under the envelope cap independently", () => {
  expect(PIPELINE_V2_RUN_PLAN_PLAN_MAX_LIMIT).toBe(200);
  const maxId = "i".repeat(128);
  const records = Array.from({ length: 200 }, (_, i) => ({
    record: "task",
    stage_position: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    task_position: i + 1,
    id: maxId,
    revision: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    sha256: hex("f"),
  }));
  const cliPlanDoc = {
    ok: true,
    exitCode: 0,
    schemaVersion: 1,
    runId: maxId,
    revision: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    manifestSha256: hex("f"),
    previousSha256: null,
    originExecution: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    after: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    records,
    nextAfter: 2 * PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
  };
  const cliBytes = Buffer.byteLength(`${JSON.stringify(cliPlanDoc)}\n`, "utf8");
  expect(cliBytes).toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
  const httpPlanDoc = {
    schema_version: 1,
    kind: "plan_revision",
    run_id: maxId,
    revision: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    manifest_sha256: hex("f"),
    previous_sha256: null,
    origin_execution: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    after: PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
    records,
    next_after: 2 * PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP,
  };
  const httpBytes = Buffer.byteLength(JSON.stringify(httpPlanDoc), "utf8");
  expect(httpBytes).toBeLessThanOrEqual(PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP);
}, 30000);
