import { readFileSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  loadPipelineV2PlanRevision,
  loadPipelineV2TaskRevision,
  PipelineV2RunPlanStoreError,
  publishPipelineV2PlanRevision,
  publishPipelineV2TaskRevision,
} from "../src/pipeline_v2_run_plan_store.ts";
import {
  PipelineV2RunPlanBindingError,
  validateRootTaskBinding,
} from "../src/pipeline_v2_run_plan_bindings.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PipelineV2RunPlanRevisionManifest,
  type PipelineV2RunTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  publishPipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
  type PreparePipelineV2RunPlanCandidateOptions,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  hasPreparedRunPlanCandidateProvenance,
  publishPipelineV2RunPlanCandidateWithOps,
  realPipelineV2RunPlanCandidatePublicationOps,
  type PipelineV2RunPlanCandidatePublicationOps,
} from "../src/pipeline_v2_run_plan_candidate_internal.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const CANARY = "CANARY_secret_task_body";

const UNTRUSTED_MANIFEST_MESSAGE =
  "the operation requires the frozen prepared run plan object returned by " +
  "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
  "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
  "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
  "hand-built objects, casts, clones and Proxies are rejected before any field is read";

const UNTRUSTED_CANDIDATE_MESSAGE =
  "the operation requires the frozen prepared run plan candidate returned by " +
  "preparePipelineV2RunPlanCandidate; hand-built objects, casts, clones and " +
  "Proxies are rejected before any field is read";

interface Fixture {
  root: string;
  runRoot: string;
}

async function setup(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-run-plan-candidate-"));
  const runRoot = join(root, RUN_ID);
  await mkdir(runRoot, { mode: 0o700 });
  return { root, runRoot };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
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

function planValue(
  stages: readonly StageSpec[],
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 10,
    stages,
    ...overrides,
  };
}

function preparedPlan(
  stages: readonly StageSpec[],
  overrides: Record<string, unknown> = {},
  runId = RUN_ID,
): PreparedPipelineV2RunPlanRevision {
  const value = planValue(stages, overrides);
  if (runId !== RUN_ID) {
    value.run_id = runId;
  }
  return preparePlanRevisionManifest(value);
}

const A1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
const B1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");
const C1 = preparedTask("task-c", 1, null, "planning_proposal", "Body C one");
const A2 = preparedTask("task-a", 2, A1.sha256, "user_response", "Body A two");
const B2 = preparedTask("task-b", 2, B1.sha256, "user_response", "Body B two");

const STAGE_AB_1: StageSpec = {
  id: "stage-1",
  template: "development",
  tasks: [
    { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
    { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
  ],
};
const STAGE_AB_2: StageSpec = {
  id: "stage-1",
  template: "development",
  tasks: [
    { id: "task-a", revision: 2, sha256: A2.sha256, depends_on: [] },
    { id: "task-b", revision: 2, sha256: B2.sha256, depends_on: ["task-a"] },
  ],
};

const PLAN_AB_1 = preparedPlan([STAGE_AB_1]);
const PLAN_AB_2 = preparedPlan([STAGE_AB_2], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });

const CANARY_A1 = preparedTask("task-a", 1, null, "planning_proposal", `body with ${CANARY}`);
const CANARY_A2 = preparedTask("task-a", 2, hex("f"), "user_response", `revised with ${CANARY}`);

function candidateOptions(
  plan: PreparedPipelineV2RunPlanRevision,
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  previousPlan: PreparedPipelineV2RunPlanRevision | null = null,
  previousTaskRevisions: readonly PreparedPipelineV2RunTaskRevision[] = [],
  protectedInputDigest = PROTECTED_DIGEST,
): PreparePipelineV2RunPlanCandidateOptions {
  return { plan, taskRevisions, previousPlan, previousTaskRevisions, protectedInputDigest };
}

function catchOf(run: () => unknown): unknown {
  let caught: unknown = null;
  try {
    run();
  } catch (cause) {
    caught = cause;
  }
  return caught;
}

function expectBindingMessage(cause: unknown, message: string): void {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanBindingError);
  expect((cause as Error).message).toBe(message);
}

async function catchAsyncOf(run: () => Promise<unknown>): Promise<unknown> {
  try {
    await run();
    return null;
  } catch (cause) {
    return cause;
  }
}

function expectStoreError(
  cause: unknown,
  outcome: "not_published" | "durability_unknown",
  reason: "invalid_layout" | "conflict" | "io_failure",
  messageFragment = "",
): PipelineV2RunPlanStoreError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
  const error = cause as PipelineV2RunPlanStoreError;
  expect(error.outcome).toBe(outcome);
  expect(error.reason).toBe(reason);
  if (messageFragment !== "") {
    expect(error.message).toContain(messageFragment);
  }
  return error;
}

function injectedStoreError(
  outcome: "not_published" | "durability_unknown",
  reason: "invalid_layout" | "conflict" | "io_failure",
): PipelineV2RunPlanStoreError {
  return new PipelineV2RunPlanStoreError(outcome, reason, "injected failure");
}

/**
 * Recording per-call ops over the real store functions: every publication
 * is logged as `task:<id>` / `plan:<revision>` and then performed for real.
 */
function recordingOps(log: string[]): PipelineV2RunPlanCandidatePublicationOps {
  return {
    publishTaskRevision: async (runRoot, value) => {
      log.push(`task:${(value as PipelineV2RunTaskRevisionManifest).task_id}`);
      return await publishPipelineV2TaskRevision(runRoot, value);
    },
    publishPlanRevision: async (runRoot, value) => {
      log.push(`plan:${(value as PipelineV2RunPlanRevisionManifest).revision}`);
      return await publishPipelineV2PlanRevision(runRoot, value);
    },
  };
}

/** Counting per-call ops delegating to the real store functions. */
function countingOps(): {
  ops: PipelineV2RunPlanCandidatePublicationOps;
  taskCalls: () => number;
  planCalls: () => number;
} {
  let tasks = 0;
  let plans = 0;
  const ops: PipelineV2RunPlanCandidatePublicationOps = {
    publishTaskRevision: async (runRoot, value) => {
      tasks += 1;
      return await publishPipelineV2TaskRevision(runRoot, value);
    },
    publishPlanRevision: async (runRoot, value) => {
      plans += 1;
      return await publishPipelineV2PlanRevision(runRoot, value);
    },
  };
  return { ops, taskCalls: () => tasks, planCalls: () => plans };
}

function taskFilePath(fixture: Fixture, taskId: string, revision: number): string {
  return join(fixture.runRoot, "run-plan", "tasks", taskId, `${revision}.json`);
}

function planFilePath(fixture: Fixture, revision: number): string {
  return join(fixture.runRoot, "run-plan", "plans", `${revision}.json`);
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

// --- preparation -------------------------------------------------------------

test("1. revision 1 happy path: exact prepared objects in canonical order", () => {
  const candidate = preparePipelineV2RunPlanCandidate(
    candidateOptions(PLAN_AB_1, [B1, A1]),
  );
  expect(candidate.plan).toBe(PLAN_AB_1);
  expect(candidate.task_revisions[0]).toBe(A1);
  expect(candidate.task_revisions[1]).toBe(B1);
});

test("2. revision 2 happy path: successor plan and chained task revisions", () => {
  const candidate = preparePipelineV2RunPlanCandidate(
    candidateOptions(PLAN_AB_2, [B2, A2], PLAN_AB_1, [A1, B1]),
  );
  expect(candidate.plan).toBe(PLAN_AB_2);
  expect(candidate.task_revisions[0]).toBe(A2);
  expect(candidate.task_revisions[1]).toBe(B2);
  expect(candidate.task_revisions[0]?.manifest.previous_sha256).toBe(A1.sha256);
  expect(candidate.task_revisions[1]?.manifest.previous_sha256).toBe(B1.sha256);
});

test("3. several stages: candidate order is stage declaration order", () => {
  const plan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] }, { id: "task-c", revision: 1, sha256: C1.sha256, depends_on: ["task-b"] }] },
  ]);
  const candidate = preparePipelineV2RunPlanCandidate(
    candidateOptions(plan, [C1, B1, A1]),
  );
  expect(candidate.task_revisions.map((task) => task.manifest.task_id)).toEqual(["task-a", "task-b", "task-c"]);
});

test("4. caller task permutation does not change the candidate order", () => {
  const first = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
  const second = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [B1, A1]));
  expect(second.task_revisions[0]).toBe(A1);
  expect(second.task_revisions[1]).toBe(B1);
  expect(second.task_revisions).toEqual(first.task_revisions);
});

test("5. previous-task permutation does not change the candidate", () => {
  const first = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [A1, B1]));
  const second = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [B1, A1]));
  expect(second.task_revisions[0]).toBe(A2);
  expect(second.task_revisions[1]).toBe(B2);
  expect(second.task_revisions).toEqual(first.task_revisions);
});

test("6. protected TASK digest mismatch is rejected", () => {
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1], null, [], hex("c")))),
    "validateRootTaskBinding the plan root task does not bind the protected input digest",
  );
});

test("7. plan revision-chain failures are rejected", () => {
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_2, [A2, B2], null, []),
      ),
    ),
    "validatePlanRevisionChain requires revision 1 when no predecessor is passed",
  );
  const wrongDigestPlan = preparedPlan([STAGE_AB_2], { revision: 2, previous_sha256: hex("f") });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(wrongDigestPlan, [A2, B2], PLAN_AB_1, [A1, B1]),
      ),
    ),
    "validatePlanRevisionChain previous_sha256 does not name the predecessor digest",
  );
  const gapPlan = preparedPlan([STAGE_AB_2], { revision: 3, previous_sha256: PLAN_AB_1.sha256 });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(gapPlan, [A2, B2], PLAN_AB_1, [A1, B1]),
      ),
    ),
    "validatePlanRevisionChain revision numbers are not consecutive",
  );
  const foreignRunPlan = preparedPlan([STAGE_AB_2], { revision: 2, previous_sha256: PLAN_AB_1.sha256 }, "run-2");
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(foreignRunPlan, [A2, B2], PLAN_AB_1, [A1, B1]),
      ),
    ),
    "validatePlanRevisionChain covers two different runs",
  );
  const changedRootPlan = preparePlanRevisionManifest({
    ...planValue([STAGE_AB_2], { revision: 2, previous_sha256: PLAN_AB_1.sha256 }),
    root_task: { input_id: "task", sha256: hex("c") },
  });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(changedRootPlan, [A2, B2], PLAN_AB_1, [A1, B1], hex("c")),
      ),
    ),
    "validatePlanRevisionChain the protected root task binding changed between revisions",
  );
});

test("8. task revision-chain failures: wrong digest and revision gap", () => {
  const wrongDigestA2 = preparedTask("task-a", 2, hex("f"), "user_response", "Body A wrong digest");
  const wrongDigestPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 2, sha256: wrongDigestA2.sha256, depends_on: [] }, { id: "task-b", revision: 2, sha256: B2.sha256, depends_on: ["task-a"] }] },
  ], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(wrongDigestPlan, [wrongDigestA2, B2], PLAN_AB_1, [A1, B1]),
      ),
    ),
    "validateTaskRevisionChain previous_sha256 does not name the predecessor digest",
  );
  const A3 = preparedTask("task-a", 3, A2.sha256, "user_response", "Body A three");
  const gapPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 3, sha256: A3.sha256, depends_on: [] }, { id: "task-b", revision: 2, sha256: B2.sha256, depends_on: ["task-a"] }] },
  ], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(gapPlan, [A3, B2], PLAN_AB_1, [A1, B1]),
      ),
    ),
    "validateTaskRevisionChain revision numbers are not consecutive",
  );
});

test("9. missing current task revision is rejected", () => {
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1]))),
    'validatePlanTaskBindings is missing the task revision for plan task "task-b"',
  );
});

test("10. extra current task revision is rejected", () => {
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1, C1]))),
    "validatePlanTaskBindings carries a task revision the plan does not declare at position 2",
  );
});

test("11. duplicate current task revision is rejected", () => {
  const duplicate = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, duplicate, B1]))),
    "validatePlanTaskBindings carries a duplicate task revision at position 1",
  );
});

test("12. sparse and undefined current and previous arrays are rejected", () => {
  const sparse: PreparedPipelineV2RunTaskRevision[] = [A1];
  sparse.length = 2;
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, sparse))),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_1, [A1, undefined as unknown as PreparedPipelineV2RunTaskRevision]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [A1, undefined as unknown as PreparedPipelineV2RunTaskRevision]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
});

test("13. missing predecessor is rejected", () => {
  expectBindingMessage(
    catchOf(() => preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, []))),
    'preparePipelineV2RunPlanCandidate is missing the predecessor for task "task-a"',
  );
});

test("14. predecessor for a task the candidate does not declare is rejected", () => {
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1], null, [C1])),
    ),
    'preparePipelineV2RunPlanCandidate carries a predecessor for task "task-c" which the candidate does not declare',
  );
});

test("15. duplicate predecessor for one task is rejected", () => {
  const duplicate = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [A1, duplicate])),
    ),
    'preparePipelineV2RunPlanCandidate carries a duplicate predecessor for task "task-a"',
  );
});

test("16. predecessor for a revision 1 task is rejected", () => {
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1], null, [A1])),
    ),
    'preparePipelineV2RunPlanCandidate carries a predecessor for revision 1 task "task-a"',
  );
});

test("17. foreign-run predecessor is rejected by the chain", () => {
  const foreignA1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one", "run-2");
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [foreignA1, B1])),
    ),
    "validateTaskRevisionChain covers two different runs",
  );
});

test("18. wrong-task predecessor is rejected", () => {
  // the previous set names a task that the candidate itself declares only at revision 1
  const planBOnly = preparedPlan([
    { id: "stage-1", template: "development", tasks: [
      { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] },
    ] },
  ]);
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(planBOnly, [B1], null, [B1]),
      ),
    ),
    'preparePipelineV2RunPlanCandidate carries a predecessor for revision 1 task "task-b"',
  );
  // the previous set names a task the candidate does not declare at all
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [C1, A1, B1])),
    ),
    'preparePipelineV2RunPlanCandidate carries a predecessor for task "task-c" which the candidate does not declare',
  );
});

test("19/20. revision gap and wrong previous digest through the predecessor set", () => {
  const A3 = preparedTask("task-a", 3, A2.sha256, "user_response", "Body A three");
  const gapPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 3, sha256: A3.sha256, depends_on: [] }] },
  ], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(gapPlan, [A3], PLAN_AB_1, [A1])),
    ),
    "validateTaskRevisionChain revision numbers are not consecutive",
  );
  const wrongDigestA2 = preparedTask("task-a", 2, hex("f"), "user_response", "Body A wrong digest");
  const pointerBoundPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 2, sha256: wrongDigestA2.sha256, depends_on: [] }] },
  ], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(candidateOptions(pointerBoundPlan, [wrongDigestA2], PLAN_AB_1, [A1])),
    ),
    "validateTaskRevisionChain previous_sha256 does not name the predecessor digest",
  );
});

test("21/22. exact candidate shape and deep-freeze", () => {
  const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
  expect(Object.keys(candidate).sort()).toEqual(["plan", "task_revisions"]);
  expect(candidate.plan).toBe(PLAN_AB_1);
  expect(candidate.task_revisions).toEqual([A1, B1]);
  expect(Object.isFrozen(candidate)).toBe(true);
  expect(Object.isFrozen(candidate.task_revisions)).toBe(true);
  expect(Object.isFrozen(candidate.plan)).toBe(true);
  expect(Object.isFrozen(candidate.plan.manifest)).toBe(true);
  expect(Object.isFrozen(candidate.task_revisions[0]?.manifest)).toBe(true);
  expect(hasPreparedRunPlanCandidateProvenance(candidate)).toBe(true);
});

test("23. caller mutation isolation and unfrozen caller objects", () => {
  const options = {
    plan: PLAN_AB_1,
    taskRevisions: [A1, B1],
    previousPlan: null,
    previousTaskRevisions: [] as PreparedPipelineV2RunTaskRevision[],
    protectedInputDigest: PROTECTED_DIGEST,
  };
  const candidate = preparePipelineV2RunPlanCandidate(options);
  const otherPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }] },
  ]);
  options.plan = otherPlan;
  options.taskRevisions.push(preparedTask("task-z", 1, null, "planning_proposal", "Extra body"));
  options.previousTaskRevisions.push(A1);
  options.protectedInputDigest = hex("c");
  expect(candidate.plan).toBe(PLAN_AB_1);
  expect(candidate.task_revisions.find((task) => task.manifest.task_id === "task-z")).toBeUndefined();
  expect(candidate.task_revisions).toEqual([A1, B1]);
  expect(Object.isFrozen(options)).toBe(false);
  expect(Object.isFrozen(options.taskRevisions)).toBe(false);
  expect(Object.isFrozen(options.previousTaskRevisions)).toBe(false);
});

test("24. options getters are read exactly once, in order; a second read never happens", () => {
  const reads: string[] = [];
  const options = {
    get plan() {
      reads.push("plan");
      return PLAN_AB_1;
    },
    get taskRevisions() {
      reads.push("taskRevisions");
      return [A1, B1];
    },
    get previousPlan() {
      reads.push("previousPlan");
      return null;
    },
    get previousTaskRevisions() {
      reads.push("previousTaskRevisions");
      return [] as PreparedPipelineV2RunTaskRevision[];
    },
    get protectedInputDigest() {
      reads.push("protectedInputDigest");
      return PROTECTED_DIGEST;
    },
  };
  const candidate = preparePipelineV2RunPlanCandidate(options);
  expect(reads).toEqual(["plan", "taskRevisions", "previousPlan", "previousTaskRevisions", "protectedInputDigest"]);
  expect(candidate.plan).toBe(PLAN_AB_1);

  let planReads = 0;
  const secondReadThrower = {
    get plan() {
      planReads += 1;
      if (planReads > 1) {
        throw new Error("second read");
      }
      return PLAN_AB_1;
    },
    taskRevisions: [A1, B1],
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  };
  expect(() => preparePipelineV2RunPlanCandidate(secondReadThrower)).not.toThrow();
  expect(planReads).toBe(1);

  const boom = new Error("first read");
  let taskReads = 0;
  const firstReadThrower = {
    get plan(): PreparedPipelineV2RunPlanRevision {
      throw boom;
    },
    get taskRevisions() {
      taskReads += 1;
      return [A1, B1];
    },
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  };
  const caught = catchOf(() => preparePipelineV2RunPlanCandidate(firstReadThrower));
  expect(caught).toBe(boom);
  expect(taskReads).toBe(0);
});

test("25. manifest provenance battery: forged arguments are rejected before any field is read", () => {
  const lookAlikePlan = {
    manifest: JSON.parse(JSON.stringify(PLAN_AB_1.manifest)),
    canonical_json: PLAN_AB_1.canonical_json,
    sha256: PLAN_AB_1.sha256,
  };
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(lookAlikePlan as unknown as PreparedPipelineV2RunPlanRevision, [A1, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions({ ...PLAN_AB_1 } as unknown as PreparedPipelineV2RunPlanRevision, [A1, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(structuredClone(PLAN_AB_1) as unknown as PreparedPipelineV2RunPlanRevision, [A1, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(A1 as unknown as PreparedPipelineV2RunPlanRevision, [A1, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_1, [{ ...A1 } as unknown as PreparedPipelineV2RunTaskRevision, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_2, [A2, B2], structuredClone(PLAN_AB_1) as unknown as PreparedPipelineV2RunPlanRevision, [A1, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  expectBindingMessage(
    catchOf(() =>
      preparePipelineV2RunPlanCandidate(
        candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, [structuredClone(A1) as unknown as PreparedPipelineV2RunTaskRevision, B1]),
      ),
    ),
    UNTRUSTED_MANIFEST_MESSAGE,
  );
  // message parity with the binding module's own provenance gate
  const bindingCaught = catchOf(() =>
    validateRootTaskBinding({
      plan: lookAlikePlan as unknown as PreparedPipelineV2RunPlanRevision,
      protectedInputDigest: PROTECTED_DIGEST,
    }),
  );
  expect((bindingCaught as Error).message).toBe(UNTRUSTED_MANIFEST_MESSAGE);
});

test("26. candidate provenance battery: forged candidates are rejected with zero store calls", async () => {
  const realCandidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
  const forged = [
    { plan: PLAN_AB_1, task_revisions: [A1, B1] },
    { ...realCandidate },
    structuredClone(realCandidate),
  ];
  for (const forgedCandidate of forged) {
    const counting = countingOps();
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(
        counting.ops,
        "/nonexistent-run-root",
        forgedCandidate as PreparedPipelineV2RunPlanCandidate,
      ),
    );
    expectBindingMessage(caught, UNTRUSTED_CANDIDATE_MESSAGE);
    expect(counting.taskCalls()).toBe(0);
    expect(counting.planCalls()).toBe(0);
  }
  let trapCount = 0;
  const proxyHandler = {
    get(target: object, property: string | symbol, receiver: unknown): unknown {
      trapCount += 1;
      return Reflect.get(target, property, receiver);
    },
    has(target: object, property: string | symbol): boolean {
      trapCount += 1;
      return Reflect.has(target, property);
    },
  };
  const proxyReal = countingOps();
  const caughtProxyReal = await catchAsyncOf(() =>
    publishPipelineV2RunPlanCandidateWithOps(
      proxyReal.ops,
      "/nonexistent-run-root",
      new Proxy(realCandidate, proxyHandler) as unknown as PreparedPipelineV2RunPlanCandidate,
    ),
  );
  expectBindingMessage(caughtProxyReal, UNTRUSTED_CANDIDATE_MESSAGE);
  const proxyLookalike = countingOps();
  const caughtProxyLookalike = await catchAsyncOf(() =>
    publishPipelineV2RunPlanCandidateWithOps(
      proxyLookalike.ops,
      "/nonexistent-run-root",
      new Proxy({ plan: PLAN_AB_1, task_revisions: [A1, B1] }, proxyHandler) as unknown as PreparedPipelineV2RunPlanCandidate,
    ),
  );
  expectBindingMessage(caughtProxyLookalike, UNTRUSTED_CANDIDATE_MESSAGE);
  expect(trapCount).toBe(0);
  expect(proxyReal.taskCalls()).toBe(0);
  expect(proxyReal.planCalls()).toBe(0);
  expect(proxyLookalike.taskCalls()).toBe(0);
  expect(proxyLookalike.planCalls()).toBe(0);
});

test("27. exact public export surface", async () => {
  const publicNamespace = (await import("../src/pipeline_v2_run_plan_candidate.ts")) as Record<
    string,
    unknown
  >;
  expect(Object.keys(publicNamespace).sort()).toEqual([
    "preparePipelineV2RunPlanCandidate",
    "publishPipelineV2RunPlanCandidate",
  ]);
  const internalNamespace = (await import("../src/pipeline_v2_run_plan_candidate_internal.ts")) as Record<
    string,
    unknown
  >;
  expect(Object.keys(internalNamespace).sort()).toEqual([
    "hasPreparedRunPlanCandidateProvenance",
    "preparePipelineV2RunPlanCandidateCore",
    "publishPipelineV2RunPlanCandidateWithOps",
    "realPipelineV2RunPlanCandidatePublicationOps",
  ]);
});

// --- publication -------------------------------------------------------------

test("28. task-before-plan ordering: tasks first, plan last", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [B1, A1]));
    const log: string[] = [];
    const result = await publishPipelineV2RunPlanCandidateWithOps(recordingOps(log), fixture.runRoot, candidate);
    expect(result).toBe(candidate);
    expect(log).toEqual(["task:task-a", "task:task-b", "plan:1"]);
  } finally {
    await dispose(fixture);
  }
});

test("29. plan is never called after a task failure", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const log: string[] = [];
    const injected = injectedStoreError("not_published", "conflict");
    const failingOps: PipelineV2RunPlanCandidatePublicationOps = {
      publishTaskRevision: async (runRoot, value) => {
        const taskId = (value as PipelineV2RunTaskRevisionManifest).task_id;
        log.push(`task:${taskId}`);
        if (taskId === "task-b") {
          throw injected;
        }
        return await publishPipelineV2TaskRevision(runRoot, value);
      },
      publishPlanRevision: async (runRoot, value) => {
        log.push(`plan:${(value as PipelineV2RunPlanRevisionManifest).revision}`);
        return await publishPipelineV2PlanRevision(runRoot, value);
      },
    };
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(failingOps, fixture.runRoot, candidate),
    );
    expect(caught).toBe(injected);
    expect(log).toEqual(["task:task-a", "task:task-b"]);
  } finally {
    await dispose(fixture);
  }
});

test("30/31. task not_published and durability_unknown pass the original error unchanged", async () => {
  for (const outcome of ["not_published", "durability_unknown"] as const) {
    const fixture = await setup();
    try {
      const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
      const injected = injectedStoreError(outcome, "conflict");
      const log: string[] = [];
      const caught = await catchAsyncOf(() =>
        publishPipelineV2RunPlanCandidateWithOps(
          {
            publishTaskRevision: async () => {
              log.push("task");
              throw injected;
            },
            publishPlanRevision: async () => {
              log.push("plan");
              return await publishPipelineV2PlanRevision(fixture.runRoot, PLAN_AB_1.manifest);
            },
          },
          fixture.runRoot,
          candidate,
        ),
      );
      expect(caught).toBe(injected);
      expect(log).toEqual(["task"]);
    } finally {
      await dispose(fixture);
    }
  }
});

test("32. plan not_published: tasks remain published", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const injected = injectedStoreError("not_published", "io_failure");
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(
        {
          publishTaskRevision: async (runRoot, value) => publishPipelineV2TaskRevision(runRoot, value),
          publishPlanRevision: async () => {
            throw injected;
          },
        },
        fixture.runRoot,
        candidate,
      ),
    );
    expect(caught).toBe(injected);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await lstat(planFilePath(fixture, 1)).then(() => true).catch(() => false)).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("33. plan durability_unknown: no rollback of the published tasks", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const injected = injectedStoreError("durability_unknown", "io_failure");
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(
        {
          publishTaskRevision: async (runRoot, value) => publishPipelineV2TaskRevision(runRoot, value),
          publishPlanRevision: async () => {
            throw injected;
          },
        },
        fixture.runRoot,
        candidate,
      ),
    );
    expect(caught).toBe(injected);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await lstat(planFilePath(fixture, 1)).then(() => true).catch(() => false)).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("34. hostile injected return mismatch is a typed failed publication", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    let planCalls = 0;
    const hostileTaskOps = {
      publishTaskRevision: async () =>
        ({
          task: {
            manifest: { kind: "task_revision", run_id: RUN_ID, task_id: "task-a", revision: 1 },
            canonical_json: "{}",
            sha256: hex("1"),
          },
          task_path: "/somewhere",
        }) as unknown,
      publishPlanRevision: async () => {
        planCalls += 1;
        return await publishPipelineV2PlanRevision(fixture.runRoot, PLAN_AB_1.manifest);
      },
    } as unknown as PipelineV2RunPlanCandidatePublicationOps;
    const taskCaught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(hostileTaskOps, fixture.runRoot, candidate),
    );
    expectStoreError(
      taskCaught,
      "not_published",
      "io_failure",
      "the published task revision manifest does not match the candidate task revision",
    );
    expect(planCalls).toBe(0);

    const hostilePlanOps = {
      publishTaskRevision: async (runRoot: string, value: unknown) =>
        publishPipelineV2TaskRevision(runRoot, value),
      publishPlanRevision: async () =>
        ({
          plan: {
            manifest: { kind: "plan_revision", run_id: RUN_ID, revision: 1 },
            canonical_json: "{}",
            sha256: hex("2"),
          },
          plan_path: "/somewhere",
        }) as unknown,
    } as unknown as PipelineV2RunPlanCandidatePublicationOps;
    const planCaught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(hostilePlanOps, fixture.runRoot, candidate),
    );
    expectStoreError(
      planCaught,
      "not_published",
      "io_failure",
      "the published plan revision manifest does not match the candidate plan revision",
    );
  } finally {
    await dispose(fixture);
  }
});

test("35/36. real filesystem happy path and load round-trip", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(
      candidateOptions(PLAN_AB_1, [B1, A1]),
    );
    const result = await publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate);
    expect(result).toBe(candidate);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await readFile(planFilePath(fixture, 1), "utf8")).toBe(PLAN_AB_1.canonical_json);
    const loadedTask = await loadPipelineV2TaskRevision(fixture.runRoot, "task-a", 1);
    expect(loadedTask?.task.sha256).toBe(A1.sha256);
    expect(loadedTask?.task.canonical_json).toBe(A1.canonical_json);
    const loadedPlan = await loadPipelineV2PlanRevision(fixture.runRoot, 1);
    expect(loadedPlan?.plan.sha256).toBe(PLAN_AB_1.sha256);
    expect(loadedPlan?.plan.canonical_json).toBe(PLAN_AB_1.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("37. exact retry: inode, mode, mtime and bytes preserved", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const first = await publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate);
    expect(first).toBe(candidate);
    const paths = [
      taskFilePath(fixture, "task-a", 1),
      taskFilePath(fixture, "task-b", 1),
      planFilePath(fixture, 1),
    ];
    const identitiesBefore = await Promise.all(paths.map(fileIdentity));
    const secondCandidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const second = await publishPipelineV2RunPlanCandidate(fixture.runRoot, secondCandidate);
    expect(second).toBe(secondCandidate);
    const identitiesAfter = await Promise.all(paths.map(fileIdentity));
    expect(identitiesAfter).toEqual(identitiesBefore);
  } finally {
    await dispose(fixture);
  }
});

test("38. partial task publication: retry with production ops adopts and completes", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(
      candidateOptions(
        preparedPlan([
          { id: "stage-1", template: "development", tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
            { id: "task-c", revision: 1, sha256: C1.sha256, depends_on: ["task-a"] },
          ] },
        ]),
        [A1, B1, C1],
      ),
    );
    const injected = injectedStoreError("not_published", "conflict");
    const log: string[] = [];
    const partialOps: PipelineV2RunPlanCandidatePublicationOps = {
      publishTaskRevision: async (runRoot, value) => {
        const taskId = (value as PipelineV2RunTaskRevisionManifest).task_id;
        log.push(`task:${taskId}`);
        if (taskId === "task-b") {
          throw injected;
        }
        return await publishPipelineV2TaskRevision(runRoot, value);
      },
      publishPlanRevision: async (runRoot, value) => {
        log.push(`plan:${(value as PipelineV2RunPlanRevisionManifest).revision}`);
        return await publishPipelineV2PlanRevision(runRoot, value);
      },
    };
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(partialOps, fixture.runRoot, candidate),
    );
    expect(caught).toBe(injected);
    expect(log).toEqual(["task:task-a", "task:task-b"]);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    const adoptedIno = (await lstat(taskFilePath(fixture, "task-a", 1))).ino;

    const retried = await publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate);
    expect(retried).toBe(candidate);
    expect((await lstat(taskFilePath(fixture, "task-a", 1))).ino).toBe(adoptedIno);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-c", 1), "utf8")).toBe(C1.canonical_json);
    expect(await readFile(planFilePath(fixture, 1), "utf8")).toBe(candidate.plan.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("39. conflicting task target: loser stops before the plan, target unchanged", async () => {
  const fixture = await setup();
  try {
    const winner = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    expect(await publishPipelineV2RunPlanCandidate(fixture.runRoot, winner)).toBe(winner);
    const winnerPlanIdentity = await fileIdentity(planFilePath(fixture, 1));
    const b1Other = preparedTask("task-b", 1, null, "planning_proposal", "Body B other");
    const loserPlan = preparedPlan([
      { id: "stage-1", template: "development", tasks: [
        { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
        { id: "task-b", revision: 1, sha256: b1Other.sha256, depends_on: ["task-a"] },
      ] },
    ]);
    const loser = preparePipelineV2RunPlanCandidate(candidateOptions(loserPlan, [A1, b1Other]));
    const log: string[] = [];
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(recordingOps(log), fixture.runRoot, loser),
    );
    expectStoreError(caught, "not_published", "conflict");
    expect(log).toEqual(["task:task-a", "task:task-b"]);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await fileIdentity(planFilePath(fixture, 1))).toEqual(winnerPlanIdentity);
  } finally {
    await dispose(fixture);
  }
});

test("40. conflicting plan target: tasks adopted, plan winner unchanged", async () => {
  const fixture = await setup();
  try {
    const planA = preparedPlan([STAGE_AB_1]);
    // planB changes only the plan's own content (the depends_on edge) while
    // binding the same task revisions — same plan path, different bytes
    const planB = preparedPlan([
      { id: "stage-1", template: "development", tasks: [
        { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
        { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: [] },
      ] },
    ]);
    expect(planB.sha256).not.toBe(planA.sha256);
    const candidateA = preparePipelineV2RunPlanCandidate(candidateOptions(planA, [A1, B1]));
    expect(await publishPipelineV2RunPlanCandidate(fixture.runRoot, candidateA)).toBe(candidateA);
    const winnerIdentity = await fileIdentity(planFilePath(fixture, 1));
    const candidateB = preparePipelineV2RunPlanCandidate(candidateOptions(planB, [A1, B1]));
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidateB),
    );
    expectStoreError(caught, "not_published", "conflict");
    expect(await readFile(planFilePath(fixture, 1), "utf8")).toBe(planA.canonical_json);
    expect(await fileIdentity(planFilePath(fixture, 1))).toEqual(winnerIdentity);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("41. identical concurrent candidates both succeed without replacing artifacts", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const [first, second] = await Promise.all([
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate),
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate),
    ]);
    expect(first).toBe(candidate);
    expect(second).toBe(candidate);
    const paths = [
      taskFilePath(fixture, "task-a", 1),
      taskFilePath(fixture, "task-b", 1),
      planFilePath(fixture, 1),
    ];
    expect(await readFile(paths[0] as string, "utf8")).toBe(A1.canonical_json);
    expect(await readFile(paths[1] as string, "utf8")).toBe(B1.canonical_json);
    expect(await readFile(paths[2] as string, "utf8")).toBe(PLAN_AB_1.canonical_json);
    const identities = await Promise.all(paths.map(fileIdentity));
    await publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate);
    expect(await Promise.all(paths.map(fileIdentity))).toEqual(identities);
  } finally {
    await dispose(fixture);
  }
});

test("42. conflicting concurrent candidates: one plan wins, loser tasks remain orphans", async () => {
  const fixture = await setup();
  try {
    const planA = preparedPlan([
      { id: "stage-1", template: "development", tasks: [
        { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
        { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
      ] },
    ]);
    const planB = preparedPlan([
      { id: "stage-1", template: "development", tasks: [
        { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
        { id: "task-c", revision: 1, sha256: C1.sha256, depends_on: ["task-a"] },
      ] },
    ]);
    const candidateA = preparePipelineV2RunPlanCandidate(candidateOptions(planA, [A1, B1]));
    const candidateB = preparePipelineV2RunPlanCandidate(candidateOptions(planB, [A1, C1]));
    const settled = await Promise.allSettled([
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidateA),
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidateB),
    ]);
    const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
    const rejected = settled.filter((entry) => entry.status === "rejected");
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    const rejectedReason = (rejected[0] as PromiseRejectedResult).reason;
    expectStoreError(rejectedReason, "not_published", "conflict");
    const planBytes = await readFile(planFilePath(fixture, 1), "utf8");
    const winnerIsA = planBytes === planA.canonical_json;
    const winnerIsB = planBytes === planB.canonical_json;
    expect(winnerIsA !== winnerIsB).toBe(true);
    // both extra task artifacts exist; the loser's is an orphan boundary
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-c", 1), "utf8")).toBe(C1.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("43. per-call isolation: injected ops never reach the parallel public call", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const injected = injectedStoreError("not_published", "conflict");
    let taskCalls = 0;
    let planCalls = 0;
    const failingOps: PipelineV2RunPlanCandidatePublicationOps = {
      publishTaskRevision: async () => {
        taskCalls += 1;
        throw injected;
      },
      publishPlanRevision: async () => {
        planCalls += 1;
        return await publishPipelineV2PlanRevision(fixture.runRoot, PLAN_AB_1.manifest);
      },
    };
    const settled = await Promise.allSettled([
      publishPipelineV2RunPlanCandidateWithOps(failingOps, fixture.runRoot, candidate),
      publishPipelineV2RunPlanCandidate(fixture.runRoot, candidate),
    ]);
    expect((settled[0] as PromiseRejectedResult).reason).toBe(injected);
    expect((settled[1] as PromiseFulfilledResult<PreparedPipelineV2RunPlanCandidate>).value).toBe(candidate);
    expect(taskCalls).toBe(1);
    expect(planCalls).toBe(0);
    expect(await readFile(taskFilePath(fixture, "task-a", 1), "utf8")).toBe(A1.canonical_json);
    expect(await readFile(taskFilePath(fixture, "task-b", 1), "utf8")).toBe(B1.canonical_json);
    expect(await readFile(planFilePath(fixture, 1), "utf8")).toBe(PLAN_AB_1.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("44. preparation and publication diagnostics never echo bodies, JSON, canaries or caller paths", async () => {
  // chain failure whose failing manifests carry canary bodies
  const canaryPlan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [
      { id: "task-a", revision: 2, sha256: CANARY_A2.sha256, depends_on: [] },
    ] },
  ], { revision: 2, previous_sha256: PLAN_AB_1.sha256 });
  const canaryCaught = catchOf(() =>
    preparePipelineV2RunPlanCandidate(
      candidateOptions(canaryPlan, [CANARY_A2], PLAN_AB_1, [CANARY_A1]),
    ),
  );
  expectBindingMessage(
    canaryCaught,
    "validateTaskRevisionChain previous_sha256 does not name the predecessor digest",
  );
  // missing-predecessor failure with canary bodies in both revisions
  const missingPredecessorCaught = catchOf(() =>
    preparePipelineV2RunPlanCandidate(
      candidateOptions(canaryPlan, [CANARY_A2], PLAN_AB_1, []),
    ),
  );
  expectBindingMessage(
    missingPredecessorCaught,
    'preparePipelineV2RunPlanCandidate is missing the predecessor for task "task-a"',
  );
  // forged candidate through the publisher gate
  const forgedCaught = await catchAsyncOf(() =>
    publishPipelineV2RunPlanCandidateWithOps(
      realPipelineV2RunPlanCandidatePublicationOps,
      "/nonexistent-run-root",
      { plan: canaryPlan, task_revisions: [CANARY_A2] } as unknown as PreparedPipelineV2RunPlanCandidate,
    ),
  );
  expectBindingMessage(forgedCaught, UNTRUSTED_CANDIDATE_MESSAGE);
  for (const caught of [canaryCaught, missingPredecessorCaught, forgedCaught]) {
    const message = (caught as Error).message;
    expect(message).not.toContain(CANARY);
    expect(message).not.toContain("body with");
    expect(message).not.toContain("revised with");
    expect(message).not.toContain("{");
    expect(message).not.toContain(tmpdir());
  }
});

test("45. source scan: single-owner layer with no forbidden imports or seams", async () => {
  const internalSource = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_run_plan_candidate_internal.ts"),
    "utf8",
  );
  const publicSource = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_run_plan_candidate.ts"),
    "utf8",
  );
  for (const source of [internalSource, publicSource]) {
    expect(source.includes("pipeline_state")).toBe(false);
    expect(source.includes("pipeline_v2_state")).toBe(false);
    expect(source.includes("coordinator")).toBe(false);
    expect(source.includes("pipeline_runner")).toBe(false);
    expect(source.includes("CryptoHasher")).toBe(false);
    expect(source.includes("createHash")).toBe(false);
    expect(source.includes("canonicalJson")).toBe(false);
    expect(source.includes("isPipelineV2SafeId")).toBe(false);
    expect(source.includes("isLowercaseSha256")).toBe(false);
    expect(source.includes("/^[")).toBe(false);
    expect(source.includes(".message")).toBe(false);
    expect(source.includes("Promise.all")).toBe(false);
    expect(source.includes("let real")).toBe(false);
  }
  expect(Object.isFrozen(realPipelineV2RunPlanCandidatePublicationOps)).toBe(true);
  const internalNamespace = (await import("../src/pipeline_v2_run_plan_candidate_internal.ts")) as Record<
    string,
    unknown
  >;
  for (const key of Object.keys(internalNamespace)) {
    expect(key.toLowerCase()).not.toContain("install");
    expect(key.toLowerCase()).not.toContain("reset");
    expect(key.toLowerCase()).not.toContain("setops");
  }
});

test("46. single-snapshot current array: a mutating Proxy array cannot swap the bound task", () => {
  const goodTask = preparedTask("task-d", 1, null, "planning_proposal", "Good body");
  const rogueTask = preparedTask("task-d", 1, null, "planning_proposal", "Rogue body");
  expect(goodTask.sha256).not.toBe(rogueTask.sha256);
  const plan = preparedPlan([
    { id: "stage-1", template: "development", tasks: [
      { id: "task-d", revision: 1, sha256: goodTask.sha256, depends_on: [] },
    ] },
  ]);
  const backing = [goodTask];
  let iteratorReads = 0;
  const rogueArray = new Proxy(backing, {
    get(target: PreparedPipelineV2RunTaskRevision[], property: string | symbol, receiver: unknown): unknown {
      if (property === Symbol.iterator) {
        iteratorReads += 1;
        const values = iteratorReads === 1 ? target : [rogueTask];
        return function* (): Generator<PreparedPipelineV2RunTaskRevision> {
          yield* values;
        };
      }
      return Reflect.get(target, property, receiver);
    },
  }) as unknown as PreparedPipelineV2RunTaskRevision[];
  const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(plan, rogueArray));
  // pre-fix regression (99182c1): the array was re-read after validation —
  // the first iterator read fed [goodTask] to the provenance gate and the
  // index reads of validatePlanTaskBindings, while the map-building loop's
  // second iterator read received [rogueTask]; every read was a provenance-
  // valid prepared object, so the candidate was accepted with
  // candidate.task_revisions[0] === rogueTask and a plan pointer naming
  // goodTask.sha256 — an internally inconsistent filesystem commit marker.
  // After the single-snapshot fix each caller array is materialized exactly
  // once and every later step uses only that snapshot.
  expect(iteratorReads).toBe(1);
  expect(candidate.task_revisions[0]).toBe(goodTask);
  const pointer = candidate.plan.manifest.stages[0]?.tasks[0];
  expect(pointer?.sha256).toBe(goodTask.sha256);
  expect(candidate.task_revisions[0]?.sha256).toBe(pointer?.sha256);
  // the caller's backing array is untouched
  expect(backing[0]).toBe(goodTask);
  expect(Object.isFrozen(backing)).toBe(false);
});

test("47. single-snapshot previous array: exactly one iterator read", () => {
  const backing = [A1, B1];
  let iteratorReads = 0;
  const previousArray = new Proxy(backing, {
    get(target: PreparedPipelineV2RunTaskRevision[], property: string | symbol, receiver: unknown): unknown {
      if (property === Symbol.iterator) {
        iteratorReads += 1;
        if (iteratorReads > 1) {
          throw new Error("SECOND_PREVIOUS_ITERATOR_READ");
        }
        return function* (): Generator<PreparedPipelineV2RunTaskRevision> {
          yield* target;
        };
      }
      return Reflect.get(target, property, receiver);
    },
  }) as unknown as PreparedPipelineV2RunTaskRevision[];
  // pre-fix regression (99182c1): the previous array was read twice (the
  // provenance gate loop, then the predecessor-map loop), so preparation
  // aborted with the second read's sentinel error. After the single-
  // snapshot fix the array is materialized exactly once and the second
  // read never happens.
  const candidate = preparePipelineV2RunPlanCandidate(
    candidateOptions(PLAN_AB_2, [A2, B2], PLAN_AB_1, previousArray),
  );
  expect(iteratorReads).toBe(1);
  expect(candidate.task_revisions[0]?.manifest.previous_sha256).toBe(A1.sha256);
  expect(candidate.task_revisions[1]?.manifest.previous_sha256).toBe(B1.sha256);
  // the caller's backing array is untouched
  expect(backing[0]).toBe(A1);
  expect(backing[1]).toBe(B1);
  expect(Object.isFrozen(backing)).toBe(false);
});

test("48. publication ops getters are read exactly once, in order", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const calls: string[] = [];
    let taskGetterReads = 0;
    let planGetterReads = 0;
    const ops = {
      get publishTaskRevision() {
        taskGetterReads += 1;
        if (taskGetterReads > 1) {
          throw new Error("SECOND_TASK_OP_READ");
        }
        return async (runRoot: string, value: unknown) => {
          calls.push(`task:${(value as PipelineV2RunTaskRevisionManifest).task_id}`);
          return await publishPipelineV2TaskRevision(runRoot, value);
        };
      },
      get publishPlanRevision() {
        planGetterReads += 1;
        if (planGetterReads > 1) {
          throw new Error("SECOND_PLAN_OP_READ");
        }
        return async (runRoot: string, value: unknown) => {
          calls.push(`plan:${(value as PipelineV2RunPlanRevisionManifest).revision}`);
          return await publishPipelineV2PlanRevision(runRoot, value);
        };
      },
    };
    const result = await publishPipelineV2RunPlanCandidateWithOps(
      ops as unknown as PipelineV2RunPlanCandidatePublicationOps,
      fixture.runRoot,
      candidate,
    );
    expect(result).toBe(candidate);
    expect(taskGetterReads).toBe(1);
    expect(planGetterReads).toBe(1);
    expect(calls).toEqual(["task:task-a", "task:task-b", "plan:1"]);
  } finally {
    await dispose(fixture);
  }
});

test("49. a throwing first ops getter propagates unchanged with zero store calls", async () => {
  const fixture = await setup();
  try {
    const candidate = preparePipelineV2RunPlanCandidate(candidateOptions(PLAN_AB_1, [A1, B1]));
    const boom = new Error("first task op read");
    let planGetterReads = 0;
    let planCalls = 0;
    const ops = {
      get publishTaskRevision(): PipelineV2RunPlanCandidatePublicationOps["publishTaskRevision"] {
        throw boom;
      },
      get publishPlanRevision() {
        planGetterReads += 1;
        return async (runRoot: string, value: unknown) => {
          planCalls += 1;
          return await publishPipelineV2PlanRevision(runRoot, value);
        };
      },
    };
    const caught = await catchAsyncOf(() =>
      publishPipelineV2RunPlanCandidateWithOps(
        ops as unknown as PipelineV2RunPlanCandidatePublicationOps,
        fixture.runRoot,
        candidate,
      ),
    );
    expect(caught).toBe(boom);
    expect(planGetterReads).toBe(0);
    expect(planCalls).toBe(0);
    // candidate fields are not mutated
    expect(candidate.plan).toBe(PLAN_AB_1);
    expect(Object.isFrozen(candidate)).toBe(true);
    // no task artifact was published
    expect(
      await readFile(taskFilePath(fixture, "task-a", 1), "utf8").then(() => true).catch(() => false),
    ).toBe(false);
  } finally {
    await dispose(fixture);
  }
});
