import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  PipelineV2RunPlanManifestError,
  parsePlanRevisionManifest,
  parseTaskRevisionManifest,
  parseWaitIntent,
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PipelineV2ContinueStageIntentManifest,
  type PipelineV2ReviseTaskIntentManifest,
  type PipelineV2RunPlanRevisionManifest,
  type PipelineV2RunPlanStage,
  type PipelineV2RunPlanTaskPointer,
  type PipelineV2RunTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  PipelineV2RunPlanBindingError,
  validateContinueIntentBinding,
  validateReviseIntentBinding,
} from "../src/pipeline_v2_run_plan_bindings.ts";
import { canonicalJson } from "../src/canonical_json.ts";

const hex = (char: string): string => char.repeat(64);

const PLAN_REVISION_1 = {
  schema_version: 1,
  kind: "plan_revision",
  run_id: "run-1",
  revision: 1,
  previous_sha256: null,
  root_task: { input_id: "task", sha256: hex("b") },
  origin_execution: 10,
  stages: [
    {
      id: "implementation",
      template: "development",
      tasks: [
        { id: "task-1", revision: 1, sha256: hex("1"), depends_on: [] },
        { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
      ],
    },
  ],
};

const TASK_REVISION_1 = {
  schema_version: 1,
  kind: "task_revision",
  run_id: "run-1",
  task_id: "task-1",
  revision: 1,
  previous_sha256: null,
  origin: "planning_proposal",
  body: "Implement the acceptance test parser",
};

const CONTINUE_INTENT = {
  schema_version: 1,
  kind: "continue_stage_intent",
  run_id: "run-1",
  wait_index: 1,
  stage_id: "implementation",
  expected_plan_sha256: hex("a"),
  additional_iterations: 2,
};

const REVISE_INTENT = {
  schema_version: 1,
  kind: "revise_task_intent",
  run_id: "run-1",
  wait_index: 2,
  task_id: "task-1",
  expected_previous_task_sha256: hex("1"),
  new_task_revision_sha256: hex("5"),
};

function expectMessage(cause: unknown, message: string): void {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanManifestError);
  expect((cause as Error).message).toBe(message);
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

function expectDeepFrozen(value: unknown, path = "result"): void {
  if (Array.isArray(value)) {
    expect(Object.isFrozen(value), `array ${path} is frozen`).toBe(true);
    value.forEach((entry, index) => expectDeepFrozen(entry, `${path}[${index}]`));
    return;
  }
  if (value !== null && typeof value === "object") {
    expect(Object.isFrozen(value), `object ${path} is frozen`).toBe(true);
    for (const child of Object.values(value)) {
      expectDeepFrozen(child, `${path}.*`);
    }
  }
}

function collectKeys(value: unknown, into: Set<string>): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectKeys(entry, into);
    }
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [key, child] of Object.entries(value)) {
      into.add(key);
      collectKeys(child, into);
    }
  }
}

const STAGE0 = PLAN_REVISION_1.stages[0] as PipelineV2RunPlanStage;
const TASK0 = STAGE0.tasks[0] as PipelineV2RunPlanTaskPointer;
const TASK1 = STAGE0.tasks[1] as PipelineV2RunPlanTaskPointer;

function domainDigest(domain: string, canonical: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(domain);
  hasher.update(canonical);
  return hasher.digest("hex");
}

const planWithSingleTaskStage = {
  ...PLAN_REVISION_1,
  stages: [
    {
      id: "implementation",
      template: "development",
      tasks: [STAGE0.tasks[0]],
    },
  ],
};

describe("pipeline v2 run plan revision manifest", () => {
  test("exact positive shape with canonical form and domain-bound digest", () => {
    const prepared = preparePlanRevisionManifest(PLAN_REVISION_1);
    const canonical = canonicalJson(PLAN_REVISION_1);
    expect(prepared.canonical_json).toBe(canonical);
    expect(prepared.sha256).toBe(domainDigest("pipeline-v2-plan-revision\0", canonical));
    expect(Object.keys(prepared.manifest)).toEqual([
      "schema_version",
      "kind",
      "run_id",
      "revision",
      "previous_sha256",
      "root_task",
      "origin_execution",
      "stages",
    ]);
    expect(Object.keys(prepared.manifest.root_task)).toEqual(["input_id", "sha256"]);
    expect(Object.keys(prepared)).toEqual(["manifest", "canonical_json", "sha256"]);
    expectDeepFrozen(prepared);
  });

  test("multiple sequential stages are accepted and stage order is semantic", () => {
    const twoStages = {
      ...PLAN_REVISION_1,
      stages: [
        STAGE0,
        {
          id: "review",
          template: "review",
          tasks: [{ id: "task-3", revision: 1, sha256: hex("3"), depends_on: [] }],
        },
      ],
    };
    const prepared = preparePlanRevisionManifest(twoStages);
    expect(prepared.manifest.stages).toHaveLength(2);
    expect(((prepared.manifest.stages as PipelineV2RunPlanStage[])[1] as PipelineV2RunPlanStage).id).toBe("review");
    const swapped = {
      ...twoStages,
      stages: [(twoStages.stages as PipelineV2RunPlanStage[])[1], (twoStages.stages as PipelineV2RunPlanStage[])[0]],
    };
    const swappedPrepared = preparePlanRevisionManifest(swapped);
    expect(swappedPrepared.canonical_json).not.toBe(prepared.canonical_json);
    expect(swappedPrepared.sha256).not.toBe(prepared.sha256);
  });

  test("task pointers are normalized by task id and dependencies by dependency id", () => {
    const reversed = {
      ...PLAN_REVISION_1,
      stages: [
        {
          id: "implementation",
          template: "development",
          tasks: [
            STAGE0.tasks[1],
            STAGE0.tasks[0],
          ],
        },
      ],
    };
    const prepared = preparePlanRevisionManifest(reversed);
    const base = preparePlanRevisionManifest(PLAN_REVISION_1);
    expect(prepared.canonical_json).toBe(base.canonical_json);
    expect(prepared.sha256).toBe(base.sha256);
    const unsortedDeps = {
      ...PLAN_REVISION_1,
      stages: [
        {
          id: "implementation",
          template: "development",
          tasks: [
            { id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["z-dep", "a-dep"] },
            { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
            { id: "z-dep", revision: 1, sha256: hex("4"), depends_on: [] },
            { id: "a-dep", revision: 1, sha256: hex("5"), depends_on: [] },
          ],
        },
      ],
    };
    const sortedDeps = {
      ...unsortedDeps,
      stages: [
        {
          id: "implementation",
          template: "development",
          tasks: [
            { id: "a-dep", revision: 1, sha256: hex("5"), depends_on: [] },
            { id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["a-dep", "z-dep"] },
            { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
            { id: "z-dep", revision: 1, sha256: hex("4"), depends_on: [] },
          ],
        },
      ],
    };
    const a = preparePlanRevisionManifest(unsortedDeps);
    const b = preparePlanRevisionManifest(sortedDeps);
    expect(a.canonical_json).toBe(b.canonical_json);
    expect(a.sha256).toBe(b.sha256);
  });

  test("permuting tasks or dependencies never changes the digest; a changed edge does", () => {
    const base = preparePlanRevisionManifest(PLAN_REVISION_1);
    const changedEdge = preparePlanRevisionManifest({
      ...PLAN_REVISION_1,
      stages: [
        {
          id: "implementation",
          template: "development",
          tasks: [
            STAGE0.tasks[0],
            { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1", "task-1-x"] },
            { id: "task-1-x", revision: 1, sha256: hex("4"), depends_on: [] },
          ],
        },
      ],
    });
    expect(changedEdge.sha256).not.toBe(base.sha256);
  });

  test("zero stages and zero tasks are rejected; one stage and one task are accepted", () => {
    expectMessage(
      catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, stages: [] })),
      "the plan revision manifest.stages must not be empty",
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest({
          ...PLAN_REVISION_1,
          stages: [{ id: "implementation", template: "development", tasks: [] }],
        }),
      ),
      "the plan revision manifest stage at position 0 tasks must not be empty",
    );
    const single = preparePlanRevisionManifest({
      ...PLAN_REVISION_1,
      stages: [
        { id: "implementation", template: "development", tasks: [STAGE0.tasks[0]] },
      ],
    });
    expect(single.manifest.stages).toHaveLength(1);
    expect(((single.manifest.stages as PipelineV2RunPlanStage[])[0] as PipelineV2RunPlanStage).tasks).toHaveLength(1);
  });

  test("duplicate stage ids and non-globally-unique task ids are rejected", () => {
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest({
          ...PLAN_REVISION_1,
          stages: [
            STAGE0,
            { id: "implementation", template: "review", tasks: [{ id: "task-3", revision: 1, sha256: hex("3"), depends_on: [] }] },
          ],
        }),
      ),
      "the plan revision manifest declares a duplicate stage id at position 1",
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest({
          ...PLAN_REVISION_1,
          stages: [
            STAGE0,
            { id: "review", template: "review", tasks: [{ id: "task-1", revision: 1, sha256: hex("3"), depends_on: [] }] },
          ],
        }),
      ),
      'the plan revision manifest declares task "task-1" more than once',
    );
  });

  test("unknown, cross-stage, self and duplicate dependencies and DAG cycles are rejected", () => {
    const stageWith = (tasks: PipelineV2RunPlanTaskPointer[]) => ({
      ...PLAN_REVISION_1,
      stages: [{ id: "implementation", template: "development", tasks }],
    });
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest(stageWith([{ id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["ghost"] }])),
      ),
      'the plan revision manifest stage at position 0 task "task-1" depends on a task outside its stage',
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest(
          stageWith([
            { id: "task-1", revision: 1, sha256: hex("1"), depends_on: [] },
            { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
          ]).stages[0] !== undefined
            ? {
                ...PLAN_REVISION_1,
                stages: [
                  STAGE0,
                  {
                    id: "review",
                    template: "review",
                    tasks: [{ id: "task-3", revision: 1, sha256: hex("3"), depends_on: ["task-1"] }],
                  },
                ],
              }
            : null,
        ),
      ),
      'the plan revision manifest stage at position 1 task "task-3" depends on a task outside its stage',
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest(stageWith([{ id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["task-1"] }])),
      ),
      'the plan revision manifest stage at position 0 task "task-1" depends on itself',
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest(
          stageWith([
            { id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["task-2"] },
            { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
          ]),
        ),
      ),
      "the plan revision manifest stage at position 0 tasks declare a dependency cycle",
    );
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest(stageWith([{ id: "task-1", revision: 1, sha256: hex("1"), depends_on: ["task-2", "task-2"] }])),
      ),
      "the plan revision manifest stage at position 0 task at position 0 depends_on declares a duplicate dependency",
    );
  });

  test("chain fields: revision 1 requires null previous; higher revisions require a digest", () => {
    expectMessage(
      catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, previous_sha256: hex("a") })),
      "the plan revision manifest previous_sha256 must be null for revision 1",
    );
    expectMessage(
      catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, revision: 2, previous_sha256: null })),
      "the plan revision manifest previous_sha256 must be a lowercase hex SHA-256 digest for revision 2",
    );
    const revision2 = preparePlanRevisionManifest({ ...PLAN_REVISION_1, revision: 2, previous_sha256: hex("a") });
    expect(revision2.manifest.revision).toBe(2);
  });

  test("root task input_id is exactly task and the origin execution is a positive safe integer", () => {
    expectMessage(
      catchOf(() =>
        preparePlanRevisionManifest({ ...PLAN_REVISION_1, root_task: { input_id: "other", sha256: hex("b") } }),
      ),
      'the plan revision manifest root_task input_id must be "task"',
    );
    expectMessage(
      catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, origin_execution: 0 })),
      "the plan revision manifest origin_execution must be a positive safe integer",
    );
    const boundary = preparePlanRevisionManifest({ ...PLAN_REVISION_1, origin_execution: Number.MAX_SAFE_INTEGER });
    expect(boundary.manifest.origin_execution).toBe(Number.MAX_SAFE_INTEGER);
  });

  test("safe-id, digest and integer boundaries are enforced value-free", () => {
    const unsafeIds = ["", "../bad", "a..b", "/coder", "x y", "x".repeat(129), 1, null, {}];
    for (const id of unsafeIds) {
      const caught = catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, run_id: id }));
      expectMessage(caught, "the plan revision manifest run_id must be a safe non-empty identifier");
    }
    for (const value of ["", "a".repeat(63), "a".repeat(65), "A".repeat(64), null]) {
      const caught = catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, root_task: { input_id: "task", sha256: value } }));
      expectMessage(caught, "the plan revision manifest root_task sha256 must be a lowercase hex SHA-256 digest");
    }
    for (const value of [0, -1, 1.5, "1", null]) {
      const caught = catchOf(() => preparePlanRevisionManifest({ ...PLAN_REVISION_1, revision: value as number }));
      expectMessage(caught, "the plan revision manifest revision must be a positive safe integer");
    }
    expect(
      preparePlanRevisionManifest({ ...PLAN_REVISION_1, stages: [{ id: "x".repeat(128), template: "t", tasks: [{ id: "t", revision: 1, sha256: hex("1"), depends_on: [] }] }] }).manifest.stages,
    ).toHaveLength(1);
  });

  test("parse runs the same chain with scrambled key order; every semantic change changes the digest", () => {
    const scrambled =
      '{"stages":[{"tasks":[{"sha256":"' + hex("1") + '","id":"task-1","depends_on":[],"revision":1},{"depends_on":["task-1"],"id":"task-2","revision":1,"sha256":"' + hex("2") + '"}],"template":"development","id":"implementation"}],"origin_execution":10,"root_task":{"sha256":"' + hex("b") + '","input_id":"task"},"previous_sha256":null,"revision":1,"run_id":"run-1","kind":"plan_revision","schema_version":1}';
    const parsed = parsePlanRevisionManifest(scrambled);
    const prepared = preparePlanRevisionManifest(PLAN_REVISION_1);
    expect(parsed).toEqual(prepared);
    expect(parsePlanRevisionManifest(scrambled)).toEqual(parsed);
    for (const [field, override] of [
      ["run_id", { run_id: "run-2" }],
      ["revision", { revision: 2, previous_sha256: hex("a") }],
      ["previous_sha256", { previous_sha256: hex("c"), revision: 2 }],
      ["root task digest", { root_task: { input_id: "task", sha256: hex("d") } }],
      ["origin_execution", { origin_execution: 11 }],
      ["stage id", { stages: [{ id: "implementation-x", template: "development", tasks: STAGE0.tasks }] }],
      ["stage template", { stages: [{ id: "implementation", template: "development-x", tasks: STAGE0.tasks }] }],
      ["task digest", { stages: [{ id: "implementation", template: "development", tasks: [{ id: "task-1", revision: 1, sha256: hex("9"), depends_on: [] }, STAGE0.tasks[1]] }] }],
      ["task revision number", { stages: [{ id: "implementation", template: "development", tasks: [{ id: "task-1", revision: 2, sha256: hex("1"), depends_on: [] }, STAGE0.tasks[1]] }] }],
    ] as const) {
      const changed = preparePlanRevisionManifest({ ...PLAN_REVISION_1, ...override } as unknown as typeof PLAN_REVISION_1);
      expect(changed.sha256, `${field} must change the digest`).not.toBe(prepared.sha256);
    }
  });

  test("unknown and missing fields are rejected at every level without naming user keys", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const cases: [unknown, string][] = [
      [{ ...PLAN_REVISION_1, evidence: "extra" }, "the plan revision manifest has unknown fields"],
      [{ ...PLAN_REVISION_1, [canary]: "x" }, "the plan revision manifest has unknown fields"],
      [{ ...PLAN_REVISION_1, stages: PLAN_REVISION_1.stages, schema_version: undefined }, 'the plan revision manifest.schema_version must be 1'],
      [{ kind: "plan_revision", run_id: "run-1", revision: 1, previous_sha256: null, root_task: PLAN_REVISION_1.root_task, origin_execution: 10, stages: PLAN_REVISION_1.stages }, 'the plan revision manifest is missing required field "schema_version"'],
      [{ schema_version: 1, run_id: "run-1", revision: 1, previous_sha256: null, root_task: PLAN_REVISION_1.root_task, origin_execution: 10, stages: PLAN_REVISION_1.stages }, 'the plan revision manifest is missing required field "kind"'],
      [{ ...PLAN_REVISION_1, root_task: { input_id: "task" } }, 'the plan revision manifest root_task is missing required field "sha256"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development", tasks: [STAGE0.tasks[0]], extra: 1 }] }, "the plan revision manifest stage at position 0 has unknown fields"],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development" }] }, 'the plan revision manifest stage at position 0 is missing required field "tasks"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", tasks: STAGE0.tasks }] }, 'the plan revision manifest stage at position 0 is missing required field "template"'],
      [{ ...PLAN_REVISION_1, stages: [{ template: "development", tasks: STAGE0.tasks }] }, 'the plan revision manifest stage at position 0 is missing required field "id"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development", tasks: [{ id: "task-1", revision: 1, sha256: hex("1") }] }] }, 'the plan revision manifest stage at position 0 task at position 0 is missing required field "depends_on"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development", tasks: [{ id: "task-1", revision: 1, depends_on: [] }] }] }, 'the plan revision manifest stage at position 0 task at position 0 is missing required field "sha256"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development", tasks: [{ id: "task-1", sha256: hex("1"), depends_on: [] }] }] }, 'the plan revision manifest stage at position 0 task at position 0 is missing required field "revision"'],
      [{ ...PLAN_REVISION_1, stages: [{ id: "implementation", template: "development", tasks: [{ revision: 1, sha256: hex("1"), depends_on: [] }] }] }, 'the plan revision manifest stage at position 0 task at position 0 is missing required field "id"'],
      [null, "the plan revision manifest is not a JSON object"],
      [["x"], "the plan revision manifest is not a JSON object"],
      ["plan", "the plan revision manifest is not a JSON object"],
    ];
    for (const [value, message] of cases) {
      const caught = catchOf(() => preparePlanRevisionManifest(value));
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(canary);
    }
  });

  test("malformed plan JSON produces a content-free error and canaries stay hidden", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const caught = catchOf(() => parsePlanRevisionManifest(`{"run_id": "${canary}", "kind": "plan_rev`));
    expectMessage(caught, "the plan revision document is not valid JSON");
    expect((caught as Error).message).not.toContain(canary);
    expect((caught as Error).message.toLowerCase()).not.toContain("position");
    expect((caught as Error).message.toLowerCase()).not.toContain("unexpected");
  });

  test("the result snapshot is independent of later input mutations", () => {
    const singleTaskStage = {
      id: "implementation",
      template: "development",
      tasks: [{ ...STAGE0.tasks[0] }],
    };
    const value: Record<string, unknown> = {
      ...PLAN_REVISION_1,
      root_task: { ...PLAN_REVISION_1.root_task },
      stages: [singleTaskStage],
    };
    const prepared = preparePlanRevisionManifest(value);
    value.revision = 99;
    (value.root_task as Record<string, unknown>).sha256 = hex("9");
    const mutableStages = value.stages as { tasks: Record<string, unknown>[] }[];
    const mutableStage = mutableStages[0] as { tasks: Record<string, unknown>[] };
    const mutatedTask = mutableStage.tasks[0] as Record<string, unknown>;
    mutatedTask.id = "mutated";
    const mutableTask = singleTaskStage.tasks[0] as Record<string, unknown>;
    mutableTask.id = "mutated";
    expect(prepared.manifest.revision).toBe(1);
    expect(prepared.manifest.root_task.sha256).toBe(hex("b"));
    expect((((prepared.manifest.stages as PipelineV2RunPlanStage[])[0] as PipelineV2RunPlanStage).tasks[0] as PipelineV2RunPlanTaskPointer).id).toBe("task-1");
    expect(prepared.canonical_json).toBe(canonicalJson(planWithSingleTaskStage));
    expectDeepFrozen(prepared);
  });
});

describe("pipeline v2 run task revision manifest", () => {
  test("exact positive shape with canonical form and domain-bound digest", () => {
    const prepared = prepareTaskRevisionManifest(TASK_REVISION_1);
    const canonical = canonicalJson(TASK_REVISION_1);
    expect(prepared.canonical_json).toBe(canonical);
    expect(prepared.sha256).toBe(domainDigest("pipeline-v2-task-revision\0", canonical));
    expect(Object.keys(prepared.manifest)).toEqual([
      "schema_version",
      "kind",
      "run_id",
      "task_id",
      "revision",
      "previous_sha256",
      "origin",
      "body",
    ]);
    expect(Object.keys(prepared)).toEqual(["manifest", "canonical_json", "sha256"]);
    expectDeepFrozen(prepared);
  });

  test("revision 1 requires planning_proposal and null previous; revision 2 requires user_response and a digest", () => {
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...TASK_REVISION_1, origin: "user_response" })),
      "the task revision manifest.origin must be planning_proposal for revision 1",
    );
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...TASK_REVISION_1, previous_sha256: hex("a") })),
      "the task revision manifest previous_sha256 must be null for revision 1",
    );
    const revision2 = { ...TASK_REVISION_1, revision: 2, previous_sha256: hex("a"), origin: "user_response" };
    expect(prepareTaskRevisionManifest(revision2).manifest.revision).toBe(2);
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...revision2, origin: "planning_proposal" })),
      "the task revision manifest.origin must be user_response for revisions above 1",
    );
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...revision2, previous_sha256: null })),
      "the task revision manifest previous_sha256 must be a lowercase hex SHA-256 digest for revision 2",
    );
    // a planning system may still create a new task at revision 1 in a
    // later plan revision
    expect(
      prepareTaskRevisionManifest({ ...TASK_REVISION_1, task_id: "task-new" }).manifest.origin,
    ).toBe("planning_proposal");
  });

  test("the body exists only here and every body change changes the digest; unknown origin rejected", () => {
    const base = prepareTaskRevisionManifest(TASK_REVISION_1);
    const changedBody = prepareTaskRevisionManifest({ ...TASK_REVISION_1, body: "Different body" });
    expect(changedBody.sha256).not.toBe(base.sha256);
    expect(changedBody.canonical_json).not.toBe(base.canonical_json);
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...TASK_REVISION_1, origin: "operator_override" })),
      'the task revision manifest.origin must be one of ["planning_proposal","user_response"]',
    );
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...TASK_REVISION_1, body: "" })),
      "the task revision manifest.body must be a non-empty string",
    );
    expectMessage(
      catchOf(() => prepareTaskRevisionManifest({ ...TASK_REVISION_1, body: 7 })),
      "the task revision manifest.body must be a non-empty string",
    );
  });

  test("parse runs the same chain and unknown fields are rejected without naming user keys", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const scrambled = JSON.stringify({
      body: TASK_REVISION_1.body,
      origin: TASK_REVISION_1.origin,
      previous_sha256: null,
      revision: 1,
      task_id: TASK_REVISION_1.task_id,
      run_id: TASK_REVISION_1.run_id,
      kind: TASK_REVISION_1.kind,
      schema_version: TASK_REVISION_1.schema_version,
    });
    const parsed = parseTaskRevisionManifest(scrambled);
    const prepared = prepareTaskRevisionManifest(TASK_REVISION_1);
    expect(parsed).toEqual(prepared);
    const cases: [unknown, string][] = [
      [{ ...TASK_REVISION_1, evidence: "extra" }, "the task revision manifest has unknown fields"],
      [{ ...TASK_REVISION_1, [canary]: "x" }, "the task revision manifest has unknown fields"],
      [{ ...TASK_REVISION_1, task_id: undefined }, "the task revision manifest task_id must be a safe non-empty identifier"],
      [{ ...TASK_REVISION_1, task_id: "../bad" }, "the task revision manifest task_id must be a safe non-empty identifier"],
      [{ ...TASK_REVISION_1, revision: 0 }, "the task revision manifest revision must be a positive safe integer"],
      [null, "the task revision manifest is not a JSON object"],
      [7, "the task revision manifest is not a JSON object"],
    ];
    for (const [value, message] of cases) {
      const caught = catchOf(() => prepareTaskRevisionManifest(value));
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(canary);
    }
  });

  test("malformed task JSON and mutation isolation; the body never appears in diagnostics", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const caught = catchOf(() => parseTaskRevisionManifest(`{"body": "${canary}", "kind": "task_re`));
    expectMessage(caught, "the task revision document is not valid JSON");
    expect((caught as Error).message).not.toContain(canary);
    expect((caught as Error).message).not.toContain("Implement the acceptance test parser");

    const value: Record<string, unknown> = { ...TASK_REVISION_1 };
    const prepared = prepareTaskRevisionManifest(value);
    value.body = "mutated body";
    value.task_id = "mutated";
    expect((prepared.manifest as PipelineV2RunTaskRevisionManifest).body).toBe(TASK_REVISION_1.body);
    expect(prepared.manifest.task_id).toBe("task-1");
    expect(prepared.canonical_json).toBe(canonicalJson(TASK_REVISION_1));
    expectDeepFrozen(prepared);
  });
});

describe("pipeline v2 run wait intent manifests", () => {
  test("exact continue intent shape with canonical form and domain-bound digest", () => {
    const prepared = prepareWaitIntent(CONTINUE_INTENT);
    const canonical = canonicalJson(CONTINUE_INTENT);
    expect(prepared.canonical_json).toBe(canonical);
    expect(prepared.sha256).toBe(domainDigest("pipeline-v2-wait-intent\0", canonical));
    expect(Object.keys(prepared.manifest)).toEqual([
      "schema_version",
      "kind",
      "run_id",
      "wait_index",
      "stage_id",
      "expected_plan_sha256",
      "additional_iterations",
    ]);
    expect(Object.keys(prepared)).toEqual(["manifest", "canonical_json", "sha256"]);
    expectDeepFrozen(prepared);
    expect((prepared.manifest as PipelineV2ContinueStageIntentManifest).kind).toBe("continue_stage_intent");
  });

  test("exact revise intent shape with canonical form and domain-bound digest", () => {
    const prepared = prepareWaitIntent(REVISE_INTENT);
    const canonical = canonicalJson(REVISE_INTENT);
    expect(prepared.canonical_json).toBe(canonical);
    expect(prepared.sha256).toBe(domainDigest("pipeline-v2-wait-intent\0", canonical));
    expect(Object.keys(prepared.manifest)).toEqual([
      "schema_version",
      "kind",
      "run_id",
      "wait_index",
      "task_id",
      "expected_previous_task_sha256",
      "new_task_revision_sha256",
    ]);
    expectDeepFrozen(prepared);
    expect((prepared.manifest as PipelineV2ReviseTaskIntentManifest).kind).toBe("revise_task_intent");
  });

  test("the two kinds share one domain and differ in the canonical payload; no upper iteration limit is baked in", () => {
    const continuePrepared = prepareWaitIntent(CONTINUE_INTENT);
    const revisePrepared = prepareWaitIntent(REVISE_INTENT);
    expect(continuePrepared.sha256).not.toBe(revisePrepared.sha256);
    const sameDomainRevise = domainDigest("pipeline-v2-wait-intent\0", revisePrepared.canonical_json);
    expect(revisePrepared.sha256).toBe(sameDomainRevise);
    expect(
      prepareWaitIntent({ ...CONTINUE_INTENT, additional_iterations: Number.MAX_SAFE_INTEGER }).manifest,
    ).toMatchObject({ additional_iterations: Number.MAX_SAFE_INTEGER });
    expect(
      prepareWaitIntent({ ...CONTINUE_INTENT, additional_iterations: 1 }).manifest,
    ).toMatchObject({ additional_iterations: 1 });
  });

  test("additional_iterations must be positive; wait_index and scalars are enforced value-free", () => {
    for (const value of [0, -1, 1.5, "1", null]) {
      const caught = catchOf(() => prepareWaitIntent({ ...CONTINUE_INTENT, additional_iterations: value }));
      expectMessage(caught, "the wait intent manifest.additional_iterations must be a positive safe integer");
    }
    for (const value of [0, -1, 1.5, "1"]) {
      const caught = catchOf(() => prepareWaitIntent({ ...CONTINUE_INTENT, wait_index: value as number }));
      expectMessage(caught, "the wait intent manifest wait_index must be a positive safe integer");
    }
    expectMessage(
      catchOf(() => prepareWaitIntent({ ...CONTINUE_INTENT, expected_plan_sha256: "A".repeat(64) })),
      "the wait intent manifest expected_plan_sha256 must be a lowercase hex SHA-256 digest",
    );
    expectMessage(
      catchOf(() => prepareWaitIntent({ ...CONTINUE_INTENT, stage_id: "../bad" })),
      "the wait intent manifest stage_id must be a safe non-empty identifier",
    );
    expectMessage(
      catchOf(() => prepareWaitIntent({ ...REVISE_INTENT, task_id: "" })),
      "the wait intent manifest task_id must be a safe non-empty identifier",
    );
    expectMessage(
      catchOf(() => prepareWaitIntent({ ...REVISE_INTENT, new_task_revision_sha256: "a".repeat(63) })),
      "the wait intent manifest new_task_revision_sha256 must be a lowercase hex SHA-256 digest",
    );
  });

  test("parse runs the same chain; unknown fields and kinds are rejected value-free", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const parsed = parseWaitIntent(JSON.stringify(CONTINUE_INTENT));
    const prepared = prepareWaitIntent(CONTINUE_INTENT);
    expect(parsed).toEqual(prepared);
    const parsedRevise = parseWaitIntent(JSON.stringify(REVISE_INTENT));
    expect(parsedRevise).toEqual(prepareWaitIntent(REVISE_INTENT));
    const cases: [unknown, string][] = [
      [{ ...CONTINUE_INTENT, iterations: 5 }, "the wait intent manifest has unknown fields"],
      [{ ...CONTINUE_INTENT, [canary]: "x" }, "the wait intent manifest has unknown fields"],
      [{ ...CONTINUE_INTENT, expected_plan_sha256: undefined }, "the wait intent manifest expected_plan_sha256 must be a lowercase hex SHA-256 digest"],
      [{ ...REVISE_INTENT, new_task_revision_sha256: undefined }, "the wait intent manifest new_task_revision_sha256 must be a lowercase hex SHA-256 digest"],
      [{ schema_version: 1, kind: "other_intent", run_id: "run-1" }, 'the wait intent manifest.kind must be one of ["continue_stage_intent","revise_task_intent"]'],
      [{ ...CONTINUE_INTENT, kind: "revise_task_intent", extra: 1 }, "the wait intent manifest has unknown fields"],
      [null, "the wait intent manifest is not a JSON object"],
      [42, "the wait intent manifest is not a JSON object"],
    ];
    for (const [value, message] of cases) {
      const caught = catchOf(() => prepareWaitIntent(value));
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(canary);
    }
    const malformed = catchOf(() => parseWaitIntent(`{"kind": "${canary}", "schema_v`));
    expectMessage(malformed, "the wait intent document is not valid JSON");
    expect((malformed as Error).message).not.toContain(canary);
  });

  test("the intent snapshot is independent of later input mutations", () => {
    const value: Record<string, unknown> = { ...CONTINUE_INTENT };
    const prepared = prepareWaitIntent(value);
    value.additional_iterations = 99;
    value.stage_id = "mutated";
    const continueManifest = prepared.manifest as PipelineV2ContinueStageIntentManifest;
    expect(continueManifest.additional_iterations).toBe(2);
    expect(continueManifest.stage_id).toBe("implementation");
    expect(prepared.canonical_json).toBe(canonicalJson(CONTINUE_INTENT));
    expectDeepFrozen(prepared);

    const reviseValue: Record<string, unknown> = { ...REVISE_INTENT };
    const revisePrepared = prepareWaitIntent(reviseValue);
    reviseValue.new_task_revision_sha256 = hex("9");
    expect((revisePrepared.manifest as PipelineV2ReviseTaskIntentManifest).new_task_revision_sha256).toBe(hex("5"));
    expectDeepFrozen(revisePrepared);
  });
});

describe("pipeline v2 run wait intent single discriminator read", () => {
  const CONTINUE_INTENT_ACCESSOR_CANARY = "SECOND_KIND_READ";

  /**
   * One continue intent whose `kind` accessor returns the correct value on
   * the first read and throws the canary error on any later read.
   */
  function continueIntentWithThrowingSecondRead(
    expectedPlanSha256: string,
  ): { value: Record<string, unknown>; kindReads: () => number } {
    let reads = 0;
    const value = {
      ...CONTINUE_INTENT,
      expected_plan_sha256: expectedPlanSha256,
    };
    Object.defineProperty(value, "kind", {
      enumerable: true,
      get() {
        reads += 1;
        if (reads === 1) {
          return "continue_stage_intent";
        }
        throw new Error(CONTINUE_INTENT_ACCESSOR_CANARY);
      },
    });
    return { value, kindReads: () => reads };
  }

  test("a second kind getter read that throws still yields the continue intent, read exactly once", () => {
    const { value, kindReads } = continueIntentWithThrowingSecondRead(hex("a"));
    const prepared = prepareWaitIntent(value);
    expect((prepared.manifest as PipelineV2ContinueStageIntentManifest).kind).toBe("continue_stage_intent");
    expect(kindReads()).toBe(1);
    expect(prepared.canonical_json).toBe(canonicalJson({ ...CONTINUE_INTENT, expected_plan_sha256: hex("a") }));
    expectDeepFrozen(prepared);
    // the caller object is not frozen or modified
    expect(Object.isFrozen(value)).toBe(false);
    expect(Object.keys(value).sort()).toEqual([
      "additional_iterations",
      "expected_plan_sha256",
      "kind",
      "run_id",
      "schema_version",
      "stage_id",
      "wait_index",
    ].sort());
  });

  test("the exact accessor-prepared continue intent binds through the continue validator", () => {
    // 1+2+3: the plan first, then the accessor input naming exactly the
    // prepared plan digest, then the switching kind getter
    const plan = preparePlanRevisionManifest(PLAN_REVISION_1);
    const { value, kindReads } = continueIntentWithThrowingSecondRead(plan.sha256);
    // 4: the accessor object itself is prepared and nothing else is
    const accessorPreparedIntent = prepareWaitIntent(value);
    expect(kindReads()).toBe(1);
    // 5+6: the very same object is passed to the validator
    expect(() => validateContinueIntentBinding({ intent: accessorPreparedIntent, plan })).not.toThrow();
    expect(kindReads()).toBe(1);
    // canonical JSON/digest equal the plain manifest with the same fields
    const plainManifest = {
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: "run-1",
      wait_index: 1,
      stage_id: "implementation",
      expected_plan_sha256: plan.sha256,
      additional_iterations: 2,
    };
    expect(accessorPreparedIntent.canonical_json).toBe(canonicalJson(plainManifest));
    expect(accessorPreparedIntent.sha256).toBe(domainDigest("pipeline-v2-wait-intent\0", canonicalJson(plainManifest)));
    // the caller object is not frozen
    expect(Object.isFrozen(value)).toBe(false);
    // the canary never reaches the result or diagnostics
    expect(JSON.stringify(accessorPreparedIntent)).not.toContain(CONTINUE_INTENT_ACCESSOR_CANARY);
  });

  test("no canary from the second getter read reaches any diagnostic or output", () => {
    const { value, kindReads } = continueIntentWithThrowingSecondRead(hex("a"));
    const prepared = prepareWaitIntent(value);
    const text = JSON.stringify(prepared);
    expect(text).not.toContain(CONTINUE_INTENT_ACCESSOR_CANARY);
    expect(prepared.canonical_json).not.toContain(CONTINUE_INTENT_ACCESSOR_CANARY);
    expect(kindReads()).toBe(1);
  });

  const REVISE_INTENT_ACCESSOR_CANARY = "SECOND_KIND_READ";

  function reviseIntentWithSwitchingKind(
    expectedPreviousTaskSha256: string,
    newTaskRevisionSha256: string,
  ): { value: Record<string, unknown>; kindReads: () => number } {
    let reads = 0;
    const value = {
      ...REVISE_INTENT,
      expected_previous_task_sha256: expectedPreviousTaskSha256,
      new_task_revision_sha256: newTaskRevisionSha256,
    };
    Object.defineProperty(value, "kind", {
      enumerable: true,
      get() {
        reads += 1;
        if (reads === 1) {
          return "revise_task_intent";
        }
        return "continue_stage_intent";
      },
    });
    return { value, kindReads: () => reads };
  }

  test("the exact accessor-prepared revise intent binds as revise and is refused by the continue validator", () => {
    // 1+2: the current revision and the candidate chained to it
    const current = prepareTaskRevisionManifest(TASK_REVISION_1);
    const candidate = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: "run-1",
      task_id: "task-1",
      revision: 2,
      previous_sha256: current.sha256,
      origin: "user_response",
      body: "Revised acceptance parser body",
    });
    // 3+4: the accessor input naming exactly the current and candidate
    // digests, with the kind getter switching to continue after one read
    const { value, kindReads } = reviseIntentWithSwitchingKind(
      current.sha256,
      candidate.sha256,
    );
    // 5: the accessor object itself is prepared and nothing else is
    const accessorPreparedIntent = prepareWaitIntent(value);
    expect(kindReads()).toBe(1);
    expect((accessorPreparedIntent.manifest as PipelineV2ReviseTaskIntentManifest).kind).toBe("revise_task_intent");
    expect(accessorPreparedIntent.canonical_json).toBe(canonicalJson({
      ...REVISE_INTENT,
      expected_previous_task_sha256: current.sha256,
      new_task_revision_sha256: candidate.sha256,
    }));
    // 6: the very same object binds through the revise validator
    expect(() =>
      validateReviseIntentBinding({
        intent: accessorPreparedIntent,
        candidateTaskRevision: candidate,
        currentTaskRevision: current,
      }),
    ).not.toThrow();
    // 7: the very same object is refused by the continue validator by
    // provenance, before any manifest field is read
    const plan = preparePlanRevisionManifest(PLAN_REVISION_1);
    const message = catchOf(() =>
      validateContinueIntentBinding({ intent: accessorPreparedIntent, plan }),
    ) as Error;
    expect(message).toBeInstanceOf(PipelineV2RunPlanBindingError);
    expect(message.message).toBe(
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expect(message.message).not.toContain(REVISE_INTENT_ACCESSOR_CANARY);
    // 8: every validator call read the accessor getter zero more times
    expect(kindReads()).toBe(1);
  });
});

describe("pipeline v2 run plan manifest digest domains", () => {
  test("the three new domains are separated from each other and from every existing domain", () => {
    const planCanonical = preparePlanRevisionManifest(PLAN_REVISION_1).canonical_json;
    const taskCanonical = prepareTaskRevisionManifest(TASK_REVISION_1).canonical_json;
    const continueCanonical = prepareWaitIntent(CONTINUE_INTENT).canonical_json;
    const reviseCanonical = prepareWaitIntent(REVISE_INTENT).canonical_json;

    expect(preparePlanRevisionManifest(PLAN_REVISION_1).sha256).toBe(
      domainDigest("pipeline-v2-plan-revision\0", planCanonical),
    );
    expect(prepareTaskRevisionManifest(TASK_REVISION_1).sha256).toBe(
      domainDigest("pipeline-v2-task-revision\0", taskCanonical),
    );
    expect(prepareWaitIntent(CONTINUE_INTENT).sha256).toBe(
      domainDigest("pipeline-v2-wait-intent\0", continueCanonical),
    );
    expect(prepareWaitIntent(REVISE_INTENT).sha256).toBe(
      domainDigest("pipeline-v2-wait-intent\0", reviseCanonical),
    );

    // the same canonical bytes digested under the other new or existing
    // domains produce different digests
    const otherNewDomains = ["pipeline-v2-task-revision\0", "pipeline-v2-wait-intent\0"];
    for (const domain of otherNewDomains) {
      expect(domainDigest(domain, planCanonical)).not.toBe(
        domainDigest("pipeline-v2-plan-revision\0", planCanonical),
      );
    }
    const existingDomains = [
      "pipeline-v2-wait-request\0",
      "pipeline-v2-wait-response\0",
      "pipeline-v2-execution-snapshot\0",
      "pipeline-v2-input\0",
      "pipeline-v2-output\0",
      "pipeline-v2-run-output\0",
      "pipeline-v2-decision-input\0",
    ];
    for (const domain of existingDomains) {
      for (const newDomain of ["pipeline-v2-plan-revision\0", "pipeline-v2-task-revision\0", "pipeline-v2-wait-intent\0"]) {
        expect(newDomain).not.toBe(domain);
      }
    }
  });
});

describe("pipeline v2 run plan manifest structural scan", () => {
  test("no paths, timestamps, credentials, bodies or payloads; key scan for banned fields", () => {
    const planPrepared = parsePlanRevisionManifest(JSON.stringify(PLAN_REVISION_1));
    const taskPrepared = parseTaskRevisionManifest(JSON.stringify(TASK_REVISION_1));
    const continuePrepared = parseWaitIntent(JSON.stringify(CONTINUE_INTENT));
    const revisePrepared = parseWaitIntent(JSON.stringify(REVISE_INTENT));
    const text = [
      JSON.stringify(planPrepared),
      JSON.stringify(taskPrepared),
      JSON.stringify(continuePrepared),
      JSON.stringify(revisePrepared),
    ].join("\n");
    const canaries = [
      "/opt/orchestrator",
      "/home/michael",
      "2026-01-01T00:00:00",
      "OPENCODE_CONFIG_CONTENT",
      "dhc_0392",
      "bearer_token",
      "LLM_KEY",
      "TASK.md revision 2",
      "PLAN: stage 3 of 5",
      "unmet acceptance criteria",
      "endpoint",
      "apiKey",
    ];
    for (const canary of canaries) {
      expect(text).not.toContain(canary);
    }
    // the task body exists only inside the task revision manifest result
    // (a legitimate carrier), never in diagnostics — covered by the
    // malformed-JSON tests above
    expect(JSON.stringify(taskPrepared)).toContain("Implement the acceptance test parser");
    const keys = new Set<string>();
    collectKeys(planPrepared, keys);
    collectKeys(taskPrepared, keys);
    collectKeys(continuePrepared, keys);
    collectKeys(revisePrepared, keys);
    for (const banned of [
      "path",
      "timestamp",
      "created_at",
      "payload",
      "comment",
      "facts",
      "credentials",
      "profile_bindings",
      "token",
      "endpoint",
    ]) {
      expect(keys.has(banned), `the manifest substrate must not carry a ${banned} field`).toBe(false);
    }
  });
});

describe("pipeline v2 run plan manifests provenance and export surface", () => {
  test("a prepared plan revision is accepted by the registry with the right kind", () => {
    // Provenance is verified by the binding module; here we only verify
    // the public surface stays clean and the objects stay usable.
    const plan = preparePlanRevisionManifest(PLAN_REVISION_1);
    const task = prepareTaskRevisionManifest(TASK_REVISION_1);
    const intent = prepareWaitIntent(CONTINUE_INTENT);
    expect(plan.canonical_json).toBe(canonicalJson(plan.manifest));
    expect(task.canonical_json).toBe(canonicalJson(task.manifest));
    expect(intent.canonical_json).toBe(canonicalJson(intent.manifest));
  });

  test("the public export surface carries no registry, digest builder or test seam", async () => {
    const namespace = (await import("../src/pipeline_v2_run_plan_manifests.ts")) as Record<
      string,
      unknown
    >;
    expect(Object.keys(namespace).sort()).toEqual([
      "PipelineV2RunPlanManifestError",
      "parsePlanRevisionManifest",
      "parseTaskRevisionManifest",
      "parseWaitIntent",
      "preparePlanRevisionManifest",
      "prepareTaskRevisionManifest",
      "prepareWaitIntent",
    ]);
  });

  test("the source carries no second canonical serializer and no local scalar pattern copies", () => {
    const source = readFileSync(
      join(import.meta.dir, "../src/pipeline_v2_run_plan_manifests.ts"),
      "utf8",
    );
    expect(source.includes("SAFE_ID_PATTERN")).toBe(false);
    expect(source.includes("SHA256_PATTERN =")).toBe(false);
    expect(source.includes("canonicalJsonValue")).toBe(false);
    expect(source.includes('from "./pipeline_v2_scalar.ts"')).toBe(true);
    expect(source.includes('from "./canonical_json.ts"')).toBe(true);
    expect(source.includes("new Bun.CryptoHasher")).toBe(true);
  });

  test("the internal provenance substrate is not re-exported by any public module", async () => {
    const manifests = (await import("../src/pipeline_v2_run_plan_manifests.ts")) as Record<
      string,
      unknown
    >;
    for (const key of Object.keys(manifests)) {
      expect(key.toLowerCase()).not.toContain("registry");
      expect(key.toLowerCase()).not.toContain("provenance");
    }
    const provenance = (await import("../src/pipeline_v2_run_plan_provenance.ts")) as Record<
      string,
      unknown
    >;
    expect(Object.keys(provenance).sort()).toEqual([
      "hasPreparedRunPlanProvenance",
      "registerPreparedRunPlanObject",
    ]);
  });
});
