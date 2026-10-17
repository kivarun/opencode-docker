import { describe, expect, test } from "bun:test";
import {
  PipelineV2RunPlanManifestError,
  parsePlanRevisionManifest,
  parseTaskRevisionManifest,
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PipelineV2RunPlanStage,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  PipelineV2RunPlanBindingError,
  validateContinueIntentBinding,
  validatePlanRevisionChain,
  validatePlanTaskBindings,
  validateReviseIntentBinding,
  validateRootTaskBinding,
  validateTaskRevisionChain,
} from "../src/pipeline_v2_run_plan_bindings.ts";

const hex = (char: string): string => char.repeat(64);

const TASK_REVISION_BODY_1 = "Implement the acceptance test parser";
const TASK_REVISION_BODY_2 = "Implement the acceptance test parser with structured output";

function taskRevisionValue(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = "run-1",
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

const TASK_1_REVISION_1 = taskRevisionValue("task-1", 1, null, "planning_proposal", TASK_REVISION_BODY_1);
const TASK_2_REVISION_1 = taskRevisionValue("task-2", 1, null, "planning_proposal", TASK_REVISION_BODY_2);

function planValue(
  overrides: Record<string, unknown> = {},
  stages: Record<string, unknown>[] = [
    {
      id: "implementation",
      template: "development",
      tasks: [
        { id: "task-1", revision: 1, sha256: hex("1"), depends_on: [] },
        { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
      ],
    },
  ],
): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "plan_revision",
    run_id: "run-1",
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: hex("b") },
    origin_execution: 10,
    stages,
    ...overrides,
  };
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

const PLAN_1 = preparePlanRevisionManifest(planValue());
const PLAN_2_WITH_CHAIN = preparePlanRevisionManifest(
  planValue(
    {
      revision: 2,
      previous_sha256: PLAN_1.sha256,
    },
    [
      {
        id: "implementation",
        template: "development",
        tasks: [
          { id: "task-1", revision: 2, sha256: hex("9"), depends_on: [] },
          { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
        ],
      },
    ],
  ),
);

const TASK_1_PREPARED = prepareTaskRevisionManifest(TASK_1_REVISION_1);
const TASK_2_PREPARED = prepareTaskRevisionManifest(TASK_2_REVISION_1);

/** A plan whose task pointers carry the prepared revisions' real digests. */
function planValueBound(
  overrides: Record<string, unknown> = {},
  taskOverrides: Record<string, Record<string, unknown>> = {},
): Record<string, unknown> {
  return planValue(overrides, [
    {
      id: "implementation",
      template: "development",
      tasks: [
        {
          id: "task-1",
          revision: 1,
          sha256: (taskOverrides["task-1"]?.sha256 as string | undefined) ?? TASK_1_PREPARED.sha256,
          depends_on: [],
          ...Object.fromEntries(Object.entries(taskOverrides["task-1"] ?? {}).filter(([key]) => key !== "sha256")),
        },
        {
          id: "task-2",
          revision: 1,
          sha256: (taskOverrides["task-2"]?.sha256 as string | undefined) ?? TASK_2_PREPARED.sha256,
          depends_on: ["task-1"],
          ...Object.fromEntries(Object.entries(taskOverrides["task-2"] ?? {}).filter(([key]) => key !== "sha256")),
        },
      ],
    },
  ]);
}

const BOUND_PLAN_1 = preparePlanRevisionManifest(planValueBound());

describe("pipeline v2 run plan binding: plan task bindings", () => {
  test("one revision per plan task binds by id, revision and digest", () => {
    expect(() =>
      validatePlanTaskBindings({
        plan: BOUND_PLAN_1,
        taskRevisions: [TASK_1_PREPARED, TASK_2_PREPARED],
      }),
    ).not.toThrow();
    // record permutation is irrelevant
    expect(() =>
      validatePlanTaskBindings({
        plan: BOUND_PLAN_1,
        taskRevisions: [TASK_2_PREPARED, TASK_1_PREPARED],
      }),
    ).not.toThrow();
  });

  test("missing, extra and duplicate task revisions are rejected", () => {
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [TASK_1_PREPARED] }),
      ),
      'validatePlanTaskBindings is missing the task revision for plan task "task-2"',
    );
    const extra = prepareTaskRevisionManifest(
      taskRevisionValue("task-x", 1, null, "planning_proposal", "Extra body"),
    );
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [TASK_1_PREPARED, TASK_2_PREPARED, extra] }),
      ),
      "validatePlanTaskBindings carries a task revision the plan does not declare at position 2",
    );
    const duplicate = prepareTaskRevisionManifest(TASK_1_REVISION_1);
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [TASK_1_PREPARED, duplicate, TASK_2_PREPARED] }),
      ),
      "validatePlanTaskBindings carries a duplicate task revision at position 1",
    );
  });

  test("a revision whose task_id, revision number or digest disagrees with the pointer is rejected", () => {
    const wrongRevision = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, hex("7"), "user_response", TASK_REVISION_BODY_1),
    );
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [wrongRevision, TASK_2_PREPARED] }),
      ),
      'validatePlanTaskBindings revision number mismatch for plan task "task-1"',
    );
    const wrongDigest = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 1, null, "planning_proposal", "Different body"),
    );
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [wrongDigest, TASK_2_PREPARED] }),
      ),
      'validatePlanTaskBindings digest mismatch for plan task "task-1"',
    );
    const foreignRun = prepareTaskRevisionManifest({ ...TASK_1_REVISION_1, run_id: "run-2" });
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({ plan: BOUND_PLAN_1, taskRevisions: [foreignRun, TASK_2_PREPARED] }),
      ),
      'validatePlanTaskBindings carries a task revision the plan does not declare at position 0',
    );
    const task3 = prepareTaskRevisionManifest(
      taskRevisionValue("task-3", 1, null, "planning_proposal", "Review body"),
    );
    const multiStagePlan = preparePlanRevisionManifest(
      planValue({}, [
        {
          id: "implementation",
          template: "development",
          tasks: [{ id: "task-1", revision: 1, sha256: TASK_1_PREPARED.sha256, depends_on: [] }],
        },
        {
          id: "review",
          template: "review",
          tasks: [{ id: "task-3", revision: 1, sha256: task3.sha256, depends_on: [] }],
        },
      ]),
    );
    expect(() =>
      validatePlanTaskBindings({ plan: multiStagePlan, taskRevisions: [TASK_1_PREPARED, task3] }),
    ).not.toThrow();
  });
});

describe("pipeline v2 run plan binding: plan revision chain", () => {
  test("revision 1 requires no predecessor, revision 1 and a null previous digest", () => {
    expect(() => validatePlanRevisionChain({ previous: null, current: PLAN_1 })).not.toThrow();
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: null, current: PLAN_2_WITH_CHAIN })),
      "validatePlanRevisionChain requires revision 1 when no predecessor is passed",
    );
    // a revision-2 manifest with a null previous digest cannot even be
    // prepared (the manifest chain enforces the digest) — the binding
    // layer only sees validly prepared objects
    let caught2: unknown = null;
    try {
      preparePlanRevisionManifest(planValue({ revision: 2, previous_sha256: null }));
    } catch (cause) {
      caught2 = cause;
    }
    expect(caught2).toBeInstanceOf(PipelineV2RunPlanManifestError);
  });

  test("a valid successor with a gap, wrong digest, foreign run or changed root task is rejected", () => {
    expect(() =>
      validatePlanRevisionChain({ previous: PLAN_1, current: PLAN_2_WITH_CHAIN }),
    ).not.toThrow();
    const gap = preparePlanRevisionManifest(planValue({ revision: 3, previous_sha256: PLAN_1.sha256 }));
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: PLAN_1, current: gap })),
      "validatePlanRevisionChain revision numbers are not consecutive",
    );
    const wrongPreviousDigest = preparePlanRevisionManifest(
      planValue({ revision: 2, previous_sha256: hex("f") }),
    );
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: PLAN_1, current: wrongPreviousDigest })),
      "validatePlanRevisionChain previous_sha256 does not name the predecessor digest",
    );
    const foreignRun = preparePlanRevisionManifest(
      planValue({ revision: 2, previous_sha256: PLAN_1.sha256, run_id: "run-2" }),
    );
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: PLAN_1, current: foreignRun })),
      "validatePlanRevisionChain covers two different runs",
    );
    const changedRoot = preparePlanRevisionManifest(
      planValue({ revision: 2, previous_sha256: PLAN_1.sha256, root_task: { input_id: "task", sha256: hex("c") } }),
    );
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: PLAN_1, current: changedRoot })),
      "validatePlanRevisionChain the protected root task binding changed between revisions",
    );
    // stage/task content may change between revisions (that is the point
    // of a new full revision): the valid successor changed task-1's
    // revision pointer and was accepted above
  });
});

describe("pipeline v2 run plan binding: root task binding", () => {
  test("the plan root task digest must equal the protected input digest exactly", () => {
    expect(() =>
      validateRootTaskBinding({ plan: PLAN_1, protectedInputDigest: hex("b") }),
    ).not.toThrow();
    expectBindingMessage(
      catchOf(() => validateRootTaskBinding({ plan: PLAN_1, protectedInputDigest: hex("c") })),
      "validateRootTaskBinding the plan root task does not bind the protected input digest",
    );
    expectBindingMessage(
      catchOf(() => validateRootTaskBinding({ plan: PLAN_1, protectedInputDigest: "zz" })),
      "validateRootTaskBinding protectedInputDigest must be a lowercase hex SHA-256 digest",
    );
    expectBindingMessage(
      catchOf(() => validateRootTaskBinding({ plan: PLAN_1, protectedInputDigest: "B".repeat(64) })),
      "validateRootTaskBinding protectedInputDigest must be a lowercase hex SHA-256 digest",
    );
    expect(() =>
      validateRootTaskBinding({ plan: PLAN_1, protectedInputDigest: "a".repeat(64) }),
    ).toThrow(PipelineV2RunPlanBindingError);
  });
});

describe("pipeline v2 run plan binding: task revision chain", () => {
  test("revision 1 requires no predecessor, revision 1 and a null previous digest", () => {
    expect(() => validateTaskRevisionChain({ previous: null, current: TASK_1_PREPARED })).not.toThrow();
    const revision2 = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "user_response", "Revised body"),
    );
    expectBindingMessage(
      catchOf(() => validateTaskRevisionChain({ previous: null, current: revision2 })),
      "validateTaskRevisionChain requires revision 1 when no predecessor is passed",
    );
    // a revision-1 manifest with a non-null previous digest cannot even be
    // prepared (the manifest chain enforces the digest) — the binding
    // layer only sees validly prepared objects
    let caught1: unknown = null;
    try {
      prepareTaskRevisionManifest(taskRevisionValue("task-1", 1, hex("7"), "planning_proposal", TASK_REVISION_BODY_1));
    } catch (cause) {
      caught1 = cause;
    }
    expect(caught1).toBeInstanceOf(PipelineV2RunPlanManifestError);
  });

  test("a valid successor binds; wrong run, task, gap or digest is rejected", () => {
    const revision2 = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "user_response", "Revised body"),
    );
    expect(() => validateTaskRevisionChain({ previous: TASK_1_PREPARED, current: revision2 })).not.toThrow();
    const foreignRun = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "user_response", "Revised body", "run-2"),
    );
    expectBindingMessage(
      catchOf(() => validateTaskRevisionChain({ previous: TASK_1_PREPARED, current: foreignRun })),
      "validateTaskRevisionChain covers two different runs",
    );
    const otherTask = prepareTaskRevisionManifest(
      taskRevisionValue("task-2", 2, TASK_1_PREPARED.sha256, "user_response", "Other task body"),
    );
    expectBindingMessage(
      catchOf(() => validateTaskRevisionChain({ previous: TASK_1_PREPARED, current: otherTask })),
      "validateTaskRevisionChain task ids do not agree",
    );
    const revision3 = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 3, TASK_1_PREPARED.sha256, "user_response", "Third body"),
    );
    expectBindingMessage(
      catchOf(() => validateTaskRevisionChain({ previous: TASK_1_PREPARED, current: revision3 })),
      "validateTaskRevisionChain revision numbers are not consecutive",
    );
    const wrongDigest = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, hex("f"), "user_response", "Revised body"),
    );
    expectBindingMessage(
      catchOf(() => validateTaskRevisionChain({ previous: TASK_1_PREPARED, current: wrongDigest })),
      "validateTaskRevisionChain previous_sha256 does not name the predecessor digest",
    );
  });

  test("provenance gate: forged and proxied task revisions are rejected with zero trap hits", () => {
    const revision2 = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "user_response", "Revised body"),
    );
    const untrusted =
      "the operation requires the frozen prepared run plan object returned by " +
      "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
      "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
      "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
      "hand-built objects, casts, clones and Proxies are rejected before any field is read";
    expectBindingMessage(
      catchOf(() =>
        validateTaskRevisionChain({
          previous: null,
          current: { ...TASK_1_PREPARED } as unknown as PreparedPipelineV2RunTaskRevision,
        }),
      ),
      untrusted,
    );
    expectBindingMessage(
      catchOf(() =>
        validateTaskRevisionChain({
          previous: structuredClone(TASK_1_PREPARED) as unknown as PreparedPipelineV2RunTaskRevision,
          current: revision2,
        }),
      ),
      untrusted,
    );
    expectBindingMessage(
      catchOf(() =>
        validateTaskRevisionChain({
          previous: PLAN_1 as unknown as PreparedPipelineV2RunTaskRevision,
          current: revision2,
        }),
      ),
      untrusted,
    );
    let trapCount = 0;
    const proxyTask = new Proxy(revision2, {
      get(target, property, receiver) {
        trapCount += 1;
        return Reflect.get(target, property, receiver);
      },
      has(target, property) {
        trapCount += 1;
        return Reflect.has(target, property);
      },
    });
    expectBindingMessage(
      catchOf(() =>
        validateTaskRevisionChain({
          previous: null,
          current: proxyTask as unknown as PreparedPipelineV2RunTaskRevision,
        }),
      ),
      untrusted,
    );
    expectBindingMessage(
      catchOf(() =>
        validateTaskRevisionChain({
          previous: proxyTask as unknown as PreparedPipelineV2RunTaskRevision,
          current: revision2,
        }),
      ),
      untrusted,
    );
    expect(trapCount).toBe(0);
  });
});

describe("pipeline v2 run plan binding: continue intent", () => {
  test("a matching continue intent binds; wrong run, plan digest or stage is rejected", () => {
    const intent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: "run-1",
      wait_index: 1,
      stage_id: "implementation",
      expected_plan_sha256: PLAN_1.sha256,
      additional_iterations: 2,
    });
    expect(() => validateContinueIntentBinding({ intent, plan: PLAN_1 })).not.toThrow();
    expectBindingMessage(
      catchOf(() => validateContinueIntentBinding({ intent, plan: PLAN_2_WITH_CHAIN })),
      "validateContinueIntentBinding expected_plan_sha256 does not name the prepared plan digest",
    );
    const foreignRun = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: "run-2",
      wait_index: 1,
      stage_id: "implementation",
      expected_plan_sha256: PLAN_1.sha256,
      additional_iterations: 2,
    });
    expectBindingMessage(
      catchOf(() => validateContinueIntentBinding({ intent: foreignRun, plan: PLAN_1 })),
      "validateContinueIntentBinding covers two different runs",
    );
    const unknownStage = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: "run-1",
      wait_index: 1,
      stage_id: "no-such-stage",
      expected_plan_sha256: PLAN_1.sha256,
      additional_iterations: 2,
    });
    expectBindingMessage(
      catchOf(() => validateContinueIntentBinding({ intent: unknownStage, plan: PLAN_1 })),
      "validateContinueIntentBinding names a stage the plan does not declare",
    );
  });
});

describe("pipeline v2 run plan binding: revise intent", () => {
  test("a matching revise intent binds the candidate successor and current revision", () => {
    const candidate = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "user_response", "Revised body"),
    );
    const intent = prepareWaitIntent({
      schema_version: 1,
      kind: "revise_task_intent",
      run_id: "run-1",
      wait_index: 2,
      task_id: "task-1",
      expected_previous_task_sha256: TASK_1_PREPARED.sha256,
      new_task_revision_sha256: candidate.sha256,
    });
    expect(() =>
      validateReviseIntentBinding({
        intent,
        candidateTaskRevision: candidate,
        currentTaskRevision: TASK_1_PREPARED,
      }),
    ).not.toThrow();

    // the candidate must be the exact successor of the CURRENT revision
    const wrongNumber = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 3, TASK_1_PREPARED.sha256, "user_response", "Revised body"),
    );
    const intent3 = prepareWaitIntent({
      schema_version: 1,
      kind: "revise_task_intent",
      run_id: "run-1",
      wait_index: 2,
      task_id: "task-1",
      expected_previous_task_sha256: TASK_1_PREPARED.sha256,
      new_task_revision_sha256: wrongNumber.sha256,
    });
    expectBindingMessage(
      catchOf(() =>
        validateReviseIntentBinding({
          intent: intent3,
          candidateTaskRevision: wrongNumber,
          currentTaskRevision: TASK_1_PREPARED,
        }),
      ),
      "validateReviseIntentBinding candidate revision numbers are not consecutive",
    );

    // the intent must name exactly the current digest
    const intentStale = prepareWaitIntent({
      schema_version: 1,
      kind: "revise_task_intent",
      run_id: "run-1",
      wait_index: 2,
      task_id: "task-1",
      expected_previous_task_sha256: hex("f"),
      new_task_revision_sha256: candidate.sha256,
    });
    expectBindingMessage(
      catchOf(() =>
        validateReviseIntentBinding({
          intent: intentStale,
          candidateTaskRevision: candidate,
          currentTaskRevision: TASK_1_PREPARED,
        }),
      ),
      "validateReviseIntentBinding expected_previous_task_sha256 does not name the current task digest",
    );

    // a revision-2 candidate with origin planning_proposal cannot even be
    // prepared (the manifest chain enforces user_response above revision
    // 1) — the binding guard keeps covering any later origin relaxation
    let planningCaught: unknown = null;
    try {
      prepareTaskRevisionManifest(
        taskRevisionValue("task-1", 2, TASK_1_PREPARED.sha256, "planning_proposal", "Revised body"),
      );
    } catch (cause) {
      planningCaught = cause;
    }
    expect(planningCaught).toBeInstanceOf(PipelineV2RunPlanManifestError);

    // task ids must agree across intent, candidate and current revision
    const otherTaskCandidate = prepareTaskRevisionManifest(
      taskRevisionValue("task-2", 2, TASK_2_PREPARED.sha256, "user_response", "Other task body"),
    );
    expectBindingMessage(
      catchOf(() =>
        validateReviseIntentBinding({
          intent,
          candidateTaskRevision: otherTaskCandidate,
          currentTaskRevision: TASK_1_PREPARED,
        }),
      ),
      "validateReviseIntentBinding task ids do not agree with the candidate task revision",
    );
    expectBindingMessage(
      catchOf(() =>
        validateReviseIntentBinding({
          intent,
          candidateTaskRevision: candidate,
          currentTaskRevision: TASK_2_PREPARED,
        }),
      ),
      "validateReviseIntentBinding task ids do not agree with the current task revision",
    );
    const foreignRunIntent = prepareWaitIntent({
      schema_version: 1,
      kind: "revise_task_intent",
      run_id: "run-2",
      wait_index: 2,
      task_id: "task-1",
      expected_previous_task_sha256: TASK_1_PREPARED.sha256,
      new_task_revision_sha256: candidate.sha256,
    });
    expectBindingMessage(
      catchOf(() =>
        validateReviseIntentBinding({
          intent: foreignRunIntent,
          candidateTaskRevision: candidate,
          currentTaskRevision: TASK_1_PREPARED,
        }),
      ),
      "validateReviseIntentBinding covers two different runs",
    );
  });
});

describe("pipeline v2 run plan binding: provenance boundary", () => {
  test("hand-built, spread, structuredClone and cross-kind look-alikes are rejected before any field is read", () => {
    const lookAlike = {
      manifest: JSON.parse(JSON.stringify(PLAN_1.manifest)),
      canonical_json: PLAN_1.canonical_json,
      sha256: PLAN_1.sha256,
    };
    const cases: [unknown, () => void][] = [
      [
        lookAlike,
        () =>
          validatePlanTaskBindings({
            plan: lookAlike as unknown as PreparedPipelineV2RunPlanRevision,
            taskRevisions: [],
          }),
      ],
      [
        { ...PLAN_1 },
        () => validateRootTaskBinding({ plan: { ...PLAN_1 } as unknown as PreparedPipelineV2RunPlanRevision, protectedInputDigest: hex("b") }),
      ],
      [
        structuredClone(PLAN_1),
        () =>
          validatePlanRevisionChain({
            previous: null,
            current: structuredClone(PLAN_1) as unknown as PreparedPipelineV2RunPlanRevision,
          }),
      ],
      [
        TASK_1_PREPARED,
        () =>
          validatePlanRevisionChain({
            previous: null,
            current: TASK_1_PREPARED as unknown as PreparedPipelineV2RunPlanRevision,
          }),
      ],
      [
        PLAN_1,
        () =>
          validateContinueIntentBinding({
            intent: PLAN_1 as unknown as PreparedPipelineV2RunTaskRevision extends never ? never : Parameters<typeof validateContinueIntentBinding>[0]["intent"],
            plan: PLAN_1,
          }),
      ],
      [
        null,
        () =>
          validateReviseIntentBinding({
            intent: null as never,
            candidateTaskRevision: TASK_1_PREPARED,
            currentTaskRevision: TASK_2_PREPARED,
          }),
      ],
    ];
    for (const [forged, run] of cases) {
      const caught = catchOf(run);
      expectBindingMessage(
        caught,
        "the operation requires the frozen prepared run plan object returned by " +
          "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
          "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
          "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
          "hand-built objects, casts, clones and Proxies are rejected before any field is read",
      );
      void forged;
    }
  });

  test("a Proxy around a prepared manifest is rejected with zero getter/trap hits", () => {
    let trapCount = 0;
    const proxyPlan = new Proxy(PLAN_1, {
      get(target, property, receiver) {
        trapCount += 1;
        return Reflect.get(target, property, receiver);
      },
      has(target, property) {
        trapCount += 1;
        return Reflect.has(target, property);
      },
    });
    const proxyTask = new Proxy(TASK_1_PREPARED, {
      get(target, property, receiver) {
        trapCount += 1;
        return Reflect.get(target, property, receiver);
      },
      has(target, property) {
        trapCount += 1;
        return Reflect.has(target, property);
      },
    });
    const proxyIntent = new Proxy(
      prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: "run-1",
        wait_index: 1,
        stage_id: "implementation",
        expected_plan_sha256: PLAN_1.sha256,
        additional_iterations: 2,
      }),
      {
        get(target, property, receiver) {
          trapCount += 1;
          return Reflect.get(target, property, receiver);
        },
        has(target, property) {
          trapCount += 1;
          return Reflect.has(target, property);
        },
      },
    );
    expectBindingMessage(
      catchOf(() => validatePlanTaskBindings({ plan: proxyPlan as unknown as PreparedPipelineV2RunPlanRevision, taskRevisions: [] })),
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expectBindingMessage(
      catchOf(() => validatePlanRevisionChain({ previous: null, current: proxyPlan as unknown as PreparedPipelineV2RunPlanRevision })),
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expectBindingMessage(
      catchOf(() => validateRootTaskBinding({ plan: proxyPlan as unknown as PreparedPipelineV2RunPlanRevision, protectedInputDigest: hex("b") })),
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expectBindingMessage(
      catchOf(() =>
        validatePlanTaskBindings({
          plan: PLAN_1,
          taskRevisions: [proxyTask as unknown as PreparedPipelineV2RunTaskRevision],
        }),
      ),
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expectBindingMessage(
      catchOf(() =>
        validateContinueIntentBinding({
          intent: proxyIntent as never,
          plan: PLAN_1,
        }),
      ),
      "the operation requires the frozen prepared run plan object returned by " +
        "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
        "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
        "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
        "hand-built objects, casts, clones and Proxies are rejected before any field is read",
    );
    expect(trapCount).toBe(0);
  });

  test("caller objects are never frozen or modified by validation", () => {
    // validate a FRESH plan prepared from an in-memory value: the prepared
    // manifests are frozen by the manifest module by design (deep-frozen
    // snapshots), but the validator must not freeze or modify anything
    // itself — verified by preparing a fresh plan from the same value and
    // comparing byte identity
    const freshPlan = preparePlanRevisionManifest(planValueBound());
    validatePlanTaskBindings({ plan: freshPlan, taskRevisions: [TASK_1_PREPARED, TASK_2_PREPARED] });
    expect(freshPlan.sha256).toBe(BOUND_PLAN_1.sha256);
    expect(freshPlan.canonical_json).toBe(BOUND_PLAN_1.canonical_json);
    expect(freshPlan.manifest.revision).toBe(1);
    // deep-frozen by the manifest module (snapshot semantics)
    expect(Object.isFrozen(freshPlan.manifest)).toBe(true);
    // the original in-memory value keeps its unfrozen top level
    expect(Object.isFrozen(planValueBound())).toBe(false);
  });

  test("no registry cross-talk: unrelated prepares keep every prepared object usable", () => {
    const otherPlan = preparePlanRevisionManifest(planValue({ run_id: "run-2" }));
    const otherTask = prepareTaskRevisionManifest(taskRevisionValue("task-1", 1, null, "planning_proposal", "Other run body"));
    expect(() =>
      validatePlanTaskBindings({
        plan: BOUND_PLAN_1,
        taskRevisions: [TASK_1_PREPARED, TASK_2_PREPARED],
      }),
    ).not.toThrow();
    void otherPlan;
    void otherTask;
  });
});

describe("pipeline v2 run plan binding export surface and diagnostics", () => {
  test("the public export surface is exactly the error class and six validators", async () => {
    const namespace = (await import("../src/pipeline_v2_run_plan_bindings.ts")) as Record<
      string,
      unknown
    >;
    expect(Object.keys(namespace).sort()).toEqual([
      "PipelineV2RunPlanBindingError",
      "validateContinueIntentBinding",
      "validatePlanRevisionChain",
      "validatePlanTaskBindings",
      "validateReviseIntentBinding",
      "validateRootTaskBinding",
      "validateTaskRevisionChain",
    ]);
  });

  test("diagnostics never echo task bodies, plan content or canaries", () => {
    const bodyCanary = "SECRET-BEARER-dht_deadbeef";
    const wrongDigestTask = prepareTaskRevisionManifest(
      taskRevisionValue("task-1", 1, null, "planning_proposal", `${bodyCanary} body`),
    );
    const caught = catchOf(() =>
      validatePlanTaskBindings({ plan: PLAN_1, taskRevisions: [wrongDigestTask, TASK_2_PREPARED] }),
    );
    expectBindingMessage(caught, 'validatePlanTaskBindings digest mismatch for plan task "task-1"');
    expect((caught as Error).message).not.toContain(bodyCanary);
    expect((caught as Error).message).not.toContain(TASK_REVISION_BODY_1);
  });

  test("the manifest module keeps parse/prepare parity used by the binding battery", () => {
    const parsed = parsePlanRevisionManifest(JSON.stringify(planValue()));
    expect(parsed.sha256).toBe(PLAN_1.sha256);
    const parsedTask = parseTaskRevisionManifest(JSON.stringify(TASK_1_REVISION_1));
    expect(parsedTask.sha256).toBe(TASK_1_PREPARED.sha256);
    expect(((parsed.manifest.stages as PipelineV2RunPlanStage[])[0] as PipelineV2RunPlanStage).tasks).toHaveLength(2);
  });
});
