/**
 * The pure agent-authored run plan proposal substrate
 * (`pipeline_v2_run_plan_proposal.ts`): one validation/snapshot chain, a
 * direct deep-frozen proposal return (no digest envelope of its own),
 * the shared provenance registry under its own kind, content-free
 * diagnostics and verbatim declared order. The plan/graph semantics
 * (unique ids, dependency DAG, non-empty policy) belong to the plan
 * revision manifest layer and the durable-ledger rules to the
 * construction layer — the responsibility-boundary matrix below pins
 * that this layer accepts shape-valid documents regardless of plan
 * semantics.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import {
  PipelineV2RunPlanProposalError,
  parsePipelineV2RunPlanProposal,
  preparePipelineV2RunPlanProposal,
  type PipelineV2RunPlanProposal,
  type PipelineV2RunPlanProposalStage,
} from "../src/pipeline_v2_run_plan_proposal.ts";
import { hasPreparedRunPlanProvenance } from "../src/pipeline_v2_run_plan_provenance.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";

const hex = (char: string): string => char.repeat(64);

function catchOf(fn: () => unknown): unknown {
  try {
    fn();
  } catch (error) {
    return error;
  }
  return null;
}

function expectMessage(error: unknown, message: string): void {
  expect(error).toBeInstanceOf(PipelineV2RunPlanProposalError);
  expect((error as Error).message).toBe(message);
}

function expectDeepFrozen(value: unknown): void {
  expect(Object.isFrozen(value)).toBe(true);
  if (Array.isArray(value)) {
    for (const entry of value) {
      expectDeepFrozen(entry);
    }
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value as Record<string, unknown>)) {
      expectDeepFrozen(child);
    }
  }
}

const PROPOSAL = {
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
    {
      id: "stage-2",
      template: "review",
      tasks: [{ id: "task-c", depends_on: [] }],
    },
  ],
  new_tasks: [
    { id: "task-a", body: "Implement the acceptance test parser" },
    { id: "task-b", body: "Wire the parser into the stage runner" },
  ],
};

describe("pipeline v2 run plan proposal contract", () => {
  test("prepare returns the direct four-key proposal, no digest envelope", () => {
    const proposal = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(Object.keys(proposal).sort()).toEqual(["kind", "new_tasks", "schema_version", "stages"]);
    expect(proposal.schema_version).toBe(1);
    expect(proposal.kind).toBe("run_plan_proposal");
    expect(proposal.stages).toHaveLength(2);
    expect(proposal.new_tasks).toHaveLength(2);
    const record = proposal as unknown as Record<string, unknown>;
    expect("manifest" in record && Object.prototype.hasOwnProperty.call(record, "manifest")).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(record, "canonical_json")).toBe(false);
    expect(Object.prototype.hasOwnProperty.call(record, "sha256")).toBe(false);
    const stage = proposal.stages[0] as PipelineV2RunPlanProposalStage;
    expect(stage.template).toBe("development");
    expect(stage.tasks[1]?.depends_on).toEqual(["task-a"]);
  });

  test("the snapshot is deep-frozen and independent of later input mutations", () => {
    const value: { new_tasks: Array<{ id: string; body: string }>; stages: Array<Record<string, unknown>> } =
      structuredClone(PROPOSAL);
    const proposal = preparePipelineV2RunPlanProposal(value);
    value.new_tasks[0] = { id: "task-a", body: "MUTATED-BODY" };
    value.stages[0]!.tasks = [];
    expect(proposal.new_tasks[0]?.body).toBe("Implement the acceptance test parser");
    expect(proposal.stages[0]!.tasks).toHaveLength(2);
    expectDeepFrozen(proposal);
    expect(Object.isFrozen(proposal.stages[0]?.tasks[0]?.depends_on)).toBe(true);
    expect(Object.isFrozen(proposal.new_tasks)).toBe(true);
  });

  test("declared order is preserved verbatim at all four array levels", () => {
    const proposal = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        { id: "zz-stage", template: "zz-template", tasks: [{ id: "z-task", depends_on: ["b-task", "a-task"] }, { id: "b-task", depends_on: [] }, { id: "a-task", depends_on: [] }] },
        { id: "aa-stage", template: "aa-template", tasks: [{ id: "z-task", depends_on: [] }] },
      ],
      new_tasks: [{ id: "z-task", body: "z" }, { id: "b-task", body: "b" }, { id: "a-task", body: "a" }],
    });
    expect(proposal.stages.map((stage) => stage.id)).toEqual(["zz-stage", "aa-stage"]);
    expect(proposal.stages[0]?.tasks.map((task) => task.id)).toEqual(["z-task", "b-task", "a-task"]);
    expect(proposal.stages[0]?.tasks[0]?.depends_on).toEqual(["b-task", "a-task"]);
    expect(proposal.new_tasks.map((task) => task.id)).toEqual(["z-task", "b-task", "a-task"]);
    expect(proposal.stages.map((stage) => stage.template)).toEqual(["zz-template", "aa-template"]);
  });

  test("permuting arrays changes the snapshot; the layer never normalizes", () => {
    const base = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-b", depends_on: [] }] }],
      new_tasks: [{ id: "task-a", body: "a" }, { id: "task-b", body: "b" }],
    });
    const permuted = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-b", depends_on: [] }, { id: "task-a", depends_on: [] }] }],
      new_tasks: [{ id: "task-b", body: "b" }, { id: "task-a", body: "a" }],
    });
    expect(base.stages[0]?.tasks.map((task) => task.id)).toEqual(["task-a", "task-b"]);
    expect(permuted.stages[0]?.tasks.map((task) => task.id)).toEqual(["task-b", "task-a"]);
    expect(base.new_tasks.map((task) => task.id)).toEqual(["task-a", "task-b"]);
    expect(permuted.new_tasks.map((task) => task.id)).toEqual(["task-b", "task-a"]);
  });

  test("exact own enumerable fields at every level; unknown own keys rejected echo-free", () => {
    const canary = "SECRET-CANARY-in-field-name";
    const cases: [unknown, string][] = [
      [null, "the run plan proposal is not a JSON object"],
      [42, "the run plan proposal is not a JSON object"],
      [["array"], "the run plan proposal is not a JSON object"],
      [{ ...PROPOSAL, extra: 1 }, "the run plan proposal has unknown fields"],
      [{ ...PROPOSAL, [canary]: "x" }, "the run plan proposal has unknown fields"],
      [
        { ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0], id: "stage-1", extra: 1 }, PROPOSAL.stages[1]!] },
        "the run plan proposal stage at position 0 has unknown fields",
      ],
      [
        {
          ...PROPOSAL,
          stages: [
            { ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: [], extra: 1 }, PROPOSAL.stages[0]!.tasks[1]] },
            PROPOSAL.stages[1]!,
          ],
        },
        "the run plan proposal stage at position 0 task at position 0 has unknown fields",
      ],
      [
        { ...PROPOSAL, new_tasks: [{ id: "task-a", body: "b", extra: 1 }, PROPOSAL.new_tasks[1]!] },
        "the run plan proposal new task at position 0 has unknown fields",
      ],
      [
        { ...PROPOSAL, schema_version: 2 },
        "the run plan proposal.schema_version must be 1",
      ],
      [{ ...PROPOSAL, kind: "plan_revision" }, 'the run plan proposal.kind must be "run_plan_proposal"'],
      [{ ...PROPOSAL, stages: null }, "the run plan proposal.stages must be an array"],
      [{ ...PROPOSAL, new_tasks: null }, "the run plan proposal.new_tasks must be an array"],
      [
        { ...PROPOSAL, stages: [{ id: "stage-1", template: "development" }, PROPOSAL.stages[1]!] },
        'the run plan proposal stage at position 0 is missing required field "tasks"',
      ],
      [
        { ...PROPOSAL, new_tasks: [{ id: "task-a" }, PROPOSAL.new_tasks[1]!] },
        'the run plan proposal new task at position 0 is missing required field "body"',
      ],
      [
        { ...PROPOSAL, new_tasks: [{ ...PROPOSAL.new_tasks[0], body: "" }, PROPOSAL.new_tasks[1]!] },
        "the run plan proposal new task at position 0 body must be a non-empty string",
      ],
      [
        { ...PROPOSAL, new_tasks: [{ ...PROPOSAL.new_tasks[0], body: 7 }, PROPOSAL.new_tasks[1]!] },
        "the run plan proposal new task at position 0 body must be a non-empty string",
      ],
    ];
    for (const [value, message] of cases) {
      const caught = catchOf(() => preparePipelineV2RunPlanProposal(value));
      expectMessage(caught, message);
      expect((caught as Error).message).not.toContain(canary);
    }
  });

  test("required fields must be own enumerable properties, never inherited", () => {
    const inheritedRoot = Object.create(
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "s1", template: "t", tasks: [{ id: "a", depends_on: [] }] }],
        new_tasks: [],
      },
    );
    const caught = catchOf(() => preparePipelineV2RunPlanProposal(inheritedRoot));
    expect(caught).toBeInstanceOf(PipelineV2RunPlanProposalError);
    expect((caught as Error).message).toBe('the run plan proposal is missing required field "schema_version"');
    expect((caught as Error).name).toBe("PipelineV2RunPlanProposalError");

    const inheritedStage = Object.create({
      id: "s1",
      template: "t",
      tasks: [{ id: "a", depends_on: [] }],
    });
    const stageCaught = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [inheritedStage],
        new_tasks: [],
      }),
    );
    expect(stageCaught).toBeInstanceOf(PipelineV2RunPlanProposalError);
    expect((stageCaught as Error).message).toBe('the run plan proposal stage at position 0 is missing required field "id"');
    expect((stageCaught as Error).name).toBe("PipelineV2RunPlanProposalError");
  });

  test("identifier grammar is enforced value-free for every id position", () => {
    const unsafeIds = [null, "", 7, " ", "a".repeat(129), "..", ".hidden", "-lead", "_lead", "with space", "has/slash"];
    for (const id of unsafeIds) {
      const stageCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, stages: [{ id, template: "development", tasks: PROPOSAL.stages[0]!.tasks }] }),
      );
      expectMessage(stageCaught, "the run plan proposal stage at position 0 id must be a safe non-empty identifier");
      const templateCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0]!, template: id }] }),
      );
      expectMessage(templateCaught, "the run plan proposal stage at position 0 template must be a safe non-empty identifier");
      const taskCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({
          ...PROPOSAL,
          stages: [{ ...PROPOSAL.stages[0]!, tasks: [{ id, depends_on: [] }] }],
        }),
      );
      expectMessage(taskCaught, "the run plan proposal stage at position 0 task at position 0 id must be a safe non-empty identifier");
      const depCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({
          ...PROPOSAL,
          stages: [{ ...PROPOSAL.stages[0]!, tasks: [{ id: "task-a", depends_on: [id] }] }],
        }),
      );
      expectMessage(
        depCaught,
        "the run plan proposal stage at position 0 task at position 0 depends_on entries must be safe non-empty identifiers",
      );
      const newTaskCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, new_tasks: [{ id, body: "b" }] }),
      );
      expectMessage(newTaskCaught, "the run plan proposal new task at position 0 id must be a safe non-empty identifier");
    }
  });

  test("duplicate new task ids are rejected as ambiguous content ownership", () => {
    const duplicate = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        ...PROPOSAL,
        new_tasks: [...PROPOSAL.new_tasks, { id: "task-a", body: "again" }],
      }),
    );
    expectMessage(duplicate, "the run plan proposal declares a duplicate new task id at position 2");
  });

  test("responsibility boundary: plan semantics are not validated here", () => {
    // Duplicate task pointers, duplicate stage ids, cycles, unknown and
    // cross-stage dependencies, empty stages and empty task lists are all
    // shape-valid proposals for this layer. They are NOT valid plans —
    // the plan revision manifest layer and the construction layer own
    // those rejections.
    const shapes: unknown[] = [
      // duplicate task pointers within one stage and across stages
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }, { id: "task-a", depends_on: [] }] },
          { id: "stage-2", template: "review", tasks: [{ id: "task-a", depends_on: [] }] },
        ],
        new_tasks: [{ id: "task-a", body: "b" }],
      },
      // duplicate stage ids
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
          { id: "stage-1", template: "review", tasks: [{ id: "task-b", depends_on: [] }] },
        ],
        new_tasks: [
          { id: "task-a", body: "b" },
          { id: "task-b", body: "b" },
        ],
      },
      // a dependency cycle (self edge and two-task cycle)
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", depends_on: ["task-a"] },
              { id: "task-b", depends_on: ["task-c"] },
              { id: "task-c", depends_on: ["task-b"] },
            ],
          },
        ],
        new_tasks: [
          { id: "task-a", body: "a" },
          { id: "task-b", body: "b" },
          { id: "task-c", body: "c" },
        ],
      },
      // an unknown dependency and a cross-stage dependency
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: ["no-such-task", "task-b"] }] },
          { id: "stage-2", template: "review", tasks: [{ id: "task-b", depends_on: [] }] },
        ],
        new_tasks: [
          { id: "task-a", body: "a" },
          { id: "task-b", body: "b" },
        ],
      },
      // duplicate dependency entries
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: ["task-a", "task-a"] }] }],
        new_tasks: [{ id: "task-a", body: "a" }],
      },
      // empty stage list
      { schema_version: 1, kind: "run_plan_proposal", stages: [], new_tasks: [] },
      // empty task list inside a stage
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [] }],
        new_tasks: [],
      },
      // a new task declared but referenced by no task pointer, and a
      // pointer task declared neither durable nor new — construction-layer
      // questions this substrate cannot answer
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
        new_tasks: [{ id: "task-orphan", body: "never planned" }],
      },
      {
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-missing", depends_on: [] }] }],
        new_tasks: [],
      },
    ];
    for (const shape of shapes) {
      expect(() => preparePipelineV2RunPlanProposal(shape)).not.toThrow();
    }
  });

  test("parse runs the same chain; malformed JSON carries no parser fragments", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const parsed = parsePipelineV2RunPlanProposal(JSON.stringify(PROPOSAL));
    expect(parsed).toEqual(preparePipelineV2RunPlanProposal(PROPOSAL));
    expect(parsed.stages[1]?.tasks[0]?.id).toBe("task-c");

    const malformed = catchOf(() => parsePipelineV2RunPlanProposal(`{"kind": "${canary}", "schema_v`));
    expectMessage(malformed, "the run plan proposal document is not valid JSON");
    expect((malformed as Error).message).not.toContain(canary);

    const structurallyInvalid = catchOf(() =>
      parsePipelineV2RunPlanProposal(`{"schema_version": 1, "kind": "run_plan_proposal", "stages": [], "new_tasks": [], "${canary}": 1}`),
    );
    expectMessage(structurallyInvalid, "the run plan proposal has unknown fields");
    expect((structurallyInvalid as Error).message).not.toContain(canary);
  });

  test("diagnostics never echo bodies or unknown property names", () => {
    const bodyCanary = "BODY-CANARY-unmet-acceptance-criteria";
    const caught = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
        new_tasks: [{ id: "task-a", body: bodyCanary }, { id: "task-a", body: "dup" }],
      }),
    );
    expect((caught as Error).message).not.toContain(bodyCanary);
    expect((caught as Error).message).toBe("the run plan proposal declares a duplicate new task id at position 1");
  });
});

describe("pipeline v2 run plan proposal provenance", () => {
  test("prepare and parse register their own exact frozen identities; structural equality holds", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    const parsed = parsePipelineV2RunPlanProposal(JSON.stringify(PROPOSAL));
    expect(parsed).toEqual(prepared);
    expect(parsed).not.toBe(prepared);
    expect(hasPreparedRunPlanProvenance(prepared, "run_plan_proposal")).toBe(true);
    expect(hasPreparedRunPlanProvenance(parsed, "run_plan_proposal")).toBe(true);
  });

  test("other prepared kinds are never recognized as proposals", () => {
    const planPrepared = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: "run-1",
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: hex("b") },
      origin_execution: 1,
      stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", revision: 1, sha256: hex("c"), depends_on: [] }] }],
    });
    const taskPrepared = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: "run-1",
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "b",
    });
    const intentPrepared = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: "run-1",
      wait_index: 1,
      stage_id: "stage-1",
      expected_plan_sha256: hex("d"),
      additional_iterations: 1,
    });
    expect(hasPreparedRunPlanProvenance(planPrepared, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(taskPrepared, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(intentPrepared, "run_plan_proposal")).toBe(false);
    const proposal = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(hasPreparedRunPlanProvenance(proposal, "plan_revision")).toBe(false);
    expect(hasPreparedRunPlanProvenance(proposal, "task_revision")).toBe(false);
    expect(hasPreparedRunPlanProvenance(proposal, "continue_stage_intent")).toBe(false);
    expect(hasPreparedRunPlanProvenance(proposal, "revise_task_intent")).toBe(false);
  });

  test("hand-built look-alikes, clones and nested objects carry no provenance", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    const lookalike: unknown = { schema_version: 1, kind: "run_plan_proposal", stages: prepared.stages, new_tasks: prepared.new_tasks };
    expect(hasPreparedRunPlanProvenance(lookalike, "run_plan_proposal")).toBe(false);
    const spread = { ...prepared };
    expect(hasPreparedRunPlanProvenance(spread, "run_plan_proposal")).toBe(false);
    const clone = structuredClone(prepared);
    expect(hasPreparedRunPlanProvenance(clone, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(null, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance("text", "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(42, "run_plan_proposal")).toBe(false);
  });

  test("a Proxy over the prepared object is rejected with traps never invoked", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    let traps = 0;
    const proxied = new Proxy(prepared, {
      get(target, property) {
        traps += 1;
        return Reflect.get(target, property);
      },
      has(target, property) {
        traps += 1;
        return Reflect.has(target, property);
      },
      ownKeys() {
        traps += 1;
        return Reflect.ownKeys(prepared);
      },
      getOwnPropertyDescriptor(target, property) {
        traps += 1;
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    expect(hasPreparedRunPlanProvenance(proxied, "run_plan_proposal")).toBe(false);
    expect(traps).toBe(0);
  });
});

describe("pipeline v2 run plan proposal export surface and source scan", () => {
  test("the runtime export surface is exactly the three contract keys", async () => {
    const namespace = (await import("../src/pipeline_v2_run_plan_proposal.ts")) as Record<
      string,
      unknown
    >;
    expect(Object.keys(namespace).sort()).toEqual([
      "PipelineV2RunPlanProposalError",
      "parsePipelineV2RunPlanProposal",
      "preparePipelineV2RunPlanProposal",
    ]);
  });

  test("the source carries no digest machinery, serializer or graph validator", () => {
    const source = readFileSync(
      join(import.meta.dir, "../src/pipeline_v2_run_plan_proposal.ts"),
      "utf8",
    );
    expect(source.includes("canonicalJson")).toBe(false);
    expect(source.includes("CryptoHasher")).toBe(false);
    expect(source.includes("pipeline-v2-run-plan-proposal")).toBe(false);
    expect(source.includes("canonical_json")).toBe(false);
    expect(source.includes("sha256")).toBe(false);
    expect(source.includes("JSON.parse(")).toBe(true);
    expect(source.split("JSON.parse(").length - 1).toBe(1);
    expect(source.includes("SAFE_ID_PATTERN")).toBe(false);
    expect(source.includes('from "./pipeline_v2_scalar.ts"')).toBe(true);
    expect(source.includes('from "./pipeline_v2_run_plan_provenance.ts"')).toBe(true);
    expect(source.includes('from "./pipeline_v2_freeze_internal.ts"')).toBe(true);
    expect(source.includes("registerPreparedRunPlanObject")).toBe(true);
    // no graph traversal, cycle or sorting machinery (prose in the doc
    // comment may mention the boundary; machinery may not exist)
    for (const banned of [
      "dependenciesByTask",
      "assertNoDependencyCycle",
      "topolog",
      "stageIds.has",
      "taskIds.has",
      "newTaskIds.has(entryId) && false",
    ]) {
      expect(source.includes(banned)).toBe(false);
    }
    expect(source.includes(".sort(")).toBe(false);
  });

  test("the source imports no filesystem, state, coordinator, runner or CLI module", () => {
    const source = readFileSync(
      join(import.meta.dir, "../src/pipeline_v2_run_plan_proposal.ts"),
      "utf8",
    );
    for (const banned of [
      "node:fs",
      "node:path",
      "node:os",
      "pipeline_v2_state",
      "pipeline_v2_coordinator",
      "pipeline_v2_runner",
      "pipeline_v2_runtime",
      "pipeline_engine",
      "main.ts",
      "cli",
    ]) {
      expect(source.includes(banned)).toBe(false);
    }
  });

  test("the module exposes no registry, minter or test seam", async () => {
    const namespace = (await import("../src/pipeline_v2_run_plan_proposal.ts")) as Record<
      string,
      unknown
    >;
    for (const key of Object.keys(namespace)) {
      expect(key.toLowerCase()).not.toContain("registry");
      expect(key.toLowerCase()).not.toContain("provenance");
      expect(key.toLowerCase()).not.toContain("freeze");
      expect(key.toLowerCase()).not.toContain("normalize");
    }
  });
});
