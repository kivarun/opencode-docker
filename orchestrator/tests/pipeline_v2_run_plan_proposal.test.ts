/**
 * The pure agent-authored run plan proposal substrate
 * (`pipeline_v2_run_plan_proposal.ts`): one validation/normalization
 * chain, the shared provenance registry under its own kind, deep-frozen
 * prepared snapshots, content-free diagnostics and the dedicated digest
 * domain. The durable-ledger rules (existing-task reuse, revision
 * numbers, digests, bindings) belong to the future construction layer
 * and are deliberately out of scope here; these tests pin the structural
 * contract exactly.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  PipelineV2RunPlanProposalError,
  parsePipelineV2RunPlanProposal,
  preparePipelineV2RunPlanProposal,
  type PipelineV2RunPlanProposal,
} from "../src/pipeline_v2_run_plan_proposal.ts";
import { canonicalJson } from "../src/canonical_json.ts";
import { hasPreparedRunPlanProvenance } from "../src/pipeline_v2_run_plan_provenance.ts";
import { preparePlanRevisionManifest } from "../src/pipeline_v2_run_plan_manifests.ts";

const hex = (char: string): string => char.repeat(64);
const domainDigest = (domain: string, canonical: string): string =>
  createHash("sha256").update(domain).update(canonical).digest("hex");

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
  test("prepare returns the exact prepared shape with the dedicated digest domain", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(Object.keys(prepared).sort()).toEqual(["canonical_json", "manifest", "sha256"]);
    expect(prepared.canonical_json).toBe(canonicalJson(prepared.manifest));
    expect(prepared.sha256).toBe(
      domainDigest("pipeline-v2-run-plan-proposal\0", prepared.canonical_json),
    );
    const manifest = prepared.manifest as PipelineV2RunPlanProposal;
    expect(manifest.schema_version).toBe(1);
    expect(manifest.kind).toBe("run_plan_proposal");
    expect(manifest.stages).toHaveLength(2);
    expect(manifest.stages[0]?.tasks.map((task) => task.id)).toEqual(["task-a", "task-b"]);
    expect(manifest.new_tasks.map((task) => task.id)).toEqual(["task-a", "task-b"]);
  });

  test("the snapshot is deep-frozen and independent of later input mutations", () => {
    const value: { new_tasks: Array<{ id: string; body: string }>; stages: Array<Record<string, unknown>> } =
      structuredClone(PROPOSAL);
    const prepared = preparePipelineV2RunPlanProposal(value);
    value.new_tasks[0] = { id: "task-a", body: "MUTATED-BODY" };
    value.stages[0]!.tasks = [];
    const manifest = prepared.manifest as PipelineV2RunPlanProposal;
    expect((manifest.new_tasks[0] as { body: string }).body).toBe("Implement the acceptance test parser");
    expect(manifest.stages[0]?.tasks).toHaveLength(2);
    expect(prepared.canonical_json).toBe(canonicalJson(PROPOSAL));
    expectDeepFrozen(prepared);
    expect(Object.isFrozen(manifest.stages[0]?.tasks)).toBe(true);
    expect(Object.isFrozen(manifest.stages[0]?.tasks[0]?.depends_on)).toBe(true);
  });

  test("repeated preparation of an equivalent proposal is deterministic", () => {
    const first = preparePipelineV2RunPlanProposal(PROPOSAL);
    const second = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(second).toEqual(first);
    expect(second.sha256).toBe(first.sha256);
    expect(second.canonical_json).toBe(first.canonical_json);
  });

  test("task order and dependency order inside a stage are never semantic; stage order is", () => {
    const permuted = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-b", depends_on: ["task-a"] },
            { id: "task-a", depends_on: [] },
          ],
        },
        { id: "stage-2", template: "review", tasks: [{ id: "task-c", depends_on: [] }] },
      ],
      new_tasks: [
        { id: "task-b", body: "Wire the parser into the stage runner" },
        { id: "task-a", body: "Implement the acceptance test parser" },
      ],
    });
    const base = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(permuted.canonical_json).toBe(base.canonical_json);
    expect(permuted.sha256).toBe(base.sha256);

    const reorderedStages = preparePipelineV2RunPlanProposal({
      schema_version: 1,
      kind: "run_plan_proposal",
      stages: [
        { id: "stage-2", template: "review", tasks: [{ id: "task-c", depends_on: [] }] },
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
        { id: "task-a", body: "Implement the acceptance test parser" },
        { id: "task-b", body: "Wire the parser into the stage runner" },
      ],
    });
    expect(reorderedStages.sha256).not.toBe(base.sha256);
  });

  test("exact fields at every level; unknown keys are never echoed", () => {
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
            { ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: [], extra: 1 }, PROPOSAL.stages[0]!.tasks[1]!] },
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
        { ...PROPOSAL, stages: [] },
        "the run plan proposal.stages must not be empty",
      ],
      [
        { ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0], tasks: [] }, PROPOSAL.stages[1]!] },
        "the run plan proposal stage at position 0 tasks must not be empty",
      ],
      [
        { ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0], tasks: null }, PROPOSAL.stages[1]!] },
        "the run plan proposal stage at position 0 tasks must be an array",
      ],
      [
        { ...PROPOSAL, stages: [{ id: "stage-1", template: "development" }, PROPOSAL.stages[1]!] },
        "the run plan proposal stage at position 0 is missing required field \"tasks\"",
      ],
      [
        { ...PROPOSAL, new_tasks: [{ id: "task-a" }, PROPOSAL.new_tasks[1]!] },
        "the run plan proposal new task at position 0 is missing required field \"body\"",
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

  test("identifier grammar is enforced value-free", () => {
    const unsafeIds = [null, "", 7, " ", "a".repeat(129), "..", ".hidden", "-lead", "_lead", "with space", "has/slash"];
    for (const id of unsafeIds) {
      const caught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, stages: [{ id, template: "development", tasks: PROPOSAL.stages[0]!.tasks }] }),
      );
      expect(caught).toBeInstanceOf(PipelineV2RunPlanProposalError);
      expect((caught as Error).message).toBe("the run plan proposal stage at position 0 id must be a safe non-empty identifier");
      const templateCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0], template: id }] }),
      );
      expect(templateCaught).toBeInstanceOf(PipelineV2RunPlanProposalError);
      expect((templateCaught as Error).message).toBe("the run plan proposal stage at position 0 template must be a safe non-empty identifier");
      const taskCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({
          ...PROPOSAL,
          stages: [{ ...PROPOSAL.stages[0], tasks: [{ id, depends_on: [] }] }],
        }),
      );
      expect(taskCaught).toBeInstanceOf(PipelineV2RunPlanProposalError);
      expect((taskCaught as Error).message).toBe("the run plan proposal stage at position 0 task at position 0 id must be a safe non-empty identifier");
      const newTaskCaught = catchOf(() =>
        preparePipelineV2RunPlanProposal({ ...PROPOSAL, new_tasks: [{ id, body: "b" }] }),
      );
      expect(newTaskCaught).toBeInstanceOf(PipelineV2RunPlanProposalError);
      expect((newTaskCaught as Error).message).toBe("the run plan proposal new task at position 0 id must be a safe non-empty identifier");
    }
  });

  test("stage and task ids are plan-unique; duplicates are rejected", () => {
    const duplicateStage = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [PROPOSAL.stages[0], PROPOSAL.stages[0]],
        new_tasks: PROPOSAL.new_tasks,
      }),
    );
    expectMessage(duplicateStage, "the run plan proposal declares a duplicate stage id at position 1");

    const duplicateTaskSameStage = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", depends_on: [] },
              { id: "task-a", depends_on: [] },
            ],
          },
        ],
        new_tasks: [{ id: "task-a", body: "b" }],
      }),
    );
    expectMessage(duplicateTaskSameStage, 'the run plan proposal declares task "task-a" more than once');

    const duplicateTaskAcrossStages = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [
          { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
          { id: "stage-2", template: "review", tasks: [{ id: "task-a", depends_on: [] }] },
        ],
        new_tasks: [{ id: "task-a", body: "b" }],
      }),
    );
    expectMessage(duplicateTaskAcrossStages, 'the run plan proposal declares task "task-a" more than once');
  });

  test("dependency rules: duplicates, unsafe entries, self edges, cross-stage edges and cycles", () => {
    const cases: [unknown, string][] = [
      [
        { ...PROPOSAL, stages: [{ ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: ["task-a"] }, PROPOSAL.stages[0]!.tasks[1]] }, PROPOSAL.stages[1]!] },
        'the run plan proposal stage at position 0 task "task-a" depends on itself',
      ],
      [
        {
          ...PROPOSAL,
          stages: [
            { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
            { id: "stage-2", template: "review", tasks: [{ id: "task-b", depends_on: ["task-a"] }] },
          ],
          new_tasks: [
            { id: "task-a", body: "b" },
            { id: "task-b", body: "b" },
          ],
        },
        'the run plan proposal stage at position 1 task "task-b" depends on a task outside its stage',
      ],
      [
        {
          ...PROPOSAL,
          stages: [
            {
              id: "stage-1",
              template: "development",
              tasks: [
                { id: "task-a", depends_on: ["task-b"] },
                { id: "task-b", depends_on: ["task-a"] },
              ],
            },
          ],
          new_tasks: [
            { id: "task-a", body: "b" },
            { id: "task-b", body: "b" },
          ],
        },
        "the run plan proposal stage at position 0 tasks declare a dependency cycle",
      ],
      [
        {
          ...PROPOSAL,
          stages: [{ ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: ["task-a", "task-b"] }, PROPOSAL.stages[0]!.tasks[1]] }, PROPOSAL.stages[1]!],
        },
        'the run plan proposal stage at position 0 task "task-a" depends on itself',
      ],
    ];
    for (const [value, message] of cases) {
      const caught = catchOf(() => preparePipelineV2RunPlanProposal(value));
      expectMessage(caught, message);
    }
    const unsafeEntry = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        ...PROPOSAL,
        stages: [{ ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: [null] }] }],
      }),
    );
    expectMessage(
      unsafeEntry,
      "the run plan proposal stage at position 0 task at position 0 depends_on entries must be safe non-empty identifiers",
    );
    const notArray = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        ...PROPOSAL,
        stages: [{ ...PROPOSAL.stages[0], tasks: [{ id: "task-a", depends_on: "task-b" }] }],
      }),
    );
    expectMessage(
      notArray,
      "the run plan proposal stage at position 0 task at position 0 depends_on must be an array",
    );
    const duplicateDep = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        ...PROPOSAL,
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", depends_on: [] },
              { id: "task-b", depends_on: ["task-a", "task-a"] },
            ],
          },
        ],
        new_tasks: PROPOSAL.new_tasks,
      }),
    );
    expectMessage(
      duplicateDep,
      "the run plan proposal stage at position 0 task at position 1 depends_on declares a duplicate dependency",
    );
  });

  test("new tasks: duplicates, unused entries rejected; intersection with task references allowed", () => {
    const duplicate = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        ...PROPOSAL,
        new_tasks: [...PROPOSAL.new_tasks, { id: "task-a", body: "again" }],
      }),
    );
    expectMessage(duplicate, "the run plan proposal declares a duplicate new task id at position 2");

    const unused = catchOf(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
        new_tasks: [
          { id: "task-a", body: "b" },
          { id: "task-orphan", body: "never planned" },
        ],
      }),
    );
    expectMessage(unused, 'the run plan proposal declares new task "task-orphan" that no stage task references');

    // A task reference may name an id also present in new_tasks: whether
    // that id is a durable task (a silent rewrite) is a ledger question
    // the construction layer answers, not this substrate.
    expect(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
        new_tasks: [{ id: "task-a", body: "b" }],
      }),
    ).not.toThrow();

    expect(() =>
      preparePipelineV2RunPlanProposal({
        schema_version: 1,
        kind: "run_plan_proposal",
        stages: PROPOSAL.stages,
        new_tasks: [],
      }),
    ).not.toThrow();
  });

  test("parse runs the same chain; malformed JSON carries no parser fragments", () => {
    const canary = "SECRET-BEARER-dht_deadbeef";
    const parsed = parsePipelineV2RunPlanProposal(JSON.stringify(PROPOSAL));
    expect(parsed).toEqual(preparePipelineV2RunPlanProposal(PROPOSAL));
    expect(parsed.sha256).toBe(preparePipelineV2RunPlanProposal(PROPOSAL).sha256);

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
  });
});

describe("pipeline v2 run plan proposal provenance", () => {
  test("the prepared object is registered under its own kind; look-alikes and clones are not", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    expect(hasPreparedRunPlanProvenance(prepared, "run_plan_proposal")).toBe(true);
    expect(hasPreparedRunPlanProvenance(prepared, "plan_revision")).toBe(false);
    expect(hasPreparedRunPlanProvenance(prepared, "task_revision")).toBe(false);
    expect(hasPreparedRunPlanProvenance(prepared, "continue_stage_intent")).toBe(false);
    expect(hasPreparedRunPlanProvenance(prepared, "revise_task_intent")).toBe(false);

    const lookalike = { manifest: prepared.manifest, canonical_json: prepared.canonical_json, sha256: prepared.sha256 };
    expect(hasPreparedRunPlanProvenance(lookalike, "run_plan_proposal")).toBe(false);
    const clone = structuredClone(prepared);
    expect(hasPreparedRunPlanProvenance(clone, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(prepared.manifest, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(null, "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance("text", "run_plan_proposal")).toBe(false);
    expect(hasPreparedRunPlanProvenance(42, "run_plan_proposal")).toBe(false);

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
    expect(hasPreparedRunPlanProvenance(planPrepared, "run_plan_proposal")).toBe(false);
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

describe("pipeline v2 run plan proposal digest domains and structural scan", () => {
  test("the proposal domain is separated from the plan, task and intent domains", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    for (const domain of [
      "pipeline-v2-plan-revision\0",
      "pipeline-v2-task-revision\0",
      "pipeline-v2-wait-intent\0",
      "pipeline-v2-wait-request\0",
      "pipeline-v2-wait-response\0",
      "pipeline-v2-execution-snapshot\0",
      "pipeline-v2-input\0",
      "pipeline-v2-output\0",
      "pipeline-v2-run-output\0",
      "pipeline-v2-decision-input\0",
    ]) {
      expect(domain).not.toBe("pipeline-v2-run-plan-proposal\0");
      expect(domainDigest(domain, prepared.canonical_json)).not.toBe(prepared.sha256);
    }
  });

  test("no paths, timestamps, credentials or payloads; key scan for banned fields", () => {
    const prepared = preparePipelineV2RunPlanProposal(PROPOSAL);
    const text = JSON.stringify(prepared);
    for (const canary of [
      "/opt/orchestrator",
      "/home/michael",
      "2026-01-01T00:00:00",
      "OPENCODE_CONFIG_CONTENT",
      "dhc_0392",
      "bearer_token",
      "LLM_KEY",
      "endpoint",
      "apiKey",
    ]) {
      expect(text).not.toContain(canary);
    }
    const keys = new Set<string>();
    const collect = (value: unknown): void => {
      if (Array.isArray(value)) {
        for (const entry of value) collect(entry);
        return;
      }
      if (value !== null && typeof value === "object") {
        for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
          keys.add(key);
          collect(child);
        }
      }
    };
    collect(prepared.manifest);
    for (const banned of [
      "run_id",
      "revision",
      "previous_sha256",
      "sha256",
      "root_task",
      "origin_execution",
      "origin",
      "wait_index",
      "generation",
      "iteration",
      "path",
      "timestamp",
      "created_at",
      "payload",
      "comment",
      "facts",
      "credentials",
      "token",
      "endpoint",
    ]) {
      expect(keys.has(banned), `the proposal must not carry a ${banned} field`).toBe(false);
    }
    expect(keys.has("schema_version")).toBe(true);
    expect(keys.has("kind")).toBe(true);
    expect(keys.has("stages")).toBe(true);
    expect(keys.has("new_tasks")).toBe(true);
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

  test("the source carries exactly one JSON.parse, the shared serializer, scalars, registry and freeze helper", () => {
    const source = readFileSync(
      join(import.meta.dir, "../src/pipeline_v2_run_plan_proposal.ts"),
      "utf8",
    );
    expect(source.split("JSON.parse(").length - 1).toBe(1);
    expect(source.includes("SAFE_ID_PATTERN")).toBe(false);
    expect(source.includes("canonicalJsonValue")).toBe(false);
    expect(source.includes('from "./pipeline_v2_scalar.ts"')).toBe(true);
    expect(source.includes('from "./canonical_json.ts"')).toBe(true);
    expect(source.includes('from "./pipeline_v2_run_plan_provenance.ts"')).toBe(true);
    expect(source.includes('from "./pipeline_v2_freeze_internal.ts"')).toBe(true);
    expect(source.includes("deepFreezeValue")).toBe(true);
    expect(source.includes("new Bun.CryptoHasher")).toBe(true);
    expect(source.includes("registerPreparedRunPlanObject")).toBe(true);
    expect(source.includes('"run_plan_proposal"')).toBe(true);
    expect(source.toLowerCase().includes("registry")).toBe(true);
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

  test("the module exposes no registry, minter, digest builder or test seam", async () => {
    const namespace = (await import("../src/pipeline_v2_run_plan_proposal.ts")) as Record<
      string,
      unknown
    >;
    for (const key of Object.keys(namespace)) {
      expect(key.toLowerCase()).not.toContain("registry");
      expect(key.toLowerCase()).not.toContain("provenance");
      expect(key.toLowerCase()).not.toContain("digest");
      expect(key.toLowerCase()).not.toContain("freeze");
      expect(key.toLowerCase()).not.toContain("normalize");
    }
  });
});
