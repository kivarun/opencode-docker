/**
 * The deterministic apply-all planner over the verified Project Changes
 * observation (`pipeline_v2_apply_planner_internal.ts`): every reviewed
 * change kind, mixed relations, containment, type substitutions, the
 * fixed ordering, dense indexes, determinism and the hardlink policy.
 *
 * Every fixture is a real finalized schema-v10 run built through the real
 * provenance copy, the real reducer and the real read-only observation —
 * the planner consumes exactly the trees the production observation
 * produces (no second scanner).
 */
import { afterAll, describe, expect, test } from "bun:test";
import { rm, link, mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { buildFinalizedApplyRun, disposeApplyRoots, writeProjectTree, type ApplyFixtureEntry } from "./apply_fixtures.ts";
import {
  planProjectApplySteps,
  PipelineV2ApplyPlannerError,
} from "../src/pipeline_v2_apply_planner_internal.ts";
import type { PipelineV2ApplyPlan } from "../src/pipeline_v2_apply_plan_internal.ts";
import type { PipelineV2ProjectChangeEntry } from "../src/pipeline_v2_project_changes_internal.ts";

afterAll(async () => {
  await disposeApplyRoots();
});

type PlanShape = {
  steps: Array<{ step_index: number; change_index: number; path: string; kind: string; payload_index?: number; expected_before: unknown; desired_after: unknown }>;
  payloads: Array<{ payload_index: number; change_index: number; byte_count: number; sha256: string; artifact: string }>;
  noops: Array<{ change_index: number; path: string; expected: unknown }>;
  total_payload_byte_count: number;
};

function planOf(run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>): PlanShape {
  const plan: PipelineV2ApplyPlan = planProjectApplySteps(
    { operator: run.observed.operator, changes: run.observed.changes, baseline: run.observed.baseline },
    run.runId,
    run.runId,
    "proj",
    "b".repeat(64),
    run.observation,
    run.state.revision,
  );
  return plan as unknown as PlanShape;
}

function stepsOf(run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>): string[] {
  return planOf(run).steps.map((step) => `${step.kind}:${step.path}`);
}

describe("apply planner: change kinds", () => {
  test("an added regular file plans one publication with a sealed payload binding", async () => {
    const run = await buildFinalizedApplyRun("planner-add", {
      worker: async (ws) => {
        await writeProjectTree(ws, [{ path: "new.txt", kind: "file", content: "NEW" }]);
      },
    });
    expect(stepsOf(run)).toEqual(["publish_regular_file:new.txt"]);
    const plan = planOf(run);
    expect(plan.steps[0]!.step_index).toBe(1);
    expect(plan.steps[0]!.change_index).toBe(1);
    expect(plan.steps[0]!.payload_index).toBe(1);
    expect(plan.payloads).toHaveLength(1);
    expect(plan.payloads[0]).toEqual({
      payload_index: 1,
      change_index: 1,
      byte_count: 3,
      sha256: plan.payloads[0]!.sha256,
      artifact: "1.payload",
    });
    expect(plan.total_payload_byte_count).toBe(3);
  });

  test("an added directory subtree plans shallowest-first creations and later publications", async () => {
    const run = await buildFinalizedApplyRun("planner-addtree", {
      worker: async (ws) => {
        await writeProjectTree(ws, [
          { path: "a", kind: "directory" },
          { path: "a/b", kind: "directory" },
          { path: "a/b/f.txt", kind: "file", content: "F" },
        ]);
      },
    });
    expect(stepsOf(run)).toEqual([
      "create_directory:a",
      "create_directory:a/b",
      "publish_regular_file:a/b/f.txt",
    ]);
  });

  test("a deleted subtree plans explicit deepest-first leaf removals and a final directory removal", async () => {
    const run = await buildFinalizedApplyRun("planner-deltree", {
      source: [
        { path: "keep.txt", kind: "file", content: "KEEP" },
        { path: "gone", kind: "directory" },
        { path: "gone/inner", kind: "directory" },
        { path: "gone/inner/deep.txt", kind: "file", content: "DEEP" },
        { path: "gone/shallow.txt", kind: "file", content: "SHALLOW" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "gone"), { recursive: true });
      },
    });
    const steps = stepsOf(run);
    expect(steps).toEqual([
      "remove_leaf:gone/inner/deep.txt",
      "remove_directory:gone/inner",
      "remove_leaf:gone/shallow.txt",
      "remove_directory:gone",
    ]);
    // The dependency invariants hold: every directory is removed only
    // after all of its projected children were removed before it.
    const removedBefore = new Map<string, number>();
    planOf(run).steps.forEach((step) => removedBefore.set(step.path, step.step_index));
    expect(removedBefore.get("gone/inner/deep.txt")!).toBeLessThan(removedBefore.get("gone/inner")!);
    expect(removedBefore.get("gone/shallow.txt")!).toBeLessThan(removedBefore.get("gone")!);
    expect(removedBefore.get("gone/inner")!).toBeLessThan(removedBefore.get("gone")!);
    const plan = planOf(run);
    expect(plan.steps.every((step) => step.kind === "remove_leaf" || step.kind === "remove_directory")).toBe(true);
    expect(plan.steps.map((step) => step.step_index)).toEqual([1, 2, 3, 4]);
    expect(plan.payloads).toHaveLength(0);
    expect(plan.total_payload_byte_count).toBe(0);
  });

  test("content, mode and combined modifications plan the exact operation", async () => {
    const run = await buildFinalizedApplyRun("planner-content", {
      source: [
        { path: "content.txt", kind: "file", content: "OLD" },
        { path: "mode-only.txt", kind: "file", content: "SAME" },
      ],
      worker: async (ws) => {
        await writeFile(join(ws, "content.txt"), "NEW");
        await import("node:fs/promises").then((fs) => fs.chmod(join(ws, "mode-only.txt"), 0o400));
      },
    });
    const plan = planOf(run);
    const byPath = new Map(plan.steps.map((step) => [step.path, step]));
    expect(byPath.get("content.txt")!.kind).toBe("publish_regular_file");
    expect(byPath.get("mode-only.txt")!.kind).toBe("set_mode");
    expect(byPath.get("mode-only.txt")!.desired_after).toEqual({ kind: "regular_file", mode: 0o400, byte_count: 4, sha256: (byPath.get("mode-only.txt")!.desired_after as { sha256: string }).sha256 });
    expect(byPath.get("mode-only.txt")!.expected_before).toEqual({ kind: "regular_file", mode: 0o644, byte_count: 4, sha256: (byPath.get("mode-only.txt")!.expected_before as { sha256: string }).sha256 });
  });

  test("a mode-only modification of a directory plans a set_mode step", async () => {
    const run = await buildFinalizedApplyRun("planner-dirmode", {
      source: [{ path: "d", kind: "directory", mode: 0o750 }],
      worker: async (ws) => {
        await import("node:fs/promises").then((fs) => fs.chmod(join(ws, "d"), 0o755));
      },
    });
    const plan = planOf(run);
    expect(plan.steps.map((step) => `${step.kind}:${step.path}`)).toEqual(["set_mode:d"]);
    expect(plan.steps[0]!.desired_after).toEqual({ kind: "directory", mode: 0o755 });
    expect(plan.steps[0]!.expected_before).toEqual({ kind: "directory", mode: 0o750 });
  });

  test("a symlink target modification plans one replacement publication", async () => {
    const run = await buildFinalizedApplyRun("planner-symlink", {
      source: [{ path: "l", kind: "symlink", target: "old-target" }],
      worker: async (ws) => {
        await rm(join(ws, "l"));
        await import("node:fs/promises").then((fs) => fs.symlink("new-target", join(ws, "l")));
      },
    });
    expect(stepsOf(run)).toEqual(["publish_symlink:l"]);
    const plan = planOf(run);
    expect(plan.steps[0]!.desired_after).toEqual({ kind: "symlink", target: "new-target" });
    expect(plan.steps[0]!.expected_before).toEqual({ kind: "symlink", target: "old-target" });
  });
});

describe("apply planner: type substitutions", () => {
  const substitutionCases: Array<{
    name: string;
    source: ApplyFixtureEntry[];
    worker: (ws: string) => Promise<void>;
    expected: string[];
  }> = [
    {
      name: "file to directory with an added child",
      source: [{ path: "d", kind: "file", content: "WAS-FILE" }],
      worker: async (ws) => {
        await rm(join(ws, "d"));
        await writeProjectTree(ws, [
          { path: "d", kind: "directory" },
          { path: "d/child.txt", kind: "file", content: "CHILD" },
        ]);
      },
      expected: ["remove_leaf:d", "create_directory:d", "publish_regular_file:d/child.txt"],
    },
    {
      name: "directory to file",
      source: [
        { path: "d", kind: "directory" },
        { path: "d/a.txt", kind: "file", content: "A" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "d"), { recursive: true });
        await writeFile(join(ws, "d"), "NOW-FILE");
      },
      expected: ["remove_leaf:d/a.txt", "remove_directory:d", "publish_regular_file:d"],
    },
    {
      name: "file to symlink",
      source: [{ path: "f", kind: "file", content: "F" }],
      worker: async (ws) => {
        await rm(join(ws, "f"));
        await import("node:fs/promises").then((fs) => fs.symlink("t", join(ws, "f")));
      },
      expected: ["remove_leaf:f", "publish_symlink:f"],
    },
    {
      name: "symlink to file",
      source: [{ path: "f", kind: "symlink", target: "t" }],
      worker: async (ws) => {
        await rm(join(ws, "f"));
        await writeFile(join(ws, "f"), "NOW-FILE");
      },
      expected: ["remove_leaf:f", "publish_regular_file:f"],
    },
    {
      name: "directory to symlink",
      source: [
        { path: "d", kind: "directory" },
        { path: "d/a.txt", kind: "file", content: "A" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "d"), { recursive: true });
        await import("node:fs/promises").then((fs) => fs.symlink("t", join(ws, "d")));
      },
      expected: ["remove_leaf:d/a.txt", "remove_directory:d", "publish_symlink:d"],
    },
    {
      name: "symlink to directory",
      source: [{ path: "d", kind: "symlink", target: "t" }],
      worker: async (ws) => {
        await rm(join(ws, "d"));
        await mkdir(join(ws, "d"));
        await writeFile(join(ws, "d", "x.txt"), "X");
      },
      expected: ["remove_leaf:d", "create_directory:d", "publish_regular_file:d/x.txt"],
    },
  ];

  for (const candidate of substitutionCases) {
    test(`type substitution ${candidate.name} produces an explicit removal and creation`, async () => {
      const run = await buildFinalizedApplyRun("planner-sub", { source: candidate.source, worker: candidate.worker });
      expect(stepsOf(run)).toEqual(candidate.expected);
      const plan = planOf(run);
      // Every substitution keeps exactly two steps per substituted path,
      // the removal always before the creation, and every step carries its
      // own dense execution index distinct from the change reference.
      for (const step of plan.steps) {
        expect(step.step_index).toBeGreaterThan(0);
      }
      const byPath = new Map<string, string[]>();
      for (const step of plan.steps) {
        byPath.set(step.path, [...(byPath.get(step.path) ?? []), step.kind]);
      }
      for (const [path, kinds] of byPath) {
        if (kinds.length === 2) {
          expect(kinds[0]).toMatch(/^remove_/);
          expect(kinds[1]).toMatch(/^(create_directory|publish_)/);
        } else {
          expect(kinds).toHaveLength(1);
        }
        expect(path.length).toBeGreaterThan(0);
      }
    });
  }
});

describe("apply planner: mixed relations, containment and policy", () => {
  test("mixed matches_result and matches_baseline changes plan only the pending work", async () => {
    const run = await buildFinalizedApplyRun("planner-mixed", {
      source: [
        { path: "pending.txt", kind: "file", content: "OLD" },
        { path: "applied.txt", kind: "file", content: "OLD2" },
      ],
      worker: async (ws) => {
        await writeFile(join(ws, "pending.txt"), "NEW");
        await writeFile(join(ws, "applied.txt"), "NEW2");
      },
      operator: async (op) => {
        // The operator already carried one change out; the other is pending.
        await writeFile(join(op, "applied.txt"), "NEW2");
      },
    });
    const plan = planOf(run);
    expect(plan.steps.map((step) => `${step.kind}:${step.path}`)).toEqual(["publish_regular_file:pending.txt"]);
    expect(plan.noops.map((noop) => noop.path)).toEqual(["applied.txt"]);
    const applied = run.observed.changes.find((change: PipelineV2ProjectChangeEntry) => change.path === "applied.txt")!;
    const expectedNode = plan.noops[0]!.expected as { kind: string; mode: number; byte_count: number; sha256: string };
    expect(expectedNode.kind).toBe("regular_file");
    expect(expectedNode.byte_count).toBe(applied.result && "byte_count" in applied.result ? applied.result.byte_count : -1);
    expect(expectedNode.sha256).toBe(applied.result && "sha256" in applied.result ? applied.result.sha256 : "");
    expect(applied.source_relation).toBe("matches_result");
  });

  test("operator-only paths outside affected subtrees are preserved and never planned", async () => {
    const run = await buildFinalizedApplyRun("planner-operatoronly", {
      source: [{ path: "base.txt", kind: "file", content: "BASE" }],
      worker: async (ws) => {
        await writeProjectTree(ws, [{ path: "new.txt", kind: "file", content: "NEW" }]);
      },
      operator: async (op) => {
        await writeProjectTree(op, [{ path: "operator-only", kind: "directory" }, { path: "operator-only/note.txt", kind: "file", content: "LOCAL" }]);
      },
    });
    const plan = planOf(run);
    expect(plan.steps.map((step) => step.path)).toEqual(["new.txt"]);
    expect(JSON.stringify(plan)).not.toContain("operator-only");
  });

  test("an operator-only descendant inside a removed directory is a conflict with zero planned steps", async () => {
    const run = await buildFinalizedApplyRun("planner-containment", {
      source: [
        { path: "gone", kind: "directory" },
        { path: "gone/a.txt", kind: "file", content: "A" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "gone"), { recursive: true });
      },
      operator: async (op) => {
        await writeProjectTree(op, [{ path: "gone/local.txt", kind: "file", content: "LOCAL-DATA" }]);
      },
    });
    let refusal: unknown = undefined;
    try {
      planOf(run);
    } catch (cause) {
      refusal = cause;
    }
    expect(refusal instanceof PipelineV2ApplyPlannerError).toBe(true);
    expect((refusal as PipelineV2ApplyPlannerError).refusal).toBe("diverged_project");
    expect((refusal as Error).message).toBe("an operator-only path lives inside a directory the reviewed plan removes");
  });

  test("a diverged affected path refuses the whole plan", async () => {
    const run = await buildFinalizedApplyRun("planner-diverged", {
      source: [{ path: "base.txt", kind: "file", content: "BASE" }],
      worker: async (ws) => {
        await writeFile(join(ws, "base.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "base.txt"), "SOMEONE-ELSE");
      },
    });
    expect(() => planOf(run)).toThrow("an affected path no longer matches the reviewed observation");
  });

  test("an operator hardlink pair created by the worker is refused for v1", async () => {
    const run = await buildFinalizedApplyRun("planner-hardlink", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "SHARED");
        await link(join(ws, "a.txt"), join(ws, "b.txt"));
      },
    });
    let refusal: unknown = undefined;
    try {
      planOf(run);
    } catch (cause) {
      refusal = cause;
    }
    expect(refusal instanceof PipelineV2ApplyPlannerError).toBe(true);
    expect((refusal as PipelineV2ApplyPlannerError).refusal).toBe("unsupported_metadata");
    expect((refusal as Error).message).toBe("a reviewed result carries a hardlink group");
  });

  test("a source hardlink pair touched by the worker is refused for v1", async () => {
    const run = await buildFinalizedApplyRun("planner-baseline-hardlink", {
      source: [{ path: "shared.txt", kind: "file", content: "SHARED" }],
      mutateSource: async (sourceProject) => {
        await link(join(sourceProject, "shared.txt"), join(sourceProject, "twin.txt"));
      },
      worker: async (ws) => {
        await writeFile(join(ws, "shared.txt"), "CHANGED");
      },
    });
    let refusal: unknown = undefined;
    try {
      planOf(run);
    } catch (cause) {
      refusal = cause;
    }
    expect(refusal instanceof PipelineV2ApplyPlannerError).toBe(true);
    expect((refusal as PipelineV2ApplyPlannerError).refusal).toBe("unsupported_metadata");
    expect((refusal as Error).message).toBe("a touched baseline path carries a hardlink group");
  });

  test("a rename stays exactly one deletion and one addition", async () => {
    const run = await buildFinalizedApplyRun("planner-rename", {
      source: [{ path: "old.txt", kind: "file", content: "MOVED" }],
      worker: async (ws) => {
        await rm(join(ws, "old.txt"));
        await writeProjectTree(ws, [{ path: "new.txt", kind: "file", content: "MOVED" }]);
      },
    });
    expect(stepsOf(run)).toEqual([
      "remove_leaf:old.txt",
      "publish_regular_file:new.txt",
    ]);
  });

  test("the plan never carries a recursive delete step and never deletes more than the reviewed entries", async () => {
    const run = await buildFinalizedApplyRun("planner-norecursive", {
      source: [
        { path: "gone", kind: "directory" },
        { path: "gone/a", kind: "directory" },
        { path: "gone/a/b", kind: "directory" },
        { path: "gone/a/b/f.txt", kind: "file", content: "F" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "gone"), { recursive: true });
      },
    });
    const plan = planOf(run);
    const kinds = new Set(plan.steps.map((step) => step.kind));
    expect(kinds.has("remove_leaf")).toBe(true);
    expect(kinds.has("remove_directory")).toBe(true);
    expect(plan.steps.filter((step) => step.kind === "remove_directory")).toHaveLength(3);
    // Every removed path is an explicitly reviewed deletion.
    const changes = new Map(run.observed.changes.map((change: PipelineV2ProjectChangeEntry) => [change.path, change]));
    for (const step of plan.steps) {
      expect(changes.has(step.path)).toBe(true);
      expect(changes.get(step.path)!.change).toBe("deleted");
    }
  });

  test("identical verified input yields the byte-identical canonical plan", async () => {
    const spec = {
      source: [
        { path: "base.txt", kind: "file", content: "BASE" },
        { path: "gone", kind: "directory" },
        { path: "gone/x.txt", kind: "file", content: "X" },
      ],
      worker: async (ws: string) => {
        await writeProjectTree(ws, [
          { path: "new.txt", kind: "file", content: "NEW" },
          { path: "added", kind: "directory" },
          { path: "added/y.txt", kind: "file", content: "Y" },
        ]);
      },
    } as const;
    const first = await buildFinalizedApplyRun("planner-det-a", spec);
    const second = await buildFinalizedApplyRun("planner-det-b", spec);
    const args = (run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>) => ({
      operator: run.observed.operator,
      changes: run.observed.changes,
      baseline: run.observed.baseline,
    });
    const planA = planProjectApplySteps(args(first), first.runId, first.runId, "proj", "b".repeat(64), first.observation, first.state.revision);
    const planB = planProjectApplySteps(args(second), second.runId, second.runId, "proj", "b".repeat(64), second.observation, second.state.revision);
    expect(planB.steps).toEqual(planA.steps);
    expect(planB.payloads).toEqual(planA.payloads);
    expect(planB.noops).toEqual(planA.noops);
    expect(planB.total_payload_byte_count).toBe(planA.total_payload_byte_count);
  });

  test("step indexes are dense and distinct from change indexes", async () => {
    const run = await buildFinalizedApplyRun("planner-indexes", {
      source: [
        { path: "a/m.txt", kind: "file", content: "M" },
        { path: "a/b/x.txt", kind: "file", content: "X" },
      ],
      worker: async (ws) => {
        await rm(join(ws, "a", "m.txt"));
        await rm(join(ws, "a", "b", "x.txt"));
        await rm(join(ws, "a", "b"), { recursive: true });
      },
    });
    const plan = planOf(run);
    expect(plan.steps.map((step) => step.step_index)).toEqual(plan.steps.map((_, index) => index + 1));
    // The depth-first step order disagrees with the lexical change order:
    // the shallower leaf is removed first although it sorts later.
    const disagreements = plan.steps.filter((step) => step.step_index !== step.change_index);
    expect(disagreements.length).toBeGreaterThan(0);
    expect(new Set(plan.steps.map((step) => step.change_index)).size).toBe(plan.steps.length);
  });
});
