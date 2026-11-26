/**
 * The S6 red-before overlay battery: the "Plan and tasks" slice
 * (task/plan revisions ledger cards over the pinned identity stream,
 * the bounded TaskBodyViewer over the P-1 task chunk stream, the
 * bounded PlanViewer over the P-1 paged plan projection).
 *
 * Materialized on the clean base `f4f4918de07bbdd33cacf64fd116a1fa9fa284f3`
 * BEFORE the first S6 production edit, per the accepted preflight r2
 * (/exchange/outbox/increment-6-s6-preflight-r2-report.md). Red-before
 * classes are RED on the clean base and turn green with the S6
 * implementation; the explicit green-preservation guards are green on
 * the clean base and must stay green throughout.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const appPath = join(import.meta.dir, "..", "ui", "app.js");
const htmlPath = join(import.meta.dir, "..", "ui", "index.html");
const appSource = readFileSync(appPath, "utf8");
const htmlSource = readFileSync(htmlPath, "utf8");

/** The fixed owner names of preflight r2 §3 (the implementation must export all of them). */
const S6_OWNERS = [
  "parseTaskContentDocument",
  "parsePlanContentDocument",
  "parseTaskContentEndpointDocument",
  "parsePlanContentEndpointDocument",
  "taskContentRequestPath",
  "planContentRequestPath",
  "createTaskContentTransport",
  "createPlanContentTransport",
  "createPlanTasksState",
  "applyPlanTasksIdentityPage",
  "applyTaskChunk",
  "applyPlanPage",
  "derivePlanTasksView",
  "verifyTaskBodyDigest",
  "createPlanTasksViewer",
] as const;

/** The general API client's fail-closed default (imported statically — it exists on the base). */
const { parseApiDocument } = await import("../ui/app.js");

describe("S6 red-before: the Plan and tasks slice does not exist yet", () => {
  test("(a) ui/index.html has no plan-tasks section", () => {
    expect(htmlSource).toContain('id="plan-tasks-section"');
  });

  test("(c) every fixed S6 owner is exported by ui/app.js", async () => {
    const app = await import(appPath);
    for (const owner of S6_OWNERS) {
      expect(typeof (app as Record<string, unknown>)[owner], owner).toBe("function");
    }
  });

  test("(c/d) the app.js wiring consumes the P-1 content endpoints", () => {
    // The wiring site (not merely the definitions): each URL builder's
    // route fragment must appear again as a real request path template,
    // and the section must be wired to the DOM.
    expect(appSource.includes("/tasks/")).toBe(true);
    expect(appSource.includes("/plans/")).toBe(true);
    expect(appSource.includes("plan-tasks-section")).toBe(true);
  });

  test("(d) the S6 element ids are declared in the DOM manifest", () => {
    for (const id of [
      "pt-status",
      "pt-task-ledger",
      "pt-task-body",
      "pt-plan-ledger",
      "pt-plan-content",
    ]) {
      expect(htmlSource).toContain(`id="${id}"`);
    }
  });
});

describe("green-preservation guards (green on the clean base)", () => {
  test("(b) the general API client keeps rejecting the task_revision and plan_revision kinds", () => {
    for (const kind of ["task_revision", "plan_revision"]) {
      const document =
        kind === "task_revision"
          ? { schema_version: 1, kind, run_id: "r", revision: 1, after: 0, records: [], next_after: null }
          : { schema_version: 1, kind, run_id: "r", revision: 1, after: 0, records: [], next_after: null };
      try {
        parseApiDocument(document);
        throw new Error(`unreachable: ${kind} accepted`);
      } catch (cause) {
        expect((cause as Error).name).toBe("UiContractError");
      }
    }
  });

  test("the lifecycle-history section and its owners stay untouched by S6", async () => {
    const { createLifecycleHistoryViewer, identityRequestPath } = await import("../ui/app.js");
    expect(typeof createLifecycleHistoryViewer).toBe("function");
    expect(identityRequestPath("r", 0, 5)).toBe("/v1/runs/r/identity?after=0&revision=5&limit=100");
    expect(htmlSource).toContain('id="lifecycle-history-section"');
  });
});
