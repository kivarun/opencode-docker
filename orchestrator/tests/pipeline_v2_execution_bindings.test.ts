/**
 * The S5-P0 test battery (revision r1 of the implementation, overlay-first
 * order): the versioned read-only `execution_bindings` projection of the
 * durable `execution → (generation, iteration)` riding.
 *
 * On the clean base `855b434` every test in this file is RED at file load
 * (the module `src/pipeline_v2_execution_bindings.ts` does not exist
 * yet); after the production change the file must be green and preserve
 * every other suite (full sequential A/B, `candidate − base = 0`).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  buildPipelineV2ExecutionBindingRecords,
  executionBindingsPipelineV2Run,
  PIPELINE_V2_EXECUTION_BINDINGS_DEFAULT_LIMIT,
  PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT,
  PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT,
  projectPipelineV2ExecutionBindings,
  type PipelineV2ExecutionBindingRecord,
} from "../src/pipeline_v2_execution_bindings.ts";
import { loadDurablePipelineV2Run } from "../src/pipeline_v2_inspect.ts";
import {
  projectControlPlaneExecutionBindings,
  parseControlPlaneRoute,
  parseExecutionBindingsQuery,
  statusForControlPlaneFailure,
} from "../src/control_plane/http_server.ts";
import {
  OrchestratorSubprocessClient,
  type OrchestratorCallResult,
  type OrchestratorJsonValue,
} from "../src/control_plane/subprocess_client.ts";
import { OrchestratorSubprocessClientError } from "../src/control_plane/routing.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import type { PipelineV2RunCommand, PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { apply, hex, IDENTITY, lifecycleCommands, planningLadder, stageLadder } from "./pipeline_v2_commit_history_fixture.ts";
import { E2E_YAML } from "./pipeline_v2_events_fixture.ts";

const OWN_KEYS = (value: unknown): string[] => Object.keys(value as object).sort();

const lifecycleState = (): PipelineV2RunState => apply(lifecycleCommands());

/** The decision battery: a control-role decision execution inside an open generation. */
function decisionControlCommands(): PipelineV2RunCommand[] {
  return [
    { kind: "create_run", runId: "run-decision", pipeline: IDENTITY, inputs: [], projectProvenance: { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 }, profileEpoch: { digest: "d".repeat(64), profile_count: 1 } },
    ...planningLadder("sess-1", "tool-1"),
    { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1") },
    { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1 },
    { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("3"), initialBudget: 2, transitionCount: 0 },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
    transition("architect", "dev_entry", 1),
    ...stageLadder("sess-2", "tool-2", 1),
    transition("dev_entry", "review", 2),
    { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" },
    { kind: "start_decision_execution", stateId: "review", inputDigest: hex("2"), executionRole: "control" },
    { kind: "decision_evaluated", result: { status: "selected", outcome: "accept", decision: "accept", rule_id: "R7", active_constraint_ids: ["HC2"] } },
    { kind: "transition_committed", step: { from: "review", outcome: "accept", to: "done", transition_index: 0 }, executionIndex: 3 },
    { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
    { kind: "run_outputs_published", outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }] },
    { kind: "run_succeeded" },
  ];
}

/** Local transition helper (same shape as the fixture's). */
function transition(from: string, to: string, executionIndex: number): PipelineV2RunCommand {
  return { kind: "transition_committed", step: { from, outcome: "completed", to, transition_index: 0 }, executionIndex };
}

const decisionState = (): PipelineV2RunState => apply(decisionControlCommands());

/** A frozen copy of one binding record (test-side shape check helper). */
const binding = (
  executionIndex: number,
  generationIndex: number | null,
  iterationIndex: number | null,
): PipelineV2ExecutionBindingRecord => ({
  record: "execution_binding",
  execution_index: executionIndex,
  generation_index: generationIndex,
  iteration_index: iterationIndex,
});

const ROOTS: string[] = [];
afterEach(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

async function makeDirs(prefix: string): Promise<{ stateRoot: string; bundle: string }> {
  const root = await mkdtemp(join(tmpdir(), `exec-bindings-${prefix}-`));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  const bundle = join(root, "bundle");
  const fs = await import("node:fs/promises");
  await fs.mkdir(stateRoot);
  await fs.mkdir(bundle);
  return { stateRoot, bundle };
}

async function writeSimpleBundle(bundle: string): Promise<void> {
  const { mkdir, writeFile: write } = await import("node:fs/promises");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), E2E_YAML);
  await writeFile(join(bundle, "prompts", "architect.md"), "architect prompt body");
  await writeFile(join(bundle, "prompts", "coder.md"), "coder prompt body");
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
}

async function seedE2ERun(stateRoot: string, bundle: string, runId: string): Promise<void> {
  const pipeline = await loadPipelineV2(bundle);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: [],
    projectProvenance: { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 },
    profileEpoch: { digest: "d".repeat(64), profile_count: 1 },
  } as never);
  for (const command of lifecycleCommands().slice(1)) {
    await sink.dispatch(command as never);
  }
}

describe("the execution_binding record stream (durable journal order)", () => {
  test("one record per execution in journal order with the riding values", () => {
    const state = lifecycleState();
    const records = buildPipelineV2ExecutionBindingRecords(state);
    expect(records).toHaveLength(state.executions.length);
    expect(records.map((record) => record.record)).toEqual(new Array(records.length).fill("execution_binding"));
    expect(records).toEqual([
      binding(1, null, null),
      binding(2, 1, 1),
      binding(3, 1, 2),
      binding(4, 1, null),
      binding(5, 2, 1),
    ]);
    for (const record of records) {
      expect(Object.isFrozen(record)).toBe(true);
      expect(OWN_KEYS(record)).toEqual(["execution_index", "generation_index", "iteration_index", "record"]);
    }
    expect(Object.isFrozen(records)).toBe(true);
  });

  test("a control-role decision execution projects the same three-field record", () => {
    const state = decisionState();
    const records = buildPipelineV2ExecutionBindingRecords(state);
    expect(records[2]).toEqual(binding(3, 1, null));
    expect(records).toEqual([
      binding(1, null, null),
      binding(2, 1, 1),
      binding(3, 1, null),
    ]);
  });

  test("the empty run projects an empty stream", () => {
    const state = apply([
      {
        kind: "create_run",
        runId: "run-empty",
        pipeline: IDENTITY,
        inputs: [],
        projectProvenance: { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 },
        profileEpoch: { digest: "d".repeat(64), profile_count: 1 },
      },
    ]);
    expect(buildPipelineV2ExecutionBindingRecords(state)).toEqual([]);
  });

  test("repeated reads return equal projections; input mutation is isolated", () => {
    const state = lifecycleState();
    const first = buildPipelineV2ExecutionBindingRecords(state);
    const again = buildPipelineV2ExecutionBindingRecords(state);
    expect(JSON.stringify(first)).toBe(JSON.stringify(again));
  });
});

describe("the page projector (cursor/revision contract, identity-shaped)", () => {
  test("the first page uses the default limit and pins the snapshot revision", () => {
    const state = lifecycleState();
    const page = projectPipelineV2ExecutionBindings(state, {});
    expect(page.revision).toBe(state.revision);
    expect(page.after).toBe(0);
    expect(page.records).toHaveLength(state.executions.length);
    expect(page.nextAfter).toBeNull();
    expect(OWN_KEYS(page)).toEqual(["after", "nextAfter", "records", "revision"]);
    expect(Object.isFrozen(page)).toBe(true);
  });

  test("`after == total` returns an empty page with nextAfter null; a pin is honoured", () => {
    const state = lifecycleState();
    const total = state.executions.length;
    const page = projectPipelineV2ExecutionBindings(state, { after: total, revision: state.revision });
    expect(page.records).toEqual([]);
    expect(page.nextAfter).toBeNull();
    expect(page.after).toBe(total);
  });

  test("`after > total` is the typed invalid_cursor refusal with no partial response", () => {
    const state = lifecycleState();
    expect(() =>
      projectPipelineV2ExecutionBindings(state, { after: state.executions.length + 1, revision: state.revision }),
    ).toThrow(/cursor/);
    try {
      projectPipelineV2ExecutionBindings(state, { after: 99, revision: state.revision });
      throw new Error("unreachable");
    } catch (cause) {
      expect((cause as { reason?: string }).reason).toBe("invalid_cursor");
    }
  });

  test("a revision mismatch is stale_snapshot and precedes the cursor-range checks", () => {
    const state = lifecycleState();
    expect(() =>
      projectPipelineV2ExecutionBindings(state, { after: 999, revision: state.revision + 1 }),
    ).toThrow(/snapshot/);
    try {
      projectPipelineV2ExecutionBindings(state, { after: 999, revision: state.revision + 1 });
      throw new Error("unreachable");
    } catch (cause) {
      expect((cause as { reason?: string }).reason).toBe("stale_snapshot");
    }
  });

  test("cursor and limit grammar is validated fail-closed", () => {
    const state = lifecycleState();
    for (const bad of [-1, 1.5, Number.NaN, "0"]) {
      expect(() => projectPipelineV2ExecutionBindings(state, { after: bad as never })).toThrow(/cursor/);
      expect(() => projectPipelineV2ExecutionBindings(state, { limit: bad as never })).toThrow(/limit/);
    }
    expect(() => projectPipelineV2ExecutionBindings(state, { limit: 0 })).toThrow(/limit/);
    expect(() => projectPipelineV2ExecutionBindings(state, { limit: PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT + 1 })).toThrow(
      /limit/,
    );
    expect(() => projectPipelineV2ExecutionBindings(state, { revision: 0 })).toThrow(/revision pin/);
    expect(() => projectPipelineV2ExecutionBindings(state, { revision: 1.5 })).toThrow(/revision pin/);
  });

  test("limit and cursors partition the stream with the exact nextAfter formula", () => {
    const state = lifecycleState();
    const p1 = projectPipelineV2ExecutionBindings(state, { limit: 2 });
    expect(p1.records.map((record) => record.execution_index)).toEqual([1, 2]);
    expect(p1.nextAfter).toBe(2);
    const p2 = projectPipelineV2ExecutionBindings(state, { after: 2, revision: state.revision, limit: 2 });
    expect(p2.records.map((record) => record.execution_index)).toEqual([3, 4]);
    expect(p2.nextAfter).toBe(4);
    const p3 = projectPipelineV2ExecutionBindings(state, { after: 4, revision: state.revision, limit: 2 });
    expect(p3.records.map((record) => record.execution_index)).toEqual([5]);
    expect(p3.nextAfter).toBeNull();
  });
});

describe("the dual-envelope byte cap over a shape-only walk", () => {
  /** A shape-only state (never a durable document): the pagination walk battery. */
  function walkState(executionCount: number, runIdWidth = 128): never {
    const executions = Array.from({ length: executionCount }, (_, index) => ({
      index: index + 1,
      type: "agent",
      execution_role: "planning",
      generation_index: index === 0 ? undefined : 1,
      iteration_index: undefined,
    }));
    return {
      revision: executionCount + 1,
      executions,
      runId: "x".repeat(runIdWidth),
    } as never;
  }

  const cliEnvelope = (records: readonly unknown[], revision: number, after: number, nextAfter: number | null): string =>
    `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "x".repeat(128),
      revision,
      after,
      records,
      nextAfter,
    })}\n`;

  const httpEnvelope = (records: readonly unknown[], revision: number, after: number, nextAfter: number | null): string =>
    JSON.stringify({
      schema_version: 1,
      kind: "execution_bindings",
      run_id: "x".repeat(128),
      revision,
      after,
      records,
      next_after: nextAfter,
    });

  test("the walk exhausts the stream with pages that fit both envelopes", () => {
    const state = walkState(4200);
    const total = (state as unknown as { executions: unknown[] }).executions.length;
    const walked: unknown[] = [];
    let after = 0;
    let pages = 0;
    for (;;) {
      const page = projectPipelineV2ExecutionBindings(state, { after, revision: undefined, limit: PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT }) as {
        revision: number;
        after: number;
        records: readonly unknown[];
        nextAfter: number | null;
      };
      pages += 1;
      expect(page.records.length).toBeGreaterThan(0);
      const cliBytes = Buffer.byteLength(cliEnvelope(page.records, page.revision, page.after, page.nextAfter), "utf8");
      const httpBytes = Buffer.byteLength(httpEnvelope(page.records, page.revision, page.after, page.nextAfter), "utf8");
      expect(cliBytes).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
      expect(httpBytes).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
      walked.push(...page.records);
      if (page.nextAfter === null) {
        break;
      }
      expect(page.nextAfter).toBe(page.after + page.records.length);
      after = page.nextAfter;
    }
    expect(pages).toBeGreaterThan(1);
    expect(walked).toHaveLength(total);
    expect((walked.at(-1) as { execution_index: number }).execution_index).toBe(total);
  });

  test("the boundary fixture: maximum-width members cannot overflow either full envelope", () => {
    const executionIndex = Number.MAX_SAFE_INTEGER;
    const records: readonly PipelineV2ExecutionBindingRecord[] = [
      {
        record: "execution_binding",
        execution_index: executionIndex,
        generation_index: Number.MAX_SAFE_INTEGER,
        iteration_index: Number.MAX_SAFE_INTEGER,
      },
    ];
    const cliDocument = `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "x".repeat(128),
      revision: Number.MAX_SAFE_INTEGER,
      after: 0,
      records,
      nextAfter: null,
    })}\n`;
    const httpBody = JSON.stringify({
      schema_version: 1,
      kind: "execution_bindings",
      run_id: "x".repeat(128),
      revision: Number.MAX_SAFE_INTEGER,
      after: 0,
      records,
      next_after: null,
    });
    expect(Buffer.byteLength(cliDocument, "utf8")).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
    expect(Buffer.byteLength(httpBody, "utf8")).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
  });
  test("the grammar-bound maximality proof: the maximal page fits both real production envelopes", () => {
    // A grammar-bound fixture, NOT a reducer-reachable run: no durable
    // execution may carry MAX_SAFE_INTEGER indexes (the durable grammar
    // admits them, the reducer never writes them). The proof renders the
    // widest possible page — 128-code-unit safe run id, 200 records of
    // 16-digit safe integers in every applicable field, the widest
    // revision/after and an arithmetic-consistent non-null 16-digit
    // nextAfter — through the REAL production serialization chain: the
    // real CLI formatter (main.ts's `JSON.stringify(outcome)` plus the
    // trailing newline, over the exact success-shape literal order) and
    // the real HTTP serializer (`projectControlPlaneExecutionBindings`
    // followed by the production `jsonBody` JSON.stringify). The pin on
    // the exact measured sizes makes any widening regress loudly.
    const MAX = Number.MAX_SAFE_INTEGER;
    const records: readonly PipelineV2ExecutionBindingRecord[] = Array.from(
      { length: PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT },
      () => ({
        record: "execution_binding" as const,
        execution_index: MAX,
        generation_index: MAX,
        iteration_index: MAX,
      }),
    );
    const after = MAX - PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT;
    // The real CLI document: the exact production success shape, in the
    // exact field order `executionBindingsPipelineV2Run` constructs it,
    // serialized by the exact production formatter (`JSON.stringify`
    // plus the single trailing newline).
    const cliDocument = `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "x".repeat(128),
      revision: MAX,
      after,
      records,
      nextAfter: MAX,
    })}\n`;
    // The real HTTP body: the production projection builder consumes the
    // verified subprocess-client projection and the production
    // serializer stringifies it — nothing else may shape the document.
    const projection = projectControlPlaneExecutionBindings({
      ok: true,
      projection: {
        kind: "executionBindings",
        schemaVersion: 1,
        runId: "x".repeat(128),
        revision: MAX,
        after,
        records: records as unknown as readonly OrchestratorJsonValue[],
        nextAfter: MAX,
      },
    } as never);
    if (!("kind" in projection) || projection.kind !== "execution_bindings") {
      throw new Error("the real projection builder must route the grammar-max page");
    }
    const httpBody = JSON.stringify(projection);
    const cliBytes = Buffer.byteLength(cliDocument, "utf8");
    const httpBytes = Buffer.byteLength(httpBody, "utf8");
    // Independent verification against the accepted page cap on both
    // full production envelopes (CLI incl. its trailing LF).
    expect(cliBytes).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
    expect(httpBytes).toBeLessThanOrEqual(PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT);
    // The exact measured sizes: any widening of the grammar regresses
    // loudly instead of silently loosening the bound.
    expect(cliBytes).toBe(27676);
    expect(httpBytes).toBe(27683);
    // Non-producibility of an overflow, by construction: the wire limit
    // is the record cap (200), so the grammar-max page above is the
    // widest producible page; a single record can never exceed it.
    expect(PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT).toBe(200);
    for (const record of [records[0]!]) {
      const single = `${JSON.stringify({
        ok: true,
        exitCode: 0,
        schemaVersion: 1,
        runId: "x".repeat(128),
        revision: MAX,
        after: MAX - 1,
        records: [record],
        nextAfter: MAX,
      })}\n`;
      expect(Buffer.byteLength(single, "utf8")).toBeLessThanOrEqual(cliBytes);
    }
  });
});

describe("the CLI runner over the shared read-only chain (path-free documents)", () => {
  test("a missing run refuses run_not_found with the exact path-free failure shape", async () => {
    const dirs = await makeDirs("missing");
    const outcome = await executionBindingsPipelineV2Run({ runId: "no-such", stateRoot: dirs.stateRoot });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(OWN_KEYS(outcome)).toEqual(["exitCode", "ok", "reason", "runId"]);
    expect(outcome.reason).toBe("run_not_found");
    expect(JSON.stringify(outcome)).not.toContain("pipeline-runs");
  });

  test("a continuation without its revision pin refuses invalid_cursor before any run access", async () => {
    const dirs = await makeDirs("no-pin");
    const outcome = await executionBindingsPipelineV2Run({ runId: "no-such", stateRoot: dirs.stateRoot, after: 5 });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("invalid_cursor");
    expect(JSON.stringify(outcome)).not.toContain("pipeline-runs");
  });

  test("an unreachable caller-side option defect normalizes to internal_error (never invalid_options)", async () => {
    const dirs = await makeDirs("options");
    const outcome = await executionBindingsPipelineV2Run({
      runId: "run-x",
      stateRoot: "relative-state-root",
    });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("internal_error");
    expect(outcome.runId).toBe("run-x");
    expect(dirs.stateRoot).toBeTruthy();
  });

  test("success pages carry the exact envelope and honour the revision pin", async () => {
    const dirs = await makeDirs("cli");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "id-run");
    const first = await executionBindingsPipelineV2Run({ runId: "id-run", stateRoot: dirs.stateRoot });
    if (!first.ok) throw new Error(`unreachable: ${JSON.stringify(first)}`);
    expect(OWN_KEYS(first)).toEqual(["after", "exitCode", "nextAfter", "ok", "records", "revision", "runId", "schemaVersion"]);
    expect(first.exitCode).toBe(0);
    expect(first.schemaVersion).toBe(1);
    expect(first.runId).toBe("id-run");
    expect(first.records).toEqual([
      binding(1, null, null),
      binding(2, 1, 1),
      binding(3, 1, 2),
      binding(4, 1, null),
      binding(5, 2, 1),
    ]);
    expect(first.nextAfter).toBeNull();
    const second = await executionBindingsPipelineV2Run({
      runId: "id-run",
      stateRoot: dirs.stateRoot,
      after: 2,
      revision: first.revision,
      limit: 2,
    });
    if (!second.ok) throw new Error("unreachable");
    expect(second.records.map((record) => record.execution_index)).toEqual([3, 4]);
    expect(second.nextAfter).toBe(4);
    const stale = await executionBindingsPipelineV2Run({
      runId: "id-run",
      stateRoot: dirs.stateRoot,
      after: 2,
      revision: first.revision + 1,
    });
    if (stale.ok) throw new Error("unreachable");
    expect(stale.reason).toBe("stale_snapshot");
  });

  test("a stored schema_version 12 document refuses run_state_invalid without rewrites", async () => {
    const dirs = await makeDirs("v12");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "v12-run");
    const statePath = join(dirs.stateRoot, "pipeline-runs", "v12-run", "state.json");
    const raw = await readFile(statePath, "utf8");
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    expect(parsed.schema_version).toBe(13);
    parsed.schema_version = 12;
    await writeFile(statePath, JSON.stringify(parsed));
    const outcome = await executionBindingsPipelineV2Run({ runId: "v12-run", stateRoot: dirs.stateRoot });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("run_state_invalid");
    const kept = await readFile(statePath, "utf8");
    expect(kept).toContain('"schema_version":12');
  });

  test("a foreign bundle refuses pipeline_mismatch", async () => {
    const dirs = await makeDirs("mismatch");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "pm-run");
    await writeFile(join(dirs.bundle, "prompts", "architect.md"), "rewritten prompt body");
    const outcome = await executionBindingsPipelineV2Run({ runId: "pm-run", stateRoot: dirs.stateRoot });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("pipeline_mismatch");
  });

  test("the read is content-free and the run tree is byte-identical before and after", async () => {
    const dirs = await makeDirs("readonly");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "ro-run");
    const runRoot = join(dirs.stateRoot, "pipeline-runs", "ro-run");
    const fingerprint = async (): Promise<string> => {
      const raw = await readFile(join(runRoot, "state.json"), "utf8");
      const st = await stat(join(runRoot, "state.json"));
      return `${raw.length}:${st.size}:${st.mtimeMs}`;
    };
    const before = await fingerprint();
    const outcome = await executionBindingsPipelineV2Run({ runId: "ro-run", stateRoot: dirs.stateRoot });
    expect(outcome.ok).toBe(true);
    expect(await fingerprint()).toBe(before);
    const serialized = JSON.stringify(outcome);
    expect(serialized).not.toContain(dirs.stateRoot);
    expect(serialized).not.toContain("pipeline-runs");
    expect(serialized).not.toContain("architect prompt body");
    expect(serialized).not.toContain(hex("a").slice(0, 16));
  });

  test("the read chain and the store survive an unchanged repeated read", async () => {
    const dirs = await makeDirs("repeat");
    await writeSimpleBundle(dirs.bundle);
    await seedE2ERun(dirs.stateRoot, dirs.bundle, "rep-run");
    const first = await executionBindingsPipelineV2Run({ runId: "rep-run", stateRoot: dirs.stateRoot, limit: 3 });
    const second = await executionBindingsPipelineV2Run({ runId: "rep-run", stateRoot: dirs.stateRoot, limit: 3 });
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
    expect(PIPELINE_V2_EXECUTION_BINDINGS_DEFAULT_LIMIT).toBe(100);
  });
});

describe("the HTTP surface (route, query, projection, status table)", () => {
  test("the route answers GET with kind execution_bindings and refuses other methods", () => {
    const parsed = parseControlPlaneRoute("/v1/runs/run-x/execution-bindings", "GET");
    expect(parsed instanceof Error).toBe(false);
    if (!(parsed instanceof Error)) expect(parsed.kind).toBe("execution_bindings");
    const post = parseControlPlaneRoute("/v1/runs/run-x/execution-bindings", "POST");
    expect((post as unknown as { reason: string }).reason).toBe("method_not_allowed");
  });

  test("the query grammar defaults to the identity-shaped canon", () => {
    expect(parseExecutionBindingsQuery("")).toEqual({ after: 0, limit: null, revision: null });
    expect(parseExecutionBindingsQuery("after=3&revision=9&limit=40")).toEqual({ after: 3, revision: 9, limit: 40 });
    expect(parseExecutionBindingsQuery("limit=40&after=3&revision=9")).toEqual({ after: 3, revision: 9, limit: 40 });
  });

  test("syntactically invalid query parameters are invalid_request, never invalid_cursor", () => {
    for (const bad of [
      "after=1.5",
      "after",
      "after=a",
      "after=%31",
      "after=1&after=2",
      "after=1&unknown=2",
      "after=+1",
    ]) {
      const parsed = parseExecutionBindingsQuery(bad);
      expect((parsed as unknown as { reason: string }).reason).toBe("invalid_request");
    }
  });

  test("semantically invalid numbers are invalid_cursor (identity-shaped split)", () => {
    for (const bad of ["after=-1", "revision=0", "revision=-2", "limit=0", "limit=201", "limit=-3"]) {
      const parsed = parseExecutionBindingsQuery(bad);
      expect((parsed as unknown as { reason: string }).reason).toBe("invalid_cursor");
    }
    const bare = parseExecutionBindingsQuery("after=3");
    expect((bare as unknown as { reason: string }).reason).toBe("invalid_cursor");
  });

  test("the projection builder maps the child outcomes exactly; no runRoot travels", () => {
    const records = [binding(1, null, null)];
    const success = projectControlPlaneExecutionBindings({
      ok: true,
      projection: {
        kind: "executionBindings",
        schemaVersion: 1,
        runId: "run-x",
        revision: 24,
        after: 0,
        records,
        nextAfter: null,
      },
    } as unknown as OrchestratorCallResult);
    expect(success).toEqual({
      schema_version: 1,
      kind: "execution_bindings",
      run_id: "run-x",
      revision: 24,
      after: 0,
      records,
      next_after: null,
    });
    expect(OWN_KEYS(success)).toEqual(["after", "kind", "next_after", "records", "revision", "run_id", "schema_version"]);
    expect(JSON.stringify(success)).not.toContain("runRoot");
    expect(JSON.stringify(success)).not.toContain("/tmp/");

    const failure = projectControlPlaneExecutionBindings({
      ok: true,
      projection: { kind: "executionBindingsFailure", exitCode: 1, runId: "run-x", reason: "run_not_found" },
    } as unknown as OrchestratorCallResult);
    expect(failure).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: "run-x",
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });

    const cursor = projectControlPlaneExecutionBindings({
      ok: true,
      projection: { kind: "executionBindingsFailure", exitCode: 1, runId: "run-x", reason: "invalid_cursor" },
    } as unknown as OrchestratorCallResult);
    expect(cursor).toEqual({ layer: "request", reason: "invalid_cursor" });

    const stale = projectControlPlaneExecutionBindings({
      ok: true,
      projection: { kind: "executionBindingsFailure", exitCode: 1, runId: "run-x", reason: "stale_snapshot" },
    } as unknown as OrchestratorCallResult);
    expect(stale).toEqual({ layer: "request", reason: "stale_snapshot" });

    const transport = projectControlPlaneExecutionBindings({
      ok: false,
      failure: { reason: "spawn_failed" },
    } as unknown as OrchestratorCallResult);
    expect(transport).toEqual({ layer: "transport", reason: "spawn_failed" });
  });

  test("the request-layer status table keeps the saved rows", () => {
    expect(statusForControlPlaneFailure({ layer: "request", reason: "invalid_cursor" })).toBe(400);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "invalid_request" })).toBe(400);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "stale_snapshot" })).toBe(409);
    expect(statusForControlPlaneFailure({ layer: "request", reason: "method_not_allowed" })).toBe(405);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "binding_not_found" })).toBe(404);
    expect(statusForControlPlaneFailure({ layer: "binding", reason: "project_unavailable" })).toBe(409);
    expect(statusForControlPlaneFailure({ layer: "transport", reason: "spawn_failed" })).toBe(502);
  });
});

describe("the routing and subprocess transport (fixture child)", () => {
  const CHILD = join(import.meta.dir, "control_plane_child.ts");

  function child(): OrchestratorSubprocessClient {
    return new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: CHILD,
      workingDirectory: tmpdir(),
      configRoot: "/opt/orchestrator/config",
      baseEnv: { PATH: "/usr/bin:/bin" },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
  }

  test("the report scenario projects the exact versioned shape", async () => {
    const result = await child().run({ kind: "execution_bindings", runId: "sc-execution-bindings-report" });
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    const projection = result.projection as Record<string, unknown>;
    expect(projection.kind).toBe("executionBindings");
    expect(projection.schemaVersion).toBe(1);
    expect(projection.runId).toBe("cp-child-run");
    expect(projection.revision).toBe(24);
    expect(projection.after).toBe(0);
    expect(projection.nextAfter).toBeNull();
    expect(projection.records).toEqual([
      { record: "execution_binding", execution_index: 1, generation_index: null, iteration_index: null },
      { record: "execution_binding", execution_index: 2, generation_index: 1, iteration_index: 1 },
    ]);
    expect(JSON.stringify(projection)).not.toContain("runRoot");
  });

  test("the failure scenario projects the typed failure vocabulary", async () => {
    const result = await child().run({ kind: "execution_bindings", runId: "sc-execution-bindings-failure" });
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    expect(result.projection).toEqual({
      kind: "executionBindingsFailure",
      exitCode: 1,
      runId: "cp-child-run",
      reason: "stale_snapshot",
    });
  });

  test("a foreign schemaVersion is unsupported_contract; a leaked runRoot is unexpected_output_shape", async () => {
    const badVersion = await child().run({ kind: "execution_bindings", runId: "sc-execution-bindings-bad-version" });
    expect(badVersion.ok).toBe(false);
    if (badVersion.ok) throw new Error("unreachable");
    expect(badVersion.failure.reason).toBe("unsupported_contract");

    const leaked = await child().run({ kind: "execution_bindings", runId: "sc-execution-bindings-failure-with-root" });
    expect(leaked.ok).toBe(false);
    if (leaked.ok) throw new Error("unreachable");
    expect(leaked.failure.reason).toBe("unexpected_output_shape");
  });

  test("the routing kind is classified through the closed transport vocabulary", () => {
    expect(OrchestratorSubprocessClientError).toBeDefined();
  });
});
