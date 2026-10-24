/**
 * The versioned read-only lifecycle events API: the stable external
 * projection of the schema-v9 `commit_history`.
 *
 * These tests pin: the single projection owner (cursor semantics, exact
 * entry pass-through, frozen pages, typed invalid_cursor), the read-only
 * CLI runner over the one shared load chain (failure shapes, zero-write
 * fingerprints), the 66-command lifecycle pagination (reconstructed
 * concatenation == complete commit history), the concurrent-append and
 * crash-window matrices, the content-free leakage scan, and the source
 * restrictions of the events layer.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  PIPELINE_V2_COMMIT_PROVENANCE_KINDS,
  parsePipelineV2RunState,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import {
  eventsPipelineV2Run,
  PipelineV2EventsError,
  projectPipelineV2Events,
} from "../src/pipeline_v2_events.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import {
  apply,
  IDENTITY,
  lifecycleCommands,
} from "./pipeline_v2_commit_history_fixture.ts";
import { E2E_YAML } from "./pipeline_v2_events_fixture.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

const ROOTS: string[] = [];
afterEach(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

/** The reducer-built lifecycle state (66 commands → revisions 1..66). */
function lifecycleState(): PipelineV2RunState {
  return validatePipelineV2RunState(apply(lifecycleCommands()));
}

/** The lifecycle commands through and including the Nth occurrence of a kind. */
function upTo(kind: string, occurrence = 1): PipelineV2RunCommand[] {
  const all = lifecycleCommands();
  let seen = 0;
  for (let index = 0; index < all.length; index += 1) {
    const command = all[index];
    if (command !== undefined && command.kind === kind) {
      seen += 1;
      if (seen === occurrence) {
        return all.slice(0, index + 1);
      }
    }
  }
  throw new Error(`lifecycle has no ${kind} #${occurrence}`);
}

describe("events projection owner", () => {
  test("default page: the whole history, nextAfter null when it fits", () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 0, limit: 100 });
    expect(page.revision).toBe(66);
    expect(page.events.length).toBe(66);
    expect(page.nextAfter).toBeNull();
    expect(page.events[0]).toEqual({ revision: 1, kind: "create_run" });
    expect(page.events[65]).toEqual({ revision: 66, kind: "run_succeeded" });
    void 0;
    expect(Object.isFrozen(page)).toBe(true);
    expect(Object.isFrozen(page.events)).toBe(true);
  });

  test("limit slices in history order with the exact next-after cursor", () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 0, limit: 10 });
    expect(page.events.length).toBe(10);
    expect(page.events[9]!.revision).toBe(10);
    expect(page.nextAfter).toBe(10);
    const second = projectPipelineV2Events(state, { after: 10, limit: 10 });
    expect(second.events[0]!.revision).toBe(11);
    expect(second.nextAfter).toBe(20);
    const final = projectPipelineV2Events(state, { after: 60, limit: 10 });
    expect(final.events.length).toBe(6);
    expect(final.nextAfter).toBeNull();
  });

  test("a page exactly full without remainder has nextAfter null", () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 56, limit: 10 });
    expect(page.events.length).toBe(10);
    expect(page.events[9]!.revision).toBe(66);
    expect(page.nextAfter).toBeNull();
  });

  test("after at the final revision succeeds with an empty page", () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 66, limit: 100 });
    expect(page.revision).toBe(66);
    expect(page.events).toEqual([]);
    expect(page.nextAfter).toBeNull();
  });

  test("after beyond the snapshot is the one typed invalid_cursor, never a partial page", () => {
    const state = lifecycleState();
    let error: unknown;
    try {
      projectPipelineV2Events(state, { after: 67, limit: 100 });
    } catch (cause) {
      error = cause;
    }
    expect(error).toBeInstanceOf(PipelineV2EventsError);
    const typed = error as PipelineV2EventsError;
    expect(typed.reason).toBe("invalid_cursor");
    const descriptor = Object.getOwnPropertyDescriptor(typed, "reason");
    expect(descriptor?.writable).toBe(false);
  });

  test("every public event is the durable provenance entry passed through verbatim", () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
    for (let index = 0; index < page.events.length; index += 1) {
      const event = page.events[index]!;
      expect(event).toBe(state.commit_history[index]!);
      expect(PIPELINE_V2_COMMIT_PROVENANCE_KINDS).toContain(event.kind);
    }
    expect(page.events.map((event) => event.revision)).toEqual(
      state.commit_history.map((entry) => entry.revision),
    );
  });

  test("every provenance kind is published verbatim through the pass-through", () => {
    const state = lifecycleState();
    for (const entry of state.commit_history) {
      expect(PIPELINE_V2_COMMIT_PROVENANCE_KINDS).toContain(entry.kind);
    }
    // the full 28-kind vocabulary is published across the reducer-built
    // coverage states: the lifecycle plus the decision and failure suffixes
    const covered = new Set<string>(state.commit_history.map((entry) => entry.kind));
    const decisionState = validatePipelineV2RunState(
      apply([
        { kind: "create_run", runId: "r", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
        { kind: "start_decision_execution", stateId: "architect", inputDigest: "a".repeat(64), executionRole: "planning" },
        { kind: "decision_evaluated", result: { status: "selected", outcome: "s2", decision: "s2", rule_id: "r1", active_constraint_ids: [] } },
      ]),
    );
    for (const entry of decisionState.commit_history) {
      covered.add(entry.kind);
    }
    const decisionFailedState = validatePipelineV2RunState(
      apply([
        { kind: "create_run", runId: "r", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
        { kind: "start_decision_execution", stateId: "architect", inputDigest: "a".repeat(64), executionRole: "planning" },
        { kind: "decision_failed", reason: "decision_input_invalid" },
      ]),
    );
    for (const entry of decisionFailedState.commit_history) {
      covered.add(entry.kind);
    }
    const failedState = validatePipelineV2RunState(
      apply([
        { kind: "create_run", runId: "r", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
        { kind: "start_agent_execution", stateId: "architect", profile: "p", executionRole: "planning" },
        { kind: "agent_data_prepared" },
        { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "not_required", tool: "not_required" }, workerLog: { status: "unavailable" }  },
        { kind: "run_failed", reason: "worker_failed" },
      ]),
    );
    for (const entry of failedState.commit_history) {
      covered.add(entry.kind);
    }
    const cleanupState = validatePipelineV2RunState(
      apply([
        { kind: "create_run", runId: "r", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
        { kind: "start_agent_execution", stateId: "architect", profile: "p", executionRole: "planning" },
        { kind: "agent_data_prepared" },
        { kind: "agent_execution_session_created", sessionId: "s" },
        { kind: "agent_tool_session_created", sessionId: "t" },
        { kind: "agent_running" },
        { kind: "agent_failed", reason: "session_cleanup_failed", sessionCleanup: { execution: "completed", tool: "failed" }, workerLog: { status: "unavailable" }  },
        { kind: "run_cleanup_failed" },
      ]),
    );
    for (const entry of cleanupState.commit_history) {
      covered.add(entry.kind);
    }
    expect(covered.size).toBe(PIPELINE_V2_COMMIT_PROVENANCE_KINDS.length);
    for (const kind of PIPELINE_V2_COMMIT_PROVENANCE_KINDS) {
      expect(covered.has(kind)).toBe(true);
    }
  });

  test("hostile pagination scalars never reach the cursor semantics", () => {
    const state = lifecycleState();
    for (const after of [-1, 0.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => projectPipelineV2Events(state, { after, limit: 10 })).toThrow(TypeError);
    }
    for (const limit of [0, -1, 0.5, Number.NaN]) {
      expect(() => projectPipelineV2Events(state, { after: 0, limit })).toThrow(TypeError);
    }
  });

  test("the projection never mutates the state and is deterministic", () => {
    const state = lifecycleState();
    const before = JSON.stringify(state);
    const first = projectPipelineV2Events(state, { after: 5, limit: 7 });
    const second = projectPipelineV2Events(state, { after: 5, limit: 7 });
    expect(JSON.stringify(state)).toBe(before);
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
  });
});

/** Minimal honest CLI harness: a real bundle, a real sink, real durable state. */
async function makeDirs(prefix: string): Promise<{ root: string; bundle: string; stateRoot: string }> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  return { root, bundle, stateRoot };
}

const SIMPLE_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20
inputs: []
outputs: []
states:
  - id: coder
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
        to: done
  - id: done
    type: terminal
    result: success
`;

async function writeSimpleBundle(bundle: string): Promise<void> {
  await writeFile(join(bundle, "pipeline.yaml"), SIMPLE_YAML);
  await writeFile(join(bundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
}

async function writeStateJson(stateRoot: string, runId: string, text: string): Promise<void> {
  await writeFile(join(stateRoot, "pipeline-runs", runId, "state.json"), text);
}

function ladderCommands(): PipelineV2RunCommand[] {
  return [
    { kind: "start_agent_execution", stateId: "coder", profile: "coder", executionRole: "planning" },
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "sess-events-1" },
    { kind: "agent_tool_session_created", sessionId: "tool-events-1" },
    { kind: "agent_running" },
  ];
}

async function seedRun(
  stateRoot: string,
  bundle: string,
  runId: string,
  commands: readonly PipelineV2RunCommand[] = ladderCommands(),
): Promise<PipelineV2RunStateSink> {
  const pipeline = await loadPipelineV2(bundle);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId });
  await sink.dispatch({ kind: "create_run", runId, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING });
  for (const command of commands) {
    await sink.dispatch(command as never);
  }
  return sink;
}

describe("events CLI runner (read-only chain)", () => {
  test("success document: exact keys, one snapshot revision, events from the history", async () => {
    const dirs = await makeDirs("events-cli");
    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-run");
    const outcome = await eventsPipelineV2Run({ runId: "ev-run", stateRoot: dirs.stateRoot });
    expect(Object.keys(outcome).sort()).toEqual(
      outcome.ok
        ? ["events", "exitCode", "nextAfter", "ok", "revision", "runId", "runRoot", "schemaVersion"]
        : [],
    );
    if (!outcome.ok) throw new Error("unreachable");
    expect(outcome.schemaVersion).toBe(1);
    expect(outcome.exitCode).toBe(0);
    expect(outcome.runId).toBe("ev-run");
    expect(outcome.runRoot).toBe(`${dirs.stateRoot}/pipeline-runs/ev-run`);
    expect(outcome.revision).toBe(6);
    expect(outcome.events.length).toBe(6);
    expect(outcome.nextAfter).toBeNull();
    expect(outcome.events[0]).toEqual({ revision: 1, kind: "create_run" });
  });

  test("pagination through the CLI runner: after and limit honor the cursor contract", async () => {
    const dirs = await makeDirs("events-cli-page");
    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-page");
    const first = await eventsPipelineV2Run({ runId: "ev-page", stateRoot: dirs.stateRoot, limit: 2 });
    if (!first.ok) throw new Error("unreachable");
    expect(first.events.map((event) => event.revision)).toEqual([1, 2]);
    expect(first.nextAfter).toBe(2);
    const second = await eventsPipelineV2Run({ runId: "ev-page", stateRoot: dirs.stateRoot, after: 2, limit: 2 });
    if (!second.ok) throw new Error("unreachable");
    expect(second.events.map((event) => event.revision)).toEqual([3, 4]);
    expect(second.nextAfter).toBe(4);
    const tail = await eventsPipelineV2Run({ runId: "ev-page", stateRoot: dirs.stateRoot, after: 6, limit: 2 });
    if (!tail.ok) throw new Error("unreachable");
    expect(tail.events).toEqual([]);
    expect(tail.nextAfter).toBeNull();
  });

  test("invalid_cursor through the CLI runner", async () => {
    const dirs = await makeDirs("events-cli-cursor");
    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-cursor", []);
    const outcome = await eventsPipelineV2Run({ runId: "ev-cursor", stateRoot: dirs.stateRoot, after: 5 });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("invalid_cursor");
    expect(outcome.exitCode).toBe(1);
    expect(outcome.runRoot).toBe(`${dirs.stateRoot}/pipeline-runs/ev-cursor`);
    expect(outcome.runId).toBe("ev-cursor");
  });

  test("failure shapes: run_not_found, invalid_options, run_state_invalid", async () => {
    const dirs = await makeDirs("events-cli-fail");
    const missing = await eventsPipelineV2Run({ runId: "nope-run", stateRoot: dirs.stateRoot });
    if (missing.ok) throw new Error("unreachable");
    expect(missing.reason).toBe("run_not_found");
    expect(missing.runRoot).toBeNull();
    expect(missing.runId).toBe("nope-run");

    const badId = await eventsPipelineV2Run({ runId: "../escape", stateRoot: dirs.stateRoot });
    if (badId.ok) throw new Error("unreachable");
    expect(badId.reason).toBe("invalid_options");
    expect(badId.runId).toBe("");

    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-damage", []);
    await writeStateJson(dirs.stateRoot, "ev-damage", "{not json");
    const damaged = await eventsPipelineV2Run({ runId: "ev-damage", stateRoot: dirs.stateRoot });
    if (damaged.ok) throw new Error("unreachable");
    expect(damaged.reason).toBe("run_state_invalid");
    expect(damaged.runRoot).toBe(`${dirs.stateRoot}/pipeline-runs/ev-damage`);
  });

  test("pipeline_mismatch: a changed bundle after the run is a typed failure", async () => {
    const dirs = await makeDirs("events-cli-bundle");
    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-bundle", []);
    await writeFile(
      join(dirs.bundle, "pipeline.yaml"),
      SIMPLE_YAML.replace("max_transitions: 20", "max_transitions: 21"),
    );
    const outcome = await eventsPipelineV2Run({ runId: "ev-bundle", stateRoot: dirs.stateRoot });
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("pipeline_mismatch");
  });

  test("zero-write guarantee: the run tree fingerprint is identical across a read", async () => {
    const dirs = await makeDirs("events-cli-fingerprint");
    await writeSimpleBundle(dirs.bundle);
    await seedRun(dirs.stateRoot, dirs.bundle, "ev-fp");
    const fingerprint = async (): Promise<string> => {
      const entries: string[] = [];
      const walk = async (dir: string): Promise<void> => {
        for (const name of (await readdir(dir)).sort()) {
          const path = join(dir, name);
          const info = await stat(path);
          if (info.isDirectory()) {
            entries.push(`d ${path}`);
            await walk(path);
          } else {
            entries.push(`f ${path} ${info.mode.toString(8)} ${await readFile(path, "utf8")}`);
          }
        }
      };
      await walk(join(dirs.stateRoot, "pipeline-runs", "ev-fp"));
      return entries.join("\n");
    };
    const before = await fingerprint();
    const first = await eventsPipelineV2Run({ runId: "ev-fp", stateRoot: dirs.stateRoot, limit: 3 });
    const second = await eventsPipelineV2Run({ runId: "ev-fp", stateRoot: dirs.stateRoot, limit: 3 });
    const after = await fingerprint();
    expect(after).toBe(before);
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
  });
});

describe("events across crash windows and concurrent appends", () => {
  test("a durability-unknown adopted candidate is read exactly once through a fresh reopen", async () => {
    const dirs = await makeDirs("events-durability");
    await writeSimpleBundle(dirs.bundle);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const sink = new PipelineV2RunStateSink({
      stateRoot: dirs.stateRoot,
      runId: "ev-dur",
      now: () => new Date(Date.UTC(2026, 0, 1)),
      io: faultIo({ failCommit: 1, failStep: "dirsync" }),
    });
    let durabilityUnknown = false;
    try {
      await sink.dispatch({ kind: "create_run", runId: "ev-dur", pipeline: identity, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING });
    } catch {
      durabilityUnknown = true;
    }
    expect(durabilityUnknown).toBe(true);
    expect(sink.poisoned).toBe(true);
    const outcome = await eventsPipelineV2Run({ runId: "ev-dur", stateRoot: dirs.stateRoot });
    if (!outcome.ok) throw new Error("unreachable");
    expect(outcome.revision).toBe(1);
    expect(outcome.events.length).toBe(1);
    expect(outcome.events[0]).toEqual({ revision: 1, kind: "create_run" });
  });

  test("concurrent append: page two after a mutation has no duplicate and no gap", async () => {
    const dirs = await makeDirs("events-concurrent");
    await writeSimpleBundle(dirs.bundle);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId: "ev-conc" });
    await sink.dispatch({ kind: "create_run", runId: "ev-conc", pipeline: identity, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING });
    await sink.dispatch(ladderCommands()[0] as never);
    await sink.dispatch(ladderCommands()[1] as never);
    const page1 = await eventsPipelineV2Run({ runId: "ev-conc", stateRoot: dirs.stateRoot, limit: 3 });
    if (!page1.ok) throw new Error("unreachable");
    expect(page1.revision).toBe(3);
    expect(page1.nextAfter).toBeNull();
    for (const command of ladderCommands().slice(2)) {
      await sink.dispatch(command as never);
    }
    const tail = await eventsPipelineV2Run({ runId: "ev-conc", stateRoot: dirs.stateRoot, after: 3, limit: 100 });
    if (!tail.ok) throw new Error("unreachable");
    expect(tail.events.map((event) => event.revision)).toEqual([4, 5, 6]);
    const full = await eventsPipelineV2Run({ runId: "ev-conc", stateRoot: dirs.stateRoot, limit: 200 });
    if (!full.ok) throw new Error("unreachable");
    expect(full.events.map((event) => event.revision)).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test("every durable window ends its feed at the exact current revision", () => {
    const windows: Array<[string, PipelineV2RunCommand[]]> = [
      ["create_run", upTo("create_run")],
      ["in_flight", upTo("agent_execution_session_created")],
      ["settled_unbound", upTo("agent_cleanup_completed")],
      ["committed_transition", upTo("transition_committed")],
      ["accepted_plan", upTo("plan_revision_accepted")],
      ["generation_opened", upTo("stage_generation_opened")],
      ["waiting", upTo("run_waiting")],
      ["terminal", upTo("terminal_reached")],
      ["outputs_published", upTo("run_outputs_published")],
      ["succeeded", upTo("run_succeeded")],
    ];
    for (const [name, commands] of windows) {
      const state = validatePipelineV2RunState(apply(commands));
      const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
      expect(page.revision, name).toBe(state.revision);
      expect(page.events.length, name).toBe(state.revision);
      expect(page.nextAfter, name).toBeNull();
      const tail = projectPipelineV2Events(state, { after: state.revision - 1, limit: 1 });
      expect(tail.events.length, name).toBe(1);
      expect(tail.events[0]!.revision, name).toBe(state.revision);
    }
  });

  test("a failed run finalization carries its exact failure suffix", () => {
    const base = upTo("agent_running");
    const state = validatePipelineV2RunState(
      apply([
        ...base,
        { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" }, workerLog: { status: "unavailable" }  },
        { kind: "run_failed", reason: "worker_failed" },
      ]),
    );
    const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
    expect(page.events.at(-1)).toEqual({ revision: state.revision, kind: "run_failed" });
    expect(page.nextAfter).toBeNull();
  });
});

describe("events leakage matrix", () => {
  test("no canary from any forbidden category enters the events page", () => {
    const HOST_PATH = "/tmp/CANARY-HOST-PATH/bundle";
    const SESSION = "sess-CANARY-SESSION-ID";
    const DIGEST = "d".repeat(63) + "1";
    const commands: PipelineV2RunCommand[] = [
      { kind: "create_run", runId: "canary-run", pipeline: { ...IDENTITY, bundle_root: HOST_PATH }, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
      { kind: "start_agent_execution", stateId: "architect", profile: "CANARY-PROFILE", executionRole: "planning" },
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: SESSION },
      { kind: "agent_tool_session_created", sessionId: "tool-CANARY" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: DIGEST }], workerLog: { status: "unavailable" }  },
      { kind: "agent_failed", reason: "worker_failed", sessionCleanup: { execution: "completed", tool: "completed" }, workerLog: { status: "unavailable" }  },
      { kind: "run_failed", reason: "worker_failed" },
    ];
    const state = validatePipelineV2RunState(apply(commands));
    const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
    const text = JSON.stringify(page.events);
    expect(text).not.toContain("CANARY-HOST-PATH");
    expect(text).not.toContain("CANARY-SESSION-ID");
    expect(text).not.toContain("CANARY-PROFILE");
    expect(text).not.toContain("tool-CANARY");
    expect(text).not.toContain(DIGEST);
    expect(text).not.toContain("canary-run");
    for (const event of page.events) {
      expect(PIPELINE_V2_COMMIT_PROVENANCE_KINDS).toContain(event.kind);
    }
  });
});

describe("events source restrictions", () => {
  test("the events module imports only the freeze helper, the shared read chain and the state types", async () => {
    const source = await readFile(new URL("../src/pipeline_v2_events.ts", import.meta.url), "utf8");
    const imports = source.match(/from "\.\/[^"]+"/g) ?? [];
    expect(imports.sort()).toEqual([
      'from "./pipeline_v2_freeze_internal.ts"',
      'from "./pipeline_v2_inspect.ts"',
      'from "./pipeline_v2_state.ts"',
    ]);
    expect(source).not.toContain("start_agent_execution");
    expect(source).not.toContain("reducePipelineV2RunCommand");
    expect(source).not.toContain("PipelineV2RunStateStore");
    expect(source).not.toContain("readdir");
    expect(source).not.toContain("new Date");
    expect(source).not.toContain("console.");
  });

  test("exactly one projection owner and one typed cursor owner", async () => {
    const mod = await import("../src/pipeline_v2_events.ts");
    expect(Object.keys(mod).sort()).toEqual([
      "PipelineV2EventsError",
      "eventsPipelineV2Run",
      "projectPipelineV2Events",
    ]);
  });

  test("inspect/HTTP/UI never expose the internal history through a second path", async () => {
    const inspect = await readFile(new URL("../src/pipeline_v2_inspect.ts", import.meta.url), "utf8");
    expect(inspect).not.toContain("commit_history");
    const http = await readFile(new URL("../src/control_plane/http_server.ts", import.meta.url), "utf8");
    expect(http).not.toContain("commit_history");
    const ui = await readFile(new URL("../ui/app.js", import.meta.url), "utf8");
    // the browser UI consumes only the versioned HTTP events projection
    // through its dedicated events client; the internal history name and
    // every raw state document stay outside the UI module
    expect(ui.includes("commit_history")).toBe(false);
    expect(ui.includes("state.json")).toBe(false);
  });

  test("the round-trip through the real loader preserves the projected page byte-for-byte", async () => {
    const state = lifecycleState();
    const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
    const canonical = JSON.stringify(state);
    const reparsed = parsePipelineV2RunState(canonical);
    expect(reparsed).not.toBeNull();
    const reparsedPage = projectPipelineV2Events(reparsed as PipelineV2RunState, { after: 0, limit: 200 });
    expect(JSON.stringify(reparsedPage)).toBe(JSON.stringify(page));
  });
});

describe("events end to end through the real CLI subprocess", () => {
  test(
    "the 66-event lifecycle paginates through the subprocess wire with no duplicate and no gap",
    async () => {
      const dirs = await makeDirs("events-e2e");
      await writeFile(join(dirs.bundle, "pipeline.yaml"), E2E_YAML);
      await writeFile(join(dirs.bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
      await writeFile(join(dirs.bundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
      await mkdir(join(dirs.bundle, "schemas"), { recursive: true });
      await writeFile(join(dirs.bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
      const pipeline = await loadPipelineV2(dirs.bundle);
      const identity = pipelineV2RunPipelineIdentity(pipeline);
      const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId: "ev-e2e" });
      for (const command of lifecycleCommands()) {
        if (command.kind === "create_run") {
          await sink.dispatch({ ...command, runId: "ev-e2e", pipeline: identity } as never);
          continue;
        }
        await sink.dispatch(command as never);
      }
      expect(sink.snapshot?.revision).toBe(66);
      const client = new (await import("../src/control_plane/subprocess_client.ts")).OrchestratorSubprocessClient({
        bunExecutable: process.execPath,
        orchestratorEntrypoint: new URL("../src/main.ts", import.meta.url).pathname,
        workingDirectory: dirs.root,
        configRoot: dirs.root,
        baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: dirs.stateRoot },
        stdoutLimitBytes: 1_000_000,
        stderrLimitBytes: 1_000_000,
        sigintGraceMs: 2000,
      });
      const pages: Array<{ revision: number; events: Array<{ revision: number; kind: string }>; nextAfter: number | null }> = [];
      let cursor = 0;
      for (;;) {
        const command = cursor === 0 ? { kind: "events" as const, runId: "ev-e2e", limit: 20 } : { kind: "events" as const, runId: "ev-e2e", after: cursor, limit: 20 };
        const result = await client.run(command);
        if (!result.ok) throw new Error(`transport failure: ${result.failure.reason}`);
        if (result.projection.kind !== "events") throw new Error("unreachable");
        pages.push({ revision: result.projection.revision, events: result.projection.events as never, nextAfter: result.projection.nextAfter });
        if (result.projection.nextAfter === null) break;
        cursor = result.projection.nextAfter;
      }
      expect(pages.length).toBe(4);
      const revisions = pages.flatMap((page) => page.events.map((event) => event.revision));
      expect(revisions).toEqual(Array.from({ length: 66 }, (_, index) => index + 1));
      for (const page of pages) {
        expect(page.revision).toBe(66);
      }
      // the reconstructed concatenation equals the complete durable history
      const durable = parsePipelineV2RunState(
        await readFile(join(dirs.stateRoot, "pipeline-runs", "ev-e2e", "state.json"), "utf8"),
      );
      expect(durable).not.toBeNull();
      expect(JSON.stringify(pages.flatMap((page) => page.events))).toBe(
        JSON.stringify((durable as PipelineV2RunState).commit_history),
      );
      // a repeated read over the unchanged run is byte-identical
      const again = await client.run({ kind: "events", runId: "ev-e2e", after: 60, limit: 20 });
      if (!again.ok || again.projection.kind !== "events") throw new Error("unreachable");
      expect(JSON.stringify(again.projection.events)).toBe(
        JSON.stringify(pages[3]!.events),
      );
      // a cursor beyond the revision is the typed invalid_cursor failure
      const beyond = await client.run({ kind: "events", runId: "ev-e2e", after: 999 });
      if (!beyond.ok) throw new Error("unreachable");
      expect(beyond.projection.kind).toBe("eventsFailure");
      if (beyond.projection.kind !== "eventsFailure") throw new Error("unreachable");
      expect(beyond.projection.reason).toBe("invalid_cursor");
    },
    30000,
  );
});

describe("worker-log bindings stay invisible to the events projection", () => {
  test("a captured binding adds no event and leaks no metadata into the pages", () => {
    const commands = lifecycleCommands().map((command) => {
      if (command.kind === "agent_outputs_accepted") {
        return {
          ...command,
          workerLog: {
            status: "captured" as const,
            sha256: "b".repeat(64),
            byte_count: 10,
            truncated_daemon: false,
            truncated_local: false,
          },
        };
      }
      return command;
    });
    const state = validatePipelineV2RunState(apply(commands));
    const page = projectPipelineV2Events(state, { after: 0, limit: 200 });
    // the commit-history vocabulary is unchanged: no binding event exists
    const kinds = new Set<string>(page.events.map((event) => event.kind));
    expect(kinds.has("worker_log_recorded")).toBe(false);
    for (const event of page.events) {
      expect(Object.keys(event).filter((key) => key.startsWith("worker"))).toEqual([]);
    }
    expect(JSON.stringify(page)).not.toContain("worker_log");
    expect(JSON.stringify(page)).not.toContain("workerlog");
    expect(JSON.stringify(page)).not.toContain("b".repeat(64));
    expect(page.revision).toBe(state.revision);
    expect(page.nextAfter).toBeNull();
  });
});
