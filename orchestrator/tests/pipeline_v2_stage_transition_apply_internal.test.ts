import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readFile } from "node:fs/promises";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { PipelineV2RunStateDurabilityError, PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { applyStageTransitionCommit } from "../src/pipeline_v2_stage_transition_apply_internal.ts";

const RUN_ID = "kernel-transition";

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 0, 2, 0, 0, clockCounter));
}

interface KernelFailLog {
  readonly errors: Error[];
}

function fail(log: KernelFailLog): (reason: string, message: string, state: PipelineV2RunState | null) => Error {
  return (reason, message, state) => {
    const error = new Error(message) as Error & { reason: string; state: PipelineV2RunState | null };
    error.reason = reason;
    error.state = state;
    log.errors.push(error);
    return error;
  };
}

const WORDING = {
  precheckRejected: "precheck rejected",
  notDurable: "not durable",
  notCommitted: "not committed",
  raceConflict: "race conflict",
  missingTransition: "missing transition",
};

function step(to = "anywhere", transitionIndex = 0): Record<string, unknown> {
  return { from: "planner", outcome: "completed", to, transition_index: transitionIndex };
}

/** A minimal honest pre-state: one settled unbound agent execution. */
async function settledPreState(): Promise<{ sink: PipelineV2RunStateSink; root: string; state: PipelineV2RunState }> {
  const root = await mkdtemp(join(tmpdir(), "kernel-transition-"));
  const sink = new PipelineV2RunStateSink({ stateRoot: root, runId: RUN_ID, now: nextTick });
  const state = sink.snapshot as PipelineV2RunState;
  return { sink, root, state };
}

async function seedSettledExecution(sink: PipelineV2RunStateSink, pipeline: ResolvedPipelineV2): Promise<PipelineV2RunState> {
  await sink.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: [],
  });
  await sink.dispatch({ kind: "start_agent_execution", stateId: "planner", profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: "exec-1" },
    { kind: "agent_tool_session_created", sessionId: "tool-1" },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [] },
    { kind: "agent_cleanup_completed" },
  ] as PipelineV2RunCommand[]) {
    await sink.dispatch(command);
  }
  return sink.snapshot as PipelineV2RunState;
}

let pipeline: ResolvedPipelineV2;

async function anyPipeline(): Promise<ResolvedPipelineV2> {
  if (pipeline !== undefined) {
    return pipeline;
  }
  const { mkdir, writeFile } = await import("node:fs/promises");
  const root = await mkdtemp(join(tmpdir(), "kernel-transition-bundle-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await writeFile(
    join(bundle, "pipeline.yaml"),
    [
      "schema_version: 2",
      "entry_state: planner",
      "max_transitions: 8",
      "inputs: []",
      "outputs: []",
      "states:",
      "  - id: planner",
      "    type: agent",
      "    profile: coder",
      "    prompt: prompts/coder.md",
      "    inputs: []",
      "    outputs: []",
      "    timeout_seconds: 60",
      "    max_attempts: 1",
      "    transitions:",
      "      - outcome: completed",
      "        to: done",
      "  - id: done",
      "    type: terminal",
      "    result: success",
      "",
    ].join("\n"),
  );
  await writeFile(join(bundle, "prompts", "coder.md"), "WORK\n");
  pipeline = await loadPipelineV2(bundle);
  return pipeline;
}

function callKernel(
  sink: unknown,
  overrides: Record<string, unknown> = {},
  wording: unknown = WORDING,
  log: KernelFailLog = { errors: [] },
): Promise<unknown> {
  return applyStageTransitionCommit(
    sink,
    {
      preState: (sink as { snapshot: PipelineV2RunState | null }).snapshot,
      step: step(),
      executionIndex: 1,
      priorTransitionCount: 0,
      ...overrides,
    },
    fail(log),
    wording,
  );
}

describe("applyStageTransitionCommit", () => {
  test("1. the happy path dispatches exactly the transition command and returns the verified post-state", async () => {
    const bundle = await anyPipeline();
    const { sink, root } = await settledPreState();
    try {
      const state = await seedSettledExecution(sink, bundle);
      const log: KernelFailLog = { errors: [] };
      const result = (await callKernel(sink, { preState: state }, WORDING, log)) as { state: PipelineV2RunState };
      expect(log.errors).toEqual([]);
      expect(result.state.cursor).toEqual({ current_state: "anywhere", transition_count: 1 });
      expect(result.state.transitions).toEqual([
        { index: 0, from: "planner", outcome: "completed", to: "anywhere", execution_index: 1 },
      ]);
      expect(result.state.revision).toBe(state.revision + 1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("2. the input validation matrix refuses before any dispatch", async () => {
    const bundle = await anyPipeline();
    const { sink, root } = await settledPreState();
    try {
      const state = await seedSettledExecution(sink, bundle);
      const log: KernelFailLog = { errors: [] };
      const base = { preState: state, step: step(), executionIndex: 1, priorTransitionCount: 0 };
      const cases: Array<[string, Record<string, unknown>]> = [
        ["no preState", { preState: null }],
        ["preState primitive", { preState: 7 }],
        ["step primitive", { step: 7 }],
        ["executionIndex zero", { executionIndex: 0 }],
        ["executionIndex fraction", { executionIndex: 1.5 }],
        ["priorTransitionCount negative", { priorTransitionCount: -1 }],
        ["outcome empty", { step: { ...step(), outcome: "" } }],
        ["from unsafe", { step: { ...step(), from: "../x" } }],
        ["to unsafe", { step: { ...step(), to: "../x" } }],
        ["transition index negative", { step: { ...step("anywhere", -1) } }],
      ];
      for (const [label, overrides] of cases) {
        const before = sink.snapshot?.revision;
        const cause = await callKernel(sink, { ...base, ...overrides }, WORDING, log).catch((error) => error);
        expect(String(cause)).toContain("the transition application request is malformed");
        expect(sink.snapshot?.revision).toBe(before);
        void label;
      }
      // A non-safe-id outcome is a legal step under the transition contract.
      const exotic = await callKernel(sink, { ...base, step: { ...step(), outcome: "has space" } }, WORDING, log).then(
        () => "resolved",
        (error) => String(error),
      );
      expect(exotic).toBe("resolved");
      // Extra request keys are refused.
      const extra = await callKernel(sink, { ...base, extra: 1 }, WORDING, log).catch((error) => error);
      expect(String(extra)).toContain("the transition application request is malformed");
      // A malformed wording is refused.
      const badWording = await callKernel(sink, base, { ...WORDING, raceConflict: "" }, log).catch((error) => error);
      expect(String(badWording)).toContain("the transition application wording is malformed");
      // A malformed sink is refused.
      const badSink = await callKernel({ poisoned: "yes" }, base, WORDING, log).catch((error) => error);
      expect(String(badSink)).toContain("the transition application sink is malformed");
      expect(sink.snapshot?.transitions).toHaveLength(1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("3. the reducer pre-check rejection is the wording's precheck message with zero dispatch", async () => {
    const bundle = await anyPipeline();
    const { sink, root } = await settledPreState();
    try {
      const state = await seedSettledExecution(sink, bundle);
      const log: KernelFailLog = { errors: [] };
      // The settled execution has no accepted outputs recorded; force the
      // rejection through an already-committed transition instead.
      await sink.dispatch({
        kind: "transition_committed",
        step: { from: "planner", outcome: "completed", to: "first", transition_index: 0 },
        executionIndex: 1,
      });
      const after = sink.snapshot as PipelineV2RunState;
      const cause = await callKernel(sink, { preState: after }, WORDING, log).catch((error) => error);
      expect(String(cause)).toBe("Error: precheck rejected");
      expect((cause as { reason?: string }).reason).toBe("invalid_state");
      expect((cause as { state?: unknown }).state).toBe(after);
      expect(after.transitions).toHaveLength(1);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("4. the durability and racing classifications keep the owner's messages", async () => {
    const bundle = await anyPipeline();
    const { sink, root } = await settledPreState();
    try {
      const state = await seedSettledExecution(sink, bundle);
      const log: KernelFailLog = { errors: [] };
      const notCommitted = {
        get snapshot() {
          return sink.snapshot;
        },
        get poisoned() {
          return false;
        },
        async dispatch() {
          throw new PipelineV2RunStateStoreError("refused");
        },
      };
      const cause = await callKernel(notCommitted, { preState: state }, WORDING, log).catch((error) => error);
      expect(String(cause)).toBe("Error: not committed");
      expect((cause as { reason?: string }).reason).toBe("state_persist_failed");
      const notDurable = {
        get snapshot() {
          return sink.snapshot;
        },
        get poisoned() {
          return false;
        },
        async dispatch() {
          throw new PipelineV2RunStateDurabilityError(0, state, "unconfirmed");
        },
      };
      const cause2 = await callKernel(notDurable, { preState: state }, WORDING, log).catch((error) => error);
      expect(String(cause2)).toBe("Error: not durable");
      // Racing identical: the commit lands, then the dispatch throws; the
      // exact verification accepts.
      const raced = {
        get snapshot() {
          return sink.snapshot;
        },
        get poisoned() {
          return false;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await sink.dispatch(command);
          throw new PipelineV2StateError("racing");
        },
      };
      const ok = (await callKernel(raced, { preState: state }, WORDING, log)) as { state: PipelineV2RunState };
      expect(ok.state.cursor).toEqual({ current_state: "anywhere", transition_count: 1 });
      // Racing different: a lying presentation with the matching execution
      // index is the race-conflict message; a missing transition is the
      // missing-transition message.
      const { sink: sink2, root: root2 } = await settledPreState();
      try {
        const state2 = await seedSettledExecution(sink2, bundle);
        let raced2 = false;
        const lying = {
          get snapshot(): PipelineV2RunState | null {
            const snapshot = sink2.snapshot;
            if (snapshot === null || !raced2) {
              return snapshot;
            }
            const clone = structuredClone(snapshot) as PipelineV2RunState;
            (clone.transitions[0] as unknown as Record<string, unknown>)["to"] = "elsewhere";
            return clone;
          },
          get poisoned() {
            return false;
          },
          async dispatch(command: PipelineV2RunCommand) {
            await sink2.dispatch(command);
            raced2 = true;
            throw new PipelineV2StateError("racing");
          },
        };
        const cause3 = await callKernel(lying, { preState: state2 }, WORDING, log).catch((error) => error);
        expect(String(cause3)).toBe("Error: race conflict");
        const { sink: sink3, root: root3 } = await settledPreState();
        try {
          const state3 = await seedSettledExecution(sink3, bundle);
          const silent = {
            get snapshot() {
              return sink3.snapshot;
            },
            get poisoned() {
              return false;
            },
            async dispatch() {
              // Resolves without any durable change.
            },
          };
          const cause4 = await callKernel(silent, { preState: state3 }, WORDING, log).catch((error) => error);
          expect(String(cause4)).toBe("Error: missing transition");
        } finally {
          await rm(root3, { recursive: true, force: true });
        }
      } finally {
        await rm(root2, { recursive: true, force: true });
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("5. the kernel source is the single dispatch site with no policy imports (source scan)", async () => {
    const source = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_stage_transition_apply_internal.ts"), "utf8");
    const countOf = (pattern: string): number => source.split(pattern).length - 1;
    expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
    expect(countOf("dispatchCommand(transitionCommand)")).toBe(1);
    expect(countOf(".message")).toBe(0);
    expect(countOf(".match(")).toBe(0);
    expect(countOf("RegExp(")).toBe(0);
    expect(countOf("JSON.parse")).toBe(0);
    expect(countOf("createHash")).toBe(0);
    expect(countOf("node:fs")).toBe(0);
    expect(countOf("node:path")).toBe(0);
    for (const banned of [
      "pipeline_v2_coordinator",
      "pipeline_v2_runner",
      "main.ts",
      "cli_",
      "docker",
      "launcher",
      "pipeline_v2_run_plan",
      "pipeline_v2_wait",
      "pipeline_v2_revise",
      "pipeline_v2_continue",
      "pipeline_v2_initial_stage",
      "pipeline_v2_replanned",
      "pipeline_v2_stage_iteration",
    ]) {
      expect(source).not.toContain(banned);
    }
    expect(countOf("Object.freeze")).toBe(0);
    expect(countOf("let production")).toBe(0);
  });
});
