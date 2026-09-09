import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PipelineRunStateSink, type PipelineRunSinkParams } from "../src/pipeline_state_sink.ts";
import {
  PipelineStateDurabilityError,
  PipelineStateStoreError,
  pipelineRunStatePath,
  type PipelineStateIo,
} from "../src/pipeline_state_store.ts";
import type { TransitionStep } from "../src/pipeline_engine.ts";
import { DockerHelperError } from "../src/docker_helper.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const IDENTITY = {
  schema_version: 1,
  bundle_root: "/opt/orchestrator/pipelines/default",
  execution_snapshot_sha256: "a".repeat(64),
  entry_state: "execute",
  max_transitions: 1,
};

const PROTECTED_INPUT = {
  id: "task",
  path: "TASK.md",
  sha256: "b".repeat(64),
};

const STEP: TransitionStep = { from: "execute", outcome: "completed", to: "completed", transition_index: 0 };

let clock = 0;
function tick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock));
}

function makeSink(io: PipelineStateIo, root: string, runId = "sink-run"): PipelineRunStateSink {
  clock = 0;
  return new PipelineRunStateSink({
    stateDirPath: join(root, "state-root"),
    runId,
    workspace: "/work",
    identity: IDENTITY,
    protectedInputs: [PROTECTED_INPUT],
    io,
    now: tick,
  });
}

async function withRoot(fn: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-sink-"));
  try {
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function statePath(root: string, runId = "sink-run"): string {
  return pipelineRunStatePath(join(root, "state-root"), runId);
}

async function committedSnapshotRevision(root: string): Promise<number> {
  const raw = JSON.parse(await readFile(statePath(root), "utf8")) as { revision: number };
  return raw.revision;
}

/** Drives one activation (start, session, agent run, result) up to cleanup. */
async function runActivationToAccepted(sink: PipelineRunStateSink, sessionId = "s1"): Promise<void> {
  await sink.startActivation("execute", "default");
  await sink.activationSessionCreated(sessionId);
  await sink.activationAgentRunning();
  await sink.activationResultAccepted({ resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] });
}

describe("pipeline run state sink", () => {
  test("a not_committed commit leaves the previous revision authoritative and writable", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 7, failStep: "sync" }), root);
      await sink.initialize();
      await runActivationToAccepted(sink);
      await sink.activationCleanupCompleted();

      const error = await sink
        .recordTransition(STEP, { resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] })
        .catch((cause: unknown) => cause);
      // typed not_committed outcome: the store wrapped it, the sink did not
      // adopt anything and is not poisoned
      expect(error).toBeInstanceOf(PipelineStateStoreError);
      expect(error).not.toBeInstanceOf(PipelineStateDurabilityError);
      expect(sink.poisoned).toBe(false);
      expect(sink.snapshot?.revision).toBe(6);
      expect(await committedSnapshotRevision(root)).toBe(6);
      expect(await readFile(statePath(root), "utf8")).toContain('"status": "active"');

      // the run can still record the normalized failure durably
      await sink.finalize({
        status: "failed",
        failure: error as Error,
        signal: null,
        sessionId: "s1",
      });
      const finalState = JSON.parse(await readFile(statePath(root), "utf8"));
      expect(finalState.revision).toBe(7);
      expect(finalState.status).toBe("failed");
      expect(finalState.failure).toEqual({ reason: "state_persist_failed" });
    });
  });

  test("a durability_unknown commit adopts the candidate and poisons the sink", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 7, failStep: "dirsync" }), root);
      await sink.initialize();
      await runActivationToAccepted(sink);
      await sink.activationCleanupCompleted();

      const error = await sink
        .recordTransition(STEP, { resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] })
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineStateDurabilityError);
      const durability = error as PipelineStateDurabilityError;
      expect(durability.revision).toBe(7);
      expect(durability.candidate.status).toBe("active");
      expect(durability.message).toContain("durability could not be confirmed");
      // the sink adopted the visible candidate, not the previous revision
      expect(sink.poisoned).toBe(true);
      expect(sink.snapshot?.revision).toBe(7);
      expect(sink.snapshot?.cursor).toEqual({ current_state: "completed", transition_count: 1 });
      expect(await committedSnapshotRevision(root)).toBe(7);

      // the poisoned sink refuses every further write without touching disk
      const before = await readFile(statePath(root), "utf8");
      await expect(sink.recordTerminal("completed", "success")).rejects.toThrow(/poisoned/);
      await expect(
        sink.finalize({
          status: "failed",
          failure: error as Error,
          signal: null,
          sessionId: "s1",
        }),
      ).rejects.toThrow(/poisoned/);
      await expect(sink.startActivation("execute", "default")).rejects.toThrow(/poisoned/);
      await expect(sink.activationFailed("worker_failed", "completed")).rejects.toThrow(/poisoned/);
      await expect(sink.initialize()).rejects.toThrow(/poisoned/);
      expect(await readFile(statePath(root), "utf8")).toBe(before);
      expect(await committedSnapshotRevision(root)).toBe(7);
    });
  });

  test("a durability_unknown create adopts the first candidate and poisons the sink", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 1, failStep: "dirsync" }), root);
      const error = await sink.initialize().catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineStateDurabilityError);
      expect((error as PipelineStateDurabilityError).revision).toBe(1);
      // the first candidate (run_created, active) is visible and adopted
      expect(sink.poisoned).toBe(true);
      expect(sink.snapshot?.revision).toBe(1);
      expect(sink.snapshot?.status).toBe("active");
      expect(await committedSnapshotRevision(root)).toBe(1);
      await expect(sink.startActivation("execute", "default")).rejects.toThrow(/poisoned/);
    });
  });

  test("the activation lifecycle records the exact ordered journal", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 0 }), root);
      await sink.initialize();
      const index = await sink.startActivation("execute", "default");
      expect(index).toBe(1);
      await sink.activationSessionCreated("s1");
      await sink.activationAgentRunning();
      await sink.activationResultAccepted({ resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] });
      await sink.activationCleanupCompleted();
      await sink.recordTransition(STEP, { resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] });
      await sink.recordTerminal("completed", "success");
      await sink.finalize({ status: "success", failure: null, signal: null, sessionId: "s1" });

      const state = JSON.parse(await readFile(statePath(root), "utf8"));
      expect(state.status).toBe("success");
      expect(state.activations).toHaveLength(1);
      expect(state.activations[0]).toMatchObject({
        index: 1,
        state_id: "execute",
        attempt: 1,
        profile: "default",
        phase: "session_cleanup_completed",
        session_id: "s1",
        session_cleanup: "completed",
      });
      expect(state.transitions[0]).toMatchObject({ activation_index: 1, from: "execute", to: "completed" });
      expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
        "run_created",
        "activation_started",
        "session_created",
        "agent_running",
        "result_accepted",
        "session_cleanup_completed",
        "transition_committed",
        "terminal_reached",
        "run_succeeded",
      ]);
      // no summary text anywhere in the durable document
      expect(await readFile(statePath(root), "utf8")).not.toContain("summary");
    });
  });

  test("an activation failure with its cleanup outcome is recorded and finalizes", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 0 }), root);
      await sink.initialize();
      await sink.startActivation("execute", "default");
      await sink.activationSessionCreated("s1");
      await sink.activationAgentRunning();
      await sink.activationFailed("worker_failed", "completed");
      await sink.finalize({
        status: "failed",
        failure: new DockerHelperError("cli_failure", "agent container failed (exit 1)"),
        signal: null,
        sessionId: "s1",
      });
      const state = JSON.parse(await readFile(statePath(root), "utf8"));
      expect(state.status).toBe("failed");
      expect(state.failure).toEqual({ reason: "worker_failed" });
      expect(state.activations[0]).toMatchObject({
        phase: "failed",
        failure_reason: "worker_failed",
        session_cleanup: "completed",
      });
    });
  });
});
