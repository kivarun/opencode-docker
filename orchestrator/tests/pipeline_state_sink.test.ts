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
    protectedInput: PROTECTED_INPUT,
    attempt: { stateId: "execute", attempt: 1, profile: "default" },
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

describe("pipeline run state sink", () => {
  test("a not_committed commit leaves the previous revision authoritative and writable", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 5, failStep: "sync" }), root);
      await sink.initialize();
      await sink.phase("creating_session");
      await sink.phase("session_created", "dhs_child");
      await sink.phase("agent_running");

      const error = await sink
        .recordTransition(STEP, { resultSha256: "c".repeat(64), artifacts: [] })
        .catch((cause: unknown) => cause);
      // typed not_committed outcome: the store wrapped it, the sink did not
      // adopt anything and is not poisoned
      expect(error).toBeInstanceOf(PipelineStateStoreError);
      expect(error).not.toBeInstanceOf(PipelineStateDurabilityError);
      expect(sink.poisoned).toBe(false);
      expect(sink.snapshot?.revision).toBe(4);
      expect(await committedSnapshotRevision(root)).toBe(4);
      expect(await readFile(statePath(root), "utf8")).toContain('"status": "active"');

      // the run can still record the normalized failure durably
      await sink.finalize({
        status: "failed",
        failure: error as Error,
        signal: null,
        sessionId: "dhs_child",
      });
      const finalState = JSON.parse(await readFile(statePath(root), "utf8"));
      expect(finalState.revision).toBe(5);
      expect(finalState.status).toBe("failed");
      expect(finalState.failure).toEqual({ reason: "state_persist_failed" });
    });
  });

  test("a durability_unknown commit adopts the candidate and poisons the sink", async () => {
    await withRoot(async (root) => {
      const sink = makeSink(faultIo({ failCommit: 5, failStep: "dirsync" }), root);
      await sink.initialize();
      await sink.phase("creating_session");
      await sink.phase("session_created", "dhs_child");
      await sink.phase("agent_running");

      const error = await sink
        .recordTransition(STEP, { resultSha256: "c".repeat(64), artifacts: [] })
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineStateDurabilityError);
      const durability = error as PipelineStateDurabilityError;
      expect(durability.revision).toBe(5);
      expect(durability.candidate.status).toBe("active");
      expect(durability.message).toContain("durability could not be confirmed");
      // the sink adopted the visible candidate, not the previous revision
      expect(sink.poisoned).toBe(true);
      expect(sink.snapshot?.revision).toBe(5);
      expect(sink.snapshot?.cursor).toEqual({ current_state: "completed", transition_count: 1 });
      expect(await committedSnapshotRevision(root)).toBe(5);

      // the poisoned sink refuses every further write without touching disk
      const before = await readFile(statePath(root), "utf8");
      await expect(sink.recordTerminal("completed", "success")).rejects.toThrow(/poisoned/);
      await expect(
        sink.finalize({
          status: "failed",
          failure: error as Error,
          signal: null,
          sessionId: "dhs_child",
        }),
      ).rejects.toThrow(/poisoned/);
      await expect(sink.phase("creating_session")).rejects.toThrow(/poisoned/);
      await expect(sink.initialize()).rejects.toThrow(/poisoned/);
      expect(await readFile(statePath(root), "utf8")).toBe(before);
      expect(await committedSnapshotRevision(root)).toBe(5);
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
      await expect(sink.phase("creating_session")).rejects.toThrow(/poisoned/);
    });
  });
});
