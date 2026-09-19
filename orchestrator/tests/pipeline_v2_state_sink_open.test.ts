import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  parsePipelineV2RunState,
  PipelineV2StateError,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
  pipelineV2RunStatePath,
} from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { buildStates, successCommands, tick, V2_IDENTITY, V2_INPUTS } from "./pipeline_v2_state_fixtures.ts";
import { countingIo, faultIo, type IoCounts } from "./state_io_test_helpers.ts";
import type { PipelineStateIo } from "../src/pipeline_state_store.ts";

/**
 * Tests for `PipelineV2RunStateSink.open`: the single way to continue an
 * already durable pipeline v2 run after a process restart. The open loads
 * exclusively through the existing store `load()` and its single state
 * schema v6 validator, refuses a missing state or a foreign run id with a
 * typed store error, performs no filesystem mutation at all, and yields a
 * fully initialized sink whose next dispatch is an ordinary commit from
 * the loaded revision. The fresh constructor keeps its exact observable
 * semantics; reducer, store protocol and durability poisoning are not
 * duplicated.
 */

const RUN_ID = "run-1";

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return tick(clock - 1);
}

interface OpenCtx {
  sink: PipelineV2RunStateSink;
  root: string;
  statePath: string;
  counts: IoCounts;
  runDir: string;
}

async function withRoot(fn: (root: string) => Promise<void>): Promise<void> {
  const outer = await mkdtemp(join(tmpdir(), "pipeline-v2-sink-open-"));
  const root = join(outer, "state-root");
  await mkdir(root, { recursive: true });
  try {
    await fn(root);
  } finally {
    await rm(outer, { recursive: true, force: true });
  }
}

/** Creates a fresh sink and plays the first `count` happy-path commands. */
async function playUpTo(
  root: string,
  count: number,
  io: PipelineStateIo,
): Promise<void> {
  const fresh = new PipelineV2RunStateSink({
    stateRoot: root,
    runId: RUN_ID,
    io,
    now: nextTick,
  });
  for (const command of successCommands(RUN_ID).slice(0, count)) {
    await fresh.dispatch(command);
  }
}

describe("pipeline v2 run state sink: open", () => {
  test("open loads the existing snapshot and the next dispatch is an ordinary commit", async () => {
    const states = buildStates(RUN_ID);
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      await playUpTo(root, 10, counted.io); // create .. start_decision_execution
      const before = await readFile(pipelineV2RunStatePath(root, RUN_ID), "utf8");
      const writesBefore = { tempOpens: counted.counts.tempOpens, renames: counted.counts.renames, dirSyncs: counted.counts.dirSyncs };

      const sink = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      });
      // the exact normalized loaded snapshot, no mutation at open
      const snapshot = sink.snapshot as PipelineV2RunState;
      const loaded = states[9];
      if (loaded === undefined) {
        throw new Error("fixture state 9 is missing");
      }
      expect(snapshot).toEqual(loaded);
      expect(snapshot.revision).toBe(10);
      expect(sink.poisoned).toBe(false);
      expect(sink.statePath).toBe(pipelineV2RunStatePath(root, RUN_ID));
      expect(await readFile(pipelineV2RunStatePath(root, RUN_ID), "utf8")).toBe(before);
      expect({
        tempOpens: counted.counts.tempOpens,
        renames: counted.counts.renames,
        dirSyncs: counted.counts.dirSyncs,
      }).toEqual(writesBefore);

      // the next dispatch commits from the loaded revision: +1, no create
      const next = successCommands(RUN_ID)[10]!;
      await sink.dispatch(next);
      const committed = sink.snapshot as PipelineV2RunState;
      expect(committed.revision).toBe(11);
      expect(committed.executions[1]?.state_id).toBe("check");
      expect(parsePipelineV2RunState(await readFile(pipelineV2RunStatePath(root, RUN_ID), "utf8"))).toEqual(committed);
      expect(counted.counts.tempOpens).toBe(writesBefore.tempOpens + 1);
      expect(counted.counts.renames).toBe(writesBefore.renames + 1);
    });
  });

  test("open round-trips through a second reopen and stays normalized", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      await playUpTo(root, 21, counted.io); // create .. terminal_reached
      const first = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      });
      await first.dispatch(successCommands(RUN_ID)[21]!); // run_outputs_published
      const second = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      });
      expect(second.snapshot).toEqual(first.snapshot);
      expect((second.snapshot as PipelineV2RunState).revision).toBe(22);
      await second.dispatch({ kind: "run_succeeded" });
      expect((second.snapshot as PipelineV2RunState).status).toBe("success");
      expect((second.snapshot as PipelineV2RunState).revision).toBe(23);
    });
  });

  test("open refuses a missing state with a typed error and creates nothing", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      const error = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      }).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2RunStateStoreError);
      expect((error as Error).message).toContain("no durable pipeline v2 run state exists for run");
      expect((error as Error).message).toContain('"run-1"');
      // nothing was created at any level, not even the run directory
      expect(await readdir(root)).toEqual([]);
      expect(counted.counts.tempOpens).toBe(0);
      // the same refusal on a completely absent state root
      const absent = await PipelineV2RunStateSink.open({
        stateRoot: join(root, "no-such-state-root"),
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      }).catch((cause: unknown) => cause);
      expect(absent).toBeInstanceOf(PipelineV2RunStateStoreError);
      expect(await readdir(root)).toEqual([]);
    });
  });

  test("open refuses a document whose run id differs from the sink's run", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      await playUpTo(root, 1, counted.io);
      // the document now lives in a run directory named "run-2" while it
      // still belongs to run "run-1": open must refuse it by document
      const renamedDir = join(root, "pipeline-runs", "run-2");
      const { rename } = await import("node:fs/promises");
      await rename(join(root, "pipeline-runs", RUN_ID), renamedDir);
      const statePath = join(renamedDir, "state.json");
      const before = await readFile(statePath, "utf8");
      const error = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: "run-2",
        io: counted.io,
        now: nextTick,
      }).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2RunStateStoreError);
      expect((error as Error).message).toContain('belongs to run "run-1"');
      expect((error as Error).message).toContain('"run-2"');
      expect(await readFile(statePath, "utf8")).toBe(before);
      expect(counted.counts.tempOpens).toBe(1); // only the fresh play wrote
    });
  });

  test("open propagates the single validator's parse errors and rewrites nothing", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      await playUpTo(root, 1, counted.io);
      const statePath = pipelineV2RunStatePath(root, RUN_ID);
      await writeFile(statePath, "{ this is not json");
      const before = await readFile(statePath, "utf8");
      const error = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      }).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2StateError);
      expect(await readFile(statePath, "utf8")).toBe(before);
      expect(counted.counts.tempOpens).toBe(1);
    });
  });

  test("a resumed sink still poisons exactly like a fresh one on durability unknown", async () => {
    const states = buildStates(RUN_ID);
    const faulted = countingIo(faultIo({ failCommit: 12, failStep: "dirsync" }));
    await withRoot(async (root) => {
      clock = 0;
      await playUpTo(root, 11, faulted.io); // create .. transition_committed (2)
      const sink = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: faulted.io,
        now: nextTick,
      });
      const writesBefore = { tempOpens: faulted.counts.tempOpens, renames: faulted.counts.renames, dirSyncs: faulted.counts.dirSyncs };
      expect(writesBefore).toEqual({ tempOpens: 11, renames: 11, dirSyncs: 11 });
      const error = await sink
        .dispatch(successCommands(RUN_ID)[11]!) // start_agent_execution on ship
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2RunStateDurabilityError);
      const durability = error as PipelineV2RunStateDurabilityError;
      expect(durability.revision).toBe(12);
      const candidate = states[11];
      if (candidate === undefined) {
        throw new Error("fixture state 11 is missing");
      }
      expect(durability.candidate).toEqual(candidate);
      // the visible candidate was adopted and the sink is poisoned
      expect(sink.snapshot).toBe(durability.candidate);
      expect(sink.poisoned).toBe(true);
      // zero further dispatch: refused before the reducer and the store
      await expect(sink.dispatch({ kind: "agent_data_prepared" })).rejects.toThrow(/poisoned/);
      await expect(sink.dispatch({ kind: "run_succeeded" })).rejects.toThrow(/poisoned/);
      // the durability-unknown attempt itself opened the temp file and
      // renamed (the fault hits the post-rename directory fsync)
      expect({
        tempOpens: faulted.counts.tempOpens,
        renames: faulted.counts.renames,
        dirSyncs: faulted.counts.dirSyncs,
      }).toEqual({ tempOpens: 12, renames: 12, dirSyncs: 11 });
    });
  });

  test("an invalid run id is refused before any filesystem I/O, like the fresh constructor", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      const error = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: "../escape",
        io: counted.io,
        now: nextTick,
      }).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2StateError);
      expect(await readdir(root)).toEqual([]);
      expect(counted.counts.tempOpens).toBe(0);
    });
  });

  test("the fresh constructor keeps its exact observable semantics beside open", async () => {
    await withRoot(async (root) => {
      clock = 0;
      const counted = countingIo();
      // a fresh sink over an existing state refuses to create again
      await playUpTo(root, 1, counted.io);
      const fresh = new PipelineV2RunStateSink({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      });
      expect(fresh.snapshot).toBeNull();
      expect(fresh.poisoned).toBe(false);
      await expect(fresh.dispatch({
        kind: "create_run",
        runId: RUN_ID,
        pipeline: V2_IDENTITY,
        inputs: V2_INPUTS,
      } satisfies PipelineV2RunCommand)).rejects.toThrow(PipelineV2RunStateStoreError);
      expect(fresh.snapshot).toBeNull();
      // and open still works over the untouched state
      const reopened = await PipelineV2RunStateSink.open({
        stateRoot: root,
        runId: RUN_ID,
        io: counted.io,
        now: nextTick,
      });
      expect((reopened.snapshot as PipelineV2RunState).revision).toBe(1);
    });
  });
});
