import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
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

const RUN_ID = "run-1";

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return tick(clock - 1);
}

interface SinkCtx {
  sink: PipelineV2RunStateSink;
  root: string;
  statePath: string;
  counts: IoCounts;
  runDir: string;
}

async function withRoot(fn: (root: string) => Promise<void>): Promise<void> {
  const outer = await mkdtemp(join(tmpdir(), "pipeline-v2-state-sink-"));
  const root = join(outer, "state-root");
  await mkdir(root, { recursive: true });
  try {
    await fn(root);
  } finally {
    await rm(outer, { recursive: true, force: true });
  }
}

async function withSink(
  fn: (ctx: SinkCtx) => Promise<void>,
  io: PipelineStateIo = countingIo().io,
): Promise<void> {
  await withRoot(async (root) => {
    clock = 0;
    const counted = countingIo(io);
    const sink = new PipelineV2RunStateSink({
      stateRoot: root,
      runId: RUN_ID,
      io: counted.io,
      now: nextTick,
    });
    await fn({
      sink,
      root,
      statePath: pipelineV2RunStatePath(root, RUN_ID),
      counts: counted.counts,
      runDir: join(root, "pipeline-runs", RUN_ID),
    });
  });
}

/** Dispatches the first `count` happy-path commands without any fault. */
async function playUpTo(sink: PipelineV2RunStateSink, count: number): Promise<void> {
  for (const command of successCommands(RUN_ID).slice(0, count)) {
    await sink.dispatch(command);
  }
}

describe("pipeline v2 run state sink", () => {
  test("dispatches the full happy path and round-trips the durable document", async () => {
    const states = buildStates(RUN_ID);
    await withSink(async ({ sink, statePath }) => {
      expect(sink.snapshot).toBeNull();
      expect(sink.poisoned).toBe(false);
      for (const command of successCommands(RUN_ID)) {
        await sink.dispatch(command);
      }
      const snapshot = sink.snapshot as PipelineV2RunState;
      expect(snapshot.revision).toBe(states.length);
      expect(snapshot.status).toBe("success");
      expect(snapshot).toEqual(states[states.length - 1]!);
      const raw = await readFile(statePath, "utf8");
      expect(raw.endsWith("\n")).toBe(true);
      expect(parsePipelineV2RunState(raw)).toEqual(snapshot);
    });
  });

  test("concurrent dispatches of one run are serialized in command order", async () => {
    const commands = successCommands(RUN_ID);
    await withSink(async ({ sink, statePath }) => {
      await Promise.all(commands.map((command) => sink.dispatch(command)));
      const snapshot = sink.snapshot as PipelineV2RunState;
      expect(snapshot).toEqual(buildStates(RUN_ID)[commands.length - 1]!);
      expect(parsePipelineV2RunState(await readFile(statePath, "utf8"))).toEqual(snapshot);
      expect(snapshot.executions.map((execution) => execution.state_id)).toEqual([
        "implement",
        "check",
        "ship",
      ]);
      expect(snapshot.transitions.map((transition) => transition.to)).toEqual([
        "check",
        "ship",
        "done",
      ]);
    });
  });

  test("a rejected command writes nothing: no temp file, no rename, no snapshot change", async () => {
    await withSink(async ({ sink, statePath, counts, root }) => {
      // no run state yet: the reducer rejects before any filesystem I/O
      await expect(sink.dispatch({ kind: "agent_data_prepared" })).rejects.toThrow(
        "no pipeline v2 run state exists yet",
      );
      expect(counts.tempOpens).toBe(0);
      expect(counts.renames).toBe(0);
      expect(counts.dirSyncs).toBe(0);
      expect(sink.snapshot).toBeNull();
      expect(await readdir(root)).toEqual([]);
      // and mid-run: the create commits, then an incoherent transition changes nothing
      await sink.dispatch(successCommands(RUN_ID)[0]!);
      const before = await readFile(statePath, "utf8");
      expect(counts.tempOpens).toBe(1);
      await expect(
        sink.dispatch({
          kind: "transition_committed",
          step: { from: "implement", outcome: "completed", to: "check", transition_index: 0 },
          executionIndex: 1,
        } as unknown as PipelineV2RunCommand),
      ).rejects.toThrow(PipelineV2StateError);
      expect(counts.tempOpens).toBe(1);
      expect(counts.renames).toBe(1);
      expect(sink.snapshot?.revision).toBe(1);
      expect(await readFile(statePath, "utf8")).toBe(before);
    });
  });

  test("create_run must name the run id the sink owns", async () => {
    await withSink(async ({ sink, counts }) => {
      const command: PipelineV2RunCommand = {
        kind: "create_run",
        runId: "other-run",
        pipeline: V2_IDENTITY,
        inputs: V2_INPUTS,
      };
      await expect(sink.dispatch(command)).rejects.toThrow(
        'create_run names run "other-run", but this sink owns run "run-1"',
      );
      expect(counts.tempOpens).toBe(0);
      expect(counts.renames).toBe(0);
      expect(sink.snapshot).toBeNull();
    });
  });

  test("a not_committed commit keeps the previous snapshot and allows the normalized persist failure", async () => {
    const states = buildStates(RUN_ID);
    await withSink(
      async ({ sink, statePath, counts }) => {
        await playUpTo(sink, 19); // create .. terminal_reached
        // dispatch 20 (run_outputs_published) fails before the rename
        const error = await sink
          .dispatch(successCommands(RUN_ID)[19]!)
          .catch((cause: unknown) => cause);
        expect(error).toBeInstanceOf(PipelineV2RunStateStoreError);
        expect(error).not.toBeInstanceOf(PipelineV2RunStateDurabilityError);
        const snapshot = sink.snapshot as PipelineV2RunState;
        // the previous snapshot remains authoritative, byte-for-byte
        expect(snapshot.revision).toBe(19);
        expect(snapshot.terminal?.state_id).toBe("done");
        expect(sink.poisoned).toBe(false);
        const raw = await readFile(statePath, "utf8");
        expect(parsePipelineV2RunState(raw).revision).toBe(19);
        expect(counts.renames).toBe(19);
        // recovery: record the normalized failure from the unchanged state
        await sink.dispatch({ kind: "run_failed", reason: "state_persist_failed" });
        const recovered = sink.snapshot as PipelineV2RunState;
        expect(recovered.revision).toBe(20);
        expect(recovered.status).toBe("failed");
        expect(recovered.failure?.reason).toBe("state_persist_failed");
        const persisted = parsePipelineV2RunState(await readFile(statePath, "utf8"));
        expect(persisted.status).toBe("failed");
        expect(persisted.failure?.reason).toBe("state_persist_failed");
      },
      faultIo({ failCommit: 20, failStep: "write" }),
    );
  });

  test("a durability_unknown commit adopts the candidate, poisons the sink, and stops all writes", async () => {
    const states = buildStates(RUN_ID);
    const counted = countingIo(faultIo({ failCommit: 20, failStep: "dirsync" }));
    await withSink(
      async ({ sink, statePath, counts, runDir }) => {
        await playUpTo(sink, 19);
        // dispatch 20 (run_outputs_published) fails after the rename
        const error = await sink
          .dispatch(successCommands(RUN_ID)[19]!)
          .catch((cause: unknown) => cause);
        expect(error).toBeInstanceOf(PipelineV2RunStateDurabilityError);
        const durability = error as PipelineV2RunStateDurabilityError;
        expect(durability.message).toContain("durability could not be confirmed");
        expect(durability.revision).toBe(20);
        expect(durability.candidate).toEqual(states[19]!);
        // the visible candidate was adopted and the sink is poisoned
        expect(sink.snapshot).toBe(durability.candidate);
        expect(sink.poisoned).toBe(true);
        expect(parsePipelineV2RunState(await readFile(statePath, "utf8"))).toEqual(states[19]!);
        const committed = { tempOpens: counts.tempOpens, renames: counts.renames, dirSyncs: counts.dirSyncs };
        // every further dispatch is refused before the reducer and the store
        await expect(sink.dispatch({ kind: "run_succeeded" })).rejects.toThrow(/poisoned/);
        await expect(sink.dispatch({ kind: "agent_data_prepared" })).rejects.toThrow(/poisoned/);
        expect({ tempOpens: counts.tempOpens, renames: counts.renames, dirSyncs: counts.dirSyncs }).toEqual(committed);
      },
      counted.io,
    );
  });

  test("the poisoned sink rejects commands that the reducer would reject too, before the reducer", async () => {
    const counted = countingIo(faultIo({ failCommit: 20, failStep: "dirsync" }));
    await withSink(
      async ({ sink, counts }) => {
        await playUpTo(sink, 19);
        const poisonError = await sink
          .dispatch(successCommands(RUN_ID)[19]!)
          .catch((cause: unknown) => cause);
        expect(poisonError).toBeInstanceOf(PipelineV2RunStateDurabilityError);
        const attempts = { tempOpens: counts.tempOpens, renames: counts.renames, dirSyncs: counts.dirSyncs };
        // agent_data_prepared cannot be applied here at all: the poison message proves
        // the sink refused it before the reducer and before any filesystem I/O
        const error = await sink
          .dispatch({ kind: "agent_data_prepared" })
          .catch((cause: unknown) => cause);
        expect((error as Error).message).toContain("poisoned");
        expect((error as Error).message).not.toContain("execution");
        expect({ tempOpens: counts.tempOpens, renames: counts.renames, dirSyncs: counts.dirSyncs }).toEqual(attempts);
      },
      counted.io,
    );
  });
});
