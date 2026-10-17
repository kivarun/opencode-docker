import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parsePipelineV2RunState, PipelineV2StateError } from "../src/pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStore,
  PipelineV2RunStateStoreError,
  pipelineV2RunStatePath,
} from "../src/pipeline_v2_state_store.ts";
import { buildStates } from "./pipeline_v2_state_fixtures.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "store-run";

async function withStore(
  runId: string,
  fn: (ctx: { root: string; store: PipelineV2RunStateStore; statePath: string }) => Promise<void>,
): Promise<void> {
  const outer = await mkdtemp(join(tmpdir(), "pipeline-v2-state-store-"));
  const root = join(outer, "state-root");
  await mkdir(root, { recursive: true });
  try {
    await fn({
      root,
      statePath: pipelineV2RunStatePath(root, runId),
      store: new PipelineV2RunStateStore({ stateRoot: root, runId }),
    });
  } finally {
    await rm(outer, { recursive: true, force: true });
  }
}

async function listDir(path: string): Promise<string[]> {
  return (await readdir(path)).sort();
}

describe("pipeline v2 run state store", () => {
  test("create, commit, and load round-trip the full schema v5 document", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ store, statePath }) => {
      expect(await store.load()).toBeNull();
      await store.create(states[0]!);
      for (let i = 1; i < states.length; i++) {
        await store.commit(states[i]!, states[i - 1]!.revision);
        const loaded = await store.load();
        expect(loaded?.revision).toBe(states[i]!.revision);
        expect(loaded).toEqual(states[i]!);
      }
      const raw = await readFile(statePath, "utf8");
      expect(raw.endsWith("\n")).toBe(true);
      expect(parsePipelineV2RunState(raw)).toEqual(states[states.length - 1]!);
      expect(states[states.length - 1]!.status).toBe("success");
    });
  });

  test("legacy schema versions and malformed documents are rejected without touching the file", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store, statePath }) => {
      await mkdir(join(root, "pipeline-runs", "store-run"), { recursive: true });
      const cases: [string, string][] = [
        [
          JSON.stringify({ schema_version: 1, revision: 1, run_id: "x", status: "active", phase: "validating" }),
          "schema_version 1",
        ],
        [
          JSON.stringify({ schema_version: 3, revision: 1, run_id: "x", status: "active", phase: "running" }),
          "schema_version 3",
        ],
        [
          JSON.stringify({ schema_version: 4, revision: 1, run_id: "x", status: "active", phase: "running" }),
          "schema_version 4",
        ],
        ["{ not json", "is not valid JSON"],
        ['{"schema_version":3,"revision":1', "is not valid JSON"],
      ];
      for (const [body, messagePart] of cases) {
        await writeFile(statePath, `${body}\n`);
        const before = await readFile(statePath, "utf8");
        await expect(store.load()).rejects.toThrow(messagePart);
        expect(await readFile(statePath, "utf8")).toBe(before);
      }
      const unknown = { ...JSON.parse(JSON.stringify(states[0])), surprise: true };
      await writeFile(statePath, JSON.stringify(unknown));
      await expect(store.load()).rejects.toThrow(/unknown field "surprise"/);
      expect(await readFile(statePath, "utf8")).toBe(JSON.stringify(unknown));
      // a leftover temp file is not state: loading ignores it
      await rm(statePath);
      await writeFile(join(root, "pipeline-runs", "store-run", "state.json.tmp-leftover"), "garbage");
      expect(await store.load()).toBeNull();
    });
  });

  test("create refuses to overwrite an existing run", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ store, statePath }) => {
      await store.create(states[0]!);
      const before = await readFile(statePath, "utf8");
      await expect(store.create(states[0]!)).rejects.toThrow(
        /refusing to overwrite an existing pipeline v2 run state/,
      );
      await expect(store.create(states[1]!)).rejects.toThrow(/refusing to overwrite/);
      expect(await readFile(statePath, "utf8")).toBe(before);
      expect((await store.load())?.revision).toBe(1);
    });
  });

  test("a revision mismatch or gap is rejected fail-closed", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ store }) => {
      await store.create(states[0]!);
      await expect(store.commit(states[2]!, 1)).rejects.toThrow(/must be exactly 2/);
      await expect(store.commit(states[1]!, 2)).rejects.toThrow(/committed snapshot has revision 1, expected 2/);
      expect((await store.load())?.revision).toBe(1);
    });
  });

  test("private directories and the state file have restrictive permissions", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store, statePath }) => {
      await store.create(states[0]!);
      const fileMode = (await stat(statePath)).mode & 0o777;
      expect(fileMode).toBe(0o600);
      const runDirMode = (await stat(join(root, "pipeline-runs", "store-run"))).mode & 0o777;
      expect(runDirMode).toBe(0o700);
      const runsDirMode = (await stat(join(root, "pipeline-runs"))).mode & 0o777;
      expect(runsDirMode).toBe(0o700);
    });
  });

  test("a failure before the rename keeps the previous snapshot byte-for-byte intact", async () => {
    const states = buildStates(RUN_ID);
    for (const failStep of ["open", "write", "sync", "close", "rename"] as const) {
      await withStore("store-run", async ({ root, store, statePath }) => {
        await store.create(states[0]!);
        const before = await readFile(statePath, "utf8");
        const failing = new PipelineV2RunStateStore({
          stateRoot: root,
          runId: "store-run",
          io: faultIo({ failCommit: 1, failStep }),
        });
        await expect(failing.commit(states[1]!, 1)).rejects.toThrow(/injected pipeline state failure/);
        const after = await readFile(statePath, "utf8");
        expect(after).toBe(before);
        // temp cleanup: no residue in the run directory
        expect(await listDir(join(root, "pipeline-runs", "store-run"))).toEqual(["state.json"]);
        // one-shot fault: the real store can commit cleanly afterwards
        await store.commit(states[1]!, 1);
        expect((await store.load())?.revision).toBe(2);
      });
    }
  });

  test("post-rename failures report durability_unknown with the candidate snapshot", async () => {
    const states = buildStates(RUN_ID);
    for (const failStep of ["dirsync", "dirfsync", "dirclose"] as const) {
      await withStore("store-run", async ({ root, store, statePath }) => {
        await store.create(states[0]!);
        const failing = new PipelineV2RunStateStore({
          stateRoot: root,
          runId: "store-run",
          io: faultIo({ failCommit: 1, failStep }),
        });
        const error = await failing.commit(states[1]!, 1).catch((cause: unknown) => cause);
        expect(error).toBeInstanceOf(PipelineV2RunStateDurabilityError);
        const durability = error as PipelineV2RunStateDurabilityError;
        expect(durability.message).toContain("durability could not be confirmed");
        expect(durability.revision).toBe(states[1]!.revision);
        expect(durability.candidate).toEqual(states[1]!);
        // the rename already happened: the loader sees the complete new snapshot
        expect(parsePipelineV2RunState(await readFile(statePath, "utf8"))).toEqual(states[1]!);
        expect(await listDir(join(root, "pipeline-runs", "store-run"))).toEqual(["state.json"]);
        // the real store continues from the on-disk revision (poison policy lives in the sink)
        await store.commit(states[2]!, 2);
        expect((await store.load())?.revision).toBe(3);
      });
    }
  });

  test("a post-rename failure of the first create reports durability_unknown with the candidate", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, statePath }) => {
      const failing = new PipelineV2RunStateStore({
        stateRoot: root,
        runId: "store-run",
        io: faultIo({ failCommit: 1, failStep: "dirsync" }),
      });
      const error = await failing.create(states[0]!).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2RunStateDurabilityError);
      const durability = error as PipelineV2RunStateDurabilityError;
      expect(durability.revision).toBe(1);
      expect(durability.candidate).toEqual(states[0]!);
      expect(parsePipelineV2RunState(await readFile(statePath, "utf8"))).toEqual(states[0]!);
    });
  });

  test("a pre-rename fsync failure reports not_committed and leaves the previous snapshot intact", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store, statePath }) => {
      await store.create(states[0]!);
      const before = await readFile(statePath, "utf8");
        const failing = new PipelineV2RunStateStore({
          stateRoot: root,
          runId: "store-run",
          io: faultIo({ failCommit: 1, failStep: "sync" }),
        });
      const error = await failing.commit(states[1]!, 1).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineV2RunStateStoreError);
      expect(error).not.toBeInstanceOf(PipelineV2RunStateDurabilityError);
      expect((error as Error).message).toContain("cannot commit pipeline v2 run state");
      expect(await readFile(statePath, "utf8")).toBe(before);
      expect((await store.load())?.revision).toBe(1);
      expect(await listDir(join(root, "pipeline-runs", "store-run"))).toEqual(["state.json"]);
    });
  });

  test("symlinked state targets are rejected", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store, statePath }) => {
      await mkdir(join(root, "pipeline-runs", "store-run"), { recursive: true });
      const outside = join(root, "outside-state.json");
      await writeFile(outside, "not state");
      await symlink(outside, statePath);
      await expect(store.load()).rejects.toThrow(/symlink/);
      await expect(store.create(states[0]!)).rejects.toThrow(/refusing to overwrite/);
      expect(await readFile(outside, "utf8")).toBe("not state");
    });
  });

  test("symlinked run directories are rejected before any state file is written", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store }) => {
      await mkdir(join(root, "pipeline-runs"), { recursive: true });
      const outside = join(root, "outside-run");
      await mkdir(outside, { recursive: true });
      await symlink(outside, join(root, "pipeline-runs", "store-run"));
      const sentinel = join(outside, "sentinel.txt");
      await writeFile(sentinel, "keep");
      await expect(store.create(states[0]!)).rejects.toThrow(/symlink/);
      expect(await readFile(sentinel, "utf8")).toBe("keep");
      expect(await listDir(outside)).toEqual(["sentinel.txt"]);
    });
  });

  test("the run id must obey the shared schema-v3 safe-id contract", async () => {
    await withStore("store-run", async ({ root }) => {
      const badRunIds = [
        "../escape",
        "a/b",
        "..",
        ".",
        "",
        "a..b",
        "run id",
        "rün",
        "a\\b",
        "a".repeat(129),
      ];
      for (const badRunId of badRunIds) {
        let constructorMessage = "";
        try {
          new PipelineV2RunStateStore({ stateRoot: root, runId: badRunId });
        } catch (cause) {
          expect(cause).toBeInstanceOf(PipelineV2StateError);
          constructorMessage = (cause as Error).message;
        }
        expect(constructorMessage).toBe(
          'pipeline v2 run id must be a safe non-empty identifier, got ' + JSON.stringify(badRunId),
        );
        // the path helper rejects with the same grammar and the same message
        let pathMessage = "";
        try {
          pipelineV2RunStatePath(root, badRunId);
        } catch (cause) {
          expect(cause).toBeInstanceOf(PipelineV2StateError);
          pathMessage = (cause as Error).message;
        }
        expect(pathMessage).toBe(constructorMessage);
      }
      // neither the constructor nor the path helper performed filesystem I/O
      expect(await listDir(root)).toEqual([]);
    });
  });

  test("valid boundary run ids are accepted and produce the documented path", async () => {
    const boundaryId = "a" + "b".repeat(127); // exactly 128 characters
    await withStore(boundaryId, async ({ root, store, statePath }) => {
      expect(statePath).toBe(
        join(root, "pipeline-runs", boundaryId, "state.json"),
      );
      expect(pipelineV2RunStatePath(root, boundaryId)).toBe(statePath);
      expect(pipelineV2RunStatePath(root, "run-1.x_y")).toBe(
        join(root, "pipeline-runs", "run-1.x_y", "state.json"),
      );
      const states = buildStates(RUN_ID);
      const withBoundary = states.map((state) => ({ ...state, run_id: boundaryId }));
      await store.create(withBoundary[0]!);
      await store.commit(withBoundary[1]!, 1);
      expect((await store.load())?.run_id).toBe(boundaryId);
    });
  });

  test("load on a missing tree returns null and creates nothing", async () => {
    await withStore("store-run", async ({ root, store }) => {
      expect(await store.load()).toBeNull();
      expect(await listDir(root)).toEqual([]);
      // a second load behaves identically
      expect(await store.load()).toBeNull();
      expect(await listDir(root)).toEqual([]);
    });
  });

  test("a symlinked pipeline-runs directory is rejected and the outside directory is untouched", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store }) => {
      const outside = join(root, "outside");
      await mkdir(outside, { recursive: true });
      const sentinel = join(outside, "sentinel.txt");
      await writeFile(sentinel, "keep");
      await symlink(outside, join(root, "pipeline-runs"));
      await expect(store.load()).rejects.toThrow(/is a symlink; symlinked state directories are rejected/);
      await expect(store.create(states[0]!)).rejects.toThrow(/is a symlink; symlinked state directories are rejected/);
      await expect(store.commit(states[1]!, 1)).rejects.toThrow(/symlink/);
      expect(await readFile(sentinel, "utf8")).toBe("keep");
      expect(await listDir(outside)).toEqual(["sentinel.txt"]);
    });
  });

  test("a symlinked run directory is rejected and the outside state file is never read", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store }) => {
      await mkdir(join(root, "pipeline-runs"), { recursive: true });
      const outside = join(root, "outside-run");
      await mkdir(outside, { recursive: true });
      const outsideState = join(outside, "state.json");
      await writeFile(outsideState, "not state");
      await symlink(outside, join(root, "pipeline-runs", "store-run"));
      // a symlink error, never a parse error: the outside file was not read
      await expect(store.load()).rejects.toThrow(/is a symlink; symlinked state directories are rejected/);
      await expect(store.load()).rejects.not.toThrow(/not valid JSON/);
      await expect(store.create(states[0]!)).rejects.toThrow(/symlink/);
      await expect(store.commit(states[1]!, 1)).rejects.toThrow(/symlink/);
      expect(await readFile(outsideState, "utf8")).toBe("not state");
    });
  });

  test("regular files in place of store-owned directories are rejected", async () => {
    const states = buildStates(RUN_ID);
    await withStore("store-run", async ({ root, store }) => {
      await writeFile(join(root, "pipeline-runs"), "not a directory");
      await expect(store.load()).rejects.toThrow(/is not a directory/);
      await expect(store.create(states[0]!)).rejects.toThrow(/is not a directory/);
      await expect(store.commit(states[1]!, 1)).rejects.toThrow(/is not a directory/);
      expect(await readFile(join(root, "pipeline-runs"), "utf8")).toBe("not a directory");
      await rm(join(root, "pipeline-runs"));
      await mkdir(join(root, "pipeline-runs"), { recursive: true });
      await writeFile(join(root, "pipeline-runs", "store-run"), "not a directory");
      await expect(store.load()).rejects.toThrow(/is not a directory/);
      await expect(store.create(states[0]!)).rejects.toThrow(/is not a directory/);
      expect(await readFile(join(root, "pipeline-runs", "store-run"), "utf8")).toBe("not a directory");
    });
  });

  test("the state path lives under the state root at the documented location", async () => {
    await withStore("store-run", async ({ root, statePath }) => {
      expect(statePath).toBe(join(root, "pipeline-runs", "store-run", "state.json"));
    });
  });
});
