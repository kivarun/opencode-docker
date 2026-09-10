import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { realpath } from "node:fs/promises";
import {
  parsePipelineRunState,
  reducePipelineRunCommand,
  validatePipelineRunState,
  type PipelineRunCommand,
  type PipelineRunState,
} from "../src/pipeline_state.ts";
import {
  defaultPipelineStateIo,
  PipelineStateDurabilityError,
  PipelineStateStore,
  PipelineStateStoreError,
  pipelineRunStatePath,
  type PipelineStateIo,
} from "../src/pipeline_state_store.ts";
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

const TICKS = [1, 2, 3, 4, 5, 6, 7, 8, 9].map((t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t)));

function tick(index: number): Date {
  const value = TICKS[index];
  if (value === undefined) {
    throw new Error(`tick ${index} out of range`);
  }
  return value;
}

function commands(): PipelineRunCommand[] {
  return [
    {
      kind: "create_run",
      runId: "store-run",
      workspace: "/work",
      identity: IDENTITY,
      protectedInputs: [PROTECTED_INPUT],
    },
    { kind: "start_activation", stateId: "execute", profile: "default" },
    { kind: "activation_session_created", sessionId: "dhs_child" },
    { kind: "activation_agent_running" },
    { kind: "activation_result_accepted", resultSha256: "c".repeat(64), artifacts: ["out/product.txt"] },
    { kind: "activation_cleanup_completed" },
    {
      kind: "transition_committed",
      step: { from: "execute", outcome: "completed", to: "completed", transition_index: 0 },
      activationIndex: 1,
      resultSha256: "c".repeat(64),
      artifacts: ["out/product.txt"],
    },
    { kind: "terminal_reached", terminalStateId: "completed", terminalResult: "success" },
    { kind: "run_succeeded" },
  ];
}

function buildStates(): PipelineRunState[] {
  const states: PipelineRunState[] = [];
  let current: PipelineRunState | null = null;
  commands().forEach((command, index) => {
    current = reducePipelineRunCommand(current, command, tick(index));
    states.push(current);
  });
  return states;
}

async function withStore(
  runId: string,
  fn: (ctx: {
    root: string;
    store: PipelineStateStore;
    workspace: string;
    statePath: string;
  }) => Promise<void>,
): Promise<void> {
  const outer = await mkdtemp(join(tmpdir(), "pipeline-state-store-"));
  const root = join(outer, "state-root");
  const workspace = join(outer, "workspace");
  await mkdir(root, { recursive: true });
  await mkdir(workspace, { recursive: true });
  try {
    await fn({
      root,
      workspace,
      statePath: pipelineRunStatePath(root, runId),
      store: new PipelineStateStore(root, runId),
    });
  } finally {
    await rm(outer, { recursive: true, force: true });
  }
}

async function listDir(path: string): Promise<string[]> {
  return (await readdir(path)).sort();
}

describe("pipeline run state store", () => {
  test("first create persists a complete snapshot with a trailing newline", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ store, statePath }) => {
      expect(await store.load()).toBeNull();
      await store.create(states[0]!);
      const loaded = await store.load();
      expect(loaded).toEqual(states[0]!);
      const raw = await readFile(statePath, "utf8");
      expect(raw.endsWith("\n")).toBe(true);
      expect(raw.trimEnd().startsWith("{")).toBe(true);
      // the loader validates the exact document, not just a shape
      expect(parsePipelineRunState(raw)).toEqual(states[0]!);
    });
  });

  test("several revisions are committed in order and the loader always sees a full snapshot", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ store, statePath }) => {
      await store.create(states[0]!);
      for (let i = 1; i < states.length; i++) {
        await store.commit(states[i]!, states[i - 1]!.revision);
        const loaded = await store.load();
        expect(loaded?.revision).toBe(states[i]!.revision);
        expect(loaded).toEqual(states[i]!);
      }
      const finalRaw = await readFile(statePath, "utf8");
      expect(parsePipelineRunState(finalRaw).status).toBe("success");
    });
  });

  test("private directories and the state file have restrictive permissions", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, statePath }) => {
      const store = new PipelineStateStore(root, "store-run");
      await store.create(states[0]!);
      const fileMode = (await stat(statePath)).mode & 0o777;
      expect(fileMode).toBe(0o600);
      const runDirMode = (await stat(join(root, "pipeline-runs", "store-run"))).mode & 0o777;
      expect(runDirMode).toBe(0o700);
      const runsDirMode = (await stat(join(root, "pipeline-runs"))).mode & 0o777;
      expect(runsDirMode).toBe(0o700);
    });
  });

  test("a wrong expected revision is rejected fail-closed", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ store }) => {
      await store.create(states[0]!);
      await expect(store.commit(states[2]!, 1)).rejects.toThrow(/must be exactly 2/);
      await expect(store.commit(states[1]!, 2)).rejects.toThrow(/committed snapshot has revision 1, expected 2/);
      // the committed snapshot is untouched
      expect((await store.load())?.revision).toBe(1);
    });
  });

  test("malformed, truncated, and unknown-field state documents are rejected", async () => {
    await withStore("store-run", async ({ root, store, statePath }) => {
      await mkdir(join(root, "pipeline-runs", "store-run"), { recursive: true });
      const cases: string[] = [
        "{ not json",
        '{"schema_version":1,"revision":1',
        "[]",
        "null",
        JSON.stringify({ schema_version: 1, revision: 1, run_id: "x", status: "active", phase: "validating" }),
      ];
      for (const body of cases) {
        await writeFile(statePath, `${body}\n`);
        await expect(store.load()).rejects.toThrow();
      }
      const unknown = {
        ...JSON.parse(JSON.stringify(buildStates()[0])),
        surprise: true,
      };
      await writeFile(statePath, JSON.stringify(unknown));
      await expect(store.load()).rejects.toThrow(/unknown field "surprise"/);
      // a leftover temp file is not state: loading ignores it
      await rm(statePath);
      await writeFile(join(root, "pipeline-runs", "store-run", "state.json.tmp-leftover"), "garbage");
      expect(await store.load()).toBeNull();
    });
  });

  test("a failure before the rename keeps the previous snapshot byte-for-byte intact", async () => {
    const states = buildStates();
    for (const failStep of ["open", "write", "sync", "rename"] as const) {
      await withStore("store-run", async ({ root, store, statePath }) => {
        await store.create(states[0]!);
        const before = await readFile(statePath, "utf8");
        const failing = new PipelineStateStore(root, "store-run", faultIo({ failCommit: 1, failStep }));
        await expect(failing.commit(states[1]!, 1)).rejects.toThrow(/injected pipeline state failure/);
        const after = await readFile(statePath, "utf8");
        expect(after).toBe(before);
        // temp cleanup: no residue in the run directory
        expect(await listDir(join(root, "pipeline-runs", "store-run"))).toEqual(["state.json"]);
        // and the store can commit cleanly afterwards (one-shot fault)
        await store.commit(states[1]!, 1);
        expect((await store.load())?.revision).toBe(2);
      });
    }
  });

  test("a post-rename failure (directory fsync) reports durability_unknown with the candidate snapshot", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, store, statePath }) => {
      await store.create(states[0]!);
      const failing = new PipelineStateStore(root, "store-run", faultIo({ failCommit: 1, failStep: "dirsync" }));
      const error = await failing.commit(states[1]!, 1).catch((cause: unknown) => cause);
      // typed durability_unknown outcome: rename succeeded, durability unknown
      expect(error).toBeInstanceOf(PipelineStateDurabilityError);
      const durability = error as PipelineStateDurabilityError;
      expect(durability.message).toContain("durability could not be confirmed");
      expect(durability.message).not.toContain("remains authoritative");
      expect(durability.revision).toBe(states[1]!.revision);
      expect(durability.candidate).toEqual(states[1]!);
      // the rename already happened: the loader sees the complete new
      // snapshot, never a partial one
      expect(parsePipelineRunState(await readFile(statePath, "utf8"))).toEqual(states[1]!);
      const runDir = join(root, "pipeline-runs", "store-run");
      expect(await listDir(runDir)).toEqual(["state.json"]);
      // the real store continues from the on-disk revision (the poison policy
      // lives in the sink, not in the store)
      await store.commit(states[2]!, 2);
      expect((await store.load())?.revision).toBe(3);
    });
  });

  test("a post-rename failure of the first create reports durability_unknown with the candidate", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, statePath }) => {
      const failing = new PipelineStateStore(root, "store-run", faultIo({ failCommit: 1, failStep: "dirsync" }));
      const error = await failing.create(states[0]!).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineStateDurabilityError);
      const durability = error as PipelineStateDurabilityError;
      expect(durability.revision).toBe(1);
      expect(durability.candidate).toEqual(states[0]!);
      // the first snapshot is visible on disk nonetheless
      expect(parsePipelineRunState(await readFile(statePath, "utf8"))).toEqual(states[0]!);
    });
  });

  test("a pre-rename fsync failure reports not_committed and leaves the previous snapshot intact", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, store, statePath }) => {
      await store.create(states[0]!);
      const before = await readFile(statePath, "utf8");
      const failing = new PipelineStateStore(root, "store-run", faultIo({ failCommit: 1, failStep: "sync" }));
      const error = await failing.commit(states[1]!, 1).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineStateStoreError);
      expect(error).not.toBeInstanceOf(PipelineStateDurabilityError);
      expect((error as Error).message).toContain("cannot commit pipeline run state");
      // the previous snapshot is byte-for-byte intact and authoritative
      expect(await readFile(statePath, "utf8")).toBe(before);
      expect((await store.load())?.revision).toBe(1);
      // no residue in the run directory
      expect(await listDir(join(root, "pipeline-runs", "store-run"))).toEqual(["state.json"]);
    });
  });

  test("create refuses to overwrite an existing run", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ store, statePath }) => {
      await store.create(states[0]!);
      const before = await readFile(statePath, "utf8");
      await expect(store.create(states[0]!)).rejects.toThrow(/refusing to overwrite an existing pipeline run state/);
      expect(await readFile(statePath, "utf8")).toBe(before);
      // even a different revision cannot clobber it via create
      await expect(store.create(states[1]!)).rejects.toThrow(/refusing to overwrite/);
      expect(await readFile(statePath, "utf8")).toBe(before);
    });
  });

  test("concurrent commits of one run are serialized in-process", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ store }) => {
      await store.create(states[0]!);
      // fire both commits without awaiting the first
      const second = store.commit(states[1]!, 1);
      const third = store.commit(states[2]!, 2);
      await Promise.all([second, third]);
      const loaded = await store.load();
      expect(loaded?.revision).toBe(3);
      expect(loaded?.events.map((event) => event.kind)).toEqual([
        "run_created",
        "activation_started",
        "session_created",
      ]);
    });
  });

  test("symlinked state targets are rejected", async () => {
    const states = buildStates();
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

  test("store-owned directories are verified component-wise below the trusted state root", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, store }) => {
      // a missing tree: load returns null and creates nothing
      expect(await store.load()).toBeNull();
      expect((await readdir(root)).length).toBe(0);
      // a symlinked `pipeline-runs` directory is rejected before anything is
      // created inside the outside directory, with the unchanged v1 message
      const outside = join(root, "outside");
      await mkdir(outside, { recursive: true });
      const sentinel = join(outside, "sentinel.txt");
      await writeFile(sentinel, "keep");
      await symlink(outside, join(root, "pipeline-runs"));
      const symlinkError = await store.load().catch((cause: unknown) => cause);
      expect(symlinkError).toBeInstanceOf(PipelineStateStoreError);
      expect((symlinkError as Error).message).toBe(
        `pipeline run state directory ${join(root, "pipeline-runs")} is a symlink; symlinked state directories are rejected`,
      );
      await expect(store.create(states[0]!)).rejects.toThrow(/symlink/);
      expect(await readFile(sentinel, "utf8")).toBe("keep");
      expect(await readdir(outside)).toEqual(["sentinel.txt"]);
      // a regular file in place of a store-owned directory is rejected
      await rm(join(root, "pipeline-runs"));
      await writeFile(join(root, "pipeline-runs"), "not a directory");
      await expect(store.load()).rejects.toThrow(
        `pipeline run state directory ${join(root, "pipeline-runs")} is not a directory`,
      );
    });
  });

  test("the state path lives under the state root, never inside the workspace", async () => {
    await withStore("store-run", async ({ root, statePath, workspace }) => {
      expect(statePath).toBe(join(root, "pipeline-runs", "store-run", "state.json"));
      expect(statePath.startsWith(workspace)).toBe(false);
      expect((await realpath(workspace))).not.toBe(await realpath(root));
    });
  });

  test("the commit protocol runs in the documented order", async () => {
    const states = buildStates();
    await withStore("store-run", async ({ root, store }) => {
      const order: string[] = [];
      const observing = new PipelineStateStore(root, "store-run", loggingIo(order));
      await observing.create(states[0]!);
      expect(order).toEqual(["open", "write", "sync", "close", "rename", "dirsync"]);
      // the observing store wrote the same thing the real store reads back
      expect((await store.load())?.revision).toBe(1);
    });
  });
});

function loggingIo(order: string[]): PipelineStateIo {
  return {
    ...defaultPipelineStateIo,
    async openExclusive(path, mode) {
      const handle = await defaultPipelineStateIo.openExclusive(path, mode);
      order.push("open");
      return {
        chmod: (m) => handle.chmod(m),
        writeAll: async (bytes) => {
          await handle.writeAll(bytes);
          order.push("write");
        },
        sync: async () => {
          await handle.sync();
          order.push("sync");
        },
        close: async () => {
          await handle.close();
          order.push("close");
        },
      };
    },
    async rename(from, to) {
      await defaultPipelineStateIo.rename(from, to);
      order.push("rename");
    },
    async openDir(path) {
      const handle = await defaultPipelineStateIo.openDir(path);
      return {
        sync: async () => {
          await handle.sync();
          order.push("dirsync");
        },
        close: () => handle.close(),
      };
    },
  };
}
