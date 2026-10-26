/**
 * File-wide local test deadline policy: every test in this file builds a
 * durable fixture (a full finalized schema-v10 run through the real
 * reducer/provenance copy) and most spawn real child OS processes under
 * the parallel suite. Each test therefore carries an explicit local
 * 20000ms deadline; assertions, matrices and fixtures are unchanged and
 * the default suite command stays untouched.
 *
 * The cooperative project lock matrix: every orchestrator-owned reader of
 * the registered operator project takes the shared mode of the single
 * project lock while the filesystem writer holds the exclusive mode, so a
 * mid-apply multi-step state is never observed, copied, served or planned
 * through the cooperative contract. The writer side of every cross-process
 * scenario is a real child OS process holding a real kernel `flock` on an
 * open descriptor of the project root; the reader side uses the real
 * production facades and the real lock acquisition (real `flock`, real
 * fds) — there is no in-process fake mutex anywhere in this file.
 *
 * The non-cooperative boundary is unchanged and documented: IDEs, git or
 * any host writer that does not take the lock are not excluded.
 */
import { existsSync, mkdirSync } from "node:fs";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, test } from "bun:test";
import { spawn } from "bun";
import {
  applyPreparedPipelineV2Project,
  inspectPipelineV2ProjectApplyOperation,
  preparePipelineV2ProjectApply,
} from "../src/pipeline_v2_project_apply.ts";
import { projectChangesPipelineV2Run } from "../src/pipeline_v2_project_changes.ts";
import { projectChangeContentPipelineV2Run } from "../src/pipeline_v2_project_change_content.ts";
import { prepareRunProject } from "../src/pipeline_v2_runtime.ts";
import { runPipelineV2, type PipelineV2RunnerDeps } from "../src/pipeline_v2_runner.ts";
import {
  acquirePipelineV2ApplyProjectLock,
  acquirePipelineV2SharedProjectLock,
  PipelineV2ApplyProjectLockError,
  PipelineV2ProjectLockBusyError,
  type PipelineV2ApplyProjectLock,
} from "../src/pipeline_v2_apply_project_lock_internal.ts";
import { realProjectCopyIo, stageProjectCopy } from "../src/pipeline_v2_project_copy_internal.ts";
import { buildFinalizedApplyRun, disposeApplyRoots, fingerprintTree } from "./apply_fixtures.ts";

const ROOTS: string[] = [];
afterAll(async () => {
  await disposeApplyRoots();
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true }).catch(() => undefined);
  }
});

type Fixture = Awaited<ReturnType<typeof buildFinalizedApplyRun>>;

/** A finalized run plus a prepared multi-path operation (delete + two publications). */
async function preparedMultiPath(label: string): Promise<Fixture> {
  const fixture = await buildFinalizedApplyRun(`lock-${label}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`, {
    source: [
      { path: "gone.txt", kind: "file", content: "GONE" },
      { path: "chg.txt", kind: "file", content: "OLD" },
      { path: "sub", kind: "directory" },
      { path: "sub/keep.txt", kind: "file", content: "KEEP" },
    ],
    worker: async (ws) => {
      await rm(join(ws, "gone.txt"));
      await writeFile(join(ws, "chg.txt"), "NEW");
      await writeFile(join(ws, "added.txt"), "ADDED");
    },
  });
  await preparePipelineV2ProjectApply({
    applyStoreRoot: fixture.applyStoreRoot,
    applyId: "applyone",
    runId: fixture.runId,
    projectId: "proj1",
    projectRoot: fixture.operatorProject,
    observation: fixture.observation,
    stateRoot: fixture.stateRoot,
  });
  return fixture;
}

interface ChildHandle {
  readonly child: ReturnType<typeof spawn>;
  readonly output: string;
  readonly waitUntil: (marker: string) => Promise<void>;
  readonly readerDone: Promise<void>;
  readonly exited: Promise<number>;
}

/** One persistent stdout reader per child: markers and completion share it. */
function spawnChild(mode: string, fixture: Fixture): ChildHandle {
  const child = spawn({
    cmd: [
      process.execPath,
      join(import.meta.dir, "apply_writer_race_child.ts"),
      mode,
      fixture.applyStoreRoot,
      "applyone",
      fixture.operatorProject,
      "proj1",
    ],
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
  const state: { text: string; markers: Array<{ pattern: string; resolve: () => void }> } = {
    text: "",
    markers: [],
  };
  const decoder = new TextDecoder();
  const reader = (child.stdout as unknown as ReadableStream<Uint8Array>).getReader();
  const readerDone = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      state.text += decoder.decode(value, { stream: true });
      for (const marker of state.markers.splice(0)) {
        if (state.text.includes(marker.pattern)) {
          marker.resolve();
        }
      }
    }
    for (const marker of state.markers.splice(0)) {
      marker.resolve();
    }
  })();
  return {
    child,
    get output(): string {
      return state.text;
    },
    async waitUntil(marker: string): Promise<void> {
      if (state.text.includes(marker)) {
        return;
      }
      await new Promise<void>((resolve) => {
        state.markers.push({ pattern: marker, resolve });
      });
    },
    readerDone,
    exited: child.exited,
  };
}

async function readUntil(child: ChildHandle, marker: string): Promise<string> {
  await child.waitUntil(marker);
  return child.output;
}

async function runChildToCompletion(child: ChildHandle): Promise<{ stdout: string; exitCode: number }> {
  const [, exitCode] = await Promise.all([child.readerDone, child.exited]);
  return { stdout: child.output.trim(), exitCode };
}

function childResult(stdout: string): Record<string, unknown> {
  return JSON.parse(stdout.split("\n").filter((line) => line.trim().startsWith("{")).pop() ?? "{}") as Record<string, unknown>;
}

function writeStdin(child: ChildHandle, data: string): void {
  (child.child.stdin as unknown as { write(data: string): void }).write(data);
}

async function journal(fixture: Fixture, applyId: string): Promise<Record<string, unknown>> {
  return JSON.parse(
    await readFile(join(fixture.applyStoreRoot, "operations", applyId, "state.json"), "utf8"),
  ) as Record<string, unknown>;
}

async function copyTreeState(runRoot: string): Promise<{ gone: string; chg: string; added: string }> {
  const project = join(runRoot, "project");
  const read = (path: string) =>
    readFile(join(project, path), "utf8").then(
      (content) => content,
      (cause) => `absent(${cause instanceof Error ? cause.name : "unknown"})`,
    );
  return {
    gone: await read("gone.txt"),
    chg: await read("chg.txt"),
    added: await read("added.txt"),
  };
}

function expectLock(acquisition: { busy: boolean; lock?: PipelineV2ApplyProjectLock }): PipelineV2ApplyProjectLock {
  if (acquisition.busy || acquisition.lock === undefined) {
    throw new Error("the project lock was unexpectedly busy");
  }
  return acquisition.lock;
}

describe("cooperative project lock: shared readers versus the exclusive writer", () => {
  test("A: an applying writer blocks the fresh-run project copy with the typed busy refusal and creates no run", async () => {
    const fixture = await preparedMultiPath("a-copy");
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    try {
      const freshRoot = join(fixture.root, "fresh-a");
      await mkdir(freshRoot, { mode: 0o700 });
      const freshRunRoot = join(freshRoot, "pipeline-runs", "freshone");
      await mkdir(freshRunRoot, { mode: 0o700, recursive: true });
      let busyError: unknown = null;
      try {
        await prepareRunProject(fixture.operatorProject, freshRunRoot);
      } catch (cause) {
        busyError = cause;
      }
      expect(busyError).toBeInstanceOf(PipelineV2ProjectLockBusyError);
      expect((busyError as PipelineV2ProjectLockBusyError).reason).toBe("project_busy");
      expect(existsSync(join(freshRunRoot, "project"))).toBe(false);
      expect(existsSync(join(freshRunRoot, "project-provenance.json"))).toBe(false);
      // The runner-level gate refuses before any run root is claimed.
      const credentialFile = join(fixture.root, "credential.token");
      await writeFile(credentialFile, "dht_dummy");
      const runsDir = join(fixture.stateRoot, "pipeline-runs");
      const before = (await readdir(runsDir)).slice().sort();
      const deps: PipelineV2RunnerDeps = {
        cli: async () => ({ code: 1 }),
        fetchAuth: async () => ({
          status: 200,
          body: { authority: "launcher", principal: "michael", launcher_id: "dhl_locktest" },
        }),
        helperConfig: { socketPath: "/run/docker-helper/docker-helper.sock", credentialFile },
        baseEnv: { HOME: "/home/u" },
        stateRootProjection: { localRoot: fixture.stateRoot, daemonRoot: fixture.stateRoot },
      };
      const outcome = await runPipelineV2(
        {
          pipelineRoot: join(fixture.root, "bundle"),
          configRoot: join(fixture.root, "config"),
          projectSourcePath: fixture.operatorProject,
          inputBindings: [{ id: "task", path: join(fixture.operatorProject, "chg.txt") }],
          launcherId: "dhl_locktest",
        },
        deps,
      );
      expect(outcome).toEqual({
        ok: false,
        exitCode: 1,
        runId: "",
        runRoot: null,
        state: null,
        reason: "project_busy",
      });
      expect((await readdir(runsDir)).slice().sort()).toEqual(before);
    } finally {
      writeStdin(holder, "done\n");
      await runChildToCompletion(holder);
    }
  }, 20000);

  test("B: after the writer releases, the fresh run receives the whole result tree, never a hybrid", async () => {
    const fixture = await preparedMultiPath("b-hybrid");
    const child = spawnChild("execute-gated", fixture);
    await readUntil(child, "GATED");
    const freshRoot = join(fixture.root, "fresh-b");
    await mkdir(freshRoot, { mode: 0o700 });
    const busyRunRoot = join(freshRoot, "pipeline-runs", "busy");
    await mkdir(busyRunRoot, { mode: 0o700, recursive: true });
    await expect(prepareRunProject(fixture.operatorProject, busyRunRoot)).rejects.toBeInstanceOf(
      PipelineV2ProjectLockBusyError,
    );
    expect(existsSync(join(busyRunRoot, "project"))).toBe(false);
    writeStdin(child, "go\n");
    const finished = await runChildToCompletion(child);
    const result = childResult(finished.stdout);
    expect(result["ok"]).toBe(true);
    expect(result["outcome"]).toBe("applied");
    const afterRunRoot = join(freshRoot, "pipeline-runs", "after");
    await mkdir(afterRunRoot, { mode: 0o700, recursive: true });
    await prepareRunProject(fixture.operatorProject, afterRunRoot);
    expect(await copyTreeState(afterRunRoot)).toEqual({ gone: "absent(Error)", chg: "NEW", added: "ADDED" });
  }, 20000);

  test("C: a shared fresh-run reader blocks the writer before the first mutation with zero package or project damage", async () => {
    const fixture = await preparedMultiPath("c-sh-writer");
    const before = await fingerprintTree(fixture.operatorProject);
    const lock = expectLock(acquirePipelineV2SharedProjectLock(fixture.operatorProject));
    try {
      const child = spawnChild("execute", fixture);
      const finished = await runChildToCompletion(child);
      const result = JSON.parse(finished.stdout) as Record<string, unknown>;
      expect(result["ok"]).toBe(false);
      expect(result["reason"]).toBe("project_busy");
      expect(await fingerprintTree(fixture.operatorProject)).toBe(before);
      const state = await journal(fixture, "applyone");
      expect(state["status"]).toBe("prepared");
      expect(state["execution"]).toBe(null);
    } finally {
      lock.release();
    }
    // After the reader releases, the writer applies the whole plan.
    const applied = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    if (!applied.ok) {
      throw new Error(`apply failed: ${JSON.stringify(applied)}`);
    }
    expect(applied.ok).toBe(true);
  }, 20000);

  test("D: several shared readers are compatible, across processes and in one process", async () => {
    const fixture = await preparedMultiPath("d-shared");
    const child = spawnChild("hold-shared", fixture);
    expect((await readUntil(child, "HOLD")).includes("HOLD")).toBe(true);
    try {
      const first = expectLock(acquirePipelineV2SharedProjectLock(fixture.operatorProject));
      const second = expectLock(acquirePipelineV2SharedProjectLock(fixture.operatorProject));
      second.release();
      const childWriter = spawnChild("execute", fixture);
      const finished = await runChildToCompletion(childWriter);
      const result = JSON.parse(finished.stdout) as Record<string, unknown>;
      expect(result["reason"]).toBe("project_busy");
      first.release();
    } finally {
      writeStdin(child, "done\n");
      await runChildToCompletion(child);
    }
  }, 20000);

  test("E: an applying writer blocks the Project Changes observation with zero partial response and zero writes", async () => {
    const fixture = await preparedMultiPath("e-changes");
    const before = await fingerprintTree(fixture.operatorProject);
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    try {
      const result = await projectChangesPipelineV2Run({
        runId: fixture.runId,
        stateRoot: fixture.stateRoot,
        projectPath: fixture.operatorProject,
      });
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe("project_busy");
        expect(result.runId).toBe(fixture.runId);
        expect(result.runRoot).toBe(fixture.runRoot);
      }
      expect(await fingerprintTree(fixture.operatorProject)).toBe(before);
    } finally {
      writeStdin(holder, "done\n");
      await runChildToCompletion(holder);
    }
  }, 20000);

  test("F: an applying writer blocks the operator-side content read and serves no payload byte", async () => {
    const fixture = await preparedMultiPath("f-content");
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    try {
      const result = await projectChangeContentPipelineV2Run({
        runId: fixture.runId,
        stateRoot: fixture.stateRoot,
        projectPath: fixture.operatorProject,
        changeIndex: 1,
        side: "baseline",
        observation: "a".repeat(64),
      });
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe("project_busy");
      }
      expect(JSON.stringify(result).includes("dataBase64")).toBe(false);
    } finally {
      writeStdin(holder, "done\n");
      await runChildToCompletion(holder);
    }
  }, 20000);

  test("G: an applying writer blocks the Apply preparation before any store effect", async () => {
    const fixture = await preparedMultiPath("g-prepare");
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    try {
      let failure: unknown = null;
      try {
        await preparePipelineV2ProjectApply({
          applyStoreRoot: fixture.applyStoreRoot,
          applyId: "applytwo",
          runId: fixture.runId,
          projectId: "proj1",
          projectRoot: fixture.operatorProject,
          observation: fixture.observation,
          stateRoot: fixture.stateRoot,
        });
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).name).toBe("PipelineV2ProjectApplyError");
      expect((failure as Error & { reason: string }).reason).toBe("preparation_busy");
      expect(existsSync(join(fixture.applyStoreRoot, "operations", "applytwo"))).toBe(false);
      const state = await journal(fixture, "applyone");
      expect(state["status"]).toBe("prepared");
    } finally {
      writeStdin(holder, "done\n");
      await runChildToCompletion(holder);
    }
  }, 20000);

  test("H: a crashed shared reader releases the lock; the writer then applies", async () => {
    const fixture = await preparedMultiPath("h-crash-sh");
    const child = spawnChild("hold-shared", fixture);
    expect((await readUntil(child, "HOLD")).includes("HOLD")).toBe(true);
    child.child.kill("SIGKILL");
    await child.exited;
    const applied = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    if (!applied.ok) {
      throw new Error(`apply failed: ${JSON.stringify(applied)}`);
    }
    expect(applied.ok).toBe(true);
  }, 20000);

  test("I: a crashed exclusive writer releases the lock; the recovery then applies", async () => {
    const fixture = await preparedMultiPath("i-crash-ex");
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    writeStdin(holder, "kill\n");
    await holder.exited;
    const applied = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    if (!applied.ok) {
      throw new Error(`apply failed: ${JSON.stringify(applied)}`);
    }
    expect(applied.ok).toBe(true);
  }, 20000);

  test("K: the staged copy proves its scanned source against the held lock identity", async () => {
    const fixture = await preparedMultiPath("k-identity");
    const runRoot = join(fixture.root, "k-run");
    await mkdir(join(runRoot, "pipeline-runs", "kid"), { mode: 0o700, recursive: true });
    const lock = expectLock(acquirePipelineV2SharedProjectLock(fixture.operatorProject));
    try {
      const staged = await stageProjectCopy(realProjectCopyIo, fixture.operatorProject, runRoot, lock);
      expect(staged.run_root).toBe(runRoot);
    } finally {
      lock.release();
    }
    const wrongIdentity: PipelineV2ApplyProjectLock = {
      fd: -1,
      identity: { dev: lock.identity.dev, ino: lock.identity.ino + 1 },
      release: () => undefined,
    };
    await expect(
      stageProjectCopy(realProjectCopyIo, fixture.operatorProject, runRoot, wrongIdentity),
    ).rejects.toThrow("the held project lock does not cover the scanned project source directory");
  }, 20000);

  test("the inspect projection of a prepared operation stays read-only beside the lock matrix", async () => {
    const fixture = await preparedMultiPath("k-inspect");
    const projection = await inspectPipelineV2ProjectApplyOperation({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
    });
    expect(projection.status).toBe("prepared");
  }, 20000);
});

describe("cooperative project lock: the single lock owner", () => {
  test("shared and exclusive modes exclude each other in both directions; shared is reentrant", () => {
    const project = join(tmpdir(), `lock-owner-${Date.now()}-${Math.floor(Math.random() * 1e9)}`);
    ROOTS.push(project);
    mkdirSync(project, { mode: 0o700 });
    const shared1 = expectLock(acquirePipelineV2SharedProjectLock(project));
    const shared2 = expectLock(acquirePipelineV2SharedProjectLock(project));
    expect(acquirePipelineV2ApplyProjectLock(project).busy).toBe(true);
    shared2.release();
    expect(acquirePipelineV2ApplyProjectLock(project).busy).toBe(true);
    shared1.release();
    const exclusive = expectLock(acquirePipelineV2ApplyProjectLock(project));
    expect(acquirePipelineV2SharedProjectLock(project).busy).toBe(true);
    exclusive.release();
    const again = expectLock(acquirePipelineV2SharedProjectLock(project));
    again.release();
  }, 20000);

  test("the shared acquisition fixes the canonical identity and releases idempotently", async () => {
    const root = await mkdtemp(join(tmpdir(), "lock-owner-"));
    ROOTS.push(root);
    const project = join(root, "project");
    await mkdir(project, { mode: 0o700 });
    const lock = expectLock(acquirePipelineV2SharedProjectLock(project));
    const identity = await stat(project);
    expect(lock.identity.dev).toBe(identity.dev);
    expect(lock.identity.ino).toBe(identity.ino);
    lock.release();
    lock.release();
    const reacquired = expectLock(acquirePipelineV2SharedProjectLock(project));
    reacquired.release();
  }, 20000);

  test("a missing or non-directory root is the typed root_invalid refusal; a symlinked leaf canonicalizes", async () => {
    const root = await mkdtemp(join(tmpdir(), "lock-owner-"));
    ROOTS.push(root);
    const missing = join(root, "missing");
    expect(() => acquirePipelineV2SharedProjectLock(missing)).toThrow(PipelineV2ApplyProjectLockError);
    const file = join(root, "file.txt");
    await writeFile(file, "x");
    let fileFailure: unknown = null;
    try {
      acquirePipelineV2SharedProjectLock(file);
    } catch (cause) {
      fileFailure = cause;
    }
    expect(fileFailure).toBeInstanceOf(PipelineV2ApplyProjectLockError);
    expect((fileFailure as PipelineV2ApplyProjectLockError).reason).toBe("root_invalid");
    const target = join(root, "target");
    await mkdir(target, { mode: 0o700 });
    const link = join(root, "link");
    await symlink(target, link);
    const throughLink = expectLock(acquirePipelineV2SharedProjectLock(link));
    const targetIdentity = await stat(target);
    expect(throughLink.identity.ino).toBe(targetIdentity.ino);
    throughLink.release();
  }, 20000);
});
