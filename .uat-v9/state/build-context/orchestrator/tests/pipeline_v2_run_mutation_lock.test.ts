/**
 * The per-run mutation lock: the lock owner's own contract (typed busy
 * result, fail-closed acquisition, idempotent kernel release, the public
 * reason mapping) and the cross-process kernel proof — a second OS
 * process observes the held lock, and the kernel releases the lock when
 * the holder is killed. The runner-level busy outcomes and the race
 * matrix live in the dedicated suites.
 */
import { afterAll, expect, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  acquirePipelineV2RunMutationLock,
  PipelineV2RunMutationLockError,
  pipelineV2RunMutationLockPublicReason,
} from "../src/pipeline_v2_run_mutation_lock.ts";
import { PipelineV2StateError } from "../src/pipeline_v2_state.ts";

const ROOTS: string[] = [];

async function makeRunRoot(prefix: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const runRoot = join(root, "pipeline-runs", "run-1");
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  return runRoot;
}

async function readLine(stream: ReadableStream<Uint8Array>): Promise<string> {
  const iterable = stream as unknown as AsyncIterable<Uint8Array>;
  let buffer = "";
  const decoder = new TextDecoder();
  for await (const chunk of iterable) {
    buffer += decoder.decode(chunk);
    const newline = buffer.indexOf("\n");
    if (newline >= 0) return buffer.slice(0, newline).trim();
  }
  return buffer.trim();
}

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

test("acquire, release, reacquire: one exclusive kernel lock per run root", async () => {
  const dir = await makeRunRoot("p2lock-basic-");
  const first = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
  expect(first.busy).toBe(false);
  if (first.busy) throw new Error("unreachable");
  // A second open file description conflicts even inside one process.
  const second = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
  expect(second.busy).toBe(true);
  first.lock.release();
  first.lock.release();
  const third = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
  expect(third.busy).toBe(false);
  if (!third.busy) third.lock.release();
});

test("the lock handle is a frozen single-method object", async () => {
  const dir = await makeRunRoot("p2lock-frozen-");
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
  if (acquisition.busy) throw new Error("unreachable");
  expect(Object.isFrozen(acquisition.lock)).toBe(true);
  expect(Object.keys(acquisition.lock)).toEqual(["release"]);
  acquisition.lock.release();
});

test("the existing mode requires the state document before the lock", async () => {
  const root = await mkdtemp(join(tmpdir(), "p2lock-existing-XXXXXX"));
  const runRoot = join(root, "pipeline-runs", "run-1");
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  expect(() =>
    acquirePipelineV2RunMutationLock({ runRoot, runId: "run-1", mode: "existing" }),
  ).toThrow(PipelineV2RunMutationLockError);
  await writeFile(join(runRoot, "state.json"), "{}", { mode: 0o600 });
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId: "run-1", mode: "existing" });
  expect(acquisition.busy).toBe(false);
  if (!acquisition.busy) acquisition.lock.release();
});

test("fail-closed on unsafe run-root objects", async () => {
  const root = await mkdtemp(join(tmpdir(), "p2lock-unsafe-XXXXXX"));
  const realDir = join(root, "real");
  await mkdir(realDir, { recursive: true, mode: 0o700 });
  await symlink(realDir, join(root, "link"));
  await writeFile(join(root, "file"), "x", { mode: 0o600 });
  const fifo = join(root, "fifo");
  await Bun.$`mkfifo ${fifo}`.quiet();
  for (const target of [join(root, "link"), join(root, "file"), fifo, join(root, "absent")]) {
    let failure: PipelineV2RunMutationLockError | null = null;
    try {
      const acquisition = acquirePipelineV2RunMutationLock({ runRoot: target, runId: "run-1", mode: "fresh" });
      if (!acquisition.busy) acquisition.lock.release();
    } catch (cause) {
      failure = cause instanceof PipelineV2RunMutationLockError ? cause : null;
    }
    expect(failure).not.toBeNull();
    expect(failure!.reason).toBe("run_layout_invalid");
  }
});

test("fail-closed on an unsafe run id", async () => {
  const runRoot = await makeRunRoot("p2lock-badid-");
  expect(() =>
    acquirePipelineV2RunMutationLock({ runRoot, runId: "../escape", mode: "fresh" }),
  ).toThrow(PipelineV2StateError);
});

test("release on every throw path: the finally pattern frees the lock", async () => {
  const dir = await makeRunRoot("p2lock-finally-");
  const attempt = (): void => {
    const acquisition = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
    if (acquisition.busy) throw new Error("busy");
    try {
      throw new Error("the command failed");
    } finally {
      acquisition.lock.release();
    }
  };
  expect(attempt).toThrow("the command failed");
  const after = acquirePipelineV2RunMutationLock({ runRoot: dir, runId: "run-1", mode: "fresh" });
  expect(after.busy).toBe(false);
  if (!after.busy) after.lock.release();
});

test("the public reason mapping of the lock failures", () => {
  const stateMissing = new PipelineV2RunMutationLockError("state_document_missing", "m");
  const layout = new PipelineV2RunMutationLockError("run_layout_invalid", "m");
  const infrastructure = new PipelineV2RunMutationLockError("lock_infrastructure", "m");
  expect(pipelineV2RunMutationLockPublicReason(stateMissing)).toBe("missing_state");
  expect(pipelineV2RunMutationLockPublicReason(layout)).toBe("run_layout_invalid");
  expect(pipelineV2RunMutationLockPublicReason(infrastructure)).toBe("internal_error");
  expect(pipelineV2RunMutationLockPublicReason(new Error("unrelated"))).toBeNull();
  expect(pipelineV2RunMutationLockPublicReason(null)).toBeNull();
});

test("cross-process: a second process observes the held lock and its release", async () => {
  const runRoot = await makeRunRoot("p2lock-cross-");
  const probe = join(import.meta.dir, "helpers", "pipeline_v2_lock_probe.ts");
  const before = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(before.stdout.toString().trim()).toBe("FREE");

  const holder = Bun.spawn([process.execPath, probe, "hold", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdin: "pipe",
    stdout: "pipe",
  });
  expect(await readLine(holder.stdout)).toBe("HELD");
  const during = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(during.stdout.toString().trim()).toBe("BUSY");
  holder.stdin!.write("release\n");
  await holder.exited;
  const after = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(after.stdout.toString().trim()).toBe("FREE");
}, 60000);

test("cross-process: SIGKILL of the holder frees the lock with no repair", async () => {
  const runRoot = await makeRunRoot("p2lock-kill-");
  const probe = join(import.meta.dir, "helpers", "pipeline_v2_lock_probe.ts");
  const holder = Bun.spawn([process.execPath, probe, "hold", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdin: "pipe",
    stdout: "pipe",
  });
  expect(await readLine(holder.stdout)).toBe("HELD");
  const during = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(during.stdout.toString().trim()).toBe("BUSY");
  holder.kill(9);
  await holder.exited;
  const after = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "run-1"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(after.stdout.toString().trim()).toBe("FREE");
}, 60000);

test("different run ids never block each other", async () => {
  const root = await mkdtemp(join(tmpdir(), "p2lock-parallel-"));
  const runsDir = join(root, "pipeline-runs");
  await mkdir(join(runsDir, "run-a"), { recursive: true, mode: 0o700 });
  await mkdir(join(runsDir, "run-b"), { recursive: true, mode: 0o700 });
  const a = acquirePipelineV2RunMutationLock({ runRoot: join(runsDir, "run-a"), runId: "run-a", mode: "fresh" });
  expect(a.busy).toBe(false);
  const b = acquirePipelineV2RunMutationLock({ runRoot: join(runsDir, "run-b"), runId: "run-b", mode: "fresh" });
  expect(b.busy).toBe(false);
  if (!a.busy) a.lock.release();
  if (!b.busy) b.lock.release();
});

test("an acquired lock does not change the run tree: no artifact is created", async () => {
  const runRoot = await makeRunRoot("p2lock-artifact-");
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId: "run-1", mode: "fresh" });
  if (acquisition.busy) throw new Error("unreachable");
  const before = await Array.fromAsync(new Bun.Glob("**/*").scan({ cwd: runRoot }));
  expect(before).toEqual([]);
  acquisition.lock.release();
  const after = await Array.fromAsync(new Bun.Glob("**/*").scan({ cwd: runRoot }));
  expect(after).toEqual([]);
});

test("an unsafe existing run-root mode is not chmodded by the lock", async () => {
  const root = await mkdtemp(join(tmpdir(), "p2lock-mode-"));
  const runRoot = join(root, "pipeline-runs", "run-1");
  await mkdir(runRoot, { recursive: true, mode: 0o755 });
  let failure: PipelineV2RunMutationLockError | null = null;
  try {
    const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId: "run-1", mode: "fresh" });
    if (!acquisition.busy) acquisition.lock.release();
  } catch (cause) {
    failure = cause instanceof PipelineV2RunMutationLockError ? cause : null;
  }
  // The lock itself does not police the mode (the callers verify it); the
  // directory is left exactly as found.
  expect(failure).toBeNull();
  const info = await lstat(runRoot);
  expect(info.mode & 0o7777).toBe(0o755);
  void chmod;
});

// --- source restrictions ----------------------------------------------------

import { readFile } from "node:fs/promises";

const lockModule = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_run_mutation_lock.ts"), "utf8");
const runnerModule = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_runner.ts"), "utf8");
const respondModule = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_wait_respond.ts"), "utf8");
const inspectModule = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_inspect.ts"), "utf8");
const mainModule = await readFile(join(import.meta.dir, "..", "src", "main.ts"), "utf8");
const reasonModule = await readFile(join(import.meta.dir, "..", "src", "pipeline_v2_public_reason.ts"), "utf8");

test("source: exactly one lock implementation body and one acquisition call per entrypoint", async () => {
  // One implementation body: exactly one real flock call site
  // (`libc.flock(...)`), in the lock module only; every other occurrence
  // is documentation or the symbol declaration.
  expect((lockModule.match(/libc\.flock\(/g) ?? []).length).toBe(1);
  for (const module of [runnerModule, respondModule, inspectModule, mainModule, reasonModule]) {
    expect(module.includes("libc.flock(")).toBe(false);
    expect(module.includes('"flock"')).toBe(false);
  }
  // One fresh-run acquisition site, placed after the atomic claim.
  expect((runnerModule.match(/acquirePipelineV2RunMutationLock\(/g) ?? []).length).toBe(2);
  const claimIndex = runnerModule.indexOf("prepareRunRoot(deps.stateRootProjection");
  const freshLockIndex = runnerModule.indexOf('mode: "fresh"');
  expect(claimIndex).toBeGreaterThan(0);
  expect(freshLockIndex).toBeGreaterThan(claimIndex);
  // One shared existing-run acquisition site and one respond site.
  expect((runnerModule.match(/mode: "existing"/g) ?? []).length).toBe(1);
  expect((respondModule.match(/acquirePipelineV2RunMutationLock\(/g) ?? []).length).toBe(1);
});

test("source: one run_busy public reason, typed construction, no message parsing", async () => {
  expect((reasonModule.match(/"run_busy"/g) ?? []).length).toBe(1);
  // The busy outcome is constructed by value at the runner/respond sites;
  // the string never appears in a catch-based classification.
  // One busy outcome per mutating entrypoint family: the fresh runner and
  // the shared existing-run core in the runner module, one in the respond
  // command.
  expect((runnerModule.match(/"run_busy"/g) ?? []).length).toBe(2);
  expect((respondModule.match(/reason: "run_busy"/g) ?? []).length).toBe(1);
  expect(lockModule.includes(".message.includes")).toBe(false);
  expect(lockModule.includes("includes(")).toBe(false);
  expect(lockModule.includes(".match(")).toBe(false);
});

test("source: no stale-lock heuristics, no registry, no second machinery", async () => {
  expect(lockModule.includes("Date.now")).toBe(false);
  expect(lockModule.includes("mtimeMs")).toBe(false);
  expect(lockModule.includes("process.kill")).toBe(false);
  expect(lockModule.includes("new Map(")).toBe(false);
  expect(lockModule.includes("new Set(")).toBe(false);
  expect(lockModule.includes("JSON.parse")).toBe(false);
  expect(lockModule.includes("canonicalJson")).toBe(false);
  expect(lockModule.includes("Bun.spawn")).toBe(false);
  expect(lockModule.includes("exec ")).toBe(false);
  expect(lockModule.includes("flock")).toBe(true);
  // No reducer or state-store involvement.
  expect(lockModule.includes("reducePipelineV2RunCommand")).toBe(false);
  expect(lockModule.includes("PipelineV2RunStateSink")).toBe(false);
});

test("source: inspect never imports the lock owner; the reporter handles busy", async () => {
  expect(inspectModule.includes("pipeline_v2_run_mutation_lock")).toBe(false);
  // The human reporter prints the busy line for every mutating command and
  // keeps the controlled suspensions ahead of the failure branches.
  expect(mainModule.includes('"run_busy"')).toBe(true);
  const busyIndex = mainModule.indexOf('"run_busy"');
  const waitingIndex = mainModule.indexOf("outcome.waiting === true");
  const planReadyIndex = mainModule.indexOf("outcome.planReady === true");
  expect(waitingIndex).toBeGreaterThan(0);
  expect(planReadyIndex).toBeGreaterThan(waitingIndex);
  expect(busyIndex).toBeGreaterThan(planReadyIndex);
});
