/**
 * Suite A — the project apply lock (`pipeline_v2_apply_project_lock_internal.ts`):
 * cross-process exclusion over one canonical project root, kernel release
 * on process death, independence of different projects, the absence of
 * any lock artifact, and the typed refusals.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, mkdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  PipelineV2ApplyProjectLockError,
  acquirePipelineV2ApplyProjectLock,
} from "../src/pipeline_v2_apply_project_lock_internal.ts";

const ROOTS: string[] = [];
const tmp = async (): Promise<string> => {
  const root = await mkdtemp(join(tmpdir(), "apply-lock-"));
  ROOTS.push(root);
  return root;
};

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

const PROBE = new URL("./apply_lock_probe.ts", import.meta.url).pathname;

describe("apply project lock", () => {
  test("acquire, hold and release; reacquire after release", () => {
    const root = join(tmpdir(), "apply-lock-static");
    return (async () => {
      await mkdir(root, { recursive: true });
      const first = acquirePipelineV2ApplyProjectLock(root);
      expect(first.busy).toBe(false);
      if (first.busy) return;
      const second = acquirePipelineV2ApplyProjectLock(root);
      expect(second.busy).toBe(true);
      first.lock.release();
      first.lock.release();
      const third = acquirePipelineV2ApplyProjectLock(root);
      expect(third.busy).toBe(false);
      if (!third.busy) third.lock.release();
    })();
  });

  test("cross-process exclusion: a second process sees the lock busy and wins after release", async () => {
    const root = await tmp();
    const holder = spawn(process.execPath, [PROBE, "hold", root], { stdio: ["pipe", "inherit", "inherit"] });
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 2000);
      (holder.stdout as import("node:stream").Readable | null)?.once("data", () => {
        clearTimeout(timer);
        resolve();
      });
    });
    const during = spawnSync(process.execPath, [PROBE, "probe", root], { encoding: "utf8" });
    expect(during.stdout.trim()).toBe("busy");
    holder.stdin!.end("release");
    const exited = new Promise<void>((resolve) => holder.once("exit", () => resolve()));
    await exited;
    const after = spawnSync(process.execPath, [PROBE, "probe", root], { encoding: "utf8" });
    expect(after.stdout.trim()).toBe("free");
  });

  test("the lock frees on process death without any cleanup step", async () => {
    const root = await tmp();
    const holder = spawn(process.execPath, [PROBE, "hold", root], { stdio: ["pipe", "inherit", "inherit"] });
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 2000);
      (holder.stdout as import("node:stream").Readable | null)?.once("data", () => {
        clearTimeout(timer);
        resolve();
      });
    });
    holder.kill("SIGKILL");
    await new Promise<void>((resolve) => holder.once("exit", () => resolve()));
    const after = spawnSync(process.execPath, [PROBE, "probe", root], { encoding: "utf8" });
    expect(after.stdout.trim()).toBe("free");
  });

  test("different projects are independent; the lock key is the root identity", async () => {
    const one = await tmp();
    const two = await tmp();
    const first = acquirePipelineV2ApplyProjectLock(one);
    const second = acquirePipelineV2ApplyProjectLock(two);
    expect(first.busy).toBe(false);
    expect(second.busy).toBe(false);
    if (!first.busy) first.lock.release();
    if (!second.busy) second.lock.release();
  });

  test("the lock leaves no artifact inside or beside the project", async () => {
    const root = await tmp();
    await writeFile(join(root, "data.txt"), "x");
    const { readdir } = await import("node:fs/promises");
    const before = (await readdir(root)).sort();
    const lock = acquirePipelineV2ApplyProjectLock(root);
    expect(lock.busy).toBe(false);
    const during = (await readdir(root)).sort();
    expect(during).toEqual(before);
    if (!lock.busy) lock.lock.release();
    const after = (await readdir(root)).sort();
    expect(after).toEqual(before);
  });

  test("the held lock exposes the root identity for the post-lock re-verification", async () => {
    const root = await tmp();
    const lock = acquirePipelineV2ApplyProjectLock(root);
    expect(lock.busy).toBe(false);
    if (lock.busy) return;
    const { statSync } = await import("node:fs");
    const stat = statSync(root);
    expect(lock.lock.identity.dev).toBe(stat.dev);
    expect(lock.lock.identity.ino).toBe(stat.ino);
    lock.lock.release();
  });

  test("a symlinked, missing or non-directory root is refused fail-closed", async () => {
    const root = await tmp();
    await symlink(join(root, "elsewhere"), join(root, "link"));
    expect(() => acquirePipelineV2ApplyProjectLock(join(root, "link"))).toThrow(PipelineV2ApplyProjectLockError);
    try {
      acquirePipelineV2ApplyProjectLock(join(root, "link"));
    } catch (cause) {
      expect((cause as PipelineV2ApplyProjectLockError).reason).toBe("root_invalid");
    }
    expect(() => acquirePipelineV2ApplyProjectLock(join(root, "missing"))).toThrow(PipelineV2ApplyProjectLockError);
    await writeFile(join(root, "file.txt"), "x");
    expect(() => acquirePipelineV2ApplyProjectLock(join(root, "file.txt"))).toThrow(PipelineV2ApplyProjectLockError);
    expect(() => acquirePipelineV2ApplyProjectLock("relative/path")).toThrow(PipelineV2ApplyProjectLockError);
  });
});
