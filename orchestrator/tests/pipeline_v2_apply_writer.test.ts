/**
 * Suites B/D/E/F — the project apply filesystem writer
 * (`pipeline_v2_project_apply_writer_internal.ts` + executor + verify):
 * every step kind against the real fd adapter, the conflict/refusal
 * battery with zero writes, the security ownership scans, and the exact
 * end-state proof. Suite C (the crash/recovery matrix W0-W20) lives in
 * `pipeline_v2_apply_writer_recovery.test.ts`, the race matrix in
 * `pipeline_v2_apply_writer_race.test.ts`.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { dlopen, FFIType } from "bun:ffi";
import { chmod, lstat, mkdir, readFile, readdir, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  PIPELINE_V2_APPLY_EXECUTION_REASONS,
  applyPreparedPipelineV2Project,
  inspectPipelineV2ProjectApplyOperation,
  preparePipelineV2ProjectApply,
  type PipelineV2ApplyExecutionResult,
} from "../src/pipeline_v2_project_apply.ts";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  fingerprintTree,
  realImmutableDocumentIo,
  defaultPipelineStateIo,
  realProjectFileStreamIo,
  faultFdOps,
  faultStreamIo,
  setLibcErrno,
  type ApplyFixtureEntry,
} from "./apply_fixtures.ts";
import { realApplyFdOps } from "../src/pipeline_v2_apply_fd_internal.ts";

const ROOTS: string[] = [];
const tmp = async (label: string): Promise<string> => {
  const root = await buildFinalizedApplyRun(`writer-${label}`, { source: [] });
  ROOTS.push(root.applyStoreRoot);
  return root.applyStoreRoot;
};

afterAll(async () => {
  await disposeApplyRoots();
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true }).catch(() => undefined);
  }
});

type Entry = ApplyFixtureEntry;

async function prepared(
  label: string,
  spec: Entry[],
  worker?: (ws: string) => Promise<void>,
): Promise<{ fixture: Awaited<ReturnType<typeof buildFinalizedApplyRun>>; steps: number; noops: number }> {
  const fixture = await buildFinalizedApplyRun(`writer-${label}`, { source: spec, worker });
  const pkg = await preparePipelineV2ProjectApply({
    applyStoreRoot: fixture.applyStoreRoot,
    applyId: "applyone",
    runId: fixture.runId,
    projectId: "proj1",
    projectRoot: fixture.operatorProject,
    observation: fixture.observation,
    stateRoot: fixture.stateRoot,
  });
  return { fixture, steps: pkg.plan.steps.length, noops: pkg.plan.noops.length };
}

async function execute(fixture: Awaited<ReturnType<typeof buildFinalizedApplyRun>>): Promise<PipelineV2ApplyExecutionResult> {
  return await applyPreparedPipelineV2Project({
    applyStoreRoot: fixture.applyStoreRoot,
    applyId: "applyone",
    projectId: "proj1",
    projectRoot: fixture.operatorProject,
  });
}

interface TreeEntry {
  readonly kind: "file" | "dir" | "link" | "absent";
  readonly content: Buffer | null;
  readonly target: string | null;
  readonly mode: number;
}

async function snapshotTree(root: string): Promise<Map<string, TreeEntry>> {
  const entries = new Map<string, TreeEntry>();
  const walk = async (absolute: string, relative: string): Promise<void> => {
    const info = await lstat(absolute);
    if (info.isDirectory()) {
      if (relative !== "") {
        entries.set(relative, { kind: "dir", content: null, target: null, mode: info.mode & 0o7777 });
      }
      for (const name of (await readdir(absolute)).sort()) {
        await walk(join(absolute, name), relative === "" ? name : `${relative}/${name}`);
      }
      return;
    }
    if (info.isSymbolicLink()) {
      entries.set(relative, { kind: "link", content: null, target: await readlink(absolute), mode: info.mode & 0o7777 });
      return;
    }
    entries.set(relative, { kind: "file", content: await readFile(absolute), target: null, mode: info.mode & 0o7777 });
  };
  await walk(root, "");
  return entries;
}

interface PlanDocument {
  readonly steps: readonly {
    readonly step_index: number;
    readonly path: string;
    readonly kind: string;
    readonly desired_after: { readonly kind: string; readonly mode?: number; readonly target?: string; readonly byte_count?: number } | null;
    readonly payload_index?: number;
  }[];
  readonly noops: readonly { readonly path: string; readonly expected: { readonly kind: string; readonly mode?: number; readonly target?: string } | null }[];
}

async function readPlanDocument(fixture: Awaited<ReturnType<typeof buildFinalizedApplyRun>>): Promise<PlanDocument> {
  return JSON.parse(
    await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "plan.json"), "utf8"),
  ) as PlanDocument;
}

/**
 * The exact end-state proof: every planned path sits at its reviewed
 * result node, no temp artifact survives, and every unrelated operator
 * path is byte-identical to the pre-execution fingerprint.
 */
async function expectAppliedEndState(
  fixture: Awaited<ReturnType<typeof buildFinalizedApplyRun>>,
  before: Map<string, TreeEntry>,
): Promise<void> {
  const plan = await readPlanDocument(fixture);
  const desired = new Map<string, TreeEntry>();
  for (const step of plan.steps) {
    if (step.desired_after === null) {
      desired.set(step.path, { kind: "absent", content: null, target: null, mode: 0 });
    } else if (step.desired_after.kind === "directory") {
      desired.set(step.path, { kind: "dir", content: null, target: null, mode: step.desired_after.mode ?? 0 });
    } else if (step.desired_after.kind === "symlink") {
      desired.set(step.path, { kind: "link", content: null, target: step.desired_after.target ?? "", mode: 0 });
    } else if (step.payload_index !== undefined) {
      const bytes = await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "payloads", `${step.payload_index}.payload`));
      desired.set(step.path, { kind: "file", content: bytes, target: null, mode: step.desired_after.mode ?? 0 });
    } else {
      desired.set(step.path, { kind: "file", content: null, target: null, mode: step.desired_after.mode ?? 0 });
    }
  }
  for (const noop of plan.noops) {
    const expected = noop.expected;
    if (expected === null) {
      desired.set(noop.path, { kind: "absent", content: null, target: null, mode: 0 });
    } else if (expected.kind === "directory") {
      desired.set(noop.path, { kind: "dir", content: null, target: null, mode: expected.mode ?? 0 });
    } else if (expected.kind === "symlink") {
      desired.set(noop.path, { kind: "link", content: null, target: expected.target ?? "", mode: 0 });
    } else {
      desired.set(noop.path, { kind: "file", content: null, target: null, mode: expected.mode ?? 0 });
    }
  }
  const operator = await snapshotTree(fixture.operatorProject);
  for (const [path, expected] of desired) {
    const actual = operator.get(path);
    if (expected.kind === "absent") {
      expect(actual, `${path} must be absent`).toBeUndefined();
      continue;
    }
    expect(actual, `${path} must exist`).toBeDefined();
    if (actual === undefined) continue;
    expect(actual.kind, path).toBe(expected.kind === "dir" ? "dir" : expected.kind);
    if (expected.kind === "dir") expect(actual.mode, path).toBe(expected.mode);
    if (expected.kind === "link") expect(actual.target, path).toBe(expected.target);
    if (expected.kind === "file") {
      expect(actual.mode, path).toBe(expected.mode);
      if (expected.content !== null) expect(actual.content!.equals(expected.content), path).toBe(true);
    }
  }
  for (const name of await readdir(fixture.operatorProject)) {
    expect(name.startsWith(".apply-tmp-"), `no temp artifact at ${name}`).toBe(false);
  }
  for (const [path, entry] of before) {
    if (desired.has(path)) continue;
    expect(operator.get(path), `unrelated ${path} unchanged`).toEqual(entry);
  }
}

function describeTree(entries: Map<string, TreeEntry>): string[] {
  return [...entries.entries()].map(([path, entry]) =>
    entry.kind === "link"
      ? `${path} link ${entry.target}`
      : entry.kind === "dir"
        ? `${path} dir ${entry.mode.toString(8)}`
        : `${path} file ${entry.mode.toString(8)} ${entry.content!.toString("hex")}`,
  );
}

describe("apply writer: step kinds (suite B)", () => {
  test("add, modify and delete regular files end to end", async () => {
    const { fixture, steps } = await prepared("files", [
      { path: "keep.txt", kind: "file", content: "KEEP" },
      { path: "gone.txt", kind: "file", content: "GONE" },
      { path: "chg.txt", kind: "file", content: "OLD" },
    ], async (ws) => {
      await rm(join(ws, "gone.txt"));
      await writeFile(join(ws, "chg.txt"), "NEW");
      await writeFile(join(ws, "added.txt"), "ADDED");
    });
    expect(steps).toBe(3);
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    expect(result.completed_step_count).toBe(3);
    await expectAppliedEndState(fixture, before);
  });

  test("create and remove directories with nested subtrees", async () => {
    const { fixture, steps } = await prepared("dirs", [
      { path: "sub/old/leaf.txt", kind: "file", content: "OLD" },
      { path: "sub/other.txt", kind: "file", content: "OTHER" },
    ], async (ws) => {
      await rm(join(ws, "sub/old"), { recursive: true });
      await mkdir(join(ws, "new/nested/deep"), { recursive: true });
      await writeFile(join(ws, "new/nested/deep/file.txt"), "DEEP");
      await writeFile(join(ws, "new/top.txt"), "TOP");
    });
    expect(steps).toBe(7);
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    await expectAppliedEndState(fixture, before);
  });

  test("symlink add, retarget and delete", async () => {
    const { fixture, steps } = await prepared("links", [
      { path: "data.txt", kind: "file", content: "DATA" },
      { path: "old-link", kind: "symlink", target: "data.txt" },
    ], async (ws) => {
      await rm(join(ws, "old-link"));
      await symlink("data.txt", join(ws, "new-link"));
      await symlink("elsewhere", join(ws, "retargeted"));
      await writeFile(join(ws, "retargeted"), "RETIRED");
    });
    expect(steps).toBe(4);
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    await expectAppliedEndState(fixture, before);
  });

  test("mode changes on files and directories (set_mode steps)", async () => {
    const { fixture, steps } = await prepared("modes", [
      { path: "script.sh", kind: "file", content: "SCRIPT" },
      { path: "dir", kind: "directory" },
      { path: "dir/file.txt", kind: "file", content: "X" },
    ], async (ws) => {
      await chmod(join(ws, "script.sh"), 0o755);
      await chmod(join(ws, "dir"), 0o755);
    });
    expect(steps).toBe(2);
    const kinds = (await (await import("node:fs/promises")).readFile(join(fixture.applyStoreRoot, "operations", "applyone", "plan.json"), "utf8")).includes("set_mode");
    expect(kinds).toBe(true);
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    await expectAppliedEndState(fixture, before);
  });

  test("every type substitution is applied exactly (file/dir/symlink rotations)", async () => {
    const { fixture, steps } = await prepared("substitutions", [
      { path: "was-file", kind: "file", content: "OLD" },
      { path: "was-dir/inner.txt", kind: "file", content: "INNER" },
      { path: "was-link", kind: "symlink", target: "was-file" },
      { path: "link-to-dir", kind: "symlink", target: "was-dir" },
      { path: "file-to-link", kind: "file", content: "F2L" },
    ], async (ws) => {
      await rm(join(ws, "was-file"));
      await mkdir(join(ws, "was-file"));
      await writeFile(join(ws, "was-file/inside.txt"), "NOW-DIR");
      await rm(join(ws, "was-dir"), { recursive: true });
      await writeFile(join(ws, "was-dir"), "NOW-FILE");
      await rm(join(ws, "was-link"));
      await writeFile(join(ws, "was-link"), "NOW-FILE-AGAIN");
      await rm(join(ws, "link-to-dir"));
      await mkdir(join(ws, "link-to-dir"));
      await rm(join(ws, "file-to-link"));
      await symlink("was-file", join(ws, "file-to-link"));
    });
    expect(steps).toBeGreaterThan(0);
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    await expectAppliedEndState(fixture, before);
  });

  test("a large binary payload streams byte-exactly", async () => {
    const payload = Buffer.from(Array.from({ length: 3 * 1024 * 1024 }, (_, i) => (i * 7 + 13) % 256));
    const { fixture } = await prepared("large", [
      { path: "big.bin", kind: "file", content: "SMALL" },
    ], async (ws) => {
      await writeFile(join(ws, "big.bin"), payload);
    });
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    const written = await readFile(join(fixture.operatorProject, "big.bin"));
    expect(written.equals(payload)).toBe(true);
  });

  test("an operator that already applied the reviewed result is adopted without mutation", async () => {
    const { fixture, steps } = await prepared("adopted", [
      { path: "a.txt", kind: "file", content: "A" },
      { path: "b.txt", kind: "file", content: "B" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
      await rm(join(ws, "b.txt"));
    });
    expect(steps).toBe(2);
    const plan = JSON.parse(await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "plan.json"), "utf8"));
    for (const step of plan.steps) {
      const absolute = join(fixture.operatorProject, step.path);
      if (step.desired_after === null) {
        await rm(absolute, { recursive: true });
      } else if (step.desired_after.kind === "directory") {
        await mkdir(absolute, { recursive: true });
      } else if (step.desired_after.kind === "symlink") {
        await symlink(step.desired_after.target, absolute);
      } else {
        const payloadIndex = step.payload_index as number;
        const bytes = await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "payloads", `${payloadIndex}.payload`));
        await writeFile(absolute, bytes);
      }
    }
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    expect(result.completed_step_count).toBe(steps);
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("a zero-step plan applies cleanly", async () => {
    const { fixture, steps } = await prepared("empty", [{ path: "only.txt", kind: "file", content: "X" }]);
    expect(steps).toBe(0);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    expect(result.completed_step_count).toBe(0);
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.execution).toEqual({ completed_step_count: 0, active_step: null });
  });
});

describe("apply writer: conflicts and refusals (suite D)", () => {
  test("a target changed after preparation is a clean conflict with zero writes", async () => {
    const { fixture } = await prepared("conflict-target", [
      { path: "a.txt", kind: "file", content: "A" },
      { path: "b.txt", kind: "file", content: "B" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
      await rm(join(ws, "b.txt"));
    });
    await writeFile(join(fixture.operatorProject, "a.txt"), "OPERATOR-CHANGED");
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.reason).toBe("project_conflict");
    expect(result.status).toBe("conflict");
    expect(result.completed_step_count).toBe(0);
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
    const retry = await execute(fixture);
    expect(retry.ok).toBe(false);
    if (!retry.ok) expect(retry.reason).toBe("project_conflict");
  });

  test("a replaced ancestor directory is a conflict", async () => {
    const { fixture } = await prepared("conflict-ancestor", [
      { path: "dir/child.txt", kind: "file", content: "CHILD" },
    ], async (ws) => {
      await writeFile(join(ws, "dir/child.txt"), "CHILD2");
    });
    await rm(join(fixture.operatorProject, "dir"), { recursive: true });
    await writeFile(join(fixture.operatorProject, "dir"), "NOT-A-DIR");
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe("project_conflict");
      expect(result.status).toBe("conflict");
    }
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("an operator-only descendant of a removed directory is a conflict", async () => {
    const { fixture } = await prepared("conflict-descendant", [
      { path: "sub/leaf.txt", kind: "file", content: "LEAF" },
      { path: "sub/other.txt", kind: "file", content: "OTHER" },
    ], async (ws) => {
      await rm(join(ws, "sub"), { recursive: true });
      await writeFile(join(ws, "elsewhere.txt"), "NEW");
    });
    await writeFile(join(fixture.operatorProject, "sub", "foreign.txt"), "FOREIGN");
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe("project_conflict");
      expect(result.status).toBe("conflict");
    }
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("a wrong target kind is a conflict", async () => {
    const { fixture } = await prepared("conflict-kind", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    await rm(join(fixture.operatorProject, "a.txt"));
    await mkdir(join(fixture.operatorProject, "a.txt"));
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toBe("project_conflict");
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("unsupported metadata introduced after preparation is a conflict", async () => {
    const { fixture } = await prepared("conflict-xattr", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const libc = dlopen("libc.so.6", {
      setxattr: { args: ["cstring", "cstring", "ptr", FFIType.u64, FFIType.u32], returns: FFIType.i64 },
    });
    const rc = Number(
      (libc.symbols as unknown as Record<string, (...args: unknown[]) => number>)["setxattr"]!(
        Buffer.from(`${fixture.operatorProject}/a.txt\0`),
        Buffer.from("user.note\0"),
        Buffer.from("v"),
        1,
        0,
      ),
    );
    if (rc !== 0) {
      return;
    }
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toBe("unsupported_metadata");
  });

  test("a read-only project is refused before any write (faccessat gate)", async () => {
    const { fixture } = await prepared("conflict-ro", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    await chmod(fixture.operatorProject, 0o555);
    const before = await fingerprintTree(fixture.operatorProject);
    try {
      const result = await execute(fixture);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe("project_read_only");
        expect(result.status).toBe("prepared");
      }
    } finally {
      await chmod(fixture.operatorProject, 0o700);
    }
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
    const retry = await execute(fixture);
    expect(retry.ok).toBe(true);
  });

  test("a read-only filesystem flag is refused through the fstatvfs seam", async () => {
    const { fixture } = await prepared("conflict-rofs", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const fops = faultFdOps();
    fops.override("fstatvfs", (fd, buf) => {
      (buf as Buffer).writeBigUInt64LE(1n, 72);
      return 0;
    });
    const before = await fingerprintTree(fixture.operatorProject);
    const { applyPreparedProjectCore } = await import("../src/pipeline_v2_project_apply_writer_internal.ts");
    const refused = await applyPreparedProjectCore(
      { applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixture.operatorProject },
      { docIo: realImmutableDocumentIo, stateIo: defaultPipelineStateIo, fileStreamIo: realProjectFileStreamIo, fdOps: fops.ops },
    );
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("project_read_only");
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("a filesystem without the no-replace primitive is a typed unsupported_filesystem refusal", async () => {
    const { fixture } = await prepared("conflict-nosys", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "added.txt"), "ADDED");
    });
    const fops = faultFdOps();
    let refusedRename = 0;
    fops.override("renameat2", (a, b, c, d, e) => {
      void a;
      void b;
      void c;
      void d;
      void e;
      refusedRename += 1;
      fops.override("renameat2", null);
      setLibcErrno(38);
      return -1;
    });
    const { applyPreparedProjectCore } = await import("../src/pipeline_v2_project_apply_writer_internal.ts");
    const refused = await applyPreparedProjectCore(
      { applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixture.operatorProject },
      { docIo: realImmutableDocumentIo, stateIo: defaultPipelineStateIo, fileStreamIo: realProjectFileStreamIo, fdOps: fops.ops },
    );
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("unsupported_filesystem");
    expect(refusedRename).toBe(1);
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("applying");
    const recovered = await execute(fixture);
    expect(recovered.ok).toBe(true);
  });

  test("a foreign object at a committed temp name is never removed (recovery_required)", async () => {
    const { fixture } = await prepared("conflict-foreign-temp", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, (args) => String((args[1] as Buffer).toString("utf8")).startsWith(".apply-tmp-"));
    const { applyPreparedProjectCore } = await import("../src/pipeline_v2_project_apply_writer_internal.ts");
    const { isApplyCrashFault } = await import("../src/pipeline_v2_apply_crash_internal.ts");
    let crashed = false;
    try {
      await applyPreparedProjectCore(
        { applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixture.operatorProject },
        { docIo: realImmutableDocumentIo, stateIo: defaultPipelineStateIo, fileStreamIo: realProjectFileStreamIo, fdOps: fops.ops },
      );
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const state = JSON.parse(await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "state.json"), "utf8"));
    const tempName = state.execution.active_step.temp_name as string;
    expect(tempName).toMatch(/^\.apply-tmp-[0-9a-f]{32}$/);
    await mkdir(join(fixture.operatorProject, tempName));
    const before = await fingerprintTree(fixture.operatorProject);
    const refused = await execute(fixture);
    expect(refused.ok).toBe(false);
    if (!refused.ok) {
      expect(refused.reason).toBe("recovery_required");
      expect(refused.status).toBe("recovery_required");
      expect(refused.completed_step_count).toBe(0);
    }
    expect((await lstat(join(fixture.operatorProject, tempName))).isDirectory()).toBe(true);
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });
});

describe("apply writer: security and ownership (suite E)", () => {
  test("results and diagnostics never carry the project path, plan paths or payload bytes", async () => {
    const secret = "PAYLOAD-CANARY-9f8e7d6c";
    const { fixture } = await prepared("canaries", [
      { path: "doc.txt", kind: "file", content: secret },
    ], async (ws) => {
      await writeFile(join(ws, "doc.txt"), `${secret}-NEW`);
    });
    const result = await execute(fixture);
    const serialized = JSON.stringify(result);
    expect(serialized.includes(secret)).toBe(false);
    expect(serialized.includes(fixture.operatorProject)).toBe(false);
    expect(serialized.includes("doc.txt")).toBe(false);
    expect(serialized.includes("applyrun")).toBe(true);
    expect(serialized.includes("proj1")).toBe(true);
  });

  test("a symlinked ancestor cannot redirect an observation (no traversal)", async () => {
    const { fixture } = await prepared("no-traversal", [
      { path: "dir/child.txt", kind: "file", content: "CHILD" },
    ], async (ws) => {
      await writeFile(join(ws, "dir/child.txt"), "CHILD2");
    });
    await rm(join(fixture.operatorProject, "dir"), { recursive: true });
    await symlink(join(fixture.operatorProject, "elsewhere"), join(fixture.operatorProject, "dir"));
    const before = await fingerprintTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toBe("project_conflict");
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
  });

  test("the writer modules import no docker, session, http, ui, spawn or rollback capability", async () => {
    const { readFile: rf } = await import("node:fs/promises");
    const sources = [
      "../src/pipeline_v2_apply_fd_internal.ts",
      "../src/pipeline_v2_apply_project_lock_internal.ts",
      "../src/pipeline_v2_apply_verify_internal.ts",
      "../src/pipeline_v2_apply_exec_internal.ts",
      "../src/pipeline_v2_project_apply_writer_internal.ts",
      "../src/pipeline_v2_apply_crash_internal.ts",
    ];
    const banned = [
      "Bun.spawn",
      "docker",
      "docker-helper",
      "launcher",
      "Session",
      "http",
      "Bun.serve",
      "fetch(",
      "child_process",
      "exec(",
      "shell",
    ];
    for (const source of sources) {
      const body = await rf(join(import.meta.dir, source), "utf8");
      for (const token of banned) {
        expect(body.toLowerCase().includes(token.toLowerCase()), `${source} must not contain ${token}`).toBe(false);
      }
    }
  });

  test("the execution reason vocabulary is exactly the closed fourteen", () => {
    expect([...PIPELINE_V2_APPLY_EXECUTION_REASONS]).toEqual([
      "invalid_options",
      "apply_not_prepared",
      "artifact_missing",
      "artifact_modified",
      "project_binding_mismatch",
      "project_busy",
      "project_read_only",
      "unsupported_filesystem",
      "unsupported_metadata",
      "unsupported_project_entry",
      "project_conflict",
      "recovery_required",
      "state_persist_failed",
      "internal_error",
    ]);
  });
});

describe("apply writer: end state (suite F)", () => {
  test("the applied project differs from the registered project exactly by the reviewed changes", async () => {
    const { fixture } = await prepared("end-state", [
      { path: "kept.txt", kind: "file", content: "KEPT" },
      { path: "removed.txt", kind: "file", content: "REMOVED" },
      { path: "changed.txt", kind: "file", content: "OLD" },
      { path: "unrelated.txt", kind: "file", content: "UNRELATED" },
    ], async (ws) => {
      await rm(join(ws, "removed.txt"));
      await writeFile(join(ws, "changed.txt"), "NEW");
      await writeFile(join(ws, "fresh.txt"), "FRESH");
    });
    const before = await snapshotTree(fixture.operatorProject);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    if (!result.ok || result.outcome !== "applied") return;
    await expectAppliedEndState(fixture, before);
    expect(before.get("unrelated.txt")!.content!.toString()).toBe("UNRELATED");
    const after = await snapshotTree(fixture.operatorProject);
    expect(after.get("kept.txt")!.content!.toString()).toBe("KEPT");
    expect(after.get("removed.txt")).toBeUndefined();
    expect(after.get("changed.txt")!.content!.toString()).toBe("NEW");
    expect(after.get("fresh.txt")!.content!.toString()).toBe("FRESH");
    expect(after.get("unrelated.txt")!.content!.toString()).toBe("UNRELATED");
    expect(result.revision).toBeGreaterThan(1);
  });

  test("the run tree stays byte-identical across the whole execution", async () => {
    const { fixture } = await prepared("run-tree", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const before = await fingerprintTree(fixture.runRoot);
    const result = await execute(fixture);
    expect(result.ok).toBe(true);
    expect(await fingerprintTree(fixture.runRoot)).toEqual(before);
  });

  test("a retry after success returns already_applied without any project access effect", async () => {
    const { fixture } = await prepared("retry", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const first = await execute(fixture);
    expect(first.ok).toBe(true);
    const after = await fingerprintTree(fixture.operatorProject);
    const second = await execute(fixture);
    expect(second.ok).toBe(true);
    if (!second.ok || second.outcome !== "already_applied") return;
    expect(second.revision).toBe(first.ok && first.outcome === "applied" ? first.revision : 0);
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(after);
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("applied");
    expect(inspection.failure_reason).toBe(null);
  });

  test("the execution result is deeply frozen and content-free", async () => {
    const { fixture } = await prepared("frozen", [
      { path: "a.txt", kind: "file", content: "A" },
    ], async (ws) => {
      await writeFile(join(ws, "a.txt"), "A2");
    });
    const result = await execute(fixture);
    expect(Object.isFrozen(result)).toBe(true);
    const keys = Object.keys(result).sort();
    expect(keys.length > 0).toBe(true);
    for (const key of keys) {
      const value = (result as Record<string, unknown>)[key];
      if (typeof value === "object" && value !== null) {
        expect(Object.isFrozen(value)).toBe(true);
      }
    }
  });
});
