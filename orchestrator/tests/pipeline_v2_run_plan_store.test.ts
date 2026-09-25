import type { Stats } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  PipelineV2RunPlanStoreError,
  loadPipelineV2TaskRevisionWithIo,
  publishPipelineV2PlanRevisionWithIo,
  publishPipelineV2TaskRevisionWithIo,
  realRunPlanStoreIo,
} from "../src/pipeline_v2_run_plan_store_internal.ts";
import type { ImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import {
  loadPipelineV2PlanRevision,
  loadPipelineV2TaskRevision,
  publishPipelineV2PlanRevision,
  publishPipelineV2TaskRevision,
} from "../src/pipeline_v2_run_plan_store.ts";
import {
  PipelineV2RunPlanManifestError,
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
} from "../src/pipeline_v2_run_plan_manifests.ts";

type StoreIo = ImmutableDocumentIo;

interface Fixture {
  root: string;
  runRoot: string;
  runPlan: string;
  plans: string;
  tasks: string;
}

const RUN_ID = "run-1";

async function setup(runId = RUN_ID): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-run-plan-store-"));
  const runRoot = join(root, runId);
  await mkdir(runRoot, { mode: 0o700 });
  return {
    root,
    runRoot,
    runPlan: join(runRoot, "run-plan"),
    plans: join(runRoot, "run-plan", "plans"),
    tasks: join(runRoot, "run-plan", "tasks"),
  };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

const hex = (char: string): string => char.repeat(64);

function taskValue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: "task-1",
    revision: 1,
    previous_sha256: null,
    origin: "planning_proposal",
    body: "Implement the acceptance test parser",
    ...overrides,
  };
}

function planValue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: hex("b") },
    origin_execution: 10,
    stages: [
      {
        id: "implementation",
        template: "development",
        tasks: [
          { id: "task-1", revision: 1, sha256: hex("1"), depends_on: [] },
          { id: "task-2", revision: 1, sha256: hex("2"), depends_on: ["task-1"] },
        ],
      },
    ],
    ...overrides,
  };
}

const CANARY = "CANARY_secret_body_value";

function taskWithCanary(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return taskValue({ body: `body with ${CANARY}`, ...overrides });
}

function expectStoreError(
  cause: unknown,
  outcome: "not_published" | "durability_unknown",
  reason: "invalid_layout" | "conflict" | "io_failure",
  messageFragment = "",
): PipelineV2RunPlanStoreError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanStoreError);
  const error = cause as PipelineV2RunPlanStoreError;
  expect(error.outcome).toBe(outcome);
  expect(error.reason).toBe(reason);
  if (messageFragment !== "") {
    expect(error.message).toContain(messageFragment);
  }
  return error;
}

function injectedFailure(code?: string): Error {
  return Object.assign(new Error("injected failure"), code === undefined ? {} : { code });
}

function ioFaulting(hook: keyof StoreIo, failure: unknown): StoreIo {
  return Object.freeze({
    ...realRunPlanStoreIo,
    [hook]: async () => {
      throw failure;
    },
  }) as unknown as StoreIo;
}

function ioReplacing(hook: keyof StoreIo, implementation: unknown): StoreIo {
  return Object.freeze({
    ...realRunPlanStoreIo,
    [hook]: implementation,
  }) as unknown as StoreIo;
}

async function dirMode(path: string): Promise<number> {
  return (await lstat(path)).mode & 0o777;
}

async function fileIdentity(
  path: string,
): Promise<{ dev: number; ino: number; mtimeMs: number; mode: number }> {
  const info = await lstat(path);
  return { dev: info.dev, ino: info.ino, mtimeMs: info.mtimeMs, mode: info.mode & 0o777 };
}

function tempFileNames(dir: string): Promise<string[]> {
  return readdir(dir).then((names) => names.filter((name) => name.startsWith(".run-plan-publish-")));
}

async function makeFifo(path: string): Promise<void> {
  const child = Bun.spawnSync(["mkfifo", path]);
  if (child.exitCode !== 0) {
    throw new Error("mkfifo failed");
  }
}

/** Barrier IO: both link() calls arrive before either proceeds (no sleeps). */
function linkBarrierIo(): StoreIo {
  let arrived = 0;
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  return ioReplacing("link", async (from: string, to: string) => {
    arrived += 1;
    if (arrived === 2) {
      release();
    }
    await gate;
    return realRunPlanStoreIo.link(from, to);
  });
}

// --- 1-2. round-trips --------------------------------------------------------

test("1. task publish/load round-trip: exact path, canonical bytes, binding", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const expectedPath = join(fixture.tasks, "task-1", "1.json");
    expect(published.task_path).toBe(expectedPath);
    expect(published.task.manifest.run_id).toBe(RUN_ID);
    expect(published.task.manifest.task_id).toBe("task-1");
    expect(published.task.manifest.revision).toBe(1);
    const loaded = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1);
    expect(loaded?.task_path).toBe(expectedPath);
    expect(loaded?.task.sha256).toBe(published.task.sha256);
    expect(loaded?.task.canonical_json).toBe(published.task.canonical_json);
    expect(Object.isFrozen(loaded)).toBe(true);
    expect(Object.isFrozen(loaded?.task.manifest)).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("2. plan publish/load round-trip: exact path, canonical bytes, binding", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const expectedPath = join(fixture.plans, "1.json");
    expect(published.plan_path).toBe(expectedPath);
    expect(published.plan.manifest.run_id).toBe(RUN_ID);
    expect(published.plan.manifest.revision).toBe(1);
    const loaded = await loadPipelineV2PlanRevision(fixture.runRoot, 1);
    expect(loaded?.plan_path).toBe(expectedPath);
    expect(loaded?.plan.sha256).toBe(published.plan.sha256);
    expect(loaded?.plan.canonical_json).toBe(published.plan.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

// --- 3-4. canonical bytes and modes ------------------------------------------

test("3. stored bytes are exactly the canonical JSON without a trailing newline", async () => {
  const fixture = await setup();
  try {
    const publishedTask = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const storedTask = await readFile(publishedTask.task_path, "utf8");
    expect(storedTask).toBe(publishedTask.task.canonical_json);
    expect(storedTask.endsWith("\n")).toBe(false);
    expect(storedTask).toBe(JSON.stringify(JSON.parse(storedTask)));
    const publishedPlan = await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const storedPlan = await readFile(publishedPlan.plan_path, "utf8");
    expect(storedPlan).toBe(publishedPlan.plan.canonical_json);
    expect(storedPlan.endsWith("\n")).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("4. directory components have mode 0700, manifest files mode 0600", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    expect(await dirMode(fixture.runPlan)).toBe(0o700);
    expect(await dirMode(fixture.tasks)).toBe(0o700);
    expect(await dirMode(join(fixture.tasks, "task-1"))).toBe(0o700);
    expect(await dirMode(fixture.plans)).toBe(0o700);
    expect((await fileIdentity(join(fixture.tasks, "task-1", "1.json"))).mode).toBe(0o600);
    expect((await fileIdentity(join(fixture.plans, "1.json"))).mode).toBe(0o600);
  } finally {
    await dispose(fixture);
  }
});

// --- 5. missing tree load ------------------------------------------------------

test("5. missing tree load returns null and leaves the filesystem untouched", async () => {
  const fixture = await setup();
  try {
    expect(await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1)).toBeNull();
    expect(await loadPipelineV2PlanRevision(fixture.runRoot, 1)).toBeNull();
    expect(await readdir(fixture.runRoot)).toEqual([]);
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    expect(await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1)).toBeNull();
    expect(await loadPipelineV2PlanRevision(fixture.runRoot, 2)).toBeNull();
    expect(await readdir(fixture.plans).then((names) => names.filter((n) => n.endsWith(".json")))).toEqual(["1.json"]);
  } finally {
    await dispose(fixture);
  }
});

// --- 6-9. idempotency and conflicts -------------------------------------------

test("6. repeated exact task publication preserves inode, mtime, bytes", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const before = await fileIdentity(first.task_path);
    const second = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const after = await fileIdentity(first.task_path);
    expect(second.task.sha256).toBe(first.task.sha256);
    expect(after.dev).toBe(before.dev);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(after.mode).toBe(before.mode);
    expect(await readFile(first.task_path, "utf8")).toBe(first.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("7. repeated exact plan publication preserves inode, mtime, bytes", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const before = await fileIdentity(first.plan_path);
    const second = await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const after = await fileIdentity(first.plan_path);
    expect(second.plan.sha256).toBe(first.plan.sha256);
    expect(after.dev).toBe(before.dev);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(after.mode).toBe(before.mode);
    expect(await readFile(first.plan_path, "utf8")).toBe(first.plan.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("8. conflicting task body fails as conflict and the old file is untouched", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const before = await fileIdentity(first.task_path);
    const cause = await publishPipelineV2TaskRevision(
      fixture.runRoot,
      taskValue({ body: "a different body" }),
    ).catch((error) => error);
    expectStoreError(cause, "not_published", "conflict", "different canonical bytes");
    const after = await fileIdentity(first.task_path);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(await readFile(first.task_path, "utf8")).toBe(first.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("9. a different plan on the same revision path is a typed conflict", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const before = await fileIdentity(first.plan_path);
    const cause = await publishPipelineV2PlanRevision(
      fixture.runRoot,
      planValue({ origin_execution: 11 }),
    ).catch((error) => error);
    expectStoreError(cause, "not_published", "conflict", "different canonical bytes");
    const after = await fileIdentity(first.plan_path);
    expect(after.ino).toBe(before.ino);
    expect(await readFile(first.plan_path, "utf8")).toBe(first.plan.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

// --- 10-12. bindings -----------------------------------------------------------

test("10. run-id/basename mismatch fails before anything is written", async () => {
  const fixture = await setup("run-2");
  try {
    const taskCause = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(taskCause, "not_published", "invalid_layout", "run identifier");
    const planCause = await publishPipelineV2PlanRevision(fixture.runRoot, planValue()).catch(
      (error) => error,
    );
    expectStoreError(planCause, "not_published", "invalid_layout", "run identifier");
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("11. the task path is bound by the manifest task id and revision only", async () => {
  const fixture = await setup();
  try {
    const second = await publishPipelineV2TaskRevision(
      fixture.runRoot,
      taskValue({ task_id: "task-2", revision: 3, previous_sha256: hex("9"), origin: "user_response" }),
    );
    expect(second.task_path).toBe(join(fixture.tasks, "task-2", "3.json"));
    expect(await readFile(second.task_path, "utf8")).toBe(second.task.canonical_json);
    const loadedSecond = await loadPipelineV2TaskRevision(fixture.runRoot, "task-2", 3);
    expect(loadedSecond?.task.manifest.task_id).toBe("task-2");
    expect(await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 3)).toBeNull();
    expect(await loadPipelineV2TaskRevision(fixture.runRoot, "task-2", 1)).toBeNull();
  } finally {
    await dispose(fixture);
  }
});

test("12. the plan path is bound by the manifest revision only", async () => {
  const fixture = await setup();
  try {
    const second = await publishPipelineV2PlanRevision(fixture.runRoot, planValue({ revision: 2, previous_sha256: hex("9") }));
    expect(second.plan_path).toBe(join(fixture.plans, "2.json"));
    const loadedSecond = await loadPipelineV2PlanRevision(fixture.runRoot, 2);
    expect(loadedSecond?.plan.manifest.revision).toBe(2);
    expect(await loadPipelineV2PlanRevision(fixture.runRoot, 1)).toBeNull();
  } finally {
    await dispose(fixture);
  }
});

// --- 13-19. damaged artifacts and layouts --------------------------------------

test("13. malformed JSON load keeps the existing manifest error class", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    await writeFile(join(fixture.tasks, "task-1", "1.json"), "{not json", { mode: 0o600 });
    let cause: unknown;
    try {
      await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1);
    } catch (error) {
      cause = error;
    }
    expect(cause).toBeInstanceOf(PipelineV2RunPlanManifestError);
    expect(cause).not.toBeInstanceOf(PipelineV2RunPlanStoreError);
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    await writeFile(join(fixture.plans, "1.json"), "nonsense", { mode: 0o600 });
    let planCause: unknown;
    try {
      await loadPipelineV2PlanRevision(fixture.runRoot, 1);
    } catch (error) {
      planCause = error;
    }
    expect(planCause).toBeInstanceOf(PipelineV2RunPlanManifestError);
  } finally {
    await dispose(fixture);
  }
});

test("14. a noncanonical but valid JSON artifact fails as conflict on load", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const parsed = JSON.parse(published.task.canonical_json) as Record<string, unknown>;
    const reordered = JSON.stringify({
      body: parsed["body"],
      origin: parsed["origin"],
      previous_sha256: parsed["previous_sha256"],
      revision: parsed["revision"],
      task_id: parsed["task_id"],
      run_id: parsed["run_id"],
      kind: parsed["kind"],
      schema_version: parsed["schema_version"],
    });
    expect(reordered).not.toBe(published.task.canonical_json);
    await writeFile(join(fixture.tasks, "task-1", "1.json"), reordered, { mode: 0o600 });
    const cause = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "conflict", "does not carry its own canonical JSON");
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    const planCanonical = preparePlanRevisionManifest(planValue()).canonical_json;
    const planParsed = JSON.parse(planCanonical) as Record<string, unknown>;
    const planReordered = JSON.stringify({
      stages: planParsed["stages"],
      origin_execution: planParsed["origin_execution"],
      root_task: planParsed["root_task"],
      previous_sha256: planParsed["previous_sha256"],
      revision: planParsed["revision"],
      run_id: planParsed["run_id"],
      kind: planParsed["kind"],
      schema_version: planParsed["schema_version"],
    });
    await writeFile(join(fixture.plans, "1.json"), planReordered, { mode: 0o600 });
    const planCause = await loadPipelineV2PlanRevision(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(planCause, "not_published", "conflict", "does not carry its own canonical JSON");
  } finally {
    await dispose(fixture);
  }
});

test("15. a stored artifact with the wrong mode fails as conflict", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    await chmod(join(fixture.tasks, "task-1", "1.json"), 0o644);
    const taskCause = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(taskCause, "not_published", "conflict", "mode 0600");
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    await chmod(join(fixture.plans, "1.json"), 0o644);
    const planCause = await loadPipelineV2PlanRevision(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(planCause, "not_published", "conflict", "mode 0600");
  } finally {
    await dispose(fixture);
  }
});

test("16. a symlinked artifact target fails as conflict on load and publication", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    const realPath = join(fixture.tasks, "task-1", "1.json");
    await rm(realPath);
    await symlink(published.task_path, realPath);
    const loadCause = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(loadCause, "not_published", "conflict", "symbolic link");
    const republishCause = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(republishCause, "not_published", "conflict", "symbolic link");
  } finally {
    await dispose(fixture);
  }
});

test("17. directory, FIFO and socket artifact targets fail closed and stay untouched", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.plans, { mode: 0o700, recursive: true });
    const target = join(fixture.plans, "1.json");
    await mkdir(target, { mode: 0o700 });
    const dirCause = await loadPipelineV2PlanRevision(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(dirCause, "not_published", "conflict", "exists but is a directory");
    await rm(target, { recursive: true });
    await makeFifo(target);
    const fifoCause = await loadPipelineV2PlanRevision(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(fifoCause, "not_published", "conflict", "exists but is a FIFO");
    const publishCause = await publishPipelineV2PlanRevision(fixture.runRoot, planValue()).catch(
      (error) => error,
    );
    expectStoreError(publishCause, "not_published", "conflict", "exists but is a FIFO");
    expect((await lstat(target)).isFIFO()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("18. a symlinked store-owned directory component fails closed as invalid layout", async () => {
  const fixture = await setup();
  const outsideA = join(fixture.root, "outside-a");
  const outsideB = join(fixture.root, "outside-b");
  const outsideC = join(fixture.root, "outside-c");
  const outsideD = join(fixture.root, "outside-d");
  for (const outside of [outsideA, outsideB, outsideC, outsideD]) {
    await mkdir(outside, { mode: 0o700 });
  }
  try {
    const runPlanLink = join(fixture.runRoot, "run-plan");
    await symlink(outsideA, runPlanLink);
    const loadCause = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(loadCause, "not_published", "invalid_layout", "symbolic link");
    const publishCause = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(publishCause, "not_published", "invalid_layout", "symbolic link");
    await rm(runPlanLink);
    await mkdir(fixture.runPlan, { mode: 0o700 });
    await symlink(outsideB, join(fixture.runPlan, "tasks"));
    const tasksLink = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(tasksLink, "not_published", "invalid_layout", "symbolic link");
    await rm(join(fixture.runPlan, "tasks"));
    await mkdir(fixture.tasks, { mode: 0o700 });
    await symlink(outsideC, join(fixture.tasks, "task-1"));
    const taskLink = await loadPipelineV2TaskRevision(fixture.runRoot, "task-1", 1).catch(
      (error) => error,
    );
    expectStoreError(taskLink, "not_published", "invalid_layout", "symbolic link");
    await rm(join(fixture.tasks, "task-1"));
    await symlink(outsideD, join(fixture.runPlan, "plans"));
    const plansLink = await loadPipelineV2PlanRevision(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(plansLink, "not_published", "invalid_layout", "symbolic link");
    for (const outside of [outsideA, outsideB, outsideC, outsideD]) {
      expect(await readdir(outside)).toEqual([]);
    }
  } finally {
    await dispose(fixture);
  }
});


test("19. a regular file in place of any store-owned directory component fails closed", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.runRoot, "run-plan"), "x\n", { mode: 0o600 });
    const runPlanFile = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(runPlanFile, "not_published", "invalid_layout", "exists but is a regular file");
    await rm(join(fixture.runRoot, "run-plan"));
    await mkdir(fixture.runPlan, { mode: 0o700 });
    await writeFile(join(fixture.runPlan, "tasks"), "x\n", { mode: 0o600 });
    const tasksFile = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(tasksFile, "not_published", "invalid_layout", "exists but is a regular file");
    await rm(join(fixture.runPlan, "tasks"));
    await mkdir(fixture.tasks, { mode: 0o700 });
    await writeFile(join(fixture.tasks, "task-1"), "x\n", { mode: 0o600 });
    const taskFile = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(taskFile, "not_published", "invalid_layout", "exists but is a regular file");
    await rm(join(fixture.tasks, "task-1"));
    await writeFile(join(fixture.runPlan, "plans"), "x\n", { mode: 0o600 });
    const plansFile = await publishPipelineV2PlanRevision(fixture.runRoot, planValue()).catch(
      (error) => error,
    );
    expectStoreError(plansFile, "not_published", "invalid_layout", "exists but is a regular file");
  } finally {
    await dispose(fixture);
  }
});

// --- 20-21. write loop ---------------------------------------------------------

test("20. partial writes are completed by the write-all loop", async () => {
  const fixture = await setup();
  try {
    let writeCalls = 0;
    const io = ioReplacing("openTempExclusive", async (path: string) => {
      const real = await realRunPlanStoreIo.openTempExclusive(path);
      return Object.freeze({
        write: async (chunk: Uint8Array, offset: number, length: number, position: number) => {
          writeCalls += 1;
          const bytesWritten = writeCalls === 1 ? Math.min(5, length) : length;
          await real.write(chunk, offset, bytesWritten, position);
          return bytesWritten;
        },
        stat: real.stat.bind(real),
        sync: real.sync.bind(real),
        close: real.close.bind(real),
      });
    });
    const published = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue());
    expect(writeCalls).toBeGreaterThan(1);
    expect(await readFile(published.task_path, "utf8")).toBe(published.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("21. a zero-progress, NaN or oversized write fails after exactly one attempt", async () => {
  const fixture = await setup();
  try {
    for (const result of [0, Number.NaN, 999999]) {
      let calls = 0;
      const io = ioReplacing("openTempExclusive", async (path: string) => {
        const real = await realRunPlanStoreIo.openTempExclusive(path);
        return Object.freeze({
          write: async () => {
            calls += 1;
            return result;
          },
          stat: real.stat.bind(real),
          sync: real.sync.bind(real),
          close: real.close.bind(real),
        });
      });
      const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "io_failure", "without progress");
      expect(calls).toBe(1);
      expect(await tempFileNames(fixture.tasks)).toEqual([]);
      expect((await readdir(fixture.tasks)).filter((name) => name.endsWith(".json"))).toEqual([]);
    }
  } finally {
    await dispose(fixture);
  }
});

// --- 22-23. fault matrices ------------------------------------------------------

test("22. pre-link fault matrix: mkdir, chmod, open, stat, write, fsync, close, link fail before publication", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.tasks, { mode: 0o700, recursive: true });
    await mkdir(join(fixture.tasks, "task-1"), { mode: 0o700 });
    const faults: [StoreIo, string][] = [
      [ioFaulting("openTempExclusive", injectedFailure("EACCES")), "(errno EACCES)"],
      [
        ioReplacing("openTempExclusive", async (path: string) => {
          const real = await realRunPlanStoreIo.openTempExclusive(path);
          return Object.freeze({
            write: real.write.bind(real),
            stat: async () => {
              throw injectedFailure("EIO");
            },
            sync: real.sync.bind(real),
            close: real.close.bind(real),
          });
        }),
        "could not be inspected",
      ],
      [
        ioReplacing("openTempExclusive", async (path: string) => {
          const real = await realRunPlanStoreIo.openTempExclusive(path);
          return Object.freeze({
            write: real.write.bind(real),
            stat: real.stat.bind(real),
            sync: async () => {
              throw injectedFailure("EIO");
            },
            close: real.close.bind(real),
          });
        }),
        "could not be synced",
      ],
      [
        ioReplacing("openTempExclusive", async (path: string) => {
          const real = await realRunPlanStoreIo.openTempExclusive(path);
          return Object.freeze({
            write: real.write.bind(real),
            stat: real.stat.bind(real),
            sync: real.sync.bind(real),
            close: async () => {
              try {
                await real.close();
              } catch {
                // best effort
              }
              throw injectedFailure("EIO");
            },
          });
        }),
        "could not be closed",
      ],
      [ioFaulting("link", injectedFailure("EACCES")), "(errno EACCES)"],
    ];
    for (const [io, fragment] of faults) {
      const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "io_failure", fragment);
      // the stat-fault case cannot capture the temp identity, so its
      // ownership-gated cleanup is skipped and the temp stays (documented
      // best-effort boundary); every other case removes exactly its own temp
      if (fragment === "could not be inspected") {
        const leftovers = await tempFileNames(join(fixture.tasks, "task-1"));
        expect(leftovers.length).toBe(1);
        for (const leftover of leftovers) {
          await rm(join(fixture.tasks, "task-1", leftover), { force: true });
        }
      } else {
        expect(await tempFileNames(join(fixture.tasks, "task-1"))).toEqual([]);
      }
      expect(
        (await readdir(join(fixture.tasks, "task-1"))).filter((name) => name.endsWith(".json")),
      ).toEqual([]);
    }
    await rm(fixture.runPlan, { recursive: true, force: true });
    const mkdirCause = await publishPipelineV2TaskRevisionWithIo(
      ioFaulting("mkdirExclusive", injectedFailure("EACCES")),
      fixture.runRoot,
      taskValue(),
    ).catch((error) => error);
    expectStoreError(mkdirCause, "not_published", "io_failure", "could not be created");
    const chmodCause = await publishPipelineV2TaskRevisionWithIo(
      ioFaulting("chmod", injectedFailure("EACCES")),
      fixture.runRoot,
      taskValue(),
    ).catch((error) => error);
    expectStoreError(chmodCause, "not_published", "io_failure", "could not be created");
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("23. post-link fault matrix: temp unlink and parent durability become durability_unknown, exact retry confirms", async () => {
  const fixture = await setup();
  try {
    const cases: [string, StoreIo, string][] = [
      ["temp_unlink", ioFaulting("unlink", injectedFailure("EACCES")), "could not be removed"],
      [
        "dir_open",
        ioReplacing("openDir", async (path: string) => {
          // Fault only the post-link durability fsync of the task
          // directory: the ensure-phase parent fsyncs open the ancestors.
          if (path === join(fixture.tasks, "task-2")) {
            throw injectedFailure("EACCES");
          }
          return realRunPlanStoreIo.openDir(path);
        }),
        "could not be synced",
      ],
      [
        "dir_fsync",
        ioReplacing("openDir", async (path: string) => {
          const real = await realRunPlanStoreIo.openDir(path);
          if (path === join(fixture.tasks, "task-2")) {
            return Object.freeze({
              sync: async () => {
                throw injectedFailure("EIO");
              },
              close: real.close.bind(real),
            });
          }
          return real;
        }),
        "could not be synced",
      ],
      [
        "dir_close",
        ioReplacing("openDir", async (path: string) => {
          const real = await realRunPlanStoreIo.openDir(path);
          if (path === join(fixture.tasks, "task-2")) {
            return Object.freeze({
              sync: real.sync.bind(real),
              close: async () => {
                try {
                  await real.close();
                } catch {
                  // best effort
                }
                throw injectedFailure("EIO");
              },
            });
          }
          return real;
        }),
        "could not be synced",
      ],
    ];
    for (const [name, io, fragment] of cases) {
      const value = taskValue({ revision: 2, task_id: "task-2", body: `body-${name}`, previous_sha256: hex("9"), origin: "user_response" });
      const prepared = prepareTaskRevisionManifest(value);
      const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, value).catch(
        (error) => error,
      );
      const error = expectStoreError(cause, "durability_unknown", "io_failure", fragment);
      const finalPath = join(fixture.tasks, "task-2", "2.json");
      const candidate = error.candidate;
      if (candidate?.kind !== "task") throw new Error("unreachable");
      expect(candidate.run_id).toBe(RUN_ID);
      expect(candidate.task_id).toBe("task-2");
      expect(candidate.revision).toBe(2);
      expect(candidate.sha256).toBe(prepared.sha256);
      expect(candidate.final_path).toBe(finalPath);
      expect(Object.isFrozen(candidate)).toBe(true);
      expect(Object.keys(candidate).sort()).toEqual(["final_path", "kind", "revision", "run_id", "sha256", "task_id"]);
      expect(await readFile(finalPath, "utf8")).toBe(prepared.canonical_json);
      // exact retry confirms the artifact and completes as success
      const retry = await publishPipelineV2TaskRevision(fixture.runRoot, value);
      expect(retry.task.sha256).toBe(prepared.sha256);
      expect(await readFile(finalPath, "utf8")).toBe(prepared.canonical_json);
      await rm(finalPath, { force: true });
    }
    // plan publication durability-unknown: the candidate names the plan identity
    const planValue2 = planValue({ revision: 3, previous_sha256: hex("9") });
    const preparedPlan = preparePlanRevisionManifest(planValue2);
    const planCause = await publishPipelineV2PlanRevisionWithIo(
      ioFaulting("unlink", injectedFailure("EACCES")),
      fixture.runRoot,
      planValue2,
    ).catch((error) => error);
    const planError = expectStoreError(planCause, "durability_unknown", "io_failure");
    const planCandidate = planError.candidate;
    if (planCandidate?.kind !== "plan") throw new Error("unreachable");
    expect(planCandidate.run_id).toBe(RUN_ID);
    expect(planCandidate.revision).toBe(3);
    expect(planCandidate.sha256).toBe(preparedPlan.sha256);
    expect(Object.keys(planCandidate).sort()).toEqual(["final_path", "kind", "revision", "run_id", "sha256"]);
    expect("task_id" in planCandidate).toBe(false);
    const planRetry = await publishPipelineV2PlanRevision(fixture.runRoot, planValue2);
    expect(planRetry.plan.sha256).toBe(preparedPlan.sha256);
  } finally {
    await dispose(fixture);
  }
});

// --- 24-26. concurrency ---------------------------------------------------------

test("24. concurrent identical task publishers both succeed through a link barrier", async () => {
  const fixture = await setup();
  try {
    const io = linkBarrierIo();
    const both = await Promise.allSettled([
      publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()),
      publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()),
    ]);
    expect(both.map((entry) => entry.status).sort()).toEqual(["fulfilled", "fulfilled"]);
    const stored = await readFile(join(fixture.tasks, "task-1", "1.json"), "utf8");
    expect(stored).toBe(prepareTaskRevisionManifest(taskValue()).canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("25. concurrent conflicting task publishers: one wins, the loser gets a conflict", async () => {
  const fixture = await setup();
  try {
    const a = taskValue({ body: "body a" });
    const b = taskValue({ body: "body b" });
    const io = linkBarrierIo();
    const both = await Promise.allSettled([
      publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, a),
      publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, b),
    ]);
    expect(both.map((entry) => entry.status).sort()).toEqual(["fulfilled", "rejected"]);
    const stored = await readFile(join(fixture.tasks, "task-1", "1.json"), "utf8");
    expect([
      prepareTaskRevisionManifest(a).canonical_json,
      prepareTaskRevisionManifest(b).canonical_json,
    ]).toContain(stored);
  } finally {
    await dispose(fixture);
  }
});

test("26. concurrent plan publishers: identical both succeed, conflicting one wins", async () => {
  const fixture = await setup();
  try {
    const io = linkBarrierIo();
    const identical = await Promise.allSettled([
      publishPipelineV2PlanRevisionWithIo(io, fixture.runRoot, planValue()),
      publishPipelineV2PlanRevisionWithIo(io, fixture.runRoot, planValue()),
    ]);
    expect(identical.map((entry) => entry.status).sort()).toEqual(["fulfilled", "fulfilled"]);
    await rm(fixture.runPlan, { recursive: true, force: true });
    const a = planValue({ origin_execution: 10 });
    const b = planValue({ origin_execution: 11 });
    const conflicting = await Promise.allSettled([
      publishPipelineV2PlanRevisionWithIo(io, fixture.runRoot, a),
      publishPipelineV2PlanRevisionWithIo(io, fixture.runRoot, b),
    ]);
    expect(conflicting.map((entry) => entry.status).sort()).toEqual(["fulfilled", "rejected"]);
    const stored = await readFile(join(fixture.plans, "1.json"), "utf8");
    expect([
      preparePlanRevisionManifest(a).canonical_json,
      preparePlanRevisionManifest(b).canonical_json,
    ]).toContain(stored);
  } finally {
    await dispose(fixture);
  }
});

// --- 27-28. temp hygiene ---------------------------------------------------------

test("27. an orphan temp file does not block publication", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.tasks, "task-1"), { mode: 0o700, recursive: true });
    await writeFile(join(fixture.tasks, "task-1", ".run-plan-publish-orphan"), "orphan\n", {
      mode: 0o600,
    });
    const published = await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    expect(published.task_path).toBe(join(fixture.tasks, "task-1", "1.json"));
    expect(await readFile(join(fixture.tasks, "task-1", ".run-plan-publish-orphan"), "utf8")).toBe(
      "orphan\n",
    );
    expect(await readFile(published.task_path, "utf8")).toBe(published.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("28. the temp cleanup ownership gate never removes a substituted object", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.tasks, "task-1"), { mode: 0o700, recursive: true });
    let tempPath = "";
    const forgedStats: Stats = {
      isFile: () => true,
      isSymbolicLink: () => false,
      isDirectory: () => false,
      isFIFO: () => false,
      isSocket: () => false,
      dev: 12345,
      ino: 99999,
      mode: 0o600,
    } as unknown as Stats;
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      openTempExclusive: async (path: string) => {
        tempPath = path;
        return realRunPlanStoreIo.openTempExclusive(path);
      },
      link: async (from: string, to: string) => {
        throw injectedFailure("EACCES");
      },
      lstatOrNull: async (path: string) => {
        if (path === tempPath) {
          return forgedStats;
        }
        return realRunPlanStoreIo.lstatOrNull(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be published");
    expect(tempPath).not.toBe("");
    expect((await lstat(tempPath)).isFile()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

// --- 29-33. isolation, diagnostics, surface, proofs ------------------------------

test("29. published results are deep-frozen and input mutations cannot reach them", async () => {
  const fixture = await setup();
  try {
    const taskInput = taskValue();
    const publishedTask = await publishPipelineV2TaskRevision(fixture.runRoot, taskInput);
    expect(Object.isFrozen(publishedTask)).toBe(true);
    expect(Object.isFrozen(publishedTask.task)).toBe(true);
    expect(Object.isFrozen(publishedTask.task.manifest)).toBe(true);
    const bodyBefore = publishedTask.task.manifest.body;
    (taskInput as Record<string, unknown>).body = "mutated after publish";
    expect(publishedTask.task.manifest.body).toBe(bodyBefore);
    expect(() => {
      (publishedTask.task.manifest as unknown as Record<string, unknown>).run_id = "other";
    }).toThrow();
    const planInput = planValue();
    const publishedPlan = await publishPipelineV2PlanRevision(fixture.runRoot, planInput);
    expect(Object.isFrozen(publishedPlan)).toBe(true);
    expect(Object.isFrozen(publishedPlan.plan.manifest)).toBe(true);
    const stagesBefore = publishedPlan.plan.manifest.stages;
    (planInput as Record<string, unknown>).stages = [];
    expect(publishedPlan.plan.manifest.stages).toBe(stagesBefore);
  } finally {
    await dispose(fixture);
  }
});

test("30. diagnostics never contain manifest bodies, task bodies, raw JSON or canaries", async () => {
  const fixture = await setup();
  try {
    const value = taskWithCanary();
    await publishPipelineV2TaskRevision(fixture.runRoot, value);
    const conflictCause = await publishPipelineV2TaskRevision(
      fixture.runRoot,
      taskWithCanary({ body: `other ${CANARY}` }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    const layoutCause = await publishPipelineV2TaskRevision("/nonexistent-root", value).catch(
      (error) => error,
    );
    expectStoreError(layoutCause, "not_published", "invalid_layout");
    expect((layoutCause as Error).message).not.toContain("/nonexistent-root");
    for (const error of [conflictCause, layoutCause]) {
      expect((error as Error).message).not.toContain(CANARY);
      expect((error as Error).message).not.toContain("planning_proposal");
      expect((error as Error).message).not.toContain("task_revision");
      expect((error as Error).message).not.toContain("{");
    }
    const durabilityCause = await publishPipelineV2PlanRevisionWithIo(
      ioFaulting("unlink", injectedFailure("EACCES")),
      fixture.runRoot,
      planValue({ revision: 4, previous_sha256: hex("9") }),
    ).catch((error) => error);
    expectStoreError(durabilityCause, "durability_unknown", "io_failure");
    expect((durabilityCause as Error).message).not.toContain(CANARY);
    expect((durabilityCause as Error).message).not.toContain(fixture.runRoot);
  } finally {
    await dispose(fixture);
  }
});

test("31. the public export surface carries exactly the seven runtime keys", async () => {
  const namespace = (await import("../src/pipeline_v2_run_plan_store.ts")) as Record<string, unknown>;
  const runtimeKeys = Object.keys(namespace).filter((key) => typeof (namespace as Record<string, unknown>)[key] !== "undefined" || true).sort();
  expect(runtimeKeys).toEqual([
    "PipelineV2RunPlanStoreError",
    "loadPipelineV2PlanRevision",
    "loadPipelineV2TaskRevision",
    "loadPipelineV2WaitIntent",
    "publishPipelineV2PlanRevision",
    "publishPipelineV2TaskRevision",
    "publishPipelineV2WaitIntent",
  ]);
});

test("32. the immutable publication protocol lives exactly once (single-protocol source proof)", async () => {
  const { readFileSync } = await import("node:fs");
  const substrate = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_immutable_document_store_internal.ts"),
    "utf8",
  );
  const waitInternal = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_wait_store_internal.ts"),
    "utf8",
  );
  const runPlanInternal = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_run_plan_store_internal.ts"),
    "utf8",
  );
  const waitPublic = readFileSync(join(import.meta.dir, "../src/pipeline_v2_wait_store.ts"), "utf8");
  const runPlanPublic = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_run_plan_store.ts"),
    "utf8",
  );
  // the raw filesystem primitives exist exactly once, in the substrate
  for (const primitive of [
    "O_NOFOLLOW",
    "O_EXCL",
    "O_CREAT",
    "constants.",
    "openDir",
    "mkdirExclusive",
    "lstatOrNull",
    "realpath(",
    ".link(",
    "fsyncImmutableDirectory(",
    "readWholeFile(",
  ] as const) {
    expect(substrate.includes(primitive)).toBe(true);
    expect(waitInternal.includes(primitive)).toBe(false);
    expect(runPlanInternal.includes(primitive)).toBe(false);
    expect(waitPublic.includes(primitive)).toBe(false);
    expect(runPlanPublic.includes(primitive)).toBe(false);
  }
  // the protocol entry points are defined exactly once, in the substrate
  for (const entryPoint of [
    "function publishImmutableDocumentFile(",
    "function ensureImmutableDirectory(",
    "function verifyStoredDirectoryComponent(",
  ] as const) {
    expect(substrate.includes(entryPoint)).toBe(true);
    expect(waitInternal.includes(entryPoint)).toBe(false);
    expect(runPlanInternal.includes(entryPoint)).toBe(false);
  }
  // no adapter defines its own exclusive temp/link/dir-fsync sequence
  for (const adapter of [waitInternal, runPlanInternal]) {
    expect(adapter.includes("O_CREAT")).toBe(false);
    expect(adapter.includes("O_EXCL")).toBe(false);
  }
});

test("33. publication never modifies anything outside <runRoot>/run-plan", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.runRoot, "state.json"), '{"revision":1}\n', { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "project"), { mode: 0o700 });
    await writeFile(join(fixture.runRoot, "project", "a.md"), "project body\n", { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "data", "inputs"), { mode: 0o700, recursive: true });
    await writeFile(join(fixture.runRoot, "data", "inputs", "x.json"), "{}\n", { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "waits"), { mode: 0o700 });
    await writeFile(join(fixture.runRoot, "waits", "1.request.json"), "{}\n", { mode: 0o600 });
    await writeFile(join(fixture.runRoot, "sentinel"), "sentinel\n", { mode: 0o600 });
    const paths = [
      join(fixture.runRoot, "state.json"),
      join(fixture.runRoot, "project", "a.md"),
      join(fixture.runRoot, "data", "inputs", "x.json"),
      join(fixture.runRoot, "waits", "1.request.json"),
      join(fixture.runRoot, "sentinel"),
    ];
    const before = await Promise.all(paths.map((path) => readFile(path)));
    await publishPipelineV2TaskRevision(fixture.runRoot, taskValue());
    await publishPipelineV2PlanRevision(fixture.runRoot, planValue());
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(before);
    expect((await readdir(fixture.runRoot)).sort()).toEqual([
      "data",
      "project",
      "run-plan",
      "sentinel",
      "state.json",
      "waits",
    ]);
    const conflictCause = await publishPipelineV2TaskRevision(
      fixture.runRoot,
      taskValue({ body: "other" }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(before);
  } finally {
    await dispose(fixture);
  }
});

// --- 34-38. concurrent directory adoption and parent durability ---------------

test("34. a concurrently created correct task directory is adopted without chmod or rmdir", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.tasks, { mode: 0o700, recursive: true });
    const taskDir = join(fixture.tasks, "task-1");
    const chmodPaths: string[] = [];
    const rmdirPaths: string[] = [];
    let concurrentIno = -1;
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      mkdirExclusive: async (path: string) => {
        if (path === taskDir) {
          // The concurrent creator wins the race: it creates the
          // directory first, so the publisher's own exclusive mkdir
          // fails with EEXIST and the call must adopt the directory.
          await realRunPlanStoreIo.mkdirExclusive(taskDir);
          await realRunPlanStoreIo.chmod(taskDir, 0o700);
          concurrentIno = (await lstat(taskDir)).ino;
        }
        return await realRunPlanStoreIo.mkdirExclusive(path);
      },
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
    }) as unknown as StoreIo;
    const published = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue());
    expect(published.task_path).toBe(join(taskDir, "1.json"));
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
    const info = await lstat(taskDir);
    expect(info.ino).toBe(concurrentIno);
    expect(info.mode & 0o777).toBe(0o700);
    expect(await readFile(published.task_path, "utf8")).toBe(published.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("35. a concurrently created wrong-mode task directory fails closed without repair", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.tasks, { mode: 0o700, recursive: true });
    const taskDir = join(fixture.tasks, "task-1");
    const chmodPaths: string[] = [];
    const rmdirPaths: string[] = [];
    let concurrentIno = -1;
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      mkdirExclusive: async (path: string) => {
        if (path === taskDir) {
          await realRunPlanStoreIo.mkdirExclusive(taskDir);
          await realRunPlanStoreIo.chmod(taskDir, 0o755);
          await writeFile(join(taskDir, ".sentinel"), "sentinel\n", { mode: 0o600 });
          concurrentIno = (await lstat(taskDir)).ino;
        }
        return await realRunPlanStoreIo.mkdirExclusive(path);
      },
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "invalid_layout", "mode 0700");
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
    const info = await lstat(taskDir);
    expect(info.ino).toBe(concurrentIno);
    expect(info.mode & 0o777).toBe(0o755);
    expect(await readFile(join(taskDir, ".sentinel"), "utf8")).toBe("sentinel\n");
    expect((await readdir(taskDir)).filter((name) => name.endsWith(".json"))).toEqual([]);
    expect(await tempFileNames(taskDir)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("36. a chmod failure on a directory created by this call still removes it", async () => {
  const fixture = await setup();
  try {
    const rmdirPaths: string[] = [];
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      chmod: async () => {
        throw injectedFailure("EACCES");
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be created");
    expect(rmdirPaths).toEqual([fixture.runPlan]);
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("37. a parent fsync failure after creating the directory keeps not_published and may leave it", async () => {
  const fixture = await setup();
  try {
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      openDir: async (path: string) => {
        if (path === fixture.runRoot) {
          throw injectedFailure("EIO");
        }
        return realRunPlanStoreIo.openDir(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be synced");
    expect(await readdir(fixture.runRoot)).toEqual(["run-plan"]);
    expect(await readdir(fixture.runPlan)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("38. an exact retry after a parent fsync failure adopts the directory, re-syncs the parent and publishes", async () => {
  const fixture = await setup();
  try {
    const faulted = Object.freeze({
      ...realRunPlanStoreIo,
      openDir: async (path: string) => {
        if (path === fixture.runRoot) {
          throw injectedFailure("EIO");
        }
        return realRunPlanStoreIo.openDir(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(faulted, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be synced");
    const openDirPaths: string[] = [];
    const chmodPaths: string[] = [];
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      openDir: async (path: string) => {
        openDirPaths.push(path);
        return realRunPlanStoreIo.openDir(path);
      },
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
    }) as unknown as StoreIo;
    const retry = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue());
    expect(retry.task_path).toBe(join(fixture.tasks, "task-1", "1.json"));
    expect(openDirPaths).toContain(fixture.runRoot);
    expect(chmodPaths).not.toContain(fixture.runPlan);
    expect(await readFile(retry.task_path, "utf8")).toBe(retry.task.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("39. a concurrently created regular file at a directory component fails as invalid layout", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.tasks, { mode: 0o700, recursive: true });
    const taskDir = join(fixture.tasks, "task-1");
    const FILE_CANARY = "CANARY_concurrent_file_body";
    const chmodPaths: string[] = [];
    const rmdirPaths: string[] = [];
    let linkCalls = 0;
    let concurrentDev = -1;
    let concurrentIno = -1;
    let concurrentMode = -1;
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      mkdirExclusive: async (path: string) => {
        if (path === taskDir) {
          await writeFile(taskDir, `${FILE_CANARY}\n`, { mode: 0o600 });
          const info = await lstat(taskDir);
          concurrentDev = info.dev;
          concurrentIno = info.ino;
          concurrentMode = info.mode & 0o777;
        }
        return await realRunPlanStoreIo.mkdirExclusive(path);
      },
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
      link: async (from: string, to: string) => {
        linkCalls += 1;
        return realRunPlanStoreIo.link(from, to);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2TaskRevisionWithIo(io, fixture.runRoot, taskValue()).catch(
      (error) => error,
    );
    const error = expectStoreError(
      cause,
      "not_published",
      "invalid_layout",
      "exists but is a regular file",
    );
    expect(error.message).not.toContain(FILE_CANARY);
    expect(error.message).not.toContain(taskDir);
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
    expect(linkCalls).toBe(0);
    const after = await fileIdentity(taskDir);
    expect(after.dev).toBe(concurrentDev);
    expect(after.ino).toBe(concurrentIno);
    expect(after.mode).toBe(concurrentMode);
    expect(await readFile(taskDir, "utf8")).toBe(`${FILE_CANARY}\n`);
    expect((await readdir(fixture.tasks)).filter((name) => name.endsWith(".json"))).toEqual([]);
    expect(await tempFileNames(fixture.tasks)).toEqual([]);
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["run-plan"]);
  } finally {
    await dispose(fixture);
  }
});

// --- 40-56. wait intent store -------------------------------------------------

import {
  loadPipelineV2WaitIntent,
  publishPipelineV2WaitIntent,
} from "../src/pipeline_v2_run_plan_store.ts";
import {
  loadPipelineV2WaitIntentWithIo,
  publishPipelineV2WaitIntentWithIo,
} from "../src/pipeline_v2_run_plan_store_internal.ts";
import { prepareWaitIntent } from "../src/pipeline_v2_run_plan_manifests.ts";

function continueIntentValue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: RUN_ID,
    wait_index: 3,
    stage_id: "implementation",
    expected_plan_sha256: hex("f"),
    additional_iterations: 2,
    ...overrides,
  };
}

function reviseIntentValue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "revise_task_intent",
    run_id: RUN_ID,
    wait_index: 4,
    task_id: "task-1",
    expected_previous_task_sha256: hex("e"),
    new_task_revision_sha256: hex("d"),
    ...overrides,
  };
}

function intentsPath(fixture: Fixture): string {
  return join(fixture.runPlan, "intents");
}

test("40. continue intent publish/load round-trip: exact layout, modes and canonical bytes", async () => {
  const fixture = await setup();
  try {
    const value = continueIntentValue();
    const published = await publishPipelineV2WaitIntent(fixture.runRoot, value);
    const expectedPath = join(intentsPath(fixture), "3.json");
    expect(published.intent_path).toBe(expectedPath);
    expect(published.intent.manifest.kind).toBe("continue_stage_intent");
    if (published.intent.manifest.kind !== "continue_stage_intent") throw new Error("unreachable");
    expect(published.intent.manifest.run_id).toBe(RUN_ID);
    expect(published.intent.manifest.wait_index).toBe(3);
    expect(published.intent.manifest.stage_id).toBe("implementation");
    expect(published.intent.manifest.additional_iterations).toBe(2);
    expect(await dirMode(fixture.runPlan)).toBe(0o700);
    expect(await dirMode(intentsPath(fixture))).toBe(0o700);
    expect((await fileIdentity(expectedPath)).mode).toBe(0o600);
    const stored = await readFile(expectedPath, "utf8");
    expect(stored).toBe(published.intent.canonical_json);
    expect(stored.endsWith("\n")).toBe(false);
    expect(stored).toBe(JSON.stringify(JSON.parse(stored)));
    const loaded = await loadPipelineV2WaitIntent(fixture.runRoot, 3);
    expect(loaded?.intent_path).toBe(expectedPath);
    expect(loaded?.intent.sha256).toBe(published.intent.sha256);
    expect(loaded?.intent.canonical_json).toBe(published.intent.canonical_json);
    expect(Object.isFrozen(loaded)).toBe(true);
    expect(Object.isFrozen(loaded?.intent.manifest)).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("41. revise intent publish/load round-trip with the same exact contract", async () => {
  const fixture = await setup();
  try {
    const value = reviseIntentValue();
    const published = await publishPipelineV2WaitIntent(fixture.runRoot, value);
    const expectedPath = join(intentsPath(fixture), "4.json");
    expect(published.intent_path).toBe(expectedPath);
    expect(published.intent.manifest.kind).toBe("revise_task_intent");
    if (published.intent.manifest.kind !== "revise_task_intent") throw new Error("unreachable");
    expect(published.intent.manifest.wait_index).toBe(4);
    expect(published.intent.manifest.task_id).toBe("task-1");
    expect((await fileIdentity(expectedPath)).mode).toBe(0o600);
    expect(await readFile(expectedPath, "utf8")).toBe(published.intent.canonical_json);
    const loaded = await loadPipelineV2WaitIntent(fixture.runRoot, 4);
    expect(loaded?.intent.sha256).toBe(published.intent.sha256);
    expect(loaded?.intent.manifest.kind).toBe("revise_task_intent");
  } finally {
    await dispose(fixture);
  }
});

test("42. the run root basename and the wait index bind both publication and load", async () => {
  const fixture = await setup();
  try {
    const foreignRun = join(fixture.root, "run-other");
    await mkdir(foreignRun, { mode: 0o700 });
    const publishCause = await publishPipelineV2WaitIntent(foreignRun, continueIntentValue()).catch(
      (error) => error,
    );
    expectStoreError(publishCause, "not_published", "invalid_layout", "does not match the wait intent manifest run identifier");
    expect(await readdir(foreignRun)).toEqual([]);
    // the load's wait index is validated before any path is built
    for (const waitIndex of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Number.NaN]) {
      const cause = await loadPipelineV2WaitIntent(fixture.runRoot, waitIndex).catch((error) => error);
      expectStoreError(cause, "not_published", "invalid_layout", "the wait index must be a positive safe integer");
    }
    expect(await loadPipelineV2WaitIntent(fixture.runRoot, 3)).toBeNull();
    // the file name is bound to the manifest's own wait index only
    await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue({ wait_index: 5 }));
    const loaded = await loadPipelineV2WaitIntent(fixture.runRoot, 5);
    expect(loaded?.intent.manifest.wait_index).toBe(5);
    expect(await loadPipelineV2WaitIntent(fixture.runRoot, 6)).toBeNull();
    // a hand-placed foreign wait index under the target name is a conflict
    await mkdir(intentsPath(fixture), { mode: 0o700, recursive: true });
    const foreign = prepareWaitIntent(continueIntentValue({ wait_index: 9 }));
    await writeFile(join(intentsPath(fixture), "10.json"), foreign.canonical_json, { mode: 0o600 });
    const wrongCause = await loadPipelineV2WaitIntent(fixture.runRoot, 10).catch((error) => error);
    expectStoreError(wrongCause, "not_published", "conflict", "names another wait index");
  } finally {
    await dispose(fixture);
  }
});

test("43. exact retry preserves inode, mode, mtime and bytes; a conflict changes nothing", async () => {
  const fixture = await setup();
  try {
    const value = continueIntentValue();
    const first = await publishPipelineV2WaitIntent(fixture.runRoot, value);
    const before = await fileIdentity(first.intent_path);
    const retry = await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue());
    expect(retry.intent_path).toBe(first.intent_path);
    expect(await fileIdentity(first.intent_path)).toEqual(before);
    expect(await readFile(first.intent_path, "utf8")).toBe(first.intent.canonical_json);
    const conflictCause = await publishPipelineV2WaitIntent(
      fixture.runRoot,
      reviseIntentValue({ wait_index: 3 }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    expect(await fileIdentity(first.intent_path)).toEqual(before);
    expect(await readFile(first.intent_path, "utf8")).toBe(first.intent.canonical_json);
    expect(await tempFileNames(intentsPath(fixture))).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("44. one wait index owns one immutable intent: both kinds conflict in both directions", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue({ wait_index: 2 }));
    const firstWinner = await readFile(join(intentsPath(fixture), "2.json"), "utf8");
    const reviseCause = await publishPipelineV2WaitIntent(
      fixture.runRoot,
      reviseIntentValue({ wait_index: 2 }),
    ).catch((error) => error);
    expectStoreError(reviseCause, "not_published", "conflict", "carries different canonical bytes");
    expect((await readdir(intentsPath(fixture))).filter((name) => name.endsWith(".json"))).toEqual(["2.json"]);
    expect(await readFile(join(intentsPath(fixture), "2.json"), "utf8")).toBe(firstWinner);
    await rm(join(intentsPath(fixture), "2.json"));
    await publishPipelineV2WaitIntent(fixture.runRoot, reviseIntentValue({ wait_index: 2 }));
    const secondWinner = await readFile(join(intentsPath(fixture), "2.json"), "utf8");
    expect(secondWinner).toBe(prepareWaitIntent(reviseIntentValue({ wait_index: 2 })).canonical_json);
    const continueCause = await publishPipelineV2WaitIntent(
      fixture.runRoot,
      continueIntentValue({ wait_index: 2 }),
    ).catch((error) => error);
    expectStoreError(continueCause, "not_published", "conflict", "carries different canonical bytes");
    expect(await readFile(join(intentsPath(fixture), "2.json"), "utf8")).toBe(secondWinner);
  } finally {
    await dispose(fixture);
  }
});

test("45. concurrent identical intent publications both succeed; different ones race to one winner", async () => {
  const fixture = await setup();
  try {
    const io = linkBarrierIo();
    const identical = await Promise.allSettled([
      publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue()),
      publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue()),
    ]);
    expect(identical.map((entry) => entry.status).sort()).toEqual(["fulfilled", "fulfilled"]);
    const inodes = new Set<number>();
    for (const entry of identical) {
      const value = (entry as PromiseFulfilledResult<{ intent_path: string }>).value;
      inodes.add((await lstat(value.intent_path)).ino);
    }
    expect(inodes.size).toBe(1);
    await rm(fixture.runPlan, { recursive: true, force: true });
    const different = await Promise.allSettled([
      publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue({ wait_index: 3 })),
      publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, reviseIntentValue({ wait_index: 3 })),
    ]);
    expect(different.map((entry) => entry.status).sort()).toEqual(["fulfilled", "rejected"]);
    const stored = await readFile(join(intentsPath(fixture), "3.json"), "utf8");
    expect([
      prepareWaitIntent(continueIntentValue({ wait_index: 3 })).canonical_json,
      prepareWaitIntent(reviseIntentValue({ wait_index: 3 })).canonical_json,
    ]).toContain(stored);
  } finally {
    await dispose(fixture);
  }
});

test("46. load: missing tree and missing target are null; malformed and noncanonical fail closed", async () => {
  const fixture = await setup();
  try {
    expect(await loadPipelineV2WaitIntent(fixture.runRoot, 1)).toBeNull();
    await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue({ wait_index: 1 }));
    const target = join(intentsPath(fixture), "1.json");
    expect(await loadPipelineV2WaitIntent(fixture.runRoot, 1)).not.toBeNull();
    // malformed JSON keeps the manifest module's error class
    await rm(target);
    await writeFile(target, "{not json", { mode: 0o600 });
    const malformedCause = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expect(malformedCause).toBeInstanceOf(PipelineV2RunPlanManifestError);
    // valid JSON with noncanonical bytes fails as a conflict
    const prepared = prepareWaitIntent(continueIntentValue({ wait_index: 1 }));
    await rm(target);
    await writeFile(target, `${JSON.stringify(JSON.parse(prepared.canonical_json), null, 2)}\n`, { mode: 0o600 });
    const noncanonicalCause = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(noncanonicalCause, "not_published", "conflict", "does not carry its own canonical JSON");
  } finally {
    await dispose(fixture);
  }
});

test("47. load: wrong mode, symlink, directory and FIFO targets fail closed untouched", async () => {
  const fixture = await setup();
  try {
    await mkdir(intentsPath(fixture), { mode: 0o700, recursive: true });
    const prepared = prepareWaitIntent(continueIntentValue({ wait_index: 1 }));
    const target = join(intentsPath(fixture), "1.json");
    await writeFile(target, prepared.canonical_json, { mode: 0o644 });
    const modeCause = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(modeCause, "not_published", "conflict", "does not have the required mode 0600");
    await chmod(target, 0o600);
    await rm(target);
    await symlink(join(intentsPath(fixture), "elsewhere.json"), target);
    const symlinkCause = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(symlinkCause, "not_published", "conflict", "exists but is");
    expect((await lstat(target)).isSymbolicLink()).toBe(true);
    await rm(target);
    await mkdir(target, { mode: 0o700 });
    const dirCause = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(dirCause, "not_published", "conflict", "exists but is a directory");
    const publishCause = await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue({ wait_index: 1 })).catch(
      (error) => error,
    );
    expectStoreError(publishCause, "not_published", "conflict", "exists but is a directory");
    await rm(target, { recursive: true });
    await makeFifo(target);
    const fifoLoad = await loadPipelineV2WaitIntent(fixture.runRoot, 1).catch((error) => error);
    expectStoreError(fifoLoad, "not_published", "conflict", "exists but is a FIFO");
    const fifoPublish = await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue({ wait_index: 1 })).catch(
      (error) => error,
    );
    expectStoreError(fifoPublish, "not_published", "conflict", "exists but is a FIFO");
    expect((await lstat(target)).isFIFO()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("48. load: foreign run id and foreign wait index fail as conflicts", async () => {
  const fixture = await setup();
  try {
    await mkdir(intentsPath(fixture), { mode: 0o700, recursive: true });
    const foreignRun = prepareWaitIntent(continueIntentValue({ wait_index: 2, run_id: "run-other" }));
    await writeFile(join(intentsPath(fixture), "2.json"), foreignRun.canonical_json, { mode: 0o600 });
    const runCause = await loadPipelineV2WaitIntent(fixture.runRoot, 2).catch((error) => error);
    expectStoreError(runCause, "not_published", "conflict", "does not belong to this run root");
    const foreignIndex = prepareWaitIntent(continueIntentValue({ wait_index: 3 }));
    await writeFile(join(intentsPath(fixture), "2.json"), foreignIndex.canonical_json, { mode: 0o600 });
    const indexCause = await loadPipelineV2WaitIntent(fixture.runRoot, 2).catch((error) => error);
    expectStoreError(indexCause, "not_published", "conflict", "names another wait index");
  } finally {
    await dispose(fixture);
  }
});

test("49. pre-link fault keeps not_published; post-link fault is durability_unknown and the exact retry confirms", async () => {
  const fixture = await setup();
  try {
    // pre-link: the link() call fails, the target stays absent, the temp is cleaned
    const linkCause = await publishPipelineV2WaitIntentWithIo(
      ioFaulting("link", injectedFailure("EACCES")),
      fixture.runRoot,
      continueIntentValue(),
    ).catch((error) => error);
    expectStoreError(linkCause, "not_published", "io_failure", "(errno EACCES)");
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["run-plan"]);
    expect(await readdir(fixture.runPlan)).toEqual(["intents"]);
    expect(await readdir(intentsPath(fixture))).toEqual([]);
    // post-link: the intents directory's post-link fsync fails
    const value = continueIntentValue({ wait_index: 1 });
    const prepared = prepareWaitIntent(value);
    const faulted = Object.freeze({
      ...realRunPlanStoreIo,
      openDir: async (path: string) => {
        if (path === intentsPath(fixture)) {
          const real = await realRunPlanStoreIo.openDir(path);
          return Object.freeze({
            sync: async () => {
              throw injectedFailure("EIO");
            },
            close: real.close.bind(real),
          });
        }
        return realRunPlanStoreIo.openDir(path);
      },
    }) as unknown as StoreIo;
    const durableCause = await publishPipelineV2WaitIntentWithIo(faulted, fixture.runRoot, value).catch(
      (error) => error,
    );
    const durableError = expectStoreError(durableCause, "durability_unknown", "io_failure");
    const durableCandidate = durableError.candidate;
    if (durableCandidate?.kind !== "wait_intent") throw new Error("unreachable");
    expect(durableCandidate.run_id).toBe(RUN_ID);
    expect(durableCandidate.wait_index).toBe(1);
    expect(durableCandidate.sha256).toBe(prepared.sha256);
    expect(durableCandidate.final_path).toBe(join(intentsPath(fixture), "1.json"));
    expect(Object.isFrozen(durableCandidate)).toBe(true);
    expect(Object.keys(durableCandidate).sort()).toEqual(["final_path", "kind", "run_id", "sha256", "wait_index"]);
    expect("revision" in durableCandidate).toBe(false);
    expect("task_id" in durableCandidate).toBe(false);
    expect(await readFile(join(intentsPath(fixture), "1.json"), "utf8")).toBe(prepared.canonical_json);
    // the exact retry adopts the published file and completes as success
    const retry = await publishPipelineV2WaitIntent(fixture.runRoot, value);
    expect(retry.intent.sha256).toBe(prepared.sha256);
    expect(await readFile(retry.intent_path, "utf8")).toBe(prepared.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("50. an existing intents directory with the wrong mode or kind fails closed without chmod or rmdir", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.runPlan, { mode: 0o700 });
    await mkdir(intentsPath(fixture), { mode: 0o755 });
    await writeFile(join(intentsPath(fixture), ".sentinel"), "sentinel\n", { mode: 0o600 });
    const chmodPaths: string[] = [];
    const rmdirPaths: string[] = [];
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
    }) as unknown as StoreIo;
    const cause = await publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "invalid_layout", "mode 0700");
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
    expect((await lstat(intentsPath(fixture))).mode & 0o777).toBe(0o755);
    expect(await readFile(join(intentsPath(fixture), ".sentinel"), "utf8")).toBe("sentinel\n");
    // a regular file in the intents position fails as invalid layout
    await rm(intentsPath(fixture), { recursive: true });
    await writeFile(intentsPath(fixture), "file\n", { mode: 0o600 });
    const fileCause = await publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue()).catch(
      (error) => error,
    );
    expectStoreError(fileCause, "not_published", "invalid_layout", "exists but is a regular file");
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("51. a concurrently created correct intents directory is adopted and its parent is fsynced in both creation and adoption", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.runPlan, { mode: 0o700 });
    const intents = intentsPath(fixture);
    let concurrentIno = -1;
    const chmodPaths: string[] = [];
    const rmdirPaths: string[] = [];
    const openDirPaths: string[] = [];
    const io = Object.freeze({
      ...realRunPlanStoreIo,
      mkdirExclusive: async (path: string) => {
        if (path === intents) {
          await realRunPlanStoreIo.mkdirExclusive(intents);
          await realRunPlanStoreIo.chmod(intents, 0o700);
          concurrentIno = (await lstat(intents)).ino;
        }
        return await realRunPlanStoreIo.mkdirExclusive(path);
      },
      chmod: async (path: string, mode: number) => {
        chmodPaths.push(path);
        return realRunPlanStoreIo.chmod(path, mode);
      },
      rmdir: async (path: string) => {
        rmdirPaths.push(path);
        return realRunPlanStoreIo.rmdir(path);
      },
      openDir: async (path: string) => {
        openDirPaths.push(path);
        return realRunPlanStoreIo.openDir(path);
      },
    }) as unknown as StoreIo;
    const published = await publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, continueIntentValue());
    expect(published.intent_path).toBe(join(intents, "3.json"));
    expect(chmodPaths).toEqual([]);
    expect(rmdirPaths).toEqual([]);
    expect((await lstat(intents)).ino).toBe(concurrentIno);
    // the adopted directory's parent (run-plan) is fsynced before the link,
    // and the intents directory itself is fsynced after the link
    expect(openDirPaths).toContain(fixture.runPlan);
    expect(openDirPaths).toContain(intents);
    // a fully created tree keeps the same fsync behavior on a fresh run root
    await rm(fixture.runPlan, { recursive: true, force: true });
    openDirPaths.length = 0;
    const created = await publishPipelineV2WaitIntentWithIo(io, fixture.runRoot, reviseIntentValue());
    expect(created.intent_path).toBe(join(intents, "4.json"));
    expect(openDirPaths).toContain(fixture.runPlan);
    expect(openDirPaths).toContain(intents);
    expect((await dirMode(intents))).toBe(0o700);
  } finally {
    await dispose(fixture);
  }
});

test("52. intent publication never modifies anything outside <runRoot>/run-plan/intents", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.runRoot, "state.json"), '{"revision":1}\n', { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "waits"), { mode: 0o700 });
    await writeFile(join(fixture.runRoot, "waits", "3.request.json"), "{}\n", { mode: 0o600 });
    await writeFile(join(fixture.runRoot, "sentinel"), "sentinel\n", { mode: 0o600 });
    const paths = [
      join(fixture.runRoot, "state.json"),
      join(fixture.runRoot, "waits", "3.request.json"),
      join(fixture.runRoot, "sentinel"),
    ];
    const before = await Promise.all(paths.map((path) => readFile(path)));
    await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue());
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(before);
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["run-plan", "sentinel", "state.json", "waits"]);
    const conflictCause = await publishPipelineV2WaitIntent(
      fixture.runRoot,
      reviseIntentValue({ wait_index: 3 }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(before);
  } finally {
    await dispose(fixture);
  }
});

test("53. the caller value is neither mutated nor frozen and later mutations cannot reach the store", async () => {
  const fixture = await setup();
  try {
    const value = continueIntentValue();
    const published = await publishPipelineV2WaitIntent(fixture.runRoot, value);
    expect(Object.isFrozen(value)).toBe(false);
    expect((value as Record<string, unknown>).additional_iterations).toBe(2);
    (value as Record<string, unknown>).additional_iterations = 7;
    (value as Record<string, unknown>).kind = "revise_task_intent";
    const loaded = await loadPipelineV2WaitIntent(fixture.runRoot, 3);
    expect(loaded?.intent.canonical_json).toBe(published.intent.canonical_json);
    if (loaded?.intent.manifest.kind !== "continue_stage_intent") throw new Error("unreachable");
    expect(loaded.intent.manifest.additional_iterations).toBe(2);
    expect(loaded.intent.manifest.kind).toBe("continue_stage_intent");
    expect(Object.isFrozen(published.intent.manifest)).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("54. diagnostics are content-free: no canary, no raw JSON, no caller property names", async () => {
  const fixture = await setup();
  try {
    const hostile = continueIntentValue({ schema_version: 2, extra_field: `${CANARY}_extra` });
    const manifestCause = await publishPipelineV2WaitIntent(fixture.runRoot, hostile).catch((error) => error);
    expect(manifestCause).toBeInstanceOf(PipelineV2RunPlanManifestError);
    expect((manifestCause as Error).message).not.toContain(CANARY);
    expect((manifestCause as Error).message).not.toContain("extra_field");
    await publishPipelineV2WaitIntent(fixture.runRoot, continueIntentValue());
    const conflictCause = await publishPipelineV2WaitIntent(
      fixture.runRoot,
      reviseIntentValue({ wait_index: 3 }),
    ).catch((error) => error);
    const conflict = expectStoreError(conflictCause, "not_published", "conflict");
    expect(conflict.message).not.toContain(CANARY);
    expect(conflict.message).not.toContain(fixture.runRoot);
    expect(conflict.candidate).toBeUndefined();
  } finally {
    await dispose(fixture);
  }
});

test("55. the wait intent layer uses the immutable substrate exactly once and owns no second protocol", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(
    join(import.meta.dir, "..", "src", "pipeline_v2_run_plan_store_internal.ts"),
    "utf8",
  );
  // the substrate's protocol entry points are used; no raw primitives exist here
  for (const used of [
    "publishImmutableDocumentFile(",
    "ensureImmutableDirectory(",
    "verifyStoredDirectoryComponent(",
    "readStoredImmutableDocument(",
    "inspectStoredDocumentOrNull(",
    "requireImmutableDocumentRunRoot(",
  ] as const) {
    expect(source.includes(used)).toBe(true);
  }
  for (const banned of [
    "O_EXCL",
    "O_CREAT",
    "O_NOFOLLOW",
    ".link(",
    "fsyncImmutableDirectory(",
    "readWholeFile(",
    "mkdirExclusive(",
    "canonicalJson(",
    "createHash",
    "CryptoHasher",
    "new WeakMap",
    "new WeakSet",
    "publishPipelineV2WaitRequest",
    "acceptPipelineV2WaitResponse",
    "reducePipelineV2RunCommand",
    "validatePipelineV2RunState",
    "from \"./pipeline_v2_state.ts\"",
    "from \"./pipeline_v2_coordinator.ts\"",
    "from \"./pipeline_v2_runner.ts\"",
    "from \"./main.ts\"",
    "from \"./docker_helper.ts\"",
    "from \"./launcher.ts\"",
  ] as const) {
    expect(source.includes(banned)).toBe(false);
  }
  // exactly one validation chain per direction, no second manifest validator
  expect(source.split("prepareWaitIntent(").length - 1).toBe(1);
  expect(source.split("parseWaitIntent(").length - 1).toBe(1);
  expect(source.includes("prepareTaskRevisionManifest(")).toBe(true);
  expect(source.includes("preparePlanRevisionManifest(")).toBe(true);
});

test("56. an unexpected cause is sanitized to the fixed not-published io failure fallback", async () => {
  const fixture = await setup();
  try {
    const canary = "CANARY_secret_io_body";
    const secretPath = "/tmp/secret/io/path";
    const injected = Object.assign(new Error(`${canary} ${secretPath}`), { code: "EIO" });
    const cause = await publishPipelineV2WaitIntent(fixture.runRoot, {
      get kind(): string {
        throw injected;
      },
      schema_version: 1,
    }).catch((error) => error);
    const error = expectStoreError(cause, "not_published", "io_failure");
    expect(error.message).toBe("the wait intent manifest publication failed");
    expect(cause).not.toBe(injected);
    expect(error.message).not.toContain(canary);
    expect(error.message).not.toContain(secretPath);
    expect(error.candidate).toBeUndefined();
    // the target was never published
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});
