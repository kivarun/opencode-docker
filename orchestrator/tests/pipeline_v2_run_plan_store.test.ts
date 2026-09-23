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
      expect(error.candidate?.kind).toBe("task");
      expect(error.candidate?.run_id).toBe(RUN_ID);
      expect(error.candidate?.task_id).toBe("task-2");
      expect(error.candidate?.revision).toBe(2);
      expect(error.candidate?.sha256).toBe(prepared.sha256);
      expect(error.candidate?.final_path).toBe(finalPath);
      expect(Object.isFrozen(error.candidate)).toBe(true);
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
    expect(planError.candidate?.kind).toBe("plan");
    expect(planError.candidate?.run_id).toBe(RUN_ID);
    expect(planError.candidate?.revision).toBe(3);
    expect(planError.candidate?.sha256).toBe(preparedPlan.sha256);
    expect("task_id" in (planError.candidate as object)).toBe(false);
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

test("31. the public export surface carries exactly the five runtime keys", async () => {
  const namespace = (await import("../src/pipeline_v2_run_plan_store.ts")) as Record<string, unknown>;
  const runtimeKeys = Object.keys(namespace).filter((key) => typeof (namespace as Record<string, unknown>)[key] !== "undefined" || true).sort();
  expect(runtimeKeys).toEqual([
    "PipelineV2RunPlanStoreError",
    "loadPipelineV2PlanRevision",
    "loadPipelineV2TaskRevision",
    "publishPipelineV2PlanRevision",
    "publishPipelineV2TaskRevision",
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
