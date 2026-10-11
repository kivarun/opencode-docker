/**
 * The runner-level `run_busy` contract: with the per-run mutation lock
 * held by another process, every mutating pipeline v2 command returns the
 * exact busy failure outcome — zero state reads, zero dispatches, zero
 * sessions, zero publications — while `inspect` stays readable. The lock
 * is always released after the refused command, on every path.
 */
import { afterAll, expect, test } from "bun:test";
import { readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { parsePipelineV2RunState } from "../src/pipeline_v2_state.ts";
import {
  continuePipelineV2Stage,
  resumePipelineV2,
  resumePipelineV2PlanningRunPlan,
  revisePipelineV2Task,
  runPipelineV2,
  type PipelineV2RunnerDeps,
} from "../src/pipeline_v2_runner.ts";
import { respondPipelineV2Wait, type PipelineV2WaitResponseOutcome } from "../src/pipeline_v2_wait_respond.ts";
import { inspectPipelineV2Run } from "../src/pipeline_v2_inspect.ts";
import { acquirePipelineV2RunMutationLock } from "../src/pipeline_v2_run_mutation_lock.ts";
import type { AuthFetcher, CliRunner } from "../src/docker_helper.ts";
import { faultIo, countingIo } from "./state_io_test_helpers.ts";
import {
  buildHarness,
  cleanupRoots,
  prefixCustomWait,
  prefixResumableBoundary,
  readState,
  treeFingerprint,
  WORKER_OUTPUTS,
  PROFILE_ENV,
  type Harness,
} from "./helpers/pipeline_v2_race_harness.ts";

const EXPECTED_LAUNCHER_ID = "dhl_runner";

afterAll(async () => {
  await cleanupRoots();
});

function makeDeps(harness: Harness): { deps: PipelineV2RunnerDeps; calls: { cli: number; auth: number } } {
  const calls = { cli: 0, auth: 0 };
  const cli: CliRunner = async () => {
    calls.cli += 1;
    return { code: 1 };
  };
  const fetchAuth: AuthFetcher = async () => {
    calls.auth += 1;
    return {
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: EXPECTED_LAUNCHER_ID },
    };
  };
  const deps: PipelineV2RunnerDeps = {
    cli,
    fetchAuth,
    helperConfig: {
      socketPath: "/run/docker-helper/docker-helper.sock",
      credentialFile: `${harness.configRoot}/credential.token`,
    },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", ...PROFILE_ENV },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
  };
  return { deps, calls };
}

const FRESH_RUN_OPTIONS = (harness: Harness, runId: string) => ({
  pipelineRoot: harness.bundlePlanReady,
  configRoot: harness.configRoot,
  projectSourcePath: harness.projectSource,
  inputBindings: [{ id: "task", path: harness.taskFile }],
  runId,
});

/**
 * Drives one honest fresh run to the waiting boundary with in-process
 * fakes (the same flow the subprocess races use).
 */
async function freshRunToWaiting(harness: Harness, runId: string): Promise<void> {
  const { deps } = makeDeps(harness);
  const outcome = await runPipelineV2(FRESH_RUN_OPTIONS(harness, runId), {
    ...deps,
    cli: workerCli(harness, runId, 0),
  });
  expect(outcome.exitCode).toBe(0);
  expect(outcome).toMatchObject({ waiting: true });
}

function workerCli(harness: Harness, runId: string, runCode: number): CliRunner {
  let createIndex = 0;
  const processTag = Math.random().toString(36).slice(2, 8);
  return async (args) => {
    if (args[0] === "session" && args[1] === "create") {
      createIndex += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${processTag}_${createIndex}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_${processTag}_${createIndex}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] ?? "" }) };
    }
    if (args[0] === "pull") {
      return { code: 0 };
    }
    if (args[0] === "run") {
      let mountStart = -1;
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] === "--mount") {
          mountStart = i;
          break;
        }
      }
      const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
      for (let i = mountStart; i >= 0 && i < args.length && args[i] === "--mount"; i += 2) {
        const spec = args[i + 1] ?? "";
        const [source, target] = spec.split(":");
        if (target === "/pipeline/outputs") {
          const segments = (source ?? "").split("/");
          const dash = segments[1] ?? "";
          const stateId = dash.slice(dash.indexOf("-") + 1);
          const outputs = (WORKER_OUTPUTS as Record<string, Record<string, string>>)[stateId];
          if (outputs !== undefined) {
            const { mkdir, writeFile } = await import("node:fs/promises");
            const dir = join(runRoot, source ?? "");
            await mkdir(dir, { recursive: true });
            for (const [name, content] of Object.entries(outputs)) {
              await writeFile(join(dir, name), content as string);
            }
          }
        }
      }
      return { code: runCode, timedOut: false };
    }
    return { code: 1 };
  };
}

async function holdLock(runRoot: string): Promise<() => void> {
  const acquisition = acquirePipelineV2RunMutationLock({ runRoot, runId: runRoot.split("/").pop()!, mode: "fresh" });
  if (acquisition.busy) throw new Error("the test lock could not be taken");
  return () => acquisition.lock.release();
}

test("resume on a locked run: the exact busy outcome with zero effects", async () => {
  const harness = await buildHarness("busy-resume");
  await freshRunToWaiting(harness, "busy-run");
  await prefixResumableBoundary(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const before = await treeFingerprint(runRoot);
  const release = await holdLock(runRoot);
  try {
    const { deps, calls } = makeDeps(harness);
    const outcome = await resumePipelineV2({ runId: "busy-run", configRoot: harness.configRoot }, deps);
    expect(outcome).toEqual({
      ok: false,
      exitCode: 1,
      runId: "busy-run",
      runRoot,
      state: null,
      reason: "run_busy",
    });
    expect(calls.cli).toBe(0);
    expect(calls.auth).toBe(0);
    expect(await treeFingerprint(runRoot)).toBe(before);
  } finally {
    release();
  }
  // After the release the same command proceeds past the lock again (the
  // run is at a resumable boundary): the refusal was transport-level only,
  // and the continued run reaches its next controlled boundary.
  const { deps, calls } = makeDeps(harness);
  const retried = await resumePipelineV2(
    { runId: "busy-run", configRoot: harness.configRoot },
    { ...deps, cli: workerCli(harness, "busy-run", 0) },
  );
  expect(retried).toMatchObject({ ok: false, waiting: true, exitCode: 0, runId: "busy-run" });
  expect(calls.auth).toBe(1);
  const after = await readState(harness.stateRoot, "busy-run");
  expect(after.status).toBe("waiting");
  expect(after.waits.length).toBe(2);
}, 60000);

test("continue-stage on a locked run: the exact busy outcome with zero effects", async () => {
  const harness = await buildHarness("busy-continue");
  await freshRunToWaiting(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const before = await treeFingerprint(runRoot);
  const release = await holdLock(runRoot);
  try {
    const { deps, calls } = makeDeps(harness);
    const outcome = await continuePipelineV2Stage(
      { runId: "busy-run", waitIndex: 1, additionalIterations: 1, configRoot: harness.configRoot },
      deps,
    );
    expect(outcome).toEqual({
      ok: false,
      exitCode: 1,
      runId: "busy-run",
      runRoot,
      state: null,
      reason: "run_busy",
    });
    expect(calls.cli).toBe(0);
    expect(calls.auth).toBe(0);
    expect(await treeFingerprint(runRoot)).toBe(before);
  } finally {
    release();
  }
}, 60000);

test("revise-task on a locked run: the exact busy outcome with zero effects", async () => {
  const harness = await buildHarness("busy-revise");
  await freshRunToWaiting(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const before = await treeFingerprint(runRoot);
  const release = await holdLock(runRoot);
  try {
    const { deps, calls } = makeDeps(harness);
    const outcome = await revisePipelineV2Task(
      {
        runId: "busy-run",
        waitIndex: 1,
        taskId: "task-a",
        taskBody: "REVISED BODY",
        configRoot: harness.configRoot,
      },
      deps,
    );
    expect(outcome).toEqual({
      ok: false,
      exitCode: 1,
      runId: "busy-run",
      runRoot,
      state: null,
      reason: "run_busy",
    });
    expect(calls.cli).toBe(0);
    expect(calls.auth).toBe(0);
    expect(await treeFingerprint(runRoot)).toBe(before);
  } finally {
    release();
  }
}, 60000);

test("resume-plan on a locked run: the exact busy outcome with zero effects", async () => {
  const harness = await buildHarness("busy-resume-plan");
  const { deps } = makeDeps(harness);
  const outcome = await runPipelineV2(
    { ...FRESH_RUN_OPTIONS(harness, "busy-run"), pipelineRoot: harness.bundleManual },
    { ...deps, cli: workerCli(harness, "busy-run", 0) },
  );
  expect(outcome).toMatchObject({ planReady: true, exitCode: 0 });
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const before = await treeFingerprint(runRoot);
  const release = await holdLock(runRoot);
  try {
    const { deps, calls } = makeDeps(harness);
    const busy = await resumePipelineV2PlanningRunPlan(
      { runId: "busy-run", stageId: "stage-1", initialBudget: 1, configRoot: harness.configRoot },
      deps,
    );
    expect(busy).toEqual({
      ok: false,
      exitCode: 1,
      runId: "busy-run",
      runRoot,
      state: null,
      reason: "run_busy",
    });
    expect(calls.cli).toBe(0);
    expect(calls.auth).toBe(0);
    expect(await treeFingerprint(runRoot)).toBe(before);
  } finally {
    release();
  }
}, 60000);

test("respond on a locked run: the exact busy outcome with zero store commits", async () => {
  const harness = await buildHarness("busy-respond");
  await freshRunToWaiting(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const before = await treeFingerprint(runRoot);
  const release = await holdLock(runRoot);
  const { io, counts } = countingIo();
  try {
    const outcome: PipelineV2WaitResponseOutcome = await respondPipelineV2Wait(
      { runId: "busy-run", waitIndex: 1, actionId: "continue_stage" },
      {
        stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
        io,
      },
    );
    expect(outcome).toEqual({
      ok: false,
      exitCode: 1,
      runId: "busy-run",
      runRoot,
      state: null,
      reason: "run_busy",
    });
    expect(counts.tempOpens).toBe(0);
    expect(counts.renames).toBe(0);
    expect(counts.dirSyncs).toBe(0);
    expect(await treeFingerprint(runRoot)).toBe(before);
  } finally {
    release();
  }
}, 60000);

test("a fresh run collides instead of reporting busy while another process holds the claim", async () => {
  const harness = await buildHarness("busy-fresh");
  const { deps } = makeDeps(harness);
  // The run root is claimed by an existing object: the collision reason is
  // unchanged (the fresh runner never reaches the lock when the claim
  // fails, and a claimed-but-uninitialized root is refused as busy for
  // existing-run commands by the state-document precondition).
  await (await import("node:fs/promises")).mkdir(
    join(harness.stateRoot, "pipeline-runs", "busy-run"),
    { recursive: true, mode: 0o700 },
  );
  const collision = await runPipelineV2(FRESH_RUN_OPTIONS(harness, "busy-run"), deps);
  expect(collision).toMatchObject({ ok: false, exitCode: 1, reason: "run_already_exists" });
}, 60000);

test("an existing-run command refuses a claimed-but-stateless run root without the lock", async () => {
  const harness = await buildHarness("busy-stateless");
  await (await import("node:fs/promises")).mkdir(
    join(harness.stateRoot, "pipeline-runs", "busy-run"),
    { recursive: true, mode: 0o700 },
  );
  const { deps, calls } = makeDeps(harness);
  const outcome = await resumePipelineV2({ runId: "busy-run", configRoot: harness.configRoot }, deps);
  expect(outcome).toMatchObject({
    ok: false,
    exitCode: 1,
    runId: "busy-run",
    state: null,
    reason: "missing_state",
  });
  expect(calls.cli).toBe(0);
  expect(calls.auth).toBe(0);
}, 60000);

test("inspect stays readable while the mutation lock is held", async () => {
  const harness = await buildHarness("busy-inspect");
  await freshRunToWaiting(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const release = await holdLock(runRoot);
  try {
    const report = await inspectPipelineV2Run({ runId: "busy-run", stateRoot: harness.stateRoot });
    expect(report.ok).toBe(true);
    if (report.ok) {
      expect(report.boundary.kind).toBe("waiting");
    }
  } finally {
    release();
  }
}, 60000);

test("a durability-unknown sink failure releases the lock for the next caller", async () => {
  const harness = await buildHarness("busy-durability");
  await prefixCustomWait(harness, "busy-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "busy-run");
  const outcome = await respondPipelineV2Wait(
    { runId: "busy-run", waitIndex: 1, actionId: "approve" },
    {
      stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
      io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
    },
  );
  expect(outcome).toMatchObject({ ok: false, exitCode: 1, reason: "state_persist_failed" });
  // The lock was held by the failed command and is free again: the next
  // caller acquires it without any repair step. The renamed response is
  // durable on disk (the directory fsync failed after the rename), so a
  // fresh caller recognizes the exact same answer idempotently.
  const next = await respondPipelineV2Wait(
    { runId: "busy-run", waitIndex: 1, actionId: "approve" },
    { stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot } },
  );
  expect(next).toMatchObject({ ok: true, exitCode: 0, actionId: "approve" });
  const state = parsePipelineV2RunState(
    await readFile(join(runRoot, "state.json"), "utf8"),
  );
  expect(state.status).toBe("active");
}, 60000);

test("different run ids run concurrently while one lock is held", async () => {
  const harness = await buildHarness("busy-parallel");
  await freshRunToWaiting(harness, "busy-run-a");
  const runRootA = join(harness.stateRoot, "pipeline-runs", "busy-run-a");
  const release = await holdLock(runRootA);
  try {
    const { deps } = makeDeps(harness);
    const other = await runPipelineV2(
      { ...FRESH_RUN_OPTIONS(harness, "busy-run-b"), runId: "busy-run-b" },
      { ...deps, cli: workerCli(harness, "busy-run-b", 0) },
    );
    expect(other).toMatchObject({ waiting: true, exitCode: 0, runId: "busy-run-b" });
  } finally {
    release();
  }
  void rm;
  void readFile;
}, 60000);
