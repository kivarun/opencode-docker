/**
 * The permanent inter-process concurrency matrix of the pipeline v2 run
 * mutation contract: two real OS processes race to mutate one run and the
 * kernel-owned per-run mutation lock decides the outcome. Every race has
 * exactly one winner — whose command sequence, outcome and durable state
 * are identical to a solo run — and exactly one loser, which reports the
 * typed `run_busy` refusal with zero state reads, zero dispatches, zero
 * sessions and zero published artifacts. The matrix also proves the
 * kernel release on SIGKILL, the retry semantics on every boundary, the
 * unchanged fresh-run collision reason and the cross-run independence.
 */
import { afterAll, expect, test } from "bun:test";
import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { parsePipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { inspectPipelineV2Run } from "../src/pipeline_v2_inspect.ts";
import {
  buildHarness,
  cleanupRoots,
  MutatingChild,
  prefixCustomWait,
  prefixResumableBoundary,
  PROFILE_ENV,
  readState,
  summarize,
  treeFingerprint,
  WORKER_OUTPUTS,
  type ChildSpec,
  type Harness,
} from "./helpers/pipeline_v2_race_harness.ts";

const PROPOSAL_WORKER_OUTPUTS = WORKER_OUTPUTS;
/**
 * The pointer-only run-plan proposal of a replanning execution: the
 * revised task already exists durably, so the proposal references it
 * instead of declaring it again.
 */
const POINTER_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
  ],
  new_tasks: [],
};
const POINTER_WORKER_OUTPUTS = { architect: { plan: JSON.stringify(POINTER_PROPOSAL) } };

function specWith(harness: Harness, base: Partial<ChildSpec> & { command: ChildSpec["command"]; runId: string }): ChildSpec {
  return {
    stateRoot: harness.stateRoot,
    bundle: harness.bundlePlanReady,
    configRoot: harness.configRoot,
    profileEnv: PROFILE_ENV,
    workerOutputs: PROPOSAL_WORKER_OUTPUTS,
    runCode: 0,
    ...base,
  } as ChildSpec;
}

interface RaceResult {
  winner: Awaited<ReturnType<MutatingChild["finish"]>>;
  loser: Awaited<ReturnType<MutatingChild["finish"]>>;
}

/**
 * Runs one two-process race: the winner child enters its mid-flow gate
 * (holding the kernel lock), then the loser child is started and refused
 * with `run_busy` before any effect; the winner is released and its
 * outcome is observed.
 */
async function runRace(
  harness: Harness,
  winnerSpec: ChildSpec,
  loserSpec: ChildSpec,
  winnerGate: string,
): Promise<RaceResult> {
  const winner = new MutatingChild(winnerSpec, harness.root, new Set([winnerGate]));
  await winner.gate(winnerGate);
  const loser = new MutatingChild(loserSpec, harness.root);
  // The loser starts only now, against the already held kernel lock.
  loser.release();
  const loserResult = await loser.finish();
  winner.release();
  const winnerResult = await winner.finish();
  return { winner: winnerResult, loser: loserResult };
}

function expectBusy(loser: Awaited<ReturnType<MutatingChild["finish"]>>, runId: string, runRoot: string): void {
  expect(loser.outcome).toEqual({
    ok: false,
    exitCode: 1,
    runId,
    runRoot,
    state: null,
    reason: "run_busy",
  });
  const stats = loser.stats as Record<string, number>;
  expect(stats.cli ?? 0).toBe(0);
  expect(stats.auth ?? 0).toBe(0);
  expect(stats.create ?? 0).toBe(0);
}

async function waitForStateFile(stateRoot: string, runId: string): Promise<void> {
  const path = join(stateRoot, "pipeline-runs", runId, "state.json");
  for (let attempt = 0; attempt < 4000; attempt += 1) {
    try {
      await stat(path);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  throw new Error(`the state document never appeared at ${path}`);
}

test("control: one fresh child run reaches the waiting boundary", async () => {
  const harness = await buildHarness("race-control");
  const child = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "control-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const result = await child.finish();
  expect(result.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0, runId: "control-run" });
  const state = await readState(harness.stateRoot, "control-run");
  expect(state.status).toBe("waiting");
  expect(state.waits.length).toBe(1);
  expect(state.executions.length).toBe(2);
}, 60000);

test("matrix: resume vs resume at the resumable boundary", async () => {
  const harness = await buildHarness("race-resume-resume");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ waiting: true, exitCode: 0 });
  await prefixResumableBoundary(harness, "race-run");
  const resumable = await readState(harness.stateRoot, "race-run");
  expect(resumable.status).toBe("active");
  expect(resumable.executions.length).toBe(resumable.transitions.length);

  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const { winner, loser } = await runRace(
    harness,
    specWith(harness, { command: "resume", runId: "race-run", gateSessionCreate: 1 }),
    specWith(harness, { command: "resume", runId: "race-run", waitStart: true }),
    "create-1",
  );
  expectBusy(loser, "race-run", runRoot);
  expect(winner.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0, runId: "race-run" });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  expect(final.waits.length).toBe(2);
  expect(final.executions.length).toBe(3);
  expect(final.grants.length).toBe(1);
  // The refused process left no trace: the durable history matches a solo
  // continuation exactly.
  expect(final.executions.every((execution) => execution.phase !== "failed")).toBe(true);
  const inspect = await inspectPipelineV2Run({ runId: "race-run", stateRoot: harness.stateRoot });
  expect(inspect.ok).toBe(true);
  if (inspect.ok) {
    expect(inspect.status).toBe("waiting");
    expect(inspect.boundary.kind).toBe("waiting");
  }
  // The loser's retry after the release refuses on the new boundary —
  // never with `run_busy` again.
  const retry = new MutatingChild(specWith(harness, { command: "resume", runId: "race-run" }), harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state", state: retryResult.outcome.state });
  expect(retryResult.outcome.reason).not.toBe("run_busy");
}, 120000);

test("matrix: continue-stage vs continue-stage at the waiting boundary", async () => {
  const harness = await buildHarness("race-continue-continue");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ waiting: true, exitCode: 0 });
  const before = await readState(harness.stateRoot, "race-run");
  expect(before.status).toBe("waiting");

  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const { winner, loser } = await runRace(
    harness,
    specWith(harness, {
      command: "continue-stage",
      runId: "race-run",
      waitIndex: 1,
      additionalIterations: 1,
      gateSessionCreate: 1,
    }),
    specWith(harness, {
      command: "continue-stage",
      runId: "race-run",
      waitIndex: 1,
      additionalIterations: 1,
      waitStart: true,
    }),
    "create-1",
  );
  expectBusy(loser, "race-run", runRoot);
  expect(winner.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0 });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  expect(final.waits.length).toBe(2);
  expect(final.grants.length).toBe(1);
  expect(final.executions.length).toBe(3);
  // The retry refuses on the moved boundary (the target wait is no longer
  // the last record) — never with `run_busy`.
  const retry = new MutatingChild(specWith(harness, {
    command: "continue-stage",
    runId: "race-run",
    waitIndex: 1,
    additionalIterations: 1,
  }), harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state" });
}, 120000);

test("matrix: revise-task vs revise-task at the waiting boundary", async () => {
  const harness = await buildHarness("race-revise-revise");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ waiting: true, exitCode: 0 });

  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const reviseBase = {
    command: "revise-task" as const,
    runId: "race-run",
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "REVISED: implement the staged work differently",
  };
  const { winner, loser } = await runRace(
    harness,
    specWith(harness, { ...reviseBase, gateSessionCreate: 1, workerOutputs: POINTER_WORKER_OUTPUTS }),
    specWith(harness, { ...reviseBase, waitStart: true, workerOutputs: POINTER_WORKER_OUTPUTS }),
    "create-1",
  );
  expectBusy(loser, "race-run", runRoot);
  expect(winner.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0 });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  expect(final.waits.length).toBe(2);
  expect(final.task_revisions.length).toBe(2);
  expect(final.generations.length).toBe(2);
  // The retry refuses on the moved boundary — never with `run_busy`.
  const retry = new MutatingChild(specWith(harness, { ...reviseBase }), harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state" });
}, 120000);

test("matrix: continue-stage vs revise-task at one multi-action wait", async () => {
  const harness = await buildHarness("race-continue-revise");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ waiting: true, exitCode: 0 });

  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const winner = new MutatingChild(specWith(harness, {
    command: "continue-stage",
    runId: "race-run",
    waitIndex: 1,
    additionalIterations: 1,
    gateSessionCreate: 1,
  }), harness.root, new Set(["create-1"]));
  await winner.gate("create-1");
  const loser = new MutatingChild(specWith(harness, {
    command: "revise-task",
    runId: "race-run",
    waitIndex: 1,
    taskId: "task-a",
    taskBody: "REVISED: implement the staged work differently",
    waitStart: true,
    workerOutputs: POINTER_WORKER_OUTPUTS,
  }), harness.root);
  loser.release();
  const loserResult = await loser.finish();
  expectBusy(loserResult, "race-run", runRoot);
  winner.release();
  const winnerResult = await winner.finish();
  expect(winnerResult.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0 });
  // The revise-task loser published no intent and no task artifact: the
  // durable ledgers match the continue-stage-only continuation.
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  expect(final.waits.length).toBe(2);
  expect(final.grants.length).toBe(1);
  expect(final.task_revisions.length).toBe(1);
  const inspect = await inspectPipelineV2Run({ runId: "race-run", stateRoot: harness.stateRoot });
  expect(inspect.ok).toBe(true);
}, 120000);

test("matrix: resume-plan vs resume-plan at the plan-ready boundary", async () => {
  const harness = await buildHarness("race-resume-plan");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
    bundle: harness.bundleManual,
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ ok: false, planReady: true, exitCode: 0 });

  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const resumePlanBase = {
    command: "resume-plan" as const,
    runId: "race-run",
    stageId: "stage-1",
    initialBudget: 1,
  };
  const { winner, loser } = await runRace(
    harness,
    specWith(harness, { ...resumePlanBase, gateSessionCreate: 1 }),
    specWith(harness, { ...resumePlanBase, waitStart: true }),
    "create-1",
  );
  expectBusy(loser, "race-run", runRoot);
  expect(winner.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0 });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  expect(final.plan_revisions.length).toBe(1);
  expect(final.generations.length).toBe(1);
  expect(final.transitions.length).toBe(2);
  // The retry refuses on the moved boundary — never with `run_busy`.
  const retry = new MutatingChild(specWith(harness, { ...resumePlanBase }), harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state" });
}, 120000);

test("matrix: respond vs respond for one wait record (deterministic lock)", async () => {
  const harness = await buildHarness("race-respond");
  await prefixCustomWait(harness, "race-run");
  const before = await readState(harness.stateRoot, "race-run");
  expect(before.status).toBe("waiting");
  expect(before.waits[0]!.actions.map((action) => action.id)).toEqual(["approve"]);

  const respondSpec: ChildSpec = {
    command: "respond",
    stateRoot: harness.stateRoot,
    bundle: harness.bundlePlanReady,
    configRoot: harness.configRoot,
    runId: "race-run",
    waitIndex: 1,
    actionId: "approve",
    waitStart: true,
  };
  // The parent holds the kernel lock exactly like a competing process:
  // the first responder is refused before any publication or dispatch.
  const lockProbe = join(import.meta.dir, "helpers", "pipeline_v2_lock_probe.ts");
  const holder = Bun.spawn([process.execPath, lockProbe, "hold", join(harness.stateRoot, "pipeline-runs", "race-run"), "race-run"], {
    cwd: import.meta.dir,
    stdin: "pipe",
    stdout: "pipe",
  });
  const heldLine = await new Promise<string>((resolve) => {
    const decoder = new TextDecoder();
    let buffer = "";
    const stream = holder.stdout as unknown as AsyncIterable<Uint8Array>;
    void (async () => {
      for await (const chunk of stream) {
        buffer += decoder.decode(chunk);
        const newline = buffer.indexOf("\n");
        if (newline >= 0) resolve(buffer.slice(0, newline).trim());
      }
    })();
  });
  expect(heldLine).toBe("HELD");
  const loser = new MutatingChild(respondSpec, harness.root);
  loser.release();
  const loserResult = await loser.finish();
  expectBusy(loserResult, "race-run", join(harness.stateRoot, "pipeline-runs", "race-run"));
  // Release: the retry records the response through the ordinary chain.
  holder.stdin!.write("release\n");
  await holder.exited;
  const retry = new MutatingChild({ ...respondSpec, waitStart: false }, harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: true, exitCode: 0, actionId: "approve" });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("active");
  expect(final.waits[0]!.response?.action_id).toBe("approve");
}, 60000);

test("matrix: fresh run vs existing-run command around the atomic claim", async () => {
  const harness = await buildHarness("race-fresh-existing");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
    gateWorkerRun: true,
  }), harness.root, new Set(["run"]));
  await fresh.gate("run");
  await waitForStateFile(harness.stateRoot, "race-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");
  const before = await treeFingerprint(runRoot);

  const resumeLoser = new MutatingChild(specWith(harness, {
    command: "resume",
    runId: "race-run",
    waitStart: true,
  }), harness.root);
  resumeLoser.release();
  const resumeLoserResult = await resumeLoser.finish();
  expectBusy(resumeLoserResult, "race-run", runRoot);
  expect(await treeFingerprint(runRoot)).toBe(before);

  // A concurrent fresh `run` with the same id still reports the unchanged
  // collision reason while the winner holds the lock.
  const collision = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
    waitStart: true,
  }), harness.root);
  collision.release();
  const collisionResult = await collision.finish();
  expect(collisionResult.outcome).toMatchObject({
    ok: false,
    exitCode: 1,
    runId: "race-run",
    reason: "run_already_exists",
  });

  fresh.release();
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0 });
  const final = await readState(harness.stateRoot, "race-run");
  expect(final.status).toBe("waiting");
  // The loser's retry refuses on the waiting boundary — never busy.
  const retry = new MutatingChild(specWith(harness, { command: "resume", runId: "race-run" }), harness.root);
  const retryResult = await retry.finish();
  expect(retryResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state" });
}, 120000);

test("matrix: SIGKILL of the winner frees the lock and needs no repair", async () => {
  const harness = await buildHarness("race-kill");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const freshResult = await fresh.finish();
  expect(freshResult.outcome).toMatchObject({ waiting: true, exitCode: 0 });
  await prefixResumableBoundary(harness, "race-run");
  const runRoot = join(harness.stateRoot, "pipeline-runs", "race-run");

  const holder = new MutatingChild(specWith(harness, {
    command: "resume",
    runId: "race-run",
    gateSessionCreate: 1,
  }), harness.root, new Set(["create-1"]));
  await holder.gate("create-1");
  // Several durable writes happened (start + data_prepared); the baseline
  // for the no-repair proof is the killed holder's own durable state.
  const before = await treeFingerprint(runRoot);
  // Several durable writes happened (start + data_prepared); the holder
  // is killed without any release path.
  holder.kill();
  await holder.result;
  const probe = join(import.meta.dir, "helpers", "pipeline_v2_lock_probe.ts");
  const probeChild = Bun.spawnSync([process.execPath, probe, "probe", runRoot, "race-run"], {
    cwd: import.meta.dir,
    stdout: "pipe",
  });
  expect(probeChild.stdout.toString().trim()).toBe("FREE");

  // The next mutating command acquires the freed lock and applies the
  // existing recovery/refusal contract — no repair, no unlock command.
  const next = new MutatingChild(specWith(harness, { command: "resume", runId: "race-run" }), harness.root);
  const nextResult = await next.finish();
  expect(nextResult.outcome).toMatchObject({ ok: false, exitCode: 1, reason: "invalid_state" });
  expect(nextResult.stats.create ?? 0).toBe(0);
  expect(await treeFingerprint(runRoot)).toBe(before);
  const state = await readState(harness.stateRoot, "race-run");
  expect(state.status).toBe("active");
}, 120000);

test("matrix: different run ids run fully in parallel", async () => {
  const harness = await buildHarness("race-parallel");
  const first = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run-a",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
    gateWorkerRun: true,
  }), harness.root, new Set(["run"]));
  await first.gate("run");
  const second = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run-b",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  }), harness.root);
  const secondResult = await second.finish();
  expect(secondResult.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0, runId: "race-run-b" });
  // A mutating command on the second run proceeds while the first lock is
  // held (a refused resume is the new waiting boundary's refusal, not
  // `run_busy`).
  const other = new MutatingChild(specWith(harness, { command: "resume", runId: "race-run-b" }), harness.root);
  const otherResult = await other.finish();
  expect(otherResult.outcome.reason).not.toBe("run_busy");
  first.release();
  const firstResult = await first.finish();
  expect(firstResult.outcome).toMatchObject({ ok: false, waiting: true, exitCode: 0, runId: "race-run-a" });
}, 120000);

test("matrix: the durable state stays loader-valid after every race", async () => {
  const harness = await buildHarness("race-valid");
  const fresh = new MutatingChild(specWith(harness, {
    command: "run",
    runId: "race-run",
    projectSource: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
    gateWorkerRun: true,
  }), harness.root, new Set(["run"]));
  await fresh.gate("run");
  await waitForStateFile(harness.stateRoot, "race-run");
  const loser = new MutatingChild(specWith(harness, {
    command: "resume",
    runId: "race-run",
    waitStart: true,
  }), harness.root);
  loser.release();
  const loserResult = await loser.finish();
  expect(loserResult.outcome).toMatchObject({ reason: "run_busy", state: null });
  fresh.release();
  await fresh.finish();
  const raw = await readFile(join(harness.stateRoot, "pipeline-runs", "race-run", "state.json"), "utf8");
  const state = parsePipelineV2RunState(raw);
  expect(state.status).toBe("waiting");
  expect(state.revision).toBeGreaterThan(0);
}, 120000);
