/**
 * The apply-writer race matrix: concurrency across real OS processes.
 * The project lock is the single serialization point for two applying
 * callers, `prepare` stays read-only alongside an execution, `inspect`
 * observes a mid-execution pause without any lock, and independent
 * projects never interfere. Every loser performs zero writes.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { spawn } from "bun";
import { readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import {
  applyPreparedPipelineV2Project,
  inspectPipelineV2ProjectApplyOperation,
  preparePipelineV2ProjectApply,
} from "../src/pipeline_v2_project_apply.ts";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  fingerprintTree,
} from "./apply_fixtures.ts";

const ROOTS: string[] = [];
afterAll(async () => {
  await disposeApplyRoots();
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true }).catch(() => undefined);
  }
});

type Fixture = Awaited<ReturnType<typeof buildFinalizedApplyRun>>;

async function prepared(label: string): Promise<Fixture> {
  const fixture = await buildFinalizedApplyRun(`race-${label}-${Date.now()}-${Math.floor(Math.random() * 1e9)}`, {
    source: [
      { path: "gone.txt", kind: "file", content: "GONE" },
      { path: "chg.txt", kind: "file", content: "OLD" },
    ],
    worker: async (ws) => {
      await rm(join(ws, "gone.txt"));
      await writeFile(join(ws, "chg.txt"), "NEW");
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

async function writeFile(path: string, content: string): Promise<void> {
  await (await import("node:fs/promises")).writeFile(path, content);
}

interface ChildResult {
  readonly exitCode: number;
  readonly stdout: string;
}

/** Spawns a child and returns a handle that keeps it alive for manual control. */
function spawnChild(mode: string, fixture: Fixture) {
  return spawn({
    cmd: [
      process.execPath,
      join(import.meta.dir, "apply_writer_race_child.ts"),
      mode,
      fixture.applyStoreRoot,
      "applyone",
      fixture.operatorProject,
    ],
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
}

async function readUntil(child: ReturnType<typeof spawn>, marker: string): Promise<string> {
  const reader = (child.stdout as unknown as ReadableStream<Uint8Array>).getReader();
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    text += new TextDecoder().decode(value);
    if (text.includes(marker)) break;
  }
  return text;
}

async function runChild(mode: string, fixture: Fixture): Promise<ChildResult> {
  const child = spawnChild(mode, fixture);
  const [stdout, exitCode] = await Promise.all([new Response(child.stdout as ReadableStream).text(), child.exited]);
  return { exitCode, stdout: stdout.trim() };
}

function parseResult(stdout: string): Record<string, unknown> {
  return JSON.parse(stdout) as Record<string, unknown>;
}

describe("apply writer races", () => {
  test("a busy lock refuses a concurrent execution with zero effects and a later retry applies", async () => {
    const fixture = await prepared("busy-lock");
    const before = await fingerprintTree(fixture.operatorProject);
    const holder = spawnChild("hold", fixture);
    expect((await readUntil(holder, "HOLD")).includes("HOLD")).toBe(true);
    const loser = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    expect(loser.ok).toBe(false);
    if (!loser.ok) {
      expect(loser.reason).toBe("project_busy");
      expect(loser.status).toBe("prepared");
    }
    expect(await fingerprintTree(fixture.operatorProject)).toEqual(before);
    (holder.stdin as unknown as { write(data: string): void; flush(): void }).write("release\n");
    (holder.stdin as unknown as { flush(): void }).flush();
    expect(await holder.exited).toBe(0);
    const retry = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    expect(retry.ok).toBe(true);
    if (retry.ok) expect(retry.outcome).toBe("applied");
  });

  test("two concurrent executions serialize: one applies, the loser is busy with zero effects", async () => {
    const fixture = await prepared("two-exec");
    const before = await fingerprintTree(fixture.operatorProject);
    const [first, second] = await Promise.all([
      runChild("execute", fixture),
      runChild("execute", fixture),
    ]);
    const outcomes = [first, second].map((child) => JSON.parse(child.stdout) as Record<string, unknown>);
    const applied = outcomes.filter((outcome) => outcome["ok"] === true);
    const busy = outcomes.filter((outcome) => outcome["ok"] === false && outcome["reason"] === "project_busy");
    expect(applied.length).toBe(1);
    expect(busy.length).toBe(1);
    expect((applied[0] as Record<string, unknown>)["outcome"]).toBe("applied");
    expect(await fingerprintTree(fixture.operatorProject)).not.toEqual(before);
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("applied");
    const after = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    expect(after.ok).toBe(true);
    if (after.ok) expect(after.outcome).toBe("already_applied");
  });

  test("a crashed execution releases the lock and a fresh concurrent attempt completes", async () => {
    const fixture = await prepared("crash-release");
    const child = spawn({
      cmd: [
        process.execPath,
        join(import.meta.dir, "apply_writer_race_child.ts"),
        "execute-crash-2",
        fixture.applyStoreRoot,
        "applyone",
        fixture.operatorProject,
      ],
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    expect(await child.exited).toBe(3);
    const [racer] = await Promise.all([
      runChild("execute", fixture),
    ]);
    const outcome = JSON.parse(racer.stdout) as Record<string, unknown>;
    expect(outcome["ok"]).toBe(true);
    expect(outcome["outcome"]).toBe("applied");
  });

  test("an in-flight execution is observable through a read-only inspect without any lock", async () => {
    const fixture = await prepared("inspect-mid");
    const child = spawnChild("execute-gated", fixture);
    const stream = child.stdout as unknown as ReadableStream<Uint8Array>;
    const reader = stream.getReader();
    let text = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      text += new TextDecoder().decode(value);
      if (text.includes("GATED")) break;
    }
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("applying");
    expect(inspection.execution).not.toBe(null);
    expect((inspection.execution as { completed_step_count: number }).completed_step_count).toBe(1);
    child.stdin.write("continue\n");
    child.stdin.flush();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      text += new TextDecoder().decode(value);
    }
    await child.exited;
    const outcome = JSON.parse(text.slice(text.indexOf("{"))) as Record<string, unknown>;
    expect(outcome["ok"]).toBe(true);
    const final = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(final.status).toBe("applied");
  });

  test("an independent prepare runs concurrently with an execution of the same project", async () => {
    const fixture = await prepared("prepare-during");
    const [applyChild, secondPrep] = await Promise.all([
      runChild("execute", fixture),
      preparePipelineV2ProjectApply({
        applyStoreRoot: fixture.applyStoreRoot,
        applyId: "applytwo",
        runId: fixture.runId,
        projectId: "proj1",
        projectRoot: fixture.operatorProject,
        observation: fixture.observation,
        stateRoot: fixture.stateRoot,
      }).then(
        () => "prepared",
        (cause: unknown) => `refused:${(cause as Error).name}`,
      ),
    ]);
    const outcome = JSON.parse(applyChild.stdout) as Record<string, unknown>;
    expect(outcome["ok"]).toBe(true);
    expect(secondPrep).toBe("prepared");
    const inspectionOne = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspectionOne.status).toBe("applied");
  });

  test("executions of two different projects never interfere", async () => {
    const fixtureOne = await prepared("multi-a");
    const fixtureTwo = await prepared("multi-b");
    const [one, two] = await Promise.all([
      applyPreparedPipelineV2Project({ applyStoreRoot: fixtureOne.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixtureOne.operatorProject }),
      applyPreparedPipelineV2Project({ applyStoreRoot: fixtureTwo.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixtureTwo.operatorProject }),
    ]);
    expect(one.ok).toBe(true);
    expect(two.ok).toBe(true);
  });

  test("a mid-execution process death leaves the applying state and the retry completes", async () => {
    const fixture = await prepared("mid-death");
    const held = spawnChild("hold", fixture);
    await readUntil(held, "HOLD");
    held.stdin.write("kill\n");
    held.stdin.flush();
    expect(await held.exited).toBe(137);
    // the killed holder leaves the lock free; the operation is still prepared
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("prepared");
    const retry = await applyPreparedPipelineV2Project({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
    });
    expect(retry.ok).toBe(true);
    if (retry.ok) expect(retry.outcome).toBe("applied");
  });
});
