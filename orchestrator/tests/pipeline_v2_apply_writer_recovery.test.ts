/**
 * Suite C — the crash/recovery matrix of the project apply filesystem
 * writer. Every window simulates abrupt process death through a marked
 * fault in an injected seam (fd operations, payload stream or journal
 * commit); the process "dies" by rethrowing without any cleanup or
 * journal write. A fresh reopened attempt then classifies the durable
 * state through the recovery walk and rolls forward — with zero worker,
 * run-workspace or endpoint dependencies.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { readFile, rm, writeFile, mkdir, symlink } from "node:fs/promises";
import { join } from "node:path";
import {
  applyPreparedPipelineV2Project,
  inspectPipelineV2ProjectApplyOperation,
  preparePipelineV2ProjectApply,
} from "../src/pipeline_v2_project_apply.ts";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  realImmutableDocumentIo,
  realProjectFileStreamIo,
  faultFdOps,
  faultStreamIo,
  setLibcErrno,
  type ApplyFixtureEntry,
} from "./apply_fixtures.ts";
import { defaultPipelineStateIo, type PipelineStateIo } from "../src/run_snapshot_store.ts";
import { realApplyFdOps } from "../src/pipeline_v2_apply_fd_internal.ts";
import { isApplyCrashFault } from "../src/pipeline_v2_apply_crash_internal.ts";
import { applyPreparedProjectCore } from "../src/pipeline_v2_project_apply_writer_internal.ts";

const ROOTS: string[] = [];
afterAll(async () => {
  await disposeApplyRoots();
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true }).catch(() => undefined);
  }
});

type Entry = ApplyFixtureEntry;
type Fixture = Awaited<ReturnType<typeof buildFinalizedApplyRun>>;

async function prepared(
  label: string,
  spec: Entry[],
  worker?: (ws: string) => Promise<void>,
): Promise<Fixture> {
  const fixture = await buildFinalizedApplyRun(`recovery-${label}`, { source: spec, worker });
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

function components(overrides?: { fdOps?: ReturnType<typeof faultFdOps>["ops"]; stateIo?: PipelineStateIo; stream?: ReturnType<typeof faultStreamIo>["io"] }) {
  return {
    docIo: realImmutableDocumentIo,
    stateIo: overrides?.stateIo ?? defaultPipelineStateIo,
    fileStreamIo: overrides?.stream ?? realProjectFileStreamIo,
    fdOps: overrides?.fdOps ?? realApplyFdOps,
  };
}

async function attempt(
  fixture: Fixture,
  overrides?: Parameters<typeof components>[0],
): Promise<Awaited<ReturnType<typeof applyPreparedProjectCore>>> {
  return await applyPreparedProjectCore(
    { applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone", projectId: "proj1", projectRoot: fixture.operatorProject },
    components(overrides),
  );
}

const tempFaultMatch = (args: unknown[]): boolean => String((args[1] as Buffer).toString("utf8")).startsWith(".apply-tmp-");

/** A journal seam that crashes the commit whose payload records `completed` progress. */
function crashAtProgressCommit(target: number): PipelineStateIo {
  const inner = defaultPipelineStateIo;
  return {
    ...inner,
    openExclusive: async (path: string, mode: number) => {
      const handle = await inner.openExclusive(path, mode);
      return {
        chmod: (m: number) => handle.chmod(m),
        writeAll: async (bytes: Uint8Array) => {
          const document = JSON.parse(Buffer.from(bytes).toString("utf8")) as Record<string, unknown>;
          const execution = document["execution"] as Record<string, unknown> | null;
          if (
            document["status"] === "applying" &&
            execution !== null &&
            execution["completed_step_count"] === target &&
            execution["active_step"] === null
          ) {
            throw isApplyCrashFaultMarker(`progress commit at ${target}`);
          }
          await handle.writeAll(bytes);
        },
        sync: () => handle.sync(),
        close: () => handle.close(),
      };
    },
  } as PipelineStateIo;
}

function isApplyCrashFaultMarker(_detail: string): Error {
  const error = new Error("injected crash fault at the progress commit");
  Object.defineProperty(error, "applyCrashFault", { value: true });
  return error;
}

async function readJournal(fixture: Fixture): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "state.json"), "utf8"));
}

const BASIC_SPEC: Entry[] = [
  { path: "gone.txt", kind: "file", content: "GONE" },
  { path: "chg.txt", kind: "file", content: "OLD" },
];
const BASIC_WORKER = async (ws: string): Promise<void> => {
  await rm(join(ws, "gone.txt"));
  await writeFile(join(ws, "chg.txt"), "NEW");
};

describe("apply writer recovery: pre-execution windows", () => {
  test("W0: a crash while opening the project root leaves the operation prepared", async () => {
    const fixture = await prepared("w0", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, (args) => args[0] === -100);
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
    expect(inspection.status).toBe("prepared");
    expect(inspection.execution).toBe(null);
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
  });

  test("a busy project lock refuses with zero effects and never opens the store", async () => {
    const fixture = await prepared("w1-busy", BASIC_SPEC, BASIC_WORKER);
    const { acquirePipelineV2ApplyProjectLock } = await import("../src/pipeline_v2_apply_project_lock_internal.ts");
    const holder = acquirePipelineV2ApplyProjectLock(fixture.operatorProject);
    expect(holder.busy).toBe(false);
    if (holder.busy) return;
    try {
      const result = await attempt(fixture);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe("project_busy");
        expect(result.status).toBe("prepared");
        expect(result.completed_step_count).toBe(null);
      }
      const inspection = await inspectPipelineV2ProjectApplyOperation({ applyStoreRoot: fixture.applyStoreRoot, applyId: "applyone" });
      expect(inspection.status).toBe("prepared");
    } finally {
      holder.lock.release();
    }
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
  });

  test("W2: a crash inside the preflight walk leaves the operation prepared and retryable", async () => {
    const fixture = await prepared("w2", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("readFd", 1);
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    expect((await readJournal(fixture))["status"]).toBe("prepared");
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
  });

  test("a graceful preflight failure commits a clean conflict", async () => {
    const fixture = await prepared("w2-graceful", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    let refused = false;
    fops.override("openat", (dirfd, name, flags, mode) => {
      if (dirfd === -100) {
        return realApplyFdOps.openat(dirfd as number, name as Buffer, flags as number, mode as number);
      }
      if (!refused) {
        refused = true;
        setLibcErrno(13);
        return -1;
      }
      fops.override("openat", null);
      return realApplyFdOps.openat(dirfd as number, name as Buffer, flags as number, mode as number);
    });
    const result = await attempt(fixture, { fdOps: fops.ops });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toBe("unsupported_metadata");
    expect((await readJournal(fixture))["status"]).toBe("conflict");
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(false);
    if (!retry.ok) expect(retry.reason).toBe("unsupported_metadata");
  });

  test("W3: the applying transition crash leaves prepared; durability-unknown adopts the candidate", async () => {
    const fixture = await prepared("w3", BASIC_SPEC, BASIC_WORKER);
    let inner = defaultPipelineStateIo;
    const failing: PipelineStateIo = {
      ...inner,
      openExclusive: async (path: string, mode: number) => {
        const handle = await inner.openExclusive(path, mode);
        return {
          chmod: (m: number) => handle.chmod(m),
          writeAll: async (bytes: Uint8Array) => {
            const document = JSON.parse(Buffer.from(bytes).toString("utf8")) as Record<string, unknown>;
            if (document["status"] === "applying") {
              const error = new Error("injected commit failure");
              (error as unknown as { code: string }).code = "EACCES";
              throw error;
            }
            await handle.writeAll(bytes);
          },
          sync: () => handle.sync(),
          close: () => handle.close(),
        };
      },
    } as PipelineStateIo;
    const refused = await attempt(fixture, { stateIo: failing });
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("state_persist_failed");
    expect((await readJournal(fixture))["status"]).toBe("prepared");
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
  });
});

describe("apply writer recovery: temp ownership windows", () => {
  test("W5: a crash after the temp intent leaves no temp; the retry recreates at the committed name", async () => {
    const fixture = await prepared("w5", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, tempFaultMatch);
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const state = await readJournal(fixture);
    expect(state["status"]).toBe("applying");
    const execution = state["execution"] as Record<string, unknown>;
    expect(execution["completed_step_count"]).toBe(1);
    const intent = execution["active_step"] as Record<string, unknown>;
    expect(String(intent["temp_name"])).toMatch(/^\.apply-tmp-[0-9a-f]{32}$/);
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    const finalState = await readJournal(fixture);
    expect(finalState["status"]).toBe("applied");
  });

  test("W6/W7: an incomplete committed temp is cleaned and recreated under a fresh intent", async () => {
    const fixture = await prepared("w6", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, tempFaultMatch);
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const state = await readJournal(fixture);
    const intent = (state["execution"] as Record<string, unknown>)["active_step"] as Record<string, unknown>;
    const tempName = String(intent["temp_name"]);
    // Simulate the crash states: an empty temp (W6) and a partial temp (W7).
    for (const partial of [Buffer.alloc(0), Buffer.from("NEW".slice(0, 1))]) {
      await writeFile(join(fixture.operatorProject, tempName), partial as Buffer, { mode: 0o600 });
      const retry = await attempt(fixture);
      expect(retry.ok).toBe(true);
      const finalState = await readJournal(fixture);
      expect(finalState["status"]).toBe("applied");
    }
  });

  test("a stream read crash mid-copy leaves a partial temp that the retry cleans", async () => {
    const fixture = await prepared("w7-live", BASIC_SPEC, BASIC_WORKER);
    const stream = faultStreamIo();
    stream.failReadCrashOnce(2);
    let crashed = false;
    try {
      await attempt(fixture, { stream: stream.io });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const listing = await (await import("node:fs/promises")).readdir(fixture.operatorProject);
    expect(listing.some((name) => name.startsWith(".apply-tmp-"))).toBe(true);
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    const after = await (await import("node:fs/promises")).readdir(fixture.operatorProject);
    expect(after.some((name) => name.startsWith(".apply-tmp-"))).toBe(false);
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
  });

  test("W8/W9: a complete committed temp is adopted and published by the retry", async () => {
    const fixture = await prepared("w8", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, tempFaultMatch);
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const state = await readJournal(fixture);
    const tempName = String(((state["execution"] as Record<string, unknown>)["active_step"] as Record<string, unknown>)["temp_name"]);
    const payload = await readFile(join(fixture.applyStoreRoot, "operations", "applyone", "payloads", "1.payload"));
    await writeFile(join(fixture.operatorProject, tempName), payload, { mode: 0o600 });
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
    const listing = await (await import("node:fs/promises")).readdir(fixture.operatorProject);
    expect(listing.some((name) => name.startsWith(".apply-tmp-"))).toBe(false);
  });
});

describe("apply writer recovery: post-publication windows", () => {
  test("W10-W12: a publication whose progress commit crashed is adopted by the recovery walk", async () => {
    const fixture = await prepared("w12", BASIC_SPEC, BASIC_WORKER);
    const refusing = crashAtProgressCommit(2);
    const refused = await attempt(fixture, { stateIo: refusing });
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("state_persist_failed");
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
    expect((await (await import("node:fs/promises")).stat(join(fixture.operatorProject, "gone.txt")).catch(() => null))).toBe(null);
    expect((await readJournal(fixture))["status"]).toBe("applying");
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    expect((await readJournal(fixture))["status"]).toBe("applied");
  });

  test("W13: a durability-unknown progress commit adopts the candidate and continues", async () => {
    const fixture = await prepared("w13", BASIC_SPEC, BASIC_WORKER);
    const { faultStateIo } = await import("./apply_fixtures.ts");
    const stateFaults = faultStateIo();
    stateFaults.failCommitOnce("dirfsync", 3);
    const result = await attempt(fixture, { stateIo: stateFaults.io });
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.outcome).toBe("applied");
  });

  test("W15: a removal whose progress commit crashed is adopted as a completed step", async () => {
    const fixture = await prepared("w15", BASIC_SPEC, BASIC_WORKER);
    const refusing = crashAtProgressCommit(1);
    const refused = await attempt(fixture, { stateIo: refusing });
    expect(refused.ok).toBe(false);
    expect((await (await import("node:fs/promises")).stat(join(fixture.operatorProject, "gone.txt")).catch(() => null))).toBe(null);
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    expect((await readJournal(fixture))["status"]).toBe("applied");
  });

  test("W16/W17: a crash after the last progress commit (final walk or applied commit) is completed by the retry", async () => {
    const fixture = await prepared("w17", BASIC_SPEC, BASIC_WORKER);
    // The last progress commit arms the very next project openat — the
    // first observation of the final verification walk. A crash between
    // the final verification and the applied commit leaves the identical
    // durable state (everything published, completed = plan step count,
    // no applied status), so one window covers both crash points.
    const fops = faultFdOps();
    let armed = false;
    let fired = false;
    const realOpenat = fops.ops.openat;
    const counting = {
      ...fops.ops,
      openat: ((...args: unknown[]) => {
        if (armed) {
          armed = false;
          fired = true;
          throw isApplyCrashFaultMarker("final walk crash");
        }
        return (realOpenat as (...a: unknown[]) => unknown)(...args);
      }) as typeof fops.ops.openat,
    };
    const inner = defaultPipelineStateIo;
    const failing: PipelineStateIo = {
      ...inner,
      openExclusive: async (path: string, mode: number) => {
        const handle = await inner.openExclusive(path, mode);
        return {
          chmod: (m: number) => handle.chmod(m),
          writeAll: async (bytes: Uint8Array) => {
            const document = JSON.parse(Buffer.from(bytes).toString("utf8")) as Record<string, unknown>;
            const execution = document["execution"] as Record<string, unknown> | null;
            if (
              document["status"] === "applying" &&
              execution !== null &&
              execution["active_step"] === null &&
              execution["completed_step_count"] === 2
            ) {
              armed = true;
            }
            await handle.writeAll(bytes);
          },
          sync: () => handle.sync(),
          close: () => handle.close(),
        };
      },
    } as PipelineStateIo;
    let crashed = false;
    try {
      await attempt(fixture, { stateIo: failing, fdOps: counting as typeof fops.ops });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    expect(fired).toBe(true);
    expect((await readJournal(fixture))["status"]).toBe("applying");
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    expect((await readJournal(fixture))["status"]).toBe("applied");
  });
});

describe("apply writer recovery: restart at every prefix", () => {
  const SPEC: Entry[] = [
    { path: "gone.txt", kind: "file", content: "GONE" },
    { path: "chg.txt", kind: "file", content: "OLD" },
    { path: "new/added.txt", kind: "file", content: "ADDED" },
  ];
  const WORKER = async (ws: string): Promise<void> => {
    await rm(join(ws, "gone.txt"));
    await writeFile(join(ws, "chg.txt"), "NEW");
    await mkdir(join(ws, "new"), { recursive: true });
    await writeFile(join(ws, "new/added.txt"), "ADDED-BY-WORKER");
  };

  test("W20: after every completed prefix a fresh attempt completes the plan", async () => {
    const probe = await prepared("w20-probe", SPEC, WORKER);
    const planDocument = JSON.parse(
      await readFile(join(probe.applyStoreRoot, "operations", "applyone", "plan.json"), "utf8"),
    ) as { steps: unknown[] };
    const stepCount = planDocument.steps.length;
    await rm(join(probe.applyStoreRoot, "operations"), { recursive: true, force: true });
    for (let prefix = 0; prefix < stepCount; prefix += 1) {
      const fixture = await prepared(`w20-${prefix}`, SPEC, WORKER);
      const fops = faultFdOps();
      let mutations = 0;
      
      const originalWrap = fops.ops;
      void originalWrap;
      const counting: typeof fops.ops = {
        ...fops.ops,
        unlinkat: (...args: Parameters<typeof fops.ops.unlinkat>) => {
          mutations += 1;
          if (mutations === prefix + 1) {
            throw isApplyCrashFaultMarker(`mutation ${mutations}`);
          }
          return fops.ops.unlinkat(...args);
        },
        mkdirat: (...args: Parameters<typeof fops.ops.mkdirat>) => {
          mutations += 1;
          if (mutations === prefix + 1) {
            throw isApplyCrashFaultMarker(`mutation ${mutations}`);
          }
          return fops.ops.mkdirat(...args);
        },
        renameat: (...args: Parameters<typeof fops.ops.renameat>) => {
          mutations += 1;
          if (mutations === prefix + 1) {
            throw isApplyCrashFaultMarker(`mutation ${mutations}`);
          }
          return fops.ops.renameat(...args);
        },
        renameat2: (...args: Parameters<typeof fops.ops.renameat2>) => {
          mutations += 1;
          if (mutations === prefix + 1) {
            throw isApplyCrashFaultMarker(`mutation ${mutations}`);
          }
          return fops.ops.renameat2(...args);
        },
        symlinkat: (...args: Parameters<typeof fops.ops.symlinkat>) => {
          mutations += 1;
          if (mutations === prefix + 1) {
            throw isApplyCrashFaultMarker(`mutation ${mutations}`);
          }
          return fops.ops.symlinkat(...args);
        },
      };
      let crashed = false;
      try {
        await attempt(fixture, { fdOps: counting });
      } catch (cause) {
        crashed = isApplyCrashFault(cause);
      }
      expect(crashed, `prefix ${prefix}`).toBe(true);
      const state = await readJournal(fixture);
      expect(state["status"]).toBe("applying");
      expect(((state["execution"] as Record<string, unknown>)["completed_step_count"] as number)).toBe(prefix);
      const retry = await attempt(fixture);
      expect(retry.ok, `prefix ${prefix} retry`).toBe(true);
      expect((await readJournal(fixture))["status"]).toBe("applied");
      expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
      expect((await readFile(join(fixture.operatorProject, "new/added.txt"))).toString()).toBe("ADDED-BY-WORKER");
      expect((await (await import("node:fs/promises")).stat(join(fixture.operatorProject, "gone.txt")).catch(() => null))).toBe(null);
    }
  });

  test("the run workspace may disappear entirely before execution", async () => {
    const fixture = await prepared("no-workspace", BASIC_SPEC, BASIC_WORKER);
    await rm(fixture.runRoot, { recursive: true, force: true });
    const result = await attempt(fixture);
    expect(result.ok).toBe(true);
    if (result.ok) expect(result.outcome).toBe("applied");
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
  });

  test("a tampered payload refuses the attempt at load and leaves the project untouched", async () => {
    const fixture = await prepared("payload-tamper", BASIC_SPEC, BASIC_WORKER);
    await writeFile(join(fixture.applyStoreRoot, "operations", "applyone", "payloads", "1.payload"), "TAMPERED");
    const result = await attempt(fixture);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.reason).toBe("artifact_modified");
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("OLD");
  });

  test("a foreign temp from an earlier crash keeps the frontier intent on the recovery refusal", async () => {
    const fixture = await prepared("w-foreign", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, tempFaultMatch);
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch {
      // expected crash
    }
    const state = await readJournal(fixture);
    const tempName = String(((state["execution"] as Record<string, unknown>)["active_step"] as Record<string, unknown>)["temp_name"]);
    await mkdir(join(fixture.operatorProject, tempName));
    const first = await attempt(fixture);
    expect(first.ok).toBe(false);
    if (!first.ok) expect(first.reason).toBe("recovery_required");
    const after = await readJournal(fixture);
    expect(after["status"]).toBe("recovery_required");
    expect((((after["execution"] as Record<string, unknown>)["active_step"]) as Record<string, unknown> | null)?.["temp_name"]).toBe(tempName);
    // a soft retry without removing the foreign object refuses again
    const second = await attempt(fixture);
    expect(second.ok).toBe(false);
    if (!second.ok) expect(second.reason).toBe("recovery_required");
  });

  test("a symlink temp adopted from a committed intent publishes exactly", async () => {
    const fixture = await prepared("w-symlink-temp", [
      { path: "target.txt", kind: "file", content: "TARGET" },
    ], async (ws) => {
      await symlink("target.txt", join(ws, "fresh-link"));
    });
    const fops = faultFdOps();
    let intents = 0;
    const counting: typeof fops.ops = {
      ...fops.ops,
      symlinkat: (...args: Parameters<typeof fops.ops.symlinkat>) => {
        intents += 1;
        if (intents === 1) {
          throw isApplyCrashFaultMarker("temp symlink crash");
        }
        return fops.ops.symlinkat(...args);
      },
    };
    let crashed = false;
    try {
      await attempt(fixture, { fdOps: counting });
    } catch (cause) {
      crashed = isApplyCrashFault(cause);
    }
    expect(crashed).toBe(true);
    const retry = await attempt(fixture);
    expect(retry.ok).toBe(true);
    expect(await (await import("node:fs/promises")).readlink(join(fixture.operatorProject, "fresh-link"))).toBe("target.txt");
  });
});

describe("apply writer recovery: state machine edges", () => {
  test("an executing a preparing or failed operation is refused as apply_not_prepared", async () => {
    const fixture = await buildFinalizedApplyRun("recovery-not-prepared", {
      source: [{ path: "a.txt", kind: "file", content: "A" }],
    });
    // prepare normally, then rewrite the journal into the preparing shape
    await preparePipelineV2ProjectApply({
      applyStoreRoot: fixture.applyStoreRoot,
      applyId: "applyone",
      runId: fixture.runId,
      projectId: "proj1",
      projectRoot: fixture.operatorProject,
      observation: fixture.observation,
      stateRoot: fixture.stateRoot,
    });
    const statePath = join(fixture.applyStoreRoot, "operations", "applyone", "state.json");
    const state = JSON.parse(await readFile(statePath, "utf8")) as Record<string, unknown>;
    const preparing = { ...state, status: "preparing", execution: null };
    await writeFile(statePath, `${JSON.stringify(preparing, null, 2)}\n`);
    const refused = await attempt(fixture);
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("apply_not_prepared");
    expect((JSON.parse(await readFile(statePath, "utf8")) as Record<string, unknown>)["status"]).toBe("preparing");
  });

  test("a recovery_required state heals once the foreign object is removed by the operator", async () => {
    const fixture = await prepared("w-heal", BASIC_SPEC, BASIC_WORKER);
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, tempFaultMatch);
    try {
      await attempt(fixture, { fdOps: fops.ops });
    } catch {
      // expected crash
    }
    const state = await readJournal(fixture);
    const tempName = String(((state["execution"] as Record<string, unknown>)["active_step"] as Record<string, unknown>)["temp_name"]);
    await mkdir(join(fixture.operatorProject, tempName));
    const refused = await attempt(fixture);
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("recovery_required");
    // the operator removes the foreign object; the retry recovers fully
    await rm(join(fixture.operatorProject, tempName), { recursive: true });
    const healed = await attempt(fixture);
    expect(healed.ok).toBe(true);
    if (healed.ok) expect(healed.outcome).toBe("applied");
    expect((await readJournal(fixture))["status"]).toBe("applied");
    expect((await readFile(join(fixture.operatorProject, "chg.txt"))).toString()).toBe("NEW");
  });
});
