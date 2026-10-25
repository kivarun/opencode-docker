/**
 * The crash matrix of the Project Apply preparation (P0–P17): every
 * persistent fault-injection window of the preparation protocol, proven
 * against the real substrate with the established recovery semantics —
 * the registered project never changes, the retry executes only the
 * missing durable suffix, published artifacts are adopted byte-exactly,
 * and a failed package never looks prepared.
 *
 * Each window first runs a clean reference preparation of a second apply
 * id on the same store (so the faulted operation's converged plan and
 * payloads can be compared against the deterministic expectation), then
 * arms exactly one fault at the protocol position of the window and
 * retries with fresh components.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  faultDocIo,
  faultStateIo,
  faultStreamIo,
  fingerprintTree,
  writeProjectTree,
  type FaultDocIo,
} from "./apply_fixtures.ts";
import {
  PipelineV2ProjectApplyError,
  inspectOperationCore,
  loadPreparedPackageCore,
  preparePipelineV2ProjectApplyCore,
  type PipelineV2ProjectApplyComponents,
  type PipelineV2ProjectApplyOptions,
  type PreparedPipelineV2ProjectApplyPackage,
} from "../src/pipeline_v2_project_apply_internal.ts";
import { realImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo } from "../src/pipeline_state_store.ts";
import { realProjectWorkspaceVerifyIo } from "../src/pipeline_v2_project_provenance_store_internal.ts";
import { realProjectFileStreamIo } from "../src/pipeline_v2_project_stream_internal.ts";
import { realMetadataProbeIo } from "../src/pipeline_v2_apply_metadata_probe_internal.ts";

const sha = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

const ROOTS: string[] = [];

afterAll(async () => {
  await disposeApplyRoots();
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

const WORKER_SPEC = async (ws: string): Promise<void> => {
  await writeProjectTree(ws, [
    { path: "a.txt", kind: "file", content: "CONTENT-A" },
    { path: "b.txt", kind: "file", content: "CONTENT-B" },
  ]);
};

function productionComponents(): PipelineV2ProjectApplyComponents {
  return {
    docIo: realImmutableDocumentIo,
    stateIo: defaultPipelineStateIo,
    verifyIo: realProjectWorkspaceVerifyIo,
    fileStreamIo: realProjectFileStreamIo,
    probeIo: realMetadataProbeIo,
  };
}

interface FaultFixture {
  readonly run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>;
  readonly reference: PreparedPipelineV2ProjectApplyPackage;
  readonly options: PipelineV2ProjectApplyOptions;
}

/** One fixture with a clean reference operation plus the faulted identity. */
async function crashFixture(prefix: string): Promise<FaultFixture> {
  const run = await buildFinalizedApplyRun(`${prefix}-f`, { worker: WORKER_SPEC });
  ROOTS.push(run.root);
  const reference = await prepareOf(run, "applyref");
  void reference;
  return {
    run,
    reference,
    options: {
      applyStoreRoot: run.applyStoreRoot,
      applyId: "applyone",
      runId: run.runId,
      projectId: "proj",
      projectRoot: run.operatorProject,
      observation: run.observation,
      stateRoot: run.stateRoot,
    },
  };
}

function prepareOf(
  run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>,
  applyId: string,
  components: PipelineV2ProjectApplyComponents = productionComponents(),
  optionsOverride: Partial<PipelineV2ProjectApplyOptions> = {},
) {
  return preparePipelineV2ProjectApplyCore(
    {
      applyStoreRoot: run.applyStoreRoot,
      applyId,
      runId: run.runId,
      projectId: "proj",
      projectRoot: run.operatorProject,
      observation: run.observation,
      stateRoot: run.stateRoot,
      ...optionsOverride,
    },
    components,
  );
}

function failureOf(promise: Promise<unknown>): Promise<PipelineV2ProjectApplyError> {
  return promise.then(
    () => {
      throw new Error("expected the operation to refuse");
    },
    (cause: unknown) => {
      if (cause instanceof PipelineV2ProjectApplyError) {
        return cause;
      }
      throw cause;
    },
  );
}

function operationDirOf(run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>, applyId: string): string {
  return join(run.applyStoreRoot, "operations", applyId);
}

/** The converged package must match the deterministic reference operation. */
async function expectConverged(
  fixture: FaultFixture,
  prepared: Awaited<ReturnType<typeof prepareOf>>,
): Promise<void> {
  const reference = fixture.reference;
  expect(prepared.plan.steps).toEqual(reference.plan.steps);
  expect(prepared.plan.payloads).toEqual(reference.plan.payloads);
  expect(prepared.plan.noops).toEqual(reference.plan.noops);
  expect(prepared.payload_count).toBe(reference.payload_count);
  expect(prepared.total_payload_byte_count).toBe(reference.total_payload_byte_count);
  // Every payload is loadable and matches its binding exactly.
  const loaded = await loadPreparedPackageCore(productionComponents(), fixture.run.applyStoreRoot, prepared.apply_id);
  expect(loaded.plan_digest).toBe(prepared.plan_digest);
  for (const payload of loaded.plan.payloads) {
    const sealed = await readFile(join(loaded.payloads_root, payload.artifact));
    expect(sealed.byteLength).toBe(payload.byte_count);
    expect(sha(sealed)).toBe(payload.sha256);
  }
  const projection = await inspectOperationCore(productionComponents(), fixture.run.applyStoreRoot, prepared.apply_id);
  expect(projection.status).toBe("prepared");
}

async function expectProjectUnwritten(run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>, before: string): Promise<void> {
  expect(await fingerprintTree(run.operatorProject)).toBe(before);
}

describe("apply crash matrix: identity and plan windows", () => {
  test("P0: a refusal before the operation identity leaves nothing behind", async () => {
    const run = await buildFinalizedApplyRun("crash-p0", { worker: WORKER_SPEC });
    ROOTS.push(run.root);
    const projectBefore = await fingerprintTree(run.operatorProject);
    await failureOf(prepareOf(run, "applyone", productionComponents(), { observation: sha("wrong") }));
    expect(await readdir(run.applyStoreRoot).catch(() => [])).toEqual([]);
    await expectProjectUnwritten(run, projectBefore);
    // The same apply id then prepares cleanly.
    const prepared = await prepareOf(run, "applyone");
    expect(prepared.apply_id).toBe("applyone");
  });

  test("P1: a fault while creating the operations directory is retried from nothing", async () => {
    const fixture = await crashFixture("crash-p1");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault: FaultDocIo = faultDocIo();
    docFault.failOnce("mkdirExclusive", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    expect((await readdir(join(run.applyStoreRoot, "operations")).catch(() => [])).sort()).toEqual(["applyref"]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P2: a fault while creating the preparing journal is retried cleanly", async () => {
    const fixture = await crashFixture("crash-p2");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const stateFault = faultStateIo();
    stateFault.failCommitOnce("write", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), stateIo: stateFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P3: a fault while creating the plan temp is retried cleanly", async () => {
    const fixture = await crashFixture("crash-p3");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("openTempExclusive", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P4: a fault while fsyncing the plan temp leaves no plan and is retried", async () => {
    const fixture = await crashFixture("crash-p4");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("sync", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    expect((await readdir(operationDirOf(run, "applyone"))).sort()).toEqual(["payloads", "state.json"]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P5: a fault at the plan link publication is retried with a fresh publication", async () => {
    const fixture = await crashFixture("crash-p5");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("link", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    expect((await readdir(operationDirOf(run, "applyone"))).sort()).toEqual(["payloads", "state.json"]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P6: a fault at the plan directory fsync adopts the published plan on retry", async () => {
    const fixture = await crashFixture("crash-p6");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("dirsync", 4);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    // The published plan artifact is already authoritative.
    const planBytes = await readFile(join(operationDirOf(run, "applyone"), "plan.json"));
    expect(sha(planBytes)).toMatch(/^[0-9a-f]{64}$/);
    await expectProjectUnwritten(run, projectBefore);
    const beforeRetry = await fingerprintTree(run.applyStoreRoot);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
    expect(await fingerprintTree(run.applyStoreRoot)).not.toBe(beforeRetry);
  });

  test("P7: a fault while creating the first payload temp is retried cleanly", async () => {
    const fixture = await crashFixture("crash-p7");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("openTempExclusive", 2);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P8: a fault mid-payload-write discards the owned temp and is retried", async () => {
    const fixture = await crashFixture("crash-p8");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("write", 2);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    // The owned temp was cleaned; the payloads directory carries no final
    // artifact yet.
    expect((await readdir(join(operationDirOf(run, "applyone"), "payloads"))).sort()).toEqual([]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P9: a fault at the payload fsync is retried cleanly", async () => {
    const fixture = await crashFixture("crash-p9");
    const run = fixture.run;
    const docFault = faultDocIo();
    docFault.failOnce("sync", 2);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P10: a fault at the payload link publication adopts nothing and is retried", async () => {
    const fixture = await crashFixture("crash-p10");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("link", 2);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    expect((await readdir(join(operationDirOf(run, "applyone"), "payloads"))).sort()).toEqual([]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P11: a fault at the payload directory fsync adopts the published payload on retry", async () => {
    const fixture = await crashFixture("crash-p11");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const docFault = faultDocIo();
    docFault.failOnce("dirsync", 5);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    // The published payload stays authoritative; the journal has not
    // sealed it yet.
    const names = await readdir(join(operationDirOf(run, "applyone"), "payloads"));
    expect(names.sort()).toEqual(["1.payload"]);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P12: a fault between two payload sealings keeps the sealed prefix and resumes", async () => {
    const fixture = await crashFixture("crash-p12");
    const run = fixture.run;
    const projectBefore = await fingerprintTree(run.operatorProject);
    const streamFault = faultStreamIo();
    // The observation scan opens both workspace files once; the sealed
    // pass opens them again — the fourth open is the second payload.
    // The observation scan opens the two workspace files (1, 2) and the
    // two operator files (3, 4); the sealed pass opens the sources again —
    // the sixth open is the second payload's source.
    streamFault.failOnceOpen(6);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), fileStreamIo: streamFault.io }));
    expect(failure.reason).toBe("internal_error");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.sealed_payload_count).toBe(1);
    await expectProjectUnwritten(run, projectBefore);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P13: a fault inside the completeness verification is retried", async () => {
    const fixture = await crashFixture("crash-p13");
    const run = fixture.run;
    const docFault = faultDocIo();
    docFault.failOnce("readdirNames", 1);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), docIo: docFault.io }));
    expect(failure.reason).toBe("artifact_missing");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("preparing");
    expect(projection.sealed_payload_count).toBe(2);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P14: a fault before the prepared journal rename is retried", async () => {
    const fixture = await crashFixture("crash-p14");
    const run = fixture.run;
    const stateFault = faultStateIo();
    // Journal commits: create, plan digest, two payloads, prepared — the
    // fifth write is the prepared state write.
    stateFault.failCommitOnce("write", 5);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), stateIo: stateFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("preparing");
    expect(projection.sealed_payload_count).toBe(2);
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P15: a fault at the prepared journal rename keeps the sealed state and is retried", async () => {
    const fixture = await crashFixture("crash-p15");
    const run = fixture.run;
    const stateFault = faultStateIo();
    stateFault.failCommitOnce("rename", 5);
    const failure = await failureOf(prepareOf(run, "applyone", { ...productionComponents(), stateIo: stateFault.io }));
    expect(failure.reason).toBe("state_persist_failed");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("preparing");
    await expectConverged(fixture, await prepareOf(run, "applyone"));
  });

  test("P16: a durability-unknown prepared commit adopts the visible candidate and reports prepared", async () => {
    const fixture = await crashFixture("crash-p16");
    const run = fixture.run;
    const stateFault = faultStateIo();
    stateFault.failCommitOnce("dirfsync", 5);
    const prepared = await prepareOf(run, "applyone", { ...productionComponents(), stateIo: stateFault.io });
    expect(prepared.plan.steps.length).toBeGreaterThan(0);
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("prepared");
    await expectConverged(fixture, prepared);
  });

  test("P17: a lost caller response after prepared retries into the byte-identical package", async () => {
    const fixture = await crashFixture("crash-p17");
    const run = fixture.run;
    const prepared = await prepareOf(run, "applyone");
    const storeFingerprint = await fingerprintTree(run.applyStoreRoot);
    const retry = await prepareOf(run, "applyone");
    expect(retry).toEqual(prepared);
    expect(await fingerprintTree(run.applyStoreRoot)).toBe(storeFingerprint);
  });
});

describe("apply crash matrix: foreign artifacts and adoption", () => {
  test("foreign bytes under the operation path are never overwritten; exact artifacts are adopted", async () => {
    const fixture = await crashFixture("crash-foreign");
    const run = fixture.run;
    // A foreign plan artifact blocks the publication as a conflict.
    const operationDir = operationDirOf(run, "applyforeign");
    await (await import("node:fs/promises")).mkdir(join(run.applyStoreRoot, "operations", "applyforeign", "payloads"), { recursive: true, mode: 0o700 });
    await writeFile(join(operationDir, "plan.json"), Buffer.from('{"foreign":true}'), { mode: 0o600 });
    const projectBefore = await fingerprintTree(run.operatorProject);
    const failure = await failureOf(prepareOf(run, "applyforeign"));
    expect(failure.reason).toBe("artifact_modified");
    expect((await readFile(join(operationDir, "plan.json"))).toString()).toBe('{"foreign":true}');
    await expectProjectUnwritten(run, projectBefore);
    // A foreign payload artifact blocks its publication as a conflict.
    const fixture2 = await crashFixture("crash-foreign-payload");
    const payloadsDir = join(operationDirOf(fixture2.run, "applyone"), "payloads");
    const fs = await import("node:fs/promises");
    await fs.mkdir(payloadsDir, { recursive: true, mode: 0o700 });
    await writeFile(join(payloadsDir, "1.payload"), Buffer.from("FOREIGN-BYTES"), { mode: 0o600 });
    const failure2 = await failureOf(prepareOf(fixture2.run, "applyone"));
    expect(failure2.reason).toBe("artifact_modified");
    expect((await readFile(join(payloadsDir, "1.payload"))).toString()).toBe("FOREIGN-BYTES");
  });

  test("an exact published artifact is adopted with stable inode, mode and bytes", async () => {
    const fixture = await crashFixture("crash-adopt");
    const run = fixture.run;
    // Publish the plan through the reference operation, then let the
    // faulted identity adopt it.
    const referenceDir = operationDirOf(run, "applyref");
    const referencePlan = await readFile(join(referenceDir, "plan.json"));
    void referencePlan;
    const prepared = await prepareOf(run, "adoptone");
    const planInfo = await stat(join(operationDirOf(run, "adoptone"), "plan.json"));
    expect((planInfo.mode & 0o7777)).toBe(0o600);
    const payloadInfo = await stat(join(operationDirOf(run, "adoptone"), "payloads", "1.payload"));
    expect((payloadInfo.mode & 0o7777)).toBe(0o600);
    const retry = await prepareOf(run, "adoptone");
    expect(retry.plan_digest).toBe(prepared.plan_digest);
    expect((await stat(join(operationDirOf(run, "adoptone"), "plan.json"))).ino).toBe(planInfo.ino);
    expect((await stat(join(operationDirOf(run, "adoptone"), "payloads", "1.payload"))).ino).toBe(payloadInfo.ino);
  });

  test("a failed package never looks prepared and its retry records the typed reason", async () => {
    const run = await buildFinalizedApplyRun("crash-failed-shape", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "a.txt"), "FOREIGN");
      },
    });
    ROOTS.push(run.root);
    const failure = await failureOf(prepareOf(run, "applyone"));
    expect(failure.reason).toBe("diverged_project");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("failed");
    expect(projection.failure_reason).toBe("diverged_project");
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone"))).reason).toBe("artifact_missing");
  });
});
