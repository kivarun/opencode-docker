/**
 * The durable Project Apply preparation package facade
 * (`pipeline_v2_project_apply.ts` + internal core): the trusted-internal
 * authority contract, the operation identity and idempotent retry, the
 * read-only package loader, the zero-write proof over the registered
 * project, the leakage boundary and the single-owner source scans.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, readdir, rm, stat, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dlopen, FFIType } from "bun:ffi";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  fingerprintTree,
  writeProjectTree,
} from "./apply_fixtures.ts";
import {
  PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS,
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

function productionComponents(): PipelineV2ProjectApplyComponents {
  return {
    docIo: realImmutableDocumentIo,
    stateIo: defaultPipelineStateIo,
    verifyIo: realProjectWorkspaceVerifyIo,
    fileStreamIo: realProjectFileStreamIo,
    probeIo: realMetadataProbeIo,
  };
}

function optionsOf(
  run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>,
  overrides: Partial<PipelineV2ProjectApplyOptions> = {},
): PipelineV2ProjectApplyOptions {
  return {
    applyStoreRoot: run.applyStoreRoot,
    applyId: "applyone",
    runId: run.runId,
    projectId: "proj",
    projectRoot: run.operatorProject,
    observation: run.observation,
    stateRoot: run.stateRoot,
    ...overrides,
  };
}

async function prepare(
  run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>,
  overrides: Partial<PipelineV2ProjectApplyOptions> = {},
  components: PipelineV2ProjectApplyComponents = productionComponents(),
): Promise<PreparedPipelineV2ProjectApplyPackage> {
  return await preparePipelineV2ProjectApplyCore(optionsOf(run, overrides), components);
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

async function makeFifo(path: string): Promise<void> {
  const libc = dlopen("libc.so.6", {
    mkfifo: { args: ["cstring", FFIType.u32], returns: FFIType.i64 },
  });
  const rc = Number(libc.symbols.mkfifo(Buffer.from(`${path}\0`), 0o600));
  if (rc !== 0) {
    throw new Error("the test environment could not create a FIFO");
  }
}

describe("apply preparation facade: identity and idempotency", () => {
  test("a successful preparation returns a frozen self-contained package", async () => {
    const run = await buildFinalizedApplyRun("apply-happy", {
      worker: async (ws) => {
        await writeProjectTree(ws, [
          { path: "new.txt", kind: "file", content: "NEW" },
          { path: "gone", kind: "directory" },
        ]);
      },
      operator: async (op) => {
        await writeProjectTree(op, [{ path: "gone", kind: "directory" }, { path: "gone/a.txt", kind: "file", content: "A" }]);
      },
    });
    const fingerprint = await fingerprintTree(run.operatorProject);
    const prepared = await prepare(run);
    expect(Object.isFrozen(prepared)).toBe(true);
    expect(prepared.apply_id).toBe("applyone");
    expect(prepared.run_id).toBe(run.runId);
    expect(prepared.project_id).toBe("proj");
    expect(prepared.observation).toBe(run.observation);
    expect(prepared.terminal_revision).toBe(run.state.revision);
    expect(prepared.plan.steps.length).toBeGreaterThan(0);
    expect(prepared.payload_count).toBe(1);
    expect(prepared.plan_digest).toMatch(/^[0-9a-f]{64}$/);
    // Zero writes to the registered project.
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
    // The durable journal records the prepared operation.
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("prepared");
    expect(projection.plan_digest).toBe(prepared.plan_digest);
    expect(projection.sealed_payload_count).toBe(prepared.payload_count);
  });

  test("an exact retry after prepared returns the byte-identical package without growing the journal", async () => {
    const run = await buildFinalizedApplyRun("apply-retry", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    const prepared = await prepare(run);
    const stateBefore = await readFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "utf8");
    const retry = await prepare(run);
    expect(retry).toEqual(prepared);
    expect(retry.plan_digest).toBe(prepared.plan_digest);
    const stateAfter = await readFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "utf8");
    expect(stateAfter).toBe(stateBefore);
  });

  test("a conflicting identity for the same apply id is refused without touching the package", async () => {
    const run = await buildFinalizedApplyRun("apply-conflict", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    await prepare(run);
    const stateBefore = await readFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "utf8");
    const conflict = await failureOf(prepare(run, { observation: sha("different") }));
    expect(conflict.reason).toBe("identity_conflict");
    const differentRun = await failureOf(prepare(run, { runId: "otherrun" }));
    expect(differentRun.reason).toBe("identity_conflict");
    const differentProject = await failureOf(prepare(run, { projectId: "otherproj" }));
    expect(differentProject.reason).toBe("identity_conflict");
    const stateAfter = await readFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "utf8");
    expect(stateAfter).toBe(stateBefore);
  });

  test("different apply ids represent independent operations", async () => {
    const run = await buildFinalizedApplyRun("apply-independent", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    const first = await prepare(run, { applyId: "applya" });
    const second = await prepare(run, { applyId: "applyb" });
    expect(first.apply_id).toBe("applya");
    expect(second.apply_id).toBe("applyb");
    expect(await readdir(join(run.applyStoreRoot, "operations")).then((names) => names.sort())).toEqual(["applya", "applyb"]);
  });
});

describe("apply preparation facade: preflight refusals", () => {
  test("an unknown run is run_not_found and creates nothing", async () => {
    const run = await buildFinalizedApplyRun("apply-notfound", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    const failure = await failureOf(prepare(run, { runId: "missingrun" }));
    expect(failure.reason).toBe("run_not_found");
    expect(await readdir(run.applyStoreRoot).catch(() => [])).toEqual([]);
  });

  test("a nonterminal run is refused as run_not_terminal", async () => {
    const run = await buildFinalizedApplyRun("apply-nonterminal", { stopAfterCreate: true });
    const failure = await failureOf(prepare(run, { observation: sha("unused-request") }));
    expect(failure.reason).toBe("run_not_terminal");
    expect(await readdir(run.applyStoreRoot).catch(() => [])).toEqual([]);
  });

  test("a stale observation is refused before any artifact exists", async () => {
    const run = await buildFinalizedApplyRun("apply-stale", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    const requested = run.observation;
    await writeFile(join(run.operatorProject, "late-change.txt"), "OPERATOR-ACTIVITY");
    const failure = await failureOf(prepare(run, { observation: requested }));
    expect(failure.reason).toBe("stale_observation");
    expect(await readdir(run.applyStoreRoot).catch(() => [])).toEqual([]);
  });

  test("a diverged affected path is refused as diverged_project and records the failure", async () => {
    const run = await buildFinalizedApplyRun("apply-diverged", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "a.txt"), "FOREIGN-EDIT");
      },
    });
    const failure = await failureOf(prepare(run));
    expect(failure.reason).toBe("diverged_project");
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("failed");
    expect(projection.failure_reason).toBe("diverged_project");
  });

  test("a user extended attribute on a touched operator path refuses as unsupported_metadata", async () => {
    const run = await buildFinalizedApplyRun("apply-xattr", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "a.txt"), "OLD");
      },
    });
    const libc = dlopen("libc.so.6", {
      setxattr: { args: ["cstring", "cstring", "ptr", FFIType.u64, FFIType.u32], returns: FFIType.i64 },
    });
    const rc = Number(libc.symbols.setxattr(
      Buffer.from(`${join(run.operatorProject, "a.txt")}\0`),
      Buffer.from("user.flag\0"),
      Buffer.from("v"),
      1,
      0,
    ));
    if (rc !== 0) {
      return;
    }
    const failure = await failureOf(prepare(run));
    expect(failure.reason).toBe("unsupported_metadata");
  });

  test("a FIFO touched by the plan refuses as unsupported_project_entry", async () => {
    const run = await buildFinalizedApplyRun("apply-fifo", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "a.txt"), "OLD");
      },
    });
    // The FIFO appears after the fixture's own observation; the
    // preparation's read-only re-observation is the first owner to see it.
    await makeFifo(join(run.operatorProject, "pipe"));
    const failure = await failureOf(prepare(run));
    expect(failure.reason).toBe("unsupported_project_entry");
  });

  test("invalid options are refused before any filesystem effect", async () => {
    const run = await buildFinalizedApplyRun("apply-options", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    expect((await failureOf(prepare(run, { applyId: "../bad" }))).reason).toBe("invalid_options");
    expect((await failureOf(prepare(run, { observation: "not-a-digest" }))).reason).toBe("invalid_options");
    expect((await failureOf(prepare(run, { applyStoreRoot: join(tmpdir(), "apply-missing-store") }))).reason).toBe("invalid_options");
    const unsafeRoot = join(tmpdir(), "apply-unsafe");
    await mkdir(unsafeRoot, { mode: 0o755 });
    ROOTS.push(unsafeRoot);
    expect((await failureOf(prepare(run, { applyStoreRoot: unsafeRoot }))).reason).toBe("invalid_options");
    expect(await readdir(run.applyStoreRoot).catch(() => [])).toEqual([]);
  });
});

describe("apply package loader", () => {
  async function preparedRun(name: string): Promise<Awaited<ReturnType<typeof buildFinalizedApplyRun>>> {
    const run = await buildFinalizedApplyRun(name, {
      worker: async (ws) => {
        await writeProjectTree(ws, [
          { path: "a.txt", kind: "file", content: "CONTENT-A" },
          { path: "d", kind: "directory" },
          { path: "d/b.txt", kind: "file", content: "CONTENT-B" },
        ]);
      },
    });
    await prepare(run);
    return run;
  }

  test("the loader restores the package without the run workspace and without repairing anything", async () => {
    const run = await preparedRun("loader-roundtrip");
    await rm(run.workspaceRoot, { recursive: true, force: true });
    const fingerprint = await fingerprintTree(run.applyStoreRoot);
    const loaded = await loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(loaded.plan.steps.length).toBeGreaterThan(0);
    expect(await fingerprintTree(run.applyStoreRoot)).toBe(fingerprint);
  });

  test("a missing operation, a non-prepared operation and an unknown apply id are artifact_missing", async () => {
    const run = await preparedRun("loader-missing");
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "nosuchapply"))).reason).toBe("artifact_missing");
    // A failed operation is never prepared.
    const conflictRun = await buildFinalizedApplyRun("loader-failed", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "NEW");
      },
      operator: async (op) => {
        await writeFile(join(op, "a.txt"), "FOREIGN");
      },
    });
    await failureOf(prepare(conflictRun));
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), conflictRun.applyStoreRoot, "applyone"))).reason).toBe("artifact_missing");
  });

  test("a tampered plan is artifact_modified; the loader repairs nothing", async () => {
    const run = await preparedRun("loader-plan-tamper");
    const planPath = join(run.applyStoreRoot, "operations", "applyone", "plan.json");
    const original = await readFile(planPath);
    await writeFile(planPath, Buffer.from(original.toString("utf8").replace("publish_regular_file", "remove_leafxxxxxx")));
    const fingerprint = await fingerprintTree(run.applyStoreRoot);
    const failure = await failureOf(loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone"));
    expect(failure.reason).toBe("artifact_modified");
    expect(await fingerprintTree(run.applyStoreRoot)).toBe(fingerprint);
  });

  test("a tampered, missing, extra or symlinked payload is refused fail-closed", async () => {
    const tampered = await preparedRun("loader-payload-tamper");
    const payloadPath = join(tampered.applyStoreRoot, "operations", "applyone", "payloads", "1.payload");
    await writeFile(payloadPath, Buffer.from("CORRUPTED-BYTES"));
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), tampered.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");

    const missing = await preparedRun("loader-payload-missing");
    await unlink(join(missing.applyStoreRoot, "operations", "applyone", "payloads", "1.payload"));
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), missing.applyStoreRoot, "applyone"))).reason).toBe("artifact_missing");

    const extra = await preparedRun("loader-payload-extra");
    await writeFile(join(extra.applyStoreRoot, "operations", "applyone", "payloads", "9.payload"), Buffer.from("EXTRA"));
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), extra.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");

    const linked = await preparedRun("loader-payload-symlink");
    const payloadsDir = join(linked.applyStoreRoot, "operations", "applyone", "payloads");
    const realBytes = await readFile(join(payloadsDir, "1.payload"));
    await unlink(join(payloadsDir, "1.payload"));
    await writeFile(join(linked.root, "outside.bin"), realBytes, { mode: 0o600 });
    await symlink(join(linked.root, "outside.bin"), join(payloadsDir, "1.payload"));
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), linked.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");
  });

  test("a wrong-mode artifact or state document is artifact_modified", async () => {
    const loosePlan = await preparedRun("loader-plan-mode");
    await chmod(join(loosePlan.applyStoreRoot, "operations", "applyone", "plan.json"), 0o644);
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), loosePlan.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");

    const loosePayload = await preparedRun("loader-payload-mode");
    await chmod(join(loosePayload.applyStoreRoot, "operations", "applyone", "payloads", "1.payload"), 0o644);
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), loosePayload.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");

    const looseState = await preparedRun("loader-state-mode");
    await chmod(join(looseState.applyStoreRoot, "operations", "applyone", "state.json"), 0o644);
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), looseState.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");
  });

  test("a corrupted state document is artifact_modified and never repaired", async () => {
    const run = await preparedRun("loader-state-corrupt");
    await writeFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "{corrupted");
    const fingerprint = await fingerprintTree(run.applyStoreRoot);
    const failure = await failureOf(loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone"));
    expect(failure.reason).toBe("artifact_modified");
    expect(await fingerprintTree(run.applyStoreRoot)).toBe(fingerprint);
  });

  test("an owned publication-temp orphan is tolerated; a foreign-named artifact is not", async () => {
    const orphan = await preparedRun("loader-orphan-temp");
    await writeFile(join(orphan.applyStoreRoot, "operations", "applyone", "payloads", ".tmp-payload-1-0123456789abcdef"), Buffer.from("ORPHAN"), { mode: 0o600 });
    const loaded = await loadPreparedPackageCore(productionComponents(), orphan.applyStoreRoot, "applyone");
    expect(loaded.payload_count).toBeGreaterThan(0);
    const foreign = await preparedRun("loader-foreign-temp");
    await writeFile(join(foreign.applyStoreRoot, "operations", "applyone", "payloads", ".tmp-other-1"), Buffer.from("FOREIGN"), { mode: 0o600 });
    expect((await failureOf(loadPreparedPackageCore(productionComponents(), foreign.applyStoreRoot, "applyone"))).reason).toBe("artifact_modified");
  });

  test("a fresh process (new components) loads the identical package", async () => {
    const run = await preparedRun("loader-restart");
    const first = await loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone");
    const freshComponents: PipelineV2ProjectApplyComponents = {
      docIo: realImmutableDocumentIo,
      stateIo: defaultPipelineStateIo,
      verifyIo: realProjectWorkspaceVerifyIo,
      fileStreamIo: realProjectFileStreamIo,
      probeIo: realMetadataProbeIo,
    };
    const second = await loadPreparedPackageCore(freshComponents, run.applyStoreRoot, "applyone");
    expect(second).toEqual(first);
  });
});

describe("apply concurrency", () => {
  test("two identical concurrent preparations converge to one durable package", async () => {
    const run = await buildFinalizedApplyRun("apply-concurrent", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
        await writeFile(join(ws, "b.txt"), "CONTENT-B");
      },
    });
    const [first, second] = await Promise.all([
      prepare(run),
      prepare(run),
    ]);
    expect(first.plan_digest).toBe(second.plan_digest);
    expect(first.plan).toEqual(second.plan);
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("prepared");
    expect(projection.sealed_payload_count).toBe(first.payload_count);
    // The payloads directory carries exactly the bound artifacts.
    const names = (await readdir(join(run.applyStoreRoot, "operations", "applyone", "payloads"))).sort();
    expect(names).toEqual(["1.payload", "2.payload"]);
  });
});

describe("apply authority and leakage", () => {
  test("the canonical plan and journal carry no absolute path, run root or apply-store path", async () => {
    const run = await buildFinalizedApplyRun("apply-leak", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    await prepare(run);
    const operationDir = join(run.applyStoreRoot, "operations", "applyone");
    const plan = await readFile(join(operationDir, "plan.json"), "utf8");
    const state = await readFile(join(operationDir, "state.json"), "utf8");
    for (const document of [plan, state]) {
      expect(document).not.toContain(run.root);
      expect(document).not.toContain(run.operatorProject);
      expect(document).not.toContain(run.workspaceRoot);
      expect(document).not.toContain(run.applyStoreRoot);
      expect(document).not.toContain(tmpdir());
      expect(document).not.toContain("sess-");
      expect(document).not.toContain("tool-");
    }
    expect(plan).not.toContain("/");
    // The stored root binding is an opaque digest, never a path.
    const stateDocument = JSON.parse(state) as { project_root_binding: string };
    expect(stateDocument.project_root_binding).toMatch(/^[0-9a-f]{64}$/);
  });

  test("payload bytes never appear in the plan, the journal or any failure message", async () => {
    const secret = "PAYLOAD-SECRET-CONTENT-\0\x01";
    const run = await buildFinalizedApplyRun("apply-secret", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), secret);
      },
    });
    await prepare(run);
    const operationDir = join(run.applyStoreRoot, "operations", "applyone");
    const plan = await readFile(join(operationDir, "plan.json"), "utf8");
    const state = await readFile(join(operationDir, "state.json"), "utf8");
    expect(plan).not.toContain("PAYLOAD-SECRET");
    expect(state).not.toContain("PAYLOAD-SECRET");
    let refusal: unknown = undefined;
    try {
      await prepare(run, { applyId: "applytwo", observation: sha("other") });
    } catch (cause) {
      refusal = cause;
    }
    expect(String(refusal)).not.toContain("PAYLOAD-SECRET");
  });

  test("a stored root binding cannot be used as a filesystem path", async () => {
    const run = await buildFinalizedApplyRun("apply-binding", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    await prepare(run);
    const state = JSON.parse(await readFile(join(run.applyStoreRoot, "operations", "applyone", "state.json"), "utf8")) as {
      project_root_binding: string;
    };
    // The binding is a digest: it names no path and opens nothing.
    expect(state.project_root_binding.startsWith("/")).toBe(false);
    const statFailure = await stat(state.project_root_binding).then(
      () => "opened",
      () => "refused",
    );
    expect(statFailure).toBe("refused");
  });
});

describe("apply zero-write and ownership", () => {
  test("the registered project stays byte-identical across preparation, failures, retries and loads", async () => {
    const run = await buildFinalizedApplyRun("apply-zerowrite", {
      source: [{ path: "base.txt", kind: "file", content: "BASE" }],
      worker: async (ws) => {
        await writeFile(join(ws, "base.txt"), "NEW");
      },
    });
    const fingerprint = await fingerprintTree(run.operatorProject);
    await prepare(run);
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
    await loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
    await failureOf(prepare(run, { applyId: "applytwo", observation: sha("conflict") }));
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
    await failureOf(prepare(run, { runId: "missingrun" }));
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
    await failureOf(loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "nosuchapply"));
    expect(await fingerprintTree(run.operatorProject)).toBe(fingerprint);
  });

  test("the operation store layout is private: 0700 directories and 0600 documents", async () => {
    const run = await buildFinalizedApplyRun("apply-modes", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    await prepare(run);
    const storeInfo = await stat(run.applyStoreRoot);
    expect((storeInfo.mode & 0o7777)).toBe(0o700);
    const operationsInfo = await stat(join(run.applyStoreRoot, "operations"));
    expect((operationsInfo.mode & 0o7777)).toBe(0o700);
    const operationInfo = await stat(join(run.applyStoreRoot, "operations", "applyone"));
    expect((operationInfo.mode & 0o7777)).toBe(0o700);
    const payloadsInfo = await stat(join(run.applyStoreRoot, "operations", "applyone", "payloads"));
    expect((payloadsInfo.mode & 0o7777)).toBe(0o700);
    const planInfo = await stat(join(run.applyStoreRoot, "operations", "applyone", "plan.json"));
    expect((planInfo.mode & 0o7777)).toBe(0o600);
    const stateInfo = await stat(join(run.applyStoreRoot, "operations", "applyone", "state.json"));
    expect((stateInfo.mode & 0o7777)).toBe(0o600);
    const payloadInfo = await stat(join(run.applyStoreRoot, "operations", "applyone", "payloads", "1.payload"));
    expect((payloadInfo.mode & 0o7777)).toBe(0o600);
  });

  test("the failure reason vocabulary is closed and content-free", () => {
    expect([...PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS]).toEqual([
      "invalid_options",
      "run_not_found",
      "run_not_terminal",
      "run_state_invalid",
      "pipeline_mismatch",
      "artifact_missing",
      "artifact_modified",
      "stale_observation",
      "diverged_project",
      "unsupported_metadata",
      "unsupported_project_entry",
      "identity_conflict",
      "preparation_busy",
      "apply_not_prepared",
      "project_unstable",
      "workspace_unstable",
      "state_persist_failed",
      "internal_error",
    ]);
  });
});

describe("apply ownership source scans", () => {
  const readSource = async (path: string): Promise<string> => await readFile(join(import.meta.dir, "..", "src", path), "utf8");

  test("the preparation modules import no worker, helper, session, runner or control-plane capability", async () => {
    for (const module of [
      "pipeline_v2_project_apply.ts",
      "pipeline_v2_project_apply_internal.ts",
      "pipeline_v2_apply_plan_internal.ts",
      "pipeline_v2_apply_planner_internal.ts",
      "pipeline_v2_apply_state_internal.ts",
      "pipeline_v2_apply_metadata_probe_internal.ts",
      "pipeline_v2_immutable_blob_internal.ts",
    ]) {
      const source = await readSource(module);
      const importLines = source.split("\n").filter((line) => line.startsWith("import "));
      expect(importLines.length).toBeGreaterThan(0);
      for (const line of importLines) {
        expect(line).not.toMatch(/docker|launcher|helper|session|control_plane|runner|coordinator/i);
      }
    }
  });

  test("the single owners stay single: plan schema, plan digest, blob publication, loader, probe", async () => {
    const sources = await Promise.all([
      readSource("pipeline_v2_apply_plan_internal.ts"),
      readSource("pipeline_v2_apply_planner_internal.ts"),
      readSource("pipeline_v2_project_apply_internal.ts"),
      readSource("pipeline_v2_immutable_blob_internal.ts"),
      readSource("pipeline_v2_apply_metadata_probe_internal.ts"),
    ]);
    const [plan, planner, core, blob, probe] = sources;
    // The plan digest domain is defined exactly once and never re-hashed.
    const digestDomain = "pipeline-v2-project-apply-plan\\0";
    expect(plan!.split(digestDomain).length).toBe(2);
    expect(planner! + core! + blob! + probe!).not.toContain(digestDomain);
    // The payload publication protocol (O_EXCL temp + link) lives in one
    // module; the core only calls it.
    expect((blob!.match(/openImmutableBlobTemp|publishImmutableBlob/g) ?? []).length).toBeGreaterThan(0);
    expect((core!.match(/O_EXCL/g) ?? []).length).toBe(0);
    // The metadata FFI bindings live in one module (one dlopen call site).
    expect((probe!.match(/dlopen\(/g) ?? []).length).toBe(1);
    expect((core! + planner! + plan! + blob!).match(/dlopen\(/g)).toBeNull();
  });
});
