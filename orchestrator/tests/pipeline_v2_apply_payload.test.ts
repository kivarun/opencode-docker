/**
 * The sealed payload preparation (`pipeline_v2_project_apply_internal.ts`
 * payload sealing over the single streaming observation owner): every
 * future regular-file publication's bytes are sealed streaming from the
 * run workspace into one immutable blob — never buffered whole, never
 * re-hashed after the fact — and stay recoverable from the package alone
 * after the run workspace changes or disappears.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  buildFinalizedApplyRun,
  disposeApplyRoots,
  realProjectFileStreamIo,
} from "./apply_fixtures.ts";
import type { ProjectFileStreamIo } from "../src/pipeline_v2_project_stream_internal.ts";
import {
  inspectOperationCore,
  preparePipelineV2ProjectApplyCore,
  loadPreparedPackageCore,
  PipelineV2ProjectApplyError,
  type PipelineV2ProjectApplyComponents,
} from "../src/pipeline_v2_project_apply_internal.ts";
import { realImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo } from "../src/pipeline_state_store.ts";
import { realProjectWorkspaceVerifyIo } from "../src/pipeline_v2_project_provenance_store_internal.ts";
import { realMetadataProbeIo } from "../src/pipeline_v2_apply_metadata_probe_internal.ts";
import { PROJECT_FILE_STREAM_CHUNK_BYTES, ProjectFileStreamError } from "../src/pipeline_v2_project_stream_internal.ts";

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

function componentsWithStream(fileStreamIo: ProjectFileStreamIo): PipelineV2ProjectApplyComponents {
  return { ...productionComponents(), fileStreamIo };
}

async function prepare(run: Awaited<ReturnType<typeof buildFinalizedApplyRun>>, components = productionComponents()) {
  return await preparePipelineV2ProjectApplyCore(
    {
      applyStoreRoot: run.applyStoreRoot,
      applyId: "applyone",
      runId: run.runId,
      projectId: "proj",
      projectRoot: run.operatorProject,
      observation: run.observation,
      stateRoot: run.stateRoot,
    },
    components,
  );
}

describe("payload sealing: content classes", () => {
  test("text, binary, NUL bytes, invalid UTF-8 and missing final newlines seal exactly", async () => {
    const run = await buildFinalizedApplyRun("payload-classes", {
      worker: async (ws) => {
        await writeFile(join(ws, "text.bin"), "plain text\n");
        await writeFile(join(ws, "nul.bin"), Buffer.from([0x00, 0x01, 0x02, 0x00]));
        await writeFile(join(ws, "utf8.bin"), Buffer.from([0x48, 0xc3, 0x28, 0xe2, 0x82, 0xac]));
        await writeFile(join(ws, "noeol.bin"), "no trailing newline");
        await writeFile(join(ws, "binary.bin"), Buffer.from([0xff, 0xfe, 0x00, 0x1b, 0x5b, 0x41]));
      },
    });
    const prepared = await prepare(run);
    const byArtifact = new Map(prepared.plan.payloads.map((payload) => [payload.artifact, payload]));
    expect(byArtifact.size).toBe(5);
    for (const payload of prepared.plan.payloads) {
      const sealed = await readFile(join(prepared.payloads_root, payload.artifact));
      expect(sealed.byteLength).toBe(payload.byte_count);
      expect(sha(sealed)).toBe(payload.sha256);
    }
    const loaded = await loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(loaded.plan_digest).toBe(prepared.plan_digest);
  });

  test("a large file is sealed through bounded streaming chunks without a whole-file buffer", async () => {
    const run = await buildFinalizedApplyRun("payload-large", {
      worker: async (ws) => {
        const chunk = Buffer.alloc(64 * 1024, 0x5a);
        const handle = await import("node:fs/promises").then((fs) => fs.open(join(ws, "large.bin"), "w"));
        for (let index = 0; index < 96; index += 1) {
          await handle.write(chunk);
        }
        await handle.close();
      },
    });
    let maxReadLength = 0;
    let wholeFileRequested = false;
    const recordingStream: ProjectFileStreamIo = {
      openNoFollow: async (path) => {
        const handle = await realProjectFileStreamIo.openNoFollow(path);
        return {
          fstat: () => handle.fstat(),
          read: async (buffer: Buffer, offset: number, length: number, position: number) => {
            if (length > PROJECT_FILE_STREAM_CHUNK_BYTES) {
              wholeFileRequested = true;
            }
            maxReadLength = Math.max(maxReadLength, length);
            return await handle.read(buffer, offset, length, position);
          },
          close: () => handle.close(),
        };
      },
    };
    const prepared = await prepare(run, componentsWithStream(recordingStream));
    expect(wholeFileRequested).toBe(false);
    expect(maxReadLength).toBe(PROJECT_FILE_STREAM_CHUNK_BYTES);
    const payload = prepared.plan.payloads[0]!;
    const sealed = await readFile(join(prepared.payloads_root, payload.artifact));
    expect(sealed.byteLength).toBe(96 * 64 * 1024);
    expect(sha(sealed)).toBe(payload.sha256);
  });

  test("an empty result file seals as a zero-byte payload", async () => {
    const run = await buildFinalizedApplyRun("payload-empty", {
      worker: async (ws) => {
        await writeFile(join(ws, "empty.txt"), "");
      },
    });
    const prepared = await prepare(run);
    const payload = prepared.plan.payloads[0]!;
    expect(payload.byte_count).toBe(0);
    expect(payload.sha256).toBe(sha(Buffer.alloc(0)));
    expect(await readFile(join(prepared.payloads_root, payload.artifact))).toHaveLength(0);
  });
});

describe("payload sealing: source mutation windows", () => {
  test("a source replaced before sealing keeps the reviewed contract and refuses as unstable", async () => {
    const run = await buildFinalizedApplyRun("payload-mutated", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "REVIEWED");
      },
    });
    // The observation is captured first; then the workspace file changes.
    const observation = run.observation;
    await writeFile(join(run.workspaceRoot, "a.txt"), "CHANGED-AFTER-OBSERVATION");
    let failure: unknown = undefined;
    try {
      await preparePipelineV2ProjectApplyCore(
        {
          applyStoreRoot: run.applyStoreRoot,
          applyId: "applyone",
          runId: run.runId,
          projectId: "proj",
          projectRoot: run.operatorProject,
          observation,
          stateRoot: run.stateRoot,
        },
        productionComponents(),
      );
    } catch (cause) {
      failure = cause;
    }
    expect((failure as { reason?: string }).reason).toBe("stale_observation");
  });

  test("a source mutating between two payload sealings stops the operation and resumes exactly", async () => {
    const run = await buildFinalizedApplyRun("payload-between", {
      worker: async (ws) => {
        await writeFile(join(ws, "first.txt"), "FIRST");
        await writeFile(join(ws, "second.txt"), "SECOND");
      },
    });
    // The observation scan opens every workspace file exactly once; the
    // payload loop opens each source again. Firing on the second open of
    // the second source places the mutation exactly between the two
    // payload sealings.
    let secondOpens = 0;
    const mutatingStream: ProjectFileStreamIo = {
      openNoFollow: async (path: string) => {
        if (path.endsWith("second.txt")) {
          secondOpens += 1;
          if (secondOpens === 2) {
            await writeFile(join(run.workspaceRoot, "second.txt"), "MUTATED-MID-PREPARE");
          }
        }
        return await realProjectFileStreamIo.openNoFollow(path);
      },
    };
    let failure: unknown = undefined;
    try {
      await prepare(run, componentsWithStream(mutatingStream));
    } catch (cause) {
      failure = cause;
    }
    expect(failure instanceof PipelineV2ProjectApplyError).toBe(true);
    expect((failure as PipelineV2ProjectApplyError).reason).toBe("workspace_unstable");
    // The first payload stays sealed; the journal records the progress.
    const projection = await inspectOperationCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(projection.status).toBe("preparing");
    expect(projection.sealed_payload_count).toBe(1);
    // Restoring the workspace and retrying with fresh components converges.
    await writeFile(join(run.workspaceRoot, "second.txt"), "SECOND");
    const prepared = await prepare(run);
    expect(prepared.payload_count).toBe(2);
    const sealed = await readFile(join(prepared.payloads_root, "2.payload"));
    expect(sha(sealed)).toBe(sha("SECOND"));
  });

  test("an inode-swapped source between the scan and the sealed pass is refused as unstable", async () => {
    const run = await buildFinalizedApplyRun("payload-swap", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "REVIEWED");
      },
    });
    // The observation scan opens the file once; the sealed pass opens it
    // again. The swap fires exactly before the sealed pass and keeps the
    // byte length so only the content digest detects it.
    let opens = 0;
    const swappingStream: ProjectFileStreamIo = {
      openNoFollow: async (path: string) => {
        if (path.endsWith("a.txt")) {
          opens += 1;
          if (opens === 2) {
            await unlink(join(run.workspaceRoot, "a.txt"));
            await writeFile(join(run.workspaceRoot, "a.txt"), "REPLACED");
          }
        }
        return await realProjectFileStreamIo.openNoFollow(path);
      },
    };
    let failure: unknown = undefined;
    try {
      await prepare(run, componentsWithStream(swappingStream));
    } catch (cause) {
      failure = cause;
    }
    expect(failure instanceof PipelineV2ProjectApplyError).toBe(true);
    expect((failure as PipelineV2ProjectApplyError).reason).toBe("workspace_unstable");
  });
});

describe("payload sealing: self-containment after preparation", () => {
  test("deleting the whole run workspace keeps every payload recoverable from the package alone", async () => {
    const run = await buildFinalizedApplyRun("payload-recover", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "RECOVER-ME");
        await writeFile(join(ws, "b.bin"), Buffer.from([0x00, 0xff, 0x7f]));
      },
    });
    const prepared = await prepare(run);
    const payloads = prepared.plan.payloads.map((payload) => ({
      artifact: payload.artifact,
      byte_count: payload.byte_count,
      sha256: payload.sha256,
    }));
    await rm(run.workspaceRoot, { recursive: true, force: true });
    const loaded = await loadPreparedPackageCore(productionComponents(), run.applyStoreRoot, "applyone");
    expect(loaded.plan_digest).toBe(prepared.plan_digest);
    for (const payload of loaded.plan.payloads) {
      const sealed = await readFile(join(loaded.payloads_root, payload.artifact));
      expect(sealed.byteLength).toBe(payload.byte_count);
      expect(sha(sealed)).toBe(payload.sha256);
    }
    expect(payloads.length).toBe(2);
  });

  test("payload artifacts are regular files with mode 0600 and canonical numeric names", async () => {
    const run = await buildFinalizedApplyRun("payload-layout", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "A");
        await writeFile(join(ws, "b.txt"), "B");
      },
    });
    const prepared = await prepare(run);
    for (let index = 1; index <= prepared.payload_count; index += 1) {
      const artifact = `${index}.payload`;
      const info = await stat(join(prepared.payloads_root, artifact));
      expect(info.isFile()).toBe(true);
      expect((info.mode & 0o7777)).toBe(0o600);
      expect(info.isSymbolicLink()).toBe(false);
    }
    expect(prepared.plan.payloads.map((payload) => payload.artifact)).toEqual(["1.payload", "2.payload"]);
  });

  test("the run workspace is never read after the prepared marker by the loader", async () => {
    const run = await buildFinalizedApplyRun("payload-loader-ro", {
      worker: async (ws) => {
        await writeFile(join(ws, "a.txt"), "CONTENT");
      },
    });
    await prepare(run);
    let reads = 0;
    const counting: ProjectFileStreamIo = {
      openNoFollow: async (path: string) => {
        reads += 1;
        return await realProjectFileStreamIo.openNoFollow(path);
      },
    };
    await loadPreparedPackageCore(componentsWithStream(counting), run.applyStoreRoot, "applyone");
    expect(reads).toBe(0);
  });
});
