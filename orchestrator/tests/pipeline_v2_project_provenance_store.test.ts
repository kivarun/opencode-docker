import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile, lstat, chmod, stat } from "node:fs/promises";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { createHash } from "node:crypto";
import {
  preparePipelineV2ProjectProvenance,
  pipelineV2ProjectProvenanceBinding,
  PipelineV2ProjectProvenanceError,
} from "../src/pipeline_v2_project_provenance.ts";
import {
  loadPipelineV2ProjectProvenance,
  loadVerifiedPipelineV2ProjectProvenance,
  PROJECT_PROVENANCE_FILE_NAME,
  publishPipelineV2ProjectProvenance,
  verifyPipelineV2InitialProjectWorkspace,
} from "../src/pipeline_v2_project_provenance_store.ts";

const hex = (char: string): string => char.repeat(64);

interface Fixture {
  root: string;
  runRoot: string;
  projectRoot: string;
}

async function setup(): Promise<Fixture> {
  const root = await mkdtemp(join("/tmp", "provenance-store-"));
  const runRoot = join(root, "runs", "run-1");
  await mkdir(runRoot, { mode: 0o700, recursive: true });
  const projectRoot = join(runRoot, "project");
  await mkdir(projectRoot, { mode: 0o700 });
  return { root, runRoot, projectRoot };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

/** Builds a real workspace tree and the manifest that describes it. */
async function materialize(fixture: Fixture): Promise<ReturnType<typeof preparePipelineV2ProjectProvenance>> {
  const { projectRoot } = fixture;
  await mkdir(join(projectRoot, "docs"), { mode: 0o700 });
  await writeFile(join(projectRoot, "docs", "a.md"), "alpha\n", { mode: 0o600 });
  await writeFile(join(projectRoot, "run.sh"), "#!/bin/sh\n", { mode: 0o700 });
  await symlink("a.md", join(projectRoot, "docs", "link"));
  const digestOf = (text: string): string => createHash("sha256").update(text, "utf8").digest("hex");
  const prepared = preparePipelineV2ProjectProvenance([
    { path: "docs", kind: "directory", source_mode: 0o755, workspace_mode: 0o700 },
    { path: "docs/a.md", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 6, sha256: digestOf("alpha\n"), multiple_links: false },
    { path: "docs/link", kind: "symlink", target: "a.md", targets_identical: true },
    { path: "run.sh", kind: "regular_file", source_mode: 0o755, workspace_mode: 0o700, byte_count: 10, sha256: digestOf("#!/bin/sh\n"), multiple_links: false },
  ]);
  return prepared;
}

describe("pipeline v2 project provenance store", () => {
  test("publication, fixed layout, canonical bytes and modes", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      const finalPath = await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      expect(finalPath).toBe(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME));
      const info = await lstat(finalPath);
      expect(info.isFile()).toBe(true);
      expect(info.isSymbolicLink()).toBe(false);
      expect(info.mode & 0o7777).toBe(0o600);
      const stored = await readFile(finalPath, "utf8");
      expect(stored).toBe(prepared.canonical_json);
      expect(stored.endsWith("\n")).toBe(false);
      const names = (await readdir(fixture.runRoot)).sort();
      expect(names).toContain("project");
      expect(names).toContain("project-provenance.json");
    } finally {
      await dispose(fixture);
    }
  });

  test("an exact retry adopts the published artifact without touching it", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      const before = await lstat(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME));
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      const after = await lstat(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME));
      expect(after.ino).toBe(before.ino);
      expect(after.mtimeMs).toBe(before.mtimeMs);
      expect(await readFile(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME), "utf8")).toBe(prepared.canonical_json);
    } finally {
      await dispose(fixture);
    }
  });

  test("a different manifest on the busy path is a typed conflict and nothing is overwritten", async () => {
    const fixture = await setup();
    try {
      const first = preparePipelineV2ProjectProvenance([]);
      const second = await materialize(fixture);
      await publishPipelineV2ProjectProvenance(fixture.runRoot, first);
      let failure: unknown;
      try {
        await publishPipelineV2ProjectProvenance(fixture.runRoot, second);
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(Object);
      expect((failure as Error).name).toBe("PipelineV2ProjectProvenanceStoreError");
      expect((failure as { outcome?: string }).outcome).toBe("not_published");
      expect((failure as { reason?: string }).reason).toBe("conflict");
      expect(await readFile(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME), "utf8")).toBe(first.canonical_json);
    } finally {
      await dispose(fixture);
    }
  });

  test("read-only load: null for absence, round-trip for presence", async () => {
    const fixture = await setup();
    try {
      expect(await loadPipelineV2ProjectProvenance(fixture.runRoot)).toBeNull();
      const prepared = await materialize(fixture);
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      const loaded = await loadPipelineV2ProjectProvenance(fixture.runRoot);
      expect(loaded?.sha256).toBe(prepared.sha256);
      expect(loaded?.canonical_json).toBe(prepared.canonical_json);
    } finally {
      await dispose(fixture);
    }
  });

  test("binding-verified load refuses missing and mismatching artifacts", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      const binding = pipelineV2ProjectProvenanceBinding(prepared);
      let failure: unknown;
      try {
        await loadVerifiedPipelineV2ProjectProvenance(fixture.runRoot, binding);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as Error).name).toBe("PipelineV2ProjectProvenanceStoreError");
      expect((failure as { reason?: string }).reason).toBe("invalid_layout");
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      expect((await loadVerifiedPipelineV2ProjectProvenance(fixture.runRoot, binding)).sha256).toBe(prepared.sha256);
      const foreign = pipelineV2ProjectProvenanceBinding(preparePipelineV2ProjectProvenance([]));
      failure = undefined;
      try {
        await loadVerifiedPipelineV2ProjectProvenance(fixture.runRoot, foreign);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as { reason?: string }).reason).toBe("conflict");
    } finally {
      await dispose(fixture);
    }
  });

  test("a tampered stored document fails the canonical-byte verification", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      const path = join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME);
      const original = await readFile(path);
      await rm(path);
      await writeFile(path, original.toString("utf8").replace("regular_file", "regular_filf"), { mode: 0o600 });
      let failure: unknown;
      try {
        await loadPipelineV2ProjectProvenance(fixture.runRoot);
      } catch (cause) {
        failure = cause;
      }
      expect(["PipelineV2ProjectProvenanceStoreError", "PipelineV2ProjectProvenanceError"]).toContain((failure as Error).name);
    } finally {
      await dispose(fixture);
    }
  });

  test("the run root is never created, chmodded or removed", async () => {
    const fixture = await setup();
    try {
      const before = await lstat(fixture.runRoot);
      await publishPipelineV2ProjectProvenance(fixture.runRoot, preparePipelineV2ProjectProvenance([]));
      const after = await lstat(fixture.runRoot);
      expect(after.dev).toBe(before.dev);
      expect(after.ino).toBe(before.ino);
      expect(after.mode & 0o7777).toBe(0o700);
    } finally {
      await dispose(fixture);
    }
  });

  test("a symlinked, missing or non-directory run root fails closed", async () => {
    const root = await mkdtemp(join("/tmp", "provenance-root-"));
    try {
      const real = join(root, "real");
      await mkdir(real, { mode: 0o700 });
      const link = join(root, "link");
      await symlink(real, link);
      for (const bad of [link, join(root, "missing"), join(root, "file")]) {
        if (bad.includes("file")) {
          await writeFile(bad, "x");
        }
        let failure: unknown;
        try {
          await publishPipelineV2ProjectProvenance(bad, preparePipelineV2ProjectProvenance([]));
        } catch (cause) {
          failure = cause;
        }
        expect((failure as Error).name).toBe("PipelineV2ProjectProvenanceStoreError");
        expect((failure as { reason?: string }).reason).toBe("invalid_layout");
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("pipeline v2 initial workspace baseline verifier", () => {
  test("a matching pristine tree verifies", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
    } finally {
      await dispose(fixture);
    }
  });

  test("an empty baseline verifies against an empty tree and rejects extra entries", async () => {
    const fixture = await setup();
    try {
      const prepared = preparePipelineV2ProjectProvenance([]);
      await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      await writeFile(join(fixture.projectRoot, "stray.txt"), "x", { mode: 0o600 });
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(PipelineV2ProjectProvenanceError);
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      expect((failure as Error).message).toContain("undeclared");
    } finally {
      await dispose(fixture);
    }
  });

  test("a modified file content, byte count or mode fails closed", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await writeFile(join(fixture.projectRoot, "docs", "a.md"), "Xlpha\n", { mode: 0o600 });
      await expectReject("content digest");
      await writeFile(join(fixture.projectRoot, "docs", "a.md"), "alpha\nextra", { mode: 0o600 });
      await expectReject("byte count");
      await chmod(join(fixture.projectRoot, "docs", "a.md"), 0o700);
      await writeFile(join(fixture.projectRoot, "docs", "a.md"), "alpha\n", { mode: 0o700 });
      await expectReject("workspace mode");
      async function expectReject(fragment: string): Promise<void> {
        let failure: unknown;
        try {
          await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
        } catch (cause) {
          failure = cause;
        }
        expect(failure).toBeInstanceOf(PipelineV2ProjectProvenanceError);
        expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
        expect((failure as Error).message).toContain(fragment);
      }
    } finally {
      await dispose(fixture);
    }
  });

  test("type substitutions fail closed: symlink to file, file to directory, directory to FIFO", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await rm(join(fixture.projectRoot, "docs", "link"));
      await writeFile(join(fixture.projectRoot, "docs", "link"), "not a link\n", { mode: 0o600 });
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      // file -> directory
      await rm(join(fixture.projectRoot, "docs", "link"));
      await mkdir(join(fixture.projectRoot, "docs", "link"));
      failure = undefined;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      // FIFO inside a directory entry
      await rm(join(fixture.projectRoot, "docs", "link"), { recursive: true });
      execSync(`mkfifo ${JSON.stringify(join(fixture.projectRoot, "docs", "link"))}`);
      failure = undefined;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
    } finally {
      await dispose(fixture);
    }
  });

  test("a changed symlink target text fails closed", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await rm(join(fixture.projectRoot, "docs", "link"));
      await symlink("b.md", join(fixture.projectRoot, "docs", "link"));
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      expect((failure as Error).message).toContain("baseline target text");
    } finally {
      await dispose(fixture);
    }
  });

  test("a missing baseline entry fails closed", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      await rm(join(fixture.projectRoot, "run.sh"));
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      expect((failure as Error).message).toContain("missing the baseline entry");
    } finally {
      await dispose(fixture);
    }
  });

  test("a symlinked workspace root fails closed and nothing is ever written", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      const fingerprint = (await readdir(fixture.projectRoot)).sort();
      await rm(fixture.projectRoot, { recursive: true });
      await symlink(fixture.root, fixture.projectRoot);
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      } catch (cause) {
        failure = cause;
      }
      expect((failure as PipelineV2ProjectProvenanceError).reason).toBe("workspace_mismatch");
      expect(fingerprint.length).toBeGreaterThan(0);
    } finally {
      await dispose(fixture);
    }
  });

  test("verification is deterministic and read-only across repeats", async () => {
    const fixture = await setup();
    try {
      const prepared = await materialize(fixture);
      const before = await stat(fixture.projectRoot);
      await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      await verifyPipelineV2InitialProjectWorkspace(fixture.projectRoot, prepared);
      const after = await stat(fixture.projectRoot);
      expect(after.ino).toBe(before.ino);
      expect(after.mtimeMs).toBe(before.mtimeMs);
    } finally {
      await dispose(fixture);
    }
  });
});

// --- C4: the durability-unknown publication window (internal store core) ----

import {
  publishProjectProvenanceWithIo,
  realProjectWorkspaceVerifyIo as _realVerifyIo,
  type ProjectWorkspaceVerifyIo,
} from "../src/pipeline_v2_project_provenance_store_internal.ts";
import {
  realImmutableDocumentIo,
  type ImmutableDocumentIo,
} from "../src/pipeline_v2_immutable_document_store_internal.ts";

function ioFaultingDirFsync(failure: unknown): ImmutableDocumentIo {
  // The substrate syncs a directory through `openDir` + `DirHandle.sync()`;
  // fault exactly that post-link durability step.
  return {
    ...realImmutableDocumentIo,
    openDir: async (path: string) => {
      const real = await realImmutableDocumentIo.openDir(path);
      return Object.freeze({
        sync: async () => {
          throw failure;
        },
        close: real.close.bind(real),
      });
    },
  } as ImmutableDocumentIo;
}

test("C4. a durability-unknown manifest publication leaves the visible candidate and fails closed", async () => {
  const fixture = await setup();
  try {
    const prepared = await materialize(fixture);
    const cause = await publishProjectProvenanceWithIo(
      ioFaultingDirFsync(Object.assign(new Error("dir fsync failed"), { code: "EIO" })),
      fixture.runRoot,
      prepared,
    ).then(
      () => null,
      (error: unknown) => error,
    );
    expect((cause as Error).name).toBe("PipelineV2ProjectProvenanceStoreError");
    expect((cause as { outcome?: string }).outcome).toBe("durability_unknown");
    expect((cause as { reason?: string }).reason).toBe("io_failure");
    expect((cause as { candidate?: { digest: string } }).candidate?.digest).toBe(prepared.sha256);
    // The candidate is visible with exactly the canonical bytes; a fresh
    // reopen adopts it (the substrate's byte-exact adoption) instead of
    // publishing a second artifact.
    expect(await readFile(join(fixture.runRoot, PROJECT_PROVENANCE_FILE_NAME), "utf8")).toBe(prepared.canonical_json);
    const adopted = await loadVerifiedPipelineV2ProjectProvenance(
      fixture.runRoot,
      pipelineV2ProjectProvenanceBinding(prepared),
    );
    expect(adopted.sha256).toBe(prepared.sha256);
  } finally {
    await dispose(fixture);
  }
});

// --- source-structure pins ---------------------------------------------------

test("source scans: one serializer, one digest domain, one publication substrate, no second source scan", async () => {
  const provenance = await import("node:fs/promises").then(() =>
    readFile(new URL("../src/pipeline_v2_project_provenance.ts", import.meta.url).pathname, "utf8"),
  );
  const storeInternal = await import("node:fs/promises").then(() =>
    readFile(new URL("../src/pipeline_v2_project_provenance_store_internal.ts", import.meta.url).pathname, "utf8"),
  );
  const copyCore = await import("node:fs/promises").then(() =>
    readFile(new URL("../src/pipeline_v2_project_copy_internal.ts", import.meta.url).pathname, "utf8"),
  );
  // exactly one digest domain owner (the quoted literal form; the module
  // documentation may name the domain in prose)
  expect((provenance.match(/"pipeline-v2-project-provenance\\0"/g) ?? []).length).toBe(1);
  expect(storeInternal).not.toContain("pipeline-v2-project-provenance\\0");
  // the store adapter has no own fsync/link implementation (comments that
  // document the shared substrate's protocol are stripped first)
  const storeCode = storeInternal.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
  expect(storeCode).not.toContain("O_EXCL");
  expect(storeCode.match(/(?<!read)link\(/g) ?? []).toHaveLength(0);
  expect(storeInternal).toContain("publishImmutableDocumentFile");
  // the copy traversal is the single capture owner; no second scan entry point
  expect(copyCore).toContain("copyProjectTree");
  expect((copyCore.match(/readdir\(/g) ?? []).length).toBe(1);
  // the manifest module is pure: no filesystem imports
  expect(provenance).not.toContain("node:fs");
  expect(provenance).not.toContain("node:fs/promises");
  // no migration, no Apply, no public event kind
  expect(provenance.toLowerCase()).not.toContain("migrat");
  expect(provenance).not.toContain("apply");
  expect(storeInternal).not.toContain("run_failed");
  // the publication order is manifest before workspace rename (C2 is
  // unreachable by construction; only the mirrored window exists)
  const runtime = await import("node:fs/promises").then(() =>
    readFile(new URL("../src/pipeline_v2_runtime.ts", import.meta.url).pathname, "utf8"),
  );
  expect(runtime.indexOf("publishPipelineV2ProjectProvenance(")).toBeGreaterThan(0);
  expect(runtime.indexOf("publishStagedProjectCopy(")).toBeGreaterThan(
    runtime.indexOf("publishPipelineV2ProjectProvenance("),
  );
  // the manifest payload is never projected publicly: no inspect, events,
  // HTTP, UI or diagnostics projection reads the artifact or its fields
  for (const name of [
    "pipeline_v2_inspect.ts",
    "pipeline_v2_events.ts",
    "pipeline_v2_public_reason.ts",
    "control_plane/http_server.ts",
    "control_plane/routing.ts",
    "control_plane/subprocess_client.ts",
  ]) {
    const source = await import("node:fs/promises").then(() =>
      readFile(new URL(`../src/${name}`, import.meta.url).pathname, "utf8"),
    );
    expect(source).not.toContain("project_provenance");
    expect(source).not.toContain("project-provenance");
    expect(source).not.toContain("PipelineV2ProjectProvenance");
  }
  const ui = await import("node:fs/promises").then(() =>
    readFile(new URL("../ui/app.js", import.meta.url).pathname, "utf8"),
  );
  // the legacy "commit provenance" vocabulary is unrelated; the project
  // provenance manifest is never projected
  for (const banned of ["project_provenance", "project-provenance", "copy_policy_version"]) {
    expect(ui).not.toContain(banned);
  }
});
