import { execSync } from "node:child_process";
import { createServer } from "node:net";
import { chmod, lstat, mkdir, mkdtemp, readlink, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  prepareProjectCopy,
  realProjectCopyIo,
  type ProjectCopyIo,
} from "../src/pipeline_v2_project_copy_internal.ts";
import { prepareRunProject, type PreparedRunProjectWithProvenance } from "../src/pipeline_v2_runtime.ts";
import { PipelineV2RuntimeError } from "../src/pipeline_v2_runtime_error.ts";

interface Fixture {
  root: string;
  runRoot: string;
  source: string;
}

async function setup(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-project-"));
  const runRoot = join(root, "run");
  await mkdir(runRoot, { recursive: true });
  const source = join(root, "project-source");
  await mkdir(source, { recursive: true });
  return { root, runRoot, source };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

async function entryMode(path: string): Promise<number> {
  return (await lstat(path)).mode & 0o777;
}

interface TreeEntry {
  relativePath: string;
  kind: "directory" | "file" | "symlink";
  mode?: number;
  content?: string;
  target?: string;
}

function expectFailClosed(
  cause: unknown,
  messageFragment = "",
): void {
  expect(cause).toBeInstanceOf(PipelineV2RuntimeError);
  expect((cause as PipelineV2RuntimeError).reason).toBe("run_input_invalid");
  if (messageFragment !== "") {
    expect((cause as Error).message).toContain(messageFragment);
  }
}

test("1. the full tree is copied: files, nested/empty/hidden directories, symlink, executable file", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.source, ".git"));
    await writeFile(join(fixture.source, ".git", "HEAD"), "ref: refs/heads/main\n");
    await mkdir(join(fixture.source, "docs", "nested", "deeper"), { recursive: true });
    await writeFile(join(fixture.source, "README.md"), "readme body\n");
    await writeFile(join(fixture.source, "docs", "design.md"), "design body\n");
    await writeFile(join(fixture.source, "docs", "nested", "leaf.txt"), "leaf\n");
    await writeFile(join(fixture.source, "setup.sh"), "#!/bin/sh\necho hi\n");
    await chmod(join(fixture.source, "setup.sh"), 0o755);
    await mkdir(join(fixture.source, "empty-dir"), { recursive: true });
    await symlink("../docs/design.md", join(fixture.source, "link-to-design"));

    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(prepared.run_root).toBe(fixture.runRoot);
    expect(prepared.project_root).toBe(join(fixture.runRoot, "project"));

    const project = prepared.project_root;
    const names = (await readdir(project)).sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
    expect(names).toEqual([".git", "README.md", "docs", "empty-dir", "link-to-design", "setup.sh"]);
    expect(await readFile(join(project, ".git", "HEAD"), "utf8")).toBe("ref: refs/heads/main\n");
    expect(await readFile(join(project, "docs", "nested", "deeper"), "utf8").catch(() => "empty")).toBe("empty");
    expect((await readdir(join(project, "docs", "nested", "deeper"))).length).toBe(0);
    expect(await readFile(join(project, "README.md"), "utf8")).toBe("readme body\n");
    expect(await readFile(join(project, "docs", "nested", "leaf.txt"), "utf8")).toBe("leaf\n");
    expect(await readFile(join(project, "setup.sh"), "utf8")).toBe("#!/bin/sh\necho hi\n");
    // the symlink is copied as a symlink with the verbatim target text
    const linkInfo = await lstat(join(project, "link-to-design"));
    expect(linkInfo.isSymbolicLink()).toBe(true);
    expect(await readlink(join(project, "link-to-design"))).toBe("../docs/design.md");
  } finally {
    await dispose(fixture);
  }
});

test("2. source mutation after the copy does not change the project copy", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "original\n");
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    await writeFile(join(fixture.source, "task.md"), "MUTATED\n");
    await writeFile(join(fixture.source, "added.md"), "added in source\n");
    await rm(join(fixture.source, "task.md"));
    expect(await readFile(join(prepared.project_root, "task.md"), "utf8")).toBe("original\n");
    expect((await readdir(prepared.project_root)).sort()).toEqual(["task.md"]);
  } finally {
    await dispose(fixture);
  }
});

test("3. agent changes inside <runRoot>/project do not touch the source", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "original\n");
    await writeFile(join(fixture.source, "notes.txt"), "notes\n");
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    await writeFile(join(prepared.project_root, "task.md"), "CHANGED BY AGENT\n");
    await rm(join(prepared.project_root, "notes.txt"));
    await writeFile(join(prepared.project_root, "agent-output.txt"), "agent\n");
    expect(await readFile(join(fixture.source, "task.md"), "utf8")).toBe("original\n");
    expect(await readFile(join(fixture.source, "notes.txt"), "utf8")).toBe("notes\n");
    expect((await readdir(fixture.source)).sort()).toEqual(["notes.txt", "task.md"]);
  } finally {
    await dispose(fixture);
  }
});

test("4. source/runRoot overlap fails closed in both directions", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "marker.md"), "body\n");

    // source inside the run root
    const nestedSource = join(fixture.runRoot, "src");
    await mkdir(nestedSource);
    let failure: unknown;
    try {
      await prepareRunProject(nestedSource, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "must not overlap");
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    await expectLeftoverStaging(fixture.runRoot, 0);

    // run root inside the source
    const nestedRunRoot = join(fixture.source, "nested-run");
    await mkdir(nestedRunRoot);
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, nestedRunRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "must not overlap");
    expect((await lstatOrNull(join(nestedRunRoot, "project"))) === null).toBe(true);

    // identical paths
    failure = undefined;
    try {
      await prepareRunProject(fixture.runRoot, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "must not overlap");
  } finally {
    await dispose(fixture);
  }
});

test("5. a symlink, a regular file or a missing path as the project source fails closed", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.root, "file.txt"), "x\n");
    await symlink(join(fixture.root, "file.txt"), join(fixture.root, "source-link"));

    let failure: unknown;
    try {
      await prepareRunProject(join(fixture.root, "source-link"), fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "symbolic link");

    failure = undefined;
    try {
      await prepareRunProject(join(fixture.root, "file.txt"), fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "regular file");

    failure = undefined;
    try {
      await prepareRunProject(join(fixture.root, "absent"), fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "does not exist");

    // relative paths are rejected too
    failure = undefined;
    try {
      await prepareRunProject("relative/project", fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "absolute path");
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, "relative/run-root");
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "absolute path");
  } finally {
    await dispose(fixture);
  }
});

test("6. a pre-existing project of any kind fails closed and keeps its sentinel", async () => {
  const fixture = await setup();
  try {
    // existing directory
    await mkdir(join(fixture.runRoot, "project"));
    await writeFile(join(fixture.runRoot, "project", "sentinel"), "keep me\n");
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "already exists");
    expect(await readFile(join(fixture.runRoot, "project", "sentinel"), "utf8")).toBe("keep me\n");
    await rm(join(fixture.runRoot, "project"), { recursive: true });

    // existing regular file
    await writeFile(join(fixture.runRoot, "project"), "file sentinel\n");
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "already exists");
    expect(await readFile(join(fixture.runRoot, "project"), "utf8")).toBe("file sentinel\n");
    await rm(join(fixture.runRoot, "project"));

    // existing symlink
    await symlink(fixture.source, join(fixture.runRoot, "project"));
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "already exists");
    const linkStill = await lstat(join(fixture.runRoot, "project"));
    expect(linkStill.isSymbolicLink()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("7. a FIFO or a unix socket inside the source fails closed and leaves no staging tree", async () => {
  const fixture = await setup();
  try {
    // FIFO inside a nested directory (after earlier successful copies)
    await mkdir(join(fixture.source, "docs"));
    await writeFile(join(fixture.source, "docs", "a.md"), "a\n");
    await mkdir(join(fixture.source, "deeper"));
    await writeFile(join(fixture.source, "deeper", "b.md"), "b\n");
    execSync(`mkfifo ${JSON.stringify(join(fixture.source, "deeper", "pipe"))}`);
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    await expectLeftoverStaging(fixture.runRoot, 0);
    // the source stays untouched (the FIFO is still there)
    const fifoInfo = await lstat(join(fixture.source, "deeper", "pipe"));
    expect(fifoInfo.isFIFO()).toBe(true);

    // unix socket in place of the earlier FIFO
    await rm(join(fixture.source, "deeper", "pipe"));
    const server = createServer();
    await new Promise<void>((resolve) => server.listen(join(fixture.source, "deeper", "sock"), () => resolve()));
    try {
      failure = undefined;
      try {
        await prepareRunProject(fixture.source, fixture.runRoot);
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
      expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
      await expectLeftoverStaging(fixture.runRoot, 0);
    } finally {
      server.close();
    }
  } finally {
    await dispose(fixture);
  }
});

test("8. a symlink is copied verbatim and its external target is never read or changed", async () => {
  const fixture = await setup();
  try {
    const externalTarget = join(fixture.root, "outside", "target.txt");
    await mkdir(join(fixture.root, "outside"));
    await writeFile(externalTarget, "external sentinel body\n");
    await symlink(externalTarget, join(fixture.source, "out-link"));
    await writeFile(join(fixture.source, "local.md"), "local body\n");

    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(await readlink(join(prepared.project_root, "out-link"))).toBe(externalTarget);
    // the target was never read (its content is unchanged) and never followed
    expect(await readFile(externalTarget, "utf8")).toBe("external sentinel body\n");
    // resolving the copied symlink still reaches the external target, but
    // the copy itself stayed a symlink
    expect((await lstat(join(prepared.project_root, "out-link"))).isSymbolicLink()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("9. a mid-copy failure removes the whole staging tree and creates no project", async () => {
  const fixture = await setup();
  try {
    // entries sorted before the failing one are already copied when the
    // FIFO inside "z-dir" is hit
    await writeFile(join(fixture.source, "a.md"), "a\n");
    await mkdir(join(fixture.source, "m-dir"));
    await writeFile(join(fixture.source, "m-dir", "b.md"), "b\n");
    await mkdir(join(fixture.source, "z-dir"));
    execSync(`mkfifo ${JSON.stringify(join(fixture.source, "z-dir", "pipe"))}`);
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure);
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    await expectLeftoverStaging(fixture.runRoot, 0);
  } finally {
    await dispose(fixture);
  }
});

test("10. a repeated call fails closed and never overwrites the published project", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "body\n");
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const before = await treeDigest(prepared.project_root);
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "already carries a project provenance manifest");
    expect(await treeDigest(prepared.project_root)).toBe(before);
  } finally {
    await dispose(fixture);
  }
});

test("11. the run-owned copy uses the contract modes: 0700 directories, 0600 files, 0700 executables", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.source, "d1"));
    await mkdir(join(fixture.source, "d2", "d3"), { recursive: true });
    await writeFile(join(fixture.source, "plain.txt"), "plain\n");
    await writeFile(join(fixture.source, "run.sh"), "#!/bin/sh\n");
    await chmod(join(fixture.source, "run.sh"), 0o700);
    await writeFile(join(fixture.source, "run-other.sh"), "#!/bin/sh\n");
    await chmod(join(fixture.source, "run-other.sh"), 0o474); // owner r only + group/other execute bits

    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(await entryMode(prepared.project_root)).toBe(0o700);
    expect(await entryMode(join(prepared.project_root, "d1"))).toBe(0o700);
    expect(await entryMode(join(prepared.project_root, "d2"))).toBe(0o700);
    expect(await entryMode(join(prepared.project_root, "d2", "d3"))).toBe(0o700);
    expect(await entryMode(join(prepared.project_root, "plain.txt"))).toBe(0o600);
    expect(await entryMode(join(prepared.project_root, "run.sh"))).toBe(0o700);
    expect(await entryMode(join(prepared.project_root, "run-other.sh"))).toBe(0o700);
  } finally {
    await dispose(fixture);
  }
});

test("12. the traversal is deterministic and code-unit sorted across runs", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.source, "sub"));
    await writeFile(join(fixture.source, "Zebra.txt"), "z\n");
    await writeFile(join(fixture.source, "apple.txt"), "a\n");
    await writeFile(join(fixture.source, "Zeta.md"), "z2\n");
    await writeFile(join(fixture.source, "sub", "_under.txt"), "u\n");
    await writeFile(join(fixture.source, "sub", "Beta.md"), "b\n");

    const otherRunRoot = join(fixture.root, "run-two");
    await mkdir(otherRunRoot);
    const first = await prepareRunProject(fixture.source, fixture.runRoot);
    const second = await prepareRunProject(fixture.source, otherRunRoot);
    const firstPaths = (await walkNames(first.project_root)).slice();
    const secondNames = await walkNames(second.project_root);
    expect(secondNames).toEqual(firstPaths);
    const sorted = firstPaths.slice().sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
    expect(firstPaths).toEqual(sorted);
    // code-unit order places uppercase before lowercase
    expect(firstPaths[0]).toBe("Zebra.txt");
  } finally {
    await dispose(fixture);
  }
});

test("13. the returned object is deep-frozen and carries no source path", async () => {
  const fixture = await setup();
  try {
    const prepared: PreparedRunProjectWithProvenance = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(Object.isFrozen(prepared)).toBe(true);
    expect(Object.keys(prepared).sort()).toEqual(["binding", "manifest", "project_root", "run_root"]);
    expect(prepared.manifest.manifest.entries.length).toBe(prepared.binding.entry_count);
    expect(prepared.binding.copy_policy_version).toBe(1);
    expect(JSON.stringify(prepared)).not.toContain(fixture.source);
  } finally {
    await dispose(fixture);
  }
});

test("14. run-root validation: missing, file, symlink and non-canonical roots fail closed", async () => {
  const fixture = await setup();
  try {
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, join(fixture.root, "absent"));
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "does not exist");

    await writeFile(join(fixture.root, "root-file"), "x\n");
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, join(fixture.root, "root-file"));
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "regular file");

    await symlink(fixture.runRoot, join(fixture.root, "root-link"));
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, join(fixture.root, "root-link"));
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "symbolic link");

    // a non-canonical but real run-root path (resolving through a symlinked
    // ancestor directory elsewhere) is rejected by the canonical requirement
    await symlink(fixture.runRoot, join(fixture.root, "alias"));
    const nonCanonical = join(fixture.root, "alias", "sub");
    await mkdir(join(fixture.runRoot, "sub"));
    failure = undefined;
    try {
      await prepareRunProject(fixture.source, nonCanonical);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "not canonical");
  } finally {
    await dispose(fixture);
  }
});

test("15. diagnostics never contain the source path or file contents", async () => {
  const fixture = await setup();
  try {
    const secret = "PROJECT-SECRET-BODY";
    await writeFile(join(fixture.source, "secret.txt"), `${secret}\n`);
    // a failing entry later in the traversal
    await mkdir(join(fixture.source, "zz"));
    execSync(`mkfifo ${JSON.stringify(join(fixture.source, "zz", "pipe"))}`);
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure);
    const message = (failure as Error).message;
    expect(message).not.toContain(fixture.source);
    expect(message).not.toContain(secret);
    // the failure text names the entry relative to the project root only
    expect(message).toContain('"zz/pipe"');
  } finally {
    await dispose(fixture);
  }
});

test("16. run root ownership: the coordinator-owned roots are not created or removed", async () => {
  const fixture = await setup();
  try {
    const runRootInfoBefore = await lstat(fixture.runRoot);
    await writeFile(join(fixture.source, "x.md"), "x\n");
    await prepareRunProject(fixture.source, fixture.runRoot);
    const runRootInfoAfter = await lstat(fixture.runRoot);
    expect(runRootInfoAfter.dev).toBe(runRootInfoBefore.dev);
    expect(runRootInfoAfter.ino).toBe(runRootInfoBefore.ino);
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["project", "project-provenance.json"]);
  } finally {
    await dispose(fixture);
  }
});

test("17. one block is written through several partial writes and the copy is byte-exact", async () => {
  const fixture = await setup();
  try {
    const body = Buffer.alloc(300 * 1024);
    for (let index = 0; index < body.length; index += 1) {
      body[index] = index % 251;
    }
    await writeFile(join(fixture.source, "blob.bin"), body);
    let writeCalls = 0;
    let rmCalls = 0;
    const prepared = await prepareProjectCopy(
      {
        ...realProjectCopyIo,
        destinationWrite: async (handle, chunk, offset, length, position) => {
          writeCalls += 1;
          const take = Math.max(1, Math.floor(length / 2));
          const result = await handle.write(chunk, offset, take, position);
          return result.bytesWritten;
        },
        stagingRm: async (path, options) => {
          rmCalls += 1;
          await rm(path, options);
        },
      },
      fixture.source,
      fixture.runRoot,
    );
    const copied = await readFile(join(prepared.project_root, "blob.bin"));
    expect(copied.equals(body)).toBe(true);
    // 300 KiB is three full 128 KiB blocks; every block needed several
    // writes, so the write-all loop advanced offset and position
    const blocks = Math.ceil(300 * 1024 / (128 * 1024));
    expect(writeCalls).toBeGreaterThan(blocks);
    expect(writeCalls).toBeGreaterThanOrEqual(6);
    // a successful copy never runs the staging cleanup
    expect(rmCalls).toBe(0);
    expect((await lstat(prepared.project_root)).isDirectory()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("18. a zero-progress or impossible write fails once, without looping", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "blob.bin"), Buffer.alloc(4096, 7));
    for (const bogus of [0, Number.NaN, Number.MAX_SAFE_INTEGER]) {
      let writeCalls = 0;
      let failure: unknown;
      try {
        await prepareProjectCopy(
          {
            ...realProjectCopyIo,
            destinationWrite: async () => {
              writeCalls += 1;
              return bogus;
            },
          },
          fixture.source,
          fixture.runRoot,
        );
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "could not be written without progress");
      // exactly one write attempt: no infinite loop
      expect(writeCalls).toBe(1);
      expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
      await expectLeftoverStaging(fixture.runRoot, 0);
    }
  } finally {
    await dispose(fixture);
  }
});

test("19. a failing staging cleanup never replaces the original failure", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.source, "zz"));
    execSync(`mkfifo ${JSON.stringify(join(fixture.source, "zz", "pipe"))}`);

    // rm fault: the recursive removal fails after the ownership proof
    let rmCalls = 0;
    let failure: unknown;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingRm: async () => {
            rmCalls += 1;
            throw new Error("CLEANUP-RM-BOOM");
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
    expect((failure as Error).message).not.toContain("CLEANUP-RM-BOOM");
    // the staging tree stays behind as the documented absence of crash
    // recovery; the original failure remains authoritative
    await expectLeftoverStaging(fixture.runRoot, 1);
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    // remove the leftover staging tree of the first sub-case before the
    // second one starts on the same run root
    for (const leftover of (await readdir(fixture.runRoot)).filter((name) =>
      name.startsWith(".project-staging-"),
    )) {
      await rm(join(fixture.runRoot, leftover), { recursive: true, force: true });
    }

    // realpath fault: without a confirmed canonical resolution the removal
    // is skipped entirely
    let realpathCalls = 0;
    rmCalls = 0;
    failure = undefined;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingRealpath: async () => {
            realpathCalls += 1;
            throw new Error("CLEANUP-REALPATH-BOOM");
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
    expect((failure as Error).message).not.toContain("CLEANUP-REALPATH-BOOM");
    expect(realpathCalls).toBe(1);
    await expectLeftoverStaging(fixture.runRoot, 1);
    expect(rmCalls).toBe(0);
  } finally {
    await dispose(fixture);
  }
});

test("20. a vanished or replaced staging tree is never removed and sentinels stay", async () => {
  const fixture = await setup();
  try {
    await mkdir(join(fixture.source, "zz"));
    execSync(`mkfifo ${JSON.stringify(join(fixture.source, "zz", "pipe"))}`);
    await writeFile(join(fixture.root, "plain-file.txt"), "plain\n");
    await symlink(join(fixture.root, "plain-file.txt"), join(fixture.root, "link"));
    await mkdir(join(fixture.root, "other-dir"));

    const scenarios: Array<{
      name: string;
      inspect: (path: string) => Promise<import("node:fs").Stats | null>;
      realpath?: (path: string) => Promise<string>;
    }> = [
      { name: "vanished", inspect: async () => null },
      {
        name: "replaced by a regular file",
        inspect: async () => await lstat(join(fixture.root, "plain-file.txt")),
      },
      {
        name: "replaced by a symlink",
        inspect: async () => await lstat(join(fixture.root, "link")),
      },
      {
        name: "replaced by another directory",
        inspect: async () => await lstat(join(fixture.root, "other-dir")),
      },
      {
        name: "canonical resolution moved away",
        inspect: async (path) => await lstat(path),
        realpath: async () => join(fixture.root, "elsewhere"),
      },
    ];

    for (const scenario of scenarios) {
      await writeFile(join(fixture.runRoot, "sentinel.txt"), "KEEP\n");
      let rmCalls = 0;
      let failure: unknown;
      try {
        await prepareProjectCopy(
          {
            ...realProjectCopyIo,
            stagingInspect: scenario.inspect,
            ...(scenario.realpath === undefined ? {} : { stagingRealpath: scenario.realpath }),
            stagingRm: async (path, options) => {
              rmCalls += 1;
              await rm(path, options);
            },
          },
          fixture.source,
          fixture.runRoot,
        );
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
      // the replacement is never removed: the cleanup never ran
      expect(rmCalls).toBe(0);
      // the external sentinel in the run root is untouched
      expect(await readFile(join(fixture.runRoot, "sentinel.txt"), "utf8")).toBe("KEEP\n");
      // the staging tree stays behind (documented no-crash-recovery)
      await expectLeftoverStaging(fixture.runRoot, 1);
      // clean up for the next scenario
      await rm(join(fixture.runRoot, "sentinel.txt"));
      for (const leftover of (await readdir(fixture.runRoot)).filter((name) =>
        name.startsWith(".project-staging-"),
      )) {
        await rm(join(fixture.runRoot, leftover), { recursive: true, force: true });
      }
    }
  } finally {
    await dispose(fixture);
  }
});

test("21. an unreadable source file fails as a sanitized run-level input failure", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "unreadable.txt"), "secret body\n");
    await chmod(join(fixture.source, "unreadable.txt"), 0o000);
    try {
      let failure: unknown;
      try {
        await prepareRunProject(fixture.source, fixture.runRoot);
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "could not be opened for reading");
      const message = (failure as Error).message;
      // the diagnostic names only the relative entry and the errno code
      expect(message).toContain('"unreadable.txt"');
      expect(message).toContain("(errno EACCES)");
      expect(message).not.toContain(fixture.source);
      expect(message).not.toContain("permission denied");
      expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
      await expectLeftoverStaging(fixture.runRoot, 0);
    } finally {
      await chmod(join(fixture.source, "unreadable.txt"), 0o600);
    }
  } finally {
    await dispose(fixture);
  }
});

test("22. a fault-injected internal core cannot affect a parallel public prepareRunProject call", async () => {
  const fixture = await setup();
  try {
    // the source of the fault-injected internal call
    await writeFile(join(fixture.source, "task.md"), "injected source\n");
    // a different source for the public production call
    const publicSource = join(fixture.root, "public-source");
    await mkdir(publicSource);
    await writeFile(join(publicSource, "public.md"), "public body\n");

    // The internal core is fault-injected per call and blocks on a barrier
    // at its first destination write. There is no global IO to flip, so the
    // public call below runs on the fixed real IO while the injected core
    // is still parked.
    let reachedBarrier = false;
    let injectedWrites = 0;
    let releaseBarrier: () => void = () => {};
    const barrier = new Promise<void>((resolve) => {
      releaseBarrier = resolve;
    });
    const injectedIo: ProjectCopyIo = {
      ...realProjectCopyIo,
      destinationWrite: async (handle, chunk, offset, length, position) => {
        injectedWrites += 1;
        reachedBarrier = true;
        await barrier;
        const result = await handle.write(chunk, offset, length, position);
        return result.bytesWritten;
      },
    };

    const injectedCall = prepareProjectCopy(injectedIo, fixture.source, fixture.runRoot);
    while (!reachedBarrier) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    expect(injectedWrites).toBe(1);

    // The public production call must complete while the injected core is
    // still blocked: its IO is the fixed immutable real IO, not the
    // injected one.
    const prepared = await prepareRunProject(publicSource, fixture.runRoot);
    expect(prepared.project_root).toBe(join(fixture.runRoot, "project"));
    expect(await readFile(join(prepared.project_root, "public.md"), "utf8")).toBe("public body\n");
    expect((await readdir(prepared.project_root)).sort()).toEqual(["public.md"]);
    // The injected call's source never entered the public copy, and the
    // injected hook was not used again by the public call.
    expect(
      await readFile(join(prepared.project_root, "task.md"), "utf8").catch(() => "absent"),
    ).toBe("absent");
    expect(injectedWrites).toBe(1);

    // Release the barrier: the injected call continues on its own staging
    // tree, must fail closed because the project now exists, and must clean
    // its own staging tree. Its failure never touches the public project.
    releaseBarrier();
    let injectedFailure: unknown = "not-set";
    await injectedCall.then(
      () => {
        injectedFailure = null;
      },
      (cause) => {
        injectedFailure = cause;
      },
    );
    expect(injectedFailure).not.toBe(null);
    expectFailClosed(
      injectedFailure,
      "appeared at the fixed run-root location during preparation",
    );
    expect((injectedFailure as PipelineV2RuntimeError).reason).toBe("run_input_invalid");
    // The public copy stands exactly as published; nothing else remains.
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["project", "project-provenance.json"]);
    expect((await readdir(prepared.project_root)).sort()).toEqual(["public.md"]);
  } finally {
    await dispose(fixture);
  }
});

test("23. a chmod failure after the staging identity is captured removes the own staging tree", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "body\n");
    let chmodCalls = 0;
    let rmCalls = 0;
    let failure: unknown;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingChmod: async () => {
            chmodCalls += 1;
            throw new Error("CHMOD-BOOM");
          },
          stagingRm: async (path, options) => {
            rmCalls += 1;
            await rm(path, options);
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    // The original creation failure stays authoritative; the cleanup text
    // never replaces it.
    expectFailClosed(failure, "staging directory could not be created");
    expect((failure as Error).message).not.toContain("CHMOD-BOOM");
    expect(chmodCalls).toBe(1);
    // The own staging tree is removed exactly once.
    expect(rmCalls).toBe(1);
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    await expectLeftoverStaging(fixture.runRoot, 0);
  } finally {
    await dispose(fixture);
  }
});

test("24. a failed primary staging identity fixation never removes blindly", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "body\n");
    await writeFile(join(fixture.root, "plain-file.txt"), "plain\n");
    await writeFile(join(fixture.runRoot, "sentinel.txt"), "KEEP\n");

    // The identity hook fails: without a captured identity the ownership
    // proof is impossible, so no cleanup inspection and no removal may run.
    let identityInspects = 0;
    let cleanupInspects = 0;
    let rmCalls = 0;
    let failure: unknown;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingIdentityInspect: async () => {
            identityInspects += 1;
            throw new Error("IDENTITY-INSPECT-BOOM");
          },
          stagingInspect: async (path) => {
            cleanupInspects += 1;
            return await lstat(path);
          },
          stagingRm: async (path, options) => {
            rmCalls += 1;
            await rm(path, options);
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "staging directory could not be inspected after creation");
    expect((failure as Error).message).not.toContain("IDENTITY-INSPECT-BOOM");
    expect(identityInspects).toBe(1);
    expect(cleanupInspects).toBe(0);
    expect(rmCalls).toBe(0);
    // The staging directory stays behind (honest boundary) and nothing
    // else in the run root was touched.
    await expectLeftoverStaging(fixture.runRoot, 1);
    expect(await readFile(join(fixture.runRoot, "sentinel.txt"), "utf8")).toBe("KEEP\n");
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    for (const leftover of (await readdir(fixture.runRoot)).filter((name) =>
      name.startsWith(".project-staging-"),
    )) {
      await rm(join(fixture.runRoot, leftover), { recursive: true, force: true });
    }

    // The identity hook substitutes another object: the created directory
    // has no captured identity either, so nothing is removed.
    identityInspects = 0;
    cleanupInspects = 0;
    rmCalls = 0;
    failure = undefined;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingIdentityInspect: async () => {
            identityInspects += 1;
            return await lstat(join(fixture.root, "plain-file.txt"));
          },
          stagingInspect: async (path) => {
            cleanupInspects += 1;
            return await lstat(path);
          },
          stagingRm: async (path, options) => {
            rmCalls += 1;
            await rm(path, options);
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "staging directory is not a real directory after creation");
    expect(identityInspects).toBe(1);
    expect(cleanupInspects).toBe(0);
    expect(rmCalls).toBe(0);
    await expectLeftoverStaging(fixture.runRoot, 1);
    expect(await readFile(join(fixture.runRoot, "sentinel.txt"), "utf8")).toBe("KEEP\n");
  } finally {
    await dispose(fixture);
  }
});

test("25. a failing staging cleanup during creation never replaces the original failure", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "task.md"), "body\n");
    let rmCalls = 0;
    let failure: unknown;
    try {
      await prepareProjectCopy(
        {
          ...realProjectCopyIo,
          stagingChmod: async () => {
            throw new Error("CHMOD-BOOM");
          },
          stagingRm: async () => {
            rmCalls += 1;
            throw new Error("CLEANUP-RM-BOOM");
          },
        },
        fixture.source,
        fixture.runRoot,
      );
    } catch (cause) {
      failure = cause;
    }
    // The typed reason and the original diagnostic survive; neither the
    // chmod nor the cleanup text may replace or extend it.
    expectFailClosed(failure, "staging directory could not be created");
    expect((failure as Error).message).not.toContain("CHMOD-BOOM");
    expect((failure as Error).message).not.toContain("CLEANUP-RM-BOOM");
    // The removal was attempted exactly once and failed; the staging tree
    // stays behind as the documented absence of crash recovery.
    expect(rmCalls).toBe(1);
    await expectLeftoverStaging(fixture.runRoot, 1);
    expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("26. the errno suffix accepts only the closed system-code form and never leaks getter failures", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "blob.bin"), Buffer.alloc(4096, 1));

    const errnoError = (code: string): Error => {
      const error = new Error("synthetic fs failure");
      (error as NodeJS.ErrnoException).code = code;
      return error;
    };
    const cases: Array<{ name: string; makeCause: () => unknown; keep: string | null }> = [
      { name: "EACCES", makeCause: () => errnoError("EACCES"), keep: "(errno EACCES)" },
      // the upper bound of the closed form: 32 chars, uppercase + digits + _
      {
        name: "32-character code",
        makeCause: () => errnoError(`E${"A".repeat(31)}`),
        keep: `(errno E${"A".repeat(31)})`,
      },
      // anything outside the form renders no suffix at all
      { name: "33-character code", makeCause: () => errnoError(`E${"A".repeat(32)}`), keep: null },
      { name: "absolute path code", makeCause: () => errnoError("/etc/passwd"), keep: null },
      { name: "space code", makeCause: () => errnoError("EACCES permission"), keep: null },
      { name: "newline code", makeCause: () => errnoError("EACCES\nsecret"), keep: null },
      { name: "lowercase code", makeCause: () => errnoError("eacces"), keep: null },
      { name: "digit-leading code", makeCause: () => errnoError("1EACCES"), keep: null },
      { name: "punctuated code", makeCause: () => errnoError("EACCES-OR-WORSE"), keep: null },
      { name: "canary code", makeCause: () => errnoError("PROJECT-SECRET-CANARY"), keep: null },
      {
        name: "throwing code getter",
        makeCause: () => {
          const error = new Error("boom");
          Object.defineProperty(error, "code", {
            get() {
              throw new Error("GETTER-BOOM");
            },
          });
          return error;
        },
        keep: null,
      },
    ];
    for (const testCase of cases) {
      let failure: unknown;
      try {
        await prepareProjectCopy(
          {
            ...realProjectCopyIo,
            destinationWrite: async () => {
              throw testCase.makeCause();
            },
          },
          fixture.source,
          fixture.runRoot,
        );
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, 'project source file entry could not be copied "blob.bin"');
      const message = (failure as Error).message;
      if (testCase.keep === null) {
        expect(`${testCase.name}: ${message}`).not.toContain("(errno");
      } else {
        expect(message).toContain(testCase.keep);
      }
      // a throwing getter and arbitrary code text are never revealed
      expect(message).not.toContain("GETTER-BOOM");
      expect(message).not.toContain(fixture.source);
      // the failure cleaned its own staging tree
      await expectLeftoverStaging(fixture.runRoot, 0);
      expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    }
  } finally {
    await dispose(fixture);
  }
});

test("27. no mutable IO seam on the runtime module; the real IO is fixed and immutable", async () => {
  const runtime = await import("../src/pipeline_v2_runtime.ts");
  const runtimeKeys = Object.keys(runtime);
  expect(runtimeKeys).not.toContain("setProjectCopyIoForTests");
  expect(runtimeKeys).not.toContain("ProjectCopyIo");
  expect(runtimeKeys).not.toContain("projectCopyIo");

  const internal = await import("../src/pipeline_v2_project_copy_internal.ts");
  // the internal module exports exactly the per-call core, the fixed real
  // IO and the IO type — no installer, no mutable module state
  expect(Object.keys(internal).sort()).toEqual(["discardStagedProjectCopy", "prepareProjectCopy", "publishStagedProjectCopy", "realProjectCopyIo", "stageProjectCopy"]);
  expect(Object.isFrozen(internal.realProjectCopyIo)).toBe(true);
  expect(Object.keys(internal.realProjectCopyIo).sort()).toEqual([
    "destinationWrite",
    "stagingChmod",
    "stagingIdentityInspect",
    "stagingInspect",
    "stagingRealpath",
    "stagingRm",
  ]);
});

async function lstatOrNull(path: string): Promise<import("node:fs").Stats | null> {
  try {
    return await lstat(path);
  } catch {
    return null;
  }
}

async function expectLeftoverStaging(runRoot: string, count: number): Promise<void> {
  const entries = (await readdir(runRoot)).filter((name) => name.startsWith(".project-staging-"));
  expect(entries.length).toBe(count);
}

async function walkNames(root: string, prefix = ""): Promise<string[]> {
  const names: string[] = [];
  const entries = (await readdir(root)).sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
  for (const name of entries) {
    const path = join(root, name);
    const relativePath = prefix === "" ? name : `${prefix}/${name}`;
    const info = await lstat(path);
    if (info.isDirectory()) {
      names.push(`${relativePath}/`);
      names.push(...(await walkNames(path, relativePath)));
    } else if (info.isSymbolicLink()) {
      names.push(`${relativePath}@`);
    } else {
      names.push(relativePath);
    }
  }
  return names;
}

async function treeDigest(root: string): Promise<string> {
  const { createHash } = await import("node:crypto");
  const hash = createHash("sha256");
  for (const name of await walkNames(root)) {
    hash.update(name);
    hash.update("\0");
    const path = join(root, name.replace(/\/$/, ""));
    const info = await lstat(path);
    if (info.isFile()) {
      hash.update(await readFile(path));
    }
    hash.update("\n");
  }
  return hash.digest("hex");
}

// --- provenance capture of the single copy traversal (state schema v10) ---

import {
  createHash,
} from "node:crypto";
import {
  pipelineV2ProjectProvenanceBinding,
} from "../src/pipeline_v2_project_provenance.ts";
import {
  loadPipelineV2ProjectProvenance,
  verifyPipelineV2InitialProjectWorkspace,
} from "../src/pipeline_v2_project_provenance_store.ts";

const digestOf = (bytes: Buffer | string): string => createHash("sha256").update(bytes).digest("hex");

test("28. the manifest records the source mode and the normalized workspace mode", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "plain.txt"), "plain\n", { mode: 0o644 });
    await writeFile(join(fixture.source, "script.sh"), "#!/bin/sh\n", { mode: 0o755 });
    await mkdir(join(fixture.source, "dir"), { mode: 0o750 });
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const byPath = new Map(prepared.manifest.manifest.entries.map((entry) => [entry.path, entry]));
    const plain = byPath.get("plain.txt") as { kind: string; source_mode: number; workspace_mode: number };
    const script = byPath.get("script.sh") as { kind: string; source_mode: number; workspace_mode: number };
    const dir = byPath.get("dir") as { kind: string; source_mode: number; workspace_mode: number };
    expect(plain.source_mode).toBe(0o644);
    expect(plain.workspace_mode).toBe(0o600);
    expect(script.source_mode).toBe(0o755);
    expect(script.workspace_mode).toBe(0o700);
    expect(dir.source_mode).toBe(0o750);
    expect(dir.workspace_mode).toBe(0o700);
  } finally {
    await dispose(fixture);
  }
});

test("29. the manifest records the exact copied bytes and byte count", async () => {
  const fixture = await setup();
  try {
    const body = "the exact copied bytes\n";
    await writeFile(join(fixture.source, "f.txt"), body, { mode: 0o644 });
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const entry = prepared.manifest.manifest.entries[0] as { kind: string; byte_count: number; sha256: string };
    expect(entry.byte_count).toBe(body.length);
    expect(entry.sha256).toBe(digestOf(body));
  } finally {
    await dispose(fixture);
  }
});

test("30. a source hardlink pair is split and durably recorded as one canonical group", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "h1.txt"), "linked\n", { mode: 0o644 });
    await (await import("node:fs/promises")).link(join(fixture.source, "h1.txt"), join(fixture.source, "h2.txt"));
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const entries = prepared.manifest.manifest.entries as unknown as { path: string; multiple_links: boolean; hardlink_group?: number }[];
    const h1 = entries.find((entry) => entry.path === "h1.txt");
    const h2 = entries.find((entry) => entry.path === "h2.txt");
    expect(h1?.multiple_links).toBe(true);
    expect(h2?.multiple_links).toBe(true);
    expect(h1?.hardlink_group).toBe(h2?.hardlink_group);
    expect(typeof h1?.hardlink_group).toBe("number");
    // the workspace copies are independent regular files
    const ino1 = (await lstat(join(fixture.runRoot, "project", "h1.txt"))).ino;
    const ino2 = (await lstat(join(fixture.runRoot, "project", "h2.txt"))).ino;
    expect(ino1).not.toBe(ino2);
  } finally {
    await dispose(fixture);
  }
});

test("31. a symlink entry carries the verbatim target and the identical-targets confirmation", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "real.txt"), "x\n", { mode: 0o644 });
    await symlink("real.txt", join(fixture.source, "link"));
    await symlink(join(fixture.source, "real.txt"), join(fixture.source, "abs"));
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const entries = prepared.manifest.manifest.entries as { path: string; kind: string; target?: string; targets_identical?: boolean }[];
    const link = entries.find((entry) => entry.path === "link");
    const abs = entries.find((entry) => entry.path === "abs");
    expect(link?.target).toBe("real.txt");
    expect(link?.targets_identical).toBe(true);
    expect(abs?.target).toBe(join(fixture.source, "real.txt"));
    expect(abs?.targets_identical).toBe(true);
    expect(await readlink(join(fixture.runRoot, "project", "link"))).toBe("real.txt");
  } finally {
    await dispose(fixture);
  }
});

test("32. the manifest entries are in deterministic code-unit order and the workspace verifies", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "b.txt"), "b\n", { mode: 0o644 });
    await mkdir(join(fixture.source, "a"), { mode: 0o700 });
    await writeFile(join(fixture.source, "a", "c.txt"), "c\n", { mode: 0o644 });
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(prepared.manifest.manifest.entries.map((entry) => entry.path)).toEqual(["a", "a/c.txt", "b.txt"]);
    await verifyPipelineV2InitialProjectWorkspace(prepared.project_root, prepared.manifest);
    const loaded = await loadPipelineV2ProjectProvenance(fixture.runRoot);
    expect(loaded?.sha256).toBe(prepared.manifest.sha256);
    expect(prepared.binding).toEqual(pipelineV2ProjectProvenanceBinding(prepared.manifest));
  } finally {
    await dispose(fixture);
  }
});

test("33. an empty project produces a zero-entry manifest and verifies", async () => {
  const fixture = await setup();
  try {
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(prepared.manifest.manifest.entries).toEqual([]);
    expect(prepared.binding.entry_count).toBe(0);
    await verifyPipelineV2InitialProjectWorkspace(prepared.project_root, prepared.manifest);
  } finally {
    await dispose(fixture);
  }
});

test("34. two histories with identical final and current trees produce different manifests (the audit indistinguishability is broken)", async () => {
  // History A: baseline {f.txt}; the agent created h.txt in the workspace.
  const a = await setup();
  // History B: baseline {f.txt, h.txt}; the agent changed nothing; the operator
  // deleted h.txt from the registered project after the snapshot.
  const b = await setup();
  try {
    await writeFile(join(a.source, "f.txt"), "X\n", { mode: 0o644 });
    const preparedA = await prepareRunProject(a.source, a.runRoot);
    await writeFile(join(a.runRoot, "project", "h.txt"), "Q\n", { mode: 0o600 });

    await writeFile(join(b.source, "f.txt"), "X\n", { mode: 0o644 });
    await writeFile(join(b.source, "h.txt"), "Q\n", { mode: 0o600 });
    const preparedB = await prepareRunProject(b.source, b.runRoot);
    await rm(join(b.source, "h.txt"));

    // Identical observables: the final run workspace trees and the current
    // operator project trees are byte-identical across the two histories.
    const finalA = await readFile(join(a.runRoot, "project", "h.txt"), "utf8");
    const finalB = await readFile(join(b.runRoot, "project", "h.txt"), "utf8");
    expect(finalA).toBe(finalB);
    const hasOperatorH = await lstat(join(b.source, "h.txt")).then(() => true, () => false);
    expect(hasOperatorH).toBe(false);
    expect(await lstat(join(a.source, "h.txt")).then(() => true, () => false)).toBe(false);

    // But the durable manifests differ exactly where the truth differs:
    // A's baseline has no h.txt (the agent added it); B's baseline has it.
    const pathsA = preparedA.manifest.manifest.entries.map((entry) => entry.path);
    const pathsB = preparedB.manifest.manifest.entries.map((entry) => entry.path);
    expect(pathsA).toEqual(["f.txt"]);
    expect(pathsB).toEqual(["f.txt", "h.txt"]);
    expect(preparedA.manifest.sha256).not.toBe(preparedB.manifest.sha256);
  } finally {
    await dispose(a);
    await dispose(b);
  }
});

test("35. a source object modified while it is being read fails the preparation closed", async () => {
  const fixture = await setup();
  try {
    // A file large enough to span several write-loop iterations, mutated by
    // a concurrent writer through the destination-write seam while the copy
    // is still reading it.
    const body = Buffer.alloc(4 * 128 * 1024, 0x61);
    await writeFile(join(fixture.source, "big.txt"), body, { mode: 0o644 });
    let writes = 0;
    const wrappedIo: ProjectCopyIo = {
      ...realProjectCopyIo,
      destinationWrite: async (handle, chunk, offset, length, position) => {
        writes += 1;
        if (writes === 2) {
          // The writer appends to the source while the copy holds the read
          // open; the post-read coherence re-stat must detect it.
          const handle2 = await (await import("node:fs/promises")).open(join(fixture.source, "big.txt"), "a");
          await handle2.write(Buffer.from("APPENDED-DURING-READ"));
          await handle2.close();
        }
        return realProjectCopyIo.destinationWrite(handle, chunk, offset, length, position);
      },
    };
    let failure: unknown;
    try {
      await (await import("../src/pipeline_v2_project_copy_internal.ts")).prepareProjectCopy(wrappedIo, fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "was modified while it was being read");
  } finally {
    await dispose(fixture);
  }
});

test("36. a source object whose permission mode changes while it is being read fails closed", async () => {
  const fixture = await setup();
  try {
    const body = Buffer.alloc(2 * 128 * 1024, 0x62);
    await writeFile(join(fixture.source, "touched.txt"), body, { mode: 0o644 });
    let writes = 0;
    const wrappedIo: ProjectCopyIo = {
      ...realProjectCopyIo,
      destinationWrite: async (handle, chunk, offset, length, position) => {
        writes += 1;
        if (writes === 1) {
          // A metadata-only touch during the read changes the ctime; the
          // post-read coherence re-stat must reject the copy.
          await chmod(join(fixture.source, "touched.txt"), 0o666);
        }
        return realProjectCopyIo.destinationWrite(handle, chunk, offset, length, position);
      },
    };
    let failure: unknown;
    try {
      await (await import("../src/pipeline_v2_project_copy_internal.ts")).prepareProjectCopy(wrappedIo, fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "was modified while it was being read");
  } finally {
    await dispose(fixture);
  }
});

test("37. C5: a copy whose published bytes diverge from its capture fails closed before any binding", async () => {
  // The public wrapper composes exactly these steps with the fixed real
  // IO; the corrupted-write fault is injected at the internal composition
  // boundary, which runs the same verification gate the wrapper runs
  // before it returns.
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "f.txt"), "ORIGINAL\n", { mode: 0o644 });
    const corruptedWrite: ProjectCopyIo = {
      ...realProjectCopyIo,
      destinationWrite: async (handle, chunk, offset, length, position) => {
        if (position === 0 && chunk.byteLength > 0) {
          const corrupted = Buffer.from(chunk);
          corrupted[0] = corrupted[0] === 0x58 ? 0x59 : 0x58;
          return realProjectCopyIo.destinationWrite(handle, corrupted, offset, length, position);
        }
        return realProjectCopyIo.destinationWrite(handle, chunk, offset, length, position);
      },
    };
    const { stageProjectCopy, publishStagedProjectCopy, discardStagedProjectCopy } =
      await import("../src/pipeline_v2_project_copy_internal.ts");
    const { preparePipelineV2ProjectProvenance, PipelineV2ProjectProvenanceError } =
      await import("../src/pipeline_v2_project_provenance.ts");
    const {
      publishPipelineV2ProjectProvenance,
      verifyPipelineV2InitialProjectWorkspace,
    } = await import("../src/pipeline_v2_project_provenance_store.ts");
    const staged = await stageProjectCopy(corruptedWrite, fixture.source, fixture.runRoot);
    try {
      // The capture digest describes the bytes read from the source; the
      // corrupted write published different bytes into the workspace.
      const prepared = preparePipelineV2ProjectProvenance(staged.entries);
      expect(prepared.manifest.entries[0]).toMatchObject({ byte_count: 9 });
      await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
      await publishStagedProjectCopy(corruptedWrite, staged);
      let failure: unknown;
      try {
        await verifyPipelineV2InitialProjectWorkspace(join(fixture.runRoot, "project"), prepared);
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(PipelineV2ProjectProvenanceError);
      expect((failure as { reason?: string }).reason).toBe("workspace_mismatch");
      expect((failure as Error).message).toContain("content digest");
      // The published project and the manifest stay for diagnostics; the
      // wrapper's gate would have refused the preparation before any
      // state document could exist.
    } finally {
      await discardStagedProjectCopy(realProjectCopyIo, {
        run_root: staged.run_root,
        staging_path: staged.staging_path,
        staging_identity: staged.staging_identity,
        entries: [],
      });
    }
  } finally {
    await dispose(fixture);
  }
});

test("38. C1: a manifest-publication failure leaves nothing published and a fresh retry binds the new copy", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "f.txt"), "one\n", { mode: 0o644 });
    const { stageProjectCopy, discardStagedProjectCopy } = await import("../src/pipeline_v2_project_copy_internal.ts");
    const {
      preparePipelineV2ProjectProvenance,
    } = await import("../src/pipeline_v2_project_provenance.ts");
    const {
      publishProjectProvenanceWithIo,
    } = await import("../src/pipeline_v2_project_provenance_store_internal.ts");
    const { realImmutableDocumentIo } = await import("../src/pipeline_v2_immutable_document_store_internal.ts");
    const staged = await stageProjectCopy(realProjectCopyIo, fixture.source, fixture.runRoot);
    const prepared = preparePipelineV2ProjectProvenance(staged.entries);
    const failingOpen = {
      ...realImmutableDocumentIo,
      openTempExclusive: async () => {
        throw Object.assign(new Error("denied"), { code: "EACCES" });
      },
    } as unknown as typeof realImmutableDocumentIo;
    let failure: unknown;
    try {
      await publishProjectProvenanceWithIo(failingOpen, fixture.runRoot, prepared);
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(Object);
    expect((failure as { outcome?: string }).outcome).toBe("not_published");
    await discardStagedProjectCopy(realProjectCopyIo, staged);
    // Nothing was published: no manifest, no workspace, no staging leftover.
    expect((await readdir(fixture.runRoot)).sort()).toEqual([]);
    // A fresh preparation binds the new copy: manifest and workspace agree.
    const retry = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(retry.binding.entry_count).toBe(1);
    const loaded = await loadPipelineV2ProjectProvenance(fixture.runRoot);
    expect(loaded?.sha256).toBe(retry.manifest.sha256);
  } finally {
    await dispose(fixture);
  }
});

test("39. C2/C3: a visible manifest with an absent workspace refuses the retry and never re-derives provenance from the source", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "f.txt"), "before\n", { mode: 0o644 });
    const { stageProjectCopy, discardStagedProjectCopy } = await import("../src/pipeline_v2_project_copy_internal.ts");
    const {
      preparePipelineV2ProjectProvenance,
    } = await import("../src/pipeline_v2_project_provenance.ts");
    const {
      publishPipelineV2ProjectProvenance,
    } = await import("../src/pipeline_v2_project_provenance_store.ts");
    // The crash window between the manifest publication and the workspace
    // rename: the manifest is durable, the workspace was never published.
    const staged = await stageProjectCopy(realProjectCopyIo, fixture.source, fixture.runRoot);
    const prepared = preparePipelineV2ProjectProvenance(staged.entries);
    await publishPipelineV2ProjectProvenance(fixture.runRoot, prepared);
    await discardStagedProjectCopy(realProjectCopyIo, staged);
    const manifestBytes = await readFile(join(fixture.runRoot, "project-provenance.json"), "utf8");
    // The source changes while the run is interrupted.
    await writeFile(join(fixture.source, "added-after-crash.txt"), "later\n", { mode: 0o644 });
    let failure: unknown;
    try {
      await prepareRunProject(fixture.source, fixture.runRoot);
    } catch (cause) {
      failure = cause;
    }
    expectFailClosed(failure, "already carries a project provenance manifest");
    // The manifest is byte-identical; the workspace was never materialized
    // from the changed source; nothing was rebound.
    expect(await readFile(join(fixture.runRoot, "project-provenance.json"), "utf8")).toBe(manifestBytes);
    expect(await lstatOrNull(join(fixture.runRoot, "project"))).toBeNull();
  } finally {
    await dispose(fixture);
  }
});

test("40. C6: worker-style workspace changes leave the manifest byte-identical", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.source, "f.txt"), "base\n", { mode: 0o644 });
    await mkdir(join(fixture.source, "d"), { mode: 0o700 });
    const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
    const manifestPath = join(fixture.runRoot, "project-provenance.json");
    const before = await lstat(manifestPath);
    const beforeBytes = await readFile(manifestPath, "utf8");
    // The worker (or anyone with the workspace authority) adds, modifies,
    // deletes, re-modes and re-types entries.
    await writeFile(join(fixture.runRoot, "project", "worker-added.txt"), "added\n", { mode: 0o600 });
    await writeFile(join(fixture.runRoot, "project", "f.txt"), "modified by worker\n", { mode: 0o600 });
    await rm(join(fixture.runRoot, "project", "d"), { recursive: true });
    await chmod(join(fixture.runRoot, "project", "f.txt"), 0o700);
    await symlink("elsewhere", join(fixture.runRoot, "project", "worker-link"));
    const after = await lstat(manifestPath);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(await readFile(manifestPath, "utf8")).toBe(beforeBytes);
    expect(await readFile(manifestPath, "utf8")).toBe(prepared.manifest.canonical_json);
    // The initial-workspace verifier is deliberately no longer meaningful:
    // the workspace is worker-mutable after the first activation.
    let failure: unknown;
    try {
      await verifyPipelineV2InitialProjectWorkspace(prepared.project_root, prepared.manifest);
    } catch (cause) {
      failure = cause;
    }
    expect((failure as { reason?: string }).reason).toBe("workspace_mismatch");
  } finally {
    await dispose(fixture);
  }
});
