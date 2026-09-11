import { execSync } from "node:child_process";
import { createServer } from "node:net";
import { chmod, lstat, mkdir, mkdtemp, readlink, readFile, readdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  prepareRunProject,
  setProjectCopyIoForTests,
  type PreparedRunProject,
  type ProjectCopyIo,
} from "../src/pipeline_v2_runtime.ts";
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
    expectFailClosed(failure, "already exists");
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
    const prepared: PreparedRunProject = await prepareRunProject(fixture.source, fixture.runRoot);
    expect(Object.isFrozen(prepared)).toBe(true);
    expect(Object.keys(prepared).sort()).toEqual(["project_root", "run_root"]);
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
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["project"]);
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
    setProjectCopyIoForTests({
      ...realIo(),
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
    });
    try {
      const prepared = await prepareRunProject(fixture.source, fixture.runRoot);
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
      setProjectCopyIoForTests(null);
    }
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
      setProjectCopyIoForTests({
        ...realIo(),
        destinationWrite: async () => {
          writeCalls += 1;
          return bogus;
        },
      });
      try {
        let failure: unknown;
        try {
          await prepareRunProject(fixture.source, fixture.runRoot);
        } catch (cause) {
          failure = cause;
        }
        expectFailClosed(failure, "could not be written without progress");
        // exactly one write attempt: no infinite loop
        expect(writeCalls).toBe(1);
        expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
        await expectLeftoverStaging(fixture.runRoot, 0);
      } finally {
        setProjectCopyIoForTests(null);
      }
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
    setProjectCopyIoForTests({
      ...realIo(),
      stagingRm: async () => {
        rmCalls += 1;
        throw new Error("CLEANUP-RM-BOOM");
      },
    });
    try {
      let failure: unknown;
      try {
        await prepareRunProject(fixture.source, fixture.runRoot);
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
      expect((failure as Error).message).not.toContain("CLEANUP-RM-BOOM");
      // the staging tree stays behind as the documented absence of crash
      // recovery; the original failure remains authoritative
      await expectLeftoverStaging(fixture.runRoot, 1);
      expect((await lstatOrNull(join(fixture.runRoot, "project"))) === null).toBe(true);
    } finally {
      setProjectCopyIoForTests(null);
    }
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
    setProjectCopyIoForTests({
      ...realIo(),
      stagingRealpath: async () => {
        realpathCalls += 1;
        throw new Error("CLEANUP-REALPATH-BOOM");
      },
    });
    try {
      let failure: unknown;
      try {
        await prepareRunProject(fixture.source, fixture.runRoot);
      } catch (cause) {
        failure = cause;
      }
      expectFailClosed(failure, "FIFO, socket, device or another unsupported object");
      expect((failure as Error).message).not.toContain("CLEANUP-REALPATH-BOOM");
      expect(realpathCalls).toBe(1);
      await expectLeftoverStaging(fixture.runRoot, 1);
      expect(rmCalls).toBe(0);
    } finally {
      setProjectCopyIoForTests(null);
    }
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
      setProjectCopyIoForTests({
        ...realIo(),
        stagingInspect: scenario.inspect,
        ...(scenario.realpath === undefined ? {} : { stagingRealpath: scenario.realpath }),
        stagingRm: async (path, options) => {
          rmCalls += 1;
          await rm(path, options);
        },
      });
      try {
        let failure: unknown;
        try {
          await prepareRunProject(fixture.source, fixture.runRoot);
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
      } finally {
        setProjectCopyIoForTests(null);
      }
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

function realIo(): ProjectCopyIo {
  return {
    destinationWrite: async (handle, chunk, offset, length, position) => {
      const result = await handle.write(chunk, offset, length, position);
      return result.bytesWritten;
    },
    stagingInspect: async (path) => {
      try {
        return await lstat(path);
      } catch {
        return null;
      }
    },
    stagingRealpath: (path) => realpath(path),
    stagingRm: (path, options) => rm(path, options),
  };
}

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
