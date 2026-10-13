/**
 * The permanent contract suite of the durable control-plane run binding
 * store: the versioned immutable runId-to-projectId authority binding.
 *
 * The battery covers the exact document shape and canonical bytes, the
 * exclusive publication with read-back verification over the shared
 * immutable-document substrate, the created/existing/conflict claim
 * semantics, the strictly read-only load with the full hostile-artifact
 * matrix, the store infrastructure defenses (namespace mode, root
 * identity fixation), the crash and durability windows through per-call
 * IO fault injection, the authority resolution boundary over the loaded
 * registry, real independent OS processes for the concurrency matrix,
 * the permanent composition smoke through the production layers
 * (registry -> resolve -> claim -> subprocess client -> real CLI + fake
 * Docker Helper daemon -> fresh store -> authority -> inspect), and the
 * source-restriction scans.
 */
import { spawn } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readdirSync,
  lstatSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { chmod, link, lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test, afterAll } from "bun:test";
import {
  ControlPlaneRunBindingStore,
  ControlPlaneRunBindingError,
  resolveBoundControlPlaneProject,
  type ControlPlaneRunBindingFailureReason,
} from "../src/control_plane/run_binding_store.ts";
import {
  claimRunBindingWithIo,
  loadRunBindingWithIo,
  openRunBindingStoreWithIo,
  prepareRunBinding,
  realRunBindingStoreIo,
  type RunBindingStoreIo,
} from "../src/control_plane/run_binding_store_internal.ts";
import {
  ControlPlaneProjectRegistryError,
  loadControlPlaneProjectRegistry,
  resolveControlPlaneRunCommand,
  type ControlPlaneProjectRegistry,
} from "../src/control_plane/project_registry.ts";
import { OrchestratorSubprocessClient } from "../src/control_plane/subprocess_client.ts";

async function makeRoot(prefix = "cp-binding-"): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  await chmod(root, 0o700);
  return root;
}

async function makeFifo(path: string): Promise<void> {
  const child = spawn("mkfifo", [path]);
  await new Promise<void>((resolve, reject) => {
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

function injectedFailure(code?: string): Error {
  return Object.assign(new Error("injected failure"), code === undefined ? {} : { code });
}

function ioFaulting(hook: keyof RunBindingStoreIo, failure: unknown): RunBindingStoreIo {
  return Object.freeze({
    ...realRunBindingStoreIo,
    [hook]: async () => {
      throw failure;
    },
  }) as unknown as RunBindingStoreIo;
}

/**
 * An IO that hides one leaf path from exactly one lstat (so a claim that
 * already ran once reaches the substrate's EEXIST adoption path) and can
 * fail the Nth directory-open call (the parent durability barriers).
 */
function claimIov2(options: { hideLeafOnce?: string; openDirFailOnCall?: number }): RunBindingStoreIo {
  const base = realRunBindingStoreIo;
  let hidden = false;
  let openDirCalls = 0;
  return Object.freeze({
    ...base,
    lstatOrNull: async (path: string) => {
      if (options.hideLeafOnce !== undefined && !hidden && path === options.hideLeafOnce) {
        hidden = true;
        return null;
      }
      return base.lstatOrNull(path);
    },
    openDir: async (path: string) => {
      openDirCalls += 1;
      if (options.openDirFailOnCall !== undefined && openDirCalls === options.openDirFailOnCall) {
        throw injectedFailure("EIO");
      }
      return base.openDir(path);
    },
  }) as unknown as RunBindingStoreIo;
}

function handleFaultIo(fault: Partial<Record<"write" | "sync" | "close", unknown>>): RunBindingStoreIo {
  const base = realRunBindingStoreIo;
  return Object.freeze({
    ...base,
    openTempExclusive: async (path: string) => {
      const handle = await base.openTempExclusive(path);
      return {
        write: async (chunk: Uint8Array, offset: number, length: number, position: number) => {
          if (fault.write !== undefined) throw fault.write;
          return handle.write(chunk, offset, length, position);
        },
        read: (chunk: Uint8Array, offset: number, length: number, position: number) => handle.read(chunk, offset, length, position),
        stat: () => handle.stat(),
        sync: async () => {
          if (fault.sync !== undefined) throw fault.sync;
          return handle.sync();
        },
        close: async () => {
          if (fault.close !== undefined) {
            try {
              await handle.close();
            } catch {
              // the fault is observed by the caller; no descriptor leaks
            }
            throw fault.close;
          }
          return handle.close();
        },
      };
    },
  }) as unknown as RunBindingStoreIo;
}

async function leafMode(path: string): Promise<number> {
  return (await lstat(path)).mode & 0o7777;
}

/** Recursive fingerprint of relative paths, kinds, modes, identities and bytes. */
async function fingerprint(root: string): Promise<string> {
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return "absent";
  }
  const parts: string[] = [];
  for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
    const child = join(root, entry.name);
    const info = await lstat(child);
    const kind = info.isSymbolicLink() ? "link" : info.isDirectory() ? "dir" : info.isFile() ? "file" : "other";
    const bytes = info.isFile() ? (await readFile(child)).toString("hex") : "";
    parts.push(`${entry.name}:${kind}:${(info.mode & 0o7777).toString(8)}:${info.dev}:${info.ino}:${bytes.length}:${bytes}`);
    if (info.isDirectory() && !info.isSymbolicLink()) {
      parts.push(await fingerprint(child));
    }
  }
  return parts.join("|");
}

function expectBindingError(cause: unknown, reason: ControlPlaneRunBindingFailureReason, fragment = ""): ControlPlaneRunBindingError {
  expect(cause).toBeInstanceOf(ControlPlaneRunBindingError);
  const error = cause as ControlPlaneRunBindingError;
  expect(error.reason).toBe(reason);
  if (fragment !== "") {
    expect(error.message).toContain(fragment);
  }
  return error;
}

async function expectWrongRoot(root: string): Promise<void> {
  let cause: unknown;
  try {
    await ControlPlaneRunBindingStore.open(root);
  } catch (caught) {
    cause = caught;
  }
  expectBindingError(cause, "invalid_binding_root");
}

/** Synchronous recursive fingerprint of relative paths, kinds, modes, identities and bytes. */
function fingerprintSync(root: string): string {
  let entries;
  try {
    entries = readdirSync(root, { withFileTypes: true });
  } catch {
    return "absent";
  }
  const parts: string[] = [];
  for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
    const child = join(root, entry.name);
    const info = lstatSync(child);
    const kind = info.isSymbolicLink() ? "link" : info.isDirectory() ? "dir" : info.isFile() ? "file" : "other";
    const bytes = info.isFile() ? readFileSync(child).toString("hex") : "";
    parts.push(`${entry.name}:${kind}:${(info.mode & 0o7777).toString(8)}:${info.dev}:${info.ino}:${bytes.length}:${bytes}`);
    if (info.isDirectory() && !info.isSymbolicLink()) {
      parts.push(fingerprintSync(child));
    }
  }
  return parts.join("|");
}

interface ChildClaimResult {
  ok: boolean;
  kind?: string;
  reason?: string;
  message?: string;
  binding?: { schemaVersion: number; runId: string; projectId: string };
}

async function runChild(storeRoot: string, runId: string, projectId: string, mode?: "load"): Promise<ChildClaimResult> {
  const args = [join(import.meta.dir, "control_plane_run_binding_claim_child.ts"), storeRoot, runId, projectId];
  if (mode !== undefined) args.push(mode);
  const proc = Bun.spawn([process.execPath, ...args], { stdout: "pipe", stderr: "pipe" });
  const out = await new Response(proc.stdout as unknown as ReadableStream<Uint8Array>).text();
  await proc.exited;
  return JSON.parse(out) as ChildClaimResult;
}

const CANARY = "CANARY_secret_value";

describe("control plane run binding store: open", () => {
  test("1. rejects a root that is not a clean absolute path", async () => {
    for (const bad of ["relative/path", "", "x", "./nested", "../up", "/double//slash/", "/trailing/"]) {
      let cause: unknown;
      try {
        await ControlPlaneRunBindingStore.open(bad);
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "invalid_binding_root");
    }
  });

  test("2. rejects a missing root, a file root, a FIFO root and a leaf symlink root", async () => {
    const root = await makeRoot();
    try {
      await expectWrongRoot(join(root, "missing"));
      const fileRoot = join(root, "file-root");
      await writeFile(fileRoot, "x", { mode: 0o600 });
      await expectWrongRoot(fileRoot);
      const fifoRoot = join(root, "fifo-root");
      await makeFifo(fifoRoot);
      await expectWrongRoot(fifoRoot);
      const realDir = join(root, "real");
      await mkdir(realDir, { mode: 0o700 });
      const linkRoot = join(root, "root-link");
      await symlink(realDir, linkRoot);
      await expectWrongRoot(linkRoot);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("3. canonicalizes a symlinked ancestor and binds the real directory", async () => {
    const root = await makeRoot();
    try {
      const real = join(root, "real-root");
      await mkdir(real, { mode: 0o700 });
      const linkParent = join(root, "link-parent");
      await symlink(root, linkParent);
      const store = await ControlPlaneRunBindingStore.open(join(linkParent, "real-root"));
      const claim = await store.claim({ runId: "anc-run", projectId: "anc-project" });
      expect(claim.kind).toBe("created");
      const stored = await readFile(join(real, "bindings", "anc-run.json"), "utf8");
      expect(stored).toBe(prepareRunBinding("anc-run", "anc-project").canonical_json);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("4. the store instance is frozen and repeated opens stay independent", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      expect(Object.isFrozen(store)).toBe(true);
      const other = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "frozen-run", projectId: "frozen-project" });
      const claim = await other.claim({ runId: "frozen-run", projectId: "frozen-project" });
      expect(claim.kind).toBe("existing");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("5. an operator root with a wider mode is accepted; the namespace stays 0700", async () => {
    const root = await makeRoot();
    try {
      await chmod(root, 0o755);
      const store = await ControlPlaneRunBindingStore.open(root);
      const claim = await store.claim({ runId: "wide-run", projectId: "wide-project" });
      expect(claim.kind).toBe("created");
      expect(await leafMode(join(root, "bindings"))).toBe(0o700);
      expect(await leafMode(join(root, "bindings", "wide-run.json"))).toBe(0o600);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("control plane run binding store: claim and load", () => {
  test("6. the first claim publishes the exact canonical bytes at the fixed layout", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      const claim = await store.claim({ runId: "bind-run", projectId: "bind-project" });
      expect(claim.kind).toBe("created");
      expect(claim.binding).toEqual({ schemaVersion: 1, runId: "bind-run", projectId: "bind-project" });
      expect(Object.isFrozen(claim.binding)).toBe(true);
      expect(Object.isFrozen(claim)).toBe(true);
      const prepared = prepareRunBinding("bind-run", "bind-project");
      expect(prepared.canonical_json).toBe('{"project_id":"bind-project","run_id":"bind-run","schema_version":1}');
      expect(prepared.sha256).toMatch(/^[0-9a-f]{64}$/);
      const stored = await readFile(join(root, "bindings", "bind-run.json"), "utf8");
      expect(stored).toBe(prepared.canonical_json);
      expect(stored.endsWith("\n")).toBe(false);
      expect(await leafMode(join(root, "bindings"))).toBe(0o700);
      expect(await leafMode(join(root, "bindings", "bind-run.json"))).toBe(0o600);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("7. an exact retry adopts the binding without touching inode, mtime or bytes", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      const first = await store.claim({ runId: "retry-run", projectId: "retry-project" });
      expect(first.kind).toBe("created");
      const leaf = join(root, "bindings", "retry-run.json");
      const before = await lstat(leaf);
      const bytes = await readFile(leaf);
      const second = await store.claim({ runId: "retry-run", projectId: "retry-project" });
      expect(second.kind).toBe("existing");
      expect(second.binding).toEqual(first.binding);
      const after = await lstat(leaf);
      expect(after.dev).toBe(before.dev);
      expect(after.ino).toBe(before.ino);
      expect(after.mtimeMs).toBe(before.mtimeMs);
      expect(await readFile(leaf)).toEqual(bytes);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("8. a different project is a typed conflict that never names the owner", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "conflict-run", projectId: "owner-project-alpha" });
      const leaf = join(root, "bindings", "conflict-run.json");
      const before = await lstat(leaf);
      let cause: unknown;
      try {
        await store.claim({ runId: "conflict-run", projectId: "intruder-project-beta" });
      } catch (caught) {
        cause = caught;
      }
      const error = expectBindingError(cause, "binding_conflict");
      expect(error.message).not.toContain("owner-project-alpha");
      expect(error.message).not.toContain(CANARY);
      const after = await lstat(leaf);
      expect(after.ino).toBe(before.ino);
      expect(await readFile(leaf, "utf8")).toBe(prepareRunBinding("conflict-run", "owner-project-alpha").canonical_json);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("9. unsafe claim scalars are rejected without any write", async () => {
    const root = await makeRoot();
    try {
      const before = await fingerprint(root);
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const bad of ["", "..x", "a/b", "a\\b", 42, null, {}, "a".repeat(129)]) {
        let cause: unknown;
        try {
          await store.claim({ runId: bad as string, projectId: "ok-project" });
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_invalid");
        let cause2: unknown;
        try {
          await store.claim({ runId: "ok-run", projectId: bad as string });
        } catch (caught) {
          cause2 = caught;
        }
        expectBindingError(cause2, "binding_invalid");
      }
      expect(await fingerprint(root)).toBe(before);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("10. different run ids bind independently", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      const a = await store.claim({ runId: "run-a", projectId: "project-a" });
      const b = await store.claim({ runId: "run-b", projectId: "project-b" });
      expect(a.kind).toBe("created");
      expect(b.kind).toBe("created");
      expect(await store.load("run-a")).toEqual({ schemaVersion: 1, runId: "run-a", projectId: "project-a" });
      expect(await store.load("run-b")).toEqual({ schemaVersion: 1, runId: "run-b", projectId: "project-b" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("11. load returns the frozen binding and never creates the namespace", async () => {
    const root = await makeRoot();
    try {
      const before = await fingerprint(root);
      const store = await ControlPlaneRunBindingStore.open(root);
      let cause: unknown;
      try {
        await store.load("absent-run");
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_not_found");
      expect(await fingerprint(root)).toBe(before);
      await store.claim({ runId: "loaded-run", projectId: "loaded-project" });
      const loaded = await store.load("loaded-run");
      expect(loaded).toEqual({ schemaVersion: 1, runId: "loaded-run", projectId: "loaded-project" });
      expect(Object.isFrozen(loaded)).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("12. load with an existing namespace but a missing file is not found", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "present-run", projectId: "present-project" });
      let cause: unknown;
      try {
        await store.load("other-run");
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_not_found");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("13. load rejects unsafe identifiers", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const bad of ["", "../escape", "a/b", 42, null]) {
        let cause: unknown;
        try {
          await store.load(bad as string);
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_invalid");
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("14. claim reads each option field exactly once and hostile extras never", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      let runIdReads = 0;
      let projectIdReads = 0;
      let extraReads = 0;
      const options = new Proxy(
        { runId: "proxy-run", projectId: "proxy-project", hostileExtra: CANARY },
        {
          get(target, property) {
            if (property === "runId") {
              runIdReads += 1;
              return "proxy-run";
            }
            if (property === "projectId") {
              projectIdReads += 1;
              return "proxy-project";
            }
            extraReads += 1;
            return (target as Record<string | symbol, unknown>)[property];
          },
        },
      );
      const claim = await store.claim(options as { runId: string; projectId: string });
      expect(claim.kind).toBe("created");
      expect(runIdReads).toBe(1);
      expect(projectIdReads).toBe(1);
      expect(extraReads).toBe(0);
      let reads = 0;
      const mutating = {
        get runId() {
          reads += 1;
          if (reads === 1) return "mutating-run";
          throw new Error("second read must never happen");
        },
        get projectId() {
          return "mutating-project";
        },
      };
      await store.claim(mutating);
      expect(await store.load("mutating-run")).toEqual({ schemaVersion: 1, runId: "mutating-run", projectId: "mutating-project" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("control plane run binding store: hostile stored objects", () => {
  const canonical = prepareRunBinding("defect-run", "defect-project").canonical_json;

  function canonicalJsonBinding(value: Record<string, unknown>): string {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${JSON.stringify(value[key])}`).join(",")}}`;
  }

  interface DefectCase {
    name: string;
    setup: (leaf: string) => Promise<void>;
  }

  const defectCases: DefectCase[] = [
    { name: "empty file", setup: async (leaf) => void (await writeFile(leaf, "", { mode: 0o600 })) },
    { name: "malformed JSON", setup: async (leaf) => void (await writeFile(leaf, "{not json", { mode: 0o600 })) },
    { name: "array document", setup: async (leaf) => void (await writeFile(leaf, "[]", { mode: 0o600 })) },
    { name: "string document", setup: async (leaf) => void (await writeFile(leaf, '"x"', { mode: 0o600 })) },
    {
      name: "extra field",
      setup: async (leaf) =>
        void (await writeFile(
          leaf,
          canonicalJsonBinding({ schema_version: 1, run_id: "defect-run", project_id: "defect-project", extra: CANARY }),
          { mode: 0o600 },
        )),
    },
    {
      name: "missing field",
      setup: async (leaf) => void (await writeFile(leaf, '{"schema_version":1,"run_id":"defect-run"}', { mode: 0o600 })),
    },
    {
      name: "wrong schema version",
      setup: async (leaf) =>
        void (await writeFile(leaf, '{"project_id":"defect-project","run_id":"defect-run","schema_version":2}', { mode: 0o600 })),
    },
    {
      name: "string schema version",
      setup: async (leaf) =>
        void (await writeFile(leaf, '{"project_id":"defect-project","run_id":"defect-run","schema_version":"1"}', { mode: 0o600 })),
    },
    {
      name: "unsafe stored id",
      setup: async (leaf) =>
        void (await writeFile(leaf, '{"project_id":"a/b","run_id":"defect-run","schema_version":1}', { mode: 0o600 })),
    },
    {
      name: "another run inside the document",
      setup: async (leaf) =>
        void (await writeFile(leaf, '{"project_id":"defect-project","run_id":"other-run","schema_version":1}', { mode: 0o600 })),
    },
    {
      name: "noncanonical key order",
      setup: async (leaf) => void (await writeFile(leaf, '{"schema_version":1,"run_id":"defect-run","project_id":"defect-project"}', { mode: 0o600 })),
    },
    {
      name: "padded canonical bytes",
      setup: async (leaf) => void (await writeFile(leaf, `${canonical} `, { mode: 0o600 })),
    },
    {
      name: "trailing newline",
      setup: async (leaf) => void (await writeFile(leaf, `${canonical}\n`, { mode: 0o600 })),
    },
    { name: "oversized file", setup: async (leaf) => void (await writeFile(leaf, "x".repeat(5000), { mode: 0o600 })) },
    { name: "symlink leaf", setup: async (leaf) => void (await symlink("/etc/hostname", leaf)) },
    { name: "directory leaf", setup: async (leaf) => void (await mkdir(leaf, { mode: 0o700 })) },
    { name: "FIFO leaf", setup: async (leaf) => void (await makeFifo(leaf)) },
    {
      name: "wrong mode file",
      setup: async (leaf) => void (await writeFile(leaf, canonical, { mode: 0o644 })),
    },
  ];

  async function prepareLeafCase(root: string, defect: DefectCase): Promise<string> {
    await mkdir(join(root, "bindings"), { mode: 0o700 });
    const leaf = join(root, "bindings", "defect-run.json");
    await defect.setup(leaf);
    return leaf;
  }

  for (const defect of defectCases) {
    test(`15. load fails closed on ${defect.name}`, async () => {
      const root = await makeRoot();
      try {
        const leaf = await prepareLeafCase(root, defect);
        const before = await lstat(leaf);
        const beforeBytes = before.isFile() ? await readFile(leaf) : null;
        const store = await ControlPlaneRunBindingStore.open(root);
        let cause: unknown;
        try {
          await store.load("defect-run");
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_invalid");
        const after = await lstat(leaf);
        expect(after.dev).toBe(before.dev);
        expect(after.ino).toBe(before.ino);
        expect((after.mode & 0o7777)).toBe((before.mode & 0o7777));
        if (beforeBytes !== null) {
          expect(await readFile(leaf)).toEqual(beforeBytes);
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }, 20000);

    test(`16. claim fails closed on ${defect.name} without overwriting the target`, async () => {
      const root = await makeRoot();
      try {
        const leaf = await prepareLeafCase(root, defect);
        const before = await lstat(leaf);
        const store = await ControlPlaneRunBindingStore.open(root);
        let cause: unknown;
        try {
          await store.claim({ runId: "defect-run", projectId: "defect-project" });
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_invalid");
        const after = await lstat(leaf);
        expect(after.dev).toBe(before.dev);
        expect(after.ino).toBe(before.ino);
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }, 20000);
  }

  test("17. a hardlink mutation that breaks the canonical bytes fails closed on load and claim", async () => {
    const root = await makeRoot();
    try {
      await mkdir(join(root, "bindings"), { mode: 0o700 });
      const leaf = join(root, "bindings", "defect-run.json");
      await writeFile(leaf, canonical, { mode: 0o600 });
      const alias = join(root, "outside.json");
      await link(leaf, alias);
      await writeFile(alias, `${canonical} `, { mode: 0o600 });
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const attempt of [
        () => store.load("defect-run"),
        () => store.claim({ runId: "defect-run", projectId: "defect-project" }),
      ]) {
        let cause: unknown;
        try {
          await attempt();
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_invalid");
      }
      expect(await readFile(leaf, "utf8")).toBe(`${canonical} `);
      await unlink(alias);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("control plane run binding store: store infrastructure", () => {
  test("18. a symlinked bindings namespace fails closed on claim and load", async () => {
    const root = await makeRoot();
    try {
      const outside = await makeRoot("cp-binding-outside-");
      await symlink(outside, join(root, "bindings"));
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const attempt of [
        () => store.claim({ runId: "ns-run", projectId: "ns-project" }),
        () => store.load("ns-run"),
      ]) {
        let cause: unknown;
        try {
          await attempt();
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_store_unavailable");
      }
      expect(await readdir(outside)).toEqual([]);
      await rm(outside, { recursive: true, force: true });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("19. a file and a FIFO at the bindings path fail closed", async () => {
    for (const makeObject of [
      async (path: string) => void (await writeFile(path, "x", { mode: 0o600 })),
      async (path: string) => void (await makeFifo(path)),
    ]) {
      const root = await makeRoot();
      try {
        await makeObject(join(root, "bindings"));
        const store = await ControlPlaneRunBindingStore.open(root);
        for (const attempt of [
          () => store.claim({ runId: "ns-run", projectId: "ns-project" }),
          () => store.load("ns-run"),
        ]) {
          let cause: unknown;
          try {
            await attempt();
          } catch (caught) {
            cause = caught;
          }
          expectBindingError(cause, "binding_store_unavailable");
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  });

  test("20. a bindings namespace with the wrong mode fails closed without repair", async () => {
    const root = await makeRoot();
    try {
      await mkdir(join(root, "bindings"), { mode: 0o755 });
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const attempt of [
        () => store.claim({ runId: "mode-run", projectId: "mode-project" }),
        () => store.load("mode-run"),
      ]) {
        let cause: unknown;
        try {
          await attempt();
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_store_unavailable");
      }
      expect(await leafMode(join(root, "bindings"))).toBe(0o755);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("21. a replaced store root fails closed on its changed identity", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "identity-run", projectId: "identity-project" });
      await rm(root, { recursive: true, force: true });
      await mkdir(root, { mode: 0o700 });
      for (const attempt of [
        () => store.claim({ runId: "identity-run", projectId: "identity-project" }),
        () => store.load("identity-run"),
      ]) {
        let cause: unknown;
        try {
          await attempt();
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_store_unavailable");
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("22. a vanished store root fails closed", async () => {
    const root = await makeRoot();
    const store = await ControlPlaneRunBindingStore.open(root);
    await rm(root, { recursive: true, force: true });
    for (const attempt of [
      () => store.claim({ runId: "gone-run", projectId: "gone-project" }),
      () => store.load("gone-run"),
    ]) {
      let cause: unknown;
      try {
        await attempt();
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_store_unavailable");
    }
  });
});

describe("control plane run binding store: durability windows", () => {
  test("23. pre-link temp faults keep nothing published and a retry creates the binding", async () => {
    const cases: [RunBindingStoreIo, string][] = [
      [ioFaulting("openTempExclusive", injectedFailure("EACCES")), "temp file could not be created"],
      [handleFaultIo({ write: injectedFailure("ENOSPC") }), "could not be written"],
      [handleFaultIo({ sync: injectedFailure("EIO") }), "could not be synced"],
      [handleFaultIo({ close: injectedFailure("EIO") }), "could not be closed"],
      [ioFaulting("link", injectedFailure("EACCES")), "could not be published"],
    ];
    for (const [io, fragment] of cases) {
      const root = await makeRoot();
      try {
        const state = await openRunBindingStoreWithIo(io, root);
        let cause: unknown;
        try {
          await claimRunBindingWithIo(io, state, { runId: "fault-run", projectId: "fault-project" });
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_persist_failed", fragment);
        expect(await readdir(join(root, "bindings"))).toEqual([]);
        const realState = await openRunBindingStoreWithIo(realRunBindingStoreIo, root);
        const retry = await claimRunBindingWithIo(realRunBindingStoreIo, realState, { runId: "fault-run", projectId: "fault-project" });
        expect(retry.kind).toBe("created");
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  });

  test("24. a failed temp removal leaves a tolerated orphan temp; the retry converges", async () => {
    const root = await makeRoot();
    try {
      const io = ioFaulting("unlink", injectedFailure("EACCES"));
      const state = await openRunBindingStoreWithIo(io, root);
      let cause: unknown;
      try {
        await claimRunBindingWithIo(io, state, { runId: "orphan-run", projectId: "orphan-project" });
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_persist_failed");
      const leaf = join(root, "bindings", "orphan-run.json");
      expect(await readFile(leaf, "utf8")).toBe(prepareRunBinding("orphan-run", "orphan-project").canonical_json);
      const entries = await readdir(join(root, "bindings"));
      expect(entries.length).toBe(2);
      expect(entries.filter((name) => name.startsWith(".")).length).toBe(1);
      const realStore = await ControlPlaneRunBindingStore.open(root);
      const retry = await realStore.claim({ runId: "orphan-run", projectId: "orphan-project" });
      expect(retry.kind).toBe("existing");
      const loaded = await realStore.load("orphan-run");
      expect(loaded).toEqual({ schemaVersion: 1, runId: "orphan-run", projectId: "orphan-project" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("25. a parent durability failure after publication keeps the binding and the retry confirms it", async () => {
    const root = await makeRoot();
    try {
      const io = claimIov2({ openDirFailOnCall: 2 });
      const state = await openRunBindingStoreWithIo(io, root);
      let cause: unknown;
      try {
        await claimRunBindingWithIo(io, state, { runId: "durable-run", projectId: "durable-project" });
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_persist_failed", "could not be synced after publication");
      const leaf = join(root, "bindings", "durable-run.json");
      const published = await lstat(leaf);
      expect(await readFile(leaf, "utf8")).toBe(prepareRunBinding("durable-run", "durable-project").canonical_json);
      const retry = await claimRunBindingWithIo(realRunBindingStoreIo, await openRunBindingStoreWithIo(realRunBindingStoreIo, root), {
        runId: "durable-run",
        projectId: "durable-project",
      });
      expect(retry.kind).toBe("existing");
      const confirmed = await lstat(leaf);
      expect(confirmed.ino).toBe(published.ino);
      expect(confirmed.mtimeMs).toBe(published.mtimeMs);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("26. an adopted equal file whose parent durability fails is retried to existing", async () => {
    const root = await makeRoot();
    try {
      const first = await ControlPlaneRunBindingStore.open(root);
      await first.claim({ runId: "adopt-run", projectId: "adopt-project" });
      const leaf = join(root, "bindings", "adopt-run.json");
      const original = await lstat(leaf);
      const io = claimIov2({ hideLeafOnce: leaf, openDirFailOnCall: 2 });
      const state = await openRunBindingStoreWithIo(io, root);
      let cause: unknown;
      try {
        await claimRunBindingWithIo(io, state, { runId: "adopt-run", projectId: "adopt-project" });
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_persist_failed");
      expect((await lstat(leaf)).ino).toBe(original.ino);
      const retry = await first.claim({ runId: "adopt-run", projectId: "adopt-project" });
      expect(retry.kind).toBe("existing");
      expect((await lstat(leaf)).ino).toBe(original.ino);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("27. a racing different-project publication through the EEXIST path is the typed conflict", async () => {
    const root = await makeRoot();
    try {
      const first = await ControlPlaneRunBindingStore.open(root);
      await first.claim({ runId: "race-run", projectId: "winner-project-one" });
      const leaf = join(root, "bindings", "race-run.json");
      const io = claimIov2({ hideLeafOnce: leaf });
      const state = await openRunBindingStoreWithIo(io, root);
      let cause: unknown;
      try {
        await claimRunBindingWithIo(io, state, { runId: "race-run", projectId: "loser-project-two" });
      } catch (caught) {
        cause = caught;
      }
      const error = expectBindingError(cause, "binding_conflict");
      expect(error.message).not.toContain("winner-project-one");
      expect(await readFile(leaf, "utf8")).toBe(prepareRunBinding("race-run", "winner-project-one").canonical_json);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("28. namespace creation failures fail closed before any binding write", async () => {
    for (const [io, reason] of [
      [ioFaulting("mkdirExclusive", injectedFailure("EACCES")), "binding_store_unavailable"],
      [ioFaulting("chmod", injectedFailure("EACCES")), "binding_store_unavailable"],
    ] as [RunBindingStoreIo, ControlPlaneRunBindingFailureReason][]) {
      const root = await makeRoot();
      try {
        const state = await openRunBindingStoreWithIo(io, root);
        let cause: unknown;
        try {
          await claimRunBindingWithIo(io, state, { runId: "ns-fault-run", projectId: "ns-fault-project" });
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, reason);
        const retry = await ControlPlaneRunBindingStore.open(root);
        const claim = await retry.claim({ runId: "ns-fault-run", projectId: "ns-fault-project" });
        expect(claim.kind).toBe("created");
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  });
});

describe("control plane run binding store: authority resolution", () => {
  function registryWithProjects(...ids: string[]): ControlPlaneProjectRegistry {
    return deepFrozenRegistry({
      schemaVersion: 1,
      projects: ids.map((id) => ({
        id,
        projectRoot: "/opt/project",
        configRoot: "/opt/config",
        pipelineRoot: null,
        launcherId: null,
        inputRoots: [],
      })),
    }) as ControlPlaneProjectRegistry;
  }

  function deepFrozenRegistry(document: unknown): unknown {
    if (Array.isArray(document)) {
      return Object.freeze(document.map((entry) => deepFrozenRegistry(entry)));
    }
    if (document !== null && typeof document === "object") {
      const copy: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(document as Record<string, unknown>)) {
        copy[key] = deepFrozenRegistry(value);
      }
      return Object.freeze(copy);
    }
    return document;
  }

  test("29. resolves the binding to its frozen registry project", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "auth-run", projectId: "project-alpha" });
      const authority = await resolveBoundControlPlaneProject(
        registryWithProjects("project-alpha", "project-beta"),
        store,
        "auth-run",
      );
      expect(authority.binding).toEqual({ schemaVersion: 1, runId: "auth-run", projectId: "project-alpha" });
      expect(authority.project.id).toBe("project-alpha");
      expect(authority.project.configRoot).toBe("/opt/config");
      expect(Object.isFrozen(authority)).toBe(true);
      expect(Object.isFrozen(authority.project)).toBe(true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("30. an unbound run stays unbound: not found, never adopted, no caller project accepted", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const attempt of [
        () => store.load("cli-run"),
        () => resolveBoundControlPlaneProject(registryWithProjects("project-alpha"), store, "cli-run"),
      ]) {
        let cause: unknown;
        try {
          await attempt();
        } catch (caught) {
          cause = caught;
        }
        expectBindingError(cause, "binding_not_found");
      }
      expect(resolveBoundControlPlaneProject.length).toBe(3);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("31. a binding whose project left the registry is project_unavailable with zero writes", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      await store.claim({ runId: "moved-run", projectId: "removed-project" });
      const before = await fingerprint(root);
      let cause: unknown;
      try {
        await resolveBoundControlPlaneProject(registryWithProjects("project-alpha"), store, "moved-run");
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "project_unavailable");
      expect(await fingerprint(root)).toBe(before);
      expect(await store.load("moved-run")).toEqual({ schemaVersion: 1, runId: "moved-run", projectId: "removed-project" });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("32. a hostile registry or store argument is refused fail-closed", async () => {
    const root = await makeRoot();
    try {
      const store = await ControlPlaneRunBindingStore.open(root);
      for (const bad of [null, 42, "x", {}, Object.freeze({ schemaVersion: 1 }), { projects: [] }]) {
        let cause: unknown;
        try {
          await resolveBoundControlPlaneProject(bad as never, store, "any-run");
        } catch (caught) {
          cause = caught;
        }
        expect(cause).toBeInstanceOf(ControlPlaneProjectRegistryError);
        expect((cause as ControlPlaneProjectRegistryError).reason).toBe("invalid_registry");
      }
      let cause2: unknown;
      try {
        await resolveBoundControlPlaneProject(registryWithProjects("project-alpha"), {} as never, "any-run");
      } catch (caught) {
        cause2 = caught;
      }
      expectBindingError(cause2, "binding_invalid");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("33. the binding store is independent of the Pipeline V2 run tree", async () => {
    const root = await makeRoot();
    try {
      const stateRoot = join(root, "state");
      await mkdir(join(stateRoot, "pipeline-runs", "ghost-run"), { recursive: true, mode: 0o700 });
      await writeFile(join(stateRoot, "pipeline-runs", "ghost-run", "state.json"), '{"revision":1}', { mode: 0o600 });
      const bindingRoot = join(root, "bindings-root");
      await mkdir(bindingRoot, { mode: 0o700 });
      const store = await ControlPlaneRunBindingStore.open(bindingRoot);
      let cause: unknown;
      try {
        await store.load("ghost-run");
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_not_found");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});

describe("control plane run binding store: real process races", () => {
  test(
    "34. same run id and project id: one publisher, identical bindings, stable inode",
    async () => {
      const root = await makeRoot();
      try {
        const results = await Promise.all([
          runChild(root, "race-same", "same-project"),
          runChild(root, "race-same", "same-project"),
          runChild(root, "race-same", "same-project"),
        ]);
        for (const result of results) {
          expect(result.ok).toBe(true);
          expect(result.binding).toEqual({ schemaVersion: 1, runId: "race-same", projectId: "same-project" });
        }
        expect(results.filter((result) => result.kind === "created").length).toBeGreaterThanOrEqual(1);
        for (const result of results) {
          if (result.kind === "existing") {
            expect(result.binding).toEqual({ schemaVersion: 1, runId: "race-same", projectId: "same-project" });
          }
        }
        const leaf = join(root, "bindings", "race-same.json");
        const winner = await lstat(leaf);
        const store = await ControlPlaneRunBindingStore.open(root);
        expect((await store.claim({ runId: "race-same", projectId: "same-project" })).kind).toBe("existing");
        expect((await lstat(leaf)).ino).toBe(winner.ino);
        expect(await readFile(leaf, "utf8")).toBe(prepareRunBinding("race-same", "same-project").canonical_json);
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
    30000,
  );

  test(
    "35. same run id, different projects: one winner, typed conflicts, owner never revealed",
    async () => {
      const root = await makeRoot();
      try {
        const results = await Promise.all([
          runChild(root, "race-diff", "candidate-project-one"),
          runChild(root, "race-diff", "candidate-project-two"),
          runChild(root, "race-diff", "candidate-project-one"),
          runChild(root, "race-diff", "candidate-project-two"),
        ]);
        const winners = results.filter((result) => result.ok);
        expect(winners.length).toBeGreaterThanOrEqual(1);
        const firstWinner = winners[0];
        const winnerProject = firstWinner === undefined ? undefined : firstWinner.binding?.projectId;
        if (winnerProject === undefined) throw new Error("unreachable");
        expect(["candidate-project-one", "candidate-project-two"]).toContain(winnerProject);
        for (const winner of winners) {
          expect(winner.binding).toEqual({ schemaVersion: 1, runId: "race-diff", projectId: winnerProject });
        }
        for (const loser of results.filter((result) => !result.ok)) {
          expect(loser.reason).toBe("binding_conflict");
          expect(loser.message ?? "").not.toContain("candidate-project-one");
          expect(loser.message ?? "").not.toContain("candidate-project-two");
        }
        const leaf = join(root, "bindings", "race-diff.json");
        const stored = JSON.parse(await readFile(leaf, "utf8")) as { project_id: string };
        expect(stored.project_id).toBe(winnerProject);
        const stable = await lstat(leaf);
        const losingProject = winnerProject === "candidate-project-one" ? "candidate-project-two" : "candidate-project-one";
        const late = await runChild(root, "race-diff", losingProject);
        expect(late.ok).toBe(false);
        expect(late.reason).toBe("binding_conflict");
        expect((await lstat(leaf)).ino).toBe(stable.ino);
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
    30000,
  );

  test(
    "36. different run ids proceed in parallel",
    async () => {
      const root = await makeRoot();
      try {
        const results = await Promise.all([
          runChild(root, "parallel-a", "project-a"),
          runChild(root, "parallel-b", "project-b"),
        ]);
        for (const [index, result] of results.entries()) {
          expect(result.ok).toBe(true);
          expect(result.kind).toBe("created");
          expect(result.binding?.runId).toBe(index === 0 ? "parallel-a" : "parallel-b");
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
    30000,
  );

  test(
    "37. a killed claiming process converges on retry without duplicates",
    async () => {
      const root = await makeRoot();
      try {
        const proc = Bun.spawn(
          [process.execPath, join(import.meta.dir, "control_plane_run_binding_claim_child.ts"), root, "killed-run", "killed-project"],
          { stdout: "pipe", stderr: "pipe" },
        );
        await Bun.sleep(20);
        try {
          proc.kill("SIGKILL");
        } catch {
          // the child may already be gone; either way the retry converges
        }
        await proc.exited;
        const store = await ControlPlaneRunBindingStore.open(root);
        const retry = await store.claim({ runId: "killed-run", projectId: "killed-project" });
        expect(retry.kind === "created" || retry.kind === "existing").toBe(true);
        expect(retry.binding).toEqual({ schemaVersion: 1, runId: "killed-run", projectId: "killed-project" });
        const entries = (await readdir(join(root, "bindings"))).filter((name) => !name.startsWith("."));
        expect(entries).toEqual(["killed-run.json"]);
        expect(await store.load("killed-run")).toEqual(retry.binding);
        expect(await readFile(join(root, "bindings", "killed-run.json"), "utf8")).toBe(
          prepareRunBinding("killed-run", "killed-project").canonical_json,
        );
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    },
    30000,
  );
});

describe("control plane run binding store: real composition smoke", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), "cp-bind-smoke-"));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const configRoot = join(import.meta.dir, "..", "fixtures", "control-plane", "config");
  const pipelineRoot = join(import.meta.dir, "..", "fixtures", "control-plane", "bundle");
  const inputRoot = join(smokeRoot, "inputs");
  const taskFile = join(inputRoot, "task.md");
  const registryPath = join(smokeRoot, "registry.json");
  const bindingStoreRoot = join(smokeRoot, "binding-store");
  const runId = `cbss-${Math.random().toString(36).slice(2, 8)}`;
  let registry: ControlPlaneProjectRegistry;
  let daemonPid = 0;

  const start = async (): Promise<void> => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "smoke project\n", { mode: 0o600 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(taskFile, "SMOKE-TASK\n", { mode: 0o600 });
    mkdirSync(bindingStoreRoot, { mode: 0o700 });
    const document = {
      schema_version: 1,
      projects: [
        {
          id: "bound-project-a",
          project_root: projectDir,
          config_root: configRoot,
          pipeline_root: pipelineRoot,
          launcher_id: null,
          input_roots: [{ id: "tasks", root: inputRoot }],
        },
        {
          id: "bound-project-b",
          project_root: projectDir,
          config_root: configRoot,
          pipeline_root: pipelineRoot,
          launcher_id: null,
          input_roots: [{ id: "tasks", root: inputRoot }],
        },
      ],
    };
    writeFileSync(registryPath, JSON.stringify(document), { mode: 0o600 });
    const daemonLog = openSync(join(smokeRoot, "daemon.log"), "a");
    const daemon = spawn(process.execPath, [join(import.meta.dir, "control_plane_daemon.ts"), socketPath], {
      stdio: ["ignore", daemonLog, daemonLog],
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("smoke daemon socket never appeared");
    registry = await loadControlPlaneProjectRegistry(registryPath);
  };

  function makeClient(configRootOverride: string): OrchestratorSubprocessClient {
    return new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
      workingDirectory: smokeRoot,
      configRoot: configRootOverride,
      pipelineRoot,
      baseEnv: {
        PATH: "/usr/bin:/bin",
        HOME: smokeRoot,
        XDG_CONFIG_HOME: credDir,
        ORCHESTRATOR_STATE_ROOT: stateRoot,
        DOCKER_HELPER_SOCKET_PATH: socketPath,
        ARCHITECT_SOURCE_VAR_1: "smoke-a",
        CODER_SOURCE_VAR_1: "smoke-c",
      },
      stdoutLimitBytes: 4_000_000,
      stderrLimitBytes: 4_000_000,
      sigintGraceMs: 3000,
    });
  }

  afterAll(() => {
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test(
    "38. registry -> resolve -> claim -> real run -> fresh authority -> inspect, with a conflicting project refused",
    async () => {
      await start();
      const resolved = await resolveControlPlaneRunCommand(registry, {
        runId,
        projectId: "bound-project-a",
        inputs: [{ id: "task", rootId: "tasks", ref: "task.md" }],
      });
      expect(resolved.command.kind).toBe("run");
      const store = await ControlPlaneRunBindingStore.open(bindingStoreRoot);
      const claim = await store.claim({ runId, projectId: "bound-project-a" });
      expect(claim.kind).toBe("created");
      expect(claim.binding.projectId).toBe("bound-project-a");
      const client = makeClient(resolved.routingConfig.configRoot);
      const runResult = await client.run(resolved.command);
      expect(runResult.ok).toBe(true);
      if (!runResult.ok) throw new Error("unreachable");
      const projection = runResult.projection as { kind: string; runId: string };
      expect(projection.kind).toBe("planReady");
      expect(projection.runId).toBe(runId);

      const before = fingerprintSync(bindingStoreRoot) + "|" + fingerprintSync(join(stateRoot, "pipeline-runs", runId));

      const freshStore = await ControlPlaneRunBindingStore.open(bindingStoreRoot);
      const loaded = await freshStore.load(runId);
      expect(loaded).toEqual({ schemaVersion: 1, runId, projectId: "bound-project-a" });
      const authority = await resolveBoundControlPlaneProject(registry, freshStore, runId);
      expect(authority.project.id).toBe("bound-project-a");
      expect(authority.project.configRoot).toBe(resolved.routingConfig.configRoot);
      expect(authority.project.pipelineRoot).toBe(pipelineRoot);

      const authorityAgain = await resolveBoundControlPlaneProject(registry, freshStore, runId);
      expect(authorityAgain.project.id).toBe("bound-project-a");

      const boundClient = makeClient(authority.project.configRoot);
      const inspectResult = await boundClient.run({ kind: "inspect", runId });
      expect(inspectResult.ok).toBe(true);
      if (!inspectResult.ok) throw new Error("unreachable");
      expect(inspectResult.projection.kind).toBe("inspect");
      if (inspectResult.projection.kind !== "inspect") throw new Error("unreachable");
      expect(inspectResult.projection.schemaVersion).toBe(1);
      expect(inspectResult.projection.status).toBe("running");
      expect(inspectResult.projection.boundary.kind).toBe("plan_ready");

      const after = fingerprintSync(bindingStoreRoot) + "|" + fingerprintSync(join(stateRoot, "pipeline-runs", runId));
      expect(after).toBe(before);
    },
    120000,
  );

  test(
    "39. a lost run result is recovered through the binding and an unbound run stays closed",
    async () => {
      const inspect = makeClient(join(import.meta.dir, "..", "fixtures", "control-plane", "config"));
      const lostRecovery = await inspect.run({ kind: "inspect", runId });
      expect(lostRecovery.ok).toBe(true);
      if (!lostRecovery.ok) throw new Error("unreachable");
      expect(lostRecovery.projection.kind).toBe("inspect");

      const store = await ControlPlaneRunBindingStore.open(bindingStoreRoot);
      let cause: unknown;
      try {
        await store.load("never-claimed-run");
      } catch (caught) {
        cause = caught;
      }
      expectBindingError(cause, "binding_not_found");
      const rebound = await store
        .claim({ runId, projectId: "bound-project-b" })
        .then(() => null)
        .catch((caught: unknown) => caught);
      expect((rebound as ControlPlaneRunBindingError).reason).toBe("binding_conflict");
    },
    60000,
  );
});

describe("control plane run binding store: source restrictions", () => {
  const publicSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "run_binding_store.ts"), "utf8");
  const internalSource = readFileSync(
    join(import.meta.dir, "..", "src", "control_plane", "run_binding_store_internal.ts"),
    "utf8",
  );
  const combined = `${publicSource}\n${internalSource}`;

  test("40. no spawn, no server, no discovery, no deletion, no run-state access", () => {
    for (const forbidden of [
      "Bun.spawn",
      "Bun.serve",
      "fetch(",
      "readdir",
      "unlink",
      "rmSync",
      "state.json",
      "pipeline-runs",
      "docker",
      "Docker",
      "watch",
      "process.pid",
      "mtime",
      "TTL",
    ]) {
      expect(combined).not.toContain(forbidden);
    }
  });

  test("41. one binding serializer/parser owner and no second publication implementation", () => {
    expect(combined.split("canonicalJson(").length - 1).toBe(1);
    expect(publicSource).not.toContain("canonicalJson");
    expect(publicSource).not.toContain("node:fs");
    expect(internalSource).not.toContain("O_EXCL");
    expect(internalSource).not.toContain("openSync");
    expect(internalSource).not.toContain("writeSync");
    expect(internalSource).not.toContain("rename");
    expect(internalSource.split("publishImmutableDocumentFile(").length - 1).toBe(1);
    expect(internalSource.split("ensureImmutableDirectory(").length - 1).toBe(1);
    expect(internalSource.split("readStoredImmutableDocument(").length - 1).toBe(1);
    expect(combined).not.toContain("new Map(");
    expect(combined).not.toContain("new Set(");
    expect(combined).not.toContain("A-Za-z0-9");
  });

  test("42. the shared predicates and the registry error are reused; no orchestration run-state imports", () => {
    expect(combined).toContain('from "../pipeline_v2_scalar.ts"');
    expect(combined).toContain('from "./project_registry.ts"');
    for (const forbidden of [
      'from "../pipeline_v2_state',
      'from "../pipeline_state_store',
      'from "../pipeline_v2_coordinator',
      'from "../pipeline_v2_runner',
      'from "../pipeline_runner',
      'from "../run_snapshot_store',
    ]) {
      expect(combined).not.toContain(forbidden);
    }
    expect(combined).not.toContain('"worker_failed"');
    expect(combined).not.toContain('"run_busy"');
    expect(combined.split("JSON.parse(").length - 1).toBe(1);
  });

  test("43. the export surfaces carry exactly the contract", async () => {
    const publicModule = await import("../src/control_plane/run_binding_store.ts");
    expect(Object.keys(publicModule).sort()).toEqual([
      "ControlPlaneRunBindingError",
      "ControlPlaneRunBindingStore",
      "resolveBoundControlPlaneProject",
    ]);
    const internalModule = await import("../src/control_plane/run_binding_store_internal.ts");
    expect(Object.keys(internalModule).sort()).toEqual([
      "ControlPlaneRunBindingError",
      "claimRunBindingWithIo",
      "loadRunBindingWithIo",
      "openRunBindingStoreWithIo",
      "parseRunBindingDocument",
      "prepareRunBinding",
      "realRunBindingStoreIo",
    ]);
  });
});
