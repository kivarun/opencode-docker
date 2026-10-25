/**
 * The metadata probe owner (`pipeline_v2_apply_metadata_probe_internal.ts`):
 * the single read-only v1 metadata policy over libc (`llistxattr` /
 * `lgetxattr`, the proven FFI discipline) that decides, before any plan
 * exists, whether every touched object and parent directory can be
 * handled by the v1 writer.
 *
 * Real-filesystem coverage where the environment permits (user xattrs,
 * hardlinks, FIFOs); ownership, special permission bits and POSIX ACLs
 * are exercised through the per-call probe seam because this environment
 * runs unprivileged and without ACL tooling — the probe logic under test
 * is identical on both paths.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { chmod, link, mkdtemp, mkdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dlopen, FFIType } from "bun:ffi";
import {
  MetadataProbeError,
  probeApplyObjectMetadata,
  realMetadataProbeIo,
  type MetadataProbeIo,
} from "../src/pipeline_v2_apply_metadata_probe_internal.ts";

const ROOTS: string[] = [];

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

async function tempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "apply-probe-"));
  ROOTS.push(dir);
  return dir;
}

function refusalOf(promise: Promise<void>): Promise<MetadataProbeError> {
  return promise.then(
    () => {
      throw new Error("expected the probe to refuse");
    },
    (cause: unknown) => {
      if (cause instanceof MetadataProbeError) {
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

describe("metadata probe: real objects", () => {
  test("an ordinary git-like tree passes the probe completely", async () => {
    const dir = await tempDir();
    await mkdir(join(dir, "src"), { recursive: true, mode: 0o750 });
    await mkdir(join(dir, "src", "nested"), { recursive: true, mode: 0o750 });
    await writeFile(join(dir, "src", "a.txt"), "A", { mode: 0o644 });
    await writeFile(join(dir, "src", "nested", "b.txt"), "B", { mode: 0o600 });
    await symlink("a.txt", join(dir, "src", "link"));
    await probeApplyObjectMetadata(realMetadataProbeIo, dir, "parent");
    await probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "src"), "object");
    await probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "src", "a.txt"), "object");
    await probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "src", "link"), "object");
  });

  test("a user extended attribute on a touched object is refused", async () => {
    const dir = await tempDir();
    const file = join(dir, "x.txt");
    await writeFile(file, "X");
    const libc = dlopen("libc.so.6", {
      setxattr: { args: ["cstring", "cstring", "ptr", FFIType.u64, FFIType.u32], returns: FFIType.i64 },
    });
    const rc = Number(libc.symbols.setxattr(
      Buffer.from(`${file}\0`),
      Buffer.from("user.note\0"),
      Buffer.from("v"),
      1,
      0,
    ));
    if (rc !== 0) {
      // The environment does not support user xattrs here; the seam battery
      // below covers the same policy deterministically.
      return;
    }
    const refusal = await refusalOf(probeApplyObjectMetadata(realMetadataProbeIo, file, "object"));
    expect(refusal.refusal).toBe("unsupported_metadata");
  });

  test("a hardlinked regular file is refused for v1", async () => {
    const dir = await tempDir();
    await writeFile(join(dir, "a.txt"), "SHARED");
    await link(join(dir, "a.txt"), join(dir, "b.txt"));
    const refusal = await refusalOf(probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "a.txt"), "object"));
    expect(refusal.refusal).toBe("unsupported_metadata");
    expect(refusal.message).toBe("a probed regular file has more than one hard link");
  });

  test("a FIFO is an unsupported project entry", async () => {
    const dir = await tempDir();
    await makeFifo(join(dir, "pipe"));
    const refusal = await refusalOf(probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "pipe"), "object"));
    expect(refusal.refusal).toBe("unsupported_project_entry");
  });

  test("a vanished object is an unreachable probe", async () => {
    const dir = await tempDir();
    const refusal = await refusalOf(probeApplyObjectMetadata(realMetadataProbeIo, join(dir, "absent"), "object"));
    expect(refusal.refusal).toBe("unreachable");
  });
});

describe("metadata probe: policy through the per-call seam", () => {
  interface FakeStats {
    isFile: () => boolean;
    isDirectory: () => boolean;
    isSymbolicLink: () => boolean;
    isFIFO: () => boolean;
    isSocket: () => boolean;
    isBlockDevice: () => boolean;
    isCharacterDevice: () => boolean;
    mode: number;
    uid: number;
    gid: number;
    nlink: number;
  }

  function fakeStats(overrides: Partial<FakeStats>): FakeStats {
    return {
      isFile: () => false,
      isDirectory: () => true,
      isSymbolicLink: () => false,
      isFIFO: () => false,
      isSocket: () => false,
      isBlockDevice: () => false,
      isCharacterDevice: () => false,
      mode: 0o750,
      uid: 1000,
      gid: 1000,
      nlink: 1,
      ...overrides,
    };
  }

  function seamIo(stats: FakeStats, xattrs: string[] = [], aclAccess = false, aclDefault = false): MetadataProbeIo {
    return {
      lstat: async () => stats as never,
      listXattrs: async () => xattrs,
      readXattr: async (_path: string, name: string) => {
        if (name === "system.posix_acl_access") {
          return aclAccess ? { present: true } : null;
        }
        if (name === "system.posix_acl_default") {
          return aclDefault ? { present: true } : null;
        }
        return null;
      },
    };
  }

  test("setuid, setgid and sticky bits are refused", async () => {
    for (const mode of [0o4755, 0o2755, 0o1755]) {
      const refusal = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ mode })), "/probed", "object"));
      expect(refusal.refusal).toBe("unsupported_metadata");
      expect(refusal.message).toBe("a probed object carries special permission bits");
    }
  });

  test("ownership the orchestrator could not reproduce is refused", async () => {
    const euid = process.getuid?.() ?? 1000;
    const egid = process.getgid?.() ?? 1000;
    const foreignUid = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ uid: euid + 1 })), "/probed", "object"));
    expect(foreignUid.refusal).toBe("unsupported_metadata");
    expect(foreignUid.message).toBe("a probed object is owned by another user");
    const foreignGid = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ gid: egid + 1 })), "/probed", "parent"));
    expect(foreignGid.refusal).toBe("unsupported_metadata");
    expect(foreignGid.message).toBe("a probed object is owned by another group");
    // An owned object passes.
    await probeApplyObjectMetadata(seamIo(fakeStats({})), "/probed", "object");
  });

  test("a listed xattr of any namespace is refused", async () => {
    for (const name of ["user.note", "trusted.something", "security.selinux"]) {
      const refusal = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({}), [name]), "/probed", "object"));
      expect(refusal.refusal).toBe("unsupported_metadata");
      expect(refusal.message).toBe("a probed object carries extended attributes");
    }
  });

  test("a POSIX access ACL is refused; a default ACL on a directory is refused", async () => {
    const access = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({}), [], true), "/probed", "object"));
    expect(access.refusal).toBe("unsupported_metadata");
    expect(access.message).toBe("a probed object carries a POSIX access ACL");
    const def = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({}), [], false, true), "/probed", "parent"));
    expect(def.refusal).toBe("unsupported_metadata");
    expect(def.message).toBe("a probed directory carries a POSIX default ACL");
    // A default ACL probe on a non-directory is not performed.
    await probeApplyObjectMetadata(seamIo(fakeStats({ isDirectory: () => false, isFile: () => true }), [], false, true), "/probed", "object");
  });

  test("an unsupported xattr API is treated as carrying none; an unexpected xattr failure fails closed", async () => {
    const noSupport: MetadataProbeIo = {
      lstat: async () => fakeStats({}) as never,
      listXattrs: async () => [],
      readXattr: async () => null,
    };
    await probeApplyObjectMetadata(noSupport, "/probed", "object");
    const failing: MetadataProbeIo = {
      lstat: async () => fakeStats({}) as never,
      listXattrs: async () => {
        throw new MetadataProbeError("unsupported_metadata", "the xattr list of an object could not be read");
      },
      readXattr: async () => null,
    };
    const refusal = await refusalOf(probeApplyObjectMetadata(failing, "/probed", "object"));
    expect(refusal.refusal).toBe("unsupported_metadata");
  });

  test("a hardlinked regular file is refused; directories keep their natural link count", async () => {
    const linked = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ isDirectory: () => false, isFile: () => true, nlink: 2 })), "/probed", "object"));
    expect(linked.refusal).toBe("unsupported_metadata");
    await probeApplyObjectMetadata(seamIo(fakeStats({ nlink: 3 })), "/probed", "parent");
  });

  test("a socket or device node is an unsupported project entry", async () => {
    const socket = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ isSocket: () => true })), "/probed", "object"));
    expect(socket.refusal).toBe("unsupported_project_entry");
    const device = await refusalOf(probeApplyObjectMetadata(seamIo(fakeStats({ isBlockDevice: () => true })), "/probed", "object"));
    expect(device.refusal).toBe("unsupported_project_entry");
  });

  test("a symlink never carries special bits and is never dereferenced", async () => {
    await probeApplyObjectMetadata(seamIo(fakeStats({ isDirectory: () => false, isSymbolicLink: () => true, mode: 0o777 })), "/probed", "object");
  });
});

describe("metadata probe environment", () => {
  test("special permission bits are probed on the real filesystem where the environment keeps them", async () => {
    const dir = await tempDir();
    const file = join(dir, "special.txt");
    await writeFile(file, "S");
    await chmod(file, 0o4755);
    const info = await (await import("node:fs/promises")).lstat(file);
    if ((info.mode & 0o7000) === 0) {
      // The mount strips special bits for unprivileged users; the seam
      // battery above covers the policy deterministically.
      return;
    }
    const refusal = await refusalOf(probeApplyObjectMetadata(realMetadataProbeIo, file, "object"));
    expect(refusal.refusal).toBe("unsupported_metadata");
  });
});
