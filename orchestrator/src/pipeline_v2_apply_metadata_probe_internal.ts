import { type Stats } from "node:fs";
import { dlopen, FFIType } from "bun:ffi";
import { isErrnoException } from "./fs_checks.ts";

/**
 * The single metadata probe owner of the project apply preparation: one
 * read-only probe over libc (the same `dlopen` + `__errno_location` +
 * `memcpy` discipline the run mutation lock proves) that decides whether
 * every object the future Apply touches can be handled by the v1 writer
 * policy. The probe reads only — `lstat`, `llistxattr`, `lgetxattr` — and
 * never writes, chmods or repairs anything.
 *
 * v1 metadata policy (fail closed):
 *   - setuid/setgid/sticky bits on any touched object or parent
 *     directory → `unsupported_metadata` (the observation masks them,
 *     the probe reads the full mode);
 *   - ownership the orchestrator could not reproduce (uid != euid; gid
 *     neither the effective gid nor a supplementary group) on a touched
 *     object or parent directory → `unsupported_metadata`;
 *   - more than one hard link on a touched regular file →
 *     `unsupported_metadata` (hardlink recreation is out of v1 scope);
 *   - any listed xattr (user/trusted/security/…) on a touched object or
 *     parent directory → `unsupported_metadata` (v1 preserves none);
 *   - a present POSIX ACL (`system.posix_acl_access`, and
 *     `system.posix_acl_default` on directories) → `unsupported_metadata`;
 *     a filesystem that reports xattr APIs as unsupported is treated as
 *     carrying none (there is nothing to preserve);
 *   - an xattr read that fails with anything other than absence or
 *     unsupported → `unsupported_metadata` (fail closed);
 *   - FIFO/socket/device objects → `unsupported_project_entry`.
 *
 * Directories keep their natural `nlink` (2 + subdirectories); only
 * regular files are hardlink-checked. Symlinks are probed through the
 * `l*` variants and never dereferenced; their own mode is fixed 0777 by
 * the kernel and carries no special bits.
 */

export type MetadataProbeRefusal = "unsupported_metadata" | "unsupported_project_entry" | "unreachable";

/** The typed refusal of one probed object; the diagnostic is content-free. */
export class MetadataProbeError extends Error {
  declare readonly refusal: MetadataProbeRefusal;

  constructor(refusal: MetadataProbeRefusal, message: string) {
    super(message);
    this.name = "MetadataProbeError";
    Object.defineProperty(this, "refusal", {
      value: refusal,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

export interface MetadataProbeXattrRead {
  /** true when the named xattr exists; false for ENODATA/ENOTSUP/ENOSYS. */
  readonly present: boolean;
}

export interface MetadataProbeIo {
  /** lstat; rejects when the path is missing or unreadable. */
  readonly lstat: (path: string) => Promise<Stats>;
  /** `llistxattr` names; empty when the filesystem reports no support. */
  readonly listXattrs: (path: string) => Promise<string[]>;
  /** `lgetxattr` presence probe; null means the attribute is absent. */
  readonly readXattr: (path: string, name: string) => Promise<MetadataProbeXattrRead | null>;
}

const MAX_XATTR_LIST_BYTES = 64 * 1024;
const MAX_XATTR_VALUE_BYTES = 64 * 1024;

interface LibcMetadataBinding {
  llistxattr(path: Uint8Array, buffer: Uint8Array, size: number): bigint;
  lgetxattr(path: Uint8Array, name: Uint8Array, buffer: Uint8Array, size: number): bigint;
  errnoLocation(): unknown;
  memcpy(destination: unknown, source: unknown, length: number): unknown;
}

let libcBinding: LibcMetadataBinding | null = null;

function metadataLibc(): LibcMetadataBinding {
  if (libcBinding !== null) {
    return libcBinding;
  }
  const symbols = {
    llistxattr: { args: ["cstring", "ptr", FFIType.u64], returns: FFIType.i64 },
    lgetxattr: { args: ["cstring", "cstring", "ptr", FFIType.u64], returns: FFIType.i64 },
    __errno_location: { args: [], returns: FFIType.ptr },
    memcpy: { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.ptr },
  } as const;
  const muslArch = process.arch === "arm64" ? "aarch64" : "x86_64";
  const candidates = [
    "libc.so.6",
    "libc.so",
    `/lib/ld-musl-${muslArch}.so.1`,
    `/lib/libc.musl-${muslArch}.so.1`,
  ];
  let lastFailure: unknown = null;
  for (const candidate of candidates) {
    try {
      const library = dlopen(candidate, symbols);
      const record = library.symbols as unknown as Record<string, unknown>;
      const llistxattr = record["llistxattr"];
      const lgetxattr = record["lgetxattr"];
      const errnoLocation = record["__errno_location"];
      const memcpy = record["memcpy"];
      if (
        typeof llistxattr !== "function" ||
        typeof lgetxattr !== "function" ||
        typeof errnoLocation !== "function" ||
        typeof memcpy !== "function"
      ) {
        throw new Error(`libc candidate ${candidate} is missing the required symbols`);
      }
      libcBinding = {
        llistxattr: llistxattr as LibcMetadataBinding["llistxattr"],
        lgetxattr: lgetxattr as LibcMetadataBinding["lgetxattr"],
        errnoLocation: errnoLocation as LibcMetadataBinding["errnoLocation"],
        memcpy: memcpy as LibcMetadataBinding["memcpy"],
      };
      return libcBinding;
    } catch (cause) {
      lastFailure = cause;
    }
  }
  const detail = lastFailure instanceof Error ? lastFailure.message : String(lastFailure ?? "");
  throw new MetadataProbeError("unsupported_metadata", `the metadata probe could not bind libc (${detail})`);
}

const errnoBuffer = new Uint8Array(4);

function currentErrno(): number {
  const binding = libcBinding!;
  binding.memcpy(errnoBuffer as unknown as Uint8Array, binding.errnoLocation() as never, 4);
  return new DataView(errnoBuffer.buffer).getUint32(0, true);
}

const posixAclAccessXattr = "system.posix_acl_access";
const posixAclDefaultXattr = "system.posix_acl_default";

class RealMetadataProbeIo implements MetadataProbeIo {
  async lstat(path: string): Promise<Stats> {
    const { lstat } = await import("node:fs/promises");
    return await lstat(path);
  }

  async listXattrs(path: string): Promise<string[]> {
    const binding = metadataLibc();
    const size = Number(binding.llistxattr(Buffer.from(`${path}\0`, "utf8"), Buffer.alloc(0), 0));
    if (size < 0) {
      const errno = currentErrno();
      if (errno === 61 /* ENODATA */ || errno === 95 /* EOPNOTSUPP */ || errno === 38 /* ENOSYS */ || errno === 2 /* ENOENT */) {
        return [];
      }
      throw new MetadataProbeError("unsupported_metadata", "the xattr list of an object could not be read");
    }
    if (size === 0) {
      return [];
    }
    if (size > MAX_XATTR_LIST_BYTES) {
      throw new MetadataProbeError("unsupported_metadata", "the xattr list of an object is unexpectedly large");
    }
    const buffer = Buffer.alloc(size);
    const read = Number(binding.llistxattr(Buffer.from(`${path}\0`, "utf8"), buffer as unknown as Uint8Array, size));
    if (read < 0) {
      throw new MetadataProbeError("unsupported_metadata", "the xattr list of an object could not be read");
    }
    const names: string[] = [];
    let start = 0;
    for (let index = 0; index < read; index += 1) {
      if (buffer[index] === 0) {
        names.push(buffer.subarray(start, index).toString("utf8"));
        start = index + 1;
      }
    }
    return names;
  }

  async readXattr(path: string, name: string): Promise<MetadataProbeXattrRead | null> {
    const binding = metadataLibc();
    const size = Number(binding.lgetxattr(
      Buffer.from(`${path}\0`, "utf8"),
      Buffer.from(`${name}\0`, "utf8"),
      Buffer.alloc(0),
      0,
    ));
    if (size < 0) {
      const errno = currentErrno();
      if (errno === 61 /* ENODATA */ || errno === 95 /* EOPNOTSUPP */ || errno === 38 /* ENOSYS */) {
        return null;
      }
      if (errno === 13 /* EACCES */ || errno === 1 /* EPERM */) {
        throw new MetadataProbeError("unsupported_metadata", "an extended attribute could not be inspected");
      }
      if (errno === 2 /* ENOENT */) {
        throw new MetadataProbeError("unreachable", "an object disappeared during the metadata probe");
      }
      throw new MetadataProbeError("unsupported_metadata", "an extended attribute could not be inspected");
    }
    if (size > MAX_XATTR_VALUE_BYTES) {
      throw new MetadataProbeError("unsupported_metadata", "an extended attribute is unexpectedly large");
    }
    return { present: true };
  }
}

/** The fixed real probe IO; tests inject their own object per call. */
export const realMetadataProbeIo: MetadataProbeIo = new RealMetadataProbeIo();

export type MetadataProbeRole = "object" | "parent";

export interface MetadataProbeStats {
  readonly isFile: boolean;
  readonly isDirectory: boolean;
  readonly isSymbolicLink: boolean;
  readonly isFIFO: boolean;
  readonly isSocket: boolean;
  readonly isBlockDevice: boolean;
  readonly isCharacterDevice: boolean;
  readonly mode: number;
  readonly uid: number;
  readonly gid: number;
  readonly nlink: number;
}

/**
 * Probes one object for the v1 metadata policy. `role` "object" is a
 * touched object (replaced, removed or created-over); "parent" is an
 * existing directory the writer must write into (including the project
 * root). The probe is strictly read-only.
 */
export async function probeApplyObjectMetadata(
  io: MetadataProbeIo,
  path: string,
  role: MetadataProbeRole,
): Promise<void> {
  let info: MetadataProbeStats;
  try {
    const stats = await io.lstat(path);
    info = {
      isFile: stats.isFile(),
      isDirectory: stats.isDirectory(),
      isSymbolicLink: stats.isSymbolicLink(),
      isFIFO: stats.isFIFO(),
      isSocket: stats.isSocket(),
      isBlockDevice: stats.isBlockDevice(),
      isCharacterDevice: stats.isCharacterDevice(),
      mode: stats.mode,
      uid: stats.uid,
      gid: stats.gid,
      nlink: stats.nlink,
    };
  } catch (cause) {
    if (cause instanceof MetadataProbeError) {
      throw cause;
    }
    if (isErrnoException(cause, "ENOENT")) {
      throw new MetadataProbeError("unreachable", "a probed object disappeared before it could be inspected");
    }
    throw new MetadataProbeError("unsupported_metadata", "a probed object could not be inspected");
  }
  if (info.isFIFO || info.isSocket || info.isBlockDevice || info.isCharacterDevice) {
    throw new MetadataProbeError("unsupported_project_entry", "a probed object is an unsupported filesystem object kind");
  }
  const euid = process.getuid?.() ?? -1;
  const egid = process.getgid?.() ?? -1;
  const groups = process.getgroups?.() ?? [];
  if (info.uid !== euid) {
    throw new MetadataProbeError("unsupported_metadata", "a probed object is owned by another user");
  }
  if (info.gid !== egid && !groups.includes(info.gid)) {
    throw new MetadataProbeError("unsupported_metadata", "a probed object is owned by another group");
  }
  if (!info.isSymbolicLink) {
    const specialBits = info.mode & 0o7000;
    if (specialBits !== 0) {
      throw new MetadataProbeError("unsupported_metadata", "a probed object carries special permission bits");
    }
  }
  if (role === "object" && info.isFile && info.nlink > 1) {
    throw new MetadataProbeError("unsupported_metadata", "a probed regular file has more than one hard link");
  }
  const names = await io.listXattrs(path);
  if (names.length > 0) {
    throw new MetadataProbeError("unsupported_metadata", "a probed object carries extended attributes");
  }
  const accessAcl = await io.readXattr(path, posixAclAccessXattr);
  if (accessAcl !== null && accessAcl.present) {
    throw new MetadataProbeError("unsupported_metadata", "a probed object carries a POSIX access ACL");
  }
  if (info.isDirectory) {
    const defaultAcl = await io.readXattr(path, posixAclDefaultXattr);
    if (defaultAcl !== null && defaultAcl.present) {
      throw new MetadataProbeError("unsupported_metadata", "a probed directory carries a POSIX default ACL");
    }
  }
}
