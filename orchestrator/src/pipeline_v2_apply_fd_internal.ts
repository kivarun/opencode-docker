import { type Stats } from "node:fs";
import * as fs from "node:fs";
import { dlopen, FFIType } from "bun:ffi";

/**
 * The single owner of the fd-relative syscall adapter of the project
 * apply filesystem writer: one frozen libc binding (the same `dlopen` +
 * `__errno_location` + `memcpy` discipline the run mutation lock and the
 * metadata probe prove) exposing exactly the operations the writer needs
 * — `openat`, `mkdirat`, `unlinkat`, `symlinkat`, `renameat2`,
 * `readlinkat`, `flistxattr`, `fstatvfs`, `faccessat` and the raw
 * `getdents64` syscall — plus the fd-based Node primitives (fstat, read,
 * write, fchmod, fsync, close) the writer drives over raw descriptors.
 *
 * Every path argument is a caller-built NUL-terminated buffer containing
 * exactly one safe relative path component or leaf name; the adapter
 * never assembles, normalizes or echoes paths. Every call returns the raw
 * libc result (-1 plus the thread errno on failure); interpretation and
 * classification belong to the callers (the verify and executor owners),
 * never to this module. There is no mutable test seam: tests inject a
 * full replacement `ApplyFdOps` object per call.
 *
 * Object metadata is always read through `fstat` on a held descriptor —
 * regular files and directories via `O_RDONLY` opens, symlinks via an
 * `O_PATH | O_NOFOLLOW` open — so no `struct stat` layout is ever parsed
 * from raw buffers. `fstatvfs`'s `f_flag` sits at offset 72 (u64) in both
 * glibc and musl on x86_64 and aarch64. The `getdents64` raw syscall
 * number is the x86_64 value (217) or the asm-generic value (61) used by
 * aarch64/riscv64.
 */

export type ApplyFdFailureReason = "libc_unavailable";

export class ApplyFdError extends Error {
  declare readonly reason: ApplyFdFailureReason;

  constructor(reason: ApplyFdFailureReason, message: string) {
    super(message);
    this.name = "ApplyFdError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** Linux `open(2)` flag values used by the writer (stable ABI). */
export const FD_OPEN_RDONLY = 0;
export const FD_OPEN_WRONLY = 1;
export const FD_OPEN_RDWR = 2;
export const FD_OPEN_CLOEXEC = 0o2000000;
export const FD_OPEN_CREAT = 0o100;
export const FD_OPEN_EXCL = 0o200;
export const FD_OPEN_NONBLOCK = 0o4000;
export const FD_OPEN_DIRECTORY = 0o200000;
export const FD_OPEN_NOFOLLOW = 0o400000;
/** `O_PATH` — descriptor for metadata-only inspection (symlinks, xattrs). */
export const FD_OPEN_PATH = 0o10000000;
/** `AT_FDCWD` — the openat directory descriptor meaning the current directory. */
export const FD_AT_FDCWD = -100;

/** `unlinkat`/`faccessat`/`renameat2` flag values (Linux ABI). */
export const FD_AT_SYMLINK_NOFOLLOW = 0x100;
export const FD_AT_REMOVEDIR = 0x200;
export const FD_AT_EACCESS = 0x200;
export const FD_RENAME_NOREPLACE = 1;
export const FD_W_OK = 2;

/** The errno values the writer's classification depends on (Linux). */
export const FD_EPERM = 1;
export const FD_ENOENT = 2;
export const FD_EACCES = 13;
export const FD_EEXIST = 17;
export const FD_EXDEV = 18;
export const FD_EISDIR = 21;
export const FD_EINVAL = 22;
export const FD_ENOSPC = 28;
export const FD_EROFS = 30;
export const FD_ENOTEMPTY = 39;
export const FD_ELOOP = 40;
export const FD_ENOSYS = 38;
export const FD_ENOTDIR = 20;
export const FD_ENODATA = 61;
export const FD_ENOTSUP = 95;

/** `ST_RDONLY` — the `fstatvfs` read-only filesystem flag. */
export const FD_ST_RDONLY = 1;

const GETDENTS64_SYSCALL_X86_64 = 217;
const GETDENTS64_SYSCALL_ASM_GENERIC = 61;

/** Encodes one safe relative path component as a NUL-terminated buffer. */
export function encodeFdName(name: string): Buffer {
  return Buffer.from(`${name}\0`, "utf8");
}

interface LibcFdBinding {
  openat(dirfd: number, path: Buffer, flags: number, mode: number): number;
  mkdirat(dirfd: number, path: Buffer, mode: number): number;
  unlinkat(dirfd: number, path: Buffer, flags: number): number;
  symlinkat(target: Buffer, newdirfd: number, path: Buffer): number;
  renameat2(olddirfd: number, oldpath: Buffer, newdirfd: number, newpath: Buffer, flags: number): number;
  renameat(olddirfd: number, oldpath: Buffer, newdirfd: number, newpath: Buffer): number;
  readlinkat(dirfd: number, path: Buffer, buf: Buffer, size: number): bigint;
  flistxattr(fd: number, buf: Buffer, size: number): bigint;
  fstatvfs(fd: number, buf: Buffer): number;
  faccessat(dirfd: number, path: Buffer, mode: number, flags: number): number;
  syscall(number: bigint, arg2: number, arg3: Buffer, arg4: number): bigint;
  errnoLocation(): unknown;
  memcpy(destination: unknown, source: unknown, length: number): unknown;
}

let libcBinding: LibcFdBinding | null = null;

function fdLibc(): LibcFdBinding {
  if (libcBinding !== null) {
    return libcBinding;
  }
  const symbols = {
    openat: { args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.u32], returns: FFIType.i32 },
    mkdirat: { args: [FFIType.i32, FFIType.ptr, FFIType.u32], returns: FFIType.i32 },
    unlinkat: { args: [FFIType.i32, FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
    symlinkat: { args: [FFIType.ptr, FFIType.i32, FFIType.ptr], returns: FFIType.i32 },
    renameat2: { args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr, FFIType.u32], returns: FFIType.i32 },
  renameat: { args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.ptr], returns: FFIType.i32 },
    readlinkat: { args: [FFIType.i32, FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
    flistxattr: { args: [FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
    fstatvfs: { args: [FFIType.i32, FFIType.ptr], returns: FFIType.i32 },
    faccessat: { args: [FFIType.i32, FFIType.ptr, FFIType.i32, FFIType.i32], returns: FFIType.i32 },
    syscall: { args: [FFIType.u64, FFIType.i32, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
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
      const required = [
        "openat",
        "mkdirat",
        "unlinkat",
        "symlinkat",
        "renameat2",
        "renameat",
        "readlinkat",
        "flistxattr",
        "fstatvfs",
        "faccessat",
        "syscall",
        "__errno_location",
        "memcpy",
      ];
      for (const name of required) {
        if (typeof record[name] !== "function") {
          throw new Error(`libc candidate ${candidate} is missing the required symbol ${name}`);
        }
      }
      libcBinding = {
        openat: record["openat"] as LibcFdBinding["openat"],
        mkdirat: record["mkdirat"] as LibcFdBinding["mkdirat"],
        unlinkat: record["unlinkat"] as LibcFdBinding["unlinkat"],
        symlinkat: record["symlinkat"] as LibcFdBinding["symlinkat"],
        renameat2: record["renameat2"] as LibcFdBinding["renameat2"],
        renameat: record["renameat"] as LibcFdBinding["renameat"],
        readlinkat: record["readlinkat"] as LibcFdBinding["readlinkat"],
        flistxattr: record["flistxattr"] as LibcFdBinding["flistxattr"],
        fstatvfs: record["fstatvfs"] as LibcFdBinding["fstatvfs"],
        faccessat: record["faccessat"] as LibcFdBinding["faccessat"],
        syscall: record["syscall"] as LibcFdBinding["syscall"],
        errnoLocation: record["__errno_location"] as LibcFdBinding["errnoLocation"],
        memcpy: record["memcpy"] as LibcFdBinding["memcpy"],
      };
      return libcBinding;
    } catch (cause) {
      lastFailure = cause;
    }
  }
  const detail = lastFailure instanceof Error ? lastFailure.message : String(lastFailure ?? "");
  throw new ApplyFdError("libc_unavailable", `the apply fd adapter could not bind libc (${detail})`);
}

const errnoBuffer = new Uint8Array(4);

/**
 * The raw syscall surface of the writer. Every method returns the raw
 * libc result; a -1 return means the caller must consult `errno()`.
 */
export interface ApplyFdOps {
  readonly openat: (dirfd: number, name: Buffer, flags: number, mode: number) => number;
  readonly mkdirat: (dirfd: number, name: Buffer, mode: number) => number;
  readonly unlinkat: (dirfd: number, name: Buffer, flags: number) => number;
  readonly symlinkat: (target: Buffer, dirfd: number, name: Buffer) => number;
  readonly renameat2: (olddirfd: number, oldName: Buffer, newdirfd: number, newName: Buffer, flags: number) => number;
  readonly renameat: (olddirfd: number, oldName: Buffer, newdirfd: number, newName: Buffer) => number;
  /** Returns the target length (BigInt) or -1. */
  readonly readlinkat: (dirfd: number, name: Buffer, buf: Buffer, size: number) => bigint;
  /** Returns the xattr list size (BigInt) or -1. */
  readonly flistxattr: (fd: number, buf: Buffer, size: number) => bigint;
  /** Writes the raw `struct statvfs`; returns 0 or -1. */
  readonly fstatvfs: (fd: number, buf: Buffer) => number;
  /** Returns 0 (allowed) or -1. */
  readonly faccessat: (dirfd: number, name: Buffer, mode: number, flags: number) => number;
  /** The raw `getdents64` syscall; returns the byte count (BigInt) or -1. */
  readonly getdents64: (fd: number, buf: Buffer, size: number) => bigint;
  /** The current thread errno (only meaningful after a -1 return). */
  readonly errno: () => number;
  /** `fstat` on a raw descriptor (metadata-only for `O_PATH` too). */
  readonly fstat: (fd: number) => Stats;
  /** `pread` on a raw descriptor; returns the byte count. */
  readonly readFd: (fd: number, buffer: Buffer, offset: number, length: number, position: number) => number;
  /** `pwrite` on a raw descriptor; returns the byte count. */
  readonly writeFd: (fd: number, buffer: Buffer, offset: number, length: number, position: number) => number;
  /** `fchmod` on a raw descriptor. */
  readonly fchmod: (fd: number, mode: number) => void;
  /** `fsync` on a raw descriptor (files and directories). */
  readonly fsyncFd: (fd: number) => void;
  /** `close` on a raw descriptor. */
  readonly closeFd: (fd: number) => void;
}

function buildRealApplyFdOps(): ApplyFdOps {
  const readErrno = (): number => {
    const binding = libcBinding!;
    binding.memcpy(errnoBuffer as unknown as Uint8Array, binding.errnoLocation() as never, 4);
    return new DataView(errnoBuffer.buffer).getInt32(0, true);
  };
  const getdentsNumber = BigInt(process.arch === "x64" ? GETDENTS64_SYSCALL_X86_64 : GETDENTS64_SYSCALL_ASM_GENERIC);
  const lazy = (): LibcFdBinding => {
    const binding = fdLibc();
    if (binding === null) {
      throw new ApplyFdError("libc_unavailable", "the apply fd adapter could not bind libc");
    }
    return binding;
  };
  const adapter: ApplyFdOps = {
    openat: (dirfd, name, flags, mode) => lazy().openat(dirfd, name, flags, mode),
    mkdirat: (dirfd, name, mode) => lazy().mkdirat(dirfd, name, mode),
    unlinkat: (dirfd, name, flags) => lazy().unlinkat(dirfd, name, flags),
    symlinkat: (target, dirfd, name) => lazy().symlinkat(target, dirfd, name),
    renameat2: (olddirfd, oldName, newdirfd, newName, flags) => lazy().renameat2(olddirfd, oldName, newdirfd, newName, flags),
    renameat: (olddirfd, oldName, newdirfd, newName) => lazy().renameat(olddirfd, oldName, newdirfd, newName),
    readlinkat: (dirfd, name, buf, size) => lazy().readlinkat(dirfd, name, buf, size),
    flistxattr: (fd, buf, size) => lazy().flistxattr(fd, buf, size),
    fstatvfs: (fd, buf) => lazy().fstatvfs(fd, buf),
    faccessat: (dirfd, name, mode, flags) => lazy().faccessat(dirfd, name, mode, flags),
    getdents64: (fd, buf, size) => lazy().syscall(getdentsNumber, fd, buf, size),
    errno: readErrno,
    fstat: (fd) => fs.fstatSync(fd),
    readFd: (fd, buffer, offset, length, position) => fs.readSync(fd, buffer, offset, length, position),
    writeFd: (fd, buffer, offset, length, position) => fs.writeSync(fd, buffer, offset, length, position),
    fchmod: (fd, mode) => fs.fchmodSync(fd, mode),
    fsyncFd: (fd) => fs.fsyncSync(fd),
    closeFd: (fd) => fs.closeSync(fd),
  };
  return Object.freeze(adapter);
}

/** The fixed real adapter; tests inject their own object per call. */
export const realApplyFdOps: ApplyFdOps = buildRealApplyFdOps();

/** The `f_flag` field of a raw `struct statvfs` (offset 72, u64). */
export function parseStatvfsFlag(buf: Buffer): number {
  return Number(buf.readBigUInt64LE(72) & 0xffffffffn);
}

/** Parses one `getdents64` batch into the contained entry names. */
export function parseDirentNames(buf: Buffer, byteCount: number): string[] {
  const names: string[] = [];
  let offset = 0;
  while (offset < byteCount) {
    const recordLength = buf.readUInt16LE(offset + 16);
    if (recordLength < 20 || offset + recordLength > byteCount) {
      throw new ApplyFdError("libc_unavailable", "the directory record batch is malformed");
    }
    const name = buf.toString("utf8", offset + 19, offset + recordLength);
    const nul = name.indexOf("\0");
    names.push(nul === -1 ? name : name.slice(0, nul));
    offset += recordLength;
  }
  return names;
}
