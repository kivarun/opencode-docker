import { createHash } from "node:crypto";
import {
  encodeFdName,
  FD_AT_EACCESS,
  FD_AT_REMOVEDIR,
  FD_EACCES,
  FD_ELOOP,
  FD_ENOENT,
  FD_ENODATA,
  FD_ENOSYS,
  FD_ENOTSUP,
  FD_OPEN_CLOEXEC,
  FD_OPEN_DIRECTORY,
  FD_OPEN_NOFOLLOW,
  FD_OPEN_NONBLOCK,
  FD_OPEN_PATH,
  FD_OPEN_RDONLY,
  FD_ST_RDONLY,
  FD_W_OK,
  parseDirentNames,
  parseStatvfsFlag,
  type ApplyFdOps,
} from "./pipeline_v2_apply_fd_internal.ts";
import type { PipelineV2ApplyNode, PipelineV2ApplyPlan, PipelineV2ApplyPlanStep } from "./pipeline_v2_apply_plan_internal.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/**
 * The verification owner of the project apply filesystem writer: every
 * project-object observation, every node comparison, the fd-based v1
 * metadata gate, and the single plan-walk engine shared by the full
 * preflight, the recovery preflight and the final verification.
 *
 * Everything here is fd-relative: a path is observed by walking its
 * ancestor chain from the held project-root descriptor with
 * `openat(..., O_DIRECTORY | O_NOFOLLOW)` and inspecting the leaf through
 * `fstat` on a held descriptor (regular files and directories via
 * `O_RDONLY` opens, symlinks via `O_PATH | O_NOFOLLOW`). No absolute
 * target path is ever assembled; a substituted ancestor cannot redirect
 * an observation because each component is opened through the previous
 * component's descriptor.
 *
 * The walk (see `walkApplyPlanAgainstProject`) verifies every affected
 * path of the plan against the plan's own execution history:
 *
 *   - a completed step (index ≤ completed_step_count) must sit exactly at
 *     its postcondition, or the whole path must already sit at the final
 *     planned state;
 *   - a pending step must sit at its projected precondition, at its own
 *     postcondition (adoptable), or the whole path must already sit at
 *     the final planned state;
 *   - every noop path must sit exactly at its recorded expected state;
 *   - every step the walk classifies as applicable gets an ancestor-chain
 *     check (existing unplanned ancestors are real directories, carried
 *     through the fd chain, metadata-gated and write-access checked;
 *     planned ancestors must resolve to directories) and a containment
 *     check for directory removals (every current direct child must be a
 *     planned path under that directory — no operator-only descendant
 *     survives a reviewed removal).
 *
 * A verification failure is a typed `PipelineV2ApplyStateMismatch`; the
 * orchestration owner maps it to `conflict` (before any mutation) or
 * `recovery_required` (after partial apply) — never by message text.
 */

/** The typed refusal of one plan-state verification; content-free. */
export type PipelineV2ApplyMismatchKind = "diverged" | "unsupported_metadata" | "unsupported_project_entry" | "foreign_temp";

export class PipelineV2ApplyStateMismatch extends Error {
  declare readonly mismatch: PipelineV2ApplyMismatchKind;

  constructor(mismatch: PipelineV2ApplyMismatchKind, message: string) {
    super(message);
    this.name = "PipelineV2ApplyStateMismatch";
    Object.defineProperty(this, "mismatch", {
      value: mismatch,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

export const applyDiverged = (message: string): PipelineV2ApplyStateMismatch =>
  new PipelineV2ApplyStateMismatch("diverged", message);
export const applyUnsupportedMetadata = (message: string): PipelineV2ApplyStateMismatch =>
  new PipelineV2ApplyStateMismatch("unsupported_metadata", message);
export const applyUnsupportedEntry = (message: string): PipelineV2ApplyStateMismatch =>
  new PipelineV2ApplyStateMismatch("unsupported_project_entry", message);
export const applyForeignTemp = (message: string): PipelineV2ApplyStateMismatch =>
  new PipelineV2ApplyStateMismatch("foreign_temp", message);

/** One observed project node (absent is `null` at the leaf level). */
export type ApplyFdNodeSnapshot =
  | {
      readonly kind: "regular_file";
      readonly mode: number;
      readonly byte_count: number;
      readonly sha256: string;
      readonly nlink: number;
      readonly uid: number;
      readonly gid: number;
    }
  | { readonly kind: "directory"; readonly mode: number; readonly uid: number; readonly gid: number }
  | { readonly kind: "symlink"; readonly target: string; readonly uid: number; readonly gid: number };

export type ApplyObservedPath =
  | {
      readonly reachable: false;
      readonly cause: "missing_ancestor" | "invalid_ancestor";
      /** The 0-based ancestor component that broke the chain (invalid only). */
      readonly brokenAt?: number;
    }
  | { readonly reachable: true; readonly node: ApplyFdNodeSnapshot | null };

const DIGEST_CHUNK_BYTES = 128 * 1024;
const MAX_XATTR_LIST_BYTES = 64 * 1024;
const DIRENT_BATCH_BYTES = 8192;

function hashFd(fdOps: ApplyFdOps, fd: number, byteCount: number): string {
  const hasher = createHash("sha256");
  const chunk = Buffer.allocUnsafe(DIGEST_CHUNK_BYTES);
  let read = 0;
  while (read < byteCount) {
    const want = Math.min(chunk.byteLength, byteCount - read);
    const got = fdOps.readFd(fd, chunk, 0, want, read);
    if (got <= 0) {
      throw applyDiverged("a project file shrank while it was being observed");
    }
    hasher.update(chunk.subarray(0, got));
    read += got;
  }
  return hasher.digest("hex");
}

function fdXattrNames(fd: number, fdOps: ApplyFdOps): string[] {
  const size = Number(fdOps.flistxattr(fd, Buffer.alloc(0), 0));
  if (size < 0) {
    const errno = fdOps.errno();
    if (errno === FD_ENODATA || errno === FD_ENOTSUP || errno === FD_ENOSYS) {
      return [];
    }
    throw applyUnsupportedMetadata("an extended attribute list could not be read");
  }
  if (size === 0) {
    return [];
  }
  if (size > MAX_XATTR_LIST_BYTES) {
    throw applyUnsupportedMetadata("an extended attribute list is unexpectedly large");
  }
  const buffer = Buffer.alloc(size);
  const read = Number(fdOps.flistxattr(fd, buffer, size));
  if (read < 0) {
    throw applyUnsupportedMetadata("an extended attribute list could not be read");
  }
  const names: string[] = [];
  let start = 0;
  for (let index = 0; index < read; index += 1) {
    if (buffer[index] === 0) {
      names.push(buffer.toString("utf8", start, index));
      start = index + 1;
    }
  }
  return names;
}

/**
 * The fd-based twin of the preparation metadata probe: the exact v1
 * policy over one observed node. The caller holds the descriptor the
 * snapshot was taken from; the check is strictly read-only.
 */
export function gateApplyNodeMetadata(
  fdOps: ApplyFdOps,
  node: Exclude<ApplyFdNodeSnapshot, null>,
  fd: number | null,
  checkXattrs: boolean,
): void {
  const euid = process.getuid?.() ?? -1;
  const egid = process.getgid?.() ?? -1;
  const groups = process.getgroups?.() ?? [];
  if (node.uid !== euid) {
    throw applyUnsupportedMetadata("an observed project object is owned by another user");
  }
  if (node.gid !== egid && !groups.includes(node.gid)) {
    throw applyUnsupportedMetadata("an observed project object is owned by another group");
  }
  if (node.kind !== "symlink") {
    const specialBits = node.mode & 0o7000;
    if (specialBits !== 0) {
      throw applyUnsupportedMetadata("an observed project object carries special permission bits");
    }
  }
  if (node.kind === "regular_file" && node.nlink > 1) {
    throw applyUnsupportedMetadata("an observed project regular file has more than one hard link");
  }
  if (fd === null || !checkXattrs) {
    // An O_PATH descriptor of a symlink does not carry xattr operations
    // (EBADF); symlink extended attributes are outside the v1 logical
    // contract and are not re-checked by the writer.
    return;
  }
  if (fdXattrNames(fd, fdOps).length > 0) {
    throw applyUnsupportedMetadata("an observed project object carries extended attributes");
  }
}

/**
 * Observes one leaf relative to a held parent descriptor. The leaf is
 * never dereferenced: a symlink is recognized through `ELOOP` of the
 * `O_NOFOLLOW` open and inspected through an `O_PATH` descriptor.
 */
export function observeNodeAt(fdOps: ApplyFdOps, parentFd: number, name: string): ApplyFdNodeSnapshot | null {
  const fd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_RDONLY | FD_OPEN_NOFOLLOW | FD_OPEN_NONBLOCK | FD_OPEN_CLOEXEC, 0);
  if (fd >= 0) {
    try {
      const stat = fdOps.fstat(fd);
      if (stat.isFile()) {
        const node: ApplyFdNodeSnapshot = deepFreezeValue({
          kind: "regular_file",
          mode: stat.mode & 0o7777,
          byte_count: stat.size,
          sha256: hashFd(fdOps, fd, stat.size),
          nlink: stat.nlink,
          uid: stat.uid,
          gid: stat.gid,
        });
        gateApplyNodeMetadata(fdOps, node, fd, true);
        return node;
      }
      if (stat.isDirectory()) {
        const node: ApplyFdNodeSnapshot = deepFreezeValue({
          kind: "directory",
          mode: stat.mode & 0o7777,
          uid: stat.uid,
          gid: stat.gid,
        });
        gateApplyNodeMetadata(fdOps, node, fd, true);
        return node;
      }
      throw applyUnsupportedEntry("an observed project object is an unsupported filesystem object kind");
    } finally {
      try {
        fdOps.closeFd(fd);
      } catch {
        // best effort
      }
    }
  }
  const errno = fdOps.errno();
  if (errno === FD_ENOENT) {
    return null;
  }
  if (errno !== FD_ELOOP) {
    if (errno === FD_EACCES) {
      throw applyUnsupportedMetadata("an observed project object could not be opened for inspection");
    }
    throw applyDiverged("an observed project object could not be inspected");
  }
  // The leaf is a symlink (or the parent chain itself broke with ELOOP,
  // which the ancestor check rejects before any leaf observation).
  const pathFd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_PATH | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (pathFd < 0) {
    throw applyDiverged("an observed project symlink could not be inspected");
  }
  try {
    const stat = fdOps.fstat(pathFd);
    if ((stat.mode & 0o170000) !== 0o120000) {
      throw applyDiverged("an observed project symlink could not be confirmed");
    }
    const linkBuffer = Buffer.alloc(4096);
    const length = Number(fdOps.readlinkat(parentFd, encodeFdName(name), linkBuffer, linkBuffer.byteLength));
    if (length < 0) {
      throw applyDiverged("an observed project symlink target could not be read");
    }
    const node: ApplyFdNodeSnapshot = deepFreezeValue({
      kind: "symlink",
      target: linkBuffer.toString("utf8", 0, length),
      uid: stat.uid,
      gid: stat.gid,
    });
    gateApplyNodeMetadata(fdOps, node, pathFd, false);
    return node;
  } finally {
    try {
      fdOps.closeFd(pathFd);
    } catch {
      // best effort
    }
  }
}

/**
 * Observes one plan-relative path through the held project-root
 * descriptor. Ancestor components are opened `O_DIRECTORY | O_NOFOLLOW`;
 * a missing ancestor makes the path absent, an existing non-directory
 * ancestor is a divergence.
 */
export function observeApplyPath(fdOps: ApplyFdOps, rootFd: number, relativePath: string): ApplyObservedPath {
  const segments = relativePath.split("/");
  let current = rootFd;
  const owned: number[] = [];
  try {
    for (let index = 0; index < segments.length - 1; index += 1) {
      const fd = fdOps.openat(current, encodeFdName(segments[index]!), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
      if (fd >= 0) {
        owned.push(fd);
        current = fd;
        continue;
      }
      if (fdOps.errno() === FD_ENOENT) {
        return { reachable: false, cause: "missing_ancestor" };
      }
      return { reachable: false, cause: "invalid_ancestor", brokenAt: index };
    }
    return { reachable: true, node: observeNodeAt(fdOps, current, segments[segments.length - 1]!) };
  } finally {
    closeAll(fdOps, owned);
  }
}

function closeAll(fdOps: ApplyFdOps, fds: readonly number[]): void {
  for (const fd of fds) {
    try {
      fdOps.closeFd(fd);
    } catch {
      // best effort
    }
  }
}

/** Whether the observed node satisfies the plan node exactly (absent ⇔ null). */
export function fdNodeMatches(node: PipelineV2ApplyNode | null, observed: ApplyFdNodeSnapshot | null): boolean {
  if (node === null || observed === null) {
    return node === null && observed === null;
  }
  if (node.kind !== observed.kind) {
    return false;
  }
  if (node.kind === "directory" && observed.kind === "directory") {
    return node.mode === observed.mode;
  }
  if (node.kind === "symlink" && observed.kind === "symlink") {
    return node.target === observed.target;
  }
  if (node.kind === "regular_file" && observed.kind === "regular_file") {
    return node.mode === observed.mode && node.byte_count === observed.byte_count && node.sha256 === observed.sha256;
  }
  return false;
}

/**
 * Lists the direct entry names of one held directory descriptor through
 * the raw `getdents64` syscall. `.` and `..` are filtered.
 */
export function listDirectChildNames(fdOps: ApplyFdOps, dirFd: number): string[] {
  const buffer = Buffer.allocUnsafe(DIRENT_BATCH_BYTES);
  const names: string[] = [];
  for (;;) {
    const got = Number(fdOps.getdents64(dirFd, buffer, buffer.byteLength));
    if (got < 0) {
      throw applyDiverged("a project directory could not be read");
    }
    if (got === 0) {
      break;
    }
    for (const name of parseDirentNames(buffer, got)) {
      if (name !== "." && name !== "..") {
        names.push(name);
      }
    }
  }
  return names;
}

/** Whether one held directory descriptor carries no entries beyond `.`/`..`. */
export function isDirectoryEmpty(fdOps: ApplyFdOps, dirFd: number): boolean {
  return listDirectChildNames(fdOps, dirFd).length === 0;
}

/** The read-only filesystem probe over the held root descriptor. */
export function projectRootIsReadOnly(fdOps: ApplyFdOps, rootFd: number): boolean {
  const buffer = Buffer.alloc(160);
  if (fdOps.fstatvfs(rootFd, buffer) !== 0) {
    throw applyUnsupportedMetadata("the project filesystem could not be inspected");
  }
  return (parseStatvfsFlag(buffer) & FD_ST_RDONLY) !== 0;
}

/** The write-authority probe of one held directory descriptor. */
export function directoryIsWritable(fdOps: ApplyFdOps, dirFd: number): boolean {
  return fdOps.faccessat(dirFd, encodeFdName("."), FD_W_OK, FD_AT_EACCESS) === 0;
}

/** Opens one directory leaf through its held parent descriptor. */
export function openDirectoryAt(fdOps: ApplyFdOps, parentFd: number, name: string): number {
  const fd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (fd < 0) {
    throw applyDiverged("a project directory could not be opened");
  }
  return fd;
}

/**
 * Opens every ancestor directory of one plan-relative leaf path through
 * the fd chain and returns the parent descriptor of the leaf. The caller
 * owns closing the returned descriptor when `owned` is true.
 */
export function openParentDirectoryOf(fdOps: ApplyFdOps, rootFd: number, relativePath: string): { fd: number; owned: boolean } {
  const segments = relativePath.split("/");
  let current = rootFd;
  const owned: number[] = [];
  for (let index = 0; index < segments.length - 1; index += 1) {
    const fd = fdOps.openat(current, encodeFdName(segments[index]!), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
    if (fd < 0) {
      closeAll(fdOps, owned);
      throw applyDiverged("a project parent directory could not be opened");
    }
    owned.push(fd);
    current = fd;
  }
  return { fd: current, owned: owned.length > 0 };
}

/**
 * Opens one plan-relative directory path itself through the fd chain.
 * The caller owns closing the returned descriptor.
 */
export function openDirectoryAtPath(fdOps: ApplyFdOps, rootFd: number, relativePath: string): number {
  const parent = openParentDirectoryOf(fdOps, rootFd, relativePath);
  try {
    return openDirectoryAt(fdOps, parent.fd, relativePath.split("/").pop()!);
  } finally {
    if (parent.owned) {
      try {
        fdOps.closeFd(parent.fd);
      } catch {
        // best effort
      }
    }
  }
}

/** Removes one directory link (AT_REMOVEDIR) through its parent descriptor. */
export function removeDirectoryAt(fdOps: ApplyFdOps, parentFd: number, name: string): number {
  return fdOps.unlinkat(parentFd, encodeFdName(name), FD_AT_REMOVEDIR);
}

/** The per-step classification the walk and the executor share. */
export type ApplyStepClassification = "satisfied" | "adopted" | "ready";

/**
 * Classifies one step of a path against the observed node and the
 * projected model state:
 *   - `satisfied` — the whole path already sits at the final planned
 *     state (every step of the path is thereby resolved);
 *   - `adopted` — this step's own postcondition already holds;
 *   - `ready` — the projected precondition holds and the step applies.
 */
export function classifyApplyStep(
  step: PipelineV2ApplyPlanStep,
  finalState: PipelineV2ApplyNode | null,
  modelState: PipelineV2ApplyNode | null,
  observed: ApplyFdNodeSnapshot | null,
): ApplyStepClassification {
  if (fdNodeMatches(finalState, observed)) {
    return "satisfied";
  }
  if (fdNodeMatches(step.desired_after, observed)) {
    return "adopted";
  }
  if (fdNodeMatches(modelState, observed)) {
    return "ready";
  }
  throw applyDiverged("an affected path no longer matches the plan's projected state");
}

/**
 * Walks every affected path of the plan against the observed project and
 * returns the per-step classification the executor resumes from. Throws
 * `PipelineV2ApplyStateMismatch` on the first inconsistency.
 */
export interface ApplyPlanWalk {
  /** Steps grouped by path, in plan order. */
  readonly stepsByPath: ReadonlyMap<string, readonly number[]>;
  /** The 1-based step indexes classified satisfied by the final-state rule. */
  readonly satisfied: ReadonlySet<number>;
  /** The 1-based step indexes classified adopted (own postcondition holds). */
  readonly adopted: ReadonlySet<number>;
  /** The 1-based step indexes classified ready (precondition holds). */
  readonly ready: ReadonlySet<number>;
}

export function walkApplyPlanAgainstProject(
  fdOps: ApplyFdOps,
  rootFd: number,
  plan: PipelineV2ApplyPlan,
  completedStepCount: number,
  options: { readonly verifyApplicable: boolean },
): ApplyPlanWalk {
  if (!Number.isSafeInteger(completedStepCount) || completedStepCount < 0 || completedStepCount > plan.steps.length) {
    throw applyDiverged("the journal progress does not fit the plan");
  }
  const stepsByPath = new Map<string, number[]>();
  plan.steps.forEach((step, index) => {
    const list = stepsByPath.get(step.path) ?? [];
    list.push(index);
    stepsByPath.set(step.path, list);
  });

  const actual = new Map<string, ApplyObservedPath>();
  const noopPaths = plan.noops.map((entry) => entry.path);
  const observedOf = (path: string, planned: boolean): ApplyObservedPath => {
    let entry = actual.get(path);
    if (entry === undefined) {
      entry = observeApplyPath(fdOps, rootFd, path);
      if (
        planned &&
        !entry.reachable &&
        entry.cause === "invalid_ancestor" &&
        entry.brokenAt !== undefined
      ) {
        // An ancestor component exists but is not a directory. For a
        // planned path that ancestor may be a planned substitution still
        // pending its own steps; only an unplanned broken ancestor is a
        // real divergence.
        const segments = path.split("/");
        const brokenAncestor = segments.slice(0, entry.brokenAt + 1).join("/");
        if (stepsByPath.has(brokenAncestor) || noopPaths.includes(brokenAncestor)) {
          entry = { reachable: false, cause: "missing_ancestor" };
        }
      }
      actual.set(path, entry);
    }
    return entry;
  };

  const satisfied = new Set<number>();
  const adopted = new Set<number>();
  const ready = new Set<number>();

  for (const [path, stepIndexes] of stepsByPath) {
    const observed = observedOf(path, true);
    if (!observed.reachable && observed.cause === "invalid_ancestor") {
      throw applyDiverged("an affected path carries an ancestor that is no longer a real directory");
    }
    const node = observed.reachable ? observed.node : null;
    const lastIndex = stepIndexes[stepIndexes.length - 1]!;
    const finalState = plan.steps[lastIndex]!.desired_after;
    if (fdNodeMatches(finalState, node)) {
      // The whole path already sits at its final planned state; every
      // step of the path is resolved (the executor adopts them in order).
      for (const index of stepIndexes) {
        satisfied.add(index + 1);
      }
      continue;
    }
    // The completed prefix of this path (global progress is dense, so the
    // completed steps of a path form a prefix of its own step list) must
    // sit exactly at its recorded postconditions.
    let completedOnPath = 0;
    for (const index of stepIndexes) {
      if (index + 1 > completedStepCount) {
        break;
      }
      if (!fdNodeMatches(plan.steps[index]!.desired_after, node)) {
        throw applyDiverged("a completed apply step no longer sits at its recorded postcondition");
      }
      completedOnPath += 1;
    }
    const pending = stepIndexes.slice(completedOnPath);
    if (pending.length === 0) {
      throw applyDiverged("a fully applied path no longer sits at its final planned state");
    }
    const firstPending = plan.steps[pending[0]!]!;
    const beforePending =
      completedOnPath > 0 ? plan.steps[stepIndexes[completedOnPath - 1]!]!.desired_after : firstPending.expected_before;
    if (fdNodeMatches(beforePending, node)) {
      // The path sits at the first pending step's precondition: the whole
      // pending sequence applies in order from here.
      ready.add(pending[0]! + 1);
      for (let position = 1; position < pending.length; position += 1) {
        // Later steps of the path apply after their predecessors; they are
        // neither adopted nor ready at this observation point.
      }
      if (options.verifyApplicable) {
        verifyAncestorChainForStep(fdOps, rootFd, firstPending, stepsByPath, noopPaths);
        if (firstPending.kind === "remove_directory") {
          verifyDirectoryContainment(fdOps, rootFd, path, plan);
        }
      }
      continue;
    }
    // The path may already carry partial pending progress: find the
    // largest pending step whose own postcondition holds.
    let adoptedUpTo = -1;
    for (const index of pending) {
      if (fdNodeMatches(plan.steps[index]!.desired_after, node)) {
        adoptedUpTo = index;
      }
    }
    if (adoptedUpTo < 0) {
      throw applyDiverged("an affected path no longer matches the plan's projected state");
    }
    for (const index of pending) {
      if (index <= adoptedUpTo) {
        adopted.add(index + 1);
      }
    }
  }

  for (const noop of plan.noops) {
    const observed = observedOf(noop.path, false);
    if (!observed.reachable && observed.cause === "invalid_ancestor") {
      throw applyDiverged("an affected path carries an ancestor that is no longer a real directory");
    }
    const node = observed.reachable ? observed.node : null;
    if (!fdNodeMatches(noop.expected, node)) {
      throw applyDiverged("a reviewed unchanged path no longer matches its recorded state");
    }
  }

  return deepFreezeValue({
    stepsByPath,
    satisfied,
    adopted,
    ready,
  });
}

/**
 * The ancestor-chain check of one applicable step: planned ancestors
 * (step or noop paths) are verified by their own steps of the same walk
 * and by the planner's projected ordering, so they are skipped here;
 * every unplanned ancestor must exist as a real directory in the actual
 * project, metadata-gated and write-access checked through the fd chain.
 */
function verifyAncestorChainForStep(
  fdOps: ApplyFdOps,
  rootFd: number,
  step: PipelineV2ApplyPlanStep,
  stepsByPath: ReadonlyMap<string, readonly number[]>,
  noopPaths: readonly string[],
): void {
  const segments = step.path.split("/");
  for (let index = 1; index < segments.length; index += 1) {
    const ancestor = segments.slice(0, index).join("/");
    if (stepsByPath.has(ancestor) || noopPaths.includes(ancestor)) {
      continue;
    }
    const observed = observeApplyPath(fdOps, rootFd, ancestor);
    if (!observed.reachable || observed.node === null || observed.node.kind !== "directory") {
      throw applyDiverged("an apply step targets a path whose parent directory is missing or not a directory");
    }
    const dirFd = openDirectoryAtPath(fdOps, rootFd, ancestor);
    try {
      if (!directoryIsWritable(fdOps, dirFd)) {
        throw applyDiverged("a project parent directory does not grant the writer write access");
      }
    } finally {
      try {
        fdOps.closeFd(dirFd);
      } catch {
        // best effort
      }
    }
  }
}

/**
 * The containment check of one directory removal: every current direct
 * child of the directory must be the leaf of a planned path under it.
 */
function verifyDirectoryContainment(
  fdOps: ApplyFdOps,
  rootFd: number,
  directoryPath: string,
  plan: PipelineV2ApplyPlan,
): void {
  const dirFd = openDirectoryAtPath(fdOps, rootFd, directoryPath);
  try {
    const plannedChildren = new Set<string>();
    const prefix = `${directoryPath}/`;
    for (const step of plan.steps) {
      if (step.path.startsWith(prefix)) {
        plannedChildren.add(step.path.slice(prefix.length).split("/")[0]!);
      }
    }
    for (const noop of plan.noops) {
      if (noop.path.startsWith(prefix)) {
        plannedChildren.add(noop.path.slice(prefix.length).split("/")[0]!);
      }
    }
    for (const child of listDirectChildNames(fdOps, dirFd)) {
      if (!plannedChildren.has(child)) {
        throw applyDiverged("a directory the plan removes carries an operator-only descendant");
      }
    }
  } finally {
    try {
      fdOps.closeFd(dirFd);
    } catch {
      // best effort
    }
  }
}
