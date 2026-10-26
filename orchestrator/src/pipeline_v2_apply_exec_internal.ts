import { createHash, randomBytes } from "node:crypto";
import {
  encodeFdName,
  FD_AT_REMOVEDIR,
  FD_EEXIST,
  FD_EINVAL,
  FD_ELOOP,
  FD_ENOENT,
  FD_ENOSYS,
  FD_ENOTEMPTY,
  FD_ENOTSUP,
  FD_OPEN_CLOEXEC,
  FD_OPEN_CREAT,
  FD_OPEN_DIRECTORY,
  FD_OPEN_EXCL,
  FD_OPEN_NOFOLLOW,
  FD_OPEN_PATH,
  FD_OPEN_RDONLY,
  FD_OPEN_RDWR,
  FD_RENAME_NOREPLACE,
  type ApplyFdOps,
} from "./pipeline_v2_apply_fd_internal.ts";
import type { ProjectFileStreamIo } from "./pipeline_v2_project_stream_internal.ts";
import type { PipelineV2ApplyNode, PipelineV2ApplyPlan, PipelineV2ApplyPlanStep } from "./pipeline_v2_apply_plan_internal.ts";
import {
  applyDiverged,
  applyForeignTemp,
  classifyApplyStep,
  fdNodeMatches,
  isDirectoryEmpty,
  observeNodeAt,
  openParentDirectoryOf,
} from "./pipeline_v2_apply_verify_internal.ts";
import { isApplyCrashFault } from "./pipeline_v2_apply_crash_internal.ts";

/**
 * The step executor of the project apply filesystem writer: the single
 * owner of every project mutation. It applies one plan step at a time
 * against the held project-root descriptor using fd-relative operations
 * only (`openat`, `mkdirat`, `unlinkat`, `symlinkat`, `renameat`/
 * `renameat2`, `fchmod`, `fsync`), never assembling an absolute target
 * path and never following a leaf symlink.
 *
 * Per-step protocol: verify the precondition (fresh observation plus the
 * fd metadata gate) → adopt an exact postcondition when one already holds
 * → commit the durable temp intent (temp-owning steps only) → mutate →
 * fsync the changed object where applicable → fsync the parent directory
 * → verify the postcondition → commit the durable progress increment.
 * No next step starts before the previous step's progress is durably
 * recorded.
 *
 * Publication policy: an expected-absent target publishes through
 * `renameat2(RENAME_NOREPLACE)`; a replacement re-verifies its exact
 * precondition immediately before a plain atomic `renameat`. `EEXIST`
 * from a no-replace publication never overwrites: the target is
 * re-observed and either adopted (exact desired state) or refused as a
 * divergence. A filesystem without the no-replace primitive is a typed
 * `unsupported_filesystem` refusal, never a blind replacement.
 *
 * Temp ownership: every temp-owning step commits its generated temp name
 * into the journal before creating the temp; the committed name is the
 * ownership root, so a crash leaves a temp the next attempt classifies as
 * absent (recreate), owned-complete (adopt and publish), owned-incomplete
 * (safe clean and recreate under a fresh committed name) or foreign
 * (never removed — a typed recovery refusal).
 */

export const APPLY_TEMP_NAME_PREFIX = ".apply-tmp-";
export const APPLY_TEMP_NAME_PATTERN = /^\.apply-tmp-[0-9a-f]{32}$/;

/** The sealed payload of a publishing step no longer matches its contract. */
export class ApplyPayloadMismatchError extends Error {
  constructor() {
    super("the sealed payload no longer matches its plan content contract");
    this.name = "ApplyPayloadMismatchError";
  }
}

/** The filesystem lacks the no-replace publication primitive. */
export class ApplyFilesystemUnsupportedError extends Error {
  constructor() {
    super("the project filesystem does not support the no-replace publication primitive");
    this.name = "ApplyFilesystemUnsupportedError";
  }
}

export type ApplyTempState = "absent" | "complete" | "incomplete" | "foreign";

export interface ApplyExecutorOptions {
  readonly fdOps: ApplyFdOps;
  readonly fileStreamIo: ProjectFileStreamIo;
  readonly rootFd: number;
  readonly plan: PipelineV2ApplyPlan;
  readonly payloadsRoot: string;
  /** The first 1-based step index to execute. */
  readonly startStep: number;
  /** Steps grouped by path (plan order), as computed by the walk. */
  readonly stepsByPath: ReadonlyMap<string, readonly number[]>;
  /** The committed temp intent restored from the journal, if any. */
  readonly initialActiveStep: { readonly step_index: number; readonly temp_name: string } | null;
  /** Durable temp-intent commit (orchestration-owned state transition). */
  readonly commitTempIntent: (stepIndex: number, tempName: string) => Promise<void>;
  /** Durable progress commit (orchestration-owned state transition). */
  readonly commitProgress: (stepIndex: number) => Promise<void>;
}

const CHUNK_BYTES = 128 * 1024;

function freshTempName(): string {
  return `${APPLY_TEMP_NAME_PREFIX}${randomBytes(16).toString("hex")}`;
}

function leafOf(path: string): string {
  return path.split("/").pop()!;
}

function hashFdContent(fdOps: ApplyFdOps, fd: number): { byteCount: number; sha256: string } {
  const hasher = createHash("sha256");
  const chunk = Buffer.allocUnsafe(CHUNK_BYTES);
  let read = 0;
  for (;;) {
    const got = fdOps.readFd(fd, chunk, 0, chunk.byteLength, read);
    if (got <= 0) {
      break;
    }
    hasher.update(chunk.subarray(0, got));
    read += got;
  }
  return { byteCount: read, sha256: hasher.digest("hex") };
}

function unexpectedSyscall(operation: string, errno: number): Error {
  return new Error(`the apply writer could not ${operation} (errno ${errno})`);
}

/**
 * Executes the plan steps from `startStep` to the end, strictly in order,
 * committing durable progress after each one. Throws a
 * `PipelineV2ApplyStateMismatch` for every plan-state divergence, an
 * `ApplyPayloadMismatchError` for a tampered payload, an
 * `ApplyFilesystemUnsupportedError` for a missing no-replace primitive,
 * and a plain error for infrastructure failures; a crash-fault marker
 * propagates immediately without cleanup.
 */
export async function executeApplySteps(options: ApplyExecutorOptions): Promise<void> {
  const { fdOps, fileStreamIo, rootFd, plan, payloadsRoot, stepsByPath } = options;
  let activeIntent = options.initialActiveStep;
  for (let stepIndex = options.startStep; stepIndex <= plan.steps.length; stepIndex += 1) {
    const step = plan.steps[stepIndex - 1]!;
    const pathSteps = stepsByPath.get(step.path) ?? [stepIndex - 1];
    const lastIndex = pathSteps[pathSteps.length - 1]!;
    const finalState = plan.steps[lastIndex]!.desired_after;
    const position = pathSteps.indexOf(stepIndex - 1);
    const modelState =
      position <= 0 ? step.expected_before : plan.steps[pathSteps[position - 1]!]!.desired_after;
    const parent = openParentDirectoryOf(fdOps, rootFd, step.path);
    try {
      const name = leafOf(step.path);
      const observed = observeNodeAt(fdOps, parent.fd, name);
      const classification = classifyApplyStep(step, finalState, modelState, observed);
      if (classification === "satisfied" || classification === "adopted") {
        await options.commitProgress(stepIndex);
        continue;
      }
      switch (step.kind) {
        case "remove_leaf":
          await executeRemoveLeaf(fdOps, parent.fd, name, step, stepIndex, options.commitProgress);
          break;
        case "remove_directory":
          await executeRemoveDirectory(fdOps, parent.fd, name, step, stepIndex, options.commitProgress);
          break;
        case "create_directory":
          await executeCreateDirectory(fdOps, parent.fd, name, step, stepIndex, options.commitProgress);
          break;
        case "publish_regular_file":
          activeIntent = await executePublishRegularFile(
            fdOps,
            fileStreamIo,
            parent.fd,
            name,
            step,
            stepIndex,
            plan,
            payloadsRoot,
            activeIntent,
            options.commitTempIntent,
            options.commitProgress,
          );
          break;
        case "publish_symlink":
          activeIntent = await executePublishSymlink(
            fdOps,
            parent.fd,
            name,
            step,
            stepIndex,
            activeIntent,
            options.commitTempIntent,
            options.commitProgress,
          );
          break;
        case "set_mode":
          await executeSetMode(fdOps, parent.fd, name, step, stepIndex, options.commitProgress);
          break;
      }
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
}

async function executeRemoveLeaf(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  if (step.desired_after !== null) {
    throw applyDiverged("the remove step must remove to absence");
  }
  const result = fdOps.unlinkat(parentFd, encodeFdName(name), 0);
  if (result < 0) {
    const errno = fdOps.errno();
    if (errno === FD_ENOENT) {
      // The object vanished between the precondition verify and the
      // unlink; absence is the exact postcondition.
      await verifyAbsentAndCommit(fdOps, parentFd, name, stepIndex, commitProgress);
      return;
    }
    throw unexpectedSyscall("remove the project object", errno);
  }
  fdOps.fsyncFd(parentFd);
  await verifyAbsentAndCommit(fdOps, parentFd, name, stepIndex, commitProgress);
}

async function verifyAbsentAndCommit(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  const observed = observeNodeAt(fdOps, parentFd, name);
  if (observed !== null) {
    throw applyDiverged("a removed project object reappeared before its progress was recorded");
  }
  await commitProgress(stepIndex);
}

async function executeRemoveDirectory(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  if (step.desired_after !== null) {
    throw applyDiverged("the remove step must remove to absence");
  }
  const dirFd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (dirFd < 0) {
    const errno = fdOps.errno();
    if (errno === FD_ENOENT) {
      await verifyAbsentAndCommit(fdOps, parentFd, name, stepIndex, commitProgress);
      return;
    }
    throw unexpectedSyscall("open the project directory", errno);
  }
  let empty: boolean;
  try {
    empty = isDirectoryEmpty(fdOps, dirFd);
  } finally {
    try {
      fdOps.closeFd(dirFd);
    } catch {
      // best effort
    }
  }
  if (!empty) {
    throw applyDiverged("a directory the plan removes is not empty at its turn");
  }
  const result = fdOps.unlinkat(parentFd, encodeFdName(name), FD_AT_REMOVEDIR);
  if (result < 0) {
    const errno = fdOps.errno();
    if (errno === FD_ENOENT) {
      await verifyAbsentAndCommit(fdOps, parentFd, name, stepIndex, commitProgress);
      return;
    }
    if (errno === FD_ENOTEMPTY) {
      throw applyDiverged("a directory the plan removes is not empty at its turn");
    }
    throw unexpectedSyscall("remove the project directory", errno);
  }
  fdOps.fsyncFd(parentFd);
  await verifyAbsentAndCommit(fdOps, parentFd, name, stepIndex, commitProgress);
}

async function executeCreateDirectory(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  const desired = step.desired_after;
  if (desired === null || desired.kind !== "directory") {
    throw applyDiverged("the create step must create a directory node");
  }
  const result = fdOps.mkdirat(parentFd, encodeFdName(name), 0o700);
  if (result < 0) {
    const errno = fdOps.errno();
    if (errno === FD_EEXIST) {
      // The directory appeared between the precondition verify and the
      // mkdir; an exact desired directory may still be adopted.
      const observed = observeNodeAt(fdOps, parentFd, name);
      if (observed !== null && observed.kind === "directory" && observed.mode === desired.mode) {
        await verifyCreatedDirectory(fdOps, parentFd, name, desired, stepIndex, commitProgress);
        return;
      }
      throw applyDiverged("a directory the plan creates appeared with a foreign shape");
    }
    throw unexpectedSyscall("create the project directory", errno);
  }
  const dirFd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (dirFd < 0) {
    throw unexpectedSyscall("open the created project directory", fdOps.errno());
  }
  try {
    fdOps.fchmod(dirFd, desired.mode);
    fdOps.fsyncFd(dirFd);
  } finally {
    try {
      fdOps.closeFd(dirFd);
    } catch {
      // best effort
    }
  }
  fdOps.fsyncFd(parentFd);
  await verifyCreatedDirectory(fdOps, parentFd, name, desired, stepIndex, commitProgress);
}

async function verifyCreatedDirectory(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  desired: PipelineV2ApplyNode & { readonly kind: "directory" },
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  const dirFd = fdOps.openat(parentFd, encodeFdName(name), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (dirFd < 0) {
    throw applyDiverged("a created project directory could not be reopened for verification");
  }
  let mode: number;
  let empty: boolean;
  try {
    const stat = fdOps.fstat(dirFd);
    mode = stat.mode & 0o7777;
    empty = isDirectoryEmpty(fdOps, dirFd);
  } finally {
    try {
      fdOps.closeFd(dirFd);
    } catch {
      // best effort
    }
  }
  if (mode !== desired.mode || !empty) {
    throw applyDiverged("a created project directory does not match its exact desired state");
  }
  await commitProgress(stepIndex);
}

type ActiveIntent = { readonly step_index: number; readonly temp_name: string } | null;

async function executePublishRegularFile(
  fdOps: ApplyFdOps,
  fileStreamIo: ProjectFileStreamIo,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  plan: PipelineV2ApplyPlan,
  payloadsRoot: string,
  activeIntent: ActiveIntent,
  commitTempIntent: (stepIndex: number, tempName: string) => Promise<void>,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<ActiveIntent> {
  const desired = step.desired_after;
  if (desired === null || desired.kind !== "regular_file" || step.payload_index === undefined) {
    throw applyDiverged("the publish step must carry a regular-file node and its payload");
  }
  const payload = plan.payloads.find((entry) => entry.payload_index === step.payload_index);
  if (payload === undefined) {
    throw applyDiverged("the publish step references an unknown payload");
  }
  const contract = { sha256: payload.sha256, byteCount: payload.byte_count };
  const prior = activeIntent !== null && activeIntent.step_index === stepIndex ? activeIntent : null;
  let tempName: string;
  let resumedComplete = false;
  let createTemp = true;
  if (prior !== null) {
    const state = inspectOwnedTemp(fdOps, parentFd, prior.temp_name, contract, desired.mode);
    if (state === "foreign") {
      throw applyForeignTemp("a committed apply temp name carries a foreign object");
    }
    if (state === "incomplete") {
      await cleanOwnedTemp(fdOps, parentFd, prior.temp_name);
      tempName = freshTempName();
      await commitTempIntent(stepIndex, tempName);
    } else {
      tempName = prior.temp_name;
      resumedComplete = state === "complete";
      createTemp = !resumedComplete;
    }
  } else {
    tempName = freshTempName();
    await commitTempIntent(stepIndex, tempName);
  }
  const tempFd = fdOps.openat(
    parentFd,
    encodeFdName(tempName),
    (createTemp ? FD_OPEN_RDWR | FD_OPEN_CREAT | FD_OPEN_EXCL : FD_OPEN_RDWR) | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC,
    0o600,
  );
  if (tempFd < 0) {
    const errno = fdOps.errno();
    if (errno === FD_EEXIST) {
      // The just-committed name is occupied by a foreign object; it is
      // never removed — the attempt refuses and a recovery attempt
      // classifies the foreign temp through the walk.
      throw applyForeignTemp("a committed apply temp name is already occupied");
    }
    throw unexpectedSyscall("create the apply temp file", errno);
  }
  try {
    const stat = fdOps.fstat(tempFd);
    if (!stat.isFile() || (createTemp && (stat.mode & 0o7777) !== 0o600)) {
      throw new Error("the apply temp file does not have its required creation shape");
    }
    if (!resumedComplete) {
      await streamPayloadIntoTemp(fdOps, fileStreamIo, `${payloadsRoot}/${payload.artifact}`, tempFd, contract);
    }
    fdOps.fchmod(tempFd, desired.mode);
    fdOps.fsyncFd(tempFd);
    const readBack = hashFdContent(fdOps, tempFd);
    if (readBack.byteCount !== contract.byteCount || readBack.sha256 !== contract.sha256) {
      throw new ApplyPayloadMismatchError();
    }
    await publishTemp(fdOps, parentFd, tempName, name, step, desired);
    fdOps.fsyncFd(parentFd);
    await verifyPublishedPostcondition(fdOps, parentFd, name, desired, stepIndex, commitProgress);
    return null;
  } catch (cause) {
    try {
      fdOps.closeFd(tempFd);
    } catch {
      // best effort
    }
    if (isApplyCrashFault(cause)) {
      throw cause;
    }
    // Best-effort cleanup of the writer's own temp; the ownership proof
    // is the journal-committed name of this very step.
    await cleanOwnedTemp(fdOps, parentFd, tempName).catch(() => undefined);
    throw cause;
  }
}

async function executePublishSymlink(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  activeIntent: ActiveIntent,
  commitTempIntent: (stepIndex: number, tempName: string) => Promise<void>,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<ActiveIntent> {
  const desired = step.desired_after;
  if (desired === null || desired.kind !== "symlink") {
    throw applyDiverged("the publish step must create a symlink node");
  }
  const prior = activeIntent !== null && activeIntent.step_index === stepIndex ? activeIntent : null;
  let tempName: string;
  let createTemp = true;
  if (prior !== null) {
    const state = inspectOwnedSymlinkTemp(fdOps, parentFd, prior.temp_name, desired.target);
    if (state === "foreign") {
      throw applyForeignTemp("a committed apply temp name carries a foreign object");
    }
    tempName = prior.temp_name;
    createTemp = state === "absent";
  } else {
    tempName = freshTempName();
    await commitTempIntent(stepIndex, tempName);
  }
  if (createTemp) {
    const created = fdOps.symlinkat(encodeFdName(desired.target), parentFd, encodeFdName(tempName));
    if (created < 0) {
      const errno = fdOps.errno();
      if (errno === FD_EEXIST) {
        throw applyForeignTemp("a committed apply temp name is already occupied");
      }
      throw unexpectedSyscall("create the apply temp symlink", errno);
    }
  }
  try {
    await publishTemp(fdOps, parentFd, tempName, name, step, desired);
    fdOps.fsyncFd(parentFd);
    await verifyPublishedPostcondition(fdOps, parentFd, name, desired, stepIndex, commitProgress);
    return null;
  } catch (cause) {
    if (isApplyCrashFault(cause)) {
      throw cause;
    }
    await cleanOwnedTemp(fdOps, parentFd, tempName).catch(() => undefined);
    throw cause;
  }
}

function inspectOwnedTemp(
  fdOps: ApplyFdOps,
  parentFd: number,
  tempName: string,
  contract: { readonly sha256: string; readonly byteCount: number },
  desiredMode: number,
): ApplyTempState {
  const fd = fdOps.openat(parentFd, encodeFdName(tempName), FD_OPEN_RDONLY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (fd < 0) {
    return fdOps.errno() === FD_ENOENT ? "absent" : "foreign";
  }
  try {
    const stat = fdOps.fstat(fd);
    const mode = stat.mode & 0o7777;
    // A temp the writer owns is either still being written (0600) or
    // already carries its desired mode (the crash came after the fchmod).
    if (!stat.isFile() || (mode !== 0o600 && mode !== desiredMode)) {
      return "foreign";
    }
    const content = hashFdContent(fdOps, fd);
    if (content.byteCount !== contract.byteCount || content.sha256 !== contract.sha256) {
      return "incomplete";
    }
    return "complete";
  } finally {
    try {
      fdOps.closeFd(fd);
    } catch {
      // best effort
    }
  }
}

function inspectOwnedSymlinkTemp(
  fdOps: ApplyFdOps,
  parentFd: number,
  tempName: string,
  expectedTarget: string,
): Exclude<ApplyTempState, "incomplete"> {
  const fd = fdOps.openat(parentFd, encodeFdName(tempName), FD_OPEN_RDONLY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (fd >= 0) {
    // A regular file (or directory) at a temp symlink name is foreign.
    try {
      fdOps.closeFd(fd);
    } catch {
      // best effort
    }
    return "foreign";
  }
  const errno = fdOps.errno();
  if (errno === FD_ENOENT) {
    return "absent";
  }
  if (errno !== FD_ELOOP) {
    return "foreign";
  }
  // ELOOP: the object is a symlink; verify its kind and target.
  const pathFd = fdOps.openat(parentFd, encodeFdName(tempName), FD_OPEN_PATH | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
  if (pathFd < 0) {
    return "foreign";
  }
  try {
    const stat = fdOps.fstat(pathFd);
    if ((stat.mode & 0o170000) !== 0o120000) {
      return "foreign";
    }
  } catch {
    return "foreign";
  } finally {
    try {
      fdOps.closeFd(pathFd);
    } catch {
      // best effort
    }
  }
  const linkBuffer = Buffer.alloc(4096);
  const length = Number(fdOps.readlinkat(parentFd, encodeFdName(tempName), linkBuffer, linkBuffer.byteLength));
  if (length < 0) {
    return "foreign";
  }
  return linkBuffer.toString("utf8", 0, length) === expectedTarget ? "complete" : "foreign";
}

async function cleanOwnedTemp(fdOps: ApplyFdOps, parentFd: number, tempName: string): Promise<void> {
  const result = fdOps.unlinkat(parentFd, encodeFdName(tempName), 0);
  if (result < 0 && fdOps.errno() !== FD_ENOENT) {
    throw new Error(`the apply writer could not clean its own temp artifact (errno ${fdOps.errno()})`);
  }
  const observed = observeNodeAt(fdOps, parentFd, tempName);
  if (observed !== null) {
    throw new Error("the apply writer's own temp artifact could not be confirmed removed");
  }
}

async function streamPayloadIntoTemp(
  fdOps: ApplyFdOps,
  fileStreamIo: ProjectFileStreamIo,
  payloadPath: string,
  tempFd: number,
  contract: { readonly sha256: string; readonly byteCount: number },
): Promise<void> {
  const handle = await fileStreamIo.openNoFollow(payloadPath);
  try {
    const before = await handle.fstat();
    if (!before.isFile() || before.size !== contract.byteCount) {
      throw new ApplyPayloadMismatchError();
    }
    const hasher = createHash("sha256");
    const chunk = Buffer.allocUnsafe(CHUNK_BYTES);
    let read = 0;
    for (;;) {
      const { bytesRead } = await handle.read(chunk, 0, chunk.byteLength, read);
      if (bytesRead === 0) {
        break;
      }
      if (bytesRead < 0) {
        throw new ApplyPayloadMismatchError();
      }
      const window = chunk.subarray(0, bytesRead);
      hasher.update(window);
      let written = 0;
      while (written < bytesRead) {
        const put = fdOps.writeFd(tempFd, window, written, bytesRead - written, read + written);
        if (put <= 0) {
          throw new Error("the apply temp file could not be written without progress");
        }
        written += put;
      }
      read += bytesRead;
      if (read > contract.byteCount) {
        throw new ApplyPayloadMismatchError();
      }
    }
    if (read !== contract.byteCount || hasher.digest("hex") !== contract.sha256) {
      throw new ApplyPayloadMismatchError();
    }
  } finally {
    try {
      await handle.close();
    } catch {
      // best effort
    }
  }
}

/**
 * Publishes a verified temp over its target: `renameat2(NOREPLACE)` for
 * an expected-absent target, a plain atomic `renameat` for a replacement
 * whose exact precondition is re-verified immediately before the rename.
 */
async function publishTemp(
  fdOps: ApplyFdOps,
  parentFd: number,
  tempName: string,
  name: string,
  step: PipelineV2ApplyPlanStep,
  desired: PipelineV2ApplyNode,
): Promise<void> {
  if (step.expected_before === null) {
    const result = fdOps.renameat2(parentFd, encodeFdName(tempName), parentFd, encodeFdName(name), FD_RENAME_NOREPLACE);
    if (result < 0) {
      const errno = fdOps.errno();
      if (errno === FD_EEXIST) {
        // The target appeared between the precondition verify and the
        // publication; re-observe and adopt or refuse — never overwrite.
        const observed = observeNodeAt(fdOps, parentFd, name);
        if (fdNodeMatches(desired, observed)) {
          return;
        }
        throw applyDiverged("a published project target appeared with a foreign shape before its publication");
      }
      if (errno === FD_ENOSYS || errno === FD_EINVAL || errno === FD_ENOTSUP) {
        throw new ApplyFilesystemUnsupportedError();
      }
      throw unexpectedSyscall("publish the project object", errno);
    }
    return;
  }
  // Replacement: the exact precondition is re-verified immediately before
  // the atomic rename (the verify→rename window of a non-cooperative
  // writer remains a documented limitation).
  const observed = observeNodeAt(fdOps, parentFd, name);
  if (!fdNodeMatches(step.expected_before, observed)) {
    throw applyDiverged("a replaced project object no longer matches its exact precondition");
  }
  const result = fdOps.renameat(parentFd, encodeFdName(tempName), parentFd, encodeFdName(name));
  if (result < 0) {
    throw unexpectedSyscall("publish the project object", fdOps.errno());
  }
}

async function verifyPublishedPostcondition(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  desired: PipelineV2ApplyNode,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  const observed = observeNodeAt(fdOps, parentFd, name);
  if (!fdNodeMatches(desired, observed)) {
    throw applyDiverged("a published project object does not match its exact desired state");
  }
  await commitProgress(stepIndex);
}

async function executeSetMode(
  fdOps: ApplyFdOps,
  parentFd: number,
  name: string,
  step: PipelineV2ApplyPlanStep,
  stepIndex: number,
  commitProgress: (stepIndex: number) => Promise<void>,
): Promise<void> {
  const desired = step.desired_after;
  const expected = step.expected_before;
  if (desired === null || expected === null || desired.kind !== expected.kind || desired.kind === "symlink") {
    throw applyDiverged("the mode step must keep the object kind");
  }
  const isDirectory = desired.kind === "directory";
  const fd = fdOps.openat(
    parentFd,
    encodeFdName(name),
    FD_OPEN_RDONLY | (isDirectory ? FD_OPEN_DIRECTORY : 0) | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC,
    0,
  );
  if (fd < 0) {
    throw unexpectedSyscall("open the project object", fdOps.errno());
  }
  try {
    const stat = fdOps.fstat(fd);
    const actualKind = stat.isFile() ? "regular_file" : stat.isDirectory() ? "directory" : "other";
    if (actualKind !== desired.kind) {
      throw applyDiverged("the project object to re-mode changed its kind");
    }
    if (desired.kind === "regular_file") {
      const content = hashFdContent(fdOps, fd);
      if (content.byteCount !== desired.byte_count || content.sha256 !== desired.sha256) {
        throw applyDiverged("the project object to re-mode no longer carries its exact content");
      }
    }
    fdOps.fchmod(fd, desired.mode);
    fdOps.fsyncFd(fd);
  } finally {
    try {
      fdOps.closeFd(fd);
    } catch {
      // best effort
    }
  }
  const observed = observeNodeAt(fdOps, parentFd, name);
  if (!fdNodeMatches(desired, observed)) {
    throw applyDiverged("a re-moded project object does not match its exact desired state");
  }
  await commitProgress(stepIndex);
}
