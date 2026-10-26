import { realpath } from "node:fs/promises";
import { isCleanAbsolutePath } from "./clean_path.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import type { ImmutableDocumentIo } from "./pipeline_v2_immutable_document_store_internal.ts";
import type { PipelineStateIo } from "./run_snapshot_store.ts";
import { RunSnapshotDurabilityError, RunSnapshotStoreError } from "./run_snapshot_store.ts";
import type { ProjectFileStreamIo } from "./pipeline_v2_project_stream_internal.ts";
import { FD_AT_FDCWD, FD_OPEN_CLOEXEC, FD_OPEN_DIRECTORY, FD_OPEN_NOFOLLOW, FD_OPEN_RDONLY, type ApplyFdOps } from "./pipeline_v2_apply_fd_internal.ts";
import type { PipelineV2ApplyPlan } from "./pipeline_v2_apply_plan_internal.ts";
import {
  PipelineV2ApplyStateError,
  validateApplyOperationState,
  type PipelineV2ApplyExecutionState,
  type PipelineV2ApplyOperationState,
} from "./pipeline_v2_apply_state_internal.ts";
import {
  computeApplyRootBindingDigest,
  commitApplyOperationState,
  loadApplyOperationForExecution,
  openApplyOperationStore,
  PipelineV2ProjectApplyError,
  type OperationStore,
} from "./pipeline_v2_project_apply_internal.ts";
import { isApplyCrashFault } from "./pipeline_v2_apply_crash_internal.ts";
import {
  directoryIsWritable,
  projectRootIsReadOnly,
  walkApplyPlanAgainstProject,
  PipelineV2ApplyStateMismatch,
  type PipelineV2ApplyMismatchKind,
} from "./pipeline_v2_apply_verify_internal.ts";
import {
  ApplyFilesystemUnsupportedError,
  ApplyPayloadMismatchError,
  executeApplySteps,
} from "./pipeline_v2_apply_exec_internal.ts";
import {
  PipelineV2ApplyProjectLockError,
  acquirePipelineV2ApplyProjectLock,
} from "./pipeline_v2_apply_project_lock_internal.ts";

/**
 * The orchestration owner of the project apply filesystem writer: the
 * single authority for the apply operation state transitions
 * (`prepared → applying → applied`, plus the `conflict` and
 * `recovery_required` refusals) and for the recovery classification.
 *
 * Fixed execution order (every refusal happens before the first project
 * mutation):
 *
 *   1. the prepared package is loaded read-only (plan, payloads, layout)
 *      — the run workspace, the provenance artifact and the Project
 *      Changes endpoint are never read;
 *   2. the project root is opened `O_NOFOLLOW`, its freshly resolved
 *      identity is bound against the package's opaque root-binding digest
 *      — the stored path is never an authority;
 *   3. the project lock is acquired (`flock` on the root descriptor) and
 *      the root identity is re-verified under the lock;
 *   4. the read-only filesystem and write-authority gates run;
 *   5. the full plan walk verifies the whole applicable pre-state
 *      (prepared) or the recovery state (applying) before the
 *      `applying` transition is durably committed;
 *   6. only then the first project write happens, one step at a time
 *      with per-step durable progress;
 *   7. the final verification walks every affected path at its planned
 *      final state before the immutable `applied` terminal.
 *
 * After the first durable project mutation the operation only rolls
 * forward: a divergence is `recovery_required` (a later attempt may
 * continue once the project state is provably recoverable), never a
 * rollback. Before any mutation a divergence is a clean `conflict` with
 * zero completed steps and a byte-identical project. A foreign object at
 * a committed temp name is never removed; it is a typed
 * `recovery_required` refusal that keeps the frontier intent.
 */

export const PIPELINE_V2_APPLY_EXECUTION_REASONS = [
  "invalid_options",
  "apply_not_prepared",
  "artifact_missing",
  "artifact_modified",
  "project_binding_mismatch",
  "project_busy",
  "project_read_only",
  "unsupported_filesystem",
  "unsupported_metadata",
  "unsupported_project_entry",
  "project_conflict",
  "recovery_required",
  "state_persist_failed",
  "internal_error",
] as const;
export type PipelineV2ApplyExecutionReason = (typeof PIPELINE_V2_APPLY_EXECUTION_REASONS)[number];

export class PipelineV2ApplyExecutionError extends Error {
  declare readonly reason: PipelineV2ApplyExecutionReason;

  constructor(reason: PipelineV2ApplyExecutionReason, message: string) {
    super(message);
    this.name = "PipelineV2ApplyExecutionError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

export interface PipelineV2ApplyExecutionComponents {
  readonly docIo: ImmutableDocumentIo;
  readonly stateIo: PipelineStateIo;
  readonly fileStreamIo: ProjectFileStreamIo;
  readonly fdOps: ApplyFdOps;
}

export interface PipelineV2ApplyExecutionOptions {
  /** Deployment-owned apply store root (mode 0700, real directory). */
  readonly applyStoreRoot: string;
  /** The caller-owned idempotency identity of the operation. */
  readonly applyId: string;
  /** The freshly resolved project id (must match the stored operation). */
  readonly projectId: string;
  /** The freshly resolved canonical registered project root. */
  readonly projectRoot: string;
}

export type PipelineV2ApplyExecutionResult =
  | {
      readonly ok: true;
      readonly outcome: "applied";
      readonly apply_id: string;
      readonly run_id: string;
      readonly project_id: string;
      readonly revision: number;
      readonly completed_step_count: number;
    }
  | {
      readonly ok: true;
      readonly outcome: "already_applied";
      readonly apply_id: string;
      readonly run_id: string;
      readonly project_id: string;
      readonly revision: number;
    }
  | {
      readonly ok: false;
      readonly reason: PipelineV2ApplyExecutionReason;
      readonly apply_id: string;
      readonly run_id: string;
      readonly project_id: string;
      readonly revision: number | null;
      readonly status: PipelineV2ApplyOperationState["status"] | null;
      readonly completed_step_count: number | null;
    };

function executionFailure(reason: PipelineV2ApplyExecutionReason, message: string): PipelineV2ApplyExecutionError {
  return new PipelineV2ApplyExecutionError(reason, message);
}

/** Maps one stored preparation reason onto the execution vocabulary. */
function mapStoredReason(reason: string | null): PipelineV2ApplyExecutionReason {
  if (reason !== null && (PIPELINE_V2_APPLY_EXECUTION_REASONS as readonly string[]).includes(reason)) {
    return reason as PipelineV2ApplyExecutionReason;
  }
  return "internal_error";
}

/** Maps one verification mismatch onto its typed refusal reason. */
function mapMismatch(mismatch: PipelineV2ApplyMismatchKind): PipelineV2ApplyExecutionReason {
  if (mismatch === "unsupported_metadata") {
    return "unsupported_metadata";
  }
  if (mismatch === "unsupported_project_entry") {
    return "unsupported_project_entry";
  }
  return "project_conflict";
}

/**
 * Runs one execution attempt of a prepared package against the freshly
 * resolved registered project. Every outcome is content-free; no project
 * path, plan path, payload byte, temp name or raw system message ever
 * enters a result or a diagnostic.
 */
export async function applyPreparedProjectCore(
  options: PipelineV2ApplyExecutionOptions,
  components: PipelineV2ApplyExecutionComponents,
): Promise<PipelineV2ApplyExecutionResult> {
  const { applyStoreRoot, applyId, projectId, projectRoot } = options;
  const { fdOps, stateIo, docIo, fileStreamIo } = components;
  if (!isPipelineV2SafeId(applyId) || !isPipelineV2SafeId(projectId)) {
    throw executionFailure("invalid_options", "the apply and project ids must be safe ids");
  }
  if (typeof applyStoreRoot !== "string" || !isCleanAbsolutePath(applyStoreRoot)) {
    throw executionFailure("invalid_options", "the apply store root must be a clean absolute path");
  }
  if (typeof projectRoot !== "string" || !isCleanAbsolutePath(projectRoot)) {
    throw executionFailure("invalid_options", "the project root must be a clean absolute path");
  }

  try {
    // 1. The prepared package (read-only; the run workspace is never read).
    let loaded: Awaited<ReturnType<typeof loadApplyOperationForExecution>>;
    try {
      loaded = await loadApplyOperationForExecution({ docIo, stateIo }, applyStoreRoot, applyId);
    } catch (cause) {
      if (cause instanceof PipelineV2ProjectApplyError) {
        const mapped: PipelineV2ApplyExecutionReason = (PIPELINE_V2_APPLY_EXECUTION_REASONS as readonly string[]).includes(cause.reason)
          ? (cause.reason as PipelineV2ApplyExecutionReason)
          : "internal_error";
        return executionRefusal(null, mapped, null);
      }
      throw cause;
    }
    const state = loaded.state;
    if (loaded.pkg === null) {
      return terminalResult(state);
    }
    if (state.project_id !== projectId) {
      return executionRefusal(state, "invalid_options", null);
    }
    const pkg = loaded.pkg;

    // 2. The freshly resolved project root, bound against the package.
    let canonicalRoot: string;
    try {
      canonicalRoot = await realpath(projectRoot);
    } catch {
      throw executionFailure("project_binding_mismatch", "the project root could not be resolved");
    }
    if (canonicalRoot !== projectRoot) {
      throw executionFailure("project_binding_mismatch", "the project root is not canonical");
    }
    const probeIdentity = inspectRootIdentity(fdOps, projectRoot);
  if (probeIdentity === null) {
    return executionRefusal(state, "project_binding_mismatch", null);
  }
    const binding = computeApplyRootBindingDigest(projectRoot, probeIdentity.dev, probeIdentity.ino);
    if (binding !== pkg.plan.project_root_binding) {
      throw executionFailure("project_binding_mismatch", "the project root does not match the prepared operation binding");
    }

    // 3. The project lock, then the identity re-verification under it.
    let acquisition: ReturnType<typeof acquirePipelineV2ApplyProjectLock>;
    try {
      acquisition = acquirePipelineV2ApplyProjectLock(projectRoot);
    } catch (cause) {
      if (cause instanceof PipelineV2ApplyProjectLockError) {
        return executionRefusal(
          state,
          cause.reason === "root_invalid" ? "project_binding_mismatch" : "internal_error",
          null,
        );
      }
      throw cause;
    }
    if (acquisition.busy) {
      return deepFreezeValue({
        ok: false,
        reason: "project_busy",
        apply_id: applyId,
        run_id: state.run_id,
        project_id: state.project_id,
        revision: state.revision,
        status: state.status,
        completed_step_count: state.execution?.completed_step_count ?? null,
      });
    }
    const lock = acquisition.lock;
    try {
      let lockedIdentity: { dev: number; ino: number };
      try {
        const stat = fdOps.fstat(lock.fd);
        lockedIdentity = { dev: stat.dev, ino: stat.ino };
      } catch {
        return executionRefusal(state, "project_binding_mismatch", null);
      }
      if (lockedIdentity.dev !== probeIdentity.dev || lockedIdentity.ino !== probeIdentity.ino) {
        return executionRefusal(state, "project_binding_mismatch", null);
      }
      const lockedCanonical = await realpath(projectRoot).catch(() => null);
      if (lockedCanonical !== projectRoot) {
        return executionRefusal(state, "project_binding_mismatch", null);
      }

      // 4. The read-only filesystem and write-authority gates.
      if (projectRootIsReadOnly(fdOps, lock.fd)) {
        return executionRefusal(state, "project_read_only", null);
      }
      if (!directoryIsWritable(fdOps, lock.fd)) {
        return executionRefusal(state, "project_read_only", null);
      }

      return await runLockedExecution(
        {
          store: openApplyOperationStore(applyStoreRoot, applyId, stateIo),
          state,
          plan: pkg.plan,
          payloadsRoot: pkg.payloads_root,
        },
        { fdOps, fileStreamIo, rootFd: lock.fd },
      );
    } finally {
      lock.release();
    }
  } catch (cause) {
    if (isApplyCrashFault(cause) || cause instanceof PipelineV2ApplyExecutionError) {
      throw cause;
    }
    return deepFreezeValue({
      ok: false,
      reason: "internal_error",
      apply_id: applyId,
      run_id: "",
      project_id: projectId,
      revision: null,
      status: null,
      completed_step_count: null,
    });
  }
}

/** Builds one typed refusal result from the last authoritative state. */
function executionRefusal(
  state: PipelineV2ApplyOperationState | null,
  reason: PipelineV2ApplyExecutionReason,
  completed: number | null,
): PipelineV2ApplyExecutionResult {
  return deepFreezeValue({
    ok: false,
    reason,
    apply_id: state?.apply_id ?? "",
    run_id: state?.run_id ?? "",
    project_id: state?.project_id ?? "",
    revision: state?.revision ?? null,
    status: state?.status ?? null,
    completed_step_count: completed ?? state?.execution?.completed_step_count ?? null,
  });
}

function inspectRootIdentity(fdOps: ApplyFdOps, projectRoot: string): { dev: number; ino: number } | null {
  let fd = -1;
  try {
    fd = fdOps.openat(FD_AT_FDCWD, encodeRootPath(projectRoot), FD_OPEN_RDONLY | FD_OPEN_DIRECTORY | FD_OPEN_NOFOLLOW | FD_OPEN_CLOEXEC, 0);
    if (fd < 0) {
      return null;
    }
    const stat = fdOps.fstat(fd);
    return { dev: stat.dev, ino: stat.ino };
  } finally {
    if (fd >= 0) {
      try {
        fdOps.closeFd(fd);
      } catch {
        // best effort
      }
    }
  }
}



/** Encodes one absolute path as a NUL-terminated openat buffer (root only). */
function encodeRootPath(path: string): Buffer {
  return Buffer.from(`${path}\0`, "utf8");
}

interface LockedExecutionInput {
  readonly store: OperationStore;
  readonly state: PipelineV2ApplyOperationState;
  readonly plan: PipelineV2ApplyPlan;
  readonly payloadsRoot: string;
}

async function runLockedExecution(
  loaded: LockedExecutionInput,
  context: { readonly fdOps: ApplyFdOps; readonly fileStreamIo: ProjectFileStreamIo; readonly rootFd: number },
): Promise<PipelineV2ApplyExecutionResult> {
  const { store, plan, payloadsRoot } = loaded;
  const { fdOps, fileStreamIo, rootFd } = context;
  let current = loaded.state;
  let completed = current.execution?.completed_step_count ?? 0;

  const commitState = async (
    next: PipelineV2ApplyOperationState,
    isAdvance: (reloaded: PipelineV2ApplyOperationState) => boolean,
  ): Promise<void> => {
    current = await commitApplyOperationState(store, next, current.revision, isAdvance);
  };

  const executionOf = (completedCount: number, activeStep: PipelineV2ApplyExecutionState["active_step"]): PipelineV2ApplyExecutionState =>
    deepFreezeValue({ completed_step_count: completedCount, active_step: activeStep });

  const failureResult = (reason: PipelineV2ApplyExecutionReason): PipelineV2ApplyExecutionResult =>
    deepFreezeValue({
      ok: false,
      reason,
      apply_id: current.apply_id,
      run_id: current.run_id,
      project_id: current.project_id,
      revision: current.revision,
      status: current.status,
      completed_step_count: completed,
    });

  const refuse = async (
    status: "conflict" | "recovery_required",
    reason: PipelineV2ApplyExecutionReason,
    keepActiveStep: boolean,
  ): Promise<PipelineV2ApplyExecutionResult> => {
    const before = current.revision;
    const next = validateApplyOperationState({
      ...current,
      revision: before + 1,
      status,
      failure_reason: reason,
      execution: executionOf(completed, keepActiveStep ? current.execution?.active_step ?? null : null),
    });
    try {
      await commitState(next, (reloaded) => reloaded.status === status && reloaded.revision >= before + 1);
    } catch (cause) {
      if (cause instanceof RunSnapshotDurabilityError) {
        current = validateApplyOperationState(cause.candidate);
      } else if (!(cause instanceof RunSnapshotStoreError)) {
        throw cause;
      }
      // A plain persistence failure of the refusal record leaves the
      // durable pre-refusal state; the typed result still reports the
      // refusal and a later attempt re-derives it.
    }
    return deepFreezeValue({
      ok: false,
      reason,
      apply_id: current.apply_id,
      run_id: current.run_id,
      project_id: current.project_id,
      revision: current.revision,
      status: current.status,
      completed_step_count: completed,
    });
  };

  try {
    let walk: ReturnType<typeof walkApplyPlanAgainstProject>;
    if (current.status === "prepared") {
      // 5a. The full preflight: the whole plan against the untouched
      // project, before any mutation and before the applying transition.
      try {
        walk = walkApplyPlanAgainstProject(fdOps, rootFd, plan, 0, { verifyApplicable: true });
      } catch (cause) {
        if (cause instanceof PipelineV2ApplyStateMismatch) {
          return await classifyRefusal(cause, 0, null);
        }
        throw cause;
      }
      const before = current.revision;
      await commitState(
        validateApplyOperationState({
          ...current,
          revision: before + 1,
          status: "applying",
          failure_reason: null,
          execution: executionOf(0, null),
        }),
        (reloaded) => reloaded.status === "applying" && reloaded.revision >= before + 1,
      );
    } else {
      // 5b. The recovery preflight: completed steps at their recorded
      // postconditions, pending steps at their projected preconditions.
      completed = current.execution?.completed_step_count ?? 0;
      try {
        walk = walkApplyPlanAgainstProject(fdOps, rootFd, plan, completed, { verifyApplicable: true });
      } catch (cause) {
        if (cause instanceof PipelineV2ApplyStateMismatch) {
          return await classifyRefusal(cause, completed, current.execution?.active_step ?? null);
        }
        throw cause;
      }
      if (current.status === "recovery_required") {
        // A soft retry: the project state is provably recoverable, so the
        // operation returns to applying and rolls forward.
        const before = current.revision;
        await commitState(
          validateApplyOperationState({
            ...current,
            revision: before + 1,
            status: "applying",
            failure_reason: null,
            execution: executionOf(completed, current.execution?.active_step ?? null),
          }),
          (reloaded) => reloaded.status === "applying" && reloaded.revision >= before + 1,
        );
      }
    }

    // 6. The step execution with per-step durable progress.
    try {
      await executeApplySteps({
        fdOps,
        fileStreamIo,
        rootFd,
        plan,
        payloadsRoot,
        startStep: completed + 1,
        stepsByPath: walk.stepsByPath,
        initialActiveStep: current.execution?.active_step ?? null,
        commitTempIntent: async (stepIndex, tempName) => {
          const before = current.revision;
          await commitState(
            validateApplyOperationState({
              ...current,
              revision: before + 1,
              execution: executionOf(
                completed,
                deepFreezeValue({ step_index: stepIndex, temp_name: tempName, temp_identity: null }),
              ),
            }),
            (reloaded) =>
              reloaded.execution?.active_step?.step_index === stepIndex &&
              reloaded.execution.active_step.temp_name === tempName &&
              reloaded.execution.active_step.temp_identity === null &&
              reloaded.revision >= before + 1,
          );
        },
        commitTempIdentity: async (stepIndex, tempName, dev, ino) => {
          const before = current.revision;
          await commitState(
            validateApplyOperationState({
              ...current,
              revision: before + 1,
              execution: executionOf(
                completed,
                deepFreezeValue({ step_index: stepIndex, temp_name: tempName, temp_identity: deepFreezeValue({ dev, ino }) }),
              ),
            }),
            (reloaded) =>
              reloaded.execution?.active_step?.step_index === stepIndex &&
              reloaded.execution.active_step.temp_name === tempName &&
              reloaded.execution.active_step.temp_identity !== null &&
              reloaded.execution.active_step.temp_identity.dev === dev &&
              reloaded.execution.active_step.temp_identity.ino === ino &&
              reloaded.revision >= before + 1,
          );
        },
        commitProgress: async (stepIndex) => {
          completed = stepIndex;
          const before = current.revision;
          await commitState(
            validateApplyOperationState({
              ...current,
              revision: before + 1,
              execution: executionOf(stepIndex, null),
            }),
            (reloaded) =>
              (reloaded.execution?.completed_step_count ?? 0) >= stepIndex && reloaded.revision >= before + 1,
          );
        },
      });
    } catch (cause) {
      if (isApplyCrashFault(cause)) {
        throw cause;
      }
      if (cause instanceof PipelineV2ApplyStateMismatch) {
        return await classifyRefusal(cause, completed, current.execution?.active_step ?? null);
      }
      if (cause instanceof ApplyPayloadMismatchError) {
        return failureResult("artifact_modified");
      }
      if (cause instanceof ApplyFilesystemUnsupportedError) {
        return failureResult("unsupported_filesystem");
      }
      if (
        cause instanceof RunSnapshotStoreError ||
        cause instanceof RunSnapshotDurabilityError ||
        cause instanceof PipelineV2ApplyStateError ||
        (cause instanceof PipelineV2ProjectApplyError && cause.reason === "state_persist_failed")
      ) {
        return failureResult("state_persist_failed");
      }
      return failureResult("internal_error");
    }

    // 7. The final verification, then the immutable applied terminal.
    try {
      walkApplyPlanAgainstProject(fdOps, rootFd, plan, plan.steps.length, { verifyApplicable: false });
    } catch (cause) {
      if (cause instanceof PipelineV2ApplyStateMismatch) {
        return await classifyRefusal(cause, completed, null);
      }
      throw cause;
    }
    const before = current.revision;
    await commitState(
      validateApplyOperationState({
        ...current,
        revision: before + 1,
        status: "applied",
        failure_reason: null,
        execution: executionOf(plan.steps.length, null),
      }),
      (reloaded) => reloaded.status === "applied" && reloaded.revision >= before + 1,
    );
    return deepFreezeValue({
      ok: true,
      outcome: "applied",
      apply_id: current.apply_id,
      run_id: current.run_id,
      project_id: current.project_id,
      revision: current.revision,
      completed_step_count: plan.steps.length,
    });
  } catch (cause) {
    if (isApplyCrashFault(cause)) {
      throw cause;
    }
    if (
      cause instanceof RunSnapshotStoreError ||
      cause instanceof RunSnapshotDurabilityError ||
      cause instanceof PipelineV2ApplyStateError ||
      (cause instanceof PipelineV2ProjectApplyError && cause.reason === "state_persist_failed")
    ) {
        return failureResult("state_persist_failed");
    }
    throw cause;
  }

  /** Maps a mismatch onto the conflict/recovery refusal by durable progress. */
  async function classifyRefusal(
    mismatch: PipelineV2ApplyStateMismatch,
    completedCount: number,
    activeStep: PipelineV2ApplyExecutionState["active_step"],
  ): Promise<PipelineV2ApplyExecutionResult> {
    if (mismatch.mismatch === "foreign_temp") {
      // A foreign object at a committed temp name is never removed; the
      // frontier intent stays for manual intervention.
      return await refuse("recovery_required", "recovery_required", true);
    }
    if (completedCount === 0) {
      return await refuse("conflict", mapMismatch(mismatch.mismatch), false);
    }
    return await refuse("recovery_required", "recovery_required", activeStep !== null);
  }
}

/**
 * The terminal-result projection of a stored journal state: an applied
 * operation reports `already_applied`; a conflicted, recovery-required or
 * failed operation reports its stored typed refusal without any retry.
 */
function terminalResult(state: PipelineV2ApplyOperationState): PipelineV2ApplyExecutionResult {
  if (state.status === "applied") {
    return deepFreezeValue({
      ok: true,
      outcome: "already_applied",
      apply_id: state.apply_id,
      run_id: state.run_id,
      project_id: state.project_id,
      revision: state.revision,
    });
  }
  return deepFreezeValue({
    ok: false,
    reason: mapStoredReason(state.failure_reason),
    apply_id: state.apply_id,
    run_id: state.run_id,
    project_id: state.project_id,
    revision: state.revision,
    status: state.status,
    completed_step_count: state.execution?.completed_step_count ?? null,
  });
}
