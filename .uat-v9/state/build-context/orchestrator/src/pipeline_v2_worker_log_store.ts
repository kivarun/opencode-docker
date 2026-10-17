import {
  loadWorkerLogWithIo,
  publishWorkerLogWithIo,
  realWorkerLogStoreIo,
  verifyWorkerLogNamespaceWithIo,
  bindingOfParsedWorkerLog,
  PipelineV2WorkerLogStoreError,
  type PipelineV2WorkerLogStoreCandidate,
  type PipelineV2WorkerLogStoreFailureReason,
  type PipelineV2WorkerLogStoreOutcome,
  type PublishedPipelineV2WorkerLog,
  type WorkerLogStoreIo,
} from "./pipeline_v2_worker_log_store_internal.ts";
import {
  PipelineV2WorkerLogArtifactError,
  type ParsedWorkerLogArtifact,
} from "./pipeline_v2_worker_log.ts";
import type { WorkerLogCapture } from "./worker_operation.ts";


/**
 * Public facade of the worker-log artifact store.
 *
 * The whole filesystem protocol lives exactly once in the shared
 * immutable-document substrate; the adapter (and this facade) add only
 * the worker-log layout and error class. Production always runs the
 * single frozen `realWorkerLogStoreIo`; tests inject their own per-call
 * IO object — there is no mutable module-global seam and no installer.
 *
 * Layout: `<runRoot>/logs/<execution_index>.workerlog.json`, the `logs/`
 * directory mode 0700, artifact files mode 0600, canonical JSON bytes
 * with no trailing newline. `publishPipelineV2WorkerLog` returns `null`
 * without any filesystem effect for an unavailable capture.
 */
export {
  PipelineV2WorkerLogStoreError,
  realWorkerLogStoreIo,
  bindingOfParsedWorkerLog,
};
export type {
  PipelineV2WorkerLogStoreCandidate,
  PipelineV2WorkerLogStoreFailureReason,
  PipelineV2WorkerLogStoreOutcome,
  PublishedPipelineV2WorkerLog,
  WorkerLogStoreIo,
  ParsedWorkerLogArtifact,
};

export async function publishPipelineV2WorkerLog(
  runRoot: string,
  executionIndex: number,
  capture: WorkerLogCapture,
): Promise<PublishedPipelineV2WorkerLog | null> {
  return await publishWorkerLogWithIo(realWorkerLogStoreIo, runRoot, executionIndex, capture);
}

export async function loadPipelineV2WorkerLog(
  runRoot: string,
  executionIndex: number,
): Promise<ParsedWorkerLogArtifact | null> {
  return await loadWorkerLogWithIo(realWorkerLogStoreIo, runRoot, executionIndex);
}

export async function verifyPipelineV2WorkerLogNamespace(
  runRoot: string,
  requiredIndexes: readonly number[],
  toleratedIndexes: readonly number[] = [],
): Promise<void> {
  return await verifyWorkerLogNamespaceWithIo(
    realWorkerLogStoreIo,
    runRoot,
    requiredIndexes,
    toleratedIndexes,
  );
}

/**
 * One typed worker-log integrity defect of a durable binding, surfaced by
 * `verifyPipelineV2WorkerLogBindings`: `missing` — a captured/truncated
 * binding has no artifact at its fixed path; `modified` — the stored
 * artifact is damaged, foreign, or does not match its binding (including
 * a damaged tolerated orphan and any unexpected namespace entry).
 */
export interface PipelineV2WorkerLogBindingDefect {
  readonly kind: "missing" | "modified";
  readonly executionIndex: number;
}

/**
 * The one read-only integrity verification of every durable worker-log
 * binding against its run-root artifact. Every consumer that reads a
 * durable run state goes through this single implementation — the resume
 * restore, `inspect`, and `events` — so a damaged, foreign, missing, or
 * extra artifact is always the same typed defect and the verification is
 * always zero-write.
 */
export async function verifyPipelineV2WorkerLogBindings(
  runRoot: string,
  executions: ReadonlyArray<
    { readonly index: number } & Partial<Pick<import("./pipeline_v2_state.ts").PipelineV2AgentExecutionState, "worker_log">>
  >,
): Promise<PipelineV2WorkerLogBindingDefect | null> {
  const required: number[] = [];
  const tolerated: number[] = [];
  for (const execution of executions) {
    const binding = execution.worker_log;
    if (binding === undefined) {
      // A decision execution record carries no worker_log key at all.
      continue;
    }
    if (binding !== null && binding.status !== "unavailable") {
      required.push(execution.index);
    } else if (binding === null) {
      tolerated.push(execution.index);
    }
  }
  for (const index of required) {
    let parsed: ParsedWorkerLogArtifact | null;
    try {
      parsed = await loadPipelineV2WorkerLog(runRoot, index);
    } catch (cause) {
      // A damaged or foreign stored envelope is a modified defect; any
      // other store failure keeps the missing classification of the
      // established restore mapping.
      if (
        (cause instanceof PipelineV2WorkerLogStoreError && cause.reason === "conflict") ||
        cause instanceof PipelineV2WorkerLogArtifactError
      ) {
        return { kind: "modified", executionIndex: index };
      }
      if (cause instanceof PipelineV2WorkerLogStoreError) {
        return { kind: "missing", executionIndex: index };
      }
      throw cause;
    }
    const binding = executions[index - 1]?.worker_log;
    if (parsed === null || binding === undefined || binding === null || binding.status === "unavailable") {
      return { kind: "missing", executionIndex: index };
    }
    if (
      parsed.status !== binding.status ||
      parsed.sha256 !== binding.sha256 ||
      parsed.byte_count !== binding.byte_count ||
      parsed.truncated_daemon !== binding.truncated_daemon ||
      parsed.truncated_local !== binding.truncated_local
    ) {
      return { kind: "modified", executionIndex: index };
    }
  }
  for (const index of tolerated) {
    // A tolerated publish-then-bind orphan is read for integrity only: a
    // damaged or foreign envelope is a typed defect; a well-formed
    // artifact of any capture stays adoptable by the recovery.
    try {
      await loadPipelineV2WorkerLog(runRoot, index);
    } catch (cause) {
      if (
        (cause instanceof PipelineV2WorkerLogStoreError && cause.reason === "conflict") ||
        cause instanceof PipelineV2WorkerLogArtifactError
      ) {
        return { kind: "modified", executionIndex: index };
      }
      throw cause;
    }
  }
  try {
    await verifyPipelineV2WorkerLogNamespace(runRoot, required, tolerated);
  } catch (cause) {
    // A namespace-level defect (an unexpected entry shape, a substituted
    // logs directory) carries no specific execution index.
    if (cause instanceof PipelineV2WorkerLogStoreError) {
      return {
        kind: cause.reason === "conflict" ? "modified" : "missing",
        executionIndex: required.length > 0 ? required[0]! : 0,
      };
    }
    throw cause;
  }
  return null;
}
