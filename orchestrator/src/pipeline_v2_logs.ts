/**
 * The versioned read-only Worker Logs API: the stable external projection
 * of the durable schema-v9 worker-log bindings and their immutable
 * artifacts (`<runRoot>/logs/<execution_index>.workerlog.json`).
 *
 * This module is the single logs projection owner. It reuses — never
 * duplicates — the established owners: the shared read-only durable-run
 * load chain (`loadDurablePipelineV2Run`, also serving `inspect` and
 * `events`), the schema-v9 worker-log artifact parser/verifier and its
 * established raw-payload digest domain, and the shared read-only
 * binding integrity verifier. There is no second loader, no second
 * parser, no second digest implementation, no filesystem protocol of its
 * own and no write on any path: a missing run is never created, a damaged
 * artifact is never repaired, and a repeated read over an unchanged run
 * returns byte-identical JSON.
 *
 * The public contract:
 * - the list projection carries only agent executions in execution-index
 *   order; decision executions never appear;
 * - `worker_log: null` (before the terminal worker result) projects as
 *   `pending`; `{status: "unavailable"}` projects as `unavailable`;
 *   captured/truncated metadata comes from the durable binding and is
 *   confirmed against the artifact by the shared verifier;
 * - an execution read serves a status document for `pending`/
 *   `unavailable` and a bounded raw-byte slice of the verified payload
 *   as base64 for `captured`/`truncated`; offsets are raw payload bytes;
 *   `captured` never means upstream completeness and no `complete`
 *   status exists;
 * - content is served only after the full state-binding and artifact
 *   verification succeeded; corrupt/missing/extra artifact trees fail
 *   closed before any log bytes are produced;
 * - diagnostics are content-free: no payload, no cursor values, no
 *   paths, no envelope fields.
 *
 * The payload is secret-bearing and served verbatim by design
 * (documented in `docs/orchestration-workflow.md`); nothing in this
 * module filters, interprets or stores log content.
 */
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import type { PipelineV2InspectErrorReason } from "./pipeline_v2_inspect.ts";
import {
  loadPipelineV2WorkerLog,
  verifyPipelineV2WorkerLogBindings,
  PipelineV2WorkerLogStoreError,
  type PipelineV2WorkerLogBindingDefect,
} from "./pipeline_v2_worker_log_store.ts";
import { PipelineV2WorkerLogArtifactError } from "./pipeline_v2_worker_log.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";

/** The chunk-read reasons owned by this layer; everything else comes from
 * the shared loader and verifier vocabularies. */
export type PipelineV2LogsErrorReason =
  | PipelineV2InspectErrorReason
  | "execution_not_found"
  | "log_not_applicable"
  | "invalid_cursor";

/** The default and maximum raw-byte chunk limit (the CLI/HTTP contract). */
export const PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT = 65_536;
export const PIPELINE_V2_LOGS_MAX_CHUNK_LIMIT = 262_144;

/** The typed logs failure of the projection layer. */
export class PipelineV2LogsError extends Error {
  readonly reason: "execution_not_found" | "log_not_applicable" | "invalid_cursor";
  constructor(reason: "execution_not_found" | "log_not_applicable" | "invalid_cursor", message: string) {
    super(message);
    this.name = "PipelineV2LogsError";
    this.reason = reason;
  }
}

/** The metadata union of one agent execution's worker log. */
export type PipelineV2WorkerLogMetadata =
  | { readonly executionIndex: number; readonly stateId: string; readonly status: "pending" }
  | { readonly executionIndex: number; readonly stateId: string; readonly status: "unavailable" }
  | {
      readonly executionIndex: number;
      readonly stateId: string;
      readonly status: "captured" | "truncated";
      readonly sha256: string;
      readonly byteCount: number;
      readonly truncatedDaemon: boolean;
      readonly truncatedLocal: boolean;
    };

/** The versioned logs list of one durable run. */
export interface PipelineV2LogsPage {
  readonly revision: number;
  readonly logs: readonly PipelineV2WorkerLogMetadata[];
}

/**
 * Projects the worker-log metadata list of one authoritative state
 * snapshot: only agent executions, strictly in execution-index order
 * (the executions journal order), decision executions omitted.
 */
export function projectPipelineV2Logs(state: PipelineV2RunState): PipelineV2LogsPage {
  const logs: PipelineV2WorkerLogMetadata[] = [];
  for (const execution of state.executions) {
    if (execution.type !== "agent") {
      continue;
    }
    const binding = execution.worker_log;
    if (binding === null) {
      logs.push(deepFreezeValue({
        executionIndex: execution.index,
        stateId: execution.state_id,
        status: "pending" as const,
      }));
    } else if (binding.status === "unavailable") {
      logs.push(deepFreezeValue({
        executionIndex: execution.index,
        stateId: execution.state_id,
        status: "unavailable" as const,
      }));
    } else {
      logs.push(deepFreezeValue({
        executionIndex: execution.index,
        stateId: execution.state_id,
        status: binding.status,
        sha256: binding.sha256,
        byteCount: binding.byte_count,
        truncatedDaemon: binding.truncated_daemon,
        truncatedLocal: binding.truncated_local,
      }));
    }
  }
  return deepFreezeValue({ revision: state.revision, logs });
}

/**
 * The one read-only integrity verification of the run's worker-log
 * namespace before any metadata or content is served: every required
 * binding must match its immutable artifact, every tolerated binding
 * must have no artifact, and any extra final entry is a defect.
 */
async function verifyLogsIntegrity(
  runRoot: string,
  state: PipelineV2RunState,
): Promise<PipelineV2InspectErrorReason | null> {
  let defect: PipelineV2WorkerLogBindingDefect | null;
  try {
    defect = await verifyPipelineV2WorkerLogBindings(runRoot, state.executions);
  } catch (cause) {
    // The verifier's own typed store/artifact errors keep their
    // established mapping; anything else is the honest internal error of
    // the read chain.
    if (cause instanceof PipelineV2WorkerLogStoreError || cause instanceof PipelineV2WorkerLogArtifactError) {
      return "artifact_modified";
    }
    return "internal_error";
  }
  if (defect === null) {
    return null;
  }
  return defect.kind === "missing" ? "artifact_missing" : "artifact_modified";
}

/** The bounded raw-byte slice request of one verified artifact. */
export interface PipelineV2LogChunkRequest {
  readonly offset: number;
  readonly limit: number;
}

/** The status document of a pending or unavailable execution. */
export interface PipelineV2LogStatusResult {
  readonly kind: "status";
  readonly executionIndex: number;
  readonly stateId: string;
  readonly status: "pending" | "unavailable";
}

/** The bounded chunk result: base64 of exactly the returned raw bytes. */
export interface PipelineV2LogChunkResult {
  readonly kind: "chunk";
  readonly executionIndex: number;
  readonly stateId: string;
  readonly status: "captured" | "truncated";
  readonly sha256: string;
  readonly byteCount: number;
  readonly truncatedDaemon: boolean;
  readonly truncatedLocal: boolean;
  readonly offset: number;
  readonly dataBase64: string;
  readonly nextOffset: number | null;
}

export type PipelineV2LogExecutionResult = PipelineV2LogStatusResult | PipelineV2LogChunkResult;

/**
 * Reads one agent execution's worker log: the full integrity
 * verification runs before anything is resolved, a `pending` or
 * `unavailable` binding yields the status document without any content
 * field, and a `captured`/`truncated` binding yields the bounded
 * raw-byte slice of the verified payload. The artifact is loaded through
 * the single parser/verifier (which re-verifies the canonical envelope
 * and the payload digest internally); offsets are raw payload bytes,
 * never UTF-8 code points and never base64 characters.
 */
export async function readPipelineV2LogExecution(
  state: PipelineV2RunState,
  runRoot: string,
  executionIndex: number,
  chunk: PipelineV2LogChunkRequest,
): Promise<PipelineV2InspectErrorReason | PipelineV2LogExecutionResult> {
  const integrity = await verifyLogsIntegrity(runRoot, state);
  if (integrity !== null) {
    return integrity;
  }
  const execution = state.executions[executionIndex - 1];
  if (execution === undefined || execution.index !== executionIndex) {
    throw new PipelineV2LogsError(
      "execution_not_found",
      "the requested execution index does not name a durable execution",
    );
  }
  if (execution.type !== "agent") {
    throw new PipelineV2LogsError(
      "log_not_applicable",
      "decision executions carry no worker log",
    );
  }
  const binding = execution.worker_log;
  if (binding === null || binding.status === "unavailable") {
    return deepFreezeValue({
      kind: "status" as const,
      executionIndex: execution.index,
      stateId: execution.state_id,
      status: binding === null ? ("pending" as const) : ("unavailable" as const),
    });
  }
  if (chunk.offset > binding.byte_count) {
    throw new PipelineV2LogsError("invalid_cursor", "the requested offset is beyond the captured payload");
  }
  const parsed = await loadPipelineV2WorkerLog(runRoot, executionIndex);
  if (parsed === null) {
    return "artifact_missing";
  }
  const end = Math.min(chunk.offset + chunk.limit, binding.byte_count);
  const nextOffset = end < binding.byte_count ? end : null;
  return deepFreezeValue({
    kind: "chunk" as const,
    executionIndex: execution.index,
    stateId: execution.state_id,
    status: parsed.status,
    sha256: parsed.sha256,
    byteCount: parsed.byte_count,
    truncatedDaemon: parsed.truncated_daemon,
    truncatedLocal: parsed.truncated_local,
    offset: chunk.offset,
    dataBase64: Buffer.from(parsed.payload.subarray(chunk.offset, end)).toString("base64"),
    nextOffset,
  });
}

/** The options of one read-only `orchestrator logs` invocation. */
export interface PipelineV2LogsOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly executionIndex?: number;
  readonly offset?: number;
  readonly limit?: number;
}

/** The failure document (the events/inspect failure shape). */
export interface PipelineV2LogsFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly runRoot: string | null;
  readonly reason: PipelineV2LogsErrorReason;
}

/** The versioned logs list document (schemaVersion 1). */
export interface PipelineV2LogsListOutcome {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly runRoot: string;
  readonly revision: number;
  readonly logs: readonly PipelineV2WorkerLogMetadata[];
}

/** The versioned execution document (schemaVersion 1). */
export type PipelineV2LogsExecutionOutcome =
  | {
      readonly ok: true;
      readonly exitCode: 0;
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly runRoot: string;
      readonly revision: number;
      readonly executionIndex: number;
      readonly stateId: string;
      readonly status: "pending" | "unavailable";
    }
  | {
      readonly ok: true;
      readonly exitCode: 0;
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly runRoot: string;
      readonly revision: number;
      readonly executionIndex: number;
      readonly stateId: string;
      readonly status: "captured" | "truncated";
      readonly sha256: string;
      readonly byteCount: number;
      readonly truncatedDaemon: boolean;
      readonly truncatedLocal: boolean;
      readonly offset: number;
      readonly dataBase64: string;
      readonly nextOffset: number | null;
    };

export type PipelineV2LogsOutcome = PipelineV2LogsListOutcome | PipelineV2LogsExecutionOutcome | PipelineV2LogsFailure;

/**
 * The fail-closed caller-scalar validation of one logs invocation:
 * execution mode requires a positive safe execution index, the offset a
 * non-negative safe integer and the limit a positive safe integer within
 * the fixed chunk bound. The checked run id is echoed only when it is
 * already a string; a hostile value never reaches a diagnostic.
 */
function validateLogsScalars(options: PipelineV2LogsOptions): "invalid" | null {
  if (options.executionIndex !== undefined) {
    const executionIndex = options.executionIndex;
    if (typeof executionIndex !== "number" || !Number.isSafeInteger(executionIndex) || executionIndex < 1) {
      return "invalid";
    }
    const offset = options.offset ?? 0;
    if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0) {
      return "invalid";
    }
    const limit = options.limit ?? PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT;
    if (
      typeof limit !== "number" ||
      !Number.isSafeInteger(limit) ||
      limit < 1 ||
      limit > PIPELINE_V2_LOGS_MAX_CHUNK_LIMIT
    ) {
      return "invalid";
    }
  } else if (options.offset !== undefined || options.limit !== undefined) {
    // The list mode accepts no cursor scalars (the CLI parser and the
    // HTTP layer already enforce this; the runner stays fail-closed).
    return "invalid";
  }
  return null;
}

function failure(
  runId: string,
  runRoot: string | null,
  reason: PipelineV2LogsErrorReason,
): PipelineV2LogsFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, runRoot, reason });
}

/**
 * Reads the worker logs of one durable pipeline v2 run through the one
 * shared read-only load chain and returns the versioned projection, or
 * exactly one typed failure. There is no mutation lock, no Docker Helper
 * involvement, no Launcher credential, no durable change and no
 * filesystem write on any path.
 */
export async function logsPipelineV2Run(
  options: PipelineV2LogsOptions,
): Promise<PipelineV2LogsOutcome> {
  const scalars = validateLogsScalars(options);
  if (scalars !== null) {
    return failure(typeof options.runId === "string" ? options.runId : "", null, "invalid_options");
  }
  const read = await loadDurablePipelineV2Run(options);
  if (!read.ok) {
    return failure(read.runId, read.runRoot, read.reason);
  }
  const { runId, runRoot, state } = read;
  try {
    if (options.executionIndex === undefined) {
      const integrity = await verifyLogsIntegrity(runRoot, state);
      if (integrity !== null) {
        return failure(runId, runRoot, integrity);
      }
      const page = projectPipelineV2Logs(state);
      return deepFreezeValue({
        ok: true as const,
        exitCode: 0 as const,
        schemaVersion: 1 as const,
        runId,
        runRoot,
        revision: page.revision,
        logs: page.logs,
      });
    }
    const result = await readPipelineV2LogExecution(
      state,
      runRoot,
      options.executionIndex,
      {
        offset: options.offset ?? 0,
        limit: options.limit ?? PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT,
      },
    );
    if (typeof result === "string") {
      return failure(runId, runRoot, result);
    }
    if (result.kind === "status") {
      return deepFreezeValue({
        ok: true as const,
        exitCode: 0 as const,
        schemaVersion: 1 as const,
        runId,
        runRoot,
        revision: state.revision,
        executionIndex: result.executionIndex,
        stateId: result.stateId,
        status: result.status,
      });
    }
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId,
      runRoot,
      revision: state.revision,
      executionIndex: result.executionIndex,
      stateId: result.stateId,
      status: result.status,
      sha256: result.sha256,
      byteCount: result.byteCount,
      truncatedDaemon: result.truncatedDaemon,
      truncatedLocal: result.truncatedLocal,
      offset: result.offset,
      dataBase64: result.dataBase64,
      nextOffset: result.nextOffset,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2LogsError) {
      return failure(runId, runRoot, cause.reason);
    }
    if (cause instanceof PipelineV2WorkerLogStoreError || cause instanceof PipelineV2WorkerLogArtifactError) {
      return failure(runId, runRoot, "artifact_modified");
    }
    return failure(runId, runRoot, "internal_error");
  }
}
