/**
 * The versioned read-only agent-output content projection (P-2): the
 * bounded, digest-bound raw-byte slice of one accepted agent execution
 * output, served from the fixed orchestrator-owned activation artifact.
 *
 * The single public facade is this module. It reuses — never duplicates —
 * the established owners: the shared read-only durable-run load chain
 * (`loadDurablePipelineV2Run`, also serving `inspect`/`events`/`logs` and
 * the P-1 task/plan content), the durable accepted-output record as the
 * availability authority, the single data-plane
 * framing owner behind `acceptedOutputDigest` (which the one-handle
 * streaming helper shares),
 * and the typed one-handle streaming helper with its linearizing
 * identity token. There is no second loader, no second digest
 * implementation, no second content path and no write of any kind on any
 * path (no mutation lock, no temporary file, no project access, no
 * Session, no helper call).
 *
 * Authority: the caller names the run id, the execution index and the
 * declared output port id only (safe ids and safe integers) — never a
 * filesystem path; the artifact path is derived exclusively from the
 * fixed orchestrator-owned activation layout. The durable accepted-output
 * record must exist, its state must be a declared agent state of the
 * verified compiled pipeline whose output port is declared exactly once,
 * and the port kind must be a bytes kind (`file`/`json`; a declared
 * `directory` port is `unsupported_output_kind`). The served slice is
 * published only after the streaming-helper digest equals the durable
 * record digest; a cursor window beyond the artifact is `invalid_cursor`
 * and is evaluated strictly after artifact presence and digest integrity.
 *
 * Every success document is path-free (no run root, no artifact path) and
 * the output content bytes travel only inside `dataBase64`; every failure
 * carries exactly one closed producer reason with zero content bytes.
 */
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  resolveAcceptedOutputArtifact,
  streamAcceptedOutputSlice,
} from "./pipeline_v2_runtime.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import type { ResolvedPipelineV2 } from "./pipeline_v2.ts";
import {
  PIPELINE_V2_OUTPUT_CONTENT_REASONS,
  PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX,
} from "./pipeline_v2_content_reasons.ts";

export { PIPELINE_V2_OUTPUT_CONTENT_REASONS } from "./pipeline_v2_content_reasons.ts";

/** The default raw-byte slice size of the P-2 agent-output surface. */
export const PIPELINE_V2_AGENT_OUTPUT_DEFAULT_CHUNK_LIMIT = 65_536;

export type PipelineV2AgentOutputContentReason = (typeof PIPELINE_V2_OUTPUT_CONTENT_REASONS)[number];

const REASONS: ReadonlySet<PipelineV2AgentOutputContentReason> = new Set<
  PipelineV2AgentOutputContentReason
>(PIPELINE_V2_OUTPUT_CONTENT_REASONS);

/** The typed P-2 producer failure of the facade layer. */
export class PipelineV2AgentOutputContentError extends Error {
  declare readonly reason: PipelineV2AgentOutputContentReason;

  constructor(reason: PipelineV2AgentOutputContentReason, message: string) {
    if (!REASONS.has(reason)) {
      throw new TypeError(`unknown pipeline v2 agent output content error reason ${JSON.stringify(reason)}`);
    }
    super(message);
    this.name = "PipelineV2AgentOutputContentError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

/** The options of one read-only `orchestrator agent-output` invocation. */
export interface PipelineV2AgentOutputContentOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly executionIndex: number;
  readonly outputId: string;
  readonly offset?: number;
  readonly limit?: number;
}

/** The shared path-free failure document (exact top-level keys). */
export interface PipelineV2AgentOutputContentFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly reason: PipelineV2AgentOutputContentReason;
}

/** The versioned success document of one agent-output slice. */
export interface PipelineV2AgentOutputContentReport {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly executionIndex: number;
  readonly stateId: string;
  readonly outputId: string;
  readonly digest: string;
  readonly byteCount: number;
  readonly offset: number;
  readonly limit: number;
  readonly returnedByteCount: number;
  readonly nextOffset: number | null;
  readonly dataBase64: string;
}

export type PipelineV2AgentOutputContentOutcome =
  | PipelineV2AgentOutputContentReport
  | PipelineV2AgentOutputContentFailure;

function failure(
  runId: string,
  reason: PipelineV2AgentOutputContentReason,
): PipelineV2AgentOutputContentFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, reason });
}

function safeRunIdForDiagnostics(runId: string): string {
  return typeof runId === "string" && isPipelineV2SafeId(runId) ? runId : "";
}

interface VerifiedRead {
  readonly runId: string;
  readonly runRoot: string;
  readonly state: PipelineV2RunState;
  readonly pipeline: ResolvedPipelineV2;
}

/**
 * The one authoritative read of the verified durable run behind the P-2
 * surface (the P-1 shape): invalid option shapes that reach the facade
 * past the accepted CLI and routing guards are programmer/invariant
 * failures mapped to `internal_error` with zero content, and every real
 * `loadDurablePipelineV2Run` reason stays producer-public except the
 * reserved pre-runner `invalid_options` shape.
 */
async function verifiedRun(options: { runId: unknown; stateRoot: unknown }): Promise<VerifiedRead | PipelineV2AgentOutputContentFailure> {
  const runId = options.runId;
  if (typeof runId !== "string" || !isPipelineV2SafeId(runId)) {
    return failure("", "internal_error");
  }
  const stateRoot = options.stateRoot;
  if (typeof stateRoot !== "string" || stateRoot === "") {
    return failure(runId, "internal_error");
  }
  const read = await loadDurablePipelineV2Run({ runId, stateRoot });
  if (!read.ok) {
    const reason: PipelineV2AgentOutputContentReason =
      read.reason === "invalid_options" ? "internal_error" : read.reason;
    return failure(runId, reason);
  }
  return { runId: read.runId, runRoot: read.runRoot, state: read.state, pipeline: read.pipeline };
}

/**
 * The durable-record selection of one accepted agent execution output
 * (accepted preflight r4 §5). Fail-closed order:
 *
 * 1. the execution index must name a durable agent execution
 *    (`unknown_output` for an absent execution and for a decision
 *    execution — a decision carries no activation and no outputs);
 * 2. the execution must carry the output id in its durable accepted
 *    records (`unknown_output` otherwise);
 * 3. the execution's state must be a declared agent state of the verified
 *    compiled pipeline and the output id must be declared exactly once on
 *    it with a single type — a divergence the loader cannot catch is a
 *    durable-state divergence (`run_state_invalid`);
 * 4. a declared `directory` port has no raw-byte content contract
 *    (`unsupported_output_kind`), evaluated before any artifact read.
 */
function selectAcceptedOutput(
  state: PipelineV2RunState,
  pipeline: ResolvedPipelineV2,
  executionIndex: number,
  outputId: string,
): { stateId: string; digest: string; type: "file" | "json" } | PipelineV2AgentOutputContentFailure {
  const runId = state.run_id;
  const execution = state.executions.find((entry) => entry.index === executionIndex);
  if (execution === undefined || execution.type !== "agent") {
    return failure(runId, "unknown_output");
  }
  const record = execution.outputs?.find((entry) => entry.id === outputId);
  if (record === undefined) {
    return failure(runId, "unknown_output");
  }
  const stateSpec = pipeline.states.find((entry) => entry.id === execution.state_id);
  if (stateSpec === undefined || stateSpec.type !== "agent") {
    return failure(runId, "run_state_invalid");
  }
  const ports = stateSpec.outputs.filter((entry) => entry.id === outputId);
  if (ports.length !== 1) {
    return failure(runId, "run_state_invalid");
  }
  const type = ports[0]!.type;
  if (type === "directory") {
    return failure(runId, "unsupported_output_kind");
  }
  return { stateId: execution.state_id, digest: record.digest, type };
}

/**
 * Reads one bounded raw-byte slice of one accepted agent execution
 * output, or returns exactly one typed failure with zero content bytes.
 * Read-only end to end: the durable record is the availability authority,
 * the token mint linearizes the artifact identity at the open, and the
 * served slice leaves the facade only after its completed streaming
 * digest equals the durable record digest.
 */
export async function agentOutputContentPipelineV2Run(
  options: PipelineV2AgentOutputContentOptions,
): Promise<PipelineV2AgentOutputContentOutcome> {
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  const executionIndex = options.executionIndex;
  const outputId = options.outputId;
  const offset = options.offset ?? 0;
  const limit = options.limit ?? PIPELINE_V2_AGENT_OUTPUT_DEFAULT_CHUNK_LIMIT;
  if (
    typeof runId !== "string" ||
    typeof stateRoot !== "string" ||
    stateRoot === "" ||
    typeof executionIndex !== "number" ||
    !Number.isSafeInteger(executionIndex) ||
    executionIndex < 1 ||
    typeof outputId !== "string" ||
    !isPipelineV2SafeId(outputId) ||
    typeof offset !== "number" ||
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    typeof limit !== "number" ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX
  ) {
    return failure(safeRunIdForDiagnostics(runId), "internal_error");
  }
  try {
    const read = await verifiedRun({ runId, stateRoot });
    if ("ok" in read) {
      return read;
    }
    const selected = selectAcceptedOutput(read.state, read.pipeline, executionIndex, outputId);
    if ("ok" in selected) {
      return selected;
    }
    const token = await resolveAcceptedOutputArtifact(
      read.runRoot,
      executionIndex,
      selected.stateId,
      outputId,
      selected.type,
    );
    const streamed = await streamAcceptedOutputSlice(token, offset, limit);
    if (streamed.digest !== selected.digest) {
      return failure(read.runId, "artifact_modified");
    }
    if (offset > streamed.byteCount) {
      return failure(read.runId, "invalid_cursor");
    }
    const end = Math.min(offset + limit, streamed.byteCount);
    const returnedByteCount = end - offset;
    const nextOffset = end < streamed.byteCount ? end : null;
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId: read.runId,
      executionIndex,
      stateId: selected.stateId,
      outputId,
      digest: streamed.digest,
      byteCount: streamed.byteCount,
      offset,
      limit,
      returnedByteCount,
      nextOffset,
      dataBase64: Buffer.from(streamed.slice.buffer, streamed.slice.byteOffset, streamed.slice.byteLength).toString("base64"),
    }) satisfies PipelineV2AgentOutputContentReport as PipelineV2AgentOutputContentReport;
  } catch (cause) {
    if (cause instanceof PipelineV2AgentOutputContentError) {
      return failure(safeRunIdForDiagnostics(runId), cause.reason);
    }
    if (typeof cause === "object" && cause !== null && "reason" in cause) {
      const reason = (cause as { readonly reason?: unknown }).reason;
      if (typeof reason === "string" && REASONS.has(reason as PipelineV2AgentOutputContentReason)) {
        return failure(safeRunIdForDiagnostics(runId), reason as PipelineV2AgentOutputContentReason);
      }
    }
    return failure(safeRunIdForDiagnostics(runId), "internal_error");
  }
}
