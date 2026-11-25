import {
  loadDurablePipelineV2Run,
  type PipelineV2InspectErrorReason,
} from "./pipeline_v2_inspect.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/**
 * The versioned read-only execution→generation/iteration binding
 * projection of one durable pipeline v2 run (S5-P0): the authoritative
 * answer to "which durable generation and iteration did each settled
 * execution actually run in", produced exclusively by the reducer's own
 * start-instant writes (durable schema v13) and the loader's own
 * positional replay — never re-derived in this projection.
 *
 * The single projection owner is this module. It reads only the
 * loader-validated schema-v13 state through the ONE shared read-only load
 * chain (`loadDurablePipelineV2Run` — there is no second loader), starts
 * no worker, takes no mutation lock and writes nothing. Every value is
 * content-free (safe ids, closed enums, bounded integers); publications,
 * session ids, worker-log bindings, terminal/failure projections and
 * status are deliberately not part of the record shape.
 *
 * Pagination is revision-pinned exactly like the identity projection:
 * every continuation re-proves its revision against the revision of the
 * single atomic snapshot the request loaded, and a mismatch is the typed
 * `stale_snapshot` failure with zero partial response. The boundedness
 * cap applies to BOTH full production envelopes (the CLI document and the
 * HTTP body) and is probed with literal production serializers, not
 * approximations.
 */

/** The execution-bindings projection schema version. */
export const PIPELINE_V2_EXECUTION_BINDINGS_SCHEMA_VERSION = 1;

/** The default page record limit. */
export const PIPELINE_V2_EXECUTION_BINDINGS_DEFAULT_LIMIT = 100;

/** The maximum page record limit. */
export const PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT = 200;

/**
 * The byte cap of ONE page: the complete serialized page document of BOTH
 * production surfaces must fit within this limit. The cap is probed from
 * the second record of each page; the bounded record grammar makes an
 * oversized first record unproducible, so a page always makes progress.
 */
export const PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT = 262144;

/** One projected binding record (exact own keys; nulls are normalized). */
export interface PipelineV2ExecutionBindingRecord {
  readonly record: "execution_binding";
  readonly execution_index: number;
  /** NULL exactly when no generation was open at the execution's start. */
  readonly generation_index: null | number;
  /** NULL exactly when the execution carried no open iteration. */
  readonly iteration_index: null | number;
}

/**
 * The closed execution-bindings failure vocabulary. The load-chain
 * reasons come verbatim from the one shared read-only loader
 * (`invalid_options` is possible only before the run root is resolved and
 * is surfaced as `internal_error` — the wired surfaces never pass option
 * errors here); `invalid_cursor` and `stale_snapshot` are the
 * cursor/revision-contract reasons owned by this module's page projector.
 */
export type PipelineV2ExecutionBindingsErrorReason =
  | "run_not_found"
  | "run_state_invalid"
  | "pipeline_mismatch"
  | "invalid_cursor"
  | "stale_snapshot"
  | "internal_error";

/** The cursor/revision-contract error of the execution-bindings projection. */
export class PipelineV2ExecutionBindingsError extends Error {
  readonly reason!: "invalid_cursor" | "stale_snapshot";

  constructor(reason: "invalid_cursor" | "stale_snapshot", message: string) {
    super(message);
    this.name = "PipelineV2ExecutionBindingsError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

const normalized = <T>(value: T | undefined | null): T | null =>
  value === undefined || value === null ? null : value;

/**
 * Builds the full binding stream of one loader-validated schema-v13 state:
 * exactly one `execution_binding` record per durable execution, in
 * durable journal order (executions are the journal order), with the
 * reducer-written riding values normalized to `null` when absent. The
 * values are read directly from the durable execution records — the
 * projection never re-derives a generation or iteration from the
 * ledgers, the anchors or any replay.
 */
export function buildPipelineV2ExecutionBindingRecords(
  state: PipelineV2RunState,
): readonly PipelineV2ExecutionBindingRecord[] {
  const records: PipelineV2ExecutionBindingRecord[] = state.executions.map((execution) =>
    deepFreezeValue({
      record: "execution_binding" as const,
      execution_index: execution.index,
      generation_index: normalized(execution.generation_index),
      iteration_index: normalized(execution.iteration_index),
    }),
  );
  return deepFreezeValue(records);
}

/** The request of one execution-bindings page (the surfaces own the grammar). */
export interface PipelineV2ExecutionBindingsPageRequest {
  /** The exclusive cursor (default 0). */
  readonly after?: number;
  /** The revision pin; REQUIRED for every continuation. */
  readonly revision?: number;
  /** The page record limit (default 100, maximum 200). */
  readonly limit?: number;
}

/** The exact deep-frozen page: the snapshot revision, the records, the cursor. */
export interface PipelineV2ExecutionBindingsPage {
  /** The revision of the single loader-validated snapshot the page was read from. */
  readonly revision: number;
  /** The exclusive cursor this page starts at. */
  readonly after: number;
  readonly records: readonly PipelineV2ExecutionBindingRecord[];
  /** `after + records.length` exactly when a tail exists; `null` otherwise. */
  readonly nextAfter: number | null;
}

/**
 * Projects one execution-bindings page from a loader-validated state with
 * the exact cursor/revision contract of the identity projection: the
 * revision pin is checked BEFORE the cursor-range checks, any provided
 * revision — including on the first page — must equal the loaded
 * snapshot's revision, `after` beyond the stream is the typed
 * `invalid_cursor` refusal, and the page honors both stop rules (record
 * cap, full-document byte cap on both full production envelopes) with the
 * deterministic stop-before partition.
 */
export function projectPipelineV2ExecutionBindings(
  state: PipelineV2RunState,
  request: PipelineV2ExecutionBindingsPageRequest,
): PipelineV2ExecutionBindingsPage {
  if (
    request.after !== undefined &&
    (typeof request.after !== "number" || !Number.isSafeInteger(request.after) || request.after < 0)
  ) {
    throw new PipelineV2ExecutionBindingsError(
      "invalid_cursor",
      "the execution-bindings cursor must be a non-negative safe integer",
    );
  }
  if (
    request.limit !== undefined &&
    (typeof request.limit !== "number" ||
      !Number.isSafeInteger(request.limit) ||
      request.limit < 1 ||
      request.limit > PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT)
  ) {
    throw new PipelineV2ExecutionBindingsError(
      "invalid_cursor",
      `the execution-bindings limit must be a positive safe integer up to ${PIPELINE_V2_EXECUTION_BINDINGS_MAX_LIMIT}`,
    );
  }
  if (request.revision !== undefined && request.revision !== null) {
    if (
      typeof request.revision !== "number" ||
      !Number.isSafeInteger(request.revision) ||
      request.revision < 1
    ) {
      throw new PipelineV2ExecutionBindingsError(
        "invalid_cursor",
        "the execution-bindings revision pin must be a positive safe integer",
      );
    }
    if (request.revision !== state.revision) {
      throw new PipelineV2ExecutionBindingsError(
        "stale_snapshot",
        "the execution-bindings revision pin does not match the loaded durable snapshot; restart the stream from the first page",
      );
    }
  }
  const after = request.after ?? 0;
  const limit = request.limit ?? PIPELINE_V2_EXECUTION_BINDINGS_DEFAULT_LIMIT;
  const records = buildPipelineV2ExecutionBindingRecords(state);
  if (after > records.length) {
    throw new PipelineV2ExecutionBindingsError(
      "invalid_cursor",
      "the execution-bindings cursor is beyond the projected binding stream",
    );
  }
  if (after === records.length) {
    return deepFreezeValue({ revision: state.revision, after, records: [], nextAfter: null });
  }
  const page: PipelineV2ExecutionBindingRecord[] = [];
  for (const record of records.slice(after)) {
    // The stop-before partition probes BOTH full production envelopes for
    // the candidate page: the first record is always admitted (the bounded
    // record grammar makes an oversized first record unproducible; the
    // boundary battery proves the single-record freedom over the exact
    // maximum envelopes), from the second record a candidate is rejected
    // when either full envelope would exceed the cap. The probe documents
    // use the maximum-bound run id, so the candidate size is an upper
    // bound of both real surfaces at every step.
    if (page.length >= 1) {
      const candidateRecords = [...page, record];
      const nextAfterCandidate = after + candidateRecords.length;
      const cliBytes = Buffer.byteLength(
        `${executionBindingsCliEnvelopeJson(state.revision, after, candidateRecords, nextAfterCandidate)}\n`,
        "utf8",
      );
      const httpBytes = Buffer.byteLength(
        executionBindingsHttpEnvelopeJson(state.revision, after, candidateRecords, nextAfterCandidate),
        "utf8",
      );
      if (cliBytes > PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT || httpBytes > PIPELINE_V2_EXECUTION_BINDINGS_PAGE_BYTE_LIMIT) {
        break;
      }
    }
    page.push(record);
    if (page.length >= limit) {
      break;
    }
  }
  const nextAfter = after + page.length;
  return deepFreezeValue({
    revision: state.revision,
    after,
    records: page,
    nextAfter: nextAfter >= records.length ? null : nextAfter,
  });
}

/** The canonical CLI envelope JSON for one candidate page (the partition probe). */
function executionBindingsCliEnvelopeJson(
  revision: number,
  after: number,
  records: readonly PipelineV2ExecutionBindingRecord[],
  nextAfter: number | null,
): string {
  return canonicalJson({
    ok: true,
    exitCode: 0,
    schemaVersion: PIPELINE_V2_EXECUTION_BINDINGS_SCHEMA_VERSION,
    runId: "x".repeat(128),
    revision,
    after,
    records,
    nextAfter,
  });
}

/** The canonical HTTP envelope JSON for one candidate page (the partition probe). */
function executionBindingsHttpEnvelopeJson(
  revision: number,
  after: number,
  records: readonly PipelineV2ExecutionBindingRecord[],
  nextAfter: number | null,
): string {
  return canonicalJson({
    schema_version: PIPELINE_V2_EXECUTION_BINDINGS_SCHEMA_VERSION,
    kind: "execution_bindings",
    run_id: "x".repeat(128),
    revision,
    after,
    records,
    next_after: nextAfter,
  });
}

/** The execution-bindings success document (exact own keys; path-free — no runRoot). */
export interface PipelineV2ExecutionBindingsSuccess {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly revision: number;
  readonly after: number;
  readonly records: readonly PipelineV2ExecutionBindingRecord[];
  readonly nextAfter: number | null;
}

/** The failure document (exact own keys; exitCode always 1; path-free). */
export interface PipelineV2ExecutionBindingsFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly reason: PipelineV2ExecutionBindingsErrorReason;
}

/** The result union: one page document or exactly one typed failure. */
export type PipelineV2ExecutionBindingsOutcome =
  | PipelineV2ExecutionBindingsSuccess
  | PipelineV2ExecutionBindingsFailure;

export interface PipelineV2ExecutionBindingsOptions {
  /** The run id (the shared safe-id grammar; never echoed when invalid). */
  readonly runId: string;
  /** The local orchestrator state root (absolute; read-only access). */
  readonly stateRoot: string;
  /** The exclusive momentary cursor (default 0; REQUIRED with it: the revision pin). */
  readonly after?: number;
  /** The revision pin (REQUIRED for every continuation `after > 0`). */
  readonly revision?: number;
  /** The page limit (default 100, maximum 200). */
  readonly limit?: number;
}

const executionBindingsFailure = (
  runId: string,
  reason: PipelineV2ExecutionBindingsErrorReason,
): PipelineV2ExecutionBindingsFailure =>
  deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, reason });

/**
 * Reads one page of the durable execution-binding projection of one
 * pipeline v2 run through the one shared read-only load chain. There is
 * no mutation lock, no Docker Helper involvement, no durable change and
 * no filesystem write on any path; the run tree is byte-identical before
 * and after the call and a repeated read over an unchanged run is
 * byte-identical.
 */
export async function executionBindingsPipelineV2Run(
  options: PipelineV2ExecutionBindingsOptions,
): Promise<PipelineV2ExecutionBindingsOutcome> {
  const runId = options.runId;
  if (afterWithoutRevision(options.after, options.revision) !== undefined) {
    return executionBindingsFailure(typeof runId === "string" ? runId : "", "invalid_cursor");
  }
  const read = await loadDurablePipelineV2Run(options as { runId: unknown; stateRoot: string });
  if (!read.ok) {
    const reason: PipelineV2ExecutionBindingsErrorReason =
      read.reason === "invalid_options"
        ? "internal_error"
        : read.reason === "artifact_missing" || read.reason === "artifact_modified"
          ? "run_state_invalid"
          : read.reason;
    return executionBindingsFailure(read.runId, reason);
  }
  const { runId: safeRunId, state } = read;
  try {
    const page = projectPipelineV2ExecutionBindings(state, {
      after: options.after,
      revision: options.revision,
      limit: options.limit,
    });
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      // The constant is declared as the literal 1, so this stays the exact
      // literal type the versioned document contract requires.
      schemaVersion: 1 as const,
      runId: safeRunId,
      revision: page.revision,
      after: page.after,
      records: page.records,
      nextAfter: page.nextAfter,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2ExecutionBindingsError) {
      return executionBindingsFailure(safeRunId, cause.reason);
    }
    return executionBindingsFailure(safeRunId, "internal_error");
  }
}

/**
 * The cross-field grammar: every continuation (`after > 0`) must carry its
 * revision pin. A violation is the typed cursor refusal, regardless of the
 * loaded snapshot (no filesystem effect happens before this check).
 */
function afterWithoutRevision(
  after: number | undefined,
  revision: number | undefined,
): "invalid_cursor" | undefined {
  if ((after ?? 0) > 0 && revision === undefined) {
    return "invalid_cursor";
  }
  return undefined;
}
