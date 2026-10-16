import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  loadDurablePipelineV2Run,
  type PipelineV2InspectErrorReason,
} from "./pipeline_v2_inspect.ts";
import type { PipelineV2CommitProvenance, PipelineV2RunState } from "./pipeline_v2_state.ts";

/**
 * The versioned read-only lifecycle events API of one durable pipeline v2
 * run: the stable external projection of the schema-v8 `commit_history`.
 *
 * The single projection owner is `projectPipelineV2Events`: it accepts
 * only a loader-validated schema-v8 state (the coherence of the history
 * against the projection was proven by the state loader) plus the
 * pagination request, reads ONLY `commit_history`, never reconstructs
 * order from executions/transitions/controllers, accepts no sink, store
 * or filesystem handle, never mutates the state, never classifies
 * messages, never copies the command-kind vocabulary (the public entry
 * union IS the provenance union, passed through verbatim), and returns
 * the exact deep-frozen page.
 *
 * The cursor is an exclusive durable revision cursor: an event is included
 * iff `event.revision > after`; the limit applies after the exclusive
 * filter; `after === state.revision` succeeds with an empty page;
 * `after > state.revision` is the typed `invalid_cursor` failure; the
 * cursor is never an array offset and never depends on process memory or
 * a pagination session — one call reads one atomic snapshot, so a
 * concurrent mutation is either fully absent or fully present, and a
 * restart reproduces the same cursors and the same event bytes.
 *
 * This module is the only events layer. There is no second event schema,
 * no second command-kind list, no timestamps, no derived descriptions, no
 * phase-event merging, and no state mutation on any path.
 */

/** The closed events failure vocabulary: the inspect vocabulary plus the cursor. */
export type PipelineV2EventsErrorReason = PipelineV2InspectErrorReason | "invalid_cursor";

/**
 * The one typed events failure. `invalid_cursor` has exactly this closed
 * owner — it is never classified from message text, and no other layer
 * constructs it.
 */
export class PipelineV2EventsError extends Error {
  readonly reason!: "invalid_cursor";

  constructor(reason: "invalid_cursor", message: string) {
    super(message);
    this.name = "PipelineV2EventsError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** One projected event: the durable provenance entry, passed through verbatim. */
export type PipelineV2Event = PipelineV2CommitProvenance;

/** The exact deep-frozen page: the snapshot revision, the events, the cursor. */
export interface PipelineV2EventsPage {
  /** The revision of the single loader-validated snapshot the page was read from. */
  readonly revision: number;
  /** The projected events in history order (never reordered, never merged). */
  readonly events: readonly PipelineV2Event[];
  /**
   * The revision of the last returned event only when further events
   * exist in this snapshot; `null` otherwise (including the empty page).
   */
  readonly nextAfter: number | null;
}

/**
 * Projects one events page from a loader-validated schema-v8 state.
 * `after` must be a non-negative safe integer and `limit` a positive safe
 * integer (both surfaces parse them strictly before the loader, so a
 * hostile scalar here is a caller contract violation); a cursor beyond
 * the snapshot revision is the typed `invalid_cursor` failure with no
 * partial response. The returned page is deep-frozen and shares the
 * loader's frozen entry records — no value is copied, normalized or
 * reordered.
 */
export function projectPipelineV2Events(
  state: PipelineV2RunState,
  request: { readonly after: number; readonly limit: number },
): PipelineV2EventsPage {
  const after = request.after;
  const limit = request.limit;
  if (!Number.isSafeInteger(after) || after < 0) {
    throw new TypeError("the events cursor must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new TypeError("the events limit must be a positive safe integer");
  }
  if (after > state.revision) {
    throw new PipelineV2EventsError(
      "invalid_cursor",
      "the events cursor is beyond the observed revision of the run state",
    );
  }
  const events: PipelineV2Event[] = [];
  for (const entry of state.commit_history) {
    if (entry.revision > after) {
      events.push(entry);
      if (events.length === limit) {
        break;
      }
    }
  }
  const last = events[events.length - 1];
  const nextAfter = last !== undefined && last.revision < state.revision ? last.revision : null;
  return deepFreezeValue({ revision: state.revision, events, nextAfter });
}

/** The successful versioned events document (exact top-level keys). */
export interface PipelineV2EventsSuccess {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly runRoot: string;
  readonly revision: number;
  readonly events: readonly PipelineV2Event[];
  readonly nextAfter: number | null;
}

/** The events failure document (exact top-level keys; exitCode always 1). */
export interface PipelineV2EventsFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly runRoot: string | null;
  readonly reason: PipelineV2EventsErrorReason;
}

/** The events result union: one success document or exactly one typed failure. */
export type PipelineV2EventsOutcome = PipelineV2EventsSuccess | PipelineV2EventsFailure;

export interface PipelineV2EventsOptions {
  /** The run id (the shared safe-id grammar; never echoed when invalid). */
  readonly runId: string;
  /** The local orchestrator state root (absolute; read-only access). */
  readonly stateRoot: string;
  /** The exclusive durable revision cursor (default 0). */
  readonly after?: number;
  /** The page limit (default 100). */
  readonly limit?: number;
}

function failure(
  runId: string,
  runRoot: string | null,
  reason: PipelineV2EventsErrorReason,
): PipelineV2EventsFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, runRoot, reason });
}

/**
 * Reads the lifecycle events of one durable pipeline v2 run through the
 * one shared read-only load chain and returns the versioned projection,
 * or exactly one typed failure. There is no mutation lock, no Docker
 * Helper involvement, no durable change and no filesystem write on any
 * path: the run tree is byte-identical before and after the call, and a
 * repeated read over an unchanged run is byte-identical.
 */
export async function eventsPipelineV2Run(
  options: PipelineV2EventsOptions,
): Promise<PipelineV2EventsOutcome> {
  const read = await loadDurablePipelineV2Run(options);
  if (!read.ok) {
    return failure(read.runId, read.runRoot, read.reason);
  }
  const { runId, runRoot, state } = read;
  try {
    const page = projectPipelineV2Events(state, {
      after: options.after ?? 0,
      limit: options.limit ?? 100,
    });
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId,
      runRoot,
      revision: page.revision,
      events: page.events,
      nextAfter: page.nextAfter,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2EventsError) {
      return failure(runId, runRoot, cause.reason);
    }
    return failure(runId, runRoot, "internal_error");
  }
}
