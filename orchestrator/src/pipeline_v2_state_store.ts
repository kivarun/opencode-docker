import {
  expectSafeId,
  parsePipelineV2RunState,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  defaultPipelineStateIo,
  retargetSnapshotStoreError,
  RunSnapshotStore,
  runSnapshotStatePath,
  type PipelineStateIo,
} from "./run_snapshot_store.ts";

/**
 * Typed adapter of the shared durable snapshot store substrate
 * (`run_snapshot_store.ts`) for the pipeline v2 run state contract
 * (`pipeline_v2_state.ts`, schema version 3). The substrate owns the layout,
 * the atomic commit protocol, permissions, symlink refusals, and the
 * in-process commit serialization; this adapter only pins the v2 parser,
 * the v2 snapshot type, and the v2 typed commit outcomes:
 *
 * - `PipelineV2RunStateStoreError` (`not_committed`): the rename did not
 *   happen; the previous committed snapshot remains authoritative
 *   byte-for-byte.
 * - `PipelineV2RunStateDurabilityError` (`durability_unknown`): the rename
 *   succeeded, but the post-rename durability confirmation failed; the
 *   candidate revision is already visible at the state path and its survival
 *   across a crash is unknown. The error carries the exact candidate
 *   revision and snapshot.
 *
 * Parser and validator errors (`PipelineV2StateError`) propagate unchanged;
 * they are never masked as I/O errors. The run id obeys the shared schema-v3
 * safe-id contract (`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, no `..`) and is
 * validated before any filesystem I/O, both here and in
 * `pipelineV2RunStatePath()`. Multi-process locking is intentionally out of
 * scope.
 */

export class PipelineV2RunStateStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2RunStateStoreError";
  }
}

export class PipelineV2RunStateDurabilityError extends PipelineV2RunStateStoreError {
  readonly revision: number;
  readonly candidate: PipelineV2RunState;

  constructor(revision: number, candidate: PipelineV2RunState, message: string) {
    super(message);
    this.name = "PipelineV2RunStateDurabilityError";
    this.revision = revision;
    this.candidate = candidate;
  }
}

const V2_DOCUMENT_LABEL = "pipeline v2 run state";

export function pipelineV2RunStatePath(stateRoot: string, runId: string): string {
  // The path helper validates the run id with the shared schema-v3 safe-id
  // contract itself, never relying on the store constructor having run.
  expectSafeId(runId, "pipeline v2 run id");
  return runSnapshotStatePath(stateRoot, runId);
}

function retarget(cause: unknown): unknown {
  return retargetSnapshotStoreError<PipelineV2RunState>(cause, {
    notCommitted: (message) => new PipelineV2RunStateStoreError(message),
    durabilityUnknown: (revision, candidate, message) =>
      new PipelineV2RunStateDurabilityError(
        revision,
        candidate as PipelineV2RunState,
        message,
      ),
  });
}

export interface PipelineV2RunStateStoreParams {
  stateRoot: string;
  runId: string;
  io?: PipelineStateIo;
}

export class PipelineV2RunStateStore {
  private readonly inner: RunSnapshotStore<PipelineV2RunState>;

  constructor(params: { stateRoot: string; runId: string; io?: PipelineStateIo }) {
    const runId = expectSafeId(params.runId, "pipeline v2 run id");
    this.inner = new RunSnapshotStore<PipelineV2RunState>(params.stateRoot, runId, {
      parseSnapshot: parsePipelineV2RunState,
      documentLabel: V2_DOCUMENT_LABEL,
    }, params.io ?? defaultPipelineStateIo);
  }

  get path(): string {
    return this.inner.path;
  }

  /**
   * Loads the committed snapshot, or null when no run state exists yet.
   * Rejects symlinks and every document the exact-field validator rejects.
   */
  async load(): Promise<PipelineV2RunState | null> {
    try {
      return await this.inner.load();
    } catch (cause) {
      throw retarget(cause);
    }
  }

  /** First write of a run; refuses to overwrite an existing run. */
  async create(snapshot: PipelineV2RunState): Promise<void> {
    try {
      await this.inner.create(snapshot);
    } catch (cause) {
      throw retarget(cause);
    }
  }

  /**
   * Subsequent write: the committed on-disk revision must equal
   * `expectedRevision` and `snapshot.revision` must be exactly one higher.
   */
  async commit(snapshot: PipelineV2RunState, expectedRevision: number): Promise<void> {
    try {
      await this.inner.commit(snapshot, expectedRevision);
    } catch (cause) {
      throw retarget(cause);
    }
  }
}
