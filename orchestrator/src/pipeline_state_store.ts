import {
  parsePipelineRunState,
  type PipelineRunState,
} from "./pipeline_state.ts";
import {
  defaultPipelineStateIo,
  retargetSnapshotStoreError,
  RunSnapshotStore,
  type PipelineStateIo,
} from "./run_snapshot_store.ts";

/**
 * Durable, atomically updated store for the pipeline run state, backed by the
 * schema-agnostic snapshot store substrate in `run_snapshot_store.ts` (which
 * owns the layout, the commit protocol, permissions, symlink refusals, and
 * the in-process commit serialization). This file is the compatibility
 * adapter for the pipeline v1 contract: it keeps the public names, typed
 * error outcomes, messages, and observable semantics of the original
 * `pipeline_state_store` unchanged.
 *
 * Commit failures are reported as one of two typed outcomes:
 *
 * - `PipelineStateStoreError` (`not_committed`): the rename did not happen;
 *   the previous committed snapshot is guaranteed to remain the authoritative
 *   one, byte-for-byte.
 * - `PipelineStateDurabilityError` (`durability_unknown`): the rename
 *   succeeded, but the post-rename durability confirmation failed. The
 *   candidate revision is already visible at the state path, yet whether the
 *   previous or the candidate revision survives a crash is not guaranteed.
 *   The rename is never rolled back and no automatic recovery is implemented.
 */

export class PipelineStateStoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineStateStoreError";
  }
}

export class PipelineStateDurabilityError extends PipelineStateStoreError {
  readonly revision: number;
  readonly candidate: PipelineRunState;

  constructor(revision: number, candidate: PipelineRunState, message: string) {
    super(message);
    this.name = "PipelineStateDurabilityError";
    this.revision = revision;
    this.candidate = candidate;
  }
}

export {
  defaultPipelineStateIo,
  runSnapshotStatePath as pipelineRunStatePath,
} from "./run_snapshot_store.ts";
export type {
  PipelineStateIo,
  StateDirHandle,
  StateFileHandle,
} from "./run_snapshot_store.ts";

const V1_DOCUMENT_LABEL = "pipeline run state";

function retarget(cause: unknown): unknown {
  return retargetSnapshotStoreError<PipelineRunState>(cause, {
    notCommitted: (message) => new PipelineStateStoreError(message),
    durabilityUnknown: (revision, candidate, message) =>
      new PipelineStateDurabilityError(revision, candidate as PipelineRunState, message),
  });
}

export class PipelineStateStore {
  private readonly inner: RunSnapshotStore<PipelineRunState>;

  constructor(
    stateRoot: string,
    runId: string,
    io: PipelineStateIo = defaultPipelineStateIo,
  ) {
    this.inner = new RunSnapshotStore<PipelineRunState>(stateRoot, runId, {
      parseSnapshot: parsePipelineRunState,
      documentLabel: V1_DOCUMENT_LABEL,
    }, io);
  }

  get path(): string {
    return this.inner.path;
  }

  /**
   * Loads the committed snapshot, or null when no run state exists yet.
   * Rejects symlinks and every document the exact-field validator rejects.
   */
  async load(): Promise<PipelineRunState | null> {
    try {
      return await this.inner.load();
    } catch (cause) {
      throw retarget(cause);
    }
  }

  /** First write of a run; refuses to overwrite an existing run. */
  async create(state: PipelineRunState): Promise<void> {
    try {
      await this.inner.create(state);
    } catch (cause) {
      throw retarget(cause);
    }
  }

  /**
   * Subsequent write: the committed on-disk revision must equal
   * `expectedRevision` and `state.revision` must be exactly one higher.
   */
  async commit(state: PipelineRunState, expectedRevision: number): Promise<void> {
    try {
      await this.inner.commit(state, expectedRevision);
    } catch (cause) {
      throw retarget(cause);
    }
  }
}
