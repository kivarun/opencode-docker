import {
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStore,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import type { PipelineStateIo } from "./run_snapshot_store.ts";

/**
 * Durable run state sink for pipeline schema version 2 (`pipeline_v2_state.ts`,
 * state schema version 5). The sink owns no vocabulary of its own: every mutation
 * goes through the existing `PipelineV2RunCommand` union via
 * `dispatch(command)`, which always runs
 *
 *  1. current snapshot -> `reducePipelineV2RunCommand(...)`;
 *  2. atomic store `create`/`commit` of the candidate;
 *  3. adopt the candidate as the current snapshot only after the commit
 *     outcome is known.
 *
 * Commit outcome handling:
 *
 * - reducer rejection (`PipelineV2StateError`): nothing is written, the
 *   snapshot stays exactly as it was;
 * - `not_committed` (`PipelineV2RunStateStoreError`): the snapshot on disk is
 *   unchanged; the sink stays on the previous revision, which remains
 *   authoritative. The caller may then dispatch a command the reducer still
 *   accepts from that state, such as the normalized
 *   `run_failed: state_persist_failed`.
 * - `durability_unknown` (`PipelineV2RunStateDurabilityError`): the candidate
 *   revision is already visible on disk; the sink adopts it as the visible
 *   snapshot, poisons itself, and refuses every further dispatch for this
 *   run. The error propagates.
 *
 * A poisoned sink rejects every subsequent dispatch before the reducer and
 * before any filesystem I/O. Dispatches of one run are serialized inside the
 * process. The sink never interprets lifecycle signals, worker errors, or
 * terminal policy and performs no automatic finalize. The run state never
 * records credentials, environment values, prompt/input bodies, decision
 * facts, result summaries, or raw worker output.
 */

export interface PipelineV2RunStateSinkParams {
  stateRoot: string;
  runId: string;
  io?: PipelineStateIo;
  /** Injected clock (tests); defaults to the wall clock. */
  now?: () => Date;
}

export class PipelineV2RunStateSink {
  private readonly store: PipelineV2RunStateStore;
  private readonly runId: string;
  private readonly clockSource?: () => Date;
  private current: PipelineV2RunState | null = null;
  private chain: Promise<unknown> = Promise.resolve();
  private isPoisoned = false;

  constructor(params: PipelineV2RunStateSinkParams) {
    this.runId = params.runId;
    this.clockSource = params.now;
    this.store = new PipelineV2RunStateStore(params);
  }

  /** Path of the durable state document (diagnostics and tests only). */
  get statePath(): string {
    return this.store.path;
  }

  /** Last committed (or, after a failed commit, last good) snapshot. */
  get snapshot(): PipelineV2RunState | null {
    return this.current;
  }

  /**
   * True after a `durability_unknown` commit: the sink adopted the visible
   * candidate snapshot and refuses every further command for this run.
   */
  get poisoned(): boolean {
    return this.isPoisoned;
  }

  /**
   * Applies one command through the reducer and atomically commits the
   * candidate. Rejected commands leave the snapshot and the durable state
   * untouched.
   */
  async dispatch(command: PipelineV2RunCommand): Promise<void> {
    // Serialize dispatches of this run inside the process: every command
    // reads and replaces `this.current`.
    const result = this.chain.then(() => this.dispatchNow(command));
    this.chain = result.catch(() => undefined);
    return await result;
  }

  private async dispatchNow(command: PipelineV2RunCommand): Promise<void> {
    if (this.isPoisoned) {
      throw new PipelineV2RunStateStoreError(
        "the pipeline v2 run state is poisoned by a durability-unknown commit; no further writes are accepted for this run",
      );
    }
    if (command.kind === "create_run" && command.runId !== this.runId) {
      throw new PipelineV2RunStateStoreError(
        `create_run names run ${JSON.stringify(command.runId)}, but this sink owns run ${JSON.stringify(this.runId)}`,
      );
    }
    const candidate = reducePipelineV2RunCommand(this.current, command, this.clock());
    try {
      if (this.current === null) {
        await this.store.create(candidate);
      } else {
        await this.store.commit(candidate, this.current.revision);
      }
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        // `durability_unknown`: the rename succeeded, so the candidate
        // revision is already visible at the state path. Adopt it as the
        // visible state, poison the sink (no further writes for this run),
        // and propagate: the previous snapshot is never claimed to have
        // survived and the rename is never rolled back.
        this.current = cause.candidate;
        this.isPoisoned = true;
      }
      throw cause;
    }
    // The committed snapshot becomes the in-memory state only after the
    // durable write succeeded; a failed commit leaves the previous snapshot
    // authoritative.
    this.current = candidate;
  }

  private clock(): Date {
    return this.clockSource ? this.clockSource() : new Date();
  }
}
