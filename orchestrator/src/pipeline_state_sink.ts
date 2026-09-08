import {
  reducePipelineRunCommand,
  PipelineStateError,
  type PipelineIdentityState,
  type PipelineRunCommand,
  type PipelineRunState,
  type ProtectedInputState,
} from "./pipeline_state.ts";
import {
  PipelineStateDurabilityError,
  PipelineStateStore,
  PipelineStateStoreError,
  type PipelineStateIo,
} from "./pipeline_state_store.ts";
import { classifyRunFailure } from "./run_errors.ts";
import type { TransitionStep } from "./pipeline_engine.ts";
import type {
  RunFinalization,
  RunStateSink,
} from "./lifecycle.ts";

/**
 * Production run state sink for `agent-smoke`: the only state adapter of that
 * command. It owns the durable pipeline run state under the operator state
 * root, applies every mutation through the pure reducer, and atomically
 * commits each new snapshot. It never records credentials, environment
 * values, prompt/input bodies, OpenCode configuration, raw worker output, or
 * result summaries.
 *
 * Commit outcome handling:
 *
 * - `not_committed` (`PipelineStateStoreError`): the snapshot on disk is
 *   unchanged; the sink stays on the previous revision, which remains
 *   authoritative and can still record the normalized failure at finalize.
 * - `durability_unknown` (`PipelineStateDurabilityError`): the candidate
 *   revision is already visible on disk; the sink adopts it as the visible
 *   state, poisons itself, and refuses every further commit and finalize for
 *   this run. The error propagates and fails the run.
 */

export interface PipelineRunSinkParams {
  stateDirPath: string;
  runId: string;
  /** Canonical workspace path recorded as run identity. */
  workspace: string;
  identity: PipelineIdentityState;
  protectedInput: ProtectedInputState;
  attempt: { stateId: string; attempt: number; profile: string };
  io?: PipelineStateIo;
  /** Injected clock (tests); defaults to the wall clock. */
  now?: () => Date;
}

export interface AcceptedAgentResultRecord {
  resultSha256: string;
  artifacts: readonly string[];
}

export class PipelineRunStateSink implements RunStateSink {
  private readonly store: PipelineStateStore;
  private readonly params: PipelineRunSinkParams;
  private current: PipelineRunState | null = null;
  private chain: Promise<unknown> = Promise.resolve();
  private isPoisoned = false;

  constructor(params: PipelineRunSinkParams) {
    this.params = params;
    this.store = new PipelineStateStore(params.stateDirPath, params.runId, params.io);
  }

  /** Path of the durable state document (diagnostics and tests only). */
  get statePath(): string {
    return this.store.path;
  }

  /** Last committed (or, after a failed commit, last good) snapshot. */
  get snapshot(): PipelineRunState | null {
    return this.current;
  }

  /**
   * True after a `durability_unknown` commit: the sink adopted the visible
   * candidate snapshot and refuses every further write for this run.
   */
  get poisoned(): boolean {
    return this.isPoisoned;
  }

  currentStatus(): string {
    return this.current?.status ?? "failed";
  }

  private async dispatch(command: PipelineRunCommand): Promise<void> {
    // Serialize dispatches of this run inside the process: every command
    // reads and replaces `this.current`.
    const result = this.chain.then(() => this.dispatchNow(command));
    this.chain = result.catch(() => undefined);
    return await result;
  }

  private async dispatchNow(command: PipelineRunCommand): Promise<void> {
    if (this.isPoisoned) {
      throw new PipelineStateStoreError(
        "the pipeline run state is poisoned by a durability-unknown commit; no further writes are accepted for this run",
      );
    }
    const now = this.clock();
    const next = reducePipelineRunCommand(this.current, command, now);
    try {
      if (this.current === null) {
        await this.store.create(next);
      } else {
        await this.store.commit(next, this.current.revision);
      }
    } catch (cause) {
      if (cause instanceof PipelineStateDurabilityError) {
        // `durability_unknown`: the rename succeeded, so the candidate
        // revision is already visible at the state path. Adopt it as the
        // visible state, poison the sink (no further commits or finalize for
        // this run), and propagate: the run fails with exit 1, the Session is
        // still cleaned up exactly once, and neither the previous snapshot is
        // claimed to have survived nor the rename rolled back.
        this.current = cause.candidate;
        this.isPoisoned = true;
      }
      throw cause;
    }
    // The committed snapshot becomes the in-memory state only after the
    // durable write succeeded; a failed commit leaves the previous snapshot
    // authoritative.
    this.current = next;
  }

  private clock(): Date {
    return this.params.now ? this.params.now() : new Date();
  }

  async initialize(): Promise<void> {
    await this.dispatch({
      kind: "create_run",
      runId: this.params.runId,
      workspace: this.params.workspace,
      identity: this.params.identity,
      protectedInput: this.params.protectedInput,
      initialPhase: "validating",
    });
  }

  async phase(status: string, sessionId?: string): Promise<void> {
    switch (status) {
      case "creating_session":
        return await this.dispatch({ kind: "enter_phase", phase: "creating_session" });
      case "session_created": {
        if (typeof sessionId !== "string" || sessionId === "") {
          throw new PipelineStateError("the pipeline run state requires the child session id with the session_created status");
        }
        return await this.dispatch({ kind: "session_created", sessionId });
      }
      case "agent_running":
        return await this.dispatch({
          kind: "attempt_started",
          stateId: this.params.attempt.stateId,
          attempt: this.params.attempt.attempt,
          profile: this.params.attempt.profile,
        });
      default:
        throw new PipelineStateError(
          `the pipeline run state does not support the phase update ${JSON.stringify(status)}; run finalization goes through finalize()`,
        );
    }
  }

  async finalize(outcome: RunFinalization): Promise<void> {
    if (this.current === null) {
      // The initial snapshot never committed: record the run so the failure
      // is durable too, then fail it. If the store is still broken, this
      // propagates and the lifecycle reports the persist failure.
      await this.initialize();
    }
    switch (outcome.status) {
      case "success":
        return await this.dispatch({ kind: "run_succeeded" });
      case "cleanup_failed":
        return await this.dispatch({ kind: "run_cleanup_failed", reason: "session_cleanup_failed" });
      case "failed":
        return await this.dispatch({
          kind: "run_failed",
          reason: classifyRunFailure(outcome.failure, outcome.signal),
        });
    }
  }

  /** Engine transition hook: cursor, transition, and event in one commit. */
  async recordTransition(
    step: TransitionStep,
    accepted: AcceptedAgentResultRecord,
  ): Promise<void> {
    return await this.dispatch({
      kind: "transition_committed",
      step,
      attempt: this.params.attempt.attempt,
      resultSha256: accepted.resultSha256,
      artifacts: accepted.artifacts,
    });
  }

  async recordTerminal(terminalStateId: string, terminalResult: "success" | "failed"): Promise<void> {
    return await this.dispatch({
      kind: "terminal_reached",
      terminalStateId,
      terminalResult,
    });
  }
}
