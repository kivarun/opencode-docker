import {
  reducePipelineRunCommand,
  PipelineStateError,
  type PipelineIdentityState,
  type PipelineRunCommand,
  type PipelineRunState,
  type ProtectedInputState,
} from "./pipeline_state.ts";
import {
  PipelineStateStore,
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
    const now = this.params.now ? this.params.now() : new Date();
    const next = reducePipelineRunCommand(this.current, command, now);
    if (this.current === null) {
      await this.store.create(next);
    } else {
      await this.store.commit(next, this.current.revision);
    }
    // The committed snapshot becomes the in-memory state only after the
    // durable write succeeded; a failed commit leaves the previous snapshot
    // authoritative.
    this.current = next;
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
