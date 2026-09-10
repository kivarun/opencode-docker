/**
 * Production-neutral coordinator for pipeline schema version 2: the first
 * orchestration layer that assembles the already existing v2 components —
 * `executePipelineV2Graph`, the v2 data plane, the decision evaluator, the
 * execution snapshot/digest, the durable state schema v3, the run state
 * sink and the typed runtime failures — into one coordination flow.
 *
 * The coordinator owns the durable run state end to end: it creates the
 * run-input snapshot itself (`snapshotRunInputs` — a caller-provided
 * snapshot is never accepted, so the durable `create_run.inputs` always
 * rests on the object the coordinator just received), dispatches every
 * command through the injected state sink, and drives the graph strictly
 * through the single engine `executePipelineV2Graph`. There is no second
 * graph loop, no manual transition search and no coordinator-owned cursor:
 * the engine selects the next state, applies the fixed agent lifecycle
 * outcome `completed` after the void agent callback, and the transition
 * commit hook is the only writer of transitions.
 *
 * The agent runtime boundary is production-neutral: an injected
 * `PipelineV2AgentRuntime` creates one Session per agent-state activation
 * and returns a session whose `run()` reports only the lifecycle result
 * (completed, or a typed worker failure) and whose `cleanup()` releases it.
 * The runtime never sees transitions, transition targets, accepted records
 * or any right to choose an outcome. Runtime and session contract
 * functions are captured exactly once before the first side effect and
 * again right after the Session is created, so swapping methods while a
 * callback is pending cannot change the dispatch; user objects are never
 * frozen or modified.
 *
 * Failure policy is classification by typed errors and context only — no
 * message parsing. Every expected failure becomes the reason of the
 * durable state; an agent execution records `agent_failed` with the
 * correct session cleanup outcome, a decision execution records
 * `decision_failed`, a session cleanup failure takes priority and
 * finalizes the run as `run_cleanup_failed`, and everything unexpected is
 * `internal_error`.
 *
 * Every failure/final write is outcome-aware: one internal helper
 * distinguishes a committed write, a not-committed write (the previous
 * snapshot stays authoritative) and a durability-unknown write (the
 * rename landed, the sink is poisoned) instead of swallowing commit
 * outcomes. The unfinished-execution tracking clears only after a
 * confirmed `agent_failed`/`decision_failed` commit, a knowingly
 * incompatible run-level write is never attempted, and any unconfirmed
 * failure or final-status write reports `state_persist_failed` on the
 * last authoritative snapshot. No recursive finalization: a failed
 * failure-write admits exactly one bounded `run_failed:
 * state_persist_failed` attempt when the reducer still accepts it from
 * the committed snapshot; after a durability-unknown commit the sink is
 * poisoned, the visible snapshot is adopted, all state writes stop and
 * the run fails with `state_persist_failed`.
 *
 * The result contract mirrors the durable state: `ok: true` is returned
 * only after a confirmed `run_succeeded` commit, and a failed terminal —
 * whose run outputs are still published — returns
 * `{ok: false, reason: "terminal_failed", state}` with the durable failed
 * state. Whenever the returned reason is not `state_persist_failed`, it
 * equals the final `state.failure.reason`.
 *
 * Resume is not supported: the coordinator accepts only a fresh sink with
 * `snapshot === null` and no poisoning. This module is still not wired
 * into production: no `agent-smoke`, no CLI, no Docker Helper, no Launcher
 * auth, no real Session transport, no profiles and no default pipeline —
 * and the production loader keeps rejecting schema version 2 before
 * Launcher auth and before any Session.
 */
import { isAbsolute } from "node:path";
import { validateSafeId } from "./pipeline.ts";
import {
  executePipelineV2Graph,
  PipelineExecutionError,
  type PipelineV2GraphExecutors,
  type TransitionStep,
  type V2AgentExecutionView,
  type V2DecisionExecutionView,
} from "./pipeline_engine.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  collectRunOutputs,
  evaluatePreparedDecisionState,
  prepareActivationData,
  prepareDecisionStateData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
  type RunOutputSnapshotEntry,
} from "./pipeline_v2_runtime.ts";
import { PipelineV2RuntimeError } from "./pipeline_v2_runtime_error.ts";
import {
  PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
  PIPELINE_V2_TERMINAL_FAILURE_REASON,
  type PipelineDecisionStateRecord,
  type PipelineV2ExecutionState,
  type PipelineV2FailureReason,
  type PipelineV2RunCommand,
  type PipelineV2RunOutputState,
  type PipelineV2RunState,
  type PipelineV2SessionCleanup,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import {
  requireResolvedPipelineV2Provenance,
  type PipelineDecisionStateResult,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";

/**
 * Lifecycle result of one worker run, reported by the injected agent
 * session. It carries no stdout, no artifacts and no outcome string: after
 * a completed run the engine applies its own fixed `completed` outcome,
 * and a typed worker failure fails the execution.
 */
export type PipelineV2WorkerRunResult =
  | { readonly status: "completed" }
  | { readonly status: "failed"; readonly reason: "worker_failed" | "worker_timeout" };

/**
 * One agent Session created by the injected runtime for one activation.
 * `run()` reports only the lifecycle result; `cleanup()` releases the
 * session and is invoked exactly once by the coordinator.
 */
export interface PipelineV2AgentSession {
  readonly sessionId: string;
  run(): Promise<PipelineV2WorkerRunResult>;
  cleanup(): Promise<void>;
}

/**
 * Production-neutral agent runtime boundary. The coordinator captures
 * `createSession` exactly once before its first side effect; the runtime
 * never sees transitions, transition targets, accepted records or any
 * right to choose an outcome.
 */
export interface PipelineV2AgentRuntime {
  createSession(
    state: V2AgentExecutionView,
    activation: PreparedActivationData,
  ): Promise<PipelineV2AgentSession>;
}

/**
 * The minimal structural view of the durable run state sink the
 * coordinator needs. The production `PipelineV2RunStateSink` satisfies it
 * structurally; tests inject equivalent fakes with fault injection.
 */
export interface PipelineV2CoordinatorStateSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  dispatch(command: PipelineV2RunCommand): Promise<void>;
}

export interface PipelineV2CoordinatorParams {
  /** The exact deep-frozen snapshot a successful `loadPipelineV2` returned. */
  readonly pipeline: ResolvedPipelineV2;
  readonly runId: string;
  /** Canonical orchestrator-owned run root; `project/` must already exist. */
  readonly runRoot: string;
  /** The original run-input bindings, bound once by the caller. */
  readonly inputBindings: readonly RunInputBinding[];
  readonly sink: PipelineV2CoordinatorStateSink;
  readonly runtime: PipelineV2AgentRuntime;
}

export type PipelineV2CoordinationResult =
  | {
      readonly ok: true;
      readonly state: PipelineV2RunState;
    }
  | {
      readonly ok: false;
      readonly reason: PipelineV2FailureReason;
      readonly state: PipelineV2RunState | null;
    };

/** Internal abort marker after a failure was already durably recorded. */
class CoordinationAbortedError extends Error {
  constructor() {
    super("pipeline v2 coordination aborted: the failure was already recorded durably");
    this.name = "CoordinationAbortedError";
  }
}

function describeValue(value: unknown): string {
  return value === null ? "null" : typeof value;
}

/**
 * Captures one contract function of an injected object exactly once,
 * before any side effect: the function is read one time, checked against
 * the contract, and bound to its owner. Reassigning the property later
 * cannot change the dispatch, and the user object is never frozen or
 * modified.
 */
function captureContractFunction<O extends object>(owner: O, name: string, ownerLabel: string): unknown {
  const value = (owner as unknown as Record<string, unknown>)[name];
  if (typeof value !== "function") {
    throw new Error(
      `${ownerLabel} contract violated: ${name} must be a function, got ${describeValue(value)}`,
    );
  }
  return (value as (...args: never[]) => unknown).bind(owner);
}

type CapturedCreateSession = (
  state: V2AgentExecutionView,
  activation: PreparedActivationData,
) => Promise<PipelineV2AgentSession>;

function captureCreateSession(runtime: PipelineV2AgentRuntime): CapturedCreateSession {
  return captureContractFunction(
    runtime,
    "createSession",
    "pipeline v2 agent runtime",
  ) as CapturedCreateSession;
}

/**
 * Captures one session contract member exactly once: the property is read
 * one time (a throwing getter is a contract violation, never propagated),
 * checked against the contract, and bound to its owner. Reassigning the
 * property later cannot change the dispatch, and the user object is never
 * frozen or modified. Diagnostics carry no values.
 */
function captureSessionFunction(
  session: PipelineV2AgentSession,
  name: "run" | "cleanup",
): unknown {
  let value: unknown;
  try {
    value = (session as unknown as Record<string, unknown>)[name];
  } catch {
    throw new Error(
      `pipeline v2 agent session contract violated: ${name} must be a function`,
    );
  }
  if (typeof value !== "function") {
    throw new Error(
      `pipeline v2 agent session contract violated: ${name} must be a function`,
    );
  }
  return (value as (...args: never[]) => unknown).bind(session);
}

/**
 * Captures one session contract member exactly once, right after the
 * Session is created. Reassigning the property later cannot change the
 * dispatch; the violation diagnostics never echo values.
 */
function captureSessionId(session: PipelineV2AgentSession): string {
  let sessionId: unknown;
  try {
    sessionId = session.sessionId;
  } catch {
    throw new Error(
      "pipeline v2 agent session contract violated: sessionId must be a non-empty string",
    );
  }
  if (typeof sessionId !== "string" || sessionId === "") {
    throw new Error(
      "pipeline v2 agent session contract violated: sessionId must be a non-empty string",
    );
  }
  return sessionId;
}

/**
 * The exact worker run result shapes. Anything else — arrays, missing or
 * extra fields, wrong literals, throwing getters — is rejected without
 * echoing the offending value or field names.
 */
type ParsedWorkerRunResult =
  | { readonly status: "completed" }
  | { readonly status: "failed"; readonly reason: "worker_failed" | "worker_timeout" };

function parseWorkerRunResult(value: unknown): ParsedWorkerRunResult | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  let keys: string[];
  let status: unknown;
  try {
    keys = Object.keys(value);
    status = (value as Record<string, unknown>).status;
  } catch {
    return undefined;
  }
  if (status === "completed") {
    return keys.length === 1 && keys[0] === "status" ? { status: "completed" } : undefined;
  }
  if (status !== "failed" || keys.length !== 2 || !keys.includes("reason")) {
    return undefined;
  }
  let reason: unknown;
  try {
    reason = (value as Record<string, unknown>).reason;
  } catch {
    return undefined;
  }
  if (reason === "worker_failed" || reason === "worker_timeout") {
    return { status: "failed", reason };
  }
  return undefined;
}

/**
 * Classifies one unexpected failure by typed errors and context only —
 * never by parsing messages. The typed data-plane reasons, the engine
 * reasons (all of which exist in state schema v3) and the store/durability
 * failures keep their reasons; everything else is `internal_error`.
 */
function classifyCause(cause: unknown): PipelineV2FailureReason {
  if (cause instanceof PipelineV2RuntimeError) {
    return cause.reason;
  }
  if (cause instanceof PipelineExecutionError) {
    return cause.reason;
  }
  if (
    cause instanceof PipelineV2RunStateDurabilityError ||
    cause instanceof PipelineV2RunStateStoreError
  ) {
    return "state_persist_failed";
  }
  return "internal_error";
}

/**
 * The execution-level failure vocabulary is a closed subset of the run
 * reasons; a classified reason outside it (for example a state-store
 * failure during an execution) is recorded as `internal_error` on the
 * execution while the run-level finalize keeps the true reason.
 */
const AGENT_EXECUTION_REASONS: readonly PipelineV2FailureReason[] = [
  "internal_error",
  "run_input_invalid",
  "run_input_modified",
  "activation_prepare_failed",
  "worker_failed",
  "worker_timeout",
  "activation_output_invalid",
  "accepted_output_modified",
  "unknown_outcome",
  "invalid_outcome",
  "session_cleanup_failed",
  "signal_sigint",
  "signal_sigterm",
];

const DECISION_EXECUTION_REASONS: readonly PipelineV2FailureReason[] = [
  "internal_error",
  "run_input_invalid",
  "run_input_modified",
  "accepted_output_modified",
  "decision_input_invalid",
  "unknown_outcome",
  "invalid_outcome",
  "signal_sigint",
  "signal_sigterm",
];

function executionReasonFor(
  kind: "agent" | "decision",
  reason: PipelineV2FailureReason,
): PipelineV2FailureReason {
  const allowed = kind === "agent" ? AGENT_EXECUTION_REASONS : DECISION_EXECUTION_REASONS;
  return allowed.includes(reason) ? reason : "internal_error";
}

/** The durable, content-free record form of one evaluated decision state. */
function toDecisionRecord(result: PipelineDecisionStateResult): PipelineDecisionStateRecord {
  switch (result.status) {
    case "selected":
      return {
        status: "selected",
        outcome: result.outcome,
        decision: result.decision,
        rule_id: result.rule_id,
        active_constraint_ids: [...result.active_constraint_ids],
      };
    case "uncovered":
      return {
        status: "uncovered",
        outcome: "uncovered",
        active_constraint_ids: [...result.active_constraint_ids],
      };
    case "inconsistent_facts":
      return {
        status: "inconsistent_facts",
        outcome: "inconsistent_facts",
        violated_relation_ids: [...result.violated_relation_ids],
      };
    case "invalid_facts": {
      const record: PipelineDecisionStateRecord = {
        status: "invalid_facts",
        outcome: "invalid_facts",
        reason: result.reason,
      };
      if (result.fact_id !== undefined && result.actual_type !== undefined) {
        return { ...record, fact_id: result.fact_id, actual_type: result.actual_type };
      }
      if (result.fact_id !== undefined) {
        return { ...record, fact_id: result.fact_id };
      }
      if (result.actual_type !== undefined) {
        return { ...record, actual_type: result.actual_type };
      }
      return record;
    }
  }
}

/** One durable run-output entry without the internal snapshot path. */
function toRunOutputState(entry: RunOutputSnapshotEntry): PipelineV2RunOutputState {
  if (entry.present) {
    return {
      id: entry.id,
      type: entry.type,
      required: entry.required,
      present: true,
      digest: entry.digest,
    };
  }
  return { id: entry.id, type: entry.type, required: false, present: false };
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

/**
 * The three explicit outcomes of one durable state write inside the
 * failure finalizer. Nothing is ever swallowed: `committed` means the
 * reducer accepted the command and the commit is confirmed,
 * `not_committed` means the rename did not happen and the previous
 * snapshot stays authoritative, and `durability_unknown` means the rename
 * landed while its crash survival is unknown (the sink is poisoned).
 */
type DispatchOutcome =
  | { readonly outcome: "committed" }
  | { readonly outcome: "not_committed"; readonly durabilityUnknown: boolean };

/** A run-level finalize command the reducer can still accept. */
type RunFinalizeCommand =
  | { readonly kind: "run_cleanup_failed" }
  | { readonly kind: "run_failed"; readonly reason: PipelineV2FailureReason };

/** An execution whose phase can never change again (mirrors the reducer). */
function executionSettled(execution: PipelineV2ExecutionState): boolean {
  if (execution.type === "agent") {
    return execution.phase === "cleanup_completed" || execution.phase === "failed";
  }
  return execution.phase === "evaluated" || execution.phase === "failed";
}

/**
 * Whether the reducer would still accept a `run_failed` with the given
 * reason from the given committed snapshot, decided by typed context
 * only — never by parsing messages. Mirrors the reducer's admissibility:
 * every execution must be settled, a failed session cleanup finalizes
 * only with `run_cleanup_failed`, and a failed terminal with published
 * run outputs finalizes only with the terminal failure reason.
 */
function runFailedAdmissible(
  state: PipelineV2RunState,
  reason: PipelineV2FailureReason,
): boolean {
  for (const execution of state.executions) {
    if (!executionSettled(execution)) {
      return false;
    }
  }
  const last = state.executions[state.executions.length - 1];
  if (
    last !== undefined &&
    last.type === "agent" &&
    last.phase === "failed" &&
    last.session_cleanup === "failed"
  ) {
    return false;
  }
  const terminalFailedPublished =
    state.terminal !== undefined &&
    state.terminal.result === "failed" &&
    state.run_outputs !== undefined;
  if (terminalFailedPublished) {
    return reason === PIPELINE_V2_TERMINAL_FAILURE_REASON;
  }
  return reason !== PIPELINE_V2_TERMINAL_FAILURE_REASON;
}

/** Bookkeeping of the current agent session (one agent execution only). */
interface AgentSessionTracking {
  /** The session id was durably recorded by `agent_session_created`. */
  createdDurably: boolean;
  /** The captured cleanup ran exactly once (successfully or not). */
  cleanupDone: boolean;
  cleanupFailed: boolean;
  /** The captured cleanup function; bound exactly once at session capture. */
  cleanup: () => Promise<void>;
}

interface ExecutionTracking {
  kind: "agent" | "decision" | null;
  /** An execution record exists that no transition has committed yet. */
  unfinished: boolean;
  session: AgentSessionTracking | null;
}

/**
 * Coordinates one fresh pipeline v2 run end to end. The result is
 * deep-frozen and carries no raw error messages, stdout/stderr, facts,
 * prompt or input bodies, credentials or environment values.
 */
export async function coordinatePipelineV2Run(
  params: PipelineV2CoordinatorParams,
): Promise<PipelineV2CoordinationResult> {
  const { pipeline, runId, runRoot, sink, runtime } = params;

  // Caller contract: a fresh sink for a new run; resume is not supported.
  if (sink.poisoned || sink.snapshot !== null) {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }
  try {
    validateSafeId(runId, "pipeline v2 coordinator run id");
  } catch {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }
  if (typeof runRoot !== "string" || runRoot === "" || !isAbsolute(runRoot)) {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

  // The runtime contract functions are captured exactly once, before the
  // first filesystem or state side effect.
  let capturedCreateSession: CapturedCreateSession;
  try {
    capturedCreateSession = captureCreateSession(runtime);
  } catch {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

  // Phase 0: the run-input snapshot is created here, by the coordinator;
  // a caller-provided snapshot is never accepted.
  let runInputs: RunInputsSnapshot;
  try {
    runInputs = await snapshotRunInputs(pipeline, params.inputBindings, runRoot);
  } catch (cause) {
    return deepFreeze({ ok: false as const, reason: classifyCause(cause), state: null });
  }

  // Phase 1: the durable run state is created from the snapshot the
  // coordinator itself just received.
  const runInputStates = runInputs.inputs.map((entry) => ({
    id: entry.id,
    type: entry.type,
    protected: entry.protected,
    digest: entry.digest,
  }));
  try {
    await sink.dispatch({
      kind: "create_run",
      runId,
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: runInputStates,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      // The rename landed; the visible candidate snapshot is adopted.
      return deepFreeze({ ok: false as const, reason: "state_persist_failed" as const, state: sink.snapshot });
    }
    return deepFreeze({ ok: false as const, reason: "state_persist_failed" as const, state: null });
  }

  // --- shared coordination state -----------------------------------------

  let stateAbandoned = false;
  let finalized = false;
  let failureReason: PipelineV2FailureReason | undefined;
  const tracking: ExecutionTracking = { kind: null, unfinished: false, session: null };
  const accepted: AcceptedStateOutput[] = [];

  const dispatchState = async (command: PipelineV2RunCommand): Promise<void> => {
    try {
      await sink.dispatch(command);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        stateAbandoned = true;
      }
      throw cause;
    }
  };

  const requireSnapshot = (): PipelineV2RunState => {
    const state = sink.snapshot;
    if (state === null) {
      throw new Error("the pipeline v2 run state snapshot disappeared after create_run");
    }
    return state;
  };

  const requireLastExecutionIndex = (): number => {
    const state = requireSnapshot();
    const last = state.executions[state.executions.length - 1];
    if (last === undefined) {
      throw new Error("the pipeline v2 run state records no execution");
    }
    return last.index;
  };

  /**
   * One outcome-aware durable state write for the failure finalizer: it
   * distinguishes a committed write, a not-committed write and a
   * durability-unknown write instead of swallowing the commit outcome.
   * When the state is already abandoned the command is never dispatched.
   * A reducer rejection and a store failure are both `not_committed` —
   * nothing was durably changed.
   */
  const dispatchOutcomeAware = async (
    command: PipelineV2RunCommand,
  ): Promise<DispatchOutcome> => {
    if (stateAbandoned) {
      return { outcome: "not_committed", durabilityUnknown: true };
    }
    try {
      await sink.dispatch(command);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        stateAbandoned = true;
        return { outcome: "not_committed", durabilityUnknown: true };
      }
      return { outcome: "not_committed", durabilityUnknown: false };
    }
    return { outcome: "committed" };
  };

  /** The agent_failed command for the still-unfinished agent execution. */
  const agentFailureCommand = (reason: PipelineV2FailureReason): PipelineV2RunCommand => {
    // Session not durably recorded -> "not_required"; created and cleanup
    // confirmed -> "completed"; cleanup not confirmed -> "failed" with the
    // session cleanup failure reason.
    const sessionCleanup: PipelineV2SessionCleanup =
      tracking.session === null || !tracking.session.createdDurably
        ? "not_required"
        : tracking.session.cleanupFailed ? "failed" : "completed";
    let recordReason = tracking.session !== null && tracking.session.cleanupFailed
      ? PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON
      : executionReasonFor("agent", reason);
    if (sessionCleanup === "not_required" && recordReason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
      recordReason = "internal_error";
    }
    return { kind: "agent_failed", reason: recordReason, sessionCleanup };
  };

  /**
   * Records one failure exactly once, outcome-aware: best-effort session
   * cleanup (the only allowed side effect after a failed durable write),
   * the execution-level failure record for a still-unfinished execution —
   * whose commit must be confirmed before the execution counts as settled
   * — and the run-level finalize. A not-committed failure write never
   * clears the tracking, never triggers a knowingly incompatible run-level
   * write, and reports `state_persist_failed` on the last authoritative
   * snapshot. Exactly one bounded `run_failed: state_persist_failed`
   * attempt follows a not-committed first finalize, only when the reducer
   * still accepts it from the committed snapshot. No recursive
   * finalization; after a durability-unknown commit all further writes
   * stop.
   */
  const finalizeFailure = async (
    cause: unknown,
    reasonOverride?: PipelineV2FailureReason,
  ): Promise<void> => {
    if (finalized) {
      return;
    }
    finalized = true;
    let reason = reasonOverride ?? classifyCause(cause);

    // Best-effort cleanup of an existing session, exactly once; the only
    // allowed side effect after a failed durable write. The cleanup
    // outcome decides the session-cleanup failure priority.
    if (tracking.session !== null && !tracking.session.cleanupDone) {
      const session = tracking.session;
      session.cleanupDone = true;
      try {
        await session.cleanup();
      } catch {
        session.cleanupFailed = true;
      }
    }
    if (tracking.session !== null && tracking.session.cleanupFailed) {
      reason = PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON;
    }
    failureReason = reason;

    if (tracking.unfinished && tracking.kind !== null && !stateAbandoned) {
      const kind = tracking.kind;
      const outcome = kind === "agent"
        ? await dispatchOutcomeAware(agentFailureCommand(reason))
        : await dispatchOutcomeAware({
            kind: "decision_failed",
            reason: executionReasonFor("decision", reason),
          });
      if (outcome.outcome !== "committed") {
        // The execution stays unfinished in the authoritative snapshot;
        // no knowingly incompatible run-level write is attempted.
        failureReason = "state_persist_failed";
        return;
      }
      tracking.unfinished = false;
    }

    if (stateAbandoned) {
      return;
    }

    // The run-level finalize: never a knowingly incompatible command.
    if (
      tracking.session === null || !tracking.session.cleanupFailed
    ) {
      const state = sink.snapshot;
      if (state !== null && !runFailedAdmissible(state, reason)) {
        failureReason = "state_persist_failed";
        return;
      }
    }
    const finalizeCommand: RunFinalizeCommand =
      tracking.session !== null && tracking.session.cleanupFailed
        ? { kind: "run_cleanup_failed" }
        : { kind: "run_failed", reason };
    const finalizeOutcome = await dispatchOutcomeAware(finalizeCommand);
    if (finalizeOutcome.outcome === "committed") {
      failureReason =
        finalizeCommand.kind === "run_cleanup_failed"
          ? PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON
          : finalizeCommand.reason;
      return;
    }
    if (finalizeOutcome.durabilityUnknown) {
      // The caller reports state_persist_failed on the adopted candidate.
      return;
    }
    // One bounded attempt at the normalized state failure, only when the
    // reducer still accepts it from the committed snapshot; no recursive
    // finalization beyond that.
    const snapshot = sink.snapshot;
    if (snapshot !== null && runFailedAdmissible(snapshot, "state_persist_failed")) {
      await dispatchOutcomeAware({ kind: "run_failed", reason: "state_persist_failed" });
    }
    failureReason = "state_persist_failed";
  };

  /**
   * The captured cleanup of the current agent session, runnable exactly
   * once on every path; a cleanup failure takes priority in the failure
   * policy.
   */
  const runSessionCleanup = async (): Promise<void> => {
    const session = tracking.session;
    if (session === null || session.cleanupDone) {
      return;
    }
    session.cleanupDone = true;
    try {
      await session.cleanup();
    } catch (cause) {
      session.cleanupFailed = true;
      throw cause;
    }
  };

  // --- the single execution flow through the engine -----------------------

  const executors: PipelineV2GraphExecutors = {
    executeAgent: async (view: V2AgentExecutionView): Promise<void> => {
      tracking.kind = "agent";
      tracking.unfinished = false;
      const sessionTracking: AgentSessionTracking = {
        createdDurably: false,
        cleanupDone: false,
        cleanupFailed: false,
        cleanup: async () => {},
      };
      tracking.session = sessionTracking;

      // 1. start_agent_execution
      await dispatchState({ kind: "start_agent_execution", stateId: view.id, profile: view.profile });
      tracking.unfinished = true;

      // 2. the committed execution index comes from the durable snapshot
      const executionIndex = requireLastExecutionIndex();

      // 3. prepare the activation data through the existing data plane
      //    (it re-verifies the protected inputs and the accepted outputs)
      const activation = await prepareActivationData(pipeline, runInputs, accepted, view.id, executionIndex);

      // 4. agent_data_prepared
      await dispatchState({ kind: "agent_data_prepared" });

      // 5. runtime.createSession (captured once at coordination start)
      const session = await capturedCreateSession(view, activation);

      // The session contract is captured exactly once, right after the
      // Session is created, in a fixed order: the cleanup first — when a
      // later member violates the contract, the already-captured cleanup
      // is still runnable exactly once — then the session id, then the
      // run entry point. Reassigning any member afterwards cannot change
      // the dispatch.
      let capturedCleanup: () => Promise<void>;
      try {
        capturedCleanup = captureSessionFunction(session, "cleanup") as () => Promise<void>;
      } catch {
        // A missing or non-callable cleanup is a trusted runtime contract
        // violation: nothing runnable exists, so the coordinator must not
        // claim a confirmed cleanup.
        sessionTracking.cleanupDone = true;
        throw new Error(
          "pipeline v2 agent session contract violated: cleanup must be a function",
        );
      }
      sessionTracking.cleanup = capturedCleanup;
      const sessionId = captureSessionId(session);
      const capturedRun = captureSessionFunction(session, "run") as () => Promise<unknown>;

      // 6. agent_session_created — recorded immediately
      await dispatchState({ kind: "agent_session_created", sessionId });
      sessionTracking.createdDurably = true;

      // 7. agent_running
      await dispatchState({ kind: "agent_running" });

      // 8. session.run() — only the lifecycle result crosses the boundary
      const outcome = await capturedRun();
      const parsed = parseWorkerRunResult(outcome);
      if (parsed === undefined) {
        throw new Error(
          "pipeline v2 agent session contract violated: run must return a completed or a typed failed result",
        );
      }
      if (parsed.status === "failed") {
        // The session cleanup still runs exactly once; the failure is
        // recorded durably and the engine stops before any transition.
        await runSessionCleanup().catch(() => undefined);
        await finalizeFailure(undefined, parsed.reason);
        throw new CoordinationAbortedError();
      }

      // 9. accept the activation outputs (the worker held them read-write)
      const records = await acceptActivationOutputs(pipeline, activation);

      // 10. agent_outputs_accepted
      await dispatchState({
        kind: "agent_outputs_accepted",
        outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
      });

      // 11. session.cleanup() exactly once
      await runSessionCleanup();

      // 12. agent_cleanup_completed
      await dispatchState({ kind: "agent_cleanup_completed" });
      tracking.unfinished = false;

      // 13. the accepted records join the runner-owned history
      accepted.push(...records);

      // 14. return void: the engine applies its own fixed completed outcome
    },

    executeDecision: async (view: V2DecisionExecutionView): Promise<string> => {
      tracking.kind = "decision";
      tracking.unfinished = false;
      tracking.session = null;

      // 1. the next global execution index
      const executionIndex = requireSnapshot().executions.length + 1;

      // 2. prepare the decision data (the input is read exactly once)
      const prepared = await prepareDecisionStateData(pipeline, runInputs, accepted, view.id, executionIndex);

      // 3. start_decision_execution with the prepared input digest
      await dispatchState({
        kind: "start_decision_execution",
        stateId: view.id,
        inputDigest: prepared.input_digest,
      });
      tracking.unfinished = true;

      // 4. evaluate the prepared decision state (no second read)
      const result = evaluatePreparedDecisionState(pipeline, prepared);

      // 5. decision_evaluated with the content-free record form
      await dispatchState({ kind: "decision_evaluated", result: toDecisionRecord(result) });
      tracking.unfinished = false;

      // 6. only the outcome is returned to the engine; the transition
      //    table owns the routing
      return result.outcome;
    },
  };

  /**
   * The transition commit hook is the only writer of transitions: it
   * dispatches `transition_committed` with the index of the last settled
   * execution and must complete before the engine moves the cursor. A
   * hook failure stops the graph before the next callback; the failure
   * policy finalizes the run.
   */
  const onTransitionCommit = async (step: TransitionStep): Promise<void> => {
    const executionIndex = requireLastExecutionIndex();
    await dispatchState({ kind: "transition_committed", step, executionIndex });
    tracking.unfinished = false;
  };

  try {
    const engineResult = await executePipelineV2Graph(pipeline, executors, { onTransitionCommit });

    // --- terminal phase ---------------------------------------------------

    // 1. terminal_reached
    await dispatchState({
      kind: "terminal_reached",
      terminalStateId: engineResult.terminalStateId,
      terminalResult: engineResult.terminalResult,
    });

    // 2./3. collect and publish the run outputs (both terminal results)
    const runOutputs = await collectRunOutputs(pipeline, runInputs, accepted);
    await dispatchState({
      kind: "run_outputs_published",
      outputs: runOutputs.outputs.map(toRunOutputState),
    });

    // 4./5. finalize the run status. `ok: true` is reserved for a
    // confirmed `run_succeeded` commit; a failed terminal still publishes
    // its outputs and returns the durable failed state with the terminal
    // failure reason — never a second finalize after the committed write.
    if (engineResult.terminalResult === "success") {
      await dispatchState({ kind: "run_succeeded" });
      const state = requireSnapshot();
      return deepFreeze({ ok: true as const, state });
    }
    await dispatchState({ kind: "run_failed", reason: PIPELINE_V2_TERMINAL_FAILURE_REASON });
    const state = requireSnapshot();
    return deepFreeze({
      ok: false as const,
      reason: PIPELINE_V2_TERMINAL_FAILURE_REASON,
      state,
    });
  } catch (cause) {
    await finalizeFailure(cause);
    if (stateAbandoned) {
      return deepFreeze({ ok: false as const, reason: "state_persist_failed" as const, state: sink.snapshot });
    }
    const reason = failureReason ?? classifyCause(cause);
    return deepFreeze({ ok: false as const, reason, state: sink.snapshot });
  }
}
