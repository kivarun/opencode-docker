/**
 * Production-neutral coordinator for pipeline schema version 2: the first
 * orchestration layer that assembles the already existing v2 components —
 * `executePipelineV2Graph`, the v2 data plane, the decision evaluator, the
 * execution snapshot/digest, the durable state schema v4, the run state
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
 * The agent runtime boundary follows the two-session capability model: an
 * injected `PipelineV2AgentRuntime` creates the orchestrator-owned
 * Execution Session (run-root scope; it launches the worker and is never
 * handed to the worker) and the Tool Session (project scope; its bearer is
 * the worker's only authority) for one agent-state activation. The
 * Execution session reports the worker run result through `runAgent(tool)`
 * (completed, or a typed worker failure) and both sessions expose a
 * `cleanup()`. The cleanup order is fixed: the Tool session first — it
 * revokes the authority handed to the worker — then the Execution
 * session. The runtime never sees transitions, transition targets,
 * accepted records or any right to choose an outcome, and the concrete
 * runtime keeps bearers in private state; the coordinator sees only
 * opaque session objects, ids and lifecycle methods. Runtime and session
 * contract functions are captured exactly once before the first side
 * effect and again right after each Session is created (cleanup first,
 * then the session id, then the run entry point), so swapping methods
 * while a callback is pending cannot change the dispatch; user objects
 * are never frozen or modified.
 *
 * Failure policy is classification by typed errors and context only — no
 * message parsing. Every expected failure becomes the reason of the
 * durable state; an agent execution records `agent_failed` with the two
 * independent session cleanup outcomes (a durable slot says `completed`
 * or `failed`; a never-durably-recorded slot says `not_required`), a
 * decision execution records `decision_failed`, a durably failed session
 * cleanup takes priority and finalizes the run as `run_cleanup_failed`,
 * and everything unexpected is `internal_error`. The cleanup sequence is
 * never interrupted by a first cleanup error: the second session is
 * still cleaned exactly once.
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
 * Signal control is a required, production-neutral synchronous boundary
 * (`PipelineV2CoordinatorControl`): the owner of the signal state (the
 * runner's shared `RunCauseGate`) provides `currentSignal` and
 * `freezeSignal`, both captured exactly once after the provenance gate and
 * before any side effect. Synchronous checkpoints run before the project
 * copy, after the copy, after the input snapshot and at its post-snapshot
 * check (both inside the pre-`create_run` signal handling, so a signal
 * noticed after a successful snapshot still yields the pre-`create_run`
 * signal outcome with no state document and no escaping abort marker),
 * after `create_run`, at the start of every execution, immediately before
 * each Session create, before the worker run, before the decision
 * evaluation and after the engine completes — no await between a
 * checkpoint and the guarded call. A signal accepted up to `create_run`
 * keeps the state document absent; a signal after `create_run` finalizes
 * durably. After the durable state exists, every completion path freezes
 * signal acceptance exactly once through one memoized cutoff call whose
 * snapshot is reused everywhere; the cutoff sits synchronously after
 * cleanup has settled (failure paths) and before the single final
 * run-status write (both terminal results). A durably failed session
 * cleanup outranks a signal, a signal outranks the classified cause, and
 * a frozen signal at the cutoff of a success terminal finalizes the run
 * as a signal failure (published outputs stay). A failed terminal is
 * already determined and cannot be rewritten by a signal. No checkpoint
 * runs between a settled execution and its `transition_committed` write.
 *
 * Resume is not supported: the coordinator accepts only a fresh sink with
 * `snapshot === null` and no poisoning. This module is wired through the
 * production runner (`pipeline_v2_runner.ts`); `agent-smoke`, the CLI and
 * the default pipeline keep executing v1.
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
  prepareRunProject,
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
  type PipelineV2AgentExecutionState,
  type PipelineV2ExecutionState,
  type PipelineV2FailureReason,
  type PipelineV2RunCommand,
  type PipelineV2RunOutputState,
  type PipelineV2RunState,
  type PipelineV2SessionCleanup,
  type PipelineV2SessionCleanupPair,
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
 * Lifecycle result of one worker run, reported by the injected Execution
 * Session. It carries no stdout, no artifacts and no outcome string: after
 * a completed run the engine applies its own fixed `completed` outcome,
 * and a typed worker failure fails the execution.
 */
export type PipelineV2WorkerRunResult =
  | { readonly status: "completed" }
  | { readonly status: "failed"; readonly reason: "worker_failed" | "worker_timeout" };

/**
 * The Tool Session of one activation: project scope, its bearer is the
 * worker's only authority. The coordinator never receives the bearer and
 * only releases the session exactly once.
 */
export interface PipelineV2ToolSession {
  readonly sessionId: string;
  cleanup(): Promise<void>;
}

/**
 * The orchestrator-owned Execution Session of one activation: run-root
 * scope, it launches the worker and is never handed to the worker. The
 * Tool Session object is passed to `runAgent` as the worker's authority;
 * the concrete runtime keeps both bearers in private state.
 */
export interface PipelineV2ExecutionSession {
  readonly sessionId: string;
  runAgent(toolSession: PipelineV2ToolSession): Promise<PipelineV2WorkerRunResult>;
  cleanup(): Promise<void>;
}

/**
 * Production-neutral agent runtime boundary of the two-session capability
 * model. The coordinator captures `createExecutionSession` and
 * `createToolSession` exactly once before their first side effect; the
 * runtime never sees transitions, transition targets, accepted records or
 * any right to choose an outcome.
 */
export interface PipelineV2AgentRuntime {
  createExecutionSession(
    state: V2AgentExecutionView,
    activation: PreparedActivationData,
  ): Promise<PipelineV2ExecutionSession>;

  createToolSession(
    state: V2AgentExecutionView,
    activation: PreparedActivationData,
  ): Promise<PipelineV2ToolSession>;
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
  /** Canonical orchestrator-owned run root. */
  readonly runRoot: string;
  /**
   * The caller-provided project source directory. The coordinator prepares
   * the run-owned `<runRoot>/project` copy from it itself; the source is
   * never modified, its path never enters the pipeline, execution
   * document, worker env/argv, durable state or results, and a ready-made
   * `PreparedRunProject` is never accepted.
   */
  readonly projectSourcePath: string;
  /** The original run-input bindings, bound once by the caller. */
  readonly inputBindings: readonly RunInputBinding[];
  readonly sink: PipelineV2CoordinatorStateSink;
  readonly runtime: PipelineV2AgentRuntime;
}

/**
 * Production-neutral synchronous signal control boundary of the
 * coordinator. `currentSignal` reports the currently accepted run-level
 * signal (null when none); `freezeSignal` atomically snapshots the
 * accepted signal and closes signal acceptance, returning the frozen
 * signal (null when none was accepted). Both functions are captured
 * exactly once by the coordinator after the provenance gate and before
 * the first side effect; rebinding or replacing the control's methods
 * later cannot influence a running coordination. The owner of the
 * underlying signal state is the runner (the shared `RunCauseGate`); the
 * coordinator never registers signal handlers itself.
 */
export interface PipelineV2CoordinatorControl {
  readonly currentSignal: () => "SIGINT" | "SIGTERM" | null;
  readonly freezeSignal: () => "SIGINT" | "SIGTERM" | null;
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

/**
 * Internal abort marker for a signal accepted while coordination was in
 * flight. It carries the signal literal only; the coordinator maps it onto
 * the closed `signal_sigint`/`signal_sigterm` failure reasons and never
 * records the signal in the durable state beyond that reason.
 */
class CoordinatorSignalAbort extends Error {
  readonly signal: "SIGINT" | "SIGTERM";
  constructor(signal: "SIGINT" | "SIGTERM") {
    super(`pipeline v2 coordination aborted by ${signal}`);
    this.name = "CoordinatorSignalAbort";
    this.signal = signal;
  }
}

function signalFailureReason(signal: "SIGINT" | "SIGTERM"): PipelineV2FailureReason {
  return signal === "SIGINT" ? "signal_sigint" : "signal_sigterm";
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

type CapturedCreateExecutionSession = (
  state: V2AgentExecutionView,
  activation: PreparedActivationData,
) => Promise<PipelineV2ExecutionSession>;

type CapturedCreateToolSession = (
  state: V2AgentExecutionView,
  activation: PreparedActivationData,
) => Promise<PipelineV2ToolSession>;

function captureCreateExecutionSession(runtime: PipelineV2AgentRuntime): CapturedCreateExecutionSession {
  return captureContractFunction(
    runtime,
    "createExecutionSession",
    "pipeline v2 agent runtime",
  ) as CapturedCreateExecutionSession;
}

function captureCreateToolSession(runtime: PipelineV2AgentRuntime): CapturedCreateToolSession {
  return captureContractFunction(
    runtime,
    "createToolSession",
    "pipeline v2 agent runtime",
  ) as CapturedCreateToolSession;
}

/**
 * Captures one session contract member exactly once: the property is read
 * one time (a throwing getter is a contract violation, never propagated),
 * checked against the contract, and bound to its owner. Reassigning the
 * property later cannot change the dispatch, and the user object is never
 * frozen or modified. Diagnostics carry no values.
 */
function captureSessionFunction(
  session: PipelineV2ExecutionSession | PipelineV2ToolSession,
  name: "runAgent" | "cleanup",
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
function captureSessionId(
  session: PipelineV2ExecutionSession | PipelineV2ToolSession,
): string {
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
 * reasons (all of which exist in state schema v4) and the store/durability
 * failures keep their reasons; everything else is `internal_error`.
 */
function classifyCause(cause: unknown): PipelineV2FailureReason {
  if (cause instanceof CoordinatorSignalAbort) {
    return signalFailureReason(cause.signal);
  }
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
 * every execution must be settled, a durably failed session cleanup
 * finalizes only with `run_cleanup_failed`, and a failed terminal with
 * published run outputs finalizes only with the terminal failure reason.
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
    hasDurableFailedCleanup(last)
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

/** Mirrors the loader: at least one durable cleanup slot failed. */
function hasDurableFailedCleanup(execution: PipelineV2AgentExecutionState): boolean {
  const cleanup = execution.session_cleanup;
  return cleanup !== undefined && (cleanup.execution === "failed" || cleanup.tool === "failed");
}

/** The two independent cleanup slots of one agent execution. */
interface SessionSlotTracking {
  /** The captured cleanup; null until the session object existed. */
  cleanup: (() => Promise<void>) | null;
  /** The captured cleanup ran exactly once (successfully or not). */
  cleanupDone: boolean;
  cleanupFailed: boolean;
}

interface AgentSessionTracking {
  execution: SessionSlotTracking;
  tool: SessionSlotTracking;
  /** The execution session id was durably recorded. */
  executionCreatedDurably: boolean;
  /** The tool session id was durably recorded. */
  toolCreatedDurably: boolean;
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
  control: PipelineV2CoordinatorControl,
): Promise<PipelineV2CoordinationResult> {
  // The provenance gate is the first statement and runs before any side
  // effect: a forged pipeline is rejected before the sink is read or
  // dispatched, before the runtime callbacks are captured, before the
  // control functions are captured, before any filesystem operation and
  // before any Session. The pipeline is never re-compiled or re-validated
  // here; provenance is the structural trust anchor established by
  // `loadPipelineV2`.
  try {
    requireResolvedPipelineV2Provenance(params.pipeline, "pipeline v2 coordinator");
  } catch {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

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

  // The signal control functions are captured exactly once, together with
  // the runtime contract functions and before the first filesystem or
  // state side effect. Replacing the control's methods later cannot
  // influence this coordination.
  let capturedCurrentSignal: () => "SIGINT" | "SIGTERM" | null;
  let capturedFreezeSignal: () => "SIGINT" | "SIGTERM" | null;
  try {
    capturedCurrentSignal = captureContractFunction(
      control,
      "currentSignal",
      "pipeline v2 coordinator control",
    ) as () => "SIGINT" | "SIGTERM" | null;
    capturedFreezeSignal = captureContractFunction(
      control,
      "freezeSignal",
      "pipeline v2 coordinator control",
    ) as () => "SIGINT" | "SIGTERM" | null;
  } catch {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

  /**
   * Synchronous signal checkpoint: throws the internal abort marker when a
   * signal was accepted. Checkpoints never await; the marker propagates
   * into the failure policy, which maps it onto the signal reason. No
   * checkpoint runs between a settled execution and its
   * `transition_committed` write: a committed transition already belongs
   * to the engine and must stay durable.
   */
  const checkSignal = (): void => {
    const signal = capturedCurrentSignal();
    if (signal !== null) {
      throw new CoordinatorSignalAbort(signal);
    }
  };

  /**
   * The single signal cutoff of the coordination. After the durable run
   * state exists, every completion path freezes signal acceptance exactly
   * once: the first call wins and its snapshot is reused by every later
   * decision — including `finalizeFailure`, which never freezes again. A
   * hostile control whose second `freezeSignal` call would throw or
   * return a different signal therefore never reaches that second call.
   */
  let cutoffTaken = false;
  let cutoffSignal: "SIGINT" | "SIGTERM" | null = null;
  const takeCutoff = (): "SIGINT" | "SIGTERM" | null => {
    if (cutoffTaken) {
      return cutoffSignal;
    }
    cutoffTaken = true;
    cutoffSignal = capturedFreezeSignal();
    return cutoffSignal;
  };

  // The runtime contract functions are captured exactly once, before the
  // first filesystem or state side effect.
  let capturedCreateExecutionSession: CapturedCreateExecutionSession;
  let capturedCreateToolSession: CapturedCreateToolSession;
  try {
    capturedCreateExecutionSession = captureCreateExecutionSession(runtime);
    capturedCreateToolSession = captureCreateToolSession(runtime);
  } catch {
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

  // Phase 0a: the run-owned project copy is prepared by the coordinator
  // from the caller's project source. The source is never modified; the
  // copy is published atomically as `<runRoot>/project`. A preparation
  // failure reaches no command and no Session: the run fails with
  // `run_input_invalid` and no state document, and the staging tree is
  // removed by the data plane. A signal accepted up to and including the
  // copy keeps the state document absent; the signal reason wins over the
  // classified failure (the priority below the session-cleanup failure,
  // which cannot exist before any Session).
  try {
    checkSignal();
    await prepareRunProject(params.projectSourcePath, runRoot);
  } catch (cause) {
    const signal = capturedCurrentSignal();
    if (signal !== null) {
      return deepFreeze({
        ok: false as const,
        reason: signalFailureReason(signal),
        state: null,
      });
    }
    if (cause instanceof PipelineV2RuntimeError && cause.reason === "run_input_invalid") {
      return deepFreeze({ ok: false as const, reason: "run_input_invalid" as const, state: null });
    }
    return deepFreeze({ ok: false as const, reason: "internal_error" as const, state: null });
  }

  // Phase 0: the run-input snapshot is created here, by the coordinator;
  // a caller-provided snapshot is never accepted. An already published
  // project copy stays in the run root for diagnostics when this or the
  // state creation below fails. A signal accepted during the snapshot —
  // or noticed at the post-snapshot checkpoint after it completed — keeps
  // the state document absent, wins over the classified failure and is
  // handled inside this region, so no abort marker can escape the
  // coordinator before `create_run`.
  let runInputs: RunInputsSnapshot;
  try {
    checkSignal();
    runInputs = await snapshotRunInputs(pipeline, params.inputBindings, runRoot);
    // Checkpoint after the successful snapshot: the published project
    // copy and input snapshot stay in the run root; the signal outcome
    // precedes `create_run`, so no state document exists yet.
    checkSignal();
  } catch (cause) {
    const signal = capturedCurrentSignal();
    if (signal !== null) {
      return deepFreeze({
        ok: false as const,
        reason: signalFailureReason(signal),
        state: null,
      });
    }
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

  /**
   * The durable cleanup pair the authoritative record can honestly
   * claim: a slot whose session was never durably recorded (never
   * created, or the durable write was not confirmed) is `not_required`
   * — the record never claims the session existed; a durably recorded
   * slot is `completed` when its cleanup succeeded and `failed` when it
   * did not.
   */
  const durableCleanupPair = (session: AgentSessionTracking | null): PipelineV2SessionCleanupPair => {
    if (session === null) {
      return { execution: "not_required", tool: "not_required" };
    }
    const slotOutcome = (slot: SessionSlotTracking, createdDurably: boolean): PipelineV2SessionCleanup =>
      createdDurably ? (slot.cleanupFailed ? "failed" : "completed") : "not_required";
    return {
      execution: slotOutcome(session.execution, session.executionCreatedDurably),
      tool: slotOutcome(session.tool, session.toolCreatedDurably),
    };
  };

  /** At least one durable cleanup slot failed (mirrors the loader). */
  const durableCleanupFailed = (): boolean => {
    const pair = durableCleanupPair(tracking.session);
    return pair.execution === "failed" || pair.tool === "failed";
  };

  /** The agent_failed command for the still-unfinished agent execution. */
  const agentFailureCommand = (reason: PipelineV2FailureReason): PipelineV2RunCommand => {
    const sessionCleanup = durableCleanupPair(tracking.session);
    // The session cleanup failure reason is used if and only if at least
    // one durable cleanup outcome failed; a durably failed slot forces
    // the priority reason, everything else keeps the classified reason.
    const recordReason = durableCleanupFailed()
      ? PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON
      : executionReasonFor("agent", reason);
    return { kind: "agent_failed", reason: recordReason, sessionCleanup };
  };

  /**
   * Records one failure exactly once, outcome-aware: best-effort cleanup
   * of every created session (the only allowed side effect after a
   * failed durable write), the execution-level failure record for a
   * still-unfinished execution — whose commit must be confirmed before
   * the execution counts as settled — and the run-level finalize. A
   * not-committed failure write never clears the tracking, never
   * triggers a knowingly incompatible run-level write, and reports
   * `state_persist_failed` on the last authoritative snapshot. Exactly
   * one bounded `run_failed: state_persist_failed` attempt follows a
   * not-committed first finalize, only when the reducer still accepts it
   * from the committed snapshot. No recursive finalization; after a
   * durability-unknown commit all further writes stop.
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

    // Best-effort cleanup of every created session, exactly once, tool
    // first; the first error never blocks the second. The cleanup
    // outcomes then decide the session-cleanup failure priority.
    await runSessionCleanups();
    if (durableCleanupFailed()) {
      reason = PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON;
    }

    // The signal cutoff, taken at most once per run: if the terminal
    // phase already froze acceptance, its snapshot is reused here without
    // a second `freezeSignal` call. Cleanup has settled; the cutoff
    // snapshot is read synchronously with no await between reading the
    // accepted signal and closing acceptance. A durably failed session
    // cleanup keeps its first-wins priority over a signal; otherwise an
    // accepted signal becomes the failure reason, outranking the
    // classified cause. Any signal delivered from here on is late and can
    // no longer change the recorded failure.
    const frozenSignal = takeCutoff();
    if (frozenSignal !== null && reason !== PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
      reason = signalFailureReason(frozenSignal);
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

    // The run-level finalize: never a knowingly incompatible command. A
    // durably failed session cleanup finalizes with run_cleanup_failed.
    const cleanupFailedDurably = durableCleanupFailed();
    if (!cleanupFailedDurably) {
      const state = sink.snapshot;
      if (state !== null && !runFailedAdmissible(state, reason)) {
        failureReason = "state_persist_failed";
        return;
      }
    }
    const finalizeCommand: RunFinalizeCommand = cleanupFailedDurably
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
   * The captured cleanups of the current agent execution, run in the
   * fixed order — the Tool session first (it revokes the worker's
   * authority), then the Execution session — each exactly once; the
   * first error never blocks the second.
   */
  const runSessionCleanups = async (): Promise<void> => {
    const session = tracking.session;
    if (session === null) {
      return;
    }
    const cleanupSlot = async (slot: SessionSlotTracking): Promise<void> => {
      if (slot.cleanup === null || slot.cleanupDone) {
        return;
      }
      slot.cleanupDone = true;
      try {
        await slot.cleanup();
      } catch {
        slot.cleanupFailed = true;
      }
    };
    await cleanupSlot(session.tool);
    await cleanupSlot(session.execution);
  };

  // --- the single execution flow through the engine -----------------------

  const executors: PipelineV2GraphExecutors = {
    executeAgent: async (view: V2AgentExecutionView): Promise<void> => {
      // Execution-start checkpoint: a signal accepted between states stops
      // here, before any new execution record or Session; the previous
      // transition is already durable and stays.
      checkSignal();
      tracking.kind = "agent";
      tracking.unfinished = false;
      const sessionTracking: AgentSessionTracking = {
        execution: { cleanup: null, cleanupDone: false, cleanupFailed: false },
        tool: { cleanup: null, cleanupDone: false, cleanupFailed: false },
        executionCreatedDurably: false,
        toolCreatedDurably: false,
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

      // 5. runtime.createExecutionSession (captured once at coordination
      //    start). The session contract is captured exactly once, right
      //    after the Session is created, in a fixed order: the cleanup
      //    first — when a later member violates the contract, the
      //    already-captured cleanup is still runnable exactly once — then
      //    the session id, then the run entry point. All captures happen
      //    before the durable write, so a durably recorded session always
      //    has a captured cleanup. Reassigning any member afterwards
      //    cannot change the dispatch. The signal checkpoint sits
      //    synchronously before the create call: a signal accepted up to
      //    this point never creates a Session.
      checkSignal();
      const executionSession = await capturedCreateExecutionSession(view, activation);
      let executionCleanup: () => Promise<void>;
      try {
        executionCleanup = captureSessionFunction(executionSession, "cleanup") as () => Promise<void>;
      } catch {
        // A missing or non-callable cleanup is a trusted runtime contract
        // violation: nothing runnable exists, so the coordinator must not
        // claim a confirmed cleanup.
        sessionTracking.execution.cleanupDone = true;
        throw new Error(
          "pipeline v2 agent session contract violated: cleanup must be a function",
        );
      }
      sessionTracking.execution.cleanup = executionCleanup;
      const executionSessionId = captureSessionId(executionSession);
      const capturedRunAgent = captureSessionFunction(executionSession, "runAgent") as (
        toolSession: PipelineV2ToolSession,
      ) => Promise<unknown>;

      // 6. agent_execution_session_created — recorded immediately
      await dispatchState({
        kind: "agent_execution_session_created",
        sessionId: executionSessionId,
      });
      sessionTracking.executionCreatedDurably = true;

      // 7. runtime.createToolSession — the worker's authority is created
      //    only after the Execution Session is durably recorded. The
      //    signal checkpoint sits synchronously before the create call.
      checkSignal();
      const toolSession = await capturedCreateToolSession(view, activation);
      let toolCleanup: () => Promise<void>;
      try {
        toolCleanup = captureSessionFunction(toolSession, "cleanup") as () => Promise<void>;
      } catch {
        sessionTracking.tool.cleanupDone = true;
        throw new Error(
          "pipeline v2 agent session contract violated: cleanup must be a function",
        );
      }
      sessionTracking.tool.cleanup = toolCleanup;
      const toolSessionId = captureSessionId(toolSession);

      // 8. agent_tool_session_created — recorded immediately
      await dispatchState({
        kind: "agent_tool_session_created",
        sessionId: toolSessionId,
      });
      sessionTracking.toolCreatedDurably = true;

      // 9. agent_running — both sessions are durably recorded
      await dispatchState({ kind: "agent_running" });

      // 10. executionSession.runAgent(toolSession) — only the lifecycle
      //     result crosses the boundary. The signal checkpoint sits
      //     synchronously before the call with no await in between: a
      //     signal accepted up to this point never starts the worker.
      checkSignal();
      const outcome = await capturedRunAgent(toolSession);
      const parsed = parseWorkerRunResult(outcome);
      if (parsed === undefined) {
        throw new Error(
          "pipeline v2 agent session contract violated: runAgent must return a completed or a typed failed result",
        );
      }
      if (parsed.status === "failed") {
        // Both sessions are cleaned exactly once, tool first; the
        // failure is recorded durably and the engine stops before any
        // transition.
        await runSessionCleanups();
        await finalizeFailure(undefined, parsed.reason);
        throw new CoordinationAbortedError();
      }

      // 11. accept the activation outputs (the worker held them read-write)
      const records = await acceptActivationOutputs(pipeline, activation);

      // 12. agent_outputs_accepted
      await dispatchState({
        kind: "agent_outputs_accepted",
        outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
      });

      // 13./14. cleanup the Tool session, then the Execution session,
      // each exactly once; the first error never blocks the second.
      await runSessionCleanups();

      // A durably failed cleanup becomes the agent failure; the run
      // finalizes with run_cleanup_failed and no transition is recorded.
      if (durableCleanupFailed()) {
        await finalizeFailure(undefined);
        throw new CoordinationAbortedError();
      }

      // 15. agent_cleanup_completed — both sessions were confirmed
      //     cleaned
      await dispatchState({ kind: "agent_cleanup_completed" });
      tracking.unfinished = false;

      // 16. the accepted records join the runner-owned history
      accepted.push(...records);

      // 17. return void: the engine applies its own fixed completed outcome
    },

    executeDecision: async (view: V2DecisionExecutionView): Promise<string> => {
      // Execution-start checkpoint: a signal accepted between states stops
      // here, before any new execution record; the previous transition is
      // already durable and stays.
      checkSignal();
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

      // 4. evaluate the prepared decision state (no second read). The
      //    signal checkpoint sits synchronously before the evaluation.
      checkSignal();
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
    // Checkpoint after create_run: the run state document exists from here
    // on, so a signal accepted now finalizes durably (`run_failed` with
    // the signal reason) instead of leaving the state absent.
    checkSignal();

    const engineResult = await executePipelineV2Graph(pipeline, executors, { onTransitionCommit });

    // Checkpoint after the engine, before the terminal record and the run
    // output publication: a signal accepted while the engine ran stops the
    // terminal phase here; every execution is settled and its transition
    // is durable.
    checkSignal();

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

    // The single signal cutoff of the run, taken once here — synchronously
    // before the final run-status write — and reused by every remaining
    // decision. A signal accepted while the terminal outputs were
    // collected finalizes the run as a signal failure; the already
    // atomically published outputs are not rolled back. A signal
    // delivered from here on — including while the final write is in
    // flight — is late and can no longer change the recorded outcome. For
    // a failed terminal the outcome is already determined: the reducer
    // finalizes a failed terminal with published run outputs with the
    // terminal failure reason only, so a frozen signal cannot rewrite it.
    const cutoff = takeCutoff();
    if (cutoff !== null && engineResult.terminalResult === "success") {
      await finalizeFailure(new CoordinatorSignalAbort(cutoff));
      if (stateAbandoned) {
        return deepFreeze({ ok: false as const, reason: "state_persist_failed" as const, state: sink.snapshot });
      }
      const reason = failureReason ?? signalFailureReason(cutoff);
      return deepFreeze({ ok: false as const, reason, state: sink.snapshot });
    }

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
