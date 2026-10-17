import {
  compiledTransitionFor,
  PipelineExecutionError,
  type TransitionStep,
} from "./pipeline_engine.ts";
import {
  PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON,
  PIPELINE_V2_TERMINAL_FAILURE_REASON,
  type PipelineV2AgentExecutionState,
  type PipelineV2DecisionExecutionState,
  type PipelineV2ExecutionState,
  type PipelineV2FailureReason,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import type { ResolvedPipelineV2 } from "./pipeline_v2.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/**
 * The single authoritative recovery classifier of the pipeline v2 resume
 * coordinator: a pure, deterministic, catch-free classification of one
 * loader-validated durable run state into the exact missing durable
 * suffix a resumed coordination must complete after a post-commit crash.
 *
 * Structural coherence is owned by the state loader — this classifier
 * never re-validates the document. It checks only the exact bindings
 * needed to choose one recovery suffix (the settled-but-unbound last
 * execution and its compiled transition, the durable failure reason of a
 * failed execution, the durable terminal record) and derives every
 * recovery decision from the trusted compiled pipeline snapshot and the
 * durable state alone. A settled-unbound planning execution is
 * deliberately NOT a recovery window: it stays owned by the existing
 * plan-ready chains (`resume-plan` / the trusted auto continuation), so
 * the classification returns `none` for it and the established planning
 * behavior is untouched.
 *
 * The classification kinds:
 *
 *   - `none` — the existing resume chains own the boundary: an ordinary
 *     resumable boundary (every execution bound to its transition), a
 *     settled-but-unbound planning execution (the plan-ready boundary),
 *     a genuinely in-flight execution (never recoverable in this
 *     increment), a waiting run, a finalized run and every other shape.
 *   - `pipeline-mismatch` — the compiled transition of a settled
 *     unbound execution does not resolve through the engine's own
 *     compiled resolver (an unknown decision outcome recorded by a
 *     forged or foreign durable document). The resume refuses
 *     `pipeline_mismatch` before any dispatch.
 *   - `transition` — a settled stage or control execution is waiting for
 *     its exactly one `transition_committed` commit; the step is resolved
 *     through `compiledTransitionFor` from the execution's own durable
 *     outcome contract (the fixed agent lifecycle outcome `completed`,
 *     or the durable decision record's outcome) — never re-derived from
 *     facts, rules, worker output or free text.
 *   - `run-finalization` — the last execution is durably failed while the
 *     run is still active; the missing suffix is exactly one run-level
 *     finalization command carrying the durable failure reason (or the
 *     cleanup failure finalization when the durable cleanup pair failed).
 *   - `terminal` — the terminal lifecycle of a reached (or about-to-be
 *     reached) terminal is incomplete: the missing suffix is the
 *     `terminal_reached` record (when absent), the run-output publication
 *     (when the durable record is absent; an already published orphan
 *     tree is adopted through the existing publisher), and the final run
 *     status command derived from the durable terminal result.
 *
 * Diagnostics are content-free: no file bodies, no digests beyond safe
 * ids and indexes, no worker output. The classifier is shared by the
 * coordinator's resume flow and by the restore's resumable-boundary
 * policy, so the restore accepts exactly the windows the coordinator
 * recovers — one authority, no second validator.
 */

/**
 * The run-level commands a recovery suffix may dispatch: the failure
 * finalizations (the durable failure reason, or the cleanup-failure
 * finalization) and the success finalization of an adopted terminal.
 */
export type PipelineV2RecoveryFinalizeCommand =
  | { readonly kind: "run_failed"; readonly reason: PipelineV2FailureReason }
  | { readonly kind: "run_cleanup_failed" }
  | { readonly kind: "run_succeeded" };

/**
 * The executable recovery classifications the continuation chain
 * completes; `none` and `pipeline-mismatch` are decided before the chain.
 *
 * The two worker-log windows (schema v9) recover an agent execution whose
 * worker already ran in a previous process — the worker is never re-run:
 *
 *   - `worker-settled` — the durable acceptance is complete (phase
 *     `outputs_accepted`); the missing suffix is exactly the session
 *     cleanup of the two durably recorded sessions and the execution's
 *     transition. The binding and outputs stay untouched.
 *   - `agent-in-flight` — the execution is in phase `running` (the worker
 *     operation was in flight or had finished when the process died): the
 *     worker outcome is unverifiable without a re-run, so the recovery
 *     adopts the published log artifact when one exists (C5), records the
 *     honest binding, deletes both durable sessions and fails the
 *     execution (`agent_failed`), after which the run finalizes with the
 *     existing failure semantics. Phases before the worker ran stay the
 *     existing refusal (C0: the existing recovery behavior).
 */
export type PipelineV2ExecutableRecovery = Extract<
  PipelineV2RecoveryClassification,
  { kind: "transition" | "run-finalization" | "terminal" | "worker-settled" | "agent-in-flight" }
>;

/** The failure-only finalization commands of the `run-finalization` window. */
export type PipelineV2RecoveryFailureCommand = Extract<
  PipelineV2RecoveryFinalizeCommand,
  { kind: "run_failed" | "run_cleanup_failed" }
>;

/** The classification result (deep-frozen; never mutated by callers). */
export type PipelineV2RecoveryClassification =
  | { readonly kind: "none" }
  | { readonly kind: "pipeline-mismatch" }
  | {
      readonly kind: "transition";
      /** The global index of the settled execution awaiting its transition. */
      readonly executionIndex: number;
      /** The authoritative compiled transition step to commit. */
      readonly step: TransitionStep;
    }
  | {
      readonly kind: "run-finalization";
      /** The exactly one missing run-level finalization command. */
      readonly command: PipelineV2RecoveryFailureCommand;
    }
  | {
      readonly kind: "terminal";
      readonly terminalStateId: string;
      readonly terminalResult: "success" | "failed";
      /** `terminal_reached` is already durable. */
      readonly terminalRecorded: boolean;
      /** `run_outputs_published` is already durable. */
      readonly outputsRecorded: boolean;
      /** The final run-status command after the (remaining) publication. */
      readonly finalize: PipelineV2RecoveryFinalizeCommand;
    }
  | {
      /** The accepted agent execution (schema v9): cleanup + transition missing. */
      readonly kind: "worker-settled";
      readonly executionIndex: number;
      readonly stateId: string;
      readonly executionSessionId: string;
      readonly toolSessionId: string;
    }
  | {
      /** The in-flight agent execution at phase `running`: adopt the artifact, fail the execution. */
      readonly kind: "agent-in-flight";
      readonly executionIndex: number;
      readonly stateId: string;
      readonly executionSessionId: string;
      readonly toolSessionId: string;
    };

const NONE: PipelineV2RecoveryClassification = deepFreezeValue({ kind: "none" });
const PIPELINE_MISMATCH: PipelineV2RecoveryClassification = deepFreezeValue({ kind: "pipeline-mismatch" });

/** The compiled transition of one settled unbound execution, or a typed mismatch. */
function resolveRecoveryTransition(
  pipeline: ResolvedPipelineV2,
  stateId: string,
  outcome: string,
): { readonly step: TransitionStep } | { readonly mismatch: true } {
  try {
    return { step: compiledTransitionFor(pipeline, stateId, outcome) };
  } catch (cause) {
    // Typed translation of the engine's own resolver failure — never a
    // classification by message text: the engine throws the typed
    // `missing_state` / `unknown_outcome` pipeline execution errors, and
    // only that exact class maps to the resume refusal.
    if (cause instanceof PipelineExecutionError) {
      return { mismatch: true };
    }
    throw cause;
  }
}

/** The settled phases of the two execution types (mirrors the loader). */
function isSettledExecution(execution: PipelineV2ExecutionState): boolean {
  if (execution.type === "agent") {
    return execution.phase === "cleanup_completed" || execution.phase === "failed";
  }
  return execution.phase === "evaluated" || execution.phase === "failed";
}

/** The transition outcome of one durable decision execution record. */
function decisionRecordOutcome(execution: PipelineV2DecisionExecutionState): string | undefined {
  return execution.result?.outcome;
}

/** The durable cleanup pair of an agent execution failed in at least one slot. */
function hasDurableFailedCleanup(execution: PipelineV2AgentExecutionState): boolean {
  const cleanup = execution.session_cleanup;
  return cleanup !== undefined && (cleanup.execution === "failed" || cleanup.tool === "failed");
}

/**
 * Classifies one loader-validated durable run state for the recovery
 * suffix selection. Pure, deterministic and repeatable; the state and
 * the pipeline are never mutated.
 */
export function classifyPipelineV2ResumeRecovery(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
): PipelineV2RecoveryClassification {
  // Only an active run can be missing a durable suffix; a waiting or a
  // finalized run stays owned by the existing refusal behavior.
  if (state.status !== "active") {
    return NONE;
  }
  if (state.terminal !== undefined) {
    // The terminal lifecycle windows: the durable terminal record exists,
    // so only the publication and the final status can be missing. The
    // loader guarantees phase `running` (publication not yet recorded) or
    // `publishing_outputs` (outputs recorded) for a reached terminal; a
    // divergent shape is defensive `none` — the existing restore refusal
    // stays the owner of anything the loader does not admit.
    if (state.run_outputs !== undefined) {
      if (state.phase !== "publishing_outputs") {
        return NONE;
      }
      return deepFreezeValue({
        kind: "terminal" as const,
        terminalStateId: state.terminal.state_id,
        terminalResult: state.terminal.result,
        terminalRecorded: true,
        outputsRecorded: true,
        finalize:
          state.terminal.result === "failed"
            ? ({ kind: "run_failed", reason: PIPELINE_V2_TERMINAL_FAILURE_REASON } as const)
            : ({ kind: "run_succeeded" } as const),
      });
    }
    if (state.phase !== "running") {
      return NONE;
    }
    return deepFreezeValue({
      kind: "terminal" as const,
      terminalStateId: state.terminal.state_id,
      terminalResult: state.terminal.result,
      terminalRecorded: true,
      outputsRecorded: false,
      finalize:
        state.terminal.result === "failed"
          ? ({ kind: "run_failed", reason: PIPELINE_V2_TERMINAL_FAILURE_REASON } as const)
          : ({ kind: "run_succeeded" } as const),
    });
  }
  if (state.phase !== "running") {
    // A publishing phase without a terminal record is not a loader-valid
    // shape; the existing refusal stays the owner.
    return NONE;
  }
  const executions = state.executions;
  const transitions = state.transitions;
  if (executions.length !== transitions.length + 1) {
    // Every execution is bound to its committed transition (or the run
    // has none): the ordinary resumable boundary — including the cursor
    // resting on a terminal state — is the existing resume chain.
    return NONE;
  }
  const last = executions[executions.length - 1];
  if (last === undefined || last.state_id !== state.cursor.current_state) {
    // The loader binds the unbound execution to the cursor; a divergent
    // shape is defensive `none`.
    return NONE;
  }
  if (last.phase === "failed") {
    // The durable failure record is authoritative; the missing suffix is
    // exactly one run-level finalization carrying the recorded reason.
    const reason = last.failure_reason;
    if (reason === undefined) {
      return NONE;
    }
    if (reason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
      return deepFreezeValue({
        kind: "run-finalization" as const,
        command: { kind: "run_cleanup_failed" } as const,
      });
    }
    if (last.type === "agent" && hasDurableFailedCleanup(last)) {
      return deepFreezeValue({
        kind: "run-finalization" as const,
        command: { kind: "run_cleanup_failed" } as const,
      });
    }
    return deepFreezeValue({
      kind: "run-finalization" as const,
      command: { kind: "run_failed", reason } as const,
    });
  }
  if (last.type === "agent") {
    // A settled-but-unbound planning execution is the plan-ready
    // boundary — the existing planning chains own it, never a generic
    // transition recovery.
    if (last.execution_role === "planning") {
      return NONE;
    }
    if (last.execution_role === "stage") {
      // The worker-log recovery windows (schema v9) sit BEFORE the
      // settled gate: phases `running` and `outputs_accepted` are exactly
      // the post-worker crash windows this recovery owns — the worker
      // already ran in a previous process and is never re-run. Both
      // windows require the two durably recorded sessions; a divergent
      // shape is defensive `none`.
      if (last.phase === "running") {
        if (last.execution_session_id === undefined || last.tool_session_id === undefined) {
          return NONE;
        }
        return deepFreezeValue({
          kind: "agent-in-flight" as const,
          executionIndex: last.index,
          stateId: last.state_id,
          executionSessionId: last.execution_session_id,
          toolSessionId: last.tool_session_id,
        });
      }
      if (last.phase === "outputs_accepted") {
        if (last.execution_session_id === undefined || last.tool_session_id === undefined) {
          return NONE;
        }
        return deepFreezeValue({
          kind: "worker-settled" as const,
          executionIndex: last.index,
          stateId: last.state_id,
          executionSessionId: last.execution_session_id,
          toolSessionId: last.tool_session_id,
        });
      }
    }
  }
  if (!isSettledExecution(last)) {
    // A genuinely in-flight execution (prepared, sessions created,
    // running, evaluated-without-record) is never recovered in this
    // increment: the existing refusal stays the owner.
    return NONE;
  }
  if (last.type === "agent") {
    if (last.execution_role !== "stage") {
      return NONE;
    }
    // The stage agent's lifecycle outcome is the engine's own fixed
    // `completed` — the same contract the live coordinator's engine
    // callback returns; it is never re-derived from worker output.
    const resolved = resolveRecoveryTransition(pipeline, last.state_id, "completed");
    if ("mismatch" in resolved) {
      return PIPELINE_MISMATCH;
    }
    return deepFreezeValue({
      kind: "transition" as const,
      executionIndex: last.index,
      step: resolved.step,
    });
  }
  if (last.execution_role !== "control") {
    return NONE;
  }
  const outcome = decisionRecordOutcome(last);
  if (outcome === undefined) {
    return NONE;
  }
  const resolved = resolveRecoveryTransition(pipeline, last.state_id, outcome);
  if ("mismatch" in resolved) {
    return PIPELINE_MISMATCH;
  }
  return deepFreezeValue({
    kind: "transition" as const,
    executionIndex: last.index,
    step: resolved.step,
  });
}
