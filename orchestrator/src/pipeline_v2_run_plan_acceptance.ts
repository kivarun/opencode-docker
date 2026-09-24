/**
 * The compiled acceptance boundary for run plan candidates (pure,
 * production-neutral, unwired).
 *
 * Answers one fail-closed question: is this prepared run plan candidate
 * ready for the future durable acceptance? The invariant is controller
 * policy, checked here against the compiled data only:
 *
 * > the candidate's plan revision must name, as its `origin_execution`,
 * > the last successful settled-but-unbound agent execution of the
 * > current run, and the compiled execution role of that state must be
 * > `planning`.
 *
 * Fixed order, each gate before any later one:
 *
 * 1. the pipeline provenance gate (`requireResolvedPipelineV2Provenance`)
 *    runs before the state or the candidate are read at all;
 * 2. `compilePipelineV2RunPlanCandidate` gates the candidate provenance
 *    and resolves the stage templates — a forged candidate is rejected
 *    and a missing orchestration section or unknown template raises the
 *    existing typed `PipelineV2OrchestrationError` — both before the
 *    state is touched;
 * 3. `validatePipelineV2RunState` is the single state validator; the
 *    caller's object is never trusted beyond it (a malformed document is
 *    this layer's `invalid_state`, never analyzed further);
 * 4. the exact durable pipeline identity is compared field by field with
 *    the single shared structural comparator
 *    (`pipeline_v2_identity_compare.ts`) — the same comparator the
 *    resume verifier uses; there is no second comparator;
 * 5. the acceptance boundary: an active running run with no terminal,
 *    run outputs, failure or open wait, carrying exactly one
 *    settled-but-unbound execution which is an agent execution in the
 *    `cleanup_completed` phase (all earlier executions are settled and
 *    bound — proven by the state loader's coherence plus the exact
 *    executions/transitions length relation). There is no own cursor
 *    replay: the loader already proved the unbound execution's cursor
 *    coherence;
 * 6. the candidate/run binding: the compiled plan's `run_id` must be the
 *    normalized state's run id;
 * 7. the origin execution binding: the compiled plan's
 *    `origin_execution` must equal the index of the last execution — the
 *    exact last array element, never an arbitrary older execution found
 *    by search;
 * 8. the compiled role of the unbound execution's state, resolved only
 *    through `compiledExecutionRoleFor`; only the `planning` role is
 *    acceptable — the role is never inferred from the execution type,
 *    profile, state name, prompt or graph position.
 *
 * On success the return value is the exact provenance-backed compiled
 * plan object produced in step 2 — the projection is not copied and no
 * second registry is created.
 *
 * Errors stay with their owners: pipeline provenance failures remain the
 * stable `PipelineError` of the pipeline gate; candidate provenance,
 * template and lookup failures remain `PipelineV2CompiledRunPlanError`;
 * missing orchestration metadata and unknown templates remain
 * `PipelineV2OrchestrationError`; unexpected programmer errors propagate
 * unchanged. Only this layer's own failures are
 * `PipelineV2RunPlanAcceptanceError` with the closed reason set
 * (`invalid_state`, `pipeline_mismatch`, `candidate_mismatch`,
 * `origin_execution_mismatch`, `origin_role_mismatch`), assigned by the
 * failing operation's semantics — never by classifying message text.
 * Diagnostics are content-free: they name closed enum values, validated
 * safe ids and counts only — no task bodies, canonical JSON, paths or
 * arbitrary caller values.
 *
 * Nothing here touches the filesystem, the durable state, the reducer,
 * the coordinator, the runner, the CLI, the wait/replanning wiring or
 * schema v7; the loader and reducer are not strengthened and the future
 * controller stays the only caller.
 */
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import {
  comparePipelineV2RunIdentity,
} from "./pipeline_v2_identity_compare.ts";
import { compiledExecutionRoleFor } from "./pipeline_v2_orchestration.ts";
import {
  compilePipelineV2RunPlanCandidate,
  type CompiledPipelineV2RunPlan,
} from "./pipeline_v2_run_plan_compiled.ts";
import type { PreparedPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_candidate.ts";

/** The closed reason set of the compiled run-plan acceptance boundary. */
const PIPELINE_V2_RUN_PLAN_ACCEPTANCE_ERROR_REASONS = [
  "invalid_state",
  "pipeline_mismatch",
  "candidate_mismatch",
  "origin_execution_mismatch",
  "origin_role_mismatch",
] as const;

export type PipelineV2RunPlanAcceptanceErrorReason =
  (typeof PIPELINE_V2_RUN_PLAN_ACCEPTANCE_ERROR_REASONS)[number];

const REASON_SET: ReadonlySet<string> = new Set(PIPELINE_V2_RUN_PLAN_ACCEPTANCE_ERROR_REASONS);

/**
 * A failure of the compiled run-plan acceptance boundary with its stable
 * machine-readable `reason`. The reason is assigned where the failing
 * operation's semantics are known (never by classifying message text),
 * is immutable, and is one of the fixed closed reason set.
 */
export class PipelineV2RunPlanAcceptanceError extends Error {
  declare readonly reason: PipelineV2RunPlanAcceptanceErrorReason;

  constructor(reason: PipelineV2RunPlanAcceptanceErrorReason, message: string) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 run plan acceptance error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2RunPlanAcceptanceError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

const INVALID_STATE_DIAGNOSTIC =
  "verifyPipelineV2RunPlanCandidateForAcceptance requires a durable pipeline v2 run state document; " +
  "the argument is not a valid schema version 6 run state";

/**
 * The acceptance boundary on the already normalized state: an active
 * running run with no terminal, run outputs, failure or open wait,
 * carrying exactly one settled-but-unbound execution which is an agent
 * execution in the `cleanup_completed` phase. Every violation is this
 * layer's `invalid_state`; the diagnostics name closed enum values,
 * validated safe ids and counts only.
 */
function checkAcceptanceBoundary(state: PipelineV2RunState): void {
  const fail = (message: string): PipelineV2RunPlanAcceptanceError =>
    new PipelineV2RunPlanAcceptanceError("invalid_state", message);
  if (state.status !== "active" || state.phase !== "running") {
    throw fail(
      `the run is not on the active running boundary: status ${JSON.stringify(state.status)}, phase ${JSON.stringify(state.phase)}`,
    );
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw fail(
      "the run already carries a terminal, run outputs or a failure; a plan candidate cannot be accepted at a finished run",
    );
  }
  const lastWait = state.waits[state.waits.length - 1];
  if (lastWait !== undefined && lastWait.response === undefined) {
    throw fail(
      "the run carries an open wait; a plan candidate cannot be accepted while the run waits for a user response",
    );
  }
  if (state.executions.length !== state.transitions.length + 1) {
    throw fail(
      `the acceptance boundary requires exactly one settled-but-unbound execution, got ${state.executions.length} executions and ${state.transitions.length} committed transitions`,
    );
  }
  const unbound = state.executions[state.executions.length - 1];
  if (unbound === undefined) {
    throw fail("the run carries no execution to accept a plan candidate");
  }
  if (unbound.type !== "agent") {
    throw fail(
      `the last execution is a decision execution for state ${JSON.stringify(unbound.state_id)}; plan acceptance requires a settled agent execution`,
    );
  }
  if (unbound.phase !== "cleanup_completed") {
    throw fail(
      `the agent execution for state ${JSON.stringify(unbound.state_id)} is in phase ${JSON.stringify(unbound.phase)}; plan acceptance requires the settled phase "cleanup_completed"`,
    );
  }
}

/**
 * Verifies that the prepared run plan candidate is ready for the future
 * durable acceptance and returns the exact provenance-backed compiled
 * plan projection produced by `compilePipelineV2RunPlanCandidate`.
 */
export function verifyPipelineV2RunPlanCandidateForAcceptance(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
  candidate: PreparedPipelineV2RunPlanCandidate,
): CompiledPipelineV2RunPlan {
  // Gate 1: pipeline provenance before the state or candidate are read.
  requireResolvedPipelineV2Provenance(
    pipeline,
    "verifyPipelineV2RunPlanCandidateForAcceptance",
  );
  // Gate 2: candidate provenance and template resolution, before the state.
  const compiledPlan = compilePipelineV2RunPlanCandidate(pipeline, candidate);
  // Gate 3: the single state validator; the caller object is not trusted.
  let normalized: PipelineV2RunState;
  try {
    normalized = validatePipelineV2RunState(state);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw new PipelineV2RunPlanAcceptanceError("invalid_state", INVALID_STATE_DIAGNOSTIC);
    }
    throw cause;
  }
  // Gate 4: the exact durable pipeline identity, via the one shared
  // comparator (no second comparator, no message parsing).
  const comparison = comparePipelineV2RunIdentity(
    pipelineV2RunPipelineIdentity(pipeline),
    normalized.pipeline,
  );
  if (comparison.kind === "mismatch") {
    throw new PipelineV2RunPlanAcceptanceError(
      "pipeline_mismatch",
      `the durable run state was created for a different pipeline: the compiled identity field ${JSON.stringify(comparison.field)} differs`,
    );
  }
  // Gate 5: the acceptance boundary on the normalized state.
  checkAcceptanceBoundary(normalized);
  // Gate 6: the candidate belongs to this run.
  if (compiledPlan.run_id !== normalized.run_id) {
    throw new PipelineV2RunPlanAcceptanceError(
      "candidate_mismatch",
      `the compiled plan candidate belongs to run ${JSON.stringify(compiledPlan.run_id)}; the durable run is ${JSON.stringify(normalized.run_id)}`,
    );
  }
  // Gate 7: the origin is exactly the last execution — the last array
  // element, never an arbitrary older execution found by search.
  const unbound = normalized.executions[normalized.executions.length - 1];
  if (unbound === undefined || compiledPlan.origin_execution !== unbound.index) {
    throw new PipelineV2RunPlanAcceptanceError(
      "origin_execution_mismatch",
      `the compiled plan's origin_execution ${compiledPlan.origin_execution} does not name the last settled-but-unbound execution`,
    );
  }
  // Gate 8: the compiled role of the origin state, resolved only through
  // the trusted orchestration resolver; only "planning" is acceptable.
  const role = compiledExecutionRoleFor(pipeline, unbound.state_id);
  if (role.role !== "planning") {
    throw new PipelineV2RunPlanAcceptanceError(
      "origin_role_mismatch",
      `the compiled execution role of state ${JSON.stringify(unbound.state_id)} is ${JSON.stringify(role.role)}; plan acceptance requires the "planning" role`,
    );
  }
  return compiledPlan;
}
