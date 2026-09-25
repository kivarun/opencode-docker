/**
 * Durable run state for pipeline schema v2 (state schema version 7).
 *
 * Pure substrate: a versioned state document, an exact-field loader, and a
 * pure command reducer. Nothing here touches the filesystem, docker-helper,
 * Sessions, or the production runner; `pipeline_state.ts` stays the
 * production run-state contract of pipeline schema v1 and is not wired to
 * any of this. There are no migrations between state versions in either
 * direction.
 *
 * Schema version 7 adds the plan-driven stage lifecycle on top of the v6
 * wait journal: every agent and decision execution carries a mandatory
 * `execution_role` (`planning | control | stage`, fixed at start and never
 * rewritten) with the role contract for `iteration_index` (present exactly
 * for stage executions, referencing the open iteration at the start).
 * Generations (`generations[]`) are separate durable records binding a plan
 * stage (`stage_id`, `stage_position`, `template_id`, `plan_sha256`) to an
 * immutable `initial_budget`; iterations live inside their generation as an
 * append-only `iterations[]` list with open/close anchors. The accepted
 * plan revisions (`plan_revisions[]`), task revisions (`task_revisions[]`)
 * and iteration grants (`grants[]`) are top-level append-only ledgers of
 * content-free refs; the accepted wait intent is recorded inside the open
 * wait record itself (`waits[].intent`). The effective iteration budget of
 * a generation is derived — never stored — as
 * `initial_budget + sum of that generation's grant.additional_iterations`,
 * so a grant can never retroactively make already-invalidated history
 * valid. The reducer and the loader enforce the identical successor rules
 * from their two sides: the reducer at write time, the loader by a single
 * joint replay.
 *
 * The post-failure successor contract is unified: once the run's last
 * execution (agent or decision) has failed, the only durable successor is
 * the run failure finalization — `run_failed` for the ordinary failure,
 * `run_cleanup_failed` for the agent failure with an unconfirmed session
 * cleanup, each decided by its own existing case rules. No lifecycle,
 * task/plan, wait, execution, transition, terminal or publication command
 * is accepted after a failed execution, and the stage generation/iteration
 * openings additionally require the boundary's last execution to be
 * settled, so a reducer-produced document can never place an opening
 * before the failure it later carries. The loader verifies only provable
 * positional coherence for the same rules: a plan revision's planning
 * origin must be cleanly settled (`cleanup_completed`), a failed stage
 * execution's referenced iteration must still be genuinely open at its
 * start boundary, and no generation/iteration opening may share the start
 * boundary of a failed planning/control execution. Revision-1 task
 * revisions are position-free records: the reducer forbids adding them
 * after a failure, and the loader claims no impossible temporal check for
 * records that carry no anchor.
 *
 * The loader is a single
 * positional replay that orders generations, iterations, grants, task and
 * plan revisions, executions and the joint transition+wait cursor
 * replay by their recorded anchors. There are no event journals, no second
 * cursor, no second transition journal, no durable registry of templates
 * or roles; the execution snapshot digest stays the single durable anchor
 * of the compiled pipeline, and matching a durable role to a concrete
 * compiled state is the job of the coordinator/controller and the restore
 * verifier — never of this module.
 *
 * Schema version 6 adds the durable user-response successor for the user
 * wait. The single optional top-level `wait` record of schema v5 is
 * replaced by the required wait journal `waits` — an ordered list of
 * content-free wait records (`PipelineV2WaitRecord`), each carrying its
 * journal `index` (contiguous from 1), the `transition_count` of committed
 * graph transitions at the moment the run entered the wait, the waiting
 * graph state, a normalized policy reason (for example the P01 reason
 * `stage_iteration_limit_exhausted`), the SHA-256 of the future
 * orchestrator-owned user request manifest, and the declared wait actions
 * (`{id, to}` in declaration order; several action ids may target the same
 * state). An open record (no `response`) marks `status: "waiting"` /
 * `phase: "waiting"`; `run_waiting` appends a new open record on a clean
 * boundary, and the one new command `wait_response_recorded` atomically
 * closes the open record with a content-free response
 * (`{action_id, response_sha256}`), returns the run to
 * `active`/`running`, and moves the cursor to the declared action target
 * without touching the transition count, the transition budget, or any
 * history. `waits` is the single authoritative journal of waits and
 * responses: there is no `events[]`, no separate response journal, and no
 * current-wait pointer. The loader re-derives the cursor by a joint replay
 * of the committed transitions and the wait journal. Both digests
 * (`request_sha256`, `response_sha256`) reserve the link to future
 * orchestrator-owned manifests; the reducer itself trusts only an already
 * validated command from that future policy/controller layer and records
 * no user intent payload. No paths, evidence bodies, TASK/PLAN content,
 * facts, prompts, worker output, environment values, profiles or
 * credentials ever enter the document.
 *
 * Deliberate difference from state schema v2: there is NO event journal.
 * `executions`, `transitions`, `waits`, `terminal` and `run_outputs` are
 * the only authoritative journal of the run, and the loader re-derives
 * every invariant from those records in both directions (no second event
 * model, no bidirectional event/record coherence layer). Audit and
 * observation streams are a separate later layer.
 *
 * Two-session capability model: one agent execution durably records two
 * independent session ids — the orchestrator-owned Execution Session
 * (scope: the run root; it launches the worker and is never handed to the
 * worker) and the Tool Session (scope: the project; its bearer is the
 * worker's only authority). Session ids are not secrets and are durable;
 * bearers, endpoints and credentials never enter the document. Each slot
 * has its own cleanup outcome, and one id can never be reused — not even
 * once as an Execution and once as a Tool session.
 *
 * Runtime layout paths (`run_root`, `project_root`, snapshot and activation
 * paths) are intentionally absent: they belong to the fixed runtime layout
 * and are derived from the run directory and the record indexes. Agent
 * executions reuse the shared execution `index` as their activation index
 * (decision executions occupy an index without creating a directory), so
 * activation directories may have gaps.
 */
import type { DecisionFactValidationReason } from "./decision.ts";
import type { TransitionStep } from "./pipeline_engine.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";

export const PIPELINE_V2_RUN_STATE_SCHEMA_VERSION = 7;

/** The closed execution-role vocabulary fixed on every execution at start. */
export const PIPELINE_V2_EXECUTION_ROLES = ["planning", "control", "stage"] as const;
export type PipelineV2ExecutionRole = (typeof PIPELINE_V2_EXECUTION_ROLES)[number];

/** The closed close-reason vocabulary of one stage iteration. */
export const PIPELINE_V2_STAGE_ITERATION_CLOSE_REASONS = [
  "grant",
  "replanned",
  "normal_close",
  "exhausted",
] as const;
export type PipelineV2StageIterationCloseReason =
  (typeof PIPELINE_V2_STAGE_ITERATION_CLOSE_REASONS)[number];

/** The close reasons that bind the iteration closure to an open user wait. */
const WAIT_BOUND_ITERATION_CLOSE_REASONS: readonly PipelineV2StageIterationCloseReason[] = [
  "grant",
  "replanned",
];

/** The closed close-reason vocabulary of one stage generation. */
export const PIPELINE_V2_STAGE_GENERATION_CLOSE_REASONS = [
  "next_stage",
  "final_stage",
  "replanned",
] as const;
export type PipelineV2StageGenerationCloseReason =
  (typeof PIPELINE_V2_STAGE_GENERATION_CLOSE_REASONS)[number];

export const PIPELINE_V2_RUN_STATUSES = [
  "active",
  "waiting",
  "success",
  "failed",
  "cleanup_failed",
] as const;
export type PipelineV2RunStatus = (typeof PIPELINE_V2_RUN_STATUSES)[number];

export const PIPELINE_V2_RUN_PHASES = ["running", "waiting", "publishing_outputs", "finished"] as const;
export type PipelineV2RunPhase = (typeof PIPELINE_V2_RUN_PHASES)[number];

export const PIPELINE_V2_AGENT_EXECUTION_PHASES = [
  "started",
  "data_prepared",
  "execution_session_created",
  "sessions_created",
  "running",
  "outputs_accepted",
  "cleanup_completed",
  "failed",
] as const;
export type PipelineV2AgentExecutionPhase = (typeof PIPELINE_V2_AGENT_EXECUTION_PHASES)[number];

export const PIPELINE_V2_DECISION_EXECUTION_PHASES = ["evaluating", "evaluated", "failed"] as const;
export type PipelineV2DecisionExecutionPhase = (typeof PIPELINE_V2_DECISION_EXECUTION_PHASES)[number];

export const PIPELINE_V2_FAILURE_REASONS = [
  "internal_error",
  "run_input_invalid",
  "run_input_modified",
  "activation_prepare_failed",
  "worker_failed",
  "worker_timeout",
  "activation_output_invalid",
  "accepted_output_modified",
  "decision_input_invalid",
  "unknown_outcome",
  "invalid_outcome",
  "invalid_executor",
  "transition_budget_exhausted",
  "invalid_graph",
  "missing_state",
  "run_output_missing",
  "run_output_invalid",
  "run_output_publish_failed",
  "state_persist_failed",
  "session_cleanup_failed",
  "signal_sigint",
  "signal_sigterm",
  "terminal_failed",
] as const;
export type PipelineV2FailureReason = (typeof PIPELINE_V2_FAILURE_REASONS)[number];

export const PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON: PipelineV2FailureReason =
  "session_cleanup_failed";
export const PIPELINE_V2_TERMINAL_FAILURE_REASON: PipelineV2FailureReason = "terminal_failed";

/**
 * Failure reasons that only ever belong to the run, never to a single
 * execution: `terminal_failed` is the finalization of a published failed
 * terminal, run-output reasons are publication failures, and
 * `state_persist_failed` is a durable-state write failure.
 */
const RUN_ONLY_FAILURE_REASONS: readonly PipelineV2FailureReason[] = [
  "terminal_failed",
  "run_output_missing",
  "run_output_invalid",
  "run_output_publish_failed",
  "state_persist_failed",
];

/**
 * Failure reasons an agent execution can record. Everything that fires
 * before an execution can start (provenance, graph shape, executor contract,
 * transition budget) is a run-level failure; a decision execution has no
 * Session, no worker, and no activation preparation.
 */
export const PIPELINE_V2_AGENT_EXECUTION_FAILURE_REASONS = [
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
] as const;

export const PIPELINE_V2_DECISION_EXECUTION_FAILURE_REASONS = [
  "internal_error",
  "run_input_invalid",
  "run_input_modified",
  "accepted_output_modified",
  "decision_input_invalid",
  "unknown_outcome",
  "invalid_outcome",
  "signal_sigint",
  "signal_sigterm",
] as const;

export const PIPELINE_V2_PORT_TYPES = ["file", "directory", "json"] as const;
export type PipelineV2PortType = (typeof PIPELINE_V2_PORT_TYPES)[number];

export type PipelineV2SessionCleanup = "not_required" | "completed" | "failed";

const PIPELINE_V2_SESSION_CLEANUP_VALUES: readonly PipelineV2SessionCleanup[] = [
  "not_required",
  "completed",
  "failed",
];

/**
 * Independent cleanup outcomes of the two durable sessions of one agent
 * execution. Each slot obeys the same biconditional with its durable
 * session id: no durable id -> only "not_required"; a durable id ->
 * "completed" or "failed".
 */
export interface PipelineV2SessionCleanupPair {
  execution: PipelineV2SessionCleanup;
  tool: PipelineV2SessionCleanup;
}

/** Whether at least one durable cleanup slot failed (mirrors the loader). */
function hasFailedSessionCleanup(execution: PipelineV2AgentExecutionState): boolean {
  const cleanup = execution.session_cleanup;
  return cleanup !== undefined && (cleanup.execution === "failed" || cleanup.tool === "failed");
}

export interface PipelineV2RunPipelineIdentity {
  schema_version: 2;
  bundle_root: string;
  execution_snapshot_sha256: string;
  entry_state: string;
  max_transitions: number;
}

export interface PipelineV2RunInputState {
  id: string;
  type: PipelineV2PortType;
  protected: boolean;
  digest: string;
}

export interface PipelineV2RunCursorState {
  current_state: string;
  transition_count: number;
}

/** Digest of one accepted agent output port; paths, types and content stay out. */
export interface PipelineV2AgentOutputState {
  id: string;
  digest: string;
}

export interface PipelineV2AgentExecutionState {
  index: number;
  type: "agent";
  state_id: string;
  attempt: number;
  profile: string;
  /** The compiled execution role, fixed at start and never rewritten. */
  execution_role: PipelineV2ExecutionRole;
  /** The open iteration a stage execution runs in; absent for planning/control. */
  iteration_index?: number;
  phase: PipelineV2AgentExecutionPhase;
  /** Durable id of the orchestrator-owned Execution Session (run-root scope). */
  execution_session_id?: string;
  /** Durable id of the Tool Session (project scope; the worker's bearer). */
  tool_session_id?: string;
  /** Independent cleanup outcomes of both durable sessions. */
  session_cleanup?: PipelineV2SessionCleanupPair;
  outputs?: PipelineV2AgentOutputState[];
  failure_reason?: PipelineV2FailureReason;
}

export interface PipelineV2DecisionExecutionState {
  index: number;
  type: "decision";
  state_id: string;
  /** The compiled execution role, fixed at start and never rewritten. */
  execution_role: PipelineV2ExecutionRole;
  /** The open iteration a stage execution runs in; absent for planning/control. */
  iteration_index?: number;
  phase: PipelineV2DecisionExecutionPhase;
  input_digest: string;
  result?: PipelineDecisionStateRecord;
  failure_reason?: PipelineV2FailureReason;
}

export type PipelineV2ExecutionState =
  | PipelineV2AgentExecutionState
  | PipelineV2DecisionExecutionState;

export interface PipelineV2CommittedTransitionState {
  /** The engine's original transition index within the source state. */
  index: number;
  from: string;
  outcome: string;
  to: string;
  /** The single completed execution this transition commits. */
  execution_index: number;
}

export interface PipelineV2TerminalState {
  state_id: string;
  result: "success" | "failed";
}

export type PipelineV2RunOutputState =
  | { id: string; type: PipelineV2PortType; required: boolean; present: true; digest: string }
  | { id: string; type: PipelineV2PortType; required: false; present: false };

export interface PipelineV2FailureState {
  reason: PipelineV2FailureReason;
}

/**
 * One declared wait action: a stable action id and the graph state the
 * accepted user response may route to. Declaration order is preserved and
 * several action ids may target the same state.
 */
export interface PipelineV2WaitAction {
  readonly id: string;
  readonly to: string;
}

/**
 * Content-free durable record of the user's accepted response to one wait
 * (schema v6): the declared action id the response selected and the
 * SHA-256 of the future orchestrator-owned response manifest. The routing
 * target itself is never duplicated here — it is always the declared
 * `to` of the referenced action. No response body, evidence, TASK/PLAN
 * content, facts, paths, environment values, profiles or credentials ever
 * enter the record.
 */
export interface PipelineV2WaitResponseRecord {
  readonly action_id: string;
  readonly response_sha256: string;
}

/**
 * Content-free durable record of one user wait (schema v6). The record
 * carries its journal `index` (contiguous from 1), the count of committed
 * graph transitions at the moment the run entered the wait, the waiting
 * graph state, a normalized policy reason (the P01 reason
 * `stage_iteration_limit_exhausted` belongs to the policy/context stage,
 * never to the engine transition budget), the SHA-256 of the
 * orchestrator-owned user request manifest that a later increment
 * publishes on the data plane, and the declared actions. An open record
 * (no `response`) marks the run as waiting; the response closes the same
 * record atomically. No user response body, evidence, TASK/PLAN content,
 * facts, prompts, paths, environment values, profiles or credentials ever
 * enter the document.
 */
export interface PipelineV2WaitRecord {
  readonly index: number;
  /** Committed graph transitions at the moment the run entered the wait. */
  readonly transition_count: number;
  readonly state_id: string;
  readonly reason: string;
  readonly request_sha256: string;
  readonly actions: readonly PipelineV2WaitAction[];
  /** The accepted wait intent, recorded inside the open wait record (schema v7). */
  readonly intent?: PipelineV2WaitIntentRecord;
  readonly response?: PipelineV2WaitResponseRecord;
}

/**
 * Content-free durable record of one accepted user wait intent (schema
 * v7): the SHA-256 of the orchestrator-owned intent manifest, published on
 * the data plane before the durable acceptance. The reducer accepts the
 * digest but never interprets the intent kind (continue vs revise); the
 * controller owns the meaning. One intent per wait: an exact digest repeat
 * is a no-op, a different digest is rejected. No intent body, evidence,
 * TASK/PLAN content, facts, paths, env values, profiles or credentials
 * ever enter the record.
 */
export interface PipelineV2WaitIntentRecord {
  readonly intent_sha256: string;
}

/**
 * Content-free record of one closed stage iteration (schema v7), inside
 * its generation's append-only `iterations[]`. The `by` reason is the
 * closed close vocabulary; `wait_index` is present exactly for the
 * wait-bound closures (`grant`, `replanned`) and names the open wait the
 * intervention ran in; `closed_transition_count` is the committed
 * transition count at the moment the reducer closed the iteration (the
 * anchor the loader's positional replay orders the closure by).
 */
export interface PipelineV2StageIterationRecord {
  readonly index: number;
  readonly opened_transition_count: number;
  readonly closed?: {
    readonly by: PipelineV2StageIterationCloseReason;
    readonly wait_index?: number;
    readonly closed_transition_count: number;
  };
}

/**
 * Content-free durable record of one stage generation (schema v7): the
 * binding of one plan stage (`stage_id`, its 1-based semantic
 * `stage_position`) to one compiled stage template (`template_id`) under
 * one accepted plan revision (`plan_sha256`), with the immutable
 * `initial_budget` and the transition anchor of the opening. The record is
 * append-only: `iterations[]` grows, `iteration_count` mirrors
 * `iterations.length`, and the open/closed projections (`open_iteration`,
 * `closed`) describe only the current lifecycle state; closed
 * generations and their historical iterations are never rewritten. The
 * effective iteration budget is derived (`initial_budget` plus the
 * generation's recorded grants) and never stored on the record.
 */
export interface PipelineV2StageGenerationRecord {
  readonly index: number;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly plan_sha256: string;
  readonly initial_budget: number;
  readonly opened_transition_count: number;
  readonly iteration_count: number;
  readonly open_iteration?: {
    readonly index: number;
    readonly opened_transition_count: number;
  };
  readonly closed?: {
    readonly by: PipelineV2StageGenerationCloseReason;
    readonly closed_transition_count: number;
  };
  readonly iterations: readonly PipelineV2StageIterationRecord[];
}

/**
 * Content-free durable record of one accepted task revision (schema v7).
 * Revision 1 tasks are accepted by separate commands during the planning
 * flow (no wait link); revisions above 1 are accepted in the revise flow
 * and carry the `wait_index`/`intent_sha256` links of the open wait whose
 * revise intent delivered them. The per-task revision chain
 * (`revision = previous + 1`, `previous_sha256` = the previous record's
 * digest) is enforced by reducer and loader; the task body itself lives
 * only in the filesystem manifest.
 */
export interface PipelineV2TaskRevisionState {
  readonly index: number;
  readonly task_id: string;
  readonly revision: number;
  readonly sha256: string;
  readonly previous_sha256: string | null;
  readonly wait_index?: number;
  readonly intent_sha256?: string;
}

/**
 * Content-free durable record of one accepted plan revision (schema v7).
 * The record carries only the revision chain and the digest links plus the
 * `origin_execution` — the index of the settled-but-unbound planning
 * execution the plan was produced by; the plan body (stages, tasks,
 * templates) lives only in the filesystem manifest. An accepted plan
 * revision is never rewritten.
 */
export interface PipelineV2PlanRevisionState {
  readonly index: number;
  readonly revision: number;
  readonly sha256: string;
  readonly previous_sha256: string | null;
  readonly origin_execution: number;
}

/**
 * Content-free durable record of one iteration grant (schema v7),
 * append-only. The grant references the exact open generation, the open
 * wait and the accepted intent of that wait; `additional_iterations`
 * extends the generation's effective budget from the moment of the grant
 * onward. The generation record is never rewritten; historical
 * generation/iteration records stay untouched.
 */
export interface PipelineV2IterationGrantState {
  readonly index: number;
  readonly generation_index: number;
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly additional_iterations: number;
}

/**
 * Content-free record of one evaluated decision state: the shared evaluator's
 * `PipelineDecisionStateResult` without its duplicate `state_id` (the
 * execution record already names the state). Fact values, JSON bodies and
 * arbitrary diagnostics are structurally impossible.
 */
export type PipelineDecisionStateRecord =
  | {
      status: "selected";
      outcome: string;
      decision: string;
      rule_id: string;
      active_constraint_ids: string[];
    }
  | { status: "uncovered"; outcome: "uncovered"; active_constraint_ids: string[] }
  | {
      status: "inconsistent_facts";
      outcome: "inconsistent_facts";
      violated_relation_ids: string[];
    }
  | {
      status: "invalid_facts";
      outcome: "invalid_facts";
      reason: DecisionFactValidationReason;
      fact_id?: string;
      actual_type?: string;
    };

export interface PipelineV2RunState {
  schema_version: 7;
  revision: number;
  run_id: string;
  status: PipelineV2RunStatus;
  phase: PipelineV2RunPhase;
  started_at: string;
  updated_at: string;
  pipeline: PipelineV2RunPipelineIdentity;
  inputs: PipelineV2RunInputState[];
  cursor: PipelineV2RunCursorState;
  executions: PipelineV2ExecutionState[];
  transitions: PipelineV2CommittedTransitionState[];
  /**
   * The append-only stage-generation journal (schema v7). At most the last
   * generation may be open; an open iteration (if any) belongs to it.
   */
  generations: PipelineV2StageGenerationRecord[];
  /** The append-only accepted task revision ledger (schema v7). */
  task_revisions: PipelineV2TaskRevisionState[];
  /** The append-only accepted plan revision ledger (schema v7). */
  plan_revisions: PipelineV2PlanRevisionState[];
  /** The append-only iteration grant ledger (schema v7). */
  grants: PipelineV2IterationGrantState[];
  terminal?: PipelineV2TerminalState;
  run_outputs?: PipelineV2RunOutputState[];
  failure?: PipelineV2FailureState;
  /**
   * The single authoritative wait/response journal, in chronological
   * order. Empty for a run that never waited; an open last record marks
   * the run as waiting.
   */
  waits: PipelineV2WaitRecord[];
}

export type PipelineV2RunCommand =
  | {
      kind: "create_run";
      runId: string;
      pipeline: PipelineV2RunPipelineIdentity;
      inputs: readonly PipelineV2RunInputState[];
    }
  | {
      kind: "start_agent_execution";
      stateId: string;
      profile: string;
      /** The exact compiled execution role; mandatory, no default. */
      executionRole: PipelineV2ExecutionRole;
      /** The open iteration index; mandatory exactly for the stage role. */
      iterationIndex?: number;
    }
  | { kind: "agent_data_prepared" }
  | { kind: "agent_execution_session_created"; sessionId: string }
  | { kind: "agent_tool_session_created"; sessionId: string }
  | { kind: "agent_running" }
  | { kind: "agent_outputs_accepted"; outputs: readonly PipelineV2AgentOutputState[] }
  | { kind: "agent_cleanup_completed" }
  | {
      kind: "agent_failed";
      reason: PipelineV2FailureReason;
      sessionCleanup: PipelineV2SessionCleanupPair;
    }
  | {
      kind: "start_decision_execution";
      stateId: string;
      inputDigest: string;
      /** The exact compiled execution role; mandatory, no default. */
      executionRole: PipelineV2ExecutionRole;
      /** The open iteration index; mandatory exactly for the stage role. */
      iterationIndex?: number;
    }
  | { kind: "decision_evaluated"; result: PipelineDecisionStateRecord }
  | { kind: "decision_failed"; reason: PipelineV2FailureReason }
  | { kind: "transition_committed"; step: TransitionStep; executionIndex: number }
  | {
      kind: "run_waiting";
      stateId: string;
      reason: string;
      /** SHA-256 of the future orchestrator-owned user request manifest. */
      requestSha256: string;
      actions: readonly { id: string; to: string }[];
    }
  | {
      kind: "wait_response_recorded";
      /** The wait record the response closes; must be the open last record. */
      waitIndex: number;
      /** The request manifest digest of the open wait record. */
      expectedRequestSha256: string;
      /** A declared action id of the open wait record. */
      actionId: string;
      /** SHA-256 of the future orchestrator-owned response manifest. */
      responseSha256: string;
    }
  | { kind: "terminal_reached"; terminalStateId: string; terminalResult: "success" | "failed" }
  | { kind: "run_outputs_published"; outputs: readonly PipelineV2RunOutputState[] }
  | { kind: "run_succeeded" }
  | { kind: "run_failed"; reason: PipelineV2FailureReason }
  | { kind: "run_cleanup_failed" }
  | {
      kind: "stage_generation_opened";
      stageId: string;
      stagePosition: number;
      templateId: string;
      planSha256: string;
      initialBudget: number;
      transitionCount: number;
    }
  | {
      kind: "stage_iteration_opened";
      generationIndex: number;
      iterationIndex: number;
      transitionCount: number;
    }
  | {
      kind: "stage_iteration_closed";
      generationIndex: number;
      iterationIndex: number;
      by: PipelineV2StageIterationCloseReason;
      /** The open wait; mandatory exactly for the wait-bound closures. */
      waitIndex?: number;
    }
  | {
      kind: "stage_generation_closed";
      generationIndex: number;
      by: PipelineV2StageGenerationCloseReason;
    }
  | {
      kind: "plan_intent_accepted";
      waitIndex: number;
      intentSha256: string;
    }
  | {
      kind: "task_revision_accepted";
      taskId: string;
      revision: number;
      taskSha256: string;
      /** The open wait; mandatory exactly for user-response revisions. */
      waitIndex?: number;
      intentSha256?: string;
    }
  | {
      kind: "plan_revision_accepted";
      planRevision: number;
      planSha256: string;
      originExecution: number;
    }
  | {
      kind: "iteration_grant_recorded";
      generationIndex: number;
      waitIndex: number;
      intentSha256: string;
      additionalIterations: number;
    };

export class PipelineV2StateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2StateError";
  }
}

const ISO_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const JS_TYPE_NAMES: readonly string[] = [
  "string",
  "number",
  "boolean",
  "object",
  "bigint",
  "symbol",
  "function",
  "undefined",
];
const DECISION_FACT_VALIDATION_REASONS: readonly DecisionFactValidationReason[] = [
  "not_mapping",
  "unknown_fact",
  "missing_fact",
  "non_boolean_fact",
];

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

/**
 * Adds two non-negative safe integers and rejects an unrepresentable
 * result, so an effective budget can never silently wrap. The diagnostic
 * carries the `what` label only.
 */
function additionSafe(a: number, b: number, what: string): number {
  if (!isNonNegativeSafeInteger(a) || !isNonNegativeSafeInteger(b)) {
    throw new PipelineV2StateError(`${what} carries a non-integer budget component`);
  }
  const sum = a + b;
  if (!Number.isSafeInteger(sum)) {
    throw new PipelineV2StateError(`${what} has an unrepresentable effective budget sum`);
  }
  return sum;
}

function isIsoTimestamp(value: unknown): value is string {
  return typeof value === "string" && ISO_TIMESTAMP_PATTERN.test(value);
}

function isCanonicalAbsolutePath(value: unknown): value is string {
  if (!isNonEmptyString(value) || !value.startsWith("/") || value.length < 2) {
    return false;
  }
  return value
    .slice(1)
    .split("/")
    .every((segment) => segment !== "" && segment !== "." && segment !== ".." && segment !== "~");
}

function raise(message: string): never {
  throw new PipelineV2StateError(message);
}

function expectExactObject(
  value: unknown,
  what: string,
  keys: readonly string[],
  optionalKeys: readonly string[] = [],
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} is not a JSON object`);
  }
  const obj = value as Record<string, unknown>;
  const expected = new Set([...keys, ...optionalKeys]);
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      throw new PipelineV2StateError(`${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new PipelineV2StateError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
  return obj;
}

function expectEnum<T extends string>(value: unknown, allowed: readonly T[], what: string): T {
  if (typeof value !== "string" || !(allowed as readonly string[]).includes(value)) {
    throw new PipelineV2StateError(
      `${what} must be one of ${JSON.stringify(allowed)}, got ${JSON.stringify(value)}`,
    );
  }
  return value as T;
}

/**
 * The single safe-id contract of schema v3, shared by the reducer's
 * `create_run`, the durable store, the sink, and the v2 path helper.
 */
export function expectSafeId(value: unknown, what: string): string {
  if (!isPipelineV2SafeId(value)) {
    throw new PipelineV2StateError(`${what} must be a safe non-empty identifier, got ${JSON.stringify(value)}`);
  }
  return value;
}

function expectNonEmptyString(value: unknown, what: string): string {
  if (!isNonEmptyString(value)) {
    throw new PipelineV2StateError(`${what} must be a non-empty string, got ${JSON.stringify(value)}`);
  }
  return value;
}

function expectSha256(value: unknown, what: string): string {
  if (!isLowercaseSha256(value)) {
    throw new PipelineV2StateError(
      `${what} must be a lowercase hex SHA-256 digest, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectIsoTimestamp(value: unknown, what: string): string {
  if (!isIsoTimestamp(value)) {
    throw new PipelineV2StateError(
      `${what} must be an ISO-8601 UTC timestamp (YYYY-MM-DDTHH:MM:SS.sssZ), got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectSafePositiveInteger(value: unknown, what: string): number {
  if (!isPositiveSafeInteger(value)) {
    throw new PipelineV2StateError(
      `${what} must be a positive safe integer, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectSafeNonNegativeInteger(value: unknown, what: string): number {
  if (!isNonNegativeSafeInteger(value)) {
    throw new PipelineV2StateError(
      `${what} must be a non-negative safe integer, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectSafeIdList(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} must be an array`);
  }
  const seen = new Set<string>();
  return value.map((entry, index) => {
    const id = expectSafeId(entry, `${what}[${index}]`);
    if (seen.has(id)) {
      throw new PipelineV2StateError(`${what} declares id ${JSON.stringify(id)} more than once`);
    }
    seen.add(id);
    return id;
  });
}

function rejectLegacySchemaVersions(value: unknown): void {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return;
  }
  const version = (value as Record<string, unknown>).schema_version;
  if (version === 1) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 1, which is unsupported by this orchestrator (schema version 7 is the supported contract; no v1 migration exists)",
    );
  }
  if (version === 2) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 2, which is the production pipeline v1 run-state contract, not a pipeline v2 run state (schema version 7 is the supported contract; no v2 migration exists)",
    );
  }
  if (version === 3) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 3, which is unsupported by this orchestrator (schema version 7 is the supported contract; no v3 migration exists)",
    );
  }
  if (version === 4) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 4, which is unsupported by this orchestrator (schema version 7 is the supported contract; no v4 migration exists)",
    );
  }
  if (version === 5) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 5, which is unsupported by this orchestrator (schema version 7 is the supported contract; no v5 migration exists)",
    );
  }
  if (version === 6) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 6, which is unsupported by this orchestrator (schema version 7 is the supported contract; no v6 migration exists)",
    );
  }
  if (version !== undefined && version !== PIPELINE_V2_RUN_STATE_SCHEMA_VERSION) {
    throw new PipelineV2StateError(
      `pipeline v2 run state has schema_version ${JSON.stringify(version)}, expected ${PIPELINE_V2_RUN_STATE_SCHEMA_VERSION}`,
    );
  }
}

export function validatePipelineIdentityV2(value: unknown, what: string): PipelineV2RunPipelineIdentity {
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "bundle_root", "execution_snapshot_sha256", "entry_state", "max_transitions"],
  );
  if (obj.schema_version !== 2) {
    throw new PipelineV2StateError(
      `${what}.schema_version must be 2, got ${JSON.stringify(obj.schema_version)}`,
    );
  }
  return {
    schema_version: 2,
    bundle_root: isCanonicalAbsolutePath(obj.bundle_root)
      ? obj.bundle_root
      : raise(`${what}.bundle_root must be an absolute canonical directory path, got ${JSON.stringify(obj.bundle_root)}`),
    execution_snapshot_sha256: expectSha256(obj.execution_snapshot_sha256, `${what}.execution_snapshot_sha256`),
    entry_state: expectSafeId(obj.entry_state, `${what}.entry_state`),
    max_transitions: expectSafePositiveInteger(obj.max_transitions, `${what}.max_transitions`),
  };
}

export function validateRunInputState(value: unknown, what: string): PipelineV2RunInputState {
  const obj = expectExactObject(value, what, ["id", "type", "protected", "digest"]);
  if (typeof obj.protected !== "boolean") {
    throw new PipelineV2StateError(`${what}.protected must be a boolean, got ${JSON.stringify(obj.protected)}`);
  }
  return {
    id: expectSafeId(obj.id, `${what}.id`),
    type: expectEnum(obj.type, PIPELINE_V2_PORT_TYPES, `${what}.type`),
    protected: obj.protected,
    digest: expectSha256(obj.digest, `${what}.digest`),
  };
}

function validateCursorState(value: unknown, what: string): PipelineV2RunCursorState {
  const obj = expectExactObject(value, what, ["current_state", "transition_count"]);
  return {
    current_state: expectSafeId(obj.current_state, `${what}.current_state`),
    transition_count: expectSafeNonNegativeInteger(obj.transition_count, `${what}.transition_count`),
  };
}

function validateAgentOutputs(value: unknown, what: string): PipelineV2AgentOutputState[] {
  if (!Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} must be an array`);
  }
  const seen = new Set<string>();
  return value.map((entry, index) => {
    const obj = expectExactObject(entry, `${what}[${index}]`, ["id", "digest"]);
    const id = expectSafeId(obj.id, `${what}[${index}].id`);
    if (seen.has(id)) {
      throw new PipelineV2StateError(`${what} declares output id ${JSON.stringify(id)} more than once`);
    }
    seen.add(id);
    return { id, digest: expectSha256(obj.digest, `${what}[${index}].digest`) };
  });
}

function validateAgentExecution(value: unknown, what: string): PipelineV2AgentExecutionState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "type", "state_id", "attempt", "profile", "phase", "execution_role"],
    ["iteration_index", "execution_session_id", "tool_session_id", "session_cleanup", "outputs", "failure_reason"],
  );
  if (obj.type !== "agent") {
    throw new PipelineV2StateError(`${what}.type must be "agent", got ${JSON.stringify(obj.type)}`);
  }
  const phase = expectEnum(obj.phase, PIPELINE_V2_AGENT_EXECUTION_PHASES, `${what}.phase`);
  const execution: PipelineV2AgentExecutionState = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    type: "agent",
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    attempt: expectSafePositiveInteger(obj.attempt, `${what}.attempt`),
    profile: expectNonEmptyString(obj.profile, `${what}.profile`),
    execution_role: expectEnum(
      obj.execution_role,
      PIPELINE_V2_EXECUTION_ROLES,
      `${what}.execution_role`,
    ),
    phase,
  };
  if (execution.attempt !== 1) {
    throw new PipelineV2StateError(
      `execution ${execution.index} declares attempt ${execution.attempt}; only attempt 1 is supported`,
    );
  }
  if (obj.execution_session_id !== undefined) {
    execution.execution_session_id = expectNonEmptyString(
      obj.execution_session_id,
      `${what}.execution_session_id`,
    );
  }
  if (obj.tool_session_id !== undefined) {
    execution.tool_session_id = expectNonEmptyString(obj.tool_session_id, `${what}.tool_session_id`);
  }
  if (obj.session_cleanup !== undefined) {
    const pair = expectExactObject(
      obj.session_cleanup,
      `${what}.session_cleanup`,
      ["execution", "tool"],
    );
    execution.session_cleanup = {
      execution: expectEnum(
        pair.execution,
        PIPELINE_V2_SESSION_CLEANUP_VALUES,
        `${what}.session_cleanup.execution`,
      ),
      tool: expectEnum(pair.tool, PIPELINE_V2_SESSION_CLEANUP_VALUES, `${what}.session_cleanup.tool`),
    };
  }
  if (obj.outputs !== undefined) {
    if (!Array.isArray(obj.outputs)) {
      throw new PipelineV2StateError(`${what}.outputs must be an array`);
    }
    const seen = new Set<string>();
    execution.outputs = obj.outputs.map((entry, index) => {
      const output = expectExactObject(entry, `${what}.outputs[${index}]`, ["id", "digest"]);
      const id = expectSafeId(output.id, `${what}.outputs[${index}].id`);
      if (seen.has(id)) {
        throw new PipelineV2StateError(
          `${what}.outputs declares output id ${JSON.stringify(id)} more than once`,
        );
      }
      seen.add(id);
      return { id, digest: expectSha256(output.digest, `${what}.outputs[${index}].digest`) };
    });
  }
  if (obj.failure_reason !== undefined) {
    execution.failure_reason = expectEnum(
      obj.failure_reason,
      PIPELINE_V2_AGENT_EXECUTION_FAILURE_REASONS,
      `${what}.failure_reason`,
    );
  }

  // The role/iteration biconditional: a stage execution carries exactly
  // the open iteration index it runs in; planning and control executions
  // never carry one.
  if (execution.execution_role === "stage") {
    if (obj.iteration_index === undefined) {
      throw new PipelineV2StateError(
        `${what} has the "stage" execution role but records no iteration index`,
      );
    }
    execution.iteration_index = expectSafePositiveInteger(
      obj.iteration_index,
      `${what}.iteration_index`,
    );
  } else if (obj.iteration_index !== undefined) {
    throw new PipelineV2StateError(
      `${what} has the ${JSON.stringify(execution.execution_role)} execution role but records an iteration index`,
    );
  }

  // Phase coherence of the two durable session slots: neither session
  // exists before "execution_session_created"; the Tool session only
  // after the Execution session; both must exist from "sessions_created"
  // on (a successful execution requires both durable sessions).
  const hasExecutionSession = execution.execution_session_id !== undefined;
  const hasToolSession = execution.tool_session_id !== undefined;
  if ((phase === "started" || phase === "data_prepared") && (hasExecutionSession || hasToolSession)) {
    throw new PipelineV2StateError(
      `${what} has phase ${JSON.stringify(phase)} but already records a session`,
    );
  }
  if (phase === "execution_session_created" && (!hasExecutionSession || hasToolSession)) {
    if (!hasExecutionSession) {
      throw new PipelineV2StateError(
        `${what} has phase "execution_session_created" but records no execution session`,
      );
    }
    throw new PipelineV2StateError(
      `${what} has phase "execution_session_created" but already records a tool session`,
    );
  }
  if (
    (phase === "sessions_created" ||
      phase === "running" ||
      phase === "outputs_accepted" ||
      phase === "cleanup_completed") &&
    (!hasExecutionSession || !hasToolSession)
  ) {
    throw new PipelineV2StateError(
      `${what} has phase ${JSON.stringify(phase)} but does not record both durable sessions`,
    );
  }
  if (execution.session_cleanup !== undefined && phase !== "cleanup_completed" && phase !== "failed") {
    throw new PipelineV2StateError(
      `${what} has phase ${JSON.stringify(phase)} but already records a session cleanup outcome`,
    );
  }
  if (phase === "cleanup_completed") {
    if (execution.session_cleanup === undefined) {
      throw new PipelineV2StateError(`${what} finished cleanup but does not record the cleanup outcome`);
    }
    if (
      execution.session_cleanup.execution !== "completed" ||
      execution.session_cleanup.tool !== "completed"
    ) {
      throw new PipelineV2StateError(
        `${what} has phase "cleanup_completed" but records session cleanup ${JSON.stringify(execution.session_cleanup)}`,
      );
    }
  }
  if (phase === "failed" && execution.session_cleanup === undefined) {
    throw new PipelineV2StateError(`${what} failed but does not record its session cleanup outcome`);
  }
  // Per-slot biconditional: a durable session id exists -> the slot is
  // "completed" or "failed"; no durable id -> the slot is "not_required".
  if (execution.session_cleanup !== undefined) {
    const slots: readonly [string | undefined, PipelineV2SessionCleanup, string, string][] = [
      [execution.execution_session_id, execution.session_cleanup.execution, "execution", "an"],
      [execution.tool_session_id, execution.session_cleanup.tool, "tool", "a"],
    ];
    for (const [sessionId, outcome, label, article] of slots) {
      if (sessionId === undefined && outcome !== "not_required") {
        throw new PipelineV2StateError(
          `${what} records no ${label} session, so its cleanup outcome must be "not_required"`,
        );
      }
      if (sessionId !== undefined && outcome === "not_required") {
        throw new PipelineV2StateError(
          `${what} records ${article} ${label} session but marks its cleanup not_required`,
        );
      }
    }
  }
  if (
    execution.outputs !== undefined &&
    phase !== "outputs_accepted" &&
    phase !== "cleanup_completed" &&
    phase !== "failed"
  ) {
    throw new PipelineV2StateError(
      `${what} has phase ${JSON.stringify(phase)} but already records accepted outputs`,
    );
  }
  if (
    execution.outputs === undefined &&
    (phase === "outputs_accepted" || phase === "cleanup_completed")
  ) {
    throw new PipelineV2StateError(
      `${what} has phase ${JSON.stringify(phase)} but does not record accepted outputs`,
    );
  }
  if (execution.failure_reason !== undefined && phase !== "failed") {
    throw new PipelineV2StateError(
      `${what} records a failure reason but has phase ${JSON.stringify(phase)}`,
    );
  }
  if (phase === "failed" && execution.failure_reason === undefined) {
    throw new PipelineV2StateError(`${what} has phase "failed" but no failure reason`);
  }
  if (
    execution.failure_reason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON &&
    !hasFailedSessionCleanup(execution)
  ) {
    throw new PipelineV2StateError(
      `${what} records the session cleanup failure reason but no cleanup outcome failed`,
    );
  }
  if (
    hasFailedSessionCleanup(execution) &&
    execution.failure_reason !== PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON
  ) {
    throw new PipelineV2StateError(
      `${what} records a failed session cleanup but carries failure reason ${JSON.stringify(execution.failure_reason)}`,
    );
  }
  return execution;
}

function validateDecisionRecord(value: unknown, what: string): PipelineDecisionStateRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} is not a JSON object`);
  }
  const status = (value as Record<string, unknown>).status;
  if (status === "selected") {
    const obj = expectExactObject(value, what, [
      "status",
      "outcome",
      "decision",
      "rule_id",
      "active_constraint_ids",
    ]);
    const outcome = expectSafeId(obj.outcome, `${what}.outcome`);
    const decision = expectSafeId(obj.decision, `${what}.decision`);
    if (outcome !== decision) {
      throw new PipelineV2StateError(
        `${what} records the selected decision ${JSON.stringify(decision)}, but its outcome is ${JSON.stringify(outcome)}`,
      );
    }
    return {
      status: "selected",
      outcome,
      decision,
      rule_id: expectSafeId(obj.rule_id, `${what}.rule_id`),
      active_constraint_ids: expectSafeIdList(obj.active_constraint_ids, `${what}.active_constraint_ids`),
    };
  }
  if (status === "uncovered") {
    const obj = expectExactObject(value, what, ["status", "outcome", "active_constraint_ids"]);
    if (obj.outcome !== "uncovered") {
      throw new PipelineV2StateError(`${what}.outcome must be "uncovered", got ${JSON.stringify(obj.outcome)}`);
    }
    return {
      status: "uncovered",
      outcome: "uncovered",
      active_constraint_ids: expectSafeIdList(obj.active_constraint_ids, `${what}.active_constraint_ids`),
    };
  }
  if (status === "inconsistent_facts") {
    const obj = expectExactObject(value, what, ["status", "outcome", "violated_relation_ids"]);
    if (obj.outcome !== "inconsistent_facts") {
      throw new PipelineV2StateError(
        `${what}.outcome must be "inconsistent_facts", got ${JSON.stringify(obj.outcome)}`,
      );
    }
    return {
      status: "inconsistent_facts",
      outcome: "inconsistent_facts",
      violated_relation_ids: expectSafeIdList(obj.violated_relation_ids, `${what}.violated_relation_ids`),
    };
  }
  if (status === "invalid_facts") {
    const obj = expectExactObject(value, what, ["status", "outcome", "reason"], ["fact_id", "actual_type"]);
    if (obj.outcome !== "invalid_facts") {
      throw new PipelineV2StateError(`${what}.outcome must be "invalid_facts", got ${JSON.stringify(obj.outcome)}`);
    }
    const reason = expectEnum(obj.reason, DECISION_FACT_VALIDATION_REASONS, `${what}.reason`);
    const record: PipelineDecisionStateRecord = {
      status: "invalid_facts",
      outcome: "invalid_facts",
      reason,
    };
    if (obj.fact_id !== undefined) {
      record.fact_id = expectSafeId(obj.fact_id, `${what}.fact_id`);
    }
    if (obj.actual_type !== undefined) {
      const actualType = expectNonEmptyString(obj.actual_type, `${what}.actual_type`);
      if (!JS_TYPE_NAMES.includes(actualType)) {
        throw new PipelineV2StateError(
          `${what}.actual_type must be a JavaScript type name, got ${JSON.stringify(actualType)}`,
        );
      }
      record.actual_type = actualType;
    }
    if (reason === "not_mapping" || reason === "unknown_fact") {
      if (record.fact_id !== undefined || record.actual_type !== undefined) {
        throw new PipelineV2StateError(
          `${what} with reason ${JSON.stringify(reason)} must not carry fact_id or actual_type`,
        );
      }
    }
    if (reason === "missing_fact") {
      if (record.fact_id === undefined) {
        throw new PipelineV2StateError(`${what} with reason "missing_fact" must name the model-declared fact_id`);
      }
      if (record.actual_type !== undefined) {
        throw new PipelineV2StateError(`${what} with reason "missing_fact" must not carry actual_type`);
      }
    }
    if (reason === "non_boolean_fact") {
      if (record.fact_id === undefined || record.actual_type === undefined) {
        throw new PipelineV2StateError(
          `${what} with reason "non_boolean_fact" requires the fact_id and the value's actual_type`,
        );
      }
    }
    return record;
  }
  throw new PipelineV2StateError(
    `${what}.status must be one of ${JSON.stringify(["selected", "uncovered", "inconsistent_facts", "invalid_facts"])}, got ${JSON.stringify(status)}`,
  );
}

function validateDecisionExecution(value: unknown, what: string): PipelineV2DecisionExecutionState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "type", "state_id", "phase", "input_digest", "execution_role"],
    ["iteration_index", "result", "failure_reason"],
  );
  if (obj.type !== "decision") {
    throw new PipelineV2StateError(`${what}.type must be "decision", got ${JSON.stringify(obj.type)}`);
  }
  const phase = expectEnum(obj.phase, PIPELINE_V2_DECISION_EXECUTION_PHASES, `${what}.phase`);
  const execution: PipelineV2DecisionExecutionState = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    type: "decision",
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    execution_role: expectEnum(
      obj.execution_role,
      PIPELINE_V2_EXECUTION_ROLES,
      `${what}.execution_role`,
    ),
    phase,
    input_digest: expectSha256(obj.input_digest, `${what}.input_digest`),
  };
  if (execution.execution_role === "stage") {
    if (obj.iteration_index === undefined) {
      throw new PipelineV2StateError(
        `${what} has the "stage" execution role but records no iteration index`,
      );
    }
    execution.iteration_index = expectSafePositiveInteger(
      obj.iteration_index,
      `${what}.iteration_index`,
    );
  } else if (obj.iteration_index !== undefined) {
    throw new PipelineV2StateError(
      `${what} has the ${JSON.stringify(execution.execution_role)} execution role but records an iteration index`,
    );
  }
  if (obj.result !== undefined) {
    execution.result = validateDecisionRecord(obj.result, `${what}.result`);
  }
  if (obj.failure_reason !== undefined) {
    execution.failure_reason = expectEnum(
      obj.failure_reason,
      PIPELINE_V2_DECISION_EXECUTION_FAILURE_REASONS,
      `${what}.failure_reason`,
    );
  }
  if (phase === "evaluating" && execution.result !== undefined) {
    throw new PipelineV2StateError(`${what} is still evaluating but already records a decision result`);
  }
  if (phase === "evaluating" && execution.failure_reason !== undefined) {
    throw new PipelineV2StateError(`${what} is still evaluating but already records a failure reason`);
  }
  if (phase === "evaluated" && execution.result === undefined) {
    throw new PipelineV2StateError(`${what} has phase "evaluated" but no decision result`);
  }
  if (phase === "evaluated" && execution.failure_reason !== undefined) {
    throw new PipelineV2StateError(`${what} has phase "evaluated" but records a failure reason`);
  }
  if (phase === "failed" && execution.failure_reason === undefined) {
    throw new PipelineV2StateError(`${what} has phase "failed" but no failure reason`);
  }
  if (phase === "failed" && execution.result !== undefined) {
    throw new PipelineV2StateError(`${what} has phase "failed" but records a decision result`);
  }
  return execution;
}

function validateExecution(value: unknown, what: string): PipelineV2ExecutionState {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} is not a JSON object`);
  }
  const type = (value as Record<string, unknown>).type;
  if (type === "agent") {
    return validateAgentExecution(value, what);
  }
  if (type === "decision") {
    return validateDecisionExecution(value, what);
  }
  throw new PipelineV2StateError(`${what}.type must be "agent" or "decision", got ${JSON.stringify(type)}`);
}

function validateTransition(value: unknown, what: string): PipelineV2CommittedTransitionState {
  const obj = expectExactObject(value, what, ["index", "from", "outcome", "to", "execution_index"]);
  return {
    index: expectSafeNonNegativeInteger(obj.index, `${what}.index`),
    from: expectSafeId(obj.from, `${what}.from`),
    outcome: expectNonEmptyString(obj.outcome, `${what}.outcome`),
    to: expectSafeId(obj.to, `${what}.to`),
    execution_index: expectSafePositiveInteger(obj.execution_index, `${what}.execution_index`),
  };
}

function validateTerminal(value: unknown, what: string): PipelineV2TerminalState {
  const obj = expectExactObject(value, what, ["state_id", "result"]);
  if (obj.result !== "success" && obj.result !== "failed") {
    throw new PipelineV2StateError(`${what}.result must be "success" or "failed", got ${JSON.stringify(obj.result)}`);
  }
  return {
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    result: obj.result as PipelineV2TerminalState["result"],
  };
}

function validateRunOutput(value: unknown, what: string): PipelineV2RunOutputState {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2StateError(`${what} is not a JSON object`);
  }
  const present = (value as Record<string, unknown>).present;
  if (present === true) {
    const obj = expectExactObject(value, what, ["id", "type", "required", "present", "digest"]);
    return {
      id: expectSafeId(obj.id, `${what}.id`),
      type: expectEnum(obj.type, PIPELINE_V2_PORT_TYPES, `${what}.type`),
      required: typeof obj.required === "boolean"
        ? obj.required
        : raise(`${what}.required must be a boolean`),
      present: true,
      digest: expectSha256(obj.digest, `${what}.digest`),
    };
  }
  if (present === false) {
    const obj = expectExactObject(value, what, ["id", "type", "required", "present"]);
    if (obj.required !== false) {
      throw new PipelineV2StateError(
        `${what} is absent, so its required flag must be false, got ${JSON.stringify(obj.required)}`,
      );
    }
    return {
      id: expectSafeId(obj.id, `${what}.id`),
      type: expectEnum(obj.type, PIPELINE_V2_PORT_TYPES, `${what}.type`),
      required: false,
      present: false,
    };
  }
  throw new PipelineV2StateError(`${what}.present must be a boolean, got ${JSON.stringify(present)}`);
}

function validateFailureState(value: unknown, what: string): PipelineV2FailureState {
  const obj = expectExactObject(value, what, ["reason"]);
  return {
    reason: expectEnum(obj.reason, PIPELINE_V2_FAILURE_REASONS, `${what}.reason`),
  };
}

function validateWaitAction(value: unknown, what: string): PipelineV2WaitAction {
  const obj = expectExactObject(value, what, ["id", "to"]);
  return {
    id: expectSafeId(obj.id, `${what}.id`),
    to: expectSafeId(obj.to, `${what}.to`),
  };
}

/**
 * Exact-field validation of one wait record of the wait journal: a
 * positive journal index and the committed-transition count, safe ids for
 * the waiting state, the normalized reason and every action id/target, a
 * lowercase hex SHA-256 for the request manifest digest, a non-empty
 * action list with unique ids in preserved declaration order, and the
 * optional content-free response record (declared action id plus the
 * response manifest digest).
 */
function validateWaitResponse(value: unknown, what: string): PipelineV2WaitResponseRecord {
  const obj = expectExactObject(value, what, ["action_id", "response_sha256"]);
  return {
    action_id: expectSafeId(obj.action_id, `${what}.action_id`),
    response_sha256: expectSha256(obj.response_sha256, `${what}.response_sha256`),
  };
}

function validateWaitRecord(value: unknown, what: string): PipelineV2WaitRecord {
  const obj = expectExactObject(
    value,
    what,
    ["index", "transition_count", "state_id", "reason", "request_sha256", "actions"],
    ["intent", "response"],
  );
  if (!Array.isArray(obj.actions)) {
    throw new PipelineV2StateError(`${what}.actions must be an array`);
  }
  if (obj.actions.length === 0) {
    throw new PipelineV2StateError(`${what}.actions must not be empty`);
  }
  const seen = new Set<string>();
  const actions = obj.actions.map((entry, index) => {
    const action = validateWaitAction(entry, `${what}.actions[${index}]`);
    if (seen.has(action.id)) {
      throw new PipelineV2StateError(
        `${what} declares action id ${JSON.stringify(action.id)} more than once`,
      );
    }
    seen.add(action.id);
    return action;
  });
  const record: PipelineV2WaitRecord = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    transition_count: expectSafeNonNegativeInteger(obj.transition_count, `${what}.transition_count`),
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    reason: expectSafeId(obj.reason, `${what}.reason`),
    request_sha256: expectSha256(obj.request_sha256, `${what}.request_sha256`),
    actions,
  };
  const intent = obj.intent !== undefined ? validateWaitIntent(obj.intent, `${what}.intent`) : undefined;
  const response = obj.response !== undefined ? validateWaitResponse(obj.response, `${what}.response`) : undefined;
  if (intent !== undefined || response !== undefined) {
    return {
      ...record,
      ...(intent !== undefined ? { intent } : {}),
      ...(response !== undefined ? { response } : {}),
    };
  }
  return record;
}

function validateWaitIntent(value: unknown, what: string): PipelineV2WaitIntentRecord {
  const obj = expectExactObject(value, what, ["intent_sha256"]);
  return { intent_sha256: expectSha256(obj.intent_sha256, `${what}.intent_sha256`) };
}

/** Exact-field validation of one closed stage iteration record. */
function validateStageIteration(value: unknown, what: string): PipelineV2StageIterationRecord {
  const obj = expectExactObject(
    value,
    what,
    ["index", "opened_transition_count"],
    ["closed"],
  );
  const record: PipelineV2StageIterationRecord = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    opened_transition_count: expectSafeNonNegativeInteger(
      obj.opened_transition_count,
      `${what}.opened_transition_count`,
    ),
  };
  if (obj.closed !== undefined) {
    const closed = expectExactObject(
      obj.closed,
      `${what}.closed`,
      ["by", "closed_transition_count"],
      ["wait_index"],
    );
    const by = expectEnum(
      closed.by,
      PIPELINE_V2_STAGE_ITERATION_CLOSE_REASONS,
      `${what}.closed.by`,
    );
    const closedTransitionCount = expectSafeNonNegativeInteger(
      closed.closed_transition_count,
      `${what}.closed.closed_transition_count`,
    );
    const waitIndex = closed.wait_index === undefined
      ? undefined
      : expectSafePositiveInteger(closed.wait_index, `${what}.closed.wait_index`);
    const iteration: PipelineV2StageIterationRecord = {
      index: record.index,
      opened_transition_count: record.opened_transition_count,
      closed: {
        by,
        closed_transition_count: closedTransitionCount,
        ...(waitIndex !== undefined ? { wait_index: waitIndex } : {}),
      },
    };
    if (by === "grant" || by === "replanned") {
      if (iteration.closed?.wait_index === undefined) {
        throw new PipelineV2StateError(
          `${what} closed with ${JSON.stringify(by)} must name the wait it was closed in`,
        );
      }
    } else if (iteration.closed?.wait_index !== undefined) {
      throw new PipelineV2StateError(
        `${what} closed with ${JSON.stringify(by)} must not carry a wait index`,
      );
    }
    if (iteration.closed !== undefined && iteration.closed.closed_transition_count < record.opened_transition_count) {
      throw new PipelineV2StateError(
        `${what} records a closed transition count below its opened transition count`,
      );
    }
    return iteration;
  }
  return record;
}

/** Exact-field validation of one stage generation record. */
function validateStageGeneration(value: unknown, what: string): PipelineV2StageGenerationRecord {
  const obj = expectExactObject(
    value,
    what,
    [
      "index",
      "stage_id",
      "stage_position",
      "template_id",
      "plan_sha256",
      "initial_budget",
      "opened_transition_count",
      "iteration_count",
      "iterations",
    ],
    ["open_iteration", "closed"],
  );
  if (!Array.isArray(obj.iterations)) {
    throw new PipelineV2StateError(`${what}.iterations must be an array`);
  }
  const generation: PipelineV2StageGenerationRecord = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    stage_id: expectSafeId(obj.stage_id, `${what}.stage_id`),
    stage_position: expectSafePositiveInteger(obj.stage_position, `${what}.stage_position`),
    template_id: expectSafeId(obj.template_id, `${what}.template_id`),
    plan_sha256: expectSha256(obj.plan_sha256, `${what}.plan_sha256`),
    initial_budget: expectSafePositiveInteger(obj.initial_budget, `${what}.initial_budget`),
    opened_transition_count: expectSafeNonNegativeInteger(
      obj.opened_transition_count,
      `${what}.opened_transition_count`,
    ),
    iteration_count: expectSafeNonNegativeInteger(obj.iteration_count, `${what}.iteration_count`),
    iterations: obj.iterations.map((entry, index) =>
      validateStageIteration(entry, `${what}.iterations[${index}]`),
    ),
  };
  if (generation.iteration_count !== generation.iterations.length) {
    throw new PipelineV2StateError(
      `${what} declares iteration_count ${generation.iteration_count}, but records ${generation.iterations.length} iterations`,
    );
  }
  for (let position = 0; position < generation.iterations.length; position++) {
    const iteration = generation.iterations[position]!;
    if (iteration.index !== position + 1) {
      throw new PipelineV2StateError(
        `${what} iteration at position ${position} declares index ${iteration.index}; iteration indexes must be contiguous from 1`,
      );
    }
    if (iteration.opened_transition_count < generation.opened_transition_count) {
      throw new PipelineV2StateError(
        `${what} iteration ${iteration.index} opened before its generation`,
      );
    }
  }
  const lastIteration = generation.iterations[generation.iterations.length - 1];
  const lastIterationOpen = lastIteration !== undefined && lastIteration.closed === undefined;
  if (obj.open_iteration !== undefined) {
    const open = expectExactObject(
      obj.open_iteration,
      `${what}.open_iteration`,
      ["index", "opened_transition_count"],
    );
    if (!lastIterationOpen) {
      throw new PipelineV2StateError(
        `${what} records an open iteration while its last iteration is closed`,
      );
    }
    if (
      open.index !== lastIteration.index ||
      open.opened_transition_count !== lastIteration.opened_transition_count
    ) {
      throw new PipelineV2StateError(
        `${what}.open_iteration does not match its last open iteration record`,
      );
    }
    return {
      ...generation,
      open_iteration: { index: open.index, opened_transition_count: open.opened_transition_count },
    };
  }
  if (lastIterationOpen) {
    throw new PipelineV2StateError(
      `${what} records an open iteration without its open_iteration projection`,
    );
  }
  if (obj.closed !== undefined) {
    const closed = expectExactObject(
      obj.closed,
      `${what}.closed`,
      ["by", "closed_transition_count"],
    );
    const by = expectEnum(
      closed.by,
      PIPELINE_V2_STAGE_GENERATION_CLOSE_REASONS,
      `${what}.closed.by`,
    );
    if (lastIterationOpen) {
      throw new PipelineV2StateError(
        `${what} is closed while its last iteration is still open`,
      );
    }
    if (lastIteration === undefined) {
      throw new PipelineV2StateError(`${what} is closed without any recorded iteration`);
    }
    const closedAnchor = expectSafeNonNegativeInteger(
      closed.closed_transition_count,
      `${what}.closed.closed_transition_count`,
    );
    if (closedAnchor < generation.opened_transition_count) {
      throw new PipelineV2StateError(
        `${what} records a closed transition count below its opened transition count`,
      );
    }
    const lastClosedAnchor =
      lastIteration?.closed?.closed_transition_count ?? generation.opened_transition_count;
    if (lastIteration !== undefined && closedAnchor < lastClosedAnchor) {
      throw new PipelineV2StateError(
        `${what} closed before its last iteration`,
      );
    }
    return {
      ...generation,
      closed: { by, closed_transition_count: closedAnchor },
    };
  }
  return generation;
}

/** Exact-field validation of one accepted task revision ledger record. */
function validateTaskRevision(value: unknown, what: string): PipelineV2TaskRevisionState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "task_id", "revision", "sha256", "previous_sha256"],
    ["wait_index", "intent_sha256"],
  );
  const revision = expectSafePositiveInteger(obj.revision, `${what}.revision`);
  const previous = obj.previous_sha256 === null ? null : expectSha256(
    obj.previous_sha256,
    `${what}.previous_sha256`,
  );
  const record: PipelineV2TaskRevisionState = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    task_id: expectSafeId(obj.task_id, `${what}.task_id`),
    revision,
    sha256: expectSha256(obj.sha256, `${what}.sha256`),
    previous_sha256: previous,
  };
  if ((revision === 1) !== (previous === null)) {
    throw new PipelineV2StateError(
      `${what} declares revision ${revision} with previous_sha256 ${previous === null ? "null" : "set"}; revision 1 must carry a null previous digest and every later revision a digest`,
    );
  }
  if (revision === 1) {
    if (obj.wait_index !== undefined || obj.intent_sha256 !== undefined) {
      throw new PipelineV2StateError(
        `${what} accepts a revision-1 task revision outside the user wait; revision 1 carries no wait links`,
      );
    }
  } else {
    if (obj.wait_index === undefined || obj.intent_sha256 === undefined) {
      throw new PipelineV2StateError(
        `${what} accepts a user-response task revision without its wait and intent links`,
      );
    }
    return {
      ...record,
      wait_index: expectSafePositiveInteger(obj.wait_index, `${what}.wait_index`),
      intent_sha256: expectSha256(obj.intent_sha256, `${what}.intent_sha256`),
    };
  }
  return record;
}

/** Exact-field validation of one accepted plan revision ledger record. */
function validatePlanRevision(value: unknown, what: string): PipelineV2PlanRevisionState {
  const obj = expectExactObject(value, what, [
    "index",
    "revision",
    "sha256",
    "previous_sha256",
    "origin_execution",
  ]);
  const revision = expectSafePositiveInteger(obj.revision, `${what}.revision`);
  const previous = obj.previous_sha256 === null
    ? null
    : expectSha256(obj.previous_sha256, `${what}.previous_sha256`);
  if ((revision === 1) !== (previous === null)) {
    throw new PipelineV2StateError(
      `${what} declares revision ${revision} with previous_sha256 ${previous === null ? "null" : "set"}; revision 1 must carry a null previous digest and every later revision a digest`,
    );
  }
  return {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    revision,
    sha256: expectSha256(obj.sha256, `${what}.sha256`),
    previous_sha256: previous,
    origin_execution: expectSafePositiveInteger(obj.origin_execution, `${what}.origin_execution`),
  };
}

/** Exact-field validation of one iteration grant ledger record. */
function validateIterationGrant(value: unknown, what: string): PipelineV2IterationGrantState {
  const obj = expectExactObject(value, what, [
    "index",
    "generation_index",
    "wait_index",
    "intent_sha256",
    "additional_iterations",
  ]);
  return {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    generation_index: expectSafePositiveInteger(obj.generation_index, `${what}.generation_index`),
    wait_index: expectSafePositiveInteger(obj.wait_index, `${what}.wait_index`),
    intent_sha256: expectSha256(obj.intent_sha256, `${what}.intent_sha256`),
    additional_iterations: expectSafePositiveInteger(
      obj.additional_iterations,
      `${what}.additional_iterations`,
    ),
  };
}

/**
 * Validates one pipeline v2 run state document and returns an independent,
 * deep-frozen snapshot. Every invariant the reducer enforces is re-checked
 * here against the authoritative records alone: there is no event journal
 * to reconcile against, so phantom executions, transitions, waits,
 * terminals and run outputs are impossible by construction.
 */
export function validatePipelineV2RunState(value: unknown): PipelineV2RunState {
  rejectLegacySchemaVersions(value);
  const obj = expectExactObject(
    value,
    "pipeline v2 run state",
    [
      "schema_version",
      "revision",
      "run_id",
      "status",
      "phase",
      "started_at",
      "updated_at",
      "pipeline",
      "inputs",
      "cursor",
      "executions",
      "transitions",
      "generations",
      "task_revisions",
      "plan_revisions",
      "grants",
      "waits",
    ],
    ["terminal", "run_outputs", "failure"],
  );
  if (obj.schema_version !== PIPELINE_V2_RUN_STATE_SCHEMA_VERSION) {
    throw new PipelineV2StateError(
      `pipeline v2 run state has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PIPELINE_V2_RUN_STATE_SCHEMA_VERSION}`,
    );
  }
  const revision = expectSafePositiveInteger(obj.revision, "pipeline v2 run state revision");
  const runId = expectSafeId(obj.run_id, "pipeline v2 run state run_id");
  const status = expectEnum(obj.status, PIPELINE_V2_RUN_STATUSES, "pipeline v2 run state status");
  const phase = expectEnum(obj.phase, PIPELINE_V2_RUN_PHASES, "pipeline v2 run state phase");
  const startedAt = expectIsoTimestamp(obj.started_at, "pipeline v2 run state started_at");
  const updatedAt = expectIsoTimestamp(obj.updated_at, "pipeline v2 run state updated_at");
  const pipeline = validatePipelineIdentityV2(obj.pipeline, "pipeline v2 run state pipeline");
  if (!Array.isArray(obj.inputs)) {
    throw new PipelineV2StateError("pipeline v2 run state inputs must be an array");
  }
  const inputs = obj.inputs.map((input, index) =>
    validateRunInputState(input, `pipeline v2 run state inputs[${index}]`),
  );
  {
    const ids = new Set<string>();
    for (const input of inputs) {
      if (ids.has(input.id)) {
        throw new PipelineV2StateError(
          `pipeline v2 run state inputs declares id ${JSON.stringify(input.id)} more than once`,
        );
      }
      ids.add(input.id);
    }
  }
  const cursor = validateCursorState(obj.cursor, "pipeline v2 run state cursor");
  if (!Array.isArray(obj.executions)) {
    throw new PipelineV2StateError("pipeline v2 run state executions must be an array");
  }
  const executions = obj.executions.map((execution, index) =>
    validateExecution(execution, `pipeline v2 run state executions[${index}]`),
  );
  if (!Array.isArray(obj.transitions)) {
    throw new PipelineV2StateError("pipeline v2 run state transitions must be an array");
  }
  const transitions = obj.transitions.map((transition, index) =>
    validateTransition(transition, `pipeline v2 run state transitions[${index}]`),
  );
  const terminal =
    obj.terminal === undefined ? undefined : validateTerminal(obj.terminal, "pipeline v2 run state terminal");
  const failure =
    obj.failure === undefined ? undefined : validateFailureState(obj.failure, "pipeline v2 run state failure");
  let runOutputs: PipelineV2RunOutputState[] | undefined;
  if (obj.run_outputs !== undefined) {
    if (!Array.isArray(obj.run_outputs)) {
      throw new PipelineV2StateError("pipeline v2 run state run_outputs must be an array");
    }
    const seen = new Set<string>();
    runOutputs = obj.run_outputs.map((output, index) => {
      const validated = validateRunOutput(output, `pipeline v2 run state run_outputs[${index}]`);
      if (seen.has(validated.id)) {
        throw new PipelineV2StateError(
          `pipeline v2 run state run_outputs declares id ${JSON.stringify(validated.id)} more than once`,
        );
      }
      seen.add(validated.id);
      return validated;
    });
  }
  if (!Array.isArray(obj.waits)) {
    throw new PipelineV2StateError("pipeline v2 run state waits must be an array");
  }
  const waits = obj.waits.map((wait, index) =>
    validateWaitRecord(wait, `pipeline v2 run state waits[${index}]`),
  );
  if (!Array.isArray(obj.generations)) {
    throw new PipelineV2StateError("pipeline v2 run state generations must be an array");
  }
  const generations = obj.generations.map((generation, index) =>
    validateStageGeneration(generation, `pipeline v2 run state generations[${index}]`),
  );
  for (let position = 0; position < generations.length; position++) {
    const generation = generations[position]!;
    if (generation.index !== position + 1) {
      throw new PipelineV2StateError(
        `generation at position ${position} declares index ${generation.index}; generation indexes must be contiguous from 1`,
      );
    }
  }
  if (!Array.isArray(obj.task_revisions)) {
    throw new PipelineV2StateError("pipeline v2 run state task_revisions must be an array");
  }
  const taskRevisions = obj.task_revisions.map((task, index) =>
    validateTaskRevision(task, `pipeline v2 run state task_revisions[${index}]`),
  );
  for (let position = 0; position < taskRevisions.length; position++) {
    const task = taskRevisions[position]!;
    if (task.index !== position + 1) {
      throw new PipelineV2StateError(
        `task revision at position ${position} declares index ${task.index}; task revision indexes must be contiguous from 1`,
      );
    }
  }
  if (!Array.isArray(obj.plan_revisions)) {
    throw new PipelineV2StateError("pipeline v2 run state plan_revisions must be an array");
  }
  const planRevisions = obj.plan_revisions.map((plan, index) =>
    validatePlanRevision(plan, `pipeline v2 run state plan_revisions[${index}]`),
  );
  if (!Array.isArray(obj.grants)) {
    throw new PipelineV2StateError("pipeline v2 run state grants must be an array");
  }
  const grants = obj.grants.map((grant, index) =>
    validateIterationGrant(grant, `pipeline v2 run state grants[${index}]`),
  );
  for (let position = 0; position < grants.length; position++) {
    const grant = grants[position]!;
    if (grant.index !== position + 1) {
      throw new PipelineV2StateError(
        `grant record at position ${position} declares index ${grant.index}; grant record indexes must be contiguous from 1`,
      );
    }
  }
  // The accepted plan revisions form one chain: revision and index are
  // contiguous from 1 and every record's previous digest is exactly the
  // previous ledger record's digest (the reducer derives it there; the
  // loader re-derives it from the records alone).
  for (let position = 0; position < planRevisions.length; position++) {
    const plan = planRevisions[position]!;
    if (plan.index !== position + 1 || plan.revision !== position + 1) {
      throw new PipelineV2StateError(
        `plan revision at position ${position} declares index ${plan.index} and revision ${plan.revision}; plan revisions must be contiguous from 1 and match their ledger position`,
      );
    }
    const expectedPrevious = position === 0 ? null : planRevisions[position - 1]!.sha256;
    if (plan.previous_sha256 !== expectedPrevious) {
      throw new PipelineV2StateError(
        `plan revision record ${plan.index} declares a previous digest that does not match the previous ledger record`,
      );
    }
  }
  // The accepted task revisions form one chain per task: the records of
  // one task appear in ledger order with revisions contiguous from 1 and
  // each record's previous digest naming exactly its predecessor.
  {
    const lastPerTask = new Map<string, PipelineV2TaskRevisionState>();
    for (let position = 0; position < taskRevisions.length; position++) {
      const task = taskRevisions[position]!;
      const last = lastPerTask.get(task.task_id);
      if (task.revision === 1) {
        if (last !== undefined) {
          throw new PipelineV2StateError(
            `task revision record ${task.index} accepts revision 1 of ${JSON.stringify(task.task_id)}, which is already recorded at revision ${last.revision}`,
          );
        }
      } else {
        if (last === undefined) {
          throw new PipelineV2StateError(
            `task revision record ${task.index} has no recorded predecessor for ${JSON.stringify(task.task_id)}`,
          );
        }
        if (last.revision !== task.revision - 1) {
          throw new PipelineV2StateError(
            `task revision record ${task.index} must follow revision ${last.revision + 1} of ${JSON.stringify(task.task_id)}`,
          );
        }
        if (task.previous_sha256 !== last.sha256) {
          throw new PipelineV2StateError(
            `task revision record ${task.index} declares a previous digest that does not match the previous record of ${JSON.stringify(task.task_id)}`,
          );
        }
      }
      lastPerTask.set(task.task_id, task);
    }
  }
  // One grant belongs to exactly one (generation, wait) pair: a repeated
  // grant for the same pair is rejected before the replay consumes the
  // ledger (the reducer rejects the repeat at write time).
  {
    const grantPairs = new Set<string>();
    for (const grant of grants) {
      const pair = `${grant.generation_index}:${grant.wait_index}`;
      if (grantPairs.has(pair)) {
        throw new PipelineV2StateError(
          `grant record ${grant.index} repeats the grant of generation ${grant.generation_index} for wait ${grant.wait_index}`,
        );
      }
      grantPairs.add(pair);
    }
  }
  // Wait record indexes are contiguous from 1: gaps and duplicates are
  // rejected before the joint replay consumes the journal.
  for (let position = 0; position < waits.length; position++) {
    const wait = waits[position]!;
    if (wait.index !== position + 1) {
      throw new PipelineV2StateError(
        `wait record at position ${position} declares index ${wait.index}; wait record indexes must be contiguous from 1`,
      );
    }
  }

  // The waiting status, the "waiting" phase and an open last wait record
  // are one indivisible triple: each implies the other two, and no other
  // status or phase may carry an open wait record.
  const lastWaitRecord = waits.length > 0 ? waits[waits.length - 1]! : undefined;
  const hasOpenWait = lastWaitRecord !== undefined && lastWaitRecord.response === undefined;
  if (status === "waiting") {
    if (phase !== "waiting") {
      throw new PipelineV2StateError('run status "waiting" requires phase "waiting"');
    }
    if (!hasOpenWait) {
      throw new PipelineV2StateError(
        'run status "waiting" requires an open wait record as the last wait record',
      );
    }
  } else if (phase === "waiting") {
    throw new PipelineV2StateError('run phase "waiting" requires run status "waiting"');
  }
  if (hasOpenWait && status !== "waiting") {
    throw new PipelineV2StateError('an open wait record requires run status "waiting"');
  }

  if (transitions.length > pipeline.max_transitions) {
    throw new PipelineV2StateError(
      `pipeline v2 run state records ${transitions.length} committed transitions, more than the pipeline transition budget ${pipeline.max_transitions}`,
    );
  }

  // execution indexes are contiguous and unique, only the last execution may
  // still be in flight, and every settled non-last execution must already
  // carry its committed transition
  const sessionIds = new Set<string>();
  for (let index = 0; index < executions.length; index++) {
    const execution = executions[index]!;
    if (execution.index !== index + 1) {
      throw new PipelineV2StateError(
        `execution at position ${index} declares index ${execution.index}; execution indexes must be contiguous from 1`,
      );
    }
    // Global session id uniqueness across both durable slots of every
    // execution: one id can never be reused, not even once as an
    // Execution and once as a Tool session.
    if (execution.type === "agent") {
      for (const sessionId of [execution.execution_session_id, execution.tool_session_id]) {
        if (sessionId === undefined) {
          continue;
        }
        if (sessionIds.has(sessionId)) {
          throw new PipelineV2StateError(
            `execution ${execution.index} reuses session ${JSON.stringify(sessionId)}; a session id belongs to exactly one durable session slot`,
          );
        }
        sessionIds.add(sessionId);
      }
    }
    if (index < executions.length - 1 && !isSettledExecution(execution)) {
      throw new PipelineV2StateError(
        `execution ${execution.index} has phase ${JSON.stringify(execution.phase)}; only the last execution may still be unfinished`,
      );
    }
  }

  // transitions bind executions in order; each referenced execution must be
  // the completed execution whose state matches the transition's origin.
  // The transition chain itself (entry state, response targets, cursor) is
  // re-derived by the joint replay below.
  for (let index = 0; index < transitions.length; index++) {
    const transition = transitions[index]!;
    if (transition.execution_index !== index + 1) {
      throw new PipelineV2StateError(
        `transition at position ${index} references execution ${transition.execution_index}; transitions must reference executions in order`,
      );
    }
    const execution = executions[index];
    if (execution === undefined) {
      throw new PipelineV2StateError(
        `transition at position ${index} references execution ${transition.execution_index} which does not exist`,
      );
    }
    if (transition.from !== execution.state_id) {
      throw new PipelineV2StateError(
        `transition at position ${index} starts at ${JSON.stringify(transition.from)}, but execution ${execution.index} ran state ${JSON.stringify(execution.state_id)}`,
      );
    }
    if (execution.type === "agent") {
      if (execution.phase !== "cleanup_completed") {
        throw new PipelineV2StateError(
          `transition at position ${index} references execution ${execution.index} whose phase ${JSON.stringify(execution.phase)} is not a cleaned agent execution`,
        );
      }
    } else {
      if (execution.phase !== "evaluated") {
        throw new PipelineV2StateError(
          `transition at position ${index} references execution ${execution.index} whose phase ${JSON.stringify(execution.phase)} is not an evaluated decision`,
        );
      }
      if (execution.result !== undefined && execution.result.outcome !== transition.outcome) {
        throw new PipelineV2StateError(
          `transition at position ${index} carries outcome ${JSON.stringify(transition.outcome)}, but its decision execution recorded outcome ${JSON.stringify(execution.result.outcome)}`,
        );
      }
    }
  }

  if (cursor.transition_count !== transitions.length) {
    throw new PipelineV2StateError(
      `cursor.transition_count ${cursor.transition_count} does not match ${transitions.length} committed transitions`,
    );
  }

  // Joint replay of the authoritative record streams: the committed graph
  // transitions, the wait journal, the stage generations and their
  // iterations, the iteration grants, the accepted task revisions, and the
  // accepted plan revisions. The replay cursor starts at the entry state;
  // every record is anchored at the committed transition count at which the
  // reducer durably recorded it (the record's own `transition_count` /
  // `opened_transition_count` / `closed_transition_count`, the wait's count,
  // the plan origin minus one), and the ordinal loop visits those boundaries
  // in order, applying transitions[ordinal] at the end of each non-final
  // boundary. Records that share one boundary form a partial order given by
  // the reducer's successor rules — a planning or control execution starts
  // before the generation and iteration opened at its own boundary (the
  // stage-boundary hook: settled unbound, then the generation, then the
  // iteration, then the transition), a stage iteration closes after the
  // last execution it contains and before its generation closes, a
  // generation closes before the next one opens, and a wait is entered
  // after the lifecycle records of its boundary and answered only after
  // the durable intervention closed an open iteration. The replay resolves
  // that intra-boundary order with one worklist: every round applies the
  // anchored records whose preconditions hold — the plan revisions (whose
  // acceptance position is derived from their settled-but-unbound planning
  // origin), the active-bound iteration closures, the generation closures,
  // the generation openings, the iteration openings, the wait entries, the
  // grants and wait-bound iteration closures bound to the entered wait by
  // its exact wait index, and the responses — repeating until nothing
  // applies. Every round terminates: a record whose preconditions do not
  // hold simply does not apply in that round, so no malformed state can
  // loop the worklist. A record that never becomes applicable fails closed
  // with its specific diagnostic (a pending record at a visited boundary
  // fails that boundary's pending pass; an answered wait's response and
  // every intervention record of the wait resolve inside the wait's own
  // boundary — the transition count cannot move while the run is waiting —
  // and a response that is still unapplicable at the end of that boundary
  // fails that boundary's pending pass, before its execution start and
  // graph transition); there is no second cursor, no second journal and no
  // event stream. The execution
  // starts are checked interval-wise after
  // the worklist settles: a stage execution must name an iteration that
  // was open at its start (opened at or before the start boundary, closed
  // at or after it — a same-boundary `normal_close`/`exhausted` closure
  // follows the execution it contains, while a wait-bound closure at the
  // same boundary precedes every start of that boundary), and a
  // planning/control execution must not start inside an
  // iteration that was unambiguously open before its boundary (an
  // iteration opened at exactly the start boundary opened after a
  // planning/control start — the stage-boundary hook — and an iteration
  // closed at exactly the start boundary closed before it). A response
  // never consumes graph budget, and the persisted cursor must equal the
  // replayed cursor after the full replay.
  let replayCursor = pipeline.entry_state;
  let waitPosition = 0;
  let planPosition = 0;
  const iterationPositions = generations.map(() => 0);
  /** The consumed iteration closes, keyed `${generation.index}:${iteration.index}`. */
  const closedCloses = new Set<string>();
  /** The generation indexes whose opening the replay consumed. */
  const openedGenerations = new Set<number>();
  /** The generation indexes whose closure the replay consumed. */
  const closedGenerations = new Set<number>();
  /** The grant ledger indexes whose intervention the replay consumed. */
  const processedGrants = new Set<number>();
  let openGenerationIndex: number | null = null;
  let openIterationGeneration: number | null = null;
  let openIterationIndex: number | null = null;
  /** The entered but not yet answered wait record (at most one), by journal index. */
  let enteredWaitIndex: number | null = null;

  /**
   * The execution starting at a boundary, when it is a planning or control
   * execution that has failed. In reducer time the generation and
   * iteration openings of a planning/control execution's own start
   * boundary follow that execution's clean settlement (the stage-boundary
   * hook order); a failed execution's only successor is the run failure
   * finalization, so no opening can follow it. Durable openings anchored
   * at that boundary are therefore unreachable in reducer time and are
   * rejected before they replay.
   */
  const failedPlanningControlStartAt = (boundary: number): PipelineV2ExecutionState | undefined => {
    const starting = executions[boundary];
    return starting !== undefined && starting.phase === "failed" && starting.execution_role !== "stage"
      ? starting
      : undefined;
  };

  const grantWaitOf = (grant: PipelineV2IterationGrantState): PipelineV2WaitRecord => {
    const wait = waits[grant.wait_index - 1];
    if (wait === undefined) {
      throw new PipelineV2StateError(
        `grant record ${grant.index} references wait ${grant.wait_index}, which does not exist`,
      );
    }
    return wait;
  };
  const taskWaitOf = (task: PipelineV2TaskRevisionState): PipelineV2WaitRecord => {
    const wait = task.wait_index === undefined ? undefined : waits[task.wait_index - 1];
    if (task.wait_index === undefined || wait === undefined) {
      throw new PipelineV2StateError(
        `task revision record ${task.index} references wait ${JSON.stringify(task.wait_index)}, which does not exist`,
      );
    }
    return wait;
  };
  /**
   * The iterations unambiguously open at a boundary: opened strictly before
   * it and closed after it (or never). An iteration opened or closed at
   * exactly the checked boundary shares that boundary with the checked
   * event; the role-specific rules of the caller resolve which side of the
   * event it fell on.
   */
  const unambiguouslyOpenAt = (
    boundary: number,
  ): { generation: PipelineV2StageGenerationRecord; iteration: PipelineV2StageIterationRecord }[] => {
    const open: { generation: PipelineV2StageGenerationRecord; iteration: PipelineV2StageIterationRecord }[] = [];
    for (const generation of generations) {
      for (const iteration of generation.iterations) {
        if (
          iteration.opened_transition_count < boundary &&
          (iteration.closed === undefined || iteration.closed.closed_transition_count > boundary)
        ) {
          open.push({ generation, iteration });
        }
      }
    }
    return open;
  };

  // The records that anchor into the wait journal by wait index are checked
  // before the replay: a grant or a user-response task revision naming a
  // wait outside the journal can never become applicable at any boundary.
  for (const grant of grants) {
    grantWaitOf(grant);
  }
  for (const task of taskRevisions) {
    if (task.revision !== 1) {
      taskWaitOf(task);
    }
  }
  // A wait-bound iteration closure is recorded while the run is waiting,
  // and the transition count cannot move while waiting (the transition
  // commit requires phase "running"), so the closure's anchor must equal
  // the transition count of the wait it was closed in. A closure anchored
  // elsewhere is unreachable in reducer time and is rejected before the
  // replay, deterministically, like the other wait-journal references.
  for (const generation of generations) {
    for (const iteration of generation.iterations) {
      const closed = iteration.closed;
      if (closed === undefined || (closed.by !== "grant" && closed.by !== "replanned")) {
        continue;
      }
      if (closed.wait_index === undefined) {
        // the exact-field validation above already rejects a wait-bound
        // closure without its wait index; the guard keeps the type honest
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} closes with ${JSON.stringify(closed.by)} without a wait index`,
        );
      }
      const wait = waits[closed.wait_index - 1];
      if (wait === undefined) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} closes with ${JSON.stringify(closed.by)} for wait ${closed.wait_index}, which does not exist`,
        );
      }
      if (closed.closed_transition_count !== wait.transition_count) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} closes at committed transition count ${closed.closed_transition_count}, but its wait record ${wait.index} anchors at ${wait.transition_count}`,
        );
      }
    }
  }

  for (let ordinal = 0; ordinal <= transitions.length; ordinal++) {
    // ---- the intra-boundary worklist ----
    for (;;) {
      let progress = false;

      // (1) The plan revisions accepted at this boundary: the acceptance
      // position is derived from the settled-but-unbound planning execution
      // the plan names — the origin execution starts at committed transition
      // count origin - 1 and must be exactly the last execution started
      // there, settled, and carried the planning role. An iteration is open
      // at the acceptance only when it opened strictly before this boundary
      // and closes after it; an iteration opened or closed at exactly this
      // boundary shares the reducer's one-boundary order with the
      // acceptance (the plan precedes the iteration opening) and cannot
      // reject it.
      while (
        planPosition < planRevisions.length &&
        planRevisions[planPosition]!.origin_execution - 1 === ordinal
      ) {
        const plan = planRevisions[planPosition]!;
        if (plan.origin_execution !== ordinal + 1) {
          throw new PipelineV2StateError(
            `plan revision record ${plan.index} names origin execution ${plan.origin_execution}, which is not the last execution started at committed transition count ${ordinal}`,
          );
        }
        const origin = executions[plan.origin_execution - 1];
        if (origin === undefined) {
          throw new PipelineV2StateError(
            `plan revision record ${plan.index} names origin execution ${plan.origin_execution}, which does not exist`,
          );
        }
        if (origin.execution_role !== "planning") {
          throw new PipelineV2StateError(
            `plan revision record ${plan.index} names origin execution ${plan.origin_execution} with the ${JSON.stringify(origin.execution_role)} role; plan acceptance requires a planning execution`,
          );
        }
        if (origin.type !== "agent" || origin.phase !== "cleanup_completed") {
          throw new PipelineV2StateError(
            `plan revision record ${plan.index} names origin execution ${plan.origin_execution} with phase ${JSON.stringify(origin.phase)}; plan acceptance requires the planning execution to be cleanly settled ("cleanup_completed")`,
          );
        }
        const openAtAcceptance = unambiguouslyOpenAt(ordinal);
        if (openAtAcceptance.length > 0) {
          const open = openAtAcceptance[0]!;
          throw new PipelineV2StateError(
            `plan revision record ${plan.index} is accepted while iteration ${open.iteration.index} of generation ${open.generation.index} is open`,
          );
        }
        planPosition += 1;
        progress = true;
      }
      // (2) The active-bound iteration closures (normal_close / exhausted)
      // anchored at this boundary: the closure follows the last execution
      // the iteration contains and precedes the generation closure. This
      // block also runs on the final boundary (ordinal ===
      // transitions.length): a run may end its journal with a just-closed
      // iteration, an open iteration, a just-opened generation, or a
      // settled-but-unbound planning execution whose plan revision was
      // accepted — every one of those records must still be
      // replay-verified. Only the transition application below is skipped
      // on the final boundary.
      for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
        const generation = generations[generationIndex]!;
        if (openIterationGeneration !== generation.index) {
          continue;
        }
        const iteration = generation.iterations[openIterationIndex! - 1]!;
        const closed = iteration.closed;
        if (
          closed === undefined ||
          !(closed.by === "normal_close" || closed.by === "exhausted") ||
          closed.closed_transition_count !== ordinal
        ) {
          continue;
        }
        closedCloses.add(`${generation.index}:${iteration.index}`);
        openIterationGeneration = null;
        openIterationIndex = null;
        progress = true;
      }
      // (3) The generation closures anchored at this boundary.
      for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
        const generation = generations[generationIndex]!;
        if (generation.closed === undefined || generation.closed.closed_transition_count !== ordinal) {
          continue;
        }
        if (openGenerationIndex !== generation.index) {
          continue;
        }
        if (openIterationGeneration !== null) {
          continue;
        }
        if (iterationPositions[generationIndex] !== generation.iterations.length) {
          continue;
        }
        closedGenerations.add(generation.index);
        openGenerationIndex = null;
        progress = true;
      }
      // (4) The generation openings anchored at this boundary, bound to the
      // last plan revision accepted at or before this boundary (the plans
      // of this very boundary ran first in the same worklist).
      for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
        const generation = generations[generationIndex]!;
        if (generation.opened_transition_count !== ordinal || openedGenerations.has(generation.index)) {
          continue;
        }
        if (openGenerationIndex !== null || enteredWaitIndex !== null) {
          continue;
        }
        const acceptedPlan = planPosition > 0 ? planRevisions[planPosition - 1] : undefined;
        if (acceptedPlan === undefined || acceptedPlan.sha256 !== generation.plan_sha256) {
          continue;
        }
        const failedStart = failedPlanningControlStartAt(ordinal);
        if (failedStart !== undefined) {
          throw new PipelineV2StateError(
            `generation ${generation.index} opens at committed transition count ${ordinal}, where execution ${failedStart.index} has failed with the ${JSON.stringify(failedStart.execution_role)} role; no stage generation may open after a failed planning or control execution`,
          );
        }
        openedGenerations.add(generation.index);
        openGenerationIndex = generation.index;
        progress = true;
      }
      // (5) The iteration openings anchored at this boundary, with the
      // effective-budget check: the immutable initial budget plus the
      // generation's grants whose wait boundary is at or before this
      // boundary. The openings are processed before the wait entries of the
      // same boundary because a stage execution starts inside the iteration
      // the boundary opened.
      for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
        const generation = generations[generationIndex]!;
        const position = iterationPositions[generationIndex]!;
        if (position >= generation.iterations.length) {
          continue;
        }
        const iteration = generation.iterations[position]!;
        if (iteration.opened_transition_count !== ordinal) {
          continue;
        }
        if (openGenerationIndex !== generation.index || openIterationGeneration !== null || enteredWaitIndex !== null) {
          continue;
        }
        const failedStart = failedPlanningControlStartAt(ordinal);
        if (failedStart !== undefined) {
          throw new PipelineV2StateError(
            `iteration ${iteration.index} of generation ${generation.index} opens at committed transition count ${ordinal}, where execution ${failedStart.index} has failed with the ${JSON.stringify(failedStart.execution_role)} role; no stage iteration may open after a failed planning or control execution`,
          );
        }
        // The effective-budget safety checks mirror the reducer's opening
        // rule exactly: the accumulated grant sum and the initial-budget
        // sum must both stay representable, so no rounded value can reach
        // the comparison below.
        let grantsSum = 0;
        for (const grant of grants) {
          if (grant.generation_index !== generation.index || !processedGrants.has(grant.index)) {
            continue;
          }
          grantsSum += grant.additional_iterations;
          if (!Number.isSafeInteger(grantsSum)) {
            throw new PipelineV2StateError(
              `the effective iteration budget of generation ${generation.index} is unrepresentable`,
            );
          }
        }
        const effectiveBudget = generation.initial_budget + grantsSum;
        if (!Number.isSafeInteger(effectiveBudget)) {
          throw new PipelineV2StateError(
            `the effective iteration budget of generation ${generation.index} is unrepresentable`,
          );
        }
        if (iteration.index > effectiveBudget) {
          throw new PipelineV2StateError(
            `iteration ${iteration.index} of generation ${generation.index} exceeds the effective iteration budget ${effectiveBudget} (initial budget ${generation.initial_budget} plus recorded grants)`,
          );
        }
        openIterationGeneration = generation.index;
        openIterationIndex = iteration.index;
        iterationPositions[generationIndex] = position + 1;
        progress = true;
      }
      // (6) The wait entries: the next wait in journal order, entered when
      // its boundary and the replay cursor match and no other wait is open.
      if (waitPosition < waits.length) {
        const wait = waits[waitPosition]!;
        if (wait.transition_count < ordinal) {
          throw new PipelineV2StateError(
            `wait record ${wait.index} declares transition_count ${wait.transition_count}, below the replay boundary ${ordinal} already consumed by earlier wait records; wait records must be ordered by transition_count`,
          );
        }
        if (wait.transition_count === ordinal && wait.state_id === replayCursor && enteredWaitIndex === null) {
          if (wait.response === undefined) {
            if (waitPosition !== waits.length - 1) {
              throw new PipelineV2StateError(
                `wait record ${wait.index} is open but is not the last wait record; an open wait record may only end the wait journal`,
              );
            }
            if (ordinal !== transitions.length) {
              throw new PipelineV2StateError(
                `wait record ${wait.index} is open but ${transitions.length - ordinal} committed transitions follow it; no transition may follow an open wait record`,
              );
            }
            if (executions.length !== transitions.length) {
              throw new PipelineV2StateError(
                `wait record ${wait.index} is open while execution ${transitions.length + 1} has no committed transition; an open wait record requires a fully settled and committed history`,
              );
            }
          }
          // The user-response task revisions of this wait, in ledger order:
          // every revision-above-1 record names exactly this wait and
          // carries the intent the wait accepted.
          for (const task of taskRevisions) {
            if (task.revision === 1 || task.wait_index !== wait.index) {
              continue;
            }
            if (wait.intent === undefined || wait.intent.intent_sha256 !== task.intent_sha256) {
              throw new PipelineV2StateError(
                `task revision record ${task.index} references an intent that wait record ${wait.index} has not accepted`,
              );
            }
          }
          waitPosition += 1;
          enteredWaitIndex = wait.index;
          progress = true;
        }
      }
      // (7) The grants of the entered wait, bound by the exact wait index:
      // the grant references the generation that is open at this boundary
      // and the intent this wait accepted.
      if (enteredWaitIndex !== null) {
        const wait = waits[enteredWaitIndex - 1]!;
        for (const grant of grants) {
          if (grant.wait_index !== wait.index || processedGrants.has(grant.index)) {
            continue;
          }
          const generation = generations[grant.generation_index - 1];
          if (generation === undefined) {
            throw new PipelineV2StateError(
              `grant record ${grant.index} references generation ${grant.generation_index}, which does not exist`,
            );
          }
          if (openGenerationIndex !== grant.generation_index) {
            throw new PipelineV2StateError(
              `grant record ${grant.index} references generation ${grant.generation_index}, which is not open at committed transition count ${ordinal}`,
            );
          }
          if (wait.intent === undefined || wait.intent.intent_sha256 !== grant.intent_sha256) {
            throw new PipelineV2StateError(
              `grant record ${grant.index} references an intent that wait record ${wait.index} has not accepted`,
            );
          }
          processedGrants.add(grant.index);
          progress = true;
        }
      }
      // (8) The wait-bound iteration closures of the entered wait: the
      // closure binds to the currently open iteration, the wait's accepted
      // intent and its recorded intervention.
      if (enteredWaitIndex !== null) {
        const wait = waits[enteredWaitIndex - 1]!;
        for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
          const generation = generations[generationIndex]!;
          if (openIterationGeneration !== generation.index) {
            continue;
          }
          const iteration = generation.iterations[openIterationIndex! - 1]!;
          const closed = iteration.closed;
          if (
            closed === undefined ||
            !(closed.by === "grant" || closed.by === "replanned") ||
            closed.wait_index !== wait.index ||
            closed.closed_transition_count !== ordinal
          ) {
            continue;
          }
          if (wait.intent === undefined) {
            throw new PipelineV2StateError(
              `wait record ${wait.index} closes an iteration with ${JSON.stringify(closed.by)} but carries no accepted intent`,
            );
          }
          if (closed.by === "grant") {
            const grant = grants.find(
              (candidate) =>
                candidate.generation_index === generation.index &&
                candidate.wait_index === wait.index &&
                candidate.intent_sha256 === wait.intent!.intent_sha256,
            );
            if (grant === undefined) {
              throw new PipelineV2StateError(
                `iteration ${iteration.index} of generation ${generation.index} closed with ${JSON.stringify("grant")} without a recorded grant for wait ${wait.index}`,
              );
            }
          } else {
            const task = taskRevisions.some((candidate) => candidate.wait_index === wait.index);
            if (!task) {
              throw new PipelineV2StateError(
                `iteration ${iteration.index} of generation ${generation.index} closed with ${JSON.stringify("replanned")} without an accepted task revision for wait ${wait.index}`,
              );
            }
          }
          closedCloses.add(`${generation.index}:${iteration.index}`);
          openIterationGeneration = null;
          openIterationIndex = null;
          progress = true;
        }
      }
      // (9) The response of the entered wait: applied only after the
      // durable intervention records of the wait were consumed and no
      // iteration is open.
      if (enteredWaitIndex !== null) {
        const wait = waits[enteredWaitIndex - 1]!;
        if (wait.response !== undefined) {
          const grantsDone = grants.every(
            (grant) => grant.wait_index !== wait.index || processedGrants.has(grant.index),
          );
          const closuresDone = generations.every((generation) =>
            generation.iterations.every(
              (iteration) =>
                iteration.closed === undefined ||
                iteration.closed.wait_index !== wait.index ||
                iteration.closed.closed_transition_count !== ordinal ||
                closedCloses.has(`${generation.index}:${iteration.index}`),
            ),
          );
          // The response is not applicable this round: the round ends and
          // the worklist termination check decides whether another record
          // of this wait's boundary can still make progress. The response
          // must resolve inside its own boundary's fixpoint; the pending
          // pass below fails closed when it does not — an execution start
          // or a graph transition of this boundary is never reached with
          // the response unresolved.
          if (grantsDone && closuresDone && openIterationGeneration === null) {
            const action = wait.actions.find((candidate) => candidate.id === wait.response!.action_id);
            if (action === undefined) {
              throw new PipelineV2StateError(
                `wait record ${wait.index} records response action ${JSON.stringify(wait.response.action_id)}, which it does not declare`,
              );
            }
            replayCursor = action.to;
            enteredWaitIndex = null;
            progress = true;
          }
        }
      }

      if (!progress) {
        break;
      }
    }

    // The pending-record pass: every record anchored at this boundary must
    // have been consumed by the worklist. Scan in the records' own order
    // and fail closed on the first pending record with its specific
    // diagnostic.
    // An entered wait must fully resolve at its own boundary. The reducer
    // records the response and every intervention record of the wait while
    // the run is waiting, and the transition count cannot move while
    // waiting, so the response applies at the wait's own transition count
    // or the document is incoherent. This fires before the boundary's
    // execution-start check and before the boundary's graph transition —
    // neither may happen inside an unresolved answered wait.
    if (enteredWaitIndex !== null) {
      const wait = waits[enteredWaitIndex - 1]!;
      if (wait.transition_count === ordinal && wait.response !== undefined) {
        if (openIterationGeneration !== null) {
          const generation = generations.find((candidate) => candidate.index === openIterationGeneration)!;
          const iteration = generation.iterations[openIterationIndex! - 1]!;
          throw new PipelineV2StateError(
            `wait record ${wait.index} is answered while iteration ${iteration.index} of generation ${generation.index} is still open`,
          );
        }
        throw new PipelineV2StateError(
          `wait record ${wait.index} is answered but its response could not be applied at committed transition count ${ordinal}`,
        );
      }
    }
    if (waitPosition < waits.length && waits[waitPosition]!.transition_count === ordinal) {
      const wait = waits[waitPosition]!;
      if (wait.state_id !== replayCursor) {
        throw new PipelineV2StateError(
          `wait record ${wait.index} names state ${JSON.stringify(wait.state_id)}, which does not match the replay cursor ${JSON.stringify(replayCursor)}`,
        );
      }
      if (enteredWaitIndex !== null && wait.response !== undefined) {
        const open = unambiguouslyOpenAt(ordinal).concat(
          openIterationGeneration === null
            ? []
            : (() => {
                const generation = generations.find((candidate) => candidate.index === openIterationGeneration)!;
                const iteration = generation.iterations[openIterationIndex! - 1]!;
                return [{ generation, iteration }];
              })(),
        );
        const distinct = open.filter(
          (candidate, index) =>
            open.findIndex(
              (other) =>
                other.generation.index === candidate.generation.index &&
                other.iteration.index === candidate.iteration.index,
            ) === index,
        );
        if (distinct.length > 0) {
          const openIteration = distinct[0]!;
          throw new PipelineV2StateError(
            `wait record ${wait.index} is answered while iteration ${openIteration.iteration.index} of generation ${openIteration.generation.index} is still open`,
          );
        }
      }
    }
    for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
      const generation = generations[generationIndex]!;
      if (generation.opened_transition_count !== ordinal || openedGenerations.has(generation.index)) {
        continue;
      }
      if (openGenerationIndex !== null) {
        throw new PipelineV2StateError(
          `generation ${generation.index} opens while generation ${openGenerationIndex} is still open; at most one generation may be open`,
        );
      }
      if (enteredWaitIndex !== null) {
        throw new PipelineV2StateError(
          `generation ${generation.index} opens on the boundary of the open wait record ${enteredWaitIndex}; no stage generation may open while the run is waiting`,
        );
      }
      const acceptedPlan = planPosition > 0 ? planRevisions[planPosition - 1] : undefined;
      if (acceptedPlan === undefined || acceptedPlan.sha256 !== generation.plan_sha256) {
        throw new PipelineV2StateError(
          `generation ${generation.index} binds plan digest ${JSON.stringify(generation.plan_sha256)}, which is not the last accepted plan revision at committed transition count ${ordinal}`,
        );
      }
    }
    for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
      const generation = generations[generationIndex]!;
      const position = iterationPositions[generationIndex]!;
      if (position >= generation.iterations.length) {
        continue;
      }
      const iteration = generation.iterations[position]!;
      if (iteration.opened_transition_count !== ordinal) {
        continue;
      }
      if (openGenerationIndex !== generation.index) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} opens while that generation is not the open generation`,
        );
      }
      if (openIterationGeneration !== null) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} opens while the iteration of generation ${openIterationGeneration} is still open; at most one iteration may be open`,
        );
      }
      if (enteredWaitIndex !== null) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} opens on the boundary of the open wait record ${enteredWaitIndex}; no stage iteration may open while the run is waiting`,
        );
      }
    }
    for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
      const generation = generations[generationIndex]!;
      if (openIterationGeneration !== generation.index) {
        continue;
      }
      const iteration = generation.iterations[openIterationIndex! - 1]!;
      const closed = iteration.closed;
      if (
        closed !== undefined &&
        (closed.by === "normal_close" || closed.by === "exhausted") &&
        closed.closed_transition_count === ordinal &&
        !closedCloses.has(`${generation.index}:${iteration.index}`)
      ) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} records a close that never matched the open iteration at its anchor`,
        );
      }
    }
    for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
      const generation = generations[generationIndex]!;
      if (generation.closed === undefined || generation.closed.closed_transition_count !== ordinal) {
        continue;
      }
      if (closedGenerations.has(generation.index)) {
        continue;
      }
      if (openGenerationIndex !== generation.index) {
        throw new PipelineV2StateError(
          `generation ${generation.index} closes at committed transition count ${ordinal}, but it is not the open generation there`,
        );
      }
      if (openIterationGeneration !== null) {
        throw new PipelineV2StateError(
          `generation ${generation.index} closes while an iteration is still open`,
        );
      }
      if (iterationPositions[generationIndex] !== generation.iterations.length) {
        throw new PipelineV2StateError(
          `generation ${generation.index} closes at committed transition count ${ordinal} but not all of its iterations were opened`,
        );
      }
    }
    if (enteredWaitIndex !== null) {
      const wait = waits[enteredWaitIndex - 1]!;
      for (const grant of grants) {
        if (grant.wait_index !== wait.index || processedGrants.has(grant.index)) {
          continue;
        }
        const generation = generations[grant.generation_index - 1];
        if (generation === undefined) {
          throw new PipelineV2StateError(
            `grant record ${grant.index} references generation ${grant.generation_index}, which does not exist`,
          );
        }
        if (openGenerationIndex !== grant.generation_index) {
          throw new PipelineV2StateError(
            `grant record ${grant.index} references generation ${grant.generation_index}, which is not open at committed transition count ${ordinal}`,
          );
        }
        throw new PipelineV2StateError(
          `grant record ${grant.index} references an intent that wait record ${wait.index} has not accepted`,
        );
      }
      for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
        const generation = generations[generationIndex]!;
        if (openIterationGeneration !== generation.index) {
          continue;
        }
        const iteration = generation.iterations[openIterationIndex! - 1]!;
        const closed = iteration.closed;
        if (
          closed !== undefined &&
          (closed.by === "grant" || closed.by === "replanned") &&
          closed.wait_index === wait.index &&
          closed.closed_transition_count === ordinal &&
          !closedCloses.has(`${generation.index}:${iteration.index}`)
        ) {
          throw new PipelineV2StateError(
            `wait record ${wait.index} closes an iteration with ${JSON.stringify(closed.by)} but carries no accepted intent`,
          );
        }
      }
    }

    // The execution starts: the global execution index k starts at
    // committed transition count k - 1. A stage execution must reference
    // the iteration that was open at its start; a planning or control
    // execution must start outside any iteration that was unambiguously
    // open before its boundary.
    const startingExecution = executions[ordinal];
    if (startingExecution !== undefined) {
      if (startingExecution.execution_role === "stage") {
        // The referenced iteration was open at the start, by the same
        // unified interval the shared query applies (see
        // `iterationOpenForStart`): inclusive of a closure anchored at the
        // start boundary (`normal_close`/`exhausted` follow the execution
        // they contain), excluding a wait-bound closure at the same
        // boundary (the wait cycle precedes every start of the boundary).
        // The touching same-boundary combination (one iteration closed and
        // another opened at the boundary) admits both orders and is not
        // distinguishable by the durable anchors alone; a reference inside
        // the candidate set is accepted, exactly as the restore verifier
        // resolves it through the shared query.
        const referencedIndex = startingExecution.iteration_index!;
        const candidates: { generation: PipelineV2StageGenerationRecord; iteration: PipelineV2StageIterationRecord }[] = [];
        for (const generation of generations) {
          for (const iteration of generation.iterations) {
            if (iterationOpenForStart(iteration, ordinal)) {
              candidates.push({ generation, iteration });
            }
          }
        }
        if (candidates.length === 0) {
          throw new PipelineV2StateError(
            `execution ${startingExecution.index} has the "stage" role but no iteration is open at committed transition count ${ordinal}`,
          );
        }
        if (!candidates.some((candidate) => candidate.iteration.index === referencedIndex)) {
          const open = candidates[candidates.length - 1]!;
          throw new PipelineV2StateError(
            `execution ${startingExecution.index} references iteration ${referencedIndex}, which is not the open iteration ${open.iteration.index} at committed transition count ${ordinal}`,
          );
        }
        if (startingExecution.phase === "failed") {
          // A failed stage execution's only successor is the run failure
          // finalization, so no iteration closure can follow the failure
          // at the execution's start boundary: the referenced iteration
          // must still be genuinely open after the boundary worklist. An
          // interval candidate that is already closed at this boundary is
          // not enough, ambiguous candidates are never chosen, and the
          // replay's single open-iteration projection is the only proof
          // of membership.
          if (openIterationGeneration === null || openIterationIndex !== referencedIndex) {
            throw new PipelineV2StateError(
              `execution ${startingExecution.index} has failed, so its referenced iteration ${referencedIndex} must still be open at committed transition count ${ordinal}`,
            );
          }
        }
      } else {
        const unambiguous = unambiguouslyOpenAt(ordinal);
        if (unambiguous.length > 0) {
          const open = unambiguous[0]!;
          throw new PipelineV2StateError(
            `execution ${startingExecution.index} has the ${JSON.stringify(startingExecution.execution_role)} role but starts inside the open iteration of generation ${open.generation.index}`,
          );
        }
      }
    }
    if (ordinal === transitions.length) {
      break;
    }
    const transition = transitions[ordinal]!;
    if (transition.from !== replayCursor) {
      throw new PipelineV2StateError(
        `transition at position ${ordinal} starts at ${JSON.stringify(transition.from)}, expected the replay cursor ${JSON.stringify(replayCursor)}`,
      );
    }
    replayCursor = transition.to;
  }
  // An entered wait that is still unanswered here is the legal end state
  // (the run ends waiting); an answered wait was already forced to resolve
  // inside its own boundary's pending pass, so no post-loop answered-wait
  // check exists.
  if (waitPosition !== waits.length) {
    const wait = waits[waitPosition]!;
    throw new PipelineV2StateError(
      `wait record ${wait.index} declares transition_count ${wait.transition_count}, which exceeds the ${transitions.length} committed transitions`,
    );
  }
  // Every iteration grant was bound to its wait during the replay; a grant
  // was processed exactly when its wait was entered and the grant's
  // generation was open at that boundary. A grant that was never processed
  // can only name a wait outside the journal.
  for (const grant of grants) {
    if (processedGrants.has(grant.index)) {
      continue;
    }
    grantWaitOf(grant);
    throw new PipelineV2StateError(
      `grant record ${grant.index} was not applied at the boundary of the wait record ${grant.wait_index}`,
    );
  }
  // Every user-response task revision names a wait whose anchor must be
  // within the committed transition count; revision-1 records are
  // position-free (the planning flow accepts them on any active boundary).
  // Honest loader boundary: a revision-1 task record carries no execution
  // or transition anchor, so the loader cannot prove whether it was
  // accepted before or after a failed execution — forbidding additions
  // after a failure is the reducer's unified successor-gate duty, and this
  // module does not claim an impossible temporal check (nor add a
  // timestamp, anchor or schema field) for position-free records.
  for (const task of taskRevisions) {
    if (task.revision === 1) {
      continue;
    }
    const ordinal = taskWaitOf(task).transition_count;
    if (ordinal > transitions.length) {
      throw new PipelineV2StateError(
        `task revision record ${task.index} declares a wait whose transition count exceeds the ${transitions.length} committed transitions`,
      );
    }
  }
  if (planPosition !== planRevisions.length) {
    const plan = planRevisions[planPosition]!;
    throw new PipelineV2StateError(
      `plan revision record ${plan.index} names origin execution ${plan.origin_execution}, whose start exceeds the ${transitions.length} committed transitions`,
    );
  }
  for (let generationIndex = 0; generationIndex < generations.length; generationIndex++) {
    const generation = generations[generationIndex]!;
    if (iterationPositions[generationIndex] !== generation.iterations.length) {
      throw new PipelineV2StateError(
        `generation ${generation.index} records iterations whose anchors exceed the ${transitions.length} committed transitions`,
      );
    }
    for (const iteration of generation.iterations) {
      if (iteration.closed !== undefined && !closedCloses.has(`${generation.index}:${iteration.index}`)) {
        throw new PipelineV2StateError(
          `iteration ${iteration.index} of generation ${generation.index} records a close that never matched the open iteration at its anchor`,
        );
      }
    }
    // Every generation lifecycle record the document carries must have been
    // consumed by the single worklist replay. An un-consumed record with an
    // anchor inside the replayed boundaries already failed its boundary's
    // pending pass; what reaches here names a boundary that was never
    // replayed. A generation that is genuinely still open carries no
    // closure and stays valid.
    if (!openedGenerations.has(generation.index)) {
      throw new PipelineV2StateError(
        `generation ${generation.index} opens at committed transition count ${generation.opened_transition_count}, which exceeds the ${transitions.length} committed transitions`,
      );
    }
    if (generation.closed !== undefined && !closedGenerations.has(generation.index)) {
      throw new PipelineV2StateError(
        `generation ${generation.index} closes at committed transition count ${generation.closed.closed_transition_count}, which exceeds the ${transitions.length} committed transitions`,
      );
    }
  }
  if (cursor.current_state !== replayCursor) {
    throw new PipelineV2StateError(
      `cursor.current_state ${JSON.stringify(cursor.current_state)} does not match the replayed cursor ${JSON.stringify(replayCursor)}`,
    );
  }
  if (transitions.length < executions.length - 1) {
    const uncommitted = executions[transitions.length]!;
    throw new PipelineV2StateError(
      `execution ${uncommitted.index} has no committed transition; a new execution starts only after the previous execution's transition is committed`,
    );
  }
  // The single execution without a committed transition — the one started
  // after the last committed transition (or after the last response) — was
  // started at the replayed cursor. Every transition-bound execution is
  // already tied to the replay cursor through its own transition; the
  // unbound one is tied here. This holds for agent and decision executions
  // in any phase, including an in-flight, a settled-but-unbound, and a
  // failed execution. The diagnostic names only the execution index and
  // safe state ids.
  if (executions.length === transitions.length + 1) {
    const unbound = executions[transitions.length]!;
    if (unbound.state_id !== replayCursor) {
      throw new PipelineV2StateError(
        `execution ${unbound.index} ran state ${JSON.stringify(unbound.state_id)}, expected the replayed cursor ${JSON.stringify(replayCursor)}`,
      );
    }
  }

  if (terminal !== undefined) {
    if (terminal.state_id !== cursor.current_state) {
      throw new PipelineV2StateError(
        `terminal state ${JSON.stringify(terminal.state_id)} does not match the cursor ${JSON.stringify(cursor.current_state)}`,
      );
    }
    if (executions.length > transitions.length) {
      throw new PipelineV2StateError(
        `the terminal was reached but execution ${executions[transitions.length]!.index}'s transition was never committed`,
      );
    }
    for (const execution of executions) {
      if (!isSettledExecution(execution)) {
        throw new PipelineV2StateError(
          `the terminal was reached but execution ${execution.index} has phase ${JSON.stringify(execution.phase)}`,
        );
      }
      if (execution.type === "agent" && execution.phase !== "cleanup_completed") {
        throw new PipelineV2StateError(
          `the terminal was reached but agent execution ${execution.index} is not cleaned up`,
        );
      }
    }
    if (status === "active" && phase !== "running" && phase !== "publishing_outputs") {
      throw new PipelineV2StateError(
        `a reached terminal state requires phase "running" or "publishing_outputs", got ${JSON.stringify(phase)}`,
      );
    }
  }

  if (runOutputs !== undefined && terminal === undefined) {
    throw new PipelineV2StateError("run_outputs exist but the terminal state was never reached");
  }

  if (status === "success") {
    if (terminal === undefined || terminal.result !== "success") {
      throw new PipelineV2StateError('run status success requires a reached terminal state with result "success"');
    }
    if (failure !== undefined) {
      throw new PipelineV2StateError('run status success must not carry a failure reason');
    }
    if (phase !== "finished") {
      throw new PipelineV2StateError('run status success requires phase "finished"');
    }
    if (runOutputs === undefined) {
      throw new PipelineV2StateError('run status success requires published run_outputs (an empty list is valid)');
    }
  } else if (status === "failed") {
    if (failure === undefined) {
      throw new PipelineV2StateError('run status failed requires a normalized failure reason');
    }
    if (phase !== "finished") {
      throw new PipelineV2StateError('run status failed requires phase "finished"');
    }
    if (failure.reason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
      throw new PipelineV2StateError(
        `run status failed must not carry the session cleanup failure reason ${JSON.stringify(PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON)}; a cleanup failure finalizes as status "cleanup_failed"`,
      );
    }
    const terminalFailedPublished =
      terminal !== undefined && terminal.result === "failed" && runOutputs !== undefined;
    if (failure.reason === PIPELINE_V2_TERMINAL_FAILURE_REASON && !terminalFailedPublished) {
      throw new PipelineV2StateError(
        `failure reason ${JSON.stringify(PIPELINE_V2_TERMINAL_FAILURE_REASON)} requires a failed terminal with published run_outputs`,
      );
    }
    if (terminalFailedPublished && failure.reason !== PIPELINE_V2_TERMINAL_FAILURE_REASON) {
      throw new PipelineV2StateError(
        `a failed terminal with published run_outputs finalizes with failure reason ${JSON.stringify(PIPELINE_V2_TERMINAL_FAILURE_REASON)}, got ${JSON.stringify(failure.reason)}`,
      );
    }
    for (const execution of executions) {
      if (execution.type === "agent" && hasFailedSessionCleanup(execution)) {
        throw new PipelineV2StateError(
          `execution ${execution.index} records a failed session cleanup, so the run must finalize as status "cleanup_failed"`,
        );
      }
    }
  } else if (status === "cleanup_failed") {
    if (failure === undefined || failure.reason !== PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
      throw new PipelineV2StateError(
        `run status cleanup_failed requires the failure reason ${JSON.stringify(PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON)}`,
      );
    }
    if (phase !== "finished") {
      throw new PipelineV2StateError('run status cleanup_failed requires phase "finished"');
    }
    if (terminal !== undefined) {
      throw new PipelineV2StateError("a run with a reached terminal state cannot end as a cleanup failure");
    }
    if (runOutputs !== undefined) {
      throw new PipelineV2StateError("run status cleanup_failed must not carry published run_outputs");
    }
    const last = executions[executions.length - 1];
    if (
      last === undefined ||
      last.type !== "agent" ||
      last.phase !== "failed" ||
      !hasFailedSessionCleanup(last)
    ) {
      throw new PipelineV2StateError(
        "run status cleanup_failed requires the last agent execution to have failed with an unconfirmed session cleanup",
      );
    }
  } else if (status === "waiting") {
    // A waiting run is temporarily immobile, not finalized: it must be a
    // clean graph pause at the cursor with no terminal, no published
    // outputs, no failure, every execution settled and every committed
    // transition bound to its execution. The open wait record itself and
    // the cursor position are already enforced by the biconditional and
    // the joint replay above.
    if (terminal !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry a reached terminal state");
    }
    if (runOutputs !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry published run_outputs");
    }
    if (failure !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry a failure reason");
    }
    for (const execution of executions) {
      if (!isSettledExecution(execution)) {
        throw new PipelineV2StateError(
          `a waiting run requires execution ${execution.index} to be finished, it has phase ${JSON.stringify(execution.phase)}`,
        );
      }
    }
    if (transitions.length !== executions.length) {
      const unbound = executions[transitions.length]!;
      throw new PipelineV2StateError(
        `a waiting run requires every execution's transition to be committed; execution ${unbound.index} has no committed transition`,
      );
    }
  } else {
    if (failure !== undefined) {
      throw new PipelineV2StateError('an active run must not carry a failure reason');
    }
    if (phase === "finished") {
      throw new PipelineV2StateError('an active run must not be in the finished phase');
    }
    if (phase === "publishing_outputs") {
      if (terminal === undefined) {
        throw new PipelineV2StateError('phase "publishing_outputs" requires a reached terminal state');
      }
      if (runOutputs === undefined) {
        throw new PipelineV2StateError('phase "publishing_outputs" requires published run_outputs');
      }
    }
    if (runOutputs !== undefined && phase !== "publishing_outputs") {
      throw new PipelineV2StateError(
        `run_outputs exist while phase is ${JSON.stringify(phase)}; an active run records run_outputs only in phase "publishing_outputs"`,
      );
    }
  }

  const state: PipelineV2RunState = {
    schema_version: PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
    revision,
    run_id: runId,
    status,
    phase,
    started_at: startedAt,
    updated_at: updatedAt,
    pipeline,
    inputs,
    cursor,
    executions,
    transitions,
    generations,
    task_revisions: taskRevisions,
    plan_revisions: planRevisions,
    grants,
    waits,
  };
  if (terminal !== undefined) {
    state.terminal = terminal;
  }
  if (runOutputs !== undefined) {
    state.run_outputs = runOutputs;
  }
  if (failure !== undefined) {
    state.failure = failure;
  }
  return deepFreeze(state);
}

/**
 * The single open stage iteration of a validated run state, if any: the
 * open generation (the last record without a `closed` projection) whose
 * last iteration entry is still open. The coordinator resolves the exact
 * durable `iteration_index` of a stage start from it. There is no second
 * registry and no replay here — the loader has already proven the
 * document's coherence; the restore verifier's projection check is the
 * sibling membership predicate over the shared interval resolver, not
 * this open-iteration query.
 */
export function pipelineV2OpenStageIteration(
  state: PipelineV2RunState,
): {
  readonly generation_index: number;
  readonly template_id: string;
  readonly stage_id: string;
  readonly iteration_index: number;
} | null {
  const last = state.generations[state.generations.length - 1];
  if (last === undefined || last.closed !== undefined) {
    return null;
  }
  const iteration = last.iterations[last.iterations.length - 1];
  if (iteration === undefined || iteration.closed !== undefined) {
    return null;
  }
  return Object.freeze({
    generation_index: last.index,
    template_id: last.template_id,
    stage_id: last.stage_id,
    iteration_index: iteration.index,
  });
}

/**
 * The unified positional interval of one stage iteration relative to an
 * execution start boundary: the iteration was open at the start exactly
 * when it opened at or before the boundary and is not closed strictly
 * before it. A closure anchored at the start boundary follows the
 * execution it contains (`normal_close`/`exhausted` — the closure is
 * recorded after the last contained execution settled), while a wait-bound
 * closure (`grant`/`replanned`) at the same boundary precedes every start
 * of that boundary (the wait cycle runs before the response moves the
 * cursor and admits new executions). The loader's execution-start check
 * and the shared query below use exactly this predicate, so the two sides
 * cannot drift apart.
 */
function iterationOpenForStart(iteration: PipelineV2StageIterationRecord, boundary: number): boolean {
  const closed = iteration.closed;
  if (iteration.opened_transition_count > boundary) {
    return false;
  }
  if (closed === undefined) {
    return true;
  }
  if (closed.closed_transition_count < boundary) {
    return false;
  }
  return !(
    closed.closed_transition_count === boundary &&
    (closed.by === "grant" || closed.by === "replanned")
  );
}

/**
 * The single shared internal candidate resolver for stage-iteration
 * projection at an execution start boundary: every generation/iteration
 * pair whose unified positional interval is open at the boundary,
 * narrowed by the optional recorded per-generation iteration index and
 * the optional compiled stage template. Both public consumers — the
 * exact lookup and the membership predicate — resolve through this one
 * resolver, so their candidate logic cannot drift apart. The candidate
 * list itself is never exported.
 */
interface StageIterationCandidate {
  readonly generation: PipelineV2StageGenerationRecord;
  readonly iteration: PipelineV2StageIterationRecord;
}

function stageIterationCandidatesAt(
  state: PipelineV2RunState,
  transitionCount: number,
  recordedIterationIndex?: number,
  stageTemplate?: string,
): StageIterationCandidate[] {
  const matches: StageIterationCandidate[] = [];
  for (const generation of state.generations) {
    for (const iteration of generation.iterations) {
      if (iterationOpenForStart(iteration, transitionCount)) {
        matches.push({ generation, iteration });
      }
    }
  }
  let candidates = matches;
  if (recordedIterationIndex !== undefined) {
    candidates = candidates.filter((match) => match.iteration.index === recordedIterationIndex);
  }
  if (stageTemplate !== undefined) {
    candidates = candidates.filter((match) => match.generation.template_id === stageTemplate);
  }
  return candidates;
}

/**
 * The exact stage-iteration projection at an execution start boundary —
 * answered only when the durable data proves exactly one candidate. The
 * unified positional interval, the optional recorded iteration index
 * (which restarts at 1 in every generation and is therefore not a global
 * identifier) and the optional stage template are applied as filters by
 * the single shared internal candidate resolver. A unique candidate is
 * returned as the frozen projection; zero candidates resolve to `null`
 * (nothing open at the boundary matches the filters); more than one
 * candidate — e.g. a reused template with the same iteration index across
 * touching generations on one boundary — also resolves to `null`: the
 * durable data cannot distinguish the generation ordinals, so no
 * generation is claimed and no first or last element of the ambiguous
 * set is ever selected. A reference or template outside the candidates
 * resolves to nothing.
 */
export function pipelineV2StageIterationAt(
  state: PipelineV2RunState,
  transitionCount: number,
  recordedIterationIndex?: number,
  stageTemplate?: string,
): {
  readonly generation_index: number;
  readonly template_id: string;
  readonly stage_id: string;
  readonly iteration_index: number;
} | null {
  const candidates = stageIterationCandidatesAt(state, transitionCount, recordedIterationIndex, stageTemplate);
  if (candidates.length !== 1) {
    return null;
  }
  const chosen = candidates[0]!;
  return Object.freeze({
    generation_index: chosen.generation.index,
    template_id: chosen.generation.template_id,
    stage_id: chosen.generation.stage_id,
    iteration_index: chosen.iteration.index,
  });
}

/**
 * The membership form of the same shared candidate resolver, exported for
 * the restore verifier in `pipeline_v2_resume_context.ts`: the execution
 * belongs to the admissible candidate set exactly when at least one
 * candidate matches the start boundary's interval, the recorded iteration
 * index and the compiled stage template. Ambiguity between several
 * matching generations is not a mismatch here — the durable data cannot
 * distinguish their ordinals and the verifier claims no generation of its
 * own; the absence of any matching candidate is the mismatch. The
 * candidate list itself stays internal and is not exported.
 */
export function pipelineV2StageIterationMembershipAt(
  state: PipelineV2RunState,
  transitionCount: number,
  recordedIterationIndex: number,
  stageTemplate: string,
): boolean {
  return stageIterationCandidatesAt(state, transitionCount, recordedIterationIndex, stageTemplate).length > 0;
}

export function parsePipelineV2RunState(raw: string): PipelineV2RunState {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2StateError("pipeline v2 run state document is not valid JSON");
  }
  return validatePipelineV2RunState(parsed);
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreeze(entry);
    }
    Object.freeze(value);
    return value;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

/** An execution whose phase can never change again (failed or completed). */
function isSettledExecution(execution: PipelineV2ExecutionState): boolean {
  if (execution.type === "agent") {
    return execution.phase === "cleanup_completed" || execution.phase === "failed";
  }
  return execution.phase === "evaluated" || execution.phase === "failed";
}

function cloneIdentity(identity: PipelineV2RunPipelineIdentity): PipelineV2RunPipelineIdentity {
  return { ...identity };
}

function cloneRunInput(input: PipelineV2RunInputState): PipelineV2RunInputState {
  return { ...input };
}

function cloneAgentOutput(output: PipelineV2AgentOutputState): PipelineV2AgentOutputState {
  return { ...output };
}

function cloneDecisionRecord(record: PipelineDecisionStateRecord): PipelineDecisionStateRecord {
  switch (record.status) {
    case "selected":
      return { ...record, active_constraint_ids: [...record.active_constraint_ids] };
    case "uncovered":
      return { ...record, active_constraint_ids: [...record.active_constraint_ids] };
    case "inconsistent_facts":
      return { ...record, violated_relation_ids: [...record.violated_relation_ids] };
    case "invalid_facts":
      return { ...record };
  }
}

function cloneSessionCleanupPair(
  pair: PipelineV2SessionCleanupPair,
): PipelineV2SessionCleanupPair {
  return { execution: pair.execution, tool: pair.tool };
}

function cloneExecution(execution: PipelineV2ExecutionState): PipelineV2ExecutionState {
  if (execution.type === "agent") {
    const clone: PipelineV2AgentExecutionState = { ...execution };
    if (execution.session_cleanup !== undefined) {
      clone.session_cleanup = cloneSessionCleanupPair(execution.session_cleanup);
    }
    if (execution.outputs !== undefined) {
      clone.outputs = execution.outputs.map(cloneAgentOutput);
    }
    return clone;
  }
  const clone: PipelineV2DecisionExecutionState = { ...execution };
  if (execution.result !== undefined) {
    clone.result = cloneDecisionRecord(execution.result);
  }
  return clone;
}

function cloneTransition(transition: PipelineV2CommittedTransitionState): PipelineV2CommittedTransitionState {
  return { ...transition };
}

function cloneRunOutput(output: PipelineV2RunOutputState): PipelineV2RunOutputState {
  return { ...output };
}

function cloneWaitRecord(record: PipelineV2WaitRecord): PipelineV2WaitRecord {
  const response =
    record.response === undefined
      ? undefined
      : {
          action_id: record.response.action_id,
          response_sha256: record.response.response_sha256,
        };
  const intent =
    record.intent === undefined ? undefined : { intent_sha256: record.intent.intent_sha256 };
  return {
    index: record.index,
    transition_count: record.transition_count,
    state_id: record.state_id,
    reason: record.reason,
    request_sha256: record.request_sha256,
    actions: record.actions.map((action) => ({ id: action.id, to: action.to })),
    ...(intent !== undefined ? { intent } : {}),
    ...(response !== undefined ? { response } : {}),
  };
}

function cloneStageIteration(iteration: PipelineV2StageIterationRecord): PipelineV2StageIterationRecord {
  if (iteration.closed === undefined) {
    return { index: iteration.index, opened_transition_count: iteration.opened_transition_count };
  }
  const closed: PipelineV2StageIterationRecord["closed"] = {
    by: iteration.closed.by,
    closed_transition_count: iteration.closed.closed_transition_count,
  };
  if (iteration.closed.wait_index !== undefined) {
    return {
      index: iteration.index,
      opened_transition_count: iteration.opened_transition_count,
      closed: { ...closed, wait_index: iteration.closed.wait_index },
    };
  }
  return {
    index: iteration.index,
    opened_transition_count: iteration.opened_transition_count,
    closed,
  };
}

function cloneStageGeneration(
  generation: PipelineV2StageGenerationRecord,
): PipelineV2StageGenerationRecord {
  const open =
    generation.open_iteration === undefined
      ? {}
      : {
          open_iteration: {
            index: generation.open_iteration.index,
            opened_transition_count: generation.open_iteration.opened_transition_count,
          },
        };
  const closed =
    generation.closed === undefined
      ? {}
      : {
          closed: {
            by: generation.closed.by,
            closed_transition_count: generation.closed.closed_transition_count,
          },
        };
  return {
    index: generation.index,
    stage_id: generation.stage_id,
    stage_position: generation.stage_position,
    template_id: generation.template_id,
    plan_sha256: generation.plan_sha256,
    initial_budget: generation.initial_budget,
    opened_transition_count: generation.opened_transition_count,
    iteration_count: generation.iteration_count,
    iterations: generation.iterations.map(cloneStageIteration),
    ...open,
    ...closed,
  };
}

function cloneTaskRevision(task: PipelineV2TaskRevisionState): PipelineV2TaskRevisionState {
  const waitLinks =
    task.wait_index === undefined || task.intent_sha256 === undefined
      ? {}
      : { wait_index: task.wait_index, intent_sha256: task.intent_sha256 };
  return {
    index: task.index,
    task_id: task.task_id,
    revision: task.revision,
    sha256: task.sha256,
    previous_sha256: task.previous_sha256,
    ...waitLinks,
  };
}

function clonePlanRevision(plan: PipelineV2PlanRevisionState): PipelineV2PlanRevisionState {
  return { ...plan };
}

function cloneIterationGrant(grant: PipelineV2IterationGrantState): PipelineV2IterationGrantState {
  return { ...grant };
}

function cloneState(state: PipelineV2RunState): PipelineV2RunState {
  const clone: PipelineV2RunState = {
    schema_version: state.schema_version,
    revision: state.revision,
    run_id: state.run_id,
    status: state.status,
    phase: state.phase,
    started_at: state.started_at,
    updated_at: state.updated_at,
    pipeline: { ...state.pipeline },
    inputs: state.inputs.map(cloneRunInput),
    cursor: { ...state.cursor },
    executions: state.executions.map(cloneExecution),
    transitions: state.transitions.map(cloneTransition),
    generations: state.generations.map(cloneStageGeneration),
    task_revisions: state.task_revisions.map(cloneTaskRevision),
    plan_revisions: state.plan_revisions.map(clonePlanRevision),
    grants: state.grants.map(cloneIterationGrant),
    waits: state.waits.map(cloneWaitRecord),
  };
  if (state.terminal !== undefined) {
    clone.terminal = { ...state.terminal };
  }
  if (state.run_outputs !== undefined) {
    clone.run_outputs = state.run_outputs.map(cloneRunOutput);
  }
  if (state.failure !== undefined) {
    clone.failure = { ...state.failure };
  }
  return clone;
}

function fail(current: PipelineV2RunState, message: string): never {
  throw new PipelineV2StateError(
    `command rejected for run ${JSON.stringify(current.run_id)} (revision ${current.revision}, status ${current.status}, phase ${current.phase}): ${message}`,
  );
}

function requireLastExecution(current: PipelineV2RunState): PipelineV2ExecutionState {
  const execution = current.executions[current.executions.length - 1];
  if (execution === undefined) {
    fail(current, "no execution is in flight");
  }
  return execution;
}

function requireUnfinishedAgent(current: PipelineV2RunState, what: string): PipelineV2AgentExecutionState {
  const execution = requireLastExecution(current);
  if (execution.type !== "agent") {
    fail(
      current,
      `${what} applies to an agent execution, but execution ${execution.index} is a ${execution.type} execution`,
    );
  }
  if (execution.phase === "cleanup_completed" || execution.phase === "failed") {
    fail(
      current,
      `execution ${execution.index} already finished with phase ${JSON.stringify(execution.phase)}`,
    );
  }
  return execution;
}

function requireUnfinishedDecision(current: PipelineV2RunState, what: string): PipelineV2DecisionExecutionState {
  const execution = requireLastExecution(current);
  if (execution.type !== "decision") {
    fail(
      current,
      `${what} applies to a decision execution, but execution ${execution.index} is an ${execution.type} execution`,
    );
  }
  if (execution.phase !== "evaluating") {
    fail(
      current,
      `${what} requires decision execution phase "evaluating", got ${JSON.stringify(execution.phase)}`,
    );
  }
  return execution;
}

/**
 * Global durable session id uniqueness across both slots of every agent
 * execution, including the in-flight one: one id can never be reused, not
 * even once as an Execution and once as a Tool session.
 */
function rejectDurableSessionId(current: PipelineV2RunState, sessionId: string): void {
  for (const existing of current.executions) {
    if (existing.type !== "agent") {
      continue;
    }
    if (existing.execution_session_id === sessionId) {
      fail(
        current,
        `session ${JSON.stringify(sessionId)} already belongs to execution ${existing.index} as an execution session`,
      );
    }
    if (existing.tool_session_id === sessionId) {
      fail(
        current,
        `session ${JSON.stringify(sessionId)} already belongs to execution ${existing.index} as a tool session`,
      );
    }
  }
}

/**
 * The stage lifecycle openings record their durable anchor at the boundary
 * whose last execution must be settled: the contract hook order is a
 * settled execution, then the generation and iteration opened at the same
 * boundary, then the transition. An opening recorded while an execution is
 * still in flight would let a later failure share its boundary with the
 * opening in a way no durable anchor can disambiguate, so the opening
 * cases reject it; the failed-execution case is already excluded by the
 * unified post-failure successor gate before the switch.
 */
function requireNoInFlightExecution(
  current: PipelineV2RunState,
  what: string,
): void {
  const last = current.executions[current.executions.length - 1];
  if (last === undefined || isSettledExecution(last)) {
    return;
  }
  fail(
    current,
    `${what} requires the run's last execution ${last.index} to be settled, got the in-flight phase ${JSON.stringify(last.phase)}`,
  );
}

/**
 * The single TransitionStep contract shared by the reducer and the loader:
 * an engine step obeys the same safe-id and integer rules as the persisted
 * transition record, so an accepted command can never produce a document
 * the loader rejects.
 */
function expectTransitionStep(step: unknown, what: string): TransitionStep {
  if (step === null || typeof step !== "object" || Array.isArray(step)) {
    throw new PipelineV2StateError(`${what} is not a JSON object`);
  }
  const obj = step as Record<string, unknown>;
  return {
    from: expectSafeId(obj.from, `${what}.from`),
    outcome: expectNonEmptyString(obj.outcome, `${what}.outcome`),
    to: expectSafeId(obj.to, `${what}.to`),
    transition_index: expectSafeNonNegativeInteger(obj.transition_index, `${what}.transition_index`),
  };
}

/**
 * The single open stage generation of a run state (the last generation
 * record without a `closed` projection), if any. The loader proves there
 * is at most one; the reducer mirrors that rule at write time.
 */
function openGenerationOf(
  current: PipelineV2RunState,
): PipelineV2StageGenerationRecord | undefined {
  const last = current.generations[current.generations.length - 1];
  return last !== undefined && last.closed === undefined ? last : undefined;
}

/**
 * The open iteration of the open generation, if any. At most one
 * iteration is open at a time and it belongs to the open generation.
 */
function openIterationOf(current: PipelineV2RunState): {
  readonly generation: PipelineV2StageGenerationRecord;
  readonly index: number;
} | undefined {
  const generation = openGenerationOf(current);
  const iteration = generation?.iterations[generation.iterations.length - 1];
  if (generation === undefined || iteration === undefined || iteration.closed !== undefined) {
    return undefined;
  }
  return { generation, index: iteration.index };
}

/**
 * The role contract of the start commands: a stage execution must name
 * exactly the open iteration of the open generation; a planning or
 * control execution must start outside any open iteration and carry no
 * iteration index. Returns the durable iteration index for a stage role.
 */
function requireIterationForRole(
  current: PipelineV2RunState,
  role: PipelineV2ExecutionRole,
  commandIterationIndex: number | undefined,
  what: string,
): number | undefined {
  const open = openIterationOf(current);
  if (role === "stage") {
    if (open === undefined) {
      fail(current, `${what} with the "stage" role requires an open stage generation with an open iteration`);
    }
    if (commandIterationIndex === undefined) {
      throw new PipelineV2StateError(
        `${what} with the "stage" role requires the open iteration index`,
      );
    }
    if (!isPositiveSafeInteger(commandIterationIndex)) {
      throw new PipelineV2StateError(
        `${what} iteration index must be a positive safe integer, got ${JSON.stringify(commandIterationIndex)}`,
      );
    }
    if (commandIterationIndex !== open.index) {
      fail(
        current,
        `${what} names iteration ${JSON.stringify(commandIterationIndex)}, but the open iteration of generation ${open.generation.index} is ${open.index}`,
      );
    }
    return open.index;
  }
  if (commandIterationIndex !== undefined) {
    throw new PipelineV2StateError(
      `${what} with the ${JSON.stringify(role)} role must not carry an iteration index`,
    );
  }
  if (open !== undefined) {
    fail(
      current,
      `${what} with the ${JSON.stringify(role)} role cannot start inside the open iteration of generation ${open.generation.index}`,
    );
  }
  return undefined;
}

/** Guards shared by both start commands: cursor match, previous commit, budget. */
function requireStartableCursor(current: PipelineV2RunState, stateId: string): void {
  if (current.phase !== "running") {
    fail(current, `starting an execution requires phase "running", got ${JSON.stringify(current.phase)}`);
  }
  if (current.terminal !== undefined) {
    fail(current, "the terminal state is already reached; no further executions are possible");
  }
  const previous = current.executions[current.executions.length - 1];
  if (previous !== undefined) {
    if (!isSettledExecution(previous)) {
      fail(
        current,
        `a new execution requires the previous execution to be finished, execution ${previous.index} has phase ${JSON.stringify(previous.phase)}`,
      );
    }
    if (current.transitions.length !== current.executions.length) {
      fail(
        current,
        "a new execution requires the previous execution's transition to be committed",
      );
    }
  }
  if (current.cursor.transition_count >= current.pipeline.max_transitions) {
    fail(
      current,
      `starting execution ${current.executions.length + 1} would exceed the pipeline transition budget ${current.pipeline.max_transitions} (${current.cursor.transition_count} transitions already committed)`,
    );
  }
  if (stateId !== current.cursor.current_state) {
    fail(
      current,
      `execution state ${JSON.stringify(stateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
    );
  }
}

/**
 * Applies one command to the run state and returns the next independent,
 * deep-frozen snapshot. Each accepted command grows the revision by exactly
 * one and refreshes `updated_at`; the input state and the command are never
 * mutated. Rejected commands throw `PipelineV2StateError` and leave the
 * input state untouched.
 */
export function reducePipelineV2RunCommand(
  current: PipelineV2RunState | null,
  command: PipelineV2RunCommand,
  now: Date,
): PipelineV2RunState {
  if (command.kind === "create_run") {
    if (current !== null) {
      throw new PipelineV2StateError(
        `create_run rejected: run ${JSON.stringify(current.run_id)} already exists (revision ${current.revision})`,
      );
    }
    const pipeline = cloneIdentity(command.pipeline);
    if (pipeline.schema_version !== 2) {
      throw new PipelineV2StateError("create_run requires pipeline schema_version 2");
    }
    if (!isCanonicalAbsolutePath(pipeline.bundle_root)) {
      throw new PipelineV2StateError("create_run requires an absolute canonical bundle root");
    }
    if (!isLowercaseSha256(pipeline.execution_snapshot_sha256)) {
      throw new PipelineV2StateError("create_run requires a lowercase hex execution snapshot digest");
    }
    if (!isPipelineV2SafeId(pipeline.entry_state)) {
      throw new PipelineV2StateError("create_run requires a safe entry state id");
    }
    if (!isPositiveSafeInteger(pipeline.max_transitions)) {
      throw new PipelineV2StateError("create_run requires a positive max_transitions");
    }
    if (!Array.isArray(command.inputs)) {
      throw new PipelineV2StateError("create_run requires a run inputs array");
    }
    const inputs = command.inputs.map((input, index) =>
      validateRunInputState(input, `create_run inputs[${index}]`),
    );
    {
      const ids = new Set<string>();
      for (const input of inputs) {
        if (ids.has(input.id)) {
          throw new PipelineV2StateError(
            `create_run declares run input ${JSON.stringify(input.id)} more than once`,
          );
        }
        ids.add(input.id);
      }
    }
    if (!isPipelineV2SafeId(command.runId)) {
      throw new PipelineV2StateError("create_run requires a safe non-empty run id");
    }
    const at = now.toISOString();
    const state: PipelineV2RunState = {
      schema_version: PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
      revision: 1,
      run_id: command.runId,
      status: "active",
      phase: "running",
      started_at: at,
      updated_at: at,
      pipeline,
      inputs,
      cursor: { current_state: pipeline.entry_state, transition_count: 0 },
      executions: [],
      transitions: [],
      generations: [],
      task_revisions: [],
      plan_revisions: [],
      grants: [],
      waits: [],
    };
    return deepFreeze(state);
  }

  if (current === null) {
    throw new PipelineV2StateError(`command ${command.kind} rejected: no pipeline v2 run state exists yet`);
  }
  // The commands a waiting run still accepts: the user-response successor
  // and the schema v7 intervention records that must be durable before
  // the response. Everything else waits for the response or is rejected
  // on a finalized run.
  const waitingAllowed = new Set<string>([
    "wait_response_recorded",
    "plan_intent_accepted",
    "task_revision_accepted",
    "iteration_grant_recorded",
    "stage_iteration_closed",
  ]);
  if (command.kind !== "wait_response_recorded" && current.status !== "active") {
    if (current.status === "waiting") {
      if (!waitingAllowed.has(command.kind)) {
        fail(
          current,
          "the run is waiting for an explicit user response; only the wait response and the durable intervention records advance a waiting run",
        );
      }
    } else {
      fail(
        current,
        `the run is already finalized with status ${JSON.stringify(current.status)}; the terminal run status is immutable`,
      );
    }
  }

  // The unified post-failure successor gate. Once the last execution of
  // the run has failed — agent or decision — the only durable successor is
  // the run failure finalization: `run_failed` for the ordinary failure,
  // `run_cleanup_failed` for the agent failure with an unconfirmed session
  // cleanup; their own case rules decide which of the two is admissible
  // and reject the wrong one with their existing typed semantics. No
  // lifecycle, task/plan, wait, execution, transition, terminal or
  // publication command is accepted after a failed execution. The gate
  // reads only the command discriminator (never its payload), does not
  // call the clock, and leaves the revision and the state untouched on
  // rejection.
  const lastExecution = current.executions[current.executions.length - 1];
  if (
    lastExecution !== undefined &&
    lastExecution.phase === "failed" &&
    command.kind !== "run_failed" &&
    command.kind !== "run_cleanup_failed"
  ) {
    throw new PipelineV2StateError(
      `command ${JSON.stringify(command.kind)} rejected: the run's last execution ${lastExecution.index} has failed; a failed execution allows only the run failure finalization (run_failed or run_cleanup_failed)`,
    );
  }

  const next = cloneState(current);
  const at = now.toISOString();
  next.updated_at = at;
  next.revision = current.revision + 1;
  switch (command.kind) {
    case "start_agent_execution": {
      if (!isPipelineV2SafeId(command.stateId)) {
        throw new PipelineV2StateError(
          `start_agent_execution requires a safe agent state id, got ${JSON.stringify(command.stateId)}`,
        );
      }
      if (!isNonEmptyString(command.profile)) {
        throw new PipelineV2StateError("start_agent_execution requires a non-empty profile name");
      }
      const role = expectEnum(
        command.executionRole,
        PIPELINE_V2_EXECUTION_ROLES,
        "start_agent_execution executionRole",
      );
      requireStartableCursor(current, command.stateId);
      const iterationIndex = requireIterationForRole(current, role, command.iterationIndex, "start_agent_execution");
      const execution: PipelineV2AgentExecutionState = {
        index: current.executions.length + 1,
        type: "agent",
        state_id: command.stateId,
        attempt: 1,
        profile: command.profile,
        execution_role: role,
        phase: "started",
        ...(iterationIndex !== undefined ? { iteration_index: iterationIndex } : {}),
      };
      next.executions = [...next.executions, execution];
      break;
    }
    case "agent_data_prepared": {
      const execution = requireUnfinishedAgent(current, "agent_data_prepared");
      if (execution.phase !== "started") {
        fail(current, `data preparation requires execution phase "started", got ${JSON.stringify(execution.phase)}`);
      }
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.phase = "data_prepared";
      break;
    }
    case "agent_execution_session_created": {
      const execution = requireUnfinishedAgent(current, "agent_execution_session_created");
      if (execution.phase !== "data_prepared") {
        fail(
          current,
          `the execution session requires execution phase "data_prepared", got ${JSON.stringify(execution.phase)}`,
        );
      }
      if (!isNonEmptyString(command.sessionId)) {
        throw new PipelineV2StateError("agent_execution_session_created requires a non-empty session id");
      }
      rejectDurableSessionId(current, command.sessionId);
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.execution_session_id = command.sessionId;
      last.phase = "execution_session_created";
      break;
    }
    case "agent_tool_session_created": {
      const execution = requireUnfinishedAgent(current, "agent_tool_session_created");
      if (execution.phase !== "execution_session_created") {
        fail(
          current,
          `the tool session requires execution phase "execution_session_created", got ${JSON.stringify(execution.phase)}`,
        );
      }
      if (!isNonEmptyString(command.sessionId)) {
        throw new PipelineV2StateError("agent_tool_session_created requires a non-empty session id");
      }
      rejectDurableSessionId(current, command.sessionId);
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.tool_session_id = command.sessionId;
      last.phase = "sessions_created";
      break;
    }
    case "agent_running": {
      const execution = requireUnfinishedAgent(current, "agent_running");
      if (execution.phase !== "sessions_created") {
        fail(current, `starting the agent requires execution phase "sessions_created", got ${JSON.stringify(execution.phase)}`);
      }
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.phase = "running";
      break;
    }
    case "agent_outputs_accepted": {
      const execution = requireUnfinishedAgent(current, "agent_outputs_accepted");
      if (execution.phase !== "running") {
        fail(current, `accepting outputs requires execution phase "running", got ${JSON.stringify(execution.phase)}`);
      }
      if (!Array.isArray(command.outputs)) {
        throw new PipelineV2StateError("agent_outputs_accepted requires an outputs array");
      }
      const seen = new Set<string>();
      const outputs = command.outputs.map((entry, index) => {
        const output = expectExactObject(entry, `agent_outputs_accepted outputs[${index}]`, ["id", "digest"]);
        const id = expectSafeId(output.id, `agent_outputs_accepted outputs[${index}].id`);
        if (seen.has(id)) {
          throw new PipelineV2StateError(
            `agent_outputs_accepted declares output id ${JSON.stringify(id)} more than once`,
          );
        }
        seen.add(id);
        return { id, digest: expectSha256(output.digest, `agent_outputs_accepted outputs[${index}].digest`) };
      });
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.phase = "outputs_accepted";
      last.outputs = outputs;
      break;
    }
    case "agent_cleanup_completed": {
      const execution = requireUnfinishedAgent(current, "agent_cleanup_completed");
      if (execution.phase !== "outputs_accepted") {
        fail(current, `recording the session cleanup requires execution phase "outputs_accepted", got ${JSON.stringify(execution.phase)}`);
      }
      if (execution.execution_session_id === undefined || execution.tool_session_id === undefined) {
        fail(current, `execution ${execution.index} has no recorded tool or execution session to clean up`);
      }
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.phase = "cleanup_completed";
      last.session_cleanup = { execution: "completed", tool: "completed" };
      break;
    }
    case "agent_failed": {
      const execution = requireUnfinishedAgent(current, "agent_failed");
      const reason = expectEnum(
        command.reason,
        PIPELINE_V2_AGENT_EXECUTION_FAILURE_REASONS,
        "agent_failed reason",
      );
      const cleanup = command.sessionCleanup;
      if (cleanup === null || typeof cleanup !== "object" || Array.isArray(cleanup)) {
        throw new PipelineV2StateError("agent_failed requires a session cleanup pair");
      }
      const sessionCleanup: PipelineV2SessionCleanupPair = {
        execution: expectEnum(
          cleanup.execution,
          PIPELINE_V2_SESSION_CLEANUP_VALUES,
          "agent_failed sessionCleanup.execution",
        ),
        tool: expectEnum(
          cleanup.tool,
          PIPELINE_V2_SESSION_CLEANUP_VALUES,
          "agent_failed sessionCleanup.tool",
        ),
      };
      if (execution.execution_session_id === undefined && sessionCleanup.execution !== "not_required") {
        fail(
          current,
          `execution ${execution.index} records no execution session, so its cleanup outcome must be "not_required"`,
        );
      }
      if (execution.execution_session_id !== undefined && sessionCleanup.execution === "not_required") {
        fail(
          current,
          `execution ${execution.index} has a recorded execution session, so its cleanup outcome must be "completed" or "failed"`,
        );
      }
      if (execution.tool_session_id === undefined && sessionCleanup.tool !== "not_required") {
        fail(
          current,
          `execution ${execution.index} records no tool session, so its cleanup outcome must be "not_required"`,
        );
      }
      if (execution.tool_session_id !== undefined && sessionCleanup.tool === "not_required") {
        fail(
          current,
          `execution ${execution.index} has a recorded tool session, so its cleanup outcome must be "completed" or "failed"`,
        );
      }
      if (
        reason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON &&
        sessionCleanup.execution !== "failed" &&
        sessionCleanup.tool !== "failed"
      ) {
        fail(current, 'the session cleanup failure reason requires a failed session cleanup outcome');
      }
      if (
        (sessionCleanup.execution === "failed" || sessionCleanup.tool === "failed") &&
        reason !== PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON
      ) {
        fail(current, 'a failed session cleanup requires the session cleanup failure reason');
      }
      const last = next.executions[next.executions.length - 1] as PipelineV2AgentExecutionState;
      last.phase = "failed";
      last.failure_reason = reason;
      last.session_cleanup = cloneSessionCleanupPair(sessionCleanup);
      break;
    }
    case "start_decision_execution": {
      if (!isPipelineV2SafeId(command.stateId)) {
        throw new PipelineV2StateError(
          `start_decision_execution requires a safe decision state id, got ${JSON.stringify(command.stateId)}`,
        );
      }
      if (!isLowercaseSha256(command.inputDigest)) {
        throw new PipelineV2StateError(
          `start_decision_execution requires a lowercase hex input digest, got ${JSON.stringify(command.inputDigest)}`,
        );
      }
      const role = expectEnum(
        command.executionRole,
        PIPELINE_V2_EXECUTION_ROLES,
        "start_decision_execution executionRole",
      );
      requireStartableCursor(current, command.stateId);
      const iterationIndex = requireIterationForRole(current, role, command.iterationIndex, "start_decision_execution");
      const execution: PipelineV2DecisionExecutionState = {
        index: current.executions.length + 1,
        type: "decision",
        state_id: command.stateId,
        execution_role: role,
        phase: "evaluating",
        input_digest: command.inputDigest,
        ...(iterationIndex !== undefined ? { iteration_index: iterationIndex } : {}),
      };
      next.executions = [...next.executions, execution];
      break;
    }
    case "decision_evaluated": {
      const execution = requireUnfinishedDecision(current, "decision_evaluated");
      const result = validateDecisionRecord(command.result, "decision_evaluated result");
      const last = next.executions[next.executions.length - 1] as PipelineV2DecisionExecutionState;
      last.phase = "evaluated";
      last.result = result;
      break;
    }
    case "decision_failed": {
      const execution = requireUnfinishedDecision(current, "decision_failed");
      const reason = expectEnum(
        command.reason,
        PIPELINE_V2_DECISION_EXECUTION_FAILURE_REASONS,
        "decision_failed reason",
      );
      const last = next.executions[next.executions.length - 1] as PipelineV2DecisionExecutionState;
      last.phase = "failed";
      last.failure_reason = reason;
      break;
    }
    case "transition_committed": {
      if (current.phase !== "running") {
        fail(current, `committing a transition requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; no further transitions are possible");
      }
      if (!isPositiveSafeInteger(command.executionIndex)) {
        throw new PipelineV2StateError(
          `transition_committed requires a positive execution index, got ${JSON.stringify(command.executionIndex)}`,
        );
      }
      const step = expectTransitionStep(command.step, "transition_committed step");
      const execution = requireLastExecution(current);
      if (command.executionIndex !== execution.index) {
        fail(
          current,
          `transition references execution ${command.executionIndex}, but the last execution is ${execution.index}`,
        );
      }
      if (execution.type === "agent") {
        if (execution.phase !== "cleanup_completed") {
          fail(
            current,
            `committing a transition requires the agent execution to be cleaned up, execution ${execution.index} has phase ${JSON.stringify(execution.phase)}`,
          );
        }
      } else {
        if (execution.phase !== "evaluated") {
          fail(
            current,
            `committing a transition requires the decision execution to be evaluated, execution ${execution.index} has phase ${JSON.stringify(execution.phase)}`,
          );
        }
        if (execution.result !== undefined && execution.result.outcome !== step.outcome) {
          fail(
            current,
            `transition carries outcome ${JSON.stringify(step.outcome)}, but execution ${execution.index} recorded decision outcome ${JSON.stringify(execution.result.outcome)}`,
          );
        }
      }
      if (current.transitions.some((existing) => existing.execution_index === command.executionIndex)) {
        fail(current, `execution ${execution.index} already carries a committed transition`);
      }
      if (step.from !== current.cursor.current_state) {
        fail(
          current,
          `transition starts at ${JSON.stringify(step.from)}, but the cursor is at ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      if (current.cursor.transition_count >= current.pipeline.max_transitions) {
        fail(
          current,
          `committing transition ${current.cursor.transition_count + 1} would exceed the pipeline transition budget ${current.pipeline.max_transitions}`,
        );
      }
      if (step.from !== execution.state_id) {
        fail(
          current,
          `transition starts at ${JSON.stringify(step.from)}, but execution ${execution.index} ran state ${JSON.stringify(execution.state_id)}`,
        );
      }
      next.transitions = [
        ...next.transitions,
        {
          index: step.transition_index,
          from: step.from,
          outcome: step.outcome,
          to: step.to,
          execution_index: command.executionIndex,
        },
      ];
      next.cursor = {
        current_state: step.to,
        transition_count: current.cursor.transition_count + 1,
      };
      break;
    }
    case "run_waiting": {
      // Appends one open wait record to the wait journal (schema v6). The
      // wait is a policy / context-stage decision of the caller (for
      // example the P01 reason "stage_iteration_limit_exhausted"); it
      // never consumes or touches the immutable engine transition budget
      // and it records no transition of its own. Waiting is not a
      // terminal result: the record stays open until the explicit
      // wait_response_recorded command closes it. The record index is the
      // journal position and the transition count is the number of
      // committed graph transitions at this clean boundary.
      const stateId = expectSafeId(command.stateId, "run_waiting state id");
      const reason = expectSafeId(command.reason, "run_waiting reason");
      const requestSha256 = expectSha256(command.requestSha256, "run_waiting request_sha256");
      if (!Array.isArray(command.actions)) {
        throw new PipelineV2StateError("run_waiting requires an actions array");
      }
      if (command.actions.length === 0) {
        throw new PipelineV2StateError("run_waiting requires at least one action");
      }
      const seenActions = new Set<string>();
      const actions = command.actions.map((entry, index) => {
        const action = expectExactObject(entry, `run_waiting actions[${index}]`, ["id", "to"]);
        const id = expectSafeId(action.id, `run_waiting actions[${index}].id`);
        const to = expectSafeId(action.to, `run_waiting actions[${index}].to`);
        if (seenActions.has(id)) {
          throw new PipelineV2StateError(
            `run_waiting declares action id ${JSON.stringify(id)} more than once`,
          );
        }
        seenActions.add(id);
        return { id, to };
      });
      if (current.phase !== "running") {
        fail(current, `entering the wait requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a waiting run cannot be recorded");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "run outputs are already published; a waiting run cannot be recorded");
      }
      if (current.failure !== undefined) {
        fail(current, "the run already carries a failure reason; a waiting run cannot be recorded");
      }
      const lastWait = current.waits[current.waits.length - 1];
      if (lastWait !== undefined && lastWait.response === undefined) {
        fail(
          current,
          `wait record ${lastWait.index} is still open; record its response before entering a new wait`,
        );
      }
      for (const execution of current.executions) {
        if (!isSettledExecution(execution)) {
          fail(
            current,
            `entering the wait requires execution ${execution.index} to be finished, it has phase ${JSON.stringify(execution.phase)}`,
          );
        }
      }
      if (current.transitions.length !== current.executions.length) {
        fail(current, "entering the wait requires every execution's transition to be committed");
      }
      if (stateId !== current.cursor.current_state) {
        fail(
          current,
          `wait state ${JSON.stringify(stateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      next.status = "waiting";
      next.phase = "waiting";
      next.waits = [
        ...next.waits,
        {
          index: current.waits.length + 1,
          transition_count: current.cursor.transition_count,
          state_id: stateId,
          reason,
          request_sha256: requestSha256,
          actions,
        },
      ];
      break;
    }
    case "wait_response_recorded": {
      // The single user-response successor (schema v6). The command
      // carries only content-free references: the wait record index, the
      // expected request manifest digest, the declared action id and the
      // response manifest digest. The future policy/controller layer
      // validates the user intent (for the P01 reasons: the TASK
      // revision, the budget grant and the model-profile replacements)
      // before dispatching this command; the reducer trusts that already
      // validated command and records no user intent payload. Recording
      // the response appends the content-free response to the open wait
      // record atomically, returns the run to active/running, and moves
      // the cursor to the declared action target without touching the
      // transition count, the transition budget, or any history. Ordinary
      // commands apply again afterwards under the existing successor
      // rules.
      const waitIndex = expectSafePositiveInteger(
        command.waitIndex,
        "wait_response_recorded wait index",
      );
      const expectedRequestSha256 = expectSha256(
        command.expectedRequestSha256,
        "wait_response_recorded expected request digest",
      );
      const actionId = expectSafeId(command.actionId, "wait_response_recorded action id");
      const responseSha256 = expectSha256(
        command.responseSha256,
        "wait_response_recorded response digest",
      );
      if (current.status === "active") {
        fail(current, "recording a wait response requires a waiting run; the run is active with no open wait record");
      }
      if (current.status !== "waiting") {
        fail(
          current,
          `the run is already finalized with status ${JSON.stringify(current.status)}; the terminal run status is immutable`,
        );
      }
      if (current.phase !== "waiting") {
        fail(current, `recording a wait response requires phase "waiting", got ${JSON.stringify(current.phase)}`);
      }
      const last = current.waits[current.waits.length - 1];
      if (last === undefined) {
        fail(current, "the run records no wait to respond to");
      }
      if (last.response !== undefined) {
        fail(current, `wait record ${last.index} already carries a response`);
      }
      if (waitIndex !== last.index) {
        fail(
          current,
          `wait_response_recorded targets wait index ${waitIndex}, but the open wait record is ${last.index}`,
        );
      }
      if (expectedRequestSha256 !== last.request_sha256) {
        fail(current, `the expected request digest does not match open wait record ${last.index}`);
      }
      const action = last.actions.find((candidate) => candidate.id === actionId);
      if (action === undefined) {
        fail(current, `action ${JSON.stringify(actionId)} is not declared in wait record ${last.index}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "a waiting run must not carry a reached terminal state");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "a waiting run must not carry published run outputs");
      }
      if (current.failure !== undefined) {
        fail(current, "a waiting run must not carry a failure reason");
      }
      // Schema v7: the durable intervention of a stage iteration must be
      // complete before the run leaves the wait. A stage iteration that
      // was open when the run entered the wait is closed by the
      // intervention (grant or replanning) — a response while it is still
      // open would route the cursor to a state whose next execution
      // cannot legally start.
      const openIteration = openIterationOf(current);
      if (openIteration !== undefined) {
        fail(
          current,
          `the open iteration ${openIteration.index} of generation ${openIteration.generation.index} is not closed; the durable intervention must close it before the response is recorded`,
        );
      }
      next.waits = [
        ...next.waits.slice(0, -1),
        {
          index: last.index,
          transition_count: last.transition_count,
          state_id: last.state_id,
          reason: last.reason,
          request_sha256: last.request_sha256,
          actions: last.actions.map((entry) => ({ id: entry.id, to: entry.to })),
          ...(last.intent !== undefined ? { intent: { intent_sha256: last.intent.intent_sha256 } } : {}),
          response: { action_id: actionId, response_sha256: responseSha256 },
        },
      ];
      next.status = "active";
      next.phase = "running";
      next.cursor = {
        current_state: action.to,
        transition_count: current.cursor.transition_count,
      };
      break;
    }
    case "terminal_reached": {
      if (current.phase !== "running") {
        fail(current, `recording the terminal state requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached and immutable");
      }
      const terminalStateId = expectSafeId(
        command.terminalStateId,
        "terminal_reached terminal state id",
      );
      if (command.terminalResult !== "success" && command.terminalResult !== "failed") {
        throw new PipelineV2StateError(
          `terminal_reached requires terminalResult "success" or "failed", got ${JSON.stringify(command.terminalResult)}`,
        );
      }
      if (terminalStateId !== current.cursor.current_state) {
        fail(
          current,
          `terminal state ${JSON.stringify(terminalStateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      for (const execution of current.executions) {
        if (!isSettledExecution(execution)) {
          fail(
            current,
            `recording the terminal state requires execution ${execution.index} to be finished, it has phase ${JSON.stringify(execution.phase)}`,
          );
        }
      }
      if (current.transitions.length !== current.executions.length) {
        fail(
          current,
          "recording the terminal state requires every execution's transition to be committed",
        );
      }
      next.terminal = { state_id: terminalStateId, result: command.terminalResult };
      break;
    }
    case "run_outputs_published": {
      if (current.phase !== "running") {
        fail(current, `publishing run outputs requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal === undefined) {
        fail(current, "publishing run outputs requires a reached terminal state");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "run outputs are already published and immutable");
      }
      if (!Array.isArray(command.outputs)) {
        throw new PipelineV2StateError("run_outputs_published requires an outputs array");
      }
      const seen = new Set<string>();
      const outputs = command.outputs.map((entry, index) => {
        const validated = validateRunOutput(entry, `run_outputs_published outputs[${index}]`);
        if (seen.has(validated.id)) {
          throw new PipelineV2StateError(
            `run_outputs_published declares id ${JSON.stringify(validated.id)} more than once`,
          );
        }
        seen.add(validated.id);
        return validated;
      });
      next.run_outputs = outputs;
      next.phase = "publishing_outputs";
      break;
    }
    case "run_succeeded": {
      if (current.terminal === undefined) {
        fail(current, "success requires a reached terminal state");
      }
      if (current.terminal.result !== "success") {
        fail(current, "success requires a terminal state with result success; a failed terminal cannot become success");
      }
      if (current.phase !== "publishing_outputs") {
        fail(current, `success requires phase "publishing_outputs", got ${JSON.stringify(current.phase)}`);
      }
      next.status = "success";
      next.phase = "finished";
      break;
    }
    case "run_failed": {
      const reason = expectEnum(command.reason, PIPELINE_V2_FAILURE_REASONS, "run_failed reason");
      if (reason === PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON) {
        throw new PipelineV2StateError(
          `run_failed cannot use the session cleanup failure reason ${JSON.stringify(PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON)}; a cleanup failure finalizes with run_cleanup_failed`,
        );
      }
      for (const execution of current.executions) {
        if (!isSettledExecution(execution)) {
          fail(
            current,
            `run failure requires the unfinished execution ${execution.index} to be recorded as failed first`,
          );
        }
      }
      if (current.executions.length > 0) {
        const last = current.executions[current.executions.length - 1]!;
        if (
          last.type === "agent" &&
          last.phase === "failed" &&
          hasFailedSessionCleanup(last)
        ) {
          fail(current, "a failed session cleanup finalizes with run_cleanup_failed");
        }
      }
      if (current.terminal !== undefined && current.terminal.result === "failed" && current.run_outputs !== undefined) {
        if (reason !== PIPELINE_V2_TERMINAL_FAILURE_REASON) {
          fail(
            current,
            `a failed terminal with published run_outputs finalizes with failure reason ${JSON.stringify(PIPELINE_V2_TERMINAL_FAILURE_REASON)}, got ${JSON.stringify(reason)}`,
          );
        }
      } else if (command.reason === PIPELINE_V2_TERMINAL_FAILURE_REASON) {
        fail(
          current,
          `failure reason ${JSON.stringify(PIPELINE_V2_TERMINAL_FAILURE_REASON)} requires a failed terminal with published run_outputs`,
        );
      }
      next.status = "failed";
      next.phase = "finished";
      next.failure = { reason };
      break;
    }
    case "run_cleanup_failed": {
      if (current.terminal !== undefined) {
        fail(current, "a run with a reached terminal state cannot end as a cleanup failure");
      }
      const last = current.executions[current.executions.length - 1];
      if (
        last === undefined ||
        last.type !== "agent" ||
        last.phase !== "failed" ||
        !hasFailedSessionCleanup(last)
      ) {
        fail(
          current,
          "a cleanup failure requires the last agent execution to have failed with an unconfirmed session cleanup",
        );
      }
      next.status = "cleanup_failed";
      next.phase = "finished";
      next.failure = { reason: PIPELINE_V2_SESSION_CLEANUP_FAILURE_REASON };
      break;
    }
    case "stage_generation_opened": {
      // Opens one stage generation (schema v7). The reducer checks the
      // self-sufficient structural form only: the clean active boundary,
      // no open generation, the cursor anchor, and the binding to the
      // last accepted plan revision digest. The compiled correspondence
      // of the stage/template pair is the controller's and the restore
      // verifier's responsibility — never this module's.
      const stageId = expectSafeId(command.stageId, "stage_generation_opened stage id");
      const templateId = expectSafeId(command.templateId, "stage_generation_opened template id");
      const planSha256 = expectSha256(command.planSha256, "stage_generation_opened plan digest");
      if (!isPositiveSafeInteger(command.stagePosition)) {
        throw new PipelineV2StateError(
          `stage_generation_opened requires a positive stage position, got ${JSON.stringify(command.stagePosition)}`,
        );
      }
      if (!isPositiveSafeInteger(command.initialBudget)) {
        throw new PipelineV2StateError(
          `stage_generation_opened requires a positive initial budget, got ${JSON.stringify(command.initialBudget)}`,
        );
      }
      if (!isNonNegativeSafeInteger(command.transitionCount)) {
        throw new PipelineV2StateError(
          `stage_generation_opened requires a non-negative transition anchor, got ${JSON.stringify(command.transitionCount)}`,
        );
      }
      if (current.phase !== "running") {
        fail(current, `opening a stage generation requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a stage generation cannot be opened");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "run outputs are already published; a stage generation cannot be opened");
      }
      if (current.failure !== undefined) {
        fail(current, "the run already carries a failure reason; a stage generation cannot be opened");
      }
      const lastWait = current.waits[current.waits.length - 1];
      if (lastWait !== undefined && lastWait.response === undefined) {
        fail(current, `wait record ${lastWait.index} is still open; a stage generation cannot be opened`);
      }
      const openGeneration = openGenerationOf(current);
      if (openGeneration !== undefined) {
        fail(current, `generation ${openGeneration.index} is still open; a new stage generation requires it to be closed first`);
      }
      if (command.transitionCount !== current.cursor.transition_count) {
        fail(
          current,
          `the generation anchor ${command.transitionCount} does not match the cursor transition count ${current.cursor.transition_count}`,
        );
      }
      requireNoInFlightExecution(current, "opening a stage generation");
      const lastPlan = current.plan_revisions[current.plan_revisions.length - 1];
      if (lastPlan === undefined || lastPlan.sha256 !== planSha256) {
        fail(current, `the generation binds plan digest ${JSON.stringify(planSha256)}, which is not the last accepted plan revision`);
      }
      next.generations = [
        ...next.generations,
        {
          index: current.generations.length + 1,
          stage_id: stageId,
          stage_position: command.stagePosition,
          template_id: templateId,
          plan_sha256: planSha256,
          initial_budget: command.initialBudget,
          opened_transition_count: command.transitionCount,
          iteration_count: 0,
          iterations: [],
        },
      ];
      break;
    }
    case "stage_iteration_opened": {
      // Opens one iteration inside the open stage generation. The
      // effective iteration budget is derived here from the immutable
      // initial budget plus the generation's recorded grants, with an
      // overflow guard; a grant can never retroactively make an already
      // rejected iteration valid.
      if (!isPositiveSafeInteger(command.generationIndex)) {
        throw new PipelineV2StateError(
          `stage_iteration_opened requires a positive generation index, got ${JSON.stringify(command.generationIndex)}`,
        );
      }
      if (!isPositiveSafeInteger(command.iterationIndex)) {
        throw new PipelineV2StateError(
          `stage_iteration_opened requires a positive iteration index, got ${JSON.stringify(command.iterationIndex)}`,
        );
      }
      if (!isNonNegativeSafeInteger(command.transitionCount)) {
        throw new PipelineV2StateError(
          `stage_iteration_opened requires a non-negative transition anchor, got ${JSON.stringify(command.transitionCount)}`,
        );
      }
      if (current.phase !== "running") {
        fail(current, `opening a stage iteration requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a stage iteration cannot be opened");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "run outputs are already published; a stage iteration cannot be opened");
      }
      if (current.failure !== undefined) {
        fail(current, "the run already carries a failure reason; a stage iteration cannot be opened");
      }
      const lastWait = current.waits[current.waits.length - 1];
      if (lastWait !== undefined && lastWait.response === undefined) {
        fail(current, `wait record ${lastWait.index} is still open; a stage iteration cannot be opened`);
      }
      const openGeneration = openGenerationOf(current);
      if (openGeneration === undefined) {
        fail(current, "opening a stage iteration requires an open generation");
      }
      if (openGeneration !== undefined && openGeneration.index !== command.generationIndex) {
        fail(
          current,
          `stage_iteration_opened names generation ${command.generationIndex}, but the open generation is ${openGeneration.index}`,
        );
      }
      const generation = next.generations[command.generationIndex - 1] as PipelineV2StageGenerationRecord;
      if (generation.open_iteration !== undefined) {
        fail(
          current,
          `generation ${generation.index} already has an open iteration; close it before opening the next one`,
        );
      }
      if (command.iterationIndex !== generation.iteration_count + 1) {
        fail(
          current,
          `the iteration index ${command.iterationIndex} must be exactly the next iteration of generation ${generation.index} (${generation.iteration_count + 1})`,
        );
      }
      if (command.transitionCount !== current.cursor.transition_count) {
        fail(
          current,
          `the iteration anchor ${command.transitionCount} does not match the cursor transition count ${current.cursor.transition_count}`,
        );
      }
      requireNoInFlightExecution(current, "opening a stage iteration");
      let grantsSum = 0;
      for (const grant of current.grants) {
        if (grant.generation_index === generation.index) {
          grantsSum += grant.additional_iterations;
          if (!Number.isSafeInteger(grantsSum)) {
            fail(
              current,
              `the effective iteration budget of generation ${generation.index} is unrepresentable`,
            );
          }
        }
      }
      const effectiveBudget = generation.initial_budget + grantsSum;
      if (!Number.isSafeInteger(effectiveBudget)) {
        fail(current, `the effective iteration budget of generation ${generation.index} is unrepresentable`);
      }
      if (generation.iteration_count + 1 > effectiveBudget) {
        fail(
          current,
          `opening iteration ${command.iterationIndex} of generation ${generation.index} exceeds its effective iteration budget ${effectiveBudget} (initial budget ${generation.initial_budget} plus recorded grants)`,
        );
      }
      const iteration: PipelineV2StageIterationRecord = {
        index: command.iterationIndex,
        opened_transition_count: command.transitionCount,
      };
      const updated: PipelineV2StageGenerationRecord = {
        ...generation,
        iteration_count: generation.iteration_count + 1,
        iterations: [...generation.iterations, iteration],
        open_iteration: { index: iteration.index, opened_transition_count: iteration.opened_transition_count },
      };
      next.generations = [
        ...next.generations.slice(0, -1),
        updated,
      ];
      break;
    }
    case "stage_iteration_closed": {
      // Closes the open iteration of the open generation. The wait-bound
      // closures (grant, replanned) run inside the open wait and require
      // its accepted intent plus the matching durable intervention record;
      // the ordinary closures run on the active boundary without a wait.
      if (!isPositiveSafeInteger(command.generationIndex)) {
        throw new PipelineV2StateError(
          `stage_iteration_closed requires a positive generation index, got ${JSON.stringify(command.generationIndex)}`,
        );
      }
      if (!isPositiveSafeInteger(command.iterationIndex)) {
        throw new PipelineV2StateError(
          `stage_iteration_closed requires a positive iteration index, got ${JSON.stringify(command.iterationIndex)}`,
        );
      }
      const by = expectEnum(
        command.by,
        PIPELINE_V2_STAGE_ITERATION_CLOSE_REASONS,
        "stage_iteration_closed by",
      );
      const waitBound = by === "grant" || by === "replanned";
      if (waitBound !== (command.waitIndex !== undefined)) {
        throw new PipelineV2StateError(
          `stage_iteration_closed with ${JSON.stringify(by)} ${waitBound ? "requires" : "must not carry"} a wait index`,
        );
      }
      if (waitBound) {
        if (current.phase !== "waiting") {
          fail(current, `a wait-bound iteration closure requires phase "waiting", got ${JSON.stringify(current.phase)}`);
        }
      } else if (current.phase !== "running") {
        fail(current, `closing a stage iteration requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a stage iteration cannot be closed");
      }
      const openIteration = openIterationOf(current);
      if (openIteration === undefined) {
        fail(current, "closing a stage iteration requires an open iteration");
      }
      if (openIteration.generation.index !== command.generationIndex) {
        fail(
          current,
          `stage_iteration_closed names generation ${command.generationIndex}, but the open iteration belongs to generation ${openIteration.generation.index}`,
        );
      }
      if (openIteration.index !== command.iterationIndex) {
        fail(
          current,
          `stage_iteration_closed names iteration ${command.iterationIndex}, but the open iteration is ${openIteration.index}`,
        );
      }
      const waitIndex = command.waitIndex;
      let waitIntentSha256: string | undefined;
      if (waitBound && waitIndex !== undefined) {
        if (current.status !== "waiting") {
          fail(current, "a wait-bound iteration closure requires a waiting run");
        }
        const wait = current.waits[current.waits.length - 1];
        if (wait === undefined || wait.response !== undefined) {
          fail(current, "a wait-bound iteration closure requires the open wait record");
        }
        if (waitIndex !== wait.index) {
          fail(
            current,
            `stage_iteration_closed targets wait index ${waitIndex}, but the open wait record is ${wait.index}`,
          );
        }
        if (wait.intent === undefined) {
          fail(current, `wait record ${wait.index} carries no accepted intent; the closure requires one`);
        } else {
          waitIntentSha256 = wait.intent.intent_sha256;
        }
        if (by === "grant") {
          const grant = current.grants.find(
            (candidate) =>
              candidate.generation_index === openIteration.generation.index &&
              candidate.wait_index === wait.index &&
              (waitIntentSha256 === undefined || candidate.intent_sha256 === waitIntentSha256),
          );
          if (grant === undefined) {
            fail(
              current,
              `the iteration closure with ${JSON.stringify(by)} requires the recorded grant of wait ${wait.index}`,
            );
          }
        } else {
          const task = current.task_revisions.some((candidate) => candidate.wait_index === wait.index);
          if (!task) {
            fail(
              current,
              `the iteration closure with ${JSON.stringify(by)} requires an accepted task revision of wait ${wait.index}`,
            );
          }
        }
      }
      const generation = next.generations[openIteration.generation.index - 1] as PipelineV2StageGenerationRecord;
      const lastIteration = generation.iterations[generation.iterations.length - 1]!;
      const closedRecord: PipelineV2StageIterationRecord = {
        index: openIteration.index,
        opened_transition_count: lastIteration.opened_transition_count,
        closed: {
          by,
          closed_transition_count: current.cursor.transition_count,
          ...(waitIndex !== undefined ? { wait_index: waitIndex } : {}),
        },
      };
      const iterations = [...generation.iterations.slice(0, -1), closedRecord];
      const { open_iteration: _closedProjection, ...rest } = generation;
      const updated: PipelineV2StageGenerationRecord = { ...rest, iterations };
      next.generations = [...next.generations.slice(0, -1), updated];
      break;
    }
    case "stage_generation_closed": {
      // Closes the open stage generation. Requires no open iteration; the
      // closed generation is never rewritten.
      if (!isPositiveSafeInteger(command.generationIndex)) {
        throw new PipelineV2StateError(
          `stage_generation_closed requires a positive generation index, got ${JSON.stringify(command.generationIndex)}`,
        );
      }
      const by = expectEnum(
        command.by,
        PIPELINE_V2_STAGE_GENERATION_CLOSE_REASONS,
        "stage_generation_closed by",
      );
      if (current.phase !== "running") {
        fail(current, `closing a stage generation requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a stage generation cannot be closed");
      }
      const openGeneration = openGenerationOf(current);
      if (openGeneration === undefined || openGeneration.index !== command.generationIndex) {
        fail(
          current,
          `stage_generation_closed names generation ${command.generationIndex}, but the open generation is ${openGeneration?.index ?? "none"}`,
        );
      }
      if (openIterationOf(current) !== undefined) {
        fail(current, `generation ${openGeneration.index} still has an open iteration; close it first`);
      }
      if (openGeneration.iterations.length === 0) {
        fail(
          current,
          `generation ${openGeneration.index} has no iterations to close; open and close one iteration first`,
        );
      }
      const generation = next.generations[openGeneration.index - 1] as PipelineV2StageGenerationRecord;
      const updated: PipelineV2StageGenerationRecord = {
        ...generation,
        closed: { by, closed_transition_count: current.cursor.transition_count },
      };
      next.generations = [...next.generations.slice(0, -1), updated];
      break;
    }
    case "plan_intent_accepted": {
      // Records one accepted user wait intent inside the open wait
      // record. The reducer accepts the digest but never interprets the
      // intent kind; the controller owns the meaning. One intent per
      // wait: an exact digest repeat is a no-op that grows no revision,
      // a different digest is rejected.
      const waitIndex = expectSafePositiveInteger(command.waitIndex, "plan_intent_accepted wait index");
      const intentSha256 = expectSha256(command.intentSha256, "plan_intent_accepted intent digest");
      if (current.status !== "waiting") {
        fail(current, "accepting a wait intent requires a waiting run");
      }
      const wait = current.waits[current.waits.length - 1];
      if (wait === undefined || wait.response !== undefined) {
        fail(current, "accepting a wait intent requires the open wait record");
      }
      if (waitIndex !== wait.index) {
        fail(
          current,
          `plan_intent_accepted targets wait index ${waitIndex}, but the open wait record is ${wait.index}`,
        );
      }
      if (wait.intent !== undefined) {
        if (wait.intent.intent_sha256 !== intentSha256) {
          fail(
            current,
            `wait record ${wait.index} already accepted a different intent; one intent belongs to one wait`,
          );
        }
        return current;
      }
      next.waits = [
        ...next.waits.slice(0, -1),
        {
          ...wait,
          actions: wait.actions.map((entry) => ({ id: entry.id, to: entry.to })),
          intent: { intent_sha256: intentSha256 },
        },
      ];
      break;
    }
    case "task_revision_accepted": {
      // Records one accepted task revision in the append-only ledger.
      // Revision 1 tasks are accepted during the planning flow on the
      // active boundary without wait links; revisions above 1 are
      // accepted inside the open wait and carry its intent link. The
      // per-task revision chain is enforced against the durable records.
      const taskId = expectSafeId(command.taskId, "task_revision_accepted task id");
      if (!isPositiveSafeInteger(command.revision)) {
        throw new PipelineV2StateError(
          `task_revision_accepted requires a positive revision, got ${JSON.stringify(command.revision)}`,
        );
      }
      const taskSha256 = expectSha256(command.taskSha256, "task_revision_accepted task digest");
      const revision = command.revision;
      const waitBound = revision > 1;
      if (waitBound !== (command.waitIndex !== undefined)) {
        throw new PipelineV2StateError(
          `task_revision_accepted of revision ${revision} ${waitBound ? "requires" : "must not carry"} the wait and intent links`,
        );
      }
      const previousRecord = [...current.task_revisions]
        .reverse()
        .find((candidate) => candidate.task_id === taskId);
      if (revision === 1) {
        if (previousRecord !== undefined) {
          fail(
            current,
            `task revision 1 of ${JSON.stringify(taskId)} already exists (revision ${previousRecord.revision} is recorded)`,
          );
        }
        if (current.phase !== "running") {
          fail(current, `accepting a revision-1 task revision requires phase "running", got ${JSON.stringify(current.phase)}`);
        }
        if (current.terminal !== undefined) {
          fail(current, "the terminal state is already reached; a task revision cannot be accepted");
        }
        if (current.run_outputs !== undefined) {
          fail(current, "run outputs are already published; a task revision cannot be accepted");
        }
        if (current.failure !== undefined) {
          fail(current, "the run already carries a failure reason; a task revision cannot be accepted");
        }
        const lastWait = current.waits[current.waits.length - 1];
        if (lastWait !== undefined && lastWait.response === undefined) {
          fail(current, `wait record ${lastWait.index} is still open; a task revision cannot be accepted`);
        }
      } else {
        if (current.status !== "waiting") {
          fail(current, "accepting a user-response task revision requires a waiting run");
        }
        const wait = current.waits[current.waits.length - 1];
        if (wait === undefined || wait.response !== undefined) {
          fail(current, "accepting a user-response task revision requires the open wait record");
        }
        if (command.waitIndex !== wait.index) {
          fail(
            current,
            `task_revision_accepted targets wait index ${command.waitIndex}, but the open wait record is ${wait.index}`,
          );
        }
        if (wait.intent === undefined || wait.intent.intent_sha256 !== command.intentSha256) {
          fail(current, `the task revision references an intent that open wait record ${wait.index} has not accepted`);
        }
        if (previousRecord === undefined) {
          fail(
            current,
            `task revision ${revision} of ${JSON.stringify(taskId)} has no recorded predecessor`,
          );
        } else if (previousRecord.revision !== revision - 1) {
          fail(
            current,
            `task revision ${revision} of ${JSON.stringify(taskId)} must follow revision ${previousRecord.revision + 1}`,
          );
        }
      }
      const record: PipelineV2TaskRevisionState = {
        index: current.task_revisions.length + 1,
        task_id: taskId,
        revision,
        sha256: taskSha256,
        previous_sha256: previousRecord === undefined ? null : previousRecord.sha256,
        ...(waitBound && command.waitIndex !== undefined && command.intentSha256 !== undefined
          ? { wait_index: command.waitIndex, intent_sha256: command.intentSha256 }
          : {}),
      };
      next.task_revisions = [...next.task_revisions, record];
      break;
    }
    case "plan_revision_accepted": {
      // Records one accepted plan revision in the append-only ledger. The
      // acceptance runs on the active boundary and names, as its origin,
      // exactly the last settled-but-unbound execution, which must carry
      // the planning role; the plan body lives in the filesystem manifest
      // and is never recorded here. The revision chain is enforced
      // against the durable ledger.
      if (!isPositiveSafeInteger(command.planRevision)) {
        throw new PipelineV2StateError(
          `plan_revision_accepted requires a positive plan revision, got ${JSON.stringify(command.planRevision)}`,
        );
      }
      const planSha256 = expectSha256(command.planSha256, "plan_revision_accepted plan digest");
      if (!isPositiveSafeInteger(command.originExecution)) {
        throw new PipelineV2StateError(
          `plan_revision_accepted requires a positive origin execution index, got ${JSON.stringify(command.originExecution)}`,
        );
      }
      if (current.phase !== "running") {
        fail(current, `accepting a plan revision requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; a plan revision cannot be accepted");
      }
      if (current.run_outputs !== undefined) {
        fail(current, "run outputs are already published; a plan revision cannot be accepted");
      }
      if (current.failure !== undefined) {
        fail(current, "the run already carries a failure reason; a plan revision cannot be accepted");
      }
      const lastWait = current.waits[current.waits.length - 1];
      if (lastWait !== undefined && lastWait.response === undefined) {
        fail(current, `wait record ${lastWait.index} is still open; a plan revision cannot be accepted`);
      }
      if (openIterationOf(current) !== undefined) {
        fail(current, "a plan revision cannot be accepted while a stage iteration is open");
      }
      if (current.executions.length !== current.transitions.length + 1) {
        fail(
          current,
          `plan acceptance requires exactly one settled-but-unbound execution, got ${current.executions.length} executions and ${current.transitions.length} committed transitions`,
        );
      }
      const origin = current.executions[current.executions.length - 1];
      if (origin === undefined || origin.index !== command.originExecution) {
        fail(
          current,
          `the plan's origin execution ${command.originExecution} is not the last execution of the run`,
        );
      }
      if (origin.execution_role !== "planning") {
        fail(
          current,
          `the plan's origin execution ${origin.index} carries the ${JSON.stringify(origin.execution_role)} role; plan acceptance requires the "planning" role`,
        );
      }
      if (origin.type !== "agent" || origin.phase !== "cleanup_completed") {
        fail(
          current,
          `the plan's origin execution ${origin.index} has phase ${JSON.stringify(origin.phase)}; plan acceptance requires the settled phase "cleanup_completed"`,
        );
      }
      if (command.planRevision !== current.plan_revisions.length + 1) {
        fail(
          current,
          `the plan revision ${command.planRevision} must be exactly the next revision of the ledger (${current.plan_revisions.length + 1})`,
        );
      }
      const previousRecord = current.plan_revisions[current.plan_revisions.length - 1];
      next.plan_revisions = [
        ...next.plan_revisions,
        {
          index: current.plan_revisions.length + 1,
          revision: command.planRevision,
          sha256: planSha256,
          previous_sha256: previousRecord === undefined ? null : previousRecord.sha256,
          origin_execution: command.originExecution,
        },
      ];
      break;
    }
    case "iteration_grant_recorded": {
      // Appends one iteration grant to the append-only ledger. The grant
      // references the exact open generation, the open wait and its
      // accepted intent; it never rewrites the generation record. A
      // repeated grant for the same wait and intent is rejected — the
      // controller recognizes an already durable grant from the snapshot
      // and does not re-dispatch.
      if (!isPositiveSafeInteger(command.generationIndex)) {
        throw new PipelineV2StateError(
          `iteration_grant_recorded requires a positive generation index, got ${JSON.stringify(command.generationIndex)}`,
        );
      }
      if (!isPositiveSafeInteger(command.waitIndex)) {
        throw new PipelineV2StateError(
          `iteration_grant_recorded requires a positive wait index, got ${JSON.stringify(command.waitIndex)}`,
        );
      }
      const intentSha256 = expectSha256(command.intentSha256, "iteration_grant_recorded intent digest");
      if (!isPositiveSafeInteger(command.additionalIterations)) {
        throw new PipelineV2StateError(
          `iteration_grant_recorded requires a positive additional iteration count, got ${JSON.stringify(command.additionalIterations)}`,
        );
      }
      if (current.status !== "waiting") {
        fail(current, "recording an iteration grant requires a waiting run");
      }
      const wait = current.waits[current.waits.length - 1];
      if (wait === undefined || wait.response !== undefined) {
        fail(current, "recording an iteration grant requires the open wait record");
      }
      if (command.waitIndex !== wait.index) {
        fail(
          current,
          `iteration_grant_recorded targets wait index ${command.waitIndex}, but the open wait record is ${wait.index}`,
        );
      }
      if (wait.intent === undefined || wait.intent.intent_sha256 !== intentSha256) {
        fail(current, `the grant references an intent that open wait record ${wait.index} has not accepted`);
      }
      const openGeneration = openGenerationOf(current);
      if (openGeneration === undefined || openGeneration.index !== command.generationIndex) {
        fail(
          current,
          `iteration_grant_recorded names generation ${command.generationIndex}, but the open generation is ${openGeneration?.index ?? "none"}`,
        );
      }
      const existing = current.grants.find(
        (candidate) => candidate.generation_index === openGeneration.index && candidate.wait_index === wait.index,
      );
      if (existing !== undefined) {
        fail(
          current,
          `wait ${wait.index} already carries a recorded grant for generation ${openGeneration.index}`,
        );
      }
      next.grants = [
        ...next.grants,
        {
          index: current.grants.length + 1,
          generation_index: openGeneration.index,
          wait_index: wait.index,
          intent_sha256: intentSha256,
          additional_iterations: command.additionalIterations,
        },
      ];
      break;
    }
    default: {
      const exhaustive: never = command;
      throw new PipelineV2StateError(`unknown pipeline v2 run command: ${JSON.stringify(exhaustive)}`);
    }
  }
  return deepFreeze(next);
}
