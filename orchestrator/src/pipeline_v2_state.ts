/**
 * Durable run state for pipeline schema v2 (state schema version 5).
 *
 * Pure substrate: a versioned state document, an exact-field loader, and a
 * pure command reducer. Nothing here touches the filesystem, docker-helper,
 * Sessions, or the production runner; `pipeline_state.ts` stays the
 * production run-state contract of pipeline schema v1 and is not wired to
 * any of this. There are no migrations between state versions in either
 * direction.
 *
 * Schema version 5 adds the durable user-wait: `status: "waiting"`,
 * `phase: "waiting"` and one optional top-level `wait` record
 * (`PipelineV2WaitRecord`). A waiting run is a temporarily immobile
 * durable state, not a terminal result: the graph cursor sits at
 * `wait.state_id` with every execution settled and every transition
 * committed, and this schema version provides no successor — a later
 * increment adds the explicit user-response command that consumes the
 * wait. The wait record is content-free: it names the waiting state, a
 * normalized policy reason (for example the P01 reason
 * `stage_iteration_limit_exhausted`), the SHA-256 of the future
 * orchestrator-owned user request manifest, and the declared wait actions
 * (`{id, to}` in declaration order; several action ids may target the
 * same state). It never carries paths, evidence bodies, TASK/PLAN
 * content, facts, prompts, worker output, environment values, profiles
 * or credentials.
 *
 * Deliberate difference from state schema v2: there is NO event journal.
 * `executions`, `transitions`, `terminal` and `run_outputs` are the only
 * authoritative journal of the run, and the loader re-derives every
 * invariant from those records in both directions (no second event model,
 * no bidirectional event/record coherence layer). Audit and observation
 * streams are a separate later layer.
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

export const PIPELINE_V2_RUN_STATE_SCHEMA_VERSION = 5;

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
 * Content-free durable record of one user wait (schema v5). The record
 * names the waiting graph state, a normalized policy reason (the P01
 * reason `stage_iteration_limit_exhausted` belongs to the policy/context
 * stage, never to the engine transition budget), the SHA-256 of the
 * orchestrator-owned user request manifest that a later increment
 * publishes on the data plane, and the declared actions. No user response
 * body, evidence, TASK/PLAN content, facts, prompts, paths, environment
 * values, profiles or credentials ever enter the document.
 */
export interface PipelineV2WaitRecord {
  readonly state_id: string;
  readonly reason: string;
  readonly request_sha256: string;
  readonly actions: readonly PipelineV2WaitAction[];
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
  schema_version: 5;
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
  terminal?: PipelineV2TerminalState;
  run_outputs?: PipelineV2RunOutputState[];
  failure?: PipelineV2FailureState;
  /** Present if and only if status and phase are both "waiting". */
  wait?: PipelineV2WaitRecord;
}

export type PipelineV2RunCommand =
  | {
      kind: "create_run";
      runId: string;
      pipeline: PipelineV2RunPipelineIdentity;
      inputs: readonly PipelineV2RunInputState[];
    }
  | { kind: "start_agent_execution"; stateId: string; profile: string }
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
  | { kind: "start_decision_execution"; stateId: string; inputDigest: string }
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
  | { kind: "terminal_reached"; terminalStateId: string; terminalResult: "success" | "failed" }
  | { kind: "run_outputs_published"; outputs: readonly PipelineV2RunOutputState[] }
  | { kind: "run_succeeded" }
  | { kind: "run_failed"; reason: PipelineV2FailureReason }
  | { kind: "run_cleanup_failed" };

export class PipelineV2StateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2StateError";
  }
}

const ISO_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
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

function isSafeId(value: unknown): value is string {
  return isNonEmptyString(value) && SAFE_ID_PATTERN.test(value) && !value.includes("..");
}

function isSha256Hex(value: unknown): value is string {
  return typeof value === "string" && SHA256_PATTERN.test(value);
}

function isIsoTimestamp(value: unknown): value is string {
  return typeof value === "string" && ISO_TIMESTAMP_PATTERN.test(value);
}

function isSafePositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isSafeNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
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
  if (!isSafeId(value)) {
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
  if (!isSha256Hex(value)) {
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
  if (!isSafePositiveInteger(value)) {
    throw new PipelineV2StateError(
      `${what} must be a positive safe integer, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectSafeNonNegativeInteger(value: unknown, what: string): number {
  if (!isSafeNonNegativeInteger(value)) {
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
      "pipeline v2 run state has schema_version 1, which is unsupported by this orchestrator (schema version 5 is the supported contract; no v1 migration exists)",
    );
  }
  if (version === 2) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 2, which is the production pipeline v1 run-state contract, not a pipeline v2 run state (schema version 5 is the supported contract; no v2 migration exists)",
    );
  }
  if (version === 3) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 3, which is unsupported by this orchestrator (schema version 5 is the supported contract; no v3 migration exists)",
    );
  }
  if (version === 4) {
    throw new PipelineV2StateError(
      "pipeline v2 run state has schema_version 4, which is unsupported by this orchestrator (schema version 5 is the supported contract; no v4 migration exists)",
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
    ["index", "type", "state_id", "attempt", "profile", "phase"],
    ["execution_session_id", "tool_session_id", "session_cleanup", "outputs", "failure_reason"],
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
    ["index", "type", "state_id", "phase", "input_digest"],
    ["result", "failure_reason"],
  );
  if (obj.type !== "decision") {
    throw new PipelineV2StateError(`${what}.type must be "decision", got ${JSON.stringify(obj.type)}`);
  }
  const phase = expectEnum(obj.phase, PIPELINE_V2_DECISION_EXECUTION_PHASES, `${what}.phase`);
  const execution: PipelineV2DecisionExecutionState = {
    index: expectSafePositiveInteger(obj.index, `${what}.index`),
    type: "decision",
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    phase,
    input_digest: expectSha256(obj.input_digest, `${what}.input_digest`),
  };
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
 * Exact-field validation of the optional top-level wait record: safe ids
 * for the waiting state, the normalized reason and every action id/target,
 * a lowercase hex SHA-256 for the request manifest digest, a non-empty
 * action list with unique ids, and the declaration order preserved.
 */
function validateWaitRecord(value: unknown, what: string): PipelineV2WaitRecord {
  const obj = expectExactObject(value, what, ["state_id", "reason", "request_sha256", "actions"]);
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
  return {
    state_id: expectSafeId(obj.state_id, `${what}.state_id`),
    reason: expectSafeId(obj.reason, `${what}.reason`),
    request_sha256: expectSha256(obj.request_sha256, `${what}.request_sha256`),
    actions,
  };
}

/**
 * Validates one pipeline v2 run state document and returns an independent,
 * deep-frozen snapshot. Every invariant the reducer enforces is re-checked
 * here against the authoritative records alone: there is no event journal
 * to reconcile against, so phantom executions, transitions, terminals and
 * run outputs are impossible by construction.
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
    ],
    ["terminal", "run_outputs", "failure", "wait"],
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
  const wait = obj.wait === undefined ? undefined : validateWaitRecord(obj.wait, "pipeline v2 run state wait");

  // The waiting status, the "waiting" phase and the wait record are one
  // indivisible triple: each implies the other two, and no other status or
  // phase may carry a wait record.
  if (status === "waiting") {
    if (phase !== "waiting" || wait === undefined) {
      throw new PipelineV2StateError('run status "waiting" requires phase "waiting" and a wait record');
    }
  } else if (phase === "waiting") {
    throw new PipelineV2StateError('run phase "waiting" requires run status "waiting"');
  }
  if (wait !== undefined && status !== "waiting") {
    throw new PipelineV2StateError('a wait record requires run status "waiting"');
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
  // the completed execution whose state matches the transition's origin
  for (let index = 0; index < transitions.length; index++) {
    const transition = transitions[index]!;
    if (index === 0) {
      if (transition.from !== pipeline.entry_state) {
        throw new PipelineV2StateError(
          `the first committed transition starts at ${JSON.stringify(transition.from)}, expected the entry state ${JSON.stringify(pipeline.entry_state)}`,
        );
      }
    } else if (transition.from !== transitions[index - 1]!.to) {
      throw new PipelineV2StateError(
        `transition at position ${index} starts at ${JSON.stringify(transition.from)}, expected the previous transition target ${JSON.stringify(transitions[index - 1]!.to)}`,
      );
    }
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
  const expectedCursor =
    transitions.length > 0 ? transitions[transitions.length - 1]!.to : pipeline.entry_state;
  if (cursor.current_state !== expectedCursor) {
    throw new PipelineV2StateError(
      `cursor.current_state ${JSON.stringify(cursor.current_state)} does not match the expected cursor ${JSON.stringify(expectedCursor)}`,
    );
  }
  if (transitions.length < executions.length - 1) {
    const uncommitted = executions[transitions.length]!;
    throw new PipelineV2StateError(
      `execution ${uncommitted.index} has no committed transition; a new execution starts only after the previous execution's transition is committed`,
    );
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
    // transition bound to its execution.
    if (wait === undefined) {
      throw new PipelineV2StateError('run status "waiting" requires phase "waiting" and a wait record');
    }
    if (terminal !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry a reached terminal state");
    }
    if (runOutputs !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry published run_outputs");
    }
    if (failure !== undefined) {
      throw new PipelineV2StateError("a waiting run must not carry a failure reason");
    }
    if (wait.state_id !== cursor.current_state) {
      throw new PipelineV2StateError(
        `the wait record names state ${JSON.stringify(wait.state_id)}, which does not match the cursor ${JSON.stringify(cursor.current_state)}`,
      );
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
  if (wait !== undefined) {
    state.wait = wait;
  }
  return deepFreeze(state);
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
  return {
    state_id: record.state_id,
    reason: record.reason,
    request_sha256: record.request_sha256,
    actions: record.actions.map((action) => ({ id: action.id, to: action.to })),
  };
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
  if (state.wait !== undefined) {
    clone.wait = cloneWaitRecord(state.wait);
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
    if (!isSha256Hex(pipeline.execution_snapshot_sha256)) {
      throw new PipelineV2StateError("create_run requires a lowercase hex execution snapshot digest");
    }
    if (!isSafeId(pipeline.entry_state)) {
      throw new PipelineV2StateError("create_run requires a safe entry state id");
    }
    if (!isSafePositiveInteger(pipeline.max_transitions)) {
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
    if (!isSafeId(command.runId)) {
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
    };
    return deepFreeze(state);
  }

  if (current === null) {
    throw new PipelineV2StateError(`command ${command.kind} rejected: no pipeline v2 run state exists yet`);
  }
  if (current.status !== "active") {
    if (current.status === "waiting") {
      fail(
        current,
        "the run is waiting for an explicit user response; no command of this schema version advances a waiting run",
      );
    }
    fail(
      current,
      `the run is already finalized with status ${JSON.stringify(current.status)}; the terminal run status is immutable`,
    );
  }

  const next = cloneState(current);
  const at = now.toISOString();
  next.updated_at = at;
  next.revision = current.revision + 1;

  switch (command.kind) {
    case "start_agent_execution": {
      if (!isSafeId(command.stateId)) {
        throw new PipelineV2StateError(
          `start_agent_execution requires a safe agent state id, got ${JSON.stringify(command.stateId)}`,
        );
      }
      if (!isNonEmptyString(command.profile)) {
        throw new PipelineV2StateError("start_agent_execution requires a non-empty profile name");
      }
      requireStartableCursor(current, command.stateId);
      const execution: PipelineV2AgentExecutionState = {
        index: current.executions.length + 1,
        type: "agent",
        state_id: command.stateId,
        attempt: 1,
        profile: command.profile,
        phase: "started",
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
      if (!isSafeId(command.stateId)) {
        throw new PipelineV2StateError(
          `start_decision_execution requires a safe decision state id, got ${JSON.stringify(command.stateId)}`,
        );
      }
      if (!isSha256Hex(command.inputDigest)) {
        throw new PipelineV2StateError(
          `start_decision_execution requires a lowercase hex input digest, got ${JSON.stringify(command.inputDigest)}`,
        );
      }
      requireStartableCursor(current, command.stateId);
      const execution: PipelineV2DecisionExecutionState = {
        index: current.executions.length + 1,
        type: "decision",
        state_id: command.stateId,
        phase: "evaluating",
        input_digest: command.inputDigest,
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
      if (!isSafePositiveInteger(command.executionIndex)) {
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
      // Enters the durable user wait (schema v5). The wait is a policy /
      // context-stage decision of the caller (for example the P01 reason
      // "stage_iteration_limit_exhausted"); it never consumes or touches
      // the immutable engine transition budget and it records no
      // transition of its own. Waiting is not a terminal result: this
      // schema version offers no successor, so the run stays immobile
      // until a later increment adds the explicit user-response command.
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
      if (current.wait !== undefined) {
        fail(current, "the run is already waiting; the wait record is immutable");
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
      next.wait = {
        state_id: stateId,
        reason,
        request_sha256: requestSha256,
        actions,
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
    default: {
      const exhaustive: never = command;
      throw new PipelineV2StateError(`unknown pipeline v2 run command: ${JSON.stringify(exhaustive)}`);
    }
  }
  return deepFreeze(next);
}
