import type { TransitionStep } from "./pipeline_engine.ts";

/**
 * Versioned machine-owned contract for the durable pipeline run state of
 * `agent-smoke`. The JSON document is the source of truth for an accepted
 * pipeline, the execution cursor, the ordered activations (one child Session
 * per agent-state activation), committed transitions, and the final result.
 * It deliberately contains no credentials, no session bearer, no environment
 * values, no prompt or input bodies, no OpenCode configuration, no raw worker
 * output, and no result summary text.
 *
 * Schema version 2 is the multi-state contract: it replaces the v1
 * single-`protected_input`/`session_id`/`attempt` shape with a list of
 * protected inputs and an ordered activation list. Version 1 documents are
 * unsupported and rejected by the loader; there is no v1 migration.
 *
 * All mutations go through the pure reducer (`reducePipelineRunCommand`):
 * no filesystem access, no randomness, timestamps come from an injected
 * clock, and the input snapshot is never mutated.
 */

export const PIPELINE_RUN_STATE_SCHEMA_VERSION = 2;

export const PIPELINE_RUN_STATUSES = [
  "active",
  "success",
  "failed",
  "cleanup_failed",
] as const;
export type PipelineRunStatus = (typeof PIPELINE_RUN_STATUSES)[number];

export const PIPELINE_RUN_PHASES = [
  "validating",
  "running",
  "finalizing",
  "finished",
] as const;
export type PipelineRunPhase = (typeof PIPELINE_RUN_PHASES)[number];

export const PIPELINE_RUN_ACTIVATION_PHASES = [
  "creating_session",
  "session_created",
  "agent_running",
  "result_accepted",
  "session_cleanup_completed",
  "failed",
] as const;
export type PipelineRunActivationPhase = (typeof PIPELINE_RUN_ACTIVATION_PHASES)[number];

/** Activation phases that are finished: no further activation mutation. */
export const FINISHED_ACTIVATION_PHASES: readonly PipelineRunActivationPhase[] = [
  "session_cleanup_completed",
  "failed",
];

export const PIPELINE_RUN_FAILURE_REASONS = [
  "internal_error",
  "worker_failed",
  "worker_timeout",
  "agent_result_invalid",
  "protected_input_modified",
  "control_path_invalid",
  "runtime_input_missing",
  "unknown_outcome",
  "invalid_outcome",
  "transition_budget_exhausted",
  "invalid_graph",
  "missing_state",
  "execution_failed",
  "state_persist_failed",
  "session_cleanup_failed",
  "signal_sigint",
  "signal_sigterm",
] as const;
export type FailureReason = (typeof PIPELINE_RUN_FAILURE_REASONS)[number];

export const SESSION_CLEANUP_FAILURE_REASON: FailureReason = "session_cleanup_failed";

export const PIPELINE_RUN_EVENT_KINDS = [
  "run_created",
  "activation_started",
  "session_created",
  "agent_running",
  "result_accepted",
  "session_cleanup_completed",
  "activation_failed",
  "transition_committed",
  "terminal_reached",
  "run_succeeded",
  "run_failed",
  "run_cleanup_failed",
] as const;
export type PipelineRunEventKind = (typeof PIPELINE_RUN_EVENT_KINDS)[number];

export class PipelineStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineStateError";
  }
}

export interface PipelineIdentityState {
  schema_version: number;
  bundle_root: string;
  execution_snapshot_sha256: string;
  entry_state: string;
  max_transitions: number;
}

export interface ProtectedInputState {
  id: string;
  path: string;
  sha256: string;
}

export interface PipelineCursorState {
  current_state: string;
  transition_count: number;
}

/**
 * One agent-state activation: exactly one child Session and one worker run.
 * `index` is the global monotonic activation index (contiguous from 1);
 * `attempt` is always 1 in this increment (no retries).
 */
export interface ActivationState {
  index: number;
  state_id: string;
  attempt: number;
  profile: string;
  phase: PipelineRunActivationPhase;
  session_id?: string;
  session_cleanup?: "completed" | "failed";
  result_sha256?: string;
  artifacts?: string[];
  failure_reason?: FailureReason;
}

export interface CommittedTransitionState {
  /** The engine's original transition index within the source state. */
  index: number;
  from: string;
  outcome: string;
  to: string;
  /** The activation whose accepted result this transition commits. */
  activation_index: number;
  result_sha256: string;
  artifacts: string[];
}

export interface TerminalStateState {
  state_id: string;
  result: "success" | "failed";
}

export interface FailureState {
  reason: FailureReason;
}

/**
 * Discriminated event: the base fields are always present; the payload fields
 * depend on the kind and name the state, activation, session, or transition
 * the event belongs to.
 */
export type PipelineRunEvent =
  | { sequence: number; kind: "run_created"; at: string }
  | { sequence: number; kind: "activation_started"; state_id: string; activation_index: number; at: string }
  | { sequence: number; kind: "session_created"; state_id: string; activation_index: number; session_id: string; at: string }
  | { sequence: number; kind: "agent_running"; state_id: string; activation_index: number; at: string }
  | { sequence: number; kind: "result_accepted"; state_id: string; activation_index: number; at: string }
  | { sequence: number; kind: "session_cleanup_completed"; state_id: string; activation_index: number; at: string }
  | { sequence: number; kind: "activation_failed"; state_id: string; activation_index: number; at: string }
  | {
      sequence: number;
      kind: "transition_committed";
      from: string;
      outcome: string;
      to: string;
      transition_index: number;
      activation_index: number;
      at: string;
    }
  | { sequence: number; kind: "terminal_reached"; state_id: string; at: string }
  | { sequence: number; kind: "run_succeeded"; at: string }
  | { sequence: number; kind: "run_failed"; at: string }
  | { sequence: number; kind: "run_cleanup_failed"; at: string };

export interface PipelineRunState {
  schema_version: number;
  revision: number;
  run_id: string;
  status: PipelineRunStatus;
  phase: PipelineRunPhase;
  started_at: string;
  updated_at: string;
  workspace: string;
  pipeline: PipelineIdentityState;
  protected_inputs: ProtectedInputState[];
  cursor: PipelineCursorState;
  activations: ActivationState[];
  transitions: CommittedTransitionState[];
  terminal?: TerminalStateState;
  failure?: FailureState;
  events: PipelineRunEvent[];
}

export type PipelineRunCommand =
  | {
      kind: "create_run";
      runId: string;
      workspace: string;
      identity: PipelineIdentityState;
      protectedInputs: readonly ProtectedInputState[];
    }
  | { kind: "start_activation"; stateId: string; profile: string }
  | { kind: "activation_session_created"; sessionId: string }
  | { kind: "activation_agent_running" }
  | { kind: "activation_result_accepted"; resultSha256: string; artifacts: readonly string[] }
  | { kind: "activation_cleanup_completed" }
  | { kind: "activation_failed"; reason: FailureReason; sessionCleanup: "completed" | "failed" }
  | {
      kind: "transition_committed";
      step: TransitionStep;
      activationIndex: number;
      resultSha256: string;
      artifacts: readonly string[];
    }
  | { kind: "terminal_reached"; terminalStateId: string; terminalResult: "success" | "failed" }
  | { kind: "run_succeeded" }
  | { kind: "run_failed"; reason: FailureReason }
  | { kind: "run_cleanup_failed"; reason: FailureReason };

const ISO_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const RUN_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

function isCleanWorkspaceRelativePath(value: string): boolean {
  if (value === "" || value.startsWith("/") || value.startsWith("~")) {
    return false;
  }
  return value.split("/").every(
    (segment) => segment !== "" && segment !== "." && segment !== ".." && segment !== "~",
  );
}

function isIsoTimestamp(value: unknown): value is string {
  return typeof value === "string" && ISO_TIMESTAMP_PATTERN.test(value);
}

function isSha256Hex(value: unknown): value is string {
  return typeof value === "string" && SHA256_PATTERN.test(value);
}

function isSafePositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isSafeNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

function expectExactObject(
  value: unknown,
  what: string,
  keys: readonly string[],
  optionalKeys: readonly string[] = [],
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineStateError(`${what} is not a JSON object`);
  }
  const obj = value as Record<string, unknown>;
  const expected = new Set([...keys, ...optionalKeys]);
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      throw new PipelineStateError(`${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new PipelineStateError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
  return obj;
}

function expectEnum<T extends string>(value: unknown, allowed: readonly T[], what: string): T {
  if (typeof value !== "string" || !(allowed as readonly string[]).includes(value)) {
    throw new PipelineStateError(
      `${what} must be one of ${JSON.stringify(allowed)}, got ${JSON.stringify(value)}`,
    );
  }
  return value as T;
}

function expectSha256(value: unknown, what: string): string {
  if (!isSha256Hex(value)) {
    throw new PipelineStateError(`${what} must be a lowercase hex SHA-256 digest, got ${JSON.stringify(value)}`);
  }
  return value;
}

function expectIsoTimestamp(value: unknown, what: string): string {
  if (!isIsoTimestamp(value)) {
    throw new PipelineStateError(
      `${what} must be an ISO-8601 UTC timestamp (YYYY-MM-DDTHH:MM:SS.sssZ), got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectCleanWorkspaceRelativePath(value: unknown, what: string): string {
  if (!isNonEmptyString(value) || !isCleanWorkspaceRelativePath(value)) {
    throw new PipelineStateError(
      `${what} must be a clean workspace-relative path, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function expectAbsolutePath(value: unknown, what: string): string {
  if (!isNonEmptyString(value) || !value.startsWith("/")) {
    throw new PipelineStateError(`${what} must be an absolute path, got ${JSON.stringify(value)}`);
  }
  return value;
}

function raise(message: string): never {
  throw new PipelineStateError(message);
}

export function validatePipelineIdentityState(value: unknown, what: string): PipelineIdentityState {
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "bundle_root", "execution_snapshot_sha256", "entry_state", "max_transitions"],
  );
  if (obj.schema_version !== 1) {
    throw new PipelineStateError(`${what}.schema_version must be 1, got ${JSON.stringify(obj.schema_version)}`);
  }
  return {
    schema_version: obj.schema_version,
    bundle_root: expectAbsolutePath(obj.bundle_root, `${what}.bundle_root`),
    execution_snapshot_sha256: expectSha256(obj.execution_snapshot_sha256, `${what}.execution_snapshot_sha256`),
    entry_state: isNonEmptyString(obj.entry_state)
      ? obj.entry_state
      : raise(`${what}.entry_state must be a non-empty string`),
    max_transitions: isSafePositiveInteger(obj.max_transitions)
      ? obj.max_transitions
      : raise(`${what}.max_transitions must be a positive safe integer`),
  };
}

export function validateProtectedInputState(value: unknown, what: string): ProtectedInputState {
  const obj = expectExactObject(value, what, ["id", "path", "sha256"]);
  return {
    id: isNonEmptyString(obj.id) ? obj.id : raise(`${what}.id must be a non-empty string`),
    path: expectCleanWorkspaceRelativePath(obj.path, `${what}.path`),
    sha256: expectSha256(obj.sha256, `${what}.sha256`),
  };
}

function validateCursorState(value: unknown, what: string): PipelineCursorState {
  const obj = expectExactObject(value, what, ["current_state", "transition_count"]);
  return {
    current_state: isNonEmptyString(obj.current_state)
      ? obj.current_state
      : raise(`${what}.current_state must be a non-empty string`),
    transition_count: isSafeNonNegativeInteger(obj.transition_count)
      ? obj.transition_count
      : raise(`${what}.transition_count must be a non-negative safe integer`),
  };
}

function validateActivationState(value: unknown, what: string): ActivationState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "state_id", "attempt", "profile", "phase"],
    ["session_id", "session_cleanup", "result_sha256", "artifacts", "failure_reason"],
  );
  const phase = expectEnum(obj.phase, PIPELINE_RUN_ACTIVATION_PHASES, `${what}.phase`);
  const activation: ActivationState = {
    index: isSafePositiveInteger(obj.index) ? obj.index : raise(`${what}.index must be a positive safe integer`),
    state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
    attempt: isSafePositiveInteger(obj.attempt) ? obj.attempt : raise(`${what}.attempt must be a positive safe integer`),
    profile: isNonEmptyString(obj.profile) ? obj.profile : raise(`${what}.profile must be a non-empty string`),
    phase,
  };
  if (obj.session_id !== undefined) {
    if (!isNonEmptyString(obj.session_id)) {
      throw new PipelineStateError(`${what}.session_id must be a non-empty string when present`);
    }
    activation.session_id = obj.session_id;
  }
  if (obj.session_cleanup !== undefined) {
    if (obj.session_cleanup !== "completed" && obj.session_cleanup !== "failed") {
      throw new PipelineStateError(
        `${what}.session_cleanup must be "completed" or "failed", got ${JSON.stringify(obj.session_cleanup)}`,
      );
    }
    activation.session_cleanup = obj.session_cleanup;
  }
  if (obj.result_sha256 !== undefined) {
    activation.result_sha256 = expectSha256(obj.result_sha256, `${what}.result_sha256`);
  }
  if (obj.artifacts !== undefined) {
    if (!Array.isArray(obj.artifacts)) {
      throw new PipelineStateError(`${what}.artifacts must be an array`);
    }
    activation.artifacts = obj.artifacts.map((artifact, i) =>
      expectCleanWorkspaceRelativePath(artifact, `${what}.artifacts[${i}]`),
    );
  }
  if (obj.failure_reason !== undefined) {
    activation.failure_reason = expectEnum(obj.failure_reason, PIPELINE_RUN_FAILURE_REASONS, `${what}.failure_reason`);
  }
  return activation;
}

function validateCommittedTransition(value: unknown, what: string): CommittedTransitionState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "from", "outcome", "to", "activation_index", "result_sha256", "artifacts"],
  );
  if (!Array.isArray(obj.artifacts)) {
    throw new PipelineStateError(`${what}.artifacts must be an array`);
  }
  return {
    index: isSafeNonNegativeInteger(obj.index) ? obj.index : raise(`${what}.index must be a non-negative safe integer`),
    from: isNonEmptyString(obj.from) ? obj.from : raise(`${what}.from must be a non-empty string`),
    outcome: isNonEmptyString(obj.outcome) ? obj.outcome : raise(`${what}.outcome must be a non-empty string`),
    to: isNonEmptyString(obj.to) ? obj.to : raise(`${what}.to must be a non-empty string`),
    activation_index: isSafePositiveInteger(obj.activation_index)
      ? obj.activation_index
      : raise(`${what}.activation_index must be a positive safe integer`),
    result_sha256: expectSha256(obj.result_sha256, `${what}.result_sha256`),
    artifacts: obj.artifacts.map((artifact, i) =>
      expectCleanWorkspaceRelativePath(artifact, `${what}.artifacts[${i}]`),
    ),
  };
}

function validateTerminalState(value: unknown, what: string): TerminalStateState {
  const obj = expectExactObject(value, what, ["state_id", "result"]);
  if (obj.result !== "success" && obj.result !== "failed") {
    throw new PipelineStateError(
      `${what}.result must be "success" or "failed", got ${JSON.stringify(obj.result)}`,
    );
  }
  return {
    state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
    result: obj.result,
  };
}

function validateFailureState(value: unknown, what: string): FailureState {
  const obj = expectExactObject(value, what, ["reason"]);
  return {
    reason: expectEnum(obj.reason, PIPELINE_RUN_FAILURE_REASONS, `${what}.reason`),
  };
}

const EVENT_BASE_KEYS = ["sequence", "kind", "at"] as const;

function validateEvent(value: unknown, what: string): PipelineRunEvent {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineStateError(`${what} is not a JSON object`);
  }
  const peeked = value as Record<string, unknown>;
  if (!("kind" in peeked)) {
    throw new PipelineStateError(`${what} is missing required field "kind"`);
  }
  const kind = expectEnum(peeked.kind, PIPELINE_RUN_EVENT_KINDS, `${what}.kind`);
  // exact fields per kind: the payload keys belong to the discriminated kind
  const obj = expectExactObject(
    value,
    what,
    [...EVENT_BASE_KEYS, ...eventPayloadKeys(kind)],
  );
  if (typeof obj.sequence !== "number") {
    throw new PipelineStateError(`${what}.sequence must be a number`);
  }
  const at = expectIsoTimestamp(obj.at, `${what}.at`);
  const sequence = obj.sequence;
  switch (kind) {
    case "run_created":
    case "run_succeeded":
    case "run_failed":
    case "run_cleanup_failed":
      return { sequence, kind, at } as PipelineRunEvent;
    case "activation_started":
    case "agent_running":
    case "result_accepted":
    case "session_cleanup_completed":
    case "activation_failed":
      return {
        sequence,
        kind,
        state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
        activation_index: isSafePositiveInteger(obj.activation_index)
          ? obj.activation_index
          : raise(`${what}.activation_index must be a positive safe integer`),
        at,
      } as PipelineRunEvent;
    case "session_created":
      return {
        sequence,
        kind,
        state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
        activation_index: isSafePositiveInteger(obj.activation_index)
          ? obj.activation_index
          : raise(`${what}.activation_index must be a positive safe integer`),
        session_id: isNonEmptyString(obj.session_id)
          ? obj.session_id
          : raise(`${what}.session_id must be a non-empty string`),
        at,
      } as PipelineRunEvent;
    case "transition_committed":
      return {
        sequence,
        kind,
        from: isNonEmptyString(obj.from) ? obj.from : raise(`${what}.from must be a non-empty string`),
        outcome: isNonEmptyString(obj.outcome) ? obj.outcome : raise(`${what}.outcome must be a non-empty string`),
        to: isNonEmptyString(obj.to) ? obj.to : raise(`${what}.to must be a non-empty string`),
        transition_index: isSafeNonNegativeInteger(obj.transition_index)
          ? obj.transition_index
          : raise(`${what}.transition_index must be a non-negative safe integer`),
        activation_index: isSafePositiveInteger(obj.activation_index)
          ? obj.activation_index
          : raise(`${what}.activation_index must be a positive safe integer`),
        at,
      } as PipelineRunEvent;
    case "terminal_reached":
      return {
        sequence,
        kind,
        state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
        at,
      } as PipelineRunEvent;
    default: {
      const exhaustive: never = kind;
      throw new PipelineStateError(`${what} has unsupported kind ${JSON.stringify(exhaustive)}`);
    }
  }
}

function validateEvents(value: unknown, what: string): PipelineRunEvent[] {
  if (!Array.isArray(value)) {
    throw new PipelineStateError(`${what} must be an array`);
  }
  const events: PipelineRunEvent[] = [];
  for (let index = 0; index < value.length; index++) {
    const event = validateEvent(value[index], `${what}[${index}]`);
    if (event.sequence !== index + 1) {
      throw new PipelineStateError(
        `${what}[${index}].sequence must be ${index + 1} (contiguous monotonic sequence), got ${JSON.stringify(event.sequence)}`,
      );
    }
    events.push(event);
  }
  return events;
}

/**
 * Legal successor event kinds after a given event kind. Mirrors the reducer's
 * command guards so a loaded document that could never have been produced by
 * the reducer is rejected fail-closed.
 */
const EVENT_SUCCESSORS: Record<PipelineRunEventKind, readonly PipelineRunEventKind[]> = {
  run_created: ["activation_started", "terminal_reached", "run_failed", "run_cleanup_failed"],
  activation_started: ["session_created", "activation_failed", "run_failed", "run_cleanup_failed"],
  session_created: ["agent_running", "activation_failed", "run_failed", "run_cleanup_failed"],
  agent_running: ["result_accepted", "activation_failed", "run_failed", "run_cleanup_failed"],
  result_accepted: ["session_cleanup_completed", "activation_failed", "run_failed", "run_cleanup_failed"],
  session_cleanup_completed: [
    "transition_committed",
    "terminal_reached",
    "activation_failed",
    "run_failed",
    "run_cleanup_failed",
  ],
  activation_failed: ["run_failed", "run_cleanup_failed"],
  transition_committed: ["activation_started", "terminal_reached", "run_failed", "run_cleanup_failed"],
  terminal_reached: ["run_succeeded", "run_failed", "run_cleanup_failed"],
  run_succeeded: [],
  run_failed: [],
  run_cleanup_failed: [],
};

function eventPayloadKeys(kind: PipelineRunEventKind): readonly string[] {
  switch (kind) {
    case "run_created":
    case "run_succeeded":
    case "run_failed":
    case "run_cleanup_failed":
      return [];
    case "terminal_reached":
      return ["state_id"];
    case "activation_started":
    case "agent_running":
    case "result_accepted":
    case "session_cleanup_completed":
    case "activation_failed":
      return ["state_id", "activation_index"];
    case "session_created":
      return ["state_id", "activation_index", "session_id"];
    case "transition_committed":
      return ["from", "outcome", "to", "transition_index", "activation_index"];
  }
}

function validateEventOrder(events: PipelineRunEvent[]): void {
  if (events.length === 0) {
    throw new PipelineStateError("pipeline run state must contain at least the run_created event");
  }
  if (events[0]!.kind !== "run_created") {
    throw new PipelineStateError("the first pipeline run event must be run_created");
  }
  for (let index = 1; index < events.length; index++) {
    const previous = events[index - 1]!;
    const current = events[index]!;
    if (!EVENT_SUCCESSORS[previous.kind].includes(current.kind)) {
      throw new PipelineStateError(
        `event ${current.sequence} (${current.kind}) may not follow event ${previous.sequence} (${previous.kind})`,
      );
    }
  }
}

/**
 * Every lifecycle event payload must name exactly the activation, session,
 * transition, or terminal it belongs to. Events are replayed in order
 * against the activation/transition/terminal records, so a mutated
 * `state_id`, `activation_index`, `session_id`, transition payload/index, or
 * terminal state id fails closed.
 */
function validateEventPayloadCoherence(
  events: PipelineRunEvent[],
  activations: ActivationState[],
  transitions: CommittedTransitionState[],
  terminal: TerminalStateState | undefined,
): void {
  let activationPointer = 0;
  let transitionPointer = 0;
  for (const event of events) {
    switch (event.kind) {
      case "activation_started": {
        const expected = activations[activationPointer];
        if (
          expected === undefined ||
          event.activation_index !== expected.index ||
          event.state_id !== expected.state_id
        ) {
          throw new PipelineStateError(
            `event ${event.sequence} (activation_started) names activation ${event.activation_index} (${JSON.stringify(event.state_id)}), which does not match the next activation record`,
          );
        }
        activationPointer += 1;
        break;
      }
      case "session_created": {
        const current = activations[activationPointer - 1];
        if (
          current === undefined ||
          event.activation_index !== current.index ||
          event.state_id !== current.state_id
        ) {
          throw new PipelineStateError(
            `event ${event.sequence} (session_created) names activation ${event.activation_index} (${JSON.stringify(event.state_id)}), which is not the activation in progress`,
          );
        }
        if (current.session_id === undefined || event.session_id !== current.session_id) {
          throw new PipelineStateError(
            `event ${event.sequence} (session_created) names session ${JSON.stringify(event.session_id)}, which is not the session recorded by activation ${current.index}`,
          );
        }
        break;
      }
      case "agent_running":
      case "result_accepted":
      case "session_cleanup_completed":
      case "activation_failed": {
        const current = activations[activationPointer - 1];
        if (
          current === undefined ||
          event.activation_index !== current.index ||
          event.state_id !== current.state_id
        ) {
          throw new PipelineStateError(
            `event ${event.sequence} (${event.kind}) names activation ${event.activation_index} (${JSON.stringify(event.state_id)}), which is not the activation in progress`,
          );
        }
        break;
      }
      case "transition_committed": {
        const transition = transitions[transitionPointer];
        if (
          transition === undefined ||
          event.from !== transition.from ||
          event.outcome !== transition.outcome ||
          event.to !== transition.to ||
          event.transition_index !== transition.index ||
          event.activation_index !== transition.activation_index
        ) {
          throw new PipelineStateError(
            `event ${event.sequence} (transition_committed) does not match the next committed transition record`,
          );
        }
        transitionPointer += 1;
        break;
      }
      case "terminal_reached": {
        if (terminal === undefined || event.state_id !== terminal.state_id) {
          throw new PipelineStateError(
            `event ${event.sequence} (terminal_reached) names terminal state ${JSON.stringify(event.state_id)}, which is not the reached terminal`,
          );
        }
        break;
      }
      case "run_created":
      case "run_succeeded":
      case "run_failed":
      case "run_cleanup_failed":
        break;
    }
  }
}

/**
 * Exact-field, fail-closed validation of a parsed pipeline run state
 * document, including cross-field consistency (revision/sequence, cursor
 * versus transition chain, activation contiguity and session ownership,
 * transition-to-activation references, terminal placement, status/phase/
 * failure coherence). Unknown fields, wrong types, and inconsistent
 * combinations are rejected. Version 1 documents are rejected as unsupported.
 */
export function validatePipelineRunState(value: unknown): PipelineRunState {
  if (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    (value as Record<string, unknown>).schema_version === 1
  ) {
    throw new PipelineStateError(
      "pipeline run state has schema_version 1, which is unsupported by this orchestrator (schema version 2 is the supported contract; no v1 migration exists)",
    );
  }
  const obj = expectExactObject(
    value,
    "pipeline run state",
    [
      "schema_version",
      "revision",
      "run_id",
      "status",
      "phase",
      "started_at",
      "updated_at",
      "workspace",
      "pipeline",
      "protected_inputs",
      "cursor",
      "activations",
      "transitions",
      "events",
    ],
    ["terminal", "failure"],
  );
  if (obj.schema_version !== PIPELINE_RUN_STATE_SCHEMA_VERSION) {
    throw new PipelineStateError(
      `pipeline run state has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PIPELINE_RUN_STATE_SCHEMA_VERSION}`,
    );
  }
  if (!isSafePositiveInteger(obj.revision)) {
    throw new PipelineStateError("pipeline run state revision must be a positive safe integer");
  }
  const runId = isNonEmptyString(obj.run_id) && RUN_ID_PATTERN.test(obj.run_id)
    ? obj.run_id
    : raise("pipeline run state run_id must be a safe non-empty identifier");
  const status = expectEnum(obj.status, PIPELINE_RUN_STATUSES, "pipeline run state status");
  const phase = expectEnum(obj.phase, PIPELINE_RUN_PHASES, "pipeline run state phase");
  const startedAt = expectIsoTimestamp(obj.started_at, "pipeline run state started_at");
  const updatedAt = expectIsoTimestamp(obj.updated_at, "pipeline run state updated_at");
  const workspace = expectAbsolutePath(obj.workspace, "pipeline run state workspace");
  const pipeline = validatePipelineIdentityState(obj.pipeline, "pipeline run state pipeline");
  if (!Array.isArray(obj.protected_inputs)) {
    throw new PipelineStateError("pipeline run state protected_inputs must be an array");
  }
  const protectedInputs = obj.protected_inputs.map((input, index) =>
    validateProtectedInputState(input, `pipeline run state protected_inputs[${index}]`),
  );
  {
    const ids = new Set<string>();
    for (const input of protectedInputs) {
      if (ids.has(input.id)) {
        throw new PipelineStateError(
          `pipeline run state protected_inputs declares id ${JSON.stringify(input.id)} more than once`,
        );
      }
      ids.add(input.id);
    }
  }
  const cursor = validateCursorState(obj.cursor, "pipeline run state cursor");
  if (!Array.isArray(obj.activations)) {
    throw new PipelineStateError("pipeline run state activations must be an array");
  }
  const activations = obj.activations.map((activation, index) =>
    validateActivationState(activation, `pipeline run state activations[${index}]`),
  );
  if (!Array.isArray(obj.transitions)) {
    throw new PipelineStateError("pipeline run state transitions must be an array");
  }
  const transitions = obj.transitions.map((transition, index) =>
    validateCommittedTransition(transition, `pipeline run state transitions[${index}]`),
  );
  if (transitions.length > pipeline.max_transitions) {
    throw new PipelineStateError(
      `pipeline run state records ${transitions.length} committed transitions, more than the pipeline transition budget ${pipeline.max_transitions}`,
    );
  }
  const terminal = obj.terminal === undefined ? undefined : validateTerminalState(obj.terminal, "pipeline run state terminal");
  const failure = obj.failure === undefined ? undefined : validateFailureState(obj.failure, "pipeline run state failure");
  const events = validateEvents(obj.events, "pipeline run state events");

  const revision: number = obj.revision;
  if (revision !== events.length) {
    throw new PipelineStateError(
      `pipeline run state revision ${revision} must equal the number of events ${events.length}`,
    );
  }
  validateEventOrder(events);

  // Activation indexes are contiguous, attempts are 1, sessions are unique,
  // only the last activation may still be active, and identity/phase fields
  // are coherent with the activation phase.
  const sessionIds = new Set<string>();
  for (let index = 0; index < activations.length; index++) {
    const activation = activations[index]!;
    if (activation.index !== index + 1) {
      throw new PipelineStateError(
        `activation at position ${index} declares index ${activation.index}; activation indexes must be contiguous from 1`,
      );
    }
    if (activation.attempt !== 1) {
      throw new PipelineStateError(
        `activation ${activation.index} declares attempt ${activation.attempt}; only attempt 1 is supported`,
      );
    }
    if (activation.session_id !== undefined) {
      if (sessionIds.has(activation.session_id)) {
        throw new PipelineStateError(
          `activation ${activation.index} reuses session ${JSON.stringify(activation.session_id)}; a session belongs to exactly one activation`,
        );
      }
      sessionIds.add(activation.session_id);
    }
    if (index < activations.length - 1) {
      if (!FINISHED_ACTIVATION_PHASES.includes(activation.phase)) {
        throw new PipelineStateError(
          `activation ${activation.index} has phase ${JSON.stringify(activation.phase)}; only the last activation may still be active`,
        );
      }
    }
    if (activation.phase === "creating_session" && activation.session_id !== undefined) {
      throw new PipelineStateError(
        `activation ${activation.index} has no session yet but records a session_id`,
      );
    }
    if (
      activation.phase !== "creating_session" &&
      activation.phase !== "failed" &&
      activation.session_id === undefined
    ) {
      throw new PipelineStateError(
        `activation ${activation.index} has phase ${JSON.stringify(activation.phase)} but no session_id`,
      );
    }
    if (activation.phase === "creating_session" && activation.session_cleanup !== undefined) {
      throw new PipelineStateError(
        `activation ${activation.index} has no session yet but records a session cleanup outcome`,
      );
    }
    if (
      (activation.phase === "session_cleanup_completed" || activation.phase === "failed") &&
      activation.session_cleanup === undefined &&
      activation.session_id !== undefined
    ) {
      throw new PipelineStateError(
        `activation ${activation.index} finished but does not record its session cleanup outcome`,
      );
    }
    if (
      activation.session_cleanup !== undefined &&
      activation.phase !== "session_cleanup_completed" &&
      activation.phase !== "failed"
    ) {
      throw new PipelineStateError(
        `activation ${activation.index} has phase ${JSON.stringify(activation.phase)} but already records a session cleanup outcome`,
      );
    }
    if (
      (activation.phase === "result_accepted" ||
        activation.phase === "session_cleanup_completed" ||
        activation.phase === "failed") &&
      activation.result_sha256 === undefined &&
      activation.failure_reason === undefined
    ) {
      throw new PipelineStateError(
        `activation ${activation.index} has phase ${JSON.stringify(activation.phase)} but neither an accepted result digest nor a failure reason`,
      );
    }
    if (
      (activation.phase === "creating_session" ||
        activation.phase === "session_created" ||
        activation.phase === "agent_running") &&
      (activation.result_sha256 !== undefined || activation.artifacts !== undefined)
    ) {
      throw new PipelineStateError(
        `activation ${activation.index} has phase ${JSON.stringify(activation.phase)} but already records an accepted result`,
      );
    }
    if (activation.failure_reason !== undefined && activation.phase !== "failed") {
      throw new PipelineStateError(
        `activation ${activation.index} records a failure reason but has phase ${JSON.stringify(activation.phase)}`,
      );
    }
    // NOTE: `failure_reason` and `result_sha256` may coexist: the result was
    // accepted and then the activation still failed (e.g. the session cleanup
    // failed right after acceptance).
  }

  if (status === "success") {
    if (terminal === undefined || terminal.result !== "success") {
      throw new PipelineStateError('run status success requires a reached terminal state with result "success"');
    }
    if (failure !== undefined) {
      throw new PipelineStateError('run status success must not carry a failure reason');
    }
    if (phase !== "finished") {
      throw new PipelineStateError('run status success requires phase "finished"');
    }
    if (events[events.length - 1]!.kind !== "run_succeeded") {
      throw new PipelineStateError('run status success requires the last event to be run_succeeded');
    }
    for (const activation of activations) {
      if (activation.phase !== "session_cleanup_completed") {
        throw new PipelineStateError(
          `run status success requires every activation to be cleaned up, activation ${activation.index} has phase ${JSON.stringify(activation.phase)}`,
        );
      }
    }
  } else if (status === "failed") {
    if (failure === undefined) {
      throw new PipelineStateError('run status failed requires a normalized failure reason');
    }
    if (phase !== "finished") {
      throw new PipelineStateError('run status failed requires phase "finished"');
    }
    if (events[events.length - 1]!.kind !== "run_failed") {
      throw new PipelineStateError('run status failed requires the last event to be run_failed');
    }
  } else if (status === "cleanup_failed") {
    if (failure === undefined || failure.reason !== SESSION_CLEANUP_FAILURE_REASON) {
      throw new PipelineStateError(
        `run status cleanup_failed requires the failure reason ${JSON.stringify(SESSION_CLEANUP_FAILURE_REASON)}`,
      );
    }
    if (phase !== "finished") {
      throw new PipelineStateError('run status cleanup_failed requires phase "finished"');
    }
    if (events[events.length - 1]!.kind !== "run_cleanup_failed") {
      throw new PipelineStateError('run status cleanup_failed requires the last event to be run_cleanup_failed');
    }
  } else {
    if (failure !== undefined) {
      throw new PipelineStateError('an active run must not carry a failure reason');
    }
    if (phase === "finished") {
      throw new PipelineStateError('an active run must not be in the finished phase');
    }
    if (phase === "validating" && activations.length > 0) {
      throw new PipelineStateError('an active run in the validating phase must not have activations');
    }
    if (phase === "running" && activations.length === 0) {
      throw new PipelineStateError('an active run in the running phase must have activations');
    }
    const lastKind = events[events.length - 1]!.kind;
    if (lastKind === "run_succeeded" || lastKind === "run_failed" || lastKind === "run_cleanup_failed") {
      throw new PipelineStateError(`an active run must not end with the terminal event ${lastKind}`);
    }
  }

  const state: PipelineRunState = {
    schema_version: obj.schema_version,
    revision,
    run_id: runId,
    status,
    phase,
    started_at: startedAt,
    updated_at: updatedAt,
    workspace,
    pipeline,
    protected_inputs: protectedInputs,
    cursor,
    activations,
    transitions,
    events,
  };
  if (cursor.transition_count !== transitions.length) {
    throw new PipelineStateError(
      `cursor.transition_count ${cursor.transition_count} does not match ${transitions.length} committed transitions`,
    );
  }
  for (let index = 0; index < transitions.length; index++) {
    const transition = transitions[index]!;
    if (index === 0) {
      if (transition.from !== pipeline.entry_state) {
        throw new PipelineStateError(
          `the first committed transition starts at ${JSON.stringify(transition.from)}, expected the entry state ${JSON.stringify(pipeline.entry_state)}`,
        );
      }
    } else if (transition.from !== transitions[index - 1]!.to) {
      throw new PipelineStateError(
        `transition at position ${index} starts at ${JSON.stringify(transition.from)}, expected the previous transition target ${JSON.stringify(transitions[index - 1]!.to)}`,
      );
    }
    const activation = activations.find((candidate) => candidate.index === transition.activation_index);
    if (activation === undefined) {
      throw new PipelineStateError(
        `transition at position ${index} references activation ${transition.activation_index} which does not exist`,
      );
    }
    if (transition.activation_index !== index + 1) {
      throw new PipelineStateError(
        `transition at position ${index} references activation ${transition.activation_index}; transitions must reference activations in order`,
      );
    }
    if (activation.phase !== "session_cleanup_completed") {
      throw new PipelineStateError(
        `transition at position ${index} references activation ${transition.activation_index} whose phase ${JSON.stringify(activation.phase)} is not a cleaned activation`,
      );
    }
    if (activation.result_sha256 === undefined) {
      throw new PipelineStateError(
        `transition at position ${index} references activation ${transition.activation_index} without an accepted result digest`,
      );
    }
    if (activation.state_id !== transition.from) {
      throw new PipelineStateError(
        `transition at position ${index} starts at ${JSON.stringify(transition.from)}, but its activation ran state ${JSON.stringify(activation.state_id)}`,
      );
    }
    if (activation.result_sha256 !== transition.result_sha256) {
      throw new PipelineStateError(
        `transition at position ${index} carries result digest ${transition.result_sha256}, but its activation accepted ${activation.result_sha256}`,
      );
    }
    if (JSON.stringify(activation.artifacts ?? []) !== JSON.stringify(transition.artifacts)) {
      throw new PipelineStateError(
        `transition at position ${index} carries artifacts that do not match its activation's accepted artifacts`,
      );
    }
    // transitions must reference cleaned activations in activation order
    if (index > 0) {
      const previous = transitions[index - 1]!;
      if (transition.activation_index <= previous.activation_index) {
        throw new PipelineStateError(
          `transition at position ${index} references activation ${transition.activation_index} which is not after the previous transition's activation ${previous.activation_index}`,
        );
      }
    }
  }
  const expectedCursor =
    transitions.length > 0 ? transitions[transitions.length - 1]!.to : pipeline.entry_state;
  if (cursor.current_state !== expectedCursor) {
    throw new PipelineStateError(
      `cursor.current_state ${JSON.stringify(cursor.current_state)} does not match the expected cursor ${JSON.stringify(expectedCursor)}`,
    );
  }

  if (terminal !== undefined) {
    if (cursor.current_state !== terminal.state_id) {
      throw new PipelineStateError(
        `terminal state ${JSON.stringify(terminal.state_id)} does not match the cursor ${JSON.stringify(cursor.current_state)}`,
      );
    }
    if (activations.length > transitions.length) {
      const uncommitted = activations[transitions.length];
      throw new PipelineStateError(
        `the terminal was reached but activation ${uncommitted?.index}'s transition was never committed`,
      );
    }
    if (phase !== "finalizing" && phase !== "finished") {
      throw new PipelineStateError(
        `a reached terminal state requires phase "finalizing" or "finished", got ${JSON.stringify(phase)}`,
      );
    }
  } else if (phase === "finalizing") {
    throw new PipelineStateError('phase "finalizing" requires a reached terminal state');
  }

  // a new activation starts only after the previous one is cleaned up and its
  // transition is committed: every non-last cleaned activation must have its
  // transition committed
  for (let index = 0; index < activations.length; index++) {
    const activation = activations[index]!;
    if (index === activations.length - 1) {
      break;
    }
    if (activation.phase === "session_cleanup_completed" && transitions.length < activation.index) {
      throw new PipelineStateError(
        `activation ${activation.index} completed cleanup but its transition was never committed`,
      );
    }
  }

  // last: every event payload must name exactly the record it belongs to
  validateEventPayloadCoherence(events, activations, transitions, terminal);

  if (terminal !== undefined) {
    state.terminal = terminal;
  }
  if (failure !== undefined) {
    state.failure = failure;
  }
  return state;
}

export function parsePipelineRunState(raw: string): PipelineRunState {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new PipelineStateError(`pipeline run state is not valid JSON: ${String(cause)}`);
  }
  return validatePipelineRunState(parsed);
}

const PHASE_ORDER: readonly PipelineRunPhase[] = PIPELINE_RUN_PHASES;

function cloneIdentity(identity: PipelineIdentityState): PipelineIdentityState {
  return { ...identity };
}

function cloneProtectedInput(input: ProtectedInputState): ProtectedInputState {
  return { ...input };
}

function cloneActivation(activation: ActivationState): ActivationState {
  const clone: ActivationState = { ...activation };
  if (activation.artifacts !== undefined) {
    clone.artifacts = [...activation.artifacts];
  }
  return clone;
}

function cloneTransition(transition: CommittedTransitionState): CommittedTransitionState {
  return { ...transition, artifacts: [...transition.artifacts] };
}

function cloneState(state: PipelineRunState): PipelineRunState {
  const clone: PipelineRunState = {
    schema_version: state.schema_version,
    revision: state.revision,
    run_id: state.run_id,
    status: state.status,
    phase: state.phase,
    started_at: state.started_at,
    updated_at: state.updated_at,
    workspace: state.workspace,
    pipeline: cloneIdentity(state.pipeline),
    protected_inputs: state.protected_inputs.map(cloneProtectedInput),
    cursor: { ...state.cursor },
    activations: state.activations.map(cloneActivation),
    transitions: state.transitions.map(cloneTransition),
    events: state.events.map((event) => ({ ...event })),
  };
  if (state.terminal !== undefined) {
    clone.terminal = { ...state.terminal };
  }
  if (state.failure !== undefined) {
    clone.failure = { ...state.failure };
  }
  return clone;
}

function event(kind: PipelineRunEventKind, sequence: number, now: Date): PipelineRunEvent {
  return { sequence, kind, at: now.toISOString() } as PipelineRunEvent;
}

function appendPayloadEvent(
  events: PipelineRunEvent[],
  kind: PipelineRunEventKind,
  sequence: number,
  now: Date,
  payload: Record<string, unknown>,
): void {
  events.push({ ...payload, sequence, kind, at: now.toISOString() } as PipelineRunEvent);
}

function fail(
  current: PipelineRunState,
  message: string,
): never {
  throw new PipelineStateError(
    `command rejected for run ${JSON.stringify(current.run_id)} (revision ${current.revision}, status ${current.status}, phase ${current.phase}): ${message}`,
  );
}

/**
 * The activation the next activation-scope command applies to: the last
 * activation, which must still be active (not cleaned up, not failed).
 */
function activeActivation(current: PipelineRunState): ActivationState {
  const activation = current.activations[current.activations.length - 1];
  if (activation === undefined) {
    fail(current, "no activation is in flight");
  }
  if (activation.phase === "session_cleanup_completed" || activation.phase === "failed") {
    fail(current, `the last activation ${activation.index} already finished with phase ${JSON.stringify(activation.phase)}`);
  }
  return activation;
}

function cloneActivationList(activations: readonly ActivationState[]): ActivationState[] {
  return activations.map(cloneActivation);
}

function markLastActivationFailed(
  activations: ActivationState[],
  reason: FailureReason,
  cleanupOutcome: "completed" | "failed",
): void {
  const activation = activations[activations.length - 1];
  if (activation === undefined) {
    return;
  }
  if (activation.phase === "session_cleanup_completed" || activation.phase === "failed") {
    return;
  }
  activation.phase = "failed";
  activation.failure_reason = reason;
  // run_failed is only reachable when the executor's session cleanup settled
  // without a cleanup failure: with a recorded session the delete succeeded
  // ("completed"); a cleanup failure always takes the run_cleanup_failed
  // path ("failed"). Without a recorded session there is no cleanup outcome.
  if (activation.session_id !== undefined) {
    activation.session_cleanup = cleanupOutcome;
  }
}

/**
 * Applies one command to the run state and returns the next immutable
 * snapshot. Exactly one event is appended per command, the revision grows by
 * exactly one, and identity fields never change after creation.
 */
export function reducePipelineRunCommand(
  current: PipelineRunState | null,
  command: PipelineRunCommand,
  now: Date,
): PipelineRunState {
  if (command.kind === "create_run") {
    if (current !== null) {
      throw new PipelineStateError(
        `create_run rejected: run ${JSON.stringify(current.run_id)} already exists (revision ${current.revision})`,
      );
    }
    const identity = cloneIdentity(command.identity);
    if (identity.schema_version !== 1) {
      throw new PipelineStateError("create_run requires pipeline schema_version 1");
    }
    if (!identity.bundle_root.startsWith("/")) {
      throw new PipelineStateError("create_run requires an absolute canonical bundle root");
    }
    if (!isSha256Hex(identity.execution_snapshot_sha256)) {
      throw new PipelineStateError("create_run requires a lowercase hex execution snapshot digest");
    }
    if (!isNonEmptyString(identity.entry_state)) {
      throw new PipelineStateError("create_run requires a non-empty entry state");
    }
    if (!isSafePositiveInteger(identity.max_transitions)) {
      throw new PipelineStateError("create_run requires a positive max_transitions");
    }
    if (!Array.isArray(command.protectedInputs)) {
      throw new PipelineStateError("create_run requires a protected inputs array");
    }
    const protectedInputs = command.protectedInputs.map(cloneProtectedInput);
    const seenIds = new Set<string>();
    for (const input of protectedInputs) {
      if (!isNonEmptyString(input.id)) {
        throw new PipelineStateError("create_run requires a non-empty protected input id");
      }
      if (seenIds.has(input.id)) {
        throw new PipelineStateError(`create_run declares protected input ${JSON.stringify(input.id)} more than once`);
      }
      seenIds.add(input.id);
      if (!isCleanWorkspaceRelativePath(input.path)) {
        throw new PipelineStateError("create_run requires clean workspace-relative protected input paths");
      }
      if (!isSha256Hex(input.sha256)) {
        throw new PipelineStateError("create_run requires lowercase hex protected input digests");
      }
    }
    if (!isNonEmptyString(command.runId) || !RUN_ID_PATTERN.test(command.runId)) {
      throw new PipelineStateError("create_run requires a safe non-empty run id");
    }
    if (!isNonEmptyString(command.workspace) || !command.workspace.startsWith("/")) {
      throw new PipelineStateError("create_run requires an absolute canonical workspace path");
    }
    const at = now.toISOString();
    const state: PipelineRunState = {
      schema_version: PIPELINE_RUN_STATE_SCHEMA_VERSION,
      revision: 1,
      run_id: command.runId,
      status: "active",
      phase: "validating",
      started_at: at,
      updated_at: at,
      workspace: command.workspace,
      pipeline: identity,
      protected_inputs: protectedInputs,
      cursor: { current_state: identity.entry_state, transition_count: 0 },
      activations: [],
      transitions: [],
      events: [event("run_created", 1, now)],
    };
    return state;
  }

  if (current === null) {
    throw new PipelineStateError(`command ${command.kind} rejected: no pipeline run state exists yet`);
  }

  const next = cloneState(current);
  const sequence = current.events.length + 1;
  const at = now.toISOString();
  next.updated_at = at;
  next.revision = current.revision + 1;
  const appendEvent = (kind: PipelineRunEventKind): void => {
    next.events = [...next.events, event(kind, sequence, now)];
  };

  switch (command.kind) {
    case "start_activation": {
      if (current.status !== "active") {
        fail(current, "starting an activation requires an active run");
      }
      if (current.phase !== "validating" && current.phase !== "running") {
        fail(current, `starting an activation requires phase "validating" or "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; no further activations are possible");
      }
      if (!isNonEmptyString(command.stateId)) {
        throw new PipelineStateError("start_activation requires a non-empty agent state id");
      }
      if (!isNonEmptyString(command.profile)) {
        throw new PipelineStateError("start_activation requires a non-empty profile name");
      }
      const previous = current.activations[current.activations.length - 1];
      if (previous !== undefined && previous.phase !== "session_cleanup_completed") {
        fail(
          current,
          `a new activation requires the previous activation to be cleaned up, activation ${previous.index} has phase ${JSON.stringify(previous.phase)}`,
        );
      }
      if (previous !== undefined) {
        const lastTransition = current.transitions[current.transitions.length - 1];
        if (lastTransition === undefined || lastTransition.activation_index !== previous.index) {
          fail(
            current,
            "a new activation requires the previous activation's transition to be committed",
          );
        }
      }
      if (command.stateId !== current.cursor.current_state) {
        fail(
          current,
          `activation state ${JSON.stringify(command.stateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      const activation: ActivationState = {
        index: current.activations.length + 1,
        state_id: command.stateId,
        attempt: 1,
        profile: command.profile,
        phase: "creating_session",
      };
      next.activations = [...cloneActivationList(current.activations), activation];
      if (current.phase === "validating") {
        next.phase = "running";
      }
      const activationStartedEvent: PipelineRunEvent = {
        sequence,
        kind: "activation_started",
        state_id: activation.state_id,
        activation_index: activation.index,
        at,
      };
      next.events = [...next.events, activationStartedEvent];
      return next;
    }
    case "activation_session_created": {
      if (current.status !== "active") {
        fail(current, "session creation requires an active run");
      }
      const activation = activeActivation(current);
      if (activation.phase !== "creating_session") {
        fail(current, `session creation requires activation phase "creating_session", got ${JSON.stringify(activation.phase)}`);
      }
      if (!isNonEmptyString(command.sessionId)) {
        throw new PipelineStateError("activation_session_created requires a non-empty session id");
      }
      for (const existing of current.activations) {
        if (existing.session_id === command.sessionId) {
          fail(current, `session ${JSON.stringify(command.sessionId)} already belongs to activation ${existing.index}`);
        }
      }
      const activations = cloneActivationList(next.activations);
      const last = activations[activations.length - 1]!;
      last.session_id = command.sessionId;
      last.phase = "session_created";
      next.activations = activations;
      const event: PipelineRunEvent = {
        sequence,
        kind: "session_created",
        state_id: last.state_id,
        activation_index: last.index,
        session_id: command.sessionId,
        at,
      };
      next.events = [...next.events, event];
      return next;
    }
    case "activation_agent_running": {
      if (current.status !== "active") {
        fail(current, "starting the agent requires an active run");
      }
      const activation = activeActivation(current);
      if (activation.phase !== "session_created") {
        fail(current, `starting the agent requires activation phase "session_created", got ${JSON.stringify(activation.phase)}`);
      }
      const activations = cloneActivationList(next.activations);
      activations[activations.length - 1]!.phase = "agent_running";
      next.activations = activations;
      appendPayloadEvent(next.events, "agent_running", sequence, now, {
        state_id: activation.state_id,
        activation_index: activation.index,
      });
      return next;
    }
    case "activation_result_accepted": {
      if (current.status !== "active") {
        fail(current, "accepting a result requires an active run");
      }
      const activation = activeActivation(current);
      if (activation.phase !== "agent_running") {
        fail(current, `accepting a result requires activation phase "agent_running", got ${JSON.stringify(activation.phase)}`);
      }
      if (!isSha256Hex(command.resultSha256)) {
        throw new PipelineStateError("activation_result_accepted requires a lowercase hex result digest");
      }
      const artifacts: string[] = [];
      for (const artifact of command.artifacts) {
        if (!isNonEmptyString(artifact) || !isCleanWorkspaceRelativePath(artifact)) {
          throw new PipelineStateError(
            `activation_result_accepted artifacts must be clean workspace-relative paths, got ${JSON.stringify(artifact)}`,
          );
        }
        artifacts.push(artifact);
      }
      const activations = cloneActivationList(next.activations);
      const updated = activations[activations.length - 1]!;
      updated.phase = "result_accepted";
      updated.result_sha256 = command.resultSha256;
      updated.artifacts = artifacts;
      next.activations = activations;
      appendPayloadEvent(next.events, "result_accepted", sequence, now, {
        state_id: activation.state_id,
        activation_index: activation.index,
      });
      return next;
    }
    case "activation_cleanup_completed": {
      if (current.status !== "active") {
        fail(current, "recording the session cleanup requires an active run");
      }
      const activation = activeActivation(current);
      if (activation.phase !== "result_accepted") {
        fail(current, `recording the session cleanup requires activation phase "result_accepted", got ${JSON.stringify(activation.phase)}`);
      }
      if (activation.session_id === undefined) {
        fail(current, "recording the session cleanup requires the recorded session");
      }
      const activations = cloneActivationList(next.activations);
      const updated = activations[activations.length - 1]!;
      updated.phase = "session_cleanup_completed";
      updated.session_cleanup = "completed";
      next.activations = activations;
      appendPayloadEvent(next.events, "session_cleanup_completed", sequence, now, {
        state_id: activation.state_id,
        activation_index: activation.index,
      });
      return next;
    }
    case "activation_failed": {
      if (current.status !== "active") {
        fail(current, "failing an activation requires an active run");
      }
      if (!PIPELINE_RUN_FAILURE_REASONS.includes(command.reason)) {
        throw new PipelineStateError(
          `activation_failed requires a normalized failure reason, got ${JSON.stringify(command.reason)}`,
        );
      }
      if (command.sessionCleanup !== "completed" && command.sessionCleanup !== "failed") {
        throw new PipelineStateError(
          `activation_failed requires sessionCleanup "completed" or "failed", got ${JSON.stringify(command.sessionCleanup)}`,
        );
      }
      const activation = activeActivation(current);
      if (
        command.reason === SESSION_CLEANUP_FAILURE_REASON &&
        command.sessionCleanup !== "failed"
      ) {
        throw new PipelineStateError(
          "activation_failed with the session cleanup failure reason requires sessionCleanup \"failed\"",
        );
      }
      const activations = cloneActivationList(next.activations);
      const updated = activations[activations.length - 1]!;
      updated.phase = "failed";
      updated.failure_reason = command.reason;
      updated.session_cleanup = command.sessionCleanup;
      if (command.sessionCleanup === "failed" && activation.session_id === undefined) {
        throw new PipelineStateError(
          "activation_failed with a failed session cleanup requires a recorded session",
        );
      }
      next.activations = activations;
      appendPayloadEvent(next.events, "activation_failed", sequence, now, {
        state_id: activation.state_id,
        activation_index: activation.index,
      });
      return next;
    }
    case "transition_committed": {
      if (current.status !== "active") {
        fail(current, "committing a transition requires an active run");
      }
      if (current.phase !== "running") {
        fail(current, `committing a transition requires phase "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; no further transitions are possible");
      }
      const activation = current.activations[current.activations.length - 1];
      if (activation === undefined) {
        fail(current, "committing a transition requires a cleaned activation");
      }
      if (activation.phase !== "session_cleanup_completed") {
        fail(
          current,
          `committing a transition requires the last activation to be cleaned up, activation ${activation.index} has phase ${JSON.stringify(activation.phase)}`,
        );
      }
      if (activation.result_sha256 === undefined) {
        fail(current, `activation ${activation.index} has no accepted result digest`);
      }
      const step = command.step;
      if (
        step === null ||
        typeof step !== "object" ||
        !isNonEmptyString(step.from) ||
        !isNonEmptyString(step.outcome) ||
        !isNonEmptyString(step.to) ||
        !isSafeNonNegativeInteger(step.transition_index)
      ) {
        throw new PipelineStateError("transition_committed requires a well-formed engine TransitionStep");
      }
      if (command.activationIndex !== activation.index) {
        fail(
          current,
          `transition references activation ${command.activationIndex}, but the cleaned activation is ${activation.index}`,
        );
      }
      if (command.resultSha256 !== activation.result_sha256) {
        fail(
          current,
          `transition carries result digest ${command.resultSha256}, but activation ${activation.index} accepted ${activation.result_sha256}`,
        );
      }
      if (JSON.stringify(activation.artifacts ?? []) !== JSON.stringify([...command.artifacts])) {
        fail(current, `transition artifacts do not match the artifacts accepted by activation ${activation.index}`);
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
      if (step.from !== activation.state_id) {
        fail(
          current,
          `transition starts at ${JSON.stringify(step.from)}, but activation ${activation.index} ran state ${JSON.stringify(activation.state_id)}`,
        );
      }
      for (const artifact of command.artifacts) {
        if (!isNonEmptyString(artifact) || !isCleanWorkspaceRelativePath(artifact)) {
          throw new PipelineStateError(
            `transition_committed artifacts must be clean workspace-relative paths, got ${JSON.stringify(artifact)}`,
          );
        }
      }
      next.transitions = [
        ...current.transitions,
        {
          index: step.transition_index,
          from: step.from,
          outcome: step.outcome,
          to: step.to,
          activation_index: activation.index,
          result_sha256: command.resultSha256,
          artifacts: [...command.artifacts],
        },
      ];
      next.cursor = {
        current_state: step.to,
        transition_count: current.cursor.transition_count + 1,
      };
      const transitionEvent: PipelineRunEvent = {
        sequence,
        kind: "transition_committed",
        from: step.from,
        outcome: step.outcome,
        to: step.to,
        transition_index: step.transition_index,
        activation_index: activation.index,
        at,
      };
      next.events = [...next.events, transitionEvent];
      return next;
    }
    case "terminal_reached": {
      if (current.status !== "active") {
        fail(current, "recording the terminal state requires an active run");
      }
      if (current.phase !== "validating" && current.phase !== "running") {
        fail(current, `recording the terminal state requires phase "validating" or "running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached and immutable");
      }
      if (command.terminalStateId !== current.cursor.current_state) {
        fail(
          current,
          `terminal state ${JSON.stringify(command.terminalStateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      // every started activation must be cleaned up before the terminal is
      // recorded, and every cleaned activation's transition must be committed:
      // the engine only reaches the terminal after the last transition, and
      // that transition required a cleaned activation
      const last = current.activations[current.activations.length - 1];
      if (last !== undefined && last.phase !== "session_cleanup_completed") {
        fail(
          current,
          `recording the terminal state requires the last activation to be cleaned up, activation ${last.index} has phase ${JSON.stringify(last.phase)}`,
        );
      }
      if (current.activations.length > current.transitions.length) {
        const uncommitted = current.activations[current.transitions.length];
        fail(
          current,
          `recording the terminal state requires activation ${uncommitted?.index}'s transition to be committed first`,
        );
      }
      next.terminal = { state_id: command.terminalStateId, result: command.terminalResult };
      next.phase = "finalizing";
      const terminalEvent: PipelineRunEvent = {
        sequence,
        kind: "terminal_reached",
        state_id: command.terminalStateId,
        at,
      };
      next.events = [...next.events, terminalEvent];
      return next;
    }
    case "run_succeeded": {
      if (current.status !== "active") {
        fail(current, "success requires an active run");
      }
      if (current.terminal === undefined) {
        fail(current, "success requires a reached terminal state");
      }
      if (current.terminal.result !== "success") {
        fail(current, "success requires a terminal state with result success");
      }
      if (current.phase !== "finalizing") {
        fail(current, `success requires phase "finalizing", got ${JSON.stringify(current.phase)}`);
      }
      next.status = "success";
      next.phase = "finished";
      appendEvent("run_succeeded");
      return next;
    }
    case "run_failed": {
      if (!PIPELINE_RUN_FAILURE_REASONS.includes(command.reason)) {
        throw new PipelineStateError(`run_failed requires a normalized failure reason, got ${JSON.stringify(command.reason)}`);
      }
      if (current.status !== "active") {
        fail(current, `failure cannot overwrite status ${JSON.stringify(current.status)}; the terminal run status is immutable`);
      }
      if (current.phase === "finished" && current.status === "active") {
        fail(current, "an active run in the finished phase cannot fail");
      }
      next.status = "failed";
      next.phase = "finished";
      next.failure = { reason: command.reason };
      markLastActivationFailed(next.activations, command.reason, "completed");
      appendEvent("run_failed");
      return next;
    }
    case "run_cleanup_failed": {
      if (!PIPELINE_RUN_FAILURE_REASONS.includes(command.reason)) {
        throw new PipelineStateError(`run_cleanup_failed requires a normalized failure reason, got ${JSON.stringify(command.reason)}`);
      }
      if (current.status !== "active") {
        fail(current, `cleanup failure cannot overwrite status ${JSON.stringify(current.status)}`);
      }
      next.status = "cleanup_failed";
      next.phase = "finished";
      next.failure = { reason: command.reason };
      markLastActivationFailed(next.activations, command.reason, "failed");
      appendEvent("run_cleanup_failed");
      return next;
    }
    default: {
      const exhaustive: never = command;
      throw new PipelineStateError(`unknown pipeline run command: ${JSON.stringify(exhaustive)}`);
    }
  }
}
