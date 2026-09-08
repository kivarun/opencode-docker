import type { TransitionStep } from "./pipeline_engine.ts";

/**
 * Versioned machine-owned contract for the durable pipeline run state of
 * `agent-smoke`. The JSON document is the source of truth for an accepted
 * pipeline, the execution cursor, the active attempt, committed transitions,
 * and the final result. It deliberately contains no credentials, no
 * environment values, no prompt or input bodies, no OpenCode configuration,
 * no raw worker output, and no result summary text.
 *
 * All mutations go through the pure reducer (`reducePipelineRunCommand`):
 * no filesystem access, no randomness, timestamps come from an injected
 * clock, and the input snapshot is never mutated.
 */

export const PIPELINE_RUN_STATE_SCHEMA_VERSION = 1;

export const PIPELINE_RUN_STATUSES = [
  "active",
  "success",
  "failed",
  "cleanup_failed",
] as const;
export type PipelineRunStatus = (typeof PIPELINE_RUN_STATUSES)[number];

export const PIPELINE_RUN_PHASES = [
  "validating",
  "creating_session",
  "session_created",
  "agent_running",
  "finalizing",
  "finished",
] as const;
export type PipelineRunPhase = (typeof PIPELINE_RUN_PHASES)[number];

export const PIPELINE_RUN_ATTEMPT_PHASES = [
  "running",
  "completed",
  "failed",
] as const;
export type PipelineRunAttemptPhase = (typeof PIPELINE_RUN_ATTEMPT_PHASES)[number];

export const PIPELINE_RUN_FAILURE_REASONS = [
  "internal_error",
  "worker_failed",
  "worker_timeout",
  "agent_result_invalid",
  "protected_input_modified",
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
  "phase_entered",
  "session_created",
  "attempt_started",
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

export interface AttemptState {
  state_id: string;
  attempt: number;
  profile: string;
  session_id: string;
  phase: PipelineRunAttemptPhase;
}

export interface CommittedTransitionState {
  index: number;
  from: string;
  outcome: string;
  to: string;
  attempt: number;
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

export interface PipelineRunEvent {
  sequence: number;
  kind: PipelineRunEventKind;
  at: string;
}

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
  protected_input: ProtectedInputState;
  session_id?: string;
  cursor: PipelineCursorState;
  attempt?: AttemptState;
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
      protectedInput: ProtectedInputState;
      initialPhase: "validating" | "creating_session";
    }
  | { kind: "enter_phase"; phase: PipelineRunPhase }
  | { kind: "session_created"; sessionId: string }
  | { kind: "attempt_started"; stateId: string; attempt: number; profile: string }
  | {
      kind: "transition_committed";
      step: TransitionStep;
      attempt: number;
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

function raise(message: string): never {
  throw new PipelineStateError(message);
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

function validateAttemptState(value: unknown, what: string): AttemptState {
  const obj = expectExactObject(value, what, ["state_id", "attempt", "profile", "session_id", "phase"]);
  return {
    state_id: isNonEmptyString(obj.state_id) ? obj.state_id : raise(`${what}.state_id must be a non-empty string`),
    attempt: isSafePositiveInteger(obj.attempt) ? obj.attempt : raise(`${what}.attempt must be a positive safe integer`),
    profile: isNonEmptyString(obj.profile) ? obj.profile : raise(`${what}.profile must be a non-empty string`),
    session_id: isNonEmptyString(obj.session_id)
      ? obj.session_id
      : raise(`${what}.session_id must be a non-empty string`),
    phase: expectEnum(obj.phase, PIPELINE_RUN_ATTEMPT_PHASES, `${what}.phase`),
  };
}

function validateCommittedTransition(value: unknown, what: string): CommittedTransitionState {
  const obj = expectExactObject(
    value,
    what,
    ["index", "from", "outcome", "to", "attempt", "result_sha256", "artifacts"],
  );
  if (!Array.isArray(obj.artifacts)) {
    throw new PipelineStateError(`${what}.artifacts must be an array`);
  }
  return {
    index: isSafeNonNegativeInteger(obj.index) ? obj.index : raise(`${what}.index must be a non-negative safe integer`),
    from: isNonEmptyString(obj.from) ? obj.from : raise(`${what}.from must be a non-empty string`),
    outcome: isNonEmptyString(obj.outcome) ? obj.outcome : raise(`${what}.outcome must be a non-empty string`),
    to: isNonEmptyString(obj.to) ? obj.to : raise(`${what}.to must be a non-empty string`),
    attempt: isSafePositiveInteger(obj.attempt) ? obj.attempt : raise(`${what}.attempt must be a positive safe integer`),
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

function validateEvents(value: unknown, what: string): PipelineRunEvent[] {
  if (!Array.isArray(value)) {
    throw new PipelineStateError(`${what} must be an array`);
  }
  const events: PipelineRunEvent[] = [];
  for (let index = 0; index < value.length; index++) {
    const obj = expectExactObject(value[index], `${what}[${index}]`, ["sequence", "kind", "at"]);
    if (obj.sequence !== index + 1) {
      throw new PipelineStateError(
        `${what}[${index}].sequence must be ${index + 1} (contiguous monotonic sequence), got ${JSON.stringify(obj.sequence)}`,
      );
    }
    events.push({
      sequence: obj.sequence,
      kind: expectEnum(obj.kind, PIPELINE_RUN_EVENT_KINDS, `${what}[${index}].kind`),
      at: expectIsoTimestamp(obj.at, `${what}[${index}].at`),
    });
  }
  return events;
}

/**
 * Legal successor event kinds after a given event kind. Mirrors the reducer's
 * command guards so a loaded document that could never have been produced by
 * the reducer is rejected fail-closed.
 */
const EVENT_SUCCESSORS: Record<PipelineRunEventKind, readonly PipelineRunEventKind[]> = {
  run_created: ["phase_entered", "run_failed", "run_cleanup_failed"],
  phase_entered: ["session_created", "run_failed", "run_cleanup_failed"],
  session_created: ["attempt_started", "run_failed", "run_cleanup_failed"],
  attempt_started: ["transition_committed", "run_failed", "run_cleanup_failed"],
  transition_committed: ["transition_committed", "terminal_reached", "run_failed", "run_cleanup_failed"],
  terminal_reached: ["run_succeeded", "run_failed", "run_cleanup_failed"],
  run_succeeded: [],
  run_failed: [],
  run_cleanup_failed: [],
};

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
 * Exact-field, fail-closed validation of a parsed pipeline run state
 * document, including cross-field consistency (revision/sequence, cursor
 * versus transition chain, terminal placement, status/phase/failure
 * coherence). Unknown fields, wrong types, and inconsistent combinations are
 * rejected.
 */
export function validatePipelineRunState(value: unknown): PipelineRunState {
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
      "protected_input",
      "cursor",
      "transitions",
      "events",
    ],
    ["session_id", "attempt", "terminal", "failure"],
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
  const protectedInput = validateProtectedInputState(obj.protected_input, "pipeline run state protected_input");
  if (obj.session_id !== undefined && !isNonEmptyString(obj.session_id)) {
    throw new PipelineStateError("pipeline run state session_id must be a non-empty string when present");
  }
  const cursor = validateCursorState(obj.cursor, "pipeline run state cursor");
  const attempt = obj.attempt === undefined ? undefined : validateAttemptState(obj.attempt, "pipeline run state attempt");
  if (!Array.isArray(obj.transitions)) {
    throw new PipelineStateError("pipeline run state transitions must be an array");
  }
  const transitions = obj.transitions.map((transition, index) =>
    validateCommittedTransition(transition, `pipeline run state transitions[${index}]`),
  );
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

  if (cursor.transition_count !== transitions.length) {
    throw new PipelineStateError(
      `cursor.transition_count ${cursor.transition_count} does not match ${transitions.length} committed transitions`,
    );
  }
  for (let index = 0; index < transitions.length; index++) {
    const transition = transitions[index]!;
    if (transition.index !== index) {
      throw new PipelineStateError(
        `transition at position ${index} declares index ${transition.index}; committed transition indexes must be contiguous`,
      );
    }
    if (index === 0) {
      if (transition.from !== pipeline.entry_state) {
        throw new PipelineStateError(
          `the first committed transition starts at ${JSON.stringify(transition.from)}, expected the entry state ${JSON.stringify(pipeline.entry_state)}`,
        );
      }
    } else if (transition.from !== transitions[index - 1]!.to) {
      throw new PipelineStateError(
        `transition ${transition.index} starts at ${JSON.stringify(transition.from)}, expected the previous transition target ${JSON.stringify(transitions[index - 1]!.to)}`,
      );
    }
  }
  const expectedCursor =
    transitions.length > 0 ? transitions[transitions.length - 1]!.to : pipeline.entry_state;
  if (cursor.current_state !== expectedCursor) {
    throw new PipelineStateError(
      `cursor.current_state ${JSON.stringify(cursor.current_state)} does not match the expected cursor ${JSON.stringify(expectedCursor)}`,
    );
  }

  if (attempt === undefined) {
    if (transitions.length > 0) {
      throw new PipelineStateError("committed transitions require a recorded attempt");
    }
    if (terminal !== undefined) {
      throw new PipelineStateError("a reached terminal requires a recorded attempt");
    }
  } else {
    if (attempt.session_id !== obj.session_id) {
      throw new PipelineStateError("attempt.session_id must equal the run session_id");
    }
    for (const transition of transitions) {
      if (transition.attempt > attempt.attempt) {
        throw new PipelineStateError(
          `transition ${transition.index} references attempt ${transition.attempt} which was never started (latest recorded attempt is ${attempt.attempt})`,
        );
      }
    }
    if (terminal !== undefined) {
      if (attempt.phase !== "completed") {
        throw new PipelineStateError(
          `attempt phase must be "completed" once the terminal state is reached, got ${JSON.stringify(attempt.phase)}`,
        );
      }
      if (cursor.current_state !== terminal.state_id) {
        throw new PipelineStateError(
          `terminal state ${JSON.stringify(terminal.state_id)} does not match the cursor ${JSON.stringify(cursor.current_state)}`,
        );
      }
    } else if (attempt.phase === "completed") {
      throw new PipelineStateError('attempt phase "completed" requires a reached terminal state');
    }
    if (attempt.phase === "running" && terminal !== undefined) {
      throw new PipelineStateError('attempt phase "running" is impossible once the terminal state is reached');
    }
    if (attempt.phase === "failed" && status !== "failed" && status !== "cleanup_failed") {
      throw new PipelineStateError('attempt phase "failed" requires the run status failed or cleanup_failed');
    }
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
    const lastKind = events[events.length - 1]!.kind;
    if (lastKind === "run_succeeded" || lastKind === "run_failed" || lastKind === "run_cleanup_failed") {
      throw new PipelineStateError(`an active run must not end with the terminal event ${lastKind}`);
    }
  }

  if (terminal !== undefined && phase !== "finalizing" && phase !== "finished") {
    throw new PipelineStateError(
      `a reached terminal state requires phase "finalizing" or "finished", got ${JSON.stringify(phase)}`,
    );
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
    protected_input: protectedInput,
    cursor,
    transitions,
    events,
  };
  if (obj.session_id !== undefined) {
    state.session_id = obj.session_id as string;
  }
  if (attempt !== undefined) {
    state.attempt = attempt;
  }
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

function cloneAttempt(attempt: AttemptState): AttemptState {
  return { ...attempt };
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
    protected_input: cloneProtectedInput(state.protected_input),
    cursor: { ...state.cursor },
    transitions: state.transitions.map(cloneTransition),
    events: state.events.map((event) => ({ ...event })),
  };
  if (state.session_id !== undefined) {
    clone.session_id = state.session_id;
  }
  if (state.attempt !== undefined) {
    clone.attempt = cloneAttempt(state.attempt);
  }
  if (state.terminal !== undefined) {
    clone.terminal = { ...state.terminal };
  }
  if (state.failure !== undefined) {
    clone.failure = { ...state.failure };
  }
  return clone;
}

function event(kind: PipelineRunEventKind, sequence: number, now: Date): PipelineRunEvent {
  return { sequence, kind, at: now.toISOString() };
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
    const protectedInput = cloneProtectedInput(command.protectedInput);
    if (!isNonEmptyString(protectedInput.id)) {
      throw new PipelineStateError("create_run requires a non-empty protected input id");
    }
    if (!isCleanWorkspaceRelativePath(protectedInput.path)) {
      throw new PipelineStateError("create_run requires a clean workspace-relative protected input path");
    }
    if (!isSha256Hex(protectedInput.sha256)) {
      throw new PipelineStateError("create_run requires a lowercase hex protected input digest");
    }
    if (!isNonEmptyString(command.runId) || !RUN_ID_PATTERN.test(command.runId)) {
      throw new PipelineStateError("create_run requires a safe non-empty run id");
    }
    if (!isNonEmptyString(command.workspace) || !command.workspace.startsWith("/")) {
      throw new PipelineStateError("create_run requires an absolute canonical workspace path");
    }
    if (command.initialPhase !== "validating" && command.initialPhase !== "creating_session") {
      throw new PipelineStateError("create_run initial phase must be validating or creating_session");
    }
    const at = now.toISOString();
    const state: PipelineRunState = {
      schema_version: PIPELINE_RUN_STATE_SCHEMA_VERSION,
      revision: 1,
      run_id: command.runId,
      status: "active",
      phase: command.initialPhase,
      started_at: at,
      updated_at: at,
      workspace: command.workspace,
      pipeline: identity,
      protected_input: protectedInput,
      cursor: { current_state: identity.entry_state, transition_count: 0 },
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
    case "enter_phase": {
      if (current.status !== "active") {
        fail(current, "phase changes require an active run");
      }
      const currentIndex = PHASE_ORDER.indexOf(current.phase);
      const targetIndex = PHASE_ORDER.indexOf(command.phase);
      if (targetIndex !== currentIndex + 1 || command.phase === "finished") {
        fail(
          current,
          `illegal phase transition ${JSON.stringify(current.phase)} -> ${JSON.stringify(command.phase)}; the next phase would be ${JSON.stringify(PHASE_ORDER[currentIndex + 1])}`,
        );
      }
      next.phase = command.phase;
      appendEvent("phase_entered");
      return next;
    }
    case "session_created": {
      if (current.status !== "active") {
        fail(current, "session creation requires an active run");
      }
      if (current.phase !== "creating_session") {
        fail(current, `session creation requires phase "creating_session", got ${JSON.stringify(current.phase)}`);
      }
      if (current.session_id !== undefined) {
        fail(current, "the child session is already recorded");
      }
      if (!isNonEmptyString(command.sessionId)) {
        throw new PipelineStateError("session_created requires a non-empty session id");
      }
      next.session_id = command.sessionId;
      next.phase = "session_created";
      appendEvent("session_created");
      return next;
    }
    case "attempt_started": {
      if (current.status !== "active") {
        fail(current, "starting an attempt requires an active run");
      }
      if (current.phase !== "session_created") {
        fail(current, `starting an attempt requires phase "session_created", got ${JSON.stringify(current.phase)}`);
      }
      if (current.attempt !== undefined) {
        fail(current, "an attempt is already recorded; parallel or repeated attempts are not supported");
      }
      if (current.session_id === undefined) {
        fail(current, "an attempt requires the recorded child session");
      }
      if (!isSafePositiveInteger(command.attempt)) {
        throw new PipelineStateError("attempt_started requires a positive attempt number");
      }
      if (!isNonEmptyString(command.stateId)) {
        throw new PipelineStateError("attempt_started requires a non-empty agent state id");
      }
      if (!isNonEmptyString(command.profile)) {
        throw new PipelineStateError("attempt_started requires a non-empty profile name");
      }
      next.attempt = {
        state_id: command.stateId,
        attempt: command.attempt,
        profile: command.profile,
        session_id: current.session_id,
        phase: "running",
      };
      next.phase = "agent_running";
      appendEvent("attempt_started");
      return next;
    }
    case "transition_committed": {
      if (current.status !== "active") {
        fail(current, "committing a transition requires an active run");
      }
      if (current.phase !== "agent_running") {
        fail(current, `committing a transition requires phase "agent_running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.attempt === undefined) {
        fail(current, "committing a transition requires a started attempt");
      }
      if (current.attempt.phase !== "running") {
        fail(current, `committing a transition requires a running attempt, got ${JSON.stringify(current.attempt.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached; no further transitions are possible");
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
      if (command.attempt !== current.attempt.attempt) {
        fail(current, `transition references attempt ${command.attempt}, but the running attempt is ${current.attempt.attempt}`);
      }
      if (!isSha256Hex(command.resultSha256)) {
        throw new PipelineStateError("transition_committed requires a lowercase hex accepted result digest");
      }
      if (step.from !== current.cursor.current_state) {
        fail(
          current,
          `transition starts at ${JSON.stringify(step.from)}, but the cursor is at ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      if (step.transition_index !== current.transitions.length) {
        fail(
          current,
          `transition index ${step.transition_index} does not match the next transition index ${current.transitions.length}`,
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
          attempt: command.attempt,
          result_sha256: command.resultSha256,
          artifacts: [...command.artifacts],
        },
      ];
      next.cursor = {
        current_state: step.to,
        transition_count: current.cursor.transition_count + 1,
      };
      appendEvent("transition_committed");
      return next;
    }
    case "terminal_reached": {
      if (current.status !== "active") {
        fail(current, "recording the terminal state requires an active run");
      }
      if (current.phase !== "agent_running") {
        fail(current, `recording the terminal state requires phase "agent_running", got ${JSON.stringify(current.phase)}`);
      }
      if (current.terminal !== undefined) {
        fail(current, "the terminal state is already reached and immutable");
      }
      if (current.attempt === undefined || current.attempt.phase !== "running") {
        fail(current, "recording the terminal state requires a running attempt");
      }
      if (command.terminalStateId !== current.cursor.current_state) {
        fail(
          current,
          `terminal state ${JSON.stringify(command.terminalStateId)} does not match the cursor ${JSON.stringify(current.cursor.current_state)}`,
        );
      }
      next.terminal = { state_id: command.terminalStateId, result: command.terminalResult };
      next.attempt = { ...current.attempt, phase: "completed" };
      next.phase = "finalizing";
      appendEvent("terminal_reached");
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
      if (next.attempt !== undefined && next.attempt.phase === "running") {
        next.attempt = { ...next.attempt, phase: "failed" };
      }
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
      if (next.attempt !== undefined && next.attempt.phase === "running") {
        next.attempt = { ...next.attempt, phase: "failed" };
      }
      appendEvent("run_cleanup_failed");
      return next;
    }
    default: {
      const exhaustive: never = command;
      throw new PipelineStateError(`unknown pipeline run command: ${JSON.stringify(exhaustive)}`);
    }
  }
}
