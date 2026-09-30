/**
 * Production-neutral composition of the full `revise_task` handoff
 * (unwired): the restart-aware revise-task intervention followed by the
 * coordinator's resume entrypoint.
 *
 * This controller is the single layer that binds the two existing
 * authoritative facades into one fixed sequence:
 *
 * 1. `applyPipelineV2ReviseTaskIntervention` — the restart-aware revise
 *    intervention (the plan restoration, the derivation of the compiled
 *    stage/pointer/next revision/`revise_task_intent`, the acceptance and
 *    the completion; its own R0–R4 windows, racing reconciliation and full
 *    verification stay authoritative);
 * 2. the defensive verification of the successful handoff — the flat
 *    result carries exactly the typed contract fields, the durable state
 *    is the exact authoritative sink snapshot object (proven by identity,
 *    never by a structural re-comparison and never by a second parser or a
 *    second state-delta machine), and the completed boundary carries the
 *    targeted durable bindings (the answered target wait with the exact
 *    `revise_task` response, the single wait-bound task revision, the last
 *    open generation bound to the last plan record with the exact
 *    replanned closure and no open iteration);
 * 3. `resumePipelineV2Run` — the coordinator's production-neutral resume
 *    entrypoint, called with the captured pipeline, the captured run id,
 *    run root and sink, and stable runtime/control adapters built from the
 *    contract functions captured exactly once in the preflight (so a
 *    mutation of the caller-owned runtime or control objects during the
 *    pending intervention cannot change the resume).
 *
 * The controller owns no durable side effect of its own: it dispatches
 * nothing, publishes nothing, loads nothing, never opens the sink, never
 * calls the reducer, validator, store, manifest loader, serializer or
 * digest machinery, and never starts a worker. The caller hands over the
 * opened sink and the loaded pipeline; after a process crash the caller
 * opens a fresh sink and repeats the whole facade — no reopen happens
 * inside this layer. In a normal call both facades use the same handed
 * sink. The intervention's typed errors pass through by object identity
 * and the resume is never started after one; the coordinator's thrown
 * errors pass through by object identity as well. The signal lifecycle
 * (acceptance and cutoff) stays owned by the coordinator through the
 * captured control functions; this layer interprets no signal itself.
 *
 * The caller's `taskBody` is a captured content input passed unchanged to
 * the intervention; its digest contract belongs to the intervention facade
 * and the body is re-hashed nowhere here — no second manifest builder,
 * parser, validator or state-delta machine exists, and the body never
 * enters the result, the durable state or any diagnostic.
 *
 * Capture and preflight (all before the first await and before any durable
 * intervention): the options shape; then the nine option fields
 * (`pipeline`, `runRoot`, `sink`, `runtime`, `control`, `runId`,
 * `waitIndex`, `taskId`, `taskBody`) each read exactly once; then the ops
 * record shape and its two members each read exactly once with function
 * checks; then the pipeline provenance gate; then the scalar/structural
 * validations — a non-empty absolute `runRoot`, a structural sink (a
 * record with a `dispatch` function; the `snapshot`/`poisoned` getters
 * stay owned by the composed layers and the coordinator), a record
 * runtime, a record control, a safe `runId`, a positive safe integer
 * `waitIndex`, a safe `taskId` and a non-empty string `taskBody`; then the
 * runtime functions `createExecutionSession`/`createToolSession` and the
 * control functions `currentSignal`/`freezeSignal` are read and bound
 * exactly once into stable frozen adapters. Hostile extra option fields
 * are never read; caller-owned objects are never frozen or modified. No
 * intent, candidate, digest, stage, plan or budget field is accepted from
 * the caller — every one of them is derived from the durable data by the
 * intervention facade and verified here against the authoritative
 * snapshot.
 *
 * The successfully returned coordinator result is verified defensively —
 * only the exact runtime shapes of the coordinator's
 * `PipelineV2ResumeCoordinationResult` are accepted (success: exactly the
 * own keys `ok`,`state` with a state identical to the authoritative
 * snapshot; refusal: exactly `ok`,`refused`,`reason`,`state` with
 * `refused === true` and a reason from the coordinator's refusal
 * vocabulary; ordinary failure: exactly `ok`,`reason`,`state` with a
 * reason from the canonical `PIPELINE_V2_FAILURE_REASONS`; failure and
 * refusal states null or exactly the authoritative snapshot) — and
 * returned unchanged by object identity: refusal, worker failure, signal
 * failure and persistence failure stay coordinator-owned classifications.
 * A malformed result of either facade is this layer's own `invalid_result`
 * carrying the last authoritative snapshot — never a leaked `TypeError`,
 * never healed downstream.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ReviseTaskResumeControllerError`,
 * `applyPipelineV2ReviseTaskResumeWithIo` and the frozen
 * `productionReviseTaskResumeOps`; the public module exports exactly the
 * error and `resumePipelineV2RunAfterReviseTaskIntervention` (types are
 * not runtime keys).
 *
 * Not implemented (stays unwired): the revise-task intervention selection
 * policy, the runner, the CLI, the default pipeline bundle, automatic
 * resume, schema/reducer changes, migrations/API/T3 and multi-process
 * locking.
 */
import { isAbsolute } from "node:path";
import {
  applyPipelineV2ReviseTaskIntervention,
  type AppliedPipelineV2ReviseTaskIntervention,
} from "./pipeline_v2_revise_task_intervention_controller.ts";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
} from "./pipeline_v2_coordinator.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import { PIPELINE_V2_FAILURE_REASONS } from "./pipeline_v2_state.ts";

/**
 * The coordinator's refusal reason vocabulary, pinned to the coordinator's
 * own `PipelineV2ResumeRefusalReason` type: the compile-time assertions
 * below fail the build if the list is invalid or incomplete. There is no
 * runtime export of the vocabulary from the coordinator, so this typed
 * literal list is the vocabulary itself, never a weaker local subset of
 * some other list.
 */
const PIPELINE_V2_RESUME_REFUSAL_REASONS = [
  "missing_state",
  "sink_poisoned",
  "run_id_mismatch",
  "invalid_state",
  "pipeline_mismatch",
  "run_layout_invalid",
  "run_input_modified",
  "accepted_output_modified",
  "internal_error",
] as const;

type ListedRefusalReason = (typeof PIPELINE_V2_RESUME_REFUSAL_REASONS)[number];
type ListedRefusalReasonsValid =
  ListedRefusalReason extends PipelineV2ResumeRefusalReason ? true : never;
const LISTED_REFUSAL_REASONS_VALID: ListedRefusalReasonsValid = true;
void LISTED_REFUSAL_REASONS_VALID;
type AllRefusalReasonsListed =
  Exclude<PipelineV2ResumeRefusalReason, ListedRefusalReason> extends never ? true : never;
const ALL_REFUSAL_REASONS_LISTED: AllRefusalReasonsListed = true;
void ALL_REFUSAL_REASONS_LISTED;

const PIPELINE_V2_RESUME_REFUSAL_REASON_SET: ReadonlySet<string> = new Set(
  PIPELINE_V2_RESUME_REFUSAL_REASONS,
);

const PIPELINE_V2_RESUME_FAILURE_REASON_SET: ReadonlySet<string> = new Set(
  PIPELINE_V2_FAILURE_REASONS,
);

export type PipelineV2ReviseTaskResumeControllerFailureReason = "invalid_options" | "invalid_result";

/**
 * A failure of the handoff composition layer itself with its stable
 * machine-readable `reason` and the last authoritative durable state
 * (`null` when none was established). The composed facades' typed errors
 * pass through by identity and never take this shape.
 */
export class PipelineV2ReviseTaskResumeControllerError extends Error {
  readonly reason: PipelineV2ReviseTaskResumeControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReviseTaskResumeControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReviseTaskResumeControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The single ops seam: exactly the two existing authoritative facades and
 * nothing else. No reducer, filesystem, store, publisher, serializer,
 * digest builder, registry or CLI/runner capability is reachable through
 * it.
 */
export interface PipelineV2ReviseTaskResumeOps {
  readonly applyIntervention: typeof applyPipelineV2ReviseTaskIntervention;
  readonly resumeRun: typeof resumePipelineV2Run;
}

/**
 * The frozen production ops: the two existing facades bound by identity;
 * no installer and no mutable module-global seam.
 */
export const productionReviseTaskResumeOps: PipelineV2ReviseTaskResumeOps = deepFreezeValue({
  applyIntervention: applyPipelineV2ReviseTaskIntervention,
  resumeRun: resumePipelineV2Run,
}) as unknown as PipelineV2ReviseTaskResumeOps;

export interface ApplyPipelineV2ReviseTaskResumeOptions {
  /** The exact deep-frozen snapshot a successful `loadPipelineV2` returned. */
  readonly pipeline: ResolvedPipelineV2;
  /** Canonical orchestrator-owned run root of the same run as the sink. */
  readonly runRoot: string;
  /**
   * The opened existing state sink the intervention and the resume share;
   * the production `PipelineV2RunStateSink` satisfies it structurally.
   */
  readonly sink: PipelineV2CoordinatorStateSink;
  /** The agent runtime the coordinator consumes for the resumed execution. */
  readonly runtime: PipelineV2AgentRuntime;
  /** The signal control boundary; acceptance/cutoff stay coordinator-owned. */
  readonly control: PipelineV2CoordinatorControl;
  /** The run id the opened sink already owns (must match the durable state). */
  readonly runId: string;
  /** The wait journal index of the revise_task wait. */
  readonly waitIndex: number;
  /** The task id the intervention revises. */
  readonly taskId: string;
  /** The caller's revised task body; a captured content input only. */
  readonly taskBody: string;
}

const REVISE_TASK_ACTION_ID = "revise_task";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

function controllerError(
  reason: PipelineV2ReviseTaskResumeControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskResumeControllerError {
  return new PipelineV2ReviseTaskResumeControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2ReviseTaskResumeControllerError {
  return controllerError("invalid_options", message, null);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskResumeControllerError {
  return controllerError("invalid_result", message, state);
}

/**
 * The captured caller policy: the three scalars the intervention facade is
 * called with and the result and durable state are verified against. No
 * intent, candidate, digest, stage, plan or budget field belongs here —
 * every one of them is derived by the intervention facade from the durable
 * data and verified against the authoritative snapshot.
 */
interface ReviseTaskResumePolicy {
  readonly runId: string;
  readonly waitIndex: number;
  readonly taskId: string;
}

/**
 * Captures one contract function of an injected object exactly once,
 * before any side effect: the function is read one time, checked against
 * the contract, and bound to its owner. Reassigning the property later
 * cannot change the dispatch, and the user object is never frozen or
 * modified. Diagnostics carry no values.
 */
function captureContractFunction(
  owner: Record<string, unknown>,
  name: "createExecutionSession" | "createToolSession" | "currentSignal" | "freezeSignal",
  ownerLabel: string,
): unknown {
  let value: unknown;
  try {
    value = owner[name];
  } catch {
    throw invalidOptions(`${ownerLabel} contract violated: ${name} must be a function`);
  }
  if (typeof value !== "function") {
    throw invalidOptions(`${ownerLabel} contract violated: ${name} must be a function`);
  }
  return (value as (...args: never[]) => unknown).bind(owner);
}

/**
 * The defensive verification of one successful intervention result: the
 * flat shape carries exactly the typed contract fields, the durable state
 * is the exact authoritative sink snapshot object by identity, and the
 * completed boundary carries the targeted durable bindings. Total for
 * malformed values: every check is a typed `invalid_result`, never a
 * `TypeError`.
 */
function verifyInterventionResult(
  resultValue: unknown,
  policy: ReviseTaskResumePolicy,
  authoritative: PipelineV2RunState | null,
): void {
  try {
    if (!isRecord(resultValue)) {
      throw invalidResult("the revise-task intervention result is not a record", authoritative);
    }
    const result = resultValue as Record<string, unknown>;
    if (
      !isPositiveSafeInteger(result["wait_index"]) ||
      result["wait_index"] !== policy.waitIndex ||
      !isString(result["task_id"]) ||
      result["task_id"] !== policy.taskId ||
      !isPositiveSafeInteger(result["task_revision"]) ||
      !isLowercaseSha256(result["intent_sha256"]) ||
      !isLowercaseSha256(result["request_sha256"]) ||
      !isLowercaseSha256(result["response_sha256"]) ||
      !isLowercaseSha256(result["task_sha256"]) ||
      result["action_id"] !== REVISE_TASK_ACTION_ID ||
      !isNonEmptyString(result["action_to"]) ||
      !isPositiveSafeInteger(result["generation_index"]) ||
      !isPositiveSafeInteger(result["iteration_index"])
    ) {
      throw invalidResult("the revise-task intervention result does not carry the exact contract fields", authoritative);
    }
    const stateValue = result["state"];
    if (authoritative === null || stateValue !== authoritative) {
      throw invalidResult("the revise-task intervention result does not carry the authoritative durable state", authoritative);
    }
    verifyHandoffBoundary(stateValue as PipelineV2RunState, result, policy, authoritative);
  } catch (cause) {
    if (cause instanceof PipelineV2ReviseTaskResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the revise-task intervention result could not be verified", authoritative);
  }
}

/**
 * The targeted completed-boundary bindings of the handoff, read only from
 * the authoritative snapshot the real intervention result carries by
 * identity: run identity, status/phase, the cursor at the declared action
 * target on the wait anchor, the journals at the anchor, the answered
 * target wait (the last and only record of its index, carrying the exact
 * request/intent/`revise_task` response digests and the declared routing
 * target), the single wait-bound task revision of the intervention, the
 * last plan record matching the last open generation's plan binding, and
 * the last open generation's last iteration closed by the exact replanned
 * closure with no open iteration.
 */
function verifyHandoffBoundary(
  state: PipelineV2RunState,
  result: Record<string, unknown>,
  policy: ReviseTaskResumePolicy,
  authoritative: PipelineV2RunState | null,
): void {
  if (state.run_id !== policy.runId || state.status !== "active" || state.phase !== "running") {
    throw invalidResult("the handoff boundary is not the active running run of the caller identity", authoritative);
  }
  const cursor = state.cursor as unknown;
  if (
    !isRecord(cursor) ||
    !isNonNegativeSafeInteger(cursor["transition_count"]) ||
    cursor["current_state"] !== result["action_to"]
  ) {
    throw invalidResult("the handoff boundary cursor is not at the declared revise_task routing target", authoritative);
  }
  const transitionCount = cursor["transition_count"] as number;
  if (
    !Array.isArray(state.transitions) ||
    state.transitions.length !== transitionCount ||
    !Array.isArray(state.executions) ||
    state.executions.length !== transitionCount
  ) {
    throw invalidResult("the handoff boundary journals do not sit at the wait boundary", authoritative);
  }
  const waits = state.waits as unknown;
  if (!Array.isArray(waits) || waits.length === 0) {
    throw invalidResult("the handoff boundary carries no wait record", authoritative);
  }
  let target: Record<string, unknown> | undefined;
  for (const entry of waits) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed wait record", authoritative);
    }
    if (entry["index"] === policy.waitIndex) {
      if (target !== undefined) {
        throw invalidResult("the target wait is not the only record of its index", authoritative);
      }
      target = entry;
    }
  }
  if (target === undefined || waits[waits.length - 1] !== target) {
    throw invalidResult("the target wait is not the last wait record", authoritative);
  }
  if (
    target["transition_count"] !== transitionCount ||
    target["request_sha256"] !== result["request_sha256"]
  ) {
    throw invalidResult("the target wait does not carry the declared request digest on the wait boundary", authoritative);
  }
  const intentRecord = target["intent"] as unknown;
  if (!isRecord(intentRecord) || intentRecord["intent_sha256"] !== result["intent_sha256"]) {
    throw invalidResult("the target wait does not carry the exact accepted revise task intent", authoritative);
  }
  const responseRecord = target["response"] as unknown;
  if (
    !isRecord(responseRecord) ||
    responseRecord["action_id"] !== REVISE_TASK_ACTION_ID ||
    responseRecord["response_sha256"] !== result["response_sha256"]
  ) {
    throw invalidResult("the target wait does not carry the exact revise_task response", authoritative);
  }
  const actions = target["actions"] as unknown;
  if (!Array.isArray(actions)) {
    throw invalidResult("the target wait carries no declared actions", authoritative);
  }
  let declaredTo: unknown;
  let declaredCount = 0;
  for (const action of actions) {
    if (!isRecord(action)) {
      throw invalidResult("the target wait carries a malformed declared action", authoritative);
    }
    if (action["id"] === REVISE_TASK_ACTION_ID) {
      declaredCount += 1;
      declaredTo = action["to"];
    }
  }
  if (declaredCount !== 1 || declaredTo !== result["action_to"]) {
    throw invalidResult("the target wait does not declare the exact revise_task routing target", authoritative);
  }
  const taskRevisions = state.task_revisions as unknown;
  if (!Array.isArray(taskRevisions)) {
    throw invalidResult("the handoff boundary carries no task revision ledger", authoritative);
  }
  let bound: Record<string, unknown> | undefined;
  for (const entry of taskRevisions) {
    if (!isRecord(entry)) {
      throw invalidResult("the task revision ledger carries a malformed record", authoritative);
    }
    if (entry["wait_index"] === policy.waitIndex) {
      if (bound !== undefined) {
        throw invalidResult("the target wait carries more than one wait-bound task revision", authoritative);
      }
      bound = entry;
    }
    if (
      isString(entry["task_id"]) &&
      entry["task_id"] === result["task_id"] &&
      isPositiveSafeInteger(entry["revision"]) &&
      (entry["revision"] as number) > (result["task_revision"] as number)
    ) {
      throw invalidResult("the task revision ledger carries a later revision of the revised task", authoritative);
    }
  }
  if (
    bound === undefined ||
    bound["task_id"] !== result["task_id"] ||
    bound["revision"] !== result["task_revision"] ||
    bound["sha256"] !== result["task_sha256"] ||
    bound["intent_sha256"] !== result["intent_sha256"]
  ) {
    throw invalidResult("the wait-bound task revision does not match the intervention result", authoritative);
  }
  const planRevisions = state.plan_revisions as unknown;
  if (!Array.isArray(planRevisions) || planRevisions.length === 0) {
    throw invalidResult("the handoff boundary carries no accepted plan revision", authoritative);
  }
  for (const entry of planRevisions) {
    if (!isRecord(entry)) {
      throw invalidResult("the plan revision ledger carries a malformed record", authoritative);
    }
  }
  const lastPlan = planRevisions[planRevisions.length - 1] as Record<string, unknown>;
  const generations = state.generations as unknown;
  if (!Array.isArray(generations) || generations.length === 0) {
    throw invalidResult("the handoff boundary carries no stage generation", authoritative);
  }
  const generation = generations[generations.length - 1] as unknown;
  if (!isRecord(generation) || generation["closed"] !== undefined) {
    throw invalidResult("the revised generation is not the last open generation", authoritative);
  }
  if (generation["index"] !== result["generation_index"]) {
    throw invalidResult("the revised generation does not carry the intervention's generation index", authoritative);
  }
  if (generation["plan_sha256"] !== lastPlan["sha256"]) {
    throw invalidResult("the revised generation's plan binding does not match the last accepted plan revision", authoritative);
  }
  const iterations = generation["iterations"] as unknown;
  if (!Array.isArray(iterations)) {
    throw invalidResult("the revised generation carries no iteration history", authoritative);
  }
  const lastIteration: unknown = iterations[iterations.length - 1];
  if (!isRecord(lastIteration) || lastIteration["index"] !== result["iteration_index"]) {
    throw invalidResult("the revised generation does not end at the intervention's iteration", authoritative);
  }
  const closed = lastIteration["closed"] as unknown;
  if (
    !isRecord(closed) ||
    closed["by"] !== "replanned" ||
    closed["wait_index"] !== policy.waitIndex ||
    closed["closed_transition_count"] !== transitionCount
  ) {
    throw invalidResult("the intervention's iteration does not carry the exact replanned closure", authoritative);
  }
  if (generation["open_iteration"] !== undefined) {
    throw invalidResult("the revised generation carries an open iteration after the intervention", authoritative);
  }
}

function hasExactOwnKeys(record: Record<string, unknown>, ...expected: string[]): boolean {
  const keys = Object.keys(record);
  return keys.length === expected.length && expected.every((key) => keys.includes(key));
}

/**
 * The defensive verification of one successfully returned coordinator
 * result union: only the exact runtime shapes of the coordinator's
 * `PipelineV2ResumeCoordinationResult` are accepted — a success carries
 * exactly the own enumerable keys `ok`,`state` (state record identical to
 * the authoritative snapshot); a refusal carries exactly
 * `ok`,`refused`,`reason`,`state` with `refused === true` and a reason
 * from the pinned refusal vocabulary; an ordinary failure carries exactly
 * `ok`,`reason`,`state` with no own `refused` field and a reason from the
 * canonical `PIPELINE_V2_FAILURE_REASONS` (the imported state-vocabulary
 * list, never a local copy); failure and refusal states are null or
 * exactly the authoritative sink snapshot. The verified result is
 * returned unchanged by object identity — refusal, worker failure,
 * signal failure and persistence failure stay coordinator-owned
 * classifications, and diagnostics never echo a hostile reason, key or
 * value.
 */
function verifyResumeResult(
  resultValue: unknown,
  authoritative: PipelineV2RunState | null,
): PipelineV2ResumeCoordinationResult {
  try {
    if (!isRecord(resultValue)) {
      throw invalidResult("the resume coordinator result is not a record", authoritative);
    }
    const result = resultValue as Record<string, unknown>;
    const keys = Object.keys(result);
    const ok = result["ok"];
    if (ok === true) {
      if (!hasExactOwnKeys(result, "ok", "state")) {
        throw invalidResult("the resume coordinator success result carries foreign fields", authoritative);
      }
      const state = result["state"];
      if (state !== authoritative || !isRecord(state)) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
    }
    if (ok !== false) {
      throw invalidResult("the resume coordinator result carries no valid ok discriminant", authoritative);
    }
    if (keys.includes("refused")) {
      if (!hasExactOwnKeys(result, "ok", "refused", "reason", "state")) {
        throw invalidResult("the resume coordinator refusal result carries foreign fields", authoritative);
      }
      if (result["refused"] !== true) {
        throw invalidResult("the resume coordinator result carries a malformed refusal discriminant", authoritative);
      }
      const reason = result["reason"];
      if (!isString(reason) || !PIPELINE_V2_RESUME_REFUSAL_REASON_SET.has(reason)) {
        throw invalidResult("the resume coordinator refusal result carries no valid refusal reason", authoritative);
      }
      const state = result["state"];
      if (state !== null && state !== authoritative) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
    }
    if (!hasExactOwnKeys(result, "ok", "reason", "state")) {
      throw invalidResult("the resume coordinator failure result carries foreign fields", authoritative);
    }
    const reason = result["reason"];
    if (!isString(reason) || !PIPELINE_V2_RESUME_FAILURE_REASON_SET.has(reason)) {
      throw invalidResult("the resume coordinator failure result carries no valid failure reason", authoritative);
    }
    const state = result["state"];
    if (state !== null && state !== authoritative) {
      throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
    }
    return resultValue as unknown as PipelineV2ResumeCoordinationResult;
  } catch (cause) {
    if (cause instanceof PipelineV2ReviseTaskResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the resume coordinator result could not be verified", authoritative);
  }
}

/**
 * Validate, compose and verify one full `revise_task` handoff through the
 * two existing facades (see the module docstring for the full order,
 * capture boundary and verification semantics).
 */
export async function applyPipelineV2ReviseTaskResumeWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<PipelineV2ResumeCoordinationResult> {
  // Capture boundary: the options shape, every options field and the ops
  // record shape and its two members are read exactly once, all before the
  // first await and before any durable intervention. A later mutation of
  // the caller's options, runtime, control or ops cannot change this
  // handoff.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot: unknown = options["runRoot"];
  const sink: unknown = options["sink"];
  const runtime: unknown = options["runtime"];
  const control: unknown = options["control"];
  const runId: unknown = options["runId"];
  const waitIndex: unknown = options["waitIndex"];
  const taskId: unknown = options["taskId"];
  const taskBody: unknown = options["taskBody"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const applyIntervention: unknown = ops["applyIntervention"];
  const resumeRun: unknown = ops["resumeRun"];
  if (typeof applyIntervention !== "function" || typeof resumeRun !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires the two composed facade functions");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any field of a durable state is read. Its own
  // typed error propagates unchanged.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 revise task resume controller");
  if (!isString(runRoot) || runRoot === "" || !isAbsolute(runRoot)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires an absolute runRoot string");
  }
  if (!isRecord(sink) || typeof (sink as Record<string, unknown>)["dispatch"] !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a structural state sink");
  }
  if (!isRecord(runtime)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires an agent runtime object");
  }
  if (!isRecord(control)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a signal control object");
  }
  if (!isPipelineV2SafeId(runId)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a safe run id");
  }
  if (!isPositiveSafeInteger(waitIndex)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a positive safe integer wait index");
  }
  if (!isPipelineV2SafeId(taskId)) {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a safe task id");
  }
  if (!isString(taskBody) || taskBody === "") {
    throw invalidOptions("resumePipelineV2RunAfterReviseTaskIntervention requires a non-empty task body");
  }
  const policy: ReviseTaskResumePolicy = {
    runId,
    waitIndex,
    taskId,
  };

  // The runtime and control contract functions are captured exactly once
  // and bound to their owners; the stable adapters the coordinator
  // consumes are built from the captured functions only, so a mutation of
  // the caller-owned runtime or control objects during the pending
  // intervention cannot change the resume. The signal lifecycle stays
  // coordinator-owned through the captured control functions.
  const capturedCreateExecutionSession = captureContractFunction(
    runtime as Record<string, unknown>,
    "createExecutionSession",
    "the agent runtime",
  );
  const capturedCreateToolSession = captureContractFunction(
    runtime as Record<string, unknown>,
    "createToolSession",
    "the agent runtime",
  );
  const capturedCurrentSignal = captureContractFunction(
    control as Record<string, unknown>,
    "currentSignal",
    "the signal control",
  );
  const capturedFreezeSignal = captureContractFunction(
    control as Record<string, unknown>,
    "freezeSignal",
    "the signal control",
  );
  const adaptedRuntime = deepFreezeValue({
    createExecutionSession: (state: Parameters<PipelineV2AgentRuntime["createExecutionSession"]>[0], activation: Parameters<PipelineV2AgentRuntime["createExecutionSession"]>[1]) =>
      (capturedCreateExecutionSession as PipelineV2AgentRuntime["createExecutionSession"])(state, activation),
    createToolSession: (state: Parameters<PipelineV2AgentRuntime["createToolSession"]>[0], activation: Parameters<PipelineV2AgentRuntime["createToolSession"]>[1]) =>
      (capturedCreateToolSession as PipelineV2AgentRuntime["createToolSession"])(state, activation),
  }) as unknown as PipelineV2AgentRuntime;
  const adaptedControl = deepFreezeValue({
    currentSignal: () => (capturedCurrentSignal as PipelineV2CoordinatorControl["currentSignal"])(),
    freezeSignal: () => (capturedFreezeSignal as PipelineV2CoordinatorControl["freezeSignal"])(),
  }) as unknown as PipelineV2CoordinatorControl;
  const structuralSink = sink as unknown as PipelineV2CoordinatorStateSink;

  // Step 1: the restart-aware revise-task intervention facade. Its typed
  // errors pass through by object identity; the resume is never started
  // after one.
  const interventionResult: AppliedPipelineV2ReviseTaskIntervention = await (
    applyIntervention as typeof applyPipelineV2ReviseTaskIntervention
  )({
    pipeline,
    runRoot,
    sink: structuralSink,
    runId,
    waitIndex,
    taskId,
    taskBody,
  });

  // Step 2: the defensive verification of the successful handoff. The
  // authoritative snapshot is read exactly once, immediately after the
  // intervention and before the resume; the real intervention result
  // carries it as the exact same immutable state object.
  const authoritativeBeforeResume: PipelineV2RunState | null = structuralSink.snapshot;
  verifyInterventionResult(interventionResult, policy, authoritativeBeforeResume);

  // Step 3: the coordinator's resume entrypoint — the captured pipeline,
  // run id, run root and sink, and the stable runtime/control adapters. A
  // thrown error passes through by object identity.
  const resumeResult = await (resumeRun as typeof resumePipelineV2Run)(
    {
      pipeline,
      runId,
      runRoot,
      sink: structuralSink,
      runtime: adaptedRuntime,
    },
    adaptedControl,
  );

  // Step 4: the defensive verification of the coordinator result union;
  // the last authoritative snapshot is read exactly once for the identity
  // binding and as the verification failures' error state.
  const authoritativeAfterResume: PipelineV2RunState | null = structuralSink.snapshot;
  return verifyResumeResult(resumeResult, authoritativeAfterResume);
}
