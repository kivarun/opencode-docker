import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "./pipeline_v2_run_plan_compiled_internal.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import { restorePipelineV2AcceptedRunPlan } from "./pipeline_v2_run_plan_restore.ts";
import type {
  RestorePipelineV2AcceptedRunPlanOptions,
  RestoredPipelineV2AcceptedRunPlan,
} from "./pipeline_v2_run_plan_restore_internal.ts";
import {
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunTaskRevision,
  type PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";
import {
  acceptPipelineV2ReviseTaskIntent,
  PipelineV2ReviseTaskIntentControllerError,
  type AcceptPipelineV2ReviseTaskIntentOptions,
} from "./pipeline_v2_revise_task_intent_controller.ts";
import {
  completePipelineV2ReviseTask,
  type CompletePipelineV2ReviseTaskOptions,
} from "./pipeline_v2_revise_task_completion_controller.ts";
import { isLowercaseSha256, isPipelineV2SafeId, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type { PipelineV2RunCommand, PipelineV2RunState } from "./pipeline_v2_state.ts";

/**
 * Production-neutral restart-aware composition controller for the first
 * half of the `revise_task` user intervention (unwired).
 *
 * The public API owns the minimal user-policy contract
 * `{runId, waitIndex, taskId, taskBody}` plus the runtime resources
 * (`pipeline`, `runRoot`, `sink`) and composes the existing authoritative
 * layers into one intervention that survives a process restart:
 *
 * 1. a read-only restoration of the last durably accepted plan through the
 *    existing `restorePipelineV2AcceptedRunPlan` on the single captured
 *    authoritative `before = sink.snapshot`, fully verified unchanged
 *    (every schema-owned field pinned position by position — not
 *    identity-only and not length-only);
 * 2. the derivation, entirely from durable data and the restored compiled
 *    plan, with no caller-supplied revision, digest, stage, generation or
 *    iteration index: the last and only open stage generation bound to the
 *    last accepted plan revision, its exact compiled stage (the single
 *    trusted compiled-stage resolver, which is also the compiled-plan
 *    provenance probe, cross-checked against the durable pipeline identity
 *    through the hidden originating identity), exactly one task pointer
 *    carrying the caller `taskId`, the next revision `pointer.revision +
 *    1`, the predecessor digest `pointer.sha256` and the fixed origin
 *    `user_response` — then the candidate task revision and the exact
 *    `revise_task_intent` through the single manifest preparers (the body
 *    exists only inside the prepared candidate and never enters
 *    diagnostics, results or durable state);
 * 3. the fixed sequence, classified from the authoritative state without
 *    message parsing:
 *    - the waiting boundary with the open wait and the iteration still
 *      open: the existing `acceptPipelineV2ReviseTaskIntent` (the sole
 *      owner of the R0/R1/R2 windows and of the intent/candidate
 *      conflicts) with the derived candidate and intent, its successful
 *      result fully verified as the exact contiguous
 *      `intent → task_revision` suffix progression of the captured
 *      `before` state, then the existing `completePipelineV2ReviseTask`,
 *      its result fully verified as the exact contiguous
 *      `closure → response` progression;
 *    - the progressed retry windows R3 (the exact durable `replanned`
 *      closure without a response) and R4 (the exact durable
 *      `revise_task` response on the active run): the acceptance is
 *      skipped — allowed only when the authoritative state proves the
 *      entire exact accepted prefix for the derived candidate/intent (the
 *      target wait the last and only record of its index carrying the
 *      exact intent digest, exactly one wait-bound candidate revision in
 *      the ledger with exact predecessor/revision/task/wait/intent
 *      bindings and no later revision of the same task, the exact
 *      generation and iteration, the journals exactly at the wait
 *      boundary) and the progression is exactly the replanned closure
 *      without a response or the recorded revise_task response — then
 *      only the completion runs;
 *    - the racing path: an acceptance typed `invalid_state` whose
 *      authoritative `error.state` passes the same full exact-progressed
 *      verification AND is the exact durable suffix progression of the
 *      captured `before` state continues with the completion; every other
 *      error is re-thrown unchanged by object identity.
 *
 * The controller dispatches nothing itself, performs no filesystem work of
 * its own, never calls the reducer and owns no store traversal, parser,
 * compiler, digest builder, publisher or response path: the five composed
 * facades and the single trusted compiled-stage resolver remain the only
 * owners of durable side effects and of the task/intent/response
 * substrates. The unified result is flat, deep-frozen and content-free.
 *
 * Runtime export surface (public module) is exactly two keys:
 * `PipelineV2ReviseTaskInterventionControllerError` and
 * `applyPipelineV2ReviseTaskIntervention`. The internal core module exports
 * exactly the error, the frozen production ops and one with-ops entrypoint.
 *
 * Own failure reasons are exactly `invalid_options | invalid_state |
 * invalid_result`; downstream typed and unexpected errors pass through by
 * object identity and are never classified from message text. Diagnostics
 * are content-free: the caller task body, prepared manifests, canonical
 * JSON, digest values, paths, environment values and credentials never
 * enter them; hostile extra options fields are never read.
 *
 * The controller ends its work at the active/running planning boundary:
 * the architect execution, the next plan revision, the replanned
 * generation/stage/transition opening and the resume are later increments
 * and stay unwired, as do coordinator/runner/CLI wiring, the second half
 * of the revise flow, migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ReviseTaskInterventionControllerFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "invalid_result";

export class PipelineV2ReviseTaskInterventionControllerError extends Error {
  readonly reason: PipelineV2ReviseTaskInterventionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReviseTaskInterventionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReviseTaskInterventionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam passed through unchanged to the composed
 * controllers; the production `PipelineV2RunStateSink` satisfies it
 * without an adapter. The intervention itself reads the authoritative
 * `snapshot` exactly once (the captured `before` state) and never reads it
 * again: every verified state comes from a composed facade's result.
 */
export interface PipelineV2ReviseTaskInterventionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface ApplyPipelineV2ReviseTaskInterventionOptions {
  /** The trusted provenance-backed resolved pipeline (runtime resource). */
  readonly pipeline: ResolvedPipelineV2;
  /** The orchestrator-owned run root (runtime resource). */
  readonly runRoot: string;
  /** The durable run state sink (runtime resource). */
  readonly sink: PipelineV2ReviseTaskInterventionControllerSink;
  /** User policy scalar: the durable run id. */
  readonly runId: string;
  /** User policy scalar: the open wait journal index to respond to. */
  readonly waitIndex: number;
  /** User policy scalar: the task to revise (never derivable from the run). */
  readonly taskId: string;
  /** User policy content: the revised task body (the only content input). */
  readonly taskBody: string;
}

export interface AppliedPipelineV2ReviseTaskIntervention {
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly request_sha256: string;
  readonly response_sha256: string;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_sha256: string;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly action_id: "revise_task";
  readonly action_to: string;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call structural ops of the internal core: the five composed
 * facades, bound by one frozen production object. Tests inject their own
 * per-call object; there is no mutable module-global seam, no installer
 * and no public export of the seam.
 */
export interface PipelineV2ReviseTaskInterventionOps {
  readonly restorePlan: typeof restorePipelineV2AcceptedRunPlan;
  readonly prepareTaskRevision: typeof prepareTaskRevisionManifest;
  readonly prepareIntent: typeof prepareWaitIntent;
  readonly acceptIntent: typeof acceptPipelineV2ReviseTaskIntent;
  readonly completeTask: typeof completePipelineV2ReviseTask;
}

export const productionReviseTaskInterventionOps: PipelineV2ReviseTaskInterventionOps =
  Object.freeze({
    restorePlan: restorePipelineV2AcceptedRunPlan,
    prepareTaskRevision: prepareTaskRevisionManifest,
    prepareIntent: prepareWaitIntent,
    acceptIntent: acceptPipelineV2ReviseTaskIntent,
    completeTask: completePipelineV2ReviseTask,
  });

const REVISE_TASK_ACTION_ID = "revise_task";

const ISO_TIMESTAMP_SHAPE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function interventionError(
  reason: PipelineV2ReviseTaskInterventionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskInterventionControllerError {
  return new PipelineV2ReviseTaskInterventionControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2ReviseTaskInterventionControllerError {
  return interventionError("invalid_options", message, null);
}

function invalidState(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskInterventionControllerError {
  return interventionError("invalid_state", message, state);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskInterventionControllerError {
  return interventionError("invalid_result", message, state);
}

// --- full schema-owned record comparators -----------------------------------
// Every durable record type of the schema v7 run state is compared field by
// field in schema-owned order; nothing is compared by serialization and
// there is no recursive generic comparator. Every comparator is total for
// malformed values (null, primitives, arrays, missing or extra nested
// shapes all compare unequal without throwing).

function orderedActionsEqual(before: unknown, after: unknown): boolean {
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    const beforeAction = before[position];
    const afterAction = after[position];
    if (!isRecord(beforeAction) || !isRecord(afterAction)) {
      return false;
    }
    if (afterAction["id"] !== beforeAction["id"] || afterAction["to"] !== beforeAction["to"]) {
      return false;
    }
  }
  return true;
}

function waitIntentProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["intent_sha256"] === before["intent_sha256"]
  );
}

function waitResponseProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["action_id"] === before["action_id"] &&
    after["response_sha256"] === before["response_sha256"]
  );
}

function waitRecordEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  return (
    after["index"] === before["index"] &&
    after["transition_count"] === before["transition_count"] &&
    after["state_id"] === before["state_id"] &&
    after["reason"] === before["reason"] &&
    after["request_sha256"] === before["request_sha256"] &&
    orderedActionsEqual(before["actions"], after["actions"]) &&
    waitIntentProjectionEquals(before["intent"], after["intent"]) &&
    waitResponseProjectionEquals(before["response"], after["response"])
  );
}

function inputStateEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["id"] === before["id"] &&
    after["type"] === before["type"] &&
    after["protected"] === before["protected"] &&
    after["digest"] === before["digest"]
  );
}

function stringListEquals(before: unknown, after: unknown): boolean {
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (before[position] !== after[position]) {
      return false;
    }
  }
  return true;
}

function sessionCleanupEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["execution"] === before["execution"] &&
    after["tool"] === before["tool"]
  );
}

function agentOutputEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["id"] === before["id"] &&
    after["digest"] === before["digest"]
  );
}

function agentOutputListEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (!agentOutputEquals(before[position], after[position])) {
      return false;
    }
  }
  return true;
}

function decisionResultEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!isRecord(before) || !isRecord(after) || after["status"] !== before["status"]) {
    return false;
  }
  if (before["status"] === "selected") {
    return (
      after["outcome"] === before["outcome"] &&
      after["decision"] === before["decision"] &&
      after["rule_id"] === before["rule_id"] &&
      stringListEquals(before["active_constraint_ids"], after["active_constraint_ids"])
    );
  }
  if (before["status"] === "uncovered") {
    return (
      after["outcome"] === before["outcome"] &&
      stringListEquals(before["active_constraint_ids"], after["active_constraint_ids"])
    );
  }
  if (before["status"] === "inconsistent_facts") {
    return (
      after["outcome"] === before["outcome"] &&
      stringListEquals(before["violated_relation_ids"], after["violated_relation_ids"])
    );
  }
  return (
    after["outcome"] === before["outcome"] &&
    after["reason"] === before["reason"] &&
    after["fact_id"] === before["fact_id"] &&
    after["actual_type"] === before["actual_type"]
  );
}

function executionRecordEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before["index"] ||
    after["type"] !== before["type"] ||
    after["state_id"] !== before["state_id"] ||
    after["execution_role"] !== before["execution_role"] ||
    after["phase"] !== before["phase"] ||
    after["iteration_index"] !== before["iteration_index"]
  ) {
    return false;
  }
  if (before["type"] === "agent") {
    return (
      after["attempt"] === before["attempt"] &&
      after["profile"] === before["profile"] &&
      after["execution_session_id"] === before["execution_session_id"] &&
      after["tool_session_id"] === before["tool_session_id"] &&
      sessionCleanupEquals(before["session_cleanup"], after["session_cleanup"]) &&
      agentOutputListEquals(before["outputs"], after["outputs"]) &&
      after["failure_reason"] === before["failure_reason"]
    );
  }
  return (
    after["input_digest"] === before["input_digest"] &&
    decisionResultEquals(before["result"], after["result"]) &&
    after["failure_reason"] === before["failure_reason"]
  );
}

function transitionEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["from"] === before["from"] &&
    after["outcome"] === before["outcome"] &&
    after["to"] === before["to"] &&
    after["execution_index"] === before["execution_index"]
  );
}

function grantEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["generation_index"] === before["generation_index"] &&
    after["wait_index"] === before["wait_index"] &&
    after["intent_sha256"] === before["intent_sha256"] &&
    after["additional_iterations"] === before["additional_iterations"]
  );
}

function taskRevisionEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["task_id"] === before["task_id"] &&
    after["revision"] === before["revision"] &&
    after["sha256"] === before["sha256"] &&
    after["previous_sha256"] === before["previous_sha256"] &&
    after["wait_index"] === before["wait_index"] &&
    after["intent_sha256"] === before["intent_sha256"]
  );
}

function planRecordEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["revision"] === before["revision"] &&
    after["sha256"] === before["sha256"] &&
    after["previous_sha256"] === before["previous_sha256"] &&
    after["origin_execution"] === before["origin_execution"]
  );
}

function closureProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["by"] === before["by"] &&
    after["closed_transition_count"] === before["closed_transition_count"] &&
    after["wait_index"] === before["wait_index"]
  );
}

function openIterationProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["index"] === before["index"] &&
    after["opened_transition_count"] === before["opened_transition_count"]
  );
}

function iterationRecordEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  return (
    after["index"] === before["index"] &&
    after["opened_transition_count"] === before["opened_transition_count"] &&
    closureProjectionEquals(before["closed"], after["closed"])
  );
}

function regionListEquals(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
  field: string,
  equals: (before: unknown, after: unknown) => boolean,
): boolean {
  const beforeList = before[field];
  const afterList = after[field];
  if (!Array.isArray(beforeList) || !Array.isArray(afterList) || beforeList.length !== afterList.length) {
    return false;
  }
  for (let position = 0; position < beforeList.length; position += 1) {
    if (!equals(beforeList[position], afterList[position])) {
      return false;
    }
  }
  return true;
}

function runOutputStateEquals(before: unknown, after: unknown): boolean {
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["id"] === before["id"] &&
    after["type"] === before["type"] &&
    after["required"] === before["required"] &&
    after["present"] === before["present"] &&
    after["digest"] === before["digest"]
  );
}

function runOutputsListEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  if (!Array.isArray(before) || !Array.isArray(after) || before.length !== after.length) {
    return false;
  }
  for (let position = 0; position < before.length; position += 1) {
    if (!runOutputStateEquals(before[position], after[position])) {
      return false;
    }
  }
  return true;
}

function terminalStateEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["state_id"] === before["state_id"] &&
    after["result"] === before["result"]
  );
}

function failureStateEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return isRecord(before) && isRecord(after) && after["reason"] === before["reason"];
}

function pipelineIdentityEquals(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  return (
    after["schema_version"] === before["schema_version"] &&
    after["bundle_root"] === before["bundle_root"] &&
    after["execution_snapshot_sha256"] === before["execution_snapshot_sha256"] &&
    after["entry_state"] === before["entry_state"] &&
    after["max_transitions"] === before["max_transitions"]
  );
}

/**
 * The exact field equality of one closed stage GENERATION projection
 * (`{by, closed_transition_count}` — no wait index; the wait-bound
 * wait_index lives on the iteration's closure). The validator rebuilds
 * fresh nested records, so equality is structural, never by reference.
 */
function generationClosedProjectionEquals(before: unknown, after: unknown): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(before) &&
    isRecord(after) &&
    after["by"] === before["by"] &&
    after["closed_transition_count"] === before["closed_transition_count"]
  );
}

function generationRecordUnchanged(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  if (
    after["index"] !== before["index"] ||
    after["stage_id"] !== before["stage_id"] ||
    after["stage_position"] !== before["stage_position"] ||
    after["template_id"] !== before["template_id"] ||
    after["plan_sha256"] !== before["plan_sha256"] ||
    after["initial_budget"] !== before["initial_budget"] ||
    after["opened_transition_count"] !== before["opened_transition_count"] ||
    after["iteration_count"] !== before["iteration_count"] ||
    !generationClosedProjectionEquals(before["closed"], after["closed"])
  ) {
    return false;
  }
  const beforeIterations = before["iterations"];
  const afterIterations = after["iterations"];
  if (!Array.isArray(beforeIterations) || !Array.isArray(afterIterations) || beforeIterations.length !== afterIterations.length) {
    return false;
  }
  for (let position = 0; position < beforeIterations.length; position += 1) {
    if (!iterationRecordEquals(beforeIterations[position], afterIterations[position])) {
      return false;
    }
  }
  return openIterationProjectionEquals(before["open_iteration"], after["open_iteration"]);
}

/**
 * The full unchanged-state equality of two schema v7 run states: every
 * schema-owned top-level field and every element of every journal is
 * pinned field by field in schema-owned order. Total for malformed
 * values; nothing is compared by serialization.
 */
function statesFullyEqual(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  const left = before as unknown as Record<string, unknown>;
  const right = after as unknown as Record<string, unknown>;
  if (
    right["schema_version"] !== left["schema_version"] ||
    right["revision"] !== left["revision"] ||
    right["run_id"] !== left["run_id"] ||
    right["status"] !== left["status"] ||
    right["phase"] !== left["phase"] ||
    right["started_at"] !== left["started_at"] ||
    right["updated_at"] !== left["updated_at"] ||
    !terminalStateEquals(left["terminal"], right["terminal"]) ||
    !runOutputsListEquals(left["run_outputs"], right["run_outputs"]) ||
    !failureStateEquals(left["failure"], right["failure"])
  ) {
    return false;
  }
  if (!pipelineIdentityEquals(left["pipeline"], right["pipeline"])) {
    return false;
  }
  if (!isRecord(left["cursor"]) || !isRecord(right["cursor"])) {
    return false;
  }
  const leftCursor = left["cursor"] as Record<string, unknown>;
  const rightCursor = right["cursor"] as Record<string, unknown>;
  if (
    rightCursor["current_state"] !== leftCursor["current_state"] ||
    rightCursor["transition_count"] !== leftCursor["transition_count"]
  ) {
    return false;
  }
  if (!regionListEquals(left, right, "inputs", inputStateEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "executions", executionRecordEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "transitions", transitionEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "generations", generationRecordUnchanged)) {
    return false;
  }
  if (!regionListEquals(left, right, "task_revisions", taskRevisionEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "plan_revisions", planRecordEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "grants", grantEquals)) {
    return false;
  }
  if (!regionListEquals(left, right, "waits", waitRecordEquals)) {
    return false;
  }
  return true;
}

// --- the derived intervention policy ----------------------------------------

interface InterventionPolicy {
  readonly runId: string;
  readonly waitIndex: number;
  readonly taskId: string;
  readonly candidateRevision: number;
  readonly candidateSha256: string;
  readonly candidatePreviousSha256: string;
  readonly intentSha256: string;
  /** The target wait's request manifest digest (durable record). */
  readonly requestSha256: string;
  /** The committed transition count at the wait boundary. */
  readonly waitTransitionCount: number;
  /** The state the wait was entered at (a safe durable id). */
  readonly waitStateId: string;
  /** The declared `revise_task` routing target. */
  readonly actionTo: string;
  /** The derived generation's durable index. */
  readonly generationIndex: number;
  /** The derived generation's last (target) iteration index. */
  readonly iterationIndex: number;
}

interface TargetWait {
  readonly wait: Record<string, unknown>;
  readonly transitionCount: number;
  readonly requestSha256: string;
  readonly stateId: string;
  readonly actionTo: string;
}

/**
 * Locates the target wait by one full journal pass: every viewed entry
 * must be a record, the caller wait index must occur exactly once and at
 * the last position, and the record must carry well-shaped base fields
 * and declare exactly the `revise_task` action (its `to` is the only
 * accepted routing target). Total for malformed values.
 */
function findTargetWait(state: Record<string, unknown>, runId: string, waitIndex: number): TargetWait | null {
  if (state["run_id"] !== runId) {
    return null;
  }
  const waits = state["waits"];
  if (!Array.isArray(waits) || waits.length === 0) {
    return null;
  }
  let position = -1;
  for (let index = 0; index < waits.length; index += 1) {
    const entry = waits[index];
    if (!isRecord(entry)) {
      return null;
    }
    if (entry["index"] === waitIndex) {
      if (position !== -1) {
        return null;
      }
      position = index;
    }
  }
  if (position !== waits.length - 1) {
    return null;
  }
  const wait = waits[position] as Record<string, unknown>;
  const transitionCount = wait["transition_count"];
  if (!isPositiveSafeInteger(transitionCount)) {
    return null;
  }
  const requestSha256 = wait["request_sha256"];
  if (!isLowercaseSha256(requestSha256)) {
    return null;
  }
  const stateId = wait["state_id"];
  if (!isString(stateId)) {
    return null;
  }
  const actions = wait["actions"];
  if (!Array.isArray(actions)) {
    return null;
  }
  let actionTo: string | undefined;
  for (const action of actions) {
    if (!isRecord(action) || !isString(action["id"]) || !isString(action["to"])) {
      return null;
    }
    if (action["id"] === REVISE_TASK_ACTION_ID) {
      if (actionTo !== undefined) {
        return null;
      }
      actionTo = action["to"];
    }
  }
  if (actionTo === undefined) {
    return null;
  }
  return { wait, transitionCount, requestSha256, stateId, actionTo };
}

/**
 * Locates the derived generation record by its durable index (the loader
 * keeps the journal positions and the record indexes contiguous from 1;
 * the lookup is defensive and requires the exact position match).
 */
function findGenerationRecord(state: Record<string, unknown>, generationIndex: number): Record<string, unknown> | null {
  const generations = state["generations"];
  if (!Array.isArray(generations) || generations.length < generationIndex) {
    return null;
  }
  const record = generations[generationIndex - 1];
  if (!isRecord(record) || record["index"] !== generationIndex) {
    return null;
  }
  return record;
}

/**
 * The last iteration record of the located generation, defensively
 * shape-checked (the loader keeps `iteration_count` equal to the journal
 * length).
 */
function findTargetIteration(generation: Record<string, unknown>): Record<string, unknown> | null {
  const iterations = generation["iterations"];
  const iterationCount = generation["iteration_count"];
  if (
    !Array.isArray(iterations) ||
    iterations.length === 0 ||
    !isPositiveSafeInteger(iterationCount) ||
    iterationCount !== iterations.length
  ) {
    return null;
  }
  const iteration = iterations[iterations.length - 1];
  if (!isRecord(iteration)) {
    return null;
  }
  return iteration;
}

interface TaskLedgerScan {
  /** Records of the target wait bound to any task. */
  readonly waitBoundCount: number;
  /** The exact wait-bound candidate revision record exists. */
  readonly candidateExact: boolean;
  /** A later revision of the caller task exists in the ledger. */
  readonly laterTaskRevision: boolean;
}

function scanTaskLedger(state: Record<string, unknown>, policy: InterventionPolicy): TaskLedgerScan | null {
  const ledger = state["task_revisions"];
  if (!Array.isArray(ledger)) {
    return null;
  }
  let waitBoundCount = 0;
  let candidateExact = false;
  let laterTaskRevision = false;
  for (const record of ledger) {
    if (!isRecord(record)) {
      return null;
    }
    if (record["wait_index"] === policy.waitIndex) {
      waitBoundCount += 1;
    }
    if (
      record["task_id"] === policy.taskId &&
      record["revision"] === policy.candidateRevision &&
      record["sha256"] === policy.candidateSha256 &&
      record["previous_sha256"] === policy.candidatePreviousSha256 &&
      record["wait_index"] === policy.waitIndex &&
      record["intent_sha256"] === policy.intentSha256
    ) {
      candidateExact = true;
    }
    if (
      record["task_id"] === policy.taskId &&
      isPositiveSafeInteger(record["revision"]) &&
      (record["revision"] as number) > policy.candidateRevision
    ) {
      laterTaskRevision = true;
    }
  }
  return { waitBoundCount, candidateExact, laterTaskRevision };
}

interface SuffixSteps {
  readonly intentPresent: boolean;
  readonly candidatePresent: boolean;
  readonly closurePresent: boolean;
  readonly responsePresent: boolean;
  readonly answered: boolean;
  readonly waitBoundTaskCount: number;
  readonly laterTaskRevision: boolean;
}

/**
 * Computes the intervention-suffix step projections of one state for the
 * derived policy. Total for malformed values; returns null when the state
 * does not carry the target wait or the derived generation/iteration.
 */
function stepsOf(state: Record<string, unknown>, policy: InterventionPolicy): SuffixSteps | null {
  const declared = findTargetWait(state, policy.runId, policy.waitIndex);
  if (declared === null) {
    return null;
  }
  const wait = declared.wait;
  const intentValue = wait["intent"];
  const intentPresent = isRecord(intentValue) && intentValue["intent_sha256"] === policy.intentSha256;
  const responseValue = wait["response"];
  const answered = responseValue !== undefined;
  const responsePresent =
    isRecord(responseValue) &&
    responseValue["action_id"] === REVISE_TASK_ACTION_ID &&
    isLowercaseSha256(responseValue["response_sha256"]);
  const ledger = scanTaskLedger(state, policy);
  if (ledger === null) {
    return null;
  }
  const generation = findGenerationRecord(state, policy.generationIndex);
  if (generation === null) {
    return null;
  }
  const iteration = findTargetIteration(generation);
  if (iteration === null) {
    return null;
  }
  const closed = iteration["closed"];
  const closurePresent =
    isRecord(closed) &&
    closed["by"] === "replanned" &&
    closed["wait_index"] === policy.waitIndex &&
    closed["closed_transition_count"] === policy.waitTransitionCount;
  return {
    intentPresent,
    candidatePresent: ledger.candidateExact,
    closurePresent,
    responsePresent,
    answered,
    waitBoundTaskCount: ledger.waitBoundCount,
    laterTaskRevision: ledger.laterTaskRevision,
  };
}

/**
 * The exact appended task-ledger record of the derived candidate: exactly
 * one record appended at the end with the exact candidate bindings.
 */
function taskLedgerAppendsExactly(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
  policy: InterventionPolicy,
): boolean {
  const beforeList = before["task_revisions"];
  const afterList = after["task_revisions"];
  if (!Array.isArray(beforeList) || !Array.isArray(afterList) || afterList.length !== beforeList.length + 1) {
    return false;
  }
  for (let position = 0; position < beforeList.length; position += 1) {
    if (!taskRevisionEquals(beforeList[position], afterList[position])) {
      return false;
    }
  }
  const appended = afterList[afterList.length - 1];
  return (
    isRecord(appended) &&
    appended["index"] === beforeList.length + 1 &&
    appended["task_id"] === policy.taskId &&
    appended["revision"] === policy.candidateRevision &&
    appended["sha256"] === policy.candidateSha256 &&
    appended["previous_sha256"] === policy.candidatePreviousSha256 &&
    appended["wait_index"] === policy.waitIndex &&
    appended["intent_sha256"] === policy.intentSha256
  );
}

function generationBaseUnchanged(before: unknown, after: unknown): boolean {
  if (!isRecord(before) || !isRecord(after)) {
    return false;
  }
  return (
    after["index"] === before["index"] &&
    after["stage_id"] === before["stage_id"] &&
    after["stage_position"] === before["stage_position"] &&
    after["template_id"] === before["template_id"] &&
    after["plan_sha256"] === before["plan_sha256"] &&
    after["initial_budget"] === before["initial_budget"] &&
    after["opened_transition_count"] === before["opened_transition_count"] &&
    after["iteration_count"] === before["iteration_count"] &&
    after["closed"] === undefined &&
    before["closed"] === undefined
  );
}

/**
 * The wait journal region of the delta: the journal length is pinned; the
 * non-target records are pinned element by element; the target wait's
 * base fields and declared actions are pinned, and its intent/response
 * projections change only by the exact allowed steps.
 */
function waitsRegionEquals(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
  policy: InterventionPolicy,
  beforeSteps: SuffixSteps,
  afterSteps: SuffixSteps,
): boolean {
  const beforeList = before["waits"];
  const afterList = after["waits"];
  if (!Array.isArray(beforeList) || !Array.isArray(afterList) || afterList.length !== beforeList.length) {
    return false;
  }
  const intentAppended = afterSteps.intentPresent && !beforeSteps.intentPresent;
  const responseAppended = afterSteps.responsePresent && !beforeSteps.responsePresent;
  for (let position = 0; position < afterList.length; position += 1) {
    const beforeWait = beforeList[position];
    const afterWait = afterList[position];
    if (!isRecord(beforeWait) || !isRecord(afterWait)) {
      return false;
    }
    if (beforeWait["index"] !== policy.waitIndex) {
      if (!waitRecordEquals(beforeWait, afterWait)) {
        return false;
      }
      continue;
    }
    if (
      afterWait["index"] !== beforeWait["index"] ||
      afterWait["transition_count"] !== beforeWait["transition_count"] ||
      afterWait["state_id"] !== beforeWait["state_id"] ||
      afterWait["reason"] !== beforeWait["reason"] ||
      afterWait["request_sha256"] !== beforeWait["request_sha256"] ||
      !orderedActionsEqual(beforeWait["actions"], afterWait["actions"])
    ) {
      return false;
    }
    if (intentAppended) {
      if (beforeWait["intent"] !== undefined) {
        return false;
      }
      const intent = afterWait["intent"];
      if (!isRecord(intent) || intent["intent_sha256"] !== policy.intentSha256) {
        return false;
      }
    } else if (!waitIntentProjectionEquals(beforeWait["intent"], afterWait["intent"])) {
      return false;
    }
    if (responseAppended) {
      if (beforeWait["response"] !== undefined) {
        return false;
      }
      const response = afterWait["response"];
      if (
        !isRecord(response) ||
        response["action_id"] !== REVISE_TASK_ACTION_ID ||
        !isLowercaseSha256(response["response_sha256"])
      ) {
        return false;
      }
    } else if (!waitResponseProjectionEquals(beforeWait["response"], afterWait["response"])) {
      return false;
    }
  }
  return true;
}

/**
 * The generation journal region of the delta: the journal length is
 * pinned; the historical generations and every non-target iteration are
 * pinned element by element; the derived generation's base fields stay
 * unchanged and its target iteration's closure changes only by the exact
 * allowed closure step (the open-iteration projection is removed exactly
 * when the closure is appended).
 */
function generationsRegionEquals(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
  policy: InterventionPolicy,
  beforeSteps: SuffixSteps,
  afterSteps: SuffixSteps,
): boolean {
  const beforeList = before["generations"];
  const afterList = after["generations"];
  if (!Array.isArray(beforeList) || !Array.isArray(afterList) || afterList.length !== beforeList.length) {
    return false;
  }
  const closureAppended = afterSteps.closurePresent && !beforeSteps.closurePresent;
  for (let position = 0; position < afterList.length; position += 1) {
    const beforeGeneration = beforeList[position];
    const afterGeneration = afterList[position];
    if (!isRecord(beforeGeneration) || !isRecord(afterGeneration)) {
      return false;
    }
    if (position !== policy.generationIndex - 1) {
      if (!generationRecordUnchanged(beforeGeneration, afterGeneration)) {
        return false;
      }
      continue;
    }
    if (!generationBaseUnchanged(beforeGeneration, afterGeneration)) {
      return false;
    }
    const beforeIterations = beforeGeneration["iterations"];
    const afterIterations = afterGeneration["iterations"];
    if (
      !Array.isArray(beforeIterations) ||
      !Array.isArray(afterIterations) ||
      afterIterations.length !== beforeIterations.length ||
      beforeGeneration["iteration_count"] !== beforeIterations.length ||
      afterGeneration["iteration_count"] !== afterIterations.length
    ) {
      return false;
    }
    const targetPosition = beforeIterations.length - 1;
    for (let index = 0; index < afterIterations.length; index += 1) {
      const beforeIteration = beforeIterations[index];
      const afterIteration = afterIterations[index];
      if (!isRecord(beforeIteration) || !isRecord(afterIteration)) {
        return false;
      }
      if (
        afterIteration["index"] !== beforeIteration["index"] ||
        afterIteration["opened_transition_count"] !== beforeIteration["opened_transition_count"]
      ) {
        return false;
      }
      if (index === targetPosition) {
        if (closureAppended) {
          const closed = afterIteration["closed"];
          if (
            !isRecord(closed) ||
            closed["by"] !== "replanned" ||
            closed["wait_index"] !== policy.waitIndex ||
            closed["closed_transition_count"] !== policy.waitTransitionCount
          ) {
            return false;
          }
          if (beforeIteration["closed"] !== undefined) {
            return false;
          }
        } else if (!closureProjectionEquals(beforeIteration["closed"], afterIteration["closed"])) {
          return false;
        }
      } else if (!closureProjectionEquals(beforeIteration["closed"], afterIteration["closed"])) {
        return false;
      }
    }
    if (closureAppended) {
      if (afterGeneration["open_iteration"] !== undefined || beforeGeneration["open_iteration"] === undefined) {
        return false;
      }
    } else if (!openIterationProjectionEquals(beforeGeneration["open_iteration"], afterGeneration["open_iteration"])) {
      return false;
    }
  }
  return true;
}

interface AllowedSteps {
  readonly intent: boolean;
  readonly task: boolean;
  readonly closure: boolean;
  readonly response: boolean;
}

/**
 * The full durable delta comparator between the verified base state and a
 * candidate after state: every schema-owned region is pinned field by
 * field; the only permitted changes are the exact contiguous suffix steps
 * from the allowed set (each appended at most once, in the fixed order
 * `intent → task revision → closure → response`), with the revision delta
 * equal to the number of appended steps and the `updated_at` accounting
 * exact (the zero-step recognition preserves it; a real dispatch refreshes
 * it to any schema-valid timestamp). Returns the appended step count, or
 * -1 on any mismatch. Total for malformed values.
 */
function compareDelta(
  before: PipelineV2RunState,
  afterValue: unknown,
  policy: InterventionPolicy,
  allowed: AllowedSteps,
): number {
  const beforeRecord = before as unknown as Record<string, unknown>;
  if (!isRecord(afterValue)) {
    return -1;
  }
  const after = afterValue as Record<string, unknown>;
  const beforeSteps = stepsOf(beforeRecord, policy);
  const afterSteps = stepsOf(after, policy);
  if (beforeSteps === null || afterSteps === null) {
    return -1;
  }
  if (afterSteps.intentPresent && !beforeSteps.intentPresent && !allowed.intent) {
    return -1;
  }
  if (afterSteps.candidatePresent && !beforeSteps.candidatePresent && !allowed.task) {
    return -1;
  }
  if (afterSteps.closurePresent && !beforeSteps.closurePresent && !allowed.closure) {
    return -1;
  }
  if (afterSteps.responsePresent && !beforeSteps.responsePresent && !allowed.response) {
    return -1;
  }
  if (beforeSteps.intentPresent && !afterSteps.intentPresent) {
    return -1;
  }
  if (beforeSteps.candidatePresent && !afterSteps.candidatePresent) {
    return -1;
  }
  if (beforeSteps.closurePresent && !afterSteps.closurePresent) {
    return -1;
  }
  if (beforeSteps.responsePresent && !afterSteps.responsePresent) {
    return -1;
  }
  let appended = 0;
  if (afterSteps.intentPresent && !beforeSteps.intentPresent) {
    appended += 1;
  }
  if (afterSteps.candidatePresent && !beforeSteps.candidatePresent) {
    appended += 1;
  }
  if (afterSteps.closurePresent && !beforeSteps.closurePresent) {
    appended += 1;
  }
  if (afterSteps.responsePresent && !beforeSteps.responsePresent) {
    appended += 1;
  }
  if (!isPositiveSafeInteger(after["revision"]) || !isPositiveSafeInteger(beforeRecord["revision"])) {
    return -1;
  }
  if ((after["revision"] as number) !== (beforeRecord["revision"] as number) + appended) {
    return -1;
  }
  const updatedAt = after["updated_at"];
  if (!isString(updatedAt) || ISO_TIMESTAMP_SHAPE.exec(updatedAt) === null) {
    return -1;
  }
  if (appended === 0 && updatedAt !== beforeRecord["updated_at"]) {
    return -1;
  }
  if (
    after["schema_version"] !== beforeRecord["schema_version"] ||
    !isString(after["run_id"]) ||
    after["run_id"] !== beforeRecord["run_id"] ||
    after["started_at"] !== beforeRecord["started_at"]
  ) {
    return -1;
  }
  const responseAppended = afterSteps.responsePresent && !beforeSteps.responsePresent;
  if (responseAppended) {
    if (after["status"] !== "active" || after["phase"] !== "running") {
      return -1;
    }
  } else if (after["status"] !== beforeRecord["status"] || after["phase"] !== beforeRecord["phase"]) {
    return -1;
  }
  if (
    after["terminal"] !== beforeRecord["terminal"] ||
    after["run_outputs"] !== beforeRecord["run_outputs"] ||
    after["failure"] !== beforeRecord["failure"]
  ) {
    return -1;
  }
  if (!pipelineIdentityEquals(beforeRecord["pipeline"], after["pipeline"])) {
    return -1;
  }
  if (!regionListEquals(beforeRecord, after, "inputs", inputStateEquals)) {
    return -1;
  }
  if (!regionListEquals(beforeRecord, after, "executions", executionRecordEquals)) {
    return -1;
  }
  if (!regionListEquals(beforeRecord, after, "transitions", transitionEquals)) {
    return -1;
  }
  if (!regionListEquals(beforeRecord, after, "plan_revisions", planRecordEquals)) {
    return -1;
  }
  if (!regionListEquals(beforeRecord, after, "grants", grantEquals)) {
    return -1;
  }
  if (afterSteps.candidatePresent && !beforeSteps.candidatePresent) {
    if (!taskLedgerAppendsExactly(beforeRecord, after, policy)) {
      return -1;
    }
  } else if (!regionListEquals(beforeRecord, after, "task_revisions", taskRevisionEquals)) {
    return -1;
  }
  if (!generationsRegionEquals(beforeRecord, after, policy, beforeSteps, afterSteps)) {
    return -1;
  }
  if (!waitsRegionEquals(beforeRecord, after, policy, beforeSteps, afterSteps)) {
    return -1;
  }
  if (responseAppended) {
    const beforeCursor = beforeRecord["cursor"];
    const afterCursor = after["cursor"];
    if (!isRecord(beforeCursor) || !isRecord(afterCursor)) {
      return -1;
    }
    if (
      afterCursor["transition_count"] !== beforeCursor["transition_count"] ||
      afterCursor["current_state"] !== policy.actionTo
    ) {
      return -1;
    }
  } else {
    const beforeCursor = beforeRecord["cursor"];
    const afterCursor = after["cursor"];
    if (!isRecord(beforeCursor) || !isRecord(afterCursor)) {
      return -1;
    }
    if (
      afterCursor["transition_count"] !== beforeCursor["transition_count"] ||
      afterCursor["current_state"] !== beforeCursor["current_state"]
    ) {
      return -1;
    }
  }
  return appended;
}

// --- verified results --------------------------------------------------------

interface VerifiedRestore {
  readonly state: PipelineV2RunState;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
}

/**
 * The full defensive verification of the restore's successful result: the
 * restored state is the exact unchanged authoritative state on every
 * schema-owned field (the restore is read-only, `updated_at` included —
 * not identity-only and not length-only), and the restored compiled plan
 * is a record. Malformed shapes are this layer's own `invalid_result`,
 * never a leaked `TypeError`.
 */
function verifyRestoreResult(resultValue: unknown, before: PipelineV2RunState): VerifiedRestore {
  if (!isRecord(resultValue)) {
    throw invalidResult("the restored run plan result is not a record", before);
  }
  const compiledPlanValue = resultValue["compiled_plan"];
  if (!isRecord(compiledPlanValue)) {
    throw invalidResult("the restored run plan result carries no compiled plan", before);
  }
  const stateValue = resultValue["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the restored run plan result carries no durable run state", before);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  if (!statesFullyEqual(before, state)) {
    throw invalidResult("the restored run plan result does not carry the unchanged authoritative durable state", before);
  }
  return { state, compiledPlan: compiledPlanValue as unknown as CompiledPipelineV2RunPlan };
}

/**
 * The full defensive verification of the acceptance's successful result
 * against the verified base state and the internally derived
 * candidate/intent: the flat fields are exact, the authoritative state is
 * the exact contiguous `intent → task_revision` suffix progression of the
 * base state (the revision delta equal to the number of appended steps,
 * the zero-step recognition preserving `updated_at`, every other durable
 * region pinned field by field), and both the exact intent and the exact
 * candidate are durably present. No healing rewrite passes.
 */
function verifyAcceptanceResult(
  resultValue: unknown,
  before: PipelineV2RunState,
  policy: InterventionPolicy,
): PipelineV2RunState {
  if (!isRecord(resultValue)) {
    throw invalidResult("the accepted revise task intent result is not a record", before);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    result["wait_index"] !== policy.waitIndex ||
    result["intent_sha256"] !== policy.intentSha256 ||
    result["task_id"] !== policy.taskId ||
    result["task_revision"] !== policy.candidateRevision ||
    result["task_sha256"] !== policy.candidateSha256
  ) {
    throw invalidResult("the accepted revise task intent result does not match the derived candidate and intent", before);
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the accepted revise task intent result carries no durable run state", before);
  }
  const appended = compareDelta(before, stateValue, policy, { intent: true, task: true, closure: false, response: false });
  if (appended < 0) {
    throw invalidResult("the accepted revise task intent state is not the exact durable progression of the verified state", before);
  }
  const afterSteps = stepsOf(stateValue, policy);
  if (afterSteps === null || !afterSteps.intentPresent || !afterSteps.candidatePresent) {
    throw invalidResult("the accepted revise task intent state does not carry the exact intent and candidate", before);
  }
  return stateValue as unknown as PipelineV2RunState;
}

/**
 * The full defensive verification of the completion's successful result
 * against the verified accepted or progressed state: the flat
 * wait/task/intent/request/response/action fields are exact, the
 * authoritative state is the exact contiguous `closure → response`
 * progression (the zero-step recognition preserving `updated_at`), the
 * exact replanned closure and the exact revise_task response are durably
 * present, the cursor sits on the declared routing target with the
 * transition count unchanged, the journals stay exactly at the wait
 * boundary, and no task/plan/generation/execution/transition/wait history
 * is rewritten or extended.
 */
function verifyCompletionResult(
  resultValue: unknown,
  verified: PipelineV2RunState,
  policy: InterventionPolicy,
): AppliedPipelineV2ReviseTaskIntervention {
  if (!isRecord(resultValue)) {
    throw invalidResult("the completed revise task result is not a record", verified);
  }
  const result = resultValue as Record<string, unknown>;
  if (
    result["wait_index"] !== policy.waitIndex ||
    result["intent_sha256"] !== policy.intentSha256 ||
    result["task_id"] !== policy.taskId ||
    result["task_revision"] !== policy.candidateRevision ||
    result["task_sha256"] !== policy.candidateSha256 ||
    result["generation_index"] !== policy.generationIndex ||
    result["iteration_index"] !== policy.iterationIndex ||
    result["request_sha256"] !== policy.requestSha256 ||
    result["action_id"] !== REVISE_TASK_ACTION_ID ||
    result["action_to"] !== policy.actionTo
  ) {
    throw invalidResult("the completed revise task result does not match the derived intervention bindings", verified);
  }
  const responseSha256 = result["response_sha256"];
  if (!isLowercaseSha256(responseSha256)) {
    throw invalidResult("the completed revise task result carries a malformed response digest", verified);
  }
  const stateValue = result["state"];
  if (!isRecord(stateValue)) {
    throw invalidResult("the completed revise task result carries no durable run state", verified);
  }
  const appended = compareDelta(verified, stateValue, policy, { intent: false, task: false, closure: true, response: true });
  if (appended < 0) {
    throw invalidResult("the completed revise task state is not the exact durable progression of the verified state", verified);
  }
  const state = stateValue as unknown as PipelineV2RunState;
  const afterSteps = stepsOf(state as unknown as Record<string, unknown>, policy);
  if (
    afterSteps === null ||
    !afterSteps.intentPresent ||
    !afterSteps.candidatePresent ||
    !afterSteps.closurePresent ||
    !afterSteps.responsePresent
  ) {
    throw invalidResult("the completed revise task state does not carry the exact closure and response", verified);
  }
  const declared = findTargetWait(state as unknown as Record<string, unknown>, policy.runId, policy.waitIndex);
  if (declared === null) {
    throw invalidResult("the completed revise task state does not carry the answered target wait", verified);
  }
  const responseRecord = declared.wait["response"];
  if (!isRecord(responseRecord) || responseRecord["response_sha256"] !== responseSha256) {
    throw invalidResult("the completed revise task response digest does not match the durable response record", verified);
  }
  return deepFreezeValue({
    wait_index: result["wait_index"],
    intent_sha256: result["intent_sha256"],
    request_sha256: result["request_sha256"],
    response_sha256: responseSha256,
    task_id: result["task_id"],
    task_revision: result["task_revision"],
    task_sha256: result["task_sha256"],
    generation_index: result["generation_index"],
    iteration_index: result["iteration_index"],
    action_id: result["action_id"],
    action_to: result["action_to"],
    state,
  }) as unknown as AppliedPipelineV2ReviseTaskIntervention;
}

// --- derivation and retry classification -------------------------------------

interface DerivedIntervention {
  readonly policy: InterventionPolicy;
  readonly candidate: PreparedPipelineV2RunTaskRevision;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

/**
 * The derivation, entirely from the verified restored state, the trusted
 * compiled plan and the caller scalars: the last and only open stage
 * generation bound to the last accepted plan revision, its exact compiled
 * stage (the single trusted resolver, also the compiled-plan provenance
 * probe, cross-checked against the durable pipeline identity through the
 * hidden originating identity), exactly one task pointer with the caller
 * task id, the next revision, the predecessor digest and the fixed origin
 * — then the candidate task revision and the exact revise task intent
 * through the single manifest preparers, and the target wait's durable
 * binding fields completing the policy.
 */
function deriveIntervention(
  restored: VerifiedRestore,
  runId: string,
  waitIndex: number,
  taskId: string,
  taskBody: string,
  prepareTaskRevision: typeof prepareTaskRevisionManifest,
  prepareIntent: typeof prepareWaitIntent,
): DerivedIntervention {
  const state = restored.state;
  const stateRecord = state as unknown as Record<string, unknown>;
  const generations = stateRecord["generations"];
  if (!Array.isArray(generations) || generations.length === 0) {
    throw invalidState("the run carries no stage generation for the revise task intervention", state);
  }
  let openCount = 0;
  for (const generation of generations) {
    if (!isRecord(generation)) {
      throw invalidState("the stage generation journal carries a malformed record", state);
    }
    if (generation["closed"] === undefined) {
      openCount += 1;
    }
  }
  const lastGeneration = generations[generations.length - 1];
  if (
    lastGeneration === undefined ||
    !isRecord(lastGeneration) ||
    lastGeneration["closed"] !== undefined ||
    openCount !== 1
  ) {
    throw invalidState("the run does not carry exactly one open last stage generation", state);
  }
  const generationIndex = lastGeneration["index"];
  if (!isPositiveSafeInteger(generationIndex)) {
    throw invalidState("the open stage generation carries a malformed index", state);
  }
  const stageId = lastGeneration["stage_id"];
  if (!isPipelineV2SafeId(stageId)) {
    throw invalidState("the open stage generation carries a malformed stage id", state);
  }
  const planRevisions = stateRecord["plan_revisions"];
  if (!Array.isArray(planRevisions) || planRevisions.length === 0) {
    throw invalidState("the run carries no accepted plan revision", state);
  }
  const lastPlanRecord = planRevisions[planRevisions.length - 1];
  if (!isRecord(lastPlanRecord)) {
    throw invalidState("the plan revision ledger carries a malformed record", state);
  }
  if (lastGeneration["plan_sha256"] !== lastPlanRecord["sha256"]) {
    throw invalidState(
      `the open stage generation ${generationIndex} does not belong to the last accepted plan revision`,
      state,
    );
  }
  let compiledStage: CompiledPipelineV2RunPlanStage;
  try {
    compiledStage = compiledPipelineV2RunPlanStageFor(restored.compiledPlan, stageId);
  } catch (cause) {
    if (cause instanceof PipelineV2CompiledRunPlanError && cause.reason === "invalid_plan") {
      throw invalidResult("the restored run plan result does not carry the real provenance-backed compiled plan", state);
    }
    if (cause instanceof PipelineV2CompiledRunPlanError) {
      throw invalidState(
        `the compiled run plan does not carry the open generation's stage ${JSON.stringify(stageId)}`,
        state,
      );
    }
    throw cause;
  }
  let originIdentityMatches: boolean;
  try {
    originIdentityMatches =
      comparePipelineV2RunIdentity(
        compiledRunPlanOriginIdentity(restored.compiledPlan),
        state.pipeline,
      ).kind === "match";
  } catch (cause) {
    throw invalidResult("the restored run plan result does not carry the real compiled plan identity", state);
  }
  if (!originIdentityMatches) {
    throw invalidResult("the restored compiled plan belongs to a different pipeline identity", state);
  }
  if (
    restored.compiledPlan.plan_revision !== lastPlanRecord["revision"] ||
    restored.compiledPlan.plan_sha256 !== lastPlanRecord["sha256"] ||
    restored.compiledPlan.origin_execution !== lastPlanRecord["origin_execution"] ||
    restored.compiledPlan.run_id !== runId
  ) {
    throw invalidResult("the restored compiled plan does not match the authoritative accepted plan revision", state);
  }
  const pointers = compiledStage.tasks.filter((task) => task.id === taskId);
  if (pointers.length !== 1 || pointers[0] === undefined) {
    throw invalidState(`the stage ${JSON.stringify(stageId)} of the accepted plan does not carry the caller task`, state);
  }
  const pointer = pointers[0];
  const candidateRevision = pointer.revision + 1;
  if (!isPositiveSafeInteger(candidateRevision)) {
    throw invalidState(
      `the derived candidate revision of task ${JSON.stringify(taskId)} overflows the safe integer range`,
      state,
    );
  }
  const candidatePreviousSha256 = pointer.sha256;
  const candidate = prepareTaskRevision({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision: candidateRevision,
    previous_sha256: candidatePreviousSha256,
    origin: "user_response",
    body: taskBody,
  });
  const intent = prepareIntent({
    schema_version: 1,
    kind: "revise_task_intent",
    run_id: runId,
    wait_index: waitIndex,
    task_id: taskId,
    expected_previous_task_sha256: candidatePreviousSha256,
    new_task_revision_sha256: candidate.sha256,
  });
  const declared = findTargetWait(stateRecord, runId, waitIndex);
  if (declared === null) {
    throw invalidState(
      `the run does not carry the caller wait ${waitIndex} as its last record declaring the revise_task action`,
      state,
    );
  }
  const generation = findGenerationRecord(stateRecord, generationIndex);
  const iteration = generation === null ? null : findTargetIteration(generation);
  if (generation === null || iteration === null) {
    throw invalidState("the run state does not carry the derived generation and iteration", state);
  }
  const iterationIndex = iteration["index"];
  if (!isPositiveSafeInteger(iterationIndex)) {
    throw invalidState("the target iteration carries a malformed index", state);
  }
  const policy: InterventionPolicy = {
    runId,
    waitIndex,
    taskId,
    candidateRevision,
    candidateSha256: candidate.sha256,
    candidatePreviousSha256,
    intentSha256: intent.sha256,
    requestSha256: declared.requestSha256,
    waitTransitionCount: declared.transitionCount,
    waitStateId: declared.stateId,
    actionTo: declared.actionTo,
    generationIndex,
    iterationIndex,
  };
  return { policy, candidate, intent };
}

/**
 * The exact accepted-prefix verification of a progressed state: the exact
 * intent on the target wait, exactly one wait-bound task record in the
 * ledger and it is the exact candidate with exact
 * predecessor/revision/task/wait/intent bindings, and no later revision
 * of the caller task.
 */
function verifyProgressedPrefix(
  state: PipelineV2RunState,
  policy: InterventionPolicy,
  steps: SuffixSteps,
): void {
  if (!steps.intentPresent) {
    throw invalidState(
      `the target wait ${policy.waitIndex} does not carry the exact accepted revise task intent`,
      state,
    );
  }
  if (!steps.candidatePresent || steps.waitBoundTaskCount !== 1 || steps.laterTaskRevision) {
    throw invalidState(
      `the task ledger does not carry exactly the accepted candidate revision of task ${JSON.stringify(policy.taskId)} for wait ${policy.waitIndex}`,
      state,
    );
  }
}

/**
 * The exactness of the derived generation on a progressed (skip-acceptance)
 * state: the generation is still the last durable record and still open,
 * no successor iteration was opened after the replanned closure, and the
 * generation is still bound to the last durable plan revision (no new plan
 * revision, no new generation). A state whose lifecycle moved past the
 * wait boundary in the generation/plan dimension is never a retry.
 */
function verifySkipAcceptanceGenerationExact(
  state: PipelineV2RunState,
  policy: InterventionPolicy,
): void {
  const stateRecord = state as unknown as Record<string, unknown>;
  const generations = stateRecord["generations"];
  if (!Array.isArray(generations) || generations.length !== policy.generationIndex) {
    throw invalidState(
      `the derived stage generation ${policy.generationIndex} is no longer the last durable generation`,
      state,
    );
  }
  const generation = generations[policy.generationIndex - 1];
  if (!isRecord(generation) || generation["closed"] !== undefined) {
    throw invalidState(
      `the derived stage generation ${policy.generationIndex} is closed; the revise task intervention does not apply`,
      state,
    );
  }
  if (generation["open_iteration"] !== undefined) {
    throw invalidState(
      `the stage generation ${policy.generationIndex} still projects an open iteration`,
      state,
    );
  }
  const planRevisions = stateRecord["plan_revisions"];
  const lastPlanRecord = Array.isArray(planRevisions)
    ? planRevisions[planRevisions.length - 1]
    : undefined;
  if (!isRecord(lastPlanRecord) || generation["plan_sha256"] !== lastPlanRecord["sha256"]) {
    throw invalidState(
      `the stage generation ${policy.generationIndex} does not belong to the last durable plan revision`,
      state,
    );
  }
}

/**
 * The retry classification over the authoritative state, without message
 * parsing: the common boundary prerequisites, then the acceptance window
 * (waiting, open wait, open iteration — the acceptance owns R0/R1/R2 and
 * all its conflicts), the progressed R3 window (the exact accepted prefix
 * plus the exact replanned closure without a response) and the progressed
 * R4 window (plus the exact revise_task response on the active run).
 */
function classifyRetryWindow(
  state: PipelineV2RunState,
  policy: InterventionPolicy,
): "acceptance" | "completion" {
  const stateRecord = state as unknown as Record<string, unknown>;
  if (
    stateRecord["terminal"] !== undefined ||
    stateRecord["run_outputs"] !== undefined ||
    stateRecord["failure"] !== undefined
  ) {
    throw invalidState("the run is terminal, publishing or failed; the revise task intervention does not apply", state);
  }
  const declared = findTargetWait(stateRecord, policy.runId, policy.waitIndex);
  if (declared === null) {
    throw invalidState(
      `the run does not carry the caller wait ${policy.waitIndex} as its last record declaring the revise_task action`,
      state,
    );
  }
  const transitions = stateRecord["transitions"];
  const executions = stateRecord["executions"];
  if (
    !Array.isArray(transitions) ||
    !Array.isArray(executions) ||
    transitions.length !== declared.transitionCount ||
    executions.length !== transitions.length
  ) {
    throw invalidState("the transition and execution journals are not exactly at the wait boundary", state);
  }
  const cursor = stateRecord["cursor"];
  if (!isRecord(cursor)) {
    throw invalidState("the run state carries a malformed cursor", state);
  }
  const steps = stepsOf(stateRecord, policy);
  if (steps === null) {
    throw invalidState("the run state does not carry the derived generation and iteration for the intervention", state);
  }
  if (state.status === "waiting" && state.phase === "waiting") {
    if (steps.answered) {
      throw invalidState("the run is waiting but the target wait is answered", state);
    }
    if (cursor["current_state"] !== declared.stateId) {
      throw invalidState("the cursor is not on the wait state at the wait boundary", state);
    }
    if (steps.closurePresent) {
      verifySkipAcceptanceGenerationExact(state, policy);
      verifyProgressedPrefix(state, policy, steps);
      return "completion";
    }
    const generation = findGenerationRecord(stateRecord, policy.generationIndex);
    if (generation === null || generation["open_iteration"] === undefined) {
      throw invalidState(
        `the stage generation ${policy.generationIndex} carries no open iteration for the revise task intervention`,
        state,
      );
    }
    return "acceptance";
  }
  if (state.status === "active" && state.phase === "running") {
    if (!steps.answered) {
      throw invalidState("the run is active but the target wait is not answered; the intervention does not apply", state);
    }
    if (!steps.responsePresent) {
      throw invalidState("the target wait was answered with another action; this is not the revise_task boundary", state);
    }
    if (cursor["current_state"] !== policy.actionTo) {
      throw invalidState("the cursor is not on the declared revise_task routing target", state);
    }
    if (!steps.closurePresent) {
      throw invalidState("the answered wait boundary carries no exact replanned iteration closure", state);
    }
    verifySkipAcceptanceGenerationExact(state, policy);
    verifyProgressedPrefix(state, policy, steps);
    return "completion";
  }
  throw invalidState("the run status does not admit the revise task intervention", state);
}

/**
 * The full progressed-state verification for the skip-acceptance path:
 * the exact retry window classification of the authoritative state and,
 * when a base state is given (the racing path), the exact durable suffix
 * progression of that base state with all four intervention steps
 * allowed.
 */
function verifyProgressedState(
  state: PipelineV2RunState,
  policy: InterventionPolicy,
  base: PipelineV2RunState | null,
): void {
  // Progressed recovery is only the completed boundary: the exact R3/R4
  // windows classify as "completion". Any "acceptance" window (unchanged
  // R0, the durable intent R1, the durable candidate R2) is NOT a
  // progression past the acceptance boundary — recovering into the
  // completion there would run it without the proven closure/response
  // progression.
  const window = classifyRetryWindow(state, policy);
  if (window !== "completion") {
    throw invalidState("the progressed state is still in the acceptance window; the racing reconciliation does not apply", state);
  }
  if (base !== null) {
    const appended = compareDelta(base, state, policy, { intent: true, task: true, closure: true, response: true });
    if (appended < 0) {
      throw invalidState("the progressed state is not the exact durable progression of the verified state", state);
    }
  }
}

/**
 * The narrow racing reconciliation of one acceptance `invalid_state`: the
 * authoritative `error.state` must pass the full exact-progressed
 * verification (the retry-window classification plus the exact durable
 * suffix progression of the verified base state); any verification
 * refusal returns `null` and the original cause is re-thrown by object
 * identity. Never classified from message text.
 */
function tryProgressedState(
  cause: PipelineV2ReviseTaskIntentControllerError,
  policy: InterventionPolicy,
  base: PipelineV2RunState,
): PipelineV2RunState | null {
  const state = cause.state;
  if (state === null) {
    return null;
  }
  try {
    verifyProgressedState(state, policy, base);
  } catch {
    return null;
  }
  return state;
}

// --- the main entrypoint ------------------------------------------------------

/**
 * Validate, compose and complete the restart-aware revise-task
 * intervention through the existing facades (see the module docstring for
 * the full order, classification and verification contracts).
 */
export async function applyPipelineV2ReviseTaskInterventionWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<AppliedPipelineV2ReviseTaskIntervention> {
  // Capture boundary: the options shape, every options field exactly once
  // in the fixed order, the ops record shape and each ops member exactly
  // once — all before the first await. A hostile extra options field is
  // never read; caller objects are never frozen or modified.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipelineValue = options["pipeline"];
  const runRootValue = options["runRoot"];
  const sinkValue = options["sink"];
  const runIdValue = options["runId"];
  const waitIndexValue = options["waitIndex"];
  const taskIdValue = options["taskId"];
  const taskBodyValue = options["taskBody"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const restorePlan = ops["restorePlan"];
  const prepareTaskRevision = ops["prepareTaskRevision"];
  const prepareIntent = ops["prepareIntent"];
  const acceptIntent = ops["acceptIntent"];
  const completeTask = ops["completeTask"];
  if (
    typeof restorePlan !== "function" ||
    typeof prepareTaskRevision !== "function" ||
    typeof prepareIntent !== "function" ||
    typeof acceptIntent !== "function" ||
    typeof completeTask !== "function"
  ) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires the five composed facade functions");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any field of the pipeline is read. Its typed
  // error keeps its identity.
  requireResolvedPipelineV2Provenance(pipelineValue as ResolvedPipelineV2, "pipeline v2 revise task intervention");
  if (!isString(runRootValue) || runRootValue === "") {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a non-empty runRoot string");
  }
  if (!isRecord(sinkValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a state sink");
  }
  if (!isPipelineV2SafeId(runIdValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a safe run id");
  }
  if (!isPositiveSafeInteger(waitIndexValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a positive safe integer wait index");
  }
  if (!isPipelineV2SafeId(taskIdValue)) {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a safe task id");
  }
  if (!isString(taskBodyValue) || taskBodyValue === "") {
    throw invalidOptions("applyPipelineV2ReviseTaskIntervention requires a non-empty task body");
  }
  const pipeline = pipelineValue as ResolvedPipelineV2;
  const runRoot = runRootValue;
  const sink = sinkValue as unknown as PipelineV2ReviseTaskInterventionControllerSink;
  const runId = runIdValue;
  const waitIndex = waitIndexValue;
  const taskId = taskIdValue;
  const taskBody = taskBodyValue;
  // The single authoritative pre-call state, captured once after the
  // gates and never read again for the result construction.
  const before = sink.snapshot;
  if (before === null) {
    throw invalidState("applyPipelineV2ReviseTaskIntervention requires a durable run state", null);
  }

  // Step 1: the read-only restoration of the last accepted plan on the
  // captured authoritative state, fully verified unchanged; its typed
  // errors keep their identity.
  const restored = verifyRestoreResult(
    await (restorePlan as typeof restorePipelineV2AcceptedRunPlan)({
      pipeline,
      runRoot,
      state: before,
    } as RestorePipelineV2AcceptedRunPlanOptions),
    before,
  );

  // Step 2: the derivation — every value from durable data and the
  // restored compiled plan; the caller passes none of them.
  const derived = deriveIntervention(
    restored,
    runId,
    waitIndex,
    taskId,
    taskBody,
    prepareTaskRevision as typeof prepareTaskRevisionManifest,
    prepareIntent as typeof prepareWaitIntent,
  );

  // Step 3: the retry classification over the authoritative state.
  const window = classifyRetryWindow(restored.state, derived.policy);
  let verifiedState: PipelineV2RunState;
  if (window === "acceptance") {
    let acceptedState: PipelineV2RunState;
    try {
      const accepted = await (acceptIntent as typeof acceptPipelineV2ReviseTaskIntent)({
        runRoot,
        sink,
        intent: derived.intent,
        candidateTaskRevision: derived.candidate,
      } as AcceptPipelineV2ReviseTaskIntentOptions);
      acceptedState = verifyAcceptanceResult(accepted, restored.state, derived.policy);
    } catch (cause) {
      // The narrow racing reconciliation: the acceptance collided with a
      // concurrent closure/response. The continuation is allowed only
      // after the same full exact-progressed verification of the
      // acceptance's authoritative state, pinned as the exact durable
      // suffix progression of the captured before state.
      const progressed =
        cause instanceof PipelineV2ReviseTaskIntentControllerError && cause.reason === "invalid_state"
          ? tryProgressedState(cause, derived.policy, restored.state)
          : null;
      if (progressed === null) {
        throw cause;
      }
      acceptedState = progressed;
    }
    verifiedState = acceptedState;
  } else {
    verifiedState = restored.state;
  }

  // Step 4: the completion through the existing composition; its typed
  // errors keep their identity.
  const completed = await (completeTask as typeof completePipelineV2ReviseTask)({
    runRoot,
    sink,
    intent: derived.intent,
  } as CompletePipelineV2ReviseTaskOptions);

  // Step 5: the full defensive verification of the completion result
  // against the verified accepted or progressed state.
  return verifyCompletionResult(completed, verifiedState, derived.policy);
}
