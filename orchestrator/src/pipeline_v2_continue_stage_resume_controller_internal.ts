/**
 * Production-neutral composition of the full `continue_stage` handoff
 * (wired into the production runner through `continuePipelineV2Stage`
 * and `orchestrator continue-stage`): the restart-aware continue-stage
 * intervention followed by the
 * coordinator's resume entrypoint.
 *
 * This controller is the single layer that binds the two existing
 * authoritative facades into one fixed sequence:
 *
 * 1. `applyPipelineV2ContinueStageIntervention` — the restart-aware
 *    intervention (the intent acceptance, the authoritative run-plan
 *    restoration and the continued-stage composition; its own C0–C5
 *    windows, progressed-retry reconciliation and full verification stay
 *    authoritative);
 * 2. the defensive verification of the successful handoff — the flat
 *    result must bind exactly to the captured provenance-backed intent
 *    (run id, wait index, intent digest, additional iterations, request
 *    digest, `continue_stage` action and its routing target), to the
 *    caller budget through the durable granted generation's
 *    `initial_budget`, and to the authoritative durable state, which the
 *    real intervention result carries as the exact sink snapshot object
 *    (proven by identity, not by a structural re-comparison and never by
 *    a second parser or a second state-delta machine);
 * 3. `resumePipelineV2Run` — the coordinator's production-neutral resume
 *    entrypoint, called with the captured pipeline, the run id derived
 *    exclusively from the verified intent, the captured run root and sink,
 *    and stable runtime/control adapters built from the contract functions
 *    captured exactly once in the preflight (so a mutation of the
 *    caller-owned runtime or control objects during the pending
 *    intervention cannot change the resume).
 *
 * The controller owns no durable side effect of its own: it dispatches
 * nothing, publishes nothing, never calls the reducer, validator, store,
 * manifest loader, serializer or digest machinery, and never starts a
 * worker. The intervention's typed errors pass through by object identity
 * and the resume is never started after one; the coordinator's thrown
 * errors pass through by object identity as well. The signal lifecycle
 * (acceptance and cutoff) stays owned by the coordinator through the
 * captured control functions; this layer interprets no signal itself.
 *
 * The successfully returned coordinator result is verified defensively —
 * only the exact runtime shapes of the coordinator union are accepted:
 * a success carries exactly the own enumerable keys `ok`,`state` with a
 * state record identical to the authoritative sink snapshot; a refusal
 * carries exactly `ok`,`refused`,`reason`,`state` with `refused === true`
 * and a reason from the coordinator's refusal vocabulary; an ordinary
 * failure carries exactly `ok`,`reason`,`state` with no own `refused` and
 * a reason from the canonical `PIPELINE_V2_FAILURE_REASONS`; failure and
 * refusal states are null or exactly the authoritative snapshot — and
 * returned unchanged by object identity: refusal, worker failure, signal
 * failure and persistence failure stay coordinator-owned
 * classifications. A malformed success result of either facade is this
 * layer's own `invalid_result` carrying the last authoritative snapshot —
 * never a leaked `TypeError`, never healed downstream.
 *
 * Capture and preflight (all before the first await and before any durable
 * intervention): the options shape; then the seven option fields
 * (`pipeline`, `runRoot`, `sink`, `runtime`, `control`, `intent`,
 * `initialBudget`) each read exactly once; then the ops record shape and
 * its two members each read exactly once with function checks; then the
 * validations — a non-empty absolute `runRoot`, a structural sink (a
 * record with a `dispatch` function; the `snapshot`/`poisoned` getters
 * stay owned by the composed layers and the coordinator), a record
 * runtime, a record control, a prepared intent record and a positive safe
 * integer budget; then the pipeline provenance gate and the intent
 * provenance gate (the existing registry, strictly the
 * `continue_stage_intent` kind) plus the intent's continue-stage contract
 * fields; the policy is fixed as captured scalars and a hostile extra
 * options field is never read. Invalid options are this layer's own
 * `invalid_options` with state null and zero facade calls.
 *
 * Retry and crash windows (the honest boundaries): C0 — the intervention
 * has not started; the fresh call runs the full five-command intervention
 * suffix and then the resume. C5/crash seam — the intervention is already
 * fully durable while the resume has not started (for example after a
 * resume-side crash); a fresh call's intervention recognizes the exact
 * completed boundary with zero dispatch through the intervention facade's
 * own progressed-retry reconciliation and then runs the resume once. A
 * resume that already began durable execution has no new convergence
 * guarantee here: the coordinator's existing continuation contract stays
 * the only owner of that semantics. Unrecognized progressed states and
 * downstream lifecycle errors pass through by identity.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ContinueStageResumeControllerError`,
 * `applyPipelineV2ContinueStageResumeWithIo` and the frozen
 * `productionContinueStageResumeOps`; the public module exports exactly
 * the error and `resumePipelineV2RunAfterContinueStageIntervention`
 * (types are not runtime keys).
 *
 * Not implemented (stays unwired): the action/`additional_iterations`
 * selection policy, the automatic intervention loop, the default pipeline
 * bundle, migrations/API/T3 and multi-process locking (the revise-task
 * branch is the wired `orchestrator revise-task` command's).
 */
import { isAbsolute } from "node:path";
import {
  applyPipelineV2ContinueStageIntervention,
  type AppliedPipelineV2ContinueStageIntervention,
} from "./pipeline_v2_continue_stage_intervention_controller.ts";
import {
  resumePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
  type PipelineV2CoordinatorStateSink,
  type PipelineV2ResumeCoordinationResult,
  type PipelineV2ResumeRefusalReason,
} from "./pipeline_v2_coordinator.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type {
  PipelineV2ContinueStageIntentManifest,
  PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import { PIPELINE_V2_FAILURE_REASONS } from "./pipeline_v2_state.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

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

export type PipelineV2ContinueStageResumeControllerFailureReason = "invalid_options" | "invalid_result";

/**
 * A failure of the handoff composition layer itself with its stable
 * machine-readable `reason` and the last authoritative durable state
 * (`null` when none was established). The composed facades' typed errors
 * pass through by identity and never take this shape.
 */
export class PipelineV2ContinueStageResumeControllerError extends Error {
  readonly reason: PipelineV2ContinueStageResumeControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinueStageResumeControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinueStageResumeControllerError";
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
export interface PipelineV2ContinueStageResumeOps {
  readonly applyIntervention: typeof applyPipelineV2ContinueStageIntervention;
  readonly resumeRun: typeof resumePipelineV2Run;
}

/**
 * The frozen production ops: the two existing facades bound by identity;
 * no installer and no mutable module-global seam.
 */
export const productionContinueStageResumeOps: PipelineV2ContinueStageResumeOps = deepFreezeValue({
  applyIntervention: applyPipelineV2ContinueStageIntervention,
  resumeRun: resumePipelineV2Run,
}) as unknown as PipelineV2ContinueStageResumeOps;

export interface ApplyPipelineV2ContinueStageResumeOptions {
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
  /** The provenance-backed prepared `continue_stage_intent`. */
  readonly intent: PreparedPipelineV2RunWaitIntent;
  /** The caller-owned stage iteration budget; no policy is applied here. */
  readonly initialBudget: number;
}

const CONTINUE_STAGE_ACTION_ID = "continue_stage";

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
  reason: PipelineV2ContinueStageResumeControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageResumeControllerError {
  return new PipelineV2ContinueStageResumeControllerError(reason, message, state);
}

function invalidOptions(message: string): PipelineV2ContinueStageResumeControllerError {
  return controllerError("invalid_options", message, null);
}

function invalidResult(
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageResumeControllerError {
  return controllerError("invalid_result", message, state);
}

/**
 * The fixed caller policy, captured once before the first side effect: the
 * intent's binding scalars and the caller-owned budget. The run id the
 * resume consumes is derived exclusively from the verified intent.
 */
interface ContinueStageResumePolicy {
  readonly runId: string;
  readonly waitIndex: number;
  readonly intentSha256: string;
  readonly stageId: string;
  readonly expectedPlanSha256: string;
  readonly additionalIterations: number;
  readonly initialBudget: number;
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
 * flat shape binds exactly to the captured intent and the caller budget,
 * the durable state is the exact authoritative sink snapshot object by
 * identity, and the completed handoff boundary carries the targeted
 * durable bindings (the answered target wait, the exact single grant, the
 * granted last open generation with the grant-closed iteration and the
 * open successor). Total for malformed values: every check is a typed
 * `invalid_result`, never a `TypeError`.
 */
function verifyInterventionResult(
  resultValue: unknown,
  policy: ContinueStageResumePolicy,
  authoritative: PipelineV2RunState | null,
): void {
  try {
    if (!isRecord(resultValue)) {
      throw invalidResult("the continue-stage intervention result is not a record", authoritative);
    }
    const result = resultValue as Record<string, unknown>;
    if (
      !isPositiveSafeInteger(result["wait_index"]) ||
      result["wait_index"] !== policy.waitIndex ||
      !isLowercaseSha256(result["intent_sha256"]) ||
      result["intent_sha256"] !== policy.intentSha256 ||
      !isLowercaseSha256(result["request_sha256"]) ||
      !isLowercaseSha256(result["response_sha256"]) ||
      !isPositiveSafeInteger(result["additional_iterations"]) ||
      result["additional_iterations"] !== policy.additionalIterations ||
      result["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
      !isNonEmptyString(result["action_to"]) ||
      !isPositiveSafeInteger(result["closed_iteration_index"]) ||
      !isPositiveSafeInteger(result["iteration_index"]) ||
      result["iteration_index"] !== (result["closed_iteration_index"] as number) + 1 ||
      !isPositiveSafeInteger(result["generation_index"]) ||
      !isRecord(result["compiled_stage"])
    ) {
      throw invalidResult("the continue-stage intervention result does not match the accepted intent", authoritative);
    }
    const stateValue = result["state"];
    if (authoritative === null || stateValue !== authoritative) {
      throw invalidResult("the continue-stage intervention result does not carry the authoritative durable state", authoritative);
    }
    verifyHandoffBoundary(stateValue as PipelineV2RunState, result, policy, authoritative);
  } catch (cause) {
    if (cause instanceof PipelineV2ContinueStageResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the continue-stage intervention result could not be verified", authoritative);
  }
}

/**
 * The targeted completed-boundary bindings of the handoff, read only from
 * the authoritative snapshot the real intervention result carries by
 * identity: run identity, status/phase, the cursor at the declared action
 * target on the wait anchor, the journals at the anchor, the answered
 * target wait (the last and only record of its index, carrying the exact
 * accepted intent, the declared request digest and the exact
 * `continue_stage` response), the exact single grant of the granted pair,
 * and the granted last open generation bound to the intent's stage, the
 * expected plan digest and the caller budget with the grant-closed
 * iteration and the open successor.
 */
function verifyHandoffBoundary(
  state: PipelineV2RunState,
  result: Record<string, unknown>,
  policy: ContinueStageResumePolicy,
  authoritative: PipelineV2RunState | null,
): void {
  if (state.run_id !== policy.runId || state.status !== "active" || state.phase !== "running") {
    throw invalidResult("the handoff boundary is not the active running run of the accepted intent", authoritative);
  }
  const cursor = state.cursor as unknown;
  if (
    !isRecord(cursor) ||
    !isNonNegativeSafeInteger(cursor["transition_count"]) ||
    cursor["current_state"] !== result["action_to"]
  ) {
    throw invalidResult("the handoff boundary cursor is not at the declared action target", authoritative);
  }
  const transitionCount = cursor["transition_count"] as number;
  if (
    !Array.isArray(state.transitions) ||
    state.transitions.length !== transitionCount ||
    !Array.isArray(state.executions) ||
    state.executions.length !== transitionCount
  ) {
    throw invalidResult("the handoff boundary journals do not sit at the wait anchor", authoritative);
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
    throw invalidResult("the target wait does not carry the declared request digest on the wait anchor", authoritative);
  }
  const intentRecord = target["intent"] as unknown;
  if (!isRecord(intentRecord) || intentRecord["intent_sha256"] !== policy.intentSha256) {
    throw invalidResult("the target wait does not carry the exact accepted intent", authoritative);
  }
  const responseRecord = target["response"] as unknown;
  if (
    !isRecord(responseRecord) ||
    responseRecord["action_id"] !== CONTINUE_STAGE_ACTION_ID ||
    responseRecord["response_sha256"] !== result["response_sha256"]
  ) {
    throw invalidResult("the target wait does not carry the exact continue_stage response", authoritative);
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
    if (action["id"] === CONTINUE_STAGE_ACTION_ID) {
      declaredCount += 1;
      declaredTo = action["to"];
    }
  }
  if (declaredCount !== 1 || declaredTo !== result["action_to"]) {
    throw invalidResult("the target wait does not declare the exact continue_stage routing target", authoritative);
  }
  const grants = state.grants as unknown;
  if (!Array.isArray(grants)) {
    throw invalidResult("the handoff boundary carries no grant ledger", authoritative);
  }
  let pair: Record<string, unknown> | undefined;
  for (const entry of grants) {
    if (!isRecord(entry)) {
      throw invalidResult("the handoff boundary carries a malformed grant record", authoritative);
    }
    if (entry["generation_index"] === result["generation_index"] && entry["wait_index"] === policy.waitIndex) {
      if (pair !== undefined) {
        throw invalidResult("the granted pair carries more than one grant", authoritative);
      }
      pair = entry;
    }
  }
  if (
    pair === undefined ||
    pair["intent_sha256"] !== policy.intentSha256 ||
    pair["additional_iterations"] !== policy.additionalIterations
  ) {
    throw invalidResult("the granted pair does not carry the exact accepted grant", authoritative);
  }
  const generations = state.generations as unknown;
  if (!Array.isArray(generations) || generations.length === 0) {
    throw invalidResult("the handoff boundary carries no stage generation", authoritative);
  }
  const generation = generations[generations.length - 1] as unknown;
  if (!isRecord(generation) || generation["closed"] !== undefined) {
    throw invalidResult("the granted generation is not the last open generation", authoritative);
  }
  if (
    generation["index"] !== result["generation_index"] ||
    generation["stage_id"] !== policy.stageId ||
    generation["plan_sha256"] !== policy.expectedPlanSha256 ||
    generation["initial_budget"] !== policy.initialBudget
  ) {
    throw invalidResult("the granted generation is not bound to the accepted intent, plan and caller budget", authoritative);
  }
  const iterations = generation["iterations"] as unknown;
  if (!Array.isArray(iterations)) {
    throw invalidResult("the granted generation carries no iteration history", authoritative);
  }
  const closedIteration: unknown = iterations[(result["closed_iteration_index"] as number) - 1];
  if (!isRecord(closedIteration) || closedIteration["index"] !== result["closed_iteration_index"]) {
    throw invalidResult("the granted generation does not carry the grant-closed iteration", authoritative);
  }
  const closed = closedIteration["closed"] as unknown;
  if (
    !isRecord(closed) ||
    closed["by"] !== "grant" ||
    closed["wait_index"] !== policy.waitIndex ||
    closed["closed_transition_count"] !== transitionCount
  ) {
    throw invalidResult("the grant-closed iteration does not carry the exact grant closure", authoritative);
  }
  const lastIteration: unknown = iterations[iterations.length - 1];
  if (
    !isRecord(lastIteration) ||
    lastIteration["index"] !== result["iteration_index"] ||
    lastIteration["closed"] !== undefined
  ) {
    throw invalidResult("the granted generation does not end at the open successor iteration", authoritative);
  }
  const openIteration = generation["open_iteration"] as unknown;
  if (
    !isRecord(openIteration) ||
    openIteration["index"] !== result["iteration_index"] ||
    openIteration["opened_transition_count"] !== transitionCount
  ) {
    throw invalidResult("the open successor iteration is not projected on the wait anchor", authoritative);
  }
}

/**
 * The defensive verification of one successfully returned coordinator
 * result union: a well-formed discriminant (`ok` true with a state record;
 * `ok` false with a non-empty reason, `refused` exactly `true` when
 * present, and a state that is null or exactly the authoritative sink
 * snapshot). The verified result is returned unchanged by object
 * identity — refusal, worker failure, signal failure and persistence
 * failure stay coordinator-owned classifications.
 */
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
function hasExactOwnKeys(record: Record<string, unknown>, ...expected: string[]): boolean {
  const keys = Object.keys(record);
  return keys.length === expected.length && expected.every((key) => keys.includes(key));
}

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
    if (keys.includes("waiting")) {
      // The exact controlled-suspension branch of the coordinator: the run
      // is durably waiting at the trusted stage-wait boundary and is
      // returned by identity — never reclassified as a failure or refusal.
      if (!hasExactOwnKeys(result, "ok", "waiting", "state")) {
        throw invalidResult("the resume coordinator waiting result carries foreign fields", authoritative);
      }
      if (result["waiting"] !== true) {
        throw invalidResult("the resume coordinator result carries a malformed waiting discriminant", authoritative);
      }
      const state = result["state"];
      if (state !== authoritative || !isRecord(state)) {
        throw invalidResult("the resume coordinator result does not carry the authoritative durable state", authoritative);
      }
      return resultValue as unknown as PipelineV2ResumeCoordinationResult;
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
    if (cause instanceof PipelineV2ContinueStageResumeControllerError) {
      throw cause;
    }
    throw invalidResult("the resume coordinator result could not be verified", authoritative);
  }
}

/**
 * Validate, compose and verify one full `continue_stage` handoff through
 * the two existing facades (see the module docstring for the full order,
 * capture boundary, retry windows and verification semantics).
 */
export async function applyPipelineV2ContinueStageResumeWithIo(
  opsValue: unknown,
  optionsValue: unknown,
): Promise<PipelineV2ResumeCoordinationResult> {
  // Capture boundary: the options shape, every options field and the ops
  // record shape and its two members are read exactly once, all before the
  // first await and before any durable intervention. A later mutation of
  // the caller's options, runtime, control or ops cannot change this
  // handoff.
  if (!isRecord(optionsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires an options object");
  }
  const options = optionsValue as Record<string, unknown>;
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot: unknown = options["runRoot"];
  const sink: unknown = options["sink"];
  const runtime: unknown = options["runtime"];
  const control: unknown = options["control"];
  const intent: unknown = options["intent"];
  const initialBudget: unknown = options["initialBudget"];
  if (!isRecord(opsValue)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires an ops object");
  }
  const ops = opsValue as Record<string, unknown>;
  const applyIntervention: unknown = ops["applyIntervention"];
  const resumeRun: unknown = ops["resumeRun"];
  if (typeof applyIntervention !== "function" || typeof resumeRun !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires the two composed facade functions");
  }
  if (!isString(runRoot) || runRoot === "" || !isAbsolute(runRoot)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires an absolute runRoot string");
  }
  if (!isRecord(sink) || typeof (sink as Record<string, unknown>)["dispatch"] !== "function") {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires a structural state sink");
  }
  if (!isRecord(runtime)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires an agent runtime object");
  }
  if (!isRecord(control)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires a signal control object");
  }
  if (!isRecord(intent)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires a prepared wait intent object");
  }
  if (!isPositiveSafeInteger(initialBudget)) {
    throw invalidOptions("resumePipelineV2RunAfterContinueStageIntervention requires a positive safe integer initial budget");
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any field of the intent or of a durable state is
  // read. Its own typed error propagates unchanged.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 continue stage resume controller");
  // The intent provenance gate: the exact registered prepared object of the
  // manifest substrate, and strictly the continue-stage kind.
  if (!hasPreparedRunPlanProvenance(intent, "continue_stage_intent")) {
    throw invalidOptions("the prepared wait intent is not a provenance-registered continue_stage_intent");
  }
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  const manifestValue: unknown = preparedIntent.manifest;
  if (
    !isRecord(manifestValue) ||
    manifestValue["kind"] !== "continue_stage_intent" ||
    !isString(manifestValue["run_id"]) ||
    !isPositiveSafeInteger(manifestValue["wait_index"]) ||
    !isString(manifestValue["stage_id"]) ||
    !isString(manifestValue["expected_plan_sha256"]) ||
    !isPositiveSafeInteger(manifestValue["additional_iterations"])
  ) {
    throw invalidOptions("the prepared wait intent does not carry the continue_stage contract fields");
  }
  const manifest = manifestValue as unknown as PipelineV2ContinueStageIntentManifest;
  const policy: ContinueStageResumePolicy = {
    runId: manifest.run_id,
    waitIndex: manifest.wait_index,
    intentSha256: preparedIntent.sha256,
    stageId: manifest.stage_id,
    expectedPlanSha256: manifest.expected_plan_sha256,
    additionalIterations: manifest.additional_iterations,
    initialBudget,
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

  // Step 1: the restart-aware intervention facade — the intent acceptance,
  // the authoritative plan restoration and the continued-stage
  // composition. Its typed errors pass through by object identity; the
  // resume is never started after one.
  const interventionResult = await (applyIntervention as typeof applyPipelineV2ContinueStageIntervention)({
    pipeline,
    runRoot,
    sink: structuralSink,
    intent: preparedIntent,
    initialBudget,
  });

  // Step 2: the defensive verification of the successful handoff. The
  // authoritative snapshot is read exactly once, immediately after the
  // intervention and before the resume; the real intervention result
  // carries it as the exact same immutable state object.
  const authoritativeBeforeResume: PipelineV2RunState | null = structuralSink.snapshot;
  verifyInterventionResult(interventionResult, policy, authoritativeBeforeResume);

  // Step 3: the coordinator's resume entrypoint — the run id derived
  // exclusively from the verified intent, the captured pipeline/run
  // root/sink and the stable runtime/control adapters. A thrown error
  // passes through by object identity.
  const resumeResult = await (resumeRun as typeof resumePipelineV2Run)(
    {
      pipeline,
      runId: policy.runId,
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
