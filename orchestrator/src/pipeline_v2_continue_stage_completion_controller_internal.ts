import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  applyPipelineV2ContinueStageGrant,
  type AppliedPipelineV2ContinueStageGrant,
} from "./pipeline_v2_continue_stage_grant_controller.ts";
import {
  recordPipelineV2WaitAction,
  type RecordedPipelineV2WaitResponse,
} from "./pipeline_v2_wait_controller.ts";
import type {
  PipelineV2RunCommand,
  PipelineV2RunState,
  PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import type {
  PreparedPipelineV2RunWaitIntent,
  PipelineV2ContinueStageIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral completion controller for the continue-stage
 * intervention (unwired).
 *
 * The controller completes an already durably accepted
 * `continue_stage_intent` by composing the two existing authoritative
 * steps in the fixed order — apply (or confirm) the grant and the
 * grant-bound iteration closure through
 * `applyPipelineV2ContinueStageGrant`, then record the user's
 * `continue_stage` response through the existing generic wait controller
 * (`recordPipelineV2WaitAction`). No new response manifest, serializer,
 * digest builder, store protocol or response dispatcher is introduced;
 * the response document, its publication and the durable
 * `wait_response_recorded` command remain entirely owned by the existing
 * wait-manifest substrate. The controller itself performs no filesystem
 * work, never calls the reducer or any store, never calls the intent
 * acceptance controller, does not open the next iteration and does not
 * resume the run.
 *
 * Composition ordering (always): capture the options → apply or confirm
 * the grant → verify the grant result against the accepted intent →
 * record the wait action with the fixed `continue_stage` action id →
 * verify the response result against the durable target wait → return
 * the unified result carrying the response controller's authoritative
 * state. The response is never published or dispatched before the grant
 * result is confirmed.
 *
 * Capture and provenance: the options shape, then `runRoot` → `sink` →
 * `intent` read exactly once, all references captured before the first
 * await; caller mutation after the capture cannot influence the
 * execution. The completion reads no intent or snapshot fields itself
 * before the grant call: the provenance authority stays inside
 * `applyPipelineV2ContinueStageGrant` (its gate runs before any field
 * read), so the intent's manifest fields are read only after the grant
 * result has been verified. There is no second state validator and no
 * duplicated durable grant binding logic.
 *
 * Grant result verification (the contract-owned values, complete and
 * strictly before any response filesystem work or dispatch): the wait
 * index, the additional iteration count and the intent digest must equal
 * the accepted intent's, the generation and iteration indexes must be
 * positive safe integers, the target wait must be the last wait record
 * carrying the exact accepted intent digest and the declared
 * `continue_stage` action, the boundary must be exactly the waiting/open
 * form (C0–C3) or the exact active/answered C4 form, the exact durable
 * grant must be present exactly once, the grant's generation must be the
 * last open generation bound to the intent's stage and plan digest, and
 * the target iteration must be the last one closed with the exact grant
 * closure. Every field access is defensive; a hostile or structurally
 * inconsistent injected result (including a removed closure, a replaced
 * wait intent, a changed stage/plan binding and malformed nested state)
 * is the controller's own typed `invalid_result` — never a leaked
 * `TypeError` — with zero response publication and zero dispatch.
 *
 * Response result verification (against the verified pre-response wait of
 * the grant result, never against the result's own final wait): the wait
 * bindings (index, transition count, state id, reason, request digest,
 * ordered actions, journal position, accepted intent) must be unchanged;
 * the only allowed change is the exact `continue_stage` response; the
 * request digest and the routing target are taken from the pre-response
 * wait; the final state must be active and running with the cursor at the
 * declared action target and with the cursor transition count, the
 * transition journal and the execution journal exactly at the
 * pre-response wait's boundary; the C4 boundary must keep the
 * pre-response response binding (the same action id and response digest)
 * unchanged; and the exact grant, the exact grant closure
 * (checked against the original wait anchor) and the generation/iteration
 * bindings (including the iteration count and the iteration list length)
 * of the grant result's state must be unchanged. A coherent
 * hostile mutation of both the result fields and the final state is
 * detected by the binding comparison; a hostile injected result is the
 * controller's own typed `invalid_result`.
 *
 * Retry windows: C0 (accepted intent, no grant — the full suffix grant →
 * closure → response, three durable revisions), C1 (the durable grant —
 * the closure then the response), C2 (the grant and closure durable, the
 * wait open — the response only), C3 (the response file published but not
 * durable — the existing wait controller adopts the exact file and
 * dispatches once) and C4 (the response durable or durability-unknown —
 * the grant controller's active/answered S2 recognition returns with zero
 * dispatch and the wait controller recognizes the exact durable response,
 * verifies or restores the request/response publications and dispatches
 * nothing). Conflicts surface as the existing controllers' typed errors
 * unchanged; nothing is ever rewritten.
 *
 * Runtime export surface (public module) is exactly
 * `PipelineV2ContinueStageCompletionControllerError` and
 * `completePipelineV2ContinueStage`. Diagnostics are content-free (no
 * digest values, canonical JSON, paths, bodies, arbitrary caller values,
 * env values or credentials); errors are never classified from message
 * text; unexpected errors and the downstream controllers' typed errors
 * keep their class and identity.
 *
 * Not implemented (stays unwired): the intent selection policy, the
 * choice of `additional_iterations`, `revise_task_intent`, the task/plan
 * revision replanning chain, opening the next iteration, automatic
 * resume, coordinator/runner/CLI wiring, schema changes,
 * migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ContinueStageCompletionControllerFailureReason = "invalid_options" | "invalid_result";

export class PipelineV2ContinueStageCompletionControllerError extends Error {
  readonly reason: PipelineV2ContinueStageCompletionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinueStageCompletionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinueStageCompletionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam passed through to the existing controllers;
 * the production `PipelineV2RunStateSink` satisfies it without an
 * adapter. The completion controller itself never reads the sink's
 * members or dispatches commands.
 */
export interface PipelineV2ContinueStageCompletionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface CompletePipelineV2ContinueStageOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2ContinueStageCompletionControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

export interface CompletedPipelineV2ContinueStage {
  readonly wait_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly additional_iterations: number;
  readonly intent_sha256: string;
  readonly request_sha256: string;
  readonly response_sha256: string;
  readonly action_id: "continue_stage";
  readonly action_to: string;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call structural ops of the internal core: the existing grant
 * application and the existing wait-action recording, bound by one frozen
 * production object. Tests inject their own per-call object; there is no
 * mutable module-global seam, no installer and no public export of the
 * seam.
 */
export interface PipelineV2ContinueStageCompletionOps {
  readonly applyGrant: typeof applyPipelineV2ContinueStageGrant;
  readonly recordWaitAction: typeof recordPipelineV2WaitAction;
}

export const productionContinueStageCompletionOps: PipelineV2ContinueStageCompletionOps = Object.freeze({
  applyGrant: applyPipelineV2ContinueStageGrant,
  recordWaitAction: recordPipelineV2WaitAction,
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function completionError(
  reason: PipelineV2ContinueStageCompletionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageCompletionControllerError {
  return new PipelineV2ContinueStageCompletionControllerError(reason, message, state);
}

const CONTINUE_STAGE_ACTION_ID = "continue_stage";

/**
 * The exact durable grant record of the completion's grant result: exactly
 * one grant for the (generation, wait) pair with the exact intent digest
 * and the exact additional iteration count.
 */
function grantResultStateCarriesExactGrant(grant: AppliedPipelineV2ContinueStageGrant, intentSha256: string): boolean {
  const state = grant.state;
  let count = 0;
  let match = false;
  for (const record of state.grants) {
    if (record.generation_index === grant.generation_index && record.wait_index === grant.wait_index) {
      count += 1;
      match =
        record.intent_sha256 === intentSha256 &&
        record.additional_iterations === grant.additional_iterations;
    }
  }
  return count === 1 && match;
}

/**
 * The exact grant closure of the grant result's state: the generation's
 * last iteration closed by the grant against the target wait with the
 * wait's anchor and no open iteration projection.
 */
function grantResultStateCarriesExactClosure(
  grant: AppliedPipelineV2ContinueStageGrant,
  wait: PipelineV2WaitRecord,
): boolean {
  const generation = grant.state.generations[grant.generation_index - 1];
  if (
    generation === undefined ||
    generation.index !== grant.generation_index ||
    generation.closed !== undefined ||
    generation.open_iteration !== undefined ||
    grant.state.generations.length !== grant.generation_index
  ) {
    return false;
  }
  const iteration = generation.iterations[generation.iterations.length - 1];
  const closed = iteration?.closed;
  return (
    iteration !== undefined &&
    iteration.index === grant.iteration_index &&
    iteration.opened_transition_count !== undefined &&
    closed !== undefined &&
    closed.by === "grant" &&
    closed.wait_index === wait.index &&
    closed.closed_transition_count === wait.transition_count
  );
}

/**
 * The contract-owned binding fields of the target generation and its
 * target iteration must be identical between the grant result's state and
 * the final state: the generation stays the last open one with unchanged
 * identity bindings, and the target iteration stays the last one with its
 * opening anchor unchanged. No general deep comparator is used — only
 * these contract-owned fields.
 */
function generationBindingsUnchanged(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
  generationIndex: number,
  iterationIndex: number,
): boolean {
  const beforeGeneration = before.generations[generationIndex - 1];
  const afterGeneration = after.generations[generationIndex - 1];
  if (beforeGeneration === undefined || afterGeneration === undefined) {
    return false;
  }
  if (
    afterGeneration.index !== generationIndex ||
    after.generations.length !== generationIndex ||
    afterGeneration.closed !== undefined ||
    afterGeneration.open_iteration !== undefined
  ) {
    return false;
  }
  if (
    beforeGeneration.stage_id !== afterGeneration.stage_id ||
    beforeGeneration.stage_position !== afterGeneration.stage_position ||
    beforeGeneration.template_id !== afterGeneration.template_id ||
    beforeGeneration.plan_sha256 !== afterGeneration.plan_sha256 ||
    beforeGeneration.initial_budget !== afterGeneration.initial_budget ||
    beforeGeneration.opened_transition_count !== afterGeneration.opened_transition_count ||
    beforeGeneration.iteration_count !== afterGeneration.iteration_count ||
    beforeGeneration.iterations.length !== afterGeneration.iterations.length
  ) {
    return false;
  }
  const beforeIteration = beforeGeneration.iterations[beforeGeneration.iterations.length - 1];
  const afterIteration = afterGeneration.iterations[afterGeneration.iterations.length - 1];
  if (
    beforeIteration === undefined ||
    afterIteration === undefined ||
    afterIteration.index !== iterationIndex ||
    beforeIteration.index !== afterIteration.index ||
    beforeIteration.opened_transition_count !== afterIteration.opened_transition_count
  ) {
    return false;
  }
  return true;
}

/**
 * The full pre-response verification of the grant result state, against
 * the contract-owned values only: the target wait must be the last wait
 * record and carry the exact accepted intent digest and the declared
 * `continue_stage` action, the boundary must be exactly the waiting/open
 * form or the exact active/answered C4 form, the exact durable grant must
 * be present exactly once, the grant's generation must be the last open
 * generation bound to the intent's stage and plan digest, and the target
 * iteration must be the last one closed with the exact grant closure.
 * Every field access is defensive; a structurally inconsistent injected
 * result becomes the controller's own typed `invalid_result`, never a
 * leaked `TypeError`.
 */
/**
 * The exact immediate wait boundary: the cursor sits exactly at the
 * expected state with the transition journal, the transition count and
 * the execution journal all exactly at the wait's boundary. The expected
 * cursor state is `wait.state_id` for the waiting/open form and the
 * declared `continue_stage` action's target for the active/answered C4
 * form.
 */
function verifyImmediateWaitBoundary(
  state: PipelineV2RunState,
  wait: PipelineV2WaitRecord,
  expectedCursorState: string,
): void {
  const cursor = state.cursor;
  if (!isRecord(cursor) || cursor.current_state !== expectedCursorState) {
    throw completionError(
      "invalid_result",
      "the applied grant result state cursor is not at the expected boundary state",
      state,
    );
  }
  if (cursor.transition_count !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied grant result state cursor transition count does not match the wait boundary",
      state,
    );
  }
  if (!Array.isArray(state.transitions) || state.transitions.length !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied grant result state transition journal does not match the wait boundary",
      state,
    );
  }
  if (!Array.isArray(state.executions) || state.executions.length !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied grant result state execution journal does not match the wait boundary",
      state,
    );
  }
}

function verifyGrantResultBeforeResponse(
  grant: AppliedPipelineV2ContinueStageGrant,
  manifest: PipelineV2ContinueStageIntentManifest,
  intentSha256: string,
): void {
  const state = grant.state;
  if (!Array.isArray(state.waits) || state.waits.length !== grant.wait_index) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the target wait as the last wait record",
      isRecord(state) ? (state as PipelineV2RunState) : null,
    );
  }
  const wait = state.waits[grant.wait_index - 1];
  if (!isRecord(wait) || wait.index !== grant.wait_index) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the target wait",
      state,
    );
  }
  if (wait.intent?.intent_sha256 !== intentSha256) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the exact accepted intent on the target wait",
      state,
    );
  }
  if (!Array.isArray(wait.actions) || !wait.actions.some((action) => isRecord(action) && action.id === CONTINUE_STAGE_ACTION_ID)) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not declare the continue_stage action on the target wait",
      state,
    );
  }
  // The acceptable boundary form: the waiting/open wait (C0–C3) or the
  // exact active/answered C4 boundary — in both forms the cursor and the
  // transition/execution journals sit exactly at the wait boundary.
  if (state.status === "waiting" && state.phase === "waiting") {
    if (wait.response !== undefined) {
      throw completionError(
        "invalid_result",
        "the applied grant result state claims a waiting run with a recorded response",
        state,
      );
    }
    verifyImmediateWaitBoundary(state, wait as PipelineV2WaitRecord, wait.state_id);
  } else if (state.status === "active" && state.phase === "running") {
    if (!isRecord(wait.response) || wait.response.action_id !== CONTINUE_STAGE_ACTION_ID) {
      throw completionError(
        "invalid_result",
        "the applied grant result state claims an active run without the exact continue_stage response",
        state,
      );
    }
    const declaredAction = (wait.actions as Array<Record<string, unknown>>).find(
      (action) => action.id === CONTINUE_STAGE_ACTION_ID,
    );
    if (!isRecord(declaredAction)) {
      throw completionError(
        "invalid_result",
        "the applied grant result state does not declare the continue_stage action on the target wait",
        state,
      );
    }
    verifyImmediateWaitBoundary(state, wait as PipelineV2WaitRecord, declaredAction.to as string);
  } else {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry an acceptable completion boundary",
      state,
    );
  }
  if (!grantResultStateCarriesExactGrant(grant, intentSha256)) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the exact durable grant",
      state,
    );
  }
  const generation = state.generations[grant.generation_index - 1];
  if (
    !isRecord(generation) ||
    generation.index !== grant.generation_index ||
    !Array.isArray(state.generations) ||
    state.generations.length !== grant.generation_index ||
    generation.closed !== undefined
  ) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the last open target generation",
      state,
    );
  }
  if (generation.stage_id !== manifest.stage_id || generation.plan_sha256 !== manifest.expected_plan_sha256) {
    throw completionError(
      "invalid_result",
      "the applied grant result state generation bindings do not match the accepted intent",
      state,
    );
  }
  if (!grantResultStateCarriesExactClosure(grant, wait as PipelineV2WaitRecord)) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the exact grant closure",
      state,
    );
  }
}

/**
 * The contract-owned ordered action list equality: same length, same
 * order, same ids and targets.
 */
function orderedActionsEqual(a: unknown, b: unknown): boolean {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
    return false;
  }
  for (let index = 0; index < a.length; index += 1) {
    const left = a[index];
    const right = b[index];
    if (!isRecord(left) || !isRecord(right) || left.id !== right.id || left.to !== right.to) {
      return false;
    }
  }
  return true;
}

/**
 * The response result verification against the verified pre-response wait:
 * the wait bindings must be unchanged, the only allowed change is the
 * exact `continue_stage` response, the request digest and the routing
 * target come from the pre-response wait, and the exact grant, grant
 * closure and generation/iteration bindings of the grant result's state
 * must be unchanged in the final state. No response digest is rebuilt by
 * this controller.
 */
function verifyResponseResult(
  response: RecordedPipelineV2WaitResponse,
  grant: AppliedPipelineV2ContinueStageGrant,
  beforeWait: PipelineV2WaitRecord,
  intentSha256: string,
): void {
  if (!isRecord(response)) {
    throw completionError("invalid_result", "the recorded wait response result is not an object", null);
  }
  const finalState = response.state;
  if (!isRecord(finalState)) {
    throw completionError("invalid_result", "the recorded wait response result is not an object", null);
  }
  if (response.wait_index !== grant.wait_index || response.action_id !== CONTINUE_STAGE_ACTION_ID) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not match the requested action",
      finalState,
    );
  }
  if (!Array.isArray(finalState.waits) || finalState.waits.length !== grant.wait_index) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the wait journal position",
      finalState,
    );
  }
  const finalWait = finalState.waits[grant.wait_index - 1];
  if (!isRecord(finalWait) || finalWait.index !== grant.wait_index) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not carry the target wait",
      finalState,
    );
  }
  if (
    finalWait.transition_count !== beforeWait.transition_count ||
    finalWait.state_id !== beforeWait.state_id ||
    finalWait.reason !== beforeWait.reason ||
    finalWait.request_sha256 !== beforeWait.request_sha256 ||
    !orderedActionsEqual(finalWait.actions, beforeWait.actions) ||
    finalWait.intent?.intent_sha256 !== intentSha256
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the target wait bindings",
      finalState,
    );
  }
  // The only allowed change on the target wait: the exact continue_stage
  // response.
  if (
    !isRecord(finalWait.response) ||
    finalWait.response.action_id !== CONTINUE_STAGE_ACTION_ID ||
    finalWait.response.response_sha256 !== response.response_sha256
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not match the durable response on the target wait",
      finalState,
    );
  }
  // The request digest and the routing target come from the pre-response
  // wait, never from the result's own final wait.
  const beforeDeclared = beforeWait.actions.find((action) => action.id === CONTINUE_STAGE_ACTION_ID);
  if (!isRecord(beforeDeclared)) {
    throw completionError(
      "invalid_result",
      "the pre-response wait record does not declare the continue_stage action",
      finalState,
    );
  }
  if (response.request_sha256 !== beforeWait.request_sha256) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result request digest does not match the pre-response wait",
      finalState,
    );
  }
  if (response.action_to !== beforeDeclared.to || finalState.cursor.current_state !== beforeDeclared.to) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not route to the declared action target",
      finalState,
    );
  }
  if (finalState.status !== "active" || finalState.phase !== "running") {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not carry the active post-response state",
      finalState,
    );
  }
  // The final state pins the exact post-response boundary: the cursor
  // count and the transition/execution journals sit exactly at the
  // pre-response wait's boundary.
  if (
    finalState.cursor.transition_count !== beforeWait.transition_count ||
    !Array.isArray(finalState.transitions) ||
    finalState.transitions.length !== beforeWait.transition_count ||
    !Array.isArray(finalState.executions) ||
    finalState.executions.length !== beforeWait.transition_count
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not keep the transition and execution journals at the response boundary",
      finalState,
    );
  }
  // The C4 boundary: the pre-response wait already carries the response,
  // and the final state must keep exactly that response binding unchanged.
  if (
    beforeWait.response !== undefined &&
    (finalWait.response === undefined ||
      finalWait.response.action_id !== beforeWait.response.action_id ||
      finalWait.response.response_sha256 !== beforeWait.response.response_sha256)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the pre-response response binding",
      finalState,
    );
  }
  // The exact grant, the exact grant closure and the generation/iteration
  // bindings of the grant result's state must be unchanged in the final
  // state; the final closure is checked against the original wait anchor.
  if (
    !grantResultStateCarriesExactGrant({ ...grant, state: finalState }, intentSha256) ||
    !grantResultStateCarriesExactClosure({ ...grant, state: finalState }, beforeWait) ||
    !grantResultStateCarriesExactClosure(grant, beforeWait) ||
    !generationBindingsUnchanged(grant.state, finalState, grant.generation_index, grant.iteration_index)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the durable grant or iteration bindings",
      finalState,
    );
  }
}

/**
 * Validate, compose and complete the continue-stage intervention through
 * the existing controllers (see the module docstring for the full order
 * and durability semantics).
 */
export async function completePipelineV2ContinueStageWithIo(
  ops: PipelineV2ContinueStageCompletionOps,
  options: unknown,
): Promise<CompletedPipelineV2ContinueStage> {
  // Capture boundary: every options field is read exactly once
  // (`runRoot` → `sink` → `intent`), all references are captured before
  // the first await, and later caller mutations cannot influence the
  // execution. The intent's fields are not read here: the provenance
  // authority stays inside the grant controller.
  if (!isRecord(options)) {
    throw completionError("invalid_options", "completePipelineV2ContinueStage requires an options object", null);
  }
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const intent = options["intent"];
  if (typeof runRoot !== "string") {
    throw completionError("invalid_options", "completePipelineV2ContinueStage requires a runRoot string", null);
  }
  if (!isRecord(sink)) {
    throw completionError("invalid_options", "completePipelineV2ContinueStage requires a sink object", null);
  }
  if (!isRecord(intent)) {
    throw completionError("invalid_options", "completePipelineV2ContinueStage requires a prepared wait intent object", null);
  }
  // The per-call ops getters are read exactly once before the first await.
  const applyGrant = ops.applyGrant;
  const recordWaitAction = ops.recordWaitAction;
  if (typeof applyGrant !== "function" || typeof recordWaitAction !== "function") {
    throw completionError("invalid_options", "the completion controller requires its grant and wait action operations", null);
  }
  // Step 2: apply or confirm the grant through the existing controller;
  // its provenance gate, state validation, bindings, reconciliation,
  // dispatch verification and durability mapping are authoritative and
  // its typed errors keep their identity.
  const grant = await applyGrant({ sink: sink as unknown as Parameters<typeof applyPipelineV2ContinueStageGrant>[0]["sink"], intent: intent as never });
  // The intent's fields may be read only after the grant result was
  // produced: the production grant controller already ran the provenance
  // gate over this exact object.
  if (!isRecord(grant)) {
    throw completionError("invalid_result", "the applied grant result is not an object", null);
  }
  const manifest = (intent as unknown as PreparedPipelineV2RunWaitIntent).manifest as PipelineV2ContinueStageIntentManifest;
  if (
    grant.wait_index !== manifest.wait_index ||
    grant.additional_iterations !== manifest.additional_iterations ||
    grant.intent_sha256 !== (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256 ||
    !isPositiveSafeInteger(grant.generation_index) ||
    !isPositiveSafeInteger(grant.iteration_index) ||
    !isRecord(grant.state)
  ) {
    throw completionError(
      "invalid_result",
      "the applied grant result does not match the accepted continue_stage intent",
      isRecord(grant.state) ? (grant.state as PipelineV2RunState) : null,
    );
  }
  // The full pre-response verification of the grant result state: the
  // target wait, the accepted intent, the declared action, the boundary
  // form, the exact grant, the generation/plan bindings and the exact
  // grant closure must all hold before any response filesystem work or
  // dispatch. Field accesses are defensive; a structurally inconsistent
  // injected result becomes the controller's own `invalid_result`, never
  // a leaked `TypeError`.
  const intentSha256 = (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256;
  try {
    verifyGrantResultBeforeResponse(grant, manifest, intentSha256);
  } catch (cause) {
    if (cause instanceof PipelineV2ContinueStageCompletionControllerError) {
      throw cause;
    }
    throw completionError(
      "invalid_result",
      "the applied grant result state is structurally inconsistent",
      isRecord(grant.state) ? (grant.state as PipelineV2RunState) : null,
    );
  }
  const beforeWait = grant.state.waits[grant.wait_index - 1] as PipelineV2WaitRecord;
  // Step 4: record the continue_stage response through the existing
  // generic wait controller; its publication, acceptance, dispatch and
  // durability semantics are authoritative and its typed errors keep
  // their identity.
  const response = await recordWaitAction({
    runRoot,
    sink: sink as unknown as Parameters<typeof recordPipelineV2WaitAction>[0]["sink"],
    waitIndex: grant.wait_index,
    actionId: CONTINUE_STAGE_ACTION_ID,
  });
  // Step 5: verify the response result against the pre-response wait of
  // the verified grant result — never against the hostile result's own
  // final wait. The wait bindings (index, transition count, state id,
  // reason, request digest, ordered actions, journal position, accepted
  // intent) must be unchanged; the only allowed change is the exact
  // continue_stage response; the request digest and the routing target
  // are taken from the pre-response wait; the final state pins the exact
  // post-response boundary (the cursor count and the transition/execution
  // journals at the pre-response wait's boundary); the C4 boundary keeps
  // the pre-response response binding unchanged; and the final closure is
  // checked against the original wait anchor. Field accesses are
  // defensive; a structurally inconsistent injected result becomes the
  // controller's own `invalid_result`, never a leaked `TypeError`.
  try {
    verifyResponseResult(response, grant, beforeWait, intentSha256);
  } catch (cause) {
    if (cause instanceof PipelineV2ContinueStageCompletionControllerError) {
      throw cause;
    }
    throw completionError(
      "invalid_result",
      "the recorded wait response result is structurally inconsistent",
      isRecord(response) && isRecord((response as Record<string, unknown>).state)
        ? ((response as Record<string, unknown>).state as PipelineV2RunState)
        : null,
    );
  }
  // Step 6: the unified content-free result with the response
  // controller's authoritative state.
  return deepFreezeValue({
    wait_index: grant.wait_index,
    generation_index: grant.generation_index,
    iteration_index: grant.iteration_index,
    additional_iterations: grant.additional_iterations,
    intent_sha256: intentSha256,
    request_sha256: response.request_sha256,
    response_sha256: response.response_sha256,
    action_id: CONTINUE_STAGE_ACTION_ID,
    action_to: response.action_to,
    state: response.state,
  });
}
