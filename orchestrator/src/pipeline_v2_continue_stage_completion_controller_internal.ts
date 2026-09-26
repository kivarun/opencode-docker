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
 * Grant result verification (the contract-owned values): the wait index,
 * the additional iteration count and the intent digest must equal the
 * accepted intent's, the generation and iteration indexes must be
 * positive safe integers, and the result state must carry the exact
 * durable grant and the exact grant closure. A hostile injected result is
 * the controller's own typed `invalid_result` with zero response
 * publication and zero dispatch.
 *
 * Response result verification: the wait index and the action id must
 * match, the request digest must equal the durable target wait's request
 * digest, the response digest must equal the durable wait's recorded
 * response digest, the routing target must equal the declared
 * `continue_stage` action's target, the final state must be active and
 * running with the cursor at the action target, the target wait must keep
 * the exact accepted intent digest and the exact response, and the exact
 * grant, the exact grant closure and the generation/iteration bindings of
 * the grant result's state must be unchanged. A hostile injected result is
 * the controller's own typed `invalid_result`.
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
    beforeGeneration.opened_transition_count !== afterGeneration.opened_transition_count
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
  const beforeWait = grant.state.waits[grant.wait_index - 1];
  if (
    !grantResultStateCarriesExactGrant(grant, (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256) ||
    beforeWait === undefined ||
    beforeWait.index !== grant.wait_index
  ) {
    throw completionError(
      "invalid_result",
      "the applied grant result state does not carry the exact durable grant",
      grant.state,
    );
  }
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
  // Step 5: verify the response result against the durable target wait.
  if (!isRecord(response)) {
    throw completionError("invalid_result", "the recorded wait response result is not an object", null);
  }
  const finalState = response.state;
  const finalWait = finalState.waits[grant.wait_index - 1];
  const declaredAction = finalWait?.actions.find((action) => action.id === CONTINUE_STAGE_ACTION_ID);
  if (
    response.wait_index !== grant.wait_index ||
    response.action_id !== CONTINUE_STAGE_ACTION_ID ||
    finalWait === undefined ||
    response.request_sha256 !== finalWait.request_sha256 ||
    finalWait.response === undefined ||
    response.response_sha256 !== finalWait.response.response_sha256 ||
    finalWait.response.action_id !== CONTINUE_STAGE_ACTION_ID ||
    finalWait.intent?.intent_sha256 !== (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256 ||
    declaredAction === undefined ||
    response.action_to !== declaredAction.to ||
    finalState.status !== "active" ||
    finalState.phase !== "running" ||
    finalState.cursor.current_state !== declaredAction.to
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not match the durable target wait",
      finalState,
    );
  }
  // The exact grant, the exact grant closure and the generation/iteration
  // bindings of the grant result's state must be unchanged in the final
  // state.
  if (
    !grantResultStateCarriesExactGrant({ ...grant, state: finalState }, (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256) ||
    !grantResultStateCarriesExactClosure({ ...grant, state: finalState }, finalWait) ||
    !grantResultStateCarriesExactClosure(grant, beforeWait) ||
    !generationBindingsUnchanged(grant.state, finalState, grant.generation_index, grant.iteration_index)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the durable grant or iteration bindings",
      finalState,
    );
  }
  // Step 6: the unified content-free result with the response
  // controller's authoritative state.
  return deepFreezeValue({
    wait_index: grant.wait_index,
    generation_index: grant.generation_index,
    iteration_index: grant.iteration_index,
    additional_iterations: grant.additional_iterations,
    intent_sha256: (intent as unknown as PreparedPipelineV2RunWaitIntent).sha256,
    request_sha256: response.request_sha256,
    response_sha256: response.response_sha256,
    action_id: CONTINUE_STAGE_ACTION_ID,
    action_to: response.action_to,
    state: finalState,
  });
}
