import {
  PipelineV2ContinueStageIntentControllerError,
  acceptPipelineV2ContinueStageIntentWithIo,
  productionContinueStageIntentOps,
  type AcceptedPipelineV2ContinueStageIntent,
  type AcceptPipelineV2ContinueStageIntentOptions,
  type PipelineV2ContinueStageIntentControllerFailureReason,
  type PipelineV2ContinueStageIntentControllerOps,
  type PipelineV2ContinueStageIntentControllerSink,
} from "./pipeline_v2_continue_stage_intent_controller_internal.ts";

/**
 * Production-neutral acceptance controller for `continue_stage_intent`
 * wait intents (unwired).
 *
 * The public API accepts one provenance-registered prepared
 * `continue_stage_intent` of the run-plan manifest substrate, binds it
 * against the durable schema-v7 run state (the waiting run, its open wait
 * record with a declared `continue_stage` action, the open stage
 * generation and its open iteration, the generation's plan binding), loads
 * the authoritative plan revision manifest from the existing run-plan
 * store, validates the binding through the existing
 * `validateContinueIntentBinding`, pre-checks the existing
 * `plan_intent_accepted` command through the single reducer, publishes
 * the intent through the existing `publishPipelineV2WaitIntent` and only
 * then dispatches the durable command through the structural sink
 * (satisfied by the production `PipelineV2RunStateSink` without an
 * adapter). The controller owns no successor rules, no policy, no routing
 * target and no durable-state acceptance beyond this one command.
 *
 * Reconciliation: a wait without a durable intent is pre-checked,
 * published and dispatched; an exact durable intent digest is an
 * idempotent retry (publication re-verified or restored, no second
 * dispatch); a different durable digest is a typed `intent_conflict`
 * before any filesystem read. After every dispatch the authoritative sink
 * snapshot is re-read and must carry exactly the accepted intent digest
 * inside the same wait record with its binding fields unchanged. Sink
 * `not_committed` keeps the intent file as an orphan with the previous
 * snapshot authoritative; sink `durability_unknown` adopts the visible
 * candidate and dispatches nothing further. Nothing is ever rolled back.
 *
 * Runtime export surface is exactly two keys:
 * `PipelineV2ContinueStageIntentControllerError` and
 * `acceptPipelineV2ContinueStageIntent`. The full algorithm, capture
 * order, durability semantics and honest boundaries are documented in
 * `pipeline_v2_continue_stage_intent_controller_internal.ts`.
 */
export {
  PipelineV2ContinueStageIntentControllerError,
  type PipelineV2ContinueStageIntentControllerFailureReason,
  type PipelineV2ContinueStageIntentControllerSink,
  type AcceptPipelineV2ContinueStageIntentOptions,
  type AcceptedPipelineV2ContinueStageIntent,
};

/**
 * Validates, binds and accepts one provenance-registered
 * `continue_stage_intent` for the open wait of the durable run (see the
 * module docstring). The returned result is deep-frozen and content-free:
 * the wait index, the accepted intent digest and the last authoritative
 * durable state.
 */
export async function acceptPipelineV2ContinueStageIntent(
  options: AcceptPipelineV2ContinueStageIntentOptions,
): Promise<AcceptedPipelineV2ContinueStageIntent> {
  return await acceptPipelineV2ContinueStageIntentWithIo(productionContinueStageIntentOps, options);
}
