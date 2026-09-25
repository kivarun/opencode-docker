import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunPlanStoreError,
  loadPipelineV2PlanRevision,
  publishPipelineV2WaitIntent,
  type PublishedPipelineV2RunPlanRevision,
  type PublishedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_store.ts";
import {
  PipelineV2RunPlanBindingError,
  validateContinueIntentBinding,
} from "./pipeline_v2_run_plan_bindings.ts";
import { PipelineV2RunStateDurabilityError, PipelineV2RunStateStoreError } from "./pipeline_v2_state_store.ts";
import type { PreparedPipelineV2RunWaitIntent } from "./pipeline_v2_run_plan_manifests.ts";
import type { PipelineV2ContinueStageIntentManifest } from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral acceptance controller for `continue_stage_intent`
 * wait intents (unwired).
 *
 * The controller accepts one provenance-registered prepared wait intent
 * (the exact deep-frozen object of the run-plan manifest substrate, and
 * strictly the `continue_stage_intent` kind), binds it against the
 * current durable run — the waiting status, the open wait record, the
 * declared `continue_stage` action, the open stage generation and its
 * open iteration, the generation's plan binding — loads the authoritative
 * plan revision manifest from the existing run-plan filesystem store,
 * validates the binding through the existing
 * `validateContinueIntentBinding`, pre-checks the existing
 * `plan_intent_accepted` command through the single reducer, publishes
 * the intent through the existing `publishPipelineV2WaitIntent` and only
 * then dispatches the durable command through the structural sink. The
 * controller owns no successor rules, no policy and no routing: the
 * action's target stays the property of the declared wait action and the
 * future response layer, and the fact that `continue_stage` is allowed is
 * determined by the declared action, never by the wait's reason string.
 *
 * Validation order (fail-closed): the options shape; every options field
 * read exactly once (`runRoot` → `sink` → `intent`); the sink's
 * `poisoned`, `dispatch` and initial `snapshot` members captured exactly
 * once as opaque references with `dispatch` bound to the sink before the
 * first await; the poisoned-sink latch; the intent provenance gate
 * (registry lookup — hand-built, cast, spread, `structuredClone` and
 * Proxy look-alikes are rejected before any field of the intent or of the
 * durable snapshot is read, Proxy traps never invoked); the provenance
 * kind must be exactly `continue_stage_intent`; only then the single
 * `validatePipelineV2RunState` of the durable snapshot and the durable
 * bindings. A hostile extra options field is ignored.
 *
 * Reconciliation: a wait without a durable intent is pre-checked
 * (reducer, local snapshot — a rejection is `invalid_state` with zero
 * filesystem effects), published and dispatched; an exact durable intent
 * digest is an idempotent retry (the publication is re-verified or
 * restored, no second dispatch, the authoritative state returned); a
 * different durable digest is a typed `intent_conflict` with zero
 * filesystem writes and zero dispatch, checked before the plan artifact
 * load. After every dispatch the authoritative sink snapshot is re-read
 * and must carry exactly the accepted intent digest inside the same wait
 * record with its binding fields unchanged; a racing identical dispatch
 * is idempotent success only on that exact match. Sink `not_committed`
 * keeps the intent file as an orphan with the previous snapshot
 * authoritative; sink `durability_unknown` adopts the visible candidate,
 * poisons the sink and dispatches nothing further. Nothing is ever rolled
 * back.
 *
 * Errors are a closed typed contract (`PipelineV2ContinueStageIntentControllerError`
 * with the closed reason set `invalid_intent` | `invalid_state` |
 * `intent_conflict` | `state_persist_failed` and the last authoritative
 * state — `null` when none exists): hostile options, a non-registered or
 * foreign-kind intent are `invalid_intent`; an invalid or missing durable
 * state, a missing open wait/generation/iteration/plan, a durable-plan
 * mismatch and a hostile publication result are `invalid_state`; a
 * different durable intent is `intent_conflict`; sink commit failures are
 * `state_persist_failed`. Existing manifest, binding and store errors
 * keep their original classes and identity. Diagnostics are content-free
 * (validated safe ids and indexes only — no digest values, canonical
 * JSON, paths, bodies, env values or credentials); unexpected causes
 * propagate unchanged without message parsing.
 *
 * Not implemented (stays unwired): `iteration_grant_recorded`, the
 * wait-bound `stage_iteration_closed {by: "grant"}`, the response
 * manifest and `wait_response_recorded`, `revise_task_intent`, task
 * revision publication/acceptance, the continue/revise action policy,
 * automatic resume, coordinator/runner/CLI wiring, schema changes,
 * migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ContinueStageIntentControllerFailureReason =
  | "invalid_intent"
  | "invalid_state"
  | "intent_conflict"
  | "state_persist_failed";

export class PipelineV2ContinueStageIntentControllerError extends Error {
  readonly reason: PipelineV2ContinueStageIntentControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ContinueStageIntentControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ContinueStageIntentControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam; the production `PipelineV2RunStateSink`
 * satisfies it without an adapter. The initial `snapshot` may be `null`
 * (no durable run); after every dispatch the sink's authoritative
 * snapshot is re-read — never memoized.
 */
export interface PipelineV2ContinueStageIntentControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface AcceptPipelineV2ContinueStageIntentOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2ContinueStageIntentControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

export interface AcceptedPipelineV2ContinueStageIntent {
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call structural ops of the internal core: the plan revision
 * loader and the wait-intent publisher of the existing run-plan store.
 * One frozen production object binds them to the public store functions;
 * tests inject their own per-call object. There is no mutable
 * module-global seam and no installer.
 */
export interface PipelineV2ContinueStageIntentControllerOps {
  readonly loadPlanRevision: (
    runRoot: string,
    revision: number,
  ) => Promise<PublishedPipelineV2RunPlanRevision | null>;
  readonly publishWaitIntent: (
    runRoot: string,
    manifest: unknown,
  ) => Promise<PublishedPipelineV2RunWaitIntent>;
}

export const productionContinueStageIntentOps: PipelineV2ContinueStageIntentControllerOps = Object.freeze({
  loadPlanRevision: (runRoot: string, revision: number) => loadPipelineV2PlanRevision(runRoot, revision),
  publishWaitIntent: (runRoot: string, manifest: unknown) => publishPipelineV2WaitIntent(runRoot, manifest),
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2ContinueStageIntentControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ContinueStageIntentControllerError {
  return new PipelineV2ContinueStageIntentControllerError(reason, message, state);
}

function invalidIntent(message: string): PipelineV2ContinueStageIntentControllerError {
  return controllerError("invalid_intent", message, null);
}

const CONTINUE_STAGE_ACTION_ID = "continue_stage";

/**
 * The binding fields of one wait record that an intent acceptance must
 * not change: everything except the accepted intent and the response.
 */
function waitBindingMatches(before: PipelineV2WaitRecord, after: PipelineV2WaitRecord): boolean {
  if (
    before.index !== after.index ||
    before.transition_count !== after.transition_count ||
    before.state_id !== after.state_id ||
    before.reason !== after.reason ||
    before.request_sha256 !== after.request_sha256 ||
    after.response !== undefined ||
    before.actions.length !== after.actions.length
  ) {
    return false;
  }
  return before.actions.every((action, position) => {
    const other = after.actions[position];
    return other !== undefined && other.id === action.id && other.to === action.to;
  });
}

/**
 * The durable authoritative state after a dispatch must carry exactly the
 * accepted intent digest inside the same wait record, with its binding
 * fields unchanged.
 */
function waitCarriesIntent(
  state: PipelineV2RunState | null,
  waitIndex: number,
  intentSha256: string,
  before: PipelineV2WaitRecord,
): boolean {
  if (state === null) {
    return false;
  }
  let after: PipelineV2WaitRecord | undefined;
  for (const record of state.waits) {
    if (record.index === waitIndex) {
      after = record;
    }
  }
  if (after === undefined) {
    return false;
  }
  if (after.intent?.intent_sha256 !== intentSha256) {
    return false;
  }
  return waitBindingMatches(before, after);
}

/**
 * The published wait intent must match the accepted provenance intent
 * exactly (kind, run id, wait index, canonical JSON and digest); a
 * hostile publisher result fails closed before any dispatch.
 */
function requirePublishedIntent(
  published: PublishedPipelineV2RunWaitIntent,
  intent: PreparedPipelineV2RunWaitIntent,
  state: PipelineV2RunState,
): void {
  if (
    published.intent.manifest.kind !== "continue_stage_intent" ||
    published.intent.manifest.run_id !== intent.manifest.run_id ||
    published.intent.manifest.wait_index !== intent.manifest.wait_index ||
    published.intent.canonical_json !== intent.canonical_json ||
    published.intent.sha256 !== intent.sha256
  ) {
    throw controllerError(
      "invalid_state",
      "the published wait intent manifest does not match the accepted continue_stage intent",
      state,
    );
  }
}

/**
 * The reducer pre-check of the `plan_intent_accepted` command on a local
 * snapshot, before any filesystem side effect: a reducer rejection is a
 * typed `invalid_state` with zero dispatch; any other cause propagates
 * unchanged. Through loader-valid states the reconciliation already
 * proves every reducer precondition, so this is defense-in-depth; the
 * helper is the single pre-check path of both the flow and its
 * error-contract test.
 */
export function precheckPlanIntentAcceptance(
  state: PipelineV2RunState,
  command: PipelineV2RunCommand,
  snapshot: PipelineV2RunState,
): void {
  try {
    reducePipelineV2RunCommand(state, command, new Date());
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "the current run state does not accept the plan intent acceptance",
        snapshot,
      );
    }
    throw cause;
  }
}

/**
 * Validate, bind and accept one `continue_stage_intent` through the
 * existing reducer, plan store and binding validator (see the module
 * docstring for the full order and durability semantics).
 */
export async function acceptPipelineV2ContinueStageIntentWithIo(
  ops: PipelineV2ContinueStageIntentControllerOps,
  options: unknown,
): Promise<AcceptedPipelineV2ContinueStageIntent> {
  // Capture boundary: every options field is read exactly once
  // (`runRoot` → `sink` → `intent`), and the sink's `poisoned`, `dispatch`
  // and initial `snapshot` members are read exactly once as opaque
  // references. No field of the intent or of the durable snapshot is read
  // here.
  if (!isRecord(options)) {
    throw invalidIntent("acceptPipelineV2ContinueStageIntent requires an options object");
  }
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const intent = options["intent"];
  if (typeof runRoot !== "string") {
    throw invalidIntent("acceptPipelineV2ContinueStageIntent requires a runRoot string");
  }
  if (!isRecord(sink)) {
    throw invalidIntent("acceptPipelineV2ContinueStageIntent requires a sink object");
  }
  if (!isRecord(intent)) {
    throw invalidIntent("acceptPipelineV2ContinueStageIntent requires a prepared wait intent object");
  }
  const poisoned = sink["poisoned"];
  const dispatch = sink["dispatch"];
  const initialSnapshot = sink["snapshot"];
  if (typeof poisoned !== "boolean") {
    throw invalidIntent("the run state sink requires a boolean poisoned flag");
  }
  if (typeof dispatch !== "function") {
    throw invalidIntent("the run state sink requires a dispatch function");
  }
  const loadPlanRevision = ops.loadPlanRevision;
  const publishWaitIntent = ops.publishWaitIntent;
  if (typeof loadPlanRevision !== "function" || typeof publishWaitIntent !== "function") {
    throw invalidIntent("the continue stage intent controller requires its plan loader and intent publisher");
  }
  const sinkRef = sink as unknown as PipelineV2ContinueStageIntentControllerSink;
  // The dispatch is bound to the sink immediately at capture: a later
  // reassignment of the sink's member cannot change the dispatch target.
  const dispatchCommand = (command: PipelineV2RunCommand): Promise<unknown> =>
    Promise.resolve((dispatch as (...args: unknown[]) => unknown).call(sink, command));
  // The fail-closed poison latch: a poisoned sink accepts no acceptance.
  if (poisoned) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned; no plan intent can be accepted",
      null,
    );
  }
  // The intent provenance gate: the exact registered prepared object of
  // the manifest substrate, and strictly the continue-stage kind. Hand
  // -built, cast, spread, cloned and Proxy look-alikes are rejected here,
  // before any field of the intent or of the durable snapshot is read.
  if (!hasPreparedRunPlanProvenance(intent, "continue_stage_intent")) {
    throw invalidIntent("the intent is not a provenance-registered continue_stage_intent");
  }
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  // The single state validation of the durable snapshot.
  const state = validatePipelineV2RunState(initialSnapshot as unknown as PipelineV2RunState | null);
  // The provenance gate above guarantees the exact continue-stage kind;
  // the manifest is the frozen continue-stage form from here on.
  const manifest = preparedIntent.manifest as PipelineV2ContinueStageIntentManifest;
  // The durable boundary: the waiting run, its open wait record and its
  // declared continue_stage action.
  if (state.status !== "waiting" || state.phase !== "waiting") {
    throw controllerError(
      "invalid_state",
      "the run is not waiting; a continue_stage intent is accepted only inside the open wait",
      state,
    );
  }
  const wait = state.waits[state.waits.length - 1];
  if (wait === undefined || wait.response !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no open wait record; a continue_stage intent is accepted only inside the open wait",
      state,
    );
  }
  if (manifest.run_id !== state.run_id) {
    throw controllerError(
      "invalid_state",
      "the wait intent names another run than the durable run state",
      state,
    );
  }
  if (manifest.wait_index !== wait.index) {
    throw controllerError(
      "invalid_state",
      `the wait intent names wait index ${manifest.wait_index}, but the open wait record is ${wait.index}`,
      state,
    );
  }
  if (!wait.actions.some((action) => action.id === CONTINUE_STAGE_ACTION_ID)) {
    throw controllerError(
      "invalid_state",
      "the open wait does not declare the continue_stage action",
      state,
    );
  }
  // The durable plan revision must exist before the open stage generation
  // is examined; the generation then has to belong to it.
  const lastPlanRecord = state.plan_revisions[state.plan_revisions.length - 1];
  if (lastPlanRecord === undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no durable plan revision for the wait intent",
      state,
    );
  }
  // The open stage generation and its open iteration.
  const generation = state.generations[state.generations.length - 1];
  if (generation === undefined || generation.closed !== undefined) {
    throw controllerError(
      "invalid_state",
      "the run has no open stage generation for the wait intent",
      state,
    );
  }
  if (generation.open_iteration === undefined) {
    throw controllerError(
      "invalid_state",
      `the stage generation ${generation.index} carries no open iteration for the wait intent`,
      state,
    );
  }
  if (manifest.stage_id !== generation.stage_id) {
    throw controllerError(
      "invalid_state",
      `the wait intent names stage ${JSON.stringify(manifest.stage_id)}, but the open stage generation ${generation.index} belongs to stage ${JSON.stringify(generation.stage_id)}`,
      state,
    );
  }
  if (generation.plan_sha256 !== lastPlanRecord.sha256) {
    throw controllerError(
      "invalid_state",
      `the open stage generation ${generation.index} does not belong to the last durable plan revision ${lastPlanRecord.revision}`,
      state,
    );
  }
  // A different durable intent is a conflict before any filesystem read.
  if (wait.intent !== undefined && wait.intent.intent_sha256 !== intent.sha256) {
    throw controllerError(
      "intent_conflict",
      `the open wait ${wait.index} already accepted a different continue_stage intent; one intent belongs to one wait`,
      state,
    );
  }
  // The authoritative plan revision: loaded only from the durable ledger
  // through the existing run-plan store, then bound to the durable record
  // exactly.
  const loadedPlan = await loadPlanRevision(runRoot, lastPlanRecord.revision);
  if (loadedPlan === null) {
    throw controllerError(
      "invalid_state",
      `the durable plan revision ${lastPlanRecord.revision} is not published on the run's data plane`,
      state,
    );
  }
  const plan = loadedPlan.plan;
  if (
    plan.manifest.run_id !== state.run_id ||
    plan.manifest.revision !== lastPlanRecord.revision ||
    plan.sha256 !== lastPlanRecord.sha256 ||
    plan.manifest.previous_sha256 !== lastPlanRecord.previous_sha256 ||
    plan.manifest.origin_execution !== lastPlanRecord.origin_execution
  ) {
    throw controllerError(
      "invalid_state",
      `the published plan revision ${lastPlanRecord.revision} does not match the durable plan record`,
      state,
    );
  }
  // The existing binding validator is the only binding authority; its
  // errors keep their original class.
  validateContinueIntentBinding({ intent: preparedIntent, plan });
  const command: PipelineV2RunCommand = {
    kind: "plan_intent_accepted",
    waitIndex: manifest.wait_index,
    intentSha256: preparedIntent.sha256,
  };
  if (wait.intent === undefined) {
    // The reducer pre-check on the local snapshot, before any filesystem
    // side effect.
    precheckPlanIntentAcceptance(state, command, state);
    // Publish through the existing store; its errors keep their original
    // class and identity (a conflict leaves nothing behind).
    const published = await publishWaitIntent(runRoot, manifest);
    requirePublishedIntent(published, preparedIntent, state);
    // The durable dispatch, then the authoritative verification.
    try {
      await dispatchCommand(command);
    } catch (cause) {
      if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError(
          "state_persist_failed",
          "the plan intent acceptance could not be confirmed durable",
          sinkRef.snapshot,
        );
      }
      if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          "the plan intent acceptance could not be committed",
          sinkRef.snapshot,
        );
      }
      if (cause instanceof PipelineV2StateError) {
        // A racing identical dispatch is idempotent success only on the
        // exact durable record.
        const after = sinkRef.snapshot;
        if (after !== null && waitCarriesIntent(after, manifest.wait_index, preparedIntent.sha256, wait)) {
          return deepFreezeValue({
            wait_index: manifest.wait_index,
            intent_sha256: preparedIntent.sha256,
            state: after,
          });
        }
        throw controllerError(
          "invalid_state",
          `the run state rejected the plan intent acceptance and does not carry it in the open wait ${manifest.wait_index}`,
          after,
        );
      }
      throw cause;
    }
    const after = sinkRef.snapshot;
    if (after === null || !waitCarriesIntent(after, manifest.wait_index, preparedIntent.sha256, wait)) {
      throw controllerError(
        "invalid_state",
        `the committed run state does not carry the accepted intent in the open wait ${manifest.wait_index}`,
        after,
      );
    }
    return deepFreezeValue({
      wait_index: manifest.wait_index,
      intent_sha256: preparedIntent.sha256,
      state: after,
    });
  }
  // The exact durable intent: idempotent retry. The publication is
  // re-verified or restored; no second dispatch happens.
  const published = await publishWaitIntent(runRoot, manifest);
  requirePublishedIntent(published, preparedIntent, state);
  return deepFreezeValue({
    wait_index: manifest.wait_index,
    intent_sha256: preparedIntent.sha256,
    state,
  });
}
