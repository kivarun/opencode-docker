import { basename } from "node:path";
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import {
  PipelineV2WaitManifestError,
  acceptPipelineV2WaitResponse,
  preparePipelineV2WaitRequest,
  type AcceptedPipelineV2WaitResponse,
  type PipelineV2WaitManifestAction,
  type PreparedPipelineV2WaitRequest,
} from "./pipeline_v2_wait_manifest.ts";
import {
  PipelineV2WaitStoreError,
  publishPipelineV2WaitRequest,
  publishPipelineV2WaitResponse,
} from "./pipeline_v2_wait_store.ts";

/**
 * Production-neutral wait controller for pipeline schema v2 (unwired).
 *
 * This module is the single layer that owns the agreed order between the
 * filesystem publication of the wait manifests and the durable state:
 *
 *   publish request  → durable run_waiting
 *   publish response → durable wait_response_recorded
 *
 * Filesystem publication always happens before the durable commit; a
 * published manifest without its durable reference stays an orphan and is
 * never part of the history, and an exact retry safely reuses it. The
 * controller owns no successor rules of its own: the reducer
 * (`reducePipelineV2RunCommand`) is consulted directly as the pre-check and
 * the sink (`PipelineV2RunStateSink`) remains the only writer of the
 * durable state. The wait manifest module stays the only manifest
 * authority and the wait store the only filesystem publication authority.
 *
 * Request flow (`enterPipelineV2Wait`): the caller supplies only the
 * policy `reason` and the declared `actions`; `run_id`, `wait_index`,
 * `transition_count` and `state_id` are derived exclusively from the
 * authoritative sink snapshot (`wait_index = waits.length + 1`,
 * `transition_count`/`state_id` from the cursor). The controller prepares
 * the manifest (pure), pre-checks the exact `run_waiting` command against
 * the reducer — before any filesystem side effect — publishes the request
 * manifest under `<runRoot>/waits/<waitIndex>.request.json`, verifies that
 * the publisher returned the derived manifest and digest, and only then
 * dispatches `run_waiting` through the sink. The post-dispatch state is
 * read from the authoritative sink snapshot, never from a pre-computed
 * candidate.
 *
 * Idempotent request retry: when the last durable wait record is still
 * open, the call is a re-entry: the request manifest is reconstructed from
 * the durable record, the caller's `reason`/`actions` must match it
 * exactly (anything else is a typed conflict without any write), the
 * filesystem publication is verified or restored through the existing
 * publisher, the digest must equal the durable `request_sha256`, no second
 * `run_waiting` is dispatched, and the ordinary success result is returned.
 * After a lost durable commit (`not_committed`), the retry reuses the
 * orphan request file byte-for-byte (the publisher adopts it) and repeats
 * the dispatch. After a reducer rejection of a racing identical dispatch,
 * the controller accepts the call as idempotent success only after the
 * authoritative snapshot is verified to carry exactly the same digest and
 * payload.
 *
 * Response flow (`recordPipelineV2WaitResponse`): the caller supplies only
 * the `waitIndex` and the raw response document. The controller requires
 * the durable wait record, reconstructs the request manifest exclusively
 * from the durable record, verifies or restores the canonical request file
 * (its digest must equal the durable `request_sha256`), accepts the raw
 * response through the manifest module against that request, and only then
 * publishes the response manifest and dispatches `wait_response_recorded`
 * with the digests and action id taken from the accepted manifests. The
 * routing target is never accepted from the caller; it is the accepted
 * response's `action_to`. A wait that already carries a durable response
 * accepts only the identical response (same action id and response
 * digest), verifies or restores both filesystem manifests, dispatches
 * nothing, and returns success — this works across restarts through the
 * durable record, not through object identity.
 *
 * Durability semantics: a wait-store failure before the dispatch leaves
 * the state untouched and dispatches nothing (`not_published` failures are
 * `wait_conflict` for a busy target, otherwise `wait_storage_failed`); a
 * wait-store `durability_unknown` behaves the same — the final file may
 * exist and an exact retry confirms it first. A sink `not_committed`
 * leaves the published manifest as an orphan with the previous snapshot
 * authoritative and returns `state_persist_failed` without an automatic
 * second dispatch; a sink `durability_unknown` adopts the visible
 * candidate snapshot (the sink poisons itself), returns
 * `state_persist_failed`, and a fresh controller with a freshly loaded
 * sink later recognizes the durable record as idempotent success.
 * Published manifests are never removed on state failures.
 *
 * Capture boundary: the options and sink shapes are validated and the
 * `dispatch` function is captured exactly once — bound to the sink — in
 * the synchronous prefix before any filesystem or sink side effect; the
 * caller's objects are never frozen or modified, and reassigning
 * `sink.dispatch` afterwards cannot influence the running call. There is
 * no module-global mutable seam.
 *
 * Failure contract (closed, typed — classified by error class and typed
 * fields, never by message text): `PipelineV2WaitControllerError` carries
 * the immutable `reason` (`invalid_state`, `invalid_request`,
 * `invalid_response`, `wait_conflict`, `wait_storage_failed`,
 * `state_persist_failed`), the `operation` (`enter_wait` |
 * `record_response`) and the last authoritative `state | null`. Manifest
 * validation failures map to `invalid_request`/`invalid_response`, wait
 * store conflicts to `wait_conflict`, other wait store failures to
 * `wait_storage_failed`, reducer rejections to `invalid_state` (after the
 * idempotency check) and store/sink commit failures to
 * `state_persist_failed`. Unexpected errors propagate unchanged and are
 * never masked as expected user-facing failures. Diagnostics are
 * content-free: no manifest bodies, action values, response documents,
 * facts, credentials or parser fragments.
 *
 * Not implemented (stays unwired): the coordinator, the production runner
 * and the CLI never call this module yet; the user response is supplied as
 * a raw string, never read from a file; resume, P01 validation, TASK
 * revision, iteration budgets, model-profile replacements, API/T3 and
 * migrations are later increments.
 */

export interface PipelineV2WaitControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export type PipelineV2WaitControllerOperation = "enter_wait" | "record_response";

export type PipelineV2WaitControllerFailureReason =
  | "invalid_state"
  | "invalid_request"
  | "invalid_response"
  | "wait_conflict"
  | "wait_storage_failed"
  | "state_persist_failed";

export class PipelineV2WaitControllerError extends Error {
  readonly reason: PipelineV2WaitControllerFailureReason;
  readonly operation: PipelineV2WaitControllerOperation;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2WaitControllerFailureReason,
    operation: PipelineV2WaitControllerOperation,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2WaitControllerError";
    this.reason = reason;
    this.operation = operation;
    this.state = state;
  }
}

export interface EnteredPipelineV2Wait {
  readonly wait_index: number;
  readonly request_sha256: string;
  readonly state: PipelineV2RunState;
}

export interface RecordedPipelineV2WaitResponse {
  readonly wait_index: number;
  readonly request_sha256: string;
  readonly response_sha256: string;
  readonly action_id: string;
  readonly action_to: string;
  readonly state: PipelineV2RunState;
}

export interface EnterPipelineV2WaitOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2WaitControllerSink;
  readonly reason: string;
  readonly actions: readonly PipelineV2WaitManifestAction[];
}

export interface RecordPipelineV2WaitResponseOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2WaitControllerSink;
  readonly waitIndex: number;
  readonly raw: string;
}

interface CapturedSink {
  readonly runRoot: string;
  readonly sink: PipelineV2WaitControllerSink;
  readonly dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  readonly snapshot: PipelineV2RunState;
}

function controllerError(
  reason: PipelineV2WaitControllerFailureReason,
  operation: PipelineV2WaitControllerOperation,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2WaitControllerError {
  return new PipelineV2WaitControllerError(reason, operation, message, state);
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * The synchronous capture boundary: validates the option and sink shapes,
 * captures `dispatch` exactly once (bound to the sink so a later
 * reassignment of `sink.dispatch` cannot influence this call), and reads
 * the authoritative snapshot. No filesystem or sink side effect happens
 * here; every rejection is a typed controller failure.
 */
function captureBoundary(
  options: unknown,
  operation: PipelineV2WaitControllerOperation,
): CapturedSink {
  const what = operation === "enter_wait" ? "enterPipelineV2Wait" : "recordPipelineV2WaitResponse";
  if (!isRecord(options)) {
    throw controllerError("invalid_state", operation, `${what} requires an options object`, null);
  }
  const runRoot = options["runRoot"];
  if (typeof runRoot !== "string" || runRoot === "") {
    throw controllerError("invalid_state", operation, `${what} requires a non-empty run root`, null);
  }
  const sink = options["sink"];
  if (!isRecord(sink)) {
    throw controllerError("invalid_state", operation, `${what} requires a state sink`, null);
  }
  let poisoned: unknown;
  try {
    poisoned = sink["poisoned"];
  } catch {
    throw controllerError("invalid_state", operation, `${what} requires a readable state sink`, null);
  }
  if (poisoned === true) {
    throw controllerError(
      "invalid_state",
      operation,
      "the run state sink is poisoned by a durability-unknown commit; no wait operation is accepted for this run",
      null,
    );
  }
  const dispatch = sink["dispatch"];
  if (typeof dispatch !== "function") {
    throw controllerError("invalid_state", operation, `${what} requires a dispatchable state sink`, null);
  }
  let snapshot: unknown;
  try {
    snapshot = sink["snapshot"];
  } catch {
    throw controllerError("invalid_state", operation, `${what} requires a readable state sink`, null);
  }
  if (!isRecord(snapshot)) {
    throw controllerError("invalid_state", operation, "no durable pipeline v2 run state exists yet", null);
  }
  if (
    snapshot["schema_version"] !== 6 ||
    !isRecord(snapshot["pipeline"]) ||
    (snapshot["pipeline"] as Record<string, unknown>)["schema_version"] !== 2
  ) {
    throw controllerError(
      "invalid_state",
      operation,
      "the durable run state is not a pipeline v2 state document",
      null,
    );
  }
  const runId = snapshot["run_id"];
  const cursor = snapshot["cursor"];
  const waits = snapshot["waits"];
  if (
    typeof runId !== "string" ||
    !isRecord(cursor) ||
    typeof cursor["current_state"] !== "string" ||
    typeof cursor["transition_count"] !== "number" ||
    !Array.isArray(waits)
  ) {
    throw controllerError(
      "invalid_state",
      operation,
      "the durable run state does not carry the cursor and wait journal",
      null,
    );
  }
  if (basename(runRoot) !== runId) {
    throw controllerError(
      "invalid_state",
      operation,
      "the run root does not belong to this run",
      null,
    );
  }
  const state = snapshot as unknown as PipelineV2RunState;
  return {
    runRoot,
    sink: sink as unknown as PipelineV2WaitControllerSink,
    dispatch: (dispatch as (command: PipelineV2RunCommand) => Promise<void>).bind(sink),
    snapshot: state,
  };
}

function currentSnapshot(sink: PipelineV2WaitControllerSink): PipelineV2RunState | null {
  return sink.snapshot;
}

function lastWaitRecord(state: PipelineV2RunState): PipelineV2WaitRecord | undefined {
  return state.waits[state.waits.length - 1];
}

function waitManifestValue(state: PipelineV2RunState, record: PipelineV2WaitRecord): Record<string, unknown> {
  return {
    schema_version: 1,
    run_id: state.run_id,
    wait_index: record.index,
    transition_count: record.transition_count,
    state_id: record.state_id,
    reason: record.reason,
    actions: record.actions,
  };
}

function manifestsEqual(
  left: PreparedPipelineV2WaitRequest,
  right: PreparedPipelineV2WaitRequest,
): boolean {
  const a = left.manifest;
  const b = right.manifest;
  return (
    a.run_id === b.run_id &&
    a.wait_index === b.wait_index &&
    a.transition_count === b.transition_count &&
    a.state_id === b.state_id &&
    a.reason === b.reason &&
    a.actions.length === b.actions.length &&
    a.actions.every((action, index) => {
      const other = b.actions[index];
      return other !== undefined && action.id === other.id && action.to === other.to;
    })
  );
}

/**
 * Maps one wait-store failure by its typed fields: a busy final target is
 * a `wait_conflict`; every other store failure (invalid layout, I/O,
 * durability-unknown) is a `wait_storage_failed`. No message text is ever
 * inspected.
 */
function waitStoreFailure(
  cause: PipelineV2WaitStoreError,
  operation: PipelineV2WaitControllerOperation,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2WaitControllerError {
  const reason = cause.reason === "conflict" ? "wait_conflict" : "wait_storage_failed";
  return controllerError(reason, operation, message, state);
}

/**
 * Reconstructs and verifies the request manifest of one durable wait
 * record: the manifest is prepared from the record's own fields (so the
 * manifest module stays the only authority), the digest must equal the
 * durable `request_sha256`, and the canonical request file is verified or
 * restored through the existing publisher.
 */
async function verifyDurableRequestPublication(
  runRoot: string,
  state: PipelineV2RunState,
  record: PipelineV2WaitRecord,
  operation: PipelineV2WaitControllerOperation,
  stateAtFailure: PipelineV2RunState | null,
): Promise<PreparedPipelineV2WaitRequest> {
  const value = waitManifestValue(state, record);
  let prepared: PreparedPipelineV2WaitRequest;
  try {
    prepared = preparePipelineV2WaitRequest(value);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitManifestError) {
      throw controllerError(
        "invalid_state",
        operation,
        "the durable wait record does not carry a valid request manifest",
        stateAtFailure,
      );
    }
    throw cause;
  }
  if (prepared.sha256 !== record.request_sha256) {
    throw controllerError(
      "invalid_state",
      operation,
      "the durable wait record does not match its own request digest",
      stateAtFailure,
    );
  }
  let published: Awaited<ReturnType<typeof publishPipelineV2WaitRequest>>;
  try {
    published = await publishPipelineV2WaitRequest(runRoot, value);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw waitStoreFailure(
        cause,
        operation,
        operation === "enter_wait"
          ? "the wait request could not be published"
          : "the stored wait request could not be verified",
        stateAtFailure,
      );
    }
    throw cause;
  }
  if (published.request.sha256 !== record.request_sha256) {
    throw new Error(
      "pipeline v2 wait controller invariant violated: the published request digest does not match the durable wait record",
    );
  }
  return published.request;
}

function publishedRequestMatchesDerived(
  published: PreparedPipelineV2WaitRequest,
  expected: PreparedPipelineV2WaitRequest,
): boolean {
  return published.sha256 === expected.sha256 && manifestsEqual(published, expected);
}

export async function enterPipelineV2Wait(
  options: EnterPipelineV2WaitOptions,
): Promise<EnteredPipelineV2Wait> {
  const ctx = captureBoundary(options, "enter_wait");
  const runOptions = options as EnterPipelineV2WaitOptions;
  const state = ctx.snapshot;
  const openWait = lastWaitRecord(state);
  if (openWait !== undefined && openWait.response === undefined) {
    return await reenterOpenWait(ctx, runOptions, openWait);
  }
  return await enterNewWait(ctx, runOptions, state);
}

/**
 * The fresh-wait path: derive the manifest from the authoritative
 * snapshot, pre-check the exact `run_waiting` command against the reducer
 * (before any filesystem side effect), publish the request manifest,
 * verify the publisher result, dispatch through the captured sink
 * function, and verify the authoritative post-dispatch snapshot.
 */
async function enterNewWait(
  ctx: CapturedSink,
  options: EnterPipelineV2WaitOptions,
  state: PipelineV2RunState,
): Promise<EnteredPipelineV2Wait> {
  const waitIndex = state.waits.length + 1;
  const transitionCount = state.cursor.transition_count;
  const stateId = state.cursor.current_state;
  const value = {
    schema_version: 1,
    run_id: state.run_id,
    wait_index: waitIndex,
    transition_count: transitionCount,
    state_id: stateId,
    reason: options.reason,
    actions: options.actions,
  };
  let prepared: PreparedPipelineV2WaitRequest;
  try {
    prepared = preparePipelineV2WaitRequest(value);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitManifestError) {
      throw controllerError(
        "invalid_request",
        "enter_wait",
        "the wait request is not a valid manifest",
        state,
      );
    }
    throw cause;
  }
  const command: PipelineV2RunCommand = {
    kind: "run_waiting",
    stateId,
    reason: prepared.manifest.reason,
    requestSha256: prepared.sha256,
    actions: prepared.manifest.actions,
  };
  try {
    reducePipelineV2RunCommand(state, command, new Date());
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "enter_wait",
        "the current run state does not accept a new wait",
        state,
      );
    }
    throw cause;
  }
  let published: Awaited<ReturnType<typeof publishPipelineV2WaitRequest>>;
  try {
    published = await publishPipelineV2WaitRequest(ctx.runRoot, value);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw waitStoreFailure(
        cause,
        "enter_wait",
        "the wait request could not be published",
        currentSnapshot(ctx.sink),
      );
    }
    throw cause;
  }
  if (!publishedRequestMatchesDerived(published.request, prepared)) {
    throw new Error(
      "pipeline v2 wait controller invariant violated: the published request does not match the derived manifest",
    );
  }
  try {
    await ctx.dispatch(command);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      const after = currentSnapshot(ctx.sink);
      const record =
        after !== null
          ? after.waits.find(
              (candidate) =>
                candidate.index === waitIndex && candidate.request_sha256 === prepared.sha256,
            )
          : undefined;
      if (
        after !== null &&
        record !== undefined &&
        record.index === waitIndex &&
        record.transition_count === transitionCount &&
        record.state_id === stateId &&
        record.reason === prepared.manifest.reason &&
        record.request_sha256 === prepared.sha256 &&
        record.actions.length === prepared.manifest.actions.length &&
        record.actions.every((action, index) => {
          const other = prepared.manifest.actions[index];
          return other !== undefined && action.id === other.id && action.to === other.to;
        })
      ) {
        return deepFreeze({ wait_index: waitIndex, request_sha256: prepared.sha256, state: after });
      }
      throw controllerError(
        "invalid_state",
        "enter_wait",
        "the run state rejected the wait and does not carry it",
        after,
      );
    }
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "enter_wait",
        "the wait state could not be confirmed durable",
        currentSnapshot(ctx.sink),
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "enter_wait",
        "the wait state could not be committed",
        currentSnapshot(ctx.sink),
      );
    }
    throw cause;
  }
  const after = currentSnapshot(ctx.sink);
  const record = after !== null ? lastWaitRecord(after) : undefined;
  if (
    after === null ||
    record === undefined ||
    record.index !== waitIndex ||
    record.request_sha256 !== prepared.sha256
  ) {
    throw controllerError(
      "invalid_state",
      "enter_wait",
      "the committed run state does not carry the new wait",
      after,
    );
  }
  return deepFreeze({ wait_index: waitIndex, request_sha256: prepared.sha256, state: after });
}

/**
 * The idempotent re-entry path for a durably open wait: the caller's
 * `reason`/`actions` must match the open record exactly, the filesystem
 * publication is verified or restored, the digest must match the durable
 * record, and no second `run_waiting` is dispatched.
 */
async function reenterOpenWait(
  ctx: CapturedSink,
  options: EnterPipelineV2WaitOptions,
  openWait: PipelineV2WaitRecord,
): Promise<EnteredPipelineV2Wait> {
  const state = currentSnapshot(ctx.sink);
  if (state === null) {
    throw controllerError(
      "invalid_state",
      "enter_wait",
      "no durable pipeline v2 run state exists anymore",
      null,
    );
  }
  const callerValue = {
    schema_version: 1,
    run_id: state.run_id,
    wait_index: openWait.index,
    transition_count: openWait.transition_count,
    state_id: openWait.state_id,
    reason: options.reason,
    actions: options.actions,
  };
  let callerPrepared: PreparedPipelineV2WaitRequest;
  try {
    callerPrepared = preparePipelineV2WaitRequest(callerValue);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitManifestError) {
      throw controllerError(
        "invalid_request",
        "enter_wait",
        "the wait request is not a valid manifest",
        state,
      );
    }
    throw cause;
  }
  let recordPrepared: PreparedPipelineV2WaitRequest;
  try {
    recordPrepared = preparePipelineV2WaitRequest(waitManifestValue(state, openWait));
  } catch (cause) {
    if (cause instanceof PipelineV2WaitManifestError) {
      throw controllerError(
        "invalid_state",
        "enter_wait",
        "the durable wait record does not carry a valid request manifest",
        state,
      );
    }
    throw cause;
  }
  if (!manifestsEqual(callerPrepared, recordPrepared)) {
    throw controllerError(
      "wait_conflict",
      "enter_wait",
      "the open wait does not match the requested wait",
      state,
    );
  }
  await verifyDurableRequestPublication(ctx.runRoot, state, openWait, "enter_wait", state);
  return deepFreeze({
    wait_index: openWait.index,
    request_sha256: openWait.request_sha256,
    state,
  });
}

export async function recordPipelineV2WaitResponse(
  options: RecordPipelineV2WaitResponseOptions,
): Promise<RecordedPipelineV2WaitResponse> {
  const ctx = captureBoundary(options, "record_response");
  const runOptions = options as RecordPipelineV2WaitResponseOptions;
  if (!isPositiveSafeInteger(runOptions.waitIndex)) {
    throw controllerError(
      "invalid_response",
      "record_response",
      "the wait index must be a positive safe integer",
      ctx.snapshot,
    );
  }
  if (typeof runOptions.raw !== "string") {
    throw controllerError(
      "invalid_response",
      "record_response",
      "the wait response must be a raw JSON document string",
      ctx.snapshot,
    );
  }
  const state = ctx.snapshot;
  const record = state.waits.find((candidate) => candidate.index === runOptions.waitIndex);
  if (record === undefined) {
    throw controllerError(
      "invalid_response",
      "record_response",
      "the run records no wait with the requested index",
      state,
    );
  }
  const last = lastWaitRecord(state);
  if (record !== last && record.response === undefined) {
    throw controllerError(
      "invalid_state",
      "record_response",
      "an open wait record must be the last wait of the journal",
      state,
    );
  }
  // The request manifest is reconstructed exclusively from the durable
  // wait record; its canonical file is verified or restored first.
  await verifyDurableRequestPublication(
    ctx.runRoot,
    state,
    record,
    "record_response",
    currentSnapshot(ctx.sink),
  );
  let accepted: AcceptedPipelineV2WaitResponse;
  try {
    accepted = acceptPipelineV2WaitResponse(
      preparePipelineV2WaitRequest(waitManifestValue(state, record)),
      runOptions.raw,
    );
  } catch (cause) {
    if (cause instanceof PipelineV2WaitManifestError) {
      throw controllerError(
        "invalid_response",
        "record_response",
        "the wait response is not a valid response for the stored request",
        currentSnapshot(ctx.sink),
      );
    }
    throw cause;
  }
  if (record.response !== undefined) {
    return await retryDurableResponse(ctx, runOptions, record, accepted);
  }
  return await recordFreshResponse(ctx, runOptions, record, accepted);
}

/**
 * The already-answered path: only the identical response (same action id
 * and response digest) is accepted; both filesystem manifests are verified
 * or restored and nothing is dispatched.
 */
async function retryDurableResponse(
  ctx: CapturedSink,
  options: RecordPipelineV2WaitResponseOptions,
  record: PipelineV2WaitRecord,
  accepted: AcceptedPipelineV2WaitResponse,
): Promise<RecordedPipelineV2WaitResponse> {
  const state = currentSnapshot(ctx.sink);
  if (
    state === null ||
    record.response === undefined ||
    accepted.manifest.action_id !== record.response.action_id ||
    accepted.sha256 !== record.response.response_sha256
  ) {
    throw controllerError(
      "wait_conflict",
      "record_response",
      "the wait already carries a different response",
      state,
    );
  }
  let published: Awaited<ReturnType<typeof publishPipelineV2WaitResponse>>;
  try {
    published = await publishPipelineV2WaitResponse(ctx.runRoot, options.waitIndex, options.raw);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw waitStoreFailure(
        cause,
        "record_response",
        "the wait response could not be published",
        state,
      );
    }
    throw cause;
  }
  if (
    published.response.sha256 !== record.response.response_sha256 ||
    published.request.sha256 !== record.request_sha256
  ) {
    throw new Error(
      "pipeline v2 wait controller invariant violated: the republished manifests do not match the durable wait record",
    );
  }
  return deepFreeze({
    wait_index: record.index,
    request_sha256: record.request_sha256,
    response_sha256: accepted.sha256,
    action_id: accepted.manifest.action_id,
    action_to: accepted.action_to,
    state,
  });
}

/**
 * The fresh-response path: publish the response manifest, dispatch
 * `wait_response_recorded` with the accepted digests and action id, and
 * verify the authoritative post-dispatch snapshot.
 */
async function recordFreshResponse(
  ctx: CapturedSink,
  options: RecordPipelineV2WaitResponseOptions,
  record: PipelineV2WaitRecord,
  accepted: AcceptedPipelineV2WaitResponse,
): Promise<RecordedPipelineV2WaitResponse> {
  let published: Awaited<ReturnType<typeof publishPipelineV2WaitResponse>>;
  try {
    published = await publishPipelineV2WaitResponse(ctx.runRoot, options.waitIndex, options.raw);
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw waitStoreFailure(
        cause,
        "record_response",
        "the wait response could not be published",
        currentSnapshot(ctx.sink),
      );
    }
    throw cause;
  }
  if (
    published.response.sha256 !== accepted.sha256 ||
    published.response.action_to !== accepted.action_to
  ) {
    throw new Error(
      "pipeline v2 wait controller invariant violated: the published response does not match the accepted response",
    );
  }
  const command: PipelineV2RunCommand = {
    kind: "wait_response_recorded",
    waitIndex: options.waitIndex,
    expectedRequestSha256: record.request_sha256,
    actionId: accepted.manifest.action_id,
    responseSha256: accepted.sha256,
  };
  try {
    await ctx.dispatch(command);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      const after = currentSnapshot(ctx.sink);
      if (after !== null && responseRecordMatches(after, options.waitIndex, accepted)) {
        return deepFreeze({
          wait_index: options.waitIndex,
          request_sha256: record.request_sha256,
          response_sha256: accepted.sha256,
          action_id: accepted.manifest.action_id,
          action_to: accepted.action_to,
          state: after,
        });
      }
      throw controllerError(
        "invalid_state",
        "record_response",
        "the run state rejected the response and does not carry it",
        after,
      );
    }
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      throw controllerError(
        "state_persist_failed",
        "record_response",
        "the response state could not be confirmed durable",
        currentSnapshot(ctx.sink),
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      throw controllerError(
        "state_persist_failed",
        "record_response",
        "the response state could not be committed",
        currentSnapshot(ctx.sink),
      );
    }
    throw cause;
  }
  const after = currentSnapshot(ctx.sink);
  if (after === null || !responseRecordMatches(after, options.waitIndex, accepted)) {
    throw controllerError(
      "invalid_state",
      "record_response",
      "the committed run state does not carry the response",
      after,
    );
  }
  return deepFreeze({
    wait_index: options.waitIndex,
    request_sha256: record.request_sha256,
    response_sha256: accepted.sha256,
    action_id: accepted.manifest.action_id,
    action_to: accepted.action_to,
    state: after,
  });
}

function responseRecordMatches(
  state: PipelineV2RunState,
  waitIndex: number,
  accepted: AcceptedPipelineV2WaitResponse,
): boolean {
  const record = state.waits.find((candidate) => candidate.index === waitIndex);
  return (
    record !== undefined &&
    record.response !== undefined &&
    record.response.action_id === accepted.manifest.action_id &&
    record.response.response_sha256 === accepted.sha256
  );
}
