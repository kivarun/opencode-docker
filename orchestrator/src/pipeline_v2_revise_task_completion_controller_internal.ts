import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  applyPipelineV2ReviseTaskClosure,
  type AppliedPipelineV2ReviseTaskClosure,
} from "./pipeline_v2_revise_task_closure_controller.ts";
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
  PipelineV2ReviseTaskIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral completion controller for the revise-task
 * intervention (production-reachable transitively through the
 * revise-task intervention controller).
 *
 * The controller completes an already durably accepted `revise_task`
 * intervention by composing the existing authoritative steps in the fixed
 * order — `applyPipelineV2ReviseTaskClosure` (the wait-bound `replanned`
 * iteration closure, with its own provenance gate, bindings,
 * reconciliation and durability mapping), then the existing generic wait
 * controller's structured `revise_task` action path
 * (`recordPipelineV2WaitAction`, which synthesizes, publishes and durably
 * records the user response through the existing wait-manifest
 * substrate). No new response manifest, serializer, digest builder, store
 * protocol or response dispatcher is introduced; the response document,
 * its publication and the durable `wait_response_recorded` command remain
 * entirely owned by the existing wait-manifest substrate. The controller
 * itself performs no filesystem work, never calls the reducer or any
 * store, never calls the intent acceptance controller, never loads a task
 * or plan manifest, never creates a plan revision, never closes the
 * generation, never opens the next generation or iteration, never runs
 * the architect/replanning execution and never resumes the run.
 *
 * Composition ordering (always): capture the options → apply or confirm
 * the closure → verify the closure result against the accepted intent →
 * record the wait action with the fixed `revise_task` action id → verify
 * the response result against the verified pre-response state → return
 * the unified result carrying the response controller's authoritative
 * state. The response is never published or dispatched before the closure
 * result is fully verified.
 *
 * Capture and provenance: the options shape, then `runRoot` → `sink` →
 * `intent` read exactly once, and the per-call ops getters read exactly
 * once, all before the first await; caller mutation after the capture
 * cannot influence the execution. The completion reads no intent or
 * snapshot fields itself before the closure call: the provenance
 * authority stays inside `applyPipelineV2ReviseTaskClosure` (its gate
 * runs before any field read), so the intent's manifest fields are read
 * only after the closure result has been produced. There is no second
 * state validator and no duplicated closure binding logic. The completion
 * reads the sink's authoritative `snapshot` directly exactly once, right
 * after the closure result was produced and before any response work (the
 * durable reference for the accepted task binding, the exact identity
 * bindings and the iteration history); that read sits outside the
 * verification try/catch, so an unexpected getter error keeps its class
 * and identity, while structural verification failures become the
 * controller's own `invalid_result`; after that single direct read the
 * snapshot is never read again by this controller (reads performed inside
 * the composed controllers belong to those controllers). When a racing
 * completion has already moved the durable run past the closure result's
 * boundary, the durable comparison does not apply and the response
 * verification remains the backstop; after the classification the durable
 * snapshot is never read again.
 *
 * Closure result verification (the contract-owned values, complete and
 * strictly before any response filesystem work or dispatch): the result
 * is an object; the wait, generation and iteration indexes and the task
 * revision are positive safe integers; the wait index, task id, task
 * digest and intent digest match the prepared revise intent; the target
 * wait is the last and only record of its index carrying the exact
 * accepted intent and the declared `revise_task` action; the state is
 * exactly one of the two immediate-boundary forms — the waiting/open wait
 * (C0–C3) with the cursor at the wait's state, or the exact
 * active/answered retry (C4) with the response carrying exactly the
 * `revise_task` action id and the cursor at the declared action target;
 * in both forms the cursor transition count, the transition journal and
 * the execution journal sit exactly at the wait boundary; the last
 * durable plan record exists; the generation is the last open one with
 * its exact index, its plan digest bound to the last durable plan record
 * and its identity bindings present; the target iteration is the last
 * one with no `open_iteration` projection and the exact `replanned`
 * closure of the target wait; the wait-bound task records of the target
 * wait — of ANY task — are exactly one, matching the result and the
 * intent on every contract field (`task_id`, `revision`, `sha256`,
 * `previous_sha256`, `wait_index`, `intent_sha256`) with no later
 * revision of the same task. Every field access is defensive; a hostile
 * or structurally inconsistent injected result (a missing or different
 * closure, a mutated generation index, a changed plan or stage binding, a
 * changed historical iteration prefix, a replaced wait intent, an extra
 * wait-bound task record of another task, a mismatching task revision
 * field, and malformed nested task/generation/iteration/wait/cursor
 * shapes) is the controller's own typed `invalid_result` — never a leaked
 * `TypeError` — with zero response publication and zero dispatch.
 *
 * Response result verification (against the verified pre-response state
 * of the closure result, never against the result's own final wait): the
 * run identity is unchanged; the wait journal keeps its length, its
 * target position and every record's bindings (index, transition count,
 * state id, reason, request digest, ordered `{id,to}` actions, exact
 * intent) with no earlier response binding changed; the only allowed
 * change on the target wait is the exact `revise_task` response
 * (`action_id` and the response digest equal the response result's); the
 * request digest and the routing target are taken from the pre-response
 * wait's declared action, never from the result's own final wait; the
 * final state is active and running with the cursor at the declared
 * action target and with the cursor transition count, the transition
 * journal and the execution journal exactly at the pre-response wait's
 * boundary; the state revision is exactly `before + 1` on the
 * waiting/open form and exactly unchanged on the active/answered form;
 * the C4 boundary keeps the pre-response response binding (the same
 * action id and response digest) unchanged; the task ledger, the plan
 * ledger, the target generation's exact index and identity bindings, the
 * iteration count, the iteration list length, the whole historical
 * iteration prefix (index, opening anchor and the exact closed
 * projection), the target iteration's exact `replanned` closure and
 * anchor, and the absent `open_iteration` projection must all be
 * unchanged. A structurally inconsistent injected response result is the
 * controller's own typed `invalid_result` — never a success and never a
 * leaked `TypeError`. The unified result is built from the single final
 * snapshot that passed the verification; after its one direct read of the
 * sink's authoritative `snapshot` (performed once after the closure
 * result and before any response work) this controller does not read the
 * snapshot again — reads performed inside the composed controllers belong
 * to those controllers.
 *
 * Retry windows: C0 (the accepted intent with the accepted task revision
 * and the iteration still open — the closure then the response, two
 * durable revisions), C1 (the exact durable closure — the response only),
 * C2 (the response file published but not durable after a
 * `not_committed` — the existing wait controller adopts the exact orphan
 * file and commits the response once), C3 (the response durable or
 * durability-unknown — the closure controller's C1/answered recognition
 * and the wait controller's durable-response recognition dispatch
 * nothing and verify or restore the publications), C4 (the response
 * durably recorded — zero dispatch through both recognitions). A closure
 * `not_committed` or `durability_unknown` stops the completion before
 * any response work — the response is never started into a poisoned
 * sink; a fresh retry with a reopened sink performs the remaining suffix
 * only. A response failure never rolls the closure back; a conflicting
 * retry rewrites nothing. The downstream controllers' typed errors and
 * unexpected errors keep their class and identity; errors are never
 * classified from message text.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ReviseTaskCompletionControllerError`,
 * `completePipelineV2ReviseTaskWithIo` and the one frozen
 * `productionReviseTaskCompletionOps`; the public module exports exactly
 * `PipelineV2ReviseTaskCompletionControllerError` and
 * `completePipelineV2ReviseTask`. The closed own reason set is
 * `invalid_options | invalid_result` with the last authoritative state
 * (`null` when none exists). Diagnostics are content-free (no digest
 * values, canonical JSON, paths, task bodies, response bodies, arbitrary
 * caller values, env values or credentials).
 *
 * Not implemented (stays unwired): the automatic choice of the wait
 * action, the automatic task/body selection, the automatic intervention
 * loop, the default-pipeline bundle, migrations/API/T3 and multi-process
 * locking.
 */

export type PipelineV2ReviseTaskCompletionControllerFailureReason = "invalid_options" | "invalid_result";

export class PipelineV2ReviseTaskCompletionControllerError extends Error {
  readonly reason: PipelineV2ReviseTaskCompletionControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReviseTaskCompletionControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReviseTaskCompletionControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam passed through to the existing controllers;
 * the production `PipelineV2RunStateSink` satisfies it without an
 * adapter. The completion itself reads the sink's authoritative
 * `snapshot` directly exactly once (after the closure result, before any
 * response work) and dispatches nothing; every other sink read and every
 * dispatch belongs to the composed controllers.
 */
export interface PipelineV2ReviseTaskCompletionControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface CompletePipelineV2ReviseTaskOptions {
  readonly runRoot: string;
  readonly sink: PipelineV2ReviseTaskCompletionControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
}

export interface CompletedPipelineV2ReviseTask {
  readonly wait_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_sha256: string;
  readonly intent_sha256: string;
  readonly request_sha256: string;
  readonly response_sha256: string;
  readonly action_id: "revise_task";
  readonly action_to: string;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call structural ops of the internal core: the existing closure
 * application and the existing wait-action recording, bound by one frozen
 * production object. Tests inject their own per-call object; there is no
 * mutable module-global seam, no installer and no public export of the
 * seam.
 */
export interface PipelineV2ReviseTaskCompletionOps {
  readonly applyClosure: typeof applyPipelineV2ReviseTaskClosure;
  readonly recordWaitAction: typeof recordPipelineV2WaitAction;
}

export const productionReviseTaskCompletionOps: PipelineV2ReviseTaskCompletionOps = Object.freeze({
  applyClosure: applyPipelineV2ReviseTaskClosure,
  recordWaitAction: recordPipelineV2WaitAction,
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function completionError(
  reason: PipelineV2ReviseTaskCompletionControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReviseTaskCompletionControllerError {
  return new PipelineV2ReviseTaskCompletionControllerError(reason, message, state);
}

const REVISE_TASK_ACTION_ID = "revise_task";

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
    if (!isRecord(left) || !isRecord(right) || left["id"] !== right["id"] || left["to"] !== right["to"]) {
      return false;
    }
  }
  return true;
}

/**
 * The contract-owned binding equality of one wait record against its
 * position: identity fields, ordered `{id,to}` actions and the accepted
 * intent (positional equality; the accepted digest value is verified
 * separately where the contract requires it). The response binding is
 * compared only when a change is not allowed.
 */
function waitRecordBindingsUnchanged(
  before: PipelineV2WaitRecord | undefined,
  after: unknown,
  allowResponseChange: boolean,
): boolean {
  if (before === undefined || !isRecord(after)) {
    return false;
  }
  if (
    before.index !== after["index"] ||
    before.transition_count !== after["transition_count"] ||
    before.state_id !== after["state_id"] ||
    before.reason !== after["reason"] ||
    before.request_sha256 !== after["request_sha256"]
  ) {
    return false;
  }
  if (!orderedActionsEqual(before.actions, after["actions"])) {
    return false;
  }
  const beforeIntent = before.intent;
  const afterIntent = after["intent"];
  const intentEqual =
    beforeIntent === undefined
      ? afterIntent === undefined
      : isRecord(beforeIntent) &&
        isRecord(afterIntent) &&
        beforeIntent["intent_sha256"] === (afterIntent as Record<string, unknown>)["intent_sha256"];
  if (!intentEqual) {
    return false;
  }
  if (allowResponseChange) {
    return true;
  }
  const beforeResponse = before.response;
  const afterResponse = after["response"];
  return beforeResponse === undefined
    ? afterResponse === undefined
    : isRecord(beforeResponse) &&
        isRecord(afterResponse) &&
        beforeResponse["action_id"] === (afterResponse as Record<string, unknown>)["action_id"] &&
        beforeResponse["response_sha256"] === (afterResponse as Record<string, unknown>)["response_sha256"];
}

/**
 * The task ledger is fully unchanged: the same length and every record's
 * contract fields (index, task id, revision, digest, chain predecessor,
 * wait and intent links) at its position.
 */
function taskLedgerUnchanged(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.task_revisions) || after.task_revisions.length !== before.task_revisions.length) {
    return false;
  }
  return before.task_revisions.every((beforeEntry, position) => {
    const afterEntry = after.task_revisions[position];
    return (
      afterEntry !== undefined &&
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["task_id"] === beforeEntry.task_id &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["wait_index"] === beforeEntry.wait_index &&
      afterEntry["intent_sha256"] === beforeEntry.intent_sha256
    );
  });
}

/**
 * The plan ledger boundary is unchanged: the same length and every
 * record's chain fields (index, revision, digest, predecessor, origin
 * execution) at its position.
 */
function planLedgerUnchanged(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.plan_revisions) || after.plan_revisions.length !== before.plan_revisions.length) {
    return false;
  }
  return before.plan_revisions.every((beforeEntry, position) => {
    const afterEntry = after.plan_revisions[position];
    return (
      afterEntry !== undefined &&
      isRecord(afterEntry) &&
      afterEntry["index"] === beforeEntry.index &&
      afterEntry["revision"] === beforeEntry.revision &&
      afterEntry["sha256"] === beforeEntry.sha256 &&
      afterEntry["previous_sha256"] === beforeEntry.previous_sha256 &&
      afterEntry["origin_execution"] === beforeEntry.origin_execution
    );
  });
}

/**
 * The target generation stays the last open one with unchanged identity
 * bindings, the same iteration count and iteration list length, and the
 * same target iteration with its opening anchor. No general deep
 * comparator is used — only these contract-owned fields.
 */
function generationBindingsUnchanged(
  before: PipelineV2RunState,
  after: PipelineV2RunState,
  generationIndex: number,
  iterationIndex: number,
): boolean {
  if (!Array.isArray(after.generations) || after.generations.length !== generationIndex) {
    return false;
  }
  const beforeGeneration = before.generations[generationIndex - 1];
  const afterGeneration = after.generations[generationIndex - 1];
  if (beforeGeneration === undefined || afterGeneration === undefined || !isRecord(afterGeneration)) {
    return false;
  }
  if (
    afterGeneration["index"] !== generationIndex ||
    afterGeneration["closed"] !== undefined ||
    afterGeneration["open_iteration"] !== undefined
  ) {
    return false;
  }
  if (
    beforeGeneration.stage_id !== afterGeneration["stage_id"] ||
    beforeGeneration.stage_position !== afterGeneration["stage_position"] ||
    beforeGeneration.template_id !== afterGeneration["template_id"] ||
    beforeGeneration.plan_sha256 !== afterGeneration["plan_sha256"] ||
    beforeGeneration.initial_budget !== afterGeneration["initial_budget"] ||
    beforeGeneration.opened_transition_count !== afterGeneration["opened_transition_count"] ||
    beforeGeneration.iteration_count !== afterGeneration["iteration_count"] ||
    !Array.isArray(afterGeneration["iterations"]) ||
    beforeGeneration.iterations.length !== (afterGeneration["iterations"] as unknown[]).length
  ) {
    return false;
  }
  const afterIterations = afterGeneration["iterations"] as unknown[];
  const beforeIteration = beforeGeneration.iterations[beforeGeneration.iterations.length - 1];
  const afterIteration = afterIterations[afterIterations.length - 1];
  return (
    beforeIteration !== undefined &&
    isRecord(afterIteration) &&
    afterIteration["index"] === iterationIndex &&
    beforeIteration.index === iterationIndex &&
    beforeIteration.opened_transition_count === afterIteration["opened_transition_count"]
  );
}

/**
 * The whole historical iteration prefix is unchanged: every iteration
 * before the last keeps its index, its opening anchor and its exact
 * closed projection (absence/presence, `by`, wait index and boundary).
 */
function historicalIterationsUnchanged(
  beforeGeneration: PipelineV2RunState["generations"][number],
  afterGeneration: Record<string, unknown>,
): boolean {
  const afterIterations = afterGeneration["iterations"];
  if (!Array.isArray(afterIterations)) {
    return false;
  }
  const lastPosition = beforeGeneration.iterations.length - 1;
  for (let position = 0; position < lastPosition; position += 1) {
    const beforeIteration = beforeGeneration.iterations[position];
    const entry = afterIterations[position];
    if (beforeIteration === undefined || entry === undefined || !isRecord(entry)) {
      return false;
    }
    const beforeClosed = beforeIteration.closed;
    const closed = entry["closed"];
    const closedEqual =
      beforeClosed === undefined
        ? closed === undefined
        : isRecord(closed) &&
          closed["by"] === beforeClosed.by &&
          closed["wait_index"] === beforeClosed.wait_index &&
          closed["closed_transition_count"] === beforeClosed.closed_transition_count;
    if (
      entry["index"] !== beforeIteration.index ||
      entry["opened_transition_count"] !== beforeIteration.opened_transition_count ||
      !closedEqual
    ) {
      return false;
    }
  }
  return true;
}

/**
 * The target iteration keeps the exact `replanned` closure and anchor of
 * the target wait: the last iteration with its recorded index and the
 * exact closure fields.
 */
function targetIterationClosureExact(
  afterGeneration: Record<string, unknown>,
  wait: PipelineV2WaitRecord,
  iterationIndex: number,
): boolean {
  const iterations = afterGeneration["iterations"];
  if (!Array.isArray(iterations) || iterations.length === 0) {
    return false;
  }
  const last = iterations[iterations.length - 1];
  if (last === undefined || !isRecord(last)) {
    return false;
  }
  const closed = last["closed"];
  return (
    last["index"] === iterationIndex &&
    closed !== undefined &&
    isRecord(closed) &&
    closed["by"] === "replanned" &&
    closed["wait_index"] === wait.index &&
    closed["closed_transition_count"] === wait.transition_count
  );
}

/**
 * The exact immediate wait boundary: the cursor sits exactly at the
 * expected state with the transition journal, the transition count and
 * the execution journal all exactly at the wait's boundary. The expected
 * cursor state is `wait.state_id` for the waiting/open form and the
 * declared `revise_task` action's target for the active/answered C4
 * form.
 */
function verifyImmediateWaitBoundary(
  state: PipelineV2RunState,
  wait: PipelineV2WaitRecord,
  expectedCursorState: string,
): void {
  const cursor = state.cursor;
  if (!isRecord(cursor) || cursor["current_state"] !== expectedCursorState) {
    throw completionError(
      "invalid_result",
      "the applied closure result state cursor is not at the expected boundary state",
      state,
    );
  }
  if (cursor["transition_count"] !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied closure result state cursor transition count does not match the wait boundary",
      state,
    );
  }
  if (!Array.isArray(state.transitions) || state.transitions.length !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied closure result state transition journal does not match the wait boundary",
      state,
    );
  }
  if (!Array.isArray(state.executions) || state.executions.length !== wait.transition_count) {
    throw completionError(
      "invalid_result",
      "the applied closure result state execution journal does not match the wait boundary",
      state,
    );
  }
}

/**
 * The full pre-response verification of the closure result, against the
 * contract-owned values only (see the module docstring for the complete
 * list). Every field access is defensive; a structurally inconsistent
 * injected result becomes the controller's own typed `invalid_result`,
 * never a leaked `TypeError`.
 */
function verifyClosureResultBeforeResponse(
  closure: AppliedPipelineV2ReviseTaskClosure,
  manifest: PipelineV2ReviseTaskIntentManifest,
  intentSha256: string,
): void {  const state = closure.state;
  if (!isRecord(state)) {
    throw completionError("invalid_result", "the applied closure result state is not an object", null);
  }
  if (
    closure.wait_index !== manifest.wait_index ||
    closure.task_id !== manifest.task_id ||
    closure.task_sha256 !== manifest.new_task_revision_sha256 ||
    closure.intent_sha256 !== intentSha256 ||
    !isPositiveSafeInteger(closure.wait_index) ||
    !isPositiveSafeInteger(closure.generation_index) ||
    !isPositiveSafeInteger(closure.iteration_index) ||
    !isPositiveSafeInteger(closure.task_revision)
  ) {
    throw completionError(
      "invalid_result",
      "the applied closure result does not match the accepted revise_task intent",
      state,
    );
  }
  if (!Array.isArray(state.waits) || state.waits.length !== closure.wait_index) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the target wait as the last wait record",
      state,
    );
  }
  const wait = state.waits[closure.wait_index - 1];
  if (!isRecord(wait) || wait["index"] !== closure.wait_index) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the target wait",
      state,
    );
  }
  // The target wait must be the last and ONLY record of its index: a
  // defensive pass over the whole wait journal. Every viewed entry is a
  // record; the target index must occur exactly once; with the pinned
  // journal length above, exactly one occurrence also pins the target to
  // the last position (an early record carrying the target index — e.g.
  // a mutated reducer-produced multi-wait journal — is rejected).
  let targetOccurrences = 0;
  for (let position = 0; position < state.waits.length; position += 1) {
    const entry = state.waits[position];
    if (!isRecord(entry)) {
      throw completionError(
        "invalid_result",
        "the applied closure result state wait journal carries a malformed record",
        state,
      );
    }
    if (entry["index"] === closure.wait_index) {
      targetOccurrences += 1;
    }
  }
  if (targetOccurrences !== 1) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the target wait as the last and only record of its index",
      state,
    );
  }
  const waitIntent = wait["intent"];
  if (waitIntent === undefined || !isRecord(waitIntent) || waitIntent["intent_sha256"] !== intentSha256) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the exact accepted intent on the target wait",
      state,
    );
  }
  const declaredAction = (Array.isArray(wait["actions"]) ? (wait["actions"] as unknown[]) : []).find(
    (action) => isRecord(action) && action["id"] === REVISE_TASK_ACTION_ID,
  );
  if (!isRecord(declaredAction) || typeof declaredAction["to"] !== "string") {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not declare the revise_task action on the target wait",
      state,
    );
  }
  const waitRecord = wait as unknown as PipelineV2WaitRecord;
  // The acceptable boundary form: the waiting/open wait (C0–C3) or the
  // exact active/answered C4 boundary — in both forms the cursor and the
  // transition/execution journals sit exactly at the wait boundary.
  if (state.status === "waiting" && state.phase === "waiting") {
    if (wait["response"] !== undefined) {
      throw completionError(
        "invalid_result",
        "the applied closure result state claims a waiting run with a recorded response",
        state,
      );
    }
    verifyImmediateWaitBoundary(state, waitRecord, waitRecord.state_id);
  } else if (state.status === "active" && state.phase === "running") {
    const response = wait["response"];
    if (!isRecord(response) || response["action_id"] !== REVISE_TASK_ACTION_ID) {
      throw completionError(
        "invalid_result",
        "the applied closure result state claims an active run without the exact revise_task response",
        state,
      );
    }
    verifyImmediateWaitBoundary(state, waitRecord, declaredAction["to"] as string);
  } else {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry an acceptable closure boundary",
      state,
    );
  }
  if (!Array.isArray(state.plan_revisions) || state.plan_revisions.length === 0) {
    throw completionError(
      "invalid_result",
      "the applied closure result state carries no durable plan revision",
      state,
    );
  }
  const lastPlanRecord = state.plan_revisions[state.plan_revisions.length - 1];
  if (!isRecord(lastPlanRecord)) {
    throw completionError(
      "invalid_result",
      "the applied closure result state carries no durable plan revision",
      state,
    );
  }
  if (!Array.isArray(state.generations) || state.generations.length !== closure.generation_index) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the last open target generation",
      state,
    );
  }
  const generation = state.generations[closure.generation_index - 1];
  if (!isRecord(generation) || generation["index"] !== closure.generation_index || generation["closed"] !== undefined) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the last open target generation",
      state,
    );
  }
  if (generation["plan_sha256"] !== lastPlanRecord["sha256"]) {
    throw completionError(
      "invalid_result",
      "the applied closure result state generation does not belong to the last durable plan revision",
      state,
    );
  }
  const iterations = generation["iterations"];
  if (!Array.isArray(iterations) || iterations.length === 0) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not record iterations on the target generation",
      state,
    );
  }
  const iteration = iterations[iterations.length - 1];
  if (!isRecord(iteration) || iteration["index"] !== closure.iteration_index) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the target iteration",
      state,
    );
  }
  if (generation["open_iteration"] !== undefined) {
    throw completionError(
      "invalid_result",
      "the applied closure result state still projects an open iteration",
      state,
    );
  }
  const closed = iteration["closed"];
  if (
    !isRecord(closed) ||
    closed["by"] !== "replanned" ||
    closed["wait_index"] !== closure.wait_index ||
    closed["closed_transition_count"] !== wait.transition_count
  ) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry the exact replanned closure",
      state,
    );
  }
  if (!Array.isArray(state.task_revisions)) {
    throw completionError(
      "invalid_result",
      "the applied closure result state carries no task ledger",
      state,
    );
  }
  const waitBound = (state.task_revisions as unknown[]).filter(
    (entry) => isRecord(entry) && entry["wait_index"] === closure.wait_index,
  );
  if (waitBound.length !== 1 || !isRecord(waitBound[0])) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not carry exactly one accepted task revision for the wait",
      state,
    );
  }
  const record = waitBound[0] as Record<string, unknown>;
  if (
    record["task_id"] !== manifest.task_id ||
    record["sha256"] !== manifest.new_task_revision_sha256 ||
    record["previous_sha256"] !== manifest.expected_previous_task_sha256 ||
    record["wait_index"] !== closure.wait_index ||
    record["intent_sha256"] !== intentSha256 ||
    record["revision"] !== closure.task_revision
  ) {
    throw completionError(
      "invalid_result",
      "the accepted task revision of the target wait contradicts the revise intent",
      state,
    );
  }
  const movedFurther = (state.task_revisions as unknown[]).some(
    (entry) =>
      isRecord(entry) &&
      entry["task_id"] === manifest.task_id &&
      typeof entry["revision"] === "number" &&
      entry["revision"] > closure.task_revision,
  );
  if (movedFurther) {
    throw completionError(
      "invalid_result",
      "the task ledger already moved past the accepted revision",
      state,
    );
  }
}

/**
 * The closure result state must be the durable state on every
 * contract-owned field the response verification will use as its
 * pre-response reference. The durable snapshot is read exactly once from
 * the captured sink. When the durable run has already moved past the
 * closure result's boundary — a racing completion committed the response
 * before this verification read the snapshot — the comparison does not
 * apply: the closure result was captured at its own boundary and the
 * response verification remains the backstop for it. The comparison uses
 * the same targeted helpers as the response verification; there is no
 * second state validator and no general deep comparator.
 */
function verifyClosureResultAgainstDurable(
  closure: AppliedPipelineV2ReviseTaskClosure,
  durable: unknown,
  intentSha256: string,
): void {
  const resultState = closure.state;
  if (!isRecord(durable)) {
    throw completionError(
      "invalid_result",
      "the run state sink carries no authoritative durable snapshot for the closure result",
      resultState,
    );
  }
  const durableState = durable as unknown as PipelineV2RunState;
  if (
    durableState.revision !== resultState.revision ||
    durableState.run_id !== resultState.run_id ||
    durableState.status !== resultState.status ||
    durableState.phase !== resultState.phase
  ) {
    // A racing completion already moved the durable run forward; the
    // response verification remains the backstop for this closure result.
    return;
  }
  if (!Array.isArray(durableState.waits) || durableState.waits.length !== resultState.waits.length) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable wait journal",
      resultState,
    );
  }
  for (let position = 0; position < durableState.waits.length; position += 1) {
    if (!waitRecordBindingsUnchanged(durableState.waits[position], resultState.waits[position], false)) {
      throw completionError(
        "invalid_result",
        "the applied closure result state does not match the durable wait journal",
        resultState,
      );
    }
  }
  if (!taskLedgerUnchanged(durableState, resultState) || !planLedgerUnchanged(durableState, resultState)) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable ledgers",
      resultState,
    );
  }
  if (!generationBindingsUnchanged(durableState, resultState, closure.generation_index, closure.iteration_index)) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable generation bindings",
      resultState,
    );
  }
  const durableGeneration = durableState.generations[closure.generation_index - 1];
  const resultGeneration = resultState.generations[closure.generation_index - 1];
  if (
    durableGeneration === undefined ||
    resultGeneration === undefined ||
    !isRecord(resultGeneration) ||
    !historicalIterationsUnchanged(durableGeneration, resultGeneration)
  ) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable iteration history",
      resultState,
    );
  }
  const durableWait = durableState.waits[closure.wait_index - 1] as PipelineV2WaitRecord;
  if (!targetIterationClosureExact(resultGeneration, durableWait, closure.iteration_index)) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable closure",
      resultState,
    );
  }
  if (
    !isRecord(resultState.cursor) ||
    !isRecord(durableState.cursor) ||
    resultState.cursor["current_state"] !== durableState.cursor["current_state"] ||
    resultState.cursor["transition_count"] !== durableState.cursor["transition_count"] ||
    !Array.isArray(resultState.transitions) ||
    !Array.isArray(durableState.transitions) ||
    resultState.transitions.length !== durableState.transitions.length ||
    !Array.isArray(resultState.executions) ||
    !Array.isArray(durableState.executions) ||
    resultState.executions.length !== durableState.executions.length
  ) {
    throw completionError(
      "invalid_result",
      "the applied closure result state does not match the durable boundary",
      resultState,
    );
  }
}

/**
 * The response result verification against the verified pre-response
 * state: the wait bindings must be unchanged, the only allowed change is
 * the exact `revise_task` response, the request digest and the routing
 * target come from the pre-response wait, the final state pins the exact
 * post-response boundary (including the revision delta by form), and the
 * durable closure, ledgers and generation/iteration bindings of the
 * pre-response state must be unchanged in the final state. No response
 * digest is rebuilt by this controller.
 */
function verifyResponseResult(
  response: RecordedPipelineV2WaitResponse,
  closure: AppliedPipelineV2ReviseTaskClosure,
  beforeState: PipelineV2RunState,
  beforeWait: PipelineV2WaitRecord,
): void {
  if (!isRecord(response)) {
    throw completionError("invalid_result", "the recorded wait response result is not an object", null);
  }
  const finalState = response.state;
  if (!isRecord(finalState)) {
    throw completionError("invalid_result", "the recorded wait response result is not an object", null);
  }
  if (response.wait_index !== beforeWait.index || response.action_id !== REVISE_TASK_ACTION_ID) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not match the requested action",
      finalState,
    );
  }
  if (finalState.run_id !== beforeState.run_id) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the run identity",
      finalState,
    );
  }
  if (!Array.isArray(finalState.waits) || finalState.waits.length !== beforeState.waits.length) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the wait journal position",
      finalState,
    );
  }
  const finalWait = finalState.waits[beforeWait.index - 1];
  if (!isRecord(finalWait) || finalWait["index"] !== beforeWait.index) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not carry the target wait",
      finalState,
    );
  }
  for (let position = 0; position < beforeState.waits.length; position += 1) {
    const beforeEntry = beforeState.waits[position];
    const changed = waitRecordBindingsUnchanged(
      beforeEntry,
      finalState.waits[position],
      position === beforeWait.index - 1,
    );
    if (!changed) {
      throw completionError(
        "invalid_result",
        "the recorded wait response result changed an earlier wait record binding",
        finalState,
      );
    }
  }
  // The only allowed change on the target wait: the exact revise_task
  // response.
  const finalResponse = finalWait["response"];
  if (
    !isRecord(finalResponse) ||
    finalResponse["action_id"] !== REVISE_TASK_ACTION_ID ||
    finalResponse["response_sha256"] !== response.response_sha256
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not match the durable response on the target wait",
      finalState,
    );
  }
  // The request digest and the routing target come from the pre-response
  // wait, never from the result's own final wait.
  if (response.request_sha256 !== beforeWait.request_sha256) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result request digest does not match the pre-response wait",
      finalState,
    );
  }
  const beforeDeclared = beforeWait.actions.find((action) => action.id === REVISE_TASK_ACTION_ID);
  if (beforeDeclared === undefined) {
    throw completionError(
      "invalid_result",
      "the pre-response wait record does not declare the revise_task action",
      finalState,
    );
  }
  if (!isRecord(finalState.cursor) || finalState.cursor["current_state"] !== beforeDeclared.to) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result does not route to the declared action target",
      finalState,
    );
  }
  if (response.action_to !== beforeDeclared.to) {
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
    !isRecord(finalState.cursor) ||
    finalState.cursor["transition_count"] !== beforeWait.transition_count ||
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
  // The state revision moves exactly once on the waiting/open form and
  // stays exactly unchanged on the active/answered form.
  const expectedRevision = beforeState.status === "waiting" ? beforeState.revision + 1 : beforeState.revision;
  if (finalState.revision !== expectedRevision) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result carries an unexpected state revision",
      finalState,
    );
  }
  // The C4 boundary: the pre-response wait already carries the response,
  // and the final state must keep exactly that response binding unchanged.
  if (
    beforeWait.response !== undefined &&
    (!isRecord(finalResponse) ||
      finalResponse["action_id"] !== beforeWait.response.action_id ||
      finalResponse["response_sha256"] !== beforeWait.response.response_sha256)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the pre-response response binding",
      finalState,
    );
  }
  // The task and plan ledgers and the durable closure/iteration bindings
  // of the pre-response state must be unchanged in the final state; the
  // final closure is checked against the original wait anchor.
  if (!taskLedgerUnchanged(beforeState, finalState) || !planLedgerUnchanged(beforeState, finalState)) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the durable task or plan ledger",
      finalState,
    );
  }
  if (
    !generationBindingsUnchanged(beforeState, finalState, closure.generation_index, closure.iteration_index)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the durable generation or iteration bindings",
      finalState,
    );
  }
  const beforeGeneration = beforeState.generations[closure.generation_index - 1];
  const finalGeneration = finalState.generations[closure.generation_index - 1];
  if (
    beforeGeneration === undefined ||
    finalGeneration === undefined ||
    !isRecord(finalGeneration) ||
    !historicalIterationsUnchanged(beforeGeneration, finalGeneration) ||
    !targetIterationClosureExact(finalGeneration, beforeWait, closure.iteration_index)
  ) {
    throw completionError(
      "invalid_result",
      "the recorded wait response result changed the durable closure or iteration history",
      finalState,
    );
  }
}

/**
 * Validate, compose and complete the revise-task intervention through the
 * existing controllers (see the module docstring for the full order and
 * durability semantics).
 */
export async function completePipelineV2ReviseTaskWithIo(
  ops: PipelineV2ReviseTaskCompletionOps,
  options: unknown,
): Promise<CompletedPipelineV2ReviseTask> {
  // Capture boundary: every options field is read exactly once
  // (`runRoot` → `sink` → `intent`), the per-call ops getters are read
  // exactly once, and all references are captured before the first
  // await; later caller mutations cannot influence the execution. The
  // intent's fields are not read here: the provenance authority stays
  // inside the closure controller.
  if (!isRecord(options)) {
    throw completionError("invalid_options", "completePipelineV2ReviseTask requires an options object", null);
  }
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const intent = options["intent"];
  if (typeof runRoot !== "string") {
    throw completionError("invalid_options", "completePipelineV2ReviseTask requires a runRoot string", null);
  }
  if (!isRecord(sink)) {
    throw completionError("invalid_options", "completePipelineV2ReviseTask requires a sink object", null);
  }
  if (!isRecord(intent)) {
    throw completionError("invalid_options", "completePipelineV2ReviseTask requires a prepared wait intent object", null);
  }
  const applyClosure = ops.applyClosure;
  const recordWaitAction = ops.recordWaitAction;
  if (typeof applyClosure !== "function" || typeof recordWaitAction !== "function") {
    throw completionError(
      "invalid_options",
      "the completion controller requires its closure and wait action operations",
      null,
    );
  }
  // Step 1: apply or confirm the closure through the existing controller;
  // its provenance gate, state validation, bindings, reconciliation,
  // dispatch verification and durability mapping are authoritative and
  // its typed errors keep their identity.
  const closure = await applyClosure({
    sink: sink as unknown as PipelineV2ReviseTaskCompletionControllerSink,
    intent: intent as unknown as PreparedPipelineV2RunWaitIntent,
  });
  if (!isRecord(closure)) {
    throw completionError("invalid_result", "the applied closure result is not an object", null);
  }
  // The intent's fields may be read only after the closure result was
  // produced: the production closure controller already ran the
  // provenance gate over this exact object.
  const preparedIntent = intent as unknown as PreparedPipelineV2RunWaitIntent;
  const manifest = preparedIntent.manifest as PipelineV2ReviseTaskIntentManifest;
  const intentSha256 = preparedIntent.sha256;
  if (
    closure.wait_index !== manifest.wait_index ||
    closure.intent_sha256 !== intentSha256 ||
    !isRecord(closure.state)
  ) {
    throw completionError(
      "invalid_result",
      "the applied closure result does not match the accepted revise_task intent",
      isRecord(closure.state) ? (closure.state as PipelineV2RunState) : null,
    );
  }
  // The full pre-response verification of the closure result state (see
  // the module docstring): every check must hold before any response
  // filesystem work or dispatch. The authoritative sink snapshot is read
  // DIRECTLY exactly once here, outside the verification try/catch, so an
  // unexpected getter error keeps its class and identity, while the
  // structural verification failures become the controller's own
  // `invalid_result`; the snapshot is never read again after the
  // classification. Field accesses are defensive; a structurally
  // inconsistent injected result becomes the controller's own
  // `invalid_result`, never a leaked `TypeError`.
  const durableSnapshot = (sink as Record<string, unknown>)["snapshot"];
  try {
    verifyClosureResultBeforeResponse(closure, manifest, intentSha256);
    verifyClosureResultAgainstDurable(closure, durableSnapshot, intentSha256);
  } catch (cause) {
    if (cause instanceof PipelineV2ReviseTaskCompletionControllerError) {
      throw cause;
    }
    throw completionError(
      "invalid_result",
      "the applied closure result state is structurally inconsistent",
      isRecord(closure.state) ? (closure.state as PipelineV2RunState) : null,
    );
  }
  const beforeState = closure.state;
  const beforeWait = beforeState.waits[closure.wait_index - 1] as PipelineV2WaitRecord;
  // Step 2: record the revise_task response through the existing generic
  // wait controller; the response publication and the durable
  // `wait_response_recorded` command remain entirely owned by the
  // existing wait-manifest substrate, and its typed errors keep their
  // identity.
  const response = await recordWaitAction({
    runRoot,
    sink: sink as unknown as PipelineV2ReviseTaskCompletionControllerSink,
    waitIndex: closure.wait_index,
    actionId: REVISE_TASK_ACTION_ID,
  });
  // Step 3: verify the response result against the verified pre-response
  // state — never against the hostile result's own final wait. Field
  // accesses are defensive; a structurally inconsistent injected result
  // becomes the controller's own `invalid_result`, never a success and
  // never a leaked `TypeError`.
  try {
    verifyResponseResult(response, closure, beforeState, beforeWait);
  } catch (cause) {
    if (cause instanceof PipelineV2ReviseTaskCompletionControllerError) {
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
  // Step 4: the unified content-free result with the response
  // controller's authoritative state.
  return deepFreezeValue({
    wait_index: closure.wait_index,
    generation_index: closure.generation_index,
    iteration_index: closure.iteration_index,
    task_id: closure.task_id,
    task_revision: closure.task_revision,
    task_sha256: closure.task_sha256,
    intent_sha256: intentSha256,
    request_sha256: response.request_sha256,
    response_sha256: response.response_sha256,
    action_id: REVISE_TASK_ACTION_ID,
    action_to: response.action_to,
    state: response.state,
  });
}
