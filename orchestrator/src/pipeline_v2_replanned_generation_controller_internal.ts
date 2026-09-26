import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
  type PipelineV2WaitRecord,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateStoreError,
  PipelineV2RunStateDurabilityError,
} from "./pipeline_v2_state_store.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import type { CompiledPipelineV2RunPlan } from "./pipeline_v2_run_plan_compiled.ts";
import {
  hasCompiledRunPlanProvenance,
  compiledRunPlanOriginIdentity,
} from "./pipeline_v2_run_plan_compiled_internal.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import type {
  PreparedPipelineV2RunWaitIntent,
  PipelineV2ReviseTaskIntentManifest,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Production-neutral replanned-generation controller (unwired).
 *
 * The controller is the durable bridge that closes the previous stage
 * generation after the NEXT plan revision has been accepted: the boundary
 * `completed revise_task response → settled planning execution → accepted
 * next plan revision → this controller closes the previous generation`,
 * from which a future increment opens the new plan's generation and
 * iteration. The single durable command is
 * `stage_generation_closed {generationIndex, by: "replanned"}`; the
 * reducer remains the only successor authority.
 *
 * The controller performs no filesystem work: it never runs the
 * architect, never builds or accepts a plan candidate, never publishes
 * any manifest, never opens a new generation or iteration, never selects
 * a stage or a budget, never commits a graph transition, and never
 * resumes the run. It is not wired into the coordinator, the runner, the
 * CLI or the default pipeline.
 *
 * Capture and provenance ordering (fail-closed, tested): the options
 * shape; then `sink` → `intent` → `compiledPlan` each read exactly once;
 * then the sink's `poisoned`, `dispatch` and initial `snapshot` members
 * each captured exactly once as opaque references with `dispatch` bound
 * to the sink before the first await; then the poison latch; then the
 * existing manifest-registry provenance gate of the prepared intent
 * (hand-built, cast, spread, deep-cloned and Proxy look-alikes are
 * rejected before any intent field is read, Proxy traps never invoked)
 * with the strict `revise_task_intent` kind; then the existing
 * compiled-plan provenance gate, equally before any field of the compiled
 * plan is read and with zero Proxy traps; only then the single
 * `validatePipelineV2RunState` (a missing or invalid initial snapshot is
 * the controller's own typed `invalid_state` with `state: null` and a
 * fixed content-free diagnostic; unexpected causes propagate unchanged),
 * the hidden compiled-plan pipeline identity comparison through the
 * single existing structural comparator, the durable bindings and the
 * reconciliation. No second parser, validator or reducer, no new
 * provenance registry, no digest builder and no general deep comparator
 * are introduced; the reducer runs only for the pre-check of the single
 * closing command. Unexpected getter, registry or dispatch errors keep
 * their class and identity unless they belong to the handled durable
 * failure contract (`PipelineV2RunStateStoreError` and its
 * durability-unknown subclass, and a racing `PipelineV2StateError`);
 * errors are never classified from message text.
 *
 * Exact prerequisite boundary: the immediate post-plan-acceptance
 * boundary of one completed revise_task cycle. The durable bindings
 * checked defensively before any reconciliation: the run is active and
 * running with no terminal, no published run outputs and no failure; the
 * target wait is the last wait-journal record and the ONLY record of its
 * index (a full defensive journal pass: every viewed entry is a record,
 * the target index occurs exactly once and sits at the last position);
 * the wait is answered with exactly the `revise_task` action id; the
 * ordered actions declare `revise_task` exactly and the cursor sits on
 * its declared target; the durable wait intent is the exact accepted
 * intent digest; the intent manifest's run id, wait index, task id,
 * predecessor digest and new-task digest are bound to the durable state;
 * the cursor transition count and the transition journal length sit
 * exactly at the wait boundary. The accepted task revision is read
 * exclusively from the ledger: the wait-bound records of the target wait
 * — of ANY task — must be exactly one, matching the intent on
 * `task_id`, `sha256` (the new task digest), `previous_sha256` (the
 * predecessor digest), `wait_index` and `intent_sha256`, with a positive
 * safe revision above 1 and no later revision of the same task; absent →
 * `invalid_state`; several records → `revision_conflict`. The old
 * generation must be the LAST durable generation with an exact positive
 * index, bound to the previous plan digest, without an `open_iteration`,
 * with at least one iteration, and with the LAST iteration as the exact
 * target (`closed.by === "replanned"`, `closed.wait_index === wait.index`,
 * `closed.closed_transition_count === wait.transition_count`); the whole
 * historical iteration prefix must be well-shaped; a replaced or
 * non-last generation, a later iteration or a contradicting lifecycle is
 * a `lifecycle_conflict`. The new accepted plan must be exactly the last
 * durable plan revision: equal run id, equal revision, digest and origin
 * execution, `plan_revisions.length === compiledPlan.plan_revision`, a
 * revision above 1, the immediately preceding plan record at revision −1
 * whose digest equals the new record's predecessor and the old
 * generation's plan digest — no intermediate plan revisions; the
 * compiled plan projection must carry the revised task exactly once with
 * the exact accepted id, revision and digest (the task may live in any
 * stage; absent, duplicated or stale → `plan_conflict`). The plan
 * revision must originate from the settled planning execution after the
 * target response: `executions.length === transitions.length + 1`, the
 * last execution is exactly `compiledPlan.origin_execution`, its journal
 * position agrees with its index, its role is strictly `planning`, its
 * type strictly `agent`, its phase strictly `cleanup_completed`, its
 * state id equals the declared `revise_task` target, its start boundary
 * equals the wait's transition count, and no committed transition, new
 * wait or new execution follows; any advance past the boundary is
 * fail-closed `invalid_state` or `lifecycle_conflict` with zero dispatch.
 *
 * Reconciliation is ONE internal classification point. C0: the old
 * generation is still open — the single closing command is pre-checked
 * through the single reducer on the local validated snapshot BEFORE the
 * dispatch (a rejection is a typed `invalid_state` with zero dispatch),
 * dispatched exactly once, and the authoritative snapshot is then
 * re-read and fully verified. C1: the same generation already carries
 * the exact closure (`by: "replanned"` with
 * `closed_transition_count === wait.transition_count`) — an idempotent
 * zero-dispatch success whose result is built from the already verified
 * authoritative snapshot; no snapshot is re-read after the
 * classification and no state is ever restored. A partially matching
 * closure is never an idempotent success.
 *
 * Post-dispatch verification is the same targeted comparison for the
 * normal resolve path and the racing `PipelineV2StateError` path: the
 * state revision moved exactly `before + 1`; the run id and the durable
 * pipeline identity did not change; status, phase, cursor and the
 * boundary journals are unchanged; the wait journal, the task ledger and
 * the plan ledger keep their length and their positional records; the
 * generation journal keeps its length with historical generations and
 * the historical iteration prefix unchanged; the target generation keeps
 * its index and identity bindings and its target iteration with the
 * exact replanned closure; the single new field is the exact generation
 * closure with `closed_transition_count === wait.transition_count`. The
 * comparisons are defensive contract-owned helpers with
 * `Array.isArray`/record guards; a malformed hostile snapshot yields a
 * typed controller error, never a `TypeError`; nothing is compared by
 * serialization and there is no recursive deep comparator. A racing
 * exact closure is recognized as an idempotent success on that full
 * verification; a resolve-without-change, a wrong revision delta, a
 * changed run id, an altered plan/task/wait/lifecycle binding or a
 * partial closure is a failure, never a success. The result is built
 * from the same snapshot that passed the verification; the sink's
 * snapshot is not read again after a successful classification.
 *
 * Durability: `PipelineV2RunStateDurabilityError` adopts the exact
 * closure candidate visible on disk, poisons the sink, stops every
 * further dispatch and fails `state_persist_failed` with the adopted
 * state; a fresh retry with a reopened sink recognizes the exact C1
 * closure with zero dispatch. A plain store error (`not_committed`)
 * leaves the previous open-generation state authoritative (the fresh
 * authoritative snapshot is re-read for the error) and a fresh retry
 * dispatches the closing command again. Unexpected errors keep their
 * identity. Diagnostics are content-free: no digest values, canonical
 * JSON, task bodies, paths, env values, credentials or hostile canaries.
 *
 * Runtime export surface (internal core) is exactly
 * `PipelineV2ReplannedGenerationControllerError` and
 * `closePipelineV2ReplannedGenerationInternal`; the public module exports
 * exactly `PipelineV2ReplannedGenerationControllerError` and
 * `closePipelineV2ReplannedGeneration` (types are not runtime keys). The
 * closed own reason set is `invalid_intent | invalid_state |
 * revision_conflict | plan_conflict | lifecycle_conflict |
 * state_persist_failed` with the last authoritative state (`null` until
 * the initial snapshot validated).
 *
 * Not implemented (stays unwired): the action/intent selection policy,
 * the architect execution and its output parsing, plan candidate
 * construction, the plan acceptance controller, opening the next
 * generation or iteration, stage selection and the initial-budget
 * policy, the graph transition commit, automatic resume,
 * coordinator/runner/CLI/default-pipeline wiring, schema changes,
 * migrations/API/T3 and multi-process locking.
 */

export type PipelineV2ReplannedGenerationControllerFailureReason =
  | "invalid_intent"
  | "invalid_state"
  | "revision_conflict"
  | "plan_conflict"
  | "lifecycle_conflict"
  | "state_persist_failed";

export class PipelineV2ReplannedGenerationControllerError extends Error {
  readonly reason: PipelineV2ReplannedGenerationControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2ReplannedGenerationControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    super(message);
    this.name = "PipelineV2ReplannedGenerationControllerError";
    this.reason = reason;
    this.state = state;
  }
}

/**
 * The structural sink seam; the production `PipelineV2RunStateSink`
 * satisfies it without an adapter.
 */
export interface PipelineV2ReplannedGenerationControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface ClosePipelineV2ReplannedGenerationOptions {
  readonly sink: PipelineV2ReplannedGenerationControllerSink;
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly compiledPlan: CompiledPipelineV2RunPlan;
}

export interface ClosedPipelineV2ReplannedGeneration {
  readonly wait_index: number;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly intent_sha256: string;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_sha256: string;
  readonly previous_plan_revision: number;
  readonly previous_plan_sha256: string;
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly origin_execution: number;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function controllerError(
  reason: PipelineV2ReplannedGenerationControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2ReplannedGenerationControllerError {
  return new PipelineV2ReplannedGenerationControllerError(reason, message, state);
}

const REVISE_TASK_ACTION_ID = "revise_task";

/**
 * The full defensive pass over the wait journal: every viewed entry is a
 * record; the target index occurs exactly once; with the journal length
 * pinned by the exact-count comparison the single occurrence also pins
 * the target to the last position. Duplicate target indexes, malformed
 * historical records and partial matches never pass.
 */
function waitJournalTargetRecord(state: PipelineV2RunState, waitIndex: number): PipelineV2WaitRecord {
  if (!Array.isArray(state.waits)) {
    throw controllerError("invalid_state", "the durable wait journal is not an array", state);
  }
  let occurrences = 0;
  let lastPosition = -1;
  for (let position = 0; position < state.waits.length; position += 1) {
    const entry = state.waits[position];
    if (!isRecord(entry)) {
      throw controllerError("invalid_state", "the durable wait journal carries a malformed record", state);
    }
    if (entry["index"] === waitIndex) {
      occurrences += 1;
      lastPosition = position;
    }
  }
  if (occurrences !== 1 || lastPosition !== state.waits.length - 1) {
    throw controllerError(
      "invalid_state",
      "the target wait is not the last and only record of its index in the durable wait journal",
      state,
    );
  }
  return state.waits[lastPosition] as unknown as PipelineV2WaitRecord;
}

/**
 * The accepted task revision of the target wait, read exclusively from
 * the ledger: exactly one wait-bound record (of ANY task), exact contract
 * fields against the intent, a positive safe revision above 1, and no
 * later revision of the same task.
 */
function acceptedTaskRevisionOfWait(
  state: PipelineV2RunState,
  wait: PipelineV2WaitRecord,
  manifest: PipelineV2ReviseTaskIntentManifest,
  intentSha256: string,
): Record<string, unknown> {
  if (!Array.isArray(state.task_revisions)) {
    throw controllerError("invalid_state", "the durable task ledger is not an array", state);
  }
  const bound: Record<string, unknown>[] = [];
  for (const entry of state.task_revisions) {
    if (!isRecord(entry)) {
      throw controllerError("invalid_state", "the durable task ledger carries a malformed record", state);
    }
    if (entry["wait_index"] === wait.index) {
      bound.push(entry);
    }
  }
  if (bound.length === 0) {
    throw controllerError("invalid_state", "the target wait carries no accepted task revision", state);
  }
  if (bound.length > 1) {
    throw controllerError(
      "revision_conflict",
      "the target wait carries several accepted task revisions",
      state,
    );
  }
  const record = bound[0];
  if (record === undefined) {
    throw controllerError("invalid_state", "the target wait carries no accepted task revision", state);
  }
  if (
    record["task_id"] !== manifest.task_id ||
    record["sha256"] !== manifest.new_task_revision_sha256 ||
    record["previous_sha256"] !== manifest.expected_previous_task_sha256 ||
    record["wait_index"] !== wait.index ||
    record["intent_sha256"] !== intentSha256 ||
    !isPositiveSafeInteger(record["revision"]) ||
    (record["revision"] as number) <= 1
  ) {
    throw controllerError(
      "revision_conflict",
      "the accepted task revision of the target wait contradicts the revise intent",
      state,
    );
  }
  const movedFurther = state.task_revisions.some(
    (entry) =>
      isRecord(entry) &&
      entry["task_id"] === manifest.task_id &&
      typeof entry["revision"] === "number" &&
      entry["revision"] > (record["revision"] as number),
  );
  if (movedFurther) {
    throw controllerError(
      "revision_conflict",
      "the task ledger already moved past the accepted revision",
      state,
    );
  }
  return record;
}

interface ReplannedGenerationContext {
  readonly generation: Record<string, unknown>;
  readonly generationIndex: number;
  readonly iterationIndex: number;
  readonly form: "c0" | "c1";
}

/**
 * The old generation must be the last durable generation, bound to the
 * previous plan digest, without an open iteration, with at least one
 * iteration whose last member is the exact replanned closure of the
 * target wait; the whole historical iteration prefix must be well-shaped.
 * The classification point: only the still-open form (C0) and the exact
 * already-durable replanned closure form (C1) are admissible.
 */
function replannedGenerationContext(
  state: PipelineV2RunState,
  wait: PipelineV2WaitRecord,
): ReplannedGenerationContext {
  if (!Array.isArray(state.generations) || state.generations.length === 0) {
    throw controllerError("invalid_state", "the durable state carries no stage generation", state);
  }
  const generation = state.generations[state.generations.length - 1];
  if (!isRecord(generation)) {
    throw controllerError("invalid_state", "the durable generation record is malformed", state);
  }
  const generationIndex = generation["index"];
  if (!isPositiveSafeInteger(generationIndex) || generationIndex !== state.generations.length) {
    throw controllerError(
      "lifecycle_conflict",
      "the replanned generation is not the last durable generation",
      state,
    );
  }
  if (
    typeof generation["stage_id"] !== "string" ||
    !isPositiveSafeInteger(generation["stage_position"]) ||
    typeof generation["template_id"] !== "string" ||
    typeof generation["plan_sha256"] !== "string" ||
    !isPositiveSafeInteger(generation["initial_budget"]) ||
    !isNonNegativeSafeInteger(generation["opened_transition_count"]) ||
    !isNonNegativeSafeInteger(generation["iteration_count"])
  ) {
    throw controllerError(
      "invalid_state",
      "the durable generation identity bindings are malformed",
      state,
    );
  }
  if (generation["open_iteration"] !== undefined) {
    throw controllerError(
      "invalid_state",
      "the target generation still carries an open iteration",
      state,
    );
  }
  const iterations = generation["iterations"];
  if (!Array.isArray(iterations) || iterations.length === 0) {
    throw controllerError(
      "invalid_state",
      "the target generation records no iterations",
      state,
    );
  }
  for (let position = 0; position < iterations.length; position += 1) {
    const iteration = iterations[position];
    if (!isRecord(iteration)) {
      throw controllerError(
        "invalid_state",
        "the durable iteration history carries a malformed record",
        state,
      );
    }
    if (!isPositiveSafeInteger(iteration["index"]) || !isNonNegativeSafeInteger(iteration["opened_transition_count"])) {
      throw controllerError(
        "invalid_state",
        "the durable iteration history carries a malformed record",
        state,
      );
    }
    const closed = iteration["closed"];
    if (closed !== undefined && (!isRecord(closed) || typeof closed["by"] !== "string")) {
      throw controllerError(
        "invalid_state",
        "the durable iteration history carries a malformed closure",
        state,
      );
    }
  }
  if (generation["iteration_count"] !== iterations.length) {
    throw controllerError(
      "invalid_state",
      "the durable generation iteration count does not match the iteration history",
      state,
    );
  }
  const lastIteration = iterations[iterations.length - 1] as Record<string, unknown>;
  if (lastIteration["index"] !== iterations.length) {
    throw controllerError(
      "invalid_state",
      "the target iteration is not the last recorded iteration",
      state,
    );
  }
  const closed = lastIteration["closed"];
  if (
    !isRecord(closed) ||
    closed["by"] !== "replanned" ||
    closed["wait_index"] !== wait.index ||
    closed["closed_transition_count"] !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the target iteration closure is not the exact replanned closure of the target wait",
      state,
    );
  }
  const generationClosed = generation["closed"];
  if (generationClosed === undefined) {
    return {
      generation,
      generationIndex,
      iterationIndex: lastIteration["index"] as number,
      form: "c0",
    };
  }
  if (
    !isRecord(generationClosed) ||
    generationClosed["by"] !== "replanned" ||
    generationClosed["closed_transition_count"] !== wait.transition_count
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the generation closure is not the exact replanned closure of the target wait",
      state,
    );
  }
  return {
    generation,
    generationIndex,
    iterationIndex: lastIteration["index"] as number,
    form: "c1",
  };
}

/**
 * The new accepted plan must be exactly the last durable plan revision,
 * an exact successor of the immediately preceding plan record whose
 * digest binds the old generation: no intermediate revisions, no stale or
 * foreign plan.
 */
function acceptedPlanBoundaries(
  state: PipelineV2RunState,
  compiledPlan: CompiledPipelineV2RunPlan,
  generation: Record<string, unknown>,
): Record<string, unknown> {
  if (!Array.isArray(state.plan_revisions)) {
    throw controllerError("invalid_state", "the durable plan ledger is not an array", state);
  }
  if (!isPositiveSafeInteger(compiledPlan.plan_revision) || compiledPlan.plan_revision <= 1) {
    throw controllerError("plan_conflict", "the accepted plan revision is not above the first revision", state);
  }
  if (state.plan_revisions.length !== compiledPlan.plan_revision) {
    throw controllerError("plan_conflict", "the compiled plan is not the last durable plan revision", state);
  }
  const last = state.plan_revisions[state.plan_revisions.length - 1];
  if (!isRecord(last)) {
    throw controllerError("plan_conflict", "the compiled plan is not the last durable plan revision", state);
  }
  if (
    last["revision"] !== compiledPlan.plan_revision ||
    last["sha256"] !== compiledPlan.plan_sha256 ||
    last["origin_execution"] !== compiledPlan.origin_execution
  ) {
    throw controllerError("plan_conflict", "the compiled plan is not the last durable plan revision", state);
  }
  const previous = state.plan_revisions[state.plan_revisions.length - 2];
  if (!isRecord(previous) || previous["revision"] !== compiledPlan.plan_revision - 1) {
    throw controllerError(
      "plan_conflict",
      "the immediately preceding durable plan revision is not the compiled plan's predecessor",
      state,
    );
  }
  if (last["previous_sha256"] !== previous["sha256"] || previous["sha256"] !== generation["plan_sha256"]) {
    throw controllerError(
      "plan_conflict",
      "the accepted plan is not the exact successor of the replanned generation's plan",
      state,
    );
  }
  return previous;
}

/**
 * The compiled plan projection must carry the revised task exactly once
 * with the exact accepted id, revision and digest; the task may live in
 * any stage of the plan.
 */
function compiledPlanCarriesRevisedTask(
  compiledPlan: CompiledPipelineV2RunPlan,
  taskRecord: Record<string, unknown>,
  state: PipelineV2RunState,
): void {
  if (!Array.isArray(compiledPlan.stages)) {
    throw controllerError("plan_conflict", "the compiled plan carries no stages", state);
  }
  let found = 0;
  for (const stage of compiledPlan.stages) {
    if (!isRecord(stage) || !Array.isArray(stage["tasks"])) {
      throw controllerError("plan_conflict", "the compiled plan carries a malformed stage", state);
    }
    for (const task of stage["tasks"]) {
      if (!isRecord(task)) {
        throw controllerError("plan_conflict", "the compiled plan carries a malformed task", state);
      }
      if (task["id"] === taskRecord["task_id"]) {
        found += 1;
        if (task["revision"] !== taskRecord["revision"] || task["sha256"] !== taskRecord["sha256"]) {
          throw controllerError(
            "plan_conflict",
            "the compiled plan does not carry the exact accepted task revision",
            state,
          );
        }
      }
    }
  }
  if (found !== 1) {
    throw controllerError(
      "plan_conflict",
      "the compiled plan does not carry the accepted task revision exactly once",
      state,
    );
  }
}

/**
 * The planning execution boundary: the settled planning execution right
 * after the target response, with no committed transition, new wait or
 * new execution beyond it. Any advance past the boundary fails closed.
 */
function planningExecutionBoundary(
  state: PipelineV2RunState,
  compiledPlan: CompiledPipelineV2RunPlan,
  declaredTo: string,
): void {
  if (!Array.isArray(state.executions) || !Array.isArray(state.transitions)) {
    throw controllerError("invalid_state", "the durable execution or transition journal is not an array", state);
  }
  if (state.executions.length !== state.transitions.length + 1) {
    throw controllerError(
      "invalid_state",
      "the durable run carries no settled-but-unbound planning execution",
      state,
    );
  }
  if (state.transitions.length > state.executions.length - 1) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable run already committed a transition past the planning execution",
      state,
    );
  }
  const last = state.executions[state.executions.length - 1];
  if (!isRecord(last)) {
    throw controllerError("invalid_state", "the durable execution journal carries a malformed record", state);
  }
  if (last["index"] !== state.executions.length) {
    throw controllerError(
      "invalid_state",
      "the durable execution journal position does not agree with the execution index",
      state,
    );
  }
  if (last["index"] > compiledPlan.origin_execution) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable run already advanced past the accepted plan's origin execution",
      state,
    );
  }
  if (last["index"] !== compiledPlan.origin_execution) {
    throw controllerError(
      "invalid_state",
      "the accepted plan revision does not originate from the last settled planning execution",
      state,
    );
  }
  if (last["type"] !== "agent" || last["execution_role"] !== "planning" || last["phase"] !== "cleanup_completed") {
    throw controllerError(
      "invalid_state",
      "the last settled execution is not the completed planning execution",
      state,
    );
  }
  if (last["state_id"] !== declaredTo) {
    throw controllerError(
      "invalid_state",
      "the planning execution does not run on the declared revise_task action target",
      state,
    );
  }
  if (last["index"] - 1 !== state.transitions.length) {
    throw controllerError(
      "invalid_state",
      "the planning execution start boundary does not match the wait boundary",
      state,
    );
  }
}

/**
 * The full targeted verification of the state that carries the applied
 * generation closure, against the pre-dispatch state. The same helper
 * verifies the normal resolve path and the racing reducer-rejection path.
 * Defensive, contract-owned, positional; no serialization and no deep
 * comparator.
 */
function verifyAppliedClosure(
  before: PipelineV2RunState,
  afterValue: unknown,
  wait: PipelineV2WaitRecord,
  generationIndex: number,
): PipelineV2RunState {
  if (!isRecord(afterValue)) {
    throw controllerError(
      "invalid_state",
      "the committed run state is not a durable pipeline v2 run state document",
      null,
    );
  }
  const after = afterValue as unknown as PipelineV2RunState;
  const mismatch = (): PipelineV2ReplannedGenerationControllerError =>
    controllerError(
      "lifecycle_conflict",
      "the committed run state does not carry the exact replanned generation closure",
      after,
    );
  if (after.revision !== before.revision + 1) {
    throw mismatch();
  }
  if (after.run_id !== before.run_id) {
    throw mismatch();
  }
  if (comparePipelineV2RunIdentity(before.pipeline, after.pipeline).kind !== "match") {
    throw mismatch();
  }
  if (after.status !== before.status || after.phase !== before.phase) {
    throw mismatch();
  }
  if (
    !isRecord(after.cursor) ||
    !isRecord(before.cursor) ||
    after.cursor["current_state"] !== before.cursor["current_state"] ||
    after.cursor["transition_count"] !== before.cursor["transition_count"]
  ) {
    throw mismatch();
  }
  if (!Array.isArray(after.transitions) || after.transitions.length !== before.transitions.length) {
    throw mismatch();
  }
  if (!Array.isArray(after.executions) || after.executions.length !== before.executions.length) {
    throw mismatch();
  }
  if (!Array.isArray(after.waits) || after.waits.length !== before.waits.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.waits.length; position += 1) {
    const beforeWait = before.waits[position];
    if (beforeWait === undefined || !waitRecordEquals(beforeWait, after.waits[position])) {
      throw mismatch();
    }
  }
  if (!taskLedgerEquals(before, after) || !planLedgerEquals(before, after)) {
    throw mismatch();
  }
  if (!Array.isArray(after.generations) || after.generations.length !== before.generations.length) {
    throw mismatch();
  }
  for (let position = 0; position < before.generations.length; position += 1) {
    const target = position === before.generations.length - 1;
    const beforeGeneration = before.generations[position];
    if (
      beforeGeneration === undefined ||
      !generationRecordEquals(beforeGeneration, after.generations[position], target, wait)
    ) {
      throw mismatch();
    }
  }
  return after;
}

/**
 * The exact positional equality of one wait record: identity fields,
 * ordered actions, intent and response digests.
 */
function waitRecordEquals(before: PipelineV2WaitRecord, afterValue: unknown): boolean {
  if (!isRecord(afterValue)) {
    return false;
  }
  const after = afterValue as unknown as PipelineV2WaitRecord;
  if (
    after.index !== before.index ||
    after.transition_count !== before.transition_count ||
    after.state_id !== before.state_id ||
    after.reason !== before.reason ||
    after.request_sha256 !== before.request_sha256
  ) {
    return false;
  }
  if (!Array.isArray(after.actions) || after.actions.length !== before.actions.length) {
    return false;
  }
  for (let position = 0; position < before.actions.length; position += 1) {
    const beforeAction = before.actions[position];
    const afterAction = after.actions[position];
    if (
      beforeAction === undefined ||
      !isRecord(afterAction) ||
      afterAction["id"] !== beforeAction.id ||
      afterAction["to"] !== beforeAction.to
    ) {
      return false;
    }
  }
  if (before.intent === undefined) {
    if (after.intent !== undefined) {
      return false;
    }
  } else {
    if (!isRecord(after.intent) || after.intent["intent_sha256"] !== before.intent.intent_sha256) {
      return false;
    }
  }
  if (before.response === undefined) {
    return after.response === undefined;
  }
  return (
    isRecord(after.response) &&
    after.response["action_id"] === before.response.action_id &&
    after.response["response_sha256"] === before.response.response_sha256
  );
}

/**
 * The exact positional equality of one task-ledger record.
 */
function taskLedgerEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.task_revisions) || after.task_revisions.length !== before.task_revisions.length) {
    return false;
  }
  return before.task_revisions.every((beforeEntry, position) => {
    const afterEntry = after.task_revisions[position];
    return (
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
 * The exact positional equality of one plan-ledger record.
 */
function planLedgerEquals(before: PipelineV2RunState, after: PipelineV2RunState): boolean {
  if (!Array.isArray(after.plan_revisions) || after.plan_revisions.length !== before.plan_revisions.length) {
    return false;
  }
  return before.plan_revisions.every((beforeEntry, position) => {
    const afterEntry = after.plan_revisions[position];
    return (
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
 * The exact positional equality of one generation record: identity
 * bindings, iteration count and history, open-iteration and closure
 * projections. On the target position the closure must be the exact
 * replanned closure of the wait; on historical positions the closure
 * projection must be unchanged.
 */
function generationRecordEquals(
  before: PipelineV2StageGenerationRecordView,
  afterValue: unknown,
  target: boolean,
  wait: PipelineV2WaitRecord,
): boolean {
  if (!isRecord(afterValue)) {
    return false;
  }
  const after = afterValue as unknown as Record<string, unknown>;
  if (
    after["index"] !== before.index ||
    after["stage_id"] !== before.stage_id ||
    after["stage_position"] !== before.stage_position ||
    after["template_id"] !== before.template_id ||
    after["plan_sha256"] !== before.plan_sha256 ||
    after["initial_budget"] !== before.initial_budget ||
    after["opened_transition_count"] !== before.opened_transition_count ||
    after["iteration_count"] !== before.iteration_count
  ) {
    return false;
  }
  if (!iterationProjectionEquals(before.open_iteration, after["open_iteration"])) {
    return false;
  }
  const afterClosed = after["closed"];
  if (target) {
    // The single new field of the C0 dispatch: the exact replanned
    // generation closure of the target wait.
    if (
      !isRecord(afterClosed) ||
      afterClosed["by"] !== "replanned" ||
      afterClosed["closed_transition_count"] !== wait.transition_count
    ) {
      return false;
    }
  } else if (!closureProjectionEquals(before.closed, afterClosed)) {
    return false;
  }
  if (!Array.isArray(after["iterations"]) || after["iterations"].length !== before.iterations.length) {
    return false;
  }
  for (let position = 0; position < before.iterations.length; position += 1) {
    const beforeIteration = before.iterations[position];
    if (beforeIteration === undefined || !iterationRecordEquals(beforeIteration, after["iterations"][position])) {
      return false;
    }
  }
  return true;
}

interface PipelineV2StageGenerationRecordView {
  readonly index: number;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly plan_sha256: string;
  readonly initial_budget: number;
  readonly opened_transition_count: number;
  readonly iteration_count: number;
  readonly open_iteration?: { readonly index: number; readonly opened_transition_count: number };
  readonly closed?: { readonly by: string; readonly closed_transition_count: number };
  readonly iterations: readonly PipelineV2StageIterationRecordView[];
}

interface PipelineV2StageIterationRecordView {
  readonly index: number;
  readonly opened_transition_count: number;
  readonly closed?: {
    readonly by: string;
    readonly wait_index?: number;
    readonly closed_transition_count: number;
  };
}

function iterationProjectionEquals(
  before: { readonly index: number; readonly opened_transition_count: number } | undefined,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(after) &&
    after["index"] === before.index &&
    after["opened_transition_count"] === before.opened_transition_count
  );
}

function closureProjectionEquals(
  before: { readonly by: string; readonly closed_transition_count: number } | undefined,
  after: unknown,
): boolean {
  if (before === undefined) {
    return after === undefined;
  }
  return (
    isRecord(after) &&
    after["by"] === before.by &&
    after["closed_transition_count"] === before.closed_transition_count
  );
}

function iterationRecordEquals(
  before: PipelineV2StageIterationRecordView,
  after: unknown,
): boolean {
  if (!isRecord(after) || after["index"] !== before.index || after["opened_transition_count"] !== before.opened_transition_count) {
    return false;
  }
  const beforeClosed = before.closed;
  const afterClosed = after["closed"];
  if (beforeClosed === undefined) {
    return afterClosed === undefined;
  }
  return (
    isRecord(afterClosed) &&
    afterClosed["by"] === beforeClosed.by &&
    afterClosed["wait_index"] === beforeClosed.wait_index &&
    afterClosed["closed_transition_count"] === beforeClosed.closed_transition_count
  );
}

/**
 * The declared `revise_task` action target of the answered target wait,
 * defensively checked: exactly one declared entry with a string target.
 */
function declaredReviseActionTo(state: PipelineV2RunState, wait: PipelineV2WaitRecord): string {
  if (!Array.isArray(wait.actions)) {
    throw controllerError("invalid_state", "the target wait declares no ordered actions", state);
  }
  let declaredTo: string | undefined;
  let declaredCount = 0;
  for (const action of wait.actions) {
    if (!isRecord(action)) {
      throw controllerError("invalid_state", "the target wait declares a malformed action", state);
    }
    if (action["id"] === REVISE_TASK_ACTION_ID) {
      declaredCount += 1;
      if (typeof action["to"] === "string") {
        declaredTo = action["to"];
      }
    }
  }
  if (declaredCount !== 1 || declaredTo === undefined) {
    throw controllerError(
      "invalid_state",
      "the target wait does not declare the revise_task action exactly",
      state,
    );
  }
  return declaredTo;
}

/**
 * The full validated boundary the controller closes the generation at.
 * The fixed check order (tested): the run shape → the wait journal → the
 * accepted intent → the task ledger → the old generation → the accepted
 * plan → the revised task in the compiled plan → the declared action
 * target with the cursor and boundary journals → the planning execution.
 * Used once on the initial authoritative snapshot and again on the
 * racing re-read.
 */
interface ValidatedReplannedBoundary {
  readonly state: PipelineV2RunState;
  readonly wait: PipelineV2WaitRecord;
  readonly declaredTo: string;
  readonly taskRecord: Record<string, unknown>;
  readonly generationIndex: number;
  readonly iterationIndex: number;
  readonly previousPlan: Record<string, unknown>;
  readonly form: "c0" | "c1";
}

function validateReplannedBoundary(
  state: PipelineV2RunState,
  manifest: PipelineV2ReviseTaskIntentManifest,
  intentSha256: string,
  compiledPlan: CompiledPipelineV2RunPlan,
): ValidatedReplannedBoundary {
  if (state.status !== "active" || state.phase !== "running") {
    throw controllerError("invalid_state", "the durable run is not at an active post-response boundary", state);
  }
  if (state.terminal !== undefined) {
    throw controllerError("invalid_state", "the durable run already reached a terminal state", state);
  }
  if (state.run_outputs !== undefined) {
    throw controllerError("invalid_state", "the durable run already published run outputs", state);
  }
  if (state.failure !== undefined) {
    throw controllerError("invalid_state", "the durable run already carries a failure reason", state);
  }
  const wait = waitJournalTargetRecord(state, manifest.wait_index);
  if (!isRecord(wait.intent) || wait.intent["intent_sha256"] !== intentSha256) {
    throw controllerError(
      "invalid_intent",
      "the target wait does not carry the exact accepted revise intent digest",
      state,
    );
  }
  if (!isRecord(wait.response) || wait.response["action_id"] !== REVISE_TASK_ACTION_ID) {
    throw controllerError(
      "invalid_state",
      "the target wait does not carry the exact revise_task response",
      state,
    );
  }
  if (manifest.run_id !== state.run_id) {
    throw controllerError("invalid_intent", "the revise intent belongs to a different run", state);
  }
  const taskRecord = acceptedTaskRevisionOfWait(state, wait, manifest, intentSha256);
  const context = replannedGenerationContext(state, wait);
  const previousPlan = acceptedPlanBoundaries(state, compiledPlan, context.generation);
  compiledPlanCarriesRevisedTask(compiledPlan, taskRecord, state);
  const declaredTo = declaredReviseActionTo(state, wait);
  if (!isRecord(state.cursor)) {
    throw controllerError("invalid_state", "the durable cursor is malformed", state);
  }
  if (
    !Array.isArray(state.transitions) ||
    !Array.isArray(state.executions)
  ) {
    throw controllerError(
      "invalid_state",
      "the durable transition or execution journal is not an array",
      state,
    );
  }
  if (state.cursor["transition_count"] > wait.transition_count || state.transitions.length > wait.transition_count) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable run already advanced past the wait boundary",
      state,
    );
  }
  if (
    state.cursor["transition_count"] !== wait.transition_count ||
    state.transitions.length !== wait.transition_count
  ) {
    throw controllerError(
      "invalid_state",
      "the durable cursor and transition journal do not sit exactly at the wait boundary",
      state,
    );
  }
  if (state.cursor["current_state"] !== declaredTo) {
    throw controllerError(
      "invalid_state",
      "the durable cursor is not on the declared revise_task action target",
      state,
    );
  }
  planningExecutionBoundary(state, compiledPlan, declaredTo);
  return {
    state,
    wait,
    declaredTo,
    taskRecord,
    generationIndex: context.generationIndex,
    iterationIndex: context.iterationIndex,
    previousPlan,
    form: context.form,
  };
}

/**
 * The manifest shape of the provenance-backed prepared intent, read only
 * after the registry gate.
 */
function intentManifestOf(intent: unknown): PipelineV2ReviseTaskIntentManifest {
  const manifest = (intent as Record<string, unknown>)["manifest"];
  if (!isRecord(manifest) || manifest["kind"] !== "revise_task_intent") {
    throw controllerError(
      "invalid_intent",
      "closePipelineV2ReplannedGeneration requires the provenance-backed prepared revise_task intent",
      null,
    );
  }
  const manifestRecord = manifest as unknown as PipelineV2ReviseTaskIntentManifest;
  if (
    typeof manifestRecord.run_id !== "string" ||
    !isPositiveSafeInteger(manifestRecord.wait_index) ||
    typeof manifestRecord.task_id !== "string"
  ) {
    throw controllerError(
      "invalid_intent",
      "closePipelineV2ReplannedGeneration requires the provenance-backed prepared revise_task intent",
      null,
    );
  }
  return manifestRecord;
}

export async function closePipelineV2ReplannedGenerationInternal(
  options: unknown,
): Promise<ClosedPipelineV2ReplannedGeneration> {
  // Capture boundary: the options shape, then every options field and the
  // sink's `poisoned`/`dispatch`/`snapshot` members each read exactly
  // once as opaque references, with `dispatch` bound to the sink before
  // the first await; later caller mutations cannot influence the run.
  if (!isRecord(options)) {
    throw controllerError(
      "invalid_state",
      "closePipelineV2ReplannedGeneration requires an options object",
      null,
    );
  }
  const sinkValue = options["sink"];
  const intentValue = options["intent"];
  const compiledPlanValue = options["compiledPlan"];
  if (!isRecord(sinkValue)) {
    throw controllerError("invalid_state", "closePipelineV2ReplannedGeneration requires a state sink", null);
  }
  if (!isRecord(intentValue)) {
    throw controllerError(
      "invalid_intent",
      "closePipelineV2ReplannedGeneration requires a prepared wait intent object",
      null,
    );
  }
  if (!isRecord(compiledPlanValue)) {
    throw controllerError(
      "plan_conflict",
      "closePipelineV2ReplannedGeneration requires a compiled run plan object",
      null,
    );
  }
  const sink = sinkValue as unknown as PipelineV2ReplannedGenerationControllerSink;
  const poisoned: unknown = (sinkValue as Record<string, unknown>)["poisoned"];
  if (poisoned === true) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned by a durability-unknown commit; the replanned generation is not closed for this run",
      null,
    );
  }
  const dispatchValue = (sinkValue as Record<string, unknown>)["dispatch"];
  if (typeof dispatchValue !== "function") {
    throw controllerError(
      "invalid_state",
      "closePipelineV2ReplannedGeneration requires a dispatchable state sink",
      null,
    );
  }
  const dispatchBound = (dispatchValue as (command: PipelineV2RunCommand) => void | Promise<void>).bind(sinkValue);
  const initialSnapshot: unknown = (sinkValue as Record<string, unknown>)["snapshot"];

  // Provenance gates: the prepared intent first, then the compiled plan,
  // both before any field read and with zero Proxy traps.
  if (!hasPreparedRunPlanProvenance(intentValue, "revise_task_intent")) {
    throw controllerError(
      "invalid_intent",
      "closePipelineV2ReplannedGeneration requires the provenance-backed prepared revise_task intent",
      null,
    );
  }
  const manifest = intentManifestOf(intentValue);
  const intentSha256 = (intentValue as Record<string, unknown>)["sha256"];
  if (typeof intentSha256 !== "string") {
    throw controllerError(
      "invalid_intent",
      "closePipelineV2ReplannedGeneration requires the provenance-backed prepared revise_task intent",
      null,
    );
  }
  if (!hasCompiledRunPlanProvenance(compiledPlanValue)) {
    throw controllerError(
      "plan_conflict",
      "closePipelineV2ReplannedGeneration requires the trusted compiled run plan returned by the plan acceptance",
      null,
    );
  }
  const compiledPlan = compiledPlanValue as unknown as CompiledPipelineV2RunPlan;

  // The single state validator; a missing or invalid snapshot is the
  // controller's own typed `invalid_state` with a fixed content-free
  // diagnostic and `state: null`; unexpected causes keep their identity.
  let state: PipelineV2RunState;
  try {
    state = validatePipelineV2RunState(initialSnapshot);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "closePipelineV2ReplannedGeneration requires a durable pipeline v2 run state document",
        null,
      );
    }
    throw cause;
  }

  // The hidden compiled-plan pipeline identity comparison through the
  // single existing structural comparator.
  const comparison = comparePipelineV2RunIdentity(
    compiledRunPlanOriginIdentity(compiledPlanValue),
    state.pipeline,
  );
  if (comparison.kind !== "match") {
    throw controllerError(
      "lifecycle_conflict",
      `the compiled plan's originating pipeline identity does not match the durable run identity (field ${comparison.field})`,
      state,
    );
  }

  // The full durable boundary validation on the initial authoritative
  // snapshot.
  const boundary = validateReplannedBoundary(state, manifest, intentSha256, compiledPlan);

  // The single reconciliation classification point.
  if (boundary.form === "c1") {
    return buildResult(boundary, compiledPlan);
  }

  // C0: pre-check the single closing command through the single reducer
  // on the local validated snapshot, then dispatch it exactly once.
  const command: PipelineV2RunCommand = {
    kind: "stage_generation_closed",
    generationIndex: boundary.generationIndex,
    by: "replanned",
  };
  try {
    reducePipelineV2RunCommand(state, command, new Date());
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "the verified boundary does not admit the stage_generation_closed command",
        state,
      );
    }
    throw cause;
  }
  try {
    await dispatchBound(command);
  } catch (cause) {
    if (cause instanceof PipelineV2RunStateDurabilityError) {
      // The rename succeeded; the candidate carries the exact closure.
      // The sink poisons itself; a fresh retry recognizes the exact C1
      // closure with zero dispatch.
      throw controllerError(
        "state_persist_failed",
        "the replanned generation closure commit is not confirmed durable; the visible closure candidate is authoritative",
        cause.candidate,
      );
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      // The rename did not happen; the previous open-generation state
      // stays authoritative. Re-read the fresh authoritative snapshot
      // for the failure.
      let authoritative: PipelineV2RunState | null = null;
      const fresh: unknown = (sinkValue as Record<string, unknown>)["snapshot"];
      if (isRecord(fresh)) {
        try {
          authoritative = validatePipelineV2RunState(fresh);
        } catch {
          authoritative = null;
        }
      }
      throw controllerError(
        "state_persist_failed",
        "the replanned generation closure commit did not happen; the previous state stays authoritative",
        authoritative,
      );
    }
    if (cause instanceof PipelineV2StateError) {
      // A racing completion already applied the exact closure; recognize
      // it through the same full targeted verification.
      const after: unknown = (sinkValue as Record<string, unknown>)["snapshot"];
      const verified = verifyAppliedClosure(state, after, boundary.wait, boundary.generationIndex);
      return buildResult(
        {
          ...boundary,
          state: verified,
          form: "c1",
        },
        compiledPlan,
      );
    }
    throw cause;
  }

  // The authoritative snapshot after the dispatch, verified by the same
  // targeted comparison; the result is built from the verified snapshot
  // without a second read.
  const after: unknown = (sinkValue as Record<string, unknown>)["snapshot"];
  const verified = verifyAppliedClosure(state, after, boundary.wait, boundary.generationIndex);
  return buildResult({ ...boundary, state: verified, form: "c1" }, compiledPlan);
}

/**
 * The unified content-free result, built from the single verified
 * snapshot; nothing caller-owned and no manifest body enters it.
 */
function buildResult(
  boundary: ValidatedReplannedBoundary,
  compiledPlan: CompiledPipelineV2RunPlan,
): ClosedPipelineV2ReplannedGeneration {
  return deepFreezeValue({
    wait_index: boundary.wait.index,
    generation_index: boundary.generationIndex,
    iteration_index: boundary.iterationIndex,
    intent_sha256: (boundary.wait.intent as { intent_sha256: string }).intent_sha256,
    task_id: boundary.taskRecord["task_id"] as string,
    task_revision: boundary.taskRecord["revision"] as number,
    task_sha256: boundary.taskRecord["sha256"] as string,
    previous_plan_revision: boundary.previousPlan["revision"] as number,
    previous_plan_sha256: boundary.previousPlan["sha256"] as string,
    plan_revision: compiledPlan.plan_revision,
    plan_sha256: compiledPlan.plan_sha256,
    origin_execution: compiledPlan.origin_execution,
    state: boundary.state,
  }) as ClosedPipelineV2ReplannedGeneration;
}
