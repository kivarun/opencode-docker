/**
 * Internal core of the restart-aware planning-run-plan handoff controller
 * for pipeline schema v2 (production-neutral, unwired).
 *
 * `applyPipelineV2PlanningRunPlanHandoff` composes the existing
 * authoritative chain into one fixed sequence that moves an existing
 * durable run from its planning boundary onto the caller-selected stage
 * of the newly accepted plan:
 *
 * - Branch A (the settled-but-unbound planning acceptance boundary):
 *   the planning-output composition accepts the run plan, the accepted
 *   `revise_task` intent is restored from the durable run through the
 *   public loader, the replanned stage opens (the old generation closed
 *   `by: "replanned"`, the new generation and its first iteration opened)
 *   and the planning transition is committed.
 * - Branch B (the exact committed handoff boundary, the crash/retry
 *   seam after the planning transition became durable while the result
 *   was lost): the accepted run plan is restored read-only, the same
 *   intent is restored, and the transition controller alone recognizes
 *   the exact C1 zero-dispatch boundary.
 *
 * The branch is selected once, from the captured authoritative snapshot,
 * before any downstream call; branch selection is never based on catching
 * a downstream error. Caller policy is exactly `stageId` and
 * `initialBudget`, captured before the first await and passed unchanged
 * to every downstream call — the durable state does not pin the caller
 * budget until the new generation exists, so only the caller replay
 * determines it.
 *
 * Runtime export surface is exactly `PipelineV2PlanningRunPlanHandoffControllerError`,
 * `applyPipelineV2PlanningRunPlanHandoffWithIo` and the frozen
 * `productionPlanningRunPlanHandoffOps` (the six existing public
 * facades/resolvers captured by identity — no reducer, store, filesystem,
 * parser, serializer, digest builder or registry capability is reachable).
 * Diagnostics are content-free; every composed layer's typed error and
 * every unexpected error passes through unchanged by object identity.
 */
import { requireResolvedPipelineV2Provenance } from "./pipeline_v2.ts";
import {
  acceptPipelineV2PlanningRunPlan,
} from "./pipeline_v2_planning_run_plan_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "./pipeline_v2_run_plan_restore.ts";
import { loadPipelineV2WaitIntent } from "./pipeline_v2_run_plan_store.ts";
import {
  openPipelineV2ReplannedStage,
  type OpenedPipelineV2ReplannedStage,
} from "./pipeline_v2_replanned_stage_controller.ts";
import {
  openPipelineV2ReplannedStageTransition,
  type OpenedPipelineV2ReplannedStageTransition,
} from "./pipeline_v2_replanned_stage_transition_controller.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import type { ResolvedPipelineV2 } from "./pipeline_v2.ts";
import type { PipelineV2RunCommand, PipelineV2RunState } from "./pipeline_v2_state.ts";
import { isPipelineV2SafeId, isLowercaseSha256, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/** The closed set of the controller's own failure reasons. */
export type PipelineV2PlanningRunPlanHandoffFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "invalid_result"
  | "artifact_missing";

const HANDOFF_REASONS: readonly PipelineV2PlanningRunPlanHandoffFailureReason[] = [
  "invalid_options",
  "invalid_state",
  "invalid_result",
  "artifact_missing",
];

/**
 * The controller's own typed error. Every composed layer's typed error
 * and every unexpected error passes through unchanged; this class marks
 * only the controller's own refusals and defensive verification failures.
 */
export class PipelineV2PlanningRunPlanHandoffControllerError extends Error {
  readonly reason: PipelineV2PlanningRunPlanHandoffFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2PlanningRunPlanHandoffFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    if (!HANDOFF_REASONS.includes(reason)) {
      throw new TypeError(`unknown pipeline v2 planning run plan handoff failure reason: ${String(reason)}`);
    }
    super(message);
    this.name = "PipelineV2PlanningRunPlanHandoffControllerError";
    this.reason = reason;
    this.state = state;
  }
}

function handoffError(
  reason: PipelineV2PlanningRunPlanHandoffFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2PlanningRunPlanHandoffControllerError {
  return new PipelineV2PlanningRunPlanHandoffControllerError(reason, message, state);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ownKeySet(value: object): string[] {
  return Object.keys(value).sort();
}

/**
 * The exact-key check of a verified-verification helper: a mismatch is the
 * controller's own `invalid_result` carrying the authoritative state the
 * caller has already established (never a lost `null`).
 */
function expectExactKeys(
  value: object,
  keys: readonly string[],
  what: string,
  state: PipelineV2RunState | null,
): void {
  const actual = ownKeySet(value).join(",");
  const expected = [...keys].sort().join(",");
  if (actual !== expected) {
    throw handoffError("invalid_result", `${what} does not carry the exact contract field set`, state);
  }
}

/**
 * Strict structural equality over plain JSON state documents: own
 * enumerable keys, arrays element-wise, scalars by ===. No serialization,
 * no key-order sensitivity, no undefined-key tolerance.
 */
function statesStructurallyEqual(left: unknown, right: unknown): boolean {
  if (left === right) {
    return true;
  }
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    if (Array.isArray(left) || Array.isArray(right)) {
      return false;
    }
    return left === right;
  }
  if (Array.isArray(left) !== Array.isArray(right)) {
    return false;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) {
      return false;
    }
    for (let index = 0; index < left.length; index += 1) {
      if (!statesStructurallyEqual(left[index], right[index])) {
        return false;
      }
    }
    return true;
  }
  const leftKeys = Object.keys(left as object).sort();
  const rightKeys = Object.keys(right as object).sort();
  if (leftKeys.length !== rightKeys.length) {
    return false;
  }
  for (let index = 0; index < leftKeys.length; index += 1) {
    if (leftKeys[index] !== rightKeys[index]) {
      return false;
    }
  }
  for (const key of leftKeys) {
    if (
      !statesStructurallyEqual(
        (left as Record<string, unknown>)[key],
        (right as Record<string, unknown>)[key],
      )
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Structural sink seam satisfied by the production `PipelineV2RunStateSink`
 * without an adapter.
 */
export interface PipelineV2PlanningRunPlanHandoffSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  dispatch: (command: PipelineV2RunCommand) => Promise<void>;
}

export interface PipelineV2PlanningRunPlanHandoffOps {
  readonly acceptPlanningRunPlan: typeof acceptPipelineV2PlanningRunPlan;
  readonly restoreAcceptedRunPlan: typeof restorePipelineV2AcceptedRunPlan;
  readonly loadWaitIntent: typeof loadPipelineV2WaitIntent;
  readonly openReplannedStage: typeof openPipelineV2ReplannedStage;
  readonly openReplannedStageTransition: typeof openPipelineV2ReplannedStageTransition;
  readonly compiledStageFor: typeof compiledPipelineV2RunPlanStageFor;
}

/**
 * The single frozen production ops object over the existing public
 * facades and resolvers.
 */
export const productionPlanningRunPlanHandoffOps: PipelineV2PlanningRunPlanHandoffOps = deepFreezeValue({
  acceptPlanningRunPlan: acceptPipelineV2PlanningRunPlan,
  restoreAcceptedRunPlan: restorePipelineV2AcceptedRunPlan,
  loadWaitIntent: loadPipelineV2WaitIntent,
  openReplannedStage: openPipelineV2ReplannedStage,
  openReplannedStageTransition: openPipelineV2ReplannedStageTransition,
  compiledStageFor: compiledPipelineV2RunPlanStageFor,
}) as PipelineV2PlanningRunPlanHandoffOps;

export interface ApplyPipelineV2PlanningRunPlanHandoffOptions {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRoot: string;
  readonly sink: PipelineV2PlanningRunPlanHandoffSink;
  readonly stageId: string;
  readonly initialBudget: number;
}

/** The exact downstream transition result, returned by object identity. */
export type AppliedPipelineV2PlanningRunPlanHandoff = OpenedPipelineV2ReplannedStageTransition;

interface DurableWaitRecord {
  readonly index: number;
  readonly transition_count: number;
  readonly state_id: string;
  readonly intent?: { readonly intent_sha256: string };
  readonly response?: { readonly action_id: string };
  readonly actions: readonly { readonly id: string; readonly to: string }[];
}

interface DurableGenerationRecord {
  readonly index: number;
  readonly stage_id: string;
  readonly template_id: string;
  readonly plan_sha256: string;
  readonly initial_budget: number;
  readonly opened_transition_count: number;
  readonly closed?: { readonly by: string; readonly closed_transition_count: number; readonly wait_index?: number };
  readonly open_iteration?: { readonly index: number };
  readonly iterations: readonly { readonly index: number; readonly opened_transition_count: number; readonly closed?: { readonly by: string; readonly wait_index?: number; readonly closed_transition_count: number } }[];
}

interface DurableExecutionRecord {
  readonly index: number;
  readonly state_id: string;
  readonly type: string;
  readonly phase: string;
  readonly execution_role?: string;
  readonly iteration_index?: number;
}

interface DurablePlanRecord {
  readonly revision: number;
  readonly sha256: string;
  readonly previous_sha256: string | null;
  readonly origin_execution: number;
}

interface DurableTaskRecord {
  readonly task_id: string;
  readonly revision: number;
  readonly sha256: string;
  readonly wait_index?: number;
  readonly intent_sha256?: string;
}

/**
 * The verified durable view used between the downstream calls; built
 * from one authoritative snapshot read per phase, never from a hostile
 * downstream result.
 */
interface VerifiedBoundary {
  readonly state: PipelineV2RunState;
  readonly targetWait: DurableWaitRecord;
}

/**
 * The authoritative revise target: the latest durable iteration closure
 * `{by: "replanned", wait_index}` names the current cycle's wait; the
 * journal must carry exactly one wait with that index, that wait must be
 * the last journal entry, answered `revise_task`, and carry its exact
 * intent. Arbitrary unchanged historical waits and generations are
 * allowed and never participate in the target uniqueness.
 */
function targetWaitOf(state: PipelineV2RunState): DurableWaitRecord {
  const generations = state.generations as readonly unknown[];
  if (!Array.isArray(generations)) {
    throw handoffError("invalid_state", "the durable generation journal is malformed", state);
  }
  let latestWaitIndex: number | null = null;
  for (const generation of generations) {
    if (!isRecord(generation) || !Array.isArray(generation["iterations"])) {
      throw handoffError("invalid_state", "the durable generation journal is malformed", state);
    }
    for (const iteration of generation["iterations"] as readonly unknown[]) {
      if (!isRecord(iteration)) {
        throw handoffError("invalid_state", "the durable generation journal is malformed", state);
      }
      const closed = iteration["closed"];
      if (isRecord(closed) && closed["by"] === "replanned" && typeof closed["wait_index"] === "number") {
        latestWaitIndex = closed["wait_index"];
      }
    }
  }
  if (latestWaitIndex === null) {
    throw handoffError(
      "invalid_state",
      "the durable revise cycle carries no replanned iteration closure",
      state,
    );
  }
  const journal = state.waits as readonly unknown[];
  if (!Array.isArray(journal) || journal.length === 0) {
    throw handoffError("invalid_state", "the durable wait journal is malformed", state);
  }
  let target: DurableWaitRecord | null = null;
  for (const entry of journal) {
    if (!isRecord(entry)) {
      throw handoffError("invalid_state", "the durable wait journal is malformed", state);
    }
    const actions = entry["actions"];
    if (!Array.isArray(actions)) {
      throw handoffError("invalid_state", "the durable wait journal is malformed", state);
    }
    for (const action of actions) {
      if (!isRecord(action) || typeof action["id"] !== "string") {
        throw handoffError("invalid_state", "the durable wait journal is malformed", state);
      }
    }
    if (entry["index"] === latestWaitIndex) {
      if (target !== null) {
        throw handoffError("invalid_state", "the durable wait journal carries the target wait index twice", state);
      }
      target = entry as unknown as DurableWaitRecord;
    }
  }
  if (target === null) {
    throw handoffError(
      "invalid_state",
      "the durable run carries no wait for the latest replanned iteration closure",
      state,
    );
  }
  const lastEntry = journal[journal.length - 1]! as unknown as DurableWaitRecord;
  if (lastEntry.index !== latestWaitIndex) {
    throw handoffError("invalid_state", "the run has progressed past the revise boundary", state);
  }
  if (target.response?.action_id !== "revise_task") {
    throw handoffError("invalid_state", "the durable revise_task wait is not answered", state);
  }
  if (target.intent === undefined || typeof target.intent.intent_sha256 !== "string") {
    throw handoffError("invalid_state", "the durable revise_task wait carries no accepted intent", state);
  }
  return target;
}

/**
 * The previous generation for the stage verification: the generation that
 * carries the target wait's replanned iteration closure — the same
 * generation every composed form (C0 fresh, C1, C2-bare, C2-open retry)
 * closed or recognized.
 */
function previousGenerationIndexOf(state: PipelineV2RunState, targetWait: DurableWaitRecord): number {
  const generations = state.generations as readonly unknown[];
  let found: number | null = null;
  for (const generation of generations) {
    if (!isRecord(generation) || !Array.isArray(generation["iterations"])) {
      throw handoffError("invalid_state", "the durable generation journal is malformed", state);
    }
    for (const iteration of generation["iterations"] as readonly unknown[]) {
      if (!isRecord(iteration)) {
        throw handoffError("invalid_state", "the durable generation journal is malformed", state);
      }
      const closed = iteration["closed"];
      if (isRecord(closed) && closed["by"] === "replanned" && closed["wait_index"] === targetWait.index) {
        found = (generation as Record<string, unknown>)["index"] as number;
      }
    }
  }
  if (found === null) {
    throw handoffError("invalid_state", "the durable revise cycle carries no replanned iteration closure", state);
  }
  return found;
}

function lastPlanRecordOf(state: PipelineV2RunState): DurablePlanRecord {
  const ledger = state.plan_revisions as readonly unknown[];
  if (!Array.isArray(ledger) || ledger.length === 0) {
    throw handoffError("invalid_state", "the durable run carries no accepted plan revision", state);
  }
  const last = ledger[ledger.length - 1]!;
  if (
    !isRecord(last) ||
    typeof last["revision"] !== "number" ||
    typeof last["sha256"] !== "string" ||
    typeof last["origin_execution"] !== "number"
  ) {
    throw handoffError("invalid_state", "the durable plan ledger is malformed", state);
  }
  return last as unknown as DurablePlanRecord;
}

function waitBoundTaskRecordOf(state: PipelineV2RunState, waitIndex: number): DurableTaskRecord {
  const ledger = state.task_revisions as readonly unknown[];
  if (!Array.isArray(ledger)) {
    throw handoffError("invalid_state", "the durable task ledger is malformed", state);
  }
  const bound = ledger.filter(
    (entry): entry is DurableTaskRecord => isRecord(entry) && (entry as Record<string, unknown>)["wait_index"] === waitIndex,
  );
  if (bound.length !== 1) {
    throw handoffError(
      "invalid_state",
      "the durable run does not carry exactly one wait-bound task revision",
      state,
    );
  }
  return bound[0]!;
}

function lastExecutionOf(state: PipelineV2RunState): DurableExecutionRecord {
  const executions = state.executions as readonly unknown[];
  if (!Array.isArray(executions) || executions.length === 0) {
    throw handoffError("invalid_state", "the durable run carries no executions", state);
  }
  const last = executions[executions.length - 1]!;
  if (!isRecord(last)) {
    throw handoffError("invalid_state", "the durable execution journal is malformed", state);
  }
  return last as unknown as DurableExecutionRecord;
}

/** The common active/running run boundary shared by both branches. */
function commonRunBoundary(state: PipelineV2RunState): void {
  if (state.status !== "active" || state.phase !== "running") {
    throw handoffError("invalid_state", "the run is not on an active running boundary", state);
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw handoffError("invalid_state", "the run has already reached a terminal outcome", state);
  }
}

/** The durable planning-execution shape shared by both branches. */
function settledPlanningExecution(state: PipelineV2RunState): void {
  const last = lastExecutionOf(state);
  if (last.type !== "agent" || last.execution_role !== "planning" || last.iteration_index !== undefined) {
    throw handoffError("invalid_state", "the last execution is not the planning execution", state);
  }
  if (last.phase !== "cleanup_completed") {
    throw handoffError("invalid_state", "the planning execution is not settled", state);
  }
}

/** Branch A: the settled-but-unbound planning acceptance boundary. */
function isPlanningAcceptanceBoundary(state: PipelineV2RunState): boolean {
  return state.executions.length === state.transitions.length + 1;
}

/**
 * The exact branch classifier. Reads only the captured authoritative
 * snapshot and never a downstream result; returns the branch id or
 * refuses with the controller's own typed failure.
 */
function classifyHandoffBoundary(state: PipelineV2RunState): "branch_a" | "branch_b" {
  commonRunBoundary(state);
  const targetWait = targetWaitOf(state);
  settledPlanningExecution(state);
  // Nothing may have progressed past the revise boundary in the wait
  // journal: a newer wait is later progress, never a handoff window.
  const journal = state.waits as readonly unknown[];
  const lastWait = journal[journal.length - 1]! as unknown as DurableWaitRecord;
  if (lastWait.transition_count > targetWait.transition_count) {
    throw handoffError("invalid_state", "the run has progressed past the revise boundary", state);
  }
  if (isPlanningAcceptanceBoundary(state)) {
    // Branch A: the planning execution is settled but unbound; the
    // execution sits on the cursor and the revise cycle is complete.
    const last = lastExecutionOf(state);
    if (last.state_id !== state.cursor.current_state) {
      throw handoffError("invalid_state", "the planning execution is not on the run cursor", state);
    }
    if (state.cursor.transition_count !== state.transitions.length) {
      throw handoffError("invalid_state", "the durable cursor does not match the transition journal", state);
    }
    return "branch_a";
  }
  if (state.executions.length === state.transitions.length) {
    // Branch B: the exact committed handoff boundary.
    const last = lastExecutionOf(state);
    const transitions = state.transitions as readonly unknown[];
    const lastTransition = transitions[transitions.length - 1]! as unknown as Record<string, unknown>;
    if (!isRecord(lastTransition)) {
      throw handoffError("invalid_state", "the durable transition journal is malformed", state);
    }
    if (lastTransition["execution_index"] !== last.index) {
      throw handoffError("invalid_state", "the last transition does not bind the planning execution", state);
    }
    if (state.cursor.current_state !== lastTransition["to"]) {
      throw handoffError("invalid_state", "the durable cursor does not sit on the planning transition target", state);
    }
    const generations = state.generations as readonly unknown[];
    const lastGeneration = generations[generations.length - 1]! as unknown as DurableGenerationRecord;
    if (
      !isRecord(lastGeneration) ||
      lastGeneration.closed !== undefined ||
      !isRecord(lastGeneration.open_iteration) ||
      lastGeneration.open_iteration["index"] !== lastGeneration.iterations.length
    ) {
      throw handoffError(
        "invalid_state",
        "the durable run does not carry the open replanned generation with its open iteration",
        state,
      );
    }
    if (state.cursor.transition_count !== state.transitions.length) {
      throw handoffError("invalid_state", "the durable cursor does not match the transition journal", state);
    }
    return "branch_b";
  }
  throw handoffError(
    "invalid_state",
    "the run is not on a planning handoff boundary",
    state,
  );
}

/**
 * The structural sink shape check: a record carrying a dispatch function.
 * Only the three contract members are ever read; hostile extras stay
 * unread. Runs after the pipeline provenance gate and before the snapshot
 * getter.
 */
function validateHandoffSinkShape(sink: unknown): void {
  if (!isRecord(sink)) {
    throw handoffError("invalid_options", "the handoff sink must be a structural record", null);
  }
  if (typeof sink["dispatch"] !== "function") {
    throw handoffError("invalid_options", "the handoff sink must carry a dispatch function", null);
  }
}

/** Probes compiled-plan provenance through the captured public stage selector. */
function probeCompiledPlan(
  compiledStageFor: PipelineV2PlanningRunPlanHandoffOps["compiledStageFor"],
  plan: unknown,
  state: PipelineV2RunState | null,
): CompiledPipelineV2RunPlanStage {
  if (!isRecord(plan)) {
    throw handoffError("invalid_result", "the composed plan is not a record", state);
  }
  const stages = plan["stages"];
  if (!Array.isArray(stages) || stages.length === 0) {
    throw handoffError("invalid_result", "the composed plan carries no stages", state);
  }
  const first = stages[0];
  if (!isRecord(first) || typeof first["id"] !== "string") {
    throw handoffError("invalid_result", "the composed plan stages are malformed", state);
  }
  try {
    return compiledStageFor(plan as unknown as CompiledPipelineV2RunPlan, first["id"]);
  } catch (cause) {
    if (cause instanceof PipelineV2CompiledRunPlanError) {
      throw handoffError("invalid_result", "the composed plan is not a provenance-backed compiled run plan", state);
    }
    throw cause;
  }
}

/**
 * The narrow post-call authoritative snapshot shape: after every composed
 * call the snapshot must be a record — any non-record value (not only
 * `null`) is the controller's own `invalid_state` carrying the last
 * previously verified authoritative snapshot, and the hostile value never
 * enters the diagnostics or the error state.
 */
function requirePostCallSnapshot(
  value: unknown,
  lastVerified: PipelineV2RunState | null,
): PipelineV2RunState {
  if (!isRecord(value)) {
    throw handoffError("invalid_state", "the run lost its durable state", lastVerified);
  }
  return value as unknown as PipelineV2RunState;
}

/**
 * The stage position in the compiled plan's declaration order; the
 * compiled stage projection itself does not carry a position field.
 */
function compiledStagePosition(plan: unknown, stageId: string): number {
  const stages = (plan as Record<string, unknown>)["stages"];
  if (!Array.isArray(stages)) {
    throw handoffError("invalid_result", "the composed plan stages are malformed", null);
  }
  for (let index = 0; index < stages.length; index += 1) {
    const stage = stages[index];
    if (isRecord(stage) && stage["id"] === stageId) {
      return index + 1;
    }
  }
  throw handoffError("invalid_result", "the composed plan does not declare the caller stage", null);
}

/**
 * Resolves the caller-selected stage through the exact public compiled
 * resolver; the real `stage_not_found` and provenance errors pass through
 * unchanged by identity. Separate from the provenance probe so the
 * template/entry bindings are always taken from the SELECTED stage.
 */
function resolveCallerStage(
  compiledStageFor: PipelineV2PlanningRunPlanHandoffOps["compiledStageFor"],
  compiledPlan: unknown,
  stageId: string,
): CompiledPipelineV2RunPlanStage {
  return compiledStageFor(compiledPlan as unknown as CompiledPipelineV2RunPlan, stageId);
}

interface VerifiedIntent {
  readonly intent: Record<string, unknown>;
  readonly digest: string;
}

/** Verifies the exact loader wrapper and binds the prepared intent to the durable state. */
function verifyIntentWrapper(
  wrapper: unknown,
  state: PipelineV2RunState,
  targetWait: DurableWaitRecord,
): VerifiedIntent {
  if (!isRecord(wrapper)) {
    throw handoffError("invalid_result", "the intent loader returned an unexpected shape", state);
  }
  expectExactKeys(wrapper, ["intent", "intent_path"], "the intent loader wrapper", state);
  if (typeof wrapper["intent_path"] !== "string") {
    throw handoffError("invalid_result", "the intent loader wrapper is malformed", state);
  }
  const intent = wrapper["intent"];
  if (!isRecord(intent) || !isRecord(intent["manifest"]) || intent["manifest"]["kind"] !== "revise_task_intent") {
    throw handoffError("invalid_result", "the loaded intent is not a prepared revise_task intent", state);
  }
  const digest = intent["sha256"];
  if (typeof digest !== "string" || !isLowercaseSha256(digest)) {
    throw handoffError("invalid_result", "the loaded intent is not a prepared revise_task intent", state);
  }
  const manifest = intent["manifest"] as Record<string, unknown>;
  if (manifest["run_id"] !== state.run_id || manifest["wait_index"] !== targetWait.index) {
    throw handoffError("invalid_state", "the loaded intent does not bind to the durable run and wait", state);
  }
  if (intent["sha256"] !== targetWait.intent?.intent_sha256) {
    throw handoffError("invalid_state", "the loaded intent does not match the accepted durable intent", state);
  }
  const taskRecord = waitBoundTaskRecordOf(state, targetWait.index);
  if (
    manifest["task_id"] !== taskRecord.task_id ||
    manifest["new_task_revision_sha256"] !== taskRecord.sha256
  ) {
    throw handoffError("invalid_state", "the loaded intent does not bind to the accepted task revision", state);
  }
  return { intent, digest };
}

interface VerifiedAcceptance {
  readonly compiledPlan: unknown;
  readonly state: PipelineV2RunState;
}

/** Verifies the composition controller's acceptance result against the authoritative state. */
function verifyAcceptanceResult(
  compiledStageFor: PipelineV2PlanningRunPlanHandoffOps["compiledStageFor"],
  result: unknown,
  sink: PipelineV2PlanningRunPlanHandoffSink,
): VerifiedAcceptance {
  // The authoritative snapshot is read first so every post-call failure
  // carries it, never a hostile presentation.
  const snapshot = requirePostCallSnapshot(sink.snapshot, null);
  if (!isRecord(result)) {
    throw handoffError("invalid_result", "the plan acceptance returned an unexpected shape", snapshot);
  }
  expectExactKeys(result, ["compiled_plan", "state"], "the plan acceptance result", snapshot);
  const compiledPlan = result["compiled_plan"];
  probeCompiledPlan(compiledStageFor, compiledPlan, snapshot);
  const plan = compiledPlan as Record<string, unknown>;
  if (result["state"] !== snapshot) {
    throw handoffError("invalid_result", "the plan acceptance result does not carry the authoritative run state", snapshot);
  }
  const lastPlan = lastPlanRecordOf(snapshot);
  if (
    plan["plan_revision"] !== lastPlan.revision ||
    plan["plan_sha256"] !== lastPlan.sha256 ||
    plan["origin_execution"] !== lastPlan.origin_execution
  ) {
    throw handoffError("invalid_result", "the accepted plan does not match the last durable plan revision", snapshot);
  }
  if (plan["run_id"] !== snapshot.run_id) {
    throw handoffError("invalid_result", "the accepted plan does not bind to the run", snapshot);
  }
  return { compiledPlan, state: snapshot };
}

/** Verifies the read-only plan restore result against the durable plan ledger. */
function verifyRestoreResult(
  compiledStageFor: PipelineV2PlanningRunPlanHandoffOps["compiledStageFor"],
  result: unknown,
  state: PipelineV2RunState,
): Record<string, unknown> {
  if (!isRecord(result)) {
    throw handoffError("invalid_result", "the plan restore returned an unexpected shape", state);
  }
  expectExactKeys(result, ["compiled_plan", "state"], "the plan restore result", state);
  const compiledPlan = result["compiled_plan"];
  probeCompiledPlan(compiledStageFor, compiledPlan, state);
  const plan = compiledPlan as Record<string, unknown>;
  const lastPlan = lastPlanRecordOf(state);
  if (
    plan["plan_revision"] !== lastPlan.revision ||
    plan["plan_sha256"] !== lastPlan.sha256 ||
    plan["origin_execution"] !== lastPlan.origin_execution
  ) {
    throw handoffError("invalid_result", "the restored plan does not match the last durable plan revision", state);
  }
  if (plan["run_id"] !== state.run_id) {
    throw handoffError("invalid_result", "the restored plan does not bind to the run", state);
  }
  if (!isRecord(result["state"]) || !statesStructurallyEqual(result["state"], state)) {
    throw handoffError("invalid_result", "the plan restore result does not carry the authoritative run state", state);
  }
  return compiledPlan as Record<string, unknown>;
}

const STAGE_RESULT_KEYS = [
  "wait_index",
  "previous_generation_index",
  "generation_index",
  "iteration_index",
  "intent_sha256",
  "plan_revision",
  "plan_sha256",
  "origin_execution",
  "stage_id",
  "stage_position",
  "template_id",
  "initial_budget",
  "state",
] as const;

/** Verifies the replanned-stage result against the caller policy and the verified data. */
function verifyStageResult(
  result: unknown,
  authoritativeState: PipelineV2RunState,
  intentDigest: string,
  compiledPlan: unknown,
  compiledStage: CompiledPipelineV2RunPlanStage,
  stageId: string,
  initialBudget: number,
  previousGenerationIndex: number,
  lastPlan: DurablePlanRecord,
  targetWait: DurableWaitRecord,
): PipelineV2RunState {
  const stagePosition = compiledStagePosition(compiledPlan, stageId);
  if (!isRecord(result)) {
    throw handoffError("invalid_result", "the replanned stage returned an unexpected shape", authoritativeState);
  }
  expectExactKeys(result, STAGE_RESULT_KEYS, "the replanned stage result", authoritativeState);
  if (
    result["wait_index"] !== targetWait.index ||
    result["intent_sha256"] !== intentDigest
  ) {
    throw handoffError("invalid_result", "the replanned stage result does not bind to the accepted intent", authoritativeState);
  }
  if (
    result["stage_id"] !== stageId ||
    result["initial_budget"] !== initialBudget ||
    result["stage_position"] !== stagePosition ||
    result["template_id"] !== compiledStage.template
  ) {
    throw handoffError("invalid_result", "the replanned stage result does not match the caller policy", authoritativeState);
  }
  if (
    !isPositiveSafeInteger(result["previous_generation_index"]) ||
    result["previous_generation_index"] !== previousGenerationIndex
  ) {
    throw handoffError("invalid_result", "the replanned stage result does not bind the previous generation", authoritativeState);
  }
  const plan = compiledPlan as Record<string, unknown>;
  if (
    result["plan_revision"] !== plan["plan_revision"] ||
    result["plan_sha256"] !== plan["plan_sha256"] ||
    result["origin_execution"] !== plan["origin_execution"] ||
    result["plan_revision"] !== lastPlan.revision ||
    result["plan_sha256"] !== lastPlan.sha256 ||
    result["origin_execution"] !== lastPlan.origin_execution
  ) {
    throw handoffError("invalid_result", "the replanned stage result does not bind to the accepted plan", authoritativeState);
  }
  const resultState = result["state"];
  if (!isRecord(resultState) || !statesStructurallyEqual(resultState, authoritativeState)) {
    throw handoffError("invalid_result", "the replanned stage result does not carry the authoritative run state", authoritativeState);
  }
  const resultGenerations = resultState["generations"];
  if (!Array.isArray(resultGenerations) || resultGenerations.length === 0) {
    throw handoffError("invalid_result", "the replanned stage result carries no generation journal", authoritativeState);
  }
  const newGeneration = resultGenerations[resultGenerations.length - 1]! as Record<string, unknown>;
  if (
    !isRecord(newGeneration) ||
    newGeneration["stage_id"] !== stageId ||
    newGeneration["stage_position"] !== stagePosition ||
    newGeneration["template_id"] !== compiledStage.template ||
    newGeneration["initial_budget"] !== initialBudget ||
    newGeneration["plan_sha256"] !== lastPlan.sha256 ||
    newGeneration["opened_transition_count"] !== targetWait.transition_count ||
    newGeneration["closed"] !== undefined ||
    !isRecord(newGeneration["open_iteration"])
  ) {
    throw handoffError("invalid_result", "the replanned stage result opened an unexpected generation", authoritativeState);
  }
  if (
    !isPositiveSafeInteger(result["generation_index"]) ||
    result["generation_index"] !== newGeneration["index"] ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    result["iteration_index"] !== (newGeneration["open_iteration"] as Record<string, unknown>)["index"]
  ) {
    throw handoffError("invalid_result", "the replanned stage result does not bind the opened generation and iteration", authoritativeState);
  }
  return resultState as unknown as PipelineV2RunState;
}

const TRANSITION_RESULT_KEYS = [
  "wait_index",
  "from_state",
  "to_state",
  "transition_index",
  "execution_index",
  "generation_index",
  "iteration_index",
  "stage_id",
  "stage_position",
  "template_id",
  "initial_budget",
  "plan_revision",
  "plan_sha256",
  "state",
] as const;

/** Verifies the transition result against the caller policy and the verified data. */
function verifyTransitionResult(
  result: unknown,
  authoritativeState: PipelineV2RunState,
  intentDigest: string,
  compiledPlan: unknown,
  compiledStage: CompiledPipelineV2RunPlanStage,
  stageId: string,
  initialBudget: number,
  lastPlan: DurablePlanRecord,
  targetWait: DurableWaitRecord,
): PipelineV2RunState {
  const stagePosition = compiledStagePosition(compiledPlan, stageId);
  if (!isRecord(result)) {
    throw handoffError("invalid_result", "the planning transition returned an unexpected shape", authoritativeState);
  }
  expectExactKeys(result, TRANSITION_RESULT_KEYS, "the planning transition result", authoritativeState);
  if (result["wait_index"] !== targetWait.index) {
    throw handoffError("invalid_result", "the planning transition result does not bind to the accepted intent", authoritativeState);
  }
  if (typeof result["from_state"] !== "string" || typeof result["to_state"] !== "string") {
    throw handoffError("invalid_result", "the planning transition result carries no transition binding", authoritativeState);
  }
  const resultState = result["state"];
  if (!isRecord(resultState) || !statesStructurallyEqual(resultState, authoritativeState)) {
    throw handoffError("invalid_result", "the planning transition result does not carry the authoritative run state", authoritativeState);
  }
  const transitions = resultState["transitions"] as readonly unknown[];
  if (!Array.isArray(transitions) || transitions.length === 0) {
    throw handoffError("invalid_result", "the planning transition result carries no transition journal", authoritativeState);
  }
  const bound = transitions[transitions.length - 1]!;
  if (
    !isRecord(bound) ||
    bound["from"] !== result["from_state"] ||
    bound["to"] !== result["to_state"] ||
    bound["outcome"] !== "completed" ||
    bound["execution_index"] !== result["execution_index"]
  ) {
    throw handoffError("invalid_result", "the planning transition result does not bind the committed transition", authoritativeState);
  }
  if (bound["index"] !== result["transition_index"]) {
    throw handoffError("invalid_result", "the planning transition result does not bind the committed transition", authoritativeState);
  }
  if (
    result["stage_id"] !== stageId ||
    result["initial_budget"] !== initialBudget ||
    result["stage_position"] !== stagePosition ||
    result["template_id"] !== compiledStage.template
  ) {
    throw handoffError("invalid_result", "the planning transition result does not match the caller policy", authoritativeState);
  }
  if (
    result["plan_revision"] !== lastPlan.revision ||
    result["plan_sha256"] !== lastPlan.sha256
  ) {
    throw handoffError("invalid_result", "the planning transition result does not bind to the accepted plan", authoritativeState);
  }
  // The flat generation/iteration bindings are taken from the durable
  // journal of the authoritative state, never trusted from the result.
  const generations = authoritativeState["generations"] as readonly unknown[];
  if (!Array.isArray(generations) || generations.length === 0) {
    throw handoffError("invalid_result", "the planning transition result carries no generation journal", authoritativeState);
  }
  const openGeneration = generations[generations.length - 1]! as Record<string, unknown>;
  if (
    !isRecord(openGeneration) ||
    openGeneration["stage_id"] !== stageId ||
    openGeneration["stage_position"] !== stagePosition ||
    openGeneration["template_id"] !== compiledStage.template ||
    openGeneration["initial_budget"] !== initialBudget ||
    openGeneration["plan_sha256"] !== lastPlan.sha256 ||
    openGeneration["opened_transition_count"] !== targetWait.transition_count ||
    openGeneration["closed"] !== undefined ||
    !isRecord(openGeneration["open_iteration"])
  ) {
    throw handoffError("invalid_result", "the planning transition result does not bind the open generation", authoritativeState);
  }
  if (
    !isPositiveSafeInteger(result["generation_index"]) ||
    result["generation_index"] !== openGeneration["index"] ||
    !isPositiveSafeInteger(result["iteration_index"]) ||
    result["iteration_index"] !== (openGeneration["open_iteration"] as Record<string, unknown>)["index"]
  ) {
    throw handoffError("invalid_result", "the planning transition result does not bind the open generation and iteration", authoritativeState);
  }
  if (
    typeof result["execution_index"] !== "number" ||
    result["execution_index"] !== lastExecutionOf(authoritativeState).index
  ) {
    throw handoffError("invalid_result", "the planning transition result does not bind the planning execution", authoritativeState);
  }
  return resultState as unknown as PipelineV2RunState;
}

/**
 * The internal core. The caller hands over the five contract options and
 * the per-call ops; every options field and ops member is read exactly
 * once before the first await.
 */
export async function applyPipelineV2PlanningRunPlanHandoffWithIo(
  options: ApplyPipelineV2PlanningRunPlanHandoffOptions,
  ops: PipelineV2PlanningRunPlanHandoffOps,
): Promise<AppliedPipelineV2PlanningRunPlanHandoff> {
  // Capture boundary: the options shape, then the five fields exactly
  // once in contract order. Hostile extras are never read.
  if (!isRecord(options)) {
    throw handoffError("invalid_options", "the handoff options are not a record", null);
  }
  const pipeline = options["pipeline"];
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const stageId = options["stageId"];
  const initialBudget = options["initialBudget"];

  // Ops capture: every member exactly once before the first await.
  if (!isRecord(ops)) {
    throw handoffError("invalid_options", "the handoff ops are not a record", null);
  }
  const acceptPlanningRunPlan = ops["acceptPlanningRunPlan"];
  const restoreAcceptedRunPlan = ops["restoreAcceptedRunPlan"];
  const loadWaitIntent = ops["loadWaitIntent"];
  const openReplannedStage = ops["openReplannedStage"];
  const openReplannedStageTransition = ops["openReplannedStageTransition"];
  const compiledStageFor = ops["compiledStageFor"];
  for (const [name, value] of [
    ["acceptPlanningRunPlan", acceptPlanningRunPlan],
    ["restoreAcceptedRunPlan", restoreAcceptedRunPlan],
    ["loadWaitIntent", loadWaitIntent],
    ["openReplannedStage", openReplannedStage],
    ["openReplannedStageTransition", openReplannedStageTransition],
    ["compiledStageFor", compiledStageFor],
  ] as const) {
    if (typeof value !== "function") {
      throw handoffError("invalid_options", `the handoff ops member ${name} is not a function`, null);
    }
  }

  // Scalar validation of the captured policy.
  if (typeof runRoot !== "string" || runRoot.length === 0) {
    throw handoffError("invalid_options", "the handoff run root must be a non-empty string", null);
  }
  if (typeof stageId !== "string" || !isPipelineV2SafeId(stageId)) {
    throw handoffError("invalid_options", "the handoff stage id must be a safe id", null);
  }
  if (!isPositiveSafeInteger(initialBudget)) {
    throw handoffError("invalid_options", "the handoff initial budget must be a positive safe integer", null);
  }

  // The pipeline provenance gate precedes every state read, filesystem
  // access and facade call.
  requireResolvedPipelineV2Provenance(pipeline as ResolvedPipelineV2, "pipeline v2 planning run plan handoff");

  // The structural sink check precedes the snapshot getter; the pipeline
  // provenance gate above precedes both.
  validateHandoffSinkShape(sink);
  // One authoritative initial snapshot read, then the branch
  // classification — before any downstream call.
  const initialSnapshot = sink.snapshot;
  if (!isRecord(initialSnapshot)) {
    throw handoffError("invalid_state", "the run has no durable state", null);
  }
  if (sink.poisoned) {
    throw handoffError("invalid_state", "the run state sink is poisoned by a durability-unknown commit", null);
  }
  const branch = classifyHandoffBoundary(initialSnapshot);
  const targetWait = targetWaitOf(initialSnapshot);

  if (branch === "branch_a") {
    // 1. The planning-output composition accepts the run plan.
    const accepted = await acceptPlanningRunPlan({
      pipeline: pipeline as ResolvedPipelineV2,
      runRoot,
      sink,
    });
    const verifiedAcceptance = verifyAcceptanceResult(compiledStageFor, accepted, sink);
    const acceptedState = verifiedAcceptance.state;
    const verifiedTargetWait = targetWaitOf(acceptedState);
    const lastPlan = lastPlanRecordOf(acceptedState);
    const intentWrapper = await loadWaitIntent(runRoot, verifiedTargetWait.index);
    if (intentWrapper === null) {
      throw handoffError("artifact_missing", "the accepted revise_task intent artifact is missing", acceptedState);
    }
    const intent = verifyIntentWrapper(intentWrapper, acceptedState, verifiedTargetWait);
    // The caller stage is resolved through the exact public resolver
    // BEFORE any durable stage write; the provenance probe above stays
    // separate.
    const compiledStage = resolveCallerStage(compiledStageFor, verifiedAcceptance.compiledPlan, stageId);
    const previousGenerationIndex = previousGenerationIndexOf(acceptedState, verifiedTargetWait);
    // 6. The replanned stage opens.
    const stage = await openReplannedStage({
      sink,
      intent: intent.intent as never,
      compiledPlan: verifiedAcceptance.compiledPlan as never,
      stageId,
      initialBudget,
    });
    const postStageSnapshot = requirePostCallSnapshot(sink.snapshot, acceptedState);
    verifyStageResult(stage, postStageSnapshot, intent.digest, verifiedAcceptance.compiledPlan, compiledStage, stageId, initialBudget, previousGenerationIndex, lastPlan, verifiedTargetWait);
    // 8. The planning transition is committed.
    const transition = await openReplannedStageTransition({
      pipeline: pipeline as ResolvedPipelineV2,
      sink,
      intent: intent.intent as never,
      compiledPlan: verifiedAcceptance.compiledPlan as never,
      stageId,
      initialBudget,
    });
    const postTransitionSnapshot = requirePostCallSnapshot(sink.snapshot, postStageSnapshot);
    const postTransitionState = verifyTransitionResult(
      transition,
      postTransitionSnapshot,
      intent.digest,
      verifiedAcceptance.compiledPlan,
      compiledStage,
      stageId,
      initialBudget,
      lastPlan,
      verifiedTargetWait,
    );
    void postTransitionState;
    return transition;
  }

  // Branch B: the exact committed handoff boundary.
  const restored = await restoreAcceptedRunPlan({
    pipeline: pipeline as ResolvedPipelineV2,
    runRoot,
    state: initialSnapshot,
  });
  const restoredPlan = verifyRestoreResult(compiledStageFor, restored, initialSnapshot);
  const verifiedTargetWait = targetWait;
  const intentWrapper = await loadWaitIntent(runRoot, verifiedTargetWait.index);
  if (intentWrapper === null) {
    throw handoffError("artifact_missing", "the accepted revise_task intent artifact is missing", initialSnapshot);
  }
  const intent = verifyIntentWrapper(intentWrapper, initialSnapshot, verifiedTargetWait);
  const compiledStage = resolveCallerStage(compiledStageFor, restoredPlan, stageId);
  const transition = await openReplannedStageTransition({
    pipeline: pipeline as ResolvedPipelineV2,
    sink,
    intent: intent.intent as never,
    compiledPlan: restoredPlan as never,
    stageId,
    initialBudget,
  });
  // Only the exact C1 zero-dispatch boundary is accepted: the durable
  // revision must not have moved since the capture.
  const postSnapshot = requirePostCallSnapshot(sink.snapshot, initialSnapshot);
  if (postSnapshot.revision !== initialSnapshot.revision) {
    throw handoffError("invalid_state", "the committed handoff boundary moved during the retry", initialSnapshot);
  }
  verifyTransitionResult(
    transition,
    postSnapshot,
    intent.digest,
    restoredPlan,
    compiledStage,
    stageId,
    initialBudget,
    lastPlanRecordOf(initialSnapshot),
    verifiedTargetWait,
  );
  return transition;
}
