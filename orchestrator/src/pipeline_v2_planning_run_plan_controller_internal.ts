/**
 * Internal core of the restart-aware planning-output → accepted-run-plan
 * composition controller for pipeline schema v2 (production-neutral,
 * unwired).
 *
 * This module is the single layer that composes the existing authoritative
 * chain into one fixed sequence, so one call turns the settled-but-unbound
 * planning acceptance boundary of an existing durable run into the
 * accepted run plan, surviving a process restart in between:
 *
 *   capture {pipeline, runRoot, sink} and the frozen production ops
 *   → the pipeline provenance gate (before any state read or filesystem
 *     access)
 *   → ONE authoritative `sink.snapshot` read
 *   → `restorePipelineV2PlanningAcceptanceContext` (the read-only
 *     restoration of exactly the planning acceptance boundary)
 *   → the exact last execution by the restored planning execution index
 *   → `compiledExecutionRoleFor` (the exact planning role and its
 *     `plan_output`)
 *   → `readAcceptedJsonOutput` over the restored accepted history and the
 *     exact execution index (the proposal value is read once, from the
 *     fixed orchestrator-owned output location)
 *   → `preparePipelineV2RunPlanProposal` (the existing proposal chain is
 *     the only validation and normalization of the value)
 *   → `constructPipelineV2RunPlanCandidateFromProposal` (the anchored,
 *     retry-aware construction)
 *   → the existing public acceptance verifier — only as the
 *     provenance/binding gate and the source of the expected compiled
 *     projection; never a second compiler
 *   → `acceptPipelineV2RunPlanCandidate` (the existing crash-safe
 *     acceptance: publication adoption, dispatch reconciliation, durable
 *     task/plan records)
 *   → the defensive verification of the acceptance result and the exact
 *     downstream result returned by identity — no new envelope.
 *
 * The controller performs no filesystem work of its own, never calls the
 * reducer or a store, never reads an output file or a manifest directly,
 * never re-parses JSON, never re-hashes the proposal, and never
 * reconstructs the accepted history, manifests or candidate itself. It
 * owns only the defensive verification of every downstream result between
 * the composed calls, so a hostile or malformed successful result is this
 * layer's own `invalid_result` and the next facade is never called —
 * never a leaked `TypeError`, never a healed failure downstream.
 *
 * Runtime export surface is exactly `PipelineV2PlanningRunPlanControllerError`
 * (own reasons `invalid_options | invalid_result`) and
 * `acceptPipelineV2PlanningRunPlanInternal`; all typed errors of the
 * composed layers and every unexpected error pass through unchanged by
 * object identity; diagnostics are content-free. The controller never
 * opens the sink, never loads the pipeline, never touches the
 * coordinator/runner/CLI; wiring into the next planning transition is a
 * separate increment.
 */
import { basename } from "node:path";
import type { ResolvedPipelineV2 } from "./pipeline_v2.ts";
import { requireResolvedPipelineV2Provenance } from "./pipeline_v2.ts";
import { isLowercaseSha256, isPipelineV2SafeId, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import type { PipelineV2RunState } from "./pipeline_v2_state.ts";
import { restorePipelineV2PlanningAcceptanceContext } from "./pipeline_v2_resume_context.ts";
import type { RestoredPipelineV2PlanningAcceptanceContext } from "./pipeline_v2_resume_context.ts";
import { compiledExecutionRoleFor } from "./pipeline_v2_orchestration.ts";
import { readAcceptedJsonOutput } from "./pipeline_v2_runtime.ts";
import type { VerifiedAcceptedJsonOutput } from "./pipeline_v2_runtime.ts";
import { preparePipelineV2RunPlanProposal } from "./pipeline_v2_run_plan_proposal.ts";
import type { PipelineV2RunPlanProposal } from "./pipeline_v2_run_plan_proposal.ts";
import { constructPipelineV2RunPlanCandidateFromProposal } from "./pipeline_v2_run_plan_construction.ts";
import { verifyPipelineV2RunPlanCandidateForAcceptance } from "./pipeline_v2_run_plan_acceptance.ts";
import { acceptPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_controller.ts";
import type {
  AcceptedPipelineV2RunPlanCandidate,
  PipelineV2RunPlanControllerSink,
} from "./pipeline_v2_run_plan_controller_internal.ts";
import { compiledPipelineV2RunPlanStageFor } from "./pipeline_v2_run_plan_compiled.ts";
import { PipelineV2CompiledRunPlanError } from "./pipeline_v2_run_plan_compiled.ts";
import type { CompiledPipelineV2RunPlan } from "./pipeline_v2_run_plan_compiled.ts";
import type { PreparedPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_candidate.ts";

export type PipelineV2PlanningRunPlanFailureReason = "invalid_options" | "invalid_result";

const REASON_SET: ReadonlySet<PipelineV2PlanningRunPlanFailureReason> = new Set([
  "invalid_options",
  "invalid_result",
]);

/**
 * A failure of the planning-output → accepted-run-plan composition with
 * its stable machine-readable `reason`. The reason is assigned where the
 * failing operation's semantics are known (never by classifying message
 * text), is immutable, and is one of the two fixed own reasons. Every
 * typed error of a composed layer keeps its own class and identity.
 */
export class PipelineV2PlanningRunPlanControllerError extends Error {
  declare readonly reason: PipelineV2PlanningRunPlanFailureReason;

  constructor(reason: PipelineV2PlanningRunPlanFailureReason, message: string) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 planning run plan controller error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2PlanningRunPlanControllerError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

export interface AcceptPipelineV2PlanningRunPlanOptions {
  /** The trusted compiled pipeline of the run. */
  readonly pipeline: ResolvedPipelineV2;
  /** The run root of the existing durable run; its basename binds the run. */
  readonly runRoot: string;
  /** The structural sink seam (the production `PipelineV2RunStateSink` satisfies it). */
  readonly sink: PipelineV2RunPlanControllerSink;
}

/**
 * The composition capabilities: exactly the existing public functions and
 * resolvers, nothing else. Every member is read exactly once before the
 * first await.
 */
export interface PlanningRunPlanControllerOps {
  readonly restorePlanningContext: typeof restorePipelineV2PlanningAcceptanceContext;
  readonly compiledExecutionRoleFor: typeof compiledExecutionRoleFor;
  readonly readAcceptedJsonOutput: typeof readAcceptedJsonOutput;
  readonly prepareProposal: typeof preparePipelineV2RunPlanProposal;
  readonly constructCandidate: typeof constructPipelineV2RunPlanCandidateFromProposal;
  readonly verifyCandidate: typeof verifyPipelineV2RunPlanCandidateForAcceptance;
  readonly acceptCandidate: typeof acceptPipelineV2RunPlanCandidate;
  readonly compiledStageFor: typeof compiledPipelineV2RunPlanStageFor;
}

/**
 * The single frozen production ops object over the existing public
 * facades and resolvers; every member is fixed at construction and can
 * never be reassigned through this object.
 */
export const productionPlanningRunPlanOps: PlanningRunPlanControllerOps = Object.freeze({
  restorePlanningContext: restorePipelineV2PlanningAcceptanceContext,
  compiledExecutionRoleFor,
  readAcceptedJsonOutput,
  prepareProposal: preparePipelineV2RunPlanProposal,
  constructCandidate: constructPipelineV2RunPlanCandidateFromProposal,
  verifyCandidate: verifyPipelineV2RunPlanCandidateForAcceptance,
  acceptCandidate: acceptPipelineV2RunPlanCandidate,
  compiledStageFor: compiledPipelineV2RunPlanStageFor,
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ownKeysSorted(value: object): string {
  return Object.keys(value).sort().join(",");
}

function controllerError(
  reason: PipelineV2PlanningRunPlanFailureReason,
  message: string,
): PipelineV2PlanningRunPlanControllerError {
  return new PipelineV2PlanningRunPlanControllerError(reason, message);
}

/**
 * Defensive verification of the restored planning-acceptance context:
 * the exact own-key shape, the state/output-history/cursor projections
 * bound to the restored state (never swapped), and the planning
 * execution index bound to the last durable execution and to the run
 * root.
 */
function verifyRestoredContext(
  restored: unknown,
  runRoot: string,
): RestoredPipelineV2PlanningAcceptanceContext {
  if (!isRecord(restored)) {
    throw controllerError(
      "invalid_result",
      "the restored planning acceptance context is not a record",
    );
  }
  if (ownKeysSorted(restored) !== "accepted_outputs,cursor,planning_execution_index,run_inputs,state") {
    throw controllerError(
      "invalid_result",
      "the restored planning acceptance context carries a different key set",
    );
  }
  const state = restored["state"];
  if (!isRecord(state)) {
    throw controllerError("invalid_result", "the restored context carries no state record");
  }
  if (!isRecord(restored["run_inputs"])) {
    throw controllerError(
      "invalid_result",
      "the restored context carries no run inputs snapshot record",
    );
  }
  if (!Array.isArray(restored["accepted_outputs"])) {
    throw controllerError(
      "invalid_result",
      "the restored context carries no accepted outputs list",
    );
  }
  const cursor = restored["cursor"];
  if (
    !isRecord(cursor) ||
    ownKeysSorted(cursor) !== "current_state,transition_count" ||
    typeof cursor["current_state"] !== "string" ||
    typeof cursor["transition_count"] !== "number"
  ) {
    throw controllerError(
      "invalid_result",
      "the restored context carries a malformed cursor projection",
    );
  }
  const planningIndex = restored["planning_execution_index"];
  if (!isPositiveSafeInteger(planningIndex)) {
    throw controllerError(
      "invalid_result",
      "the restored context carries no positive planning execution index",
    );
  }
  const executions = state["executions"];
  if (!Array.isArray(executions) || executions.length !== planningIndex) {
    throw controllerError(
      "invalid_result",
      "the restored context's planning execution index does not name the last durable execution",
    );
  }
  const lastExecution = executions[executions.length - 1];
  if (!isRecord(lastExecution) || lastExecution["index"] !== planningIndex) {
    throw controllerError(
      "invalid_result",
      "the restored context's planning execution index does not name the last durable execution",
    );
  }
  const stateCursor = state["cursor"];
  if (
    !isRecord(stateCursor) ||
    stateCursor["current_state"] !== cursor["current_state"] ||
    stateCursor["transition_count"] !== cursor["transition_count"]
  ) {
    throw controllerError(
      "invalid_result",
      "the restored context's cursor projection does not match the restored state's cursor",
    );
  }
  if (state["run_id"] !== basename(runRoot)) {
    throw controllerError(
      "invalid_result",
      "the restored context's state does not belong to this run",
    );
  }
    return restored as unknown as RestoredPipelineV2PlanningAcceptanceContext;
}

/**
 * Defensive verification of the reader result: the exact own-key shape
 * and the exact state/output/activation binding.
 */
function verifyReadResult(
  read: unknown,
  stateId: string,
  outputId: string,
  activationIndex: number,
): VerifiedAcceptedJsonOutput {
  if (!isRecord(read)) {
    throw controllerError("invalid_result", "the accepted JSON output read result is not a record");
  }
  if (ownKeysSorted(read) !== "activation_index,digest,output,state,value") {
    throw controllerError(
      "invalid_result",
      "the accepted JSON output read result carries a different key set",
    );
  }
  if (read["state"] !== stateId || read["output"] !== outputId) {
    throw controllerError(
      "invalid_result",
      "the accepted JSON output read result does not bind the requested state and output",
    );
  }
  if (read["activation_index"] !== activationIndex) {
    throw controllerError(
      "invalid_result",
      "the accepted JSON output read result does not bind the requested activation index",
    );
  }
  if (!isLowercaseSha256(read["digest"])) {
    throw controllerError(
      "invalid_result",
      "the accepted JSON output read result carries no lowercase SHA-256 digest",
    );
  }
  return read as unknown as VerifiedAcceptedJsonOutput;
}

/**
 * Defensive verification of the acceptance verifier's compiled
 * expectation: the exact own-key shape and a stages list. It is the
 * comparison basis for the result's compiled projection, and a hostile or
 * malformed expectation is rejected before the acceptance is ever called.
 */
function verifyExpectedCompiled(expected: unknown): CompiledPipelineV2RunPlan {
  if (!isRecord(expected)) {
    throw controllerError(
      "invalid_result",
      "the acceptance verifier's compiled expectation is not a record",
    );
  }
  if (ownKeysSorted(expected) !== "origin_execution,plan_revision,plan_sha256,run_id,stages") {
    throw controllerError(
      "invalid_result",
      "the acceptance verifier's compiled expectation carries a different key set",
    );
  }
  if (!Array.isArray(expected["stages"])) {
    throw controllerError(
      "invalid_result",
      "the acceptance verifier's compiled expectation carries no stages list",
    );
  }
  return expected as unknown as CompiledPipelineV2RunPlan;
}

/**
 * Defensive verification of the successful acceptance result: the exact
 * two-key shape, the compiled projection exactly as the verifier
 * expected, the compiled plan's provenance through the existing public
 * selector, and the state identical to the authoritative sink snapshot
 * after the success.
 */
function verifyAcceptedResult(
  accepted: unknown,
  expected: CompiledPipelineV2RunPlan,
  runRoot: string,
  compiledStageFor: PlanningRunPlanControllerOps["compiledStageFor"],
  sink: PipelineV2RunPlanControllerSink,
): AcceptedPipelineV2RunPlanCandidate {
  if (!isRecord(accepted)) {
    throw controllerError("invalid_result", "the run plan acceptance result is not a record");
  }
  if (ownKeysSorted(accepted) !== "compiled_plan,state") {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result carries a different key set",
    );
  }
  const compiledPlan = accepted["compiled_plan"];
  const state = accepted["state"];
  if (!isRecord(compiledPlan)) {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result carries no compiled plan record",
    );
  }
  if (!isRecord(state)) {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result carries no state record",
    );
  }
  // The compiled projection exactly as expected, and the compiled plan's
  // provenance through the existing public selector (a look-alike is
  // rejected there; the failure is this layer's own invalid_result).
  if (JSON.stringify(compiledPlan) !== JSON.stringify(expected)) {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result's compiled projection differs from the acceptance verifier's expectation",
    );
  }
  const firstStage = expected.stages[0];
  if (firstStage !== undefined) {
    try {
      compiledStageFor(compiledPlan as unknown as CompiledPipelineV2RunPlan, firstStage.id);
    } catch (cause) {
      if (cause instanceof PipelineV2CompiledRunPlanError) {
        throw controllerError(
          "invalid_result",
          "the run plan acceptance result's compiled plan carries no compiled-plan provenance",
        );
      }
      throw cause;
    }
  }
  if (state["run_id"] !== basename(runRoot)) {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result's state does not belong to this run",
    );
  }
  // The state must be the authoritative sink snapshot after the success
  // — by identity, so a swapped or copied state can never heal a hostile
  // result.
  const snapshotAfter = sink.snapshot;
  if (state !== (snapshotAfter as unknown)) {
    throw controllerError(
      "invalid_result",
      "the run plan acceptance result's state is not the authoritative sink snapshot",
    );
  }
  return accepted as unknown as AcceptedPipelineV2RunPlanCandidate;
}

export async function acceptPipelineV2PlanningRunPlanInternal(
  options: unknown,
  ops: PlanningRunPlanControllerOps,
): Promise<AcceptedPipelineV2RunPlanCandidate> {
  // 1. Capture boundary: the options shape, the three fields and all ops
  //    members are read exactly once, in this fixed order, all before the
  //    first await. Hostile extra fields are never read. The sink's
  //    snapshot getter is not touched here (the authoritative reads are
  //    exactly the two below).
  if (!isRecord(options)) {
    throw controllerError(
      "invalid_options",
      "acceptPipelineV2PlanningRunPlan requires an options object",
    );
  }
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot = options["runRoot"];
  const sink = options["sink"] as PipelineV2RunPlanControllerSink;
  if (!isRecord(ops)) {
    throw controllerError(
      "invalid_options",
      "acceptPipelineV2PlanningRunPlan requires a frozen ops record",
    );
  }
  const restorePlanningContext = ops["restorePlanningContext"];
  const compiledRoleFor = ops["compiledExecutionRoleFor"];
  const readJsonOutput = ops["readAcceptedJsonOutput"];
  const prepareProposal = ops["prepareProposal"];
  const constructCandidate = ops["constructCandidate"];
  const verifyCandidate = ops["verifyCandidate"];
  const acceptCandidate = ops["acceptCandidate"];
  const compiledStageFor = ops["compiledStageFor"];
  if (
    typeof restorePlanningContext !== "function" ||
    typeof compiledRoleFor !== "function" ||
    typeof readJsonOutput !== "function" ||
    typeof prepareProposal !== "function" ||
    typeof constructCandidate !== "function" ||
    typeof verifyCandidate !== "function" ||
    typeof acceptCandidate !== "function" ||
    typeof compiledStageFor !== "function"
  ) {
    throw controllerError(
      "invalid_options",
      "the composition requires the eight existing facades and resolvers",
    );
  }
  if (typeof runRoot !== "string" || runRoot === "") {
    throw controllerError("invalid_options", "the run root must be a non-empty string");
  }
  if (!isRecord(sink)) {
    throw controllerError(
      "invalid_options",
      "the composition requires a structural sink with an authoritative snapshot",
    );
  }
  // 2. The pipeline provenance gate: before any state read, any
  //    filesystem access and any further sink evaluation beyond the
  //    capture.
  requireResolvedPipelineV2Provenance(pipeline, "planning run plan acceptance");

  // 3. ONE authoritative snapshot read.
  const stateBefore = sink.snapshot;

  // 4. The read-only restoration of the planning acceptance boundary;
  //    every typed failure keeps its class and identity.
  const restored = await restorePlanningContext(pipeline, stateBefore, runRoot);

  // 5. The exact planning execution, with the restored context verified
  //    defensively first.
  verifyRestoredContext(restored, runRoot);
  const executionRecord = restored.state.executions[restored.planning_execution_index - 1]!;

  // 6. The exact compiled planning role of the execution's state.
  const role = compiledRoleFor(pipeline, executionRecord.state_id);
  if (
    role.state_id !== executionRecord.state_id ||
    role.role !== "planning" ||
    !isPipelineV2SafeId(role.plan_output)
  ) {
    throw controllerError(
      "invalid_result",
      "the compiled execution role of the planning execution's state is not the exact planning role with a safe plan output",
    );
  }

  // 7. The proposal value: read once from the restored accepted history
  //    through the existing reader; its typed errors pass through.
  const read = await readJsonOutput(
    pipeline,
    runRoot,
    restored.accepted_outputs,
    executionRecord.state_id,
    role.plan_output,
    restored.planning_execution_index,
  );
  verifyReadResult(read, executionRecord.state_id, role.plan_output, restored.planning_execution_index);

  // 8. The proposal: the existing chain is the only validation; its typed
  //    errors pass through unchanged.
  const proposal = prepareProposal(read.value) as PipelineV2RunPlanProposal;

  // 9. The anchored, retry-aware construction; its typed errors pass
  //    through unchanged.
  const candidate = (await constructCandidate({
    runRoot,
    state: restored.state,
    proposal,
  })) as PreparedPipelineV2RunPlanCandidate;

  // 10. The existing public acceptance verifier: only the
  //     provenance/binding gate and the source of the expected compiled
  //     projection; never a second compiler. Its typed errors pass
  //     through unchanged; the expectation is verified defensively
  //     before the acceptance is ever called.
  const expected = verifyExpectedCompiled(verifyCandidate(pipeline, restored.state, candidate));

  // 11. The existing crash-safe acceptance; its typed errors pass
  //     through unchanged.
  const accepted = (await acceptCandidate({
    pipeline,
    runRoot,
    sink,
    candidate,
  })) as AcceptedPipelineV2RunPlanCandidate;

  // 12. Defensive verification of the successful result, then the exact
  //     downstream result by identity — no new envelope, no further
  //     snapshot read on any failure path.
  return verifyAcceptedResult(accepted, expected, runRoot, compiledStageFor, sink);
}
