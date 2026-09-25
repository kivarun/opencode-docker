/**
 * Production-neutral stage generation/iteration controller for pipeline
 * schema v2 (unwired).
 *
 * Given a provenance-backed `CompiledPipelineV2RunPlan`, a caller-chosen
 * `stageId` and a policy-owned `initialBudget`, this module guarantees —
 * against the durable run state — that the selected compiled stage has an
 * open generation bound exactly to that stage and to the last durable
 * plan revision, and that this generation has an open iteration. The
 * caller chooses the stage and the budget; the controller derives every
 * durable binding, index and anchor from the trusted compiled projection
 * and the authoritative durable state, and never accepts a stage id,
 * position, template, plan digest or anchor from the caller:
 *
 * - the stage is resolved only through the existing compiled projection's
 *   stage lookup (`compiledPipelineV2RunPlanStageFor` — the single
 *   provenance gate and lookup);
 * - the stage position is derived only from the compiled plan's stage
 *   declaration order;
 * - the transition anchor is derived only from the durable cursor's
 *   transition count;
 * - the generation and iteration indexes are derived only from the
 *   durable ledgers (`generations.length + 1`,
 *   `generation.iteration_count + 1`).
 *
 * Reconciliation over the durable state:
 *
 * - no open generation → the sequence `stage_generation_opened` then
 *   `stage_iteration_opened` (iteration 1) is prepared;
 * - an open generation that matches the compiled stage id, position,
 *   template, plan digest and the initial budget exactly: with an open
 *   iteration the call is an idempotent success (zero dispatch); without
 *   one, `stage_iteration_opened` of the next iteration index is
 *   prepared;
 * - an open generation differing in any of those five fields is a
 *   `lifecycle_conflict` — a foreign generation is never accepted, never
 *   closed and never rewritten.
 *
 * The reducer (`reducePipelineV2RunCommand`) stays the single successor
 * authority: the whole missing sequence is pre-checked against the
 * reducer on a local snapshot before the first dispatch, and the
 * authoritative state comes exclusively from the sink. After every
 * dispatch the authoritative sink snapshot is re-read and must
 * structurally carry exactly the expected record; a sink that resolves
 * without the expected snapshot change, and a reducer rejection whose
 * authoritative snapshot does not carry the exact expected
 * generation/iteration, are `lifecycle_conflict` failures — a racing
 * identical dispatch is idempotent success only on that exact match. No
 * rollback and no automatic second dispatch ever happen.
 *
 * Capture boundary: every options field and every sink member
 * (`poisoned`, `dispatch`, `snapshot`) is read exactly once as an opaque
 * reference; no field of the compiled plan, the snapshot document or the
 * stage id is traversed before the compiled-plan provenance gate has run
 * (the gate is the first validation of the compiled plan and stage id
 * contents after the fail-closed sink poison latch). An unexpected error
 * from a sink getter propagates unchanged. Caller objects are never
 * frozen or modified.
 *
 * The compiled plan's provenance is the module-private WeakMap binding of
 * the compiled-plan layer: every compiled projection carries the
 * immutable identity snapshot of its originating pipeline, and the
 * controller compares that hidden identity against the durable
 * `state.pipeline` exclusively through the single existing
 * `comparePipelineV2RunIdentity` after the state validation — a mismatch
 * of any of the five durable identity fields is `lifecycle_conflict` with
 * zero dispatch, so a compiled plan compiled from a foreign pipeline is
 * never accepted even when run id, plan revision and plan digest coincide.
 *
 * Only this layer's own failures are
 * `PipelineV2StageIterationControllerError` with the closed reason set
 * (`invalid_options`, `invalid_state`, `lifecycle_conflict`,
 * `state_persist_failed`): hostile options or a non-positive/non-safe
 * `initialBudget` are `invalid_options`; a missing or invalid durable
 * state document, an invalid run boundary and reducer pre-check
 * rejections are `invalid_state`; a foreign run, a compiled plan that is
 * not the last durable plan revision, a mismatching open generation and
 * a racing different lifecycle are `lifecycle_conflict`; sink
 * `not_committed`/`durability_unknown` outcomes are
 * `state_persist_failed` with the last authoritative (adopted) state.
 * The compiled-plan resolver's own errors keep their classes.
 * Diagnostics are content-free: validated safe ids, indexes, revisions
 * and closed operation classes only — no bodies, canonical JSON, paths,
 * digest values, env values or credentials.
 *
 * The second public API of the same layer,
 * `closePipelineV2StageIteration({compiledPlan, stageId,
 * iterationCloseReason, generationCloseReason?, sink})`, closes the
 * active stage iteration of the selected compiled stage and — by the
 * caller's explicit decision — its generation, on the contract hook
 * boundary `settled stage execution → stage_iteration_closed → optional
 * stage_generation_closed → transition_committed`. The caller has
 * already made every policy decision: the iteration close reason
 * (`normal_close` or `exhausted`), whether the generation closes
 * (`next_stage` or `final_stage`). The wait-bound closure reasons
 * (`grant`, `replanned`), the generation `replanned` reason, wait
 * indexes, caller-supplied indexes, anchors, transition counts, stage
 * positions, templates, plan digests and target states are not part of
 * this API — wait-bound closure and replanning stay a later increment.
 * Before the first dispatch the controller proves the closure boundary:
 * active/running with no terminal, run outputs, failure or open wait;
 * exactly one settled-but-unbound execution
 * (`executions.length === transitions.length + 1`) whose execution role
  * is exactly `stage` (never inferred from the profile, the executor name
  * or the state id) in the agent phase `cleanup_completed` or the
  * decision phase `evaluated` exactly, whose `state_id` is one of the
  * compiled stage's state ids, and whose recorded `iteration_index`
  * resolves — through the single existing exact resolver
  * `pipelineV2StageIterationAt` at the execution's start boundary
  * (derived only from the durable data: the global execution index k
  * starts at committed transition count k − 1) — to exactly one
  * projection matching the generation and iteration this call would
  * close (`generation_index`, `iteration_index`, `stage_id`,
  * `template_id`). Zero and ambiguous resolutions — a reused
  * template with the same iteration index across touching generations —
  * are both a `lifecycle_conflict` with zero dispatch; no
  * first/last/current resolution other than the unique exact one is ever
  * selected, and the membership form of the same shared resolver is
  * deliberately not used here: membership is the restore verifier's
  * tolerance, while a closure mutates a specific generation and demands
  * a unique exact binding. The last durable generation
  * must match the trusted compiled stage's id, declaration position,
  * template and current plan digest. Every index and anchor is derived
  * from the validated durable state. Reconciliation: with the iteration
 * open, the sequence `stage_iteration_closed` (no wait index) and — if
 * the caller asked — `stage_generation_closed` is prepared; with the
 * iteration already closed at this boundary, an exact match of the
 * durable close reason and anchor (plus the generation's state for a
 * requested generation closure) is an idempotent success with zero
 * dispatch, a requested-but-open generation is completed by the single
 * `stage_generation_closed` dispatch, and any other close reason, anchor
 * or a generation closed while the call requested only the iteration
 * closure is a `lifecycle_conflict` with zero dispatch — no candidate
 * heuristic, no historical generation guess. The reducer stays the
 * single successor authority: the whole missing suffix is pre-checked
 * through `reducePipelineV2RunCommand` on a local snapshot before the
 * first dispatch (`invalid_state` with zero dispatch on a rejection),
 * then dispatched strictly iteration close → optional generation close
 * with per-dispatch authoritative verification (a hostile
 * resolve-without-change and a conflicting race are
 * `lifecycle_conflict`; a racing identical dispatch is idempotent
 * success only on the exact durable record). Durability mapping is the
 * same as for the ensure API. Both public APIs share one validation
 * path (single capture, poison latch, compiled-plan gate, state
 * validator, hidden identity, durable bindings, stage position) and one
 * pre-check/dispatch machinery inside this module; there is no second
 * controller, comparator, replay or registry.
 *
 * Not implemented (stays unwired): stage selection/routing policy, the
 * wait-bound `grant`/`replanned` iteration closure, the plan/task
 * replanning controller, grant and effective-budget policy, automatic
 * stage/next-stage selection, transition dispatch, automatic resume,
 * coordinator/runner/CLI wiring, filesystem work, Docker
 * Helper/Sessions, migrations/API/T3 and multi-process locking are later
 * increments. The self-call of these APIs means the future policy layer
 * has already made the corresponding decision.
 */
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  PipelineV2StateError,
  pipelineV2StageIterationAt,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import {
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "./pipeline_v2_run_plan_compiled.ts";
import { compiledRunPlanOriginIdentity } from "./pipeline_v2_run_plan_compiled_internal.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/** The closed reason set of the stage iteration controller's own failures. */
export type PipelineV2StageIterationControllerFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "lifecycle_conflict"
  | "state_persist_failed";

const REASON_SET: ReadonlySet<string> = new Set<string>([
  "invalid_options",
  "invalid_state",
  "lifecycle_conflict",
  "state_persist_failed",
]);

/**
 * A failure of the stage iteration controller layer with its stable
 * machine-readable `reason` and the last authoritative durable state (or
 * `null` when no durable state exists). The reason is assigned where the
 * failing operation's semantics are known (never by classifying message
 * text) and is one of the fixed closed set.
 */
export class PipelineV2StageIterationControllerError extends Error {
  declare readonly reason: PipelineV2StageIterationControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2StageIterationControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 stage iteration controller error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2StageIterationControllerError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
    this.state = state;
  }
}

/**
 * The structural sink the controller writes through: the production
 * `PipelineV2RunStateSink` satisfies it, tests may inject fault-injectable
 * equivalents.
 */
export interface PipelineV2StageIterationControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface EnsurePipelineV2StageIterationOptions {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly initialBudget: number;
  readonly sink: PipelineV2StageIterationControllerSink;
}

export interface EnsuredPipelineV2StageIteration {
  readonly compiled_stage: CompiledPipelineV2RunPlanStage;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly state: PipelineV2RunState;
}

/** The iteration close reasons this controller's active-boundary API accepts. */
export type PipelineV2ActiveStageIterationCloseReason = "normal_close" | "exhausted";

/** The generation close reasons this controller's active-boundary API accepts. */
export type PipelineV2ActiveStageGenerationCloseReason = "next_stage" | "final_stage";

export interface ClosePipelineV2StageIterationOptions {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly iterationCloseReason: PipelineV2ActiveStageIterationCloseReason;
  readonly generationCloseReason?: PipelineV2ActiveStageGenerationCloseReason;
  readonly sink: PipelineV2StageIterationControllerSink;
}

export interface ClosedPipelineV2StageIteration {
  readonly compiled_stage: CompiledPipelineV2RunPlanStage;
  readonly generation_index: number;
  readonly iteration_index: number;
  readonly generation_closed: boolean;
  readonly state: PipelineV2RunState;
}

interface CapturedCommon {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly sink: PipelineV2StageIterationControllerSink;
  readonly dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  readonly snapshot: PipelineV2RunState;
}

interface CapturedEnsure extends CapturedCommon {
  readonly initialBudget: number;
}

interface CapturedClose extends CapturedCommon {
  readonly iterationCloseReason: PipelineV2ActiveStageIterationCloseReason;
  readonly generationCloseReason: PipelineV2ActiveStageGenerationCloseReason | undefined;
}

interface ValidatedContext {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly compiledStage: CompiledPipelineV2RunPlanStage;
  readonly stagePosition: number;
  readonly sink: PipelineV2StageIterationControllerSink;
  readonly dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  readonly snapshot: PipelineV2RunState;
  readonly state: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2StageIterationControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2StageIterationControllerError {
  return new PipelineV2StageIterationControllerError(reason, message, state);
}

/**
 * The capture boundary, shared by both public APIs: every options field
 * and the sink's `poisoned` and `dispatch` members are read exactly once
 * as an opaque reference. The authoritative `snapshot` is read once at
 * capture and is then re-read after every dispatch and on its typed
 * failures — the per-dispatch verification and the durability mapping
 * take their state from the sink's authoritative snapshot, never from a
 * memoized copy. The
 * compiled plan and the state document are not traversed here (their
 * fields are read only after the compiled-plan provenance gate has run);
 * an unexpected error from a sink getter propagates unchanged. No
 * dispatch happens here; every rejection is a typed controller failure.
 */
function captureOptionsRecord(options: unknown, api: string): Record<string, unknown> {
  if (!isRecord(options)) {
    throw controllerError("invalid_options", `${api} requires an options object`, null);
  }
  return options;
}

function captureSink(
  record: Record<string, unknown>,
  api: string,
  verb: string,
): { sink: PipelineV2StageIterationControllerSink; dispatch: (command: PipelineV2RunCommand) => Promise<void>; snapshot: PipelineV2RunState } {
  const sink = record["sink"];
  if (!isRecord(sink)) {
    throw controllerError("invalid_options", `${api} requires a state sink`, null);
  }
  const poisoned: unknown = sink["poisoned"];
  if (poisoned === true) {
    throw controllerError(
      "invalid_state",
      `the run state sink is poisoned by a durability-unknown commit; no stage iteration is ${verb} for this run`,
      null,
    );
  }
  const dispatch = sink["dispatch"];
  if (typeof dispatch !== "function") {
    throw controllerError("invalid_options", `${api} requires a dispatchable state sink`, null);
  }
  const snapshot: unknown = sink["snapshot"];
  if (!isRecord(snapshot)) {
    throw controllerError("invalid_state", "no durable pipeline v2 run state exists yet", null);
  }
  return {
    sink: sink as unknown as PipelineV2StageIterationControllerSink,
    dispatch: (dispatch as (command: PipelineV2RunCommand) => Promise<void>).bind(sink),
    snapshot: snapshot as unknown as PipelineV2RunState,
  };
}

function captureEnsure(options: unknown): CapturedEnsure {
  const record = captureOptionsRecord(options, "ensurePipelineV2StageIteration");
  const compiledPlan = record["compiledPlan"];
  const stageId = record["stageId"];
  if (typeof stageId !== "string") {
    throw controllerError("invalid_options", "ensurePipelineV2StageIteration requires a string stage id", null);
  }
  const initialBudget = record["initialBudget"];
  if (!isPositiveSafeInteger(initialBudget)) {
    throw controllerError(
      "invalid_options",
      "ensurePipelineV2StageIteration requires a positive safe integer initial budget",
      null,
    );
  }
  const captured = captureSink(record, "ensurePipelineV2StageIteration", "ensured");
  return {
    compiledPlan: compiledPlan as CompiledPipelineV2RunPlan,
    stageId,
    initialBudget: initialBudget as number,
    ...captured,
  };
}

function captureClose(options: unknown): CapturedClose {
  const record = captureOptionsRecord(options, "closePipelineV2StageIteration");
  const compiledPlan = record["compiledPlan"];
  const stageId = record["stageId"];
  if (typeof stageId !== "string") {
    throw controllerError("invalid_options", "closePipelineV2StageIteration requires a string stage id", null);
  }
  const iterationCloseReason = record["iterationCloseReason"];
  if (iterationCloseReason !== "normal_close" && iterationCloseReason !== "exhausted") {
    throw controllerError(
      "invalid_options",
      "closePipelineV2StageIteration requires the iteration close reason normal_close or exhausted",
      null,
    );
  }
  const generationCloseReason = record["generationCloseReason"];
  if (
    generationCloseReason !== undefined &&
    generationCloseReason !== "next_stage" &&
    generationCloseReason !== "final_stage"
  ) {
    throw controllerError(
      "invalid_options",
      "closePipelineV2StageIteration requires the generation close reason next_stage or final_stage",
      null,
    );
  }
  const captured = captureSink(record, "closePipelineV2StageIteration", "closed");
  return {
    compiledPlan: compiledPlan as CompiledPipelineV2RunPlan,
    stageId,
    iterationCloseReason: iterationCloseReason as PipelineV2ActiveStageIterationCloseReason,
    generationCloseReason: generationCloseReason as PipelineV2ActiveStageGenerationCloseReason | undefined,
    ...captured,
  };
}

/** The open generation: the last ledger record without a `closed` projection. */
function openGeneration(state: PipelineV2RunState) {
  const last = state.generations[state.generations.length - 1];
  return last !== undefined && last.closed === undefined ? last : undefined;
}

/**
 * One missing durable lifecycle step with the exact structural
 * expectation the post-dispatch verification re-checks.
 */
type MissingStep =
  | {
      kind: "generation";
      generationIndex: number;
      stageId: string;
      stagePosition: number;
      templateId: string;
      planSha256: string;
      initialBudget: number;
      transitionCount: number;
    }
  | { kind: "iteration"; generationIndex: number; iterationIndex: number; transitionCount: number };

function generationCommand(step: MissingStep): PipelineV2RunCommand {
  if (step.kind !== "generation") {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the step is not a generation step");
  }
  return {
    kind: "stage_generation_opened",
    stageId: step.stageId,
    stagePosition: step.stagePosition,
    templateId: step.templateId,
    planSha256: step.planSha256,
    initialBudget: step.initialBudget,
    transitionCount: step.transitionCount,
  };
}

function iterationCommand(step: MissingStep): PipelineV2RunCommand {
  if (step.kind !== "iteration") {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the step is not an iteration step");
  }
  return {
    kind: "stage_iteration_opened",
    generationIndex: step.generationIndex,
    iterationIndex: step.iterationIndex,
    transitionCount: step.transitionCount,
  };
}

function stepCommand(step: MissingStep): PipelineV2RunCommand {
  return step.kind === "generation" ? generationCommand(step) : iterationCommand(step);
}

/**
 * Whether the durable state carries exactly the expected lifecycle record.
 * A generation record's identity fields are compared exactly; its
 * iteration bookkeeping is not pinned (a racing identical dispatch may
 * already have opened the first iteration). An iteration record is exact
 * on index, anchor and open position.
 */
function stepPresent(state: PipelineV2RunState, step: MissingStep): boolean {
  if (step.kind === "generation") {
    const record = state.generations.find((candidate) => candidate.index === step.generationIndex);
    return (
      record !== undefined &&
      record.stage_id === step.stageId &&
      record.stage_position === step.stagePosition &&
      record.template_id === step.templateId &&
      record.plan_sha256 === step.planSha256 &&
      record.initial_budget === step.initialBudget &&
      record.opened_transition_count === step.transitionCount
    );
  }
  const generation = state.generations.find((candidate) => candidate.index === step.generationIndex);
  if (generation === undefined || generation.open_iteration === undefined) {
    return false;
  }
  const iteration = generation.iterations.find((candidate) => candidate.index === step.iterationIndex);
  return (
    iteration !== undefined &&
    iteration.opened_transition_count === step.transitionCount &&
    generation.open_iteration.index === step.iterationIndex
  );
}

/**
 * The active running boundary with no unfinished execution and no open
 * wait — the boundary the reducer's lifecycle commands require. A failed
 * execution is not a clean lifecycle boundary (the only successor of a
 * failure is the run failure finalization), so a settled execution means
 * agent `cleanup_completed` and decision `evaluated` exactly. Every
 * violation is this layer's `invalid_state` before any dispatch.
 */
function checkRunBoundary(state: PipelineV2RunState, boundaryWhat: string): void {
  const fail = (): PipelineV2StageIterationControllerError =>
    controllerError("invalid_state", `the run is not on a boundary that accepts ${boundaryWhat}`, state);
  if (state.status !== "active" || state.phase !== "running") {
    throw fail();
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    throw fail();
  }
  const lastWait = state.waits[state.waits.length - 1];
  if (lastWait !== undefined && lastWait.response === undefined) {
    throw fail();
  }
  const lastExecution = state.executions[state.executions.length - 1];
  if (lastExecution !== undefined) {
    // A failed execution is never a clean lifecycle boundary: after the
    // failure the only durable successor is the run failure finalization,
    // so no stage generation or iteration may be ensured (including the
    // zero-dispatch W3 idempotent path).
    if (lastExecution.type === "agent" && lastExecution.phase !== "cleanup_completed") {
      throw fail();
    }
    if (lastExecution.type === "decision" && lastExecution.phase !== "evaluated") {
      throw fail();
    }
  }
}

/**
 * One pending durable lifecycle step with its exact command and the
 * structural expectation the post-dispatch verification re-checks.
 * Shared by both public APIs; the pre-check and the dispatch loop below
 * are the module's single reducer-pre-check and dispatch machinery.
 */
interface PendingStep {
  readonly command: PipelineV2RunCommand;
  readonly present: (state: PipelineV2RunState) => boolean;
}

function openStep(step: MissingStep): PendingStep {
  const command = stepCommand(step);
  return { command, present: (state) => stepPresent(state, step) };
}

/**
 * The reducer pre-check: the whole pending sequence is applied to a
 * local snapshot through the single reducer before the first dispatch;
 * the authoritative state comes exclusively from the sink.
 */
function precheckSequence(
  state: PipelineV2RunState,
  steps: readonly PendingStep[],
  snapshot: PipelineV2RunState,
): void {
  let checked = state;
  for (const step of steps) {
    try {
      checked = reducePipelineV2RunCommand(checked, step.command, new Date());
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        throw controllerError(
          "invalid_state",
          "the current run state does not accept the stage lifecycle sequence",
          snapshot,
        );
      }
      throw cause;
    }
  }
}

/**
 * The dispatch sequence with per-dispatch authoritative verification:
 * after every dispatch the sink snapshot is re-read and must structurally
 * carry exactly the expected record; a reducer rejection after a racing
 * identical dispatch is idempotent success only on that exact match. No
 * rollback and no automatic second dispatch ever happen.
 */
async function dispatchPrecheckedSequence(
  captured: { sink: PipelineV2StageIterationControllerSink; dispatch: (command: PipelineV2RunCommand) => Promise<void>; snapshot: PipelineV2RunState },
  steps: readonly PendingStep[],
): Promise<PipelineV2RunState> {
  let latest = captured.snapshot;
  for (const step of steps) {
    let confirmed = false;
    try {
      await captured.dispatch(step.command);
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        const after = captured.sink.snapshot;
        if (after !== null && step.present(after)) {
          confirmed = true;
          latest = after;
        } else {
          throw controllerError(
            "lifecycle_conflict",
            "the run state rejected the stage lifecycle step and does not carry it",
            after,
          );
        }
      } else if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError(
          "state_persist_failed",
          "the stage lifecycle step could not be confirmed durable",
          captured.sink.snapshot,
        );
      } else if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          "the stage lifecycle step could not be committed",
          captured.sink.snapshot,
        );
      } else {
        throw cause;
      }
    }
    if (!confirmed) {
      const after = captured.sink.snapshot;
      if (after === null || !step.present(after)) {
        throw controllerError(
          "lifecycle_conflict",
          "the committed run state does not carry the expected stage lifecycle record",
          after,
        );
      }
      latest = after;
    }
  }
  return latest;
}

/**
 * The shared validation path of both public APIs, run after the capture
 * boundary and before any reconciliation: the compiled-plan provenance
 * gate and stage lookup (the single gate and lookup; the compiled-layer
 * errors keep their classes) → the single state validator → the hidden
 * originating-identity comparison through the single existing
 * structural comparator → the durable run-id and last-plan-revision
 * bindings → the stage position derived only from the compiled plan's
 * declaration order. No field of the compiled plan, the stage id or the
 * state document is read before this gate.
 */
function resolveValidatedContext(
  captured: { compiledPlan: CompiledPipelineV2RunPlan; stageId: string; sink: PipelineV2StageIterationControllerSink; dispatch: (command: PipelineV2RunCommand) => Promise<void>; snapshot: PipelineV2RunState },
  api: string,
): ValidatedContext {
  const compiledStage = compiledPipelineV2RunPlanStageFor(captured.compiledPlan, captured.stageId);

  // The single state validator; the caller's object is not trusted beyond
  // it.
  let state: PipelineV2RunState;
  try {
    state = validatePipelineV2RunState(captured.snapshot);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        `${api} requires a durable pipeline v2 run state document`,
        null,
      );
    }
    throw cause;
  }

  // The compiled plan is provenance-bound to the immutable identity
  // snapshot of its originating pipeline; the durable run must carry
  // exactly that identity. The comparison runs only through the single
  // existing structural comparator; a mismatch of any of the five durable
  // fields is a lifecycle conflict with zero dispatch, and the mismatching
  // field name is the only diagnostic detail.
  const comparison = comparePipelineV2RunIdentity(
    compiledRunPlanOriginIdentity(captured.compiledPlan),
    state.pipeline,
  );
  if (comparison.kind !== "match") {
    throw controllerError(
      "lifecycle_conflict",
      `the compiled plan's originating pipeline identity does not match the durable run identity (field ${comparison.field})`,
      captured.snapshot,
    );
  }

  // Durable bindings: the run id and the last durable plan revision must
  // name exactly the compiled plan. The errors carry the authoritative
  // captured snapshot (the validator returns an independent clone used
  // only for the derivation below).
  if (state.run_id !== captured.compiledPlan.run_id) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable run does not belong to the compiled plan's run",
      captured.snapshot,
    );
  }
  const lastPlan = state.plan_revisions[state.plan_revisions.length - 1];
  if (
    lastPlan === undefined ||
    lastPlan.revision !== captured.compiledPlan.plan_revision ||
    lastPlan.sha256 !== captured.compiledPlan.plan_sha256
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the compiled plan is not the last durable plan revision",
      captured.snapshot,
    );
  }

  // The stage position is derived only from the compiled plan's stage
  // declaration order.
  const declarationIndex = captured.compiledPlan.stages.findIndex((stage) => stage.id === compiledStage.id);
  if (declarationIndex < 0) {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the resolved stage is not in the compiled plan");
  }
  return {
    compiledPlan: captured.compiledPlan,
    compiledStage,
    stagePosition: declarationIndex + 1,
    sink: captured.sink,
    dispatch: captured.dispatch,
    snapshot: captured.snapshot,
    state,
  };
}

/**
 * Guarantees an open generation and iteration for the selected compiled
 * stage: capture → poison latch → the compiled-plan stage lookup → the
 * single state validation → the durable bindings → the reconciliation →
 * the reducer pre-check → the dispatch sequence with per-dispatch
 * authoritative verification.
 */
export async function ensurePipelineV2StageIteration(
  options: EnsurePipelineV2StageIterationOptions,
): Promise<EnsuredPipelineV2StageIteration> {
  const captured = captureEnsure(options);
  const ctx = resolveValidatedContext(captured, "ensurePipelineV2StageIteration");

  // The run boundary check before any reconciliation or dispatch.
  checkRunBoundary(ctx.state, "a stage generation or iteration");

  // Reconciliation.
  const cursorCount = ctx.state.cursor.transition_count;
  const open = openGeneration(ctx.state);
  const missing: MissingStep[] = [];
  if (open === undefined) {
    const newGenerationIndex = ctx.state.generations.length + 1;
    missing.push({
      kind: "generation",
      generationIndex: newGenerationIndex,
      stageId: ctx.compiledStage.id,
      stagePosition: ctx.stagePosition,
      templateId: ctx.compiledStage.template,
      planSha256: ctx.compiledPlan.plan_sha256,
      initialBudget: captured.initialBudget,
      transitionCount: cursorCount,
    });
    missing.push({
      kind: "iteration",
      generationIndex: newGenerationIndex,
      iterationIndex: 1,
      transitionCount: cursorCount,
    });
  } else {
    if (
      open.stage_id !== ctx.compiledStage.id ||
      open.stage_position !== ctx.stagePosition ||
      open.template_id !== ctx.compiledStage.template ||
      open.plan_sha256 !== ctx.compiledPlan.plan_sha256 ||
      open.initial_budget !== captured.initialBudget
    ) {
      throw controllerError(
        "lifecycle_conflict",
        `the open stage generation ${open.index} does not match the compiled stage ${JSON.stringify(ctx.compiledStage.id)}`,
        ctx.snapshot,
      );
    }
    if (open.open_iteration !== undefined) {
      // W3: idempotent success without any dispatch; the authoritative
      // captured snapshot is the result state.
      return deepFreezeValue({
        compiled_stage: ctx.compiledStage,
        generation_index: open.index,
        iteration_index: open.open_iteration.index,
        state: ctx.snapshot,
      });
    }
    missing.push({
      kind: "iteration",
      generationIndex: open.index,
      iterationIndex: open.iteration_count + 1,
      transitionCount: cursorCount,
    });
  }

  // The sequence always ends with the iteration step: W1 ends with
  // iteration 1 of the new generation; W2/next-iteration is the single
  // iteration step. The result indexes come from that last step.
  const lastStep = missing[missing.length - 1];
  if (lastStep === undefined || lastStep.kind !== "iteration") {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the missing sequence does not end with an iteration step");
  }
  // Reducer pre-check of the whole missing sequence, then the dispatch
  // sequence with per-dispatch authoritative verification.
  precheckSequence(ctx.state, missing.map(openStep), ctx.snapshot);
  const latest = await dispatchPrecheckedSequence(captured, missing.map(openStep));
  return deepFreezeValue({
    compiled_stage: ctx.compiledStage,
    generation_index: lastStep.generationIndex,
    iteration_index: lastStep.iterationIndex,
    state: latest,
  });
}

/**
 * Whether the durable state carries exactly the expected iteration
 * closure record: the generation's iteration record closed with the
 * exact reason, no wait index and the exact anchor. The generation's
 * iteration bookkeeping (the open projection and possible later
 * iterations) is not pinned — a racing identical dispatch may already
 * have opened the next iteration.
 */
function iterationClosePresent(
  state: PipelineV2RunState,
  generationIndex: number,
  iterationIndex: number,
  by: PipelineV2ActiveStageIterationCloseReason,
  transitionCount: number,
): boolean {
  const generation = state.generations.find((candidate) => candidate.index === generationIndex);
  if (generation === undefined) {
    return false;
  }
  const iteration = generation.iterations.find((candidate) => candidate.index === iterationIndex);
  return (
    iteration !== undefined &&
    iteration.closed !== undefined &&
    iteration.closed.by === by &&
    iteration.closed.wait_index === undefined &&
    iteration.closed.closed_transition_count === transitionCount
  );
}

/** Whether the durable state carries exactly the expected generation closure record. */
function generationClosePresent(
  state: PipelineV2RunState,
  generationIndex: number,
  by: PipelineV2ActiveStageGenerationCloseReason,
  transitionCount: number,
): boolean {
  const generation = state.generations.find((candidate) => candidate.index === generationIndex);
  return (
    generation !== undefined &&
    generation.closed !== undefined &&
    generation.closed.by === by &&
    generation.closed.closed_transition_count === transitionCount
  );
}

function iterationCloseStep(
  generationIndex: number,
  iterationIndex: number,
  by: PipelineV2ActiveStageIterationCloseReason,
  transitionCount: number,
): PendingStep {
  return {
    command: { kind: "stage_iteration_closed", generationIndex, iterationIndex, by },
    present: (state) => iterationClosePresent(state, generationIndex, iterationIndex, by, transitionCount),
  };
}

function generationCloseStep(
  generationIndex: number,
  by: PipelineV2ActiveStageGenerationCloseReason,
  transitionCount: number,
): PendingStep {
  return {
    command: { kind: "stage_generation_closed", generationIndex, by },
    present: (state) => generationClosePresent(state, generationIndex, by, transitionCount),
  };
}

/**
 * The closure boundary on top of the run boundary: exactly one
 * settled-but-unbound execution whose durable role is exactly `stage`
 * (never inferred from the profile, the executor name or the state id)
 * and whose state belongs to the selected compiled stage. The settled
 * phase itself (agent `cleanup_completed`, decision `evaluated`) and the
 * failed/in-flight rejection are `checkRunBoundary`'s. Every violation
 * is `invalid_state` before any dispatch.
 */
function checkClosureBoundary(
  state: PipelineV2RunState,
  compiledStage: CompiledPipelineV2RunPlanStage,
): void {
  const fail = (): PipelineV2StageIterationControllerError =>
    controllerError("invalid_state", "the run is not on a boundary that accepts a stage iteration closure", state);
  if (state.executions.length !== state.transitions.length + 1) {
    throw fail();
  }
  const lastExecution = state.executions[state.executions.length - 1];
  if (lastExecution === undefined || lastExecution.execution_role !== "stage") {
    throw fail();
  }
  if (!compiledStage.state_ids.includes(lastExecution.state_id)) {
    throw fail();
  }
}

/**
 * Closes the active stage iteration of the selected compiled stage and —
 * by the caller's explicit decision — its generation, on the contract
 * hook boundary `settled stage execution → stage_iteration_closed →
 * optional stage_generation_closed → transition_committed`. The caller
 * has already made every policy decision (the iteration close reason,
 * whether and why the generation closes); the controller derives every
 * durable binding, index and anchor from the trusted compiled projection
 * and the authoritative durable state, and accepts no index, anchor,
 * position, template, plan digest or target state from the caller.
 */
export async function closePipelineV2StageIteration(
  options: ClosePipelineV2StageIterationOptions,
): Promise<ClosedPipelineV2StageIteration> {
  const captured = captureClose(options);
  const ctx = resolveValidatedContext(captured, "closePipelineV2StageIteration");

  // The closure boundary: the run boundary plus the settled-but-unbound
  // stage execution of this compiled stage.
  checkRunBoundary(ctx.state, "a stage iteration closure");
  checkClosureBoundary(ctx.state, ctx.compiledStage);
  const lastExecution = ctx.state.executions[ctx.state.executions.length - 1]!;

  // Reconciliation against the last durable generation: every index and
  // anchor comes from the validated durable state, never from the caller.
  const generation = ctx.state.generations[ctx.state.generations.length - 1];
  if (generation === undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the settled stage execution ${lastExecution.index} has no durable stage generation`,
      ctx.snapshot,
    );
  }
  if (
    generation.stage_id !== ctx.compiledStage.id ||
    generation.stage_position !== ctx.stagePosition ||
    generation.template_id !== ctx.compiledStage.template ||
    generation.plan_sha256 !== ctx.compiledPlan.plan_sha256
  ) {
    throw controllerError(
      "lifecycle_conflict",
      `the last stage generation ${generation.index} does not match the compiled stage ${JSON.stringify(ctx.compiledStage.id)}`,
      ctx.snapshot,
    );
  }
  const lastIteration = generation.iterations[generation.iterations.length - 1];
  if (lastIteration === undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the stage generation ${generation.index} carries no iteration record`,
      ctx.snapshot,
    );
  }
  const executionIterationIndex = lastExecution.iteration_index;
  if (executionIterationIndex === undefined) {
    throw controllerError(
      "lifecycle_conflict",
      `the settled stage execution ${lastExecution.index} records no stage iteration index`,
      ctx.snapshot,
    );
  }
  // The exact execution → generation binding: the execution's start
  // boundary is derived only from the durable data (the global execution
  // index k starts at committed transition count k − 1), and the single
  // existing exact resolver must return exactly one projection that
  // matches the generation and iteration this call would close. Zero and
  // ambiguous resolutions — e.g. a reused template with the same
  // iteration index across touching generations — are both a lifecycle
  // conflict; no first/last/current resolution is ever selected, and the
  // membership form of the same shared resolver is deliberately not used
  // here because a closure mutates a specific generation and demands a
  // unique exact binding.
  const resolvedIteration = pipelineV2StageIterationAt(
    ctx.state,
    lastExecution.index - 1,
    executionIterationIndex,
    ctx.compiledStage.template,
  );
  if (
    resolvedIteration === null ||
    resolvedIteration.generation_index !== generation.index ||
    resolvedIteration.iteration_index !== lastIteration.index ||
    resolvedIteration.stage_id !== generation.stage_id ||
    resolvedIteration.template_id !== generation.template_id
  ) {
    throw controllerError(
      "lifecycle_conflict",
      resolvedIteration === null
        ? `the settled stage execution ${lastExecution.index} does not resolve to exactly one stage iteration of template ${JSON.stringify(ctx.compiledStage.template)} at its start boundary`
        : `the settled stage execution ${lastExecution.index} resolves to stage iteration ${resolvedIteration.iteration_index} of generation ${resolvedIteration.generation_index}, not the stage iteration ${lastIteration.index} of generation ${generation.index} this call would close`,
      ctx.snapshot,
    );
  }

  const cursorCount = ctx.state.cursor.transition_count;
  const steps: PendingStep[] = [];
  let generationClosed: boolean;
  if (lastIteration.closed === undefined) {
    // Open iteration: the iteration close and — if the caller asked — the
    // generation close, strictly in this order.
    steps.push(iterationCloseStep(generation.index, lastIteration.index, captured.iterationCloseReason, cursorCount));
    if (captured.generationCloseReason !== undefined) {
      steps.push(generationCloseStep(generation.index, captured.generationCloseReason, cursorCount));
    }
    generationClosed = captured.generationCloseReason !== undefined;
  } else {
    // Partial retry: the iteration is already closed at this boundary.
    // The durable close reason and anchor must match the call exactly.
    if (lastIteration.closed.by !== captured.iterationCloseReason) {
      throw controllerError(
        "lifecycle_conflict",
        `the stage iteration ${lastIteration.index} of generation ${generation.index} is already closed with reason ${JSON.stringify(lastIteration.closed.by)}`,
        ctx.snapshot,
      );
    }
    if (lastIteration.closed.closed_transition_count !== cursorCount) {
      throw controllerError(
        "lifecycle_conflict",
        `the stage iteration ${lastIteration.index} of generation ${generation.index} is closed at transition count ${lastIteration.closed.closed_transition_count}, but the run cursor is ${cursorCount}`,
        ctx.snapshot,
      );
    }
    if (captured.generationCloseReason === undefined) {
      if (generation.closed !== undefined) {
        throw controllerError(
          "lifecycle_conflict",
          `the stage generation ${generation.index} is already closed, but the call requested only the iteration closure`,
          ctx.snapshot,
        );
      }
      generationClosed = false;
    } else if (generation.closed === undefined) {
      steps.push(generationCloseStep(generation.index, captured.generationCloseReason, cursorCount));
      generationClosed = true;
    } else {
      if (generation.closed.by !== captured.generationCloseReason) {
        throw controllerError(
          "lifecycle_conflict",
          `the stage generation ${generation.index} is already closed with reason ${JSON.stringify(generation.closed.by)}`,
          ctx.snapshot,
        );
      }
      if (generation.closed.closed_transition_count !== cursorCount) {
        throw controllerError(
          "lifecycle_conflict",
          `the stage generation ${generation.index} is closed at transition count ${generation.closed.closed_transition_count}, but the run cursor is ${cursorCount}`,
          ctx.snapshot,
        );
      }
      generationClosed = true;
    }
  }

  // Reducer pre-check of the whole missing suffix, then the dispatch
  // sequence with per-dispatch authoritative verification.
  precheckSequence(ctx.state, steps, ctx.snapshot);
  const latest = await dispatchPrecheckedSequence(captured, steps);
  return deepFreezeValue({
    compiled_stage: ctx.compiledStage,
    generation_index: generation.index,
    iteration_index: lastIteration.index,
    generation_closed: generationClosed,
    state: latest,
  });
}
