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
 * Not implemented (stays unwired): stage selection/routing policy, the
 * closure of iterations and generations, the wait/replanning/grant
 * controllers, automatic resume, coordinator/runner/CLI wiring,
 * filesystem work, Docker Helper/Sessions, migrations/API/T3 and
 * multi-process locking are later increments. The self-call of this API
 * means the future policy layer has already chosen to continue the
 * stage.
 */
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  PipelineV2StateError,
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

interface Captured {
  readonly compiledPlan: CompiledPipelineV2RunPlan;
  readonly stageId: string;
  readonly initialBudget: number;
  readonly sink: PipelineV2StageIterationControllerSink;
  readonly dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  readonly snapshot: PipelineV2RunState;
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
 * The synchronous capture boundary: every options field and every sink
 * member is read exactly once as an opaque reference. The compiled plan
 * and the state document are not traversed here (their fields are read
 * only after the compiled-plan provenance gate has run); an unexpected
 * error from a sink getter propagates unchanged. No dispatch happens
 * here; every rejection is a typed controller failure.
 */
function captureBoundary(options: unknown): Captured {
  if (!isRecord(options)) {
    throw controllerError("invalid_options", "ensurePipelineV2StageIteration requires an options object", null);
  }
  const compiledPlan = options["compiledPlan"];
  const stageId = options["stageId"];
  const initialBudget = options["initialBudget"];
  const sink = options["sink"];
  if (typeof stageId !== "string") {
    throw controllerError("invalid_options", "ensurePipelineV2StageIteration requires a string stage id", null);
  }
  if (!isPositiveSafeInteger(initialBudget)) {
    throw controllerError(
      "invalid_options",
      "ensurePipelineV2StageIteration requires a positive safe integer initial budget",
      null,
    );
  }
  if (!isRecord(sink)) {
    throw controllerError("invalid_options", "ensurePipelineV2StageIteration requires a state sink", null);
  }
  const poisoned: unknown = sink["poisoned"];
  if (poisoned === true) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned by a durability-unknown commit; no stage iteration is ensured for this run",
      null,
    );
  }
  const dispatch = sink["dispatch"];
  if (typeof dispatch !== "function") {
    throw controllerError("invalid_options", "ensurePipelineV2StageIteration requires a dispatchable state sink", null);
  }
  const snapshot: unknown = sink["snapshot"];
  if (!isRecord(snapshot)) {
    throw controllerError("invalid_state", "no durable pipeline v2 run state exists yet", null);
  }
  return {
    compiledPlan: compiledPlan as CompiledPipelineV2RunPlan,
    stageId,
    initialBudget: initialBudget as number,
    sink: sink as unknown as PipelineV2StageIterationControllerSink,
    dispatch: (dispatch as (command: PipelineV2RunCommand) => Promise<void>).bind(sink),
    snapshot: snapshot as unknown as PipelineV2RunState,
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
function checkRunBoundary(state: PipelineV2RunState): void {
  const fail = (): PipelineV2StageIterationControllerError =>
    controllerError(
      "invalid_state",
      "the run is not on a boundary that accepts a stage generation or iteration",
      state,
    );
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
 * Guarantees an open generation and iteration for the selected compiled
 * stage: capture → poison latch → the compiled-plan stage lookup → the
 * single state validation → the durable bindings → the reconciliation →
 * the reducer pre-check → the dispatch sequence with per-dispatch
 * authoritative verification.
 */
export async function ensurePipelineV2StageIteration(
  options: EnsurePipelineV2StageIterationOptions,
): Promise<EnsuredPipelineV2StageIteration> {
  const ctx = captureBoundary(options);

  // The compiled-plan provenance gate and stage lookup — the single gate
  // and lookup; the compiled-layer errors keep their classes. No field of
  // the compiled plan, the stage id or the state document is read before
  // this gate.
  const compiledStage = compiledPipelineV2RunPlanStageFor(ctx.compiledPlan, ctx.stageId);

  // The single state validator; the caller's object is not trusted beyond
  // it.
  let state: PipelineV2RunState;
  try {
    state = validatePipelineV2RunState(ctx.snapshot);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw controllerError(
        "invalid_state",
        "ensurePipelineV2StageIteration requires a durable pipeline v2 run state document",
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
    compiledRunPlanOriginIdentity(ctx.compiledPlan),
    state.pipeline,
  );
  if (comparison.kind !== "match") {
    throw controllerError(
      "lifecycle_conflict",
      `the compiled plan's originating pipeline identity does not match the durable run identity (field ${comparison.field})`,
      ctx.snapshot,
    );
  }

  // Durable bindings: the run id and the last durable plan revision must
  // name exactly the compiled plan. The errors carry the authoritative
  // captured snapshot (the validator returns an independent clone used
  // only for the derivation below).
  if (state.run_id !== ctx.compiledPlan.run_id) {
    throw controllerError(
      "lifecycle_conflict",
      "the durable run does not belong to the compiled plan's run",
      ctx.snapshot,
    );
  }
  const lastPlan = state.plan_revisions[state.plan_revisions.length - 1];
  if (
    lastPlan === undefined ||
    lastPlan.revision !== ctx.compiledPlan.plan_revision ||
    lastPlan.sha256 !== ctx.compiledPlan.plan_sha256
  ) {
    throw controllerError(
      "lifecycle_conflict",
      "the compiled plan is not the last durable plan revision",
      ctx.snapshot,
    );
  }

  // The stage position is derived only from the compiled plan's stage
  // declaration order.
  const declarationIndex = ctx.compiledPlan.stages.findIndex((stage) => stage.id === compiledStage.id);
  if (declarationIndex < 0) {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the resolved stage is not in the compiled plan");
  }
  const stagePosition = declarationIndex + 1;

  // The run boundary check before any reconciliation or dispatch.
  checkRunBoundary(state);

  // Reconciliation.
  const cursorCount = state.cursor.transition_count;
  const open = openGeneration(state);
  const missing: MissingStep[] = [];
  if (open === undefined) {
    const newGenerationIndex = state.generations.length + 1;
    missing.push({
      kind: "generation",
      generationIndex: newGenerationIndex,
      stageId: compiledStage.id,
      stagePosition,
      templateId: compiledStage.template,
      planSha256: ctx.compiledPlan.plan_sha256,
      initialBudget: ctx.initialBudget,
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
      open.stage_id !== compiledStage.id ||
      open.stage_position !== stagePosition ||
      open.template_id !== compiledStage.template ||
      open.plan_sha256 !== ctx.compiledPlan.plan_sha256 ||
      open.initial_budget !== ctx.initialBudget
    ) {
      throw controllerError(
        "lifecycle_conflict",
        `the open stage generation ${open.index} does not match the compiled stage ${JSON.stringify(compiledStage.id)}`,
        ctx.snapshot,
      );
    }
    if (open.open_iteration !== undefined) {
      // W3: idempotent success without any dispatch; the authoritative
      // captured snapshot is the result state.
      return deepFreezeValue({
        compiled_stage: compiledStage,
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

  // Reducer pre-check: the whole missing sequence is applied to a local
  // snapshot through the single reducer before the first dispatch; the
  // authoritative state comes exclusively from the sink.
  let checked = state;
  for (const step of missing) {
    const command = stepCommand(step);
    try {
      checked = reducePipelineV2RunCommand(checked, command, new Date());
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        throw controllerError(
          "invalid_state",
          "the current run state does not accept the stage lifecycle sequence",
          ctx.snapshot,
        );
      }
      throw cause;
    }
  }

  // Dispatch strictly generation → iteration, with per-dispatch
  // authoritative verification.
  let latest = ctx.snapshot;
  for (const step of missing) {
    const command = stepCommand(step);
    let confirmed = false;
    try {
      await ctx.dispatch(command);
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        const after = ctx.sink.snapshot;
        if (after !== null && stepPresent(after, step)) {
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
          ctx.sink.snapshot,
        );
      } else if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          "the stage lifecycle step could not be committed",
          ctx.sink.snapshot,
        );
      } else {
        throw cause;
      }
    }
    if (!confirmed) {
      const after = ctx.sink.snapshot;
      if (after === null || !stepPresent(after, step)) {
        throw controllerError(
          "lifecycle_conflict",
          "the committed run state does not carry the expected stage lifecycle record",
          after,
        );
      }
      latest = after;
    }
  }

  // The sequence always ends with the iteration step: W1 ends with
  // iteration 1 of the new generation; W2/next-iteration is the single
  // iteration step. The result indexes come from that last step.
  const lastStep = missing[missing.length - 1];
  if (lastStep === undefined || lastStep.kind !== "iteration") {
    throw new Error("pipeline v2 stage iteration controller invariant violated: the missing sequence does not end with an iteration step");
  }
  return deepFreezeValue({
    compiled_stage: compiledStage,
    generation_index: lastStep.generationIndex,
    iteration_index: lastStep.iterationIndex,
    state: latest,
  });
}
