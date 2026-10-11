import { lstat, realpath } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import { comparePipelineV2RunIdentity } from "./pipeline_v2_identity_compare.ts";
import {
  classifyPipelineV2ResumeRecovery,
} from "./pipeline_v2_recovery.ts";
import {
  restorePipelineV2PlanningAcceptanceContext,
  restorePipelineV2RuntimeContext,
  PipelineV2RuntimeContextRestoreError,
} from "./pipeline_v2_resume_context.ts";
import { pipelineV2PlanReadyPolicyFor } from "./pipeline_v2_plan_ready_auto_controller.ts";
import {
  expectSafeId,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { PipelineV2RunStateStore } from "./pipeline_v2_state_store.ts";
import {
  verifyPublishedRunOutputs,
  verifyPublishedRunOutputsAgainstRecord,
  PipelineV2RunOutputTreeDefectError,
  type PipelineV2PublishedRunOutputRecord,
} from "./pipeline_v2_runtime.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/**
 * The versioned, read-only external projection of one durable pipeline v2
 * run: `orchestrator inspect`.
 *
 * The whole path is strictly read-only and reuses only proven machinery:
 * the run state is loaded through the read-only snapshot store
 * (`PipelineV2RunStateStore.load()` — never a mutable sink, never a
 * dispatch, never an adoption), the pipeline is reloaded from the durable
 * bundle root and verified against the durable identity, and the boundary
 * classification is the single authoritative one — the recovery
 * classifier `classifyPipelineV2ResumeRecovery` for every post-commit
 * crash window and the authoritative planning/runtime restores for the
 * boundaries they own. There is no second classifier, no catch-based
 * probing, no worker/runtime/Launcher/Docker Helper involvement and no
 * durable change on any path: a missing run is never created, a damaged
 * run is never repaired, and a repeated inspect over an unchanged run
 * returns byte-identical JSON.
 *
 * The projection is deliberately NOT the durable schema-v7 document: the
 * internal state is the orchestrator's, not an external integration
 * contract. The inspect document is the stable, versioned surface
 * (`schemaVersion: 1`) with exact own-key shapes and content-free values.
 */

/** The closed inspect failure vocabulary before the run root is verified. */
export type PipelineV2InspectPreRootReason =
  | "run_not_found"
  | "invalid_options"
  | "internal_error";

/** The closed inspect failure vocabulary once the run root is verified. */
export type PipelineV2InspectPostRootReason =
  | "run_state_invalid"
  | "pipeline_mismatch"
  | "artifact_missing"
  | "artifact_modified"
  | "internal_error";

/** The complete closed inspect failure vocabulary. */
export type PipelineV2InspectErrorReason =
  | PipelineV2InspectPreRootReason
  | PipelineV2InspectPostRootReason;

/** One stable run-output projection entry (exact own-key shapes). */
export type PipelineV2InspectOutput =
  | { readonly id: string; readonly type: string; readonly required: boolean; readonly present: true; readonly digest: string }
  | { readonly id: string; readonly type: string; readonly required: boolean; readonly present: false };

/**
 * The closed boundary union. `resumable` covers the ordinary runtime
 * boundary AND every recovery window (settled transition, run
 * finalization, terminal lifecycle): the one safe external action is
 * always `orchestrator resume`, so the internal recovery subtype is never
 * exported. `plan_ready` distinguishes only the operator-visible
 * continuation (`resume` for the trusted automatic `plan_ready` policy,
 * `resume_plan` for the manual contract), derived exclusively from the
 * provenance-verified compiled planning role. `in_flight` is a genuinely
 * unfinished execution — a settled execution awaiting its recovery suffix
 * is never in-flight.
 */
export type PipelineV2InspectBoundary =
  | { readonly kind: "plan_ready"; readonly planningExecutionIndex: number; readonly continuation: "resume" | "resume_plan" }
  | { readonly kind: "waiting"; readonly waitIndex: number; readonly reason: string; readonly actions: readonly { readonly id: string; readonly to: string }[] }
  | { readonly kind: "resumable"; readonly stateId: string; readonly transitionCount: number }
  | { readonly kind: "in_flight"; readonly executionIndex: number; readonly stateId: string; readonly executionRole: "planning" | "stage" | "control"; readonly phase: string }
  | { readonly kind: "succeeded"; readonly terminalStateId: string }
  | { readonly kind: "failed"; readonly reason: string };

/** The successful versioned inspect document (exact top-level keys). */
export interface PipelineV2InspectReport {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly runRoot: string;
  readonly revision: number;
  readonly status: "running" | "waiting" | "succeeded" | "failed";
  readonly boundary: PipelineV2InspectBoundary;
  readonly outputs: readonly PipelineV2InspectOutput[];
}

/** The inspect failure document (exact top-level keys; exitCode always 1). */
export interface PipelineV2InspectFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly runRoot: string | null;
  readonly reason: PipelineV2InspectErrorReason;
}

/** The inspect result union: a report or exactly one typed failure. */
export type PipelineV2InspectOutcome =
  | PipelineV2InspectReport
  | PipelineV2InspectFailure;

export interface PipelineV2InspectOptions {
  /** The run id (the shared safe-id grammar; never echoed when invalid). */
  readonly runId: string;
  /** The local orchestrator state root (absolute; read-only access). */
  readonly stateRoot: string;
}

function failure(
  runId: string,
  runRoot: string | null,
  reason: PipelineV2InspectErrorReason,
): PipelineV2InspectFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, runRoot, reason });
}

/** The stable external run status of one durable status value. */
function externalStatus(status: PipelineV2RunState["status"]): "running" | "waiting" | "succeeded" | "failed" {
  switch (status) {
    case "active":
      return "running";
    case "waiting":
      return "waiting";
    case "success":
      return "succeeded";
    case "failed":
    case "cleanup_failed":
      return "failed";
  }
}

/** The exact run-output projection of one durable run_outputs record. */
function projectOutputs(state: PipelineV2RunState): PipelineV2InspectOutput[] {
  const outputs: PipelineV2InspectOutput[] = [];
  for (const output of state.run_outputs ?? []) {
    if (output.present) {
      outputs.push({ id: output.id, type: output.type, required: output.required, present: true, digest: output.digest });
    } else {
      outputs.push({ id: output.id, type: output.type, required: output.required, present: false });
    }
  }
  return outputs;
}/**
 * The planning-acceptance candidate pre-selection: the exact structural
 * shape the authoritative planning restore owns (a settled-but-unbound
 * planning agent execution on the cursor). The restore after this
 * pre-selection is the authoritative verification; its failure is an
 * inspect failure, never a branch change.
 */
function isPlanningAcceptanceCandidate(state: PipelineV2RunState): boolean {
  if (state.status !== "active" || state.phase !== "running") {
    return false;
  }
  if (state.terminal !== undefined || state.run_outputs !== undefined || state.failure !== undefined) {
    return false;
  }
  if (state.executions.length !== state.transitions.length + 1) {
    return false;
  }
  const last = state.executions[state.executions.length - 1];
  return (
    last !== undefined &&
    last.type === "agent" &&
    last.phase === "cleanup_completed" &&
    last.execution_role === "planning" &&
    last.iteration_index === undefined &&
    last.state_id === state.cursor.current_state
  );
}

/** A genuinely unfinished execution (never a settled recovery window). */
function isInFlightExecution(execution: PipelineV2RunState["executions"][number]): boolean {
  if (execution.type === "agent") {
    return execution.phase !== "cleanup_completed" && execution.phase !== "failed";
  }
  return execution.phase !== "evaluated" && execution.phase !== "failed";
}

/** Maps one restore failure reason onto the closed inspect vocabulary. */
function mapRestoreReason(
  reason: PipelineV2RuntimeContextRestoreError["reason"],
): PipelineV2InspectPostRootReason {
  switch (reason) {
    case "invalid_state":
      return "run_state_invalid";
    case "pipeline_mismatch":
      return "pipeline_mismatch";
    case "run_layout_invalid":
      return "artifact_missing";
    case "run_input_modified":
    case "accepted_output_modified":
      return "artifact_modified";
  }
}

/**
 * Maps one published-tree verification defect onto the closed inspect
 * vocabulary: a missing durably-present artifact is `artifact_missing`,
 * every modification, substitution, extra entry, digest or schema
 * mismatch is `artifact_modified`. The verifier's own typed defect is the
 * only input — never its message text.
 */
function mapPublishedTreeDefect(
  cause: unknown,
): PipelineV2InspectPostRootReason | null {
  if (!(cause instanceof PipelineV2RunOutputTreeDefectError)) {
    return null;
  }
  return cause.defect === "missing" ? "artifact_missing" : "artifact_modified";
}

/**
 * The read-only published-output verification of one finalized run: a
 * durable `run_outputs` record obliges the published tree to exist and
 * match the record exactly (the success boundary always carries the
 * record; a failed run without one never published and is not required
 * to). Nothing is created, moved or removed; the returned reason is
 * the inspect's own closed vocabulary, `null` on a verified boundary.
 */
async function verifyFinalizedRunOutputs(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
  runRoot: string,
): Promise<PipelineV2InspectPostRootReason | null> {
  const record = state.run_outputs;
  if (record === undefined) {
    // The loader guarantees the publication record on the success
    // boundary; a missing record there is a damaged run, not a clean one.
    return state.status === "success" ? "artifact_missing" : null;
  }
  const outputsPath = join(runRoot, "outputs");
  let info;
  try {
    info = await lstat(outputsPath);
  } catch {
    return "artifact_missing";
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    return "artifact_modified";
  }
  try {
    await verifyPublishedRunOutputsAgainstRecord(
      pipeline,
      outputsPath,
      record as readonly PipelineV2PublishedRunOutputRecord[],
    );
  } catch (cause) {
    return mapPublishedTreeDefect(cause) ?? "internal_error";
  }
  return null;
}

/**
 * The read-only published-output verification of one terminal recovery
 * window. The publication phase is distinguished from the expectation:
 * before the publication has begun (no durable record, no tree) the
 * boundary is the ordinary resumable terminal chain; an existing tree
 * without the durable record is the T2 orphan and must be exactly the
 * publication the trusted sources determine; a durable record obliges
 * the tree to exist and match it exactly.
 */
async function verifyTerminalPublication(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
  runRoot: string,
  runInputs: unknown,
  acceptedOutputs: readonly unknown[],
): Promise<PipelineV2InspectPostRootReason | null> {
  const outputsPath = join(runRoot, "outputs");
  let info;
  try {
    info = await lstat(outputsPath);
  } catch {
    info = null;
  }
  if (state.run_outputs === undefined) {
    if (info === null) {
      // The publication has not begun: the ordinary resumable terminal
      // chain (the recovery records and publishes it at resume).
      return null;
    }
    if (info.isSymbolicLink() || !info.isDirectory()) {
      return "artifact_modified";
    }
    // The T2 orphan: the tree exists before its durable record; it must
    // be exactly the publication the trusted sources determine.
    try {
      await verifyPublishedRunOutputs(
        pipeline,
        runInputs as Parameters<typeof verifyPublishedRunOutputs>[1],
        acceptedOutputs,
      );
    } catch (cause) {
      return mapPublishedTreeDefect(cause) ?? "internal_error";
    }
    return null;
  }
  if (info === null) {
    return "artifact_missing";
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    return "artifact_modified";
  }
  try {
    await verifyPublishedRunOutputsAgainstRecord(
      pipeline,
      outputsPath,
      state.run_outputs as readonly PipelineV2PublishedRunOutputRecord[],
    );
  } catch (cause) {
    return mapPublishedTreeDefect(cause) ?? "internal_error";
  }
  return null;
}

/**
 * Inspects one durable pipeline v2 run and returns the versioned external
 * projection, or exactly one typed failure. Every path is read-only: the
 * run state is loaded through the read-only snapshot store, the pipeline
 * through the trusted loader, and the boundaries through the single
 * authoritative classifiers/restores. No sink, no dispatch, no publisher,
 * no runtime adapter, no Launcher credential, no Docker Helper subprocess,
 * no session, no filesystem repair.
 */
export async function inspectPipelineV2Run(
  options: PipelineV2InspectOptions,
): Promise<PipelineV2InspectOutcome> {
  // 1. The option capture: exactly one read per field, validated before
  //    any path is built. An invalid run id is never echoed.
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  if (typeof runId !== "string") {
    return failure("", null, "invalid_options");
  }
  try {
    expectSafeId(runId, "pipeline v2 inspect run id");
  } catch {
    return failure("", null, "invalid_options");
  }
  if (typeof stateRoot !== "string" || stateRoot === "" || !isAbsolute(stateRoot)) {
    return failure(runId, null, "invalid_options");
  }

  // 2. The run-root determination is a read-only existence check: a
  //    missing run is reported as `run_not_found` (runRoot null — the
  //    command contract pins the pre-root shape) and never created. The
  //    canonical path is what the authoritative restores require.
  const runRootCandidate = `${stateRoot.replace(/\/+$/, "")}/pipeline-runs/${runId}`;
  let runRootInfo;
  try {
    runRootInfo = await lstat(runRootCandidate);
  } catch {
    return failure(runId, null, "run_not_found");
  }
  if (!runRootInfo.isDirectory() || runRootInfo.isSymbolicLink()) {
    return failure(runId, null, "run_not_found");
  }
  let runRoot: string;
  try {
    runRoot = await realpath(runRootCandidate);
  } catch {
    return failure(runId, runRootCandidate, "run_state_invalid");
  }

  // 3. The read-only snapshot load through the proven store: a missing or
  //    damaged state document is a typed inspect failure, never a repair.
  const store = new PipelineV2RunStateStore({ stateRoot, runId });
  let state: PipelineV2RunState;
  try {
    const loaded = await store.load();
    if (loaded === null) {
      return failure(runId, runRoot, "run_state_invalid");
    }
    state = loaded;
  } catch {
    return failure(runId, runRoot, "run_state_invalid");
  }

  try {
    // 4. The pipeline is reloaded from the durable bundle root and
    //    verified against the durable identity; a broken or foreign
    //    bundle is a pipeline mismatch, never a mutation.
    const pipeline = await loadPipelineV2(state.pipeline.bundle_root);
    const comparison = comparePipelineV2RunIdentity(
      pipelineV2RunPipelineIdentity(pipeline),
      state.pipeline,
    );
    if (comparison.kind === "mismatch") {
      return failure(runId, runRoot, "pipeline_mismatch");
    }
    const boundary = await classifyInspectBoundary(pipeline, state, runRoot);
    if (typeof boundary === "string") {
      return failure(runId, runRoot, boundary);
    }
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId,
      runRoot,
      revision: state.revision,
      status: externalStatus(state.status),
      boundary,
      outputs: projectOutputs(state),
    });
  } catch (cause) {
    if (cause instanceof PipelineV2InspectTypedError) {
      return failure(runId, runRoot, cause.reason);
    }
    return failure(runId, runRoot, "internal_error");
  }
}

/** The internal typed carry for one classified inspect failure reason. */
class PipelineV2InspectTypedError extends Error {
  readonly reason: PipelineV2InspectPostRootReason;
  constructor(reason: PipelineV2InspectPostRootReason) {
    super(`pipeline v2 inspect failure: ${reason}`);
    this.name = "PipelineV2InspectTypedError";
    this.reason = reason;
  }
}

/**
 * The total external boundary classification of one loader-validated
 * durable state against its verified pipeline. The order is fixed:
 * finalized runs, waiting runs, the planning-acceptance candidate (the
 * authoritative planning restore verifies it), the single recovery
 * classifier, genuinely in-flight executions, and the ordinary runtime
 * boundary (the authoritative runtime restore verifies it). Every
 * loader-valid shape lands in exactly one slot; a residual shape is
 * impossible and fails closed as an internal error rather than a new
 * boundary kind.
 */
async function classifyInspectBoundary(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
  runRoot: string,
): Promise<PipelineV2InspectBoundary | PipelineV2InspectPostRootReason> {
  // 1. Finalized runs inspect successfully with their terminal boundary —
  //    but only when the published outputs they claim are intact: a
  //    durable `run_outputs` record obliges the published tree to exist
  //    and match it exactly (a failed run without a publication record
  //    never published and is not required to).
  if (state.status === "success") {
    if (state.terminal === undefined) {
      return "run_state_invalid";
    }
    const outputsReason = await verifyFinalizedRunOutputs(pipeline, state, runRoot);
    if (outputsReason !== null) {
      return outputsReason;
    }
    return deepFreezeValue({ kind: "succeeded" as const, terminalStateId: state.terminal.state_id });
  }
  if (state.status === "failed" || state.status === "cleanup_failed") {
    if (state.failure === undefined) {
      return "run_state_invalid";
    }
    if (state.run_outputs !== undefined) {
      const outputsReason = await verifyFinalizedRunOutputs(pipeline, state, runRoot);
      if (outputsReason !== null) {
        return outputsReason;
      }
    }
    return deepFreezeValue({ kind: "failed" as const, reason: state.failure.reason });
  }

  // 2. A waiting run exposes its open wait record verbatim.
  if (state.status === "waiting") {
    const lastWait = state.waits[state.waits.length - 1];
    if (lastWait === undefined || lastWait.response !== undefined) {
      return "run_state_invalid";
    }
    return deepFreezeValue({
      kind: "waiting" as const,
      waitIndex: lastWait.index,
      reason: lastWait.reason,
      actions: lastWait.actions.map((action) => ({ id: action.id, to: action.to })),
    });
  }

  // 3. The planning-acceptance candidate: pre-selected structurally, then
  //    verified by the single authoritative planning restore. The
  //    continuation is derived only from the provenance-verified compiled
  //    planning role (the trusted `plan_ready` policy present → the
  //    automatic `resume` continuation; absent → the manual
  //    `resume-plan` contract).
  if (isPlanningAcceptanceCandidate(state)) {
    try {
      await restorePipelineV2PlanningAcceptanceContext(pipeline, state, runRoot);
    } catch (cause) {
      if (cause instanceof PipelineV2RuntimeContextRestoreError) {
        return mapRestoreReason(cause.reason);
      }
      return "internal_error";
    }
    const policy = pipelineV2PlanReadyPolicyFor(pipeline, state);
    return deepFreezeValue({
      kind: "plan_ready" as const,
      planningExecutionIndex: state.executions.length,
      continuation: policy !== null ? ("resume" as const) : ("resume_plan" as const),
    });
  }

  // 4. The single authoritative recovery classifier: every post-commit
  //    crash window is externally `resumable` (the one safe action is
  //    `orchestrator resume`); a classifier pipeline mismatch is a typed
  //    inspect failure. A terminal-recovery window additionally verifies
  //    the published outputs it will adopt or has already recorded: the
  //    publication phase decides the expectation (before the publication
  //    a missing tree is the ordinary terminal chain; an existing tree is
  //    the T2 orphan and must be exact; a durable record obliges the tree
  //    to exist and match it).
  const recovery = classifyPipelineV2ResumeRecovery(pipeline, state);
  if (recovery.kind === "transition" || recovery.kind === "run-finalization") {
    return deepFreezeValue({
      kind: "resumable" as const,
      stateId: state.cursor.current_state,
      transitionCount: state.cursor.transition_count,
    });
  }
  if (recovery.kind === "pipeline-mismatch") {
    return "pipeline_mismatch";
  }
  if (recovery.kind === "terminal") {
    let restored: Awaited<ReturnType<typeof restorePipelineV2RuntimeContext>>;
    try {
      restored = await restorePipelineV2RuntimeContext(pipeline, state, runRoot);
    } catch (cause) {
      if (cause instanceof PipelineV2RuntimeContextRestoreError) {
        return mapRestoreReason(cause.reason);
      }
      return "internal_error";
    }
    const outputsReason = await verifyTerminalPublication(
      pipeline,
      state,
      runRoot,
      restored.run_inputs,
      restored.accepted_outputs,
    );
    if (outputsReason !== null) {
      return outputsReason;
    }
    return deepFreezeValue({
      kind: "resumable" as const,
      stateId: state.cursor.current_state,
      transitionCount: state.cursor.transition_count,
    });
  }

  // 5. A genuinely in-flight execution (never a settled recovery window).
  const last = state.executions[state.executions.length - 1];
  if (last !== undefined && isInFlightExecution(last)) {
    return deepFreezeValue({
      kind: "in_flight" as const,
      executionIndex: last.index,
      stateId: last.state_id,
      executionRole: last.execution_role,
      phase: last.phase,
    });
  }

  // 6. The ordinary runtime boundary: pre-selected structurally (an
  //    active running run with every execution bound and no terminal
  //    record — including a cursor resting on a terminal state), then
  //    verified by the single authoritative runtime restore.
  if (
    state.status === "active" &&
    state.phase === "running" &&
    state.terminal === undefined &&
    state.executions.length === state.transitions.length
  ) {
    try {
      await restorePipelineV2RuntimeContext(pipeline, state, runRoot);
    } catch (cause) {
      if (cause instanceof PipelineV2RuntimeContextRestoreError) {
        return mapRestoreReason(cause.reason);
      }
      return "internal_error";
    }
    return deepFreezeValue({
      kind: "resumable" as const,
      stateId: state.cursor.current_state,
      transitionCount: state.cursor.transition_count,
    });
  }

  // 7. No loader-valid shape reaches this point (the union is total over
  //    the forms the loader admits); an unexpected residual is an honest
  //    internal error, never a new boundary kind.
  return "internal_error";
}
