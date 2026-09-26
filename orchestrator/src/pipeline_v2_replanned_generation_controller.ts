import {
  closePipelineV2ReplannedGenerationInternal,
  PipelineV2ReplannedGenerationControllerError,
  type ClosePipelineV2ReplannedGenerationOptions,
  type ClosedPipelineV2ReplannedGeneration,
  type PipelineV2ReplannedGenerationControllerFailureReason,
  type PipelineV2ReplannedGenerationControllerSink,
} from "./pipeline_v2_replanned_generation_controller_internal.ts";

export {
  PipelineV2ReplannedGenerationControllerError,
  type ClosePipelineV2ReplannedGenerationOptions,
  type ClosedPipelineV2ReplannedGeneration,
  type PipelineV2ReplannedGenerationControllerFailureReason,
  type PipelineV2ReplannedGenerationControllerSink,
};

/**
 * Production-neutral replanned-generation controller (unwired).
 *
 * Closes the previous stage generation durably after the next plan
 * revision has been accepted — the boundary
 *
 * `revise intent/task accepted → iteration closed by replanned →
 * revise_task response → planning execution → next plan accepted → old
 * generation closed by replanned`
 *
 * — from which a future increment opens the new plan's generation and
 * iteration. The single durable command is
 * `stage_generation_closed {generationIndex, by: "replanned"}`, built
 * only from the durable records and dispatched through the structural
 * sink; the reducer stays the single successor authority.
 *
 * Options: `sink` — the structural state sink (the production
 * `PipelineV2RunStateSink` satisfies it without an adapter); `intent` —
 * the exact provenance-backed prepared `revise_task_intent` accepted in
 * the target wait; `compiledPlan` — the exact provenance-backed
 * `CompiledPipelineV2RunPlan` returned by the existing
 * `acceptPipelineV2RunPlanCandidate`. A hostile extra options field is
 * ignored.
 *
 * Capture and provenance ordering (fail-closed, tested): the options
 * shape → `sink` → `intent` → `compiledPlan` each read once → the sink's
 * `poisoned`, `dispatch` and initial `snapshot` members each captured
 * exactly once as opaque references with `dispatch` bound to the sink
 * before the first await → the poison latch → the existing manifest
 * registry provenance gate of the intent (no intent field is read before
 * it, Proxy traps never invoked) with the strict `revise_task_intent`
 * kind → the existing compiled-plan provenance gate, equally before any
 * field of the compiled plan is read and with zero Proxy traps → the
 * single `validatePipelineV2RunState` → the hidden compiled-plan
 * pipeline identity comparison through the single existing structural
 * comparator → the durable bindings → the reconciliation → the reducer
 * pre-check of the single closing command → the dispatch. A missing or
 * invalid initial snapshot is a typed `invalid_state` with `state: null`
 * and a fixed content-free diagnostic; unexpected getter, registry or
 * dispatch errors keep their class and identity unless they belong to
 * the handled durable failure contract; errors are never classified from
 * message text.
 *
 * Exact prerequisite boundary (one internal classification point): the
 * immediate post-plan-acceptance boundary of one completed revise_task
 * cycle — the active/running run with no terminal, run outputs or
 * failure; the target wait the last wait-journal record and the only
 * record of its index (defensive full-journal pass), answered with
 * exactly the `revise_task` response, the declared `revise_task` action
 * with the cursor on its declared target, the exact accepted intent
 * digest and the intent manifest bound to the durable state (run id,
 * wait index, task id, predecessor and new-task digests); the cursor
 * transition count and the transition journal exactly at the wait
 * boundary; the accepted task revision read exclusively from the ledger
 * (exactly one wait-bound record of ANY task, exact contract fields, a
 * positive safe revision above 1, no later revision of the same task;
 * absent → `invalid_state`, several or conflicting or later →
 * `revision_conflict`); the old generation the last durable generation
 * bound to the previous plan digest without an open iteration, with at
 * least one iteration and the last iteration as the exact replanned
 * closure of the target wait, the historical iteration prefix
 * well-shaped (wrong reason, wait or anchor, a replaced or non-last
 * generation, a later iteration or generation → `lifecycle_conflict`);
 * the new accepted plan exactly the last durable plan revision with its
 * predecessor at revision −1 bound to the old generation's plan digest
 * (a stale, foreign or non-successor plan → `plan_conflict`); the
 * compiled plan projection carrying the revised task exactly once with
 * the exact accepted id, revision and digest (absent, duplicated or
 * stale → `plan_conflict`); and the settled planning execution right
 * after the response with the role strictly `planning`, the type
 * strictly `agent`, the phase strictly `cleanup_completed`, the state id
 * on the declared action target, the start boundary at the wait's
 * transition count, and no committed transition, new wait or new
 * execution beyond the boundary.
 *
 * Reconciliation: C0 — the old generation still open: the single closing
 * command pre-checked through the single reducer on the local validated
 * snapshot (a rejection is a typed `invalid_state` with zero dispatch),
 * dispatched exactly once, and the authoritative snapshot re-read and
 * fully verified; C1 — the exact durable replanned closure already on
 * the last generation: an idempotent zero-dispatch success built from
 * the already verified snapshot, with no snapshot re-read after the
 * classification and no state restoration. A partially matching closure
 * is never an idempotent success. The post-dispatch verification is the
 * same targeted comparison on the normal resolve path and the racing
 * reducer-rejection path: the revision moved exactly `before + 1`, the
 * run id and the durable pipeline identity unchanged, status, phase,
 * cursor and boundary journals unchanged, the wait journal, the task
 * ledger and the plan ledger positionally unchanged, the generation
 * journal length unchanged with historical generations and the
 * historical iteration prefix unchanged, and the single new field the
 * exact `replanned` closure with
 * `closed_transition_count === wait.transition_count`; the comparisons
 * are defensive contract-owned helpers — a malformed hostile snapshot
 * yields a typed controller error, never a `TypeError`; a racing exact
 * closure is an idempotent success on that full verification, while a
 * resolve-without-change, a wrong revision delta, a changed run id, an
 * altered binding or a partial closure is a failure, never a success.
 *
 * Durability: a `PipelineV2RunStateDurabilityError` adopts the exact
 * closure candidate visible on disk, poisons the sink, stops every
 * further dispatch and fails `state_persist_failed` with the adopted
 * state (a fresh retry with a reopened sink recognizes the exact C1
 * closure with zero dispatch); a plain store error (`not_committed`)
 * keeps the previous open-generation state authoritative and a fresh
 * retry dispatches the closing command again; unexpected errors keep
 * their identity. Diagnostics are content-free (no digest values,
 * canonical JSON, bodies, paths, env values or credentials).
 *
 * The result is deep-frozen and content-free:
 * `{wait_index, generation_index, iteration_index, intent_sha256,
 * task_id, task_revision, task_sha256, previous_plan_revision,
 * previous_plan_sha256, plan_revision, plan_sha256, origin_execution,
 * state}` — no manifest, canonical JSON, paths, bodies, prepared intent,
 * compiled plan or other caller-owned objects. Runtime export surface is
 * exactly `PipelineV2ReplannedGenerationControllerError` and
 * `closePipelineV2ReplannedGeneration`; no planner, pre-check,
 * comparator or test seam is exported and there is no mutable ops seam
 * (the controller performs no filesystem work).
 *
 * Not implemented (stays unwired): the action/intent selection policy,
 * the architect execution and its output parsing, plan candidate
 * construction, the plan acceptance controller, opening the next
 * generation or iteration, stage selection and the initial-budget
 * policy, the graph transition commit, automatic resume,
 * coordinator/runner/CLI/default-pipeline wiring, schema changes,
 * migrations/API/T3 and multi-process locking.
 */
export function closePipelineV2ReplannedGeneration(
  options: ClosePipelineV2ReplannedGenerationOptions,
): Promise<ClosedPipelineV2ReplannedGeneration> {
  return closePipelineV2ReplannedGenerationInternal(options);
}
