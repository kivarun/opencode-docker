/**
 * Explicitly internal core of the production-neutral run plan acceptance
 * controller for pipeline schema v2 (unwired, production-neutral).
 *
 * This module is the single layer that owns the crash-safe order binding
 * the existing run-plan layers together:
 *
 *   compiled/state validation
 *   → reducer pre-check of the whole missing durable sequence
 *   → publication of the task manifests (candidate publisher, candidate
 *     order, strictly sequential)
 *   → publication of the plan manifest as the filesystem commit marker
 *   → durable `task_revision_accepted` for the missing task records
 *   → durable `plan_revision_accepted` as the state commit marker
 *
 * Every step reuses the existing layer; there is no second validator,
 * no second comparator, no second candidate compiler, no own successor
 * rules and no message parsing:
 *
 * - `verifyPipelineV2RunPlanCandidateForAcceptance` is the only acceptance
 *   authority (pipeline provenance, candidate provenance, compiled
 *   template binding, state validation, pipeline identity, acceptance
 *   boundary, run binding of the compiled plan, origin execution, planning
 *   role); its errors keep their own classes and identities;
 * - the durable reconciliation compares the candidate's exact task and
 *   plan revision chains against the durable ledgers and dispatches only
 *   the missing records;
 * - `reducePipelineV2RunCommand` is the only successor authority: the
 *   whole missing sequence is pre-checked against the reducer on a local
 *   snapshot before any filesystem side effect, and the authoritative
 *   state comes exclusively from the sink's dispatches;
 * - `publishPipelineV2RunPlanCandidate` is the only publication authority
 *   (tasks sequentially, plan last, idempotent adoption); its store
 *   failures keep the original typed class;
 * - the sink stays the only durable writer: the controller never writes
 *   the state itself and never rolls back a durable record or a published
 *   manifest.
 *
 * Durable reconciliation (fail-closed, structural):
 *
 * - a candidate task revision is already durable exactly when the ledger
 *   carries the exact `task_id` + `revision` pair with the candidate's
 *   `sha256` and `previous_sha256`, AND it is the latest durable revision
 *   of that task id (an earlier revision exists only as a chain
 *   predecessor; a durable revision newer than the candidate's is a
 *   downgrade conflict) — such a record is never re-dispatched;
 * - a durable record for the same pair with a different digest or chain
 *   is a `candidate_conflict` — nothing is written;
 * - a durable record for the same task id at a different revision is a
 *   `candidate_conflict` (the candidate's revision can never be created
 *   next to it);
 * - a missing task revision above 1 is a `candidate_conflict`: a
 *   user-response revision cannot be created on the active planning
 *   boundary;
 * - a missing revision-1 task is appended to the missing sequence;
 * - the durable plan record matching the candidate revision exactly
 *   (revision, sha256, previous_sha256, origin_execution) is an
 *   idempotent durable success only when it is the last durable plan
 *   revision — no second plan dispatch happens and the sequence is empty
 *   (every candidate task must already be durable); a durable ledger that
 *   has moved past the candidate revision makes the candidate stale
 *   (`candidate_conflict`);
 * - a durable plan record at the candidate revision with different
 *   content, a candidate revision ahead of the durable ledger (ahead/gap:
 *   the candidate revision is greater than the next expected ledger
 *   revision), or a candidate chain that does not link the durable ledger
 *   are `candidate_conflict` failures;
 * - durable task revisions for tasks outside the current candidate are
 *   never a conflict by themselves.
 *
 * Dispatch: after the publication succeeded, the missing task records are
 * dispatched strictly in candidate order and the plan record last. After
 * every dispatch the authoritative sink snapshot is re-read and must
 * structurally carry exactly the expected record (id + revision + digest
 * + chain + origin); a sink that resolves without the expected snapshot
 * change fails closed. A reducer rejection after a racing identical
 * dispatch is idempotent success only when the authoritative snapshot now
 * carries the exact expected record; otherwise it is a typed failure. No
 * second dispatch is ever issued automatically after a commit failure.
 *
 * Durability semantics: a sink `not_committed` keeps the previous
 * snapshot authoritative and the published manifests as orphans, returns
 * `state_persist_failed` and performs no automatic retry; a
 * `durability_unknown` adopts the visible candidate (the sink poisons
 * itself), returns `state_persist_failed` with that adopted state and
 * stops all further dispatch; a fresh retry with a freshly loaded sink
 * reuses the published artifacts (idempotent adoption) and dispatches
 * only the still-missing records. Nothing is ever rolled back.
 *
 * Capture boundary: every options field is read exactly once; the sink's
 * `poisoned`/`dispatch`/`snapshot` accessors are each read exactly once
 * in the synchronous prefix as opaque references — the pipeline,
 * candidate and state documents are not traversed here, so no field of
 * them is read before the pipeline/candidate provenance gates have run
 * (the acceptance chain is the first validation of pipeline/candidate/
 * state contents after the fail-closed sink poison latch: pipeline
 * provenance, candidate provenance/compile, and only then the state
 * validation and the identity/boundary checks); `dispatch` is bound to
 * the sink once before the first await, caller objects are never frozen
 * or modified, an unexpected error from a sink getter propagates
 * unchanged, and the publication ops are captured exactly once per call
 * (each method read one time, type-checked, then never read again); the
 * single frozen production ops object is bound to the existing public
 * functions and there is no mutable module-global seam, so an injected
 * call can never influence a parallel production call.
 *
 * Errors: only this layer's own failures are
 * `PipelineV2RunPlanControllerError` with the closed reason set
 * (`invalid_state`, `candidate_conflict`, `state_persist_failed`),
 * classified by structure and operation phase — never by message text —
 * each carrying the last authoritative `state`. Acceptance, orchestration,
 * compiled-plan, manifest, binding, publication and pipeline errors keep
 * their original classes and identities; unexpected errors propagate
 * unchanged. Diagnostics are content-free: validated safe ids, revisions,
 * indexes and closed operation classes only — no bodies, canonical JSON,
 * paths, digests, env values or credentials.
 *
 * Not implemented (stays unwired): the coordinator, runner, CLI, the
 * stage generation/iteration lifecycle controller, wait/replanning
 * policy, automatic resume, multi-process locking and the API/T3 layers
 * are later increments.
 */
import { basename } from "node:path";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  PipelineV2RunStateDurabilityError,
  PipelineV2RunStateStoreError,
} from "./pipeline_v2_state_store.ts";
import { verifyPipelineV2RunPlanCandidateForAcceptance } from "./pipeline_v2_run_plan_acceptance.ts";
import {
  publishPipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "./pipeline_v2_run_plan_candidate.ts";
import type { CompiledPipelineV2RunPlan } from "./pipeline_v2_run_plan_compiled.ts";
import type { ResolvedPipelineV2 } from "./pipeline_v2.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";

/** The closed reason set of the run plan controller's own failures. */
export type PipelineV2RunPlanControllerFailureReason =
  | "invalid_state"
  | "candidate_conflict"
  | "state_persist_failed";

const REASON_SET: ReadonlySet<string> = new Set<string>([
  "invalid_state",
  "candidate_conflict",
  "state_persist_failed",
]);

/**
 * A failure of the run plan controller layer with its stable
 * machine-readable `reason` and the last authoritative durable state
 * (or `null` when no durable state exists). The reason is assigned where
 * the failing operation's semantics are known (never by classifying
 * message text) and is one of the fixed closed set.
 */
export class PipelineV2RunPlanControllerError extends Error {
  declare readonly reason: PipelineV2RunPlanControllerFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2RunPlanControllerFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 run plan controller error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2RunPlanControllerError";
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
 * equivalents. The controller holds no sink method other than the one
 * `dispatch` captured in the synchronous prefix.
 */
export interface PipelineV2RunPlanControllerSink {
  readonly snapshot: PipelineV2RunState | null;
  readonly poisoned: boolean;
  readonly dispatch: (command: PipelineV2RunCommand) => void | Promise<void>;
}

export interface AcceptPipelineV2RunPlanCandidateOptions {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRoot: string;
  readonly sink: PipelineV2RunPlanControllerSink;
  readonly candidate: PreparedPipelineV2RunPlanCandidate;
}

export interface AcceptedPipelineV2RunPlanCandidate {
  readonly compiled_plan: CompiledPipelineV2RunPlan;
  readonly state: PipelineV2RunState;
}

/**
 * The per-call capability seam of the controller: the acceptance verifier
 * and the candidate publisher. The single frozen production object binds
 * the existing public functions; tests inject their own per-call object.
 */
export interface PipelineV2RunPlanControllerOps {
  readonly verifyCandidateForAcceptance: typeof verifyPipelineV2RunPlanCandidateForAcceptance;
  readonly publishCandidate: typeof publishPipelineV2RunPlanCandidate;
}

export const realPipelineV2RunPlanControllerOps: PipelineV2RunPlanControllerOps = Object.freeze({
  verifyCandidateForAcceptance: verifyPipelineV2RunPlanCandidateForAcceptance,
  publishCandidate: publishPipelineV2RunPlanCandidate,
});

interface Captured {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRoot: string;
  readonly sink: PipelineV2RunPlanControllerSink;
  readonly candidate: PreparedPipelineV2RunPlanCandidate;
  readonly dispatch: (command: PipelineV2RunCommand) => Promise<void>;
  readonly snapshot: PipelineV2RunState;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function controllerError(
  reason: PipelineV2RunPlanControllerFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2RunPlanControllerError {
  return new PipelineV2RunPlanControllerError(reason, message, state);
}

function conflict(message: string, state: PipelineV2RunState): PipelineV2RunPlanControllerError {
  return controllerError("candidate_conflict", message, state);
}

/**
 * The synchronous capture boundary: every options field is read exactly
 * once and the sink accessors (`poisoned`, `dispatch`, `snapshot`) are
 * each read exactly once as opaque references — the pipeline, candidate
 * and state documents are not traversed here (no field of them is read
 * before the pipeline/candidate provenance gates have run; the acceptance
 * chain owns the state validation and reads the document only through its
 * own gates). `dispatch` is bound to the sink. An unexpected error from a
 * sink getter propagates unchanged. No filesystem or sink side effect
 * happens here; every rejection is a typed controller failure.
 */
function captureBoundary(options: unknown): Captured {
  if (!isRecord(options)) {
    throw controllerError("invalid_state", "acceptPipelineV2RunPlanCandidate requires an options object", null);
  }
  const pipeline = options["pipeline"];
  const runRoot = options["runRoot"];
  const sink = options["sink"];
  const candidate = options["candidate"];
  if (typeof runRoot !== "string" || runRoot === "") {
    throw controllerError("invalid_state", "acceptPipelineV2RunPlanCandidate requires a non-empty run root", null);
  }
  if (!isRecord(sink)) {
    throw controllerError("invalid_state", "acceptPipelineV2RunPlanCandidate requires a state sink", null);
  }
  const poisoned: unknown = sink["poisoned"];
  if (poisoned === true) {
    throw controllerError(
      "invalid_state",
      "the run state sink is poisoned by a durability-unknown commit; no plan candidate is accepted for this run",
      null,
    );
  }
  const dispatch = sink["dispatch"];
  if (typeof dispatch !== "function") {
    throw controllerError("invalid_state", "acceptPipelineV2RunPlanCandidate requires a dispatchable state sink", null);
  }
  const snapshot: unknown = sink["snapshot"];
  // The snapshot is captured as an opaque reference only: its fields are
  // not read before the pipeline/candidate provenance gates have run (the
  // acceptance chain owns the state validation and reads the document
  // only through its own gates).
  if (!isRecord(snapshot)) {
    throw controllerError(
      "invalid_state",
      "no durable pipeline v2 run state exists yet",
      null,
    );
  }
  return {
    pipeline: pipeline as ResolvedPipelineV2,
    runRoot,
    sink: sink as unknown as PipelineV2RunPlanControllerSink,
    candidate: candidate as PreparedPipelineV2RunPlanCandidate,
    dispatch: (dispatch as (command: PipelineV2RunCommand) => Promise<void>).bind(sink),
    snapshot: snapshot as unknown as PipelineV2RunState,
  };
}

/**
 * One missing durable record of the reconciled sequence, with the exact
 * structural expectation the post-dispatch verification re-checks.
 */
type MissingRecord =
  | { kind: "task"; taskId: string; revision: number; sha256: string; previousSha256: string | null }
  | { kind: "plan"; revision: number; sha256: string; previousSha256: string | null; originExecution: number };

function taskCommand(record: MissingRecord): PipelineV2RunCommand {
  if (record.kind !== "task") {
    throw new Error("pipeline v2 run plan controller invariant violated: the record is not a task record");
  }
  return {
    kind: "task_revision_accepted",
    taskId: record.taskId,
    revision: record.revision,
    taskSha256: record.sha256,
  };
}

function planCommand(record: MissingRecord): PipelineV2RunCommand {
  if (record.kind !== "plan") {
    throw new Error("pipeline v2 run plan controller invariant violated: the record is not a plan record");
  }
  return {
    kind: "plan_revision_accepted",
    planRevision: record.revision,
    planSha256: record.sha256,
    originExecution: record.originExecution,
  };
}

function recordCommand(record: MissingRecord): PipelineV2RunCommand {
  return record.kind === "task" ? taskCommand(record) : planCommand(record);
}

/**
 * Whether the durable state carries exactly the expected record: identity,
 * digest, chain and — for the plan — the origin execution. The idempotency
 * and post-dispatch checks share this one structural expectation.
 */
function recordPresent(state: PipelineV2RunState, record: MissingRecord): boolean {
  if (record.kind === "task") {
    const durable = state.task_revisions.find(
      (candidate) => candidate.task_id === record.taskId && candidate.revision === record.revision,
    );
    return (
      durable !== undefined &&
      durable.sha256 === record.sha256 &&
      durable.previous_sha256 === record.previousSha256
    );
  }
  const durable = state.plan_revisions.find((candidate) => candidate.revision === record.revision);
  return (
    durable !== undefined &&
    durable.sha256 === record.sha256 &&
    durable.previous_sha256 === record.previousSha256 &&
    durable.origin_execution === record.originExecution
  );
}

/**
 * The durable reconciliation: compares the candidate's exact task and
 * plan revision chains against the durable ledgers and returns the
 * missing sequence (task records in candidate order, the plan record
 * last). Every mismatch is a typed conflict before any reducer call,
 * filesystem publication or dispatch.
 */
function reconcileCandidate(
  compiledPlan: CompiledPipelineV2RunPlan,
  candidate: PreparedPipelineV2RunPlanCandidate,
  state: PipelineV2RunState,
): MissingRecord[] {
  const missing: MissingRecord[] = [];
  for (const task of candidate.task_revisions) {
    const manifest = task.manifest;
    const durableForTask = state.task_revisions.filter(
      (record) => record.task_id === manifest.task_id,
    );
    const durable = durableForTask.find((record) => record.revision === manifest.revision);
    if (durable !== undefined) {
      // the exact candidate revision must be the latest durable revision of
      // the task; earlier revisions exist only as chain predecessors
      const latest = durableForTask[durableForTask.length - 1];
      if (latest !== undefined && latest.revision > manifest.revision) {
        throw conflict(
          `the durable task ledger already records a newer revision ${latest.revision} of ${JSON.stringify(manifest.task_id)} than the candidate's revision ${manifest.revision}`,
          state,
        );
      }
      if (durable.sha256 !== task.sha256 || durable.previous_sha256 !== manifest.previous_sha256) {
        throw conflict(
          `the durable task revision ${manifest.revision} of ${JSON.stringify(manifest.task_id)} does not match the candidate chain`,
          state,
        );
      }
      continue;
    }
    if (durableForTask.length > 0) {
      throw conflict(
        `the durable task ledger already records a different revision of task ${JSON.stringify(manifest.task_id)} than the candidate declares`,
        state,
      );
    }
    if (manifest.revision !== 1) {
      throw conflict(
        `the durable ledger records no task revision ${manifest.revision} of ${JSON.stringify(manifest.task_id)}; a user-response revision cannot be created on the active planning boundary`,
        state,
      );
    }
    missing.push({
      kind: "task",
      taskId: manifest.task_id,
      revision: manifest.revision,
      sha256: task.sha256,
      previousSha256: manifest.previous_sha256,
    });
  }
  const durablePlan = state.plan_revisions.find((record) => record.revision === compiledPlan.plan_revision);
  if (durablePlan !== undefined) {
    // the exact candidate revision is an idempotent durable success only
    // when it is the last durable plan revision; a ledger that has moved
    // past it makes the candidate stale
    const latestPlan = state.plan_revisions[state.plan_revisions.length - 1];
    if (latestPlan !== undefined && latestPlan.revision > compiledPlan.plan_revision) {
      throw conflict(
        `the durable plan ledger has moved past the candidate plan revision ${compiledPlan.plan_revision}; the candidate is stale`,
        state,
      );
    }
    if (
      durablePlan.sha256 !== compiledPlan.plan_sha256 ||
      durablePlan.previous_sha256 !== candidate.plan.manifest.previous_sha256 ||
      durablePlan.origin_execution !== compiledPlan.origin_execution
    ) {
      throw conflict(
        `the durable plan revision ${compiledPlan.plan_revision} does not match the candidate plan`,
        state,
      );
    }
    if (missing.length > 0) {
      throw conflict(
        `the durable plan revision ${compiledPlan.plan_revision} is already recorded while a candidate task revision is missing from the durable ledger`,
        state,
      );
    }
    return missing;
  }
  if (state.plan_revisions.length + 1 < compiledPlan.plan_revision) {
    throw conflict(
      `the candidate plan revision ${compiledPlan.plan_revision} is ahead of the durable plan ledger; the next expected revision is ${state.plan_revisions.length + 1}`,
      state,
    );
  }
  const lastPlan = state.plan_revisions[state.plan_revisions.length - 1];
  const durableChainDigest = lastPlan === undefined ? null : lastPlan.sha256;
  if (candidate.plan.manifest.previous_sha256 !== durableChainDigest) {
    throw conflict(
      `the candidate plan revision ${compiledPlan.plan_revision} does not chain to the durable plan ledger`,
      state,
    );
  }
  missing.push({
    kind: "plan",
    revision: compiledPlan.plan_revision,
    sha256: compiledPlan.plan_sha256,
    previousSha256: candidate.plan.manifest.previous_sha256,
    originExecution: compiledPlan.origin_execution,
  });
  return missing;
}

/**
 * The controller core with the per-call ops capability. The public wrapper
 * always passes the single frozen production ops object; an injected call
 * can never influence a parallel production call.
 */
export async function acceptPipelineV2RunPlanCandidateCore(
  ops: unknown,
  options: unknown,
): Promise<AcceptedPipelineV2RunPlanCandidate> {
  if (typeof ops !== "object" || ops === null) {
    throw new TypeError(
      "the run plan controller requires verifyCandidateForAcceptance and publishCandidate functions",
    );
  }
  const verifyCandidateForAcceptance = (ops as { verifyCandidateForAcceptance?: unknown })
    .verifyCandidateForAcceptance as typeof verifyPipelineV2RunPlanCandidateForAcceptance | undefined;
  const publishCandidate = (ops as { publishCandidate?: unknown }).publishCandidate as
    | typeof publishPipelineV2RunPlanCandidate
    | undefined;
  if (typeof verifyCandidateForAcceptance !== "function" || typeof publishCandidate !== "function") {
    throw new TypeError(
      "the run plan controller requires verifyCandidateForAcceptance and publishCandidate functions",
    );
  }

  const ctx = captureBoundary(options);

  // Validation: the single acceptance verifier (pipeline provenance,
  // candidate provenance, compiled template binding, the single state
  // validator, the pipeline identity comparator, the acceptance boundary,
  // the candidate/run binding, the origin execution, the planning role).
  // Its errors keep their original classes and identities.
  const compiledPlan = verifyCandidateForAcceptance(ctx.pipeline, ctx.snapshot, ctx.candidate);

  // Run binding: the run root belongs to this run.
  if (basename(ctx.runRoot) !== ctx.snapshot.run_id) {
    throw controllerError(
      "invalid_state",
      "the run root does not belong to this run",
      ctx.snapshot,
    );
  }

  // Durable reconciliation (structural; typed conflicts before any
  // reducer call, publication or dispatch).
  const missing = reconcileCandidate(compiledPlan, ctx.candidate, ctx.snapshot);

  // Reducer pre-check: the whole missing sequence is applied to a local
  // snapshot through the single reducer before any filesystem side
  // effect; the authoritative state comes exclusively from the sink.
  let checked = ctx.snapshot;
  for (const record of missing) {
    const command = recordCommand(record);
    try {
      checked = reducePipelineV2RunCommand(checked, command, new Date());
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        throw controllerError(
          "invalid_state",
          "the current run state does not accept the missing durable sequence",
          ctx.snapshot,
        );
      }
      throw cause;
    }
  }

  // Filesystem publication: the candidate publisher is called exactly
  // once; it owns the task-then-plan order and its store failures keep
  // the original typed class and identity.
  await publishCandidate(ctx.runRoot, ctx.candidate);

  // Durable dispatch: the missing task records in candidate order, then
  // the plan record; after every dispatch the authoritative snapshot is
  // re-read and must carry exactly the expected record.
  let latest = ctx.snapshot;
  for (const record of missing) {
    const command = recordCommand(record);
    const what = record.kind === "task"
      ? `the task revision ${record.revision} of ${JSON.stringify(record.taskId)}`
      : `the plan revision ${record.revision}`;
    let dispatched = false;
    try {
      await ctx.dispatch(command);
    } catch (cause) {
      if (cause instanceof PipelineV2StateError) {
        const after = ctx.sink.snapshot;
        if (after !== null && recordPresent(after, record)) {
          dispatched = true;
          latest = after;
        } else {
          throw controllerError(
            "invalid_state",
            `the run state rejected ${what} and does not carry it`,
            after,
          );
        }
      } else if (cause instanceof PipelineV2RunStateDurabilityError) {
        throw controllerError(
          "state_persist_failed",
          `${what} could not be confirmed durable`,
          ctx.sink.snapshot,
        );
      } else if (cause instanceof PipelineV2RunStateStoreError) {
        throw controllerError(
          "state_persist_failed",
          `${what} could not be committed`,
          ctx.sink.snapshot,
        );
      } else {
        throw cause;
      }
    }
    if (!dispatched) {
      const after = ctx.sink.snapshot;
      if (after === null || !recordPresent(after, record)) {
        throw controllerError(
          "invalid_state",
          `the committed run state does not carry ${what}`,
          after,
        );
      }
      latest = after;
    }
  }

  return deepFreezeValue({ compiled_plan: compiledPlan, state: latest });
}
