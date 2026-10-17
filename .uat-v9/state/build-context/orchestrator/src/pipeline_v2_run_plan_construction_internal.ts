/**
 * Internal core of the read-only proposal → candidate construction layer
 * for pipeline schema v2 (production-neutral, unwired).
 *
 * This module is the single layer that builds one provenance-backed
 * `PreparedPipelineV2RunPlanCandidate` from an agent-authored run plan
 * proposal and the durable run state, using only the existing manifest,
 * store and candidate chains. One construction call is anchored and
 * retry-aware: on a fresh, a partial and a completed plan-acceptance
 * boundary it returns the same exact candidate.
 *
 *   validated durable state → authoritative derivation
 *   → the anchored target search (zero loads): plan records whose
 *     `origin_execution` names this construction's origin execution are
 *     the acceptance evidence of exactly this boundary; zero records
 *     target the next revision, exactly one (which must be the last plan
 *     record) targets its revision, more than one or a non-last record
 *     is an incoherent ledger
 *   → the FIRST store load: the target plan artifact by the computed
 *     revision
 *   → absent artifact: a completed ledger record, or durable task
 *     evidence intersecting `new_tasks`, fails closed (`artifact_missing`
 *     — partial ownership is never recognized without the commit-marker);
 *     otherwise the fresh construction (the chain below) runs unchanged
 *   → present artifact: the single retry anchor — its exact anchor
 *     binding (the origin execution, the base predecessor digest), the
 *     normalized stage/template/task/dependency projection of the
 *     proposal, and the exact task revision/digest bindings (each new
 *     task re-prepared from the proposal body and bound to the target
 *     pointer, the artifact and a possible durable ledger record; each
 *     existing task resolved only in the durable ledger exactly at the
 *     pointer's revision) are verified before the candidate is assembled
 *   → the existing `preparePipelineV2RunPlanCandidate` (provenance,
 *     root-task binding, run/task/revision/digest/chain correspondence)
 *     returned directly, never wrapped or copied.
 *
 * Anchored load order: target plan → base predecessor plan, if any →
 * target tasks in the artifact's semantic order, each with its immediate
 * durable predecessor above revision 1. The fresh path keeps the
 * previous deterministic order (the base plan, then the pointered
 * existing tasks in proposal traversal order, each with its predecessor).
 *
 * The layer is strictly read-only: no publication, no durable dispatch, no
 * reducer call, no sink, no coordinator/runner/CLI, and no second parser,
 * serializer, digest builder or provenance registry. Loaded prepared plan
 * and task objects are opaque; their provenance and every identity and
 * chain correspondence is verified by the existing candidate preparation —
 * never re-verified here. This module checks only the safe form of a
 * loader-result wrapper (so a hostile injected result can never produce a
 * `TypeError`); everything beyond that shape is delegated. Orphan
 * artifacts at non-target revisions never select a revision: the durable
 * ledgers are the only authority for the fresh path, and the target
 * artifact is the only authority for the anchored path (its binding is
 * verified, so a foreign orphan is a conflict, never an anchor).
 *
 * Authoritative derivation (every durable field, never a caller field):
 *
 * - `run_id`: the validated state's `run_id`;
 * - the run-root binding: `basename(runRoot) === state.run_id` before any
 *   store load (the canonical store contract is enforced by the loaders
 *   themselves);
 * - `origin_execution`: the index of the last durable execution — the
 *   planning/settled/unbound acceptance boundary belongs to the
 *   acceptance controller and is deliberately not re-checked here;
 * - the root task: exactly one durable input named `task` carrying
 *   `protected: true`; its digest is the only root-task digest source;
 * - the target plan revision: the last durable plan record's revision + 1
 *   (or 1 on an empty ledger) when no plan record names this origin
 *   execution, else the matching record's revision; `previous_sha256` is
 *   `null` or the base record's digest;
 * - an existing task: the durable revision the target plan artifact's
 *   pointer names (the anchored path) or the latest durable revision of
 *   its id (the fresh path); its predecessor above revision 1 is the
 *   immediate durable predecessor record;
 * - a new task: revision 1, `previous_sha256: null`,
 *   `origin: "planning_proposal"`, and the body taken only from the
 *   proposal;
 * - stages, templates, task ids and dependencies are declared only by the
 *   proposal; the plan's graph and normalization semantics stay with the
 *   single existing `preparePlanRevisionManifest`.
 *
 * Construction policy (fixed here, checked before any store load):
 *
 * 1. every `new_tasks[].id` must be absent from the durable task ledger
 *    unless the target plan artifact binds it exactly (the anchored
 *    path); durable task evidence for proposed new tasks without the
 *    commit-marker is a closed `artifact_missing` refusal (a new task
 *    body can never silently replace a recorded task);
 * 2. every `new_tasks` entry must be referenced by at least one plan
 *    pointer — an unused entry is a typed construction conflict (a task
 *    body can never be silently dropped); repeated plan task ids remain
 *    owned by the manifest validator, never by a second graph validator;
 * 3. every plan task pointer must resolve to exactly one source — the
 *    latest durable task revision or the exact `new_tasks` entry (the
 *    fresh path), or exactly the target artifact's pointer binding (the
 *    anchored path); neither or both is a typed construction conflict.
 *
 * Failure contract (typed, classified by context — never by message
 * text): `invalid_options` (options/ops shape, the run-root scalar, an
 * untrusted proposal), `invalid_state` (the state validation, no durable
 * execution, the root-task input binding, an incoherent plan-acceptance
 * target ledger), `run_root_mismatch` (the run-root basename binding),
 * `construction_conflict` (the policy rules, a foreign or non-binding
 * retry anchor, a durable task record that never matches the proposal),
 * `artifact_missing` (a required ledger-named or anchor-bound artifact is
 * absent, or durable acceptance evidence exists without its
 * commit-marker) and `malformed_loader_result` (a hostile injected loader
 * result that is not a safe published-manifest wrapper). Store, manifest
 * and binding typed errors pass through unchanged by object identity;
 * unexpected errors propagate unchanged. The error carries the last
 * validated durable state (`null` before it exists). Diagnostics are
 * content-free: they never echo task bodies, run-root paths, raw JSON,
 * parser fragments or arbitrary loader values — safe ids, revisions and
 * counts only.
 *
 * Nothing here touches the filesystem write path, the reducer, the durable
 * state schema, the acceptance controller, the restore, the coordinator,
 * the runner or the CLI; the future planning-transition hook is the only
 * intended caller.
 */
import { basename } from "node:path";
import { loadPipelineV2PlanRevision, loadPipelineV2TaskRevision } from "./pipeline_v2_run_plan_store.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "./pipeline_v2_run_plan_candidate.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "./pipeline_v2_run_plan_manifests.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2PlanRevisionState,
  type PipelineV2RunState,
  type PipelineV2TaskRevisionState,
} from "./pipeline_v2_state.ts";
import type {
  PipelineV2RunPlanProposal,
  PipelineV2RunPlanProposalNewTask,
  PipelineV2RunPlanProposalTaskRef,
} from "./pipeline_v2_run_plan_proposal.ts";

export type PipelineV2RunPlanConstructionFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "run_root_mismatch"
  | "construction_conflict"
  | "artifact_missing"
  | "malformed_loader_result";

const REASON_SET: ReadonlySet<PipelineV2RunPlanConstructionFailureReason> = new Set([
  "invalid_options",
  "invalid_state",
  "run_root_mismatch",
  "construction_conflict",
  "artifact_missing",
  "malformed_loader_result",
]);

/**
 * A failure of the read-only proposal → candidate construction with its
 * stable machine-readable `reason`. The reason is assigned where the
 * failing operation's semantics are known (never by classifying message
 * text), is immutable, and is one of the fixed closed reason set. The
 * `state` field carries the last validated durable run state — `null`
 * until the state validation succeeded.
 */
export class PipelineV2RunPlanConstructionError extends Error {
  declare readonly reason: PipelineV2RunPlanConstructionFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2RunPlanConstructionFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 run plan construction error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2RunPlanConstructionError";
    this.state = state;
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

export interface ConstructPipelineV2RunPlanCandidateOptions {
  /** The run root of the existing durable run; its basename binds the run. */
  readonly runRoot: string;
  /** The durable run state document; validated once, never trusted beyond it. */
  readonly state: unknown;
  /** The prepared agent-authored proposal; provenance-gated before any read. */
  readonly proposal: PipelineV2RunPlanProposal;
}

/**
 * The read-only store capabilities of the construction: exactly the two
 * existing loaders, nothing else. Results are erased so fault injection
 * can substitute primitives without a parallel typed path; the real
 * production methods return the store's provenance-backed wrappers.
 */
export interface PipelineV2RunPlanConstructionOps {
  readonly loadPlanRevision: (runRoot: string, revision: number) => Promise<unknown>;
  readonly loadTaskRevision: (runRoot: string, taskId: string, revision: number) => Promise<unknown>;
}

/**
 * The single frozen production ops object over the existing store loaders;
 * both methods are fixed at construction and can never be reassigned
 * through this object.
 */
export const productionRunPlanConstructionOps: PipelineV2RunPlanConstructionOps = Object.freeze({
  loadPlanRevision: loadPipelineV2PlanRevision,
  loadTaskRevision: loadPipelineV2TaskRevision,
});

/** The fixed root-task input binding of every run plan (shared contract value). */
const ROOT_TASK_INPUT_ID = "task";

const PROPOSAL_GATE_MESSAGE =
  "the construction requires the frozen prepared run plan proposal returned by " +
  "preparePipelineV2RunPlanProposal or parsePipelineV2RunPlanProposal; hand-built " +
  "objects, casts, clones and Proxies are rejected before any field is read";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function constructionError(
  reason: PipelineV2RunPlanConstructionFailureReason,
  message: string,
  state: PipelineV2RunState | null,
): PipelineV2RunPlanConstructionError {
  return new PipelineV2RunPlanConstructionError(reason, message, state);
}

/**
 * The safe form of one loaded store result: `null` is a missing artifact;
 * anything that is not the store's published-manifest wrapper of the
 * expected manifest kind is this layer's typed loader failure. Every
 * deeper property (identity, digests, chains, canonical bytes) is verified
 * by the existing candidate preparation, never re-verified here.
 */
function requireLoadedManifest(
  loaded: unknown,
  wrapperField: "plan" | "task",
  manifestKind: "plan_revision" | "task_revision",
  missingWhat: string,
  state: PipelineV2RunState,
): PreparedPipelineV2RunPlanRevision | PreparedPipelineV2RunTaskRevision {
  if (loaded === null) {
    throw constructionError("artifact_missing", missingWhat, state);
  }
  if (!isRecord(loaded)) {
    throw constructionError(
      "malformed_loader_result",
      "the loaded store result is not a published manifest wrapper",
      state,
    );
  }
  const prepared = loaded[wrapperField];
  if (!isRecord(prepared)) {
    throw constructionError(
      "malformed_loader_result",
      `the loaded store result carries no ${wrapperField} prepared manifest`,
      state,
    );
  }
  const manifest = prepared["manifest"];
  if (!isRecord(manifest)) {
    throw constructionError(
      "malformed_loader_result",
      "the loaded prepared manifest carries no manifest record",
      state,
    );
  }
  if (manifest["kind"] !== manifestKind) {
    throw constructionError(
      "malformed_loader_result",
      "the loaded prepared manifest carries a different manifest kind",
      state,
    );
  }
  return prepared as unknown as PreparedPipelineV2RunPlanRevision | PreparedPipelineV2RunTaskRevision;
}

/** One resolved plan task pointer: exactly one of the two allowed sources. */
interface ResolvedPointer {
  readonly pointer: PipelineV2RunPlanProposalTaskRef;
  /** The latest durable revision when the pointer names a recorded task. */
  readonly latest: PipelineV2TaskRevisionState | null;
  /** The exact proposal entry when the pointer names a proposed new task. */
  readonly entry: PipelineV2RunPlanProposalNewTask | null;
}

export async function constructPipelineV2RunPlanCandidateFromProposalInternal(
  options: unknown,
  ops: PipelineV2RunPlanConstructionOps,
): Promise<PreparedPipelineV2RunPlanCandidate> {
  // Capture boundary: the options shape, every options field and both ops
  // methods are read exactly once, in this fixed order, all before the
  // first await. No field of the durable state or of the proposal is read
  // here; a later mutation of the caller's options cannot change this
  // construction's policy.
  if (!isRecord(options)) {
    throw constructionError(
      "invalid_options",
      "constructPipelineV2RunPlanCandidateFromProposal requires an options object",
      null,
    );
  }
  const runRoot = options["runRoot"];
  const state = options["state"];
  const proposal = options["proposal"] as PipelineV2RunPlanProposal;
  if (!isRecord(ops)) {
    throw constructionError(
      "invalid_options",
      "constructPipelineV2RunPlanCandidateFromProposal requires a read-only loader record",
      null,
    );
  }
  const loadPlanRevision = ops["loadPlanRevision"];
  const loadTaskRevision = ops["loadTaskRevision"];
  if (typeof loadPlanRevision !== "function" || typeof loadTaskRevision !== "function") {
    throw constructionError(
      "invalid_options",
      "the construction requires read-only plan and task revision loaders",
      null,
    );
  }
  if (typeof runRoot !== "string" || runRoot === "") {
    throw constructionError("invalid_options", "the run root must be a non-empty string", null);
  }
  // The narrowed run-root scalar the inner construction paths close over.
  const boundRunRoot: string = runRoot;
  // The proposal provenance gate: the shared registry lookup fires before
  // any proposal field is read, so clones, casts, spreads and Proxies are
  // rejected with their getters and traps never invoked.
  if (!hasPreparedRunPlanProvenance(proposal, "run_plan_proposal")) {
    throw constructionError("invalid_options", PROPOSAL_GATE_MESSAGE, null);
  }
  // The single state validation of the durable state document; the
  // normalized snapshot is independent of the caller's object.
  let validated: PipelineV2RunState;
  try {
    validated = validatePipelineV2RunState(state);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw constructionError(
        "invalid_state",
        "the durable run state is missing or not a valid pipeline v2 run state document",
        null,
      );
    }
    throw cause;
  }
  const failConflict = (message: string): PipelineV2RunPlanConstructionError =>
    constructionError("construction_conflict", message, validated);

  // The run-root binding, before any store load.
  if (basename(runRoot) !== validated.run_id) {
    throw constructionError("run_root_mismatch", "the run root does not belong to this run", validated);
  }

  // The plan's origin: exactly the last durable execution's index. The
  // planning/settled/unbound acceptance boundary is the acceptance
  // controller's policy and is deliberately not re-checked here.
  const lastExecution = validated.executions[validated.executions.length - 1];
  if (lastExecution === undefined) {
    throw constructionError(
      "invalid_state",
      "the durable run state records no execution; a plan proposal has no planning execution to name as the plan's origin",
      validated,
    );
  }
  const originExecution = lastExecution.index;

  // The protected root task: exactly one durable input named `task`,
  // protected, whose digest is the only root-task digest source.
  const rootTaskInputs = validated.inputs.filter((input) => input.id === ROOT_TASK_INPUT_ID);
  if (rootTaskInputs.length !== 1) {
    throw constructionError(
      "invalid_state",
      `the durable run inputs do not carry exactly one input named ${JSON.stringify(ROOT_TASK_INPUT_ID)}; a plan candidate cannot bind its protected root task`,
      validated,
    );
  }
  const rootTaskInput = rootTaskInputs[0]!;
  if (!rootTaskInput.protected) {
    throw constructionError(
      "invalid_state",
      `the durable input named ${JSON.stringify(ROOT_TASK_INPUT_ID)} is not protected; a plan candidate cannot bind an unprotected root task`,
      validated,
    );
  }
  const protectedInputDigest = rootTaskInput.digest;

  // The plan ledger: the authoritative chain the new revision extends.
  const planRecords = validated.plan_revisions;
  const lastPlanRecord: PipelineV2PlanRevisionState | null =
    planRecords.length === 0 ? null : planRecords[planRecords.length - 1]!;

  // The proposal snapshot: frozen prepared arrays read once into locals.
  const proposalStages = proposal.stages;
  const proposalNewTasks = proposal.new_tasks;
  const newTaskById = new Map<string, PipelineV2RunPlanProposalNewTask>();
  for (const entry of proposalNewTasks) {
    newTaskById.set(entry.id, entry);
  }
  // The latest durable revision of every recorded task: the last ledger
  // record of the task id (the loader proved the per-task chain order).
  const latestByTaskId = new Map<string, PipelineV2TaskRevisionState>();
  for (const record of validated.task_revisions) {
    latestByTaskId.set(record.task_id, record);
  }

  // --- the anchored target search (zero loads) --------------------------------
  //
  // The plan acceptance's target is anchored to the origin planning
  // execution: plan records whose `origin_execution` names this
  // construction's origin execution are acceptance evidence of exactly
  // this boundary. Zero matching records mean the acceptance never
  // reached the plan ledger — the target is the next revision. Exactly
  // one matching record means the acceptance of this very execution is
  // already (at least partially) durable — the target is that record's
  // revision, and its immediate predecessor is the base. More than one
  // matching record, or a matching record that is not the last plan
  // revision, is an incoherent ledger (fail closed).
  const matchingOriginPlans = planRecords.filter(
    (plan) => plan.origin_execution === originExecution,
  );
  if (matchingOriginPlans.length > 1) {
    throw constructionError(
      "invalid_state",
      `the durable plan ledger records ${matchingOriginPlans.length} plan revisions for origin execution ${originExecution}; the plan acceptance target of this construction is ambiguous`,
      validated,
    );
  }
  const targetOriginRecord: PipelineV2PlanRevisionState | null =
    matchingOriginPlans.length === 1 ? matchingOriginPlans[0]! : null;
  if (targetOriginRecord !== null && planRecords[planRecords.length - 1] !== targetOriginRecord) {
    throw constructionError(
      "invalid_state",
      `the durable plan record for origin execution ${originExecution} is not the last plan revision; a completed plan acceptance is never superseded inside one construction`,
      validated,
    );
  }
  const basePlanRecord: PipelineV2PlanRevisionState | null =
    targetOriginRecord !== null
      ? planRecords.length >= 2
        ? planRecords[planRecords.length - 2]!
        : null
      : lastPlanRecord;
  const targetPlanRevision: number =
    targetOriginRecord !== null
      ? targetOriginRecord.revision
      : basePlanRecord === null
        ? 1
        : basePlanRecord.revision + 1;
  const previousPlanDigest: string | null = basePlanRecord === null ? null : basePlanRecord.sha256;

  // --- the fresh construction: the previous chain, unchanged -------------------
  //
  // Used only when the target plan artifact is absent, no completed
  // ledger record of this origin execution exists, and the durable task
  // ledger does not intersect `new_tasks` (both closed refusals above).
  // The manifest preparation owns the graph semantics; the loads follow
  // the previous deterministic order.
  async function freshConstruction(): Promise<PreparedPipelineV2RunPlanCandidate> {
    // Pointer resolution (zero loads): every plan task pointer resolves
    // to exactly one of the two allowed sources.
    const resolvedPointers: ResolvedPointer[] = [];
    for (const stage of proposalStages) {
      for (const pointer of stage.tasks) {
        const latest = latestByTaskId.get(pointer.id) ?? null;
        const entry = newTaskById.get(pointer.id) ?? null;
        if (latest !== null && entry !== null) {
          // unreachable: the no-commit-marker intersection refusal above
          // proved the disjointness of the ledger and `new_tasks`; kept
          // fail-closed
          throw failConflict(
            `the plan task pointer ${JSON.stringify(pointer.id)} resolves to both the durable task ledger and a proposed new task`,
          );
        }
        if (latest === null && entry === null) {
          throw failConflict(
            `the plan task pointer ${JSON.stringify(pointer.id)} resolves to neither the durable task ledger nor a proposed new task`,
          );
        }
        resolvedPointers.push({ pointer, latest, entry });
      }
    }

    // Policy rule (zero loads): every new task entry must be referenced by
    // at least one plan pointer — an unused body can never be silently
    // dropped. Repeated pointer ids stay owned by the manifest validator.
    const referencedNewTaskIds = new Set<string>();
    for (const resolved of resolvedPointers) {
      if (resolved.entry !== null) {
        referencedNewTaskIds.add(resolved.entry.id);
      }
    }
    for (const entry of proposalNewTasks) {
      if (!referencedNewTaskIds.has(entry.id)) {
        throw failConflict(
          `the proposal declares new task ${JSON.stringify(entry.id)} which no plan stage references; an unused new task body can never be silently dropped`,
        );
      }
    }

    // The new task revision manifests (bodies only from the proposal) and
    // the plan pointer facts of every resolved pointer: revisions and
    // digests come only from the durable ledger or from the prepared new
    // task manifests.
    const preparedNewTasks = new Map<string, PreparedPipelineV2RunTaskRevision>();
    const pointerFacts = new Map<string, { revision: number; sha256: string }>();
    for (const resolved of resolvedPointers) {
      if (resolved.entry !== null) {
        const prepared = prepareTaskRevisionManifest({
          schema_version: 1,
          kind: "task_revision",
          run_id: validated.run_id,
          task_id: resolved.entry.id,
          revision: 1,
          previous_sha256: null,
          origin: "planning_proposal",
          body: resolved.entry.body,
        });
        preparedNewTasks.set(resolved.entry.id, prepared);
        pointerFacts.set(resolved.entry.id, { revision: 1, sha256: prepared.sha256 });
      } else {
        const latest = resolved.latest!;
        pointerFacts.set(latest.task_id, { revision: latest.revision, sha256: latest.sha256 });
      }
    }

    // The plan revision manifest value: stages, templates, task ids and
    // dependencies only from the proposal; every durable field from the
    // derivation above. The single existing manifest preparer owns the
    // normalization and the graph semantics (and may reject with its own
    // typed error, which passes through unchanged).
    const preparedPlan = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: validated.run_id,
      revision: targetPlanRevision,
      previous_sha256: previousPlanDigest,
      root_task: { input_id: ROOT_TASK_INPUT_ID, sha256: protectedInputDigest },
      origin_execution: originExecution,
      stages: proposalStages.map((stage) => ({
        id: stage.id,
        template: stage.template,
        tasks: stage.tasks.map((pointer) => {
          const facts = pointerFacts.get(pointer.id);
          if (facts === undefined) {
            // unreachable: every pointer was resolved above; kept
            // fail-closed
            throw failConflict(
              `the plan task pointer ${JSON.stringify(pointer.id)} was not resolved before the plan manifest was built`,
            );
          }
          return {
            id: pointer.id,
            revision: facts.revision,
            sha256: facts.sha256,
            depends_on: [...pointer.depends_on],
          };
        }),
      })),
    });

    // Fixed strictly sequential read order; no parallel batch loads. The
    // base plan first, then only the pointered existing tasks in proposal
    // traversal order, each current revision followed by its immediate
    // durable predecessor above revision 1. Orphan artifacts are never
    // read: the ledger revision is the only authority.
    let preparedPreviousPlan: PreparedPipelineV2RunPlanRevision | null = null;
    if (basePlanRecord !== null) {
      const loadedPlan = await loadPlanRevision(boundRunRoot, basePlanRecord.revision);
      preparedPreviousPlan = requireLoadedManifest(
        loadedPlan,
        "plan",
        "plan_revision",
        `the accepted plan revision ${basePlanRecord.revision} manifest is missing from the run plan store`,
        validated,
      ) as PreparedPipelineV2RunPlanRevision;
    }

    const loadedCurrentByTaskId = new Map<string, PreparedPipelineV2RunTaskRevision>();
    const loadedPreviousByTaskId = new Map<string, PreparedPipelineV2RunTaskRevision>();
    for (const resolved of resolvedPointers) {
      const latest = resolved.latest;
      if (latest === null || loadedCurrentByTaskId.has(latest.task_id)) {
        continue;
      }
      const loadedCurrent = await loadTaskRevision(boundRunRoot, latest.task_id, latest.revision);
      loadedCurrentByTaskId.set(
        latest.task_id,
        requireLoadedManifest(
          loadedCurrent,
          "task",
          "task_revision",
          `the task revision ${latest.revision} manifest of task ${JSON.stringify(latest.task_id)} is missing from the run plan store`,
          validated,
        ) as PreparedPipelineV2RunTaskRevision,
      );
      if (latest.revision > 1) {
        const predecessor = validated.task_revisions.find(
          (record) => record.task_id === latest.task_id && record.revision === latest.revision - 1,
        );
        if (predecessor === undefined) {
          // unreachable: the state loader proved the per-task revision
          // chain; kept fail-closed
          throw constructionError(
            "invalid_state",
            `the durable task ledger does not record the immediate predecessor revision ${latest.revision - 1} of task ${JSON.stringify(latest.task_id)}`,
            validated,
          );
        }
        const loadedPrevious = await loadTaskRevision(boundRunRoot, predecessor.task_id, predecessor.revision);
        loadedPreviousByTaskId.set(
          predecessor.task_id,
          requireLoadedManifest(
            loadedPrevious,
            "task",
            "task_revision",
            `the predecessor task revision ${predecessor.revision} manifest of task ${JSON.stringify(predecessor.task_id)} is missing from the run plan store`,
            validated,
          ) as PreparedPipelineV2RunTaskRevision,
        );
      }
    }

    // The candidate inputs in proposal traversal order (the candidate
    // preparation owns the plan's deterministic task order; the caller's
    // order is not semantic).
    const taskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
    const previousTaskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
    for (const resolved of resolvedPointers) {
      if (resolved.entry !== null) {
        taskRevisions.push(preparedNewTasks.get(resolved.entry.id)!);
        continue;
      }
      const latest = resolved.latest!;
      taskRevisions.push(loadedCurrentByTaskId.get(latest.task_id)!);
      const predecessor = loadedPreviousByTaskId.get(latest.task_id);
      if (predecessor !== undefined) {
        previousTaskRevisions.push(predecessor);
      }
    }

    // The existing candidate preparation is the single verifier of the
    // loaded prepared objects (provenance, run/task/revision/digest/chain
    // correspondence); the construction returns its exact result, never a
    // copy or an envelope.
    return preparePipelineV2RunPlanCandidate({
      plan: preparedPlan,
      taskRevisions,
      previousPlan: preparedPreviousPlan,
      previousTaskRevisions,
      protectedInputDigest,
    });
  }

  // --- the anchored retry construction -----------------------------------------
  //
  // The published target plan artifact is the single retry anchor. The
  // anchor binding is verified through the existing preparers and the
  // candidate preparation's own validators: the loader binds the artifact
  // to the requested revision and the run id; the origin execution and
  // the base predecessor digest are bound here; the root-task binding and
  // the exact task revision/digest bindings are verified by the existing
  // candidate preparation; the stage/template/task/dependency projection
  // of the proposal is bound here against the artifact.
  async function anchoredRetryConstruction(
    loadedTarget: unknown,
  ): Promise<PreparedPipelineV2RunPlanCandidate> {
    const targetPrepared = requireLoadedManifest(
      loadedTarget,
      "plan",
      "plan_revision",
      `the target plan revision ${targetPlanRevision} manifest is missing from the run plan store`,
      validated,
    ) as PreparedPipelineV2RunPlanRevision;
    const targetManifest = targetPrepared.manifest;

    // The exact anchor binding: the origin execution and the exact base
    // predecessor digest. An artifact with foreign values is never a
    // retry anchor.
    if (targetManifest.origin_execution !== originExecution) {
      throw failConflict(
        "the target plan artifact carries a foreign origin execution; an orphan plan artifact is never accepted as the retry anchor",
      );
    }
    if (targetManifest.previous_sha256 !== previousPlanDigest) {
      throw failConflict(
        "the target plan artifact carries a different predecessor digest; an orphan plan artifact is never accepted as the retry anchor",
      );
    }

    // The normalized stage/template/task/dependency projection of the
    // proposal must be exactly the target artifact's projection: same
    // stages in the semantic order, same templates, same task ids in the
    // normalized order, same dependencies in the normalized order.
    if (targetManifest.stages.length !== proposalStages.length) {
      throw failConflict(
        "the target plan artifact carries a different stage projection than the proposal",
      );
    }
    const byId = (a: { id: string }, b: { id: string }): number => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
    for (let stageIndex = 0; stageIndex < targetManifest.stages.length; stageIndex += 1) {
      const artifactStage = targetManifest.stages[stageIndex]!;
      const proposalStage = proposalStages[stageIndex]!;
      if (artifactStage.id !== proposalStage.id || artifactStage.template !== proposalStage.template) {
        throw failConflict(
          "the target plan artifact carries a different stage projection than the proposal",
        );
      }
      const artifactTasks = [...artifactStage.tasks].sort(byId);
      const proposalTasks = [...proposalStage.tasks].sort(byId);
      if (artifactTasks.length !== proposalTasks.length) {
        throw failConflict(
          "the target plan artifact carries a different task projection than the proposal",
        );
      }
      for (let taskIndex = 0; taskIndex < artifactTasks.length; taskIndex += 1) {
        const artifactPointer = artifactTasks[taskIndex]!;
        const proposalPointer = proposalTasks[taskIndex]!;
        if (artifactPointer.id !== proposalPointer.id) {
          throw failConflict(
            "the target plan artifact carries a different task projection than the proposal",
          );
        }
        const artifactDeps = [...artifactPointer.depends_on].sort();
        const proposalDeps = [...proposalPointer.depends_on].sort();
        if (
          artifactDeps.length !== proposalDeps.length ||
          artifactDeps.some((dep, index) => dep !== proposalDeps[index])
        ) {
          throw failConflict(
            "the target plan artifact carries a different dependency projection than the proposal",
          );
        }
      }
    }

    // Every proposed new task entry must be referenced by the target
    // artifact's pointers (the anchored rule 2).
    const referencedNewTaskIds = new Set<string>();
    for (const stage of targetManifest.stages) {
      for (const pointer of stage.tasks) {
        if (newTaskById.has(pointer.id)) {
          referencedNewTaskIds.add(pointer.id);
        }
      }
    }
    for (const entry of proposalNewTasks) {
      if (!referencedNewTaskIds.has(entry.id)) {
        throw failConflict(
          `the proposal declares new task ${JSON.stringify(entry.id)} which the target plan artifact does not reference; an unused new task body can never be silently dropped`,
        );
      }
    }

    // Fixed anchored load order: the base predecessor plan first, then
    // the target tasks in the artifact's semantic order, each current
    // revision followed by its immediate durable predecessor above
    // revision 1.
    let preparedBasePlan: PreparedPipelineV2RunPlanRevision | null = null;
    if (basePlanRecord !== null) {
      const loadedBase = await loadPlanRevision(boundRunRoot, basePlanRecord.revision);
      preparedBasePlan = requireLoadedManifest(
        loadedBase,
        "plan",
        "plan_revision",
        `the accepted plan revision ${basePlanRecord.revision} manifest is missing from the run plan store`,
        validated,
      ) as PreparedPipelineV2RunPlanRevision;
    }

    const taskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
    const previousTaskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
    for (const stage of targetManifest.stages) {
      for (const pointer of stage.tasks) {
        const entry = newTaskById.get(pointer.id) ?? null;
        if (entry !== null) {
          // A proposed new task: re-prepare the expected revision-1
          // manifest from the proposal body and bind the target pointer,
          // the artifact and a possible durable ledger record to it
          // exactly; a mismatch is a conflict, never a rewrite.
          const expected = prepareTaskRevisionManifest({
            schema_version: 1,
            kind: "task_revision",
            run_id: validated.run_id,
            task_id: entry.id,
            revision: 1,
            previous_sha256: null,
            origin: "planning_proposal",
            body: entry.body,
          });
          if (pointer.sha256 !== expected.sha256) {
            throw failConflict(
              `the target plan artifact's task digest of ${JSON.stringify(entry.id)} differs from the restored proposal's new task body; a task body can never be silently replaced`,
            );
          }
          if (pointer.revision !== 1) {
            throw failConflict(
              `the target plan artifact points new task ${JSON.stringify(entry.id)} at revision ${pointer.revision}; a proposed new task is always revision 1`,
            );
          }
          const loadedTask = await loadTaskRevision(boundRunRoot, entry.id, pointer.revision);
          const taskPrepared = requireLoadedManifest(
            loadedTask,
            "task",
            "task_revision",
            `the task revision ${pointer.revision} manifest of task ${JSON.stringify(entry.id)} is missing from the run plan store`,
            validated,
          ) as PreparedPipelineV2RunTaskRevision;
          if (taskPrepared.sha256 !== expected.sha256) {
            throw failConflict(
              `the published task artifact of ${JSON.stringify(entry.id)} differs from the restored proposal's new task body; a task body can never be silently replaced`,
            );
          }
          const durableFirst = validated.task_revisions.find(
            (record) => record.task_id === entry.id && record.revision === 1,
          );
          if (durableFirst !== undefined) {
            if (
              durableFirst.revision !== 1 ||
              durableFirst.sha256 !== expected.sha256 ||
              durableFirst.previous_sha256 !== null
            ) {
              throw failConflict(
                `the durable task record of ${JSON.stringify(entry.id)} does not match the restored proposal's new task revision; a durable record is never rewritten`,
              );
            }
          }
          taskRevisions.push(taskPrepared);
          continue;
        }
        // A pointered existing task: resolved only in the durable ledger,
        // exactly at the target artifact's pointer revision.
        const durable = validated.task_revisions.find(
          (record) => record.task_id === pointer.id && record.revision === pointer.revision,
        );
        if (durable === undefined) {
          throw failConflict(
            `the target plan artifact points at revision ${pointer.revision} of task ${JSON.stringify(pointer.id)}, which the durable task ledger does not record`,
          );
        }
        if (durable.sha256 !== pointer.sha256) {
          throw failConflict(
            `the durable task record of ${JSON.stringify(pointer.id)} does not match the target plan artifact's pointer; a task revision is never rewritten`,
          );
        }
        const loadedCurrent = await loadTaskRevision(boundRunRoot, pointer.id, pointer.revision);
        taskRevisions.push(
          requireLoadedManifest(
            loadedCurrent,
            "task",
            "task_revision",
            `the task revision ${pointer.revision} manifest of task ${JSON.stringify(pointer.id)} is missing from the run plan store`,
            validated,
          ) as PreparedPipelineV2RunTaskRevision,
        );
        if (pointer.revision > 1) {
          const predecessor = validated.task_revisions.find(
            (record) => record.task_id === pointer.id && record.revision === pointer.revision - 1,
          );
          if (predecessor === undefined) {
            // unreachable: the state loader proved the per-task revision
            // chain; kept fail-closed
            throw constructionError(
              "invalid_state",
              `the durable task ledger does not record the immediate predecessor revision ${pointer.revision - 1} of task ${JSON.stringify(pointer.id)}`,
              validated,
            );
          }
          const loadedPrevious = await loadTaskRevision(boundRunRoot, predecessor.task_id, predecessor.revision);
          previousTaskRevisions.push(
            requireLoadedManifest(
              loadedPrevious,
              "task",
              "task_revision",
              `the predecessor task revision ${predecessor.revision} manifest of task ${JSON.stringify(predecessor.task_id)} is missing from the run plan store`,
              validated,
            ) as PreparedPipelineV2RunTaskRevision,
          );
        }
      }
    }

    // The existing candidate preparation is the single verifier of the
    // loaded prepared objects (provenance, run/task/revision/digest/chain
    // correspondence); the construction returns its exact result, never a
    // copy or an envelope.
    return preparePipelineV2RunPlanCandidate({
      plan: targetPrepared,
      taskRevisions,
      previousPlan: preparedBasePlan,
      previousTaskRevisions,
      protectedInputDigest,
    });
  }

  // The FIRST store load: the target plan artifact by the computed
  // revision. The published target plan artifact is the single retry
  // anchor of the anchored path; its absence decides between the fresh
  // construction and the closed refusals below. The loader binds the
  // artifact to the requested revision and the run id; nothing here
  // writes.
  const loadedTargetPlan = await loadPlanRevision(boundRunRoot, targetPlanRevision);

  if (loadedTargetPlan === null) {
    // A completed ledger record without its artifact: the acceptance is
    // durable but the commit-marker is gone — fail closed, never
    // reconstruct heuristically.
    if (targetOriginRecord !== null) {
      throw constructionError(
        "artifact_missing",
        `the accepted plan revision ${targetPlanRevision} manifest is missing from the run plan store; the durable plan record of this origin execution has no artifact to reconstruct the acceptance from`,
        validated,
      );
    }
    // Durable task evidence for proposed new tasks without the plan
    // commit-marker: partial ownership is never recognized without the
    // artifact.
    const intersecting = proposalNewTasks.find((entry) => latestByTaskId.has(entry.id));
    if (intersecting !== undefined) {
      throw constructionError(
        "artifact_missing",
        `the durable task ledger already records task ${JSON.stringify(intersecting.id)} proposed as new, but no target plan commit-marker exists; partial acceptance evidence without the plan artifact is never completed heuristically`,
        validated,
      );
    }
    return await freshConstruction();
  }

  return await anchoredRetryConstruction(loadedTargetPlan);
}
