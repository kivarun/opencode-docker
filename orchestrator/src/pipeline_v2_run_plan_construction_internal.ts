/**
 * Internal core of the read-only proposal → candidate construction layer
 * for pipeline schema v2 (production-neutral, unwired).
 *
 * This module is the single layer that builds one provenance-backed
 * `PreparedPipelineV2RunPlanCandidate` from an agent-authored run plan
 * proposal and the durable run state, using only the existing manifest,
 * store and candidate chains:
 *
 *   validated durable state → authoritative derivation
 *   → the fixed construction-policy rules (zero store loads)
 *   → exactly one resolution per plan task pointer
 *   → the new task revision manifests (bodies only from the proposal)
 *   → the plan revision manifest value (revisions, digests, root task,
 *     origin execution only from the durable ledgers/inputs)
 *   → strictly sequential read-only store loads (the previous plan, then
 *     only the pointered existing tasks in proposal traversal order, each
 *     current revision followed by its immediate durable predecessor)
 *   → the existing `preparePipelineV2RunPlanCandidate` (provenance, run/
 *     task/revision/digest/chain correspondence) returned directly, never
 *     wrapped or copied.
 *
 * The layer is strictly read-only: no publication, no durable dispatch, no
 * reducer call, no sink, no coordinator/runner/CLI, and no second parser,
 * serializer, digest builder or provenance registry. Loaded prepared plan
 * and task objects are opaque; their provenance and every identity and
 * chain correspondence is verified by the existing candidate preparation —
 * never re-verified here. This module checks only the safe form of a
 * loader-result wrapper (so a hostile injected result can never produce a
 * `TypeError`); everything beyond that shape is delegated. Orphan
 * artifacts never select a revision: the durable ledgers are the only
 * authority, and the filesystem is read only at the exact revisions the
 * ledgers name.
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
 * - the next plan revision: `1` on an empty plan ledger, else the last
 *   durable plan record's revision + 1; `previous_sha256` is `null` or
 *   that record's digest;
 * - an existing task: the latest durable revision of its id (the last
 *   ledger record of that task); its predecessor above revision 1 is the
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
 * 1. every `new_tasks[].id` must be absent from the whole durable task
 *    ledger — the intersection is a typed construction conflict (a new
 *    task body can never silently replace a recorded task);
 * 2. every `new_tasks` entry must be referenced by at least one plan task
 *    pointer — an unused entry is a typed construction conflict (a task
 *    body can never be silently dropped); repeated plan task ids remain
 *    owned by the manifest validator, never by a second graph validator;
 * 3. every plan task pointer must resolve to exactly one source — the
 *    latest durable task revision or the exact `new_tasks` entry; neither
 *    or both is a typed construction conflict.
 *
 * Failure contract (typed, classified by context — never by message
 * text): `invalid_options` (options/ops shape, the run-root scalar, an
 * untrusted proposal), `invalid_state` (the state validation, no durable
 * execution, the root-task input binding), `run_root_mismatch` (the
 * run-root basename binding), `construction_conflict` (the three policy
 * rules), `artifact_missing` (a required ledger-named artifact is absent)
 * and `malformed_loader_result` (a hostile injected loader result that is
 * not a safe published-manifest wrapper). Store, manifest and binding
 * typed errors pass through unchanged by object identity; unexpected
 * errors propagate unchanged. The error carries the last validated
 * durable state (`null` before it exists). Diagnostics are content-free:
 * they never echo task bodies, run-root paths, raw JSON, parser fragments
 * or arbitrary loader values — safe ids, revisions and counts only.
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
  const lastPlanRecord: PipelineV2PlanRevisionState | null =
    validated.plan_revisions.length === 0
      ? null
      : validated.plan_revisions[validated.plan_revisions.length - 1]!;
  const nextPlanRevision = lastPlanRecord === null ? 1 : lastPlanRecord.revision + 1;
  const previousPlanDigest: string | null = lastPlanRecord === null ? null : lastPlanRecord.sha256;

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

  // Policy rule 1 (zero loads): a new task id must be absent from the
  // whole durable task ledger — the body can never silently replace a
  // recorded task.
  for (const entry of proposalNewTasks) {
    if (latestByTaskId.has(entry.id)) {
      throw failConflict(
        `the proposal declares new task ${JSON.stringify(entry.id)}, which the durable task ledger already records; a new task body can never silently replace a recorded task`,
      );
    }
  }

  // Pointer resolution (zero loads): every plan task pointer resolves to
  // exactly one of the two allowed sources.
  const resolvedPointers: ResolvedPointer[] = [];
  for (const stage of proposalStages) {
    for (const pointer of stage.tasks) {
      const latest = latestByTaskId.get(pointer.id) ?? null;
      const entry = newTaskById.get(pointer.id) ?? null;
      if (latest !== null && entry !== null) {
        // unreachable after rule 1; kept fail-closed
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

  // Policy rule 2 (zero loads): every new task entry must be referenced by
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
    revision: nextPlanRevision,
    previous_sha256: previousPlanDigest,
    root_task: { input_id: ROOT_TASK_INPUT_ID, sha256: protectedInputDigest },
    origin_execution: originExecution,
    stages: proposalStages.map((stage) => ({
      id: stage.id,
      template: stage.template,
      tasks: stage.tasks.map((pointer) => {
        const facts = pointerFacts.get(pointer.id);
        if (facts === undefined) {
          // unreachable: every pointer was resolved above; kept fail-closed
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
  // previous plan first, then only the pointered existing tasks in
  // proposal traversal order, each current revision followed by its
  // immediate durable predecessor above revision 1. Orphan artifacts are
  // never read: the ledger revision is the only authority.
  let preparedPreviousPlan: PreparedPipelineV2RunPlanRevision | null = null;
  if (lastPlanRecord !== null) {
    const loadedPlan = await loadPlanRevision(runRoot, lastPlanRecord.revision);
    preparedPreviousPlan = requireLoadedManifest(
      loadedPlan,
      "plan",
      "plan_revision",
      `the accepted plan revision ${lastPlanRecord.revision} manifest is missing from the run plan store`,
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
    const loadedCurrent = await loadTaskRevision(runRoot, latest.task_id, latest.revision);
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
        // unreachable: the state loader proved the per-task revision chain;
        // kept fail-closed
        throw constructionError(
          "invalid_state",
          `the durable task ledger does not record the immediate predecessor revision ${latest.revision - 1} of task ${JSON.stringify(latest.task_id)}`,
          validated,
        );
      }
      const loadedPrevious = await loadTaskRevision(runRoot, predecessor.task_id, predecessor.revision);
      loadedPreviousByTaskId.set(
        latest.task_id,
        requireLoadedManifest(
          loadedPrevious,
          "task",
          "task_revision",
          `the predecessor task revision ${predecessor.revision} manifest of task ${JSON.stringify(latest.task_id)} is missing from the run plan store`,
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
