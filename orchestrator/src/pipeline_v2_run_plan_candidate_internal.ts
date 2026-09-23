import { PipelineV2RunPlanBindingError, validatePlanRevisionChain, validatePlanTaskBindings, validateRootTaskBinding, validateTaskRevisionChain } from "./pipeline_v2_run_plan_bindings.ts";
import {
  PipelineV2RunPlanStoreError,
  publishPipelineV2PlanRevision,
  publishPipelineV2TaskRevision,
} from "./pipeline_v2_run_plan_store.ts";
import {
  hasPreparedRunPlanProvenance,
  type PipelineV2RunPlanProvenanceKind,
} from "./pipeline_v2_run_plan_provenance.ts";
import { deepFreezeValue } from "./pipeline_v2_immutable_document_store_internal.ts";
import type {
  PreparedPipelineV2RunPlanCandidate,
  PreparePipelineV2RunPlanCandidateOptions,
} from "./pipeline_v2_run_plan_candidate.ts";
import type {
  PreparedPipelineV2RunPlanRevision,
  PreparedPipelineV2RunTaskRevision,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Explicitly internal core of the pipeline v2 run plan candidate layer
 * (unwired, production-neutral).
 *
 * A run plan candidate is one coherent publication unit: one prepared plan
 * revision plus exactly the prepared task revisions its task pointers
 * declare, in the plan's deterministic order. This module owns:
 *
 * - the module-private provenance registry for prepared candidates
 *   (`WeakSet`; registration only by the preparation core immediately
 *   before a successful return, membership check before any field is
 *   read) — it never shares or replaces the manifest registry of
 *   `pipeline_v2_run_plan_provenance.ts`;
 * - the preparation core: capture → provenance gates → the binding
 *   validators → the task predecessor set → the canonical task order →
 *   deep-freeze → registration. It performs no filesystem or store
 *   operation;
 * - the publication core with a per-call publication ops capability: the
 *   task revisions are published first, strictly sequentially (never a
 *   joined parallel batch), and the plan revision is published only after every
 *   task publication has succeeded — the plan artifact is the filesystem
 *   commit marker of the whole candidate. The published store results are
 *   structurally verified against the candidate (defends against hostile
 *   injected ops; unreachable with the production store);
 * - the single frozen production ops object; the public wrapper always
 *   calls the publication core with it. There is no mutable module-global
 *   ops, no installer and no setter, so an injected call can never change
 *   a parallel production call.
 *
 * Error classes stay with their owners: manifest failures remain
 * `PipelineV2RunPlanManifestError`, consistency/provenance failures
 * remain `PipelineV2RunPlanBindingError`, filesystem failures remain
 * `PipelineV2RunPlanStoreError` — never re-tagged by message text. The
 * candidate layer never touches the durable state, never dispatches a
 * reducer command and never declares a candidate accepted: a published
 * plan artifact alone is not durable acceptance; a partial failure leaves
 * immutable orphan task artifacts that nothing here removes, and an exact
 * retry safely reuses them through the store's idempotent adoption.
 *
 * Trust boundary: every manifest argument must be the exact deep-frozen
 * prepared object the manifest module returned, verified through the
 * shared provenance registry before any field is read; the candidate
 * argument must be the exact prepared object this module's preparation
 * core returned, verified through the candidate registry before
 * `candidate.plan`/`candidate.task_revisions` are read. Hand-built
 * look-alikes, casts, spreads, `structuredClone` results and Proxies are
 * rejected with the argument's getters and Proxy traps never invoked.
 * Caller objects are never frozen or modified, and no diagnostic ever
 * echoes a task body, raw JSON or a caller path.
 */

export interface PipelineV2RunPlanCandidatePublicationOps {
  readonly publishTaskRevision: typeof publishPipelineV2TaskRevision;
  readonly publishPlanRevision: typeof publishPipelineV2PlanRevision;
}

/**
 * The single frozen production publication ops object, bound to the
 * public run-plan store functions. Never reassigned, never exported as a
 * mutable seam.
 */
export const realPipelineV2RunPlanCandidatePublicationOps: PipelineV2RunPlanCandidatePublicationOps =
  Object.freeze({
    publishTaskRevision: publishPipelineV2TaskRevision,
    publishPlanRevision: publishPipelineV2PlanRevision,
  });

const preparedRunPlanCandidates = new WeakSet<object>();

function registerPreparedRunPlanCandidate(value: object): void {
  preparedRunPlanCandidates.add(value);
}

/**
 * Pure candidate registry lookup: the argument must be the exact prepared
 * candidate this module returned. Getters and Proxy traps of the argument
 * are never invoked.
 */
export function hasPreparedRunPlanCandidateProvenance(value: unknown): boolean {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return preparedRunPlanCandidates.has(value);
}

const UNTRUSTED_MANIFEST_MESSAGE =
  "the operation requires the frozen prepared run plan object returned by " +
  "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
  "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
  "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
  "hand-built objects, casts, clones and Proxies are rejected before any field is read";

const UNTRUSTED_CANDIDATE_MESSAGE =
  "the operation requires the frozen prepared run plan candidate returned by " +
  "preparePipelineV2RunPlanCandidate; hand-built objects, casts, clones and " +
  "Proxies are rejected before any field is read";

function requireManifestProvenance(value: unknown, kind: PipelineV2RunPlanProvenanceKind): void {
  if (!hasPreparedRunPlanProvenance(value, kind)) {
    throw new PipelineV2RunPlanBindingError(UNTRUSTED_MANIFEST_MESSAGE);
  }
}

function requirePreparedCandidate(value: unknown): void {
  if (!hasPreparedRunPlanCandidateProvenance(value)) {
    throw new PipelineV2RunPlanBindingError(UNTRUSTED_CANDIDATE_MESSAGE);
  }
}

/**
 * The preparation core: validates the whole candidate before any
 * filesystem side effect (there are none here) and returns the
 * deep-frozen candidate in the plan's deterministic task order.
 *
 * Fixed order: provenance gates (plan → both task arrays are arrays →
 * every current task revision → previous plan → every previous task
 * revision) → `validateRootTaskBinding` → `validatePlanRevisionChain` →
 * `validatePlanTaskBindings` → the task predecessor set → the canonical
 * task order → deep-freeze → registration → return.
 */
export function preparePipelineV2RunPlanCandidateCore(
  plan: PreparedPipelineV2RunPlanRevision,
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  previousPlan: PreparedPipelineV2RunPlanRevision | null,
  previousTaskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  protectedInputDigest: string,
): PreparedPipelineV2RunPlanCandidate {
  requireManifestProvenance(plan, "plan_revision");
  if (!Array.isArray(taskRevisions)) {
    throw new PipelineV2RunPlanBindingError(
      "preparePipelineV2RunPlanCandidate requires an array of prepared task revisions",
    );
  }
  if (!Array.isArray(previousTaskRevisions)) {
    throw new PipelineV2RunPlanBindingError(
      "preparePipelineV2RunPlanCandidate requires an array of prepared previous task revisions",
    );
  }
  for (const task of taskRevisions) {
    requireManifestProvenance(task, "task_revision");
  }
  if (previousPlan !== null) {
    requireManifestProvenance(previousPlan, "plan_revision");
  }
  for (const previous of previousTaskRevisions) {
    requireManifestProvenance(previous, "task_revision");
  }
  validateRootTaskBinding({ plan, protectedInputDigest });
  validatePlanRevisionChain({ previous: previousPlan, current: plan });
  validatePlanTaskBindings({ plan, taskRevisions });
  const currentByTaskId = new Map<string, PreparedPipelineV2RunTaskRevision>();
  for (const task of taskRevisions) {
    currentByTaskId.set(task.manifest.task_id, task);
  }
  const previousByTaskId = new Map<string, PreparedPipelineV2RunTaskRevision>();
  for (const previous of previousTaskRevisions) {
    const taskId = previous.manifest.task_id;
    if (previousByTaskId.has(taskId)) {
      throw new PipelineV2RunPlanBindingError(
        `preparePipelineV2RunPlanCandidate carries a duplicate predecessor for task ${JSON.stringify(taskId)}`,
      );
    }
    if (!currentByTaskId.has(taskId)) {
      throw new PipelineV2RunPlanBindingError(
        `preparePipelineV2RunPlanCandidate carries a predecessor for task ${JSON.stringify(taskId)} which the candidate does not declare`,
      );
    }
    previousByTaskId.set(taskId, previous);
  }
  const orderedTasks: PreparedPipelineV2RunTaskRevision[] = [];
  for (const stage of plan.manifest.stages) {
    for (const pointer of stage.tasks) {
      const current = currentByTaskId.get(pointer.id);
      if (current === undefined) {
        // unreachable: validatePlanTaskBindings proved exact pointer
        // coverage; kept fail-closed
        throw new PipelineV2RunPlanBindingError(
          `preparePipelineV2RunPlanCandidate is missing the task revision for plan task ${JSON.stringify(pointer.id)}`,
        );
      }
      const previous = previousByTaskId.get(pointer.id) ?? null;
      if (current.manifest.revision === 1) {
        if (previous !== null) {
          throw new PipelineV2RunPlanBindingError(
            `preparePipelineV2RunPlanCandidate carries a predecessor for revision 1 task ${JSON.stringify(pointer.id)}`,
          );
        }
        validateTaskRevisionChain({ previous: null, current });
      } else {
        if (previous === null) {
          throw new PipelineV2RunPlanBindingError(
            `preparePipelineV2RunPlanCandidate is missing the predecessor for task ${JSON.stringify(pointer.id)}`,
          );
        }
        validateTaskRevisionChain({ previous, current });
      }
      orderedTasks.push(current);
    }
  }
  const candidate: PreparedPipelineV2RunPlanCandidate = deepFreezeValue({
    plan,
    task_revisions: orderedTasks,
  });
  registerPreparedRunPlanCandidate(candidate);
  return candidate;
}

/**
 * Structural check of a hostile or broken store result against the exact
 * candidate task: the publication must be a task revision result whose
 * manifest kind, run id, task id, revision, canonical JSON and digest
 * all match. Classification is by typed structure, never by message
 * text; unreachable with the production store (it re-prepares the same
 * manifest deterministically).
 */
function publishedTaskMatchesCandidate(
  published: unknown,
  task: PreparedPipelineV2RunTaskRevision,
): boolean {
  if (typeof published !== "object" || published === null) {
    return false;
  }
  const prepared = (published as { task?: unknown }).task;
  if (typeof prepared !== "object" || prepared === null) {
    return false;
  }
  const manifest = (prepared as { manifest?: unknown }).manifest;
  if (typeof manifest !== "object" || manifest === null) {
    return false;
  }
  const manifestFields = manifest as {
    kind?: unknown;
    run_id?: unknown;
    task_id?: unknown;
    revision?: unknown;
  };
  const preparedFields = prepared as { canonical_json?: unknown; sha256?: unknown };
  return (
    manifestFields.kind === "task_revision" &&
    manifestFields.run_id === task.manifest.run_id &&
    manifestFields.task_id === task.manifest.task_id &&
    manifestFields.revision === task.manifest.revision &&
    preparedFields.canonical_json === task.canonical_json &&
    preparedFields.sha256 === task.sha256
  );
}

/** Plan counterpart of `publishedTaskMatchesCandidate`. */
function publishedPlanMatchesCandidate(
  published: unknown,
  plan: PreparedPipelineV2RunPlanRevision,
): boolean {
  if (typeof published !== "object" || published === null) {
    return false;
  }
  const prepared = (published as { plan?: unknown }).plan;
  if (typeof prepared !== "object" || prepared === null) {
    return false;
  }
  const manifest = (prepared as { manifest?: unknown }).manifest;
  if (typeof manifest !== "object" || manifest === null) {
    return false;
  }
  const manifestFields = manifest as { kind?: unknown; run_id?: unknown; revision?: unknown };
  const preparedFields = prepared as { canonical_json?: unknown; sha256?: unknown };
  return (
    manifestFields.kind === "plan_revision" &&
    manifestFields.run_id === plan.manifest.run_id &&
    manifestFields.revision === plan.manifest.revision &&
    preparedFields.canonical_json === plan.canonical_json &&
    preparedFields.sha256 === plan.sha256
  );
}

/**
 * The publication core with a per-call ops capability: publish every task
 * revision first, strictly sequentially in candidate order, then the plan
 * revision — never a joined parallel batch, and the plan publication never starts
 * before the last task publication has succeeded. The plan artifact is
 * the filesystem commit marker of the whole candidate; a failure before
 * it leaves the earlier task artifacts as immutable orphans and the
 * caller retries with a new call (no retry loop here, no cleanup, no
 * rollback, no durable dispatch). The original store errors pass through
 * unchanged; a structurally inconsistent store result (hostile injected
 * ops) is a failed publication.
 */
export async function publishPipelineV2RunPlanCandidateWithOps(
  ops: PipelineV2RunPlanCandidatePublicationOps,
  runRoot: string,
  candidate: PreparedPipelineV2RunPlanCandidate,
): Promise<PreparedPipelineV2RunPlanCandidate> {
  requirePreparedCandidate(candidate);
  if (
    typeof ops !== "object" ||
    ops === null ||
    typeof (ops as { publishTaskRevision?: unknown }).publishTaskRevision !== "function" ||
    typeof (ops as { publishPlanRevision?: unknown }).publishPlanRevision !== "function"
  ) {
    throw new TypeError(
      "the run plan candidate publication requires publishTaskRevision and publishPlanRevision functions",
    );
  }
  const publishTaskRevision = ops.publishTaskRevision;
  const publishPlanRevision = ops.publishPlanRevision;
  const plan = candidate.plan;
  const taskRevisions = candidate.task_revisions;
  for (const task of taskRevisions) {
    const published: unknown = await publishTaskRevision(runRoot, task.manifest);
    if (!publishedTaskMatchesCandidate(published, task)) {
      throw new PipelineV2RunPlanStoreError(
        "not_published",
        "io_failure",
        "the published task revision manifest does not match the candidate task revision",
      );
    }
  }
  const publishedPlan: unknown = await publishPlanRevision(runRoot, plan.manifest);
  if (!publishedPlanMatchesCandidate(publishedPlan, plan)) {
    throw new PipelineV2RunPlanStoreError(
      "not_published",
      "io_failure",
      "the published plan revision manifest does not match the candidate plan revision",
    );
  }
  return candidate;
}
