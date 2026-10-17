/**
 * Pure binding validators for pipeline v2 run plan manifests (unwired).
 *
 * This module is the only cross-manifest consistency layer between the
 * prepared run plan objects of `pipeline_v2_run_plan_manifests.ts` and the
 * surrounding run facts a future policy/controller layer will bind:
 *
 * - `validatePlanTaskBindings`: every plan task pointer is covered by
 *   exactly one passed task revision (no missing, extra or duplicate
 *   revisions) and each revision matches the pointer by `task_id`,
 *   `revision` and `sha256`;
 * - `validatePlanRevisionChain`: the successor relationship between two
 *   prepared plan revisions (first revision, exact revision number
 *   increment, digest chaining and the unchanged protected root-task
 *   binding — stage/task content may change freely between revisions);
 * - `validateTaskRevisionChain`: the successor relationship between two
 *   prepared task revisions (first revision, exact revision number
 *   increment, digest chaining, same run id and same task id);
 * - `validateRootTaskBinding`: exact equality of the plan's root task
 *   digest with a digest of the protected input the caller already
 *   computed (the input object or its body is never received);
 * - `validateContinueIntentBinding` / `validateReviseIntentBinding`:
 *   wait-intent to manifest bindings (run id, expected digests, declared
 *   stage or task, candidate chain and the `user_response` origin of a
 *   candidate task revision).
 *
 * Trust boundary: every manifest argument must be the exact deep-frozen
 * prepared object the manifest module returned, verified through the
 * shared module-private provenance registry
 * (`pipeline_v2_run_plan_provenance.ts`) before any field is read. Hand-
 * built look-alikes, casts, spreads, `structuredClone` results and
 * Proxies are rejected with the argument's getters and Proxy traps never
 * invoked. Caller objects are never frozen or modified, and no argument
 * value is ever echoed in a diagnostic.
 *
 * Everything here is pure: no filesystem, no reducer, no state schema, no
 * policy (a template-specific maximum iteration grant is deliberately
 * out of scope) and no wiring. The single non-pure input a validator
 * receives is a content-free SHA-256 digest string.
 */
import {
  PipelineV2RunPlanManifestError,
  type PipelineV2ContinueStageIntentManifest,
  type PipelineV2ReviseTaskIntentManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
  type PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";
import { hasPreparedRunPlanProvenance } from "./pipeline_v2_run_plan_provenance.ts";

export class PipelineV2RunPlanBindingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2RunPlanBindingError";
  }
}

const UNTRUSTED_MANIFEST_MESSAGE =
  "the operation requires the frozen prepared run plan object returned by " +
  "preparePlanRevisionManifest/parsePlanRevisionManifest, " +
  "prepareTaskRevisionManifest/parseTaskRevisionManifest or " +
  "prepareWaitIntent/parseWaitIntent for the same manifest kind; " +
  "hand-built objects, casts, clones and Proxies are rejected before any field is read";

function raise(message: string): never {
  throw new PipelineV2RunPlanBindingError(message);
}

/**
 * Registry gate for one prepared manifest argument: verified through the
 * shared provenance registry before any field is read, so getters and
 * Proxy traps of a forged object are never invoked.
 */
function requireProvenance(value: unknown, kind: Parameters<typeof hasPreparedRunPlanProvenance>[1]): void {
  if (!hasPreparedRunPlanProvenance(value, kind)) {
    throw new PipelineV2RunPlanBindingError(UNTRUSTED_MANIFEST_MESSAGE);
  }
}

function expectSha256Digest(value: string, what: string): string {
  if (typeof value !== "string" || value.length !== 64 || !/^[0-9a-f]+$/.test(value)) {
    throw new PipelineV2RunPlanBindingError(`${what} must be a lowercase hex SHA-256 digest`);
  }
  return value;
}

/**
 * Validates that the passed task revisions bind exactly to the plan's
 * task pointers: one revision per pointer, no missing, extra or duplicate
 * revisions, and identity equality on `task_id`, `revision` and
 * `sha256`. The task pointers of all stages are addressed by their global
 * task ids; stage boundaries play no role beyond id uniqueness, which the
 * manifest compiler already enforced.
 */
export function validatePlanTaskBindings({
  plan,
  taskRevisions,
}: {
  plan: PreparedPipelineV2RunPlanRevision;
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[];
}): void {
  requireProvenance(plan, "plan_revision");
  if (!Array.isArray(taskRevisions)) {
    raise("validatePlanTaskBindings requires an array of prepared task revisions");
  }
  const pointers = new Map<string, { runId: string; revision: number; sha256: string }>();
  for (const stage of plan.manifest.stages) {
    for (const task of stage.tasks) {
      pointers.set(task.id, { runId: plan.manifest.run_id, revision: task.revision, sha256: task.sha256 });
    }
  }
  const covered = new Map<string, PreparedPipelineV2RunTaskRevision>();
  for (let index = 0; index < taskRevisions.length; index += 1) {
    const revision = taskRevisions[index];
    if (revision === undefined) {
      continue;
    }
    requireProvenance(revision, "task_revision");
    const taskId = revision.manifest.task_id;
    const pointer = pointers.get(taskId);
    if (pointer === undefined || revision.manifest.run_id !== pointer.runId) {
      raise(
        `validatePlanTaskBindings carries a task revision the plan does not declare at position ${index}`,
      );
    }
    if (covered.has(taskId)) {
      raise(`validatePlanTaskBindings carries a duplicate task revision at position ${index}`);
    }
    covered.set(taskId, revision);
  }
  for (const [taskId, pointer] of pointers) {
    const revision = covered.get(taskId);
    if (revision === undefined) {
      raise(
        `validatePlanTaskBindings is missing the task revision for plan task ${JSON.stringify(taskId)}`,
      );
    }
    if (revision.manifest.revision !== pointer.revision) {
      raise(
        `validatePlanTaskBindings revision number mismatch for plan task ${JSON.stringify(taskId)}`,
      );
    }
    if (revision.sha256 !== pointer.sha256) {
      raise(
        `validatePlanTaskBindings digest mismatch for plan task ${JSON.stringify(taskId)}`,
      );
    }
  }
}

/**
 * Validates the chain relationship between two prepared plan revisions.
 * For revision 1 the predecessor must be `null`, the revision number must
 * be exactly 1 and `previous_sha256` must be `null`; for a successor the
 * run ids must match, the revision number must be exactly
 * `previous.revision + 1`, `previous_sha256` must be the predecessor's
 * digest, and the protected root-task binding (input id and digest) must
 * be unchanged. Stage and task content may differ — that is the point of
 * a new full revision.
 */
export function validatePlanRevisionChain({
  previous,
  current,
}: {
  previous: PreparedPipelineV2RunPlanRevision | null;
  current: PreparedPipelineV2RunPlanRevision;
}): void {
  if (previous === null) {
    requireProvenance(current, "plan_revision");
    if (current.manifest.revision !== 1) {
      raise("validatePlanRevisionChain requires revision 1 when no predecessor is passed");
    }
    if (current.manifest.previous_sha256 !== null) {
      raise("validatePlanRevisionChain requires previous_sha256 null for revision 1");
    }
    return;
  }
  requireProvenance(previous, "plan_revision");
  requireProvenance(current, "plan_revision");
  if (previous.manifest.run_id !== current.manifest.run_id) {
    raise("validatePlanRevisionChain covers two different runs");
  }
  if (current.manifest.revision !== previous.manifest.revision + 1) {
    raise("validatePlanRevisionChain revision numbers are not consecutive");
  }
  if (current.manifest.previous_sha256 !== previous.sha256) {
    raise("validatePlanRevisionChain previous_sha256 does not name the predecessor digest");
  }
  const previousRoot = previous.manifest.root_task;
  const currentRoot = current.manifest.root_task;
  if (previousRoot.input_id !== currentRoot.input_id || previousRoot.sha256 !== currentRoot.sha256) {
    raise("validatePlanRevisionChain the protected root task binding changed between revisions");
  }
}

/**
 * Validates the protected root-task binding: the plan's root task digest
 * must equal the caller-computed digest of the protected input exactly.
 * The validator receives a digest string only — never the TASK object or
 * its body.
 */
export function validateRootTaskBinding({
  plan,
  protectedInputDigest,
}: {
  plan: PreparedPipelineV2RunPlanRevision;
  protectedInputDigest: string;
}): void {
  requireProvenance(plan, "plan_revision");
  expectSha256Digest(protectedInputDigest, "validateRootTaskBinding protectedInputDigest");
  if (plan.manifest.root_task.sha256 !== protectedInputDigest) {
    raise("validateRootTaskBinding the plan root task does not bind the protected input digest");
  }
}

/**
 * Validates the chain relationship between two prepared task revisions.
 * For revision 1 the predecessor must be `null`, the revision number must
 * be exactly 1 and `previous_sha256` must be `null`; for a successor the
 * run ids and task ids must match, the revision number must be exactly
 * `previous.revision + 1`, and `previous_sha256` must be the predecessor's
 * digest. The body and the content-free `origin` class are owned by the
 * manifest chain (revision 1 is only ever `planning_proposal`, revisions
 * above 1 only `user_response`) — no second origin validator exists here.
 */
export function validateTaskRevisionChain({
  previous,
  current,
}: {
  readonly previous: PreparedPipelineV2RunTaskRevision | null;
  readonly current: PreparedPipelineV2RunTaskRevision;
}): void {
  if (previous === null) {
    requireProvenance(current, "task_revision");
    if (current.manifest.revision !== 1) {
      raise("validateTaskRevisionChain requires revision 1 when no predecessor is passed");
    }
    if (current.manifest.previous_sha256 !== null) {
      raise("validateTaskRevisionChain requires previous_sha256 null for revision 1");
    }
    return;
  }
  requireProvenance(previous, "task_revision");
  requireProvenance(current, "task_revision");
  if (previous.manifest.run_id !== current.manifest.run_id) {
    raise("validateTaskRevisionChain covers two different runs");
  }
  if (previous.manifest.task_id !== current.manifest.task_id) {
    raise("validateTaskRevisionChain task ids do not agree");
  }
  if (current.manifest.revision !== previous.manifest.revision + 1) {
    raise("validateTaskRevisionChain revision numbers are not consecutive");
  }
  if (current.manifest.previous_sha256 !== previous.sha256) {
    raise("validateTaskRevisionChain previous_sha256 does not name the predecessor digest");
  }
}

/**
 * Validates one continue-stage wait intent against the prepared plan:
 * the intent must be a `continue_stage_intent` of the same run, its
 * `expected_plan_sha256` must name exactly the prepared plan's digest,
 * and the declared stage must exist in the plan. A template-specific
 * maximum iteration grant is policy, not binding, and stays out of
 * scope.
 */
export function validateContinueIntentBinding({
  intent,
  plan,
}: {
  intent: PreparedPipelineV2RunWaitIntent;
  plan: PreparedPipelineV2RunPlanRevision;
}): void {
  requireProvenance(plan, "plan_revision");
  requireProvenance(intent, "continue_stage_intent");
  const manifest = intent.manifest as PipelineV2ContinueStageIntentManifest;
  if (manifest.run_id !== plan.manifest.run_id) {
    raise("validateContinueIntentBinding covers two different runs");
  }
  if (manifest.expected_plan_sha256 !== plan.sha256) {
    raise("validateContinueIntentBinding expected_plan_sha256 does not name the prepared plan digest");
  }
  const stageId = manifest.stage_id;
  const stage = plan.manifest.stages.find((candidate) => candidate.id === stageId);
  if (stage === undefined) {
    raise("validateContinueIntentBinding names a stage the plan does not declare");
  }
}

/**
 * Validates one revise-task wait intent against the current accepted task
 * revision and the candidate replacement: same run and task, the
 * candidate must be the exact successor of the current revision
 * (`revision === current.revision + 1`, `previous_sha256` naming the
 * current digest), the intent's two digests must name exactly the current
 * and candidate digests, and the candidate's origin must be
 * `user_response` (a task replacement comes from a user response; the
 * planning system proposes revisions only at revision 1).
 */
export function validateReviseIntentBinding({
  intent,
  candidateTaskRevision,
  currentTaskRevision,
}: {
  intent: PreparedPipelineV2RunWaitIntent;
  candidateTaskRevision: PreparedPipelineV2RunTaskRevision;
  currentTaskRevision: PreparedPipelineV2RunTaskRevision;
}): void {
  requireProvenance(intent, "revise_task_intent");
  requireProvenance(candidateTaskRevision, "task_revision");
  requireProvenance(currentTaskRevision, "task_revision");
  const intentManifest = intent.manifest as PipelineV2ReviseTaskIntentManifest;
  if (intentManifest.run_id !== candidateTaskRevision.manifest.run_id) {
    raise("validateReviseIntentBinding covers two different runs");
  }
  if (intentManifest.run_id !== currentTaskRevision.manifest.run_id) {
    raise("validateReviseIntentBinding covers two different runs");
  }
  if (intentManifest.task_id !== candidateTaskRevision.manifest.task_id) {
    raise("validateReviseIntentBinding task ids do not agree with the candidate task revision");
  }
  if (intentManifest.task_id !== currentTaskRevision.manifest.task_id) {
    raise("validateReviseIntentBinding task ids do not agree with the current task revision");
  }
  if (candidateTaskRevision.manifest.revision !== currentTaskRevision.manifest.revision + 1) {
    raise("validateReviseIntentBinding candidate revision numbers are not consecutive");
  }
  if (candidateTaskRevision.manifest.previous_sha256 !== currentTaskRevision.sha256) {
    raise("validateReviseIntentBinding candidate previous_sha256 does not name the current task digest");
  }
  if (intentManifest.expected_previous_task_sha256 !== currentTaskRevision.sha256) {
    raise("validateReviseIntentBinding expected_previous_task_sha256 does not name the current task digest");
  }
  if (intentManifest.new_task_revision_sha256 !== candidateTaskRevision.sha256) {
    raise("validateReviseIntentBinding new_task_revision_sha256 does not name the candidate task digest");
  }
  if (candidateTaskRevision.manifest.origin !== "user_response") {
    raise("validateReviseIntentBinding candidate task revision origin must be user_response");
  }
}
