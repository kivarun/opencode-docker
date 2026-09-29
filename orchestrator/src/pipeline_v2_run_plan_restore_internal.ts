/**
 * Internal core of the read-only restoration of the last durable-accepted
 * compiled pipeline v2 run plan.
 *
 * After a process restart the continued-stage composition accepts only a
 * real provenance-backed `CompiledPipelineV2RunPlan` of the existing
 * chain; such an object exists in memory only immediately after the
 * run-plan acceptance controller compiled it. This module is the single
 * official way to rebuild that exact object from the durable state and
 * the immutable plan/task manifests: it never creates a second parser,
 * compiler, provenance registry or minter — it loads the exact prepared
 * manifests through the existing read-only store loaders, verifies them
 * against the durable ledgers and hands them to the existing
 * `preparePipelineV2RunPlanCandidate` and `compilePipelineV2RunPlanCandidate`.
 *
 * The restoration is strictly read-only: no filesystem write, no durable
 * dispatch, no recovery or repair. Newer or foreign orphan artifacts in
 * the store are ignored — the filesystem never selects the authoritative
 * revision; the durable plan ledger does.
 *
 * Failure contract (typed, classified by context — never by message
 * text): `invalid_options` (options/ops shape, before any effect),
 * `invalid_state` (malformed durable state or no accepted plan),
 * `pipeline_mismatch` (durable pipeline identity or run-root binding),
 * `artifact_missing` (a required exact plan/task/predecessor artifact is
 * absent) and `artifact_mismatch` (a loaded prepared artifact, durable
 * ledger record or root-task binding does not match). Typed errors of the
 * store, manifest, binding and compiler layers keep their own classes and
 * identity and are never re-classified; unexpected errors propagate
 * unchanged. Diagnostics are content-free: no task bodies, canonical
 * JSON, parser text, foreign artifact paths or canary values.
 */
import { basename } from "node:path";
import { loadPipelineV2PlanRevision, loadPipelineV2TaskRevision } from "./pipeline_v2_run_plan_store.ts";
import {
  compilePipelineV2RunPlanCandidate,
  type CompiledPipelineV2RunPlan,
} from "./pipeline_v2_run_plan_compiled.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "./pipeline_v2_run_plan_candidate.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2PlanRevisionState,
  type PipelineV2RunState,
  type PipelineV2TaskRevisionState,
} from "./pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import {
  comparePipelineV2RunIdentity,
  type PipelineV2RunIdentityField,
} from "./pipeline_v2_identity_compare.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import type { PreparedPipelineV2RunPlanRevision, PreparedPipelineV2RunTaskRevision } from "./pipeline_v2_run_plan_manifests.ts";

export type PipelineV2RunPlanRestoreFailureReason =
  | "invalid_options"
  | "invalid_state"
  | "pipeline_mismatch"
  | "artifact_missing"
  | "artifact_mismatch";

const REASON_SET: ReadonlySet<PipelineV2RunPlanRestoreFailureReason> = new Set([
  "invalid_options",
  "invalid_state",
  "pipeline_mismatch",
  "artifact_missing",
  "artifact_mismatch",
]);

export class PipelineV2RunPlanRestoreError extends Error {
  declare readonly reason: PipelineV2RunPlanRestoreFailureReason;
  readonly state: PipelineV2RunState | null;

  constructor(
    reason: PipelineV2RunPlanRestoreFailureReason,
    message: string,
    state: PipelineV2RunState | null,
  ) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 run plan restore error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2RunPlanRestoreError";
    this.state = state;
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

export interface RestorePipelineV2AcceptedRunPlanOptions {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRoot: string;
  readonly state: unknown;
}

export interface RestoredPipelineV2AcceptedRunPlan {
  readonly compiled_plan: CompiledPipelineV2RunPlan;
  readonly state: PipelineV2RunState;
}

/**
 * The read-only store capabilities of the restoration: exactly the two
 * existing loaders, nothing else. Results are erased so fault injection
 * can substitute primitives without a parallel typed path; the real
 * production methods return the store's provenance-backed wrappers.
 */
export interface PipelineV2RunPlanRestoreOps {
  readonly loadPlanRevision: (runRoot: string, revision: number) => Promise<unknown>;
  readonly loadTaskRevision: (runRoot: string, taskId: string, revision: number) => Promise<unknown>;
}

/**
 * The single frozen production ops object over the existing store
 * loaders; both methods are fixed at construction and can never be
 * reassigned through this object.
 */
export const productionRunPlanRestoreOps: PipelineV2RunPlanRestoreOps = Object.freeze({
  loadPlanRevision: (runRoot: string, revision: number): Promise<unknown> =>
    loadPipelineV2PlanRevision(runRoot, revision),
  loadTaskRevision: (runRoot: string, taskId: string, revision: number): Promise<unknown> =>
    loadPipelineV2TaskRevision(runRoot, taskId, revision),
});

const IDENTITY_FIELD_MESSAGES: Record<PipelineV2RunIdentityField, string> = {
  schema_version: "the pipeline schema version differs",
  bundle_root: "the canonical bundle root differs",
  execution_snapshot_sha256: "the execution snapshot digest differs",
  entry_state: "the entry state differs",
  max_transitions: "the transition budget differs",
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * The exact store wrapper of one loaded prepared manifest: the record
 * with the prepared object and the manifest of the expected kind. Every
 * malformed shape is this layer's typed failure — never a TypeError.
 */
function requirePreparedManifest(
  loaded: unknown,
  wrapperField: "plan" | "task",
  manifestKind: "plan_revision" | "task_revision",
  failMissing: () => PipelineV2RunPlanRestoreError,
  failMismatch: (what: string) => PipelineV2RunPlanRestoreError,
): PreparedPipelineV2RunPlanRevision | PreparedPipelineV2RunTaskRevision {
  if (loaded === null) {
    throw failMissing();
  }
  if (!isRecord(loaded)) {
    throw failMismatch("the loaded store result is not a published manifest wrapper");
  }
  const prepared = loaded[wrapperField];
  if (!isRecord(prepared)) {
    throw failMismatch(`the loaded store result carries no ${wrapperField} prepared manifest`);
  }
  const manifest = prepared["manifest"];
  if (!isRecord(manifest)) {
    throw failMismatch("the loaded prepared manifest carries no manifest record");
  }
  if (manifest["kind"] !== manifestKind) {
    throw failMismatch("the loaded prepared manifest carries a different manifest kind");
  }
  return prepared as unknown as PreparedPipelineV2RunPlanRevision | PreparedPipelineV2RunTaskRevision;
}

/**
 * The exact match of one loaded plan manifest against its durable ledger
 * record and the run binding. Every deviation is `artifact_mismatch`.
 */
function requirePlanManifestMatchesRecord(
  prepared: PreparedPipelineV2RunPlanRevision,
  record: PipelineV2PlanRevisionState,
  runId: string,
  failMismatch: (what: string) => PipelineV2RunPlanRestoreError,
): void {
  const manifest = prepared.manifest;
  if (manifest.run_id !== runId) {
    throw failMismatch("the loaded plan manifest belongs to a different run");
  }
  if (manifest.revision !== record.revision) {
    throw failMismatch("the loaded plan manifest carries a different revision");
  }
  if (prepared.sha256 !== record.sha256) {
    throw failMismatch("the loaded plan manifest digest differs from the durable plan record");
  }
  if (manifest.previous_sha256 !== record.previous_sha256) {
    throw failMismatch("the loaded plan manifest's previous digest differs from the durable plan record");
  }
  if (manifest.origin_execution !== record.origin_execution) {
    throw failMismatch("the loaded plan manifest carries a different origin execution");
  }
}

/**
 * The exact match of one loaded task manifest against its durable ledger
 * record, the plan pointer that names it and the run binding. Every
 * deviation is `artifact_mismatch`.
 */
function requireTaskManifestMatchesRecord(
  prepared: PreparedPipelineV2RunTaskRevision,
  taskId: string,
  revision: number,
  pointerSha256: string,
  record: PipelineV2TaskRevisionState,
  runId: string,
  failMismatch: (what: string) => PipelineV2RunPlanRestoreError,
): void {
  const manifest = prepared.manifest;
  if (manifest.run_id !== runId) {
    throw failMismatch("the loaded task manifest belongs to a different run");
  }
  if (manifest.task_id !== taskId) {
    throw failMismatch("the loaded task manifest carries a different task id");
  }
  if (manifest.revision !== revision) {
    throw failMismatch("the loaded task manifest carries a different revision");
  }
  if (prepared.sha256 !== pointerSha256) {
    throw failMismatch("the loaded task manifest digest differs from the plan pointer");
  }
  if (manifest.previous_sha256 !== record.previous_sha256) {
    throw failMismatch("the loaded task manifest's previous digest differs from the durable task record");
  }
}

/**
 * The exact durable task record for one pointer: exactly one ledger entry
 * of the task id and revision, its digest equal to the pointer's, and the
 * chain binding of the ledger. Every deviation is `artifact_mismatch`.
 */
function requireTaskRecord(
  state: PipelineV2RunState,
  taskId: string,
  revision: number,
  pointerSha256: string,
  failMismatch: (what: string) => PipelineV2RunPlanRestoreError,
): PipelineV2TaskRevisionState {
  const matches = state.task_revisions.filter(
    (record) => record.task_id === taskId && record.revision === revision,
  );
  if (matches.length !== 1) {
    throw failMismatch(
      `the durable task ledger does not carry exactly one accepted revision ${revision} of task ${JSON.stringify(taskId)}`,
    );
  }
  const record = matches[0]!;
  if (record.sha256 !== pointerSha256) {
    throw failMismatch(
      `the durable task ledger digest of task ${JSON.stringify(taskId)} differs from the plan pointer`,
    );
  }
  if (revision === 1 && record.previous_sha256 !== null) {
    throw failMismatch(
      `the durable task ledger records a predecessor for revision 1 of task ${JSON.stringify(taskId)}`,
    );
  }
  return record;
}

export async function restorePipelineV2AcceptedRunPlanInternal(
  options: unknown,
  ops: PipelineV2RunPlanRestoreOps,
): Promise<RestoredPipelineV2AcceptedRunPlan> {
  // Capture boundary: the options shape, every options field, the ops
  // record shape and both ops methods are read exactly once, all before
  // the first await. No field of the durable state or of the pipeline is
  // read here; a later mutation of the caller's options or ops cannot
  // change this restoration's policy.
  if (!isRecord(options)) {
    throw new PipelineV2RunPlanRestoreError(
      "invalid_options",
      "restorePipelineV2AcceptedRunPlan requires an options object",
      null,
    );
  }
  const pipeline = options["pipeline"] as ResolvedPipelineV2;
  const runRoot = options["runRoot"];
  const state = options["state"];
  if (!isRecord(ops)) {
    throw new PipelineV2RunPlanRestoreError(
      "invalid_options",
      "restorePipelineV2AcceptedRunPlan requires a read-only loader record",
      null,
    );
  }
  const loadPlanRevision = ops["loadPlanRevision"];
  const loadTaskRevision = ops["loadTaskRevision"];
  if (typeof loadPlanRevision !== "function" || typeof loadTaskRevision !== "function") {
    throw new PipelineV2RunPlanRestoreError(
      "invalid_options",
      "the restoration requires read-only plan and task revision loaders",
      null,
    );
  }
  if (typeof runRoot !== "string" || runRoot === "") {
    throw new PipelineV2RunPlanRestoreError(
      "invalid_options",
      "the run root must be a non-empty string",
      null,
    );
  }
  // The pipeline provenance gate: clones, casts, spreads and Proxies are
  // rejected here, before any field of the durable state or of the
  // pipeline is read.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 accepted run plan restoration");
  // The single state validation of the durable state document.
  let validated: PipelineV2RunState;
  try {
    validated = validatePipelineV2RunState(state);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw new PipelineV2RunPlanRestoreError(
        "invalid_state",
        "the durable run state is missing or not a valid pipeline v2 run state document",
        null,
      );
    }
    throw cause;
  }
  const failMissing = (what: string): PipelineV2RunPlanRestoreError =>
    new PipelineV2RunPlanRestoreError("artifact_missing", what, validated);
  const failMismatch = (what: string): PipelineV2RunPlanRestoreError =>
    new PipelineV2RunPlanRestoreError("artifact_mismatch", what, validated);
  // The exact durable pipeline identity via the single shared comparator.
  const identityComparison = comparePipelineV2RunIdentity(
    pipelineV2RunPipelineIdentity(pipeline),
    validated.pipeline,
  );
  if (identityComparison.kind === "mismatch") {
    throw new PipelineV2RunPlanRestoreError(
      "pipeline_mismatch",
      `the durable run state was created for a different pipeline: the ${identityComparison.field} differs`,
      validated,
    );
  }
  // The run-root binding, before any store load.
  if (basename(runRoot) !== validated.run_id) {
    throw new PipelineV2RunPlanRestoreError(
      "pipeline_mismatch",
      "the run root does not belong to this run",
      validated,
    );
  }
  // The accepted plan ledger: exactly the last durable record is the
  // authoritative plan revision; the filesystem never selects it.
  if (validated.plan_revisions.length === 0) {
    throw new PipelineV2RunPlanRestoreError(
      "invalid_state",
      "the durable run state records no accepted plan revision",
      validated,
    );
  }
  const currentPlanRecord = validated.plan_revisions[validated.plan_revisions.length - 1]!;

  // Fixed sequential read order; no parallel batch loads.
  // 1. the current plan manifest at the exact durable revision.
  const loadedPlan = await loadPlanRevision(runRoot, currentPlanRecord.revision);
  const preparedPlan = requirePreparedManifest(
    loadedPlan,
    "plan",
    "plan_revision",
    () => failMissing(`the accepted plan revision ${currentPlanRecord.revision} manifest is missing from the run plan store`),
    failMismatch,
  ) as PreparedPipelineV2RunPlanRevision;
  requirePlanManifestMatchesRecord(preparedPlan, currentPlanRecord, validated.run_id, failMismatch);

  // 2. the immediate plan predecessor, read and verified only above
  //    revision 1.
  let preparedPreviousPlan: PreparedPipelineV2RunPlanRevision | null = null;
  if (currentPlanRecord.revision > 1) {
    const previousPlanRecord = validated.plan_revisions[currentPlanRecord.revision - 2];
    if (previousPlanRecord === undefined || previousPlanRecord.revision !== currentPlanRecord.revision - 1) {
      throw failMismatch("the durable plan ledger does not carry the immediate predecessor plan revision");
    }
    const loadedPreviousPlan = await loadPlanRevision(runRoot, previousPlanRecord.revision);
    preparedPreviousPlan = requirePreparedManifest(
      loadedPreviousPlan,
      "plan",
      "plan_revision",
      () => failMissing(`the predecessor plan revision ${previousPlanRecord.revision} manifest is missing from the run plan store`),
      failMismatch,
    ) as PreparedPipelineV2RunPlanRevision;
    requirePlanManifestMatchesRecord(preparedPreviousPlan, previousPlanRecord, validated.run_id, failMismatch);
    if (preparedPreviousPlan.sha256 !== preparedPlan.manifest.previous_sha256) {
      throw failMismatch("the loaded predecessor plan manifest does not chain to the current plan manifest");
    }
  }

  // 3. the exact current task revisions of the current plan, in the
  //    manifest's normalized semantic order, each with its immediate
  //    predecessor read only above revision 1.
  const taskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
  const previousTaskRevisions: PreparedPipelineV2RunTaskRevision[] = [];
  for (const stage of preparedPlan.manifest.stages) {
    for (const pointer of stage.tasks) {
      const taskRecord = requireTaskRecord(
        validated,
        pointer.id,
        pointer.revision,
        pointer.sha256,
        failMismatch,
      );
      const loadedTask = await loadTaskRevision(runRoot, pointer.id, pointer.revision);
      const preparedTask = requirePreparedManifest(
        loadedTask,
        "task",
        "task_revision",
        () => failMissing(`the task revision ${pointer.revision} manifest of task ${JSON.stringify(pointer.id)} is missing from the run plan store`),
        failMismatch,
      ) as PreparedPipelineV2RunTaskRevision;
      requireTaskManifestMatchesRecord(
        preparedTask,
        pointer.id,
        pointer.revision,
        pointer.sha256,
        taskRecord,
        validated.run_id,
        failMismatch,
      );
      if (taskRecord.revision > 1) {
        const previousTaskRecord = requireTaskRecord(
          validated,
          pointer.id,
          taskRecord.revision - 1,
          taskRecord.previous_sha256 ?? "",
          failMismatch,
        );
        const loadedPreviousTask = await loadTaskRevision(runRoot, pointer.id, previousTaskRecord.revision);
        const preparedPreviousTask = requirePreparedManifest(
          loadedPreviousTask,
          "task",
          "task_revision",
          () => failMissing(`the predecessor task revision ${previousTaskRecord.revision} manifest of task ${JSON.stringify(pointer.id)} is missing from the run plan store`),
          failMismatch,
        ) as PreparedPipelineV2RunTaskRevision;
        requireTaskManifestMatchesRecord(
          preparedPreviousTask,
          pointer.id,
          previousTaskRecord.revision,
          previousTaskRecord.sha256,
          previousTaskRecord,
          validated.run_id,
          failMismatch,
        );
        if (preparedPreviousTask.sha256 !== preparedTask.manifest.previous_sha256) {
          throw failMismatch(
            `the loaded predecessor task manifest does not chain to the current task manifest of task ${JSON.stringify(pointer.id)}`,
          );
        }
        previousTaskRevisions.push(preparedPreviousTask);
      }
      taskRevisions.push(preparedTask);
    }
  }

  // 4. the durable protected input bound by the plan's root task: exactly
  //    one existing durable input of that id, protected, with the exact
  //    digest. No input artifact is read.
  const rootTask = preparedPlan.manifest.root_task;
  const rootTaskInputs = validated.inputs.filter((input) => input.id === rootTask.input_id);
  if (rootTaskInputs.length !== 1) {
    throw failMismatch(
      "the durable run inputs do not carry exactly one input bound by the plan's root task",
    );
  }
  const rootTaskInput = rootTaskInputs[0]!;
  if (!rootTaskInput.protected) {
    throw failMismatch("the plan's root task binds a durable run input that is not protected");
  }
  if (rootTaskInput.digest !== rootTask.sha256) {
    throw failMismatch("the durable protected input digest differs from the plan's root task binding");
  }

  // 5.+6. the existing candidate preparation and compilation over the
  //    exact loaded prepared objects; their typed errors keep their own
  //    classes and identity.
  const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
    plan: preparedPlan,
    taskRevisions,
    previousPlan: preparedPreviousPlan,
    previousTaskRevisions,
    protectedInputDigest: rootTaskInput.digest,
  });
  const compiledPlan = compilePipelineV2RunPlanCandidate(pipeline, candidate);
  return deepFreezeValue({
    compiled_plan: compiledPlan,
    state: validated,
  });
}
