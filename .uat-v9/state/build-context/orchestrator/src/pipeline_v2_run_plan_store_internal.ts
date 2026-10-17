import { basename, join } from "node:path";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  immutableDocumentIoFailure,
  immutableDocumentInvalidLayout,
  immutableDocumentConflict,
  ImmutableDocumentStoreError,
  inspectStoredDocumentOrNull,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  realImmutableDocumentIo,
  requireImmutableDocumentRunRoot,
  ensureImmutableDirectory,
  verifyStoredDirectoryComponent,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import {
  PipelineV2RunPlanManifestError,
  parsePlanRevisionManifest,
  parseTaskRevisionManifest,
  parseWaitIntent,
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
  type PreparedPipelineV2RunWaitIntent,
} from "./pipeline_v2_run_plan_manifests.ts";

/**
 * Internal per-call core of the pipeline v2 run-plan manifest filesystem
 * publication and load (unwired, production-neutral).
 *
 * This module is explicitly internal: the public wrapper
 * `pipeline_v2_run_plan_store.ts` always calls the core functions with the
 * single fixed, immutable production IO re-exported from the neutral
 * immutable-document substrate; tests call the same core with their own
 * per-call IO object. The IO is a per-call capability passed through the
 * whole call chain — there is no mutable module-global IO and no
 * installer, so a fault-injected test call can never change the behavior
 * of any parallel production call, and the public module exports no IO or
 * test seam.
 *
 * This module is the RUN-PLAN ADAPTER of the neutral immutable-document
 * substrate: the whole filesystem protocol — run-root verification,
 * exclusive directory creation with identity fixation and chmod
 * enforcement, the exclusive temp file, the full
 * write-all loop, file fsync, close, the exclusive publication,
 * the ownership-checked temp removal, the parent-directory fsync, the
 * idempotent adoption and the no-follow read path — lives
 * exactly once in
 * `pipeline_v2_immutable_document_store_internal.ts`; this adapter owns
 * only the run-plan layout, names, binding checks and public error class.
 *
 * Layout (fixed):
 *
 *   <runRoot>/run-plan/plans/<revision>.json
 *   <runRoot>/run-plan/tasks/<task-id>/<revision>.json
 *   <runRoot>/run-plan/intents/<wait-index>.json
 *
 * Manifest files are 0600 regular files whose content is exactly the
 * manifest's canonical JSON (no trailing newline), inside 0700 real
 * non-symlink directory components (`run-plan`, `plans`, `tasks`,
 * `<task-id>`, `intents`) of the canonical run root. The run root itself
 * is never
 * created, chmodded or removed; its basename must be the manifest's run
 * id. The paths never enter the manifests, and the manifests carry no
 * filesystem provenance.
 *
 * The wait intent manifests (`continue_stage_intent`,
 * `revise_task_intent`) share one flat, kind-independent layout: one wait
 * index owns exactly one immutable intent file, so a different intent for
 * the same wait is a typed conflict and never a second file. The store
 * never accepts a published intent into the durable run state and never
 * dispatches a reducer command — the acceptance commands, the wait-bound
 * iteration closure and the whole wait/response policy stay later
 * increments.
 *
 * Store responsibility ends at the immutable canonical artifacts:
 * plan↔task linkage, revision chains and the acceptance of published
 * manifests into the durable run state are the future controller's
 * responsibility — this module never dispatches reducer commands, never
 * touches the durable state, the project copy, inputs or outputs, and
 * never reads or writes any other run-root object.
 *
 * Publication is atomic and idempotent: a repeat with the same canonical
 * bytes adopts the existing file without touching its inode, mode, mtime
 * or content and re-fsyncs the parent, so an exact retry after a former
 * durability-unknown outcome confirms it. A different manifest on a busy
 * path is a typed conflict; nothing is ever overwritten, repaired or
 * removed except the caller's own temp file.
 *
 * Load is strictly read-only: it validates the trusted scalars, walks the
 * store-owned tree with `lstat`/`realpath` only (wrong kind, symlinked
 * component or wrong mode fails closed), and returns `null` when the
 * artifact or its store-owned parent tree is absent — nothing is created,
 * chmodded, linked, renamed or removed. A stored manifest is accepted only
 * when its bytes equal its own canonical JSON and its `run_id`,
 * `task_id`/`revision` (task) or `revision` (plan) or `wait_index` (wait
 * intent) equal the requested binding; malformed JSON keeps the
 * manifest module's `PipelineV2RunPlanManifestError` class.
 *
 * Errors are a closed typed contract (`PipelineV2RunPlanStoreError` with
 * `outcome` `not_published`|`durability_unknown`, `reason`
 * `invalid_layout`|`conflict`|`io_failure`, and an immutable content-free
 * candidate on durability-unknown outcomes); manifest validation failures
 * keep their `PipelineV2RunPlanManifestError` class. Diagnostics are
 * content-free: no manifest body, task body, raw JSON, caller path, env
 * value or credential — only operation classes, safe ids, revisions and
 * the getter-safe errno suffix. The final boundary sanitizes unexpected
 * causes: only this module's own store errors and the manifest module's
 * error class pass unchanged, neutral substrate errors are re-tagged by
 * typed fields, and every other thrown cause (a programmer error from an
 * injected or future callback) becomes a `not_published` `io_failure`
 * carrying only the wording's fixed content-free fallback message — the
 * original cause is never re-thrown and never echoed. Classification is
 * never by message text.
 */

export { realImmutableDocumentIo as realRunPlanStoreIo };

export type PipelineV2RunPlanStoreFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type PipelineV2RunPlanStoreOutcome = "not_published" | "durability_unknown";

/**
 * The published candidate of a durability-unknown outcome: content-free
 * (kind, run id, task id for tasks, wait index for wait intents, revision
 * for tasks and plans, digest); the canonical target path appears only as
 * an internal candidate field, never in a diagnostic message. The exact
 * discriminated union makes every impossible combination (a task without
 * its task id or revision, a wait intent with a task id or revision, a
 * plan with a wait index) a compile-time error. An exact retry re-verifies
 * and re-fsyncs the file.
 */
export type PipelineV2RunPlanStoreCandidate =
  | {
      readonly kind: "task";
      readonly run_id: string;
      readonly task_id: string;
      readonly revision: number;
      readonly sha256: string;
      readonly final_path: string;
    }
  | {
      readonly kind: "plan";
      readonly run_id: string;
      readonly revision: number;
      readonly sha256: string;
      readonly final_path: string;
    }
  | {
      readonly kind: "wait_intent";
      readonly run_id: string;
      readonly wait_index: number;
      readonly sha256: string;
      readonly final_path: string;
    };

export interface PublishedPipelineV2RunTaskRevision {
  readonly task: PreparedPipelineV2RunTaskRevision;
  readonly task_path: string;
}

export interface PublishedPipelineV2RunPlanRevision {
  readonly plan: PreparedPipelineV2RunPlanRevision;
  readonly plan_path: string;
}

/**
 * The published or loaded wait intent: the exact prepared intent of the
 * manifest substrate (provenance and deep-freeze semantics unchanged) plus
 * the fixed publication path; no caller input is carried.
 */
export interface PublishedPipelineV2RunWaitIntent {
  readonly intent: PreparedPipelineV2RunWaitIntent;
  readonly intent_path: string;
}

export class PipelineV2RunPlanStoreError extends Error {
  readonly outcome: PipelineV2RunPlanStoreOutcome;
  readonly reason: PipelineV2RunPlanStoreFailureReason;
  readonly candidate?: PipelineV2RunPlanStoreCandidate;

  constructor(
    outcome: PipelineV2RunPlanStoreOutcome,
    reason: PipelineV2RunPlanStoreFailureReason,
    message: string,
    candidate?: PipelineV2RunPlanStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2RunPlanStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreezeValue(candidate);
    }
  }
}

const RUN_PLAN_DIR_NAME = "run-plan";
const PLANS_DIR_NAME = "plans";
const TASKS_DIR_NAME = "tasks";
const INTENTS_DIR_NAME = "intents";
const TEMP_PREFIX = ".run-plan-publish-";

const TASK_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "task revision manifest",
  publicationFailed: "the task revision manifest publication failed",
});

const PLAN_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "plan revision manifest",
  publicationFailed: "the plan revision manifest publication failed",
});

const INTENT_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "wait intent manifest",
  publicationFailed: "the wait intent manifest publication failed",
});

/**
 * Final typed boundary: only `PipelineV2RunPlanStoreError` and
 * `PipelineV2RunPlanManifestError` pass unchanged; neutral substrate
 * errors are re-tagged into the run-plan store class by typed fields
 * (outcome, reason, candidate identity) — never by message text; any
 * other failure becomes a sanitized not-published io failure with the
 * wording's exact fallback message.
 */
async function withRunPlanStoreGuard<T>(
  wording: ImmutableDocumentWording,
  operation: () => Promise<T>,
): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2RunPlanStoreError) {
      throw cause;
    }
    if (cause instanceof PipelineV2RunPlanManifestError) {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw runPlanStoreErrorFromSubstrate(cause);
    }
    throw runPlanStoreErrorFromSubstrate(immutableDocumentIoFailure(wording.publicationFailed));
  }
}

interface RunPlanCandidateIdentity {
  readonly kind?: unknown;
  readonly run_id?: unknown;
  readonly task_id?: unknown;
  readonly wait_index?: unknown;
  readonly revision?: unknown;
}

/**
 * Map a neutral substrate error into the run-plan store class by typed
 * fields only: outcome and reason pass through, and the substrate
 * candidate's frozen identity descriptor ({kind, run_id, task_id,
 * wait_index, revision}) becomes the run-plan candidate's own exact
 * union branch — a task candidate always carries its task id and
 * revision, a plan candidate never carries a task id or wait index, a
 * wait-intent candidate always carries its wait index and never a task
 * id or revision. Identity is
 * always the descriptor this adapter bound at publication time.
 */
function runPlanStoreErrorFromSubstrate(
  cause: ImmutableDocumentStoreError,
): PipelineV2RunPlanStoreError {
  const substrateCandidate = cause.candidate;
  if (substrateCandidate === undefined) {
    return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message);
  }
  const identity = substrateCandidate.identity as RunPlanCandidateIdentity;
  const kind = identity.kind;
  const runId = identity.run_id;
  if (typeof runId !== "string") {
    return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message);
  }
  if (kind === "wait_intent") {
    const waitIndex = identity.wait_index;
    if (typeof waitIndex !== "number") {
      return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message);
    }
    return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message, {
      kind: "wait_intent",
      run_id: runId,
      wait_index: waitIndex,
      sha256: substrateCandidate.sha256,
      final_path: substrateCandidate.final_path,
    });
  }
  const revision = identity.revision;
  if ((kind !== "task" && kind !== "plan") || typeof revision !== "number") {
    return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message);
  }
  if (kind === "task") {
    const taskId = identity.task_id;
    if (typeof taskId !== "string") {
      return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message);
    }
    return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message, {
      kind: "task",
      run_id: runId,
      task_id: taskId,
      revision,
      sha256: substrateCandidate.sha256,
      final_path: substrateCandidate.final_path,
    });
  }
  return new PipelineV2RunPlanStoreError(cause.outcome, cause.reason, cause.message, {
    kind: "plan",
    run_id: runId,
    revision,
    sha256: substrateCandidate.sha256,
    final_path: substrateCandidate.final_path,
  });
}

function invalidLayout(message: string): PipelineV2RunPlanStoreError {
  return new PipelineV2RunPlanStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2RunPlanStoreError {
  return new PipelineV2RunPlanStoreError("not_published", "conflict", message);
}

/**
 * Bind the canonical run root of a publication: absolute, existing, real
 * non-symlink directory, canonically equal to its own path, and its
 * basename must be the manifest's run id. Never created or removed.
 */
async function bindPublicationRunRoot(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  runId: string,
  wording: ImmutableDocumentWording,
): Promise<string> {
  const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
  if (basename(runRootCanonical) !== runId) {
    throw immutableDocumentInvalidLayout(
      `the run root does not match the ${wording.document} run identifier`,
    );
  }
  return runRootCanonical;
}

/**
 * Validate the trusted load scalars: a safe task id and a canonical
 * positive decimal safe-integer revision (a number's decimal form carries
 * no leading zeros; the file name is composed only from the number).
 */
function requireLoadScalars(taskId: string, revision: number): void {
  if (!isPipelineV2SafeId(taskId)) {
    throw invalidLayout("the task id must be a safe pipeline v2 identifier");
  }
  if (!isPositiveSafeInteger(revision)) {
    throw invalidLayout("the revision must be a positive safe integer");
  }
}

/**
 * Walk the store-owned tree of a load path strictly read-only: every
 * component must be absent (→ `null`) or a verified 0700 directory. The
 * verification itself lives in the neutral substrate; nothing is created,
 * chmodded, linked, renamed or removed.
 */
async function requireStoreOwnedDirectoryForLoad(
  io: ImmutableDocumentIo,
  parentCanonical: string,
  dirName: string,
  dirNoun: string,
): Promise<string | null> {
  return await verifyStoredDirectoryComponent(io, parentCanonical, dirName, dirNoun);
}

/**
 * The run-id binding of a loaded manifest: the stored `run_id` must equal
 * the run root's basename (checked after parse, before any acceptance).
 */
function requireLoadedRunId(
  manifestRunId: string,
  runId: string,
): void {
  if (manifestRunId !== runId) {
    throw conflict("the stored manifest does not belong to this run root");
  }
}

/**
 * Validate, bind and publish one task revision manifest under
 * `<runRoot>/run-plan/tasks/<task_id>/<revision>.json`. The task id and
 * revision of the path come only from the normalized manifest; the run
 * root must be an existing absolute canonical real non-symlink directory
 * whose basename is the manifest's run id; it is never created or removed.
 */
export async function publishPipelineV2TaskRevisionWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2RunTaskRevision> {
  return await withRunPlanStoreGuard(TASK_WORDING, async () => {
    const prepared = prepareTaskRevisionManifest(value);
    const runRootCanonical = await bindPublicationRunRoot(
      io,
      runRoot,
      prepared.manifest.run_id,
      TASK_WORDING,
    );
    const runPlanPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
      "run root",
    );
    const tasksPath = await ensureImmutableDirectory(
      io,
      runPlanPath,
      TASKS_DIR_NAME,
      "tasks directory",
      "run-plan directory",
    );
    const taskDirPath = await ensureImmutableDirectory(
      io,
      tasksPath,
      prepared.manifest.task_id,
      "task directory",
      "tasks directory",
    );
    const fileName = `${prepared.manifest.revision}.json`;
    const taskPath = join(taskDirPath, fileName);
    await publishImmutableDocumentFile(
      io,
      taskDirPath,
      {
        fileName,
        tempPrefix: TEMP_PREFIX,
        tempStem: `task-${prepared.manifest.task_id}-${prepared.manifest.revision}`,
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: deepFreezeValue({
          kind: "task",
          run_id: prepared.manifest.run_id,
          task_id: prepared.manifest.task_id,
          revision: prepared.manifest.revision,
        }),
      },
      TASK_WORDING,
      "task directory",
    );
    return deepFreezeValue({ task: prepared, task_path: taskPath });
  });
}

/**
 * Load the stored task revision manifest
 * `<runRoot>/run-plan/tasks/<task_id>/<revision>.json` strictly read-only.
 * Returns `null` when the artifact or its store-owned parent tree is
 * absent; a damaged, foreign, noncanonical or wrongly-moded artifact fails
 * closed; malformed JSON keeps the manifest module's error class.
 */
export async function loadPipelineV2TaskRevisionWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  taskId: string,
  revision: number,
): Promise<PublishedPipelineV2RunTaskRevision | null> {
  return await withRunPlanStoreGuard(TASK_WORDING, async () => {
    requireLoadScalars(taskId, revision);
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const runId = basename(runRootCanonical);
    const runPlanPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
    );
    if (runPlanPath === null) {
      return null;
    }
    const tasksPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runPlanPath,
      TASKS_DIR_NAME,
      "tasks directory",
    );
    if (tasksPath === null) {
      return null;
    }
    const taskDirPath = await requireStoreOwnedDirectoryForLoad(
      io,
      tasksPath,
      taskId,
      "task directory",
    );
    if (taskDirPath === null) {
      return null;
    }
    const taskPath = join(taskDirPath, `${revision}.json`);
    const artifactInfo = await inspectStoredDocumentOrNull(
      io,
      taskPath,
      "stored task revision manifest file",
    );
    if (artifactInfo === null) {
      return null;
    }
    const stored = await readStoredImmutableDocument(
      io,
      taskPath,
      "stored task revision manifest file",
      TASK_WORDING,
    );
    const raw = stored.toString("utf8");
    const prepared = parseTaskRevisionManifest(raw);
    requireLoadedRunId(prepared.manifest.run_id, runId);
    if (prepared.manifest.task_id !== taskId) {
      throw conflict("the stored task revision manifest names another task id");
    }
    if (prepared.manifest.revision !== revision) {
      throw conflict("the stored task revision manifest names another revision");
    }
    if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
      throw conflict("the stored task revision manifest file does not carry its own canonical JSON");
    }
    return deepFreezeValue({ task: prepared, task_path: taskPath });
  });
}

/**
 * Validate, bind and publish one plan revision manifest under
 * `<runRoot>/run-plan/plans/<revision>.json`. The revision of the path
 * comes only from the normalized manifest; the run root must be an
 * existing absolute canonical real non-symlink directory whose basename
 * is the manifest's run id; it is never created or removed.
 */
export async function publishPipelineV2PlanRevisionWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2RunPlanRevision> {
  return await withRunPlanStoreGuard(PLAN_WORDING, async () => {
    const prepared = preparePlanRevisionManifest(value);
    const runRootCanonical = await bindPublicationRunRoot(
      io,
      runRoot,
      prepared.manifest.run_id,
      PLAN_WORDING,
    );
    const runPlanPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
      "run root",
    );
    const plansPath = await ensureImmutableDirectory(
      io,
      runPlanPath,
      PLANS_DIR_NAME,
      "plans directory",
      "run-plan directory",
    );
    const fileName = `${prepared.manifest.revision}.json`;
    const planPath = join(plansPath, fileName);
    await publishImmutableDocumentFile(
      io,
      plansPath,
      {
        fileName,
        tempPrefix: TEMP_PREFIX,
        tempStem: `plan-${prepared.manifest.revision}`,
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: deepFreezeValue({
          kind: "plan",
          run_id: prepared.manifest.run_id,
          revision: prepared.manifest.revision,
        }),
      },
      PLAN_WORDING,
      "plans directory",
    );
    return deepFreezeValue({ plan: prepared, plan_path: planPath });
  });
}

/**
 * Load the stored plan revision manifest
 * `<runRoot>/run-plan/plans/<revision>.json` strictly read-only. Returns
 * `null` when the artifact or its store-owned parent tree is absent; a
 * damaged, foreign, noncanonical or wrongly-moded artifact fails closed;
 * malformed JSON keeps the manifest module's error class.
 */
export async function loadPipelineV2PlanRevisionWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  revision: number,
): Promise<PublishedPipelineV2RunPlanRevision | null> {
  return await withRunPlanStoreGuard(PLAN_WORDING, async () => {
    if (!isPositiveSafeInteger(revision)) {
      throw invalidLayout("the revision must be a positive safe integer");
    }
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const runId = basename(runRootCanonical);
    const runPlanPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
    );
    if (runPlanPath === null) {
      return null;
    }
    const plansPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runPlanPath,
      PLANS_DIR_NAME,
      "plans directory",
    );
    if (plansPath === null) {
      return null;
    }
    const planPath = join(plansPath, `${revision}.json`);
    const artifactInfo = await inspectStoredDocumentOrNull(
      io,
      planPath,
      "stored plan revision manifest file",
    );
    if (artifactInfo === null) {
      return null;
    }
    const stored = await readStoredImmutableDocument(
      io,
      planPath,
      "stored plan revision manifest file",
      PLAN_WORDING,
    );
    const raw = stored.toString("utf8");
    const prepared = parsePlanRevisionManifest(raw);
    requireLoadedRunId(prepared.manifest.run_id, runId);
    if (prepared.manifest.revision !== revision) {
      throw conflict("the stored plan revision manifest names another revision");
    }
    if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
      throw conflict("the stored plan revision manifest file does not carry its own canonical JSON");
    }
    return deepFreezeValue({ plan: prepared, plan_path: planPath });
  });
}

/**
 * Validate, bind and publish one wait intent manifest under
 * `<runRoot>/run-plan/intents/<wait_index>.json`. The wait index of the
 * path comes only from the normalized manifest; the run root must be an
 * existing absolute canonical real non-symlink directory whose basename is
 * the manifest's run id; it is never created or removed. One wait index
 * owns exactly one immutable intent file independent of the intent kind —
 * a different intent on the same wait is a typed conflict, never a second
 * file.
 */
export async function publishPipelineV2WaitIntentWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2RunWaitIntent> {
  return await withRunPlanStoreGuard(INTENT_WORDING, async () => {
    const prepared = prepareWaitIntent(value);
    const runRootCanonical = await bindPublicationRunRoot(
      io,
      runRoot,
      prepared.manifest.run_id,
      INTENT_WORDING,
    );
    const runPlanPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
      "run root",
    );
    const intentsPath = await ensureImmutableDirectory(
      io,
      runPlanPath,
      INTENTS_DIR_NAME,
      "intents directory",
      "run-plan directory",
    );
    const fileName = `${prepared.manifest.wait_index}.json`;
    const intentPath = join(intentsPath, fileName);
    await publishImmutableDocumentFile(
      io,
      intentsPath,
      {
        fileName,
        tempPrefix: TEMP_PREFIX,
        tempStem: `intent-${prepared.manifest.wait_index}`,
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: deepFreezeValue({
          kind: "wait_intent",
          run_id: prepared.manifest.run_id,
          wait_index: prepared.manifest.wait_index,
        }),
      },
      INTENT_WORDING,
      "intents directory",
    );
    return deepFreezeValue({ intent: prepared, intent_path: intentPath });
  });
}

/**
 * Load the stored wait intent manifest
 * `<runRoot>/run-plan/intents/<wait_index>.json` strictly read-only.
 * Returns `null` when the artifact or its store-owned parent tree is
 * absent; a damaged, foreign, noncanonical or wrongly-moded artifact fails
 * closed; malformed JSON keeps the manifest module's error class.
 */
export async function loadPipelineV2WaitIntentWithIo(
  io: typeof realImmutableDocumentIo,
  runRoot: string,
  waitIndex: number,
): Promise<PublishedPipelineV2RunWaitIntent | null> {
  return await withRunPlanStoreGuard(INTENT_WORDING, async () => {
    if (!isPositiveSafeInteger(waitIndex)) {
      throw invalidLayout("the wait index must be a positive safe integer");
    }
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const runId = basename(runRootCanonical);
    const runPlanPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runRootCanonical,
      RUN_PLAN_DIR_NAME,
      "run-plan directory",
    );
    if (runPlanPath === null) {
      return null;
    }
    const intentsPath = await requireStoreOwnedDirectoryForLoad(
      io,
      runPlanPath,
      INTENTS_DIR_NAME,
      "intents directory",
    );
    if (intentsPath === null) {
      return null;
    }
    const intentPath = join(intentsPath, `${waitIndex}.json`);
    const artifactInfo = await inspectStoredDocumentOrNull(
      io,
      intentPath,
      "stored wait intent manifest file",
    );
    if (artifactInfo === null) {
      return null;
    }
    const stored = await readStoredImmutableDocument(
      io,
      intentPath,
      "stored wait intent manifest file",
      INTENT_WORDING,
    );
    const raw = stored.toString("utf8");
    const prepared = parseWaitIntent(raw);
    requireLoadedRunId(prepared.manifest.run_id, runId);
    if (prepared.manifest.wait_index !== waitIndex) {
      throw conflict("the stored wait intent manifest names another wait index");
    }
    if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
      throw conflict("the stored wait intent manifest file does not carry its own canonical JSON");
    }
    return deepFreezeValue({ intent: prepared, intent_path: intentPath });
  });
}
