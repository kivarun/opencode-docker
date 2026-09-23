import {
  loadPipelineV2PlanRevisionWithIo,
  loadPipelineV2TaskRevisionWithIo,
  publishPipelineV2PlanRevisionWithIo,
  publishPipelineV2TaskRevisionWithIo,
  realRunPlanStoreIo,
  PipelineV2RunPlanStoreError,
  type PipelineV2RunPlanStoreCandidate,
  type PipelineV2RunPlanStoreFailureReason,
  type PipelineV2RunPlanStoreOutcome,
  type PublishedPipelineV2RunPlanRevision,
  type PublishedPipelineV2RunTaskRevision,
} from "./pipeline_v2_run_plan_store_internal.ts";

/**
 * Immutable filesystem publication and load of the pipeline v2 run plan
 * and task revision manifests (unwired, production-neutral).
 *
 * This module publishes the orchestrator-owned canonical manifests of the
 * pure `pipeline_v2_run_plan_manifests.ts` substrate under the fixed
 * layout
 *
 *   <runRoot>/run-plan/plans/<revision>.json
 *   <runRoot>/run-plan/tasks/<task-id>/<revision>.json
 *
 * as 0600 regular files whose content is exactly the manifest's canonical
 * JSON (no trailing newline), inside 0700 real non-symlink directory
 * components (`run-plan`, `plans`, `tasks`, `<task-id>`) of the canonical
 * run root. The run root itself is never created, chmodded or removed;
 * its basename must be the manifest's run id.
 *
 * Publication is atomic (exclusive temp file, full write-all, file fsync,
 * close, exclusive `link()` — never a replace-capable `rename()` — then
 * ownership-checked temp removal and a parent-directory fsync) and
 * idempotent: a repeat with the same canonical
 * bytes adopts the existing file without touching its inode, mode, mtime
 * or content and re-fsyncs the parent, so a retry after a former
 * durability-unknown outcome confirms it. A different manifest on a busy
 * path is a typed conflict; nothing is ever overwritten, repaired or
 * removed except the caller's own temp file.
 *
 * Load is strictly read-only and returns `null` when the artifact or its
 * store-owned parent tree is absent; a damaged, foreign, noncanonical or
 * wrongly-moded artifact fails closed; malformed JSON keeps the manifest
 * module's `PipelineV2RunPlanManifestError` class.
 *
 * Errors are a closed typed contract (`PipelineV2RunPlanStoreError` with
 * `outcome` `not_published`|`durability_unknown`, `reason`
 * `invalid_layout`|`conflict`|`io_failure`, and an immutable content-free
 * candidate on durability-unknown outcomes). Diagnostics are content-free.
 *
 * Store responsibility ends at the immutable canonical artifacts:
 * plan↔task linkage, revision chains and the acceptance of published
 * manifests into the durable run state are the future controller's
 * responsibility — this module never dispatches reducer commands and never
 * touches the durable state, the project copy, inputs or outputs. The
 * lifecycle controller, reducer wiring and production runner are later
 * increments.
 *
 * The full algorithm, concurrency semantics, ownership rules and honest
 * boundaries are documented in `pipeline_v2_run_plan_store_internal.ts`;
 * the filesystem protocol itself lives exactly once in
 * `pipeline_v2_immutable_document_store_internal.ts`.
 */
export {
  PipelineV2RunPlanStoreError,
  type PipelineV2RunPlanStoreFailureReason,
  type PipelineV2RunPlanStoreOutcome,
  type PipelineV2RunPlanStoreCandidate,
  type PublishedPipelineV2RunTaskRevision,
  type PublishedPipelineV2RunPlanRevision,
};

/**
 * Validates, binds and publishes one task revision manifest under
 * `<runRoot>/run-plan/tasks/<task_id>/<revision>.json`. The task id and
 * revision of the path come only from the normalized manifest; the run
 * root must be an existing absolute canonical real non-symlink directory
 * whose basename is the manifest's run id; it is never created or removed.
 */
export async function publishPipelineV2TaskRevision(
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2RunTaskRevision> {
  return await publishPipelineV2TaskRevisionWithIo(realRunPlanStoreIo, runRoot, value);
}

/**
 * Loads the stored task revision manifest
 * `<runRoot>/run-plan/tasks/<task_id>/<revision>.json` strictly read-only:
 * returns the stored manifest bound to the trusted scalars, or `null` when
 * the artifact or its store-owned parent tree is absent.
 */
export async function loadPipelineV2TaskRevision(
  runRoot: string,
  taskId: string,
  revision: number,
): Promise<PublishedPipelineV2RunTaskRevision | null> {
  return await loadPipelineV2TaskRevisionWithIo(realRunPlanStoreIo, runRoot, taskId, revision);
}

/**
 * Validates, binds and publishes one plan revision manifest under
 * `<runRoot>/run-plan/plans/<revision>.json`. The revision of the path
 * comes only from the normalized manifest; the run root must be an
 * existing absolute canonical real non-symlink directory whose basename
 * is the manifest's run id; it is never created or removed.
 */
export async function publishPipelineV2PlanRevision(
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2RunPlanRevision> {
  return await publishPipelineV2PlanRevisionWithIo(realRunPlanStoreIo, runRoot, value);
}

/**
 * Loads the stored plan revision manifest
 * `<runRoot>/run-plan/plans/<revision>.json` strictly read-only: returns
 * the stored manifest bound to the trusted scalars, or `null` when the
 * artifact or its store-owned parent tree is absent.
 */
export async function loadPipelineV2PlanRevision(
  runRoot: string,
  revision: number,
): Promise<PublishedPipelineV2RunPlanRevision | null> {
  return await loadPipelineV2PlanRevisionWithIo(realRunPlanStoreIo, runRoot, revision);
}
