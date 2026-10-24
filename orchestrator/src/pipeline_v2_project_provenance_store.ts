import {
  loadProjectProvenanceWithIo,
  loadVerifiedProjectProvenanceWithIo,
  PROJECT_PROVENANCE_FILE_NAME,
  publishProjectProvenanceWithIo,
  realProjectWorkspaceVerifyIo,
  verifyProjectWorkspaceBaselineWithIo,
  PipelineV2ProjectProvenanceStoreError,
  type PipelineV2ProjectProvenanceStoreCandidate,
  type PipelineV2ProjectProvenanceStoreFailureReason,
  type PipelineV2ProjectProvenanceStoreOutcome,
  type ProjectWorkspaceVerifyIo,
} from "./pipeline_v2_project_provenance_store_internal.ts";
import { realImmutableDocumentIo, type ImmutableDocumentIo } from "./pipeline_v2_immutable_document_store_internal.ts";
import type {
  PreparedPipelineV2ProjectProvenance,
  PipelineV2ProjectProvenanceBinding,
} from "./pipeline_v2_project_provenance.ts";

/**
 * Filesystem publication and read-only loading of the durable project
 * provenance manifest (state schema v10).
 *
 * The manifest is published as one immutable canonical JSON artifact at
 * the fixed run-root location `<runRoot>/project-provenance.json` —
 * outside the worker-mounted `<runRoot>/project` tree — through the shared
 * immutable-document substrate (exclusive temp file, file fsync, exclusive
 * `link()` publication, parent fsync; byte-exact adoption on an exact
 * retry; typed conflict on anything else; `durability_unknown` with a
 * frozen candidate after the link when the parent fsync fails).
 *
 * `loadPipelineV2ProjectProvenance` is strictly read-only and returns
 * `null` when the artifact is absent. `loadVerifiedPipelineV2ProjectProvenance`
 * additionally binds the loaded manifest to the durable state binding
 * (digest, entry count, copy policy version) and refuses a missing
 * artifact as a typed integrity failure.
 *
 * `verifyPipelineV2InitialProjectWorkspace` proves that a materialized
 * project tree matches the manifest's workspace baseline exactly. It is
 * read-only and is deliberately only meaningful before the first worker
 * activation or during crash recovery before a first activation; after a
 * first activation the workspace is worker-mutable by contract.
 *
 * This module owns no diff, no Apply, no project lock and no public
 * projection; the coordinator and the restore verifier are its production
 * consumers. Diagnostics are content-free.
 */
export {
  PROJECT_PROVENANCE_FILE_NAME,
  PipelineV2ProjectProvenanceStoreError,
  type PipelineV2ProjectProvenanceStoreCandidate,
  type PipelineV2ProjectProvenanceStoreFailureReason,
  type PipelineV2ProjectProvenanceStoreOutcome,
};

/** Publishes the prepared provenance manifest at the fixed run-root path. */
export async function publishPipelineV2ProjectProvenance(
  runRoot: string,
  prepared: PreparedPipelineV2ProjectProvenance,
): Promise<string> {
  return await publishProjectProvenanceWithIo(
    realImmutableDocumentIo,
    runRoot,
    prepared,
  );
}

/** Read-only load; `null` when the artifact is absent. */
export async function loadPipelineV2ProjectProvenance(
  runRoot: string,
): Promise<PreparedPipelineV2ProjectProvenance | null> {
  return await loadProjectProvenanceWithIo(
    realImmutableDocumentIo,
    runRoot,
  );
}

/**
 * Read-only load bound to the durable state binding; a missing or
 * mismatching artifact is a typed integrity refusal.
 */
export async function loadVerifiedPipelineV2ProjectProvenance(
  runRoot: string,
  binding: PipelineV2ProjectProvenanceBinding,
): Promise<PreparedPipelineV2ProjectProvenance> {
  return await loadVerifiedProjectProvenanceWithIo(
    realImmutableDocumentIo,
    runRoot,
    binding,
  );
}

/**
 * Read-only verification that the tree at `workspaceRoot` matches the
 * manifest's workspace baseline exactly.
 */
export async function verifyPipelineV2InitialProjectWorkspace(
  workspaceRoot: string,
  prepared: PreparedPipelineV2ProjectProvenance,
): Promise<void> {
  return await verifyProjectWorkspaceBaselineWithIo(realProjectWorkspaceVerifyIo, workspaceRoot, prepared);
}

export type { ProjectWorkspaceVerifyIo, ImmutableDocumentIo };
