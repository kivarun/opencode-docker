import { isAbsolute } from "node:path";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical_json.ts";
import { isCleanAbsolutePath } from "./clean_path.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId, isLowercaseSha256 } from "./pipeline_v2_scalar.ts";
import {
  RunSnapshotDurabilityError,
  RunSnapshotStore,
  RunSnapshotStoreError,
  type PipelineStateIo,
} from "./run_snapshot_store.ts";
import {
  ensureImmutableDirectory,
  ImmutableDocumentStoreError,
  isImmutablePublicationTempName,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  verifyStoredDirectoryComponent,
  realImmutableDocumentIo,
  type ImmutableDocumentIo,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import {
  openImmutableBlobTemp,
  publishImmutableBlob,
  verifyStoredImmutableBlob,
  ImmutableBlobStoreError,
} from "./pipeline_v2_immutable_blob_internal.ts";
import {
  observeRegularFileStream,
  ProjectFileStreamError,
  realProjectFileStreamIo,
  type ProjectFileStreamIo,
} from "./pipeline_v2_project_stream_internal.ts";
import {
  observeProjectChangesTreesWithIo,
  PipelineV2ProjectChangesError,
  type PipelineV2ProjectChangesFailureReason,
} from "./pipeline_v2_project_changes_internal.ts";
import { realProjectWorkspaceVerifyIo, type ProjectWorkspaceVerifyIo } from "./pipeline_v2_project_provenance_store_internal.ts";
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import { isPipelineV2FinalizedRunStatus, type PipelineV2RunState } from "./pipeline_v2_state.ts";
import {
  applyPlanCanonicalJson,
  parseApplyPlan,
  applyPlanDigest,
  type PipelineV2ApplyPlan,
} from "./pipeline_v2_apply_plan_internal.ts";
import {
  parseApplyOperationState,
  validateApplyOperationState,
  type PipelineV2ApplyOperationState,
} from "./pipeline_v2_apply_state_internal.ts";
import {
  planProjectApplySteps,
  PipelineV2ApplyPlannerError,
  type ObservedApplyNode,
} from "./pipeline_v2_apply_planner_internal.ts";
import {
  MetadataProbeError,
  probeApplyObjectMetadata,
  realMetadataProbeIo,
  type MetadataProbeIo,
} from "./pipeline_v2_apply_metadata_probe_internal.ts";

/**
 * The durable Project Apply preparation package: the single production
 * facade that turns one reviewed, finalized pipeline v2 run plus one
 * exact Project Changes observation into a fully self-contained prepared
 * operation package — an immutable canonical plan, sealed payload blobs
 * for every future regular-file publication, and a crash-safe revisioned
 * operation journal — without writing a single byte into the registered
 * project.
 *
 * Authority: the caller passes only trusted internal parameters (the
 * apply-store root, the caller-owned apply id, the run id, the project
 * id, the canonical project root for the current read-only observation,
 * the requested observation digest, and the deployment run-state root).
 * The stored package binds the project through an opaque root-binding
 * digest over the canonical root and its current identity; the canonical
 * path itself is never persisted and recovery must re-resolve the root
 * through `run binding → frozen registry project → canonical project
 * root` and re-verify the binding before any future use.
 *
 * Stop condition honored by construction: the `prepared` status is
 * written only after the plan and every payload were published, re-read
 * and verified, every payload binding matched, the package completeness
 * was confirmed and the directory fsyncs completed (or a
 * `durability_unknown` was adopted through the established protocol).
 * No filesystem writer, no project lock, no HTTP/UI surface and no write
 * to the registered project exists in this increment.
 */

export const PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS = [
  "invalid_options",
  "run_not_found",
  "run_not_terminal",
  "run_state_invalid",
  "pipeline_mismatch",
  "artifact_missing",
  "artifact_modified",
  "stale_observation",
  "diverged_project",
  "unsupported_metadata",
  "unsupported_project_entry",
  "identity_conflict",
  "preparation_busy",
  "apply_not_prepared",
  "project_unstable",
  "workspace_unstable",
  "state_persist_failed",
  "internal_error",
] as const;
export type PipelineV2ProjectApplyFailureReason = (typeof PIPELINE_V2_PROJECT_APPLY_FAILURE_REASONS)[number];

export class PipelineV2ProjectApplyError extends Error {
  declare readonly reason: PipelineV2ProjectApplyFailureReason;

  constructor(reason: PipelineV2ProjectApplyFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ProjectApplyError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

const ROOT_BINDING_DIGEST_DOMAIN = "pipeline-v2-project-apply-root-binding\0";
const PLAN_FILE_NAME = "plan.json";
const PAYLOADS_DIR_NAME = "payloads";
const OPERATIONS_DIR_NAME = "operations";

export interface PipelineV2ProjectApplyOptions {
  /** Deployment-owned apply store root (mode 0700, real directory). */
  readonly applyStoreRoot: string;
  /** Caller-owned idempotency identity of the operation. */
  readonly applyId: string;
  readonly runId: string;
  readonly projectId: string;
  /** The canonical registered project root for the current read-only observation. */
  readonly projectRoot: string;
  /** The exact reviewed observation digest the operation is bound to. */
  readonly observation: string;
  /** The deployment run-state root (trusted internal configuration). */
  readonly stateRoot: string;
}

/** The per-call filesystem capabilities of one preparation. */
export interface PipelineV2ProjectApplyComponents {
  readonly docIo: ImmutableDocumentIo;
  readonly stateIo: PipelineStateIo;
  readonly verifyIo: ProjectWorkspaceVerifyIo;
  readonly fileStreamIo: ProjectFileStreamIo;
  readonly probeIo: MetadataProbeIo;
}

export interface PreparedPipelineV2ProjectApplyPackage {
  readonly apply_id: string;
  readonly run_id: string;
  readonly project_id: string;
  readonly observation: string;
  readonly terminal_revision: number;
  readonly plan_digest: string;
  readonly plan: PipelineV2ApplyPlan;
  readonly payload_count: number;
  readonly total_payload_byte_count: number;
  readonly operation_root: string;
  readonly payloads_root: string;
}

export interface PipelineV2ProjectApplyOperationProjection {
  readonly apply_id: string;
  readonly run_id: string;
  readonly project_id: string;
  readonly status: PipelineV2ApplyOperationState["status"];
  readonly revision: number;
  readonly observation: string;
  readonly plan_digest: string | null;
  readonly sealed_payload_count: number;
  readonly failure_reason: string | null;
  /** The durable execution progress (schema v2); null before the writer ran. */
  readonly execution: {
    readonly completed_step_count: number;
    readonly active_step: { readonly step_index: number } | null;
  } | null;
}

function applyFailure(reason: PipelineV2ProjectApplyFailureReason, message: string): PipelineV2ProjectApplyError {
  return new PipelineV2ProjectApplyError(reason, message);
}

function statePathLayout(stateRoot: string, id: string): string {
  return `${stateRoot.replace(/\/+$/, "")}/${OPERATIONS_DIR_NAME}/${id}/state.json`;
}

function applyStoreStatePath(applyStoreRoot: string, applyId: string): string {
  return statePathLayout(applyStoreRoot, applyId);
}

function operationRootOf(applyStoreRoot: string, applyId: string): string {
  return `${applyStoreRoot.replace(/\/+$/, "")}/${OPERATIONS_DIR_NAME}/${applyId}`;
}

function rootBindingDigest(canonicalRoot: string, dev: number, ino: number): string {
  return createHash("sha256")
    .update(ROOT_BINDING_DIGEST_DOMAIN)
    .update(canonicalJson({ canonical_root: canonicalRoot, dev, ino }), "utf8")
    .digest("hex");
}

/** Maps one typed Project Changes failure into the apply vocabulary. */
function mapChangesFailure(reason: PipelineV2ProjectChangesFailureReason): PipelineV2ProjectApplyFailureReason {
  switch (reason) {
    case "artifact_missing":
      return "artifact_missing";
    case "artifact_modified":
      return "artifact_modified";
    case "changes_not_ready":
      return "run_not_terminal";
    case "project_invalid":
      return "invalid_options";
    case "project_unstable":
      return "project_unstable";
    case "workspace_unstable":
      return "workspace_unstable";
    case "unsupported_project_entry":
      return "unsupported_project_entry";
    default:
      return "internal_error";
  }
}

async function requireCanonicalStoreRoot(
  docIo: ImmutableDocumentIo,
  applyStoreRoot: string,
): Promise<void> {
  if (typeof applyStoreRoot !== "string" || !isAbsolute(applyStoreRoot) || !isCleanAbsolutePath(applyStoreRoot)) {
    throw applyFailure("invalid_options", "the apply store root must be a clean absolute path");
  }
  const info = await docIo.lstatOrNull(applyStoreRoot).catch((cause: unknown) => {
    throw applyFailure("invalid_options", "the apply store root could not be inspected");
  });
  if (info === null) {
    throw applyFailure("invalid_options", "the apply store root does not exist");
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw applyFailure("invalid_options", "the apply store root is not a real directory");
  }
  if ((info.mode & 0o7777) !== 0o700) {
    throw applyFailure("invalid_options", "the apply store root does not have the required mode 0700");
  }
  const canonical = await docIo.realpath(applyStoreRoot).catch(() => {
    throw applyFailure("invalid_options", "the apply store root cannot be canonicalized");
  });
  if (canonical !== applyStoreRoot) {
    throw applyFailure("invalid_options", "the apply store root is not canonical");
  }
}

async function computeProjectRootBinding(
  docIo: ImmutableDocumentIo,
  projectRoot: string,
): Promise<string> {
  if (typeof projectRoot !== "string" || !isCleanAbsolutePath(projectRoot)) {
    throw applyFailure("invalid_options", "the project root must be a clean absolute path");
  }
  const info = await docIo.lstatOrNull(projectRoot).catch(() => {
    throw applyFailure("invalid_options", "the project root could not be inspected");
  });
  if (info === null || info.isSymbolicLink() || !info.isDirectory()) {
    throw applyFailure("invalid_options", "the project root is not a real directory");
  }
  const canonical = await docIo.realpath(projectRoot).catch(() => {
    throw applyFailure("invalid_options", "the project root cannot be canonicalized");
  });
  if (canonical !== projectRoot) {
    throw applyFailure("invalid_options", "the project root is not canonical");
  }
  return rootBindingDigest(projectRoot, info.dev, info.ino);
}

/** The journal statuses a preparation retry may still operate on. */
const EXECUTABLE_FOR_PREPARATION = new Set(["preparing", "failed", "prepared"]);

function identityOf(state: PipelineV2ApplyOperationState): {  applyId: string;
  runId: string;
  projectId: string;
  observation: string;
  rootBinding: string;
} {
  return {
    applyId: state.apply_id,
    runId: state.run_id,
    projectId: state.project_id,
    observation: state.observation,
    rootBinding: state.project_root_binding,
  };
}

function identityMatches(
  state: PipelineV2ApplyOperationState,
  applyId: string,
  runId: string,
  projectId: string,
  observation: string,
  rootBinding: string,
): boolean {
  const identity = identityOf(state);
  return (
    identity.applyId === applyId &&
    identity.runId === runId &&
    identity.projectId === projectId &&
    identity.observation === observation &&
    identity.rootBinding === rootBinding
  );
}

export interface OperationStore {
  load(): Promise<PipelineV2ApplyOperationState | null>;
  create(snapshot: PipelineV2ApplyOperationState): Promise<void>;
  commit(snapshot: PipelineV2ApplyOperationState, expectedRevision: number): Promise<void>;
}

function openOperationStore(
  applyStoreRoot: string,
  applyId: string,
  stateIo: PipelineStateIo,
): OperationStore {
  const store = new RunSnapshotStore<PipelineV2ApplyOperationState>(applyStoreRoot, applyId, {
    parseSnapshot: parseApplyOperationState,
    documentLabel: "apply operation state",
    pathLayout: statePathLayout,
  }, stateIo);
  return store;
}

/**
 * The stored state document must be a real non-symlink regular file with
 * mode 0600 before its content is parsed; anything else is an integrity
 * failure of the stored package, never an infrastructure error.
 */
async function requireStoredStateShape(docIo: ImmutableDocumentIo, statePath: string): Promise<void> {
  const info = await docIo.lstatOrNull(statePath).catch(() => {
    throw applyFailure("state_persist_failed", "the stored operation state could not be inspected");
  });
  if (info === null) {
    return;
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw applyFailure("artifact_modified", "the stored operation state exists but is not a regular state document");
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw applyFailure("artifact_modified", "the stored operation state does not have the required mode 0600");
  }
}

/**
 * Commits one state transition with the established reconciliation:
 * a `durability_unknown` adopts the visible candidate; a plain
 * `not_committed` reloads and adopts a strictly more advanced state of
 * the same identity, otherwise it is a persistence failure.
 */
async function commitOperationState(
  store: OperationStore,
  next: PipelineV2ApplyOperationState,
  expectedRevision: number,
  isAdvance: (current: PipelineV2ApplyOperationState) => boolean,
): Promise<PipelineV2ApplyOperationState> {
  try {
    await store.commit(next, expectedRevision);
    return next;
  } catch (cause) {
    if (cause instanceof RunSnapshotDurabilityError) {
      return validateApplyOperationState(cause.candidate);
    }
    if (cause instanceof RunSnapshotStoreError) {
      const reloaded = await store.load().catch(() => null);
      if (reloaded !== null && isAdvance(reloaded)) {
        return reloaded;
      }
      throw applyFailure("state_persist_failed", "the operation state could not be committed");
    }
    throw cause;
  }
}

function probesToApplyFailure(cause: MetadataProbeError, side: "project" | "workspace"): PipelineV2ProjectApplyError {
  if (cause.refusal === "unreachable") {
    return applyFailure(side === "project" ? "project_unstable" : "workspace_unstable", cause.message);
  }
  return applyFailure(cause.refusal === "unsupported_project_entry" ? "unsupported_project_entry" : "unsupported_metadata", cause.message);
}

interface ProbeTarget {
  readonly path: string;
  readonly role: "object" | "parent";
  readonly side: "project" | "workspace";
}

/**
 * Runs the metadata probe over every touched object, every existing
 * ancestor directory of an affected path, and every desired-after object.
 * Purely read-only; a refusal fails the whole preparation before any
 * artifact beyond the operation identity exists.
 */
async function probeApplyMetadata(
  probeIo: MetadataProbeIo,
  targets: readonly ProbeTarget[],
): Promise<void> {
  for (const target of targets) {
    try {
      await probeApplyObjectMetadata(probeIo, target.path, target.role);
    } catch (cause) {
      if (cause instanceof MetadataProbeError) {
        throw probesToApplyFailure(cause, target.side);
      }
      throw cause;
    }
  }
}

/**
 * The core preparation flow (fresh, resume, retry). No write to the
 * registered project happens on any path; every refusal before the
 * prepared marker leaves the project byte-identical.
 */
async function preparePipelineV2ProjectApplyWithComponents(
  options: PipelineV2ProjectApplyOptions,
  components: PipelineV2ProjectApplyComponents,
): Promise<PreparedPipelineV2ProjectApplyPackage> {
  const { docIo, stateIo, verifyIo, fileStreamIo, probeIo } = components;
  // 1. Option capture: one read per field, validated before any path.
  const applyStoreRoot = options.applyStoreRoot;
  const applyId = options.applyId;
  const runId = options.runId;
  const projectId = options.projectId;
  const projectRoot = options.projectRoot;
  const observation = options.observation;
  const stateRoot = options.stateRoot;
  if (!isPipelineV2SafeId(applyId) || !isPipelineV2SafeId(runId) || !isPipelineV2SafeId(projectId)) {
    throw applyFailure("invalid_options", "the apply, run and project ids must be safe ids");
  }
  if (typeof observation !== "string" || !isLowercaseSha256(observation)) {
    throw applyFailure("invalid_options", "the observation must be a lowercase 64-hex digest");
  }
  if (typeof stateRoot !== "string" || !isCleanAbsolutePath(stateRoot)) {
    throw applyFailure("invalid_options", "the state root must be a clean absolute path");
  }
  await requireCanonicalStoreRoot(docIo, applyStoreRoot);
  const rootBinding = await computeProjectRootBinding(docIo, projectRoot);

  // 2. The existing operation identity: the caller-owned apply id is the
  //    idempotency identity, so a conflicting retry is refused before any
  //    other run is even looked up.
  const store = openOperationStore(applyStoreRoot, applyId, stateIo);
  await requireStoredStateShape(docIo, applyStoreStatePath(applyStoreRoot, applyId));
  const existing = await store.load().catch((cause: unknown) => {
    if (cause instanceof Error && cause.name === "PipelineV2ApplyStateError") {
      throw applyFailure("artifact_modified", "the stored operation state is not a valid operation state");
    }
    throw applyFailure("state_persist_failed", "the stored operation state could not be read");
  });
  if (existing !== null && !identityMatches(existing, applyId, runId, projectId, observation, rootBinding)) {
    throw applyFailure("identity_conflict", "the operation identity does not match the stored operation");
  }
  if (existing !== null && !EXECUTABLE_FOR_PREPARATION.has(existing.status)) {
    // The filesystem writer owns every post-preparation status; a
    // preparation retry over an executing or finished operation is a
    // typed refusal, never a reinterpretation of the journal.
    throw applyFailure("apply_not_prepared", "the operation has already left the preparation phase");
  }

  // 3. The durable run through the shared read-only load chain.
  const read = await loadDurablePipelineV2Run({ runId, stateRoot });
  if (!read.ok) {
    throw applyFailure(read.reason, "the durable run could not be loaded");
  }
  const state: PipelineV2RunState = read.state;
  const runRoot: string = read.runRoot;
  if (!isPipelineV2FinalizedRunStatus(state.status)) {
    throw applyFailure("run_not_terminal", "the run has not reached its immutable terminal status");
  }
  if (existing !== null && existing.status === "prepared") {
    return await loadPreparedPackageWithComponents(components, applyStoreRoot, applyId, existing);
  }

  // 4. The full observation recompute through the single changes owner.
  let observed;
  try {
    observed = await observeProjectChangesTreesWithIo(verifyIo, runRoot, projectRoot, state, fileStreamIo);
  } catch (cause) {
    if (cause instanceof PipelineV2ProjectChangesError) {
      throw applyFailure(mapChangesFailure(cause.reason), cause.message);
    }
    throw cause;
  }
  if (observed.observation !== observation) {
    throw applyFailure("stale_observation", "the requested observation does not match the current trees");
  }
  if (existing !== null && existing.observation !== observed.observation) {
    throw applyFailure("stale_observation", "the stored operation observation does not match the current trees");
  }

  // 5. The operation identity: created exactly once, then resumed.
  const operationRoot = operationRootOf(applyStoreRoot, applyId);
  const operationsDir = await ensureImmutableDirectory(docIo, applyStoreRoot, OPERATIONS_DIR_NAME, "the apply operations directory", "the apply store root")
    .catch((cause: unknown) => {
      if (cause instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
  await ensureImmutableDirectory(docIo, operationsDir, applyId, "the apply operation directory", "the apply operations directory")
    .catch((cause: unknown) => {
      if (cause instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });

  let current = existing;
  if (current === null) {
    const initial: PipelineV2ApplyOperationState = validateApplyOperationState({
      schema_version: 2,
      kind: "pipeline_v2_project_apply_operation",
      revision: 1,
      apply_id: applyId,
      run_id: runId,
      project_id: projectId,
      observation: observed.observation,
      terminal_revision: state.revision,
      project_root_binding: rootBinding,
      plan_digest: null,
      plan_step_count: null,
      plan_payload_count: null,
      plan_total_payload_byte_count: null,
      sealed_payload_count: 0,
      status: "preparing",
      failure_reason: null,
      execution: null,
    });
    try {
      await store.create(initial);
      current = initial;
    } catch (cause) {
      const reloaded = await store.load().catch(() => null);
      if (reloaded !== null && identityMatches(reloaded, applyId, runId, projectId, observed.observation, rootBinding)) {
        current = reloaded;
      } else if (cause instanceof RunSnapshotDurabilityError) {
        current = validateApplyOperationState(cause.candidate);
      } else {
        throw applyFailure("state_persist_failed", "the operation identity could not be created");
      }
    }
  }

  // 6. The metadata probe (read-only) and the planner.
  const workspaceRoot = join(runRoot, "project");
  try {
    const probeList: ProbeTarget[] = [
      { path: projectRoot, role: "parent", side: "project" },
    ];
    for (const change of observed.changes) {
      const segments = change.path.split("/");
      for (let index = 1; index < segments.length; index += 1) {
        const ancestor = segments.slice(0, index).join("/");
        const ancestorNode = observed.operator.get(ancestor);
        if (ancestorNode !== undefined && ancestorNode.kind === "directory") {
          probeList.push({ path: join(projectRoot, ancestor), role: "parent", side: "project" });
        }
      }
      if (observed.operator.has(change.path)) {
        probeList.push({ path: join(projectRoot, change.path), role: "object", side: "project" });
      }
      if (change.result !== null) {
        probeList.push({ path: join(workspaceRoot, change.path), role: "object", side: "workspace" });
      }
    }
    await probeApplyMetadata(probeIo, probeList);
  } catch (cause) {
    await recordFailure(store, current, cause);
    throw cause;
  }

  let plan: PipelineV2ApplyPlan;
  try {
    plan = planProjectApplySteps(
      { operator: observed.operator, changes: observed.changes, baseline: observed.baseline },
      applyId,
      runId,
      projectId,
      rootBinding,
      observed.observation,
      state.revision,
    );
  } catch (cause) {
    if (cause instanceof PipelineV2ApplyPlannerError) {
      const failure = applyFailure(cause.refusal === "diverged_project" ? "diverged_project" : cause.refusal === "unsupported_metadata" ? "unsupported_metadata" : "unsupported_project_entry", cause.message);
      await recordFailure(store, current, failure);
      throw failure;
    }
    await recordFailure(store, current, applyFailure("internal_error", "the apply plan could not be built"));
    throw cause;
  }
  // 7. The immutable plan publication.
  const payloadsDir = await ensureImmutableDirectory(docIo, operationRoot, PAYLOADS_DIR_NAME, "the apply payloads directory", "the apply operation directory")
    .catch((cause: unknown) => {
      if (cause instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
  const planCanonical = canonicalJson(plan as unknown as Record<string, unknown>);
  const planDigest = applyPlanDigest(plan);
  if (current.plan_digest !== null && current.plan_digest !== planDigest) {
    throw applyFailure("artifact_modified", "the stored plan digest does not match the freshly planned operation");
  }
  if (current.plan_digest === null) {
    await publishImmutableDocumentFile(
      docIo,
      operationRoot,
      {
        fileName: PLAN_FILE_NAME,
        tempPrefix: ".tmp-plan-",
        tempStem: "plan",
        canonicalJson: planCanonical,
        sha256: planDigest,
        identity: { apply_id: applyId, artifact: "plan" },
      },
      { document: "apply plan", publicationFailed: "the apply plan publication failed" },
      "the apply operation directory",
    ).catch((cause: unknown) => {
      if (cause instanceof ImmutableDocumentStoreError) {
        throw cause.outcome === "durability_unknown"
          ? applyFailure("state_persist_failed", "the apply plan publication durability could not be confirmed")
          : cause.reason === "conflict"
            ? applyFailure("artifact_modified", cause.message)
            : applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
  }
  // Read the plan back through the stored-document contract and verify it.
  const storedPlanBytes = await readStoredImmutableDocument(
    docIo,
    `${operationRoot}/${PLAN_FILE_NAME}`,
    "the apply plan",
    { document: "apply plan", publicationFailed: "the apply plan publication failed" },
  ).catch((cause: unknown) => {
    if (cause instanceof ImmutableDocumentStoreError) {
      throw applyFailure(cause.reason === "conflict" ? "artifact_modified" : "artifact_missing", cause.message);
    }
    throw cause;
  });
  let storedPlan: PipelineV2ApplyPlan;
  try {
    storedPlan = parseApplyPlan(storedPlanBytes.toString("utf8"));
  } catch (cause) {
    throw applyFailure("artifact_modified", "the stored apply plan is not a valid plan document");
  }
  if (storedPlanBytes.toString("utf8") !== applyPlanCanonicalJson(storedPlan)) {
    throw applyFailure("artifact_modified", "the stored apply plan is not carried in its canonical form");
  }
  if (applyPlanDigest(storedPlan) !== planDigest) {
    throw applyFailure("artifact_modified", "the stored apply plan does not match its digest");
  }
  if (current.plan_digest === null) {
    const beforeRevision = current.revision;
    current = await commitOperationState(
      store,
      validateApplyOperationState({
        ...current,
        revision: current.revision + 1,
        plan_digest: planDigest,
        plan_step_count: plan.steps.length,
        plan_payload_count: plan.payloads.length,
        plan_total_payload_byte_count: plan.total_payload_byte_count,
        status: current.status === "failed" ? "preparing" : current.status,
      }),
      beforeRevision,
      (reloaded) => reloaded.plan_digest === planDigest && reloaded.revision >= beforeRevision + 1,
    );
  }

  // 8. Payload sealing: one immutable blob per publishing step, in
  //    payload order, each verified by one single-pass stream with the
  //    plan's content contract as its expectation.
  for (const payload of plan.payloads) {
    if (payload.payload_index <= current.sealed_payload_count) {
      continue;
    }
    const step = plan.steps.find((candidate) => candidate.kind === "publish_regular_file" && candidate.payload_index === payload.payload_index);
    if (step === undefined) {
      throw applyFailure("internal_error", "a payload binding lost its publishing step");
    }
    const sourcePath = join(workspaceRoot, step.path);
    const temp = await openImmutableBlobTemp(docIo, payloadsDir, ".tmp-payload-", `${payload.payload_index}`, {
      document: "apply payload",
      publicationFailed: "the apply payload publication failed",
    }).catch((cause: unknown) => {
      if (cause instanceof ImmutableBlobStoreError || cause instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
    let observed_;
    let sinkStoreFailure: unknown = undefined;
    try {
      observed_ = await observeRegularFileStream(fileStreamIo, sourcePath, {
        expected: { byteCount: payload.byte_count, sha256: payload.sha256 },
        sink: async (chunk) => {
          try {
            await temp.writeChunk(chunk);
          } catch (cause) {
            sinkStoreFailure = cause;
            throw cause;
          }
        },
      });
    } catch (cause) {
      await temp.discard();
      if (sinkStoreFailure instanceof ImmutableBlobStoreError || sinkStoreFailure instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", "the apply payload temp file could not be written");
      }
      if (cause instanceof ProjectFileStreamError) {
        throw applyFailure(cause.reason === "unstable" ? "workspace_unstable" : "internal_error", cause.message);
      }
      throw cause;
    }
    if (observed_.byteCount !== payload.byte_count || observed_.sha256 !== payload.sha256) {
      await temp.discard();
      throw applyFailure("workspace_unstable", "the sealed payload does not match its reviewed content contract");
    }
    await temp.syncAndClose().catch(async (cause: unknown) => {
      await temp.discard();
      if (cause instanceof ImmutableBlobStoreError || cause instanceof ImmutableDocumentStoreError) {
        throw applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
    await publishImmutableBlob(
      docIo,
      payloadsDir,
      temp,
      payload.artifact,
      { sha256: payload.sha256, byteCount: payload.byte_count },
      { document: "apply payload", publicationFailed: "the apply payload publication failed" },
      "the apply payloads directory",
    ).catch((cause: unknown) => {
      if (cause instanceof ImmutableBlobStoreError) {
        throw cause.outcome === "durability_unknown"
          ? applyFailure("state_persist_failed", "the apply payload publication durability could not be confirmed")
          : cause.reason === "conflict"
            ? applyFailure("artifact_modified", cause.message)
            : applyFailure("state_persist_failed", cause.message);
      }
      throw cause;
    });
    const beforeRevision = current.revision;
    current = await commitOperationState(
      store,
      validateApplyOperationState({
        ...current,
        revision: current.revision + 1,
        sealed_payload_count: payload.payload_index,
        status: current.status === "failed" ? "preparing" : current.status,
        failure_reason: null,
      }),
      beforeRevision,
      (reloaded) => reloaded.sealed_payload_count >= payload.payload_index && reloaded.revision >= beforeRevision + 1,
    );
  }

  // 9. Package completeness through the loader core, then the prepared marker.
  await verifyPreparedPackageContentsWithComponents(components, applyStoreRoot, applyId, current);
  const preparedState = validateApplyOperationState({
    ...current,
    revision: current.revision + 1,
    status: "prepared",
    failure_reason: null,
  });
  current = await commitOperationState(store, preparedState, current.revision, (reloaded) => reloaded.status === "prepared" && reloaded.plan_digest === planDigest);
  return deepFreezeValue({
    apply_id: applyId,
    run_id: runId,
    project_id: projectId,
    observation: observed.observation,
    terminal_revision: state.revision,
    plan_digest: planDigest,
    plan: storedPlan,
    payload_count: plan.payloads.length,
    total_payload_byte_count: plan.total_payload_byte_count,
    operation_root: operationRoot,
    payloads_root: payloadsDir,
  });
}

/** Best-effort durable failure record; never masks the original failure. */
async function recordFailure(
  store: OperationStore,
  current: PipelineV2ApplyOperationState | null,
  cause: unknown,
): Promise<void> {
  if (current === null || current.status === "prepared") {
    return;
  }
  const reason = cause instanceof PipelineV2ProjectApplyError ? cause.reason : "internal_error";
  try {
    await store.commit(
      validateApplyOperationState({ ...current, revision: current.revision + 1, status: "failed", failure_reason: reason }),
      current.revision,
    );
  } catch {
    // Best-effort: the original typed failure propagates either way.
  }
}

/**
 * Loads one prepared package read-only: the operation state (when not
 * already known), the layout/modes, the canonical plan, its digest and
 * identity binding, and every payload artifact. The run workspace is
 * never needed; nothing is created, repaired or removed.
 */
async function loadPreparedPackageWithComponents(
  components: PipelineV2ProjectApplyComponents,
  applyStoreRoot: string,
  applyId: string,
  knownState?: PipelineV2ApplyOperationState,
): Promise<PreparedPipelineV2ProjectApplyPackage> {
  const { docIo, stateIo } = components;
  await requireCanonicalStoreRoot(docIo, applyStoreRoot);
  if (!isPipelineV2SafeId(applyId)) {
    throw applyFailure("invalid_options", "the apply id must be a safe id");
  }
  let state: PipelineV2ApplyOperationState | undefined = knownState;
  if (state === undefined) {
    const store = openOperationStore(applyStoreRoot, applyId, stateIo);
    await requireStoredStateShape(docIo, applyStoreStatePath(applyStoreRoot, applyId));
    const loaded = await store.load().catch((cause: unknown) => {
      if (cause instanceof Error && cause.name === "PipelineV2ApplyStateError") {
        throw applyFailure("artifact_modified", "the stored operation state is not a valid operation state");
      }
      throw applyFailure("state_persist_failed", "the stored operation state could not be read");
    });
    if (loaded === null) {
      throw applyFailure("artifact_missing", "the operation does not exist");
    }
    state = loaded;
  }
  if (state.status !== "prepared") {
    throw applyFailure("artifact_missing", "the operation is not prepared");
  }
  return await verifyPreparedPackageContentsWithComponents(components, applyStoreRoot, applyId, state);
}

/**
 * The read-only package verifier: layout, modes, state, canonical plan,
 * digest and identity binding, dense indexes, payload existence and
 * exact digests, and the absence of extra payload artifacts. Nothing is
 * created, repaired or removed; the run workspace is never needed.
 */
async function verifyPreparedPackageContentsWithComponents(
  components: { readonly docIo: ImmutableDocumentIo },
  applyStoreRoot: string,
  applyId: string,
  state: PipelineV2ApplyOperationState,
): Promise<PreparedPipelineV2ProjectApplyPackage> {
  const { docIo } = components;
  const operationRoot = operationRootOf(applyStoreRoot, applyId);
  const operationsDir = await verifyStoredDirectoryComponent(docIo, applyStoreRoot, OPERATIONS_DIR_NAME, "the apply operations directory").catch(wrapLayout);
  if (operationsDir === null) {
    throw applyFailure("artifact_missing", "the apply operations directory does not exist");
  }
  const operationDir = await verifyStoredDirectoryComponent(docIo, operationsDir, applyId, "the apply operation directory").catch(wrapLayout);
  if (operationDir === null) {
    throw applyFailure("artifact_missing", "the apply operation directory does not exist");
  }
  const payloadsDir = await verifyStoredDirectoryComponent(docIo, operationDir, PAYLOADS_DIR_NAME, "the apply payloads directory").catch(wrapLayout);
  if (payloadsDir === null) {
    throw applyFailure("artifact_missing", "the apply payloads directory does not exist");
  }
  const planBytes = await readStoredImmutableDocument(
    docIo,
    `${operationDir}/${PLAN_FILE_NAME}`,
    "the apply plan",
    { document: "apply plan", publicationFailed: "the apply plan publication failed" },
  ).catch((cause: unknown) => {
    if (cause instanceof ImmutableDocumentStoreError) {
      throw applyFailure(cause.reason === "conflict" ? "artifact_modified" : "artifact_missing", cause.message);
    }
    throw cause;
  });
  let plan: PipelineV2ApplyPlan;
  try {
    plan = parseApplyPlan(planBytes.toString("utf8"));
  } catch {
    throw applyFailure("artifact_modified", "the stored apply plan is not a valid plan document");
  }
  if (planBytes.toString("utf8") !== applyPlanCanonicalJson(plan)) {
    throw applyFailure("artifact_modified", "the stored apply plan is not carried in its canonical form");
  }
  if (applyPlanDigest(plan) !== state.plan_digest) {
    throw applyFailure("artifact_modified", "the stored apply plan does not match its journal digest");
  }
  if (
    plan.apply_id !== state.apply_id ||
    plan.run_id !== state.run_id ||
    plan.project_id !== state.project_id ||
    plan.observation !== state.observation ||
    plan.terminal_revision !== state.terminal_revision ||
    plan.project_root_binding !== state.project_root_binding
  ) {
    throw applyFailure("artifact_modified", "the stored apply plan identity does not match its journal");
  }
  if (
    plan.steps.length !== state.plan_step_count ||
    plan.payloads.length !== state.plan_payload_count ||
    plan.total_payload_byte_count !== state.plan_total_payload_byte_count
  ) {
    throw applyFailure("artifact_modified", "the stored apply plan does not match its journal projection");
  }
  // Removal must precede creation for every two-step substitution.
  const firstStepByPath = new Map<string, number>();
  for (const step of plan.steps) {
    const previous = firstStepByPath.get(step.path);
    if (previous !== undefined && (step.kind === "create_directory" || step.kind === "publish_regular_file")) {
      // A creation after another step on the same path must follow a removal.
      const removalStep = plan.steps.find((candidate) => candidate.path === step.path && (candidate.kind === "remove_leaf" || candidate.kind === "remove_directory"));
      if (removalStep === undefined || removalStep.step_index > step.step_index) {
        throw applyFailure("artifact_modified", "the stored apply plan carries a creation without its preceding removal");
      }
    }
    if (previous === undefined) {
      firstStepByPath.set(step.path, step.step_index);
    }
  }
  // The payloads directory carries exactly the bound artifacts (own
  // publication-temp orphans of the shared substrate grammar tolerated).
  const names = await docIo.readdirNames(payloadsDir).catch(() => {
    throw applyFailure("artifact_missing", "the apply payloads directory could not be read");
  });
  const boundArtifacts = new Set(plan.payloads.map((payload) => payload.artifact));
  for (const name of names) {
    if (boundArtifacts.has(name)) {
      continue;
    }
    if (isImmutablePublicationTempName(name, ".tmp-payload-")) {
      continue;
    }
    throw applyFailure("artifact_modified", "the apply payloads directory carries an unexpected artifact");
  }
  for (const payload of plan.payloads) {
    const payloadPath = `${payloadsDir}/${payload.artifact}`;
    const payloadInfo = await docIo.lstatOrNull(payloadPath).catch(() => {
      throw applyFailure("artifact_modified", "the apply payload artifact could not be inspected");
    });
    if (payloadInfo === null) {
      throw applyFailure("artifact_missing", "the apply payload artifact does not exist");
    }
    await verifyStoredImmutableBlob(
      docIo,
      payloadPath,
      { sha256: payload.sha256, byteCount: payload.byte_count },
      { document: "apply payload", publicationFailed: "the apply payload publication failed" },
    ).catch((cause: unknown) => {
      if (cause instanceof ImmutableBlobStoreError) {
        throw applyFailure(cause.reason === "conflict" ? "artifact_modified" : "artifact_missing", cause.message);
      }
      throw cause;
    });
  }
  return deepFreezeValue({
    apply_id: state.apply_id,
    run_id: state.run_id,
    project_id: state.project_id,
    observation: state.observation,
    terminal_revision: state.terminal_revision,
    plan_digest: state.plan_digest!,
    plan,
    payload_count: plan.payloads.length,
    total_payload_byte_count: plan.total_payload_byte_count,
    operation_root: operationDir,
    payloads_root: payloadsDir,
  });
}

function wrapLayout(cause: unknown): never {
  if (cause instanceof ImmutableDocumentStoreError) {
    throw applyFailure(cause.reason === "conflict" ? "artifact_modified" : "artifact_missing", cause.message);
  }
  throw cause;
}

/** The read-only projection of one operation's journal state. */
async function inspectOperationWithComponents(
  components: PipelineV2ProjectApplyComponents,
  applyStoreRoot: string,
  applyId: string,
): Promise<PipelineV2ProjectApplyOperationProjection> {
  const { docIo, stateIo } = components;
  await requireCanonicalStoreRoot(docIo, applyStoreRoot);
  if (!isPipelineV2SafeId(applyId)) {
    throw applyFailure("invalid_options", "the apply id must be a safe id");
  }
  const store = openOperationStore(applyStoreRoot, applyId, stateIo);
  await requireStoredStateShape(docIo, applyStoreStatePath(applyStoreRoot, applyId));
  const state = await store.load().catch((cause: unknown) => {
    if (cause instanceof Error && cause.name === "PipelineV2ApplyStateError") {
      throw applyFailure("artifact_modified", "the stored operation state is not a valid operation state");
    }
    throw applyFailure("state_persist_failed", "the stored operation state could not be read");
  });
  if (state === null) {
    throw applyFailure("artifact_missing", "the operation does not exist");
  }
  return deepFreezeValue({
    apply_id: state.apply_id,
    run_id: state.run_id,
    project_id: state.project_id,
    status: state.status,
    revision: state.revision,
    observation: state.observation,
    plan_digest: state.plan_digest,
    sealed_payload_count: state.sealed_payload_count,
    failure_reason: state.failure_reason,
    execution:
      state.execution === null
        ? null
        : deepFreezeValue({
            completed_step_count: state.execution.completed_step_count,
            active_step: state.execution.active_step === null ? null : deepFreezeValue({ step_index: state.execution.active_step.step_index }),
          }),
  });
}

export {
  preparePipelineV2ProjectApplyWithComponents as preparePipelineV2ProjectApplyCore,
  loadPreparedPackageWithComponents as loadPreparedPackageCore,
  inspectOperationWithComponents as inspectOperationCore,
  applyStoreStatePath,
};

/** The statuses whose stored result the execution facade returns verbatim. */
const EXECUTION_TERMINAL_STATUSES = new Set(["applied", "conflict", "failed"]);

export interface LoadedApplyOperationForExecution {
  readonly state: PipelineV2ApplyOperationState;
  /** The verified package; null exactly for the terminal-result statuses. */
  readonly pkg: PreparedPipelineV2ProjectApplyPackage | null;
}

/**
 * Loads one operation for the filesystem writer: the journal state of any
 * status, plus the fully verified package exactly when the operation is
 * still executable (`prepared`/`applying`). Terminal-result statuses
 * return without touching payload artifacts; every executable status gets
 * the full package verification (plan, payloads, layout, modes).
 */
async function loadApplyOperationForExecutionCore(
  components: { readonly docIo: ImmutableDocumentIo; readonly stateIo: PipelineStateIo },
  applyStoreRoot: string,
  applyId: string,
): Promise<LoadedApplyOperationForExecution> {
  const { docIo, stateIo } = components;
  await requireCanonicalStoreRoot(docIo, applyStoreRoot);
  if (!isPipelineV2SafeId(applyId)) {
    throw applyFailure("invalid_options", "the apply id must be a safe id");
  }
  const store = openOperationStore(applyStoreRoot, applyId, stateIo);
  await requireStoredStateShape(docIo, applyStoreStatePath(applyStoreRoot, applyId));
  const state = await store.load().catch((cause: unknown) => {
    if (cause instanceof Error && cause.name === "PipelineV2ApplyStateError") {
      throw applyFailure("artifact_modified", "the stored operation state is not a valid operation state");
    }
    throw applyFailure("state_persist_failed", "the stored operation state could not be read");
  });
  if (state === null) {
    throw applyFailure("artifact_missing", "the operation does not exist");
  }
  if (EXECUTION_TERMINAL_STATUSES.has(state.status)) {
    return { state, pkg: null };
  }
  if (state.status !== "prepared" && state.status !== "applying" && state.status !== "recovery_required") {
    throw applyFailure("apply_not_prepared", "the operation is not in an executable state");
  }
  return { state, pkg: await verifyPreparedPackageContentsWithComponents(components, applyStoreRoot, applyId, state) };
}

export {
  loadApplyOperationForExecutionCore as loadApplyOperationForExecution,
  commitOperationState as commitApplyOperationState,
  openOperationStore as openApplyOperationStore,
  rootBindingDigest as computeApplyRootBindingDigest,
};
