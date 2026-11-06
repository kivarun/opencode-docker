/**
 * Filesystem publication and read-only loading of the durable profile
 * epoch manifest (state schema v12).
 *
 * The manifest is published as one immutable canonical JSON artifact at
 * the fixed run-root location `<runRoot>/profile-epoch.json` through the
 * shared immutable-document substrate (exclusive temp file, file fsync,
 * exclusive `link()` publication, parent fsync; byte-exact adoption on an
 * exact retry; typed conflict on anything else; `durability_unknown` with
 * a frozen candidate after the link when the parent fsync fails). The run
 * root itself is a store-adopted parent: verified real non-symlink 0700
 * directory, never created, chmodded or removed by this module.
 *
 * `loadPipelineV2ProfileEpoch` is strictly read-only and returns `null`
 * when the artifact is absent. `loadVerifiedPipelineV2ProfileEpoch`
 * additionally binds the loaded manifest to the durable state binding
 * (digest, profile count) and refuses a missing artifact as a typed
 * integrity failure.
 *
 * `verifyPipelineV2ProfileEpochForExistingRun` is the single verification
 * entry of every mutating existing-run flow: it proves the stored artifact
 * against the durable binding and then recomputes the epoch manifest from
 * the freshly loaded profile identities and requires exact canonical-byte
 * equality with the stored artifact and digest equality with the binding.
 * The recomputation uses the same loaded profile snapshot the runtime
 * factory receives; a mismatch is the typed public refusal
 * `profile_epoch_mismatch` (zero durable writes, zero Sessions).
 *
 * This module contains no second fsync/link/adoption implementation and
 * no public projection; diagnostics are content-free.
 */

import {
  ImmutableDocumentStoreError,
  immutableDocumentIoFailure,
  inspectStoredDocumentOrNull,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  requireImmutableDocumentRunRoot,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import { realImmutableDocumentIo } from "./pipeline_v2_immutable_document_store_internal.ts";
import {
  parsePipelineV2ProfileEpoch,
  PipelineV2ProfileEpochError,
  pipelineV2ProfileEpochDigest,
  preparePipelineV2ProfileEpoch,
  requireProfileEpochBindingMatch,
  validatePipelineV2ProfileEpochBinding,
  type PipelineV2ProfileEpochBinding,
  type PreparedPipelineV2ProfileEpoch,
  type ProfileEpochProfileEntry,
} from "./pipeline_v2_profile_epoch.ts";
import { PipelineV2PublicReasonError } from "./pipeline_v2_public_reason.ts";

export const PROFILE_EPOCH_FILE_NAME = "profile-epoch.json";
const PROFILE_EPOCH_TEMP_PREFIX = ".profile-epoch-";

const WORDING: ImmutableDocumentWording = Object.freeze({
  document: "profile epoch manifest",
  publicationFailed: "the profile epoch manifest publication failed",
});

/** Typed failure of the profile epoch store adapter. */
export type PipelineV2ProfileEpochStoreFailureReason = "invalid_layout" | "conflict" | "io_failure";
export type PipelineV2ProfileEpochStoreOutcome = "not_published" | "durability_unknown";

export interface PipelineV2ProfileEpochStoreCandidate {
  readonly kind: "profile_epoch";
  readonly digest: string;
  readonly final_path: string;
  readonly sha256: string;
}

export class PipelineV2ProfileEpochStoreError extends Error {
  declare readonly outcome: PipelineV2ProfileEpochStoreOutcome;
  declare readonly reason: PipelineV2ProfileEpochStoreFailureReason;
  declare readonly candidate?: PipelineV2ProfileEpochStoreCandidate;

  constructor(
    outcome: PipelineV2ProfileEpochStoreOutcome,
    reason: PipelineV2ProfileEpochStoreFailureReason,
    message: string,
    candidate?: PipelineV2ProfileEpochStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2ProfileEpochStoreError";
    Object.defineProperty(this, "outcome", { value: outcome, enumerable: true, writable: false, configurable: false });
    Object.defineProperty(this, "reason", { value: reason, enumerable: true, writable: false, configurable: false });
    if (candidate !== undefined) {
      Object.defineProperty(this, "candidate", { value: candidate, enumerable: true, writable: false, configurable: false });
    }
  }
}

function invalidLayout(message: string): PipelineV2ProfileEpochStoreError {
  return new PipelineV2ProfileEpochStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2ProfileEpochStoreError {
  return new PipelineV2ProfileEpochStoreError("not_published", "conflict", message);
}

interface EpochCandidateIdentity {
  readonly kind?: unknown;
}

function epochStoreErrorFromSubstrate(cause: ImmutableDocumentStoreError): PipelineV2ProfileEpochStoreError {
  const substrateCandidate = cause.candidate;
  if (substrateCandidate === undefined) {
    return new PipelineV2ProfileEpochStoreError(cause.outcome, cause.reason, cause.message);
  }
  const identity = substrateCandidate.identity as EpochCandidateIdentity;
  if (identity === undefined || identity === null || identity.kind !== "profile_epoch") {
    return new PipelineV2ProfileEpochStoreError(cause.outcome, cause.reason, cause.message);
  }
  return new PipelineV2ProfileEpochStoreError(cause.outcome, cause.reason, cause.message, {
    kind: "profile_epoch",
    digest: substrateCandidate.sha256,
    final_path: substrateCandidate.final_path,
    sha256: substrateCandidate.sha256,
  });
}

async function withEpochStoreGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2ProfileEpochStoreError) {
      throw cause;
    }
    if (cause instanceof PipelineV2ProfileEpochError) {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw epochStoreErrorFromSubstrate(cause);
    }
    throw epochStoreErrorFromSubstrate(immutableDocumentIoFailure(WORDING.publicationFailed));
  }
}

/**
 * Publishes one prepared epoch manifest at `<runRoot>/profile-epoch.json`.
 * The run root must be an existing absolute canonical real non-symlink
 * directory; it is never created, chmodded or removed. An exact retry
 * (same canonical bytes) adopts the published artifact.
 */
export async function publishPipelineV2ProfileEpoch(
  runRoot: string,
  prepared: PreparedPipelineV2ProfileEpoch,
): Promise<string> {
  return await withEpochStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(realImmutableDocumentIo, runRoot);
    await publishImmutableDocumentFile(
      realImmutableDocumentIo,
      runRootCanonical,
      {
        fileName: PROFILE_EPOCH_FILE_NAME,
        tempPrefix: PROFILE_EPOCH_TEMP_PREFIX,
        tempStem: "publish",
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: Object.freeze({ kind: "profile_epoch" }),
      },
      WORDING,
      "run root",
    );
    return `${runRootCanonical}/${PROFILE_EPOCH_FILE_NAME}`;
  });
}

/**
 * Read-only load of the stored epoch manifest; `null` when the artifact is
 * absent (the caller decides whether absence is legal in its phase).
 */
export async function loadPipelineV2ProfileEpoch(
  runRoot: string,
): Promise<PreparedPipelineV2ProfileEpoch | null> {
  return await withEpochStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(realImmutableDocumentIo, runRoot);
    const finalPath = `${runRootCanonical}/${PROFILE_EPOCH_FILE_NAME}`;
    const artifactInfo = await inspectStoredDocumentOrNull(realImmutableDocumentIo, finalPath, "profile epoch manifest file");
    if (artifactInfo === null) {
      return null;
    }
    return await loadStoredEpoch(finalPath);
  });
}

async function loadStoredEpoch(finalPath: string): Promise<PreparedPipelineV2ProfileEpoch> {
  const stored = await readStoredImmutableDocument(realImmutableDocumentIo, finalPath, "profile epoch manifest file", WORDING);
  const raw = stored.toString("utf8");
  let prepared: PreparedPipelineV2ProfileEpoch;
  try {
    prepared = parsePipelineV2ProfileEpoch(raw);
  } catch (cause) {
    if (cause instanceof PipelineV2ProfileEpochError) {
      throw conflict("the stored profile epoch manifest is not a valid profile epoch manifest");
    }
    throw cause;
  }
  if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
    throw conflict("the stored profile epoch manifest file does not carry its own canonical JSON");
  }
  const recomputed = pipelineV2ProfileEpochDigest(prepared.manifest);
  if (recomputed !== prepared.sha256) {
    throw conflict("the stored profile epoch manifest does not match its own digest");
  }
  return prepared;
}

/**
 * Loads the stored manifest and confirms it against the durable state
 * binding (digest, profile count). A missing artifact is a typed
 * integrity refusal, not `null`.
 */
export async function loadVerifiedPipelineV2ProfileEpoch(
  runRoot: string,
  binding: PipelineV2ProfileEpochBinding,
): Promise<PreparedPipelineV2ProfileEpoch> {
  return await withEpochStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(realImmutableDocumentIo, runRoot);
    const finalPath = `${runRootCanonical}/${PROFILE_EPOCH_FILE_NAME}`;
    const artifactInfo = await inspectStoredDocumentOrNull(realImmutableDocumentIo, finalPath, "profile epoch manifest file");
    if (artifactInfo === null) {
      throw invalidLayout("the profile epoch manifest does not exist");
    }
    const prepared = await loadStoredEpoch(finalPath);
    const validatedBinding = validatePipelineV2ProfileEpochBinding(binding, "the durable profile epoch binding");
    try {
      requireProfileEpochBindingMatch(prepared, validatedBinding);
    } catch (cause) {
      if (cause instanceof PipelineV2ProfileEpochError) {
        throw conflict("the durable profile epoch binding does not match the stored profile epoch manifest");
      }
      throw cause;
    }
    return prepared;
  });
}

/**
 * The typed refusal reasons of the existing-run epoch verification. All
 * three are members of the closed public reason vocabulary; the errors
 * are carried by `PipelineV2PublicReasonError` so the runner boundary
 * maps them without any message parsing.
 */
export type PipelineV2ProfileEpochVerificationRefusal =
  | "artifact_missing"
  | "artifact_modified"
  | "profile_epoch_mismatch";

/**
 * The single verification entry of every mutating existing-run flow. It
 * proves the stored artifact against the durable binding and then
 * recomputes the epoch manifest from the freshly loaded profile identity
 * entries (the same loaded snapshot the runtime factory receives) and
 * requires exact canonical-byte equality with the stored artifact and
 * digest/count equality with the binding. Strictly read-only: no
 * filesystem mutation, no durable write, no Session.
 *
 * Failures:
 * - the artifact is absent → `artifact_missing`;
 * - the artifact is damaged, non-canonical, wrongly-moded, foreign or its
 *   own digest diverges → `artifact_modified`;
 * - the recomputed manifest differs from the stored artifact or from the
 *   durable binding → `profile_epoch_mismatch`.
 */
export async function verifyPipelineV2ProfileEpochForExistingRun(options: {
  readonly runRoot: string;
  readonly binding: unknown;
  readonly profileEntries: readonly ProfileEpochProfileEntry[];
}): Promise<void> {
  const { runRoot, binding, profileEntries } = options;
  try {
    const validatedBinding = validatePipelineV2ProfileEpochBinding(binding, "the durable profile epoch binding");
    const stored = await loadVerifiedPipelineV2ProfileEpoch(runRoot, validatedBinding);
    const recomputed = preparePipelineV2ProfileEpoch(profileEntries);
    if (
      recomputed.sha256 !== validatedBinding.digest ||
      recomputed.manifest.profiles.length !== validatedBinding.profile_count ||
      recomputed.canonical_json !== stored.canonical_json
    ) {
      throw new PipelineV2PublicReasonError(
        "profile_epoch_mismatch",
        "the effective execution profiles do not match the durable profile epoch of this run",
      );
    }
  } catch (cause) {
    if (cause instanceof PipelineV2PublicReasonError) {
      throw cause;
    }
    if (cause instanceof PipelineV2ProfileEpochStoreError) {
      // Typed-field mapping only, never message text: the artifact is
      // missing, damaged, or the read itself failed.
      if (cause.outcome === "not_published" && cause.reason === "invalid_layout") {
        throw new PipelineV2PublicReasonError("artifact_missing", "the profile epoch manifest is missing from the run root");
      }
      if (cause.outcome === "not_published" && cause.reason === "conflict") {
        throw new PipelineV2PublicReasonError("artifact_modified", "the stored profile epoch manifest does not carry its recorded content");
      }
      throw new PipelineV2PublicReasonError("internal_error", "the profile epoch manifest could not be read");
    }
    if (cause instanceof PipelineV2ProfileEpochError) {
      throw new PipelineV2PublicReasonError("artifact_modified", "the stored profile epoch manifest does not carry its recorded content");
    }
    throw cause;
  }
}
