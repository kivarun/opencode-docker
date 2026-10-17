import { basename, join } from "node:path";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import {
  immutableDocumentIoFailure,
  immutableDocumentInvalidLayout,
  immutableDocumentConflict,
  ImmutableDocumentStoreError,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  realImmutableDocumentIo,
  requireImmutableDocumentRunRoot,
  ensureImmutableDirectory,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import {
  PipelineV2WaitManifestError,
  acceptPipelineV2WaitResponse,
  parsePipelineV2WaitRequest,
  preparePipelineV2WaitRequest,
  type AcceptedPipelineV2WaitResponse,
  type PreparedPipelineV2WaitRequest,
} from "./pipeline_v2_wait_manifest.ts";

/**
 * Internal per-call core of the wait manifest filesystem publication.
 *
 * This module is explicitly internal: the public wrapper
 * `pipeline_v2_wait_store.ts` always calls the core functions with the
 * single fixed, immutable production IO re-exported from the neutral
 * immutable-document substrate; tests call the same core with their own
 * per-call IO object. The IO is a per-call capability passed through the
 * whole call chain — there is no mutable module-global IO and no
 * installer, so a fault-injected test call can never change the behavior
 * of any parallel production call.
 *
 * Layout (fixed, flat):
 *
 *   <runRoot>/waits/<waitIndex>.request.json
 *   <runRoot>/waits/<waitIndex>.response.json
 *
 * `waits/` is a 0700 real non-symlink directory inside the canonical run
 * root; manifest files are 0600 regular files whose content is exactly the
 * canonical JSON of the manifest — no trailing newline. The paths never
 * enter the manifests or the durable state, and the manifests carry no
 * filesystem provenance.
 *
 * This module is the WAIT-SPECIFIC ADAPTER of the immutable publication
 * substrate: the whole filesystem protocol — canonical run-root
 * verification, exclusive directory creation with identity fixation and
 * chmod enforcement, the exclusive temp file, the full write-all loop,
 * file fsync, close, the exclusive publication, the ownership-checked temp
 * removal, the parent-directory fsync, the idempotent adoption and the
 * no-follow read path — lives
 * exactly once in
 * `pipeline_v2_immutable_document_store_internal.ts`; this adapter owns
 * only the wait-specific layout, names, binding checks and public error
 * class. Every diagnostic keeps its exact wait-store string via the
 * adapter wording below.
 *
 * Idempotent retry: a repeat with the same canonical bytes adopts the
 * existing file without touching its inode, mode, mtime or content and
 * re-fsyncs the waits directory, so an exact retry after a former
 * `durability_unknown` confirms the durability of the already visible
 * file. Different bytes, a wrong mode, or a non-file object (symlink,
 * directory, FIFO, socket) fail closed as a typed conflict: nothing is
 * overwritten or repaired, external sentinels are untouched, and a target
 * is never replaced.
 *
 * Concurrency: two publishers of the same manifest may both succeed (the
 * loser adopts the winner's identical bytes); publishers of different
 * manifests race for one path — exactly one wins, the loser gets a typed
 * conflict, and the target always keeps the winner's bytes.
 *
 * Error contract (closed, typed — no message parsing downstream):
 *
 *   - pre-publication failures throw `PipelineV2WaitStoreError` with
 *     `outcome: "not_published"` and a stable `reason`
 *     (`invalid_layout`, `conflict`, `io_failure`);
 *   - after a successful `link()`, if the unlink/fsync/close durability
 *     phase cannot be confirmed, the final file is never rolled back: the
 *     error carries `outcome: "durability_unknown"` with reason
 *     `io_failure` and an immutable deep-frozen `candidate` (kind, wait
 *     index, final path, canonical JSON, digest) — the final file exists
 *     with its full canonical bytes and an exact retry confirms it;
 *   - manifest validation failures keep their `PipelineV2WaitManifestError`
 *     class and are never retagged by message text.
 *
 * Diagnostics are content-free: only a controlled operation class and an
 * optional errno suffix (limited `[A-Z][A-Z0-9_]{0,31}` code form, read
 * getter-safe) — never a manifest body, an action value, a canary or raw
 * parser output.
 *
 * Semantic boundary with the durable state (future, not wired here):
 *
 *   publish request  → dispatch run_waiting(request_sha256)
 *   publish response → dispatch wait_response_recorded(response_sha256)
 *
 * A manifest file published without the corresponding durable commit is an
 * orphan, not part of the history; a retry with the same canonical bytes
 * safely reuses it (idempotent adoption), and a different manifest on a
 * busy path is a conflict. This module never dispatches reducer commands
 * and never mutates the durable state, the project copy, inputs or
 * outputs — it only creates and maintains `<runRoot>/waits/`.
 *
 * Honest boundaries: the run root is never created or removed here; there
 * is no protection against a trusted host process racing the
 * verify-then-use steps, and there is no crash recovery for a leftover
 * temp file (the next publication uses a fresh exclusive temp name).
 */

export type PipelineV2WaitStoreFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type PipelineV2WaitStoreOutcome = "not_published" | "durability_unknown";

/**
 * The published candidate of a durability-unknown outcome: the final file
 * exists with exactly these canonical bytes and this digest, but its crash
 * survival is unconfirmed. An exact retry re-verifies and re-fsyncs it.
 */
export interface PipelineV2WaitStoreCandidate {
  readonly kind: "request" | "response";
  readonly wait_index: number;
  readonly final_path: string;
  readonly canonical_json: string;
  readonly sha256: string;
}

export class PipelineV2WaitStoreError extends Error {
  readonly outcome: PipelineV2WaitStoreOutcome;
  readonly reason: PipelineV2WaitStoreFailureReason;
  readonly candidate?: PipelineV2WaitStoreCandidate;

  constructor(
    outcome: PipelineV2WaitStoreOutcome,
    reason: PipelineV2WaitStoreFailureReason,
    message: string,
    candidate?: PipelineV2WaitStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2WaitStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreezeValue(candidate);
    }
  }
}

export interface PublishedPipelineV2WaitRequest {
  readonly request: PreparedPipelineV2WaitRequest;
  readonly request_path: string;
}

export interface PublishedPipelineV2WaitResponse {
  readonly request: PreparedPipelineV2WaitRequest;
  readonly response: AcceptedPipelineV2WaitResponse;
  readonly request_path: string;
  readonly response_path: string;
}

export { realImmutableDocumentIo as realWaitStoreIo };

export type WaitStoreIo = ImmutableDocumentIo;

const WAITS_DIR_NAME = "waits";
const REQUEST_SUFFIX = "request.json";
const RESPONSE_SUFFIX = "response.json";
const TEMP_PREFIX = ".wait-publish-";

const WAIT_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "wait manifest",
  publicationFailed: "the wait manifest publication failed",
});

/**
 * Final typed boundary: only `PipelineV2WaitStoreError` and
 * `PipelineV2WaitManifestError` pass unchanged; neutral substrate errors
 * are re-tagged into the wait store class by typed fields (outcome,
 * reason, candidate identity) — never by message text; any other failure
 * becomes a sanitized not-published io failure with the exact wait-store
 * fallback message.
 */
async function withWaitStoreGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2WaitStoreError) {
      throw cause;
    }
    if (cause instanceof PipelineV2WaitManifestError) {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw waitStoreErrorFromSubstrate(cause);
    }
    throw waitStoreErrorFromSubstrate(
      immutableDocumentIoFailure(WAIT_WORDING.publicationFailed),
    );
  }
}

interface WaitCandidateIdentity {
  readonly kind?: unknown;
  readonly wait_index?: unknown;
}

/**
 * Map a neutral substrate error into the wait store class by typed fields
 * only: outcome and reason pass through, and the substrate candidate's
 * frozen identity descriptor ({kind, wait_index}) becomes the wait
 * candidate's own fields. Identity is always the descriptor this adapter
 * bound at publication time.
 */
function waitStoreErrorFromSubstrate(cause: ImmutableDocumentStoreError): PipelineV2WaitStoreError {
  const substrateCandidate = cause.candidate;
  if (substrateCandidate === undefined) {
    return new PipelineV2WaitStoreError(cause.outcome, cause.reason, cause.message);
  }
  const identity = substrateCandidate.identity as WaitCandidateIdentity;
  const kind = identity.kind;
  const waitIndex = identity.wait_index;
  if ((kind !== "request" && kind !== "response") || typeof waitIndex !== "number") {
    return new PipelineV2WaitStoreError(cause.outcome, cause.reason, cause.message);
  }
  return new PipelineV2WaitStoreError(cause.outcome, cause.reason, cause.message, {
    kind,
    wait_index: waitIndex,
    final_path: substrateCandidate.final_path,
    canonical_json: substrateCandidate.canonical_json,
    sha256: substrateCandidate.sha256,
  });
}

function invalidLayout(message: string): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2WaitStoreError {
  return new PipelineV2WaitStoreError("not_published", "conflict", message);
}

/**
 * Publish one wait request manifest: prepare (the manifest validator is
 * the single authority — its failures propagate unchanged), bind the
 * canonical run root (its basename must be the manifest run id), ensure
 * the waits directory, then publish `<waitIndex>.request.json` through
 * the neutral immutable publication protocol.
 */
export async function publishWaitRequestWithIo(
  io: WaitStoreIo,
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2WaitRequest> {
  return await withWaitStoreGuard(async () => {
    const prepared = preparePipelineV2WaitRequest(value);
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    if (basename(runRootCanonical) !== prepared.manifest.run_id) {
      throw invalidLayout("the run root does not match the wait manifest run identifier");
    }
    const waitsPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      WAITS_DIR_NAME,
      "waits directory",
      "run root",
    );
    const requestPath = join(waitsPath, `${prepared.manifest.wait_index}.${REQUEST_SUFFIX}`);
    await publishImmutableDocumentFile(
      io,
      waitsPath,
      {
        fileName: `${prepared.manifest.wait_index}.${REQUEST_SUFFIX}`,
        tempPrefix: TEMP_PREFIX,
        tempStem: `request-${prepared.manifest.wait_index}`,
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: deepFreezeValue({ kind: "request", wait_index: prepared.manifest.wait_index }),
      },
      WAIT_WORDING,
      "waits directory",
    );
    return deepFreezeValue({ request: prepared, request_path: requestPath });
  });
}

/**
 * Load the stored request file for a response publication: the object must
 * be a real non-symlink regular file with mode 0600; its bytes are read
 * through a no-follow read, parsed by the manifest module (its failures
 * propagate unchanged) and must carry exactly the canonical JSON of their
 * own manifest, the requested wait index and the run root's run id. A
 * damaged, foreign or noncanonical file fails closed before anything is
 * written.
 */
async function loadStoredWaitRequest(
  io: WaitStoreIo,
  requestPath: string,
  waitsPath: string,
  waitIndex: number,
  runId: string,
): Promise<PreparedPipelineV2WaitRequest> {
  void waitsPath;
  const stored = await readStoredImmutableDocument(
    io,
    requestPath,
    "published wait request file",
    WAIT_WORDING,
  );
  const raw = stored.toString("utf8");
  const prepared = parsePipelineV2WaitRequest(raw);
  if (prepared.manifest.wait_index !== waitIndex) {
    throw conflict("the stored wait request manifest names another wait index");
  }
  if (prepared.manifest.run_id !== runId) {
    throw conflict("the stored wait request manifest does not belong to this run root");
  }
  if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
    throw conflict("the stored wait request file does not carry its own canonical JSON");
  }
  return prepared;
}

/**
 * Publish one wait response: validate the wait index, bind the canonical
 * run root, load and verify the stored request file, accept the user's
 * response through the manifest module (its failures propagate
 * unchanged), then publish `<waitIndex>.response.json` through the
 * neutral immutable publication protocol.
 */
export async function publishWaitResponseWithIo(
  io: WaitStoreIo,
  runRoot: string,
  waitIndex: number,
  raw: string,
): Promise<PublishedPipelineV2WaitResponse> {
  return await withWaitStoreGuard(async () => {
    if (!isPositiveSafeInteger(waitIndex)) {
      throw invalidLayout("the wait index must be a positive safe integer");
    }
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const runId = basename(runRootCanonical);
    const waitsPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      WAITS_DIR_NAME,
      "waits directory",
      "run root",
    );
    const requestPath = join(waitsPath, `${waitIndex}.${REQUEST_SUFFIX}`);
    const prepared = await loadStoredWaitRequest(io, requestPath, waitsPath, waitIndex, runId);
    const accepted = acceptPipelineV2WaitResponse(prepared, raw);
    const responsePath = join(waitsPath, `${waitIndex}.${RESPONSE_SUFFIX}`);
    await publishImmutableDocumentFile(
      io,
      waitsPath,
      {
        fileName: `${waitIndex}.${RESPONSE_SUFFIX}`,
        tempPrefix: TEMP_PREFIX,
        tempStem: `response-${waitIndex}`,
        canonicalJson: accepted.canonical_json,
        sha256: accepted.sha256,
        identity: deepFreezeValue({ kind: "response", wait_index: waitIndex }),
      },
      WAIT_WORDING,
      "waits directory",
    );
    return deepFreezeValue({
      request: prepared,
      response: accepted,
      request_path: requestPath,
      response_path: responsePath,
    });
  });
}
