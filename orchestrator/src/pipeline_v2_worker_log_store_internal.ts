import { join } from "node:path";
import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  ensureImmutableDirectory,
  immutableDocumentConflict,
  immutableDocumentIoFailure,
  immutableDocumentInvalidLayout,
  ImmutableDocumentStoreError,
  isImmutablePublicationTempName,
  publishImmutableDocumentFile,
  realImmutableDocumentIo,
  readStoredImmutableDocument,
  requireImmutableDocumentRunRoot,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import {
  parseWorkerLogArtifact,
  prepareWorkerLogArtifact,
  workerLogBindingFromArtifact,
  WORKER_LOG_ARTIFACT_EXTENSION,
  type ParsedWorkerLogArtifact,
} from "./pipeline_v2_worker_log.ts";
import type { WorkerLogCapture } from "./worker_operation.ts";

/**
 * Internal per-call core of the worker-log artifact publication.
 *
 * This module is the WORKER-LOG ADAPTER of the immutable-document
 * substrate: the whole filesystem protocol — canonical run-root
 * verification, the exclusive 0700 directory creation with identity
 * fixation and chmod enforcement, the exclusive 0600 temp file, the full
 * write-all loop, the file fsync, the exclusive `link()` publication, the
 * ownership-checked temp removal, the parent-directory fsync, the
 * idempotent byte-exact adoption and the no-follow read path — lives
 * exactly once in
 * `pipeline_v2_immutable_document_store_internal.ts`. This adapter owns
 * only the worker-log layout, names and its public error class; there is
 * no second fsync/link/adoption implementation anywhere.
 *
 * Layout (fixed, flat):
 *
 *   <runRoot>/logs/<execution_index>.workerlog.json
 *
 * `logs/` is a 0700 real non-symlink directory inside the canonical run
 * root; artifact files are 0600 regular files whose content is exactly the
 * canonical JSON of the envelope — no trailing newline. There is exactly
 * one artifact per agent execution index; no second sidecar manifest
 * exists.
 *
 * Idempotent retry: a repeat with the same canonical bytes adopts the
 * existing file without touching its inode, mode, mtime or content and
 * re-fsyncs the logs directory, so an exact retry after a former
 * `durability_unknown` confirms the durability of the already visible
 * file. Different bytes, a wrong mode, or a non-file object (symlink,
 * directory, FIFO, socket) fail closed as a typed conflict: nothing is
 * overwritten, repaired or removed.
 *
 * Crash-window semantics (the publish-then-bind order):
 *
 *   publish/adopt artifact  →  dispatch agent_outputs_accepted /
 *                              agent_failed with the rebuilt binding
 *
 * An artifact published without its durable binding is an orphan the
 * recovery adopts exactly once; the orphan is never exposed as an
 * authoritative log before its binding exists. A `not_committed`
 * publication leaves no final artifact; a `durability_unknown` outcome
 * keeps the visible candidate and an exact retry confirms it.
 *
 * Diagnostics are content-free: only a controlled operation class and an
 * optional errno suffix — never a payload byte, a canary, a session or
 * operation id, or raw parser output.
 */

export type PipelineV2WorkerLogStoreFailureReason = "invalid_layout" | "conflict" | "io_failure";

export type PipelineV2WorkerLogStoreOutcome = "not_published" | "durability_unknown";

/**
 * The published candidate of a durability-unknown outcome: the final file
 * exists with exactly these canonical bytes and this payload digest, but
 * its crash survival is unconfirmed. An exact retry re-verifies and
 * re-fsyncs it.
 */
export interface PipelineV2WorkerLogStoreCandidate {
  readonly kind: "worker_log";
  readonly execution_index: number;
  readonly final_path: string;
  readonly canonical_json: string;
  readonly sha256: string;
}

export class PipelineV2WorkerLogStoreError extends Error {
  readonly outcome: PipelineV2WorkerLogStoreOutcome;
  readonly reason: PipelineV2WorkerLogStoreFailureReason;
  readonly candidate?: PipelineV2WorkerLogStoreCandidate;

  constructor(
    outcome: PipelineV2WorkerLogStoreOutcome,
    reason: PipelineV2WorkerLogStoreFailureReason,
    message: string,
    candidate?: PipelineV2WorkerLogStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2WorkerLogStoreError";
    this.outcome = outcome;
    this.reason = reason;
    if (candidate !== undefined) {
      this.candidate = deepFreezeValue(candidate);
    }
  }
}

export { realImmutableDocumentIo as realWorkerLogStoreIo };

export type WorkerLogStoreIo = ImmutableDocumentIo;

export interface PublishedPipelineV2WorkerLog {
  readonly kind: "worker_log";
  readonly execution_index: number;
  readonly artifact_path: string;
  readonly canonical_json: string;
  readonly sha256: string;
  readonly binding: {
    status: "captured" | "truncated";
    sha256: string;
    byte_count: number;
    truncated_daemon: boolean;
    truncated_local: boolean;
  };
}

const LOGS_DIR_NAME = "logs";
const TEMP_PREFIX = ".workerlog-publish-";

const WORKER_LOG_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "worker log artifact",
  publicationFailed: "the worker log artifact publication failed",
});

function invalidLayout(message: string): PipelineV2WorkerLogStoreError {
  return new PipelineV2WorkerLogStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2WorkerLogStoreError {
  return new PipelineV2WorkerLogStoreError("not_published", "conflict", message);
}

function artifactFileName(executionIndex: number): string {
  return `${executionIndex}.${WORKER_LOG_ARTIFACT_EXTENSION}`;
}

function workerLogErrorFromSubstrate(cause: ImmutableDocumentStoreError): PipelineV2WorkerLogStoreError {
  const substrateCandidate = cause.candidate;
  if (substrateCandidate === undefined) {
    return new PipelineV2WorkerLogStoreError(cause.outcome, cause.reason, cause.message);
  }
  const identity = substrateCandidate.identity as { kind?: unknown; execution_index?: unknown };
  const kind = identity.kind;
  const executionIndex = identity.execution_index;
  if (kind !== "worker_log" || typeof executionIndex !== "number") {
    return new PipelineV2WorkerLogStoreError(cause.outcome, cause.reason, cause.message);
  }
  return new PipelineV2WorkerLogStoreError(cause.outcome, cause.reason, cause.message, {
    kind: "worker_log",
    execution_index: executionIndex,
    final_path: substrateCandidate.final_path,
    canonical_json: substrateCandidate.canonical_json,
    sha256: substrateCandidate.sha256,
  });
}

/**
 * Final typed boundary: only `PipelineV2WorkerLogStoreError` and
 * `PipelineV2WorkerLogArtifactError` pass unchanged; neutral substrate
 * errors are re-tagged into the worker-log store class by typed fields —
 * never by message text; any other failure becomes a sanitized
 * not-published io failure with the fixed fallback message.
 */
async function withWorkerLogStoreGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2WorkerLogStoreError) {
      throw cause;
    }
    if (cause instanceof Error && cause.name === "PipelineV2WorkerLogArtifactError") {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw workerLogErrorFromSubstrate(cause);
    }
    throw workerLogErrorFromSubstrate(
      immutableDocumentIoFailure(WORKER_LOG_WORDING.publicationFailed),
    );
  }
}

/**
 * Publishes (or byte-exactly adopts) one execution's worker-log artifact
 * and returns the verified binding rebuilt from the prepared artifact.
 * Returns `null` without any filesystem effect for an `unavailable` or
 * zero-byte capture: no artifact exists for those.
 */
export async function publishWorkerLogWithIo(
  io: WorkerLogStoreIo,
  runRoot: string,
  executionIndex: number,
  capture: WorkerLogCapture,
): Promise<PublishedPipelineV2WorkerLog | null> {
  return await withWorkerLogStoreGuard(async () => {
    const prepared = prepareWorkerLogArtifact(executionIndex, capture);
    if (prepared === null) {
      return null;
    }
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const logsPath = await ensureImmutableDirectory(
      io,
      runRootCanonical,
      LOGS_DIR_NAME,
      "logs directory",
      "run root",
    );
    const artifactPath = join(logsPath, artifactFileName(executionIndex));
    await publishImmutableDocumentFile(
      io,
      logsPath,
      {
        fileName: artifactFileName(executionIndex),
        tempPrefix: TEMP_PREFIX,
        tempStem: `worker-log-${executionIndex}`,
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: deepFreezeValue({ kind: "worker_log", execution_index: executionIndex }),
      },
      WORKER_LOG_WORDING,
      "logs directory",
    );
    return deepFreezeValue({
      kind: "worker_log" as const,
      execution_index: executionIndex,
      artifact_path: artifactPath,
      canonical_json: prepared.canonical_json,
      sha256: prepared.sha256,
      binding: {
        status: prepared.manifest.status,
        sha256: prepared.sha256,
        byte_count: prepared.byte_count,
        truncated_daemon: prepared.manifest.truncated_daemon,
        truncated_local: prepared.manifest.truncated_local,
      },
    });
  });
}

/**
 * The read-only load of one stored artifact. Returns `null` when the
 * artifact does not exist (absent logs directory or absent file); any
 * present object must be a real non-symlink regular file with mode 0600
 * whose bytes are exactly the canonical JSON of a valid envelope bound to
 * the expected execution index — anything else fails closed.
 */
export async function loadWorkerLogWithIo(
  io: WorkerLogStoreIo,
  runRoot: string,
  executionIndex: number,
): Promise<ParsedWorkerLogArtifact | null> {
  return await withWorkerLogStoreGuard(async () => {
    if (!Number.isSafeInteger(executionIndex) || executionIndex <= 0) {
      throw invalidLayout("the worker log execution index must be a positive safe integer");
    }
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const logsPath = join(runRootCanonical, LOGS_DIR_NAME);
    const finalPath = join(logsPath, artifactFileName(executionIndex));
    const info = await io.lstatOrNull(finalPath);
    if (info === null) {
      return null;
    }
    const stored = await readStoredImmutableDocument(
      io,
      finalPath,
      "stored worker log artifact",
      WORKER_LOG_WORDING,
    );
    const raw = stored.toString("utf8");
    const parsed = parseWorkerLogArtifact(raw, executionIndex);
    if (!stored.equals(Buffer.from(canonicalJsonOfParsed(parsed), "utf8"))) {
      throw conflict("the stored worker log artifact does not carry its own canonical JSON");
    }
    return parsed;
  });
}

/**
 * Rebuilds the canonical JSON of a parsed artifact. The parsed artifact is
 * a structural projection of the envelope; re-serializing it through the
 * same canonical serializer is the byte-exact check against the stored
 * bytes.
 */
function canonicalJsonOfParsed(parsed: ParsedWorkerLogArtifact): string {
  const manifest = {
    schema_version: 1,
    kind: "pipeline_v2_worker_log",
    execution_index: parsed.execution_index,
    status: parsed.status,
    sha256: parsed.sha256,
    byte_count: parsed.byte_count,
    truncated_daemon: parsed.truncated_daemon,
    truncated_local: parsed.truncated_local,
    payload_base64: Buffer.from(parsed.payload).toString("base64"),
  };
  return canonicalJson(manifest);
}

/**
 * The read-only verification of the whole logs namespace: every entry must
 * be either the final artifact of one required execution index, the
 * tolerated publish-then-bind orphan of a null-binding agent execution, or
 * an exact publication-temp orphan of the substrate's own grammar
 * (tolerated, never repaired). A foreign file name, a directory, a symlink
 * or an unexpected extra final artifact fails closed.
 */
export async function verifyWorkerLogNamespaceWithIo(
  io: WorkerLogStoreIo,
  runRoot: string,
  requiredIndexes: readonly number[],
  toleratedIndexes: readonly number[] = [],
): Promise<void> {
  await withWorkerLogStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const logsPath = join(runRootCanonical, LOGS_DIR_NAME);
    const info = await io.lstatOrNull(logsPath);
    if (info === null) {
      if (requiredIndexes.length > 0) {
        throw immutableDocumentInvalidLayout("the logs directory does not exist");
      }
      return;
    }
    if (info.isSymbolicLink() || !info.isDirectory()) {
      throw immutableDocumentConflict(
        `the logs directory exists but is not a real directory`,
      );
    }
    const required = new Set(requiredIndexes);
    const tolerated = new Set(toleratedIndexes);
    const names = await io.readdirNames(logsPath);
    for (const name of names) {
      if (isImmutablePublicationTempName(name, TEMP_PREFIX)) {
        continue;
      }
      const suffix = `.${WORKER_LOG_ARTIFACT_EXTENSION}`;
      if (!name.endsWith(suffix)) {
        throw conflict(`the logs directory carries an unexpected entry name shape`);
      }
      const stem = name.slice(0, name.length - suffix.length);
      if (!/^[1-9][0-9]*$/.test(stem)) {
        throw conflict(`the logs directory carries an unexpected entry name shape`);
      }
      const index = Number(stem);
      if (!Number.isSafeInteger(index) || (!required.has(index) && !tolerated.has(index))) {
        throw conflict(`the logs directory carries an unexpected worker log artifact`);
      }
    }
    for (const index of requiredIndexes) {
      const finalInfo = await io.lstatOrNull(join(logsPath, artifactFileName(index)));
      if (finalInfo === null) {
        throw immutableDocumentInvalidLayout(
          "the logs directory is missing a recorded worker log artifact",
        );
      }
      if (finalInfo.isSymbolicLink() || !finalInfo.isFile()) {
        throw conflict(`a recorded worker log artifact is not a regular file`);
      }
      if ((finalInfo.mode & 0o7777) !== 0o600) {
        throw conflict(`a recorded worker log artifact does not have the required mode 0600`);
      }
    }
  });
}

/**
 * Convenience passthrough for the durable binding of a parsed artifact.
 */
export function bindingOfParsedWorkerLog(parsed: ParsedWorkerLogArtifact) {
  return workerLogBindingFromArtifact(parsed);
}
