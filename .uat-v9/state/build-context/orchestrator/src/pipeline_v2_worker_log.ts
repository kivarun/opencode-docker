/**
 * The one self-describing worker-log artifact: canonical JSON envelope
 * with a base64 payload.
 *
 * Every agent execution's bounded worker-log capture is stored as ONE
 * immutable artifact per execution index at the fixed layout
 * `<runRoot>/logs/<execution_index>.workerlog.json`. The envelope is
 * self-describing on purpose: the crash window "artifact published, state
 * binding not yet committed" (C5) must preserve not only the bytes but
 * also the capture status and both truncation flags, so the recovery can
 * rebuild the full durable binding from the artifact alone — a raw
 * `<index>.log` sidecar pair would create a second, two-file crash window
 * and is deliberately not used.
 *
 * Envelope (canonical JSON, keys sorted, no trailing newline):
 *
 *   {
 *     "schema_version": 1,
 *     "kind": "pipeline_v2_worker_log",
 *     "execution_index": <positive safe integer>,
 *     "status": "captured" | "truncated",
 *     "sha256": <lowercase hex digest of the raw payload>,
 *     "byte_count": <non-negative safe integer>,
 *     "truncated_daemon": <boolean>,
 *     "truncated_local": <boolean>,
 *     "payload_base64": <base64 of the raw captured payload>
 *   }
 *
 * The digest domain is `pipeline-v2-worker-log\0` over the raw captured
 * payload bytes (the bytes the daemon delivered and the local tail
 * retained — never a rendering, stderr text or a decoded UI string). The
 * payload is recovered byte-exactly from `payload_base64`; `byte_count`
 * and `sha256` are verified against the decoded bytes, so a damaged or
 * foreign envelope fails closed. `captured` never means upstream
 * completeness, and there is no `complete` status. `unavailable` captures
 * never produce an artifact.
 *
 * The artifact is a secret-bearing untrusted record: its payload is stored
 * verbatim with no redaction of any kind — task bodies, credentials,
 * control characters and ANSI escapes pass through byte-exactly. The
 * module performs no filesystem work of its own; publication belongs to
 * the worker-log store over the shared immutable-document substrate.
 */

import { canonicalJson } from "./canonical_json.ts";
import type { WorkerLogCapture } from "./worker_operation.ts";

export const WORKER_LOG_ARTIFACT_SCHEMA_VERSION = 1;
export const WORKER_LOG_ARTIFACT_KIND = "pipeline_v2_worker_log" as const;

/** The fixed canonical file extension of one worker-log artifact. */
export const WORKER_LOG_ARTIFACT_EXTENSION = "workerlog.json";

const WORKER_LOG_DIGEST_DOMAIN = "pipeline-v2-worker-log\0";

export class PipelineV2WorkerLogArtifactError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2WorkerLogArtifactError";
  }
}

export interface PreparedWorkerLogArtifact {
  readonly manifest: {
    readonly schema_version: 1;
    readonly kind: typeof WORKER_LOG_ARTIFACT_KIND;
    readonly execution_index: number;
    readonly status: "captured" | "truncated";
    readonly sha256: string;
    readonly byte_count: number;
    readonly truncated_daemon: boolean;
    readonly truncated_local: boolean;
    readonly payload_base64: string;
  };
  readonly canonical_json: string;
  /** The payload digest over the `pipeline-v2-worker-log\0` domain. */
  readonly sha256: string;
  readonly byte_count: number;
  readonly payload: Uint8Array;
}

export interface ParsedWorkerLogArtifact {
  readonly execution_index: number;
  readonly status: "captured" | "truncated";
  readonly sha256: string;
  readonly byte_count: number;
  readonly truncated_daemon: boolean;
  readonly truncated_local: boolean;
  readonly payload: Uint8Array;
}

/** The one digest builder over the raw captured payload. */
export function workerLogPayloadDigest(payload: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(Buffer.from(WORKER_LOG_DIGEST_DOMAIN, "utf8"));
  hasher.update(payload);
  return hasher.digest("hex");
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isLowercaseSha256(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

/**
 * Builds the canonical artifact from one finished capture. Returns `null`
 * for an `unavailable` capture or a zero-byte capture: no artifact exists
 * for those, the execution command records `{status: "unavailable"}`
 * instead.
 */
export function prepareWorkerLogArtifact(
  executionIndex: number,
  capture: WorkerLogCapture,
): PreparedWorkerLogArtifact | null {
  if (!isPositiveSafeInteger(executionIndex)) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact execution index must be a positive safe integer",
    );
  }
  if (capture === null || typeof capture !== "object") {
    throw new PipelineV2WorkerLogArtifactError("the worker log capture must be an object");
  }
  if (capture.status === "unavailable" || capture.byteCount === 0) {
    return null;
  }
  if (capture.status !== "captured" && capture.status !== "truncated") {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log capture status must be captured, truncated or unavailable",
    );
  }
  if (!(capture.tailBytes instanceof Uint8Array)) {
    throw new PipelineV2WorkerLogArtifactError("the worker log capture tail must be a byte view");
  }
  const payload = new Uint8Array(capture.tailBytes);
  const sha256 = workerLogPayloadDigest(payload);
  const manifest = {
    schema_version: WORKER_LOG_ARTIFACT_SCHEMA_VERSION as 1,
    kind: WORKER_LOG_ARTIFACT_KIND,
    execution_index: executionIndex,
    status: capture.status,
    sha256,
    byte_count: payload.byteLength,
    truncated_daemon: capture.truncatedDaemon === true,
    truncated_local: capture.truncatedLocal === true,
    payload_base64: Buffer.from(payload).toString("base64"),
  };
  return Object.freeze({
    manifest: Object.freeze(manifest),
    canonical_json: canonicalJson(manifest),
    sha256,
    byte_count: payload.byteLength,
    payload,
  });
}

function decodeBase64(value: unknown, what: string): Buffer {
  if (typeof value !== "string" || value === "") {
    throw new PipelineV2WorkerLogArtifactError(`${what} must be a non-empty base64 string`);
  }
  let decoded: Buffer;
  try {
    decoded = Buffer.from(value, "base64");
  } catch {
    throw new PipelineV2WorkerLogArtifactError(`${what} is not valid base64`);
  }
  // Node's base64 decoder is lenient: re-encode and compare so a
  // noncanonical encoding fails closed instead of silently decoding.
  if (decoded.toString("base64") !== value) {
    throw new PipelineV2WorkerLogArtifactError(`${what} is not canonical base64`);
  }
  return decoded;
}

/**
 * The one exact parser of a stored artifact. Validates the exact own-key
 * shape, the schema version and kind, the execution index binding, the
 * capture status, the digest and byte count against the decoded payload.
 * Malformed, foreign or damaged envelopes fail closed with content-free
 * diagnostics.
 */
export function parseWorkerLogArtifact(
  raw: string,
  expectedExecutionIndex: number,
): ParsedWorkerLogArtifact {
  if (!isPositiveSafeInteger(expectedExecutionIndex)) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact execution index must be a positive safe integer",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    throw new PipelineV2WorkerLogArtifactError("the worker log artifact is not valid JSON");
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new PipelineV2WorkerLogArtifactError("the worker log artifact is not a JSON object");
  }
  const obj = parsed as Record<string, unknown>;
  const expectedKeys = [
    "schema_version",
    "kind",
    "execution_index",
    "status",
    "sha256",
    "byte_count",
    "truncated_daemon",
    "truncated_local",
    "payload_base64",
  ];
  const seen = new Set(Object.keys(obj));
  for (const key of expectedKeys) {
    if (!seen.has(key)) {
      throw new PipelineV2WorkerLogArtifactError(
        `the worker log artifact is missing required field ${JSON.stringify(key)}`,
      );
    }
  }
  for (const key of seen) {
    if (!expectedKeys.includes(key)) {
      throw new PipelineV2WorkerLogArtifactError(
        `the worker log artifact has unknown field ${JSON.stringify(key)}`,
      );
    }
  }
  if (obj.schema_version !== WORKER_LOG_ARTIFACT_SCHEMA_VERSION) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact has an unsupported schema_version",
    );
  }
  if (obj.kind !== WORKER_LOG_ARTIFACT_KIND) {
    throw new PipelineV2WorkerLogArtifactError("the worker log artifact has an unexpected kind");
  }
  const executionIndex = obj.execution_index;
  if (!isPositiveSafeInteger(executionIndex)) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact execution index must be a positive safe integer",
    );
  }
  if (executionIndex !== expectedExecutionIndex) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact names another execution index",
    );
  }
  const status = obj.status;
  if (status !== "captured" && status !== "truncated") {
    throw new PipelineV2WorkerLogArtifactError(
      'the worker log artifact status must be "captured" or "truncated"',
    );
  }
  const sha256 = obj.sha256;
  if (!isLowercaseSha256(sha256)) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact sha256 must be a lowercase hex SHA-256 digest",
    );
  }
  const byteCount = obj.byte_count;
  if (!isNonNegativeSafeInteger(byteCount)) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact byte_count must be a non-negative safe integer",
    );
  }
  if (typeof obj.truncated_daemon !== "boolean" || typeof obj.truncated_local !== "boolean") {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact truncation flags must both be booleans",
    );
  }
  const payload = decodeBase64(obj.payload_base64, "the worker log artifact payload_base64");
  if (payload.byteLength !== byteCount) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact payload does not carry its declared byte count",
    );
  }
  const actualDigest = workerLogPayloadDigest(payload);
  if (actualDigest !== sha256) {
    throw new PipelineV2WorkerLogArtifactError(
      "the worker log artifact payload does not match its declared digest",
    );
  }
  return Object.freeze({
    execution_index: executionIndex,
    status,
    sha256,
    byte_count: byteCount,
    truncated_daemon: obj.truncated_daemon,
    truncated_local: obj.truncated_local,
    // The decoded bytes are a fresh per-parse copy; typed arrays cannot
    // be frozen (non-configurable indices), so the projection freezes
    // everything but the payload view.
    payload,
  });
}

/**
 * The durable state binding rebuilt from a verified artifact: exactly the
 * schema v9 `captured`/`truncated` record form, no path, no timestamps and
 * no content.
 */
export function workerLogBindingFromArtifact(
  artifact: ParsedWorkerLogArtifact,
): {
  status: "captured" | "truncated";
  sha256: string;
  byte_count: number;
  truncated_daemon: boolean;
  truncated_local: boolean;
} {
  return {
    status: artifact.status,
    sha256: artifact.sha256,
    byte_count: artifact.byte_count,
    truncated_daemon: artifact.truncated_daemon,
    truncated_local: artifact.truncated_local,
  };
}
