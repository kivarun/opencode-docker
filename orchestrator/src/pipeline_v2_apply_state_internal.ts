import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId, isLowercaseSha256, isNonNegativeSafeInteger, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";

/**
 * The durable preparation journal of one project apply operation: a
 * revisioned, crash-safe state document stored at
 * `<applyStoreRoot>/operations/<applyId>/state.json` through the shared
 * revisioned snapshot-store substrate. v1 statuses are exactly
 * `preparing`, `prepared` and `failed`; the schema leaves room for a
 * future writer to extend the vocabulary with its own version bump
 * without reinterpretation of these fields.
 *
 * The journal never stores: raw error messages, host paths, file
 * contents, payload bytes, timestamps, operation/session ids or
 * credentials. `prepared` is written only after the plan and every
 * payload were published, re-read and verified and the package
 * completeness was confirmed.
 */

export const PIPELINE_V2_APPLY_STATE_KIND = "pipeline_v2_project_apply_operation";
export const PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION = 1;

export const PIPELINE_V2_APPLY_OPERATION_STATUSES = [
  "preparing",
  "prepared",
  "failed",
] as const;
export type PipelineV2ApplyOperationStatus = (typeof PIPELINE_V2_APPLY_OPERATION_STATUSES)[number];

export class PipelineV2ApplyStateError extends Error {
  declare readonly reason: "invalid_state" | "invalid_json";

  constructor(reason: "invalid_state" | "invalid_json", message: string) {
    super(message);
    this.name = "PipelineV2ApplyStateError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

export interface PipelineV2ApplyOperationState {
  readonly schema_version: typeof PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION;
  readonly kind: typeof PIPELINE_V2_APPLY_STATE_KIND;
  readonly revision: number;
  readonly apply_id: string;
  readonly run_id: string;
  readonly project_id: string;
  readonly observation: string;
  readonly terminal_revision: number;
  /** The opaque root-binding digest (never a host path). */
  readonly project_root_binding: string;
  /** The plan digest once the plan was published and re-read (null before). */
  readonly plan_digest: string | null;
  readonly plan_step_count: number | null;
  readonly plan_payload_count: number | null;
  readonly plan_total_payload_byte_count: number | null;
  /** The number of payload artifacts durably sealed so far. */
  readonly sealed_payload_count: number;
  readonly status: PipelineV2ApplyOperationStatus;
  /** The closed, content-free failure reason; null unless status is failed. */
  readonly failure_reason: string | null;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectExactKeys(value: Record<string, unknown>, what: string, required: readonly string[]): void {
  const allowed = new Set(required);
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      throw new PipelineV2ApplyStateError("invalid_state", `${what} is missing the required field "${key}"`);
    }
  }
  for (const key of Object.getOwnPropertyNames(value)) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      continue;
    }
    if (!allowed.has(key)) {
      throw new PipelineV2ApplyStateError("invalid_state", `${what} carries an unknown field`);
    }
  }
}

function optionalDigest(value: unknown, what: string): string | null {
  if (value === null) {
    return null;
  }
  if (!isLowercaseSha256(value)) {
    throw new PipelineV2ApplyStateError("invalid_state", `${what} must be null or a lowercase 64-hex digest`);
  }
  return value;
}

function optionalCount(value: unknown, what: string): number | null {
  if (value === null) {
    return null;
  }
  if (!isNonNegativeSafeInteger(value)) {
    throw new PipelineV2ApplyStateError("invalid_state", `${what} must be null or a non-negative safe integer`);
  }
  return value;
}

export function validateApplyOperationState(raw: unknown): PipelineV2ApplyOperationState {
  if (!isPlainObject(raw)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state is not an object");
  }
  expectExactKeys(
    raw,
    "the operation state",
    [
      "schema_version",
      "kind",
      "revision",
      "apply_id",
      "run_id",
      "project_id",
      "observation",
      "terminal_revision",
      "project_root_binding",
      "plan_digest",
      "plan_step_count",
      "plan_payload_count",
      "plan_total_payload_byte_count",
      "sealed_payload_count",
      "status",
      "failure_reason",
    ],
  );
  if (raw["schema_version"] !== PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state schema version is not supported");
  }
  if (raw["kind"] !== PIPELINE_V2_APPLY_STATE_KIND) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state kind is not supported");
  }
  if (!isPositiveSafeInteger(raw["revision"])) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state revision must be a positive safe integer");
  }
  for (const field of ["apply_id", "run_id", "project_id"] as const) {
    if (!isPipelineV2SafeId(raw[field])) {
      throw new PipelineV2ApplyStateError("invalid_state", `the operation state ${field} is not a safe id`);
    }
  }
  if (!isLowercaseSha256(raw["observation"])) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state observation is not a lowercase 64-hex digest");
  }
  if (!isPositiveSafeInteger(raw["terminal_revision"])) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state terminal_revision must be a positive safe integer");
  }
  if (!isLowercaseSha256(raw["project_root_binding"])) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state project_root_binding is not a lowercase 64-hex digest");
  }
  const status = raw["status"];
  if (typeof status !== "string" || !(PIPELINE_V2_APPLY_OPERATION_STATUSES as readonly string[]).includes(status)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state status is not supported");
  }
  const failureReason = raw["failure_reason"];
  if (failureReason !== null && (typeof failureReason !== "string" || failureReason.length === 0 || failureReason.length > 64)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state failure_reason must be null or a short reason token");
  }
  const sealedPayloadCount = raw["sealed_payload_count"];
  if (!isNonNegativeSafeInteger(sealedPayloadCount)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state sealed_payload_count must be a non-negative safe integer");
  }
  if (status === "prepared" && raw["plan_digest"] === null) {
    throw new PipelineV2ApplyStateError("invalid_state", "a prepared operation state must carry its plan digest");
  }
  if (status === "failed" && typeof failureReason !== "string") {
    throw new PipelineV2ApplyStateError("invalid_state", "a failed operation state must carry its failure reason");
  }
  if (status !== "failed" && failureReason !== null) {
    throw new PipelineV2ApplyStateError("invalid_state", "only a failed operation state carries a failure reason");
  }
  const planStepCount = optionalCount(raw["plan_step_count"], "the operation state plan_step_count");
  const planPayloadCount = optionalCount(raw["plan_payload_count"], "the operation state plan_payload_count");
  if ((planStepCount === null) !== (raw["plan_digest"] === null) || (planPayloadCount === null) !== (raw["plan_digest"] === null)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the plan projection fields must be present exactly when the plan digest is");
  }
  const planTotalPayloadByteCount = raw["plan_total_payload_byte_count"];
  if ((planTotalPayloadByteCount === null) !== (raw["plan_digest"] === null)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the plan projection fields must be present exactly when the plan digest is");
  }
  if (planTotalPayloadByteCount !== null && !isNonNegativeSafeInteger(planTotalPayloadByteCount)) {
    throw new PipelineV2ApplyStateError("invalid_state", "the operation state plan_total_payload_byte_count must be a non-negative safe integer");
  }
  if (status === "prepared" && sealedPayloadCount !== planPayloadCount) {
    throw new PipelineV2ApplyStateError("invalid_state", "a prepared operation state must have every payload sealed");
  }
  return deepFreezeValue({
    schema_version: PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION,
    kind: PIPELINE_V2_APPLY_STATE_KIND,
    revision: raw["revision"],
    apply_id: raw["apply_id"] as string,
    run_id: raw["run_id"] as string,
    project_id: raw["project_id"] as string,
    observation: raw["observation"] as string,
    terminal_revision: raw["terminal_revision"] as number,
    project_root_binding: raw["project_root_binding"],
    plan_digest: optionalDigest(raw["plan_digest"], "the operation state plan_digest"),
    plan_step_count: planStepCount,
    plan_payload_count: planPayloadCount,
    plan_total_payload_byte_count: planTotalPayloadByteCount as number | null,
    sealed_payload_count: sealedPayloadCount,
    status: status as PipelineV2ApplyOperationStatus,
    failure_reason: failureReason as string | null,
  });
}

export function parseApplyOperationState(raw: string): PipelineV2ApplyOperationState {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2ApplyStateError("invalid_json", "the stored operation state is not valid JSON");
  }
  return validateApplyOperationState(parsed);
}
