/**
 * Neutral scalar predicates shared by pipeline v2 validation layers.
 *
 * This module owns the single copy of the scalar contracts that both the
 * durable pipeline v2 run state (`pipeline_v2_state.ts`) and the wait
 * request/response manifests (`pipeline_v2_wait_manifest.ts`) must accept
 * identically:
 *
 * - safe id: `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, non-empty, no `..`;
 * - lowercase SHA-256: exactly 64 lowercase hex characters;
 * - positive / non-negative safe integers (`Number.isSafeInteger`).
 *
 * Only pure predicates live here. Exact-object validation, error classes,
 * parsing, normalization and diagnostic policies stay with the consumers;
 * nothing in this module mutates or freezes its input.
 */

const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

const SHA256_PATTERN = /^[0-9a-f]{64}$/;

/** The single safe-id contract of the pipeline v2 scalar layer. */
export function isPipelineV2SafeId(value: unknown): value is string {
  return typeof value === "string" && value !== "" && SAFE_ID_PATTERN.test(value) && !value.includes("..");
}

/** Lowercase hex SHA-256: exactly 64 lowercase hex characters. */
export function isLowercaseSha256(value: unknown): value is string {
  return typeof value === "string" && SHA256_PATTERN.test(value);
}

/** Positive safe integer (`Number.isSafeInteger` and `> 0`). */
export function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

/** Non-negative safe integer (`Number.isSafeInteger` and `>= 0`). */
export function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
