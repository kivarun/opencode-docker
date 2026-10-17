/**
 * Canonical JSON serialization shared by the v1 and v2 pipeline digests.
 *
 * The serialization is the single canonical form both digests are built
 * on: object keys are sorted by code point so the key order of source
 * objects (e.g. a `JSON.parse`d result schema or a YAML mapping) never
 * changes the digest, while array order is preserved verbatim because
 * declaration order is execution-relevant. Built as a string, never as an
 * object, so hostile own keys such as "__proto__" cannot hit prototype
 * setters and are preserved verbatim.
 */

function canonicalJsonValue(value: unknown): string {
  if (value === null) {
    return "null";
  }
  const kind = typeof value;
  if (kind === "string" || kind === "boolean") {
    return JSON.stringify(value);
  }
  if (kind === "number") {
    if (!Number.isFinite(value as number)) {
      throw new Error("canonical JSON supports finite numbers only");
    }
    return JSON.stringify(value);
  }
  if (kind === "bigint" || kind === "function" || kind === "undefined" || kind === "symbol") {
    throw new Error(`canonical JSON does not support values of type ${kind}`);
  }
  if (kind !== "object") {
    throw new Error(`canonical JSON does not support values of type ${kind}`);
  }
  if (Array.isArray(value)) {
    return `[${(value as unknown[]).map((item) => canonicalJsonValue(item)).join(",")}]`;
  }
  // Own enumerable string keys, deterministically sorted.
  const keys = Object.keys(value as Record<string, unknown>).sort();
  const parts = keys.map((key) => `${JSON.stringify(key)}:${canonicalJsonValue((value as Record<string, unknown>)[key])}`);
  return `{${parts.join(",")}}`;
}

/**
 * Serializes any JSON-compatible value canonically: object keys are sorted
 * deterministically, array order is preserved, and the output is
 * byte-identical for structurally equal values. Non-JSON values (bigint,
 * functions, symbols, non-finite numbers) are rejected instead of being
 * silently coerced.
 */
export function canonicalJson(value: unknown): string {
  return canonicalJsonValue(value);
}
