/**
 * The single shared lexical contract for operator- and runtime-supplied
 * absolute paths (state roots, projections). A clean absolute path:
 *
 * - is a non-empty string;
 * - is an absolute POSIX path (starts with "/");
 * - contains no NUL character;
 * - has no leading or trailing whitespace;
 * - has no empty path segments (so no doubled or trailing slashes) except
 *   the leading "/" of the root;
 * - has no "." or ".." segments;
 * - has no trailing "/" except the root "/" itself.
 *
 * The value is never normalized or rewritten: a non-canonical lexical form
 * is rejected as-is. This helper is pure — it performs no filesystem I/O.
 * Diagnostics name the checked value ("what") and never repeat the value
 * itself.
 */

function cleanAbsolutePathFailure(value: unknown, what: string): string | null {
  if (typeof value !== "string" || value === "") {
    return `${what} must be a non-empty absolute path`;
  }
  if (value !== value.trim()) {
    return `${what} must not contain leading or trailing whitespace`;
  }
  if (!value.startsWith("/")) {
    return `${what} must be an absolute path`;
  }
  if (value.includes("\0")) {
    return `${what} must not contain a NUL character`;
  }
  if (value !== value.trim()) {
    return `${what} must not contain leading or trailing whitespace`;
  }
  if (value !== "/") {
    for (const segment of value.slice(1).split("/")) {
      if (segment === "" || segment === "." || segment === "..") {
        return `${what} must be a clean absolute path (no empty, "." or ".." path segments and no trailing slash except the root "/")`;
      }
    }
  }
  return null;
}

/** True when `value` satisfies the clean absolute path contract above. */
export function isCleanAbsolutePath(value: unknown): value is string {
  return cleanAbsolutePathFailure(value, "") === null;
}

/**
 * Returns `value` when it satisfies the clean absolute path contract above,
 * otherwise throws a content-free error naming `what` (never the value).
 */
export function assertCleanAbsolutePath(value: unknown, what: string): string {
  const failure = cleanAbsolutePathFailure(value, what);
  if (failure !== null) {
    throw new Error(failure);
  }
  return value as string;
}
