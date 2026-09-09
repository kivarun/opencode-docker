import { isAbsolute } from "node:path";
import { stat, realpath } from "node:fs/promises";
import { describeError } from "./docker_helper.ts";

/**
 * Error class used by the shared bundle-file helpers. Each owning module
 * passes its own error type (PipelineError, DecisionModelError, ...) so the
 * helpers stay free of module-specific failure types.
 */
export type BundleFileErrorClass = new (message: string) => Error;

/**
 * Canonicalize an absolute bundle root that must be a real directory.
 * Symlinked roots are allowed (they resolve through realpath); missing
 * roots, files, and inaccessible roots are rejected fail-closed. The
 * returned path is the verified canonical directory.
 */
export async function requireCanonicalDirectoryRoot(
  bundleRoot: string,
  what: string,
  errorClass: BundleFileErrorClass,
): Promise<string> {
  if (!isAbsolute(bundleRoot)) {
    throw new errorClass(
      `${what} must be an absolute path, got ${JSON.stringify(bundleRoot)}`,
    );
  }
  let rootCanonical: string;
  try {
    rootCanonical = await realpath(bundleRoot);
  } catch (cause) {
    throw new errorClass(
      `${what} ${bundleRoot} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  let rootInfo;
  try {
    rootInfo = await stat(rootCanonical);
  } catch (cause) {
    throw new errorClass(
      `${what} ${rootCanonical} is not accessible: ${describeError(cause)}`,
    );
  }
  if (!rootInfo.isDirectory()) {
    throw new errorClass(`${what} ${rootCanonical} is not a directory`);
  }
  return rootCanonical;
}

/**
 * Bundle-relative references must be clean: no absolute paths, no home
 * expansion, no empty segments, no `.` or `..` traversal segments; realpath
 * containment inside the bundle is verified separately at load time.
 */
export function validateBundleRelativePath(
  value: string,
  what: string,
  errorClass: BundleFileErrorClass,
): string {
  if (isAbsolute(value) || value.startsWith("~")) {
    throw new errorClass(`${what} must be a bundle-relative path, got ${JSON.stringify(value)}`);
  }
  for (const segment of value.split("/")) {
    if (segment === "" || segment === "." || segment === ".." || segment === "~") {
      throw new errorClass(
        `${what} must be a clean bundle-relative path without empty, ".", ".." or "~" segments, got ${JSON.stringify(value)}`,
      );
    }
  }
  return value;
}

/**
 * A bundle file reference must be a regular file that resolves inside the
 * canonical bundle root: a symlink to a file inside the bundle is allowed, a
 * symlink escape outside the bundle is rejected. The verified canonical path
 * is returned; callers read the file from that path only. `scopeName` names
 * the owning root in error messages (e.g. "pipeline bundle").
 */
export async function requireBundleFileInsideRoot(
  path: string,
  rootCanonical: string,
  what: string,
  errorClass: BundleFileErrorClass,
  scopeName: string = "bundle root",
): Promise<string> {
  let info;
  try {
    info = await stat(path);
  } catch (cause) {
    throw new errorClass(`${what} ${path} is not accessible: ${describeError(cause)}`);
  }
  if (!info.isFile()) {
    throw new errorClass(`${what} ${path} is not a regular file`);
  }
  let canonical: string;
  try {
    canonical = await realpath(path);
  } catch (cause) {
    throw new errorClass(`${what} ${path} cannot be canonicalized: ${describeError(cause)}`);
  }
  if (canonical !== rootCanonical && !canonical.startsWith(`${rootCanonical}/`)) {
    throw new errorClass(`${what} ${path} resolves outside the ${scopeName}`);
  }
  return canonical;
}

export async function readBundleFile(
  path: string,
  what: string,
  errorClass: BundleFileErrorClass,
): Promise<string> {
  try {
    return await Bun.file(path).text();
  } catch (cause) {
    throw new errorClass(`${what} ${path} is not readable: ${describeError(cause)}`);
  }
}
