/**
 * Versioned server-side project registry for the control-plane.
 *
 * This module is the single authority-mapping owner between the
 * browser-facing world and the host: a public project id plus
 * server-owned input references resolve into the canonical project,
 * config, pipeline and input paths of one typed `OrchestratorCommand`
 * (kind `run`). A browser or API caller never supplies a host path, a
 * config root, a pipeline root, a launcher id, an executable, an
 * environment or raw argv — those live exclusively in the
 * operator-owned registry file and reach the command only through this
 * composition boundary.
 *
 * The registry document is a versioned JSON file (schema version 1)
 * loaded once per service lifetime: the loader validates the exact
 * schema, canonicalizes every operator-configured root through the real
 * filesystem and returns a recursively frozen registry. It never
 * creates, repairs or rewrites anything; a later change to or deletion
 * of the file cannot alter an already-loaded registry, and there is no
 * hot reload, no background observation and no discovery.
 *
 * Input resolution happens per request, immediately before the command
 * is built: a reference is one immediate child of one declared input
 * root, must be a real non-symlink regular file, and its canonical path
 * must remain an immediate child of the canonical root. The module never
 * reads input contents — the existing runner remains the owner of the
 * final input snapshot and modified detection; this layer only decides
 * which paths the caller is allowed to name.
 *
 * Diagnostics are content-free: they never contain the registry body,
 * unknown field names, input or task contents, host paths or raw system
 * error text — only already-validated safe ids and, where the style
 * allows, a bare errno code.
 */
import { constants as fsConstants, fstatSync, lstatSync, openSync, readSync, closeSync } from "node:fs";
import { isAbsolute, join, dirname } from "node:path";
import { realpath } from "node:fs/promises";
import { isErrnoException } from "../fs_checks.ts";
import { isCleanAbsolutePath } from "../clean_path.ts";
import { isPipelineV2SafeId } from "../pipeline_v2_scalar.ts";
import { deepFreezeValue } from "../pipeline_v2_freeze_internal.ts";
import {
  routeOrchestratorCommand,
  type OrchestratorCommand,
  type OrchestratorRoutingConfig,
} from "./routing.ts";

/** The fixed size limit of one registry document. */
const REGISTRY_SIZE_LIMIT_BYTES = 1_048_576;

/**
 * The closed failure vocabulary of the project registry. These reasons
 * are disjoint from `PipelineV2PublicReason` and from the subprocess
 * client's transport reasons: a registry failure happens before any
 * subprocess spawn, is not a Pipeline V2 outcome, creates no run root
 * and never calls inspect.
 */
export type ControlPlaneRegistryFailureReason =
  | "invalid_registry_path"
  | "registry_missing"
  | "registry_unreadable"
  | "registry_too_large"
  | "invalid_registry"
  | "unsupported_registry_version"
  | "project_not_found"
  | "input_root_not_found"
  | "input_not_found"
  | "input_invalid"
  | "invalid_request";

/** The single typed registry error class with an immutable content-free reason. */
export class ControlPlaneProjectRegistryError extends Error {
  readonly reason!: ControlPlaneRegistryFailureReason;

  constructor(reason: ControlPlaneRegistryFailureReason, message: string) {
    super(message);
    this.name = "ControlPlaneProjectRegistryError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** One resolved input root: the declared id and its canonical directory. */
export interface ResolvedControlPlaneInputRoot {
  readonly id: string;
  readonly root: string;
}

/** One resolved project: operator identity plus canonical host paths. */
export interface ResolvedControlPlaneProject {
  readonly id: string;
  readonly projectRoot: string;
  readonly configRoot: string;
  readonly pipelineRoot: string | null;
  readonly launcherId: string | null;
  readonly inputRoots: readonly ResolvedControlPlaneInputRoot[];
}

/** The loaded registry: schema version 1 with projects in declaration order. */
export interface ControlPlaneProjectRegistry {
  readonly schemaVersion: 1;
  readonly projects: readonly ResolvedControlPlaneProject[];
}

/** One public input binding: a pipeline input id inside a declared root, named by one child reference. */
export interface ControlPlaneRunInput {
  readonly id: string;
  readonly rootId: string;
  readonly ref: string;
}

/**
 * The transport-neutral public run request. The browser-facing caller
 * supplies only identifiers: the run id, the project id and, per input,
 * the pipeline input id, the declared input root id and one child
 * reference. No host path is expressible.
 */
export interface ControlPlaneRunRequest {
  readonly runId: string;
  readonly projectId: string;
  readonly inputs: readonly ControlPlaneRunInput[];
}

/** The composition result: the existing typed run command plus its routing configuration. */
export interface ControlPlaneResolvedRun {
  readonly command: OrchestratorCommand;
  readonly routingConfig: OrchestratorRoutingConfig;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectSafeId(record: Record<string, unknown>, key: string, what: string): string {
  const value = record[key];
  if (!isPipelineV2SafeId(value)) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} must be a safe identifier`);
  }
  return value as string;
}

function expectCleanPathOrNull(
  record: Record<string, unknown>,
  key: string,
  what: string,
): string | null {
  const value = record[key];
  if (value === null) return null;
  if (!isCleanAbsolutePath(value)) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} must be null or a clean absolute path`);
  }
  return value as string;
}

/**
 * The single JSON.parse owner of the registry module: the raw registry
 * file is read once and parsed once; a malformed document is a typed
 * `invalid_registry` failure with no parser fragments in the message.
 */
function parseRegistryDocument(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry document is not valid JSON");
  }
}

interface CapturedRegistryProject {
  readonly id: string;
  readonly projectRoot: string;
  readonly configRoot: string;
  readonly pipelineRoot: string | null;
  readonly launcherId: string | null;
  readonly inputRoots: readonly { id: string; root: string }[];
}

/**
 * Validates one registry document against the exact schema version 1:
 * exact own keys at every level (prototype-chain fields never satisfy the
 * shape, unknown and `__proto__`-like own extras are rejected), strict
 * scalar types, unique project and input-root ids, non-empty projects,
 * and declaration order preserved verbatim.
 */
function validateRegistryDocument(document: unknown): readonly CapturedRegistryProject[] {
  if (!isRecord(document)) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry document must be an object");
  }
  const rootKeys = Object.keys(document);
  if (rootKeys.length !== 2 || !Object.hasOwn(document, "schema_version") || !Object.hasOwn(document, "projects")) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry document must have exactly schema_version and projects");
  }
  if (document["schema_version"] !== 1) {
    throw new ControlPlaneProjectRegistryError("unsupported_registry_version", "the registry document schema version must be 1");
  }
  const projectsValue = document["projects"];
  if (!Array.isArray(projectsValue) || projectsValue.length === 0) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry projects must be a non-empty list");
  }
  const seenProjectIds = new Set<string>();
  const captured: CapturedRegistryProject[] = [];
  for (let index = 0; index < projectsValue.length; index += 1) {
    const entry = projectsValue[index];
    if (!isRecord(entry)) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "each registry project must be an object");
    }
    const projectKeys = Object.keys(entry);
    if (
      projectKeys.length !== 6 ||
      !Object.hasOwn(entry, "id") ||
      !Object.hasOwn(entry, "project_root") ||
      !Object.hasOwn(entry, "config_root") ||
      !Object.hasOwn(entry, "pipeline_root") ||
      !Object.hasOwn(entry, "launcher_id") ||
      !Object.hasOwn(entry, "input_roots")
    ) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "each registry project must have exactly id, project_root, config_root, pipeline_root, launcher_id and input_roots");
    }
    const id = expectSafeId(entry, "id", "the registry project id");
    if (seenProjectIds.has(id)) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "registry project ids must be unique");
    }
    seenProjectIds.add(id);
    const projectRoot = entry["project_root"];
    if (!isCleanAbsolutePath(projectRoot)) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry project_root must be a clean absolute path");
    }
    const configRoot = entry["config_root"];
    if (!isCleanAbsolutePath(configRoot)) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry config_root must be a clean absolute path");
    }
    const pipelineRoot = expectCleanPathOrNull(entry, "pipeline_root", "the registry pipeline_root");
    const launcherIdValue = entry["launcher_id"];
    let launcherId: string | null = null;
    if (launcherIdValue !== null) {
      if (typeof launcherIdValue !== "string" || !launcherIdValue.startsWith("dhl_") || launcherIdValue.length === 4) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry launcher_id must be null or a launcher ID (dhl_...)");
      }
      launcherId = launcherIdValue;
    }
    const inputRootsValue = entry["input_roots"];
    if (!Array.isArray(inputRootsValue)) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry input_roots must be a list");
    }
    const seenRootIds = new Set<string>();
    const inputRoots: { id: string; root: string }[] = [];
    for (let rootIndex = 0; rootIndex < inputRootsValue.length; rootIndex += 1) {
      const rootEntry = inputRootsValue[rootIndex];
      if (!isRecord(rootEntry)) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "each input root must be an object");
      }
      const rootKeys2 = Object.keys(rootEntry);
      if (rootKeys2.length !== 2 || !Object.hasOwn(rootEntry, "id") || !Object.hasOwn(rootEntry, "root")) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "each input root must have exactly id and root");
      }
      const rootId = expectSafeId(rootEntry, "id", "the input root id");
      if (seenRootIds.has(rootId)) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "input root ids must be unique within a project");
      }
      seenRootIds.add(rootId);
      const rootPath = rootEntry["root"];
      if (!isCleanAbsolutePath(rootPath)) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "the input root must be a clean absolute path");
      }
      inputRoots.push({ id: rootId, root: rootPath });
    }
    captured.push({ id, projectRoot, configRoot, pipelineRoot, launcherId, inputRoots });
  }
  return captured;
}

/**
 * Canonicalizes one operator-configured root: the configured spelling is
 * absolute, the final component must be a real non-symlink directory,
 * and the canonical path is fixed through real filesystem resolution
 * (symlinked ancestor components resolve; no symlink spelling survives).
 */
async function canonicalizeRoot(configured: string, what: string): Promise<string> {
  if (!isAbsolute(configured)) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} must be an absolute path`);
  }
  let info;
  try {
    info = lstatSync(configured);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} does not exist`);
    }
    if (isErrnoException(cause, "EACCES")) {
      throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} cannot be inspected (errno EACCES)`);
    }
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} cannot be inspected`);
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} is not a real directory`);
  }
  try {
    return await realpath(configured);
  } catch {
    throw new ControlPlaneProjectRegistryError("invalid_registry", `${what} cannot be canonicalized`);
  }
}

/**
 * Loads the versioned project registry once: read-only, no-follow, size
 * bounded, exact schema version 1, canonicalized operator roots,
 * recursively frozen result. Nothing is created, repaired or rewritten.
 */
export async function loadControlPlaneProjectRegistry(registryPath: string): Promise<ControlPlaneProjectRegistry> {
  if (!isCleanAbsolutePath(registryPath)) {
    throw new ControlPlaneProjectRegistryError("invalid_registry_path", "the registry path must be a clean absolute path");
  }
  let handle: number;
  try {
    // O_NONBLOCK keeps a hostile or accidental FIFO at the path from
    // blocking the open; the fstat kind check rejects it fail-closed.
    handle = openSync(registryPath, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      throw new ControlPlaneProjectRegistryError("registry_missing", "the registry file does not exist");
    }
    if (isErrnoException(cause, "EACCES")) {
      throw new ControlPlaneProjectRegistryError("registry_unreadable", "the registry file cannot be read (errno EACCES)");
    }
    if (isErrnoException(cause, "ELOOP")) {
      throw new ControlPlaneProjectRegistryError("invalid_registry_path", "the registry path must not be a symbolic link");
    }
    if (isErrnoException(cause, "ENOTDIR")) {
      throw new ControlPlaneProjectRegistryError("invalid_registry_path", "the registry path is not a file location");
    }
    throw new ControlPlaneProjectRegistryError("invalid_registry_path", "the registry path cannot be opened");
  }
  let text: string;
  try {
    const info = fstatSync(handle);
    if (!info.isFile()) {
      throw new ControlPlaneProjectRegistryError("invalid_registry_path", "the registry path must be a regular file");
    }
    if (info.size > REGISTRY_SIZE_LIMIT_BYTES) {
      throw new ControlPlaneProjectRegistryError("registry_too_large", "the registry file exceeds the fixed size limit");
    }
    const chunks: Buffer[] = [];
    let readTotal = 0;
    const buffer = Buffer.alloc(64 * 1024);
    for (;;) {
      const read = readSync(handle, buffer, 0, buffer.length, readTotal);
      if (read === 0) break;
      chunks.push(Buffer.from(buffer.subarray(0, read)));
      readTotal += read;
      if (readTotal > REGISTRY_SIZE_LIMIT_BYTES) {
        throw new ControlPlaneProjectRegistryError("registry_too_large", "the registry file exceeds the fixed size limit");
      }
    }
    text = Buffer.concat(chunks).toString("utf8");
  } catch (cause) {
    if (cause instanceof ControlPlaneProjectRegistryError) {
      throw cause;
    }
    if (isErrnoException(cause, "EACCES")) {
      throw new ControlPlaneProjectRegistryError("registry_unreadable", "the registry file cannot be read (errno EACCES)");
    }
    throw new ControlPlaneProjectRegistryError("registry_unreadable", "the registry file cannot be read");
  } finally {
    closeSync(handle);
  }
  const projects = validateRegistryDocument(parseRegistryDocument(text));
  const resolvedProjects: ResolvedControlPlaneProject[] = [];
  for (const project of projects) {
    const projectRoot = await canonicalizeRoot(project.projectRoot, "the registry project root");
    const configRoot = await canonicalizeRoot(project.configRoot, "the registry config root");
    const pipelineRoot = project.pipelineRoot === null ? null : await canonicalizeRoot(project.pipelineRoot, "the registry pipeline root");
    const inputRoots: ResolvedControlPlaneInputRoot[] = [];
    const seenCanonicalRoots = new Set<string>();
    for (const inputRoot of project.inputRoots) {
      const canonical = await canonicalizeRoot(inputRoot.root, "the registry input root");
      if (seenCanonicalRoots.has(canonical)) {
        throw new ControlPlaneProjectRegistryError("invalid_registry", "input roots must resolve to distinct directories within a project");
      }
      seenCanonicalRoots.add(canonical);
      inputRoots.push({ id: inputRoot.id, root: canonical });
    }
    resolvedProjects.push({
      id: project.id,
      projectRoot,
      configRoot,
      pipelineRoot,
      launcherId: project.launcherId,
      inputRoots,
    });
  }
  return deepFreezeValue({ schemaVersion: 1 as const, projects: resolvedProjects });
}

interface CapturedRunRequest {
  readonly runId: string;
  readonly projectId: string;
  readonly inputs: readonly { id: string; rootId: string; ref: string }[];
}

/**
 * Captures the public run request exactly once: exact own keys at both
 * levels, every field read once in contract order, safe ids everywhere,
 * non-empty inputs with unique ids, declaration order preserved. A
 * hostile Proxy or a later mutation of the caller's object cannot change
 * the resolution.
 */
function captureRunRequest(request: unknown): CapturedRunRequest {
  if (!isRecord(request)) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the run request must be an object");
  }
  const requestKeys = Object.keys(request);
  if (requestKeys.length !== 3 || !Object.hasOwn(request, "runId") || !Object.hasOwn(request, "projectId") || !Object.hasOwn(request, "inputs")) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the run request must have exactly runId, projectId and inputs");
  }
  const runId = request["runId"];
  if (!isPipelineV2SafeId(runId)) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the run request runId must be a safe identifier");
  }
  const projectId = request["projectId"];
  if (!isPipelineV2SafeId(projectId)) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the run request projectId must be a safe identifier");
  }
  const inputsValue = request["inputs"];
  if (!Array.isArray(inputsValue) || inputsValue.length === 0) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the run request inputs must be a non-empty list");
  }
  const inputs: { id: string; rootId: string; ref: string }[] = [];
  const seenIds = new Set<string>();
  for (let index = 0; index < inputsValue.length; index += 1) {
    const entry = inputsValue[index];
    if (!isRecord(entry)) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "each run request input must be an object");
    }
    const entryKeys = Object.keys(entry);
    if (entryKeys.length !== 3 || !Object.hasOwn(entry, "id") || !Object.hasOwn(entry, "rootId") || !Object.hasOwn(entry, "ref")) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "each run request input must have exactly id, rootId and ref");
    }
    const id = entry["id"];
    if (!isPipelineV2SafeId(id)) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "the run request input id must be a safe identifier");
    }
    if (seenIds.has(id)) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "the run request input ids must be unique");
    }
    seenIds.add(id);
    const rootId = entry["rootId"];
    if (!isPipelineV2SafeId(rootId)) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "the run request input rootId must be a safe identifier");
    }
    const ref = entry["ref"];
    if (!isPipelineV2SafeId(ref)) {
      throw new ControlPlaneProjectRegistryError("invalid_request", "the run request input ref must be a single child name");
    }
    inputs.push({ id, rootId, ref });
  }
  return { runId, projectId, inputs };
}

/**
 * The single input-reference resolution policy of the registry: the
 * reference must be one immediate child of one declared input root of the
 * project, a real non-symlink regular file whose canonical path remains
 * an immediate child of the canonical root. `what` carries the caller's
 * noun for the content-free diagnostics. Both the run-command composition
 * and the bound-project input resolver reuse exactly this policy.
 */
async function resolveProjectInputReference(
  project: ResolvedControlPlaneProject,
  rootId: string,
  ref: string,
  what: string,
): Promise<string> {
  let root: ResolvedControlPlaneInputRoot | null = null;
  for (const candidate of project.inputRoots) {
    if (candidate.id === rootId) {
      root = candidate;
      break;
    }
  }
  if (root === null) {
    throw new ControlPlaneProjectRegistryError("input_root_not_found", `the project ${project.id} has no input root ${rootId}`);
  }
  const leaf = join(root.root, ref);
  let info;
  try {
    info = lstatSync(leaf);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      throw new ControlPlaneProjectRegistryError("input_not_found", `${what} does not exist in root ${rootId}`);
    }
    if (isErrnoException(cause, "EACCES")) {
      throw new ControlPlaneProjectRegistryError("input_invalid", `${what} cannot be inspected (errno EACCES)`);
    }
    throw new ControlPlaneProjectRegistryError("input_invalid", `${what} cannot be inspected`);
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new ControlPlaneProjectRegistryError("input_invalid", `${what} must be a regular file`);
  }
  let canonicalLeaf: string;
  try {
    canonicalLeaf = await realpath(leaf);
  } catch {
    throw new ControlPlaneProjectRegistryError("input_invalid", `${what} cannot be canonicalized`);
  }
  if (dirname(canonicalLeaf) !== root.root) {
    throw new ControlPlaneProjectRegistryError("input_invalid", `${what} must be an immediate child of its declared root`);
  }
  return canonicalLeaf;
}

/**
 * Resolves one safe input reference against an already-bound project of
 * the loaded registry — the one reusable resolver for a caller-supplied
 * reference that must become the authoritative canonical regular-file
 * path of a subprocess argument (e.g. the revised task body file of a
 * `revise_task` command). The project must be the exact frozen entry of
 * the loaded registry; the caller can never supply a host path.
 */
export async function resolveControlPlaneBoundProjectInput(
  registry: ControlPlaneProjectRegistry,
  project: ResolvedControlPlaneProject,
  rootId: string,
  ref: string,
): Promise<string> {
  if (
    !isRecord(registry) ||
    !Object.isFrozen(registry) ||
    !Array.isArray((registry as unknown as Record<string, unknown>)["projects"])
  ) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry must be a loaded, frozen registry object");
  }
  let declared = false;
  for (const candidate of registry.projects) {
    if (candidate === project) {
      declared = true;
      break;
    }
  }
  if (!declared) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the project must be a declared entry of the loaded registry");
  }
  if (!isPipelineV2SafeId(rootId) || !isPipelineV2SafeId(ref)) {
    throw new ControlPlaneProjectRegistryError("invalid_request", "the input reference must use safe identifiers");
  }
  return await resolveProjectInputReference(project, rootId, ref, "the input reference");
}

/**
 * Resolves one public run request against the loaded registry and
 * returns the existing typed `OrchestratorCommand` (kind `run`) plus its
 * routing configuration. Every path comes exclusively from the registry;
 * the caller can never override the config root, pipeline root, launcher
 * id or project path. The composed command is proven routable through
 * the existing `routeOrchestratorCommand` (called, never duplicated);
 * downstream routing errors pass through unchanged.
 */
export async function resolveControlPlaneRunCommand(
  registry: ControlPlaneProjectRegistry,
  request: ControlPlaneRunRequest,
): Promise<ControlPlaneResolvedRun> {
  if (!isRecord(registry) || !Object.isFrozen(registry) || !Array.isArray((registry as unknown as Record<string, unknown>)["projects"])) {
    throw new ControlPlaneProjectRegistryError("invalid_registry", "the registry must be a loaded, frozen registry object");
  }
  const captured = captureRunRequest(request);
  let project: ResolvedControlPlaneProject | null = null;
  for (const candidate of registry.projects) {
    if (candidate.id === captured.projectId) {
      project = candidate;
      break;
    }
  }
  if (project === null) {
    throw new ControlPlaneProjectRegistryError("project_not_found", `the registry has no project ${captured.projectId}`);
  }
  const resolvedInputs: { id: string; path: string }[] = [];
  for (const input of captured.inputs) {
    const path = await resolveProjectInputReference(project, input.rootId, input.ref, `the input ${input.id}`);
    resolvedInputs.push({ id: input.id, path });
  }
  const command: OrchestratorCommand = {
    kind: "run",
    runId: captured.runId,
    projectPath: project.projectRoot,
    inputs: resolvedInputs,
    ...(project.launcherId === null ? {} : { launcherId: project.launcherId }),
  };
  const routingConfig: OrchestratorRoutingConfig = {
    configRoot: project.configRoot,
    ...(project.pipelineRoot === null ? {} : { pipelineRoot: project.pipelineRoot }),
  };
  routeOrchestratorCommand(command, routingConfig);
  return { command, routingConfig };
}
