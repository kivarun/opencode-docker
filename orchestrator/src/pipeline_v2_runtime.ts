import { isAbsolute, join } from "node:path";
import { constants, type Stats } from "node:fs";
import {
  link,
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  rename,
  rm,
  unlink,
  type FileHandle,
} from "node:fs/promises";
import { describeError } from "./docker_helper.ts";
import {
  PipelineError,
  expectExactKeys,
  expectNonEmptyString,
  expectPositiveSafeInteger,
  validateSafeId,
} from "./pipeline.ts";
import {
  ACTIVATION_INPUTS_ROOT,
  ACTIVATION_OUTPUTS_ROOT,
  PROJECT_MOUNT_TARGET,
  parsePortType,
  requireResolvedPipelineV2Provenance,
  type PipelinePortSource,
  type PortType,
  type ResolvedPipelineV2,
  type ResolvedV2State,
} from "./pipeline_v2.ts";

/**
 * Pipeline schema v2 data plane: host-side runtime substrate for run-input
 * snapshots and per-activation data layouts. Pure orchestrator-owned
 * filesystem work — no Sessions, no containers, no helper calls, no
 * bearers, no env, no Docker options.
 *
 * Run layout: `<runRoot>/project` is the shared project directory of the
 * whole run. It must exist as a real non-symlink directory before
 * `snapshotRunInputs` is called; the runtime never creates, clears or
 * copies it (preparing the initial project content is the caller's
 * responsibility), and every activation mounts exactly this one directory
 * at `/workspace` read-write, so changes persist across activations.
 *
 * Run-input bindings + snapshot (`snapshotRunInputs`): every declared
 * pipeline input is bound exactly once to an absolute host path whose real
 * object kind must match the declared `file`/`directory`/`json` type (a
 * `json` binding must additionally be a regular file with valid JSON; the
 * schema is the immutable snapshot already carried by the resolved
 * pipeline — no schema path is re-read). Each input is copied into the
 * orchestrator-owned snapshot `<runRoot>/data/inputs/<input-id>`:
 * file/json byte-for-byte, directories recursively in deterministic
 * (relative-path code-unit sorted) order, only real directories and
 * regular files inside a directory (symlinks, FIFOs, sockets, devices are
 * rejected fail-closed). After the snapshot succeeds, the returned runtime
 * metadata is the only data source of the run: user source paths are no
 * longer read by this module. The frozen snapshot object is registered in
 * a module-private `WeakMap` together with the exact trusted pipeline and
 * the canonical run/project roots; `prepareActivationData` accepts only
 * that exact object (same identity, same pipeline) — hand-built objects,
 * casts, clones, Proxies and snapshots of another pipeline are rejected
 * before any field is read.
 *
 * Activation data layout (`prepareActivationData`): a fresh
 * `<runRoot>/activations/<activation-index>-<state-id>/data/` tree with
 * `inputs/` (copied per declared input port, strictly in declaration
 * order) and `outputs/` (empty real directory pre-created for `directory`
 * output ports; `file`/`json` output paths deliberately absent until a
 * worker run). There is no per-activation project directory. Pipeline
 * inputs are taken only from the run-owned snapshot; state outputs only
 * from the explicitly passed runner-owned accepted records
 * `{state, output, activation_index}` — the type is derived from the
 * declared output port, the object path is the fixed orchestrator-derived
 * `<runRoot>/activations/<index>-<state>/data/outputs/<output>`, and the
 * record with the highest activation index wins for a `state`/`output`
 * pair (list order is irrelevant). Future or current activation indexes,
 * duplicate records, one index spanning several states, missing/forward
 * references and first-visit self-references fail before any downstream
 * use. The activation index is globally unique within the run: before the
 * leaf is created, any existing `activations/` entry with the same
 * `<index>-` prefix (any state, any object kind, never followed) rejects
 * the request.
 *
 * Ownership and modes: every directory is created 0700 and every copied
 * file 0600 by the orchestrator user. Worker-UID compatibility of the
 * output tree is an explicit limitation left to the next runtime
 * increment — no 0777 workaround is used.
 *
 * Failure behavior: all validation happens before any mutation; every
 * created object is tracked and removed again when the operation fails, so
 * a partial snapshot never becomes authoritative and existing snapshots
 * (any pre-existing object at a target path) are never overwritten. The
 * project root is never created, modified or removed by this module.
 * File snapshots are published atomically through `link()` (EEXIST-safe);
 * directory snapshots are built in a temporary directory inside
 * `data/inputs` and published with one `rename()`.
 *
 * Honest boundaries: no protection is claimed against a trusted host
 * process mutating a source while it is being read (validation and copy
 * are separate passes; a mid-copy change is copied as read); `rename()`
 * never replaces a file, a symlink or a non-empty directory, but an
 * adversarially created empty directory could be replaced; a crash
 * (SIGKILL) mid-copy leaves partial objects behind — there is no
 * crash-recovery contract, and a retry fails closed on the leftovers.
 * Copied files are fsynced; no durability claim beyond that. The
 * activation leaf is created exclusively (mkdir), so pre-placed leaves,
 * symlink traps on `runRoot/data`, `data/inputs`, `activations` and leaf
 * paths are rejected, and nothing outside `runRoot` is ever created,
 * written or removed.
 */

/**
 * Module-private provenance registry of run input snapshots.
 *
 * `snapshotRunInputs` registers the exact frozen snapshot object it returns,
 * together with the trusted pipeline object it was created for and the
 * canonical run/project roots. `prepareActivationData` accepts only a
 * registered object whose recorded pipeline is the identical
 * `ResolvedPipelineV2` object it was handed. The registry is keyed by
 * object identity: hand-built objects, casts, shallow or deep clones
 * (e.g. `structuredClone`), Proxies and snapshots created for another
 * pipeline object are all unregistered or mismatched and rejected before
 * any snapshot field is read. There is no second structural validation
 * pass behind this gate, and a future resume loader could mint its own
 * trusted runtime state through the same registration path.
 */
interface RunInputSnapshotProvenance {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRootCanonical: string;
  readonly projectRootCanonical: string;
}

const runInputSnapshotProvenance = new WeakMap<object, RunInputSnapshotProvenance>();

/** Stable rejection message for any snapshot argument without provenance. */
const UNTRUSTED_RUN_INPUT_SNAPSHOT_MESSAGE =
  "activation data preparation requires the frozen run input snapshot object " +
  "returned by a successful snapshotRunInputs call for the same trusted pipeline; " +
  "hand-built objects, casts, clones, snapshots of another pipeline and Proxies " +
  "are rejected before any field is read";

export interface RunInputBinding {
  readonly id: string;
  readonly path: string;
}

/** Runtime metadata for one snapshotted run input. */
export interface RunInputSnapshotEntry {
  readonly id: string;
  readonly type: PortType;
  /** Recorded as declared; no additional policy is applied to it. */
  readonly protected: boolean;
  /** Canonical orchestrator-owned snapshot path (never a user path). */
  readonly snapshot_path: string;
  /** Deterministic content digest; independent of host paths and times. */
  readonly digest: string;
}

export interface RunInputsSnapshot {
  readonly run_root: string;
  readonly inputs_root: string;
  /**
   * Canonical shared project directory of the whole run. It must exist
   * before the snapshot is created; the runtime never creates, clears or
   * copies it, and every activation mounts exactly this directory at
   * `/workspace` read-write.
   */
  readonly project_root: string;
  readonly inputs: readonly RunInputSnapshotEntry[];
}

/**
 * One previously accepted state output as handed over by the runner: a
 * logical reference only. The accepted object always lives at the fixed
 * orchestrator-derived path
 * `<runRoot>/activations/<activation_index>-<state>/data/outputs/<output>`;
 * no user paths and no types are accepted — the type is derived from the
 * declared output port and the path is computed by the runtime. The list is
 * trusted input of the runner (agent envelopes, stdout and worker files can
 * never create records); the last accepted output of a state/output pair is
 * the record with the highest activation index, independent of list order.
 */
export interface AcceptedStateOutput {
  readonly state: string;
  readonly output: string;
  readonly activation_index: number;
}

export interface PreparedActivationInputPort {
  readonly id: string;
  readonly source: PipelinePortSource;
  readonly type: PortType;
  readonly path: string;
}

export interface PreparedActivationOutputPort {
  readonly id: string;
  readonly type: PortType;
  readonly path: string;
}

export interface PreparedActivationMount {
  readonly source: string;
  readonly target: string;
  readonly read_only: boolean;
}

export interface PreparedActivationData {
  readonly run_root: string;
  readonly state_id: string;
  readonly activation_index: number;
  readonly activation_root: string;
  readonly data_root: string;
  readonly inputs_root: string;
  readonly outputs_root: string;
  readonly project_root: string;
  readonly input_ports: readonly PreparedActivationInputPort[];
  readonly output_ports: readonly PreparedActivationOutputPort[];
  readonly mounts: readonly PreparedActivationMount[];
  /**
   * Kept from the compiled plan: undeclared-output validation after a
   * worker run is not implemented in this increment.
   */
  readonly reject_undeclared_outputs: true;
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

function fail(what: string, cause?: unknown): PipelineError {
  if (cause === undefined) {
    return new PipelineError(what);
  }
  return new PipelineError(`${what}: ${describeError(cause)}`);
}

function isErrnoException(cause: unknown, code: string): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as NodeJS.ErrnoException).code === code
  );
}

async function lstatOrNull(path: string): Promise<Stats | null> {
  try {
    return await lstat(path);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      return null;
    }
    throw fail(`filesystem entry ${path} cannot be inspected`, cause);
  }
}

function describeEntry(info: Stats): string {
  return info.isSymbolicLink()
    ? "a symbolic link"
    : info.isDirectory()
      ? "an existing directory"
      : info.isFile()
        ? "an existing regular file"
        : "an unexpected object";
}

async function requireRealDirectory(path: string, what: string): Promise<void> {
  const info = await lstatOrNull(path);
  if (info === null) {
    throw new PipelineError(`${what} ${path} does not exist`);
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new PipelineError(`${what} ${path} exists but is ${describeEntry(info)}`);
  }
}

/**
 * Ensure a real non-symlink directory at `path` (created 0700 when absent).
 * An existing object that is not a real directory is rejected; creation is
 * exclusive, so a concurrent creator loses instead of being adopted.
 */
async function ensureRealDirectory(path: string, what: string): Promise<void> {
  const info = await lstatOrNull(path);
  if (info !== null) {
    if (info.isSymbolicLink() || !info.isDirectory()) {
      throw new PipelineError(`${what} ${path} exists but is ${describeEntry(info)}`);
    }
    return;
  }
  try {
    await mkdir(path, { mode: 0o700 });
  } catch (cause) {
    throw fail(`${what} ${path} could not be created as a real directory`, cause);
  }
}

/** Create a directory that must be absent; any pre-existing object rejects. */
async function createRealDirectoryExclusive(path: string, what: string): Promise<void> {
  const info = await lstatOrNull(path);
  if (info !== null) {
    throw new PipelineError(`${what} ${path} already exists, found ${describeEntry(info)}`);
  }
  try {
    await mkdir(path, { mode: 0o700 });
  } catch (cause) {
    throw fail(`${what} ${path} could not be created as a new directory`, cause);
  }
}

async function requireAbsent(path: string, what: string): Promise<void> {
  const info = await lstatOrNull(path);
  if (info !== null) {
    throw new PipelineError(`${what} ${path} already exists, found ${describeEntry(info)}`);
  }
}

/**
 * Canonicalize an absolute run root that must be a real directory. Symlinked
 * ancestors resolve through realpath (the returned canonical path is used
 * exclusively); a symlinked final component is rejected.
 */
async function requireCanonicalRunRoot(runRoot: string, what: string): Promise<string> {
  if (!isAbsolute(runRoot)) {
    throw new PipelineError(`${what} must be an absolute path, got ${JSON.stringify(runRoot)}`);
  }
  await requireRealDirectory(runRoot, what);
  try {
    return await realpath(runRoot);
  } catch (cause) {
    throw fail(`${what} ${runRoot} cannot be canonicalized`, cause);
  }
}

function isInsideRoot(rootCanonical: string, canonicalPath: string): boolean {
  return canonicalPath.startsWith(`${rootCanonical}/`);
}

async function readRegularFileBytes(path: string, what: string): Promise<Buffer> {
  let handle: FileHandle;
  try {
    handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  } catch (cause) {
    throw fail(`${what} ${path} is not readable as a regular file`, cause);
  }
  try {
    const chunks: Buffer[] = [];
    const scratch = Buffer.allocUnsafe(1 << 20);
    for (;;) {
      const { bytesRead } = await handle.read(scratch, 0, scratch.byteLength, null);
      if (bytesRead <= 0) {
        break;
      }
      chunks.push(Buffer.from(scratch.subarray(0, bytesRead)));
    }
    return Buffer.concat(chunks);
  } catch (cause) {
    throw fail(`${what} ${path} could not be read`, cause);
  } finally {
    try {
      await handle.close();
    } catch {
      // best effort; the original failure, if any, propagates
    }
  }
}

async function writeRegularFileExclusive(
  path: string,
  content: Uint8Array,
  what: string,
): Promise<void> {
  let handle: FileHandle;
  try {
    handle = await open(
      path,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
  } catch (cause) {
    if (isErrnoException(cause, "EEXIST")) {
      throw new PipelineError(`${what} ${path} already exists`);
    }
    throw fail(`${what} ${path} could not be created exclusively`, cause);
  }
  try {
    let written = 0;
    while (written < content.byteLength) {
      const { bytesWritten } = await handle.write(content, written, content.byteLength - written);
      if (bytesWritten <= 0) {
        throw fail(`${what} ${path} could not be written completely`);
      }
      written += bytesWritten;
    }
    await handle.sync();
  } catch (cause) {
    throw fail(`${what} ${path} could not be written`, cause);
  } finally {
    try {
      await handle.close();
    } catch {
      // best effort
    }
  }
}

interface ScannedTreeEntry {
  readonly kind: "directory" | "file";
  readonly relativePath: string;
  readonly absolutePath: string;
}

/**
 * Scan a source tree into a deterministic order: relative POSIX paths
 * sorted by code-unit order (parents always precede their descendants).
 * Only real directories and regular files are allowed; symlinks, FIFOs,
 * sockets, devices and other special entries are rejected fail-closed.
 */
async function scanDirectoryTree(sourcePath: string, what: string): Promise<ScannedTreeEntry[]> {
  const entries: ScannedTreeEntry[] = [];
  const walk = async (dirPath: string, relative: string): Promise<void> => {
    let dirents;
    try {
      dirents = await readdir(dirPath, { withFileTypes: true });
    } catch (cause) {
      throw fail(`${what} ${dirPath} could not be listed`, cause);
    }
    for (const dirent of dirents) {
      const childRelative = relative === "" ? dirent.name : `${relative}/${dirent.name}`;
      const childPath = join(dirPath, dirent.name);
      if (dirent.isSymbolicLink()) {
        throw new PipelineError(
          `${what} contains symlink entry ${JSON.stringify(childRelative)}`,
        );
      }
      if (dirent.isDirectory()) {
        entries.push({ kind: "directory", relativePath: childRelative, absolutePath: childPath });
        await walk(childPath, childRelative);
      } else if (dirent.isFile()) {
        entries.push({ kind: "file", relativePath: childRelative, absolutePath: childPath });
      } else {
        throw new PipelineError(
          `${what} contains unsupported entry ${JSON.stringify(childRelative)}: only real directories and regular files are allowed`,
        );
      }
    }
  };
  await walk(sourcePath, "");
  entries.sort((a, b) => (a.relativePath < b.relativePath ? -1 : a.relativePath > b.relativePath ? 1 : 0));
  return entries;
}

function hashBytes(hasher: Bun.CryptoHasher, bytes: Uint8Array): void {
  const length = Buffer.alloc(8);
  length.writeBigUInt64BE(BigInt(bytes.byteLength));
  hasher.update(length);
  hasher.update(bytes);
}

function hashTag(hasher: Bun.CryptoHasher, tag: string): void {
  hasher.update(Buffer.from(tag, "utf8"));
}

/**
 * Digest stream for one input snapshot. It opens with the fixed domain tag
 * and the declared port type. For `file`/`json` the length-framed content
 * follows. For directories every entry appears in sorted relative-path
 * order as: entry kind tag (`directory\0` or `file\0`), the length-framed
 * UTF-8 relative path, and — for regular files — the length-framed content.
 * Empty directories contribute their kind tag and path only, so names,
 * entry kinds, and empty directories all change the digest. Absolute
 * paths, inodes, permissions and timestamps never participate.
 */
function inputDigestHasher(type: PortType): Bun.CryptoHasher {
  const hasher = new Bun.CryptoHasher("sha256");
  hashTag(hasher, "pipeline-v2-input\0");
  hashTag(hasher, `${type}\0`);
  return hasher;
}

function hashRelativePath(hasher: Bun.CryptoHasher, relativePath: string): void {
  hashBytes(hasher, Buffer.from(relativePath, "utf8"));
}

/** Hash one directory entry: kind tag, length-framed relative path, and —
 * for regular files — the length-framed content. */
function hashDirectoryEntry(
  hasher: Bun.CryptoHasher,
  entry: ScannedTreeEntry,
  content: Buffer | undefined,
): void {
  hashTag(hasher, `${entry.kind}\0`);
  hashRelativePath(hasher, entry.relativePath);
  if (entry.kind === "file" && content !== undefined) {
    hashBytes(hasher, content);
  }
}

/**
 * Copy a directory tree from `sourcePath` into the existing real directory
 * `destPath`, creating children exclusively in deterministic sorted order.
 */
async function copyDirectoryTreeInto(
  sourcePath: string,
  destPath: string,
  what: string,
  hasher?: Bun.CryptoHasher,
): Promise<void> {
  const tree = await scanDirectoryTree(sourcePath, what);
  for (const entry of tree) {
    const target = join(destPath, entry.relativePath);
    if (entry.kind === "directory") {
      await createRealDirectoryExclusive(
        target,
        `${what} directory entry ${JSON.stringify(entry.relativePath)}`,
      );
      continue;
    }
    const content = await readRegularFileBytes(
      entry.absolutePath,
      `${what} file entry ${JSON.stringify(entry.relativePath)}`,
    );
    if (hasher !== undefined) {
      hashBytes(hasher, content);
    }
    await writeRegularFileExclusive(
      target,
      content,
      `${what} file entry ${JSON.stringify(entry.relativePath)}`,
    );
  }
}

/**
 * Copy one typed value (file, json or directory object at `sourcePath`)
 * into a fresh target path below an existing real directory. Regular files
 * are written O_EXCL|O_NOFOLLOW, directories are created exclusively and
 * filled recursively.
 */
async function copyTypedValue(
  sourcePath: string,
  type: PortType,
  targetPath: string,
  what: string,
  hasher?: Bun.CryptoHasher,
): Promise<void> {
  if (type === "directory") {
    await createRealDirectoryExclusive(targetPath, what);
    await copyDirectoryTreeInto(sourcePath, targetPath, what, hasher);
    return;
  }
  const content = await readRegularFileBytes(sourcePath, what);
  if (hasher !== undefined) {
    hashBytes(hasher, content);
  }
  await writeRegularFileExclusive(targetPath, content, what);
}

let tmpCounter = 0;

/** Unique temporary name inside the inputs root; never collides with safe ids. */
function tmpEntryName(id: string): string {
  tmpCounter += 1;
  const random = Math.random().toString(36).slice(2, 10);
  return `.tmp-${tmpCounter}-${random}-${id}`;
}

/**
 * Best-effort removal of objects created by one failed operation. A path
 * that became a symlink is unlinked (never followed); directories are
 * removed recursively only while their canonical resolution stays inside
 * the canonical run root. Cleanup errors are swallowed: the original
 * operation failure is the authoritative outcome.
 */
async function removeTrackedPath(path: string, runRootCanonical: string): Promise<void> {
  try {
    const info = await lstatOrNull(path);
    if (info === null) {
      return;
    }
    if (info.isSymbolicLink()) {
      await unlink(path);
      return;
    }
    if (!info.isDirectory()) {
      await rm(path, { force: true });
      return;
    }
    const canonical = await realpath(path);
    if (canonical !== runRootCanonical && !isInsideRoot(runRootCanonical, canonical)) {
      return;
    }
    await rm(path, { recursive: true, force: true });
  } catch {
    // best effort
  }
}

interface PreparedBindingSource {
  readonly content?: Buffer;
  readonly tree?: ScannedTreeEntry[];
}

function parseRunInputBindings(bindings: readonly unknown[]): RunInputBinding[] {
  if (!Array.isArray(bindings)) {
    throw new PipelineError("run input bindings must be a list");
  }
  return bindings.map((raw, index) => {
    const what = `run input binding ${index}`;
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new PipelineError(`${what} is not an object`);
    }
    const entry = raw as Record<string, unknown>;
    expectExactKeys(entry, ["id", "path"], what);
    const id = validateSafeId(entry.id, `${what} id`);
    const path = expectNonEmptyString(entry.path, `${what} path`);
    if (!isAbsolute(path)) {
      throw new PipelineError(`${what} path must be an absolute path, got ${JSON.stringify(path)}`);
    }
    return { id, path };
  });
}

/**
 * Bind every declared pipeline input exactly once and snapshot it into the
 * orchestrator-owned run data tree. The trusted resolved pipeline must come
 * from `loadPipelineV2`; `runRoot` must be an existing absolute real
 * directory. On success the returned frozen metadata (snapshot paths and
 * digests) is the run's only data source — user source paths are never read
 * again by this module.
 */
export async function snapshotRunInputs(
  pipeline: ResolvedPipelineV2,
  bindings: readonly unknown[],
  runRoot: string,
): Promise<RunInputsSnapshot> {
  requireResolvedPipelineV2Provenance(pipeline, "run input binding");
  const parsedBindings = parseRunInputBindings(bindings);
  const declared = pipeline.inputs;
  const declaredById = new Map(declared.map((input) => [input.id, input]));

  const seenIds = new Set<string>();
  for (const binding of parsedBindings) {
    if (!declaredById.has(binding.id)) {
      throw new PipelineError(
        `run input binding ${JSON.stringify(binding.id)} does not match a declared pipeline input`,
      );
    }
    if (seenIds.has(binding.id)) {
      throw new PipelineError(
        `pipeline input ${JSON.stringify(binding.id)} is bound more than once`,
      );
    }
    seenIds.add(binding.id);
  }
  for (const input of declared) {
    if (!seenIds.has(input.id)) {
      throw new PipelineError(`pipeline input ${JSON.stringify(input.id)} is not bound`);
    }
  }

  const runRootCanonical = await requireCanonicalRunRoot(runRoot, "run root");

  // The shared project directory of the whole run must already exist as a
  // real non-symlink directory; the runtime never creates, clears or copies
  // it, and it is mounted at /workspace by every activation.
  const projectRootPath = join(runRootCanonical, "project");
  await requireRealDirectory(projectRootPath, "run project root");
  const projectRootCanonical = await realpath(projectRootPath);

  // Validate every bound object (real kind, json parseability, clean tree,
  // outside the run root) and cache contents/plans before any mutation.
  interface PreparedBinding extends RunInputBinding {
    readonly content?: Buffer;
    readonly tree?: ScannedTreeEntry[];
  }
  const prepared: PreparedBinding[] = [];
  for (const binding of parsedBindings) {
    const input = declaredById.get(binding.id);
    if (input === undefined) {
      throw new PipelineError(
        `run input binding ${JSON.stringify(binding.id)} does not match a declared pipeline input`,
      );
    }
    const what = `pipeline input ${JSON.stringify(binding.id)}`;
    const info = await lstatOrNull(binding.path);
    if (info === null) {
      throw new PipelineError(`${what} bound path ${binding.path} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new PipelineError(
        `${what} bound path ${binding.path} is a symbolic link; bind the real object`,
      );
    }
    if (input.type === "directory") {
      if (!info.isDirectory()) {
        throw new PipelineError(
          `${what} declares type "directory" but the bound path is not a real directory`,
        );
      }
    } else if (!info.isFile()) {
      throw new PipelineError(
        `${what} declares type ${JSON.stringify(input.type)} but the bound path is not a regular file`,
      );
    }
    let canonicalSource: string;
    try {
      canonicalSource = await realpath(binding.path);
    } catch (cause) {
      throw fail(`${what} bound path ${binding.path} cannot be canonicalized`, cause);
    }
    if (
      canonicalSource === runRootCanonical ||
      isInsideRoot(runRootCanonical, canonicalSource)
    ) {
      throw new PipelineError(
        `${what} bound path must resolve outside the canonical run root ${runRootCanonical}`,
      );
    }
    if (input.type === "directory") {
      prepared.push({ ...binding, tree: await scanDirectoryTree(binding.path, what) });
    } else {
      const content = await readRegularFileBytes(binding.path, what);
      if (input.type === "json") {
        try {
          JSON.parse(content.toString("utf8"));
        } catch (cause) {
          throw new PipelineError(
            `${what} bound file ${binding.path} is not valid JSON: ${describeError(cause)}`,
          );
        }
      }
      prepared.push({ ...binding, content });
    }
  }

  const dataPath = join(runRootCanonical, "data");
  const inputsRoot = join(dataPath, "inputs");
  const dataExisted = (await lstatOrNull(dataPath)) !== null;
  await ensureRealDirectory(dataPath, "run data root");
  const inputsRootExisted = (await lstatOrNull(inputsRoot)) !== null;
  await ensureRealDirectory(inputsRoot, "run inputs root");

  const createdPaths = new Set<string>();
  const cleanupCreated = async (): Promise<void> => {
    if (!inputsRootExisted) {
      await removeTrackedPath(inputsRoot, runRootCanonical);
    }
    for (const path of createdPaths) {
      await removeTrackedPath(path, runRootCanonical);
    }
    if (!dataExisted) {
      await removeTrackedPath(dataPath, runRootCanonical);
    }
  };

  try {
    // Fail fast before any copy when a snapshot entry already exists.
    for (const input of declared) {
      await requireAbsent(
        join(inputsRoot, input.id),
        `run input snapshot of ${JSON.stringify(input.id)}`,
      );
    }

    const entries: RunInputSnapshotEntry[] = [];
    for (const input of declared) {
      const binding = prepared.find((entry) => entry.id === input.id);
      if (binding === undefined) {
        throw new PipelineError(
          `run input binding for ${JSON.stringify(input.id)} disappeared during snapshot`,
        );
      }
      const what = `run input snapshot of ${JSON.stringify(input.id)}`;
      const finalPath = join(inputsRoot, input.id);
      const hasher = inputDigestHasher(input.type);
      if (input.type === "directory") {
        const tree = binding.tree;
        if (tree === undefined) {
          throw new PipelineError(`${what} has no scanned source tree`);
        }
        const tmpDir = join(inputsRoot, tmpEntryName(input.id));
        createdPaths.add(tmpDir);
        await createRealDirectoryExclusive(tmpDir, what);
        for (const entry of tree) {
          const target = join(tmpDir, entry.relativePath);
          if (entry.kind === "directory") {
            hashDirectoryEntry(hasher, entry, undefined);
            await createRealDirectoryExclusive(
              target,
              `${what} directory entry ${JSON.stringify(entry.relativePath)}`,
            );
            continue;
          }
          const content = await readRegularFileBytes(
            entry.absolutePath,
            `${what} file entry ${JSON.stringify(entry.relativePath)}`,
          );
          hashDirectoryEntry(hasher, entry, content);
          await writeRegularFileExclusive(
            target,
            content,
            `${what} file entry ${JSON.stringify(entry.relativePath)}`,
          );
        }
        await requireAbsent(finalPath, what);
        try {
          await rename(tmpDir, finalPath);
        } catch (cause) {
          throw fail(`${what} could not be published at ${finalPath}`, cause);
        }
        createdPaths.delete(tmpDir);
        createdPaths.add(finalPath);
      } else {
        const content = binding.content;
        if (content === undefined) {
          throw new PipelineError(`${what} has no cached source content`);
        }
        hashBytes(hasher, content);
        const tmpPath = join(inputsRoot, tmpEntryName(input.id));
        createdPaths.add(tmpPath);
        await writeRegularFileExclusive(tmpPath, content, what);
        try {
          await link(tmpPath, finalPath);
        } catch (cause) {
          if (isErrnoException(cause, "EEXIST")) {
            throw new PipelineError(`${what} ${finalPath} already exists`);
          }
          throw fail(`${what} could not be published at ${finalPath}`, cause);
        }
        createdPaths.delete(tmpPath);
        createdPaths.add(finalPath);
        await unlink(tmpPath).catch(() => {
          // leftover temporary name is harmless garbage, never a snapshot
        });
      }
      entries.push({
        id: input.id,
        type: input.type,
        protected: input.protected,
        snapshot_path: finalPath,
        digest: hasher.digest("hex"),
      });
    }

    const snapshot = deepFreeze({
      run_root: runRootCanonical,
      inputs_root: inputsRoot,
      project_root: projectRootCanonical,
      inputs: entries,
    });
    // Register provenance only after the full snapshot succeeded; the
    // exact frozen object is the key, so hand-built objects, casts,
    // clones and Proxies can never acquire provenance.
    runInputSnapshotProvenance.set(snapshot, {
      pipeline,
      runRootCanonical,
      projectRootCanonical,
    });
    return snapshot;
  } catch (cause) {
    await cleanupCreated();
    throw cause;
  }
}

interface ParsedAcceptedOutput {
  readonly state: string;
  readonly output: string;
  readonly activationIndex: number;
  /** Derived from the declared output port; never taken from the record. */
  readonly type: PortType;
}

/**
 * Phase 1 of accepted-history validation. Parse and validate the
 * runner-owned accepted records `{state, output, activation_index}`: the
 * argument must be a list (never a stray `TypeError`), every record has
 * exact fields, safe ids, a positive safe index below the current
 * activation index, a declared agent state, and a declared output port
 * (whose declared type becomes the derived record type). Cross-record
 * invariants: no duplicate records for one activation, and one activation
 * index can never belong to two different states. List order is
 * irrelevant — selection happens only after every record has been fully
 * resolved (see `resolveAcceptedOutputs`).
 */
function parseAcceptedStateOutputs(
  acceptedOutputs: readonly unknown[],
  pipeline: ResolvedPipelineV2,
  currentActivationIndex: number,
): ParsedAcceptedOutput[] {
  if (!Array.isArray(acceptedOutputs)) {
    throw new PipelineError("accepted state outputs must be a list");
  }
  const agentStates = new Map<string, Map<string, PortType>>();
  for (const state of pipeline.states) {
    if (state.type !== "agent") {
      continue;
    }
    agentStates.set(
      state.id,
      new Map(state.outputs.map((port) => [port.id, port.type])),
    );
  }
  const parsed: ParsedAcceptedOutput[] = acceptedOutputs.map((raw, index) => {
    const what = `accepted state output ${index}`;
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
      throw new PipelineError(`${what} is not an object`);
    }
    const entry = raw as Record<string, unknown>;
    expectExactKeys(entry, ["state", "output", "activation_index"], what);
    const state = validateSafeId(entry.state, `${what} state id`);
    const output = validateSafeId(entry.output, `${what} output port id`);
    const activationIndex = expectPositiveSafeInteger(
      entry.activation_index,
      `${what} activation index`,
    );
    const declaredOutputs = agentStates.get(state);
    if (declaredOutputs === undefined) {
      throw new PipelineError(
        `${what} references state ${JSON.stringify(state)} which is not a declared agent state`,
      );
    }
    const declaredType = declaredOutputs.get(output);
    if (declaredType === undefined) {
      throw new PipelineError(
        `${what} references output ${JSON.stringify(output)} which is not declared by state ${JSON.stringify(state)}`,
      );
    }
    if (activationIndex >= currentActivationIndex) {
      throw new PipelineError(
        `${what} records activation index ${activationIndex} which is not below the current activation index ${currentActivationIndex}`,
      );
    }
    return { state, output, activationIndex, type: declaredType };
  });

  const seenRecords = new Set<string>();
  const activationStates = new Map<number, string>();
  for (const record of parsed) {
    const recordKey = `${record.activationIndex}\u0000${record.state}\u0000${record.output}`;
    if (seenRecords.has(recordKey)) {
      throw new PipelineError(
        `accepted state output for ${JSON.stringify(record.state)}.${JSON.stringify(record.output)} at activation index ${record.activationIndex} is listed more than once`,
      );
    }
    seenRecords.add(recordKey);
    const declaredState = activationStates.get(record.activationIndex);
    if (declaredState === undefined) {
      activationStates.set(record.activationIndex, record.state);
    } else if (declaredState !== record.state) {
      throw new PipelineError(
        `activation index ${record.activationIndex} cannot belong to both state ${JSON.stringify(declaredState)} and state ${JSON.stringify(record.state)}`,
      );
    }
  }
  return parsed;
}

interface ResolvedAcceptedOutput {
  readonly type: PortType;
  readonly canonicalPath: string;
  readonly activationIndex: number;
}

/**
 * Phases 2 and 3 of accepted-history validation. Phase 2 resolves EVERY
 * record to its fixed orchestrator-derived path: the accepted history is a
 * single runner-owned journal and must be internally coherent as a whole,
 * so a newer correct record never excuses an older phantom or corrupted
 * one. Each record's activation leaf, `data` and `outputs` parents and the
 * final object must be real non-symlink objects (the final one of the kind
 * matching the declared output port type), and the canonical resolution
 * must stay inside the canonical run root. Phase 3 — only after every
 * record resolved — selects the record with the highest activation index
 * for each `state`/`output` pair (permutation of the record list changes
 * nothing).
 */
async function resolveAcceptedOutputs(
  acceptedOutputs: readonly ParsedAcceptedOutput[],
  runRootCanonical: string,
): Promise<Map<string, ResolvedAcceptedOutput>> {
  const activationsRoot = join(runRootCanonical, "activations");
  interface FullyResolvedRecord {
    readonly record: ParsedAcceptedOutput;
    readonly type: PortType;
    readonly canonicalPath: string;
  }
  const fullyResolved: FullyResolvedRecord[] = [];
  if (acceptedOutputs.length > 0) {
    await requireRealDirectory(activationsRoot, "activations root");
  }
  for (const record of acceptedOutputs) {
    const what = `accepted state output for ${JSON.stringify(record.state)}.${JSON.stringify(record.output)} at activation index ${record.activationIndex}`;
    const leafPath = join(activationsRoot, `${record.activationIndex}-${record.state}`);
    await requireRealDirectory(leafPath, `${what} activation leaf`);
    await requireRealDirectory(join(leafPath, "data"), `${what} activation data root`);
    await requireRealDirectory(join(leafPath, "data", "outputs"), `${what} activation outputs root`);
    const fixedPath = join(leafPath, "data", "outputs", record.output);
    const info = await lstatOrNull(fixedPath);
    if (info === null) {
      throw new PipelineError(`${what} fixed output path ${fixedPath} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new PipelineError(`${what} fixed output path ${fixedPath} is a symbolic link`);
    }
    if (record.type === "directory") {
      if (!info.isDirectory()) {
        throw new PipelineError(
          `${what} fixed output ${fixedPath} is not a real directory, found ${describeEntry(info)}`,
        );
      }
    } else if (!info.isFile()) {
      throw new PipelineError(
        `${what} fixed output ${fixedPath} is not a regular file, found ${describeEntry(info)}`,
      );
    }
    let canonical: string;
    try {
      canonical = await realpath(fixedPath);
    } catch (cause) {
      throw fail(`${what} fixed output ${fixedPath} cannot be canonicalized`, cause);
    }
    if (!isInsideRoot(runRootCanonical, canonical)) {
      throw new PipelineError(
        `${what} fixed output resolves outside the canonical run root ${runRootCanonical}`,
      );
    }
    fullyResolved.push({ record, type: record.type, canonicalPath: canonical });
  }

  const resolved = new Map<string, ResolvedAcceptedOutput>();
  for (const resolvedRecord of fullyResolved) {
    const key = `${resolvedRecord.record.state}\u0000${resolvedRecord.record.output}`;
    const existing = resolved.get(key);
    if (existing === undefined || resolvedRecord.record.activationIndex > existing.activationIndex) {
      resolved.set(key, {
        type: resolvedRecord.type,
        canonicalPath: resolvedRecord.canonicalPath,
        activationIndex: resolvedRecord.record.activationIndex,
      });
    }
  }
  return resolved;
}

function findAgentState(
  pipeline: ResolvedPipelineV2,
  stateId: string,
): ResolvedV2State | undefined {
  let found: ResolvedV2State | undefined;
  for (const candidate of pipeline.states) {
    if (candidate.id === stateId) {
      found = candidate;
      break;
    }
  }
  return found;
}

/**
 * Prepare the host-side data layout for one activation of an agent state:
 * a fresh `<runRoot>/activations/<activation-index>-<state-id>/data/` tree
 * with per-port `inputs/` (copied in declaration order from the run-owned
 * snapshot or the accepted records) and per-port `outputs/` (empty real
 * directories for `directory` outputs; `file`/`json` outputs absent until a
 * worker run creates them). The shared run project directory is mounted at
 * `/workspace`; there is no per-activation project directory. Every
 * directory is created 0700, every copied file 0600; the activation index
 * must be globally unused, the leaf must be absent beforehand, and the
 * whole leaf tree is removed again when the preparation fails. The project
 * root is never created, modified or removed here.
 */
export async function prepareActivationData(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  acceptedOutputs: readonly unknown[],
  stateId: string,
  activationIndex: number,
): Promise<PreparedActivationData> {
  requireResolvedPipelineV2Provenance(pipeline, "activation data preparation");
  const provenance = runInputSnapshotProvenance.get(runInputs);
  if (provenance === undefined || provenance.pipeline !== pipeline) {
    throw new PipelineError(UNTRUSTED_RUN_INPUT_SNAPSHOT_MESSAGE);
  }
  const runRootCanonical = provenance.runRootCanonical;
  const projectRoot = provenance.projectRootCanonical;

  expectPositiveSafeInteger(activationIndex, "activation index");
  const safeStateId = validateSafeId(stateId, "activation state id");
  const state = findAgentState(pipeline, safeStateId);
  if (state === undefined) {
    throw new PipelineError(
      `state ${JSON.stringify(safeStateId)} is not declared by the pipeline`,
    );
  }
  if (state.type !== "agent") {
    throw new PipelineError(
      `state ${JSON.stringify(safeStateId)} is not an agent state; activation data is prepared for agent states only`,
    );
  }
  const agentState = state;

  await requireRealDirectory(runRootCanonical, "run root");
  await requireRealDirectory(projectRoot, "run project root");

  const parsedAccepted = parseAcceptedStateOutputs(acceptedOutputs, pipeline, activationIndex);
  const acceptedByRef = await resolveAcceptedOutputs(parsedAccepted, runRootCanonical);

  const activationsRoot = join(runRootCanonical, "activations");
  await ensureRealDirectory(activationsRoot, "activations root");

  // The activation index is globally unique within the run: any existing
  // entry with the same `<index>-` prefix (any state, any object kind,
  // symlinks never followed) rejects the request before the leaf is made.
  const indexPrefix = `${activationIndex}-`;
  let dirents;
  try {
    dirents = await readdir(activationsRoot, { withFileTypes: true });
  } catch (cause) {
    throw fail(`activations root ${activationsRoot} could not be listed`, cause);
  }
  const sortedNames = dirents.map((dirent) => dirent.name).sort();
  for (const name of sortedNames) {
    if (name.startsWith(indexPrefix)) {
      throw new PipelineError(
        `activation index ${activationIndex} is already in use at ${join(activationsRoot, name)}`,
      );
    }
  }

  const activationRoot = join(activationsRoot, `${activationIndex}-${safeStateId}`);
  await requireAbsent(activationRoot, "activation leaf");

  try {
    await mkdir(activationRoot, { mode: 0o700 });
  } catch (cause) {
    throw fail(`activation leaf ${activationRoot} could not be created as a new directory`, cause);
  }

  try {
    const dataRoot = join(activationRoot, "data");
    await ensureRealDirectory(dataRoot, "activation data root");
    const inputsRoot = join(dataRoot, "inputs");
    await ensureRealDirectory(inputsRoot, "activation inputs root");
    const outputsRoot = join(dataRoot, "outputs");
    await ensureRealDirectory(outputsRoot, "activation outputs root");

    const preparedInputs: PreparedActivationInputPort[] = [];
    for (const port of agentState.inputs) {
      const what = `input port ${JSON.stringify(port.id)} of agent state ${JSON.stringify(agentState.id)}`;
      const target = join(inputsRoot, port.id);
      let sourcePath: string;
      if ("pipeline_input" in port.source) {
        let entry: RunInputSnapshotEntry | undefined;
        for (const candidate of runInputs.inputs) {
          if (candidate.id === port.source.pipeline_input) {
            entry = candidate;
            break;
          }
        }
        if (entry === undefined) {
          throw new PipelineError(
            `${what} references pipeline input ${JSON.stringify(port.source.pipeline_input)} which has no run input snapshot entry`,
          );
        }
        sourcePath = entry.snapshot_path;
      } else {
        const accepted = acceptedByRef.get(
          `${port.source.state_output.state}\u0000${port.source.state_output.output}`,
        );
        if (accepted === undefined) {
          throw new PipelineError(
            `${what} references state output ${JSON.stringify(port.source.state_output.state)}.${JSON.stringify(port.source.state_output.output)} which has no accepted output yet (missing, forward or first-visit self reference)`,
          );
        }
        if (accepted.type !== port.type) {
          throw new PipelineError(
            `${what} expects type ${JSON.stringify(port.type)} but the accepted state output has type ${JSON.stringify(accepted.type)}`,
          );
        }
        sourcePath = accepted.canonicalPath;
      }
      await copyTypedValue(sourcePath, port.type, target, what);
      preparedInputs.push({
        id: port.id,
        source: port.source,
        type: port.type,
        path: target,
      });
    }

    const preparedOutputs: PreparedActivationOutputPort[] = [];
    for (const port of agentState.outputs) {
      const what = `output port ${JSON.stringify(port.id)} of agent state ${JSON.stringify(agentState.id)}`;
      const target = join(outputsRoot, port.id);
      if (port.type === "directory") {
        await createRealDirectoryExclusive(target, what);
      } else {
        await requireAbsent(target, `${what} path`);
      }
      preparedOutputs.push({ id: port.id, type: port.type, path: target });
    }

    const mounts: PreparedActivationMount[] = [
      { source: projectRoot, target: PROJECT_MOUNT_TARGET, read_only: false },
      { source: inputsRoot, target: ACTIVATION_INPUTS_ROOT, read_only: true },
      { source: outputsRoot, target: ACTIVATION_OUTPUTS_ROOT, read_only: false },
    ];

    return deepFreeze({
      run_root: runRootCanonical,
      state_id: agentState.id,
      activation_index: activationIndex,
      activation_root: activationRoot,
      data_root: dataRoot,
      inputs_root: inputsRoot,
      outputs_root: outputsRoot,
      project_root: projectRoot,
      input_ports: preparedInputs,
      output_ports: preparedOutputs,
      mounts,
      reject_undeclared_outputs: true as const,
    });
  } catch (cause) {
    // The leaf was created exclusively by this call; remove exactly this
    // tree and nothing else. Run-level infrastructure (data, activations
    // root) and the shared project directory persist.
    await removeTrackedPath(activationRoot, runRootCanonical);
    throw cause;
  }
}
