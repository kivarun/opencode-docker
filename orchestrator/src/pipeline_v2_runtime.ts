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
  evaluatePipelineDecisionState,
  parsePortType,
  requireResolvedPipelineV2Provenance,
  type PipelineDecisionStateResult,
  type PipelinePortSource,
  type PortType,
  type ResolvedPipelineV2,
  type ResolvedV2AgentOutputPort,
  type ResolvedV2DecisionState,
  type ResolvedV2State,
} from "./pipeline_v2.ts";
import { validatePipelineJson } from "./pipeline_v2_schema.ts";

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
 * Output acceptance (`acceptActivationOutputs`): after a worker run, the
 * finished `outputs/` tree of one prepared activation is validated and
 * converted into trusted accepted-output records. The function accepts only
 * the exact `PreparedActivationData` object a successful
 * `prepareActivationData` call returned for the same `ResolvedPipelineV2`
 * object (registered in a module-private `WeakMap` at preparation time;
 * hand-built objects, casts, clones, activations of another pipeline and
 * Proxies are rejected before any field is read). It creates, fixes,
 * renames and deletes nothing — it only reads. The outputs root must
 * contain exactly one top-level entry per declared output port of the
 * state: a missing declared output fails, any undeclared top-level entry
 * fails, and no duplicate/alias/fallback semantics exist — names, paths and
 * types come only from the compiled pipeline and the prepared layout, and
 * the agent never reports them. `file` outputs must be existing regular
 * files with a non-symlink final component, read through `O_NOFOLLOW`;
 * `json` outputs must additionally be valid JSON conforming to the
 * declared, loader-compiled JSON Schema; `directory` outputs must be real
 * non-symlink directories whose recursion allows only real directories and
 * regular files (symlinks, FIFOs, sockets and devices fail). All parents
 * and canonical paths must stay inside the canonical run root and the exact
 * activation outputs location. Every accepted record carries a lowercase
 * SHA-256 digest over a separate `pipeline-v2-output` domain and the
 * declared type, with the same unambiguous framing as input digests
 * (length-framed bytes for file/json; kind tag, length-framed UTF-8
 * relative path, and length-framed file content in code-unit sorted order
 * for directories — names, entry kinds and empty directories participate;
 * host paths, inodes, permissions and timestamps never do). Records are
 * returned deep-frozen, one per declared output, in declaration order.
 * Acceptance re-checks the tree fresh (the worker held the outputs root
 * read-write), so a tree that was replaced, escaped, or filled with
 * symlinks after preparation is rejected.
 *
 * Accepted-history binding: `prepareActivationData` requires the exact
 * record form `{state, output, activation_index, digest}` and re-verifies
 * the whole history before anything is prepared — every record (including
 * old, non-winning ones) is resolved to its fixed orchestrator-derived
 * location and its digest is recomputed and compared, and only then is the
 * winning record per `state`/`output` pair selected by highest activation
 * index. An accepted output that changed after acceptance — content, entry
 * name, empty directory, kind, or JSON bytes — fails the next activation
 * before its leaf is created. The records remain trusted runner-owned
 * input: agent envelopes, stdout and worker files can never create one.
 *
 * Accepted-history coherence: for every `{activation_index, state}` pair
 * the recorded set must be exactly the declared output ports of that agent
 * state — one record per declared output, no missing and no extra records.
 * `acceptActivationOutputs` always releases a full set, so a correct
 * runtime path is unaffected; a partially constructed runner history is
 * rejected as incoherent before any location is resolved, both before the
 * next activation and before run-output collection.
 *
 * Run-input integrity (`verifyRunInputsSnapshot`): before every
 * `prepareActivationData` call and before `collectRunOutputs`, each
 * snapshot entry is re-verified at its fixed orchestrator-owned path — it
 * must still be a real non-symlink object of its declared kind resolving
 * exactly to itself inside the canonical run root, its directory tree may
 * contain only real directories and regular files, and its digest is
 * recomputed with the `pipeline-v2-input` framing and must still match the
 * recorded digest. A modified, relocated or escaped run-input snapshot is
 * detected before the activation leaf or the run-output staging tree is
 * created. The original user binding paths are never read again.
 *
 * Decision-state data adapter (`evaluateDecisionStateFromData`): a pure,
 * read-only host-side adapter that resolves a declared v2 `decision`
 * state's single `json` input through the same data plane and evaluates the
 * state's compiled decision model via `evaluatePipelineDecisionState`. It
 * creates nothing (no decision activation leaf, no `data/inputs`, no
 * `data/outputs`), modifies and deletes nothing, launches nothing, and
 * consumes no activation index: `nextActivationIndex` is only the bound
 * that every accepted record's index must stay strictly below. After both
 * provenance gates, the whole run-input snapshot is re-verified (not just
 * the decision's own input) and the complete accepted history is validated
 * through the same single `resolveAcceptedHistory` chain used by
 * `prepareActivationData` and `collectRunOutputs` (parse, coherence,
 * fixed-location resolution, digest verification, then highest-index
 * winner selection); the decision input then resolves by its declared
 * source — a pipeline input only from the verified snapshot, a state
 * output only from the verified winner map — and its JSON bytes are read
 * from the fixed orchestrator-owned path through `O_NOFOLLOW`, parsed
 * with a content-free diagnostic and validated against the port's
 * loader-compiled schema snapshot. Malformed JSON and schema failures are
 * `PipelineError`s before the evaluator and never become `invalid_facts`;
 * only a schema-conforming value that fails the model's fact-assignment
 * contract yields the existing typed `invalid_facts`. Raw JSON bytes,
 * parsed facts and fact values never appear in results, errors or
 * diagnostics. The adapter returns exactly the existing
 * `PipelineDecisionStateResult`; it never selects a transition target and
 * never moves a graph cursor, and it is not wired into the production
 * runner.
 *
 * Run-output collection (`collectRunOutputs`): when a terminal state has
 * been reached (deciding that is the graph runner's job — this function
 * takes no terminal id, no user paths, no types, no mounts and no Docker
 * options), every declared run-level output is resolved and materialized
 * into the fixed orchestrator-owned `<runRoot>/outputs/<run-output-id>`
 * tree. A source is either a declared pipeline input (resolved only from
 * the trusted run-input snapshot — a pipeline input counts as existing
 * once its snapshot succeeded, and its digest is re-verified) or an agent
 * state output (resolved only from the runner-owned accepted history;
 * the record with the highest activation index wins, and the whole
 * history is parsed, resolved, digest-verified and coherence-checked
 * before any winner is selected — a corrupted old non-winning record
 * fails the whole publication). A present source is published regardless
 * of `required`; a missing source fails the whole operation before
 * publication when `required: true` and marks the output absent (no
 * filesystem entry) when `required: false` — optionality never masks a
 * damaged accepted history. Outputs are processed strictly in declaration
 * order. `json` sources are re-parsed and re-validated against the same
 * loader-compiled Draft 2020-12 schema carried by the resolved run
 * output, and the original JSON bytes are published — never a
 * reserialization. The `outputs` path must be absent beforehand (any
 * pre-existing object fails closed, nothing is ever overwritten; a
 * repeated call after successful publication is therefore rejected); the
 * publication is built in an exclusive temporary sibling directory inside
 * the canonical run root (directories 0700, files 0600, no shell, no
 * `cp`, no `tar`; only real directories and regular files — symlinks,
 * FIFOs, sockets and devices rejected), and is published with a single
 * `rename()` after the full staging tree checks out; a failure before the
 * rename removes exactly the staging tree and touches nothing else —
 * run inputs, activations, accepted outputs and the shared project are
 * never modified. The returned `RunOutputsSnapshot` is deep-frozen, lists
 * one discriminated entry per declared run output in declaration order
 * (`snapshot_path` and `digest` only for published outputs, with the
 * digest taken over a separate `pipeline-v2-run-output` domain from the
 * actually published copy), and is registered in a module-private
 * provenance registry only after the atomic publish succeeded.
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
 * written or removed. The same applies to the run-output staging
 * directory.
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
  "the operation requires the frozen run input snapshot object " +
  "returned by a successful snapshotRunInputs call for the same trusted pipeline; " +
  "hand-built objects, casts, clones, snapshots of another pipeline and Proxies " +
  "are rejected before any field is read";

/**
 * Module-private provenance registry of prepared activation layouts.
 *
 * `prepareActivationData` registers the exact deep-frozen prepared object
 * it returns, together with the trusted pipeline object it was prepared for
 * and the canonical run root. `acceptActivationOutputs` accepts only a
 * registered object whose recorded pipeline is the identical
 * `ResolvedPipelineV2` object it was handed. The registry is keyed by
 * object identity: hand-built objects, casts, shallow or deep clones,
 * Proxies and prepared activations of another pipeline are all unregistered
 * or mismatched and rejected before any field is read. There is no second
 * structural validation pass behind this gate, and a future resume loader
 * could mint trusted runtime state through the same registration path.
 */
interface PreparedActivationProvenance {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRootCanonical: string;
}

const preparedActivationProvenance = new WeakMap<object, PreparedActivationProvenance>();

/** Stable rejection message for any prepared activation without provenance. */
const UNTRUSTED_PREPARED_ACTIVATION_MESSAGE =
  "output acceptance requires the frozen prepared activation data object " +
  "returned by a successful prepareActivationData call for the same trusted pipeline; " +
  "hand-built objects, casts, clones, activations of another pipeline and Proxies " +
  "are rejected before any field is read";

/**
 * Module-private provenance registry of published run output snapshots.
 *
 * `collectRunOutputs` registers the exact deep-frozen snapshot object it
 * returns, together with the trusted pipeline object, the canonical run
 * root and the canonical outputs root — but only after the atomic publish
 * succeeded, so an object that never reached `rename()` can never acquire
 * provenance. The registry is keyed by object identity: hand-built
 * objects, casts, clones and Proxies are unregistered. It is never
 * exported, never serialized, and no provenance marker is embedded in the
 * snapshot itself; a future download/API layer can consume only registered
 * snapshots.
 */
interface RunOutputsSnapshotProvenance {
  readonly pipeline: ResolvedPipelineV2;
  readonly runRootCanonical: string;
  readonly outputsRootCanonical: string;
}

const runOutputsSnapshotProvenance = new WeakMap<object, RunOutputsSnapshotProvenance>();

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
 * logical reference plus the content digest captured at acceptance time.
 * The accepted object always lives at the fixed orchestrator-derived path
 * `<runRoot>/activations/<activation_index>-<state>/data/outputs/<output>`;
 * no user paths and no types are accepted — the type is derived from the
 * declared output port and the path is computed by the runtime. The digest
 * is the lowercase SHA-256 over the `pipeline-v2-output` domain recorded by
 * `acceptActivationOutputs` when the output was accepted; before a new
 * activation is prepared, every record's digest is recomputed from the
 * fixed location and must still match. The list is trusted input of the
 * runner (agent envelopes, stdout and worker files can never create
 * records); the last accepted output of a state/output pair is the record
 * with the highest activation index, independent of list order.
 */
export interface AcceptedStateOutput {
  readonly state: string;
  readonly output: string;
  readonly activation_index: number;
  readonly digest: string;
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
   * Kept from the compiled plan and enforced by `acceptActivationOutputs`
   * since the output-acceptance increment: the top-level entries of the
   * finished outputs tree must be exactly the declared output ports.
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
 * Digest hasher for one typed port value with a fixed domain tag. It opens
 * with the domain tag and the declared port type. For `file`/`json` the
 * length-framed content follows. For directories every entry appears in
 * sorted relative-path order as: entry kind tag (`directory\0` or
 * `file\0`), the length-framed UTF-8 relative path, and — for regular
 * files — the length-framed content. Empty directories contribute their
 * kind tag and path only, so names, entry kinds, and empty directories all
 * change the digest. Absolute paths, inodes, permissions and timestamps
 * never participate.
 */
function portValueDigestHasher(domain: string, type: PortType): Bun.CryptoHasher {
  const hasher = new Bun.CryptoHasher("sha256");
  hashTag(hasher, domain);
  hashTag(hasher, `${type}\0`);
  return hasher;
}

function inputDigestHasher(type: PortType): Bun.CryptoHasher {
  return portValueDigestHasher("pipeline-v2-input\0", type);
}

function outputDigestHasher(type: PortType): Bun.CryptoHasher {
  return portValueDigestHasher("pipeline-v2-output\0", type);
}

function runOutputDigestHasher(type: PortType): Bun.CryptoHasher {
  return portValueDigestHasher("pipeline-v2-run-output\0", type);
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

interface ReadPortValue {
  /** Deterministic digest over the `pipeline-v2-output` domain. */
  readonly digest: string;
  /** Parsed JSON value; present exactly when requested and reading json. */
  readonly parsedJson?: unknown;
}

/**
 * Read one typed port value and compute its output-domain digest. For
 * `file`/`json` the content is read once through `O_NOFOLLOW` and hashed
 * length-framed; for `json` the same bytes are parsed (only when requested)
 * so callers can validate the parsed value against a compiled schema — the
 * digest always covers the raw bytes. For `directory` the tree is scanned
 * fail-closed (only real directories and regular files) and hashed with
 * the unambiguous directory framing. This function performs no containment
 * or kind checks of its own beyond what reading/scanning requires; callers
 * establish type and containment first. It never writes anything.
 */
async function readPortValueForDigest(
  type: PortType,
  path: string,
  what: string,
  parseJson: boolean,
): Promise<ReadPortValue> {
  const hasher = outputDigestHasher(type);
  if (type !== "directory") {
    const content = await readRegularFileBytes(path, what);
    hashBytes(hasher, content);
    let parsedJson: unknown;
    if (type === "json" && parseJson) {
      try {
        parsedJson = JSON.parse(content.toString("utf8"));
      } catch {
        // Stable, content-free diagnostic: the parser message can echo the
        // offending token or an input fragment, so it is never included.
        throw new PipelineError(`${what} ${path} is not valid JSON`);
      }
    }
    return { digest: hasher.digest("hex"), ...(parsedJson !== undefined ? { parsedJson } : {}) };
  }
  const tree = await scanDirectoryTree(path, what);
  for (const entry of tree) {
    if (entry.kind === "directory") {
      hashDirectoryEntry(hasher, entry, undefined);
      continue;
    }
    const content = await readRegularFileBytes(
      entry.absolutePath,
      `${what} file entry ${JSON.stringify(entry.relativePath)}`,
    );
    hashDirectoryEntry(hasher, entry, content);
  }
  return { digest: hasher.digest("hex") };
}

/**
 * Compute the accepted-output digest of one typed value at `path` with the
 * same framing `acceptActivationOutputs` and accepted-history verification
 * use. `type` must be a declared port type. No containment is checked and
 * nothing is written; the path must already be established as a real
 * non-symlink object of the declared kind by the caller. Exposed so tests
 * (and a future resume loader) bind exactly the digest form recorded at
 * acceptance time.
 */
export async function acceptedOutputDigest(
  type: PortType,
  path: string,
  what: string,
): Promise<string> {
  const portType = parsePortType(type, "accepted output digest type");
  return (await readPortValueForDigest(portType, path, what, false)).digest;
}

/**
 * Re-verify the full integrity of a trusted run-input snapshot at its fixed
 * orchestrator-owned paths, without ever re-reading the original user
 * binding paths. Every snapshot entry must still be a real non-symlink
 * object of its declared kind (`file`/`json` regular file, `directory`
 * real directory) whose canonical resolution is exactly its recorded
 * snapshot path — so a replaced final component, a relocated or symlinked
 * ancestor, or an escape out of the canonical run root is detected. A
 * directory snapshot is scanned fail-closed (only real directories and
 * regular files; symlinks, FIFOs, sockets and devices rejected) and every
 * digest is recomputed with the exact `pipeline-v2-input` framing and
 * compared against the recorded digest, so any changed byte, entry name,
 * empty directory, or kind fails before anything is built from the
 * snapshot. Called before every `prepareActivationData` and before
 * `collectRunOutputs`.
 */
async function verifyRunInputsSnapshot(
  runInputs: RunInputsSnapshot,
  provenance: RunInputSnapshotProvenance,
): Promise<void> {
  for (const entry of runInputs.inputs) {
    const what = `run input snapshot of ${JSON.stringify(entry.id)}`;
    const path = entry.snapshot_path;
    const info = await lstatOrNull(path);
    if (info === null) {
      throw new PipelineError(`${what} snapshot object ${path} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new PipelineError(`${what} snapshot object ${path} is a symbolic link`);
    }
    if (entry.type === "directory") {
      if (!info.isDirectory()) {
        throw new PipelineError(
          `${what} snapshot object ${path} is not a real directory, found ${describeEntry(info)}`,
        );
      }
    } else if (!info.isFile()) {
      throw new PipelineError(
        `${what} snapshot object ${path} is not a regular file, found ${describeEntry(info)}`,
      );
    }
    let canonical: string;
    try {
      canonical = await realpath(path);
    } catch (cause) {
      throw fail(`${what} snapshot object ${path} cannot be canonicalized`, cause);
    }
    if (canonical !== path) {
      throw new PipelineError(
        `${what} snapshot object ${path} no longer resolves to itself; the snapshot was relocated or escaped`,
      );
    }
    const hasher = inputDigestHasher(entry.type);
    let recomputed: string;
    if (entry.type === "directory") {
      const tree = await scanDirectoryTree(path, what);
      for (const treeEntry of tree) {
        if (treeEntry.kind === "directory") {
          hashDirectoryEntry(hasher, treeEntry, undefined);
          continue;
        }
        const content = await readRegularFileBytes(
          treeEntry.absolutePath,
          `${what} file entry ${JSON.stringify(treeEntry.relativePath)}`,
        );
        hashDirectoryEntry(hasher, treeEntry, content);
      }
      recomputed = hasher.digest("hex");
    } else {
      const content = await readRegularFileBytes(path, what);
      hashBytes(hasher, content);
      recomputed = hasher.digest("hex");
    }
    if (recomputed !== entry.digest) {
      throw new PipelineError(
        `${what} digest mismatch at ${path}: recorded ${entry.digest}, recomputed ${recomputed}`,
      );
    }
  }
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
        let parsedJson: unknown;
        try {
          parsedJson = JSON.parse(content.toString("utf8"));
        } catch {
          // Stable, content-free diagnostic: the parser message can echo
          // the offending token or an input fragment, so it is never
          // included.
          throw new PipelineError(
            `${what} bound file ${binding.path} is not valid JSON`,
          );
        }
        // The same compiled Draft 2020-12 mechanism that validates JSON
        // agent outputs validates declared json run inputs here, before any
        // snapshot is created. Diagnostics never contain parsed values.
        if (input.schema !== undefined) {
          validatePipelineJson(input.schema, parsedJson, `${what} bound file ${binding.path}`);
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
  readonly digest: string;
  /** Derived from the declared output port; never taken from the record. */
  readonly type: PortType;
}

const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/;

/**
 * Phase 1 of accepted-history validation. Parse and validate the
 * runner-owned accepted records `{state, output, activation_index, digest}`:
 * the argument must be a list (never a stray `TypeError`), every record has
 * exact fields, safe ids, a positive safe index, a lowercase SHA-256 hex
 * digest, a declared agent state, and a declared output port (whose
 * declared type becomes the derived record type). When
 * `currentActivationIndex` is provided (the next-activation path), every
 * index must be below it; run-output collection passes no bound because
 * there is no current activation — there, activation existence is proven by
 * resolving every record's fixed location. Cross-record invariants: no
 * duplicate records for one activation, one activation index can never
 * belong to two different states, and — for every recorded
 * `{activation_index, state}` pair — the recorded set must be exactly the
 * declared output ports of that agent state: `acceptActivationOutputs`
 * releases one record per declared output, so a partially constructed
 * runner history is incoherent and rejected as a whole before any location
 * is resolved, both before the next activation and before run-output
 * publication. List order is irrelevant — selection happens only after
 * every record has been fully resolved and digest-verified (see
 * `resolveAllAcceptedOutputs` and `verifyAcceptedOutputDigests`).
 */
function parseAcceptedStateOutputs(
  acceptedOutputs: readonly unknown[],
  pipeline: ResolvedPipelineV2,
  currentActivationIndex: number | undefined,
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
    expectExactKeys(entry, ["state", "output", "activation_index", "digest"], what);
    const state = validateSafeId(entry.state, `${what} state id`);
    const output = validateSafeId(entry.output, `${what} output port id`);
    const activationIndex = expectPositiveSafeInteger(
      entry.activation_index,
      `${what} activation index`,
    );
    const digest = expectNonEmptyString(entry.digest, `${what} digest`);
    if (!SHA256_HEX_PATTERN.test(digest)) {
      throw new PipelineError(
        `${what} digest ${JSON.stringify(digest)} is not a lowercase SHA-256 hex digest`,
      );
    }
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
    if (currentActivationIndex !== undefined && activationIndex >= currentActivationIndex) {
      throw new PipelineError(
        `${what} records activation index ${activationIndex} which is not below the current activation index ${currentActivationIndex}`,
      );
    }
    return { state, output, activationIndex, digest, type: declaredType };
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

  // Accepted-history coherence: every recorded activation must have
  // recorded exactly the declared output ports of its state. The one-index-
  // one-state invariant above already guarantees a single state per index.
  const recordedByActivation = new Map<number, Set<string>>();
  for (const record of parsed) {
    let recorded = recordedByActivation.get(record.activationIndex);
    if (recorded === undefined) {
      recorded = new Set<string>();
      recordedByActivation.set(record.activationIndex, recorded);
    }
    recorded.add(record.output);
  }
  for (const [activationIndex, recorded] of recordedByActivation) {
    const stateId = activationStates.get(activationIndex);
    const declaredOutputs = stateId === undefined ? undefined : agentStates.get(stateId);
    if (stateId === undefined || declaredOutputs === undefined) {
      throw new PipelineError(
        `accepted history activation index ${activationIndex} records state ${JSON.stringify(stateId)} which is not a declared agent state`,
      );
    }
    const declaredIds = [...declaredOutputs.keys()];
    const missing = declaredIds.filter((id) => !recorded.has(id));
    if (missing.length > 0) {
      throw new PipelineError(
        `accepted history activation index ${activationIndex} of state ${JSON.stringify(stateId)} is incomplete: ` +
          `the activation must record exactly the declared output ports ${JSON.stringify(declaredIds)}, ` +
          `missing ${JSON.stringify(missing)}`,
      );
    }
    const extra = [...recorded].filter((id) => !declaredOutputs.has(id));
    if (extra.length > 0) {
      throw new PipelineError(
        `accepted history activation index ${activationIndex} of state ${JSON.stringify(stateId)} ` +
          `records output ports that the state does not declare: ${JSON.stringify(extra)}`,
      );
    }
  }
  return parsed;
}

interface FullyResolvedRecord {
  readonly record: ParsedAcceptedOutput;
  readonly type: PortType;
  readonly canonicalPath: string;
}

/**
 * Phase 2 of accepted-history validation. EVERY record is resolved to its
 * fixed orchestrator-derived path: the accepted history is a single
 * runner-owned journal and must be internally coherent as a whole, so a
 * newer correct record never excuses an older phantom or corrupted one.
 * Each record's activation leaf, `data` and `outputs` parents and the final
 * object must be real non-symlink objects (the final one of the kind
 * matching the declared output port type), and the canonical resolution
 * must stay inside the canonical run root.
 */
async function resolveAllAcceptedOutputs(
  acceptedOutputs: readonly ParsedAcceptedOutput[],
  runRootCanonical: string,
): Promise<FullyResolvedRecord[]> {
  const activationsRoot = join(runRootCanonical, "activations");
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
  return fullyResolved;
}

/**
 * Phase 3 of accepted-history validation. Every record — including old,
 * non-winning ones — has its digest recomputed from the fixed location with
 * the exact `pipeline-v2-output` framing used at acceptance time; any
 * mismatch (changed content, entry name, empty directory, kind, or JSON
 * bytes) fails the whole preparation before the winning records are
 * selected and before any activation leaf is created.
 */
async function verifyAcceptedOutputDigests(
  fullyResolved: readonly FullyResolvedRecord[],
): Promise<void> {
  for (const resolvedRecord of fullyResolved) {
    const what = `accepted state output for ${JSON.stringify(resolvedRecord.record.state)}.${JSON.stringify(resolvedRecord.record.output)} at activation index ${resolvedRecord.record.activationIndex}`;
    const recomputed = (await readPortValueForDigest(
      resolvedRecord.type,
      resolvedRecord.canonicalPath,
      what,
      false,
    )).digest;
    if (recomputed !== resolvedRecord.record.digest) {
      throw new PipelineError(
        `${what} digest mismatch: recorded ${resolvedRecord.record.digest}, recomputed ${recomputed}`,
      );
    }
  }
}

interface ResolvedAcceptedOutput {
  readonly type: PortType;
  readonly canonicalPath: string;
  readonly activationIndex: number;
}

/**
 * Phase 4 of accepted-history validation — only after every record has
 * been fully resolved and digest-verified — selects the record with the
 * highest activation index for each `state`/`output` pair (permutation of
 * the record list changes nothing).
 */
function selectWinningAcceptedOutputs(
  fullyResolved: readonly FullyResolvedRecord[],
): Map<string, ResolvedAcceptedOutput> {
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

/**
 * The single accepted-history validation chain shared by every runtime API
 * that consumes accepted state outputs: parse + exact-record/coherence
 * validation (`parseAcceptedStateOutputs`), fixed-location resolution of
 * every record including old non-winning ones
 * (`resolveAllAcceptedOutputs`), digest recomputation against the recorded
 * digests (`verifyAcceptedOutputDigests`), and only then the
 * highest-activation-index winner selection per `state`/`output` pair
 * (`selectWinningAcceptedOutputs`). `currentActivationIndex` binds the
 * next-activation path (every recorded index must be strictly below it);
 * run-output collection and other consumers without a current activation
 * pass `undefined` so activation existence is proven by resolving each
 * record's fixed location. Callers must never re-implement this chain: a
 * second resolver could diverge in error order or skip a phase.
 */
async function resolveAcceptedHistory(
  pipeline: ResolvedPipelineV2,
  acceptedOutputs: readonly unknown[],
  runRootCanonical: string,
  currentActivationIndex: number | undefined,
): Promise<Map<string, ResolvedAcceptedOutput>> {
  const parsedAccepted = parseAcceptedStateOutputs(
    acceptedOutputs,
    pipeline,
    currentActivationIndex,
  );
  const fullyResolved = await resolveAllAcceptedOutputs(parsedAccepted, runRootCanonical);
  await verifyAcceptedOutputDigests(fullyResolved);
  return selectWinningAcceptedOutputs(fullyResolved);
}

function findStateById(
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
 *
 * The accepted records must be exact-field
 * `{state, output, activation_index, digest}` objects. Before anything is
 * prepared, the whole accepted history is validated: every record is
 * resolved to its fixed orchestrator-derived location and its digest is
 * recomputed and compared (including old, non-winning records); only after
 * the whole history checks out is the winning record per `state`/`output`
 * pair selected by highest activation index. The returned object is
 * deep-frozen and registered in the module-private prepared-activation
 * provenance registry; only that exact object can later be passed to
 * `acceptActivationOutputs` for the same pipeline object.
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
  const state = findStateById(pipeline, safeStateId);
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

  // The run-owned snapshot is re-verified before anything else happens: a
  // modified, relocated or escaped snapshot input fails the activation
  // before the accepted history is parsed and long before the activation
  // leaf is created.
  await verifyRunInputsSnapshot(runInputs, provenance);

  const acceptedByRef = await resolveAcceptedHistory(
    pipeline,
    acceptedOutputs,
    runRootCanonical,
    activationIndex,
  );

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

    const prepared: PreparedActivationData = deepFreeze({
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
    // Register provenance only after the full preparation succeeded; the
    // exact frozen object is the key, so hand-built objects, casts, clones
    // and Proxies can never acquire provenance.
    preparedActivationProvenance.set(prepared, { pipeline, runRootCanonical });
    return prepared;
  } catch (cause) {
    // The leaf was created exclusively by this call; remove exactly this
    // tree and nothing else. Run-level infrastructure (data, activations
    // root) and the shared project directory persist.
    await removeTrackedPath(activationRoot, runRootCanonical);
    throw cause;
  }
}

/**
 * Accept the finished worker outputs of one prepared activation: validate
 * the completed `outputs/` tree and release trusted accepted-output
 * records. The function creates, fixes, renames and deletes nothing — it
 * only reads already-written output objects.
 *
 * The argument must be the exact `PreparedActivationData` object a
 * successful `prepareActivationData` call returned for the same
 * `ResolvedPipelineV2` object; anything else is rejected before any field
 * is read. All checks re-examine the filesystem fresh (the worker held the
 * outputs root read-write): the outputs root must still be a real
 * non-symlink directory resolving exactly to itself inside the canonical
 * run root, its top-level entries must be exactly the declared output ports
 * (a missing declared output fails, any undeclared entry fails, and there
 * are no duplicate, alias or fallback semantics), and each output must be a
 * real non-symlink object of its declared type whose canonical path stays
 * inside the outputs root. `file` outputs are read through `O_NOFOLLOW`;
 * `json` outputs must be valid JSON conforming to the loader-compiled
 * schema (no coercion, no defaults, nothing removed, value never modified);
 * `directory` outputs allow only real directories and regular files
 * recursively. Diagnostics never contain file contents or JSON values.
 *
 * Returns one deep-frozen record per declared output, in declaration
 * order: `{state, output, activation_index, digest}` with the lowercase
 * SHA-256 digest over the `pipeline-v2-output` domain and the declared
 * type. No path, type, schema, summary, timestamp, or output content is
 * recorded.
 */
export async function acceptActivationOutputs(
  pipeline: ResolvedPipelineV2,
  activation: PreparedActivationData,
): Promise<readonly AcceptedStateOutput[]> {
  requireResolvedPipelineV2Provenance(pipeline, "output acceptance");
  const provenance = preparedActivationProvenance.get(activation);
  if (provenance === undefined || provenance.pipeline !== pipeline) {
    throw new PipelineError(UNTRUSTED_PREPARED_ACTIVATION_MESSAGE);
  }
  const runRootCanonical = provenance.runRootCanonical;

  const state = findStateById(pipeline, activation.state_id);
  if (state === undefined || state.type !== "agent") {
    throw new PipelineError(
      `prepared activation names state ${JSON.stringify(activation.state_id)} which is not a declared agent state of the trusted pipeline`,
    );
  }
  const agentState = state;

  await requireRealDirectory(runRootCanonical, "run root");
  await requireRealDirectory(activation.outputs_root, "activation outputs root");
  let outputsRootCanonical: string;
  try {
    outputsRootCanonical = await realpath(activation.outputs_root);
  } catch (cause) {
    throw fail(
      `activation outputs root ${activation.outputs_root} cannot be canonicalized`,
      cause,
    );
  }
  if (outputsRootCanonical !== activation.outputs_root) {
    throw new PipelineError(
      `activation outputs root ${activation.outputs_root} does not resolve exactly to itself; it was replaced or escaped`,
    );
  }
  if (!isInsideRoot(runRootCanonical, outputsRootCanonical)) {
    throw new PipelineError(
      `activation outputs root resolves outside the canonical run root ${runRootCanonical}`,
    );
  }

  let dirents;
  try {
    dirents = await readdir(activation.outputs_root, { withFileTypes: true });
  } catch (cause) {
    throw fail(`activation outputs root ${activation.outputs_root} could not be listed`, cause);
  }
  const declaredPorts = new Map<string, ResolvedV2AgentOutputPort>(
    agentState.outputs.map((port) => [port.id, port]),
  );
  const found = new Set<string>();
  for (const dirent of dirents) {
    if (!declaredPorts.has(dirent.name)) {
      throw new PipelineError(
        `activation outputs root ${activation.outputs_root} contains undeclared entry ${JSON.stringify(dirent.name)}`,
      );
    }
    found.add(dirent.name);
  }
  for (const port of agentState.outputs) {
    if (!found.has(port.id)) {
      throw new PipelineError(
        `output port ${JSON.stringify(port.id)} of agent state ${JSON.stringify(agentState.id)} activation ${activation.activation_index} is missing from the activation outputs root ${activation.outputs_root}`,
      );
    }
  }

  const records: AcceptedStateOutput[] = [];
  for (const port of agentState.outputs) {
    const what = `output port ${JSON.stringify(port.id)} of agent state ${JSON.stringify(agentState.id)} activation ${activation.activation_index}`;
    const target = join(activation.outputs_root, port.id);
    const info = await lstatOrNull(target);
    if (info === null) {
      throw new PipelineError(`${what} output ${target} does not exist`);
    }
    if (info.isSymbolicLink()) {
      throw new PipelineError(`${what} output ${target} is a symbolic link`);
    }
    let canonicalTarget: string;
    try {
      canonicalTarget = await realpath(target);
    } catch (cause) {
      throw fail(`${what} output ${target} cannot be canonicalized`, cause);
    }
    if (!isInsideRoot(outputsRootCanonical, canonicalTarget)) {
      throw new PipelineError(
        `${what} output resolves outside the activation outputs root ${outputsRootCanonical}`,
      );
    }
    if (port.type === "directory") {
      if (!info.isDirectory()) {
        throw new PipelineError(
          `${what} declares type "directory" but the output is ${describeEntry(info)}`,
        );
      }
    } else if (!info.isFile()) {
      throw new PipelineError(
        `${what} declares type ${JSON.stringify(port.type)} but the output is ${describeEntry(info)}`,
      );
    }
    const read = await readPortValueForDigest(port.type, target, what, true);
    if (port.type === "json") {
      if (port.schema === undefined) {
        throw new PipelineError(`${what} has no compiled JSON schema`);
      }
      validatePipelineJson(port.schema, read.parsedJson, what);
    }
    records.push({
      state: agentState.id,
      output: port.id,
      activation_index: activation.activation_index,
      digest: read.digest,
    });
  }
  return deepFreeze(records);
}

/**
 * One published run output as recorded in the returned snapshot: an
 * optional output whose source was absent is listed as `present: false`
 * (with `required: false` — a missing required source fails the whole
 * publication), a published output carries its fixed orchestrator-owned
 * `snapshot_path` and the deterministic `pipeline-v2-run-output` digest of
 * the actually published copy. No user paths, no accepted-history paths,
 * no timestamps.
 */
export type RunOutputSnapshotEntry =
  | {
      readonly id: string;
      readonly type: PortType;
      readonly required: boolean;
      readonly present: true;
      readonly snapshot_path: string;
      readonly digest: string;
    }
  | {
      readonly id: string;
      readonly type: PortType;
      readonly required: false;
      readonly present: false;
    };

/**
 * The deep-frozen result of a successful `collectRunOutputs`: the canonical
 * run root, the fixed canonical outputs root, and one entry per declared
 * run output in declaration order.
 */
export interface RunOutputsSnapshot {
  readonly run_root: string;
  readonly outputs_root: string;
  readonly outputs: readonly RunOutputSnapshotEntry[];
}

/**
 * Collect and publish the run-level outputs of a v2 pipeline whose graph
 * runner has already reached a terminal state — deciding whether a
 * terminal may be entered is the graph runner's responsibility; this
 * function only materializes the declared run outputs. It accepts no
 * terminal id, no user paths, no types, no mounts and no Docker options:
 * sources, types and destinations come only from the trusted resolved
 * pipeline.
 *
 * Arguments are provenance-gated: `pipeline` must be the exact deep-frozen
 * snapshot a successful `loadPipelineV2` returned, and `runInputs` must be
 * the exact frozen `RunInputsSnapshot` a successful `snapshotRunInputs`
 * call returned for that same pipeline object. The run root and the shared
 * project directory must still be real directories, and the fixed
 * `<runRoot>/outputs` path must be absent (any pre-existing file,
 * directory or symlink fails closed before anything is created, so a
 * repeated call after a successful publication is rejected and never
 * overwrites).
 *
 * The whole accepted history is validated first (exact record shape,
 * declared state/output, per-activation coherence, fixed-location
 * resolution, digest recomputation of every record including old
 * non-winning ones); only then is the winning record per `state`/`output`
 * pair selected by highest activation index. Each declared run output is
 * then resolved in declaration order from either the verified run-input
 * snapshot (`pipeline_input`) or the winner map (`state_output`). A
 * missing `required: true` source fails the whole operation before any
 * publication; a missing `required: false` source is listed as absent and
 * creates no filesystem entry; a present source is published regardless
 * of `required`. `json` sources are re-parsed and re-validated against the
 * loader-compiled schema of the resolved run output (parser diagnostics
 * stay content-free; the original bytes are published, never
 * reserialized).
 *
 * The publication is staged in an exclusive temporary sibling directory
 * inside the canonical run root (directories 0700, files 0600; only real
 * directories and regular files), digest-hashed with the separate
 * `pipeline-v2-run-output` domain while being copied, and published with a
 * single `rename()` after the full staging tree checks out. A failure
 * before the rename removes exactly the staging tree — run inputs,
 * activations, accepted outputs and the shared project are never
 * modified. On success the deep-frozen `RunOutputsSnapshot` is registered
 * in the module-private provenance registry and returned.
 */
export async function collectRunOutputs(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  acceptedOutputs: readonly unknown[],
): Promise<RunOutputsSnapshot> {
  requireResolvedPipelineV2Provenance(pipeline, "run output collection");
  const provenance = runInputSnapshotProvenance.get(runInputs);
  if (provenance === undefined || provenance.pipeline !== pipeline) {
    throw new PipelineError(UNTRUSTED_RUN_INPUT_SNAPSHOT_MESSAGE);
  }
  const runRootCanonical = provenance.runRootCanonical;

  await requireRealDirectory(runRootCanonical, "run root");
  await requireRealDirectory(provenance.projectRootCanonical, "run project root");

  // The publication target is fixed by the runtime, must be absent, and is
  // never overwritten: this also makes a repeated call after a successful
  // publication fail closed.
  const outputsPath = join(runRootCanonical, "outputs");
  await requireAbsent(outputsPath, "run outputs root");

  // The run-owned snapshot must still be intact before anything is
  // resolved or staged; the original user binding paths are never read.
  await verifyRunInputsSnapshot(runInputs, provenance);

  // The whole accepted history is validated before any winner is selected
  // and before anything is staged. There is no current activation bound
  // here: every record must name an activation that actually happened,
  // which is proven by resolving its fixed location below.
  const acceptedByRef = await resolveAcceptedHistory(
    pipeline,
    acceptedOutputs,
    runRootCanonical,
    undefined,
  );

  const stagingPath = join(runRootCanonical, tmpEntryName("run-outputs"));
  let stagingCreated = false;
  try {
    await createRealDirectoryExclusive(stagingPath, "run outputs staging root");
    stagingCreated = true;

    const entries: RunOutputSnapshotEntry[] = [];
    for (const output of pipeline.outputs) {
      const what = `run output ${JSON.stringify(output.id)}`;
      let sourcePath: string;
      let sourceWhat: string;
      if ("pipeline_input" in output.source) {
        let entry: RunInputSnapshotEntry | undefined;
        for (const candidate of runInputs.inputs) {
          if (candidate.id === output.source.pipeline_input) {
            entry = candidate;
            break;
          }
        }
        if (entry === undefined) {
          throw new PipelineError(
            `${what} references pipeline input ${JSON.stringify(output.source.pipeline_input)} which has no run input snapshot entry`,
          );
        }
        if (entry.type !== output.type) {
          throw new PipelineError(
            `${what} expects type ${JSON.stringify(output.type)} but the run input snapshot entry has type ${JSON.stringify(entry.type)}`,
          );
        }
        sourcePath = entry.snapshot_path;
        sourceWhat = `${what} source pipeline input ${JSON.stringify(output.source.pipeline_input)}`;
      } else {
        const accepted = acceptedByRef.get(
          `${output.source.state_output.state}\u0000${output.source.state_output.output}`,
        );
        if (accepted === undefined) {
          if (output.required) {
            throw new PipelineError(
              `${what} references required state output ${JSON.stringify(output.source.state_output.state)}.${JSON.stringify(output.source.state_output.output)} which has no accepted output yet`,
            );
          }
          // Absent optional output: recorded in the snapshot, no
          // filesystem entry, nothing staged for it.
          entries.push({
            id: output.id,
            type: output.type,
            required: false,
            present: false,
          });
          continue;
        }
        if (accepted.type !== output.type) {
          throw new PipelineError(
            `${what} expects type ${JSON.stringify(output.type)} but the accepted state output has type ${JSON.stringify(accepted.type)}`,
          );
        }
        sourcePath = accepted.canonicalPath;
        sourceWhat = `${what} source state output ${JSON.stringify(output.source.state_output.state)}.${JSON.stringify(output.source.state_output.output)}`;
      }

      const target = join(stagingPath, output.id);
      const hasher = runOutputDigestHasher(output.type);
      let parsedJson: unknown;
      if (output.type === "directory") {
        await createRealDirectoryExclusive(target, sourceWhat);
        const tree = await scanDirectoryTree(sourcePath, sourceWhat);
        for (const treeEntry of tree) {
          const entryTarget = join(target, treeEntry.relativePath);
          if (treeEntry.kind === "directory") {
            hashDirectoryEntry(hasher, treeEntry, undefined);
            await createRealDirectoryExclusive(
              entryTarget,
              `${sourceWhat} directory entry ${JSON.stringify(treeEntry.relativePath)}`,
            );
            continue;
          }
          const content = await readRegularFileBytes(
            treeEntry.absolutePath,
            `${sourceWhat} file entry ${JSON.stringify(treeEntry.relativePath)}`,
          );
          hashDirectoryEntry(hasher, treeEntry, content);
          await writeRegularFileExclusive(
            entryTarget,
            content,
            `${sourceWhat} file entry ${JSON.stringify(treeEntry.relativePath)}`,
          );
        }
      } else {
        const content = await readRegularFileBytes(sourcePath, sourceWhat);
        hashBytes(hasher, content);
        await writeRegularFileExclusive(target, content, sourceWhat);
        if (output.type === "json") {
          try {
            parsedJson = JSON.parse(content.toString("utf8"));
          } catch {
            // Stable, content-free diagnostic: the parser message can echo
            // the offending token or an input fragment, so it is never
            // included.
            throw new PipelineError(`${sourceWhat} ${sourcePath} is not valid JSON`);
          }
        }
      }
      if (output.type === "json") {
        if (output.schema === undefined) {
          throw new PipelineError(`${what} has no compiled JSON schema`);
        }
        // The same compiled Draft 2020-12 mechanism that validated the
        // declaring site validates the collected value again here; the
        // value is never modified and the original bytes are published.
        validatePipelineJson(output.schema, parsedJson, sourceWhat);
      }
      entries.push({
        id: output.id,
        type: output.type,
        required: output.required,
        present: true,
        snapshot_path: join(outputsPath, output.id),
        digest: hasher.digest("hex"),
      });
    }

    const snapshot = deepFreeze({
      run_root: runRootCanonical,
      outputs_root: outputsPath,
      outputs: entries,
    });

    // Defensively re-check the target right before the single atomic
    // publish; an adversarially created object at the target still fails
    // closed here (an empty directory could in principle still be replaced
    // by rename — an honest limitation shared with directory snapshots).
    await requireAbsent(outputsPath, "run outputs root");
    try {
      await rename(stagingPath, outputsPath);
    } catch (cause) {
      throw fail(`run outputs root ${outputsPath} could not be published`, cause);
    }

    // Provenance is registered only after the atomic publish succeeded.
    runOutputsSnapshotProvenance.set(snapshot, {
      pipeline,
      runRootCanonical,
      outputsRootCanonical: outputsPath,
    });
    return snapshot;
  } catch (cause) {
    // A failure before the rename removes exactly the staging tree this
    // call created (only when it actually was created); run inputs,
    // activations, accepted outputs and the shared project are untouched.
    if (stagingCreated) {
      await removeTrackedPath(stagingPath, runRootCanonical);
    }
    throw cause;
  }
}

/**
 * Pure host-side data adapter for one declared v2 `decision` state: resolve
 * the state's single `json` input through the existing v2 data plane, parse
 * and validate it, and evaluate the compiled decision model — without
 * creating anything. No decision activation leaf, no `data/inputs` or
 * `data/outputs`, no Session, no container, no helper call, no env and no
 * credentials; nothing on the filesystem is created, modified or removed,
 * so a repeated call with the same inputs returns a structurally identical
 * (deep-frozen) result.
 *
 * Execution order is strict: pipeline provenance, then run-input snapshot
 * provenance for the same pipeline object (both before any field is read),
 * then the `nextActivationIndex` bound, then the safe state id and the
 * `type: "decision"` check, then the canonical run/project root checks, the
 * full `verifyRunInputsSnapshot` of every run input (not only the
 * decision's own input), and the complete accepted-history chain
 * (`resolveAcceptedHistory` — the same single chain `prepareActivationData`
 * and `collectRunOutputs` use) with every record's index required to stay
 * strictly below `nextActivationIndex`. The winner is selected only after
 * the whole history checks out. `nextActivationIndex` is the next unused
 * global activation index of the run; the decision state itself consumes no
 * index and creates no record. Finally the single input port resolves by
 * its declared source — a pipeline input only from the verified snapshot
 * (the original user binding path is never read) or a state output only
 * from the fully verified winner map — and the JSON bytes are read from the
 * fixed orchestrator-owned path through `O_NOFOLLOW`.
 *
 * JSON handling: the parser diagnostic stays content-free (`<what> <path>
 * is not valid JSON` — no parser message, offending token, position or
 * input fragment), and the parsed value is validated against the
 * loader-compiled Draft 2020-12 schema snapshot carried by the input port
 * (value-free diagnostics; the same compiled schema, no second compiler).
 * Malformed JSON and schema failures are `PipelineError`s before the
 * evaluator and never become `invalid_facts`; only a structurally valid,
 * schema-conforming JSON value that does not satisfy the decision model's
 * fact-assignment contract maps to the existing typed `invalid_facts` via
 * `evaluatePipelineDecisionState`. Raw JSON bytes, parsed facts and fact
 * values never appear in results, errors or diagnostics.
 *
 * The function never selects a transition target and never moves a graph
 * cursor: it returns exactly the existing `PipelineDecisionStateResult`,
 * whose outcome is routed to the next state only by the pipeline's
 * transition table. This is substrate only — the production runner does not
 * execute v2 pipelines or decision states yet.
 */
export async function evaluateDecisionStateFromData(
  pipeline: ResolvedPipelineV2,
  runInputs: RunInputsSnapshot,
  acceptedOutputs: readonly unknown[],
  stateId: string,
  nextActivationIndex: number,
): Promise<PipelineDecisionStateResult> {
  // 1. Pipeline provenance: only the exact loadPipelineV2 snapshot.
  requireResolvedPipelineV2Provenance(pipeline, "evaluateDecisionStateFromData");
  // 2. Snapshot provenance for the same pipeline object, before any of its
  //    fields are read.
  const provenance = runInputSnapshotProvenance.get(runInputs);
  if (provenance === undefined || provenance.pipeline !== pipeline) {
    throw new PipelineError(UNTRUSTED_RUN_INPUT_SNAPSHOT_MESSAGE);
  }
  const runRootCanonical = provenance.runRootCanonical;
  const projectRoot = provenance.projectRootCanonical;

  // 3. The next activation index is a plain runner-owned input.
  expectPositiveSafeInteger(nextActivationIndex, "next activation index");

  // 4. The safe state id must name a declared decision state.
  const safeStateId = validateSafeId(stateId, "decision state id");
  const state = findStateById(pipeline, safeStateId);
  if (state === undefined) {
    throw new PipelineError(
      `state ${JSON.stringify(safeStateId)} is not declared by the pipeline`,
    );
  }
  if (state.type !== "decision") {
    throw new PipelineError(
      `state ${JSON.stringify(safeStateId)} is not a decision state; the decision data adapter exists for decision states only`,
    );
  }
  const decisionState: ResolvedV2DecisionState = state;

  // 5. The canonical run root and the shared project root must still be
  // real non-symlink directories.
  await requireRealDirectory(runRootCanonical, "run root");
  await requireRealDirectory(projectRoot, "run project root");

  // 6. The whole run-owned snapshot is re-verified before anything else:
  // not only the decision's own input — every run input must still be
  // intact, and the original user binding paths are never read again.
  await verifyRunInputsSnapshot(runInputs, provenance);

  // 7./8./9. The complete accepted history is validated (parse, coherence,
  // fixed-location resolution, digest verification of every record
  // including old non-winning ones) with every activation index strictly
  // below `nextActivationIndex`; only then is the winner per
  // state/output pair selected by highest index, independent of record
  // order.
  const acceptedByRef = await resolveAcceptedHistory(
    pipeline,
    acceptedOutputs,
    runRootCanonical,
    nextActivationIndex,
  );

  // 10. The single declared input port resolves by its declared source.
  const port = decisionState.inputs[0];
  if (port === undefined) {
    throw new PipelineError(
      `decision state ${JSON.stringify(safeStateId)} declares no input port`,
    );
  }
  const portWhat = `input port ${JSON.stringify(port.id)} of decision state ${JSON.stringify(safeStateId)}`;
  let sourcePath: string;
  let sourceWhat: string;
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
        `${portWhat} references pipeline input ${JSON.stringify(port.source.pipeline_input)} which has no run input snapshot entry`,
      );
    }
    if (entry.type !== port.type) {
      throw new PipelineError(
        `${portWhat} expects type ${JSON.stringify(port.type)} but the run input snapshot entry has type ${JSON.stringify(entry.type)}`,
      );
    }
    sourcePath = entry.snapshot_path;
    sourceWhat = `${portWhat} source pipeline input ${JSON.stringify(port.source.pipeline_input)}`;
  } else {
    const accepted = acceptedByRef.get(
      `${port.source.state_output.state}\u0000${port.source.state_output.output}`,
    );
    if (accepted === undefined) {
      throw new PipelineError(
        `${portWhat} references state output ${JSON.stringify(port.source.state_output.state)}.${JSON.stringify(port.source.state_output.output)} which has no accepted output yet (missing, forward or first-visit self reference)`,
      );
    }
    if (accepted.type !== port.type) {
      throw new PipelineError(
        `${portWhat} expects type ${JSON.stringify(port.type)} but the accepted state output has type ${JSON.stringify(accepted.type)}`,
      );
    }
    sourcePath = accepted.canonicalPath;
    sourceWhat = `${portWhat} source state output ${JSON.stringify(port.source.state_output.state)}.${JSON.stringify(port.source.state_output.output)}`;
  }

  // 11. The value lives at its fixed orchestrator-owned path, verified by
  // the snapshot/history validation above; reading goes through
  // O_NOFOLLOW so a swapped symlink never resolves.
  const content = await readRegularFileBytes(sourcePath, sourceWhat);

  // 12. Content-free parse diagnostic: no parser message, offending token,
  // position or input fragment.
  let parsedJson: unknown;
  try {
    parsedJson = JSON.parse(content.toString("utf8"));
  } catch {
    throw new PipelineError(`${sourceWhat} ${sourcePath} is not valid JSON`);
  }

  // 13. The loader-compiled schema snapshot of this input port is the only
  // contract; no second schema compiler exists.
  if (port.schema === undefined) {
    throw new PipelineError(`${portWhat} has no compiled JSON schema`);
  }
  validatePipelineJson(port.schema, parsedJson, sourceWhat);

  // 14. The existing pure evaluator maps the parsed fact assignment; any
  // earlier failure above never becomes `invalid_facts`. Raw bytes, parsed
  // facts and fact values are not returned or recorded.
  return evaluatePipelineDecisionState(pipeline, safeStateId, parsedJson);
}
