/**
 * Internal per-call core of the durable control-plane run binding store.
 *
 * This module is explicitly internal: the public facade
 * `control_plane/run_binding_store.ts` always calls the core functions
 * with the single fixed, immutable production IO re-exported from the
 * neutral immutable-document substrate; tests call the same core with
 * their own per-call IO object. The IO is a per-call capability passed
 * through the whole call chain — there is no mutable module-global IO and
 * no installer, so a fault-injected test call can never change the
 * behavior of any parallel production call.
 *
 * Purpose: before the control-plane starts the first `run` for a caller
 * run id, it durably binds that run id to one registry project id. After
 * any restart, every later operation resolves its project, config and
 * pipeline authority exclusively from this binding — never from a
 * caller-supplied project id. The binding is control-plane authority
 * data, not Pipeline V2 run state: it never touches the run root, the
 * durable run state tree or the reducer.
 *
 * Layout (fixed, flat, inside one operator-configured store root):
 *
 *   <store-root>/bindings/<runId>.json
 *
 * `bindings/` is a 0700 real non-symlink directory inside the canonical
 * store root; a binding file is a 0600 regular file whose content is
 * exactly the canonical JSON of the binding document — no trailing
 * newline. The document is a versioned immutable record:
 *
 *   {"project_id":"...","run_id":"...","schema_version":1}
 *
 * (canonical JSON sorts the keys; the schema version is the number 1).
 * It carries no paths, no config, no launcher, no inputs, no timestamps
 * and no credentials. After publication a binding is never updated,
 * rewritten, replaced or deleted; the store exposes no delete or update
 * API. The one read-only enumeration (`listRunBindingsWithIo`) re-verifies
 * the namespace and partitions its entries by the exact binding-file and
 * substrate temp grammars - no other directory discovery exists.
 *
 * Claim semantics: the caller resolves the request and the project
 * through the registry first; the claim happens immediately before the
 * subprocess spawn. An invalid request never reserves a run id; a
 * successful claim survives a later spawn or preflight failure (the same
 * project may retry; a different project is always a conflict). The
 * claim is inter-process atomic through the shared immutable publication
 * protocol — an exclusive candidate file with its durability barrier,
 * the exclusive publication without overwrite, the parent durability
 * barrier and the read-back verification — so a fresh process always
 * converges to one binding without duplicates and without changing
 * already published bytes.
 *
 * Error contract (closed, typed — no message parsing downstream):
 * `invalid_binding_root` (the configured root is not a usable clean
 * absolute real directory), `binding_store_unavailable` (store
 * infrastructure or I/O failures: a vanished or identity-changed root, a
 * defective `bindings/` namespace, unreadable files), `binding_not_found`
 * (load: no binding), `binding_conflict` (the run id is already bound to
 * a different project; the diagnostics never name the existing owner),
 * `binding_invalid` (unsafe identifiers, a malformed, tampered,
 * noncanonical or defective stored binding artifact — symlink, wrong
 * kind, wrong mode, hardlink, oversized), `binding_persist_failed` (the
 * publication could not be completed or its durability could not be
 * confirmed; the final file, once linked, is never rolled back) and
 * `project_unavailable` (the binding references a project that is not in
 * the loaded registry). Diagnostics are content-free: no paths, no
 * document bytes, no owner project ids, no raw system error text — only
 * an operation class, an optional errno code and already-validated safe
 * ids the caller supplied.
 *
 * Honest boundaries: the store root itself is operator-owned and is never
 * created, chmodded or removed here; there is no protection against a
 * trusted host process racing the verify-then-use steps, and there is no
 * crash recovery for a leftover temp file (the next publication uses a
 * fresh exclusive temp name, so a crash between the candidate fsync and
 * the link never blocks a retry). In a simultaneous same-content race
 * the process that loses the `link()` adopts the winner's byte-identical
 * file through the substrate and reports `created` for its own attempt;
 * the durable binding is identical either way and the final file is never
 * rewritten.
 */

import { createHash } from "node:crypto";
import type { Stats } from "node:fs";
import {
  ImmutableDocumentStoreError,
  describeImmutableObject,
  isImmutablePublicationTempName,
  ensureImmutableDirectory,
  immutableDocumentErrnoSuffix,
  inspectStoredDocumentOrNull,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  realImmutableDocumentIo,
  verifyStoredDirectoryComponent,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "../pipeline_v2_immutable_document_store_internal.ts";
import { canonicalJson } from "../canonical_json.ts";
import { isCleanAbsolutePath } from "../clean_path.ts";
import { isPipelineV2SafeId } from "../pipeline_v2_scalar.ts";
import { deepFreezeValue } from "../pipeline_v2_freeze_internal.ts";

/**
 * The closed failure vocabulary of the run binding store. Disjoint from
 * the registry failure vocabulary, the transport reasons and the public
 * run reasons; `project_unavailable` belongs to the authority resolution
 * boundary that composes the store with the loaded registry.
 */
export type ControlPlaneRunBindingFailureReason =
  | "invalid_binding_root"
  | "binding_store_unavailable"
  | "binding_not_found"
  | "binding_conflict"
  | "binding_invalid"
  | "binding_persist_failed"
  | "project_unavailable";

/** The single typed binding error class with an immutable content-free reason. */
export class ControlPlaneRunBindingError extends Error {
  readonly reason!: ControlPlaneRunBindingFailureReason;

  constructor(reason: ControlPlaneRunBindingFailureReason, message: string) {
    super(message);
    this.name = "ControlPlaneRunBindingError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** One durable run authority binding: the run id and the project id it is bound to. */
export interface ControlPlaneRunBinding {
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly projectId: string;
}

/**
 * The claim result: `created` for the process that won the exclusive
 * publication, `existing` when an exactly matching binding (same run id,
 * same project id) was already published — never for a different project,
 * which is the typed `binding_conflict` failure.
 */
export type ControlPlaneRunBindingClaim =
  | { readonly kind: "created"; readonly binding: ControlPlaneRunBinding }
  | { readonly kind: "existing"; readonly binding: ControlPlaneRunBinding };

export type RunBindingStoreIo = ImmutableDocumentIo;

export { realImmutableDocumentIo as realRunBindingStoreIo };

const BINDINGS_DIR_NAME = "bindings";
const BINDING_FILE_SUFFIX = ".json";
const TEMP_PREFIX = ".binding-publish-";
const BINDING_MAX_BYTES = 4096;
const BINDING_DIGEST_DOMAIN = "control-plane-run-binding\0";

const BINDING_WORDING: ImmutableDocumentWording = Object.freeze({
  document: "run binding",
  publicationFailed: "the run binding publication failed",
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalidRoot(message: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError("invalid_binding_root", message);
}

function storeUnavailable(message: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError("binding_store_unavailable", message);
}

function bindingInvalid(message: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError("binding_invalid", message);
}

function bindingNotFound(runId: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError("binding_not_found", `no run binding exists for run "${runId}"`);
}

function bindingConflict(runId: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError(
    "binding_conflict",
    `the run binding for run "${runId}" is already claimed by another project`,
  );
}

function persistFailed(message: string): ControlPlaneRunBindingError {
  return new ControlPlaneRunBindingError("binding_persist_failed", message);
}

/** The canonical JSON of one binding document (the single serializer use). */
function bindingCanonicalJson(runId: string, projectId: string): string {
  return canonicalJson({ schema_version: 1, run_id: runId, project_id: projectId });
}

/** The digest of one binding document over its own separated domain. */
function bindingDigest(canonical: string): string {
  return createHash("sha256").update(BINDING_DIGEST_DOMAIN, "utf8").update(canonical, "utf8").digest("hex");
}

/**
 * The prepared claim of one binding: the validated frozen binding, its
 * exact canonical JSON bytes and its digest. This is the serializer owner
 * of the binding document — every published file carries exactly these
 * bytes and every stored file is accepted only against them.
 */
export interface PreparedRunBinding {
  readonly binding: ControlPlaneRunBinding;
  readonly canonical_json: string;
  readonly sha256: string;
}

export function prepareRunBinding(runId: string, projectId: string): PreparedRunBinding {
  if (!isPipelineV2SafeId(runId) || !isPipelineV2SafeId(projectId)) {
    throw bindingInvalid("the run binding identifiers must be safe identifiers");
  }
  const canonical = bindingCanonicalJson(runId, projectId);
  return deepFreezeValue({
    binding: deepFreezeValue({ schemaVersion: 1 as const, runId, projectId }),
    canonical_json: canonical,
    sha256: bindingDigest(canonical),
  });
}

/**
 * The single parser of a stored binding document: exact own keys,
 * strict schema version, safe identifiers and the requested run id. The
 * canonical byte check stays with the caller (it owns the stored bytes).
 */
export function parseRunBindingDocument(bytes: Buffer, expectedRunId: string): ControlPlaneRunBinding {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw bindingInvalid("the run binding document is not valid JSON");
  }
  if (!isRecord(parsed)) {
    throw bindingInvalid("the run binding document must be an object");
  }
  const keys = Object.keys(parsed);
  if (
    keys.length !== 3 ||
    !Object.hasOwn(parsed, "schema_version") ||
    !Object.hasOwn(parsed, "run_id") ||
    !Object.hasOwn(parsed, "project_id")
  ) {
    throw bindingInvalid("the run binding document must have exactly schema_version, run_id and project_id");
  }
  if (parsed["schema_version"] !== 1) {
    throw bindingInvalid("the run binding document schema version must be 1");
  }
  const runId = parsed["run_id"];
  const projectId = parsed["project_id"];
  if (!isPipelineV2SafeId(runId) || !isPipelineV2SafeId(projectId)) {
    throw bindingInvalid("the run binding document fields must be safe identifiers");
  }
  if (runId !== expectedRunId) {
    throw bindingInvalid("the run binding document names another run");
  }
  return deepFreezeValue({ schemaVersion: 1 as const, runId, projectId });
}

/**
 * The immutable per-store state: the production or injected IO plus the
 * store root fixed at open time (canonical path and `dev`/`ino`
 * identity). Every later operation re-verifies the root against this
 * fixation and fails closed on any divergence.
 */
export interface RunBindingStoreState {
  readonly io: RunBindingStoreIo;
  readonly rootCanonical: string;
  readonly rootDev: number;
  readonly rootIno: number;
}

/**
 * The final typed boundary of the store: typed binding errors pass
 * unchanged; a leaked neutral substrate error is mapped to the store
 * unavailable reason by its own message (the substrate messages are
 * already content-free); any unexpected programmer failure becomes the
 * sanitized fallback. Nothing is ever classified by matching message
 * text.
 */
async function withBindingGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof ControlPlaneRunBindingError) {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw storeUnavailable(cause.message);
    }
    throw storeUnavailable("the control-plane run binding store failed unexpectedly");
  }
}

async function bindStoreRoot(io: RunBindingStoreIo, root: string): Promise<RunBindingStoreState> {
  if (!isCleanAbsolutePath(root)) {
    throw invalidRoot("the binding store root must be a clean absolute path");
  }
  let info: Stats | null;
  try {
    info = await io.lstatOrNull(root);
  } catch (cause) {
    throw storeUnavailable(`the binding store root cannot be inspected${immutableDocumentErrnoSuffix(cause)}`);
  }
  if (info === null) {
    throw invalidRoot("the binding store root does not exist");
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw invalidRoot(`the binding store root exists but is ${describeImmutableObject(info)}`);
  }
  let canonical: string;
  try {
    canonical = await io.realpath(root);
  } catch (cause) {
    throw storeUnavailable(`the binding store root cannot be canonicalized${immutableDocumentErrnoSuffix(cause)}`);
  }
  return deepFreezeValue({ io, rootCanonical: canonical, rootDev: info.dev, rootIno: info.ino });
}

/**
 * Re-verify the store root against its open-time fixation: the object at
 * the canonical path must still be a real non-symlink directory with the
 * same `dev`/`ino`. A vanished root or an identity change fails closed as
 * a store-unavailable failure; a wrong-kind object as an invalid root.
 */
async function verifyStoreRoot(state: RunBindingStoreState): Promise<void> {
  const io = state.io;
  let info: Stats | null;
  try {
    info = await io.lstatOrNull(state.rootCanonical);
  } catch (cause) {
    throw storeUnavailable(`the binding store root cannot be inspected${immutableDocumentErrnoSuffix(cause)}`);
  }
  if (info === null) {
    throw storeUnavailable("the binding store root disappeared");
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw invalidRoot(`the binding store root exists but is ${describeImmutableObject(info)}`);
  }
  if (info.dev !== state.rootDev || info.ino !== state.rootIno) {
    throw storeUnavailable("the binding store root identity changed");
  }
}

/**
 * Ensure the store-owned `bindings/` namespace (claim path): exclusive
 * creation with identity fixation and chmod enforcement through the
 * substrate, adopted without modification when it already exists. Every
 * substrate failure is a store-unavailable failure — the namespace is
 * store infrastructure, not a binding artifact.
 */
async function ensureBindingsDirectory(state: RunBindingStoreState): Promise<string> {
  try {
    return await ensureImmutableDirectory(
      state.io,
      state.rootCanonical,
      BINDINGS_DIR_NAME,
      "bindings directory",
      "binding store root",
    );
  } catch (cause) {
    if (cause instanceof ImmutableDocumentStoreError) {
      throw storeUnavailable(cause.message);
    }
    throw cause;
  }
}

/**
 * Read-only verification of the `bindings/` namespace (load path): absent
 * → `null` (no binding can exist), otherwise a real non-symlink 0700
 * directory. Nothing is created or repaired.
 */
async function verifyBindingsDirectory(state: RunBindingStoreState): Promise<string | null> {
  try {
    return await verifyStoredDirectoryComponent(
      state.io,
      state.rootCanonical,
      BINDINGS_DIR_NAME,
      "bindings directory",
    );
  } catch (cause) {
    if (cause instanceof ImmutableDocumentStoreError) {
      throw storeUnavailable(cause.message);
    }
    throw cause;
  }
}

function bindingLeafPath(bindingsPath: string, runId: string): string {
  return `${bindingsPath}/${runId}${BINDING_FILE_SUFFIX}`;
}

async function inspectBindingLeaf(io: RunBindingStoreIo, leafPath: string): Promise<Stats | null> {
  try {
    return await inspectStoredDocumentOrNull(io, leafPath, "run binding file");
  } catch (cause) {
    if (cause instanceof ImmutableDocumentStoreError) {
      throw storeUnavailable(cause.message);
    }
    throw cause;
  }
}

/**
 * Read one stored binding file through the substrate's no-follow read
 * path. A conflict from the substrate (the object changed kind or mode
 * between inspection and open) is an invalid binding artifact; a vanished
 * file or an I/O failure is a store-unavailable failure.
 */
async function readBindingLeafBytes(io: RunBindingStoreIo, leafPath: string): Promise<Buffer> {
  try {
    return await readStoredImmutableDocument(io, leafPath, "run binding file", BINDING_WORDING);
  } catch (cause) {
    if (cause instanceof ImmutableDocumentStoreError) {
      if (cause.reason === "conflict") {
        throw bindingInvalid(cause.message);
      }
      throw storeUnavailable(cause.message);
    }
    throw cause;
  }
}

/**
 * Load and fully verify one existing binding leaf from its already taken
 * lstat: real non-symlink regular file, mode 0600, size bounded, exact
 * canonical bytes of a valid document naming the requested run. Every
 * defect fails closed as `binding_invalid`; the object is never repaired,
 * replaced or removed.
 *
 * There is deliberately no hardlink-count rejection: the publication
 * protocol itself makes the final inode a transient two-link object
 * between the exclusive publication and the owned temp removal, so a
 * crash in that window must stay loadable and claimable (crash
 * convergence). The integrity defense against an external hardlink is
 * the canonical byte verification itself — any mutation through a second
 * name fails the canonical comparison on every read.
 */
async function loadBindingLeafFromInfo(
  io: RunBindingStoreIo,
  leafPath: string,
  info: Stats,
  runId: string,
): Promise<ControlPlaneRunBinding> {
  if (info.isSymbolicLink() || !info.isFile()) {
    throw bindingInvalid(`the run binding file exists but is ${describeImmutableObject(info)}`);
  }
  if ((info.mode & 0o7777) !== 0o600) {
    throw bindingInvalid("the run binding file does not have the required mode 0600");
  }
  if (info.size > BINDING_MAX_BYTES) {
    throw bindingInvalid("the run binding file exceeds the fixed size limit");
  }
  const bytes = await readBindingLeafBytes(io, leafPath);
  const binding = parseRunBindingDocument(bytes, runId);
  if (!bytes.equals(Buffer.from(bindingCanonicalJson(binding.runId, binding.projectId), "utf8"))) {
    throw bindingInvalid("the run binding file does not carry its own canonical JSON");
  }
  return binding;
}

/**
 * Open one binding store over the given root: the root must be a clean
 * absolute real non-symlink directory; its canonical path and `dev`/`ino`
 * identity are fixed for the store's lifetime. Nothing is created,
 * chmodded or removed.
 */
export async function openRunBindingStoreWithIo(
  io: RunBindingStoreIo,
  root: string,
): Promise<RunBindingStoreState> {
  return await withBindingGuard(() => bindStoreRoot(io, root));
}

/**
 * Claim one run binding for the given run id and project id. The claim is
 * the exclusive publication of the canonical binding document: an exact
 * existing binding is adopted without any write (`existing`), a binding
 * for a different project is the typed conflict, a defective existing
 * object fails closed, and a fresh claim publishes through the neutral
 * immutable publication protocol and verifies the published bytes by
 * read-back.
 */
export async function claimRunBindingWithIo(
  io: RunBindingStoreIo,
  state: RunBindingStoreState,
  options: { readonly runId: string; readonly projectId: string },
): Promise<ControlPlaneRunBindingClaim> {
  return await withBindingGuard(async () => {
    const runId = options.runId;
    const projectId = options.projectId;
    const prepared = prepareRunBinding(runId, projectId);
    await verifyStoreRoot(state);
    const bindingsPath = await ensureBindingsDirectory(state);
    const leafPath = bindingLeafPath(bindingsPath, runId);
    const existingInfo = await inspectBindingLeaf(io, leafPath);
    if (existingInfo !== null) {
      const binding = await loadBindingLeafFromInfo(io, leafPath, existingInfo, runId);
      if (binding.projectId === projectId) {
        return deepFreezeValue({ kind: "existing" as const, binding });
      }
      throw bindingConflict(runId);
    }
    try {
      await publishImmutableDocumentFile(
        io,
        bindingsPath,
        {
          fileName: `${runId}${BINDING_FILE_SUFFIX}`,
          tempPrefix: TEMP_PREFIX,
          tempStem: runId,
          canonicalJson: prepared.canonical_json,
          sha256: prepared.sha256,
          identity: deepFreezeValue({ run_id: runId }),
        },
        BINDING_WORDING,
        "bindings directory",
      );
    } catch (cause) {
      if (cause instanceof ImmutableDocumentStoreError) {
        if (cause.reason === "conflict" && cause.outcome === "not_published") {
          // Lost a publication race: the existing object decides, never
          // this call's intent. Classify the winner's bytes fail-closed.
          const racedInfo = await inspectBindingLeaf(io, leafPath);
          if (racedInfo === null) {
            throw storeUnavailable("the run binding target disappeared while it was being claimed");
          }
          const binding = await loadBindingLeafFromInfo(io, leafPath, racedInfo, runId);
          if (binding.projectId === projectId) {
            return deepFreezeValue({ kind: "existing" as const, binding });
          }
          throw bindingConflict(runId);
        }
        throw persistFailed(cause.message);
      }
      throw cause;
    }
    // Read-back verification of the freshly published file: the bytes at
    // the final path must be exactly the published canonical bytes.
    const publishedBytes = await readBindingLeafBytes(io, leafPath);
    if (!publishedBytes.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
      throw persistFailed("the published run binding file does not carry the published canonical bytes");
    }
    return deepFreezeValue({ kind: "created" as const, binding: prepared.binding });
  });
}

/**
 * Load one run binding strictly read-only: a missing namespace or a
 * missing file is `binding_not_found`; a defective artifact is a typed
 * failure. Nothing is created, repaired, rewritten or removed, and the
 * store never looks at any Pipeline V2 run tree.
 */

/**
 * One binding projection of the authoritative enumeration: only the run
 * id and the project id - never paths, modes, digests or timestamps.
 */
export interface ListedControlPlaneRunBinding {
  readonly runId: string;
  readonly projectId: string;
}

/**
 * The authoritative read-only enumeration of the durable bindings. The
 * store root and the `bindings/` namespace are re-verified against the
 * open-time fixation; the namespace entries are enumerated once with
 * `readdir`, partitioned by the exact binding-file name grammar and the
 * substrate-owned temp-file grammar, and every final binding file is
 * loaded through the existing single leaf-verification path. A name that
 * is neither a final binding nor a protocol temp file is corruption and
 * fails closed as `binding_invalid`; nothing is ever created, repaired,
 * rewritten or removed, no Pipeline V2 run tree is read and no in-memory
 * index is kept. The result is sorted lexicographically by run id and
 * freshly frozen.
 */
export async function listRunBindingsWithIo(
  io: RunBindingStoreIo,
  state: RunBindingStoreState,
): Promise<readonly ListedControlPlaneRunBinding[]> {
  return await withBindingGuard(async () => {
    await verifyStoreRoot(state);
    const bindingsPath = await verifyBindingsDirectory(state);
    if (bindingsPath === null) {
      return deepFreezeValue([] as readonly ListedControlPlaneRunBinding[]);
    }
    let names: string[];
    try {
      names = await io.readdirNames(bindingsPath);
    } catch (cause) {
      throw storeUnavailable(`the bindings directory cannot be enumerated${immutableDocumentErrnoSuffix(cause)}`);
    }
    const bindings: ListedControlPlaneRunBinding[] = [];
    for (const name of names) {
      if (!name.endsWith(BINDING_FILE_SUFFIX)) {
        if (isImmutablePublicationTempName(name, TEMP_PREFIX)) {
          continue;
        }
        throw bindingInvalid("the bindings directory carries an unrecognized entry");
      }
      const runId = name.slice(0, name.length - BINDING_FILE_SUFFIX.length);
      if (!isPipelineV2SafeId(runId)) {
        throw bindingInvalid("the bindings directory carries an entry that is not a run binding file");
      }
      const leafPath = bindingLeafPath(bindingsPath, runId);
      const info = await inspectBindingLeaf(io, leafPath);
      if (info === null) {
        throw bindingInvalid("the bindings directory carries an entry that is not a run binding file");
      }
      const binding = await loadBindingLeafFromInfo(io, leafPath, info, runId);
      bindings.push({ runId: binding.runId, projectId: binding.projectId });
    }
    bindings.sort((first, second) => (first.runId < second.runId ? -1 : first.runId > second.runId ? 1 : 0));
    return deepFreezeValue(bindings as readonly ListedControlPlaneRunBinding[]);
  });
}

export async function loadRunBindingWithIo(
  io: RunBindingStoreIo,
  state: RunBindingStoreState,
  runId: string,
): Promise<ControlPlaneRunBinding> {
  return await withBindingGuard(async () => {
    if (!isPipelineV2SafeId(runId)) {
      throw bindingInvalid("the run binding identifiers must be safe identifiers");
    }
    await verifyStoreRoot(state);
    const bindingsPath = await verifyBindingsDirectory(state);
    if (bindingsPath === null) {
      throw bindingNotFound(runId);
    }
    const leafPath = bindingLeafPath(bindingsPath, runId);
    const info = await inspectBindingLeaf(io, leafPath);
    if (info === null) {
      throw bindingNotFound(runId);
    }
    return await loadBindingLeafFromInfo(io, leafPath, info, runId);
  });
}
