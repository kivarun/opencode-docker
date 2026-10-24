/**
 * Internal core of the project provenance store and of the initial
 * workspace baseline verifier.
 *
 * The store is an adapter over the shared immutable-document substrate
 * (`pipeline_v2_immutable_document_store_internal.ts`): it owns only its
 * fixed layout, its own wording and its own typed error class. It contains
 * no second fsync/link/adoption implementation. The provenance artifact is
 * published as one immutable canonical JSON file at the fixed run-root
 * location
 *
 *   <runRoot>/project-provenance.json
 *
 * — outside the worker-mounted `<runRoot>/project` tree (the worker mounts
 * the run root read-only, so the artifact is unreachable for the worker).
 * The run root itself is a store-adopted parent: verified real non-symlink
 * 0700 directory, never created, chmodded or removed by this module.
 *
 * Publication is atomic and idempotent exactly like every other immutable
 * artifact: exclusive temp file 0600 with recorded ownership identity,
 * full write-all loop, file fsync, close, exclusive `link()` publication
 * (never a replace-capable `rename()`), ownership-checked temp unlink,
 * parent fsync. On `link()` `EEXIST` the existing object must be a real
 * non-symlink regular file mode 0600 carrying exactly the new canonical
 * bytes (equal bytes succeed untouched and re-fsync the parent; anything
 * else is a typed conflict — nothing is ever overwritten or repaired).
 * The closed failure contract follows the substrate:
 * `not_published`/`durability_unknown` × `invalid_layout`/`conflict`/
 * `io_failure` with a frozen candidate on durability-unknown outcomes.
 *
 * The initial workspace baseline verifier proves that a materialized
 * project tree (the fresh `<runRoot>/project` right after preparation, a
 * recovery re-materialization before its rename, or an adoption boundary
 * before the first worker activation) matches the manifest's workspace
 * baseline exactly: entry set, kinds, workspace permission modes,
 * regular-file byte counts and byte digests, and verbatim symlink
 * targets. It is deliberately only run before the first worker activation
 * or during crash recovery before a first activation; after durable
 * evidence of a first activation the workspace is worker-mutable by
 * contract and the manifest stays provenance, not a workspace invariant.
 */

import { constants as fsConstants, type Stats } from "node:fs";
import { lstat, open, readdir, readlink, realpath } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join } from "node:path";
import {
  ImmutableDocumentStoreError,
  immutableDocumentIoFailure,
  inspectStoredDocumentOrNull,
  publishImmutableDocumentFile,
  readStoredImmutableDocument,
  requireImmutableDocumentRunRoot,
  type ImmutableDocumentIo,
  type ImmutableDocumentWording,
} from "./pipeline_v2_immutable_document_store_internal.ts";
import { isErrnoException } from "./fs_checks.ts";
import {
  parsePipelineV2ProjectProvenance,
  PipelineV2ProjectProvenanceError,
  pipelineV2ProjectProvenanceDigest,
  requireProvenanceBindingMatch,
  validatePipelineV2ProjectProvenanceBinding,
  type PreparedPipelineV2ProjectProvenance,
  type PipelineV2ProjectProvenanceBinding,
  type PipelineV2ProjectProvenanceEntry,
} from "./pipeline_v2_project_provenance.ts";

export const PROJECT_PROVENANCE_FILE_NAME = "project-provenance.json";
const PROJECT_PROVENANCE_TEMP_PREFIX = ".project-provenance-";

const WORDING: ImmutableDocumentWording = Object.freeze({
  document: "project provenance manifest",
  publicationFailed: "the project provenance manifest publication failed",
});

/** Typed failure of the provenance store adapter. */
export type PipelineV2ProjectProvenanceStoreFailureReason =
  | "invalid_layout"
  | "conflict"
  | "io_failure";
export type PipelineV2ProjectProvenanceStoreOutcome = "not_published" | "durability_unknown";

export interface PipelineV2ProjectProvenanceStoreCandidate {
  readonly kind: "project_provenance";
  readonly digest: string;
  readonly final_path: string;
  readonly sha256: string;
}

export class PipelineV2ProjectProvenanceStoreError extends Error {
  declare readonly outcome: PipelineV2ProjectProvenanceStoreOutcome;
  declare readonly reason: PipelineV2ProjectProvenanceStoreFailureReason;
  declare readonly candidate?: PipelineV2ProjectProvenanceStoreCandidate;

  constructor(
    outcome: PipelineV2ProjectProvenanceStoreOutcome,
    reason: PipelineV2ProjectProvenanceStoreFailureReason,
    message: string,
    candidate?: PipelineV2ProjectProvenanceStoreCandidate,
  ) {
    super(message);
    this.name = "PipelineV2ProjectProvenanceStoreError";
    Object.defineProperty(this, "outcome", { value: outcome, enumerable: true, writable: false, configurable: false });
    Object.defineProperty(this, "reason", { value: reason, enumerable: true, writable: false, configurable: false });
    if (candidate !== undefined) {
      Object.defineProperty(this, "candidate", { value: candidate, enumerable: true, writable: false, configurable: false });
    }
  }
}

function invalidLayout(message: string): PipelineV2ProjectProvenanceStoreError {
  return new PipelineV2ProjectProvenanceStoreError("not_published", "invalid_layout", message);
}

function conflict(message: string): PipelineV2ProjectProvenanceStoreError {
  return new PipelineV2ProjectProvenanceStoreError("not_published", "conflict", message);
}

interface ProvenanceCandidateIdentity {
  readonly kind?: unknown;
}

/**
 * Map a neutral substrate error into the provenance store class by typed
 * fields only: outcome and reason pass through, and the substrate
 * candidate's frozen identity descriptor becomes the candidate's own
 * fields. Identity is always the descriptor this adapter bound at
 * publication time.
 */
function provenanceStoreErrorFromSubstrate(cause: ImmutableDocumentStoreError): PipelineV2ProjectProvenanceStoreError {
  const substrateCandidate = cause.candidate;
  if (substrateCandidate === undefined) {
    return new PipelineV2ProjectProvenanceStoreError(cause.outcome, cause.reason, cause.message);
  }
  const identity = substrateCandidate.identity as ProvenanceCandidateIdentity;
  if (identity === undefined || identity === null || identity.kind !== "project_provenance") {
    return new PipelineV2ProjectProvenanceStoreError(cause.outcome, cause.reason, cause.message);
  }
  return new PipelineV2ProjectProvenanceStoreError(cause.outcome, cause.reason, cause.message, {
    kind: "project_provenance",
    digest: substrateCandidate.sha256,
    final_path: substrateCandidate.final_path,
    sha256: substrateCandidate.sha256,
  });
}

/**
 * Final typed boundary: only `PipelineV2ProjectProvenanceStoreError` and
 * `PipelineV2ProjectProvenanceError` pass unchanged; neutral substrate
 * errors are re-tagged by typed fields — never by message text; any other
 * failure becomes a sanitized not-published io failure.
 */
async function withProvenanceStoreGuard<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2ProjectProvenanceStoreError) {
      throw cause;
    }
    if (cause instanceof PipelineV2ProjectProvenanceError) {
      throw cause;
    }
    if (cause instanceof ImmutableDocumentStoreError) {
      throw provenanceStoreErrorFromSubstrate(cause);
    }
    throw provenanceStoreErrorFromSubstrate(immutableDocumentIoFailure(WORDING.publicationFailed));
  }
}

/**
 * Publishes one prepared provenance manifest at
 * `<runRoot>/project-provenance.json`. The run root must be an existing
 * absolute canonical real non-symlink directory; it is never created,
 * chmodded or removed. An exact retry (same canonical bytes) adopts the
 * published artifact.
 */
export async function publishProjectProvenanceWithIo(
  io: ImmutableDocumentIo,
  runRoot: string,
  prepared: PreparedPipelineV2ProjectProvenance,
): Promise<string> {
  return await withProvenanceStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    await publishImmutableDocumentFile(
      io,
      runRootCanonical,
      {
        fileName: PROJECT_PROVENANCE_FILE_NAME,
        tempPrefix: PROJECT_PROVENANCE_TEMP_PREFIX,
        tempStem: "publish",
        canonicalJson: prepared.canonical_json,
        sha256: prepared.sha256,
        identity: Object.freeze({ kind: "project_provenance" }),
      },
      WORDING,
      "run root",
    );
    return `${runRootCanonical}/${PROJECT_PROVENANCE_FILE_NAME}`;
  });
}

/**
 * Read-only load of the stored provenance manifest. Returns `null` when
 * the artifact is absent (the caller decides whether absence is legal in
 * its phase). A present artifact must be a real non-symlink regular file
 * mode 0600 whose bytes are exactly the manifest's canonical JSON; the
 * manifest digest is recomputed over the parsed document and must match.
 */
export async function loadProjectProvenanceWithIo(
  io: ImmutableDocumentIo,
  runRoot: string,
): Promise<PreparedPipelineV2ProjectProvenance | null> {
  return await withProvenanceStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const finalPath = `${runRootCanonical}/${PROJECT_PROVENANCE_FILE_NAME}`;
    const artifactInfo = await inspectStoredDocumentOrNull(io, finalPath, "project provenance manifest file");
    if (artifactInfo === null) {
      return null;
    }
    return await loadStoredProvenance(io, finalPath);
  });
}

async function loadStoredProvenance(
  io: ImmutableDocumentIo,
  finalPath: string,
): Promise<PreparedPipelineV2ProjectProvenance> {
  const stored = await readStoredImmutableDocument(io, finalPath, "project provenance manifest file", WORDING);
  const raw = stored.toString("utf8");
  const prepared = parsePipelineV2ProjectProvenance(raw);
  if (!stored.equals(Buffer.from(prepared.canonical_json, "utf8"))) {
    throw conflict("the stored project provenance manifest file does not carry its own canonical JSON");
  }
  const recomputed = pipelineV2ProjectProvenanceDigest(prepared.manifest);
  if (recomputed !== prepared.sha256) {
    throw conflict("the stored project provenance manifest does not match its own digest");
  }
  return prepared;
}

/**
 * Loads the stored manifest and confirms it against the durable state
 * binding (digest, entry count, copy policy version). Used by the restore
 * verifier and the coordinator adoption paths. A missing artifact is a
 * typed integrity refusal, not `null`.
 */
export async function loadVerifiedProjectProvenanceWithIo(
  io: ImmutableDocumentIo,
  runRoot: string,
  binding: PipelineV2ProjectProvenanceBinding,
): Promise<PreparedPipelineV2ProjectProvenance> {
  return await withProvenanceStoreGuard(async () => {
    const runRootCanonical = await requireImmutableDocumentRunRoot(io, runRoot);
    const finalPath = `${runRootCanonical}/${PROJECT_PROVENANCE_FILE_NAME}`;
    const artifactInfo = await inspectStoredDocumentOrNull(io, finalPath, "project provenance manifest file");
    if (artifactInfo === null) {
      throw invalidLayout("the project provenance manifest does not exist");
    }
    const prepared = await loadStoredProvenance(io, finalPath);
    const validatedBinding = validatePipelineV2ProjectProvenanceBinding(binding, "the durable project provenance binding");
    try {
      requireProvenanceBindingMatch(prepared, validatedBinding);
    } catch (cause) {
      if (cause instanceof PipelineV2ProjectProvenanceError) {
        throw conflict("the durable project provenance binding does not match the stored provenance manifest");
      }
      throw cause;
    }
    return prepared;
  });
}

/** The per-call filesystem capability of the workspace baseline verifier. */
export interface ProjectWorkspaceVerifyIo {
  readonly lstatOrNull: (path: string) => Promise<Stats | null>;
  readonly readdirNames: (path: string) => Promise<string[]>;
  readonly readWholeFile: (path: string) => Promise<Buffer>;
  readonly readlink: (path: string) => Promise<string>;
  readonly realpath: (path: string) => Promise<string>;
}

const realProjectWorkspaceVerifyIoImplementation: ProjectWorkspaceVerifyIo = {
  lstatOrNull: async (path) => {
    try {
      return await lstat(path);
    } catch (cause) {
      if (isErrnoException(cause, "ENOENT")) {
        return null;
      }
      throw cause;
    }
  },
  readdirNames: async (path) => await readdir(path),
  readWholeFile: async (path) => {
    const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    try {
      const chunks: Buffer[] = [];
      const chunk = Buffer.allocUnsafe(128 * 1024);
      let position = 0;
      for (;;) {
        const { bytesRead } = await handle.read(chunk, 0, chunk.length, position);
        if (bytesRead === 0) {
          break;
        }
        chunks.push(Buffer.from(chunk.subarray(0, bytesRead)));
        position += bytesRead;
      }
      return Buffer.concat(chunks);
    } finally {
      await handle.close();
    }
  },
  readlink: (path) => readlink(path),
  realpath: (path) => realpath(path),
};

/** The fixed immutable real implementation of the verifier IO. */
export const realProjectWorkspaceVerifyIo: ProjectWorkspaceVerifyIo = Object.freeze(
  realProjectWorkspaceVerifyIoImplementation,
);

function describeKind(info: Stats): string {
  if (info.isSymbolicLink()) {
    return "a symbolic link";
  }
  if (info.isDirectory()) {
    return "a directory";
  }
  if (info.isFile()) {
    return "a regular file";
  }
  return "an unsupported object kind";
}

/**
 * Verifies that the tree at `workspaceRoot` matches the manifest's
 * workspace baseline exactly (entry set, kinds, workspace permission
 * modes, byte counts, byte digests, verbatim symlink targets). Read-only:
 * it never creates, modifies or removes anything. Failures are typed
 * `PipelineV2ProjectProvenanceError("workspace_mismatch")` with
 * content-free diagnostics.
 */
export async function verifyProjectWorkspaceBaselineWithIo(
  io: ProjectWorkspaceVerifyIo,
  workspaceRoot: string,
  prepared: PreparedPipelineV2ProjectProvenance,
): Promise<void> {
  const expected = new Map<string, PipelineV2ProjectProvenanceEntry>();
  for (const entry of prepared.manifest.entries) {
    expected.set(entry.path, entry);
  }
  const found = new Set<string>();
  const walk = async (directory: string, relative: string): Promise<void> => {
    const names = (await io.readdirNames(directory)).sort();
    for (const name of names) {
      const childRelative = relative === "" ? name : `${relative}/${name}`;
      const childPath = join(directory, name);
      const info = await io.lstatOrNull(childPath);
      if (info === null) {
        throw new PipelineV2ProjectProvenanceError(
          "workspace_mismatch",
          `the workspace entry ${JSON.stringify(childRelative)} disappeared during verification`,
        );
      }
      const entry = expected.get(childRelative);
      if (entry === undefined) {
        throw new PipelineV2ProjectProvenanceError(
          "workspace_mismatch",
          `the workspace carries the undeclared entry ${JSON.stringify(childRelative)}`,
        );
      }
      found.add(childRelative);
      if (entry.kind === "directory") {
        if (info.isSymbolicLink() || !info.isDirectory()) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace entry ${JSON.stringify(childRelative)} is ${describeKind(info)}, expected a directory`,
          );
        }
        if ((info.mode & 0o777) !== entry.workspace_mode) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace directory ${JSON.stringify(childRelative)} does not have the baseline workspace mode`,
          );
        }
        await walk(childPath, childRelative);
      } else if (entry.kind === "regular_file") {
        if (info.isSymbolicLink() || !info.isFile()) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace entry ${JSON.stringify(childRelative)} is ${describeKind(info)}, expected a regular file`,
          );
        }
        if ((info.mode & 0o777) !== entry.workspace_mode) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace file ${JSON.stringify(childRelative)} does not have the baseline workspace mode`,
          );
        }
        if (info.size !== entry.byte_count) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace file ${JSON.stringify(childRelative)} does not have the baseline byte count`,
          );
        }
        const bytes = await io.readWholeFile(childPath);
        if (bytes.byteLength !== entry.byte_count) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace file ${JSON.stringify(childRelative)} does not have the baseline byte count`,
          );
        }
        const digest = createHash("sha256").update(bytes).digest("hex");
        if (digest !== entry.sha256) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace file ${JSON.stringify(childRelative)} does not match the baseline content digest`,
          );
        }
      } else {
        if (!info.isSymbolicLink()) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace entry ${JSON.stringify(childRelative)} is ${describeKind(info)}, expected a symbolic link`,
          );
        }
        const target = await io.readlink(childPath);
        if (target !== entry.target) {
          throw new PipelineV2ProjectProvenanceError(
            "workspace_mismatch",
            `the workspace symlink ${JSON.stringify(childRelative)} does not have the baseline target text`,
          );
        }
      }
    }
  };
  const rootInfo = await io.lstatOrNull(workspaceRoot);
  if (rootInfo === null) {
    throw new PipelineV2ProjectProvenanceError("workspace_mismatch", "the workspace root does not exist");
  }
  if (rootInfo.isSymbolicLink() || !rootInfo.isDirectory()) {
    throw new PipelineV2ProjectProvenanceError("workspace_mismatch", "the workspace root is not a real directory");
  }
  await walk(workspaceRoot, "");
  for (const path of expected.keys()) {
    if (!found.has(path)) {
      throw new PipelineV2ProjectProvenanceError(
        "workspace_mismatch",
        `the workspace is missing the baseline entry ${JSON.stringify(path)}`,
      );
    }
  }
  const canonical = await io.realpath(workspaceRoot);
  if (canonical !== workspaceRoot) {
    throw new PipelineV2ProjectProvenanceError("workspace_mismatch", "the workspace root is not canonical");
  }
}
