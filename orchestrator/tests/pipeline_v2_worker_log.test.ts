/**
 * The schema v9 worker-log battery: the self-describing artifact, the
 * logs store over the immutable-document substrate, the durable binding
 * union, the publish-then-bind composition, the recovery windows and the
 * read-only verification surfaces.
 */
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  parseWorkerLogArtifact,
  prepareWorkerLogArtifact,
  workerLogBindingFromArtifact,
  workerLogPayloadDigest,
  WORKER_LOG_ARTIFACT_EXTENSION,
  WORKER_LOG_ARTIFACT_KIND,
  WORKER_LOG_ARTIFACT_SCHEMA_VERSION,
  type ParsedWorkerLogArtifact,
} from "../src/pipeline_v2_worker_log.ts";
import {
  PipelineV2WorkerLogStoreError,
  publishPipelineV2WorkerLog,
} from "../src/pipeline_v2_worker_log_store.ts";
import {
  loadWorkerLogWithIo,
  publishWorkerLogWithIo,
  realWorkerLogStoreIo,
  verifyWorkerLogNamespaceWithIo,
  type WorkerLogStoreIo,
} from "../src/pipeline_v2_worker_log_store_internal.ts";
import type { WorkerLogCapture } from "../src/worker_operation.ts";

const RUN_ID = "wl-run-1";

function capture(
  overrides: Partial<WorkerLogCapture> = {},
): WorkerLogCapture {
  return Object.freeze({
    status: "captured",
    byteCount: 11,
    tailBytes: new Uint8Array([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]),
    truncatedDaemon: false,
    truncatedLocal: false,
    ...overrides,
  });
}

interface Fixture {
  root: string;
  runRoot: string;
  logs: string;
}

async function setup(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-worker-log-"));
  const runRoot = join(root, RUN_ID);
  await mkdir(runRoot, { mode: 0o700 });
  return { root, runRoot, logs: join(runRoot, "logs") };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

// --- artifact module -----------------------------------------------------------

test("the canonical envelope round-trips byte-exactly for every content class", () => {
  const payloads: Array<{ name: string; bytes: Uint8Array }> = [
    { name: "ascii", bytes: new TextEncoder().encode("line-1\nline-2\n") },
    {
      name: "nul-ansi-unicode",
      bytes: new TextEncoder().encode("\u001b[31mred\u0000mid-é-moji-\u{1F600}tail"),
    },
    { name: "bounded-tail", bytes: new Uint8Array(8 * 1024 * 1024).fill(0x41) },
  ];
  for (const { name, bytes } of payloads) {
    const prepared = prepareWorkerLogArtifact(3, capture({ byteCount: bytes.byteLength, tailBytes: bytes }));
    expect(prepared).not.toBeNull();
    const parsed = parseWorkerLogArtifact(prepared!.canonical_json, 3);
    expect(parsed.execution_index).toBe(3);
    expect(parsed.status).toBe("captured");
    expect(parsed.byte_count).toBe(bytes.byteLength);
    expect(Buffer.from(parsed.payload).equals(Buffer.from(bytes))).toBe(true);
    expect(parsed.sha256).toBe(prepared!.sha256);
    expect(name.length).toBeGreaterThan(0);
  }
});

test("the envelope is deterministic and the digest domain is exact", () => {
  const first = prepareWorkerLogArtifact(2, capture());
  const second = prepareWorkerLogArtifact(2, capture());
  expect(first!.canonical_json).toBe(second!.canonical_json);
  expect(first!.sha256).toBe(second!.sha256);
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(Buffer.from("pipeline-v2-worker-log\0", "utf8"));
  hasher.update(new Uint8Array([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]));
  expect(first!.sha256).toBe(hasher.digest("hex"));
  // digest sensitivity: one byte moves the digest
  const other = prepareWorkerLogArtifact(2, capture({ tailBytes: new Uint8Array([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33]) }));
  expect(other!.sha256).not.toBe(first!.sha256);
  void workerLogPayloadDigest;
});

test("an unavailable or zero-byte capture produces no artifact", () => {
  expect(prepareWorkerLogArtifact(1, capture({ status: "unavailable", byteCount: 0, tailBytes: new Uint8Array(0) }))).toBeNull();
  expect(prepareWorkerLogArtifact(1, capture({ byteCount: 0, tailBytes: new Uint8Array(0) }))).toBeNull();
});

test("the truncation flags survive the round-trip in both directions", () => {
  for (const [daemon, local] of [
    [true, false],
    [false, true],
    [true, true],
    [false, false],
  ] as const) {
    const prepared = prepareWorkerLogArtifact(1, capture({ truncatedDaemon: daemon, truncatedLocal: local }));
    const parsed = parseWorkerLogArtifact(prepared!.canonical_json, 1);
    expect(parsed.truncated_daemon).toBe(daemon);
    expect(parsed.truncated_local).toBe(local);
    expect(workerLogBindingFromArtifact(parsed)).toEqual({
      status: "captured",
      sha256: parsed.sha256,
      byte_count: parsed.byte_count,
      truncated_daemon: daemon,
      truncated_local: local,
    });
  }
});

test("malformed and foreign envelopes fail closed with content-free diagnostics", () => {
  const good = prepareWorkerLogArtifact(4, capture())!;
  const valid = good.canonical_json;
  const parsed: Record<string, unknown> = JSON.parse(valid);
  const shapes: Array<[string, string]> = [
    ["not json", "{not json"],
    ["array", "[]"],
    ["string", '"x"'],
  ];
  for (const field of [
    "schema_version",
    "kind",
    "execution_index",
    "status",
    "sha256",
    "byte_count",
    "truncated_daemon",
    "truncated_local",
    "payload_base64",
  ]) {
    const clone = { ...parsed };
    delete clone[field];
    shapes.push([`missing ${field}`, JSON.stringify(clone)]);
  }
  for (const field of ["schema_version", "kind", "execution_index", "status", "sha256", "byte_count", "truncated_daemon", "truncated_local", "payload_base64"]) {
    const clone = { ...parsed, [field]: field === "payload_base64" ? "AAAA" : "bogus" };
    shapes.push([`wrong ${field}`, JSON.stringify(clone)]);
  }
  shapes.push(["unknown field", JSON.stringify({ ...parsed, extra: 1 })]);
  shapes.push(["foreign index", JSON.stringify({ ...parsed, execution_index: 5 })]);
  shapes.push(["unavailable status", JSON.stringify({ ...parsed, status: "unavailable" })]);
  shapes.push(["byte count mismatch", JSON.stringify({ ...parsed, byte_count: parsed.byte_count as number + 1 })]);
  shapes.push(["digest mismatch", JSON.stringify({ ...parsed, sha256: "a".repeat(64) })]);
  shapes.push(["invalid base64", JSON.stringify({ ...parsed, payload_base64: "!!!!" })]);
  shapes.push(["empty base64", JSON.stringify({ ...parsed, payload_base64: "" })]);
  for (const [label, raw] of shapes) {
    let message = "";
    try {
      parseWorkerLogArtifact(raw, 4);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).not.toBe("");
    expect(message.includes("hello")).toBe(false);
    expect(label.length).toBeGreaterThan(0);
  }
});

// --- store publication ---------------------------------------------------------

test("the publication creates the 0700 logs directory and the 0600 canonical artifact", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    expect(published).not.toBeNull();
    expect(published!.binding.status).toBe("captured");
    expect(published!.binding.byte_count).toBe(11);
    const logsInfo = await lstat(fixture.logs);
    expect(logsInfo.isDirectory()).toBe(true);
    expect(logsInfo.mode & 0o7777).toBe(0o700);
    const artifactInfo = await lstat(published!.artifact_path);
    expect(artifactInfo.isFile()).toBe(true);
    expect(artifactInfo.mode & 0o7777).toBe(0o600);
    const bytes = await readFile(published!.artifact_path);
    const prepared = prepareWorkerLogArtifact(1, capture())!;
    expect(bytes.equals(Buffer.from(prepared.canonical_json, "utf8"))).toBe(true);
    expect(bytes[bytes.byteLength - 1]).not.toBe(0x0a);
  } finally {
    await dispose(fixture);
  }
});

test("an exact retry adopts byte-exactly without touching inode, mode, mtime or bytes", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    const before = await lstat(first!.artifact_path);
    const bytes = await readFile(first!.artifact_path);
    await Bun.sleep(5);
    const second = await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    expect(second!.artifact_path).toBe(first!.artifact_path);
    const after = await lstat(second!.artifact_path);
    expect(after.ino).toBe(before.ino);
    expect(after.mode).toBe(before.mode);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect((await readFile(second!.artifact_path)).equals(bytes)).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("a different capture at the same path is a typed conflict and the artifact is never rewritten", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    const bytes = await readFile(join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`));
    let failure: unknown = null;
    try {
      await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture({ tailBytes: new Uint8Array([1, 2, 3]), byteCount: 3 }));
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    const storeError = failure as PipelineV2WorkerLogStoreError;
    expect(storeError.outcome).toBe("not_published");
    expect(storeError.reason).toBe("conflict");
    expect((await readFile(join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`))).equals(bytes)).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("an unavailable capture publishes nothing and creates no logs directory", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture({ status: "unavailable", byteCount: 0, tailBytes: new Uint8Array(0) }));
    expect(published).toBeNull();
    let exists = true;
    try {
      await lstat(fixture.logs);
    } catch {
      exists = false;
    }
    expect(exists).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("fault injection: pre-link failures leave no final artifact and clean the owned temp", async () => {
  const fixture = await setup();
  try {
    const failing: WorkerLogStoreIo = {
      ...realWorkerLogStoreIo,
      openTempExclusive: async () => {
        throw Object.assign(new Error("denied"), { code: "EACCES" });
      },
    };
    let failure: unknown = null;
    try {
      await publishWorkerLogWithIo(failing, fixture.runRoot, 1, capture());
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    expect((failure as PipelineV2WorkerLogStoreError).outcome).toBe("not_published");
    const names = await readdir(fixture.logs);
    expect(names.filter((name) => !name.startsWith("."))).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("fault injection: a post-link directory fsync failure keeps the candidate and an exact retry confirms it", async () => {
  const fixture = await setup();
  try {
    let fsyncCalls = 0;
    const failing: WorkerLogStoreIo = {
      ...realWorkerLogStoreIo,
      openDir: async (path: string) => {
        fsyncCalls += 1;
        if (fsyncCalls >= 2) {
          // the first open is the ensured logs directory; the second the
          // post-publication parent fsync
          throw Object.assign(new Error("fsync lost"), { code: "EIO" });
        }
        return realWorkerLogStoreIo.openDir(path);
      },
    };
    let failure: unknown = null;
    try {
      await publishWorkerLogWithIo(failing, fixture.runRoot, 1, capture());
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    const durabilityError = failure as PipelineV2WorkerLogStoreError;
    expect(durabilityError.outcome).toBe("durability_unknown");
    expect(durabilityError.candidate).toBeDefined();
    expect(durabilityError.candidate!.kind).toBe("worker_log");
    expect(durabilityError.candidate!.execution_index).toBe(1);
    // the candidate file is visible with its full canonical bytes
    const candidateBytes = await readFile(durabilityError.candidate!.final_path);
    expect(candidateBytes.equals(Buffer.from(durabilityError.candidate!.canonical_json, "utf8"))).toBe(true);
    // the exact retry confirms the durability without rewriting
    const retry = await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    expect(retry!.binding.sha256).toBe(durabilityError.candidate!.sha256);
    const after = await lstat(retry!.artifact_path);
    expect(after.mode & 0o7777).toBe(0o600);
  } finally {
    await dispose(fixture);
  }
});

test("layout attacks fail closed: a symlinked logs directory and a substituted artifact", async () => {
  const fixture = await setup();
  try {
    const outside = join(fixture.root, "outside");
    await mkdir(outside, { mode: 0o700 });
    await symlink(outside, fixture.logs);
    let failure: unknown = null;
    try {
      await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("invalid_layout");
  } finally {
    await dispose(fixture);
  }
  const fixture2 = await setup();
  try {
    await mkdir(fixture2.logs, { mode: 0o700 });
    await symlink(join(fixture2.root, "elsewhere"), join(fixture2.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`));
    let failure: unknown = null;
    try {
      await publishPipelineV2WorkerLog(fixture2.runRoot, 1, capture());
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("conflict");
  } finally {
    await dispose(fixture2);
  }
});

// --- namespace verification ----------------------------------------------------

test("the namespace walk accepts required artifacts and tolerates only exact orphans", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], []);
    // a tolerated orphan (a null-binding in-flight execution) is accepted
    await publishPipelineV2WorkerLog(fixture.runRoot, 2, capture({ byteCount: 3, tailBytes: new Uint8Array([1, 2, 3]) }));
    await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], [2]);
    // an artifact for an execution that must not have one is a conflict
    await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], []).then(
      () => {
        throw new Error("expected a conflict for the extra artifact");
      },
      (cause) => {
        expect((cause as PipelineV2WorkerLogStoreError).reason).toBe("conflict");
      },
    );
    // a foreign file name is a conflict
    await writeFile(join(fixture.logs, "foreign.txt"), "x", { mode: 0o600 });
    let failure: unknown = null;
    try {
      await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], [2]);
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("conflict");
    await rm(join(fixture.logs, "foreign.txt"));
    // a missing required artifact is a typed failure
    await rm(join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`));
    failure = null;
    try {
      await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], [2]);
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("invalid_layout");
  } finally {
    await dispose(fixture);
  }
});

test("the namespace walk rejects a substituted required artifact and a symlinked directory", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    await rm(join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`));
    await mkdir(join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`), { mode: 0o700 });
    let failure: unknown = null;
    try {
      await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], []);
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("conflict");
  } finally {
    await dispose(fixture);
  }
  const fixture2 = await setup();
  try {
    const outside = join(fixture2.root, "elsewhere");
    await mkdir(outside, { mode: 0o700 });
    await symlink(outside, fixture2.logs);
    let failure: unknown = null;
    try {
      await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture2.runRoot, [], []);
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("conflict");
  } finally {
    await dispose(fixture2);
  }
});

test("the read-only load binds the execution index and rejects damaged stored bytes", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    const loaded = await loadWorkerLogWithIo(realWorkerLogStoreIo, fixture.runRoot, 1);
    expect(loaded).not.toBeNull();
    expect(loaded!.execution_index).toBe(1);
    const absent = await loadWorkerLogWithIo(realWorkerLogStoreIo, fixture.runRoot, 2);
    expect(absent).toBeNull();
    const path = join(fixture.logs, `1.${WORKER_LOG_ARTIFACT_EXTENSION}`);
    const good = await readFile(path);
    await writeFile(path, Buffer.from(good.toString("utf8").replace('"byte_count":11', '"byte_count":12')), { mode: 0o600 });
    let failure: unknown = null;
    try {
      await loadWorkerLogWithIo(realWorkerLogStoreIo, fixture.runRoot, 1);
    } catch (cause) {
      failure = cause;
    }
    // a damaged stored envelope is the artifact module's typed integrity
    // failure (surfaced by the load chain as artifact_modified)
    expect((failure as Error).name).toBe("PipelineV2WorkerLogArtifactError");
  } finally {
    await dispose(fixture);
  }
});

test("the artifact constants and kind are pinned", () => {
  expect(WORKER_LOG_ARTIFACT_SCHEMA_VERSION).toBe(1);
  expect(WORKER_LOG_ARTIFACT_KIND).toBe("pipeline_v2_worker_log");
  expect(WORKER_LOG_ARTIFACT_EXTENSION).toBe("workerlog.json");
});

test("the parsed artifact is deep-frozen except the byte view", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    const loaded = (await loadWorkerLogWithIo(realWorkerLogStoreIo, fixture.runRoot, 1)) as unknown as ParsedWorkerLogArtifact;
    expect(Object.isFrozen(loaded)).toBe(true);
    expect(Object.isFrozen(loaded.payload)).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("the logs directory is created exclusively with identity fixation on an adopted directory", async () => {
  const fixture = await setup();
  try {
    // a pre-existing correct logs directory is adopted without chmod
    await mkdir(fixture.logs, { mode: 0o700 });
    const before = await lstat(fixture.logs);
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture());
    const after = await lstat(fixture.logs);
    expect(after.ino).toBe(before.ino);
    expect(after.mode & 0o7777).toBe(0o700);
  } finally {
    await dispose(fixture);
  }
  const fixture2 = await setup();
  try {
    await mkdir(fixture2.logs, { mode: 0o755 });
    let failure: unknown = null;
    try {
      await publishPipelineV2WorkerLog(fixture2.runRoot, 1, capture());
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineV2WorkerLogStoreError);
    expect((failure as PipelineV2WorkerLogStoreError).reason).toBe("invalid_layout");
    const info = await lstat(fixture2.logs);
    expect(info.mode & 0o7777).toBe(0o755);
  } finally {
    await dispose(fixture2);
  }
});

test("the truncated capture publishes the truncated envelope", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2WorkerLog(
      fixture.runRoot,
      7,
      capture({ status: "truncated", truncatedDaemon: true, truncatedLocal: true }),
    );
    expect(published!.binding.status).toBe("truncated");
    expect(published!.binding.truncated_daemon).toBe(true);
    expect(published!.binding.truncated_local).toBe(true);
    const stored = JSON.parse((await readFile(published!.artifact_path)).toString("utf8")) as Record<string, unknown>;
    expect(stored["status"]).toBe("truncated");
    expect(stored["truncated_daemon"]).toBe(true);
    expect(stored["truncated_local"]).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("no content-free diagnostic ever carries payload bytes", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WorkerLog(fixture.runRoot, 1, capture({ tailBytes: new TextEncoder().encode("CANARY-LOG-BODY"), byteCount: 15 }));
    await writeFile(join(fixture.logs, `2.${WORKER_LOG_ARTIFACT_EXTENSION}`), "x", { mode: 0o600 });
    let message = "";
    try {
      await verifyWorkerLogNamespaceWithIo(realWorkerLogStoreIo, fixture.runRoot, [1], []);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message.includes("CANARY-LOG-BODY")).toBe(false);
    expect(message).not.toBe("");
  } finally {
    await dispose(fixture);
  }
});

// keep the unused import list honest
void chmod;
void symlink;

// --- source-scan pins (§13) --------------------------------------------------

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join as pjoin } from "node:path";

const SRC = pjoin(import.meta.dir, "..", "src");

function sourceOf(name: string): string {
  return readFileSync(pjoin(SRC, name), "utf8");
}

test("one worker-log serializer/parser owner and one digest-domain owner", () => {
  // the envelope builder and parser live in exactly one production module
  const owners = readdirSync(SRC).filter(
    (name) => name.endsWith(".ts") && /function (prepare|parse)WorkerLogArtifact/.test(sourceOf(name)),
  );
  expect(owners).toEqual(["pipeline_v2_worker_log.ts"]);
  const domains = readdirSync(SRC).filter(
    (name) => name.endsWith(".ts") && /WORKER_LOG_DIGEST_DOMAIN\s*=\s*"pipeline-v2-worker-log\\0"/.test(sourceOf(name)),
  );
  expect(domains).toEqual(["pipeline_v2_worker_log.ts"]);
  // the state module never touches the artifact format
  expect(sourceOf("pipeline_v2_state.ts")).not.toContain("payload_base64");
  expect(sourceOf("pipeline_v2_state.ts")).not.toContain("workerlog");
});

test("one publication primitive: the store delegates to the immutable-document substrate", () => {
  const source = sourceOf("pipeline_v2_worker_log_store_internal.ts");
  expect(source).toContain("publishImmutableDocumentFile");
  expect(source).toContain("readStoredImmutableDocument");
  // no second protocol implementation: no direct open/write/fsync/link/rename
  for (const banned of ["openSync", "writeSync", "fsyncSync", " link(", " rename(", "fs.promises", 'from "node:fs"']) {
    expect(source).not.toContain(banned);
  }
});

test("no new commit-history or event kind and no operation id in the state", () => {
  for (const name of readdirSync(SRC).filter((entry) => entry.endsWith(".ts"))) {
    expect(sourceOf(name)).not.toContain("worker_log_recorded");
  }
  const state = sourceOf("pipeline_v2_state.ts");
  expect(state).not.toContain("operation_id");
  expect(state).not.toContain("op_");
  // the events projection stays the verbatim commit-history pass-through
  const events = sourceOf("pipeline_v2_events.ts");
  expect(events).not.toContain("worker_log");
  expect(events).not.toContain("byte_count");
});

test("no artifact paths in HTTP responses, and no log bytes in the non-logs surfaces", () => {
  // the browser UI (v5 viewer) reads only the two versioned logs endpoints
  // and never the durable artifact layout, its filename or its raw envelope
  const ui = readFileSync(pjoin(SRC, "..", "ui", "app.js"), "utf8");
  expect(ui).not.toContain("workerlog");
  expect(ui).not.toContain("payload_base64");
  expect(ui).not.toContain(".workerlog");
  // the routing layer never names the artifact format or its namespace
  const routing = sourceOf(pjoin("control_plane", "routing.ts"));
  expect(routing).not.toContain("workerlog");
  // the events and inspect projections stay free of worker-log metadata
  const events = sourceOf("pipeline_v2_events.ts");
  expect(events).not.toContain("worker_log");
  const state = sourceOf("pipeline_v2_state.ts");
  expect(state).not.toContain("payload_base64");
});

test("no direct directory enumeration outside the substrate IO and no durable registry", () => {
  const store = sourceOf("pipeline_v2_worker_log_store_internal.ts");
  expect(store).toContain("io.readdirNames");
  expect(store).not.toContain("readdir(");
  expect(store).not.toContain("opendir");
  // no module-global mutable registry, no process-local durable map
  expect(store).not.toMatch(/^\s*const\s+\w+\s*=\s*new Map\(/m);
  expect(sourceOf("pipeline_v2_worker_log.ts")).not.toMatch(/^\s*const\s+\w+\s*=\s*new Map\(/m);
});

test("no migration-on-read and no redaction machinery anywhere in the worker-log modules", () => {
  for (const name of ["pipeline_v2_worker_log.ts", "pipeline_v2_worker_log_store.ts", "pipeline_v2_worker_log_store_internal.ts"]) {
    // comments are honest documentation (the no-redaction statement is
    // required there); the scan targets executable machinery only
    const code = sourceOf(name)
      .split("\n")
      .filter((line) => !line.trimStart().startsWith("//") && !line.trimStart().startsWith("*"))
      .join("\n");
    for (const banned of ["migrate", "redact", "scrub", "sanitize"]) {
      expect(code).not.toContain(banned);
    }
  }
  // the exact no-migration contract lives in the state module
  expect(sourceOf("pipeline_v2_state.ts")).toContain("no v8 migration exists");
});
