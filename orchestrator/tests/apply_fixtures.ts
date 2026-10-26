import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { prepareRunProject } from "../src/pipeline_v2_runtime.ts";
import { observeProjectChangesTreesWithIo } from "../src/pipeline_v2_project_changes_internal.ts";
import { realProjectWorkspaceVerifyIo } from "../src/pipeline_v2_project_provenance_store_internal.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { realImmutableDocumentIo, type ImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo, type PipelineStateIo } from "../src/pipeline_state_store.ts";
import { realProjectFileStreamIo, type ProjectFileStreamIo } from "../src/pipeline_v2_project_stream_internal.ts";
import { realMetadataProbeIo, type MetadataProbeIo } from "../src/pipeline_v2_apply_metadata_probe_internal.ts";
import { dlopen, FFIType } from "bun:ffi";
import { realApplyFdOps, type ApplyFdOps } from "../src/pipeline_v2_apply_fd_internal.ts";
import { applyCrashFault } from "../src/pipeline_v2_apply_crash_internal.ts";

/**
 * Shared fixtures for the durable Project Apply preparation suites: a
 * finalized schema-v10 run with a real provenance baseline, an
 * independent operator project, the real observation, and the read-only
 * tree fingerprint used by the zero-write proofs.
 */

export interface ApplyFixtureEntry {
  readonly path: string;
  readonly kind: "file" | "directory" | "symlink";
  readonly content?: string;
  readonly target?: string;
  readonly mode?: number;
}

export async function writeProjectTree(root: string, spec: readonly ApplyFixtureEntry[]): Promise<void> {
  for (const entry of spec) {
    const target = join(root, entry.path);
    if (entry.kind === "directory") {
      await mkdir(target, { recursive: true, mode: entry.mode ?? 0o750 });
      continue;
    }
    if (entry.kind === "symlink") {
      await mkdir(join(target, ".."), { recursive: true });
      await symlink(entry.target ?? "", target);
      continue;
    }
    await mkdir(join(target, ".."), { recursive: true });
    await writeFile(target, entry.content ?? "", { mode: entry.mode ?? 0o644 });
  }
}

export const APPLY_PIPELINE_YAML = `schema_version: 2
entry_state: work
max_transitions: 10

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: work
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task

states:
  - id: work
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

export interface BuiltApplyRun {
  readonly root: string;
  readonly stateRoot: string;
  readonly runId: string;
  readonly runRoot: string;
  readonly workspaceRoot: string;
  readonly operatorProject: string;
  readonly applyStoreRoot: string;
  readonly state: PipelineV2RunState;
  readonly observation: string;
  readonly summary: Awaited<ReturnType<typeof observeProjectChangesTreesWithIo>>["summary"];
  readonly observed: Awaited<ReturnType<typeof observeProjectChangesTreesWithIo>>;
}

export interface BuildApplyRunOptions {
  readonly runId?: string;
  readonly source?: readonly ApplyFixtureEntry[];
  /** Extra source mutations applied before the provenance copy. */
  readonly mutateSource?: (projectSource: string) => Promise<void>;
  readonly worker?: (workspaceRoot: string) => Promise<void>;
  readonly operator?: (operatorProject: string) => Promise<void>;
  readonly finalize?: "success" | "failed";
  /** Stop the reducer right after `create_run` (a nonterminal run). */
  readonly stopAfterCreate?: boolean;
}

const ROOTS: string[] = [];

export async function buildFinalizedApplyRun(prefix: string, options: BuildApplyRunOptions = {}): Promise<BuiltApplyRun> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), APPLY_PIPELINE_YAML);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(join(configRoot, "profiles", "coder.yaml"), "schema_version: 1\nimage: ghcr.io/example/worker:1\nopencode_config: opencode/coder.json\nenv: {}\n");
  await writeFile(join(configRoot, "opencode", "coder.json"), "{}");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource);
  const baseSpec: ApplyFixtureEntry[] = [{ path: "base.txt", kind: "file", content: "BASE" }];
  await writeProjectTree(projectSource, options.source ?? baseSpec);
  if (options.mutateSource !== undefined) {
    await options.mutateSource(projectSource);
  }
  const runId = options.runId ?? "applyrun";
  const stateRoot = join(root, "state");
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { mode: 0o700, recursive: true });
  const pipeline = await loadPipelineV2(bundle);
  const prepared = await prepareRunProject(projectSource, runRoot);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId, now: nextClock });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: [],
    projectProvenance: prepared.binding,
  });
  if (options.stopAfterCreate === true) {
    const state = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
    const operatorProject = join(root, "operator-project");
    await mkdir(operatorProject);
    for (const entry of options.source ?? baseSpec) {
      const to = join(operatorProject, entry.path);
      if (entry.kind === "directory") {
        await mkdir(to, { recursive: true, mode: entry.mode ?? 0o750 });
      } else if (entry.kind === "symlink") {
        await mkdir(join(to, ".."), { recursive: true });
        await symlink(entry.target ?? "", to);
      } else {
        await mkdir(join(to, ".."), { recursive: true });
        await writeFile(to, entry.content ?? "", { mode: entry.mode ?? 0o644 });
      }
    }
    const applyStoreRoot = join(root, "applies");
    await mkdir(applyStoreRoot, { mode: 0o700 });
    return {
      root,
      stateRoot,
      runId,
      runRoot,
      workspaceRoot: join(runRoot, "project"),
      operatorProject,
      applyStoreRoot,
      state,
      observation: "",
      summary: { total: 0, added: 0, deleted: 0, content_modified: 0, mode_modified: 0, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 0, matches_result: 0, diverged: 0 },
      observed: undefined as never,
    };
  }
  await sink.dispatch({ kind: "start_agent_execution", stateId: "work", profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${runId}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${runId}` },
    { kind: "agent_running" },
  ]) {
    await sink.dispatch(command as never);
  }
  await sink.dispatch({ kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: "a".repeat(64) }], workerLog: { status: "unavailable" } } as never);
  await sink.dispatch({ kind: "agent_cleanup_completed" } as never);
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: "work", outcome: "completed", to: "done", transition_index: 0 },
    executionIndex: 1,
  } as never);
  await sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: options.finalize === "failed" ? "failed" : "success" } as never);
  await sink.dispatch({ kind: "run_outputs_published", outputs: [] } as never);
  await sink.dispatch(options.finalize === "failed" ? ({ kind: "run_failed", reason: "terminal_failed" } as never) : ({ kind: "run_succeeded" } as never));
  const workspaceRoot = join(runRoot, "project");
  if (options.worker !== undefined) {
    await options.worker(workspaceRoot);
  }
  const operatorProject = join(root, "operator-project");
  await mkdir(operatorProject);
  const sourceEntries = options.source ?? baseSpec;
  for (const entry of sourceEntries) {
    const from = join(projectSource, entry.path);
    const to = join(operatorProject, entry.path);
    if (entry.kind === "directory") {
      await mkdir(to, { recursive: true, mode: entry.mode ?? 0o750 });
    } else if (entry.kind === "symlink") {
      await mkdir(join(to, ".."), { recursive: true });
      await symlink(entry.target ?? "", to);
    } else {
      await mkdir(join(to, ".."), { recursive: true });
      await writeFile(to, entry.content ?? "", { mode: entry.mode ?? 0o644 });
    }
  }
  if (options.operator !== undefined) {
    await options.operator(operatorProject);
  }
  const state = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
  const full = await observeProjectChangesTreesWithIo(realProjectWorkspaceVerifyIo, runRoot, operatorProject, state);
  const applyStoreRoot = join(root, "applies");
  await mkdir(applyStoreRoot, { mode: 0o700 });
  return {
    root,
    stateRoot,
    runId,
    runRoot,
    workspaceRoot,
    operatorProject,
    applyStoreRoot,
    state,
    observation: full.observation,
    summary: full.summary,
    observed: full,
  };
}

let clockValue = 0;
function nextClock(): Date {
  clockValue += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
}

export async function disposeApplyRoots(): Promise<void> {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
}

/**
 * The read-only zero-write fingerprint: per-path kind, full permission
 * bits, size, content digest (regular files), symlink target, sorted
 * directory entries, and available xattr names. `atime` is deliberately
 * excluded; every mutation the fingerprint detects is a write.
 */
export async function fingerprintTree(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const entries = (await readdir(dir)).sort();
    for (const name of entries) {
      const path = join(dir, name);
      const relative = prefix === "" ? name : `${prefix}/${name}`;
      const info = await lstat(path);
      let xattrs = "";
      try {
        const names = await realMetadataProbeIo.listXattrs(path);
        xattrs = names.length > 0 ? ` xattrs=${JSON.stringify(names.slice().sort())}` : "";
      } catch {
        xattrs = " xattrs=<unavailable>";
      }
      if (info.isDirectory()) {
        lines.push(`${relative}/ dir ${info.mode.toString(8)}${xattrs}`);
        await walk(path, relative);
      } else if (info.isSymbolicLink()) {
        lines.push(`${relative} link -> ${await readFile(path, "utf8").catch(() => "external")}${xattrs}`);
      } else if (info.isFile()) {
        const bytes = await readFile(path);
        lines.push(`${relative} file ${info.mode.toString(8)} ${createHash("sha256").update(bytes).digest("hex")}${xattrs}`);
      } else {
        lines.push(`${relative} special`);
      }
    }
  };
  await walk(root, "");
  return createHash("sha256").update(lines.join("\n")).digest("hex");
}

/** One-shot fault wrapper over the real immutable-document IO. */
export interface FaultDocIo {
  readonly io: ImmutableDocumentIo;
  /** Fault the `nth` occurrence (1-based) of one operation. */
  failOnce: (op: DocFaultOp, nth?: number) => void;
  /** The recorded operation sequence of the wrapped IO. */
  readonly ops: string[];
}

export type DocFaultOp =
  | "mkdirExclusive"
  | "chmod"
  | "openTempExclusive"
  | "write"
  | "sync"
  | "close"
  | "link"
  | "unlink"
  | "dirsync"
  | "dirclose"
  | "readdirNames"
  | "openReadNoFollow";

export function faultDocIo(): FaultDocIo {
  const armed = new Map<DocFaultOp, number>();
  const counts = new Map<DocFaultOp, number>();
  const ops: string[] = [];
  const fault = new Error("injected immutable-document fault");
  const hit = (op: DocFaultOp): boolean => {
    ops.push(op);
    const nth = counts.get(op) ?? 0;
    counts.set(op, nth + 1);
    const target = armed.get(op);
    if (target !== undefined && nth + 1 >= target) {
      armed.delete(op);
      return true;
    }
    return false;
  };
  const baseOpenTemp = realImmutableDocumentIo.openTempExclusive;
  const baseOpenRead = realImmutableDocumentIo.openReadNoFollow;
  const baseOpenDir = realImmutableDocumentIo.openDir;
  const wrapHandle = (handle: Awaited<ReturnType<ImmutableDocumentIo["openTempExclusive"]>>): Awaited<ReturnType<ImmutableDocumentIo["openTempExclusive"]>> => ({
    write: async (chunk: Uint8Array, offset: number, length: number, position: number) => {
      if (hit("write")) {
        throw fault;
      }
      return await handle.write(chunk, offset, length, position);
    },
    read: (chunk: Uint8Array, offset: number, length: number, position: number) => handle.read(chunk, offset, length, position),
    stat: () => handle.stat(),
    sync: async () => {
      if (hit("sync")) {
        throw fault;
      }
      return await handle.sync();
    },
    close: async () => {
      if (hit("close")) {
        throw fault;
      }
      return await handle.close();
    },
  });
  const io: ImmutableDocumentIo = {
    ...realImmutableDocumentIo,
    async mkdirExclusive(path: string) {
      if (hit("mkdirExclusive")) {
        throw fault;
      }
      return await realImmutableDocumentIo.mkdirExclusive(path);
    },
    async chmod(path: string, mode: number) {
      if (hit("chmod")) {
        throw fault;
      }
      return await realImmutableDocumentIo.chmod(path, mode);
    },
    async openTempExclusive(path: string) {
      if (hit("openTempExclusive")) {
        throw fault;
      }
      return wrapHandle(await baseOpenTemp(path));
    },
    async openReadNoFollow(path: string) {
      if (hit("openReadNoFollow")) {
        throw fault;
      }
      return wrapHandle(await baseOpenRead(path));
    },
    async link(existingPath: string, newPath: string) {
      if (hit("link")) {
        throw fault;
      }
      return await realImmutableDocumentIo.link(existingPath, newPath);
    },
    async unlink(path: string) {
      if (hit("unlink")) {
        throw fault;
      }
      return await realImmutableDocumentIo.unlink(path);
    },
    async readdirNames(path: string) {
      if (hit("readdirNames")) {
        throw fault;
      }
      return await realImmutableDocumentIo.readdirNames(path);
    },
    async openDir(path: string) {
      const handle = await baseOpenDir(path);
      return {
        sync: async () => {
          if (hit("dirsync")) {
            throw fault;
          }
          return await handle.sync();
        },
        close: async () => {
          if (hit("dirclose")) {
            throw fault;
          }
          return await handle.close();
        },
      };
    },
  };
  return { io, failOnce: (op, nth = 1) => { armed.set(op, nth); }, ops };
}

/** One-shot fault wrapper over the real streaming observation IO. */
export interface FaultStreamIo {
  readonly io: ProjectFileStreamIo;
  failOnceOpen: (nth?: number) => void;
  /** Fault the `nth` read call of any opened stream as a crash. */
  failReadCrashOnce: (nth?: number) => void;
}

export function faultStreamIo(): FaultStreamIo {
  let armedNth: number | null = null;
  let armedReadNth: number | null = null;
  let opens = 0;
  const io: ProjectFileStreamIo = {
    openNoFollow: async (path: string) => {
      opens += 1;
      if (armedNth !== null && opens >= armedNth) {
        armedNth = null;
        throw Object.assign(new Error("injected stream open failure"), { code: "EACCES" });
      }
      const handle = await realProjectFileStreamIo.openNoFollow(path);
      if (armedReadNth === null) {
        return handle;
      }
      const readTarget = armedReadNth;
      let reads = 0;
      return {
        fstat: () => handle.fstat(),
        read: async (chunk, offset, length, position) => {
          reads += 1;
          if (reads >= readTarget) {
            armedReadNth = null;
            throw applyCrashFault("injected stream read crash");
          }
          return await handle.read(chunk, offset, length, position);
        },
        close: () => handle.close(),
      };
    },
  };
  return {
    io,
    failOnceOpen: (nth = 1) => {
      armedNth = nth;
    },
    failReadCrashOnce: (nth = 1) => {
      armedReadNth = nth;
    },
  };
}

export interface FaultStateIo {
  readonly io: PipelineStateIo;
  failCommitOnce: (step: "open" | "write" | "sync" | "close" | "rename" | "dirfsync" | "dirclose", nth?: number) => void;
}

/** One-shot fault wrapper over the real state IO (apply journal commits). */
export function faultStateIo(): FaultStateIo {
  let armedStep: string | null = null;
  let armedNth = 1;
  let seen = 0;
  const fault = new Error("injected apply state fault");
  const maybeFault = (step: string, path?: string): boolean => {
    if (armedStep !== step) {
      return false;
    }
    if (path !== undefined && !path.endsWith("state.json") && !path.includes("state.json.tmp-")) {
      return false;
    }
    seen += 1;
    if (seen < armedNth) {
      return false;
    }
    armedStep = null;
    return true;
  };
  const io: PipelineStateIo = {
    ...defaultPipelineStateIo,
    async openExclusive(path: string, mode: number) {
      const handle = await defaultPipelineStateIo.openExclusive(path, mode);
      if (maybeFault("open", path)) {
        try { await handle.close(); } catch { /* best effort */ }
        throw fault;
      }
      if (armedStep === null) {
        return handle;
      }
      return {
        chmod: (m: number) => handle.chmod(m),
        writeAll: async (bytes: Uint8Array) => {
          if (maybeFault("write", path)) { throw fault; }
          return await handle.writeAll(bytes);
        },
        sync: async () => {
          if (maybeFault("sync", path)) { throw fault; }
          return await handle.sync();
        },
        close: async () => {
          if (maybeFault("close", path)) {
            try { await handle.close(); } catch { /* best effort */ }
            throw fault;
          }
          return await handle.close();
        },
      };
    },
    async rename(from: string, to: string) {
      if (maybeFault("rename", from)) {
        throw fault;
      }
      return await defaultPipelineStateIo.rename(from, to);
    },
    async openDir(path: string) {
      if (armedStep === "dirfsync" || armedStep === "dirclose") {
        const step = armedStep;
        seen += 1;
        if (seen < armedNth) {
          return await defaultPipelineStateIo.openDir(path);
        }
        armedStep = null;
        if (step === "dirfsync") {
          const handle = await defaultPipelineStateIo.openDir(path);
          return {
            sync: async () => { throw fault; },
            close: () => handle.close(),
          };
        }
        throw fault;
      }
      return await defaultPipelineStateIo.openDir(path);
    },
  };
  return { io, failCommitOnce: (step, nth = 1) => { armedStep = step; armedNth = nth; seen = 0; } };
}

export interface FaultProbeIo {
  readonly io: MetadataProbeIo;
  setLstatOverride: (fn: ((path: string) => Promise<unknown>) | null) => void;
  setXattrOverride: (fn: ((path: string) => Promise<string[]>) | null) => void;
  setAclOverride: (fn: ((path: string, name: string) => Promise<{ present: boolean } | null>) | null) => void;
}

/** Injection seam over the real metadata probe for ownership/ACL fixtures. */
export function faultProbeIo(): FaultProbeIo {
  let lstatOverride: ((path: string) => Promise<unknown>) | null = null;
  let xattrOverride: ((path: string) => Promise<string[]>) | null = null;
  let aclOverride: ((path: string, name: string) => Promise<{ present: boolean } | null>) | null = null;
  const io: MetadataProbeIo = {
    lstat: async (path) => {
      if (lstatOverride !== null) {
        return (await lstatOverride(path)) as Awaited<ReturnType<MetadataProbeIo["lstat"]>>;
      }
      return await realMetadataProbeIo.lstat(path);
    },
    listXattrs: async (path) => {
      if (xattrOverride !== null) {
        return await xattrOverride(path);
      }
      return await realMetadataProbeIo.listXattrs(path);
    },
    readXattr: async (path, name) => {
      if (aclOverride !== null) {
        return await aclOverride(path, name);
      }
      return await realMetadataProbeIo.readXattr(path, name);
    },
  };
  return {
    io,
    setLstatOverride: (fn) => { lstatOverride = fn; },
    setXattrOverride: (fn) => { xattrOverride = fn; },
    setAclOverride: (fn) => { aclOverride = fn; },
  };
}

export interface FaultFdOps {
  readonly ops: ApplyFdOps;
  /** Fault the `nth` occurrence (1-based) of one operation as a crash. */
  failCrashOnce: (op: FdFaultOp, nth?: number, match?: (args: unknown[]) => boolean) => void;
  /** Fault the `nth` occurrence of one operation as a graceful errno. */
  failErrnoOnce: (op: FdFaultOp, nth?: number) => void;
  /** Overrides one operation entirely (deterministic seams). */
  override: (op: FdFaultOp, fn: ((...args: unknown[]) => unknown) | null) => void;
  /** The recorded operation sequence of the wrapped adapter. */
  readonly log: string[];
  readonly counts: ReadonlyMap<string, number>;
}

export type FdFaultOp =
  | "openat"
  | "mkdirat"
  | "unlinkat"
  | "symlinkat"
  | "renameat2"
  | "renameat"
  | "readlinkat"
  | "flistxattr"
  | "fstatvfs"
  | "faccessat"
  | "getdents64"
  | "fstat"
  | "readFd"
  | "writeFd"
  | "fchmod"
  | "fsyncFd"
  | "closeFd";

/** One-shot crash/errno fault injection over the real fd adapter. */
export function faultFdOps(): FaultFdOps {
  const crashArmed = new Map<string, { nth: number; match?: (args: unknown[]) => boolean }>();
  const errnoArmed = new Map<string, number>();
  const overrides = new Map<string, (...args: unknown[]) => unknown>();
  const log: string[] = [];
  const counts = new Map<string, number>();
  const maybeFault = (op: string, args: unknown[]): boolean => {
    log.push(op);
    counts.set(op, (counts.get(op) ?? 0) + 1);
    const crashSpec = crashArmed.get(op);
    if (crashSpec !== undefined && (counts.get(op) ?? 0) >= crashSpec.nth && (crashSpec.match === undefined || crashSpec.match(args) === true)) {
      crashArmed.delete(op);
      throw applyCrashFault(`injected crash fault at ${op}`);
    }
    if (errnoArmed.has(op) && (counts.get(op) ?? 0) >= (errnoArmed.get(op) ?? 0)) {
      errnoArmed.delete(op);
      const cause = new Error(`injected errno fault at ${op}`);
      (cause as unknown as { code: string }).code = "APPLY_INJECTED_ERRNO";
      throw cause;
    }
    return false;
  };
  const wrap = <A extends unknown[], R>(op: FdFaultOp, real: (...args: A) => R) => (...args: A): R => {
    maybeFault(op, args);
    const override = overrides.get(op);
    if (override !== undefined) {
      return override(...args) as R;
    }
    return real(...args);
  };
  const ops_: ApplyFdOps = {
    openat: wrap("openat", (dirfd, name, flags, mode) => realApplyFdOps.openat(dirfd, name, flags, mode)),
    mkdirat: wrap("mkdirat", (dirfd, name, mode) => realApplyFdOps.mkdirat(dirfd, name, mode)),
    unlinkat: wrap("unlinkat", (dirfd, name, flags) => realApplyFdOps.unlinkat(dirfd, name, flags)),
    symlinkat: wrap("symlinkat", (target, dirfd, name) => realApplyFdOps.symlinkat(target, dirfd, name)),
    renameat2: wrap("renameat2", (a, b, c, d, e) => realApplyFdOps.renameat2(a, b, c, d, e)),
    renameat: wrap("renameat", (a, b, c, d) => realApplyFdOps.renameat(a, b, c, d)),
    readlinkat: wrap("readlinkat", (a, b, c, d) => realApplyFdOps.readlinkat(a, b, c, d)),
    flistxattr: wrap("flistxattr", (a, b, c) => realApplyFdOps.flistxattr(a, b, c)),
    fstatvfs: wrap("fstatvfs", (a, b) => realApplyFdOps.fstatvfs(a, b)),
    faccessat: wrap("faccessat", (a, b, c, d) => realApplyFdOps.faccessat(a, b, c, d)),
    getdents64: wrap("getdents64", (a, b, c) => realApplyFdOps.getdents64(a, b, c)),
    fstat: wrap("fstat", (fd) => realApplyFdOps.fstat(fd)),
    readFd: wrap("readFd", (a, b, c, d, e) => realApplyFdOps.readFd(a, b, c, d, e)),
    writeFd: wrap("writeFd", (a, b, c, d, e) => realApplyFdOps.writeFd(a, b, c, d, e)),
    fchmod: wrap("fchmod", (a, b) => realApplyFdOps.fchmod(a, b)),
    fsyncFd: wrap("fsyncFd", (fd) => realApplyFdOps.fsyncFd(fd)),
    closeFd: wrap("closeFd", (fd) => realApplyFdOps.closeFd(fd)),
    errno: () => realApplyFdOps.errno(),
  };
  return {
    ops: ops_,
    failCrashOnce: (op, nth = 1, match) => {
      crashArmed.set(op, { nth, match });
    },
    failErrnoOnce: (op, nth = 1) => {
      errnoArmed.set(op, nth);
    },
    override: (op, fn) => {
      if (fn === null) {
        overrides.delete(op);
      } else {
        overrides.set(op, fn);
      }
    },
    log,
    counts,
  };
}

/** Sets the calling thread's libc errno (test determinism for faulted syscalls). */
export function setLibcErrno(value: number): void {
  const lib = dlopen("libc.so.6", {
    __errno_location: { args: [], returns: FFIType.ptr },
    memcpy: { args: [FFIType.ptr, FFIType.ptr, FFIType.u64], returns: FFIType.ptr },
  });
  const symbols = lib.symbols as unknown as Record<string, (...args: unknown[]) => unknown>;
  const location = symbols["__errno_location"]!();
  const buffer = new Uint8Array(8);
  new DataView(buffer.buffer).setInt32(0, value, true);
  symbols["memcpy"]!(location, buffer, 4);
}

export { applyCrashFault, realApplyFdOps, realImmutableDocumentIo, defaultPipelineStateIo, realProjectFileStreamIo, realMetadataProbeIo as realProbe };
export { chmod, cp, mkdir, rm, symlink, writeFile };
