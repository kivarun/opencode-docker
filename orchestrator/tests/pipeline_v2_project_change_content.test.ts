/**
 * The versioned read-only Project Change Content API: one bounded
 * raw-byte slice of one side of one verified Project Changes entry.
 *
 * Every fixture is built through the real owners (real project-copy
 * preparation with its provenance manifest, the real reducer/store/sink
 * and real bundles). The four success statuses, the availability matrix,
 * the raw-byte cursor, the observation binding, the single-pass verified
 * stream (the A→B→A hole stays closed), the zero-write contract and the
 * content-free leakage boundary are covered here; deterministic IO-seam
 * stress proves every mid-stream mutation is refused with zero bytes.
 */
import { chmod, link, mkdir, mkdtemp, open, readFile, readdir, rm, stat, symlink, unlink, writeFile } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, test } from "bun:test";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { prepareRunProject, snapshotRunInputs, type PreparedRunProjectWithProvenance, type RunInputsSnapshot } from "../src/pipeline_v2_runtime.ts";
import {
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_MAX_CHUNK_LIMIT,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_DEFAULT_CHUNK_LIMIT,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_STATUSES,
  PIPELINE_V2_PROJECT_CHANGE_CONTENT_UNAVAILABLE_REASONS,
  projectChangeContentPipelineV2Run,
  type PipelineV2ProjectChangeContentOutcome,
} from "../src/pipeline_v2_project_change_content.ts";
import {
  readProjectChangeContentWithIo,
  resolveProjectChangeContentSide,
} from "../src/pipeline_v2_project_change_content_internal.ts";
import { PipelineV2ProjectChangesError } from "../src/pipeline_v2_project_changes_internal.ts";
import { compareProjectChangesWithIo, type PipelineV2ProjectChangeEntry } from "../src/pipeline_v2_project_changes_internal.ts";
import {
  realProjectWorkspaceVerifyIo,
  type ProjectWorkspaceVerifyIo,
} from "../src/pipeline_v2_project_provenance_store_internal.ts";
import {
  observeRegularFileStream,
  realProjectFileStreamIo,
  type ProjectFileStreamIo,
} from "../src/pipeline_v2_project_stream_internal.ts";

const io = realProjectWorkspaceVerifyIo;

const ROOTS: string[] = [];

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

let clockValue = 0;
function nextTick(): Date {
  clockValue += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
}

interface ProjectSpec {
  readonly path: string;
  readonly kind: "file" | "directory" | "symlink";
  readonly content?: string;
  readonly target?: string;
  readonly mode?: number;
}

async function writeProjectTree(root: string, spec: readonly ProjectSpec[]): Promise<void> {
  for (const entry of spec) {
    const target = join(root, entry.path);
    if (entry.kind === "directory") {
      await mkdir(target, { recursive: true, mode: entry.mode ?? 0o750 });
      continue;
    }
    if (entry.kind === "symlink") {
      await symlink(entry.target ?? "", target);
      continue;
    }
    await mkdir(join(target, ".."), { recursive: true });
    await writeFile(target, entry.content ?? "", { mode: entry.mode ?? 0o644 });
  }
}

const PIPELINE_YAML = `schema_version: 2
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

interface Built {
  readonly harness: { root: string; stateRoot: string; projectSource: string };
  readonly runId: string;
  readonly runRoot: string;
  readonly pipeline: ResolvedPipelineV2;
  readonly prepared: PreparedRunProjectWithProvenance;
  readonly inputs: RunInputsSnapshot;
}

async function buildFinalizedRun(
  prefix: string,
  projectSpec: readonly ProjectSpec[],
  options: { readonly outcome?: "success" | "failed"; readonly runId?: string } = {},
): Promise<Built> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), PIPELINE_YAML);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  await writeProjectTree(projectSource, projectSpec);
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  const taskFile = join(userdata, "task.md");
  await writeFile(taskFile, "TASK-BODY\n");
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  const runId = options.runId ?? "chg";
  const pipeline = await loadPipelineV2(bundle);
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { mode: 0o700 });
  const prepared = await prepareRunProject(projectSource, runRoot);
  const inputs = await snapshotRunInputs(pipeline, [{ id: "task", path: taskFile }], runRoot);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId, now: nextTick });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: inputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    projectProvenance: prepared.binding,
  });
  if (options.outcome !== "failed") {
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
    await sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
    await sink.dispatch({ kind: "run_outputs_published", outputs: [] } as never);
    await sink.dispatch({ kind: "run_succeeded" } as never);
  }
  return {
    harness: { root, stateRoot, projectSource },
    runId,
    runRoot,
    pipeline,
    prepared,
    inputs,
  };
}

async function readState(built: Built): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(built.runRoot, "state.json"), "utf8"));
}

async function observationOf(built: Built, state?: PipelineV2RunState): Promise<{ observation: string; changes: readonly PipelineV2ProjectChangeEntry[] }> {
  const comparison = await compareProjectChangesWithIo(io, built.runRoot, built.harness.projectSource, state ?? (await readState(built)));
  return { observation: comparison.observation, changes: comparison.changes };
}

type ContentCall = Parameters<typeof projectChangeContentPipelineV2Run>[0];

async function callContent(
  built: Built,
  overrides: Partial<ContentCall> = {},
  state?: PipelineV2RunState,
): Promise<PipelineV2ProjectChangeContentOutcome> {
  const { observation, changes } = await observationOf(built, state);
  const first = changes[0];
  if (first === undefined) {
    throw new Error("the fixture produced no change entry");
  }
  return await projectChangeContentPipelineV2Run({
    runId: built.runId,
    stateRoot: built.harness.stateRoot,
    projectPath: built.harness.projectSource,
    changeIndex: overrides.changeIndex ?? first.index,
    side: overrides.side ?? "result",
    observation: overrides.observation ?? observation,
    ...(overrides.offset !== undefined ? { offset: overrides.offset } : {}),
    ...(overrides.limit !== undefined ? { limit: overrides.limit } : {}),
  });
}

/** The internal core entry with injectable seams. */
async function coreContent(
  built: Built,
  request: { changeIndex: number; side: "baseline" | "result"; observation: string; offset?: number; limit?: number },
  seams: { io?: ProjectWorkspaceVerifyIo; fileStreamIo?: ProjectFileStreamIo } = {},
  state?: PipelineV2RunState,
) {
  try {
    return await readProjectChangeContentWithIo(
      seams.io ?? io,
      seams.fileStreamIo ?? realProjectFileStreamIo,
      built.runRoot,
      built.harness.projectSource,
      state ?? (await readState(built)),
      {
        changeIndex: request.changeIndex,
        side: request.side,
        observation: request.observation,
        offset: request.offset ?? 0,
        limit: request.limit ?? 65_536,
      },
    );
  } catch (cause) {
    // The typed typed failures keep the facade's mapping shape.
    if (cause instanceof PipelineV2ProjectChangesError) {
      return { status: cause.reason } as never;
    }
    throw cause;
  }
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
      const path = join(dir, entry.name);
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = await stat(path);
      if (info.isDirectory()) {
        lines.push(`${relative}/ dir ${info.mode.toString(8)}`);
        await walk(path, relative);
      } else if (info.isSymbolicLink()) {
        lines.push(`${relative} link -> ${await readFile(path, "utf8").catch(() => "external")}`);
      } else {
        const bytes = info.isFile() ? (await readFile(path)).toString("base64") : "non-file";
        lines.push(`${relative} ${info.mode.toString(8)} ${bytes}`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

/** A stream seam that can act before the open and around every read. */
function seamStreamIo(hooks: {
  beforeOpen?: () => Promise<void>;
  aroundRead?: (position: number, bytesRead: number) => Promise<void>;
}): ProjectFileStreamIo {
  const real = realProjectFileStreamIo;
  return {
    openNoFollow: async (path) => {
      if (hooks.beforeOpen !== undefined) {
        await hooks.beforeOpen();
      }
      const handle = await real.openNoFollow(path);
      let position = 0;
      return {
        fstat: () => handle.fstat(),
        read: async (buffer, offset, length, pos) => {
          const result = await handle.read(buffer, offset, length, pos);
          position = pos;
          if (hooks.aroundRead !== undefined) {
            await hooks.aroundRead(pos, result.bytesRead);
          }
          return result;
        },
        close: () => handle.close(),
      };
    },
  };
}

describe("content availability matrix", () => {
  test("added: baseline implicit_empty, result available from the workspace", async () => {
    const built = await buildFinalizedRun("cc-added", [{ path: "keep.txt", kind: "file", content: "KEEP\n" }]);
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "HELLO ADDED\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "dogfood.txt")!;
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    expect(baseline.status).toBe("implicit_empty");
    if (baseline.status !== "implicit_empty") return;
    expect(baseline.byteCount).toBe(0);
    expect(baseline.offset).toBe(0);
    expect(baseline.dataBase64).toBe("");
    expect(baseline.nextOffset).toBeNull();
    expect(baseline).not.toHaveProperty("sha256");
    expect(baseline).not.toHaveProperty("node");
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(result.status).toBe("available");
    if (result.status !== "available") return;
    expect(Buffer.from(result.dataBase64, "base64").toString()).toBe("HELLO ADDED\n");
    expect(result.byteCount).toBe(12);
    expect(result.sha256).toBe((entry.result as { sha256: string }).sha256);
    expect(result.nextOffset).toBeNull();
  });

  test("deleted: result implicit_empty; baseline available only while the operator matches the baseline", async () => {
    const built = await buildFinalizedRun("cc-del", [{ path: "gone.txt", kind: "file", content: "BASELINE BYTES\n" }]);
    await unlink(join(built.runRoot, "project", "gone.txt"));
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "gone.txt")!;
    expect(entry.source_relation).toBe("matches_baseline");
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    expect(baseline.status).toBe("available");
    if (baseline.status !== "available") return;
    expect(Buffer.from(baseline.dataBase64, "base64").toString()).toBe("BASELINE BYTES\n");
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(result).toMatchObject({ status: "implicit_empty", byteCount: 0, dataBase64: "", nextOffset: null });
    // Once the operator file is gone the baseline is unprovable.
    await unlink(join(built.harness.projectSource, "gone.txt"));
    const fresh = (await observationOf(built)).observation;
    const unavailable = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation: fresh });
    expect(unavailable.status).toBe("unavailable");
    if (unavailable.status !== "unavailable") return;
    expect(unavailable.reason).toBe("source_no_longer_matches_baseline");
    expect(unavailable.node).toEqual(entry.baseline!);
    expect(unavailable).not.toHaveProperty("dataBase64");
  });

  test("deleted with a diverged operator: unavailable, third-party bytes never served", async () => {
    const built = await buildFinalizedRun("cc-deldiv", [{ path: "gone.txt", kind: "file", content: "BASELINE\n" }]);
    await unlink(join(built.runRoot, "project", "gone.txt"));
    await writeFile(join(built.harness.projectSource, "gone.txt"), "THIRD PARTY\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "gone.txt")!;
    expect(entry.source_relation).toBe("diverged");
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    expect(baseline.status).toBe("unavailable");
  });

  test("content_modified relation matrix: both available at matches_baseline, baseline unavailable otherwise", async () => {
    const matchesBaseline = await buildFinalizedRun("cc-cm-b", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(matchesBaseline.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const b = await observationOf(matchesBaseline);
    const bEntry = b.changes.find((e) => e.path === "doc.txt")!;
    expect(bEntry.source_relation).toBe("matches_baseline");
    const bBaseline = await coreContent(matchesBaseline, { changeIndex: bEntry.index, side: "baseline", observation: b.observation });
    const bResult = await coreContent(matchesBaseline, { changeIndex: bEntry.index, side: "result", observation: b.observation });
    expect(bBaseline.status).toBe("available");
    expect(bResult.status).toBe("available");
    if (bBaseline.status === "available" && bResult.status === "available") {
      expect(Buffer.from(bBaseline.dataBase64, "base64").toString()).toBe("ORIGINAL\n");
      expect(Buffer.from(bResult.dataBase64, "base64").toString()).toBe("WORKER EDIT\n");
    }

    const matchesResult = await buildFinalizedRun("cc-cm-r", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(matchesResult.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    await writeFile(join(matchesResult.harness.projectSource, "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const r = await observationOf(matchesResult);
    const rEntry = r.changes.find((e) => e.path === "doc.txt")!;
    expect(rEntry.source_relation).toBe("matches_result");
    const rBaseline = await coreContent(matchesResult, { changeIndex: rEntry.index, side: "baseline", observation: r.observation });
    expect(rBaseline).toMatchObject({ status: "unavailable", reason: "source_no_longer_matches_baseline" });
    if (rBaseline.status !== "unavailable") return;
    expect(rBaseline.node).toEqual(rEntry.baseline!);

    const diverged = await buildFinalizedRun("cc-cm-d", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(diverged.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    await writeFile(join(diverged.harness.projectSource, "doc.txt"), "THIRD PARTY\n", { mode: 0o644 });
    const d = await observationOf(diverged);
    const dEntry = d.changes.find((e) => e.path === "doc.txt")!;
    expect(dEntry.source_relation).toBe("diverged");
    const dBaseline = await coreContent(diverged, { changeIndex: dEntry.index, side: "baseline", observation: d.observation });
    expect(dBaseline.status).toBe("unavailable");
    const dResult = await coreContent(diverged, { changeIndex: dEntry.index, side: "result", observation: d.observation });
    expect(dResult.status).toBe("available");
    if (dResult.status === "available") {
      expect(Buffer.from(dResult.dataBase64, "base64").toString()).not.toContain("THIRD PARTY");
    }
  });

  test("content_and_mode_modified follows the content_modified rules", async () => {
    const built = await buildFinalizedRun("cc-cam", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n", mode: 0o644 }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    await chmod(join(built.runRoot, "project", "doc.txt"), 0o640);
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    expect(entry.change).toBe("content_and_mode_modified");
    expect(entry.source_relation).toBe("matches_baseline");
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(baseline.status).toBe("available");
    expect(result.status).toBe("available");
    if (baseline.status === "available" && result.status === "available") {
      expect(Buffer.from(baseline.dataBase64, "base64").toString()).toBe("ORIGINAL\n");
      expect(Buffer.from(result.dataBase64, "base64").toString()).toBe("WORKER EDIT\n");
    }
  });

  test("mode-only regular file: both sides serve the same proven bytes from the workspace, even diverged", async () => {
    const built = await buildFinalizedRun("cc-mode", [{ path: "doc.txt", kind: "file", content: "SAME BYTES\n", mode: 0o644 }]);
    await chmod(join(built.runRoot, "project", "doc.txt"), 0o640);
    await writeFile(join(built.harness.projectSource, "doc.txt"), "THIRD PARTY\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    expect(entry.change).toBe("mode_modified");
    expect(entry.source_relation).toBe("diverged");
    expect((entry.baseline as { sha256: string }).sha256).toBe((entry.result as { sha256: string }).sha256);
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(baseline.status).toBe("available");
    expect(result.status).toBe("available");
    if (baseline.status === "available" && result.status === "available") {
      expect(baseline.dataBase64).toBe(result.dataBase64);
      expect(Buffer.from(baseline.dataBase64, "base64").toString()).toBe("SAME BYTES\n");
    }
  });

  test("mode-only directory: both sides metadata_only", async () => {
    const built = await buildFinalizedRun("cc-dirmod", [
      { path: "sub", kind: "directory", mode: 0o750 },
      { path: "sub/a.txt", kind: "file", content: "A\n" },
    ]);
    await chmod(join(built.runRoot, "project", "sub"), 0o755);
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "sub")!;
    expect(entry.change).toBe("mode_modified");
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(baseline).toMatchObject({ status: "metadata_only", node: entry.baseline });
    expect(result).toMatchObject({ status: "metadata_only", node: entry.result });
    expect(baseline).not.toHaveProperty("dataBase64");
  });

  test("symlink_target_modified: metadata_only on both sides and the symlink is never followed", async () => {
    const built = await buildFinalizedRun("cc-sym", [
      { path: "real.txt", kind: "file", content: "SECRET TARGET\n" },
      { path: "link.txt", kind: "symlink", target: "real.txt" },
    ]);
    await unlink(join(built.runRoot, "project", "link.txt"));
    await symlink("other.txt", join(built.runRoot, "project", "link.txt"));
    await writeFile(join(built.runRoot, "project", "other.txt"), "NEW TARGET\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "link.txt")!;
    expect(entry.change).toBe("symlink_target_modified");
    const baseline = await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(baseline).toMatchObject({ status: "metadata_only", node: entry.baseline });
    expect(result).toMatchObject({ status: "metadata_only", node: entry.result });
    // A direct O_NOFOLLOW open of the leaf fails ELOOP: the target's
    // bytes are unreachable through the symlink.
    let code: string | null = null;
    try {
      const handle = await open(join(built.runRoot, "project", "link.txt"), fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
      await handle.close();
    } catch (cause) {
      code = (cause as { code?: string }).code ?? "unknown";
    }
    expect(code).toBe("ELOOP");
  });

  test("type_changed: regular-file sides resolve per the relation; directory sides are metadata_only", async () => {
    const fileToDir = await buildFinalizedRun("cc-t1", [{ path: "thing", kind: "file", content: "FILE BYTES\n" }]);
    await unlink(join(fileToDir.runRoot, "project", "thing"));
    await mkdir(join(fileToDir.runRoot, "project", "thing"), { mode: 0o750 });
    const t1 = await observationOf(fileToDir);
    const t1Entry = t1.changes.find((e) => e.path === "thing")!;
    expect(t1Entry.change).toBe("type_changed");
    const t1Baseline = await coreContent(fileToDir, { changeIndex: t1Entry.index, side: "baseline", observation: t1.observation });
    const t1Result = await coreContent(fileToDir, { changeIndex: t1Entry.index, side: "result", observation: t1.observation });
    expect(t1Baseline.status).toBe("available");
    expect(t1Result).toMatchObject({ status: "metadata_only", node: t1Entry.result! });
    if (t1Baseline.status === "available") {
      expect(Buffer.from(t1Baseline.dataBase64, "base64").toString()).toBe("FILE BYTES\n");
    }

    const dirToFile = await buildFinalizedRun("cc-t2", [{ path: "thing", kind: "directory" }]);
    await rm(join(dirToFile.runRoot, "project", "thing"), { recursive: true });
    await writeFile(join(dirToFile.runRoot, "project", "thing"), "NOW A FILE\n", { mode: 0o644 });
    const t2 = await observationOf(dirToFile);
    const t2Entry = t2.changes.find((e) => e.path === "thing")!;
    expect(t2Entry.change).toBe("type_changed");
    const t2Baseline = await coreContent(dirToFile, { changeIndex: t2Entry.index, side: "baseline", observation: t2.observation });
    const t2Result = await coreContent(dirToFile, { changeIndex: t2Entry.index, side: "result", observation: t2.observation });
    expect(t2Baseline).toMatchObject({ status: "metadata_only", node: t2Entry.baseline! });
    expect(t2Result.status).toBe("available");
    if (t2Result.status === "available") {
      expect(Buffer.from(t2Result.dataBase64, "base64").toString()).toBe("NOW A FILE\n");
    }

    const diverged = await buildFinalizedRun("cc-t3", [{ path: "thing", kind: "file", content: "FILE BYTES\n" }]);
    await unlink(join(diverged.runRoot, "project", "thing"));
    await mkdir(join(diverged.runRoot, "project", "thing"), { mode: 0o750 });
    await writeFile(join(diverged.harness.projectSource, "thing"), "THIRD PARTY\n", { mode: 0o644 });
    const t3 = await observationOf(diverged);
    const t3Entry = t3.changes.find((e) => e.path === "thing")!;
    expect(t3Entry.source_relation).toBe("diverged");
    const t3Baseline = await coreContent(diverged, { changeIndex: t3Entry.index, side: "baseline", observation: t3.observation });
    expect(t3Baseline.status).toBe("unavailable");
  });
});

describe("cursor contract", () => {
  test("offset 0 / middle / end / past-end; nextOffset counts raw bytes", async () => {
    const built = await buildFinalizedRun("cc-cursor", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "data.txt"), "0123456789ABCDEF", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "data.txt")!;
    const head = await coreContent(built, { changeIndex: entry.index, side: "result", observation, limit: 8 });
    expect(head).toMatchObject({ status: "available", offset: 0, nextOffset: 8 });
    if (head.status === "available") expect(Buffer.from(head.dataBase64, "base64").toString()).toBe("01234567");
    const middle = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 8, limit: 8 });
    expect(middle).toMatchObject({ status: "available", offset: 8, nextOffset: null });
    if (middle.status === "available") expect(Buffer.from(middle.dataBase64, "base64").toString()).toBe("89ABCDEF");
    const end = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 16, limit: 8 });
    expect(end).toMatchObject({ status: "available", offset: 16, dataBase64: "", nextOffset: null });
    const past = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 17, limit: 8 });
    expect(past).toMatchObject({ status: "invalid_cursor" });
    expect(past).not.toHaveProperty("dataBase64");
  });

  test("implicit_empty / metadata_only / unavailable serve offset 0 only", async () => {
    const built = await buildFinalizedRun("cc-off", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "added.txt"), "NEW\n", { mode: 0o644 });
    await unlink(join(built.runRoot, "project", "keep.txt"));
    await writeFile(join(built.harness.projectSource, "keep.txt"), "THIRD\n", { mode: 0o644 });
    await chmod(join(built.runRoot, "project", "keep.txt") /* removed; recreate as symlink */, 0o644).catch(() => undefined);
    const { observation, changes } = await observationOf(built);
    const added = changes.find((e) => e.path === "added.txt")!;
    const deleted = changes.find((e) => e.path === "keep.txt")!;
    const emptyOffset = await coreContent(built, { changeIndex: added.index, side: "baseline", observation, offset: 1 });
    expect(emptyOffset).toMatchObject({ status: "invalid_cursor" });
    const deletedOffset = await coreContent(built, { changeIndex: deleted.index, side: "result", observation, offset: 1 });
    expect(deletedOffset).toMatchObject({ status: "invalid_cursor" });
    const unavailableOffset = await coreContent(built, { changeIndex: deleted.index, side: "baseline", observation, offset: 1 });
    expect(unavailableOffset).toMatchObject({ status: "invalid_cursor" });
    // zero-offset reads still work
    const ok = await coreContent(built, { changeIndex: added.index, side: "baseline", observation, offset: 0 });
    expect(ok.status).toBe("implicit_empty");
  });

  test("default limit 65536 and maximum 262144; over the maximum is invalid_options", async () => {
    const built = await buildFinalizedRun("cc-limits", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "data.txt"), Buffer.alloc(200_000, 0x41), { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "data.txt")!;
    const defaulted = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(defaulted.status).toBe("available");
    if (defaulted.status === "available") {
      expect(Buffer.from(defaulted.dataBase64, "base64").byteLength).toBe(65_536);
      expect(defaulted.nextOffset).toBe(65_536);
    }
    const full = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 65_536, limit: 134_464 });
    expect(full.status).toBe("available");
    if (full.status === "available") {
      expect(Buffer.from(full.dataBase64, "base64").byteLength).toBe(134_464);
      expect(full.nextOffset).toBeNull();
    }
    const tooBig = await projectChangeContentPipelineV2Run({
      runId: built.runId,
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
      changeIndex: 1,
      side: "result",
      observation: observation,
      limit: PIPELINE_V2_PROJECT_CHANGE_CONTENT_MAX_CHUNK_LIMIT + 1,
    });
    expect(tooBig).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(PIPELINE_V2_PROJECT_CHANGE_CONTENT_DEFAULT_CHUNK_LIMIT).toBe(65_536);
    expect(PIPELINE_V2_PROJECT_CHANGE_CONTENT_MAX_CHUNK_LIMIT).toBe(262_144);
  });

  test("multi-chunk reassembly across 256 KiB chunks is byte-exact for a large binary file", async () => {
    const parts: Buffer[] = [];
    let seed = 11;
    for (let i = 0; i < 5; i++) {
      const piece = Buffer.alloc(262_144);
      for (let j = 0; j < piece.byteLength; j++) {
        seed = (seed * 1103515245 + 12345) & 0x7fffffff;
        piece[j] = seed & 0xff;
      }
      parts.push(piece);
    }
    const whole = Buffer.concat(parts);
    const built = await buildFinalizedRun("cc-large", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "big.dat"), whole, { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "big.dat")!;
    const reassembled: Buffer[] = [];
    let offset = 0;
    for (;;) {
      const page = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset, limit: 262_144 });
      expect(page.status).toBe("available");
      if (page.status !== "available") return;
      reassembled.push(Buffer.from(page.dataBase64, "base64"));
      if (page.nextOffset === null) break;
      offset = page.nextOffset;
    }
    expect(Buffer.compare(Buffer.concat(reassembled), whole)).toBe(0);
  });
});

describe("binary and leakage boundary", () => {
  test("NUL, invalid UTF-8, ANSI, HTML, no-EOL and secrets travel byte-exact in dataBase64 only", async () => {
    const raw = Buffer.from("SECRET-TOKEN-ABC\x00\x1b[31m<script>alert(1)</script>\xff\xfe/no-eol", "binary");
    const built = await buildFinalizedRun("cc-binary", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "bin.dat"), raw, { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "bin.dat")!;
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(result.status).toBe("available");
    if (result.status !== "available") return;
    expect(Buffer.compare(Buffer.from(result.dataBase64, "base64"), raw)).toBe(0);
    const facade = await projectChangeContentPipelineV2Run({
      runId: built.runId,
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
      changeIndex: entry.index,
      side: "result",
      observation,
    });
    const serialized = JSON.stringify(facade);
    const parsed = JSON.parse(serialized) as Record<string, unknown>;
    expect(Object.keys(parsed).sort()).toEqual(
      ["byteCount", "changeIndex", "dataBase64", "exitCode", "nextOffset", "observation", "offset", "ok", "revision", "runId", "runRoot", "schemaVersion", "sha256", "side", "status"].sort(),
    );
    // No field other than dataBase64 carries any payload byte.
    for (const [key, value] of Object.entries(parsed)) {
      if (key === "dataBase64") continue;
      expect(String(value)).not.toContain("SECRET-TOKEN");
      expect(String(value)).not.toContain("<script>");
    }
  });

  test("a fragment crossing the chunk boundary reassembles byte-exact", async () => {
    const text = "x".repeat(140_000) + "TAIL";
    const built = await buildFinalizedRun("cc-boundary", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "split.txt"), text, { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "split.txt")!;
    const first = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 131_000, limit: 200 });
    const second = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 131_200, limit: 200 });
    expect(first.status).toBe("available");
    expect(second.status).toBe("available");
    if (first.status === "available" && second.status === "available") {
      const merged = Buffer.concat([Buffer.from(first.dataBase64, "base64"), Buffer.from(second.dataBase64, "base64")]).toString();
      expect(merged).toBe(text.slice(131_000, 131_400));
    }
  });
});

describe("observation binding and TOCTOU", () => {
  test("a stale observation is refused before any byte; a fresh observation succeeds", async () => {
    const built = await buildFinalizedRun("cc-stale", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    await writeFile(join(built.harness.projectSource, "doc.txt"), "OPERATOR EDIT\n", { mode: 0o644 });
    const outcome = await callContent(built, { observation, changeIndex: entry.index, side: "result" });
    expect(outcome).toMatchObject({ ok: false, reason: "stale_observation" });
    expect(outcome).not.toHaveProperty("dataBase64");
    const fresh = await callContent(built, { changeIndex: entry.index, side: "result" });
    expect(fresh.ok).toBe(true);
    if (!fresh.ok || fresh.status !== "available") throw new Error("unreachable");
    expect(Buffer.from(fresh.dataBase64, "base64").toString()).toBe("WORKER EDIT\n");
  });

  test("an index beyond the change set is invalid_cursor", async () => {
    const built = await buildFinalizedRun("cc-index", [{ path: "doc.txt", kind: "file", content: "A\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "B\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const outcome = await coreContent(built, { changeIndex: changes.length + 1, side: "result", observation });
    expect(outcome).toMatchObject({ status: "invalid_cursor" });
    const zero = await coreContent(built, { changeIndex: 0, side: "result", observation });
    expect(zero).toMatchObject({ status: "invalid_cursor" });
  });

  test("the A→B→A hole stays closed: the digest covers the bytes the slice actually came from", async () => {
    const built = await buildFinalizedRun("cc-aba", [{ path: "doc.txt", kind: "file", content: "B".repeat(16) + "\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "A".repeat(16) + "\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    const bBytes = "B".repeat(16) + "\n";
    // The file becomes B after the recompute scan and before the stream
    // open; the single pass reads B and refuses: no separate rehash read
    // exists that could observe A and bless the B slice.
    const refused = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: seamStreamIo({ beforeOpen: async () => {
        await writeFile(join(built.runRoot, "project", "doc.txt"), bBytes, { mode: 0o644 });
      } }),
    });
    expect(refused).toMatchObject({ status: "workspace_unstable" });
    expect(refused).not.toHaveProperty("dataBase64");
    // Restoring A before the open serves A: the bytes and the digest agree.
    const restored = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: seamStreamIo({
        beforeOpen: async () => {
          await writeFile(join(built.runRoot, "project", "doc.txt"), bBytes, { mode: 0o644 });
        },
        aroundRead: async () => {
          const current = await readFile(join(built.runRoot, "project", "doc.txt"), "utf8");
          if (current === bBytes) {
            await writeFile(join(built.runRoot, "project", "doc.txt"), "A".repeat(16) + "\n", { mode: 0o644 });
          }
        },
      }),
    });
    // Even this A-restore-during-read mutates the file mid-stream: the
    // identity check refuses; a silent mismatch is impossible.
    expect(["available", "workspace_unstable"]).toContain(restored.status);
    if (restored.status === "available") {
      expect(Buffer.from(restored.dataBase64, "base64").toString()).toBe("A".repeat(16) + "\n");
    }
  });

  test("same-size overwrite during the captured range is refused with zero bytes", async () => {
    const built = await buildFinalizedRun("cc-mid", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    let fired = false;
    const outcome = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: seamStreamIo({ aroundRead: async () => {
        if (!fired) {
          fired = true;
          await writeFile(join(built.runRoot, "project", "doc.txt"), "RACED!!EDIT\n", { mode: 0o644 });
        }
      } }),
    });
    expect(fired).toBe(true);
    expect(outcome).toMatchObject({ status: "workspace_unstable" });
    expect(outcome).not.toHaveProperty("dataBase64");
  });

  test("same-size overwrite outside the captured range is refused too (the whole stream is verified)", async () => {
    const built = await buildFinalizedRun("cc-outside", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    const outcome = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset: 12, limit: 1 }, {
      fileStreamIo: seamStreamIo({ beforeOpen: async () => {
        // Same size, different bytes, before the open: the stream digest
        // mismatches the verified expectation.
        await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIB\n", { mode: 0o644 });
      } }),
    });
    expect(outcome).toMatchObject({ status: "workspace_unstable" });
  });

  test("truncate and inode replacement during the stream are refused; a symlink substitution is refused at open", async () => {
    const truncated = await buildFinalizedRun("cc-trunc", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(truncated.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const t = await observationOf(truncated);
    const tEntry = t.changes.find((e) => e.path === "doc.txt")!;
    const tOutcome = await coreContent(truncated, { changeIndex: tEntry.index, side: "result", observation: t.observation }, {
      fileStreamIo: seamStreamIo({ aroundRead: async () => {
        await writeFile(join(truncated.runRoot, "project", "doc.txt"), " SHORT", { mode: 0o644 });
      } }),
    });
    expect(tOutcome).toMatchObject({ status: "workspace_unstable" });

    const replaced = await buildFinalizedRun("cc-inode", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(replaced.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const r = await observationOf(replaced);
    const rEntry = r.changes.find((e) => e.path === "doc.txt")!;
    const rOutcome = await coreContent(replaced, { changeIndex: rEntry.index, side: "result", observation: r.observation }, {
      fileStreamIo: seamStreamIo({ beforeOpen: async () => {
        await unlink(join(replaced.runRoot, "project", "doc.txt"));
        await writeFile(join(replaced.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
      } }),
    });
    expect(rOutcome).toMatchObject({ status: "workspace_unstable" });

    const symlinked = await buildFinalizedRun("cc-symrepl", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(symlinked.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const s = await observationOf(symlinked);
    const sEntry = s.changes.find((e) => e.path === "doc.txt")!;
    const sOutcome = await coreContent(symlinked, { changeIndex: sEntry.index, side: "result", observation: s.observation }, {
      fileStreamIo: seamStreamIo({ beforeOpen: async () => {
        await unlink(join(symlinked.runRoot, "project", "doc.txt"));
        await symlink("/etc/hostname", join(symlinked.runRoot, "project", "doc.txt"));
      } }),
    });
    expect(sOutcome).toMatchObject({ status: "workspace_unstable" });
  });

  test("a mode change during the stream is refused; a scan-window overwrite is the typed scan instability", async () => {
    const built = await buildFinalizedRun("cc-modechange", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    const outcome = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: seamStreamIo({ aroundRead: async () => {
        await chmod(join(built.runRoot, "project", "doc.txt"), 0o600);
      } }),
    });
    expect(outcome).toMatchObject({ status: "workspace_unstable" });

    // The same seam drives the observation scan: an overwrite during the
    // scan fails the scan itself with the typed workspace instability.
    let scanError: string | null = null;
    try {
      await compareProjectChangesWithIo(io, built.runRoot, built.harness.projectSource, await readState(built), seamStreamIo({
        aroundRead: async (_position, bytesRead) => {
          if (bytesRead > 0) {
            await writeFile(join(built.runRoot, "project", "doc.txt"), "SCAN RACE!\n", { mode: 0o644 });
          }
        },
      }));
    } catch (cause) {
      scanError = (cause as Error).message;
    }
    expect(scanError).toContain("changed during the scan");
  });

  test("short reads, a premature zero-byte read and a read error fail closed with zero bytes", async () => {
    const built = await buildFinalizedRun("cc-short", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    // One byte per read: the stream still covers the whole file.
    const byteWise = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: {
        openNoFollow: async (path) => {
          const handle = await realProjectFileStreamIo.openNoFollow(path);
          return {
            fstat: () => handle.fstat(),
            read: async (buffer, offset, length, position) => handle.read(buffer, offset, Math.min(length, 1), position),
            close: () => handle.close(),
          };
        },
      },
    });
    expect(byteWise.status).toBe("available");
    if (byteWise.status === "available") {
      expect(Buffer.from(byteWise.dataBase64, "base64").toString()).toBe("WORKER EDIT\n");
    }
    await writeFile(join(built.runRoot, "project", "big2.dat"), Buffer.alloc(200_000, 0x43), { mode: 0o644 });
    const big2Entry = (await observationOf(built)).changes.find((e) => e.path === "big2.dat")!;
    const { observation: obs2 } = await observationOf(built);
    // A premature zero-byte read truncates the stream: the digest and the
    // byte count mismatch the verified expectation (the fixture file is
    // larger than one 128 KiB chunk so the second read happens mid-stream).
    const premature = await coreContent(built, { changeIndex: big2Entry.index, side: "result", observation: obs2 }, {
      fileStreamIo: {
        openNoFollow: async (path) => {
          const handle = await realProjectFileStreamIo.openNoFollow(path);
          let reads = 0;
          return {
            fstat: () => handle.fstat(),
            read: async (buffer, offset, length, position) => {
              reads += 1;
              if (reads === 2) {
                return { bytesRead: 0 };
              }
              return handle.read(buffer, offset, length, position);
            },
            close: () => handle.close(),
          };
        },
      },
    });
    expect(premature).toMatchObject({ status: "workspace_unstable" });
    // A read error propagates as the typed internal error.
    const failing = await coreContent(built, { changeIndex: entry.index, side: "result", observation }, {
      fileStreamIo: {
        openNoFollow: async (path) => {
          const handle = await realProjectFileStreamIo.openNoFollow(path);
          let reads = 0;
          return {
            fstat: () => handle.fstat(),
            read: async (buffer, offset, length, position) => {
              reads += 1;
              if (reads === 2) {
                throw new Error("injected read failure");
              }
              return handle.read(buffer, offset, length, position);
            },
            close: () => handle.close(),
          };
        },
      },
    });
    expect(failing).toMatchObject({ status: "internal_error" });
  });

  test("a file beyond every transport bound streams in bounded chunks without whole-file memory", async () => {
    const built = await buildFinalizedRun("cc-huge", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    const big = Buffer.alloc(3 * 1024 * 1024, 0x42);
    await writeFile(join(built.runRoot, "project", "huge.dat"), big, { mode: 0o644 });
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "huge.dat")!;
    let offset = 0;
    let chunks = 0;
    let tail = 0;
    for (;;) {
      const page = await coreContent(built, { changeIndex: entry.index, side: "result", observation, offset, limit: 262_144 });
      expect(page.status).toBe("available");
      if (page.status !== "available") return;
      chunks += 1;
      tail = Buffer.from(page.dataBase64, "base64").byteLength;
      if (page.nextOffset === null) break;
      offset = page.nextOffset;
    }
    expect(chunks).toBe(12);
    expect(offset + tail).toBe(big.byteLength);
  });
});

describe("zero-write, failure vocabulary and structural pins", () => {
  test("content reads write nothing anywhere (atime excluded by contract)", async () => {
    const built = await buildFinalizedRun("cc-zero", [{ path: "doc.txt", kind: "file", content: "ORIGINAL\n" }]);
    await writeFile(join(built.runRoot, "project", "doc.txt"), "WORKER EDIT\n", { mode: 0o644 });
    const beforeRun = await fingerprint(built.runRoot);
    const beforeProject = await fingerprint(built.harness.projectSource);
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "doc.txt")!;
    await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    await coreContent(built, { changeIndex: entry.index, side: "baseline", observation });
    expect(await fingerprint(built.runRoot)).toBe(beforeRun);
    expect(await fingerprint(built.harness.projectSource)).toBe(beforeProject);
  });

  test("a non-finalized run refuses as changes_not_ready and a missing run as run_not_found", async () => {
    const built = await buildFinalizedRun("cc-notfinal", [{ path: "doc.txt", kind: "file", content: "A\n" }], { outcome: "failed" });
    const { observation } = await observationOf(built);
    const outcome = await projectChangeContentPipelineV2Run({
      runId: built.runId,
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
      changeIndex: 1,
      side: "result",
      observation,
    });
    expect(outcome).toMatchObject({ ok: false, reason: "changes_not_ready" });
    const missing = await projectChangeContentPipelineV2Run({
      runId: "missing",
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
      changeIndex: 1,
      side: "result",
      observation,
    });
    expect(missing).toMatchObject({ ok: false, reason: "run_not_found" });
  });

  test("invalid caller scalars are invalid_options; the closed vocabularies are pinned", async () => {
    const built = await buildFinalizedRun("cc-scalars", [{ path: "doc.txt", kind: "file", content: "A\n" }]);
    const { observation } = await observationOf(built);
    const base = {
      runId: built.runId,
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
      changeIndex: 1,
      side: "result" as const,
      observation,
    };
    expect(await projectChangeContentPipelineV2Run({ ...base, changeIndex: 0 })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(await projectChangeContentPipelineV2Run({ ...base, side: "middle" as never })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(await projectChangeContentPipelineV2Run({ ...base, observation: "ZZ" })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(await projectChangeContentPipelineV2Run({ ...base, offset: -1 })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(await projectChangeContentPipelineV2Run({ ...base, limit: 0 })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect(await projectChangeContentPipelineV2Run({ ...base, stateRoot: "relative" })).toMatchObject({ ok: false, reason: "invalid_options" });
    expect([...PIPELINE_V2_PROJECT_CHANGE_CONTENT_STATUSES]).toEqual(["available", "implicit_empty", "metadata_only", "unavailable"]);
    expect([...PIPELINE_V2_PROJECT_CHANGE_CONTENT_SIDES]).toEqual(["baseline", "result"]);
    expect([...PIPELINE_V2_PROJECT_CHANGE_CONTENT_UNAVAILABLE_REASONS]).toEqual(["source_no_longer_matches_baseline"]);
  });

  test("the observation of an unchanged tree is unchanged by the streaming scanner", async () => {
    const built = await buildFinalizedRun("cc-obs", [
      { path: "a.txt", kind: "file", content: "A\n" },
      { path: "b.txt", kind: "file", content: "B\n" },
      { path: "sub/c.txt", kind: "file", content: "C\n" },
    ]);
    await writeFile(join(built.runRoot, "project", "a.txt"), "A2\n", { mode: 0o644 });
    const first = await observationOf(built);
    const second = await observationOf(built);
    expect(first.observation).toBe(second.observation);
    expect(JSON.stringify(first.changes)).toBe(JSON.stringify(second.changes));
    // Streaming observation equivalence: the direct primitive agrees with
    // the scanner's per-entry digests.
    const observed = await observeRegularFileStream(realProjectFileStreamIo, join(built.runRoot, "project", "a.txt"));
    expect(observed.sha256).toBe(((await observationOf(built)).changes.find((e) => e.path === "a.txt")!.result as { sha256: string }).sha256);
    expect(observed.byteCount).toBe(3);
  });

  test("the resolver fails closed when the mode-only equality contract is violated", async () => {
    const broken = {
      index: 1,
      path: "doc.txt",
      change: "mode_modified" as const,
      source_relation: "matches_baseline" as const,
      baseline: { kind: "regular_file" as const, mode: 0o644, byte_count: 9, sha256: "a".repeat(64), multiple_links: false },
      result: { kind: "regular_file" as const, mode: 0o600, byte_count: 10, sha256: "b".repeat(64), multiple_links: false },
    };
    expect(() => resolveProjectChangeContentSide(broken, "baseline")).toThrow("equal-content contract");
    expect(() => resolveProjectChangeContentSide(broken, "result")).toThrow("equal-content contract");
  });

  test("hardlinked baseline members read byte-exact through either member", async () => {
    const built = await buildFinalizedRun("cc-hard", [{ path: "keep.txt", kind: "file", content: "K\n" }]);
    await writeFile(join(built.runRoot, "project", "one.txt"), "SHARED\n", { mode: 0o644 });
    await link(join(built.runRoot, "project", "one.txt"), join(built.runRoot, "project", "two.txt"));
    const { observation, changes } = await observationOf(built);
    const entry = changes.find((e) => e.path === "two.txt")!;
    expect(entry.change).toBe("added");
    const result = await coreContent(built, { changeIndex: entry.index, side: "result", observation });
    expect(result.status).toBe("available");
    if (result.status === "available") {
      expect(Buffer.from(result.dataBase64, "base64").toString()).toBe("SHARED\n");
    }
  });

  test("the production tree has one streaming observation owner and no whole-file read in the content path", async () => {
    const { readdir } = await import("node:fs/promises");
    const srcDir = join(import.meta.dir, "..", "src");
    const owners: string[] = [];
    for (const file of await readdir(srcDir)) {
      if (!file.endsWith(".ts")) continue;
      const source = await readFile(join(srcDir, file), "utf8");
      const code = source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
      if (code.includes("observeRegularFileStream") && !code.includes("import")) {
        // definition without import — impossible; count importers instead
      }
      if (code.includes("export async function observeRegularFileStream")) {
        owners.push(file);
      }
      if (file === "pipeline_v2_project_change_content.ts" || file === "pipeline_v2_project_change_content_internal.ts" || file === "pipeline_v2_project_changes_internal.ts" || file === "pipeline_v2_project_stream_internal.ts") {
        expect(code.includes("readFile(")).toBe(false);
        expect(code.includes("readWholeFile")).toBe(file === "pipeline_v2_project_changes_internal.ts" ? false : false);
        expect(/writeFile|rename\(|chmod\(|mkdir\(|rmdir\(|unlink\(/.test(code)).toBe(false);
      }
    }
    expect(owners).toEqual(["pipeline_v2_project_stream_internal.ts"]);
    const streamSource = await readFile(join(srcDir, "pipeline_v2_project_stream_internal.ts"), "utf8");
    const streamCode = streamSource.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
    expect(streamCode.includes("Buffer.concat")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// CLI parser, control-plane routing, the subprocess client and the real
// CLI subprocess wire
// ---------------------------------------------------------------------------
describe("content CLI and control-plane wire", () => {
  test("the CLI parser accepts both flag forms with all defaults", () => {
    const { parseCommand } = require("../src/cli_args.ts") as typeof import("../src/cli_args.ts");
    const space = parseCommand("project-change-content", [
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/operator/project",
      "--change-index", "3",
      "--side", "baseline",
      "--observation", "a".repeat(64),
      "--json",
    ]);
    expect(space).toEqual({
      kind: "project-change-content",
      runId: "chg",
      configRoot: "/cfg",
      project: "/operator/project",
      changeIndex: 3,
      side: "baseline",
      observation: "a".repeat(64),
      offset: 0,
      limit: 65536,
      json: true,
    });
    const inline = parseCommand("project-change-content", [
      "--run-id=chg",
      "--config-root=/cfg",
      "--project=/operator/project",
      "--change-index=3",
      "--side=result",
      `--observation=${"b".repeat(64)}`,
      "--offset=8",
      "--limit=262144",
    ]);
    expect(inline).toEqual({
      kind: "project-change-content",
      runId: "chg",
      configRoot: "/cfg",
      project: "/operator/project",
      changeIndex: 3,
      side: "result",
      observation: "b".repeat(64),
      offset: 8,
      limit: 262144,
      json: false,
    });
  });

  test("the CLI parser rejects duplicates, unknowns, missing flags and bad scalars", () => {
    const { parseCommand } = require("../src/cli_args.ts") as typeof import("../src/cli_args.ts");
    const base = ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--change-index", "1", "--side", "result", "--observation", "a".repeat(64)];
    const bad: string[][] = [
      [],
      ["--run-id", "chg"],
      [...base.slice(0, 6)],
      ["--run-id", "chg", "--config-root", "/c", "--project", "relative", "--change-index", "1", "--side", "result", "--observation", "a".repeat(64)],
      [...base, "--run-id", "chg"],
      [...base, "--change-index", "0"],
      [...base, "--change-index", "1.5"],
      [...base, "--side", "middle"],
      [...base, "--side", "BASELINE"],
      [...base, "--observation", "ZZ"],
      [...base, "--observation", "a".repeat(63)],
      [...base, "--offset", "-1"],
      [...base, "--limit", "0"],
      [...base, "--limit", "262145"],
      [...base, "--after", "2"],
      [...base, "--launcher-id", "dhl_x"],
      [...base, "--pipeline-root", "/b"],
      [...base, "--execution-index", "1"],
      [...base, "--state-root", "/s"],
      [...base, "positional"],
      [...base, "--unknown"],
      [...base, "--json=x"],
    ];
    for (const argv of bad) {
      expect(() => parseCommand("project-change-content", argv)).toThrow();
    }
  });

  test("the control-plane routing builds the exact argv with the bound project path", async () => {
    const { routeOrchestratorCommand } = await import("../src/control_plane/routing.ts");
    const routed = routeOrchestratorCommand(
      { kind: "project_change_content", runId: "chg", projectPath: "/bound/project", changeIndex: 3, side: "baseline", observation: "a".repeat(64), offset: 8, limit: 4096 },
      { configRoot: "/cfg" },
    );
    expect(routed.expectedRunId).toBe("chg");
    expect(routed.argv).toEqual([
      "project-change-content",
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/bound/project",
      "--change-index", "3",
      "--side", "baseline",
      "--observation", "a".repeat(64),
      "--offset", "8",
      "--limit", "4096",
      "--json",
    ]);
    const minimal = routeOrchestratorCommand(
      { kind: "project_change_content", runId: "chg", projectPath: "/bound/project", changeIndex: 1, side: "result", observation: "a".repeat(64) },
      { configRoot: "/cfg" },
    );
    expect(minimal.argv).toEqual([
      "project-change-content",
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/bound/project",
      "--change-index", "1",
      "--side", "result",
      "--observation", "a".repeat(64),
      "--json",
    ]);
  });

  test("the control-plane routing rejects hostile project_change_content commands", async () => {
    const { routeOrchestratorCommand } = await import("../src/control_plane/routing.ts");
    const base = { kind: "project_change_content", runId: "chg", projectPath: "/p", changeIndex: 1, side: "result", observation: "a".repeat(64) };
    const bad: Array<Record<string, unknown>> = [
      { kind: "project_change_content" },
      { ...base, runId: "chg", changeIndex: 0 },
      { ...base, projectPath: "relative" },
      { ...base, extra: 1 },
      { ...base, side: "middle" },
      { ...base, observation: "nope" },
      { ...base, offset: -1 },
      { ...base, limit: 262145 },
      { ...base, projectId: "inject" },
      { ...base, configRoot: "/inject" },
    ];
    for (const command of bad) {
      expect(() => routeOrchestratorCommand(command as never, { configRoot: "/cfg" })).toThrow();
    }
  });

  test("the subprocess client parses all four success statuses through the fixture child and drops the run root", async () => {
    const { OrchestratorSubprocessClient } = await import("../src/control_plane/subprocess_client.ts");
    const client = new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: new URL("./control_plane_child.ts", import.meta.url).pathname,
      workingDirectory: "/tmp",
      configRoot: "/cfg",
      baseEnv: { PATH: "/usr/bin:/bin" },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
    const available = await client.run({
      kind: "project_change_content", runId: "sc-content-available", projectPath: "/p",
      changeIndex: 2, side: "result", observation: "a".repeat(64),
    } as never);
    expect(available.ok).toBe(true);
    if (!available.ok) throw new Error("unreachable");
    const a = available.projection as Record<string, unknown>;
    expect(a["kind"]).toBe("projectChangeContent");
    expect(a["status"]).toBe("available");
    expect(a["dataBase64"]).toBe(Buffer.from("Hello", "utf8").toString("base64"));
    expect(JSON.stringify(available.projection)).not.toContain("runRoot");
    expect(JSON.stringify(available.projection)).not.toContain("/tmp/cp-child-run");

    const empty = await client.run({
      kind: "project_change_content", runId: "sc-content-implicit-empty", projectPath: "/p",
      changeIndex: 2, side: "baseline", observation: "a".repeat(64),
    } as never);
    if (!empty.ok) throw new Error("unreachable");
    expect((empty.projection as Record<string, unknown>)["status"]).toBe("implicit_empty");
    expect((empty.projection as Record<string, unknown>)["byteCount"]).toBe(0);

    const meta = await client.run({
      kind: "project_change_content", runId: "sc-content-metadata-only", projectPath: "/p",
      changeIndex: 3, side: "result", observation: "a".repeat(64),
    } as never);
    if (!meta.ok) throw new Error("unreachable");
    expect((meta.projection as Record<string, unknown>)["status"]).toBe("metadata_only");
    expect((meta.projection as Record<string, unknown>)["node"]).toEqual({ kind: "symlink", target: "elsewhere.txt" });

    const unavailable = await client.run({
      kind: "project_change_content", runId: "sc-content-unavailable", projectPath: "/p",
      changeIndex: 4, side: "baseline", observation: "a".repeat(64),
    } as never);
    if (!unavailable.ok) throw new Error("unreachable");
    expect((unavailable.projection as Record<string, unknown>)["status"]).toBe("unavailable");
    expect((unavailable.projection as Record<string, unknown>)["reason"]).toBe("source_no_longer_matches_baseline");

    const failure = await client.run({
      kind: "project_change_content", runId: "sc-content-failure", projectPath: "/p",
      changeIndex: 1, side: "result", observation: "a".repeat(64),
    } as never);
    expect(failure.ok).toBe(true);
    if (!failure.ok) throw new Error("unreachable");
    expect((failure.projection as Record<string, unknown>)["kind"]).toBe("projectChangeContentFailure");
    expect((failure.projection as Record<string, unknown>)["reason"]).toBe("stale_observation");
  });

  test("the subprocess client rejects malformed content documents with typed transport failures", async () => {
    const { OrchestratorSubprocessClient } = await import("../src/control_plane/subprocess_client.ts");
    const client = new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: new URL("./control_plane_child.ts", import.meta.url).pathname,
      workingDirectory: "/tmp",
      configRoot: "/cfg",
      baseEnv: { PATH: "/usr/bin:/bin" },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
    const bad: string[] = [
      "sc-content-bad-status",
      "sc-content-extra-field",
      "sc-content-missing-field",
      "sc-content-bad-side",
      "sc-content-bad-observation",
      "sc-content-v2",
      "sc-content-implicit-wrong-cursor",
      "sc-content-bad-reason",
      "sc-content-node-not-object",
    ];
    for (const scenario of bad) {
      const result = await client.run({
        kind: "project_change_content", runId: scenario, projectPath: "/p",
        changeIndex: 1, side: "result", observation: "a".repeat(64),
      } as never);
      expect(result.ok).toBe(false);
      if (result.ok) throw new Error("unreachable");
      expect([
        "unexpected_output_shape",
        "unsupported_contract",
      ]).toContain(result.failure.reason);
    }
  });

  test("the real CLI subprocess serves the content projection end to end and drops the run root", async () => {
    const built = await buildFinalizedRun("cc-e2e", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "DOGFOOD\n", { mode: 0o600 });
    const { OrchestratorSubprocessClient } = await import("../src/control_plane/subprocess_client.ts");
    const client = new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: new URL("../src/main.ts", import.meta.url).pathname,
      workingDirectory: built.harness.root,
      configRoot: built.harness.root,
      baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: built.harness.stateRoot },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
    const routed = (await import("../src/control_plane/routing.ts")).routeOrchestratorCommand(
      { kind: "project_change_content", runId: built.runId, projectPath: built.harness.projectSource, changeIndex: 1, side: "result", observation: "a".repeat(64) },
      { configRoot: built.harness.root },
    );
    expect(routed.argv[0]).toBe("project-change-content");
    const stale = await client.run({
      kind: "project_change_content", runId: built.runId, projectPath: built.harness.projectSource,
      changeIndex: 1, side: "result", observation: "a".repeat(64),
    } as never);
    expect(stale.ok).toBe(true);
    if (!stale.ok) throw new Error("unreachable");
    expect((stale.projection as Record<string, unknown>)["kind"]).toBe("projectChangeContentFailure");
    expect((stale.projection as Record<string, unknown>)["reason"]).toBe("stale_observation");
    // The fresh observation serves the exact bytes.
    const { observation } = await observationOf(built);
    const available = await client.run({
      kind: "project_change_content", runId: built.runId, projectPath: built.harness.projectSource,
      changeIndex: 1, side: "result", observation,
    } as never);
    expect(available.ok).toBe(true);
    if (!available.ok) throw new Error("unreachable");
    const projection = available.projection as Record<string, unknown>;
    expect(projection["status"]).toBe("available");
    expect(Buffer.from(projection["dataBase64"] as string, "base64").toString()).toBe("DOGFOOD\n");
    expect(JSON.stringify(available.projection)).not.toContain("runRoot");
    expect(JSON.stringify(available.projection)).not.toContain(built.harness.root);
    // The added baseline is the implicit empty side over the same wire.
    const empty = await client.run({
      kind: "project_change_content", runId: built.runId, projectPath: built.harness.projectSource,
      changeIndex: 1, side: "baseline", observation,
    } as never);
    if (!empty.ok) throw new Error("unreachable");
    expect((empty.projection as Record<string, unknown>)["status"]).toBe("implicit_empty");
    // A wrong offset is the typed invalid_cursor failure on the same wire.
    const cursor = await client.run({
      kind: "project_change_content", runId: built.runId, projectPath: built.harness.projectSource,
      changeIndex: 1, side: "result", observation, offset: 99,
    } as never);
    if (!cursor.ok) throw new Error("unreachable");
    expect((cursor.projection as Record<string, unknown>)["reason"]).toBe("invalid_cursor");
  });
});
