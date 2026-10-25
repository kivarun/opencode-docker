/**
 * The versioned read-only Project Changes API: the "run workspace changes
 * relative to the durable project baseline" projection over the durable
 * schema-v10 provenance.
 *
 * Every fixture is built through the real owners: the real project-copy
 * preparation (which publishes the provenance manifest), the real
 * reducer/store/sink, and real bundles. The read-only surfaces are proven
 * zero-write with full run-tree fingerprints; classification, relations,
 * pagination, observation identity, failure vocabulary, the CLI wire and
 * the control-plane routing/client are covered here.
 */
import { chmod, link, mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, unlink, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, test } from "bun:test";
import { parseCommand } from "../src/cli_args.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { prepareRunProject, snapshotRunInputs, type PreparedRunProjectWithProvenance, type RunInputsSnapshot } from "../src/pipeline_v2_runtime.ts";
import {
  PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS,
  projectChangesPipelineV2Run,
  type PipelineV2ProjectChangesOutcome,
} from "../src/pipeline_v2_project_changes.ts";
import {
  compareProjectChangesWithIo,
  type PipelineV2ProjectChangeEntry,
} from "../src/pipeline_v2_project_changes_internal.ts";
import { realProjectWorkspaceVerifyIo, type ProjectWorkspaceVerifyIo } from "../src/pipeline_v2_project_provenance_store_internal.ts";
import { publishPipelineV2ProjectProvenance } from "../src/pipeline_v2_project_provenance_store.ts";
import { preparePipelineV2ProjectProvenance } from "../src/pipeline_v2_project_provenance.ts";

import { createHash } from "node:crypto";

const sha = (content: string): string => createHash("sha256").update(content).digest("hex");

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

const FAILED_PIPELINE_YAML = PIPELINE_YAML.replace("  - id: done\n    type: terminal\n    result: success", "  - id: done\n    type: terminal\n    result: failed");

const PLAN_VALUE = { ok: true };

interface Harness {
  readonly root: string;
  readonly stateRoot: string;
  readonly bundle: string;
  readonly configRoot: string;
  readonly projectSource: string;
  readonly taskFile: string;
}

async function makeHarness(prefix: string, projectSpec: readonly ProjectSpec[], yaml: string = PIPELINE_YAML): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(
    join(configRoot, "profiles", "coder.yaml"),
    [
      "schema_version: 1",
      "image: ghcr.io/example/worker:1",
      "opencode_config: opencode/coder.json",
      "env:",
      "  MODEL_API_KEY:",
      "    from_env: CODER_SOURCE_VAR_1",
      "    required: true",
      "",
    ].join("\n"),
  );
  await writeFile(join(configRoot, "opencode", "coder.json"), JSON.stringify({ model: "glm53-flash" }));
  const projectSource = join(root, "project-source");
  await mkdir(projectSource, { recursive: true });
  await writeProjectTree(projectSource, projectSpec);
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  const taskFile = join(userdata, "task.md");
  await writeFile(taskFile, "TASK-BODY\n");
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
  return { root, stateRoot, bundle, configRoot, projectSource, taskFile };
}

interface BuiltRun {
  readonly harness: Harness;
  readonly runId: string;
  readonly runRoot: string;
  readonly pipeline: ResolvedPipelineV2;
  readonly prepared: PreparedRunProjectWithProvenance;
  readonly inputs: RunInputsSnapshot;
}

async function buildFinalizedRun(
  prefix: string,
  projectSpec: readonly ProjectSpec[],
  options: {
    readonly outcome?: "success" | "failed";
    readonly runId?: string;
    readonly finalize?: boolean;
    readonly yaml?: string;
    readonly prepareSource?: (projectSource: string) => Promise<void>;
  } = {},
): Promise<BuiltRun> {
  const harness = await makeHarness(prefix, projectSpec, options.yaml);
  if (options.prepareSource !== undefined) {
    await options.prepareSource(harness.projectSource);
  }
  const runId = options.runId ?? "chg";
  const pipeline = await loadPipelineV2(harness.bundle);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { mode: 0o700 });
  const prepared = await prepareRunProject(harness.projectSource, runRoot);
  const inputs = await snapshotRunInputs(pipeline, [{ id: "task", path: harness.taskFile }], runRoot);
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: inputs.inputs.map((entry) => ({ id: entry.id, type: entry.type, protected: entry.protected, digest: entry.digest })),
    projectProvenance: prepared.binding,
  });
  if (options.finalize !== false) {
    await sink.dispatch({ kind: "start_agent_execution", stateId: "work", profile: "coder", executionRole: "planning" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: `sess-${runId}` },
      { kind: "agent_tool_session_created", sessionId: `tool-${runId}` },
      { kind: "agent_running" },
    ]) {
      await sink.dispatch(command as never);
    }
    const digest = "a".repeat(64);
    await sink.dispatch({ kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest }], workerLog: { status: "unavailable" } } as never);
    await sink.dispatch({ kind: "agent_cleanup_completed" } as never);
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "work", outcome: "completed", to: "done", transition_index: 0 },
      executionIndex: 1,
    } as never);
    const failed = options.outcome === "failed";
    await sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: failed ? "failed" : "success" } as never);
    await sink.dispatch({ kind: "run_outputs_published", outputs: [] } as never);
    await sink.dispatch(failed ? ({ kind: "run_failed", reason: "terminal_failed" } as never) : ({ kind: "run_succeeded" } as never));
  }
  return { harness, runId, runRoot, pipeline, prepared, inputs };
}

async function readState(built: BuiltRun): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(built.runRoot, "state.json"), "utf8"));
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

type ChangesCall = Parameters<typeof projectChangesPipelineV2Run>[0];

async function callChanges(
  built: BuiltRun,
  overrides: Partial<ChangesCall> = {},
): Promise<PipelineV2ProjectChangesOutcome> {
  return await projectChangesPipelineV2Run({
    runId: built.runId,
    stateRoot: built.harness.stateRoot,
    projectPath: built.harness.projectSource,
    ...overrides,
  } as ChangesCall);
}

async function expectChanges(built: BuiltRun, overrides: Partial<ChangesCall> = {}): Promise<Extract<PipelineV2ProjectChangesOutcome, { ok: true }>> {
  const outcome = await callChanges(built, overrides);
  if (!outcome.ok) {
    throw new Error(`expected success, got ${outcome.reason}`);
  }
  return outcome;
}

function entryOf(outcome: { changes: readonly PipelineV2ProjectChangeEntry[] }, path: string): PipelineV2ProjectChangeEntry {
  const entry = outcome.changes.find((candidate) => candidate.path === path);
  if (entry === undefined) {
    throw new Error(`no change entry for ${path}`);
  }
  return entry;
}

// ---------------------------------------------------------------------------
// Eligibility and typed refusals
// ---------------------------------------------------------------------------
describe("project changes eligibility", () => {
  test("a nonterminal run refuses with changes_not_ready and zero writes", async () => {
    const built = await buildFinalizedRun("pc-elig", [{ path: "README.md", kind: "file", content: "SEED\n" }], { finalize: false });
    const before = await fingerprint(built.runRoot);
    const outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("changes_not_ready");
    expect(outcome.exitCode).toBe(1);
    expect(outcome.runId).toBe(built.runId);
    expect(outcome.runRoot).toBe(built.runRoot);
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("a missing run is run_not_found and an unsafe run id is never echoed", async () => {
    const built = await buildFinalizedRun("pc-miss", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const missing = await callChanges(built, { runId: "nosuch" });
    expect(missing.ok).toBe(false);
    if (missing.ok) throw new Error("unreachable");
    expect(missing.reason).toBe("run_not_found");
    expect(missing.runRoot).toBeNull();
    const unsafe = await callChanges(built, { runId: "../escape" });
    expect(unsafe.ok).toBe(false);
    if (unsafe.ok) throw new Error("unreachable");
    expect(unsafe.reason).toBe("invalid_options");
    expect(unsafe.runId).toBe("");
    expect(unsafe.runRoot).toBeNull();
  });

  test("invalid options are refused before any filesystem read", async () => {
    const built = await buildFinalizedRun("pc-opts", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const cases: Array<Partial<ChangesCall>> = [
      { limit: 0 },
      { limit: 201 },
      { after: -1 },
      { observation: "NOT-A-DIGEST" },
      { after: 1 },
    ];
    for (const overrides of cases) {
      const outcome = await callChanges(built, overrides);
      expect(outcome.ok).toBe(false);
      if (outcome.ok) throw new Error("unreachable");
      expect(outcome.reason).toBe("invalid_options");
      expect(outcome.runRoot).toBeNull();
    }
  });
});

// ---------------------------------------------------------------------------
// Classification battery
// ---------------------------------------------------------------------------
describe("project changes classification", () => {
  test("an unchanged pair of trees yields zero changes and a stable observation", async () => {
    const built = await buildFinalizedRun("pc-none", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "src", kind: "directory" },
      { path: "src/index.js", kind: "file", content: "hi\n" },
    ]);
    const first = await expectChanges(built);
    expect(first.changes).toEqual([]);
    expect(first.summary).toEqual({
      total: 0,
      added: 0,
      deleted: 0,
      content_modified: 0,
      mode_modified: 0,
      content_and_mode_modified: 0,
      symlink_target_modified: 0,
      type_changed: 0,
      matches_baseline: 0,
      matches_result: 0,
      diverged: 0,
    });
    expect(first.nextAfter).toBeNull();
    const second = await expectChanges(built);
    expect(second.observation).toBe(first.observation);
    expect(JSON.stringify(second)).toBe(JSON.stringify(first));
  });

  test("added regular file and added directory tree", async () => {
    const built = await buildFinalizedRun("pc-added", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "DOGFOOD\n", { mode: 0o600 });
    await mkdir(join(built.runRoot, "project", "tools"), { mode: 0o700 });
    await writeFile(join(built.runRoot, "project", "tools", "run.sh"), "#!/bin/sh\n", { mode: 0o700 });
    const outcome = await expectChanges(built);
    expect(outcome.summary.total).toBe(3);
    expect(outcome.summary.added).toBe(3);
    const dogfood = entryOf(outcome, "dogfood.txt");
    expect(dogfood.index).toBe(1);
    expect(dogfood.change).toBe("added");
    expect(dogfood.baseline).toBeNull();
    expect(dogfood.result).toEqual({ kind: "regular_file", mode: 0o600, byte_count: 8, sha256: sha("DOGFOOD\n"), multiple_links: false });
    const tools = entryOf(outcome, "tools");
    expect(tools.change).toBe("added");
    expect(tools.result).toEqual({ kind: "directory", mode: 0o700 });
    const script = entryOf(outcome, "tools/run.sh");
    expect(script.change).toBe("added");
    expect(script.result).toEqual({ kind: "regular_file", mode: 0o700, byte_count: 10, sha256: sha("#!/bin/sh\n"), multiple_links: false });
    expect(outcome.summary.matches_baseline).toBe(3);
  });

  test("deleted file and deleted tree; renames are never inferred", async () => {
    const built = await buildFinalizedRun("pc-deleted", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "src", kind: "directory" },
      { path: "src/index.js", kind: "file", content: "hi\n" },
    ]);
    await rm(join(built.runRoot, "project", "src"), { recursive: true });
    const outcome = await expectChanges(built);
    expect(outcome.summary.deleted).toBe(2);
    expect(outcome.summary.total).toBe(2);
    const src = entryOf(outcome, "src");
    expect(src.change).toBe("deleted");
    expect(src.result).toBeNull();
    expect(src.baseline).toEqual({ kind: "directory", mode: 0o750 });
    const file = entryOf(outcome, "src/index.js");
    expect(file.change).toBe("deleted");
    expect(file.baseline).toEqual({
      kind: "regular_file",
      mode: 0o644,
      byte_count: 3,
      sha256: sha("hi\n"),
      multiple_links: false,
    });
    expect(outcome.summary.matches_baseline).toBe(2);
  });

  test("content modified keeps the original source mode as the result mode", async () => {
    const built = await buildFinalizedRun("pc-content", [{ path: "README.md", kind: "file", content: "SEED\n", mode: 0o644 }]);
    await writeFile(join(built.runRoot, "project", "README.md"), "CHANGED\n", { mode: 0o600 });
    const outcome = await expectChanges(built);
    const entry = entryOf(outcome, "README.md");
    expect(entry.change).toBe("content_modified");
    expect(entry.source_relation).toBe("matches_baseline");
    expect(entry.baseline).toEqual({ kind: "regular_file", mode: 0o644, byte_count: 5, sha256: sha("SEED\n"), multiple_links: false });
    expect(entry.result).toEqual({ kind: "regular_file", mode: 0o644, byte_count: 8, sha256: sha("CHANGED\n"), multiple_links: false });
  });

  test("the normalized workspace mode is never a false-positive mode change", async () => {
    // source 0644 -> workspace baseline 0600, worker does not chmod:
    // a naive source-vs-workspace comparison would report a mode change.
    const built = await buildFinalizedRun("pc-falsepos", [{ path: "README.md", kind: "file", content: "SEED\n", mode: 0o644 }]);
    const outcome = await expectChanges(built);
    expect(outcome.changes).toEqual([]);
  });

  test("source 0755 maps to workspace 0700 and a content-only change keeps result mode 0755", async () => {
    const built = await buildFinalizedRun("pc-exec", [{ path: "run.sh", kind: "file", content: "#!/bin/sh\n", mode: 0o755 }]);
    await writeFile(join(built.runRoot, "project", "run.sh"), "#!/bin/sh\necho hi\n", { mode: 0o700 });
    const outcome = await expectChanges(built);
    const entry = entryOf(outcome, "run.sh");
    expect(entry.change).toBe("content_modified");
    expect(entry.result).not.toBeNull();
    if (entry.result === null || entry.result.kind !== "regular_file") throw new Error("unreachable");
    expect(entry.result.mode).toBe(0o755);
    expect(entry.source_relation).toBe("matches_baseline");
  });

  test("worker chmod after the normalized baseline projects the exact final mode", async () => {
    const built = await buildFinalizedRun("pc-chmod", [{ path: "README.md", kind: "file", content: "SEED\n", mode: 0o644 }]);
    await chmod(join(built.runRoot, "project", "README.md"), 0o700);
    const outcome = await expectChanges(built);
    const entry = entryOf(outcome, "README.md");
    expect(entry.change).toBe("mode_modified");
    expect(entry.result).toEqual({ kind: "regular_file", mode: 0o700, byte_count: 5, sha256: sha("SEED\n"), multiple_links: false });
    expect(entry.source_relation).toBe("matches_baseline");
  });

  test("content and mode modified together project the exact final mode", async () => {
    const built = await buildFinalizedRun("pc-both", [{ path: "README.md", kind: "file", content: "SEED\n", mode: 0o644 }]);
    const target = join(built.runRoot, "project", "README.md");
    await writeFile(target, "CHANGED\n", { mode: 0o600 });
    await chmod(target, 0o700);
    const outcome = await expectChanges(built);
    const entry = entryOf(outcome, "README.md");
    expect(entry.change).toBe("content_and_mode_modified");
    if (entry.result === null || entry.result.kind !== "regular_file") throw new Error("unreachable");
    expect(entry.result.mode).toBe(0o700);
    expect(entry.source_relation).toBe("matches_baseline");
  });

  test("a directory without a worker chmod is never a change", async () => {
    const built = await buildFinalizedRun("pc-dir", [
      { path: "src", kind: "directory", mode: 0o750 },
      { path: "src/index.js", kind: "file", content: "hi\n" },
    ]);
    const outcome = await expectChanges(built);
    expect(outcome.changes).toEqual([]);
  });

  test("a worker chmod on a directory is a mode change; descendants alone are not", async () => {
    const built = await buildFinalizedRun("pc-dirmode", [
      { path: "src", kind: "directory", mode: 0o750 },
      { path: "src/index.js", kind: "file", content: "hi\n" },
    ]);
    await writeFile(join(built.runRoot, "project", "src", "index.js"), "changed\n", { mode: 0o600 });
    await chmod(join(built.runRoot, "project", "src"), 0o755);
    const outcome = await expectChanges(built);
    expect(outcome.summary.total).toBe(2);
    const dir = entryOf(outcome, "src");
    expect(dir.change).toBe("mode_modified");
    expect(dir.result).toEqual({ kind: "directory", mode: 0o755 });
    const file = entryOf(outcome, "src/index.js");
    expect(file.change).toBe("content_modified");
  });

  test("symlink target modified", async () => {
    const built = await buildFinalizedRun("pc-symlink", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "latest", kind: "symlink", target: "README.md" },
    ]);
    await rm(join(built.runRoot, "project", "latest"));
    await symlink("src", join(built.runRoot, "project", "latest"));
    const outcome = await expectChanges(built);
    const entry = entryOf(outcome, "latest");
    expect(entry.change).toBe("symlink_target_modified");
    expect(entry.baseline).toEqual({ kind: "symlink", target: "README.md" });
    expect(entry.result).toEqual({ kind: "symlink", target: "src" });
    expect(entry.source_relation).toBe("matches_baseline");
  });

  test("file to directory and file to symlink are type changes", async () => {
    const built = await buildFinalizedRun("pc-type", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const target = join(built.runRoot, "project", "README.md");
    await rm(target);
    await mkdir(target, { mode: 0o755 });
    const first = await expectChanges(built);
    const dirEntry = entryOf(first, "README.md");
    expect(dirEntry.change).toBe("type_changed");
    expect(dirEntry.baseline).toEqual({ kind: "regular_file", mode: 0o644, byte_count: 5, sha256: sha("SEED\n"), multiple_links: false });
    expect(dirEntry.result).toEqual({ kind: "directory", mode: 0o755 });

    await rm(target, { recursive: true });
    await symlink("elsewhere", target);
    const second = await expectChanges(built);
    const linkEntry = entryOf(second, "README.md");
    expect(linkEntry.change).toBe("type_changed");
    expect(linkEntry.result).toEqual({ kind: "symlink", target: "elsewhere" });
    expect(second.summary.type_changed).toBe(1);
  });

  test("source relations: matches_result, diverged and the incompatible ancestor", async () => {
    const built = await buildFinalizedRun("pc-rel", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "keeper", kind: "file", content: "KEEP\n" },
    ]);
    await writeFile(join(built.runRoot, "project", "README.md"), "CHANGED\n", { mode: 0o600 });
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "DOGFOOD\n", { mode: 0o600 });
    await mkdir(join(built.runRoot, "project", "blocked"), { mode: 0o700 });
    await writeFile(join(built.runRoot, "project", "blocked", "child.txt"), "C\n", { mode: 0o600 });
    // the operator project already carries the exact intended result of the
    // README change
    await writeFile(join(built.harness.projectSource, "README.md"), "CHANGED\n", { mode: 0o600 });
    // the operator carries a third byte state for the dogfood addition
    await writeFile(join(built.harness.projectSource, "dogfood.txt"), "THIRD\n", { mode: 0o600 });
    // the operator blocks the added tree with a regular file at the ancestor
    await writeFile(join(built.harness.projectSource, "blocked"), "NOT-A-DIR\n", { mode: 0o600 });

    const outcome = await expectChanges(built);
    expect(entryOf(outcome, "README.md").source_relation).toBe("matches_result");
    expect(entryOf(outcome, "dogfood.txt").source_relation).toBe("diverged");
    expect(entryOf(outcome, "blocked").source_relation).toBe("diverged");
    expect(entryOf(outcome, "blocked/child.txt").source_relation).toBe("diverged");
    // unchanged operator entries produce no change entry at all
    expect(outcome.changes.some((candidate) => candidate.path === "keeper")).toBe(false);
    expect(outcome.summary.matches_result).toBe(1);
    expect(outcome.summary.diverged).toBe(3);
  });

  test("an added path with the operator project absent at the path matches the baseline observation", async () => {
    const built = await buildFinalizedRun("pc-addrel", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "DOGFOOD\n", { mode: 0o600 });
    const outcome = await expectChanges(built);
    expect(entryOf(outcome, "dogfood.txt").source_relation).toBe("matches_baseline");
  });

  test("hardlink metadata: the baseline nodes carry the schema-v10 group and the unchanged operator stays matches_baseline", async () => {
    const rebuilt = await buildFinalizedRun("pc-links", [
      { path: "a.txt", kind: "file", content: "SHARED\n" },
      { path: "b.txt", kind: "file", content: "SHARED\n" },
    ], {
      prepareSource: async (projectSource) => {
        await rm(join(projectSource, "b.txt"));
        await link(join(projectSource, "a.txt"), join(projectSource, "b.txt"));
      },
    });
    // the worker deletes b in the workspace so the deleted entry carries the
    // full hardlink metadata of the source observation.
    await rm(join(rebuilt.runRoot, "project", "b.txt"));
    const outcome = await expectChanges(rebuilt);
    const entry = entryOf(outcome, "b.txt");
    expect(entry.change).toBe("deleted");
    expect(entry.baseline).not.toBeNull();
    if (entry.baseline === null || entry.baseline.kind !== "regular_file") throw new Error("unreachable");
    expect(entry.baseline.multiple_links).toBe(true);
    expect(entry.baseline.hardlink_group).toBe(1);
    // the operator still carries the hardlink pair matching the source
    expect(entry.source_relation).toBe("matches_baseline");
  });

  test("workspace hardlink groups survive into the result node and the observation identity", async () => {
    const built = await buildFinalizedRun("pc-wlinks", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "one.txt", kind: "file", content: "ONE\n" },
    ]);
    // the worker creates a second path sharing the inode of one.txt and
    // modifies both in place
    await link(join(built.runRoot, "project", "one.txt"), join(built.runRoot, "project", "two.txt"));
    await writeFile(join(built.runRoot, "project", "one.txt"), "ONE-CHANGED\n", { mode: 0o600 });
    const outcome = await expectChanges(built);
    const one = entryOf(outcome, "one.txt");
    expect(one.change).toBe("content_modified");
    expect(one.result).not.toBeNull();
    if (one.result === null || one.result.kind !== "regular_file") throw new Error("unreachable");
    expect(one.result.multiple_links).toBe(true);
    expect(one.result.hardlink_group).toBe(1);
    const two = entryOf(outcome, "two.txt");
    expect(two.change).toBe("added");
    expect(two.result).not.toBeNull();
    if (two.result === null || two.result.kind !== "regular_file") throw new Error("unreachable");
    expect(two.result.multiple_links).toBe(true);
    expect(two.result.hardlink_group).toBe(1);
    // the observation carries the workspace group, so a topology change
    // moves the identity even when every digest stays the same
    const observation = outcome.observation;
    await rm(join(built.runRoot, "project", "two.txt"));
    await link(join(built.runRoot, "project", "one.txt"), join(built.runRoot, "project", "three.txt"));
    const changed = await expectChanges(built);
    expect(changed.observation).not.toBe(observation);
  });
});

// ---------------------------------------------------------------------------
// Failed terminal runs and provenance integrity
// ---------------------------------------------------------------------------
describe("project changes failed runs and provenance integrity", () => {
  test("a failed terminal run supports the same comparison as a succeeded one", async () => {
    const built = await buildFinalizedRun("pc-failed", [{ path: "README.md", kind: "file", content: "SEED\n" }], { outcome: "failed" });
    await writeFile(join(built.runRoot, "project", "partial.txt"), "PARTIAL\n", { mode: 0o600 });
    const outcome = await expectChanges(built);
    expect(outcome.summary.added).toBe(1);
    expect(entryOf(outcome, "partial.txt").change).toBe("added");
    const state = await readState(built);
    expect(state.status).toBe("failed");
  });

  test("a missing provenance manifest is artifact_missing with zero writes", async () => {
    const built = await buildFinalizedRun("pc-pvmiss", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    await rm(join(built.runRoot, "project-provenance.json"));
    const before = await fingerprint(built.runRoot);
    const outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_missing");
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("a tampered, wrong-mode or symlinked provenance manifest is artifact_modified", async () => {
    const built = await buildFinalizedRun("pc-pvtamper", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const manifestPath = join(built.runRoot, "project-provenance.json");
    const original = await readFile(manifestPath);
    const before = await fingerprint(built.runRoot);

    await writeFile(manifestPath, Buffer.concat([original, Buffer.from("\n")]));
    let outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_modified");

    await writeFile(manifestPath, original, { mode: 0o600 });
    await chmod(manifestPath, 0o644);
    outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_modified");

    await chmod(manifestPath, 0o600);
    await rm(manifestPath);
    await symlink("/etc/hostname", manifestPath);
    outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_modified");

    await rm(manifestPath);
    await writeFile(manifestPath, original, { mode: 0o600 });
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("a self-consistent manifest that diverges from its durable binding is artifact_modified", async () => {
    const built = await buildFinalizedRun("pc-pvbind", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const manifestPath = join(built.runRoot, "project-provenance.json");
    const parsed = JSON.parse(await readFile(manifestPath, "utf8")) as { entries: Array<{ path: string; kind: string; source_mode: number }> };
    const fileEntry = parsed.entries.find((candidate) => candidate.kind === "regular_file");
    if (fileEntry === undefined) throw new Error("unreachable");
    fileEntry.source_mode = fileEntry.source_mode === 420 ? 436 : 420;
    await writeFile(manifestPath, JSON.stringify(parsed), { mode: 0o600 });
    const outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("artifact_modified");
  });

  test("a missing operator project is project_invalid with zero writes", async () => {
    const built = await buildFinalizedRun("pc-noproj", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const before = await fingerprint(built.runRoot);
    const outcome = await callChanges(built, { projectPath: join(built.harness.root, "gone") });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("project_invalid");
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("a file substituted for the operator project is project_invalid", async () => {
    const built = await buildFinalizedRun("pc-projfile", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const substitute = join(built.harness.root, "projfile");
    await writeFile(substitute, "not a directory\n");
    const outcome = await callChanges(built, { projectPath: substitute });
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("project_invalid");
  });
});

// ---------------------------------------------------------------------------
// Pagination, observation identity and determinism
// ---------------------------------------------------------------------------
describe("project changes pagination and observation identity", () => {
  async function buildMultiPageRun(prefix: string): Promise<BuiltRun> {
    const built = await buildFinalizedRun(prefix, [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    for (let index = 1; index <= 5; index++) {
      await writeFile(join(built.runRoot, "project", `f${index}.txt`), `body-${index}\n`, { mode: 0o600 });
    }
    return built;
  }

  test("the full ordered set paginates without duplicates or gaps", async () => {
    const built = await buildMultiPageRun("pc-page");
    const first = await expectChanges(built, { limit: 2 });
    expect(first.changes.length).toBe(2);
    expect(first.nextAfter).toBe(2);
    expect(first.summary.total).toBe(5);
    const middle = await expectChanges(built, { after: 2, limit: 2, observation: first.observation });
    expect(middle.changes.length).toBe(2);
    expect(middle.nextAfter).toBe(4);
    const last = await expectChanges(built, { after: 4, limit: 2, observation: first.observation });
    expect(last.changes.length).toBe(1);
    expect(last.nextAfter).toBeNull();
    const indices = [...first.changes, ...middle.changes, ...last.changes].map((entry) => entry.index);
    expect(indices).toEqual([1, 2, 3, 4, 5]);
    const paths = [...first.changes, ...middle.changes, ...last.changes].map((entry) => entry.path);
    expect(paths).toEqual([...paths].sort());
    expect(new Set(paths).size).toBe(5);
    expect(paths).toEqual(["f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt"]);
  });

  test("after == total is an empty page with a null cursor; after > total is invalid_cursor", async () => {
    const built = await buildMultiPageRun("pc-cursor");
    const full = await expectChanges(built);
    const atEnd = await expectChanges(built, { after: 5, limit: 2, observation: full.observation });
    expect(atEnd.changes).toEqual([]);
    expect(atEnd.nextAfter).toBeNull();
    const beyond = await callChanges(built, { after: 6, limit: 2, observation: full.observation });
    expect(beyond.ok).toBe(false);
    if (beyond.ok) throw new Error("unreachable");
    expect(beyond.reason).toBe("invalid_cursor");
  });

  test("after=0 with an observation revalidates byte-identically; a mismatched observation is stale", async () => {
    const built = await buildMultiPageRun("pc-obs");
    const first = await expectChanges(built);
    const repeat = await expectChanges(built, { observation: first.observation });
    expect(JSON.stringify(repeat)).toBe(JSON.stringify(first));
    const stale = await callChanges(built, { observation: "a".repeat(64) });
    expect(stale.ok).toBe(false);
    if (stale.ok) throw new Error("unreachable");
    expect(stale.reason).toBe("stale_observation");
  });

  test("a final workspace change between pages is stale_observation with no partial page", async () => {
    const built = await buildMultiPageRun("pc-wschange");
    const first = await expectChanges(built, { limit: 2 });
    await writeFile(join(built.runRoot, "project", "late.txt"), "LATE\n", { mode: 0o600 });
    const before = await fingerprint(built.runRoot);
    const second = await callChanges(built, { after: 2, limit: 2, observation: first.observation });
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.reason).toBe("stale_observation");
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("an operator project change on a changed path is stale_observation", async () => {
    const built = await buildMultiPageRun("pc-opchange");
    const first = await expectChanges(built, { limit: 2 });
    await writeFile(join(built.harness.projectSource, "f3.txt"), "operator-touched\n", { mode: 0o600 });
    const second = await callChanges(built, { after: 2, limit: 2, observation: first.observation });
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.reason).toBe("stale_observation");
  });

  test("a successful read is zero-write over the whole run tree and the operator project", async () => {
    const built = await buildMultiPageRun("pc-zw");
    const beforeRun = await fingerprint(built.runRoot);
    const beforeProject = await fingerprint(built.harness.projectSource);
    await expectChanges(built);
    expect(await fingerprint(built.runRoot)).toBe(beforeRun);
    expect(await fingerprint(built.harness.projectSource)).toBe(beforeProject);
  });

  test("two parallel readers do not block each other and agree byte-identically", async () => {
    const built = await buildMultiPageRun("pc-par");
    const [a, b] = await Promise.all([expectChanges(built), expectChanges(built, { limit: 2 })]);
    expect(a.observation).toBe(b.observation);
    expect(a.summary.total).toBe(b.summary.total);
  });

  test("a held run mutation lock never blocks the read and the read takes no lock", async () => {
    const built = await buildMultiPageRun("pc-nolock");
    const { acquirePipelineV2RunMutationLock } = await import("../src/pipeline_v2_run_mutation_lock.ts");
    const acquisition = acquirePipelineV2RunMutationLock({ runRoot: built.runRoot, runId: built.runId, mode: "existing" });
    if (acquisition.busy) throw new Error("the test lock could not be taken");
    try {
      const outcome = await expectChanges(built, { limit: 2 });
      expect(outcome.summary.total).toBe(5);
      // the run tree carries no lock artifacts of any kind
      const names = await readdir(built.runRoot);
      expect(names.filter((name) => name.includes("lock"))).toEqual([]);
    } finally {
      acquisition.lock.release();
    }
  });

  test("an unrelated mutation of the run root does not affect the comparison", async () => {
    const built = await buildMultiPageRun("pc-unrelated");
    const first = await expectChanges(built);
    // an unrelated durable mutation: another run in the same state root and
    // an extra file in the run root outside the observed workspace
    const otherRun = join(built.harness.stateRoot, "pipeline-runs", "other-run");
    await mkdir(otherRun, { recursive: true, mode: 0o700 });
    await writeFile(join(otherRun, "state.json"), "{}\n", { mode: 0o600 });
    await writeFile(join(built.runRoot, "unrelated.txt"), "UNRELATED\n", { mode: 0o600 });
    const second = await expectChanges(built);
    expect(second.observation).toBe(first.observation);
    expect(JSON.stringify(second.changes)).toBe(JSON.stringify(first.changes));
  });

  test("the full current-project observation is bound: an operator change on an unchanged path moves the observation", async () => {
    const built = await buildMultiPageRun("pc-fullproj");
    const first = await expectChanges(built, { limit: 2 });
    // the operator touches a path that is not part of the change set: the
    // chosen semantics bind the FULL current-project observation, so the
    // identity moves and the next page is stale
    await writeFile(join(built.harness.projectSource, "README.md"), "operator-touched\n", { mode: 0o600 });
    const second = await callChanges(built, { after: 2, limit: 2, observation: first.observation });
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.reason).toBe("stale_observation");
  });
});

// ---------------------------------------------------------------------------
// Scan stability and unsupported entries
// ---------------------------------------------------------------------------
describe("project changes scan stability", () => {
  /** An IO wrapper that fires one mutation on the first readdir of a tree root. */
  function mutatingIo(
    watchRoot: string,
    mutation: (root: string) => Promise<void>,
  ): ProjectWorkspaceVerifyIo {
    const real = realProjectWorkspaceVerifyIo;
    let fired = false;
    return {
      lstatOrNull: (path) => real.lstatOrNull(path),
      readdirNames: async (path) => {
        const names = await real.readdirNames(path);
        if (!fired && path === watchRoot) {
          fired = true;
          await mutation(watchRoot);
        }
        return names;
      },
      readWholeFile: (path) => real.readWholeFile(path),
      readlink: (path) => real.readlink(path),
      realpath: (path) => real.realpath(path),
    };
  }

  test("an operator project entry removed during the scan is project_unstable", async () => {
    const built = await buildFinalizedRun("pc-opunstable", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "victim.txt", kind: "file", content: "V\n" },
    ]);
    const before = await fingerprint(built.runRoot);
    const io = mutatingIo(built.harness.projectSource, async (root) => {
      await rm(join(root, "victim.txt"));
    });
    let caught: string | null = null;
    try {
      await compareProjectChangesWithIo(io, built.runRoot, built.harness.projectSource, await readState(built));
    } catch (cause) {
      caught = cause instanceof Error ? cause.message : String(cause);
    }
    expect(caught).toContain("disappeared during the scan");
    // the same instability through the public API is the typed reason
    const unstableApi = await projectChangesPipelineV2Run({
      runId: built.runId,
      stateRoot: built.harness.stateRoot,
      projectPath: built.harness.projectSource,
    });
    expect(unstableApi.ok).toBe(true);
    expect(await fingerprint(built.runRoot)).toBe(before);
  });

  test("an operator project entry added during the scan is project_unstable", async () => {
    const built = await buildFinalizedRun("pc-opadd", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const io = mutatingIo(built.harness.projectSource, async (root) => {
      await writeFile(join(root, "late.txt"), "LATE\n", { mode: 0o600 });
    });
    let caught: string | null = null;
    try {
      await compareProjectChangesWithIo(io, built.runRoot, built.harness.projectSource, await readState(built));
    } catch (cause) {
      caught = cause instanceof Error ? cause.message : String(cause);
    }
    expect(caught).toContain("entry set changed during the scan");
  });

  test("a final workspace change during the scan is workspace_unstable through the public API", async () => {
    const built = await buildFinalizedRun("pc-wsunstable", [
      { path: "README.md", kind: "file", content: "SEED\n" },
      { path: "victim.txt", kind: "file", content: "V\n" },
    ]);
    const workspaceRoot = join(built.runRoot, "project");
    const io = mutatingIo(workspaceRoot, async (root) => {
      await rm(join(root, "victim.txt"));
    });
    let caught: string | null = null;
    try {
      await compareProjectChangesWithIo(io, built.runRoot, built.harness.projectSource, await readState(built));
    } catch (cause) {
      caught = cause instanceof Error ? cause.message : String(cause);
    }
    expect(caught).toContain("disappeared during the scan");
    // the facade maps the workspace instability over the real IO by
    // tampering between two reads instead: the second read sees a changed
    // workspace and returns stale_observation for the first observation.
    const first = await expectChanges(built);
    await writeFile(join(workspaceRoot, "late.txt"), "LATE\n", { mode: 0o600 });
    const second = await callChanges(built, { observation: first.observation });
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.reason).toBe("stale_observation");
  });

  test("a FIFO in either tree is unsupported_project_entry", async () => {
    const built = await buildFinalizedRun("pc-fifo", [{ path: "README.md", kind: "file", content: "SEED\n" }]);
    const mkfifo = (path: string): Promise<void> =>
      new Promise((resolve, reject) => {
        const child = spawn("mkfifo", [path]);
        child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
      });
    await mkfifo(join(built.runRoot, "project", "pipe"));
    let outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("unsupported_project_entry");
    await rm(join(built.runRoot, "project", "pipe"));
    await mkfifo(join(built.harness.projectSource, "pipe"));
    outcome = await callChanges(built);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) throw new Error("unreachable");
    expect(outcome.reason).toBe("unsupported_project_entry");
  });
});

// ---------------------------------------------------------------------------
// CLI parser, control-plane routing and the real CLI subprocess wire
// ---------------------------------------------------------------------------
describe("project changes CLI and control-plane wire", () => {
  test("the CLI parser accepts both flag forms with all defaults", () => {
    const space = parseCommand("project-changes", [
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/operator/project",
      "--json",
    ]);
    expect(space).toEqual({
      kind: "project-changes",
      runId: "chg",
      configRoot: "/cfg",
      project: "/operator/project",
      after: 0,
      limit: 100,
      observation: null,
      json: true,
    });
    const inline = parseCommand("project-changes", [
      "--run-id=chg",
      "--config-root=/cfg",
      "--project=/operator/project",
      "--after=2",
      "--limit=7",
      "--observation=" + "a".repeat(64),
    ]);
    expect(inline).toEqual({
      kind: "project-changes",
      runId: "chg",
      configRoot: "/cfg",
      project: "/operator/project",
      after: 2,
      limit: 7,
      observation: "a".repeat(64),
      json: false,
    });
  });

  test("the CLI parser rejects duplicates, unknowns, missing flags and bad scalars", () => {
    const bad: string[][] = [
      [],
      ["--run-id", "chg"],
      ["--run-id", "chg", "--config-root", "/c"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "relative"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--run-id", "chg"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--limit", "0"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--limit", "201"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--after", "-1"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--observation", "zz"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--launcher-id", "dhl_x"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--pipeline-root", "/b"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--state-root", "/s"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "positional"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--unknown"],
      ["--run-id", "chg", "--config-root", "/c", "--project", "/p", "--json=x"],
    ];
    for (const argv of bad) {
      expect(() => parseCommand("project-changes", argv)).toThrow();
    }
  });

  test("the control-plane routing builds the exact argv with the bound project path", async () => {
    const { routeOrchestratorCommand } = await import("../src/control_plane/routing.ts");
    const routed = routeOrchestratorCommand(
      { kind: "project_changes", runId: "chg", projectPath: "/bound/project", after: 2, limit: 7, observation: "a".repeat(64) },
      { configRoot: "/cfg" },
    );
    expect(routed.expectedRunId).toBe("chg");
    expect(routed.argv).toEqual([
      "project-changes",
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/bound/project",
      "--after", "2",
      "--limit", "7",
      "--observation", "a".repeat(64),
      "--json",
    ]);
    const minimal = routeOrchestratorCommand(
      { kind: "project_changes", runId: "chg", projectPath: "/bound/project" },
      { configRoot: "/cfg" },
    );
    expect(minimal.argv).toEqual([
      "project-changes",
      "--run-id", "chg",
      "--config-root", "/cfg",
      "--project", "/bound/project",
      "--json",
    ]);
  });

  test("the control-plane routing rejects hostile project_changes commands", async () => {
    const { routeOrchestratorCommand } = await import("../src/control_plane/routing.ts");
    const bad: Array<Record<string, unknown>> = [
      { kind: "project_changes" },
      { kind: "project_changes", runId: "chg" },
      { kind: "project_changes", runId: "chg", projectPath: "relative" },
      { kind: "project_changes", runId: "chg", projectPath: "/p", extra: 1 },
      { kind: "project_changes", runId: "chg", projectPath: "/p", after: -1 },
      { kind: "project_changes", runId: "chg", projectPath: "/p", observation: "nope" },
      { kind: "project_changes", runId: "chg", projectPath: "/p", projectId: "inject" },
      { kind: "project_changes", runId: "chg", projectPath: "/p", configRoot: "/inject" },
    ];
    for (const command of bad) {
      expect(() => routeOrchestratorCommand(command as never, { configRoot: "/cfg" })).toThrow();
    }
  });

  test("the real CLI subprocess serves the changes projection and drops the run root", async () => {
    const built = await buildFinalizedRun("pc-e2e", [
      { path: "README.md", kind: "file", content: "SEED\n" },
    ]);
    await writeFile(join(built.runRoot, "project", "dogfood.txt"), "DOGFOOD\n", { mode: 0o600 });
    const client = new (await import("../src/control_plane/subprocess_client.ts")).OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: new URL("../src/main.ts", import.meta.url).pathname,
      workingDirectory: built.harness.root,
      configRoot: built.harness.root,
      baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: built.harness.stateRoot },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
    const result = await client.run({
      kind: "project_changes",
      runId: built.runId,
      projectPath: built.harness.projectSource,
      limit: 10,
    } as never);
    if (!result.ok) throw new Error(`transport failure: ${result.failure.reason}`);
    expect(result.projection.kind).toBe("projectChanges");
    if (result.projection.kind !== "projectChanges") throw new Error("unreachable");
    expect(result.projection.schemaVersion).toBe(1);
    expect(result.projection.runId).toBe(built.runId);
    expect(result.projection.revision).toBeGreaterThanOrEqual(1);
    expect(JSON.stringify(result.projection)).not.toContain("runRoot");
    expect(JSON.stringify(result.projection)).not.toContain(built.harness.root);
    const summary = result.projection.summary as Record<string, unknown>;
    expect(summary["total"]).toBe(1);
    const changes = result.projection.changes as Array<Record<string, unknown>>;
    expect(changes.length).toBe(1);
    expect(changes[0]!["path"]).toBe("dogfood.txt");
    expect(changes[0]!["change"]).toBe("added");
    // the next page requires the exact observation
    const stale = await client.run({
      kind: "project_changes",
      runId: built.runId,
      projectPath: built.harness.projectSource,
      after: 1,
      observation: "b".repeat(64),
    } as never);
    if (!stale.ok) throw new Error("unreachable");
    expect(stale.projection.kind).toBe("projectChangesFailure");
    if (stale.projection.kind !== "projectChangesFailure") throw new Error("unreachable");
    expect(stale.projection.reason).toBe("stale_observation");
    // a nonterminal refusal follows the failure projection (its own state root)
    const fresh = await buildFinalizedRun("pc-e2e2", [{ path: "README.md", kind: "file", content: "SEED\n" }], { finalize: false });
    const freshClient = new (await import("../src/control_plane/subprocess_client.ts")).OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: new URL("../src/main.ts", import.meta.url).pathname,
      workingDirectory: fresh.harness.root,
      configRoot: fresh.harness.root,
      baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: fresh.harness.stateRoot },
      stdoutLimitBytes: 1_000_000,
      stderrLimitBytes: 1_000_000,
      sigintGraceMs: 2000,
    });
    const notReady = await freshClient.run({
      kind: "project_changes",
      runId: fresh.runId,
      projectPath: fresh.harness.projectSource,
    } as never);
    if (!notReady.ok) throw new Error("unreachable");
    expect(notReady.projection.kind).toBe("projectChangesFailure");
    if (notReady.projection.kind !== "projectChangesFailure") throw new Error("unreachable");
    expect(notReady.projection.reason).toBe("changes_not_ready");
  }, 30000);
});

// ---------------------------------------------------------------------------
// Ownership pins and UI fail-closed boundary
// ---------------------------------------------------------------------------
describe("project changes ownership and UI boundary", () => {
  test("parseApiDocument rejects the project_changes document kind", async () => {
    const ui = await import("../ui/app.js");
    const document = {
      schema_version: 1,
      kind: "project_changes",
      run_id: "chg",
      revision: 3,
      observation: "a".repeat(64),
      summary: {},
      changes: [],
      next_after: null,
    };
    expect(() => ui.parseApiDocument(document)).toThrow();
    expect(() => ui.parseApiDocument({ schema_version: 1, kind: "changes", run_id: "chg" })).toThrow();
  });

  test("the browser UI v6 panel stays read-only with no content or POST capability", async () => {
    const appSource = await readFile(join(import.meta.dir, "..", "ui", "app.js"), "utf8");
    // the dedicated v6 owner exists beside the general fail-closed client
    expect(appSource).toContain("parseProjectChangesDocument");
    expect(appSource).toContain("projectChangesRequestPath");
    expect(appSource).toContain("createProjectChangesViewer");
    // the general client still rejects the document kind
    const ui = await import("../ui/app.js");
    expect(() =>
      ui.parseApiDocument({ schema_version: 1, kind: "project_changes", run_id: "chg", revision: 1, observation: "a".repeat(64), summary: {}, changes: [], next_after: null }),
    ).toThrow();
    // no content, download, apply or write capability anywhere in the
    // browser module
    for (const banned of ["/diff", "/apply", "showSaveFilePicker", "showOpenFilePicker", "navigator.clipboard", "download", "innerHTML"]) {
      expect(appSource.includes(banned)).toBe(false);
    }
    const html = await readFile(join(import.meta.dir, "..", "ui", "index.html"), "utf8");
    expect(html).toContain("project-changes-section");
    expect(html).toContain("Run workspace changes relative to the durable project baseline");
    expect(html).toContain("The registered project is not modified.");
  });

  test("the failure vocabulary is closed and the public reasons never carry content", async () => {
    expect(PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS).toEqual([
      "run_not_found",
      "invalid_options",
      "run_state_invalid",
      "pipeline_mismatch",
      "artifact_missing",
      "artifact_modified",
      "changes_not_ready",
      "project_invalid",
      "project_unstable",
      "workspace_unstable",
      "unsupported_project_entry",
      "invalid_cursor",
      "stale_observation",
      "internal_error",
    ]);
  });

  test("the production tree has one provenance parser and one changes owner with no write paths", async () => {
    const { readdir } = await import("node:fs/promises");
    const srcDir = join(import.meta.dir, "..", "src");
    const files = (await readdir(srcDir)).filter((name) => name.endsWith(".ts"));
    const parsers: string[] = [];
    const facadeUsers: string[] = [];
    const coreUsers: string[] = [];
    for (const file of files) {
      const source = await readFile(join(srcDir, file), "utf8");
      const code = source.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
      if (code.includes("project-provenance.json")) {
        parsers.push(file);
      }
      if (code.includes("projectChangesPipelineV2Run")) {
        facadeUsers.push(file);
      }
      if (code.includes("compareProjectChangesWithIo")) {
        coreUsers.push(file);
      }
      if (file.startsWith("pipeline_v2_project_changes")) {
        expect(code.includes("PipelineV2RunMutationLock")).toBe(false);
        expect(code.includes("docker-helper")).toBe(false);
        expect(/writeFile|rename\(|chmod\(|mkdir\(|rmdir\(|unlink\(/.test(code)).toBe(false);
      }
    }
    expect(parsers.sort()).toEqual([
      "pipeline_v2_project_provenance_store_internal.ts",
    ]);
    expect(facadeUsers.sort()).toEqual(["main.ts", "pipeline_v2_project_changes.ts"]);
    expect(coreUsers.sort()).toEqual(["pipeline_v2_project_changes.ts", "pipeline_v2_project_changes_internal.ts"]);
  });

  test("the facade runs the real read-only scanner IO and no test seam is exported", async () => {
    const facade = await import("../src/pipeline_v2_project_changes.ts");
    const keys = Object.keys(facade).sort();
    expect(keys).toEqual([
      "PIPELINE_V2_PROJECT_CHANGES_DEFAULT_LIMIT",
      "PIPELINE_V2_PROJECT_CHANGES_FAILURE_REASONS",
      "PIPELINE_V2_PROJECT_CHANGES_MAX_LIMIT",
      "PIPELINE_V2_PROJECT_CHANGE_KINDS",
      "PIPELINE_V2_PROJECT_SOURCE_RELATIONS",
      "PipelineV2ProjectChangesError",
      "projectChangesPipelineV2Run",
    ].sort());
  });
});
