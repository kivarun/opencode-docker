import { chmod, lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { spawn } from "node:child_process";
import { expect, test } from "bun:test";
import {
  PipelineError,
  loadPipeline,
  parsePipelineSpec,
  planMultiStateExecution,
} from "../src/pipeline.ts";
import {
  loadPipelineV2,
  parsePipelineV2Spec,
  planActivationLayout,
} from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  prepareActivationData,
  snapshotRunInputs,
} from "../src/pipeline_v2_runtime.ts";
import { runAgentSmoke, type AgentSmokeDeps } from "../src/agent_smoke.ts";
import { STANDARD_AGENT_RESULT_SCHEMA } from "../src/agent_result.ts";

const TASK_JSON = JSON.stringify({ goal: "the task" });
const CONFIG_JSON = JSON.stringify({ ok: true, deep: { nested: null } });

const RUNTIME_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true
  - id: specs
    type: directory
    protected: false
  - id: config
    type: json
    protected: true
    schema: schemas/config.schema.json

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: specs
        source:
          pipeline_input: specs
      - id: config
        source:
          pipeline_input: config
    outputs:
      - id: patch
        type: file
      - id: scratch
        type: directory
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: patch
        source:
          state_output:
            state: coder
            output: patch
      - id: scratch
        source:
          state_output:
            state: coder
            output: scratch
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: report
        type: file
      - id: facts
        type: json
        schema: schemas/facts.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;

const SELFREF_YAML = RUNTIME_YAML.replace(
  `      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: report`,
  `      - id: task
        source:
          pipeline_input: task
      - id: facts
        source:
          state_output:
            state: architect
            output: facts
    outputs:
      - id: report`,
);

const V1_PIPELINE_YAML = `schema_version: 1
entry_state: execute
max_transitions: 1

inputs:
  - id: task
    path: TASK.md
    protected: true

states:
  - id: execute
    type: agent
    profile: default
    prompt: prompts/execute.md
    inputs:
      - task
    result_schema: schemas/agent-result.schema.json
    timeout_seconds: 3600
    max_attempts: 1
    transitions:
      - outcome: completed
        to: completed
  - id: completed
    type: terminal
    result: success
`;

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-runtime-test-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  return { root, bundle };
}

async function writeRuntimeBundle(
  dirs: BundleDirs,
  yaml: string = RUNTIME_YAML,
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "review the patch\n");
  await writeFile(
    join(dirs.bundle, "schemas", "config.schema.json"),
    JSON.stringify({ type: "object", required: ["ok"] }),
  );
  await writeFile(
    join(dirs.bundle, "schemas", "facts.schema.json"),
    JSON.stringify({ type: "object", required: ["revision"] }),
  );
}

async function writeSources(root: string): Promise<string> {
  const dir = join(root, "userdata");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "task.txt"), TASK_JSON);
  await mkdir(join(dir, "specs", "sub"), { recursive: true });
  await writeFile(join(dir, "specs", "a.txt"), "A");
  await writeFile(join(dir, "specs", "sub", "b.txt"), "B");
  await writeFile(join(dir, "config.json"), CONFIG_JSON);
  return dir;
}

async function makeRunRoot(root: string): Promise<string> {
  const runRoot = join(root, "run");
  await mkdir(runRoot, { recursive: true });
  // the shared project directory of the whole run is created by the caller
  // (with closed permissions); the runtime never creates or copies it
  await mkdir(join(runRoot, "project"), { mode: 0o700 });
  await writeFile(join(runRoot, "project", "README.md"), "project seed\n");
  return runRoot;
}

const ALL_BINDINGS = (sources: string) => [
  { id: "task", path: join(sources, "task.txt") },
  { id: "specs", path: join(sources, "specs") },
  { id: "config", path: join(sources, "config.json") },
];

async function withRuntime(
  fn: (dirs: BundleDirs, sources: string) => Promise<void>,
  yaml: string = RUNTIME_YAML,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeRuntimeBundle(dirs, yaml);
  const sources = await writeSources(dirs.root);
  try {
    await fn(dirs, sources);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

async function expectReject(
  run: () => Promise<unknown> | unknown,
  message: RegExp | string,
): Promise<void> {
  let failure: unknown;
  try {
    await run();
  } catch (cause) {
    failure = cause;
  }
  expect(failure).toBeInstanceOf(PipelineError);
  const error = failure as Error;
  if (typeof message === "string") {
    expect(error.message).toBe(message);
  } else {
    expect(error.message).toMatch(message);
  }
}

async function makeFifo(path: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn("mkfifo", [path]);
    child.on("error", (cause) => reject(cause));
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

/**
 * A hand-minted accepted record for cases whose rejection happens during
 * parsing or fixed-location resolution, before any digest is verified.
 * Real records come from `acceptActivationOutputs`; the digest here is a
 * plausible-looking but never-verified placeholder.
 */
const FAKE_DIGEST = "a".repeat(64);

const rec = (
  state: string,
  output: string,
  activationIndex: number,
  digest: string = FAKE_DIGEST,
): { state: string; output: string; activation_index: number; digest: string } => ({
  state,
  output,
  activation_index: activationIndex,
  digest,
});

test("1. the exact binding set is enforced before any filesystem mutation", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);

    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    expect(snap.inputs.map((entry) => entry.id)).toEqual(["task", "specs", "config"]);
    expect(snap.inputs.map((entry) => entry.protected)).toEqual([true, false, true]);
    for (const entry of snap.inputs) {
      expect(entry.digest).toMatch(/^[0-9a-f]{64}$/);
      expect(
        entry.snapshot_path.startsWith(`${await realpath(runRoot)}/data/inputs/`),
      ).toBe(true);
    }

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [{ id: "task", path: join(sources, "task.txt") }],
        await makeRunRoot(join(dirs.root, "r-missing")),
      );
    }, /pipeline input "specs" is not bound/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [...ALL_BINDINGS(sources), { id: "task", path: join(sources, "task.txt") }],
        await makeRunRoot(join(dirs.root, "r-duplicate")),
      );
    }, /pipeline input "task" is bound more than once/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [...ALL_BINDINGS(sources), { id: "nope", path: join(sources, "task.txt") }],
        await makeRunRoot(join(dirs.root, "r-unknown")),
      );
    }, /run input binding "nope" does not match a declared pipeline input/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [{ ...ALL_BINDINGS(sources)[0], extra: 1 }, ...ALL_BINDINGS(sources).slice(1)],
        await makeRunRoot(join(dirs.root, "r-extra")),
      );
    }, /run input binding 0 has unknown field "extra"/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [{ id: "task", path: "userdata/task.txt" }, ...ALL_BINDINGS(sources).slice(1)],
        await makeRunRoot(join(dirs.root, "r-relative")),
      );
    }, /run input binding 0 path must be an absolute path/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [{ id: "task", path: join(sources, "task.txt") }, { id: "specs", path: join(sources, "specs") }],
        await makeRunRoot(join(dirs.root, "r-partial")),
      );
    }, /pipeline input "config" is not bound/);

    // a binding path that resolves inside the run root is rejected
    await expectReject(async () => {
      const inside = await makeRunRoot(join(dirs.root, "r-inside"));
      await writeFile(join(inside, "inside.txt"), TASK_JSON);
      await snapshotRunInputs(pipeline, [
        { id: "task", path: join(inside, "inside.txt") },
        { id: "specs", path: join(sources, "specs") },
        { id: "config", path: join(sources, "config.json") },
      ], inside);
    }, /pipeline input "task" bound path must resolve outside the canonical run root/);
  });
});

test("2. the bound object kind must match the declared port type", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(sources, "specs") },
          { id: "specs", path: join(sources, "specs") },
          { id: "config", path: join(sources, "config.json") },
        ],
        await makeRunRoot(join(dirs.root, "r-file-dir")),
      );
    }, /pipeline input "task" declares type "file" but the bound path is not a regular file/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(sources, "task.txt") },
          { id: "specs", path: join(sources, "task.txt") },
          { id: "config", path: join(sources, "config.json") },
        ],
        await makeRunRoot(join(dirs.root, "r-dir-file")),
      );
    }, /pipeline input "specs" declares type "directory" but the bound path is not a real directory/);

    await expectReject(async () => {
      await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(sources, "task.txt") },
          { id: "specs", path: join(sources, "specs") },
          { id: "config", path: join(sources, "specs") },
        ],
        await makeRunRoot(join(dirs.root, "r-json-dir")),
      );
    }, /pipeline input "config" declares type "json" but the bound path is not a regular file/);

    await expectReject(async () => {
      await symlink(join(sources, "task.txt"), join(sources, "task-link.txt"));
      await snapshotRunInputs(
        pipeline,
        [
          { id: "task", path: join(sources, "task-link.txt") },
          { id: "specs", path: join(sources, "specs") },
          { id: "config", path: join(sources, "config.json") },
        ],
        await makeRunRoot(join(dirs.root, "r-symlink")),
      );
    }, /pipeline input "task" bound path .* is a symbolic link; bind the real object/);
  });
});

test("3. invalid JSON is rejected before mutation; valid JSON is copied byte-for-byte", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);

    await writeFile(join(sources, "config.json"), "{ not json");
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    }, /pipeline input "config" bound file .* is not valid JSON/);
    await expect(lstat(join(runRoot, "data"))).rejects.toThrow();

    // a JSON value that parses but violates the declared schema is also
    // rejected before any mutation (same validation mechanism as outputs)
    const scalar = "null";
    await writeFile(join(sources, "config.json"), scalar);
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    }, /pipeline input "config" bound file .* does not conform to its JSON schema/);
    await expect(lstat(join(runRoot, "data"))).rejects.toThrow();

    const valid = '{"ok":true,"deep":{"nested":null}}';
    await writeFile(join(sources, "config.json"), valid);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const config = snap.inputs.find((entry) => entry.id === "config");
    expect(await readFile(config?.snapshot_path ?? "", "utf8")).toBe(valid);
    const task = snap.inputs.find((entry) => entry.id === "task");
    expect(await readFile(task?.snapshot_path ?? "", "utf8")).toBe(TASK_JSON);
  });
});

test("4. the snapshot is immutable against later source change and deletion", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(await loadPipelineV2(dirs.bundle), ALL_BINDINGS(sources), runRoot);
    const digests = snap.inputs.map((entry) => entry.digest);
    const taskPath = snap.inputs[0]?.snapshot_path ?? "";
    const taskBefore = await readFile(taskPath);

    await writeFile(join(sources, "task.txt"), "MUTATED AFTER SNAPSHOT");
    await writeFile(join(sources, "specs", "a.txt"), "MUTATED TREE");
    expect(await readFile(taskPath)).toEqual(taskBefore);
    expect(await readFile(join(runRoot, "data", "inputs", "specs", "a.txt"), "utf8")).toBe("A");
    expect(snap.inputs.map((entry) => entry.digest)).toEqual(digests);

    await rm(join(dirs.root, "userdata"), { recursive: true, force: true });
    expect(await readFile(taskPath)).toEqual(taskBefore);
    expect(await readFile(join(runRoot, "data", "inputs", "specs", "sub", "b.txt"), "utf8")).toBe("B");
  });
});

test("5. the digest depends only on type, relative paths and content", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    const runRootA = await makeRunRoot(join(dirs.root, "run-a"));
    const snapA = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRootA);

    const other = join(dirs.root, "other-location");
    await mkdir(other, { recursive: true });
    await writeFile(join(other, "task.txt"), TASK_JSON);
    await mkdir(join(other, "specs", "sub"), { recursive: true });
    await writeFile(join(other, "specs", "a.txt"), "A");
    await writeFile(join(other, "specs", "sub", "b.txt"), "B");
    await writeFile(join(other, "config.json"), CONFIG_JSON);
    const epoch = new Date(0);
    await utimes(join(other, "task.txt"), epoch, epoch);
    await utimes(join(other, "specs", "a.txt"), epoch, epoch);
    const snapB = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(other, "task.txt") },
      { id: "specs", path: join(other, "specs") },
      { id: "config", path: join(other, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-b")));

    expect(snapA.inputs.map((entry) => entry.digest)).toEqual(
      snapB.inputs.map((entry) => entry.digest),
    );

    await writeFile(join(other, "task.txt"), `${TASK_JSON} `);
    const snapC = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(other, "task.txt") },
      { id: "specs", path: join(other, "specs") },
      { id: "config", path: join(other, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-c")));
    expect(snapC.inputs[0]?.digest).not.toBe(snapA.inputs[0]?.digest);
    expect(snapC.inputs[1]?.digest).toBe(snapA.inputs[1]?.digest);

    const sameBytes = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "config.json") },
      { id: "specs", path: join(sources, "specs") },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-d")));
    const fileDigest = sameBytes.inputs.find((entry) => entry.id === "task")?.digest;
    const jsonDigest = sameBytes.inputs.find((entry) => entry.id === "config")?.digest;
    expect(fileDigest).not.toBe(jsonDigest);

    const empty = join(dirs.root, "empty-dir");
    await mkdir(empty, { recursive: true });
    const emptyBindings = (root: string) =>
      snapshotRunInputs(pipeline, [
        { id: "task", path: join(sources, "task.txt") },
        { id: "specs", path: empty },
        { id: "config", path: join(sources, "config.json") },
      ], root);
    const emptyA = await emptyBindings(await makeRunRoot(join(dirs.root, "run-e")));
    const emptyB = await emptyBindings(await makeRunRoot(join(dirs.root, "run-f")));
    expect(emptyA.inputs[1]?.digest).toBe(emptyB.inputs[1]?.digest);
    expect(emptyA.inputs[1]?.digest).toMatch(/^[0-9a-f]{64}$/);

    // the same file under a different name digests differently
    const renamed = join(dirs.root, "renamed-tree");
    await mkdir(renamed, { recursive: true });
    await writeFile(join(renamed, "z.txt"), "A");
    const renameA = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: renamed },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-g")));
    await rm(join(renamed, "z.txt"), { force: true });
    await writeFile(join(renamed, "y.txt"), "A");
    const renameB = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: renamed },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-h")));
    expect(renameA.inputs[1]?.digest).not.toBe(renameB.inputs[1]?.digest);

    // adding or removing an empty directory changes the digest
    await rm(join(renamed, "y.txt"), { force: true });
    await writeFile(join(renamed, "same.txt"), "A");
    const base = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: renamed },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-i")));
    await mkdir(join(renamed, "empty-sub"), { recursive: true });
    const withEmpty = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: renamed },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-j")));
    await rm(join(renamed, "empty-sub"), { recursive: true, force: true });
    const withoutEmpty = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: renamed },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-k")));
    expect(withEmpty.inputs[1]?.digest).not.toBe(base.inputs[1]?.digest);
    expect(withoutEmpty.inputs[1]?.digest).toBe(base.inputs[1]?.digest);

    // a directory entry and a file entry are never the same representation
    const dirEntry = join(dirs.root, "dir-entry");
    await mkdir(join(dirEntry, "x"), { recursive: true });
    const fileEntry = join(dirs.root, "file-entry");
    await mkdir(fileEntry, { recursive: true });
    await writeFile(join(fileEntry, "x"), "");
    const dirEntrySnap = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: dirEntry },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-l")));
    const fileEntrySnap = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: fileEntry },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-m")));
    expect(dirEntrySnap.inputs[1]?.digest).not.toBe(fileEntrySnap.inputs[1]?.digest);

    // file creation order does not influence the digest
    const orderOne = join(dirs.root, "order-one");
    await mkdir(orderOne, { recursive: true });
    await writeFile(join(orderOne, "a.txt"), "A");
    await writeFile(join(orderOne, "b.txt"), "B");
    await mkdir(join(orderOne, "sub"), { recursive: true });
    await writeFile(join(orderOne, "sub", "c.txt"), "C");
    const orderTwo = join(dirs.root, "order-two");
    await mkdir(join(orderTwo, "sub"), { recursive: true });
    await writeFile(join(orderTwo, "sub", "c.txt"), "C");
    await writeFile(join(orderTwo, "b.txt"), "B");
    await writeFile(join(orderTwo, "a.txt"), "A");
    const orderOneSnap = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: orderOne },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-n")));
    const orderTwoSnap = await snapshotRunInputs(pipeline, [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: orderTwo },
      { id: "config", path: join(sources, "config.json") },
    ], await makeRunRoot(join(dirs.root, "run-o")));
    expect(orderOneSnap.inputs[1]?.digest).toBe(orderTwoSnap.inputs[1]?.digest);
  });
});

test("6. declared inputs are processed in declaration order", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    await expectReject(async () => {
      await snapshotRunInputs(pipeline, [
        { id: "task", path: join(sources, "missing.txt") },
        { id: "specs", path: join(sources, "task.txt") },
        { id: "config", path: join(sources, "task.txt") },
      ], await makeRunRoot(join(dirs.root, "r-order")));
    }, /pipeline input "task" bound path .* does not exist/);

    const runRoot = await makeRunRoot(join(dirs.root, "r-order-2"));
    await writeFile(join(sources, "config.json"), "{ broken");
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    }, /is not valid JSON/);
    await expect(lstat(join(runRoot, "data"))).rejects.toThrow();
  });
});

test("7. symlink and special-file entries inside directory inputs are rejected", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    await symlink(join(sources, "task.txt"), join(sources, "specs", "link.txt"));
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), await makeRunRoot(join(dirs.root, "r-link")));
    }, /pipeline input "specs" contains symlink entry "link\.txt"/);
    await rm(join(sources, "specs", "link.txt"), { force: true });

    await new Promise<void>((resolve, reject) => {
      const child = spawn("mkfifo", [join(sources, "specs", "pipe")]);
      child.on("error", reject);
      child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
    });
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), await makeRunRoot(join(dirs.root, "r-fifo")));
    }, /pipeline input "specs" contains unsupported entry "pipe"/);
    await rm(join(sources, "specs", "pipe"), { force: true });

    const socketPath = join(sources, "specs", "sock");
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(socketPath, resolve));
    try {
      await expectReject(async () => {
        await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), await makeRunRoot(join(dirs.root, "r-sock")));
      }, /pipeline input "specs" contains unsupported entry "sock"/);
    } finally {
      server.close();
    }
  });
});

test("8. pipeline inputs are materialized into a fresh activation data tree", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(
      pipeline,
      ALL_BINDINGS(sources),
      runRoot,
    );
    const prep = await prepareActivationData(
      pipeline,
      snap,
      [],
      "coder",
      1,
    );

    expect(prep.run_root).toBe(await realpath(runRoot));
    expect(prep.state_id).toBe("coder");
    expect(prep.activation_index).toBe(1);
    expect(prep.activation_root).toBe(join(prep.run_root, "activations", "1-coder"));
    expect(prep.reject_undeclared_outputs).toBe(true);
    expect(snap.project_root).toBe(join(prep.run_root, "project"));
    expect(prep.project_root).toBe(snap.project_root);
    expect(prep.mounts[0]?.source).toBe(prep.project_root);
    // there is no per-activation project directory
    await expect(lstat(join(prep.data_root, "project"))).rejects.toThrow();

    expect(prep.input_ports.map((port) => port.id)).toEqual(["task", "specs", "config"]);
    expect(prep.input_ports.map((port) => port.type)).toEqual(["file", "directory", "json"]);
    expect(prep.input_ports.map((port) => port.path)).toEqual([
      join(prep.inputs_root, "task"),
      join(prep.inputs_root, "specs"),
      join(prep.inputs_root, "config"),
    ]);
    expect(await readFile(join(prep.inputs_root, "task"), "utf8")).toBe(TASK_JSON);
    expect(await readFile(join(prep.inputs_root, "specs", "sub", "b.txt"), "utf8")).toBe("B");
    expect(await readFile(join(prep.inputs_root, "config"), "utf8")).toBe(CONFIG_JSON);

    expect(prep.output_ports.map((port) => port.id)).toEqual(["patch", "scratch"]);
    await expect(lstat(join(prep.outputs_root, "patch"))).rejects.toThrow();
    expect((await lstat(join(prep.outputs_root, "scratch"))).isDirectory()).toBe(true);

    expect(prep.mounts).toEqual([
      { source: prep.project_root, target: "/workspace", read_only: false },
      { source: prep.inputs_root, target: "/pipeline/inputs", read_only: true },
      { source: prep.outputs_root, target: "/pipeline/outputs", read_only: false },
    ]);

    expect((await lstat(prep.inputs_root)).mode & 0o777).toBe(0o700);
    expect((await lstat(join(prep.inputs_root, "task"))).mode & 0o777).toBe(0o600);
    expect((await lstat(prep.outputs_root)).mode & 0o777).toBe(0o700);
    expect((await lstat(prep.project_root)).mode & 0o777).toBe(0o700);
    expect((await lstat(join(prep.inputs_root, "specs", "sub"))).mode & 0o777).toBe(0o700);
  });
});

test("9. accepted state outputs are handed over into the next activation", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH-1");
    await writeFile(join(coderPrep.outputs_root, "scratch", "work.txt"), "WORK");
    await mkdir(join(coderPrep.outputs_root, "scratch", "nested"), { recursive: true });
    await writeFile(join(coderPrep.outputs_root, "scratch", "nested", "deep.txt"), "DEEP");
    const coderRecords = await acceptActivationOutputs(pipeline, coderPrep);

    const architectPrep = await prepareActivationData(pipeline, snap, coderRecords, "architect", 2);

    expect(architectPrep.state_id).toBe("architect");
    expect(architectPrep.activation_root).toBe(join(runRoot, "activations", "2-architect"));
    expect(architectPrep.input_ports.map((port) => port.id)).toEqual(["patch", "scratch", "task"]);
    expect(await readFile(join(architectPrep.inputs_root, "patch"), "utf8")).toBe("PATCH-1");
    expect(await readFile(join(architectPrep.inputs_root, "scratch", "nested", "deep.txt"), "utf8")).toBe("DEEP");
    expect(await readFile(join(architectPrep.inputs_root, "task"), "utf8")).toBe(TASK_JSON);
    expect(architectPrep.output_ports.map((port) => port.id)).toEqual(["report", "facts"]);
    await expect(lstat(join(architectPrep.outputs_root, "report"))).rejects.toThrow();
    await expect(lstat(join(architectPrep.outputs_root, "facts"))).rejects.toThrow();
  });
});

test("10. the highest activation index wins, independent of list order", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
    const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
    await writeFile(join(first.outputs_root, "patch"), "PATCH-1");
    await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
    const firstRecords = await acceptActivationOutputs(pipeline, first);
    const secondRecords = await acceptActivationOutputs(pipeline, second);
    expect(firstRecords.map((record) => [record.state, record.output, record.activation_index]))
      .toEqual([["coder", "patch", 1], ["coder", "scratch", 1]]);

    const one = await prepareActivationData(pipeline, snap, [
      ...secondRecords.filter((record) => record.output === "patch"),
      ...firstRecords,
      ...secondRecords.filter((record) => record.output === "scratch"),
    ], "architect", 3);
    expect(await readFile(join(one.inputs_root, "patch"), "utf8")).toBe("PATCH-2");

    // permuting the records cannot change the accepted output
    const two = await prepareActivationData(pipeline, snap, [
      ...firstRecords.filter((record) => record.output === "scratch"),
      ...secondRecords.filter((record) => record.output === "patch"),
      ...firstRecords.filter((record) => record.output === "patch"),
      ...secondRecords.filter((record) => record.output === "scratch"),
    ], "architect", 4);
    expect(await readFile(join(two.inputs_root, "patch"), "utf8")).toBe("PATCH-2");
  });
});

test("11. missing, future and first-visit self state outputs are rejected", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    // missing: no accepted records at all
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 1);
    }, /input port "patch" of agent state "architect" references state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "1-architect"))).rejects.toThrow();

    // a specific reference stays missing while another reference is present:
    // a partially constructed runner history is rejected as incoherent
    // before any missing-reference resolution happens
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH-1");
    const coderRecords = await acceptActivationOutputs(pipeline, coderPrep);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        ...coderRecords.filter((record) => record.output === "patch"),
      ], "architect", 2);
    }, /accepted history activation index 1 of state "coder" is incomplete: the activation must record exactly the declared output ports \["patch","scratch"\], missing \["scratch"\]/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
  });

  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    // first visit of a self-referencing input port fails
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH-1");
    const coderRecords = await acceptActivationOutputs(pipeline, coderPrep);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, coderRecords, "architect", 2);
    }, /input port "facts" of agent state "architect" references state output "architect"\."facts" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();

    // a revisit resolves the self-reference from the fixed location of an
    // earlier architect activation whose worker produced the output; the
    // planted record set must be complete (report + facts), exactly as a
    // real acceptance of that activation would have released it
    const factsLeaf = join(runRoot, "activations", "2-architect", "data", "outputs");
    await mkdir(factsLeaf, { recursive: true });
    await writeFile(join(factsLeaf, "report"), "REPORT-2");
    await writeFile(join(factsLeaf, "facts"), JSON.stringify({ revision: 7 }));
    const reportDigest = await acceptedOutputDigest(
      "file",
      join(factsLeaf, "report"),
      "planted architect report output",
    );
    const factsDigest = await acceptedOutputDigest(
      "json",
      join(factsLeaf, "facts"),
      "planted architect facts output",
    );
    const revisit = await prepareActivationData(pipeline, snap, [
      ...coderRecords,
      { state: "architect", output: "report", activation_index: 2, digest: reportDigest },
      { state: "architect", output: "facts", activation_index: 2, digest: factsDigest },
    ], "architect", 3);
    expect(await readFile(join(revisit.inputs_root, "facts"), "utf8")).toBe(
      JSON.stringify({ revision: 7 }),
    );
  }, SELFREF_YAML);
});

test("12. accepted records are validated: shape, declaration, index, fixed location", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    await prepareActivationData(pipeline, snap, [], "coder", 1);

    const coderOutputs = join(runRoot, "activations", "1-coder", "data", "outputs");

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("nowhere", "patch", 1),
      ], "architect", 2);
    }, /accepted state output 0 references state "nowhere" which is not a declared agent state/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("done", "patch", 1),
      ], "architect", 2);
    }, /accepted state output 0 references state "done" which is not a declared agent state/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "nope", 1),
      ], "architect", 2);
    }, /references output "nope" which is not declared by state "coder"/);

    // no user paths and no types are accepted at all
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { ...rec("coder", "patch", 1), path: coderOutputs + "/patch" },
      ], "architect", 2);
    }, /accepted state output 0 has unknown field "path"/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { ...rec("coder", "patch", 1), type: "file" },
      ], "architect", 2);
    }, /accepted state output 0 has unknown field "type"/);

    // future and current activation indexes are rejected
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 2),
      ], "architect", 2);
    }, /records activation index 2 which is not below the current activation index 2/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 4),
      ], "architect", 2);
    }, /records activation index 4 which is not below the current activation index 2/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 0),
      ], "architect", 2);
    }, /activation index must be a positive safe integer/);

    // duplicate records are rejected
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "patch", 1),
      ], "architect", 2);
    }, /is listed more than once/);

    // one activation index cannot belong to two different states
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("architect", "report", 1),
      ], "architect", 2);
    }, /activation index 1 cannot belong to both state "coder" and state "architect"/);

    // the fixed location must exist: missing activation leaf (the index is
    // below the current one, but no leaf for that index and state exists;
    // the record set is complete, so coherence passes and resolution fails)
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("architect", "report", 2),
        rec("architect", "facts", 2),
      ], "architect", 3);
    }, /activation leaf .*2-architect does not exist/);

    // the fixed location must exist: missing output object
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "scratch", 1),
      ], "architect", 2);
    }, /fixed output .*1-coder\/data\/outputs\/patch does not exist/);

    // the fixed location must hold a real object of the declared kind
    await mkdir(join(coderOutputs, "patch"), { recursive: true });
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "scratch", 1),
      ], "architect", 2);
    }, /fixed output .*1-coder\/data\/outputs\/patch is not a regular file/);
    await rm(join(coderOutputs, "patch"), { recursive: true, force: true });

    // a symlink at the fixed location is rejected
    await symlink(join(dirs.root, "outside.txt"), join(coderOutputs, "patch"));
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "scratch", 1),
      ], "architect", 2);
    }, /fixed output .*1-coder\/data\/outputs\/patch is a symbolic link/);
  });
});

test("13. activation indexes are globally unique and paths are never reused", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    await prepareActivationData(pipeline, snap, [], "coder", 1);

    // exact repeat
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "coder", 1);
    }, /activation index 1 is already in use at .*1-coder/);

    // the same index with a different state fails identically
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 1);
    }, /activation index 1 is already in use at .*1-coder/);

    // a fresh index prepares normally
    const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
    expect(second.activation_root).toBe(join(runRoot, "activations", "2-coder"));

    for (const kind of ["file", "dir", "symlink"] as const) {
      const runRootX = await makeRunRoot(join(dirs.root, `r-leaf-${kind}`));
      const snapX = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRootX);
      // pre-placed entries under the requested index prefix are rejected
      // no matter their state id or object kind
      const leaf = join(runRootX, "activations", "1-coder");
      const other = join(runRootX, "activations", "1-architect");
      if (kind === "dir") {
        await mkdir(other, { recursive: true });
      } else {
        await mkdir(join(runRootX, "activations"), { recursive: true });
        if (kind === "file") {
          await writeFile(other, "not a directory");
        } else {
          await symlink(dirs.root, other);
        }
      }
      await expectReject(async () => {
        await prepareActivationData(pipeline, snapX, [], "coder", 1);
      }, /activation index 1 is already in use at .*1-architect/);
      await expect(lstat(leaf)).rejects.toThrow();
      // the pre-placed object was not modified
      if (kind === "dir") {
        expect((await lstat(other)).isDirectory()).toBe(true);
      } else if (kind === "file") {
        expect(await readFile(other, "utf8")).toBe("not a directory");
      } else {
        expect((await lstat(other)).isSymbolicLink()).toBe(true);
      }
    }
  });
});

test("14. symlink traps on run-owned paths are rejected and preserved", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const bindings = ALL_BINDINGS(sources);

    const runRootData = await makeRunRoot(join(dirs.root, "r-data-link"));
    const dataSentinel = join(runRootData, "data");
    await symlink(join(dirs.root, "elsewhere"), dataSentinel);
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, runRootData);
    }, /run data root .* exists but is a symbolic link/);
    expect((await lstat(dataSentinel)).isSymbolicLink()).toBe(true);

    const runRootInputs = await makeRunRoot(join(dirs.root, "r-inputs-link"));
    await mkdir(join(runRootInputs, "data"), { recursive: true });
    await symlink(join(dirs.root, "elsewhere"), join(runRootInputs, "data", "inputs"));
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, runRootInputs);
    }, /run inputs root .* exists but is a symbolic link/);
    expect((await lstat(join(runRootInputs, "data", "inputs"))).isSymbolicLink()).toBe(true);

    const runRootAct = await makeRunRoot(join(dirs.root, "r-act-link"));
    const snapAct = await snapshotRunInputs(pipeline, bindings, runRootAct);
    const actSentinel = join(runRootAct, "activations");
    await symlink(dirs.root, actSentinel);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snapAct, [], "coder", 1);
    }, /activations root .* exists but is a symbolic link/);
    expect((await lstat(actSentinel)).isSymbolicLink()).toBe(true);

    const runRootSentinel = await makeRunRoot(join(dirs.root, "r-sentinel"));
    const firstSnap = await snapshotRunInputs(pipeline, bindings, runRootSentinel);
    const taskSnapshot = firstSnap.inputs[0]?.snapshot_path ?? "";
    const sentinelBytes = await readFile(taskSnapshot);
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, runRootSentinel);
    }, /run input snapshot of "task" .* already exists, found an existing regular file/);
    expect(await readFile(taskSnapshot)).toEqual(sentinelBytes);
  });
});

test("15. records resolve only the fixed location; arbitrary paths never satisfy them", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    await prepareActivationData(pipeline, snap, [], "coder", 1);

    // a valid file at an arbitrary contained path cannot satisfy a record:
    // only the fixed orchestrator-derived location is resolved
    const arbitrary = join(runRoot, "arbitrary", "patch");
    await mkdir(join(runRoot, "arbitrary"), { recursive: true });
    await writeFile(arbitrary, "PATCH");
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "scratch", 1),
      ], "architect", 2);
    }, /fixed output .*1-coder\/data\/outputs\/patch does not exist/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
    expect(await readFile(arbitrary, "utf8")).toBe("PATCH");

    // a symlink planted at the fixed location is rejected regardless of
    // where it points
    const outside = join(dirs.root, "outside.txt");
    await writeFile(outside, "ESCAPED");
    const escapeLink = join(runRoot, "activations", "1-coder", "data", "outputs", "patch");
    await symlink(outside, escapeLink);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        rec("coder", "patch", 1),
        rec("coder", "scratch", 1),
      ], "architect", 3);
    }, /fixed output .*1-coder\/data\/outputs\/patch is a symbolic link/);
    expect(await readFile(outside, "utf8")).toBe("ESCAPED");
  });
});

test("16. snapshot and prepared layout results are frozen and mutation-resistant", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);

    expect(Object.isFrozen(snap)).toBe(true);
    expect(Object.isFrozen(snap.inputs)).toBe(true);
    expect(Object.isFrozen(snap.inputs[0])).toBe(true);
    expect(Object.isFrozen(prep)).toBe(true);
    expect(Object.isFrozen(prep.input_ports)).toBe(true);
    expect(Object.isFrozen(prep.input_ports[0])).toBe(true);
    expect(Object.isFrozen(prep.output_ports)).toBe(true);
    expect(Object.isFrozen(prep.mounts)).toBe(true);
    expect(Object.isFrozen(prep.mounts[0])).toBe(true);

    expect(() =>
      (snap.inputs as unknown as { push(value: unknown): number }).push(snap.inputs[0] as never),
    ).toThrow();
    expect(() => (prep.mounts as unknown as { pop(): unknown }).pop()).toThrow();
    expect(() => {
      (prep as unknown as { state_id: string }).state_id = "changed";
    }).toThrow();
  });
});

test("17. digests and serialized artifacts carry no source host paths", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const compiled = parsePipelineV2Spec(RUNTIME_YAML);
    const compiledJson = JSON.stringify(compiled);
    expect(compiledJson).not.toContain(sources);
    expect(compiledJson).not.toContain(dirs.root);

    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const serialized = JSON.stringify(snap);
    expect(serialized).not.toContain(sources);
    expect(serialized).toContain(snap.run_root);
    expect(snap.project_root).toBe(join(snap.run_root, "project"));
    for (const entry of snap.inputs) {
      expect(entry.digest).toMatch(/^[0-9a-f]{64}$/);
    }

    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    const serializedPrep = JSON.stringify(prep);
    expect(serializedPrep).not.toContain(sources);
    expect(serializedPrep).toContain(prep.run_root);
    for (const mount of prep.mounts) {
      expect(mount.source.startsWith(`${prep.run_root}/`)).toBe(true);
    }
  });
});

test("18. run root validation and rejected activation requests", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const bindings = ALL_BINDINGS(sources);

    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, join(dirs.root, "missing-run-root"));
    }, /run root .* does not exist/);

    const fileRoot = join(dirs.root, "not-a-dir");
    await writeFile(fileRoot, "x");
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, fileRoot);
    }, /run root .* exists but is an existing regular file/);

    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, "relative/run-root");
    }, /run root must be an absolute path/);

    const linkRoot = join(dirs.root, "symlinked-run-root");
    await symlink(dirs.root, linkRoot);
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, linkRoot);
    }, /run root .* exists but is a symbolic link/);

    // the shared project directory must pre-exist as a real directory
    const noProject = await makeRunRoot(join(dirs.root, "r-no-project"));
    await rm(join(noProject, "project"), { recursive: true, force: true });
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, noProject);
    }, /run project root .* does not exist/);

    const linkProject = await makeRunRoot(join(dirs.root, "r-link-project"));
    await rm(join(linkProject, "project"), { recursive: true, force: true });
    await symlink(dirs.root, join(linkProject, "project"));
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, linkProject);
    }, /run project root .* exists but is a symbolic link/);

    const fileProject = await makeRunRoot(join(dirs.root, "r-file-project"));
    await rm(join(fileProject, "project"), { recursive: true, force: true });
    await writeFile(join(fileProject, "project"), "not a directory");
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, bindings, fileProject);
    }, /run project root .* exists but is an existing regular file/);

    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, bindings, runRoot);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "../evil", 1);
    }, /activation state id "\.\.\/evil" is not a safe identifier/);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "unknown", 1);
    }, /state "unknown" is not declared by the pipeline/);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "done", 1);
    }, /state "done" is not an agent state/);
    for (const bad of [0, -1, 1.5, Number.NaN, "1"]) {
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [], "coder", bad as number);
      }, /activation index must be a positive safe integer/);
    }
  });
});

test("19. failed operations clean up exactly what they created", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    const runRoot = await makeRunRoot(dirs.root);
    const broken = join(sources, "broken-specs");
    await mkdir(join(broken, "sub"), { recursive: true });
    await writeFile(join(broken, "readable.txt"), "OK");
    await writeFile(join(broken, "unreadable.txt"), "SECRET");
    await chmod(join(broken, "unreadable.txt"), 0o000);
    await expectReject(async () => {
      await snapshotRunInputs(pipeline, [
        { id: "task", path: join(sources, "task.txt") },
        { id: "specs", path: broken },
        { id: "config", path: join(sources, "config.json") },
      ], runRoot);
    }, /run input snapshot of "specs" file entry "unreadable\.txt" .* is not readable as a regular file/);
    await chmod(join(broken, "unreadable.txt"), 0o600);
    await expect(lstat(join(runRoot, "data"))).rejects.toThrow();

    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    expect(snap.inputs.length).toBe(3);

    // a missing accepted output creates nothing at all
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 2);
    }, /state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();

    // a mid-copy activation failure removes exactly its own leaf tree: the
    // coder outputs are accepted while readable, then the unreadable file
    // breaks the accepted history's digest verification before the next
    // leaf is created
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH");
    const scratch = join(coderPrep.outputs_root, "scratch");
    await writeFile(join(scratch, "readable.txt"), "OK");
    await writeFile(join(scratch, "unreadable.txt"), "SECRET");
    const coderRecords = await acceptActivationOutputs(pipeline, coderPrep);
    await chmod(join(scratch, "unreadable.txt"), 0o000);

    // the scratch handoff is missing first: a partially constructed
    // history is rejected as incoherent before the leaf is created, then a
    // complete history fails during digest verification once the unreadable
    // file is present; both attempts remove exactly their own leaf tree
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        ...coderRecords.filter((record) => record.output === "patch"),
      ], "architect", 3);
    }, /accepted history activation index 1 of state "coder" is incomplete: the activation must record exactly the declared output ports \["patch","scratch"\], missing \["scratch"\]/);
    await expect(lstat(join(runRoot, "activations", "3-architect"))).rejects.toThrow();

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, coderRecords, "architect", 3);
    }, /accepted state output for "coder"\."scratch" at activation index 1 file entry "unreadable\.txt" .* is not readable as a regular file/);
    await chmod(join(scratch, "unreadable.txt"), 0o600);
    await expect(lstat(join(runRoot, "activations", "3-architect"))).rejects.toThrow();

    // run-level infrastructure and the successful activation persist
    expect((await lstat(join(runRoot, "activations"))).isDirectory()).toBe(true);
    expect((await lstat(join(runRoot, "data"))).isDirectory()).toBe(true);
    expect((await lstat(join(runRoot, "activations", "1-coder"))).isDirectory()).toBe(true);
  });
});

test("20. runtime APIs honor the loadPipelineV2 and snapshotRunInputs provenance boundaries", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const bindings = ALL_BINDINGS(sources);
    const snap = await snapshotRunInputs(pipeline, bindings, runRoot);

    // pipeline provenance
    await expectReject(async () => {
      await snapshotRunInputs(structuredClone(pipeline) as never, bindings, await makeRunRoot(join(dirs.root, "r-clone")));
    }, /run input binding requires the deep-frozen snapshot object returned by loadPipelineV2/);
    await expectReject(async () => {
      await prepareActivationData({} as never, snap, [], "coder", 1);
    }, /activation data preparation requires the deep-frozen snapshot object returned by loadPipelineV2/);

    // snapshot provenance: clones and forged objects are rejected before
    // any field is read
    const forgedMessage = /the operation requires the frozen run input snapshot object returned by a successful snapshotRunInputs call for the same trusted pipeline/;
    await expectReject(async () => {
      await prepareActivationData(pipeline, structuredClone(snap), [], "coder", 1);
    }, forgedMessage);

    const forgedFields = structuredClone(snap) as unknown as {
      run_root: string;
      inputs_root: string;
      project_root: string;
      inputs: Record<string, unknown>[];
    };
    forgedFields.inputs.forEach((entry) => {
      entry.snapshot_path = join(runRoot, "evil");
      entry.digest = "0".repeat(64);
      entry.protected = true;
    });
    forgedFields.inputs.push({ ...forgedFields.inputs[0] });
    await expectReject(async () => {
      await prepareActivationData(pipeline, forgedFields as never, [], "coder", 1);
    }, forgedMessage);

    // a snapshot created for a different pipeline object is rejected
    const otherPipeline = await loadPipelineV2(dirs.bundle);
    await expectReject(async () => {
      await prepareActivationData(otherPipeline, snap, [], "coder", 1);
    }, forgedMessage);

    // getters and Proxy traps are never invoked
    let getterInvoked = 0;
    const spySnapshot = new Proxy(snap, {
      get(target, property, receiver) {
        getterInvoked += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    await expectReject(async () => {
      await prepareActivationData(pipeline, spySnapshot as never, [], "coder", 1);
    }, forgedMessage);
    expect(getterInvoked).toBe(0);

    // every forged attempt was rejected before any filesystem mutation
    await expect(lstat(join(runRoot, "activations", "1-coder"))).rejects.toThrow();
    expect((await readdir(join(runRoot))).filter((name) => name !== "project" && name !== "data").length).toBe(0);

    // the genuine snapshot and pipeline keep working
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    expect(prep.state_id).toBe("coder");
    await snapshotRunInputs(pipeline, bindings, await makeRunRoot(join(dirs.root, "r-again")));
    expect(planActivationLayout(pipeline, "coder").state_id).toBe("coder");
  });
});

test("21. the runtime uses the loaded schema snapshot, never a schema path", async () => {
  await withRuntime(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    await rm(join(dirs.bundle, "schemas"), { recursive: true, force: true });
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    expect(snap.inputs.map((entry) => entry.id)).toEqual(["task", "specs", "config"]);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    expect(prep.input_ports.map((port) => port.id)).toEqual(["task", "specs", "config"]);
  });
});

test("22. the v1 path is unchanged and the production loader still rejects v2", async () => {
  const dirs = await makeBundleDirs();
  try {
    await writeFile(join(dirs.bundle, "pipeline.yaml"), V1_PIPELINE_YAML);
    await writeFile(join(dirs.bundle, "prompts", "execute.md"), "implementation agent\n");
    await writeFile(
      join(dirs.bundle, "schemas", "agent-result.schema.json"),
      JSON.stringify(STANDARD_AGENT_RESULT_SCHEMA),
    );
    const resolved = await loadPipeline(dirs.bundle);
    expect(resolved.schema_version).toBe(1);
    expect(resolved.entry_state).toBe("execute");
    const plan = planMultiStateExecution(resolved);
    expect(plan.profileNames).toEqual(["default"]);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }

  await withRuntime(async (bundleDirs) => {
    await expect(loadPipeline(bundleDirs.bundle)).rejects.toThrow(
      "pipeline schema version 2 is not executable yet",
    );
    expect(() => parsePipelineSpec(RUNTIME_YAML)).toThrow(
      "pipeline schema version 2 is not executable yet",
    );

    const workspace = join(bundleDirs.root, "workspace");
    await mkdir(workspace, { recursive: true });
    const state = join(bundleDirs.root, "state");
    await mkdir(state, { recursive: true });
    const configDir = join(bundleDirs.root, "config", "docker-helper");
    await mkdir(configDir, { recursive: true, mode: 0o700 });
    const credentialFile = join(configDir, "credential.token");
    await writeFile(credentialFile, `dhc_${"a".repeat(64)}\n`, { mode: 0o600 });
    const deps: AgentSmokeDeps = {
      cli: async () => {
        throw new Error("cli must not be called for a v2 pipeline");
      },
      fetchAuth: async () => {
        throw new Error("auth must not be reached for a v2 pipeline");
      },
      config: { socketPath: "/run/docker-helper/test.sock", credentialFile },
      stateDirPath: state,
      baseEnv: {},
    };
    const outcome = await runAgentSmoke(
      {
        workspace,
        configRoot: join(bundleDirs.root, "operator-config"),
        pipelineRoot: bundleDirs.bundle,
      },
      deps,
    );
    expect(outcome.ok).toBe(false);
    expect(outcome.exitCode).toBe(1);
    expect(outcome.runId).toBe("");
    expect(outcome.detail).toBe("pipeline schema version 2 is not executable yet");
  });
});

test("23. the v2 compile path is unchanged (JSON Schema validation untouched)", () => {
  const spec = parsePipelineV2Spec(RUNTIME_YAML);
  expect(spec.schema_version).toBe(2);
  expect(spec.inputs.map((input) => input.id)).toEqual(["task", "specs", "config"]);
  expect(spec.outputs).toEqual([]);
  const architect = spec.states[1];
  if (architect?.type !== "agent") {
    throw new Error("expected architect state");
  }
  expect(architect.inputs.map((port) => port.type)).toEqual(["file", "directory", "file"]);
  expect(architect.outputs.map((port) => port.type)).toEqual(["file", "json"]);
});

test("24. the shared project root persists across activations", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
    const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
    expect(first.project_root).toBe(second.project_root);
    expect(first.mounts[0]?.source).toBe(second.mounts[0]?.source);
    expect(first.mounts[0]?.target).toBe("/workspace");
    expect(first.mounts[0]?.read_only).toBe(false);
    expect(second.mounts[0]?.read_only).toBe(false);

    // a change to the shared project is visible to later activations
    // through the same mount source; the runtime copied nothing
    await writeFile(join(second.project_root, "notes.txt"), "second activation\n");
    expect(await readFile(join(first.project_root, "notes.txt"), "utf8")).toBe("second activation\n");
    expect((await readdir(first.data_root)).sort()).toEqual(["inputs", "outputs"]);
    expect((await readdir(second.data_root)).sort()).toEqual(["inputs", "outputs"]);

    // a failed preparation removes its leaf only; the project stays intact
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 3);
    }, /state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "3-architect"))).rejects.toThrow();
    expect(await readFile(join(runRoot, "project", "notes.txt"), "utf8")).toBe("second activation\n");
    expect((await lstat(join(runRoot, "project"))).isDirectory()).toBe(true);
  });
});

test("25. the whole accepted history is validated before the latest record is selected", async () => {
  await withRuntime(async (dirs, sources) => {
    const setup = async (name: string) => {
      const runRoot = await makeRunRoot(join(dirs.root, name));
      const pipeline = await loadPipelineV2(dirs.bundle);
      const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
      return { runRoot, pipeline, snap };
    };
    const architectLeaf = (runRoot: string, index: number) =>
      join(runRoot, "activations", `${index}-architect`);

    // a phantom index 1 is rejected even though index 2 of the same pair
    // is correct; the index-2 records are minted by real acceptance and the
    // index-1 set is complete, so coherence passes and resolution fails
    {
      const { runRoot, pipeline, snap } = await setup("r-phantom");
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [
          rec("coder", "patch", 1),
          rec("coder", "scratch", 1),
          ...secondRecords,
        ], "architect", 3);
      }, /accepted state output for "coder"\."patch" at activation index 1 activation leaf .*1-coder does not exist/);
      await expect(lstat(architectLeaf(runRoot, 3))).rejects.toThrow();
      expect(await readFile(join(second.outputs_root, "patch"), "utf8")).toBe("PATCH-2");
    }

    // a missing output at index 1 is rejected even though index 2 exists
    {
      const { runRoot, pipeline, snap } = await setup("r-missing-output");
      await prepareActivationData(pipeline, snap, [], "coder", 1);
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [
          rec("coder", "patch", 1),
          rec("coder", "scratch", 1),
          ...secondRecords,
        ], "architect", 3);
      }, /fixed output .*1-coder\/data\/outputs\/patch does not exist/);
      await expect(lstat(architectLeaf(runRoot, 3))).rejects.toThrow();
    }

    // a symlink output at index 1 is rejected even though index 2 is
    // correct; the external sentinel is untouched
    {
      const { runRoot, pipeline, snap } = await setup("r-symlink");
      const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
      const sentinel = join(dirs.root, "r-symlink-sentinel.txt");
      await writeFile(sentinel, "SENTINEL");
      await symlink(sentinel, join(first.outputs_root, "patch"));
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [
          rec("coder", "patch", 1),
          rec("coder", "scratch", 1),
          ...secondRecords,
        ], "architect", 3);
      }, /fixed output .*1-coder\/data\/outputs\/patch is a symbolic link/);
      await expect(lstat(architectLeaf(runRoot, 3))).rejects.toThrow();
      expect(await readFile(sentinel, "utf8")).toBe("SENTINEL");
      expect(await readFile(join(second.outputs_root, "patch"), "utf8")).toBe("PATCH-2");
    }

    // a wrong filesystem kind at index 1 is rejected even though index 2
    // is a correct regular file
    {
      const { runRoot, pipeline, snap } = await setup("r-kind");
      const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
      await mkdir(join(first.outputs_root, "patch"));
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [
          rec("coder", "patch", 1),
          rec("coder", "scratch", 1),
          ...secondRecords,
        ], "architect", 3);
      }, /fixed output .*1-coder\/data\/outputs\/patch is not a regular file/);
      await expect(lstat(architectLeaf(runRoot, 3))).rejects.toThrow();
    }

    // a substituted parent of an early record corrupts the whole history
    {
      const { runRoot, pipeline, snap } = await setup("r-parent");
      const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
      await writeFile(join(first.outputs_root, "patch"), "PATCH-1");
      const firstRecords = await acceptActivationOutputs(pipeline, first);
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      await rm(join(first.activation_root, "data"), { recursive: true, force: true });
      await symlink(join(dirs.root, "outside-data"), join(first.activation_root, "data"));
      await expectReject(async () => {
        await prepareActivationData(pipeline, snap, [
          ...firstRecords,
          ...secondRecords,
        ], "architect", 3);
      }, /activation data root .*1-coder\/data exists but is a symbolic link/);
      await expect(lstat(architectLeaf(runRoot, 3))).rejects.toThrow();
      expect(await readFile(join(second.outputs_root, "patch"), "utf8")).toBe("PATCH-2");
    }

    // two correct records resolve in any list order; the highest index wins
    {
      const { runRoot, pipeline, snap } = await setup("r-correct");
      const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
      await writeFile(join(first.outputs_root, "patch"), "PATCH-1");
      const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
      await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
      const firstRecords = await acceptActivationOutputs(pipeline, first);
      const secondRecords = await acceptActivationOutputs(pipeline, second);
      const ascending = await prepareActivationData(pipeline, snap, [
        ...firstRecords,
        ...secondRecords,
      ], "architect", 3);
      expect(await readFile(join(ascending.inputs_root, "patch"), "utf8")).toBe("PATCH-2");
      const descending = await prepareActivationData(pipeline, snap, [
        ...secondRecords,
        ...firstRecords,
      ], "architect", 4);
      expect(await readFile(join(descending.inputs_root, "patch"), "utf8")).toBe("PATCH-2");
    }

    // a non-list acceptedOutputs argument fails with a stable
    // PipelineError, never a stray TypeError from .map()
    {
      const { runRoot, pipeline, snap } = await setup("r-nonlist");
      for (const bad of [null, { state: "coder" }, "patch"] as unknown[]) {
        await expectReject(async () => {
          await prepareActivationData(pipeline, snap, bad as never, "coder", 1);
        }, /accepted state outputs must be a list/);
      }
      await expect(lstat(architectLeaf(runRoot, 1))).rejects.toThrow();
    }
  });
});
