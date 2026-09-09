import { chmod, lstat, mkdir, mkdtemp, readFile, realpath, rm, symlink, utimes, writeFile } from "node:fs/promises";
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
  prepareActivationData,
  snapshotRunInputs,
  type RunInputsSnapshot,
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

async function snapshotAll(
  bundle: string,
  sources: string,
  runRoot: string,
): Promise<RunInputsSnapshot> {
  const pipeline = await loadPipelineV2(bundle);
  return snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
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

    const scalar = "null";
    await writeFile(join(sources, "config.json"), scalar);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const config = snap.inputs.find((entry) => entry.id === "config");
    expect(await readFile(config?.snapshot_path ?? "", "utf8")).toBe(scalar);
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
    const snap = await snapshotRunInputs(
      await loadPipelineV2(dirs.bundle),
      ALL_BINDINGS(sources),
      runRoot,
    );
    const prep = await prepareActivationData(
      await loadPipelineV2(dirs.bundle),
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

    const architectPrep = await prepareActivationData(pipeline, snap, [
      { state: "coder", output: "patch", type: "file", path: join(coderPrep.outputs_root, "patch") },
      {
        state: "coder",
        output: "scratch",
        type: "directory",
        path: join(coderPrep.outputs_root, "scratch"),
      },
    ], "architect", 2);

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

test("10. the last accepted activation wins for the same state output", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const first = await prepareActivationData(pipeline, snap, [], "coder", 1);
    const second = await prepareActivationData(pipeline, snap, [], "coder", 2);
    await writeFile(join(first.outputs_root, "patch"), "PATCH-1");
    await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
    const scratchEntry = (prep: { outputs_root: string }) => ({
      state: "coder",
      output: "scratch",
      type: "directory",
      path: join(prep.outputs_root, "scratch"),
    });

    const latest = await prepareActivationData(pipeline, snap, [
      { state: "coder", output: "patch", type: "file", path: join(first.outputs_root, "patch") },
      { state: "coder", output: "patch", type: "file", path: join(second.outputs_root, "patch") },
      scratchEntry(first),
    ], "architect", 3);
    expect(await readFile(join(latest.inputs_root, "patch"), "utf8")).toBe("PATCH-2");

    const earliest = await prepareActivationData(pipeline, snap, [
      { state: "coder", output: "patch", type: "file", path: join(first.outputs_root, "patch") },
      { state: "coder", output: "patch", type: "file", path: join(second.outputs_root, "patch") },
      { state: "coder", output: "patch", type: "file", path: join(first.outputs_root, "patch") },
      scratchEntry(first),
    ], "architect", 4);
    expect(await readFile(join(earliest.inputs_root, "patch"), "utf8")).toBe("PATCH-1");
  });
});

test("11. missing, forward and first-visit self state outputs are rejected", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    // missing: no accepted outputs at all
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 1);
    }, /input port "patch" of agent state "architect" references state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "1-architect"))).rejects.toThrow();

    // forward: a later state's output is accepted while an earlier one is not
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH-1");
    const fabricatedReport = join(runRoot, "fabricated", "report");
    await mkdir(join(runRoot, "fabricated"), { recursive: true });
    await writeFile(fabricatedReport, "REPORT");
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "architect", output: "report", type: "file", path: fabricatedReport },
        {
          state: "coder",
          output: "scratch",
          type: "directory",
          path: join(coderPrep.outputs_root, "scratch"),
        },
      ], "architect", 2);
    }, /input port "patch" of agent state "architect" references state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
  });

  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    // first visit of a self-referencing input port fails
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH-1");
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: join(coderPrep.outputs_root, "patch") },
        {
          state: "coder",
          output: "scratch",
          type: "directory",
          path: join(coderPrep.outputs_root, "scratch"),
        },
      ], "architect", 2);
    }, /input port "facts" of agent state "architect" references state output "architect"\."facts" which has no accepted output yet/);

    // a revisit resolves the self-reference from the accepted list
    const factsPath = join(runRoot, "fabricated", "facts");
    await mkdir(join(runRoot, "fabricated"), { recursive: true });
    await writeFile(factsPath, JSON.stringify({ revision: 7 }));
    const revisit = await prepareActivationData(pipeline, snap, [
      { state: "coder", output: "patch", type: "file", path: join(coderPrep.outputs_root, "patch") },
      {
        state: "coder",
        output: "scratch",
        type: "directory",
        path: join(coderPrep.outputs_root, "scratch"),
      },
      { state: "architect", output: "facts", type: "json", path: factsPath },
    ], "architect", 3);
    expect(await readFile(join(revisit.inputs_root, "facts"), "utf8")).toBe(
      JSON.stringify({ revision: 7 }),
    );
  }, SELFREF_YAML);
});

test("12. the accepted-output list is validated: declaration, type, kind, existence", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(coderPrep.outputs_root, "patch"), "PATCH");
    const patchPath = join(coderPrep.outputs_root, "patch");

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "nowhere", output: "patch", type: "file", path: patchPath },
      ], "architect", 2);
    }, /accepted state output 0 references state "nowhere" which is not a declared agent state/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "done", output: "patch", type: "file", path: patchPath },
      ], "architect", 2);
    }, /accepted state output 0 references state "done" which is not a declared agent state/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "nope", type: "file", path: patchPath },
      ], "architect", 2);
    }, /references output "nope" which is not declared by state "coder"/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "directory", path: patchPath },
      ], "architect", 2);
    }, /accepted state output 0 declares type "directory" but state "coder" declares output port "patch" as "file"/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: join(coderPrep.outputs_root, "scratch") },
      ], "architect", 2);
    }, /accepted output .* is not a regular file/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: join(coderPrep.outputs_root, "missing") },
      ], "architect", 2);
    }, /accepted output .* does not exist/);

    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: "relative/patch" },
      ], "architect", 2);
    }, /path must be an absolute path/);
  });
});

test("13. activation leaf and path reuse are rejected fail-closed", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    await prepareActivationData(pipeline, snap, [], "coder", 1);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "coder", 1);
    }, /activation leaf .* already exists, found an existing directory/);

    // the same index is allowed for a different state with its own leaf
    await writeFile(join(runRoot, "activations", "1-coder", "data", "outputs", "patch"), "PATCH");
    const architectSameIndex = await prepareActivationData(pipeline, snap, [
      { state: "coder", output: "patch", type: "file", path: join(runRoot, "activations", "1-coder", "data", "outputs", "patch") },
      {
        state: "coder",
        output: "scratch",
        type: "directory",
        path: join(runRoot, "activations", "1-coder", "data", "outputs", "scratch"),
      },
    ], "architect", 1);
    expect(architectSameIndex.activation_root).toBe(join(runRoot, "activations", "1-architect"));

    for (const kind of ["file", "dir", "symlink"] as const) {
      const runRootX = await makeRunRoot(join(dirs.root, `r-leaf-${kind}`));
      const snapX = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRootX);
      const leaf = join(runRootX, "activations", "1-coder");
      if (kind === "dir") {
        await mkdir(leaf, { recursive: true });
      } else {
        await mkdir(join(runRootX, "activations"), { recursive: true });
        if (kind === "file") {
          await writeFile(leaf, "not a directory");
        } else {
          await symlink(dirs.root, leaf);
        }
      }
      await expectReject(async () => {
        await prepareActivationData(pipeline, snapX, [], "coder", 1);
      }, /activation leaf .* already exists/);
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

test("15. escape through the accepted-output list is rejected before anything is created", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    await prepareActivationData(pipeline, snap, [], "coder", 1);

    const outside = join(dirs.root, "outside-report.txt");
    await writeFile(outside, "ESCAPED");
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: outside },
      ], "architect", 2);
    }, /accepted output .*?resolves outside the canonical run root/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
    expect(await readFile(outside, "utf8")).toBe("ESCAPED");

    const escapeLink = join(runRoot, "activations", "1-coder", "data", "outputs", "patch");
    await symlink(outside, escapeLink);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        { state: "coder", output: "patch", type: "file", path: escapeLink },
      ], "architect", 3);
    }, /accepted output .* is a symbolic link/);
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

    await prepareActivationData(pipeline, snap, [], "coder", 1);

    // a missing accepted output creates nothing at all
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [], "architect", 2);
    }, /state output "coder"\."patch" which has no accepted output yet/);
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();

    // a mid-copy activation failure removes exactly its own leaf tree
    await writeFile(join(runRoot, "activations", "1-coder", "data", "outputs", "patch"), "PATCH");
    const scratch = join(runRoot, "fabricated", "scratch");
    await mkdir(scratch, { recursive: true });
    await writeFile(join(scratch, "readable.txt"), "OK");
    await writeFile(join(scratch, "unreadable.txt"), "SECRET");
    await chmod(join(scratch, "unreadable.txt"), 0o000);
    await expectReject(async () => {
      await prepareActivationData(pipeline, snap, [
        {
          state: "coder",
          output: "patch",
          type: "file",
          path: join(runRoot, "activations", "1-coder", "data", "outputs", "patch"),
        },
        { state: "coder", output: "scratch", type: "directory", path: scratch },
      ], "architect", 3);
    }, /input port "scratch" of agent state "architect" file entry "unreadable\.txt" .* is not readable as a regular file/);
    await chmod(join(scratch, "unreadable.txt"), 0o600);
    await expect(lstat(join(runRoot, "activations", "3-architect"))).rejects.toThrow();

    // run-level infrastructure and the successful activation persist
    expect((await lstat(join(runRoot, "activations"))).isDirectory()).toBe(true);
    expect((await lstat(join(runRoot, "data"))).isDirectory()).toBe(true);
    expect((await lstat(join(runRoot, "activations", "1-coder"))).isDirectory()).toBe(true);
  });
});

test("20. runtime APIs honor the loadPipelineV2 provenance boundary", async () => {
  await withRuntime(async (dirs, sources) => {
    const runRoot = await makeRunRoot(dirs.root);
    const pipeline = await loadPipelineV2(dirs.bundle);
    const bindings = ALL_BINDINGS(sources);
    const snap = await snapshotRunInputs(pipeline, bindings, runRoot);

    const clone = structuredClone(pipeline);
    await expectReject(async () => {
      await snapshotRunInputs(clone as never, bindings, await makeRunRoot(join(dirs.root, "r-clone")));
    }, /run input binding requires the deep-frozen snapshot object returned by loadPipelineV2/);
    await expectReject(async () => {
      await prepareActivationData({} as never, snap, [], "coder", 1);
    }, /activation data preparation requires the deep-frozen snapshot object returned by loadPipelineV2/);
    await expectReject(async () => {
      await prepareActivationData(structuredClone(pipeline) as never, snap, [], "coder", 1);
    }, /activation data preparation requires the deep-frozen snapshot object returned by loadPipelineV2/);

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
