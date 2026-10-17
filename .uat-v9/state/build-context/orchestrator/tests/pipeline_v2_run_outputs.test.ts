import { chmod, lstat, mkdir, mkdtemp, readdir, readFile, realpath, rename, rm, symlink, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import net from "node:net";
import { spawn } from "node:child_process";
import { expect, test } from "bun:test";
import { PipelineError, loadPipeline, parsePipelineSpec } from "../src/pipeline.ts";
import { loadPipelineV2, parsePipelineV2Spec, type PortType } from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  collectRunOutputs,
  prepareActivationData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type RunOutputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";

const BRIEF_BYTES = "BRIEF-LINE-1\nBRIEF-LINE-2\n";
// Irregular whitespace on purpose: publishing must reproduce the original
// JSON bytes, never a reserialization.
const CONFIG_BYTES = '{"ok":true,  "deep":{  "nested":null  }}';
const FACTS_BYTES = '{ "revision": 3, "actor": "me" }';
const FACTS_VALUE = { revision: 3, actor: "me" };

const CONFIG_SCHEMA = { type: "object", required: ["ok"] };
const FACTS_SCHEMA = {
  type: "object",
  required: ["revision", "actor"],
  additionalProperties: false,
  properties: {
    revision: { type: "integer", minimum: 1 },
    actor: { type: "string" },
  },
};

const MIXED_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: brief
    type: file
    protected: true
  - id: assets
    type: directory
    protected: false
  - id: config
    type: json
    protected: true
    schema: schemas/config.schema.json

outputs:
  - id: final-brief
    required: true
    source:
      pipeline_input: brief
  - id: final-assets
    required: false
    source:
      pipeline_input: assets
  - id: final-config
    required: true
    source:
      pipeline_input: config
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report
  - id: log
    required: false
    source:
      state_output:
        state: coder
        output: log
  - id: facts-copy
    required: true
    source:
      state_output:
        state: coder
        output: facts
  - id: summary
    required: true
    source:
      state_output:
        state: architect
        output: summary
  - id: report-alias
    required: false
    source:
      state_output:
        state: coder
        output: report

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: brief
        source:
          pipeline_input: brief
    outputs:
      - id: report
        type: file
      - id: log
        type: directory
      - id: facts
        type: json
        schema: schemas/facts.schema.json
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
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    outputs:
      - id: summary
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;

/** Run outputs sourced only from pipeline inputs; no state outputs needed. */
const INPUT_ONLY_YAML = MIXED_YAML.replace(
  `  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report
  - id: log
    required: false
    source:
      state_output:
        state: coder
        output: log
  - id: facts-copy
    required: true
    source:
      state_output:
        state: coder
        output: facts
  - id: summary
    required: true
    source:
      state_output:
        state: architect
        output: summary
  - id: report-alias
    required: false
    source:
      state_output:
        state: coder
        output: report
`,
  "",
);

/** A pipeline without any declared run outputs. */
const NO_OUTPUTS_YAML = INPUT_ONLY_YAML.replace(
  `outputs:
  - id: final-brief
    required: true
    source:
      pipeline_input: brief
  - id: final-assets
    required: false
    source:
      pipeline_input: assets
  - id: final-config
    required: true
    source:
      pipeline_input: config
`,
  "outputs: []\n",
);

/** Only optional state-output run outputs: everything may be absent. */
const OPTIONAL_ONLY_YAML = MIXED_YAML.replace(
  `outputs:
  - id: final-brief
    required: true
    source:
      pipeline_input: brief
  - id: final-assets
    required: false
    source:
      pipeline_input: assets
  - id: final-config
    required: true
    source:
      pipeline_input: config
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report
  - id: log
    required: false
    source:
      state_output:
        state: coder
        output: log
  - id: facts-copy
    required: true
    source:
      state_output:
        state: coder
        output: facts
  - id: summary
    required: true
    source:
      state_output:
        state: architect
        output: summary
  - id: report-alias
    required: false
    source:
      state_output:
        state: coder
        output: report
`,
  `outputs:
  - id: log
    required: false
    source:
      state_output:
        state: coder
        output: log
`,
);

/**
 * Two required state-output run outputs whose declaration order disagrees
 * with the alphabetical order: with both sources missing, the error must
 * name the first declared output (z9), never the alphabetically first one.
 */
const ORDER_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: brief
    type: file
    protected: true

outputs:
  - id: z9
    required: true
    source:
      state_output:
        state: coder
        output: beta
  - id: m1
    required: true
    source:
      state_output:
        state: coder
        output: alpha

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: brief
        source:
          pipeline_input: brief
    outputs:
      - id: alpha
        type: file
      - id: beta
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;

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
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-run-outputs-test-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  return { root, bundle };
}

async function writeBundle(dirs: BundleDirs, yaml: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "review the patch\n");
  await writeFile(
    join(dirs.bundle, "schemas", "config.schema.json"),
    JSON.stringify(CONFIG_SCHEMA),
  );
  await writeFile(
    join(dirs.bundle, "schemas", "facts.schema.json"),
    JSON.stringify(FACTS_SCHEMA),
  );
}

async function writeSources(root: string, configBytes: string = CONFIG_BYTES): Promise<string> {
  const dir = join(root, "userdata");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "brief.txt"), BRIEF_BYTES);
  await mkdir(join(dir, "assets", "sub"), { recursive: true });
  await writeFile(join(dir, "assets", "a.txt"), "A");
  await writeFile(join(dir, "assets", "sub", "b.txt"), "B");
  await writeFile(join(dir, "config.json"), configBytes);
  return dir;
}

async function makeRunRoot(root: string, name: string = "run"): Promise<string> {
  const runRoot = join(root, name);
  await mkdir(runRoot, { recursive: true });
  // the shared project directory of the whole run is created by the caller;
  // the runtime never creates, clears or copies it
  await mkdir(join(runRoot, "project"), { mode: 0o700 });
  await writeFile(join(runRoot, "project", "README.md"), "project seed\n");
  return runRoot;
}

const ALL_BINDINGS = (sources: string) => [
  { id: "brief", path: join(sources, "brief.txt") },
  { id: "assets", path: join(sources, "assets") },
  { id: "config", path: join(sources, "config.json") },
];

async function withPipeline(
  fn: (dirs: BundleDirs, sources: string) => Promise<void>,
  yaml: string = MIXED_YAML,
  configBytes: string = CONFIG_BYTES,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeBundle(dirs, yaml);
  const sources = await writeSources(dirs.root, configBytes);
  try {
    await fn(dirs, sources);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

async function expectReject(
  run: () => Promise<unknown> | unknown,
  message: RegExp | string,
  notContaining?: string,
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
  if (notContaining !== undefined) {
    expect(error.message).not.toContain(notContaining);
  }
}

async function makeFifo(path: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn("mkfifo", [path]);
    child.on("error", (cause) => reject(cause));
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

/** Run-root entries that the run infrastructure always owns. */
const PERMANENT_RUN_ROOT_ENTRIES = ["data", "project"];

/** Assert no run-output staging leftovers exist below the run root. */
async function expectNoStagingLeftovers(runRoot: string): Promise<void> {
  const names = (await readdir(runRoot)).sort();
  expect(names.filter((name) => name.startsWith(".tmp-"))).toEqual([]);
}

/** Assert no published outputs and no staging leftovers exist. */
async function expectNoOutputsAndNoStaging(runRoot: string): Promise<void> {
  const names = (await readdir(runRoot)).sort();
  expect(names.filter((name) => name === "outputs" || name.startsWith(".tmp-"))).toEqual([]);
  for (const permanent of PERMANENT_RUN_ROOT_ENTRIES) {
    expect(names).toContain(permanent);
  }
}

interface PlantedOutput {
  readonly output: string;
  readonly type: PortType;
  readonly fileBytes?: string;
  /** Relative POSIX path → content for `directory` outputs. */
  readonly tree?: Readonly<Record<string, string>>;
}

/**
 * Plant a complete, internally consistent accepted activation leaf at the
 * fixed orchestrator-derived location and mint the runner-owned records
 * (digests are computed over the actual bytes, so history verification
 * passes). Real records come from `acceptActivationOutputs`; planting is
 * the established test pattern for histories an acceptance run never saw.
 */
async function plantActivation(
  runRoot: string,
  stateId: string,
  activationIndex: number,
  outputs: readonly PlantedOutput[],
): Promise<AcceptedStateOutput[]> {
  const outputsRoot = join(
    runRoot,
    "activations",
    `${activationIndex}-${stateId}`,
    "data",
    "outputs",
  );
  await mkdir(outputsRoot, { recursive: true });
  const records: AcceptedStateOutput[] = [];
  for (const spec of outputs) {
    const target = join(outputsRoot, spec.output);
    if (spec.type === "directory") {
      for (const [relative, content] of Object.entries(spec.tree ?? {})) {
        await mkdir(join(target, dirname(relative)), { recursive: true });
        await writeFile(join(target, relative), content);
      }
    } else {
      await writeFile(target, spec.fileBytes ?? "");
    }
    records.push({
      state: stateId,
      output: spec.output,
      activation_index: activationIndex,
      digest: await acceptedOutputDigest(spec.type, target, `planted ${stateId}.${spec.output}`),
    });
  }
  return records;
}

/** Run one real coder and one real architect activation through acceptance. */
async function runCoderAndArchitect(
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>,
  snap: Awaited<ReturnType<typeof snapshotRunInputs>>,
): Promise<AcceptedStateOutput[]> {
  const coderPrep = await prepareActivationData(pipeline, snap, [], "coder", 1);
  await writeFile(join(coderPrep.outputs_root, "report"), "REPORT-1");
  await writeFile(join(coderPrep.outputs_root, "log", "work.txt"), "WORK-1");
  await writeFile(join(coderPrep.outputs_root, "facts"), FACTS_BYTES);
  const coderRecords = await acceptActivationOutputs(pipeline, coderPrep);

  const coderPrep2 = await prepareActivationData(pipeline, snap, coderRecords, "coder", 2);
  await writeFile(join(coderPrep2.outputs_root, "report"), "REPORT-2");
  await writeFile(join(coderPrep2.outputs_root, "log", "work.txt"), "WORK-2");
  await writeFile(join(coderPrep2.outputs_root, "facts"), FACTS_BYTES);
  const coderRecords2 = await acceptActivationOutputs(pipeline, coderPrep2);

  const architectPrep = await prepareActivationData(
    pipeline,
    snap,
    [...coderRecords, ...coderRecords2],
    "architect",
    3,
  );
  await writeFile(join(architectPrep.outputs_root, "summary"), "SUMMARY");
  const architectRecords = await acceptActivationOutputs(pipeline, architectPrep);
  return [...coderRecords, ...coderRecords2, ...architectRecords];
}

/** Independent transcription of the run-output digest framing for files. */
function referenceRunOutputFileDigest(bytes: Uint8Array): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(Buffer.from("pipeline-v2-run-output\0", "utf8"));
  hasher.update(Buffer.from("file\0", "utf8"));
  const length = Buffer.alloc(8);
  length.writeBigUInt64BE(BigInt(bytes.byteLength));
  hasher.update(length);
  hasher.update(bytes);
  return hasher.digest("hex");
}

function entryOf(snapshot: RunOutputsSnapshot, id: string) {
  const entry = snapshot.outputs.find((candidate) => candidate.id === id);
  if (entry === undefined) {
    throw new Error(`run output ${id} missing from snapshot`);
  }
  return entry;
}

function requirePresentDigest(entry: ReturnType<typeof entryOf>): string {
  if (!entry.present) {
    throw new Error(`expected a present entry for ${entry.id}`);
  }
  return entry.digest;
}

test("1. file/json/directory run outputs are published from pipeline inputs in declaration order", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const snapshot = await collectRunOutputs(pipeline, snap, []);
    const runRootCanonical = await realpath(runRoot);

    expect(snapshot.run_root).toBe(runRootCanonical);
    expect(snapshot.outputs_root).toBe(join(runRootCanonical, "outputs"));
    expect(snapshot.outputs.map((entry) => entry.id)).toEqual([
      "final-brief",
      "final-assets",
      "final-config",
    ]);
    expect(snapshot.outputs.map((entry) => entry.type)).toEqual(["file", "directory", "json"]);
    expect(snapshot.outputs.map((entry) => entry.required)).toEqual([true, false, true]);
    for (const entry of snapshot.outputs) {
      expect(entry.present).toBe(true);
      if (entry.present) {
        expect(entry.snapshot_path).toBe(join(runRootCanonical, "outputs", entry.id));
        expect(entry.digest).toMatch(/^[0-9a-f]{64}$/);
      }
    }

    // the published bytes are the original source bytes, byte for byte
    expect(await readFile(join(snapshot.outputs_root, "final-brief"), "utf8")).toBe(BRIEF_BYTES);
    expect(await readFile(join(snapshot.outputs_root, "final-config"), "utf8")).toBe(CONFIG_BYTES);
    expect(await readFile(join(snapshot.outputs_root, "final-assets", "sub", "b.txt"), "utf8")).toBe("B");

    // modes: directories 0700, files 0600
    expect((await lstat(snapshot.outputs_root)).mode & 0o777).toBe(0o700);
    expect((await lstat(join(snapshot.outputs_root, "final-brief"))).mode & 0o777).toBe(0o600);
    expect((await lstat(join(snapshot.outputs_root, "final-assets", "sub"))).mode & 0o777).toBe(0o700);
    expect((await lstat(join(snapshot.outputs_root, "final-assets", "a.txt"))).mode & 0o777).toBe(0o600);

    // the file digest matches an independent transcription of the
    // pipeline-v2-run-output framing
    expect(requirePresentDigest(entryOf(snapshot, "final-brief"))).toBe(
      referenceRunOutputFileDigest(Buffer.from(BRIEF_BYTES, "utf8")),
    );

    // the serialized snapshot never carries a user source path
    const serialized = JSON.stringify(snapshot);
    expect(serialized).not.toContain(sources);
    expect(serialized).toContain(snapshot.outputs_root);

    // the whole snapshot and every entry are deep-frozen
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.outputs)).toBe(true);
    for (const entry of snapshot.outputs) {
      expect(Object.isFrozen(entry)).toBe(true);
    }
  }, INPUT_ONLY_YAML);
});

test("2. run outputs from state outputs use the highest-activation-index winner; one source can feed several outputs", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const records = await runCoderAndArchitect(pipeline, snap);

    const snapshot = await collectRunOutputs(pipeline, snap, records);
    expect(snapshot.outputs.map((entry) => entry.id)).toEqual([
      "final-brief",
      "final-assets",
      "final-config",
      "report",
      "log",
      "facts-copy",
      "summary",
      "report-alias",
    ]);

    // the winner for coder.report is the highest activation index
    expect(await readFile(join(snapshot.outputs_root, "report"), "utf8")).toBe("REPORT-2");
    expect(await readFile(join(snapshot.outputs_root, "report-alias"), "utf8")).toBe("REPORT-2");
    expect(await readFile(join(snapshot.outputs_root, "log", "work.txt"), "utf8")).toBe("WORK-2");
    expect(await readFile(join(snapshot.outputs_root, "summary"), "utf8")).toBe("SUMMARY");
    expect(JSON.parse(await readFile(join(snapshot.outputs_root, "facts-copy"), "utf8")))
      .toEqual(FACTS_VALUE);

    // several run outputs from one source share the same digest
    expect(requirePresentDigest(entryOf(snapshot, "report"))).toBe(
      requirePresentDigest(entryOf(snapshot, "report-alias")),
    );

    // permuting the record list cannot change any published output
    const shuffled = [...records.slice(2), ...records.slice(0, 2)];
    const reshuffledRoot = await makeRunRoot(dirs.root, "run-reshuffled");
    const reshuffledSnap = await snapshotRunInputs(
      pipeline,
      ALL_BINDINGS(sources),
      reshuffledRoot,
    );
    await plantActivation(reshuffledRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    await plantActivation(reshuffledRoot, "coder", 2, [
      { output: "report", type: "file", fileBytes: "REPORT-2" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-2" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    await plantActivation(reshuffledRoot, "architect", 3, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const reshuffled = await collectRunOutputs(pipeline, reshuffledSnap, shuffled);
    expect(reshuffled.outputs.map((entry) => requirePresentDigest(entry))).toEqual(
      snapshot.outputs.map((entry) => requirePresentDigest(entry)),
    );
    expect(await readFile(join(reshuffled.outputs_root, "report"), "utf8")).toBe("REPORT-2");
  });
});

test("3. a missing required source fails the whole publication before anything is published", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run output "report" references required state output "coder"\."report" which has no accepted output yet/,
    );
    await expectNoOutputsAndNoStaging(runRoot);
    expect(await readFile(join(runRoot, "project", "README.md"), "utf8")).toBe("project seed\n");

    // with coder accepted but architect not run, the first declared missing
    // required output is summary (report and facts-copy are present; the
    // later declared optional report-alias is never reached)
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    await expectReject(
      () => collectRunOutputs(pipeline, snap, coderRecords),
      /run output "summary" references required state output "architect"\."summary" which has no accepted output yet/,
    );
    await expectNoOutputsAndNoStaging(runRoot);
  });
});

test("4. outputs are processed strictly in declaration order, not alphabetically", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "brief", path: join(sources, "brief.txt") }],
      runRoot,
    );

    // z9 is declared first and alphabetically last; both sources are
    // missing, so the error must name the first declared output
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run output "z9" references required state output "coder"\."beta" which has no accepted output yet/,
    );
    await expectNoOutputsAndNoStaging(runRoot);
  }, ORDER_YAML);
});

test("5. a missing optional source is recorded as absent without a filesystem entry; a present optional source is published", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const absent = await collectRunOutputs(pipeline, snap, []);
    expect(absent.outputs).toEqual([
      { id: "log", type: "directory", required: false, present: false },
    ]);
    // the outputs root itself is published; no entry exists for the absent
    // optional output
    expect((await lstat(absent.outputs_root)).isDirectory()).toBe(true);
    expect(await readdir(absent.outputs_root)).toEqual([]);
  }, OPTIONAL_ONLY_YAML);

  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);

    const present = await collectRunOutputs(pipeline, snap, coderRecords);
    const logEntry = entryOf(present, "log");
    if (!logEntry.present) {
      throw new Error("expected present log entry");
    }
    expect(logEntry.required).toBe(false);
    expect(await readFile(join(present.outputs_root, "log", "work.txt"), "utf8")).toBe("WORK-1");
    expect(logEntry.digest).toMatch(/^[0-9a-f]{64}$/);
  }, OPTIONAL_ONLY_YAML);
});

test("6. the whole accepted history is verified before the winner is selected; optionality never masks damage", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const firstRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const secondRecords = await plantActivation(runRoot, "coder", 2, [
      { output: "report", type: "file", fileBytes: "REPORT-2" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-2" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 3, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const history = [...firstRecords, ...secondRecords, ...architectRecords];

    // corrupting the OLD non-winning coder.report (activation 1) fails the
    // whole publication even though the activation-2 winner is intact
    const reportPath = join(runRoot, "activations", "1-coder", "data", "outputs", "report");
    await writeFile(reportPath, "REPORT-TAMPERED");
    await expectReject(
      () => collectRunOutputs(pipeline, snap, history),
      /accepted state output for "coder"\."report" at activation index 1 digest mismatch: recorded [0-9a-f]{64}, recomputed [0-9a-f]{64}/,
    );
    await expectNoOutputsAndNoStaging(runRoot);

    // optionality never masks damage: the run output "log" is optional, but
    // damaging its old non-winning accepted source still fails everything
    await writeFile(reportPath, "REPORT-1");
    await writeFile(
      join(runRoot, "activations", "1-coder", "data", "outputs", "log", "work.txt"),
      "TAMPERED-OPTIONAL",
    );
    await expectReject(
      () => collectRunOutputs(pipeline, snap, history),
      /accepted state output for "coder"\."log" at activation index 1 digest mismatch/,
    );
    await expectNoOutputsAndNoStaging(runRoot);

    // with the history intact, publication succeeds
    await writeFile(
      join(runRoot, "activations", "1-coder", "data", "outputs", "log", "work.txt"),
      "WORK-1",
    );
    const snapshot = await collectRunOutputs(pipeline, snap, history);
    expect(snapshot.outputs.every((entry) => entry.present)).toBe(true);
  });
});

test("7. an incomplete record set for one accepted activation is rejected as incoherent, before the next activation and before publication", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);

    // history missing the log record for activation 1
    const partial = coderRecords.filter((record) => record.output !== "log");
    const incomplete = /accepted history activation index 1 of state "coder" is incomplete: the activation must record exactly the declared output ports \["report","log","facts"\], missing \["log"\]/;

    // before the next activation
    await expectReject(
      () => prepareActivationData(pipeline, snap, partial, "architect", 2),
      incomplete,
    );
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();

    // before run-output publication
    await expectReject(() => collectRunOutputs(pipeline, snap, partial), incomplete);
    await expectNoOutputsAndNoStaging(runRoot);

    // the complete history works for both
    const next = await prepareActivationData(pipeline, snap, coderRecords, "architect", 2);
    expect(next.state_id).toBe("architect");
    await rm(join(runRoot, "activations", "2-architect"), { recursive: true, force: true });
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const published = await collectRunOutputs(pipeline, snap, [
      ...coderRecords,
      ...architectRecords,
    ]);
    expect(published.outputs.map((entry) => entry.id)).toContain("summary");
  });
});

test("8. a modified run-input snapshot is rejected before the activation leaf and before the run-output staging tree", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const briefPath = snap.inputs[0]?.snapshot_path ?? "";
    const original = await readFile(briefPath);

    // modified file snapshot
    await writeFile(briefPath, `${original.toString("utf8")}TAMPERED`);
    const digestMismatch = /run input snapshot of "brief" digest mismatch at .*: recorded [0-9a-f]{64}, recomputed [0-9a-f]{64}/;
    await expectReject(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      digestMismatch,
    );
    await expect(lstat(join(runRoot, "activations", "1-coder"))).rejects.toThrow();
    await expectReject(() => collectRunOutputs(pipeline, snap, []), digestMismatch);
    await expectNoOutputsAndNoStaging(runRoot);
    await writeFile(briefPath, original);

    // modified directory snapshot (an extra file changes names and content)
    const assetsPath = snap.inputs[1]?.snapshot_path ?? "";
    await writeFile(join(assetsPath, "extra.txt"), "EXTRA");
    await expectReject(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      /run input snapshot of "assets" digest mismatch/,
    );
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run input snapshot of "assets" digest mismatch/,
    );
    await rm(join(assetsPath, "extra.txt"), { force: true });

    // modified json snapshot
    const configPath = snap.inputs[2]?.snapshot_path ?? "";
    await writeFile(configPath, '{"ok":false}');
    await expectReject(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      /run input snapshot of "config" digest mismatch/,
    );
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run input snapshot of "config" digest mismatch/,
    );
    await writeFile(configPath, CONFIG_BYTES);

    // with the snapshot restored, both operations succeed
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    expect(prep.state_id).toBe("coder");
    await rm(join(runRoot, "activations", "1-coder"), { recursive: true, force: true });
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const snapshot = await collectRunOutputs(pipeline, snap, [...coderRecords, ...architectRecords]);
    expect(snapshot.outputs.every((entry) => entry.present)).toBe(true);
  });
});

test("9. symlink, FIFO and socket entries inside the run-input snapshot are rejected after creation", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const briefPath = snap.inputs[0]?.snapshot_path ?? "";

    // final component replaced by a symlink
    await rm(briefPath, { force: true });
    await symlink(join(sources, "brief.txt"), briefPath);
    await expectReject(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      /run input snapshot of "brief" snapshot object .* is a symbolic link/,
    );
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run input snapshot of "brief" snapshot object .* is a symbolic link/,
    );
    await rm(briefPath, { force: true });
    await writeFile(briefPath, BRIEF_BYTES);

    // FIFO inside a directory snapshot
    const assetsPath = snap.inputs[1]?.snapshot_path ?? "";
    await makeFifo(join(assetsPath, "pipe"));
    await expectReject(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      /run input snapshot of "assets" contains unsupported entry "pipe"/,
    );
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run input snapshot of "assets" contains unsupported entry "pipe"/,
    );
    await rm(join(assetsPath, "pipe"), { force: true });

    // socket inside a directory snapshot
    const socketPath = join(assetsPath, "sock");
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(socketPath, resolve));
    try {
      await expectReject(
        () => collectRunOutputs(pipeline, snap, []),
        /run input snapshot of "assets" contains unsupported entry "sock"/,
      );
    } finally {
      server.close();
    }
    await rm(socketPath, { force: true });

    // a relocated ancestor: the snapshot object no longer resolves to itself
    const dataReal = join(dirs.root, "data-real");
    await rename(join(runRoot, "data"), dataReal);
    await symlink(dataReal, join(runRoot, "data"));
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run input snapshot of "brief" snapshot object .* no longer resolves to itself/,
    );
    await expectNoOutputsAndNoStaging(runRoot);
    // restore for the final success check
    await rm(join(runRoot, "data"), { force: true });
    await rename(dataReal, join(runRoot, "data"));

    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const snapshot = await collectRunOutputs(pipeline, snap, [...coderRecords, ...architectRecords]);
    expect(snapshot.outputs.every((entry) => entry.present)).toBe(true);
  });
});

test("10. the original user binding paths are never re-read after the snapshot", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    // deleting the user sources entirely must not affect activation
    // preparation or run-output collection
    await rm(join(dirs.root, "userdata"), { recursive: true, force: true });
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    expect(await readFile(join(prep.inputs_root, "brief"), "utf8")).toBe(BRIEF_BYTES);
    await rm(join(runRoot, "activations", "1-coder"), { recursive: true, force: true });
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);
    const snapshot = await collectRunOutputs(pipeline, snap, [...coderRecords, ...architectRecords]);
    expect(await readFile(join(snapshot.outputs_root, "final-brief"), "utf8")).toBe(BRIEF_BYTES);
  });
});

test("11. a pre-existing object at the fixed outputs path fails closed and is never overwritten", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    for (const kind of ["file", "directory", "symlink"] as const) {
      const runRoot = await makeRunRoot(dirs.root, `run-${kind}`);
      const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
      const target = join(runRoot, "outputs");
      if (kind === "file") {
        await writeFile(target, "SENTINEL");
      } else if (kind === "directory") {
        await mkdir(target);
        await writeFile(join(target, "sentinel.txt"), "SENTINEL");
      } else {
        await symlink(dirs.root, target);
      }

      await expectReject(
        () => collectRunOutputs(pipeline, snap, []),
        kind === "directory"
          ? /run outputs root .* already exists, found an existing directory/
          : kind === "file"
            ? /run outputs root .* already exists, found an existing regular file/
            : /run outputs root .* already exists, found a symbolic link/,
      );
      // the run-output staging tree was never created next to the sentinel
      await expectNoStagingLeftovers(runRoot);

      // the sentinel was not modified
      if (kind === "file") {
        expect(await readFile(target, "utf8")).toBe("SENTINEL");
      } else if (kind === "directory") {
        expect(await readFile(join(target, "sentinel.txt"), "utf8")).toBe("SENTINEL");
      } else {
        expect((await lstat(target)).isSymbolicLink()).toBe(true);
      }
    }
  }, INPUT_ONLY_YAML);
});

test("12. a later output error never publishes the earlier staged outputs; staging cleanup touches only the staging tree", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);

    // the first six outputs stage fine; the required summary then fails
    await expectReject(
      () => collectRunOutputs(pipeline, snap, coderRecords),
      /run output "summary" references required state output "architect"\."summary" which has no accepted output yet/,
    );
    await expectNoOutputsAndNoStaging(runRoot);

    // nothing staged leaked anywhere: the run root holds exactly the
    // permanent infrastructure
    expect((await readdir(runRoot)).sort()).toEqual(["activations", "data", "project"]);
    expect(await readFile(join(runRoot, "project", "README.md"), "utf8")).toBe("project seed\n");
    expect((await lstat(join(runRoot, "data", "inputs", "brief"))).isFile()).toBe(true);
    expect((await lstat(join(runRoot, "activations", "1-coder"))).isDirectory()).toBe(true);
  });
});

test("13. json run outputs are re-parsed and validated against the compiled schema with content-free diagnostics", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: '{"revision": "5", "actor": "x"}' },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);

    // schema violation: the diagnostic names the instance path only
    await expectReject(
      () => collectRunOutputs(pipeline, snap, [...coderRecords, ...architectRecords]),
      /run output "facts-copy" source state output "coder"\."facts" does not conform to its JSON schema: \/revision: must be integer/,
      '"5"',
    );
    await expectNoOutputsAndNoStaging(runRoot);

    // malformed JSON: one stable, content-free diagnostic that never
    // carries the parser message, the offending token or any fragment
    const factsPath = join(runRoot, "activations", "1-coder", "data", "outputs", "facts");
    await writeFile(factsPath, '{"revision": 2, "actor": "AFTER_SECRET_CANARY",}');
    const factsRecord = (digest: string): AcceptedStateOutput => ({
      state: "coder",
      output: "facts",
      activation_index: 1,
      digest,
    });
    const malformedHistory = [
      ...coderRecords.filter((record) => record.output !== "facts"),
      factsRecord(await acceptedOutputDigest("json", factsPath, "planted malformed facts")),
      ...architectRecords,
    ];
    await expectReject(
      () => collectRunOutputs(pipeline, snap, malformedHistory),
      `run output "facts-copy" source state output "coder"."facts" ${factsPath} is not valid JSON`,
      "AFTER_SECRET_CANARY",
    );
    await expectNoOutputsAndNoStaging(runRoot);

    // valid json publishes the original bytes, never a reserialization
    await writeFile(factsPath, FACTS_BYTES);
    const validHistory = [
      ...coderRecords.filter((record) => record.output !== "facts"),
      factsRecord(await acceptedOutputDigest("json", factsPath, "planted valid facts")),
      ...architectRecords,
    ];
    const snapshot = await collectRunOutputs(pipeline, snap, validHistory);
    expect(await readFile(join(snapshot.outputs_root, "facts-copy"), "utf8")).toBe(FACTS_BYTES);
    expect(JSON.parse(await readFile(join(snapshot.outputs_root, "facts-copy"), "utf8")))
      .toEqual(FACTS_VALUE);
  });
});

test("14. json run outputs sourced from pipeline inputs are re-validated and keep the original bytes", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const snapshot = await collectRunOutputs(pipeline, snap, []);
    const published = await readFile(join(snapshot.outputs_root, "final-config"), "utf8");
    expect(published).toBe(CONFIG_BYTES);
    expect(JSON.parse(published)).toEqual({ ok: true, deep: { nested: null } });
    // the run-output digest never equals the accepted-output digest of the
    // same bytes: the domains differ
    expect(requirePresentDigest(entryOf(snapshot, "final-config"))).not.toBe(
      await acceptedOutputDigest(
        "json",
        join(snapshot.outputs_root, "final-config"),
        "accepted domain",
      ),
    );
  }, INPUT_ONLY_YAML);
});

test("15. the run-output digest domain is distinct from the input and accepted-output domains", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const snapshot = await collectRunOutputs(pipeline, snap, []);
    const briefDigest = requirePresentDigest(entryOf(snapshot, "final-brief"));

    // the same bytes under the three domains never agree
    const inputDigest = snap.inputs.find((entry) => entry.id === "brief")?.digest ?? "";
    const acceptedDigest = await acceptedOutputDigest(
      "file",
      join(snapshot.outputs_root, "final-brief"),
      "accepted domain",
    );
    expect(inputDigest).toMatch(/^[0-9a-f]{64}$/);
    expect(acceptedDigest).toMatch(/^[0-9a-f]{64}$/);
    expect(briefDigest).not.toBe(inputDigest);
    expect(briefDigest).not.toBe(acceptedDigest);
    expect(inputDigest).not.toBe(acceptedDigest);

    // and the published digest matches the independent framing transcription
    expect(briefDigest).toBe(
      referenceRunOutputFileDigest(await readFile(join(snapshot.outputs_root, "final-brief"))),
    );
  }, INPUT_ONLY_YAML);
});

test("16. run-output digests are location- and timestamp-independent and react to bytes, names, empty directories and kinds", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const briefBinding = (briefPath: string) => [
      { id: "brief", path: briefPath },
      { id: "assets", path: join(sources, "assets") },
      { id: "config", path: join(sources, "config.json") },
    ];

    // identical bytes at different host paths with different timestamps
    // digest identically
    const elsewhere = join(dirs.root, "elsewhere");
    await mkdir(elsewhere, { recursive: true });
    await writeFile(join(elsewhere, "brief.txt"), BRIEF_BYTES);
    const epoch = new Date(0);
    await utimes(join(elsewhere, "brief.txt"), epoch, epoch);
    const outA = await collectRunOutputs(
      pipeline,
      await snapshotRunInputs(pipeline, briefBinding(join(elsewhere, "brief.txt")), await makeRunRoot(dirs.root, "run-a")),
      [],
    );
    const outB = await collectRunOutputs(
      pipeline,
      await snapshotRunInputs(pipeline, briefBinding(join(sources, "brief.txt")), await makeRunRoot(dirs.root, "run-b")),
      [],
    );
    expect(requirePresentDigest(entryOf(outA, "final-brief"))).toBe(
      requirePresentDigest(entryOf(outB, "final-brief")),
    );

    // changed bytes change the digest
    await writeFile(join(elsewhere, "brief.txt"), `${BRIEF_BYTES} `);
    const outC = await collectRunOutputs(
      pipeline,
      await snapshotRunInputs(pipeline, briefBinding(join(elsewhere, "brief.txt")), await makeRunRoot(dirs.root, "run-c")),
      [],
    );
    expect(requirePresentDigest(entryOf(outC, "final-brief"))).not.toBe(
      requirePresentDigest(entryOf(outA, "final-brief")),
    );
  }, INPUT_ONLY_YAML);

  // directory digests: names, empty directories and entry kinds participate
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const assetsVariant = async (name: string, mutate: (dir: string) => Promise<void>) => {
      const dir = join(dirs.root, name);
      await mkdir(join(dir, "sub"), { recursive: true });
      await writeFile(join(dir, "a.txt"), "A");
      await writeFile(join(dir, "sub", "b.txt"), "B");
      await mutate(dir);
      const runRoot = await makeRunRoot(dirs.root, `run-${name}`);
      const snap = await snapshotRunInputs(pipeline, [
        { id: "brief", path: join(sources, "brief.txt") },
        { id: "assets", path: dir },
        { id: "config", path: join(sources, "config.json") },
      ], runRoot);
      const snapshot = await collectRunOutputs(pipeline, snap, []);
      return requirePresentDigest(entryOf(snapshot, "final-assets"));
    };

    const base = await assetsVariant("assets-base", async () => {});
    // changed file bytes
    const bytes = await assetsVariant("assets-bytes", async (dir) => {
      await writeFile(join(dir, "a.txt"), "A ");
    });
    // renamed entry
    const renamed = await assetsVariant("assets-renamed", async (dir) => {
      await rm(join(dir, "a.txt"), { force: true });
      await writeFile(join(dir, "z.txt"), "A");
    });
    // added empty directory
    const emptyDir = await assetsVariant("assets-empty-dir", async (dir) => {
      await mkdir(join(dir, "empty-sub"));
    });
    // file entry turned into a directory entry
    const kind = await assetsVariant("assets-kind", async (dir) => {
      await rm(join(dir, "a.txt"), { force: true });
      await mkdir(join(dir, "a.txt"));
    });
    expect(bytes).not.toBe(base);
    expect(renamed).not.toBe(base);
    expect(emptyDir).not.toBe(base);
    expect(kind).not.toBe(base);
  }, INPUT_ONLY_YAML);
});

test("17. repeated collection after a successful publication is rejected and overwrites nothing", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const coderRecords = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const architectRecords = await plantActivation(runRoot, "architect", 2, [
      { output: "summary", type: "file", fileBytes: "SUMMARY" },
    ]);

    const snapshot = await collectRunOutputs(pipeline, snap, [...coderRecords, ...architectRecords]);
    expect(snapshot.outputs.every((entry) => entry.present)).toBe(true);

    // tamper with a published output and try again: the second call is
    // rejected at the pre-existing outputs root and the tampered bytes stay
    await writeFile(join(snapshot.outputs_root, "report"), "TAMPERED-AFTER-PUBLISH");
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run outputs root .* already exists, found an existing directory/,
    );
    expect(await readFile(join(snapshot.outputs_root, "report"), "utf8")).toBe(
      "TAMPERED-AFTER-PUBLISH",
    );
    expect(await readFile(join(snapshot.outputs_root, "summary"), "utf8")).toBe("SUMMARY");
  });
});

test("18. the snapshot entries are frozen with exact per-branch fields", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    // present entries carry exactly the six documented fields
    const presentRoot = await makeRunRoot(dirs.root);
    const presentSnap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), presentRoot);
    const coderRecords = await plantActivation(presentRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT-1" },
      { output: "log", type: "directory", tree: { "work.txt": "WORK-1" } },
      { output: "facts", type: "json", fileBytes: FACTS_BYTES },
    ]);
    const present = await collectRunOutputs(pipeline, presentSnap, coderRecords);
    for (const entry of present.outputs) {
      expect(Object.keys(entry).sort()).toEqual([
        "digest",
        "id",
        "present",
        "required",
        "snapshot_path",
        "type",
      ]);
      expect(Object.isFrozen(entry)).toBe(true);
    }
    expect(Object.isFrozen(present)).toBe(true);
    expect(Object.isFrozen(present.outputs)).toBe(true);
    const first = present.outputs[0];
    if (first === undefined || !first.present) {
      throw new Error("expected a present first entry");
    }
    expect(() => {
      (first as unknown as { digest: string }).digest = "changed";
    }).toThrow();
    expect(() => {
      (present.outputs as unknown as { push(value: unknown): number }).push(first);
    }).toThrow();

    // absent entries carry exactly the four documented fields and exist
    // only for optional outputs
    const absentRoot = await makeRunRoot(dirs.root, "run-absent");
    const absentSnap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), absentRoot);
    const absent = await collectRunOutputs(pipeline, absentSnap, []);
    const logEntry = entryOf(absent, "log");
    expect(logEntry.present).toBe(false);
    expect(Object.keys(logEntry).sort()).toEqual(["id", "present", "required", "type"]);
    expect(logEntry.required).toBe(false);
    expect(Object.isFrozen(logEntry)).toBe(true);
  }, OPTIONAL_ONLY_YAML);
});

test("19. a pipeline without declared outputs still publishes the empty outputs root exactly once", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);

    const snapshot = await collectRunOutputs(pipeline, snap, []);
    expect((await lstat(snapshot.outputs_root)).isDirectory()).toBe(true);
    expect(await readdir(snapshot.outputs_root)).toEqual([]);
    expect(snapshot.outputs).toEqual([]);
    // a second call fails closed on the now-existing outputs root
    await expectReject(
      () => collectRunOutputs(pipeline, snap, []),
      /run outputs root .* already exists, found an existing directory/,
    );
  }, NO_OUTPUTS_YAML);
});

test("20. v1 regression and the production v2 rejection are unchanged", async () => {
  const dirs = await makeBundleDirs();
  try {
    await writeFile(join(dirs.bundle, "pipeline.yaml"), V1_PIPELINE_YAML);
    await writeFile(join(dirs.bundle, "prompts", "execute.md"), "implementation agent\n");
    await writeFile(
      join(dirs.bundle, "schemas", "agent-result.schema.json"),
      JSON.stringify({ type: "object" }),
    );
    const resolved = await loadPipeline(dirs.bundle);
    expect(resolved.schema_version).toBe(1);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }

  await withPipeline(async (bundleDirs) => {
    await expect(loadPipeline(bundleDirs.bundle)).rejects.toThrow(
      "pipeline schema version 2 is not executable yet",
    );
    expect(() => parsePipelineSpec(MIXED_YAML)).toThrow(
      "pipeline schema version 2 is not executable yet",
    );
    // the v2 compile path is untouched: run outputs keep declaration order
    // and derived types
    const spec = parsePipelineV2Spec(MIXED_YAML);
    expect(spec.outputs.map((output) => output.id)).toEqual([
      "final-brief",
      "final-assets",
      "final-config",
      "report",
      "log",
      "facts-copy",
      "summary",
      "report-alias",
    ]);
    expect(spec.outputs.map((output) => output.type)).toEqual([
      "file",
      "directory",
      "json",
      "file",
      "directory",
      "json",
      "file",
      "file",
    ]);

    // the collection API takes exactly the three trusted arguments: no
    // terminal id, no user paths, no types, no mounts, no Docker options
    expect(collectRunOutputs.length).toBe(3);
  });
});

test("21. unreadable snapshot bytes fail the run-input verification with a readable diagnostic", async () => {
  await withPipeline(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(sources), runRoot);
    const secretPath = join(snap.inputs[1]?.snapshot_path ?? "", "a.txt");
    await chmod(secretPath, 0o000);
    try {
      await expectReject(
        () => collectRunOutputs(pipeline, snap, []),
        /run input snapshot of "assets" file entry "a\.txt" .* is not readable as a regular file/,
      );
      await expectNoOutputsAndNoStaging(runRoot);
    } finally {
      await chmod(secretPath, 0o644);
    }
  });
});
