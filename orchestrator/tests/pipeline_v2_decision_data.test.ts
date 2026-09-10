import { lstat, mkdir, mkdtemp, readdir, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  loadPipelineV2,
  type PipelineDecisionStateResult,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  evaluateDecisionStateFromData,
  prepareActivationData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";

/**
 * Focused tests for the pure host-side decision data adapter
 * `evaluateDecisionStateFromData`: it resolves a declared v2 decision
 * state's single `json` input through the existing data plane (verified
 * run-input snapshot and fully verified accepted history), parses and
 * validates the JSON, and calls the existing `evaluatePipelineDecisionState`
 * — without creating any decision activation leaf or touching the
 * filesystem. The production runner is not wired: it still rejects schema
 * v2 before Launcher auth and before any Session.
 */

const FACTS_SCHEMA = {
  type: "object",
  required: ["f1", "f2"],
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

/** Empty Draft 2020-12 schema: admits every JSON value. */
const LOOSE_SCHEMA = {};

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations:
  - id: r1
    assert:
      not:
        all:
          - {fact: f1, equals: true}
          - {fact: f2, equals: true}
constraints:
  - id: c1
    when: {fact: f1, equals: true}
    forbid: [beta]
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const DECISION_TRANSITIONS = `      - outcome: alpha
        to: done
      - outcome: beta
        to: rework
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

/** Entry decision state fed by a pipeline json input. */
const PIPELINE_FROM_PIPELINE_INPUT = `
schema_version: 2
entry_state: check
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

states:
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${DECISION_TRANSITIONS}
  - id: rework
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** Two run inputs: only one feeds the decision state. */
const PIPELINE_TWO_INPUTS = PIPELINE_FROM_PIPELINE_INPUT.replace(
  `inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json
`,
  `inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json
  - id: notes
    type: file
    protected: false
`,
);

/** The decision input port sources the accepted json output of an agent. */
const PIPELINE_FROM_STATE_OUTPUT = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []

outputs:
  - id: facts_digest
    required: false
    source:
      state_output:
        state: coder
        output: facts

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/facts.schema.json
      - id: report
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    transitions:
${DECISION_TRANSITIONS}
  - id: rework
    type: agent
    profile: coder
    prompt: prompts/rework.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** Pipeline-input variant whose schema admits any JSON value. */
const PIPELINE_FROM_PIPELINE_INPUT_LOOSE = PIPELINE_FROM_PIPELINE_INPUT.replace(
  "schema: schemas/facts.schema.json",
  "schema: schemas/loose.schema.json",
);

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-decision-data-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  return { root, bundle };
}

async function writeDecisionBundle(
  dirs: BundleDirs,
  yaml: string,
  loose = false,
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "rework.md"), "rework the task\n");
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  if (loose) {
    await writeFile(join(dirs.bundle, "schemas", "loose.schema.json"), JSON.stringify(LOOSE_SCHEMA));
  }
  await writeFile(join(dirs.bundle, "decisions", "model.yaml"), MODEL_YAML);
}

async function writeSources(root: string): Promise<string> {
  const dir = join(root, "userdata");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "facts.json"), JSON.stringify({ f1: true, f2: false }));
  await writeFile(join(dir, "notes.txt"), "NOTES-BODY");
  return dir;
}

async function makeRunRoot(root: string): Promise<string> {
  const runRoot = join(root, "run");
  await mkdir(runRoot, { recursive: true });
  await mkdir(join(runRoot, "project"), { mode: 0o700 });
  return runRoot;
}

async function withDecision(
  fn: (dirs: BundleDirs, sources: string) => Promise<void>,
  yaml: string = PIPELINE_FROM_PIPELINE_INPUT,
  loose = false,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeDecisionBundle(dirs, yaml, loose);
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

interface PlantedOutput {
  readonly output: string;
  readonly type: "file" | "json";
  readonly fileBytes?: string;
}

/**
 * Plant a complete, internally consistent accepted activation leaf at the
 * fixed orchestrator-derived location and mint runner-owned records with
 * digests computed over the actual bytes, so history verification passes.
 * Real records come from `acceptActivationOutputs`; planting is the
 * established test pattern for histories an acceptance run never saw.
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
    await writeFile(target, spec.fileBytes ?? "");
    records.push({
      state: stateId,
      output: spec.output,
      activation_index: activationIndex,
      digest: await acceptedOutputDigest(spec.type, target, `planted ${stateId}.${spec.output}`),
    });
  }
  return records;
}

/** Deterministic serialization of a whole tree: kinds, paths, file bytes. */
async function snapshotTree(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, prefix: string): Promise<void> => {
    const dirents = (await readdir(dir, { withFileTypes: true })).sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
    );
    for (const dirent of dirents) {
      const relative = prefix === "" ? dirent.name : `${prefix}/${dirent.name}`;
      const child = join(dir, dirent.name);
      if (dirent.isDirectory()) {
        lines.push(`d ${relative}`);
        await walk(child, relative);
      } else if (dirent.isFile()) {
        lines.push(`f ${relative} ${(await readFile(child)).toString("base64")}`);
      } else {
        lines.push(`? ${relative} ${dirent.isSymbolicLink() ? "symlink" : "other"}`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

const TWO_INPUT_BINDINGS = (sources: string) => [
  { id: "facts_seed", path: join(sources, "facts.json") },
  { id: "notes", path: join(sources, "notes.txt") },
];

// --- 1. entry decision from a pipeline json input --------------------------

test("1. an entry decision state evaluates its pipeline json input and selects", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    const result = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(result).toEqual({
      state_id: "check",
      status: "selected",
      outcome: "alpha",
      decision: "alpha",
      rule_id: "rule-a",
      active_constraint_ids: ["c1"],
    });
  });
});

// --- 2. uncovered and inconsistent_facts ------------------------------------

test("2. uncovered and inconsistent_facts outcomes are mapped exactly", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    const emptyRoot = await makeRunRoot(join(dirs.root, "run-uncovered"));
    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: false, f2: false }));
    const uncoveredSnap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      emptyRoot,
    );
    expect(await evaluateDecisionStateFromData(pipeline, uncoveredSnap, [], "check", 1)).toEqual({
      state_id: "check",
      status: "uncovered",
      outcome: "uncovered",
      active_constraint_ids: [],
    });

    const inconsistentRoot = await makeRunRoot(join(dirs.root, "run-inconsistent"));
    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true, f2: true }));
    const inconsistentSnap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      inconsistentRoot,
    );
    expect(
      await evaluateDecisionStateFromData(pipeline, inconsistentSnap, [], "check", 1),
    ).toEqual({
      state_id: "check",
      status: "inconsistent_facts",
      outcome: "inconsistent_facts",
      violated_relation_ids: ["r1"],
    });
  });
});

// --- 3. schema-valid JSON that violates the model fact contract -------------

test("3. schema-valid JSON with missing/unknown/non-boolean facts maps to exact invalid_facts", async () => {
  await withDecision(
    async (dirs, sources) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const cases: { body: string; result: PipelineDecisionStateResult }[] = [
        {
          body: JSON.stringify({ f1: true }),
          result: {
            state_id: "check",
            status: "invalid_facts",
            outcome: "invalid_facts",
            reason: "missing_fact",
            fact_id: "f2",
          },
        },
        {
          body: JSON.stringify({ f1: true, f2: "MUST-NOT-LEAK" }),
          result: {
            state_id: "check",
            status: "invalid_facts",
            outcome: "invalid_facts",
            reason: "non_boolean_fact",
            fact_id: "f2",
            actual_type: "string",
          },
        },
        {
          body: JSON.stringify({ f1: true, f2: false, "CANARY-UNKNOWN-KEY": "SECRET-VALUE" }),
          result: {
            state_id: "check",
            status: "invalid_facts",
            outcome: "invalid_facts",
            reason: "unknown_fact",
          },
        },
        {
          body: JSON.stringify([true, false]),
          result: {
            state_id: "check",
            status: "invalid_facts",
            outcome: "invalid_facts",
            reason: "not_mapping",
          },
        },
      ];
      for (const [index, testCase] of cases.entries()) {
        const runRoot = await makeRunRoot(join(dirs.root, `run-${index}`));
        await writeFile(join(sources, "facts.json"), testCase.body);
        const snap = await snapshotRunInputs(
          pipeline,
          [{ id: "facts_seed", path: join(sources, "facts.json") }],
          runRoot,
        );
        const result = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
        expect(result).toEqual(testCase.result);
        const serialized = JSON.stringify(result);
        expect(serialized).not.toContain("MUST-NOT-LEAK");
        expect(serialized).not.toContain("SECRET-VALUE");
        expect(serialized).not.toContain("CANARY");
      }
    },
    PIPELINE_FROM_PIPELINE_INPUT_LOOSE,
    true,
  );
});

// --- 4. malformed JSON never leaks content -----------------------------------

test("4. malformed json from an accepted output rejects with a content-free diagnostic", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      const records = await plantActivation(runRoot, "coder", 1, [
        { output: "report", type: "file", fileBytes: "REPORT" },
        {
          output: "facts",
          type: "json",
          fileBytes: `{"f1": true, "f2": false, "CANARY-TOKEN": true,`,
        },
      ]);

      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        /is not valid JSON$/,
        "CANARY-TOKEN",
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 5. port-schema failure is a PipelineError, never invalid_facts ---------

test("5. a port-schema failure rejects as PipelineError before the evaluator", async () => {
  await withDecision(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, [], runRoot);
    const records = await plantActivation(runRoot, "coder", 1, [
      { output: "report", type: "file", fileBytes: "REPORT" },
      { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: "MUST-NOT-LEAK" }) },
    ]);

    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
      /source state output "coder"\."facts" does not conform to its JSON schema: \/f2: must be boolean/,
      "MUST-NOT-LEAK",
    );
  }, PIPELINE_FROM_STATE_OUTPUT);
});

// --- 6. decision input from a real accepted agent output ---------------------

test("6. a decision state evaluates the accepted json output of a real agent activation", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);

      const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
      await writeFile(join(prep.outputs_root, "report"), "REPORT");
      await writeFile(join(prep.outputs_root, "facts"), JSON.stringify({ f1: false, f2: true }));
      const records = await acceptActivationOutputs(pipeline, prep);
      expect(records.map((record) => record.output)).toEqual(["facts", "report"]);
      expect(records.map((record) => record.activation_index)).toEqual([1, 1]);

      const result = await evaluateDecisionStateFromData(pipeline, snap, records, "check", 2);
      expect(result).toEqual({
        state_id: "check",
        status: "selected",
        outcome: "beta",
        decision: "beta",
        rule_id: "rule-b",
        active_constraint_ids: [],
      });
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 7. latest accepted output wins, independent of record order -------------

test("7. with two accepted activations the highest index wins regardless of record order", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);

      const prep1 = await prepareActivationData(pipeline, snap, [], "coder", 1);
      await writeFile(join(prep1.outputs_root, "report"), "REPORT-1");
      await writeFile(join(prep1.outputs_root, "facts"), JSON.stringify({ f1: false, f2: true }));
      const records1 = await acceptActivationOutputs(pipeline, prep1);

      const prep2 = await prepareActivationData(pipeline, snap, records1, "coder", 2);
      await writeFile(join(prep2.outputs_root, "report"), "REPORT-2");
      await writeFile(join(prep2.outputs_root, "facts"), JSON.stringify({ f1: true, f2: false }));
      const records2 = await acceptActivationOutputs(pipeline, prep2);

      const forward = await evaluateDecisionStateFromData(
        pipeline,
        snap,
        [...records1, ...records2],
        "check",
        3,
      );
      expect(forward).toEqual({
        state_id: "check",
        status: "selected",
        outcome: "alpha",
        decision: "alpha",
        rule_id: "rule-a",
        active_constraint_ids: ["c1"],
      });

      const reorderedList = [records2[1], records1[0], records2[0], records1[1]];
      const reordered = await evaluateDecisionStateFromData(pipeline, snap, reorderedList, "check", 3);
      expect(reordered).toEqual(forward);
      expect(reordered).not.toBe(forward);
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 8. a damaged old non-winning record fails everything --------------------

test("8. a tampered old non-winning record rejects the whole evaluation before the evaluator", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      const oldRecords = await plantActivation(runRoot, "coder", 1, [
        { output: "report", type: "file", fileBytes: "REPORT-1" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: false, f2: true }) },
      ]);
      const newRecords = await plantActivation(runRoot, "coder", 2, [
        { output: "report", type: "file", fileBytes: "REPORT-2" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
      ]);

      // Corrupt the OLD, non-winning record's on-disk bytes after the
      // digests were minted; the winner itself stays intact.
      await writeFile(
        join(runRoot, "activations", "1-coder", "data", "outputs", "facts"),
        JSON.stringify({ f1: false, f2: false }),
      );

      await expectReject(
        () =>
          evaluateDecisionStateFromData(
            pipeline,
            snap,
            [...oldRecords, ...newRecords],
            "check",
            3,
          ),
        /accepted state output for "coder"\."facts" at activation index 1 digest mismatch/,
      );

      // Record order is irrelevant: the shuffle fails identically.
      const shuffled = [newRecords[1], oldRecords[0], newRecords[0], oldRecords[1]];
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, shuffled, "check", 3),
        /activation index 1 digest mismatch/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 9. an incomplete accepted history is rejected as incoherent ------------

test("9. an incomplete or incoherent accepted history is rejected before evaluation", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);

      // Only one of the two declared output ports is recorded.
      const incomplete = await plantActivation(runRoot, "coder", 1, [
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
      ]);
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, incomplete, "check", 2),
        /accepted history activation index 1 of state "coder" is incomplete/,
      );

      // A record naming an output port the state does not declare.
      const foreign = await plantActivation(runRoot, "coder", 1, [
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
        { output: "report", type: "file", fileBytes: "REPORT" },
      ]);
      foreign.push({ state: "coder", output: "ghost", activation_index: 1, digest: "c".repeat(64) });
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, foreign, "check", 2),
        /references output "ghost" which is not declared by state "coder"/,
      );

      // A record naming a state that declares no output ports (decision).
      const decisionRecord = await plantActivation(runRoot, "coder", 1, [
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
        { output: "report", type: "file", fileBytes: "REPORT" },
      ]);
      decisionRecord.push({ state: "check", output: "facts", activation_index: 4, digest: "d".repeat(64) });
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, decisionRecord, "check", 5),
        /references state "check" which is not a declared agent state/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 10. activation index bound ----------------------------------------------

test("10. records at or beyond nextActivationIndex and invalid bounds are rejected", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      const records = await plantActivation(runRoot, "coder", 1, [
        { output: "report", type: "file", fileBytes: "REPORT" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
      ]);

      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 1),
        /records activation index 1 which is not below the current activation index 1/,
      );
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 0),
        /next activation index must be a positive safe integer, got 0/,
      );
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 1.5),
        /next activation index must be a positive safe integer, got 1\.5/,
      );

      // The index bound is checked before the state id is resolved.
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "absent", 0),
        /next activation index must be a positive safe integer/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 11. missing / forward source ---------------------------------------------

test("11. a missing or forward state-output source rejects before evaluation", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);

      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "check", 1),
        /input port "facts" of decision state "check" references state output "coder"\."facts" which has no accepted output yet \(missing, forward or first-visit self reference\)/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 12. tampered run-input snapshots ----------------------------------------

test("12. tampered run-input snapshot objects reject before evaluation", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);

    // content tamper of the decision's own snapshot input
    const tamperedRoot = await makeRunRoot(join(dirs.root, "run-tamper"));
    const tamperedSnap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), tamperedRoot);
    const factsEntry = tamperedSnap.inputs.find((entry) => entry.id === "facts_seed");
    if (factsEntry === undefined) {
      throw new Error("missing facts_seed snapshot entry");
    }
    await writeFile(factsEntry.snapshot_path, JSON.stringify({ f1: false, f2: false }));
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, tamperedSnap, [], "check", 1),
      /run input snapshot of "facts_seed" digest mismatch/,
    );

    // symlinked snapshot object
    const symlinkedRoot = await makeRunRoot(join(dirs.root, "run-symlink"));
    const symlinkedSnap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), symlinkedRoot);
    const notesEntry = symlinkedSnap.inputs.find((entry) => entry.id === "notes");
    if (notesEntry === undefined) {
      throw new Error("missing notes snapshot entry");
    }
    await rm(notesEntry.snapshot_path, { force: true });
    await symlink(join(sources, "notes.txt"), notesEntry.snapshot_path);
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, symlinkedSnap, [], "check", 1),
      /run input snapshot of "notes" snapshot object .* is a symbolic link/,
    );

    // FIFO instead of a regular file
    const fifoRoot = await makeRunRoot(join(dirs.root, "run-fifo"));
    const fifoSnap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), fifoRoot);
    const fifoEntry = fifoSnap.inputs.find((entry) => entry.id === "facts_seed");
    if (fifoEntry === undefined) {
      throw new Error("missing facts_seed snapshot entry");
    }
    await rm(fifoEntry.snapshot_path, { force: true });
    await makeFifo(fifoEntry.snapshot_path);
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, fifoSnap, [], "check", 1),
      /run input snapshot of "facts_seed" snapshot object .* is not a regular file, found an unexpected object/,
    );

    // kind mismatch: json snapshot replaced by a directory
    const kindRoot = await makeRunRoot(join(dirs.root, "run-kind"));
    const kindSnap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), kindRoot);
    const kindEntry = kindSnap.inputs.find((entry) => entry.id === "facts_seed");
    if (kindEntry === undefined) {
      throw new Error("missing facts_seed snapshot entry");
    }
    await rm(kindEntry.snapshot_path, { force: true });
    await mkdir(kindEntry.snapshot_path);
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, kindSnap, [], "check", 1),
      /run input snapshot of "facts_seed" snapshot object .* is not a regular file, found an existing directory/,
    );

    // relocated snapshot object
    const movedRoot = await makeRunRoot(join(dirs.root, "run-moved"));
    const movedSnap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), movedRoot);
    const movedEntry = movedSnap.inputs.find((entry) => entry.id === "facts_seed");
    if (movedEntry === undefined) {
      throw new Error("missing facts_seed snapshot entry");
    }
    await rename(movedEntry.snapshot_path, `${movedEntry.snapshot_path}.moved`);
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, movedSnap, [], "check", 1),
      /run input snapshot of "facts_seed" snapshot object .* does not exist/,
    );
  }, PIPELINE_TWO_INPUTS);
});

test("12b. invalid accepted output objects reject before evaluation", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      const records = await plantActivation(runRoot, "coder", 1, [
        { output: "report", type: "file", fileBytes: "REPORT" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
      ]);
      const outputsRoot = join(runRoot, "activations", "1-coder", "data", "outputs");

      // symlink instead of the accepted json output
      await rm(join(outputsRoot, "facts"), { force: true });
      await symlink(join(dirs.root, "userdata", "facts.json"), join(outputsRoot, "facts"));
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        /accepted state output for "coder"\."facts" at activation index 1 fixed output path .* is a symbolic link/,
      );
      await rm(join(outputsRoot, "facts"), { force: true });
      await writeFile(join(outputsRoot, "facts"), JSON.stringify({ f1: true, f2: false }));

      // FIFO instead of the accepted json output
      await rm(join(outputsRoot, "facts"), { force: true });
      await makeFifo(join(outputsRoot, "facts"));
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        /fixed output .* is not a regular file, found an unexpected object/,
      );

      // kind mismatch: json output replaced by a directory
      await rm(join(outputsRoot, "facts"), { force: true });
      await mkdir(join(outputsRoot, "facts"));
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        /fixed output .* is not a regular file, found an existing directory/,
      );
      await rm(join(outputsRoot, "facts"), { recursive: true, force: true });

      // digest mismatch of the winning record itself
      await writeFile(join(outputsRoot, "facts"), JSON.stringify({ f1: false, f2: false }));
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        /accepted state output for "coder"\."facts" at activation index 1 digest mismatch/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 13. any other run input is verified too ----------------------------------

test("13. a modified run input that does not feed the decision still fails first", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(pipeline, TWO_INPUT_BINDINGS(sources), runRoot);
    const notesEntry = snap.inputs.find((entry) => entry.id === "notes");
    if (notesEntry === undefined) {
      throw new Error("missing notes snapshot entry");
    }
    await writeFile(notesEntry.snapshot_path, "TAMPERED");

    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, snap, [], "check", 1),
      /run input snapshot of "notes" digest mismatch/,
    );

    // The decision's own input snapshot stays intact; only the tampered
    // snapshot entry was changed.
    const factsEntry = snap.inputs.find((entry) => entry.id === "facts_seed");
    if (factsEntry === undefined) {
      throw new Error("missing facts_seed snapshot entry");
    }
    expect(JSON.parse(await readFile(factsEntry.snapshot_path, "utf8"))).toEqual({
      f1: true,
      f2: false,
    });
  }, PIPELINE_TWO_INPUTS);
});

// --- 14. original user binding paths are never re-read ------------------------

test("14. the original user binding path is not read again after the snapshot", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    // The user source disappears entirely after the snapshot.
    await rm(join(dirs.root, "userdata"), { recursive: true, force: true });
    const result = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(result).toEqual({
      state_id: "check",
      status: "selected",
      outcome: "alpha",
      decision: "alpha",
      rule_id: "rule-a",
      active_constraint_ids: ["c1"],
    });
  });
});

// --- 15. forged pipeline and snapshot arguments -------------------------------

test("15. clones, casts, other-pipeline snapshots and Proxies reject before any read", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    const forgedPipelineMessage =
      /evaluateDecisionStateFromData requires the deep-frozen snapshot object returned by loadPipelineV2/;
    await expectReject(
      () => evaluateDecisionStateFromData(structuredClone(pipeline) as ResolvedPipelineV2, snap, [], "check", 1),
      forgedPipelineMessage,
    );
    await expectReject(
      () => evaluateDecisionStateFromData({ ...pipeline } as ResolvedPipelineV2, snap, [], "check", 1),
      forgedPipelineMessage,
    );

    let pipelineTrapRan = false;
    const proxiedPipeline = new Proxy(pipeline, {
      get() {
        pipelineTrapRan = true;
        throw new Error("trap");
      },
    });
    await expectReject(
      () => evaluateDecisionStateFromData(proxiedPipeline, snap, [], "check", 1),
      forgedPipelineMessage,
    );
    expect(pipelineTrapRan).toBe(false);

    const forgedSnapshotMessage = /frozen run input snapshot object/;
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, structuredClone(snap) as RunInputsSnapshot, [], "check", 1),
      forgedSnapshotMessage,
    );
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, { ...snap, inputs: [] } as RunInputsSnapshot, [], "check", 1),
      forgedSnapshotMessage,
    );

    let snapshotTrapRan = false;
    const proxiedSnapshot = new Proxy(snap, {
      get() {
        snapshotTrapRan = true;
        throw new Error("trap");
      },
    });
    await expectReject(
      () => evaluateDecisionStateFromData(pipeline, proxiedSnapshot, [], "check", 1),
      forgedSnapshotMessage,
    );
    expect(snapshotTrapRan).toBe(false);

    // A snapshot minted for a different pipeline object of the same bundle.
    const otherPipeline = await loadPipelineV2(dirs.bundle);
    expect(otherPipeline).not.toBe(pipeline);
    await expectReject(
      () => evaluateDecisionStateFromData(otherPipeline, snap, [], "check", 1),
      forgedSnapshotMessage,
    );
  });
});

// --- 16. state id handling -----------------------------------------------------

test("16. agent, terminal, unknown and unsafe state ids are rejected", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);

      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "coder", 1),
        /state "coder" is not a decision state; the decision data adapter exists for decision states only/,
      );
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "done", 1),
        /state "done" is not a decision state; the decision data adapter exists for decision states only/,
      );
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "absent", 1),
        /state "absent" is not declared by the pipeline/,
      );
      await expectReject(
        () => evaluateDecisionStateFromData(pipeline, snap, [], "not a safe id!", 1),
        /is not a safe identifier/,
      );
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 17. the filesystem stays untouched ----------------------------------------

test("17. a successful evaluation leaves the run tree unchanged and creates no decision leaf", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    const before = await snapshotTree(runRoot);
    const result = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(result).toEqual({
      state_id: "check",
      status: "selected",
      outcome: "alpha",
      decision: "alpha",
      rule_id: "rule-a",
      active_constraint_ids: ["c1"],
    });
    expect(await snapshotTree(runRoot)).toBe(before);
    expect(await lstat(join(runRoot, "activations")).then(() => true, () => false)).toBe(false);
  });
});

test("17b. a failed evaluation leaves the run tree unchanged and creates no decision leaf", async () => {
  await withDecision(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const runRoot = await makeRunRoot(dirs.root);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      const oldRecords = await plantActivation(runRoot, "coder", 1, [
        { output: "report", type: "file", fileBytes: "REPORT-1" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: false, f2: true }) },
      ]);
      const newRecords = await plantActivation(runRoot, "coder", 2, [
        { output: "report", type: "file", fileBytes: "REPORT-2" },
        { output: "facts", type: "json", fileBytes: JSON.stringify({ f1: true, f2: false }) },
      ]);
      await writeFile(
        join(runRoot, "activations", "1-coder", "data", "outputs", "facts"),
        JSON.stringify({ f1: false, f2: false }),
      );

      const before = await snapshotTree(runRoot);
      await expectReject(
        () =>
          evaluateDecisionStateFromData(
            pipeline,
            snap,
            [...oldRecords, ...newRecords],
            "check",
            3,
          ),
        /digest mismatch/,
      );
      expect(await snapshotTree(runRoot)).toBe(before);
      const activationEntries = (await readdir(join(runRoot, "activations"))).sort();
      expect(activationEntries).toEqual(["1-coder", "2-coder"]);
      expect(activationEntries.some((name) => name.endsWith("-check"))).toBe(false);
      expect((await readdir(runRoot)).some((name) => name.startsWith(".tmp-"))).toBe(false);
    },
    PIPELINE_FROM_STATE_OUTPUT,
  );
});

// --- 18. determinism and freezing ----------------------------------------------

test("18. repeated evaluation is deterministic and the result is deep-frozen", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    const first = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    const second = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    expect(Object.isFrozen(first)).toBe(true);
    if (first.status !== "selected" || second.status !== "selected") {
      throw new Error("expected selected");
    }
    expect(Object.isFrozen(first.active_constraint_ids)).toBe(true);
    expect(first.active_constraint_ids).not.toBe(second.active_constraint_ids);
  });
});

// --- 19. no transition target selection -----------------------------------------

test("19. results never name a transition target or move the graph", async () => {
  await withDecision(async (dirs, sources) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    const snap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      runRoot,
    );

    const selected = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(Object.keys(selected).sort()).toEqual(
      ["active_constraint_ids", "decision", "outcome", "rule_id", "state_id", "status"].sort(),
    );

    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: false, f2: false }));
    const uncoveredRoot = await makeRunRoot(join(dirs.root, "run-uncovered"));
    const uncoveredSnap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      uncoveredRoot,
    );
    const uncovered = await evaluateDecisionStateFromData(pipeline, uncoveredSnap, [], "check", 1);
    expect(Object.keys(uncovered).sort()).toEqual(
      ["active_constraint_ids", "outcome", "state_id", "status"].sort(),
    );

    await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true, f2: true }));
    const inconsistentRoot = await makeRunRoot(join(dirs.root, "run-inconsistent"));
    const inconsistentSnap = await snapshotRunInputs(
      pipeline,
      [{ id: "facts_seed", path: join(sources, "facts.json") }],
      inconsistentRoot,
    );
    const inconsistent = await evaluateDecisionStateFromData(pipeline, inconsistentSnap, [], "check", 1);
    expect(Object.keys(inconsistent).sort()).toEqual(
      ["outcome", "state_id", "status", "violated_relation_ids"].sort(),
    );

    for (const result of [selected, uncovered, inconsistent]) {
      const serialized = JSON.stringify(result);
      expect(serialized).not.toMatch(/"to":/);
      expect(serialized).not.toContain("next_state");
      expect(serialized).not.toContain("transition");
      expect(serialized).not.toContain("target");
    }
  });
});
