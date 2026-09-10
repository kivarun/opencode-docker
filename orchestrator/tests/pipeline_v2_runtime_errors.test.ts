import { mkdir, mkdtemp, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  loadPipelineV2,
  type PortType,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import {
  PIPELINE_V2_FAILURE_REASONS,
} from "../src/pipeline_v2_state.ts";
import {
  acceptedOutputDigest,
  acceptActivationOutputs,
  collectRunOutputs,
  evaluateDecisionStateFromData,
  prepareActivationData,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import {
  PIPELINE_V2_RUNTIME_FAILURE_REASONS,
  PipelineV2RuntimeError,
  type PipelineV2RuntimeFailureReason,
  isPipelineV2RuntimeError,
} from "../src/pipeline_v2_runtime_error.ts";
import * as runtimeErrorModule from "../src/pipeline_v2_runtime_error.ts";

/**
 * Focused tests for the typed runtime failure contract of the pipeline v2
 * data plane: every reachable data-plane failure carries a stable
 * machine-readable `reason` (`PipelineV2RuntimeError`, still an
 * `instanceof PipelineError`) assigned where the operation semantics are
 * known — never by parsing an error message. Trust-boundary and
 * internal-contract violations stay plain `PipelineError`s. The production
 * runner is not wired: the production loader still rejects schema v2
 * before Launcher auth and before any Session, and durable state records
 * the reason string only (next increment: the coordinator).
 */

const FACTS_BYTES = '{ "f1": true, "f2": false }';
/** Schema-valid but fact-invalid: the model requires both f1 and f2. */
const MISSING_FACT_BYTES = '{ "f1": true }';
const BRIEF_BYTES = "BRIEF-1\n";
const CONFIG_BYTES = '{"ok":true,  "deep":{  "nested":null  }}';
/** Canary that must never surface in any reason, message or diagnostic. */
const CANARY = "dhcr_8bae536a507484c1d8325c4c8f8e4ac8";

const STRICT_FACTS_SCHEMA = {
  type: "object",
  required: ["f1", "f2"],
  additionalProperties: false,
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

const CONFIG_SCHEMA = { type: "object", required: ["ok"] };

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
        to: failed_end
      - outcome: uncovered
        to: done
      - outcome: inconsistent_facts
        to: done
      - outcome: invalid_facts
        to: done
`;

/** Agents-only pipeline whose run outputs source state outputs. */
const PIPELINE_YAML = `
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
  - id: log
    required: false
    source:
      state_output:
        state: coder
        output: log
  - id: summary
    required: true
    source:
      state_output:
        state: architect
        output: summary

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

/** Entry decision state reading its json input from a pipeline input. */
const DECISION_YAML = `
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
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/**
 * Entry agent state producing the json input for the decision state. The
 * producer agent never runs for real here; its accepted leaf is planted at
 * the fixed orchestrator-derived location with runner-owned records whose
 * digests are computed over the actual bytes.
 */
const DECISION_FROM_OUTPUT_YAML = `
schema_version: 2
entry_state: producer
max_transitions: 20

inputs: []

outputs: []

states:
  - id: producer
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/facts.schema.json
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
            state: producer
            output: facts
    transitions:
${DECISION_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

interface BundleDirs {
  readonly root: string;
  readonly bundle: string;
}

interface AgentsRun {
  readonly pipeline: Awaited<ReturnType<typeof loadPipelineV2>>;
  readonly secondPipeline: Awaited<ReturnType<typeof loadPipelineV2>>;
  readonly runRoot: string;
  readonly sources: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-runtime-errors-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  return { root, bundle };
}

async function writeAgentsBundle(dirs: BundleDirs, yaml: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "review the task\n");
  await writeFile(join(dirs.bundle, "schemas", "config.schema.json"), JSON.stringify(CONFIG_SCHEMA));
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(STRICT_FACTS_SCHEMA));
}

async function writeDecisionBundle(dirs: BundleDirs, yaml: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "produce facts\n");
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify({}));
  await writeFile(join(dirs.bundle, "decisions", "model.yaml"), MODEL_YAML);
}

async function writeSources(root: string): Promise<string> {
  const dir = join(root, "userdata");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "brief.txt"), BRIEF_BYTES);
  await mkdir(join(dir, "assets"), { recursive: true });
  await writeFile(join(dir, "assets", "logo.txt"), "LOGO");
  await writeFile(join(dir, "config.json"), CONFIG_BYTES);
  await writeFile(join(dir, "facts.json"), FACTS_BYTES);
  return dir;
}

async function makeRunRoot(root: string): Promise<string> {
  const runRoot = join(root, "run");
  await mkdir(runRoot, { mode: 0o700 });
  await mkdir(join(runRoot, "project"), { mode: 0o700 });
  return runRoot;
}

/** Two independently loaded snapshots of the same trusted bundle. */
async function withAgentsRun(
  fn: (run: AgentsRun) => Promise<void>,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeAgentsBundle(dirs, PIPELINE_YAML);
  const sources = await writeSources(dirs.root);
  try {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const secondPipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    await fn({ pipeline, secondPipeline, runRoot, sources });
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

async function withDecisionRun(
  fn: (
    pipeline: Awaited<ReturnType<typeof loadPipelineV2>>,
    runRoot: string,
    sources: string,
  ) => Promise<void>,
  yaml: string = DECISION_YAML,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeDecisionBundle(dirs, yaml);
  const sources = await writeSources(dirs.root);
  try {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root);
    await fn(pipeline, runRoot, sources);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

const ALL_AGENT_BINDINGS = (sources: string) => [
  { id: "brief", path: join(sources, "brief.txt") },
  { id: "assets", path: join(sources, "assets") },
  { id: "config", path: join(sources, "config.json") },
];

const DECISION_SEED_BINDING = (sources: string) => [
  { id: "facts_seed", path: join(sources, "facts.json") },
];

async function expectTypedFailure(
  run: () => Promise<unknown> | unknown,
  reason: PipelineV2RuntimeFailureReason,
  message?: RegExp | string,
  notContaining?: string,
): Promise<PipelineV2RuntimeError> {
  let failure: unknown;
  try {
    await run();
  } catch (cause) {
    failure = cause;
  }
  expect(failure).toBeInstanceOf(PipelineV2RuntimeError);
  expect(failure).toBeInstanceOf(PipelineError);
  expect(isPipelineV2RuntimeError(failure)).toBe(true);
  const error = failure as PipelineV2RuntimeError;
  expect(PIPELINE_V2_RUNTIME_FAILURE_REASONS).toContain(error.reason);
  expect(error.reason).toBe(reason);
  if (message !== undefined) {
    if (typeof message === "string") {
      expect(error.message).toBe(message);
    } else {
      expect(error.message).toMatch(message);
    }
  }
  if (notContaining !== undefined) {
    expect(error.message).not.toContain(notContaining);
  }
  return error;
}

async function expectPlainPipelineError(
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
  expect(isPipelineV2RuntimeError(failure)).toBe(false);
  const error = failure as Error;
  if (typeof message === "string") {
    expect(error.message).toBe(message);
  } else {
    expect(error.message).toMatch(message);
  }
}

/**
 * Plant a complete accepted activation leaf at the fixed
 * orchestrator-derived location and mint runner-owned records with digests
 * computed over the actual bytes, so history verification passes even when
 * the planted bytes are intentionally malformed or schema-invalid.
 */
async function plantActivation(
  runRoot: string,
  stateId: string,
  activationIndex: number,
  outputs: readonly { output: string; type: PortType; bytes?: string }[],
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
    await writeFile(target, spec.bytes ?? "");
    records.push({
      state: stateId,
      output: spec.output,
      activation_index: activationIndex,
      digest: await acceptedOutputDigest(spec.type, target, "planted"),
    });
  }
  return records;
}

/** Run one real coder activation through acceptance, accumulating records. */
async function runCoder(
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>,
  snap: RunInputsSnapshot,
  records: readonly AcceptedStateOutput[],
  index: number,
): Promise<AcceptedStateOutput[]> {
  const prep = await prepareActivationData(pipeline, snap, records, "coder", index);
  await writeFile(join(prep.outputs_root, "report"), `REPORT-${index}`);
  await writeFile(join(prep.outputs_root, "log", "work.txt"), "WORK-1");
  await writeFile(join(prep.outputs_root, "facts"), FACTS_BYTES);
  const coderRecords = await acceptActivationOutputs(pipeline, prep);
  return [...records, ...coderRecords];
}

test("1. every runtime failure reason is a state schema v3 failure reason", () => {
  expect(PIPELINE_V2_RUNTIME_FAILURE_REASONS.length).toBe(9);
  const stateReasons = new Set<string>(PIPELINE_V2_FAILURE_REASONS);
  for (const reason of PIPELINE_V2_RUNTIME_FAILURE_REASONS) {
    expect(stateReasons.has(reason)).toBe(true);
  }
  expect(stateReasons.has("internal_error")).toBe(true);
});

test("2. PipelineV2RuntimeError keeps the PipelineError identity with an immutable reason", () => {
  const error = new PipelineV2RuntimeError("run_input_modified", "diagnostic text");
  expect(error).toBeInstanceOf(PipelineV2RuntimeError);
  expect(error).toBeInstanceOf(PipelineError);
  expect(isPipelineV2RuntimeError(error)).toBe(true);
  expect(isPipelineV2RuntimeError("not an error")).toBe(false);
  expect(isPipelineV2RuntimeError(new PipelineError("plain"))).toBe(false);
  expect(error.reason).toBe("run_input_modified");
  expect(error.message).toBe("diagnostic text");
  expect(error.name).toBe("PipelineV2RuntimeError");
  const descriptor = Object.getOwnPropertyDescriptor(error, "reason");
  expect(descriptor?.writable).toBe(false);
  expect(descriptor?.configurable).toBe(false);
  expect(() => {
    (error as unknown as { reason: string }).reason = "run_output_missing";
  }).toThrow(TypeError);
  expect(error.reason).toBe("run_input_modified");
  expect(() =>
    new PipelineV2RuntimeError(
      "internal_error" as unknown as PipelineV2RuntimeFailureReason,
      "not a runtime reason",
    ),
  ).toThrow(TypeError);
});

test("3. the error module exposes exactly the fixed four-member contract", () => {
  // The only runtime exports of the neutral error module: the fixed reason
  // list, the typed error class and the type guard. The
  // `PipelineV2RuntimeFailureReason` type is compile-time only. No broad
  // retagging or construction helper is exported — the coordinator reads
  // `reason`, it never gets a tool to broadly retag arbitrary errors.
  expect(Object.keys(runtimeErrorModule).sort()).toEqual([
    "PIPELINE_V2_RUNTIME_FAILURE_REASONS",
    "PipelineV2RuntimeError",
    "isPipelineV2RuntimeError",
  ]);
  const exports = runtimeErrorModule as Record<string, unknown>;
  expect("pipelineV2RuntimeFailure" in exports).toBe(false);
  expect("withPipelineV2RuntimeReason" in exports).toBe(false);
  expect(typeof exports.isPipelineV2RuntimeError).toBe("function");
  expect(typeof exports.PipelineV2RuntimeError).toBe("function");
  expect(Object.isFrozen(PIPELINE_V2_RUNTIME_FAILURE_REASONS) || Array.isArray(PIPELINE_V2_RUNTIME_FAILURE_REASONS)).toBe(
    true,
  );

  // Explicit typed construction stays available through the class itself,
  // with the reason validated against the fixed list.
  const constructed = new PipelineV2RuntimeError("run_input_invalid", "explicit message");
  expect(constructed).toBeInstanceOf(PipelineV2RuntimeError);
  expect(constructed).toBeInstanceOf(PipelineError);
  expect(constructed.reason).toBe("run_input_invalid");
  expect(constructed.message).toBe("explicit message");
});

test("4. malformed json run input rejects as run_input_invalid with a content-free diagnostic", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    await writeFile(join(sources, "config.json"), `{"ok":true,"body":"${CANARY}`);
    await expectTypedFailure(
      () => snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot),
      "run_input_invalid",
      `pipeline input "config" bound file ${join(sources, "config.json")} is not valid JSON`,
      CANARY,
    );
  });
});

test("5. symlinked, unknown, duplicate and missing bindings reject as run_input_invalid", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    await symlink(join(sources, "brief.txt"), join(sources, "brief-link"));
    const first = await expectTypedFailure(
      () =>
        snapshotRunInputs(
          pipeline,
          [
            { id: "brief", path: join(sources, "brief-link") },
            { id: "assets", path: join(sources, "assets") },
            { id: "config", path: join(sources, "config.json") },
          ],
          runRoot,
        ),
      "run_input_invalid",
      `pipeline input "brief" bound path ${join(sources, "brief-link")} is a symbolic link; bind the real object`,
    );
    const repeated = await expectTypedFailure(
      () =>
        snapshotRunInputs(
          pipeline,
          [
            { id: "brief", path: join(sources, "brief-link") },
            { id: "assets", path: join(sources, "assets") },
            { id: "config", path: join(sources, "config.json") },
          ],
          runRoot,
        ),
      "run_input_invalid",
    );
    expect(repeated.reason).toBe(first.reason);

    await expectTypedFailure(
      () => snapshotRunInputs(pipeline, [{ id: "nope", path: join(sources, "brief.txt") }], runRoot),
      "run_input_invalid",
      `run input binding "nope" does not match a declared pipeline input`,
    );
    await expectTypedFailure(
      () =>
        snapshotRunInputs(
          pipeline,
          [
            { id: "brief", path: join(sources, "brief.txt") },
            { id: "brief", path: join(sources, "brief.txt") },
            { id: "assets", path: join(sources, "assets") },
            { id: "config", path: join(sources, "config.json") },
          ],
          runRoot,
        ),
      "run_input_invalid",
      `pipeline input "brief" is bound more than once`,
    );
    await expectTypedFailure(
      () =>
        snapshotRunInputs(
          pipeline,
          [
            { id: "brief", path: join(sources, "brief.txt") },
            { id: "assets", path: join(sources, "assets") },
          ],
          runRoot,
        ),
      "run_input_invalid",
      `pipeline input "config" is not bound`,
    );
  });
});

test("6. a json run input violating its declared schema rejects as run_input_invalid without the value", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    await writeFile(join(sources, "config.json"), `{"nope":"${CANARY}"}`);
    await expectTypedFailure(
      () => snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot),
      "run_input_invalid",
      /config\.json does not conform to its JSON schema/,
      CANARY,
    );
  });
});

test("7. a missing state output at preparation rejects as activation_prepare_failed", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await expectTypedFailure(
      () => prepareActivationData(pipeline, snap, [], "architect", 1),
      "activation_prepare_failed",
      `input port "facts" of agent state "architect" references state output "coder"."facts" which has no accepted output yet (missing, forward or first-visit self reference)`,
    );
  });
});

test("8. an occupied activation index rejects as activation_prepare_failed", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await runCoder(pipeline, snap, [], 1);
    await expectTypedFailure(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      "activation_prepare_failed",
      /activation index 1 is already in use/,
    );
  });
});

test("9. run input tampering rejects as run_input_modified before preparation", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await writeFile(join(runRoot, "data", "inputs", "brief"), "TAMPERED\n");
    await expectTypedFailure(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      "run_input_modified",
      /run input snapshot of "brief" digest mismatch/,
    );
  });
});

test("10. run input tampering rejects as run_input_modified before decision evaluation", async () => {
  await withDecisionRun(async (pipeline, runRoot, sources) => {
    const snap = await snapshotRunInputs(pipeline, DECISION_SEED_BINDING(sources), runRoot);
    await writeFile(join(runRoot, "data", "inputs", "facts_seed"), '{"f1":false,"f2":false}');
    await expectTypedFailure(
      () => evaluateDecisionStateFromData(pipeline, snap, [], "check", 1),
      "run_input_modified",
      /run input snapshot of "facts_seed" digest mismatch/,
    );
  });
});

test("11. run input tampering rejects as run_input_modified before run-output collection", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await runCoder(pipeline, snap, [], 1);
    await writeFile(join(runRoot, "data", "inputs", "brief"), "TAMPERED\n");
    await expectTypedFailure(
      () => collectRunOutputs(pipeline, snap, []),
      "run_input_modified",
      /run input snapshot of "brief" digest mismatch/,
    );
  });
});

test("12. malformed agent json output rejects as activation_output_invalid with a content-free diagnostic", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(prep.outputs_root, "report"), "REPORT");
    await writeFile(join(prep.outputs_root, "log", "work.txt"), "WORK");
    await writeFile(join(prep.outputs_root, "facts"), `{"f1":true,"body":"${CANARY}`);
    await expectTypedFailure(
      () => acceptActivationOutputs(pipeline, prep),
      "activation_output_invalid",
      /output port "facts" of agent state "coder" activation 1 .* is not valid JSON/,
      CANARY,
    );
  });
});

test("13. missing declared output and undeclared entry reject as activation_output_invalid", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(prep.outputs_root, "facts"), FACTS_BYTES);
    await writeFile(join(prep.outputs_root, "log", "work.txt"), "WORK");
    await expectTypedFailure(
      () => acceptActivationOutputs(pipeline, prep),
      "activation_output_invalid",
      /output port "report" of agent state "coder" activation 1 is missing from the activation outputs root/,
    );
  });
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(prep.outputs_root, "report"), "REPORT");
    await writeFile(join(prep.outputs_root, "facts"), FACTS_BYTES);
    await writeFile(join(prep.outputs_root, "log", "work.txt"), "WORK");
    await writeFile(join(prep.outputs_root, "undeclared.txt"), CANARY);
    await expectTypedFailure(
      () => acceptActivationOutputs(pipeline, prep),
      "activation_output_invalid",
      /contains undeclared entry "undeclared\.txt"/,
    );
  });
});

test("14. a schema-violating agent json output rejects as activation_output_invalid without the value", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
    await writeFile(join(prep.outputs_root, "report"), "REPORT");
    await writeFile(join(prep.outputs_root, "facts"), `{"f1":true,"extra":"${CANARY}"}`);
    await writeFile(join(prep.outputs_root, "log", "work.txt"), "WORK");
    await expectTypedFailure(
      () => acceptActivationOutputs(pipeline, prep),
      "activation_output_invalid",
      /facts/,
      CANARY,
    );
  });
});

test("15. accepted output tampering rejects as accepted_output_modified before the next activation", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const records = await runCoder(pipeline, snap, [], 1);
    await writeFile(
      join(runRoot, "activations", "1-coder", "data", "outputs", "facts"),
      '{"f1":false,"f2":false}',
    );
    await expectTypedFailure(
      () => prepareActivationData(pipeline, snap, records, "coder", 2),
      "accepted_output_modified",
      /accepted state output for "coder"\."facts" at activation index 1 digest mismatch/,
    );
  });
});

test("16. a corrupted old non-winning accepted output rejects as accepted_output_modified", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    let records = await runCoder(pipeline, snap, [], 1);
    records = await runCoder(pipeline, snap, records, 2);
    await writeFile(
      join(runRoot, "activations", "1-coder", "data", "outputs", "report"),
      "CORRUPTED-OLD",
    );
    await expectTypedFailure(
      () => prepareActivationData(pipeline, snap, records, "architect", 3),
      "accepted_output_modified",
      /accepted state output for "coder"\."report" at activation index 1 digest mismatch/,
    );
  });
});

test("17. accepted output tampering rejects as accepted_output_modified before decision evaluation", async () => {
  await withDecisionRun(async (pipeline, runRoot) => {
    const records = await plantActivation(runRoot, "producer", 1, [
      { output: "facts", type: "json", bytes: FACTS_BYTES },
    ]);
    const snap = await snapshotRunInputs(pipeline, [], runRoot);
    await writeFile(join(runRoot, "activations", "1-producer", "data", "outputs", "facts"), '{"f1":false}');
    await expectTypedFailure(
      () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
      "accepted_output_modified",
      /accepted state output for "producer"\."facts" at activation index 1 digest mismatch/,
    );
  }, DECISION_FROM_OUTPUT_YAML);
});

test("18. accepted output tampering rejects as accepted_output_modified before run-output collection", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const records = await runCoder(pipeline, snap, [], 1);
    await writeFile(
      join(runRoot, "activations", "1-coder", "data", "outputs", "report"),
      "TAMPERED-REPORT",
    );
    await expectTypedFailure(
      () => collectRunOutputs(pipeline, snap, records),
      "accepted_output_modified",
      /accepted state output for "coder"\."report" at activation index 1 digest mismatch/,
    );
  });
});

test("19. malformed runner-owned accepted records stay plain PipelineErrors", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await expectPlainPipelineError(
      () =>
        prepareActivationData(
          pipeline,
          snap,
          [{ state: "coder", output: "facts" }],
          "architect",
          1,
        ),
      /accepted state output 0/,
    );
    await expectPlainPipelineError(
      () =>
        prepareActivationData(
          pipeline,
          snap,
          [
            {
              state: "coder",
              output: "report",
              activation_index: 1,
              digest: "zz".repeat(32),
            },
          ],
          "coder",
          2,
        ),
      /is not a lowercase SHA-256 hex digest/,
    );
    await expectPlainPipelineError(
      () =>
        prepareActivationData(
          pipeline,
          snap,
          [
            {
              state: "ghost",
              output: "report",
              activation_index: 1,
              digest: "a".repeat(64),
            },
          ],
          "coder",
          2,
        ),
      /which is not a declared agent state/,
    );
    await expectPlainPipelineError(
      () =>
        prepareActivationData(
          pipeline,
          snap,
          [
            { state: "coder", output: "report", activation_index: 2, digest: "a".repeat(64) },
            { state: "coder", output: "log", activation_index: 2, digest: "b".repeat(64) },
            { state: "coder", output: "facts", activation_index: 2, digest: "c".repeat(64) },
          ],
          "coder",
          2,
        ),
      /is not below the current activation index/,
    );
  });
});

test("20. malformed decision json from an accepted output rejects as decision_input_invalid", async () => {
  await withDecisionRun(
    async (pipeline, runRoot) => {
      const records = await plantActivation(runRoot, "producer", 1, [
        { output: "facts", type: "json", bytes: `{"f1":"${CANARY}"` },
      ]);
      const snap = await snapshotRunInputs(pipeline, [], runRoot);
      await expectTypedFailure(
        () => evaluateDecisionStateFromData(pipeline, snap, records, "check", 2),
        "decision_input_invalid",
        /input port "facts" of decision state "check" source state output "producer"\."facts" .* is not valid JSON/,
        CANARY,
      );
    },
    DECISION_FROM_OUTPUT_YAML,
  );
});

test("21. a logically unavailable decision input rejects as decision_input_invalid", async () => {
  await withDecisionRun(async (pipeline, runRoot) => {
    const snap = await snapshotRunInputs(pipeline, [], runRoot);
    await expectTypedFailure(
      () => evaluateDecisionStateFromData(pipeline, snap, [], "check", 1),
      "decision_input_invalid",
      `input port "facts" of decision state "check" references state output "producer"."facts" which has no accepted output yet (missing, forward or first-visit self reference)`,
    );
  }, DECISION_FROM_OUTPUT_YAML);
});

test("22. schema-conforming but fact-invalid decision json stays the normal invalid_facts result", async () => {
  await withDecisionRun(async (pipeline, runRoot, sources) => {
    await writeFile(join(sources, "facts.json"), MISSING_FACT_BYTES);
    const snap = await snapshotRunInputs(pipeline, DECISION_SEED_BINDING(sources), runRoot);
    const result = await evaluateDecisionStateFromData(pipeline, snap, [], "check", 1);
    expect(result.status).toBe("invalid_facts");
    expect(result).toMatchObject({
      status: "invalid_facts",
      reason: "missing_fact",
      fact_id: "f2",
    });
    expect(Object.isFrozen(result)).toBe(true);
    expect(isPipelineV2RuntimeError(result)).toBe(false);
  });
});

test("23. a missing required run output source rejects as run_output_missing", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await expectTypedFailure(
      () => collectRunOutputs(pipeline, snap, []),
      "run_output_missing",
      `run output "summary" references required state output "architect"."summary" which has no accepted output yet`,
    );
    const names = (await readdir(runRoot)).sort();
    expect(names.filter((name) => name.startsWith(".tmp-"))).toEqual([]);
    expect(names.filter((name) => name === "outputs")).toEqual([]);
  });
});

test("24. a pre-existing run outputs root rejects as run_output_publish_failed", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    await mkdir(join(runRoot, "outputs"), { mode: 0o700 });
    await expectTypedFailure(
      () => collectRunOutputs(pipeline, snap, []),
      "run_output_publish_failed",
      /run outputs root .* already exists, found an existing directory/,
    );
  });
});

test("25. a missing optional run output stays a successful absent entry", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    const records = await plantActivation(runRoot, "architect", 1, [
      { output: "summary", type: "file", bytes: "SUMMARY" },
    ]);
    const snapshot = await collectRunOutputs(pipeline, snap, records);
    expect(snapshot.outputs.map((entry) => [entry.id, entry.present])).toEqual([
      ["final-brief", true],
      ["log", false],
      ["summary", true],
    ]);
    const logEntry = snapshot.outputs[1];
    expect(logEntry?.present).toBe(false);
    expect("snapshot_path" in (logEntry ?? {})).toBe(false);
  });
});

test("26. forged pipeline, snapshot and activation stay plain PipelineErrors; Proxy getters never run", async () => {
  await withAgentsRun(async ({ pipeline, secondPipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);

    let getterCalls = 0;
    const forgedPipeline = new Proxy({} as ResolvedPipelineV2, {
      get(target, prop) {
        getterCalls += 1;
        return (target as unknown as Record<string, unknown>)[prop as string];
      },
    });
    await expectPlainPipelineError(
      () => prepareActivationData(forgedPipeline, snap, [], "coder", 1),
      /requires the deep-frozen snapshot object returned by loadPipelineV2/,
    );
    expect(getterCalls).toBe(0);

    await expectPlainPipelineError(
      () => prepareActivationData(pipeline, {} as unknown as RunInputsSnapshot, [], "coder", 1),
      /requires the frozen run input snapshot object/,
    );

    await expectPlainPipelineError(
      () =>
        acceptActivationOutputs(
          pipeline,
          {} as unknown as PreparedActivationData,
        ),
      /requires the frozen prepared activation data object/,
    );

    await expectPlainPipelineError(
      () => prepareActivationData(secondPipeline, snap, [], "coder", 1),
      /requires the frozen run input snapshot object/,
    );
  });
});

test("27. run_output_invalid is a real typed reason but unreachable without a trusted-host race", () => {
  const error = new PipelineV2RuntimeError(
    "run_output_invalid",
    `run output "summary" source state output "coder"."report" /x is not valid JSON`,
  );
  expect(error).toBeInstanceOf(PipelineV2RuntimeError);
  expect(error.reason).toBe("run_output_invalid");
  expect(error).toBeInstanceOf(PipelineError);
  expect(isPipelineV2RuntimeError(error)).toBe(true);
});

test("28. a non-PipelineError propagates by identity through a typed region", async () => {
  await withAgentsRun(async ({ pipeline, runRoot }) => {
    const boom = new TypeError("unexpected probe failure");
    const trap = new Proxy(
      {},
      {
        get() {
          throw boom;
        },
        ownKeys() {
          throw boom;
        },
      },
    );
    let failure: unknown;
    try {
      await snapshotRunInputs(pipeline, [trap], runRoot);
    } catch (cause) {
      failure = cause;
    }
    // The retagging region converts only its own PipelineError
    // diagnostics; an unexpected exception keeps its identity and is
    // never masked or classified.
    expect(failure).toBe(boom);
    expect(failure).not.toBeInstanceOf(PipelineError);
    expect(isPipelineV2RuntimeError(failure)).toBe(false);
  });
});

test("29. run and project root infrastructure failures stay plain PipelineErrors", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);

    // The shared project directory disappearing is run infrastructure,
    // not an invalid run input.
    await rm(join(runRoot, "project"), { recursive: true, force: true });
    await expectPlainPipelineError(
      () => snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot),
      `run project root ${join(runRoot, "project")} does not exist`,
    );
    await mkdir(join(runRoot, "project"), { mode: 0o700 });

    // The run root disappearing before a preparation is run-level
    // infrastructure, not an activation preparation failure.
    await rm(runRoot, { recursive: true, force: true });
    await expectPlainPipelineError(
      () => prepareActivationData(pipeline, snap, [], "coder", 1),
      `run root ${runRoot} does not exist`,
    );
    await expectPlainPipelineError(
      () => collectRunOutputs(pipeline, snap, []),
      `run root ${runRoot} does not exist`,
    );
  });
});

test("30. invalid caller indexes and unknown state ids stay plain PipelineErrors", async () => {
  await withAgentsRun(async ({ pipeline, runRoot, sources }) => {
    const snap = await snapshotRunInputs(pipeline, ALL_AGENT_BINDINGS(sources), runRoot);
    // The activation index must be a positive safe integer.
    await expectPlainPipelineError(
      () => prepareActivationData(pipeline, snap, [], "coder", 0),
      /activation index/,
    );
    await expectPlainPipelineError(
      () => evaluateDecisionStateFromData(pipeline, snap, [], "check", 0),
      /next activation index/,
    );
    // An unknown state id is a caller-contract violation, never a
    // data-plane failure.
    await expectPlainPipelineError(
      () => prepareActivationData(pipeline, snap, [], "ghost", 1),
      `state "ghost" is not declared by the pipeline`,
    );
    await expectPlainPipelineError(
      () =>
        evaluateDecisionStateFromData(pipeline, snap, [], "coder", 3),
      `state "coder" is not a decision state; the decision data adapter exists for decision states only`,
    );
  });
});
