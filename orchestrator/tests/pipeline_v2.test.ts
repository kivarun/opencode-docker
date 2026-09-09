import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  PIPELINE_SCHEMA_VERSION,
  PipelineError,
  loadPipeline,
  parsePipelineSpec,
  planMultiStateExecution,
} from "../src/pipeline.ts";
import {
  ACTIVATION_INPUTS_ROOT,
  ACTIVATION_OUTPUTS_ROOT,
  EXECUTION_SESSION_CONTRACT,
  PIPELINE_SCHEMA_VERSION_V2,
  PROJECT_MOUNT_TARGET,
  TOOL_SESSION_CONTRACT,
  WORKER_LAUNCH_CONTRACT,
  WORKER_MOUNT_CONTRACT,
  compilePipelineV2Spec,
  loadPipelineV2,
  parsePipelineV2Spec,
  planActivationLayout,
  type ActivationLayoutPlan,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import { runAgentSmoke, type AgentSmokeDeps } from "../src/agent_smoke.ts";
import { STANDARD_AGENT_RESULT_SCHEMA } from "../src/agent_result.ts";

const FACTS_SCHEMA: Record<string, unknown> = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const NOTES_SCHEMA: Record<string, unknown> = {
  type: "object",
  required: ["revision"],
  properties: { revision: { type: "integer" } },
};

const V2_EXAMPLE_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true
  - id: review_notes
    type: directory
    protected: false
  - id: config
    type: json
    protected: true
    schema: schemas/config.schema.json

outputs:
  - id: final_report
    required: true
    source:
      state_output:
        state: architect
        output: report
  - id: facts_digest
    required: false
    source:
      state_output:
        state: architect
        output: facts

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: notes
        source:
          pipeline_input: review_notes
      - id: config
        source:
          pipeline_input: config
    outputs:
      - id: implementation
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: implementation
        source:
          state_output:
            state: coder
            output: implementation
      - id: facts
        source:
          state_output:
            state: architect
            output: facts
    outputs:
      - id: facts
        type: json
        schema: schemas/facts.schema.json
      - id: report
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: completed

  - id: completed
    type: terminal
    result: success
`;

const MINIMAL_V2_STATES = `  - id: coder
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
`;

const V2_TOP_PREFIX = "schema_version: 2\nentry_state: coder\nmax_transitions: 20\n";

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

const STANDARD_RESULT_SCHEMA_TEXT = JSON.stringify(STANDARD_AGENT_RESULT_SCHEMA, null, 2);

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-test-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  return { root, bundle };
}

async function writeV2Bundle(dirs: BundleDirs, yaml: string = V2_EXAMPLE_YAML): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "review the implementation\n");
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(dirs.bundle, "schemas", "config.schema.json"), JSON.stringify(NOTES_SCHEMA));
}

async function writeV1Bundle(dirs: BundleDirs): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), V1_PIPELINE_YAML);
  await writeFile(join(dirs.bundle, "prompts", "execute.md"), "implementation agent\n");
  await writeFile(join(dirs.bundle, "schemas", "agent-result.schema.json"), STANDARD_RESULT_SCHEMA_TEXT);
}

async function withBundle(fn: (dirs: BundleDirs) => Promise<void>): Promise<void> {
  const dirs = await makeBundleDirs();
  try {
    await fn(dirs);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

function rejectParse(yaml: string, message: RegExp | string): void {
  expect(() => parsePipelineV2Spec(yaml)).toThrow(message);
}

test("pipeline v2 schema version constant is 2", () => {
  expect(PIPELINE_SCHEMA_VERSION_V2).toBe(2);
  expect(PIPELINE_SCHEMA_VERSION).toBe(1);
});

// --- v1 regression and the v1/v2 boundary --------------------------------

test("1. v1 regression: a v1 bundle resolves with the unchanged representation", async () => {
  await withBundle(async (dirs) => {
    await writeV1Bundle(dirs);
    const resolved = await loadPipeline(dirs.bundle);
    expect(resolved.schema_version).toBe(1);
    expect(resolved.bundleRoot).toBe(await realpath(dirs.bundle));
    expect(resolved.entry_state).toBe("execute");
    expect(resolved.max_transitions).toBe(1);
    expect(resolved.inputs).toEqual([{ id: "task", path: "TASK.md", protected: true }]);
    expect(resolved.states.length).toBe(2);
    const agent = resolved.states[0];
    if (agent?.type !== "agent") {
      throw new Error("expected agent state");
    }
    expect(agent.id).toBe("execute");
    expect(agent.profile).toBe("default");
    expect(agent.inputs).toEqual(["task"]);
    expect(agent.resultSchemaPath.endsWith("schemas/agent-result.schema.json")).toBe(true);
    expect(agent.resultSchema).toEqual(STANDARD_AGENT_RESULT_SCHEMA);
    expect(agent.promptContent).toBe("implementation agent\n");
    expect(agent.timeout_seconds).toBe(3600);
    expect(agent.max_attempts).toBe(1);
    expect(agent.transitions).toEqual([{ outcome: "completed", to: "completed" }]);
    expect(resolved.states[1]).toEqual({ id: "completed", type: "terminal", result: "success" });
    const plan = planMultiStateExecution(resolved);
    expect(plan.profileNames).toEqual(["default"]);
    expect(plan.protectedInputs).toEqual([{ id: "task", path: "TASK.md", protected: true }]);
  });
});

test("2. the v2 example compiles with derived contracts and declared sources", () => {
  const spec = parsePipelineV2Spec(V2_EXAMPLE_YAML);
  expect(spec.schema_version).toBe(2);
  expect(spec.entry_state).toBe("coder");
  expect(spec.max_transitions).toBe(20);

  expect(spec.inputs).toEqual([
    { id: "task", type: "file", protected: true },
    { id: "review_notes", type: "directory", protected: false },
    { id: "config", type: "json", protected: true, schema: "schemas/config.schema.json" },
  ]);

  const coder = spec.states[0];
  if (coder?.type !== "agent") {
    throw new Error("expected coder agent state");
  }
  expect(coder.inputs).toEqual([
    { id: "task", source: { pipeline_input: "task" }, type: "file" },
    { id: "notes", source: { pipeline_input: "review_notes" }, type: "directory" },
    { id: "config", source: { pipeline_input: "config" }, type: "json" },
  ]);
  expect(coder.outputs).toEqual([{ id: "implementation", type: "file" }]);
  expect(coder.transitions).toEqual([{ outcome: "completed", to: "architect" }]);

  const architect = spec.states[1];
  if (architect?.type !== "agent") {
    throw new Error("expected architect agent state");
  }
  expect(architect.inputs).toEqual([
    { id: "task", source: { pipeline_input: "task" }, type: "file" },
    {
      id: "implementation",
      source: { state_output: { state: "coder", output: "implementation" } },
      type: "file",
    },
    {
      id: "facts",
      source: { state_output: { state: "architect", output: "facts" } },
      type: "json",
    },
  ]);
  expect(architect.outputs).toEqual([
    { id: "facts", type: "json", schema: "schemas/facts.schema.json" },
    { id: "report", type: "file" },
  ]);

  expect(spec.outputs).toEqual([
    {
      id: "final_report",
      type: "file",
      required: true,
      source: { state_output: { state: "architect", output: "report" } },
    },
    {
      id: "facts_digest",
      type: "json",
      required: false,
      source: { state_output: { state: "architect", output: "facts" } },
    },
  ]);
});

test("3. loadPipelineV2 resolves bundle files with schema snapshots and is deeply frozen", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    expect(resolved.bundleRoot).toBe(await realpath(dirs.bundle));
    expect(resolved.schema_version).toBe(2);

    expect(Object.isFrozen(resolved)).toBe(true);
    expect(Object.isFrozen(resolved.inputs)).toBe(true);
    expect(Object.isFrozen(resolved.outputs)).toBe(true);
    expect(Object.isFrozen(resolved.states)).toBe(true);

    const architect = resolved.states[1];
    if (architect?.type !== "agent") {
      throw new Error("expected agent state");
    }
    expect(architect.promptPath.endsWith("prompts/architect.md")).toBe(true);
    expect(architect.promptContent).toBe("review the implementation\n");
    expect(Object.isFrozen(architect)).toBe(true);
    expect(Object.isFrozen(architect.inputs)).toBe(true);
    expect(Object.isFrozen(architect.outputs)).toBe(true);
    expect(Object.isFrozen(architect.transitions)).toBe(true);

    const factsPort = architect.outputs[0];
    expect(factsPort?.schemaPath?.endsWith("schemas/facts.schema.json")).toBe(true);
    expect(factsPort?.schema).toEqual(FACTS_SCHEMA);
    expect(Object.isFrozen(factsPort?.schema)).toBe(true);

    // json run input keeps its declared schema at the resolved input; the
    // coder input port derives the immutable snapshot (never a path).
    const configInput = resolved.inputs.find((entry) => entry.id === "config");
    if (configInput?.schema === undefined) {
      throw new Error("expected config input schema snapshot");
    }
    expect(configInput.schema).toEqual(NOTES_SCHEMA);
    expect(configInput.schemaPath?.endsWith("schemas/config.schema.json")).toBe(true);

    const digest = resolved.outputs[1];
    if (digest?.schema === undefined) {
      throw new Error("expected facts_digest schema snapshot");
    }
    expect(digest.schema).toEqual(FACTS_SCHEMA);
    expect(digest.schemaPath?.endsWith("schemas/facts.schema.json")).toBe(true);
    expect(digest.required).toBe(false);
    expect(digest.type).toBe("json");
    expect(digest.source).toEqual({ state_output: { state: "architect", output: "facts" } });

    const coderState = resolved.states[0];
    if (coderState?.type !== "agent") {
      throw new Error("expected coder agent state");
    }
    const configPort = coderState.inputs.find((port) => port.id === "config");
    if (configPort?.schema === undefined) {
      throw new Error("expected config input port schema snapshot");
    }
    expect(configPort.schema).toEqual(NOTES_SCHEMA);
    expect(configPort.type).toBe("json");
    expect("schemaPath" in configPort).toBe(false);
    const architectFactsInput = architect.inputs.find((port) => port.id === "facts");
    if (architectFactsInput?.schema === undefined) {
      throw new Error("expected architect facts input schema snapshot");
    }
    expect(architectFactsInput.schema).toEqual(FACTS_SCHEMA);
    expect("schemaPath" in architectFactsInput).toBe(false);
  });
});

test("4. exact-field validation rejects unknown and missing fields at every level", () => {
  rejectParse(
    `${V2_TOP_PREFIX}extra: 1\ninputs: []\noutputs: []\nstates:\n${MINIMAL_V2_STATES}`,
    /pipeline has unknown field "extra"/,
  );
  rejectParse(
    "schema_version: 2\nentry_state: coder\ninputs: []\noutputs: []\nstates:\n".concat(MINIMAL_V2_STATES),
    /pipeline is missing required field "max_transitions"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\n    path: TASK.md\noutputs: []\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline input 0 has unknown field "path"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\noutputs:\n  - id: out\n    required: true\n    source:\n      state_output:\n        state: coder\n        output: implementation\n    mounts: x\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline output 0 has unknown field "mounts"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    result_schema: schemas/x.json\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /agent state "coder" has unknown field "result_schema"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs:\n      - id: task\n        source:\n          pipeline_input: task\n        target: /some/path\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /input port 0 has unknown field "target"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat("inputs: []\noutputs: []\nstates:\n")
      .concat(
        MINIMAL_V2_STATES.replace(
          "    prompt: prompts/coder.md\n",
          "    prompt: prompts/coder.md\n    image: img\n",
        ),
      ),
    /agent state "coder" has unknown field "image"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat("env: {}\ninputs: []\noutputs: []\nstates:\n")
      .concat(MINIMAL_V2_STATES),
    /pipeline has unknown field "env"/,
  );
});

test("5. port type and schema union validation", () => {
  rejectParse(
    V2_TOP_PREFIX
      .concat("inputs:\n  - id: task\n    type: text\n    protected: true\noutputs: []\nstates:\n")
      .concat(MINIMAL_V2_STATES),
    /pipeline input "task" type must be one of "file", "directory" or "json"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\n    schema: schemas/task.json\noutputs: []\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline input "task" with type "file" must not declare a schema/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat("inputs:\n  - id: data\n    type: json\n    protected: true\noutputs: []\nstates:\n")
      .concat(MINIMAL_V2_STATES),
    /pipeline input "data" with type "json" must declare a schema/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    type: json\n    required: true\n    source:\n      pipeline_input: task\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline output 0 has unknown field "type"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    required: true\n    schema: schemas/out.json\n    source:\n      pipeline_input: task\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline output 0 has unknown field "schema"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs:\n      - id: impl\n        type: file\n        schema: schemas/impl.json\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /output port "impl" with type "file" must not declare a schema/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs:\n      - id: facts\n        type: json\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /output port "facts" with type "json" must declare a schema/,
  );
});

test("6. port sources must have exactly one form", () => {
  const stateWithSource = (source: string): string =>
    MINIMAL_V2_STATES.replace(
      "    inputs: []\n",
      `    inputs:\n      - id: t\n        source:\n${source}`,
    );
  const doc = (source: string): string =>
    V2_TOP_PREFIX.concat("inputs: []\noutputs: []\nstates:\n").concat(stateWithSource(source));

  rejectParse(
    doc(
      "          pipeline_input: task\n          state_output:\n            state: coder\n            output: implementation\n",
    ),
    /must declare exactly one of "pipeline_input" or "state_output"/,
  );
  rejectParse(doc("          other: 1\n"), /input port 0 source has unknown field "other"/);
  rejectParse(
    V2_TOP_PREFIX.concat("inputs: []\noutputs: []\nstates:\n").concat(
      stateWithSource('          pipeline_input: "x y"\n'),
    ),
    /source pipeline_input id "x y" is not a safe identifier/,
  );
  rejectParse(
    V2_TOP_PREFIX.concat("inputs: []\noutputs: []\nstates:\n").concat(
      MINIMAL_V2_STATES.replace(
        "    inputs: []\n",
        "    inputs:\n      - id: t\n        source: task\n",
      ),
    ),
    /input port 0 source is not a YAML mapping/,
  );
});

test("7. duplicate and unknown references are rejected", () => {
  const prefix = V2_TOP_PREFIX;
  const states = MINIMAL_V2_STATES;

  rejectParse(
    prefix
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\n  - id: task\n    type: file\n    protected: true\noutputs: []\nstates:\n",
      )
      .concat(states),
    /pipeline declares input "task" more than once/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\noutputs:\n  - id: out\n    required: true\n    source:\n      pipeline_input: task\n  - id: out\n    required: true\n    source:\n      state_output:\n        state: coder\n        output: implementation\nstates:\n",
      )
      .concat(states),
    /pipeline declares output "out" more than once/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    required: true\n    source:\n      pipeline_input: missing\nstates:\n",
      )
      .concat(states),
    /pipeline output "out" references undeclared pipeline input "missing"/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    required: true\n    source:\n      state_output:\n        state: nowhere\n        output: x\nstates:\n",
      )
      .concat(states),
    /pipeline output "out" references undeclared state "nowhere"/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    required: true\n    source:\n      state_output:\n        state: done\n        output: x\nstates:\n",
      )
      .concat(states),
    /pipeline output "out" references state "done" which declares no output ports/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs: []\noutputs:\n  - id: out\n    required: true\n    source:\n      state_output:\n        state: coder\n        output: missing\nstates:\n",
      )
      .concat(states),
    /pipeline output "out" references undeclared output "missing" of state "coder"/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs:\n      - id: t\n        source:\n          pipeline_input: task\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /input port "t" references undeclared pipeline input "task"/,
  );
  rejectParse(
    prefix
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs:\n      - id: t\n        source:\n          pipeline_input: task\n      - id: t\n        source:\n          pipeline_input: task\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /declares input port "t" more than once/,
  );
});

test("8. type propagation derives agent input types from sources", () => {
  const spec = parsePipelineV2Spec(V2_EXAMPLE_YAML);
  const architect = spec.states[1];
  if (architect?.type !== "agent") {
    throw new Error("expected architect");
  }
  expect(architect.inputs.map((port) => port.type)).toEqual(["file", "file", "json"]);
  const coder = spec.states[0];
  if (coder?.type !== "agent") {
    throw new Error("expected coder");
  }
  expect(coder.inputs.map((port) => port.type)).toEqual(["file", "directory", "json"]);
});

test("9. self-references and cycles between state outputs compile", () => {
  const spec = parsePipelineV2Spec(V2_EXAMPLE_YAML);
  const architect = spec.states[1];
  if (architect?.type !== "agent") {
    throw new Error("expected architect");
  }
  const factsPort = architect.inputs.find((port) => port.id === "facts");
  expect(factsPort?.source).toEqual({ state_output: { state: "architect", output: "facts" } });
  expect(factsPort?.type).toBe("json");
});

test("10. run outputs derive type and schema from their source", () => {
  const spec = parsePipelineV2Spec(V2_EXAMPLE_YAML);
  expect(spec.outputs.map((output) => output.type)).toEqual(["file", "json"]);
  const factsDigest = spec.outputs[1];
  const finalReport = spec.outputs[0];
  expect(finalReport !== undefined && "type" in finalReport).toBe(true);
  expect(factsDigest !== undefined && "schema" in factsDigest).toBe(false);
});

test("11. v2 documents cannot declare user port paths or mount options", () => {
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs:\n  - id: task\n    type: file\n    protected: true\n    target: /host/task\noutputs: []\nstates:\n",
      )
      .concat(MINIMAL_V2_STATES),
    /pipeline input 0 has unknown field "target"/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n    mounts:\n      - x\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /agent state "coder" has unknown field "mounts"/,
  );
  rejectParse(
    V2_TOP_PREFIX.concat("env: {}\ninputs: []\noutputs: []\nstates:\n").concat(MINIMAL_V2_STATES),
    /pipeline has unknown field "env"/,
  );
  rejectParse(
    V2_TOP_PREFIX.concat("image: some-image\ninputs: []\noutputs: []\nstates:\n").concat(MINIMAL_V2_STATES),
    /pipeline has unknown field "image"/,
  );
});

test("12. graph shape is shared with v1 semantics", () => {
  rejectParse(
    "schema_version: 2\nentry_state: missing\nmax_transitions: 20\ninputs: []\noutputs: []\nstates:\n".concat(
      MINIMAL_V2_STATES,
    ),
    /entry_state "missing" does not name a declared state/,
  );
  rejectParse(
    "schema_version: 2\nentry_state: coder\nmax_transitions: 20\ninputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: coder\n",
    /pipeline must declare at least one terminal state/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: done\n  - id: orphan\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: orphan\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /agent state "orphan" is not reachable from entry_state/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: coder\n      - outcome: completed\n        to: done\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /declares outcome "completed" more than once/,
  );
  rejectParse(
    V2_TOP_PREFIX
      .concat(
        "inputs: []\noutputs: []\nstates:\n  - id: coder\n    type: agent\n    profile: coder\n    prompt: prompts/coder.md\n    inputs: []\n    outputs: []\n    timeout_seconds: 60\n    max_attempts: 1\n    transitions:\n      - outcome: completed\n        to: nowhere\n  - id: done\n    type: terminal\n    result: success\n",
      ),
    /targets unknown state "nowhere"/,
  );
});

test("13. safe ids and schema version are enforced", () => {
  rejectParse(
    "schema_version: 1\nentry_state: a\nmax_transitions: 1\ninputs: []\noutputs: []\nstates:\n  - id: done\n    type: terminal\n    result: success\n",
    /schema_version 1, expected 2/,
  );
  rejectParse(
    "schema_version: 2\nentry_state: ../evil\nmax_transitions: 1\ninputs: []\noutputs: []\nstates:\n".concat(
      MINIMAL_V2_STATES,
    ),
    /pipeline entry_state "\.\.\/evil" is not a safe identifier/,
  );
  rejectParse("[]", /not a YAML mapping/);
  rejectParse("schema_version: [unclosed", /not valid YAML/);
});

test("14. json schema files load through the bundle containment; internal symlinks allowed", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    await symlink(join(dirs.bundle, "schemas", "facts.schema.json"), join(dirs.bundle, "schemas", "alias.json"));
    await writeFile(
      join(dirs.bundle, "pipeline.yaml"),
      V2_EXAMPLE_YAML.replaceAll("schemas/facts.schema.json", "schemas/alias.json"),
    );
    const resolved = await loadPipelineV2(dirs.bundle);
    const architect = resolved.states[1];
    if (architect?.type !== "agent") {
      throw new Error("expected agent");
    }
    const factsPort = architect.outputs.find((port) => port.id === "facts");
    expect(factsPort?.schema).toEqual(FACTS_SCHEMA);
    // an internal symlink resolves to the canonical target path
    expect(factsPort?.schemaPath?.endsWith("schemas/facts.schema.json")).toBe(true);
  });
});

test("15. broken and escaping schema files are rejected", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const escapeTarget = join(dirs.root, "outside.json");
    await writeFile(escapeTarget, "{}");
    await symlink(escapeTarget, join(dirs.bundle, "schemas", "escape.json"));
    await writeV2Bundle(dirs, V2_EXAMPLE_YAML.replaceAll("schemas/facts.schema.json", "schemas/escape.json"));
    expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(/resolves outside the pipeline bundle/);

    await writeFile(join(dirs.bundle, "schemas", "broken.json"), "{ not json");
    await writeV2Bundle(dirs, V2_EXAMPLE_YAML.replace("schemas/facts.schema.json", "schemas/broken.json"));
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(/is not valid JSON/);

    await writeV2Bundle(dirs, V2_EXAMPLE_YAML.replace("schemas/facts.schema.json", "schemas/missing.json"));
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(/is not accessible/);
  });
});

test("16. the compiled v2 pipeline is an engine-owned deep-frozen snapshot", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    const coder = resolved.states[0];
    if (coder?.type !== "agent") {
      throw new Error("expected agent");
    }
    expect(Object.isFrozen(resolved)).toBe(true);
    expect(Object.isFrozen(coder)).toBe(true);
    expect(Object.isFrozen(coder.inputs)).toBe(true);
    expect(Object.isFrozen(coder.inputs[0])).toBe(true);
    expect(Object.isFrozen(coder.inputs[0]?.source)).toBe(true);
    expect(Object.isFrozen(coder.transitions)).toBe(true);
    expect(Object.isFrozen(resolved.outputs[0])).toBe(true);
    expect(Object.isFrozen(resolved.outputs[1]?.source)).toBe(true);
  });
});

test("17. the activation layout plan has fixed targets, flags and declaration order", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    const plan = planActivationLayout(resolved, "architect");
    expect(plan.state_id).toBe("architect");
    expect(plan.project).toEqual({ target: PROJECT_MOUNT_TARGET, read_only: false });
    expect(plan.inputs_root).toBe(ACTIVATION_INPUTS_ROOT);
    expect(plan.outputs_root).toBe(ACTIVATION_OUTPUTS_ROOT);
    expect(plan.reject_undeclared_outputs).toBe(true);

    expect(plan.input_ports).toEqual([
      {
        id: "task",
        source: { pipeline_input: "task" },
        type: "file",
        target: "/pipeline/inputs/task",
        read_only: true,
      },
      {
        id: "implementation",
        source: { state_output: { state: "coder", output: "implementation" } },
        type: "file",
        target: "/pipeline/inputs/implementation",
        read_only: true,
      },
      {
        id: "facts",
        source: { state_output: { state: "architect", output: "facts" } },
        type: "json",
        schema: FACTS_SCHEMA,
        target: "/pipeline/inputs/facts",
        read_only: true,
      },
    ]);
    expect(plan.output_ports).toEqual([
      {
        id: "facts",
        type: "json",
        schema: FACTS_SCHEMA,
        target: "/pipeline/outputs/facts",
        read_only: false,
      },
      {
        id: "report",
        type: "file",
        target: "/pipeline/outputs/report",
        read_only: false,
      },
    ]);

    const again = planActivationLayout(resolved, "architect");
    expect(again).not.toBe(plan);
    expect(again).toEqual(plan);
  });
});

test("18. the plan contains no credentials, env values, host paths or schema paths", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    for (const stateId of ["coder", "architect"]) {
      const plan = planActivationLayout(resolved, stateId);
      const serialized = JSON.stringify(plan);
      expect(serialized).not.toContain(resolved.bundleRoot);
      expect(serialized).not.toContain(dirs.root);
      expect(serialized).not.toContain("prompts/");
      expect(serialized).not.toContain("schemas/");
      expect(serialized).not.toContain("schemaPath");
      expect(serialized).not.toContain("profile");
      expect(serialized).not.toContain("prompt");
      expect(serialized).not.toContain("timeout");
      expect(serialized).not.toContain("transition");
      expect(serialized).not.toMatch(/dh[cat]_/);
      expect(serialized).not.toMatch(/bearer|token|credential|env|image|secret/i);
      for (const port of plan.input_ports) {
        expect(port.target.startsWith("/pipeline/inputs/")).toBe(true);
      }
      for (const port of plan.output_ports) {
        expect(port.target.startsWith("/pipeline/outputs/")).toBe(true);
      }
      // the schema VALUE travels, never a schema path
      if (stateId === "architect") {
        expect(plan.input_ports.find((port) => port.id === "facts")?.schema).toEqual(FACTS_SCHEMA);
      }
    }
  });
});

test("19. the plan is deeply frozen and rejects mutation attempts", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    const plan = planActivationLayout(resolved, "architect");
    expect(Object.isFrozen(plan)).toBe(true);
    expect(Object.isFrozen(plan.input_ports)).toBe(true);
    expect(Object.isFrozen(plan.output_ports)).toBe(true);
    expect(Object.isFrozen(plan.project)).toBe(true);
    expect(Object.isFrozen(plan.input_ports[0])).toBe(true);
    expect(Object.isFrozen(plan.input_ports[0]?.source)).toBe(true);
    expect(Object.isFrozen(plan.output_ports[0])).toBe(true);
    expect(Object.isFrozen(plan.output_ports[0]?.schema)).toBe(true);
    const ports = plan.input_ports as unknown as { push(value: unknown): number };
    expect(() => ports.push(plan.input_ports[0] as never)).toThrow();
  });
});

test("20. mutating a hand-built pipeline object cannot change an already-built plan", () => {
  const resolved = {
    schema_version: 2 as const,
    bundleRoot: "/tmp/hand-built",
    entry_state: "coder",
    max_transitions: 20,
    inputs: [{ id: "task", type: "file" as const, protected: true }],
    outputs: [],
    states: [
      {
        id: "coder",
        type: "agent" as const,
        profile: "coder",
        promptPath: "/tmp/hand-built/prompts/coder.md",
        promptContent: "p",
        inputs: [
          {
            id: "task",
            source: { pipeline_input: "task" },
            type: "file" as const,
          },
        ],
        outputs: [
          {
            id: "facts",
            type: "json" as const,
            schema: { type: "object", marker: "original" },
          },
        ],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [{ outcome: "completed", to: "done" }],
      },
      { id: "done", type: "terminal" as const, result: "success" as const },
    ],
  };
  const plan = planActivationLayout(resolved as unknown as ResolvedPipelineV2, "coder");
  const factsPort = plan.output_ports.find((port) => port.id === "facts");
  expect(factsPort?.schema).toEqual({ type: "object", marker: "original" });

  const mutated = resolved as unknown as {
    states: { outputs: { schema: Record<string, unknown> }[]; inputs: { source: unknown }[] }[];
  };
  mutated.states[0]!.outputs[0]!.schema.marker = "changed";
  mutated.states[0]!.inputs[0]!.source = { pipeline_input: "other" };
  expect(factsPort?.schema).toEqual({ type: "object", marker: "original" });
  expect(plan.input_ports[0]?.source).toEqual({ pipeline_input: "task" });
});

test("21. planActivationLayout rejects unknown and terminal states", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const resolved = await loadPipelineV2(dirs.bundle);
    expect(() => planActivationLayout(resolved, "missing")).toThrow(
      /state "missing" is not declared by the pipeline/,
    );
    expect(() => planActivationLayout(resolved, "completed")).toThrow(
      /state "completed" is not an agent state/,
    );
  });
});

test("22. session capability contracts are frozen and separated", () => {
  expect(EXECUTION_SESSION_CONTRACT).toEqual({
    type: "execution",
    scope: "run_root",
    bearer_shared_with_worker: false,
  });
  expect(TOOL_SESSION_CONTRACT).toEqual({
    type: "tool",
    scope: "project",
    bearer_shared_with_worker: true,
  });
  expect(Object.isFrozen(EXECUTION_SESSION_CONTRACT)).toBe(true);
  expect(Object.isFrozen(TOOL_SESSION_CONTRACT)).toBe(true);
  // helper socket projection is not a session capability
  expect("helper_socket_projected" in EXECUTION_SESSION_CONTRACT).toBe(false);
  expect("helper_socket_projected" in TOOL_SESSION_CONTRACT).toBe(false);
  expect(Object.isFrozen(WORKER_MOUNT_CONTRACT)).toBe(true);
  expect(WORKER_MOUNT_CONTRACT).toEqual([
    { source: "project", target: "/workspace", read_only: false },
    { source: "prepared_activation_inputs", target: "/pipeline/inputs", read_only: true },
    { source: "activation_outputs", target: "/pipeline/outputs", read_only: false },
  ]);
});

test("22a. the Worker Launch contract separates socket transport from Tool authority", () => {
  expect(WORKER_LAUNCH_CONTRACT).toEqual({
    launched_via: "execution_session",
    helper_socket: "projected",
    socket_grants: "transport_only",
    tool_bearer_is_authority: true,
    execution_bearer_shared_with_worker: false,
    nested_container_pipeline_port_access: false,
  });
  expect(Object.isFrozen(WORKER_LAUNCH_CONTRACT)).toBe(true);
});

test("23. the production path rejects v2 before Launcher auth and before any Session", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    const workspace = join(dirs.root, "workspace");
    await mkdir(workspace, { recursive: true });
    const state = join(dirs.root, "state");
    await mkdir(state, { recursive: true });
    const configDir = join(dirs.root, "config", "docker-helper");
    await mkdir(configDir, { recursive: true, mode: 0o700 });
    const credentialFile = join(configDir, "credential.token");
    await writeFile(credentialFile, `dhc_${"a".repeat(64)}\n`, { mode: 0o600 });

    const authCalls: number[] = [];
    const cliCalls: string[][] = [];
    const runner = async (args: string[]): Promise<{ code: number }> => {
      cliCalls.push(args);
      throw new Error("cli must not be called for a v2 pipeline");
    };
    const deps: AgentSmokeDeps = {
      cli: runner,
      fetchAuth: async () => {
        authCalls.push(1);
        throw new Error("auth must not be reached for a v2 pipeline");
      },
      config: { socketPath: "/run/docker-helper/test.sock", credentialFile },
      stateDirPath: state,
      baseEnv: {},
    };

    const outcome = await runAgentSmoke(
      { workspace, configRoot: join(dirs.root, "operator-config"), pipelineRoot: dirs.bundle },
      deps,
    );

    expect(outcome.ok).toBe(false);
    expect(outcome.exitCode).toBe(1);
    expect(outcome.runId).toBe("");
    expect(outcome.status).toBe("failed");
    expect(outcome.detail).toBe("pipeline schema version 2 is not executable yet");
    expect(authCalls).toEqual([]);
    expect(cliCalls).toEqual([]);
    await expect(readFile(join(state, "pipeline-runs"))).rejects.toThrow();
  });
});

test("24. loadPipeline rejects any version-2 document with the exact production error", async () => {
  await withBundle(async (dirs) => {
    const v1ShapedVersion2 = V1_PIPELINE_YAML.replace("schema_version: 1", "schema_version: 2");
    await writeFile(join(dirs.bundle, "pipeline.yaml"), v1ShapedVersion2);
    await writeFile(join(dirs.bundle, "prompts", "execute.md"), "implementation agent\n");
    await writeFile(join(dirs.bundle, "schemas", "agent-result.schema.json"), STANDARD_RESULT_SCHEMA_TEXT);
    // no "genuine v2" sniffing: any schema_version 2 document is rejected
    // with the same explicit message, v1-shaped or not
    expect(loadPipeline(dirs.bundle)).rejects.toThrow(
      "pipeline schema version 2 is not executable yet",
    );
    expect(() => parsePipelineSpec(v1ShapedVersion2)).toThrow(
      "pipeline schema version 2 is not executable yet",
    );
  });
});

test("25. loadPipeline rejects the v2 example with the exact production error", async () => {
  await withBundle(async (dirs) => {
    await writeV2Bundle(dirs);
    try {
      await loadPipeline(dirs.bundle);
      throw new Error("expected loadPipeline to reject a v2 bundle");
    } catch (cause) {
      expect(cause instanceof PipelineError).toBe(true);
      expect((cause as Error).message).toBe("pipeline schema version 2 is not executable yet");
    }
    expect(() => parsePipelineSpec(V2_EXAMPLE_YAML)).toThrow(
      "pipeline schema version 2 is not executable yet",
    );
  });
});

test("26. compilePipelineV2Spec works on plain objects (no YAML needed)", () => {
  const spec = compilePipelineV2Spec({
    schema_version: 2,
    entry_state: "done",
    max_transitions: 5,
    inputs: [],
    outputs: [],
    states: [{ id: "done", type: "terminal", result: "success" }],
  });
  expect(spec.entry_state).toBe("done");
  expect(spec.max_transitions).toBe(5);
  expect(spec.states[0]).toEqual({ id: "done", type: "terminal", result: "success" });
});

test("27. the planner rejects forged port ids before building target paths", () => {
  const forged = {
    schema_version: 2,
    bundleRoot: "/tmp/forged",
    entry_state: "coder",
    max_transitions: 20,
    inputs: [],
    outputs: [],
    states: [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/tmp/forged/prompts/coder.md",
        promptContent: "p",
        inputs: [{ id: "../evil", source: { pipeline_input: "task" }, type: "file" }],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [{ outcome: "completed", to: "done" }],
      },
      { id: "done", type: "terminal", result: "success" },
    ],
  } as unknown as ResolvedPipelineV2;
  try {
    planActivationLayout(forged, "coder");
    throw new Error("expected the planner to reject a forged port id");
  } catch (cause) {
    expect((cause as Error).message).toMatch(/is not a safe identifier/);
    expect((cause as Error).message).not.toContain("/pipeline/inputs/../evil");
  }
});

test("28. the planner is fail-closed against corrupted resolved objects", () => {
  const baseStates = (): unknown[] => [
    {
      id: "coder",
      type: "agent",
      profile: "coder",
      promptPath: "/tmp/x/prompts/coder.md",
      promptContent: "p",
      inputs: [{ id: "task", source: { pipeline_input: "task" }, type: "file" }],
      outputs: [],
      timeout_seconds: 60,
      max_attempts: 1,
      transitions: [{ outcome: "completed", to: "done" }],
    },
    { id: "done", type: "terminal", result: "success" },
  ];
  const basePipeline = (states: unknown): ResolvedPipelineV2 =>
    ({
      schema_version: 2,
      bundleRoot: "/tmp/x",
      entry_state: "coder",
      max_transitions: 20,
      inputs: [{ id: "task", type: "file", protected: true }],
      outputs: [],
      states,
    }) as unknown as ResolvedPipelineV2;

  // not a v2 resolved pipeline at all
  expect(() => planActivationLayout({} as unknown as ResolvedPipelineV2, "coder")).toThrow(
    /resolved pipeline has schema_version undefined, expected 2/,
  );
  expect(() => planActivationLayout(null as unknown as ResolvedPipelineV2, "coder")).toThrow(
    /resolved pipeline is not a YAML mapping/,
  );

  // unsafe requested state id
  expect(() =>
    planActivationLayout(
      { schema_version: 2, bundleRoot: "/x", entry_state: "coder", max_transitions: 1, inputs: [], outputs: [], states: [] } as unknown as ResolvedPipelineV2,
      "../evil",
    ),
  ).toThrow(/activation layout state id "\.\.\/evil" is not a safe identifier/);

  // corrupted states array element
  expect(() =>
    planActivationLayout(
      { schema_version: 2, bundleRoot: "/x", entry_state: "coder", max_transitions: 20, inputs: [], outputs: [], states: [null] } as unknown as ResolvedPipelineV2,
      "coder",
    ),
  ).toThrow(/resolved pipeline state is not a YAML mapping/);

  // non-agent state
  expect(() =>
    planActivationLayout(
      {
        schema_version: 2,
        bundleRoot: "/x",
        entry_state: "done",
        max_transitions: 20,
        inputs: [],
        outputs: [],
        states: [{ id: "done", type: "terminal", result: "success" }],
      } as unknown as ResolvedPipelineV2,
      "done",
    ),
  ).toThrow(/state "done" is not an agent state/);

  const plannerError = (states: unknown, message: RegExp): void => {
    expect(() => planActivationLayout(basePipelineWithStates(states), "coder")).toThrow(
      message,
    );
  };
  const basePipelineWithStates = (states: unknown): ResolvedPipelineV2 =>
    ({
      schema_version: 2,
      bundleRoot: "/tmp/x",
      entry_state: "coder",
      max_transitions: 20,
      inputs: [{ id: "task", type: "file", protected: true }],
      outputs: [],
      states,
    }) as unknown as ResolvedPipelineV2;

  // corrupted port object
  plannerError([null], /state "coder" inputs|is not a YAML mapping/);
  // unknown port type
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "p",
        promptPath: "/x",
        promptContent: "p",
        inputs: [{ id: "t", source: { pipeline_input: "task" }, type: "text" }],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /input port 0 of agent state "coder" type must be one of/,
  );
  // duplicate port ids
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [
          { id: "t", source: { pipeline_input: "task" }, type: "file" },
          { id: "t", source: { pipeline_input: "task" }, type: "file" },
        ],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /declares input port "t" more than once/,
  );
  // corrupt source union: both forms
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [
          {
            id: "t",
            source: { pipeline_input: "task", state_output: { state: "coder", output: "o" } },
            type: "file",
          },
        ],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /must declare exactly one of "pipeline_input" or "state_output"/,
  );
  // corrupt source union: neither form
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [{ id: "t", source: {}, type: "file" }],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /must declare exactly one of "pipeline_input" or "state_output"/,
  );
  // json port without a valid JSON schema snapshot
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [{ id: "t", source: { pipeline_input: "task" }, type: "json" }],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /json port without a valid JSON schema snapshot/,
  );
  // json port with a non-JSON snapshot value (function smuggled in)
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [
          {
            id: "t",
            source: { pipeline_input: "task" },
            type: "json",
            schema: { bad: () => "x" },
          },
        ],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /json port without a valid JSON schema snapshot/,
  );
  // unexpected extra field on a port
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [
          {
            id: "t",
            source: { pipeline_input: "task" },
            type: "file",
            target: "/host/leak",
          },
        ],
        outputs: [],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /input port 0 of agent state "coder" has unknown field "target"/,
  );
  // duplicate output port ids
  plannerError(
    [
      {
        id: "coder",
        type: "agent",
        profile: "coder",
        promptPath: "/x",
        promptContent: "p",
        inputs: [],
        outputs: [
          { id: "a", type: "file" },
          { id: "a", type: "file" },
        ],
        timeout_seconds: 60,
        max_attempts: 1,
        transitions: [],
      },
    ],
    /declares output port "a" more than once/,
  );

  expect(basePipelineWithStates).toBeDefined();
});
