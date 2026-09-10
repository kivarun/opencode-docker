import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  DECISION_RESERVED_OUTCOMES,
  evaluatePipelineDecisionState,
  loadPipelineV2,
  parsePipelineV2Spec,
  planActivationLayout,
  type PipelineDecisionStateResult,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import { evaluateDecision, parseDecisionModel, type DecisionOutcome } from "../src/decision.ts";

const REPO_ROOT = join(import.meta.dir, "..", "..");

const FACTS_SCHEMA: Record<string, unknown> = {
  type: "object",
  required: ["f1", "f2"],
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

/**
 * Small non-legacy model used for the adapter and load tests: two facts, two
 * decisions, one consistency relation, one hard constraint, two ordered
 * rules. No stage, role or oracle names appear anywhere.
 */
const SIMPLE_MODEL_YAML = `
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

const AGENT_STATE_YAML = `  - id: coder
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
`;

const REWORK_AGENT_STATE_YAML = `  - id: rework
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
`;

const SIMPLE_DECISION_STATE_YAML = `  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
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

const DECISION_FROM_PIPELINE_INPUT_YAML = `
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
${SIMPLE_DECISION_STATE_YAML}
${REWORK_AGENT_STATE_YAML}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** The facts port comes from an agent output port (json with schema). */
const DECISION_FROM_STATE_OUTPUT_YAML = `
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
${AGENT_STATE_YAML}
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
      - outcome: alpha
        to: done
      - outcome: beta
        to: rework
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
${REWORK_AGENT_STATE_YAML}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundle(modelYaml: string = SIMPLE_MODEL_YAML): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-decision-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(bundle, "decisions", "model.yaml"), modelYaml);
  return { root, bundle };
}

async function writePipeline(dirs: BundleDirs, yaml: string): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
}

async function withBundle(
  modelYaml: string | undefined,
  fn: (dirs: BundleDirs) => Promise<void>,
): Promise<void> {
  const dirs = await makeBundle(modelYaml ?? SIMPLE_MODEL_YAML);
  try {
    await fn(dirs);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

const SELECTED_FACTS = { f1: true, f2: false };

// --- compile: decision state shape ---------------------------------------

test("a decision state compiles with derived json input and preserved transition order", async () => {
  await withBundle(undefined, async (dirs) => {
    await writePipeline(dirs, DECISION_FROM_PIPELINE_INPUT_YAML);
    const spec = parsePipelineV2Spec(DECISION_FROM_PIPELINE_INPUT_YAML);
    const decision = spec.states[0];
    if (decision === undefined || decision.type !== "decision") {
      throw new Error("expected decision state first");
    }
    expect(decision.id).toBe("check");
    expect(decision.model).toBe("decisions/model.yaml");
    expect(decision.inputs).toEqual([
      { id: "facts", source: { pipeline_input: "facts_seed" }, type: "json" },
    ]);
    expect(decision.transitions.map((entry) => entry.outcome)).toEqual([
      "alpha",
      "beta",
      "uncovered",
      "inconsistent_facts",
      "invalid_facts",
    ]);
    const resolved = await loadPipelineV2(dirs.bundle);
    const state = resolved.states[0];
    if (state === undefined || state.type !== "decision") {
      throw new Error("expected resolved decision state");
    }
    expect(resolved.entry_state).toBe("check");
    expect(state.modelPath).toBe(await realpath(join(dirs.bundle, "decisions", "model.yaml")));
    expect(state.inputs).toEqual([
      {
        id: "facts",
        source: { pipeline_input: "facts_seed" },
        type: "json",
        schema: FACTS_SCHEMA,
      },
    ]);
    expect(Object.isFrozen(state)).toBe(true);
    expect(Object.isFrozen(state.model)).toBe(true);
    expect(Object.isFrozen(state.model.factIds)).toBe(true);
    expect(Object.isFrozen(state.transitions)).toBe(true);
    expect(Object.isFrozen(state.inputs)).toBe(true);
  });
});

test("a decision state input port may source a state output (json with schema)", async () => {
  await withBundle(undefined, async (dirs) => {
    await writePipeline(dirs, DECISION_FROM_STATE_OUTPUT_YAML);
    const resolved = await loadPipelineV2(dirs.bundle);
    const decision = resolved.states.find((state) => state.id === "check");
    if (decision === undefined || decision.type !== "decision") {
      throw new Error("expected decision state");
    }
    expect(decision.inputs).toEqual([
      {
        id: "facts",
        source: { state_output: { state: "coder", output: "facts" } },
        type: "json",
        schema: FACTS_SCHEMA,
      },
    ]);
  });
});

test("a decision state input source must resolve to json", async () => {
  const yaml = `
schema_version: 2
entry_state: check
max_transitions: 20

inputs:
  - id: task_file
    type: file
    protected: false

outputs: []

states:
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: task_file
    transitions:
      - outcome: alpha
        to: done
      - outcome: beta
        to: done
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /decision state "check" input port "facts" source must resolve to type "json", got "file"/,
  );
});

test("a decision state input source from a non-json agent output is rejected", () => {
  const yaml = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []
outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
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
            output: report
    transitions:
      - outcome: alpha
        to: done
      - outcome: beta
        to: done
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /decision state "check" input port "facts" source must resolve to type "json", got "file"/,
  );
});

test("a decision state cannot be the source of a data port (no output ports)", () => {
  const yaml = `
schema_version: 2
entry_state: check
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json
outputs:
  - id: digest
    required: false
    source:
      state_output:
        state: check
        output: facts
states:
${SIMPLE_DECISION_STATE_YAML}
${REWORK_AGENT_STATE_YAML}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /pipeline output "digest" references state "check" which declares no output ports/,
  );
});

test("a decision state must declare exactly one input port", async () => {
  await withBundle(undefined, async (dirs) => {
    const zero = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      `    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
`,
      `    inputs: []
`,
    );
    await writePipeline(dirs, zero);
    expect(() => parsePipelineV2Spec(zero)).toThrow(
      /decision state "check" must declare exactly one input data port, got 0/,
    );

    const two = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      `      - id: facts
        source:
          pipeline_input: facts_seed
`,
      `      - id: facts
        source:
          pipeline_input: facts_seed
      - id: more
        source:
          pipeline_input: facts_seed
`,
    );
    await writePipeline(dirs, two);
    expect(() => parsePipelineV2Spec(two)).toThrow(
      /decision state "check" must declare exactly one input data port, got 2/,
    );
  });
});

test("decision state unknown and forbidden fields are rejected", () => {
  const forbidden = [
    "profile",
    "prompt",
    "outputs",
    "timeout_seconds",
    "max_attempts",
    "image",
    "env",
    "mounts",
    "command",
    "credentials",
    "paths",
  ];
  for (const field of forbidden) {
    const injected = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "  - id: check\n    type: decision\n",
      `  - id: check\n    type: decision\n    ${field}: whatever\n`,
    );
    expect(() => parsePipelineV2Spec(injected)).toThrow(
      new RegExp(`decision state "check" has unknown field "${field}"`),
    );
  }
  const unknown = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
    "  - id: check\n    type: decision\n",
    '  - id: check\n    type: decision\n    whatever: 1\n',
  );
  expect(() => parsePipelineV2Spec(unknown)).toThrow(
    /decision state "check" has unknown field "whatever"/,
  );
});

test("the decision input port does not re-declare its type", async () => {
  await withBundle(undefined, async (dirs) => {
    const typed = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "      - id: facts\n        source:",
      "      - id: facts\n        type: json\n        source:",
    );
    await writePipeline(dirs, typed);
    expect(() => parsePipelineV2Spec(typed)).toThrow(
      /decision state "check" input port 0 has unknown field "type"/,
    );
  });
});

// --- graph shape through decision states ----------------------------------

test("duplicate decision outcomes are rejected by the shared graph shape", () => {
  const yaml = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
    "      - outcome: beta\n        to: rework\n",
    "      - outcome: alpha\n        to: rework\n",
  );
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /decision state "check" declares outcome "alpha" more than once/,
  );
});

test("decision transition targets are validated by the shared graph shape", () => {
  const yaml = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
    "      - outcome: beta\n        to: rework\n",
    "      - outcome: beta\n        to: nowhere\n",
  );
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /decision state "check" transition outcome "beta" targets unknown state "nowhere"/,
  );
});

test("an unreachable decision state is rejected", () => {
  const yaml = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []
outputs: []

states:
  - id: coder
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
${SIMPLE_DECISION_STATE_YAML}
${REWORK_AGENT_STATE_YAML}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
  expect(() => parsePipelineV2Spec(yaml)).toThrow(
    /decision state "check" is not reachable from entry_state/,
  );
});

test("an agent state reachable only through a decision state compiles", async () => {
  await withBundle(undefined, async (dirs) => {
    await writePipeline(dirs, DECISION_FROM_PIPELINE_INPUT_YAML);
    await expect(loadPipelineV2(dirs.bundle)).resolves.toBeDefined();
  });
});

test("cycles through a decision state compile within the transition budget", async () => {
  await withBundle(undefined, async (dirs) => {
    await writePipeline(dirs, DECISION_FROM_STATE_OUTPUT_YAML);
    const resolved = await loadPipelineV2(dirs.bundle);
    expect(resolved.states.map((state) => state.id)).toEqual([
      "coder",
      "check",
      "rework",
      "done",
      "failed_end",
    ]);
  });
});

// --- decision model loading ------------------------------------------------

test("model loading: valid, internal symlink and shared compiled model", async () => {
  await withBundle(undefined, async (dirs) => {
    await symlink(
      join(dirs.bundle, "decisions", "model.yaml"),
      join(dirs.bundle, "decisions", "link.yaml"),
    );
    const sharedTransitions = `      - outcome: alpha
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
    const decisionState = (id: string, model: string): string => `  - id: ${id}
    type: decision
    model: ${model}
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${sharedTransitions}`;
    const yaml = `
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
${decisionState("check", "decisions/model.yaml")}
${decisionState("check_link", "decisions/link.yaml")}
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
        to: check_link
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
    await writePipeline(dirs, yaml);
    const resolved = await loadPipelineV2(dirs.bundle);
    const first = resolved.states.find((state) => state.id === "check");
    const second = resolved.states.find((state) => state.id === "check_link");
    if (first === undefined || second === undefined) {
      throw new Error("expected both decision states");
    }
    if (first.type !== "decision" || second.type !== "decision") {
      throw new Error("expected decision states");
    }
    expect(second.modelPath).toBe(await realpath(join(dirs.bundle, "decisions", "model.yaml")));
    expect(second.model).toBe(first.model);
  });
});

test("model loading: escape, missing file, directory and wrong extension are rejected with state context", async () => {
  await withBundle(undefined, async (dirs) => {
    await writeFile(join(dirs.root, "outside.yaml"), SIMPLE_MODEL_YAML);
    await symlink(join(dirs.root, "outside.yaml"), join(dirs.bundle, "decisions", "escape.yaml"));

    const escape = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "model: decisions/model.yaml",
      "model: decisions/escape.yaml",
    );
    await writePipeline(dirs, escape);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" model .* resolves outside the pipeline bundle/,
    );

    const missing = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "model: decisions/model.yaml",
      "model: decisions/absent.yaml",
    );
    await writePipeline(dirs, missing);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" model .* is not accessible/,
    );

    const directory = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "model: decisions/model.yaml",
      "model: decisions",
    );
    await writePipeline(dirs, directory);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" model .* is not a regular file/,
    );

    const wrongExtension = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "model: decisions/model.yaml",
      "model: decisions/model.yml",
    );
    await writeFile(join(dirs.bundle, "decisions", "model.yml"), SIMPLE_MODEL_YAML);
    await writePipeline(dirs, wrongExtension);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check": decision model path must end with .yaml, got "decisions\/model.yml"/,
    );
  });
});

test("model path traversal, absolute paths and home expansion are rejected at parse", () => {
  const cases: [string, RegExp][] = [
    [
      "model: ../outside.yaml",
      /must be a clean bundle-relative path without empty, ".", "\.\." or "~" segments/,
    ],
    ["model: /etc/model.yaml", /must be a bundle-relative path/],
    ["model: ~/model.yaml", /must be a bundle-relative path/],
  ];
  for (const [modelField, message] of cases) {
    const yaml = DECISION_FROM_PIPELINE_INPUT_YAML.replace(
      "    model: decisions/model.yaml",
      `    ${modelField}`,
    );
    expect(() => parsePipelineV2Spec(yaml)).toThrow(message);
  }
});

test("a malformed decision model is wrapped as PipelineError with the state id", async () => {
  await withBundle("schema_version: 1\nfacts: nope\n", async (dirs) => {
    await writePipeline(dirs, DECISION_FROM_PIPELINE_INPUT_YAML);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check": decision model is missing required field "decisions"/,
    );
  });
});

// --- outcome contract -------------------------------------------------------

function decisionPipelineWithTransitions(transitions: string): string {
  return `
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
${transitions}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
}

const FULL_TRANSITIONS = `      - outcome: alpha
        to: done
      - outcome: beta
        to: done
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end`;

test("a missing selected-decision transition is rejected at load", async () => {
  await withBundle(undefined, async (dirs) => {
    const yaml = decisionPipelineWithTransitions(
      FULL_TRANSITIONS.replace("      - outcome: beta\n        to: done\n", ""),
    );
    await writePipeline(dirs, yaml);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" is missing a transition for decision "beta"/,
    );
  });
});

test("missing reserved-outcome transitions are rejected at load", async () => {
  for (const reserved of DECISION_RESERVED_OUTCOMES) {
    await withBundle(undefined, async (dirs) => {
      const yaml = decisionPipelineWithTransitions(
        FULL_TRANSITIONS.replace(`      - outcome: ${reserved}\n        to: failed_end`, ""),
      );
      await writePipeline(dirs, yaml);
      await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
        new RegExp(`decision state "check" is missing a transition for reserved outcome "${reserved}"`),
      );
    });
  }
});

test("an extra decision transition outcome is rejected at load", async () => {
  await withBundle(undefined, async (dirs) => {
    const yaml = decisionPipelineWithTransitions(
      `${FULL_TRANSITIONS}\n      - outcome: mystery\n        to: done`,
    );
    await writePipeline(dirs, yaml);
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" declares transition outcome "mystery" which is neither a decision of its model nor a reserved outcome \("uncovered", "inconsistent_facts", "invalid_facts"\)/,
    );
  });
});

test("a decision model may not declare a reserved decision id", async () => {
  const reservedModel = SIMPLE_MODEL_YAML.replace(
    "decisions:\n  - id: alpha\n",
    "decisions:\n  - id: alpha\n  - id: uncovered\n",
  );
  await withBundle(reservedModel, async (dirs) => {
    await writePipeline(dirs, decisionPipelineWithTransitions(FULL_TRANSITIONS));
    await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
      /decision state "check" model declares decision "uncovered", which is a reserved outcome; decision ids must not be "uncovered", "inconsistent_facts", "invalid_facts"/,
    );
  });
});

// --- pure evaluator adapter --------------------------------------------------

async function loadSimplePipeline(dirs: BundleDirs): Promise<ResolvedPipelineV2> {
  await writePipeline(dirs, DECISION_FROM_PIPELINE_INPUT_YAML);
  return loadPipelineV2(dirs.bundle);
}

test("the adapter maps selected, uncovered and inconsistent outcomes exactly", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);

    const selected = evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: false });
    expect(selected).toEqual({
      state_id: "check",
      status: "selected",
      outcome: "alpha",
      decision: "alpha",
      rule_id: "rule-a",
      active_constraint_ids: ["c1"],
    });
    expect(Object.keys(selected).sort()).toEqual(
      ["active_constraint_ids", "decision", "outcome", "rule_id", "state_id", "status"].sort(),
    );

    const uncovered = evaluatePipelineDecisionState(resolved, "check", { f1: false, f2: false });
    expect(uncovered).toEqual({
      state_id: "check",
      status: "uncovered",
      outcome: "uncovered",
      active_constraint_ids: [],
    });
    expect(Object.keys(uncovered).sort()).toEqual(
      ["active_constraint_ids", "outcome", "state_id", "status"].sort(),
    );

    const inconsistent = evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: true });
    expect(inconsistent).toEqual({
      state_id: "check",
      status: "inconsistent_facts",
      outcome: "inconsistent_facts",
      violated_relation_ids: ["r1"],
    });
    expect(Object.keys(inconsistent).sort()).toEqual(
      ["outcome", "state_id", "status", "violated_relation_ids"].sort(),
    );
  });
});

test("malformed facts map to invalid_facts with value-free reasons", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);

    const missing = evaluatePipelineDecisionState(resolved, "check", { f1: true });
    if (missing.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(missing.outcome).toBe("invalid_facts");
    expect(missing.reason).toBe('decision fact "f2" is missing');

    const nonBoolean = evaluatePipelineDecisionState(resolved, "check", {
      f1: true,
      f2: "CLASSIFIED_BODY",
    });
    if (nonBoolean.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(nonBoolean.reason).toBe('decision fact "f2" must be a boolean, got string');
    expect(nonBoolean.reason).not.toContain("CLASSIFIED_BODY");

    const extra = evaluatePipelineDecisionState(resolved, "check", {
      f1: true,
      f2: false,
      extra: "SECRET_VALUE",
    });
    if (extra.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(extra.reason).toBe('decision facts include unknown fact "extra"');
    expect(extra.reason).not.toContain("SECRET_VALUE");

    const nonMapping = evaluatePipelineDecisionState(resolved, "check", "TOP-SECRET-BODY");
    if (nonMapping.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(nonMapping.reason).toBe("decision facts must be a mapping of declared fact ids");
    expect(nonMapping.reason).not.toContain("TOP-SECRET-BODY");
    expect(Object.keys(nonMapping).sort()).toEqual(["outcome", "reason", "state_id", "status"].sort());

    const arrayFacts = evaluatePipelineDecisionState(resolved, "check", [true, false]);
    if (arrayFacts.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(arrayFacts.reason).toBe("decision facts must be a mapping of declared fact ids");

    const nullFacts = evaluatePipelineDecisionState(resolved, "check", null);
    if (nullFacts.status !== "invalid_facts") {
      throw new Error("expected invalid_facts");
    }
    expect(nullFacts.reason).toBe("decision facts must be a mapping of declared fact ids");
  });
});

test("results and diagnostics never contain fact values or bodies", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);
    const results: PipelineDecisionStateResult[] = [
      evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: "VALUE-MUST-NOT-LEAK" }),
      evaluatePipelineDecisionState(resolved, "check", "BODY-MUST-NOT-LEAK"),
      evaluatePipelineDecisionState(resolved, "check", { f1: "LEAK-ME-NOT", f2: false }),
    ];
    for (const result of results) {
      expect(JSON.stringify(result)).not.toContain("MUST-NOT-LEAK");
      expect(JSON.stringify(result)).not.toContain("LEAK-ME-NOT");
      expect(JSON.stringify(result)).not.toContain("BODY-MUST-NOT-LEAK");
      expect(JSON.stringify(result)).not.toContain("VALUE-MUST-NOT-LEAK");
    }
  });
});

test("results are deep-frozen", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);
    const selected = evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: false });
    const uncovered = evaluatePipelineDecisionState(resolved, "check", { f1: false, f2: false });
    const inconsistent = evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: true });
    const invalid = evaluatePipelineDecisionState(resolved, "check", { f1: true });
    for (const result of [selected, uncovered, inconsistent, invalid]) {
      expect(Object.isFrozen(result)).toBe(true);
    }
    if (selected.status !== "selected") {
      throw new Error("expected selected");
    }
    expect(Object.isFrozen(selected.active_constraint_ids)).toBe(true);
    if (uncovered.status !== "uncovered") {
      throw new Error("expected uncovered");
    }
    expect(Object.isFrozen(uncovered.active_constraint_ids)).toBe(true);
    if (inconsistent.status !== "inconsistent_facts") {
      throw new Error("expected inconsistent_facts");
    }
    expect(Object.isFrozen(inconsistent.violated_relation_ids)).toBe(true);
    expect(() => {
      (selected as { decision: string }).decision = "mutated";
    }).toThrow();
    expect(selected.decision).toBe("alpha");
    const uncoveredIds: readonly string[] = uncovered.active_constraint_ids;
    expect(() => {
      (uncoveredIds as string[]).push("x");
    }).toThrow();
    expect(uncoveredIds).toEqual([]);
  });
});

test("repeated evaluation is structurally identical but not the same object", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);
    const first = evaluatePipelineDecisionState(resolved, "check", { f1: false, f2: true });
    const second = evaluatePipelineDecisionState(resolved, "check", { f1: false, f2: true });
    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    if (first.status !== "selected" || second.status !== "selected") {
      throw new Error("expected selected");
    }
    expect(first.active_constraint_ids).not.toBe(second.active_constraint_ids);
  });
});

test("mutating the model file and the facts after load does not change results", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);
    const facts = { f1: true, f2: false };
    const before = evaluatePipelineDecisionState(resolved, "check", facts);
    if (before.status !== "selected") {
      throw new Error("expected selected");
    }
    expect(before.decision).toBe("alpha");

    await writeFile(
      join(dirs.bundle, "decisions", "model.yaml"),
      SIMPLE_MODEL_YAML.replace("decision: alpha", "decision: beta"),
    );
    const afterFileChange = evaluatePipelineDecisionState(resolved, "check", facts);
    expect(afterFileChange).toEqual(before);

    facts.f1 = false;
    expect(before.decision).toBe("alpha");
    const mutatedInput = evaluatePipelineDecisionState(resolved, "check", { f1: true, f2: false });
    expect(mutatedInput).toEqual(before);
  });
});

test("the adapter accepts only the trusted snapshot and only decision states", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);

    const forged = structuredClone(resolved) as unknown as ResolvedPipelineV2;
    expect(() => evaluatePipelineDecisionState(forged, "check", { f1: true, f2: false })).toThrow(
      /evaluatePipelineDecisionState requires the deep-frozen snapshot object returned by loadPipelineV2/,
    );
    expect(() =>
      evaluatePipelineDecisionState(
        { ...resolved, states: [...resolved.states] } as unknown as ResolvedPipelineV2,
        "check",
        { f1: true, f2: false },
      ),
    ).toThrow(/requires the deep-frozen snapshot object returned by loadPipelineV2/);

    expect(() => evaluatePipelineDecisionState(resolved, "absent", { f1: true, f2: false })).toThrow(
      /state "absent" is not declared by the pipeline/,
    );
    expect(() => evaluatePipelineDecisionState(resolved, "rework", { f1: true, f2: false })).toThrow(
      /state "rework" is not a decision state; the decision evaluator exists for decision states only/,
    );
    expect(() => evaluatePipelineDecisionState(resolved, "not a safe id!", { f1: true })).toThrow(
      PipelineError,
    );
  });
});

test("activation layouts stay agent-only for decision states", async () => {
  await withBundle(undefined, async (dirs) => {
    const resolved = await loadSimplePipeline(dirs);
    expect(() => planActivationLayout(resolved, "check")).toThrow(
      /state "check" is not an agent state; activation layouts exist for agent states only/,
    );
  });
});

test("arbitrary state and decision ids work with no legacy hardcode", async () => {
  const exoticModel = SIMPLE_MODEL_YAML.replace("  - id: alpha\n", "  - id: q1.r\n")
    .replace("  - id: beta\n", "  - id: beta-2_x\n")
    .replace("forbid: [beta]", "forbid: [beta-2_x]")
    .replace("decision: alpha", "decision: q1.r")
    .replace("decision: beta", "decision: beta-2_x");
  await withBundle(exoticModel, async (dirs) => {
    const yaml = `
schema_version: 2
entry_state: pipeline_state.z9-OK
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

states:
  - id: pipeline_state.z9-OK
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: q1.r
        to: done
      - outcome: beta-2_x
        to: done
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;
    await writePipeline(dirs, yaml);
    const resolved = await loadPipelineV2(dirs.bundle);
    const result = evaluatePipelineDecisionState(resolved, "pipeline_state.z9-OK", {
      f1: true,
      f2: false,
    });
    if (result.status !== "selected") {
      throw new Error("expected selected");
    }
    expect(result.decision).toBe("q1.r");
    expect(result.outcome).toBe("q1.r");
    expect(result.state_id).toBe("pipeline_state.z9-OK");
    expect(result.rule_id).toBe("rule-a");
  });
});

// --- oracle equivalence through the adapter ---------------------------------

interface OracleVectors {
  bit_order: string[];
  total_assignments: number;
  relation_rejected: number;
  consistent_vectors: number;
  selected: number;
  uncovered: number;
  rows: { id: string; input: string; expected: string | null; status: string }[];
}

const oracleVectors: OracleVectors = JSON.parse(
  await Bun.file(join(REPO_ROOT, "docs", "pipeline-oracle", "decision-vectors.json")).text(),
);

function flagsFromBits(bitOrder: readonly string[], bits: number): Record<string, boolean> {
  const flags: Record<string, boolean> = {};
  for (let index = 0; index < bitOrder.length; index++) {
    const name = bitOrder[index];
    if (name === undefined) {
      throw new Error(`missing bit_order entry at index ${index}`);
    }
    flags[name] = ((bits >> (bitOrder.length - 1 - index)) & 1) === 1;
  }
  return flags;
}

const ORACLE_DECISION_TRANSITIONS = [
  "close_stage",
  "close_stage_ignore_minor",
  "rework_same_stage",
  "rework_change_stage_contract",
  "rework_change_pipeline_plan",
  "architectural_proposal",
  "architectural_warning",
]
  .map((decision) => `      - outcome: ${decision}\n        to: done\n`)
  .join("");

test(
  "the default architect model stays oracle-equivalent through the adapter over all 2048 assignments",
  async () => {
    expect(oracleVectors.total_assignments).toBe(2048);
    expect(oracleVectors.relation_rejected).toBe(1952);
    expect(oracleVectors.selected).toBe(82);
    expect(oracleVectors.uncovered).toBe(14);

    const dirs = await makeBundle(
      await Bun.file(join(REPO_ROOT, "pipelines", "default", "decisions", "architect.yaml")).text(),
    );
    try {
      await writeFile(
        join(dirs.bundle, "pipeline.yaml"),
        decisionPipelineWithTransitions(
          `${ORACLE_DECISION_TRANSITIONS}      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end`,
        ),
      );
      const resolved = await loadPipelineV2(dirs.bundle);
      const model = parseDecisionModel(
        await Bun.file(join(dirs.bundle, "decisions", "model.yaml")).text(),
      );

      let inconsistent = 0;
      let selected = 0;
      let uncovered = 0;
      let invalid = 0;
      const counts = new Map<string, number>();

      for (let bits = 0; bits < 2 ** oracleVectors.bit_order.length; bits++) {
        const flags = flagsFromBits(oracleVectors.bit_order, bits);
        const adapter = evaluatePipelineDecisionState(resolved, "check", flags);
        const reference: DecisionOutcome = evaluateDecision(model, flags);

        if (adapter.status === "invalid_facts") {
          invalid++;
          continue;
        }
        expect(adapter.status).toBe(reference.status);
        if (reference.status === "selected" && adapter.status === "selected") {
          expect(adapter.decision).toBe(reference.decision);
          expect(adapter.rule_id).toBe(reference.rule_id);
          expect(adapter.outcome).toBe(adapter.decision);
          expect(adapter.active_constraint_ids).toEqual(reference.active_constraint_ids);
          selected++;
          counts.set(adapter.decision, (counts.get(adapter.decision) ?? 0) + 1);
        } else if (reference.status === "uncovered" && adapter.status === "uncovered") {
          expect(adapter.outcome).toBe("uncovered");
          expect(adapter.active_constraint_ids).toEqual(reference.active_constraint_ids);
          expect("decision" in adapter).toBe(false);
          uncovered++;
          counts.set("uncovered", (counts.get("uncovered") ?? 0) + 1);
        } else if (
          reference.status === "inconsistent_facts" &&
          adapter.status === "inconsistent_facts"
        ) {
          expect(adapter.outcome).toBe("inconsistent_facts");
          expect(adapter.violated_relation_ids).toEqual(reference.violated_relation_ids);
          inconsistent++;
        }
      }

      expect(invalid).toBe(0);
      expect(inconsistent).toBe(1952);
      expect(selected).toBe(82);
      expect(uncovered).toBe(14);
      expect(inconsistent + selected + uncovered).toBe(2048);
      expect(Object.fromEntries(counts)).toEqual({
        close_stage: 14,
        close_stage_ignore_minor: 7,
        rework_same_stage: 3,
        rework_change_stage_contract: 2,
        rework_change_pipeline_plan: 16,
        architectural_proposal: 32,
        architectural_warning: 8,
        uncovered: 14,
      });
    } finally {
      await rm(dirs.root, { recursive: true, force: true });
    }
  },
  { timeout: 30000 },
);
