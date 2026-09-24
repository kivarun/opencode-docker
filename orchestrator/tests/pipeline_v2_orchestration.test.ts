import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  compilePipelineV2Spec,
  loadPipelineV2,
  type PipelineV2OrchestrationSpec,
  type ResolvedPipelineV2,
  type ResolvedPipelineV2Orchestration,
} from "../src/pipeline_v2.ts";
import {
  PipelineV2OrchestrationError,
  compiledExecutionRoleFor,
  compiledStageTemplateFor,
} from "../src/pipeline_v2_orchestration.ts";
import * as orchestrationModule from "../src/pipeline_v2_orchestration.ts";
import { readFileSync } from "node:fs";

const DISPATCH_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: d_next_stage
  - id: d_plan_complete
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
  - id: r_done
    when:
      fact: f1
      equals: false
    decision: d_plan_complete
`;

const DISPATCH_MODEL_YAML_TWO_TEMPLATES = `schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: d_next_stage
  - id: d_test_stage
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
  - id: r_test
    when:
      all:
        - fact: f1
          equals: false
        - fact: f2
          equals: true
    decision: d_test_stage
`;

const GATE_MODEL_YAML = `schema_version: 1
facts:
  - id: g1
decisions:
  - id: d_rework
  - id: d_close_stage
relations: []
constraints: []
rules:
  - id: r_rework
    when:
      fact: g1
      equals: true
    decision: d_rework
  - id: r_close
    when:
      fact: g1
      equals: false
    decision: d_close_stage
`;

const FACTS_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const CANONICAL_HEADER = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs:
  - id: final_report
    required: true
    source:
      state_output:
        state: coder
        output: report

`;

const CANONICAL_ORCHESTRATION = `orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development

`;

const CANONICAL_STATES = `states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: plan
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: development_entry
      - outcome: d_plan_complete
        to: done
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: development_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: draft
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder

  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: report
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_review

  - id: stage_review
    type: agent
    profile: reviewer
    prompt: prompts/reviewer.md
    inputs:
      - id: report
        source:
          state_output:
            state: coder
            output: report
    outputs:
      - id: review
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: iteration_gate

  - id: iteration_gate
    type: decision
    model: decisions/gate.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_rework
        to: coder
      - outcome: d_close_stage
        to: stage_dispatch
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: done
    type: terminal
    result: success
  - id: failed
    type: terminal
    result: failed
`;

const CANONICAL_YAML = `${CANONICAL_HEADER}${CANONICAL_ORCHESTRATION}${CANONICAL_STATES}`;

/**
 * Two-template pipeline: the control dispatcher routes into the entry state
 * of each template; each template is an internally reachable subgraph whose
 * gate exits back to the control dispatcher.
 */
const TWO_TEMPLATES_HEADER = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

`;

const TWO_TEMPLATES_ORCHESTRATION = `orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
    - id: testing
      entry_state: testing_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: dev_agent
      role: stage
      stage_template: development
    - state_id: dev_gate
      role: stage
      stage_template: development
    - state_id: testing_entry
      role: stage
      stage_template: testing
    - state_id: test_agent
      role: stage
      stage_template: testing
    - state_id: test_gate
      role: stage
      stage_template: testing

`;

const TWO_TEMPLATES_STATES = `states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: development_entry
      - outcome: d_test_stage
        to: testing_entry
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: development_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_agent

  - id: dev_agent
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_gate

  - id: dev_gate
    type: decision
    model: decisions/gate.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_rework
        to: dev_agent
      - outcome: d_close_stage
        to: stage_dispatch
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: testing_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: test_agent

  - id: test_agent
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: test_gate

  - id: test_gate
    type: decision
    model: decisions/gate.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_rework
        to: test_agent
      - outcome: d_close_stage
        to: stage_dispatch
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: done
    type: terminal
    result: success
  - id: failed
    type: terminal
    result: failed
`;

const TWO_TEMPLATES_YAML = `${TWO_TEMPLATES_HEADER}${TWO_TEMPLATES_ORCHESTRATION}${TWO_TEMPLATES_STATES}`;

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(prefix: string): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  return { root, bundle };
}

async function writeOrchestratedBundle(
  dirs: BundleDirs,
  yaml: string = CANONICAL_YAML,
  dispatchModelYaml: string = DISPATCH_MODEL_YAML,
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "plan the work\n");
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "reviewer.md"), "review the work\n");
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(dirs.bundle, "decisions", "dispatch.yaml"), dispatchModelYaml);
  await writeFile(join(dirs.bundle, "decisions", "gate.yaml"), GATE_MODEL_YAML);
}

async function withOrchestratedBundle(
  fn: (dirs: BundleDirs) => Promise<void>,
  yaml: string = CANONICAL_YAML,
  dispatchModelYaml: string = DISPATCH_MODEL_YAML,
): Promise<void> {
  const dirs = await makeBundleDirs("pipeline-v2-orch-");
  try {
    await writeOrchestratedBundle(dirs, yaml, dispatchModelYaml);
    await fn(dirs);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

/** Compose the canonical bundle with a replaced orchestration section. */
function withOrchestrationSection(orchestrationYaml: string, base = CANONICAL_YAML): string {
  const marker = "orchestration:\n";
  const start = base.indexOf(marker);
  const end = base.indexOf("\nstates:\n");
  if (start < 0 || end < 0 || end <= start) {
    throw new Error("the base pipeline.yaml does not carry an orchestration section");
  }
  return `${base.slice(0, start)}${orchestrationYaml}${base.slice(end + 1)}`;
}

function rejectCompile(yaml: string, message: RegExp | string): void {
  expect(() => compilePipelineV2Spec(Bun.YAML.parse(yaml))).toThrow(message);
}

function acceptCompile(yaml: string): void {
  expect(() => compilePipelineV2Spec(Bun.YAML.parse(yaml))).not.toThrow();
}

const NORMALIZED_ORCHESTRATION: ResolvedPipelineV2Orchestration = {
  stage_templates: [{ id: "development", entry_state: "development_entry" }],
  execution_roles: [
    { state_id: "architect", role: "planning" },
    { state_id: "coder", role: "stage", stage_template: "development" },
    { state_id: "development_entry", role: "stage", stage_template: "development" },
    { state_id: "iteration_gate", role: "stage", stage_template: "development" },
    { state_id: "stage_dispatch", role: "control" },
    { state_id: "stage_review", role: "stage", stage_template: "development" },
  ],
};

test("1. a pipeline without orchestration resolves with no orchestration field at all", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      expect(pipeline.orchestration).toBeUndefined();
      expect(Object.keys(pipeline)).not.toContain("orchestration");
      const raw = await readFile(join(dirs.bundle, "pipeline.yaml"), "utf8");
      const spec = compilePipelineV2Spec(Bun.YAML.parse(raw));
      expect(spec.orchestration).toBeUndefined();
    },
    `${CANONICAL_HEADER}${CANONICAL_STATES}`,
  );
});

test("2. the full orchestrated happy path compiles with the exact normalized metadata", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const orchestration = pipeline.orchestration;
    if (orchestration === undefined) {
      throw new Error("the orchestrated pipeline lost its orchestration metadata");
    }
    expect(orchestration).toEqual(NORMALIZED_ORCHESTRATION);
    // the resolved metadata is deeply frozen
    expect(Object.isFrozen(orchestration)).toBe(true);
    expect(Object.isFrozen(orchestration.stage_templates)).toBe(true);
    expect(Object.isFrozen(orchestration.stage_templates[0])).toBe(true);
    expect(Object.isFrozen(orchestration.execution_roles)).toBe(true);
    for (const entry of orchestration.execution_roles) {
      expect(Object.isFrozen(entry)).toBe(true);
    }
    const roleOf = new Map(orchestration.execution_roles.map((entry) => [entry.state_id, entry.role]));
    expect(roleOf.get("architect")).toBe("planning");
    expect(roleOf.get("stage_dispatch")).toBe("control");
    expect(roleOf.get("coder")).toBe("stage");
    expect(roleOf.get("iteration_gate")).toBe("stage");
    expect(roleOf.has("done")).toBe(false);
    expect(roleOf.has("failed")).toBe(false);
    // the template's stage states are exactly its members, including the
    // stage decision and both stage agents; the exit transitions
    // (stage -> control dispatcher, dispatcher -> terminal) compile
    expect(compiledStageTemplateFor(pipeline, "development")).toEqual({
      id: "development",
      entry_state: "development_entry",
      state_ids: ["coder", "development_entry", "iteration_gate", "stage_review"],
    });
  });
});

test("3. several templates compile with per-template membership", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const orchestration = pipeline.orchestration;
      if (orchestration === undefined) {
        throw new Error("the orchestrated pipeline lost its orchestration metadata");
      }
      expect(orchestration.stage_templates).toEqual([
        { id: "development", entry_state: "development_entry" },
        { id: "testing", entry_state: "testing_entry" },
      ]);
      const membersOf = (templateId: string): string[] =>
        orchestration.execution_roles
          .filter((entry) => entry.role === "stage" && entry.stage_template === templateId)
          .map((entry) => entry.state_id);
      expect(membersOf("development")).toEqual(["dev_agent", "dev_gate", "development_entry"]);
      expect(membersOf("testing")).toEqual(["test_agent", "test_gate", "testing_entry"]);
    },
    TWO_TEMPLATES_YAML,
    DISPATCH_MODEL_YAML_TWO_TEMPLATES,
  );
});

test("4. declaration-order permutation is not semantic: identical resolved metadata", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const first = await loadPipelineV2(dirs.bundle);
    const firstOrchestration = first.orchestration;
    if (firstOrchestration === undefined) {
      throw new Error("missing orchestration");
    }
    const permuted = withOrchestrationSection(`orchestration:
  execution_roles:
    - state_id: iteration_gate
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: architect
      role: planning
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
  stage_templates:
    - id: development
      entry_state: development_entry
`);
    await withOrchestratedBundle(
      async (dirs) => {
        const second = await loadPipelineV2(dirs.bundle);
        expect(second.orchestration).toEqual(firstOrchestration);
      },
      permuted,
    );
  });
});

test("5. the resolved orchestration is deep-frozen and the parsed input is not mutated", () => {
  const parsed = Bun.YAML.parse(CANONICAL_YAML) as Record<string, unknown>;
  const inputOrchestration = parsed.orchestration as Record<string, unknown>;
  expect(Object.isFrozen(inputOrchestration)).toBe(false);
  expect(Object.isFrozen(inputOrchestration.stage_templates)).toBe(false);
  expect(Object.isFrozen(inputOrchestration.execution_roles)).toBe(false);
  const before = JSON.parse(JSON.stringify(inputOrchestration));
  const spec = compilePipelineV2Spec(parsed);
  const orchestration = spec.orchestration;
  if (orchestration === undefined) {
    throw new Error("missing orchestration");
  }
  // the compiled spec normalizes (sorts) the roles; the parsed input keeps
  // its declaration order and was neither mutated nor frozen
  expect(orchestration).toEqual(NORMALIZED_ORCHESTRATION as PipelineV2OrchestrationSpec);
  expect(inputOrchestration).toEqual(before);
  expect(Object.isFrozen(inputOrchestration)).toBe(false);
  expect(Object.isFrozen(inputOrchestration.stage_templates)).toBe(false);
  expect(Object.isFrozen(inputOrchestration.execution_roles)).toBe(false);
});

test("6. exact-field rejection at every new level", () => {
  const roleBlock = `orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
`;
  // unknown field on the orchestration object itself
  rejectCompile(
    CANONICAL_YAML.replace(
      CANONICAL_ORCHESTRATION,
      `${roleBlock}${CANONICAL_ORCHESTRATION.slice(roleBlock.length)}  extra: 1\n`,
    ),
    /pipeline orchestration has unknown field "extra"/,
  );
  // unknown field on a stage template entry
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
      template: development
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration stage_templates 0 has unknown field "template"/,
  );
  // missing required field on a stage template entry
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration stage_templates 0 is missing required field "entry_state"/,
  );
  // planning role must not carry stage_template (rule 12)
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
      stage_template: development
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration execution_roles 0 has unknown field "stage_template"/,
  );
  // stage role without stage_template (rule 11)
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration execution_roles 2 is missing required field "stage_template"/,
  );
  // orchestration is not a mapping
  rejectCompile(
    CANONICAL_YAML.replace(CANONICAL_ORCHESTRATION, "orchestration: 7\n"),
    /pipeline orchestration is not a YAML mapping/,
  );
  // stage_templates is not a list
  rejectCompile(
    CANONICAL_YAML.replace(CANONICAL_ORCHESTRATION, "orchestration:\n  stage_templates: {}\n  execution_roles:\n    - state_id: architect\n      role: planning\n"),
    /pipeline orchestration stage_templates must be a list/,
  );
  // an invalid role value
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: worker
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /execution_roles 0 role must be one of \["planning","control","stage"\]/,
  );
  // an unsafe state id
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: bad id!
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /execution_roles 0 state_id "bad id!" is not a safe identifier/,
  );
});

test("7. empty stage_templates with planning/control-only roles compile; empty roles fail coverage", () => {
  acceptCompile(`${CANONICAL_HEADER}${CANONICAL_STATES}`
    .replace("states:", `orchestration:
  stage_templates: []
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: planning
    - state_id: coder
      role: planning
    - state_id: stage_review
      role: planning
    - state_id: iteration_gate
      role: control
states:`));
  acceptCompile(
    withOrchestrationSection(`orchestration:
  stage_templates: []
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: planning
    - state_id: coder
      role: planning
    - state_id: stage_review
      role: planning
    - state_id: iteration_gate
      role: control
`),
  );
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates: []
  execution_roles: []
`),
    /pipeline orchestration does not declare an execution role for agent state "architect"/,
  );
});

test("8. duplicate template id is rejected", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
    - id: development
      entry_state: coder
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares stage template "development" more than once/,
  );
});

test("9. duplicate template entry state is rejected", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
    - id: development2
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares stage template entry_state "development_entry" more than once/,
  );
});

test("10. duplicate and missing execution roles are rejected", () => {
  // duplicate state entry
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares an execution role for state "coder" more than once/,
  );
  // a missing agent state
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration does not declare an execution role for agent state "stage_review"/,
  );
});

test("11. unknown and terminal states carry no execution role", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: ghost
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares an execution role for unknown state "ghost"/,
  );
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
    - state_id: done
      role: control
`),
    /pipeline orchestration declares an execution role for terminal state "done"/,
  );
});

test("12. the planning role is an agent-state role only", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: planning
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares role "planning" for decision state "stage_dispatch"; planning is an agent-state role/,
  );
});

test("13. the control role is a decision-state role only", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: control
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares role "control" for agent state "coder"; control is a decision-state role/,
  );
});

test("14. the stage role requires a stage_template", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /execution_roles 3 is missing required field "stage_template"/,
  );
});

test("15. planning and control roles must not carry stage_template", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
      stage_template: development
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /execution_roles 1 has unknown field "stage_template"/,
  );
});

test("16. a stage role must reference a declared template", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: ghost_template
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /pipeline orchestration declares stage role for state "coder" with unknown stage template "ghost_template"/,
  );
});

test("17. a template entry_state must name a declared state", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: ghost_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /stage template "development" entry_state "ghost_entry" does not name a declared state/,
  );
});

test("18. a template entry_state must carry the stage role of that template", () => {
  rejectCompile(
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: stage_dispatch
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: stage_review
      role: stage
      stage_template: development
    - state_id: iteration_gate
      role: stage
      stage_template: development
`),
    /stage template "development" entry_state "stage_dispatch" must carry the stage role, got "control"/,
  );
});

test("19. a template entry_state must belong to its own template", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
        /stage template "development" entry_state "testing_entry" belongs to stage template "testing"/,
      );
    },
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: testing_entry
    - id: testing
      entry_state: test_agent
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: dev_agent
      role: stage
      stage_template: development
    - state_id: dev_gate
      role: stage
      stage_template: development
    - state_id: testing_entry
      role: stage
      stage_template: testing
    - state_id: test_agent
      role: stage
      stage_template: testing
    - state_id: test_gate
      role: stage
      stage_template: testing
`, TWO_TEMPLATES_YAML),
    DISPATCH_MODEL_YAML_TWO_TEMPLATES,
  );
});

test("20. a template without stage states is rejected", () => {
  const minimalStates = `states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: stage_dispatch

  - id: stage_dispatch
    type: decision
    model: decisions/dispatch.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: d_next_stage
        to: done
      - outcome: d_plan_complete
        to: done
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: done
    type: terminal
    result: success
  - id: failed
    type: terminal
    result: failed
`;
  rejectCompile(
    `${CANONICAL_HEADER}orchestration:
  stage_templates:
    - id: development
      entry_state: stage_dispatch
    - id: testing
      entry_state: testing_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
${minimalStates}`,
    /stage template "development" declares no stage states/,
  );
});

test("21. an unreachable stage state is rejected (general graph and template topology)", () => {
  // (a) the stage state is unreachable from the pipeline entry: the shared
  // graph check rejects it first
  rejectCompile(
    CANONICAL_YAML.replace(
      `    transitions:
      - outcome: completed
        to: stage_review

  - id: stage_review`,
      `    transitions:
      - outcome: completed
        to: iteration_gate

  - id: stage_review`,
    ),
    /agent state "stage_review" is not reachable from entry_state/,
  );
  // (b) the pipeline entry sits inside the template, so every stage state is
  // graph-reachable; the template topology check catches the stage state
  // that the template entry cannot reach along internal transitions
  rejectCompile(
    `schema_version: 2
entry_state: orphan
max_transitions: 40

inputs: []

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: template_entry
  execution_roles:
    - state_id: orphan
      role: stage
      stage_template: development
    - state_id: coder
      role: stage
      stage_template: development
    - state_id: template_entry
      role: stage
      stage_template: development

states:
  - id: orphan
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder
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
        to: template_entry
  - id: template_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder
  - id: done
    type: terminal
    result: success
  - id: failed
    type: terminal
    result: failed
`,
    /stage state "orphan" is not reachable from the entry state "template_entry" of stage template "development"/,
  );
});

test("22. a transition between stage states of different templates is rejected", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      await expect(loadPipelineV2(dirs.bundle)).rejects.toThrow(
        /transition of stage state "dev_gate" \(stage template "development"\) targets stage state "testing_entry" of foreign stage template "testing"/,
      );
    },
    withOrchestrationSection(`orchestration:
  stage_templates:
    - id: development
      entry_state: development_entry
    - id: testing
      entry_state: testing_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: stage
      stage_template: development
    - state_id: dev_agent
      role: stage
      stage_template: development
    - state_id: dev_gate
      role: stage
      stage_template: development
    - state_id: testing_entry
      role: stage
      stage_template: testing
    - state_id: test_agent
      role: stage
      stage_template: testing
    - state_id: test_gate
      role: stage
      stage_template: testing
`, TWO_TEMPLATES_YAML).replace(
      `      - outcome: d_close_stage
        to: stage_dispatch
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: testing_entry`,
      `      - outcome: d_close_stage
        to: testing_entry
      - outcome: uncovered
        to: failed
      - outcome: inconsistent_facts
        to: failed
      - outcome: invalid_facts
        to: failed

  - id: testing_entry`,
    ),
    DISPATCH_MODEL_YAML_TWO_TEMPLATES,
  );
});

test("23. a transition from outside a template into a non-entry stage state is rejected", () => {
  rejectCompile(
    CANONICAL_YAML.replace(
      `      - outcome: d_rework
        to: coder
      - outcome: d_close_stage
        to: stage_dispatch`,
      `      - outcome: d_rework
        to: development_entry
      - outcome: d_close_stage
        to: stage_dispatch`,
    ).replace(
      `      - outcome: d_next_stage
        to: development_entry
      - outcome: d_plan_complete`,
      `      - outcome: d_next_stage
        to: coder
      - outcome: d_plan_complete`,
    ),
    /transition of non-stage state "stage_dispatch" targets stage state "coder", which is not the entry state of stage template "development"/,
  );
});

test("24. exits from a template to planning/control/terminal states are allowed", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const stageDispatch = pipeline.states.find((state) => state.id === "stage_dispatch");
    if (stageDispatch === undefined || stageDispatch.type !== "decision") {
      throw new Error("missing the control dispatcher");
    }
    // iteration_gate (stage) -> stage_dispatch (control): allowed exit
    // stage_dispatch (control) -> done (terminal): allowed exit
    expect(stageDispatch.transitions.map((transition) => transition.to)).toContain("done");
    const iterationGate = pipeline.states.find((state) => state.id === "iteration_gate");
    if (iterationGate === undefined || iterationGate.type !== "decision") {
      throw new Error("missing the stage gate");
    }
    expect(iterationGate.transitions.map((transition) => transition.to)).toContain("stage_dispatch");
  });
});

test("25. resolver results are exact, deep-frozen and deterministic", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const planning = compiledExecutionRoleFor(pipeline, "architect");
    expect(planning).toEqual({ state_id: "architect", role: "planning" });
    expect(Object.keys(planning)).toEqual(["state_id", "role"]);
    expect(Object.isFrozen(planning)).toBe(true);
    const control = compiledExecutionRoleFor(pipeline, "stage_dispatch");
    expect(control).toEqual({ state_id: "stage_dispatch", role: "control" });
    expect(Object.keys(control)).toEqual(["state_id", "role"]);
    const stage = compiledExecutionRoleFor(pipeline, "coder");
    expect(stage).toEqual({ state_id: "coder", role: "stage", stage_template: "development" });
    expect(Object.keys(stage)).toEqual(["state_id", "role", "stage_template"]);
    expect(Object.isFrozen(stage)).toBe(true);
    const stageDecision = compiledExecutionRoleFor(pipeline, "iteration_gate");
    expect(stageDecision).toEqual({
      state_id: "iteration_gate",
      role: "stage",
      stage_template: "development",
    });
    const template = compiledStageTemplateFor(pipeline, "development");
    expect(template).toEqual({
      id: "development",
      entry_state: "development_entry",
      state_ids: ["coder", "development_entry", "iteration_gate", "stage_review"],
    });
    expect(Object.isFrozen(template)).toBe(true);
    expect(Object.isFrozen(template.state_ids)).toBe(true);
    // repeated calls are structurally identical, freshly built objects
    expect(compiledExecutionRoleFor(pipeline, "coder")).not.toBe(stage);
    expect(compiledExecutionRoleFor(pipeline, "coder")).toEqual(stage);
    expect(compiledStageTemplateFor(pipeline, "development")).not.toBe(template);
    expect(compiledStageTemplateFor(pipeline, "development")).toEqual(template);
  });
});

test("26. missing metadata, unknown state and unknown template are typed errors", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      expect(() => compiledExecutionRoleFor(pipeline, "architect")).toThrow(
        PipelineV2OrchestrationError,
      );
      expect(() => compiledExecutionRoleFor(pipeline, "architect")).toThrow(
        /the trusted pipeline declares no orchestration section/,
      );
      expect(() => compiledStageTemplateFor(pipeline, "development")).toThrow(
        /the trusted pipeline declares no orchestration section/,
      );
    },
    `${CANONICAL_HEADER}${CANONICAL_STATES}`,
  );
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    let message = "";
    try {
      compiledExecutionRoleFor(pipeline, "ghost");
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2OrchestrationError);
      message = (cause as Error).message;
    }
    expect(message).toContain('state "ghost" is not declared by the pipeline');
    try {
      compiledExecutionRoleFor(pipeline, "done");
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2OrchestrationError);
      message = (cause as Error).message;
    }
    expect(message).toContain('state "done" is a terminal state; terminal states carry no execution role');
    try {
      compiledStageTemplateFor(pipeline, "ghost_template");
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2OrchestrationError);
      message = (cause as Error).message;
    }
    expect(message).toContain('stage template "ghost_template" is not declared by the pipeline orchestration');
    expect(() => compiledExecutionRoleFor(pipeline, "bad id!")).toThrow(
      /compiledExecutionRoleFor requires a safe state id/,
    );
    expect(() => compiledStageTemplateFor(pipeline, "bad id!")).toThrow(
      /compiledStageTemplateFor requires a safe template id/,
    );
  });
});

test("27. the provenance gate runs before the id check, getter reads and Proxy traps", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const resolved = await loadPipelineV2(dirs.bundle);
    const UNTRUSTED =
      "compiledExecutionRoleFor requires the deep-frozen snapshot object returned by loadPipelineV2; " +
      "hand-built objects, casts, clones and Proxies are rejected before any content is read";
    const UNTRUSTED_TEMPLATE =
      "compiledStageTemplateFor requires the deep-frozen snapshot object returned by loadPipelineV2; " +
      "hand-built objects, casts, clones and Proxies are rejected before any content is read";

    // a forged pipeline with an invalid id: the provenance gate fires first
    expect(() => compiledExecutionRoleFor({} as ResolvedPipelineV2, "bad id!")).toThrow(UNTRUSTED);
    expect(() => compiledStageTemplateFor({} as ResolvedPipelineV2, "bad id!")).toThrow(
      UNTRUSTED_TEMPLATE,
    );

    // deep clone with fresh identities
    const clone = structuredClone(resolved);
    expect(clone).toEqual(resolved);
    expect(() => compiledExecutionRoleFor(clone, "architect")).toThrow(UNTRUSTED);
    expect(() => compiledStageTemplateFor(clone, "development")).toThrow(UNTRUSTED_TEMPLATE);

    // shallow spread
    const shallow = { ...resolved };
    expect(() => compiledExecutionRoleFor(shallow as ResolvedPipelineV2, "architect")).toThrow(
      UNTRUSTED,
    );

    // prototype-derived object wrapping the real data
    const derived = Object.create(resolved);
    expect(() => compiledExecutionRoleFor(derived as ResolvedPipelineV2, "architect")).toThrow(
      UNTRUSTED,
    );

    // a Proxy forwarding to the exact same snapshot is a different identity
    let trapCalls = 0;
    const proxied = new Proxy(resolved, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() => compiledExecutionRoleFor(proxied, "architect")).toThrow(UNTRUSTED);
    expect(() => compiledStageTemplateFor(proxied, "development")).toThrow(UNTRUSTED_TEMPLATE);
    expect(trapCalls).toBe(0);

    // getters in a forged object are never invoked
    let getterCalls = 0;
    const getterForged = {
      get schema_version() {
        getterCalls += 1;
        return 2;
      },
      get orchestration() {
        getterCalls += 1;
        return undefined;
      },
      get states() {
        getterCalls += 1;
        return [];
      },
    };
    expect(() =>
      compiledExecutionRoleFor(getterForged as unknown as ResolvedPipelineV2, "architect"),
    ).toThrow(UNTRUSTED);
    expect(getterCalls).toBe(0);

    // the original trusted snapshot still resolves
    expect(compiledExecutionRoleFor(resolved, "architect")).toEqual({
      state_id: "architect",
      role: "planning",
    });
  });
});


test("28. mutation isolation: results and trusted snapshot are untouched by resolution", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const before = JSON.parse(JSON.stringify(pipeline));
    const first = compiledExecutionRoleFor(pipeline, "coder");
    const second = compiledStageTemplateFor(pipeline, "development");
    expect(JSON.parse(JSON.stringify(pipeline))).toEqual(before);
    // the results are frozen: writes are refused
    expect(() => {
      (first as { role: string }).role = "control";
    }).toThrow();
    expect(() => {
      (second.state_ids as string[]).push("injected");
    }).toThrow();
    expect(first).toEqual({ state_id: "coder", role: "stage", stage_template: "development" });
    expect(second.state_ids).toEqual(["coder", "development_entry", "iteration_gate", "stage_review"]);
    // a mutated pipeline copy is still rejected by provenance
    const mutated = JSON.parse(JSON.stringify(pipeline)) as ResolvedPipelineV2;
    expect(() => compiledExecutionRoleFor(mutated, "coder")).toThrow(
      /compiledExecutionRoleFor requires the deep-frozen snapshot object/,
    );
  });
});

test("29. the resolver export surface is exactly the three runtime keys", () => {
  expect(Object.keys(orchestrationModule).sort()).toEqual([
    "PipelineV2OrchestrationError",
    "compiledExecutionRoleFor",
    "compiledStageTemplateFor",
  ]);
});

test("30. source scan: the resolver imports only the pipeline and scalar modules", () => {
  const source = readFileSync(
    join(import.meta.dir, "..", "src", "pipeline_v2_orchestration.ts"),
    "utf8",
  );
  // every import statement targets only the trusted pipeline module or the
  // neutral scalar predicates; no state/reducer/coordinator/runner/run-plan
  // module, no second graph compiler, serializer or digest builder
  const importTargets = [...source.matchAll(/from "\.\/([^"]+)"/g)].map((match) => match[1] ?? "");
  expect(importTargets.length).toBeGreaterThan(0);
  for (const target of importTargets) {
    expect(["pipeline_v2.ts", "pipeline_v2_scalar.ts"]).toContain(target);
  }
  const forbiddenModules = [
    "pipeline_v2_state",
    "pipeline_state",
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "pipeline_v2_run_plan",
    "pipeline_v2_wait",
    "pipeline_v2_digest",
    "pipeline_v2_runtime",
    "pipeline_v2_resume",
    "pipeline_v2_docker",
    "pipeline_v2_schema",
    "pipeline_v2_project",
    "agent_smoke",
    "docker_helper",
    "launcher",
    "profile",
    "run_snapshot_store",
    "bundle_file",
    "decision",
  ];
  for (const forbidden of forbiddenModules) {
    expect(source.includes(`from "./${forbidden}.ts"`)).toBe(false);
  }
  // no second graph compiler, serializer or digest builder
  expect(source).not.toContain("checkGraphShape");
  expect(source).not.toContain("canonicalJson");
  expect(source).not.toContain("CryptoHasher");
  expect(source).not.toContain("Bun.YAML");
  expect(source).not.toContain("requireBundleFileInsideRoot");
  expect(source).not.toContain("readBundleFile");
  // the resolver validates no pipeline content of its own: no second
  // validation pass and no registry beyond the loader's provenance gate
  expect(source).not.toContain("WeakSet");
  expect(source).not.toContain("WeakMap");
});
