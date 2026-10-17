import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  compilePipelineV2Spec,
  loadPipelineV2,
  type PipelineV2OrchestrationSpec,
  type PipelineV2StageWaitActionName,
  type ResolvedPipelineV2,
  type ResolvedPipelineV2Orchestration,
} from "../src/pipeline_v2.ts";
import { pipelineV2ExecutionDigest, pipelineV2ExecutionSnapshot } from "../src/pipeline_v2_digest.ts";
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

const PLAN_OUTPUT_SCHEMA = { type: "object" };

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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
        type: json
        schema: schemas/plan.schema.json
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
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
  await writeFile(join(dirs.bundle, "schemas", "plan.schema.json"), JSON.stringify(PLAN_OUTPUT_SCHEMA));
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
    {
      state_id: "architect",
      role: "planning",
      plan_output: "plan",
      stage_wait: {
        reason: "stage_iteration_completed",
        actions: ["continue_stage", "revise_task"],
      },
    },
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
  // every planning state must itself declare the JSON output its plan_output
  // names, so the planning-only variants extend the canonical agents
  const planningOnlyStates = `${CANONICAL_STATES}`
    .replace("outputs:\n      - id: draft\n        type: file", "outputs:\n      - id: plan\n        type: json\n        schema: schemas/plan.schema.json\n      - id: draft\n        type: file")
    .replace("outputs:\n      - id: report\n        type: file", "outputs:\n      - id: plan\n        type: json\n        schema: schemas/plan.schema.json\n      - id: report\n        type: file")
    .replace("outputs:\n      - id: review\n        type: file", "outputs:\n      - id: plan\n        type: json\n        schema: schemas/plan.schema.json\n      - id: review\n        type: file");
  acceptCompile(`${CANONICAL_HEADER}${planningOnlyStates}`
    .replace("states:", `orchestration:
  stage_templates: []
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: coder
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_review
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: iteration_gate
      role: control
states:`));
  acceptCompile(
    `${CANONICAL_HEADER}orchestration:
  stage_templates: []
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_dispatch
      role: control
    - state_id: development_entry
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: coder
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_review
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: iteration_gate
      role: control
${planningOnlyStates}`,
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_dispatch
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
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
    expect(planning).toEqual({
      state_id: "architect",
      role: "planning",
      plan_output: "plan",
      stage_wait: { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] },
    });
    expect(Object.keys(planning)).toEqual(["state_id", "role", "plan_output", "stage_wait"]);
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
      plan_output: "plan",
      stage_wait: { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] },
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

test("31. the planning plan_output binding matrix: only the exact JSON output of the planning state compiles", async () => {
  const base = `${CANONICAL_HEADER}${CANONICAL_ORCHESTRATION}${CANONICAL_STATES}`;
  // the canonical bundle (planning plan_output -> the architect's own json
  // output) is the single success
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    expect(pipeline.orchestration?.execution_roles[0]).toEqual({
      state_id: "architect",
      role: "planning",
      plan_output: "plan",
      stage_wait: { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] },
    });
  });
  // unknown output id on the planning state
  await withOrchestratedBundle(
    async () => {},
    base.replace("      plan_output: plan\n", "      plan_output: missing\n"),
  ).catch((error: unknown) => {
    expect((error as Error).message).toBe(
      'pipeline orchestration declares planning role for state "architect" with unknown output "missing"',
    );
  });
  // an output that exists only on another state
  await withOrchestratedBundle(
    async () => {},
    base.replace("      plan_output: plan\n", "      plan_output: report\n"),
  ).catch((error: unknown) => {
    expect((error as Error).message).toBe(
      'pipeline orchestration declares planning role for state "architect" with unknown output "report"',
    );
  });
  // a file output with the same id
  await withOrchestratedBundle(
    async () => {},
    CANONICAL_YAML.replace("      - id: plan\n        type: json\n        schema: schemas/plan.schema.json", "      - id: plan\n        type: file"),
  ).catch((error: unknown) => {
    expect((error as Error).message).toBe(
      'pipeline orchestration declares planning role for state "architect" whose output "plan" has type "file"; the plan output must have type "json"',
    );
  });
  // the exact json output of the planning state is the only success
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const architect = pipeline.states.find((state) => state.id === "architect");
    if (architect === undefined || architect.type !== "agent") {
      throw new Error("the architect state is missing");
    }
    expect(architect.outputs.map((port) => [port.id, port.type])).toEqual([["plan", "json"]]);
  });
});

test("32. exact planning plan_output shape; no inference and no fallback", () => {
  const base = `${CANONICAL_HEADER}${CANONICAL_ORCHESTRATION}${CANONICAL_STATES}`;
  // planning without plan_output is rejected even when the state declares
  // exactly one json output
  const without = base.replace("      plan_output: plan\n", "");
  let caught: unknown = null;
  try {
    compilePipelineV2Spec(Bun.YAML.parse(without));
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(PipelineError);
  expect((caught as Error).message).toBe(
    'pipeline orchestration execution_roles 0 is missing required field "plan_output"',
  );
  // control/stage entries must not carry plan_output
  rejectCompile(
    base.replace(
      "    - state_id: stage_dispatch\n      role: control\n",
      "    - state_id: stage_dispatch\n      role: control\n      plan_output: plan\n",
    ),
    /pipeline orchestration execution_roles 1 has unknown field "plan_output"/,
  );
  rejectCompile(
    base.replace(
      "    - state_id: coder\n      role: stage\n      stage_template: development\n",
      "    - state_id: coder\n      role: stage\n      stage_template: development\n      plan_output: plan\n",
    ),
    /pipeline orchestration execution_roles 3 has unknown field "plan_output"/,
  );
  // missing / value-less / non-string / unsafe plan_output
  rejectCompile(
    base.replace("      plan_output: plan\n", "      plan_output:\n"),
    /pipeline orchestration execution_roles 0 plan_output/,
  );
  rejectCompile(
    base.replace("      plan_output: plan\n", "      plan_output: 7\n"),
    /pipeline orchestration execution_roles 0 plan_output/,
  );
  rejectCompile(
    base.replace("      plan_output: plan\n", "      plan_output: bad id!\n"),
    /pipeline orchestration execution_roles 0 plan_output/,
  );
});

test("33. the plan_output metadata value moves the existing pipeline digest; new hash machinery none", async () => {
  // both bundles declare the identical two JSON output ports of the
  // planning state; the only semantic difference is the plan_output value
  const twoPorts = CANONICAL_STATES.replace(
    "    outputs:\n      - id: plan\n        type: json\n        schema: schemas/plan.schema.json",
    "    outputs:\n      - id: plan\n        type: json\n        schema: schemas/plan.schema.json\n      - id: alt\n        type: json\n        schema: schemas/plan.schema.json",
  );
  const yamlA = `${CANONICAL_HEADER}${CANONICAL_ORCHESTRATION}${twoPorts}`;
  const yamlB = `${CANONICAL_HEADER}${CANONICAL_ORCHESTRATION.replace("      plan_output: plan\n", "      plan_output: alt\n")}${twoPorts}`;

  let digestA = "";
  let digestB = "";
  const snapshots: { a: Record<string, unknown>; b: Record<string, unknown> } = { a: {}, b: {} };
  const captureSnapshot = (slot: "a" | "b", digestBox: { value: string }) =>
    async (dirs: BundleDirs): Promise<void> => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      digestBox.value = pipelineV2ExecutionDigest(pipeline);
      snapshots[slot] = JSON.parse(JSON.stringify(pipelineV2ExecutionSnapshot(pipeline))) as Record<string, unknown>;
    };
  const digestABox = { value: "" };
  const digestBBox = { value: "" };
  await withOrchestratedBundle(captureSnapshot("a", digestABox), yamlA);
  await withOrchestratedBundle(captureSnapshot("b", digestBBox), yamlB);
  digestA = digestABox.value;
  digestB = digestBBox.value;

  // both bundles load; the digest moves only because of the plan_output
  expect(digestA).toMatch(/^[0-9a-f]{64}$/);
  expect(digestB).toMatch(/^[0-9a-f]{64}$/);
  expect(digestA).not.toBe(digestB);

  // the states and output declarations are structurally equal
  expect(JSON.stringify(snapshots.a["states"]))
    .toBe(JSON.stringify(snapshots.b["states"]));

  // the whole execution snapshot is equal except the exact plan_output
  // value inside the normalized orchestration metadata
  const stripPlanOutput = (snapshot: Record<string, unknown>): Record<string, unknown> => {
    const clone = JSON.parse(JSON.stringify(snapshot)) as Record<string, unknown>;
    const orchestration = clone["orchestration"] as { execution_roles: { role: string; plan_output?: string }[] };
    for (const role of orchestration.execution_roles) {
      delete role.plan_output;
    }
    return clone;
  };
  expect(stripPlanOutput(snapshots.a)).toEqual(stripPlanOutput(snapshots.b));
  const rolesA = (snapshots.a["orchestration"] as { execution_roles: { state_id: string; plan_output?: string }[] }).execution_roles;
  const rolesB = (snapshots.b["orchestration"] as { execution_roles: { state_id: string; plan_output?: string }[] }).execution_roles;
  expect(rolesA.map((role) => role.plan_output)).toEqual(["plan"]);
  expect(rolesB.map((role) => role.plan_output)).toEqual(["alt"]);
});

async function loadPipelineV2BundleDigest(yaml: string): Promise<string> {
  let digest = "";
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    digest = pipelineV2ExecutionDigest(pipeline);
  }, yaml);
  if (digest === "") {
    throw new Error("the bundle did not load");
  }
  return digest;
}

/**
 * The neutral fixture policy mechanically carried by every planning role of
 * the v2 fixtures: only fixture policy — the production wait entry does not
 * consume it in this increment.
 */
const NEUTRAL_STAGE_WAIT_YAML = `      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
`;

/** Replace the planning entry's stage_wait block of the canonical bundle. */
function withStageWait(stageWaitYaml: string): string {
  return CANONICAL_YAML.replace(NEUTRAL_STAGE_WAIT_YAML, stageWaitYaml);
}

const NEUTRAL_STAGE_WAIT = {
  reason: "stage_iteration_completed",
  actions: ["continue_stage", "revise_task"],
} as const;

test("34. minimal and full stage-wait policies compile with the exact resolved policy", async () => {
  // the single-action subsets and both permutations of the full set
  const policies: PipelineV2StageWaitActionName[][] = [
    ["continue_stage"],
    ["revise_task"],
    ["continue_stage", "revise_task"],
    ["revise_task", "continue_stage"],
  ];
  for (const actions of policies) {
    const yaml = withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
${actions.map((action) => `          - ${action}\n`).join("")}`);
    await withOrchestratedBundle(async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const orchestration = pipeline.orchestration;
      if (orchestration === undefined) {
        throw new Error("missing orchestration");
      }
      expect(orchestration.execution_roles[0]).toEqual({
        state_id: "architect",
        role: "planning",
        plan_output: "plan",
        stage_wait: { reason: "stage_iteration_completed", actions },
      });
      const compiled = compiledExecutionRoleFor(pipeline, "architect");
      if (compiled.role !== "planning") {
        throw new Error("the compiled role is not the planning role");
      }
      expect(compiled.stage_wait).toEqual({
        reason: "stage_iteration_completed",
        actions,
      });
    }, yaml);
  }
});

test("35. exact-field battery at the stage_wait level; unknown field names are never echoed", () => {
  // an unknown field inside stage_wait: value-free, name-free diagnostic
  rejectCompile(
    withStageWait(`${NEUTRAL_STAGE_WAIT_YAML}        target: coder\n`),
    /pipeline orchestration execution_roles 0 stage_wait has unknown fields/,
  );
  // missing reason
  rejectCompile(
    withStageWait(`      stage_wait:
        actions:
          - continue_stage
`),
    /pipeline orchestration execution_roles 0 stage_wait is missing required field "reason"/,
  );
  // missing actions
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
`),
    /pipeline orchestration execution_roles 0 stage_wait is missing required field "actions"/,
  );
  // stage_wait is not a mapping
  rejectCompile(
    withStageWait(`      stage_wait: 7
`),
    /pipeline orchestration execution_roles 0 stage_wait is not a YAML mapping/,
  );
  rejectCompile(
    withStageWait(`      stage_wait:
        - continue_stage
`),
    /pipeline orchestration execution_roles 0 stage_wait is not a YAML mapping/,
  );
});

test("36. inherited properties never satisfy the required shape", () => {
  const parsed = Bun.YAML.parse(CANONICAL_YAML) as Record<string, unknown>;
  // a planning entry whose stage_wait comes from the prototype chain
  const inheritedStageWait = Object.create({ stage_wait: NEUTRAL_STAGE_WAIT }) as Record<string, unknown>;
  inheritedStageWait.state_id = "architect";
  inheritedStageWait.role = "planning";
  inheritedStageWait.plan_output = "plan";
  const entryYaml = parsed as { orchestration: { execution_roles: Record<string, unknown>[] } };
  const architectEntry = entryYaml.orchestration.execution_roles.find(
    (entry) => (entry as { state_id: string }).state_id === "architect",
  );
  if (architectEntry === undefined) {
    throw new Error("the architect planning entry is missing");
  }
  const original = { ...architectEntry };
  Object.setPrototypeOf(architectEntry, inheritedStageWait);
  delete architectEntry.stage_wait;
  let caught: unknown = null;
  try {
    compilePipelineV2Spec(parsed);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(PipelineError);
  expect((caught as Error).message).toBe(
    'pipeline orchestration execution_roles 0 is missing required field "stage_wait"',
  );
  // restore and prove the same own-property rule inside stage_wait itself
  Object.setPrototypeOf(architectEntry, Object.prototype);
  Object.assign(architectEntry, original);
  const reasonProto = Object.create({ reason: "stage_iteration_completed" }) as Record<string, unknown>;
  reasonProto.actions = ["continue_stage"];
  architectEntry.stage_wait = reasonProto;
  caught = null;
  try {
    compilePipelineV2Spec(parsed);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(PipelineError);
  expect((caught as Error).message).toBe(
    'pipeline orchestration execution_roles 0 stage_wait is missing required field "reason"',
  );
  // and for actions
  const actionsProto = Object.create({ actions: ["continue_stage"] }) as Record<string, unknown>;
  actionsProto.reason = "stage_iteration_completed";
  architectEntry.stage_wait = actionsProto;
  caught = null;
  try {
    compilePipelineV2Spec(parsed);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(PipelineError);
  expect((caught as Error).message).toBe(
    'pipeline orchestration execution_roles 0 stage_wait is missing required field "actions"',
  );
  // an inherited plan_output on the planning entry is likewise not a field
  Object.setPrototypeOf(architectEntry, Object.prototype);
  Object.assign(architectEntry, original);
  const planProto = Object.create({ plan_output: "plan" }) as Record<string, unknown>;
  planProto.state_id = "architect";
  planProto.role = "planning";
  planProto.stage_wait = NEUTRAL_STAGE_WAIT;
  architectEntry.state_id = planProto.state_id;
  architectEntry.role = planProto.role;
  architectEntry.stage_wait = planProto.stage_wait;
  delete architectEntry.plan_output;
  Object.setPrototypeOf(architectEntry, planProto);
  caught = null;
  try {
    compilePipelineV2Spec(parsed);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(PipelineError);
  expect((caught as Error).message).toBe(
    'pipeline orchestration execution_roles 0 is missing required field "plan_output"',
  );
});

test("37. reason validation: missing, value-less and wrong types are typed errors without echoing the value", () => {
  const base = CANONICAL_YAML;
  // value-less reason (YAML null)
  rejectCompile(
    withStageWait(`      stage_wait:
        reason:
        actions:
          - continue_stage
          - revise_task
`),
    /pipeline orchestration execution_roles 0 stage_wait reason must be a safe non-empty identifier/,
  );
  for (const hostile of ["7", "{}", "true", '""', '"bad id!"', '"a..b"', '" stage_iteration_completed"']) {
    const yaml = withStageWait(`      stage_wait:
        reason: ${hostile}
        actions:
          - continue_stage
          - revise_task
`);
    expect(() => compilePipelineV2Spec(Bun.YAML.parse(yaml))).toThrow(
      PipelineError,
    );
    expect(() => compilePipelineV2Spec(Bun.YAML.parse(yaml))).toThrow(
      /pipeline orchestration execution_roles 0 stage_wait reason must be a safe non-empty identifier/,
    );
  }
  // the safe reason the wait manifest accepts compiles (same scalar semantics)
  acceptCompile(base);
});

test("38. actions validation: missing, non-array, empty, duplicate, unknown and non-string entries", () => {
  // non-array actions
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions: {}
`),
    /pipeline orchestration execution_roles 0 stage_wait actions must be a list, not a mapping or scalar/,
  );
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions: continue_stage
`),
    /pipeline orchestration execution_roles 0 stage_wait actions must be a list, not a mapping or scalar/,
  );
  // empty actions
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions: []
`),
    /pipeline orchestration execution_roles 0 stage_wait actions must not be empty/,
  );
  // duplicate action ids: the second occurrence's position is named
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - continue_stage
`),
    /pipeline orchestration execution_roles 0 stage_wait declares a duplicate action id at position 1/,
  );
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - revise_task
          - revise_task
`),
    /pipeline orchestration execution_roles 0 stage_wait declares a duplicate action id at position 1/,
  );
  // an unknown action id: typed rejection without echoing the value
  rejectCompile(
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - pause_stage
`),
    /pipeline orchestration execution_roles 0 stage_wait action at position 1 must be one of \["continue_stage","revise_task"\]/,
  );
  // non-string entries: typed rejection without echoing the value
  for (const hostile of ["7", "null", "true", "{}", "id: continue_stage"]) {
    const yaml = withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - ${hostile}
`);
    expect(() => compilePipelineV2Spec(Bun.YAML.parse(yaml))).toThrow(
      /pipeline orchestration execution_roles 0 stage_wait action at position 1 must be one of \["continue_stage","revise_task"\]/,
    );
  }
});

test("39. control and stage roles do not accept stage_wait", () => {
  rejectCompile(
    CANONICAL_YAML.replace(
      "    - state_id: stage_dispatch\n      role: control\n",
      `    - state_id: stage_dispatch\n      role: control\n${NEUTRAL_STAGE_WAIT_YAML}`,
    ),
    /pipeline orchestration execution_roles 1 has unknown field "stage_wait"/,
  );
  rejectCompile(
    CANONICAL_YAML.replace(
      "    - state_id: coder\n      role: stage\n      stage_template: development\n",
      `    - state_id: coder\n      role: stage\n      stage_template: development\n${NEUTRAL_STAGE_WAIT_YAML}`,
    ),
    /pipeline orchestration execution_roles 3 has unknown field "stage_wait"/,
  );
});

test("40. the declared action order is preserved verbatim and never sorted", async () => {
  await withOrchestratedBundle(
    async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      const orchestration = pipeline.orchestration;
      if (orchestration === undefined) {
        throw new Error("missing orchestration");
      }
      const planning = orchestration.execution_roles[0];
      if (planning === undefined || planning.role !== "planning") {
        throw new Error("the planning entry is missing");
      }
      expect(planning.stage_wait.actions).toEqual(["revise_task", "continue_stage"]);
      // repeated loads keep the declared order
      const second = await loadPipelineV2(dirs.bundle);
      const secondPlanning = second.orchestration?.execution_roles[0];
      if (secondPlanning === undefined || secondPlanning.role !== "planning") {
        throw new Error("the planning entry is missing");
      }
      expect(secondPlanning.stage_wait.actions).toEqual(["revise_task", "continue_stage"]);
      const compiled = compiledExecutionRoleFor(pipeline, "architect");
      if (compiled.role !== "planning") {
        throw new Error("the compiled role is not the planning role");
      }
      expect(compiled.stage_wait.actions).toEqual(["revise_task", "continue_stage"]);
    },
    withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - revise_task
          - continue_stage
`),
  );
});

test("41. the resolved policy is recursively frozen and isolated from the parsed input", async () => {
  const parsed = Bun.YAML.parse(CANONICAL_YAML) as Record<string, unknown>;
  const spec = compilePipelineV2Spec(parsed);
  const orchestration = spec.orchestration;
  if (orchestration === undefined) {
    throw new Error("missing orchestration");
  }
  const planning = orchestration.execution_roles[0];
  if (planning === undefined || planning.role !== "planning") {
    throw new Error("the planning entry is missing");
  }
  // the spec's policy objects are parser-built fresh objects; freezing
  // happens at load time, so only mutation isolation is asserted here
  expect(planning.stage_wait).toEqual(NEUTRAL_STAGE_WAIT);
  // the parsed input is not frozen and mutating it after the compile does
  // not change the compiled metadata (the parser builds its own objects)
  const inputRoles = (parsed as {
    orchestration: {
      execution_roles: { state_id: string; role: string; stage_wait?: { reason?: string; actions?: string[] } }[];
    };
  }).orchestration.execution_roles;
  const inputPlanning = inputRoles.find((entry) => entry.state_id === "architect");
  if (inputPlanning?.stage_wait === undefined) {
    throw new Error("the parsed planning entry lost its stage_wait");
  }
  expect(Object.isFrozen(inputPlanning.stage_wait)).toBe(false);
  inputPlanning.stage_wait.reason = "mutated_after_compile";
  if (inputPlanning.stage_wait.actions) {
    inputPlanning.stage_wait.actions.push("injected");
  }
  expect(planning.stage_wait).toEqual(NEUTRAL_STAGE_WAIT);
  // and the resolved snapshot of a fresh load is equally frozen
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const resolvedPlanning = pipeline.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    expect(Object.isFrozen(resolvedPlanning)).toBe(true);
    expect(Object.isFrozen(resolvedPlanning.stage_wait)).toBe(true);
    expect(Object.isFrozen(resolvedPlanning.stage_wait.actions)).toBe(true);
  });
});

test("42. the compiled planning role carries the exact policy shape; control and stage roles are unchanged", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const planning = compiledExecutionRoleFor(pipeline, "architect");
    expect(Object.keys(planning)).toEqual(["state_id", "role", "plan_output", "stage_wait"]);
    if (planning.role !== "planning") {
      throw new Error("the compiled role is not the planning role");
    }
    expect(Object.keys(planning.stage_wait)).toEqual(["reason", "actions"]);
    expect(Object.isFrozen(planning)).toBe(true);
    expect(Object.isFrozen(planning.stage_wait)).toBe(true);
    expect(Object.isFrozen(planning.stage_wait.actions)).toBe(true);
    expect(planning.stage_wait).toEqual(NEUTRAL_STAGE_WAIT);
    // the compiled actions array is a fresh frozen copy, not an alias of
    // the resolved metadata's array
    const resolvedPlanning = pipeline.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    expect(planning.stage_wait.actions).not.toBe(resolvedPlanning.stage_wait.actions);
    expect(planning.stage_wait).not.toBe(resolvedPlanning.stage_wait);
    // control and stage compiled roles stay exactly as before
    expect(compiledExecutionRoleFor(pipeline, "stage_dispatch")).toEqual({
      state_id: "stage_dispatch",
      role: "control",
    });
    expect(Object.keys(compiledExecutionRoleFor(pipeline, "stage_dispatch"))).toEqual([
      "state_id",
      "role",
    ]);
    expect(compiledExecutionRoleFor(pipeline, "coder")).toEqual({
      state_id: "coder",
      role: "stage",
      stage_template: "development",
    });
    expect(Object.keys(compiledExecutionRoleFor(pipeline, "coder"))).toEqual([
      "state_id",
      "role",
      "stage_template",
    ]);
  });
});

test("43. the provenance gate still fires before any policy read; caller mutation cannot reach the compiled policy", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const resolved = await loadPipelineV2(dirs.bundle);
    const UNTRUSTED =
      "compiledExecutionRoleFor requires the deep-frozen snapshot object returned by loadPipelineV2; " +
      "hand-built objects, casts, clones and Proxies are rejected before any content is read";
    // a deep clone carrying the same policy bytes is still rejected before
    // any field (including stage_wait) is read
    const clone = structuredClone(resolved);
    expect(clone).toEqual(resolved);
    let trapCalls = 0;
    const proxied = new Proxy(clone, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() => compiledExecutionRoleFor(proxied, "architect")).toThrow(UNTRUSTED);
    expect(trapCalls).toBe(0);
    // mutating the trusted snapshot's frozen metadata is refused
    const resolvedPlanning = resolved.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    expect(() => {
      (resolvedPlanning.stage_wait.actions as string[]).push("injected");
    }).toThrow();
    expect(resolvedPlanning.stage_wait).toEqual(NEUTRAL_STAGE_WAIT);
  });
});

test("44. content-free diagnostics: hostile policy values and field names are never echoed", async () => {
  const canaryReason = "CANARY stage wait reason 9f3a!";
  const canaryField = "CANARY_stage_wait_field_9f3a";
  const canaryAction = "CANARY_stage_wait_action_9f3a";
  const caught: string[] = [];
  const capture = (yaml: string): void => {
    try {
      compilePipelineV2Spec(Bun.YAML.parse(yaml));
      throw new Error("the hostile policy compiled");
    } catch (error) {
      if (error instanceof PipelineError) {
        caught.push(error.message);
        return;
      }
      throw error;
    }
  };
  // hostile reason value
  capture(withStageWait(`      stage_wait:
        reason: ${canaryReason}
        actions:
          - continue_stage
`));
  // hostile unknown field name
  capture(withStageWait(`${NEUTRAL_STAGE_WAIT_YAML}        ${canaryField}: 1\n`));
  // hostile action entry
  capture(withStageWait(`      stage_wait:
        reason: stage_iteration_completed
        actions:
          - ${canaryAction}
`));
  expect(caught.length).toBe(3);
  for (const message of caught) {
    expect(message).not.toContain(canaryReason);
    expect(message).not.toContain(canaryField);
    expect(message).not.toContain(canaryAction);
  }
});

test("45. the stage-wait parsing is the single chain over the shared scalar predicate; no second serializer, digest or registry", () => {
  const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2.ts"), "utf8");
  // the stage-wait reason grammar is the shared scalar predicate of the
  // wait manifest layer (no second reason validator, no wider grammar)
  const reasonChecks = source.match(/reason must be a safe non-empty identifier/g) ?? [];
  expect(reasonChecks.length).toBe(1);
  expect(source).toContain(
    'import { isPipelineV2SafeId, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts"',
  );
  // the parseStageWait body owns one chain: no serializer, no digest
  // machinery, no registry, no sorting of the declared actions
  const start = source.indexOf("function parseStageWait");
  const end = source.indexOf("function parseOrchestration");
  if (start < 0 || end < 0 || end <= start) {
    throw new Error("the parseStageWait body is missing");
  }
  const body = source.slice(start, end);
  expect(body).not.toContain("canonicalJson");
  expect(body).not.toContain("CryptoHasher");
  expect(body).not.toContain("sortById");
  expect(body).not.toContain("WeakSet");
  expect(body).not.toContain("WeakMap");
  expect(body).not.toContain("sort(");
  // the parser is invoked exactly once per planning entry (single chain)
  const callSites = source.match(/parseStageWait\(/g) ?? [];
  expect(callSites.length).toBe(2);
  // and the stage-wait policy never reaches the coordinator/runner/CLI or
  // the durable state modules from the compiler
  expect(source).not.toContain('from "./pipeline_v2_state.ts"');
  expect(source).not.toContain('from "./pipeline_v2_coordinator.ts"');
  expect(source).not.toContain('from "./pipeline_v2_runner.ts"');
  expect(source).not.toContain('from "./pipeline_v2_wait_manifest.ts"');
});

// ---------------------------------------------------------------------------
// The trusted automatic plan-ready continuation policy (planning roles only).
// ---------------------------------------------------------------------------

const NEUTRAL_PLAN_READY_YAML = `      plan_ready:
        stage_position: 1
        initial_budget: 1
`;

const NEUTRAL_PLAN_READY = { stage_position: 1, initial_budget: 1 } as const;

/** Append a plan_ready block after the canonical planning entry's stage_wait. */
function withPlanReady(planReadyYaml: string): string {
  return CANONICAL_YAML.replace(NEUTRAL_STAGE_WAIT_YAML, `${NEUTRAL_STAGE_WAIT_YAML}${planReadyYaml}`);
}

test("46. a missing plan_ready policy is accepted and the resolved/compiled shapes stay exactly as before", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const resolvedPlanning = pipeline.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    // the resolved role carries no plan_ready key at all (absent, never null)
    expect(Object.keys(resolvedPlanning)).toEqual(["state_id", "role", "plan_output", "stage_wait"]);
    expect("plan_ready" in resolvedPlanning).toBe(false);
    const compiled = compiledExecutionRoleFor(pipeline, "architect");
    expect(Object.keys(compiled)).toEqual(["state_id", "role", "plan_output", "stage_wait"]);
    if (compiled.role !== "planning") {
      throw new Error("the compiled role is not the planning role");
    }
    expect("plan_ready" in compiled).toBe(false);
  });
});

test("47. the declared plan_ready policy compiles with identical spec/resolved/compiled shapes", async () => {
  const spec = compilePipelineV2Spec(Bun.YAML.parse(withPlanReady(NEUTRAL_PLAN_READY_YAML)));
  const planningSpec = spec.orchestration?.execution_roles[0];
  if (planningSpec === undefined || planningSpec.role !== "planning") {
    throw new Error("the planning entry is missing");
  }
  expect(planningSpec.plan_ready).toEqual(NEUTRAL_PLAN_READY);
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const resolvedPlanning = pipeline.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    expect(Object.keys(resolvedPlanning)).toEqual(["state_id", "role", "plan_output", "stage_wait", "plan_ready"]);
    expect(resolvedPlanning.plan_ready).toEqual(NEUTRAL_PLAN_READY);
    const compiled = compiledExecutionRoleFor(pipeline, "architect");
    if (compiled.role !== "planning") {
      throw new Error("the compiled role is not the planning role");
    }
    expect(Object.keys(compiled)).toEqual(["state_id", "role", "plan_output", "stage_wait", "plan_ready"]);
    expect(compiled.plan_ready).toEqual(NEUTRAL_PLAN_READY);
    // non-planning compiled roles stay exactly as before
    expect(Object.keys(compiledExecutionRoleFor(pipeline, "stage_dispatch"))).toEqual(["state_id", "role"]);
  }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
});

test("48. the plan_ready policy accepts arbitrary positive safe integer positions and budgets", async () => {
  const cases = [
    { stage_position: 2, initial_budget: 1 },
    { stage_position: 1, initial_budget: 7 },
    { stage_position: 3, initial_budget: 1000 },
    { stage_position: 9007199254740991, initial_budget: 9007199254740991 },
  ];
  for (const policy of cases) {
    const yaml = withPlanReady(`      plan_ready:
        stage_position: ${policy.stage_position}
        initial_budget: ${policy.initial_budget}
`);
    acceptCompile(yaml);
    const spec = compilePipelineV2Spec(Bun.YAML.parse(yaml));
    const planningSpec = spec.orchestration?.execution_roles[0];
    if (planningSpec === undefined || planningSpec.role !== "planning") {
      throw new Error("the planning entry is missing");
    }
    expect(planningSpec.plan_ready).toEqual(policy);
  }
});

test("49. exact-field battery at the plan_ready level; unknown field names are never echoed", () => {
  const canaryField = "CANARY_plan_ready_field_9f3a";
  // an unknown field inside plan_ready: value-free, name-free diagnostic
  const unknown = withPlanReady(`${NEUTRAL_PLAN_READY_YAML}        target: coder\n`);
  let caught = "";
  try {
    compilePipelineV2Spec(Bun.YAML.parse(unknown));
  } catch (cause) {
    if (cause instanceof PipelineError) {
      caught = cause.message;
    } else {
      throw cause;
    }
  }
  expect(caught).toMatch(/execution_roles 0 plan_ready has unknown fields/);
  expect(caught).not.toContain(canaryField);
  // missing stage_position / initial_budget name only the contract field
  rejectCompile(
    withPlanReady(`      plan_ready:
        initial_budget: 1
`),
    /pipeline orchestration execution_roles 0 plan_ready is missing required field "stage_position"/,
  );
  rejectCompile(
    withPlanReady(`      plan_ready:
        stage_position: 1
`),
    /pipeline orchestration execution_roles 0 plan_ready is missing required field "initial_budget"/,
  );
  // plan_ready is not a mapping
  rejectCompile(
    withPlanReady(`      plan_ready: 7
`),
    /pipeline orchestration execution_roles 0 plan_ready is not a YAML mapping/,
  );
  rejectCompile(
    withPlanReady(`      plan_ready:
        - 1
`),
    /pipeline orchestration execution_roles 0 plan_ready is not a YAML mapping/,
  );
});

test("50. plan_ready value battery: wrong types, zero, negative, fractional and overflow are rejected without echoing values", () => {
  const wrongValues = [
    `"1"`, `true`, `null`, `[1]`, `{}`, `0`, `-1`, `1.5`, `9007199254740992`, `-9007199254740992`, `.inf`, `.nan`,
  ];
  for (const value of wrongValues) {
    rejectCompile(
      withPlanReady(`      plan_ready:
        stage_position: ${value}
        initial_budget: 1
`),
      /plan_ready stage_position must be a positive safe integer/,
    );
    rejectCompile(
      withPlanReady(`      plan_ready:
        stage_position: 1
        initial_budget: ${value}
`),
      /plan_ready initial_budget must be a positive safe integer/,
    );
  }
});

test("51. plan_ready is allowed only on planning roles; control and stage roles reject it as an unknown field", () => {
  const controlYaml = withOrchestrationSection(
    `orchestration:
  stage_templates:
    - id: development
      entry_state: coder
  execution_roles:
    - state_id: stage_dispatch
      role: control
      plan_ready:
        stage_position: 1
        initial_budget: 1
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: coder
      role: stage
      stage_template: development
`,
  );
  rejectCompile(controlYaml, /execution_roles 0 has unknown field "plan_ready"/);
  const stageYaml = withOrchestrationSection(
    `orchestration:
  stage_templates:
    - id: development
      entry_state: coder
  execution_roles:
    - state_id: coder
      role: stage
      stage_template: development
      plan_ready:
        stage_position: 1
        initial_budget: 1
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: stage_dispatch
      role: control
`,
  );
  rejectCompile(stageYaml, /execution_roles 0 has unknown field "plan_ready"/);
});

test("52. the resolved plan_ready policy is recursively frozen and isolated from the parsed input", async () => {
  const parsed = Bun.YAML.parse(withPlanReady(NEUTRAL_PLAN_READY_YAML)) as {
    orchestration: {
      execution_roles: Array<{ state_id: string; plan_ready?: { stage_position?: number; initial_budget?: number } }>;
    };
  };
  const spec = compilePipelineV2Spec(parsed);
  const planningSpec = spec.orchestration?.execution_roles[0];
  if (planningSpec === undefined || planningSpec.role !== "planning" || planningSpec.plan_ready === undefined) {
    throw new Error("the planning entry lost its plan_ready");
  }
  // the parsed input is not frozen and mutating it after the compile does
  // not change the compiled metadata (the parser builds its own objects)
  const inputPlanning = parsed.orchestration.execution_roles.find((entry) => entry.state_id === "architect");
  if (inputPlanning?.plan_ready === undefined) {
    throw new Error("the parsed planning entry lost its plan_ready");
  }
  expect(Object.isFrozen(inputPlanning.plan_ready)).toBe(false);
  inputPlanning.plan_ready.stage_position = 99;
  inputPlanning.plan_ready.initial_budget = 99;
  expect(planningSpec.plan_ready).toEqual(NEUTRAL_PLAN_READY);
  // the resolved snapshot of a fresh load is recursively frozen
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const resolvedPlanning = pipeline.orchestration?.execution_roles[0];
    if (resolvedPlanning === undefined || resolvedPlanning.role !== "planning" || resolvedPlanning.plan_ready === undefined) {
      throw new Error("the planning entry lost its plan_ready");
    }
    expect(Object.isFrozen(resolvedPlanning.plan_ready)).toBe(true);
    let threw = false;
    try {
      (resolvedPlanning.plan_ready as { stage_position: number }).stage_position = 42;
    } catch {
      threw = true;
    }
    expect(threw || resolvedPlanning.plan_ready.stage_position === 1).toBe(true);
    expect(resolvedPlanning.plan_ready).toEqual(NEUTRAL_PLAN_READY);
  }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
});

test("53. the compiled plan_ready policy is a fresh frozen copy per call; caller mutation cannot reach it", async () => {
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const first = compiledExecutionRoleFor(pipeline, "architect");
    if (first.role !== "planning" || first.plan_ready === undefined) {
      throw new Error("the compiled planning role lost its plan_ready");
    }
    const second = compiledExecutionRoleFor(pipeline, "architect");
    if (second.role !== "planning" || second.plan_ready === undefined) {
      throw new Error("the compiled planning role lost its plan_ready");
    }
    const firstReady = first.role === "planning" ? first.plan_ready : undefined;
    const secondReady = second.role === "planning" ? second.plan_ready : undefined;
    if (firstReady === undefined || secondReady === undefined) {
      throw new Error("the compiled planning role lost its plan_ready");
    }
    expect(first).toEqual(second);
    expect(first).not.toBe(second);
    expect(firstReady).not.toBe(secondReady);
    const storedReady = (pipeline.orchestration?.execution_roles[0] as unknown as { plan_ready?: unknown }).plan_ready;
    expect(firstReady).not.toBe(storedReady);
    expect(Object.isFrozen(firstReady)).toBe(true);
    // mutating the returned copy changes nothing for later calls
    const mutable = firstReady as { stage_position: number };
    try {
      mutable.stage_position = 42;
    } catch {
      // frozen
    }
    expect(compiledExecutionRoleFor(pipeline, "architect")).toEqual(second);
  }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
});

test("54. the provenance gate fires before any plan_ready read; Proxy traps and getters never run", async () => {
  let traps = 0;
  let getters = 0;
  const hostile = new Proxy(
    { orchestration: { execution_roles: [{ state_id: "architect", role: "planning", plan_output: "plan", stage_wait: NEUTRAL_STAGE_WAIT, plan_ready: NEUTRAL_PLAN_READY }] } },
    {
      get(target, property) {
        traps += 1;
        getters += 1;
        return Reflect.get(target, property);
      },
    },
  ) as unknown as ResolvedPipelineV2;
  expect(() => compiledExecutionRoleFor(hostile, "architect")).toThrow(PipelineError);
  expect(traps).toBe(0);
  expect(getters).toBe(0);
  // a spread clone is equally rejected
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const cloned = { ...pipeline } as unknown as ResolvedPipelineV2;
    expect(() => compiledExecutionRoleFor(cloned, "architect")).toThrow(PipelineError);
  }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
});

test("55. the plan_ready policy joins the execution snapshot and moves the digest separately for each field", async () => {
  const without = await (async () => {
    let digest = "";
    await withOrchestratedBundle(async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      digest = pipelineV2ExecutionDigest(pipeline);
    });
    return digest;
  })();
  const withPolicy = await (async () => {
    let digest = "";
    await withOrchestratedBundle(async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      digest = pipelineV2ExecutionDigest(pipeline);
    }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
    return digest;
  })();
  const positionMoved = await (async () => {
    let digest = "";
    await withOrchestratedBundle(async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      digest = pipelineV2ExecutionDigest(pipeline);
    }, withPlanReady(`      plan_ready:
        stage_position: 2
        initial_budget: 1
`));
    return digest;
  })();
  const budgetMoved = await (async () => {
    let digest = "";
    await withOrchestratedBundle(async (dirs) => {
      const pipeline = await loadPipelineV2(dirs.bundle);
      digest = pipelineV2ExecutionDigest(pipeline);
    }, withPlanReady(`      plan_ready:
        stage_position: 1
        initial_budget: 2
`));
    return digest;
  })();
  expect(withPolicy).not.toBe(without);
  expect(positionMoved).not.toBe(withPolicy);
  expect(budgetMoved).not.toBe(withPolicy);
  expect(positionMoved).not.toBe(budgetMoved);
  // and the snapshot carries the policy verbatim
  await withOrchestratedBundle(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const snapshot = pipelineV2ExecutionSnapshot(pipeline) as { orchestration?: { execution_roles: Array<Record<string, unknown>> } };
    const planning = snapshot.orchestration?.execution_roles[0];
    expect(planning?.["plan_ready"]).toEqual(NEUTRAL_PLAN_READY);
  }, withPlanReady(NEUTRAL_PLAN_READY_YAML));
});
