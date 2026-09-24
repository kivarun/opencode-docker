import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineV2OrchestrationError } from "../src/pipeline_v2_orchestration.ts";
import { PipelineError } from "../src/pipeline.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  comparePipelineV2RunIdentity,
  type PipelineV2RunIdentityComparison,
} from "../src/pipeline_v2_identity_compare.ts";
import {
  PipelineV2StateError,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2AgentExecutionPhase,
  type PipelineV2RunCommand,
  type PipelineV2RunPipelineIdentity,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  PipelineV2CompiledRunPlanError,
  type CompiledPipelineV2RunPlan,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import {
  PipelineV2RunPlanAcceptanceError,
  verifyPipelineV2RunPlanCandidateForAcceptance,
  type PipelineV2RunPlanAcceptanceErrorReason,
} from "../src/pipeline_v2_run_plan_acceptance.ts";
import * as acceptanceModule from "../src/pipeline_v2_run_plan_acceptance.ts";

const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const CANARY = "CANARY_secret_task_body";

const FACTS_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

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

const TWO_TEMPLATES_YAML = `schema_version: 2
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

orchestration:
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

states:
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

/** Planning agent -> control dispatcher -> stage template entry -> stage agent. */
const ORCHESTRATED_YAML = `schema_version: 2
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

orchestration:
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

states:
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
    inputs: []
    outputs: []
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
    inputs: []
    outputs: []
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

async function withPipeline(
  fn: (pipeline: ResolvedPipelineV2, bundle: string) => Promise<void>,
  yaml: string = ORCHESTRATED_YAML,
  dispatchModelYaml: string = DISPATCH_MODEL_YAML,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-acceptance-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), yaml);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "prompts", "reviewer.md"), "review the work\n");
    await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(bundle, "decisions", "dispatch.yaml"), dispatchModelYaml);
    await writeFile(join(bundle, "decisions", "gate.yaml"), GATE_MODEL_YAML);
    await fn(await loadPipelineV2(bundle), bundle);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

let sessionCounter = 0;

function agentRun(sessionLabel: string): PipelineV2RunCommand[] {
  sessionCounter += 1;
  const n = sessionCounter;
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${n}-${sessionLabel}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${n}-${sessionLabel}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
    { kind: "agent_cleanup_completed" },
  ];
}

function decisionSelected(outcome: string): PipelineV2RunCommand {
  return {
    kind: "decision_evaluated",
    result: {
      status: "selected",
      outcome,
      decision: outcome,
      rule_id: `rule-${outcome}`,
      active_constraint_ids: [],
    },
  };
}

function drive(
  identity: PipelineV2RunPipelineIdentity,
  runId: string,
  commands: readonly PipelineV2RunCommand[],
): PipelineV2RunState {
  let state: PipelineV2RunState | null = null;
  commands.forEach((command, index) => {
    state = reducePipelineV2RunCommand(state, command, new Date(Date.UTC(2026, 8, 24, 0, 0, index + 1)));
  });
  if (state === null) {
    throw new Error("no state was produced");
  }
  return state;
}

function preparedTask(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
  runId = "run-1",
): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: runId,
    task_id: taskId,
    revision,
    previous_sha256: previousSha256,
    origin,
    body,
  });
}

const A1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
const B1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");
const C1 = preparedTask("task-c", 1, null, "planning_proposal", "Body C one");

interface StageSpec {
  readonly id: string;
  readonly template: string;
  readonly tasks: readonly {
    readonly id: string;
    readonly revision: number;
    readonly sha256: string;
    readonly depends_on: readonly string[];
  }[];
}

function preparedCandidate(
  stages: readonly StageSpec[],
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
  originExecution: number,
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidate({
    plan: preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: "run-1",
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: originExecution,
      stages,
    }),
    taskRevisions,
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

/** Single-stage candidate for the development template. */
function singleStageCandidate(originExecution: number): PreparedPipelineV2RunPlanCandidate {
  return preparedCandidate(
    [
      {
        id: "stage-1",
        template: "development",
        tasks: [
          { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
          { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
        ],
      },
    ],
    [A1, B1],
    originExecution,
  );
}

/** Two-stage candidate: development then testing. */
function twoStageCandidate(originExecution: number): PreparedPipelineV2RunPlanCandidate {
  return preparedCandidate(
    [
      {
        id: "stage-1",
        template: "development",
        tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
      },
      {
        id: "stage-2",
        template: "testing",
        tasks: [{ id: "task-c", revision: 1, sha256: C1.sha256, depends_on: [] }],
      },
    ],
    [A1, C1],
    originExecution,
  );
}

const WAITING_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-waiting"),
  {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
    executionIndex: 1,
  },
  {
    kind: "run_waiting",
    stateId: "stage_dispatch",
    reason: "stage_iteration_limit_exhausted",
    requestSha256: hex("1"),
    actions: [{ id: "continue_stage", to: "development_entry" }],
  },
];

const PUBLISHING_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-publishing"),
  {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
    executionIndex: 1,
  },
  { kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" },
  decisionSelected("d_plan_complete"),
  {
    kind: "transition_committed",
    step: { from: "stage_dispatch", outcome: "d_plan_complete", to: "done", transition_index: 1 },
    executionIndex: 2,
  },
  { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
];

const PUBLISHED_COMMANDS: PipelineV2RunCommand[] = [
  ...PUBLISHING_COMMANDS,
  {
    kind: "run_outputs_published",
    outputs: [{ id: "final_report", type: "file", required: true, present: true, digest: hex("d") }],
  },
];

const SUCCESS_COMMANDS: PipelineV2RunCommand[] = [
  ...PUBLISHED_COMMANDS,
  { kind: "run_succeeded" },
];

const FAILED_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-failed").slice(0, 3),
  {
    kind: "agent_failed",
    reason: "worker_failed",
    sessionCleanup: { execution: "completed", tool: "completed" },
  },
  { kind: "run_failed", reason: "worker_failed" },
];

/** Commands up to an unbound settled agent execution on the planning state. */
const HAPPY_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-happy"),
];

/** The in-flight agent phase prefixes (after `start_agent_execution`). */
const AGENT_PHASE_COMMANDS: readonly PipelineV2RunCommand[] = agentRun("architect-phases");

function agentPhaseStates(identity: PipelineV2RunPipelineIdentity, runId: string): PipelineV2RunState[] {
  const states: PipelineV2RunState[] = [];
  for (let stop = 0; stop <= AGENT_PHASE_COMMANDS.length; stop++) {
    states.push(
      drive(identity, runId, [
        { kind: "create_run", runId, pipeline: identity, inputs: [{ id: "task", type: "file", protected: true, digest: hex("b") }] },
        { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
        ...AGENT_PHASE_COMMANDS.slice(0, stop),
      ]),
    );
  }
  return states;
}

/** Commands up to an unbound settled agent execution on the stage state `coder`. */
const STAGE_ROLE_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-chain"),
  {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
    executionIndex: 1,
  },
  { kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" },
  decisionSelected("d_next_stage"),
  {
    kind: "transition_committed",
    step: { from: "stage_dispatch", outcome: "d_next_stage", to: "development_entry", transition_index: 0 },
    executionIndex: 2,
  },
  { kind: "start_agent_execution", stateId: "development_entry", profile: "coder", executionRole: "planning" },
  ...agentRun("development-entry-chain"),
  {
    kind: "transition_committed",
    step: { from: "development_entry", outcome: "completed", to: "coder", transition_index: 0 },
    executionIndex: 3,
  },
  { kind: "start_agent_execution", stateId: "coder", profile: "coder", executionRole: "planning" },
  ...agentRun("coder-chain"),
];

/** Commands up to an unbound settled agent execution on `development_entry` (index 3). */
const SECOND_AGENT_COMMANDS: PipelineV2RunCommand[] = STAGE_ROLE_COMMANDS.slice(0, STAGE_ROLE_COMMANDS.length - 8);

const DECISION_BOUND_PRELUDE: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-decision"),
  {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
    executionIndex: 1,
  },
];

const DECISION_UNBOUND_COMMANDS: PipelineV2RunCommand[] = [
  ...DECISION_BOUND_PRELUDE,
  { kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" },
  decisionSelected("d_next_stage"),
];

const BOUND_COMMANDS: PipelineV2RunCommand[] = [
  { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
  ...agentRun("architect-bound"),
  {
    kind: "transition_committed",
    step: { from: "architect", outcome: "completed", to: "stage_dispatch", transition_index: 0 },
    executionIndex: 1,
  },
];

const BASE_INPUTS = [{ id: "task", type: "file", protected: true, digest: hex("b") }] as const;

function createState(
  identity: PipelineV2RunPipelineIdentity,
  runId: string,
  commands: readonly PipelineV2RunCommand[],
): PipelineV2RunState {
  return drive(identity, runId, [
    { kind: "create_run", runId, pipeline: identity, inputs: BASE_INPUTS },
    ...commands,
  ]);
}

function expectAcceptanceError(
  cause: unknown,
  reason: PipelineV2RunPlanAcceptanceErrorReason,
): PipelineV2RunPlanAcceptanceError {
  expect(cause).toBeInstanceOf(PipelineV2RunPlanAcceptanceError);
  const error = cause as PipelineV2RunPlanAcceptanceError;
  expect(error.reason).toBe(reason);
  return error;
}

async function catchVerify(
  pipeline: ResolvedPipelineV2,
  state: unknown,
  candidate: PreparedPipelineV2RunPlanCandidate,
): Promise<unknown> {
  try {
    verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state as PipelineV2RunState, candidate);
  } catch (cause) {
    return cause;
  }
  throw new Error("expected the acceptance verifier to reject");
}

test("1. happy path: the successful settled-but-unbound planning agent accepts the candidate", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = singleStageCandidate(1);
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state, candidate);
    expect(compiled).toEqual({
      run_id: "run-1",
      plan_revision: 1,
      plan_sha256: candidate.plan.sha256,
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          entry_state: "development_entry",
          state_ids: ["coder", "development_entry", "iteration_gate", "stage_review"],
          tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ],
    });
  });
});

test("2. the returned compiled plan is the provenance-backed object; the stage lookup accepts it", async () => {
  await withPipeline(
    async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = twoStageCandidate(1);
    const verified = verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state, candidate);
    // a fresh compile is a fresh object; the verifier's object is the one
    // registered by its own single internal compile call
    const external = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(verified).not.toBe(external);
    expect(verified).toEqual(external);
    // the verifier's returned object is provenance-backed: the stage
    // lookup accepts it and returns the exact frozen stage
    const stage = verified.stages[0];
    if (stage === undefined) {
      throw new Error("missing stage");
    }
    expect(compiledPipelineV2RunPlanStageFor(verified, "stage-1")).toBe(stage);
    },
    TWO_TEMPLATES_YAML,
    DISPATCH_MODEL_YAML_TWO_TEMPLATES,
  );
});

test("3. a multi-stage candidate verifies with both stages in the projection", async () => {
  await withPipeline(
    async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = twoStageCandidate(1);
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state, candidate);
    expect(compiled.stages.map((stage) => stage.id)).toEqual(["stage-1", "stage-2"]);
    expect(compiled.stages.map((stage) => stage.entry_state)).toEqual([
      "development_entry",
      "testing_entry",
    ]);
    },
    TWO_TEMPLATES_YAML,
    DISPATCH_MODEL_YAML_TWO_TEMPLATES,
  );
});

test("4. a candidate bound to a foreign run is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-2", HAPPY_COMMANDS);
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "candidate_mismatch");
    expect((cause as Error).message).toBe(
      'the compiled plan candidate belongs to run "run-1"; the durable run is "run-2"',
    );
  });
});

test("5. an origin naming an older bound execution is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    // unbound execution on development_entry (index 3); the candidate's
    // origin names the first, already bound execution (index 1)
    const state = createState(identity, "run-1", SECOND_AGENT_COMMANDS);
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "origin_execution_mismatch");
  });
});

test("6. an origin beyond the last execution is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = singleStageCandidate(99);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "origin_execution_mismatch");
    expect((cause as Error).message).toBe(
      "the compiled plan's origin_execution 99 does not name the last settled-but-unbound execution",
    );
  });
});

test("7. a run without executions is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", []);
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain("0 executions and 0 committed transitions");
  });
});

test("8. an execution that is already bound by its transition is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", BOUND_COMMANDS);
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain("1 executions and 1 committed transitions");
  });
});

test("9. every in-flight agent phase is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const phases: readonly PipelineV2AgentExecutionPhase[] = [
      "started",
      "data_prepared",
      "execution_session_created",
      "sessions_created",
      "running",
      "outputs_accepted",
    ];
    const states = agentPhaseStates(identity, "run-1");
    // states[0] is the phase right after start; each carries one in-flight
    // phase; the settled boundary itself is not part of this battery
    expect(phases.length).toBe(states.length - 1);
    for (let index = 0; index < phases.length; index++) {
      const phase = phases[index];
      if (phase === undefined) {
        throw new Error(`missing phase at ${index}`);
      }
      const state = states[index];
      if (state === undefined) {
        throw new Error(`missing phase state ${index}`);
      }
      const last = state.executions[0];
      expect(last?.phase).toBe(phase);
      const candidate = singleStageCandidate(1);
      const cause = await catchVerify(pipeline, state, candidate);
      expectAcceptanceError(cause, "invalid_state");
      expect((cause as Error).message).toContain(`phase "${phase}"`);
    }
  });
});

test("10. a failed agent execution is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const failedAgent = createState(identity, "run-1", [
      { kind: "start_agent_execution", stateId: "architect", profile: "coder", executionRole: "planning" },
      ...agentRun("architect-failed").slice(0, 3),
      {
        kind: "agent_failed",
        reason: "worker_failed",
        sessionCleanup: { execution: "completed", tool: "completed" },
      },
    ]);
    const candidate = singleStageCandidate(1);
    let cause = await catchVerify(pipeline, failedAgent, candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain('phase "failed"');
    // and the finalized failed run is rejected too
    cause = await catchVerify(pipeline, createState(identity, "run-1", FAILED_COMMANDS), candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain('status "failed"');
  });
});

test("11. a decision execution is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const candidate = singleStageCandidate(1);
    // in-flight decision
    let cause = await catchVerify(
      pipeline,
      createState(identity, "run-1", [
        ...DECISION_BOUND_PRELUDE,
        { kind: "start_decision_execution", stateId: "stage_dispatch", inputDigest: hex("e"), executionRole: "control" },
      ]),
      candidate,
    );
    expectAcceptanceError(cause, "invalid_state");
    // evaluated decision
    cause = await catchVerify(
      pipeline,
      createState(identity, "run-1", DECISION_UNBOUND_COMMANDS),
      candidate,
    );
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain("decision execution");
  });
});

test("12. an agent whose compiled role is stage is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", STAGE_ROLE_COMMANDS);
    const candidate = singleStageCandidate(4);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "origin_role_mismatch");
    expect((cause as Error).message).toBe(
      'the compiled execution role of state "coder" is "stage"; plan acceptance requires the "planning" role',
    );
  });
});

test("13. a waiting run is rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", WAITING_COMMANDS);
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, state, candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain('status "waiting"');
  });
});

test("14. publishing and finalized runs are rejected", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const candidate = singleStageCandidate(1);
    // terminal reached, still running: the terminal record itself rejects
    let cause = await catchVerify(pipeline, createState(identity, "run-1", PUBLISHING_COMMANDS), candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain("carries a terminal, run outputs or a failure");
    // published outputs switch the phase to publishing_outputs
    cause = await catchVerify(pipeline, createState(identity, "run-1", PUBLISHED_COMMANDS), candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain('phase "publishing_outputs"');
    // the finalized success run
    cause = await catchVerify(pipeline, createState(identity, "run-1", SUCCESS_COMMANDS), candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toContain('status "success"');
  });
});

test("15. every identity field mismatch is a pipeline_mismatch where reachable", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const candidate = singleStageCandidate(1);
    // comparator unit level: every one of the five fields
    const mismatches: string[] = [];
    for (const field of [
      "schema_version",
      "bundle_root",
      "execution_snapshot_sha256",
      "entry_state",
      "max_transitions",
    ] as const) {
      const broken = { ...identity } as Record<string, unknown>;
      if (field === "schema_version") {
        broken[field] = 3;
      } else if (field === "bundle_root") {
        broken[field] = "/opt/other-bundle";
      } else if (field === "execution_snapshot_sha256") {
        broken[field] = hex("9");
      } else if (field === "entry_state") {
        broken[field] = "elsewhere";
      } else {
        broken[field] = identity.max_transitions + 1;
      }
      const comparison: PipelineV2RunIdentityComparison = comparePipelineV2RunIdentity(
        identity,
        broken as unknown as PipelineV2RunPipelineIdentity,
      );
      expect(comparison.kind).toBe("mismatch");
      if (comparison.kind === "mismatch") {
        mismatches.push(comparison.field);
        expect(comparison.field).toBe(field);
      }
    }
    expect(mismatches).toEqual([
      "schema_version",
      "bundle_root",
      "execution_snapshot_sha256",
      "entry_state",
      "max_transitions",
    ]);
    // verifier level: the loader accepts these three mutations, so the
    // comparator's mismatch surfaces as pipeline_mismatch
    for (const mutate of [
      (broken: Record<string, unknown>) => {
        (broken.pipeline as Record<string, unknown>)["bundle_root"] = "/opt/other-bundle";
      },
      (broken: Record<string, unknown>) => {
        (broken.pipeline as Record<string, unknown>)["execution_snapshot_sha256"] = hex("9");
      },
      (broken: Record<string, unknown>) => {
        (broken.pipeline as Record<string, unknown>)["max_transitions"] = 99;
      },
    ]) {
      const broken = JSON.parse(JSON.stringify(createState(identity, "run-1", HAPPY_COMMANDS))) as Record<string, unknown>;
      mutate(broken);
      const cause = await catchVerify(pipeline, broken, candidate);
      expectAcceptanceError(cause, "pipeline_mismatch");
    }
    // entry-state and schema-version mismatches are caught earlier by the
    // state loader (cursor replay / identity schema check) as invalid_state
    const shifted = JSON.parse(JSON.stringify(createState(identity, "run-1", HAPPY_COMMANDS))) as Record<string, unknown>;
    (shifted["pipeline"] as Record<string, unknown>)["entry_state"] = "elsewhere";
    let cause = await catchVerify(pipeline, shifted, candidate);
    expectAcceptanceError(cause, "invalid_state");
    const versioned = JSON.parse(JSON.stringify(createState(identity, "run-1", HAPPY_COMMANDS))) as Record<string, unknown>;
    (versioned["pipeline"] as Record<string, unknown>)["schema_version"] = 3;
    cause = await catchVerify(pipeline, versioned, candidate);
    expectAcceptanceError(cause, "invalid_state");
  });
});

test("16. a malformed state document is invalid_state", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = singleStageCandidate(1);
    const cause = await catchVerify(pipeline, {}, candidate);
    expectAcceptanceError(cause, "invalid_state");
    expect((cause as Error).message).toBe(
      "verifyPipelineV2RunPlanCandidateForAcceptance requires a durable pipeline v2 run state document; " +
        "the argument is not a valid schema version 6 run state",
    );
    // a PipelineV2StateError is mapped, not propagated
    let mapped = false;
    try {
      validatePipelineV2RunState({});
    } catch (cause2) {
      mapped = cause2 instanceof PipelineV2StateError;
    }
    expect(mapped).toBe(true);
  });
});

test("17. the pipeline provenance gate runs first: zero state/candidate traps", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = singleStageCandidate(1);
    let stateTrapCalls = 0;
    const proxiedState = new Proxy(state, {
      get(target, property, receiver) {
        stateTrapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    let candidateTrapCalls = 0;
    const proxiedCandidate = new Proxy(candidate, {
      get(target, property, receiver) {
        candidateTrapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() =>
      verifyPipelineV2RunPlanCandidateForAcceptance(
        {} as unknown as ResolvedPipelineV2,
        proxiedState,
        proxiedCandidate,
      ),
    ).toThrow(PipelineError);
    expect(stateTrapCalls).toBe(0);
    expect(candidateTrapCalls).toBe(0);
    // getter-forged state objects are also never read
    let getterCalls = 0;
    const getterState = {
      get schema_version() {
        getterCalls += 1;
        return 6;
      },
      get run_id() {
        getterCalls += 1;
        return "run-1";
      },
    };
    expect(() =>
      verifyPipelineV2RunPlanCandidateForAcceptance(
        {} as unknown as ResolvedPipelineV2,
        getterState as PipelineV2RunState,
        candidate,
      ),
    ).toThrow(PipelineError);
    expect(getterCalls).toBe(0);
  });
});

test("18. a candidate provenance failure fires before the state is read", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const candidate = singleStageCandidate(1);
    const spread = { ...candidate };
    let cause = await catchVerify(pipeline, {}, spread as unknown as PreparedPipelineV2RunPlanCandidate);
    expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
    expect((cause as PipelineV2CompiledRunPlanError).reason).toBe("invalid_candidate");
    // a Proxy candidate with a malformed state: the candidate gate fires
    // first and the state is never validated
    let trapCalls = 0;
    const proxied = new Proxy(candidate, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    cause = await catchVerify(pipeline, {}, proxied);
    expect((cause as PipelineV2CompiledRunPlanError).reason).toBe("invalid_candidate");
    expect(trapCalls).toBe(0);
  });
});

test("19. an orchestration/template failure fires before the state is read", async () => {
  await withPipeline(
    async (pipeline) => {
      const candidate = singleStageCandidate(1);
      let caught: unknown;
      try {
        verifyPipelineV2RunPlanCandidateForAcceptance(
          pipeline,
          {} as PipelineV2RunState,
          candidate,
        );
      } catch (cause) {
        caught = cause;
      }
      expect(caught).toBeInstanceOf(PipelineV2OrchestrationError);
      expect((caught as Error).message).toContain(
        "the trusted pipeline declares no orchestration section",
      );
      expect(caught).not.toBeInstanceOf(PipelineV2RunPlanAcceptanceError);
    },
    ORCHESTRATED_YAML.replace(/orchestration:\n(?:[ ]+.*\n)+?\nstates:/, "states:"),
  );
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = preparedCandidate(
      [
        {
          id: "stage-1",
          template: "ghost_template",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ],
      [A1],
      1,
    );
    const cause = await catchVerify(pipeline, state, candidate);
    expect(cause).toBeInstanceOf(PipelineV2OrchestrationError);
    expect((cause as Error).message).toContain(
      'stage template "ghost_template" is not declared by the pipeline orchestration',
    );
  });
});

test("20. caller objects are neither mutated nor frozen", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const candidate = singleStageCandidate(1);
    const stateBefore = JSON.parse(JSON.stringify(state));
    const candidateBefore = JSON.parse(JSON.stringify(candidate));
    verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state, candidate);
    expect(JSON.parse(JSON.stringify(state))).toEqual(stateBefore);
    expect(JSON.parse(JSON.stringify(candidate))).toEqual(candidateBefore);
    expect(Object.isFrozen(state)).toBe(true); // frozen by the reducer, not by this layer
    const mutableState = JSON.parse(JSON.stringify(state)) as PipelineV2RunState;
    // a mutated copy is rejected by the state loader without freezing it
    expect(() =>
      verifyPipelineV2RunPlanCandidateForAcceptance(
        pipeline,
        { ...mutableState, status: "failed" } as PipelineV2RunState,
        candidate,
      ),
    ).toThrow(PipelineV2RunPlanAcceptanceError);
    expect(Object.isFrozen({ ...mutableState })).toBe(false);
  });
});

test("21. no canary task body in the result or in diagnostics", async () => {
  await withPipeline(async (pipeline) => {
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const canaryTask = preparedTask("task-a", 1, null, "planning_proposal", `secret body with ${CANARY}`);
    const candidate = preparePipelineV2RunPlanCandidate({
      plan: preparePlanRevisionManifest({
        schema_version: 1,
        kind: "plan_revision",
        run_id: "run-1",
        revision: 1,
        previous_sha256: null,
        root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
        origin_execution: 1,
        stages: [
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 1, sha256: canaryTask.sha256, depends_on: [] }],
          },
        ],
      }),
      taskRevisions: [canaryTask],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const state = createState(identity, "run-1", HAPPY_COMMANDS);
    const compiled = verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, state, candidate);
    expect(JSON.stringify(compiled)).not.toContain(CANARY);
    // a rejection diagnostic carries no body either
    const stageRoleState = createState(identity, "run-1", STAGE_ROLE_COMMANDS);
    let message = "";
    try {
      verifyPipelineV2RunPlanCandidateForAcceptance(pipeline, stageRoleState, candidate);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).not.toContain(CANARY);
  });
});

test("22. the runtime export surface is exactly the two keys", () => {
  expect(Object.keys(acceptanceModule).sort()).toEqual([
    "PipelineV2RunPlanAcceptanceError",
    "verifyPipelineV2RunPlanCandidateForAcceptance",
  ]);
});

test("23. source proof: no own replay, role inference, registry, message parsing or filesystem", () => {
  const source = readFileSync(
    join(import.meta.dir, "..", "src", "pipeline_v2_run_plan_acceptance.ts"),
    "utf8",
  );
  const importTargets = [...source.matchAll(/from "\.\/([^"]+)"/g)].map((match) => match[1] ?? "");
  const allowed = [
    "pipeline_v2_state.ts",
    "pipeline_v2.ts",
    "pipeline_v2_digest.ts",
    "pipeline_v2_identity_compare.ts",
    "pipeline_v2_orchestration.ts",
    "pipeline_v2_run_plan_compiled.ts",
    "pipeline_v2_run_plan_candidate.ts",
  ];
  expect(importTargets.length).toBeGreaterThan(0);
  for (const target of importTargets) {
    expect(allowed).toContain(target);
  }
  const forbidden = [
    "pipeline_v2_state_store",
    "pipeline_v2_state_sink",
    "run_snapshot_store",
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "pipeline_v2_resume_context",
    "pipeline_v2_wait",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_run_plan_manifests",
    "pipeline_v2_run_plan_bindings",
    "pipeline_v2_run_plan_candidate_internal",
    "pipeline_v2_docker",
    "pipeline_v2_runtime",
    "agent_smoke",
    "docker_helper",
    "launcher",
    "main",
    "cli",
  ];
  for (const forbiddenModule of forbidden) {
    expect(source.includes(`from "./${forbiddenModule}.ts"`)).toBe(false);
  }
  // no own cursor replay, no role inference, no second registry, no
  // message parsing, no filesystem
  expect(source).not.toContain("compiledTransitionFor");
  expect(source).not.toContain("reducePipelineV2RunCommand");
  expect(source).not.toContain("checkGraphShape");
  expect(source).not.toContain("canonicalJson");
  expect(source).not.toContain("CryptoHasher");
  expect(source).not.toContain(".profile");
  expect(source).not.toContain("promptContent");
  expect(source).not.toContain("node:fs");
  expect(source).not.toContain("node:path");
  expect(source).not.toContain(".message.includes");
  expect(source).not.toContain(".message.indexOf");
  expect(source).not.toContain("describeError");
  expect(source).not.toContain("WeakSet");
  expect(source).not.toContain("WeakMap");
});
