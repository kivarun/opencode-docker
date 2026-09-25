import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  PipelineV2OrchestrationError,
  compiledStageTemplateFor,
} from "../src/pipeline_v2_orchestration.ts";
import { PipelineError } from "../src/pipeline.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  type PreparedPipelineV2RunPlanRevision,
  type PreparedPipelineV2RunTaskRevision,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import {
  PipelineV2CompiledRunPlanError,
  compilePipelineV2RunPlanCandidate,
  compiledPipelineV2RunPlanStageFor,
  type CompiledPipelineV2RunPlan,
  type CompiledPipelineV2RunPlanStage,
} from "../src/pipeline_v2_run_plan_compiled.ts";
import * as compiledModule from "../src/pipeline_v2_run_plan_compiled.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  compiledRunPlanOriginIdentity,
  hasCompiledRunPlanProvenance,
} from "../src/pipeline_v2_run_plan_compiled_internal.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");
const CANARY = "CANARY_secret_task_body";

const FACTS_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const DISPATCH_MODEL_YAML = `schema_version: 1
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

const NO_ORCHESTRATION_YAML = TWO_TEMPLATES_YAML.replace(
  /orchestration:\n(?:[ ]+.*\n)+?\nstates:/,
  "states:",
);

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-run-plan-compiled-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  return { root, bundle };
}

async function withPipeline(
  fn: (pipeline: ResolvedPipelineV2, dirs: BundleDirs) => Promise<void>,
  yaml: string = TWO_TEMPLATES_YAML,
): Promise<void> {
  const dirs = await makeBundleDirs();
  try {
    await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
    await writeFile(join(dirs.bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(dirs.bundle, "decisions", "dispatch.yaml"), DISPATCH_MODEL_YAML);
    await writeFile(join(dirs.bundle, "decisions", "gate.yaml"), GATE_MODEL_YAML);
    await fn(await loadPipelineV2(dirs.bundle), dirs);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

function preparedTask(
  taskId: string,
  revision: number,
  previousSha256: string | null,
  origin: string,
  body: string,
): PreparedPipelineV2RunTaskRevision {
  return prepareTaskRevisionManifest({
    schema_version: 1,
    kind: "task_revision",
    run_id: RUN_ID,
    task_id: taskId,
    revision,
    previous_sha256: previousSha256,
    origin,
    body,
  });
}

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

function preparedPlan(stages: readonly StageSpec[]): PreparedPipelineV2RunPlanRevision {
  return preparePlanRevisionManifest({
    schema_version: 1,
    kind: "plan_revision",
    run_id: RUN_ID,
    revision: 1,
    previous_sha256: null,
    root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
    origin_execution: 10,
    stages,
  });
}

function candidateOf(
  plan: PreparedPipelineV2RunPlanRevision,
  taskRevisions: readonly PreparedPipelineV2RunTaskRevision[],
): PreparedPipelineV2RunPlanCandidate {
  return preparePipelineV2RunPlanCandidate({
    plan,
    taskRevisions,
    previousPlan: null,
    previousTaskRevisions: [],
    protectedInputDigest: PROTECTED_DIGEST,
  });
}

const A1 = preparedTask("task-a", 1, null, "planning_proposal", "Body A one");
const B1 = preparedTask("task-b", 1, null, "planning_proposal", "Body B one");
const C1 = preparedTask("task-c", 1, null, "planning_proposal", "Body C one");
const D1 = preparedTask("task-d", 1, null, "planning_proposal", "Body D one");
const E1 = preparedTask("task-e", 1, null, "planning_proposal", "Body E one");

function assertFullyFrozen(value: unknown, path: string): void {
  if (value === null || typeof value !== "object") {
    return;
  }
  if (!Object.isFrozen(value)) {
    throw new Error(`not frozen: ${path}`);
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertFullyFrozen(item, `${path}[${index}]`));
    return;
  }
  for (const key of Object.keys(value)) {
    assertFullyFrozen((value as Record<string, unknown>)[key], `${path}.${key}`);
  }
}

test("1. one stage/template yields the exact content-free projection form", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ]),
      [A1, B1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(compiled).toEqual({
      run_id: "run-1",
      plan_revision: 1,
      plan_sha256: candidate.plan.sha256,
      origin_execution: 10,
      stages: [
        {
          id: "stage-1",
          template: "development",
          entry_state: "development_entry",
          state_ids: ["dev_agent", "dev_gate", "development_entry"],
          tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ],
    });
    // exact key sets at every level, content-free
    expect(Object.keys(compiled)).toEqual([
      "run_id",
      "plan_revision",
      "plan_sha256",
      "origin_execution",
      "stages",
    ]);
    const stage = compiled.stages[0] as CompiledPipelineV2RunPlanStage;
    expect(Object.keys(stage)).toEqual(["id", "template", "entry_state", "state_ids", "tasks"]);
    expect(Object.keys(stage.tasks[0] as unknown as Record<string, unknown>)).toEqual([
      "id",
      "revision",
      "sha256",
      "depends_on",
    ]);
  });
});

test("2. several stages and templates compile into per-stage projections", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
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
      ]),
      [A1, C1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(compiled.stages.map((stage) => stage.template)).toEqual(["development", "testing"]);
    expect(compiled.stages.map((stage) => stage.entry_state)).toEqual([
      "development_entry",
      "testing_entry",
    ]);
  });
});

test("3. one template may be reused by several plan stages", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
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
        {
          id: "stage-3",
          template: "development",
          tasks: [{ id: "task-e", revision: 1, sha256: E1.sha256, depends_on: [] }],
        },
      ]),
      [A1, C1, E1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(compiled.stages.map((stage) => stage.id)).toEqual(["stage-1", "stage-2", "stage-3"]);
    expect(compiled.stages[0]?.entry_state).toBe("development_entry");
    expect(compiled.stages[2]?.entry_state).toBe("development_entry");
    expect(compiled.stages[0]?.state_ids).toEqual(compiled.stages[2]?.state_ids);
  });
});

test("4. the plan's semantic stage declaration order is preserved", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-2",
          template: "testing",
          tasks: [{ id: "task-c", revision: 1, sha256: C1.sha256, depends_on: [] }],
        },
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1, C1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(compiled.stages.map((stage) => stage.id)).toEqual(["stage-2", "stage-1"]);
  });
});

test("5. the normalized task and dependency order is preserved as prepared", async () => {
  await withPipeline(async (pipeline) => {
    const plan = preparedPlan([
      {
        id: "stage-1",
        template: "development",
        tasks: [
          // declaration order and unsorted depends_on; the manifest
          // preparer normalizes both
          { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-d", "task-a"] },
          { id: "task-d", revision: 1, sha256: D1.sha256, depends_on: [] },
          { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
        ],
      },
    ]);
    const candidate = candidateOf(plan, [A1, B1, D1]);
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    const manifestStage = candidate.plan.manifest.stages[0];
    if (manifestStage === undefined) {
      throw new Error("missing manifest stage");
    }
    const compiledStage = compiled.stages[0];
    if (compiledStage === undefined) {
      throw new Error("missing compiled stage");
    }
    expect(compiledStage.tasks.map((task) => task.id)).toEqual(
      manifestStage.tasks.map((pointer) => pointer.id),
    );
    expect(compiledStage.tasks.map((task) => task.id)).toEqual([
      "task-a",
      "task-b",
      "task-d",
    ]);
    const taskB = compiledStage.tasks.find((task) => task.id === "task-b");
    const pointerB = manifestStage.tasks.find((pointer) => pointer.id === "task-b");
    if (taskB === undefined || pointerB === undefined) {
      throw new Error("missing task b");
    }
    expect(taskB.depends_on).toEqual(pointerB.depends_on);
    expect(taskB.depends_on).toEqual(["task-a", "task-d"]);
  });
});

test("6. entry_state and state_ids come exactly from the trusted orchestration resolver", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
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
      ]),
      [A1, C1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    for (const stage of compiled.stages) {
      const resolved = compiledStageTemplateFor(pipeline, stage.template);
      expect(stage.entry_state).toBe(resolved.entry_state);
      expect(stage.state_ids).toEqual(resolved.state_ids);
      expect(stage.state_ids).toEqual([...resolved.state_ids]);
    }
  });
});

test("7. the projection is deeply frozen", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ]),
      [A1, B1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    assertFullyFrozen(compiled, "compiled");
  });
});

test("8. mutation isolation: no aliasing of pipeline or candidate arrays", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ]),
      [A1, B1],
    );
    const candidateBefore = JSON.parse(JSON.stringify(candidate));
    const pipelineBefore = JSON.parse(JSON.stringify(pipeline));
    const resolved = compiledStageTemplateFor(pipeline, "development");
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    const manifestStage = candidate.plan.manifest.stages[0];
    const compiledStage = compiled.stages[0];
    if (manifestStage === undefined || compiledStage === undefined) {
      throw new Error("missing stage");
    }
    // fresh arrays, never the manifest's or the resolver's array objects
    expect(compiledStage.tasks).not.toBe(manifestStage.tasks);
    expect(compiledStage.state_ids).not.toBe(resolved.state_ids);
    for (let index = 0; index < compiledStage.tasks.length; index++) {
      const task = compiledStage.tasks[index];
      const pointer = manifestStage.tasks[index];
      if (task === undefined || pointer === undefined) {
        throw new Error(`missing task at ${index}`);
      }
      expect(task.depends_on).not.toBe(pointer.depends_on);
    }
    // the trusted inputs are untouched
    expect(JSON.parse(JSON.stringify(candidate))).toEqual(candidateBefore);
    expect(JSON.parse(JSON.stringify(pipeline))).toEqual(pipelineBefore);
  });
});

test("9. the projection carries no task bodies, canonical JSON or filesystem paths", async () => {
  await withPipeline(async (pipeline, dirs) => {
    const canaryTask = preparedTask("task-a", 1, null, "planning_proposal", `secret body with ${CANARY}`);
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [
            { id: "task-a", revision: 1, sha256: canaryTask.sha256, depends_on: [] },
            { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
          ],
        },
      ]),
      [canaryTask, B1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    const json = JSON.stringify(compiled);
    expect(json).not.toContain(CANARY);
    expect(json).not.toContain("secret body");
    expect(json).not.toContain("canonical_json");
    expect(json).not.toContain('"body"');
    expect(json).not.toContain(dirs.root);
    expect(json).not.toContain("/");
  });
});

const PIPELINE_UNTRUSTED_MESSAGE =
  "compilePipelineV2RunPlanCandidate requires the deep-frozen snapshot object returned by loadPipelineV2; " +
  "hand-built objects, casts, clones and Proxies are rejected before any content is read";

test("10. pipeline provenance is checked before the candidate is read", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    // a forged pipeline is rejected first, before any candidate read
    expect(() =>
      compilePipelineV2RunPlanCandidate({} as unknown as ResolvedPipelineV2, candidate),
    ).toThrow(PipelineError);
    expect(() =>
      compilePipelineV2RunPlanCandidate({} as unknown as ResolvedPipelineV2, candidate),
    ).toThrow(PIPELINE_UNTRUSTED_MESSAGE);

    // a Proxy candidate: with a forged pipeline its traps must stay at zero,
    // proving the pipeline gate ran first
    let trapCalls = 0;
    const proxiedCandidate = new Proxy(candidate, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() =>
      compilePipelineV2RunPlanCandidate({} as unknown as ResolvedPipelineV2, proxiedCandidate),
    ).toThrow(PIPELINE_UNTRUSTED_MESSAGE);
    expect(trapCalls).toBe(0);

    // the trusted pipeline still resolves the trusted candidate
    expect(compilePipelineV2RunPlanCandidate(pipeline, candidate).stages.length).toBe(1);
  });
});

test("11. candidate provenance battery: hand-built, spread, clone, prototype, Proxy", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    const reject = (untrusted: unknown): void => {
      let reason = "";
      try {
        compilePipelineV2RunPlanCandidate(pipeline, untrusted as PreparedPipelineV2RunPlanCandidate);
      } catch (cause) {
        expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        reason = (cause as PipelineV2CompiledRunPlanError).reason;
      }
      expect(reason).toBe("invalid_candidate");
    };
    reject({});
    reject(null);
    reject([]);
    // hand-built lookalike with the right shape
    reject({ plan: candidate.plan, task_revisions: candidate.task_revisions });
    // shallow spread of the real candidate
    reject({ ...candidate });
    // deep clone with fresh identities
    reject(structuredClone(candidate));
    // prototype-derived wrapper
    reject(Object.create(candidate));
  });
});

test("12. a Proxy over the real candidate is rejected with zero traps", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    let trapCalls = 0;
    const proxied = new Proxy(candidate, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() => compilePipelineV2RunPlanCandidate(pipeline, proxied)).toThrow(
      PipelineV2CompiledRunPlanError,
    );
    expect(trapCalls).toBe(0);
  });
});

test("13. a pipeline without orchestration propagates the typed orchestration error", async () => {
  await withPipeline(
    async (pipeline) => {
      const candidate = candidateOf(
        preparedPlan([
          {
            id: "stage-1",
            template: "development",
            tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
          },
        ]),
        [A1],
      );
      let caught: unknown;
      try {
        compilePipelineV2RunPlanCandidate(pipeline, candidate);
      } catch (cause) {
        caught = cause;
      }
      expect(caught).toBeInstanceOf(PipelineV2OrchestrationError);
      expect((caught as Error).message).toContain(
        "the trusted pipeline declares no orchestration section",
      );
      expect(caught).not.toBeInstanceOf(PipelineV2CompiledRunPlanError);
    },
    NO_ORCHESTRATION_YAML,
  );
});

test("14. a plan stage with an unknown template propagates the typed orchestration error", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "ghost_template",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    let caught: unknown;
    try {
      compilePipelineV2RunPlanCandidate(pipeline, candidate);
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(PipelineV2OrchestrationError);
    expect((caught as Error).message).toContain(
      'stage template "ghost_template" is not declared by the pipeline orchestration',
    );
    expect(caught).not.toBeInstanceOf(PipelineV2CompiledRunPlanError);
  });
});

test("15. the lookup returns the exact frozen stage of the projection", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
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
      ]),
      [A1, C1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    const stageOne = compiled.stages[0];
    const stageTwo = compiled.stages[1];
    if (stageOne === undefined || stageTwo === undefined) {
      throw new Error("missing stages");
    }
    expect(compiledPipelineV2RunPlanStageFor(compiled, "stage-1")).toBe(stageOne);
    expect(compiledPipelineV2RunPlanStageFor(compiled, "stage-2")).toBe(stageTwo);
    expect(compiledPipelineV2RunPlanStageFor(compiled, "stage-1")).toEqual(stageOne);
  });
});

test("16. the lookup of an unknown stage is a typed content-free error", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    let caught: unknown;
    try {
      compiledPipelineV2RunPlanStageFor(compiled, "ghost-stage");
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(PipelineV2CompiledRunPlanError);
    expect((caught as PipelineV2CompiledRunPlanError).reason).toBe("stage_not_found");
    expect((caught as Error).message).toBe(
      'run plan stage "ghost-stage" is not declared by the compiled plan',
    );
  });
});

test("17. the lookup rejects an invalid stage id without echoing it", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    for (const badId of ["bad id!", "", "a..b", "../escape"]) {
      let caught: unknown;
      try {
        compiledPipelineV2RunPlanStageFor(compiled, badId);
      } catch (cause) {
        caught = cause;
      }
      expect(caught).toBeInstanceOf(PipelineV2CompiledRunPlanError);
      expect((caught as PipelineV2CompiledRunPlanError).reason).toBe("invalid_stage_id");
      expect((caught as Error).message).toBe(
        "compiledPipelineV2RunPlanStageFor requires a safe stage id",
      );
      if (badId !== "") {
        expect((caught as Error).message).not.toContain(badId);
      }
    }
  });
});

const UNTRUSTED_COMPILED_PLAN_MESSAGE =
  "compiledPipelineV2RunPlanStageFor requires the frozen compiled run plan object " +
  "returned by compilePipelineV2RunPlanCandidate; hand-built objects, casts, clones and " +
  "Proxies are rejected before any field is read";

test("18. compiled-plan provenance battery, Proxy with zero traps", async () => {
  await withPipeline(async (pipeline) => {
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ]),
      [A1],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    const reject = (untrusted: unknown): void => {
      let reason = "";
      try {
        compiledPipelineV2RunPlanStageFor(
          untrusted as CompiledPipelineV2RunPlan,
          "stage-1",
        );
      } catch (cause) {
        expect(cause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
        reason = (cause as PipelineV2CompiledRunPlanError).reason;
      }
      expect(reason).toBe("invalid_plan");
      expect(cause_message_of(untrusted)).toBe(UNTRUSTED_COMPILED_PLAN_MESSAGE);
    };
    const cause_message_of = (untrusted: unknown): string => {
      try {
        compiledPipelineV2RunPlanStageFor(untrusted as CompiledPipelineV2RunPlan, "stage-1");
      } catch (cause) {
        return (cause as Error).message;
      }
      return "";
    };
    reject({});
    reject(null);
    reject([]);
    reject({ run_id: "run-1", plan_revision: 1, plan_sha256: "x", origin_execution: 1, stages: [] });
    reject({ ...compiled });
    reject(structuredClone(compiled));
    reject(Object.create(compiled));
    let trapCalls = 0;
    const proxied = new Proxy(compiled, {
      get(target, property, receiver) {
        trapCalls += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    expect(() => compiledPipelineV2RunPlanStageFor(proxied, "stage-1")).toThrow(
      UNTRUSTED_COMPILED_PLAN_MESSAGE,
    );
    expect(trapCalls).toBe(0);
    // the trusted compiled plan still resolves
    expect(compiledPipelineV2RunPlanStageFor(compiled, "stage-1").id).toBe("stage-1");
  });
});

test("19. identical normalized inputs compile byte-identical projections", async () => {
  await withPipeline(async (pipeline) => {
    const build = (): PreparedPipelineV2RunPlanCandidate =>
      candidateOf(
        preparedPlan([
          {
            id: "stage-1",
            template: "development",
            tasks: [
              { id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] },
              { id: "task-b", revision: 1, sha256: B1.sha256, depends_on: ["task-a"] },
            ],
          },
          {
            id: "stage-2",
            template: "testing",
            tasks: [{ id: "task-c", revision: 1, sha256: C1.sha256, depends_on: [] }],
          },
        ]),
        [A1, B1, C1],
      );
    const first = compilePipelineV2RunPlanCandidate(pipeline, build());
    const second = compilePipelineV2RunPlanCandidate(pipeline, build());
    expect(JSON.stringify(second)).toBe(JSON.stringify(first));
    expect(second).toEqual(first);
    expect(second).not.toBe(first);
  });
});

test("20. no canary task body in the result or in any diagnostic", async () => {
  await withPipeline(async (pipeline) => {
    const canaryTask = preparedTask("task-a", 1, null, "planning_proposal", `secret body with ${CANARY}`);
    const candidate = candidateOf(
      preparedPlan([
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: canaryTask.sha256, depends_on: [] }],
        },
      ]),
      [canaryTask],
    );
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidate);
    expect(JSON.stringify(compiled)).not.toContain(CANARY);
    // diagnostics: an untrusted candidate spread from the real one carries
    // the canary, but the rejection message never echoes it
    const spread = { ...candidate };
    let message = "";
    try {
      compilePipelineV2RunPlanCandidate(pipeline, spread as PreparedPipelineV2RunPlanCandidate);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain("requires the frozen prepared run plan candidate");
    expect(message).not.toContain(CANARY);
    // and the stage-not-found diagnostic is content-free too
    try {
      compiledPipelineV2RunPlanStageFor(compiled, "ghost-stage");
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).not.toContain(CANARY);
  });
});

test("21. the runtime export surface is exactly the three keys", () => {
  expect(Object.keys(compiledModule).sort()).toEqual([
    "PipelineV2CompiledRunPlanError",
    "compilePipelineV2RunPlanCandidate",
    "compiledPipelineV2RunPlanStageFor",
  ]);
});

test("22. source scan: only pure allowed imports, no second compiler or message parsing", () => {
  const source = readFileSync(
    join(import.meta.dir, "..", "src", "pipeline_v2_run_plan_compiled.ts"),
    "utf8",
  );
  const importTargets = [...source.matchAll(/from "\.\/([^"]+)"/g)].map((match) => match[1] ?? "");
  expect(importTargets.length).toBeGreaterThan(0);
  const allowed = [
    "pipeline_v2_scalar.ts",
    "pipeline_v2.ts",
    "pipeline_v2_orchestration.ts",
    "pipeline_v2_run_plan_candidate_internal.ts",
    "pipeline_v2_run_plan_candidate.ts",
    "pipeline_v2_digest.ts",
    "pipeline_v2_run_plan_compiled_internal.ts",
  ];
  for (const target of importTargets) {
    expect(allowed).toContain(target);
  }
  const forbidden = [
    "pipeline_v2_state",
    "pipeline_state",
    "pipeline_v2_coordinator",
    "pipeline_v2_runner",
    "pipeline_v2_wait",
    "pipeline_v2_runtime",
    "pipeline_v2_run_plan_store",
    "pipeline_v2_run_plan_manifests",
    "pipeline_v2_run_plan_bindings",
    "pipeline_v2_run_plan_provenance",
    "pipeline_v2_immutable_document_store_internal",
    "pipeline_v2_freeze_internal",
    "pipeline_v2_docker",
    "pipeline_v2_resume",
    "run_snapshot_store",
    "bundle_file",
    "agent_smoke",
    "docker_helper",
    "launcher",
    "profile",
    "main",
    "cli",
  ];
  for (const forbiddenModule of forbidden) {
    expect(source.includes(`from "./${forbiddenModule}.ts"`)).toBe(false);
  }
  // no second graph compiler, no serializer/digest builder, no filesystem
  expect(source).not.toContain("checkGraphShape");
  expect(source).not.toContain("canonicalJson");
  expect(source).not.toContain("CryptoHasher");
  expect(source).not.toContain("Bun.YAML");
  expect(source).not.toContain("requireBundleFileInsideRoot");
  expect(source).not.toContain("readBundleFile");
  expect(source).not.toContain("node:fs");
  expect(source).not.toContain("node:path");
  // no message-text classification
  expect(source).not.toContain(".message.includes");
  expect(source).not.toContain(".message.indexOf");
  expect(source).not.toContain("instanceof PipelineError ? cause.message");
  expect(source).not.toContain("describeError");
});

const SINGLE_STAGE = [
  {
    id: "stage-1",
    template: "development",
    tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
  },
];

function catchLookup(fn: () => unknown): unknown {
  try {
    return fn();
  } catch (cause) {
    return cause;
  }
}

test("23. the projection shape and byte representation carry no hidden identity", async () => {
  await withPipeline(async (pipeline) => {
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidateOf(preparedPlan(SINGLE_STAGE), [A1]));
    expect(Object.keys(compiled).sort()).toEqual([
      "origin_execution",
      "plan_revision",
      "plan_sha256",
      "run_id",
      "stages",
    ]);
    const json = JSON.stringify(compiled);
    for (const bannedKey of [
      "bundle_root",
      "execution_snapshot",
      "max_transitions",
      "schema_version",
      '"pipeline"',
    ]) {
      expect(json.includes(bannedKey)).toBe(false);
    }
  });
});

test("24. the registry binds the exact immutable originating identity, built by the existing construction point", async () => {
  await withPipeline(async (pipeline) => {
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidateOf(preparedPlan(SINGLE_STAGE), [A1]));
    const stored = compiledRunPlanOriginIdentity(compiled);
    const fresh = pipelineV2RunPipelineIdentity(pipeline);
    expect(Object.isFrozen(stored)).toBe(true);
    // the stored snapshot is its own object, never an alias of a caller object
    expect(stored).not.toBe(fresh);
    expect(stored).not.toBe(pipeline);
    expect(stored).toEqual(fresh);
    expect(stored).toEqual(pipelineV2RunPipelineIdentity(pipeline));
  });
});

test("25. mutation of the pipeline and of the stored identity is impossible after the compile", async () => {
  await withPipeline(async (pipeline) => {
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidateOf(preparedPlan(SINGLE_STAGE), [A1]));
    const stored = compiledRunPlanOriginIdentity(compiled);
    expect(() => {
      (stored as unknown as Record<string, unknown>).execution_snapshot_sha256 = hex("0");
    }).toThrow(TypeError);
    expect(stored.execution_snapshot_sha256).toBe(pipelineV2RunPipelineIdentity(pipeline).execution_snapshot_sha256);
    expect(() => {
      (pipeline as unknown as Record<string, unknown>).entry_state = "architect2";
    }).toThrow(TypeError);
    expect(compiledRunPlanOriginIdentity(compiled)).toEqual(pipelineV2RunPipelineIdentity(pipeline));
  });
});

test("26. hand-built, spread, cloned and Proxy lookalikes have no registry provenance", async () => {
  await withPipeline(async (pipeline) => {
    const compiled = compilePipelineV2RunPlanCandidate(pipeline, candidateOf(preparedPlan(SINGLE_STAGE), [A1]));
    const handBuilt = JSON.parse(JSON.stringify(compiled)) as CompiledPipelineV2RunPlan;
    const spread = { ...compiled } as unknown as CompiledPipelineV2RunPlan;
    const proxy = new Proxy(compiled, {});
    for (const lookalike of [handBuilt, spread, proxy]) {
      expect(hasCompiledRunPlanProvenance(lookalike)).toBe(false);
    }
    expect(hasCompiledRunPlanProvenance(compiled)).toBe(true);
    const spreadCause = catchLookup(() => compiledPipelineV2RunPlanStageFor(spread, "stage-1"));
    expect(spreadCause).toBeInstanceOf(PipelineV2CompiledRunPlanError);
    expect((spreadCause as PipelineV2CompiledRunPlanError).reason).toBe("invalid_plan");
  });
});

test("27. the internal registry runtime surface is exactly the three keys", async () => {
  const registry = (await import("../src/pipeline_v2_run_plan_compiled_internal.ts")) as Record<string, unknown>;
  expect(Object.keys(registry).sort()).toEqual([
    "compiledRunPlanOriginIdentity",
    "hasCompiledRunPlanProvenance",
    "registerCompiledRunPlanIdentity",
  ]);
});
