import { describe, expect, test } from "bun:test";
import { lstat, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
  prepareWaitIntent,
  type PreparedPipelineV2RunWaitIntent,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import {
  preparePipelineV2RunPlanCandidate,
  type PreparedPipelineV2RunPlanCandidate,
} from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import { acceptPipelineV2ContinueStageIntent } from "../src/pipeline_v2_continue_stage_intent_controller.ts";
import {
  applyPipelineV2ContinueStageGrant,
  PipelineV2ContinueStageGrantControllerError,
  type PipelineV2ContinueStageGrantControllerFailureReason,
  type PipelineV2ContinueStageGrantControllerSink,
} from "../src/pipeline_v2_continue_stage_grant_controller.ts";
import {
  applyPipelineV2ContinueStageGrantInternal,
  planContinueStageGrant,
  precheckGrantSequence,
  requireContinueStageGrantBindings,
} from "../src/pipeline_v2_continue_stage_grant_controller_internal.ts";
import { faultIo } from "./state_io_test_helpers.ts";

const RUN_ID = "run-1";
const hex = (char: string): string => char.repeat(64);
const PROTECTED_DIGEST = hex("b");

const DISPATCH_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: d_next_stage
relations: []
constraints: []
rules:
  - id: r_next
    when:
      fact: f1
      equals: true
    decision: d_next_stage
`;

const STAGE_YAML = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
    - state_id: dev_entry
      role: stage
      stage_template: development

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
        to: dev_entry

  - id: dev_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: done
    type: terminal
    result: success
`;

async function withPipeline<T>(fn: (pipeline: ResolvedPipelineV2) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-grant-"));
  try {
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "decisions"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), STAGE_YAML);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "decisions", "dispatch.yaml"), DISPATCH_MODEL_YAML);
    return await fn(await loadPipelineV2(bundle));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface RunFixture {
  root: string;
  stateRoot: string;
  runRoot: string;
  statePath: string;
}

async function setupRun(): Promise<RunFixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-continue-grant-run-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { mode: 0o700 });
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  return { root, stateRoot, runRoot, statePath: join(runRoot, "state.json") };
}

async function disposeRun(fixture: RunFixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

let clockCounter = 0;
function nextTick(): Date {
  clockCounter += 1;
  return new Date(Date.UTC(2026, 8, 26, 0, 0, clockCounter));
}

const BASE_INPUTS = [{ id: "task", type: "file" as const, protected: true, digest: PROTECTED_DIGEST }];

let sessionCounter = 0;

function agentPhases(label: string): PipelineV2RunCommand[] {
  sessionCounter += 1;
  const n = sessionCounter;
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${n}-${label}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${n}-${label}` },
    { kind: "agent_running" },
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }] },
    { kind: "agent_cleanup_completed" },
  ];
}

const A1 = prepareTaskRevisionManifest({
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN_ID,
  task_id: "task-a",
  revision: 1,
  previous_sha256: null,
  origin: "planning_proposal",
  body: "Body A",
});

interface GrantCtx {
  fixture: RunFixture;
  sink: PipelineV2RunStateSink;
  pipeline: ResolvedPipelineV2;
  intent: PreparedPipelineV2RunWaitIntent;
}

interface GrantReadyOptions {
  acceptIntent?: boolean;
  actions?: Array<{ id: string; to: string }>;
  recordGrant?: { additionalIterations: number; intentSha256?: string };
  closeGrantIteration?: boolean;
  enterWait?: boolean;
}

/**
 * A real sink driven through the real reducer and the existing controllers
 * to the grant-ready boundary: the intent accepted durably inside the open
 * wait, the open generation with its open iteration.
 */
async function grantReady(options: GrantReadyOptions = {}): Promise<GrantCtx> {
  return await withPipeline(async (pipeline) => {
    const fixture = await setupRun();
    const sink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId: RUN_ID, now: nextTick });
    await sink.dispatch({ kind: "create_run", runId: RUN_ID, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: BASE_INPUTS });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" });
    for (const command of agentPhases("planning")) {
      await sink.dispatch(command);
    }
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: PROTECTED_DIGEST },
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: A1.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate: PreparedPipelineV2RunPlanCandidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [A1],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: PROTECTED_DIGEST,
    });
    const accepted = await acceptPipelineV2RunPlanCandidate({ pipeline, runRoot: fixture.runRoot, sink, candidate });
    await ensurePipelineV2StageIteration({ compiledPlan: accepted.compiled_plan, stageId: "stage-1", initialBudget: 2, sink });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await sink.dispatch({ kind: "start_agent_execution", stateId: "dev_entry", profile: "coder", executionRole: "stage", iterationIndex: 1 });
    for (const command of agentPhases("stage")) {
      await sink.dispatch(command);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "dev_entry", outcome: "completed", to: "architect", transition_index: 0 },
      executionIndex: 2,
    });
    const actions = options.actions ?? [
      { id: "continue_stage", to: "dev_entry" },
      { id: "revise_task", to: "architect" },
    ];
    if (options.enterWait !== false) {
      await sink.dispatch({
        kind: "run_waiting",
        stateId: "architect",
        reason: "stage_iteration_limit_exhausted",
        requestSha256: hex("1"),
        actions,
      });
    }
    const intent = prepareWaitIntent({
      schema_version: 1,
      kind: "continue_stage_intent",
      run_id: RUN_ID,
      wait_index: 1,
      stage_id: "stage-1",
      expected_plan_sha256: accepted.compiled_plan.plan_sha256,
      additional_iterations: 2,
    });
    if (options.acceptIntent !== false) {
      await acceptPipelineV2ContinueStageIntent({ runRoot: fixture.runRoot, sink, intent });
    }
    if (options.recordGrant !== undefined) {
      await sink.dispatch({
        kind: "iteration_grant_recorded",
        generationIndex: 1,
        waitIndex: 1,
        intentSha256: options.recordGrant.intentSha256 ?? intent.sha256,
        additionalIterations: options.recordGrant.additionalIterations,
      });
    }
    if (options.closeGrantIteration === true) {
      await sink.dispatch({ kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 });
    }
    return { fixture, sink, pipeline, intent };
  });
}

interface RecordingSink extends PipelineV2ContinueStageGrantControllerSink {
  commands: PipelineV2RunCommand[];
}

function recordingSink(inner: PipelineV2RunStateSink): RecordingSink {
  const commands: PipelineV2RunCommand[] = [];
  return {
    commands,
    get snapshot() {
      return inner.snapshot;
    },
    get poisoned() {
      return inner.poisoned;
    },
    async dispatch(command: PipelineV2RunCommand) {
      commands.push(command);
      await inner.dispatch(command);
    },
  };
}

async function catchAccept(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (cause) {
    return cause;
  }
  throw new Error("the call was expected to fail");
}

function expectGrantError(cause: unknown, reason: PipelineV2ContinueStageGrantControllerFailureReason): PipelineV2ContinueStageGrantControllerError {
  expect(cause).toBeInstanceOf(PipelineV2ContinueStageGrantControllerError);
  const error = cause as PipelineV2ContinueStageGrantControllerError;
  expect(error.reason).toBe(reason);
  return error;
}

describe("applyPipelineV2ContinueStageGrant", () => {
  test("1. happy path through the real reducer: exact grant and exact closure", async () => {
    const ctx = await grantReady();
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([
        {
          kind: "iteration_grant_recorded",
          generationIndex: 1,
          waitIndex: 1,
          intentSha256: ctx.intent.sha256,
          additionalIterations: 2,
        },
        {
          kind: "stage_iteration_closed",
          generationIndex: 1,
          iterationIndex: 1,
          by: "grant",
          waitIndex: 1,
        },
      ]);
      expect(result).toMatchObject({
        wait_index: 1,
        generation_index: 1,
        iteration_index: 1,
        additional_iterations: 2,
        intent_sha256: ctx.intent.sha256,
      });
      const state = result.state;
      expect(state.grants).toHaveLength(1);
      const generation = state.generations[0];
      expect(generation?.open_iteration).toBeUndefined();
      expect(generation?.iterations[0]?.closed).toEqual({
        by: "grant",
        wait_index: 1,
        closed_transition_count: 2,
      });
      const wait = state.waits[0];
      expect(wait?.response).toBeUndefined();
      expect(wait?.intent).toEqual({ intent_sha256: ctx.intent.sha256 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("2. the result carries exactly the content-free fields and is deep-frozen", async () => {
    const ctx = await grantReady();
    try {
      const result = await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      expect(Object.keys(result).sort()).toEqual([
        "additional_iterations",
        "generation_index",
        "intent_sha256",
        "iteration_index",
        "state",
        "wait_index",
      ]);
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.state)).toBe(true);
      expect(JSON.stringify(result)).not.toContain(ctx.fixture.runRoot);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("3. the applied state round-trips through the loader", async () => {
    const ctx = await grantReady();
    try {
      const result = await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      validatePipelineV2RunState(JSON.parse(JSON.stringify(result.state)) as never);
      const persisted = JSON.parse(await readFile(ctx.fixture.statePath, "utf8")) as PipelineV2RunState;
      expect(persisted.grants).toHaveLength(1);
      expect(persisted.generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("4. the exact complete retry dispatches nothing and returns the authoritative state", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const revision = ctx.sink.snapshot?.revision;
      if (typeof revision !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const recording = recordingSink(ctx.sink);
      const retry = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(retry.state.revision).toBe(revision);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("5. the partial retry after a durable grant dispatches only the closure", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const recording = recordingSink(ctx.sink);
      const result = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 },
      ]);
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1, additional_iterations: 2 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("6. the exact closure fields and anchor are recorded", async () => {
    const ctx = await grantReady();
    try {
      const result = await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      const state = result.state;
      const generation = state.generations[0];
      const iteration = generation?.iterations[0];
      expect(iteration?.closed?.by).toBe("grant");
      expect(iteration?.closed?.wait_index).toBe(1);
      const wait = state.waits[0];
      expect(iteration?.closed?.closed_transition_count).toBe(wait?.transition_count);
      expect(generation?.open_iteration).toBeUndefined();
      expect(state.status).toBe("waiting");
      expect(state.phase).toBe("waiting");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("7. the exact grant fields are recorded", async () => {
    const ctx = await grantReady();
    try {
      const result = await applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      const grant = result.state.grants[0];
      expect(grant).toEqual({
        index: 1,
        generation_index: 1,
        wait_index: 1,
        intent_sha256: ctx.intent.sha256,
        additional_iterations: 2,
      });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("8. a run without an accepted intent is rejected", async () => {
    const ctx = await grantReady({ acceptIntent: false });
    try {
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("has not accepted an intent");
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("9. an intent digest differing from the accepted one is rejected", async () => {
    const ctx = await grantReady();
    try {
      const other = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: (ctx.intent.manifest as { expected_plan_sha256: string }).expected_plan_sha256,
        additional_iterations: 5,
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: other }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("accepted a different intent");
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("10. a wait without the declared continue_stage action is rejected", async () => {
    const ctx = await grantReady({ acceptIntent: false, actions: [{ id: "revise_task", to: "architect" }] });
    try {
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("does not declare the continue_stage action");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("11. an intent of a foreign run is rejected", async () => {
    const ctx = await grantReady({ acceptIntent: false });
    try {
      const foreign = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: "run-other",
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: hex("e"),
        additional_iterations: 2,
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: foreign }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("another run than the durable run state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("12. an intent for another wait index is rejected", async () => {
    const ctx = await grantReady({ acceptIntent: false });
    try {
      const other = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 2,
        stage_id: "stage-1",
        expected_plan_sha256: hex("e"),
        additional_iterations: 2,
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: other }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("names wait index 2, but the open wait record is 1");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("13. a closed generation is rejected (defense-in-depth binding)", async () => {
    // the reducer does not close a stage generation inside a waiting run,
    // so a closed generation with an accepted intent is unreachable
    // through loader-valid states; the binding is exercised through the
    // pure helper over a state derived from the real fixture
    const ctx = await grantReady();
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const derived = structuredClone(state) as PipelineV2RunState;
      const generation = derived.generations[0];
      if (generation === undefined) {
        throw new Error("fixture generation missing");
      }
      const { open_iteration: _open, ...rest } = generation;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        closed: { by: "next_stage", closed_transition_count: 2 },
      };
      let caught: unknown;
      try {
        requireContinueStageGrantBindings(derived, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "invalid_state");
      expect(error.message).toContain("is closed; a grant is applied only inside an open generation");
      expect(error.state).toBe(derived);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("14. an existing grant with a different amount is a typed conflict", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 5 } });
    try {
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "grant_conflict");
      expect(error.message).toContain("already carries a different grant");
      expect(ctx.sink.snapshot?.grants[0]?.additional_iterations).toBe(5);
      expect(ctx.sink.snapshot?.generations[0]?.open_iteration).toBeDefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("15. the whole-suffix reducer pre-check failure is a typed invalid_state with zero dispatch", async () => {
    // the active boundary rejects the grant; the pre-check helper is the
    // single pre-check path (the controller's own bindings gate the flow
    // before the pre-check, so the suffix rejection is defense-in-depth)
    const active = await grantReady({ enterWait: false, acceptIntent: false });
    try {
      const state = active.sink.snapshot;
      if (state === null || state.status !== "active") {
        throw new Error("fixture boundary missing");
      }
      let caught: unknown;
      try {
        precheckGrantSequence(state, [
          {
            kind: "iteration_grant_recorded",
            generationIndex: 1,
            waitIndex: 1,
            intentSha256: hex("c"),
            additionalIterations: 2,
          },
          { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 },
        ], state);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "invalid_state");
      expect(error.message).toContain("does not accept the grant sequence");
      expect(error.state).toBe(state);
    } finally {
      await disposeRun(active.fixture);
    }
  });

  test("16. no stage generation is a defense-in-depth invalid_state", async () => {
    const ctx = await grantReady();
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const derived = structuredClone(state) as PipelineV2RunState;
      (derived as { generations: PipelineV2RunState["generations"] }).generations = [];
      let caught: unknown;
      try {
        requireContinueStageGrantBindings(derived, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "invalid_state");
      expect(error.message).toContain("no stage generation");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("17. no iteration history is a defense-in-depth invalid_state", async () => {
    const ctx = await grantReady();
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const derived = structuredClone(state) as PipelineV2RunState;
      const generation = derived.generations[0];
      if (generation === undefined) {
        throw new Error("fixture generation missing");
      }
      const { open_iteration: _open, ...rest } = generation;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = { ...rest, iterations: [] };
      let caught: unknown;
      try {
        requireContinueStageGrantBindings(derived, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "invalid_state");
      expect(error.message).toContain("records no iterations");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("18. a mismatching grant digest is a typed conflict (defense-in-depth)", async () => {
    // the reducer accepts a grant only for the wait's accepted intent and
    // the loader binds the grant to it, so a grant with a foreign digest
    // is unreachable through loader-valid states; the ledger check is
    // exercised through the pure planner over a state derived from the
    // real fixture
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const grant = derived.grants[0];
      if (grant === undefined) {
        throw new Error("fixture grant missing");
      }
      (derived as { grants: PipelineV2RunState["grants"] }).grants[0] = {
        ...grant,
        intent_sha256: hex("f"),
      };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, bindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "grant_conflict");
      expect(error.message).toContain("already carries a different grant");
      expect(error.state).toBe(derived);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("19. a differently closed iteration (normal_close) is a lifecycle conflict (defense-in-depth)", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const derivedGeneration = derived.generations[0];
      if (derivedGeneration === undefined) {
        throw new Error("fixture generation missing");
      }
      const iteration = derivedGeneration.iterations[0];
      if (iteration === undefined) {
        throw new Error("fixture iteration missing");
      }
      const { open_iteration: _open, ...rest } = derivedGeneration;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        open_iteration: undefined,
        iterations: [
          {
            index: iteration.index,
            opened_transition_count: iteration.opened_transition_count,
            closed: { by: "normal_close", closed_transition_count: waitTransitionCount(state) },
          },
        ],
      };
      const derivedBindings = { ...bindings, generation: derived.generations[0]!, iteration: derived.generations[0]!.iterations[0]! };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, derivedBindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "lifecycle_conflict");
      expect(error.message).toContain('was closed with "normal_close", not by the grant');
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("20. a differently closed iteration (exhausted) is a lifecycle conflict (defense-in-depth)", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const derivedGeneration = derived.generations[0];
      if (derivedGeneration === undefined) {
        throw new Error("fixture generation missing");
      }
      const iteration = derivedGeneration.iterations[0];
      if (iteration === undefined) {
        throw new Error("fixture iteration missing");
      }
      const { open_iteration: _open, ...rest } = derivedGeneration;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        open_iteration: undefined,
        iterations: [
          {
            index: iteration.index,
            opened_transition_count: iteration.opened_transition_count,
            closed: { by: "exhausted", closed_transition_count: waitTransitionCount(state) },
          },
        ],
      };
      const derivedBindings = { ...bindings, generation: derived.generations[0]!, iteration: derived.generations[0]!.iterations[0]! };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, derivedBindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "lifecycle_conflict");
      expect(error.message).toContain('was closed with "exhausted", not by the grant');
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("21. a grant closure bound to another wait is a lifecycle conflict (defense-in-depth)", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 }, closeGrantIteration: true });
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const derivedGeneration = derived.generations[0];
      if (derivedGeneration === undefined) {
        throw new Error("fixture generation missing");
      }
      const iteration = derivedGeneration.iterations[0];
      if (iteration === undefined || iteration.closed === undefined) {
        throw new Error("fixture closure missing");
      }
      const { open_iteration: _open, ...rest } = derivedGeneration;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        open_iteration: undefined,
        iterations: [
          {
            index: iteration.index,
            opened_transition_count: iteration.opened_transition_count,
            closed: { by: "grant", wait_index: 2, closed_transition_count: iteration.closed.closed_transition_count },
          },
        ],
      };
      const derivedBindings = { ...bindings, generation: derived.generations[0]!, iteration: derived.generations[0]!.iterations[0]! };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, derivedBindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "lifecycle_conflict");
      expect(error.message).toContain("another wait or another boundary");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("22. a grant closure without the exact durable grant is an invalid_state (defense-in-depth)", async () => {
    // the reducer's grant closure requires the recorded grant, so a
    // closure without an exact durable grant is unreachable through
    // reducer-valid states
    const ctx = await grantReady();
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const derivedGeneration = derived.generations[0];
      if (derivedGeneration === undefined) {
        throw new Error("fixture generation missing");
      }
      const iteration = derivedGeneration.iterations[0];
      if (iteration === undefined) {
        throw new Error("fixture iteration missing");
      }
      const { open_iteration: _open, ...rest } = derivedGeneration;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        open_iteration: undefined,
        iterations: [
          {
            index: iteration.index,
            opened_transition_count: iteration.opened_transition_count,
            closed: { by: "grant", wait_index: 1, closed_transition_count: waitTransitionCount(state) },
          },
        ],
      };
      (derived as { grants: PipelineV2RunState["grants"] }).grants = [];
      const derivedBindings = { ...bindings, generation: derived.generations[0]!, iteration: derived.generations[0]!.iterations[0]! };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, derivedBindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "invalid_state");
      expect(error.message).toContain("carries a grant closure without the exact durable grant");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  function waitTransitionCount(state: PipelineV2RunState): number {
    const wait = state.waits[state.waits.length - 1];
    if (wait === undefined) {
      throw new Error("fixture wait missing");
    }
    return wait.transition_count;
  }

  test("23. a stage mismatch is a lifecycle conflict (defense-in-depth)", async () => {
    // the accepted intent digest binds the stage id and the acceptance
    // binds the generation to the same stage, so a stage mismatch is
    // unreachable through loader-valid states; the binding is exercised
    // through the pure helper over a state derived from the real fixture
    const ctx = await grantReady();
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const derived = structuredClone(state) as PipelineV2RunState;
      const generation = derived.generations[0];
      if (generation === undefined) {
        throw new Error("fixture generation missing");
      }
      const { open_iteration: _open, ...rest } = generation;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        stage_id: "stage-2",
      };
      let caught: unknown;
      try {
        requireContinueStageGrantBindings(derived, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "lifecycle_conflict");
      expect(error.message).toContain("but the wait intent names stage");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("24. a replanned closure is a lifecycle conflict (defense-in-depth)", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const state = ctx.sink.snapshot;
      if (state === null) {
        throw new Error("fixture snapshot missing");
      }
      const bindings = requireContinueStageGrantBindings(state, ctx.intent);
      const derived = structuredClone(state) as PipelineV2RunState;
      const derivedGeneration = derived.generations[0];
      if (derivedGeneration === undefined) {
        throw new Error("fixture generation missing");
      }
      const iteration = derivedGeneration.iterations[0];
      if (iteration === undefined) {
        throw new Error("fixture iteration missing");
      }
      const { open_iteration: _open, ...rest } = derivedGeneration;
      (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
        ...rest,
        open_iteration: undefined,
        iterations: [
          {
            index: iteration.index,
            opened_transition_count: iteration.opened_transition_count,
            closed: { by: "replanned", wait_index: 1, closed_transition_count: waitTransitionCount(state) },
          },
        ],
      };
      const derivedBindings = { ...bindings, generation: derived.generations[0]!, iteration: derived.generations[0]!.iterations[0]! };
      let caught: unknown;
      try {
        planContinueStageGrant(derived, derivedBindings, ctx.intent);
      } catch (cause) {
        caught = cause;
      }
      const error = expectGrantError(caught, "lifecycle_conflict");
      expect(error.message).toContain('was closed with "replanned", not by the grant');
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("25. a grant dispatch that resolves without a snapshot change is rejected", async () => {
    const ctx = await grantReady();
    try {
      const hostileSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        dispatch() {
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostileSink, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied grant");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("26. a closure dispatch that resolves without a snapshot change is rejected", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const hostileSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        dispatch() {
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostileSink, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("does not carry the applied closure");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("27. a racing identical grant dispatch is idempotent success on the exact durable record", async () => {
    const ctx = await grantReady();
    try {
      const realDispatch = ctx.sink.dispatch.bind(ctx.sink);
      const racingSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await realDispatch(command);
          throw new PipelineV2StateError("the grant is already recorded (simulated race)");
        },
      };
      const result = await applyPipelineV2ContinueStageGrant({ sink: racingSink, intent: ctx.intent });
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(result.state.grants).toHaveLength(1);
      expect(result.state.generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28. a racing identical closure dispatch is idempotent success on the exact durable record", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const realDispatch = ctx.sink.dispatch.bind(ctx.sink);
      const racingSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot() {
          return ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await realDispatch(command);
          throw new PipelineV2StateError("the iteration is already closed (simulated race)");
        },
      };
      const result = await applyPipelineV2ContinueStageGrant({ sink: racingSink, intent: ctx.intent });
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(result.state.generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28b. a hostile grant snapshot removing the accepted wait intent is rejected", async () => {
    const ctx = await grantReady();
    try {
      let mutated = false;
      const stripIntent = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const position = derived.waits.length - 1;
        const wait = derived.waits[position];
        if (wait === undefined) {
          throw new Error("fixture wait missing");
        }
        const { intent: _removed, ...rest } = wait;
        (derived.waits as unknown as PipelineV2RunState["waits"])[position] = rest;
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? stripIntent(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "invalid_state");
      expect(error.message).toContain("missing or replaced in the authoritative state");
      // the real durable state advanced with the grant; the closure never
      // dispatched
      const durable = ctx.sink.snapshot;
      expect(durable?.grants).toHaveLength(1);
      expect(durable?.generations[0]?.open_iteration).toBeDefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28c. a hostile grant snapshot replacing the accepted intent digest is rejected", async () => {
    const ctx = await grantReady();
    try {
      let mutated = false;
      const replaceIntent = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const position = derived.waits.length - 1;
        const wait = derived.waits[position];
        if (wait === undefined || wait.intent === undefined) {
          throw new Error("fixture wait intent missing");
        }
        (derived.waits as unknown as PipelineV2RunState["waits"])[position] = {
          ...wait,
          intent: { intent_sha256: hex("f") },
        };
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? replaceIntent(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "invalid_state");
      expect((cause as Error).message).toContain("missing or replaced in the authoritative state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28d. a hostile closure snapshot removing the accepted wait intent is rejected", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      let mutated = false;
      const stripIntent = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const position = derived.waits.length - 1;
        const wait = derived.waits[position];
        if (wait === undefined) {
          throw new Error("fixture wait missing");
        }
        const { intent: _removed, ...rest } = wait;
        (derived.waits as unknown as PipelineV2RunState["waits"])[position] = rest;
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? stripIntent(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "invalid_state");
      expect((cause as Error).message).toContain("missing or replaced in the authoritative state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28e. a hostile closure snapshot replacing the accepted intent digest is rejected", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      let mutated = false;
      const replaceIntent = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const position = derived.waits.length - 1;
        const wait = derived.waits[position];
        if (wait === undefined || wait.intent === undefined) {
          throw new Error("fixture wait intent missing");
        }
        (derived.waits as unknown as PipelineV2RunState["waits"])[position] = {
          ...wait,
          intent: { intent_sha256: hex("f") },
        };
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? replaceIntent(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "invalid_state");
      expect((cause as Error).message).toContain("missing or replaced in the authoritative state");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28f. a hostile closure snapshot closing the generation is rejected", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      let mutated = false;
      const closeGeneration = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const generation = derived.generations[0];
        if (generation === undefined) {
          throw new Error("fixture generation missing");
        }
        const { open_iteration: _open, ...rest } = generation;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...rest,
          closed: { by: "next_stage", closed_transition_count: 2 },
        };
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? closeGeneration(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "lifecycle_conflict");
      expect((cause as Error).message).toContain("already closed differently");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28g. a hostile closure snapshot substituting the generation stage binding is rejected", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      let mutated = false;
      const swapStage = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const generation = derived.generations[0];
        if (generation === undefined) {
          throw new Error("fixture generation missing");
        }
        const { open_iteration: _open, ...rest } = generation;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...rest,
          stage_id: "stage-9",
        };
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? swapStage(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "lifecycle_conflict");
      expect((cause as Error).message).toContain("already closed differently");
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("28h. the same hostile shapes through the racing PipelineV2StateError path are never idempotent success", async () => {
    // stripped wait intent on the grant step: the racing dispatch must not
    // be recognized as success
    const ctx = await grantReady();
    try {
      let mutated = false;
      const stripIntent = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const position = derived.waits.length - 1;
        const wait = derived.waits[position];
        if (wait === undefined) {
          throw new Error("fixture wait missing");
        }
        const { intent: _removed, ...rest } = wait;
        (derived.waits as unknown as PipelineV2RunState["waits"])[position] = rest;
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? stripIntent(ctx.sink.snapshot as PipelineV2RunState) : ctx.sink.snapshot;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx.sink.dispatch(command);
          mutated = true;
          throw new PipelineV2StateError("the grant is already recorded (simulated race)");
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx.intent }),
      );
      expectGrantError(cause, "invalid_state");
      expect((cause as Error).message).toContain("missing or replaced in the authoritative state");
    } finally {
      await disposeRun(ctx.fixture);
    }
    // the substituted generation binding on the closure step: the racing
    // dispatch must not be recognized as success
    const ctx2 = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      let mutated = false;
      const swapStage = (state: PipelineV2RunState): PipelineV2RunState => {
        const derived = structuredClone(state) as PipelineV2RunState;
        const generation = derived.generations[0];
        if (generation === undefined) {
          throw new Error("fixture generation missing");
        }
        const { open_iteration: _open, ...rest } = generation;
        (derived.generations as unknown as PipelineV2RunState["generations"])[0] = {
          ...rest,
          stage_id: "stage-9",
        };
        return derived;
      };
      const hostile: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return mutated ? swapStage(ctx2.sink.snapshot as PipelineV2RunState) : ctx2.sink.snapshot;
        },
        get poisoned() {
          return ctx2.sink.poisoned;
        },
        async dispatch(command: PipelineV2RunCommand) {
          await ctx2.sink.dispatch(command);
          mutated = true;
          throw new PipelineV2StateError("the iteration is already closed (simulated race)");
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: hostile, intent: ctx2.intent }),
      );
      expectGrantError(cause, "lifecycle_conflict");
      expect((cause as Error).message).toContain("already closed differently");
    } finally {
      await disposeRun(ctx2.fixture);
    }
  });

  test("29. a grant not-committed leaves the state unchanged and a fresh retry commits the full suffix", async () => {
    const ctx = await grantReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: faulted, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be committed");
      expect(error.state?.grants).toHaveLength(0);
      expect(faulted.poisoned).toBe(false);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const result = await applyPipelineV2ContinueStageGrant({ sink: fresh, intent: ctx.intent });
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(result.state.grants).toHaveLength(1);
      expect(result.state.generations[0]?.open_iteration).toBeUndefined();
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("30. a grant durability-unknown adopts the grant and a fresh retry dispatches only the closure", async () => {
    const ctx = await grantReady();
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: faulted, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be confirmed durable");
      expect(error.state?.grants).toHaveLength(1);
      expect(error.state?.generations[0]?.open_iteration).toBeDefined();
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 },
      ]);
      expect(retry).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("31. a closure not-committed keeps the grant and a fresh retry dispatches only the closure", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "rename" }),
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: faulted, intent: ctx.intent }),
      );
      expectGrantError(cause, "state_persist_failed");
      expect(ctx.sink.snapshot?.grants).toHaveLength(1);
      expect(ctx.sink.snapshot?.generations[0]?.open_iteration).toBeDefined();
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const result = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([
        { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "grant", waitIndex: 1 },
      ]);
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("32. a closure durability-unknown adopts the closure and a fresh retry dispatches nothing", async () => {
    const ctx = await grantReady({ recordGrant: { additionalIterations: 2 } });
    try {
      const faulted = await PipelineV2RunStateSink.open({
        stateRoot: ctx.fixture.stateRoot,
        runId: RUN_ID,
        now: nextTick,
        io: faultIo({ failCommit: 1, failStep: "dirfsync" }),
      });
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: faulted, intent: ctx.intent }),
      );
      const error = expectGrantError(cause, "state_persist_failed");
      expect(error.message).toContain("could not be confirmed durable");
      expect(error.state?.generations[0]?.open_iteration).toBeUndefined();
      expect(error.state?.grants).toHaveLength(1);
      expect(faulted.poisoned).toBe(true);
      const fresh = await PipelineV2RunStateSink.open({ stateRoot: ctx.fixture.stateRoot, runId: RUN_ID, now: nextTick });
      const recording = recordingSink(fresh);
      const retry = await applyPipelineV2ContinueStageGrant({ sink: recording, intent: ctx.intent });
      expect(recording.commands).toEqual([]);
      expect(retry).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("33. two identical grant calls racing succeed with exactly one grant and one closure", async () => {
    const ctx = await grantReady();
    try {
      const revisionBefore = ctx.sink.snapshot?.revision;
      if (typeof revisionBefore !== "number") {
        throw new Error("the durable revision must be recorded");
      }
      const first = applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      const second = applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: ctx.intent });
      const [one, two] = await Promise.all([first, second]);
      expect(one).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(two).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      const state = ctx.sink.snapshot;
      expect(state?.grants).toHaveLength(1);
      expect(state?.generations[0]?.iterations[0]?.closed?.by).toBe("grant");
      expect(state?.revision).toBe(revisionBefore + 2);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("34. hand-built, spread, cloned and proxied intents are rejected by provenance", async () => {
    const ctx = await grantReady();
    try {
      const intent = ctx.intent;
      const handBuilt = { manifest: { ...intent.manifest }, canonical_json: intent.canonical_json, sha256: intent.sha256 };
      const spread = { ...intent };
      const cloned = structuredClone(intent);
      let proxyTraps = 0;
      const proxied = new Proxy(intent, {
        get() {
          proxyTraps += 1;
          return undefined;
        },
      });
      for (const [name, lookalike] of [["hand-built", handBuilt], ["spread", spread], ["clone", cloned], ["proxy", proxied]] as const) {
        const cause = await catchAccept(() =>
          applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: lookalike }),
        );
        const error = expectGrantError(cause, "invalid_intent");
        expect(error.message).toContain("not a provenance-registered continue_stage_intent");
        expect(error.state).toBeNull();
        if (name === "proxy") {
          expect(proxyTraps).toBe(0);
        }
      }
      expect(ctx.sink.snapshot?.grants).toHaveLength(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("35. a proxied snapshot is not read before the provenance gate passes", async () => {
    const ctx = await grantReady();
    try {
      let snapshotTraps = 0;
      const snapshot = ctx.sink.snapshot;
      if (snapshot === null) {
        throw new Error("fixture snapshot missing");
      }
      const snapshotProxy = new Proxy(snapshot as unknown as object, {
        get() {
          snapshotTraps += 1;
          return undefined;
        },
      });
      let intentTraps = 0;
      const intentProxy = new Proxy(ctx.intent, {
        get() {
          intentTraps += 1;
          return undefined;
        },
      });
      const proxiedSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return snapshotProxy as unknown as PipelineV2RunState;
        },
        get poisoned() {
          return ctx.sink.poisoned;
        },
        dispatch: ctx.sink.dispatch,
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: proxiedSink, intent: intentProxy }),
      );
      expectGrantError(cause, "invalid_intent");
      expect(intentTraps).toBe(0);
      expect(snapshotTraps).toBe(0);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("36. a missing or invalid snapshot is the controller's typed invalid_state", async () => {
    const ctx = await grantReady();
    try {
      const intent = ctx.intent;
      let dispatches: PipelineV2RunCommand[] = [];
      const nullSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return null;
        },
        get poisoned() {
          return false;
        },
        dispatch(command: PipelineV2RunCommand) {
          dispatches.push(command);
          return undefined;
        },
      };
      const nullCause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: nullSink, intent }),
      );
      const nullError = expectGrantError(nullCause, "invalid_state");
      expect(nullError.message).toBe("the durable run state is missing or not a valid pipeline v2 run state");
      expect(nullError.state).toBeNull();
      expect(dispatches).toEqual([]);
      const brokenSnapshot = {
        schema_version: 7,
        run_id: "CANARY-RUN",
        revision: 1,
        status: "not-a-status",
        phase: "running",
        waits: [{ canary: true }],
      };
      const brokenSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          return brokenSnapshot as unknown as PipelineV2RunState;
        },
        get poisoned() {
          return false;
        },
        dispatch(command: PipelineV2RunCommand) {
          dispatches.push(command);
          return undefined;
        },
      };
      const brokenCause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: brokenSink, intent }),
      );
      const brokenError = expectGrantError(brokenCause, "invalid_state");
      expect(brokenError.message).toBe("the durable run state is missing or not a valid pipeline v2 run state");
      expect(brokenError.state).toBeNull();
      expect(brokenError.message).not.toContain("CANARY-RUN");
      expect(brokenError.message).not.toContain("canary");
      expect(dispatches).toEqual([]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("37. options and sink getters are read exactly once", async () => {
    const ctx = await grantReady();
    try {
      const optionReads: string[] = [];
      const sinkReads: string[] = [];
      const inner = recordingSink(ctx.sink);
      const sinkProxy = new Proxy(inner as unknown as Record<string, unknown>, {
        get(target, property) {
          sinkReads.push(String(property));
          return target[property as string];
        },
      });
      const optionsProxy = new Proxy({ sink: sinkProxy, intent: ctx.intent } as Record<string, unknown>, {
        get(target, property) {
          optionReads.push(String(property));
          return target[property as string];
        },
      });
      const result = await applyPipelineV2ContinueStageGrantInternal(optionsProxy as unknown as Parameters<typeof applyPipelineV2ContinueStageGrant>[0]);
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(optionReads.filter((name) => name === "sink" || name === "intent")).toEqual(["sink", "intent"]);
      expect(sinkReads.filter((name) => name === "poisoned" || name === "dispatch" || name === "snapshot")).toEqual(["poisoned", "dispatch", "snapshot", "snapshot", "snapshot"]);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("38. throwing getters preserve their error identity", async () => {
    const ctx = await grantReady();
    try {
      const boom = new Error("the snapshot getter failed");
      const throwingSink: PipelineV2ContinueStageGrantControllerSink = {
        get snapshot(): PipelineV2RunState | null {
          throw boom;
        },
        get poisoned() {
          return false;
        },
        dispatch() {
          return undefined;
        },
      };
      const cause = await catchAccept(() =>
        applyPipelineV2ContinueStageGrant({ sink: throwingSink, intent: ctx.intent }),
      );
      expect(cause).toBe(boom);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("39. caller mutation after the capture cannot influence the application", async () => {
    const ctx = await grantReady();
    try {
      const options = { sink: ctx.sink, intent: ctx.intent };
      const pending = applyPipelineV2ContinueStageGrant(options);
      (options as { sink: unknown }).sink = null;
      const result = await pending;
      expect(result).toMatchObject({ wait_index: 1, generation_index: 1, iteration_index: 1 });
      expect(result.state.grants).toHaveLength(1);
    } finally {
      await disposeRun(ctx.fixture);
    }
  });

  test("40. diagnostics are content-free across the failure paths", async () => {
    const ctx = await grantReady({ acceptIntent: false });
    try {
      const messages: string[] = [];
      const foreign = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: "run-other",
        wait_index: 1,
        stage_id: "stage-1",
        expected_plan_sha256: hex("e"),
        additional_iterations: 2,
      });
      const otherWait = prepareWaitIntent({
        schema_version: 1,
        kind: "continue_stage_intent",
        run_id: RUN_ID,
        wait_index: 2,
        stage_id: "stage-1",
        expected_plan_sha256: hex("e"),
        additional_iterations: 2,
      });
      const cases: Array<() => Promise<unknown>> = [
        () => applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: foreign }),
        () => applyPipelineV2ContinueStageGrant({ sink: ctx.sink, intent: otherWait }),
      ];
      for (const run of cases) {
        const cause = await catchAccept(run);
        messages.push((cause as Error).message);
      }
      for (const message of messages) {
        expect(message).not.toContain(ctx.fixture.runRoot);
        expect(message).not.toContain(ctx.intent.canonical_json);
        expect(message).not.toContain("Body A");
      }
    } finally {
      await disposeRun(ctx.fixture);
    }
  });
});

test("41. the runtime export surfaces are exact (public two keys, internal core)", async () => {
  const publicModule = await import("../src/pipeline_v2_continue_stage_grant_controller.ts");
  expect(Object.keys(publicModule).sort()).toEqual([
    "PipelineV2ContinueStageGrantControllerError",
    "applyPipelineV2ContinueStageGrant",
  ]);
  const internalModule = await import("../src/pipeline_v2_continue_stage_grant_controller_internal.ts");
  expect(Object.keys(internalModule).sort()).toEqual([
    "PipelineV2ContinueStageGrantControllerError",
    "applyPipelineV2ContinueStageGrantInternal",
    "planContinueStageGrant",
    "precheckGrantSequence",
    "requireContinueStageGrantBindings",
  ]);
});

test("42. the controller is pure state-and-sink (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_continue_stage_grant_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("validatePipelineV2RunState(")).toBe(1);
  expect(countOf("reducePipelineV2RunCommand(")).toBe(1);
  expect(countOf("loadPipelineV2PlanRevision(")).toBe(0);
  expect(countOf("publishPipelineV2WaitIntent(")).toBe(0);
  expect(countOf("prepareWaitIntent(")).toBe(0);
  expect(countOf("parseWaitIntent(")).toBe(0);
  expect(countOf("canonicalJson(")).toBe(0);
  expect(countOf("CryptoHasher")).toBe(0);
  expect(countOf("createHash")).toBe(0);
  expect(countOf("new WeakMap")).toBe(0);
  expect(countOf("new WeakSet")).toBe(0);
  expect(countOf("JSON.parse")).toBe(0);
  expect(countOf("O_EXCL")).toBe(0);
  expect(countOf("O_CREAT")).toBe(0);
  expect(countOf("O_NOFOLLOW")).toBe(0);
  expect(countOf("fsyncImmutableDirectory(")).toBe(0);
  expect(countOf("mkdirExclusive(")).toBe(0);
  expect(countOf("readWholeFile(")).toBe(0);
  expect(countOf("lstat")).toBe(0);
  expect(countOf("node:fs")).toBe(0);
  expect(countOf("node:path")).toBe(0);
  expect(countOf(".match(")).toBe(0);
  expect(countOf("RegExp(")).toBe(0);
  for (const banned of ["pipeline_v2_coordinator", "pipeline_v2_runner", "main.ts", "cli_", "docker", "launcher", "pipeline_v2_run_plan_store", "pipeline_v2_wait_store", "pipeline_v2_wait_controller", "acceptPipelineV2ContinueStageIntent("]) {
    expect(source).not.toContain(banned);
  }
  expect(countOf("Object.freeze")).toBe(0);
  expect(countOf("let production")).toBe(0);
});

test("43. the pure planner is the single reconciliation authority (source scan)", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync("orchestrator/src/pipeline_v2_continue_stage_grant_controller_internal.ts", "utf8");
  const countOf = (pattern: string): number => source.split(pattern).length - 1;
  expect(countOf("planContinueStageGrant(")).toBe(2);
  expect(countOf('kind: "s0"')).toBe(2);
  expect(countOf('kind: "s1"')).toBe(2);
  expect(countOf('kind: "s2"')).toBe(2);
});
