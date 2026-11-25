/**
 * The S5-P0 red-before overlay battery A. Every test in this file must be
 * RED on the clean base `855b434` (except the one explicit
 * green-preservation guard, marked below), because the S5-P0 production
 * change (schema v13, the reducer-written execution riding, the
 * `execution-bindings` CLI command, the `execution_bindings` routing
 * kind and the `GET /v1/runs/:runId/execution-bindings` route) has not
 * landed yet. This file deliberately imports NO module that does not
 * already exist on the base, so each test records its own exact failure
 * instead of failing at file load.
 */
import { describe, expect, test } from "bun:test";

import { parseCommand } from "../src/cli_args.ts";
import { parseControlPlaneRoute } from "../src/control_plane/http_server.ts";
import {
  routeOrchestratorCommand,
  type OrchestratorCommand,
  type OrchestratorRoutingConfig,
} from "../src/control_plane/routing.ts";
import { buildPipelineV2IdentityRecords } from "../src/pipeline_v2_identity.ts";
import {
  PIPELINE_V2_COMMIT_PROVENANCE_KINDS,
  PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
  validatePipelineV2RunState,
  type PipelineV2AgentExecutionState,
  type PipelineV2DecisionExecutionState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { apply, hex, IDENTITY, lifecycleCommands, planningLadder, stageLadder } from "./pipeline_v2_commit_history_fixture.ts";

const RUN_CONFIG: OrchestratorRoutingConfig = {
  configRoot: "/tmp/cp-config",
} as unknown as OrchestratorRoutingConfig;

const lifecycleState = (): PipelineV2RunState => apply(lifecycleCommands());

const agentAt = (state: PipelineV2RunState, index: number): PipelineV2AgentExecutionState =>
  state.executions[index - 1] as PipelineV2AgentExecutionState;

const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };
const V2_PROFILE_EPOCH_BINDING = { digest: "d".repeat(64), profile_count: 1 };

/** Local transition helper (same shape as the fixture's). */
function transition(from: string, to: string, executionIndex: number): PipelineV2RunCommand {
  return { kind: "transition_committed", step: { from, outcome: "completed", to, transition_index: 0 }, executionIndex };
}

/** The decision battery: a control-role decision execution inside an open generation. */
function decisionControlCommands(): PipelineV2RunCommand[] {
  return [
    { kind: "create_run", runId: "run-decision", pipeline: IDENTITY, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING, profileEpoch: V2_PROFILE_EPOCH_BINDING },
    ...planningLadder("sess-1", "tool-1"),
    { kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1") },
    { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1 },
    { kind: "stage_generation_opened", stageId: "stage-1", stagePosition: 1, templateId: "development", planSha256: hex("3"), initialBudget: 2, transitionCount: 0 },
    { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 0 },
    transition("architect", "dev_entry", 1),
    ...stageLadder("sess-2", "tool-2", 1),
    transition("dev_entry", "review", 2),
    { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" },
    { kind: "start_decision_execution", stateId: "review", inputDigest: hex("2"), executionRole: "control" },
    { kind: "decision_evaluated", result: { status: "selected", outcome: "accept", decision: "accept", rule_id: "R7", active_constraint_ids: ["HC2"] } },
    { kind: "transition_committed", step: { from: "review", outcome: "accept", to: "done", transition_index: 0 }, executionIndex: 3 },
    { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
    { kind: "run_outputs_published", outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }] },
    { kind: "run_succeeded" },
  ];
}

describe("S5-P0 schema v13 (constant, versions, reducer writes)", () => {
  test("the supported run-state schema version is 13", () => {
    expect(PIPELINE_V2_RUN_STATE_SCHEMA_VERSION).toBe(13);
  });

  test("a stored schema_version 12 document is rejected with the pinned legacy message", () => {
    const parsed = JSON.parse(JSON.stringify(lifecycleState())) as Record<string, unknown>;
    parsed.schema_version = 12;
    expect(parsed.schema_version).toBe(12);
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(
      "pipeline v2 run state has schema_version 12",
    );
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(
      "(schema version 13 is the supported contract; no v12 migration exists)",
    );
  });

  test("the reducer records the open generation at every start (lifecycle battery)", () => {
    const state = lifecycleState();
    // Exec 1: first planning execution — no generation open at the start.
    expect(agentAt(state, 1).generation_index).toBeUndefined();
    // Exec 2/3: stage executions of generation 1, iterations 1 and 2.
    expect(agentAt(state, 2).generation_index).toBe(1);
    expect(agentAt(state, 2).iteration_index).toBe(1);
    expect(agentAt(state, 3).generation_index).toBe(1);
    expect(agentAt(state, 3).iteration_index).toBe(2);
    // Exec 4: the replanning execution — generation 1 is open, its
    // iteration is closed; planning roles never carry an iteration.
    expect(agentAt(state, 4).generation_index).toBe(1);
    expect(agentAt(state, 4).iteration_index).toBeUndefined();
    // Exec 5: the successor stage execution of generation 2, iteration 1.
    expect(agentAt(state, 5).generation_index).toBe(2);
    expect(agentAt(state, 5).iteration_index).toBe(1);
  });

  test("a control-role decision execution inside an open generation records the generation", () => {
    const state = apply(decisionControlCommands());
    const control = state.executions[2] as PipelineV2DecisionExecutionState;
    expect(control.type).toBe("decision");
    expect(control.execution_role).toBe("control");
    expect(control.generation_index).toBe(1);
    expect(control.iteration_index).toBeUndefined();
    expect(agentAt(state, 2).generation_index).toBe(1);
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(state)))).toBeTruthy();
  });

  test("a control-role decision execution after the generation closed records no generation", () => {
    const commands: PipelineV2RunCommand[] = [
      ...decisionControlCommands().slice(0, -6),
      { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" },
      { kind: "start_decision_execution", stateId: "review", inputDigest: hex("2"), executionRole: "control" },
      { kind: "decision_evaluated", result: { status: "selected", outcome: "accept", decision: "accept", rule_id: "R7", active_constraint_ids: ["HC2"] } },
      { kind: "transition_committed", step: { from: "review", outcome: "accept", to: "done", transition_index: 0 }, executionIndex: 3 },
      { kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" },
      { kind: "run_outputs_published", outputs: [{ id: "plan", type: "file", required: true, present: true, digest: hex("d") }] },
      { kind: "run_succeeded" },
    ];
    const state = apply(commands);
    const control = state.executions[2] as PipelineV2DecisionExecutionState;
    expect(control.type).toBe("decision");
    expect(control.execution_role).toBe("control");
    // The reducer's own truth: the generation was closed at the same
    // boundary before the control execution started, so the start wrote
    // no riding generation.
    expect(control.generation_index).toBeUndefined();
    expect(control.iteration_index).toBeUndefined();
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(state)))).toBeTruthy();
  });

  test("the loader rejects a planning/control execution that carries no generation while one is provably open", () => {
    const parsed = JSON.parse(
      JSON.stringify(apply(decisionControlCommands())),
    ) as {
      executions: Array<Record<string, unknown>>;
    };
    const control = parsed.executions[2] as Record<string, unknown>;
    delete control.generation_index;
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(
      /records no generation, but generation 1 is open/,
    );
  });

  test("the loader admits a planning/control execution riding a closed-at-boundary generation as absent as present", () => {
    // Order (a) of the R2 family: the replanned generation closure is
    // dispatched before the planning ladder at the same boundary, so the
    // planning start honestly carries no riding generation; the same
    // boundary still admits the riding form when the closure is
    // dispatched after the ladder. Both forms are loader-valid.
    const genClose: PipelineV2RunCommand = { kind: "stage_generation_closed", generationIndex: 1, by: "replanned" };
    const prefix = lifecycleCommands().slice(0, 41);
    const planningLadderCommands = () => planningLadder("sess-9", "tool-9");
    const genOpen: PipelineV2RunCommand = {
      kind: "stage_generation_opened",
      stageId: "stage-1",
      stagePosition: 1,
      templateId: "development",
      planSha256: hex("4"),
      initialBudget: 3,
      transitionCount: 3,
    };
    const iterOpen: PipelineV2RunCommand = { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 3 };
    const absentForm = apply([...prefix, genClose, ...planningLadderCommands()]);
    expect((absentForm.executions[3] as PipelineV2AgentExecutionState).generation_index).toBeUndefined();
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(absentForm)))).toBeTruthy();
    const plan2: PipelineV2RunCommand = { kind: "plan_revision_accepted", planRevision: 2, planSha256: hex("4"), originExecution: 4 };
    const presentForm = apply([...prefix, ...planningLadderCommands(), genClose, plan2, genOpen, iterOpen]);
    expect((presentForm.executions[3] as PipelineV2AgentExecutionState).generation_index).toBe(1);
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(presentForm)))).toBeTruthy();
  });

  test("the v13 reducer-produced lifecycle loader round-trips through the single validator", () => {
    expect(validatePipelineV2RunState(JSON.parse(JSON.stringify(lifecycleState())))).toBeTruthy();
  });
});

describe("S5-P0 loader checks (structural and positional)", () => {
  test("a stage execution without its generation riding is rejected", () => {
    const parsed = JSON.parse(JSON.stringify(lifecycleState())) as {
      executions: Array<Record<string, unknown>>;
    };
    delete parsed.executions[1]!.generation_index;
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(/generation/);
  });

  test("generation riding values outside the durable grammar are rejected", () => {
    for (const bad of [0, -1, 1.5, "1", null, true]) {
      const parsed = JSON.parse(JSON.stringify(lifecycleState())) as {
        executions: Array<Record<string, unknown>>;
      };
      (parsed.executions[1] as Record<string, unknown>).generation_index = bad;
      expect(() => validatePipelineV2RunState(parsed as never)).toThrow(/generation_index/);
    }
  });

  test("a planning execution naming a generation that opens after its boundary is rejected", () => {
    const parsed = JSON.parse(JSON.stringify(lifecycleState())) as {
      executions: Array<Record<string, unknown>>;
    };
    parsed.executions[0]!.generation_index = 2;
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(/generation/);
  });

  test("a stage execution naming a generation with no admissible iteration is rejected", () => {
    const parsed = JSON.parse(JSON.stringify(lifecycleState())) as {
      executions: Array<Record<string, unknown>>;
    };
    parsed.executions[1]!.generation_index = 9;
    expect(() => validatePipelineV2RunState(parsed as never)).toThrow(/generation/);
  });

  test("the commit-history vocabulary stays the 28 kinds with unchanged start entries", () => {
    const state = lifecycleState() as unknown as {
      commit_history: Array<Record<string, unknown>>;
    };
    const kinds = state.commit_history.map((entry) => entry.kind);
    for (const kind of kinds) {
      expect((PIPELINE_V2_COMMIT_PROVENANCE_KINDS as readonly unknown[]).includes(kind)).toBe(true);
    }
    const start = state.commit_history.filter((entry) => entry.kind === "start_agent_execution")[0]!;
    expect(Object.keys(start).sort()).toEqual(["execution_index", "kind", "revision", "state_id"]);
  });
});

describe("S5-P0 CLI command (parse grammar)", () => {
  test("`execution-bindings` parses the canonical read-only grammar", () => {
    const parsed = parseCommand("execution-bindings", [
      "--run-id",
      "run-x",
      "--config-root",
      "/tmp/config",
      "--json",
    ]);
    expect(parsed.kind).toBe("execution-bindings");
    expect(parsed).toEqual({
      kind: "execution-bindings",
      runId: "run-x",
      configRoot: "/tmp/config",
      after: 0,
      revision: null,
      limit: 100,
      json: true,
    });
  });

  test("`execution-bindings` parses both flag forms and explicit pagination", () => {
    const parsed = parseCommand("execution-bindings", [
      "--run-id=run-x",
      "--config-root=/tmp/config",
      "--after=6",
      "--revision=24",
      "--limit=7",
    ]);
    expect(parsed).toEqual({
      kind: "execution-bindings",
      runId: "run-x",
      configRoot: "/tmp/config",
      after: 6,
      revision: 24,
      limit: 7,
      json: false,
    });
  });

  test("`execution-bindings` rejects duplicates, unknown and forbidden flags", () => {
    expect(() =>
      parseCommand("execution-bindings", ["--run-id", "a", "--run-id", "b", "--config-root", "/c"]),
    ).toThrow(/at most once/);
    expect(() => parseCommand("execution-bindings", ["--run-id", "a", "--config-root", "/c", "extra"])).toThrow(
      /unknown argument/,
    );
    expect(() => parseCommand("execution-bindings", ["--run-id", "a", "--config-root", "/c", "--limit", "201"])).toThrow(
      /up to 200/,
    );
    expect(() =>
      parseCommand("execution-bindings", ["--run-id", "a", "--config-root", "/c", "--image", "x"]),
    ).toThrow(/--image/);
    expect(() =>
      parseCommand("execution-bindings", ["--run-id", "a", "--config-root", "/c", "--wait-index", "1"]),
    ).toThrow(/--wait-index/);
  });

  test("the execution-bindings command appears in the command list", async () => {
    const mainModule = (await import("../src/main.ts")) as unknown as {
      COMMANDS: readonly string[];
    };
    expect(mainModule.COMMANDS).toContain("execution-bindings");
  });
});

describe("S5-P0 routing kind (execution_bindings)", () => {
  test("the closed routing kind routes to the CLI argv with the exact flags", () => {
    const command: OrchestratorCommand = {
      kind: "execution_bindings",
      runId: "run-x",
      after: 3,
      revision: 9,
      limit: 40,
    };
    const routed = routeOrchestratorCommand(command, RUN_CONFIG);
    expect(routed.argv).toEqual([
      "execution-bindings",
      "--run-id",
      "run-x",
      "--config-root",
      "/tmp/cp-config",
      "--after",
      "3",
      "--revision",
      "9",
      "--limit",
      "40",
      "--json",
    ]);
  });

  test("a continuation without the revision pin is refused invalid_command", () => {
    const command: OrchestratorCommand = { kind: "execution_bindings", runId: "run-x", after: 3 };
    expect(() => routeOrchestratorCommand(command, RUN_CONFIG)).toThrow(/revision pin/);
  });

  test("unknown and hostile fields are refused", () => {
    expect(() =>
      routeOrchestratorCommand(
        { kind: "execution_bindings", runId: "run-x", stageId: "stage-1" } as never,
        RUN_CONFIG,
      ),
    ).toThrow(/unknown field/);
    expect(() =>
      routeOrchestratorCommand(
        { kind: "execution_bindings", runId: "../escape" } as never,
        RUN_CONFIG,
      ),
    ).toThrow();
  });
});

describe("S5-P0 HTTP route (versioned GET-only)", () => {
  test("the route parses and answers GET with its own kind", () => {
    const parsed = parseControlPlaneRoute("/v1/runs/run-x/execution-bindings", "GET");
    if (parsed instanceof Error) {
      throw new Error(`expected the execution-bindings route to parse: ${String((parsed as Error).message)}`);
    }
    expect(parsed.kind).toBe("execution_bindings");
    expect((parsed as unknown as { runId: string }).runId).toBe("run-x");
  });

  test("the route accepts GET only", () => {
    const parsed = parseControlPlaneRoute("/v1/runs/run-x/execution-bindings", "POST");
    if (!(parsed instanceof Error)) {
      throw new Error("expected the POST form to be refused");
    }
    expect((parsed as unknown as { reason?: string }).reason).toBe("method_not_allowed");
  });

  test("the execution-bindings query parser exists with the strict grammar", async () => {
    const httpServerModule = (await import("../src/control_plane/http_server.ts")) as unknown as Record<
      string,
      unknown
    >;
    const parse = httpServerModule.parseExecutionBindingsQuery;
    expect(typeof parse).toBe("function");
    expect((parse as (q: string) => unknown)("after=3&revision=9&limit=40")).toEqual({
      after: 3,
      revision: 9,
      limit: 40,
    });
  });
});

/**
 * Green-preservation guard (NOT a red-before case): the P-3 identity
 * stream stays byte-stable — the new durable riding must NEVER join the
 * projected identity records.
 */
describe("green-preservation guard", () => {
  test("the identity stream carries no execution riding on executions and no new record kind", () => {
    const records = buildPipelineV2IdentityRecords(lifecycleState());
    for (const record of records) {
      if (record.record === "execution_agent" || record.record === "execution_decision") {
        expect(Object.keys(record as object)).not.toContain("generation_index");
      }
    }
    expect(new Set(records.map((record) => record.record))).toEqual(
      new Set([
        "plan_revision",
        "task_revision",
        "generation",
        "iteration",
        "grant",
        "wait",
        "wait_action",
        "execution_agent",
        "agent_output",
      ]),
    );
  });
});
