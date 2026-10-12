import { describe, expect, test } from "bun:test";

import {
  OrchestratorSubprocessClientError,
  routeOrchestratorCommand,
  type OrchestratorCommand,
  type OrchestratorRunInput,
  type OrchestratorTransportReason,
} from "../src/control_plane/routing.ts";

const ROUTING_CONFIG = { configRoot: "/opt/orchestrator/config" };

function runCommand(overrides: Partial<Extract<OrchestratorCommand, { kind: "run" }>> = {}): Extract<OrchestratorCommand, { kind: "run" }> {
  return {
    kind: "run",
    runId: "cp-run-1",
    projectPath: "/srv/projects/demo",
    inputs: [{ id: "task", path: "/srv/inputs/task.md" }],
    ...overrides,
  };
}

function expectRoutingError(command: unknown, reason: OrchestratorTransportReason, config: { configRoot: string; pipelineRoot?: string } = ROUTING_CONFIG): void {
  expect(() => routeOrchestratorCommand(command as OrchestratorCommand, config)).toThrow(OrchestratorSubprocessClientError);
  try {
    routeOrchestratorCommand(command as OrchestratorCommand, config);
  } catch (error) {
    expect(error instanceof OrchestratorSubprocessClientError).toBe(true);
    expect((error as OrchestratorSubprocessClientError).reason).toBe(reason);
  }
}

describe("control plane routing", () => {
  test("run with every optional field routes the exact argv", () => {
    const routed = routeOrchestratorCommand(
      runCommand({ pipelineRoot: "/srv/pipelines/custom", launcherId: "dhl_runner" }),
      ROUTING_CONFIG,
    );
    expect(routed.argv).toEqual([
      "run",
      "--run-id",
      "cp-run-1",
      "--pipeline-root",
      "/srv/pipelines/custom",
      "--config-root",
      "/opt/orchestrator/config",
      "--project",
      "/srv/projects/demo",
      "--input",
      "task=/srv/inputs/task.md",
      "--launcher-id",
      "dhl_runner",
      "--json",
    ]);
    expect(routed.expectedRunId).toBe("cp-run-1");
  });

  test("anonymous run without any pipeline root keeps the bundled default-v2 behavior", () => {
    const routed = routeOrchestratorCommand(
      runCommand({ runId: undefined, pipelineRoot: undefined }),
      { configRoot: "/opt/orchestrator/config" },
    );
    expect(routed.argv).toEqual([
      "run",
      "--config-root",
      "/opt/orchestrator/config",
      "--project",
      "/srv/projects/demo",
      "--input",
      "task=/srv/inputs/task.md",
      "--json",
    ]);
    expect(routed.expectedRunId).toBe("");
  });

  test("run pipeline root comes from the routing config when the command omits it", () => {
    const routed = routeOrchestratorCommand(
      runCommand({ pipelineRoot: undefined }),
      { configRoot: "/opt/orchestrator/config", pipelineRoot: "/srv/pipelines/default" },
    );
    expect(routed.argv).toContain("--pipeline-root");
    expect(routed.argv.indexOf("--pipeline-root")).toBeGreaterThanOrEqual(0);
    expect(routed.argv[routed.argv.indexOf("--pipeline-root") + 1]).toBe("/srv/pipelines/default");
  });

  test("explicit command pipeline root wins over the routing config", () => {
    const routed = routeOrchestratorCommand(
      runCommand({ pipelineRoot: "/srv/pipelines/explicit" }),
      { configRoot: "/opt/orchestrator/config", pipelineRoot: "/srv/pipelines/default" },
    );
    const flagIndex = routed.argv.indexOf("--pipeline-root");
    expect(routed.argv[flagIndex + 1]).toBe("/srv/pipelines/explicit");
  });

  test("multiple inputs render in declaration order", () => {
    const inputs: readonly OrchestratorRunInput[] = [
      { id: "task", path: "/srv/inputs/task.md" },
      { id: "spec", path: "/srv/inputs/spec.json" },
      { id: "notes", path: "/srv/inputs/notes.txt" },
    ];
    const routed = routeOrchestratorCommand(runCommand({ inputs }), ROUTING_CONFIG);
    const firstInput = routed.argv.indexOf("--input");
    expect(routed.argv.slice(firstInput, firstInput + 6)).toEqual([
      "--input",
      "task=/srv/inputs/task.md",
      "--input",
      "spec=/srv/inputs/spec.json",
      "--input",
      "notes=/srv/inputs/notes.txt",
    ]);
  });

  test("inspect routes read-only flags only", () => {
    const routed = routeOrchestratorCommand({ kind: "inspect", runId: "cp-run-1" }, ROUTING_CONFIG);
    expect(routed.argv).toEqual(["inspect", "--run-id", "cp-run-1", "--config-root", "/opt/orchestrator/config", "--json"]);
    expect(routed.argv.some((arg) => arg.startsWith("--project") || arg.startsWith("--pipeline") || arg.startsWith("--stage") || arg.startsWith("--action"))).toBe(false);
  });

  test("resume routes the exact argv with an optional launcher id", () => {
    expect(routeOrchestratorCommand({ kind: "resume", runId: "cp-run-1" }, ROUTING_CONFIG).argv).toEqual([
      "resume",
      "--run-id",
      "cp-run-1",
      "--config-root",
      "/opt/orchestrator/config",
      "--json",
    ]);
    expect(routeOrchestratorCommand({ kind: "resume", runId: "cp-run-1", launcherId: "dhl_runner" }, ROUTING_CONFIG).argv).toEqual([
      "resume",
      "--run-id",
      "cp-run-1",
      "--config-root",
      "/opt/orchestrator/config",
      "--launcher-id",
      "dhl_runner",
      "--json",
    ]);
  });

  test("resume_plan routes stage and budget flags in grammar order", () => {
    const routed = routeOrchestratorCommand(
      { kind: "resume_plan", runId: "cp-run-1", stageId: "stage-1", initialBudget: 1 },
      ROUTING_CONFIG,
    );
    expect(routed.argv).toEqual([
      "resume-plan",
      "--run-id",
      "cp-run-1",
      "--stage-id",
      "stage-1",
      "--initial-budget",
      "1",
      "--config-root",
      "/opt/orchestrator/config",
      "--json",
    ]);
  });

  test("continue_stage routes wait index and additional iterations in grammar order", () => {
    const routed = routeOrchestratorCommand(
      { kind: "continue_stage", runId: "cp-run-1", waitIndex: 2, additionalIterations: 3 },
      ROUTING_CONFIG,
    );
    expect(routed.argv).toEqual([
      "continue-stage",
      "--run-id",
      "cp-run-1",
      "--wait-index",
      "2",
      "--additional-iterations",
      "3",
      "--config-root",
      "/opt/orchestrator/config",
      "--json",
    ]);
  });

  test("revise_task routes the task file path in grammar order", () => {
    const routed = routeOrchestratorCommand(
      { kind: "revise_task", runId: "cp-run-1", waitIndex: 2, taskId: "task-a", taskFile: "/srv/policy/revise.md" },
      ROUTING_CONFIG,
    );
    expect(routed.argv).toEqual([
      "revise-task",
      "--run-id",
      "cp-run-1",
      "--wait-index",
      "2",
      "--task-id",
      "task-a",
      "--task-file",
      "/srv/policy/revise.md",
      "--config-root",
      "/opt/orchestrator/config",
      "--json",
    ]);
  });

  test("respond routes no config root and no launcher id", () => {
    const routed = routeOrchestratorCommand(
      { kind: "respond", runId: "cp-run-1", waitIndex: 1, actionId: "continue_stage" },
      ROUTING_CONFIG,
    );
    expect(routed.argv).toEqual(["respond", "--run-id", "cp-run-1", "--wait-index", "1", "--action", "continue_stage", "--json"]);
  });

  test("special characters stay single argv elements without interpolation", () => {
    const routed = routeOrchestratorCommand(
      runCommand({
        projectPath: "/srv/projects/sp ace",
        inputs: [{ id: "task", path: "/srv/inputs/weird=$(rm -rf);`x`'q'.md" }],
      }),
      ROUTING_CONFIG,
    );
    const projectIndex = routed.argv.indexOf("--project");
    expect(routed.argv[projectIndex + 1]).toBe("/srv/projects/sp ace");
    const inputIndex = routed.argv.indexOf("--input");
    expect(routed.argv[inputIndex + 1]).toBe("task=/srv/inputs/weird=$(rm -rf);`x`'q'.md");
    expect(routed.argv.filter((arg) => arg === "/srv/projects/sp ace")).toHaveLength(1);
  });

  test("input values containing = stay one element and survive the first-= split", () => {
    const routed = routeOrchestratorCommand(
      runCommand({ inputs: [{ id: "task", path: "/srv/inputs/a=b=c.md" }] }),
      ROUTING_CONFIG,
    );
    const inputIndex = routed.argv.indexOf("--input");
    expect(routed.argv[inputIndex + 1]).toBe("task=/srv/inputs/a=b=c.md");
  });

  test("unsafe scalars and paths are rejected before any spawn", () => {
    expectRoutingError({ kind: "inspect", runId: "../escape" }, "invalid_command");
    expectRoutingError({ kind: "inspect", runId: "" }, "invalid_command");
    expectRoutingError({ kind: "resume", runId: "ok-id", launcherId: "nope" }, "invalid_command");
    expectRoutingError({ kind: "resume", runId: "ok-id", launcherId: "dhl_" }, "invalid_command");
    expectRoutingError({ kind: "resume_plan", runId: "ok-id", stageId: "BAD ID", initialBudget: 1 }, "invalid_command");
    expectRoutingError({ kind: "resume_plan", runId: "ok-id", stageId: "stage-1", initialBudget: 0 }, "invalid_command");
    expectRoutingError({ kind: "resume_plan", runId: "ok-id", stageId: "stage-1", initialBudget: 1.5 }, "invalid_command");
    expectRoutingError({ kind: "resume_plan", runId: "ok-id", stageId: "stage-1", initialBudget: Number.MAX_SAFE_INTEGER + 1 }, "invalid_command");
    expectRoutingError({ kind: "continue_stage", runId: "ok-id", waitIndex: 0, additionalIterations: 1 }, "invalid_command");
    expectRoutingError({ kind: "continue_stage", runId: "ok-id", waitIndex: 1, additionalIterations: -1 }, "invalid_command");
    expectRoutingError({ kind: "revise_task", runId: "ok-id", waitIndex: 1, taskId: "t", taskFile: "relative/path.md" }, "invalid_command");
    expectRoutingError({ kind: "respond", runId: "ok-id", waitIndex: 1, actionId: "bad id" }, "invalid_command");
    expectRoutingError({ kind: "unknown_kind", runId: "ok" }, "invalid_command");
    expectRoutingError("not an object", "invalid_command");
    expectRoutingError({ kind: "run", projectPath: "relative", inputs: [] }, "invalid_command");
    expectRoutingError(runCommand({ projectPath: "" }), "invalid_command");
    expectRoutingError(runCommand({ runId: "bad/id" }), "invalid_command");
    expectRoutingError(runCommand({ inputs: [{ id: "", path: "/srv/x" }] }), "invalid_command");
    expectRoutingError(runCommand({ inputs: [{ id: "task", path: "nope" }] }), "invalid_command");
    expectRoutingError(runCommand({ inputs: [{ id: "task", path: "/a" }, { id: "task", path: "/b" }] }), "invalid_command");
    expectRoutingError({ kind: "run", projectPath: "/p", inputs: [], extra: "hostile" }, "invalid_command");
    expectRoutingError(runCommand(), "invalid_client_config", { configRoot: "relative" });
    expectRoutingError(runCommand(), "invalid_client_config", { configRoot: "/c", pipelineRoot: "relative" });
  });

  test("missing required fields are rejected", () => {
    expectRoutingError({ kind: "inspect" }, "invalid_command");
    expectRoutingError({ kind: "resume_plan", runId: "ok-id", stageId: "stage-1" }, "invalid_command");
    expectRoutingError({ kind: "continue_stage", runId: "ok-id", waitIndex: 1 }, "invalid_command");
    expectRoutingError({ kind: "revise_task", runId: "ok-id", waitIndex: 1, taskId: "t" }, "invalid_command");
    expectRoutingError({ kind: "respond", runId: "ok-id", waitIndex: 1 }, "invalid_command");
    expectRoutingError({ kind: "run", inputs: [] }, "invalid_command");
  });

  test("hostile proxy extras are never read and known fields are read exactly once", () => {
    const reads: string[] = [];
    const target = {
      kind: "run",
      runId: "cp-run-1",
      projectPath: "/srv/projects/demo",
      inputs: [{ id: "task", path: "/srv/inputs/task.md" }],
    };
    const hostile = new Proxy(target, {
      get(target, property) {
        if (typeof property === "string") {
          reads.push(property);
          return (target as unknown as Record<string, unknown>)[property];
        }
        return undefined;
      },
      has() {
        return true;
      },
      ownKeys() {
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor(target, property) {
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    const routed = routeOrchestratorCommand(hostile as unknown as OrchestratorCommand, ROUTING_CONFIG);
    expect(routed.argv[0]).toBe("run");
    const expectedReads = ["kind", "runId", "projectPath", "inputs"];
    for (const field of expectedReads) {
      expect(reads.filter((read) => read === field)).toHaveLength(1);
    }
    expect(reads.some((read) => read === "extra" || read === "launcherId" || read === "pipelineRoot")).toBe(false);
  });

  test("the error class keeps its reason immutable and content-free", () => {
    const error = new OrchestratorSubprocessClientError("invalid_command", "runId must be a safe identifier");
    expect(error.reason).toBe("invalid_command");
    expect(() => {
      (error as { reason: string }).reason = "spawn_failed";
    }).toThrow();
    expect(error.message).not.toContain("/srv");
    expect(Object.keys(error)).toContain("reason");
  });
});
