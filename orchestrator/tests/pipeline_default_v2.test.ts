/**
 * The bundled pipeline v2 default (`pipelines/default-v2`): the actual
 * tracked bundle is the production input of `orchestrator run` without an
 * explicit `--pipeline-root`. This suite pins the bundle's contracts, the
 * CLI default selection, the packaging assumptions, and the full
 * operator-facing chain through the public CLI.
 */
import { expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { runCli, type CliIo } from "../src/main.ts";
import type { CliResult } from "../src/docker_helper.ts";
import { DEFAULT_PIPELINE_ROOT, DEFAULT_PIPELINE_V2_ROOT, parseCommand, usage } from "../src/cli_args.ts";
import { runPipelineV2, resumePipelineV2PlanningRunPlan } from "../src/pipeline_v2_runner.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { loadPipeline } from "../src/pipeline.ts";
import { pipelineV2ExecutionDigest, pipelineV2ExecutionSnapshotJson, pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { compiledExecutionRoleFor, compiledStageTemplateFor } from "../src/pipeline_v2_orchestration.ts";
import { prepareRunProject, snapshotRunInputs, type RunInputBinding, type RunInputsSnapshot } from "../src/pipeline_v2_runtime.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";

const REPO_ROOT = join(import.meta.dir, "..", "..");
const V2_BUNDLE = join(REPO_ROOT, "pipelines", "default-v2");
const V1_BUNDLE = join(REPO_ROOT, "pipelines", "default");
const LAUNCHER_ID = "dhl_defaultv2";

// ---------------------------------------------------------------------------
// The tracked bundle: loader, determinism, assets, bindings, portability.
// ---------------------------------------------------------------------------

test("the bundled v2 default loads, digests deterministically, and stays portable", async () => {
  expect(existsSync(join(V2_BUNDLE, "pipeline.yaml"))).toBe(true);
  const pipeline = await loadPipelineV2(V2_BUNDLE);
  const again = await loadPipelineV2(V2_BUNDLE);
  expect(pipelineV2ExecutionDigest(pipeline)).toBe(pipelineV2ExecutionDigest(again));
  expect(pipelineV2ExecutionSnapshotJson(pipeline)).toBe(pipelineV2ExecutionSnapshotJson(again));

  const copyRoot = mkdtempSync(join(tmpdir(), "default-v2-port-"));
  try {
    const moved = join(copyRoot, "default-v2");
    const copied = spawnSync("cp", ["-a", `${V2_BUNDLE}/.`, moved]);
    expect(copied.status).toBe(0);
    const second = await loadPipelineV2(moved);
    expect(pipelineV2ExecutionDigest(second)).toBe(pipelineV2ExecutionDigest(pipeline));
    expect(second.bundleRoot).toBe(moved);
    expect(pipelineV2RunPipelineIdentity(second).bundle_root).toBe(moved);
  } finally {
    rmSync(copyRoot, { recursive: true, force: true });
  }
});

test("every referenced bundle asset exists", async () => {
  for (const path of [
    "pipeline.yaml",
    "prompts/architect.md",
    "prompts/execute.md",
    "schemas/plan.schema.json",
    "schemas/agent-result-v2.schema.json",
  ]) {
    expect(existsSync(join(V2_BUNDLE, path))).toBe(true);
  }
});

test("compiled roles: architect is the planning state with the exact plan_output and trusted stage_wait; execute is the development stage", async () => {
  const pipeline = await loadPipelineV2(V2_BUNDLE);
  const planner = compiledExecutionRoleFor(pipeline, "architect");
  expect(planner.role).toBe("planning");
  if (planner.role !== "planning") {
    throw new Error("unreachable");
  }
  expect(planner.plan_output).toBe("plan");
  expect(planner.stage_wait.reason).toBe("stage_iteration_completed");
  expect([...planner.stage_wait.actions]).toEqual(["continue_stage", "revise_task"]);
  expect(compiledStageTemplateFor(pipeline, "development")).toEqual({
    id: "development",
    entry_state: "execute",
    state_ids: ["execute"],
  });
  const stage = compiledExecutionRoleFor(pipeline, "execute");
  expect(stage.role).toBe("stage");
  if (stage.role !== "stage") {
    throw new Error("unreachable");
  }
  expect(stage.stage_template).toBe("development");
});

test("the planning output is a JSON proposal port and the stage output is a JSON result port", async () => {
  const pipeline = await loadPipelineV2(V2_BUNDLE);
  const architect = pipeline.states.filter((state) => state.id === "architect")[0]!;
  const execute = pipeline.states.filter((state) => state.id === "execute")[0]!;
  if (architect.type !== "agent" || execute.type !== "agent") {
    throw new Error("unreachable");
  }
  expect(architect.outputs.map((output) => ({ id: output.id, type: output.type }))).toEqual([
    { id: "plan", type: "json" },
  ]);
  expect(architect.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "task", source: { pipeline_input: "task" } },
  ]);
  expect(execute.outputs.map((output) => ({ id: output.id, type: output.type }))).toEqual([
    { id: "result", type: "json" },
  ]);
  expect(execute.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "task", source: { pipeline_input: "task" } },
  ]);
  // the run-level result is required and bound exactly to execute/result
  expect(pipeline.outputs).toEqual([
    {
      id: "result",
      required: true,
      source: { state_output: { state: "execute", output: "result" } },
      type: "json",
      schema: execute.outputs[0]!.schema,
    },
  ]);
  // both activations read the same protected task input
  expect(pipeline.inputs.map((input) => ({ id: input.id, type: input.type, protected: input.protected }))).toEqual([
    { id: "task", type: "file", protected: true },
  ]);
});

test("the v2-native result schema drops the legacy identity fields and keeps the v1 constraints", async () => {
  const schemaText = await readFile(join(V2_BUNDLE, "schemas", "agent-result-v2.schema.json"), "utf8");
  const schema = JSON.parse(schemaText) as Record<string, unknown>;
  expect(schema["additionalProperties"]).toBe(false);
  expect(schema["required"]).toEqual(["schema_version", "status", "summary", "artifacts"]);
  const properties = schema["properties"] as Record<string, Record<string, unknown>>;
  expect(properties["schema_version"]).toEqual({ const: 3 });
  expect(properties["status"]).toEqual({ const: "completed" });
  expect(properties["summary"]).toEqual({ type: "string", minLength: 1, pattern: "\\S" });
  expect(properties["artifacts"]).toEqual({ type: "array", items: { type: "string" } });
  for (const legacy of ["run_id", "state_id", "activation_index", "attempt"]) {
    expect(schemaText).not.toContain(`"${legacy}"`);
  }
  // the prompts must not demand the legacy identity fields either
  const executePrompt = await readFile(join(V2_BUNDLE, "prompts", "execute.md"), "utf8");
  for (const legacy of ["run_id", "activation_index", "attempt"]) {
    expect(executePrompt).not.toContain(legacy);
  }
  // the v1 schema stays the v1 contract
  const v1Schema = JSON.parse(await readFile(join(V1_BUNDLE, "schemas", "agent-result.schema.json"), "utf8")) as Record<string, unknown>;
  expect(v1Schema["required"]).toContain("run_id");
  expect(v1Schema["required"]).toContain("activation_index");
});

test("the planning prompt demands the documented deterministic proposal; the prompts use only declared output paths", async () => {
  const architectPrompt = await readFile(join(V2_BUNDLE, "prompts", "architect.md"), "utf8");
  expect(architectPrompt).toContain("/pipeline/outputs/plan");
  expect(architectPrompt).toContain('"stage-1"');
  expect(architectPrompt).toContain('"development"');
  expect(architectPrompt).toContain('"task-1"');
  expect(architectPrompt).toContain("run_plan_proposal");
  expect(architectPrompt).toContain('"depends_on"');
  expect(architectPrompt).toContain("resume-plan --stage-id stage-1 --initial-budget 1");
  expect(architectPrompt).toContain("/pipeline/inputs/task");
  expect(architectPrompt).not.toContain("/pipeline/outputs/result");
  const executePrompt = await readFile(join(V2_BUNDLE, "prompts", "execute.md"), "utf8");
  expect(executePrompt).toContain("/pipeline/outputs/result");
  expect(executePrompt).toContain("schema_version");
  expect(executePrompt).toContain('"status": "completed"');
  expect(executePrompt).toContain("artifacts");
  expect(executePrompt).toContain("/pipeline/inputs/task");
  expect(executePrompt).not.toContain("/pipeline/outputs/plan");
  expect(executePrompt).toContain("Do not write `result.json` anywhere");
  expect(executePrompt).not.toContain("run_plan_proposal");
  // no prompt demands a legacy v1 result contract
  expect(executePrompt).not.toContain("Write the structured result");
});

test("the v1 default bundle is preserved: the v1 loader accepts it and the v2 loader rejects it", async () => {
  const v1 = await loadPipeline(V1_BUNDLE);
  expect(v1.states.map((state) => state.id)).toEqual(["execute", "completed"]);
  expect(v1.schema_version).toBe(1);
  const v1Prompt = await readFile(join(V1_BUNDLE, "prompts", "execute.md"), "utf8");
  expect(v1Prompt).toContain("structured result");
  expect(v1Prompt).toContain("result path");
  let v2Error = "";
  try {
    await loadPipelineV2(V1_BUNDLE);
  } catch (cause) {
    v2Error = cause instanceof Error ? cause.message : String(cause);
  }
  expect(v2Error).toBe('pipeline is missing required field "outputs"');
  // the v2 default bundle is rejected by the v1 loader (never a silent swap)
  let v1Error = "";
  try {
    await loadPipeline(V2_BUNDLE);
  } catch (cause) {
    v1Error = cause instanceof Error ? cause.message : String(cause);
  }
  expect(v1Error).toBe("pipeline schema version 2 is not executable yet");
});

test("the data plane works on the bundled v2 default", async () => {
  const pipeline = await loadPipelineV2(V2_BUNDLE);
  const root = mkdtempSync(join(tmpdir(), "default-v2-data-"));
  try {
    const sources = join(root, "userdata");
    mkdirSync(sources, { recursive: true });
    const taskPath = join(sources, "task.md");
    writeFileSync(taskPath, "BUNDLED-DEFAULT-TASK\n", { mode: 0o600 });
    const projectSource = join(root, "project-source");
    mkdirSync(projectSource, { recursive: true });
    const runRoot = join(root, "run");
    mkdirSync(runRoot, { recursive: true });
    await prepareRunProject(projectSource, runRoot);
    const runInputs: RunInputsSnapshot = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: taskPath }] as readonly RunInputBinding[],
      runRoot,
    );
    expect(runInputs.inputs).toHaveLength(1);
    expect(runInputs.inputs[0]!.protected).toBe(true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// CLI grammar: the run default selection and unchanged explicit behavior.
// ---------------------------------------------------------------------------

test("run without --pipeline-root resolves the bundled v2 default in the parser", () => {
  const parsed = parseCommand("run", ["--config-root=/c", "--project=/pr"]);
  expect(parsed.kind).toBe("run");
  if (parsed.kind !== "run") {
    throw new Error("unreachable");
  }
  expect(parsed.pipelineRoot).toBe("/opt/orchestrator/pipelines/default-v2");
  expect(DEFAULT_PIPELINE_V2_ROOT).toBe("/opt/orchestrator/pipelines/default-v2");
  // the v1 default constant is unchanged and still belongs to agent-smoke
  expect(DEFAULT_PIPELINE_ROOT).toBe("/opt/orchestrator/pipelines/default");
  const agentSmoke = parseCommand("agent-smoke", ["--config-root=/c"]);
  expect(agentSmoke.kind === "agent-smoke" && agentSmoke.pipelineRoot).toBe(DEFAULT_PIPELINE_ROOT);
});

test("run with an explicit --pipeline-root keeps the exact previous behavior", () => {
  const parsed = parseCommand("run", ["--pipeline-root", "/custom/root", "--config-root=/c", "--project=/pr"]);
  if (parsed.kind !== "run") {
    throw new Error("unreachable");
  }
  expect(parsed.pipelineRoot).toBe("/custom/root");
  // malformed explicit values are still rejected before any resolver/auth/run
  expect(() => parseCommand("run", ["--pipeline-root", "relative", "--config-root=/c", "--project=/pr"])).toThrow(
    "--pipeline-root must be an absolute path",
  );
  expect(() => parseCommand("run", ["--pipeline-root=/a", "--pipeline-root=/b", "--config-root=/c", "--project=/pr"])).toThrow(
    "--pipeline-root may be given at most once",
  );
});

test("the usage documents the optional flag, the bundled v2 default and the manual continuation", () => {
  const text = usage();
  expect(text).toContain("[--pipeline-root ABS]");
  expect(text).toContain(DEFAULT_PIPELINE_V2_ROOT);
  expect(text).toContain("planReady");
  expect(text).toContain("resume-plan --stage-id stage-1");
  expect(text).toContain("orchestrator resume-plan --stage-id stage-1");
  expect(text).toContain(DEFAULT_PIPELINE_ROOT);
  expect(text).toContain("no automatic stage/budget selection");
});

// ---------------------------------------------------------------------------
// Packaging: the whole pipelines/ tree ships; the installed path is the
// CLI constant. No second COPY and no Dockerfile change.
// ---------------------------------------------------------------------------

test("the packaging assumption holds: the Dockerfile copies pipelines/ wholesale onto the CLI default path", async () => {
  const dockerfile = await readFile(join(REPO_ROOT, "Dockerfile_orchestrator"), "utf8");
  expect(dockerfile).toContain("COPY --chown=opencode:opencode \\");
  expect(dockerfile).toContain("./pipelines/");
  expect(dockerfile).toContain("/opt/orchestrator/pipelines/");
  expect(DEFAULT_PIPELINE_V2_ROOT).toBe("/opt/orchestrator/pipelines/default-v2");
  expect(DEFAULT_PIPELINE_ROOT).toBe("/opt/orchestrator/pipelines/default");
  expect(existsSync(V2_BUNDLE)).toBe(true);
});

// ---------------------------------------------------------------------------
// Production E2E of the tracked bundle through the public CLI.
// ---------------------------------------------------------------------------

const P1_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-1", depends_on: [] }] },
  ],
  new_tasks: [{ id: "task-1", body: "BUNDLED-DEFAULT-TASK: implement the documented default workflow" }],
};

let sessionCounter = 0;

interface ChainSetup {
  stateRoot: string;
  sessionIds: string[];
  sessionDeletes: string[];
  io: CliIo;
}

function makeChain(stateRoot: string, resultBody: Record<string, unknown>, credFile: string): ChainSetup {
  const sessionIds: string[] = [];
  const sessionDeletes: string[] = [];
  let workspace = "";
  const io: CliIo = {
    baseEnv: { HOME: "/home/u", DEFAULT_SOURCE_VAR_1: "default-secret" },
    runner: {
      run: async (args: string[]): Promise<CliResult> => {
        if (args[0] === "session" && args[1] === "create") {
          sessionCounter += 1;
          const id = `dhs_defv2_${sessionCounter}`;
          sessionIds.push(id);
          if (workspace === "") {
            workspace = args[args.length - 1] ?? "";
          }
          return {
            code: 0,
            stdout: JSON.stringify({
              ok: true,
              session: { id, launcher_id: LAUNCHER_ID },
              token: `dhc_${id}`,
            }),
          };
        }
        if (args[0] === "session" && args[1] === "delete") {
          sessionDeletes.push(args[args.length - 1] ?? "");
          return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
        }
        if (args[0] === "pull") {
          return { code: 0 };
        }
        if (args[0] === "run") {
          for (let i = 0; i < args.length; i += 1) {
            if (args[i] !== "--mount") {
              continue;
            }
            const spec = args[i + 1] ?? "";
            const first = spec.indexOf(":");
            const source = spec.slice(0, first);
            const rest = spec.slice(first + 1);
            const second = rest.indexOf(":");
            const target = second === -1 ? rest : rest.slice(0, second);
            if (target === "/pipeline/outputs" && workspace !== "") {
              const segments = source.split("/");
              const dash = segments[1] ?? "";
              const stateId = dash.slice(dash.indexOf("-") + 1);
              if (stateId === "architect") {
                writeFileSync(join(workspace, source, "plan"), JSON.stringify(P1_PROPOSAL), { mode: 0o600 });
              }
              if (stateId === "execute") {
                writeFileSync(join(workspace, source, "result"), `${JSON.stringify(resultBody)}\n`, { mode: 0o600 });
              }
            }
          }
          return { code: 0 };
        }
        return { code: 1 };
      },
      killActive: () => false,
    },
    fetchAuth: () =>
      Promise.resolve({ status: 200, body: { authority: "launcher", principal: "proof", launcher_id: LAUNCHER_ID } }),
    resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: credFile }),
    resolveStateRootProjection: () => ({ localRoot: stateRoot, daemonRoot: stateRoot }),
    runSmoke: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runAgentSmoke"],
    runPipelineV2: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runPipelineV2"],
    resumePipelineV2: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["resumePipelineV2"],
    continuePipelineV2Stage: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["continuePipelineV2Stage"],
    revisePipelineV2Task: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["revisePipelineV2Task"],
    resumePipelineV2PlanningRunPlan: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["resumePipelineV2PlanningRunPlan"],
    respondPipelineV2Wait: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["respondPipelineV2Wait"],
    readTaskFile: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["readTaskFile"],
    writeStdout: () => {},
    writeError: () => {},
  };
  return { stateRoot, sessionIds, sessionDeletes, io };
}

async function chainCommand(
  setup: ChainSetup,
  args: string[],
): Promise<{ exit: number; outcome: Record<string, unknown> }> {
  const stdout: string[] = [];
  const io = setup.io;
  const originalWrite = io.writeStdout;
  io.writeStdout = (text) => {
    stdout.push(text);
    originalWrite(text);
  };
  const exit = await runCli(args, io);
  const documents = stdout.filter((line) => line.trim() !== "");
  expect(documents).toHaveLength(1);
  return { exit, outcome: JSON.parse(documents[0]!) as Record<string, unknown> };
}

test("e2e: the tracked bundled v2 default runs planReady then succeeds through the public CLI", async () => {
  const root = mkdtempSync(join(tmpdir(), "default-v2-e2e-"));
  try {
    const stateRoot = join(root, "state");
    mkdirSync(stateRoot, { recursive: true });
    const sources = join(root, "userdata");
    mkdirSync(sources, { recursive: true });
    const taskPath = join(sources, "task.md");
    writeFileSync(taskPath, "BUNDLED-DEFAULT-TASK\n", { mode: 0o600 });
    const projectSource = join(root, "project-source");
    mkdirSync(projectSource, { recursive: true });
    const configRoot = join(root, "config");
    mkdirSync(join(configRoot, "profiles"), { recursive: true });
    mkdirSync(join(configRoot, "opencode"), { recursive: true });
    writeFileSync(
      join(configRoot, "profiles", "default.yaml"),
      [
        "schema_version: 1",
        "image: ghcr.io/example/worker:1",
        "opencode_config: opencode/default.json",
        "env:",
        "  MODEL_API_KEY:",
        "    from_env: DEFAULT_SOURCE_VAR_1",
        "    required: true",
        "",
      ].join("\n"),
    );
    writeFileSync(join(configRoot, "opencode", "default.json"), JSON.stringify({ model: "glm53-flash" }));
    const resultBody = {
      schema_version: 3,
      status: "completed",
      summary: `DEFAULT-V2-CANARY-${Math.random().toString(36).slice(2)}`,
      artifacts: ["work/product.txt"],
    };
    const resultBytes = `${JSON.stringify(resultBody)}\n`;

    // step 1: the fresh run with the explicit tracked bundle path
    writeFileSync(join(root, "cred.token"), "cred-token-not-real\n", { mode: 0o600 });
    const setup1 = makeChain(stateRoot, resultBody, join(root, "cred.token"));
    setup1.io.runPipelineV2 = runPipelineV2 as unknown as CliIo["runPipelineV2"];
    const runRecord = await chainCommand(setup1, [
      "run",
      "--pipeline-root", V2_BUNDLE,
      "--config-root", configRoot,
      "--project", projectSource,
      "--input", `task=${taskPath}`,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(runRecord.exit).toBe(0);
    expect(Object.keys(runRecord.outcome).sort()).toEqual(["exitCode", "ok", "planReady", "runId", "runRoot", "state"]);
    expect(runRecord.outcome["planReady"]).toBe(true);
    const runId = runRecord.outcome["runId"] as string;
    const runRoot = join(stateRoot, "pipeline-runs", runId);
    const before: PipelineV2RunState = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
    expect(before.revision).toBe(8);
    expect(before.executions).toHaveLength(1);
    expect(before.executions[0]).toMatchObject({ state_id: "architect", execution_role: "planning", phase: "cleanup_completed" });
    expect(before.transitions).toHaveLength(0);
    expect(before.waits).toHaveLength(0);

    // step 2: full restart; the pipeline comes only from the durable root
    const setup2 = makeChain(stateRoot, resultBody, join(root, "cred.token"));
    setup2.io.resumePipelineV2PlanningRunPlan = resumePipelineV2PlanningRunPlan as unknown as CliIo["resumePipelineV2PlanningRunPlan"];
    const resumeRecord = await chainCommand(setup2, [
      "resume-plan",
      "--run-id", runId,
      "--stage-id", "stage-1",
      "--initial-budget", "1",
      "--config-root", configRoot,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(resumeRecord.exit).toBe(0);
    expect(Object.keys(resumeRecord.outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(resumeRecord.outcome["ok"]).toBe(true);

    const state: PipelineV2RunState = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
    expect(state.revision).toBe(24);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    // task/plan acceptance and the initial generation/iteration/transition
    expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-1@1"]);
    expect(state.plan_revisions.map((record) => record.revision)).toEqual([1]);
    expect(state.generations).toHaveLength(1);
    expect(state.generations[0]).toMatchObject({
      index: 1,
      stage_id: "stage-1",
      stage_position: 1,
      template_id: "development",
      initial_budget: 1,
      opened_transition_count: 0,
    });
    // the stage execution and its accepted result
    const stageExecution = state.executions[1]!;
    expect(stageExecution).toMatchObject({ index: 2, state_id: "execute", execution_role: "stage", phase: "cleanup_completed" });
    const stageOutputs = (stageExecution as { outputs?: Array<{ id: string; digest: string }> }).outputs ?? [];
    expect(stageOutputs).toHaveLength(1);
    expect(stageOutputs[0]!.id).toBe("result");
    expect(state.transitions).toEqual([
      { index: 0, from: "architect", outcome: "completed", to: "execute", execution_index: 1 },
      { index: 0, from: "execute", outcome: "completed", to: "done", execution_index: 2 },
    ]);
    // the terminal publication is exactly the stage result
    expect(state.run_outputs).toEqual([
      { id: "result", type: "json", required: true, present: true, digest: state.run_outputs![0]!.present === true ? state.run_outputs![0]!.digest : "" },
    ]);
    const publishedDir = join(runRoot, "outputs");
    expect(readdirSync(publishedDir).sort()).toEqual(["result"]);
    const publishedBytes = await readFile(join(publishedDir, "result"), "utf8");
    expect(publishedBytes).toBe(resultBytes);
    expect((JSON.parse(publishedBytes) as Record<string, unknown>)["summary"]).toBe(resultBody["summary"]);
    // the planning proposal is never the user terminal result
    expect(existsSync(join(publishedDir, "plan"))).toBe(false);
    // the published result is reachable through the outcome coordinates
    expect(resumeRecord.outcome["runRoot"]).toBe(runRoot);
    expect(existsSync(join(runRoot, "outputs", "result"))).toBe(true);
    // no legacy result.json anywhere in the run tree
    const scanForLegacy = (dir: string): number => {
      let count = 0;
      for (const entry of readdirSync(dir)) {
        const path = join(dir, entry);
        if (entry === "result.json") {
          count += 1;
        } else if (statDirSafe(path)) {
          count += scanForLegacy(path);
        }
      }
      return count;
    };
    expect(scanForLegacy(runRoot)).toBe(0);
    // loader round-trip and durable identity
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
    const sink = await PipelineV2RunStateSink.open({ stateRoot, runId });
    expect(sink.snapshot).toEqual(state);
    const reloaded = await loadPipelineV2(state.pipeline.bundle_root);
    expect(reloaded.bundleRoot).toBe(V2_BUNDLE);
    expect(pipelineV2RunPipelineIdentity(reloaded)).toEqual(state.pipeline);
    expect(state.waits).toHaveLength(0);
    // two activations, each session pair deleted exactly once, tool first
    expect(setup2.sessionIds).toHaveLength(2);
    expect(setup2.sessionDeletes).toEqual([setup2.sessionIds[1]!, setup2.sessionIds[0]!]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

function statDirSafe(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

test("e2e: run without --pipeline-root dispatches the bundled v2 default path to the runner", async () => {
  // The dispatcher proof: the exact default path reaches runPipelineV2
  // through the real CLI parser with no environment override and without
  // writing anything (the runner fake returns a plain outcome; the state
  // root is a throwaway temp directory).
  const stateRoot = mkdtempSync(join(tmpdir(), "default-v2-dispatch-"));
  try {
    let capturedPipelineRoot = "";
    const io: CliIo = {
      baseEnv: {},
      runner: { run: async (): Promise<CliResult> => ({ code: 0 }), killActive: () => false },
      fetchAuth: async () => ({ status: 200, body: {} }),
      resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: "/creds/token" }),
      resolveStateRootProjection: () => ({ localRoot: stateRoot, daemonRoot: stateRoot }),
      runSmoke: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["runSmoke"],
      runAgentSmoke: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["runAgentSmoke"],
      runPipelineV2: (async (options: unknown) => {
        capturedPipelineRoot = (options as { pipelineRoot: string }).pipelineRoot;
        return { ok: false, exitCode: 0, runId: "", runRoot: null, state: null, waiting: true, planReady: true } as never;
      }) as unknown as CliIo["runPipelineV2"],
      resumePipelineV2: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["resumePipelineV2"],
      continuePipelineV2Stage: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["continuePipelineV2Stage"],
      revisePipelineV2Task: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["revisePipelineV2Task"],
      resumePipelineV2PlanningRunPlan: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["resumePipelineV2PlanningRunPlan"],
      respondPipelineV2Wait: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["respondPipelineV2Wait"],
      readTaskFile: (async () => {
        throw new Error("fake not configured");
      }) as unknown as CliIo["readTaskFile"],
      writeStdout: () => {},
      writeError: () => {},
    };
    const exit = await runCli(["run", "--config-root=/c", "--project=/pr"], io);
    expect(capturedPipelineRoot).toBe("/opt/orchestrator/pipelines/default-v2");
    expect(exit).toBe(0);
    expect(readdirSync(stateRoot)).toEqual([]);
  } finally {
    rmSync(stateRoot, { recursive: true, force: true });
  }
});
