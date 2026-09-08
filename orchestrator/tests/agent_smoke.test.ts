import { link, mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, spyOn, test } from "bun:test";
import {
  parseAgentResult,
  verifyAgentResult,
  STANDARD_AGENT_RESULT_SCHEMA,
  type AgentResult,
} from "../src/agent_result.ts";
import { agentInstruction, agentWorkerSpec, pullArgs, runArgs } from "../src/worker.ts";
import {
  runAgentSmoke,
  agentRunDirPath,
  agentResultFilePath,
  executionDocumentPath,
  type AgentSmokeDeps,
} from "../src/agent_smoke.ts";
import { childSessionEnv, signalExitCode, type LifecycleDeps } from "../src/lifecycle.ts";
import { loadPipeline } from "../src/pipeline.ts";
import { pipelineExecutionDigest } from "../src/pipeline_digest.ts";
import { pipelineRunStatePath } from "../src/pipeline_state_store.ts";
import type { CliResult, CliRunner } from "../src/docker_helper.ts";
import { gateIoAtRename, faultIo } from "./state_io_test_helpers.ts";
import { PipelineStateStoreError } from "../src/pipeline_state_store.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";
const SOCKET = "/run/docker-helper/test.sock";
const INPUT_MARKER = "SECRET-INPUT-MARKER-42";
const INPUT_BODY = `# Task ${INPUT_MARKER}\n\nCreate the work product.\n`;
const WORK_PRODUCT_PATH = ".pipeline-agent-smoke/work-product.txt";
const WORK_PRODUCT_BODY = "opencode-agent-smoke-ok\n";
const CANARY = "CANARY_AMBIENT_VAR";
const CANARY_VALUE = "must-never-reach-the-worker";
const COMPLEX_LLM_SERVER = "https://llm.example/v1? a=b \"c\"";
const PROMPT_MARKER = "PROMPT-MARKER-77";

const LAUNCHER_SECRET = "dhc_launcher_secret_value";
const ADMIN_SECRET = "dha_admin_secret_value";
const STATE_PATH = "/host/orchestrator-state";

const PROFILE_IMAGE = "gitreg.example/opencode-docker/base:latest";
const OPENCODE_CONFIG = '{"$schema":"https://opencode.ai/config.json","model":"test/model"}';

const BASE_ENV = {
  HOME: "/home/opencode",
  XDG_CONFIG_HOME: "/uat-cred",
  XDG_STATE_HOME: "/xdg-state",
  XDG_RUNTIME_DIR: "/run/user/1000",
  LLM_SERVER: COMPLEX_LLM_SERVER,
  LLM_KEY: "sk-test-key",
  OPENCODE_ENABLE_EXA: "1",
  OPENCODE_EXPERIMENTAL_LSP_TOOL: "true",
  [CANARY]: CANARY_VALUE,
  DOCKER_HELPER_SESSION_TOKEN: "dht_launcher_session_token",
  DOCKER_HELPER_CREDENTIAL_TOKEN: LAUNCHER_SECRET,
  DOCKER_HELPER_ADMIN_TOKEN: ADMIN_SECRET,
  DOCKER_HELPER_STATE_PATH: STATE_PATH,
};

const CANONICAL_RESULT_SCHEMA = JSON.stringify(STANDARD_AGENT_RESULT_SCHEMA, null, 2);

const BUNDLE_PIPELINE_YAML = [
  "schema_version: 1",
  "entry_state: execute",
  "max_transitions: 1",
  "",
  "inputs:",
  "  - id: task",
  "    path: TASK.md",
  "    protected: true",
  "",
  "states:",
  "  - id: execute",
  "    type: agent",
  "    profile: default",
  "    prompt: prompts/execute.md",
  "    inputs:",
  "      - task",
  "    result_schema: schemas/agent-result.schema.json",
  "    timeout_seconds: 3600",
  "    max_attempts: 1",
  "    transitions:",
  "      - outcome: completed",
  "        to: completed",
  "",
  "  - id: completed",
  "    type: terminal",
  "    result: success",
  "",
].join("\n");

const PROMPT_BODY = [
  `# Implementation agent (marker ${PROMPT_MARKER})`,
  "",
  "Read the input file, do exactly the work it describes, and write the",
  "structured result as required by this document.",
  "",
].join("\n");

const BUNDLE_PROMPT = PROMPT_BODY;

const PROFILE_BODY = [
  "schema_version: 1",
  `image: ${PROFILE_IMAGE}`,
  "opencode_config: opencode/default.json",
  "env:",
  "  LLM_SERVER:",
  "    from_env: LLM_SERVER",
  "    required: true",
  "  LLM_KEY:",
  "    from_env: LLM_KEY",
  "    required: true",
  "  OPENCODE_ENABLE_EXA:",
  "    from_env: OPENCODE_ENABLE_EXA",
  "    required: false",
  "  OPENCODE_EXPERIMENTAL_LSP_TOOL:",
  "    from_env: OPENCODE_EXPERIMENTAL_LSP_TOOL",
  "    required: false",
  "",
].join("\n");

interface FakeAgentOptions {
  runCode?: number;
  pullCode?: number;
  resultBody?: string | ((runId: string) => string);
  createArtifacts?: boolean;
  modifyTask?: boolean;
  writeResult?: boolean;
  deleteCode?: number;
  createCode?: number;
  blockRun?: boolean;
  blockPull?: boolean;
  runTimeoutExpires?: boolean;
}

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: string;
  signalOnAbort: boolean;
  timeoutSeconds?: number;
}

function envPairValue(args: string[], name: string): string {
  for (let i = 1; i < args.length; i++) {
    if (args[i - 1] === "--env") {
      const pair = args[i] ?? "";
      if (pair.startsWith(`${name}=`)) {
        return pair.slice(name.length + 1);
      }
    }
  }
  return "";
}

function fakeCli(options: FakeAgentOptions & { workspace: string }) {
  const calls: RecordedCall[] = [];
  const events: string[] = [];
  let activeRun: { release: (code: number) => void } | null = null;
  let activePull: { release: () => void } | null = null;
  let notifyDeleteStart: (() => void) | null = null;
  const deleteStarted = new Promise<void>((resolve) => {
    notifyDeleteStart = resolve;
  });
  let notifyPullStart: (() => void) | null = null;
  const pullStarted = new Promise<void>((resolve) => {
    notifyPullStart = resolve;
  });

  const runner = async (
    args: string[],
    env: Record<string, string>,
    stdio: string,
    opts?: { signalOnAbort?: boolean; timeoutSeconds?: number },
  ): Promise<CliResult> => {
    calls.push({
      args: [...args],
      env: { ...env },
      stdio,
      signalOnAbort: opts?.signalOnAbort === true,
      timeoutSeconds: opts?.timeoutSeconds,
    });
    if (args[0] === "run") {
      events.push("run:start");
      if (options.blockRun === true) {
        return await new Promise<CliResult>((resolve) => {
          activeRun = {
            release: (code: number) => {
              events.push(`run:exited:${code}`);
              resolve({ code, stdout: "", stderr: "" });
            },
          };
        });
      }
      if (options.runTimeoutExpires === true) {
        events.push("run:timeout");
        return { code: 143, stdout: "", stderr: "", timedOut: true };
      }
      const runCode = options.runCode ?? 0;
      events.push(`run:exited:${runCode}`);
      const envPairs = args.filter((a, i) => args[i - 1] === "--env");
      const runId = envPairs.find((p) => p.startsWith("AGENT_SMOKE_RUN_ID="))?.slice("AGENT_SMOKE_RUN_ID=".length) ?? "";
      if ((options.modifyTask ?? false) === true) {
        const inputPair = envPairs.find((p) => p.startsWith("AGENT_SMOKE_INPUT_PATH=")) ?? "";
        const inputContainerPath = inputPair.slice("AGENT_SMOKE_INPUT_PATH=".length);
        const inputHost = join(options.workspace, inputContainerPath.replace(/^\/workspace\//, ""));
        await writeFile(inputHost, `${await readFile(inputHost, "utf8")}TAMPERED\n`);
      }
      if ((options.createArtifacts ?? true) === true) {
        await mkdir(join(options.workspace, ".pipeline-agent-smoke"), { recursive: true });
        await writeFile(join(options.workspace, WORK_PRODUCT_PATH), WORK_PRODUCT_BODY);
      }
      if ((options.writeResult ?? true) === true && runCode === 0) {
        const artifacts =
          (options.createArtifacts ?? true) === true ? [WORK_PRODUCT_PATH] : [];
        const bodyOrFactory =
          options.resultBody === undefined
            ? JSON.stringify({
                schema_version: 1,
                run_id: runId,
                status: "completed",
                summary: "created the work product",
                artifacts,
              })
            : options.resultBody;
        const body = typeof bodyOrFactory === "function" ? bodyOrFactory(runId) : bodyOrFactory;
        if (body !== "") {
          await mkdir(agentRunDirPath(options.workspace, runId), { recursive: true });
          await writeFile(agentResultFilePath(options.workspace, runId), `${body}\n`);
        }
      }
      return { code: runCode, stdout: "", stderr: "" };
    }
    if (args[0] === "pull") {
      events.push("pull:start");
      notifyPullStart?.();
      if (options.blockPull === true) {
        await new Promise<void>((resolve) => {
          activePull = { release: resolve };
        });
      }
      events.push("pull:done");
      return { code: options.pullCode ?? 0, stdout: "", stderr: "" };
    }
    if (args[0] === "session" && args[1] === "create") {
      events.push("session:create", "session:create-done");
      if ((options.createCode ?? 0) !== 0) {
        return { code: options.createCode ?? 1, stdout: "", stderr: "create boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: {
            id: CHILD_SESSION_ID,
            workspace: options.workspace,
            created_at: "t",
            expires_at: "t",
            launcher_id: LAUNCHER_ID,
          },
          token: CHILD_TOKEN,
        }),
        stderr: "",
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      events.push("session:delete");
      notifyDeleteStart?.();
      if ((options.deleteCode ?? 0) !== 0) {
        return { code: options.deleteCode ?? 1, stdout: "", stderr: "delete boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, id: CHILD_SESSION_ID, deleted: true }),
        stderr: "",
      };
    }
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };

  return {
    calls,
    events,
    runner,
    deleteStarted,
    pullStarted,
    releasePull: () => {
      const pull = activePull;
      activePull = null;
      pull?.release();
    },
    killActive: (signal: "SIGINT" | "SIGTERM") => {
      // mirrors the real runner's contract answer for tests: a delivery to the
      // active signalable worker run (pull and session ops are not signalable)
      if (activeRun !== null) {
        events.push(`run:signal:${signal}`);
        const release = activeRun.release;
        activeRun = null;
        release(signalExitCode(signal));
      }
      return true;
    },
  };
}

async function withFixture(
  fn: (dirs: {
    workspace: string;
    state: string;
    credentialFile: string;
    configRoot: string;
    profileFile: string;
    pipelineRoot: string;
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "agent-smoke-test-"));
  const workspace = join(root, "workspace");
  const state = join(root, "state");
  const configDir = join(root, "config", "docker-helper");
  const configRoot = join(root, "operator-config");
  const pipelineRoot = join(root, "pipeline-bundle");
  await mkdir(workspace, { recursive: true });
  await mkdir(state, { recursive: true });
  await mkdir(configDir, { recursive: true, mode: 0o700 });
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await mkdir(join(pipelineRoot, "prompts"), { recursive: true });
  await mkdir(join(pipelineRoot, "schemas"), { recursive: true });
  const credentialFile = join(configDir, "credential.token");
  await writeFile(credentialFile, `${LAUNCHER_TOKEN}\n`, { mode: 0o600 });
  await writeFile(join(workspace, "TASK.md"), INPUT_BODY);
  await writeFile(join(configRoot, "profiles", "default.yaml"), PROFILE_BODY);
  await writeFile(join(configRoot, "opencode", "default.json"), OPENCODE_CONFIG);
  await writeFile(join(pipelineRoot, "pipeline.yaml"), BUNDLE_PIPELINE_YAML);
  await writeFile(join(pipelineRoot, "prompts", "execute.md"), BUNDLE_PROMPT);
  await writeFile(join(pipelineRoot, "schemas", "agent-result.schema.json"), CANONICAL_RESULT_SCHEMA);
  try {
    await fn({
      workspace,
      state,
      credentialFile,
      configRoot,
      profileFile: join(configRoot, "profiles", "default.yaml"),
      pipelineRoot,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function makeDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  runner: CliRunner,
  overrides: Partial<AgentSmokeDeps> = {},
): AgentSmokeDeps {
  return {
    cli: runner,
    fetchAuth: async () => ({
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: LAUNCHER_ID },
    }),
    config: { socketPath: SOCKET, credentialFile: dirs.credentialFile },
    stateDirPath: dirs.state,
    baseEnv: BASE_ENV,
    ...overrides,
  };
}

function countingAuthDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  runner: CliRunner,
): { deps: AgentSmokeDeps; authCalls: number[] } {
  const authCalls: number[] = [];
  return {
    authCalls,
    deps: makeDeps(dirs, runner, {
      fetchAuth: async () => {
        authCalls.push(1);
        return {
          status: 200,
          body: { authority: "launcher", principal: "michael", launcher_id: LAUNCHER_ID },
        };
      },
    }),
  };
}

function agentSmokeOptions(dirs: { workspace: string; configRoot: string; pipelineRoot: string }) {
  return {
    workspace: dirs.workspace,
    configRoot: dirs.configRoot,
    pipelineRoot: dirs.pipelineRoot,
  };
}

function deleteCallCount(calls: RecordedCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length;
}

async function waitFor(desc: string, check: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${desc}`);
    }
    await Bun.sleep(5);
  }
}

function createCallCount(calls: RecordedCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "create").length;
}

function runCall(calls: RecordedCall[]): RecordedCall {
  const call = calls.find((c) => c.args[0] === "run");
  if (call === undefined) {
    throw new Error("no run call recorded");
  }
  return call;
}

function runEnvPairs(args: string[]): string[] {
  return args.filter((a, i) => args[i - 1] === "--env");
}

test("1. success: pipeline-driven agent run, result + artifacts verified, session cleaned up, exit 0", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.status).toBe("success");

    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(create.env.XDG_CONFIG_HOME).toBe("/uat-cred");
    expect(create.env.DOCKER_HELPER_SESSION_TOKEN).toBeUndefined();

    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.env).toEqual(childSessionEnv(CHILD_TOKEN));
    expect(run.signalOnAbort).toBe(true);
    expect(run.timeoutSeconds).toBe(3600);
    expect(run.stdio).toBe("inherit");
    expect(run.args[0]).toBe("run");
    expect(run.args[1]).toBe("--endpoint");
    expect(run.args[2]).toBe(SOCKET);
    expect(run.args[3]).toBe("--image");
    expect(run.args[4]).toBe(PROFILE_IMAGE);
    expect(run.args[5]).toBe("--entrypoint");
    expect(run.args[6]).toBe("opencode");
    expect(run.args[7]).toBe("--workdir");
    expect(run.args[8]).toBe("/workspace");
    expect(run.args[9]).toBe("--mount");
    expect(run.args[10]).toBe(".:/workspace");
    const envPairs = run.args.filter((a, i) => run.args[i - 1] === "--env");
    expect(envPairs).toEqual([
      `AGENT_SMOKE_INPUT_PATH=/workspace/TASK.md`,
      `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/${outcome.runId}/result.json`,
      `AGENT_SMOKE_RUN_ID=${outcome.runId}`,
      `DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`,
      "LLM_KEY=sk-test-key",
      `LLM_SERVER=${COMPLEX_LLM_SERVER}`,
      `OPENCODE_CONFIG_CONTENT=${OPENCODE_CONFIG}`,
      "OPENCODE_ENABLE_EXA=1",
      "OPENCODE_EXPERIMENTAL_LSP_TOOL=true",
    ]);
    const separator = run.args.indexOf("--");
    const instruction = run.args[run.args.length - 1]!;
    expect(run.args.slice(separator + 1)).toEqual([
      "run",
      "--format",
      "json",
      "--auto",
      instruction,
    ]);
    expect(instruction).toContain(
      `/workspace/.pipeline-agent-smoke/${outcome.runId}/execution.md`,
    );
    expect(instruction).not.toContain(PROMPT_MARKER);
    expect(instruction).not.toContain(INPUT_MARKER);
    expect(instruction).not.toContain(INPUT_BODY.trim());
    expect(run.args.indexOf("--")).toBeGreaterThan(run.args.lastIndexOf("--env"));

    expect(calls.find((c) => c.args[0] === "pull")?.args).toEqual(pullArgs(PROFILE_IMAGE, SOCKET));
    expect(calls.find((c) => c.args[0] === "pull")?.env).toEqual(childSessionEnv(CHILD_TOKEN));

    expect(deleteCallCount(calls)).toBe(1);

    // the durable pipeline run state: one authoritative document under the state root
    const statePath = pipelineRunStatePath(dirs.state, outcome.runId);
    const stateBytes = await readFile(statePath);
    const stateText = stateBytes.toString("utf8");
    const state = JSON.parse(stateText);
    const resolvedPipeline = await loadPipeline(dirs.pipelineRoot);
    const resultPath = agentResultFilePath(dirs.workspace, outcome.runId);
    const resultSha = new Bun.CryptoHasher("sha256").update(await readFile(resultPath)).digest("hex");
    const inputSha = new Bun.CryptoHasher("sha256")
      .update(await readFile(join(dirs.workspace, "TASK.md")))
      .digest("hex");
    expect(state).toEqual({
      schema_version: 1,
      revision: 7,
      run_id: outcome.runId,
      status: "success",
      phase: "finished",
      started_at: expect.any(String),
      updated_at: expect.any(String),
      workspace: dirs.workspace,
      pipeline: {
        schema_version: 1,
        bundle_root: dirs.pipelineRoot,
        execution_snapshot_sha256: pipelineExecutionDigest(resolvedPipeline),
        entry_state: "execute",
        max_transitions: 1,
      },
      protected_input: { id: "task", path: "TASK.md", sha256: inputSha },
      session_id: CHILD_SESSION_ID,
      cursor: { current_state: "completed", transition_count: 1 },
      attempt: {
        state_id: "execute",
        attempt: 1,
        profile: "default",
        session_id: CHILD_SESSION_ID,
        phase: "completed",
      },
      transitions: [
        {
          index: 0,
          from: "execute",
          outcome: "completed",
          to: "completed",
          attempt: 1,
          result_sha256: resultSha,
          artifacts: [WORK_PRODUCT_PATH],
        },
      ],
      terminal: { state_id: "completed", result: "success" },
      events: [
        { sequence: 1, kind: "run_created", at: expect.any(String) },
        { sequence: 2, kind: "phase_entered", at: expect.any(String) },
        { sequence: 3, kind: "session_created", at: expect.any(String) },
        { sequence: 4, kind: "attempt_started", at: expect.any(String) },
        { sequence: 5, kind: "transition_committed", at: expect.any(String) },
        { sequence: 6, kind: "terminal_reached", at: expect.any(String) },
        { sequence: 7, kind: "run_succeeded", at: expect.any(String) },
      ],
    });
    expect((await stat(join(dirs.state, "pipeline-runs"))).mode & 0o777).toBe(0o700);
    expect((await stat(join(dirs.state, "pipeline-runs", outcome.runId))).mode & 0o777).toBe(0o700);
    expect((await stat(statePath)).mode & 0o777).toBe(0o600);
    expect(await readdir(dirs.state)).toEqual(["pipeline-runs"]);
    for (const secret of [
      CHILD_TOKEN,
      LAUNCHER_TOKEN,
      "sk-test-key",
      OPENCODE_CONFIG,
      PROMPT_MARKER,
      INPUT_MARKER,
      PROMPT_BODY,
      INPUT_BODY,
      WORK_PRODUCT_BODY,
      "created the work product",
    ]) {
      expect(stateText).not.toContain(secret);
    }

    const workProduct = await readFile(join(dirs.workspace, WORK_PRODUCT_PATH), "utf8");
    expect(workProduct).toBe(WORK_PRODUCT_BODY);
    const result = JSON.parse(
      await readFile(agentResultFilePath(dirs.workspace, outcome.runId), "utf8"),
    );
    expect(result.run_id).toBe(outcome.runId);

    // the pipeline prompt really reaches the execution document
    const doc = await readFile(executionDocumentPath(dirs.workspace, outcome.runId), "utf8");
    expect(doc).toContain(`- run_id: ${outcome.runId}`);
    expect(doc).toContain(`- state: execute`);
    expect(doc).toContain(`- attempt: 1`);
    expect(doc).toContain(`- input (workspace-relative): TASK.md`);
    expect(doc).toContain(
      `- result (workspace-relative): .pipeline-agent-smoke/${outcome.runId}/result.json`,
    );
    expect(doc).toContain(`- allowed outcome: completed`);
    expect(doc).toContain(PROMPT_MARKER);
    expect(doc).toContain(PROMPT_BODY.trim().split("\n")[0] ?? "");
    expect(doc).toContain(`{"schema_version":1,"run_id":"${outcome.runId}"`);
    expect(doc).not.toContain("sk-test-key");
    expect(doc).not.toContain(OPENCODE_CONFIG);
    expect(doc).not.toContain(INPUT_MARKER);
  });
});

test("2. agent nonzero exit: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, runCode: 3 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("agent container failed (exit 3)");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("3. missing result.json: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeResult: false });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("agent result not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("4. invalid JSON result: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, resultBody: "{ not json" });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid JSON");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("5a. wrong schema_version: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: JSON.stringify({
        schema_version: 2,
        run_id: "x",
        status: "completed",
        summary: "s",
        artifacts: [],
      }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("schema_version");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("5b. run_id mismatch: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: JSON.stringify({
        schema_version: 1,
        run_id: "other-run",
        status: "completed",
        summary: "s",
        artifacts: [],
      }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("does not match this run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("6. listed artifact missing on disk: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (runId) =>
        JSON.stringify({
          schema_version: 1,
          run_id: runId,
          status: "completed",
          summary: "s",
          artifacts: [WORK_PRODUCT_PATH],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("is not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("7. artifact escaping the workspace: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: (runId) =>
        JSON.stringify({
          schema_version: 1,
          run_id: runId,
          status: "completed",
          summary: "s",
          artifacts: ["../../escaped.txt"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a clean workspace-relative");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("7b. artifact referencing the protected input: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (runId) =>
        JSON.stringify({
          schema_version: 1,
          run_id: runId,
          status: "completed",
          summary: "s",
          artifacts: ["TASK.md"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("protected input");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("8. agent modified the protected input: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, modifyTask: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("was modified during the agent run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("9. pull failure is non-fatal when the image may be local", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, pullCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");
    expect(calls.filter((c) => c.args[0] === "run").length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("10. exact env projection; control and ambient material never reaches worker env; prompt/input bodies never in argv", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.env).toEqual(childSessionEnv(CHILD_TOKEN));
    const runArgsText = JSON.stringify(run.args);
    for (const forbidden of [
      LAUNCHER_SECRET,
      ADMIN_SECRET,
      STATE_PATH,
      CANARY_VALUE,
      INPUT_MARKER,
      INPUT_BODY.trim(),
      PROMPT_MARKER,
      PROMPT_BODY.trim().split("\n")[0] ?? "",
    ]) {
      expect(runArgsText.includes(forbidden)).toBe(false);
    }
    // known temporary exception until docker-helper#3: resolved profile env and
    // the OpenCode config content travel as --env argv elements
    expect(run.args).toContain(`--env`);
    expect(run.args).toContain(`LLM_KEY=sk-test-key`);
    expect(run.args).toContain(`OPENCODE_CONFIG_CONTENT=${OPENCODE_CONFIG}`);

    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    const createText = JSON.stringify(create);
    for (const secret of [CHILD_TOKEN, LAUNCHER_SECRET, "sk-test-key", ADMIN_SECRET, OPENCODE_CONFIG, CANARY_VALUE, COMPLEX_LLM_SERVER]) {
      expect(createText.includes(secret)).toBe(false);
    }
    expect(deleteCallCount(calls)).toBe(1);
    for (const call of calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete")) {
      expect(JSON.stringify(call)).not.toContain(CHILD_TOKEN);
    }

    const inputContent = await readFile(join(dirs.workspace, "TASK.md"), "utf8");
    expect(inputContent).toContain(INPUT_MARKER);
  });
});

test("10b. no launcher/admin/state/canary markers in run args or env; only the workspace is mounted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const run = calls.find((c) => c.args[0] === "run")!;
    const mountPairs = run.args.filter((a, i) => run.args[i - 1] === "--mount");
    expect(mountPairs).toEqual([".:/workspace"]);

    for (const call of calls) {
      const text = JSON.stringify(call);
      if (call.args[0] === "session") {
        expect(text).not.toContain(LAUNCHER_SECRET);
        expect(text).not.toContain(ADMIN_SECRET);
        expect(text).not.toContain(STATE_PATH);
        expect(text).not.toContain(CANARY_VALUE);
        expect(text).not.toContain("sk-test-key");
        expect(text).not.toContain(OPENCODE_CONFIG);
      }
    }
    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(Object.keys(create.env).sort()).toEqual(
      ["HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR"].filter((k) => create.env[k] !== undefined).sort(),
    );
    const del = calls.find((c) => c.args[0] === "session" && c.args[1] === "delete")!;
    expect(Object.keys(del.env).sort()).toEqual(
      ["HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR"].filter((k) => del.env[k] !== undefined).sort(),
    );
  });
});

test("11. cleanup failure: overall result can never be success", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, deleteCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("cleanup_failed");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("12. missing workspace input fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await rm(join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("workspace input TASK.md is not accessible");
    expect(createCallCount(calls)).toBe(0);
    expect(deleteCallCount(calls)).toBe(0);
  });
});

test("13. workspace input escaping through a symlink is rejected", async () => {
  await withFixture(async (dirs) => {
    await writeFile(join(dirs.workspace, "..", "outside-input.md"), "task");
    await rm(join(dirs.workspace, "TASK.md"));
    await symlink(join(dirs.workspace, "..", "outside-input.md"), join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("resolves outside workspace");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("14. workspace input that is a directory is rejected", async () => {
  await withFixture(async (dirs) => {
    await rm(join(dirs.workspace, "TASK.md"));
    await mkdir(join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a regular file");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("15. unknown profile referenced by the pipeline state: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await writeFile(dirs.profileFile, PROFILE_BODY); // untouched; state still references "missing"
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      BUNDLE_PIPELINE_YAML.replace("profile: default", "profile: missing"),
    );
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("16. missing required source env: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const env = { ...BASE_ENV } as Record<string, string | undefined>;
    delete env.LLM_KEY;
    const outcome = await runAgentSmoke(
      agentSmokeOptions(dirs),
      { ...deps, baseEnv: env },
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile requires environment variable LLM_KEY");
    expect(outcome.detail).not.toContain("sk-test-key");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("17. malformed profile: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await writeFile(dirs.profileFile, "schema_version: [unclosed");
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid YAML");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("18. profile with control destination: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await writeFile(
      dirs.profileFile,
      [
        "schema_version: 1",
        `image: ${PROFILE_IMAGE}`,
        "opencode_config: opencode/default.json",
        "env:",
        "  OPENCODE_CONFIG_CONTENT:",
        "    from_env: LLM_SERVER",
        "    required: false",
        "",
      ].join("\n"),
    );
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("orchestrator-owned control or operator-path variable");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("19. profile symlink escape: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await rm(dirs.profileFile);
    const outside = join(dirs.configRoot, "..", "outside-profile.yaml");
    await writeFile(outside, PROFILE_BODY);
    await symlink(outside, dirs.profileFile);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("resolves outside the configuration root");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("19b. pipeline root that does not exist: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), pipelineRoot: join(dirs.pipelineRoot, "missing") },
      deps,
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.runId).toBe("");
    expect(outcome.detail).toContain("pipeline");
    expect(createCallCount(calls)).toBe(0);
    expect(deleteCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("20. signal during agent run: signal forwarded -> CLI exited -> Session deleted", async () => {
  await withFixture(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, events, runner, killActive } = fakeCli({
      workspace: dirs.workspace,
      blockRun: true,
    });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await waitFor("run:start", () => events.includes("run:start"));
    expect(events).toEqual([
      "session:create",
      "session:create-done",
      "pull:start",
      "pull:done",
      "run:start",
    ]);

    killActive("SIGTERM");
    signalHandler!("SIGTERM");
    await waitFor("run exit + cleanup", () => events.includes("session:delete"));

    const outcome = await pending;
    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);

    expect(events).toEqual([
      "session:create",
      "session:create-done",
      "pull:start",
      "pull:done",
      "run:start",
      "run:signal:SIGTERM",
      "run:exited:143",
      "session:delete",
    ]);
    expect(calls.find((c) => c.args[0] === "run")?.signalOnAbort).toBe(true);
    expect(calls.find((c) => c.args[0] === "pull")?.signalOnAbort).toBe(false);
    expect(events[events.length - 1]).toBe("session:delete");
  });
});

test("21. signal during the attempt_started commit: run never starts, run fails durably", async () => {
  await withFixture(async (dirs) => {
    const gate = gateIoAtRename(4);
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, killActive } = fakeCli({ workspace: dirs.workspace });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        pipelineStateIo: gate.io,
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );

    // the attempt_started commit is in flight (blocked at its rename)
    await gate.reached;
    signalHandler!("SIGTERM");
    killActive("SIGTERM");
    // the in-flight commit is not cancelled: it completes, then the recorded
    // abort fails the run before the worker starts
    gate.release();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(deleteCallCount(calls)).toBe(1);

    const state = JSON.parse(
      (await readFile(pipelineRunStatePath(dirs.state, outcome.runId))).toString("utf8"),
    );
    expect(state).toEqual({
      schema_version: 1,
      revision: 5,
      run_id: outcome.runId,
      status: "failed",
      phase: "finished",
      started_at: expect.any(String),
      updated_at: expect.any(String),
      workspace: dirs.workspace,
      pipeline: {
        schema_version: 1,
        bundle_root: dirs.pipelineRoot,
        execution_snapshot_sha256: pipelineExecutionDigest(await loadPipeline(dirs.pipelineRoot)),
        entry_state: "execute",
        max_transitions: 1,
      },
      protected_input: { id: "task", path: "TASK.md", sha256: expect.any(String) },
      session_id: CHILD_SESSION_ID,
      cursor: { current_state: "execute", transition_count: 0 },
      attempt: {
        state_id: "execute",
        attempt: 1,
        profile: "default",
        session_id: CHILD_SESSION_ID,
        phase: "failed",
      },
      transitions: [],
      failure: { reason: "signal_sigterm" },
      events: [
        { sequence: 1, kind: "run_created", at: expect.any(String) },
        { sequence: 2, kind: "phase_entered", at: expect.any(String) },
        { sequence: 3, kind: "session_created", at: expect.any(String) },
        { sequence: 4, kind: "attempt_started", at: expect.any(String) },
        { sequence: 5, kind: "run_failed", at: expect.any(String) },
      ],
    });
  });
});

test("21b. transition commit failure: run fails, no transition recorded, failure is durable", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({
      failCommit: 5,
      error: new PipelineStateStoreError("injected store failure at the transition commit"),
    });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    // the worker ran exactly once, the session was deleted exactly once
    expect(calls.filter((c) => c.args[0] === "run").length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    const state = JSON.parse(
      (await readFile(pipelineRunStatePath(dirs.state, outcome.runId))).toString("utf8"),
    );
    expect(state.revision).toBe(5);
    expect(state.status).toBe("failed");
    expect(state.phase).toBe("finished");
    expect(state.failure).toEqual({ reason: "state_persist_failed" });
    expect(state.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(state.transitions).toEqual([]);
    expect(state.terminal).toBeUndefined();
    expect(state.attempt.phase).toBe("failed");
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "run_failed",
    ]);
  });
});

test("21c. final commit failure: the complete active snapshot stays on disk, run is not ok", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({
      failCommit: 7,
      error: new PipelineStateStoreError("injected store failure at the final commit"),
    });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(calls.filter((c) => c.args[0] === "run").length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    // the last good snapshot is the revision-6 terminal_reached state
    const state = JSON.parse(
      (await readFile(pipelineRunStatePath(dirs.state, outcome.runId))).toString("utf8"),
    );
    expect(state.revision).toBe(6);
    expect(state.status).toBe("active");
    expect(state.phase).toBe("finalizing");
    expect(state.failure).toBeUndefined();
    expect(state.terminal).toEqual({ state_id: "completed", result: "success" });
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "transition_committed",
      "terminal_reached",
    ]);
  });
});

test("21d. cutoff: signal after the final write completed does not change the result", async () => {
  await withFixture(async (dirs) => {
    const gate = gateIoAtRename(7);
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        pipelineStateIo: gate.io,
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );

    await gate.reached;
    gate.release();
    const outcome = await pending;
    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.status).toBe("success");
    expect(deleteCallCount(calls)).toBe(1);

    // the outcome resolved only after the lifecycle closed signal acceptance in
    // the same synchronous tail as the final write, so this late signal is a
    // no-op by the linearization contract
    signalHandler!("SIGTERM");
    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");

    const state = JSON.parse(
      (await readFile(pipelineRunStatePath(dirs.state, outcome.runId))).toString("utf8"),
    );
    expect(state.revision).toBe(7);
    expect(state.status).toBe("success");
    expect(state.failure).toBeUndefined();
    expect(state.events[state.events.length - 1].kind).toBe("run_succeeded");
  });
});

test("21e. signal during the final success write: success is rewritten to failed, exit 130", async () => {
  await withFixture(async (dirs) => {
    const gate = gateIoAtRename(7);
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        pipelineStateIo: gate.io,
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );

    await gate.reached;
    signalHandler!("SIGINT");
    gate.release();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(130);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(deleteCallCount(calls)).toBe(1);

    const state = JSON.parse(
      (await readFile(pipelineRunStatePath(dirs.state, outcome.runId))).toString("utf8"),
    );
    expect(state.revision).toBe(8);
    expect(state.status).toBe("failed");
    expect(state.phase).toBe("finished");
    expect(state.failure).toEqual({ reason: "signal_sigint" });
    // the success event is still in the journal; the run_failed event follows it
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "phase_entered",
      "session_created",
      "attempt_started",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
      "run_failed",
    ]);
  });
});

test("21f. worker argv, mounts, env and execution document never contain the state root", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.ok).toBe(true);

    for (const call of calls) {
      for (const arg of call.args) {
        expect(arg.includes(dirs.state)).toBe(false);
      }
      for (const [key, value] of Object.entries(call.env)) {
        expect(`${key}=${value}`).not.toContain(dirs.state);
      }
    }
    const doc = await readFile(executionDocumentPath(dirs.workspace, outcome.runId), "utf8");
    expect(doc).not.toContain(dirs.state);
  });
});

test("21g. two runs against the same bundle record the same execution snapshot digest", async () => {
  await withFixture(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const first = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    const second = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(first.ok).toBe(true);
    expect(second.ok).toBe(true);
    expect(first.runId).not.toBe(second.runId);

    const readState = async (runId: string) =>
      JSON.parse((await readFile(pipelineRunStatePath(dirs.state, runId))).toString("utf8"));
    const firstState = await readState(first.runId);
    const secondState = await readState(second.runId);
    const digest = pipelineExecutionDigest(await loadPipeline(dirs.pipelineRoot));

    expect(firstState.pipeline.execution_snapshot_sha256).toBe(digest);
    expect(secondState.pipeline.execution_snapshot_sha256).toBe(digest);
    // run identity is otherwise independent: different run ids, same bundle
    expect(firstState.pipeline.bundle_root).toBe(dirs.pipelineRoot);
    expect(secondState.pipeline.bundle_root).toBe(dirs.pipelineRoot);
    expect(firstState.protected_input.sha256).toBe(secondState.protected_input.sha256);
  });
});

test("22. timeout only on the worker run; timeout expiry fails the run with a single cleanup", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      runTimeoutExpires: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(outcome.detail).toContain("timed out after 3600 seconds");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
    expect(calls.filter((c) => c.args[0] === "run").length).toBe(1);

    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.timeoutSeconds).toBe(3600);
    expect(run.signalOnAbort).toBe(true);
    const pull = calls.find((c) => c.args[0] === "pull")!;
    expect(pull.timeoutSeconds).toBeUndefined();
    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(create.timeoutSeconds).toBeUndefined();
    const del = calls.find((c) => c.args[0] === "session" && c.args[1] === "delete")!;
    expect(del.timeoutSeconds).toBeUndefined();
  });
});

test("22b. signal during pull: pull completes, run never starts, single cleanup", async () => {
  await withFixture(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, events, runner, killActive, releasePull, pullStarted } = fakeCli({
      workspace: dirs.workspace,
      blockPull: true,
    });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await pullStarted;

    // pull is not signalable: the runner has nothing to deliver to, so
    // main.ts still records the lifecycle abort; the pull runs to completion
    expect(killActive("SIGINT")).toBe(true);
    signalHandler!("SIGINT");
    releasePull();

    const outcome = await pending;
    expect(outcome.exitCode).toBe(130);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(events).toContain("pull:done");
    expect(events).not.toContain("run:start");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("23. profile comes from the pipeline's agent state", async () => {
  await withFixture(async (dirs) => {
    await writeFile(
      join(dirs.configRoot, "profiles", "alt.yaml"),
      PROFILE_BODY.replace(PROFILE_IMAGE, "alt-image.example/agent:2"),
    );
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      BUNDLE_PIPELINE_YAML.replace("profile: default", "profile: alt"),
    );
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.args[4]).toBe("alt-image.example/agent:2");
  });
});

test("24. input path comes from the pipeline", async () => {
  await withFixture(async (dirs) => {
    await mkdir(join(dirs.workspace, "docs"), { recursive: true });
    await writeFile(join(dirs.workspace, "docs", "input.md"), INPUT_BODY);
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      BUNDLE_PIPELINE_YAML.replace("path: TASK.md", "path: docs/input.md"),
    );
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    const run = calls.find((c) => c.args[0] === "run")!;
    expect(envPairValue(run.args, "AGENT_SMOKE_INPUT_PATH")).toBe("/workspace/docs/input.md");
    const doc = await readFile(executionDocumentPath(dirs.workspace, outcome.runId), "utf8");
    expect(doc).toContain("- input (workspace-relative): docs/input.md");
  });
});

test("25. the default one-step pipeline executes through the graph engine", async () => {
  await withFixture(async (dirs) => {
    const diagnostics: string[] = [];
    const restoreError = spyOn(console, "error").mockImplementation((message: unknown) => {
      diagnostics.push(String(message));
    });
    try {
      const { runner } = fakeCli({ workspace: dirs.workspace });
      const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

      expect(outcome.exitCode).toBe(0);
      expect(outcome.status).toBe("success");
      // the engine owns the outcome -> transition -> next-state mapping: it
      // started at the entry state, applied the declared transition and
      // reached the declared success terminal
      expect(diagnostics).toContain(
        `orchestrator: graph execution terminal completed (success, 1 transition(s))`,
      );
      // exactly one agent execution and exactly one applied transition
      expect(diagnostics.filter((line) => line.includes("graph execution terminal")).length).toBe(1);
      expect(diagnostics.filter((line) => line === "orchestrator: starting agent in child session").length).toBe(1);
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("26. foreign result status does not advance the graph", async () => {
  await withFixture(async (dirs) => {
    const diagnostics: string[] = [];
    const restoreError = spyOn(console, "error").mockImplementation((message: unknown) => {
      diagnostics.push(String(message));
    });
    try {
      const { calls, runner } = fakeCli({
        workspace: dirs.workspace,
        resultBody: (runId) =>
          JSON.stringify({
            schema_version: 1,
            run_id: runId,
            status: "unexpected",
            summary: "claims a status the pipeline never declared",
            artifacts: [WORK_PRODUCT_PATH],
          }),
      });
      const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

      expect(outcome.exitCode).toBe(1);
      expect(outcome.ok).toBe(false);
      expect(outcome.status).toBe("failed");
      // the standard agent result contract rejects the foreign status before
      // any transition could be applied; the engine-level unknown-outcome
      // defense itself is proven by the pure engine tests
      expect(outcome.detail).toContain('result status is "unexpected"');
      expect(outcome.detail).toContain('expected "completed"');
      // no transition was applied and no terminal diagnostic was emitted
      expect(diagnostics.some((line) => line.includes("graph execution terminal"))).toBe(false);
      expect(deleteCallCount(calls)).toBe(1);
      expect(createCallCount(calls)).toBe(1);
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("27. worker failure creates no transition; single cleanup", async () => {
  await withFixture(async (dirs) => {
    const diagnostics: string[] = [];
    const restoreError = spyOn(console, "error").mockImplementation((message: unknown) => {
      diagnostics.push(String(message));
    });
    try {
      const { calls, runner } = fakeCli({ workspace: dirs.workspace, runCode: 3 });
      const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

      expect(outcome.exitCode).toBe(1);
      expect(outcome.status).toBe("failed");
      expect(outcome.detail).toContain("agent container failed (exit 3)");
      // the callback failed, so the engine recorded no transition
      expect(diagnostics.some((line) => line.includes("graph execution terminal"))).toBe(false);
      expect(deleteCallCount(calls)).toBe(1);
      expect(createCallCount(calls)).toBe(1);
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("28. arbitrary valid state ids through the engine", async () => {
  await withFixture(async (dirs) => {
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      [
        "schema_version: 1",
        "entry_state: begin-step",
        "max_transitions: 1",
        "",
        "inputs:",
        "  - id: task",
        "    path: TASK.md",
        "    protected: true",
        "",
        "states:",
        "  - id: begin-step",
        "    type: agent",
        "    profile: default",
        "    prompt: prompts/execute.md",
        "    inputs:",
        "      - task",
        "    result_schema: schemas/agent-result.schema.json",
        "    timeout_seconds: 3600",
        "    max_attempts: 1",
        "    transitions:",
        "      - outcome: completed",
        "        to: finish-success",
        "",
        "  - id: finish-success",
        "    type: terminal",
        "    result: success",
        "",
      ].join("\n"),
    );
    const diagnostics: string[] = [];
    const restoreError = spyOn(console, "error").mockImplementation((message: unknown) => {
      diagnostics.push(String(message));
    });
    try {
      const { runner } = fakeCli({ workspace: dirs.workspace });
      const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

      expect(outcome.exitCode).toBe(0);
      expect(outcome.status).toBe("success");
      expect(diagnostics).toContain(
        `orchestrator: graph execution terminal finish-success (success, 1 transition(s))`,
      );
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("extra: agent worker spec argv, entrypoint and env", () => {
  const spec = agentWorkerSpec({
    runId: "run-x",
    childSessionToken: CHILD_TOKEN,
    workerImage: "base:latest",
    inputPathInWorkspace: "docs/input.md",
    resultPathInWorkspace: ".pipeline-agent-smoke/run-x/result.json",
    executionDocPathInWorkspace: ".pipeline-agent-smoke/run-x/execution.md",
    profileEnv: {
      LLM_SERVER: COMPLEX_LLM_SERVER,
      LLM_KEY: "sk-test-key",
      OPENCODE_ENABLE_EXA: "1",
      OPENCODE_EXPERIMENTAL_LSP_TOOL: "true",
    },
    opencodeConfigContent: OPENCODE_CONFIG,
  });
  expect(spec.image).toBe("base:latest");
  expect(spec.entrypoint).toBe("opencode");
  const instruction = spec.command[spec.command.length - 1] ?? "";
  expect(spec.command[spec.command.length - 2]).toBe("--auto");
  expect(spec.command[spec.command.length - 3]).toBe("json");
  expect(spec.command[spec.command.length - 4]).toBe("--format");
  expect(instruction).toContain("/workspace/.pipeline-agent-smoke/run-x/execution.md");
  const args = runArgs(spec, SOCKET);
  expect(args.slice(0, 9)).toEqual([
    "run",
    "--endpoint",
    SOCKET,
    "--image",
    "base:latest",
    "--entrypoint",
    "opencode",
    "--workdir",
    "/workspace",
  ]);
  const envPairs = args.filter((a, i) => args[i - 1] === "--env");
  expect(envPairs).toEqual([
    "AGENT_SMOKE_INPUT_PATH=/workspace/docs/input.md",
    `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/run-x/result.json`,
    "AGENT_SMOKE_RUN_ID=run-x",
    `DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`,
    "LLM_KEY=sk-test-key",
    `LLM_SERVER=${COMPLEX_LLM_SERVER}`,
    `OPENCODE_CONFIG_CONTENT=${OPENCODE_CONFIG}`,
    "OPENCODE_ENABLE_EXA=1",
    "OPENCODE_EXPERIMENTAL_LSP_TOOL=true",
  ]);
  const separatorIndex = args.indexOf("--");
  expect(args.slice(separatorIndex + 1)).toEqual(spec.command);
  expect(pullArgs("base:latest", SOCKET)).toEqual(["pull", "--endpoint", SOCKET, "base:latest"]);
});

test("extra: instruction contains only the execution document path", () => {
  const instruction = agentInstruction(".pipeline-agent-smoke/run/execution.md");
  expect(instruction).toContain("/workspace/.pipeline-agent-smoke/run/execution.md");
  expect(instruction).not.toContain(INPUT_MARKER);
  expect(instruction).not.toContain(INPUT_BODY.trim());
  expect(instruction).not.toContain(PROMPT_MARKER);
});

test("extra: result contract", () => {
  const ok: AgentResult = {
    schema_version: 1,
    run_id: "r",
    status: "completed",
    summary: "did the thing",
    artifacts: ["a/b.txt", ".pipeline-agent-smoke/work-product.txt"],
  };
  expect(parseAgentResult(JSON.stringify(ok), "r")).toEqual(ok);

  const invalid: Array<[string, RegExp]> = [
    ["not json", /not valid JSON/],
    ["[]", /not a JSON object/],
    [JSON.stringify({ schema_version: 2, run_id: "r", status: "completed", summary: "s", artifacts: [] }), /schema_version/],
    [JSON.stringify({ schema_version: 1, run_id: "x", status: "completed", summary: "s", artifacts: [] }), /does not match this run/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "partial", summary: "s", artifacts: [] }), /status/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "", artifacts: [] }), /summary/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "   \n\t", artifacts: [] }), /summary/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", artifacts: [] }), /summary/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s" }), /artifacts/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: "x" }), /not an array/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: [42] }), /non-string/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: [""] }), /non-string/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["/etc/passwd"] }), /not workspace-relative/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["~/secret"] }), /not workspace-relative/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["../../x"] }), /not a clean workspace-relative/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["./x"] }), /not a clean workspace-relative/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["a//b"] }), /not a clean workspace-relative/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: [], extra: true }), /unknown field "extra"/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: [], note: "hi" }), /unknown field "note"/],
  ];
  for (const [body, pattern] of invalid) {
    expect(() => parseAgentResult(body, "r")).toThrow(pattern);
  }
});

test("extra: artifacts must not alias the protected input (direct, symlink, hardlink)", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-input-artifact-"));
  try {
    const workspace = join(root, "workspace");
    await mkdir(join(workspace, "out"), { recursive: true });
    await mkdir(join(workspace, "other"), { recursive: true });
    await writeFile(join(workspace, "input.md"), "input body");
    await writeFile(join(workspace, "out", "artifact.txt"), "x");
    await writeFile(join(workspace, "other", "input.md"), "separate file with a similar name");
    await symlink(join(workspace, "input.md"), join(workspace, "out", "link.md"));
    await link(join(workspace, "input.md"), join(workspace, "out", "hard.md"));
    const inputInfo = await stat(join(workspace, "input.md"));
    const protectedInput = {
      canonical: await realpath(join(workspace, "input.md")),
      dev: inputInfo.dev,
      ino: inputInfo.ino,
    };
    const result = {
      schema_version: 1,
      run_id: "r",
      status: "completed",
      summary: "s",
      artifacts: ["out/artifact.txt"],
    };
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["input.md"] }), "r", workspace, protectedInput),
    ).rejects.toThrow(/resolves to the protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["out/link.md"] }), "r", workspace, protectedInput),
    ).rejects.toThrow(/resolves to the protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["out/hard.md"] }), "r", workspace, protectedInput),
    ).rejects.toThrow(/protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["input.md/copy.txt"] }), "r", workspace, protectedInput),
    ).rejects.toThrow(/not readable/);
    // a separate regular file with a similar name is not the protected input
    const verified = await verifyAgentResult(
      JSON.stringify({ ...result, artifacts: ["other/input.md", "out/artifact.txt"] }),
      "r",
      workspace,
      protectedInput,
    );
    expect(verified.artifacts).toEqual(["other/input.md", "out/artifact.txt"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("extra: artifact containment and existence checks", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-result-test-"));
  try {
    const workspace = join(root, "workspace");
    await mkdir(join(workspace, "out"), { recursive: true });
    await writeFile(join(workspace, "out", "artifact.txt"), "x");
    const result = {
      schema_version: 1,
      run_id: "r",
      status: "completed",
      summary: "s",
      artifacts: ["out/artifact.txt"],
    };
    const verified = await verifyAgentResult(JSON.stringify(result), "r", workspace);
    expect(verified.artifacts).toEqual(["out/artifact.txt"]);

    await expect(
      verifyAgentResult(
        JSON.stringify({ ...result, artifacts: ["out/missing.txt"] }),
        "r",
        workspace,
      ),
    ).rejects.toThrow(/not readable/);

    const outsideDir = join(root, "outside");
    await mkdir(outsideDir);
    await writeFile(join(outsideDir, "leak.txt"), "x");
    await expect(
      verifyAgentResult(
        JSON.stringify({ ...result, artifacts: [join("..", "outside", "leak.txt")] }),
        "r",
        workspace,
      ),
    ).rejects.toThrow(/not a clean workspace-relative/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("extra: artifact symlink escaping the workspace is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-result-symlink-"));
  try {
    const workspace = join(root, "workspace");
    const outside = join(root, "outside");
    await mkdir(workspace);
    await mkdir(outside);
    await writeFile(join(outside, "target.txt"), "x");
    await mkdir(join(workspace, "out"), { recursive: true });
    await symlink(join(outside, "target.txt"), join(workspace, "out", "link.txt"));
    const result = {
      schema_version: 1,
      run_id: "r",
      status: "completed",
      summary: "s",
      artifacts: ["out/link.txt"],
    };
    await expect(
      verifyAgentResult(JSON.stringify(result), "r", workspace),
    ).rejects.toThrow(/resolves outside the workspace/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
