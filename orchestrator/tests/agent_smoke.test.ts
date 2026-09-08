import { mkdir, mkdtemp, open, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  parseAgentResult,
  verifyAgentResult,
  type AgentResult,
} from "../src/agent_result.ts";
import { agentInstruction, agentWorkerSpec, pullArgs, runArgs } from "../src/worker.ts";
import { runAgentSmoke, agentRunDirPath, agentResultFilePath, type AgentSmokeDeps } from "../src/agent_smoke.ts";
import { childSessionEnv, signalExitCode, type LifecycleDeps } from "../src/lifecycle.ts";
import type { CliResult, CliRunner } from "../src/docker_helper.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";
const SOCKET = "/run/docker-helper/test.sock";
const TASK_MARKER = "SECRET-TASK-MARKER-42";
const TASK_BODY = `# Task ${TASK_MARKER}\n\nCreate the work product.\n`;
const WORK_PRODUCT_PATH = ".pipeline-agent-smoke/work-product.txt";
const WORK_PRODUCT_BODY = "opencode-agent-smoke-ok\n";
const CANARY = "CANARY_AMBIENT_VAR";
const CANARY_VALUE = "must-never-reach-the-worker";
const COMPLEX_LLM_SERVER = "https://llm.example/v1? a=b \"c\"";

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
}

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: string;
  signalOnAbort: boolean;
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

/**
 * Deterministic FIFO barrier around saveRunState: the state file itself is a
 * named pipe, so each state write blocks at open() until this reader connects,
 * and resolves only after the writer closes (EOF). The returned JSON tells the
 * test exactly which state write completed.
 */
async function drainFifo(fifo: string): Promise<string> {
  const fh = await open(fifo, "r");
  const data = await fh.readFile();
  await fh.close();
  return data.toString("utf8");
}

function fakeCli(options: FakeAgentOptions & { workspace: string }) {
  const calls: RecordedCall[] = [];
  const events: string[] = [];
  let activeRun: { release: (code: number) => void } | null = null;
  let notifyDeleteStart: (() => void) | null = null;
  const deleteStarted = new Promise<void>((resolve) => {
    notifyDeleteStart = resolve;
  });

  const runner = async (
    args: string[],
    env: Record<string, string>,
    stdio: string,
    opts?: { signalOnAbort?: boolean },
  ): Promise<CliResult> => {
    calls.push({ args: [...args], env: { ...env }, stdio, signalOnAbort: opts?.signalOnAbort === true });
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
      const runCode = options.runCode ?? 0;
      events.push(`run:exited:${runCode}`);
      const envPairs = args.filter((a, i) => args[i - 1] === "--env");
      const runId = envPairs.find((p) => p.startsWith("AGENT_SMOKE_RUN_ID="))?.slice("AGENT_SMOKE_RUN_ID=".length) ?? "";
      if ((options.modifyTask ?? false) === true) {
        const taskPair = envPairs.find((p) => p.startsWith("AGENT_SMOKE_TASK_PATH=")) ?? "";
        const taskContainerPath = taskPair.slice("AGENT_SMOKE_TASK_PATH=".length);
        const taskHost = join(options.workspace, taskContainerPath.replace(/^\/workspace\//, ""));
        await writeFile(taskHost, `${await readFile(taskHost, "utf8")}TAMPERED\n`);
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
      events.push("pull:start", "pull:done");
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
    killActive: (signal: "SIGINT" | "SIGTERM") => {
      if (activeRun !== null) {
        events.push(`run:signal:${signal}`);
        const release = activeRun.release;
        activeRun = null;
        release(signalExitCode(signal));
      }
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
  }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "agent-smoke-test-"));
  const workspace = join(root, "workspace");
  const state = join(root, "state");
  const configDir = join(root, "config", "docker-helper");
  const configRoot = join(root, "operator-config");
  await mkdir(workspace, { recursive: true });
  await mkdir(state, { recursive: true });
  await mkdir(configDir, { recursive: true, mode: 0o700 });
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  const credentialFile = join(configDir, "credential.token");
  await writeFile(credentialFile, `${LAUNCHER_TOKEN}\n`, { mode: 0o600 });
  await writeFile(join(workspace, "TASK.md"), TASK_BODY);
  await writeFile(join(configRoot, "profiles", "default.yaml"), PROFILE_BODY);
  await writeFile(join(configRoot, "opencode", "default.json"), OPENCODE_CONFIG);
  try {
    await fn({
      workspace,
      state,
      credentialFile,
      configRoot,
      profileFile: join(configRoot, "profiles", "default.yaml"),
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function makeDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  runner: CliRunner,
  overrides: Partial<LifecycleDeps> = {},
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

function agentSmokeOptions(dirs: { workspace: string; configRoot: string }) {
  return {
    workspace: dirs.workspace,
    taskPath: join(dirs.workspace, "TASK.md"),
    configRoot: dirs.configRoot,
    profileName: "default",
  };
}

function deleteCallCount(calls: RecordedCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length;
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

test("1. success: agent runs via profile, result + artifacts verified, session cleaned up, exit 0", async () => {
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
      `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/${outcome.runId}/result.json`,
      `AGENT_SMOKE_RUN_ID=${outcome.runId}`,
      "AGENT_SMOKE_TASK_PATH=/workspace/TASK.md",
      `DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`,
      "LLM_KEY=sk-test-key",
      `LLM_SERVER=${COMPLEX_LLM_SERVER}`,
      `OPENCODE_CONFIG_CONTENT=${OPENCODE_CONFIG}`,
      "OPENCODE_ENABLE_EXA=1",
      "OPENCODE_EXPERIMENTAL_LSP_TOOL=true",
    ]);
    const separator = run.args.indexOf("--");
    expect(run.args.slice(separator + 1)).toEqual([
      "run",
      "--format",
      "json",
      "--auto",
      agentInstruction("TASK.md", `.pipeline-agent-smoke/${outcome.runId}/result.json`, outcome.runId),
    ]);
    expect(run.args.indexOf("--")).toBeGreaterThan(run.args.lastIndexOf("--env"));

    expect(calls.find((c) => c.args[0] === "pull")?.args).toEqual(pullArgs(PROFILE_IMAGE, SOCKET));
    expect(calls.find((c) => c.args[0] === "pull")?.env).toEqual(childSessionEnv(CHILD_TOKEN));

    expect(deleteCallCount(calls)).toBe(1);

    const stateFiles = await readdir(dirs.state);
    const state = JSON.parse(await readFile(join(dirs.state, stateFiles[0] ?? ""), "utf8"));
    expect(state.status).toBe("success");
    expect(state.session_id).toBe(CHILD_SESSION_ID);
    expect(state.worker_image).toBe(PROFILE_IMAGE);
    expect(JSON.stringify(state)).not.toContain(CHILD_TOKEN);
    expect(JSON.stringify(state)).not.toContain(LAUNCHER_TOKEN);
    expect(JSON.stringify(state)).not.toContain("sk-test-key");
    expect(JSON.stringify(state)).not.toContain(OPENCODE_CONFIG);

    const workProduct = await readFile(join(dirs.workspace, WORK_PRODUCT_PATH), "utf8");
    expect(workProduct).toBe(WORK_PRODUCT_BODY);
    const result = JSON.parse(
      await readFile(agentResultFilePath(dirs.workspace, outcome.runId), "utf8"),
    );
    expect(result.run_id).toBe(outcome.runId);
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

test("8. agent modified TASK.md: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, modifyTask: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("task file was modified during the agent run");
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

test("10. exact env projection; control and ambient material never reaches worker env; task body never in argv", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.env).toEqual(childSessionEnv(CHILD_TOKEN));
    const runArgsText = JSON.stringify(run.args);
    for (const forbidden of [LAUNCHER_SECRET, ADMIN_SECRET, STATE_PATH, CANARY_VALUE, TASK_MARKER, TASK_BODY.trim()]) {
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

    const taskContent = await readFile(join(dirs.workspace, "TASK.md"), "utf8");
    expect(taskContent).toContain(TASK_MARKER);
  });
});

test("10b. no launcher/admin/state/canary/task markers in run args or env", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

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

test("12. missing TASK.md fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await rm(join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("is not accessible");
    expect(createCallCount(calls)).toBe(0);
    expect(deleteCallCount(calls)).toBe(0);
  });
});

test("13. TASK.md outside the workspace is rejected", async () => {
  await withFixture(async (dirs) => {
    const outside = join(dirs.workspace, "..", "outside-task.md");
    await writeFile(outside, "task");
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), taskPath: outside },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not inside workspace");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("14. TASK.md that is a directory is rejected", async () => {
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

test("15. unknown profile: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), profileName: "missing" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile");
    expect(createCallCount(calls)).toBe(0);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("16. missing required source env: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const env = { ...BASE_ENV } as Record<string, string | undefined>;
    delete env.LLM_KEY;
    const outcome = await runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, { baseEnv: env }),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile requires environment variable LLM_KEY");
    expect(outcome.detail).not.toContain("sk-test-key");
    expect(createCallCount(calls)).toBe(0);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("17. malformed profile: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await writeFile(dirs.profileFile, "schema_version: [unclosed");
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid YAML");
    expect(createCallCount(calls)).toBe(0);
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
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("orchestrator-owned control or operator-path variable");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("19. profile symlink escape: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await rm(dirs.profileFile);
    const outside = join(dirs.configRoot, "..", "outside-profile.yaml");
    await writeFile(outside, PROFILE_BODY);
    await symlink(outside, dirs.profileFile);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("resolves outside the configuration root");
    expect(createCallCount(calls)).toBe(0);
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
    await Bun.sleep(10);
    expect(events).toEqual([
      "session:create",
      "session:create-done",
      "pull:start",
      "pull:done",
      "run:start",
    ]);

    killActive("SIGTERM");
    signalHandler!("SIGTERM");
    await Bun.sleep(10);

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

test("21. signal during updateState(agent_running): run never starts, status not success", async () => {
  await withFixture(async (dirs) => {
    const runId = "agent-signal-state-run";
    await mkdir(dirs.state, { recursive: true });
    const stateFifo = join(dirs.state, `smoke-${runId}.json`);
    Bun.spawnSync(["mkfifo", stateFifo]);

    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, killActive, deleteStarted } = fakeCli({ workspace: dirs.workspace });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        randomId: () => runId,
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );

    const b1 = JSON.parse(await drainFifo(stateFifo));
    expect(b1.status).toBe("creating_session");
    const b2 = JSON.parse(await drainFifo(stateFifo));
    expect(b2.status).toBe("session_created");

    const reader3 = await open(stateFifo, "r");
    signalHandler!("SIGTERM");
    killActive("SIGTERM");
    const b3 = JSON.parse((await reader3.readFile()).toString("utf8"));
    await reader3.close();
    expect(b3.status).toBe("agent_running");

    await deleteStarted;
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    const finalState = JSON.parse(await drainFifo(stateFifo));
    const outcome = await pending;

    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length).toBe(1);
    expect(finalState.status).toBe("failed");
    expect(finalState.session_id).toBe(CHILD_SESSION_ID);
  });
});

test("extra: agent worker spec argv, entrypoint and env", () => {
  const spec = agentWorkerSpec({
    runId: "run-x",
    childSessionToken: CHILD_TOKEN,
    workerImage: "base:latest",
    taskPathInWorkspace: "docs/TASK.md",
    resultPathInWorkspace: ".pipeline-agent-smoke/run-x/result.json",
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
  expect(spec.command[spec.command.length - 1]).toBe(
    agentInstruction("docs/TASK.md", ".pipeline-agent-smoke/run-x/result.json", "run-x"),
  );
  expect(spec.command[spec.command.length - 2]).toBe("--auto");
  expect(spec.command[spec.command.length - 3]).toBe("json");
  expect(spec.command[spec.command.length - 4]).toBe("--format");
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
    `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/run-x/result.json`,
    "AGENT_SMOKE_RUN_ID=run-x",
    "AGENT_SMOKE_TASK_PATH=/workspace/docs/TASK.md",
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

test("extra: instruction never embeds the task body", () => {
  const instruction = agentInstruction("TASK.md", ".pipeline-agent-smoke/run/result.json", "run");
  expect(instruction).not.toContain(TASK_MARKER);
  expect(instruction).not.toContain(TASK_BODY.trim());
  expect(instruction).toContain("/workspace/TASK.md");
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
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["TASK.md"] }), /task file/],
    [JSON.stringify({ schema_version: 1, run_id: "r", status: "completed", summary: "s", artifacts: ["sub/TASK.md"] }), /task file/],
  ];
  for (const [body, pattern] of invalid) {
    expect(() => parseAgentResult(body, "r")).toThrow(pattern);
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
