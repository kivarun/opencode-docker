import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  parseAgentResult,
  verifyAgentResult,
  type AgentResult,
} from "../src/agent_result.ts";
import {
  agentInstruction,
  agentWorkerSpec,
  runArgs,
} from "../src/worker.ts";
import { runAgentSmoke, agentRunDirPath, agentResultFilePath, type AgentSmokeDeps } from "../src/agent_smoke.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";
const TASK_MARKER = "SECRET-TASK-MARKER-42";
const TASK_BODY = `# Task ${TASK_MARKER}\n\nCreate the work product.\n`;
const WORK_PRODUCT_PATH = ".pipeline-agent-smoke/work-product.txt";
const WORK_PRODUCT_BODY = "opencode-agent-smoke-ok\n";

const BASE_ENV = {
  HOME: "/home/opencode",
  XDG_CONFIG_HOME: "/uat-cred",
  LLM_SERVER: "https://llm.example/v1",
  LLM_KEY: "sk-test-key",
  OPENCODE_CONFIG_CONTENT: '{"$schema":"https://opencode.ai/config.json"}',
  OPENCODE_ENABLE_EXA: "1",
  OPENCODE_EXPERIMENTAL_LSP_TOOL: "true",
  DOCKER_HELPER_SESSION_TOKEN: "dht_launcher_session_token",
  DOCKER_HELPER_CREDENTIAL_TOKEN: LAUNCHER_TOKEN,
  DOCKER_HELPER_ADMIN_TOKEN: "dha_admin_secret",
  DOCKER_HELPER_STATE_PATH: "/host/orchestrator-state",
};

interface FakeAgentOptions {
  runCode?: number;
  pullCode?: number;
  writeResult?: boolean;
  resultBody?: string | ((runId: string) => string);
  createArtifacts?: boolean;
  modifyTask?: boolean;
  deleteCode?: number;
  createCode?: number;
}

function containerToHost(options: { workspace: string }, containerPath: string): string {
  const prefix = "/workspace/";
  if (!containerPath.startsWith(prefix)) {
    throw new Error(`unexpected container path: ${containerPath}`);
  }
  return join(options.workspace, containerPath.slice(prefix.length));
}

function fakeAgentCli(options: FakeAgentOptions & { workspace: string }) {
  const calls: { args: string[]; env: Record<string, string>; stdio: string }[] = [];

  const runner = async (args: string[], env: Record<string, string>, stdio: string) => {
    calls.push({ args: [...args], env: { ...env }, stdio });
    if (args[0] === "session" && args[1] === "create") {
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
      if ((options.deleteCode ?? 0) !== 0) {
        return { code: options.deleteCode ?? 1, stdout: "", stderr: "delete boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, id: CHILD_SESSION_ID, deleted: true }),
        stderr: "",
      };
    }
    if (args[0] === "pull") {
      return { code: options.pullCode ?? 0, stdout: "", stderr: "" };
    }
    if (args[0] === "run") {
      const envPairs = extractEnvPairs(args);
      const runId = envPairs.AGENT_SMOKE_RUN_ID ?? "";
      if ((options.modifyTask ?? false) === true) {
        const taskHost = containerToHost(options, envPairs.AGENT_SMOKE_TASK_PATH ?? "");
        await writeFile(taskHost, `${await readFile(taskHost, "utf8")}TAMPERED\n`);
      }
      if ((options.createArtifacts ?? true) === true) {
        const workProduct = join(options.workspace, WORK_PRODUCT_PATH);
        await mkdir(join(options.workspace, ".pipeline-agent-smoke"), { recursive: true });
        await writeFile(workProduct, WORK_PRODUCT_BODY);
      }
      if ((options.writeResult ?? true) === true) {
        const artifacts =
          (options.createArtifacts ?? true) === true
            ? [WORK_PRODUCT_PATH]
            : [];
        const bodyOrFactory =
          options.resultBody === undefined
            ? JSON.stringify({
                schema_version: 1,
                run_id: runId,
                status: "completed",
                summary: "created the work product",
                artifacts,
              } satisfies AgentResult)
            : options.resultBody;
        const body = typeof bodyOrFactory === "function" ? bodyOrFactory(runId) : bodyOrFactory;
        if (body !== "") {
          await mkdir(agentRunDirPath(options.workspace, runId), { recursive: true });
          await writeFile(agentResultFilePath(options.workspace, runId), `${body}\n`);
        }
      }
      return { code: options.runCode ?? 0, stdout: "", stderr: "" };
    }
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };

  return { calls, runner };
}

function extractEnvPairs(args: string[]): Record<string, string> {
  const pairs: Record<string, string> = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--env") {
      const pair = args[i + 1] ?? "";
      const eq = pair.indexOf("=");
      if (eq > 0) {
        pairs[pair.slice(0, eq)] = pair.slice(eq + 1);
      }
      i++;
    }
  }
  return pairs;
}

async function withTempDirs(
  fn: (dirs: { workspace: string; state: string; credentialFile: string }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "agent-smoke-test-"));
  const workspace = join(root, "workspace");
  const state = join(root, "state");
  const configDir = join(root, "config", "docker-helper");
  await mkdir(workspace, { recursive: true });
  await mkdir(state, { recursive: true });
  await mkdir(configDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(configDir, "credential.token");
  await writeFile(credentialFile, `${LAUNCHER_TOKEN}\n`, { mode: 0o600 });
  await writeFile(join(workspace, "TASK.md"), TASK_BODY);
  try {
    await fn({ workspace, state, credentialFile });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function makeDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  runner: AgentSmokeDeps["cli"],
  overrides: Partial<AgentSmokeDeps> = {},
): AgentSmokeDeps {
  return {
    cli: runner,
    fetchAuth: async () => ({
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: LAUNCHER_ID },
    }),
    config: { socketPath: "/run/docker-helper/test.sock", credentialFile: dirs.credentialFile },
    stateDirPath: dirs.state,
    baseEnv: BASE_ENV,
    ...overrides,
  };
}

function agentSmokeOptions(dirs: { workspace: string }) {
  return {
    workspace: dirs.workspace,
    taskPath: join(dirs.workspace, "TASK.md"),
    workerImage: "gitreg.example/opencode-docker/base:latest",
  };
}

function deleteCallCount(calls: { args: string[] }[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length;
}

test("1. success: agent runs, result + artifacts verified, session cleaned up, exit 0", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.status).toBe("success");

    const runCall = calls.find((c) => c.args[0] === "run")!;
    const envPairs = extractEnvPairs(runCall.args);
    expect(envPairs.AGENT_SMOKE_RUN_ID).toBe(outcome.runId);
    expect(envPairs.AGENT_SMOKE_TASK_PATH).toBe("/workspace/TASK.md");
    expect(envPairs.AGENT_SMOKE_RESULT_PATH).toBe(
      `/workspace/.pipeline-agent-smoke/${outcome.runId}/result.json`,
    );

    expect(deleteCallCount(calls)).toBe(1);

    const stateFiles = await readdir(dirs.state);
    const state = JSON.parse(
      await readFile(join(dirs.state, stateFiles[0] ?? ""), "utf8"),
    );
    expect(state.status).toBe("success");
    expect(state.session_id).toBe(CHILD_SESSION_ID);
    expect(JSON.stringify(state)).not.toContain(CHILD_TOKEN);
    expect(JSON.stringify(state)).not.toContain(LAUNCHER_TOKEN);

    const workProduct = await readFile(join(dirs.workspace, WORK_PRODUCT_PATH), "utf8");
    expect(workProduct).toBe(WORK_PRODUCT_BODY);
    const result = JSON.parse(
      await readFile(agentResultFilePath(dirs.workspace, outcome.runId), "utf8"),
    );
    expect(result.run_id).toBe(outcome.runId);
  });
});

test("2. agent nonzero exit: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, runCode: 3 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("agent container failed (exit 3)");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("3. missing result.json: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, writeResult: false });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("agent result not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("4. invalid JSON result: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, resultBody: "{ not json" });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid JSON");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("5a. wrong schema_version: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({
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
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({
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
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({
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
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({
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
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, modifyTask: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("task file was modified during the agent run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("9. pull failure is non-fatal when the image is local; post-session failures still clean up", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, pullCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("10. worker environment is the explicit allowlist; credentials never forwarded", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const runCall = calls.find((c) => c.args[0] === "run")!;
    expect(runCall.env.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);

    const envPairs = extractEnvPairs(runCall.args);
    expect(Object.keys(envPairs).sort()).toEqual([
      "AGENT_SMOKE_RESULT_PATH",
      "AGENT_SMOKE_RUN_ID",
      "AGENT_SMOKE_TASK_PATH",
      "DOCKER_HELPER_SESSION_TOKEN",
      "LLM_KEY",
      "LLM_SERVER",
      "OPENCODE_CONFIG_CONTENT",
      "OPENCODE_ENABLE_EXA",
      "OPENCODE_EXPERIMENTAL_LSP_TOOL",
    ]);
    expect(envPairs.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);
    expect(envPairs.LLM_SERVER).toBe(BASE_ENV.LLM_SERVER);
    expect(envPairs.OPENCODE_CONFIG_CONTENT).toBe(BASE_ENV.OPENCODE_CONFIG_CONTENT);

    const serializedArgs = JSON.stringify(runCall.args);
    for (const secret of [
      LAUNCHER_TOKEN,
      BASE_ENV.DOCKER_HELPER_SESSION_TOKEN,
      BASE_ENV.DOCKER_HELPER_ADMIN_TOKEN,
      BASE_ENV.DOCKER_HELPER_STATE_PATH,
      TASK_MARKER,
    ]) {
      expect(serializedArgs.includes(secret)).toBe(false);
    }
    expect(JSON.stringify(envPairs).includes(LAUNCHER_TOKEN)).toBe(false);
    expect(JSON.stringify(envPairs).includes(BASE_ENV.DOCKER_HELPER_ADMIN_TOKEN)).toBe(false);
    expect(JSON.stringify(envPairs).includes(BASE_ENV.DOCKER_HELPER_STATE_PATH)).toBe(false);
    expect(JSON.stringify(envPairs).includes(BASE_ENV.DOCKER_HELPER_SESSION_TOKEN)).toBe(false);

    for (const call of calls) {
      if (call.args[0] === "session") {
        expect(call.env.DOCKER_HELPER_SESSION_TOKEN).not.toBe(CHILD_TOKEN);
      }
    }

    const taskPath = join(dirs.workspace, "TASK.md");
    const taskContent = await readFile(taskPath, "utf8");
    expect(taskContent).toContain(TASK_MARKER);
    expect(serializedArgs.includes(taskContent.trim())).toBe(false);
  });
});

test("11. cleanup failure: overall result can never be success", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace, deleteCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("cleanup_failed");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("12. missing TASK.md fails before any session is created", async () => {
  await withTempDirs(async (dirs) => {
    await rm(join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("is not accessible");
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
    expect(deleteCallCount(calls)).toBe(0);
  });
});

test("13. TASK.md outside the workspace is rejected", async () => {
  await withTempDirs(async (dirs) => {
    const outside = join(dirs.workspace, "..", "outside-task.md");
    await writeFile(outside, "task");
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), taskPath: outside },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not inside workspace");
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
  });
});

test("14. TASK.md that is a directory is rejected", async () => {
  await withTempDirs(async (dirs) => {
    await rm(join(dirs.workspace, "TASK.md"));
    await mkdir(join(dirs.workspace, "TASK.md"));
    const { calls, runner } = fakeAgentCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a regular file");
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
  });
});

test("extra: agent worker spec argv, entrypoint and env", () => {
  const spec = agentWorkerSpec({
    runId: "run-x",
    childSessionToken: CHILD_TOKEN,
    workerImage: "base:latest",
    taskPathInWorkspace: "docs/TASK.md",
    resultPathInWorkspace: ".pipeline-agent-smoke/run-x/result.json",
    baseEnv: BASE_ENV,
  });
  const args = runArgs(spec, "/run/docker-helper/test.sock");
  expect(args[0]).toBe("run");
  expect(args).toContain("--image");
  expect(args).toContain("base:latest");
  expect(args).toContain("--entrypoint");
  expect(args).toContain("opencode");
  expect(args).toContain("--mount");
  expect(args).toContain(".:/workspace");
  expect(args).toContain("DOCKER_HELPER_SESSION_TOKEN=" + CHILD_TOKEN);
  expect(args).toContain("AGENT_SMOKE_TASK_PATH=/workspace/docs/TASK.md");
  expect(args).toContain("AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/run-x/result.json");
  expect(args[args.length - 1]).toBe(
    agentInstruction("docs/TASK.md", ".pipeline-agent-smoke/run-x/result.json", "run-x"),
  );
  expect(args[args.length - 2]).toBe("--auto");
  expect(args[args.length - 3]).toBe("json");
  expect(args[args.length - 4]).toBe("--format");

  const env = extractEnvPairs(args);
  expect(env.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);
  expect(env.LLM_KEY).toBe(BASE_ENV.LLM_KEY);
  expect(Object.keys(env)).not.toContain("DOCKER_HELPER_CREDENTIAL_TOKEN");
  expect(Object.keys(env)).not.toContain("DOCKER_HELPER_ADMIN_TOKEN");
  expect(Object.keys(env)).not.toContain("DOCKER_HELPER_STATE_PATH");
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

  const invalid: [string, RegExp][] = [
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
