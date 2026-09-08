import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  parseAgentResult,
  verifyAgentResult,
  type AgentResult,
} from "../src/agent_result.ts";
import { agentInstruction, agentWorkerSpec, type WorkerSpec } from "../src/worker.ts";
import type { HelperTransport, RunOutcome } from "../src/helper_api.ts";
import { runAgentSmoke, agentRunDirPath, agentResultFilePath, type AgentSmokeDeps } from "../src/agent_smoke.ts";
import type { CliRunner } from "../src/docker_helper.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";
const TASK_MARKER = "SECRET-TASK-MARKER-42";
const TASK_BODY = `# Task ${TASK_MARKER}\n\nCreate the work product.\n`;
const WORK_PRODUCT_PATH = ".pipeline-agent-smoke/work-product.txt";
const WORK_PRODUCT_BODY = "opencode-agent-smoke-ok\n";
const CANARY = "CANARY_AMBIENT_VAR";
const CANARY_VALUE = "must-never-reach-the-worker";

const PROFILE_IMAGE = "gitreg.example/opencode-docker/base:latest";
const OPENCODE_CONFIG = '{"$schema":"https://opencode.ai/config.json","model":"test/model"}';

const BASE_ENV = {
  HOME: "/home/opencode",
  XDG_CONFIG_HOME: "/uat-cred",
  LLM_SERVER: "https://llm.example/v1",
  LLM_KEY: "sk-test-key",
  OPENCODE_ENABLE_EXA: "1",
  OPENCODE_EXPERIMENTAL_LSP_TOOL: "true",
  [CANARY]: CANARY_VALUE,
  DOCKER_HELPER_SESSION_TOKEN: "dht_launcher_session_token",
  DOCKER_HELPER_CREDENTIAL_TOKEN: LAUNCHER_TOKEN,
  DOCKER_HELPER_ADMIN_TOKEN: "dha_admin_secret",
  DOCKER_HELPER_STATE_PATH: "/host/orchestrator-state",
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
    await fn({ workspace, state, credentialFile, configRoot, profileFile: join(configRoot, "profiles", "default.yaml") });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface FakeAgentOptions {
  runCode?: number;
  pullFails?: boolean;
  writeResult?: boolean;
  resultBody?: string | ((runId: string) => string);
  createArtifacts?: boolean;
  modifyTask?: boolean;
  deleteCode?: number;
  createCode?: number;
}

interface RecordedCliCall {
  args: string[];
  env: Record<string, string>;
  stdio: string;
}

interface RecordedTransportCall {
  kind: "pull" | "run" | "cancel";
  image?: string;
  spec?: WorkerSpec;
  bearer?: string;
}

class FakeAgentTransport implements HelperTransport {
  readonly calls: RecordedTransportCall[] = [];

  constructor(
    private readonly options: FakeAgentOptions & { workspace: string },
  ) {}

  async pull(image: string, bearer: string): Promise<void> {
    this.calls.push({ kind: "pull", image, bearer });
    if (this.options.pullFails === true) {
      throw new Error("pull boom");
    }
  }

  async run(spec: WorkerSpec, bearer: string): Promise<RunOutcome> {
    this.calls.push({ kind: "run", spec, bearer });
    const env = spec.containerEnv;
    const runId = env["AGENT_SMOKE_RUN_ID"] ?? "";
    if ((this.options.modifyTask ?? false) === true) {
      const taskContainer = env["AGENT_SMOKE_TASK_PATH"] ?? "";
      const taskHost = this.containerToHost(taskContainer);
      await writeFile(taskHost, `${await readFile(taskHost, "utf8")}TAMPERED\n`);
    }
    if ((this.options.createArtifacts ?? true) === true) {
      const workProduct = join(this.options.workspace, WORK_PRODUCT_PATH);
      await mkdir(join(this.options.workspace, ".pipeline-agent-smoke"), { recursive: true });
      await writeFile(workProduct, WORK_PRODUCT_BODY);
    }
    if ((this.options.writeResult ?? true) === true) {
      const artifacts =
        (this.options.createArtifacts ?? true) === true ? [WORK_PRODUCT_PATH] : [];
      const bodyOrFactory =
        this.options.resultBody === undefined
          ? JSON.stringify({
              schema_version: 1,
              run_id: runId,
              status: "completed",
              summary: "created the work product",
              artifacts,
            } satisfies AgentResult)
          : this.options.resultBody;
      const body = typeof bodyOrFactory === "function" ? bodyOrFactory(runId) : bodyOrFactory;
      if (body !== "") {
        await mkdir(agentRunDirPath(this.options.workspace, runId), { recursive: true });
        await writeFile(agentResultFilePath(this.options.workspace, runId), `${body}\n`);
      }
    }
    return { code: this.options.runCode ?? 0, operationId: "op_fake" };
  }

  cancelActive(): Promise<void> {
    this.calls.push({ kind: "cancel" });
    return Promise.resolve();
  }

  private containerToHost(containerPath: string): string {
    const prefix = "/workspace/";
    if (!containerPath.startsWith(prefix)) {
      throw new Error(`unexpected container path: ${containerPath}`);
    }
    return join(this.options.workspace, containerPath.slice(prefix.length));
  }
}

function fakeCli(options: FakeAgentOptions & { workspace: string }) {
  const calls: RecordedCliCall[] = [];
  const runner: CliRunner = async (args, env, stdio) => {
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
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };
  return { calls, runner };
}

function makeDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  runner: CliRunner,
  transport: HelperTransport,
  overrides: Partial<AgentSmokeDeps> = {},
): AgentSmokeDeps {
  return {
    cli: runner,
    transport,
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

function agentSmokeOptions(dirs: { workspace: string; configRoot: string }) {
  return {
    workspace: dirs.workspace,
    taskPath: join(dirs.workspace, "TASK.md"),
    configRoot: dirs.configRoot,
    profileName: "default",
  };
}

function deleteCallCount(calls: RecordedCliCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length;
}

function createCallCount(calls: RecordedCliCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "create").length;
}

test("1. success: agent runs via profile, result + artifacts verified, session cleaned up, exit 0", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.status).toBe("success");

    const createCall = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(createCall.env.XDG_CONFIG_HOME).toBe("/uat-cred");

    const runCall = transport.calls.find((c) => c.kind === "run")!;
    const env = runCall.spec?.containerEnv ?? {};
    expect(env.AGENT_SMOKE_RUN_ID).toBe(outcome.runId);
    expect(env.AGENT_SMOKE_TASK_PATH).toBe("/workspace/TASK.md");
    expect(env.AGENT_SMOKE_RESULT_PATH).toBe(
      `/workspace/.pipeline-agent-smoke/${outcome.runId}/result.json`,
    );
    expect(runCall.spec?.image).toBe(PROFILE_IMAGE);
    expect(runCall.spec?.entrypoint).toBe("opencode");
    expect(runCall.spec?.mounts).toEqual([{ source: ".", target: "/workspace" }]);

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
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace, runCode: 3 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("agent container failed (exit 3)");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("3. missing result.json: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace, writeResult: false });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("agent result not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("4. invalid JSON result: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace, resultBody: "{ not json" });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid JSON");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("5a. wrong schema_version: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({
      workspace: dirs.workspace,
      resultBody: JSON.stringify({
        schema_version: 2,
        run_id: "x",
        status: "completed",
        summary: "s",
        artifacts: [],
      }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("schema_version");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("5b. run_id mismatch: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({
      workspace: dirs.workspace,
      resultBody: JSON.stringify({
        schema_version: 1,
        run_id: "other-run",
        status: "completed",
        summary: "s",
        artifacts: [],
      }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("does not match this run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("6. listed artifact missing on disk: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({
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
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("is not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("7. artifact escaping the workspace: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({
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
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a clean workspace-relative");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("8. agent modified TASK.md: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace, modifyTask: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("task file was modified during the agent run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("9. pull failure is non-fatal when the image is local; post-session failures still clean up", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace, pullFails: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("10. worker environment is the exact profile projection; credentials and ambient env never forwarded", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));
    expect(outcome.exitCode).toBe(0);

    const runCall = transport.calls.find((c) => c.kind === "run")!;
    const env = runCall.spec?.containerEnv ?? {};
    expect(Object.keys(env).sort()).toEqual([
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
    expect(env.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);
    expect(env.LLM_SERVER).toBe(BASE_ENV.LLM_SERVER);
    expect(env.OPENCODE_CONFIG_CONTENT).toBe(OPENCODE_CONFIG);
    expect(env[CANARY]).toBeUndefined();

    const runBearer = runCall.bearer;
    expect(runBearer).toBe(CHILD_TOKEN);

    const serializedCalls = JSON.stringify(calls);
    for (const secret of [
      LAUNCHER_TOKEN,
      "sk-test-key",
      BASE_ENV.DOCKER_HELPER_ADMIN_TOKEN,
      BASE_ENV.DOCKER_HELPER_STATE_PATH,
      BASE_ENV.DOCKER_HELPER_SESSION_TOKEN,
      CANARY_VALUE,
      TASK_MARKER,
      OPENCODE_CONFIG,
    ]) {
      expect(serializedCalls.includes(secret)).toBe(false);
    }

    for (const call of calls) {
      if (call.args[0] === "session") {
        expect(call.env.DOCKER_HELPER_SESSION_TOKEN).not.toBe(CHILD_TOKEN);
      }
    }

    const taskPath = join(dirs.workspace, "TASK.md");
    const taskContent = await readFile(taskPath, "utf8");
    expect(taskContent).toContain(TASK_MARKER);
  });
});

test("10b. transport is the only secret path: no cli call carries profile env values", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));
    expect(outcome.exitCode).toBe(0);

    for (const call of calls) {
      const argsText = JSON.stringify(call.args);
      expect(argsText).not.toContain("sk-test-key");
      expect(argsText).not.toContain("LLM_KEY=");
      expect(argsText).not.toContain("OPENCODE_CONFIG_CONTENT=");
    }
  });
});

test("11. cleanup failure: overall result can never be success", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, deleteCode: 1 });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

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
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

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
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), taskPath: outside },
      makeDeps(dirs, runner, transport),
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
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a regular file");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("15. unknown profile: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(
      { ...agentSmokeOptions(dirs), profileName: "missing" },
      makeDeps(dirs, runner, transport),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile");
    expect(createCallCount(calls)).toBe(0);
    expect(transport.calls.some((c) => c.kind === "run")).toBe(false);
  });
});

test("16. missing required source env: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const env = { ...BASE_ENV };
    delete (env as Record<string, string | undefined>).LLM_KEY;
    const outcome = await runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, transport, { baseEnv: env }),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("profile requires environment variable LLM_KEY");
    expect(outcome.detail).not.toContain("sk-test-key");
    expect(createCallCount(calls)).toBe(0);
    expect(transport.calls.some((c) => c.kind === "run")).toBe(false);
  });
});

test("17. malformed profile: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await writeFile(dirs.profileFile, "schema_version: [unclosed");
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

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
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("orchestrator-owned control or operator-path variable");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("19. profile symlink escape: fails before any session is created", async () => {
  await withFixture(async (dirs) => {
    await rm(dirs.profileFile);
    const outside = join(dirs.configRoot, "..", "outside-profile.json");
    await writeFile(outside, PROFILE_BODY);
    await symlink(outside, dirs.profileFile);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("resolves outside the configuration root");
    expect(createCallCount(calls)).toBe(0);
  });
});

test("20. --image style second path cannot exist: worker image comes from the profile only", async () => {
  await withFixture(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const transport = new FakeAgentTransport({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, transport));
    const runCall = transport.calls.find((c) => c.kind === "run")!;
    expect(runCall.spec?.image).toBe(PROFILE_IMAGE);
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
      LLM_SERVER: BASE_ENV.LLM_SERVER,
      LLM_KEY: BASE_ENV.LLM_KEY,
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
  expect(spec.mounts).toEqual([{ source: ".", target: "/workspace" }]);
  expect(spec.containerEnv.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);
  expect(spec.containerEnv.LLM_KEY).toBe(BASE_ENV.LLM_KEY);
  expect(spec.containerEnv.AGENT_SMOKE_TASK_PATH).toBe("/workspace/docs/TASK.md");
  expect(spec.containerEnv.AGENT_SMOKE_RESULT_PATH).toBe(
    "/workspace/.pipeline-agent-smoke/run-x/result.json",
  );
  expect(Object.keys(spec.containerEnv)).not.toContain("DOCKER_HELPER_CREDENTIAL_TOKEN");
  expect(Object.keys(spec.containerEnv)).not.toContain("DOCKER_HELPER_ADMIN_TOKEN");
  expect(Object.keys(spec.containerEnv)).not.toContain("DOCKER_HELPER_STATE_PATH");
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
