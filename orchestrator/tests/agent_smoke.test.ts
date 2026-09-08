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
  activationDirPath,
  activationResultFilePath,
  activationExecutionDocPath,
  type AgentSmokeDeps,
} from "../src/agent_smoke.ts";
import { childSessionEnv, signalExitCode, type LifecycleDeps } from "../src/lifecycle.ts";
import { loadPipeline } from "../src/pipeline.ts";
import { pipelineExecutionDigest } from "../src/pipeline_digest.ts";
import { pipelineRunStatePath } from "../src/pipeline_state_store.ts";
import { validatePipelineRunState } from "../src/pipeline_state.ts";
import type { CliResult, CliRunner } from "../src/docker_helper.ts";
import { gateIoAtRename, faultIo } from "./state_io_test_helpers.ts";
import { PipelineStateStoreError } from "../src/pipeline_state_store.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const LAUNCHER_ID = "dhl_launcher";
const SOCKET = "/run/docker-helper/test.sock";
const INPUT_MARKER = "SECRET-INPUT-MARKER-42";
const INPUT_BODY = `# Task ${INPUT_MARKER}\n\nCreate the work product.\n`;
const WORK_PRODUCT_PATH = ".pipeline-agent-smoke/work-product.txt";
const WORK_PRODUCT_BODY = "opencode-agent-smoke-ok\n";
const NOTES_PATH = "NOTES.md";
const NOTES_BODY = "notes created by the first state\n";
const CANARY = "CANARY_AMBIENT_VAR";
const CANARY_VALUE = "must-never-reach-the-worker";
const COMPLEX_LLM_SERVER = "https://llm.example/v1? a=b \"c\"";
const PROMPT_MARKER = "PROMPT-MARKER-77";
const SUMMARY_TEXT = "created the work product";

const LAUNCHER_SECRET = "dhc_launcher_secret_value";
const ADMIN_SECRET = "dha_admin_secret_value";
const STATE_PATH = "/host/orchestrator-state";

const PROFILE_IMAGE = "gitreg.example/opencode-docker/base:latest";
const ALT_IMAGE = "alt-image.example/agent:2";
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

/** Two sequential agent states with different profiles, inputs, and timeouts. */
const TWO_STATE_YAML = [
  "schema_version: 1",
  "entry_state: first",
  "max_transitions: 3",
  "",
  "inputs:",
  "  - id: task",
  "    path: TASK.md",
  "    protected: true",
  "  - id: notes",
  "    path: NOTES.md",
  "    protected: false",
  "",
  "states:",
  "  - id: first",
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
  "        to: second",
  "",
  "  - id: second",
  "    type: agent",
  "    profile: alt",
  "    prompt: prompts/execute.md",
  "    inputs:",
  "      - notes",
  "    result_schema: schemas/agent-result.schema.json",
  "    timeout_seconds: 30",
  "    max_attempts: 1",
  "    transitions:",
  "      - outcome: completed",
  "        to: done",
  "",
  "  - id: done",
  "    type: terminal",
  "    result: success",
  "",
].join("\n");

/** A self-loop: revisits the same state until the transition budget stops it. */
const LOOP_YAML = [
  "schema_version: 1",
  "entry_state: execute",
  "max_transitions: 2",
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
    "        to: execute",
    "",
    "  - id: halt",
    "    type: terminal",
    "    result: failed",
    "",
  ].join("\n");

/** The declared transition ends at a failed terminal: a normal graph result. */
const FAILED_TERMINAL_YAML = BUNDLE_PIPELINE_YAML.replace(
  "    result: success",
  "    result: failed",
);

/** The entry state is a terminal: zero Sessions run. */
const ENTRY_TERMINAL_YAML = [
  "schema_version: 1",
  "entry_state: done",
  "max_transitions: 1",
  "",
  "inputs: []",
  "",
  "states:",
  "  - id: done",
  "    type: terminal",
  "    result: success",
  "",
].join("\n");

/** Two protected inputs; the second is aliased by an artifact in the failure test. */
const TWO_PROTECTED_YAML = BUNDLE_PIPELINE_YAML.replace(
  "inputs:\n  - id: task\n    path: TASK.md\n    protected: true",
  "inputs:\n  - id: task\n    path: TASK.md\n    protected: true\n  - id: spec\n    path: SPEC.md\n    protected: true",
);

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

interface ResultIdentity {
  runId: string;
  stateId: string;
  activationIndex: number;
}

type ResultBody = string | ((identity: ResultIdentity) => string);

interface FakeAgentOptions {
  runCode?: number;
  runCodeByState?: Record<string, number>;
  runTimedOutByState?: Record<string, boolean>;
  pullCode?: number;
  resultBody?: ResultBody;
  resultByState?: Record<string, ResultBody>;
  createArtifacts?: boolean;
  createNotesDuringFirstRun?: boolean;
  writeNotes?: boolean;
  modifyTask?: boolean;
  writeResult?: boolean;
  deleteCode?: number;
  createCode?: number;
  blockRun?: boolean;
  blockPull?: boolean;
  blockCreate?: boolean;
  blockDelete?: boolean;
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
  const sessionIds: string[] = [];
  let sessionCounter = 0;
  let activeRun: { release: (code: number) => void } | null = null;
  let activePull: { release: () => void } | null = null;
  let activeCreate: { release: () => void } | null = null;
  let activeDelete: { release: () => void } | null = null;
  let notifyDeleteStart: (() => void) | null = null;
  const deleteStarted = new Promise<void>((resolve) => {
    notifyDeleteStart = resolve;
  });
  let notifyPullStart: (() => void) | null = null;
  const pullStarted = new Promise<void>((resolve) => {
    notifyPullStart = resolve;
  });
  let notifyCreateStart: (() => void) | null = null;
  const createStarted = new Promise<void>((resolve) => {
    notifyCreateStart = resolve;
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
      const envPairs = args.filter((a, i) => args[i - 1] === "--env");
      const envPair = (name: string): string =>
        envPairs.find((p) => p.startsWith(`${name}=`))?.slice(name.length + 1) ?? "";
      const runId = envPair("AGENT_SMOKE_RUN_ID");
      const stateId = envPair("AGENT_SMOKE_STATE_ID");
      const activationIndex = Number(envPair("AGENT_SMOKE_ACTIVATION_INDEX"));
      const attempt = Number(envPair("AGENT_SMOKE_ATTEMPT"));
      if (options.runTimedOutByState?.[stateId] === true) {
        events.push("run:timeout");
        return { code: 143, stdout: "", stderr: "", timedOut: true };
      }
      const runCode = options.runCodeByState?.[stateId] ?? options.runCode ?? 0;
      events.push(`run:exited:${runCode}`);
      if ((options.modifyTask ?? false) === true) {
        const taskPair = envPair("AGENT_SMOKE_INPUT_task");
        const taskHost = join(options.workspace, taskPair.replace(/^\/workspace\//, ""));
        await writeFile(taskHost, `${await readFile(taskHost, "utf8")}TAMPERED\n`);
      }
      if (activationIndex === 1 && options.createNotesDuringFirstRun === true) {
        await writeFile(join(options.workspace, NOTES_PATH), NOTES_BODY);
      }
      if ((options.createArtifacts ?? true) === true) {
        await mkdir(join(options.workspace, ".pipeline-agent-smoke"), { recursive: true });
        await writeFile(join(options.workspace, WORK_PRODUCT_PATH), WORK_PRODUCT_BODY);
      }
      if ((options.writeResult ?? true) === true && runCode === 0) {
        const artifacts =
          (options.createArtifacts ?? true) === true ? [WORK_PRODUCT_PATH] : [];
        const bodyOrFactory =
          options.resultByState?.[stateId] ??
          options.resultBody ??
          JSON.stringify({
            schema_version: 2,
            run_id: runId,
            state_id: stateId,
            activation_index: activationIndex,
            attempt,
            status: "completed",
            summary: SUMMARY_TEXT,
            artifacts,
          });
        const body =
          typeof bodyOrFactory === "function"
            ? bodyOrFactory({ runId, stateId, activationIndex })
            : bodyOrFactory;
        if (body !== "") {
          const resultPath = activationResultFilePath(
            options.workspace,
            runId,
            activationIndex,
            stateId,
          );
          await mkdir(join(resultPath, ".."), { recursive: true });
          await writeFile(resultPath, `${body}\n`);
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
      events.push("session:create");
      notifyCreateStart?.();
      if (options.blockCreate === true) {
        await new Promise<void>((resolve) => {
          activeCreate = { release: resolve };
        });
      }
      if ((options.createCode ?? 0) !== 0) {
        events.push("session:create-done");
        return { code: options.createCode ?? 1, stdout: "", stderr: "create boom" };
      }
      sessionCounter += 1;
      const sessionId = `dhs_child_${sessionCounter}`;
      sessionIds.push(sessionId);
      events.push(`session:create-done:${sessionId}`);
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: {
            id: sessionId,
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
      if (options.blockDelete === true) {
        await new Promise<void>((resolve) => {
          activeDelete = { release: resolve };
        });
      }
      if ((options.deleteCode ?? 0) !== 0) {
        return { code: options.deleteCode ?? 1, stdout: "", stderr: "delete boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, id: sessionIds[sessionIds.length - 1] ?? "unknown", deleted: true }),
        stderr: "",
      };
    }
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };

  return {
    calls,
    events,
    sessionIds,
    runner,
    deleteStarted,
    pullStarted,
    createStarted,
    releaseDelete: () => {
      const del = activeDelete;
      activeDelete = null;
      del?.release();
    },
    releaseCreate: () => {
      const create = activeCreate;
      activeCreate = null;
      create?.release();
    },
    releasePull: () => {
      const pull = activePull;
      activePull = null;
      pull?.release();
    },
    killActive: (signal: "SIGINT" | "SIGTERM") => {
      // mirrors the real runner's contract answer for tests: a delivery to the
      // active signalable worker run (pull, create, and delete are not signalable)
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

async function withTwoStateFixture(
  fn: (dirs: Parameters<typeof withFixture>[0] extends (d: infer D) => unknown ? D : never) => Promise<void>,
): Promise<void> {
  await withFixture(async (dirs) => {
    await writeFile(
      join(dirs.configRoot, "profiles", "alt.yaml"),
      PROFILE_BODY.replace(PROFILE_IMAGE, ALT_IMAGE),
    );
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), TWO_STATE_YAML);
    await fn(dirs);
  });
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

function createCallCount(calls: RecordedCall[]): number {
  return calls.filter((c) => c.args[0] === "session" && c.args[1] === "create").length;
}

function runCalls(calls: RecordedCall[]): RecordedCall[] {
  return calls.filter((c) => c.args[0] === "run");
}

function runCall(calls: RecordedCall[], index = 0): RecordedCall {
  const call = runCalls(calls)[index];
  if (call === undefined) {
    throw new Error(`no run call at index ${index}`);
  }
  return call;
}

function runEnvPairs(args: string[]): string[] {
  return args.filter((a, i) => args[i - 1] === "--env");
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

async function readState(dirs: { state: string }, runId: string): Promise<Record<string, any>> {
  return JSON.parse((await readFile(pipelineRunStatePath(dirs.state, runId))).toString("utf8"));
}

async function expectedIdentity(dirs: { pipelineRoot: string }): Promise<Record<string, any>> {
  const resolved = await loadPipeline(dirs.pipelineRoot);
  return {
    schema_version: 1,
    bundle_root: dirs.pipelineRoot,
    execution_snapshot_sha256: pipelineExecutionDigest(resolved),
    entry_state: resolved.entry_state,
    max_transitions: resolved.max_transitions,
  };
}

test("1. default one-state pipeline: one session, v2 result identity, durable v2 state, no secrets", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner, sessionIds } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe("dhs_child_1");
    expect(outcome.status).toBe("success");
    expect(sessionIds).toEqual(["dhs_child_1"]);

    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(create.env.XDG_CONFIG_HOME).toBe("/uat-cred");
    expect(create.env.DOCKER_HELPER_SESSION_TOKEN).toBeUndefined();

    const run = runCall(calls);
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
    const envPairs = runEnvPairs(run.args);
    expect(envPairs).toEqual([
      `AGENT_SMOKE_ACTIVATION_INDEX=1`,
      `AGENT_SMOKE_ATTEMPT=1`,
      `AGENT_SMOKE_INPUT_task=/workspace/TASK.md`,
      `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/${outcome.runId}/activations/1-execute/attempt-1/result.json`,
      `AGENT_SMOKE_RUN_ID=${outcome.runId}`,
      `AGENT_SMOKE_STATE_ID=execute`,
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
      `/workspace/.pipeline-agent-smoke/${outcome.runId}/activations/1-execute/attempt-1/execution.md`,
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
    const identity = await expectedIdentity(dirs);
    const resultPath = activationResultFilePath(dirs.workspace, outcome.runId, 1, "execute");
    const resultSha = new Bun.CryptoHasher("sha256").update(await readFile(resultPath)).digest("hex");
    const inputSha = new Bun.CryptoHasher("sha256")
      .update(await readFile(join(dirs.workspace, "TASK.md")))
      .digest("hex");
    expect(state).toEqual({
      schema_version: 2,
      revision: 9,
      run_id: outcome.runId,
      status: "success",
      phase: "finished",
      started_at: expect.any(String),
      updated_at: expect.any(String),
      workspace: dirs.workspace,
      pipeline: identity,
      protected_inputs: [{ id: "task", path: "TASK.md", sha256: inputSha }],
      cursor: { current_state: "completed", transition_count: 1 },
      activations: [
        {
          index: 1,
          state_id: "execute",
          attempt: 1,
          profile: "default",
          phase: "session_cleanup_completed",
          session_id: "dhs_child_1",
          session_cleanup: "completed",
          result_sha256: resultSha,
          artifacts: [WORK_PRODUCT_PATH],
        },
      ],
      transitions: [
        {
          index: 0,
          from: "execute",
          outcome: "completed",
          to: "completed",
          activation_index: 1,
          result_sha256: resultSha,
          artifacts: [WORK_PRODUCT_PATH],
        },
      ],
      terminal: { state_id: "completed", result: "success" },
      events: [
        { sequence: 1, kind: "run_created", at: expect.any(String) },
        { sequence: 2, kind: "activation_started", state_id: "execute", activation_index: 1, at: expect.any(String) },
        { sequence: 3, kind: "session_created", state_id: "execute", activation_index: 1, session_id: "dhs_child_1", at: expect.any(String) },
        { sequence: 4, kind: "agent_running", state_id: "execute", activation_index: 1, at: expect.any(String) },
        { sequence: 5, kind: "result_accepted", state_id: "execute", activation_index: 1, at: expect.any(String) },
        { sequence: 6, kind: "session_cleanup_completed", state_id: "execute", activation_index: 1, at: expect.any(String) },
        { sequence: 7, kind: "transition_committed", from: "execute", outcome: "completed", to: "completed", transition_index: 0, activation_index: 1, at: expect.any(String) },
        { sequence: 8, kind: "terminal_reached", state_id: "completed", at: expect.any(String) },
        { sequence: 9, kind: "run_succeeded", at: expect.any(String) },
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
      SUMMARY_TEXT,
    ]) {
      expect(stateText).not.toContain(secret);
    }

    const workProduct = await readFile(join(dirs.workspace, WORK_PRODUCT_PATH), "utf8");
    expect(workProduct).toBe(WORK_PRODUCT_BODY);
    const result = JSON.parse(await readFile(resultPath, "utf8"));
    expect(result.run_id).toBe(outcome.runId);
    expect(result.state_id).toBe("execute");
    expect(result.activation_index).toBe(1);

    // the pipeline prompt really reaches the execution document
    const doc = await readFile(activationExecutionDocPath(dirs.workspace, outcome.runId, 1, "execute"), "utf8");
    expect(doc).toContain(`- run_id: ${outcome.runId}`);
    expect(doc).toContain(`- state: execute`);
    expect(doc).toContain(`- activation_index: 1`);
    expect(doc).toContain(`- attempt: 1`);
    expect(doc).toContain(`- task (workspace-relative): TASK.md`);
    expect(doc).toContain(
      `- result (workspace-relative): .pipeline-agent-smoke/${outcome.runId}/activations/1-execute/attempt-1/result.json`,
    );
    expect(doc).toContain(`- allowed outcome: completed`);
    expect(doc).toContain(PROMPT_MARKER);
    expect(doc).toContain(PROMPT_BODY.trim().split("\n")[0] ?? "");
    expect(doc).toContain(`{"schema_version":2,"run_id":"${outcome.runId}","state_id":"execute","activation_index":1,"attempt":1`);
    expect(doc).not.toContain("sk-test-key");
    expect(doc).not.toContain(OPENCODE_CONFIG);
    expect(doc).not.toContain(INPUT_MARKER);
  });
});

test("2. two sequential agent-states: two different sessions in create/run/delete order", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner, sessionIds, events } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.status).toBe("success");
    expect(sessionIds).toEqual(["dhs_child_1", "dhs_child_2"]);
    expect(outcome.sessionId).toBe("dhs_child_2");
    // strict interleaving: create/run/delete for the first activation, then the second
    expect(events.filter((event) => event.startsWith("session:") || event === "run:start" || event.startsWith("run:exit")).map((event) =>
      event.startsWith("session:create-done:") ? "session:create-done" : event,
    )).toEqual([
      "session:create",
      "session:create-done",
      "run:start",
      "run:exited:0",
      "session:delete",
      "session:create",
      "session:create-done",
      "run:start",
      "run:exited:0",
      "session:delete",
    ]);
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);
    expect(runCalls(calls).length).toBe(2);

    // each activation ran in its own session with its own image and timeout
    const firstRun = runCall(calls, 0);
    expect(envPairValue(firstRun.args, "AGENT_SMOKE_STATE_ID")).toBe("first");
    expect(envPairValue(firstRun.args, "AGENT_SMOKE_ACTIVATION_INDEX")).toBe("1");
    expect(firstRun.args[4]).toBe(PROFILE_IMAGE);
    expect(firstRun.timeoutSeconds).toBe(3600);
    expect(envPairValue(firstRun.args, "AGENT_SMOKE_INPUT_task")).toBe("/workspace/TASK.md");
    const secondRun = runCall(calls, 1);
    expect(envPairValue(secondRun.args, "AGENT_SMOKE_STATE_ID")).toBe("second");
    expect(envPairValue(secondRun.args, "AGENT_SMOKE_ACTIVATION_INDEX")).toBe("2");
    expect(secondRun.args[4]).toBe(ALT_IMAGE);
    expect(secondRun.timeoutSeconds).toBe(30);
    expect(envPairValue(secondRun.args, "AGENT_SMOKE_INPUT_notes")).toBe("/workspace/NOTES.md");
    expect(envPairValue(secondRun.args, "AGENT_SMOKE_INPUT_task")).toBe("");

    // one durable document for the whole pipeline
    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.cursor).toEqual({ current_state: "done", transition_count: 2 });
    expect(state.activations).toHaveLength(2);
    expect(state.activations.map((a: any) => a.session_id)).toEqual(["dhs_child_1", "dhs_child_2"]);
    expect(state.activations.map((a: any) => a.state_id)).toEqual(["first", "second"]);
    expect(state.activations.map((a: any) => a.profile)).toEqual(["default", "alt"]);
    expect(
      state.transitions.map((t: any) => ({
        index: t.index,
        from: t.from,
        outcome: t.outcome,
        to: t.to,
        activation_index: t.activation_index,
      })),
    ).toEqual([
      { index: 0, from: "first", outcome: "completed", to: "second", activation_index: 1 },
      { index: 0, from: "second", outcome: "completed", to: "done", activation_index: 2 },
    ]);
    expect(state.events.map((e: any) => e.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
    ]);
  });
});

test("3. the first transition commits only after the first session is deleted", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner, events } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    // create(1) ... delete(1) strictly before create(2)
    const firstCreate = calls.findIndex((c) => c.args[1] === "create");
    const firstDelete = calls.findIndex((c) => c.args[1] === "delete");
    const secondCreate = calls.findIndex((c, index) => index > firstDelete && c.args[1] === "create");
    expect(firstCreate).toBeGreaterThanOrEqual(0);
    expect(firstDelete).toBeGreaterThan(firstCreate);
    expect(secondCreate).toBeGreaterThan(firstDelete);
    // and the worker runs stay within their own session windows
    const firstRun = calls.findIndex((c) => c.args[0] === "run");
    expect(firstRun).toBeGreaterThan(firstCreate);
    expect(firstRun).toBeLessThan(firstDelete);
    // the durable trace mirrors it: activation 1 is cleaned up (session_cleanup
    // completed) and only then its transition exists, before activation 2 started
    const state = await readState(dirs, outcome.runId);
    expect(state.activations[0].session_cleanup).toBe("completed");
    expect(state.transitions[0].activation_index).toBe(1);
    expect(state.events.findIndex((e: any) => e.kind === "session_cleanup_completed" && e.activation_index === 1)).toBeLessThan(
      state.events.findIndex((e: any) => e.kind === "transition_committed" && e.activation_index === 1),
    );
    expect(state.events.findIndex((e: any) => e.kind === "transition_committed" && e.activation_index === 1)).toBeLessThan(
      state.events.findIndex((e: any) => e.kind === "activation_started" && e.activation_index === 2),
    );
    expect(events.length).toBeGreaterThan(0);
  });
});

test("4. different profiles, images, and timeouts per state (covered with test 2)", async () => {
  // asserted inline in test 2: image + timeoutSeconds per run call
  expect(true).toBe(true);
});

test("5. the second state reads an unprotected input created by the first", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");
    expect(createCallCount(calls)).toBe(2);
    const state = await readState(dirs, outcome.runId);
    // only the protected input is digested; the runtime input is checked but
    // never recorded
    expect(state.protected_inputs).toHaveLength(1);
    expect(state.protected_inputs[0].id).toBe("task");
    expect(JSON.stringify(state.protected_inputs)).not.toContain("notes");
    expect(JSON.stringify(state.events)).not.toContain("NOTES");
    expect(await readFile(join(dirs.workspace, NOTES_PATH), "utf8")).toBe(NOTES_BODY);
  });
});

test("6. a missing runtime input fails before the consuming session is created", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("NOTES.md");
    expect(outcome.detail).toContain("not accessible");
    // the first activation fully succeeded; the second never created a Session
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
    expect(runCalls(calls).length).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "runtime_input_missing" });
    expect(state.activations).toHaveLength(2);
    expect(state.activations[0].phase).toBe("session_cleanup_completed");
    expect(state.transitions).toHaveLength(1);
    expect(state.transitions[0].activation_index).toBe(1);
    expect(state.activations[1]).toMatchObject({
      index: 2,
      state_id: "second",
      phase: "failed",
      failure_reason: "runtime_input_missing",
    });
    expect(state.activations[1].session_id).toBeUndefined();
    // no session existed, so the cleanup step trivially completed
    expect(state.activations[1].session_cleanup).toBe("completed");
  });
});

test("7. worker failure of the second state: first transition kept, no second transition", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
      runCodeByState: { second: 3 },
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("agent container failed (exit 3)");
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);
    expect(runCalls(calls).length).toBe(2);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "worker_failed" });
    expect(state.cursor).toEqual({ current_state: "second", transition_count: 1 });
    expect(state.transitions).toHaveLength(1);
    expect(state.transitions[0].activation_index).toBe(1);
    expect(state.activations[1]).toMatchObject({
      index: 2,
      state_id: "second",
      phase: "failed",
      failure_reason: "worker_failed",
      session_id: "dhs_child_2",
      session_cleanup: "completed",
    });
    expect(state.terminal).toBeUndefined();
    expect(state.events.map((e: any) => e.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "activation_started",
      "session_created",
      "agent_running",
      "activation_failed",
      "run_failed",
    ]);
  });
});

test("8. the previous activation's result copied into the next: identity mismatch", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
      // the second state reuses the FIRST activation's identity: rejected
      resultByState: {
        second: (identity) =>
          JSON.stringify({
            schema_version: 2,
            run_id: identity.runId,
            state_id: "first",
            activation_index: 1,
            attempt: 1,
            status: "completed",
            summary: "replayed result",
            artifacts: [],
          }),
      },
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("state_id");
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);
    expect(runCalls(calls).length).toBe(2);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "agent_result_invalid" });
    expect(state.transitions).toHaveLength(1);
    expect(state.activations[1].phase).toBe("failed");
  });
});

test("9. a revisit uses a new activation path and a new session; the loop stops at max_transitions", async () => {
  await withFixture(async (dirs) => {
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), LOOP_YAML);
    const { calls, runner, sessionIds } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("transition budget");
    // two activations ran in two different sessions; the third never started
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);
    expect(runCalls(calls).length).toBe(2);
    expect(sessionIds).toEqual(["dhs_child_1", "dhs_child_2"]);

    // each activation has its own directory; the second cannot see the first's result
    const firstResult = activationResultFilePath(dirs.workspace, outcome.runId, 1, "execute");
    const secondResult = activationResultFilePath(dirs.workspace, outcome.runId, 2, "execute");
    expect(firstResult).not.toBe(secondResult);
    expect(firstResult).toContain("/activations/1-execute/attempt-1/result.json");
    expect(secondResult).toContain("/activations/2-execute/attempt-1/result.json");
    const firstIdentity = JSON.parse(await readFile(firstResult, "utf8"));
    const secondIdentity = JSON.parse(await readFile(secondResult, "utf8"));
    expect(firstIdentity.activation_index).toBe(1);
    expect(secondIdentity.activation_index).toBe(2);
    expect(firstIdentity.run_id).toBe(secondIdentity.run_id);
    expect(firstIdentity.state_id).toBe("execute");

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "transition_budget_exhausted" });
    expect(state.cursor).toEqual({ current_state: "execute", transition_count: 2 });
    expect(state.activations).toHaveLength(2);
    expect(state.activations.every((a: any) => a.state_id === "execute")).toBe(true);
    expect(state.activations.map((a: any) => a.session_id)).toEqual(["dhs_child_1", "dhs_child_2"]);
    expect(state.transitions).toHaveLength(2);
    expect(state.transitions.map((t: any) => t.to)).toEqual(["execute", "execute"]);
    expect(state.terminal).toBeUndefined();
  });
});

test("10. an entry terminal runs zero sessions", async () => {
  await withFixture(async (dirs) => {
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), ENTRY_TERMINAL_YAML);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.status).toBe("success");
    expect(createCallCount(calls)).toBe(0);
    expect(deleteCallCount(calls)).toBe(0);
    expect(runCalls(calls).length).toBe(0);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("success");
    expect(state.activations).toEqual([]);
    expect(state.transitions).toEqual([]);
    expect(state.cursor).toEqual({ current_state: "done", transition_count: 0 });
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.events.map((e: any) => e.kind)).toEqual(["run_created", "terminal_reached", "run_succeeded"]);
  });
});

test("11. a failed terminal is a normal graph result: durable terminal, exit 1", async () => {
  await withFixture(async (dirs) => {
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), FAILED_TERMINAL_YAML);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.terminal).toEqual({ state_id: "completed", result: "failed" });
    expect(state.failure).toEqual({ reason: "execution_failed" });
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.activations[0].phase).toBe("session_cleanup_completed");
    expect(state.events.map((e: any) => e.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
      "run_failed",
    ]);
  });
});

test("12. signal between states: the next session is never created", async () => {
  await withTwoStateFixture(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, releaseDelete, deleteStarted } = fakeCli({
      workspace: dirs.workspace,
      blockDelete: true,
      createNotesDuringFirstRun: true,
    });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    // the first activation succeeded and its delete is blocked: a signal here
    // sits between the two states
    await deleteStarted;
    signalHandler!("SIGINT");
    releaseDelete();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(130);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
    expect(runCalls(calls).length).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "signal_sigint" });
    // the first activation finished cleanly and its transition is durable;
    // the second session is never created
    expect(state.activations).toHaveLength(1);
    expect(state.activations[0]).toMatchObject({
      phase: "session_cleanup_completed",
      session_cleanup: "completed",
    });
    expect(state.transitions).toHaveLength(1);
    expect(state.transitions[0].activation_index).toBe(1);
    expect(state.terminal).toBeUndefined();
    expect(state.events.map((e: any) => e.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "run_failed",
    ]);
  });
});

test("13. signal during session create: the create completes, the session is deleted once, the worker never runs", async () => {
  await withFixture(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, releaseCreate, createStarted, events } = fakeCli({
      workspace: dirs.workspace,
      blockCreate: true,
    });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await createStarted;
    signalHandler!("SIGTERM");
    releaseCreate();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    // the create ran to completion, then the session was deleted once; no pull,
    // no run
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(events).not.toContain("pull:start");

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "signal_sigterm" });
    expect(state.activations[0]).toMatchObject({
      phase: "failed",
      failure_reason: "signal_sigterm",
      session_id: "dhs_child_1",
      session_cleanup: "completed",
    });
    expect(state.transitions).toEqual([]);
    expect(state.events.map((e: any) => e.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "activation_failed",
      "run_failed",
    ]);
  });
});

test("14. the timeout of the second state does not affect the first", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
      runTimedOutByState: { second: true },
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("timed out after 30 seconds");
    // the first activation completed fully with its own timeout
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);
    expect(runCalls(calls).length).toBe(2);
    expect(runCall(calls, 0).timeoutSeconds).toBe(3600);
    expect(runCall(calls, 1).timeoutSeconds).toBe(30);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "worker_timeout" });
    expect(state.transitions).toHaveLength(1);
    expect(state.transitions[0].activation_index).toBe(1);
    expect(state.activations[0].phase).toBe("session_cleanup_completed");
    expect(state.activations[1]).toMatchObject({ phase: "failed", failure_reason: "worker_timeout" });
    expect(state.terminal).toBeUndefined();
  });
});

test("15. a session cleanup failure forbids the transition", async () => {
  await withTwoStateFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
      deleteCode: 1,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.status).toBe("cleanup_failed");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
    expect(runCalls(calls).length).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("cleanup_failed");
    expect(state.failure).toEqual({ reason: "session_cleanup_failed" });
    expect(state.transitions).toEqual([]);
    expect(state.cursor).toEqual({ current_state: "first", transition_count: 0 });
    expect(state.activations[0]).toMatchObject({
      phase: "failed",
      failure_reason: "session_cleanup_failed",
      session_cleanup: "failed",
    });
    expect(state.activations).toHaveLength(1);
    expect(state.terminal).toBeUndefined();
  });
});

test("16. not_committed during the second activation: previous revision stays authoritative, run fails", async () => {
  await withTwoStateFixture(async (dirs) => {
    // two-state happy path until agent_running(2) is the 10th commit
    const io = faultIo({
      failCommit: 10,
      error: new PipelineStateStoreError("injected store failure at the second activation"),
    });
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    // the second worker never ran: the failed commit happened right after the
    // second session was created
    expect(runCalls(calls).length).toBe(1);
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);

    const state = await readState(dirs, outcome.runId);
    // commit 10 (agent_running of the second activation) failed; the
    // executor's activation_failed record (revision 10) and the final
    // run_failed commit (revision 11) both succeeded
    expect(state.revision).toBe(11);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "state_persist_failed" });
    expect(state.activations).toHaveLength(2);
    expect(state.activations[0].phase).toBe("session_cleanup_completed");
    expect(state.transitions).toHaveLength(1);
    expect(state.activations[1]).toMatchObject({
      phase: "failed",
      failure_reason: "state_persist_failed",
      session_id: "dhs_child_2",
      session_cleanup: "completed",
    });
    expect(state.terminal).toBeUndefined();
  });
});

test("17. durability_unknown during the second activation: candidate visible, poisoned, no further writes", async () => {
  await withTwoStateFixture(async (dirs) => {
    const io = faultIo({ failCommit: 10, failStep: "dirsync" });
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createNotesDuringFirstRun: true,
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("durability could not be confirmed");
    // the second worker never ran; the second session was still cleaned up
    expect(runCalls(calls).length).toBe(1);
    expect(createCallCount(calls)).toBe(2);
    expect(deleteCallCount(calls)).toBe(2);

    const state = await readState(dirs, outcome.runId);
    // the candidate revision is visible on disk; nothing after it was written
    expect(state.revision).toBe(10);
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.activations).toHaveLength(2);
    expect(state.activations[1].phase).toBe("agent_running");
    expect(state.transitions).toHaveLength(1);
    expect(state.terminal).toBeUndefined();
  });
});

test("18. several protected inputs; an artifact aliasing a protected input is rejected", async () => {
  await withFixture(async (dirs) => {
    await writeFile(join(dirs.workspace, "SPEC.md"), "spec body");
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), TWO_PROTECTED_YAML);
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: [WORK_PRODUCT_PATH, "SPEC.md"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("protected input");
    expect(createCallCount(calls)).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.protected_inputs).toHaveLength(2);
    expect(state.protected_inputs.map((input: any) => input.id)).toEqual(["task", "spec"]);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "agent_result_invalid" });
  });
});

test("18b. a symlinked artifact aliasing a protected input is rejected", async () => {
  await withFixture(async (dirs) => {
    await mkdir(join(dirs.workspace, "out"), { recursive: true });
    await symlink(join(dirs.workspace, "TASK.md"), join(dirs.workspace, "out", "alias.md"));
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: ["out/alias.md"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("protected input");
  });
});

test("18c. a hardlinked artifact aliasing a protected input is rejected", async () => {
  await withFixture(async (dirs) => {
    await mkdir(join(dirs.workspace, "out"), { recursive: true });
    await link(join(dirs.workspace, "TASK.md"), join(dirs.workspace, "out", "hard.md"));
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: ["out/hard.md"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("protected input");
  });
});

test("19. agent nonzero exit: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, runCode: 3 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("agent container failed (exit 3)");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("20. missing result.json: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeResult: false });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("agent result not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("21. invalid JSON result: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, resultBody: "{ not json" });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not valid JSON");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("22. unknown field in the result: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: [],
          extra: true,
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain('unknown field "extra"');
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("23. result attempt 2 is rejected", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 2,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: [],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("attempt");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("24. listed artifact missing on disk: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: [WORK_PRODUCT_PATH],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("is not readable");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("25. artifact escaping the workspace: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: ["../../escaped.txt"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("not a clean workspace-relative");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("26. artifact referencing the protected input: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      createArtifacts: false,
      resultBody: (identity) =>
        JSON.stringify({
          schema_version: 2,
          run_id: identity.runId,
          state_id: identity.stateId,
          activation_index: identity.activationIndex,
          attempt: 1,
          status: "completed",
          summary: SUMMARY_TEXT,
          artifacts: ["TASK.md"],
        }),
    });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("protected input");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("27. agent modified the protected input: run fails, session deleted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, modifyTask: true });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("was modified during the agent run");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("28. pull failure is non-fatal when the image may be local", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, pullCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");
    expect(runCalls(calls).length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("29. exact env projection; control and ambient material never reaches worker env; prompt/input bodies never in argv", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const run = runCall(calls);
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
      SUMMARY_TEXT,
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

test("30. no launcher/admin/state/canary markers in run args or env; only the workspace is mounted", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    const run = runCall(calls);
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

test("31. cleanup failure: overall result can never be success", async () => {
  await withFixture(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, deleteCode: 1 });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("cleanup_failed");
    expect(deleteCallCount(calls)).toBe(1);
  });
});

test("32. missing workspace input fails before any session is created", async () => {
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

test("33. workspace input escaping through a symlink is rejected", async () => {
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

test("34. workspace input that is a directory is rejected", async () => {
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

test("35. unknown profile referenced by the pipeline state: fails before any session is created", async () => {
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

test("36. missing required source env: fails before any session is created", async () => {
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

test("37. malformed profile: fails before any session is created", async () => {
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

test("38. profile with control destination: fails before any session is created", async () => {
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

test("39. profile symlink escape: fails before any session is created", async () => {
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

test("40. pipeline root that does not exist: fails before any session is created", async () => {
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

test("41. unsupported multi-state forms are rejected before auth and before any session", async () => {
  await withFixture(async (dirs) => {
    const twoOutcomes = BUNDLE_PIPELINE_YAML.replace(
      "    transitions:\n      - outcome: completed\n        to: completed",
      "    transitions:\n      - outcome: completed\n        to: completed\n      - outcome: blocked\n        to: completed",
    );
    await writeFile(join(dirs.pipelineRoot, "pipeline.yaml"), twoOutcomes);
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.runId).toBe("");
    expect(outcome.detail).toContain("exactly one transition");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("42. unsupported retries (max_attempts != 1) are rejected before any session", async () => {
  await withFixture(async (dirs) => {
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      BUNDLE_PIPELINE_YAML.replace("max_attempts: 1", "max_attempts: 2"),
    );
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("max_attempts");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("43. a custom result schema on a state is rejected before any session", async () => {
  await withFixture(async (dirs) => {
    await writeFile(
      join(dirs.pipelineRoot, "pipeline.yaml"),
      BUNDLE_PIPELINE_YAML.replace(
        "    result_schema: schemas/agent-result.schema.json",
        "    result_schema: schemas/custom.json",
      ),
    );
    await writeFile(join(dirs.pipelineRoot, "schemas", "custom.json"), JSON.stringify({ type: "object" }));
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const { deps, authCalls } = countingAuthDeps(dirs, runner);
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), deps);

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("standard agent result contract");
    expect(createCallCount(calls)).toBe(0);
    expect(authCalls.length).toBe(0);
  });
});

test("44. signal during the agent run: signal forwarded -> CLI exited -> session deleted", async () => {
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
      "session:create-done:dhs_child_1",
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
      "session:create-done:dhs_child_1",
      "pull:start",
      "pull:done",
      "run:start",
      "run:signal:SIGTERM",
      "run:exited:143",
      "session:delete",
    ]);
    expect(runCall(calls).signalOnAbort).toBe(true);
    expect(calls.find((c) => c.args[0] === "pull")?.signalOnAbort).toBe(false);
    expect(events[events.length - 1]).toBe("session:delete");

    const state = await readState(dirs, outcome.runId);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "signal_sigterm" });
    expect(state.activations[0]).toMatchObject({
      phase: "failed",
      failure_reason: "signal_sigterm",
      session_cleanup: "completed",
    });
    expect(state.transitions).toEqual([]);
  });
});

test("45. signal during pull: pull completes, run never starts, single cleanup", async () => {
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

    const state = await readState(dirs, outcome.runId);
    expect(state.activations[0]).toMatchObject({
      phase: "failed",
      failure_reason: "signal_sigint",
      session_cleanup: "completed",
    });
  });
});

test("46. cutoff: signal after the final write completed does not change the result", async () => {
  await withFixture(async (dirs) => {
    const gate = gateIoAtRename(9);
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

    // the outcome resolved only after the runner closed signal acceptance
    // synchronously before the single final write, so this late signal is a
    // no-op by the linearization contract
    signalHandler!("SIGTERM");
    expect(outcome.exitCode).toBe(0);
    expect(outcome.status).toBe("success");

    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(9);
    expect(state.status).toBe("success");
    expect(state.failure).toBeUndefined();
    expect(state.events[state.events.length - 1].kind).toBe("run_succeeded");
  });
});

test("47. signal while the blocked terminal write is in flight is late: persisted success, exit 0", async () => {
  await withFixture(async (dirs) => {
    const gate = gateIoAtRename(9);
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

    // the single terminal write is blocked at its rename; the signal cutoff
    // already happened before the write started, so the signal recorded while
    // the write is in flight is late and changes nothing
    await gate.reached;
    signalHandler!("SIGINT");
    gate.release();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.status).toBe("success");
    expect(deleteCallCount(calls)).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(9);
    expect(state.status).toBe("success");
    expect(state.phase).toBe("finished");
    expect(state.failure).toBeUndefined();
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
      "run_succeeded",
    ]);
  });
});

test("48. signal during cleanup, directly before the cutoff: persisted failed, exit 130", async () => {
  await withFixture(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, releaseDelete, deleteStarted } = fakeCli({
      workspace: dirs.workspace,
      blockDelete: true,
    });
    const pending = runAgentSmoke(
      agentSmokeOptions(dirs),
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );

    // the run succeeded and cleanup is blocked; the signal is recorded while
    // cleanup is in flight, directly before the cutoff
    await deleteStarted;
    signalHandler!("SIGINT");
    releaseDelete();
    const outcome = await pending;

    expect(outcome.exitCode).toBe(130);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("failed");
    expect(deleteCallCount(calls)).toBe(1);
    expect(runCalls(calls).length).toBe(1);

    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(9);
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "signal_sigint" });
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
      "run_failed",
    ]);
  });
});

test("49. transition commit failure: run fails, no transition recorded, failure is durable", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({
      failCommit: 7,
      error: new PipelineStateStoreError("injected store failure at the transition commit"),
    });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    // the worker ran exactly once, the session was deleted exactly once
    expect(runCalls(calls).length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    const state = await readState(dirs, outcome.runId);
    // commit 7 (the transition) failed; run_failed committed at revision 7
    expect(state.revision).toBe(7);
    expect(state.status).toBe("failed");
    expect(state.phase).toBe("finished");
    expect(state.failure).toEqual({ reason: "state_persist_failed" });
    expect(state.cursor).toEqual({ current_state: "execute", transition_count: 0 });
    expect(state.transitions).toEqual([]);
    expect(state.terminal).toBeUndefined();
    // the transition commit failed after the activation was already cleaned:
    // the durable activation stays cleaned and the run-level failure carries
    // the normalized persist reason
    expect(state.activations[0]).toMatchObject({ phase: "session_cleanup_completed" });
    expect(state.activations[0].failure_reason).toBeUndefined();
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "run_failed",
    ]);
  });
});

test("50. final commit failure: the complete active snapshot stays on disk, run is not ok", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({
      failCommit: 9,
      error: new PipelineStateStoreError("injected store failure at the final commit"),
    });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(runCalls(calls).length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    // the last good snapshot is the revision-8 terminal_reached state
    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(8);
    expect(state.status).toBe("active");
    expect(state.phase).toBe("finalizing");
    expect(state.failure).toBeUndefined();
    expect(state.terminal).toEqual({ state_id: "completed", result: "success" });
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.events.map((event: { kind: string }) => event.kind)).toEqual([
      "run_created",
      "activation_started",
      "session_created",
      "agent_running",
      "result_accepted",
      "session_cleanup_completed",
      "transition_committed",
      "terminal_reached",
    ]);
  });
});

test("51. transition durability failure: exit 1, one cleanup, candidate stays on disk, no further callback", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({ failCommit: 7, failStep: "dirsync" });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("durability could not be confirmed");
    // the worker ran exactly once and the Session was cleaned up exactly once
    expect(runCalls(calls).length).toBe(1);
    expect(deleteCallCount(calls)).toBe(1);

    // the candidate revision is visible on disk; the run is not reported ok
    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(7);
    expect(state.status).toBe("active");
    expect(state.cursor).toEqual({ current_state: "completed", transition_count: 1 });
    expect(state.transitions).toHaveLength(1);
    expect(state.terminal).toBeUndefined();
  });
});

test("52. terminal durability failure: exit 1, not reported as a confirmed success", async () => {
  await withFixture(async (dirs) => {
    const io = faultIo({ failCommit: 9, failStep: "dirsync" });
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner, { pipelineStateIo: io }));

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("durability could not be confirmed");
    expect(deleteCallCount(calls)).toBe(1);
    expect(runCalls(calls).length).toBe(1);

    // the candidate success snapshot is visible on disk, but the process
    // verdict is a failure: the durability of that revision is unknown
    const state = await readState(dirs, outcome.runId);
    expect(state.revision).toBe(9);
    expect(state.status).toBe("success");
    expect(state.phase).toBe("finished");
    expect(state.events[state.events.length - 1].kind).toBe("run_succeeded");
  });
});

test("53. timeout only on the worker run; timeout expiry fails the run with a single cleanup", async () => {
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
    expect(runCalls(calls).length).toBe(1);

    const run = runCall(calls);
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

test("54. worker argv, mounts, env and execution documents never contain the state root", async () => {
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
    const doc = await readFile(activationExecutionDocPath(dirs.workspace, outcome.runId, 1, "execute"), "utf8");
    expect(doc).not.toContain(dirs.state);
  });
});

test("55. two runs against the same bundle record the same execution snapshot digest", async () => {
  await withFixture(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const first = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    const second = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(first.ok).toBe(true);
    expect(second.ok).toBe(true);
    expect(first.runId).not.toBe(second.runId);

    const firstState = await readState(dirs, first.runId);
    const secondState = await readState(dirs, second.runId);
    const digest = pipelineExecutionDigest(await loadPipeline(dirs.pipelineRoot));

    expect(firstState.pipeline.execution_snapshot_sha256).toBe(digest);
    expect(secondState.pipeline.execution_snapshot_sha256).toBe(digest);
    // run identity is otherwise independent: different run ids, same bundle
    expect(firstState.pipeline.bundle_root).toBe(dirs.pipelineRoot);
    expect(secondState.pipeline.bundle_root).toBe(dirs.pipelineRoot);
    expect(firstState.protected_inputs[0].sha256).toBe(secondState.protected_inputs[0].sha256);
  });
});

test("56. pre-existing v1 state files are left untouched by new runs", async () => {
  await withFixture(async (dirs) => {
    const oldRunDir = join(dirs.state, "pipeline-runs", "legacy-v1-run");
    await mkdir(oldRunDir, { recursive: true });
    const v1State = JSON.stringify({
      schema_version: 1,
      revision: 7,
      run_id: "legacy-v1-run",
      status: "success",
      phase: "finished",
      started_at: "2026-01-01T00:00:00.000Z",
      updated_at: "2026-01-01T00:00:06.000Z",
      workspace: dirs.workspace,
      pipeline: await expectedIdentity(dirs),
      protected_input: { id: "task", path: "TASK.md", sha256: "b".repeat(64) },
      session_id: "old",
      cursor: { current_state: "completed", transition_count: 1 },
      attempt: { state_id: "execute", attempt: 1, profile: "default", session_id: "old", phase: "completed" },
      transitions: [],
      terminal: { state_id: "completed", result: "success" },
      events: [],
    });
    const oldPath = join(oldRunDir, "state.json");
    await writeFile(oldPath, `${v1State}\n`);
    // the v1 document is rejected as an unsupported version, not migrated
    const v1Document = JSON.parse(await readFile(oldPath, "utf8"));
    expect(() => validatePipelineRunState(v1Document)).toThrow(/schema_version 1, which is unsupported/);

    const { runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runAgentSmoke(agentSmokeOptions(dirs), makeDeps(dirs, runner));
    expect(outcome.exitCode).toBe(0);

    // the legacy file is byte-for-byte untouched
    expect(await readFile(oldPath, "utf8")).toBe(`${v1State}\n`);
    const state = await readState(dirs, outcome.runId);
    expect(state.schema_version).toBe(2);
    expect(state.run_id).toBe(outcome.runId);
  });
});

test("57. the default one-state pipeline executes through the graph engine", async () => {
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
      expect(diagnostics.filter((line) => line === "orchestrator: starting agent in child session (activation 1)").length).toBe(1);
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("58. a foreign result status does not advance the graph", async () => {
  await withFixture(async (dirs) => {
    const diagnostics: string[] = [];
    const restoreError = spyOn(console, "error").mockImplementation((message: unknown) => {
      diagnostics.push(String(message));
    });
    try {
      const { calls, runner } = fakeCli({
        workspace: dirs.workspace,
        resultBody: (identity) =>
          JSON.stringify({
            schema_version: 2,
            run_id: identity.runId,
            state_id: identity.stateId,
            activation_index: identity.activationIndex,
            attempt: 1,
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

test("59. arbitrary valid state ids through the engine", async () => {
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
      const state = await readState(dirs, outcome.runId);
      expect(state.activations[0].state_id).toBe("begin-step");
      expect(state.terminal.state_id).toBe("finish-success");
    } finally {
      restoreError.mockRestore();
    }
  });
});

test("60. input path comes from the pipeline", async () => {
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
    const run = runCall(calls);
    expect(envPairValue(run.args, "AGENT_SMOKE_INPUT_task")).toBe("/workspace/docs/input.md");
    expect(envPairValue(run.args, "AGENT_SMOKE_INPUT_task")).toContain("docs/input.md");
    const doc = await readFile(activationExecutionDocPath(dirs.workspace, outcome.runId, 1, "execute"), "utf8");
    expect(doc).toContain("- task (workspace-relative): docs/input.md");
    const state = await readState(dirs, outcome.runId);
    expect(state.protected_inputs[0].path).toBe("docs/input.md");
  });
});

test("extra: agent worker spec argv, entrypoint and env", () => {
  const spec = agentWorkerSpec({
    runId: "run-x",
    stateId: "execute",
    activationIndex: 3,
    attempt: 1,
    childSessionToken: CHILD_TOKEN,
    workerImage: "base:latest",
    inputs: [{ id: "task", pathInWorkspace: "docs/input.md" }],
    resultPathInWorkspace: ".pipeline-agent-smoke/run-x/activations/3-execute/attempt-1/result.json",
    executionDocPathInWorkspace: ".pipeline-agent-smoke/run-x/activations/3-execute/attempt-1/execution.md",
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
  expect(instruction).toContain("/workspace/.pipeline-agent-smoke/run-x/activations/3-execute/attempt-1/execution.md");
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
    `AGENT_SMOKE_ACTIVATION_INDEX=3`,
    `AGENT_SMOKE_ATTEMPT=1`,
    "AGENT_SMOKE_INPUT_task=/workspace/docs/input.md",
    `AGENT_SMOKE_RESULT_PATH=/workspace/.pipeline-agent-smoke/run-x/activations/3-execute/attempt-1/result.json`,
    "AGENT_SMOKE_RUN_ID=run-x",
    `AGENT_SMOKE_STATE_ID=execute`,
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
  const instruction = agentInstruction(".pipeline-agent-smoke/run/activations/1-execute/attempt-1/execution.md");
  expect(instruction).toContain("/workspace/.pipeline-agent-smoke/run/activations/1-execute/attempt-1/execution.md");
  expect(instruction).not.toContain(INPUT_MARKER);
  expect(instruction).not.toContain(INPUT_BODY.trim());
  expect(instruction).not.toContain(PROMPT_MARKER);
});

test("extra: result contract v2 identity", () => {
  const identity = { runId: "r", stateId: "execute", activationIndex: 2, attempt: 1 };
  const ok: AgentResult = {
    schema_version: 2,
    run_id: "r",
    state_id: "execute",
    activation_index: 2,
    attempt: 1,
    status: "completed",
    summary: "did the thing",
    artifacts: ["a/b.txt", ".pipeline-agent-smoke/work-product.txt"],
  };
  expect(parseAgentResult(JSON.stringify(ok), identity)).toEqual(ok);

  const invalid: Array<[string, RegExp]> = [
    ["not json", /not valid JSON/],
    ["[]", /not a JSON object/],
    [JSON.stringify({ schema_version: 1, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [] }), /schema_version/],
    [JSON.stringify({ schema_version: 2, run_id: "x", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [] }), /does not match this run/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "other", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [] }), /state_id/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 3, attempt: 1, status: "completed", summary: "s", artifacts: [] }), /activation_index/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 2, status: "completed", summary: "s", artifacts: [] }), /attempt/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "partial", summary: "s", artifacts: [] }), /status/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "", artifacts: [] }), /summary/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, summary: "s", artifacts: [] }), /status/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, status: "completed", summary: "s", artifacts: [] }), /attempt/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", attempt: 1, status: "completed", summary: "s", artifacts: [] }), /activation_index/],
    [JSON.stringify({ schema_version: 2, run_id: "r", status: "completed", summary: "s", artifacts: [] }), /state_id/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", artifacts: [] }), /summary/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s" }), /artifacts/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: "x" }), /not an array/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [42] }), /non-string/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: ["/etc/passwd"] }), /not workspace-relative/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: ["../../x"] }), /not a clean workspace-relative/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: ["a//b"] }), /not a clean workspace-relative/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [], extra: true }), /unknown field "extra"/],
    [JSON.stringify({ schema_version: 2, run_id: "r", state_id: "execute", activation_index: 2, attempt: 1, status: "completed", summary: "s", artifacts: [], note: "hi" }), /unknown field "note"/],
  ];
  for (const [body, pattern] of invalid) {
    expect(() => parseAgentResult(body, identity)).toThrow(pattern);
  }
});

test("extra: artifacts must not alias a protected input (direct, symlink, hardlink)", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-input-artifact-"));
  try {
    const workspace = join(root, "workspace");
    await mkdir(join(workspace, "out"), { recursive: true });
    await mkdir(join(workspace, "other"), { recursive: true });
    await writeFile(join(workspace, "input.md"), "input body");
    await writeFile(join(workspace, "SPEC.md"), "spec body");
    await writeFile(join(workspace, "out", "artifact.txt"), "x");
    await writeFile(join(workspace, "other", "input.md"), "separate file with a similar name");
    await symlink(join(workspace, "input.md"), join(workspace, "out", "link.md"));
    await symlink(join(workspace, "SPEC.md"), join(workspace, "out", "spec-link.md"));
    await link(join(workspace, "input.md"), join(workspace, "out", "hard.md"));
    const inputInfo = await stat(join(workspace, "input.md"));
    const specInfo = await stat(join(workspace, "SPEC.md"));
    const protectedInputs = [
      {
        canonical: await realpath(join(workspace, "input.md")),
        dev: inputInfo.dev,
        ino: inputInfo.ino,
      },
      {
        canonical: await realpath(join(workspace, "SPEC.md")),
        dev: specInfo.dev,
        ino: specInfo.ino,
      },
    ];
    const identity = { runId: "r", stateId: "execute", activationIndex: 1, attempt: 1 };
    const result = {
      schema_version: 2,
      run_id: "r",
      state_id: "execute",
      activation_index: 1,
      attempt: 1,
      status: "completed",
      summary: "s",
      artifacts: ["out/artifact.txt"],
    };
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["input.md"] }), identity, workspace, protectedInputs),
    ).rejects.toThrow(/resolves to a protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["out/link.md"] }), identity, workspace, protectedInputs),
    ).rejects.toThrow(/resolves to a protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["out/spec-link.md"] }), identity, workspace, protectedInputs),
    ).rejects.toThrow(/resolves to a protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["out/hard.md"] }), identity, workspace, protectedInputs),
    ).rejects.toThrow(/protected input/);
    await expect(
      verifyAgentResult(JSON.stringify({ ...result, artifacts: ["input.md/copy.txt"] }), identity, workspace, protectedInputs),
    ).rejects.toThrow(/not readable/);
    // a separate regular file with a similar name is not the protected input
    const verified = await verifyAgentResult(
      JSON.stringify({ ...result, artifacts: ["other/input.md", "out/artifact.txt"] }),
      identity,
      workspace,
      protectedInputs,
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
    const identity = { runId: "r", stateId: "execute", activationIndex: 1, attempt: 1 };
    const result = {
      schema_version: 2,
      run_id: "r",
      state_id: "execute",
      activation_index: 1,
      attempt: 1,
      status: "completed",
      summary: "s",
      artifacts: ["out/artifact.txt"],
    };
    const verified = await verifyAgentResult(JSON.stringify(result), identity, workspace);
    expect(verified.artifacts).toEqual(["out/artifact.txt"]);

    await expect(
      verifyAgentResult(
        JSON.stringify({ ...result, artifacts: ["out/missing.txt"] }),
        identity,
        workspace,
      ),
    ).rejects.toThrow(/not readable/);

    const outsideDir = join(root, "outside");
    await mkdir(outsideDir);
    await writeFile(join(outsideDir, "leak.txt"), "x");
    await expect(
      verifyAgentResult(
        JSON.stringify({ ...result, artifacts: [join("..", "outside", "leak.txt")] }),
        identity,
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
    const identity = { runId: "r", stateId: "execute", activationIndex: 1, attempt: 1 };
    const result = {
      schema_version: 2,
      run_id: "r",
      state_id: "execute",
      activation_index: 1,
      attempt: 1,
      status: "completed",
      summary: "s",
      artifacts: ["out/link.txt"],
    };
    await expect(
      verifyAgentResult(JSON.stringify(result), identity, workspace),
    ).rejects.toThrow(/resolves outside the workspace/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("extra: activation directories are separate per revisit", () => {
  expect(activationDirPath("/w", "run-1", 1, "execute")).toBe(
    "/w/.pipeline-agent-smoke/run-1/activations/1-execute/attempt-1",
  );
  expect(activationDirPath("/w", "run-1", 2, "execute")).toBe(
    "/w/.pipeline-agent-smoke/run-1/activations/2-execute/attempt-1",
  );
  expect(activationDirPath("/w", "run-1", 2, "review")).toBe(
    "/w/.pipeline-agent-smoke/run-1/activations/2-review/attempt-1",
  );
  expect(agentRunDirPath("/w", "run-1")).toBe("/w/.pipeline-agent-smoke/run-1");
});
