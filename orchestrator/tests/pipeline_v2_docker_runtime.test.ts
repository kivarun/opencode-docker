import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  snapshotRunInputs,
  type PreparedActivationData,
} from "../src/pipeline_v2_runtime.ts";
import {
  createDockerHelperPipelineV2Runtime,
  PIPELINE_V2_OPENCODE_CONFIG_SOURCE,
  PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE,
} from "../src/pipeline_v2_docker_runtime.ts";
import type { CliRunOptions, CliRunner, CliStdio } from "../src/docker_helper.ts";
import { DockerHelperError } from "../src/docker_helper.ts";
import type { ResolvedProfile } from "../src/profile.ts";
import type { PipelineV2ExecutionSession, PipelineV2ToolSession } from "../src/pipeline_v2_coordinator.ts";

const TASK_JSON = JSON.stringify({ goal: "the task" });
const CONFIG_JSON = JSON.stringify({ ok: true });

const RUNTIME_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true
  - id: specs
    type: directory
    protected: false
  - id: config
    type: json
    protected: true
    schema: schemas/config.schema.json

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: specs
        source:
          pipeline_input: specs
      - id: config
        source:
          pipeline_input: config
    outputs:
      - id: patch
        type: file
      - id: scratch
        type: directory
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: patch
        source:
          state_output:
            state: coder
            output: patch
      - id: scratch
        source:
          state_output:
            state: coder
            output: scratch
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: report
        type: file
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;

const SOCKET = "/run/docker-helper/docker-helper.sock";
const CREDENTIAL_FILE = "/home/op/.config/docker-helper/credential.token";
const OPERATOR_ENV = { HOME: "/home/op", XDG_CONFIG_HOME: "/cfg" };
const EXPECTED_LAUNCHER_ID = "dhl_launcher";
const INSTRUCTION = "Read /pipeline/inputs/.orchestrator/execution.md and follow it exactly.";

interface Setup {
  root: string;
  runRoot: string;
  bundle: string;
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>;
  snap: Awaited<ReturnType<typeof snapshotRunInputs>>;
  profiles: Map<string, ResolvedProfile>;
  coderPrepared: PreparedActivationData;
  coderView: {
    readonly type: "agent";
    readonly id: string;
    readonly profile: string;
    readonly promptPath: string;
    readonly promptContent: string;
    readonly timeout_seconds: number;
    readonly max_attempts: number;
  };
}

async function writeBundle(root: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), RUNTIME_YAML);
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "prompts", "architect.md"), "review the patch\n");
  await writeFile(
    join(bundle, "schemas", "config.schema.json"),
    JSON.stringify({ type: "object", required: ["ok"] }),
  );
  return bundle;
}

async function writeSources(root: string): Promise<string> {
  const dir = join(root, "userdata");
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "task.txt"), TASK_JSON);
  await mkdir(join(dir, "specs", "sub"), { recursive: true });
  await writeFile(join(dir, "specs", "a.txt"), "A");
  await writeFile(join(dir, "specs", "sub", "b.txt"), "B");
  await writeFile(join(dir, "config.json"), CONFIG_JSON);
  return dir;
}

async function makeProfiles(): Promise<Map<string, ResolvedProfile>> {
  const profiles = new Map<string, ResolvedProfile>();
  profiles.set("coder", {
    profileName: "coder",
    image: "ghcr.io/example/coder:1",
    opencodeConfigPath: "/cfg/coder.json",
    opencodeConfigContent: JSON.stringify({ model: "coder-model" }),
    env: { MODEL_API_KEY: "sk-coder-secret", SHARED_SETTING: "shared-value-1" },
  });
  profiles.set("architect", {
    profileName: "architect",
    image: "ghcr.io/example/architect:1",
    opencodeConfigPath: "/cfg/architect.json",
    opencodeConfigContent: JSON.stringify({ model: "architect-model" }),
    env: { MODEL_API_KEY: "sk-architect-secret", ARCH_ONLY: "arch-only-value" },
  });
  return profiles;
}

async function setup(): Promise<Setup> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-docker-runtime-"));
  const bundle = await writeBundle(root);
  const sources = await writeSources(root);
  const runRoot = join(root, "run");
  await mkdir(runRoot, { recursive: true });
  await mkdir(join(runRoot, "project"), { mode: 0o700 });
  await writeFile(join(runRoot, "project", "README.md"), "project seed\n");
  const pipeline = await loadPipelineV2(bundle);
  const snap = await snapshotRunInputs(
    pipeline,
    [
      { id: "task", path: join(sources, "task.txt") },
      { id: "specs", path: join(sources, "specs") },
      { id: "config", path: join(sources, "config.json") },
    ],
    runRoot,
  );
  const profiles = await makeProfiles();
  const coderPrepared = await prepareActivationData(pipeline, snap, [], "coder", 1);
  return {
    root,
    runRoot,
    bundle,
    pipeline,
    snap,
    profiles,
    coderPrepared,
    coderView: {
      type: "agent",
      id: "coder",
      profile: "coder",
      promptPath: join(bundle, "prompts", "coder.md"),
      promptContent: "implement the task\n",
      timeout_seconds: 60,
      max_attempts: 1,
    },
  };
}

async function dispose(setup_: Setup): Promise<void> {
  await rm(setup_.root, { recursive: true, force: true });
}

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: CliStdio;
  opts?: CliRunOptions;
}

interface FakeCliOptions {
  pullCode?: number;
  runCode?: number;
  runTimedOut?: boolean;
  /** Launcher id reported per create call; a function is consumed in order. */
  createLauncherId?: string | ((sessionNumber: number) => string);
  /** Exit code and diagnostics for session delete calls. */
  deleteCode?: number;
  deleteStderr?: string;
}

function makeFakeCli(
  options: FakeCliOptions = {},
): { cli: CliRunner; calls: RecordedCall[] } {
  const calls: RecordedCall[] = [];
  let sessionCounter = 0;
  const launcherIdFor = (sessionNumber: number): string => {
    const configured = options.createLauncherId;
    if (configured === undefined) {
      return EXPECTED_LAUNCHER_ID;
    }
    return typeof configured === "function" ? configured(sessionNumber) : configured;
  };
  const cli: CliRunner = async (args, env, stdio, opts) => {
    calls.push({ args: [...args], env: { ...env }, stdio, opts });
    if (args[0] === "session" && args[1] === "create") {
      sessionCounter += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${sessionCounter}`, launcher_id: launcherIdFor(sessionCounter) },
          token: `dhc_${sessionCounter}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      const id = args[args.indexOf("--id") + 1] ?? "";
      return {
        code: options.deleteCode ?? 0,
        stderr: options.deleteStderr,
        stdout: options.deleteCode === undefined ? JSON.stringify({ ok: true, deleted: true, id }) : "",
      };
    }
    if (args[0] === "pull") {
      return { code: options.pullCode ?? 0 };
    }
    if (args[0] === "run") {
      return { code: options.runCode ?? 0, timedOut: options.runTimedOut ?? false };
    }
    return { code: 1 };
  };
  return { cli, calls };
}

function makeRuntime(
  setup_: Setup,
  cli: CliRunner,
  overrides: {
    profiles?: Map<string, ResolvedProfile>;
    expectedLauncherId?: string;
  } = {},
) {
  return createDockerHelperPipelineV2Runtime({
    pipeline: setup_.pipeline,
    profiles: overrides.profiles ?? setup_.profiles,
    cli,
    helperConfig: { socketPath: SOCKET, credentialFile: CREDENTIAL_FILE },
    operatorEnv: OPERATOR_ENV,
    expectedLauncherId: overrides.expectedLauncherId ?? EXPECTED_LAUNCHER_ID,
  });
}

async function driveCoderActivation(
  setup_: Setup,
  overrides: { pullCode?: number; runCode?: number; runTimedOut?: boolean } = {},
): Promise<{
  runtime: ReturnType<typeof makeRuntime>;
  execution: PipelineV2ExecutionSession;
  tool: PipelineV2ToolSession;
  result: Awaited<ReturnType<PipelineV2ExecutionSession["runAgent"]>>;
  calls: RecordedCall[];
}> {
  const fake = makeFakeCli(overrides);
  const runtime = makeRuntime(setup_, fake.cli);
  const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
  const tool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
  const result = await execution.runAgent(tool);
  return { runtime, execution, tool, result, calls: fake.calls };
}

function runCall(calls: RecordedCall[]): RecordedCall {
  const call = calls.find((candidate) => candidate.args[0] === "run");
  expect(call).toBeDefined();
  return call as RecordedCall;
}

function pullCall(calls: RecordedCall[]): RecordedCall {
  const call = calls.find((candidate) => candidate.args[0] === "pull");
  expect(call).toBeDefined();
  return call as RecordedCall;
}

const CODER_RUN_ARGS = [
  "run",
  "--endpoint",
  SOCKET,
  "--image",
  "ghcr.io/example/coder:1",
  "--entrypoint",
  "opencode",
  "--workdir",
  "/workspace",
  "--helper-socket",
  "--mount",
  "project:/workspace",
  "--mount",
  "activations/1-coder/data/inputs:/pipeline/inputs:ro",
  "--mount",
  "activations/1-coder/data/outputs:/pipeline/outputs",
  "--env-from",
  `DOCKER_HELPER_SESSION_TOKEN=${PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE}`,
  "--env-from",
  `OPENCODE_CONFIG_CONTENT=${PIPELINE_V2_OPENCODE_CONFIG_SOURCE}`,
  "--env-from",
  "MODEL_API_KEY=ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY",
  "--env-from",
  "SHARED_SETTING=ORCHESTRATOR_V2_PROFILE_SHARED_SETTING",
  "--",
  "run",
  "--format",
  "json",
  "--auto",
  INSTRUCTION,
];

test("1. the factory rejects a missing profile before any helper or filesystem side effect", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const onlyCoder = new Map(
      [...setup_.profiles.entries()].filter(([name]) => name === "coder"),
    );
    expect(() =>
      makeRuntime(setup_, fake.cli, { profiles: onlyCoder }),
    ).toThrow(/agent state "architect" requires profile "architect" which is not loaded/);
    expect(fake.calls.length).toBe(0);
  } finally {
    await dispose(setup_);
  }
});

test("2. profile snapshots are immune to mutations of the source map and profile objects", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    // mutate the source map and the source profile objects after the
    // factory snapshot was taken
    setup_.profiles.delete("architect");
    const coderProfile = setup_.profiles.get("coder")!;
    coderProfile.image = "ghcr.io/evil/image:9";
    coderProfile.opencodeConfigContent = "EVIL-CONFIG";
    coderProfile.env.MODEL_API_KEY = "EVIL-SECRET";
    coderProfile.env.SHARED_SETTING = "EVIL-SHARED";

    const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const tool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    const result = await execution.runAgent(tool);
    expect(result).toEqual({ status: "completed" });
    const run = runCall(fake.calls);
    expect(run.args).toEqual(CODER_RUN_ARGS);
    expect(run.env[PIPELINE_V2_OPENCODE_CONFIG_SOURCE]).toBe(JSON.stringify({ model: "coder-model" }));
    expect(run.env.ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY).toBe("sk-coder-secret");
    expect(run.env.ORCHESTRATOR_V2_PROFILE_SHARED_SETTING).toBe("shared-value-1");
    // user objects are never frozen or modified by the factory
    expect(Object.isFrozen(coderProfile)).toBe(false);
    expect(Object.isFrozen(setup_.profiles)).toBe(false);
  } finally {
    await dispose(setup_);
  }
});

test("3. the Execution Session is created with the run-root workspace", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    expect(fake.calls.length).toBe(1);
    const call = fake.calls[0]!;
    expect(call.args).toEqual([
      "session",
      "create",
      "--endpoint",
      SOCKET,
      "--json",
      "--workspace",
      setup_.runRoot,
    ]);
    expect(call.env).toEqual(OPERATOR_ENV);
  } finally {
    await dispose(setup_);
  }
});

test("4. the Tool Session is created with the project-root workspace", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const tool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    expect(execution.sessionId).not.toBe(tool.sessionId);
    const toolCreate = fake.calls[1]!;
    expect(toolCreate.args).toEqual([
      "session",
      "create",
      "--endpoint",
      SOCKET,
      "--json",
      "--workspace",
      setup_.coderPrepared.project_root,
    ]);
    expect(toolCreate.env).toEqual(OPERATOR_ENV);
  } finally {
    await dispose(setup_);
  }
});

test("5. the two sessions carry different ids and tokens, and the handles expose only id and lifecycle", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    expect(driven.execution.sessionId).toBe("dhs_1");
    expect(driven.tool.sessionId).toBe("dhs_2");
    const run = runCall(driven.calls);
    // the CLI authorizes with the Execution bearer; the worker receives the
    // Tool bearer under its destination name
    expect(run.env["DOCKER_HELPER_SESSION_TOKEN"]).toBe("dhc_1");
    expect(run.env[PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE]).toBe("dhc_2");
    expect(Object.keys(driven.execution).sort()).toEqual(["cleanup", "runAgent", "sessionId"]);
    expect(Object.keys(driven.tool).sort()).toEqual(["cleanup", "sessionId"]);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("6. a launcher ownership mismatch deletes the known session and throws", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli({ createLauncherId: "dhl_other" });
    const runtime = makeRuntime(setup_, fake.cli, { expectedLauncherId: EXPECTED_LAUNCHER_ID });
    let failure: unknown;
    try {
      await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(DockerHelperError);
    expect((failure as DockerHelperError).kind).toBe("wrong_authority");
    expect((failure as Error).message).toContain(
      "session dhs_1 belongs to launcher dhl_other, expected dhl_launcher; the known session was deleted (cleanup confirmed)",
    );
    expect(fake.calls.length).toBe(2);
    expect(fake.calls[1]!.args).toEqual([
      "session",
      "delete",
      "--endpoint",
      SOCKET,
      "--json",
      "--id",
      "dhs_1",
    ]);

    // the same rule applies to the Tool session: its create is the one
    // that mismatches, the Execution create matched
    const fake2 = makeFakeCli({
      createLauncherId: (sessionNumber) =>
        sessionNumber === 1 ? EXPECTED_LAUNCHER_ID : "dhl_other",
    });
    const runtime2 = makeRuntime(setup_, fake2.cli, { expectedLauncherId: EXPECTED_LAUNCHER_ID });
    const execution2 = await runtime2.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    let toolFailure: unknown;
    try {
      await runtime2.createToolSession(setup_.coderView, setup_.coderPrepared);
    } catch (cause) {
      toolFailure = cause;
    }
    expect((toolFailure as DockerHelperError).kind).toBe("wrong_authority");
    expect((toolFailure as Error).message).toContain(
      "session dhs_2 belongs to launcher dhl_other, expected dhl_launcher",
    );
    expect((toolFailure as Error).message).toContain("the known session was deleted (cleanup confirmed)");
    expect(fake2.calls.length).toBe(3);
    expect(fake2.calls[2]!.args).toEqual([
      "session",
      "delete",
      "--endpoint",
      SOCKET,
      "--json",
      "--id",
      "dhs_2",
    ]);
    await execution2.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("6a. an Execution session mismatch with a failing delete surfaces cli_failure and claims nothing about deletion", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli({
      createLauncherId: "dhl_other",
      deleteCode: 1,
      deleteStderr: "error: API error (status 403, code forbidden): delete rejected",
    });
    const runtime = makeRuntime(setup_, fake.cli, { expectedLauncherId: EXPECTED_LAUNCHER_ID });
    let failure: unknown;
    try {
      await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(DockerHelperError);
    expect((failure as DockerHelperError).kind).toBe("cli_failure");
    const message = (failure as Error).message;
    expect(message).toContain(
      "session dhs_1 belongs to launcher dhl_other, expected dhl_launcher",
    );
    expect(message).toContain("cleanup could not be confirmed");
    expect(message).toContain("delete rejected");
    expect(message).not.toContain("was deleted");
    expect(message).not.toContain("dhc_");
    expect(message).not.toContain("dhc_1");
    // exactly one physical delete attempt: create + delete, no retry
    expect(fake.calls.length).toBe(2);
    expect(fake.calls[1]!.args).toEqual([
      "session",
      "delete",
      "--endpoint",
      SOCKET,
      "--json",
      "--id",
      "dhs_1",
    ]);
  } finally {
    await dispose(setup_);
  }
});

test("6b. a Tool session mismatch with a failing delete surfaces cli_failure and claims nothing about deletion", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli({
      createLauncherId: (sessionNumber) =>
        sessionNumber === 1 ? EXPECTED_LAUNCHER_ID : "dhl_other",
      deleteCode: 2,
      deleteStderr: "error: helper daemon unreachable",
    });
    const runtime = makeRuntime(setup_, fake.cli, { expectedLauncherId: EXPECTED_LAUNCHER_ID });
    const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    let toolFailure: unknown;
    try {
      await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    } catch (cause) {
      toolFailure = cause;
    }
    expect(toolFailure).toBeInstanceOf(DockerHelperError);
    expect((toolFailure as DockerHelperError).kind).toBe("cli_failure");
    const message = (toolFailure as Error).message;
    expect(message).toContain(
      "session dhs_2 belongs to launcher dhl_other, expected dhl_launcher",
    );
    expect(message).toContain("cleanup could not be confirmed");
    expect(message).toContain("helper daemon unreachable");
    expect(message).not.toContain("was deleted");
    expect(message).not.toContain("dhc_");
    expect(message).not.toContain("dhc_2");
    // exactly one physical delete attempt for the Tool session: two creates
    // (execution matched) + one delete, no retry
    expect(fake.calls.length).toBe(3);
    expect(fake.calls[2]!.args).toEqual([
      "session",
      "delete",
      "--endpoint",
      SOCKET,
      "--json",
      "--id",
      "dhs_2",
    ]);
    // the fake fails every delete, so the deferred Execution cleanup also
    // reports its failure instead of claiming success
    let cleanupFailure: unknown;
    try {
      await execution.cleanup();
    } catch (cause) {
      cleanupFailure = cause;
    }
    expect(cleanupFailure).toBeInstanceOf(DockerHelperError);
  } finally {
    await dispose(setup_);
  }
});

test("7. the pull uses the exact argv and the Execution bearer only", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const pull = pullCall(driven.calls);
    expect(pull.args).toEqual(["pull", "--endpoint", SOCKET, "ghcr.io/example/coder:1"]);
    expect(pull.env).toEqual({ DOCKER_HELPER_SESSION_TOKEN: "dhc_1" });
    expect(pull.stdio).toBe("inherit");
    expect(pull.opts).toBeUndefined();
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("8. the worker run uses the exact argv with the fixed mount order and :ro only on inputs", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const run = runCall(driven.calls);
    expect(run.args).toEqual(CODER_RUN_ARGS);
    expect(run.stdio).toBe("inherit");
    expect(run.opts).toEqual({ signalOnAbort: true, timeoutSeconds: 60 });
    const mounts: string[] = [];
    for (let index = 0; index < run.args.length; index += 1) {
      if (run.args[index] === "--mount") {
        mounts.push(run.args[index + 1] ?? "");
      }
    }
    expect(mounts).toEqual([
      "project:/workspace",
      "activations/1-coder/data/inputs:/pipeline/inputs:ro",
      "activations/1-coder/data/outputs:/pipeline/outputs",
    ]);
    // `:ro` appears exactly once — on the inputs mount
    expect(run.args.filter((arg) => arg.endsWith(":ro")).length).toBe(1);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("9. the helper socket is projected exactly once", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const run = runCall(driven.calls);
    expect(run.args.filter((arg) => arg === "--helper-socket").length).toBe(1);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("10./11./12. authority separation: tool bearer to the worker, execution bearer for the CLI, launcher credential nowhere", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const run = runCall(driven.calls);
    const pull = pullCall(driven.calls);
    // 10: the worker receives the Tool bearer under the session token name
    const tokenFrom = run.args[run.args.indexOf("--env-from") + 1];
    expect(tokenFrom).toBe("DOCKER_HELPER_SESSION_TOKEN=ORCHESTRATOR_V2_TOOL_SESSION_TOKEN");
    expect(run.env[PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE]).toBe("dhc_2");
    // 11: the CLI process authorizes with the Execution bearer
    expect(run.env["DOCKER_HELPER_SESSION_TOKEN"]).toBe("dhc_1");
    expect(pull.env["DOCKER_HELPER_SESSION_TOKEN"]).toBe("dhc_1");
    // 12: the launcher credential (path or content) never reaches the
    // worker env or argv
    const runEnvValues = Object.values(run.env);
    expect(runEnvValues).not.toContain(CREDENTIAL_FILE);
    expect(run.env).not.toHaveProperty("DOCKER_HELPER_CONFIG");
    expect(run.args).not.toContain(CREDENTIAL_FILE);
    expect(JSON.stringify(run.env)).not.toContain("credential");
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("13./14. no secret values, prompt bodies or input bodies in argv; no prompt or input bodies in env", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const run = runCall(driven.calls);
    const argvText = run.args.join("\n");
    // 13: profile secrets, the OpenCode config content and both tokens are
    // absent from argv
    expect(run.args).not.toContain("sk-coder-secret");
    expect(run.args).not.toContain("sk-architect-secret");
    expect(run.args).not.toContain(JSON.stringify({ model: "coder-model" }));
    expect(argvText.includes("dhc_1")).toBe(false);
    expect(argvText.includes("dhc_2")).toBe(false);
    // 14: the prompt body and the input file bodies never appear in argv
    // or env
    expect(argvText.includes("implement the task")).toBe(false);
    expect(argvText.includes(TASK_JSON)).toBe(false);
    expect(JSON.stringify(run.env).includes("implement the task")).toBe(false);
    expect(JSON.stringify(run.env)).not.toContain(TASK_JSON);
    expect(JSON.stringify(run.env)).not.toContain('"goal"');
    // by design the env carries the secrets under private source names
    // (never in argv); verify the private names are the only carriers
    expect(run.env[PIPELINE_V2_OPENCODE_CONFIG_SOURCE]).toBe(JSON.stringify({ model: "coder-model" }));
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("15. the prepared activation exposes the fixed execution document paths", async () => {
  const setup_ = await setup();
  try {
    expect(setup_.coderPrepared.execution_document.container_path).toBe(
      "/pipeline/inputs/.orchestrator/execution.md",
    );
    expect(setup_.coderPrepared.execution_document.host_path).toBe(
      join(setup_.coderPrepared.inputs_root, ".orchestrator", "execution.md"),
    );
    const doc = await readFile(setup_.coderPrepared.execution_document.host_path, "utf8");
    expect(doc).toContain('state_id: "coder"');
    expect(doc).toContain("implement the task");
  } finally {
    await dispose(setup_);
  }
});

test("17. forged, cross-runtime and cross-activation tool handles are rejected before any CLI call", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const callsAfterExecutionCreate = fake.calls.length;
    const realTool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    const callsAfterToolCreate = fake.calls.length;

    // a hand-built tool-shaped object is rejected without reading it
    const forged = { sessionId: "dhs_forged", cleanup: async (): Promise<void> => {} };
    await expect(execution.runAgent(forged as unknown as PipelineV2ToolSession)).rejects.toThrow(
      /runAgent requires the exact Tool Session handle created by this runtime/,
    );
    expect(fake.calls.length).toBe(callsAfterToolCreate);

    // a Proxy around the real tool handle is rejected without invoking
    // any trap
    let trapCount = 0;
    const proxyTool = new Proxy(realTool, {
      get(target, property, receiver) {
        trapCount += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    await expect(execution.runAgent(proxyTool as PipelineV2ToolSession)).rejects.toThrow(
      /requires the exact Tool Session handle created by this runtime/,
    );
    expect(trapCount).toBe(0);
    expect(fake.calls.length).toBe(callsAfterToolCreate);

    // a spread copy of the real handle is unregistered (a full
    // structuredClone cannot even clone the function members)
    const clone = { ...realTool } as unknown as PipelineV2ToolSession;
    await expect(execution.runAgent(clone)).rejects.toThrow(
      /requires the exact Tool Session handle created by this runtime/,
    );
    expect(fake.calls.length).toBe(callsAfterToolCreate);

    // a second runtime instance's tool handle is unregistered here
    const fakeB = makeFakeCli();
    const runtimeB = makeRuntime(setup_, fakeB.cli);
    const executionB = await runtimeB.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const toolB = await runtimeB.createToolSession(setup_.coderView, setup_.coderPrepared);
    await expect(execution.runAgent(toolB)).rejects.toThrow(
      /requires the exact Tool Session handle created by this runtime/,
    );
    expect(fake.calls.length).toBe(callsAfterToolCreate);

    // cross-activation: the architect activation's tool handle does not
    // belong to the coder execution session, and vice versa
    await writeFile(join(setup_.coderPrepared.outputs_root, "patch"), "PATCH");
    const coderRecords = await acceptActivationOutputs(setup_.pipeline, setup_.coderPrepared);
    const architectPrepared = await prepareActivationData(
      setup_.pipeline,
      setup_.snap,
      coderRecords,
      "architect",
      2,
    );
    const architectView = {
      type: "agent" as const,
      id: "architect",
      profile: "architect",
      promptPath: join(setup_.bundle, "prompts", "architect.md"),
      promptContent: "review the patch\n",
      timeout_seconds: 30,
      max_attempts: 1,
    };
    const execution2 = await runtime.createExecutionSession(architectView, architectPrepared);
    const tool2 = await runtime.createToolSession(architectView, architectPrepared);
    await expect(execution2.runAgent(realTool)).rejects.toThrow(/does not belong to this Execution Session/);
    await expect(execution.runAgent(tool2)).rejects.toThrow(/does not belong to this Execution Session/);
    expect(fake.calls.filter((call) => call.args[0] === "pull").length).toBe(0);
    expect(fake.calls.filter((call) => call.args[0] === "run").length).toBe(0);

    // a forged prepared activation is rejected before any CLI call
    const forgedPrepared = { ...setup_.coderPrepared } as unknown as PreparedActivationData;
    await expect(runtime.createExecutionSession(setup_.coderView, forgedPrepared)).rejects.toThrow(
      /the runtime requires the frozen prepared activation data object/,
    );
    await expect(runtime.createToolSession(setup_.coderView, forgedPrepared)).rejects.toThrow(
      /the runtime requires the frozen prepared activation data object/,
    );

    // a tool session cannot be created without the uncleaned execution
    // session of the same activation
    const fakeC = makeFakeCli();
    const runtimeC = makeRuntime(setup_, fakeC.cli);
    await expect(
      runtimeC.createToolSession(setup_.coderView, setup_.coderPrepared),
    ).rejects.toThrow(/the Tool Session requires the uncleaned Execution Session/);
    expect(fakeC.calls.length).toBe(0);

    // cleanup everything created here
    await execution.cleanup();
    await realTool.cleanup();
    await execution2.cleanup();
    await tool2.cleanup();
    await executionB.cleanup();
    await toolB.cleanup();
  } finally {
    await dispose(setup_);
  }
});


test("18. a repeated runAgent is rejected before any CLI call", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const runCount = driven.calls.filter((call) => call.args[0] === "run").length;
    await expect(driven.execution.runAgent(driven.tool)).rejects.toThrow(
      /the Execution Session already ran the worker/,
    );
    expect(driven.calls.filter((call) => call.args[0] === "run").length).toBe(runCount);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("19. session cleanup is memoized: repeated calls delete exactly once", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    const execution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    await Promise.all([execution.cleanup(), execution.cleanup(), execution.cleanup()]);
    await execution.cleanup();
    const deletes = fake.calls.filter((call) => call.args[1] === "delete");
    expect(deletes.length).toBe(1);
    expect(deletes[0]!.args).toContain("dhs_1");
  } finally {
    await dispose(setup_);
  }
});

test("20. the Tool and Execution cleanups are separate operations", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    await driven.tool.cleanup();
    let deletes = driven.calls.filter((call) => call.args[1] === "delete");
    expect(deletes.length).toBe(1);
    expect(deletes[0]!.args).toContain(driven.tool.sessionId);
    // the tool cleanup already happened: a worker run is no longer possible
    await expect(driven.execution.runAgent(driven.tool)).rejects.toThrow(/already cleaned/);
    await driven.execution.cleanup();
    deletes = driven.calls.filter((call) => call.args[1] === "delete");
    expect(deletes.length).toBe(2);
    expect(deletes[1]!.args).toContain(driven.execution.sessionId);
  } finally {
    await dispose(setup_);
  }
});

test("21. a pull failure is a worker failure and never reaches the worker run", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_, { pullCode: 1 });
    expect(driven.result).toEqual({ status: "failed", reason: "worker_failed" });
    expect(driven.calls.filter((call) => call.args[0] === "run").length).toBe(0);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("22. a timed-out worker run reports worker_timeout", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_, { runTimedOut: true });
    expect(driven.result).toEqual({ status: "failed", reason: "worker_timeout" });
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("23. a nonzero worker run reports worker_failed", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_, { runCode: 7 });
    expect(driven.result).toEqual({ status: "failed", reason: "worker_failed" });
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("24. a successful worker run reports completed and the stdout never participates", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    expect(driven.result).toEqual({ status: "completed" });
    const run = runCall(driven.calls);
    expect(run.opts).toEqual({ signalOnAbort: true, timeoutSeconds: 60 });
    // the run subprocess environment contains exactly the Execution bearer
    // for the CLI authority and the private --env-from source variables
    expect(Object.keys(run.env).sort()).toEqual([
      "DOCKER_HELPER_SESSION_TOKEN",
      "ORCHESTRATOR_V2_OPENCODE_CONFIG_CONTENT",
      "ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY",
      "ORCHESTRATOR_V2_PROFILE_SHARED_SETTING",
      "ORCHESTRATOR_V2_TOOL_SESSION_TOKEN",
    ]);
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("25. different agent states use their own image, env and config", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    // coder activation first
    const coderExecution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const coderTool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    expect(await coderExecution.runAgent(coderTool)).toEqual({ status: "completed" });

    // architect activation with the accepted coder outputs
    await writeFile(join(setup_.coderPrepared.outputs_root, "patch"), "PATCH");
    const coderRecords = await acceptActivationOutputs(setup_.pipeline, setup_.coderPrepared);
    const architectPrepared = await prepareActivationData(
      setup_.pipeline,
      setup_.snap,
      coderRecords,
      "architect",
      2,
    );
    const architectView = {
      type: "agent" as const,
      id: "architect",
      profile: "architect",
      promptPath: join(setup_.bundle, "prompts", "architect.md"),
      promptContent: "review the patch\n",
      timeout_seconds: 30,
      max_attempts: 1,
    };
    const architectExecution = await runtime.createExecutionSession(architectView, architectPrepared);
    const architectTool = await runtime.createToolSession(architectView, architectPrepared);
    expect(await architectExecution.runAgent(architectTool)).toEqual({ status: "completed" });

    const coderRun = runCall(fake.calls.filter((call) => call.args.includes("ghcr.io/example/coder:1")));
    const architectRun = runCall(fake.calls.filter((call) => call.args.includes("ghcr.io/example/architect:1")));
    expect(coderRun.env[PIPELINE_V2_OPENCODE_CONFIG_SOURCE]).toBe(JSON.stringify({ model: "coder-model" }));
    expect(coderRun.env.ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY).toBe("sk-coder-secret");
    expect(coderRun.env.ORCHESTRATOR_V2_PROFILE_SHARED_SETTING).toBe("shared-value-1");
    expect(architectRun.env[PIPELINE_V2_OPENCODE_CONFIG_SOURCE]).toBe(JSON.stringify({ model: "architect-model" }));
    expect(architectRun.env.ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY).toBe("sk-architect-secret");
    expect(architectRun.env.ORCHESTRATOR_V2_PROFILE_ARCH_ONLY).toBe("arch-only-value");
    expect(architectRun.args).toContain("activations/2-architect/data/inputs:/pipeline/inputs:ro");
    expect(architectRun.opts).toEqual({ signalOnAbort: true, timeoutSeconds: 30 });

    await coderTool.cleanup();
    await coderExecution.cleanup();
    await architectTool.cleanup();
    await architectExecution.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("26. env-from source names are stable across activations and never collide with destinations", async () => {
  const setup_ = await setup();
  try {
    const fake = makeFakeCli();
    const runtime = makeRuntime(setup_, fake.cli);
    const coderExecution = await runtime.createExecutionSession(setup_.coderView, setup_.coderPrepared);
    const coderTool = await runtime.createToolSession(setup_.coderView, setup_.coderPrepared);
    expect(await coderExecution.runAgent(coderTool)).toEqual({ status: "completed" });

    await writeFile(join(setup_.coderPrepared.outputs_root, "patch"), "PATCH");
    const coderRecords = await acceptActivationOutputs(setup_.pipeline, setup_.coderPrepared);
    const architectPrepared = await prepareActivationData(
      setup_.pipeline,
      setup_.snap,
      coderRecords,
      "architect",
      2,
    );
    const architectView = {
      type: "agent" as const,
      id: "architect",
      profile: "architect",
      promptPath: join(setup_.bundle, "prompts", "architect.md"),
      promptContent: "review the patch\n",
      timeout_seconds: 30,
      max_attempts: 1,
    };
    const architectExecution = await runtime.createExecutionSession(architectView, architectPrepared);
    const architectTool = await runtime.createToolSession(architectView, architectPrepared);
    expect(await architectExecution.runAgent(architectTool)).toEqual({ status: "completed" });

    const coderRun = runCall(fake.calls.filter((call) => call.args.includes("ghcr.io/example/coder:1")));
    const architectRun = runCall(fake.calls.filter((call) => call.args.includes("ghcr.io/example/architect:1")));
    const sourcesOf = (run: RecordedCall): string[] => {
      const pairs: string[] = [];
      for (let index = 0; index < run.args.length; index += 1) {
        if (run.args[index] === "--env-from") {
          pairs.push(run.args[index + 1] ?? "");
        }
      }
      return pairs;
    };
    const coderPairs = sourcesOf(coderRun);
    const architectPairs = sourcesOf(architectRun);
    // the fixed pairs are identical across states and activations
    expect(coderPairs.slice(0, 2)).toEqual([
      `DOCKER_HELPER_SESSION_TOKEN=${PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE}`,
      `OPENCODE_CONFIG_CONTENT=${PIPELINE_V2_OPENCODE_CONFIG_SOURCE}`,
    ]);
    expect(architectPairs.slice(0, 2)).toEqual(coderPairs.slice(0, 2));
    // profile pairs keep the destination-sorted order
    expect(coderPairs.slice(2)).toEqual([
      "MODEL_API_KEY=ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY",
      "SHARED_SETTING=ORCHESTRATOR_V2_PROFILE_SHARED_SETTING",
    ]);
    expect(architectPairs.slice(2)).toEqual([
      "ARCH_ONLY=ORCHESTRATOR_V2_PROFILE_ARCH_ONLY",
      "MODEL_API_KEY=ORCHESTRATOR_V2_PROFILE_MODEL_API_KEY",
    ]);
    // no source name equals any destination name
    for (const pair of [...coderPairs, ...architectPairs]) {
      const separator = pair.indexOf("=");
      const destination = pair.slice(0, separator);
      const source = pair.slice(separator + 1);
      expect(source.startsWith("ORCHESTRATOR_V2_")).toBe(true);
      expect(destination.startsWith("ORCHESTRATOR_V2_")).toBe(false);
      expect(destination).not.toBe(source);
    }
    await coderTool.cleanup();
    await coderExecution.cleanup();
    await architectTool.cleanup();
    await architectExecution.cleanup();
  } finally {
    await dispose(setup_);
  }
});

test("27. the run argv carries no absolute host paths", async () => {
  const setup_ = await setup();
  try {
    const driven = await driveCoderActivation(setup_);
    const run = runCall(driven.calls);
    for (const arg of run.args) {
      expect(arg.includes(setup_.runRoot)).toBe(false);
      expect(arg.includes(setup_.root)).toBe(false);
      expect(arg.includes(setup_.bundle)).toBe(false);
    }
    // every mount source is a clean workspace-relative path
    const mountArgs: string[] = [];
    for (let index = 0; index < run.args.length; index += 1) {
      if (run.args[index] === "--mount") {
        mountArgs.push(run.args[index + 1] ?? "");
      }
    }
    for (const mount of mountArgs) {
      expect(mount.startsWith("/")).toBe(false);
      const sourcePart = mount.slice(0, mount.indexOf(":"));
      expect(sourcePart.split("/")).not.toContain("..");
      expect(sourcePart.split("/")).not.toContain("");
    }
    await driven.execution.cleanup();
    await driven.tool.cleanup();
  } finally {
    await dispose(setup_);
  }
});
