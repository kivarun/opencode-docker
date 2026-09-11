import { chmodSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  runPipelineV2,
  type PipelineV2RunOptions,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "../src/pipeline_v2_runner.ts";
import type { AuthFetcher, CliRunner, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import { translateProjectionPath } from "../src/projection_fs.ts";
import type { PipelineV2ExecutionState, PipelineV2RunState } from "../src/pipeline_v2_state.ts";

function asAgent(
  execution: PipelineV2ExecutionState | undefined,
): Extract<PipelineV2ExecutionState, { type: "agent" }> | null {
  return execution?.type === "agent" ? execution : null;
}

/**
 * Production runner tests for pipeline schema version 2: `runPipelineV2`
 * over the real v2 loader, real profiles, the shared Launcher authority,
 * the real durable run-state sink and the real Docker Helper runtime
 * adapter, with a fake CLI transport. Everything is deterministic — no
 * sleeps, no LLM, no Docker daemon: signals are delivered synchronously at
 * controlled await points (fake `fetchAuth`, fake session create/delete,
 * fake worker run, the sink dispatch clock and a proxy over one run-input
 * binding), and every rejected signal path is exercised without timing
 * races.
 */

const LOOSE_SCHEMA = {};

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations:
  - id: r1
    assert:
      not:
        all:
          - {fact: f1, equals: true}
          - {fact: f2, equals: true}
constraints:
  - id: c1
    when: {fact: f1, equals: true}
    forbid: [beta]
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const DECISION_TRANSITIONS = `      - outcome: alpha
        to: done
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

/** agent coder -> decision check -> done/failed_end; one json agent output. */
const PIPELINE_AGENT_DECISION = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json
  - id: source
    type: file
    protected: true

outputs:
  - id: report
    required: true
    source:
      state_output:
        state: coder
        output: report

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: report
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${DECISION_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** Two agent states in a row with two distinct profiles. */
const PIPELINE_TWO_AGENTS = `
schema_version: 2
entry_state: first
max_transitions: 20

inputs:
  - id: source
    type: file
    protected: true

outputs:
  - id: report
    required: true
    source:
      state_output:
        state: first
        output: report

states:
  - id: first
    type: agent
    profile: alpha
    prompt: prompts/first.md
    inputs: []
    outputs:
      - id: report
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: second
  - id: second
    type: agent
    profile: beta
    prompt: prompts/second.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const EXPECTED_LAUNCHER_ID = "dhl_runner";
const SECRET_ENV_VALUE = "sk-coder-secret-value";
const PROMPT_BODY = "implement the task\n";

interface Harness {
  root: string;
  stateRoot: string;
  daemonStateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource: string;
  credentialFile: string;
  sourceFile: string;
  factsFile: string;
}

async function writeBundle(root: string, yaml: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts", "coder.md"), PROMPT_BODY);
  await writeFile(join(bundle, "prompts", "first.md"), PROMPT_BODY);
  await writeFile(join(bundle, "prompts", "second.md"), PROMPT_BODY);
  await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify(LOOSE_SCHEMA));
  await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  return bundle;
}

async function writeProfile(configRoot: string, name: string, sourceVar: string): Promise<void> {
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(
    join(configRoot, "profiles", `${name}.yaml`),
    [
      "schema_version: 1",
      "image: ghcr.io/example/worker:1",
      `opencode_config: opencode/${name}.json`,
      "env:",
      "  MODEL_API_KEY:",
      `    from_env: ${sourceVar}`,
      "    required: true",
      "  GH_TOKEN:",
      "    from_env: GH_TOKEN",
      "    required: false",
      "",
    ].join("\n"),
  );
  await writeFile(join(configRoot, "opencode", `${name}.json`), JSON.stringify({ model: "glm53-flash" }));
}

async function setupHarness(yaml: string, profileNames: readonly string[]): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-runner-"));
  const bundle = await writeBundle(root, yaml);
  const configRoot = join(root, "config");
  let index = 0;
  for (const name of profileNames) {
    index += 1;
    await writeProfile(configRoot, name, `${name.toUpperCase()}_SOURCE_VAR_${index}`);
  }
  const projectSource = join(root, "project-src");
  await mkdir(projectSource, { recursive: true });
  await writeFile(join(projectSource, "seed.md"), "PROJECT-SEED\n");
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  const sourceFile = join(userdata, "source.txt");
  await writeFile(sourceFile, "SOURCE-BODY\n");
  const factsFile = join(userdata, "facts.json");
  await writeFile(factsFile, JSON.stringify({ f1: true, f2: false }));
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  return {
    root,
    stateRoot,
    daemonStateRoot: stateRoot,
    bundle,
    configRoot,
    projectSource,
    credentialFile,
    sourceFile,
    factsFile,
  };
}

const ROOTS: string[] = [];

async function setup(yaml: string, profileNames: readonly string[]): Promise<Harness> {
  const harness = await setupHarness(yaml, profileNames);
  ROOTS.push(harness.root);
  return harness;
}

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

interface CliScript {
  /** Runs before the fake answers a session create call (1-based index). */
  onCreate?: (index: number) => void | Promise<void>;
  /** Runs before the fake answers a session delete call (1-based index). */
  onDelete?: (index: number) => void | Promise<void>;
  /** Runs before the fake answers a worker run call (1-based index). */
  onRun?: () => void | Promise<void>;
  runCode?: number;
  runTimedOut?: boolean;
  deleteCode?: number;
  /** Output files the fake worker writes into the activation outputs mount. */
  workerOutputs: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: CliStdio;
  opts?: CliRunOptions;
}

interface FakeTransport {
  cli: CliRunner;
  calls: RecordedCall[];
  runRootOf: () => string;
}

function makeFakeCli(runRootOf: () => string, script: CliScript): FakeTransport {
  const calls: RecordedCall[] = [];
  let createIndex = 0;
  let deleteIndex = 0;
  let runIndex = 0;
  const cli: CliRunner = async (args, env, stdio, opts) => {
    calls.push({ args: [...args], env: { ...env }, stdio, opts });
    if (args[0] === "session" && args[1] === "create") {
      createIndex += 1;
      await script.onCreate?.(createIndex);
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_fake_${createIndex}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_fake_${createIndex}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      deleteIndex += 1;
      const id = args[args.indexOf("--id") + 1] ?? "";
      await script.onDelete?.(deleteIndex);
      return {
        code: script.deleteCode ?? 0,
        stderr: script.deleteCode === undefined ? undefined : "delete rejected",
        stdout: script.deleteCode === undefined ? JSON.stringify({ ok: true, deleted: true, id }) : "",
      };
    }
    if (args[0] === "pull") {
      return { code: 0 };
    }
    if (args[0] === "run") {
      await script.onRun?.();
      // The fake worker materializes the declared outputs into the
      // activation outputs mount, exactly like a real worker would.
      let mountStart = -1;
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] === "--mount") {
          mountStart = i;
          break;
        }
      }
      for (let i = mountStart; i >= 0 && i < args.length && args[i] === "--mount"; i += 2) {
        const spec = args[i + 1] ?? "";
        const [source, target] = spec.split(":");
        if (target === "/pipeline/outputs") {
          const segments = (source ?? "").split("/");
          const dash = segments[1] ?? "";
          const stateId = dash.slice(dash.indexOf("-") + 1);
          const outputs = script.workerOutputs[stateId];
          if (outputs !== undefined) {
            const dir = join(runRootOf(), source ?? "");
            await mkdir(dir, { recursive: true });
            for (const [name, content] of Object.entries(outputs)) {
              await writeFile(join(dir, name), content);
            }
          }
        }
      }
      return { code: script.runCode ?? 0, timedOut: script.runTimedOut ?? false };
    }
    return { code: 1 };
  };
  return { cli, calls, runRootOf };
}

function createCount(fake: FakeTransport): number {
  return fake.calls.filter((call) => call.args[0] === "session" && call.args[1] === "create").length;
}

function deleteIds(fake: FakeTransport): string[] {
  return fake.calls
    .filter((call) => call.args[0] === "session" && call.args[1] === "delete")
    .map((call) => call.args[call.args.indexOf("--id") + 1] ?? "");
}

interface Captured {
  outcome: PipelineV2RunOutcome | null;
  diagnostics: string[];
  clockCalls: number;
  authCalls: number;
  handlerCalls: number;
  transport: FakeTransport;
}

function captureDiagnostics(): { errors: string[]; restore: () => void } {
  const errors: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]): void => {
    errors.push(args.map((arg) => String(arg)).join(" "));
  };
  return { errors, restore: () => (console.error = original) };
}

const DEFAULT_BASE_ENV: Readonly<Record<string, string>> = {
  HOME: "/home/u",
  XDG_CONFIG_HOME: "/cfg",
  CODER_SOURCE_VAR_1: SECRET_ENV_VALUE,
  ALPHA_SOURCE_VAR_1: "alpha-value",
  BETA_SOURCE_VAR_2: "beta-value",
  GH_TOKEN: "gh-token-value",
};

interface RunScript {
  onAuth?: () => void | Promise<void>;
  authority?: string;
  onCreate?: (index: number) => void | Promise<void>;
  onDelete?: (index: number) => void | Promise<void>;
  onRun?: () => void | Promise<void>;
  runCode?: number;
  runTimedOut?: boolean;
  deleteCode?: number;
  signalAtClockCall?: number;
  signalKind?: "SIGINT" | "SIGTERM";
  /** Delivered by tests from inside fake handlers (auth, create, delete, run). */
  deliver?: (signal: "SIGINT" | "SIGTERM") => void;
  workerOutputs?: Readonly<Record<string, Readonly<Record<string, string>>>>;
  bindings?: PipelineV2RunOptions["inputBindings"];
  baseEnv?: Readonly<Record<string, string>>;
  projectSourcePath?: string;
  launcherId?: string;
  fixedRunId?: string;
}

async function runScript(
  harness: Harness,
  script: RunScript,
  registerSignal?: (handler: (signal: "SIGINT" | "SIGTERM") => void) => void,
): Promise<Captured> {
  let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
  const deliverSignal = (signal: "SIGINT" | "SIGTERM"): void => {
    signalHandler?.(signal);
  };
  let clockCalls = 0;
  let authCalls = 0;
  let handlerCalls = 0;
  let createIndex = 0;
  let deleteIndex = 0;
  let runRootKnown = "";
  const runRootOf = (): string => runRootKnown;
  const calls: RecordedCall[] = [];
  const cli: CliRunner = async (args, env, stdio, opts) => {
    calls.push({ args: [...args], env: { ...env }, stdio, opts });
    if (args[0] === "session" && args[1] === "create") {
      createIndex += 1;
      await script.onCreate?.(createIndex);
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_fake_${createIndex}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_fake_${createIndex}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      deleteIndex += 1;
      const id = args[args.indexOf("--id") + 1] ?? "";
      await script.onDelete?.(deleteIndex);
      return {
        code: script.deleteCode ?? 0,
        stderr: script.deleteCode === undefined ? undefined : "delete rejected",
        stdout: script.deleteCode === undefined ? JSON.stringify({ ok: true, deleted: true, id }) : "",
      };
    }
    if (args[0] === "pull") {
      return { code: 0 };
    }
    if (args[0] === "run") {
      await script.onRun?.();
      let mountStart = -1;
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] === "--mount") {
          mountStart = i;
          break;
        }
      }
      for (let i = mountStart; i >= 0 && i < args.length && args[i] === "--mount"; i += 2) {
        const spec = args[i + 1] ?? "";
        const [source, target] = spec.split(":");
        if (target === "/pipeline/outputs") {
          const segments = (source ?? "").split("/");
          const dash = segments[1] ?? "";
          const stateId = dash.slice(dash.indexOf("-") + 1);
          const outputs = script.workerOutputs?.[stateId];
          if (outputs !== undefined) {
            const dir = join(runRootKnown, source ?? "");
            await mkdir(dir, { recursive: true });
            for (const [name, content] of Object.entries(outputs)) {
              await writeFile(join(dir, name), content);
            }
          }
        }
      }
      return { code: script.runCode ?? 0, timedOut: script.runTimedOut ?? false };
    }
    return { code: 1 };
  };
  const transport: FakeTransport = { cli, calls, runRootOf };
  const fetchAuth: AuthFetcher = async () => {
    authCalls += 1;
    await script.onAuth?.();
    return {
      status: 200,
      body: {
        authority: script.authority ?? "launcher",
        principal: "michael",
        launcher_id: EXPECTED_LAUNCHER_ID,
      },
    };
  };
  const deps: PipelineV2RunnerDeps = {
    cli,
    fetchAuth,
    helperConfig: {
      socketPath: "/run/docker-helper/docker-helper.sock",
      credentialFile: harness.credentialFile,
    },
    baseEnv: script.baseEnv ?? DEFAULT_BASE_ENV,
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.daemonStateRoot },
    onSignal: (handler) => {
      signalHandler = (signal) => {
        handlerCalls += 1;
        handler(signal);
      };
      registerSignal?.(signalHandler);
    },
    now: () => {
      clockCalls += 1;
      if (script.signalAtClockCall === clockCalls) {
        deliverSignal(script.signalKind ?? "SIGINT");
      }
      return new Date(0);
    },
    randomId: () => {
      const fixed = script.fixedRunId ?? `run-${clockCalls}-${authCalls}`;
      // The worker run handler needs the run root for output writing.
      runRootKnown = join(harness.stateRoot, "pipeline-runs", fixed);
      return fixed;
    },
  };
  const options: PipelineV2RunOptions = {
    pipelineRoot: harness.bundle,
    configRoot: harness.configRoot,
    projectSourcePath: script.projectSourcePath ?? harness.projectSource,
    inputBindings: script.bindings ?? [
      { id: "facts_seed", path: harness.factsFile },
      { id: "source", path: harness.sourceFile },
    ],
    launcherId: script.launcherId ?? EXPECTED_LAUNCHER_ID,
  };
  const capturedDiagnostics = captureDiagnostics();
  let outcome: PipelineV2RunOutcome | null = null;
  try {
    outcome = await runPipelineV2(options, deps);
  } finally {
    capturedDiagnostics.restore();
  }
  return {
    outcome,
    diagnostics: capturedDiagnostics.errors,
    clockCalls,
    authCalls,
    handlerCalls,
    transport: { cli, calls, runRootOf },
  };
}

function expectOk(captured: Captured): PipelineV2RunState {
  const outcome = captured.outcome;
  if (outcome === null) {
    throw new Error("the runner produced no outcome");
  }
  expect(outcome.ok).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect(outcome.state).not.toBeNull();
  return outcome.state as PipelineV2RunState;
}

function statePath(harness: Harness, runId: string): string {
  return join(harness.stateRoot, "pipeline-runs", runId, "state.json");
}

function runRootPath(harness: Harness, runId: string): string {
  return join(harness.stateRoot, "pipeline-runs", runId);
}

function modeOf(info: { mode: number }): number {
  return info.mode & 0o777;
}

async function lstatOrNull(path: string): Promise<Awaited<ReturnType<typeof lstat>> | null> {
  try {
    return await lstat(path);
  } catch {
    return null;
  }
}

// --- happy path and preflight ------------------------------------------------

test("1. happy path: agent -> decision -> success through the real sink, runtime and data plane", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  const state = expectOk(captured);
  expect(captured.authCalls).toBe(1);
  expect(createCount(captured.transport)).toBe(2);
  expect(captured.transport.calls.filter((call) => call.args[0] === "run").length).toBe(1);
  expect(captured.transport.calls.filter((call) => call.args[0] === "pull").length).toBe(1);
  // Tool cleanup first, then the Execution session; exactly two deletes.
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_2", "dhs_fake_1"]);

  // Durable state v4: success with two settled executions and two
  // transitions.
  expect(state.status).toBe("success");
  expect(state.executions.length).toBe(2);
  expect(state.executions[0]?.type).toBe("agent");
  expect(state.executions[0]?.state_id).toBe("coder");
  expect(asAgent(state.executions[0])?.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(state.executions[1]?.type).toBe("decision");
  expect(state.executions[1]?.state_id).toBe("check");
  expect(state.transitions.length).toBe(2);
  expect(state.transitions[0]?.outcome).toBe("completed");
  expect(state.transitions[1]?.outcome).toBe("alpha");
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  expect(state.run_outputs?.[0]).toMatchObject({ id: "report", present: true });

  // The state document lives inside the run root.
  const rawState = await readFile(statePath(harness, runId), "utf8");
  expect(JSON.parse(rawState).run_id).toBe(runId);

  // Run outputs are published atomically under the run root.
  const published = await readFile(join(runRootPath(harness, runId), "outputs", "report"), "utf8");
  expect(JSON.parse(published)).toEqual({ f1: true, f2: false });

  // The project source stays untouched.
  expect((await readdir(harness.projectSource)).sort()).toEqual(["seed.md"]);
  expect(await readFile(join(harness.projectSource, "seed.md"), "utf8")).toBe("PROJECT-SEED\n");

  // Light worker argv checks: the profile image, one helper socket, the
  // fixed mount order.
  const runCall = captured.transport.calls.find((call) => call.args[0] === "run");
  expect(runCall).toBeDefined();
  const args = runCall?.args ?? [];
  expect(args.filter((arg) => arg === "--helper-socket").length).toBe(1);
  expect(args[args.indexOf("--image") + 1]).toBe("ghcr.io/example/worker:1");
  const mounts = args
    .map((arg, index) => (index > 0 && args[index - 1] === "--mount" ? arg : null))
    .filter((spec): spec is string => spec !== null)
    .map((spec) => spec.split(":")[1]);
  expect(mounts).toEqual(["/workspace", "/pipeline/inputs", "/pipeline/outputs"]);
});

test("2. run-root layout, permissions and local/daemon identity in host mode", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "11111111-2222-3333-4444-555555555555";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  expectOk(captured);
  const runRoot = runRootPath(harness, runId);
  expect(modeOf((await lstat(runRoot))!)).toBe(0o700);
  expect(modeOf((await lstat(join(harness.stateRoot, "pipeline-runs")))!)).toBe(0o700);
  expect(modeOf((await lstat(statePath(harness, runId)))!)).toBe(0o600);
  expect(modeOf((await lstat(join(runRoot, "project")))!)).toBe(0o700);
  for (const component of ["data", "activations", "outputs"]) {
    expect((await lstatOrNull(join(runRoot, component))) !== null).toBe(true);
  }
  // The local and daemon state roots are the same real object (host mode).
  const localInfo = await lstat(harness.stateRoot);
  const daemonInfo = await lstat(harness.daemonStateRoot);
  expect(daemonInfo.dev).toBe(localInfo.dev);
  expect(daemonInfo.ino).toBe(localInfo.ino);
  const daemonRunRoot = await lstat(join(harness.daemonStateRoot, "pipeline-runs", runId));
  expect(daemonRunRoot.dev).toBe(localInfo.dev);
  expect(daemonRunRoot.ino).toBe((await lstat(runRoot)).ino);
});

test("3. two distinct profiles load before auth, in declaration order, and the run succeeds", async () => {
  const harness = await setup(PIPELINE_TWO_AGENTS, ["alpha", "beta"]);
  const runId = "66666666-7777-8888-9999-000000000000";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { first: { report: "REPORT-CONTENT" }, second: {} },
    bindings: [{ id: "source", path: harness.sourceFile }],
  });
  const state = expectOk(captured);
  expect(captured.authCalls).toBe(1);
  expect(createCount(captured.transport)).toBe(4);
  expect(state.executions.length).toBe(2);
  expect(state.executions[0]?.state_id).toBe("first");
  expect(state.executions[1]?.state_id).toBe("second");
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
});

test("4. a broken second profile fails before auth, run root and sessions", async () => {
  const harness = await setup(PIPELINE_TWO_AGENTS, ["alpha", "beta"]);
  const captured = await runScript(harness, {
    fixedRunId: "broken-beta-0000-0000-000000000000",
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", ALPHA_SOURCE_VAR_1: "alpha-value" },
  });
  const outcome = captured.outcome;
  expect(outcome?.ok).toBe(false);
  expect(outcome?.exitCode).toBe(1);
  expect(outcome?.runId).toBe("");
  expect(outcome?.runRoot).toBeNull();
  expect(outcome?.state).toBeNull();
  expect(captured.authCalls).toBe(0);
  expect(createCount(captured.transport)).toBe(0);
  expect(captured.diagnostics.join("\n")).toContain("BETA_SOURCE_VAR_2");
  expect((await lstatOrNull(join(harness.stateRoot, "pipeline-runs"))) === null).toBe(true);
});

test("5. with both profiles broken, the first declaration (alpha) fails first", async () => {
  const harness = await setup(PIPELINE_TWO_AGENTS, ["alpha", "beta"]);
  const captured = await runScript(harness, {
    fixedRunId: "both-broken-0000-0000-000000000000",
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg" },
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.runRoot).toBeNull();
  expect(captured.authCalls).toBe(0);
  const message = captured.diagnostics.join("\n");
  expect(message).toContain("ALPHA_SOURCE_VAR_1");
  expect(message).not.toContain("BETA_SOURCE_VAR_2");
});

test("6. pipeline load failure creates no auth, run root or session", async () => {
  const harness = await setup("schema_version: 2\nentry_state: nope\n", []);
  const captured = await runScript(harness, { fixedRunId: "load-failure-0000-000000000000" });
  const outcome = captured.outcome;
  expect(outcome?.ok).toBe(false);
  expect(outcome?.exitCode).toBe(1);
  expect(outcome?.runId).toBe("");
  expect(outcome?.runRoot).toBeNull();
  expect(outcome?.state).toBeNull();
  expect(captured.authCalls).toBe(0);
  expect(createCount(captured.transport)).toBe(0);
  expect((await lstatOrNull(join(harness.stateRoot, "pipeline-runs"))) === null).toBe(true);
});

test("7. credential missing / wrong authority / launcher mismatch create no run root", async () => {
  // credential missing
  const missing = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  await rm(missing.credentialFile);
  {
    const captured = await runScript(missing, { fixedRunId: "no-cred-00000000-000000000000" });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.exitCode).toBe(1);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(captured.outcome?.state).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
    expect((await lstatOrNull(join(missing.stateRoot, "pipeline-runs"))) === null).toBe(true);
  }
  // wrong authority
  const wrong = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  {
    const captured = await runScript(wrong, {
      fixedRunId: "wrong-auth-000000-000000000000",
      authority: "principal",
    });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
    expect((await lstatOrNull(join(wrong.stateRoot, "pipeline-runs"))) === null).toBe(true);
  }
  // launcher mismatch
  const mismatch = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  {
    const captured = await runScript(mismatch, {
      fixedRunId: "wrong-lau-0000000-000000000000",
      launcherId: "dhl_other",
    });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
    expect((await lstatOrNull(join(mismatch.stateRoot, "pipeline-runs"))) === null).toBe(true);
  }
});

test("8. no secret value appears in the outcome, the durable state or the diagnostics", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "99999999-8888-7777-6666-555555555555";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  expectOk(captured);
  const canaries = [
    SECRET_ENV_VALUE,
    "cred-token-not-real",
    "dhc_fake_",
    PROMPT_BODY.trim(),
    "SOURCE-BODY",
    JSON.stringify({ f1: true, f2: false }),
  ];
  const outcomeText = JSON.stringify(captured.outcome);
  const stateText = await readFile(statePath(harness, runId), "utf8");
  const diagnosticsText = captured.diagnostics.join("\n");
  for (const canary of canaries) {
    expect(outcomeText).not.toContain(canary);
    expect(stateText).not.toContain(canary);
    expect(diagnosticsText).not.toContain(canary);
  }
  expect(Object.isFrozen(captured.outcome)).toBe(true);
  expect(Object.isFrozen(captured.outcome?.state)).toBe(true);
  expect(Object.isFrozen(captured.outcome?.state?.executions)).toBe(true);
});

// --- run-root ownership -------------------------------------------------------

test("9. a pre-existing run directory, file or symlink with the run id is never touched", async () => {
  for (const kind of ["directory", "file", "symlink"] as const) {
    const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
    const runId = `taken-${kind}-0000-0000-000000000000`;
    const target = runRootPath(harness, runId);
    await mkdir(join(harness.stateRoot, "pipeline-runs"), { recursive: true });
    if (kind === "directory") {
      await mkdir(target, { recursive: true });
      await writeFile(join(target, "sentinel.txt"), "KEEP-ME");
    } else if (kind === "file") {
      await writeFile(target, "KEEP-ME");
    } else {
      await mkdir(join(harness.stateRoot, "elsewhere"), { recursive: true });
      await symlink(join(harness.stateRoot, "elsewhere"), target);
    }
    const before = await lstat(target);
    const captured = await runScript(harness, { fixedRunId: runId });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.exitCode).toBe(1);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(captured.outcome?.state).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
    const after = await lstat(target);
    expect(after.isSymbolicLink()).toBe(before.isSymbolicLink());
    expect(after.isFile()).toBe(before.isFile());
    expect(after.isDirectory()).toBe(before.isDirectory());
    expect(after.ino).toBe(before.ino);
    if (kind === "directory") {
      expect(await readFile(join(target, "sentinel.txt"), "utf8")).toBe("KEEP-ME");
    }
    if (kind === "file") {
      expect(await readFile(target, "utf8")).toBe("KEEP-ME");
    }
    if (kind === "symlink") {
      expect(await realpath(target)).toBe(await realpath(join(harness.stateRoot, "elsewhere")));
    }
    expect((await lstatOrNull(statePath(harness, runId))) === null).toBe(true);
  }
});

test("10. symlinked or wrong-kind pipeline-runs and a projection mismatch are rejected", async () => {
  // symlinked pipeline-runs
  const symlinked = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  {
    const elsewhere = join(symlinked.root, "runs-elsewhere");
    await mkdir(elsewhere, { recursive: true });
    await symlink(elsewhere, join(symlinked.stateRoot, "pipeline-runs"));
    const captured = await runScript(symlinked, { fixedRunId: "symlinked-runs-000000000000" });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
    expect((await lstatOrNull(join(elsewhere, "symlinked-runs-000000000000"))) === null).toBe(true);
  }
  // pipeline-runs exists as a regular file
  const asFile = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  {
    await writeFile(join(asFile.stateRoot, "pipeline-runs"), "not a directory");
    const captured = await runScript(asFile, { fixedRunId: "runs-as-file-000000000000" });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
  }
  // the daemon state root is a different real directory (projection mismatch)
  const split = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  {
    split.daemonStateRoot = join(split.root, "daemon-state");
    await mkdir(split.daemonStateRoot, { recursive: true });
    const captured = await runScript(split, { fixedRunId: "split-roots-000000000000" });
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.runRoot).toBeNull();
    expect(createCount(captured.transport)).toBe(0);
  }
});

test("11. a successful exclusive run-root creation survives later failures", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "persist-0000-0000-0000-000000000000";
  // A missing project source fails the coordinator before any command.
  const missingProject = join(harness.root, "no-such-project");
  const captured = await runScript(harness, {
    fixedRunId: runId,
    projectSourcePath: missingProject,
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.reason).toBe("run_input_invalid");
  expect(captured.outcome?.runRoot).toBe(runRootPath(harness, runId));
  expect(captured.outcome?.state).toBeNull();
  expect(createCount(captured.transport)).toBe(0);
  const info = await lstat(runRootPath(harness, runId));
  expect(info.isDirectory()).toBe(true);
  expect(modeOf(info)).toBe(0o700);
  expect((await lstatOrNull(statePath(harness, runId))) === null).toBe(true);
});

test("12. durability-unknown at create_run: the adopted candidate is visible, no further dispatch", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "durab-0000-0000-0000-000000000000";
  const runRoot = runRootPath(harness, runId);
  // A proxy over the second binding fires during the snapshot phase (after
  // the project copy) and strips the run directory's read bit: the
  // create_run rename succeeds, the post-rename directory fsync fails with
  // EACCES, and the sink reports durability-unknown with the candidate.
  const poisonedBinding = new Proxy<{ id: string; path: string }>(
    { id: "source", path: harness.sourceFile },
    {
      get(target, property, receiver) {
        if (property === "id") {
          chmodSync(runRoot, 0o300);
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const captured = await runScript(harness, {
    fixedRunId: runId,
    bindings: [{ id: "facts_seed", path: harness.factsFile }, poisonedBinding],
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(1);
  expect(captured.outcome?.reason).toBe("state_persist_failed");
  expect(captured.outcome?.runRoot).toBe(runRoot);
  // The adopted candidate snapshot is reported, not null.
  expect(captured.outcome?.state?.run_id).toBe(runId);
  // Exactly one dispatch happened (create_run); no session was created.
  expect(captured.clockCalls).toBe(1);
  expect(createCount(captured.transport)).toBe(0);
  // The renamed candidate is durable on disk inside the run root.
  const raw = await readFile(statePath(harness, runId), "utf8");
  expect(JSON.parse(raw).run_id).toBe(runId);
  // Restore the mode so the teardown can remove the tree.
  chmodSync(runRoot, 0o700);
});

// --- signals -------------------------------------------------------------------

test("13. a signal accepted during auth leaves no run root and no state", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: "auth-sig-00000000-000000000000",
    onAuth: () => script.deliver?.("SIGINT"),
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(captured.outcome?.reason).toBe("signal_sigint");
  expect(captured.outcome?.runId).toBe("");
  expect(captured.outcome?.runRoot).toBeNull();
  expect(captured.outcome?.state).toBeNull();
  expect(createCount(captured.transport)).toBe(0);
  expect((await lstatOrNull(join(harness.stateRoot, "pipeline-runs"))) === null).toBe(true);
});

test("14. a signal accepted while the data plane parses bindings leaves no snapshot and no state", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "copy-sig-00000000-0000-000000000000";
  // The proxy fires when the data plane parses the bindings — after the
  // project copy returned — records the signal and throws.
  const trappingBinding = new Proxy<{ id: string; path: string }>(
    { id: "source", path: harness.sourceFile },
    {
      get() {
        script.deliver?.("SIGINT");
        throw new Error("binding trap");
      },
    },
  );
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    bindings: [{ id: "facts_seed", path: harness.factsFile }, trappingBinding],
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(captured.outcome?.reason).toBe("signal_sigint");
  expect(captured.outcome?.runRoot).toBe(runRootPath(harness, runId));
  expect(captured.outcome?.state).toBeNull();
  expect(createCount(captured.transport)).toBe(0);
  // The copy was published; no snapshot and no state document exist.
  expect((await lstatOrNull(join(runRootPath(harness, runId), "project"))) !== null).toBe(true);
  expect((await lstatOrNull(join(runRootPath(harness, runId), "data"))) === null).toBe(true);
  expect((await lstatOrNull(statePath(harness, runId))) === null).toBe(true);
});

test("15. a signal accepted during start_agent_execution creates no Session", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "start-sig-00000000-0000-000000000000";
  const captured = await runScript(harness, { fixedRunId: runId, signalAtClockCall: 2 });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(createCount(captured.transport)).toBe(0);
  const state = captured.outcome?.state;
  expect(state?.status).toBe("failed");
  expect(state?.failure?.reason).toBe("signal_sigint");
  const agent = asAgent(state?.executions[0]);
  expect(agent?.state_id).toBe("coder");
  expect(agent?.phase).toBe("failed");
  expect(agent?.session_cleanup).toEqual({ execution: "not_required", tool: "not_required" });
  expect(state?.transitions.length).toBe(0);
});

test("16. a signal during the Execution Session create records and cleans it, no Tool, no worker", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "exec-sig-00000000-0000-000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    onCreate: (index) => {
      if (index === 1) {
        script.deliver?.("SIGINT");
      }
    },
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(createCount(captured.transport)).toBe(1);
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_1"]);
  expect(captured.transport.calls.filter((call) => call.args[0] === "run").length).toBe(0);
  const state = captured.outcome?.state;
  const agent = asAgent(state?.executions[0]);
  expect(agent?.session_cleanup).toEqual({ execution: "completed", tool: "not_required" });
  expect(state?.failure?.reason).toBe("signal_sigint");
});

test("17. a signal during the Tool Session create cleans both sessions, no worker", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "tool-sig-00000000-0000-000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    onCreate: (index) => {
      if (index === 2) {
        script.deliver?.("SIGINT");
      }
    },
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(createCount(captured.transport)).toBe(2);
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
  expect(captured.transport.calls.filter((call) => call.args[0] === "run").length).toBe(0);
  const agent = asAgent(captured.outcome?.state?.executions[0]);
  expect(agent?.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
  expect(captured.outcome?.state?.failure?.reason).toBe("signal_sigint");
});

test("18. a signal during the worker run wins over the worker failure; cleanup Tool then Execution", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "work-sig-00000000-0000-000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    runCode: 1,
    onRun: () => {
      script.deliver?.("SIGINT");
    },
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  expect(captured.outcome?.reason).toBe("signal_sigint");
  expect(captured.outcome?.state?.failure?.reason).toBe("signal_sigint");
  expect(createCount(captured.transport)).toBe(2);
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
});

test("19. SIGTERM maps to exit 143 and signal_sigterm", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "term-sig-00000000-0000-000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    runCode: 1,
    onRun: () => {
      script.deliver?.("SIGTERM");
    },
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(143);
  expect(captured.outcome?.reason).toBe("signal_sigterm");
  expect(captured.outcome?.state?.failure?.reason).toBe("signal_sigterm");
});

test("20. a signal between states keeps the previous transition durable and starts no new execution", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "gap-sig-00000000-000000000000000000";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    signalAtClockCall: 9,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  const state = captured.outcome?.state;
  expect(state?.transitions.length).toBe(1);
  expect(state?.transitions[0]?.to).toBe("check");
  expect(state?.executions.length).toBe(1);
  expect(state?.executions[0]?.phase).toBe("cleanup_completed");
  expect(createCount(captured.transport)).toBe(2);
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
  expect(state?.failure?.reason).toBe("signal_sigint");
});

test("21. a signal during the decision execution records decision_failed", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "deci-sig-00000000-000000000000000000";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    signalAtClockCall: 10,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(130);
  const state = captured.outcome?.state;
  expect(state?.executions.length).toBe(2);
  expect(state?.executions[1]?.type).toBe("decision");
  expect(state?.executions[1]?.phase).toBe("failed");
  expect(state?.transitions.length).toBe(1);
  expect(state?.failure?.reason).toBe("signal_sigint");
});

test("22. a cleanup failure outranks a signal accepted during the same cleanup", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "clean-fail-0000-0000-000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
    deleteCode: 1,
    onDelete: () => {
      script.deliver?.("SIGINT");
    },
  }, (handler) => {
    script.deliver = handler;
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(1);
  expect(captured.outcome?.reason).toBe("session_cleanup_failed");
  expect(captured.outcome?.state?.status).toBe("cleanup_failed");
  expect(captured.outcome?.state?.failure?.reason).toBe("session_cleanup_failed");
  expect(deleteIds(captured.transport).length).toBe(2);
});

test("23. a signal delivered inside the final write after the cutoff is ignored", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "late-sig-00000000-000000000000000000";
  const captured = await runScript(harness, {
    fixedRunId: runId,
    signalAtClockCall: 15,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  });
  const state = expectOk(captured);
  expect(state.status).toBe("success");
  expect(captured.handlerCalls).toBe(1);
});

test("24. a late signal after the runner returned changes nothing", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "after-sig-00000000-000000000000000000";
  const script: { deliver?: (signal: "SIGINT" | "SIGTERM") => void } = {};
  const captured = await runScript(harness, {
    fixedRunId: runId,
    workerOutputs: { coder: { report: JSON.stringify({ f1: true, f2: false }) } },
  }, (handler) => {
    script.deliver = handler;
  });
  const state = expectOk(captured);
  const before = await readFile(statePath(harness, runId), "utf8");
  script.deliver?.("SIGINT");
  const after = await readFile(statePath(harness, runId), "utf8");
  expect(after).toBe(before);
  expect(state.status).toBe("success");
});

test("25. a runner timeout keeps worker_timeout when no signal is forwarded", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION, ["coder"]);
  const runId = "time-out-00000000-000000000000000000";
  const captured = await runScript(harness, { fixedRunId: runId, runCode: 1, runTimedOut: true });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(1);
  expect(captured.outcome?.reason).toBe("worker_timeout");
  expect(captured.outcome?.state?.failure?.reason).toBe("worker_timeout");
  expect(deleteIds(captured.transport)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
});

test("26. the projection suffix translation admits only clean relative paths", () => {
  expect(translateProjectionPath("/a", "/b", "/a")).toEqual({ ok: true, daemonPath: "/b" });
  expect(translateProjectionPath("/a", "/b", "/a/x/y")).toEqual({ ok: true, daemonPath: "/b/x/y" });
  expect(translateProjectionPath("/a", "/b", "/a/../x").ok).toBe(false);
  expect(translateProjectionPath("/a", "/b", "/z").ok).toBe(false);
});
