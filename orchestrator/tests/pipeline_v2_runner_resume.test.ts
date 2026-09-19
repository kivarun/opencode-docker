import { chmodSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  resumePipelineV2,
  type PipelineV2ResumeOptions,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "../src/pipeline_v2_runner.ts";
import type { AuthFetcher, CliRunner, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareDecisionStateData,
  evaluatePreparedDecisionState,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { loadPipelineV2, type PipelineDecisionStateResult, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { parsePipelineV2RunState, type PipelineDecisionStateRecord, type PipelineV2AgentExecutionState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { pipelineV2RunStatePath } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { createHash } from "node:crypto";

/**
 * Production runner resume tests for pipeline schema version 2:
 * `resumePipelineV2` over the real v2 loader, real profiles, the shared
 * Launcher authority, the read-only existing-run-root verification, the
 * read-only `PipelineV2RunStateSink.open`, the read-only runtime-context
 * restoration and the single coordinator resume entrypoint, with a fake
 * CLI transport. The prefix of every resumed run is built with the real
 * production state and data-plane APIs (the run-owned project copy, the
 * run-input snapshot, the real durable reducer through the real sink and
 * the real activation preparation/acceptance) — never hand-built JSON.
 * Everything is deterministic: no sleeps, no LLM, no Docker daemon.
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

const DECISION_END_TRANSITIONS = `      - outcome: alpha
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
${DECISION_END_TRANSITIONS}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** coder (json out facts) -> check(beta -> coder revisit, alpha -> probe) -> probe -> done. */
const PIPELINE_DECISION_FROM_OUTPUT = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs: []

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
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
          state_output:
            state: coder
            output: facts
    transitions:
      - outcome: alpha
        to: probe
      - outcome: beta
        to: coder
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: probe
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    outputs:
      - id: report
        type: file
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** Bounded cycle: coder -> coder2 -> coder with a transition budget of 2. */
const PIPELINE_CYCLE_BUDGET = `
schema_version: 2
entry_state: coder
max_transitions: 2

inputs: []

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder2
  - id: coder2
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: coder
  - id: done
    type: terminal
    result: success
`;

const EXPECTED_LAUNCHER_ID = "dhl_runner";
const SECRET_ENV_VALUE = "sk-coder-secret-value";
const PROMPT_BODY = "implement the task\n";
const FACTS_ALPHA = JSON.stringify({ f1: true, f2: false });
const FACTS_BETA = JSON.stringify({ f1: false, f2: true });

interface Harness {
  root: string;
  stateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource: string;
  credentialFile: string;
  sourceFile: string;
  factsFile: string;
  runRoot: (runId: string) => string;
}

async function writeBundle(root: string, yaml: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts", "coder.md"), PROMPT_BODY);
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
      "",
    ].join("\n"),
  );
  await writeFile(join(configRoot, "opencode", `${name}.json`), JSON.stringify({ model: "glm53-flash" }));
}

async function setupHarness(yaml: string): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-runner-resume-"));
  const bundle = await writeBundle(root, yaml);
  const configRoot = join(root, "config");
  await writeProfile(configRoot, "coder", "CODER_SOURCE_VAR_1");
  const projectSource = join(root, "project-src");
  await mkdir(projectSource, { recursive: true });
  await writeFile(join(projectSource, "seed.md"), "PROJECT-SEED\n");
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  const sourceFile = join(userdata, "source.txt");
  await writeFile(sourceFile, "SOURCE-BODY\n");
  const factsFile = join(userdata, "facts.json");
  await writeFile(factsFile, FACTS_ALPHA);
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  return {
    root,
    stateRoot,
    bundle,
    configRoot,
    projectSource,
    credentialFile,
    sourceFile,
    factsFile,
    runRoot: (runId: string) => join(stateRoot, "pipeline-runs", runId),
  };
}

const ROOTS: string[] = [];

async function setup(yaml: string): Promise<Harness> {
  const harness = await setupHarness(yaml);
  ROOTS.push(harness.root);
  return harness;
}

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

// --- the prefix builder (real production state/data-plane APIs) -------------

let clockValue = 0;
function nextTick(): Date {
  clockValue += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
}

interface PrefixRunner {
  pipeline: ResolvedPipelineV2;
  runId: string;
  runRoot: string;
  sink: PipelineV2RunStateSink;
  runInputs: RunInputsSnapshot;
  accepted: AcceptedStateOutput[];
}

async function buildPrefix(
  harness: Harness,
  pipeline: ResolvedPipelineV2,
  runId: string,
): Promise<PrefixRunner> {
  clockValue = 0;
  const runRoot = harness.runRoot(runId);
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { mode: 0o700 });
  await mkdir(runRoot, { mode: 0o700 });
  await prepareRunProject(harness.projectSource, runRoot);
  const bindings = pipeline.inputs.map((input) => {
    if (input.id === "facts_seed") {
      return { id: input.id, path: harness.factsFile };
    }
    throw new Error(`prefix fixture has no source file for input ${input.id}`);
  }) as readonly RunInputBinding[];
  const runInputs = await snapshotRunInputs(pipeline, bindings, runRoot);
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId, now: nextTick });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  return { pipeline, runId, runRoot, sink, runInputs, accepted: [] };
}

async function prefixAgentStep(
  prefix: PrefixRunner,
  stateId: string,
  executionIndex: number,
  jsonContent: Record<string, string> = {},
  sessionIds: { execution: string; tool: string } = {
    execution: `pfx-exec-${executionIndex}`,
    tool: `pfx-tool-${executionIndex}`,
  },
): Promise<PreparedActivationData> {
  const state = prefix.pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type !== "agent") {
    throw new Error(`prefix fixture has no agent state ${JSON.stringify(stateId)}`);
  }
  const activation = await prepareActivationData(
    prefix.pipeline,
    prefix.runInputs,
    prefix.accepted,
    stateId,
    executionIndex,
  );
  await prefix.sink.dispatch({ kind: "start_agent_execution", stateId, profile: state.profile });
  await prefix.sink.dispatch({ kind: "agent_data_prepared" });
  await prefix.sink.dispatch({ kind: "agent_execution_session_created", sessionId: sessionIds.execution });
  await prefix.sink.dispatch({ kind: "agent_tool_session_created", sessionId: sessionIds.tool });
  await prefix.sink.dispatch({ kind: "agent_running" });
  for (const port of activation.output_ports) {
    if (port.type === "directory") {
      await mkdir(port.path, { recursive: true });
    } else if (port.type === "json") {
      await writeFile(port.path, jsonContent[port.id] ?? JSON.stringify({ ok: true, port: port.id }));
    } else {
      await writeFile(port.path, `${port.id} body`);
    }
  }
  const records = await acceptActivationOutputs(prefix.pipeline, activation);
  await prefix.sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await prefix.sink.dispatch({ kind: "agent_cleanup_completed" });
  const target = state.transitions[0];
  if (target === undefined) {
    throw new Error("prefix fixture agent state has no transition");
  }
  await prefix.sink.dispatch({
    kind: "transition_committed",
    step: { from: stateId, outcome: "completed", to: target.to, transition_index: 0 },
    executionIndex,
  });
  prefix.accepted.push(...records);
  return activation;
}

function toSelectedRecord(result: PipelineDecisionStateResult): PipelineDecisionStateRecord {
  if (result.status !== "selected") {
    throw new Error(`the prefix fixture only routes selected decisions, got ${result.status}`);
  }
  return {
    status: "selected",
    outcome: result.outcome,
    decision: result.decision,
    rule_id: result.rule_id,
    active_constraint_ids: [...result.active_constraint_ids],
  };
}

async function prefixDecisionStep(
  prefix: PrefixRunner,
  stateId: string,
  executionIndex: number,
): Promise<PipelineDecisionStateResult> {
  const state = prefix.pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined || state.type !== "decision") {
    throw new Error(`prefix fixture has no decision state ${JSON.stringify(stateId)}`);
  }
  const prepared = await prepareDecisionStateData(
    prefix.pipeline,
    prefix.runInputs,
    prefix.accepted,
    stateId,
    executionIndex,
  );
  await prefix.sink.dispatch({ kind: "start_decision_execution", stateId, inputDigest: prepared.input_digest });
  const result = evaluatePreparedDecisionState(prefix.pipeline, prepared);
  await prefix.sink.dispatch({ kind: "decision_evaluated", result: toSelectedRecord(result) });
  const index = state.transitions.findIndex((transition) => transition.outcome === result.outcome);
  const target = state.transitions[index];
  if (target === undefined) {
    throw new Error(`no transition for decision outcome ${result.outcome}`);
  }
  await prefix.sink.dispatch({
    kind: "transition_committed",
    step: { from: stateId, outcome: result.outcome, to: target.to, transition_index: index },
    executionIndex,
  });
  return result;
}

// --- the fake transport ------------------------------------------------------

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: CliStdio;
  opts?: CliRunOptions;
}

interface ResumeScript {
  /** Runs before the fake answers the auth call (1-based). */
  onAuth?: () => void | Promise<void>;
  onCreate?: (index: number) => void | Promise<void>;
  onDelete?: (index: number) => void | Promise<void>;
  onRun?: () => void | Promise<void>;
  runCode?: number;
  runTimedOut?: boolean;
  deleteCode?: number;
  /** Output files the fake worker writes into the activation outputs mount. */
  workerOutputs?: Readonly<Record<string, Readonly<Record<string, string>>>>;
  baseEnv?: Readonly<Record<string, string | undefined>>;
  launcherId?: string;
  /** The onSignal seam: undefined = default recorder; null = no seam. */
  onSignal?: ((handler: (signal: "SIGINT" | "SIGTERM") => void) => void) | null;
  configRoot?: string;
}

interface Captured {
  outcome: PipelineV2RunOutcome | null;
  diagnostics: string[];
  authCalls: number;
  handlerCalls: number;
  calls: RecordedCall[];
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
};

function fakeCliTransport(
  harness: Harness,
  runId: string,
  script: ResumeScript,
  calls: RecordedCall[],
): CliRunner {
  let createIndex = 0;
  let deleteIndex = 0;
  return async (args, env, stdio, opts) => {
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
      const id = args[args.length - 1] ?? "";
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
            const dir = join(harness.runRoot(runId), source ?? "");
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
}

async function runResume(
  harness: Harness,
  runId: string,
  script: ResumeScript = {},
): Promise<Captured> {
  let handlerCalls = 0;
  const calls: RecordedCall[] = [];
  const cli = fakeCliTransport(harness, runId, script, calls);
  let authCalls = 0;
  const fetchAuth: AuthFetcher = async () => {
    authCalls += 1;
    await script.onAuth?.();
    return {
      status: 200,
      body: {
        authority: "launcher",
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
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    onSignal:
      script.onSignal === null
        ? undefined
        : (handler) => {
            script.onSignal?.((signal) => {
              handlerCalls += 1;
              handler(signal);
            });
          },
    now: () => new Date(0),
  };
  const options: PipelineV2ResumeOptions = {
    runId,
    configRoot: script.configRoot ?? harness.configRoot,
    launcherId: script.launcherId ?? EXPECTED_LAUNCHER_ID,
  };
  const capturedDiagnostics = captureDiagnostics();
  let outcome: PipelineV2RunOutcome | null = null;
  try {
    outcome = await resumePipelineV2(options, deps);
  } finally {
    capturedDiagnostics.restore();
  }
  return {
    outcome,
    diagnostics: capturedDiagnostics.errors,
    authCalls,
    handlerCalls,
    calls,
  };
}

// --- helpers -----------------------------------------------------------------

function createCount(captured: Captured): number {
  return captured.calls.filter((call) => call.args[0] === "session" && call.args[1] === "create").length;
}

function deleteIds(captured: Captured): string[] {
  return captured.calls
    .filter((call) => call.args[0] === "session" && call.args[1] === "delete")
    .map((call) => call.args[call.args.length - 1] ?? "");
}

function expectRefused(
  captured: Captured,
  reason: NonNullable<PipelineV2RunOutcome["reason"]>,
): { outcome: PipelineV2RunOutcome; state: PipelineV2RunState } {
  const outcome = captured.outcome;
  if (outcome === null) {
    throw new Error("the runner produced no outcome");
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.reason).toBe(reason);
  const state = outcome.state;
  if (state === null) {
    throw new Error("a refused resume must carry the authoritative snapshot");
  }
  return { outcome, state };
}

function modeOf(info: { mode: number }): number {
  return info.mode & 0o777;
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (rel: string): Promise<void> => {
    const abs = rel === "" ? root : join(root, rel);
    let info;
    try {
      info = await lstat(abs);
    } catch {
      lines.push(`${rel}\tmissing`);
      return;
    }
    const kind = info.isSymbolicLink()
      ? "symlink"
      : info.isDirectory()
        ? "dir"
        : info.isFile()
          ? "file"
          : "other";
    let extra = "";
    if (info.isFile()) {
      extra = createHash("sha256").update(await readFile(abs)).digest("hex");
    }
    lines.push(`${rel}\t${kind}\t${modeOf(info)}\t${extra}`);
    if (info.isDirectory() && !info.isSymbolicLink()) {
      for (const entry of (await readdir(abs)).sort()) {
        await walk(rel === "" ? entry : `${rel}/${entry}`);
      }
    }
  };
  await walk("");
  return lines.join("\n");
}

async function lstatOrNull(path: string): Promise<Awaited<ReturnType<typeof lstat>> | null> {
  try {
    return await lstat(path);
  } catch {
    return null;
  }
}

// --- tests -------------------------------------------------------------------

test("1. resume after create_run reaches success through the real transport", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a1";
  await buildPrefix(harness, pipeline, runId);
  const captured = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
  });
  const outcome = captured.outcome;
  if (outcome === null || !outcome.ok || outcome.state === null) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(outcome)}; diagnostics: ${captured.diagnostics.join(" | ")}`);
  }
  expect(outcome.exitCode).toBe(0);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot(runId));
  expect(outcome.state.status).toBe("success");
  expect(outcome.state.cursor).toEqual({ current_state: "done", transition_count: 2 });
  expect(outcome.state.executions.map((execution) => [execution.index, execution.state_id])).toEqual([
    [1, "coder"],
    [2, "check"],
  ]);
  // exactly one session pair, cleaned Tool -> Execution, exactly once
  expect(createCount(captured)).toBe(2);
  expect(deleteIds(captured)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
  expect(captured.authCalls).toBe(1);
  // the outputs are published
  const report = await readFile(join(harness.runRoot(runId), "outputs", "report"), "utf8");
  expect(JSON.parse(report)).toEqual({ ok: true });
  const raw = await readFile(pipelineV2RunStatePath(harness.stateRoot, runId), "utf8");
  expect(parsePipelineV2RunState(raw)).toEqual(outcome.state);
});

test("2. agent -> decision -> agent resume uses the global next execution index", async () => {
  const harness = await setup(PIPELINE_DECISION_FROM_OUTPUT);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a2";
  const prefix = await buildPrefix(harness, pipeline, runId);
  await prefixAgentStep(prefix, "coder", 1, { facts: FACTS_ALPHA });
  await prefixDecisionStep(prefix, "check", 2); // alpha -> probe
  const captured = await runResume(harness, runId, {
    workerOutputs: { probe: { report: "probe body" } },
  });
  const outcome = captured.outcome;
  if (outcome === null || !outcome.ok || outcome.state === null) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(outcome)}; diagnostics: ${captured.diagnostics.join(" | ")}`);
  }
  expect(outcome.state.cursor).toEqual({ current_state: "done", transition_count: 3 });
  const probeExecution = outcome.state.executions[2];
  expect(probeExecution?.type).toBe("agent");
  expect(probeExecution?.index).toBe(3);
  expect(probeExecution?.state_id).toBe("probe");
  // the activation leaf carries the global index, decision gap included
  const leaf = await lstat(join(harness.runRoot(runId), "activations", "3-probe"));
  expect(leaf.isDirectory()).toBe(true);
  expect(createCount(captured)).toBe(2);
  expect(deleteIds(captured)).toEqual(["dhs_fake_2", "dhs_fake_1"]);
});

test("3. deleted original project source and input bindings are never re-read", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a3";
  await buildPrefix(harness, pipeline, runId);
  await rm(join(harness.root, "userdata"), { recursive: true, force: true });
  await rm(harness.projectSource, { recursive: true, force: true });
  const captured = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
  });
  const outcome = captured.outcome;
  if (outcome === null || !outcome.ok) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(outcome)}; diagnostics: ${captured.diagnostics.join(" | ")}`);
  }
  // the original sources stay deleted
  expect(await lstatOrNull(join(harness.root, "userdata"))).toBeNull();
  expect(await lstatOrNull(harness.projectSource)).toBeNull();
  // no diagnostic mentions the original source paths
  expect(captured.diagnostics.join("\n")).not.toContain("userdata");
  expect(captured.diagnostics.join("\n")).not.toContain("project-src");
});

test("4. old and winning accepted outputs are restored and used", async () => {
  const harness = await setup(PIPELINE_DECISION_FROM_OUTPUT);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a4";
  const prefix = await buildPrefix(harness, pipeline, runId);
  await prefixAgentStep(prefix, "coder", 1, { facts: FACTS_BETA });
  await prefixDecisionStep(prefix, "check", 2); // beta -> coder (revisit)
  await prefixAgentStep(prefix, "coder", 3, { facts: FACTS_ALPHA });
  await prefixDecisionStep(prefix, "check", 4); // alpha -> probe
  const prefixState = parsePipelineV2RunState(
    await readFile(pipelineV2RunStatePath(harness.stateRoot, runId), "utf8"),
  );
  const captured = await runResume(harness, runId, {
    workerOutputs: { probe: { report: "probe body" } },
  });
  const outcome = captured.outcome;
  if (outcome === null || !outcome.ok || outcome.state === null) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(outcome)}; diagnostics: ${captured.diagnostics.join(" | ")}`);
  }
  // both records are restored in execution order with their exact digests
  const oldDigest = (outcome.state.executions[0] as PipelineV2AgentExecutionState).outputs?.[0]?.digest;
  const winningDigest = (outcome.state.executions[2] as PipelineV2AgentExecutionState).outputs?.[0]?.digest;
  expect(oldDigest).toEqual((prefixState.executions[0] as PipelineV2AgentExecutionState).outputs?.[0]?.digest);
  expect(winningDigest).toEqual((prefixState.executions[2] as PipelineV2AgentExecutionState).outputs?.[0]?.digest);
  expect(oldDigest).not.toEqual(winningDigest);
  // both activation trees still exist
  expect((await lstat(join(harness.runRoot(runId), "activations", "1-coder"))).isDirectory()).toBe(true);
  expect((await lstat(join(harness.runRoot(runId), "activations", "3-coder"))).isDirectory()).toBe(true);
});

test("5. a terminal cursor finalizes with zero executor callbacks", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a5";
  const prefix = await buildPrefix(harness, pipeline, runId);
  await prefixAgentStep(prefix, "coder", 1, {});
  await prefixDecisionStep(prefix, "check", 2); // alpha -> done
  const captured = await runResume(harness, runId);
  const outcome = captured.outcome;
  if (outcome === null || !outcome.ok || outcome.state === null) {
    throw new Error(`expected a successful resume, got ${JSON.stringify(outcome)}; diagnostics: ${captured.diagnostics.join(" | ")}`);
  }
  expect(outcome.state.terminal?.state_id).toBe("done");
  expect(outcome.state.run_outputs).toHaveLength(1);
  // zero executor callbacks: no session, no pull, no worker run
  expect(createCount(captured)).toBe(0);
  expect(captured.calls.filter((call) => call.args[0] === "pull")).toHaveLength(0);
  expect(captured.calls.filter((call) => call.args[0] === "run")).toHaveLength(0);
});

test("6. the total transition budget counts the durable transitions", async () => {
  const harness = await setup(PIPELINE_CYCLE_BUDGET);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a6";
  const prefix = await buildPrefix(harness, pipeline, runId);
  await prefixAgentStep(prefix, "coder", 1);
  await prefixAgentStep(prefix, "coder2", 2); // cursor back at coder, count 2 = max
  const captured = await runResume(harness, runId);
  const { outcome, state } = expectRefused(captured, "transition_budget_exhausted");
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot(runId));
  expect(state.failure).toEqual({ reason: "transition_budget_exhausted" });
  expect(state.executions).toHaveLength(2);
  expect(state.transitions).toHaveLength(2);
  // zero executor callbacks ran
  expect(createCount(captured)).toBe(0);
});

test("7. a missing state refuses read-only and creates nothing", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  // a run root without a state document
  const runId = "resume-a7";
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { mode: 0o700 });
  await mkdir(harness.runRoot(runId), { mode: 0o700 });
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  const captured = await runResume(harness, runId);
  const outcome = captured.outcome;
  if (outcome === null) {
    throw new Error("the runner produced no outcome");
  }
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot(runId));
  expect(outcome.state).toBeNull();
  // zero auth, zero CLI calls, nothing created
  expect(captured.authCalls).toBe(0);
  expect(captured.calls).toHaveLength(0);
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);

  // a completely absent run root refuses with the generic preflight shape
  const absent = await runResume(harness, "resume-a7-missing");
  expect(absent.outcome?.runId).toBe("");
  expect(absent.outcome?.runRoot).toBeNull();
  expect(absent.outcome?.state).toBeNull();
  expect(absent.authCalls).toBe(0);
  expect(absent.calls).toHaveLength(0);
  // nothing was created anywhere
  expect((await readdir(harness.stateRoot)).sort()).toEqual(["pipeline-runs"]);
  expect((await readdir(join(harness.stateRoot, "pipeline-runs"))).sort()).toEqual([runId]);
});

test("8. waiting, in-flight, publishing and final states refuse without mutations", async () => {
  // waiting
  const waiting = await setup(PIPELINE_AGENT_DECISION);
  const pipelineW = await loadPipelineV2(waiting.bundle);
  const runIdW = "resume-w";
  const prefixW = await buildPrefix(waiting, pipelineW, runIdW);
  await prefixAgentStep(prefixW, "coder", 1, {});
  const coderState = pipelineW.states.find((entry) => entry.id === "coder");
  const to = coderState?.type === "agent" ? coderState.transitions[0]?.to : "";
  if (to === undefined) {
    throw new Error("fixture without a coder transition");
  }
  await prefixW.sink.dispatch({
    kind: "run_waiting",
    stateId: to,
    reason: "stage_iteration_limit_exhausted",
    requestSha256: "a".repeat(64),
    actions: [{ id: "continue_stage", to }],
  });
  const fpWaiting = await fingerprint(waiting.stateRoot);
  const capturedWaiting = await runResume(waiting, runIdW);
  const refusedWaiting = expectRefused(capturedWaiting, "invalid_state");
  expect(refusedWaiting.state.status).toBe("waiting");
  expect(createCount(capturedWaiting)).toBe(0);
  expect(await fingerprint(waiting.stateRoot)).toBe(fpWaiting);

  // in-flight
  const inFlight = await setup(PIPELINE_AGENT_DECISION);
  const pipelineI = await loadPipelineV2(inFlight.bundle);
  const runIdI = "resume-i";
  const prefixI = await buildPrefix(inFlight, pipelineI, runIdI);
  await prefixI.sink.dispatch({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
  const fpInFlight = await fingerprint(inFlight.stateRoot);
  const capturedInFlight = await runResume(inFlight, runIdI);
  expectRefused(capturedInFlight, "invalid_state");
  expect(createCount(capturedInFlight)).toBe(0);
  expect(await fingerprint(inFlight.stateRoot)).toBe(fpInFlight);

  // settled but unbound
  const unbound = await setup(PIPELINE_AGENT_DECISION);
  const pipelineU = await loadPipelineV2(unbound.bundle);
  const runIdU = "resume-u";
  const prefixU = await buildPrefix(unbound, pipelineU, runIdU);
  const activation = await prepareActivationData(prefixU.pipeline, prefixU.runInputs, [], "coder", 1);
  await prefixU.sink.dispatch({ kind: "start_agent_execution", stateId: "coder", profile: "coder" });
  await prefixU.sink.dispatch({ kind: "agent_data_prepared" });
  await prefixU.sink.dispatch({ kind: "agent_execution_session_created", sessionId: "pfx-exec-1" });
  await prefixU.sink.dispatch({ kind: "agent_tool_session_created", sessionId: "pfx-tool-1" });
  await prefixU.sink.dispatch({ kind: "agent_running" });
  for (const port of activation.output_ports) {
    await writeFile(port.path, "{}");
  }
  const records = await acceptActivationOutputs(prefixU.pipeline, activation);
  await prefixU.sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
  });
  await prefixU.sink.dispatch({ kind: "agent_cleanup_completed" });
  const fpUnbound = await fingerprint(unbound.stateRoot);
  const capturedUnbound = await runResume(unbound, runIdU);
  expectRefused(capturedUnbound, "invalid_state");
  expect(createCount(capturedUnbound)).toBe(0);
  expect(await fingerprint(unbound.stateRoot)).toBe(fpUnbound);

  // publishing outputs
  const publishing = await setup(PIPELINE_AGENT_DECISION);
  const pipelineP = await loadPipelineV2(publishing.bundle);
  const runIdP = "resume-p";
  const prefixP = await buildPrefix(publishing, pipelineP, runIdP);
  await prefixAgentStep(prefixP, "coder", 1, {});
  await prefixDecisionStep(prefixP, "check", 2);
  await prefixP.sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  const fpPublishing = await fingerprint(publishing.stateRoot);
  const capturedPublishing = await runResume(publishing, runIdP);
  expectRefused(capturedPublishing, "invalid_state");
  expect(createCount(capturedPublishing)).toBe(0);
  expect(await fingerprint(publishing.stateRoot)).toBe(fpPublishing);

  // final success
  const finished = await setup(PIPELINE_AGENT_DECISION);
  const pipelineF = await loadPipelineV2(finished.bundle);
  const runIdF = "resume-f";
  const prefixF = await buildPrefix(finished, pipelineF, runIdF);
  await prefixAgentStep(prefixF, "coder", 1, {});
  await prefixDecisionStep(prefixF, "check", 2);
  await prefixF.sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" });
  await prefixF.sink.dispatch({ kind: "run_outputs_published", outputs: [] });
  await prefixF.sink.dispatch({ kind: "run_succeeded" });
  const fpFinished = await fingerprint(finished.stateRoot);
  const capturedFinished = await runResume(finished, runIdF);
  expectRefused(capturedFinished, "invalid_state");
  expect(createCount(capturedFinished)).toBe(0);
  expect(await fingerprint(finished.stateRoot)).toBe(fpFinished);
});

test("9. damaged inputs, outputs, layout and pipeline identity refuse without Sessions or writes", async () => {
  // damaged input snapshot
  const damagedInput = await setup(PIPELINE_AGENT_DECISION);
  const pipelineD = await loadPipelineV2(damagedInput.bundle);
  const runIdD = "resume-d1";
  await buildPrefix(damagedInput, pipelineD, runIdD);
  await writeFile(join(damagedInput.runRoot(runIdD), "data", "inputs", "facts_seed"), FACTS_BETA);
  const fpD = await fingerprint(damagedInput.stateRoot);
  const capturedD = await runResume(damagedInput, runIdD);
  expectRefused(capturedD, "run_input_modified");
  expect(createCount(capturedD)).toBe(0);
  expect(await fingerprint(damagedInput.stateRoot)).toBe(fpD);

  // damaged accepted output
  const damagedOutput = await setup(PIPELINE_DECISION_FROM_OUTPUT);
  const pipelineO = await loadPipelineV2(damagedOutput.bundle);
  const runIdO = "resume-d2";
  const prefixO = await buildPrefix(damagedOutput, pipelineO, runIdO);
  await prefixAgentStep(prefixO, "coder", 1, { facts: FACTS_ALPHA });
  await writeFile(
    join(damagedOutput.runRoot(runIdO), "activations", "1-coder", "data", "outputs", "facts"),
    FACTS_BETA,
  );
  const fpO = await fingerprint(damagedOutput.stateRoot);
  const capturedO = await runResume(damagedOutput, runIdO);
  expectRefused(capturedO, "accepted_output_modified");
  expect(createCount(capturedO)).toBe(0);
  expect(await fingerprint(damagedOutput.stateRoot)).toBe(fpO);

  // damaged run layout (the run root was verified; the layout check of the
  // restore refuses before any durable write, carrying the snapshot)
  const damagedLayout = await setup(PIPELINE_AGENT_DECISION);
  const pipelineL = await loadPipelineV2(damagedLayout.bundle);
  const runIdL = "resume-d3";
  await buildPrefix(damagedLayout, pipelineL, runIdL);
  await rm(join(damagedLayout.runRoot(runIdL), "data", "inputs"), { recursive: true, force: true });
  const fpL = await fingerprint(damagedLayout.stateRoot);
  const capturedL = await runResume(damagedLayout, runIdL);
  const refusedLayout = expectRefused(capturedL, "run_layout_invalid");
  expect(refusedLayout.outcome.runId).toBe(runIdL);
  expect(refusedLayout.outcome.runRoot).toBe(damagedLayout.runRoot(runIdL));
  expect(createCount(capturedL)).toBe(0);
  expect(await fingerprint(damagedLayout.stateRoot)).toBe(fpL);

  // pipeline identity mismatch: the bundle content changed under the same
  // durable bundle root (the reachable production failure mode)
  const mismatched = await setup(PIPELINE_AGENT_DECISION);
  const pipelineM = await loadPipelineV2(mismatched.bundle);
  const runIdM = "resume-d4";
  await buildPrefix(mismatched, pipelineM, runIdM);
  await writeFile(join(mismatched.bundle, "pipeline.yaml"), PIPELINE_DECISION_FROM_OUTPUT);
  const fpM = await fingerprint(mismatched.stateRoot);
  const capturedM = await runResume(mismatched, runIdM);
  expectRefused(capturedM, "pipeline_mismatch");
  expect(createCount(capturedM)).toBe(0);
  expect(await fingerprint(mismatched.stateRoot)).toBe(fpM);
});

test("10. the pre-existing run root is never re-created and never chmodded", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a10";
  await buildPrefix(harness, pipeline, runId);
  const beforeInfo = await lstat(harness.runRoot(runId));

  // an unsafe pre-existing run root mode is rejected unchanged
  chmodSync(harness.runRoot(runId), 0o755);
  const unsafe = await runResume(harness, runId);
  expect(unsafe.outcome?.ok).toBe(false);
  expect(unsafe.outcome?.exitCode).toBe(1);
  expect(unsafe.outcome?.runId).toBe("");
  expect(unsafe.outcome?.runRoot).toBeNull();
  expect(unsafe.authCalls).toBe(0);
  expect(unsafe.calls).toHaveLength(0);
  expect(modeOf(await lstat(harness.runRoot(runId)))).toBe(0o755);

  // restore the mode and resume successfully on the SAME run-root object
  chmodSync(harness.runRoot(runId), 0o700);
  const safe = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
  });
  expect(safe.outcome?.ok).toBe(true);
  const afterInfo = await lstat(harness.runRoot(runId));
  expect(afterInfo.ino).toBe(beforeInfo.ino);
  expect(afterInfo.dev).toBe(beforeInfo.dev);
});

test("11. symlinked, file and split state-root projections are rejected", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a11";
  await buildPrefix(harness, pipeline, runId);
  const baseDeps = {
    cli: (async () => ({ code: 1 })) as CliRunner,
    fetchAuth: (async () => ({ status: 200, body: {} })) as AuthFetcher,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: DEFAULT_BASE_ENV,
  };

  // a symlinked state root is rejected
  const link = join(harness.root, "state-link");
  await symlink(harness.stateRoot, link);
  const outcomeSymlink = await resumePipelineV2(
    { runId, configRoot: harness.configRoot },
    { ...baseDeps, stateRootProjection: { localRoot: link, daemonRoot: link } },
  );
  expect(outcomeSymlink.ok).toBe(false);
  expect(outcomeSymlink.exitCode).toBe(1);
  expect(outcomeSymlink.runId).toBe("");
  expect(outcomeSymlink.runRoot).toBeNull();

  // a file as the state root is rejected
  const fileRoot = join(harness.root, "not-a-dir");
  await writeFile(fileRoot, "x");
  const outcomeFile = await resumePipelineV2(
    { runId, configRoot: harness.configRoot },
    { ...baseDeps, stateRootProjection: { localRoot: fileRoot, daemonRoot: fileRoot } },
  );
  expect(outcomeFile.ok).toBe(false);
  expect(outcomeFile.runRoot).toBeNull();

  // a split projection (two different real directories) is rejected
  const otherStateRoot = join(harness.root, "other-state");
  await mkdir(otherStateRoot, { recursive: true, mode: 0o700 });
  const outcomeSplit = await resumePipelineV2(
    { runId, configRoot: harness.configRoot },
    {
      ...baseDeps,
      stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: otherStateRoot },
    },
  );
  expect(outcomeSplit.ok).toBe(false);
  expect(outcomeSplit.runId).toBe("");
  expect(outcomeSplit.runRoot).toBeNull();
});

test("12. onSignal is read exactly once; a throwing registration yields the ordinary outcome", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a12";

  // exactly one read of the seam
  await buildPrefix(harness, pipeline, runId);
  let seamReads = 0;
  const counted = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
    onSignal: (handler) => {
      seamReads += 1;
      void handler;
    },
  });
  expect(counted.outcome?.ok).toBe(true);
  expect(seamReads).toBe(1);

  // a throwing registration is the ordinary preflight outcome
  const throwing = await runResume(harness, runId, {
    onSignal: () => {
      throw new Error("REGISTER-EXPLODED");
    },
  });
  expect(throwing.outcome?.ok).toBe(false);
  expect(throwing.outcome?.exitCode).toBe(1);
  expect(throwing.outcome?.runId).toBe("");
  expect(throwing.outcome?.runRoot).toBeNull();
  expect(throwing.outcome?.state).toBeNull();
  expect(throwing.authCalls).toBe(0);
  expect(throwing.calls).toHaveLength(0);

  // a throwing getter on the seam property is the same ordinary outcome
  const hostileDeps: PipelineV2RunnerDeps = Object.defineProperties(
    {
      cli: (async () => ({ code: 1 })) as CliRunner,
      fetchAuth: (async () => ({ status: 200, body: {} })) as AuthFetcher,
      helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
      baseEnv: DEFAULT_BASE_ENV,
      stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    },
    {
      onSignal: {
        get() {
          throw new Error("SEAM-GETTER-EXPLODED");
        },
        enumerable: true,
      },
    },
  ) as unknown as PipelineV2RunnerDeps;
  const outcomeGetter = await resumePipelineV2(
    { runId, configRoot: harness.configRoot },
    hostileDeps,
  );
  expect(outcomeGetter.ok).toBe(false);
  expect(outcomeGetter.exitCode).toBe(1);
  expect(outcomeGetter.runId).toBe("");
  expect(outcomeGetter.runRoot).toBeNull();
});

test("13. signals before and during the resumed worker path keep the existing priorities", async () => {
  // a signal recorded during the seam registration happens before the
  // run-root verification: the generic preflight shape, and the durable
  // state stays untouched (no finalization on a pre-restore signal)
  const before = await setup(PIPELINE_AGENT_DECISION);
  const pipelineB = await loadPipelineV2(before.bundle);
  const runIdB = "resume-s1";
  await buildPrefix(before, pipelineB, runIdB);
  const stateBefore = await readFile(pipelineV2RunStatePath(before.stateRoot, runIdB), "utf8");
  const capturedB = await runResume(before, runIdB, {
    onSignal: (handler) => {
      handler("SIGINT");
    },
  });
  expect(capturedB.outcome?.ok).toBe(false);
  expect(capturedB.outcome?.exitCode).toBe(130);
  expect(capturedB.outcome?.reason).toBe("signal_sigint");
  expect(capturedB.outcome?.runId).toBe("");
  expect(capturedB.outcome?.runRoot).toBeNull();
  expect(capturedB.outcome?.state).toBeNull();
  expect(capturedB.handlerCalls).toBe(1);
  expect(createCount(capturedB)).toBe(0);
  expect(await readFile(pipelineV2RunStatePath(before.stateRoot, runIdB), "utf8")).toBe(stateBefore);

  // a signal delivered during auth wins over the auth failure
  const during = await setup(PIPELINE_AGENT_DECISION);
  const pipelineD = await loadPipelineV2(during.bundle);
  const runIdD = "resume-s2";
  await buildPrefix(during, pipelineD, runIdD);
  let stored: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
  const capturedD = await runResume(during, runIdD, {
    onSignal: (handler) => {
      stored = handler;
    },
    onAuth: () => {
      stored?.("SIGTERM");
    },
  });
  expect(capturedD.outcome?.ok).toBe(false);
  expect(capturedD.outcome?.exitCode).toBe(143);
  expect(capturedD.outcome?.reason).toBe("signal_sigterm");
  expect(capturedD.outcome?.runId).toBe(runIdD);
  expect(capturedD.outcome?.runRoot).toBe(during.runRoot(runIdD));
  expect(capturedD.outcome?.state).not.toBeNull();
  expect(createCount(capturedD)).toBe(0);
});

test("14. durability unknown adopts the exact candidate and stops every further operation", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a14";
  await buildPrefix(harness, pipeline, runId);
  let hardened = false;
  const captured = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
    onAuth: () => {
      if (!hardened) {
        hardened = true;
        // The post-rename directory fsync of the first resumed dispatch
        // fails with EACCES: the rename already landed, the candidate is
        // adopted and the sink is poisoned.
        chmodSync(harness.runRoot(runId), 0o300);
      }
    },
  });
  try {
    expect(captured.outcome?.ok).toBe(false);
    expect(captured.outcome?.exitCode).toBe(1);
    expect(captured.outcome?.reason).toBe("state_persist_failed");
    expect(captured.outcome?.runId).toBe(runId);
    expect(captured.outcome?.runRoot).toBe(harness.runRoot(runId));
    // the adopted candidate is the visible state with the started execution
    expect(captured.outcome?.state?.run_id).toBe(runId);
    expect(captured.outcome?.state?.executions[0]?.phase).toBe("started");
    const raw = await readFile(pipelineV2RunStatePath(harness.stateRoot, runId), "utf8");
    expect(JSON.parse(raw).run_id).toBe(runId);
    // no Session was created for the poisoned run
    expect(createCount(captured)).toBe(0);
  } finally {
    chmodSync(harness.runRoot(runId), 0o700);
  }
});

test("15. the profile is loaded once; a missing or broken profile refuses before any Session", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a15";
  await buildPrefix(harness, pipeline, runId);

  // count loadProfile invocations through the env-source reads: the
  // profile resolves its required env binding exactly once per load
  let sourceReads = 0;
  const countingEnv = new Proxy(
    { ...DEFAULT_BASE_ENV } as Record<string, string | undefined>,
    {
      get(target, property, receiver) {
        if (property === "CODER_SOURCE_VAR_1") {
          sourceReads += 1;
        }
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const counted = await runResume(harness, runId, {
    workerOutputs: { coder: { report: '{"ok":true}' } },
    baseEnv: countingEnv,
  });
  expect(counted.outcome?.ok).toBe(true);
  expect(sourceReads).toBe(1);

  // a missing profile refuses before any Session
  const missing = await setup(PIPELINE_AGENT_DECISION);
  const pipelineM = await loadPipelineV2(missing.bundle);
  const runIdM = "resume-a15b";
  await buildPrefix(missing, pipelineM, runIdM);
  await rm(join(missing.configRoot, "profiles", "coder.yaml"));
  const fpMissing = await fingerprint(missing.stateRoot);
  const capturedMissing = await runResume(missing, runIdM);
  expect(capturedMissing.outcome?.ok).toBe(false);
  expect(capturedMissing.outcome?.exitCode).toBe(1);
  expect(capturedMissing.outcome?.runId).toBe(runIdM);
  expect(capturedMissing.outcome?.runRoot).toBe(missing.runRoot(runIdM));
  expect(createCount(capturedMissing)).toBe(0);
  expect(await fingerprint(missing.stateRoot)).toBe(fpMissing);

  // a broken profile refuses before any Session
  const broken = await setup(PIPELINE_AGENT_DECISION);
  const pipelineB = await loadPipelineV2(broken.bundle);
  const runIdB = "resume-a15c";
  await buildPrefix(broken, pipelineB, runIdB);
  await writeFile(join(broken.configRoot, "profiles", "coder.yaml"), "not: [valid");
  const fpBroken = await fingerprint(broken.stateRoot);
  const capturedBroken = await runResume(broken, runIdB);
  expect(capturedBroken.outcome?.ok).toBe(false);
  expect(createCount(capturedBroken)).toBe(0);
  expect(await fingerprint(broken.stateRoot)).toBe(fpBroken);
});

test("16. a launcher authority mismatch refuses before any Session", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a16";
  await buildPrefix(harness, pipeline, runId);
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  const captured = await runResume(harness, runId, {
    launcherId: "dhl_other",
  });
  expect(captured.outcome?.ok).toBe(false);
  expect(captured.outcome?.exitCode).toBe(1);
  expect(captured.outcome?.runId).toBe(runId);
  expect(captured.outcome?.runRoot).toBe(harness.runRoot(runId));
  expect(captured.outcome?.state).not.toBeNull();
  expect(captured.authCalls).toBe(1);
  expect(createCount(captured)).toBe(0);
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);
});

test("17. hostile option shapes are ordinary preflight outcomes with zero side effects", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const runId = "resume-a17";
  await mkdir(join(harness.stateRoot, "pipeline-runs"), { mode: 0o700 });
  await mkdir(harness.runRoot(runId), { mode: 0o700 });
  let trapHits = 0;
  const hostileOptions = new Proxy(
    { runId, configRoot: harness.configRoot },
    {
      get(target, property, receiver) {
        trapHits += 1;
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const deps: PipelineV2RunnerDeps = {
    cli: async () => ({ code: 1 }),
    fetchAuth: async () => ({ status: 200, body: {} }),
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: DEFAULT_BASE_ENV,
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
  };
  const outcome = await resumePipelineV2(
    hostileOptions as unknown as PipelineV2ResumeOptions,
    deps,
  );
  // a Proxy over valid options passes the shape validation and proceeds
  // read-only to the missing-state refusal: no corruption, no bypass
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(harness.runRoot(runId));
  expect(outcome.state).toBeNull();
  expect(trapHits).toBeGreaterThanOrEqual(2);
  // a non-safe run id is rejected before any path is built
  const badId = await resumePipelineV2(
    { runId: "../escape", configRoot: harness.configRoot },
    deps,
  );
  expect(badId.ok).toBe(false);
  expect(badId.runId).toBe("");
  expect(badId.runRoot).toBeNull();
});

test("18. the whole state root fingerprint is identical across every pre-resume refusal", async () => {
  const harness = await setup(PIPELINE_AGENT_DECISION);
  const pipeline = await loadPipelineV2(harness.bundle);
  const runId = "resume-a18";
  await buildPrefix(harness, pipeline, runId);
  const fingerprintBefore = await fingerprint(harness.stateRoot);

  const mismatchedLauncher = await runResume(harness, runId, {
    launcherId: "dhl_other",
  });
  expect(mismatchedLauncher.outcome?.ok).toBe(false);
  const missingConfig = await runResume(harness, runId, {
    configRoot: join(harness.root, "no-such-config"),
  });
  expect(missingConfig.outcome?.ok).toBe(false);
  const absentRun = await runResume(harness, "resume-a18-absent");
  expect(absentRun.outcome?.ok).toBe(false);

  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);
});
