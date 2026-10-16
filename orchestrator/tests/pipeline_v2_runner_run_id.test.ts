import { execSync } from "node:child_process";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { afterAll, expect, test } from "bun:test";
import {
  PipelineV2RunRootCollisionError,
  resumePipelineV2PlanningRunPlan,
  runPipelineV2,
  type PipelineV2RunOptions,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "../src/pipeline_v2_runner.ts";
import { inspectPipelineV2Run, type PipelineV2InspectOutcome } from "../src/pipeline_v2_inspect.ts";
import { parsePipelineV2RunState } from "../src/pipeline_v2_state.ts";
import type { AuthFetcher, CliRunner } from "../src/docker_helper.ts";
import type { PipelineV2WorkerOperationRunner } from "../src/pipeline_v2_docker_runtime.ts";

/**
 * Caller-supplied run id for the fresh pipeline v2 run. The explicit id is
 * captured once before any await, becomes the authoritative identity of the
 * run root, durable state, execution documents and outcome, and bypasses the
 * built-in generator; the run root is claimed exclusively by one `mkdir`, an
 * existing object of any shape is a typed `run_already_exists` collision that
 * never opens the existing run's state, and preflight failures before the
 * claim reserve nothing. Without the flag the generated run id is unchanged.
 */

const EXPECTED_LAUNCHER_ID = "dhl_runid";

const PIPELINE_HAPPY = `
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

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: coder
      role: planning
      plan_output: report
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: check
      role: control
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
      - outcome: alpha
        to: done
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

const PLAN_SCHEMA = { type: "object" };
const PLAN_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-1", depends_on: [] }] }],
  new_tasks: [{ id: "task-1", body: "Do the work" }],
};

/** Policy-less planning pipeline: one planning state, one stage, success terminal. */
const PIPELINE_STAGE = `
schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true

outputs:
  - id: result
    required: true
    source:
      state_output:
        state: dev_entry
        output: result

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: dev_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: result
        type: json
        schema: schemas/loose.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

const BROKEN_PIPELINE = "schema_version: 2\nentry_state: [unbalanced\n";

const PROMPT_BODY = "implement the task\n";

interface Harness {
  sessionCounter: { n: number };
  root: string;
  stateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource: string;
  otherProjectSource: string;
  credentialFile: string;
  sourceFile: string;
  otherSourceFile: string;
  factsFile: string;
  otherFactsFile: string;
}

const ROOTS: string[] = [];

async function writeBundle(root: string, yaml: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts", "coder.md"), PROMPT_BODY);
  await writeFile(join(bundle, "prompts", "architect.md"), PROMPT_BODY);
  await writeFile(join(bundle, "schemas", "loose.schema.json"), JSON.stringify({}));
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify(PLAN_SCHEMA));
  await writeFile(join(bundle, "decisions", "model.yaml"), [
    "schema_version: 1",
    "facts:",
    "  - id: f1",
    "  - id: f2",
    "decisions:",
    "  - id: alpha",
    "  - id: beta",
    "relations:",
    "  - id: r1",
    "    assert:",
    "      not:",
    "        all:",
    "          - {fact: f1, equals: true}",
    "          - {fact: f2, equals: true}",
    "constraints: []",
    "rules:",
    "  - id: rule-a",
    "    when: {fact: f1, equals: true}",
    "    decision: alpha",
    "  - id: rule-b",
    "    when: {fact: f2, equals: true}",
    "    decision: beta",
    "",
  ].join("\n"));
  return bundle;
}

async function setupHarness(yaml: string, profileNames: readonly string[]): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-run-id-"));
  const bundle = await writeBundle(root, yaml);
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  for (const name of profileNames) {
    await writeFile(
      join(configRoot, "profiles", `${name}.yaml`),
      [
        "schema_version: 1",
        "image: ghcr.io/example/worker:1",
        `opencode_config: opencode/${name}.json`,
        "env:",
        "  MODEL_API_KEY:",
        `    from_env: ${name.toUpperCase()}_SOURCE_VAR_1`,
        "    required: true",
        "",
      ].join("\n"),
    );
    await writeFile(join(configRoot, "opencode", `${name}.json`), JSON.stringify({ model: "glm53-flash" }));
  }
  const projectSource = join(root, "project-src");
  await mkdir(projectSource, { recursive: true });
  await writeFile(join(projectSource, "seed.md"), "PROJECT-SEED\n");
  const otherProjectSource = join(root, "other-project-src");
  await mkdir(otherProjectSource, { recursive: true });
  await writeFile(join(otherProjectSource, "seed.md"), "OTHER-SEED\n");
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  const sourceFile = join(userdata, "source.txt");
  await writeFile(sourceFile, "SOURCE-BODY\n");
  const otherSourceFile = join(userdata, "other-source.txt");
  await writeFile(otherSourceFile, "OTHER-SOURCE-BODY\n");
  const factsFile = join(userdata, "facts.json");
  await writeFile(factsFile, JSON.stringify({ f1: true, f2: false }));
  const otherFactsFile = join(userdata, "other-facts.json");
  await writeFile(otherFactsFile, JSON.stringify({ f1: true, f2: false }));
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const credDir = join(root, "cred", "docker-helper");
  await mkdir(credDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(credDir, "credential.token");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  const harness: Harness = {
    sessionCounter: { n: 0 },
    root,
    stateRoot,
    bundle,
    configRoot,
    projectSource,
    otherProjectSource,
    credentialFile,
    sourceFile,
    otherSourceFile,
    factsFile,
    otherFactsFile,
  };
  ROOTS.push(root);
  return harness;
}

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
}

interface CliScript {
  onCreate?: (index: number) => void | Promise<void>;
  onDelete?: () => void | Promise<void>;
  onRun?: () => void | Promise<void>;
  /** Never resolves: the abandoned-run scenarios (the caller lost the result). */
  hangOnCreate?: boolean;
  hangOnRun?: boolean;
  runCode?: number;
  workerOutputs?: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

function makeCli(
  runRootOf: () => string,
  script: CliScript,
  sessionCounter: { n: number },
): { cli: CliRunner; operationRunner: PipelineV2WorkerOperationRunner; calls: RecordedCall[] } {
  const calls: RecordedCall[] = [];
  // The fake worker resolves output mount sources against the daemon run
  // root, which the fake learns from the first session-create workspace
  // operand — independent of how the run id was produced.
  let createdRunRoot = "";
  const cli: CliRunner = async (args, env) => {
    calls.push({ args: [...args], env: { ...env } });
    if (args[0] === "session" && args[1] === "create") {
      // Only the first create (the Execution Session) carries the run
      // root as its workspace; the Tool Session workspace is the project.
      if (createdRunRoot === "") {
        createdRunRoot = args[args.length - 1] ?? "";
      }
      sessionCounter.n += 1;
      const index = sessionCounter.n;
      if (script.hangOnCreate === true) {
        return new Promise(() => undefined);
      }
      await script.onCreate?.(index);
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_fake_${index}`, launcher_id: EXPECTED_LAUNCHER_ID },
          token: `dhc_fake_${index}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      await script.onDelete?.();
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }),
      };
    }
    if (args[0] === "pull") {
      return { code: 0 };
    }
    return { code: 1 };
  };
  const operationRunner: PipelineV2WorkerOperationRunner = async (request) => {
    if (script.hangOnRun === true) {
      return new Promise(() => undefined);
    }
    await script.onRun?.();
    const body = JSON.parse(request.body) as {
      mounts?: Array<{ source?: string; target?: string }>;
    };
    const outputsMount = (body.mounts ?? []).find((mount) => mount.target === "/pipeline/outputs");
    if (outputsMount !== undefined && typeof outputsMount.source === "string") {
      const segments = outputsMount.source.split("/");
      const dash = segments[1] ?? "";
      const stateId = dash.slice(dash.indexOf("-") + 1);
      const outputs = script.workerOutputs?.[stateId];
      if (outputs !== undefined) {
        const dir = join(runRootOf() || createdRunRoot, outputsMount.source);
        await mkdir(dir, { recursive: true });
        for (const [name, content] of Object.entries(outputs)) {
          await writeFile(join(dir, name), content);
        }
      }
    }
    if ((script.runCode ?? 0) !== 0) {
      return { outcome: "worker_failed", capture: EMPTY_CAPTURE };
    }
    return { outcome: "completed", capture: EMPTY_CAPTURE };
  };
  return { cli, operationRunner, calls };
}

const EMPTY_CAPTURE = {
  status: "unavailable" as const,
  byteCount: 0,
  tailBytes: new Uint8Array(0),
  truncatedDaemon: false,
  truncatedLocal: false,
};

interface RunCall {
  options: PipelineV2RunOptions;
  deps: PipelineV2RunnerDeps;
  calls: RecordedCall[];
  /** Delivers a signal through the runner's onSignal seam. */
  deliverSignal: (signal: "SIGINT" | "SIGTERM") => void;
  /** The fake worker resolves output paths through this mutable run root. */
  runRootRef: { current: string };
}

interface RunCallOptions {
  runId?: string;
  bindings?: PipelineV2RunOptions["inputBindings"];
  projectSourcePath?: string;
  bundle?: string;
  configRoot?: string;
  onAuth?: () => void | Promise<void>;
  /** Auth barrier shared between racing calls: resolves when every party arrived. */
  authBarrier?: { arrive: () => void; wait: Promise<void> };
  script?: CliScript;
  randomId?: () => string;
  onSignalHandler?: (handler: (signal: "SIGINT" | "SIGTERM") => void) => void;
}

function makeRunCall(
  harness: Harness,
  callOptions: RunCallOptions,
): RunCall {
  const script = callOptions.script ?? {};
  const runRootRef = {
    current:
      callOptions.runId !== undefined
        ? join(harness.stateRoot, "pipeline-runs", callOptions.runId)
        : "",
  };
  const runRootOf = (): string => runRootRef.current;
  const { cli, operationRunner, calls } = makeCli(runRootOf, script, harness.sessionCounter);
  const fetchAuth: AuthFetcher = async () => {
    await callOptions.authBarrier?.arrive();
    await callOptions.authBarrier?.wait;
    await callOptions.onAuth?.();
    return {
      status: 200,
      body: { authority: "launcher", principal: "tester", launcher_id: EXPECTED_LAUNCHER_ID },
    };
  };
  let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
  const deps: PipelineV2RunnerDeps = {
    cli,
    fetchAuth,
    operationRunner,
    helperConfig: {
      socketPath: "/run/docker-helper/docker-helper.sock",
      credentialFile: harness.credentialFile,
    },
    baseEnv: {
      CODER_SOURCE_VAR_1: "coder-secret",
      ARCHITECT_SOURCE_VAR_1: "architect-secret",
    },
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    onSignal: (handler) => {
      signalHandler = handler;
      callOptions.onSignalHandler?.(handler);
    },
    randomId:
      callOptions.randomId === undefined
        ? undefined
        : () => {
            const generated = callOptions.randomId?.() ?? "";
            runRootRef.current = join(harness.stateRoot, "pipeline-runs", generated);
            return generated;
          },
  };
  const options: PipelineV2RunOptions = {
    pipelineRoot: callOptions.bundle ?? harness.bundle,
    configRoot: callOptions.configRoot ?? harness.configRoot,
    projectSourcePath: callOptions.projectSourcePath ?? harness.projectSource,
    inputBindings: callOptions.bindings ?? [
      { id: "facts_seed", path: harness.factsFile },
      { id: "source", path: harness.sourceFile },
    ],
    runId: callOptions.runId,
    launcherId: EXPECTED_LAUNCHER_ID,
  };
  return {
    options,
    deps,
    calls,
    deliverSignal: (signal) => signalHandler?.(signal),
    runRootRef,
  };
}

function expectCollision(outcome: PipelineV2RunOutcome, harness: Harness, runId: string): void {
  expect(outcome.ok).toBe(false);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "reason", "runId", "runRoot", "state"]);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(join(harness.stateRoot, "pipeline-runs", runId));
  expect(outcome.state).toBeNull();
  expect(outcome.reason).toBe("run_already_exists");
  expect(Object.isFrozen(outcome)).toBe(true);
}

/** Deterministic full-tree fingerprint: relative path, kind, mode, content. */
async function fingerprint(root: string): Promise<string> {
  const hash = createHash("sha256");
  const walk = async (absolute: string, relative: string): Promise<void> => {
    const info = await lstat(absolute);
    hash.update(`${relative}\0${info.isDirectory() ? "d" : info.isSymbolicLink() ? "l" : "f"}\0${info.mode.toString(8)}\0`);
    if (info.isSymbolicLink()) {
      hash.update(`${await readlink(absolute)}\0`);
      return;
    }
    if (!info.isDirectory() && !info.isFile()) {
      hash.update("special\0");
      return;
    }
    if (info.isDirectory()) {
      const entries = [...(await readdir(absolute))].sort();
      for (const entry of entries) {
        await walk(join(absolute, entry), `${relative}/${entry}`);
      }
      return;
    }
    hash.update(await readFile(absolute));
  };
  await walk(root, "");
  return hash.digest("hex");
}

async function readDurableState(runRoot: string): Promise<ReturnType<typeof parsePipelineV2RunState>> {
  return parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
}

/**
 * The default wall-clock deadline of the durable-state poller: strictly
 * below the default 5000ms per-test timeout, so an unmet durable boundary
 * is reported by the poller's own content-free diagnostic and never by
 * the runner's generic timeout.
 */
const WAIT_FOR_STATE_DEFAULT_DEADLINE_MS = 4200;

/**
 * Polls `read` until `predicate` holds. Bounded by a monotonic
 * wall-clock deadline — never by an iteration count — and every
 * unsuccessful poll yields to the scheduler before the next attempt, so
 * the loop cannot starve the child process it is waiting for. The fast
 * path returns on the first satisfying poll without any artificial
 * delay; the deadline diagnostic carries only the last observed scalar
 * shape, never file contents.
 */
async function waitForState<T>(
  read: () => Promise<T>,
  predicate: (value: T) => boolean,
  options?: { readonly deadlineMs?: number },
): Promise<T> {
  const deadlineMs = options?.deadlineMs ?? WAIT_FOR_STATE_DEFAULT_DEADLINE_MS;
  const deadline = performance.now() + deadlineMs;
  let lastObservation = "the observed document does not exist yet";
  for (;;) {
    try {
      const value = await read();
      if (predicate(value)) {
        return value;
      }
      lastObservation = describeObserved(value);
    } catch {
      lastObservation = "the observed document does not exist yet";
    }
    if (performance.now() >= deadline) {
      throw new Error(
        `the awaited condition was not met within ${String(deadlineMs)}ms (last observation: ${lastObservation})`,
      );
    }
    await Bun.sleep(2);
  }
}

/** Content-free description of the last observed value for the deadline diagnostic. */
function describeObserved(value: unknown): string {
  if (typeof value !== "object" || value === null) {
    return "an unexpected value shape";
  }
  const record = value as { readonly revision?: unknown; readonly status?: unknown; readonly phase?: unknown };
  return `revision ${typeof record.revision === "number" ? String(record.revision) : "unknown"}, status ${
    typeof record.status === "string" ? record.status : "unknown"
  }, phase ${typeof record.phase === "string" ? record.phase : "unknown"}`;
}

/** Polls the durable run state until the predicate holds. */
async function waitForDurableState(
  runRoot: string,
  predicate: (state: ReturnType<typeof parsePipelineV2RunState>) => boolean,
  options?: { readonly deadlineMs?: number },
): Promise<ReturnType<typeof parsePipelineV2RunState>> {
  return await waitForState(() => readDurableState(runRoot), predicate, options);
}

function expectReport(outcome: PipelineV2InspectOutcome): Extract<PipelineV2InspectOutcome, { ok: true }> {
  if (!outcome.ok) {
    throw new Error(`inspect failed: ${outcome.reason}`);
  }
  return outcome;
}

function expectInspectFailure(
  outcome: PipelineV2InspectOutcome,
): Extract<PipelineV2InspectOutcome, { ok: false }> {
  if (outcome.ok) {
    throw new Error("inspect unexpectedly succeeded");
  }
  return outcome;
}

function agentOf(
  state: ReturnType<typeof parsePipelineV2RunState>,
  index: number,
): { phase: string; state_id: string; execution_role: string } {
  const execution = state.executions[index - 1];
  if (execution === undefined || execution.type !== "agent") {
    throw new Error(`execution ${index} is not an agent execution`);
  }
  return { phase: execution.phase, state_id: execution.state_id, execution_role: execution.execution_role };
}

const HAPPY_WORKER_OUTPUTS = { coder: { report: JSON.stringify({ ok: true }) } };
const STAGE_WORKER_OUTPUTS = {
  architect: { plan: JSON.stringify(PLAN_PROPOSAL) },
  dev_entry: { result: JSON.stringify({ ok: true }) },
};

test("the explicit run id propagates exactly and bypasses the generator", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "caller.run-01_x";
  const runIdCalls: string[] = [];
  const call = makeRunCall(harness, {
    runId,
    randomId: () => {
      runIdCalls.push("called");
      return "never-generated";
    },
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const outcome = await runPipelineV2(call.options, call.deps);
  expect(outcome.ok).toBe(true);
  expect(outcome.exitCode).toBe(0);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(join(harness.stateRoot, "pipeline-runs", runId));
  expect(runIdCalls).toEqual([]);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const state = await readDurableState(runRoot);
  expect(state.run_id).toBe(runId);
  expect(state.status).toBe("success");
  expect(state.executions).toHaveLength(2);
  expect(state.transitions).toHaveLength(2);
  // the activation leaf and the execution document carry the exact id
  const activations = await readdir(join(runRoot, "activations"));
  expect(activations).toContain("1-coder");
  // the published run output lives under the exact run root
  const report = await readFile(join(runRoot, "outputs", "report"), "utf8");
  expect(JSON.parse(report)).toEqual({ ok: true });
});

test("without the flag the generated run id is unchanged (compatibility)", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const generated: string[] = [];
  const call = makeRunCall(harness, {
    randomId: () => {
      const id = `gen-run-${generated.length + 1}`;
      generated.push(id);
      return id;
    },
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const options = { ...call.options } as Record<string, unknown>;
  delete options.runId;
  const outcome = await runPipelineV2(options as unknown as PipelineV2RunOptions, call.deps);
  expect(outcome.ok).toBe(true);
  expect(outcome.runId).toBe("gen-run-1");
  expect(generated).toEqual(["gen-run-1"]);
});

test("without the flag and without a randomId the runner generates a UUID", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const call = makeRunCall(harness, { script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const outcome = await runPipelineV2(call.options, call.deps);
  expect(outcome.ok).toBe(true);
  expect(outcome.runId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
});

test("collision: an existing valid run is never reused, disclosed or repaired", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "occupied-run";
  const first = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const firstOutcome = await runPipelineV2(first.options, first.deps);
  expect(firstOutcome.ok).toBe(true);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const before = await fingerprint(runRoot);
  const second = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const outcome = await runPipelineV2(second.options, second.deps);
  expectCollision(outcome, harness, runId);
  expect(second.calls).toEqual([]);
  expect(await fingerprint(runRoot)).toBe(before);
  // the existing run's own boundary stays inspectable, unchanged
  const report = expectReport(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(report.status).toBe("succeeded");
  expect(report.boundary.kind).toBe("succeeded");
});

test("collision battery: empty directory, file, symlinks, FIFO, unsafe mode", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runsRoot = join(harness.stateRoot, "pipeline-runs");
  await mkdir(runsRoot, { recursive: true, mode: 0o700 });
  const siblingRunId = "sibling-run";
  const siblingRun = join(runsRoot, siblingRunId);
  await mkdir(siblingRun, { mode: 0o700 });
  const outsideTarget = join(harness.root, "outside-target");
  await writeFile(outsideTarget, "OUTSIDE\n");
  const outsideBefore = await fingerprint(outsideTarget);

  const shapes: Array<{ name: string; build: (target: string) => Promise<void> }> = [
    { name: "empty directory", build: async (t) => void (await mkdir(t, { mode: 0o700 })) },
    { name: "regular file", build: async (t) => void (await writeFile(t, "not-a-run\n", { mode: 0o600 })) },
    { name: "symlink to another run", build: async (t) => void (await symlink(siblingRun, t)) },
    { name: "symlink outside", build: async (t) => void (await symlink(outsideTarget, t)) },
    { name: "FIFO", build: async (t) => void execSync(`mkfifo '${t}'`) },
    {
      name: "unsafe mode directory",
      build: async (t) => {
        await mkdir(t, { mode: 0o755 });
        await writeFile(join(t, "stray.txt"), "stray\n");
      },
    },
  ];
  for (const [index, shape] of shapes.entries()) {
    const runId = `occupied-${index}`;
    const target = join(runsRoot, runId);
    await shape.build(target);
    const before = await fingerprint(target);
    const call = makeRunCall(harness, { runId });
    const outcome = await runPipelineV2(call.options, call.deps);
    expectCollision(outcome, harness, runId);
    expect(call.calls).toEqual([]);
    expect(await fingerprint(target)).toBe(before);
    await rm(target, { force: true, recursive: true });
  }
  expect(await fingerprint(outsideTarget)).toBe(outsideBefore);
});

test("preflight failures reserve nothing: the id stays free for a corrected retry", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "reserve-nothing";
  const cases: Array<{ name: string; break: () => Promise<void>; repair?: () => Promise<void> }> = [
    {
      name: "invalid pipeline",
      break: async () => void (await writeFile(join(harness.bundle, "pipeline.yaml"), BROKEN_PIPELINE)),
      repair: async () => void (await writeFile(join(harness.bundle, "pipeline.yaml"), PIPELINE_HAPPY)),
    },
    {
      name: "missing profile",
      break: async () => void (await rm(join(harness.configRoot, "profiles", "coder.yaml"))),
      repair: async () => void (await writeFile(
        join(harness.configRoot, "profiles", "coder.yaml"),
        [
          "schema_version: 1",
          "image: ghcr.io/example/worker:1",
          "opencode_config: opencode/coder.json",
          "env:",
          "  MODEL_API_KEY:",
          "    from_env: CODER_SOURCE_VAR_1",
          "    required: true",
          "",
        ].join("\n"),
      )),
    },
    {
      name: "authorization failure",
      break: async () => {
        // no repair needed: the failure is injected through the auth hook
      },
    },
  ];
  for (const [index, testCase] of cases.entries()) {
    await testCase.break();
    const call = makeRunCall(harness, {
      runId,
      ...(testCase.name === "authorization failure"
        ? {
            onAuth: () => {
              throw new Error("auth rejected");
            },
          }
        : {}),
    });
    const outcome = await runPipelineV2(call.options, call.deps);
    expect(outcome.ok).toBe(false);
    expect(outcome.exitCode).toBe(1);
    expect(outcome.runRoot).toBeNull();
    expect(outcome.state).toBeNull();
    expect(outcome.reason).toBe("invalid_options");
    expect(await lstat(join(harness.stateRoot, "pipeline-runs", runId)).then(
      () => true,
      () => false,
    )).toBe(false);
    const inspect = expectInspectFailure(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
    expect(inspect.reason).toBe("run_not_found");
    await testCase.repair?.();
  }
  // the corrected retry with the same id creates the run
  const retry = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const retryOutcome = await runPipelineV2(retry.options, retry.deps);
  expect(retryOutcome.ok).toBe(true);
  expect(retryOutcome.runId).toBe(runId);
});

test("a failure after the exclusive claim keeps the root: collision, inspect, no auto-removal", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "post-claim-failure";
  const missingProject = join(harness.root, "missing-project");
  const call = makeRunCall(harness, { runId, projectSourcePath: missingProject });
  const outcome = await runPipelineV2(call.options, call.deps);
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(1);
  expect(outcome.runId).toBe(runId);
  expect(outcome.runRoot).toBe(join(harness.stateRoot, "pipeline-runs", runId));
  // the root exists but no state document was ever committed
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  expect(await lstat(runRoot).then((info) => info.isDirectory(), () => false)).toBe(true);
  expect(
    await readFile(join(runRoot, "state.json"), "utf8").then(
      () => true,
      () => false,
    ),
  ).toBe(false);
  const inspect = expectInspectFailure(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(inspect.reason).toBe("run_state_invalid");
  const before = await fingerprint(runRoot);
  const retry = makeRunCall(harness, { runId, projectSourcePath: missingProject });
  const collision = await runPipelineV2(retry.options, retry.deps);
  expectCollision(collision, harness, runId);
  expect(retry.calls).toEqual([]);
  expect(await fingerprint(runRoot)).toBe(before);
});

test("a signal before the exclusive claim reserves nothing", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "signal-first";
  const call = makeRunCall(harness, {
    runId,
    onAuth: () => {
      call.deliverSignal("SIGINT");
    },
  });
  const outcome = await runPipelineV2(call.options, call.deps);
  expect(outcome.ok).toBe(false);
  expect(outcome.exitCode).toBe(130);
  expect(outcome.runRoot).toBeNull();
  expect(await lstat(join(harness.stateRoot, "pipeline-runs", runId)).then(
    () => true,
    () => false,
  )).toBe(false);
  const inspect = expectInspectFailure(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(inspect.reason).toBe("run_not_found");
});

test("capture: the run id is read exactly once and survives caller mutation after the await", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "capture-run";
  let runIdReads = 0;
  let mutatingOptions: PipelineV2RunOptions;
  const call = makeRunCall(harness, {
    runId,
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
    onAuth: () => {
      // mutation delivered after the capture but before the run id selection
      (mutatingOptions as { runId?: string }).runId = "mutated-after-await";
    },
  });
  mutatingOptions = new Proxy(call.options, {
    get(target, property, receiver) {
      if (property === "runId") {
        runIdReads += 1;
      }
      return Reflect.get(target, property, receiver);
    },
  });
  const outcome = await runPipelineV2(mutatingOptions, call.deps);
  expect(outcome.ok).toBe(true);
  expect(outcome.runId).toBe(runId);
  expect(runIdReads).toBe(1);
  expect(await readDurableState(join(harness.stateRoot, "pipeline-runs", runId))).toBeTruthy();
});

test("concurrency, identical options: exactly one exclusive winner, the loser collides", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "race-identical";
  let arrived = 0;
  let release: (() => void) | null = null;
  const barrier = {
    arrive: () => {
      arrived += 1;
      if (arrived === 2) {
        release?.();
      }
    },
    wait: new Promise<void>((resolve) => {
      release = resolve;
    }),
  };
  const a = makeRunCall(harness, {
    runId,
    authBarrier: barrier,
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const b = makeRunCall(harness, {
    runId,
    authBarrier: barrier,
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const [outcomeA, outcomeB] = await Promise.all([
    runPipelineV2(a.options, a.deps),
    runPipelineV2(b.options, b.deps),
  ]);
  const winner = outcomeA.ok ? a : b;
  const loser = outcomeA.ok ? b : a;
  const winnerOutcome = outcomeA.ok ? outcomeA : outcomeB;
  const loserOutcome = outcomeA.ok ? outcomeB : outcomeA;
  expect(winnerOutcome.ok).toBe(true);
  expectCollision(loserOutcome, harness, runId);
  expect(loser.calls).toEqual([]);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const state = await readDurableState(runRoot);
  expect(state.status).toBe("success");
  expect(state.executions).toHaveLength(2);
  expect(state.transitions).toHaveLength(2);
  const activations = await readdir(join(runRoot, "activations"));
  expect(activations).toEqual(["1-coder"]);
  const report = await readFile(join(runRoot, "outputs", "report"), "utf8");
  expect(JSON.parse(report)).toEqual({ ok: true });
});

test("concurrency, conflicting options: the same id still yields exactly one run", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "race-conflicting";
  let arrived = 0;
  let release: (() => void) | null = null;
  const barrier = {
    arrive: () => {
      arrived += 1;
      if (arrived === 2) {
        release?.();
      }
    },
    wait: new Promise<void>((resolve) => {
      release = resolve;
    }),
  };
  const a = makeRunCall(harness, {
    runId,
    authBarrier: barrier,
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const b = makeRunCall(harness, {
    runId,
    authBarrier: barrier,
    projectSourcePath: harness.otherProjectSource,
    bindings: [
      { id: "facts_seed", path: harness.otherFactsFile },
      { id: "source", path: harness.otherSourceFile },
    ],
    script: { workerOutputs: HAPPY_WORKER_OUTPUTS },
  });
  const [outcomeA, outcomeB] = await Promise.all([
    runPipelineV2(a.options, a.deps),
    runPipelineV2(b.options, b.deps),
  ]);
  const loserOutcome = outcomeA.ok ? outcomeB : outcomeA;
  const loser = outcomeA.ok ? b : a;
  expectCollision(loserOutcome, harness, runId);
  expect(loser.calls).toEqual([]);
  expect((await readDurableState(join(harness.stateRoot, "pipeline-runs", runId))).status).toBe("success");
});

test("lost result L1: after the durable create_run, before any worker execution", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "lost-before-execution";
  const call = makeRunCall(harness, { runId, script: { hangOnCreate: true } });
  const abandoned = runPipelineV2(call.options, call.deps);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const state = await waitForDurableState(
    runRoot,
    (current) => {
      if (current.executions.length !== 1) {
        return false;
      }
      try {
        return agentOf(current, 1).phase === "data_prepared";
      } catch {
        return false;
      }
    },
    { deadlineMs: 12000 },
  );
  expect(agentOf(state, 1).phase).toBe("data_prepared");
  void abandoned;
  const before = await fingerprint(runRoot);
  const report = expectReport(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(report.status).toBe("running");
  expect(report.boundary).toEqual({
    kind: "in_flight",
    executionIndex: 1,
    stateId: "coder",
    executionRole: "planning",
    phase: "data_prepared",
  });
  const retry = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const collision = await runPipelineV2(retry.options, retry.deps);
  expectCollision(collision, harness, runId);
  expect(retry.calls).toEqual([]);
  expect(await fingerprint(runRoot)).toBe(before);
  // measured: the hanging child reaches its durable phase in <=2s sequentially and
  // up to ~4s under bun test --parallel=2 (2026-10-13); the poller deadline (12s)
  // stays below this local timeout, so the poller diagnostic reports first.
}, 15000);

test("lost result L2: in-flight worker execution is never re-run by a second run", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "lost-in-flight";
  const call = makeRunCall(harness, { runId, script: { hangOnRun: true } });
  const abandoned = runPipelineV2(call.options, call.deps);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const state = await waitForDurableState(
    runRoot,
    (current) => {
      try {
        return agentOf(current, 1).phase === "running";
      } catch {
        return false;
      }
    },
    { deadlineMs: 12000 },
  );
  expect(agentOf(state, 1).phase).toBe("running");
  void abandoned;
  const before = await fingerprint(runRoot);
  const report = expectReport(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(report.boundary).toEqual({
    kind: "in_flight",
    executionIndex: 1,
    stateId: "coder",
    executionRole: "planning",
    phase: "running",
  });
  const retry = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const collision = await runPipelineV2(retry.options, retry.deps);
  expectCollision(collision, harness, runId);
  expect(retry.calls).toEqual([]);
  expect(await fingerprint(runRoot)).toBe(before);
// measured: the hanging child reaches phase "running" up to ~4s under
// bun test --parallel=2 (2026-10-13); the poller deadline (12s) stays below this local timeout.
}, 15000);

test("lost result L3: plan-ready boundary continues through the existing resume-plan", async () => {
  const harness = await setupHarness(PIPELINE_STAGE, ["architect", "coder"]);
  const runId = "lost-plan-ready";
  const call = makeRunCall(harness, {
    runId,
    bindings: [{ id: "task", path: harness.sourceFile }],
    script: { workerOutputs: { architect: { plan: JSON.stringify(PLAN_PROPOSAL) } } },
  });
  const first = await runPipelineV2(call.options, call.deps);
  expect(first.ok).toBe(false);
  expect(first.planReady).toBe(true);
  expect(first.exitCode).toBe(0);
  expect(first.runId).toBe(runId);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const before = await fingerprint(runRoot);
  const report = expectReport(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(report.status).toBe("running");
  expect(report.boundary).toEqual({ kind: "plan_ready", planningExecutionIndex: 1, continuation: "resume_plan" });
  const retry = makeRunCall(harness, {
    runId,
    bindings: [{ id: "task", path: harness.sourceFile }],
  });
  const collision = await runPipelineV2(retry.options, retry.deps);
  expectCollision(collision, harness, runId);
  expect(await fingerprint(runRoot)).toBe(before);
  // the existing command continues the run to terminal success
  const resumeCall = makeRunCall(harness, {
    runId,
    bindings: [{ id: "task", path: harness.sourceFile }],
    script: { workerOutputs: STAGE_WORKER_OUTPUTS },
  });
  const resumed = await resumePipelineV2PlanningRunPlan(
    {
      runId,
      stageId: "stage-1",
      initialBudget: 1,
      configRoot: harness.configRoot,
      launcherId: EXPECTED_LAUNCHER_ID,
    },
    resumeCall.deps,
  );
  expect(resumed.ok).toBe(true);
  expect(resumed.exitCode).toBe(0);
  const finalState = await readDurableState(runRoot);
  expect(finalState.status).toBe("success");
  expect(finalState.executions).toHaveLength(2);
  expect(finalState.transitions).toHaveLength(2);
  const result = await readFile(join(runRoot, "outputs", "result"), "utf8");
  expect(JSON.parse(result)).toEqual({ ok: true });
});

test("lost result L4: after terminal success the run and outputs stay byte-identical", async () => {
  const harness = await setupHarness(PIPELINE_HAPPY, ["coder"]);
  const runId = "lost-after-success";
  const call = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const first = await runPipelineV2(call.options, call.deps);
  expect(first.ok).toBe(true);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const before = await fingerprint(runRoot);
  const report = expectReport(await inspectPipelineV2Run({ runId, stateRoot: harness.stateRoot }));
  expect(report.status).toBe("succeeded");
  expect(report.boundary).toEqual({ kind: "succeeded", terminalStateId: "done" });
  expect(report.outputs).toEqual([
    { id: "report", type: "json", required: true, present: true, digest: expect.any(String) },
  ]);
  const retry = makeRunCall(harness, { runId, script: { workerOutputs: HAPPY_WORKER_OUTPUTS } });
  const collision = await runPipelineV2(retry.options, retry.deps);
  expectCollision(collision, harness, runId);
  expect(retry.calls).toEqual([]);
  expect(await fingerprint(runRoot)).toBe(before);
});

test("the collision error class carries the exact run id and root", () => {
  const error = new PipelineV2RunRootCollisionError("some-run", "/state/pipeline-runs/some-run");
  expect(error.runId).toBe("some-run");
  expect(error.localRunRoot).toBe("/state/pipeline-runs/some-run");
  expect(error instanceof Error).toBe(true);
});

test("source restrictions: one generator site, one exclusive claim, no discovery scan", async () => {
  const runner = await readFile(new URL("../src/pipeline_v2_runner.ts", import.meta.url), "utf8");
  // exactly one built-in generator site; the explicit id bypasses it
  expect(runner.split("crypto.randomUUID").length - 1).toBe(1);
  const generatorLine = runner.split("\n").findIndex((line) => line.includes("crypto.randomUUID"));
  const bypassText = runner.slice(0, runner.indexOf("crypto.randomUUID"));
  expect(bypassText).toContain("callerRunId !== undefined ? callerRunId :");
  // exactly one authoritative exclusive leaf claim; no non-recursive fallback
  const leafClaims = runner.split("mkdir(localRunRoot").length - 1;
  expect(leafClaims).toBe(1);
  const leafIndex = runner.indexOf("mkdir(localRunRoot");
  expect(runner.slice(leafIndex, leafIndex + 40)).toContain("{ mode: 0o700 }");
  // the collision path never opens the existing run's state
  const collisionIndex = runner.indexOf('reason: "run_already_exists"');
  expect(collisionIndex).toBeGreaterThan(-1);
  const collisionBranch = runner.slice(Math.max(0, collisionIndex - 900), collisionIndex + 200);
  expect(collisionBranch).toContain("PipelineV2RunRootCollisionError");
  expect(collisionBranch).not.toContain("PipelineV2RunStateStore");
  expect(collisionBranch).not.toContain(".load()");
  // no discovery scan and no process-local registry in the runner
  expect(runner).not.toContain("readdir");
  expect(runner).not.toContain("new Map<string, PipelineV2RunState>");
  // the collision error is owned by the public reason module and re-exported
  expect(runner).toContain('export { PipelineV2RunRootCollisionError }');
  const publicReasonSource = await readFile(
    new URL("../src/pipeline_v2_public_reason.ts", import.meta.url),
    "utf8",
  );
  expect(publicReasonSource).toContain("export class PipelineV2RunRootCollisionError");
  expect(publicReasonSource).toContain("export const PIPELINE_V2_PUBLIC_REASONS");
  expect(publicReasonSource).toContain("export function publicReasonFor");
  // one reason mapper and one vocabulary; classification never uses
  // substring matching, regular expressions or message text
  expect(publicReasonSource).not.toContain(".includes(");
  expect(publicReasonSource).not.toContain("RegExp");
  expect(publicReasonSource).not.toContain(".match(");
  expect(publicReasonSource).not.toContain(".test(");
  // the runner classifies only through the mapper
  expect(runner.split("publicReasonFor(").length - 1).toBeGreaterThan(0);
  expect(runner).not.toContain('.message.includes');
});

test("waitForState battery: the poller is wall-clock bounded and every negative poll yields", async () => {
  let reads = 0;
  const started = performance.now();
  let deadlineError: Error | null = null;
  try {
    await waitForState(
      async () => {
        reads += 1;
        return { marker: false };
      },
      (value) => value.marker,
      { deadlineMs: 120 },
    );
  } catch (cause) {
    deadlineError = cause as Error;
  }
  const elapsed = performance.now() - started;
  expect(deadlineError !== null).toBe(true);
  expect(deadlineError?.message).toContain("within 120ms");
  expect(deadlineError?.message).not.toContain("marker");
  expect(elapsed).toBeGreaterThanOrEqual(110);
  expect(reads).toBeLessThanOrEqual(90);
  expect(reads).toBeGreaterThan(10);
});

test("waitForState battery: the fast path returns on the first satisfying poll without delay", async () => {
  let reads = 0;
  const started = performance.now();
  const value = await waitForState(
    async () => {
      reads += 1;
      return { marker: true };
    },
    (candidate) => candidate.marker,
    { deadlineMs: 5000 },
  );
  expect(value.marker).toBe(true);
  expect(reads).toBe(1);
  expect(performance.now() - started).toBeLessThan(50);
});

test("waitForState battery: a satisfying observation immediately before the deadline is accepted", async () => {
  const barrier = Bun.sleep(80).then(() => "ready");
  const value = await waitForState(
    async () => {
      return { phase: (await barrier) === "ready" ? "settled" : "pending" };
    },
    (candidate) => candidate.phase === "settled",
    { deadlineMs: 4000 },
  );
  expect(value.phase).toBe("settled");
});

test("waitForState battery: a read failure is reported content-free by the deadline diagnostic", async () => {
  let deadlineError: Error | null = null;
  try {
    await waitForState(
      async () => {
        throw new Error("synthetic read failure");
      },
      () => true,
      { deadlineMs: 80 },
    );
  } catch (cause) {
    deadlineError = cause as Error;
  }
  expect(deadlineError?.message).toContain("within 80ms");
  expect(deadlineError?.message).toContain("does not exist yet");
  expect(deadlineError?.message).not.toContain("synthetic read failure");
});
