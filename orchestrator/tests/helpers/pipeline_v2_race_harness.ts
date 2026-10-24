/**
 * Shared test-only harness for the inter-process run-mutation concurrency
 * suites: a fixture pipeline (planning -> stage loop with the trusted
 * plan_ready policy), a fake profile configuration and a real separate
 * OS process runner for one mutating pipeline v2 command, synchronized
 * with the parent through stdin gates inside the fake transport.
 *
 * This file is test infrastructure only: production code never imports it.
 */
import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../../src/pipeline_v2_state.ts";
import { loadPipelineV2 } from "../../src/pipeline_v2.ts";
import { PipelineV2RunStateSink } from "../../src/pipeline_v2_state_sink.ts";
import { prepareWaitIntent } from "../../src/pipeline_v2_run_plan_manifests.ts";
import { acceptPipelineV2ContinueStageIntent } from "../../src/pipeline_v2_continue_stage_intent_controller.ts";
import { restorePipelineV2AcceptedRunPlan } from "../../src/pipeline_v2_run_plan_restore.ts";
import { openPipelineV2ContinuedStage } from "../../src/pipeline_v2_continued_stage_controller.ts";
import { prepareRunProject, snapshotRunInputs } from "../../src/pipeline_v2_runtime.ts";
import { enterPipelineV2Wait } from "../../src/pipeline_v2_wait_controller.ts";
import { pipelineV2RunPipelineIdentity } from "../../src/pipeline_v2_digest.ts";

const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

export const STAGE_LOOP_YAML_PLAN_READY = `schema_version: 2
entry_state: architect
max_transitions: 40

inputs:
  - id: task
    type: file
    protected: true

outputs: []

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
      plan_ready:
        stage_position: 1
        initial_budget: 1
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
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: done
    type: terminal
    result: success
`;

export const STAGE_LOOP_YAML_MANUAL = STAGE_LOOP_YAML_PLAN_READY.replace(
  "      plan_ready:\n        stage_position: 1\n        initial_budget: 1\n",
  "",
);

export const PLAN_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
  ],
  new_tasks: [{ id: "task-a", body: "RACE-TASK: implement the staged work" }],
};

export const WORKER_OUTPUTS = {
  architect: { plan: JSON.stringify(PLAN_PROPOSAL) },
};

export interface Harness {
  root: string;
  stateRoot: string;
  bundlePlanReady: string;
  bundleManual: string;
  configRoot: string;
  projectSource: string;
  taskFile: string;
}

const ROOTS: string[] = [];

export async function buildHarness(prefix: string): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true, mode: 0o700 });
  const configRoot = join(root, "config");
  for (const name of ["architect", "coder"]) {
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
        `    from_env: ${name.toUpperCase()}_SOURCE_VAR_1`,
        "    required: true",
        "",
      ].join("\n"),
    );
    await writeFile(join(configRoot, "opencode", `${name}.json`), JSON.stringify({ model: "glm53-flash" }));
  }
  await writeFile(join(configRoot, "credential.token"), "cred-token-not-real\n", { mode: 0o600 });
  const writeBundleVariant = async (name: string, yaml: string): Promise<string> => {
    const bundle = join(root, name);
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), yaml);
    await writeFile(join(bundle, "prompts", "architect.md"), "plan the work\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    return bundle;
  };
  const bundlePlanReady = await writeBundleVariant("bundle-plan-ready", STAGE_LOOP_YAML_PLAN_READY);
  const bundleManual = await writeBundleVariant("bundle-manual", STAGE_LOOP_YAML_MANUAL);
  const projectSource = join(root, "project-src");
  await mkdir(projectSource, { recursive: true });
  await writeFile(join(projectSource, "seed.md"), "PROJECT-SEED\n");
  const taskFile = join(root, "task.txt");
  await writeFile(taskFile, "RACE-TASK-BODY\n");
  return { root, stateRoot, bundlePlanReady, bundleManual, configRoot, projectSource, taskFile };
}

export async function cleanupRoots(): Promise<void> {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
}

export interface ChildSpec {
  command: "run" | "resume" | "continue-stage" | "revise-task" | "resume-plan" | "respond";
  stateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource?: string;
  inputBindings?: Array<{ id: string; path: string }>;
  runId: string;
  waitIndex?: number;
  additionalIterations?: number;
  taskId?: string;
  taskBody?: string;
  stageId?: string;
  initialBudget?: number;
  actionId?: string;
  runCode?: number;
  gateAuth?: boolean;
  gateSessionCreate?: number;
  gateWorkerRun?: boolean;
  profileEnv?: Readonly<Record<string, string>>;
  workerOutputs?: Readonly<Record<string, Readonly<Record<string, string>>>>;
  /** Blocks on a stdin line right before the mutating call starts. */
  waitStart?: boolean;
}

export const PROFILE_ENV: Record<string, string> = {
  ARCHITECT_SOURCE_VAR_1: "architect-secret",
  CODER_SOURCE_VAR_1: "coder-secret",
};

export const HELPER_PATH = join(import.meta.dir, "pipeline_v2_mutation_subprocess.ts");

export interface ChildResult {
  outcome: Record<string, unknown>;
  stats: Record<string, number>;
  exitCode: number | null;
}

export class MutatingChild {
  private readonly procRef: { value?: Bun.Subprocess<"pipe", "pipe", "pipe"> } = {};
  private readonly stdinRef: { value?: Bun.FileSink } = {};
  private readonly holdCounts = new Map<string, number>();
  private readonly gates = new Map<string, { promise: Promise<void>; resolve: () => void }>();
  private readonly heldGates!: ReadonlySet<string>;
  private resultResolve!: (value: ChildResult) => void;
  readonly result: Promise<ChildResult>;
  readonly stderrText: Promise<string>;

  constructor(spec: ChildSpec, specDir: string, heldGates: ReadonlySet<string> = new Set()) {
    this.heldGates = new Set(heldGates);
    const specPath = join(specDir, `spec-${Math.random().toString(36).slice(2)}.json`);
    void Bun.write(specPath, JSON.stringify(spec));
    this.procRef.value = Bun.spawn([process.execPath, HELPER_PATH, "--spec", specPath], {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      cwd: join(import.meta.dir, ".."),
    });
    this.stdinRef.value = this.procRef.value.stdin!;
    const readySignal: { resolve?: () => void } = {};
    const ready = new Promise<void>((resolve) => {
      readySignal.resolve = resolve;
    });
    this.result = new Promise<ChildResult>((resolve) => {
      this.resultResolve = resolve;
    });
    const stderrChunks: string[] = [];
    void (async () => {
      const decoder = new TextDecoder();
      const err = this.procRef.value!.stderr as unknown as AsyncIterable<Uint8Array>;
      for await (const chunk of err) {
        stderrChunks.push(decoder.decode(chunk));
      }
    })();
    this.stderrText = new Promise<string>((resolve) => {
      void (async () => {
        await this.procRef.value!.exited;
        resolve(stderrChunks.join(""));
      })();
    });
    void (async () => {
      let buffer = "";
      const decoder = new TextDecoder();
      const out = this.procRef.value!.stdout as unknown as AsyncIterable<Uint8Array>;
      for await (const chunk of out) {
        buffer += decoder.decode(chunk);
        for (;;) {
          const newline = buffer.indexOf("\n");
          if (newline < 0) break;
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          if (line === "") continue;
          if (line === "READY") {
            readySignal.resolve?.();
            continue;
          }
          if (line.startsWith("GATE ")) {
            const gate = line.slice(5).trim();
            const held = this.heldGates.has(gate);
            if (held) {
              const count = (this.holdCounts.get(gate) ?? 0) + 1;
              this.holdCounts.set(gate, count);
              if (count > 1) {
                // Only the first occurrence of the gate is held; later
                // occurrences (a second worker run) pass through.
                this.stdinRef.value!.write("go\n");
                void this.stdinRef.value!.flush();
              }
            } else {
              this.stdinRef.value!.write("go\n");
              void this.stdinRef.value!.flush();
            }
            this.gates.get(gate)?.resolve();
            continue;
          }
          if (line.startsWith("RESULT ")) {
            const parsed = JSON.parse(line.slice(7)) as ChildResult;
            this.resultResolve(parsed);
            continue;
          }
        }
      }
      this.resultResolve({
        outcome: { ok: false, exitCode: 1, reason: "child-crashed" },
        stats: {},
        exitCode: this.procRef.value!.exitCode,
      });
    })();
    void ready;
  }

  /** Resolves once the child reports it reached (and blocked at) the gate. */
  gate(name: string): Promise<void> {
    let entry = this.gates.get(name);
    if (entry === undefined) {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => {
        resolve = r;
      });
      entry = { promise, resolve };
      this.gates.set(name, entry);
    }
    return entry.promise;
  }

  release(): void {
    this.stdinRef.value!.write("go\n");
    void this.stdinRef.value!.flush();
  }

  async finish(): Promise<ChildResult> {
    return await this.result;
  }

  kill(): void {
    this.procRef.value!.kill(9);
  }
}

export async function readState(stateRoot: string, runId: string): Promise<PipelineV2RunState> {
  const path = join(stateRoot, "pipeline-runs", runId, "state.json");
  return parsePipelineV2RunState(await readFile(path, "utf8"));
}

export async function treeFingerprint(root: string): Promise<string> {
  const entries: string[] = [];
  const walk = async (dir: string, rel: string): Promise<void> => {
    const names = (await readdir(dir)).sort();
    for (const name of names) {
      const abs = join(dir, name);
      const relPath = rel === "" ? name : `${rel}/${name}`;
      const info = await stat(abs);
      if (info.isDirectory()) {
        entries.push(`d ${relPath} 0${(info.mode & 0o7777).toString(8)}`);
        await walk(abs, relPath);
      } else if (info.isFile()) {
        const bytes = await readFile(abs);
        entries.push(`f ${relPath} ${info.size}:${bytes.length}`);
      } else {
        entries.push(`? ${relPath}`);
      }
    }
  };
  await walk(root, "");
  return entries.join("\n");
}

/**
 * Builds the resumable post-intervention boundary through the real
 * facades: the waiting run's open wait is answered with the continue_stage
 * intent, the grant and closure are applied, the response is recorded and
 * the successor iteration is opened — with no successor execution started.
 */
export async function prefixResumableBoundary(
  harness: Harness,
  runId: string,
): Promise<void> {
  const pipeline = await loadPipelineV2(harness.bundlePlanReady);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  const sink = await PipelineV2RunStateSink.open({ stateRoot: harness.stateRoot, runId });
  const snapshot = sink.snapshot!;
  const lastWait = snapshot.waits[snapshot.waits.length - 1]!;
  const generation = snapshot.generations[snapshot.generations.length - 1]!;
  const intent = prepareWaitIntent({
    schema_version: 1,
    kind: "continue_stage_intent",
    run_id: runId,
    wait_index: lastWait.index,
    stage_id: generation.stage_id,
    expected_plan_sha256: generation.plan_sha256,
    additional_iterations: 1,
  });
  await acceptPipelineV2ContinueStageIntent({ runRoot, sink, intent });
  const restored = await restorePipelineV2AcceptedRunPlan({ pipeline, runRoot, state: sink.snapshot });
  await openPipelineV2ContinuedStage({
    runRoot,
    sink,
    intent,
    compiledPlan: restored.compiled_plan,
    initialBudget: generation.initial_budget,
  });
}

/**
 * Builds a waiting run with one non-reserved action ("approve") through
 * the honest reducer path: the run root, the run-owned project copy, the
 * run-input snapshot, `create_run` and `enterPipelineV2Wait` are exactly
 * the commands the production coordinator would dispatch at this boundary.
 */
export async function prefixCustomWait(
  harness: Harness,
  runId: string,
): Promise<void> {
  const pipeline = await loadPipelineV2(harness.bundlePlanReady);
  const runRoot = join(harness.stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  const preparedProjectBinding005 = (await prepareRunProject(harness.projectSource, runRoot)).binding;
  const runInputs = await snapshotRunInputs(pipeline, [{ id: "task", path: harness.taskFile }], runRoot);
  const sink = new PipelineV2RunStateSink({ stateRoot: harness.stateRoot, runId });
  await sink.dispatch({ kind: "create_run", runId, pipeline: pipelineV2RunPipelineIdentity(pipeline), inputs: runInputs.inputs.map((input) => ({
    id: input.id, type: input.type, protected: input.protected, digest: input.digest,
  })), projectProvenance: preparedProjectBinding005 });
  await enterPipelineV2Wait({
    runRoot,
    sink,
    reason: "user_pause",
    actions: [{ id: "approve", to: "architect" }],
  });
}

export function summarize(result: ChildResult): Record<string, unknown> {
  const outcome = result.outcome as Record<string, unknown>;
  const state = outcome.state as PipelineV2RunState | null;
  return {
    ok: outcome.ok,
    exitCode: outcome.exitCode,
    reason: outcome.reason,
    waiting: outcome.waiting,
    revision: state?.revision ?? null,
    status: state?.status ?? null,
    stats: result.stats,
  };
}
