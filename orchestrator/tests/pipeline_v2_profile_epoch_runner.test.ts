/**
 * The durable run-wide profile epoch across real production runner
 * invocations: drift refusals, allowed changes, the loaded-snapshot
 * guarantee and the content-free leakage contract.
 */
import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readFileSync, rmSync, statSync } from "node:fs";
import { afterAll, expect, test } from "bun:test";
import {
  runPipelineV2,
  resumePipelineV2PlanningRunPlan,
  resumePipelineV2,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "../src/pipeline_v2_runner.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PROFILE_EPOCH_FILE_NAME } from "../src/pipeline_v2_profile_epoch_store.ts";
import {
  preparePipelineV2ProfileEpoch,
  profileEpochConfigDigest,
  profileEpochEnvValueDigest,
} from "../src/pipeline_v2_profile_epoch.ts";

const ROOTS: string[] = [];

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

const WORKER_IMAGE = `ghcr.io/example/worker@sha256:${"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}`;
const SECRET_VALUE = "super-secret-value-9f1";
const SERVER_VALUE = "https://llm.example/v1";
const OPT_SECRET_VALUE = "optional-secret-value-9f2";
const CONFIG_CONTENT = '{"$schema":"https://opencode.ai/config.json","model":"glm53-flash"}';

const BUNDLE_YAML = `
schema_version: 2
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
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
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
        to: done
  - id: done
    type: terminal
    result: success
`;

const PLAN_PROPOSAL = {
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "PLAN-TASK-BODY" }],
};

interface Harness {
  root: string;
  stateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource: string;
  credentialFile: string;
  taskFile: string;
  runRoot: (runId: string) => string;
}

interface WorkerObservation {
  stateId: string;
  image: string;
  env: Record<string, string>;
}

interface Script {
  stageWorkerFails?: boolean;
  onStageWorker?: () => void;
  envOverrides?: Record<string, string | undefined>;
}

async function writeProfile(
  configRoot: string,
  options: { name?: string; configName?: string; image?: string; configBytes?: string } = {},
): Promise<void> {
  const name = options.name ?? "coder";
  const configName = options.configName ?? `${name}.json`;
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(
    join(configRoot, "profiles", `${name}.yaml`),
    [
      "schema_version: 2",
      `image: ${options.image ?? WORKER_IMAGE}`,
      "opencode_config:",
      `  path: opencode/${configName}`,
      "  secrets: env_only",
      "env:",
      "  CODER_API_KEY:",
      "    from_env: CODER_SOURCE_VAR_1",
      "    required: true",
      "    secret: true",
      "  LLM_SERVER:",
      "    from_env: LLM_SERVER",
      "    required: true",
      "    secret: false",
      "  OPT_KEY:",
      "    from_env: OPT_SOURCE_VAR",
      "    required: false",
      "    secret: true",
      "  OPT_SERVER:",
      "    from_env: OPT_SERVER_VALUE",
      "    required: false",
      "    secret: false",
      "",
    ].join("\n"),
  );
  await writeFile(join(configRoot, "opencode", configName), options.configBytes ?? CONFIG_CONTENT, { mode: 0o600 });
}

async function makeHarness(prefix: string): Promise<Harness> {
  const root = await mkdtemp(join(tmpdir(), `${prefix}-`));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true, mode: 0o700 });
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), BUNDLE_YAML);
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  await writeFile(join(bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
  await writeFile(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
  const configRoot = join(root, "config");
  await writeProfile(configRoot);
  const projectSource = join(root, "project-src");
  await mkdir(projectSource, { recursive: true });
  await writeFile(join(projectSource, "seed.md"), "PROJECT-SEED\n");
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  const taskFile = join(sources, "task.md");
  await writeFile(taskFile, "TASK-BODY\n");
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
    taskFile,
    runRoot: (runId: string) => join(stateRoot, "pipeline-runs", runId),
  };
}

function baseEnv(extra: Record<string, string | undefined> = {}): Record<string, string | undefined> {
  return {
    HOME: "/home/u",
    CODER_SOURCE_VAR_1: SECRET_VALUE,
    LLM_SERVER: SERVER_VALUE,
    ...extra,
  };
}

let sessionCounter = 0;
let stageWorkerCalls = 0;
let workerObservations: WorkerObservation[] = [];

function runnerDeps(harness: Harness, script: Script = {}): PipelineV2RunnerDeps {
  stageWorkerCalls = 0;
  workerObservations = [];
  const cli = async (args: readonly string[]) => {
    if (args[0] === "session" && args[1] === "create") {
      sessionCounter += 1;
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${sessionCounter}`, launcher_id: "dhl_epoch" },
          token: `dhc_${sessionCounter}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
    }
    return { code: 0 };
  };
  const operationRunner = async (request: { body: string }) => {
    const body = JSON.parse(request.body) as {
      environment?: Record<string, string>;
      mounts?: Array<{ source?: string; target?: string }>;
    };
    const outputsMount = (body.mounts ?? []).find((mount) => mount.target === "/pipeline/outputs");
    const dash = (outputsMount?.source ?? "").split("/")[1] ?? "";
    const dashIndex = dash.indexOf("-");
    const activationIndex = dashIndex === -1 ? "" : dash.slice(0, dashIndex);
    const stateId = dashIndex === -1 ? dash : dash.slice(dashIndex + 1);
    workerObservations.push({
      stateId,
      image: (body as { image?: string }).image ?? "",
      env: { ...(body.environment ?? {}) },
    });
    if (stateId === "architect" && activationIndex === "1") {
      const dir = join(harness.runRoot("epoch-flow"), outputsMount?.source ?? "");
      await mkdir(dir, { recursive: true });
      await writeFile(join(dir, "plan"), JSON.stringify(PLAN_PROPOSAL), { mode: 0o600 });
    }
    if (stateId !== "architect") {
      stageWorkerCalls += 1;
      script.onStageWorker?.();
      if (script.stageWorkerFails === true) {
        return { outcome: "worker_failed" as const, capture: EMPTY_CAPTURE };
      }
    }
    return { outcome: "completed" as const, capture: EMPTY_CAPTURE };
  };
  return {
    cli: cli as never,
    operationRunner,
    fetchAuth: (async () => ({
      status: 200,
      body: { authority: "launcher", principal: "tester", launcher_id: "dhl_epoch" },
    })) as never,
    helperConfig: { socketPath: "/run/dh.sock", credentialFile: harness.credentialFile },
    baseEnv: baseEnv(script.envOverrides),
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    now: () => new Date(0),
  };
}

const EMPTY_CAPTURE = {
  status: "unavailable" as const,
  byteCount: 0,
  tailBytes: new Uint8Array(0),
  truncatedDaemon: false,
  truncatedLocal: false,
};

function runOptions(harness: Harness): {
  pipelineRoot: string;
  configRoot: string;
  projectSourcePath: string;
  inputBindings: readonly { id: string; path: string }[];
} {
  return {
    pipelineRoot: harness.bundle,
    configRoot: harness.configRoot,
    projectSourcePath: harness.projectSource,
    inputBindings: [{ id: "task", path: harness.taskFile }],
  };
}

function expectPlanReady(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  const record = outcome as unknown as Record<string, unknown>;
  expect(record.ok).toBe(false);
  expect(record.planReady).toBe(true);
  expect(record.exitCode).toBe(0);
  expect("reason" in record).toBe(false);
  return record.state as PipelineV2RunState;
}

function expectSuccess(outcome: PipelineV2RunOutcome): PipelineV2RunState {
  expect(outcome.ok).toBe(true);
  expect(outcome.exitCode).toBe(0);
  return outcome.state!;
}

async function readDurable(harness: Harness, runId: string): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(harness.runRoot(runId), "state.json"), "utf8"));
}

function runTreeFingerprint(harness: Harness, runId: string): string {
  const entries: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    for (const name of readFileSync === undefined ? [] : []) {
      void name;
    }
    const dirHandle = require("node:fs").readdirSync(dir, { withFileTypes: true }) as Array<{
      name: string;
      isDirectory(): boolean;
    }>;
    for (const entry of dirHandle) {
      const path = join(dir, entry.name);
      const st = statSync(path);
      entries.push(`${prefix}${entry.name}:${st.isFile() ? "file" : "dir"}:${st.mode & 0o777}:${st.size}`);
      if (entry.isDirectory()) {
        walk(path, `${prefix}${entry.name}/`);
      }
    }
  };
  walk(harness.runRoot(runId), "");
  return entries.sort().join("\n");
}

const RUN_ID = "epoch-flow";

async function freshToPlanReady(harness: Harness, script: Script = {}): Promise<PipelineV2RunState> {
  const outcome = await runPipelineV2(
    { ...runOptions(harness), runId: RUN_ID },
    runnerDeps(harness, script),
  );
  return expectPlanReady(outcome);
}

async function continueWithResumePlan(
  harness: Harness,
  script: Script = {},
  configRoot = harness.configRoot,
): Promise<PipelineV2RunOutcome> {
  return await resumePipelineV2PlanningRunPlan(
    { runId: RUN_ID, stageId: "stage-1", initialBudget: 1, configRoot, launcherId: "dhl_epoch" },
    runnerDeps(harness, script),
  );
}

// --- the control and the drift refusals --------------------------------------

test("epoch runner: the control flow reaches terminal success with the epoch artifact", async () => {
  const harness = await makeHarness("epoch-control");
  const planned = await freshToPlanReady(harness);
  expect(planned.profile_epoch).toEqual({ digest: expect.any(String), profile_count: 1 });
  const artifactPath = join(harness.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME);
  const artifactBytes = await readFile(artifactPath, "utf8");
  expect(statSync(artifactPath).mode & 0o777).toBe(0o600);
  expect(artifactBytes).toContain('"kind":"pipeline_v2_profile_epoch"');
  const expectedManifest = preparePipelineV2ProfileEpoch([
    {
      id: "coder",
      image_ref: WORKER_IMAGE,
      config_policy: "env_only",
      config_sha256: profileEpochConfigDigest(CONFIG_CONTENT),
      env_bindings: [
        {
          destination: "CODER_API_KEY",
          from_env: "CODER_SOURCE_VAR_1",
          required: true,
          secret: true,
        },
        {
          destination: "LLM_SERVER",
          from_env: "LLM_SERVER",
          required: true,
          secret: false,
          value_sha256: profileEpochEnvValueDigest(SERVER_VALUE),
        },
        {
          destination: "OPT_KEY",
          from_env: "OPT_SOURCE_VAR",
          required: false,
          secret: true,
        },
        {
          destination: "OPT_SERVER",
          from_env: "OPT_SERVER_VALUE",
          required: false,
          secret: false,
        },
      ],
    },
  ]);
  expect(artifactBytes).toBe(expectedManifest.canonical_json);
  expect(planned.profile_epoch).toEqual({
    digest: expectedManifest.sha256,
    profile_count: 1,
  });
  const outcome = await continueWithResumePlan(harness);
  const state = expectSuccess(outcome);
  expect(state.status).toBe("success");
  expect(state.terminal).toEqual({ state_id: "done", result: "success" });
  // the artifact never changes after publication
  expect(await readFile(artifactPath, "utf8")).toBe(artifactBytes);
});

test("epoch runner: changed config bytes are a typed refusal with zero effects", async () => {
  const harness = await makeHarness("epoch-config-drift");
  await freshToPlanReady(harness);
  const before = await readDurable(harness, RUN_ID);
  const fingerprint = runTreeFingerprint(harness, RUN_ID);
  await writeFile(join(harness.configRoot, "opencode", "coder.json"), '{"model":"mutated-model"}', { mode: 0o600 });
  const sessionsBefore = sessionCounter;
  const outcome = await continueWithResumePlan(harness);
  const failed = outcome as unknown as Record<string, unknown>;
  expect(failed.ok).toBe(false);
  expect(failed.reason).toBe("profile_epoch_mismatch");
  expect(failed.exitCode).toBe(1);
  expect(sessionCounter - sessionsBefore).toBe(0);
  expect(stageWorkerCalls).toBe(0);
  expect(await readDurable(harness, RUN_ID)).toEqual(before);
  expect(runTreeFingerprint(harness, RUN_ID)).toBe(fingerprint);
});

test("epoch runner: a changed non-secret env value is a typed refusal with zero effects", async () => {
  const harness = await makeHarness("epoch-server-drift");
  await freshToPlanReady(harness);
  const before = await readDurable(harness, RUN_ID);
  const fingerprint = runTreeFingerprint(harness, RUN_ID);
  const sessionsBefore = sessionCounter;
  const outcome = await continueWithResumePlan(harness, {
    envOverrides: { LLM_SERVER: "https://other.example/v2" },
  });
  const failed = outcome as unknown as Record<string, unknown>;
  expect(failed.ok).toBe(false);
  expect(failed.reason).toBe("profile_epoch_mismatch");
  expect(sessionCounter - sessionsBefore).toBe(0);
  expect(stageWorkerCalls).toBe(0);
  expect(await readDurable(harness, RUN_ID)).toEqual(before);
  expect(runTreeFingerprint(harness, RUN_ID)).toBe(fingerprint);
});

test("epoch runner: a changed image digest reference is a typed refusal with zero effects", async () => {
  const harness = await makeHarness("epoch-image-drift");
  await freshToPlanReady(harness);
  const before = await readDurable(harness, RUN_ID);
  await writeProfile(harness.configRoot, {
    image: `ghcr.io/example/worker@sha256:${"f".repeat(64)}`,
  });
  const sessionsBefore = sessionCounter;
  const outcome = await continueWithResumePlan(harness);
  const failed = outcome as unknown as Record<string, unknown>;
  expect(failed.ok).toBe(false);
  expect(failed.reason).toBe("profile_epoch_mismatch");
  expect(sessionCounter - sessionsBefore).toBe(0);
  expect(stageWorkerCalls).toBe(0);
  expect(await readDurable(harness, RUN_ID)).toEqual(before);
});

// --- the allowed changes ------------------------------------------------------

test("epoch runner: a rotated secret value continues and the worker receives the new value", async () => {
  const harness = await makeHarness("epoch-rotation");
  await freshToPlanReady(harness);
  const epochBefore = await readFile(join(harness.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME), "utf8");
  const outcome = await continueWithResumePlan(harness, {
    envOverrides: { CODER_SOURCE_VAR_1: "rotated-secret-value" },
  });
  const state = expectSuccess(outcome);
  expect(state.status).toBe("success");
  const stage = workerObservations.find((observation) => observation.stateId === "dev_entry");
  expect(stage).toBeDefined();
  expect(stage!.env["CODER_API_KEY"]).toBe("rotated-secret-value");
  expect(await readFile(join(harness.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME), "utf8")).toBe(epochBefore);
});

test("epoch runner: an optional secret appearing or disappearing leaves the epoch unchanged", async () => {
  const harness = await makeHarness("epoch-optional");
  await freshToPlanReady(harness, { envOverrides: { OPT_SOURCE_VAR: undefined } });
  const epochBefore = await readFile(join(harness.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME), "utf8");
  // the optional secret binding is declared but never carries a value digest
  expect(epochBefore).toContain('"destination":"OPT_KEY"');
  const optionalEntry = (epochBefore.split('"destination":"OPT_SERVER"')[1] ?? "").split('{"destination"')[0] ?? "";
  expect(optionalEntry).not.toContain("value_sha256");
  let outcome = await continueWithResumePlan(harness, {
    envOverrides: { OPT_SOURCE_VAR: OPT_SECRET_VALUE },
  });
  expect(expectSuccess(outcome).status).toBe("success");
  const stageWith = workerObservations.find((observation) => observation.stateId === "dev_entry");
  expect(stageWith!.env["OPT_KEY"]).toBe(OPT_SECRET_VALUE);
  expect(await readFile(join(harness.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME), "utf8")).toBe(epochBefore);

  // the mirrored direction: present at creation, absent at continuation
  const harness2 = await makeHarness("epoch-optional-2");
  await freshToPlanReady(harness2, { envOverrides: { OPT_SOURCE_VAR: OPT_SECRET_VALUE } });
  const epoch2 = await readFile(join(harness2.runRoot(RUN_ID), PROFILE_EPOCH_FILE_NAME), "utf8");
  // the optional secret's presence is invisible to the epoch by construction
  expect(epoch2).toBe(epochBefore);
  outcome = await continueWithResumePlan(harness2, { envOverrides: { OPT_SOURCE_VAR: undefined } });
  expect(expectSuccess(outcome).status).toBe("success");
  const stageWithout = workerObservations.find((observation) => observation.stateId === "dev_entry");
  expect("OPT_KEY" in stageWithout!.env).toBe(false);
});

test("epoch runner: a relocated config root with identical content continues", async () => {
  const harness = await makeHarness("epoch-relocation");
  await freshToPlanReady(harness);
  const moved = join(harness.root, "config-moved");
  await rename(harness.configRoot, moved);
  const outcome = await continueWithResumePlan(harness, {}, moved);
  expect(expectSuccess(outcome).status).toBe("success");
});

test("epoch runner: a renamed config file with identical content continues", async () => {
  const harness = await makeHarness("epoch-rename");
  await freshToPlanReady(harness);
  rmSync(join(harness.configRoot, "opencode", "coder.json"));
  await writeProfile(harness.configRoot, { configName: "renamed.json" });
  const outcome = await continueWithResumePlan(harness);
  expect(expectSuccess(outcome).status).toBe("success");
});

test("epoch runner: an unused profile and a foreign file in the config root continue", async () => {
  const harness = await makeHarness("epoch-unused");
  await freshToPlanReady(harness);
  await writeProfile(harness.configRoot, { name: "unused" });
  await writeFile(join(harness.configRoot, "stray-operator-note.txt"), "not-a-profile\n", { mode: 0o600 });
  await writeFile(join(harness.configRoot, "opencode", "stray.json"), "{}", { mode: 0o600 });
  const outcome = await continueWithResumePlan(harness);
  expect(expectSuccess(outcome).status).toBe("success");
});
