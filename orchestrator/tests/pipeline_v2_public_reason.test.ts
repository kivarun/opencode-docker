import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  continuePipelineV2Stage,
  resumePipelineV2,
  resumePipelineV2PlanningRunPlan,
  revisePipelineV2Task,
  runPipelineV2,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import {
  isPipelineV2PublicReason,
  PIPELINE_V2_PUBLIC_REASONS,
  PipelineV2PublicReasonError,
  publicReasonFor,
  type PipelineV2PublicReason,
} from "../src/pipeline_v2_public_reason.ts";
import { PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import {
  parsePipelineV2RunState,
  reducePipelineV2RunCommand,
  PIPELINE_V2_FAILURE_REASONS,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { V2_IDENTITY, V2_INPUTS, tick } from "./pipeline_v2_state_fixtures.ts";
import type { PipelineV2ResumeRefusalReason } from "../src/pipeline_v2_coordinator.ts";

const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

/**
 * The shared machine-readable contract of every pipeline v2 command outcome:
 * `exitCode !== 0` implies an own `reason` property drawn from the one
 * closed public vocabulary; success, the controlled waiting suspension and
 * the controlled plan-ready suspension carry exit code 0 and no reason. The
 * single reason mapper translates typed internal failures by class and a
 * closed internal table — never by message text.
 */

const ROOTS: string[] = [];

async function makeRoot(prefix: string): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-reason-"));
  ROOTS.push(root);
  return root;
}

async function baseDeps(root: string, overrides: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
  const credentialFile = join(root, "cred");
  await writeFile(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
  return {
    cli: async () => ({ code: 1 }),
    fetchAuth: async () => ({
      status: 200,
      body: { authority: "launcher", principal: "p", launcher_id: "dhl_reason" },
    }),
    helperConfig: { socketPath: "/run/dh.sock", credentialFile },
    baseEnv: {},
    stateRootProjection: { localRoot: join(root, "state"), daemonRoot: join(root, "state") },
    ...overrides,
  };
}

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

/** The safe-id grammar of the vocabulary members (validated, never echoing). */
const SAFE_REASON = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

function expectOutcomeContract(outcome: PipelineV2RunOutcome): void {
  if (outcome.exitCode === 0) {
    expect(outcome.ok === true || (outcome as { waiting?: unknown }).waiting === true || (outcome as { planReady?: unknown }).planReady === true).toBe(true);
    expect("reason" in outcome && outcome.reason !== undefined).toBe(false);
    return;
  }
  expect(outcome.ok).toBe(false);
  const reason = (outcome as { reason?: unknown }).reason;
  expect(typeof reason).toBe("string");
  expect(isPipelineV2PublicReason(reason)).toBe(true);
  expect(SAFE_REASON.test(reason as string)).toBe(true);
  expect(Object.isFrozen(outcome)).toBe(true);
}

function expectFailureKeys(
  outcome: PipelineV2RunOutcome,
): { runId: string; runRoot: string | null; state: unknown; reason: PipelineV2PublicReason } {
  expectOutcomeContract(outcome);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "reason", "runId", "runRoot", "state"]);
  return outcome as {
    runId: string;
    runRoot: string | null;
    state: unknown;
    reason: PipelineV2PublicReason;
  };
}

// --- the vocabulary ----------------------------------------------------------

test("the public vocabulary is a closed, safe-identifier list", () => {
  expect(new Set(PIPELINE_V2_PUBLIC_REASONS).size).toBe((PIPELINE_V2_PUBLIC_REASONS as readonly string[]).length);
  for (const reason of PIPELINE_V2_PUBLIC_REASONS) {
    expect(SAFE_REASON.test(reason)).toBe(true);
    expect(isPipelineV2PublicReason(reason)).toBe(true);
  }
  expect(isPipelineV2PublicReason("not_a_reason")).toBe(false);
  expect(isPipelineV2PublicReason("")).toBe(false);
  expect(isPipelineV2PublicReason(42)).toBe(false);
});

test("every already-public reason family is contained byte-for-byte", () => {
  for (const reason of PIPELINE_V2_FAILURE_REASONS) {
    expect(isPipelineV2PublicReason(reason)).toBe(true);
  }
  const refusalReasons: readonly PipelineV2ResumeRefusalReason[] = [
    "missing_state",
    "sink_poisoned",
    "run_id_mismatch",
    "invalid_state",
    "pipeline_mismatch",
    "run_layout_invalid",
    "run_input_modified",
    "accepted_output_modified",
    "internal_error",
  ];
  for (const reason of refusalReasons) {
    expect(isPipelineV2PublicReason(reason)).toBe(true);
  }
  expect(isPipelineV2PublicReason("run_already_exists")).toBe(true);
  expect(isPipelineV2PublicReason("run_cleanup_failed")).toBe(true);
});

// --- the one reason mapper ----------------------------------------------------

test("the mapper maps typed classes and the closed internal table", () => {
  expect(publicReasonFor(new PipelineV2PublicReasonError("run_not_found", "m"), "invalid_options")).toBe("run_not_found");
  expect(publicReasonFor({ reason: "invalid_state" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "invalid_result" }, "internal_error")).toBe("invalid_result");
  // internal controller reasons translate through the explicit table only
  expect(publicReasonFor({ reason: "lifecycle_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "grant_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "intent_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "candidate_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "revision_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "plan_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "wait_conflict" }, "internal_error")).toBe("invalid_state");
  expect(publicReasonFor({ reason: "invalid_intent" }, "internal_error")).toBe("invalid_options");
  expect(publicReasonFor({ reason: "invalid_request" }, "internal_error")).toBe("invalid_options");
  expect(publicReasonFor({ reason: "invalid_response" }, "internal_error")).toBe("invalid_options");
  expect(publicReasonFor({ reason: "invalid_stage_id" }, "internal_error")).toBe("invalid_options");
  expect(publicReasonFor({ reason: "stage_not_found" }, "internal_error")).toBe("invalid_options");
  expect(publicReasonFor({ reason: "wait_storage_failed" }, "internal_error")).toBe("state_persist_failed");
  expect(publicReasonFor({ reason: "artifact_mismatch" }, "internal_error")).toBe("artifact_modified");
  expect(publicReasonFor({ reason: "artifact_missing" }, "internal_error")).toBe("artifact_missing");
  // unknown internal reasons never leave the boundary
  expect(publicReasonFor({ reason: "totally_internal_reason" }, "internal_error")).toBe("internal_error");
  expect(publicReasonFor({ reason: "totally_internal_reason" }, "invalid_options")).toBe("invalid_options");
  // untyped causes fall back
  expect(publicReasonFor(new Error("plain"), "invalid_options")).toBe("invalid_options");
  expect(publicReasonFor("a string", "internal_error")).toBe("internal_error");
  expect(publicReasonFor(undefined, "internal_error")).toBe("internal_error");
  expect(publicReasonFor(new PipelineV2RunStateStoreError("store"), "internal_error")).toBe("state_persist_failed");
  // hostile error objects never break the mapper
  const hostile = new Proxy({}, {
    get() {
      throw new Error("getter exploded");
    },
  });
  expect(publicReasonFor(hostile, "internal_error")).toBe("internal_error");
  const throwingGetter = { get reason(): string {
    throw new Error("reason getter exploded");
  } };
  expect(publicReasonFor(throwingGetter, "invalid_options")).toBe("invalid_options");
  // an injected canary never reaches a reason or a diagnostic through it
  expect(publicReasonFor({ reason: "SECRET-CANARY" }, "internal_error")).toBe("internal_error");
});

// --- the runner boundary outcomes ---------------------------------------------

const STAGE_BUNDLE = [
  "schema_version: 2",
  "entry_state: coder",
  "max_transitions: 20",
  "inputs: []",
  "outputs: []",
  "orchestration:",
  "  stage_templates: []",
  "  execution_roles:",
  "    - state_id: coder",
  "      role: planning",
  "      plan_output: plan",
  "      stage_wait:",
  "        reason: stage_iteration_completed",
  "        actions:",
  "          - continue_stage",
  "          - revise_task",
  "states:",
  "  - id: coder",
  "    type: agent",
  "    profile: coder",
  "    prompt: prompts/coder.md",
  "    inputs: []",
  "    outputs:",
  "      - id: plan",
  "        type: json",
  "        schema: schemas/plan.schema.json",
  "    timeout_seconds: 60",
  "    max_attempts: 1",
  "    transitions:",
  "      - outcome: completed",
  "        to: done",
  "  - id: done",
  "    type: terminal",
  "    result: success",
  "",
].join("\n");

async function writeBundle(root: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), STAGE_BUNDLE);
  await writeFile(join(bundle, "prompts", "coder.md"), "x\n");
  await writeFile(join(bundle, "schemas", "plan.schema.json"), "{}");
  const configRoot = join(root, "config");
  await mkdir(join(configRoot, "profiles"), { recursive: true });
  await mkdir(join(configRoot, "opencode"), { recursive: true });
  await writeFile(
    join(configRoot, "profiles", "coder.yaml"),
    "schema_version: 1\nimage: ghcr.io/example/worker:1\nopencode_config: opencode/coder.json\nenv: {}\n",
  );
  await writeFile(join(configRoot, "opencode", "coder.json"), "{}");
  const project = join(root, "project");
  await mkdir(project, { recursive: true });
  return bundle;
}

async function writeActiveRun(root: string, runId: string, bundleRoot: string): Promise<string> {
  const stateRoot = join(root, "state");
  const runRoot = join(stateRoot, "pipeline-runs", runId);
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  const identity = { ...V2_IDENTITY, bundle_root: bundleRoot };
  const state: PipelineV2RunState = reducePipelineV2RunCommand(
    null,
    { kind: "create_run", runId, pipeline: identity, inputs: V2_INPUTS, projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
    tick(1),
  );
  await writeFile(join(runRoot, "state.json"), JSON.stringify(state), { mode: 0o600 });
  return runRoot;
}

test("run: the preflight failure carries invalid_options with the exact keys", async () => {
  const root = await makeRoot("preflight-");
  const outcome = await runPipelineV2(
    {
      pipelineRoot: join(root, "missing-bundle"),
      configRoot: join(root, "config"),
      projectSourcePath: join(root, "project"),
      inputBindings: [],
    },
    (await baseDeps(root)) as never,
  );
  const failed = expectFailureKeys(outcome);
  expect(failed.reason).toBe("invalid_options");
  expect(failed.runId).toBe("");
  expect(failed.runRoot).toBeNull();
});

test("run: the collision carries run_already_exists", async () => {
  const root = await makeRoot("collision-");
  const bundle = await writeBundle(root);
  const stateRoot = join(root, "state");
  await mkdir(join(stateRoot, "pipeline-runs", "occupied"), { recursive: true, mode: 0o700 });
  const outcome = await runPipelineV2(
    {
      pipelineRoot: bundle,
      configRoot: join(root, "config"),
      projectSourcePath: join(root, "project"),
      inputBindings: [],
      runId: "occupied",
    },
    (await baseDeps(root)) as never,
  );
  const failed = expectFailureKeys(outcome);
  expect(failed.reason).toBe("run_already_exists");
  expect(failed.runId).toBe("occupied");
  expect(failed.state).toBeNull();
});

test("resume: an unknown run carries run_not_found; a damaged state document carries invalid_state", async () => {
  const root = await makeRoot("resume-");
  await mkdir(join(root, "state"), { recursive: true });
  const missing = expectFailureKeys(
    await resumePipelineV2(
      { runId: "no-such-run", configRoot: join(root, "config") },
      (await baseDeps(root)) as never,
    ),
  );
  expect(missing.reason).toBe("run_not_found");
  const runRoot = join(root, "state", "pipeline-runs", "damaged-run");
  await mkdir(runRoot, { recursive: true, mode: 0o700 });
  await writeFile(join(runRoot, "state.json"), "{not json", { mode: 0o600 });
  const damaged = expectFailureKeys(
    await resumePipelineV2(
      { runId: "damaged-run", configRoot: join(root, "config") },
      (await baseDeps(root)) as never,
    ),
  );
  expect(damaged.reason).toBe("invalid_state");
  expect(damaged.runId).toBe("damaged-run");
});

test("continue-stage and revise-task: a stale progressed retry carries invalid_state", async () => {
  const root = await makeRoot("stale-");
  await writeActiveRun(root, "stale-continue", join(root, "unused-bundle"));
  const continueOutcome = await continuePipelineV2Stage(
    {
      runId: "stale-continue",
      waitIndex: 1,
      additionalIterations: 1,
      configRoot: join(root, "config"),
    },
    (await baseDeps(root)) as never,
  );
  const continueFailed = expectFailureKeys(continueOutcome);
  expect(continueFailed.reason).toBe("invalid_state");
  expect(continueFailed.state).not.toBeNull();
  await writeActiveRun(root, "stale-revise", join(root, "unused-bundle"));
  const reviseOutcome = await revisePipelineV2Task(
    {
      runId: "stale-revise",
      waitIndex: 1,
      taskId: "task-1",
      taskBody: "body",
      configRoot: join(root, "config"),
    },
    (await baseDeps(root)) as never,
  );
  const reviseFailed = expectFailureKeys(reviseOutcome);
  expect(reviseFailed.reason).toBe("invalid_state");
  expect(reviseFailed.state).not.toBeNull();
});

test("resume-plan: the typed lifecycle refusal carries invalid_state", async () => {
  const root = await makeRoot("plan-");
  const bundle = await writeBundle(root);
  await writeActiveRun(root, "stale-plan", bundle);
  const outcome = await resumePipelineV2PlanningRunPlan(
    {
      runId: "stale-plan",
      stageId: "stage-1",
      initialBudget: 1,
      configRoot: join(root, "config"),
      launcherId: "dhl_reason",
    },
    (await baseDeps(root)) as never,
  );
  const failed = expectFailureKeys(outcome);
  expect(failed.reason).toBe("invalid_state");
  expect(failed.runId).toBe("stale-plan");
});

test("an unexpected post-claim failure carries a machine-readable reason", async () => {
  const root = await makeRoot("internal-");
  const bundle = await writeBundle(root);
  await writeActiveRun(root, "clock-explosion", bundle);
  let clockCalls = 0;
  const deps = await baseDeps(root);
  deps.now = (): Date => {
    clockCalls += 1;
    if (clockCalls > 1) {
      throw new TypeError("the clock exploded");
    }
    return tick(clockCalls);
  };
  const outcome = await resumePipelineV2PlanningRunPlan(
    {
      runId: "clock-explosion",
      stageId: "stage-1",
      initialBudget: 1,
      configRoot: join(root, "config"),
      launcherId: "dhl_reason",
    },
    deps as never,
  );
  const failed = expectFailureKeys(outcome);
  // the composed controller owns the classification of the injected failure;
  // the contract only demands a machine-readable closed-vocabulary reason
  expect(isPipelineV2PublicReason(failed.reason)).toBe(true);
  expect(failed.runId).toBe("clock-explosion");
});

// The controlled plan-ready and waiting suspensions keep exit 0 with no
// reason: their exact own-key shapes (and the absence of `reason`) are
// pinned by the existing plan-ready and wait-entry suites
// (tests/pipeline_v2_plan_ready.test.ts, tests/pipeline_v2_plan_ready_auto.test.ts,
// tests/pipeline_v2_wait_entry.test.ts) and the exact-key pins of the CLI
// JSON contract in tests/cli_main.test.ts.

// --- the source scans ----------------------------------------------------------

test("the runner classifies every failure through the one mapper; no message parsing", async () => {
  const runner = await readFile(new URL("../src/pipeline_v2_runner.ts", import.meta.url), "utf8");
  const reasonModule = await readFile(
    new URL("../src/pipeline_v2_public_reason.ts", import.meta.url),
    "utf8",
  );
  // exactly one mapper and one exported vocabulary
  expect(reasonModule.split("export function publicReasonFor").length - 1).toBe(1);
  expect(reasonModule.split("export const PIPELINE_V2_PUBLIC_REASONS").length - 1).toBe(1);
  expect(reasonModule.split("export const PIPELINE_V2_PUBLIC_REASONS").length - 1).toBe(1);
  // classification never reads message text or uses substring/regex matching
  expect(reasonModule).not.toContain(".includes(");
  expect(reasonModule).not.toContain("RegExp");
  expect(reasonModule).not.toContain(".match(");
  expect(reasonModule).not.toContain(".test(");
  // the runner never classifies by message; every failure site goes through
  // the mapper (the diagnostic messages themselves are unchanged stderr text)
  expect(runner).toContain('publicReasonFor(cause, "invalid_options")');
  expect(runner).toContain('publicReasonFor(cause, "internal_error")');
  expect(runner).toContain('publicReasonFor(cause, "invalid_state")');
  expect(runner).not.toContain(".message.includes");
  expect(runner).not.toContain(".includes(");
  // the CLI never re-classifies: the reporter only serializes the outcome
  const main = await readFile(new URL("../src/main.ts", import.meta.url), "utf8");
  expect(main).not.toContain("publicReasonFor");
  expect(main).not.toContain("PIPELINE_V2_PUBLIC_REASONS");
  // the reducer, the store and the state schema are untouched by the mapping
  expect(reasonModule).not.toContain("reducePipelineV2RunCommand");
  expect(reasonModule).not.toContain("writeFile");
  expect(reasonModule).not.toContain("mkdir");
});

test("the durable state of a reason-mapped failure never carries the public reason machinery", async () => {
  const root = await makeRoot("durable-");
  await writeActiveRun(root, "stale-continue", join(root, "unused-bundle"));
  await continuePipelineV2Stage(
    {
      runId: "stale-continue",
      waitIndex: 1,
      additionalIterations: 1,
      configRoot: join(root, "config"),
    },
    (await baseDeps(root)) as never,
  );
  const state = parsePipelineV2RunState(
    await readFile(join(root, "state", "pipeline-runs", "stale-continue", "state.json"), "utf8"),
  );
  // the stale retry wrote nothing: the state is exactly the create_run revision
  expect(state.revision).toBe(1);
  expect(state.failure).toBeUndefined();
  expect(JSON.stringify(state)).not.toContain("invalid_state");
});
