/**
 * The separate development pipeline bundle (`pipelines/development-v1`):
 * planner -> implementer -> reviewer -> deterministic review gate, with the
 * relay state that carries the changes-requested boundary into the trusted
 * stage wait. This suite pins the bundle's compiled contracts and drives the
 * full operator-facing chain (run, continue-stage, revise-task, restart
 * seams) through the public CLI over the deterministic test transport.
 *
 * Local test deadline policy: the E2E members build multi-command durable
 * reducer histories (dozens of fsync-bound commits) under
 * `bun test --parallel=2`; each carries an explicit local 15000ms deadline
 * like the bundled-default suite.
 */
import { expect, test } from "bun:test";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { runCli, type CliIo } from "../src/main.ts";
import type { CliResult } from "../src/docker_helper.ts";
import { runPipelineV2, resumePipelineV2, continuePipelineV2Stage, revisePipelineV2Task } from "../src/pipeline_v2_runner.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { evaluatePipelineDecisionState } from "../src/pipeline_v2.ts";
import { pipelineV2ExecutionDigest, pipelineV2ExecutionSnapshotJson, pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  compiledExecutionRoleFor,
  compiledStageTemplateFor,
  compiledWorkspaceAccessFor,
} from "../src/pipeline_v2_orchestration.ts";
import { compiledTransitionFor } from "../src/pipeline_engine.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateStoreError } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import {
  coordinatePipelineV2Run,
  type PipelineV2AgentRuntime,
  type PipelineV2CoordinatorControl,
} from "../src/pipeline_v2_coordinator.ts";
import { applyPipelineV2ReviseTaskIntervention } from "../src/pipeline_v2_revise_task_intervention_controller.ts";
import { applyPipelineV2PlanReadyContinuation } from "../src/pipeline_v2_plan_ready_auto_controller.ts";
import { prepareAndPublishRunProfileEpoch } from "./helpers/profile_epoch_fixture.ts";

const REPO_ROOT = join(import.meta.dir, "..", "..");
const DEV_BUNDLE = join(REPO_ROOT, "pipelines", "development-v1");
const V2_BUNDLE = join(REPO_ROOT, "pipelines", "default-v2");
const LAUNCHER_ID = "dhl_devv1";

const TASK_BODY = "DEV-V1-TASK\n";
const REVISED_BODY = "DEV-V1-TASK-REVISED\n";

// ---------------------------------------------------------------------------
// Loader and digest contracts of the tracked bundle.
// ---------------------------------------------------------------------------

test("the development-v1 bundle loads, digests deterministically, and stays portable", async () => {
  expect(existsSync(join(DEV_BUNDLE, "pipeline.yaml"))).toBe(true);
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  const again = await loadPipelineV2(DEV_BUNDLE);
  expect(pipelineV2ExecutionDigest(pipeline)).toBe(pipelineV2ExecutionDigest(again));
  expect(pipelineV2ExecutionSnapshotJson(pipeline)).toBe(pipelineV2ExecutionSnapshotJson(again));

  const copyRoot = mkdtempSync(join(tmpdir(), "dev-v1-port-"));
  try {
    const moved = join(copyRoot, "development-v1");
    const copied = spawnSync("cp", ["-a", `${DEV_BUNDLE}/.`, moved]);
    expect(copied.status).toBe(0);
    const second = await loadPipelineV2(moved);
    expect(pipelineV2ExecutionDigest(second)).toBe(pipelineV2ExecutionDigest(pipeline));
    expect(second.bundleRoot).toBe(moved);
    expect(pipelineV2RunPipelineIdentity(second).bundle_root).toBe(moved);
  } finally {
    rmSync(copyRoot, { recursive: true, force: true });
  }
}, 15000);

test("the exact state graph and normalized orchestration metadata are compiled", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  expect(pipeline.states.map((state) => state.id)).toEqual([
    "plan",
    "implement",
    "review",
    "review_gate",
    "relay",
    "replan",
    "done",
    "failed",
  ]);
  expect(pipeline.entry_state).toBe("plan");
  expect(pipeline.max_transitions).toBe(64);
  expect(compiledStageTemplateFor(pipeline, "development")).toEqual({
    id: "development",
    entry_state: "implement",
    state_ids: ["implement", "relay", "review", "review_gate"],
  });
  // execution roles are normalized sorted by state id; the planning entries
  // carry the exact trusted policies; the stage entries carry the template
  // and (only for review and relay) the explicit read-only workspace access.
  expect(pipeline.orchestration?.execution_roles).toEqual([
    { state_id: "implement", role: "stage", stage_template: "development" },
    {
      state_id: "plan",
      role: "planning",
      plan_output: "plan",
      stage_wait: { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] },
      plan_ready: { stage_position: 1, initial_budget: 1 },
    },
    { state_id: "relay", role: "stage", stage_template: "development", workspace_access: "read_only" },
    {
      state_id: "replan",
      role: "planning",
      plan_output: "plan",
      stage_wait: { reason: "stage_iteration_completed", actions: ["continue_stage", "revise_task"] },
      plan_ready: { stage_position: 1, initial_budget: 1 },
    },
    { state_id: "review", role: "stage", stage_template: "development", workspace_access: "read_only" },
    { state_id: "review_gate", role: "stage", stage_template: "development" },
  ]);
  expect(pipeline.orchestration?.stage_templates).toEqual([{ id: "development", entry_state: "implement" }]);
  expect(compiledExecutionRoleFor(pipeline, "plan").role).toBe("planning");
  expect(compiledExecutionRoleFor(pipeline, "replan").role).toBe("planning");
  for (const stateId of ["implement", "review", "review_gate", "relay"]) {
    expect(compiledExecutionRoleFor(pipeline, stateId).role).toBe("stage");
  }
}, 15000);

test("the bundle references exactly the three semantic profiles and ships no profile definitions", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  const agentStates = pipeline.states.filter((state) => state.type === "agent");
  expect(agentStates.map((state) => (state as { profile: string }).profile)).toEqual([
    "architect",
    "code",
    "review",
    "review",
    "architect",
  ]);
  // no profile bodies anywhere in the bundle: the deployment config root is
  // the only profile authority
  expect(existsSync(join(DEV_BUNDLE, "profiles"))).toBe(false);
  const tree: string[] = [];
  const walk = (dir: string): void => {
    for (const entry of readdirSync(dir).sort()) {
      const path = join(dir, entry);
      if (existsSync(path) && readdirSyncSafe(path)) {
        walk(path);
      } else {
        tree.push(path.slice(DEV_BUNDLE.length + 1));
      }
    }
  };
  walk(DEV_BUNDLE);
  expect(tree.sort()).toEqual([
    "decisions/review.yaml",
    "pipeline.yaml",
    "prompts/implement.md",
    "prompts/plan-initial.md",
    "prompts/plan-revise.md",
    "prompts/relay.md",
    "prompts/review.md",
    "schemas/agent-result.schema.json",
    "schemas/plan.schema.json",
    "schemas/review-feedback.schema.json",
    "schemas/review-verdict.schema.json",
  ]);
}, 15000);

test("the data ports match the development contract exactly", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  const byId = new Map(pipeline.states.map((state) => [state.id, state]));
  const plan = byId.get("plan");
  const implement = byId.get("implement");
  const review = byId.get("review");
  const gate = byId.get("review_gate");
  const relay = byId.get("relay");
  if (
    plan === undefined ||
    implement === undefined ||
    review === undefined ||
    gate === undefined ||
    relay === undefined ||
    plan.type !== "agent" ||
    implement.type !== "agent" ||
    review.type !== "agent" ||
    gate.type !== "decision" ||
    relay.type !== "agent"
  ) {
    throw new Error("unreachable");
  }
  // the initial planner reads the protected task input and emits the proposal
  expect(plan.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "task", source: { pipeline_input: "task" } },
  ]);
  expect(plan.outputs.map((output) => ({ id: output.id, type: output.type }))).toEqual([{ id: "plan", type: "json" }]);
  // the implementer receives the exact accepted task bytes through the
  // stage-task port and emits the agent result
  expect(implement.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "task", source: { stage_task: {} } },
  ]);
  expect(implement.outputs.map((output) => ({ id: output.id, type: output.type }))).toEqual([
    { id: "result", type: "json" },
  ]);
  // the reviewer reads the stage task bytes and the implementer's accepted
  // result, and emits the separate verdict and feedback documents
  expect(review.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "task", source: { stage_task: {} } },
    { id: "implementation_result", source: { state_output: { state: "implement", output: "result" } } },
  ]);
  expect(review.outputs.map((output) => ({ id: output.id, type: output.type }))).toEqual([
    { id: "verdict", type: "json" },
    { id: "feedback", type: "json" },
  ]);
  // the gate consumes exactly the verdict fact assignment
  expect(gate.inputs.map((input) => ({ id: input.id, source: input.source }))).toEqual([
    { id: "verdict", source: { state_output: { state: "review", output: "verdict" } } },
  ]);
  // the relay is zero I/O by declaration
  expect(relay.inputs).toEqual([]);
  expect(relay.outputs).toEqual([]);
  // the run input is the protected task file; the run output is the
  // reviewer's feedback
  expect(pipeline.inputs).toEqual([{ id: "task", type: "file", protected: true }]);
  expect(pipeline.outputs.map((output) => ({ id: output.id, required: output.required, source: output.source }))).toEqual([
    { id: "review", required: true, source: { state_output: { state: "review", output: "feedback" } } },
  ]);
}, 15000);

test("the review gate decision model compiles with the exact exhaustive transition table", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  const gate = pipeline.states.find((state) => state.id === "review_gate");
  if (gate === undefined || gate.type !== "decision") {
    throw new Error("unreachable");
  }
  expect(gate.transitions.map((transition) => [transition.outcome, transition.to])).toEqual([
    ["accepted", "done"],
    ["changes_requested", "relay"],
    ["uncovered", "failed"],
    ["inconsistent_facts", "failed"],
    ["invalid_facts", "failed"],
  ]);
  expect(gate.model.factIds).toEqual(["accepted"]);
  expect(gate.model.decisionIds).toEqual(["accepted", "changes_requested"]);
  expect(gate.model.rules.map((rule) => [rule.id, gate.model.decisionIds[rule.decisionIndex]])).toEqual([
    ["accept", "accepted"],
    ["request_changes", "changes_requested"],
  ]);
  expect(gate.model.relations).toEqual([]);
  expect(gate.model.constraints).toEqual([]);
}, 15000);

test("reserved decision outcomes route to the failed terminal through the engine resolver; the honest evaluator classifies a missing fact", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  // the engine's own transition resolution owns the routing: every decision
  // id and every reserved outcome resolves exactly as declared
  expect(compiledTransitionFor(pipeline, "review_gate", "accepted")).toEqual({
    from: "review_gate",
    outcome: "accepted",
    to: "done",
    transition_index: 0,
  });
  expect(compiledTransitionFor(pipeline, "review_gate", "changes_requested")).toEqual({
    from: "review_gate",
    outcome: "changes_requested",
    to: "relay",
    transition_index: 1,
  });
  for (const reserved of ["uncovered", "inconsistent_facts", "invalid_facts"]) {
    expect(compiledTransitionFor(pipeline, "review_gate", reserved), reserved).toEqual({
      from: "review_gate",
      outcome: reserved,
      to: "failed",
      transition_index: reserved === "uncovered" ? 2 : reserved === "inconsistent_facts" ? 3 : 4,
    });
  }
  // the honest evaluator: the exact boolean assignments select with the
  // declared rule ids; a missing fact is the structured invalid_facts result
  const selected = evaluatePipelineDecisionState(pipeline, "review_gate", { accepted: true });
  expect(selected).toMatchObject({ status: "selected", outcome: "accepted", rule_id: "accept" });
  const rejected = evaluatePipelineDecisionState(pipeline, "review_gate", { accepted: false });
  expect(rejected).toMatchObject({ status: "selected", outcome: "changes_requested", rule_id: "request_changes" });
  const invalid = evaluatePipelineDecisionState(pipeline, "review_gate", {});
  expect(invalid).toEqual({
    status: "invalid_facts",
    reason: "missing_fact",
    fact_id: "accepted",
    state_id: "review_gate",
    outcome: "invalid_facts",
  });
}, 15000);

test("workspace access policies: implement read-write, review and relay read-only, review_gate rejected by the agent-only resolver", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  expect(compiledWorkspaceAccessFor(pipeline, "implement")).toBe("read_write");
  expect(compiledWorkspaceAccessFor(pipeline, "review")).toBe("read_only");
  expect(compiledWorkspaceAccessFor(pipeline, "relay")).toBe("read_only");
  let error = "";
  try {
    compiledWorkspaceAccessFor(pipeline, "review_gate");
  } catch (cause) {
    error = cause instanceof Error ? cause.message : String(cause);
  }
  expect(error).toContain('state "review_gate" is not a declared agent state');
}, 15000);

test("digest sensitivity: the trusted policies are part of the execution identity", async () => {
  const pipeline = await loadPipelineV2(DEV_BUNDLE);
  const baseline = await pipelineV2ExecutionDigest(pipeline);
  const copyRoot = mkdtempSync(join(tmpdir(), "dev-v1-sens-"));
  const probes: Array<{ name: string; from: string; to: string }> = [
    {
      name: "stage-wait action order",
      from: "          - continue_stage\n          - revise_task",
      to: "          - revise_task\n          - continue_stage",
    },
    { name: "workspace access value", from: "      workspace_access: read_only\n    - state_id: replan", to: "      workspace_access: read_write\n    - state_id: replan" },
    { name: "plan-ready budget", from: "        initial_budget: 1", to: "        initial_budget: 2" },
  ];
  try {
    for (const probe of probes) {
      const moved = join(copyRoot, probe.name.replace(/\W+/g, "-"));
      const copied = spawnSync("cp", ["-a", `${DEV_BUNDLE}/.`, moved]);
      expect(copied.status).toBe(0);
      const yamlPath = join(moved, "pipeline.yaml");
      const text = await readFile(yamlPath, "utf8");
      expect(text.includes(probe.from), probe.name).toBe(true);
      await writeFile(yamlPath, text.replace(probe.from, probe.to));
      const mutated = await loadPipelineV2(moved);
      expect(await pipelineV2ExecutionDigest(mutated), probe.name).not.toBe(baseline);
    }
  } finally {
    rmSync(copyRoot, { recursive: true, force: true });
  }
}, 15000);

test("the default-v2 bundle stays byte-identical and the development bundle is decision-state-aware", async () => {
  // the separate bundle must not have drifted the bundled default: the
  // default's execution snapshot digest is pinned byte-stable here
  const def = await loadPipelineV2(V2_BUNDLE);
  expect(await pipelineV2ExecutionDigest(def)).toBe(
    "41ce957320f7a0a2ca60bd0469cac05dfb28b5b629da010b17eddf8052c6b5b9",
  );
  // the v1 loader still rejects the new bundle's schema version like any v2
  // document (no silent swap)
  const { loadPipeline } = await import("../src/pipeline.ts");
  let v1Error = "";
  try {
    await loadPipeline(DEV_BUNDLE);
  } catch (cause) {
    v1Error = cause instanceof Error ? cause.message : String(cause);
  }
  expect(v1Error).toBe("pipeline schema version 2 is not executable yet");
}, 15000);

// ---------------------------------------------------------------------------
// Shared E2E harness: the deterministic test transport over the public CLI.
// ---------------------------------------------------------------------------

const RESULT_BODY = {
  schema_version: 3,
  status: "completed",
  summary: "DEV-V1-WORK-COMPLETE",
  artifacts: ["work/product.txt"],
};
const RESULT_BYTES = `${JSON.stringify(RESULT_BODY)}\n`;

const FEEDBACK_1 = {
  summary: "REVIEW-SUMMARY-1",
  issues: [{ path: "work/product.txt", severity: "major", description: "REVIEW-ISSUE-1" }],
};
const FEEDBACK_2 = {
  summary: "REVIEW-SUMMARY-2",
  issues: [{ path: "work/product.txt", severity: "minor", description: "REVIEW-ISSUE-2" }],
};

interface ReviewBehavior {
  readonly accepted: boolean;
  readonly feedback: Record<string, unknown>;
}

interface RecordedBody {
  readonly stateId: string;
  readonly body: Record<string, unknown>;
}

interface RecordedActivationInputs {
  readonly stateId: string;
  readonly stageTask: string | null;
  readonly implementationResult: string | null;
}

interface ChainSetup {
  stateRoot: string;
  sessionIds: string[];
  sessionCreates: Array<{ args: string[]; roots: string[]; workspace: string }>;
  sessionDeletes: string[];
  runBodies: RecordedBody[];
  activationInputs: RecordedActivationInputs[];
  io: CliIo;
}

function proposalFor(taskBody: string, replan: boolean): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-1", depends_on: [] }] }],
    new_tasks: replan ? [] : [{ id: "task-1", body: taskBody }],
  };
}

let sessionCounter = 0;

function makeChain(stateRoot: string, reviews: readonly ReviewBehavior[], credFile: string): ChainSetup {
  const sessionIds: string[] = [];
  const sessionCreates: ChainSetup["sessionCreates"] = [];
  const sessionDeletes: string[] = [];
  const runBodies: RecordedBody[] = [];
  const activationInputs: RecordedActivationInputs[] = [];
  const reviewQueue = [...reviews];
  let workspace = "";
  const io: CliIo = {
    baseEnv: { HOME: "/home/u", DEFAULT_SOURCE_VAR_1: "dev-secret" },
    runner: {
      run: async (args: string[]): Promise<CliResult> => {
        if (args[0] === "session" && args[1] === "create") {
          sessionCounter += 1;
          const id = `dhs_devv1_${sessionCounter}`;
          const roots: string[] = [];
          for (let index = 0; index < args.length; index += 1) {
            if (args[index] === "--filesystem-root") {
              roots.push(args[index + 1] ?? "");
            }
          }
          sessionCreates.push({ args, roots, workspace: args[args.length - 1] ?? "" });
          sessionIds.push(id);
          if (workspace === "") {
            workspace = args[args.length - 1] ?? "";
          }
          return {
            code: 0,
            stdout: JSON.stringify({
              ok: true,
              session: { id, launcher_id: LAUNCHER_ID },
              token: `dhc_${id}`,
            }),
          };
        }
        if (args[0] === "session" && args[1] === "delete") {
          sessionDeletes.push(args[args.length - 1] ?? "");
          return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
        }
        if (args[0] === "pull") {
          return { code: 0 };
        }
        return { code: 1 };
      },
      killActive: () => false,
    },
    operationRunner: async (request: { body: string }) => {
      const body = JSON.parse(request.body) as {
        mounts?: Array<{ source?: string; target?: string; read_only?: boolean }>;
      };
      const mounts = body.mounts ?? [];
      const outputsMount = mounts.find((mount) => mount.target === "/pipeline/outputs");
      const inputsMount = mounts.find((mount) => mount.target === "/pipeline/inputs");
      const projectMount = mounts.find((mount) => mount.target === "/workspace");
      if (outputsMount === undefined || typeof outputsMount.source !== "string" || workspace === "") {
        return {
          outcome: "completed" as const,
          capture: { status: "unavailable" as const, byteCount: 0, tailBytes: new Uint8Array(0), truncatedDaemon: false, truncatedLocal: false },
        };
      }
      const segments = outputsMount.source.split("/");
      const dash = segments[1] ?? "";
      const stateId = dash.slice(dash.indexOf("-") + 1);
      const outputsDir = join(workspace, outputsMount.source);
      const inputsDir = inputsMount !== undefined && typeof inputsMount.source === "string"
        ? join(workspace, inputsMount.source)
        : "";
      let stageTask: string | null = null;
      let implementationResult: string | null = null;
      if (inputsDir !== "") {
        try {
          stageTask = await readFile(join(inputsDir, "task"), "utf8");
        } catch {
          stageTask = null;
        }
        try {
          implementationResult = await readFile(join(inputsDir, "implementation_result"), "utf8");
        } catch {
          implementationResult = null;
        }
      }
      activationInputs.push({ stateId, stageTask, implementationResult });
      runBodies.push({ stateId, body });
      if (stateId === "plan") {
        writeFileSync(join(outputsDir, "plan"), JSON.stringify(proposalFor(stageTask ?? "", false)), { mode: 0o600 });
      } else if (stateId === "replan") {
        writeFileSync(join(outputsDir, "plan"), JSON.stringify(proposalFor(stageTask ?? "", true)), { mode: 0o600 });
      } else if (stateId === "implement") {
        writeFileSync(join(outputsDir, "result"), RESULT_BYTES, { mode: 0o600 });
      } else if (stateId === "review") {
        const behavior = reviewQueue.shift();
        if (behavior === undefined) {
          throw new Error(`no scripted review behavior left for a review activation`);
        }
        writeFileSync(join(outputsDir, "verdict"), JSON.stringify({ accepted: behavior.accepted }), { mode: 0o600 });
        writeFileSync(join(outputsDir, "feedback"), JSON.stringify(behavior.feedback), { mode: 0o600 });
      }
      // relay: writes nothing; the declared outputs tree must stay empty
      return {
        outcome: "completed" as const,
        capture: { status: "unavailable" as const, byteCount: 0, tailBytes: new Uint8Array(0), truncatedDaemon: false, truncatedLocal: false },
      };
    },
    fetchAuth: () =>
      Promise.resolve({ status: 200, body: { authority: "launcher", principal: "proof", launcher_id: LAUNCHER_ID } }),
    resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: credFile }),
    resolveStateRootProjection: () => ({ localRoot: stateRoot, daemonRoot: stateRoot }),
    runSmoke: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runAgentSmoke"],
    runPipelineV2: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["runPipelineV2"],
    resumePipelineV2: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["resumePipelineV2"],
    continuePipelineV2Stage: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["continuePipelineV2Stage"],
    revisePipelineV2Task: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["revisePipelineV2Task"],
    resumePipelineV2PlanningRunPlan: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["resumePipelineV2PlanningRunPlan"],
    identityPipelineV2Run: (async () => {
      throw new Error("fake identityPipelineV2Run not configured");
    }) as unknown as CliIo["identityPipelineV2Run"],
    inspectPipelineV2Run: (async () => {
      throw new Error("fake inspectPipelineV2Run not configured");
    }) as unknown as CliIo["inspectPipelineV2Run"],
    eventsPipelineV2Run: (async () => {
      throw new Error("fake eventsPipelineV2Run not configured");
    }) as unknown as CliIo["eventsPipelineV2Run"],
    logsPipelineV2Run: (async () => {
      throw new Error("fake logsPipelineV2Run not configured");
    }) as unknown as CliIo["logsPipelineV2Run"],
    projectChangeContentPipelineV2Run: (async () => {
      throw new Error("fake projectChangeContentPipelineV2Run not configured");
    }) as unknown as CliIo["projectChangeContentPipelineV2Run"],
    projectApplyPipelineV2: (async () => {
      throw new Error("fake projectApplyPipelineV2Run not configured");
    }) as unknown as CliIo["projectApplyPipelineV2"],
    projectApplyInspectPipelineV2: (async () => {
      throw new Error("fake projectApplyInspectPipelineV2 not configured");
    }) as unknown as CliIo["projectApplyInspectPipelineV2"],
    respondPipelineV2Wait: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["respondPipelineV2Wait"],
    readTaskFile: (path: string) => readFile(path, "utf8"),
    projectChangesPipelineV2Run: (async () => {
      throw new Error("fake not configured");
    }) as unknown as CliIo["projectChangesPipelineV2Run"],
    writeStdout: () => {},
    writeError: () => {},
  };
  return { stateRoot, sessionIds, sessionCreates, sessionDeletes, runBodies, activationInputs, io };
}

async function chainCommand(
  setup: ChainSetup,
  args: string[],
): Promise<{ exit: number; outcome: Record<string, unknown> }> {
  const stdout: string[] = [];
  const io = setup.io;
  const originalWrite = io.writeStdout;
  io.writeStdout = (text) => {
    stdout.push(text);
    originalWrite(text);
  };
  const exit = await runCli(args, io);
  const documents = stdout.filter((line) => line.trim() !== "");
  expect(documents).toHaveLength(1);
  return { exit, outcome: JSON.parse(documents[0]!) as Record<string, unknown> };
}

interface DevFixture {
  root: string;
  stateRoot: string;
  taskPath: string;
  projectSource: string;
  configRoot: string;
  credFile: string;
}

async function makeDevFixture(prefix: string): Promise<DevFixture> {
  const root = mkdtempSync(join(tmpdir(), `${prefix}-`));
  const stateRoot = join(root, "state");
  mkdirSync(stateRoot, { recursive: true });
  const sources = join(root, "userdata");
  mkdirSync(sources, { recursive: true });
  const taskPath = join(sources, "task.md");
  writeFileSync(taskPath, TASK_BODY, { mode: 0o600 });
  const projectSource = join(root, "project-source");
  mkdirSync(projectSource, { recursive: true });
  const configRoot = join(root, "config");
  mkdirSync(join(configRoot, "profiles"), { recursive: true });
  mkdirSync(join(configRoot, "opencode"), { recursive: true });
  for (const name of ["architect", "code", "review"]) {
    writeFileSync(
      join(configRoot, "profiles", `${name}.yaml`),
      [
        "schema_version: 2",
        "image: ghcr.io/example/worker@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        "opencode_config:",
        `  path: opencode/${name}.json`,
        "  secrets: env_only",
        "env:",
        "  MODEL_API_KEY:",
        "    from_env: DEFAULT_SOURCE_VAR_1",
        "    required: true",
        "    secret: true",
        "",
      ].join("\n"),
    );
    writeFileSync(join(configRoot, "opencode", `${name}.json`), JSON.stringify({ model: "glm53-flash" }));
  }
  const credFile = join(root, "cred.token");
  writeFileSync(credFile, "cred-token-not-real\n", { mode: 0o600 });
  return { root, stateRoot, taskPath, projectSource, configRoot, credFile };
}

function statDirSafe(path: string): boolean {
  try {
    return readdirSync(path) !== undefined;
  } catch {
    return false;
  }
}

function readdirSyncSafe(path: string): boolean {
  try {
    readdirSync(path);
    return true;
  } catch {
    return false;
  }
}

/** The declared review session/worker policies of one state id, from the transport records. */
function projectAccessForState(setup: ChainSetup, stateId: string): { session: string; mount: unknown } {
  const create = setup.sessionCreates.find((entry) =>
    entry.roots.some((root) => root.endsWith(`-${stateId}/data/outputs=read_write`)),
  );
  if (create === undefined) {
    throw new Error(`no execution session recorded for state ${stateId}`);
  }
  const projectRoot = create.roots.find((root) => root.includes("/project=")) ?? "";
  const access = projectRoot.split("=")[1] ?? "";
  const body = setup.runBodies.find((entry) => entry.stateId === stateId);
  if (body === undefined) {
    throw new Error(`no worker body recorded for state ${stateId}`);
  }
  const mounts = (body.body["mounts"] ?? []) as Array<Record<string, unknown>>;
  const project = mounts.find((mount) => mount["target"] === "/workspace");
  return { session: access, mount: project?.["read_only"] };
}

function toolAccessInPairs(setup: ChainSetup): Array<{ execution: string; tool: string }> {
  const pairs: Array<{ execution: string; tool: string }> = [];
  for (let index = 0; index < setup.sessionCreates.length; index += 2) {
    const execution = setup.sessionCreates[index];
    const tool = setup.sessionCreates[index + 1];
    if (execution === undefined || tool === undefined) {
      break;
    }
    const projectRoot = execution.roots.find((root) => root.includes("/project=")) ?? "";
    pairs.push({ execution: projectRoot.split("=")[1] ?? "", tool: (tool.roots[0] ?? "").split("=")[1] ?? "" });
  }
  return pairs;
}

async function readDurableState(runRoot: string): Promise<PipelineV2RunState> {
  return parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
}

// ---------------------------------------------------------------------------
// E2E A: the accepted first review reaches terminal success in one command.
// ---------------------------------------------------------------------------

test("e2e: the accepted first review reaches terminal success in one run command", async () => {
  const fixture = await makeDevFixture("dev-v1-e2e-accepted");
  try {
    const setup = makeChain(
      fixture.stateRoot,
      [{ accepted: true, feedback: FEEDBACK_2 }],
      fixture.credFile,
    );
    setup.io.runPipelineV2 = runPipelineV2 as unknown as CliIo["runPipelineV2"];
    const record = await chainCommand(setup, [
      "run",
      "--pipeline-root", DEV_BUNDLE,
      "--config-root", fixture.configRoot,
      "--project", fixture.projectSource,
      "--input", `task=${fixture.taskPath}`,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(record.exit).toBe(0);
    expect(Object.keys(record.outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(record.outcome["ok"]).toBe(true);
    expect(record.outcome["planReady"]).toBeUndefined();
    expect(record.outcome["waiting"]).toBeUndefined();
    const runId = record.outcome["runId"] as string;
    const runRoot = join(fixture.stateRoot, "pipeline-runs", runId);

    const state = await readDurableState(runRoot);
    expect(state.revision).toBe(35);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.task_revisions.map((r) => `${r.task_id}@${r.revision}`)).toEqual(["task-1@1"]);
    expect(state.plan_revisions.map((r) => r.revision)).toEqual([1]);
    expect(state.generations).toHaveLength(1);
    expect(state.generations[0]).toMatchObject({
      index: 1,
      stage_id: "stage-1",
      stage_position: 1,
      template_id: "development",
      initial_budget: 1,
      opened_transition_count: 0,
    });
    expect(state.executions.map((execution) => [execution.state_id, execution.execution_role])).toEqual([
      ["plan", "planning"],
      ["implement", "stage"],
      ["review", "stage"],
      ["review_gate", "stage"],
    ]);
    expect(state.transitions).toEqual([
      { index: 0, from: "plan", outcome: "completed", to: "implement", execution_index: 1 },
      { index: 0, from: "implement", outcome: "completed", to: "review", execution_index: 2 },
      { index: 0, from: "review", outcome: "completed", to: "review_gate", execution_index: 3 },
      { index: 0, from: "review_gate", outcome: "accepted", to: "done", execution_index: 4 },
    ]);
    // the published run output is exactly the reviewer's accepted feedback
    expect(state.run_outputs).toHaveLength(1);
    const publishedBytes = await readFile(join(runRoot, "outputs", "review"), "utf8");
    expect(publishedBytes).toBe(JSON.stringify(FEEDBACK_2));
    expect((JSON.parse(publishedBytes) as Record<string, unknown>)["summary"]).toBe("REVIEW-SUMMARY-2");
    // the stage task bytes reach the implementer and the reviewer exactly
    expect(setup.activationInputs.map((entry) => [entry.stateId, entry.stageTask])).toEqual([
      ["plan", TASK_BODY],
      ["implement", TASK_BODY],
      ["review", TASK_BODY],
    ]);
    // the reviewer receives the implementer's accepted result bytes
    expect(setup.activationInputs[2]!.implementationResult).toBe(RESULT_BYTES);
    // workspace authority: implement read-write; review read-only (session
    // policy and worker mount agree)
    expect(projectAccessForState(setup, "implement")).toEqual({ session: "read_write", mount: undefined });
    expect(projectAccessForState(setup, "review")).toEqual({ session: "read_only", mount: true });
    // tool sessions mirror their execution pair's project access
    const pairs = toolAccessInPairs(setup);
    expect(pairs).toHaveLength(3);
    expect(pairs[0]).toEqual({ execution: "read_write", tool: "read_write" });
    expect(pairs[1]).toEqual({ execution: "read_write", tool: "read_write" });
    expect(pairs[2]).toEqual({ execution: "read_only", tool: "read_only" });
    // the relay never ran on the accepted path
    expect(setup.runBodies.map((entry) => entry.stateId)).toEqual(["plan", "implement", "review"]);
    // six sessions, every pair deleted exactly once, tool first
    expect(setup.sessionCreates).toHaveLength(6);
    expect(setup.sessionDeletes).toEqual([
      setup.sessionIds[1]!,
      setup.sessionIds[0]!,
      setup.sessionIds[3]!,
      setup.sessionIds[2]!,
      setup.sessionIds[5]!,
      setup.sessionIds[4]!,
    ]);
    // loader round-trip and durable identity
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
    const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId });
    expect(sink.snapshot).toEqual(state);
    const reloaded = await loadPipelineV2(state.pipeline.bundle_root);
    expect(reloaded.bundleRoot).toBe(DEV_BUNDLE);
    expect(pipelineV2RunPipelineIdentity(reloaded)).toEqual(state.pipeline);
    expect(state.waits).toHaveLength(0);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);

// ---------------------------------------------------------------------------
// E2E B: the changes-requested review enters the trusted stage wait.
// ---------------------------------------------------------------------------

interface WaitingFixture extends DevFixture {
  runId: string;
  runRoot: string;
  setup: ChainSetup;
}

async function buildWaitingFixture(prefix: string): Promise<WaitingFixture> {
  const fixture = await makeDevFixture(prefix);
  const setup = makeChain(fixture.stateRoot, [{ accepted: false, feedback: FEEDBACK_1 }], fixture.credFile);
  setup.io.runPipelineV2 = runPipelineV2 as unknown as CliIo["runPipelineV2"];
  const record = await chainCommand(setup, [
    "run",
    "--pipeline-root", DEV_BUNDLE,
    "--config-root", fixture.configRoot,
    "--project", fixture.projectSource,
    "--input", `task=${fixture.taskPath}`,
    "--launcher-id", LAUNCHER_ID,
    "--json",
  ]);
  expect(record.exit).toBe(0);
  const runId = record.outcome["runId"] as string;
  const runRoot = join(fixture.stateRoot, "pipeline-runs", runId);
  const state = await readDurableState(runRoot);
  expect(state.status).toBe("waiting");
  return { ...fixture, runId, runRoot, setup };
}

function expectWaitingOutcome(record: { exit: number; outcome: Record<string, unknown> }, label: string): void {
  expect(record.exit, label).toBe(0);
  expect(Object.keys(record.outcome).sort(), label).toEqual(["exitCode", "ok", "runId", "runRoot", "state", "waiting"]);
  expect(record.outcome["ok"], label).toBe(false);
  expect(record.outcome["waiting"], label).toBe(true);
  expect(record.outcome["reason"], label).toBeUndefined();
}

test("e2e: a changes-requested review enters the trusted stage wait at the replanning boundary", async () => {
  const fixture = await buildWaitingFixture("dev-v1-e2e-wait");
  try {
    const state = await readDurableState(fixture.runRoot);
    expect(state.revision).toBe(41);
    expect(state.status).toBe("waiting");
    expect(state.phase).toBe("waiting");
    expect(state.waits).toHaveLength(1);
    const wait = state.waits[0]!;
    expect(wait).toMatchObject({
      index: 1,
      transition_count: 5,
      state_id: "replan",
      reason: "stage_iteration_completed",
    });
    expect(wait.actions).toEqual([
      { id: "continue_stage", to: "implement" },
      { id: "revise_task", to: "replan" },
    ]);
    expect(wait.response).toBeUndefined();
    expect(wait.intent).toBeUndefined();
    // the request manifest was published from the trusted compiled policy
    const request = JSON.parse(await readFile(join(fixture.runRoot, "waits", "1.request.json"), "utf8")) as Record<string, unknown>;
    expect(request["state_id"]).toBe("replan");
    expect(request["reason"]).toBe("stage_iteration_completed");
    expect((request["actions"] as Array<Record<string, unknown>>).map((action) => action["id"])).toEqual([
      "continue_stage",
      "revise_task",
    ]);
    // the relay ran as the wait-bearing last stage execution; the replanner
    // did not start
    expect(state.executions.map((execution) => [execution.state_id, execution.execution_role])).toEqual([
      ["plan", "planning"],
      ["implement", "stage"],
      ["review", "stage"],
      ["review_gate", "stage"],
      ["relay", "stage"],
    ]);
    expect(state.transitions.map((transition) => [transition.from, transition.outcome, transition.to])).toEqual([
      ["plan", "completed", "implement"],
      ["implement", "completed", "review"],
      ["review", "completed", "review_gate"],
      ["review_gate", "changes_requested", "relay"],
      ["relay", "completed", "replan"],
    ]);
    // the relay held a read-only project workspace and its declared outputs
    // stayed empty
    expect(projectAccessForState(fixture.setup, "relay")).toEqual({ session: "read_only", mount: true });
    // eight sessions: plan, implement, review, relay pairs; tool-first deletes
    expect(fixture.setup.sessionCreates).toHaveLength(8);
    const ids = fixture.setup.sessionIds;
    expect(fixture.setup.sessionDeletes).toEqual([ids[1]!, ids[0]!, ids[3]!, ids[2]!, ids[5]!, ids[4]!, ids[7]!, ids[6]!]);
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);

// ---------------------------------------------------------------------------
// E2E C: continue-stage opens iteration 2 with unchanged task bytes.
// ---------------------------------------------------------------------------

test("e2e: continue-stage opens iteration 2 with unchanged task bytes and runs to success", async () => {
  const fixture = await buildWaitingFixture("dev-v1-e2e-continue");
  try {
    const setup2 = makeChain(
      fixture.stateRoot,
      [{ accepted: true, feedback: FEEDBACK_2 }],
      fixture.credFile,
    );
    setup2.io.continuePipelineV2Stage = continuePipelineV2Stage as unknown as CliIo["continuePipelineV2Stage"];
    const record = await chainCommand(setup2, [
      "continue-stage",
      "--run-id", fixture.runId,
      "--wait-index", "1",
      "--additional-iterations", "1",
      "--config-root", fixture.configRoot,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(record.exit).toBe(0);
    expect(Object.keys(record.outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(record.outcome["ok"]).toBe(true);
    const state = await readDurableState(fixture.runRoot);
    expect(state.revision).toBe(68);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    // the grant closed iteration 1 bound to the wait; iteration 2 is open
    expect(state.generations).toHaveLength(1);
    const generation = state.generations[0]!;
    expect(generation.iterations).toHaveLength(2);
    expect(generation.iterations[0]).toMatchObject({ index: 1, closed: { by: "grant", wait_index: 1 } });
    expect(generation.open_iteration).toMatchObject({ index: 2 });
    // the task revision is unchanged; the iteration-2 implementer received
    // the exact same stage task bytes
    expect(state.task_revisions.map((r) => `${r.task_id}@${r.revision}`)).toEqual(["task-1@1"]);
    const iteration2Inputs = setup2.activationInputs.filter((entry) => entry.stateId === "implement");
    expect(iteration2Inputs).toHaveLength(1);
    expect(iteration2Inputs[0]!.stageTask).toBe(TASK_BODY);
    // the second review's verdict drove the gate to accepted; the published
    // run output is the latest accepted feedback (highest activation index)
    expect(state.transitions.at(-1)).toEqual({
      index: 0,
      from: "review_gate",
      outcome: "accepted",
      to: "done",
      execution_index: 8,
    });
    const published = await readFile(join(fixture.runRoot, "outputs", "review"), "utf8");
    expect(published).toBe(JSON.stringify(FEEDBACK_2));
    // only the iteration-2 stage pair ran through this CLI process
    expect(setup2.sessionCreates).toHaveLength(4);
    expect(setup2.sessionDeletes).toEqual([
      setup2.sessionIds[1]!,
      setup2.sessionIds[0]!,
      setup2.sessionIds[3]!,
      setup2.sessionIds[2]!,
    ]);
    expect(projectAccessForState(setup2, "review")).toEqual({ session: "read_only", mount: true });
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);

// ---------------------------------------------------------------------------
// E2E D: revise-task runs the whole replanning chain in one command.
// ---------------------------------------------------------------------------

test("e2e: revise-task runs the whole replanning chain in one command and the next implementer sees the revised bytes", async () => {
  const fixture = await buildWaitingFixture("dev-v1-e2e-revise");
  try {
    const revisedPath = join(fixture.root, "revised-task.md");
    writeFileSync(revisedPath, REVISED_BODY, { mode: 0o600 });
    const setup2 = makeChain(
      fixture.stateRoot,
      [{ accepted: true, feedback: FEEDBACK_2 }],
      fixture.credFile,
    );
    setup2.io.revisePipelineV2Task = revisePipelineV2Task as unknown as CliIo["revisePipelineV2Task"];
    const record = await chainCommand(setup2, [
      "revise-task",
      "--run-id", fixture.runId,
      "--wait-index", "1",
      "--task-id", "task-1",
      "--task-file", revisedPath,
      "--config-root", fixture.configRoot,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(record.exit).toBe(0);
    expect(Object.keys(record.outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(record.outcome["ok"]).toBe(true);
    const state = await readDurableState(fixture.runRoot);
    expect(state.revision).toBe(79);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    // the revised task revision and the second plan revision are durable
    expect(state.task_revisions.map((r) => `${r.task_id}@${r.revision}`)).toEqual(["task-1@1", "task-1@2"]);
    expect(state.plan_revisions.map((r) => r.revision)).toEqual([1, 2]);
    expect(state.plan_revisions[1]!.origin_execution).toBe(6);
    // the old generation closed by the replanned closure bound to the wait;
    // the new generation is open with iteration 1
    expect(state.generations).toHaveLength(2);
    expect(state.generations[0]).toMatchObject({
      index: 1,
      stage_id: "stage-1",
      template_id: "development",
      closed: { by: "replanned", closed_transition_count: 5 },
    });
    expect(state.generations[1]).toMatchObject({
      index: 2,
      stage_id: "stage-1",
      stage_position: 1,
      template_id: "development",
      initial_budget: 1,
      opened_transition_count: 5,
    });
    expect(state.generations[1]!.open_iteration).toMatchObject({ index: 1 });
    // the replanner ran as a planning execution without an iteration index;
    // the successor implementer runs in the new generation's iteration 1
    expect(state.executions.map((execution) => [execution.state_id, execution.execution_role, (execution as { iteration_index?: number }).iteration_index])).toEqual([
      ["plan", "planning", undefined],
      ["implement", "stage", 1],
      ["review", "stage", 1],
      ["review_gate", "stage", 1],
      ["relay", "stage", 1],
      ["replan", "planning", undefined],
      ["implement", "stage", 1],
      ["review", "stage", 1],
      ["review_gate", "stage", 1],
    ]);
    // the new implementer received the EXACT revised task bytes through the
    // stage-task port of the new generation's plan binding
    const gen2Inputs = setup2.activationInputs.filter((entry) => entry.stateId === "implement");
    expect(gen2Inputs).toHaveLength(1);
    expect(gen2Inputs[0]!.stageTask).toBe(REVISED_BODY);
    // the replanner received the reviewer's latest accepted feedback
    const replanInputs = setup2.activationInputs.filter((entry) => entry.stateId === "replan");
    expect(replanInputs).toHaveLength(1);
    expect(replanInputs[0]!.stageTask).toBeNull();
    // the published run output is the latest accepted feedback
    const published = await readFile(join(fixture.runRoot, "outputs", "review"), "utf8");
    expect(published).toBe(JSON.stringify(FEEDBACK_2));
    // six sessions in this CLI process: replan, implement, review pairs
    expect(setup2.sessionCreates).toHaveLength(6);
    expect(setup2.sessionDeletes).toEqual([
      setup2.sessionIds[1]!,
      setup2.sessionIds[0]!,
      setup2.sessionIds[3]!,
      setup2.sessionIds[2]!,
      setup2.sessionIds[5]!,
      setup2.sessionIds[4]!,
    ]);
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);

// ---------------------------------------------------------------------------
// E2E E: a crash between the relay transition and the wait entry is
// recovered by plain resume.
// ---------------------------------------------------------------------------

class FaultedSink {
  constructor(
    private readonly inner: PipelineV2RunStateSink,
    private readonly faults: Readonly<Record<string, () => Error>>,
  ) {}

  get snapshot(): PipelineV2RunState | null {
    return this.inner.snapshot;
  }

  get poisoned(): boolean {
    return this.inner.poisoned;
  }

  async dispatch(command: Parameters<PipelineV2RunStateSink["dispatch"]>[0]): Promise<void> {
    const fault = this.faults[command.kind];
    if (fault !== undefined) {
      throw fault();
    }
    await this.inner.dispatch(command);
  }
}

let directClock = 0;
function directClockNow(): Date {
  directClock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, directClock));
}

function directRuntime(reviews: readonly ReviewBehavior[]): PipelineV2AgentRuntime {
  const reviewQueue = [...reviews];
  let pending: { outputsRoot: string; inputsRoot: string; stateId: string } | null = null;
  const writeOutputs = async (): Promise<void> => {
    if (pending === null) {
      return;
    }
    const { outputsRoot, inputsRoot, stateId } = pending;
    pending = null;
    if (stateId === "plan") {
      const task = await readFile(join(inputsRoot, "task"), "utf8");
      await writeFile(join(outputsRoot, "plan"), JSON.stringify(proposalFor(task, false)), { mode: 0o600 });
    } else if (stateId === "replan") {
      await writeFile(join(outputsRoot, "plan"), JSON.stringify(proposalFor("", true)), { mode: 0o600 });
    } else if (stateId === "implement") {
      await writeFile(join(outputsRoot, "result"), RESULT_BYTES, { mode: 0o600 });
    } else if (stateId === "review") {
      const behavior = reviewQueue.shift();
      if (behavior === undefined) {
        throw new Error("no scripted review behavior left");
      }
      await writeFile(join(outputsRoot, "verdict"), JSON.stringify({ accepted: behavior.accepted }), { mode: 0o600 });
      await writeFile(join(outputsRoot, "feedback"), JSON.stringify(behavior.feedback), { mode: 0o600 });
    }
  };
  return {
    createExecutionSession: async (_state: unknown, activation: { outputs_root: string; inputs_root: string; state_id: string }) => {
      pending = { outputsRoot: activation.outputs_root, inputsRoot: activation.inputs_root, stateId: activation.state_id };
      return {
        sessionId: `dir_exec_${++directClock}`,
        runAgent: async () => {
          await writeOutputs();
          return { status: "completed" as const };
        },
        cleanup: async () => {},
      } as never;
    },
    createToolSession: async () => ({ sessionId: `dir_tool_${++directClock}`, cleanup: async () => {} }) as never,
  } as unknown as PipelineV2AgentRuntime;
}

const NEUTRAL_CONTROL: PipelineV2CoordinatorControl = {
  currentSignal: (): "SIGINT" | "SIGTERM" | null => null,
  freezeSignal: (): "SIGINT" | "SIGTERM" | null => null,
};

test("e2e: a crash between the relay transition and the wait entry is recovered by plain resume", async () => {
  const fixture = await makeDevFixture("dev-v1-e2e-restart-wait");
  try {
    // Build the durable prefix through the real coordination with a sink
    // that fails the run_waiting dispatch: the live wait-entry attempt
    // fails closed without writing a durable run_failed, leaving the
    // committed relay->replan transition and no wait record.
    const runId = "dev-restart-wait";
    mkdirSync(join(fixture.stateRoot, "pipeline-runs"), { recursive: true, mode: 0o700 });
    const runRoot = join(fixture.stateRoot, "pipeline-runs", runId);
    mkdirSync(runRoot, { mode: 0o700 });
    const pipeline = await loadPipelineV2(DEV_BUNDLE);
    const profileEpoch = await prepareAndPublishRunProfileEpoch({
      runRoot,
      pipeline,
      configRoot: fixture.configRoot,
      baseEnv: { HOME: "/home/u", DEFAULT_SOURCE_VAR_1: "dev-secret" },
    });
    const realSink = new PipelineV2RunStateSink({ stateRoot: fixture.stateRoot, runId, now: directClockNow });
    const faulted = new FaultedSink(realSink, {
      run_waiting: () => new PipelineV2RunStateStoreError("crash-wait-window"),
    });
    const planned = await coordinatePipelineV2Run(
      {
        pipeline,
        runId,
        runRoot,
        projectSourcePath: fixture.projectSource,
        inputBindings: [{ id: "task", path: fixture.taskPath }],
        sink: faulted as never,
        runtime: directRuntime([]),
        profileEpoch,
      },
      NEUTRAL_CONTROL,
    );
    expect(planned.ok).toBe(false);
    expect((planned as { planReady?: boolean }).planReady).toBe(true);
    // The trusted plan-ready continuation runs through the same faulted
    // sink: the whole stage iteration executes and the wait-entry attempt
    // at the relay->replan boundary fails closed without writing a durable
    // run_failed, leaving the committed transition and no wait record.
    const continued = await applyPipelineV2PlanReadyContinuation({
      pipeline,
      runRoot,
      sink: faulted as never,
      runtime: directRuntime([{ accepted: false, feedback: FEEDBACK_1 }]),
      control: NEUTRAL_CONTROL,
    });
    expect(continued.ok).toBe(false);
    expect((continued as { reason?: string }).reason).toBe("state_persist_failed");
    const prefixState = await readDurableState(runRoot);
    expect(prefixState.status).toBe("active");
    expect(prefixState.waits).toHaveLength(0);
    expect(prefixState.transitions).toHaveLength(5);
    expect(prefixState.executions).toHaveLength(5);
    expect(prefixState.transitions.at(-1)).toMatchObject({ from: "relay", to: "replan" });

    // The plain resume command enters the wait on the restart path: no
    // repeated transition, no replan execution, no session.
    const setup = makeChain(fixture.stateRoot, [], fixture.credFile);
    setup.io.resumePipelineV2 = resumePipelineV2 as unknown as CliIo["resumePipelineV2"];
    const record = await chainCommand(setup, [
      "resume",
      "--run-id", runId,
      "--config-root", fixture.configRoot,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expectWaitingOutcome(record, "restart-wait");
    const state = await readDurableState(runRoot);
    expect(state.revision).toBe(prefixState.revision + 1);
    expect(state.status).toBe("waiting");
    expect(state.waits).toHaveLength(1);
    expect(state.waits[0]).toMatchObject({
      index: 1,
      transition_count: 5,
      state_id: "replan",
      reason: "stage_iteration_completed",
    });
    expect(state.waits[0]!.actions).toEqual([
      { id: "continue_stage", to: "implement" },
      { id: "revise_task", to: "replan" },
    ]);
    expect(state.transitions).toHaveLength(5);
    expect(state.executions).toHaveLength(5);
    expect(setup.sessionCreates).toHaveLength(0);
    const request = JSON.parse(await readFile(join(runRoot, "waits", "1.request.json"), "utf8")) as Record<string, unknown>;
    expect(request["state_id"]).toBe("replan");
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);

// ---------------------------------------------------------------------------
// E2E F: a revise-task interrupted inside the plan-ready continuation is
// continued by plain resume.
// ---------------------------------------------------------------------------

test("e2e: a revise-task interrupted inside the plan-ready continuation is continued by plain resume", async () => {
  const fixture = await buildWaitingFixture("dev-v1-e2e-revise-restart");
  try {
    // The crash seam: the revise-task intervention completed durably while
    // the plan-ready continuation never started. The honest prefix is the
    // real intervention facade over the reopened run, with the pipeline
    // loaded only from the durable bundle root.
    const durableState = await readDurableState(fixture.runRoot);
    const pipeline = await loadPipelineV2(durableState.pipeline.bundle_root);
    const sink = await PipelineV2RunStateSink.open({ stateRoot: fixture.stateRoot, runId: fixture.runId, now: directClockNow });
    await applyPipelineV2ReviseTaskIntervention({
      pipeline,
      runRoot: fixture.runRoot,
      sink: sink as never,
      runId: fixture.runId,
      waitIndex: 1,
      taskId: "task-1",
      taskBody: REVISED_BODY,
    });
    const midState = await readDurableState(fixture.runRoot);
    expect(midState.revision).toBe(45);
    expect(midState.status).toBe("active");
    expect(midState.phase).toBe("running");
    expect(midState.waits[0]!.response).toBeDefined();
    expect(midState.cursor).toEqual({ current_state: "replan", transition_count: 5 });
    expect(midState.executions).toHaveLength(5);
    expect(midState.task_revisions.map((r) => `${r.task_id}@${r.revision}`)).toEqual(["task-1@1", "task-1@2"]);

    // The plain resume command continues the whole chain: the replanning
    // worker, the automatic plan-ready continuation into the new generation,
    // and the successor stage execution with the revised bytes.
    const setup = makeChain(fixture.stateRoot, [{ accepted: true, feedback: FEEDBACK_2 }], fixture.credFile);
    setup.io.resumePipelineV2 = resumePipelineV2 as unknown as CliIo["resumePipelineV2"];
    const record = await chainCommand(setup, [
      "resume",
      "--run-id", fixture.runId,
      "--config-root", fixture.configRoot,
      "--launcher-id", LAUNCHER_ID,
      "--json",
    ]);
    expect(record.exit).toBe(0);
    expect(Object.keys(record.outcome).sort()).toEqual(["exitCode", "ok", "runId", "runRoot", "state"]);
    expect(record.outcome["ok"]).toBe(true);
    const state = await readDurableState(fixture.runRoot);
    expect(state.revision).toBe(79);
    expect(state.status).toBe("success");
    expect(state.terminal).toEqual({ state_id: "done", result: "success" });
    expect(state.plan_revisions.map((r) => r.revision)).toEqual([1, 2]);
    expect(state.generations).toHaveLength(2);
    expect(state.generations[0]).toMatchObject({ closed: { by: "replanned", closed_transition_count: 5 } });
    expect(state.generations[1]!.open_iteration).toMatchObject({ index: 1 });
    const gen2Inputs = setup.activationInputs.filter((entry) => entry.stateId === "implement");
    expect(gen2Inputs).toHaveLength(1);
    expect(gen2Inputs[0]!.stageTask).toBe(REVISED_BODY);
    const published = await readFile(join(fixture.runRoot, "outputs", "review"), "utf8");
    expect(published).toBe(JSON.stringify(FEEDBACK_2));
    expect(setup.sessionCreates).toHaveLength(6);
    expect(parsePipelineV2RunState(JSON.stringify(state))).toEqual(state);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 15000);
