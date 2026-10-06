import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { PipelineError } from "../src/pipeline.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  readAcceptedJsonOutput,
  evaluateDecisionStateFromData,
  prepareActivationData,
  runInputSnapshotDigest,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";
import {
  PipelineV2RuntimeContextRestoreError,
  restorePipelineV2RuntimeContext,
  type PipelineV2RuntimeContextRestoreFailureReason,
} from "../src/pipeline_v2_resume_context.ts";

const RUN_ID = "run-1";
const REASON = "stage_iteration_limit_exhausted";
const CANARY_BODY = "CANARY_secret_body_value";

const hex = (char: string): string => char.repeat(64);

const FACTS_SCHEMA = {
  type: "object",
  required: ["f1", "f2"],
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

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
constraints: []
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const MAIN_PIPELINE = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json
  - id: docs
    type: directory
    protected: false

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: coder
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: check
      role: control
    - state_id: ship
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/facts.schema.json
      - id: report
        type: file
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
            output: plan
    transitions:
      - outcome: alpha
        to: ship
      - outcome: beta
        to: coder
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: ship
    type: agent
    profile: coder
    prompt: prompts/ship.md
    inputs:
      - id: plan_in
        source:
          state_output:
            state: coder
            output: plan
    outputs:
      - id: plan
        type: json
        schema: schemas/facts.schema.json
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

interface Base {
  root: string;
  pipeline: ResolvedPipelineV2;
  runRoot: string;
  runInputs: RunInputsSnapshot;
  clock: { value: number };
  drive: Drive;
  sources: string;
}

interface Drive {
  state: PipelineV2RunState | null;
  records: AcceptedStateOutput[];
}

function tickOf(clock: { value: number }): Date {
  clock.value += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock.value));
}

function dispatchClock(drive: Drive, clock: { value: number }, command: PipelineV2RunCommand): void {
  drive.state = reducePipelineV2RunCommand(drive.state, command, tickOf(clock));
}

function expectRestoreError(
  cause: unknown,
  reason: PipelineV2RuntimeContextRestoreFailureReason,
): PipelineV2RuntimeContextRestoreError {
  expect(cause).toBeInstanceOf(PipelineV2RuntimeContextRestoreError);
  const error = cause as PipelineV2RuntimeContextRestoreError;
  expect(error.reason).toBe(reason);
  return error;
}

async function makeFifo(path: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn("mkfifo", [path]);
    child.on("error", reject);
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

async function setupBase(): Promise<Base> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-resume-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), MAIN_PIPELINE);
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "prompts", "ship.md"), "ship the task\n");
  await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  const pipeline = await loadPipelineV2(bundle);

  const sources = join(root, "userdata");
  await mkdir(join(sources, "docs"), { recursive: true });
  await writeFile(join(sources, "task.md"), "TASK-BODY\n");
  await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true, f2: false }));
  await writeFile(join(sources, "docs", "a.md"), "DOC-A\n");
  await writeFile(join(sources, "docs", "b.md"), "DOC-B\n");

  const runRoot = join(root, "runs", RUN_ID);
  await mkdir(join(runRoot, "project"), { mode: 0o700, recursive: true });
  const runInputs = await snapshotRunInputs(pipeline, [
    { id: "task", path: join(sources, "task.md") },
    { id: "facts_seed", path: join(sources, "facts.json") },
    { id: "docs", path: join(sources, "docs") },
  ], runRoot);

  const clock = { value: 0 };
  const drive: Drive = { state: null, records: [] };
  dispatchClock(drive, clock, {
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  return { root, pipeline, runRoot, runInputs, clock, drive, sources };
}

async function dispose(base: Base): Promise<void> {
  await rm(base.root, { recursive: true, force: true });
}

const AGENT_PHASE_COMMANDS = (sessionId: string, toolId: string): PipelineV2RunCommand[] => [
  { kind: "agent_data_prepared" },
  { kind: "agent_execution_session_created", sessionId },
  { kind: "agent_tool_session_created", sessionId: toolId },
  { kind: "agent_running" },
];

async function writeAgentOutputs(
  base: Base,
  stateId: string,
  activationIndex: number,
  planBytes: string,
): Promise<void> {
  const outputsRoot = join(base.runRoot, "activations", `${activationIndex}-${stateId}`, "data", "outputs");
  await writeFile(join(outputsRoot, "plan"), planBytes, { mode: 0o600 });
  await writeFile(join(outputsRoot, "report"), `report for ${stateId} ${activationIndex}\n`, { mode: 0o600 });
}

/** Writes exactly the declared ship ports (plan json only). */
async function writeShipOutputs(
  base: Base,
  activationIndex: number,
): Promise<void> {
  const outputsRoot = join(base.runRoot, "activations", `${activationIndex}-ship`, "data", "outputs");
  await writeFile(join(outputsRoot, "plan"), JSON.stringify({ f1: true, f2: false }), { mode: 0o600 });
}

async function runAgentActivation(
  base: Base,
  drive: Drive,
  stateId: string,
  planBytes: string,
): Promise<void> {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, { kind: "start_agent_execution", stateId, profile: "coder", ...startRoleArgs(base.pipeline, stateId, (drive.state as PipelineV2RunState)) });
  for (const command of AGENT_PHASE_COMMANDS(`sess-${executionIndex}`, `tool-${executionIndex}`)) {
    dispatchClock(drive, base.clock, command);
  }
  const prepared = await prepareActivationData(
    base.pipeline,
    base.runInputs,
    drive.records,
    stateId,
    executionIndex,
  );
  if (stateId !== "ship") {
    await writeAgentOutputs(base, stateId, executionIndex, planBytes);
  } else {
    await writeShipOutputs(base, executionIndex);
  }
  const accepted = await acceptActivationOutputs(base.pipeline, prepared);
  dispatchClock(drive, base.clock, {
    kind: "agent_outputs_accepted",
    outputs: accepted.map((record) => ({ id: record.output, digest: record.digest })),
  });
  drive.records.push(...accepted);
  dispatchClock(drive, base.clock, { kind: "agent_cleanup_completed" });
  const transitions = (drive.state as PipelineV2RunState).cursor;
  void transitions;
  dispatchClock(drive, base.clock, {
    kind: "transition_committed",
    step: {
      from: stateId,
      outcome: "completed",
      to: stateId === "coder" ? "check" : "done",
      transition_index: 0,
    },
    executionIndex,
  });
}

function runDecisionActivation(
  base: Base,
  drive: Drive,
  stateId: string,
  outcome: "alpha" | "beta",
): void {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, { kind: "start_decision_execution", stateId, inputDigest: hex("e"), ...startRoleArgs(base.pipeline, stateId, (drive.state as PipelineV2RunState)) });
  dispatchClock(drive, base.clock, {
    kind: "decision_evaluated",
    result: {
      status: "selected",
      outcome,
      decision: outcome,
      rule_id: "R1",
      active_constraint_ids: [],
    },
  });
  dispatchClock(drive, base.clock, {
    kind: "transition_committed",
    step: {
      from: stateId,
      outcome,
      to: outcome === "alpha" ? "ship" : "coder",
      transition_index: outcome === "alpha" ? 0 : 1,
    },
    executionIndex,
  });
}

function enterWait(drive: Drive, clock: { value: number }, stateId: string): void {
  dispatchClock(drive, clock, {
    kind: "run_waiting",
    stateId,
    reason: REASON,
    requestSha256: hex("e"),
    actions: [{ id: "continue_stage", to: stateId }],
  });
}

function respondWait(drive: Drive, clock: { value: number }, waitIndex: number): void {
  dispatchClock(drive, clock, {
    kind: "wait_response_recorded",
    waitIndex,
    expectedRequestSha256: hex("e"),
    actionId: "continue_stage",
    responseSha256: hex("f"),
  });
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (dir: string, rel: string): Promise<void> => {
    const entries = await readdir(dir, { withFileTypes: true });
    entries.sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
    for (const entry of entries) {
      const childRel = rel === "" ? entry.name : `${rel}/${entry.name}`;
      const childPath = join(dir, entry.name);
      const info = await lstat(childPath);
      if (info.isSymbolicLink()) {
        lines.push(`${childRel} symlink ${await readlink(childPath)}`);
      } else if (info.isDirectory()) {
        lines.push(`${childRel} dir ${(info.mode & 0o7777).toString(8)} ${info.ino}`);
        await walk(childPath, childRel);
      } else if (info.isFile()) {
        lines.push(
          `${childRel} file ${(info.mode & 0o7777).toString(8)} ${info.ino} ${(await readFile(childPath)).toString("base64")}`,
        );
      } else {
        lines.push(`${childRel} other`);
      }
    }
  };
  await walk(root, "");
  return lines.join("\n");
}

test("1. the clean active boundary right after create_run is restorable", async () => {
  const base = await setupBase();
  try {
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.state.status).toBe("active");
    expect(context.state.revision).toBe(1);
    expect(context.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    expect(context.next_execution_index).toBe(1);
    expect(context.accepted_outputs).toEqual([]);
    expect(context.run_inputs.inputs.map((entry) => entry.id)).toEqual(["task", "facts_seed", "docs"]);
    expect(context.run_inputs.inputs[2]?.type).toBe("directory");
  } finally {
    await dispose(base);
  }
});

test("2. the waiting open boundary is restorable", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    enterWait(base.drive, base.clock, "ship");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.state.status).toBe("waiting");
    expect(context.cursor).toEqual({ current_state: "ship", transition_count: 2 });
    expect(context.next_execution_index).toBe(3);
    expect(context.state.waits[0]?.response).toBeUndefined();
  } finally {
    await dispose(base);
  }
});

test("3. the active boundary after a wait response is restorable", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    enterWait(base.drive, base.clock, "ship");
    respondWait(base.drive, base.clock, 1);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.state.status).toBe("active");
    expect(context.state.phase).toBe("running");
    expect(context.state.waits[0]?.response?.action_id).toBe("continue_stage");
    expect(context.cursor.current_state).toBe("ship");
  } finally {
    await dispose(base);
  }
});

test("4. agent-decision-agent history: global next execution index and ordering", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    await runAgentActivation(base, base.drive, "ship", "unused");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.next_execution_index).toBe(4);
    expect(context.state.executions.map((execution) => execution.index)).toEqual([1, 2, 3]);
    expect(context.accepted_outputs).toEqual(base.drive.records);
    expect(context.accepted_outputs[0]?.digest).toMatch(/^[0-9a-f]{64}$/);
  } finally {
    await dispose(base);
  }
});

test("5. repeated agent activations keep deterministic accepted ordering", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "beta");
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: false, f2: true }));
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.accepted_outputs.map((record) => `${record.state}@${record.activation_index}.${record.output}`)).toEqual([
      "coder@1.plan",
      "coder@1.report",
      "coder@3.plan",
      "coder@3.report",
    ]);
    const planWinner = context.accepted_outputs.find(
      (record) => record.output === "plan" && record.activation_index === 3,
    );
    const planOld = context.accepted_outputs.find(
      (record) => record.output === "plan" && record.activation_index === 1,
    );
    expect(planWinner?.digest).not.toBe(planOld?.digest);
  } finally {
    await dispose(base);
  }
});

test("6. the restored snapshot is accepted by prepareActivationData", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    const prepared = await prepareActivationData(
      base.pipeline,
      context.run_inputs,
      context.accepted_outputs,
      "ship",
      context.next_execution_index,
    );
    expect(prepared.state_id).toBe("ship");
    expect(prepared.mounts.length).toBe(3);
  } finally {
    await dispose(base);
  }
});

test("7. the restored snapshot is accepted by the decision adapter", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    const result = await evaluateDecisionStateFromData(
      base.pipeline,
      context.run_inputs,
      context.accepted_outputs,
      "check",
      context.next_execution_index,
    );
    expect(result.status).toBe("selected");
    if (result.status === "selected") {
      expect(result.outcome).toBe("alpha");
    }
  } finally {
    await dispose(base);
  }
});

test("8. clones and proxies of the restored snapshot fail the existing provenance gate", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    const lookalikes: unknown[] = [
      { ...context.run_inputs },
      structuredClone(context.run_inputs),
      new Proxy(context.run_inputs, {}),
    ];
    for (const lookalike of lookalikes) {
      const cause = await prepareActivationData(
        base.pipeline,
        lookalike as RunInputsSnapshot,
        context.accepted_outputs,
        "ship",
        context.next_execution_index,
      ).catch((error) => error);
      expect(cause).toBeInstanceOf(PipelineError);
      expect((cause as Error).message).toContain("hand-built objects");
    }
  } finally {
    await dispose(base);
  }
});

test("9. a proxy pipeline is rejected before any state read or filesystem effect", async () => {
  const base = await setupBase();
  try {
    let stateTraps = 0;
    const stateProxy = new Proxy(base.drive.state!, {
      get(target, prop, receiver) {
        stateTraps += 1;
        return Reflect.get(target as object, prop, receiver);
      },
    });
    const sentinel = join(base.runRoot, "sentinel");
    await writeFile(sentinel, "sentinel\n");
    const cause = await restorePipelineV2RuntimeContext(
      new Proxy(base.pipeline, {}),
      stateProxy,
      base.runRoot,
    ).catch((error) => error);
    expect(cause).toBeInstanceOf(PipelineError);
    expect(cause).not.toBeInstanceOf(PipelineV2RuntimeContextRestoreError);
    expect((cause as Error).message).toContain("hand-built objects");
    expect(stateTraps).toBe(0);
    expect(await readFile(sentinel, "utf8")).toBe("sentinel\n");
  } finally {
    await dispose(base);
  }
});

test("10. deleted original sources are never read again", async () => {
  const base = await setupBase();
  try {
    await rm(base.sources, { recursive: true, force: true });
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.run_inputs.inputs.length).toBe(3);
  } finally {
    await dispose(base);
  }
});

test("11. every pipeline identity field mismatch is rejected as pipeline_mismatch", async () => {
  const base = await setupBase();
  try {
    const mutations: ((broken: Record<string, unknown>) => void)[] = [
      (broken) => {
        (broken.pipeline as Record<string, unknown>)["bundle_root"] = "/opt/other-bundle";
      },
      (broken) => {
        (broken.pipeline as Record<string, unknown>)["execution_snapshot_sha256"] = hex("9");
      },
      (broken) => {
        (broken.pipeline as Record<string, unknown>)["max_transitions"] = 21;
      },
    ];
    for (const mutate of mutations) {
      const broken = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
      mutate(broken);
      const cause = await restorePipelineV2RuntimeContext(base.pipeline, broken, base.runRoot).catch(
        (error) => error,
      );
      expectRestoreError(cause, "pipeline_mismatch");
      // the diagnostic text stays byte-identical after the identity
      // comparison moved into the shared comparator module
      expect((cause as Error).message).toMatch(
        /the durable run state was created for a different pipeline: /,
      );
    }
    // exact per-field diagnostics (byte-identical)
    const bundleRootShifted = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (bundleRootShifted["pipeline"] as Record<string, unknown>)["bundle_root"] = "/opt/other-bundle";
    const bundleCause = await restorePipelineV2RuntimeContext(base.pipeline, bundleRootShifted, base.runRoot).catch(
      (error) => error,
    );
    expect((bundleCause as Error).message).toBe(
      "the durable run state was created for a different pipeline: the canonical bundle root differs",
    );
    const digestShifted = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (digestShifted["pipeline"] as Record<string, unknown>)["execution_snapshot_sha256"] = hex("9");
    const digestCause = await restorePipelineV2RuntimeContext(base.pipeline, digestShifted, base.runRoot).catch(
      (error) => error,
    );
    expect((digestCause as Error).message).toBe(
      "the durable run state was created for a different pipeline: the execution snapshot digest differs",
    );
    const budgetShifted = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (budgetShifted["pipeline"] as Record<string, unknown>)["max_transitions"] = 21;
    const budgetCause = await restorePipelineV2RuntimeContext(base.pipeline, budgetShifted, base.runRoot).catch(
      (error) => error,
    );
    expect((budgetCause as Error).message).toBe(
      "the durable run state was created for a different pipeline: the transition budget differs",
    );
    // An entry-state mismatch is caught by the state loader's cursor replay
    // before the identity comparison: the durable state is incoherent.
    const shiftedEntry = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (shiftedEntry["pipeline"] as Record<string, unknown>)["entry_state"] = "elsewhere";
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, shiftedEntry, base.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await dispose(base);
  }
});

test("12. the run root basename must be the durable run id", async () => {
  const base = await setupBase();
  try {
    const other = join(base.root, "runs", "run-2");
    await mkdir(other, { mode: 0o700 });
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, other).catch(
      (error) => error,
    );
    expectRestoreError(cause, "pipeline_mismatch");
    expect(await readdir(other)).toEqual([]);
  } finally {
    await dispose(base);
  }
});

test("13. durable input metadata mismatches are rejected", async () => {
  const base = await setupBase();
  try {
    const reorder = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    const reordered = [
      (reorder["inputs"] as Record<string, unknown>[])[1],
      (reorder["inputs"] as Record<string, unknown>[])[0],
      (reorder["inputs"] as Record<string, unknown>[])[2],
    ];
    reorder["inputs"] = reordered;
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, reorder, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );

    const wrongType = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (wrongType["inputs"] as Record<string, unknown>[])[0]!["type"] = "directory";
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, wrongType, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );

    const wrongProtected = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (wrongProtected["inputs"] as Record<string, unknown>[])[0]!["protected"] = false;
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, wrongProtected, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );

    const forgedDigest = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (forgedDigest["inputs"] as Record<string, unknown>[])[0]!["digest"] = hex("9");
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, forgedDigest, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
  } finally {
    await dispose(base);
  }
});

test("14. missing and extra durable inputs are rejected", async () => {
  const base = await setupBase();
  try {
    const missing = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    missing["inputs"] = (missing["inputs"] as unknown[]).slice(0, 2);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, missing, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    const extra = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (extra["inputs"] as unknown[]).push({ id: "extra", type: "file", protected: false, digest: hex("1") });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, extra, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
  } finally {
    await dispose(base);
  }
});

test("15. damaged fixed input objects fail as run_input_modified", async () => {
  const base = await setupBase();
  try {
    const taskPath = join(base.runRoot, "data", "inputs", "task");
    const missing = JSON.parse(JSON.stringify(base.drive.state));
    await unlink(taskPath);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, missing, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
    await writeFile(taskPath, "TASK-BODY\n", { mode: 0o600 });

    const symlinked = JSON.parse(JSON.stringify(base.drive.state));
    await unlink(taskPath);
    await symlink("/etc/hostname", taskPath);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, symlinked, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
    await unlink(taskPath);
    await writeFile(taskPath, "TASK-BODY\n", { mode: 0o600 });

    const wrongKind = JSON.parse(JSON.stringify(base.drive.state));
    await unlink(taskPath);
    await mkdir(taskPath);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, wrongKind, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
    await rm(taskPath, { recursive: true, force: true });
    await writeFile(taskPath, "TASK-BODY\n", { mode: 0o600 });

    const digestMismatch = JSON.parse(JSON.stringify(base.drive.state));
    await writeFile(taskPath, "CHANGED\n", { mode: 0o600 });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, digestMismatch, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
    await writeFile(taskPath, "TASK-BODY\n", { mode: 0o600 });

    const relocated = JSON.parse(JSON.stringify(base.drive.state));
    const inputsDir = join(base.runRoot, "data", "inputs");
    await rm(inputsDir, { recursive: true, force: true });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, relocated, base.runRoot).catch((error) => error),
      "run_layout_invalid",
    );
  } finally {
    await dispose(base);
  }
});

test("16. a directory input carrying forbidden objects fails", async () => {
  const base = await setupBase();
  try {
    const docsPath = join(base.runRoot, "data", "inputs", "docs");
    const symlinked = JSON.parse(JSON.stringify(base.drive.state));
    await symlink("../task", join(docsPath, "link"));
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, symlinked, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
    await unlink(join(docsPath, "link"));
    const fifo = JSON.parse(JSON.stringify(base.drive.state));
    await makeFifo(join(docsPath, "pipe"));
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, fifo, base.runRoot).catch((error) => error),
      "run_input_modified",
    );
  } finally {
    await dispose(base);
  }
});

test("17. malformed JSON input with a matching forged digest is rejected", async () => {
  const base = await setupBase();
  try {
    const factsPath = join(base.runRoot, "data", "inputs", "facts_seed");
    const malformed = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    await writeFile(factsPath, `{ "f1": true, ${CANARY_BODY}`, { mode: 0o600 });
    const digest = await runInputSnapshotDigest("json", factsPath, "fixture");
    (malformed["inputs"] as Record<string, unknown>[])[1]!["digest"] = digest;
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, malformed, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "run_input_modified");
    expect((cause as Error).message).not.toContain(CANARY_BODY);
    expect((cause as Error).message).toContain("is not valid JSON");
  } finally {
    await dispose(base);
  }
});

test("18. schema-invalid JSON input with a matching forged digest is rejected", async () => {
  const base = await setupBase();
  try {
    const factsPath = join(base.runRoot, "data", "inputs", "facts_seed");
    const invalid = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    await writeFile(factsPath, JSON.stringify({ f1: "yes", f2: false }), { mode: 0o600 });
    const digest = await runInputSnapshotDigest("json", factsPath, "fixture");
    (invalid["inputs"] as Record<string, unknown>[])[1]!["digest"] = digest;
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, invalid, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "run_input_modified");
    expect((cause as Error).message).toContain("does not conform to its JSON schema");
  } finally {
    await dispose(base);
  }
});

test("19. a multi-activation accepted history is fully restorable", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "beta");
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: false, f2: true }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.accepted_outputs.length).toBe(4);
    expect(context.next_execution_index).toBe(5);
    expect(context.cursor.current_state).toBe("ship");
  } finally {
    await dispose(base);
  }
});

test("20. decision executions never add accepted-output records", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.state.executions.length).toBe(2);
    expect(
      context.accepted_outputs.every((record) => record.state === "coder" && record.activation_index === 1),
    ).toBe(true);
  } finally {
    await dispose(base);
  }
});

test("21. durable agent outputs that mismatch the declared ports are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const outputs = () =>
      ((base.drive.state as PipelineV2RunState).executions[0] as unknown as { outputs: Record<string, string>[] }).outputs;
    const reordered = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    const execution = (reordered["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>;
    execution["outputs"] = [...outputs()].reverse();
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, reordered, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    const extra = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    ((extra["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>)["outputs"] = [
      ...outputs(),
      { id: "extra", digest: hex("1") },
    ];
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, extra, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    const missing = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    ((missing["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>)["outputs"] = [
      outputs()[0],
    ];
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, missing, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    const duplicated = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    ((duplicated["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>)["outputs"] = [
      outputs()[0],
      outputs()[0],
    ];
    // Duplicate output ids are rejected by the state loader itself.
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, duplicated, base.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await dispose(base);
  }
});

test("22. phantom activation leaves and outputs are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const leaf = join(base.runRoot, "activations", "1-coder");
    const noLeaf = JSON.parse(JSON.stringify(base.drive.state));
    await rm(leaf, { recursive: true, force: true });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, noLeaf, base.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
    await runAgentActivationNoState(base, "coder", 1, JSON.stringify({ f1: true, f2: false }));
    const noOutput = JSON.parse(JSON.stringify(base.drive.state));
    await unlink(join(leaf, "data", "outputs", "plan"));
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, noOutput, base.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
  } finally {
    await dispose(base);
  }
});

/** Re-creates the activation leaf of an already-recorded execution. */
async function runAgentActivationNoState(
  base: Base,
  stateId: string,
  activationIndex: number,
  planBytes: string,
): Promise<void> {
  const prepared = await prepareActivationData(
    base.pipeline,
    base.runInputs,
    [],
    stateId,
    activationIndex,
  );
  await writeAgentOutputs(base, stateId, activationIndex, planBytes);
  await acceptActivationOutputs(base.pipeline, prepared);
}

test("23. symlinked, wrong-kind and escaped output locations are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const planPath = join(base.runRoot, "activations", "1-coder", "data", "outputs", "plan");
    const symlinked = JSON.parse(JSON.stringify(base.drive.state));
    await unlink(planPath);
    await symlink("../../../../etc/hostname", planPath);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, symlinked, base.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
    await unlink(planPath);
    const wrongKind = JSON.parse(JSON.stringify(base.drive.state));
    await mkdir(planPath);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, wrongKind, base.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
    await rm(planPath, { recursive: true, force: true });
    await writeFile(planPath, JSON.stringify({ f1: true, f2: false }), { mode: 0o600 });
    const escaped = JSON.parse(JSON.stringify(base.drive.state));
    const outputsDir = join(base.runRoot, "activations", "1-coder", "data", "outputs");
    await rm(outputsDir, { recursive: true, force: true });
    const outside = join(base.root, "outside-outputs");
    await mkdir(outside, { mode: 0o700 });
    await writeFile(join(outside, "plan"), JSON.stringify({ f1: true, f2: false }), { mode: 0o600 });
    await writeFile(join(outside, "report"), "r\n", { mode: 0o600 });
    await symlink(outside, outputsDir);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, escaped, base.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
  } finally {
    await dispose(base);
  }
});

test("24. a corrupted old non-winning output is rejected even when the new one is intact", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "beta");
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: false, f2: true }));
    const oldPlan = join(base.runRoot, "activations", "1-coder", "data", "outputs", "plan");
    await writeFile(oldPlan, "corrupted\n", { mode: 0o600 });
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "accepted_output_modified");
  } finally {
    await dispose(base);
  }
});

test("25. a winning output digest mismatch is rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const reportPath = join(base.runRoot, "activations", "1-coder", "data", "outputs", "report");
    await writeFile(reportPath, "changed bytes\n", { mode: 0o600 });
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "accepted_output_modified");
  } finally {
    await dispose(base);
  }
});

test("26. malformed and schema-invalid JSON outputs with forged digests are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const planPath = join(base.runRoot, "activations", "1-coder", "data", "outputs", "plan");
    const malformed = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    await writeFile(planPath, `{ "f1": ${CANARY_BODY}`, { mode: 0o600 });
    const digest = await acceptedOutputDigest("json", planPath, "fixture");
    const execution = (malformed["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>;
    execution["outputs"] = (execution["outputs"] as Record<string, string>[]).map((output) =>
      output["id"] === "plan" ? { id: "plan", digest } : output,
    );
    const malformedCause = await restorePipelineV2RuntimeContext(base.pipeline, malformed, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(malformedCause, "accepted_output_modified");
    expect((malformedCause as Error).message).not.toContain(CANARY_BODY);

    const schemaInvalid = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    await writeFile(planPath, JSON.stringify({ f1: 7, f2: false }), { mode: 0o600 });
    const invalidDigest = await acceptedOutputDigest("json", planPath, "fixture");
    const invalidExecution = (schemaInvalid["executions"] as Record<string, unknown>[])[0] as Record<string, unknown>;
    invalidExecution["outputs"] = (invalidExecution["outputs"] as Record<string, string>[]).map((output) =>
      output["id"] === "plan" ? { id: "plan", digest: invalidDigest } : output,
    );
    const invalidCause = await restorePipelineV2RuntimeContext(base.pipeline, schemaInvalid, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(invalidCause, "accepted_output_modified");
    expect((invalidCause as Error).message).toContain("does not conform to its JSON schema");
  } finally {
    await dispose(base);
  }
});

test("27. in-flight agent executions are rejected", async () => {
  const base = await setupBase();
  try {
    for (const command of [
      { kind: "start_agent_execution", stateId: "coder", profile: "coder", executionRole: "planning" },
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: "sess-1" },
      { kind: "agent_tool_session_created", sessionId: "tool-1" },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: [] as { id: string; digest: string }[] },
    ] as PipelineV2RunCommand[]) {
      dispatchClock(base.drive, base.clock, command);
      const cause = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch(
        (error) => error,
      );
      expectRestoreError(cause, "invalid_state");
    }
  } finally {
    await dispose(base);
  }
});

test("28. in-flight decisions and settled-but-unbound executions are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    dispatchClock(base.drive, base.clock, { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e"), executionRole: "control" });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    const fresh = await setupBase();
    try {
      await runAgentActivation(fresh, fresh.drive, "coder", JSON.stringify({ f1: true, f2: false }));
      // the transition is missing: remove it from a cloned state
      const unbound = JSON.parse(JSON.stringify(fresh.drive.state)) as Record<string, unknown>;
      unbound["transitions"] = [];
      (unbound["cursor"] as Record<string, unknown>)["transition_count"] = 0;
      expectRestoreError(
        await restorePipelineV2RuntimeContext(fresh.pipeline, unbound, fresh.runRoot).catch((error) => error),
        "invalid_state",
      );
    } finally {
      await dispose(fresh);
    }
  } finally {
    await dispose(base);
  }
});

test("29. terminal, publishing, success, failed and cleanup-failed states are rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    await runAgentActivation(base, base.drive, "ship", "unused");
    dispatchClock(base.drive, base.clock, {
      kind: "terminal_reached",
      terminalStateId: "done",
      terminalResult: "success",
    });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    const published = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    const publishedDrive: Drive = { state: published as unknown as PipelineV2RunState, records: base.drive.records };
    dispatchClock(publishedDrive, base.clock, {
      kind: "run_outputs_published",
      outputs: [{ id: "plan", type: "json", required: true, present: true, digest: hex("d") }],
    });
    dispatchClock(publishedDrive, base.clock, { kind: "run_succeeded" });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, publishedDrive.state, base.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await dispose(base);
  }
  const failed = await setupBase();
  try {
    dispatchClock(failed.drive, failed.clock, { kind: "run_failed", reason: "worker_failed" });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(failed.pipeline, failed.drive.state, failed.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await dispose(failed);
  }
  const cleanup = await setupBase();
  try {
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "start_agent_execution", stateId: "coder", profile: "coder", executionRole: "planning" });
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "agent_data_prepared" });
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "agent_execution_session_created", sessionId: "sess-1" });
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "agent_tool_session_created", sessionId: "tool-1" });
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "agent_running" });
    dispatchClock(cleanup.drive, cleanup.clock, {
      kind: "agent_failed",
      reason: "session_cleanup_failed",
      sessionCleanup: { execution: "failed", tool: "completed" },
    });
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "run_cleanup_failed" });
    expectRestoreError(
      await restorePipelineV2RuntimeContext(cleanup.pipeline, cleanup.drive.state, cleanup.runRoot).catch(
        (error) => error,
      ),
      "invalid_state",
    );
  } finally {
    await dispose(cleanup);
  }
});

test("30. the restoration mutates nothing on success or on any failure group", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const before = await fingerprint(base.root);
    await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(await fingerprint(base.root)).toBe(before);

    const broken = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    (broken["pipeline"] as Record<string, unknown>)["entry_state"] = "elsewhere";
    await restorePipelineV2RuntimeContext(base.pipeline, broken, base.runRoot).catch(() => undefined);
    expect(await fingerprint(base.root)).toBe(before);

    const reportPath = join(base.runRoot, "activations", "1-coder", "data", "outputs", "report");
    const damaged = JSON.parse(JSON.stringify(base.drive.state));
    const reportBytes = await readFile(reportPath);
    await writeFile(reportPath, "damaged\n");
    await restorePipelineV2RuntimeContext(base.pipeline, damaged, base.runRoot).catch(() => undefined);
    expect(await fingerprint(base.root)).not.toBe(before);
    await writeFile(reportPath, reportBytes);
    expect(await fingerprint(base.root)).toBe(before);
  } finally {
    await dispose(base);
  }
});

test("31. the result is deep-frozen and isolated from caller mutations", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const callerState = JSON.parse(JSON.stringify(base.drive.state));
    const context = await restorePipelineV2RuntimeContext(base.pipeline, callerState, base.runRoot);
    expect(Object.isFrozen(context)).toBe(true);
    expect(Object.isFrozen(context.state)).toBe(true);
    expect(Object.isFrozen(context.run_inputs)).toBe(true);
    expect(Object.isFrozen(context.cursor)).toBe(true);
    expect(Object.isFrozen(context.accepted_outputs)).toBe(true);
    expect(Object.isFrozen(context.accepted_outputs[0])).toBe(true);
    (callerState as Record<string, unknown>)["run_id"] = "mutated";
    expect(context.state.run_id).toBe(RUN_ID);
    expect(() => {
      (context as unknown as Record<string, unknown>)["next_execution_index"] = 42;
    }).toThrow();
  } finally {
    await dispose(base);
  }
});

test("32. repeated restoration is deterministic", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const first = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    const second = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(second).toEqual(first);
    expect(second.state).not.toBe(first.state);
  } finally {
    await dispose(base);
  }
});

test("33. diagnostics never carry bodies or canaries", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const factsPath = join(base.runRoot, "data", "inputs", "facts_seed");
    const malformed = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown>;
    await writeFile(factsPath, `${CANARY_BODY} not json`, { mode: 0o600 });
    const digest = await runInputSnapshotDigest("json", factsPath, "fixture");
    (malformed["inputs"] as Record<string, unknown>[])[1]!["digest"] = digest;
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, malformed, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "run_input_modified");
    expect((cause as Error).message).not.toContain(CANARY_BODY);
    expect((cause as Error).message).not.toContain("Unexpected token");
    expect((cause as Error).message).not.toContain("position");
  } finally {
    await dispose(base);
  }
});

test("34. the restore module classifies by phase, never by message text", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(join(import.meta.dir, "../src/pipeline_v2_resume_context.ts"), "utf8");
  expect(source.includes(".message.includes")).toBe(false);
  expect(source.includes(".message.match")).toBe(false);
  expect(source.includes(".message.startsWith")).toBe(false);
  expect(source.includes("new RegExp")).toBe(false);
});

test("35. the public export surface carries no registry, minter or test seams", async () => {
  const namespace = (await import("../src/pipeline_v2_resume_context.ts")) as Record<string, unknown>;
  expect(Object.keys(namespace).sort()).toEqual([
    "PipelineV2RuntimeContextRestoreError",
    "restorePipelineV2PlanningAcceptanceContext",
    "restorePipelineV2RuntimeContext",
  ]);
});

// --- 36-44. compiled-history verification (the engine-compatible journal) ----

/**
 * Clone a durable state document, mutate one committed transition of the
 * journal, and drive it back through the reducer's own loader contract:
 * the mutation must remain structurally valid (the loader accepts it) so
 * the compiled-history verifier is the only layer that rejects it. A
 * changed target therefore also moves the durable cursor — the loader's
 * joint replay follows recorded targets, while the compiled verifier walks
 * the pipeline's own compiled steps.
 */
function withMutatedTransition(
  state: PipelineV2RunState,
  ordinal: number,
  mutate: (transition: Record<string, unknown>) => void,
): PipelineV2RunState {
  const clone = JSON.parse(JSON.stringify(state)) as Record<string, unknown>;
  const transitions = clone["transitions"] as Record<string, unknown>[];
  const transition = transitions[ordinal]!;
  mutate(transition);
  // Keep the journal loader-coherent after a target change: the cursor
  // becomes the replayed end of the mutated chain.
  rewriteCursorFromTargets(clone);
  return clone as unknown as PipelineV2RunState;
}

/** Replays the recorded transition targets and rewrites the durable cursor accordingly. */
function rewriteCursorFromTargets(clone: Record<string, unknown>): void {
  const pipelineEntry = "coder";
  let cursor = pipelineEntry;
  for (const transition of clone["transitions"] as Record<string, unknown>[]) {
    cursor = transition["to"] as string;
  }
  (clone["cursor"] as Record<string, unknown>)["current_state"] = cursor;
}

test("36. a valid agent->decision->terminal history restores", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    await runAgentActivation(base, base.drive, "ship", "unused");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.state.transitions.map((transition) => `${transition.from}--${transition.outcome}-->${transition.to}`)).toEqual([
      "coder--completed-->check",
      "check--alpha-->ship",
      "ship--completed-->done",
    ]);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 3 });
  } finally {
    await dispose(base);
  }
});

test("37. a valid cycle/repeated-state history restores", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "beta");
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: false, f2: true }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const context = await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "ship", transition_count: 4 });
    expect(context.state.transitions[1]?.outcome).toBe("beta");
    expect(context.state.transitions[1]?.to).toBe("coder");
  } finally {
    await dispose(base);
  }
});

test("38. a structurally valid wrong target passes the loader but fails restore as pipeline_mismatch", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    // Structurally coherent journal: same state, outcome, index — but the
    // target names a different declared state. The loader accepts it.
    const mutated = withMutatedTransition(base.drive.state!, 1, (transition) => {
      transition["to"] = "done";
    });
    expect(() => validatePipelineV2RunState(mutated)).not.toThrow();
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
  } finally {
    await dispose(base);
  }
});

test("39. a structurally valid wrong transition_index passes the loader but fails restore", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    // alpha's declared index is 0; claim 1. Structurally valid, compiled-invalid.
    const mutated = withMutatedTransition(base.drive.state!, 1, (transition) => {
      transition["index"] = 1;
    });
    expect(() => validatePipelineV2RunState(mutated)).not.toThrow();
    const cause = expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    expect(cause.message).toContain("transition 2");
  } finally {
    await dispose(base);
  }
});

test("40. a declared outcome routed to another target with a foreign index is rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    // beta's declared step is (coder, index 1); record it as (done, index 0).
    runDecisionActivationWithStep(base, base.drive, "check", "beta", "done", 0);
    const cause = expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    expect(cause.message).toContain("does not match the compiled transition");
  } finally {
    await dispose(base);
  }
});

test("41. an outcome the compiled pipeline does not declare is rejected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    // The decision record claims a selected outcome the compiled model
    // neither declares as a decision id nor reserves; the transition
    // (structurally valid, loader-accepted) names it with a coherent
    // target. Only the compiled verifier rejects it.
    const executionIndex = (base.drive.state as PipelineV2RunState).executions.length + 1;
    dispatchClock(base.drive, base.clock, { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e"), executionRole: "control" });
    dispatchClock(base.drive, base.clock, {
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "mystery", decision: "mystery", rule_id: "R1", active_constraint_ids: [] },
    });
    dispatchClock(base.drive, base.clock, {
      kind: "transition_committed",
      step: { from: "check", outcome: "mystery", to: "done", transition_index: 0 },
      executionIndex,
    });
    const cause = expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, base.drive.state, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    expect(cause.message).toContain("which the compiled pipeline does not resolve");
  } finally {
    await dispose(base);
  }
});

test("42. damage in an old non-last transition is also detected", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "beta");
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: false, f2: true }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    await runAgentActivation(base, base.drive, "ship", "unused");
    // Corrupt the first transition (coder -> check): claim the wrong
    // transition index. The recorded targets stay coherent, so the loader
    // accepts the journal; the compiled verifier rejects the index.
    const mutated = withMutatedTransition(base.drive.state!, 0, (transition) => {
      transition["index"] = 7;
    });
    expect(() => validatePipelineV2RunState(mutated)).not.toThrow();
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
  } finally {
    await dispose(base);
  }
});

test("43. a compiled mismatch happens before any filesystem access", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const mutated = withMutatedTransition(base.drive.state!, 1, (transition) => {
      transition["to"] = "done";
    });
    const before = await fingerprint(base.root);
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(cause, "pipeline_mismatch");
    expect(await fingerprint(base.root)).toBe(before);
    // Stronger proof of ordering: strip the entire run tree (the layout
    // checks would fail as run_layout_invalid if they ran first) and
    // observe that the compiled mismatch still wins.
    await rm(join(base.runRoot, "project"), { recursive: true, force: true });
    await rm(join(base.runRoot, "data"), { recursive: true, force: true });
    const layout = await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch(
      (error) => error,
    );
    expectRestoreError(layout, "pipeline_mismatch");
  } finally {
    await dispose(base);
  }
});

test("44. compiled-mismatch diagnostics are content-free", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    runDecisionActivation(base, base.drive, "check", "alpha");
    const mutated = withMutatedTransition(base.drive.state!, 1, (transition) => {
      transition["to"] = "done";
    });
    const cause = expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    expect(cause.message).not.toContain(CANARY_BODY);
    expect(cause.message).not.toContain("TASK-BODY");
    expect(cause.message).not.toContain("f1");
    expect(cause.message).not.toContain("activations");
    expect(cause.message).not.toContain("cause");
    // input state and pipeline remain untouched by the verification
    const pipelineSnapshot = JSON.parse(JSON.stringify(base.pipeline));
    await restorePipelineV2RuntimeContext(base.pipeline, mutated, base.runRoot).catch(() => undefined);
    expect(JSON.parse(JSON.stringify(base.pipeline))).toEqual(pipelineSnapshot);
  } finally {
    await dispose(base);
  }
});

/**
 * Decision activation with an explicitly forged committed step: still
 * structurally valid for the reducer (which verifies outcome/decision
 * equality and cursor linkage, not compiled targets), but compiled-invalid.
 */
function runDecisionActivationWithStep(
  base: Base,
  drive: Drive,
  stateId: string,
  outcome: "alpha" | "beta" | "uncovered",
  to: string,
  transitionIndex: number,
): void {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, { kind: "start_decision_execution", stateId, inputDigest: hex("e"), ...startRoleArgs(base.pipeline, stateId, (drive.state as PipelineV2RunState)) });
  dispatchClock(drive, base.clock, {
    kind: "decision_evaluated",
    result: outcome === "uncovered"
      ? { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] }
      : { status: "selected", outcome, decision: outcome, rule_id: "R1", active_constraint_ids: [] },
  });
  dispatchClock(drive, base.clock, {
    kind: "transition_committed",
    step: { from: stateId, outcome, to, transition_index: transitionIndex },
    executionIndex,
  });
}

test("42. a durable execution role that differs from the compiled role is a pipeline mismatch", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const raw = JSON.stringify(base.drive.state);
    // the compiled orchestration assigns planning to coder; a forged
    // control role is loader-valid but compiled-incompatible
    const rawParsed = JSON.parse(raw) as { executions: [Record<string, unknown>] };
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, JSON.parse(JSON.stringify({
        ...(rawParsed as object),
        executions: [{ ...rawParsed.executions[0], execution_role: "control" }],
      })), base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    // a forged stage role without an open iteration is rejected by the
    // loader itself (the lifecycle biconditional fires first)
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, JSON.parse(JSON.stringify({
        ...(rawParsed as object),
        executions: [{ ...rawParsed.executions[0], execution_role: "stage", iteration_index: 1 }],
      })), base.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("43. a generation bound to a template the compiled pipeline does not declare is a pipeline mismatch", async () => {
  const base = await setupBase();
  try {
    // runAgentActivation binds the planning execution itself (it commits
    // the transition), so the generation can open after its plan
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    const draft = JSON.parse(JSON.stringify(base.drive.state)) as Record<string, unknown> & {
      plan_revisions: unknown[];
      generations: unknown[];
    };
    // a loader-valid plan revision accepted by the settled planning
    // execution, plus a generation bound to an undeclared template
    draft.plan_revisions = [
      { index: 1, revision: 1, sha256: "a".repeat(64), previous_sha256: null, origin_execution: 1 },
    ];
    draft.generations = [
      {
        index: 1,
        stage_id: "development",
        stage_position: 1,
        template_id: "ghost",
        plan_sha256: "a".repeat(64),
        initial_budget: 2,
        opened_transition_count: 1,
        iteration_count: 0,
        iterations: [],
      },
    ];
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, draft, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    // a generation whose plan digest was never accepted is rejected by the
    // loader's plan binding first
    const boundDraft = JSON.parse(JSON.stringify(draft)) as typeof draft;
    (boundDraft.generations[0] as Record<string, unknown>).plan_sha256 = "9".repeat(64);
    expectRestoreError(
      await restorePipelineV2RuntimeContext(base.pipeline, boundDraft, base.runRoot).catch((error) => error),
      "invalid_state",
    );
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

const STAGE_MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
decisions:
  - id: d_next_stage
  - id: d_close_stage
relations: []
constraints: []
rules:
  - id: rule-next
    when: {fact: f1, equals: true}
    decision: d_next_stage
  - id: rule-close
    when: {fact: f1, equals: false}
    decision: d_close_stage
`;

const STAGE_PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 12

inputs:
  - id: task
    type: file
    protected: true
  - id: facts
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dispatch
      role: control
    - state_id: dev
      role: stage
      stage_template: development
    - state_id: gate
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
        schema: schemas/facts.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dispatch
  - id: dispatch
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: dispatch_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: dev
      - outcome: d_close_stage
        to: halt
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: gate
  - id: gate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: gate_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: dev
      - outcome: d_close_stage
        to: done
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: done
    type: terminal
    result: success
  - id: halt
    type: terminal
    result: failed
`;

/**
 * The two-stage-template variant: the gate (development) routes its close
 * outcome into the second stage's entry state (test, template testing), so
 * one run can carry two generations with different templates; the reuse of
 * one template by several plan stages is exercised with the single-template
 * pipeline above.
 */
const STAGE_TWO_PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 12

inputs:
  - id: task
    type: file
    protected: true
  - id: facts
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev
    - id: testing
      entry_state: test
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
      stage_wait:
        reason: stage_iteration_completed
        actions:
          - continue_stage
          - revise_task
    - state_id: dispatch
      role: control
    - state_id: dev
      role: stage
      stage_template: development
    - state_id: gate
      role: stage
      stage_template: development
    - state_id: control2
      role: control
    - state_id: test
      role: stage
      stage_template: testing
    - state_id: testgate
      role: stage
      stage_template: testing

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/facts.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dispatch
  - id: dispatch
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: dispatch_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: dev
      - outcome: d_close_stage
        to: halt
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: gate
  - id: gate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: gate_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: dev
      - outcome: d_close_stage
        to: control2
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: control2
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: control2_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: test
      - outcome: d_close_stage
        to: halt
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: test
    type: agent
    profile: coder
    prompt: prompts/test.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: testgate
  - id: testgate
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: testgate_facts
        source:
          pipeline_input: facts
    transitions:
      - outcome: d_next_stage
        to: test
      - outcome: d_close_stage
        to: done
      - outcome: uncovered
        to: halt
      - outcome: inconsistent_facts
        to: halt
      - outcome: invalid_facts
        to: halt
  - id: done
    type: terminal
    result: success
  - id: halt
    type: terminal
    result: failed
`;

interface StageBase {
  root: string;
  pipeline: ResolvedPipelineV2;
  runRoot: string;
  runInputs: RunInputsSnapshot;
  clock: { value: number };
  drive: Drive;
}

async function setupStageBase(pipelineYaml: string = STAGE_PIPELINE): Promise<StageBase> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-resume-stage-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), pipelineYaml);
  await writeFile(join(bundle, "prompts", "architect.md"), "plan the task\n");
  await writeFile(join(bundle, "prompts", "dev.md"), "implement the stage\n");
  await writeFile(join(bundle, "prompts", "test.md"), "test the stage\n");
  await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(bundle, "decisions", "model.yaml"), STAGE_MODEL_YAML);
  const pipeline = await loadPipelineV2(bundle);

  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.md"), "TASK-BODY\n");
  await writeFile(join(sources, "facts.json"), JSON.stringify({ f1: true, f2: false }));

  const runRoot = join(root, "runs", RUN_ID);
  await mkdir(join(runRoot, "project"), { mode: 0o700, recursive: true });
  const runInputs = await snapshotRunInputs(pipeline, [
    { id: "task", path: join(sources, "task.md") },
    { id: "facts", path: join(sources, "facts.json") },
  ], runRoot);

  const clock = { value: 0 };
  const drive: Drive = { state: null, records: [] };
  dispatchClock(drive, clock, {
    kind: "create_run",
    runId: RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: runInputs.inputs.map((entry) => ({
      id: entry.id,
      type: entry.type,
      protected: entry.protected,
      digest: entry.digest,
    })),
  });
  return { root, pipeline, runRoot, runInputs, clock, drive };
}

function dispatchStage(base: StageBase, command: PipelineV2RunCommand): void {
  dispatchClock(base.drive, base.clock, command);
}

async function runStageAgentPhases(base: StageBase, stateId: string): Promise<number> {
  const executionIndex = (base.drive.state as PipelineV2RunState).executions.length + 1;
  dispatchStage(base, {
    kind: "start_agent_execution",
    stateId,
    profile: "coder",
    ...startRoleArgs(base.pipeline, stateId, base.drive.state as PipelineV2RunState),
  });
  for (const command of AGENT_PHASE_COMMANDS(`sess-${executionIndex}`, `tool-${executionIndex}`)) {
    dispatchStage(base, command);
  }
  const outputs: { id: string; digest: string }[] = [];
  if (stateId === "architect") {
    const prepared = await prepareActivationData(
      base.pipeline,
      base.runInputs,
      base.drive.records,
      stateId,
      executionIndex,
    );
    const outputsRoot = join(base.runRoot, "activations", `${executionIndex}-${stateId}`, "data", "outputs");
    await writeFile(join(outputsRoot, "plan"), JSON.stringify({ f1: true, f2: false }), { mode: 0o600 });
    const accepted = await acceptActivationOutputs(base.pipeline, prepared);
    for (const record of accepted) {
      outputs.push({ id: record.output, digest: record.digest });
    }
    base.drive.records.push(...accepted);
  }
  dispatchStage(base, { kind: "agent_outputs_accepted", outputs });
  dispatchStage(base, { kind: "agent_cleanup_completed" });
  return executionIndex;
}

function runStageDecision(
  base: StageBase,
  stateId: "dispatch" | "gate",
  outcome: "d_next_stage" | "d_close_stage",
): void {
  dispatchStage(base, { kind: "start_decision_execution", stateId, inputDigest: hex("e"), ...startRoleArgs(base.pipeline, stateId, base.drive.state as PipelineV2RunState) });
  dispatchStage(base, {
    kind: "decision_evaluated",
    result: { status: "selected", outcome, decision: outcome, rule_id: "R1", active_constraint_ids: [] },
  });
}

function commitStageTransition(
  base: StageBase,
  from: string,
  outcome: string,
  to: string,
  executionIndex: number,
  declaredIndex: number,
): void {
  dispatchStage(base, {
    kind: "transition_committed",
    step: { from, outcome, to, transition_index: declaredIndex },
    executionIndex,
  });
}

/**
 * The contract hook order at both stage boundaries: the control execution
 * settles unbound, then the generation and iteration open at its own
 * boundary, then the transition; the stage decision settles unbound, then
 * the iteration and the generation close at its own boundary, then the
 * transition. The run stops at the clean resumable boundary right after
 * the final transition.
 */
async function driveStageClosureBoundary(base: StageBase, closeBy: "normal_close" | "exhausted"): Promise<void> {
  await runStageAgentPhases(base, "architect");
  dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
  commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
  runStageDecision(base, "dispatch", "d_next_stage");
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
  commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
  await runStageAgentPhases(base, "dev");
  commitStageTransition(base, "dev", "completed", "gate", 3, 0);
  runStageDecision(base, "gate", "d_close_stage");
  dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: closeBy });
  dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
  commitStageTransition(base, "gate", "d_close_stage", "done", 4, 1);
}

/**
 * The same-boundary touching combination at the dev execution's start
 * boundary: one iteration closes and another opens at the same count.
 * `closedFirst` starts the dev execution inside the closed iteration
 * (before its closure); otherwise the closure and reopening happen before
 * the dev execution starts inside the reopened iteration.
 */
async function driveStageTouching(base: StageBase, closedFirst: boolean): Promise<void> {
  await runStageAgentPhases(base, "architect");
  dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
  commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
  runStageDecision(base, "dispatch", "d_next_stage");
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
  commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
  if (!closedFirst) {
    dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
    dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
  }
  await runStageAgentPhases(base, "dev");
  if (closedFirst) {
    dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
    dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 2, transitionCount: 2 });
  }
  commitStageTransition(base, "dev", "completed", "gate", 3, 0);
  runStageDecision(base, "gate", "d_close_stage");
  dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 2, by: "normal_close" });
  dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
  commitStageTransition(base, "gate", "d_close_stage", "done", 4, 1);
}

test("44. the contract-order closure boundary with a same-boundary normal_close passes the loader and the real restore verifier", async () => {
  const base = await setupStageBase();
  try {
    await driveStageClosureBoundary(base, "normal_close");
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 4 });
    expect(context.next_execution_index).toBe(5);
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("45. the same-boundary exhausted closure is restorable the same way", async () => {
  const base = await setupStageBase();
  try {
    await driveStageClosureBoundary(base, "exhausted");
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 4 });
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("46. the touching same-boundary pair restores by membership in the admissible candidate set (closed first)", async () => {
  const base = await setupStageBase();
  try {
    await driveStageTouching(base, true);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 4 });
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("47. the touching same-boundary pair restores by membership when the execution started in the reopened iteration", async () => {
  const base = await setupStageBase();
  try {
    await driveStageTouching(base, false);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 4 });
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

/**
 * The touching boundary between two generations with different templates,
 * with the dev execution started inside the OLD generation at that same
 * boundary (scenario 3).
 */
async function driveStageCrossGenerationOldExec(base: StageBase): Promise<void> {
  await runStageAgentPhases(base, "architect");
  dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
  commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
  runStageDecision(base, "dispatch", "d_next_stage");
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
  commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
  await runStageAgentPhases(base, "dev");
  dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
  dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "testing", stagePosition: 2, templateId: "testing", planSha256: hex("1"), initialBudget: 2, transitionCount: 2 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 });
  commitStageTransition(base, "dev", "completed", "gate", 3, 0);
}

/**
 * The touching generations with different templates and the test execution
 * inside the NEW generation (scenario 4): the second generation opens at
 * the same count the first one closes, the control state then routes into
 * the second stage's entry and its stage execution runs in the new
 * generation's iteration.
 */
async function driveStageCrossGenerationNewExec(base: StageBase): Promise<void> {
  await runStageAgentPhases(base, "architect");
  dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
  commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
  runStageDecision(base, "dispatch", "d_next_stage");
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
  commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
  await runStageAgentPhases(base, "dev");
  commitStageTransition(base, "dev", "completed", "gate", 3, 0);
  runStageDecision(base, "gate", "d_close_stage");
  dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
  dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "testing", stagePosition: 2, templateId: "testing", planSha256: hex("1"), initialBudget: 2, transitionCount: 3 });
  commitStageTransition(base, "gate", "d_close_stage", "control2", 4, 1);
  dispatchStage(base, { kind: "start_decision_execution", stateId: "control2", inputDigest: hex("e"), executionRole: "control" });
  dispatchStage(base, {
    kind: "decision_evaluated",
    result: { status: "selected", outcome: "d_next_stage", decision: "d_next_stage", rule_id: "R1", active_constraint_ids: [] },
  });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 4 });
  commitStageTransition(base, "control2", "d_next_stage", "test", 5, 0);
  await runStageAgentPhases(base, "test");
  commitStageTransition(base, "test", "completed", "testgate", 6, 0);
}

/**
 * One stage template reused by two plan stages: generation 2 opens with the
 * same template at the touching boundary; the dev execution started at that
 * boundary is a member of the admissible candidate set (scenario 5).
 */
async function driveStageReusedTemplate(base: StageBase): Promise<void> {
  await runStageAgentPhases(base, "architect");
  dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
  commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
  runStageDecision(base, "dispatch", "d_next_stage");
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "first_stage", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
  commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
  await runStageAgentPhases(base, "dev");
  dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
  dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
  dispatchStage(base, { kind: "stage_generation_opened", stageId: "second_stage", stagePosition: 2, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 2 });
  dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 2 });
  commitStageTransition(base, "dev", "completed", "gate", 3, 0);
}

test("48. the touching generations with different templates resolve the old generation's execution", async () => {
  const base = await setupStageBase(STAGE_TWO_PIPELINE);
  try {
    await driveStageCrossGenerationOldExec(base);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "gate", transition_count: 3 });
    expect(context.next_execution_index).toBe(4);
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("49. the touching generations with different templates resolve the new generation's execution", async () => {
  const base = await setupStageBase(STAGE_TWO_PIPELINE);
  try {
    await driveStageCrossGenerationNewExec(base);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "testgate", transition_count: 6 });
    expect(context.next_execution_index).toBe(7);
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("50. a reused template at a touching boundary passes the real restore verifier", async () => {
  const base = await setupStageBase();
  try {
    await driveStageReusedTemplate(base);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "gate", transition_count: 3 });
    expect(context.next_execution_index).toBe(4);
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

test("51. an execution whose recorded index matches no template-matching candidate is rejected", async () => {
  // The gate (template development) starts at boundary 3, where generation
  // 1's iteration closed at count 2 and only generation 2's iteration
  // (template testing) is open: the recorded index 1 matches a candidate,
  // but no candidate matches the (index, template) conjunction. The loader
  // accepts the document (it has no pipeline); the real restore verifier
  // rejects it.
  const base = await setupStageBase(STAGE_TWO_PIPELINE);
  try {
    await runStageAgentPhases(base, "architect");
    dispatchStage(base, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
    commitStageTransition(base, "architect", "completed", "dispatch", 1, 0);
    runStageDecision(base, "dispatch", "d_next_stage");
    dispatchStage(base, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
    dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
    commitStageTransition(base, "dispatch", "d_next_stage", "dev", 2, 0);
    await runStageAgentPhases(base, "dev");
    dispatchStage(base, { kind: "stage_iteration_closed", generationIndex: 1, iterationIndex: 1, by: "normal_close" });
    dispatchStage(base, { kind: "stage_generation_closed", generationIndex: 1, by: "next_stage" });
    commitStageTransition(base, "dev", "completed", "gate", 3, 0);
    dispatchStage(base, { kind: "stage_generation_opened", stageId: "testing", stagePosition: 2, templateId: "testing", planSha256: hex("1"), initialBudget: 2, transitionCount: 3 });
    dispatchStage(base, { kind: "stage_iteration_opened", generationIndex: 2, iterationIndex: 1, transitionCount: 3 });
    runStageDecision(base, "gate", "d_close_stage");
    commitStageTransition(base, "gate", "d_close_stage", "control2", 4, 1);
    const state = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    validatePipelineV2RunState(state);
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, state, base.runRoot).catch((error) => error);
    expectRestoreError(cause, "pipeline_mismatch");
    expect((cause as Error).message).toContain(
      'execution 4 records iteration 1, but no stage iteration open at its start boundary matches the stage template "development"',
    );
  } finally {
    await rm(base.root, { recursive: true, force: true });
  }
});

// --- answered-wait cursor relocation in the compiled replay -----------------

/**
 * Opens a wait at the given state with explicit declared actions (the
 * reducer validates safe ids and journal coherence, not compiled graph
 * existence), through the same reducer/dispatch helper every other test
 * uses — no hand-built state.
 */
function enterWaitWithActions(
  drive: Drive,
  clock: { value: number },
  stateId: string,
  actions: ReadonlyArray<{ id: string; to: string }>,
): void {
  dispatchClock(drive, clock, {
    kind: "run_waiting",
    stateId,
    reason: REASON,
    requestSha256: hex("e"),
    actions: actions.map((action) => ({ ...action })),
  });
}

test("52. an answered wait relocating the cursor to another declared state is restorable", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    // the graph transition brings the cursor to check; the wait opens there
    // and the chosen action routes to a DIFFERENT declared state (ship)
    enterWaitWithActions(base.drive, base.clock, "check", [{ id: "continue_stage", to: "ship" }]);
    respondWait(base.drive, base.clock, 1);
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    expect(durable.status).toBe("active");
    expect(durable.cursor).toEqual({ current_state: "ship", transition_count: 1 });
    expect(durable.transitions).toHaveLength(1);
    const context = await restorePipelineV2RuntimeContext(base.pipeline, durable, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "ship", transition_count: 1 });
    expect(context.next_execution_index).toBe(2);
  } finally {
    await dispose(base);
  }
});

test("53. the next graph transition after a wait relocation is verified from the relocated cursor", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    enterWaitWithActions(base.drive, base.clock, "check", [{ id: "continue_stage", to: "ship" }]);
    respondWait(base.drive, base.clock, 1);
    // a real execution and transition from the relocated cursor state
    await runAgentActivation(base, base.drive, "ship", "unused");
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    expect(durable.cursor).toEqual({ current_state: "done", transition_count: 2 });
    expect(durable.transitions[1]).toMatchObject({ from: "ship", outcome: "completed", to: "done" });
    const context = await restorePipelineV2RuntimeContext(base.pipeline, durable, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "done", transition_count: 2 });
    expect(context.next_execution_index).toBe(3);
  } finally {
    await dispose(base);
  }
});

test("54. several answered waits at one transition boundary replay in journal order", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    // check --(wait 1 response)--> ship --(wait 2 response)--> coder, both
    // waits anchored at the same committed transition count
    enterWaitWithActions(base.drive, base.clock, "check", [{ id: "continue_stage", to: "ship" }]);
    respondWait(base.drive, base.clock, 1);
    enterWaitWithActions(base.drive, base.clock, "ship", [{ id: "continue_stage", to: "coder" }]);
    respondWait(base.drive, base.clock, 2);
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    expect(durable.waits.map((wait) => [wait.index, wait.state_id, wait.response?.action_id])).toEqual([
      [1, "check", "continue_stage"],
      [2, "ship", "continue_stage"],
    ]);
    expect(durable.waits[0]?.transition_count).toBe(1);
    expect(durable.waits[1]?.transition_count).toBe(1);
    expect(durable.cursor).toEqual({ current_state: "coder", transition_count: 1 });
    const context = await restorePipelineV2RuntimeContext(base.pipeline, durable, base.runRoot);
    expect(context.cursor).toEqual({ current_state: "coder", transition_count: 1 });
  } finally {
    await dispose(base);
  }
});

test("55. a selected action target outside the compiled pipeline refuses before any filesystem access", async () => {
  // 55a: the selected target is not a declared state; the document stays
  // loader-valid, and the restore refuses with the typed pipeline_mismatch
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    enterWaitWithActions(base.drive, base.clock, "check", [{ id: "continue_stage", to: "ghost_state" }]);
    respondWait(base.drive, base.clock, 1);
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    expect(durable.cursor).toEqual({ current_state: "ghost_state", transition_count: 1 });
    const before = await fingerprint(base.root);
    const cause = await restorePipelineV2RuntimeContext(base.pipeline, durable, base.runRoot).catch((error) => error);
    expectRestoreError(cause, "pipeline_mismatch");
    expect((cause as Error).message).toContain("which the compiled pipeline does not declare");
    expect((cause as Error).message).not.toContain(CANARY_BODY);
    expect((cause as Error).message).not.toContain("TASK-BODY");
    expect(await fingerprint(base.root)).toBe(before);
  } finally {
    await dispose(base);
  }

  // 55b: an action targeting an undeclared state is fine while it is not
  // the selected one — only the selected target is checked
  const base2 = await setupBase();
  try {
    await runAgentActivation(base2, base2.drive, "coder", JSON.stringify({ f1: true, f2: false }));
    enterWaitWithActions(base2.drive, base2.clock, "check", [
      { id: "continue_stage", to: "ship" },
      { id: "other_action", to: "ghost_state" },
    ]);
    respondWait(base2.drive, base2.clock, 1);
    const durable = JSON.parse(JSON.stringify(base2.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    const context = await restorePipelineV2RuntimeContext(base2.pipeline, durable, base2.runRoot);
    expect(context.cursor).toEqual({ current_state: "ship", transition_count: 1 });
  } finally {
    await dispose(base2);
  }
});

// --- P. the planning-acceptance boundary restore (read-only) ------------------

import {
  restorePipelineV2PlanningAcceptanceContext,
  type RestoredPipelineV2PlanningAcceptanceContext,
} from "../src/pipeline_v2_resume_context.ts";

const PLAN_BYTES = JSON.stringify({ f1: true, f2: false });

/**
 * The honest settled-but-unbound planning execution: the same real
 * activation phases as `runAgentActivation` but no committed transition —
 * exactly the boundary the plan acceptance consumes.
 */
async function runUnboundPlanningActivation(
  base: Base,
  drive: Drive,
  stateId: string,
  planBytes: string,
): Promise<void> {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, {
    kind: "start_agent_execution",
    stateId,
    profile: "coder",
    ...startRoleArgs(base.pipeline, stateId, (drive.state as PipelineV2RunState)),
  });
  for (const command of AGENT_PHASE_COMMANDS(`sess-${executionIndex}`, `tool-${executionIndex}`)) {
    dispatchClock(drive, base.clock, command);
  }
  const prepared = await prepareActivationData(
    base.pipeline, base.runInputs, drive.records, stateId, executionIndex,
  );
  if (stateId !== "ship") {
    await writeAgentOutputs(base, stateId, executionIndex, planBytes);
  } else {
    await writeShipOutputs(base, executionIndex);
  }
  const accepted = await acceptActivationOutputs(base.pipeline, prepared);
  dispatchClock(drive, base.clock, {
    kind: "agent_outputs_accepted",
    outputs: accepted.map((record) => ({ id: record.output, digest: record.digest })),
  });
  drive.records.push(...accepted);
  dispatchClock(drive, base.clock, { kind: "agent_cleanup_completed" });
}

function expectPlanningAccepted(
  context: RestoredPipelineV2PlanningAcceptanceContext,
  expectedIndex: number,
  expectedCursor: { current_state: string; transition_count: number },
): void {
  expect(Object.keys(context).sort()).toEqual([
    "accepted_outputs",
    "cursor",
    "planning_execution_index",
    "run_inputs",
    "state",
  ]);
  expect(Object.isFrozen(context)).toBe(true);
  expect(Object.isFrozen(context.accepted_outputs)).toBe(true);
  expect(context.cursor).toEqual(expectedCursor);
  expect(context.planning_execution_index).toBe(expectedIndex);
  expect(Object.isFrozen(context.state)).toBe(true);
}

test("P1. the honest initial planning acceptance boundary: the old restore refuses, the new restore accepts read-only", async () => {
  const base = await setupBase();
  try {
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    // the ordinary resume restore keeps refusing the settled-unbound
    // execution with its exact former message
    const oldCause = await restorePipelineV2RuntimeContext(base.pipeline, durable, base.runRoot).catch((error) => error);
    expectRestoreError(oldCause, "invalid_state");
    expect((oldCause as Error).message).toBe(
      "the run carries a settled execution without its committed transition; it is not resumable",
    );
    const before = await fingerprint(base.root);
    const context = await restorePipelineV2PlanningAcceptanceContext(base.pipeline, durable, base.runRoot);
    expectPlanningAccepted(context, 1, { current_state: "coder", transition_count: 0 });
    // the accepted records are the exact execution-order × port-order
    // projection of the durable executions, and the json output reader
    // accepts them
    const agentExecution = durable.executions[0];
    if (agentExecution === undefined || agentExecution.type !== "agent" || agentExecution.outputs === undefined) {
      throw new Error("the durable execution carries no agent outputs");
    }
    const digest = agentExecution.outputs[0]!.digest;
    expect(context.accepted_outputs).toEqual([
      { state: "coder", output: "plan", activation_index: 1, digest },
      { state: "coder", output: "report", activation_index: 1, digest: agentExecution.outputs[1]!.digest },
    ]);
    const read = await readAcceptedJsonOutput(
      base.pipeline, base.runRoot, context.accepted_outputs, "coder", "plan", 1,
    );
    expect(read.value).toEqual({ f1: true, f2: false });
    // read-only on success
    expect(await fingerprint(base.root)).toBe(before);
  } finally {
    await dispose(base);
  }
});

test("P2. the replanning boundary after a restart: bound prefix, unbound planning execution is accepted", async () => {
  const base = await setupBase();
  try {
    await runAgentActivation(base, base.drive, "coder", PLAN_BYTES);
    runDecisionActivation(base, base.drive, "check", "beta");
    // coder re-entered through the decision's beta edge; the second
    // planning execution settles without its transition
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(durable.executions.length).toBe(3);
    expect(durable.transitions.length).toBe(2);
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    const before = await fingerprint(base.root);
    const context = await restorePipelineV2PlanningAcceptanceContext(base.pipeline, durable, base.runRoot);
    expectPlanningAccepted(context, 3, { current_state: "coder", transition_count: 2 });
    expect(context.cursor).toEqual({ current_state: "coder", transition_count: 2 });
    expect(context.accepted_outputs.length).toBe(4);
    expect(await fingerprint(base.root)).toBe(before);
  } finally {
    await dispose(base);
  }
});

test("P3. partial acceptance windows: the durable task ledger is not interpreted by the restore", async () => {
  for (const durableTasks of [0, 1, 2]) {
    const base = await setupBase();
    try {
      await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
      for (let index = 0; index < durableTasks; index++) {
        dispatchClock(base.drive, base.clock, {
          kind: "task_revision_accepted",
          taskId: index === 0 ? "task-a" : "task-b",
          revision: 1,
          taskSha256: hex(String(index + 1)),
        });
      }
      const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
      expect(() => validatePipelineV2RunState(durable)).not.toThrow();
      const context = await restorePipelineV2PlanningAcceptanceContext(base.pipeline, durable, base.runRoot);
      expectPlanningAccepted(context, 1, { current_state: "coder", transition_count: 0 });
      expect(context.accepted_outputs.length).toBe(2);
    } finally {
      await dispose(base);
    }
  }
});

test("P4. the completed-result-lost boundary: the durable target plan revision is accepted", async () => {
  const base = await setupBase();
  try {
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    dispatchClock(base.drive, base.clock, {
      kind: "task_revision_accepted", taskId: "task-a", revision: 1, taskSha256: hex("1"),
    });
    dispatchClock(base.drive, base.clock, {
      kind: "task_revision_accepted", taskId: "task-b", revision: 1, taskSha256: hex("2"),
    });
    dispatchClock(base.drive, base.clock, {
      kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("3"), originExecution: 1,
    });
    const durable = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    expect(() => validatePipelineV2RunState(durable)).not.toThrow();
    const context = await restorePipelineV2PlanningAcceptanceContext(base.pipeline, durable, base.runRoot);
    expectPlanningAccepted(context, 1, { current_state: "coder", transition_count: 0 });
  } finally {
    await dispose(base);
  }
});

test("P5. every non-planning boundary shape is a typed invalid_state", async () => {
  const base = await setupBase();
  try {
    // in-flight last execution
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    const settled = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    const inFlight = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState & {
      executions: [Record<string, unknown>];
    };
    inFlight.executions[0].phase = "running";
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(base.pipeline, inFlight, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    // failed last execution
    const failed = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState & {
      executions: [Record<string, unknown>];
    };
    failed.executions[0].phase = "failed";
    failed.executions[0].failure_reason = "worker_failed";
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(base.pipeline, failed, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    // a settled-unbound decision execution (the honest reducer path)
    const decisionBase = await setupBase();
    try {
      await runAgentActivation(decisionBase, decisionBase.drive, "coder", PLAN_BYTES);
      runDecisionActivationNoTransition(decisionBase, decisionBase.drive, "check", "alpha");
      const decisionDurable = JSON.parse(JSON.stringify(decisionBase.drive.state)) as PipelineV2RunState;
      expect(() => validatePipelineV2RunState(decisionDurable)).not.toThrow();
      expectRestoreError(
        await restorePipelineV2PlanningAcceptanceContext(decisionBase.pipeline, decisionDurable, decisionBase.runRoot).catch((error) => error),
        "invalid_state",
      );
    } finally {
      await dispose(decisionBase);
    }
    // a settled-unbound stage execution (the honest stage path)
    const stageBase = await setupStageBase();
    try {
      await runStageAgentPhases(stageBase, "architect");
      dispatchStage(stageBase, { kind: "plan_revision_accepted", planRevision: 1, planSha256: hex("1"), originExecution: 1 });
      commitStageTransition(stageBase, "architect", "completed", "dispatch", 1, 0);
      runStageDecision(stageBase, "dispatch", "d_next_stage");
      dispatchStage(stageBase, { kind: "stage_generation_opened", stageId: "development", stagePosition: 1, templateId: "development", planSha256: hex("1"), initialBudget: 2, transitionCount: 1 });
      dispatchStage(stageBase, { kind: "stage_iteration_opened", generationIndex: 1, iterationIndex: 1, transitionCount: 1 });
      commitStageTransition(stageBase, "dispatch", "d_next_stage", "dev", 2, 0);
      await runStageAgentPhases(stageBase, "dev");
      const stageDurable = JSON.parse(JSON.stringify(stageBase.drive.state)) as PipelineV2RunState;
      expect(() => validatePipelineV2RunState(stageDurable)).not.toThrow();
      expectRestoreError(
        await restorePipelineV2PlanningAcceptanceContext(stageBase.pipeline, stageDurable, stageBase.runRoot).catch((error) => error),
        "invalid_state",
      );
    } finally {
      await rm(stageBase.root, { recursive: true, force: true });
    }
    // an open last wait at the boundary: the honest reducer path refuses
    // to enter a wait before the unbound execution's transition is
    // committed, so the shape is forged; the typed invalid_state comes
    // from the loader or the boundary check, whichever fires first
    const waitBase = await setupBase();
    try {
      await runUnboundPlanningActivation(waitBase, waitBase.drive, "coder", PLAN_BYTES);
      const waitForged = JSON.parse(JSON.stringify(waitBase.drive.state)) as PipelineV2RunState;
      (waitForged.waits as unknown[]).push({
        index: 1,
        transition_count: 0,
        state_id: "coder",
        reason: "stage_iteration_limit_exhausted",
        request_sha256: hex("e"),
        actions: [{ id: "continue_stage", to: "coder" }],
      });
      expectRestoreError(
        await restorePipelineV2PlanningAcceptanceContext(waitBase.pipeline, waitForged, waitBase.runRoot).catch((error) => error),
        "invalid_state",
      );
    } finally {
      await dispose(waitBase);
    }
    // a reached terminal
    const terminalBase = await setupBase();
    try {
      await runAgentActivation(terminalBase, terminalBase.drive, "coder", PLAN_BYTES);
      runDecisionActivation(terminalBase, terminalBase.drive, "check", "alpha");
      await runAgentActivation(terminalBase, terminalBase.drive, "ship", "unused");
      dispatchClock(terminalBase.drive, terminalBase.clock, {
        kind: "terminal_reached",
        terminalStateId: "done",
        terminalResult: "success",
      });
      const terminalDurable = JSON.parse(JSON.stringify(terminalBase.drive.state)) as PipelineV2RunState;
      expect(() => validatePipelineV2RunState(terminalDurable)).not.toThrow();
      expectRestoreError(
        await restorePipelineV2PlanningAcceptanceContext(terminalBase.pipeline, terminalDurable, terminalBase.runRoot).catch((error) => error),
        "invalid_state",
      );
    } finally {
      await dispose(terminalBase);
    }
    // the final states
    for (const finalize of ["run_succeeded", "run_failed"] as const) {
      const finalBase = await setupBase();
      try {
        await runAgentActivation(finalBase, finalBase.drive, "coder", PLAN_BYTES);
        if (finalize === "run_succeeded") {
          runDecisionActivation(finalBase, finalBase.drive, "check", "alpha");
          await runAgentActivation(finalBase, finalBase.drive, "ship", "unused");
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "terminal_reached",
            terminalStateId: "done",
            terminalResult: "success",
          });
          dispatchClock(finalBase.drive, finalBase.clock, { kind: "run_outputs_published", outputs: [] });
          dispatchClock(finalBase.drive, finalBase.clock, { kind: "run_succeeded" });
        } else {
          // the canonical failed-terminal finalization: the uncovered edge
          // reaches the failed terminal, outputs publish, the run fails
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "start_decision_execution",
            stateId: "check",
            inputDigest: hex("e"),
            executionRole: "control",
          });
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "decision_evaluated",
            result: { status: "uncovered", outcome: "uncovered", active_constraint_ids: [] },
          });
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "transition_committed",
            step: { from: "check", outcome: "uncovered", to: "failed_end", transition_index: 2 },
            executionIndex: 2,
          });
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "terminal_reached",
            terminalStateId: "failed_end",
            terminalResult: "failed",
          });
          dispatchClock(finalBase.drive, finalBase.clock, { kind: "run_outputs_published", outputs: [] });
          dispatchClock(finalBase.drive, finalBase.clock, {
            kind: "run_failed",
            reason: "terminal_failed",
          });
        }
        const finalDurable = JSON.parse(JSON.stringify(finalBase.drive.state)) as PipelineV2RunState;
        expect(() => validatePipelineV2RunState(finalDurable)).not.toThrow();
        expectRestoreError(
          await restorePipelineV2PlanningAcceptanceContext(finalBase.pipeline, finalDurable, finalBase.runRoot).catch((error) => error),
          "invalid_state",
        );
      } finally {
        await dispose(finalBase);
      }
    }
  } finally {
    await dispose(base);
  }
});

test("P6. forged cursor, two unbound executions and planning role stay typed invalid_state; compiled role mismatch keeps pipeline_mismatch", async () => {
  const base = await setupBase();
  try {
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    const settled = JSON.parse(JSON.stringify(base.drive.state)) as PipelineV2RunState;
    // a forged cursor the durable executions do not sit on
    const cursorForged = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState;
    (cursorForged.cursor as { current_state: string }).current_state = "ghost";
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(base.pipeline, cursorForged, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    // two unbound executions
    const twice = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState & {
      executions: Array<Record<string, unknown>>;
    };
    twice.executions.push({ ...twice.executions[0], index: 2 });
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(base.pipeline, twice, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    // a durable non-planning role is refused by the boundary itself
    const controlForged = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState & {
      executions: [Record<string, unknown>];
    };
    controlForged.executions[0].execution_role = "control";
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(base.pipeline, controlForged, base.runRoot).catch((error) => error),
      "invalid_state",
    );
    // a durable planning role against a compiled stage role keeps the
    // former pipeline_mismatch classification
    const stageCoderBundle = join(base.root, "bundle-stage-coder");
    await mkdir(join(stageCoderBundle, "prompts"), { recursive: true });
    await mkdir(join(stageCoderBundle, "schemas"), { recursive: true });
    await mkdir(join(stageCoderBundle, "decisions"), { recursive: true });
    await writeFile(join(stageCoderBundle, "pipeline.yaml"), MAIN_PIPELINE.replace(
      "    - state_id: coder\n      role: planning\n      plan_output: plan\n      stage_wait:\n        reason: stage_iteration_completed\n        actions:\n          - continue_stage\n          - revise_task\n",
      "    - state_id: coder\n      role: stage\n      stage_template: development\n",
    ).replace("  stage_templates: []\n", "  stage_templates:\n    - id: development\n      entry_state: coder\n"));
    for (const prompt of ["coder.md", "ship.md"]) {
      await writeFile(join(stageCoderBundle, "prompts", prompt), "plan\n");
    }
    await writeFile(join(stageCoderBundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(stageCoderBundle, "decisions", "model.yaml"), MODEL_YAML);
    const stageCoderPipeline = await loadPipelineV2(stageCoderBundle);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(stageCoderPipeline, settled, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
    // an incompatible pipeline identity keeps pipeline_mismatch
    const foreignBundle = join(base.root, "bundle-foreign");
    await mkdir(join(foreignBundle, "prompts"), { recursive: true });
    await mkdir(join(foreignBundle, "schemas"), { recursive: true });
    await mkdir(join(foreignBundle, "decisions"), { recursive: true });
    await writeFile(join(foreignBundle, "pipeline.yaml"), MAIN_PIPELINE);
    await writeFile(join(foreignBundle, "prompts", "coder.md"), "implement the task\n");
    await writeFile(join(foreignBundle, "prompts", "ship.md"), "ship the task\n");
    await writeFile(join(foreignBundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
    await writeFile(join(foreignBundle, "decisions", "model.yaml"), MODEL_YAML);
    const foreignPipeline = await loadPipelineV2(foreignBundle);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(foreignPipeline, settled, base.runRoot).catch((error) => error),
      "pipeline_mismatch",
    );
  } finally {
    await dispose(base);
  }
});

test("P7. damaged run layout, inputs and accepted outputs keep their typed reasons; everything stays read-only", async () => {
  // a missing layout component
  const layoutBase = await setupBase();
  try {
    await runUnboundPlanningActivation(layoutBase, layoutBase.drive, "coder", PLAN_BYTES);
    const durable = JSON.parse(JSON.stringify(layoutBase.drive.state)) as PipelineV2RunState;
    await rm(join(layoutBase.runRoot, "data", "inputs"), { recursive: true });
    const before = await fingerprint(layoutBase.root);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(layoutBase.pipeline, durable, layoutBase.runRoot).catch((error) => error),
      "run_layout_invalid",
    );
    expect(await fingerprint(layoutBase.root)).toBe(before);
  } finally {
    await dispose(layoutBase);
  }
  // a corrupted run input
  const inputBase = await setupBase();
  try {
    await runUnboundPlanningActivation(inputBase, inputBase.drive, "coder", PLAN_BYTES);
    const durable = JSON.parse(JSON.stringify(inputBase.drive.state)) as PipelineV2RunState;
    await writeFile(join(inputBase.runRoot, "data", "inputs", "task"), "TAMPERED\n", { mode: 0o600 });
    const before = await fingerprint(inputBase.root);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(inputBase.pipeline, durable, inputBase.runRoot).catch((error) => error),
      "run_input_modified",
    );
    expect(await fingerprint(inputBase.root)).toBe(before);
  } finally {
    await dispose(inputBase);
  }
  // a corrupted accepted output
  const outputBase = await setupBase();
  try {
    await runUnboundPlanningActivation(outputBase, outputBase.drive, "coder", PLAN_BYTES);
    const durable = JSON.parse(JSON.stringify(outputBase.drive.state)) as PipelineV2RunState;
    await writeFile(
      join(outputBase.runRoot, "activations", "1-coder", "data", "outputs", "report"),
      "TAMPERED\n", { mode: 0o600 },
    );
    const before = await fingerprint(outputBase.root);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(outputBase.pipeline, durable, outputBase.runRoot).catch((error) => error),
      "accepted_output_modified",
    );
    expect(await fingerprint(outputBase.root)).toBe(before);
  } finally {
    await dispose(outputBase);
  }
  // a representative compiled mismatch happens before any filesystem access
  const mismatchBase = await setupBase();
  try {
    await runUnboundPlanningActivation(mismatchBase, mismatchBase.drive, "coder", PLAN_BYTES);
    const settled = JSON.parse(JSON.stringify(mismatchBase.drive.state)) as PipelineV2RunState;
    const forged = JSON.parse(JSON.stringify(settled)) as PipelineV2RunState & {
      executions: [Record<string, unknown>];
    };
    forged.executions[0].execution_role = "control";
    const before = await fingerprint(mismatchBase.root);
    expectRestoreError(
      await restorePipelineV2PlanningAcceptanceContext(mismatchBase.pipeline, forged, mismatchBase.runRoot).catch((error) => error),
      "invalid_state",
    );
    expect(await fingerprint(mismatchBase.root)).toBe(before);
  } finally {
    await dispose(mismatchBase);
  }
});

test("P8. a proxy pipeline is rejected before any state read or filesystem effect", async () => {
  const base = await setupBase();
  try {
    await runUnboundPlanningActivation(base, base.drive, "coder", PLAN_BYTES);
    let stateTraps = 0;
    const stateProxy = new Proxy(base.drive.state!, {
      get(target, prop, receiver) {
        stateTraps += 1;
        return Reflect.get(target as object, prop, receiver);
      },
    });
    const sentinel = join(base.runRoot, "sentinel");
    await writeFile(sentinel, "sentinel\n");
    const cause = await restorePipelineV2PlanningAcceptanceContext(
      new Proxy(base.pipeline, {}),
      stateProxy,
      base.runRoot,
    ).catch((error) => error);
    expect(cause).toBeInstanceOf(PipelineError);
    expect(cause).not.toBeInstanceOf(PipelineV2RuntimeContextRestoreError);
    expect((cause as Error).message).toContain("hand-built objects");
    expect(stateTraps).toBe(0);
    expect(await readFile(sentinel, "utf8")).toBe("sentinel\n");
  } finally {
    await dispose(base);
  }
});

test("P9. the runtime export surface carries the class and both restore functions", async () => {
  const namespace = (await import("../src/pipeline_v2_resume_context.ts")) as Record<string, unknown>;
  expect(Object.keys(namespace).sort()).toEqual([
    "PipelineV2RuntimeContextRestoreError",
    "restorePipelineV2PlanningAcceptanceContext",
    "restorePipelineV2RuntimeContext",
  ]);
});

/** A settled-unbound decision execution through the honest reducer path. */
function runDecisionActivationNoTransition(
  base: Base,
  drive: Drive,
  stateId: string,
  outcome: "alpha" | "beta",
): void {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, { kind: "start_decision_execution", stateId, inputDigest: hex("e"), ...startRoleArgs(base.pipeline, stateId, (drive.state as PipelineV2RunState)) });
  dispatchClock(drive, base.clock, {
    kind: "decision_evaluated",
    result: {
      status: "selected",
      outcome,
      decision: outcome,
      rule_id: "R1",
      active_constraint_ids: [],
    },
  });
}

test("P10. the planning restore shares the single core: no second mechanism, no foreign imports", async () => {
  const source = await readFile(
    join(import.meta.dir, "..", "src", "pipeline_v2_resume_context.ts"),
    "utf8",
  );
  const count = (needle: string): number => source.split(needle).length - 1;
  // one state-validation chain, one accepted-record reconstruction, one
  // shared verification call — each defined and invoked exactly once
  expect(count("validatePipelineV2RunState(")).toBe(1);
  expect(count("function reconstructAcceptedRecords(")).toBe(1);
  expect(count("reconstructAcceptedRecords(")).toBe(2);
  expect(count("verifyRestoredAcceptedHistory(")).toBe(1);
  expect(count("mintRestoredRunInputsSnapshot(")).toBe(1);
  expect(count("requireResolvedPipelineV2Provenance(")).toBe(1);
  // both entrypoints delegate to the one core; the two boundary policies
  // are the only per-entrypoint difference
  expect(count("restoreContextCore(")).toBe(3);
  expect(count("checkResumableBoundary")).toBe(2);
  expect(count("checkPlanningAcceptanceBoundary")).toBe(2);
  // no second restore machinery and no foreign layer imports
  const code = source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .split("\n")
    .filter((line) => !line.trimStart().startsWith("//"))
    .join("\n");
  expect(code).not.toContain("pipeline_v2_run_plan_store");
  expect(code).not.toContain("pipeline_v2_run_plan_controller");
  expect(code).not.toContain("pipeline_v2_coordinator");
  expect(code).not.toContain("pipeline_v2_runner");
  expect(code).not.toContain("preparePipelineV2RunPlanCandidate");
  expect(code).not.toContain("loadPipelineV2PlanRevision");
  expect(code).not.toContain("new WeakMap");
  expect(code).not.toContain("new WeakSet");
});
