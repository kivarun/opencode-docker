import { expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { PipelineError } from "../src/pipeline.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  reducePipelineV2RunCommand,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  evaluateDecisionStateFromData,
  prepareActivationData,
  runInputSnapshotDigest,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
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
    outputs: []
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

async function runAgentActivation(
  base: Base,
  drive: Drive,
  stateId: string,
  planBytes: string,
): Promise<void> {
  const executionIndex = (drive.state as PipelineV2RunState).executions.length + 1;
  dispatchClock(drive, base.clock, { kind: "start_agent_execution", stateId, profile: "coder" });
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
  if (stateId === "coder") {
    await writeAgentOutputs(base, stateId, executionIndex, planBytes);
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
  dispatchClock(drive, base.clock, { kind: "start_decision_execution", stateId, inputDigest: hex("e") });
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
      transition_index: 0,
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
    }
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
      { kind: "start_agent_execution", stateId: "coder", profile: "coder" },
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
    dispatchClock(base.drive, base.clock, { kind: "start_decision_execution", stateId: "check", inputDigest: hex("e") });
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
    dispatchClock(cleanup.drive, cleanup.clock, { kind: "start_agent_execution", stateId: "coder", profile: "coder" });
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
    "restorePipelineV2RuntimeContext",
  ]);
});
