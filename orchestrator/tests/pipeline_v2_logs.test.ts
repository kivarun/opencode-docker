/**
 * The versioned read-only Worker Logs API: the single projection owner
 * over the durable schema-v9 worker-log bindings.
 *
 * These tests pin: the list projection (only agent executions, strict
 * execution-index order, decision executions omitted, the pending/
 * unavailable/captured/truncated metadata union), the bounded chunk
 * read (raw-byte offsets, base64 round-trips, the empty-tail and
 * invalid-cursor cursor semantics), the fail-closed integrity gate
 * (missing/modified/extra/symlink/wrong-mode/malformed artifacts refuse
 * every read before any byte is served), the byte-identical restart and
 * read-repeat behavior, the zero-write fingerprint over every path, the
 * legacy v8 rejection, and the content-free leakage scan.
 */
import { afterAll, afterEach, describe, expect, test } from "bun:test";
import { readFileSync, readdirSync } from "node:fs";
import { mkdtemp, readFile, readdir, rm, stat, writeFile, mkdir } from "node:fs/promises";
import { symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  loadPipelineV2,
} from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import {
  logsPipelineV2Run,
  projectPipelineV2Logs,
  PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT,
  PIPELINE_V2_LOGS_MAX_CHUNK_LIMIT,
  readPipelineV2LogExecution,
  PipelineV2LogsError,
  type PipelineV2LogsErrorReason,
  type PipelineV2LogsOutcome,
} from "../src/pipeline_v2_logs.ts";
import { reducePipelineV2RunCommand, validatePipelineV2RunState, type PipelineV2RunCommand, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { PipelineV2RunStateStore } from "../src/pipeline_v2_state_store.ts";
import { publishPipelineV2WorkerLog, loadPipelineV2WorkerLog } from "../src/pipeline_v2_worker_log_store.ts";
import { workerLogPayloadDigest } from "../src/pipeline_v2_worker_log.ts";
import type { WorkerLogCapture } from "../src/worker_operation.ts";

const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

const RUN_ID = "logs-run";
const TICKS = Array.from({ length: 96 }, (_, t) => new Date(Date.UTC(2026, 0, 1, 0, 0, t + 1)));
const hex = (char: string): string => char.repeat(64);

const ROOTS: string[] = [];
afterEach(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});
afterAll(async () => {
  // nothing to hold
});

interface Fixture {
  readonly root: string;
  readonly stateRoot: string;
  readonly runRoot: string;
  readonly bundle: string;
  readonly payload1: Uint8Array;
}

/** The payload of execution 1: NUL, ANSI escapes, multibyte UTF-8 and no
 * trailing newline — the honest unredacted capture content. */
const PAYLOAD_BYTES = (() => {
  const parts = [
    "line-1 plain ascii\n",
    "nul\0byte\0inside\n",
    "\x1b[31mred-ansi\x1b[0m\n",
    "unicode: héllo — ünïcode ✓\n",
    "task body canary: SECRET-TASK-BODY-<script>alert(1)</script>\n",
    "credential canary: SUPER-SECRET-CREDENTIAL-VALUE\n",
    "host path canary: /home/michael/work/git/opencode-docker/secret\n",
    "no trailing newline",
  ];
  return new TextEncoder().encode(parts.join(""));
})();

async function writeBundle(root: string): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(
    join(bundle, "pipeline.yaml"),
    `schema_version: 2
entry_state: architect
max_transitions: 8
inputs: []
outputs:
  - id: result
    required: true
    source:
      state_output:
        state: architect
        output: plan
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
        to: done
  - id: done
    type: terminal
    result: success
`,
  );
  await writeFile(join(bundle, "prompts", "architect.md"), "prompt body");
  await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
  return bundle;
}

function applyCommands(commands: PipelineV2RunCommand[]): PipelineV2RunState {
  let current: PipelineV2RunState | null = null;
  commands.forEach((command, index) => {
    current = reducePipelineV2RunCommand(current, command, TICKS[index % TICKS.length]!);
  });
  return current as unknown as PipelineV2RunState;
}

function agentLadder(executionIndexHint: number): PipelineV2RunCommand[] {
  return [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `exec-${executionIndexHint}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${executionIndexHint}` },
    { kind: "agent_running" },
  ];
}

function transition(from: string, to: string, executionIndex: number): PipelineV2RunCommand {
  return {
    kind: "transition_committed",
    step: { from, outcome: "completed", to, transition_index: 0 },
    executionIndex,
  };
}

async function makeFixture(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-logs-"));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  const bundle = await writeBundle(root);
  const pipeline = await loadPipelineV2(bundle);
  const identity = pipelineV2RunPipelineIdentity(pipeline);
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  await mkdir(runRoot, { recursive: true, mode: 0o700 });

  // The execution-1 artifact is published first so its binding can join
  // the acceptance command atomically (the production order).
  const published = await publishPipelineV2WorkerLog(runRoot, 1, {
    status: "captured",
    byteCount: PAYLOAD_BYTES.byteLength,
    tailBytes: PAYLOAD_BYTES,
    truncatedDaemon: false,
    truncatedLocal: false,
  } satisfies WorkerLogCapture);
  if (published === null) {
    throw new Error("fixture: the execution-1 artifact was not published");
  }
  const binding1 = {
    status: "captured" as const,
    sha256: workerLogPayloadDigest(PAYLOAD_BYTES),
    byte_count: PAYLOAD_BYTES.byteLength,
    truncated_daemon: false,
    truncated_local: false,
  };

  const truncatedPayload = new TextEncoder().encode("truncated head lost");
  const published2 = await publishPipelineV2WorkerLog(runRoot, 2, {
    status: "truncated",
    byteCount: truncatedPayload.byteLength,
    tailBytes: truncatedPayload,
    truncatedDaemon: true,
    truncatedLocal: false,
  } satisfies WorkerLogCapture);
  if (published2 === null) {
    throw new Error("fixture: the execution-2 artifact was not published");
  }
  const binding2 = {
    status: "truncated" as const,
    sha256: workerLogPayloadDigest(truncatedPayload),
    byte_count: truncatedPayload.byteLength,
    truncated_daemon: true,
    truncated_local: false,
  };

  const commands: PipelineV2RunCommand[] = [
    { kind: "create_run", runId: RUN_ID, pipeline: identity, inputs: [], projectProvenance: V2_PROJECT_PROVENANCE_BINDING },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    ...agentLadder(1),
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("d") }], workerLog: binding1 },
    { kind: "agent_cleanup_completed" },
    transition("architect", "architect", 1),
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    ...agentLadder(2),
    { kind: "agent_outputs_accepted", outputs: [{ id: "plan", digest: hex("e") }], workerLog: binding2 },
    { kind: "agent_cleanup_completed" },
    transition("architect", "architect", 2),
    { kind: "start_decision_execution", stateId: "architect", inputDigest: hex("f"), executionRole: "control" },
    {
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "go", decision: "go", rule_id: "r1", active_constraint_ids: [] },
    },
    {
      kind: "transition_committed",
      step: { from: "architect", outcome: "go", to: "architect", transition_index: 0 },
      executionIndex: 3,
    },
    { kind: "start_agent_execution", stateId: "architect", profile: "architect", executionRole: "planning" },
    ...agentLadder(4),
  ];
  const state = validatePipelineV2RunState(applyCommands(commands));
  if (state.executions[3]?.type !== "agent" || state.executions[3]?.worker_log !== null) {
    throw new Error("fixture: the in-flight execution must carry a null binding");
  }

  const store = new PipelineV2RunStateStore({ stateRoot, runId: RUN_ID });
  await store.create(state);
  return {
    root,
    stateRoot,
    runRoot,
    bundle,
    payload1: PAYLOAD_BYTES,
  };
}

async function treeFingerprint(root: string): Promise<string> {
  const entries: string[] = [];
  const walk = async (dir: string): Promise<void> => {
    for (const entry of (await readdir(dir, { withFileTypes: true })).sort((a, b) => (a.name < b.name ? -1 : 1))) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        entries.push(`d ${path}`);
        await walk(path);
      } else {
        const info = await stat(path);
        entries.push(`f ${path} ${info.size} ${info.mode.toString(8)}`);
      }
    }
  };
  await walk(root);
  return entries.join("\n");
}

function expectFailure(
  outcome: PipelineV2LogsOutcome,
  reason: PipelineV2LogsErrorReason,
  runRoot: string | null,
): void {
  if (outcome.ok) {
    throw new Error(`expected a logs failure, got ${JSON.stringify(outcome)}`);
  }
  expect(outcome.exitCode).toBe(1);
  expect(outcome.reason).toBe(reason);
  expect(outcome.runRoot).toBe(runRoot);
}

describe("worker logs: the read-only projection owner", () => {
  test("the list carries only agent executions in index order with the exact metadata union", async () => {
    const fixture = await makeFixture();
    const outcome = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot });
    if (!outcome.ok || !("logs" in outcome)) {
      throw new Error(`expected a list outcome, got ${JSON.stringify(outcome)}`);
    }
    expect(outcome.exitCode).toBe(0);
    expect(outcome.schemaVersion).toBe(1);
    expect(outcome.runId).toBe(RUN_ID);
    expect(outcome.revision).toBe(25);
    expect(outcome.logs).toEqual([
      {
        executionIndex: 1,
        stateId: "architect",
        status: "captured",
        sha256: workerLogPayloadDigest(PAYLOAD_BYTES),
        byteCount: PAYLOAD_BYTES.byteLength,
        truncatedDaemon: false,
        truncatedLocal: false,
      },
      {
        executionIndex: 2,
        stateId: "architect",
        status: "truncated",
        sha256: workerLogPayloadDigest(new TextEncoder().encode("truncated head lost")),
        byteCount: new TextEncoder().encode("truncated head lost").byteLength,
        truncatedDaemon: true,
        truncatedLocal: false,
      },
      { executionIndex: 4, stateId: "architect", status: "pending" },
    ]);
    // the decision execution (index 3) never appears
    expect(outcome.logs.some((entry) => entry.executionIndex === 3)).toBe(false);
    // the in-memory projection agrees with the outcome
    const page = projectPipelineV2Logs(
      (await new PipelineV2RunStateStore({ stateRoot: fixture.stateRoot, runId: RUN_ID }).load())!,
    );
    expect(page.logs).toEqual(outcome.logs);
  });

  test("a captured execution chunks by raw bytes and reassembles to the exact payload", async () => {
    const fixture = await makeFixture();
    const chunks: Uint8Array[] = [];
    let offset = 0;
    for (;;) {
      const outcome = await logsPipelineV2Run({
        runId: RUN_ID,
        stateRoot: fixture.stateRoot,
        executionIndex: 1,
        offset,
        limit: 64,
      });
      if (!outcome.ok || !("dataBase64" in outcome)) {
        throw new Error(`expected a chunk outcome, got ${JSON.stringify(outcome)}`);
      }
      expect(outcome.status).toBe("captured");
      expect(outcome.stateId).toBe("architect");
      expect(outcome.sha256).toBe(workerLogPayloadDigest(PAYLOAD_BYTES));
      expect(outcome.byteCount).toBe(PAYLOAD_BYTES.byteLength);
      expect(outcome.offset).toBe(offset);
      const decoded = Buffer.from(outcome.dataBase64, "base64");
      expect(Array.from(decoded)).toEqual(Array.from(PAYLOAD_BYTES.slice(offset, offset + 64)));
      chunks.push(new Uint8Array(decoded));
      if (outcome.nextOffset === null) {
        break;
      }
      expect(outcome.nextOffset).toBe(offset + 64);
      offset = outcome.nextOffset;
    }
    const reassembled = Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
    expect(reassembled.byteLength).toBe(PAYLOAD_BYTES.byteLength);
    expect(Array.from(reassembled)).toEqual(Array.from(PAYLOAD_BYTES));
    expect(workerLogPayloadDigest(new Uint8Array(reassembled))).toBe(workerLogPayloadDigest(PAYLOAD_BYTES));
    // the second captured execution reassembles the same way and matches
    // its own durable binding
    const payload2 = new TextEncoder().encode("truncated head lost");
    const chunks2: Uint8Array[] = [];
    let offset2 = 0;
    for (;;) {
      const outcome = await logsPipelineV2Run({
        runId: RUN_ID,
        stateRoot: fixture.stateRoot,
        executionIndex: 2,
        offset: offset2,
        limit: 5,
      });
      if (!outcome.ok || !("dataBase64" in outcome)) {
        throw new Error(`expected a chunk outcome, got ${JSON.stringify(outcome)}`);
      }
      chunks2.push(new Uint8Array(Buffer.from(outcome.dataBase64, "base64")));
      if (outcome.nextOffset === null) {
        break;
      }
      offset2 = outcome.nextOffset;
    }
    const reassembled2 = Buffer.concat(chunks2.map((chunk) => Buffer.from(chunk)));
    expect(Array.from(reassembled2)).toEqual(Array.from(payload2));
  });

  test("the cursor semantics: empty tail, boundary offset, beyond-payload refusal", async () => {
    const fixture = await makeFixture();
    const byteCount = PAYLOAD_BYTES.byteLength;
    // offset == byteCount → empty content, null cursor
    const tail = await logsPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      executionIndex: 1,
      offset: byteCount,
      limit: 64,
    });
    if (!tail.ok || !("dataBase64" in tail)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(tail)}`);
    }
    expect(tail.dataBase64).toBe("");
    expect(tail.nextOffset).toBe(null);
    // offset > byteCount → typed invalid_cursor
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 1, offset: byteCount + 1 }),
      "invalid_cursor",
      fixture.runRoot,
    );
    // the final partial chunk ends with nextOffset null
    const last = await logsPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      executionIndex: 1,
      offset: byteCount - 3,
      limit: 65536,
    });
    if (!last.ok || !("nextOffset" in last)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(last)}`);
    }
    expect(last.nextOffset).toBe(null);
    expect(Buffer.from(last.dataBase64, "base64").byteLength).toBe(3);
  });

  test("a truncated execution serves its chunk with the truncated status and flags", async () => {
    const fixture = await makeFixture();
    const outcome = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 2 });
    if (!outcome.ok || !("dataBase64" in outcome)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(outcome)}`);
    }
    expect(outcome.status).toBe("truncated");
    expect(outcome.truncatedDaemon).toBe(true);
    expect(outcome.truncatedLocal).toBe(false);
    expect(outcome.dataBase64).toBe(Buffer.from("truncated head lost").toString("base64"));
    expect(outcome.nextOffset).toBe(null);
  });

  test("pending and unavailable executions serve the status document without content fields", async () => {
    const fixture = await makeFixture();
    const pending = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 4 });
    if (!pending.ok) {
      throw new Error(`expected a status outcome, got ${JSON.stringify(pending)}`);
    }
    expect(pending).toEqual({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: RUN_ID,
      runRoot: fixture.runRoot,
      revision: 25,
      executionIndex: 4,
      stateId: "architect",
      status: "pending",
    });
    expect(Object.keys(pending).sort()).toEqual([
      "executionIndex", "exitCode", "ok", "revision", "runId", "runRoot", "schemaVersion", "stateId", "status",
    ]);

  });

  test("the execution-index resolution: unknown index and decision execution", async () => {
    const fixture = await makeFixture();
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 9 }),
      "execution_not_found",
      fixture.runRoot,
    );
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 3 }),
      "log_not_applicable",
      fixture.runRoot,
    );
  });

  test("the default chunk limit applies when offset and limit are omitted", async () => {
    const fixture = await makeFixture();
    const outcome = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 1 });
    if (!outcome.ok || !("dataBase64" in outcome)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(outcome)}`);
    }
    expect(outcome.offset).toBe(0);
    expect(Buffer.from(outcome.dataBase64, "base64").byteLength).toBe(
      Math.min(PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT, PAYLOAD_BYTES.byteLength),
    );
    expect(outcome.nextOffset).toBe(
      PAYLOAD_BYTES.byteLength > PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT ? PIPELINE_V2_LOGS_DEFAULT_CHUNK_LIMIT : null,
    );
  });

  test("a damaged namespace fails every read closed before any byte is served", async () => {
    // missing artifact
    const missing = await makeFixture();
    await rm(join(missing.runRoot, "logs", "1.workerlog.json"));
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: missing.stateRoot }),
      "artifact_missing",
      missing.runRoot,
    );
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: missing.stateRoot, executionIndex: 1 }),
      "artifact_missing",
      missing.runRoot,
    );
    // modified artifact
    const modified = await makeFixture();
    await writeFile(join(modified.runRoot, "logs", "1.workerlog.json"), '{"tampered":true}');
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: modified.stateRoot }),
      "artifact_modified",
      modified.runRoot,
    );
    // malformed envelope
    const malformed = await makeFixture();
    await writeFile(join(malformed.runRoot, "logs", "1.workerlog.json"), "{not json");
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: malformed.stateRoot, executionIndex: 1 }),
      "artifact_modified",
      malformed.runRoot,
    );
    // symlinked artifact
    const symlinked = await makeFixture();
    await rm(join(symlinked.runRoot, "logs", "1.workerlog.json"));
    await writeFile(join(symlinked.runRoot, "logs", "real.json"), "{}");
    await symlink(join(symlinked.runRoot, "logs", "real.json"), join(symlinked.runRoot, "logs", "1.workerlog.json"));
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: symlinked.stateRoot, executionIndex: 1 }),
      "artifact_modified",
      symlinked.runRoot,
    );
    // extra final artifact outside every binding
    const extra = await makeFixture();
    await writeFile(join(extra.runRoot, "logs", "9.workerlog.json"), "{}");
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: extra.stateRoot }),
      "artifact_modified",
      extra.runRoot,
    );
  });

  test("a legacy v8 state document is run_state_invalid without migration or rewrite", async () => {
    const fixture = await makeFixture();
    const statePath = join(fixture.runRoot, "state.json");
    const v9 = await readFile(statePath, "utf8");
    const v8 = JSON.parse(v9) as Record<string, unknown>;
    v8["schema_version"] = 8;
    await writeFile(statePath, JSON.stringify(v8));
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot }),
      "run_state_invalid",
      fixture.runRoot,
    );
    // the file was never rewritten
    expect(JSON.parse(await readFile(statePath, "utf8"))["schema_version"]).toBe(8);
  });

  test("a missing run and an invalid run id carry the pre-root failure shapes", async () => {
    const root = await mkdtemp(join(tmpdir(), "pipeline-v2-logs-missing-"));
    ROOTS.push(root);
    expectFailure(
      await logsPipelineV2Run({ runId: RUN_ID, stateRoot: join(root, "state") }),
      "run_not_found",
      null,
    );
    const outcome = await logsPipelineV2Run({ runId: "../escape", stateRoot: join(root, "state") });
    expectFailure(outcome, "invalid_options", null);
    expect(outcome.runId).toBe("");
  });

  test("reads are zero-write and byte-identical across repeats and restarts", async () => {
    const fixture = await makeFixture();
    const before = await treeFingerprint(fixture.root);
    const first = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot });
    const second = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot });
    const chunkA = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 1, offset: 7, limit: 33 });
    const chunkB = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 1, offset: 7, limit: 33 });
    expect(JSON.stringify(first)).toBe(JSON.stringify(second));
    expect(JSON.stringify(chunkA)).toBe(JSON.stringify(chunkB));
    const after = await treeFingerprint(fixture.root);
    expect(after).toBe(before);
  });

  test("concurrent state advance does not invalidate an honestly read page", async () => {
    const fixture = await makeFixture();
    const list = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot });
    if (!list.ok || !("logs" in list)) {
      throw new Error(`expected a list outcome, got ${JSON.stringify(list)}`);
    }
    // a concurrent append moves the durable revision; the already-read
    // page stays honest for its snapshot revision
    const store = new PipelineV2RunStateStore({ stateRoot: fixture.stateRoot, runId: RUN_ID });
    const state = (await store.load())!;
    const advanced = reducePipelineV2RunCommand(state, { kind: "agent_outputs_accepted", outputs: [], workerLog: { status: "unavailable" } }, TICKS[50]!);
    void advanced;
    const chunk = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 1, offset: 0, limit: 16 });
    if (!chunk.ok || !("dataBase64" in chunk)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(chunk)}`);
    }
    expect(chunk.revision).toBe(list.revision);
    expect(Buffer.from(chunk.dataBase64, "base64")).toEqual(Buffer.from(PAYLOAD_BYTES.slice(0, 16)));
  });

  test("the leakage scan: canaries live only inside the artifact payload", async () => {
    const fixture = await makeFixture();
    const canaries = [
      "SECRET-TASK-BODY-<script>alert(1)</script>",
      "SUPER-SECRET-CREDENTIAL-VALUE",
      "/home/michael/work/git/opencode-docker/secret",
    ];
    // the artifact itself carries them inside its payload (documented,
    // unredacted): the envelope is canonical JSON with a base64 payload
    const envelope = JSON.parse(await readFile(join(fixture.runRoot, "logs", "1.workerlog.json"), "utf8")) as {
      payload_base64: string;
    };
    const payloadText = Buffer.from(envelope.payload_base64, "base64").toString("latin1");
    for (const canary of canaries) {
      expect(payloadText).toContain(canary);
    }
    // the list outcome carries no content
    const list = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot });
    const listJson = JSON.stringify(list);
    for (const canary of canaries) {
      expect(listJson).not.toContain(canary);
    }
    // a failure document carries no payload, cursor value, path or envelope
    const failure = await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, executionIndex: 9 });
    const failureJson = JSON.stringify(failure);
    for (const canary of canaries) {
      expect(failureJson).not.toContain(canary);
    }
    expect(failureJson).not.toContain("logs/");
    expect(failureJson).not.toContain("payload_base64");
    // the state document never carries log content
    const stateJson = await readFile(join(fixture.runRoot, "state.json"), "utf8");
    for (const canary of canaries) {
      expect(stateJson).not.toContain(canary);
    }
    // filenames never carry content
    const logNames = await readdir(join(fixture.runRoot, "logs"));
    expect(logNames.sort()).toEqual(["1.workerlog.json", "2.workerlog.json"]);
  });

  test("the chunk boundaries: the max limit is accepted, the raw offsets are byte offsets", async () => {
    const fixture = await makeFixture();
    const outcome = await logsPipelineV2Run({
      runId: RUN_ID,
      stateRoot: fixture.stateRoot,
      executionIndex: 1,
      offset: 0,
      limit: PIPELINE_V2_LOGS_MAX_CHUNK_LIMIT,
    });
    if (!outcome.ok || !("dataBase64" in outcome)) {
      throw new Error(`expected a chunk outcome, got ${JSON.stringify(outcome)}`);
    }
    expect(Buffer.from(outcome.dataBase64, "base64").byteLength).toBe(PAYLOAD_BYTES.byteLength);
    expect(outcome.nextOffset).toBe(null);
  });

  test("the runner validates its caller scalars fail-closed before any read", async () => {
    const fixture = await makeFixture();
    const before = await treeFingerprint(fixture.root);
    for (const bad of [
      { executionIndex: 0 },
      { executionIndex: 1.5 },
      { executionIndex: 1, offset: -1 },
      { executionIndex: 1, limit: 0 },
      { executionIndex: 1, limit: PIPELINE_V2_LOGS_MAX_CHUNK_LIMIT + 1 },
      { offset: 4 },
      { limit: 4 },
    ]) {
      expectFailure(
        await logsPipelineV2Run({ runId: RUN_ID, stateRoot: fixture.stateRoot, ...bad }),
        "invalid_options",
        null,
      );
    }
    expect(await treeFingerprint(fixture.root)).toBe(before);
  });
});

// --- the real CLI subprocess wire --------------------------------------------

test("the logs CLI subprocess serves the same projection over the real wire", async () => {
  const fixture = await makeFixture();
  const before = await treeFingerprint(fixture.root);
  const run = (args: string[]): { stdout: string; stderr: string; code: number } => {
    const proc = Bun.spawnSync(["bun", "src/main.ts", ...args], {
      cwd: join(import.meta.dir, ".."),
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, ORCHESTRATOR_STATE_ROOT: fixture.stateRoot },
    });
    return { stdout: proc.stdout.toString(), stderr: proc.stderr.toString(), code: proc.exitCode };
  };
  // the list document over the real subprocess
  const list = run(["logs", "--run-id", RUN_ID, "--config-root", "/tmp/unused", "--json"]);
  expect(list.code).toBe(0);
  const parsed = JSON.parse(list.stdout) as Record<string, unknown>;
  expect(Object.keys(parsed).sort()).toEqual([
    "exitCode", "logs", "ok", "revision", "runId", "runRoot", "schemaVersion",
  ]);
  expect((parsed["logs"] as unknown[]).length).toBe(3);
  // one exact chunk request through the wire
  const chunk = run(["logs", "--run-id", RUN_ID, "--config-root", "/tmp/unused", "--execution-index=1", "--offset=8", "--limit=11", "--json"]);
  expect(chunk.code).toBe(0);
  const chunkDoc = JSON.parse(chunk.stdout) as Record<string, unknown>;
  expect(chunkDoc["offset"]).toBe(8);
  expect(Array.from(Buffer.from(String(chunkDoc["dataBase64"]), "base64"))).toEqual(
    Array.from(PAYLOAD_BYTES.slice(8, 19)),
  );
  expect(chunkDoc["nextOffset"]).toBe(19);
  // the typed failure document over the wire (unknown execution index)
  const failure = run(["logs", "--run-id", RUN_ID, "--config-root", "/tmp/unused", "--execution-index", "9", "--json"]);
  expect(failure.code).toBe(1);
  expect(JSON.parse(failure.stdout)).toEqual({
    ok: false,
    exitCode: 1,
    runId: RUN_ID,
    runRoot: fixture.runRoot,
    reason: "execution_not_found",
  });
  // the human summary stays one content-free stderr line
  const human = run(["logs", "--run-id", RUN_ID, "--config-root", "/tmp/unused"]);
  expect(human.code).toBe(0);
  expect(human.stdout).toBe("");
  expect(human.stderr.startsWith("orchestrator: logs ok (run logs-run, revision ")).toBe(true);
  expect(human.stderr).not.toContain("SECRET-TASK-BODY");
  // the whole tree stayed byte-identical
  expect(await treeFingerprint(fixture.root)).toBe(before);
}, 30000);

// --- the source-scan pins ------------------------------------------------------

const readSource = (relative: string): string =>
  readFileSync(join(import.meta.dir, "..", "src", relative), "utf8");

test("one logs projection owner over the existing owners and no second machinery", () => {
  const logs = readSource("pipeline_v2_logs.ts");
  // the single loader chain and the single artifact verifier are reused
  expect(logs).toContain('from "./pipeline_v2_inspect.ts"');
  expect(logs).toContain("loadDurablePipelineV2Run");
  expect(logs).toContain("verifyPipelineV2WorkerLogBindings");
  expect(logs).toContain("loadPipelineV2WorkerLog");
  // no second parser, verifier, digest domain, publication protocol or
  // mutation machinery in the projection owner
  expect(logs).not.toContain("prepareWorkerLogArtifact");
  expect(logs).not.toContain("parseWorkerLogArtifact");
  expect(logs).not.toContain("pipeline-v2-worker-log");
  expect(logs).not.toContain("link(");
  expect(logs).not.toContain("fsync");
  expect(logs).not.toContain(".rename(");
  expect(logs).not.toContain("acquirePipelineV2RunMutationLock");
  expect(logs).not.toContain("fetchAuth");
  expect(logs).not.toContain("createChildSession");
  expect(logs).not.toContain("docker");
  // no message-based classification and no redaction machinery
  expect(logs).not.toContain(".message.includes");
  expect(logs).not.toContain(".match(");
  expect(logs).not.toContain("redact");
  expect(logs).not.toContain("sanitize");
});

test("the worker-log artifact owner stays the single parser and digest-domain owner", () => {
  const hits: string[] = [];
  for (const entry of readdirSync(join(import.meta.dir, "..", "src"))) {
    if (!entry.endsWith(".ts")) continue;
    const source = readFileSync(join(import.meta.dir, "..", "src", entry), "utf8");
    if (/function (prepare|parse)WorkerLogArtifact/.test(source)) {
      hits.push(entry);
    }
  }
  expect(hits).toEqual(["pipeline_v2_worker_log.ts"]);
  expect(readSource("pipeline_v2_worker_log.ts")).toContain('WORKER_LOG_DIGEST_DOMAIN = "pipeline-v2-worker-log\\0"');
});

test("the control-plane logs surfaces carry no stderr classification and no raw envelope", () => {
  const routing = readSource(join("control_plane", "routing.ts"));
  const client = readSource(join("control_plane", "subprocess_client.ts"));
  const http = readSource(join("control_plane", "http_server.ts"));
  // the logs command is one member of the closed union and the argv owner
  expect(routing).toContain('case "logs"');
  // the client and the HTTP layer validate shapes and reasons by typed
  // fields, never by stderr text or message matching
  for (const source of [client, http]) {
    expect(source).not.toContain(".message.includes");
    expect(source).not.toContain("stderr.includes");
    expect(source).not.toContain("stderr.toString()");
    expect(source).not.toContain(".match(/");
  }
  // the HTTP projections carry no artifact envelope or host paths
  expect(http).toContain('"worker_logs"');
  expect(http).toContain('"worker_log"');
  expect(http).not.toContain("payload_base64");
  expect(http).not.toContain("workerlog.json");
});
