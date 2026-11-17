/**
 * The P-2 agent-output content suite: the bounded, digest-bound raw-byte
 * content serving of one accepted agent execution output over real
 * durable state and real activation artifacts.
 *
 * Contract source: `/exchange/outbox/increment-6-p2-preflight-report.md`
 * (accepted r4, SHA-256
 * 50c312462a1d4cd5e14df09bd8cccdc2eed7f875f51fc9645cc1e4c17ecee938).
 * Every surface is read-only and path-free; the durable accepted-output
 * record is the availability authority; the digests travel the single
 * `pipeline-v2-output\0` framing owner.
 */
import { expect, test } from "bun:test";
import { chmodSync, lstatSync, mkdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { acceptedOutputDigest, streamAcceptedOutputSlice, type PipelineV2OutputStreamToken } from "../src/pipeline_v2_runtime.ts";
import { PipelineV2AgentOutputContentError } from "../src/pipeline_v2_agent_output_content.ts";
import { PipelineV2OutputStreamError } from "../src/pipeline_v2_runtime.ts";
import { open } from "node:fs/promises";
import {
  agentOutputContentPipelineV2Run,
  PIPELINE_V2_AGENT_OUTPUT_DEFAULT_CHUNK_LIMIT,
  PIPELINE_V2_OUTPUT_CONTENT_REASONS,
  type PipelineV2AgentOutputContentOutcome,
} from "../src/pipeline_v2_agent_output_content.ts";
import { PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX } from "../src/pipeline_v2_content_reasons.ts";
import { fingerprintTree } from "./apply_fixtures.ts";
import { runCli, type CliIo } from "../src/main.ts";
import { OrchestratorSubprocessClient, type OrchestratorCallResult } from "../src/control_plane/subprocess_client.ts";
import { routeOrchestratorCommand, type OrchestratorCommand } from "../src/control_plane/routing.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { loadControlPlaneProjectRegistry } from "../src/control_plane/project_registry.ts";
import { defaultControlPlaneUiRoot, loadControlPlaneUiAssets } from "../src/control_plane/ui_assets.ts";
import { handleControlPlaneHttpRequest } from "../src/control_plane/http_server.ts";

// =============================================================================
// baseline digest literals (red-before evidence, tree 5b40780)
// =============================================================================

/**
 * The P-2 fixture generator, byte-identical to the red-before capture
 * script (`/exchange/scratch/p2-red-before/capture_literals.ts`) whose
 * output was computed by the pre-refactor `acceptedOutputDigest` on the
 * clean baseline tree 5b40780. The literals below pin the framing-owner
 * refactor and the streaming helper to the baseline digests.
 */
export function fixtureBytes(seed: bigint, size: number): Uint8Array {
  const out = new Uint8Array(size);
  let state = seed & 0xffffffffffffn;
  for (let i = 0; i < size; i += 1) {
    state = (state * 25214903917n + 11n) & 0xffffffffffffn;
    out[i] = Number((state >> 24n) & 0xffn);
  }
  return out;
}

export function fixtureJson(seed: bigint, size: number): Buffer {
  const prefix = `{"v":[`;
  const suffix = `]}`;
  const bodyLength = size - prefix.length - suffix.length;
  if (bodyLength < 0) throw new Error("size too small");
  const digits = fixtureBytes(seed, Math.max(bodyLength, 1));
  const maxPairs = Math.floor((bodyLength - 1) / 2);
  let body = `${digits[0]! % 10}`;
  for (let i = 1; i <= maxPairs; i += 1) {
    body += `,${digits[i]! % 10}`;
  }
  const fill = bodyLength - body.length;
  const text = `${prefix}${body}${" ".repeat(fill)}${suffix}`;
  const bytes = Buffer.from(text, "utf8");
  if (bytes.byteLength !== size) throw new Error("json fixture size mismatch");
  JSON.parse(bytes.toString("utf8"));
  return bytes;
}

const SEED_FILE = 0x5b40780n;
const SEED_JSON = 0x50c312n;
const LITERAL_FILE_131071 = "2721fb719257109586bee623327d69dee9be8dd688b71c3b7d738bd1d5307f26";
const LITERAL_FILE_131072 = "9e80dc643b7d7a3d9a1d41e84f3308ede5f7c7a6efa5e5ee9d87738c5faeac80";
const LITERAL_FILE_131073 = "78cc5a61fe4e591d52295913b545d35c8b3724177ba01dc81a38c323fba32c4c";
const LITERAL_JSON_131071 = "aa17e57299815e77b4afa48915caed83792f7dc0fa37df68c2d9d1c49f401d68";
const LITERAL_JSON_131072 = "40dcf9e4cfe88bcc07689c95de985a20f33cb4fb139de335d2a5a3a0d42beaf7";
const LITERAL_JSON_131073 = "81acebeb2818c929f2786a572546ae802e95e1d4769dc1569897966fdc6b24b8";
const LITERAL_FILE_262144 = "3fa5efd049ca3fee77bcfebe69c6cb2e9f9adc90bd5d9d3b24d6e2d0b02c427e";

// =============================================================================
// shared fixture machinery (real reducer/sink, real artifacts)
// =============================================================================

const V2_PROFILE_EPOCH_BINDING = { digest: "d".repeat(64), profile_count: 1 };
const V2_PROJECT_PROVENANCE_BINDING = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };
const hex = (char: string): string => char.repeat(64);

type PortSuite = { readonly id: string; readonly type: "file" | "json" | "directory"; readonly content: Uint8Array | string; readonly schema?: string };

const REVIEW_STATE = "review";
const DECISION_STATE = "decided";
const CANARY = "agent_output_content_canary";

const CONTROL_ROLE = `    - state_id: ${DECISION_STATE}
      role: control`;

const GO_MODEL_YAML = `schema_version: 1
facts:
  - id: ready
decisions:
  - id: go
relations: []
constraints: []
rules:
  - id: r1
    when:
      all:
        - fact: ready
          equals: true
    decision: go
`;

const DECISION_STATE_YAML = `  - id: ${DECISION_STATE}
    type: decision
    model: decisions/go.yaml
    inputs:
      - id: report
        source: {state_output: {state: ${REVIEW_STATE}, output: verdict}}
    transitions:
      - {outcome: go, to: done}
      - {outcome: uncovered, to: done}
      - {outcome: inconsistent_facts, to: done}
      - {outcome: invalid_facts, to: done}`;

const VERDICT_SCHEMA = { type: "object", required: ["ok"], properties: { ok: { type: "boolean" } } };
const FEEDBACK_SCHEMA = { type: "object", required: ["text"], properties: { text: { type: "string" } } };

const RENDER_TRANSITION_TO_DONE = `{outcome: completed, to: done}`;

function bundleYaml(executionRoles: string, states: string): string {
  return `schema_version: 2
entry_state: ${REVIEW_STATE}
max_transitions: 20

inputs: []

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
${executionRoles}

states:
${states}`;
}

const PLANNING_ROLE_YAML = `    - state_id: ${REVIEW_STATE}
      role: planning
      plan_output: verdict
      stage_wait:
        reason: iteration_wait
        actions:
          - continue_stage`;

const RENDER_STATE_YAML = `  - id: ${REVIEW_STATE}
    type: agent
    profile: coder
    prompt: prompts/review.md
    inputs: []
    outputs:
      - id: verdict
        type: json
        schema: schemas/verdict.schema.json
      - id: feedback
        type: json
        schema: schemas/feedback.schema.json
      - id: snap
        type: file
      - id: tree
        type: directory
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - ${RENDER_TRANSITION_TO_DONE}`;

const RENDER_STATE_TO_DECISION_YAML = `  - id: ${REVIEW_STATE}
    type: agent
    profile: coder
    prompt: prompts/review.md
    inputs: []
    outputs:
      - id: verdict
        type: json
        schema: schemas/verdict.schema.json
      - id: feedback
        type: json
        schema: schemas/feedback.schema.json
      - id: snap
        type: file
      - id: tree
        type: directory
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - {outcome: completed, to: ${DECISION_STATE}}`;

const DONE_STATE_YAML = `  - id: done
    type: terminal
    result: success`;

interface Dirs {
  readonly root: string;
  readonly bundle: string;
  readonly stateRoot: string;
  runRoot: string;
  pipeline: ResolvedPipelineV2;
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, 0, clock));
}

async function makeDirs(prefix: string, roles: string, states: string, extra: (bundle: string) => void = () => {}): Promise<Dirs> {
  const root = mkdtempSync(join(tmpdir(), `${prefix}-`));
  const bundle = join(root, "bundle");
  mkdirSync(join(bundle, "prompts"), { recursive: true, mode: 0o700 });
  mkdirSync(join(bundle, "schemas"), { recursive: true, mode: 0o700 });
  mkdirSync(join(bundle, "decisions"), { recursive: true, mode: 0o700 });
  writeFileSync(join(bundle, "pipeline.yaml"), bundleYaml(roles, states), { mode: 0o600 });
  writeFileSync(join(bundle, "schemas", "verdict.schema.json"), JSON.stringify(VERDICT_SCHEMA), { mode: 0o600 });
  writeFileSync(join(bundle, "schemas", "feedback.schema.json"), JSON.stringify(FEEDBACK_SCHEMA), { mode: 0o600 });
  writeFileSync(join(bundle, "prompts", "review.md"), `REVIEW ${CANARY}\n`, { mode: 0o600 });
  const configRoot = join(root, "config");
  mkdirSync(join(configRoot, "profiles"), { recursive: true, mode: 0o700 });
  mkdirSync(join(configRoot, "opencode"), { recursive: true, mode: 0o700 });
  writeFileSync(
    join(configRoot, "profiles", "coder.yaml"),
    "schema_version: 2\nimage: ghcr.io/example/worker@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08\nopencode_config:\n  path: opencode/coder.json\n  secrets: env_only\nenv: {}\n",
    { mode: 0o600 },
  );
  writeFileSync(join(configRoot, "opencode", "coder.json"), "{}", { mode: 0o600 });
  extra(bundle);
  const pipeline = await loadPipelineV2(bundle);
  return { root, bundle, stateRoot: join(root, "state"), runRoot: "", pipeline } as Dirs;
}

/**
 * Builds one finalized durable run whose first (planning) agent execution
 * carries exactly one accepted output record for each requested port with
 * the artifact written at the fixed activation path and its real
 * `pipeline-v2-output\0` digest recorded durably.
 */
async function buildAcceptedRun(
  dirs: Dirs,
  runId: string,
  ports: readonly PortSuite[],
  options: {
    readonly decisionRun?: boolean;
    readonly agentOnDecisionState?: boolean;
    readonly emptyPlanningRecord?: boolean;
  } = {},
): Promise<{ runRoot: string; state: PipelineV2RunState }> {
  const runRoot = join(dirs.stateRoot, "pipeline-runs", runId);
  dirs.runRoot = runRoot;
  const sink = new PipelineV2RunStateSink({ stateRoot: dirs.stateRoot, runId, now: nextTick });
  await sink.dispatch({
    kind: "create_run",
    runId,
    pipeline: pipelineV2RunPipelineIdentity(dirs.pipeline),
    inputs: [],
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  await sink.dispatch({ kind: "start_agent_execution", stateId: REVIEW_STATE, profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${runId}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${runId}` },
    { kind: "agent_running" },
  ]) {
    await sink.dispatch(command as never);
  }
  const outputsRoot = join(runRoot, "activations", `1-${REVIEW_STATE}`, "data", "outputs");
  mkdirSync(outputsRoot, { recursive: true, mode: 0o700 });
  const records: { id: string; digest: string }[] = [];
  for (const port of ports) {
    const target = join(outputsRoot, port.id);
    if (port.type === "directory") {
      mkdirSync(target, { recursive: true, mode: 0o700 });
      writeFileSync(join(target, "leaf.txt"), "TREE-LEAF\n", { mode: 0o600 });
      mkdirSync(join(target, "emptychild"), { recursive: true, mode: 0o700 });
    } else {
      writeFileSync(target, port.content as never, { mode: 0o600 });
    }
    records.push({ id: port.id, digest: await acceptedOutputDigest(port.type, target, "fixture accepted output") });
  }
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: options.emptyPlanningRecord === true ? [] : records,
    workerLog: { status: "unavailable" },
  } as never);
  await sink.dispatch({ kind: "agent_cleanup_completed" } as never);
  const secondState = options.agentOnDecisionState === true || options.decisionRun === true ? DECISION_STATE : "done";
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: REVIEW_STATE, outcome: "completed", to: secondState, transition_index: 0 },
    executionIndex: 1,
  } as never);
  if (options.agentOnDecisionState === true) {
    // The honest reducer-producible divergence: `start_agent_execution`
    // never cross-checks the state type, only the cursor, so an agent
    // execution on the decision state is reducer-legal and loader-valid
    // while its state is not a compiled agent state.
    await sink.dispatch({ kind: "start_agent_execution", stateId: DECISION_STATE, profile: "coder", executionRole: "control" });
    for (const command of [
      { kind: "agent_data_prepared" },
      { kind: "agent_execution_session_created", sessionId: `sess2-${runId}` },
      { kind: "agent_tool_session_created", sessionId: `tool2-${runId}` },
      { kind: "agent_running" },
      { kind: "agent_outputs_accepted", outputs: records, workerLog: { status: "unavailable" } },
      { kind: "agent_cleanup_completed" },
    ]) {
      await sink.dispatch(command as never);
    }
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: DECISION_STATE, outcome: "completed", to: "done", transition_index: 0 },
      executionIndex: 2,
    } as never);
  } else if (options.decisionRun === true) {
    await sink.dispatch({ kind: "start_decision_execution", stateId: DECISION_STATE, inputDigest: hex("7"), executionRole: "control" });
    await sink.dispatch({
      kind: "decision_evaluated",
      result: { status: "selected", outcome: "go", decision: "go", rule_id: "r1", active_constraint_ids: [] },
    } as never);
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: DECISION_STATE, outcome: "go", to: "done", transition_index: 0 },
      executionIndex: 2,
    } as never);
  }
  await sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
  await sink.dispatch({ kind: "run_outputs_published", outputs: [] } as never);
  await sink.dispatch({ kind: "run_succeeded" } as never);
  const state = parsePipelineV2RunState(readFileSync(join(runRoot, "state.json"), "utf8"));
  return { runRoot, state };
}

function artifactPath(runRoot: string, executionIndex: number, stateId: string, outputId: string): string {
  return join(runRoot, "activations", `${executionIndex}-${stateId}`, "data", "outputs", outputId);
}

async function call(runId: string, dirs: Dirs, executionIndex: number, outputId: string, extra: { offset?: number; limit?: number } = {}): Promise<PipelineV2AgentOutputContentOutcome> {
  return await agentOutputContentPipelineV2Run({
    runId,
    stateRoot: dirs.stateRoot,
    executionIndex,
    outputId,
    ...(extra.offset === undefined ? {} : { offset: extra.offset }),
    ...(extra.limit === undefined ? {} : { limit: extra.limit }),
  });
}

function expectFailure(outcome: PipelineV2AgentOutputContentOutcome, reason: import("../src/pipeline_v2_agent_output_content.ts").PipelineV2AgentOutputContentReason, runId: string): void {
  if (outcome.ok) {
    throw new Error(`expected failure ${reason}, got success for ${JSON.stringify(outcome.dataBase64.slice(0, 24))}`);
  }
  expect(outcome.reason).toBe(reason);
  expect(outcome.runId).toBe(runId);
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "reason", "runId"]);
}

// =============================================================================
// runtime framing pins (baseline literals + streaming helper)
// =============================================================================

test("the slice-limit and default-chunk constants are the accepted r4 owners", () => {
  expect(PIPELINE_V2_OUTPUT_CONTENT_REASONS).toEqual([
    "run_not_found",
    "run_state_invalid",
    "pipeline_mismatch",
    "unknown_output",
    "artifact_missing",
    "artifact_modified",
    "unsupported_output_kind",
    "invalid_cursor",
    "internal_error",
  ]);
  expect(PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX).toBe(262_144);
  expect(PIPELINE_V2_AGENT_OUTPUT_DEFAULT_CHUNK_LIMIT).toBe(65_536);
});

test("acceptedOutputDigest still reproduces the baseline literals after the framing-owner refactor", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-literals-"));
  try {
    expect(await acceptedOutputDigest("file", write(root, "f1.bin", fixtureBytes(SEED_FILE, 131071)), "literal")).toBe(LITERAL_FILE_131071);
    expect(await acceptedOutputDigest("file", write(root, "f2.bin", fixtureBytes(SEED_FILE, 131072)), "literal")).toBe(LITERAL_FILE_131072);
    expect(await acceptedOutputDigest("file", write(root, "f3.bin", fixtureBytes(SEED_FILE, 131073)), "literal")).toBe(LITERAL_FILE_131073);
    expect(await acceptedOutputDigest("json", write(root, "j1.json", fixtureJson(SEED_JSON, 131071)), "literal")).toBe(LITERAL_JSON_131071);
    expect(await acceptedOutputDigest("json", write(root, "j2.json", fixtureJson(SEED_JSON, 131072)), "literal")).toBe(LITERAL_JSON_131072);
    expect(await acceptedOutputDigest("json", write(root, "j3.json", fixtureJson(SEED_JSON, 131073)), "literal")).toBe(LITERAL_JSON_131073);
    expect(await acceptedOutputDigest("file", write(root, "max.bin", fixtureBytes(SEED_JSON, 262_144)), "literal")).toBe(LITERAL_FILE_262144);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

function write(root: string, name: string, content: Uint8Array): string {
  const path = join(root, name);
  writeFileSync(path, content, { mode: 0o600 });
  return path;
}

function tokenFor(root: string, path: string, type: "file" | "json"): PipelineV2OutputStreamToken {
  const info = statSync(path);
  return { canonicalPath: path, dev: info.dev, ino: info.ino, type };
}

test("the streaming helper reproduces the baseline literals for file and json around the 128 KiB boundary", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-stream-"));
  try {
    const cases: [string, Uint8Array, "file" | "json", string][] = [
      [write(root, "f1.bin", fixtureBytes(SEED_FILE, 131071)), fixtureBytes(SEED_FILE, 131071), "file", LITERAL_FILE_131071],
      [write(root, "f2.bin", fixtureBytes(SEED_FILE, 131072)), fixtureBytes(SEED_FILE, 131072), "file", LITERAL_FILE_131072],
      [write(root, "f3.bin", fixtureBytes(SEED_FILE, 131073)), fixtureBytes(SEED_FILE, 131073), "file", LITERAL_FILE_131073],
      [write(root, "j1.json", fixtureJson(SEED_JSON, 131071)), fixtureJson(SEED_JSON, 131071), "json", LITERAL_JSON_131071],
      [write(root, "j2.json", fixtureJson(SEED_JSON, 131072)), fixtureJson(SEED_JSON, 131072), "json", LITERAL_JSON_131072],
      [write(root, "j3.json", fixtureJson(SEED_JSON, 131073)), fixtureJson(SEED_JSON, 131073), "json", LITERAL_JSON_131073],
    ];
    for (const [path, bytes, type, literal] of cases) {
      const token = tokenFor(root, path, type);
      const r = await streamAcceptedOutputSlice(token, 0, PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX);
      expect(r.digest).toBe(literal);
      expect(r.byteCount).toBe(bytes.byteLength);
      if (bytes.byteLength <= PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX) {
        expect(Buffer.from(r.slice).equals(Buffer.from(bytes))).toBe(true);
      }
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

test("helper and acceptedOutputDigest agree on the same fixtures (single-owner digest)", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-agree-"));
  try {
    for (const size of [131071, 131072, 131073]) {
      const fp = write(root, `f${size}.bin`, fixtureBytes(SEED_FILE, size));
      expect((await streamAcceptedOutputSlice(tokenFor(root, fp, "file"), 0, 1024)).digest)
        .toBe(await acceptedOutputDigest("file", fp, "agree"));
      const jp = write(root, `j${size}.json`, fixtureJson(SEED_JSON, size));
      expect((await streamAcceptedOutputSlice(tokenFor(root, jp, "json"), 0, 1024)).digest)
        .toBe(await acceptedOutputDigest("json", jp, "agree"));
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

test("the helper cursor slice windows are exact across chunk boundaries", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-window-"));
  try {
    const bytes = fixtureBytes(SEED_FILE, 262_144);
    const path = write(root, "big.bin", bytes);
    const token = tokenFor(root, path, "file");
    expect(statSync(path).dev === token.dev).toBe(true);
    for (const [offset, limit] of [
      [0, 1], [0, 131072], [131071, 2], [131072, 131072], [262143, 5], [200000, 262144],
    ] as const) {
      const r = await streamAcceptedOutputSlice(token, offset, limit);
      const end = Math.min(offset + limit, bytes.byteLength);
      expect(r.byteCount).toBe(bytes.byteLength);
      expect(Buffer.from(r.slice).equals(Buffer.from(bytes.subarray(offset, end)))).toBe(true);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

test("the helper cause table is typed and closed (delete-before-open, ELOOP, dev/ino mismatch, size divergence, rewrite)", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-causes-"));
  try {
    // delete-before-open -> artifact_missing
    const gonePath = write(root, "gone.bin", fixtureBytes(SEED_FILE, 16));
    const token0 = tokenFor(root, gonePath, "file");
    rmSync(gonePath);
    try {
      await streamAcceptedOutputSlice(token0, 0, 16);
      throw new Error("expected artifact_missing");
    } catch (cause) {
      expect(cause instanceof PipelineV2OutputStreamError).toBe(true);
      expect((cause as PipelineV2OutputStreamError).reason).toBe("artifact_missing");
    }
    // ELOOP leaf symlink -> artifact_modified
    const realPath = write(root, "real.bin", fixtureBytes(SEED_FILE, 16));
    const linkPath = join(root, "loop.bin");
    symlinkSync(realPath, linkPath);
    const linkInfo = lstatSync(linkPath);
    try {
      await streamAcceptedOutputSlice({ canonicalPath: linkPath, dev: linkInfo.dev, ino: linkInfo.ino, type: "file" }, 0, 16);
      throw new Error("expected artifact_modified for ELOOP");
    } catch (cause) {
      expect(cause instanceof PipelineV2OutputStreamError).toBe(true);
      expect((cause as PipelineV2OutputStreamError).reason).toBe("artifact_modified");
    }
    // opened dev/ino mismatch -> artifact_modified (token minted on file A, path of file B)
    const a = write(root, "a.bin", fixtureBytes(SEED_FILE, 32));
    write(root, "b.bin", Buffer.from("DIFFERENT CONTENT"));
    const mismatchToken: PipelineV2OutputStreamToken = { ...tokenFor(root, a, "file"), canonicalPath: join(root, "b.bin") };
    try {
      await streamAcceptedOutputSlice(mismatchToken, 0, 16);
      throw new Error("expected artifact_modified for dev/ino mismatch");
    } catch (cause) {
      expect(cause instanceof PipelineV2OutputStreamError).toBe(true);
      expect((cause as PipelineV2OutputStreamError).reason).toBe("artifact_modified");
    }
    // inode replacement between precheck and open (rename over) -> artifact_modified
    const victim = write(root, "victim.bin", fixtureBytes(SEED_FILE, 32));
    const victimToken = tokenFor(root, victim, "file");
    const replacement = write(root, "repl.bin", Buffer.from("REPLACEMENT INODE"));
    rmSync(victim);
    writeFileSync(victim, "SUBSTITUTED BYTES", { mode: 0o600 });
    void replacement;
    rmSync(replacement);
    try {
      await streamAcceptedOutputSlice(victimToken, 0, 32);
      throw new Error("expected artifact_modified for inode substitution");
    } catch (cause) {
      expect(cause instanceof PipelineV2OutputStreamError).toBe(true);
      expect((cause as PipelineV2OutputStreamError).reason).toBe("artifact_modified");
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

test("the post-open identity binding: a replaced inode with a fresh token serves the new content, the stale token refuses", async () => {
  const root = mkdtempSync(join(tmpdir(), "p2-replace-"));
  try {
    const path = write(root, "swap.bin", Buffer.from("ORIGINAL CONTENT"));
    const staleToken = tokenFor(root, path, "file");
    // Atomic inode replacement through rename-over (deterministic).
    const replacement = write(root, "swap-next.bin", Buffer.from("NEW CONTENT BYTES"));
    const { rename } = await import("node:fs/promises");
    await rename(replacement, path);
    try {
      await streamAcceptedOutputSlice(staleToken, 0, 64);
      throw new Error("expected artifact_modified with the stale token");
    } catch (cause) {
      expect(cause instanceof PipelineV2OutputStreamError).toBe(true);
      expect((cause as PipelineV2OutputStreamError).reason).toBe("artifact_modified");
    }
    const fresh = await streamAcceptedOutputSlice(tokenFor(root, path, "file"), 0, 64);
    expect(Buffer.from(fresh.slice).toString("utf8")).toBe("NEW CONTENT BYTES");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}, 60000);

// =============================================================================
// facade selection matrix over real durable runs
// =============================================================================

const SMALL_FILE_CONTENT = Buffer.from(`SNAPSHOT ${CANARY}\n`, "utf8");
const VERDICT_CONTENT = Buffer.from('{"ok":true}\n', "utf8");
const FEEDBACK_CONTENT = Buffer.from('{"text":"minor issues"}\n', "utf8");
const ALL_PORTS: readonly PortSuite[] = [
  { id: "verdict", type: "json", content: VERDICT_CONTENT },
  { id: "feedback", type: "json", content: FEEDBACK_CONTENT },
  { id: "snap", type: "file", content: SMALL_FILE_CONTENT },
  { id: "tree", type: "directory", content: "" },
];

function expectSuccess(
  outcome: PipelineV2AgentOutputContentOutcome,
  fields: { runId: string; executionIndex: number; stateId: string; outputId: string; digest: string; byteCount: number; offset: number; limit: number; returnedByteCount: number; nextOffset: number | null; dataBase64: string },
): void {
  if (!outcome.ok) {
    throw new Error(`expected success, got reason ${outcome.reason}`);
  }
  expect(outcome).toEqual({
    ok: true,
    exitCode: 0,
    schemaVersion: 1,
    runId: fields.runId,
    executionIndex: fields.executionIndex,
    stateId: fields.stateId,
    outputId: fields.outputId,
    digest: fields.digest,
    byteCount: fields.byteCount,
    offset: fields.offset,
    limit: fields.limit,
    returnedByteCount: fields.returnedByteCount,
    nextOffset: fields.nextOffset,
    dataBase64: fields.dataBase64,
  });
}

test("the plain planning agent serves its accepted file and json outputs with exact digests", async () => {
  const dirs = await makeDirs("p2-plain-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "plain-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    const before = await fingerprintTree(dirs.stateRoot);
    const snap = await call(runId, dirs, 1, "snap", { limit: 5 });
    expectSuccess(snap, {
      runId,
      executionIndex: 1,
      stateId: REVIEW_STATE,
      outputId: "snap",
      digest: await acceptedOutputDigest("file", artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "snap"), "expected"),
      byteCount: SMALL_FILE_CONTENT.byteLength,
      offset: 0,
      limit: 5,
      returnedByteCount: 5,
      nextOffset: 5,
      dataBase64: SMALL_FILE_CONTENT.subarray(0, 5).toString("base64"),
    });
    const verdictTail = await call(runId, dirs, 1, "verdict", { offset: 2 });
    expectSuccess(verdictTail, {
      runId,
      executionIndex: 1,
      stateId: REVIEW_STATE,
      outputId: "verdict",
      digest: await acceptedOutputDigest("json", artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "verdict"), "expected"),
      byteCount: VERDICT_CONTENT.byteLength,
      offset: 2,
      limit: 65_536,
      returnedByteCount: VERDICT_CONTENT.byteLength - 2,
      nextOffset: null,
      dataBase64: VERDICT_CONTENT.subarray(2).toString("base64"),
    });
    expect(await fingerprintTree(dirs.stateRoot)).toBe(before);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("a planning agent without an accepted record is unknown_output; every port is served like any other (no role special case)", async () => {
  const dirs = await makeDirs("p2-planning-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "no-record-run";
  try {
    await buildAcceptedRun(dirs, runId, [], { emptyPlanningRecord: true });
    expectFailure(await call(runId, dirs, 1, "verdict"), "unknown_output", runId);
    expectFailure(await call(runId, dirs, 1, "snap"), "unknown_output", runId);
    expectFailure(await call(runId, dirs, 99, "verdict"), "unknown_output", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("a decision execution index is unknown_output (no record, no activation)", async () => {
  const dirs = await makeDirs(
    "p2-decision-",
    `${PLANNING_ROLE_YAML}\n${CONTROL_ROLE}`,
    `${RENDER_STATE_TO_DECISION_YAML}\n${DECISION_STATE_YAML}\n${DONE_STATE_YAML}`,
    (bundle) => writeFileSync(join(bundle, "decisions", "go.yaml"), GO_MODEL_YAML, { mode: 0o600 }),
  );
  const runId = "decision-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS, { decisionRun: true });
    expectFailure(await call(runId, dirs, 2, "verdict"), "unknown_output", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("a durable record on a non-agent state is run_state_invalid before any artifact read", async () => {
  const dirs = await makeDirs(
    "p2-forgedagent-",
    `${PLANNING_ROLE_YAML}\n${CONTROL_ROLE}`,
    `${RENDER_STATE_TO_DECISION_YAML}\n${DECISION_STATE_YAML}\n${DONE_STATE_YAML}`,
    (bundle) => writeFileSync(join(bundle, "decisions", "go.yaml"), GO_MODEL_YAML, { mode: 0o600 }),
  );
  const runId = "wrong-state-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS, { agentOnDecisionState: true });
    const before = await fingerprintTree(dirs.stateRoot);
    expectFailure(await call(runId, dirs, 2, "verdict"), "run_state_invalid", runId);
    expect(await fingerprintTree(dirs.stateRoot)).toBe(before);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("a declared directory port is unsupported_output_kind before presence and digest checks", async () => {
  const dirs = await makeDirs("p2-dirport-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "dir-port-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    expectFailure(await call(runId, dirs, 1, "tree"), "unsupported_output_kind", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

// =============================================================================
// cursor matrix, tamper battery, precedence and content-free documents
// =============================================================================

test("the cursor matrix serves every §3d coherence case on a healthy artifact", async () => {
  const dirs = await makeDirs("p2-cursor-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "cursor-run";
  try {
    await buildAcceptedRun(dirs, runId, [ { id: "verdict", type: "json", content: fixtureJson(SEED_JSON, 131_072) } ]);
    const expectedDigest = await acceptedOutputDigest("json", artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "verdict"), "expected");
    // default limit and full first chunk
    const full = await call(runId, dirs, 1, "verdict", { limit: 65_536 });
    expectSuccess(full, { runId, executionIndex: 1, stateId: REVIEW_STATE, outputId: "verdict", digest: expectedDigest, byteCount: 131_072, offset: 0, limit: 65_536, returnedByteCount: 65_536, nextOffset: 65_536, dataBase64: fixtureJson(SEED_JSON, 131_072).subarray(0, 65_536).toString("base64") });
    // boundary-chunk walk: last byte served alone, terminal empty chunk
    const lastByte = await call(runId, dirs, 1, "verdict", { offset: 131_071, limit: 65_536 });
    expectSuccess(lastByte, { runId, executionIndex: 1, stateId: REVIEW_STATE, outputId: "verdict", digest: expectedDigest, byteCount: 131_072, offset: 131_071, limit: 65_536, returnedByteCount: 1, nextOffset: null, dataBase64: fixtureJson(SEED_JSON, 131_072).subarray(131_071, 131_072).toString("base64") });
    const empty = await call(runId, dirs, 1, "verdict", { offset: 131_072 });
    expectSuccess(empty, { runId, executionIndex: 1, stateId: REVIEW_STATE, outputId: "verdict", digest: expectedDigest, byteCount: 131_072, offset: 131_072, limit: 65_536, returnedByteCount: 0, nextOffset: null, dataBase64: "" });
    // beyond the artifact
    expectFailure(await call(runId, dirs, 1, "verdict", { offset: 131_073 }), "invalid_cursor", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("tamper battery: byte flip, kind substitution, symlink, truncation, growth, relocation, deletion", async () => {
  const dirs = await makeDirs("p2-tamper-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "tamper-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    const snapPath = artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "snap");
    // byte flip -> artifact_modified
    const original = readFileSync(snapPath);
    const flipped = Buffer.from(original);
    flipped[0] = flipped[0]! ^ 0xff;
    writeFileSync(snapPath, flipped, { mode: 0o600 });
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
    writeFileSync(snapPath, original, { mode: 0o600 });
    // truncation and growth -> artifact_modified
    writeFileSync(snapPath, original.subarray(0, original.byteLength - 1), { mode: 0o600 });
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
    writeFileSync(snapPath, Buffer.concat([original, Buffer.from("EXTRA")]), { mode: 0o600 });
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
    writeFileSync(snapPath, original, { mode: 0o600 });
    // kind substitution (file -> directory) -> artifact_modified
    rmSync(snapPath);
    mkdirSync(snapPath, { mode: 0o700 });
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
    rmSync(snapPath, { recursive: true });
    writeFileSync(snapPath, original, { mode: 0o600 });
    // leaf symlink -> artifact_modified
    const linkTarget = write(dirs.root, "elsewhere.bin", Buffer.from("E"));
    rmSync(snapPath);
    symlinkSync(linkTarget, snapPath);
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
    rmSync(snapPath);
    writeFileSync(snapPath, original, { mode: 0o600 });
    // deletion -> artifact_missing
    rmSync(snapPath);
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_missing", runId);
    writeFileSync(snapPath, original, { mode: 0o600 });
    // escape: the outputs parent replaced by a symlink into the bundle -> artifact_modified
    const outputsParent = join(dirs.runRoot!, "activations", `1-${REVIEW_STATE}`, "data", "outputs");
    rmSync(snapPath);
    rmSync(outputsParent, { recursive: true });
    const escaped = join(dirs.root, "escaped-outputs");
    mkdirSync(escaped, { mode: 0o700 });
    writeFileSync(join(escaped, "snap"), original, { mode: 0o600 });
    symlinkSync(escaped, outputsParent);
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("an absent parent chain and a zero-byte artifact behave exactly by presence and digest", async () => {
  const dirs = await makeDirs("p2-absent-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "absent-run";
  try {
    await buildAcceptedRun(dirs, runId, [ { id: "snap", type: "file", content: Buffer.alloc(0) } ]);
    const zeroPath = artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "snap");
    expect(statSync(zeroPath).size).toBe(0);
    const outcome = await call(runId, dirs, 1, "snap", { limit: 4 });
    expectSuccess(outcome, {
      runId, executionIndex: 1, stateId: REVIEW_STATE, outputId: "snap",
      digest: await acceptedOutputDigest("file", zeroPath, "expected"),
      byteCount: 0, offset: 0, limit: 4, returnedByteCount: 0, nextOffset: null, dataBase64: "",
    });
    // a verbatim-replayed record digest against a replaced zero-byte artifact: any byte content on a
    // 0..N byte artifact re-verifies by the same framing; a substituted kind fails
    rmSync(zeroPath);
    writeFileSync(zeroPath, "changed", { mode: 0o600 });
    expectFailure(await call(runId, dirs, 1, "snap"), "artifact_modified", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("precedence: a damaged artifact reports artifact_modified even with an out-of-range cursor", async () => {
  const dirs = await makeDirs("p2-precedence-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "precedence-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    const snapPath = artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "snap");
    writeFileSync(snapPath, Buffer.from("damaged"), { mode: 0o600 });
    expectFailure(await call(runId, dirs, 1, "snap", { offset: 999_999 }), "artifact_modified", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("hostile caller shapes reaching the facade are internal_error and never invalid_options", async () => {
  expectFailure(
    await agentOutputContentPipelineV2Run({ runId: 42 as never, stateRoot: "/tmp", executionIndex: 1, outputId: "snap" }),
    "internal_error",
    "",
  );
  expectFailure(
    await agentOutputContentPipelineV2Run({ runId: "run-ok", stateRoot: "/tmp/does-not-exist-p2", executionIndex: 1, outputId: "not a safe id!@#" }),
    "internal_error",
    "run-ok",
  );
  expectFailure(
    await agentOutputContentPipelineV2Run({ runId: "run-ok", stateRoot: "/tmp/does-not-exist-p2", executionIndex: 0, outputId: "snap" }),
    "internal_error",
    "run-ok",
  );
  expectFailure(
    await agentOutputContentPipelineV2Run({ runId: "run-ok", stateRoot: "/tmp/does-not-exist-p2", executionIndex: 1, outputId: "snap", limit: 262_145 }),
    "internal_error",
    "run-ok",
  );
});

test("success documents are deep-frozen and path-free; failures carry no run root and no artifact bytes", async () => {
  const dirs = await makeDirs("p2-doc-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "doc-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    const outcome = await call(runId, dirs, 1, "snap", { limit: 3 });
    if (!outcome.ok) throw new Error("expected success");
    expect(Object.isFrozen(outcome)).toBe(true);
    expect(Object.keys(outcome).sort()).toEqual([
      "byteCount", "dataBase64", "digest", "executionIndex", "exitCode", "limit", "nextOffset", "offset", "ok", "outputId", "returnedByteCount", "runId", "schemaVersion", "stateId",
    ]);
    expectFailure(await call(runId, dirs, 1, "absent-port"), "unknown_output", runId);
    expectFailure(await call(runId, dirs, 404, "snap"), "unknown_output", runId);
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

// =============================================================================
// content-free diagnostics + ownership source scans
// =============================================================================

test("generic content never enters any producer document field or diagnostic", async () => {
  const dirs = await makeDirs("p2-leak-", PLANNING_ROLE_YAML, `${RENDER_STATE_YAML}\n${DONE_STATE_YAML}`);
  const runId = "leak-run";
  try {
    await buildAcceptedRun(dirs, runId, ALL_PORTS);
    const evidence: string[] = [];
    for (const bad of [
      () => call(runId, dirs, 1, "absent-port"),
      () => call(runId, dirs, 7, "verdict"),
      () => call(runId, dirs, 1, "tree"),
    ]) {
      const outcome = await bad();
      if (outcome.ok) throw new Error("expected failure");
      evidence.push(JSON.stringify(outcome));
    }
    const damaged = artifactPath(dirs.runRoot!, 1, REVIEW_STATE, "verdict");
    rmSync(damaged);
    const missing = await call(runId, dirs, 1, "verdict");
    if (missing.ok) throw new Error("expected failure");
    evidence.push(JSON.stringify(missing));
    const all = evidence.join("\n");
    expect(all).not.toContain(CANARY);
    expect(all).not.toContain("minor issues");
    expect(all).not.toContain(dirs.root);
    expect(all).not.toContain("activations");
  } finally {
    rmSync(dirs.root, { recursive: true, force: true });
  }
}, 60000);

test("the facade module owns no framing, no digest domain string and no path components", async () => {
  const source = readFileSync(join(import.meta.dir, "..", "src", "pipeline_v2_agent_output_content.ts"), "utf8");
  expect(source).not.toContain("pipeline-v2-output");
  expect(source).not.toContain("writeBigUInt64BE");
  expect(source).not.toContain("activations");
  expect(source).not.toContain("data/outputs");
});

// =============================================================================
// exact full-envelope bounds (real owners, r4 §6)
// =============================================================================

const MAX_LEN = 128;
const MAX_RUN_ID = "r".repeat(MAX_LEN);
const MAX_STATE_ID = "s".repeat(MAX_LEN);
const MAX_OUTPUT_ID = "o".repeat(MAX_LEN);

const numericFieldExpansion = (value: number): number => 16 - String(value).length;

/**
 * The envelope fixture: a finalized run whose run id, state id and output
 * id all sit at the 128-character safe-id maximum and whose accepted
 * output artifact is the exact 262 144-byte max-file; the served chunk is
 * the full contract maximum.
 */
async function buildMaxEnvelopeFixture(): Promise<{ root: string; bundle: string; stateRoot: string; runId: string; stateId: string; outputId: string; artifactBytes: Uint8Array }> {
  const root = mkdtempSync(join(tmpdir(), "p2-envelope-"));
  const bundle = join(root, "bundle");
  mkdirSync(join(bundle, "prompts"), { recursive: true, mode: 0o700 });
  writeFileSync(
    join(bundle, "pipeline.yaml"),
    `schema_version: 2
entry_state: ${MAX_STATE_ID}
max_transitions: 20

inputs: []

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: ${MAX_STATE_ID}
      role: planning
      plan_output: ${MAX_OUTPUT_ID}
      stage_wait:
        reason: iteration_wait
        actions:
          - continue_stage

states:
  - id: ${MAX_STATE_ID}
    type: agent
    profile: coder
    prompt: prompts/review.md
    inputs: []
    outputs:
      - id: ${MAX_OUTPUT_ID}
        type: json
        schema: schemas/verdict.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - {outcome: completed, to: done}
  - id: done
    type: terminal
    result: success
`,
    { mode: 0o600 },
  );
  mkdirSync(join(bundle, "schemas"), { recursive: true, mode: 0o700 });
  writeFileSync(join(bundle, "schemas", "verdict.schema.json"), '{"type":"object"}\n', { mode: 0o600 });
  writeFileSync(join(bundle, "prompts", "review.md"), `REVIEW ${CANARY}\n`, { mode: 0o600 });
  const configRoot = join(root, "config");
  mkdirSync(join(configRoot, "profiles"), { recursive: true, mode: 0o700 });
  mkdirSync(join(configRoot, "opencode"), { recursive: true, mode: 0o700 });
  writeFileSync(
    join(configRoot, "profiles", "coder.yaml"),
    "schema_version: 2\nimage: ghcr.io/example/worker@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08\nopencode_config:\n  path: opencode/coder.json\n  secrets: env_only\nenv: {}\n",
    { mode: 0o600 },
  );
  writeFileSync(join(configRoot, "opencode", "coder.json"), "{}", { mode: 0o600 });
  const pipeline = await loadPipelineV2(bundle);
  const stateRoot = join(root, "state");
  const runRoot = join(stateRoot, "pipeline-runs", MAX_RUN_ID);
  const sink = new PipelineV2RunStateSink({ stateRoot, runId: MAX_RUN_ID, now: nextTick });
  await sink.dispatch({
    kind: "create_run",
    runId: MAX_RUN_ID,
    pipeline: pipelineV2RunPipelineIdentity(pipeline),
    inputs: [],
    projectProvenance: V2_PROJECT_PROVENANCE_BINDING,
    profileEpoch: V2_PROFILE_EPOCH_BINDING,
  });
  await sink.dispatch({ kind: "start_agent_execution", stateId: MAX_STATE_ID, profile: "coder", executionRole: "planning" });
  for (const command of [
    { kind: "agent_data_prepared" },
    { kind: "agent_execution_session_created", sessionId: `sess-${MAX_RUN_ID.slice(0, 24)}` },
    { kind: "agent_tool_session_created", sessionId: `tool-${MAX_RUN_ID.slice(0, 24)}` },
    { kind: "agent_running" },
  ]) {
    await sink.dispatch(command as never);
  }
  const artifactBytes = fixtureBytes(SEED_JSON, 262_144);
  const outputsRoot = join(runRoot, "activations", `1-${MAX_STATE_ID}`, "data", "outputs");
  mkdirSync(outputsRoot, { recursive: true, mode: 0o700 });
  writeFileSync(join(outputsRoot, MAX_OUTPUT_ID), artifactBytes, { mode: 0o600 });
  const digest = await acceptedOutputDigest("json", join(outputsRoot, MAX_OUTPUT_ID), "envelope artifact");
  await sink.dispatch({
    kind: "agent_outputs_accepted",
    outputs: [{ id: MAX_OUTPUT_ID, digest }],
    workerLog: { status: "unavailable" },
  } as never);
  await sink.dispatch({ kind: "agent_cleanup_completed" } as never);
  await sink.dispatch({
    kind: "transition_committed",
    step: { from: MAX_STATE_ID, outcome: "completed", to: "done", transition_index: 0 },
    executionIndex: 1,
  } as never);
  await sink.dispatch({ kind: "terminal_reached", terminalStateId: "done", terminalResult: "success" } as never);
  await sink.dispatch({ kind: "run_outputs_published", outputs: [] } as never);
  await sink.dispatch({ kind: "run_succeeded" } as never);
  return { root, bundle, stateRoot, runId: MAX_RUN_ID, stateId: MAX_STATE_ID, outputId: MAX_OUTPUT_ID, artifactBytes };
}

test("the exact 349 528-byte base64 max payload serves byte-exactly through the facade", async () => {
  const fixture = await buildMaxEnvelopeFixture();
  try {
    const outcome = await agentOutputContentPipelineV2Run({
      runId: fixture.runId,
      stateRoot: fixture.stateRoot,
      executionIndex: 1,
      outputId: fixture.outputId,
      limit: 262_144,
    });
    if (!outcome.ok) throw new Error(`expected success, got ${outcome.reason}`);
    expect(outcome.byteCount).toBe(262_144);
    expect(outcome.returnedByteCount).toBe(262_144);
    expect(outcome.nextOffset).toBeNull();
    expect(outcome.dataBase64.length).toBe(349_528);
    const decoded = Buffer.from(outcome.dataBase64, "base64");
    expect(decoded.byteLength).toBe(262_144);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 120000);

test("the full-envelope CLI document (max ids, max payload, real dispatcher) stays far below the transport ceilings", async () => {
  const fixture = await buildMaxEnvelopeFixture();
  try {
    const { io, stdout, stderr } = makeRealCliIo(fixture.stateRoot);
    const exit = await runCli(
      [
        "agent-output",
        "--run-id", fixture.runId,
        "--execution-index", "1",
        "--output", fixture.outputId,
        "--config-root", fixture.bundle,
        "--limit", "262144",
        "--json",
      ],
      io,
    );
    if (exit !== 0) {
      throw new Error(`CLI exit ${exit}: ${stdout.join("|").slice(0, 200)} / ${stderr.join("|").slice(0, 200)}`);
    }
    expect(stdout).toHaveLength(1);
    const document = stdout[0] as string;
    expect(document.endsWith("\n")).toBe(true);
    const bytes = Buffer.byteLength(document, "utf8");
    const outcome = JSON.parse(document) as {
      runId: string; stateId: string; outputId: string; executionIndex: number;
      dataBase64: string; byteCount: number; offset: number; nextOffset: number | null;
    };
    expect(outcome.runId).toBe(MAX_RUN_ID);
    expect(outcome.stateId).toBe(MAX_STATE_ID);
    expect(outcome.outputId).toBe(MAX_OUTPUT_ID);
    expect(outcome.dataBase64.length).toBe(349_528);
    // The permanent assertion measures the full document from the real
    // production owner; the manual §6 formula is the documented
    // expectation, never the oracle.
    expect(bytes, "measured CLI document bytes (in-process envelope values)")
      .toBeLessThanOrEqual(4_000_000);
    // The grammar-derived worst case: measured envelope values plus the
    // exact expansion allowance of every unachieved 16-digit numeric.
    const expansion =
      numericFieldExpansion(outcome.executionIndex) +
      numericFieldExpansion(outcome.byteCount) +
      numericFieldExpansion(outcome.offset) +
      // a null nextOffset consumes 4 characters and cannot expand
      (outcome.nextOffset === null ? 0 : numericFieldExpansion(outcome.nextOffset));
    expect(bytes + expansion, "derived grammar-max CLI document + LF").toBeLessThanOrEqual(350_246);
    expect(bytes + expansion).toBeLessThanOrEqual(4_000_000);
    // The §6 empty-payload envelope figure is the honest decomposition
    // bound for this construction: envelope (doc minus payload) <= 718.
    expect(bytes - 349_528, "CLI envelope without the max payload").toBeLessThanOrEqual(718);
    expect(stderr.join("")).toBe("");
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 120000);

test("the full-envelope HTTP response document (real service) stays far below the UI reader ceiling", async () => {
  const fixture = await buildMaxEnvelopeFixture();
  try {
    const { response } = await driveAgentOutputHttp(fixture, `/v1/runs/${fixture.runId}/agent-outputs/1/${fixture.outputId}?limit=262144`);
    expect(response.status).toBe(200);
    const text = await response.text();
    const document = JSON.parse(text) as {
      schema_version: number; kind: string; run_id: string; state_id: string; output_id: string;
      data_base64: string; byte_count: number; offset: number; next_offset: number | null;
    };
    expect(document.schema_version).toBe(1);
    expect(document.kind).toBe("agent_output");
    expect(document.run_id).toBe(MAX_RUN_ID);
    expect(document.state_id).toBe(MAX_STATE_ID);
    expect(document.output_id).toBe(MAX_OUTPUT_ID);
    expect(document.data_base64.length).toBe(349_528);
    const bytes = Buffer.byteLength(text, "utf8");
    expect(bytes, "measured HTTP response bytes").toBeLessThanOrEqual(1_048_576);
    const expansion =
      numericFieldExpansion(1) +
      numericFieldExpansion(document.byte_count) +
      numericFieldExpansion(document.offset) +
      // a null next_offset consumes 4 characters and cannot expand
      (document.next_offset === null ? 0 : numericFieldExpansion(document.next_offset));
    expect(bytes + expansion, "derived grammar-max HTTP document").toBeLessThanOrEqual(350_254);
    expect(bytes + expansion).toBeLessThanOrEqual(1_048_576);
    expect(bytes - 349_528, "HTTP envelope without the max payload").toBeLessThanOrEqual(726);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}, 120000);

test("the payload-degraded empty-payload envelope cross-check agrees with the §6 figures", () => {
  // 4 * ceil(262144/3) == 349 528; envelope 718 (CLI+LF) / 726 (HTTP).
  expect(4 * Math.ceil(262144 / 3)).toBe(349_528);
  const cliEnvelope = 718;
  const httpEnvelope = 726;
  expect(cliEnvelope + 349_528).toBe(350_246);
  expect(httpEnvelope + 349_528).toBe(350_254);
});

// =============================================================================
// real-owner helpers (mirroring the P-1 evidence practice)
// =============================================================================

function makeRealCliIo(stateRoot: string): { io: CliIo; stdout: string[]; stderr: string[] } {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const io: CliIo = {
    baseEnv: { PATH: "/usr/bin:/bin" },
    runner: { run: () => Promise.resolve({ code: 0 }), killActive: () => false },
    fetchAuth: () => Promise.resolve({ status: 200, body: {} }),
    resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: "/creds/token" }),
    resolveStateRootProjection: () => ({ localRoot: stateRoot, daemonRoot: stateRoot }),
    runPipelineV2: (async () => { throw new Error("fixture"); }) as unknown as CliIo["runPipelineV2"],
    resumePipelineV2: (async () => { throw new Error("fixture"); }) as unknown as CliIo["resumePipelineV2"],
    continuePipelineV2Stage: (async () => { throw new Error("fixture"); }) as unknown as CliIo["continuePipelineV2Stage"],
    revisePipelineV2Task: (async () => { throw new Error("fixture"); }) as unknown as CliIo["revisePipelineV2Task"],
    resumePipelineV2PlanningRunPlan: (async () => { throw new Error("fixture"); }) as unknown as CliIo["resumePipelineV2PlanningRunPlan"],
    respondPipelineV2Wait: (async () => { throw new Error("fixture"); }) as unknown as CliIo["respondPipelineV2Wait"],
    inspectPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["inspectPipelineV2Run"],
    eventsPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["eventsPipelineV2Run"],
    identityPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["identityPipelineV2Run"],
    logsPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["logsPipelineV2Run"],
    projectChangesPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["projectChangesPipelineV2Run"],
    projectChangeContentPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["projectChangeContentPipelineV2Run"],
    taskRevisionContentPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["taskRevisionContentPipelineV2Run"],
    planRevisionContentPipelineV2Run: (async () => { throw new Error("fixture"); }) as unknown as CliIo["planRevisionContentPipelineV2Run"],
    agentOutputContentPipelineV2Run,
    projectApplyPipelineV2: (async () => { throw new Error("fixture"); }) as unknown as CliIo["projectApplyPipelineV2"],
    projectApplyInspectPipelineV2: (async () => { throw new Error("fixture"); }) as unknown as CliIo["projectApplyInspectPipelineV2"],
    runSmoke: (async () => { throw new Error("fixture"); }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => { throw new Error("fixture"); }) as unknown as CliIo["runAgentSmoke"],
    readTaskFile: async () => { throw new Error("fixture"); },
    writeStdout: (text: string) => { stdout.push(text); },
    writeError: (text: string) => { stderr.push(text); },
  };
  return { io, stdout, stderr };
}

async function driveAgentOutputHttp(
  fixture: { root: string; bundle: string; stateRoot: string; runId: string },
  pathAndQuery: string,
): Promise<{ response: Response }> {
  const registryPath = join(fixture.root, "registry.json");
  const projectDir = join(fixture.root, "project");
  mkdirSync(projectDir, { recursive: true, mode: 0o700 });
  writeFileSync(
    registryPath,
    JSON.stringify({
      schema_version: 1,
      projects: [
        { id: "project-a", project_root: projectDir, config_root: fixture.bundle, pipeline_root: null, launcher_id: null, input_roots: [] },
      ],
    }),
    { mode: 0o600 },
  );
  const registry = await loadControlPlaneProjectRegistry(registryPath);
  const bindingRoot = join(fixture.root, "binding-store");
  mkdirSync(bindingRoot, { recursive: true, mode: 0o700 });
  const bindingStore = await ControlPlaneRunBindingStore.open(bindingRoot);
  await bindingStore.claim({ runId: fixture.runId, projectId: "project-a" });
  const realClient = new OrchestratorSubprocessClient({
    bunExecutable: process.execPath,
    orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
    workingDirectory: fixture.root,
    configRoot: fixture.bundle,
    baseEnv: { PATH: "/usr/bin:/bin", ORCHESTRATOR_STATE_ROOT: fixture.stateRoot },
    stdoutLimitBytes: 4_000_000,
    stderrLimitBytes: 1_000_000,
    sigintGraceMs: 2000,
  });
  const client = {
    run: async (command: OrchestratorCommand): Promise<OrchestratorCallResult> => {
      let result = await realClient.run(command);
      for (let attempt = 1; !result.ok && result.failure.reason === "malformed_output" && attempt <= 3; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        result = await realClient.run(command);
      }
      return result;
    },
  } as OrchestratorSubprocessClient;
  const port = 4799;
  const deps = {
    registry,
    bindingStore,
    client,
    assets: loadControlPlaneUiAssets(defaultControlPlaneUiRoot()),
    port,
    bodyLimitBytes: 4096,
    applyStoreRoot: null,
  };
  const request = new Request(`http://127.0.0.1:${port}${pathAndQuery}`, {
    method: "GET",
    headers: { host: `127.0.0.1:${port}` },
  });
  const response = await handleControlPlaneHttpRequest(deps as never, request);
  return { response };
}
