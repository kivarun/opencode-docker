/**
 * The durable state schema v12: the mandatory `profile_epoch` binding.
 */
import { expect, test } from "bun:test";
import {
  PIPELINE_V2_RUN_STATE_SCHEMA_VERSION,
  PipelineV2StateError,
  parsePipelineV2RunState,
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { V2_PREPARED_PROFILE_EPOCH, V2_PREPARED_PROFILE_EPOCH_BINDING } from "./helpers/profile_epoch_fixture.ts";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DIGEST = "d".repeat(64);
const BINDING = { digest: DIGEST, profile_count: 1 };

const PROVENANCE = { digest: "e".repeat(64), entry_count: 1, copy_policy_version: 1 as const };

const IDENTITY = {
  schema_version: 2 as const,
  bundle_root: "/opt/orchestrator/pipelines/v2",
  execution_snapshot_sha256: "a".repeat(64),
  entry_state: "architect",
  max_transitions: 12,
};

function createRun(profileEpoch: unknown = BINDING) {
  return reducePipelineV2RunCommand(
    null,
    {
      kind: "create_run",
      runId: "run-epoch",
      pipeline: IDENTITY,
      inputs: [],
      projectProvenance: PROVENANCE,
      profileEpoch: profileEpoch as never,
    },
    new Date("2026-01-01T00:00:01.000Z"),
  );
}

test("state v12: the schema version is 12 and v11 is rejected with the no-migration message", () => {
  expect(PIPELINE_V2_RUN_STATE_SCHEMA_VERSION).toBe(12);
  expect(() => validatePipelineV2RunState({ schema_version: 11 })).toThrow(
    "pipeline v2 run state has schema_version 11, which is unsupported by this orchestrator (schema version 12 is the supported contract; no v11 migration exists)",
  );
  expect(() => parsePipelineV2RunState('{"schema_version":11}')).toThrow(
    "pipeline v2 run state has schema_version 11, which is unsupported by this orchestrator (schema version 12 is the supported contract; no v11 migration exists)",
  );
});

test("state v12: create_run stores the mandatory profile_epoch binding atomically", () => {
  const state = createRun();
  expect(state.profile_epoch).toEqual({ digest: DIGEST, profile_count: 1 });
  expect(state.schema_version).toBe(12);
  const json = JSON.stringify(state);
  expect(json).toContain('"profile_epoch":{"digest":"' + DIGEST + '","profile_count":1}');
  const parsed = parsePipelineV2RunState(json);
  expect(parsed.profile_epoch).toEqual({ digest: DIGEST, profile_count: 1 });
});

test("state v12: the binding is mandatory and validated fail-closed", () => {
  expect(() => createRun(null)).toThrow(PipelineV2StateError);
  expect(() => createRun(null)).toThrow(/create_run profile_epoch/);
  expect(() => createRun({ digest: DIGEST, profile_count: 1, extra: true })).toThrow(/unknown field "extra"/);
  expect(() => createRun({ digest: "D".repeat(64), profile_count: 1 })).toThrow(PipelineV2StateError);
  expect(() => createRun({ digest: DIGEST.slice(0, 63), profile_count: 1 })).toThrow(PipelineV2StateError);
  expect(() => createRun({ digest: DIGEST, profile_count: 1.5 })).toThrow(PipelineV2StateError);
  expect(() => createRun({ digest: DIGEST, profile_count: -1 })).toThrow(PipelineV2StateError);
  expect(() => createRun({ digest: DIGEST, profile_count: Number.MAX_SAFE_INTEGER + 1 })).toThrow(
    PipelineV2StateError,
  );
  // a zero profile count is valid (an entry-terminal pipeline runs no profile)
  const empty = createRun({ digest: DIGEST, profile_count: 0 });
  expect(empty.profile_epoch).toEqual({ digest: DIGEST, profile_count: 0 });
});

test("state v12: the binding survives restart through the store round-trip", async () => {
  const root = await mkdtemp(join(tmpdir(), "epoch-state-"));
  try {
    const stateRoot = join(root, "state");
    const { PipelineV2RunStateSink } = await import("../src/pipeline_v2_state_sink.ts");
    const { PipelineV2RunStateStore } = await import("../src/pipeline_v2_state_store.ts");
    const bundle = join(root, "bundle");
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await writeFile(
      join(bundle, "pipeline.yaml"),
      [
        "schema_version: 2",
        "entry_state: done",
        "max_transitions: 4",
        "",
        "inputs: []",
        "outputs: []",
        "",
        "states:",
        "  - id: done",
        "    type: terminal",
        "    result: success",
        "",
      ].join("\n"),
    );
    const pipeline = await loadPipelineV2(bundle);
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: "epoch-restart" });
    await sink.dispatch({
      kind: "create_run",
      runId: "epoch-restart",
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: [],
      projectProvenance: PROVENANCE,
      profileEpoch: V2_PREPARED_PROFILE_EPOCH_BINDING,
    });
    const reopened = new PipelineV2RunStateStore({ stateRoot, runId: "epoch-restart" });
    const loaded = await reopened.load();
    expect(loaded!.profile_epoch).toEqual(V2_PREPARED_PROFILE_EPOCH_BINDING);
    expect(loaded!.schema_version).toBe(12);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("state v12: the commit history carries no new kind and the epoch entry is the create_run one", () => {
  const state = createRun();
  expect(state.commit_history).toEqual([{ revision: 1, kind: "create_run" }]);
});

test("state v12: the durable document carries only the digest and count, no profile content", () => {
  const state = createRun();
  const json = JSON.stringify(state);
  for (const canary of [
    "image_ref",
    "config_sha256",
    "env_bindings",
    "value_sha256",
    "config_policy",
    "LLM_KEY",
    "LLM_SERVER",
    "opencode",
    "/opt/config",
    "secret-value",
    "sha256:a",
  ]) {
    if (canary === "sha256:a") {
      continue; // the pipeline execution snapshot digest legitimately appears
    }
    expect(json, canary).not.toContain(canary);
  }
});

test("state v12: the loader keeps the stored binding byte-for-byte without any migration", async () => {
  const state: PipelineV2RunState = createRun();
  const json = JSON.stringify(state);
  const v11Document = json.replace('"schema_version":12', '"schema_version":11');
  expect(v11Document).not.toBe(json);
  const before = v11Document.length;
  let rethrown: unknown = null;
  try {
    parsePipelineV2RunState(v11Document);
  } catch (cause) {
    rethrown = cause;
  }
  expect(rethrown).toBeInstanceOf(PipelineV2StateError);
  expect((rethrown as Error).message).toContain("no v11 migration exists");
  expect(v11Document.length).toBe(before);
});
