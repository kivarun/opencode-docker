/**
 * The durable preparation journal schema (`pipeline_v2_apply_state_internal.ts`):
 * the exact field set of one revisioned operation state, the v1 status
 * vocabulary, the prepared/failed coherence rules and the content-free
 * parse diagnostics.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  PIPELINE_V2_APPLY_OPERATION_STATUSES,
  PIPELINE_V2_APPLY_STATE_KIND,
  PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION,
  PipelineV2ApplyStateError,
  parseApplyOperationState,
  validateApplyOperationState,
} from "../src/pipeline_v2_apply_state_internal.ts";

const sha = (content: string): string => createHash("sha256").update(content).digest("hex");

function baseState(): Record<string, unknown> {
  return {
    schema_version: PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION,
    kind: PIPELINE_V2_APPLY_STATE_KIND,
    revision: 1,
    apply_id: "applyone",
    run_id: "runone",
    project_id: "proj",
    observation: sha("o"),
    terminal_revision: 7,
    project_root_binding: sha("b"),
    plan_digest: null,
    plan_step_count: null,
    plan_payload_count: null,
    plan_total_payload_byte_count: null,
    sealed_payload_count: 0,
    status: "preparing",
    failure_reason: null,
  };
}

describe("apply operation state schema", () => {
  test("the v1 status vocabulary is exactly preparing, prepared and failed", () => {
    expect([...PIPELINE_V2_APPLY_OPERATION_STATUSES]).toEqual(["preparing", "prepared", "failed"]);
  });

  test("a preparing state validates and round-trips", () => {
    const state = validateApplyOperationState(baseState());
    expect(state.status).toBe("preparing");
    expect(state.sealed_payload_count).toBe(0);
    expect(parseApplyOperationState(JSON.stringify(state))).toEqual(state);
  });

  test("a prepared state requires its plan digest, projection and every payload sealed", () => {
    const prepared = validateApplyOperationState({
      ...baseState(),
      revision: 5,
      plan_digest: sha("p"),
      plan_step_count: 3,
      plan_payload_count: 2,
      plan_total_payload_byte_count: 10,
      sealed_payload_count: 2,
      status: "prepared",
    });
    expect(prepared.plan_digest).toBe(sha("p"));
    const missingDigest = { ...baseState(), status: "prepared" };
    expect(() => validateApplyOperationState(missingDigest)).toThrow(/must carry its plan digest/);
    const unsealed = {
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 3,
      plan_payload_count: 2,
      plan_total_payload_byte_count: 10,
      sealed_payload_count: 1,
      status: "prepared",
    };
    expect(() => validateApplyOperationState(unsealed)).toThrow(/every payload sealed/);
  });

  test("a failed state requires a reason and no other state carries one", () => {
    const failed = validateApplyOperationState({ ...baseState(), status: "failed", failure_reason: "diverged_project" });
    expect(failed.failure_reason).toBe("diverged_project");
    const failedWithoutReason = { ...baseState(), status: "failed", failure_reason: null };
    expect(() => validateApplyOperationState(failedWithoutReason)).toThrow(/must carry its failure reason/);
    const preparingWithReason = { ...baseState(), failure_reason: "diverged_project" };
    expect(() => validateApplyOperationState(preparingWithReason)).toThrow(/only a failed operation state/);
  });

  test("the plan projection fields are present exactly when the digest is", () => {
    const partial = { ...baseState(), plan_digest: sha("p") };
    expect(() => validateApplyOperationState(partial)).toThrow(/present exactly when the plan digest is/);
    const digestWithoutTotal = {
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 1,
      plan_payload_count: 1,
    };
    expect(() => validateApplyOperationState(digestWithoutTotal)).toThrow(/present exactly when the plan digest is/);
  });

  test("unknown fields are rejected without echoing their names", () => {
    const extra = { ...baseState(), hostile: "value" };
    expect(() => validateApplyOperationState(extra)).toThrow(/unknown field/);
    expect(() => validateApplyOperationState(extra)).not.toThrow(/hostile/);
    const nested = baseState();
    void nested;
  });

  test("scalars and ids are validated fail-closed", () => {
    expect(() => validateApplyOperationState({ ...baseState(), revision: 0 })).toThrow(/positive safe integer/);
    expect(() => validateApplyOperationState({ ...baseState(), apply_id: "../bad" })).toThrow(/safe id/);
    expect(() => validateApplyOperationState({ ...baseState(), observation: "xyz" })).toThrow(/64-hex digest/);
    expect(() => validateApplyOperationState({ ...baseState(), project_root_binding: "/path" })).toThrow(/64-hex digest/);
    expect(() => validateApplyOperationState({ ...baseState(), status: "applying" })).toThrow(/status is not supported/);
    expect(() => validateApplyOperationState({ ...baseState(), sealed_payload_count: -1 })).toThrow(/non-negative safe integer/);
    expect(() => validateApplyOperationState({ ...baseState(), terminal_revision: 0 })).toThrow(/positive safe integer/);
    expect(() => validateApplyOperationState({ ...baseState(), schema_version: 2 })).toThrow(/schema version is not supported/);
  });

  test("malformed JSON keeps the stable content-free diagnostic", () => {
    try {
      parseApplyOperationState("{");
      throw new Error("expected a refusal");
    } catch (cause) {
      expect((cause as PipelineV2ApplyStateError).reason).toBe("invalid_json");
      expect((cause as Error).message).toBe("the stored operation state is not valid JSON");
    }
  });

  test("the validated state is deeply frozen and mutation-resistant", () => {
    const state = validateApplyOperationState(baseState());
    expect(Object.isFrozen(state)).toBe(true);
  });
});
