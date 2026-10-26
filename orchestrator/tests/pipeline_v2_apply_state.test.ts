/**
 * The durable apply operation journal schema (`pipeline_v2_apply_state_internal.ts`):
 * the exact field set of one revisioned operation state, the v2 status
 * vocabulary (`preparing|prepared|applying|applied|conflict|
 * recovery_required|failed`), the execution-progress coherence rules
 * (`completed_step_count`, the frontier temp intent), the typed v1
 * rejection and the content-free parse diagnostics.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  PIPELINE_V2_APPLY_EXECUTABLE_STATUSES,
  PIPELINE_V2_APPLY_OPERATION_STATUSES,
  PIPELINE_V2_APPLY_STATE_KIND,
  PIPELINE_V2_APPLY_STATE_SCHEMA_VERSION,
  PIPELINE_V2_APPLY_TERMINAL_RESULT_STATUSES,
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
    execution: null,
  };
}

describe("apply operation state schema v2", () => {
  test("the status vocabulary is exactly the seven lifecycle statuses", () => {
    expect([...PIPELINE_V2_APPLY_OPERATION_STATUSES]).toEqual([
      "preparing",
      "prepared",
      "applying",
      "applied",
      "conflict",
      "recovery_required",
      "failed",
    ]);
    expect([...PIPELINE_V2_APPLY_EXECUTABLE_STATUSES]).toEqual(["prepared", "applying"]);
    expect([...PIPELINE_V2_APPLY_TERMINAL_RESULT_STATUSES]).toEqual([
      "applied",
      "conflict",
      "recovery_required",
      "failed",
    ]);
  });

  test("a preparing state validates and round-trips", () => {
    const state = validateApplyOperationState(baseState());
    expect(state.status).toBe("preparing");
    expect(state.execution).toBe(null);
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
    const missingDigest = { ...baseState(), status: "prepared", execution: null };
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

  test("an applying state carries its execution progress and frontier intent", () => {
    const applying = validateApplyOperationState({
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 3,
      plan_payload_count: 1,
      plan_total_payload_byte_count: 4,
      status: "applying",
      execution: { completed_step_count: 1, active_step: null },
    });
    expect(applying.execution).toEqual({ completed_step_count: 1, active_step: null });
    const withIntent = validateApplyOperationState({
      ...applying,
      execution: { completed_step_count: 1, active_step: { step_index: 2, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null } },
    });
    expect(withIntent.execution?.active_step).toEqual({ step_index: 2, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null });
    expect(parseApplyOperationState(JSON.stringify(withIntent))).toEqual(withIntent);
    const offFrontier = { ...applying, execution: { completed_step_count: 1, active_step: { step_index: 3, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null } } };
    expect(() => validateApplyOperationState(offFrontier)).toThrow(/frontier step/);
    const badName = { ...applying, execution: { completed_step_count: 1, active_step: { step_index: 2, temp_name: "hostile", temp_identity: null } } };
    expect(() => validateApplyOperationState(badName)).toThrow(/temp grammar/);
    const zeroWithIntent = validateApplyOperationState({
      ...applying,
      execution: { completed_step_count: 0, active_step: { step_index: 1, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null } },
    });
    expect(zeroWithIntent.execution?.active_step).toEqual({ step_index: 1, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null });
  });

  test("an applied state is terminal with full progress and no active step", () => {
    const applied = validateApplyOperationState({
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 2,
      plan_payload_count: 1,
      plan_total_payload_byte_count: 4,
      status: "applied",
      execution: { completed_step_count: 2, active_step: null },
    });
    expect(applied.status).toBe("applied");
    const partial = { ...applied, execution: { completed_step_count: 1, active_step: null } };
    expect(() => validateApplyOperationState(partial)).toThrow(/frontier step|exceeds|completed/);
    const withActive = { ...applied, execution: { completed_step_count: 2, active_step: { step_index: 3, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null } } };
    expect(() => validateApplyOperationState(withActive)).toThrow(/active temp intent/);
  });

  test("a conflict state requires zero completed steps; recovery_required may keep its frontier intent", () => {
    const conflict = validateApplyOperationState({
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 2,
      plan_payload_count: 1,
      plan_total_payload_byte_count: 4,
      status: "conflict",
      failure_reason: "project_conflict",
      execution: { completed_step_count: 0, active_step: null },
    });
    expect(conflict.failure_reason).toBe("project_conflict");
    const progressedConflict = { ...conflict, execution: { completed_step_count: 1, active_step: null } };
    expect(() => validateApplyOperationState(progressedConflict)).toThrow(/cannot carry completed steps/);
    const recovery = validateApplyOperationState({
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 2,
      plan_payload_count: 1,
      plan_total_payload_byte_count: 4,
      status: "recovery_required",
      failure_reason: "recovery_required",
      execution: { completed_step_count: 1, active_step: { step_index: 2, temp_name: `.apply-tmp-${"ab".repeat(16)}`, temp_identity: null } },
    });
    expect(recovery.status).toBe("recovery_required");
    const recoveryWithoutReason = { ...recovery, failure_reason: null };
    expect(() => validateApplyOperationState(recoveryWithoutReason)).toThrow(/must carry its failure reason/);
  });

  test("a failed state requires a reason; executable states carry none", () => {
    const failed = validateApplyOperationState({ ...baseState(), status: "failed", failure_reason: "diverged_project" });
    expect(failed.failure_reason).toBe("diverged_project");
    const failedWithoutReason = { ...baseState(), status: "failed", failure_reason: null };
    expect(() => validateApplyOperationState(failedWithoutReason)).toThrow(/must carry its failure reason/);
    const applyingWithReason = {
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 1,
      plan_payload_count: 0,
      plan_total_payload_byte_count: 0,
      status: "applying",
      failure_reason: "project_conflict",
      execution: { completed_step_count: 0, active_step: null },
    };
    expect(() => validateApplyOperationState(applyingWithReason)).toThrow(/carries a failure reason/);
  });

  test("a failed state must not carry execution progress", () => {
    const failedWithProgress = {
      ...baseState(),
      status: "failed",
      failure_reason: "diverged_project",
      execution: { completed_step_count: 1, active_step: null },
    };
    expect(() => validateApplyOperationState(failedWithProgress)).toThrow(/must not carry execution progress/);
    const preparedWithProgress = { ...baseState(), status: "prepared", execution: { completed_step_count: 0, active_step: null } };
    expect(() => validateApplyOperationState(preparedWithProgress)).toThrow(/must not carry execution progress/);
  });

  test("schema version 1 is rejected with the typed legacy message and never migrated", () => {
    const v1 = { ...baseState(), schema_version: 1 };
    delete (v1 as Record<string, unknown>)["execution"];
    try {
      validateApplyOperationState(v1);
      throw new Error("expected a refusal");
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2ApplyStateError);
      expect((cause as Error).message).toBe(
        "apply operation state schema version 1 is not supported (no migration from v1 preparation journals)",
      );
    }
    expect(() => validateApplyOperationState({ ...baseState(), schema_version: 4 })).toThrow(/schema version is not supported/);
  });

  test("schema version 2 is rejected with the typed legacy message and never migrated", () => {
    try {
      validateApplyOperationState({ ...baseState(), schema_version: 2 });
      throw new Error("expected a rejection");
    } catch (cause) {
      expect(cause).toBeInstanceOf(PipelineV2ApplyStateError);
      expect((cause as Error).message).toBe(
        "apply operation state schema version 2 is not supported (no migration from v2 temp-intent journals)",
      );
    }
    expect(() => validateApplyOperationState({ ...baseState(), schema_version: 4 })).toThrow(/schema version is not supported/);
  });

  test("the plan projection fields are present exactly when the digest is", () => {
    const partial = { ...baseState(), plan_digest: sha("p") };
    expect(() => validateApplyOperationState(partial)).toThrow(/present exactly when the plan digest is/);
  });

  test("unknown fields are rejected without echoing their names", () => {
    const extra = { ...baseState(), hostile: "value" };
    expect(() => validateApplyOperationState(extra)).toThrow(/unknown field/);
    expect(() => validateApplyOperationState(extra)).not.toThrow(/hostile/);
  });

  test("scalars and ids are validated fail-closed", () => {
    expect(() => validateApplyOperationState({ ...baseState(), revision: 0 })).toThrow(/positive safe integer/);
    expect(() => validateApplyOperationState({ ...baseState(), apply_id: "../bad" })).toThrow(/safe id/);
    expect(() => validateApplyOperationState({ ...baseState(), observation: "xyz" })).toThrow(/64-hex digest/);
    expect(() => validateApplyOperationState({ ...baseState(), project_root_binding: "/path" })).toThrow(/64-hex digest/);
    expect(() => validateApplyOperationState({ ...baseState(), status: "executing" })).toThrow(/status is not supported/);
    expect(() => validateApplyOperationState({ ...baseState(), sealed_payload_count: -1 })).toThrow(/non-negative safe integer/);
    expect(() => validateApplyOperationState({ ...baseState(), terminal_revision: 0 })).toThrow(/positive safe integer/);
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
    const withIntent = validateApplyOperationState({
      ...baseState(),
      plan_digest: sha("p"),
      plan_step_count: 1,
      plan_payload_count: 0,
      plan_total_payload_byte_count: 0,
      status: "applying",
      execution: { completed_step_count: 0, active_step: null },
    });
    expect(Object.isFrozen(withIntent.execution)).toBe(true);
  });
});
