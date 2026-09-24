/**
 * The single pure structural comparator of the durable pipeline v2 run
 * identity (neutral, internal).
 *
 * Both consumers compare the same five durable identity fields against
 * the same compiled identity in the same order: the resume runtime
 * context restoration (`pipeline_v2_resume_context.ts`) and the compiled
 * run-plan acceptance boundary (`pipeline_v2_run_plan_acceptance.ts`).
 * There is no second independent comparator; each consumer maps the
 * returned mismatch field to its own typed error, reason and diagnostic,
 * so existing restore reasons, diagnostics and ordering stay unchanged.
 *
 * The comparator is pure and structural: it reads only the two identity
 * records, mutates nothing, and performs no message parsing — a mismatch
 * is decided field by field in the fixed durable field order
 * (`schema_version`, `bundle_root`, `execution_snapshot_sha256`,
 * `entry_state`, `max_transitions`), never by comparing serialized forms
 * or classifying diagnostics.
 */
import type { PipelineV2RunPipelineIdentity } from "./pipeline_v2_state.ts";

/** The five durable identity fields, in the fixed comparison order. */
export type PipelineV2RunIdentityField =
  | "schema_version"
  | "bundle_root"
  | "execution_snapshot_sha256"
  | "entry_state"
  | "max_transitions";

export type PipelineV2RunIdentityComparison =
  | { readonly kind: "match" }
  | { readonly kind: "mismatch"; readonly field: PipelineV2RunIdentityField };

/**
 * Compares the durable identity recorded by `create_run` against the
 * identity of the trusted compiled pipeline and returns the first
 * mismatching field in the fixed field order, or `match` when every
 * field is equal. Pure; no message parsing, no serialization.
 */
export function comparePipelineV2RunIdentity(
  expected: PipelineV2RunPipelineIdentity,
  actual: PipelineV2RunPipelineIdentity,
): PipelineV2RunIdentityComparison {
  if (actual.schema_version !== expected.schema_version) {
    return { kind: "mismatch", field: "schema_version" };
  }
  if (actual.bundle_root !== expected.bundle_root) {
    return { kind: "mismatch", field: "bundle_root" };
  }
  if (actual.execution_snapshot_sha256 !== expected.execution_snapshot_sha256) {
    return { kind: "mismatch", field: "execution_snapshot_sha256" };
  }
  if (actual.entry_state !== expected.entry_state) {
    return { kind: "mismatch", field: "entry_state" };
  }
  if (actual.max_transitions !== expected.max_transitions) {
    return { kind: "mismatch", field: "max_transitions" };
  }
  return { kind: "match" };
}
