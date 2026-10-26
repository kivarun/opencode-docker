import { PipelineV2StateError, type PipelineV2FailureReason } from "./pipeline_v2_state.ts";
import { PipelineV2RunStateStoreError } from "./pipeline_v2_state_store.ts";

/**
 * The single owner of the public machine-readable failure vocabulary of the
 * pipeline v2 run outcomes and of the one mapping from typed internal
 * failures to a public reason.
 *
 * Every pipeline v2 command outcome with a nonzero exit code carries exactly
 * one `reason` drawn from this closed vocabulary; success, the controlled
 * waiting suspension and the controlled plan-ready suspension carry exit
 * code 0 and no reason. The vocabulary preserves every reason that is
 * already public — the durable failure reasons of the run state schema, the
 * coordinator's pre-resume refusal reasons and the fresh-run collision
 * reason — and adds the minimal general categories the runner boundary needs
 * (`run_not_found`, `invalid_options`, `invalid_result`,
 * `artifact_missing`, `artifact_modified` and the durable cleanup-failure
 * finalization name `run_cleanup_failed`).
 *
 * The mapping never reads `error.message`, never parses diagnostics and
 * never classifies by substring: a typed error class contributes its own
 * closed `reason` (validated against this vocabulary, or translated through
 * one explicit internal-reason table), and everything untyped falls back to
 * the category the failure site owns (caller configuration problems to
 * `invalid_options`, unexpected failures to `internal_error`).
 */

export const PIPELINE_V2_PUBLIC_REASONS = [
  // the durable failure reasons of the run state schema (already public)
  ...[
    "internal_error",
    "run_input_invalid",
    "run_input_modified",
    "activation_prepare_failed",
    "worker_failed",
    "worker_timeout",
    "activation_output_invalid",
    "accepted_output_modified",
    "decision_input_invalid",
    "unknown_outcome",
    "invalid_outcome",
    "invalid_executor",
    "transition_budget_exhausted",
    "invalid_graph",
    "missing_state",
    "run_output_missing",
    "run_output_invalid",
    "run_output_publish_failed",
    "state_persist_failed",
    "session_cleanup_failed",
    "signal_sigint",
    "signal_sigterm",
    "terminal_failed",
  ] as const,
  // the coordinator's pre-resume refusal reasons (already public)
  ...[
    "sink_poisoned",
    "run_id_mismatch",
    "invalid_state",
    "pipeline_mismatch",
    "run_layout_invalid",
  ] as const,
  // the minimal general categories of the runner boundary
  ...[
    "run_already_exists",
    "run_not_found",
    "run_busy",
    "project_busy",
    "invalid_options",
    "invalid_result",
    "run_cleanup_failed",
    "artifact_missing",
    "artifact_modified",
  ] as const,
] as const;

export type PipelineV2PublicReason = (typeof PIPELINE_V2_PUBLIC_REASONS)[number];

const PUBLIC_REASON_SET: ReadonlySet<string> = new Set<string>(PIPELINE_V2_PUBLIC_REASONS);

export function isPipelineV2PublicReason(value: unknown): value is PipelineV2PublicReason {
  return typeof value === "string" && PUBLIC_REASON_SET.has(value);
}

// Compile-time exhaustiveness: every already-public reason family is a
// subset of the public vocabulary, and every failure reason of the run
// state schema is public byte-for-byte.
type _EveryFailureReasonIsPublic = Exclude<PipelineV2FailureReason, PipelineV2PublicReason> extends never
  ? true
  : never;
const _FAILURE_REASONS_ARE_PUBLIC: _EveryFailureReasonIsPublic = true;
void _FAILURE_REASONS_ARE_PUBLIC;

/**
 * Internal controller reasons that have no public name of their own,
 * translated to the closest established public category. Only values of
 * this table are ever translated; an unknown internal reason never leaves
 * the runner boundary — the site fallback applies instead.
 */
const INTERNAL_REASON_TO_PUBLIC: Readonly<Record<string, PipelineV2PublicReason>> = {
  // caller-supplied or caller-derived material is invalid
  invalid_intent: "invalid_options",
  invalid_request: "invalid_options",
  invalid_response: "invalid_options",
  invalid_stage_id: "invalid_options",
  stage_not_found: "invalid_options",
  // stale, progressed or conflicting durable lifecycle state
  intent_conflict: "invalid_state",
  candidate_conflict: "invalid_state",
  grant_conflict: "invalid_state",
  lifecycle_conflict: "invalid_state",
  revision_conflict: "invalid_state",
  plan_conflict: "invalid_state",
  wait_conflict: "invalid_state",
  // storage-layer failures of the wait publication
  wait_storage_failed: "state_persist_failed",
  // a stored artifact does not carry its recorded content
  artifact_mismatch: "artifact_modified",
  // internal planning-chain invariants
  invalid_candidate: "internal_error",
  invalid_plan: "internal_error",
  candidate_mismatch: "internal_error",
  origin_execution_mismatch: "internal_error",
  origin_role_mismatch: "internal_error",
  construction_conflict: "internal_error",
  malformed_loader_result: "internal_error",
  run_root_mismatch: "internal_error",
};

/**
 * The typed carrier for a failure whose public reason is decided by its
 * nearest owner (for example the run-root verification of the existing-run
 * core): the reason is validated against the closed vocabulary at
 * construction, the message stays content-free.
 */
export class PipelineV2PublicReasonError extends Error {
  readonly reason: PipelineV2PublicReason;
  constructor(reason: PipelineV2PublicReason, message: string) {
    if (!isPipelineV2PublicReason(reason)) {
      throw new TypeError(`unknown pipeline v2 public reason ${JSON.stringify(reason)}`);
    }
    super(message);
    this.name = "PipelineV2PublicReasonError";
    this.reason = reason;
  }
}

/**
 * Thrown only by the authoritative exclusive run-leaf claim of the fresh
 * runner when the run id already names an existing object at
 * `<state-root>/pipeline-runs/<run-id>` (a valid run, an incomplete
 * directory, a symlink, a regular file or any other layout — the claim
 * never inspects the object and never follows a symlink). The fresh-run
 * collision path maps this error to the typed `run_already_exists`
 * outcome; the existing object's state is never opened, read or disclosed.
 */
export class PipelineV2RunRootCollisionError extends Error {
  readonly runId: string;
  readonly localRunRoot: string;
  constructor(runId: string, localRunRoot: string) {
    super(
      `pipeline v2 runner: refusing to reuse an existing object at ${JSON.stringify(localRunRoot)}`,
    );
    this.name = "PipelineV2RunRootCollisionError";
    this.runId = runId;
    this.localRunRoot = localRunRoot;
  }
}

/**
 * The typed store-error predicate, re-exported for the runner's open-time
 * classification so the runner never imports the store module itself.
 */
export function isPipelineV2RunStateStoreError(cause: unknown): cause is PipelineV2RunStateStoreError {
  return cause instanceof PipelineV2RunStateStoreError;
}

/**
 * Maps one typed internal failure to its public reason. Typed classes
 * contribute their own closed reason (validated against the public
 * vocabulary or translated through the explicit internal table); an
 * untyped cause falls back to the category the failure site owns. A
 * hostile error object (a throwing getter, a Proxy) can never make the
 * mapper throw — the fallback applies instead.
 */
export function publicReasonFor(cause: unknown, fallback: PipelineV2PublicReason): PipelineV2PublicReason {
  try {
    if (cause instanceof PipelineV2PublicReasonError) {
      return cause.reason;
    }
    if (cause instanceof PipelineV2RunRootCollisionError) {
      return "run_already_exists";
    }
    if (cause instanceof PipelineV2StateError) {
      return "invalid_state";
    }
    if (cause instanceof PipelineV2RunStateStoreError) {
      return "state_persist_failed";
    }
    if (typeof cause === "object" && cause !== null) {
      const reason = (cause as { reason?: unknown }).reason;
      if (typeof reason === "string") {
        if (isPipelineV2PublicReason(reason)) {
          return reason;
        }
        const mapped = INTERNAL_REASON_TO_PUBLIC[reason];
        if (mapped !== undefined) {
          return mapped;
        }
      }
    }
  } catch {
    // a hostile error object must never break the mapping
  }
  return fallback;
}
