/**
 * The closed producer reason vocabulary of the P-1 task/plan content
 * projection — the single owner shared by the content facade (which
 * constructs every failure) and the transport wire (which validates
 * every received failure reason without copying the list).
 *
 * The CLI option parser keeps its own `invalid_options` / exit 2;
 * invalid options that reach the content facade past the accepted
 * CLI/routing guards are programmer/invariant failures mapped to
 * `internal_error` with zero content.
 */
export const PIPELINE_V2_RUN_PLAN_CONTENT_REASONS = [
  "run_not_found",
  "run_state_invalid",
  "pipeline_mismatch",
  "unknown_revision",
  "artifact_missing",
  "artifact_modified",
  "invalid_cursor",
  "internal_error",
] as const;
