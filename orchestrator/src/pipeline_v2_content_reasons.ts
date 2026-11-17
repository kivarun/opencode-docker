/**
 * The closed producer reason vocabularies of the read-only content
 * projections — the single owner shared by the content facades (which
 * construct every failure) and the transport wire (which validates every
 * received failure reason without copying the lists). The slice maximum
 * constant is owned here too: the P-2 agent-output CLI parser, wire
 * projection, HTTP query parser and facade all read this one value.
 *
 * The CLI option parser keeps its own `invalid_options` / exit 2;
 * invalid options that reach a content facade past the accepted CLI and
 * routing guards are programmer/invariant failures mapped to
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

/**
 * The closed producer reason vocabulary of the P-2 accepted-agent-output
 * content projection (`agent-output`). `invalid_options` is deliberately
 * absent: malformed caller shapes that reach the facade past the accepted
 * CLI/routing guards are programmer/invariant failures mapped to
 * `internal_error` with zero content.
 */
export const PIPELINE_V2_OUTPUT_CONTENT_REASONS = [
  "run_not_found",
  "run_state_invalid",
  "pipeline_mismatch",
  "unknown_output",
  "artifact_missing",
  "artifact_modified",
  "unsupported_output_kind",
  "invalid_cursor",
  "internal_error",
] as const;

/**
 * The single owner of the bounded raw-byte slice maximum of the P-2
 * agent-output content surface (262 144), shared verbatim by the CLI
 * grammar, the wire projection validation, the HTTP query parser and the
 * facade.
 */
export const PIPELINE_V2_OUTPUT_SLICE_LIMIT_MAX = 262_144;
