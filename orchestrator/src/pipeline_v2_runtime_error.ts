import { PipelineError } from "./pipeline.ts";
import {
  describeError,
} from "./docker_helper.ts";
import {
  PIPELINE_V2_FAILURE_REASONS,
  type PipelineV2FailureReason,
} from "./pipeline_v2_state.ts";

/**
 * Typed runtime failures of the pipeline v2 data plane.
 *
 * The future coordinator must normalize data-plane outcomes into durable
 * `PipelineV2FailureReason` values without parsing diagnostic texts. The
 * single machine-readable contract is the `reason` property of
 * `PipelineV2RuntimeError`: it is assigned where the semantics of the
 * failing operation are known (never by classifying an error message), it
 * is one of the fixed `PIPELINE_V2_RUNTIME_FAILURE_REASONS`, and that list
 * is a compile-time subset of the state schema v3 failure reasons, so the
 * reason can be recorded durably unchanged.
 *
 * Trust-boundary and internal-contract violations (forged provenance
 * objects, malformed runner-owned records, impossible graph/state/port
 * shapes, invalid caller-supplied indexes or state ids, unexpected
 * evaluator failures) are NOT runtime failures: they stay plain
 * `PipelineError`s and are normalized to `internal_error` by the
 * coordinator, not by this module.
 *
 * This module is substrate only; the production runner does not execute v2
 * pipelines yet and nothing here is wired into durable state — the state
 * will record the reason string only, never the error object, stack or
 * message.
 */

/**
 * The machine-readable failure reasons of the v2 data plane. A strict
 * subset of `PIPELINE_V2_FAILURE_REASONS` (state schema v3): compile-time
 * proof below fails the build if the lists ever diverge.
 */
export const PIPELINE_V2_RUNTIME_FAILURE_REASONS = [
  "run_input_invalid",
  "run_input_modified",
  "activation_prepare_failed",
  "activation_output_invalid",
  "accepted_output_modified",
  "decision_input_invalid",
  "run_output_missing",
  "run_output_invalid",
  "run_output_publish_failed",
] as const;

export type PipelineV2RuntimeFailureReason =
  (typeof PIPELINE_V2_RUNTIME_FAILURE_REASONS)[number];

type RuntimeReasonsSubsetOfStateReasons =
  PipelineV2RuntimeFailureReason extends PipelineV2FailureReason ? true : never;
const RUNTIME_REASONS_ARE_STATE_REASONS: RuntimeReasonsSubsetOfStateReasons = true;
void RUNTIME_REASONS_ARE_STATE_REASONS;

const PIPELINE_V2_RUNTIME_REASON_SET: ReadonlySet<string> = new Set(
  PIPELINE_V2_RUNTIME_FAILURE_REASONS,
);

/**
 * A data-plane failure with its stable machine-readable `reason`. Still an
 * `instanceof PipelineError`, so existing callers and tests that accept a
 * `PipelineError` keep working; the diagnostic `message` is unchanged
 * human text, while `reason` is the only machine contract. The reason is
 * immutable and must be one of the fixed reason list.
 */
export class PipelineV2RuntimeError extends PipelineError {
  declare readonly reason: PipelineV2RuntimeFailureReason;

  constructor(reason: PipelineV2RuntimeFailureReason, message: string) {
    if (!PIPELINE_V2_RUNTIME_REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 runtime failure reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2RuntimeError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

export function isPipelineV2RuntimeError(
  value: unknown,
): value is PipelineV2RuntimeError {
  return value instanceof PipelineV2RuntimeError;
}

/**
 * Builds one typed failure for an explicit failure site: the owner of the
 * operation names the reason where the semantics are known. The message is
 * the unchanged diagnostic text; an optional cause contributes only its
 * safe description (never user data — the callers own the message text).
 */
export function pipelineV2RuntimeFailure(
  reason: PipelineV2RuntimeFailureReason,
  what: string,
  cause?: unknown,
): PipelineV2RuntimeError {
  if (cause === undefined) {
    return new PipelineV2RuntimeError(reason, what);
  }
  return new PipelineV2RuntimeError(reason, `${what}: ${describeError(cause)}`);
}

/**
 * Runs one operation whose whole failure region carries one reason and
 * retags its own `PipelineError` diagnostics into typed runtime failures
 * with that reason, preserving the message byte-for-byte. Never parses
 * messages: the reason is assigned explicitly by the operation owner.
 *
 * Already-typed failures keep their original reason (first assignment
 * wins), so a nested verifier such as the run-input snapshot check reports
 * `run_input_modified` through an outer region unchanged. Exceptions that
 * are not `PipelineError`s — trust-boundary rejections, programmer errors,
 * unexpected evaluator failures — propagate unchanged and are never
 * masked.
 */
export async function withPipelineV2RuntimeReason<T>(
  reason: PipelineV2RuntimeFailureReason,
  operation: () => Promise<T>,
): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof PipelineV2RuntimeError) {
      throw cause;
    }
    if (cause instanceof PipelineError) {
      throw new PipelineV2RuntimeError(reason, cause.message);
    }
    throw cause;
  }
}
