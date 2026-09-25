/**
 * The module-private provenance registry of the compiled run-plan layer
 * (neutral, internal; not exported by any public runtime surface).
 *
 * One `WeakMap` binds every compiled plan projection created by
 * `compilePipelineV2RunPlanCandidate` to an immutable snapshot of the
 * originating pipeline's durable run identity, built exclusively by the
 * existing `pipelineV2RunPipelineIdentity` construction point at compile
 * time. The `WeakSet` of the former provenance model proved only that a
 * projection was created by *some* trusted pipeline; the identity binding
 * makes the originating pipeline part of the projection's provenance, so
 * a compiled plan of one pipeline can be told apart from a compiled plan
 * of another — the durable-state consumers compare the hidden identity
 * against the durable run identity through the single existing
 * `comparePipelineV2RunIdentity`.
 *
 * The map is keyed by the exact projection object; the stored identity is
 * a shallow snapshot copy of the five scalar identity fields, never an
 * alias of the caller's object. There is no second registry, no public
 * minter/reader API, no re-validation and no way to mint provenance for
 * hand-built, cast, spread, cloned or Proxy-wrapped objects.
 */
import type { PipelineV2RunPipelineIdentity } from "./pipeline_v2_state.ts";

const compiledRunPlanIdentities = new WeakMap<
  object,
  PipelineV2RunPipelineIdentity
>();

/**
 * Binds the compiled projection to the immutable identity snapshot of its
 * originating pipeline. The identity must already be the exact record
 * built by `pipelineV2RunPipelineIdentity`; the registry stores a frozen
 * copy so later mutations of any caller object cannot change it.
 */
export function registerCompiledRunPlanIdentity(
  compiled: object,
  identity: PipelineV2RunPipelineIdentity,
): void {
  compiledRunPlanIdentities.set(compiled, Object.freeze({ ...identity }));
}

/**
 * Whether the value is the exact compiled run plan object this layer
 * created. Proxies, clones and hand-built lookalikes are rejected: the
 * registry lookup never fires their traps (a Proxy object is a distinct
 * object from its target, and a WeakMap keyed by the target does not see
 * the Proxy).
 */
export function hasCompiledRunPlanProvenance(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    compiledRunPlanIdentities.has(value as object)
  );
}

/**
 * The immutable originating pipeline identity snapshot of one
 * provenance-backed compiled run plan. Callers must already have proven
 * the compiled plan (the public stage lookup does); an unregistered
 * object is a layer invariant violation and never reaches this point
 * through the public surface.
 */
export function compiledRunPlanOriginIdentity(
  compiled: object,
): PipelineV2RunPipelineIdentity {
  const identity = compiledRunPlanIdentities.get(compiled);
  if (identity === undefined) {
    throw new Error(
      "pipeline v2 compiled run plan invariant violated: the compiled plan has no registered originating identity",
    );
  }
  return identity;
}
