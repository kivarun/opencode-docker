/**
 * Explicitly internal provenance substrate for the pipeline v2 run plan
 * manifest and binding modules (unwired).
 *
 * Both public modules (`pipeline_v2_run_plan_manifests.ts` and
 * `pipeline_v2_run_plan_bindings.ts`) share this module-private registry:
 * the manifest module registers every deep-frozen prepared object it
 * returns together with its manifest kind, and the binding module gates
 * every validator on a registry lookup before any field of the argument
 * is read. The registry is keyed by object identity, so hand-built
 * look-alikes, casts, shallow or deep clones (e.g. `structuredClone`),
 * objects of another manifest kind and Proxies are all unregistered and
 * rejected with the argument's getters and Proxy traps never invoked.
 *
 * The registry is never exported as a value, never serialized, and no
 * provenance marker is embedded in any prepared object. The check is a
 * pure boolean lookup — the error classes stay with the calling modules.
 */

export type PipelineV2RunPlanProvenanceKind =
  | "plan_revision"
  | "task_revision"
  | "continue_stage_intent"
  | "revise_task_intent";

const preparedProvenance = new WeakMap<object, PipelineV2RunPlanProvenanceKind>();

/**
 * Registers the exact deep-frozen prepared object this substrate is about
 * to return, together with its manifest kind. Called only by the manifest
 * module immediately before a successful return.
 */
export function registerPreparedRunPlanObject(
  value: object,
  kind: PipelineV2RunPlanProvenanceKind,
): void {
  preparedProvenance.set(value, kind);
}

/**
 * Pure registry lookup for the binding module: the argument must be the
 * exact registered prepared object of the expected manifest kind. Getters
 * and Proxy traps of the argument are never invoked.
 */
export function hasPreparedRunPlanProvenance(
  value: unknown,
  kind: PipelineV2RunPlanProvenanceKind,
): boolean {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return preparedProvenance.get(value) === kind;
}
