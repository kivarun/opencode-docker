/**
 * Pure compiled orchestration resolver for schema version 2 pipelines.
 *
 * Reads the trusted, loader-owned orchestration metadata of a resolved v2
 * pipeline (the optional `orchestration` section compiled and normalized by
 * `pipeline_v2.ts`) and answers two read-only questions: which compiled
 * execution role a state carries, and which states a stage template owns.
 * The resolver performs no filesystem access, no callbacks, no graph
 * execution and no second graph validation — the topology is statically
 * verified once by the trusted loader; this module only reads the
 * normalized metadata of the already-compiled snapshot.
 *
 * Provenance is checked before anything else: the pipeline argument must be
 * the exact deep-frozen snapshot object a previous successful `loadPipelineV2`
 * call returned (`requireResolvedPipelineV2Provenance`, checked before
 * `stateId`/`templateId` are read and before any pipeline field is read), so
 * hand-built objects, casts, spreads, `structuredClone` results and Proxies
 * are rejected with the stable provenance `PipelineError` and Proxy traps
 * are never invoked. Only afterwards is the requested id checked against
 * the shared v2 safe-id grammar and resolved against the metadata; a
 * pipeline without the orchestration section, an unknown state or an
 * unknown template is a typed `PipelineV2OrchestrationError`. Results are
 * deep-frozen and built only from the normalized metadata; caller objects
 * are never mutated or frozen. Diagnostics name safe ids only — no prompt
 * or model paths, no bodies, no content.
 *
 * This module is not wired into production dispatch: durable state, the
 * coordinator, the runner and the CLI are untouched, and a future
 * controller remains solely responsible for requiring this metadata before
 * any schema-v7 dispatch.
 */
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  requireResolvedPipelineV2Provenance,
  type PipelineV2StageWaitActionName,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";

export class PipelineV2OrchestrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2OrchestrationError";
  }
}

/**
 * The compiled stage-wait policy of one planning role: the trusted wait
 * `reason` and the declared intervention actions in their preserved
 * declaration order, read exclusively from the loader-owned resolved
 * metadata. The routing targets are deliberately not part of the policy
 * (a `continue_stage` target is derived later from the open generation's
 * compiled stage entry, a `revise_task` target from the planning cursor).
 */
export interface CompiledPipelineV2StageWait {
  readonly reason: string;
  readonly actions: readonly PipelineV2StageWaitActionName[];
}

/**
 * The compiled automatic plan-ready continuation policy of one planning
 * role: the trusted 1-based stage position and the initial budget of the
 * opened stage generation, both positive safe integers. Present exactly
 * when the bundle declares `plan_ready` on the planning role; its absence
 * preserves the manual `planReady -> resume-plan` chain.
 */
export interface CompiledPipelineV2PlanReady {
  readonly stage_position: number;
  readonly initial_budget: number;
}

/**
 * The compiled execution role of one state: a planning role bound to
 * exactly one declared JSON output port of its own agent state (the plan
 * proposal output) plus the mandatory trusted stage-wait policy of its
 * intervention wait and the optional automatic plan-ready continuation
 * policy, a control role with no iteration concern, or a stage role bound
 * to exactly one stage template.
 * Stage-template membership is carried by the stage role itself; a stage
 * state belongs to exactly one template by construction.
 */
export type CompiledPipelineV2ExecutionRole =
  | Readonly<{
      state_id: string;
      role: "planning";
      plan_output: string;
      stage_wait: CompiledPipelineV2StageWait;
      plan_ready?: CompiledPipelineV2PlanReady;
    }>
  | Readonly<{ state_id: string; role: "control" }>
  | Readonly<{ state_id: string; role: "stage"; stage_template: string }>;

export interface CompiledPipelineV2StageTemplate {
  readonly id: string;
  readonly entry_state: string;
  /**
   * The stage states of this template, in the normalized role order
   * (sorted by state id) of the trusted metadata.
   */
  readonly state_ids: readonly string[];
}

/**
 * The compiled execution role of one declared state of a trusted resolved
 * pipeline. Missing orchestration metadata and unknown states are typed
 * errors; the result is deep-frozen (a planning role's `stage_wait` and
 * its actions array included, plus its optional `plan_ready` policy as a
 * fresh frozen copy) and repeated calls on the same trusted snapshot
 * are structurally identical.
 */
export function compiledExecutionRoleFor(
  pipeline: ResolvedPipelineV2,
  stateId: string,
): CompiledPipelineV2ExecutionRole {
  requireResolvedPipelineV2Provenance(pipeline, "compiledExecutionRoleFor");
  if (!isPipelineV2SafeId(stateId)) {
    throw new PipelineV2OrchestrationError(
      `compiledExecutionRoleFor requires a safe state id, got ${JSON.stringify(stateId)}`,
    );
  }
  const orchestration = pipeline.orchestration;
  if (orchestration === undefined) {
    throw new PipelineV2OrchestrationError(
      "the trusted pipeline declares no orchestration section; compiled execution roles exist only for pipelines with orchestration metadata",
    );
  }
  for (const entry of orchestration.execution_roles) {
    if (entry.state_id === stateId) {
      return entry.role === "stage"
        ? Object.freeze({ state_id: stateId, role: "stage", stage_template: entry.stage_template })
        : entry.role === "planning"
          ? Object.freeze({
              state_id: stateId,
              role: "planning",
              plan_output: entry.plan_output,
              stage_wait: Object.freeze({
                reason: entry.stage_wait.reason,
                actions: Object.freeze([...entry.stage_wait.actions]),
              }),
              // The optional trusted plan-ready continuation policy is
              // returned as a fresh frozen copy per call, exactly like the
              // stage-wait policy; the key is absent when undeclared.
              ...(entry.plan_ready === undefined
                ? {}
                : {
                    plan_ready: Object.freeze({
                      stage_position: entry.plan_ready.stage_position,
                      initial_budget: entry.plan_ready.initial_budget,
                    }),
                  }),
            })
          : Object.freeze({ state_id: stateId, role: "control" });
    }
  }
  const declared = pipeline.states.find((state) => state.id === stateId);
  throw new PipelineV2OrchestrationError(
    declared === undefined
      ? `state ${JSON.stringify(stateId)} is not declared by the pipeline`
      : `state ${JSON.stringify(stateId)} is a ${declared.type} state; terminal states carry no execution role`,
  );
}

/**
 * The compiled stage template of one declared template id of a trusted
 * resolved pipeline: the template's entry state and its stage states in the
 * normalized role order. Missing orchestration metadata and unknown
 * templates are typed errors; the result is deep-frozen and repeated calls
 * on the same trusted snapshot are structurally identical.
 */
export function compiledStageTemplateFor(
  pipeline: ResolvedPipelineV2,
  templateId: string,
): CompiledPipelineV2StageTemplate {
  requireResolvedPipelineV2Provenance(pipeline, "compiledStageTemplateFor");
  if (!isPipelineV2SafeId(templateId)) {
    throw new PipelineV2OrchestrationError(
      `compiledStageTemplateFor requires a safe template id, got ${JSON.stringify(templateId)}`,
    );
  }
  const orchestration = pipeline.orchestration;
  if (orchestration === undefined) {
    throw new PipelineV2OrchestrationError(
      "the trusted pipeline declares no orchestration section; compiled stage templates exist only for pipelines with orchestration metadata",
    );
  }
  const template = orchestration.stage_templates.find((entry) => entry.id === templateId);
  if (template === undefined) {
    throw new PipelineV2OrchestrationError(
      `stage template ${JSON.stringify(templateId)} is not declared by the pipeline orchestration`,
    );
  }
  const stateIds: string[] = [];
  for (const entry of orchestration.execution_roles) {
    if (entry.role === "stage" && entry.stage_template === templateId) {
      stateIds.push(entry.state_id);
    }
  }
  return Object.freeze({
    id: template.id,
    entry_state: template.entry_state,
    state_ids: Object.freeze(stateIds),
  });
}
