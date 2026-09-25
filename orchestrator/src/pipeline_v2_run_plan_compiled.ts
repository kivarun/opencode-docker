/**
 * Pure compiled run-plan projection for pipeline schema v2 (unwired,
 * production-neutral).
 *
 * Binds one prepared run plan candidate (the exact deep-frozen
 * `PreparedPipelineV2RunPlanCandidate` returned by
 * `preparePipelineV2RunPlanCandidate`) to the trusted compiled orchestration
 * metadata of a resolved v2 pipeline: every plan stage gains its stage
 * template's compiled `entry_state` and full `state_ids`, resolved
 * exclusively through the existing trusted resolver
 * `compiledStageTemplateFor` — no second template resolution, no own
 * topology validator, no re-compilation or re-validation of the pipeline
 * or the candidate. The same template may be reused by several plan
 * stages; stages keep the plan manifest's semantic declaration order and
 * tasks keep the manifest's normalized order (task pointers sorted by id,
 * dependencies sorted by dependency id — both normalized by the manifest
 * preparer).
 *
 * The result is a content-free deep-frozen projection: ids, revision
 * numbers, digests and dependency lists only. Task bodies, canonical JSON,
 * filesystem paths and the prepared objects themselves never enter it;
 * `origin_execution` is transferred as the normalized number only —
 * verifying it against the durable planning execution belongs to the
 * future controller and is not part of this layer.
 *
 * Provenance order is fixed and fail-closed: the pipeline provenance gate
 * (`requireResolvedPipelineV2Provenance`) runs before the candidate
 * provenance gate (`hasPreparedRunPlanCandidateProvenance` — a pure
 * registry lookup, the candidate's getters and Proxy traps never fire),
 * and both run before any manifest field is read. The compiled projection
 * is registered in a module-private registry immediately before the
 * successful return, together with the immutable snapshot of the
 * originating pipeline's durable run identity built exclusively by the
 * existing `pipelineV2RunPipelineIdentity` construction point — so a
 * compiled plan of one pipeline is provenance-bound to that pipeline and
 * cannot be accepted against the durable state of another; the lookup API
 * gates on that registry before any field of the compiled plan is read,
 * and the durable-state consumers compare the hidden originating identity
 * through the single existing `comparePipelineV2RunIdentity`. The hidden
 * identity is not part of the public projection shape. There is no second
 * candidate registry, no public registry/minter/test-seam API, and caller
 * objects are never mutated or frozen.
 *
 * Errors stay with their owners: pipeline provenance failures remain the
 * stable `PipelineError` of the pipeline gate, and orchestration resolver
 * failures (missing orchestration section, unknown stage template) are
 * propagated unchanged as `PipelineV2OrchestrationError` — never masked,
 * never re-classified through message text. Only this layer's own
 * failures are `PipelineV2CompiledRunPlanError` with a closed reason set.
 * Diagnostics are content-free and name only already validated safe ids.
 *
 * Nothing here touches the filesystem, the durable state, the reducer,
 * the coordinator, the runner, the CLI, or schema v7: this is the single
 * prepared projection the future controller will consume.
 */
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";
import {
  compiledStageTemplateFor,
  type CompiledPipelineV2StageTemplate,
} from "./pipeline_v2_orchestration.ts";
import { hasPreparedRunPlanCandidateProvenance } from "./pipeline_v2_run_plan_candidate_internal.ts";
import type { PreparedPipelineV2RunPlanCandidate } from "./pipeline_v2_run_plan_candidate.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import {
  hasCompiledRunPlanProvenance,
  registerCompiledRunPlanIdentity,
} from "./pipeline_v2_run_plan_compiled_internal.ts";

/** The closed reason set of this layer's own failures. */
const PIPELINE_V2_COMPILED_RUN_PLAN_ERROR_REASONS = [
  "invalid_candidate",
  "invalid_plan",
  "invalid_stage_id",
  "stage_not_found",
] as const;

export type PipelineV2CompiledRunPlanErrorReason =
  (typeof PIPELINE_V2_COMPILED_RUN_PLAN_ERROR_REASONS)[number];

const REASON_SET: ReadonlySet<string> = new Set(PIPELINE_V2_COMPILED_RUN_PLAN_ERROR_REASONS);

/**
 * A failure of the compiled run-plan layer with its stable
 * machine-readable `reason`. The reason is assigned where the failing
 * operation's semantics are known (never by classifying message text),
 * immutable, and one of the fixed closed reason set. Errors of the
 * underlying gates and resolvers keep their own classes.
 */
export class PipelineV2CompiledRunPlanError extends Error {
  declare readonly reason: PipelineV2CompiledRunPlanErrorReason;

  constructor(reason: PipelineV2CompiledRunPlanErrorReason, message: string) {
    if (!REASON_SET.has(reason)) {
      throw new TypeError(
        `unknown pipeline v2 compiled run plan error reason ${JSON.stringify(reason)}`,
      );
    }
    super(message);
    this.name = "PipelineV2CompiledRunPlanError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

/** One normalized task pointer of a compiled stage: identity and dependencies only. */
export interface CompiledPipelineV2RunPlanTask {
  readonly id: string;
  readonly revision: number;
  readonly sha256: string;
  readonly depends_on: readonly string[];
}

/** One compiled plan stage: the plan stage bound to its compiled stage template. */
export interface CompiledPipelineV2RunPlanStage {
  readonly id: string;
  readonly template: string;
  readonly entry_state: string;
  readonly state_ids: readonly string[];
  readonly tasks: readonly CompiledPipelineV2RunPlanTask[];
}

/** The content-free compiled run plan projection; deep-frozen and trusted. */
export interface CompiledPipelineV2RunPlan {
  readonly run_id: string;
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly origin_execution: number;
  readonly stages: readonly CompiledPipelineV2RunPlanStage[];
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) deepFreeze(record[key]);
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

const UNTRUSTED_CANDIDATE_DIAGNOSTIC =
  "compilePipelineV2RunPlanCandidate requires the frozen prepared run plan candidate " +
  "returned by preparePipelineV2RunPlanCandidate; hand-built objects, casts, clones and " +
  "Proxies are rejected before any field is read";

const UNTRUSTED_COMPILED_PLAN_DIAGNOSTIC =
  "compiledPipelineV2RunPlanStageFor requires the frozen compiled run plan object " +
  "returned by compilePipelineV2RunPlanCandidate; hand-built objects, casts, clones and " +
  "Proxies are rejected before any field is read";
/**
 * Compiles the trusted run plan candidate against the trusted compiled
 * orchestration metadata: one content-free projection stage per plan
 * stage (semantic declaration order), each bound to its template's
 * compiled entry state and state ids through the existing orchestration
 * resolver, with the manifest's normalized task pointers. Deep-frozen;
 * deterministic for the same normalized inputs.
 */
export function compilePipelineV2RunPlanCandidate(
  pipeline: ResolvedPipelineV2,
  candidate: PreparedPipelineV2RunPlanCandidate,
): CompiledPipelineV2RunPlan {
  requireResolvedPipelineV2Provenance(pipeline, "compilePipelineV2RunPlanCandidate");
  if (!hasPreparedRunPlanCandidateProvenance(candidate)) {
    throw new PipelineV2CompiledRunPlanError("invalid_candidate", UNTRUSTED_CANDIDATE_DIAGNOSTIC);
  }
  const plan = candidate.plan;
  const manifest = plan.manifest;
  const templateCache = new Map<string, CompiledPipelineV2StageTemplate>();
  const stages: CompiledPipelineV2RunPlanStage[] = manifest.stages.map((stage) => {
    const templateId = stage.template;
    let resolvedTemplate = templateCache.get(templateId);
    if (resolvedTemplate === undefined) {
      resolvedTemplate = compiledStageTemplateFor(pipeline, templateId);
      templateCache.set(templateId, resolvedTemplate);
    }
    return deepFreeze({
      id: stage.id,
      template: templateId,
      entry_state: resolvedTemplate.entry_state,
      state_ids: Object.freeze([...resolvedTemplate.state_ids]),
      tasks: Object.freeze(
        stage.tasks.map((pointer) =>
          deepFreeze({
            id: pointer.id,
            revision: pointer.revision,
            sha256: pointer.sha256,
            depends_on: Object.freeze([...pointer.depends_on]),
          }),
        ),
      ),
    });
  });
  const compiled: CompiledPipelineV2RunPlan = deepFreeze({
    run_id: manifest.run_id,
    plan_revision: manifest.revision,
    plan_sha256: plan.sha256,
    origin_execution: manifest.origin_execution,
    stages: Object.freeze(stages),
  });
  // The provenance registration binds the projection to the immutable
  // identity snapshot of its originating pipeline, built exclusively by
  // the existing construction point, immediately before the successful
  // return.
  registerCompiledRunPlanIdentity(compiled, pipelineV2RunPipelineIdentity(pipeline));
  return compiled;
}

/**
 * The exact frozen compiled stage of one trusted compiled run plan,
 * looked up by stage id. The compiled plan provenance gate runs before
 * any field is read; the stage id must satisfy the shared safe-id grammar
 * (an invalid id is never echoed). An unknown stage is a typed
 * content-free error.
 */
export function compiledPipelineV2RunPlanStageFor(
  compiledPlan: CompiledPipelineV2RunPlan,
  stageId: string,
): CompiledPipelineV2RunPlanStage {
  if (!hasCompiledRunPlanProvenance(compiledPlan)) {
    throw new PipelineV2CompiledRunPlanError("invalid_plan", UNTRUSTED_COMPILED_PLAN_DIAGNOSTIC);
  }
  if (!isPipelineV2SafeId(stageId)) {
    throw new PipelineV2CompiledRunPlanError(
      "invalid_stage_id",
      "compiledPipelineV2RunPlanStageFor requires a safe stage id",
    );
  }
  for (const stage of compiledPlan.stages) {
    if (stage.id === stageId) {
      return stage;
    }
  }
  throw new PipelineV2CompiledRunPlanError(
    "stage_not_found",
    `run plan stage ${JSON.stringify(stageId)} is not declared by the compiled plan`,
  );
}
