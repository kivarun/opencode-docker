/**
 * The single stage-task resolver of pipeline schema v2.
 *
 * A stage-role agent state may declare the parameterless `stage_task: {}`
 * input source. Its worker-visible value is the exact UTF-8 body of the
 * current accepted task revision of the state's stage, resolved exclusively
 * from the trusted durable orchestration context — never from a caller
 * field, a worker output or a filesystem path chosen by anyone:
 *
 *   current durable generation (open, with its open iteration)
 *   → the generation's bound accepted plan revision/digest
 *   → the compiled plan stage at the generation's stage id/position/template
 *   → the stage's exact task pointer (single-task contract)
 *   → the exact task revision manifest from the existing run-plan store
 *
 * The resolver owns no parsing, no second digest owner for task manifests
 * and no store protocol of its own: the plan and task manifests are loaded
 * and verified exclusively through the existing official chain
 * (`restorePipelineV2AcceptedRunPlan` over the run-plan store and manifest
 * substrate), and the only new digest is the digest of the exact
 * worker-visible body bytes over the dedicated closed domain
 * `pipeline-v2-stage-task-body\0` (length-framed), which this module owns
 * once.
 *
 * Every resolution failure is a typed `PipelineV2RuntimeError` with the
 * stable reason `stage_task_invalid`: a missing or closed generation, a
 * generation/plan/stage binding mismatch, a zero-task or multi-task stage,
 * a missing, non-canonical or digest-divergent task manifest, or a task
 * revision that is not an accepted task ledger record. The diagnostics are
 * content-free: safe ids, revisions and digest hex only — never a task
 * body, a host path or a raw system error.
 *
 * The resolved context is deep-frozen and content-free; the exact
 * worker-visible bytes stay module-private (a WeakMap keyed by the exact
 * context object) and are released only to the activation data plane
 * through `preparedStageTaskBodyBytes`. Hand-built look-alikes, clones and
 * Proxies are rejected there like everywhere else in this codebase.
 */
import { createHash } from "node:crypto";
import { PipelineV2RuntimeError } from "./pipeline_v2_runtime_error.ts";
import {
  validatePipelineV2RunState,
  pipelineV2OpenStageIteration,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";
import {
  loadPipelineV2TaskRevision,
  PipelineV2RunPlanStoreError,
} from "./pipeline_v2_run_plan_store.ts";
import { PipelineV2RunPlanManifestError } from "./pipeline_v2_run_plan_manifests.ts";
import { restorePipelineV2AcceptedRunPlan, PipelineV2RunPlanRestoreError } from "./pipeline_v2_run_plan_restore.ts";

/** The dedicated digest domain of the exact worker-visible task body bytes. */
const STAGE_TASK_BODY_DIGEST_DOMAIN = "pipeline-v2-stage-task-body\0";

/** The stable runtime failure reason of every stage-task resolution failure. */
export const PIPELINE_V2_STAGE_TASK_FAILURE_REASON = "stage_task_invalid";

/** The deep-frozen, content-free orchestration identity of one stage activation. */
export interface PreparedStageTaskContext {
  readonly plan_revision: number;
  readonly plan_sha256: string;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly stage_template: string;
  readonly task_id: string;
  readonly task_revision: number;
  readonly task_manifest_sha256: string;
  /** Digest of the exact worker-visible body bytes (`pipeline-v2-stage-task-body` domain). */
  readonly body_sha256: string;
  readonly generation_index: number;
  readonly iteration_index: number;
}

/** The exact worker-visible bytes of one resolved context, keyed by identity. */
const bodyBytesByContext = new WeakMap<
  PreparedStageTaskContext,
  { readonly bytes: Uint8Array; readonly pipeline: ResolvedPipelineV2 }
>();

function stageTaskFailure(message: string): PipelineV2RuntimeError {
  return new PipelineV2RuntimeError(PIPELINE_V2_STAGE_TASK_FAILURE_REASON, message);
}

/** Digests the exact worker-visible body bytes with the dedicated closed framing. */
export function stageTaskBodyDigest(bytes: Uint8Array): string {
  const hasher = createHash("sha256");
  const length = Buffer.alloc(8);
  length.writeBigUInt64BE(BigInt(bytes.byteLength));
  hasher.update(Buffer.from(STAGE_TASK_BODY_DIGEST_DOMAIN, "utf8"));
  hasher.update(length);
  hasher.update(bytes);
  return hasher.digest("hex");
}

/**
 * Returns the exact worker-visible body bytes of a context this module
 * resolved for exactly the given trusted pipeline, or `null` for any other
 * object or pipeline (hand-built look-alikes, clones, spreads and Proxies
 * never acquire provenance). The activation data plane is the only
 * intended consumer.
 */
export function preparedStageTaskBodyBytes(
  context: PreparedStageTaskContext,
  pipeline: ResolvedPipelineV2,
): Uint8Array | null {
  const entry = bodyBytesByContext.get(context);
  return entry !== undefined && entry.pipeline === pipeline ? entry.bytes : null;
}

/**
 * Resolves the stage-task context of the current durable orchestration
 * state: the open generation's bound accepted plan revision, its stage at
 * the generation's id/position/template, the stage's single task pointer,
 * and the exact task revision manifest behind that pointer. Read-only over
 * the run-plan store; nothing is created, modified or repaired, and a
 * damaged or missing manifest fails closed.
 *
 * The durable `state` is validated by the single state validator first;
 * the resolved context is derived from the validated snapshot and the
 * provenance-backed compiled plan only. The returned context carries no
 * body bytes, no paths and no host identity — only the content-free
 * identity fields the durable binding and the execution document need.
 */
export async function resolvePipelineV2StageTaskContext(options: {
  readonly pipeline: ResolvedPipelineV2;
  readonly state: PipelineV2RunState;
  readonly runRoot: string;
}): Promise<PreparedStageTaskContext> {
  const { pipeline, runRoot } = options;
  requireResolvedPipelineV2Provenance(pipeline, "stage task context resolution");
  const state = validatePipelineV2RunState(options.state);
  if (typeof runRoot !== "string" || runRoot === "") {
    throw stageTaskFailure("the stage task context requires a non-empty run root");
  }
  if (state.status !== "active" || state.phase !== "running") {
    throw stageTaskFailure(
      `the stage task context requires an active running run, got status ${JSON.stringify(state.status)} and phase ${JSON.stringify(state.phase)}`,
    );
  }
  if (state.terminal !== undefined || state.failure !== undefined || state.run_outputs !== undefined) {
    throw stageTaskFailure("the stage task context requires a run without a terminal, failure or publication projection");
  }
  const open = pipelineV2OpenStageIteration(state);
  if (open === null) {
    throw stageTaskFailure("the stage task context requires an open stage generation with an open iteration");
  }
  const generation = state.generations.find((record) => record.index === open.generation_index);
  if (generation === undefined || generation.closed !== undefined) {
    throw stageTaskFailure(
      `the open generation ${open.generation_index} of the stage task context is not an open durable generation record`,
    );
  }

  let compiledPlan;
  try {
    const restored = await restorePipelineV2AcceptedRunPlan({ pipeline, runRoot, state });
    compiledPlan = restored.compiled_plan;
  } catch (cause) {
    if (
      cause instanceof PipelineV2RunPlanRestoreError ||
      cause instanceof PipelineV2RunPlanStoreError ||
      cause instanceof PipelineV2RunPlanManifestError
    ) {
      const reason =
        typeof (cause as { reason?: unknown }).reason === "string"
          ? (cause as { reason: string }).reason
          : cause.name;
      throw stageTaskFailure(
        `the accepted run plan of the open generation could not be restored (typed failure ${reason})`,
      );
    }
    throw cause;
  }
  if (compiledPlan.plan_sha256 !== generation.plan_sha256) {
    throw stageTaskFailure(
      "the restored accepted plan digest does not match the open generation's bound plan digest",
    );
  }
  const stagePosition = generation.stage_position;
  const compiledStage = compiledPlan.stages[stagePosition - 1];
  if (
    compiledStage === undefined ||
    compiledStage.id !== generation.stage_id ||
    compiledStage.template !== generation.template_id
  ) {
    throw stageTaskFailure(
      `the accepted plan does not bind stage ${JSON.stringify(generation.stage_id)} at position ${stagePosition} with template ${JSON.stringify(generation.template_id)}`,
    );
  }
  if (compiledStage.tasks.length === 0) {
    throw stageTaskFailure(
      `stage ${JSON.stringify(compiledStage.id)} of the accepted plan declares no task; the stage_task source requires exactly one task`,
    );
  }
  if (compiledStage.tasks.length > 1) {
    throw stageTaskFailure(
      `stage ${JSON.stringify(compiledStage.id)} of the accepted plan declares ${compiledStage.tasks.length} tasks; the stage_task source requires exactly one task`,
    );
  }
  const pointer = compiledStage.tasks[0]!;

  let preparedTask;
  try {
    preparedTask = await loadPipelineV2TaskRevision(runRoot, pointer.id, pointer.revision);
  } catch (cause) {
    if (cause instanceof PipelineV2RunPlanStoreError) {
      throw stageTaskFailure(
        `the task revision manifest of task ${JSON.stringify(pointer.id)} revision ${pointer.revision} could not be loaded (typed outcome ${cause.outcome}, reason ${cause.reason})`,
      );
    }
    if (cause instanceof PipelineV2RunPlanManifestError) {
      throw stageTaskFailure(
        `the task revision manifest of task ${JSON.stringify(pointer.id)} revision ${pointer.revision} could not be loaded (manifest validation failure)`,
      );
    }
    throw cause;
  }
  if (preparedTask === null) {
    throw stageTaskFailure(
      `the task revision manifest of task ${JSON.stringify(pointer.id)} revision ${pointer.revision} is missing`,
    );
  }
  const manifest = preparedTask.task.manifest;
  if (manifest.task_id !== pointer.id || manifest.revision !== pointer.revision) {
    throw stageTaskFailure(
      `the loaded task revision manifest does not bind task ${JSON.stringify(pointer.id)} revision ${pointer.revision}`,
    );
  }
  if (preparedTask.task.sha256 !== pointer.sha256) {
    throw stageTaskFailure(
      `the task revision manifest of task ${JSON.stringify(pointer.id)} revision ${pointer.revision} does not carry the digest bound by the accepted plan pointer`,
    );
  }
  const ledgerMatches = state.task_revisions.filter(
    (record) =>
      record.task_id === pointer.id &&
      record.revision === pointer.revision &&
      record.sha256 === pointer.sha256,
  );
  if (ledgerMatches.length !== 1) {
    throw stageTaskFailure(
      `task ${JSON.stringify(pointer.id)} revision ${pointer.revision} is not recorded exactly once in the task ledger with the pointer digest`,
    );
  }
  const body = manifest.body;
  if (typeof body !== "string" || body === "") {
    throw stageTaskFailure(
      `the task revision manifest of task ${JSON.stringify(pointer.id)} revision ${pointer.revision} carries no body`,
    );
  }
  const bytes = Buffer.from(body, "utf8");
  const context: PreparedStageTaskContext = Object.freeze({
    plan_revision: compiledPlan.plan_revision,
    plan_sha256: compiledPlan.plan_sha256,
    stage_id: compiledStage.id,
    stage_position: stagePosition,
    stage_template: compiledStage.template,
    task_id: pointer.id,
    task_revision: pointer.revision,
    task_manifest_sha256: pointer.sha256,
    body_sha256: stageTaskBodyDigest(bytes),
    generation_index: open.generation_index,
    iteration_index: open.iteration_index,
  });
  bodyBytesByContext.set(context, { bytes, pipeline });
  return context;
}
