/**
 * Pure run plan proposal substrate for pipeline schema v2 (unwired).
 *
 * This module fixes the content-free format of the agent-authored
 * planning proposal: the semantic document a planning execution hands to
 * the orchestrator to describe the next run plan. It is deliberately NOT
 * a durable manifest and carries no digest of its own: the proposal
 * holds no run id, no plan or task revision numbers, no predecessor or
 * root-task digests, no origin execution, no task SHA-256 and no
 * wait/generation/iteration bindings — every durable field is derived
 * later by the construction layer from the durable state and the
 * existing manifest preparers. The accepted execution output that
 * carries this document is already bound by its own durable output
 * digest; a second proposal digest would create a parallel identity
 * without adding authority.
 *
 * Responsibility boundary: this layer owns only the representation of
 * the document, its local scalar invariants and a safe snapshot.
 *
 * - Exact own enumerable fields at every level (`schema_version: 1`,
 *   `kind: "run_plan_proposal"`, `stages[] {id, template, tasks[]
 *   {id, depends_on}}`, `new_tasks[] {id, body}`); required fields must
 *   be own enumerable properties, never inherited ones; unknown own
 *   fields are rejected without echoing their names (a canary can hide
 *   in a property name);
 * - safe identifiers (the shared `pipeline_v2_scalar.ts` grammar) for
 *   every stage id, template reference, task id and dependency entry;
 * - a `body` as a non-empty string; a duplicate `new_tasks[].id` is
 *   rejected as the locally ambiguous ownership of one content-bearing
 *   entry;
 * - declared order is preserved verbatim at all four array levels
 *   (stages, tasks, depends_on, new_tasks): the proposal is a safe copy
 *   of the agent-authored document, not a canonical manifest; semantic
 *   task/dependency normalization belongs to the single existing
 *   `preparePlanRevisionManifest`.
 *
 * Deliberately NOT validated here (other layers own these rules):
 * plan-wide unique stage/task ids, non-empty stage/plan policy, and any
 * dependency graph semantics — self, duplicate, unknown, cross-stage
 * edges and cycles are rejected later by `preparePlanRevisionManifest`;
 * whether a task reference naming an id that also appears in
 * `new_tasks` denotes a durable task (a silent rewrite attempt) or a
 * genuinely new task is rejected/accepted by the construction layer
 * against the durable task ledger; template existence is validated by
 * the compiler. A shape-valid proposal prepared by this layer is not
 * therefore a valid plan.
 *
 * Provenance: the exact deep-frozen object returned by
 * `preparePipelineV2RunPlanProposal` and by
 * `parsePipelineV2RunPlanProposal` is registered in the shared
 * module-private registry (`pipeline_v2_run_plan_provenance.ts`) under
 * the `run_plan_proposal` kind; hand-built look-alikes, casts, clones
 * and Proxies are unregistered and rejected by the future construction
 * layer with getters and Proxy traps never invoked.
 *
 * Diagnostics are content-free: they never echo raw JSON, task bodies,
 * unknown property names or values, and the malformed-JSON diagnostic
 * carries no parser message, position, token or input fragment.
 *
 * Nothing here touches the filesystem, the run root, the reducer, the
 * durable state schema, the coordinator, the runner or the CLI: this is
 * the pure snapshot substrate those layers will consume later.
 */
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  registerPreparedRunPlanObject,
} from "./pipeline_v2_run_plan_provenance.ts";

export class PipelineV2RunPlanProposalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2RunPlanProposalError";
  }
}

/** One stage of the proposal: a sequential unit referencing a stage template. */
export interface PipelineV2RunPlanProposalStage {
  readonly id: string;
  readonly template: string;
  readonly tasks: readonly PipelineV2RunPlanProposalTaskRef[];
}

/** One task reference of a stage: identity plus same-stage dependencies. */
export interface PipelineV2RunPlanProposalTaskRef {
  readonly id: string;
  readonly depends_on: readonly string[];
}

/** One new task body: the only place a not-yet-existing task body exists. */
export interface PipelineV2RunPlanProposalNewTask {
  readonly id: string;
  readonly body: string;
}

/** The full agent-authored plan proposal. */
export interface PipelineV2RunPlanProposal {
  readonly schema_version: 1;
  readonly kind: "run_plan_proposal";
  readonly stages: readonly PipelineV2RunPlanProposalStage[];
  readonly new_tasks: readonly PipelineV2RunPlanProposalNewTask[];
}

function raise(message: string): never {
  throw new PipelineV2RunPlanProposalError(message);
}

/**
 * Exact-field check with content-free diagnostics: every required field
 * must be an OWN enumerable property (inherited properties never
 * satisfy the shape), unknown own keys are rejected without naming them
 * (a canary can hide in a property name) and no input value is ever
 * echoed. Required-field names come from this module's own contract and
 * are safe to name.
 */
function expectExactObject(
  value: unknown,
  what: string,
  keys: readonly string[],
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2RunPlanProposalError(`${what} is not a JSON object`);
  }
  const obj = value as Record<string, unknown>;
  const ownKeys = Object.keys(obj);
  const expected = new Set(keys);
  let unknownKeys = false;
  for (const key of ownKeys) {
    if (!expected.has(key)) {
      unknownKeys = true;
    }
  }
  if (unknownKeys) {
    throw new PipelineV2RunPlanProposalError(`${what} has unknown fields`);
  }
  const present = new Set(ownKeys);
  for (const key of keys) {
    if (!present.has(key)) {
      throw new PipelineV2RunPlanProposalError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
  return obj;
}

function expectSafeId(value: unknown, what: string): string {
  if (!isPipelineV2SafeId(value)) {
    throw new PipelineV2RunPlanProposalError(`${what} must be a safe non-empty identifier`);
  }
  return value;
}

function expectSchemaVersion1(value: unknown, what: string): 1 {
  if (value !== 1) {
    throw new PipelineV2RunPlanProposalError(`${what}.schema_version must be 1`);
  }
  return 1;
}

function expectKind(value: unknown, what: string, kind: string): string {
  if (typeof value !== "string" || value !== kind) {
    throw new PipelineV2RunPlanProposalError(`${what}.kind must be ${JSON.stringify(kind)}`);
  }
  return value;
}

/**
 * A declared dependency list is copied verbatim after a local scalar
 * check: every entry must be a safe id. Ordering, duplicates and graph
 * semantics (self/unknown/cross-stage edges, cycles) belong to the plan
 * revision manifest layer and are not validated here.
 */
function copyDependsOn(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) {
    throw new PipelineV2RunPlanProposalError(`${what} must be an array`);
  }
  const ids: string[] = [];
  for (const entry of value) {
    if (!isPipelineV2SafeId(entry)) {
      throw new PipelineV2RunPlanProposalError(`${what} entries must be safe non-empty identifiers`);
    }
    ids.push(entry as string);
  }
  return ids;
}

/**
 * The single validation and snapshot chain for one stage task reference:
 * exact own fields, a safe id and a verbatim copied dependency list.
 */
function copyTaskRef(value: unknown, what: string): PipelineV2RunPlanProposalTaskRef {
  const obj = expectExactObject(value, what, ["id", "depends_on"]);
  return { id: expectSafeId(obj.id, `${what} id`), depends_on: copyDependsOn(obj.depends_on, `${what} depends_on`) };
}

/**
 * The single validation and snapshot chain for the run plan proposal,
 * used by both `preparePipelineV2RunPlanProposal` and
 * `parsePipelineV2RunPlanProposal` (there is no second compiler). The
 * returned proposal is an independent copy in declared order; nothing is
 * sorted.
 */
function buildRunPlanProposal(value: unknown): PipelineV2RunPlanProposal {
  const what = "the run plan proposal";
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "kind", "stages", "new_tasks"],
  );
  expectSchemaVersion1(obj.schema_version, what);
  expectKind(obj.kind, what, "run_plan_proposal");
  if (!Array.isArray(obj.stages)) {
    raise(`${what}.stages must be an array`);
  }
  if (!Array.isArray(obj.new_tasks)) {
    raise(`${what}.new_tasks must be an array`);
  }
  const stages: PipelineV2RunPlanProposalStage[] = [];
  for (let stageIndex = 0; stageIndex < obj.stages.length; stageIndex += 1) {
    const stageWhat = `${what} stage at position ${stageIndex}`;
    const stageObj = expectExactObject(obj.stages[stageIndex], stageWhat, ["id", "template", "tasks"]);
    const stageId = expectSafeId(stageObj.id, `${stageWhat} id`);
    const template = expectSafeId(stageObj.template, `${stageWhat} template`);
    if (!Array.isArray(stageObj.tasks)) {
      throw new PipelineV2RunPlanProposalError(`${stageWhat} tasks must be an array`);
    }
    const tasks: PipelineV2RunPlanProposalTaskRef[] = [];
    for (let taskIndex = 0; taskIndex < stageObj.tasks.length; taskIndex += 1) {
      tasks.push(copyTaskRef(stageObj.tasks[taskIndex], `${stageWhat} task at position ${taskIndex}`));
    }
    stages.push({ id: stageId, template, tasks });
  }
  const newTasks: PipelineV2RunPlanProposalNewTask[] = [];
  const newTaskIds = new Set<string>();
  for (let entryIndex = 0; entryIndex < obj.new_tasks.length; entryIndex += 1) {
    const entryWhat = `${what} new task at position ${entryIndex}`;
    const entryObj = expectExactObject(obj.new_tasks[entryIndex], entryWhat, ["id", "body"]);
    const entryId = expectSafeId(entryObj.id, `${entryWhat} id`);
    if (newTaskIds.has(entryId)) {
      throw new PipelineV2RunPlanProposalError(
        `${what} declares a duplicate new task id at position ${entryIndex}`,
      );
    }
    newTaskIds.add(entryId);
    if (typeof entryObj.body !== "string" || entryObj.body === "") {
      throw new PipelineV2RunPlanProposalError(`${entryWhat} body must be a non-empty string`);
    }
    newTasks.push({ id: entryId, body: entryObj.body });
  }
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages,
    new_tasks: newTasks,
  };
}

/**
 * Validates and snapshots one agent-authored run plan proposal from an
 * in-memory value and returns the direct deep-frozen proposal object —
 * no digest envelope of its own. Later mutations of the input value
 * cannot change the snapshot. A shape-valid result is not a valid plan:
 * the plan manifest and construction layers own those rules.
 */
export function preparePipelineV2RunPlanProposal(value: unknown): PipelineV2RunPlanProposal {
  const proposal = buildRunPlanProposal(value);
  const frozen = deepFreezeValue(proposal) as PipelineV2RunPlanProposal;
  registerPreparedRunPlanObject(frozen, "run_plan_proposal");
  return frozen;
}

/**
 * Parses one run plan proposal from raw JSON text and runs the same
 * validation/snapshot chain as `preparePipelineV2RunPlanProposal`. The
 * malformed-JSON diagnostic is content-free: no parser message,
 * position, token or input fragment ever reaches the error.
 */
export function parsePipelineV2RunPlanProposal(raw: string): PipelineV2RunPlanProposal {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2RunPlanProposalError("the run plan proposal document is not valid JSON");
  }
  return preparePipelineV2RunPlanProposal(parsed);
}
