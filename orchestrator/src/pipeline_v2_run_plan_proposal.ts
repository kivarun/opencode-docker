/**
 * Pure run plan proposal contract for pipeline schema v2 (unwired).
 *
 * This module fixes the canonical content-free format of the
 * agent-authored planning proposal: the semantic document a planning
 * execution hands to the orchestrator to describe the next run plan. It
 * is deliberately NOT a durable manifest: the proposal carries no run id,
 * no plan or task revision numbers, no predecessor or root-task digests,
 * no origin execution, no task SHA-256 and no wait/generation/iteration
 * bindings — every durable field is derived later by the construction
 * layer from the durable state and the existing manifest preparers. The
 * agent chooses only: the sequential stage list (ids, stage templates and
 * per-stage task references with dependencies) and the bodies of tasks
 * that do not exist yet.
 *
 * Structural rules (checked here, ledger-free):
 * - stage ids and task ids are plan-unique safe identifiers; stage array
 *   order is semantic (a sequential plan), task order inside a stage is
 *   never semantic and is normalized (sorted by id), as are dependency
 *   lists (sorted by id);
 * - every stage carries at least one task reference; dependencies are
 *   same-stage only with no self, duplicate, unknown edge and no cycle
 *   (the same DAG rules as the plan revision manifest);
 * - `new_tasks` may be empty (a plan reusing only existing tasks); every
 *   entry carries a non-empty body; new-task ids are unique among
 *   `new_tasks` and every entry must be referenced by exactly one stage
 *   task reference;
 * - a task reference may name an id also present in `new_tasks`: whether
 *   that id is a durable task (a silent rewrite attempt) or a genuinely
 *   new task is a durable-ledger question this substrate cannot and does
 *   not answer — the construction layer rejects existing tasks declared
 *   in `new_tasks` against the durable task ledger.
 *
 * Digest: SHA-256 over the domain prefix
 * `pipeline-v2-run-plan-proposal\0` plus the canonical JSON of the
 * normalized proposal (UTF-8). The domain is distinct from every other
 * pipeline v2 digest domain. Serialization uses the one shared
 * `canonicalJson`; there is no second serializer.
 *
 * Every proposal is validated and normalized by exactly one chain;
 * `parsePipelineV2RunPlanProposal` performs exactly one `JSON.parse` and
 * then runs the same chain. Diagnostics are content-free: they never echo
 * raw JSON, task bodies, unknown property names (a canary can hide in a
 * field name) or values, and the malformed-JSON diagnostic carries no
 * parser message, position, token or input fragment.
 *
 * Provenance: the prepared result is registered in the shared
 * module-private registry (`pipeline_v2_run_plan_provenance.ts`) under
 * its own kind; a future construction layer accepts only registered
 * objects. The registry, digest builders and freezing helpers stay
 * module-private.
 *
 * Nothing here touches the filesystem, the run root, the reducer, the
 * durable state schema, the coordinator, the runner or the CLI: this is
 * the pure compile/parse/digest substrate those layers will consume
 * later.
 */
import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import {
  registerPreparedRunPlanObject,
  type PipelineV2RunPlanProvenanceKind,
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

/** The prepared proposal: the normalized manifest with its digest. */
export interface PreparedPipelineV2RunPlanProposal {
  readonly manifest: PipelineV2RunPlanProposal;
  readonly canonical_json: string;
  readonly sha256: string;
}

const PROPOSAL_DIGEST_DOMAIN = "pipeline-v2-run-plan-proposal\0";

function raise(message: string): never {
  throw new PipelineV2RunPlanProposalError(message);
}

/**
 * Exact-field check with content-free diagnostics: unknown keys are never
 * named (a canary can hide in a property name) and no input value is ever
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
  const expected = new Set(keys);
  let unknownKeys = false;
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      unknownKeys = true;
    }
  }
  if (unknownKeys) {
    throw new PipelineV2RunPlanProposalError(`${what} has unknown fields`);
  }
  for (const key of keys) {
    if (!(key in obj)) {
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
 * Normalize a dependency list: every entry must be a safe id, duplicates
 * are rejected before normalization, and the normalized list is sorted by
 * id so a permutation of equivalent dependencies never changes the
 * canonical form or the digest.
 */
function normalizeDependsOn(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) {
    throw new PipelineV2RunPlanProposalError(`${what} must be an array`);
  }
  const ids: string[] = [];
  const seen = new Set<string>();
  for (const entry of value) {
    if (!isPipelineV2SafeId(entry)) {
      throw new PipelineV2RunPlanProposalError(`${what} entries must be safe non-empty identifiers`);
    }
    const id = entry as string;
    if (seen.has(id)) {
      throw new PipelineV2RunPlanProposalError(`${what} declares a duplicate dependency`);
    }
    seen.add(id);
    ids.push(id);
  }
  return ids.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

/**
 * The single validation and normalization chain for one stage task
 * reference: exact fields, a safe id and a normalized dependency list.
 */
function normalizeTaskRef(value: unknown, what: string): PipelineV2RunPlanProposalTaskRef {
  const obj = expectExactObject(value, what, ["id", "depends_on"]);
  return { id: expectSafeId(obj.id, `${what} id`), depends_on: normalizeDependsOn(obj.depends_on, `${what} depends_on`) };
}

/**
 * Rejects any dependency cycle among the normalized task references of
 * one stage. Pure Kahn-style check over the already validated same-stage
 * edges; diagnostics never name the cycle's tasks.
 */
function assertNoDependencyCycle(tasks: readonly PipelineV2RunPlanProposalTaskRef[], what: string): void {
  const dependenciesByTask = new Map<string, Set<string>>();
  for (const task of tasks) {
    dependenciesByTask.set(task.id, new Set(task.depends_on));
  }
  const resolved = new Set<string>();
  let progress = true;
  while (progress && resolved.size < dependenciesByTask.size) {
    progress = false;
    for (const [taskId, dependencies] of dependenciesByTask) {
      if (resolved.has(taskId)) {
        continue;
      }
      let ready = true;
      for (const dependency of dependencies) {
        if (!resolved.has(dependency)) {
          ready = false;
          break;
        }
      }
      if (ready) {
        resolved.add(taskId);
        progress = true;
      }
    }
  }
  if (resolved.size !== dependenciesByTask.size) {
    throw new PipelineV2RunPlanProposalError(`${what} tasks declare a dependency cycle`);
  }
}

/**
 * The single validation and normalization chain for the run plan
 * proposal, used by both `preparePipelineV2RunPlanProposal` and
 * `parsePipelineV2RunPlanProposal` (there is no second compiler).
 */
function normalizeRunPlanProposal(value: unknown): PipelineV2RunPlanProposal {
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
  if (obj.stages.length === 0) {
    raise(`${what}.stages must not be empty`);
  }
  if (!Array.isArray(obj.new_tasks)) {
    raise(`${what}.new_tasks must be an array`);
  }
  const stageIds = new Set<string>();
  const taskIds = new Set<string>();
  const stages: PipelineV2RunPlanProposalStage[] = [];
  for (let stageIndex = 0; stageIndex < obj.stages.length; stageIndex += 1) {
    const stageWhat = `${what} stage at position ${stageIndex}`;
    const stageObj = expectExactObject(obj.stages[stageIndex], stageWhat, ["id", "template", "tasks"]);
    const stageId = expectSafeId(stageObj.id, `${stageWhat} id`);
    if (stageIds.has(stageId)) {
      throw new PipelineV2RunPlanProposalError(
        `${what} declares a duplicate stage id at position ${stageIndex}`,
      );
    }
    stageIds.add(stageId);
    const template = expectSafeId(stageObj.template, `${stageWhat} template`);
    if (!Array.isArray(stageObj.tasks)) {
      throw new PipelineV2RunPlanProposalError(`${stageWhat} tasks must be an array`);
    }
    if (stageObj.tasks.length === 0) {
      throw new PipelineV2RunPlanProposalError(`${stageWhat} tasks must not be empty`);
    }
    const stageTaskIds = new Set<string>();
    const tasks: PipelineV2RunPlanProposalTaskRef[] = [];
    for (let taskIndex = 0; taskIndex < stageObj.tasks.length; taskIndex += 1) {
      const task = normalizeTaskRef(stageObj.tasks[taskIndex], `${stageWhat} task at position ${taskIndex}`);
      if (taskIds.has(task.id)) {
        throw new PipelineV2RunPlanProposalError(
          `${what} declares task ${JSON.stringify(task.id)} more than once`,
        );
      }
      taskIds.add(task.id);
      stageTaskIds.add(task.id);
      tasks.push(task);
    }
    // Dependencies: same-stage only, no self-dependency, no duplicates
    // (already rejected), no unknown dependency and no cycle — the same
    // DAG rules as the plan revision manifest.
    for (const task of tasks) {
      for (const dependency of task.depends_on) {
        if (dependency === task.id) {
          throw new PipelineV2RunPlanProposalError(
            `${stageWhat} task ${JSON.stringify(task.id)} depends on itself`,
          );
        }
        if (!stageTaskIds.has(dependency)) {
          throw new PipelineV2RunPlanProposalError(
            `${stageWhat} task ${JSON.stringify(task.id)} depends on a task outside its stage`,
          );
        }
      }
    }
    assertNoDependencyCycle(tasks, stageWhat);
    // Normalized task order: sorted by task id; declaration order of
    // tasks is never semantic (only depends_on is). Stage order stays
    // verbatim: it is the sequential plan order.
    tasks.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
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
  for (const entryId of newTaskIds) {
    if (!taskIds.has(entryId)) {
      throw new PipelineV2RunPlanProposalError(
        `${what} declares new task ${JSON.stringify(entryId)} that no stage task references`,
      );
    }
  }
  // Normalized new-task order: sorted by id; declaration order of new
  // tasks is never semantic.
  newTasks.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return {
    schema_version: 1,
    kind: "run_plan_proposal",
    stages,
    new_tasks: newTasks,
  };
}

function digestWithDomain(domain: string, canonical: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(domain);
  hasher.update(canonical);
  return hasher.digest("hex");
}

function freezePreparedProposal(proposal: PipelineV2RunPlanProposal): PreparedPipelineV2RunPlanProposal {
  const canonical = canonicalJson(proposal);
  const prepared = {
    manifest: proposal,
    canonical_json: canonical,
    sha256: digestWithDomain(PROPOSAL_DIGEST_DOMAIN, canonical),
  };
  registerPreparedRunPlanObject(prepared, "run_plan_proposal");
  return deepFreezeValue(prepared) as PreparedPipelineV2RunPlanProposal;
}

/**
 * Validates and normalizes one agent-authored run plan proposal from an
 * in-memory value and returns an independent deep-frozen snapshot with
 * its canonical JSON and the proposal digest. Later mutations of the
 * input value cannot change the snapshot. No durable field (run id,
 * revisions, digests, bindings) is accepted from the proposal: the
 * construction layer derives them from the durable state.
 */
export function preparePipelineV2RunPlanProposal(value: unknown): PreparedPipelineV2RunPlanProposal {
  return freezePreparedProposal(normalizeRunPlanProposal(value));
}

/**
 * Parses one run plan proposal from raw JSON text and runs the same
 * validation/normalization chain as `preparePipelineV2RunPlanProposal`.
 * The malformed-JSON diagnostic is content-free: no parser message,
 * position, token or input fragment ever reaches the error.
 */
export function parsePipelineV2RunPlanProposal(raw: string): PreparedPipelineV2RunPlanProposal {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2RunPlanProposalError("the run plan proposal document is not valid JSON");
  }
  return preparePipelineV2RunPlanProposal(parsed);
}
