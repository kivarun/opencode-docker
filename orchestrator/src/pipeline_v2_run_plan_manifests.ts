/**
 * Pure run plan manifest contract for pipeline schema v2 (unwired).
 *
 * This module fixes the canonical content-free format of the three
 * manifest kinds a future user-intervention layer will exchange:
 *
 * - `plan_revision`: the full immutable snapshot of the run plan (one or
 *   more sequentially ordered stages, each carrying at least one task
 *   pointer);
 * - `task_revision`: one task's body revision (`origin` is a content-free
 *   origin class, never an authenticated identity);
 * - `continue_stage_intent` / `revise_task_intent`: content-free wait
 *   intents bound to a durable wait index and the manifests they accept.
 *
 * A plan revision is the full plan snapshot: moving to the next already
 * declared stage never requires a new revision — a revision appears only
 * when the plan itself changes. Stages are sequentially ordered and their
 * array order is semantic; task pointers inside a stage and dependency
 * lists are normalized (sorted by id by the preparer), so permuting
 * equivalent tasks or dependencies never changes the canonical form, while
 * a changed dependency edge always does. Task ids are unique across the
 * whole plan, and dependencies reference tasks of the same stage only.
 *
 * Digests: SHA-256 over a domain prefix plus the canonical JSON of the
 * normalized manifest (UTF-8). The plan domain
 * (`pipeline-v2-plan-revision\0`), the task domain
 * (`pipeline-v2-task-revision\0`) and the intent domain
 * (`pipeline-v2-wait-intent\0` — shared by both intent kinds; their
 * `kind` fields already differ in the canonical payload) are distinct from
 * each other and from every other pipeline v2 digest domain. Serialization
 * uses the one shared `canonicalJson`; there is no second serializer.
 *
 * Every manifest type is validated and normalized by exactly one chain;
 * `parse*` runs the same chain after `JSON.parse`. Diagnostics are
 * content-free: they never echo raw JSON, task bodies, unknown property
 * names (a canary can hide in a field name) or values.
 *
 * Provenance: every prepared result object is registered in the shared
 * module-private registry (`pipeline_v2_run_plan_provenance.ts`) with its
 * manifest kind; a future binding layer accepts only registered objects.
 * The registry, digest builders and freezing helpers stay module-private.
 *
 * Nothing here touches the filesystem, the run root, the reducer, the
 * durable state schema, the coordinator, the runner or the CLI: this is
 * the pure compile/parse/digest substrate those layers will consume later.
 */
import { canonicalJson } from "./canonical_json.ts";
import {
  isLowercaseSha256,
  isPipelineV2SafeId,
  isPositiveSafeInteger,
} from "./pipeline_v2_scalar.ts";
import {
  registerPreparedRunPlanObject,
  type PipelineV2RunPlanProvenanceKind,
} from "./pipeline_v2_run_plan_provenance.ts";

export class PipelineV2RunPlanManifestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2RunPlanManifestError";
  }
}

/** One task pointer of a stage: identity plus same-stage dependencies. */
export interface PipelineV2RunPlanTaskPointer {
  readonly id: string;
  readonly revision: number;
  readonly sha256: string;
  readonly depends_on: readonly string[];
}

/** One plan stage: a sequential unit referencing a pipeline stage template. */
export interface PipelineV2RunPlanStage {
  readonly id: string;
  readonly template: string;
  readonly tasks: readonly PipelineV2RunPlanTaskPointer[];
}

/** The full immutable plan snapshot carried by one plan revision manifest. */
export interface PipelineV2RunPlanRevisionManifest {
  readonly schema_version: 1;
  readonly kind: "plan_revision";
  readonly run_id: string;
  readonly revision: number;
  readonly previous_sha256: string | null;
  readonly root_task: {
    readonly input_id: string;
    readonly sha256: string;
  };
  readonly origin_execution: number;
  readonly stages: readonly PipelineV2RunPlanStage[];
}

/** One task body revision: the only place a task body exists. */
export interface PipelineV2RunTaskRevisionManifest {
  readonly schema_version: 1;
  readonly kind: "task_revision";
  readonly run_id: string;
  readonly task_id: string;
  readonly revision: number;
  readonly previous_sha256: string | null;
  readonly origin: "planning_proposal" | "user_response";
  readonly body: string;
}

/** Wait intent: continue the current stage with additional iterations. */
export interface PipelineV2ContinueStageIntentManifest {
  readonly schema_version: 1;
  readonly kind: "continue_stage_intent";
  readonly run_id: string;
  readonly wait_index: number;
  readonly stage_id: string;
  readonly expected_plan_sha256: string;
  readonly additional_iterations: number;
}

/** Wait intent: revise one task by referencing two task revision digests. */
export interface PipelineV2ReviseTaskIntentManifest {
  readonly schema_version: 1;
  readonly kind: "revise_task_intent";
  readonly run_id: string;
  readonly wait_index: number;
  readonly task_id: string;
  readonly expected_previous_task_sha256: string;
  readonly new_task_revision_sha256: string;
}

export interface PreparedPipelineV2RunPlanRevision {
  readonly manifest: PipelineV2RunPlanRevisionManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

export interface PreparedPipelineV2RunTaskRevision {
  readonly manifest: PipelineV2RunTaskRevisionManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

export interface PreparedPipelineV2RunWaitIntent {
  readonly manifest: PipelineV2ContinueStageIntentManifest | PipelineV2ReviseTaskIntentManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

const PLAN_DIGEST_DOMAIN = "pipeline-v2-plan-revision\0";
const TASK_DIGEST_DOMAIN = "pipeline-v2-task-revision\0";
const WAIT_INTENT_DIGEST_DOMAIN = "pipeline-v2-wait-intent\0";

const TASK_ORIGINS: readonly string[] = ["planning_proposal", "user_response"];

const ROOT_TASK_INPUT_ID = "task";

function raise(message: string): never {
  throw new PipelineV2RunPlanManifestError(message);
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
    throw new PipelineV2RunPlanManifestError(`${what} is not a JSON object`);
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
    throw new PipelineV2RunPlanManifestError(`${what} has unknown fields`);
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new PipelineV2RunPlanManifestError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
  return obj;
}

function expectSafeId(value: unknown, what: string): string {
  if (!isPipelineV2SafeId(value)) {
    throw new PipelineV2RunPlanManifestError(`${what} must be a safe non-empty identifier`);
  }
  return value;
}

function expectSha256(value: unknown, what: string): string {
  if (!isLowercaseSha256(value)) {
    throw new PipelineV2RunPlanManifestError(`${what} must be a lowercase hex SHA-256 digest`);
  }
  return value;
}

function expectOptionalPreviousSha256(value: unknown, what: string, revision: number): string | null {
  if (revision === 1) {
    if (value !== null) {
      throw new PipelineV2RunPlanManifestError(
        `${what} must be null for revision 1`,
      );
    }
    return null;
  }
  if (!isLowercaseSha256(value)) {
    throw new PipelineV2RunPlanManifestError(
      `${what} must be a lowercase hex SHA-256 digest for revision ${revision}`,
    );
  }
  return value;
}

function expectPositiveSafeInteger(value: unknown, what: string): number {
  if (!isPositiveSafeInteger(value)) {
    throw new PipelineV2RunPlanManifestError(`${what} must be a positive safe integer`);
  }
  return value;
}

function expectSchemaVersion1(value: unknown, what: string): 1 {
  if (value !== 1) {
    throw new PipelineV2RunPlanManifestError(`${what}.schema_version must be 1`);
  }
  return 1;
}

function expectKind(value: unknown, what: string, kind: string): string {
  if (typeof value !== "string" || value !== kind) {
    throw new PipelineV2RunPlanManifestError(`${what}.kind must be ${JSON.stringify(kind)}`);
  }
  return value;
}

/** Validates the task revision's content-free origin class. */
function expectTaskOrigin(value: unknown, what: string): "planning_proposal" | "user_response" {
  if (typeof value !== "string" || !TASK_ORIGINS.includes(value)) {
    throw new PipelineV2RunPlanManifestError(
      `${what}.origin must be one of ${JSON.stringify(TASK_ORIGINS)}`,
    );
  }
  return value as "planning_proposal" | "user_response";
}

/**
 * Normalize a dependency list: every entry must be a safe id, duplicates
 * are rejected before normalization, and the normalized list is sorted by
 * id so a permutation of equivalent dependencies never changes the
 * canonical form or the digest.
 */
function normalizeDependsOn(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) {
    throw new PipelineV2RunPlanManifestError(`${what} must be an array`);
  }
  const ids: string[] = [];
  const seen = new Set<string>();
  for (const entry of value) {
    if (!isPipelineV2SafeId(entry)) {
      throw new PipelineV2RunPlanManifestError(`${what} entries must be safe non-empty identifiers`);
    }
    const id = entry as string;
    if (seen.has(id)) {
      throw new PipelineV2RunPlanManifestError(`${what} declares a duplicate dependency`);
    }
    seen.add(id);
    ids.push(id);
  }
  return ids.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

/**
 * The single validation and normalization chain for one task pointer.
 * `depends_on` is normalized (sorted); the pointer's other fields are
 * taken verbatim after validation.
 */
function normalizeTaskPointer(
  value: unknown,
  what: string,
): PipelineV2RunPlanTaskPointer {
  const obj = expectExactObject(value, what, ["id", "revision", "sha256", "depends_on"]);
  const id = expectSafeId(obj.id, `${what} id`);
  const revision = expectPositiveSafeInteger(obj.revision, `${what} revision`);
  const sha256 = expectSha256(obj.sha256, `${what} sha256`);
  const dependsOn = normalizeDependsOn(obj.depends_on, `${what} depends_on`);
  return { id, revision, sha256, depends_on: dependsOn };
}

/**
 * The single validation and normalization chain for the plan revision
 * manifest, used by both `preparePlanRevisionManifest` and
 * `parsePlanRevisionManifest` (there is no second compiler).
 *
 * Normalization: stages stay in declaration order (the order is
 * semantic); tasks inside a stage are sorted by task id and each
 * `depends_on` list is sorted by dependency id, so permuting equivalent
 * tasks or dependencies never changes the canonical JSON or the digest.
 * Structural checks inside the plan: stage ids unique, task ids globally
 * unique, every stage at least one task, dependencies reference only
 * tasks of the same stage, no self-dependency, no duplicate dependency
 * and no dependency cycle (unknown dependencies are caught by the same
 * stage-local task-id set).
 */
function normalizePlanRevision(value: unknown): PipelineV2RunPlanRevisionManifest {
  const what = "the plan revision manifest";
  const obj = expectExactObject(
    value,
    what,
    [
      "schema_version",
      "kind",
      "run_id",
      "revision",
      "previous_sha256",
      "root_task",
      "origin_execution",
      "stages",
    ],
  );
  if (!Array.isArray(obj.stages)) {
    throw new PipelineV2RunPlanManifestError(`${what}.stages must be an array`);
  }
  if (obj.stages.length === 0) {
    throw new PipelineV2RunPlanManifestError(`${what}.stages must not be empty`);
  }
  const revision = expectPositiveSafeInteger(obj.revision, `${what} revision`);
  const previousSha256 = expectOptionalPreviousSha256(obj.previous_sha256, `${what} previous_sha256`, revision);
  const rootTaskObj = expectExactObject(obj.root_task, `${what} root_task`, ["input_id", "sha256"]);
  const rootTaskInputId = expectSafeId(rootTaskObj.input_id, `${what} root_task input_id`);
  if (rootTaskInputId !== ROOT_TASK_INPUT_ID) {
    throw new PipelineV2RunPlanManifestError(
      `${what} root_task input_id must be ${JSON.stringify(ROOT_TASK_INPUT_ID)}`,
    );
  }
  const rootTaskSha256 = expectSha256(rootTaskObj.sha256, `${what} root_task sha256`);
  const stageIds = new Set<string>();
  const taskIds = new Set<string>();
  const stages: PipelineV2RunPlanStage[] = [];
  for (let stageIndex = 0; stageIndex < obj.stages.length; stageIndex += 1) {
    const stageWhat = `${what} stage at position ${stageIndex}`;
    const stageObj = expectExactObject(
      obj.stages[stageIndex],
      stageWhat,
      ["id", "template", "tasks"],
    );
    const stageId = expectSafeId(stageObj.id, `${stageWhat} id`);
    if (stageIds.has(stageId)) {
      throw new PipelineV2RunPlanManifestError(
        `${what} declares a duplicate stage id at position ${stageIndex}`,
      );
    }
    stageIds.add(stageId);
    const template = expectSafeId(stageObj.template, `${stageWhat} template`);
    if (!Array.isArray(stageObj.tasks)) {
      throw new PipelineV2RunPlanManifestError(`${stageWhat} tasks must be an array`);
    }
    if (stageObj.tasks.length === 0) {
      throw new PipelineV2RunPlanManifestError(`${stageWhat} tasks must not be empty`);
    }
    const stageTaskIds = new Set<string>();
    const tasks: PipelineV2RunPlanTaskPointer[] = [];
    for (let taskIndex = 0; taskIndex < stageObj.tasks.length; taskIndex += 1) {
      const task = normalizeTaskPointer(stageObj.tasks[taskIndex], `${stageWhat} task at position ${taskIndex}`);
      if (taskIds.has(task.id)) {
        throw new PipelineV2RunPlanManifestError(
          `${what} declares task ${JSON.stringify(task.id)} more than once`,
        );
      }
      taskIds.add(task.id);
      stageTaskIds.add(task.id);
      tasks.push(task);
    }
    // Dependencies: same-stage only, no self-dependency, no duplicates
    // (already rejected), no unknown dependency and no cycle. The stage's
    // task set is the only allowed dependency universe; a cycle check over
    // the normalized DAG rejects every remaining structural edge error.
    for (const task of tasks) {
      for (const dependency of task.depends_on) {
        if (dependency === task.id) {
          throw new PipelineV2RunPlanManifestError(
            `${stageWhat} task ${JSON.stringify(task.id)} depends on itself`,
          );
        }
        if (!stageTaskIds.has(dependency)) {
          throw new PipelineV2RunPlanManifestError(
            `${stageWhat} task ${JSON.stringify(task.id)} depends on a task outside its stage`,
          );
        }
      }
    }
    assertNoDependencyCycle(tasks, stageWhat);
    // Normalized task order: sorted by task id; declaration order of
    // tasks is never semantic (only depends_on is).
    tasks.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    stages.push({ id: stageId, template, tasks });
  }
  return {
    schema_version: expectSchemaVersion1(obj.schema_version, what),
    kind: expectKind(obj.kind, what, "plan_revision") as "plan_revision",
    run_id: expectSafeId(obj.run_id, `${what} run_id`),
    revision,
    previous_sha256: previousSha256,
    root_task: { input_id: rootTaskInputId, sha256: rootTaskSha256 },
    origin_execution: expectPositiveSafeInteger(obj.origin_execution, `${what} origin_execution`),
    stages,
  };
}

/**
 * Rejects any dependency cycle among the normalized task pointers of one
 * stage. Pure Kahn-style check over the already validated same-stage
 * edges; diagnostics never name the cycle's tasks.
 */
function assertNoDependencyCycle(tasks: readonly PipelineV2RunPlanTaskPointer[], what: string): void {
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
    throw new PipelineV2RunPlanManifestError(`${what} tasks declare a dependency cycle`);
  }
}

/**
 * The single validation and normalization chain for the task revision
 * manifest, used by both `prepareTaskRevisionManifest` and
 * `parseTaskRevisionManifest` (there is no second compiler). The body is
 * validated by shape only and never echoed in diagnostics.
 */
function normalizeTaskRevision(value: unknown): PipelineV2RunTaskRevisionManifest {
  const what = "the task revision manifest";
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "kind", "run_id", "task_id", "revision", "previous_sha256", "origin", "body"],
  );
  const revision = expectPositiveSafeInteger(obj.revision, `${what} revision`);
  const previousSha256 = expectOptionalPreviousSha256(obj.previous_sha256, `${what} previous_sha256`, revision);
  const origin = expectTaskOrigin(obj.origin, what);
  if (revision === 1 && origin !== "planning_proposal") {
    throw new PipelineV2RunPlanManifestError(
      `${what}.origin must be planning_proposal for revision 1`,
    );
  }
  if (revision > 1 && origin !== "user_response") {
    throw new PipelineV2RunPlanManifestError(
      `${what}.origin must be user_response for revisions above 1`,
    );
  }
  if (typeof obj.body !== "string" || obj.body === "") {
    throw new PipelineV2RunPlanManifestError(`${what}.body must be a non-empty string`);
  }
  return {
    schema_version: expectSchemaVersion1(obj.schema_version, what),
    kind: expectKind(obj.kind, what, "task_revision") as "task_revision",
    run_id: expectSafeId(obj.run_id, `${what} run_id`),
    task_id: expectSafeId(obj.task_id, `${what} task_id`),
    revision,
    previous_sha256: previousSha256,
    origin: origin as PipelineV2RunTaskRevisionManifest["origin"],
    body: obj.body,
  };
}

/**
 * The single validation and normalization chain for the two wait intent
 * kinds, used by both `prepareWaitIntent` and `parseWaitIntent` (there is
 * no second compiler). The `kind` discriminator selects the exact field
 * set; an intent carries no task or plan body.
 */
function normalizeWaitIntent(value: unknown): PipelineV2ContinueStageIntentManifest | PipelineV2ReviseTaskIntentManifest {
  const what = "the wait intent manifest";
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineV2RunPlanManifestError(`${what} is not a JSON object`);
  }
  const kind = (value as Record<string, unknown>).kind;
  if (kind === "continue_stage_intent") {
    const obj = expectExactObject(
      value,
      what,
      [
        "schema_version",
        "kind",
        "run_id",
        "wait_index",
        "stage_id",
        "expected_plan_sha256",
        "additional_iterations",
      ],
    );
    return {
      schema_version: expectSchemaVersion1(obj.schema_version, what),
      kind,
      run_id: expectSafeId(obj.run_id, `${what} run_id`),
      wait_index: expectPositiveSafeInteger(obj.wait_index, `${what} wait_index`),
      stage_id: expectSafeId(obj.stage_id, `${what} stage_id`),
      expected_plan_sha256: expectSha256(obj.expected_plan_sha256, `${what} expected_plan_sha256`),
      additional_iterations: expectAdditionalIterations(obj.additional_iterations, what),
    };
  }
  if (kind === "revise_task_intent") {
    const obj = expectExactObject(
      value,
      what,
      [
        "schema_version",
        "kind",
        "run_id",
        "wait_index",
        "task_id",
        "expected_previous_task_sha256",
        "new_task_revision_sha256",
      ],
    );
    return {
      schema_version: expectSchemaVersion1(obj.schema_version, what),
      kind,
      run_id: expectSafeId(obj.run_id, `${what} run_id`),
      wait_index: expectPositiveSafeInteger(obj.wait_index, `${what} wait_index`),
      task_id: expectSafeId(obj.task_id, `${what} task_id`),
      expected_previous_task_sha256: expectSha256(
        obj.expected_previous_task_sha256,
        `${what} expected_previous_task_sha256`,
      ),
      new_task_revision_sha256: expectSha256(
        obj.new_task_revision_sha256,
        `${what} new_task_revision_sha256`,
      ),
    };
  }
  throw new PipelineV2RunPlanManifestError(
    `${what}.kind must be one of ${JSON.stringify(["continue_stage_intent", "revise_task_intent"])}`,
  );
}

/**
 * The continue intent's only pure content rule: at least one additional
 * iteration. Any maximum grant belongs to a future template/policy layer
 * and is deliberately not baked into this parser.
 */
function expectAdditionalIterations(value: unknown, what: string): number {
  if (!isPositiveSafeInteger(value)) {
    throw new PipelineV2RunPlanManifestError(
      `${what}.additional_iterations must be a positive safe integer`,
    );
  }
  return value;
}

function digestWithDomain(domain: string, canonical: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(domain);
  hasher.update(canonical);
  return hasher.digest("hex");
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreeze(entry);
    }
    Object.freeze(value);
    return value;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

/**
 * Shared freeze/registry/digest path for all three manifest kinds: the
 * canonical form is built before the manifest is frozen (freezing does
 * not change the canonical serialization of plain data), the digest is
 * domain-prefixed, and the exact prepared object is registered with its
 * manifest kind immediately before the successful return.
 */
function freezePreparedManifest<K extends PipelineV2RunPlanProvenanceKind>(
  manifest: unknown,
  kind: K,
  domain: string,
): { manifest: unknown; canonical_json: string; sha256: string } {
  const canonical = canonicalJson(manifest);
  const prepared = {
    manifest: deepFreeze(manifest),
    canonical_json: canonical,
    sha256: digestWithDomain(domain, canonical),
  };
  registerPreparedRunPlanObject(prepared, kind);
  return deepFreeze(prepared);
}

/**
 * Validates and normalizes one plan revision from an in-memory value and
 * returns an independent deep-frozen snapshot with its canonical JSON and
 * the plan digest. Later mutations of the input value cannot change the
 * snapshot.
 */
export function preparePlanRevisionManifest(value: unknown): PreparedPipelineV2RunPlanRevision {
  return freezePreparedManifest(
    normalizePlanRevision(value),
    "plan_revision",
    PLAN_DIGEST_DOMAIN,
  ) as PreparedPipelineV2RunPlanRevision;
}

/**
 * Parses one plan revision from raw JSON text and runs the same
 * validation/normalization chain as `preparePlanRevisionManifest`. The
 * malformed-JSON diagnostic is content-free: no parser message, position,
 * token or input fragment ever reaches the error.
 */
export function parsePlanRevisionManifest(raw: string): PreparedPipelineV2RunPlanRevision {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2RunPlanManifestError("the plan revision document is not valid JSON");
  }
  return preparePlanRevisionManifest(parsed);
}

/**
 * Validates and normalizes one task revision from an in-memory value and
 * returns an independent deep-frozen snapshot with its canonical JSON and
 * the task digest. The body exists only in the manifest; diagnostics
 * never echo it.
 */
export function prepareTaskRevisionManifest(value: unknown): PreparedPipelineV2RunTaskRevision {
  return freezePreparedManifest(
    normalizeTaskRevision(value),
    "task_revision",
    TASK_DIGEST_DOMAIN,
  ) as PreparedPipelineV2RunTaskRevision;
}

/**
 * Parses one task revision from raw JSON text and runs the same
 * validation/normalization chain as `prepareTaskRevisionManifest`. The
 * malformed-JSON diagnostic is content-free.
 */
export function parseTaskRevisionManifest(raw: string): PreparedPipelineV2RunTaskRevision {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2RunPlanManifestError("the task revision document is not valid JSON");
  }
  return prepareTaskRevisionManifest(parsed);
}

/**
 * Validates and normalizes one wait intent (either kind) from an
 * in-memory value and returns an independent deep-frozen snapshot with
 * its canonical JSON and the intent digest (the shared wait-intent
 * domain; the two kinds differ in their canonical payload).
 */
export function prepareWaitIntent(value: unknown): PreparedPipelineV2RunWaitIntent {
  return freezePreparedManifest(
    normalizeWaitIntent(value),
    (value as { kind?: unknown } | null | undefined)?.kind === "revise_task_intent"
      ? "revise_task_intent"
      : "continue_stage_intent",
    WAIT_INTENT_DIGEST_DOMAIN,
  ) as PreparedPipelineV2RunWaitIntent;
}

/**
 * Parses one wait intent from raw JSON text and runs the same
 * validation/normalization chain as `prepareWaitIntent`. The
 * malformed-JSON diagnostic is content-free.
 */
export function parseWaitIntent(raw: string): PreparedPipelineV2RunWaitIntent {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2RunPlanManifestError("the wait intent document is not valid JSON");
  }
  return prepareWaitIntent(parsed);
}
