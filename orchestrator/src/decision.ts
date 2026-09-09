import { isAbsolute, join } from "node:path";
import { realpath, stat } from "node:fs/promises";
import { describeError } from "./docker_helper.ts";
import {
  readBundleFile,
  requireBundleFileInsideRoot,
  validateBundleRelativePath,
} from "./bundle_file.ts";

/**
 * Pure declarative decision-table substrate (schema version 1).
 *
 * A decision model declares boolean facts, the allowed decisions, consistency
 * relations, hard constraints and ordered rules. Evaluation is deterministic:
 * validate the fact assignment, check every consistency relation, narrow the
 * allowed decision set with all active hard constraints in declaration order,
 * then select the first rule whose condition matches and whose decision is
 * still allowed. Unmatched inputs produce an `uncovered` outcome; malformed
 * inputs fail closed.
 *
 * This module is pure substrate: it holds no role, TASK, PLAN, STAGE or
 * legacy decision names, and it is not yet wired into the production
 * pipeline runner.
 */

export const DECISION_SCHEMA_VERSION = 1;

/** Static compile-time limits. Exceeding any of them fails compilation. */
export const DECISION_LIMITS = {
  maxFacts: 64,
  maxDecisions: 64,
  maxRelations: 64,
  maxConstraints: 64,
  maxRules: 64,
  maxExpressionDepth: 16,
  maxExpressionNodes: 512,
  maxTotalExpressionNodes: 4096,
} as const;

export class DecisionModelError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DecisionModelError";
  }
}

/** Same safe-identifier grammar as pipeline state/input ids. */
const DECISION_SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

/**
 * Compiled boolean expression. A fact leaf reads one declared fact and
 * compares it against a constant; composite nodes combine child expressions.
 * The evaluator supports no other node forms.
 */
export interface CompiledFactExpression {
  readonly kind: "fact";
  readonly factIndex: number;
  readonly expected: boolean;
}

export interface CompiledGroupExpression {
  readonly kind: "all" | "any";
  readonly children: readonly CompiledExpression[];
}

export interface CompiledNotExpression {
  readonly kind: "not";
  readonly child: CompiledExpression;
}

export type CompiledExpression =
  | CompiledFactExpression
  | CompiledGroupExpression
  | CompiledNotExpression;

export interface CompiledRelation {
  readonly id: string;
  readonly assert: CompiledExpression;
}

export interface CompiledConstraint {
  readonly id: string;
  readonly when: CompiledExpression;
  readonly mode: "only" | "forbid";
  readonly decisionIndices: readonly number[];
}

export interface CompiledRule {
  readonly id: string;
  readonly when: CompiledExpression;
  readonly decisionIndex: number;
}

/**
 * Engine-owned immutable evaluation snapshot. After compilation the
 * evaluator reads only this frozen structure; mutations of the parsed source
 * object cannot change evaluation results.
 */
export interface CompiledDecisionModel {
  readonly factIds: readonly string[];
  readonly decisionIds: readonly string[];
  readonly relations: readonly CompiledRelation[];
  readonly constraints: readonly CompiledConstraint[];
  readonly rules: readonly CompiledRule[];
}

export type DecisionOutcome =
  | {
      status: "selected";
      decision: string;
      rule_id: string;
      active_constraint_ids: string[];
    }
  | {
      status: "uncovered";
      active_constraint_ids: string[];
    }
  | {
      status: "inconsistent_facts";
      violated_relation_ids: string[];
    };

function expectObject(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new DecisionModelError(`${what} is not a YAML mapping`);
  }
  return value as Record<string, unknown>;
}

function expectArray(value: unknown, what: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new DecisionModelError(`${what} must be a list, not a mapping or scalar`);
  }
  return value;
}

function expectNonEmptyString(value: unknown, what: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new DecisionModelError(`${what} must be a non-empty string`);
  }
  return value;
}

function expectExactKeys(
  obj: Record<string, unknown>,
  keys: readonly string[],
  what: string,
): void {
  const expected = new Set(keys);
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      throw new DecisionModelError(`${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new DecisionModelError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
}

function validateSafeId(value: unknown, what: string): string {
  const id = expectNonEmptyString(value, what);
  if (!DECISION_SAFE_ID_PATTERN.test(id) || id.includes("..")) {
    throw new DecisionModelError(`${what} ${JSON.stringify(id)} is not a safe identifier`);
  }
  return id;
}

function expectExpressionMapping(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new DecisionModelError(
      `${what} must be an expression mapping ({fact, equals} or {all}, {any}, {not}), not a string, scalar or list`,
    );
  }
  return value as Record<string, unknown>;
}

/**
 * Compiles the raw parsed document into the immutable evaluation snapshot.
 * The evaluator never reads the raw object again, so later mutations of it
 * cannot change evaluation results.
 */
export function compileDecisionModel(raw: unknown): CompiledDecisionModel {
  const obj = expectObject(raw, "decision model");
  expectExactKeys(
    obj,
    ["schema_version", "facts", "decisions", "relations", "constraints", "rules"],
    "decision model",
  );
  if (obj.schema_version !== DECISION_SCHEMA_VERSION) {
    throw new DecisionModelError(
      `decision model schema_version must be ${DECISION_SCHEMA_VERSION}, got ${JSON.stringify(obj.schema_version)}`,
    );
  }

  const factIds: string[] = parseIds(
    expectArray(obj.facts, "decision model facts"),
    "decision model facts",
    ["id"],
    DECISION_LIMITS.maxFacts,
    "facts",
  );
  const factIndices = new Map<string, number>();
  factIds.forEach((id, index) => {
    factIndices.set(id, index);
  });

  const decisionIds: string[] = parseIds(
    expectArray(obj.decisions, "decision model decisions"),
    "decision model decisions",
    ["id"],
    DECISION_LIMITS.maxDecisions,
    "decisions",
  );
  const decisionIndices = new Map<string, number>();
  decisionIds.forEach((id, index) => {
    decisionIndices.set(id, index);
  });

  const budget: { totalNodes: number } = { totalNodes: 0 };

  const relationsRaw = expectArray(obj.relations, "decision model relations");
  if (relationsRaw.length > DECISION_LIMITS.maxRelations) {
    throw new DecisionModelError(
      `decision model declares ${relationsRaw.length} relations, more than the maximum ${DECISION_LIMITS.maxRelations}`,
    );
  }
  const relations: CompiledRelation[] = [];
  const relationIds = new Set<string>();
  for (let index = 0; index < relationsRaw.length; index++) {
    const what = `consistency relation ${index}`;
    const entry = expectObject(relationsRaw[index], what);
    expectExactKeys(entry, ["id", "assert"], what);
    const id = validateSafeId(entry.id, `${what} id`);
    if (relationIds.has(id)) {
      throw new DecisionModelError(
        `decision model declares consistency relation ${JSON.stringify(id)} more than once`,
      );
    }
    relationIds.add(id);
    const assert = compileExpression(entry.assert, `${what} assert`, factIndices, budget);
    relations.push(Object.freeze({ id, assert }));
  }

  const constraints: CompiledConstraint[] = [];
  const constraintIds = new Set<string>();
  const constraintsRaw = expectArray(obj.constraints, "decision model constraints");
  if (constraintsRaw.length > DECISION_LIMITS.maxConstraints) {
    throw new DecisionModelError(
      `decision model declares ${constraintsRaw.length} constraints, more than the maximum ${DECISION_LIMITS.maxConstraints}`,
    );
  }
  for (let index = 0; index < constraintsRaw.length; index++) {
    const what = `hard constraint ${index}`;
    const entry = expectObject(constraintsRaw[index], what);
    const keys = Object.keys(entry);
    const allowedKeys = new Set(["id", "when", "only", "forbid"]);
    for (const key of keys) {
      if (!allowedKeys.has(key)) {
        throw new DecisionModelError(`${what} has unknown field ${JSON.stringify(key)}`);
      }
    }
    if (!("id" in entry)) {
      throw new DecisionModelError(`${what} is missing required field "id"`);
    }
    if (!("when" in entry)) {
      throw new DecisionModelError(`${what} is missing required field "when"`);
    }
    const hasOnly = "only" in entry;
    const hasForbid = "forbid" in entry;
    if (hasOnly === hasForbid) {
      throw new DecisionModelError(
        `${what} must declare exactly one of "only" or "forbid"`,
      );
    }
    const id = validateSafeId(entry.id, `${what} id`);
    if (constraintIds.has(id)) {
      throw new DecisionModelError(
        `decision model declares hard constraint ${JSON.stringify(id)} more than once`,
      );
    }
    constraintIds.add(id);
    const when = compileExpression(entry.when, `${what} when`, factIndices, budget);
    const mode: "only" | "forbid" = hasOnly ? "only" : "forbid";
    const decisionIndicesForConstraint = parseDecisionReferences(
      entry[mode],
      `${what} ${mode}`,
      decisionIndices,
    );
    constraints.push(
      Object.freeze({ id, when, mode, decisionIndices: Object.freeze(decisionIndicesForConstraint) }),
    );
  }

  const rules: CompiledRule[] = [];
  const ruleIds = new Set<string>();
  const rulesRaw = expectArray(obj.rules, "decision model rules");
  if (rulesRaw.length > DECISION_LIMITS.maxRules) {
    throw new DecisionModelError(
      `decision model declares ${rulesRaw.length} rules, more than the maximum ${DECISION_LIMITS.maxRules}`,
    );
  }
  for (let index = 0; index < rulesRaw.length; index++) {
    const what = `ordered rule ${index}`;
    const entry = expectObject(rulesRaw[index], what);
    expectExactKeys(entry, ["id", "when", "decision"], what);
    const id = validateSafeId(entry.id, `${what} id`);
    if (ruleIds.has(id)) {
      throw new DecisionModelError(
        `decision model declares ordered rule ${JSON.stringify(id)} more than once`,
      );
    }
    ruleIds.add(id);
    const when = compileExpression(entry.when, `${what} when`, factIndices, budget);
    const decisionId = expectNonEmptyString(entry.decision, `${what} decision`);
    const decisionIndex = decisionIndices.get(decisionId);
    if (decisionIndex === undefined) {
      throw new DecisionModelError(
        `${what} references undeclared decision ${JSON.stringify(decisionId)}`,
      );
    }
    rules.push(Object.freeze({ id, when, decisionIndex }));
  }

  if (budget.totalNodes > DECISION_LIMITS.maxTotalExpressionNodes) {
    throw new DecisionModelError(
      `decision model exceeds the maximum total of ${DECISION_LIMITS.maxTotalExpressionNodes} expression nodes`,
    );
  }

  return deepFreeze({
    factIds: Object.freeze(factIds),
    decisionIds: Object.freeze(decisionIds),
    relations: Object.freeze(relations),
    constraints: Object.freeze(constraints),
    rules: Object.freeze(rules),
  });
}

function parseIds(
  rawList: unknown[],
  what: string,
  keys: readonly string[],
  limit: number,
  countName: string,
): string[] {
  if (rawList.length === 0) {
    throw new DecisionModelError(`${what} must not be empty`);
  }
  if (rawList.length > limit) {
    throw new DecisionModelError(
      `decision model declares ${rawList.length} ${countName}, more than the maximum ${limit}`,
    );
  }
  const ids: string[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < rawList.length; index++) {
    const entryWhat = `${what} entry ${index}`;
    const entry = expectObject(rawList[index], entryWhat);
    expectExactKeys(entry, keys, entryWhat);
    const id = validateSafeId(entry.id, `${entryWhat} id`);
    if (seen.has(id)) {
      throw new DecisionModelError(
        `decision model declares ${countName} entry ${JSON.stringify(id)} more than once`,
      );
    }
    seen.add(id);
    ids.push(id);
  }
  return ids;
}

function parseDecisionReferences(
  raw: unknown,
  what: string,
  decisionIndices: ReadonlyMap<string, number>,
): number[] {
  const list = expectArray(raw, what);
  if (list.length === 0) {
    throw new DecisionModelError(`${what} must not be empty`);
  }
  const indices: number[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < list.length; index++) {
    const decisionId = expectNonEmptyString(list[index], `${what} entry ${index}`);
    if (seen.has(decisionId)) {
      throw new DecisionModelError(`${what} declares ${JSON.stringify(decisionId)} more than once`);
    }
    seen.add(decisionId);
    const decisionIndex = decisionIndices.get(decisionId);
    if (decisionIndex === undefined) {
      throw new DecisionModelError(
        `${what} references undeclared decision ${JSON.stringify(decisionId)}`,
      );
    }
    indices.push(decisionIndex);
  }
  return indices;
}

/**
 * Boolean expression DSL node forms: `{fact, equals}`, `{all: [...]}`,
 * `{any: [...]}` and `{not: ...}`. A fact expression must have exactly the
 * fields "fact" and "equals"; group expressions exactly one field.
 */
function compileExpression(
  raw: unknown,
  what: string,
  factIndices: ReadonlyMap<string, number>,
  budget: { totalNodes: number },
  localNodes: { value: number } = { value: 0 },
  depth: number = 1,
): CompiledExpression {
  if (depth > DECISION_LIMITS.maxExpressionDepth) {
    throw new DecisionModelError(
      `${what} exceeds the maximum expression depth ${DECISION_LIMITS.maxExpressionDepth}`,
    );
  }
  if (localNodes.value + 1 > DECISION_LIMITS.maxExpressionNodes) {
    throw new DecisionModelError(
      `${what} exceeds the maximum of ${DECISION_LIMITS.maxExpressionNodes} expression nodes in one expression`,
    );
  }
  if (budget.totalNodes + 1 > DECISION_LIMITS.maxTotalExpressionNodes) {
    throw new DecisionModelError(
      `decision model exceeds the maximum total of ${DECISION_LIMITS.maxTotalExpressionNodes} expression nodes`,
    );
  }
  localNodes.value += 1;
  budget.totalNodes += 1;

  const node = expectExpressionMapping(raw, what);
  const keys = Object.keys(node);
  if (keys.includes("fact")) {
    if (keys.length !== 2 || !keys.includes("equals")) {
      throw new DecisionModelError(
        `${what} fact expression must have exactly the fields "fact" and "equals"`,
      );
    }
    const factId = expectNonEmptyString(node.fact, `${what} fact reference`);
    const factIndex = factIndices.get(factId);
    if (factIndex === undefined) {
      throw new DecisionModelError(`${what} references undeclared fact ${JSON.stringify(factId)}`);
    }
    if (typeof node.equals !== "boolean") {
      throw new DecisionModelError(
        `${what} fact expression "equals" must be a boolean, got ${JSON.stringify(node.equals)}`,
      );
    }
    return Object.freeze({ kind: "fact", factIndex, expected: node.equals });
  }
  if (keys.includes("all") || keys.includes("any")) {
    if (keys.length !== 1) {
      throw new DecisionModelError(`${what} expression must have exactly one form`);
    }
    const kind = keys.includes("all") ? "all" : "any";
    const rawChildren = node[kind];
    if (!Array.isArray(rawChildren)) {
      throw new DecisionModelError(`${what} must be a list of expressions`);
    }
    if (rawChildren.length === 0) {
      throw new DecisionModelError(`${what} must contain at least one expression`);
    }
    const children: CompiledExpression[] = [];
    for (let index = 0; index < rawChildren.length; index++) {
      children.push(
        compileExpression(
          rawChildren[index],
          `${what} child ${index}`,
          factIndices,
          budget,
          localNodes,
          depth + 1,
        ),
      );
    }
    return Object.freeze({ kind, children: Object.freeze(children) }) as CompiledGroupExpression;
  }
  if (keys.includes("not")) {
    if (keys.length !== 1) {
      throw new DecisionModelError(
        `${what} expression must have exactly the field "not"`,
      );
    }
    const child = compileExpression(
      node.not,
      `${what} operand`,
      factIndices,
      budget,
      localNodes,
      depth + 1,
    );
    return Object.freeze({ kind: "not", child });
  }
  throw new DecisionModelError(
    `${what} must be one of {fact, equals}, {all}, {any} or {not}, got fields ${JSON.stringify(keys)}`,
  );
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

/** Parse a decision-model document (YAML text) into its compiled snapshot. */
export function parseDecisionModel(raw: string): CompiledDecisionModel {
  let parsed: unknown;
  try {
    parsed = Bun.YAML.parse(raw);
  } catch (cause) {
    throw new DecisionModelError(`decision model is not valid YAML: ${describeError(cause)}`);
  }
  return compileDecisionModel(parsed);
}

/**
 * Load and compile a decision model from an absolute bundle root and a clean
 * bundle-relative path. Containment follows the shared bundle-file contract:
 * the root must be a real directory, the path must stay inside it after
 * realpath resolution, symlinks inside the bundle are allowed and symlink
 * escapes are rejected.
 */
export async function loadDecisionModel(
  bundleRoot: string,
  relativePath: string,
): Promise<CompiledDecisionModel> {
  if (!isAbsolute(bundleRoot)) {
    throw new DecisionModelError(
      `decision bundle root must be an absolute path, got ${JSON.stringify(bundleRoot)}`,
    );
  }
  let rootCanonical: string;
  try {
    rootCanonical = await realpath(bundleRoot);
  } catch (cause) {
    throw new DecisionModelError(
      `decision bundle root ${bundleRoot} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  let rootInfo;
  try {
    rootInfo = await stat(rootCanonical);
  } catch (cause) {
    throw new DecisionModelError(
      `decision bundle root ${rootCanonical} is not accessible: ${describeError(cause)}`,
    );
  }
  if (!rootInfo.isDirectory()) {
    throw new DecisionModelError(
      `decision bundle root ${rootCanonical} is not a directory`,
    );
  }
  if (typeof relativePath !== "string" || relativePath.trim() === "") {
    throw new DecisionModelError("decision model path must be a non-empty bundle-relative path");
  }
  if (!relativePath.endsWith(".yaml")) {
    throw new DecisionModelError(
      `decision model path must end with .yaml, got ${JSON.stringify(relativePath)}`,
    );
  }
  validateBundleRelativePath(relativePath, "decision model path", DecisionModelError);
  const decisionPath = await requireBundleFileInsideRoot(
    join(rootCanonical, relativePath),
    rootCanonical,
    "decision model file",
    DecisionModelError,
    "bundle root",
  );
  return parseDecisionModel(await readBundleFile(decisionPath, "decision model file", DecisionModelError));
}

function evalExpression(expression: CompiledExpression, values: readonly boolean[]): boolean {
  switch (expression.kind) {
    case "fact":
      return values[expression.factIndex] === expression.expected;
    case "all":
      return expression.children.every((child) => evalExpression(child, values));
    case "any":
      return expression.children.some((child) => evalExpression(child, values));
    case "not":
      return !evalExpression(expression.child, values);
  }
}

/**
 * Evaluate the compiled decision model against a boolean fact assignment.
 *
 * Fail-closed validation errors (non-mapping input, missing facts, extra
 * facts, non-boolean facts) throw DecisionModelError. Consistent inputs
 * produce exactly one of: a selected decision (first matching rule whose
 * decision survived the active hard constraints), an `uncovered` outcome, or
 * an `inconsistent_facts` outcome listing every violated relation in
 * declaration order.
 */
export function evaluateDecision(
  model: CompiledDecisionModel,
  facts: Readonly<Record<string, unknown>>,
): DecisionOutcome {
  if (typeof facts !== "object" || facts === null || Array.isArray(facts)) {
    throw new DecisionModelError("decision facts must be a mapping of declared fact ids");
  }
  const values: boolean[] = new Array<boolean>(model.factIds.length).fill(false);
  const given = new Set<string>();
  for (const key of Object.keys(facts)) {
    const factIndex = factIndexOf(model, key);
    if (factIndex === undefined) {
      throw new DecisionModelError(
        `decision facts include unknown fact ${JSON.stringify(key)}`,
      );
    }
    const value = facts[key];
    if (typeof value !== "boolean") {
      throw new DecisionModelError(
        `decision fact ${JSON.stringify(key)} must be a boolean, got ${typeof value}`,
      );
    }
    values[factIndex] = value;
    given.add(key);
  }
  for (const factId of model.factIds) {
    if (!given.has(factId)) {
      throw new DecisionModelError(`decision fact ${JSON.stringify(factId)} is missing`);
    }
  }

  const violated: string[] = [];
  for (const relation of model.relations) {
    if (!evalExpression(relation.assert, values)) {
      violated.push(relation.id);
    }
  }
  if (violated.length > 0) {
    return { status: "inconsistent_facts", violated_relation_ids: violated };
  }

  const allowed = new Set<number>();
  for (let index = 0; index < model.decisionIds.length; index++) {
    allowed.add(index);
  }
  const active: string[] = [];
  for (const constraint of model.constraints) {
    if (!evalExpression(constraint.when, values)) {
      continue;
    }
    active.push(constraint.id);
    if (constraint.mode === "only") {
      const keep = new Set(constraint.decisionIndices);
      for (const index of [...allowed]) {
        if (!keep.has(index)) {
          allowed.delete(index);
        }
      }
    } else {
      for (const index of constraint.decisionIndices) {
        allowed.delete(index);
      }
    }
  }

  for (const rule of model.rules) {
    if (evalExpression(rule.when, values) && allowed.has(rule.decisionIndex)) {
      return {
        status: "selected",
        decision: model.decisionIds[rule.decisionIndex] as string,
        rule_id: rule.id,
        active_constraint_ids: [...active],
      };
    }
  }
  return { status: "uncovered", active_constraint_ids: [...active] };
}

function factIndexOf(model: CompiledDecisionModel, factId: string): number | undefined {
  for (let index = 0; index < model.factIds.length; index++) {
    if (model.factIds[index] === factId) {
      return index;
    }
  }
  return undefined;
}
