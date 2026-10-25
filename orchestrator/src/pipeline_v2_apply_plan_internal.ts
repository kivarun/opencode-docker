import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId, isLowercaseSha256, isNonNegativeSafeInteger, isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import { isSafeProjectRelativePath } from "./pipeline_v2_project_provenance.ts";

/**
 * The immutable low-level apply plan: the complete, self-contained
 * description of every filesystem operation a future per-path writer will
 * execute against the registered project. The plan is a canonical JSON
 * artifact published once into the prepared operation package and never
 * rewritten; its digest — over this module's one closed digest domain plus
 * the canonical JSON — is bound into the operation journal (there is no
 * self-referencing digest field).
 *
 * The plan never carries: an absolute project path, the run root, the
 * apply-store root, operation/session ids, credentials, file contents,
 * `dev`/`ino` as portable authority, timestamps or diagnostics.
 */

export const PIPELINE_V2_APPLY_PLAN_KIND = "pipeline_v2_project_apply_plan";
export const PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION = 1;
export const PIPELINE_V2_APPLY_PLAN_DIGEST_DOMAIN = "pipeline-v2-project-apply-plan\0";
export const PIPELINE_V2_APPLY_POLICY_VERSION = 1;

/** The closed low-level step vocabulary of the v1 apply-all planner. */
export const PIPELINE_V2_APPLY_STEP_KINDS = [
  "remove_leaf",
  "remove_directory",
  "create_directory",
  "publish_regular_file",
  "publish_symlink",
  "set_mode",
] as const;
export type PipelineV2ApplyStepKind = (typeof PIPELINE_V2_APPLY_STEP_KINDS)[number];

/** One projected filesystem object state (absent is `null`). */
export type PipelineV2ApplyNode =
  | { readonly kind: "regular_file"; readonly mode: number; readonly byte_count: number; readonly sha256: string }
  | { readonly kind: "directory"; readonly mode: number }
  | { readonly kind: "symlink"; readonly target: string };

export interface PipelineV2ApplyPlanStep {
  /** Dense 1..N execution position — the journal progress cursor. */
  readonly step_index: number;
  /** The reviewed Project Changes entry this step serves (never a cursor). */
  readonly change_index: number;
  readonly path: string;
  readonly kind: PipelineV2ApplyStepKind;
  /** The exact operator state required before the step (null = absent). */
  readonly expected_before: PipelineV2ApplyNode | null;
  /** The exact state after the step (null = removed). */
  readonly desired_after: PipelineV2ApplyNode | null;
  readonly payload_index?: number;
}

export interface PipelineV2ApplyPlanPayload {
  readonly payload_index: number;
  readonly change_index: number;
  readonly byte_count: number;
  readonly sha256: string;
  readonly artifact: string;
}

export interface PipelineV2ApplyPlanNoop {
  readonly change_index: number;
  readonly path: string;
  /** The state the path must already carry (null = it must be absent). */
  readonly expected: PipelineV2ApplyNode | null;
}

export interface PipelineV2ApplyPlan {
  readonly schema_version: typeof PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION;
  readonly kind: typeof PIPELINE_V2_APPLY_PLAN_KIND;
  readonly apply_id: string;
  readonly run_id: string;
  readonly project_id: string;
  /** The opaque root-binding digest (never a path, never dev/ino). */
  readonly project_root_binding: string;
  readonly observation: string;
  readonly terminal_revision: number;
  readonly provenance: { readonly digest: string; readonly entry_count: number; readonly copy_policy_version: number };
  readonly policy_version: typeof PIPELINE_V2_APPLY_POLICY_VERSION;
  readonly steps: readonly PipelineV2ApplyPlanStep[];
  readonly payloads: readonly PipelineV2ApplyPlanPayload[];
  readonly noops: readonly PipelineV2ApplyPlanNoop[];
  readonly total_payload_byte_count: number;
}

export class PipelineV2ApplyPlanError extends Error {
  declare readonly reason: "invalid_plan" | "invalid_json";

  constructor(reason: "invalid_plan" | "invalid_json", message: string) {
    super(message);
    this.name = "PipelineV2ApplyPlanError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function ownKeys(value: Record<string, unknown>): string[] {
  return Object.getOwnPropertyNames(value).filter((name) => Object.prototype.hasOwnProperty.call(value, name));
}

function expectExactKeys(value: Record<string, unknown>, what: string, required: readonly string[], optional: readonly string[] = []): void {
  const allowed = new Set([...required, ...optional]);
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is missing the required field "${key}"`);
    }
  }
  for (const key of ownKeys(value)) {
    if (!allowed.has(key)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} carries an unknown field`);
    }
  }
}

function expectMode(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > 0o777) {
    throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must be a permission mode`);
  }
  return value;
}

export function validateApplyNode(raw: unknown, what: string): PipelineV2ApplyNode {
  if (!isPlainObject(raw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is not an object`);
  }
  const kind = raw["kind"];
  if (kind === "directory") {
    expectExactKeys(raw, what, ["kind", "mode"]);
    return deepFreezeValue({ kind: "directory", mode: expectMode(raw["mode"], `${what} mode`) });
  }
  if (kind === "symlink") {
    expectExactKeys(raw, what, ["kind", "target"]);
    const target = raw["target"];
    if (typeof target !== "string" || target.length === 0 || target.length > 4096 || target.includes("\0")) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} symlink target is not a safe target text`);
    }
    return deepFreezeValue({ kind: "symlink", target });
  }
  if (kind === "regular_file") {
    expectExactKeys(raw, what, ["kind", "mode", "byte_count", "sha256"]);
    if (!isLowercaseSha256(raw["sha256"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} sha256 is not a lowercase 64-hex digest`);
    }
    return deepFreezeValue({
      kind: "regular_file",
      mode: expectMode(raw["mode"], `${what} mode`),
      byte_count: isNonNegativeSafeInteger(raw["byte_count"])
        ? raw["byte_count"]
        : (() => { throw new PipelineV2ApplyPlanError("invalid_plan", `${what} byte_count must be a non-negative safe integer`); })(),
      sha256: raw["sha256"],
    });
  }
  throw new PipelineV2ApplyPlanError("invalid_plan", `${what} carries an unknown node kind`);
}

function expectNodeOrNull(raw: unknown, what: string): PipelineV2ApplyNode | null {
  if (raw === null) {
    return null;
  }
  return validateApplyNode(raw, what);
}

/**
 * Validates one plan document completely and returns the deep-frozen
 * snapshot. Structural rules enforced here: dense step/payload indexes,
 * one payload per publishing step, the closed step vocabulary, per-kind
 * field coherence, safe relative paths and safe ids.
 */
export function validateApplyPlan(raw: unknown): PipelineV2ApplyPlan {
  if (!isPlainObject(raw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan document is not an object");
  }
  expectExactKeys(
    raw,
    "the plan",
    [
      "schema_version",
      "kind",
      "apply_id",
      "run_id",
      "project_id",
      "project_root_binding",
      "observation",
      "terminal_revision",
      "provenance",
      "policy_version",
      "steps",
      "payloads",
      "noops",
      "total_payload_byte_count",
    ],
  );
  if (raw["schema_version"] !== PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan schema version is not supported");
  }
  if (raw["kind"] !== PIPELINE_V2_APPLY_PLAN_KIND) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan kind is not supported");
  }
  for (const field of ["apply_id", "run_id", "project_id"] as const) {
    if (!isPipelineV2SafeId(raw[field])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `the plan ${field} is not a safe id`);
    }
  }
  if (!isLowercaseSha256(raw["project_root_binding"])) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan project_root_binding is not a lowercase 64-hex digest");
  }
  if (!isLowercaseSha256(raw["observation"])) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan observation is not a lowercase 64-hex digest");
  }
  if (!isPositiveSafeInteger(raw["terminal_revision"])) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan terminal_revision must be a positive safe integer");
  }
  if (raw["policy_version"] !== PIPELINE_V2_APPLY_POLICY_VERSION) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan policy version is not supported");
  }
  const provenanceRaw = raw["provenance"];
  if (!isPlainObject(provenanceRaw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan provenance is not an object");
  }
  expectExactKeys(provenanceRaw, "the plan provenance", ["digest", "entry_count", "copy_policy_version"]);
  if (!isLowercaseSha256(provenanceRaw["digest"])) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan provenance digest is not a lowercase 64-hex digest");
  }
  const provenance = deepFreezeValue({
    digest: provenanceRaw["digest"],
    entry_count: isNonNegativeSafeInteger(provenanceRaw["entry_count"])
      ? provenanceRaw["entry_count"]
      : (() => { throw new PipelineV2ApplyPlanError("invalid_plan", "the plan provenance entry_count must be a non-negative safe integer"); })(),
    copy_policy_version: isPositiveSafeInteger(provenanceRaw["copy_policy_version"])
      ? provenanceRaw["copy_policy_version"]
      : (() => { throw new PipelineV2ApplyPlanError("invalid_plan", "the plan provenance copy_policy_version must be a positive safe integer"); })(),
  });

  const stepsRaw = raw["steps"];
  if (!Array.isArray(stepsRaw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan steps are not a list");
  }
  const steps: PipelineV2ApplyPlanStep[] = [];
  const stepByPath = new Map<string, PipelineV2ApplyStepKind[]>();
  for (let index = 0; index < stepsRaw.length; index += 1) {
    const stepRaw = stepsRaw[index];
    const what = `plan step ${index + 1}`;
    if (!isPlainObject(stepRaw)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is not an object`);
    }
    expectExactKeys(stepRaw, what, ["step_index", "change_index", "path", "kind", "expected_before", "desired_after"], ["payload_index"]);
    if (stepRaw["step_index"] !== index + 1) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} does not carry its dense step index`);
    }
    if (!isPositiveSafeInteger(stepRaw["change_index"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} change_index must be a positive safe integer`);
    }
    if (!isSafeProjectRelativePath(stepRaw["path"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} path is not a safe relative path`);
    }
    const kindValue = stepRaw["kind"];
    if (typeof kindValue !== "string" || !(PIPELINE_V2_APPLY_STEP_KINDS as readonly string[]).includes(kindValue)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} carries an unknown step kind`);
    }
    const kind = kindValue as PipelineV2ApplyStepKind;
    const expectedBefore = expectNodeOrNull(stepRaw["expected_before"], `${what} expected_before`);
    const desiredAfter = expectNodeOrNull(stepRaw["desired_after"], `${what} desired_after`);
    let payloadIndex: number | undefined;
    if (Object.prototype.hasOwnProperty.call(stepRaw, "payload_index")) {
      if (!isPositiveSafeInteger(stepRaw["payload_index"])) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} payload_index must be a positive safe integer`);
      }
      payloadIndex = stepRaw["payload_index"];
    }
    // Per-kind field coherence.
    if (kind === "remove_leaf" || kind === "remove_directory") {
      if (desiredAfter !== null) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must remove to absence`);
      }
      if (expectedBefore === null) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must name the object it removes`);
      }
      if (kind === "remove_leaf" && expectedBefore.kind === "directory") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} cannot remove a directory as a leaf`);
      }
      if (kind === "remove_directory" && expectedBefore.kind !== "directory") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} can only remove a directory`);
      }
    }
    if (kind === "create_directory") {
      if (expectedBefore !== null) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} creates into absence`);
      }
      if (desiredAfter === null || desiredAfter.kind !== "directory") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must create a directory node`);
      }
      if (payloadIndex !== undefined) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} cannot carry a payload`);
      }
    }
    if (kind === "publish_regular_file") {
      if (desiredAfter === null || desiredAfter.kind !== "regular_file") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must publish a regular file node`);
      }
      if (payloadIndex === undefined) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must carry a payload index`);
      }
      if (expectedBefore !== null && expectedBefore.kind === "directory") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} cannot replace a directory without a removal step`);
      }
    }
    if (kind === "publish_symlink") {
      if (desiredAfter === null || desiredAfter.kind !== "symlink") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must publish a symlink node`);
      }
      if (payloadIndex !== undefined) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} cannot carry a payload`);
      }
    }
    if (kind === "set_mode") {
      if (desiredAfter === null || desiredAfter.kind === "symlink") {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must set the mode of a file or directory`);
      }
      if (expectedBefore === null || expectedBefore.kind !== desiredAfter.kind) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must keep the object kind`);
      }
      if (expectedBefore.kind === "regular_file" && desiredAfter.kind === "regular_file") {
        if (expectedBefore.byte_count !== desiredAfter.byte_count || expectedBefore.sha256 !== desiredAfter.sha256) {
          throw new PipelineV2ApplyPlanError("invalid_plan", `${what} must not change file content`);
        }
      }
      if (payloadIndex !== undefined) {
        throw new PipelineV2ApplyPlanError("invalid_plan", `${what} cannot carry a payload`);
      }
    }
    const step: PipelineV2ApplyPlanStep = deepFreezeValue({
      step_index: index + 1,
      change_index: stepRaw["change_index"],
      path: stepRaw["path"],
      kind,
      expected_before: expectedBefore,
      desired_after: desiredAfter,
      ...(payloadIndex === undefined ? {} : { payload_index: payloadIndex }),
    });
    const kinds = stepByPath.get(step.path) ?? [];
    kinds.push(kind);
    stepByPath.set(step.path, kinds);
    steps.push(step);
  }
  // A path may carry several steps only for an explicit type substitution:
  // exactly one removal followed by exactly one creation, or one symlink
  // retarget publication over the old link object.
  for (const [path, kinds] of stepByPath) {
    if (kinds.length === 1) {
      continue;
    }
    if (kinds.length !== 2) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `the path ${JSON.stringify(path)} carries too many steps`);
    }
    const [first, second] = kinds as [PipelineV2ApplyStepKind, PipelineV2ApplyStepKind];
    const isRemoval = first === "remove_leaf" || first === "remove_directory";
    const isCreation =
      second === "create_directory" || second === "publish_regular_file" || second === "publish_symlink";
    if (!(isRemoval && isCreation)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `the path ${JSON.stringify(path)} carries an unsupported step sequence`);
    }
  }

  const payloadsRaw = raw["payloads"];
  if (!Array.isArray(payloadsRaw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan payloads are not a list");
  }
  const payloads: PipelineV2ApplyPlanPayload[] = [];
  for (let index = 0; index < payloadsRaw.length; index += 1) {
    const payloadRaw = payloadsRaw[index];
    const what = `plan payload ${index + 1}`;
    if (!isPlainObject(payloadRaw)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is not an object`);
    }
    expectExactKeys(payloadRaw, what, ["payload_index", "change_index", "byte_count", "sha256", "artifact"]);
    if (payloadRaw["payload_index"] !== index + 1) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} does not carry its dense payload index`);
    }
    if (!isPositiveSafeInteger(payloadRaw["change_index"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} change_index must be a positive safe integer`);
    }
    if (!isNonNegativeSafeInteger(payloadRaw["byte_count"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} byte_count must be a non-negative safe integer`);
    }
    if (!isLowercaseSha256(payloadRaw["sha256"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} sha256 is not a lowercase 64-hex digest`);
    }
    const artifact = payloadRaw["artifact"];
    if (
      typeof artifact !== "string" ||
      artifact !== `${index + 1}.payload` ||
      !isSafeProjectRelativePath(artifact)
    ) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} artifact is not the canonical payload name`);
    }
    payloads.push(deepFreezeValue({
      payload_index: index + 1,
      change_index: payloadRaw["change_index"],
      byte_count: payloadRaw["byte_count"],
      sha256: payloadRaw["sha256"],
      artifact,
    }));
  }

  const noopsRaw = raw["noops"];
  if (!Array.isArray(noopsRaw)) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan noops are not a list");
  }
  const noops: PipelineV2ApplyPlanNoop[] = [];
  for (let index = 0; index < noopsRaw.length; index += 1) {
    const noopRaw = noopsRaw[index];
    const what = `plan noop ${index + 1}`;
    if (!isPlainObject(noopRaw)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is not an object`);
    }
    expectExactKeys(noopRaw, what, ["change_index", "path", "expected"]);
    if (!Object.prototype.hasOwnProperty.call(noopRaw, "expected")) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} is missing the required field "expected"`);
    }
    if (!isPositiveSafeInteger(noopRaw["change_index"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} change_index must be a positive safe integer`);
    }
    if (!isSafeProjectRelativePath(noopRaw["path"])) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `${what} path is not a safe relative path`);
    }
    noops.push(deepFreezeValue({
      change_index: noopRaw["change_index"],
      path: noopRaw["path"],
      expected: noopRaw["expected"] === null ? null : validateApplyNode(noopRaw["expected"], `${what} expected`),
    }));
  }

  if (!isNonNegativeSafeInteger(raw["total_payload_byte_count"])) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan total_payload_byte_count must be a non-negative safe integer");
  }
  const totalPayloadByteCount = raw["total_payload_byte_count"];
  const sum = payloads.reduce((acc, payload) => acc + payload.byte_count, 0);
  if (sum !== totalPayloadByteCount) {
    throw new PipelineV2ApplyPlanError("invalid_plan", "the plan total payload byte count does not match its payload list");
  }
  const payloadIndexes = new Set(payloads.map((payload) => payload.payload_index));
  for (const step of steps) {
    if (step.payload_index !== undefined && !payloadIndexes.has(step.payload_index)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `plan step ${step.step_index} references an unknown payload`);
    }
  }
  for (const payload of payloads) {
    const bound = steps.some((step) => step.kind === "publish_regular_file" && step.payload_index === payload.payload_index);
    if (!bound) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `plan payload ${payload.payload_index} is not bound to a publishing step`);
    }
    const publisher = steps.find((step) => step.kind === "publish_regular_file" && step.payload_index === payload.payload_index)!;
    const desired = publisher.desired_after as { readonly kind: string; readonly byte_count: number; readonly sha256: string };
    if (desired.byte_count !== payload.byte_count || desired.sha256 !== payload.sha256) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `plan payload ${payload.payload_index} does not match its publishing step content contract`);
    }
  }
  const allChanges = new Set<number>();
  for (const step of steps) allChanges.add(step.change_index);
  for (const noop of noops) allChanges.add(noop.change_index);
  const maxChange = Math.max(...allChanges, 0);
  for (let change = 1; change <= maxChange; change += 1) {
    if (!allChanges.has(change)) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `the plan does not cover change index ${change}`);
    }
  }
  for (const step of steps) {
    if (step.kind !== "publish_regular_file" && step.payload_index !== undefined) {
      throw new PipelineV2ApplyPlanError("invalid_plan", `plan step ${step.step_index} carries a payload without publishing a file`);
    }
  }
  return deepFreezeValue({
    schema_version: PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION,
    kind: PIPELINE_V2_APPLY_PLAN_KIND,
    apply_id: raw["apply_id"] as string,
    run_id: raw["run_id"] as string,
    project_id: raw["project_id"] as string,
    project_root_binding: raw["project_root_binding"] as string,
    observation: raw["observation"] as string,
    terminal_revision: raw["terminal_revision"] as number,
    provenance,
    policy_version: PIPELINE_V2_APPLY_POLICY_VERSION,
    steps: deepFreezeValue(steps) as readonly PipelineV2ApplyPlanStep[],
    payloads: deepFreezeValue(payloads) as readonly PipelineV2ApplyPlanPayload[],
    noops: deepFreezeValue(noops),
    total_payload_byte_count: totalPayloadByteCount,
  });
}

/** The canonical JSON of one plan (key-sorted, arrays in contract order). */
export function applyPlanCanonicalJson(plan: PipelineV2ApplyPlan): string {
  return canonicalJson(plan as unknown as Record<string, unknown>);
}

/** The plan digest: SHA-256 over the closed domain plus the canonical JSON. */
export function applyPlanDigest(plan: PipelineV2ApplyPlan): string {
  return createHash("sha256")
    .update(PIPELINE_V2_APPLY_PLAN_DIGEST_DOMAIN)
    .update(applyPlanCanonicalJson(plan), "utf8")
    .digest("hex");
}

/** Parses one stored plan document: one JSON.parse plus the same chain. */
export function parseApplyPlan(raw: string): PipelineV2ApplyPlan {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2ApplyPlanError("invalid_json", "the stored plan is not valid JSON");
  }
  return validateApplyPlan(parsed);
}
