import { stat, realpath } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { describeError } from "./docker_helper.ts";

export const AGENT_RESULT_SCHEMA_VERSION = 2;
export const AGENT_RESULT_STATUS_COMPLETED = "completed";

/**
 * The canonical standard result contract of the multi-state execution. Every
 * supported agent state's `result_schema` must equal this object structurally
 * (JSON key order does not matter). This is a verbatim contract comparison,
 * not a JSON Schema engine.
 *
 * The v2 contract carries exact result identity (`state_id`,
 * `activation_index`, `attempt`) so a result of one activation can never be
 * replayed as the result of another activation of the same run.
 */
export const STANDARD_AGENT_RESULT_SCHEMA: Record<string, unknown> = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  title: "Agent result",
  type: "object",
  additionalProperties: false,
  required: [
    "schema_version",
    "run_id",
    "state_id",
    "activation_index",
    "attempt",
    "status",
    "summary",
    "artifacts",
  ],
  properties: {
    schema_version: { const: 2 },
    run_id: { type: "string", minLength: 1 },
    state_id: { type: "string", minLength: 1 },
    activation_index: { type: "integer", minimum: 1 },
    attempt: { type: "integer", minimum: 1 },
    status: { const: "completed" },
    summary: { type: "string", minLength: 1, pattern: "\\S" },
    artifacts: { type: "array", items: { type: "string" } },
  },
};

const RESULT_FIELDS = new Set([
  "schema_version",
  "run_id",
  "state_id",
  "activation_index",
  "attempt",
  "status",
  "summary",
  "artifacts",
]);

export function jsonEquals(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((item, i) => jsonEquals(item, b[i]));
  }
  if (
    typeof a === "object" &&
    typeof b === "object" &&
    a !== null &&
    b !== null &&
    !Array.isArray(a) &&
    !Array.isArray(b)
  ) {
    const aObj = a as Record<string, unknown>;
    const bObj = b as Record<string, unknown>;
    const aKeys = Object.keys(aObj);
    const bKeys = Object.keys(bObj);
    if (aKeys.length !== bKeys.length) {
      return false;
    }
    return aKeys.every((key) => key in bObj && jsonEquals(aObj[key], bObj[key]));
  }
  return false;
}

export function matchesStandardAgentResultSchema(schema: unknown): boolean {
  return jsonEquals(schema, STANDARD_AGENT_RESULT_SCHEMA);
}

/** Exact identity the result must carry: it is bound to one activation. */
export interface AgentResultIdentity {
  runId: string;
  stateId: string;
  activationIndex: number;
  attempt: number;
}

export interface AgentResult {
  schema_version: number;
  run_id: string;
  state_id: string;
  activation_index: number;
  attempt: number;
  status: string;
  summary: string;
  artifacts: string[];
}

export class AgentResultError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentResultError";
  }
}

/**
 * Identity of one protected workspace input used to reject artifacts that
 * alias it. `canonical` catches the direct path and any symlink alias
 * (compared after realpath); `dev`/`ino` additionally catch hardlink aliases.
 */
export interface ProtectedInputRef {
  canonical: string;
  dev: number;
  ino: number;
}

export function parseAgentResult(
  raw: string,
  expected: AgentResultIdentity,
): AgentResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new AgentResultError(`result is not valid JSON: ${describeError(cause)}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new AgentResultError("result is not a JSON object");
  }
  const obj = parsed as Record<string, unknown>;
  for (const key of Object.keys(obj)) {
    if (!RESULT_FIELDS.has(key)) {
      throw new AgentResultError(`result has unknown field ${JSON.stringify(key)}`);
    }
  }
  if (obj.schema_version !== AGENT_RESULT_SCHEMA_VERSION) {
    throw new AgentResultError(
      `result has schema_version ${JSON.stringify(obj.schema_version)}, expected ${AGENT_RESULT_SCHEMA_VERSION}`,
    );
  }
  if (typeof obj.run_id !== "string" || obj.run_id === "") {
    throw new AgentResultError("result has no run_id string");
  }
  if (obj.run_id !== expected.runId) {
    throw new AgentResultError(
      `result run_id ${JSON.stringify(obj.run_id)} does not match this run (${JSON.stringify(expected.runId)})`,
    );
  }
  if (typeof obj.state_id !== "string" || obj.state_id === "") {
    throw new AgentResultError("result has no state_id string");
  }
  if (obj.state_id !== expected.stateId) {
    throw new AgentResultError(
      `result state_id ${JSON.stringify(obj.state_id)} does not match this activation's state (${JSON.stringify(expected.stateId)})`,
    );
  }
  if (
    typeof obj.activation_index !== "number" ||
    !Number.isSafeInteger(obj.activation_index) ||
    obj.activation_index < 1
  ) {
    throw new AgentResultError("result activation_index must be a positive safe integer");
  }
  if (obj.activation_index !== expected.activationIndex) {
    throw new AgentResultError(
      `result activation_index ${JSON.stringify(obj.activation_index)} does not match this activation (${expected.activationIndex})`,
    );
  }
  if (
    typeof obj.attempt !== "number" ||
    !Number.isSafeInteger(obj.attempt) ||
    obj.attempt < 1
  ) {
    throw new AgentResultError("result attempt must be a positive safe integer");
  }
  if (obj.attempt !== expected.attempt) {
    throw new AgentResultError(
      `result attempt ${JSON.stringify(obj.attempt)} does not match this activation's attempt (${expected.attempt})`,
    );
  }
  if (obj.status !== AGENT_RESULT_STATUS_COMPLETED) {
    throw new AgentResultError(
      `result status is ${JSON.stringify(obj.status)}, expected "${AGENT_RESULT_STATUS_COMPLETED}"`,
    );
  }
  if (typeof obj.summary !== "string" || obj.summary.trim() === "") {
    throw new AgentResultError("result has no non-empty summary string");
  }
  if (!Array.isArray(obj.artifacts)) {
    throw new AgentResultError("result artifacts is not an array");
  }
  const artifacts: string[] = [];
  for (const entry of obj.artifacts) {
    artifacts.push(validateArtifactPath(entry));
  }
  return {
    schema_version: obj.schema_version,
    run_id: obj.run_id,
    state_id: obj.state_id,
    activation_index: obj.activation_index,
    attempt: obj.attempt,
    status: obj.status,
    summary: obj.summary,
    artifacts,
  };
}

function validateArtifactPath(entry: unknown): string {
  if (typeof entry !== "string" || entry === "") {
    throw new AgentResultError("result artifacts contains a non-string or empty entry");
  }
  if (isAbsolute(entry) || entry.startsWith("~")) {
    throw new AgentResultError(`artifact path is not workspace-relative: ${JSON.stringify(entry)}`);
  }
  const parts = entry.split("/");
  if (parts.some((part) => part === "" || part === "." || part === "..")) {
    throw new AgentResultError(
      `artifact path is not a clean workspace-relative path: ${JSON.stringify(entry)}`,
    );
  }
  return entry;
}

export async function verifyAgentResult(
  raw: string,
  expected: AgentResultIdentity,
  workspaceCanonical: string,
  protectedInputs: readonly ProtectedInputRef[] = [],
): Promise<AgentResult> {
  const result = parseAgentResult(raw, expected);
  for (const artifact of result.artifacts) {
    const absolute = join(workspaceCanonical, artifact);
    let info;
    try {
      info = await stat(absolute);
    } catch (cause) {
      throw new AgentResultError(
        `artifact ${JSON.stringify(artifact)} is not readable at ${absolute}: ${describeError(cause)}`,
      );
    }
    if (!info.isFile()) {
      throw new AgentResultError(
        `artifact ${JSON.stringify(artifact)} is not a regular file`,
      );
    }
    let canonical: string;
    try {
      canonical = await realpath(absolute);
    } catch (cause) {
      throw new AgentResultError(
        `artifact ${JSON.stringify(artifact)} cannot be canonicalized: ${describeError(cause)}`,
      );
    }
    if (canonical !== workspaceCanonical && !canonical.startsWith(`${workspaceCanonical}/`)) {
      throw new AgentResultError(
        `artifact ${JSON.stringify(artifact)} resolves outside the workspace`,
      );
    }
    for (const protectedInput of protectedInputs) {
      if (canonical === protectedInput.canonical) {
        throw new AgentResultError(
          `artifact ${JSON.stringify(artifact)} resolves to a protected input (direct path or symlink alias)`,
        );
      }
      if (info.dev === protectedInput.dev && info.ino === protectedInput.ino) {
        throw new AgentResultError(
          `artifact ${JSON.stringify(artifact)} is a hardlink alias of a protected input`,
        );
      }
    }
  }
  return result;
}
