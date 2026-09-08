import { stat, realpath } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { describeError } from "./docker_helper.ts";

export const AGENT_RESULT_SCHEMA_VERSION = 1;
export const AGENT_RESULT_STATUS_COMPLETED = "completed";

/**
 * The canonical standard result contract. The one-step pipeline execution
 * supports exactly this schema: a loaded pipeline `result_schema` must equal
 * this object structurally (JSON key order does not matter). This is a
 * verbatim contract comparison, not a JSON Schema engine.
 */
export const STANDARD_AGENT_RESULT_SCHEMA: Record<string, unknown> = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  title: "Agent result",
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "run_id", "status", "summary", "artifacts"],
  properties: {
    schema_version: { const: 1 },
    run_id: { type: "string", minLength: 1 },
    status: { const: "completed" },
    summary: { type: "string", minLength: 1, pattern: "\\S" },
    artifacts: { type: "array", items: { type: "string" } },
  },
};

const RESULT_FIELDS = new Set(["schema_version", "run_id", "status", "summary", "artifacts"]);

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

export interface AgentResult {
  schema_version: number;
  run_id: string;
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
 * Identity of the protected workspace input used to reject artifacts that
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
  expectedRunId: string,
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
  if (obj.run_id !== expectedRunId) {
    throw new AgentResultError(
      `result run_id ${JSON.stringify(obj.run_id)} does not match this run (${JSON.stringify(expectedRunId)})`,
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
  expectedRunId: string,
  workspaceCanonical: string,
  protectedInput?: ProtectedInputRef,
): Promise<AgentResult> {
  const result = parseAgentResult(raw, expectedRunId);
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
    if (protectedInput !== undefined) {
      if (canonical === protectedInput.canonical) {
        throw new AgentResultError(
          `artifact ${JSON.stringify(artifact)} resolves to the protected input (direct path or symlink alias)`,
        );
      }
      if (info.dev === protectedInput.dev && info.ino === protectedInput.ino) {
        throw new AgentResultError(
          `artifact ${JSON.stringify(artifact)} is a hardlink alias of the protected input`,
        );
      }
    }
  }
  return result;
}
