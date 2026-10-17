import { describeError } from "./docker_helper.ts";

export interface SmokeArtifact {
  schema_version: number;
  status: string;
  run_id: string;
  session_token_present?: boolean;
}

export class ArtifactError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ArtifactError";
  }
}

export function parseArtifact(
  raw: string,
  expectedRunId: string,
): SmokeArtifact {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new ArtifactError(`artifact is not valid JSON: ${describeError(cause)}`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new ArtifactError("artifact is not a JSON object");
  }
  const obj = parsed as Record<string, unknown>;
  if (obj.schema_version !== 1) {
    throw new ArtifactError("artifact has schema_version != 1");
  }
  if (obj.status !== "success") {
    throw new ArtifactError(`artifact status is ${JSON.stringify(obj.status)}, expected "success"`);
  }
  if (typeof obj.run_id !== "string" || obj.run_id === "") {
    throw new ArtifactError("artifact has no run_id string");
  }
  if (obj.run_id !== expectedRunId) {
    throw new ArtifactError(
      `artifact run_id ${JSON.stringify(obj.run_id)} does not match this run (${JSON.stringify(expectedRunId)})`,
    );
  }
  return {
    schema_version: obj.schema_version,
    status: obj.status,
    run_id: obj.run_id,
    session_token_present:
      obj.session_token_present === true ? true : undefined,
  };
}

export async function verifyArtifact(
  artifactPath: string,
  expectedRunId: string,
): Promise<SmokeArtifact> {
  let raw: string;
  try {
    raw = await Bun.file(artifactPath).text();
  } catch (cause) {
    throw new ArtifactError(
      `artifact not readable at ${artifactPath}: ${describeError(cause)}`,
    );
  }
  return parseArtifact(raw, expectedRunId);
}
