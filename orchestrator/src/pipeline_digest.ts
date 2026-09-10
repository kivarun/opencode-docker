import { isAbsolute } from "node:path";
import type {
  ResolvedPipeline,
  ResolvedState,
} from "./pipeline.ts";
import { canonicalJson } from "./canonical_json.ts";
import { bundleRelativePathInsideRoot } from "./bundle_file.ts";

export { canonicalJson } from "./canonical_json.ts";

/**
 * Deterministic execution-relevant snapshot of a resolved pipeline.
 *
 * The snapshot is canonical JSON: object keys are sorted by code point so the
 * key order of source objects (e.g. a `JSON.parse`d result schema) never
 * changes the digest, while array order (states, transitions, inputs,
 * artifacts) is preserved because it is execution-relevant. The bundle root
 * location is deliberately excluded — it is stored separately in the run
 * state — so moving the same bundle does not change the digest. Prompt
 * contents, result schema values, timeouts, attempt bounds, and the whole
 * graph are included: any change to them changes the digest.
 */

export interface PipelineExecutionSnapshot {
  schema_version: number;
  entry_state: string;
  max_transitions: number;
  inputs: { id: string; path: string; protected: boolean }[];
  states: Record<string, unknown>[];
}

function bundleRelativePath(bundleRoot: string, absolutePath: string): string {
  return bundleRelativePathInsideRoot(bundleRoot, absolutePath, Error, "");
}

function snapshotAgentState(
  bundleRoot: string,
  state: Extract<ResolvedState, { type: "agent" }>,
): Record<string, unknown> {
  return {
    id: state.id,
    type: state.type,
    profile: state.profile,
    prompt: bundleRelativePath(bundleRoot, state.promptPath),
    prompt_content: state.promptContent,
    inputs: [...state.inputs],
    result_schema: bundleRelativePath(bundleRoot, state.resultSchemaPath),
    result_schema_value: state.resultSchema,
    timeout_seconds: state.timeout_seconds,
    max_attempts: state.max_attempts,
    transitions: state.transitions.map((transition) => ({
      outcome: transition.outcome,
      to: transition.to,
    })),
  };
}

function snapshotTerminalState(
  state: Extract<ResolvedState, { type: "terminal" }>,
): Record<string, unknown> {
  return {
    id: state.id,
    type: state.type,
    result: state.result,
  };
}

export function pipelineExecutionSnapshot(
  pipeline: ResolvedPipeline,
): PipelineExecutionSnapshot {
  if (!isAbsolute(pipeline.bundleRoot)) {
    throw new Error(
      `pipeline bundle root ${pipeline.bundleRoot} must be an absolute canonical path`,
    );
  }
  return {
    schema_version: pipeline.schema_version,
    entry_state: pipeline.entry_state,
    max_transitions: pipeline.max_transitions,
    inputs: pipeline.inputs.map((input) => ({
      id: input.id,
      path: input.path,
      protected: input.protected,
    })),
    states: pipeline.states.map((state) =>
      state.type === "agent"
        ? snapshotAgentState(pipeline.bundleRoot, state)
        : snapshotTerminalState(state),
    ),
  };
}

export function pipelineExecutionSnapshotJson(pipeline: ResolvedPipeline): string {
  return canonicalJson(pipelineExecutionSnapshot(pipeline));
}

export function pipelineExecutionDigest(pipeline: ResolvedPipeline): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(pipelineExecutionSnapshotJson(pipeline));
  return hasher.digest("hex");
}
