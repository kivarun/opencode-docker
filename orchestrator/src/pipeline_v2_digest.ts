import { isAbsolute } from "node:path";
import type { ReadonlyJsonValue } from "./pipeline_engine.ts";
import { PipelineError } from "./pipeline.ts";
import { bundleRelativePathInsideRoot } from "./bundle_file.ts";
import { canonicalJson } from "./canonical_json.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
} from "./pipeline_v2.ts";
import type {
  CompiledDecisionModel,
  CompiledExpression,
} from "./decision.ts";
import type { PipelineV2RunPipelineIdentity } from "./pipeline_v2_state.ts";

/**
 * Deterministic execution snapshot, canonical JSON, digest and durable
 * pipeline identity for schema version 2 pipelines.
 *
 * Every function accepts only the exact deep-frozen snapshot object a
 * previous successful `loadPipelineV2` call returned (checked via the
 * existing `requireResolvedPipelineV2Provenance` gate before any field is
 * read) — hand-built objects, casts, spread/`structuredClone` clones,
 * Proxies (getters never invoked) and unregistered lookalikes are rejected
 * with a stable `PipelineError`. There is no second compiler and no second
 * validation pass behind the gate: the trusted loader owns structural
 * correctness once, and this module only maps the already-compiled
 * semantics (prompts, derived port contracts, compiled decision models,
 * ordered transitions) into an explicit normalized JSON snapshot.
 *
 * The snapshot deliberately excludes the bundle root location and every
 * other host path: bundle files (prompts, JSON schemas, decision models)
 * are represented by their bundle-relative path plus the already-loaded
 * content, so moving the same bundle to another absolute directory yields
 * the identical snapshot JSON and digest. Runtime data — run input
 * contents, binding paths, accepted outputs, sessions, credentials, env
 * values, run ids, timestamps, mutable state — is never part of the
 * snapshot; profile names are (profile content and secrets are not).
 *
 * The snapshot JSON itself is an internal execution artifact: durable
 * state and diagnostics carry only its lowercase SHA-256 digest (domain
 * separated as `pipeline-v2-execution-snapshot\0` over the canonical JSON
 * as UTF-8), never the snapshot body. This module is pure substrate and is
 * not wired into the production runner; `pipelineV2RunPipelineIdentity`
 * is the single construction point of the `create_run` pipeline identity
 * required by the schema v3 run state.
 */

/**
 * A JSON value whose containers are frozen. Objects must map every key to
 * a JSON value; arrays must contain only JSON values. Primitives are JSON
 * values as-is. The type is owned by the graph engine (`pipeline_engine.ts`)
 * and shared by this module — execution snapshots and engine views speak
 * the same JSON dialect.
 */
export type { ReadonlyJsonValue } from "./pipeline_engine.ts";

function freezeJsonValue(value: unknown): ReadonlyJsonValue {
  if (Array.isArray(value)) {
    for (const item of value) freezeJsonValue(item);
    return Object.freeze(value) as unknown as ReadonlyJsonValue;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) freezeJsonValue(record[key]);
    return Object.freeze(record) as unknown as ReadonlyJsonValue;
  }
  return value as ReadonlyJsonValue;
}

/**
 * Normalized declarative snapshot of the already compiled decision model:
 * the exact evaluation semantics the deterministic evaluator reads
 * (declaration-ordered fact and decision ids, relations, hard constraints,
 * rules in priority order, and the compiled boolean expressions with their
 * fact/decision indices). Pure data — no evaluator functions, closures or
 * caches travel with the snapshot.
 */
function snapshotDecisionModel(model: CompiledDecisionModel): Record<string, unknown> {
  return {
    fact_ids: [...model.factIds],
    decision_ids: [...model.decisionIds],
    relations: model.relations.map((relation) => ({
      id: relation.id,
      assert: snapshotDecisionExpression(relation.assert),
    })),
    constraints: model.constraints.map((constraint) => ({
      id: constraint.id,
      when: snapshotDecisionExpression(constraint.when),
      mode: constraint.mode,
      decision_indices: [...constraint.decisionIndices],
    })),
    rules: model.rules.map((rule) => ({
      id: rule.id,
      when: snapshotDecisionExpression(rule.when),
      decision_index: rule.decisionIndex,
    })),
  };
}

function snapshotDecisionExpression(expression: CompiledExpression): Record<string, unknown> {
  if (expression.kind === "fact") {
    return {
      kind: expression.kind,
      fact_index: expression.factIndex,
      expected: expression.expected,
    };
  }
  if (expression.kind === "not") {
    return { kind: expression.kind, child: snapshotDecisionExpression(expression.child) };
  }
  return {
    kind: expression.kind,
    children: expression.children.map((child) => snapshotDecisionExpression(child)),
  };
}

function snapshotRunInput(
  bundleRoot: string,
  input: ResolvedPipelineV2["inputs"][number],
): Record<string, unknown> {
  const what = `pipeline input ${JSON.stringify(input.id)} schema`;
  return {
    id: input.id,
    type: input.type,
    protected: input.protected,
    ...(input.schemaPath === undefined
      ? {}
      : {
          schema_path: bundleRelativePathInsideRoot(
            bundleRoot,
            input.schemaPath,
            PipelineError,
            what,
          ),
        }),
    ...(input.schema === undefined ? {} : { schema_value: input.schema }),
  };
}

function snapshotRunOutput(output: ResolvedPipelineV2["outputs"][number]): Record<string, unknown> {
  return {
    id: output.id,
    required: output.required,
    source: output.source,
    type: output.type,
    ...(output.schema === undefined ? {} : { schema_value: output.schema }),
  };
}

function snapshotAgentState(
  bundleRoot: string,
  state: Extract<ResolvedPipelineV2["states"][number], { type: "agent" }>,
): Record<string, unknown> {
  return {
    id: state.id,
    type: state.type,
    profile: state.profile,
    prompt: bundleRelativePathInsideRoot(
      bundleRoot,
      state.promptPath,
      PipelineError,
      `agent state ${JSON.stringify(state.id)} prompt`,
    ),
    prompt_content: state.promptContent,
    inputs: state.inputs.map((port) => ({
      id: port.id,
      source: port.source,
      type: port.type,
      ...(port.schema === undefined ? {} : { schema_value: port.schema }),
    })),
    outputs: state.outputs.map((port) => ({
      id: port.id,
      type: port.type,
      ...(port.schemaPath === undefined
        ? {}
        : {
            schema_path: bundleRelativePathInsideRoot(
              bundleRoot,
              port.schemaPath,
              PipelineError,
              `agent state ${JSON.stringify(state.id)} output port ${JSON.stringify(port.id)} schema`,
            ),
          }),
      ...(port.schema === undefined ? {} : { schema_value: port.schema }),
    })),
    timeout_seconds: state.timeout_seconds,
    max_attempts: state.max_attempts,
    transitions: state.transitions.map((transition, index) => ({
      index,
      outcome: transition.outcome,
      to: transition.to,
    })),
  };
}

function snapshotDecisionState(
  bundleRoot: string,
  state: Extract<ResolvedPipelineV2["states"][number], { type: "decision" }>,
): Record<string, unknown> {
  return {
    id: state.id,
    type: state.type,
    model: bundleRelativePathInsideRoot(
      bundleRoot,
      state.modelPath,
      PipelineError,
      `decision state ${JSON.stringify(state.id)} model`,
    ),
    decision_model: snapshotDecisionModel(state.model),
    inputs: state.inputs.map((port) => ({
      id: port.id,
      source: port.source,
      type: port.type,
      ...(port.schema === undefined ? {} : { schema_value: port.schema }),
    })),
    transitions: state.transitions.map((transition, index) => ({
      index,
      outcome: transition.outcome,
      to: transition.to,
    })),
  };
}

function snapshotState(
  bundleRoot: string,
  state: ResolvedPipelineV2["states"][number],
): Record<string, unknown> {
  if (state.type === "agent") {
    return snapshotAgentState(bundleRoot, state);
  }
  if (state.type === "decision") {
    return snapshotDecisionState(bundleRoot, state);
  }
  return { id: state.id, type: state.type, result: state.result };
}

function buildExecutionSnapshot(
  pipeline: ResolvedPipelineV2,
): Record<string, unknown> {
  if (!isAbsolute(pipeline.bundleRoot)) {
    throw new PipelineError(
      `pipeline bundle root ${pipeline.bundleRoot} must be an absolute canonical path`,
    );
  }
  return {
    schema_version: pipeline.schema_version,
    entry_state: pipeline.entry_state,
    max_transitions: pipeline.max_transitions,
    inputs: pipeline.inputs.map((input) => snapshotRunInput(pipeline.bundleRoot, input)),
    outputs: pipeline.outputs.map((output) => snapshotRunOutput(output)),
    states: pipeline.states.map((state) => snapshotState(pipeline.bundleRoot, state)),
  };
}

/**
 * Builds the explicit normalized execution snapshot of a trusted resolved
 * pipeline: schema version, entry state, transition budget, run inputs and
 * outputs in declaration order, and every state in declaration order with
 * its compiled semantics (agent prompts and port contracts, decision model
 * snapshots, ordered transitions with their original per-state indexes,
 * terminal results). The result is deep-frozen; repeated calls on the same
 * trusted snapshot are structurally identical.
 */
export function pipelineV2ExecutionSnapshot(
  pipeline: ResolvedPipelineV2,
): ReadonlyJsonValue {
  requireResolvedPipelineV2Provenance(pipeline, "pipelineV2ExecutionSnapshot");
  return freezeJsonValue(buildExecutionSnapshot(pipeline));
}

/**
 * Canonical JSON serialization of the execution snapshot: object keys are
 * sorted deterministically, declaration order of lists is preserved, and
 * identical trusted pipelines serialize byte-identically.
 */
export function pipelineV2ExecutionSnapshotJson(
  pipeline: ResolvedPipelineV2,
): string {
  requireResolvedPipelineV2Provenance(pipeline, "pipelineV2ExecutionSnapshotJson");
  return canonicalJson(pipelineV2ExecutionSnapshot(pipeline));
}

/**
 * Digest domain of the v2 execution snapshot; the prefix and the canonical
 * snapshot JSON are hashed as UTF-8 by the lowercase SHA-256 digest.
 */
const EXECUTION_SNAPSHOT_DIGEST_DOMAIN = "pipeline-v2-execution-snapshot\0";

/**
 * Lowercase SHA-256 of the domain prefix followed by the canonical
 * execution snapshot JSON. Moving the same bundle to another absolute
 * directory does not change the digest; any change to execution-relevant
 * data does.
 */
export function pipelineV2ExecutionDigest(pipeline: ResolvedPipelineV2): string {
  requireResolvedPipelineV2Provenance(pipeline, "pipelineV2ExecutionDigest");
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(EXECUTION_SNAPSHOT_DIGEST_DOMAIN);
  hasher.update(pipelineV2ExecutionSnapshotJson(pipeline));
  return hasher.digest("hex");
}

/**
 * The durable pipeline identity recorded by `create_run` in the schema v3
 * run state: the canonical bundle root (kept separately from the digest),
 * the execution snapshot digest, and the compiled entry state and
 * transition budget. This function is the single construction point of
 * that identity — the future coordinator must not duplicate the mapping.
 */
export function pipelineV2RunPipelineIdentity(
  pipeline: ResolvedPipelineV2,
): PipelineV2RunPipelineIdentity {
  requireResolvedPipelineV2Provenance(pipeline, "pipelineV2RunPipelineIdentity");
  return {
    schema_version: pipeline.schema_version,
    bundle_root: pipeline.bundleRoot,
    execution_snapshot_sha256: pipelineV2ExecutionDigest(pipeline),
    entry_state: pipeline.entry_state,
    max_transitions: pipeline.max_transitions,
  };
}
