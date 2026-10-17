import Ajv2020, { type ErrorObject, type ValidateFunction } from "ajv/dist/2020";
import { PipelineError } from "./pipeline.ts";
import { describeError } from "./docker_helper.ts";

/**
 * JSON Schema validation for pipeline v2 JSON ports, backed by the pinned
 * Ajv 2020-12 dialect compiler.
 *
 * Dialect: JSON Schema Draft 2020-12 (`ajv/dist/2020`), pinned version
 * `8.17.1` in `orchestrator/package.json` / `orchestrator/bun.lock`.
 *
 * Trust model: schemas are compiled only at trusted v2 pipeline load time —
 * `loadPipelineV2` compiles every declared JSON schema file it reads, before
 * the resolved snapshot is registered in its provenance registry. A schema
 * that cannot compile rejects the pipeline load. The compiled validator is
 * kept in a module-private `WeakMap` keyed by the exact frozen schema object;
 * validation accepts only compiled schemas, so a hand-built lookalike can
 * never smuggle an unvalidated contract in.
 *
 * Closed operation: no network access, no remote schema loading, no
 * executable callbacks (no `loadSchema`, no custom keywords). A `$ref` that
 * cannot be resolved inside the same schema — including any remote URI —
 * fails at compile time, and each compile uses a fresh Ajv instance so
 * schemas never resolve each other's `$id`s across files (compilation is
 * hermetic per schema). Unknown format keywords fail compilation as well:
 * the Draft 2020-12 core dialect does not define formats and no format
 * vocabulary is installed.
 *
 * No mutation: coercion (`coerceTypes: false`), defaults (`useDefaults:
 * false`), and additional-property removal (`removeAdditional: false`) are
 * disabled, so validation never changes the parsed JSON value it checks.
 *
 * Stable diagnostics: validation errors are rendered as
 * `<instance path>: <message>` in Ajv's fixed order; values of user data
 * (and enum candidates) never appear.
 *
 * Synchronous-only contract: pipeline v2 supports only synchronous JSON
 * Schema validators. A schema that carries `$async: true` (at the top level
 * or in any referenced subschema) either fails Ajv compilation outright
 * (`async schema in sync schema`) or compiles to a validator with `$async`
 * set; such a validator returns a `Promise` instead of a boolean, so
 * `compilePipelineJsonSchema` rejects it before it is ever registered.
 * `validatePipelineJson` re-checks `$async` defensively before calling, and
 * success is only the literal boolean `true` — a `false` is an ordinary
 * validation failure, and a `Promise`, thenable, or any other result can
 * never pass as success. Async validators are never invoked from this
 * module, so no unhandled rejection can be created here.
 */

/**
 * Strict, non-mutating Ajv options. `strict: true` (the default, made
 * explicit here) turns schema-quality problems — unknown keywords,
 * unresolved refs, unknown formats — into compile failures, keeping the
 * dialect fail-closed.
 */
function freshAjv(): Ajv2020 {
  return new Ajv2020({
    allErrors: true,
    logger: false,
    strict: true,
    // A required-only object schema (required keys without a properties
    // entry) is valid Draft 2020-12 and must compile; Ajv's strictRequired
    // typo check would reject it.
    strictRequired: false,
    coerceTypes: false,
    useDefaults: false,
    removeAdditional: false,
  });
}

const compiledValidators = new WeakMap<Record<string, unknown>, CompiledValidator>();

/**
 * A compiled Ajv validator as stored in the registry. Ajv's `ValidateFunction`
 * type does not model the `$async` marker that the compiled async function
 * carries at runtime, so it is declared here explicitly and checked before
 * every use.
 */
type CompiledValidator = ValidateFunction & { $async?: unknown };

function formatError(error: ErrorObject): string {
  const path = error.instancePath === "" ? "(root)" : error.instancePath;
  return `${path}: ${error.message ?? "failed schema validation"}`;
}

/**
 * Compile one trusted JSON schema object (Draft 2020-12) at pipeline load
 * time. Idempotent per schema object; any compile failure is a
 * `PipelineError` that rejects the `loadPipelineV2` call. The schema object
 * may be frozen — Ajv never mutates it.
 */
export function compilePipelineJsonSchema(
  schema: Record<string, unknown>,
  what: string,
): void {
  if (compiledValidators.has(schema)) {
    return;
  }
  let validator: CompiledValidator;
  try {
    validator = freshAjv().compile(schema);
  } catch (cause) {
    throw new PipelineError(
      `${what} cannot be compiled as JSON Schema Draft 2020-12: ${describeError(cause)}`,
    );
  }
  if (validator.$async === true) {
    throw new PipelineError(
      `${what} compiles to an asynchronous JSON Schema validator; ` +
        "pipeline v2 supports synchronous validators only",
    );
  }
  compiledValidators.set(schema, validator);
}

/**
 * The single decision point for whether a synchronous validator result
 * counts as success: only the literal boolean `true` is success. `false` is
 * an ordinary validation failure, and a `Promise`, thenable, or any other
 * result (an async validator slipping through) can never pass as success.
 * Exported because this exact contract is part of the sync-only guarantee
 * and is unit-tested directly.
 */
export function isSyncValidatorSuccess(result: unknown): boolean {
  return result === true;
}

/**
 * Validate an already-parsed JSON value against a schema that a successful
 * `loadPipelineV2` call compiled. The value is never modified. Failures
 * carry stable, value-free diagnostics (instance path and message only).
 * Schemas that were never compiled by the loader are rejected fail-closed.
 */
export function validatePipelineJson(
  schema: Record<string, unknown>,
  parsed: unknown,
  what: string,
): void {
  const validator = compiledValidators.get(schema);
  if (validator === undefined) {
    throw new PipelineError(
      `${what} requires the JSON schema snapshot compiled by loadPipelineV2; ` +
        "hand-built or uncompiled schemas are rejected",
    );
  }
  // Defensive sync-only gate: an async validator returning a Promise must
  // never be invoked here, so no unhandled rejection can be created.
  if (validator.$async === true) {
    throw new PipelineError(
      `${what} cannot be validated with an asynchronous JSON Schema validator; ` +
        "pipeline v2 supports synchronous validators only",
    );
  }
  if (!isSyncValidatorSuccess(validator(parsed))) {
    const details = (validator.errors ?? []).map(formatError).join("; ");
    throw new PipelineError(
      `${what} does not conform to its JSON schema: ${details}`,
    );
  }
}
