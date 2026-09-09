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

const compiledValidators = new WeakMap<Record<string, unknown>, ValidateFunction>();

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
  let validator: ValidateFunction;
  try {
    validator = freshAjv().compile(schema);
  } catch (cause) {
    throw new PipelineError(
      `${what} cannot be compiled as JSON Schema Draft 2020-12: ${describeError(cause)}`,
    );
  }
  compiledValidators.set(schema, validator);
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
  if (validator(parsed)) {
    return;
  }
  const details = (validator.errors ?? []).map(formatError).join("; ");
  throw new PipelineError(
    `${what} does not conform to its JSON schema: ${details}`,
  );
}
