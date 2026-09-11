/**
 * Pure wait request/response manifest contract for pipeline v2 (unwired).
 *
 * This module fixes the canonical content-free format of the user-facing
 * wait request and the user's accepted response. The request is what a
 * future policy/controller layer presents to the user while the run is
 * durably waiting; the response is what the user sends back. Both bind to
 * the durable `waits[]` journal of `pipeline_v2_state.ts` through the
 * already existing digests: the request digest becomes
 * `run_waiting.requestSha256` (the durable `request_sha256`), and the
 * response digest becomes `wait_response_recorded.responseSha256` (the
 * durable `response.response_sha256`). The manifests carry no user intent
 * payload: no TASK revision, no iteration grant, no model-profile
 * replacements, no bodies, paths, timestamps, facts or credentials —
 * validating such an intent stays with the future policy/controller layer.
 * The response routes only to the `to` target of an action declared by the
 * matched request; a target is never accepted from the response itself.
 *
 * Nothing here touches the filesystem, the run root, the coordinator, the
 * CLI or resume: this is the pure compile/parse/digest substrate those
 * layers will consume later.
 *
 * Digests: SHA-256 over a domain prefix plus the canonical JSON of the
 * normalized manifest (UTF-8). The request and response domains differ
 * from each other and from every other digest domain of the orchestrator.
 * Serialization uses the one shared `canonicalJson`; there is no second
 * canonical serializer.
 *
 * Provenance: `acceptPipelineV2WaitResponse` accepts only the exact
 * prepared request object that `preparePipelineV2WaitRequest` or
 * `parsePipelineV2WaitRequest` returned, tracked in a module-private
 * registry. Clones, spreads, `structuredClone` results, hand-built objects
 * and Proxies are rejected before any field is read (a Proxy getter is
 * never invoked).
 *
 * Diagnostics are content-free: user-side errors never echo unknown
 * property names (a canary can hide in a field name), values, raw JSON
 * fragments or parser positions.
 */
import { canonicalJson } from "./canonical_json.ts";

export class PipelineV2WaitManifestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineV2WaitManifestError";
  }
}

/** One declared wait action: a stable action id and its routing target. */
export interface PipelineV2WaitManifestAction {
  readonly id: string;
  readonly to: string;
}

/**
 * Content-free request manifest for one durable user wait. `wait_index`
 * and `transition_count` mirror the durable wait record (`index` and
 * `transition_count`); `run_id`, `state_id`, `reason` and the actions are
 * the data the durable record carries.
 */
export interface PipelineV2WaitRequestManifest {
  readonly schema_version: 1;
  readonly run_id: string;
  readonly wait_index: number;
  readonly transition_count: number;
  readonly state_id: string;
  readonly reason: string;
  readonly actions: readonly PipelineV2WaitManifestAction[];
}

/**
 * Content-free response manifest: the user's selection of one declared
 * action of the matched request, bound by run id, wait index and request
 * digest. There is deliberately no target field — routing always follows
 * the request's own action declaration.
 */
export interface PipelineV2WaitResponseManifest {
  readonly schema_version: 1;
  readonly run_id: string;
  readonly wait_index: number;
  readonly request_sha256: string;
  readonly action_id: string;
}

export interface PreparedPipelineV2WaitRequest {
  readonly manifest: PipelineV2WaitRequestManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

export interface AcceptedPipelineV2WaitResponse {
  readonly manifest: PipelineV2WaitResponseManifest;
  readonly action_to: string;
  readonly canonical_json: string;
  readonly sha256: string;
}

const WAIT_REQUEST_DIGEST_DOMAIN = "pipeline-v2-wait-request\0";
const WAIT_RESPONSE_DIGEST_DOMAIN = "pipeline-v2-wait-response\0";

/**
 * The exact safe-id grammar of the durable pipeline v2 run state
 * (`pipeline_v2_state.ts`): `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, no `..`,
 * non-empty.
 */
const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

const SHA256_PATTERN = /^[0-9a-f]{64}$/;

function isSafeId(value: unknown): value is string {
  return typeof value === "string" && value !== "" && SAFE_ID_PATTERN.test(value) && !value.includes("..");
}

function isSha256Hex(value: unknown): value is string {
  return typeof value === "string" && SHA256_PATTERN.test(value);
}

function isPositiveSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function raise(message: string): never {
  throw new PipelineV2WaitManifestError(message);
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
    throw new PipelineV2WaitManifestError(`${what} is not a JSON object`);
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
    throw new PipelineV2WaitManifestError(`${what} has unknown fields`);
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new PipelineV2WaitManifestError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
  return obj;
}

function expectSafeId(value: unknown, what: string): string {
  if (!isSafeId(value)) {
    throw new PipelineV2WaitManifestError(`${what} must be a safe non-empty identifier`);
  }
  return value;
}

function expectSha256(value: unknown, what: string): string {
  if (!isSha256Hex(value)) {
    throw new PipelineV2WaitManifestError(`${what} must be a lowercase hex SHA-256 digest`);
  }
  return value;
}

function expectPositiveSafeInteger(value: unknown, what: string): number {
  if (!isPositiveSafeInteger(value)) {
    throw new PipelineV2WaitManifestError(`${what} must be a positive safe integer`);
  }
  return value;
}

function expectNonNegativeSafeInteger(value: unknown, what: string): number {
  if (!isNonNegativeSafeInteger(value)) {
    throw new PipelineV2WaitManifestError(`${what} must be a non-negative safe integer`);
  }
  return value;
}

function expectSchemaVersion1(value: unknown, what: string): 1 {
  if (value !== 1) {
    throw new PipelineV2WaitManifestError(`${what}.schema_version must be 1`);
  }
  return 1;
}

/**
 * The single validation and normalization chain for the request manifest,
 * used by both `preparePipelineV2WaitRequest` and
 * `parsePipelineV2WaitRequest` (there is no second compiler).
 */
function normalizeWaitRequest(value: unknown): PipelineV2WaitRequestManifest {
  const what = "the wait request manifest";
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "run_id", "wait_index", "transition_count", "state_id", "reason", "actions"],
  );
  if (!Array.isArray(obj.actions)) {
    throw new PipelineV2WaitManifestError(`${what}.actions must be an array`);
  }
  if (obj.actions.length === 0) {
    throw new PipelineV2WaitManifestError(`${what}.actions must not be empty`);
  }
  const seen = new Set<string>();
  const actions = obj.actions.map((entry, index) => {
    const action = expectExactObject(
      entry,
      `${what} action at position ${index}`,
      ["id", "to"],
    );
    const id = expectSafeId(action.id, `${what} action at position ${index} id`);
    const to = expectSafeId(action.to, `${what} action at position ${index} target`);
    if (seen.has(id)) {
      throw new PipelineV2WaitManifestError(
        `${what} declares a duplicate action id at position ${index}`,
      );
    }
    seen.add(id);
    return { id, to };
  });
  return {
    schema_version: expectSchemaVersion1(obj.schema_version, what),
    run_id: expectSafeId(obj.run_id, `${what} run_id`),
    wait_index: expectPositiveSafeInteger(obj.wait_index, `${what} wait_index`),
    transition_count: expectNonNegativeSafeInteger(obj.transition_count, `${what} transition_count`),
    state_id: expectSafeId(obj.state_id, `${what} state_id`),
    reason: expectSafeId(obj.reason, `${what} reason`),
    actions,
  };
}

/**
 * The single validation and normalization chain for the response manifest,
 * used by `acceptPipelineV2WaitResponse` only.
 */
function normalizeWaitResponse(value: unknown): PipelineV2WaitResponseManifest {
  const what = "the wait response manifest";
  const obj = expectExactObject(
    value,
    what,
    ["schema_version", "run_id", "wait_index", "request_sha256", "action_id"],
  );
  return {
    schema_version: expectSchemaVersion1(obj.schema_version, what),
    run_id: expectSafeId(obj.run_id, `${what} run_id`),
    wait_index: expectPositiveSafeInteger(obj.wait_index, `${what} wait_index`),
    request_sha256: expectSha256(obj.request_sha256, `${what} request_sha256`),
    action_id: expectSafeId(obj.action_id, `${what} action_id`),
  };
}

function digestWithDomain(domain: string, canonical: string): string {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(domain);
  hasher.update(canonical);
  return hasher.digest("hex");
}

/**
 * Module-private provenance registry: only the exact frozen prepared
 * objects this module returned are registered, so a forged, cloned,
 * spread, `structuredClone`d or proxied look-alike is rejected before any
 * of its fields is read.
 */
const preparedRequests = new WeakSet<object>();

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

function freezePreparedRequest(manifest: PipelineV2WaitRequestManifest): PreparedPipelineV2WaitRequest {
  const canonical = canonicalJson(manifest);
  const prepared: PreparedPipelineV2WaitRequest = {
    manifest: deepFreeze(manifest),
    canonical_json: canonical,
    sha256: digestWithDomain(WAIT_REQUEST_DIGEST_DOMAIN, canonical),
  };
  preparedRequests.add(prepared);
  return deepFreeze(prepared);
}

/**
 * Validates and normalizes one wait request from an in-memory value and
 * returns an independent deep-frozen snapshot with its canonical JSON and
 * the request digest. Later mutations of the input value cannot change
 * the snapshot.
 */
export function preparePipelineV2WaitRequest(value: unknown): PreparedPipelineV2WaitRequest {
  return freezePreparedRequest(normalizeWaitRequest(value));
}

/**
 * Parses one wait request from raw JSON text and runs the same
 * validation/normalization chain as `preparePipelineV2WaitRequest`. The
 * malformed-JSON diagnostic is content-free: no parser message, position,
 * token or input fragment ever reaches the error.
 */
export function parsePipelineV2WaitRequest(raw: string): PreparedPipelineV2WaitRequest {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2WaitManifestError("the wait request document is not valid JSON");
  }
  return preparePipelineV2WaitRequest(parsed);
}

/**
 * Accepts the user's response against the exact prepared request. The
 * response must match the request by run id, wait index and request
 * digest, and must select an action the request declares; the returned
 * `action_to` is the request's own target for that action — a target can
 * never be passed through the response. The result is deep-frozen and
 * carries the canonical response JSON and the response digest.
 */
export function acceptPipelineV2WaitResponse(
  request: PreparedPipelineV2WaitRequest,
  raw: string,
): AcceptedPipelineV2WaitResponse {
  if (
    request === null ||
    typeof request !== "object" ||
    Array.isArray(request) ||
    !preparedRequests.has(request)
  ) {
    throw new PipelineV2WaitManifestError(
      "acceptPipelineV2WaitResponse requires the exact prepared wait request returned by preparePipelineV2WaitRequest or parsePipelineV2WaitRequest",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new PipelineV2WaitManifestError("the wait response document is not valid JSON");
  }
  const manifest = normalizeWaitResponse(parsed);
  if (manifest.run_id !== request.manifest.run_id) {
    throw new PipelineV2WaitManifestError("the wait response names another run");
  }
  if (manifest.wait_index !== request.manifest.wait_index) {
    throw new PipelineV2WaitManifestError("the wait response names another wait");
  }
  if (manifest.request_sha256 !== request.sha256) {
    throw new PipelineV2WaitManifestError("the wait response carries a different request digest");
  }
  const action = request.manifest.actions.find((candidate) => candidate.id === manifest.action_id);
  if (action === undefined) {
    throw new PipelineV2WaitManifestError(
      "the wait response names an action the request does not declare",
    );
  }
  const canonical = canonicalJson(manifest);
  const accepted: AcceptedPipelineV2WaitResponse = {
    manifest: deepFreeze(manifest),
    action_to: action.to,
    canonical_json: canonical,
    sha256: digestWithDomain(WAIT_RESPONSE_DIGEST_DOMAIN, canonical),
  };
  return deepFreeze(accepted);
}
