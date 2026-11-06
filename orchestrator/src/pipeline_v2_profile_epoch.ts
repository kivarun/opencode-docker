/**
 * Durable run-wide profile epoch (pure substrate, state schema v12).
 *
 * Every pipeline v2 run binds one immutable, content-free epoch manifest
 * describing the effective execution profiles the run can use — the
 * complete set of profile identities referenced by the agent states of the
 * trusted compiled pipeline. The manifest is computed from exactly the
 * loaded `ResolvedProfile` objects the runtime factory later snapshots, so
 * the epoch verification and every Session of the invocation share one
 * profile load. Between separate mutating invocations the manifest is
 * recomputed from the freshly loaded profiles and compared against the
 * durable binding: any drift in the worker image reference, the OpenCode
 * config bytes, the env binding declarations or the resolved non-secret
 * env values is a typed refusal before any Session, worker or durable
 * write. Secret env values never enter the manifest — not their values,
 * not their digests, not their presence or length.
 *
 * The manifest is path-free: no config root, no absolute host path and no
 * declared relative config path is serialized. Identity is the effective
 * bytes, so relocating the config root or renaming a config file with
 * byte-identical content leaves the epoch unchanged, while any byte
 * change moves it.
 *
 * Secret boundary (profile schema v2): the OpenCode config is a non-secret
 * artifact by the mandatory trusted-operator assertion
 * `opencode_config.secrets: env_only`; literal credentials inside it are
 * forbidden by contract and are not semantically detected. A false
 * assertion is explicitly outside the supported security contract. All
 * secrets must enter through profile env bindings declared `secret: true`.
 *
 * Digest domains (each closed, each owned once by this module; the value
 * and config digests carry an 8-byte big-endian length frame):
 *
 * - `pipeline-v2-profile-config\0`    — OpenCode config runtime content;
 * - `pipeline-v2-profile-env-value\0` — one resolved non-secret env value;
 * - `pipeline-v2-profile-epoch\0`     — the manifest's canonical JSON.
 *
 * Nothing in this module performs filesystem, store, sink, reducer,
 * coordinator, runner or CLI work; it is the single parser and digest
 * owner of the three domains.
 */

import { createHash } from "node:crypto";
import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  isLowercaseSha256,
  isNonNegativeSafeInteger,
  isPipelineV2SafeId,
} from "./pipeline_v2_scalar.ts";

export const PROFILE_EPOCH_SCHEMA_VERSION = 1;
export const PROFILE_EPOCH_KIND = "pipeline_v2_profile_epoch" as const;

/** The only supported OpenCode config secret policy of profile schema v2. */
export const PROFILE_CONFIG_POLICY_ENV_ONLY = "env_only" as const;

const CONFIG_DIGEST_DOMAIN = "pipeline-v2-profile-config\0";
const ENV_VALUE_DIGEST_DOMAIN = "pipeline-v2-profile-env-value\0";
const MANIFEST_DIGEST_DOMAIN = "pipeline-v2-profile-epoch\0";

/**
 * The image-reference grammar of the profile epoch identity: a digest-
 * pinned reference `<repository>[:<tag>]@sha256:<64 lowercase hex>`. The
 * repository part is a conservative docker-style path (optional registry
 * host with optional port, path components, optional tag); the reference
 * string is preserved verbatim as identity, never rewritten. Tag-only
 * references, bare image ids, whitespace or control characters and
 * malformed digests are rejected.
 */
const DIGEST_PINNED_IMAGE_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9._-]*(?::[0-9]+)?(?:\/[A-Za-z0-9][A-Za-z0-9._-]*)*(?::[A-Za-z0-9][A-Za-z0-9._-]*)?@sha256:[0-9a-f]{64}$/;

/**
 * Validates one digest-pinned image reference. The string is the epoch
 * identity of the worker image content; a tag can never serve as content
 * identity.
 */
export function isDigestPinnedImageRef(value: unknown): value is string {
  return typeof value === "string" && DIGEST_PINNED_IMAGE_PATTERN.test(value);
}

/** Validates one environment variable name (shared profile/manifest grammar). */
export function isValidEnvVarName(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 256 &&
    /^[A-Za-z_][A-Za-z0-9_]*$/.test(value)
  );
}

/** The canonical image-reference validation with a stable diagnostic. */
export function expectDigestPinnedImageRef(value: unknown, what: string): string {
  if (!isDigestPinnedImageRef(value)) {
    throw new PipelineV2ProfileEpochError(
      "invalid_manifest",
      `${what} must be a digest-pinned image reference of the form <repository>[:<tag>]@sha256:<64 lowercase hex>`,
    );
  }
  return value;
}

/**
 * SHA-256 over the closed config domain with an 8-byte big-endian length
 * frame over the exact UTF-8 runtime content the worker receives as
 * `OPENCODE_CONFIG_CONTENT`.
 */
export function profileEpochConfigDigest(content: string): string {
  return framedDigest(CONFIG_DIGEST_DOMAIN, Buffer.from(content, "utf8"));
}

/**
 * SHA-256 over the closed env-value domain with an 8-byte big-endian
 * length frame over one resolved non-secret env value. Secret values are
 * never passed to this function — they contribute nothing to the epoch.
 */
export function profileEpochEnvValueDigest(value: string): string {
  return framedDigest(ENV_VALUE_DIGEST_DOMAIN, Buffer.from(value, "utf8"));
}

function framedDigest(domain: string, payload: Buffer): string {
  const length = Buffer.alloc(8);
  length.writeBigUInt64BE(BigInt(payload.byteLength));
  return createHash("sha256")
    .update(Buffer.from(domain, "utf8"))
    .update(length)
    .update(payload)
    .digest("hex");
}

/** One env binding entry of the manifest (declaration; value digest for non-secret resolved values only). */
export interface ProfileEpochEnvBindingEntry {
  readonly destination: string;
  readonly from_env: string;
  readonly required: boolean;
  readonly secret: boolean;
  /** Present exactly for a non-secret binding whose value resolved at load. */
  readonly value_sha256?: string;
}

/** One profile entry of the manifest. */
export interface ProfileEpochProfileEntry {
  readonly id: string;
  readonly image_ref: string;
  readonly config_policy: typeof PROFILE_CONFIG_POLICY_ENV_ONLY;
  readonly config_sha256: string;
  readonly env_bindings: readonly ProfileEpochEnvBindingEntry[];
}

/** The immutable manifest document. */
export interface PipelineV2ProfileEpochManifest {
  readonly schema_version: typeof PROFILE_EPOCH_SCHEMA_VERSION;
  readonly kind: typeof PROFILE_EPOCH_KIND;
  readonly profiles: readonly ProfileEpochProfileEntry[];
}

/** The prepared, deep-frozen manifest snapshot with its canonical bytes. */
export interface PreparedPipelineV2ProfileEpoch {
  readonly manifest: PipelineV2ProfileEpochManifest;
  readonly canonical_json: string;
  readonly sha256: string;
}

/** The minimal immutable state binding of one run's profile epoch. */
export interface PipelineV2ProfileEpochBinding {
  readonly digest: string;
  readonly profile_count: number;
}

/** Typed failure of the pure profile-epoch substrate. */
export type PipelineV2ProfileEpochFailureReason = "invalid_manifest" | "invalid_entry";

export class PipelineV2ProfileEpochError extends Error {
  declare readonly reason: PipelineV2ProfileEpochFailureReason;

  constructor(reason: PipelineV2ProfileEpochFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ProfileEpochError";
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

function expectExactKeysWithOptional(
  value: Record<string, unknown>,
  what: string,
  keys: readonly string[],
  optional: readonly string[],
): void {
  const expected = new Set([...keys, ...optional]);
  for (const key of Object.keys(value)) {
    if (!expected.has(key)) {
      throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in value)) {
      throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
}

function expectExactKeys(
  value: Record<string, unknown>,
  what: string,
  keys: readonly string[],
): void {
  const expected = new Set(keys);
  for (const key of Object.keys(value)) {
    if (!expected.has(key)) {
      throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in value)) {
      throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
}

function expectSha256(value: unknown, what: string): string {
  if (!isLowercaseSha256(value)) {
    throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} must be a lowercase 64-hex sha256 digest`);
  }
  return value;
}

/** Validates one env binding entry of the manifest. */
function validateEnvBindingEntry(value: unknown, what: string): ProfileEpochEnvBindingEntry {
  if (!isPlainObject(value)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} must be an object`);
  }
  expectExactKeysWithOptional(value, what, ["destination", "from_env", "required", "secret"], ["value_sha256"]);
  const destination = value["destination"];
  if (!isValidEnvVarName(destination)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} destination is not a valid environment variable name`);
  }
  const fromEnv = value["from_env"];
  if (!isValidEnvVarName(fromEnv)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} from_env is not a valid environment variable name`);
  }
  if (typeof value["required"] !== "boolean") {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} required must be a boolean`);
  }
  const secret = value["secret"];
  if (typeof secret !== "boolean") {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} secret must be a boolean`);
  }
  if (secret && value["value_sha256"] !== undefined) {
    // A secret binding never carries a value digest, presence or length.
    throw new PipelineV2ProfileEpochError(
      "invalid_entry",
      `${what} has unknown field ${JSON.stringify("value_sha256")}`,
    );
  }
  const entry: ProfileEpochEnvBindingEntry = {
    destination,
    from_env: fromEnv,
    required: value["required"],
    secret,
  };
  if (secret) {
    return deepFreezeValue(entry);
  }
  if (value["value_sha256"] !== undefined) {
    const withValue: ProfileEpochEnvBindingEntry = {
      ...entry,
      value_sha256: expectSha256(value["value_sha256"], `${what} value_sha256`),
    };
    return deepFreezeValue(withValue);
  }
  return deepFreezeValue(entry);
}

/** Validates one profile entry of the manifest. */
function validateProfileEntry(value: unknown, what: string): ProfileEpochProfileEntry {
  if (!isPlainObject(value)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} must be an object`);
  }
  expectExactKeys(value, what, ["id", "image_ref", "config_policy", "config_sha256", "env_bindings"]);
  const id = value["id"];
  if (!isPipelineV2SafeId(id)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} id is not a safe identifier`);
  }
  const imageRef = expectDigestPinnedImageRef(value["image_ref"], `${what} image_ref`);
  if (value["config_policy"] !== PROFILE_CONFIG_POLICY_ENV_ONLY) {
    throw new PipelineV2ProfileEpochError(
      "invalid_entry",
      `${what} config_policy ${JSON.stringify(value["config_policy"])} is not the supported config policy`,
    );
  }
  const configSha256 = expectSha256(value["config_sha256"], `${what} config_sha256`);
  const rawBindings = value["env_bindings"];
  if (!Array.isArray(rawBindings)) {
    throw new PipelineV2ProfileEpochError("invalid_entry", `${what} env_bindings must be an array`);
  }
  const envBindings: ProfileEpochEnvBindingEntry[] = [];
  let previousDestination: string | null = null;
  for (let index = 0; index < rawBindings.length; index += 1) {
    const binding = validateEnvBindingEntry(rawBindings[index], `${what} env_bindings[${index}]`);
    if (previousDestination !== null && binding.destination <= previousDestination) {
      throw new PipelineV2ProfileEpochError(
        "invalid_entry",
        `${what} env_bindings must be sorted by destination without duplicates`,
      );
    }
    previousDestination = binding.destination;
    envBindings.push(binding);
  }
  return deepFreezeValue({
    id,
    image_ref: imageRef,
    config_policy: PROFILE_CONFIG_POLICY_ENV_ONLY,
    config_sha256: configSha256,
    env_bindings: envBindings,
  });
}

/** Validates the manifest document shape (shared by prepare and parse). */
export function validateProfileEpochManifest(value: unknown): PipelineV2ProfileEpochManifest {
  if (!isPlainObject(value)) {
    throw new PipelineV2ProfileEpochError("invalid_manifest", "the profile epoch manifest must be an object");
  }
  expectExactKeys(value, "the profile epoch manifest", ["schema_version", "kind", "profiles"]);
  if (value["schema_version"] !== PROFILE_EPOCH_SCHEMA_VERSION) {
    throw new PipelineV2ProfileEpochError(
      "invalid_manifest",
      `the profile epoch manifest has schema_version ${JSON.stringify(value["schema_version"])}, expected ${PROFILE_EPOCH_SCHEMA_VERSION}`,
    );
  }
  if (value["kind"] !== PROFILE_EPOCH_KIND) {
    throw new PipelineV2ProfileEpochError(
      "invalid_manifest",
      `the profile epoch manifest has kind ${JSON.stringify(value["kind"])}, expected ${JSON.stringify(PROFILE_EPOCH_KIND)}`,
    );
  }
  const rawProfiles = value["profiles"];
  if (!Array.isArray(rawProfiles)) {
    throw new PipelineV2ProfileEpochError("invalid_manifest", "the profile epoch manifest profiles must be an array");
  }
  const profiles: ProfileEpochProfileEntry[] = [];
  const ids = new Set<string>();
  for (let index = 0; index < rawProfiles.length; index += 1) {
    const entry = validateProfileEntry(rawProfiles[index], `the profile epoch manifest profiles[${index}]`);
    if (ids.has(entry.id)) {
      throw new PipelineV2ProfileEpochError(
        "invalid_manifest",
        `the profile epoch manifest declares profile id ${JSON.stringify(entry.id)} more than once`,
      );
    }
    ids.add(entry.id);
    profiles.push(entry);
  }
  return deepFreezeValue({
    schema_version: PROFILE_EPOCH_SCHEMA_VERSION,
    kind: PROFILE_EPOCH_KIND,
    profiles,
  });
}

/**
 * The manifest digest: SHA-256 over the closed epoch domain followed by
 * the manifest's canonical JSON bytes.
 */
export function pipelineV2ProfileEpochDigest(manifest: PipelineV2ProfileEpochManifest): string {
  return createHash("sha256")
    .update(Buffer.from(MANIFEST_DIGEST_DOMAIN, "utf8"))
    .update(Buffer.from(canonicalJson(manifest), "utf8"))
    .digest("hex");
}

/**
 * Prepares one deep-frozen epoch manifest from the per-profile identity
 * entries in their exact deterministic order (the pipeline's unique
 * profile declaration order). Every entry must already be a valid
 * manifest entry; the preparation is the single validation chain.
 */
export function preparePipelineV2ProfileEpoch(
  entries: readonly ProfileEpochProfileEntry[],
): PreparedPipelineV2ProfileEpoch {
  const manifest = validateProfileEpochManifest({
    schema_version: PROFILE_EPOCH_SCHEMA_VERSION,
    kind: PROFILE_EPOCH_KIND,
    profiles: entries,
  });
  const canonical_json = canonicalJson(manifest);
  const sha256 = pipelineV2ProfileEpochDigest(manifest);
  return deepFreezeValue({ manifest, canonical_json, sha256 });
}

/**
 * Parses one stored manifest document (canonical JSON bytes as text) into
 * the prepared snapshot. Malformed JSON is a stable content-free
 * diagnostic without parser fragments. The stored-byte canonicality check
 * belongs to the store adapter, which compares the bytes it read.
 */
export function parsePipelineV2ProfileEpoch(raw: string): PreparedPipelineV2ProfileEpoch {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    throw new PipelineV2ProfileEpochError(
      "invalid_manifest",
      "the stored profile epoch manifest is not valid JSON",
    );
  }
  const manifest = validateProfileEpochManifest(value);
  const canonical_json = canonicalJson(manifest);
  const sha256 = pipelineV2ProfileEpochDigest(manifest);
  return deepFreezeValue({ manifest, canonical_json, sha256 });
}

/** Validates one durable state binding of the profile epoch. */
export function validatePipelineV2ProfileEpochBinding(
  value: unknown,
  what: string,
): PipelineV2ProfileEpochBinding {
  if (!isPlainObject(value)) {
    throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} must be an object`);
  }
  expectExactKeys(value, what, ["digest", "profile_count"]);
  const digest = expectSha256(value["digest"], `${what} digest`);
  const profileCount = value["profile_count"];
  if (!isNonNegativeSafeInteger(profileCount)) {
    throw new PipelineV2ProfileEpochError("invalid_manifest", `${what} profile_count must be a non-negative safe integer`);
  }
  return Object.freeze({ digest, profile_count: profileCount });
}

/**
 * Confirms a loaded manifest against its durable binding (digest and
 * profile count). A mismatch is a typed integrity failure.
 */
export function requireProfileEpochBindingMatch(
  prepared: PreparedPipelineV2ProfileEpoch,
  binding: PipelineV2ProfileEpochBinding,
): void {
  const count = prepared.manifest.profiles.length;
  if (prepared.sha256 !== binding.digest || count !== binding.profile_count) {
    throw new PipelineV2ProfileEpochError(
      "invalid_manifest",
      "the durable profile epoch binding does not match the stored profile epoch manifest",
    );
  }
}
