import { realpath, stat } from "node:fs/promises";
import { isAbsolute, join, relative } from "node:path";
import { describeError } from "./docker_helper.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  expectDigestPinnedImageRef,
  isValidEnvVarName,
  PROFILE_CONFIG_POLICY_ENV_ONLY,
  profileEpochConfigDigest,
  profileEpochEnvValueDigest,
  type ProfileEpochProfileEntry,
} from "./pipeline_v2_profile_epoch.ts";

export const PROFILE_SCHEMA_VERSION = 2;

export class ProfileError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProfileError";
  }
}

export interface ProfileEnvBinding {
  from_env: string;
  required: boolean;
  secret: boolean;
}

export interface ProfileSpec {
  schema_version: number;
  image: string;
  opencode_config: { path: string; secrets: "env_only" };
  env: Record<string, ProfileEnvBinding>;
}

/**
 * One resolved execution profile: the runtime fields the worker needs and
 * the content-free identity projection the durable profile epoch binds.
 * The object is deep-frozen by `loadProfile`; the identity projection
 * carries no host path and no secret value, digest, presence or length.
 */
export interface ResolvedProfile {
  profileName: string;
  image: string;
  opencodeConfigContent: string;
  env: Record<string, string>;
  /** The content-free identity entry of the durable profile epoch manifest. */
  identity: ProfileEpochProfileEntry;
}

const ENV_NAME_MAX_LENGTH = 256;
const PROFILE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

const CONTROL_ENV_PREFIXES = ["DOCKER_HELPER_", "AGENT_SMOKE_", "ORCHESTRATOR_"] as const;
const CONTROL_ENV_NAMES = [
  "OPENCODE_CONFIG_CONTENT",
  "HOME",
  "XDG_CONFIG_HOME",
  "XDG_STATE_HOME",
  "XDG_RUNTIME_DIR",
] as const;

export function isControlEnvName(name: string): boolean {
  return (
    CONTROL_ENV_PREFIXES.some((prefix) => name.startsWith(prefix)) ||
    (CONTROL_ENV_NAMES as readonly string[]).includes(name)
  );
}

function validEnvName(name: string): boolean {
  return isValidEnvVarName(name);
}

export function validateProfileName(name: string): string {
  if (typeof name !== "string" || !PROFILE_NAME_PATTERN.test(name) || name.includes("..")) {
    throw new ProfileError(`invalid profile name ${JSON.stringify(name)}`);
  }
  return name;
}

function expectObject(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ProfileError(`${what} is not a YAML mapping`);
  }
  return value as Record<string, unknown>;
}

function expectNonEmptyString(value: unknown, what: string): string {
  if (typeof value !== "string" || value === "") {
    throw new ProfileError(`${what} must be a non-empty string`);
  }
  return value;
}

function expectExactKeys(
  obj: Record<string, unknown>,
  keys: readonly string[],
  what: string,
): void {
  const expected = new Set(keys);
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      throw new ProfileError(`${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new ProfileError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
}

export function parseProfileSpec(raw: string): ProfileSpec {
  let parsed: unknown;
  try {
    parsed = Bun.YAML.parse(raw);
  } catch (cause) {
    throw new ProfileError(`profile is not valid YAML: ${describeError(cause)}`);
  }
  const obj = expectObject(parsed, "profile");
  expectExactKeys(obj, ["schema_version", "image", "opencode_config", "env"], "profile");
  if (obj.schema_version !== PROFILE_SCHEMA_VERSION) {
    throw new ProfileError(
      `profile has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PROFILE_SCHEMA_VERSION} (no schema v1 migration exists)`,
    );
  }
  const image = expectDigestPinnedProfileImage(obj.image, "profile image");
  const opencodeConfig = parseOpencodeConfig(obj.opencode_config);
  const envObj = expectObject(obj.env, "profile env");
  const env: Record<string, ProfileEnvBinding> = {};
  for (const [destination, rawBinding] of Object.entries(envObj)) {
    if (!validEnvName(destination)) {
      throw new ProfileError(
        `profile env destination ${JSON.stringify(destination)} is not a valid environment variable name`,
      );
    }
    if (isControlEnvName(destination)) {
      throw new ProfileError(
        `profile env destination ${JSON.stringify(destination)} is an orchestrator-owned control or operator-path variable and cannot be set by a profile`,
      );
    }
    const binding = expectObject(rawBinding, `profile env binding ${JSON.stringify(destination)}`);
    expectExactKeys(binding, ["from_env", "required", "secret"], `profile env binding ${JSON.stringify(destination)}`);
    const fromEnv = expectNonEmptyString(binding.from_env, `profile env binding ${JSON.stringify(destination)} from_env`);
    if (!validEnvName(fromEnv)) {
      throw new ProfileError(
        `profile env binding ${JSON.stringify(destination)} source ${JSON.stringify(fromEnv)} is not a valid environment variable name`,
      );
    }
    if (isControlEnvName(fromEnv)) {
      throw new ProfileError(
        `profile env binding ${JSON.stringify(destination)} source ${JSON.stringify(fromEnv)} is an orchestrator-owned control or operator-path variable and cannot be used as a source`,
      );
    }
    if (typeof binding.required !== "boolean") {
      throw new ProfileError(
        `profile env binding ${JSON.stringify(destination)} required must be a boolean`,
      );
    }
    if (typeof binding.secret !== "boolean") {
      throw new ProfileError(
        `profile env binding ${JSON.stringify(destination)} secret must be a boolean`,
      );
    }
    env[destination] = { from_env: fromEnv, required: binding.required, secret: binding.secret };
  }
  return {
    schema_version: PROFILE_SCHEMA_VERSION,
    image,
    opencode_config: opencodeConfig,
    env,
  };
}

/**
 * The worker image must be a digest-pinned reference: the reference string
 * is the content identity the profile epoch binds. Tag-only references,
 * bare image ids, whitespace or control characters and malformed digests
 * are rejected; the string is preserved verbatim, never rewritten.
 */
function expectDigestPinnedProfileImage(value: unknown, what: string): string {
  const image = expectNonEmptyString(value, what);
  try {
    return expectDigestPinnedImageRef(image, what);
  } catch {
    throw new ProfileError(
      `${what} must be a digest-pinned image reference of the form <repository>[:<tag>]@sha256:<64 lowercase hex>`,
    );
  }
}

/**
 * The OpenCode config declaration of profile schema v2: an exact two-key
 * mapping. `secrets: env_only` is the mandatory trusted-operator assertion
 * that the config content is a non-secret artifact whose literal
 * credentials are forbidden by contract; all secrets must enter through
 * profile env bindings declared `secret: true`. A false assertion is
 * explicitly outside the supported security contract; the orchestrator
 * does not semantically parse the config to detect secrets.
 */
function parseOpencodeConfig(value: unknown): { path: string; secrets: "env_only" } {
  const obj = expectObject(value, "profile opencode_config");
  expectExactKeys(obj, ["path", "secrets"], "profile opencode_config");
  const path = expectNonEmptyString(obj.path, "profile opencode_config path");
  if (isAbsolute(path)) {
    throw new ProfileError(
      "profile opencode_config path must be a path relative to the configuration root",
    );
  }
  if (obj.secrets !== PROFILE_CONFIG_POLICY_ENV_ONLY) {
    throw new ProfileError(
      `profile opencode_config secrets ${JSON.stringify(obj.secrets)} is not the supported config policy ${JSON.stringify(PROFILE_CONFIG_POLICY_ENV_ONLY)}`,
    );
  }
  return { path, secrets: PROFILE_CONFIG_POLICY_ENV_ONLY };
}

async function requireRegularFileInsideRoot(
  path: string,
  rootCanonical: string,
  what: string,
): Promise<string> {
  let info;
  try {
    info = await stat(path);
  } catch (cause) {
    throw new ProfileError(`${what} ${path} is not accessible: ${describeError(cause)}`);
  }
  if (!info.isFile()) {
    throw new ProfileError(`${what} ${path} is not a regular file`);
  }
  let canonical: string;
  try {
    canonical = await realpath(path);
  } catch (cause) {
    throw new ProfileError(`${what} ${path} cannot be canonicalized: ${describeError(cause)}`);
  }
  if (canonical !== rootCanonical && !canonical.startsWith(`${rootCanonical}/`)) {
    throw new ProfileError(`${what} ${path} resolves outside the configuration root`);
  }
  return canonical;
}

async function readFileContent(path: string, what: string): Promise<string> {
  try {
    return await Bun.file(path).text();
  } catch (cause) {
    throw new ProfileError(`${what} ${path} is not readable: ${describeError(cause)}`);
  }
}

export function resolveEnvBindings(
  env: Record<string, ProfileEnvBinding>,
  baseEnv: Readonly<Record<string, string | undefined>>,
): Record<string, string> {
  const resolved: Record<string, string> = {};
  for (const [destination, binding] of Object.entries(env)) {
    const value = baseEnv[binding.from_env];
    if (typeof value !== "string" || value === "") {
      if (binding.required) {
        throw new ProfileError(
          `profile requires environment variable ${binding.from_env} (binding ${destination}) but it is not set`,
        );
      }
      continue;
    }
    resolved[destination] = value;
  }
  return resolved;
}

/**
 * Loads and resolves one execution profile in a single call: the profile
 * spec and the OpenCode config are read once, the env bindings are
 * resolved once, and the returned deep-frozen snapshot carries both the
 * runtime fields and the content-free identity projection the durable
 * profile epoch binds. The absolute canonical config path stays a local
 * implementation detail of the containment check and never enters the
 * returned object.
 */
export async function loadProfile(
  configRoot: string,
  profileName: string,
  baseEnv: Readonly<Record<string, string | undefined>>,
): Promise<ResolvedProfile> {
  validateProfileName(profileName);
  let rootCanonical: string;
  try {
    rootCanonical = await realpath(configRoot);
  } catch (cause) {
    throw new ProfileError(
      `configuration root ${configRoot} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  const profileFile = await requireRegularFileInsideRoot(
    join(rootCanonical, "profiles", `${profileName}.yaml`),
    rootCanonical,
    "profile file",
  );
  const spec = parseProfileSpec(await readFileContent(profileFile, "profile file"));
  const opencodeCanonical = await requireRegularFileInsideRoot(
    join(rootCanonical, spec.opencode_config.path),
    rootCanonical,
    "profile opencode config",
  );
  if (relative(rootCanonical, profileFile).startsWith("profiles/") === false) {
    throw new ProfileError(
      `profile file ${profileFile} is not under the profiles directory of the configuration root`,
    );
  }
  const opencodeConfigContent = await readFileContent(opencodeCanonical, "profile opencode config");
  const resolvedEnv = resolveEnvBindings(spec.env, baseEnv);
  // The identity projection: destination-sorted binding declarations plus,
  // for every non-secret binding whose value resolved at load, the
  // domain-separated digest of that value. Secret bindings carry no value,
  // no digest, no presence and no length.
  const envBindings = Object.entries(spec.env)
    .map(([destination, binding]) => {
      const value = resolvedEnv[destination];
      const entry: {
        destination: string;
        from_env: string;
        required: boolean;
        secret: boolean;
        value_sha256?: string;
      } = {
        destination,
        from_env: binding.from_env,
        required: binding.required,
        secret: binding.secret,
      };
      if (!binding.secret && typeof value === "string") {
        entry.value_sha256 = profileEpochEnvValueDigest(value);
      }
      return entry;
    })
    .sort((left, right) =>
      left.destination < right.destination ? -1 : left.destination > right.destination ? 1 : 0,
    );
  const identity: ProfileEpochProfileEntry = {
    id: profileName,
    image_ref: spec.image,
    config_policy: PROFILE_CONFIG_POLICY_ENV_ONLY,
    config_sha256: profileEpochConfigDigest(opencodeConfigContent),
    env_bindings: envBindings,
  };
  return deepFreezeValue({
    profileName,
    image: spec.image,
    opencodeConfigContent,
    env: resolvedEnv,
    identity,
  });
}
