import { realpath, stat } from "node:fs/promises";
import { isAbsolute, join, relative } from "node:path";
import { describeError } from "./docker_helper.ts";

export const PROFILE_SCHEMA_VERSION = 1;

export class ProfileError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProfileError";
  }
}

export interface ProfileEnvBinding {
  from_env: string;
  required: boolean;
}

export interface ProfileSpec {
  schema_version: number;
  image: string;
  opencode_config: string;
  env: Record<string, ProfileEnvBinding>;
}

export interface ResolvedProfile {
  profileName: string;
  image: string;
  opencodeConfigPath: string;
  opencodeConfigContent: string;
  env: Record<string, string>;
}

const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
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
  return name.length > 0 && name.length <= ENV_NAME_MAX_LENGTH && ENV_NAME_PATTERN.test(name);
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
      `profile has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PROFILE_SCHEMA_VERSION}`,
    );
  }
  const image = expectNonEmptyString(obj.image, "profile image");
  const opencodeConfig = expectNonEmptyString(obj.opencode_config, "profile opencode_config");
  if (isAbsolute(opencodeConfig)) {
    throw new ProfileError(
      "profile opencode_config must be a path relative to the configuration root",
    );
  }
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
    expectExactKeys(binding, ["from_env", "required"], `profile env binding ${JSON.stringify(destination)}`);
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
    env[destination] = { from_env: fromEnv, required: binding.required };
  }
  return { schema_version: PROFILE_SCHEMA_VERSION, image, opencode_config: opencodeConfig, env };
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
    join(rootCanonical, spec.opencode_config),
    rootCanonical,
    "profile opencode config",
  );
  if (relative(rootCanonical, profileFile).startsWith("profiles/") === false) {
    throw new ProfileError(
      `profile file ${profileFile} is not under the profiles directory of the configuration root`,
    );
  }
  return {
    profileName,
    image: spec.image,
    opencodeConfigPath: opencodeCanonical,
    opencodeConfigContent: await readFileContent(opencodeCanonical, "profile opencode config"),
    env: resolveEnvBindings(spec.env, baseEnv),
  };
}
