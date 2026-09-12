import {
  classifyAuthResult,
  credentialPath,
  describeError,
  DockerHelperError,
  fetchAuthOverSocket,
  operatorArgs,
  parseSessionCreate,
  parseSessionDelete,
  type AuthFetcher,
  type AuthInfo,
  type CliRunner,
} from "./docker_helper.ts";
import { isAbsolute } from "node:path";

export interface EnvLike {
  readonly [key: string]: string | undefined;
}

export interface HelperConfig {
  socketPath: string;
  credentialFile: string;
}

export type ChildSessionFilesystemAccess = "read_only" | "read_write";

export interface ChildSessionFilesystemEntry {
  /** Workspace-relative path (`.` for the workspace root). */
  readonly path: string;
  readonly access: ChildSessionFilesystemAccess;
}

export interface CreateChildSessionOptions {
  /**
   * Issuance-time Session filesystem narrowing, passed as repeatable
   * `--filesystem-entry PATH=ACCESS` flags after `--workspace` in list
   * order. Transport-shape only: this layer never re-decides helper
   * authorization semantics (the daemon narrows, the CLI validates
   * syntax). An absent or empty list keeps the argv byte-for-byte
   * identical to the no-policy form.
   */
  readonly filesystemEntries?: readonly ChildSessionFilesystemEntry[];
}

export function resolveHelperConfig(env: EnvLike): HelperConfig {
  const socketPath = env.DOCKER_HELPER_SOCKET_PATH?.trim() || "";
  return {
    socketPath: socketPath !== "" ? socketPath : "/run/docker-helper/docker-helper.sock",
    credentialFile: credentialPath({
      XDG_CONFIG_HOME: env.XDG_CONFIG_HOME,
      HOME: env.HOME,
    }),
  };
}

export async function requireLauncherCredential(
  config: HelperConfig,
  fetchAuth: AuthFetcher,
  fileExists: (path: string) => Promise<boolean>,
): Promise<AuthInfo> {
  const installed = await fileExists(config.credentialFile);
  if (!installed) {
    throw new DockerHelperError(
      "credential_missing",
      `no docker-helper credential installed at ${config.credentialFile} (install a Launcher credential with 'docker-helper credential install')`,
    );
  }
  let token: string;
  try {
    token = (await Bun.file(config.credentialFile).text()).trim();
  } catch (cause) {
    throw new DockerHelperError(
      "credential_missing",
      `cannot read docker-helper credential at ${config.credentialFile}: ${describeError(cause)}`,
    );
  }
  if (token === "") {
    throw new DockerHelperError(
      "credential_missing",
      `docker-helper credential at ${config.credentialFile} is empty`,
    );
  }
  const result = await fetchAuth(config.socketPath, token);
  const auth = classifyAuthResult(result);
  if (auth.authority !== "launcher") {
    throw new DockerHelperError(
      "wrong_authority",
      `docker-helper credential authority is ${auth.authority!}, expected launcher`,
    );
  }
  return auth;
}

export interface ChildSession {
  sessionId: string;
  token: string;
  launcherId?: string;
}

export async function createChildSession(
  cli: CliRunner,
  config: HelperConfig,
  workspace: string,
  env: Record<string, string> = {},
  options?: CreateChildSessionOptions,
): Promise<ChildSession> {
  const args = [...operatorArgs(["create"], config), "--workspace", workspace];
  const entries = options?.filesystemEntries;
  if (entries !== undefined) {
    if (!Array.isArray(entries)) {
      throw new Error("docker-helper session create: filesystem entries must be a list");
    }
    // Transport-shape validation and argv capture happen in one
    // synchronous pass before the first await: the caller-owned list and
    // entry objects are never frozen or modified, and a later mutation of
    // them cannot change the captured argv. Values travel only in argv,
    // never in env.
    const seen = new Set<string>();
    const pairs: string[] = [];
    for (const entry of entries) {
      if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
        throw new Error(
          "docker-helper session create: filesystem entry must be an object with path and access",
        );
      }
      const path = (entry as { path?: unknown }).path;
      const access = (entry as { access?: unknown }).access;
      if (typeof path !== "string" || path === "") {
        throw new Error(
          "docker-helper session create: filesystem entry path must be a non-empty string",
        );
      }
      if (path !== ".") {
        if (isAbsolute(path)) {
          throw new Error(
            `docker-helper session create: filesystem entry path ${JSON.stringify(path)} must be workspace-relative`,
          );
        }
        for (const component of path.split("/")) {
          if (component === "" || component === "." || component === "..") {
            throw new Error(
              `docker-helper session create: filesystem entry path ${JSON.stringify(path)} is not a clean workspace-relative path`,
            );
          }
        }
      }
      if (access !== "read_only" && access !== "read_write") {
        throw new Error(
          `docker-helper session create: filesystem entry access for path ${JSON.stringify(path)} must be exactly "read_only" or "read_write"`,
        );
      }
      if (seen.has(path)) {
        throw new Error(
          `docker-helper session create: duplicate filesystem entry path ${JSON.stringify(path)}`,
        );
      }
      seen.add(path);
      pairs.push(`${path}=${access}`);
    }
    if (entries.length > 0 && !seen.has(".")) {
      throw new Error(
        'docker-helper session create: filesystem entries must include the workspace root "." exactly once',
      );
    }
    for (const pair of pairs) {
      args.push("--filesystem-entry", pair);
    }
  }
  const result = await cli(args, env, "capture");
  if (result.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `docker-helper session create failed (exit ${result.code}): ${result.stderr?.trim() || "no diagnostics"}`,
    );
  }
  return parseSessionCreate(result.stdout ?? "");
}

export async function deleteChildSession(
  cli: CliRunner,
  config: HelperConfig,
  sessionId: string,
  env: Record<string, string> = {},
): Promise<void> {
  const args = [...operatorArgs(["delete"], config), "--id", sessionId];
  const result = await cli(args, env, "capture");
  if (result.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `docker-helper session delete failed (exit ${result.code}): ${result.stderr?.trim() || "no diagnostics"}`,
    );
  }
  parseSessionDelete(result.stdout ?? "", sessionId);
}
