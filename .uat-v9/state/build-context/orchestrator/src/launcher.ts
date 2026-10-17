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
import { isCleanAbsolutePath } from "./clean_path.ts";

export interface EnvLike {
  readonly [key: string]: string | undefined;
}

export interface HelperConfig {
  socketPath: string;
  credentialFile: string;
}

export type SessionFilesystemAccess = "read_only" | "read_write";

/**
 * One issuance-time Session filesystem root in the docker-helper 2.2.0-rc.10
 * rich-roots grammar: a daemon-visible absolute host path inside the target
 * Launcher's effective ceiling, narrowed to an explicit access mode. The
 * daemon owns overlap, canonicalization, normalization and narrowing; this
 * layer validates only the request's own transport shape.
 */
export interface SessionFilesystemRoot {
  readonly path: string;
  readonly access: SessionFilesystemAccess;
}

export interface CreateChildSessionOptions {
  /**
   * Issuance-time Session filesystem roots, passed as repeatable
   * `--filesystem-root PATH=ACCESS` flags in list order, always before the
   * positional `WORKSPACE`. Transport-shape only: this layer never
   * re-decides helper authorization semantics (the daemon narrows, the CLI
   * validates syntax). An absent or empty list keeps the argv byte-for-byte
   * identical to the no-policy form.
   */
  readonly filesystemRoots?: readonly SessionFilesystemRoot[];
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
  // docker-helper 2.2.0-rc.10 grammar: `session create ... WORKSPACE` — the
  // workspace is the one required positional operand and is always passed
  // last; every issuance-time root travels as one repeatable
  // `--filesystem-root PATH=ACCESS` flag pair.
  const args = [...operatorArgs(["create"], config)];
  const roots = options?.filesystemRoots;
  if (roots !== undefined) {
    if (!Array.isArray(roots)) {
      throw new Error("docker-helper session create: filesystem roots must be a list");
    }
    // Transport-shape validation and argv capture happen in one
    // synchronous pass before the first await: the caller-owned list and
    // root objects are never frozen or modified, and a later mutation of
    // them cannot change the captured argv. Values travel only in argv,
    // never in env. Each root serializes as exactly one flag/value pair,
    // so a path containing "=" stays one argv value (the daemon CLI
    // splits the flag value at its last "="). Overlap, duplicate
    // canonical roots and narrowing are daemon decisions and are never
    // re-implemented here.
    for (const root of roots) {
      if (typeof root !== "object" || root === null || Array.isArray(root)) {
        throw new Error(
          "docker-helper session create: filesystem root must be an object with path and access",
        );
      }
      const path = (root as { path?: unknown }).path;
      const access = (root as { access?: unknown }).access;
      if (typeof path !== "string" || path === "") {
        throw new Error(
          "docker-helper session create: filesystem root path must be a non-empty string",
        );
      }
      if (!isCleanAbsolutePath(path)) {
        throw new Error(
          `docker-helper session create: filesystem root path ${JSON.stringify(path)} must be a clean absolute host path`,
        );
      }
      if (access !== "read_only" && access !== "read_write") {
        throw new Error(
          `docker-helper session create: filesystem root access for path ${JSON.stringify(path)} must be exactly "read_only" or "read_write"`,
        );
      }
      args.push("--filesystem-root", `${path}=${access}`);
    }
  }
  args.push(workspace);
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
  // docker-helper 2.2.0-rc.10 grammar: the session id is the one required
  // positional operand of `session delete` (the legacy `--id` flag is not
  // part of the RC10 CLI surface).
  const args = [...operatorArgs(["delete"], config), sessionId];
  const result = await cli(args, env, "capture");
  if (result.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `docker-helper session delete failed (exit ${result.code}): ${result.stderr?.trim() || "no diagnostics"}`,
    );
  }
  parseSessionDelete(result.stdout ?? "", sessionId);
}
