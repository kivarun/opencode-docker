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

export interface EnvLike {
  readonly [key: string]: string | undefined;
}

export interface HelperConfig {
  socketPath: string;
  credentialFile: string;
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
): Promise<ChildSession> {
  const args = [...operatorArgs(["create"], config), "--workspace", workspace];
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
