/**
 * Production entrypoint of the control-plane HTTP service.
 *
 * This module is the runnable operator entrypoint: it parses its own
 * strict operator flags, loads the registry once, opens the durable run
 * binding store once, builds the one immutable subprocess client and
 * serves the seven versioned routes on the exact configured bind address
 * (loopback by default, or the container wildcard `0.0.0.0` for port
 * forwarding). It never
 * accepts project or run paths through HTTP, never prints registry
 * contents, credentials or absolute project/input paths, and never
 * executes orchestration itself — the subprocess client remains the sole
 * execution substrate.
 *
 * usage: bun server_main.ts --registry ABS --binding-store ABS --port N
 *                            [--listen-address 127.0.0.1|0.0.0.0] [--env KEY=VALUE ...]
 *
 *   --registry       absolute path of the versioned project registry file
 *   --binding-store  absolute path of the durable run binding store root
 *   --port           the listen port (1..65535)
 *   --listen-address the exact bind address: 127.0.0.1 (default) or
 *                    0.0.0.0 (allows a container port forwarding to reach
 *                    the service; the browser Host/Origin authority stays
 *                    127.0.0.1:<port>)
 *   --env KEY=VALUE  the exact environment of the CLI child processes;
 *                    repeatable, split at the first `=`, duplicates
 *                    rejected — there is no ambient inheritance
 *
 * The body/output/time limits are fixed module constants; the routing
 * authority (config root and pipeline-root form) is derived exclusively
 * from the loaded registry, which must declare one shared authority for
 * every project. Startup failures exit 2 with a content-free diagnostic.
 */

import { join } from "node:path";
import { isCleanAbsolutePath } from "../clean_path.ts";
import {
  startControlPlaneHttpService,
  type ControlPlaneListenAddress,
  type ControlPlaneHttpServiceConfig,
} from "./http_server.ts";

/** The fixed request-body byte limit of the service. */
const BODY_LIMIT_BYTES = 65536;
/** The fixed child stdout/stderr accumulation limits of the client. */
const OUTPUT_LIMIT_BYTES = 4_000_000;
/** The fixed SIGINT grace interval forwarded to the subprocess client. */
const SIGINT_GRACE_MS = 3000;

const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface ParsedControlPlaneServerArgs {
  readonly registryPath: string;
  readonly bindingStoreRoot: string;
  readonly port: number;
  readonly listenAddress: ControlPlaneListenAddress;
  readonly applyStoreRoot: string | null;
  readonly env: Readonly<Record<string, string>>;
}

function usage(): string {
  return [
    "usage: bun server_main.ts --registry ABS --binding-store ABS --port N [--apply-store-root ABS] [--listen-address 127.0.0.1|0.0.0.0] [--env KEY=VALUE ...]",
    "  --registry       absolute path of the versioned project registry file",
    "  --binding-store  absolute path of the durable run binding store root",
    "  --apply-store-root  optional absolute mode-0700 apply-store root; absent",
    "                   means the explicit Project Apply capability is unavailable",
    "  --port           the listen port (1..65535)",
    "  --listen-address the exact bind address: 127.0.0.1 (default) or 0.0.0.0 (container port forwarding)",
    "  --env KEY=VALUE  the exact CLI child environment; repeatable, no ambient inheritance",
  ].join("\n");
}

function failUsage(message: string): never {
  process.stderr.write(`control-plane: ${message}\n${usage()}\n`);
  process.exit(2);
}

function failMissing(flag: string): never {
  failUsage(`${flag} requires a value`);
}

/**
 * The strict operator-flag parser: exact singleton flags, clean absolute
 * paths, a canonical decimal port, and an explicitly allow-listed child
 * environment. Unknown flags, positionals and duplicates are refused
 * before any filesystem access.
 */
export function parseControlPlaneServerArgs(argv: readonly string[]): ParsedControlPlaneServerArgs {
  let registryPath: string | null = null;
  let bindingStoreRoot: string | null = null;
  let port: number | null = null;
  let listenAddress: ControlPlaneListenAddress | null = null;
  let applyStoreRoot: string | null = null;
  const env: Record<string, string> = {};
  const seenEnvKeys: string[] = [];
  let index = 0;
  while (index < argv.length) {
    const current = argv[index];
    if (current === undefined) {
      break;
    }
    const takeValue = (flag: string): string | null => {
      if (current === flag) {
        const value = argv[index + 1];
        if (value === undefined || value === "") {
          failMissing(flag);
        }
        index += 2;
        return value;
      }
      if (current.startsWith(`${flag}=`)) {
        const value = current.slice(flag.length + 1);
        if (value === "") {
          failMissing(flag);
        }
        index += 1;
        return value;
      }
      return null;
    };
    const registryValue = takeValue("--registry");
    if (registryValue !== null) {
      if (registryPath !== null) failUsage("--registry must not repeat");
      if (!isCleanAbsolutePath(registryValue)) failUsage("--registry must be a clean absolute path");
      registryPath = registryValue;
      continue;
    }
    const storeValue = takeValue("--binding-store");
    if (storeValue !== null) {
      if (bindingStoreRoot !== null) failUsage("--binding-store must not repeat");
      if (!isCleanAbsolutePath(storeValue)) failUsage("--binding-store must be a clean absolute path");
      bindingStoreRoot = storeValue;
      continue;
    }
    const portValue = takeValue("--port");
    if (portValue !== null) {
      if (port !== null) failUsage("--port must not repeat");
      if (!/^(0|[1-9][0-9]*)$/.test(portValue)) failUsage("--port must be a canonical decimal port number");
      const parsedPort = Number(portValue);
      if (!Number.isSafeInteger(parsedPort) || parsedPort < 1 || parsedPort > 65535) {
        failUsage("--port must be a port number between 1 and 65535");
      }
      port = parsedPort;
      continue;
    }
    const applyStoreValue = takeValue("--apply-store-root");
    if (applyStoreValue !== null) {
      if (applyStoreRoot !== null) failUsage("--apply-store-root must not repeat");
      if (!isCleanAbsolutePath(applyStoreValue)) failUsage("--apply-store-root must be a clean absolute path");
      applyStoreRoot = applyStoreValue;
      continue;
    }
    const listenValue = takeValue("--listen-address");
    if (listenValue !== null) {
      if (listenAddress !== null) failUsage("--listen-address must not repeat");
      if (listenValue !== "127.0.0.1" && listenValue !== "0.0.0.0") {
        failUsage("--listen-address must be exactly 127.0.0.1 or 0.0.0.0");
      }
      listenAddress = listenValue;
      continue;
    }
    const envValue = takeValue("--env");
    if (envValue !== null) {
      const separator = envValue.indexOf("=");
      if (separator <= 0) failUsage("--env must have the form KEY=VALUE");
      const key = envValue.slice(0, separator);
      if (!ENV_NAME_PATTERN.test(key)) failUsage("--env key must be a valid environment variable name");
      for (const seen of seenEnvKeys) {
        if (seen === key) failUsage(`--env key ${key} must not repeat`);
      }
      seenEnvKeys.push(key);
      env[key] = envValue.slice(separator + 1);
      continue;
    }
    failUsage(`unknown argument ${current}`);
  }
  if (registryPath === null) failUsage("--registry is required");
  if (bindingStoreRoot === null) failUsage("--binding-store is required");
  if (port === null) failUsage("--port is required");
  return { registryPath, bindingStoreRoot, port, listenAddress: listenAddress ?? "127.0.0.1", applyStoreRoot, env };
}

export function buildControlPlaneServerConfig(args: ParsedControlPlaneServerArgs): ControlPlaneHttpServiceConfig {
  return {
    registryPath: args.registryPath,
    bindingStoreRoot: args.bindingStoreRoot,
    client: {
      bunExecutable: process.execPath,
      orchestratorEntrypoint: join(import.meta.dir, "..", "main.ts"),
      workingDirectory: process.cwd(),
      baseEnv: args.env,
      stdoutLimitBytes: OUTPUT_LIMIT_BYTES,
      stderrLimitBytes: OUTPUT_LIMIT_BYTES,
      sigintGraceMs: SIGINT_GRACE_MS,
    },
    port: args.port,
    bodyLimitBytes: BODY_LIMIT_BYTES,
    listenAddress: args.listenAddress,
    ...(args.applyStoreRoot === null ? {} : { applyStoreRoot: args.applyStoreRoot }),
  };
}

if (import.meta.main) {
  const args = parseControlPlaneServerArgs(process.argv.slice(2));
  try {
    const service = await startControlPlaneHttpService(buildControlPlaneServerConfig(args));
    process.stdout.write(`control-plane: listening on ${args.listenAddress}:${service.port}\n`);
    let exiting = false;
    const shutdown = (): void => {
      if (exiting) return;
      exiting = true;
      void service.stop().then(() => process.exit(0));
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  } catch {
    process.stderr.write("control-plane: service startup failed\n");
    process.exit(2);
  }
}
