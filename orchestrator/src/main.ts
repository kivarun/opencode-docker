import {
  fetchAuthOverSocket,
  type CliRunOptions,
  type CliStdio,
  SubprocessCliRunner,
} from "./docker_helper.ts";
import { resolveHelperConfig } from "./launcher.ts";
import { runAgentSmoke, type AgentSmokeOptions } from "./agent_smoke.ts";
import { runSmoke, type SmokeOptions } from "./smoke.ts";
import type { LifecycleDeps } from "./lifecycle.ts";
import { DEFAULT_WORKER_IMAGE } from "./worker.ts";

interface ParsedSmokeArgs {
  kind: "smoke";
  workspace: string;
  workerImage: string;
  launcherId?: string;
}

interface ParsedAgentSmokeArgs {
  kind: "agent-smoke";
  workspace: string;
  configRoot: string;
  profileName: string;
  launcherId?: string;
  taskPath?: string;
}

type ParsedCommand = ParsedSmokeArgs | ParsedAgentSmokeArgs;

function usage(): string {
  return [
    "usage: orchestrator <command> [flags]",
    "",
    "commands:",
    "  smoke        one-shot launcher-delegation smoke test:",
    "               launcher credential -> child session -> worker container ->",
    "               workspace artifact -> verified cleanup",
    "  agent-smoke  OpenCode agent smoke test:",
    "               launcher credential -> child session -> OpenCode agent ->",
    "               work product -> validated result.json -> unchanged TASK.md ->",
    "               verified cleanup",
    "",
    "common flags:",
    "  --workspace PATH        workspace passed to 'docker-helper session create'; must exist",
    "                          and be visible to the orchestrator at the same absolute path",
    "                          (default: ${SMOKE_WORKSPACE:-/workspace})",
    "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
    "",
    "smoke flags:",
    `  --image WORKER_IMAGE    worker container image (default: ${DEFAULT_WORKER_IMAGE})`,
    "",
    "agent-smoke flags:",
    "  --config-root PATH      operator-controlled configuration root; must exist and be",
    "                          visible to the orchestrator at this absolute path",
    "  --profile NAME          execution profile to run under; loaded from",
    "                          <config-root>/profiles/<name>.yaml",
    "  --task PATH             task file for the agent; must be a regular file inside the",
    "                          workspace (default: <workspace>/TASK.md); the orchestrator",
    "                          does not place the file content in command lines,",
    "                          environment, state or its own structured diagnostics,",
    "                          but raw worker output may print whatever the worker holds",
    "",
    "The execution profile owns the trusted worker configuration: the worker image, the",
    "OpenCode configuration file and the exact environment bindings. There is no --image",
    "flag for agent-smoke; the image comes only from the selected profile.",
    "",
    "Profile schema (schema_version 1, YAML):",
    "  schema_version: 1",
    "  image: <worker image>",
    "  opencode_config: <path relative to the configuration root>",
    "  env:",
    "    DEST:",
    "      from_env: SOURCE_VAR",
    "      required: true|false",
    "",
    "Profile rules:",
    "  - unknown and missing fields are rejected; schema_version must be 1",
    "  - the profile name is a single safe path component; the profile file and the OpenCode",
    "    configuration must be regular files that resolve inside the configuration root",
    "  - env bindings are exact: only declared variables reach the worker, there is no",
    "    ambient inheritance; a missing required source variable fails before any child",
    "    session is created; a missing optional source variable is not forwarded",
    "  - orchestrator-owned control variables (DOCKER_HELPER_*, AGENT_SMOKE_*,",
    "    ORCHESTRATOR_*, OPENCODE_CONFIG_CONTENT) and operator path variables (HOME,",
    "    XDG_CONFIG_HOME, XDG_STATE_HOME, XDG_RUNTIME_DIR) can be neither destinations",
    "    nor sources",
    "  - profile files may reference secret environment-variable names but must never",
    "    contain secret values; resolved values are passed to the worker as",
    "    `--env KEY=VALUE` arguments of the docker-helper run CLI call; until",
    "    docker-helper issue #3 is implemented they appear in that process's argv;",
    "    they never appear in state files or the orchestrator's own diagnostics",
    "",
    "The OpenCode configuration is read by the orchestrator from the path given by",
    "opencode_config and forwarded to the worker as OPENCODE_CONFIG_CONTENT.",
    "",
    "The launcher credential is read from",
    "  ${XDG_CONFIG_HOME:-$HOME/.config}/docker-helper/credential.token",
    "install it with: XDG_CONFIG_HOME=<private-dir> docker-helper credential install",
  ].join("\n");
}

function parseValue(argv: string[], i: number, flag: string): { value: string; next: number } {
  const arg = argv[i] ?? "";
  if (arg === flag) {
    const value = argv[i + 1];
    if (value === undefined || value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i + 1 };
  }
  if (arg.startsWith(`${flag}=`)) {
    const value = arg.slice(`${flag}=`.length);
    if (value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i };
  }
  throw new Error(`unknown argument: ${arg}`);
}

function parseCommand(kind: "smoke" | "agent-smoke", argv: string[]): ParsedCommand {
  const defaultWorkspace = process.env.SMOKE_WORKSPACE?.trim() || "/workspace";
  let workspace: string | null = null;
  let workerImage: string | null = null;
  let launcherId: string | undefined;
  let configRoot: string | null = null;
  let profileName: string | null = null;
  let taskPath: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      const { value, next } = parseValue(argv, i, "--workspace");
      if (!value.startsWith("/")) {
        throw new Error("--workspace must be an absolute path");
      }
      workspace = value;
      i = next;
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke does not accept --image; the worker image comes only from the selected execution profile",
        );
      }
      const { value, next } = parseValue(argv, i, "--image");
      workerImage = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      const { value, next } = parseValue(argv, i, "--profile");
      profileName = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      launcherId = value;
      i = next;
    } else if (kind === "agent-smoke" && (arg === "--task" || arg.startsWith("--task="))) {
      const { value, next } = parseValue(argv, i, "--task");
      taskPath = value;
      i = next;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (kind === "smoke") {
    return {
      kind,
      workspace: workspace ?? defaultWorkspace,
      workerImage: workerImage ?? DEFAULT_WORKER_IMAGE,
      launcherId,
    };
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for agent-smoke");
  }
  if (profileName === null) {
    throw new Error("--profile PROFILE_NAME is required for agent-smoke");
  }
  return {
    kind,
    workspace: workspace ?? defaultWorkspace,
    configRoot,
    profileName,
    launcherId,
    taskPath,
  };
}

async function main(): Promise<number> {
  const argv = process.argv.slice(2);
  const command = argv[0];

  if (command !== "smoke" && command !== "agent-smoke") {
    console.error(
      `error: expected 'orchestrator smoke' or 'orchestrator agent-smoke'${command !== undefined ? `, got ${JSON.stringify(command)}` : ""}`,
    );
    console.error(usage());
    return 2;
  }

  let parsed: ParsedCommand;
  try {
    parsed = parseCommand(command, argv.slice(1));
  } catch (cause) {
    console.error(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    console.error(usage());
    return 2;
  }

  const config = resolveHelperConfig(process.env);
  const runner = new SubprocessCliRunner();
  const deps: LifecycleDeps = {
    cli: (args: string[], env: Record<string, string>, stdio: CliStdio, opts?: CliRunOptions) =>
      runner.run(args, env, stdio, opts),
    fetchAuth: fetchAuthOverSocket,
    config,
    baseEnv: process.env,
    onSignal: (handler: (signal: "SIGINT" | "SIGTERM") => void) => {
      for (const signal of ["SIGINT", "SIGTERM"] as const) {
        process.on(signal, () => {
          // forward the same signal to a running worker `run` CLI process only;
          // session create/delete and pull always run to completion
          runner.killActive(signal);
          handler(signal);
        });
      }
    },
  };

  if (parsed.kind === "smoke") {
    const options: SmokeOptions = {
      workspace: parsed.workspace,
      workerImage: parsed.workerImage,
      launcherId: parsed.launcherId,
    };
    const outcome = await runSmoke(options, deps);
    if (outcome.ok) {
      console.error(
        `orchestrator: smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
      );
    }
    return outcome.exitCode;
  }

  const options: AgentSmokeOptions = {
    workspace: parsed.workspace,
    configRoot: parsed.configRoot,
    profileName: parsed.profileName,
    launcherId: parsed.launcherId,
    taskPath: parsed.taskPath ?? `${parsed.workspace.replace(/\/+$/, "")}/TASK.md`,
  };
  const outcome = await runAgentSmoke(options, deps);
  if (outcome.ok) {
    console.error(
      `orchestrator: agent-smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
    );
  }
  return outcome.exitCode;
}

process.exit(await main());
