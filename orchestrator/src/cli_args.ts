import type { AgentSmokeOptions } from "./agent_smoke.ts";
import { DEFAULT_WORKER_IMAGE } from "./worker.ts";

export const DEFAULT_PIPELINE_ROOT = "/opt/orchestrator/pipelines/default";

export interface ParsedSmokeArgs {
  kind: "smoke";
  workspace: string;
  workerImage: string;
  launcherId?: string;
}

export interface ParsedAgentSmokeArgs {
  kind: "agent-smoke";
  workspace: string;
  configRoot: string;
  pipelineRoot: string;
  launcherId?: string;
}

export type ParsedCommand = ParsedSmokeArgs | ParsedAgentSmokeArgs;

export function usage(): string {
  return [
    "usage: orchestrator <command> [flags]",
    "",
    "commands:",
    "  smoke        one-shot launcher-delegation smoke test:",
    "               launcher credential -> child session -> worker container ->",
    "               workspace artifact -> verified cleanup",
    "  agent-smoke  OpenCode agent smoke test driven by a declarative pipeline:",
    "               pipeline -> one-step plan -> profile and input resolution ->",
    "               launcher credential -> child session -> OpenCode agent ->",
    "               work product -> validated result.json -> unchanged protected",
    "               input -> outcome transition -> verified cleanup",
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
    "  --pipeline-root PATH    pipeline bundle root; must exist and be visible to the",
    "                          orchestrator at this absolute path",
    `                          (default: ${DEFAULT_PIPELINE_ROOT}, the bundled default pipeline)`,
    "",
    "The pipeline is the production input for agent-smoke: the declarative pipeline",
    "selects the execution profile, the protected workspace input, the agent prompt,",
    "the result contract, and the timeout. Only the one-step execution shape is",
    "supported today: one agent state whose single transition with outcome",
    '"completed" leads to a success terminal state, max_transitions 1,',
    "max_attempts 1, exactly one protected declared input, and the standard agent",
    "result contract. Any other structurally valid pipeline is rejected before",
    "Launcher authentication and before any child Session is created.",
    "",
    "There is no --profile, --task, or --image flag for agent-smoke: the profile and",
    "input path come only from the pipeline's agent state, the worker image comes",
    "only from the selected profile.",
    "",
    "The execution profile owns the trusted worker configuration: the worker image,",
    "the OpenCode configuration file and the exact environment bindings.",
    "",
    "Profile schema (schema_version 1, YAML, under <config-root>/profiles/<name>.yaml):",
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
    "The agent prompt comes from the pipeline bundle and is materialized, together",
    "with the run identity and the exact result format, into an orchestrator-owned",
    "execution document inside the run directory of the workspace. The worker command",
    "receives only that workspace path; prompt and input bodies never appear in",
    "command lines, environment, state, or the orchestrator's own diagnostics.",
    "",
    "The pipeline's timeout_seconds bounds the worker docker-helper run: after the",
    "deadline the runner sends the CLI process SIGTERM and the run fails normally.",
    "User SIGINT/SIGTERM keeps its own semantics and exit codes 130/143.",
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

export function parseCommand(kind: "smoke" | "agent-smoke", argv: string[]): ParsedCommand {
  let workspace: string | null = null;
  let workerImage: string | null = null;
  let launcherId: string | undefined;
  let configRoot: string | null = null;
  let pipelineRoot: string | undefined;

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
          "agent-smoke does not accept --image; the worker image comes only from the pipeline-selected execution profile",
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
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      if (kind !== "agent-smoke") {
        throw new Error(`unknown argument: ${arg}`);
      }
      const { value, next } = parseValue(argv, i, "--pipeline-root");
      if (!value.startsWith("/")) {
        throw new Error("--pipeline-root must be an absolute path");
      }
      pipelineRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke no longer accepts --profile; the execution profile is selected by the pipeline's agent state",
        );
      }
      throw new Error(`unknown argument: ${arg}`);
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke no longer accepts --task; the workspace input path is declared by the pipeline",
        );
      }
      throw new Error(`unknown argument: ${arg}`);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (kind === "smoke") {
    return {
      kind,
      workspace: workspace ?? defaultWorkspace(),
      workerImage: workerImage ?? DEFAULT_WORKER_IMAGE,
      launcherId,
    };
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for agent-smoke");
  }
  return {
    kind,
    workspace: workspace ?? defaultWorkspace(),
    configRoot,
    pipelineRoot: pipelineRoot ?? DEFAULT_PIPELINE_ROOT,
    launcherId,
  };
}

function defaultWorkspace(): string {
  return process.env.SMOKE_WORKSPACE?.trim() || "/workspace";
}
