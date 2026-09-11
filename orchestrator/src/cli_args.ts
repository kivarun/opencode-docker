import type { AgentSmokeOptions } from "./agent_smoke.ts";
import type { RunInputBinding } from "./pipeline_v2_runtime.ts";
import { expectSafeId } from "./pipeline_v2_state.ts";
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

export interface ParsedPipelineRunArgs {
  readonly kind: "run";
  readonly pipelineRoot: string;
  readonly configRoot: string;
  readonly projectSourcePath: string;
  readonly inputBindings: readonly RunInputBinding[];
  readonly launcherId?: string;
  readonly json: boolean;
}

export type ParsedCommand = ParsedSmokeArgs | ParsedAgentSmokeArgs | ParsedPipelineRunArgs;

export function usage(): string {
  return [
    "usage: orchestrator <command> [flags]",
    "",
    "commands:",
    "  smoke        one-shot launcher-delegation smoke test:",
    "               launcher credential -> child session -> worker container ->",
    "               workspace artifact -> verified cleanup",
    "  agent-smoke  OpenCode agent smoke test driven by a declarative pipeline:",
    "               pipeline -> multi-state plan -> profile and input resolution ->",
    "               launcher credential -> child session per agent-state activation ->",
    "               OpenCode agent -> work product -> validated result.json ->",
    "               unchanged protected input -> outcome transition -> verified cleanup",
    "  run          production pipeline v2 execution:",
    "               pipeline bundle -> profiles -> launcher credential -> run root ->",
    "               durable state -> agent/decision activations -> decision routing ->",
    "               terminal -> published run outputs",
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
    "run flags (production pipeline v2):",
    "  --pipeline-root PATH    pipeline bundle root with schema_version 2; required; absolute path.",
    "                          There is no default: the bundled pipeline is still the v1 diagnostic",
    "                          pipeline and is never used by 'run'.",
    "  --config-root PATH      operator-controlled configuration root holding",
    "                          profiles/<name>.yaml and the OpenCode configurations; required;",
    "                          absolute path",
    "  --project PATH          project source directory; required; absolute path. The runner",
    "                          copies it once into the run-owned directory",
    "                          <state-root>/pipeline-runs/<run-id>/project; every activation",
    "                          mounts that copy read-write at /workspace and worker changes",
    "                          persist there across activations. The source directory is",
    "                          never modified, and its path never reaches the worker.",
    "  --input ID=PATH         bind one declared pipeline run input; repeatable, declaration",
    "                          order is preserved. ID is a safe identifier (letters, digits,",
    "                          '_', '.', '-', at most 128 characters) and PATH an absolute",
    "                          host path; the pair is split at the first '=', so '=' inside",
    "                          PATH is allowed. Omitting --input entirely is valid when the",
    "                          pipeline declares no run inputs; the trusted pipeline loader",
    "                          and the data plane verify that every declared input is bound",
    "                          exactly once and reject missing, duplicate or unknown ids.",
    "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
    "                          (no progress lines); worker and image-pull output is forwarded",
    "                          to stderr; the exit code is the outcome's exit code.",
    "",
    "run locations and runtime configuration:",
    "  The durable run state is written to",
    "    <state-root>/pipeline-runs/<run-id>/state.json",
    "  and the published run outputs stay at the fixed location",
    "    <state-root>/pipeline-runs/<run-id>/outputs",
    "  (run outputs are never copied to a user-chosen path). The state root is",
    "  runtime configuration, not a per-run flag:",
    "    ORCHESTRATOR_STATE_ROOT          local state root; otherwise",
    "                                     ${XDG_STATE_HOME:-$HOME/.local/state}/orchestrator",
    "    ORCHESTRATOR_DAEMON_STATE_ROOT   the same directory by the path the Docker Helper",
    "                                     daemon sees; defaults to the local root (host mode)",
    "  Both variables must be non-empty absolute clean paths; nothing is created by",
    "  the resolver, and the runner itself verifies kind, canonical form, identity",
    "  and mode 0700.",
    "",
    "run profiles and worker configuration:",
    "  The pipeline's agent states select the profiles; the worker image, the",
    "  OpenCode configuration and the exact environment bindings belong to those",
    "  profiles (see the profile schema under agent-smoke). There are no --image,",
    "  --profile, --task, --workspace or output-path flags for 'run': the user never",
    "  sets mounts, container paths, Session credentials, helper transport, or the",
    "  state-root projection per run. Prompt and input bodies travel only inside the",
    "  orchestrator-owned execution document and never appear in command lines,",
    "  environment, state, or diagnostics. A failure before the run root is created",
    "  leaves nothing behind; afterwards the run root is the diagnostic directory",
    "  and is never removed automatically.",
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
    "the result contract, and the timeout. The supported v1 execution shape covers",
    "sequential states, branching and cycles bounded by max_transitions: any number",
    "of agent and terminal states, an agent or terminal entry state, one transition",
    'per agent state with outcome "completed", max_attempts 1, and the standard',
    "agent result contract. Any other structurally valid pipeline is rejected before",
    "Launcher authentication and before any child Session is created.",
    "",
    "agent-smoke is the v1 diagnostic command; the production pipeline v2 path is",
    "'orchestrator run'.",
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

export function parseCommand(kind: "smoke" | "agent-smoke" | "run", argv: string[]): ParsedCommand {
  if (kind === "run") {
    return parseRunArgs(argv);
  }
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
      if (kind !== "agent-smoke") {
        throw new Error(`unknown argument: ${arg}`);
      }
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

/**
 * Parses the production pipeline v2 command. The user-facing contract:
 * `--pipeline-root`, `--config-root` and `--project` are required and must
 * be absolute; `--input` is repeatable in `SAFE_ID=ABSOLUTE_PATH` form
 * (split at the first `=`, so `=` inside the path is allowed); zero inputs
 * are valid. Singleton flags reject duplicates instead of silently taking
 * the last value; `--json` takes no value and cannot repeat; the v1 and
 * container flags, output-path flags and state-root flags are rejected for
 * `run`, as is every unknown flag.
 */
function parseRunArgs(argv: string[]): ParsedPipelineRunArgs {
  let pipelineRoot: string | null = null;
  let configRoot: string | null = null;
  let projectSourcePath: string | null = null;
  let launcherId: string | undefined;
  let json = false;
  const inputBindings: RunInputBinding[] = [];
  const seenInputIds = new Set<string>();

  const requireSingleton = (current: string | null, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      const { value, next } = parseValue(argv, i, "--pipeline-root");
      requireSingleton(pipelineRoot, "--pipeline-root");
      pipelineRoot = requireAbsolute(value, "--pipeline-root");
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      const { value, next } = parseValue(argv, i, "--project");
      requireSingleton(projectSourcePath, "--project");
      projectSourcePath = requireAbsolute(value, "--project");
      i = next;
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      const { value, next } = parseValue(argv, i, "--input");
      const equals = value.indexOf("=");
      if (equals <= 0) {
        throw new Error("--input requires SAFE_ID=ABSOLUTE_PATH");
      }
      const id = value.slice(0, equals);
      const path = value.slice(equals + 1);
      try {
        expectSafeId(id, "--input id");
      } catch (cause) {
        throw new Error(
          `--input id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      if (path === "") {
        throw new Error("--input requires SAFE_ID=ABSOLUTE_PATH (the path part is empty)");
      }
      if (!path.startsWith("/")) {
        throw new Error(`--input ${id} must be bound to an absolute path`);
      }
      if (seenInputIds.has(id)) {
        throw new Error(`--input ${id} is bound more than once`);
      }
      seenInputIds.add(id);
      inputBindings.push({ id, path });
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "run does not accept --workspace; the run-owned project copy is created from --project",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("run does not accept --image; the worker image comes only from the selected profile");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "run does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error(
        "run does not accept --task; run inputs are declared by the pipeline and bound with --input",
      );
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "run does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "run does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (pipelineRoot === null) {
    throw new Error("--pipeline-root ABSOLUTE_PATH is required for run");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for run");
  }
  if (projectSourcePath === null) {
    throw new Error("--project ABSOLUTE_PATH is required for run");
  }
  return {
    kind: "run",
    pipelineRoot,
    configRoot,
    projectSourcePath,
    inputBindings,
    launcherId,
    json,
  };
}

function defaultWorkspace(): string {
  return process.env.SMOKE_WORKSPACE?.trim() || "/workspace";
}
