export interface WorkerMount {
  source: string;
  target: string;
}

export interface WorkerSpec {
  image: string;
  entrypoint?: string;
  command: string[];
  workdir: string;
  mounts: WorkerMount[];
  containerEnv: Record<string, string>;
}

export const DEFAULT_WORKER_IMAGE = "alpine:3.22";
export const DEFAULT_WORKSPACE_MOUNT_TARGET = "/workspace";
export const WORKSPACE_MOUNT_SOURCE = ".";
export const SMOKE_ENV_RUN_ID = "SMOKE_RUN_ID";
export const SESSION_TOKEN_ENV = "DOCKER_HELPER_SESSION_TOKEN";

export const AGENT_SMOKE_DIR = ".pipeline-agent-smoke";
export const AGENT_SMOKE_RUN_ID_ENV = "AGENT_SMOKE_RUN_ID";
export const AGENT_SMOKE_STATE_ID_ENV = "AGENT_SMOKE_STATE_ID";
export const AGENT_SMOKE_ACTIVATION_INDEX_ENV = "AGENT_SMOKE_ACTIVATION_INDEX";
export const AGENT_SMOKE_ATTEMPT_ENV = "AGENT_SMOKE_ATTEMPT";
export const AGENT_SMOKE_RESULT_PATH_ENV = "AGENT_SMOKE_RESULT_PATH";
/** Per-input env var prefix; the input id is appended verbatim. */
export const AGENT_SMOKE_INPUT_ENV_PREFIX = "AGENT_SMOKE_INPUT_";

export const AGENT_SMOKE_ENTRYPOINT = "opencode";
export const OPENCODE_CONFIG_CONTENT_ENV = "OPENCODE_CONFIG_CONTENT";

export const WORKER_SCRIPT = `
set -eu
: "\${SMOKE_RUN_ID:?SMOKE_RUN_ID is required}"
dir=".pipeline-smoke/\${SMOKE_RUN_ID}"
mkdir -p "$dir"
token_present=false
if [ -n "\${DOCKER_HELPER_SESSION_TOKEN:-}" ]; then
  token_present=true
fi
{
  printf '{\\n'
  printf '  "schema_version": 1,\\n'
  printf '  "status": "success",\\n'
  printf '  "run_id": "%s",\\n' "$SMOKE_RUN_ID"
  printf '  "session_token_present": %s\\n' "$token_present"
  printf '}\\n'
} > "$dir/result.json"
`;

export function workspaceMount(): WorkerMount {
  return { source: WORKSPACE_MOUNT_SOURCE, target: DEFAULT_WORKSPACE_MOUNT_TARGET };
}

export function pullArgs(image: string, socketPath: string): string[] {
  return ["pull", "--endpoint", socketPath, image];
}

export function runArgs(
  spec: WorkerSpec,
  socketPath: string,
): string[] {
  const args = [
    "run",
    "--endpoint",
    socketPath,
    "--image",
    spec.image,
  ];
  if (spec.entrypoint !== undefined) {
    args.push("--entrypoint", spec.entrypoint);
  }
  args.push(
    "--workdir",
    spec.workdir,
  );
  for (const mount of spec.mounts) {
    args.push("--mount", `${mount.source}:${mount.target}`);
  }
  for (const key of Object.keys(spec.containerEnv).sort()) {
    args.push("--env", `${key}=${spec.containerEnv[key]}`);
  }
  args.push("--");
  args.push(...spec.command);
  return args;
}

export function smokeWorkerSpec(
  runId: string,
  childSessionToken: string,
  workerImage: string = DEFAULT_WORKER_IMAGE,
): WorkerSpec {
  return {
    image: workerImage,
    command: ["/bin/sh", "-eu", "-c", WORKER_SCRIPT],
    workdir: DEFAULT_WORKSPACE_MOUNT_TARGET,
    mounts: [workspaceMount()],
    containerEnv: {
      [SMOKE_ENV_RUN_ID]: runId,
      [SESSION_TOKEN_ENV]: childSessionToken,
    },
  };
}

export interface AgentWorkerInput {
  id: string;
  /** Workspace-relative path of the input. */
  pathInWorkspace: string;
}

export function agentWorkerSpec(params: {
  runId: string;
  stateId: string;
  activationIndex: number;
  attempt: number;
  childSessionToken: string;
  workerImage: string;
  inputs: readonly AgentWorkerInput[];
  resultPathInWorkspace: string;
  executionDocPathInWorkspace: string;
  profileEnv: Readonly<Record<string, string>>;
  opencodeConfigContent: string;
}): WorkerSpec {
  const containerEnv: Record<string, string> = {
    ...params.profileEnv,
    [OPENCODE_CONFIG_CONTENT_ENV]: params.opencodeConfigContent,
    [SESSION_TOKEN_ENV]: params.childSessionToken,
    [AGENT_SMOKE_RUN_ID_ENV]: params.runId,
    [AGENT_SMOKE_STATE_ID_ENV]: params.stateId,
    [AGENT_SMOKE_ACTIVATION_INDEX_ENV]: String(params.activationIndex),
    [AGENT_SMOKE_ATTEMPT_ENV]: String(params.attempt),
    [AGENT_SMOKE_RESULT_PATH_ENV]: inWorkspace(params.resultPathInWorkspace),
  };
  for (const input of params.inputs) {
    containerEnv[`${AGENT_SMOKE_INPUT_ENV_PREFIX}${input.id}`] = inWorkspace(input.pathInWorkspace);
  }
  return {
    image: params.workerImage,
    entrypoint: AGENT_SMOKE_ENTRYPOINT,
    command: [
      "run",
      "--format",
      "json",
      "--auto",
      agentInstruction(params.executionDocPathInWorkspace),
    ],
    workdir: DEFAULT_WORKSPACE_MOUNT_TARGET,
    mounts: [workspaceMount()],
    containerEnv,
  };
}

function inWorkspace(relative: string): string {
  return `${DEFAULT_WORKSPACE_MOUNT_TARGET}/${relative}`;
}

/**
 * Static instruction handed to OpenCode: it carries only the workspace path of
 * the orchestrator-owned execution document. The pipeline prompt body and the
 * input body never appear in argv or env; OpenCode reads them from the
 * workspace.
 */
export function agentInstruction(executionDocPathInWorkspace: string): string {
  const docPath = inWorkspace(executionDocPathInWorkspace);
  return [
    `Read the execution document at "${docPath}" and follow it exactly.`,
    "It defines the agent instruction, the input file to use, and the required result format.",
    "Do not modify or delete the execution document or the input file it references.",
    "Write the result file only to the result path given in the execution document.",
  ].join(" ");
}
