export interface WorkerSpec {
  image: string;
  entrypoint?: string;
  command: string[];
  workdir: string;
  mount: string;
  containerEnv: Record<string, string>;
}

export const DEFAULT_WORKER_IMAGE = "alpine:3.22";
export const DEFAULT_WORKSPACE_MOUNT_TARGET = "/workspace";
export const SMOKE_ENV_RUN_ID = "SMOKE_RUN_ID";
export const SESSION_TOKEN_ENV = "DOCKER_HELPER_SESSION_TOKEN";

export const AGENT_SMOKE_DIR = ".pipeline-agent-smoke";
export const AGENT_SMOKE_RUN_ID_ENV = "AGENT_SMOKE_RUN_ID";
export const AGENT_SMOKE_TASK_PATH_ENV = "AGENT_SMOKE_TASK_PATH";
export const AGENT_SMOKE_RESULT_PATH_ENV = "AGENT_SMOKE_RESULT_PATH";

export const AGENT_SMOKE_ENTRYPOINT = "opencode";

export const AGENT_WORKER_ENV_ALLOWLIST = [
  "LLM_SERVER",
  "LLM_KEY",
  "OPENCODE_CONFIG_CONTENT",
  "OPENCODE_ENABLE_EXA",
  "OPENCODE_EXPERIMENTAL_LSP_TOOL",
] as const;

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

export function smokeWorkerSpec(
  runId: string,
  childSessionToken: string,
  workerImage: string = DEFAULT_WORKER_IMAGE,
): WorkerSpec {
  return {
    image: workerImage,
    command: ["/bin/sh", "-eu", "-c", WORKER_SCRIPT],
    workdir: DEFAULT_WORKSPACE_MOUNT_TARGET,
    mount: `.:${DEFAULT_WORKSPACE_MOUNT_TARGET}`,
    containerEnv: {
      [SMOKE_ENV_RUN_ID]: runId,
      [SESSION_TOKEN_ENV]: childSessionToken,
    },
  };
}

export function agentWorkerSpec(params: {
  runId: string;
  childSessionToken: string;
  workerImage: string;
  taskPathInWorkspace: string;
  resultPathInWorkspace: string;
  baseEnv: Readonly<Record<string, string | undefined>>;
}): WorkerSpec {
  const containerEnv = agentContainerEnv(params.baseEnv, {
    [SESSION_TOKEN_ENV]: params.childSessionToken,
    [AGENT_SMOKE_RUN_ID_ENV]: params.runId,
    [AGENT_SMOKE_TASK_PATH_ENV]: inWorkspace(params.taskPathInWorkspace),
    [AGENT_SMOKE_RESULT_PATH_ENV]: inWorkspace(params.resultPathInWorkspace),
  });
  return {
    image: params.workerImage,
    entrypoint: AGENT_SMOKE_ENTRYPOINT,
    command: [
      "run",
      "--format",
      "json",
      "--auto",
      agentInstruction(params.taskPathInWorkspace, params.resultPathInWorkspace, params.runId),
    ],
    workdir: DEFAULT_WORKSPACE_MOUNT_TARGET,
    mount: `.:${DEFAULT_WORKSPACE_MOUNT_TARGET}`,
    containerEnv,
  };
}

function inWorkspace(relative: string): string {
  return `${DEFAULT_WORKSPACE_MOUNT_TARGET}/${relative}`;
}

export function agentContainerEnv(
  baseEnv: Readonly<Record<string, string | undefined>>,
  runEnv: Record<string, string>,
): Record<string, string> {
  const containerEnv: Record<string, string> = {};
  for (const key of AGENT_WORKER_ENV_ALLOWLIST) {
    const value = baseEnv[key];
    if (typeof value === "string" && value !== "") {
      containerEnv[key] = value;
    }
  }
  Object.assign(containerEnv, runEnv);
  return containerEnv;
}

export function agentInstruction(
  taskPathInWorkspace: string,
  resultPathInWorkspace: string,
  runId: string,
): string {
  return [
    `Read the task file at "${inWorkspace(taskPathInWorkspace)}" and perform only the work it describes.`,
    `Do not modify or delete the task file, and do not modify any other existing file unless the task explicitly requires it.`,
    `When the task is done, write the result file to "${inWorkspace(resultPathInWorkspace)}".`,
    "The result file must contain a single JSON object with exactly this shape:",
    `{"schema_version":1,"run_id":"${runId}","status":"completed","summary":"<one sentence describing the work performed>","artifacts":["<workspace-relative paths of files the task created>"]}`,
    "Use workspace-relative paths in artifacts, never absolute paths and never paths outside the workspace.",
    "Do not list the task file in artifacts.",
    "The result file must contain valid JSON and nothing else.",
  ].join(" ");
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
    "--mount",
    spec.mount,
  );
  for (const [key, value] of Object.entries(spec.containerEnv)) {
    args.push("--env", `${key}=${value}`);
  }
  args.push("--");
  args.push(...spec.command);
  return args;
}

export function pullArgs(image: string, socketPath: string): string[] {
  return ["pull", "--endpoint", socketPath, image];
}
