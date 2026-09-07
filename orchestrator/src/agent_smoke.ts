import { mkdir, realpath, stat } from "node:fs/promises";
import { isAbsolute, relative, resolve } from "node:path";
import { AgentResultError, verifyAgentResult } from "./agent_result.ts";
import { DockerHelperError, describeError } from "./docker_helper.ts";
import {
  runWithChildSession,
  type LifecycleDeps,
  type LifecycleOptions,
  type LifecycleOutcome,
  type SessionContext,
} from "./lifecycle.ts";
import { AGENT_SMOKE_DIR, agentWorkerSpec, pullArgs, runArgs } from "./worker.ts";

export interface AgentSmokeOptions extends LifecycleOptions {
  taskPath: string;
}

export type AgentSmokeDeps = LifecycleDeps;

export type AgentSmokeOutcome = LifecycleOutcome;

export function agentRunDirPath(workspace: string, runId: string): string {
  return `${workspace.replace(/\/+$/, "")}/${AGENT_SMOKE_DIR}/${runId}`;
}

export function agentResultFilePath(workspace: string, runId: string): string {
  return `${agentRunDirPath(workspace, runId)}/result.json`;
}

export class TaskFileError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TaskFileError";
  }
}

export interface ResolvedTaskFile {
  workspaceCanonical: string;
  canonical: string;
  pathInWorkspace: string;
  sha256: string;
}

async function sha256File(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(await Bun.file(path).arrayBuffer());
  return hasher.digest("hex");
}

export async function resolveTaskFile(
  workspace: string,
  taskPath: string,
): Promise<ResolvedTaskFile> {
  let workspaceCanonical: string;
  try {
    workspaceCanonical = await realpath(workspace);
  } catch (cause) {
    throw new TaskFileError(
      `workspace ${workspace} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  const candidate = isAbsolute(taskPath)
    ? resolve(taskPath)
    : resolve(workspace, taskPath);
  if (
    candidate !== workspaceCanonical &&
    !candidate.startsWith(`${workspaceCanonical}/`)
  ) {
    throw new TaskFileError(
      `task file ${taskPath} is not inside workspace ${workspace}`,
    );
  }
  let info;
  try {
    info = await stat(candidate);
  } catch (cause) {
    throw new TaskFileError(
      `task file ${candidate} is not accessible: ${describeError(cause)}`,
    );
  }
  if (!info.isFile()) {
    throw new TaskFileError(`task file ${candidate} is not a regular file`);
  }
  let canonical: string;
  try {
    canonical = await realpath(candidate);
  } catch (cause) {
    throw new TaskFileError(
      `task file ${candidate} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  if (
    canonical !== workspaceCanonical &&
    !canonical.startsWith(`${workspaceCanonical}/`)
  ) {
    throw new TaskFileError(
      `task file ${taskPath} resolves outside workspace ${workspace}`,
    );
  }
  const pathInWorkspace = relative(workspaceCanonical, canonical);
  if (pathInWorkspace === "" || pathInWorkspace.startsWith("..")) {
    throw new TaskFileError(
      `task file ${taskPath} is not a file inside workspace ${workspace}`,
    );
  }
  let sha256: string;
  try {
    sha256 = await sha256File(canonical);
  } catch (cause) {
    throw new TaskFileError(
      `task file ${canonical} is not readable: ${describeError(cause)}`,
    );
  }
  return { workspaceCanonical, canonical, pathInWorkspace, sha256 };
}

async function currentTaskSha256(task: ResolvedTaskFile): Promise<string> {
  let info;
  try {
    info = await stat(task.canonical);
  } catch (cause) {
    throw new TaskFileError(
      `task file ${task.canonical} disappeared during the agent run: ${describeError(cause)}`,
    );
  }
  if (!info.isFile()) {
    throw new TaskFileError(
      `task file ${task.canonical} is no longer a regular file after the agent run`,
    );
  }
  let current: string;
  try {
    current = await sha256File(task.canonical);
  } catch (cause) {
    throw new TaskFileError(
      `cannot re-read task file ${task.canonical} after the agent run: ${describeError(cause)}`,
    );
  }
  return current;
}

export async function runAgentSmoke(
  options: AgentSmokeOptions,
  deps: AgentSmokeDeps,
): Promise<AgentSmokeOutcome> {
  let task: ResolvedTaskFile | null = null;

  return runWithChildSession(
    deps,
    options,
    {
      preSession: async (ctx) => {
        task = await resolveTaskFile(options.workspace, options.taskPath);
        console.error(
          `orchestrator: task file ok (${task.pathInWorkspace}, sha256 ${task.sha256})`,
        );
        await mkdir(agentRunDirPath(options.workspace, ctx.runId), { recursive: true });
        console.error(`orchestrator: run dir ${agentRunDirPath(options.workspace, ctx.runId)}`);
      },
      withSession: (ctx) => agentRun(options, deps, ctx, () => task),
    },
    "agent-smoke",
  );
}

async function agentRun(
  options: AgentSmokeOptions,
  deps: AgentSmokeDeps,
  ctx: SessionContext,
  taskRef: () => ResolvedTaskFile | null,
): Promise<void> {
  const task = taskRef();
  if (task === null) {
    throw new TaskFileError("task file was not prepared before the session");
  }
  const { runId, updateState, childEnv } = ctx;
  const resultPathInWorkspace = `${AGENT_SMOKE_DIR}/${runId}/result.json`;

  console.error(`orchestrator: pulling agent image ${options.workerImage}`);
  const pull = await deps.cli(
    pullArgs(options.workerImage, deps.config.socketPath),
    childEnv,
    "inherit",
  );
  if (pull.code !== 0) {
    console.error(
      `orchestrator: warning: docker-helper pull ${options.workerImage} failed (exit ${pull.code}); continuing, the image may already be present locally`,
    );
  }

  const spec = agentWorkerSpec({
    runId,
    childSessionToken: ctx.childToken,
    workerImage: options.workerImage,
    taskPathInWorkspace: task.pathInWorkspace,
    resultPathInWorkspace,
    baseEnv: deps.baseEnv ?? {},
  });

  console.error(`orchestrator: starting agent in child session`);
  await updateState("agent_running");
  const run = await deps.cli(runArgs(spec, deps.config.socketPath), childEnv, "inherit");
  if (run.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `agent container failed (exit ${run.code})`,
    );
  }

  ctx.checkAbort();

  const currentSha256 = await currentTaskSha256(task);
  if (currentSha256 !== task.sha256) {
    throw new TaskFileError(
      `task file was modified during the agent run (sha256 ${task.sha256} -> ${currentSha256})`,
    );
  }

  const resultFile = agentResultFilePath(options.workspace, runId);
  let raw: string;
  try {
    raw = await Bun.file(resultFile).text();
  } catch (cause) {
    throw new AgentResultError(
      `agent result not readable at ${resultFile}: ${describeError(cause)}`,
    );
  }
  await verifyAgentResult(raw, runId, task.workspaceCanonical);
  console.error(`orchestrator: agent result verified for run ${runId}`);
  await updateState("success");
}
