import { verifyArtifact } from "./artifact.ts";
import {
  DockerHelperError,
} from "./docker_helper.ts";
import {
  runWithChildSession,
  type LifecycleDeps,
  type LifecycleOptions,
  type LifecycleOutcome,
  type SessionContext,
} from "./lifecycle.ts";
import { pullArgs, runArgs, smokeWorkerSpec } from "./worker.ts";

export type SmokeOptions = LifecycleOptions;

export type SmokeDeps = LifecycleDeps;

export type SmokeOutcome = LifecycleOutcome;

export const ARTIFACT_DIR = ".pipeline-smoke";

export function artifactPath(workspace: string, runId: string): string {
  return `${workspace.replace(/\/+$/, "")}/${ARTIFACT_DIR}/${runId}/result.json`;
}

export async function runSmoke(
  options: SmokeOptions,
  deps: SmokeDeps,
): Promise<SmokeOutcome> {
  return runWithChildSession(
    deps,
    options,
    {
      withSession: (ctx) => smokeWorkerRun(options, deps, ctx),
    },
    "smoke",
  );
}

class ArtifactTokenError extends Error {
  constructor() {
    super("worker did not confirm the child session token in its environment");
  }
}

async function smokeWorkerRun(
  options: SmokeOptions,
  deps: SmokeDeps,
  ctx: SessionContext,
): Promise<void> {
  const { runId, updateState, childEnv } = ctx;

  console.error(`orchestrator: pulling worker image ${options.workerImage}`);
  const pull = await deps.cli(
    pullArgs(options.workerImage, deps.config.socketPath),
    childEnv,
    "inherit",
  );
  if (pull.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `docker-helper pull ${options.workerImage} failed (exit ${pull.code})`,
    );
  }

  ctx.checkAbort();

  const spec = smokeWorkerSpec(runId, ctx.childToken, options.workerImage);
  console.error(`orchestrator: starting worker in child session`);
  await updateState("worker_running");
  const run = await deps.cli(runArgs(spec, deps.config.socketPath), childEnv, "inherit", {
    signalOnAbort: true,
  });
  if (run.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `worker container failed (exit ${run.code})`,
    );
  }

  ctx.checkAbort();

  const artifact = await verifyArtifact(
    artifactPath(options.workspace, runId),
    runId,
  );
  if (artifact.session_token_present !== true) {
    throw new ArtifactTokenError();
  }
  console.error(`orchestrator: artifact verified for run ${runId}`);
  await updateState("success");
}
