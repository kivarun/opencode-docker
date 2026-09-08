import { mkdir, realpath, stat, writeFile } from "node:fs/promises";
import { isAbsolute, relative, resolve } from "node:path";
import {
  AgentResultError,
  verifyAgentResult,
  type AgentResult,
} from "./agent_result.ts";
import { DockerHelperError, describeError } from "./docker_helper.ts";
import {
  loadPipeline,
  planOneStepExecution,
  PipelineError,
  type OneStepPlan,
} from "./pipeline.ts";
import {
  runWithChildSession,
  type LifecycleDeps,
  type LifecycleOptions,
  type LifecycleOutcome,
  type SessionContext,
  type StateSinkFactory,
} from "./lifecycle.ts";
import {
  pipelineExecutionDigest,
} from "./pipeline_digest.ts";
import {
  executePipelineGraph,
  type AgentStateView,
} from "./pipeline_engine.ts";
import {
  PipelineRunStateSink,
  type AcceptedAgentResultRecord,
  type PipelineRunSinkParams,
} from "./pipeline_state_sink.ts";
import type { PipelineIdentityState, ProtectedInputState } from "./pipeline_state.ts";
import type { PipelineStateIo } from "./pipeline_state_store.ts";
import { AgentTimeoutError, WorkspaceInputError } from "./run_errors.ts";
import type { ResolvedProfile } from "./profile.ts";
import { loadProfile } from "./profile.ts";
import { AGENT_SMOKE_DIR, agentWorkerSpec, pullArgs, runArgs } from "./worker.ts";

export { AgentTimeoutError, WorkspaceInputError } from "./run_errors.ts";

export interface AgentSmokeOptions {
  workspace: string;
  configRoot: string;
  pipelineRoot: string;
  launcherId?: string;
}

export type AgentSmokeDeps = LifecycleDeps & {
  /** IO seam for the durable pipeline run state (tests). */
  pipelineStateIo?: PipelineStateIo;
};

export type AgentSmokeOutcome = LifecycleOutcome;

export function agentRunDirPath(workspace: string, runId: string): string {
  return `${workspace.replace(/\/+$/, "")}/${AGENT_SMOKE_DIR}/${runId}`;
}

export function agentResultFilePath(workspace: string, runId: string): string {
  return `${agentRunDirPath(workspace, runId)}/result.json`;
}

export function executionDocumentPath(workspace: string, runId: string): string {
  return `${agentRunDirPath(workspace, runId)}/execution.md`;
}

export interface ResolvedWorkspaceInput {
  workspaceCanonical: string;
  canonical: string;
  pathInWorkspace: string;
  sha256: string;
  dev: number;
  ino: number;
}

async function sha256File(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(await Bun.file(path).arrayBuffer());
  return hasher.digest("hex");
}

export async function resolveWorkspaceInput(
  workspace: string,
  inputPath: string,
): Promise<ResolvedWorkspaceInput> {
  let workspaceCanonical: string;
  try {
    workspaceCanonical = await realpath(workspace);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace ${workspace} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  if (isAbsolute(inputPath)) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} must be a workspace-relative path`,
    );
  }
  const candidate = resolve(workspace, inputPath);
  let info;
  try {
    info = await stat(candidate);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not accessible: ${describeError(cause)}`,
    );
  }
  if (!info.isFile()) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not a regular file`,
    );
  }
  let canonical: string;
  try {
    canonical = await realpath(candidate);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  if (
    canonical !== workspaceCanonical &&
    !canonical.startsWith(`${workspaceCanonical}/`)
  ) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} resolves outside workspace ${workspace}`,
    );
  }
  const pathInWorkspace = relative(workspaceCanonical, canonical);
  if (pathInWorkspace === "" || pathInWorkspace.startsWith("..")) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not a file inside workspace ${workspace}`,
    );
  }
  let sha256: string;
  try {
    sha256 = await sha256File(canonical);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not readable: ${describeError(cause)}`,
    );
  }
  return {
    workspaceCanonical,
    canonical,
    pathInWorkspace,
    sha256,
    dev: info.dev,
    ino: info.ino,
  };
}

async function currentInputSha256(input: ResolvedWorkspaceInput): Promise<string> {
  let info;
  try {
    info = await stat(input.canonical);
  } catch (cause) {
    throw new WorkspaceInputError(
      `protected input ${input.pathInWorkspace} disappeared during the agent run: ${describeError(cause)}`,
    );
  }
  if (!info.isFile()) {
    throw new WorkspaceInputError(
      `protected input ${input.pathInWorkspace} is no longer a regular file after the agent run`,
    );
  }
  let current: string;
  try {
    current = await sha256File(input.canonical);
  } catch (cause) {
    throw new WorkspaceInputError(
      `cannot re-read protected input ${input.canonical} after the agent run: ${describeError(cause)}`,
    );
  }
  return current;
}

export function executionDocument(params: {
  runId: string;
  stateId: string;
  attempt: number;
  inputPathInWorkspace: string;
  resultPathInWorkspace: string;
  allowedOutcome: string;
  promptContent: string;
}): string {
  return [
    "# Execution document",
    "",
    `- run_id: ${params.runId}`,
    `- state: ${params.stateId}`,
    `- attempt: ${params.attempt}`,
    `- input (workspace-relative): ${params.inputPathInWorkspace}`,
    `- result (workspace-relative): ${params.resultPathInWorkspace}`,
    `- allowed outcome: ${params.allowedOutcome}`,
    "",
    "## Agent instruction",
    "",
    params.promptContent.trimEnd(),
    "",
    "## Required result format",
    "",
    "When the work described above is actually finished, write the result file to",
    "the result path above. The result file must contain a single JSON object with",
    "exactly these fields and no others:",
    "",
    `{"schema_version":1,"run_id":"${params.runId}","status":"${params.allowedOutcome}","summary":"<one sentence describing the work performed>","artifacts":["<workspace-relative paths of files the task created>"]}`,
    "",
    "- schema_version: exactly 1",
    "- run_id: the run_id from this document, verbatim",
    `- status: exactly ${JSON.stringify(params.allowedOutcome)}`,
    "- summary: a non-empty one-sentence description of the work performed",
    "- artifacts: workspace-relative paths of files the task created; never",
    "  absolute paths, never paths outside the workspace, never the input file",
    "",
    "The result file must contain valid JSON and nothing else. If the work cannot",
    "be completed, do not write a conforming result; explain what is missing in",
    "your normal output.",
  ].join("\n");
}

export async function runAgentSmoke(
  options: AgentSmokeOptions,
  deps: AgentSmokeDeps,
): Promise<AgentSmokeOutcome> {
  let plan: OneStepPlan;
  let profile: ResolvedProfile;
  let input: ResolvedWorkspaceInput;
  let identity: PipelineIdentityState;
  let protectedInput: ProtectedInputState;
  try {
    const pipeline = await loadPipeline(options.pipelineRoot);
    plan = planOneStepExecution(pipeline);
    console.error(
      `orchestrator: pipeline ok (${pipeline.states.length} states, entry ${pipeline.entry_state}, agent ${plan.agent.id}, max_transitions ${pipeline.max_transitions})`,
    );
    profile = await loadProfile(options.configRoot, plan.agent.profile, deps.baseEnv ?? {});
    console.error(
      `orchestrator: profile ${profile.profileName} ok (image ${profile.image}, env bindings ${Object.keys(profile.env).length})`,
    );
    input = await resolveWorkspaceInput(options.workspace, plan.input.path);
    console.error(
      `orchestrator: protected input ok (${input.pathInWorkspace}, sha256 ${input.sha256})`,
    );
    identity = {
      schema_version: pipeline.schema_version,
      bundle_root: pipeline.bundleRoot,
      execution_snapshot_sha256: pipelineExecutionDigest(pipeline),
      entry_state: pipeline.entry_state,
      max_transitions: pipeline.max_transitions,
    };
    protectedInput = {
      id: plan.input.id,
      path: input.pathInWorkspace,
      sha256: input.sha256,
    };
  } catch (cause) {
    const failure = cause instanceof Error ? cause : new Error(String(cause));
    console.error(`orchestrator: agent-smoke failed: ${failure.message}`);
    return {
      ok: false,
      exitCode: 1,
      runId: "",
      status: "failed",
      detail: failure.message,
    };
  }

  const lifecycleOptions: LifecycleOptions = {
    workspace: options.workspace,
    workerImage: profile.image,
    launcherId: options.launcherId,
  };

  // `agent-smoke` runs exclusively on the durable pipeline run state sink:
  // no legacy smoke state file is written for this command.
  const makeStateSink: StateSinkFactory = (runId, stateDirPath) =>
    new PipelineRunStateSink({
      stateDirPath,
      runId,
      workspace: input.workspaceCanonical,
      identity,
      protectedInput,
      attempt: {
        stateId: plan.agent.id,
        attempt: plan.attempt,
        profile: profile.profileName,
      },
      io: deps.pipelineStateIo,
      now: deps.now,
    });

  return runWithChildSession(
    deps,
    lifecycleOptions,
    {
      preSession: async (ctx) => {
        await mkdir(agentRunDirPath(options.workspace, ctx.runId), { recursive: true });
        await writeFile(
          executionDocumentPath(options.workspace, ctx.runId),
          executionDocument({
            runId: ctx.runId,
            stateId: plan.agent.id,
            attempt: plan.attempt,
            inputPathInWorkspace: input.pathInWorkspace,
            resultPathInWorkspace: `${AGENT_SMOKE_DIR}/${ctx.runId}/result.json`,
            allowedOutcome: plan.outcome,
            promptContent: plan.agent.promptContent,
          }),
          { encoding: "utf8" },
        );
        console.error(`orchestrator: run dir ${agentRunDirPath(options.workspace, ctx.runId)}`);
      },
      // The graph engine owns the outcome -> transition -> next-state mapping
      // even for the one-step path: the agent callback reports only the
      // validated result status, the engine resolves the declared transition,
      // and success is possible only by reaching a success terminal state.
      // Every resolved transition is durably committed (cursor, transition,
      // and event in one snapshot) before the engine moves on.
      withSession: async (ctx) => {
        const sink = requireTransitionSink(ctx);
        let accepted: AcceptedAgentResultRecord | null = null;
        const execution = await executePipelineGraph(
          plan.pipeline,
          async (state) => {
            const acceptedResult = await agentRunOutcome(options, profile, input, state, deps, ctx);
            accepted = {
              resultSha256: acceptedResult.resultSha256,
              artifacts: acceptedResult.artifacts,
            };
            return acceptedResult.outcome;
          },
          {
            onTransitionCommit: (step) => {
              if (accepted === null) {
                throw new PipelineError(
                  "no accepted agent result is available for the committed transition",
                );
              }
              return sink.recordTransition(step, accepted);
            },
          },
        );
        await sink.recordTerminal(execution.terminalStateId, execution.terminalResult);
        console.error(
          `orchestrator: graph execution terminal ${execution.terminalStateId} (${execution.terminalResult}, ${execution.transitionCount} transition(s))`,
        );
        if (execution.terminalResult !== "success") {
          throw new PipelineError(
            `pipeline execution ended at terminal ${JSON.stringify(execution.terminalStateId)} with result failed`,
          );
        }
      },
    },
    "agent-smoke",
    makeStateSink,
  );
}

function requireTransitionSink(ctx: SessionContext) {
  if (ctx.transitionSink === undefined) {
    throw new PipelineError("agent-smoke requires the pipeline run state sink");
  }
  return ctx.transitionSink;
}

async function agentRunOutcome(
  options: AgentSmokeOptions,
  profile: ResolvedProfile,
  input: ResolvedWorkspaceInput,
  state: AgentStateView,
  deps: AgentSmokeDeps,
  ctx: SessionContext,
): Promise<{ outcome: string; resultSha256: string; artifacts: readonly string[] }> {
  const { runId, updateState, childEnv } = ctx;
  const resultPathInWorkspace = `${AGENT_SMOKE_DIR}/${runId}/result.json`;
  const executionDocPathInWorkspace = `${AGENT_SMOKE_DIR}/${runId}/execution.md`;

  console.error(`orchestrator: pulling agent image ${profile.image}`);
  const pull = await deps.cli(
    pullArgs(profile.image, deps.config.socketPath),
    childEnv,
    "inherit",
  );
  if (pull.code !== 0) {
    console.error(
      `orchestrator: warning: docker-helper pull ${profile.image} failed (exit ${pull.code}); continuing, the image may already be present locally`,
    );
  }

  ctx.checkAbort();

  const spec = agentWorkerSpec({
    runId,
    childSessionToken: ctx.childToken,
    workerImage: profile.image,
    inputPathInWorkspace: input.pathInWorkspace,
    resultPathInWorkspace,
    executionDocPathInWorkspace,
    profileEnv: profile.env,
    opencodeConfigContent: profile.opencodeConfigContent,
  });

  console.error(`orchestrator: starting agent in child session`);
  await updateState("agent_running");
  // after a recorded signal no new worker run may start: check synchronously
  // right before the CLI call, with no await in between
  ctx.checkAbort();
  const run = await deps.cli(runArgs(spec, deps.config.socketPath), childEnv, "inherit", {
    signalOnAbort: true,
    timeoutSeconds: state.timeout_seconds,
  });
  if (run.timedOut === true) {
    throw new AgentTimeoutError(state.timeout_seconds);
  }
  if (run.code !== 0) {
    throw new DockerHelperError(
      "cli_failure",
      `agent container failed (exit ${run.code})`,
    );
  }

  ctx.checkAbort();

  const currentSha256 = await currentInputSha256(input);
  if (currentSha256 !== input.sha256) {
    throw new WorkspaceInputError(
      `protected input ${input.pathInWorkspace} was modified during the agent run (sha256 ${input.sha256} -> ${currentSha256})`,
    );
  }

  const resultFile = agentResultFilePath(options.workspace, runId);
  let bytes: Uint8Array;
  try {
    bytes = await Bun.file(resultFile).bytes();
  } catch (cause) {
    throw new AgentResultError(
      `agent result not readable at ${resultFile}: ${describeError(cause)}`,
    );
  }
  const resultHasher = new Bun.CryptoHasher("sha256");
  resultHasher.update(bytes);
  const resultSha256 = resultHasher.digest("hex");
  const raw = new TextDecoder().decode(bytes);
  const result: AgentResult = await verifyAgentResult(raw, runId, input.workspaceCanonical, {
    canonical: input.canonical,
    dev: input.dev,
    ino: input.ino,
  });

  console.error(`orchestrator: agent result verified for run ${runId}`);
  // the validated outcome only; the graph engine selects the next state
  return { ...result, outcome: result.status, resultSha256 };
}
