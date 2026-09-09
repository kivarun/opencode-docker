import {
  loadPipeline,
  planMultiStateExecution,
  type MultiStatePlan,
} from "./pipeline.ts";
import { pipelineExecutionDigest } from "./pipeline_digest.ts";
import type { PipelineIdentityState, ProtectedInputState } from "./pipeline_state.ts";
import type { PipelineStateIo } from "./pipeline_state_store.ts";
import { PipelineRunStateSink } from "./pipeline_state_sink.ts";
import { loadProfile, type ResolvedProfile } from "./profile.ts";
import {
  canonicalWorkspacePath,
  resolveWorkspaceInput,
  runMultiStatePipeline,
  type AgentSmokeDeps,
  type AgentSmokeOptions,
  type AgentSmokeOutcome,
  type ResolvedProtectedInput,
} from "./pipeline_runner.ts";

export {
  AgentTimeoutError,
  WorkspaceInputError,
  activationDirPath,
  activationExecutionDocPath,
  activationResultFilePath,
  agentRunDirPath,
  canonicalWorkspacePath,
  executionDocument,
  resolveWorkspaceInput,
} from "./pipeline_runner.ts";

export type {
  AgentSmokeDeps,
  AgentSmokeOptions,
  AgentSmokeOutcome,
} from "./pipeline_runner.ts";

/**
 * Runs `agent-smoke`: loads and validates the pipeline, loads every
 * referenced execution profile, and resolves every protected input — all
 * before Launcher authentication and before any child Session. The supported
 * multi-state graph is then executed by the pipeline runner through the
 * graph engine: one child Session per agent-state activation, one durable
 * pipeline run state for the whole pipeline.
 */
export async function runAgentSmoke(
  options: AgentSmokeOptions,
  deps: AgentSmokeDeps,
): Promise<AgentSmokeOutcome> {
  let plan: MultiStatePlan;
  let profiles: ReadonlyMap<string, ResolvedProfile>;
  let workspaceCanonical: string;
  let protectedInputs: ResolvedProtectedInput[];
  let protectedInputStates: ProtectedInputState[];
  let identity: PipelineIdentityState;
  try {
    const pipeline = await loadPipeline(options.pipelineRoot);
    plan = planMultiStateExecution(pipeline);
    console.error(
      `orchestrator: pipeline ok (${pipeline.states.length} states, entry ${pipeline.entry_state}, max_transitions ${pipeline.max_transitions})`,
    );

    const loaded = new Map<string, ResolvedProfile>();
    for (const profileName of plan.profileNames) {
      const profile = await loadProfile(options.configRoot, profileName, deps.baseEnv ?? {});
      loaded.set(profileName, profile);
      console.error(
        `orchestrator: profile ${profile.profileName} ok (image ${profile.image}, env bindings ${Object.keys(profile.env).length})`,
      );
    }
    profiles = loaded;

    workspaceCanonical = await canonicalWorkspacePath(options.workspace);
    protectedInputs = [];
    for (const input of plan.protectedInputs) {
      const resolved = await resolveWorkspaceInput(options.workspace, input.path, workspaceCanonical);
      protectedInputs.push({ ...resolved, id: input.id });
      console.error(
        `orchestrator: protected input ${input.id} ok (${resolved.declaredPath}, target ${resolved.pathInWorkspace}, sha256 ${resolved.sha256})`,
      );
    }
    protectedInputStates = protectedInputs.map((input) => ({
      id: input.id,
      path: input.declaredPath,
      sha256: input.sha256,
    }));
    identity = {
      schema_version: pipeline.schema_version,
      bundle_root: pipeline.bundleRoot,
      execution_snapshot_sha256: pipelineExecutionDigest(pipeline),
      entry_state: pipeline.entry_state,
      max_transitions: pipeline.max_transitions,
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

  // `agent-smoke` runs exclusively on the durable pipeline run state sink:
  // no legacy smoke state file is written for this command.
  const makeStateSink = (runId: string, stateDirPath: string) =>
    new PipelineRunStateSink({
      stateDirPath,
      runId,
      workspace: workspaceCanonical,
      identity,
      protectedInputs: protectedInputStates,
      io: deps.pipelineStateIo,
      now: deps.now,
    });

  return runMultiStatePipeline(
    deps,
    { workspace: options.workspace, launcherId: options.launcherId },
    {
      plan,
      profiles,
      workspaceCanonical,
      protectedInputs,
      makeStateSink,
    },
  );
}
