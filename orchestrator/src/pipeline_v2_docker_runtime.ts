/**
 * Docker Helper 2.1.1 runtime adapter for pipeline schema version 2: a real
 * implementation of the coordinator's two-session `PipelineV2AgentRuntime`
 * boundary on top of the official docker-helper CLI.
 *
 * The adapter is deliberately NOT wired into production: `agent-smoke`, the
 * production CLI, the lifecycle and the default pipeline keep executing v1,
 * and the production loader keeps rejecting pipeline schema version 2
 * before Launcher auth and before any Session. Nothing in this module is
 * reachable from the CLI surface.
 *
 * Two-session capability model: `createExecutionSession` creates the
 * orchestrator-owned Execution Session scoped to the canonical run root
 * (its bearer authorizes every helper CLI call of the activation and is
 * never handed to the worker); `createToolSession` creates the Tool
 * Session scoped to the canonical project directory (its bearer is the
 * worker's only authority). Bearer tokens live only in instance-private
 * session registries; the public session handles carry the session id and
 * lifecycle methods only. Every handle is bound to this runtime instance,
 * the state id, the activation index and the exact `PreparedActivationData`
 * object; a Tool Session is created only for an uncleaned Execution
 * Session of the same activation.
 *
 * The factory captures every contract input exactly once before the first
 * helper or filesystem side effect: the trusted pipeline snapshot, one
 * immutable execution snapshot per agent state (profile name, image,
 * OpenCode config content and the destination-sorted profile env bindings),
 * the CLI runner function, the helper config, the launcher operator
 * environment and the trusted run-root projection
 * `{localRoot, daemonRoot}`. Mutating the source profile map, the profile
 * objects or the projection object during activations cannot change a
 * later activation; user objects are never frozen or modified, and profile
 * secrets never appear on any public object.
 *
 * Daemon-visible run-root projection: the orchestrator works on the
 * canonical run root (`localRoot`); the Docker Helper daemon may see the
 * same directory under a different absolute path (`daemonRoot`, equal to
 * `localRoot` in host mode). Before every `createChildSession` of an
 * activation the adapter proves the projection fail-closed: the
 * activation's canonical run root must equal `localRoot`, both roots must
 * be real non-symlink directories resolving exactly to their declared
 * canonical paths with identical dev/ino, and the project, activation,
 * data, inputs and outputs roots plus the `.orchestrator` directory and
 * the execution document must have same-kind, same-dev/ino pairs under
 * both roots. Any divergence fails closed with a typed
 * `PipelineV2ProjectionError` (stable reason, never classified from
 * message text) before any Session and before any helper CLI side effect.
 * Session workspaces use only `daemonRoot + relative(localRoot, localPath)`
 * translations; mount sources stay workspace-relative, so neither root
 * path ever appears in worker argv, worker environment, the execution
 * document or durable state.
 *
 * Pair provenance: `runAgent(toolSession)` accepts only the exact Tool
 * Session handle created by this runtime for the same activation and the
 * same `PreparedActivationData` object, with both sessions still uncleaned
 * and the worker not yet run. Forged objects, casts, clones, Proxies,
 * handles of another runtime and cross-activation handles are rejected by
 * the registry lookup before any field is read and before any helper CLI
 * call — getters and Proxy traps are never invoked.
 *
 * Worker launch follows the fixed CLI 2.1.1 contract: `pull` and `run`
 * through the Execution Session, `--helper-socket` exactly once, mounts in
 * the fixed order (project RW, activation inputs RO, activation outputs
 * RW) with clean workspace-relative sources verified against the prepared
 * activation before the CLI call, and no secret value in argv — the Tool
 * bearer, every profile env value and `OPENCODE_CONFIG_CONTENT` travel
 * only through `--env-from` from deterministically named private source
 * variables of the run subprocess environment. Known 2.1.1 boundary: while
 * `--env-from` keeps secret values out of the adapter's argv, the legacy
 * daemon-side Docker CLI may still see resolved values in its own argv;
 * that risk is documented, not eliminated here.
 *
 * Every created Session is cleaned through memoized Launcher-authority
 * deletes: the physical delete runs at most once, repeated cleanup calls
 * return the same promise. A known Session whose launcher provenance does
 * not match the expected launcher id is deleted by the adapter itself
 * before the error is thrown, with exactly one delete attempt: when that
 * delete succeeds, the `wrong_authority` failure reports the confirmed
 * cleanup; when the delete itself fails, the `cli_failure` failure names
 * the mismatch, the session id and the explicit "cleanup could not be
 * confirmed" fact — a mismatched session that could not be removed stays
 * observable and is never claimed to be gone.
 *
 * UID/GID boundary: the adapter cannot prove a default image UID through
 * the CLI 2.1.1. The supported contract is the orchestrator image running
 * as `opencode` (UID/GID 1000:1000) and shipped agent images built on
 * `Dockerfile_base` with the same UID/GID; an external profile image is
 * accepted only as an operator-approved image compatible with this
 * filesystem contract. That is a runtime compatibility requirement of the
 * profile, not a verified guarantee — an incompatible image ends as
 * `worker_failed` or `activation_output_invalid`, and the pipeline schema
 * gains no `user`/`uid`/`gid` fields and no arbitrary container user
 * support.
 */
import { isAbsolute, relative } from "node:path";
import { lstat, realpath } from "node:fs/promises";
import {
  DockerHelperError,
  MAX_RUN_TIMEOUT_SECONDS,
  type CliRunner,
} from "./docker_helper.ts";
import {
  createChildSession,
  deleteChildSession,
  type HelperConfig,
} from "./launcher.ts";
import {
  ACTIVATION_INPUTS_ROOT,
  ACTIVATION_OUTPUTS_ROOT,
  PROJECT_MOUNT_TARGET,
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
  type ResolvedV2AgentState,
} from "./pipeline_v2.ts";
import {
  PIPELINE_V2_EXECUTION_DOCUMENT_CONTAINER_PATH,
  requirePreparedActivationForPipeline,
  type PreparedActivationData,
} from "./pipeline_v2_runtime.ts";
import type {
  PipelineV2AgentRuntime,
  PipelineV2ExecutionSession,
  PipelineV2ToolSession,
  PipelineV2WorkerRunResult,
} from "./pipeline_v2_coordinator.ts";
import type { V2AgentExecutionView } from "./pipeline_engine.ts";
import type { ResolvedProfile } from "./profile.ts";
import { OPENCODE_CONFIG_CONTENT_ENV, SESSION_TOKEN_ENV } from "./worker.ts";

export interface DockerHelperPipelineV2RuntimeParams {
  /** The exact deep-frozen snapshot a successful `loadPipelineV2` returned. */
  readonly pipeline: ResolvedPipelineV2;
  /**
   * Every resolved profile of the pipeline, loaded before Launcher auth
   * and before any Session. The factory takes its own snapshot; the map
   * and the profile objects are never re-read during activations.
   */
  readonly profiles: ReadonlyMap<string, ResolvedProfile>;
  /** The existing docker-helper CLI runner function. */
  readonly cli: CliRunner;
  readonly helperConfig: HelperConfig;
  /** The minimal launcher operator environment for session create/delete. */
  readonly operatorEnv: Readonly<Record<string, string>>;
  /** The launcher id learned from `/auth`, when it is known. */
  readonly expectedLauncherId?: string;
  /**
   * Trusted runtime configuration mapping the orchestrator-side canonical
   * run root to the exact absolute path by which the Docker Helper daemon
   * sees the same directory. `localRoot` is the canonical run root the
   * data plane works on; `daemonRoot` is the same directory by its
   * host-visible absolute path (host-mode deployments pass the same
   * string for both). The projection is captured at factory creation and
   * is never part of the pipeline, profile, execution document, worker
   * environment or durable state.
   */
  readonly runRootProjection: {
    readonly localRoot: string;
    readonly daemonRoot: string;
  };
}

/** Stable fail-closed reasons of the runtime projection contract. */
export type PipelineV2ProjectionFailureReason =
  | "local_root_mismatch"
  | "projection_root_invalid"
  | "projection_pair_mismatch"
  | "projection_suffix_unclean";

/**
 * Typed runtime-contract failure for an unprovable run-root projection.
 * Carries a stable machine-readable reason; never classified from the
 * message text.
 */
export class PipelineV2ProjectionError extends Error {
  readonly reason: PipelineV2ProjectionFailureReason;

  constructor(reason: PipelineV2ProjectionFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2ProjectionError";
    this.reason = reason;
  }
}

/** Private source env variable carrying the Tool Session bearer. */
export const PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE = "ORCHESTRATOR_V2_TOOL_SESSION_TOKEN";

/** Private source env variable carrying the OpenCode config content. */
export const PIPELINE_V2_OPENCODE_CONFIG_SOURCE = "ORCHESTRATOR_V2_OPENCODE_CONFIG_CONTENT";

const PROFILE_SOURCE_PREFIX = "ORCHESTRATOR_V2_PROFILE_";

const WORKER_ENTRYPOINT = "opencode";
const WORKER_WORKDIR = "/workspace";

/** The static worker instruction; the prompt body travels in the document. */
export function workerInstruction(): string {
  return `Read ${PIPELINE_V2_EXECUTION_DOCUMENT_CONTAINER_PATH} and follow it exactly.`;
}

function profileSourceName(destination: string): string {
  return `${PROFILE_SOURCE_PREFIX}${destination}`;
}

/**
 * Immutable execution snapshot of one agent state's profile: taken once at
 * factory time, never re-read from the source map or the profile objects.
 */
interface ProfileExecutionData {
  readonly name: string;
  readonly image: string;
  readonly opencodeConfigContent: string;
  /** Profile env values in stable (destination-sorted) order. */
  readonly envEntries: readonly {
    readonly destination: string;
    readonly value: string;
  }[];
}

interface SessionRecord {
  readonly kind: "execution" | "tool";
  readonly sessionId: string;
  readonly stateId: string;
  readonly activationIndex: number;
  readonly prepared: PreparedActivationData;
  /** The Session bearer; never exposed on the public handle. */
  readonly token: string;
  /** The exact Execution Session record of the same activation (tool only). */
  readonly execution: SessionRecord | null;
  cleaned: boolean;
  cleanupPromise: Promise<void> | null;
  ran: boolean;
}

function requireNonEmptyString(value: unknown, what: string): string {
  if (typeof value !== "string" || value === "") {
    throw new Error(`${what} must be a non-empty string`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * One clean workspace-relative mount source computed from the canonical
 * Execution Session root; absolute paths, empty components, `.`, `..` and
 * any non-canonical shape are rejected before the CLI is called.
 */
function cleanRelativeSource(
  fromRoot: string,
  absolute: string,
  what: string,
): string {
  const relativePath = relative(fromRoot, absolute);
  if (relativePath === "" || isAbsolute(relativePath)) {
    throw new Error(
      `${what} ${absolute} is not a clean workspace-relative path under ${fromRoot}`,
    );
  }
  for (const component of relativePath.split("/")) {
    if (component === "" || component === "." || component === "..") {
      throw new Error(
        `${what} ${absolute} is not a clean workspace-relative path under ${fromRoot}`,
      );
    }
  }
  return relativePath;
}

function findAgentState(
  pipeline: ResolvedPipelineV2,
  stateId: string,
): ResolvedV2AgentState {
  for (const candidate of pipeline.states) {
    if (candidate.id === stateId && candidate.type === "agent") {
      return candidate;
    }
  }
  throw new Error(
    `pipeline v2 agent runtime contract violated: state ${JSON.stringify(stateId)} is not a declared agent state of the trusted pipeline`,
  );
}

/**
 * Create the Docker Helper 2.1.1 runtime adapter for one pipeline run.
 *
 * Every factory validation happens before the first helper or filesystem
 * side effect: a missing profile for any agent state rejects the factory
 * outright, and the per-state execution snapshots make later mutations of
 * the profile map or the profile objects invisible to activations.
 */
export function createDockerHelperPipelineV2Runtime(
  params: DockerHelperPipelineV2RuntimeParams,
): PipelineV2AgentRuntime {
  requireResolvedPipelineV2Provenance(params.pipeline, "docker helper pipeline v2 runtime factory");

  // The projection is trusted runtime configuration: its values are
  // captured now, so mutating the caller's object after factory creation
  // cannot influence execution.
  const projectionLocalRoot = requireNonEmptyString(
    params.runRootProjection?.localRoot,
    "run root projection localRoot",
  );
  const projectionDaemonRoot = requireNonEmptyString(
    params.runRootProjection?.daemonRoot,
    "run root projection daemonRoot",
  );
  if (
    !isAbsolute(projectionLocalRoot) ||
    !isAbsolute(projectionDaemonRoot) ||
    projectionLocalRoot !== projectionLocalRoot.trim() ||
    projectionDaemonRoot !== projectionDaemonRoot.trim()
  ) {
    throw new Error(
      "pipeline v2 agent runtime factory: run root projection roots must be absolute clean paths",
    );
  }
  const projection = { localRoot: projectionLocalRoot, daemonRoot: projectionDaemonRoot };

  const profileMap = params.profiles;
  if (profileMap === null || typeof profileMap !== "object" || typeof profileMap.get !== "function") {
    throw new Error("pipeline v2 agent runtime factory requires a profile map");
  }

  // One immutable execution snapshot per agent state, built before any
  // side effect. Profile env entries are sorted by destination so the
  // env-from argument order is stable across activations.
  const executionData = new Map<string, ProfileExecutionData>();
  for (const state of params.pipeline.states) {
    if (state.type !== "agent") {
      continue;
    }
    const profile = profileMap.get(state.profile);
    if (profile === undefined) {
      throw new Error(
        `pipeline v2 agent runtime factory: agent state ${JSON.stringify(state.id)} requires profile ${JSON.stringify(state.profile)} which is not loaded`,
      );
    }
    if (profile.profileName !== state.profile) {
      throw new Error(
        `pipeline v2 agent runtime factory: profile named ${JSON.stringify(profile.profileName)} cannot serve state ${JSON.stringify(state.id)} expecting profile ${JSON.stringify(state.profile)}`,
      );
    }
    requireNonEmptyString(profile.image, `profile ${JSON.stringify(state.profile)} image`);
    if (typeof profile.opencodeConfigContent !== "string") {
      throw new Error(
        `profile ${JSON.stringify(state.profile)} OpenCode config content must be a string`,
      );
    }
    if (!isRecord(profile.env)) {
      throw new Error(
        `profile ${JSON.stringify(state.profile)} env must be a mapping of destination names to values`,
      );
    }
    const envEntries = Object.entries(profile.env)
      .map(([destination, value]) => ({
        destination,
        value: requireNonEmptyString(
          value,
          `profile ${JSON.stringify(state.profile)} env ${JSON.stringify(destination)}`,
        ),
      }))
      .sort((left, right) =>
        left.destination < right.destination
          ? -1
          : left.destination > right.destination
            ? 1
            : 0,
      );
    executionData.set(state.id, {
      name: state.profile,
      image: profile.image,
      opencodeConfigContent: profile.opencodeConfigContent,
      envEntries,
    });
  }

  const cli = params.cli;
  if (typeof cli !== "function") {
    throw new Error("pipeline v2 agent runtime factory requires a CLI runner function");
  }

  if (!isRecord(params.helperConfig)) {
    throw new Error("pipeline v2 agent runtime factory requires a helper config");
  }
  const helperConfig: HelperConfig = {
    socketPath: requireNonEmptyString(params.helperConfig.socketPath, "helper config socketPath"),
    credentialFile: requireNonEmptyString(
      params.helperConfig.credentialFile,
      "helper config credentialFile",
    ),
  };

  if (!isRecord(params.operatorEnv)) {
    throw new Error("pipeline v2 agent runtime factory requires an operator environment record");
  }
  // A frozen own-value copy: later operator-env mutations cannot leak into
  // session calls, and the record carries only the minimal launcher env.
  const operatorEnv = Object.freeze({ ...params.operatorEnv });

  const expectedLauncherId =
    params.expectedLauncherId === undefined
      ? undefined
      : requireNonEmptyString(params.expectedLauncherId, "expected launcher id");

  // Instance-private session registry: bearer tokens and the exact
  // provenance bindings (state, activation, prepared object, execution
  // counterpart) of every created Session live only here. Another runtime
  // instance has its own registry, so its handles are unregistered here
  // and rejected before any field is read.
  const sessionRegistry = new WeakMap<object, SessionRecord>();
  const records: SessionRecord[] = [];

  const cleanupSession = (record: SessionRecord): Promise<void> => {
    if (record.cleanupPromise !== null) {
      return record.cleanupPromise;
    }
    record.cleaned = true;
    const cleanupPromise = deleteChildSession(
      cli,
      helperConfig,
      record.sessionId,
      { ...operatorEnv },
    );
    record.cleanupPromise = cleanupPromise;
    return cleanupPromise;
  };

  const projectionFailure = (
    reason: PipelineV2ProjectionFailureReason,
    what: string,
    detail: string,
  ): PipelineV2ProjectionError =>
    new PipelineV2ProjectionError(
      reason,
      `pipeline v2 runtime projection contract violated: ${what} ${detail}`,
    );

  /**
   * One root of the projection pair must exist as a real non-symlink
   * directory that resolves exactly to the declared canonical path.
   */
  const verifyProjectionRoot = async (
    what: string,
    path: string,
  ): Promise<{ dev: number; ino: number }> => {
    let info;
    try {
      info = await lstat(path);
    } catch {
      throw projectionFailure("projection_root_invalid", what, `is missing at ${JSON.stringify(path)}`);
    }
    if (info.isSymbolicLink() || !info.isDirectory()) {
      throw projectionFailure(
        "projection_root_invalid",
        what,
        `is not a real non-symlink directory at ${JSON.stringify(path)}`,
      );
    }
    let resolved = "";
    try {
      resolved = await realpath(path);
    } catch {
      throw projectionFailure("projection_root_invalid", what, `cannot be resolved at ${JSON.stringify(path)}`);
    }
    if (resolved !== path) {
      throw projectionFailure(
        "projection_root_invalid",
        what,
        `resolves to ${JSON.stringify(resolved)} instead of the declared canonical path ${JSON.stringify(path)}`,
      );
    }
    return { dev: info.dev, ino: info.ino };
  };

  /**
   * Translate one orchestrator-side canonical path into the daemon
   * namespace: strictly `daemonRoot + relative(localRoot, localPath)`.
   * The relative suffix must be clean — non-empty segments only, no `.`
   * or `..`. Arbitrary per-path mappings do not exist.
   */
  const daemonPathFor = (what: string, localPath: string): string => {
    const suffix = relative(projection.localRoot, localPath);
    if (suffix === "") {
      return projection.daemonRoot;
    }
    const segments = suffix.split("/");
    if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
      throw projectionFailure(
        "projection_suffix_unclean",
        what,
        `relative suffix ${JSON.stringify(suffix)} is not clean`,
      );
    }
    return `${projection.daemonRoot}/${suffix}`;
  };

  /**
   * One projection pair (the same object under both roots) must match by
   * object kind and dev/ino.
   */
  const verifyProjectionPair = async (
    what: string,
    localPath: string,
  ): Promise<void> => {
    const daemonPath = daemonPathFor(what, localPath);
    let localInfo;
    try {
      localInfo = await lstat(localPath);
    } catch {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `local path ${JSON.stringify(localPath)} is missing`,
      );
    }
    let daemonInfo;
    try {
      daemonInfo = await lstat(daemonPath);
    } catch {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `daemon path ${JSON.stringify(daemonPath)} is missing`,
      );
    }
    if (localInfo.isSymbolicLink() !== daemonInfo.isSymbolicLink()) {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `object kind differs between ${JSON.stringify(localPath)} and ${JSON.stringify(daemonPath)}`,
      );
    }
    if (localInfo.isDirectory() !== daemonInfo.isDirectory()) {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `object kind differs between ${JSON.stringify(localPath)} and ${JSON.stringify(daemonPath)}`,
      );
    }
    if (localInfo.isFile() !== daemonInfo.isFile()) {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `object kind differs between ${JSON.stringify(localPath)} and ${JSON.stringify(daemonPath)}`,
      );
    }
    if (localInfo.dev !== daemonInfo.dev || localInfo.ino !== daemonInfo.ino) {
      throw projectionFailure(
        "projection_pair_mismatch",
        what,
        `dev/ino differ between ${JSON.stringify(localPath)} and ${JSON.stringify(daemonPath)}`,
      );
    }
  };

  /**
   * Prove the daemon-visible run-root projection for one prepared
   * activation before any Session is created: the activation's canonical
   * run root must equal the projection's local root, both roots must be
   * real non-symlink directories resolving to their declared canonical
   * paths with identical dev/ino, and every orchestrator-side object the
   * sessions and mounts depend on must have a matching daemon-side pair.
   * Any divergence fails closed before any helper CLI side effect.
   */
  const requireActivationProjection = async (
    activation: PreparedActivationData,
  ): Promise<void> => {
    if (activation.run_root !== projection.localRoot) {
      throw projectionFailure(
        "local_root_mismatch",
        "activation run root",
        `${JSON.stringify(activation.run_root)} is not the projection local root ${JSON.stringify(projection.localRoot)}`,
      );
    }
    const localRootInfo = await verifyProjectionRoot("localRoot", projection.localRoot);
    const daemonRootInfo = await verifyProjectionRoot("daemonRoot", projection.daemonRoot);
    if (localRootInfo.dev !== daemonRootInfo.dev || localRootInfo.ino !== daemonRootInfo.ino) {
      throw projectionFailure(
        "projection_root_invalid",
        "run root projection",
        `dev/ino differ between localRoot ${JSON.stringify(projection.localRoot)} and daemonRoot ${JSON.stringify(projection.daemonRoot)}`,
      );
    }
    const executionDocumentDir = activation.execution_document.host_path.slice(
      0,
      activation.execution_document.host_path.lastIndexOf("/"),
    );
    const pairs: readonly (readonly [string, string])[] = [
      ["project root", activation.project_root],
      ["activation root", activation.activation_root],
      ["data root", activation.data_root],
      ["inputs root", activation.inputs_root],
      ["outputs root", activation.outputs_root],
      [".orchestrator directory", executionDocumentDir],
      ["execution document", activation.execution_document.host_path],
    ];
    for (const [what, localPath] of pairs) {
      await verifyProjectionPair(what, localPath);
    }
  };

  /**
   * Create one child Session through the official CLI, verify its launcher
   * provenance against the expected launcher id (a mismatched known
   * Session is deleted by the adapter itself with exactly one delete
   * attempt before the error is thrown: a confirmed delete reports
   * `wrong_authority` with the confirmed cleanup, a failed delete reports
   * `cli_failure` with "cleanup could not be confirmed" and never claims
   * the session is gone), and build the instance-private record. The Tool
   * session is linked to the exact Execution Session record of the same
   * activation.
   */
  const createSessionRecord = async (
    kind: "execution" | "tool",
    activation: PreparedActivationData,
    executionRecord: SessionRecord | null,
    workspace: string,
  ): Promise<SessionRecord> => {
    const created = await createChildSession(
      cli,
      helperConfig,
      requireNonEmptyString(workspace, "session workspace"),
      { ...operatorEnv },
    );

    // Launcher ownership: a known Session whose provenance does not match
    // the expected launcher is deleted by the adapter itself before the
    // error is thrown — exactly one delete attempt. When the delete
    // succeeds, the wrong_authority failure reports the confirmed cleanup.
    // When the delete itself fails, the failure is never swallowed: the
    // cli_failure error names the mismatch, the session id and the explicit
    // "cleanup could not be confirmed" fact, so a mismatched session that
    // could not be removed stays observable instead of being claimed gone.
    if (expectedLauncherId !== undefined && created.launcherId !== expectedLauncherId) {
      let cleanupConfirmed = false;
      let cleanupFailureMessage = "";
      try {
        await deleteChildSession(
          cli,
          helperConfig,
          created.sessionId,
          { ...operatorEnv },
        );
        cleanupConfirmed = true;
      } catch (cause) {
        cleanupFailureMessage = cause instanceof Error ? cause.message : String(cause);
      }
      if (cleanupConfirmed) {
        throw new DockerHelperError(
          "wrong_authority",
          `docker-helper session create: session ${created.sessionId} belongs to launcher ${created.launcherId ?? "unknown"}, expected ${expectedLauncherId}; the known session was deleted (cleanup confirmed)`,
        );
      }
      throw new DockerHelperError(
        "cli_failure",
        `docker-helper session create: session ${created.sessionId} belongs to launcher ${created.launcherId ?? "unknown"}, expected ${expectedLauncherId}; cleanup could not be confirmed: ${cleanupFailureMessage}`,
      );
    }

    const record: SessionRecord = {
      kind,
      sessionId: created.sessionId,
      stateId: activation.state_id,
      activationIndex: activation.activation_index,
      prepared: activation,
      token: requireNonEmptyString(created.token, "docker-helper session token"),
      execution: executionRecord,
      cleaned: false,
      cleanupPromise: null,
      ran: false,
    };
    return record;
  };

  /**
   * The worker run behind one Execution Session. The pair provenance gate
   * runs entirely on the registry records — the argument object's fields,
   * getters and Proxy traps are never touched before the gate passes.
   */
  const runWorker = async (
    executionRecord: SessionRecord,
    executionHandle: object,
    toolSession: PipelineV2ToolSession,
  ): Promise<PipelineV2WorkerRunResult> => {
    const toolRecord = sessionRegistry.get(toolSession as object);
    if (toolRecord === undefined) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: runAgent requires the exact Tool Session handle created by this runtime",
      );
    }
    const executionSelfCheck = sessionRegistry.get(executionHandle);
    if (executionSelfCheck !== executionRecord) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the Execution Session handle is not owned by this runtime",
      );
    }
    if (toolRecord.kind !== "tool" || toolRecord.execution !== executionRecord) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the Tool Session does not belong to this Execution Session",
      );
    }
    if (
      toolRecord.stateId !== executionRecord.stateId ||
      toolRecord.activationIndex !== executionRecord.activationIndex ||
      toolRecord.prepared !== executionRecord.prepared
    ) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the Tool Session belongs to another activation",
      );
    }
    if (toolRecord.cleaned || executionRecord.cleaned) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the sessions were already cleaned",
      );
    }
    if (executionRecord.ran) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the Execution Session already ran the worker",
      );
    }
    executionRecord.ran = true;

    const profileData = executionData.get(executionRecord.stateId);
    if (profileData === undefined) {
      throw new Error(
        `pipeline v2 agent runtime contract violated: state ${JSON.stringify(executionRecord.stateId)} has no profile execution snapshot`,
      );
    }
    const agentState = findAgentState(params.pipeline, executionRecord.stateId);
    if (
      !Number.isSafeInteger(agentState.timeout_seconds) ||
      agentState.timeout_seconds <= 0 ||
      agentState.timeout_seconds > MAX_RUN_TIMEOUT_SECONDS
    ) {
      throw new Error(
        `pipeline v2 agent runtime contract violated: state ${JSON.stringify(executionRecord.stateId)} timeout_seconds is outside the single-timer bound`,
      );
    }

    // The image pull goes through the Execution Session; a failed pull is
    // a worker failure and never falls back to a potentially stale cached
    // image.
    const pullResult = await cli(
      ["pull", "--endpoint", helperConfig.socketPath, profileData.image],
      { [SESSION_TOKEN_ENV]: executionRecord.token },
      "inherit",
    );
    if (pullResult.code !== 0) {
      return { status: "failed", reason: "worker_failed" };
    }

    // Mount sources: clean workspace-relative paths computed from the
    // canonical Execution Session root and cross-checked against the exact
    // structural composition of the prepared activation, before any CLI
    // call.
    const runRoot = executionRecord.prepared.run_root;
    const projectSource = cleanRelativeSource(
      runRoot,
      executionRecord.prepared.project_root,
      "the prepared project mount source",
    );
    if (projectSource !== "project") {
      throw new Error(
        `the prepared project mount source ${projectSource} does not match the canonical run project directory`,
      );
    }
    const activationRelative = cleanRelativeSource(
      runRoot,
      executionRecord.prepared.activation_root,
      "the prepared activation mount source",
    );
    const expectedActivationRelative = `activations/${executionRecord.activationIndex}-${executionRecord.stateId}`;
    if (activationRelative !== expectedActivationRelative) {
      throw new Error(
        `the prepared activation mount source ${activationRelative} does not match the prepared activation ${expectedActivationRelative}`,
      );
    }
    const inputsSource = cleanRelativeSource(
      runRoot,
      executionRecord.prepared.inputs_root,
      "the prepared inputs mount source",
    );
    if (inputsSource !== `${expectedActivationRelative}/data/inputs`) {
      throw new Error(
        `the prepared inputs mount source ${inputsSource} does not match the prepared activation inputs`,
      );
    }
    const outputsSource = cleanRelativeSource(
      runRoot,
      executionRecord.prepared.outputs_root,
      "the prepared outputs mount source",
    );
    if (outputsSource !== `${expectedActivationRelative}/data/outputs`) {
      throw new Error(
        `the prepared outputs mount source ${outputsSource} does not match the prepared activation outputs`,
      );
    }

    // No secret value in argv: the Tool bearer, the OpenCode config
    // content and every profile env value travel only through --env-from
    // from deterministic private source variables of the run subprocess
    // environment.
    const args: string[] = [
      "run",
      "--endpoint",
      helperConfig.socketPath,
      "--image",
      profileData.image,
      "--entrypoint",
      WORKER_ENTRYPOINT,
      "--workdir",
      WORKER_WORKDIR,
      "--helper-socket",
      "--mount",
      `${projectSource}:${PROJECT_MOUNT_TARGET}`,
      "--mount",
      `${inputsSource}:${ACTIVATION_INPUTS_ROOT}:ro`,
      "--mount",
      `${outputsSource}:${ACTIVATION_OUTPUTS_ROOT}`,
      "--env-from",
      `${SESSION_TOKEN_ENV}=${PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE}`,
      "--env-from",
      `${OPENCODE_CONFIG_CONTENT_ENV}=${PIPELINE_V2_OPENCODE_CONFIG_SOURCE}`,
    ];
    for (const entry of profileData.envEntries) {
      args.push("--env-from", `${entry.destination}=${profileSourceName(entry.destination)}`);
    }
    args.push(
      "--",
      "run",
      "--format",
      "json",
      "--auto",
      `Read ${PIPELINE_V2_EXECUTION_DOCUMENT_CONTAINER_PATH} and follow it exactly.`,
    );

    // The run subprocess environment: the Execution bearer for the CLI
    // authority, the private source variables referenced by --env-from,
    // and nothing else.
    const runEnv: Record<string, string> = {
      [SESSION_TOKEN_ENV]: executionRecord.token,
      [PIPELINE_V2_TOOL_SESSION_TOKEN_SOURCE]: toolRecord.token,
      [PIPELINE_V2_OPENCODE_CONFIG_SOURCE]: profileData.opencodeConfigContent,
    };
    for (const entry of profileData.envEntries) {
      runEnv[profileSourceName(entry.destination)] = entry.value;
    }

    const runResult = await cli(args, runEnv, "inherit", {
      signalOnAbort: true,
      timeoutSeconds: agentState.timeout_seconds,
    });
    if (runResult.timedOut === true) {
      return { status: "failed", reason: "worker_timeout" };
    }
    if (runResult.code !== 0) {
      return { status: "failed", reason: "worker_failed" };
    }
    return { status: "completed" };
  };

  const createExecutionSession = async (
    state: V2AgentExecutionView,
    activation: PreparedActivationData,
  ): Promise<PipelineV2ExecutionSession> => {
    // Provenance gate before any field of the activation is read.
    requirePreparedActivationForPipeline(activation, params.pipeline);
    const stateId = requireNonEmptyString(state.id, "agent execution view state id");
    if (stateId !== activation.state_id) {
      throw new Error(
        `pipeline v2 agent runtime contract violated: execution view names state ${JSON.stringify(stateId)} but the prepared activation belongs to state ${JSON.stringify(activation.state_id)}`,
      );
    }
    const agentState = findAgentState(params.pipeline, stateId);
    if (executionData.get(stateId) === undefined) {
      throw new Error(
        `pipeline v2 agent runtime contract violated: state ${JSON.stringify(stateId)} has no profile execution snapshot`,
      );
    }

    // The daemon-visible projection of the run root is proven before the
    // first Session of the activation and before any helper CLI side
    // effect; the Execution Session workspace is the daemon-visible
    // correspondence of the canonical run root.
    await requireActivationProjection(activation);
    const record = await createSessionRecord(
      "execution",
      activation,
      null,
      projection.daemonRoot,
    );
    let handle: PipelineV2ExecutionSession | null = null;
    handle = Object.freeze({
      sessionId: record.sessionId,
      runAgent: async (
        toolSession: PipelineV2ToolSession,
      ): Promise<PipelineV2WorkerRunResult> => {
        if (handle === null) {
          throw new Error(
            "pipeline v2 agent runtime contract violated: the Execution Session is not initialized",
          );
        }
        return runWorker(record, handle, toolSession);
      },
      cleanup: async (): Promise<void> => {
        await cleanupSession(record);
      },
    });
    sessionRegistry.set(handle, record);
    records.push(record);
    return handle;
  };

  const createToolSession = async (
    state: V2AgentExecutionView,
    activation: PreparedActivationData,
  ): Promise<PipelineV2ToolSession> => {
    // Provenance gate before any field of the activation is read.
    requirePreparedActivationForPipeline(activation, params.pipeline);
    const stateId = requireNonEmptyString(state.id, "agent execution view state id");
    if (stateId !== activation.state_id) {
      throw new Error(
        `pipeline v2 agent runtime contract violated: execution view names state ${JSON.stringify(stateId)} but the prepared activation belongs to state ${JSON.stringify(activation.state_id)}`,
      );
    }
    findAgentState(params.pipeline, stateId);

    // The Execution Session of the same activation must exist in this
    // runtime, be bound to the same prepared object and still be
    // uncleaned before the Tool session is created.
    let executionRecord: SessionRecord | null = null;
    for (const candidate of records) {
      if (
        candidate.kind === "execution" &&
        candidate.stateId === activation.state_id &&
        candidate.activationIndex === activation.activation_index &&
        candidate.prepared === activation &&
        !candidate.cleaned
      ) {
        executionRecord = candidate;
        break;
      }
    }
    if (executionRecord === null) {
      throw new Error(
        "pipeline v2 agent runtime contract violated: the Tool Session requires the uncleaned Execution Session of the same activation created by this runtime",
      );
    }

    // The projection is re-proven before the Tool Session as well: every
    // createChildSession runs after a fail-closed verification, and the
    // Tool Session workspace is the daemon-visible correspondence of the
    // canonical project root.
    await requireActivationProjection(activation);
    const record = await createSessionRecord(
      "tool",
      activation,
      executionRecord,
      daemonPathFor("project root", activation.project_root),
    );
    const handle: PipelineV2ToolSession = Object.freeze({
      sessionId: record.sessionId,
      cleanup: async (): Promise<void> => {
        await cleanupSession(record);
      },
    });
    sessionRegistry.set(handle, record);
    records.push(record);
    return handle;
  };

  return {
    createExecutionSession,
    createToolSession,
  };
}
