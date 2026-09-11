/**
 * Production runner for pipeline schema version 2: the first
 * production-shaped contour that assembles the already implemented v2
 * components — the v2 loader, the execution profiles, the shared Launcher
 * authority, the durable run-state sink, the Docker Helper runtime adapter
 * and the coordinator — behind one testable API. The CLI, `main.ts`,
 * `agent-smoke` and the default pipeline stay on v1 and remain untouched;
 * nothing here is reachable from the command surface yet.
 *
 * Preflight order (all before the first Session and before the run root):
 * runner options/deps shape validation without reading any secret value,
 * `loadPipelineV2`, one profile load per unique agent-state profile name in
 * declaration order through the existing `loadProfile`, and only then the
 * single Launcher authority check (`lifecycleAuthority` — one `/auth`
 * round trip per run) with the expected launcher id. A broken pipeline,
 * profile or credential creates no run root, no state document, no Session
 * and never calls the coordinator or the runtime factory; the outcome is
 * `ok:false`/`exitCode:1`/`state:null`/`runRoot:null` with no secret
 * values in any diagnostic.
 *
 * State-root and run-root ownership: the runner owns the layout
 * `<state-root>/pipeline-runs/<run-id>/`. That directory is at once the
 * coordinator/data-plane `runRoot`, the run directory of the existing
 * `PipelineV2RunStateSink` (so `state.json` lives inside the run root) and
 * the local root of the trusted run-root projection handed to the Docker
 * runtime adapter. The daemon-visible path is computed only as
 * `<daemon-state-root>/pipeline-runs/<run-id>` — no arbitrary per-path
 * mappings exist. Before anything is created, both state roots must be
 * absolute canonical real non-symlink directories that are the same object
 * by dev/ino; `pipeline-runs` is created 0700 when missing, or accepted
 * only as an existing real non-symlink directory with exactly mode 0700
 * and identical dev/ino on both sides — an unsafe existing directory is
 * rejected unchanged (no `chmod`, no run leaf, no Session, no state); the
 * mode is verified explicitly for created directories too, never trusted
 * to the current umask; the run directory itself is created only by an
 * exclusive 0700 `mkdir`, verified to carry exactly 0700 — a pre-existing
 * file, directory or symlink with that run id is rejected without being
 * overwritten; after the exclusive creation the daemon-side run directory
 * must exist as the same real canonical object. Symlinked parents, kind
 * mismatches and projection mismatches are rejected before the sink, the
 * runtime or the coordinator is created.
 *
 * Failure boundaries follow the run-root creation. Before the exclusive
 * run-leaf creation nothing is bound yet: every failure (and every signal)
 * yields the generic preflight outcome `runId:""`/`runRoot:null` and
 * nothing is left behind. From the successful exclusive creation on, the
 * run root exists and is never removed automatically — it is the
 * diagnostic directory of the run — so every later failure reports the
 * actual run id, the canonical run root and the last authoritative sink
 * snapshot (`state:null` until `create_run` commits); a signal accepted
 * after the creation yields the same boundary shape with its signal exit
 * code.
 *
 * Signal semantics are shared with v1 through the same `RunCauseGate`: the
 * runner accepts the `onSignal` seam only (forwarding a signal to a
 * running worker CLI process stays the caller's job via
 * `SubprocessCliRunner.killActive`, exactly as v1 `main.ts` wires it). The
 * gate is constructed only after the protected preflight validated the
 * options/deps shape and captured `onSignal`, so a hostile contract can
 * neither escape as a rejected promise nor register a handler first. The
 * gate is handed to the coordinator as the required synchronous control
 * boundary; the coordinator's checkpoints and cutoff own the
 * acceptance/close ordering, and the runner maps the durable outcome onto
 * the exit code: `signal_sigint` → 130, `signal_sigterm` → 143, a durably
 * failed session cleanup or any other failure → 1, confirmed success → 0.
 * A signal accepted before the run root exists yields no run root and no
 * state; a signal accepted after `create_run` is finalized durably by the
 * coordinator; a signal after the coordinator's cutoff cannot change the
 * persisted outcome or the exit code.
 *
 * The result is deep-frozen and content-free: no bearer, no environment
 * value, no prompt or input body, no worker output and no raw decision
 * facts ever appear on it.
 */
import { lstat, mkdir } from "node:fs/promises";
import { isCleanAbsolutePath } from "./clean_path.ts";
import { describeError, type AuthFetcher, type CliRunner } from "./docker_helper.ts";
import {
  RunCauseGate,
  SignalAbort,
  signalExitCode,
  lifecycleAuthority,
} from "./lifecycle.ts";
import type { HelperConfig } from "./launcher.ts";
import { loadProfile, type ResolvedProfile } from "./profile.ts";
import {
  expectSafeId,
  type PipelineV2FailureReason,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import { PipelineV2RunStateSink } from "./pipeline_v2_state_sink.ts";
import {
  coordinatePipelineV2Run,
  type PipelineV2CoordinatorControl,
} from "./pipeline_v2_coordinator.ts";
import {
  createDockerHelperPipelineV2Runtime,
  type DockerHelperPipelineV2RuntimeParams,
} from "./pipeline_v2_docker_runtime.ts";
import type { RunInputBinding } from "./pipeline_v2_runtime.ts";
import {
  inspectProjectionObject,
  sameProjectionIdentity,
  translateProjectionPath,
} from "./projection_fs.ts";

export interface PipelineV2RunOptions {
  readonly pipelineRoot: string;
  readonly configRoot: string;
  readonly projectSourcePath: string;
  readonly inputBindings: readonly RunInputBinding[];
  readonly launcherId?: string;
}

/**
 * Trusted runtime configuration mapping the orchestrator-side state root
 * to the exact absolute path by which the Docker Helper daemon sees the
 * same directory. It is never part of the pipeline, the profiles, the
 * durable state or any user-facing document.
 */
export interface PipelineV2StateRootProjection {
  readonly localRoot: string;
  readonly daemonRoot: string;
}

export interface PipelineV2RunnerDeps {
  readonly cli: CliRunner;
  readonly fetchAuth: AuthFetcher;
  readonly helperConfig: HelperConfig;
  readonly baseEnv: Readonly<Record<string, string | undefined>>;
  readonly stateRootProjection: PipelineV2StateRootProjection;
  readonly onSignal?: (handler: (signal: "SIGINT" | "SIGTERM") => void) => void;
  readonly now?: () => Date;
  readonly randomId?: () => string;
}

export interface PipelineV2RunOutcome {
  readonly ok: boolean;
  readonly exitCode: number;
  readonly runId: string;
  /** The canonical local run root once it was exclusively created. */
  readonly runRoot: string | null;
  readonly state: PipelineV2RunState | null;
  /**
   * The durable failure reason; omitted for preflight failures (no state
   * document exists whose failure reason could be reported) and for
   * confirmed success.
   */
  readonly reason?: PipelineV2FailureReason;
}

function isNonEmptyAbsolutePath(value: unknown): value is string {
  return typeof value === "string" && value !== "" && value.startsWith("/");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Shape-only validation of the runner contract. It reads no secret value:
 * environment maps, profile bodies and binding paths are inspected only by
 * name/kind, never by value.
 */
function validateRunnerContract(
  options: PipelineV2RunOptions,
  deps: PipelineV2RunnerDeps,
): void {
  if (!isRecord(options)) {
    throw new Error("pipeline v2 runner options must be an object");
  }
  for (const field of ["pipelineRoot", "configRoot", "projectSourcePath"] as const) {
    if (!isNonEmptyAbsolutePath(options[field])) {
      throw new Error(`pipeline v2 runner options.${field} must be a non-empty absolute path`);
    }
  }
  if (!Array.isArray(options.inputBindings)) {
    throw new Error("pipeline v2 runner options.inputBindings must be an array");
  }
  if (options.launcherId !== undefined && !options.launcherId.startsWith("dhl_")) {
    throw new Error("pipeline v2 runner options.launcherId must be a launcher ID (dhl_...)");
  }
  if (!isRecord(deps)) {
    throw new Error("pipeline v2 runner deps must be an object");
  }
  if (typeof deps.cli !== "function") {
    throw new Error("pipeline v2 runner deps.cli must be a function");
  }
  if (typeof deps.fetchAuth !== "function") {
    throw new Error("pipeline v2 runner deps.fetchAuth must be a function");
  }
  if (!isRecord(deps.helperConfig)) {
    throw new Error("pipeline v2 runner deps.helperConfig must be an object");
  }
  if (!isNonEmptyAbsolutePath(deps.helperConfig.socketPath)) {
    throw new Error("pipeline v2 runner deps.helperConfig.socketPath must be a non-empty absolute path");
  }
  if (!isNonEmptyAbsolutePath(deps.helperConfig.credentialFile)) {
    throw new Error("pipeline v2 runner deps.helperConfig.credentialFile must be a non-empty absolute path");
  }
  if (!isRecord(deps.baseEnv)) {
    throw new Error("pipeline v2 runner deps.baseEnv must be an object");
  }
  if (!isRecord(deps.stateRootProjection)) {
    throw new Error("pipeline v2 runner deps.stateRootProjection must be an object");
  }
  for (const field of ["localRoot", "daemonRoot"] as const) {
    if (!isCleanAbsolutePath(deps.stateRootProjection[field])) {
      throw new Error(
        `pipeline v2 runner deps.stateRootProjection.${field} must be an absolute clean path`,
      );
    }
  }
  if (deps.now !== undefined && typeof deps.now !== "function") {
    throw new Error("pipeline v2 runner deps.now must be a function");
  }
  if (deps.randomId !== undefined && typeof deps.randomId !== "function") {
    throw new Error("pipeline v2 runner deps.randomId must be a function");
  }
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

/** The unique agent-state profile names of the pipeline, in declaration order. */
function uniqueProfileNames(pipeline: ResolvedPipelineV2): string[] {
  const names: string[] = [];
  const seen = new Set<string>();
  for (const state of pipeline.states) {
    if (state.type !== "agent") {
      continue;
    }
    if (!seen.has(state.profile)) {
      seen.add(state.profile);
      names.push(state.profile);
    }
  }
  return names;
}

interface PreparedRunRoot {
  readonly localRunRoot: string;
  readonly daemonRunRoot: string;
}

/**
 * Tracks whether the exclusive run-leaf `mkdir` succeeded. From that
 * moment the run root exists and is never removed: every later failure
 * must report the actual run id and the canonical run root instead of the
 * preflight shape. Set before any verification that follows the creation,
 * because the directory exists as soon as the `mkdir` returned.
 */
interface RunLeafTracker {
  created: boolean;
  localRunRoot: string | null;
}

/**
 * The runner-owned directory mode policy: every runner-owned directory
 * — `pipeline-runs` and the run leaf, whether just created or accepted as
 * a pre-existing object — must carry exactly mode 0700. The check never
 * chmods: an unsafe existing directory is rejected unchanged. The mode is
 * verified explicitly, never trusted to the current umask.
 */
async function assertExactDirectoryMode0700(path: string, what: string): Promise<void> {
  const info = await lstat(path);
  const mode = info.mode & 0o7777;
  if (mode !== 0o700) {
    throw new Error(
      `pipeline v2 runner: the ${what} ${JSON.stringify(path)} must have mode 0700, found 0${mode.toString(8)}`,
    );
  }
}

/**
 * Owns the run-root layout `<state-root>/pipeline-runs/<run-id>` and its
 * daemon-visible projection. Both state roots must already be the same
 * real canonical directory object (dev/ino); `pipeline-runs` is created
 * 0700 when missing, or accepted only as an existing real non-symlink
 * directory with exactly mode 0700 and identical dev/ino on both sides —
 * an unsafe existing directory is rejected unchanged, with no `chmod` and
 * no run leaf. The run directory is created only by an exclusive 0700
 * `mkdir` (any pre-existing object with that run id is rejected without
 * overwrite), is verified to carry exactly mode 0700 after creation, and
 * must appear on the daemon side as the same real canonical object.
 * Nothing is removed here.
 */
async function prepareRunRoot(
  stateRootProjection: PipelineV2StateRootProjection,
  runId: string,
  leaf: RunLeafTracker,
): Promise<PreparedRunRoot> {
  const localStateRoot = stateRootProjection.localRoot;
  const daemonStateRoot = stateRootProjection.daemonRoot;

  const localRootInfo = await inspectProjectionObject(localStateRoot, "directory");
  if (localRootInfo.failure !== null || localRootInfo.identity === null) {
    throw new Error(
      `pipeline v2 runner: the local state root ${JSON.stringify(localStateRoot)} is not a real non-symlink directory`,
    );
  }
  const daemonRootInfo = await inspectProjectionObject(daemonStateRoot, "directory");
  if (daemonRootInfo.failure !== null || daemonRootInfo.identity === null) {
    throw new Error(
      `pipeline v2 runner: the daemon state root ${JSON.stringify(daemonStateRoot)} is not a real non-symlink directory`,
    );
  }
  if (!sameProjectionIdentity(localRootInfo.identity, daemonRootInfo.identity)) {
    throw new Error(
      "pipeline v2 runner: the local and daemon state roots are not the same directory object (dev/ino differ)",
    );
  }

  const pipelineRunsLocal = `${localStateRoot.replace(/\/+$/, "")}/pipeline-runs`;
  const pipelineRunsDaemon = `${daemonStateRoot.replace(/\/+$/, "")}/pipeline-runs`;
  const existing = await inspectProjectionObject(pipelineRunsLocal, "directory");
  if (existing.failure === "missing") {
    // Exclusive non-recursive creation; a concurrent creator wins the race
    // and this side falls through to the shared verification below.
    try {
      await mkdir(pipelineRunsLocal, { mode: 0o700 });
    } catch (cause) {
      if (!isErrnoExceptionCode(cause, "EEXIST")) {
        throw new Error(
          `pipeline v2 runner: cannot create ${JSON.stringify(pipelineRunsLocal)}: ${describeError(cause)}`,
        );
      }
    }
  } else if (existing.failure !== null) {
    throw new Error(
      `pipeline v2 runner: ${JSON.stringify(pipelineRunsLocal)} is not a real non-symlink directory`,
    );
  }
  // The mode belongs to the runner-owned layout, not to the shared
  // inspection primitives: created and pre-existing directories are held
  // to the same exact 0700 contract.
  await assertExactDirectoryMode0700(pipelineRunsLocal, "pipeline-runs");
  const runsFailure = await verifyPair(pipelineRunsLocal, pipelineRunsDaemon, "pipeline-runs");
  if (runsFailure !== null) {
    throw runsFailure;
  }

  const localRunRoot = `${pipelineRunsLocal}/${runId}`;
  try {
    await mkdir(localRunRoot, { mode: 0o700 });
  } catch (cause) {
    if (isErrnoExceptionCode(cause, "EEXIST")) {
      throw new Error(
        `pipeline v2 runner: refusing to reuse an existing object at ${JSON.stringify(localRunRoot)}`,
      );
    }
    throw new Error(
      `pipeline v2 runner: cannot create the run directory ${JSON.stringify(localRunRoot)}: ${describeError(cause)}`,
    );
  }
  // From this moment the run root exists and is never removed; later
  // verification failures report the actual run id and canonical run root.
  leaf.created = true;
  leaf.localRunRoot = localRunRoot;
  const runRootInfo = await inspectProjectionObject(localRunRoot, "directory");
  if (runRootInfo.failure !== null || runRootInfo.identity === null) {
    throw new Error(
      `pipeline v2 runner: the created run directory ${JSON.stringify(localRunRoot)} is not a real non-symlink directory`,
    );
  }
  await assertExactDirectoryMode0700(localRunRoot, "run directory");
  const translation = translateProjectionPath(localStateRoot, daemonStateRoot, localRunRoot);
  if (!translation.ok) {
    throw new Error(
      `pipeline v2 runner: the run directory ${JSON.stringify(localRunRoot)} has no clean projection suffix under the state root`,
    );
  }
  const daemonRunRoot = translation.daemonPath;
  const runRootFailure = await verifyPair(localRunRoot, daemonRunRoot, "run directory");
  if (runRootFailure !== null) {
    throw runRootFailure;
  }
  const runRootIdentity = runRootInfo.identity;
  const daemonRunRootInfo = await inspectProjectionObject(daemonRunRoot, "directory");
  if (
    daemonRunRootInfo.failure !== null ||
    daemonRunRootInfo.identity === null ||
    !sameProjectionIdentity(runRootIdentity, daemonRunRootInfo.identity)
  ) {
    throw new Error(
      `pipeline v2 runner: the daemon-side run directory ${JSON.stringify(daemonRunRoot)} is not the same real object as ${JSON.stringify(localRunRoot)} (dev/ino differ)`,
    );
  }
  return { localRunRoot, daemonRunRoot };
}

function isErrnoExceptionCode(cause: unknown, code: string): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as { code?: unknown }).code === code
  );
}

async function verifyPair(
  localPath: string,
  daemonPath: string,
  what: string,
): Promise<Error | null> {
  const local = await inspectProjectionObject(localPath, "directory");
  if (local.failure !== null) {
    return new Error(
      `pipeline v2 runner: the local ${what} ${JSON.stringify(localPath)} is not a real non-symlink directory`,
    );
  }
  const daemon = await inspectProjectionObject(daemonPath, "directory");
  if (daemon.failure !== null) {
    return new Error(
      `pipeline v2 runner: the daemon-side ${what} ${JSON.stringify(daemonPath)} is not a real non-symlink directory`,
    );
  }
  if (!sameProjectionIdentity(local.identity!, daemon.identity!)) {
    return new Error(
      `pipeline v2 runner: the ${what} pair ${JSON.stringify(localPath)} / ${JSON.stringify(daemonPath)} is not the same object (dev/ino differ)`,
    );
  }
  return null;
}

function signalReasonOf(signal: "SIGINT" | "SIGTERM"): PipelineV2FailureReason {
  return signal === "SIGINT" ? "signal_sigint" : "signal_sigterm";
}

/**
 * Runs one pipeline v2 production contour end to end. See the module
 * documentation for the preflight order, the run-root ownership contract
 * and the signal semantics.
 */
export async function runPipelineV2(
  options: PipelineV2RunOptions,
  deps: PipelineV2RunnerDeps,
): Promise<PipelineV2RunOutcome> {
  // --- protected preflight gate ------------------------------------------
  //
  // The options/deps shape validation and the `onSignal` capture live
  // inside one protected region, and the `RunCauseGate` is constructed
  // only after both succeeded: a null or primitive deps, a Proxy or a
  // throwing getter on any validated field, or a throwing
  // `onSignal(handler)` registration must never escape as a rejected
  // promise or an exception, must not register a signal handler before
  // its type is confirmed, and must not touch the filesystem, the
  // credential or any Session.
  const preflightFailure = (cause: unknown): PipelineV2RunOutcome => {
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 run failed: ${message}`);
    return deepFreeze({ ok: false, exitCode: 1, runId: "", runRoot: null, state: null });
  };

  let gate: RunCauseGate;
  try {
    validateRunnerContract(options, deps);
    // The signal seam is read exactly once, here, inside the protected
    // preflight; the captured value alone is checked and handed to the
    // gate, so the original getter or property is never read again.
    const capturedOnSignal = deps.onSignal;
    if (capturedOnSignal !== undefined && typeof capturedOnSignal !== "function") {
      throw new Error("pipeline v2 runner deps.onSignal must be a function");
    }
    gate = new RunCauseGate(capturedOnSignal);
  } catch (cause) {
    return preflightFailure(cause);
  }
  // Read the recorded signal through a function: the gate accepts signals
  // asynchronously between awaits, so no control-flow narrowing may hide
  // a signal recorded after an earlier check.
  const recordedSignal = (): SignalAbort | null => gate.recordedSignal;

  // The pre-run-root signal outcome: before the exclusive run-leaf
  // creation nothing is bound yet, so the run id and run root stay unset.
  const signalOutcomeBeforeRunRoot = (): PipelineV2RunOutcome => {
    const abort = recordedSignal();
    if (abort === null) {
      throw new Error("no signal was recorded");
    }
    return deepFreeze({
      ok: false,
      exitCode: signalExitCode(abort.signal),
      runId: "",
      runRoot: null,
      state: null,
      reason: signalReasonOf(abort.signal),
    });
  };

  // --- preflight: pipeline, then profiles, then Launcher authority -------
  //
  // A signal accepted during an operation that then fails before the run
  // root is created wins over that operation's error.

  let pipeline: ResolvedPipelineV2;
  const profiles = new Map<string, ResolvedProfile>();
  try {
    pipeline = await loadPipelineV2(options.pipelineRoot);
    for (const profileName of uniqueProfileNames(pipeline)) {
      profiles.set(profileName, await loadProfile(options.configRoot, profileName, deps.baseEnv));
    }
  } catch (cause) {
    if (recordedSignal() !== null) {
      return signalOutcomeBeforeRunRoot();
    }
    return preflightFailure(cause);
  }
  if (recordedSignal() !== null) {
    return signalOutcomeBeforeRunRoot();
  }

  let authority: Awaited<ReturnType<typeof lifecycleAuthority>>;
  try {
    authority = await lifecycleAuthority(
      { cli: deps.cli, fetchAuth: deps.fetchAuth, config: deps.helperConfig, baseEnv: deps.baseEnv },
      { launcherId: options.launcherId },
    );
  } catch (cause) {
    if (recordedSignal() !== null) {
      return signalOutcomeBeforeRunRoot();
    }
    return preflightFailure(cause);
  }
  if (recordedSignal() !== null) {
    return signalOutcomeBeforeRunRoot();
  }

  // --- run id and run root -------------------------------------------------

  let runId: string;
  try {
    runId = deps.randomId ? deps.randomId() : crypto.randomUUID();
    expectSafeId(runId, "pipeline v2 run id");
  } catch (cause) {
    if (recordedSignal() !== null) {
      return signalOutcomeBeforeRunRoot();
    }
    return preflightFailure(cause);
  }

  const postRunRootSignalOutcome = (
    runRootPath: string,
    state: PipelineV2RunState | null,
  ): PipelineV2RunOutcome => {
    const abort = recordedSignal();
    if (abort === null) {
      throw new Error("no signal was recorded");
    }
    return deepFreeze({
      ok: false,
      exitCode: signalExitCode(abort.signal),
      runId,
      runRoot: runRootPath,
      state,
      reason: signalReasonOf(abort.signal),
    });
  };

  const postRunRootFailure = (
    cause: unknown,
    runRootPath: string,
    state: PipelineV2RunState | null,
  ): PipelineV2RunOutcome => {
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 run failed: ${message}`);
    return deepFreeze({ ok: false, exitCode: 1, runId, runRoot: runRootPath, state });
  };

  // The run-root boundary: from the successful exclusive creation of the
  // run leaf on, the directory exists and is deliberately never removed,
  // so every later failure reports the actual run id and the canonical
  // run root instead of the generic preflight shape.
  const leaf: RunLeafTracker = { created: false, localRunRoot: null };
  let runRoot: PreparedRunRoot;
  try {
    runRoot = await prepareRunRoot(deps.stateRootProjection, runId, leaf);
  } catch (cause) {
    if (recordedSignal() !== null) {
      if (leaf.created && leaf.localRunRoot !== null) {
        return postRunRootSignalOutcome(leaf.localRunRoot, null);
      }
      return signalOutcomeBeforeRunRoot();
    }
    if (leaf.created && leaf.localRunRoot !== null) {
      return postRunRootFailure(cause, leaf.localRunRoot, null);
    }
    return preflightFailure(cause);
  }

  // --- sink, runtime, coordinator -----------------------------------------
  //
  // Both are created after the run root exists: any failure here is a
  // post-run-root failure and reports the actual run id, the canonical
  // run root and the last authoritative sink snapshot (absent until
  // `create_run` commits). The created run root is never removed.
  let sinkOrNull: PipelineV2RunStateSink | null = null;
  let runtime: ReturnType<typeof createDockerHelperPipelineV2Runtime>;
  const runtimeParams: DockerHelperPipelineV2RuntimeParams = {
    pipeline,
    profiles,
    cli: deps.cli,
    helperConfig: deps.helperConfig,
    operatorEnv: authority.baseOperatorEnv,
    expectedLauncherId: authority.auth.launcher_id,
    runRootProjection: { localRoot: runRoot.localRunRoot, daemonRoot: runRoot.daemonRunRoot },
  };
  try {
    sinkOrNull = new PipelineV2RunStateSink({
      stateRoot: deps.stateRootProjection.localRoot,
      runId,
      now: deps.now,
    });
    runtime = createDockerHelperPipelineV2Runtime(runtimeParams);
  } catch (cause) {
    const state = sinkOrNull === null ? null : sinkOrNull.snapshot;
    if (recordedSignal() !== null) {
      return postRunRootSignalOutcome(runRoot.localRunRoot, state);
    }
    return postRunRootFailure(cause, runRoot.localRunRoot, state);
  }
  const sink = sinkOrNull;
  const preCoordinatorSignal = recordedSignal();
  if (preCoordinatorSignal !== null) {
    return postRunRootSignalOutcome(runRoot.localRunRoot, sink.snapshot);
  }

  const control: PipelineV2CoordinatorControl = {
    currentSignal: () => gate.recordedSignal?.signal ?? null,
    freezeSignal: () => gate.freezeSignalAcceptance()?.signal ?? null,
  };

  let result;
  try {
    result = await coordinatePipelineV2Run(
      {
        pipeline,
        runId,
        runRoot: runRoot.localRunRoot,
        projectSourcePath: options.projectSourcePath,
        inputBindings: options.inputBindings,
        sink,
        runtime,
      },
      control,
    );
  } catch (cause) {
    const state = sink.snapshot;
    if (recordedSignal() !== null) {
      return postRunRootSignalOutcome(runRoot.localRunRoot, state);
    }
    console.error(
      `orchestrator: pipeline v2 run failed: ${cause instanceof Error ? cause.message : String(cause)}`,
    );
    return deepFreeze({
      ok: false,
      exitCode: 1,
      runId,
      runRoot: runRoot.localRunRoot,
      state,
    });
  }

  if (result.ok) {
    return deepFreeze({
      ok: true,
      exitCode: 0,
      runId,
      runRoot: runRoot.localRunRoot,
      state: result.state,
    });
  }
  const reason = result.reason;
  const exitCode =
    reason === "signal_sigint"
      ? 130
      : reason === "signal_sigterm"
        ? 143
        : 1;
  if (reason !== "terminal_failed") {
    console.error(`orchestrator: pipeline v2 run failed: ${reason}`);
  }
  return deepFreeze({
    ok: false,
    exitCode,
    runId,
    runRoot: runRoot.localRunRoot,
    state: result.state,
    reason,
  });
}
