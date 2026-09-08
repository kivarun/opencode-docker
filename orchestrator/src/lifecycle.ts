import { stat } from "node:fs/promises";
import {
  DockerHelperError,
  type AuthFetcher,
  type CliRunner,
} from "./docker_helper.ts";
import {
  createChildSession,
  deleteChildSession,
  requireLauncherCredential,
  type HelperConfig,
} from "./launcher.ts";
import { saveRunState, stateDir, type RunState } from "./state.ts";
import type { TransitionStep } from "./pipeline_engine.ts";

export class SignalAbort extends Error {
  readonly signal: "SIGINT" | "SIGTERM";
  constructor(signal: "SIGINT" | "SIGTERM") {
    super(`aborted by ${signal}`);
    this.signal = signal;
  }
}

export function signalExitCode(signal: "SIGINT" | "SIGTERM"): number {
  return signal === "SIGINT" ? 130 : 143;
}

export class StatePersistError extends Error {
  constructor() {
    super("cannot persist run state");
  }
}

/**
 * Run state sink: the lifecycle speaks only this vocabulary. `smoke` keeps
 * the legacy flat adapter (unchanged contract); `agent-smoke` plugs in the
 * durable pipeline run state sink and never writes the legacy smoke state.
 */
export interface RunStateSink {
  /**
   * Called once at run start. The legacy adapter intentionally does nothing:
   * its first disk write remains the first phase update. The pipeline sink
   * creates the durable run record here.
   */
  initialize(): Promise<void>;
  /** Progress updates while the run is active. */
  phase(status: string, sessionId?: string): Promise<void>;
  /** Authoritative final status write after cleanup. */
  finalize(outcome: RunFinalization): Promise<void>;
  /** Status for the lifecycle outcome (last known). */
  currentStatus(): string;
}

export interface RunFinalization {
  status: "success" | "failed" | "cleanup_failed";
  failure: Error | null;
  signal: SignalAbort | null;
  sessionId: string | null;
}

/**
 * Legacy flat run state for `smoke`, byte-for-byte compatible with the
 * previous in-lifecycle implementation.
 */
export class LegacyRunStateSink implements RunStateSink {
  private readonly state: RunState;
  private sessionId: string | null = null;

  constructor(
    private readonly stateDirPath: string,
    params: { runId: string; workspace: string; workerImage: string; startedAt: string },
    private readonly now: () => Date,
  ) {
    this.state = {
      schema_version: 1,
      run_id: params.runId,
      workspace: params.workspace,
      worker_image: params.workerImage,
      started_at: params.startedAt,
      updated_at: params.startedAt,
      status: "starting",
    };
  }

  async initialize(): Promise<void> {
    // no initial write: the legacy contract persists the run state starting
    // with the first phase update
  }

  async phase(status: string, sessionId?: string): Promise<void> {
    if (sessionId !== undefined) {
      this.sessionId = sessionId;
    }
    this.state.status = status;
    this.state.updated_at = this.now().toISOString();
    if (this.sessionId !== null) {
      this.state.session_id = this.sessionId;
    }
    await saveRunState(this.stateDirPath, this.state);
  }

  async finalize(outcome: RunFinalization): Promise<void> {
    if (outcome.sessionId !== null) {
      this.sessionId = outcome.sessionId;
    }
    await this.phase(outcome.status);
  }

  currentStatus(): string {
    return this.state.status;
  }
}

/**
 * Pipeline-specific extension used by `agent-smoke` to commit engine
 * transitions and the reached terminal state durably.
 */
export interface PipelineTransitionSink extends RunStateSink {
  recordTransition(
    step: TransitionStep,
    accepted: { resultSha256: string; artifacts: readonly string[] },
  ): Promise<void>;
  recordTerminal(terminalStateId: string, terminalResult: "success" | "failed"): Promise<void>;
}

export function isPipelineTransitionSink(sink: RunStateSink): sink is PipelineTransitionSink {
  const candidate = sink as PipelineTransitionSink;
  return (
    typeof candidate.recordTransition === "function" &&
    typeof candidate.recordTerminal === "function"
  );
}

export type StateSinkFactory = (runId: string, stateDirPath: string) => RunStateSink;

export interface LifecycleDeps {
  cli: CliRunner;
  fetchAuth: AuthFetcher;
  config: HelperConfig;
  stateDirPath?: string;
  workspaceExists?: (path: string) => Promise<boolean>;
  now?: () => Date;
  randomId?: () => string;
  onSignal?: (handler: (signal: "SIGINT" | "SIGTERM") => void) => void;
  baseEnv?: Readonly<Record<string, string | undefined>>;
}

export interface LifecycleOptions {
  workspace: string;
  workerImage: string;
  launcherId?: string;
}

export interface PreSessionContext {
  runId: string;
  updateState: (status: string) => Promise<void>;
}

export interface SessionContext extends PreSessionContext {
  sessionId: string;
  childToken: string;
  childEnv: Record<string, string>;
  checkAbort: () => void;
  /** Present only when the command runs on the pipeline run state sink. */
  transitionSink?: PipelineTransitionSink;
}

export interface LifecycleHooks {
  preSession?: (ctx: PreSessionContext) => Promise<void>;
  withSession?: (ctx: SessionContext) => Promise<void>;
}

export interface LifecycleOutcome {
  ok: boolean;
  exitCode: number;
  runId: string;
  sessionId?: string;
  status: string;
  detail?: string;
}

/**
 * Environment for docker-helper `pull`/`run` CLI calls: the child Session
 * bearer only. The bearer authorizes the CLI call and is repeated through
 * `--env` so the worker receives its Session capability. No ambient or
 * operator environment is inherited.
 */
export function childSessionEnv(childToken: string): Record<string, string> {
  return { DOCKER_HELPER_SESSION_TOKEN: childToken };
}

const CLI_ENV_KEYS = ["HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR", "DOCKER_HELPER_CONFIG"] as const;

export function operatorEnv(
  baseEnv: Readonly<Record<string, string | undefined>>,
): Record<string, string> {
  const env: Record<string, string> = {};
  for (const key of CLI_ENV_KEYS) {
    const value = baseEnv[key];
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  return env;
}

export async function runWithChildSession(
  deps: LifecycleDeps,
  options: LifecycleOptions,
  hooks: LifecycleHooks,
  label: string,
  makeStateSink?: StateSinkFactory,
): Promise<LifecycleOutcome> {
  const runId = deps.randomId ? deps.randomId() : crypto.randomUUID();
  const nowFn = (): Date => (deps.now ? deps.now() : new Date());
  const startedAt = nowFn().toISOString();
  const stateDirPath = deps.stateDirPath ?? stateDir(deps.baseEnv ?? {});
  const workspaceExists = deps.workspaceExists ?? defaultWorkspaceExists;
  const baseOperatorEnv = operatorEnv(deps.baseEnv ?? {});

  const sink: RunStateSink = makeStateSink
    ? makeStateSink(runId, stateDirPath)
    : new LegacyRunStateSink(
        stateDirPath,
        {
          runId,
          workspace: options.workspace,
          workerImage: options.workerImage,
          startedAt,
        },
        nowFn,
      );
  const transitionSink = isPipelineTransitionSink(sink) ? sink : undefined;

  let childSessionId: string | null = null;
  const failureBox: { error: Error | null } = { error: null };
  const cleanupBox: { error: Error | null } = { error: null };
  const signalBox: { abort: SignalAbort | null } = { abort: null };
  let acceptSignals = true;
  let cleanupPromise: Promise<void> | null = null;

  const cleanup = (): Promise<void> => {
    if (cleanupPromise === null) {
      cleanupPromise = (async () => {
        if (childSessionId === null) {
          return;
        }
        try {
          await deleteChildSession(deps.cli, deps.config, childSessionId, baseOperatorEnv);
        } catch (cause) {
          cleanupBox.error = cause instanceof Error ? cause : new Error(String(cause));
        }
      })();
    }
    return cleanupPromise;
  };

  deps.onSignal?.((signal) => {
    if (!acceptSignals || signalBox.abort !== null) {
      return;
    }
    // Record the abort only. The caller (main.ts) forwards the signal to a
    // running worker `run` CLI process; everything else (`session create`,
    // `pull`, `session delete`) runs to completion. Cleanup happens solely in
    // the lifecycle `finally`, after the active hook settles — the fire-and-
    // forget cleanup path is intentionally gone. docker-helper 2.1.0 cancels
    // the container operation on the signal best-effort and does not confirm
    // a terminal operation state. Acceptance closes once the authoritative
    // final state write has completed (see the end of the `finally` below);
    // a signal delivered after that point can no longer change the outcome.
    signalBox.abort = new SignalAbort(signal);
  });

  const updateState = (status: string): Promise<void> => sink.phase(status);

  try {
    await sink.initialize();

    if (!(await workspaceExists(options.workspace))) {
      throw new Error(
        `workspace ${options.workspace} is not accessible to the orchestrator; ` +
          "mount it at the same absolute path used for the docker-helper session",
      );
    }

    const auth = await requireLauncherCredential(
      deps.config,
      deps.fetchAuth,
      defaultFileExists,
    );
    if (
      options.launcherId !== undefined &&
      auth.launcher_id !== options.launcherId
    ) {
      throw new DockerHelperError(
        "wrong_authority",
        `installed credential belongs to launcher ${auth.launcher_id ?? "unknown"}, expected ${options.launcherId} (point XDG_CONFIG_HOME at the directory holding the intended credential.token)`,
      );
    }
    console.error(
      `orchestrator: launcher credential ok (launcher ${auth.launcher_id ?? "unknown"}, principal ${auth.principal ?? "unknown"})`,
    );

    await updateState("creating_session");

    if (signalBox.abort !== null) {
      throw signalBox.abort;
    }

    if (hooks.preSession !== undefined) {
      await hooks.preSession({ runId, updateState });
      if (signalBox.abort !== null) {
        throw signalBox.abort;
      }
    }

    const child = await createChildSession(deps.cli, deps.config, options.workspace, baseOperatorEnv);
    childSessionId = child.sessionId;

    if (
      auth.launcher_id !== undefined &&
      child.launcherId !== undefined &&
      child.launcherId !== auth.launcher_id
    ) {
      throw new DockerHelperError(
        "unexpected_response",
        `created session belongs to launcher ${child.launcherId}, expected ${auth.launcher_id}`,
      );
    }
    console.error(`orchestrator: child session ${child.sessionId} created`);
    await sink.phase("session_created", child.sessionId);

    if (signalBox.abort !== null) {
      throw signalBox.abort;
    }

    if (hooks.withSession !== undefined) {
      await hooks.withSession({
        runId,
        updateState,
        sessionId: child.sessionId,
        childToken: child.token,
        childEnv: childSessionEnv(child.token),
        checkAbort: () => {
          if (signalBox.abort !== null) {
            throw signalBox.abort;
          }
        },
        transitionSink,
      });
    }
  } catch (cause) {
    failureBox.error = cause instanceof Error ? cause : new Error(String(cause));
  } finally {
    await cleanup();
    // A recorded signal (arriving at any point after the last explicit abort
    // check — e.g. during result/artifact verification or during cleanup)
    // forbids a final success status. Cleanup failure keeps the higher
    // priority; otherwise a signal-only run reports `failed`. No state machine
    // is introduced for this.
    const finalStatus =
      cleanupBox.error !== null
        ? "cleanup_failed"
        : failureBox.error === null && signalBox.abort === null
          ? "success"
          : "failed";
    try {
      await sink.finalize({
        status: finalStatus,
        failure: failureBox.error,
        signal: signalBox.abort,
        sessionId: childSessionId,
      });
      // A signal accepted while the final write was in flight invalidates a
      // persisted success: rewrite the authoritative final status and wait for
      // that write before the outcome is considered final.
      if (finalStatus === "success" && signalBox.abort !== null) {
        await sink.finalize({
          status: "failed",
          failure: failureBox.error,
          signal: signalBox.abort,
          sessionId: childSessionId,
        });
      }
    } catch {
      failureBox.error ??= new StatePersistError();
    }
    // Linearization point: the authoritative final state write has completed
    // and the last signalBox check above ran synchronously (no await in
    // between). Close signal acceptance; a signal delivered from here on can
    // no longer change the recorded outcome or the exit code.
    acceptSignals = false;
  }

  const failure = failureBox.error;
  const cleanupError = cleanupBox.error;
  if (failure !== null && !(failure instanceof SignalAbort)) {
    console.error(`orchestrator: ${label} failed: ${failure.message}`);
  }
  if (cleanupError !== null) {
    console.error(`orchestrator: cleanup failed: ${cleanupError.message}`);
  }

  let exitCode: number;
  if (cleanupError !== null) {
    exitCode = 1;
  } else if (signalBox.abort !== null) {
    exitCode = signalExitCode(signalBox.abort.signal);
  } else if (failure !== null) {
    exitCode = 1;
  } else {
    exitCode = 0;
  }

  return {
    ok: exitCode === 0,
    exitCode,
    runId,
    sessionId: childSessionId ?? undefined,
    status: sink.currentStatus(),
    detail: failure?.message,
  };
}

export async function defaultWorkspaceExists(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch {
    return false;
  }
}

export async function defaultFileExists(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}
