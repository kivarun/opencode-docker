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
 * the legacy flat adapter (unchanged contract); the multi-state pipeline
 * runner uses the dedicated pipeline run state sink instead.
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
 * Run-level cause and signal tracking shared by every run shape (`smoke`
 * single-session and the multi-state pipeline runner): exactly one
 * implementation of signal acceptance, failure/cleanup cause recording, and
 * the finalization cutoff.
 *
 * Signals are recorded only while acceptance is open. The first terminal
 * cause of the run wins: a recorded signal suppresses nothing by itself —
 * the runner decides per operation whether a signal still participates.
 * Acceptance closes synchronously at the cutoff (`freezeFinalStatus`),
 * called after all cleanup has settled and before the single authoritative
 * final state write. A signal delivered from the cutoff onwards — including
 * while the final write is in flight — is late and can no longer change the
 * recorded outcome or the exit code; the terminal status is written exactly
 * once and never rewritten.
 */
export class RunCauseGate {
  /** First non-signal failure of the run body (first-wins). */
  readonly failure: { error: Error | null } = { error: null };
  /** First cleanup failure of the run (first-wins, highest priority). */
  readonly cleanup: { error: Error | null } = { error: null };

  private abort: SignalAbort | null = null;
  private acceptSignals = true;

  constructor(onSignal?: (handler: (signal: "SIGINT" | "SIGTERM") => void) => void) {
    onSignal?.((signal) => {
      if (!this.acceptSignals || this.abort !== null) {
        return;
      }
      // Record the abort only. The caller (main.ts) forwards the signal to a
      // running worker `run` CLI process; everything else (`session create`,
      // `pull`, `session delete`) runs to completion. Cleanup happens inside
      // the run body before the final state write — the fire-and-forget
      // cleanup path is intentionally gone. docker-helper 2.1.0 cancels the
      // container operation on the signal best-effort and does not confirm a
      // terminal operation state.
      this.abort = new SignalAbort(signal);
    });
  }

  /** The recorded signal, if any. */
  get recordedSignal(): SignalAbort | null {
    return this.abort;
  }

  /** Throws the recorded abort if a signal was accepted before the cutoff. */
  checkAbort(): void {
    if (this.abort !== null) {
      throw this.abort;
    }
  }

  recordFailure(cause: unknown): void {
    this.failure.error = cause instanceof Error ? cause : new Error(String(cause));
  }

  /** First-wins cleanup failure recording. */
  recordCleanupFailure(cause: unknown): void {
    if (this.cleanup.error !== null) {
      return;
    }
    this.cleanup.error = cause instanceof Error ? cause : new Error(String(cause));
  }

  /**
   * The cutoff. Cleanup has settled; compute the final status synchronously
   * from the accepted causes, then close signal acceptance with no await
   * between the cause snapshot and this line.
   */
  freezeFinalStatus(): "success" | "failed" | "cleanup_failed" {
    const status =
      this.cleanup.error !== null
        ? "cleanup_failed"
        : this.failure.error === null && this.abort === null
          ? "success"
          : "failed";
    this.acceptSignals = false;
    return status;
  }

  /**
   * The signal-only cutoff, atomically and synchronously: snapshot the
   * currently accepted signal, close signal acceptance, and return the
   * frozen abort (null when no signal was accepted). No await may appear
   * between reading `this.abort` and closing acceptance; both happen in
   * this single synchronous body. The final-status cutoff
   * (`freezeFinalStatus`) and the v1 semantics are unchanged; a caller
   * uses exactly one of the two cutoffs per run.
   */
  freezeSignalAcceptance(): SignalAbort | null {
    const frozen = this.abort;
    this.acceptSignals = false;
    return frozen;
  }
}

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

export interface SessionContext {
  runId: string;
  sessionId: string;
  childToken: string;
  childEnv: Record<string, string>;
  checkAbort: () => void;
  updateState: (status: string) => Promise<void>;
}

export interface LifecycleHooks {
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

/** Run-level authority prelude shared by the smoke and pipeline runners. */
export interface LifecycleAuthority {
  auth: Awaited<ReturnType<typeof requireLauncherCredential>>;
  baseOperatorEnv: Record<string, string>;
}

export async function lifecycleAuthority(
  deps: LifecycleDeps,
  options: { launcherId?: string },
): Promise<LifecycleAuthority> {
  const baseOperatorEnv = operatorEnv(deps.baseEnv ?? {});
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
  return { auth, baseOperatorEnv };
}

/**
 * Run-level exit-code computation shared by every run shape: a cleanup
 * failure is exit 1, an accepted signal keeps its conventional exit code,
 * any other failure is exit 1, success is 0.
 */
export function runOutcomeExitCode(
  failure: Error | null,
  cleanupError: Error | null,
  signal: SignalAbort | null,
): number {
  if (cleanupError !== null) {
    return 1;
  }
  if (signal !== null) {
    return signalExitCode(signal.signal);
  }
  if (failure !== null) {
    return 1;
  }
  return 0;
}

/**
 * Runs the smoke command: one child Session, one worker run, one result
 * verification. The legacy flat run state is kept byte-for-byte compatible.
 */
export async function runWithChildSession(
  deps: LifecycleDeps,
  options: LifecycleOptions,
  hooks: LifecycleHooks,
  label: string,
): Promise<LifecycleOutcome> {
  const runId = deps.randomId ? deps.randomId() : crypto.randomUUID();
  const nowFn = (): Date => (deps.now ? deps.now() : new Date());
  const startedAt = nowFn().toISOString();
  const stateDirPath = deps.stateDirPath ?? stateDir(deps.baseEnv ?? {});
  const workspaceExists = deps.workspaceExists ?? defaultWorkspaceExists;

  const sink = new LegacyRunStateSink(
    stateDirPath,
    {
      runId,
      workspace: options.workspace,
      workerImage: options.workerImage,
      startedAt,
    },
    nowFn,
  );

  let childSessionId: string | null = null;
  const gate = new RunCauseGate(deps.onSignal);
  let cleanupPromise: Promise<void> | null = null;

  const cleanup = (): Promise<void> => {
    if (cleanupPromise === null) {
      cleanupPromise = (async () => {
        if (childSessionId === null) {
          return;
        }
        try {
          await deleteChildSession(deps.cli, deps.config, childSessionId, operatorEnv(deps.baseEnv ?? {}));
        } catch (cause) {
          gate.recordCleanupFailure(cause);
        }
      })();
    }
    return cleanupPromise;
  };

  const updateState = (status: string): Promise<void> => sink.phase(status);

  try {
    await sink.initialize();

    if (!(await workspaceExists(options.workspace))) {
      throw new Error(
        `workspace ${options.workspace} is not accessible to the orchestrator; ` +
          "mount it at the same absolute path used for the docker-helper session",
      );
    }

    const { auth, baseOperatorEnv } = await lifecycleAuthority(deps, {
      launcherId: options.launcherId,
    });

    await updateState("creating_session");

    gate.checkAbort();

    if (hooks.withSession !== undefined) {
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

      gate.checkAbort();

      await hooks.withSession({
        runId,
        sessionId: child.sessionId,
        childToken: child.token,
        childEnv: childSessionEnv(child.token),
        checkAbort: () => gate.checkAbort(),
        updateState,
      });
    }
  } catch (cause) {
    gate.recordFailure(cause);
  } finally {
    await cleanup();
    const finalFailure = gate.failure.error;
    const finalSignal = gate.recordedSignal;
    const finalStatus = gate.freezeFinalStatus();
    try {
      await sink.finalize({
        status: finalStatus,
        failure: finalFailure,
        signal: finalSignal,
        sessionId: childSessionId,
      });
    } catch (cause) {
      gate.failure.error ??= cause instanceof Error ? cause : new StatePersistError();
    }
  }

  const failure = gate.failure.error;
  const cleanupError = gate.cleanup.error;
  if (failure !== null && !(failure instanceof SignalAbort)) {
    console.error(`orchestrator: ${label} failed: ${failure.message}`);
  }
  if (cleanupError !== null) {
    console.error(`orchestrator: cleanup failed: ${cleanupError.message}`);
  }

  const exitCode = runOutcomeExitCode(failure, cleanupError, gate.recordedSignal);

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
