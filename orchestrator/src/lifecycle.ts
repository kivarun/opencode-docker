import { stat } from "node:fs/promises";
import {
  DockerHelperError,
  type AuthFetcher,
  type CliRunner,
} from "./docker_helper.ts";
import type { HelperTransport } from "./helper_api.ts";
import {
  createChildSession,
  deleteChildSession,
  requireLauncherCredential,
  type HelperConfig,
} from "./launcher.ts";
import { stateDir, saveRunState, type RunState } from "./state.ts";

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

export interface LifecycleDeps {
  cli: CliRunner;
  transport?: HelperTransport;
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
  checkAbort: () => void;
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
): Promise<LifecycleOutcome> {
  const runId = deps.randomId ? deps.randomId() : crypto.randomUUID();
  const startedAt = (deps.now ? deps.now() : new Date()).toISOString();
  const stateDirPath = deps.stateDirPath ?? stateDir(deps.baseEnv ?? {});
  const workspaceExists = deps.workspaceExists ?? defaultWorkspaceExists;
  const baseOperatorEnv = operatorEnv(deps.baseEnv ?? {});

  let childSessionId: string | null = null;
  const failureBox: { error: Error | null } = { error: null };
  const cleanupBox: { error: Error | null } = { error: null };
  const signalBox: { abort: SignalAbort | null } = { abort: null };
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
    if (signalBox.abort !== null) {
      return;
    }
    signalBox.abort = new SignalAbort(signal);
    void cleanup();
  });

  const state: RunState = {
    schema_version: 1,
    run_id: runId,
    workspace: options.workspace,
    worker_image: options.workerImage,
    started_at: startedAt,
    updated_at: startedAt,
    status: "starting",
  };

  const updateState = async (status: string): Promise<void> => {
    state.status = status;
    state.updated_at = (deps.now ? deps.now() : new Date()).toISOString();
    if (childSessionId !== null) {
      state.session_id = childSessionId;
    }
    await saveRunState(stateDirPath, state);
  };

  try {
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
    await updateState("session_created");

    if (signalBox.abort !== null) {
      throw signalBox.abort;
    }

    if (hooks.withSession !== undefined) {
      await hooks.withSession({
        runId,
        updateState,
        sessionId: child.sessionId,
        childToken: child.token,
        checkAbort: () => {
          if (signalBox.abort !== null) {
            throw signalBox.abort;
          }
        },
      });
    }
  } catch (cause) {
    failureBox.error = cause instanceof Error ? cause : new Error(String(cause));
  } finally {
    await cleanup();
    try {
      await updateState(
        cleanupBox.error !== null
          ? "cleanup_failed"
          : failureBox.error === null
            ? "success"
            : "failed",
      );
    } catch {
      failureBox.error ??= new StatePersistError();
    }
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
    status: state.status,
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
