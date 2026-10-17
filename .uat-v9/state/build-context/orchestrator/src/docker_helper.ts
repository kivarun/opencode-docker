export interface CliResult {
  code: number;
  stdout?: string;
  stderr?: string;
  /**
   * True only when the runner's own timeout expired and terminated the CLI
   * process. Never set for a user-signal abort (that keeps its own exit-code
   * semantics in the lifecycle).
   */
  timedOut?: boolean;
}

export type CliStdio = "capture" | "inherit" | "stderr";

/**
 * Largest timeout safely representable by a single JS timer: the timer delay
 * in milliseconds must stay below 2^31, so the seconds bound is
 * floor((2^31 - 1) / 1000). Values above it are rejected, never clamped.
 */
export const MAX_RUN_TIMEOUT_SECONDS = 2147483;

export interface CliRunOptions {
  /**
   * Forward a received SIGINT/SIGTERM to this CLI process. Only worker `run`
   * calls are signalable: `session create/delete` and `pull` always run to
   * completion.
   */
  signalOnAbort?: boolean;
  /**
   * Fail the CLI call after this many seconds by sending the process SIGTERM.
   * Only honored together with `signalOnAbort` (worker `run` calls only).
   */
  timeoutSeconds?: number;
}

export type CliRunner = (
  args: string[],
  env: Record<string, string>,
  stdio: CliStdio,
  opts?: CliRunOptions,
) => Promise<CliResult>;

export interface AuthInfo {
  authority: string;
  principal?: string;
  launcher_id?: string;
}

export type AuthFetcher = (
  socketPath: string,
  token: string,
) => Promise<{ status: number; body: unknown }>;

export const DEFAULT_SOCKET_PATH = "/run/docker-helper/docker-helper.sock";

export const CREDENTIAL_DIR_MODE = 0o700;

export function resolveSocketPath(
  envSocketPath: string | undefined,
): string {
  if (envSocketPath && envSocketPath.trim() !== "") {
    return envSocketPath.trim();
  }
  return DEFAULT_SOCKET_PATH;
}

export function credentialPath(
  env: { XDG_CONFIG_HOME?: string; HOME?: string },
): string {
  const xdgConfig = env.XDG_CONFIG_HOME?.trim() || "";
  if (xdgConfig !== "") {
    return `${xdgConfig.replace(/\/+$/, "")}/docker-helper/credential.token`;
  }
  const home = env.HOME?.trim() || "";
  if (home === "") {
    throw new Error("cannot determine home directory for docker-helper credential");
  }
  return `${home.replace(/\/+$/, "")}/.config/docker-helper/credential.token`;
}

export class DockerHelperError extends Error {
  readonly kind:
    | "endpoint_unavailable"
    | "credential_missing"
    | "credential_rejected"
    | "wrong_authority"
    | "cli_failure"
    | "unexpected_response";

  constructor(kind: DockerHelperError["kind"], message: string) {
    super(message);
    this.name = "DockerHelperError";
    this.kind = kind;
  }
}

export interface OperatorArgsOptions {
  socketPath: string;
}

export function operatorArgs(
  subcommand: string[],
  opts: OperatorArgsOptions,
): string[] {
  return ["session", ...subcommand, "--endpoint", opts.socketPath, "--json"];
}

export interface ParsedSessionCreate {
  sessionId: string;
  token: string;
  launcherId?: string;
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new DockerHelperError(
      "unexpected_response",
      `docker-helper session create: ${field} is missing or not a non-empty string`,
    );
  }
  return value;
}

export function parseSessionCreate(stdout: string): ParsedSessionCreate {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session create: stdout is not valid JSON",
    );
  }
  if (typeof parsed !== "object" || parsed === null) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session create: response is not a JSON object",
    );
  }
  const obj = parsed as Record<string, unknown>;
  if (obj.ok !== true) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session create: response has ok != true",
    );
  }
  const session = obj.session;
  if (typeof session !== "object" || session === null) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session create: response has no session object",
    );
  }
  const sessionObj = session as Record<string, unknown>;
  const launcherId =
    typeof sessionObj.launcher_id === "string" && sessionObj.launcher_id !== ""
      ? sessionObj.launcher_id
      : undefined;
  return {
    sessionId: requireString(sessionObj.id, "session.id"),
    token: requireString(obj.token, "token"),
    launcherId,
  };
}

export function parseSessionDelete(stdout: string, sessionId: string): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session delete: stdout is not valid JSON",
    );
  }
  if (typeof parsed !== "object" || parsed === null) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session delete: response is not a JSON object",
    );
  }
  const obj = parsed as Record<string, unknown>;
  if (obj.ok !== true || obj.deleted !== true || obj.id !== sessionId) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper session delete: unexpected response payload",
    );
  }
}

export async function fetchAuthOverSocket(
  socketPath: string,
  token: string,
): Promise<{ status: number; body: unknown }> {
  let response: Response;
  try {
    response = await fetch("http://localhost/auth", {
      unix: socketPath,
      headers: { Authorization: `Bearer ${token}` },
    });
  } catch (cause) {
    throw new DockerHelperError(
      "endpoint_unavailable",
      `docker-helper endpoint not reachable at ${socketPath}: ${describeError(cause)}`,
    );
  }
  let body: unknown = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  return { status: response.status, body };
}

export function classifyAuthResult(
  result: { status: number; body: unknown },
): AuthInfo {
  if (result.status === 401 || result.status === 403) {
    throw new DockerHelperError(
      "credential_rejected",
      `docker-helper rejected the installed credential (HTTP ${result.status}); the endpoint is reachable`,
    );
  }
  if (result.status !== 200) {
    throw new DockerHelperError(
      "endpoint_unavailable",
      `docker-helper /auth returned HTTP ${result.status}`,
    );
  }
  if (typeof result.body !== "object" || result.body === null) {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper /auth returned a non-object body",
    );
  }
  const body = result.body as Record<string, unknown>;
  const authority = body.authority;
  if (typeof authority !== "string" || authority === "") {
    throw new DockerHelperError(
      "unexpected_response",
      "docker-helper /auth response has no authority",
    );
  }
  const principal =
    typeof body.principal === "string" && body.principal !== ""
      ? body.principal
      : undefined;
  const launcherId =
    typeof body.launcher_id === "string" && body.launcher_id !== ""
      ? body.launcher_id
      : undefined;
  return { authority, principal, launcher_id: launcherId };
}

export function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export class SubprocessCliRunner {
  private active: {
    proc: Bun.Subprocess<"ignore", "pipe" | "inherit", "pipe" | "inherit">;
    signalOnAbort: boolean;
    cause: "user_signal" | "timeout" | null;
  } | null = null;

  /**
   * Delivers a user signal to the active signalable worker process.
   *
   * First-wins contract for one active worker run: the first terminal cause —
   * `timeout` (runner timer) or `user_signal` (this call) — is recorded once.
   * Returns true when the orchestrator should still record a user abort in
   * the lifecycle (no signalable worker is active, or this signal just claimed
   * the worker). Returns false only when a timeout already claimed the active
   * worker: a late user signal must not reclassify that termination and must
   * not reach the lifecycle as an abort.
   */
  killActive(signal: "SIGINT" | "SIGTERM"): boolean {
    const active = this.active;
    if (active === null || !active.signalOnAbort) {
      return true;
    }
    if (active.cause === "timeout") {
      return false;
    }
    if (active.cause === null) {
      active.cause = "user_signal";
      try {
        active.proc.kill(signal);
      } catch {
        // the process already exited
      }
    }
    return true;
  }

  async run(
    args: string[],
    env: Record<string, string>,
    stdio: CliStdio,
    opts?: CliRunOptions,
  ): Promise<CliResult> {
    const proc = Bun.spawn(["docker-helper", ...args], {
      env,
      stdin: "ignore",
      stdout:
        stdio === "inherit" ? "inherit" : stdio === "stderr" ? 2 : "pipe",
      stderr:
        stdio === "inherit" ? "inherit" : stdio === "stderr" ? 2 : "pipe",
    });
    const active: NonNullable<SubprocessCliRunner["active"]> = {
      proc,
      signalOnAbort: opts?.signalOnAbort === true,
      cause: null,
    };
    this.active = active;
    let timedOut = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    if (
      active.signalOnAbort &&
      typeof opts?.timeoutSeconds === "number" &&
      Number.isSafeInteger(opts.timeoutSeconds) &&
      opts.timeoutSeconds > 0 &&
      opts.timeoutSeconds <= MAX_RUN_TIMEOUT_SECONDS
    ) {
      timer = setTimeout(() => {
        // first-wins: a user signal that already claimed this worker run
        // cannot be converted into a timeout classification
        if (active.cause !== null) {
          return;
        }
        active.cause = "timeout";
        timedOut = true;
        try {
          proc.kill("SIGTERM");
        } catch {
          // the process already exited
        }
      }, opts.timeoutSeconds * 1000);
    }
    try {
      if (stdio === "inherit" || stdio === "stderr") {
        // `inherit` hands both streams to the parent's own stdout/stderr;
        // `stderr` streams the child's stdout AND stderr directly onto the
        // parent's file descriptor 2 (kernel-level, no buffering, nothing
        // is read back into memory and nothing reaches the parent's
        // stdout). Neither mode returns captured output.
        return { code: await proc.exited, timedOut };
      }
      const [stdout, stderr] = await Promise.all([
        new Response(proc.stdout as ReadableStream).text(),
        new Response(proc.stderr as ReadableStream).text(),
      ]);
      return { code: await proc.exited, stdout, stderr, timedOut };
    } finally {
      if (timer !== null) {
        clearTimeout(timer);
      }
      if (this.active === active) {
        this.active = null;
      }
    }
  }
}
