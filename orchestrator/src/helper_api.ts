import { describeError } from "./docker_helper.ts";
import type { WorkerSpec } from "./worker.ts";

export interface RunOutcome {
  code: number;
  operationId: string;
}

export interface HelperTransport {
  pull(image: string, bearer: string): Promise<void>;
  run(spec: WorkerSpec, bearer: string): Promise<RunOutcome>;
  /**
   * Request cancellation of the active worker operation.
   *
   * Resolves only after cancellation is fully resolved:
   * - a run is in flight: the cancellation is applied to that operation and the
   *   operation is confirmed to have reached a terminal state;
   * - the run is still awaiting its POST /run response: a pending cancellation
   *   is applied immediately after the operation identity is obtained, and this
   *   promise resolves after that operation reaches a terminal state;
   * - no run is in flight: resolves immediately; a later run refuses to start.
   *
   * Rejects when the cancellation request fails or the operation state cannot
   * be confirmed. Callers must treat an unsettled or rejected cancellation as
   * "the operation state is unknown" and never as success.
   */
  cancelActive(): Promise<void>;
}

export class TransportError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TransportError";
  }
}

interface HelperApiErrorBody {
  ok: false;
  code?: string;
  message?: string;
}

interface HelperResponse {
  httpStatus: number;
  body: Record<string, unknown>;
}

async function helperRequest(
  socketPath: string,
  bearer: string,
  method: "GET" | "POST",
  path: string,
  body?: unknown,
): Promise<HelperResponse> {
  let response: Response;
  try {
    response = await fetch(`http://localhost${path}`, {
      unix: socketPath,
      method,
      headers: {
        Authorization: `Bearer ${bearer}`,
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
  } catch (cause) {
    throw new TransportError(
      `docker-helper endpoint not reachable at ${socketPath}: ${describeError(cause)}`,
    );
  }
  let parsed: unknown = null;
  try {
    parsed = await response.json();
  } catch {
    parsed = null;
  }
  const obj = typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>) : null;
  if (!(response.status >= 200 && response.status < 300) || obj === null || obj.ok !== true) {
    const apiError = parsed as HelperApiErrorBody | null;
    const detail =
      apiError !== null && apiError.ok === false
        ? `${apiError.code ?? "error"}: ${apiError.message ?? "no diagnostics"}`
        : `HTTP ${response.status}`;
    throw new TransportError(`docker-helper ${method} ${path} failed (${detail})`);
  }
  return { httpStatus: response.status, body: obj };
}

function expectStringField(body: Record<string, unknown>, field: string, what: string): string {
  const value = body[field];
  if (typeof value !== "string" || value === "") {
    throw new TransportError(`${what} response has no valid ${field}`);
  }
  return value;
}

const POLL_INTERVAL_MS = 250;
const TERMINAL_RESULT_CODES = new Set(["docker_run_failed", "container_exit_nonzero", "cancelled"]);

interface OperationStatus {
  status: "running" | "succeeded" | "failed";
  exitCode: number | null;
  resultCode: string | null;
}

export class HttpHelperTransport implements HelperTransport {
  private readonly socketPath: string;
  private runInFlight = false;
  private abortRequested = false;
  private cancelSent = false;
  private abortSettled: Promise<void> | null = null;
  private abortResolve: (() => void) | null = null;
  private abortReject: ((cause: Error) => void) | null = null;
  private activeOperationId: string | null = null;
  private activeBearer: string | null = null;
  private truncationWarned = false;

  constructor(socketPath: string) {
    this.socketPath = socketPath;
  }

  async pull(image: string, bearer: string): Promise<void> {
    const result = await helperRequest(this.socketPath, bearer, "POST", "/pull", { image });
    const output = result.body.output;
    if (typeof output === "string" && output.trim() !== "") {
      console.log(output);
    }
  }

  cancelActive(): Promise<void> {
    this.abortRequested = true;
    if (this.abortSettled !== null) {
      return this.abortSettled;
    }
    if (this.runInFlight) {
      // The in-flight run owns the operation lifecycle; it applies the pending
      // cancellation (immediately once it knows the operation identity, or on
      // its next poll iteration) and settles this promise after the operation
      // reaches a confirmed terminal state.
      this.abortSettled = new Promise<void>((resolve, reject) => {
        this.abortResolve = resolve;
        this.abortReject = reject;
      });
      return this.abortSettled;
    }
    this.abortSettled = Promise.resolve();
    return this.abortSettled;
  }

  private settleAbort(cause?: Error): void {
    if (this.abortResolve === null && this.abortReject === null) {
      return;
    }
    const reject = this.abortReject;
    const resolve = this.abortResolve;
    this.abortResolve = null;
    this.abortReject = null;
    if (cause !== undefined && reject !== null) {
      reject(cause);
    } else if (resolve !== null) {
      resolve();
    }
  }

  async run(spec: WorkerSpec, bearer: string): Promise<RunOutcome> {
    if (this.runInFlight) {
      throw new TransportError("a worker run is already in flight");
    }
    if (this.abortRequested) {
      throw new TransportError("worker cancellation was requested before this run started");
    }
    this.runInFlight = true;
    this.cancelSent = false;
    this.activeBearer = bearer;
    try {
      const started = await helperRequest(this.socketPath, bearer, "POST", "/run", {
        image: spec.image,
        ...(spec.entrypoint !== undefined ? { entrypoint: spec.entrypoint } : {}),
        command: spec.command,
        environment: spec.containerEnv,
        mounts: spec.mounts,
        workdir: spec.workdir,
      });
      if (started.httpStatus !== 201) {
        throw new TransportError(
          `docker-helper POST /run returned HTTP ${started.httpStatus}, expected 201`,
        );
      }
      if (started.body.status !== "running") {
        throw new TransportError(
          `docker-helper POST /run returned unexpected status ${JSON.stringify(started.body.status)}, expected "running"`,
        );
      }
      const operationId = expectStringField(started.body, "operation_id", "docker-helper POST /run");
      this.activeOperationId = operationId;

      let logOffset = 0;
      for (;;) {
        const op = await this.fetchOperation(operationId, bearer);
        logOffset = await this.drainLogs(operationId, bearer, logOffset);
        if (this.abortRequested && !this.cancelSent) {
          this.cancelSent = true;
          await this.sendCancel(operationId, bearer);
        }
        if (op.status === "running") {
          await Bun.sleep(POLL_INTERVAL_MS);
          continue;
        }
        // Terminal state: drain any log content written between the last poll
        // and the terminal transition before reporting the outcome.
        await this.drainLogs(operationId, bearer, logOffset);
        const code = this.exitCodeFor(op);
        this.settleAbort();
        return { code, operationId };
      }
    } catch (cause) {
      this.settleAbort(cause instanceof Error ? cause : new Error(String(cause)));
      throw cause;
    } finally {
      this.runInFlight = false;
      this.activeOperationId = null;
      this.activeBearer = null;
    }
  }

  private async sendCancel(operationId: string, bearer: string): Promise<void> {
    const cancelled = await helperRequest(
      this.socketPath,
      bearer,
      "POST",
      `/operations/${operationId}/cancel`,
    );
    const echo = expectStringField(cancelled.body, "operation_id", "docker-helper cancel");
    if (echo !== operationId) {
      throw new TransportError(
        `docker-helper cancel response operation_id mismatch: ${echo} != ${operationId}`,
      );
    }
  }

  private async fetchOperation(operationId: string, bearer: string): Promise<OperationStatus> {
    const op = await helperRequest(this.socketPath, bearer, "GET", `/operations/${operationId}`);
    const echo = expectStringField(op.body, "operation_id", "docker-helper status");
    if (echo !== operationId) {
      throw new TransportError(
        `docker-helper status response operation_id mismatch: ${echo} != ${operationId}`,
      );
    }
    const status = expectStringField(op.body, "status", "docker-helper status");
    if (status !== "running" && status !== "succeeded" && status !== "failed") {
      throw new TransportError(
        `docker-helper status response has unexpected status ${JSON.stringify(status)}`,
      );
    }
    const rawResultCode = op.body.result_code;
    const rawExitCode = op.body.exit_code;
    if (status === "running") {
      return { status, exitCode: null, resultCode: null };
    }
    const resultCode =
      typeof rawResultCode === "string" && rawResultCode !== ""
        ? rawResultCode
        : null;
    if (resultCode === null) {
      throw new TransportError(
        `docker-helper terminal status response has no valid result_code (status ${JSON.stringify(status)})`,
      );
    }
    if (status === "succeeded" && resultCode !== "succeeded") {
      throw new TransportError(
        `docker-helper status response is inconsistent: status succeeded with result_code ${JSON.stringify(resultCode)}`,
      );
    }
    if (status === "failed" && !TERMINAL_RESULT_CODES.has(resultCode)) {
      throw new TransportError(
        `docker-helper status response has unexpected result_code ${JSON.stringify(resultCode)}`,
      );
    }
    const exitCode =
      typeof rawExitCode === "number" && Number.isInteger(rawExitCode) ? rawExitCode : null;
    if (status === "failed" && exitCode === null) {
      throw new TransportError(
        "docker-helper failed status response has no integer exit_code",
      );
    }
    return { status, exitCode, resultCode };
  }

  private async drainLogs(
    operationId: string,
    bearer: string,
    offset: number,
  ): Promise<number> {
    const logs = await helperRequest(
      this.socketPath,
      bearer,
      "GET",
      `/operations/${operationId}/logs?offset=${offset}`,
    );
    const echo = expectStringField(logs.body, "operation_id", "docker-helper logs");
    if (echo !== operationId) {
      throw new TransportError(
        `docker-helper logs response operation_id mismatch: ${echo} != ${operationId}`,
      );
    }
    const echoedOffset = logs.body.offset;
    if (typeof echoedOffset !== "number" || !Number.isInteger(echoedOffset) || echoedOffset !== offset) {
      throw new TransportError(
        `docker-helper logs response has unexpected offset ${JSON.stringify(echoedOffset)}, expected ${offset}`,
      );
    }
    const nextOffset = logs.body.next_offset;
    if (
      typeof nextOffset !== "number" ||
      !Number.isInteger(nextOffset) ||
      nextOffset < offset
    ) {
      throw new TransportError(
        `docker-helper logs response has no valid next_offset ${JSON.stringify(nextOffset)}`,
      );
    }
    const truncated = logs.body.truncated;
    if (typeof truncated !== "boolean") {
      throw new TransportError(
        `docker-helper logs response has non-boolean truncated ${JSON.stringify(truncated)}`,
      );
    }
    const chunk = logs.body.logs;
    if (typeof chunk !== "string") {
      throw new TransportError("docker-helper logs response has no string logs field");
    }
    if (chunk !== "") {
      process.stdout.write(chunk);
    }
    if (truncated && !this.truncationWarned) {
      console.error(
        "orchestrator: warning: docker-helper discarded the earliest operation log content (log buffer limit reached); the remaining log is incomplete",
      );
      this.truncationWarned = true;
    }
    return nextOffset;
  }

  private exitCodeFor(op: OperationStatus): number {
    if (op.status === "succeeded") {
      return 0;
    }
    if (op.resultCode === "cancelled") {
      return 143;
    }
    if (op.exitCode !== null && op.exitCode > 0) {
      return op.exitCode;
    }
    return 1;
  }
}
