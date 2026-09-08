import { describeError } from "./docker_helper.ts";
import type { WorkerSpec } from "./worker.ts";

export interface RunOutcome {
  code: number;
  operationId: string;
}

export interface HelperTransport {
  pull(image: string, bearer: string): Promise<void>;
  run(spec: WorkerSpec, bearer: string): Promise<RunOutcome>;
  cancelActive(): void;
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

async function helperRequest(
  socketPath: string,
  bearer: string,
  method: "GET" | "POST",
  path: string,
  body?: unknown,
): Promise<unknown> {
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
  return obj;
}

const POLL_INTERVAL_MS = 250;

export class HttpHelperTransport implements HelperTransport {
  private readonly socketPath: string;
  private activeOperationId: string | null = null;
  private activeBearer: string | null = null;

  constructor(socketPath: string) {
    this.socketPath = socketPath;
  }

  async pull(image: string, bearer: string): Promise<void> {
    const result = (await helperRequest(this.socketPath, bearer, "POST", "/pull", {
      image,
    })) as { message?: string; output?: string };
    if (typeof result.output === "string" && result.output.trim() !== "") {
      console.log(result.output);
    }
  }

  cancelActive(): void {
    const operationId = this.activeOperationId;
    const bearer = this.activeBearer;
    if (operationId === null || bearer === null) {
      return;
    }
    this.activeOperationId = null;
    void helperRequest(this.socketPath, bearer, "POST", `/operations/${operationId}/cancel`).catch(
      () => undefined,
    );
  }

  async run(spec: WorkerSpec, bearer: string): Promise<RunOutcome> {
    const started = (await helperRequest(this.socketPath, bearer, "POST", "/run", {
      image: spec.image,
      ...(spec.entrypoint !== undefined ? { entrypoint: spec.entrypoint } : {}),
      command: spec.command,
      environment: spec.containerEnv,
      mounts: spec.mounts,
      workdir: spec.workdir,
    })) as { operation_id?: unknown };
    if (typeof started.operation_id !== "string" || started.operation_id === "") {
      throw new TransportError("docker-helper run response has no operation_id");
    }
    const operationId = started.operation_id;
    this.activeOperationId = operationId;
    this.activeBearer = bearer;
    try {
      let logOffset = 0;
      for (;;) {
        const op = (await helperRequest(
          this.socketPath,
          bearer,
          "GET",
          `/operations/${operationId}`,
        )) as { status?: unknown; exit_code?: unknown; result_code?: unknown };
        const status = typeof op.status === "string" ? op.status : "running";
        logOffset = await this.drainLogs(operationId, bearer, logOffset);
        if (status === "running") {
          await Bun.sleep(POLL_INTERVAL_MS);
          continue;
        }
        return { code: this.exitCodeFor(status, op.exit_code, op.result_code), operationId };
      }
    } finally {
      if (this.activeOperationId === operationId) {
        this.activeOperationId = null;
        this.activeBearer = null;
      }
    }
  }

  private async drainLogs(
    operationId: string,
    bearer: string,
    offset: number,
  ): Promise<number> {
    const logs = (await helperRequest(
      this.socketPath,
      bearer,
      "GET",
      `/operations/${operationId}/logs?offset=${offset}`,
    )) as { logs?: unknown; next_offset?: unknown; truncated?: unknown };
    const chunk = typeof logs.logs === "string" ? logs.logs : "";
    const nextOffset = typeof logs.next_offset === "number" ? logs.next_offset : offset;
    if (chunk !== "") {
      process.stdout.write(chunk);
    }
    if (logs.truncated !== true && nextOffset > offset) {
      return nextOffset;
    }
    return offset;
  }

  private exitCodeFor(status: string, exitCode: unknown, resultCode: unknown): number {
    if (status === "succeeded") {
      return 0;
    }
    if (status === "cancelled" || resultCode === "cancelled") {
      return 143;
    }
    if (typeof exitCode === "number" && Number.isInteger(exitCode) && exitCode > 0) {
      return exitCode;
    }
    return 1;
  }
}
