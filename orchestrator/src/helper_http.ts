/**
 * The single HTTP transport owner for Docker Helper operations over the
 * helper Unix socket.
 *
 * Extracted from the access pattern of `fetchAuthOverSocket` (Bun `fetch`
 * with the `unix` option, explicit `Authorization: Bearer` header, no
 * shell, no PATH lookup, no ambient credentials) and hardened for the
 * worker operation path: bounded response reads, a bounded request
 * timeout, external abort support, one JSON parser owner and content-free
 * diagnostics that never carry the bearer token, request bodies or
 * response fragments.
 *
 * The transport is deliberately response-shape agnostic: endpoint handlers
 * own the exact response-shape validation. Everything in this module is
 * internal to the orchestrator; it is not a public API surface.
 */

export type HelperHttpFailureReason =
  | "request_invalid"
  | "connect_failed"
  | "status_rejected"
  | "timeout"
  | "aborted"
  | "response_too_large"
  | "protocol_invalid";

export class HelperHttpError extends Error {
  readonly reason: HelperHttpFailureReason;
  /** The HTTP status when the failure happened after a response arrived. */
  readonly statusCode: number | null;

  constructor(reason: HelperHttpFailureReason, message: string, statusCode: number | null = null) {
    super(message);
    this.name = "HelperHttpError";
    this.reason = reason;
    this.statusCode = statusCode;
  }
}

export interface HelperHttpExchangeOptions {
  /** Absolute path of the Docker Helper Unix socket. */
  readonly socketPath: string;
  /** The bearer token sent as the only credential; never logged. */
  readonly bearer: string;
  readonly method: "GET" | "POST";
  /** Absolute request path including any query string. */
  readonly path: string;
  /** Pre-serialized request body for POST requests. */
  readonly body?: string;
  /** Hard bound for the whole exchange (connect, response head, body). */
  readonly timeoutMs: number;
  /** Hard bound for the response body in bytes. */
  readonly maxResponseBytes: number;
  /**
   * External abort signal. When it fires the exchange rejects with
   * `aborted`; the per-exchange timeout rejects with `timeout`.
   */
  readonly signal?: AbortSignal;
}

export interface HelperHttpResponse {
  readonly status: number;
  readonly bodyText: string;
}

const HELPER_REQUEST_PATH_PATTERN = /^\/[A-Za-z0-9/._?=&-]{0,512}$/;

/**
 * Performs one bounded HTTP exchange over the helper Unix socket. The
 * bearer travels only in the `Authorization` header — never in the URL,
 * the body or any diagnostic.
 */
export async function helperHttpExchange(options: HelperHttpExchangeOptions): Promise<HelperHttpResponse> {
  const { socketPath, bearer, method, path, body, timeoutMs, maxResponseBytes, signal } = options;
  if (typeof socketPath !== "string" || socketPath === "" || !socketPath.startsWith("/")) {
    throw new HelperHttpError("request_invalid", "docker-helper request has an invalid socket path");
  }
  if (typeof bearer !== "string" || bearer === "") {
    throw new HelperHttpError("request_invalid", "docker-helper request has no bearer credential");
  }
  if (!HELPER_REQUEST_PATH_PATTERN.test(path)) {
    throw new HelperHttpError("request_invalid", "docker-helper request has an invalid request path");
  }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new HelperHttpError("request_invalid", "docker-helper request has an invalid timeout");
  }
  if (!Number.isSafeInteger(maxResponseBytes) || maxResponseBytes <= 0) {
    throw new HelperHttpError("request_invalid", "docker-helper request has an invalid response bound");
  }

  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  const onExternalAbort = () => {
    controller.abort();
  };
  signal?.addEventListener("abort", onExternalAbort, { once: true });

  let response: Response;
  try {
    response = await fetch(`http://localhost${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${bearer}`,
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
      },
      body,
      signal: controller.signal,
      unix: socketPath,
    } as RequestInit);
  } catch {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onExternalAbort);
    if (timedOut) {
      throw new HelperHttpError("timeout", "docker-helper request timed out");
    }
    if (signal?.aborted === true) {
      throw new HelperHttpError("aborted", "docker-helper request was aborted");
    }
    throw new HelperHttpError("connect_failed", "docker-helper endpoint not reachable");
  }

  try {
    const reader = response.body?.getReader() ?? null;
    if (reader === null) {
      return { status: response.status, bodyText: "" };
    }
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value !== undefined) {
        total += value.byteLength;
        if (total > maxResponseBytes) {
          throw new HelperHttpError(
            "response_too_large",
            "docker-helper response exceeded the response size bound",
            response.status,
          );
        }
        chunks.push(value);
      }
    }
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return { status: response.status, bodyText: new TextDecoder().decode(merged) };
  } catch (cause) {
    if (cause instanceof HelperHttpError) {
      throw cause;
    }
    if (timedOut) {
      throw new HelperHttpError("timeout", "docker-helper response timed out", response.status);
    }
    if (signal?.aborted === true) {
      throw new HelperHttpError("aborted", "docker-helper response was aborted", response.status);
    }
    throw new HelperHttpError("connect_failed", "docker-helper response stream failed", response.status);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onExternalAbort);
  }
}

/**
 * The one JSON parser owner for helper responses. Diagnostics are
 * content-free: they never carry a parser fragment, position or the
 * response body.
 */
export function parseHelperJsonObject(bodyText: string, what: string): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(bodyText) as unknown;
  } catch {
    throw new HelperHttpError("protocol_invalid", `${what} is not valid JSON`);
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new HelperHttpError("protocol_invalid", `${what} is not a JSON object`);
  }
  return parsed as Record<string, unknown>;
}
